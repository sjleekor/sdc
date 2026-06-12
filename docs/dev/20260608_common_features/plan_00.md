# 공통 시장/거시 피쳐 수집 계획

- 작성일: 2026-06-08
- 대상 저장소: `stock_data_collector`
- 입력 조사 문서:
  - `docs/dev/20260608_common_features/bare_result_00.md`
  - `docs/dev/20260608_common_features/bare_result_01.md`
- 목표: 종목별 피쳐가 아닌 시장 전체/거시 환경 피쳐를 수집하고, KRX 거래일 기준으로 point-in-time 정렬해 기존 종목별 가격/수급/재무 피쳐와 결합 가능한 기반을 만든다.

## 1. 조사 문서 요약

두 조사 문서는 현재 데이터셋의 약점을 "시장 전체 레짐을 설명하는 축이 부족하다"로 정리한다. 현재 프로젝트는 종목별 일봉, 종목별 수급/공매도, OpenDART 재무/주식수/XBRL, 일부 섹터 KPI를 수집하지만, 금리/환율/글로벌 위험선호/수출 사이클/원자재 같은 전 종목 공통 변수가 없다.

우선 후보는 아래 그룹이다.

| 그룹 | 주요 피쳐 | 1차 추천 원천 |
|---|---|---|
| 국내 시장지수 | KOSPI, KOSDAQ, KOSPI200, KOSDAQ150, KRX300, VKOSPI | KRX, pykrx/FDR fallback |
| 업종지수 | KRX/KOSPI/KOSDAQ 업종지수, 업종 수익률/변동성 | KRX, pykrx |
| 시장 breadth/수급 | 상승/하락 종목 수, 거래대금, 신고가/신저가, 시장별 투자자 순매수 | KRX |
| 국내 금리 | 기준금리, CD91, CP91, 국고채 1Y/3Y/5Y/10Y, 회사채 AA-/BBB- | 한국은행 ECOS |
| 해외 금리/위험 | 미국 2Y/10Y, Fed Funds, S&P500, Nasdaq, SOX, VIX | FRED, FDR |
| 환율 | USD/KRW, JPY/KRW, CNY/KRW, EUR/KRW, DXY | ECOS, FRED/FDR |
| 물가/경기 | CPI, Core CPI, PPI, GDP, 산업생산, 소매판매, 선행지수, 심리지수 | ECOS, KOSIS |
| 무역/수출입 | 총수출/수입, 무역수지, 반도체/자동차/선박/석유제품/2차전지 수출 | 관세청, KOSIS, KITA |
| 원자재 | WTI, Brent, 천연가스, 금, 구리, 철광석, 니켈, 리튬 | FRED, FDR, Nasdaq Data Link 등 |
| interaction | 금리 변화 x 부채비율, 환율 x 수출 노출, 유가 x COGS 비율 | 공통 피쳐 + `stock_metric_fact` |

핵심 제약은 look-ahead bias 방지다. 월간/분기 거시지표는 `period_end_date`가 아니라 실제 `release_date` 이후에만 사용해야 한다. 일간 시장 데이터도 "해당 거래일 장마감 후 알 수 있는 값"과 "당일 장 시작 전에 알 수 있는 값"을 구분해야 한다.

## 2. 현재 프로젝트 수집 구조 검토

현재 프로젝트는 포트 & 어댑터 구조를 따른다.

| 영역 | 코드/문서 | 현재 방식 |
|---|---|---|
| 아키텍처 | `docs/architecture.md` | CLI -> service use-case -> provider port -> adapter -> PostgreSQL storage |
| 실행 감사 | `ingestion_runs` | 모든 주요 수집/정규화 작업이 `run_type`, `params`, `counts`, `status`, `error_summary`를 기록 |
| 종목 마스터 | `universe sync` | FDR/pykrx provider가 `stock_master`, snapshot 테이블에 upsert |
| 종목 일봉 | `prices backfill` | pykrx로 종목별 OHLCV를 가져와 `daily_ohlcv`에 upsert |
| 수급/공매도 | `flows sync` | KRX MDC 직접 provider가 `krx_security_flow_raw` long table에 metric row 저장 |
| 재무 raw | `dart sync-*` | OpenDART 원천별 raw 테이블에 저장하고 key coverage로 재수집 skip |
| 재무 정규화 | `metrics normalize` | `metric_catalog`/`metric_mapping_rule` seed 후 `stock_metric_fact` 생성 |
| 섹터 KPI | `operating process-document` | source document와 extracted fact를 별도 테이블로 관리 |

공통 피쳐 구현에서 재사용할 패턴:

1. 도메인 dataclass는 순수 Python 객체로 둔다.
2. 외부 원천 호출은 provider protocol로 분리한다.
3. 원천별 adapter는 raw payload와 source/fetched_at을 보존한다.
4. PostgreSQL upsert는 자연키 기반으로 멱등성을 유지한다.
5. 장기 실행 작업은 `ingestion_runs`에 running -> success/partial/failed를 기록한다.
6. KRX/OpenDART처럼 불안정한 외부 호출은 retry, rate limit, progress log를 서비스 계층에서 공통 처리한다.
7. raw 수집과 모델용 정규화/피쳐화는 분리한다.

공통 피쳐 구현에서 그대로 쓰기 어려운 부분:

1. `stock_metric_fact`는 `ticker`, `corp_code`, `bsns_year`, `reprt_code`가 필수라 시장 공통 시계열을 넣기에 맞지 않는다.
2. 기존 `metric_catalog`는 기업 재무 metric 중심이다. 금리/환율/시장지수 feature code를 섞으면 의미 경계가 흐려진다.
3. 기존 일봉/수급은 `trade_date` 기준이지만, 거시지표는 `period_end_date`, `release_date`, `vintage`가 필요하다.
4. 업종지수와 종목을 결합하려면 현재 없는 `stock -> industry/sector` PIT mapping이 추가로 필요하다.

따라서 공통 피쳐는 별도 catalog/raw/fact 계층을 두고, 나중에 ML ETL의 silver/gold 계층에서 종목별 panel과 join하는 구조가 맞다.

## 3. 설계 원칙

1. **별도 공통 피쳐 계층을 둔다**
   - `stock_metric_fact`에 억지로 넣지 않는다.
   - raw observation과 daily aligned fact를 분리한다.

2. **long format을 기본으로 한다**
   - wide table은 편하지만 피쳐 추가 때마다 DDL 변경이 필요하다.
   - 저장 원장은 `(feature_date, feature_code)` long format으로 두고, 분석/ETL에서 wide view를 만든다.

3. **point-in-time availability를 명시한다**
   - 모든 observation에 `period_end_date`, `observation_date`, `release_date`, `available_from_date`, `vintage`를 둘 수 있어야 한다.
   - daily fact는 `available_from_date <= feature_date`인 값만 사용한다.

4. **원천과 변환을 분리한다**
   - ECOS/FRED/KRX/FDR 원천값은 raw observation으로 저장한다.
   - 수익률, 변화폭, rolling volatility, spread, YoY/MoM은 daily fact builder에서 생성한다.

5. **처음부터 운영 재수집을 고려한다**
   - 월간/분기 통계는 revision이 있으므로 최근 N개월을 매번 재조회한다.
   - 같은 `(source, series_id, period_end_date, release_date, vintage)`는 upsert로 덮어쓴다.

## 4. 데이터 모델 초안

### 4.0 공통 정책 (DDL/도메인 결정 사항)

아래 4개 항목은 PR 1 DDL을 굳히기 전에 결정한다. 이후 PR 단계에서 바꾸면 storage/migration 비용이 크다.

1. **NULLS NOT DISTINCT 정책** — `period_end_date`, `release_date`가 nullable이면 PostgreSQL UNIQUE에서 NULL은 서로 다른 값으로 취급되어 일간 시리즈(release_date NULL) 중복이 무한 insert 가능하다. 본 프로젝트는 PG15+를 가정하고 `UNIQUE NULLS NOT DISTINCT`를 사용한다. PG15 미만 호환이 필요할 때만 sentinel(`'0001-01-01'`) + NOT NULL로 폴백한다.
2. **`available_from_date` 산출 책임은 service 계층** — provider는 `observation_date`, `period_end_date`, `release_date`, `vintage`, `source_updated_at`, `raw_payload`만 채운다. service가 catalog `availability_policy` + `source_timezone` + `manual_lag_days`를 입력으로 도메인 함수 `compute_available_from(...)`을 호출해 raw upsert 직전에 채운다. 정책 함수는 `domain/availability.py`에 두고, PR 1에 unit test로 정책별 케이스를 모두 잠근다.
3. **vintage 선택 규칙** — daily fact 빌드시 같은 `(series_id, period_end_date)` 그룹에서 `release_date <= feature_date` (없으면 `available_from_date <= feature_date`)인 vintage 중 가장 최근 release를 PIT 값으로 쓴다. 이 규칙은 PR 3 fact builder의 핵심 invariant이며 9.2에 검사 항목으로 둔다.
4. **timezone 정책** — `feature_date`는 KRX local date(Asia/Seoul) 기준이다. raw `observation_date`는 원천 시장 local date 그대로 저장한다. catalog에 `source_timezone`을 두고, 미국 종가처럼 KRX 다음 morning에 사용하는 series는 `same_krx_session_morning` 정책 + `source_timezone='America/New_York'` 조합으로 변환한다.

### 4.1 Source enum 확장

`src/krx_collector/domain/enums.py`의 `Source`에 아래 값을 추가한다.

```python
ECOS = "ECOS"
FRED = "FRED"
KOSIS = "KOSIS"
CUSTOMS = "CUSTOMS"
KITA = "KITA"
NASDAQ_DATA_LINK = "NASDAQ_DATA_LINK"
```

기존 `FDR`, `PYKRX`, `KRX`는 국내/글로벌 시장 데이터 fallback 또는 KRX 공식/비공식 호출에 계속 사용한다.

### 4.2 `common_feature_series`

원천 series catalog다. API 코드와 수집 정책을 데이터로 관리한다.

```sql
CREATE TABLE IF NOT EXISTS common_feature_series (
    series_id             TEXT        PRIMARY KEY,
    source                TEXT        NOT NULL,
    source_series_key     TEXT        NOT NULL,
    category              TEXT        NOT NULL,
    frequency             TEXT        NOT NULL, -- D | M | Q | A | EVENT
    name_kr               TEXT        NOT NULL,
    name_en               TEXT        NOT NULL DEFAULT '',
    unit                  TEXT        NOT NULL DEFAULT '',
    country               TEXT        NOT NULL DEFAULT '',
    market                TEXT        NOT NULL DEFAULT '',
    endpoint_params       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    availability_policy   TEXT        NOT NULL DEFAULT 'release_date',
    manual_lag_days       INT         NOT NULL DEFAULT 0,
    source_timezone       TEXT        NOT NULL DEFAULT 'Asia/Seoul',
    history_start_date    DATE,
    max_stale_business_days INT       NOT NULL DEFAULT 5,
    default_transform     TEXT        NOT NULL DEFAULT '',
    active                BOOLEAN     NOT NULL DEFAULT TRUE,
    notes                 TEXT        NOT NULL DEFAULT '',
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`availability_policy` 예:

| 값 | 의미 |
|---|---|
| `next_krx_session` | KRX 장마감 후 값. 다음 KRX 거래일부터 사용 |
| `same_krx_session_morning` | 한국 장 시작 전에 확정되는 해외 전일 데이터 |
| `release_date` | 월간/분기 통계 발표일 이후 사용 |
| `event_date` | 기준금리 결정 등 이벤트일 이후 사용 |
| `manual_lag_days` | catalog의 lag 설정을 보수적으로 적용 |

`source_timezone`, `history_start_date`, `max_stale_business_days`의 의미:

- `source_timezone`: `observation_date`가 정의되는 시장의 IANA timezone. KRX/ECOS 국내 = `Asia/Seoul`, FRED/미국 시장 = `America/New_York`.
- `history_start_date`: 백필 가능한 최소 일자. CLI `--start`가 이보다 과거여도 service가 catalog 값으로 clamp 한다.
- `max_stale_business_days`: daily fact 빌더가 forward-fill을 허용하는 한도. 초과 구간은 NULL로 둬 stale 노이즈를 차단한다(monthly/quarterly는 별도, 다음 release까지 ffill).

### 4.3 `common_feature_observation_raw`

원천 observation long table이다.

```sql
CREATE TABLE IF NOT EXISTS common_feature_observation_raw (
    raw_id                BIGSERIAL   PRIMARY KEY,
    source                TEXT        NOT NULL,
    series_id             TEXT        NOT NULL REFERENCES common_feature_series(series_id),
    observation_date      DATE        NOT NULL,
    period_end_date       DATE,
    release_date          DATE,
    available_from_date   DATE        NOT NULL,
    vintage               TEXT        NOT NULL DEFAULT '',
    value_numeric         NUMERIC(30, 8),
    value_text            TEXT        NOT NULL DEFAULT '',
    unit                  TEXT        NOT NULL DEFAULT '',
    frequency             TEXT        NOT NULL,
    source_updated_at     TIMESTAMPTZ,
    fetched_at            TIMESTAMPTZ NOT NULL,
    raw_payload           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT uq_common_feature_observation_raw
        UNIQUE NULLS NOT DISTINCT (source, series_id, observation_date, period_end_date, release_date, vintage)
);

CREATE INDEX IF NOT EXISTS ix_common_feature_observation_lookup
    ON common_feature_observation_raw (series_id, available_from_date DESC, observation_date DESC);
```

일간 시계열은 `observation_date = period_end_date = 해당 시장 날짜`로 둔다. 월간/분기 통계는 `observation_date`와 `period_end_date`를 동일하게 둬도 되지만, 원천이 period code만 주는 경우 `period_end_date`를 canonical date로 복원한다.

`available_from_date`는 provider가 아니라 service가 채운다. provider는 raw 3종 날짜(`observation_date`, `period_end_date`, `release_date`)와 `vintage`, `source_updated_at`, `raw_payload`만 채워 반환한다. service는 catalog `availability_policy`, `source_timezone`, `manual_lag_days`를 입력으로 도메인 함수 `compute_available_from(...)`을 호출해 raw upsert 직전에 채운다. 정책별 산출 규칙과 fixture는 PR 1 unit test로 잠근다.

PG15 미만 환경 폴백: `UNIQUE NULLS NOT DISTINCT`가 불가하면 `period_end_date`/`release_date`를 NOT NULL + sentinel(`'0001-01-01'`)로 강제하거나, `COALESCE` 기반 expression unique index를 둔다. 본 프로젝트의 운영 PG는 PG15+이므로 NULLS NOT DISTINCT를 기본으로 한다.

### 4.4 `common_feature_catalog`

모델에 노출할 feature code catalog다. raw series와 1:1일 수도 있고, spread/rolling처럼 파생 feature일 수도 있다.

```sql
CREATE TABLE IF NOT EXISTS common_feature_catalog (
    feature_code          TEXT        PRIMARY KEY,
    feature_name_kr       TEXT        NOT NULL,
    category              TEXT        NOT NULL,
    frequency             TEXT        NOT NULL DEFAULT 'D',
    unit                  TEXT        NOT NULL DEFAULT '',
    transform_code        TEXT        NOT NULL DEFAULT '',
    description           TEXT        NOT NULL DEFAULT '',
    active                BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS common_feature_catalog_input (
    feature_code          TEXT        NOT NULL REFERENCES common_feature_catalog(feature_code) ON DELETE CASCADE,
    series_id             TEXT        NOT NULL REFERENCES common_feature_series(series_id),
    role                  TEXT        NOT NULL DEFAULT 'primary', -- primary | spread_long | spread_short | numerator | denominator | aux
    PRIMARY KEY (feature_code, series_id, role)
);

CREATE INDEX IF NOT EXISTS ix_common_feature_catalog_input_series
    ON common_feature_catalog_input (series_id);
```

`input_series_ids`를 JSONB 배열로 두는 대신 별도 link table을 사용한다. 역방향 coverage 질의("이 series가 어떤 feature에 사용되는가")가 SQL 한 줄로 가능하고 FK 무결성이 유지되며, spread/ratio 같은 multi-input feature의 role 구분도 자연스럽다.

### 4.5 `common_feature_daily_fact`

KRX 거래일 기준으로 정렬된 최종 공통 피쳐 long table이다.

```sql
CREATE TABLE IF NOT EXISTS common_feature_daily_fact (
    feature_date          DATE        NOT NULL,
    feature_code          TEXT        NOT NULL REFERENCES common_feature_catalog(feature_code),
    value_numeric         NUMERIC(30, 8),
    value_text            TEXT        NOT NULL DEFAULT '',
    unit                  TEXT        NOT NULL DEFAULT '',
    source_series_ids     JSONB       NOT NULL DEFAULT '[]'::jsonb,
    source_observation_ids JSONB      NOT NULL DEFAULT '[]'::jsonb,
    asof_available_date   DATE        NOT NULL,
    selected_vintage      TEXT        NOT NULL DEFAULT '',
    generated_at          TIMESTAMPTZ NOT NULL,
    generation_run_id     UUID,
    PRIMARY KEY (feature_date, feature_code)
);

CREATE INDEX IF NOT EXISTS ix_common_feature_daily_fact_lookup
    ON common_feature_daily_fact (feature_code, feature_date DESC);
```

`feature_date`는 KRX local date(Asia/Seoul) 기준이며, "그 날짜 예측/학습 행에 붙일 수 있는 날짜"다. 기본 정책은 장 시작 전 사용 가능 데이터만 포함하는 것이다. 예를 들어 KRX 지수의 2026-06-08 종가는 2026-06-09 feature row부터 사용할 수 있다.

`selected_vintage`는 macro revision이 발생하는 series에서 어떤 vintage를 PIT 값으로 골랐는지 추적용으로 둔다. revision 후 fact를 재생성하면 같은 `(feature_date, feature_code)` 행에서 `selected_vintage`가 바뀐다.

### 4.6 Phase 2 mapping table

업종/수출/원자재 interaction은 종목 속성이 필요하므로 별도 phase로 둔다.

```sql
CREATE TABLE IF NOT EXISTS stock_industry_classification (
    ticker              TEXT        NOT NULL,
    market              TEXT        NOT NULL,
    scheme              TEXT        NOT NULL, -- KRX_INDUSTRY, WICS 등
    valid_from          DATE        NOT NULL,
    valid_to            DATE        NOT NULL DEFAULT DATE '9999-12-31',
    industry_code       TEXT        NOT NULL,
    industry_name       TEXT        NOT NULL,
    source              TEXT        NOT NULL,
    fetched_at          TIMESTAMPTZ NOT NULL,
    raw_payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (ticker, market, scheme, valid_from)
);

CREATE INDEX IF NOT EXISTS ix_stock_industry_classification_asof
    ON stock_industry_classification (ticker, market, scheme, valid_from DESC);
```

SCD2 형태(`valid_from`, `valid_to`)로 두면 PIT 매핑 변경을 자연스럽게 보존하고, as-of join이 `valid_from <= asof_date < valid_to`로 단순해진다. 이력이 없는 시점에는 가장 이른 분류를 사용하지 않고 NULL로 둔다(과거 외삽 금지).

```sql
CREATE TABLE IF NOT EXISTS hs_sector_mapping (
    hs_code             TEXT        NOT NULL,
    hs_level            INT         NOT NULL,
    feature_group       TEXT        NOT NULL,
    sector_code         TEXT        NOT NULL,
    description         TEXT        NOT NULL DEFAULT '',
    weight              NUMERIC(12, 6) NOT NULL DEFAULT 1,
    active              BOOLEAN     NOT NULL DEFAULT TRUE,
    PRIMARY KEY (hs_code, feature_group, sector_code)
);
```

## 5. 코드 구조 계획

### 5.1 도메인 모델

`src/krx_collector/domain/models.py`에 추가한다.

```text
CommonFeatureSeries
CommonFeatureObservation
CommonFeatureCatalogEntry
CommonFeatureDailyFact
CommonFeatureSyncResult
CommonFeatureBuildResult
CommonFeatureCoverageRow
```

### 5.2 포트

새 파일 `src/krx_collector/ports/common_features.py`를 추가한다.

```python
class CommonFeatureProvider(Protocol):
    def source(self) -> Source: ...
    def fetch_series(
        self,
        series: CommonFeatureSeries,
        start: date,
        end: date,
    ) -> CommonFeatureFetchResult: ...
```

Provider는 API별로 하나씩 두되, 서비스는 같은 protocol만 바라본다.

### 5.3 어댑터

1차 구현 어댑터:

```text
src/krx_collector/adapters/common_features_fdr/
  provider.py       # FDR DataReader 기반 글로벌 지수/환율/원자재 연구용 fallback

src/krx_collector/adapters/common_features_pykrx/
  provider.py       # pykrx index API 기반 KRX 지수/업종지수 MVP

src/krx_collector/adapters/common_features_ecos/
  client.py         # ECOS StatisticSearch HTTP client
  provider.py       # 금리/환율/물가/경기 series fetch
  parsers.py

src/krx_collector/adapters/common_features_fred/
  client.py         # FRED observations HTTP client, requests 직접 사용
  provider.py       # 미국 금리/WTI 등
```

2차 구현 어댑터:

```text
src/krx_collector/adapters/common_features_krx/
  client.py         # 기존 flows_krx.client와 공통화 가능성 검토
  provider.py       # KRX 지수/breadth/시장 aggregate 직접 호출
  parsers.py

src/krx_collector/adapters/common_features_kosis/
src/krx_collector/adapters/common_features_customs/
```

주의: `FDR`/`pykrx`는 빠른 MVP에 유용하지만 공식성/라이선스/화면 변경 안정성이 낮을 수 있다. 운영 핵심 피쳐는 ECOS/FRED/KRX 공식 또는 라이선스가 명확한 원천으로 점진 교체한다.

### 5.4 서비스

새 service use-case를 추가한다.

```text
src/krx_collector/service/sync_common_features.py
src/krx_collector/service/build_common_feature_daily_facts.py
src/krx_collector/service/report_common_feature_coverage.py
```

`sync_common_features.py` 책임:

1. active series catalog 조회
2. provider source별 series grouping
3. 기존 raw coverage 확인 후 skip 또는 force 재수집
4. provider 호출, retry/rate-limit 적용
5. raw observation upsert
6. `ingestion_runs` 기록

`build_common_feature_daily_facts.py` 책임:

1. KRX 거래일 calendar 생성
2. `available_from_date <= feature_date` 기준 as-of join
3. daily transform 계산
   - level/latest
   - `ret_1d`, `ret_5d`, `ret_20d`
   - `change_1d`, `change_20d`
   - `vol_20d`, `vol_60d`
   - `yoy`, `mom`
   - spread: `kr_gov10y - kr_gov3y`, `us10y - us2y`, `corp_aa - gov3y`
4. `common_feature_daily_fact` upsert

월간/분기 지표는 raw observation의 발표일 이후부터 다음 release까지 forward-fill한다. 일간 시장 데이터는 catalog availability policy에 따라 다음 KRX 거래일 또는 같은 KRX 거래일에 붙이며, ffill 한도는 catalog `max_stale_business_days`를 따른다(초과 구간은 NULL).

vintage 선택 규칙: 같은 `(series_id, period_end_date)` 그룹에서 `release_date <= feature_date`(없으면 `available_from_date <= feature_date`)인 vintage 중 가장 최근 release를 PIT 값으로 쓴다. macro revision이 들어오면 raw 재수집 후 영향받은 feature_date 범위만 fact를 재생성한다.

`build_common_feature_daily_facts.py`의 `ingestion_runs.params` 스키마는 `{sources, series_ids, feature_codes, start, end, recent_days, recent_months, force}`로 통일한다(sync 서비스도 동일). RunType은 source별로 쪼개지 않고 `COMMON_FEATURE_SYNC`/`COMMON_FEATURE_BUILD` 두 종류만 두며, 세부 source는 `params.source`에 기록한다.

### 5.5 저장소

`Storage` protocol과 `PostgresStorage`에 아래 메서드를 추가한다.

```text
upsert_common_feature_series()
get_common_feature_series()
upsert_common_feature_observations()
count_common_feature_observations()
get_common_feature_observations()
upsert_common_feature_catalog()
get_common_feature_catalog()
upsert_common_feature_daily_facts()
get_common_feature_daily_facts()
count_common_feature_daily_facts()
```

대형 범위 조회가 예상되면 기존 normalize 개선처럼 iterator/page 기반 조회를 바로 고려한다.

### 5.6 CLI

`src/krx_collector/cli/app.py`에 `common` command group을 추가한다.

```bash
uv run krx-collector common sync \
  --sources pykrx,fdr,ecos,fred \
  --start 2020-01-01 \
  --end 2026-06-08

uv run krx-collector common build-daily \
  --start 2020-01-01 \
  --end 2026-06-08

uv run krx-collector common coverage-report \
  --start 2020-01-01 \
  --end 2026-06-08
```

옵션:

| 옵션 | 의미 |
|---|---|
| `--sources` | 수집 원천 allowlist |
| `--series` | series_id allowlist |
| `--feature-codes` | daily fact 생성 feature allowlist |
| `--start`, `--end` | 수집/생성 범위 |
| `--force` | 기존 raw observation이 있어도 재수집 |
| `--recent-days` | daily source 증분 재수집 window |
| `--recent-months` | macro source revision 대응 window |
| `--rate-limit-seconds` | provider 호출 간격 |

### 5.7 설정

`src/krx_collector/infra/config/settings.py`에 추가한다.

```text
ecos_api_key
fred_api_key
kosis_api_key
data_go_kr_api_key
common_feature_rate_limit_seconds
common_feature_recent_days
common_feature_recent_months
```

ECOS/FRED/KOSIS/공공데이터포털 키는 없어도 해당 provider만 비활성화하고, 다른 provider는 실행 가능하게 만든다.

## 6. 초기 feature catalog

### 6.1 Phase 1 MVP

현재 의존성으로 빠르게 붙일 수 있고, 모델 설명력이 클 가능성이 높은 것부터 시작한다.

| feature_code | source 후보 | raw series | transform |
|---|---|---|---|
| `market_kospi_close` | pykrx/KRX | KOSPI index | latest level |
| `market_kospi_ret_1d` | pykrx/KRX | KOSPI index | pct_change 1 |
| `market_kospi_ret_5d` | pykrx/KRX | KOSPI index | pct_change 5 |
| `market_kospi_ret_20d` | pykrx/KRX | KOSPI index | pct_change 20 |
| `market_kosdaq_ret_1d` | pykrx/KRX | KOSDAQ index | pct_change 1 |
| `market_kospi200_ret_1d` | pykrx/KRX | KOSPI200 index | pct_change 1 |
| `global_sp500_ret_1d` | FDR/FRED vendor | S&P500 | same KRX session morning policy |
| `global_nasdaq_ret_1d` | FDR | Nasdaq | same KRX session morning policy |
| `global_sox_ret_1d` | FDR | SOX | same KRX session morning policy |
| `global_vix_level` | FDR/FRED | VIX | latest level |
| `fx_usdkrw_level` | ECOS/FDR | USD/KRW | latest level |
| `fx_usdkrw_ret_5d` | ECOS/FDR | USD/KRW | pct_change 5 |
| `rate_kr_gov3y_level` | ECOS | Korea gov 3Y | latest level |
| `rate_kr_gov10y_level` | ECOS | Korea gov 10Y | latest level |
| `rate_kr_term_spread_10y_3y` | ECOS | gov10y, gov3y | spread |
| `rate_us10y_level` | FRED | US10Y | latest level |
| `rate_us_term_spread_10y_2y` | FRED | US10Y, US2Y | spread |
| `commodity_wti_ret_20d` | FRED/FDR | WTI | pct_change 20 |
| `commodity_copper_ret_20d` | FDR/vendor | Copper | pct_change 20 |

`global_*_ret_1d`는 한국 거래일 t의 feature 행에 들어가는, t 시점에 알 수 있는 가장 최근 미국 1일 수익률(보통 t-1 KST 시점에 마감된 미 증시 종가 기준)이다. naming 혼동을 줄이기 위해 README/catalog `description`에 "feature_date = KRX 거래일, ret_1d = 그 시점에 알 수 있는 가장 최근 1d 수익률"을 명시한다.

### 6.1.1 Phase 1.5 한국 시장 유동성

bare_result_00에서 한국 증시 특성으로 강조된 항목으로, 개인/레버리지 비중이 높은 KRX의 수급/유동성을 빠르게 포착한다.

| feature_code | source | raw series | transform |
|---|---|---|---|
| `liquidity_kofia_customer_deposit_level` | KOFIA freesis | 고객예탁금 | latest level |
| `liquidity_kofia_customer_deposit_chg_5d` | KOFIA freesis | 고객예탁금 | change 5 |
| `liquidity_kofia_margin_loan_level` | KOFIA freesis | 신용융자잔고 | latest level |
| `liquidity_kofia_margin_loan_chg_5d` | KOFIA freesis | 신용융자잔고 | change 5 |

KOFIA freesis는 동적 페이지라 수집 안정성이 낮을 수 있으므로 PR 6 이후 별도 어댑터(`common_features_kofia/`)로 둔다.

### 6.2 Phase 1.5 official macro

| feature_code | source | transform |
|---|---|---|
| `macro_cpi_yoy_latest` | ECOS/KOSIS | latest YoY after release |
| `macro_core_cpi_yoy_latest` | ECOS/KOSIS | latest YoY after release |
| `macro_ppi_yoy_latest` | ECOS | latest YoY after release |
| `macro_m2_yoy_latest` | ECOS | latest YoY after release |
| `macro_consumer_sentiment_level` | ECOS | latest level after release |

### 6.3 Phase 2 trade/sector

| feature_code | source | transform |
|---|---|---|
| `trade_export_total_yoy_latest` | Customs/KOSIS/KITA | YoY after release |
| `trade_import_total_yoy_latest` | Customs/KOSIS/KITA | YoY after release |
| `trade_balance_latest` | Customs/KOSIS/KITA | latest level |
| `trade_semiconductor_export_yoy_latest` | Customs/KITA HS mapping | YoY after release |
| `trade_auto_export_yoy_latest` | Customs/KITA HS mapping | YoY after release |
| `trade_ship_export_yoy_latest` | Customs/KITA HS mapping | YoY after release |
| `trade_battery_export_yoy_latest` | Customs/KITA HS mapping | YoY after release |
| `trade_export_total_10d_yoy_latest` | Customs 10일 초속보 | 10일 누적 YoY after release |
| `trade_export_total_20d_yoy_latest` | Customs 20일 초속보 | 20일 누적 YoY after release |
| `trade_semiconductor_export_10d_yoy_latest` | Customs 10일 + HS mapping | 10일 누적 YoY after release |

월간 수출 데이터는 후행성이 강하므로 관세청 10일 단위 초속보치(매월 11일/21일/익월 1일 발표)를 별도 series로 함께 수집한다. 월간과 10일치는 lead가 다르므로 같은 feature로 합치지 않고 catalog에서 각각 등록한다.

### 6.4 Phase 3 market microstructure

| feature_code | source | transform |
|---|---|---|
| `market_advancers` | KRX | count |
| `market_decliners` | KRX | count |
| `market_advance_decline_ratio` | KRX | advancers / decliners |
| `market_new_highs` | KRX | count |
| `market_new_lows` | KRX | count |
| `market_total_turnover` | KRX | level |
| `market_program_net_buy_value` | KRX | level |
| `market_vkospi_level` | KRX | level |

## 7. 구현 로드맵

### PR 0 - 원천 코드 검증 문서

목표: 실제 API 코드/파라미터를 구현 전에 고정한다.

작업:

1. ECOS 통계코드와 item code 후보를 확인해 `docs/dev/20260608_common_features/source_catalog_00.md`에 기록한다.
2. FRED series id 후보를 기록한다.
3. pykrx/KRX 지수 코드 후보를 작은 sample fetch로 확인한다.
4. 각 원천의 라이선스/상용 사용 제약을 간단히 남긴다.
5. API key 필요 여부와 `.env` 변수명을 확정한다.

산출물:

- source catalog 문서
- Phase 1 catalog seed 후보

### PR 1 - DDL, domain, storage 기반

목표: 아직 외부 API를 호출하지 않고 공통 피쳐 저장 계층을 만든다.

작업:

1. `sql/postgres_ddl.sql`에 5개 테이블 추가
   - `common_feature_series`
   - `common_feature_observation_raw` (UNIQUE NULLS NOT DISTINCT)
   - `common_feature_catalog`
   - `common_feature_catalog_input` (link table)
   - `common_feature_daily_fact`
2. `Source`, `RunType` enum 확장
   - `COMMON_FEATURE_SYNC`
   - `COMMON_FEATURE_BUILD`
3. 도메인 dataclass 추가
4. `domain/availability.py`에 `compute_available_from(...)` 정책 함수 구현
   - 정책별(`next_krx_session`, `same_krx_session_morning`, `release_date`, `event_date`, `manual_lag_days`) 케이스를 fixture 기반 unit test로 잠근다.
   - 입력: `policy`, `observation_date`, `period_end_date`, `release_date`, `source_timezone`, `manual_lag_days`, KRX 거래일 calendar provider.
5. `Storage` protocol과 `PostgresStorage` upsert/query 구현
6. DDL idempotence(반복 실행)와 repository upsert unit test 추가
   - `period_end_date`/`release_date`가 NULL인 일간 raw row가 두 번 insert될 때 멱등성 유지

검증:

```bash
uv run pytest tests/unit/test_common_features_storage.py
uv run python -m compileall src/krx_collector
```

### PR 2 - catalog seed와 FDR/pykrx MVP sync

목표: 일간 시장/글로벌/환율/원자재 일부를 빠르게 raw table에 적재한다.

작업:

1. `ports/common_features.py` 추가
2. `common_features_fdr` provider 추가
3. `common_features_pykrx` provider 추가
4. `sync_common_features.py` 구현
5. `common sync` CLI 추가
6. Phase 1 MVP series/catalog seed 추가
7. mocked provider unit test와 service partial-run test 추가

검증:

```bash
uv run krx-collector common sync --sources pykrx,fdr --series market_kospi,global_sp500 --start 2026-01-01 --end 2026-01-10
uv run pytest tests/unit/test_sync_common_features.py
```

### PR 3 - daily fact builder

목표: raw observation을 KRX 거래일 기준 feature fact로 변환한다.

작업:

1. `build_common_feature_daily_facts.py` 구현
2. trading calendar 기반 `available_from_date <= feature_date` as-of join 구현
3. pct_change, change, rolling volatility, spread transform 구현
4. `common build-daily` CLI 추가
5. feature catalog seed 추가
6. look-ahead 방지 unit test 추가

핵심 테스트(모두 fixture 기반, 외부 API 호출 없이):

1. KOSPI 2026-06-08 종가는 `next_krx_session` 정책에서 2026-06-08 feature row에 들어가면 안 된다.
2. 미국 전일 S&P500 종가는 `same_krx_session_morning` 정책에서 한국 2026-06-08 feature row에 들어갈 수 있다.
3. fake monthly series(예: pseudo-CPI)의 2026-05 period 값은 `release_date=2026-06-12` 이전 feature row에 들어가면 안 된다.
4. fake monthly series에 같은 period_end의 두 vintage가 들어 있을 때 `release_date <= feature_date`인 가장 최근 vintage가 선택되고, `selected_vintage` 컬럼에 기록된다.
5. macro revision 시나리오: 동일 period의 vintage v1 → v2가 들어오면 영향받은 feature_date 범위에서 fact가 v2로 재계산된다.
6. daily series에서 N영업일 결측이 catalog `max_stale_business_days`를 초과하면 fact가 NULL이며, 그 이하면 forward-fill된다.

PR 4/5/6에서 추가하는 series는 PR 3의 PIT 테스트가 통과하기 전까지 catalog `active=false`로 두고 raw만 적재한다(잘못된 PIT 노출 차단). PR 3은 ECOS provider 도입 전이라 release_date/vintage 케이스를 fake monthly series fixture로 미리 잠가두고, 이후 PR 4에서는 wiring만 한다.

### PR 4 - ECOS provider

목표: 국내 금리/환율/물가/통화/심리 지표를 공식 원천으로 수집한다.

작업:

1. `common_features_ecos.client` 구현
2. ECOS response parser 구현
3. `ECOS_API_KEY` 설정 추가
4. rate limit/retry 적용
5. daily 금리/환율과 monthly CPI/PPI/M2/CSI series seed
6. release_date가 API에서 직접 제공되지 않는 series는 1차로 보수적 lag policy를 catalog에 명시하고, 추후 release calendar로 교체한다.

주의:

- ECOS의 통계표별 item code는 수동 catalog 관리가 필요하다.
- 발표일을 모르는 월간 지표는 `period_end_date + conservative_lag_days`로 두되, plan/README에 "임시 정책"임을 명시한다.

### PR 5 - FRED provider

목표: 미국 금리와 일부 원자재/글로벌 지표를 수집한다.

작업:

1. `common_features_fred.client` 구현 완료
2. `FRED_API_KEY` 설정 추가 완료
3. US2Y/US10Y/WTI seed 추가 완료
4. FRED revision/vintage API를 쓸지 여부 결정
   - 1차: latest observation만 저장
   - 2차: vintage/realtime_start/realtime_end 저장
5. live smoke/readiness 완료: US2Y/US10Y/spread active 전환. WTI spot은 FDR futures fallback과 비교 후 별도 spot feature로 병행 active

### PR 6 - KRX 공식 market aggregate/breadth

목표: 현재 `flows_krx`에서 검증한 KRX MDC 직접 호출 패턴을 재사용해 시장 breadth와 업종지수를 안정화한다.

작업:

1. 완료: `flows_krx.client.KrxMdcClient`를 공통 모듈(`adapters/krx_common/client.py`)로 추출하고 `flows_krx`도 그 위에서 동작하도록 재배선했다.
2. 부분 완료: KRX direct 국내 지수 endpoint(`MDCSTAT00301`)는 source catalog에 기록하고 active 전환했다. `MDCSTAT01501` 기반 KOSPI/KOSDAQ breadth/liquidity 8개 feature도 3개월 readiness 통과 후 active 전환했다. 업종지수 4개 후보는 inactive smoke를 통과했고, VKOSPI는 후보 확인 단계에 머물러 있다.
3. 완료: pykrx 기반 KRX 지수 수집을 KRX direct provider로 교체하고 pykrx는 fallback inactive로 격하했다.
4. 완료/보류: 시장별 상승/하락/보합, 거래대금은 `MDCSTAT01501`로 active 전환했다. VKOSPI, 프로그램 매매 추가는 endpoint 확인 후 진행한다.
   - 2026-06-11 확인/활성화: 시장별 전종목 시세/breadth `dbms/MDC/STAT/standard/MDCSTAT01501`, output `OutBlock_1`, params `mktId`, `trdDd`, 주요 컬럼 `FLUC_TP_CD`, `ACC_TRDVOL`, `ACC_TRDVAL`, `MKT_ID`.
   - 2026-06-11 확인한 후보: 지수/업종 일별 시세 `MDCSTAT00101`, 지수 등락률 `MDCSTAT00201`, 개별 지수 시세 `MDCSTAT00301`. KRX 반도체/KOSPI 전기전자/KOSPI 금융/KOSDAQ 제약은 `finder_equidx` code로 inactive smoke를 통과했다.
   - 재검증 결과 `finder_equidx`와 `MDCSTAT01501` HTTP 403은 재현되지 않았다. `finder_equidx`는 업종 code discovery에 사용 가능하지만 VKOSPI 현물 endpoint는 아직 미확정이다.
5. PR 7에서 추가될 KOFIA freesis(고객예탁금/신용융자잔고) 어댑터의 client skeleton은 scope 비대를 피하기 위해 PR 7로 이연한다.

### PR 7 - 무역/업종/interaction/KOFIA 유동성

목표: 한국 시장 특화 설명력이 큰 수출입/업종 피쳐, 증시 유동성, 종목별 interaction을 준비한다.

작업:

1. KOSIS/관세청/KITA 중 1차 원천 결정 — 월간 수출입과 **10일 단위 초속보치**(매월 11일/21일/익월 1일 발표)를 둘 다 series로 등록.
2. HS code mapping seed (`hs_sector_mapping`).
3. KOFIA freesis 어댑터(`common_features_kofia/`)로 고객예탁금/신용융자잔고 일간 수집(동적 페이지 → requests + 가능한 경우 CSV endpoint, 불가 시 Playwright headless로 폴백).
4. `stock_industry_classification` (SCD2) 적재 경로 구현.
5. trade/유동성 feature raw/daily fact 생성.
6. ML ETL에서 `common_feature_daily_fact`와 `stock_metric_fact`를 결합해 interaction 생성
   - `rate_change_20d_x_debt_to_equity`
   - `credit_spread_x_interest_burden`
   - `usdkrw_ret_20d_x_export_exposure`
   - `oil_ret_20d_x_cogs_ratio`

## 8. 운영 계획

### 8.1 Cronicle 추가 순서

기존 운영 순서:

```text
sdc_daily_pipeline:
  universe-sync -> prices-backfill-incremental -> flows-sync

sdc_daily_accounts_flows:
  dart-sync-corp -> dart-sync-financials -> dart-sync-share-info -> dart-sync-xbrl -> metrics-normalize
```

공통 피쳐는 별도 daily event로 시작한다.

```text
sdc_daily_common_features:
  common-sync-daily -> common-sync-macro-recent -> common-build-daily -> common-coverage-report
```

권장 schedule:

| job | 시각(KST) | 범위 |
|---|---:|---|
| `common-sync-daily` | 장마감 이후 18:30 | 최근 14 calendar days |
| `common-sync-global` | 장 시작 전 08:00 | 최근 7 calendar days |
| `common-sync-macro-recent` | 매일 19:00 또는 월초 집중 | 최근 24 months |
| `common-build-daily` | sync 후 | 영향받은 KRX 거래일 |
| `common-coverage-report` | build 후 | 최근 60 거래일 |

초기에는 하나의 wrapper로 묶어도 되지만, global morning feature와 local after-close feature는 availability policy가 다르므로 운영상 분리하는 편이 좋다.

### 8.2 wrapper 예시

```bash
docker compose run --rm collector common sync \
  --sources pykrx,fdr,ecos,fred \
  --recent-days 14 \
  --recent-months 24

docker compose run --rm collector common build-daily \
  --recent-days 30

docker compose run --rm collector common coverage-report \
  --recent-days 60
```

## 9. 품질 기준

### 9.1 수집 품질

| 검사 | 기준 |
|---|---|
| 중복 | natural key 중복 0 |
| stale | daily series는 최근 KRX 거래일 기준 허용 지연 이내 |
| 결측 | 필수 Phase 1 feature의 최근 60 거래일 결측률 5% 이하 |
| 단위 | series catalog unit과 raw unit 불일치 시 warning |
| outlier | 금리/환율/지수 일변화 z-score 또는 임계치 경고 |
| partial run | error가 있어도 성공한 series는 저장하고 `ingestion_runs.status=partial` |

### 9.2 PIT 품질

| 검사 | 기준 |
|---|---|
| release leakage | `common_feature_daily_fact.feature_date < asof_available_date` 행 0 |
| daily close leakage | `next_krx_session` raw 값이 같은 거래일 feature에 들어간 행 0 |
| monthly ffill | release 전 월간 값 forward-fill 금지 |
| source trace | daily fact에서 사용한 raw observation id 추적 가능 |
| vintage selection | 같은 (series, period_end)에서 `release_date <= feature_date`인 가장 최근 vintage가 선택되었는지 검증 |
| revision idempotence | macro revision 후 raw 재수집 → fact 재생성 시 영향 범위 행만 `selected_vintage`/value가 갱신 |
| stale ffill 한도 | daily series ffill이 catalog `max_stale_business_days`를 초과한 행 0 |

### 9.3 회귀 테스트

필수 unit test:

1. provider parser가 원천 응답을 `CommonFeatureObservation`으로 변환한다.
2. sync service가 기존 coverage를 skip한다.
3. 일부 series 실패 시 run이 `partial`로 끝난다.
4. fact builder가 availability policy를 지킨다.
5. transform 결과가 고정 fixture와 일치한다.
6. DDL은 반복 실행 가능하다.
7. `compute_available_from`이 정책별/timezone별 케이스에서 기대값을 낸다.
8. macro revision 시 과거 daily_fact가 새 vintage로 정확히 재계산된다.
9. daily series ffill이 `max_stale_business_days` 한도에서 NULL로 끊긴다.

## 10. ML ETL 연계

`docs/dev/20260607_ETL/etl_design_and_plan.md`의 silver/gold 구조에는 공통 피쳐를 별도 silver component로 추가한다.

```text
Silver:
  common_feature_daily
    key: (trade_date, feature_code)
    source: common_feature_daily_fact

Gold:
  ml_panel_daily_v1
    join: (trade_date)로 모든 종목 행에 공통 피쳐 broadcast
```

초기에는 `common_feature_daily_fact` long table을 DuckDB/Polars에서 pivot해 wide feature matrix로 만든다.

예시:

```sql
SELECT
  feature_date AS trade_date,
  MAX(value_numeric) FILTER (WHERE feature_code = 'market_kospi_ret_1d') AS market_kospi_ret_1d,
  MAX(value_numeric) FILTER (WHERE feature_code = 'fx_usdkrw_ret_5d') AS fx_usdkrw_ret_5d,
  MAX(value_numeric) FILTER (WHERE feature_code = 'rate_kr_term_spread_10y_3y') AS rate_kr_term_spread_10y_3y
FROM common_feature_daily_fact
GROUP BY feature_date;
```

종목별 interaction은 공통 피쳐 수집 모듈이 아니라 ML ETL/gold layer에서 만든다. 이유는 interaction이 종목별 재무/업종 속성과 모델 horizon에 의존하기 때문이다.

## 11. 주요 리스크와 대응

| 리스크 | 영향 | 대응 |
|---|---|---|
| 발표일 부정확 | look-ahead leakage | `release_date` 확보 전까지 보수 lag 사용, feature catalog에 policy 명시 |
| FDR/yfinance 계열 비공식성 | 운영 데이터 안정성/라이선스 리스크 | MVP 연구용으로 제한하고 ECOS/FRED/KRX direct로 교체 |
| KRX 화면 변경 | 수집 실패 | 기존 `flows_krx`처럼 parser/column 검증, partial run, fallback provider |
| source code 수동 관리 | 잘못된 series 수집 | PR 0 source catalog 검증 문서와 seed review |
| wide schema 폭증 | DDL churn | long fact table + ETL pivot |
| macro revision | 과거 값 변동 | 최근 N개월 재수집, vintage 컬럼 저장 |
| 업종 mapping 부재 | industry relative feature 지연 | Phase 2에서 `stock_industry_classification` 별도 구축 |

## 12. 현재 구현 현황

기준: 2026-06-11, KRX breadth/liquidity active 전환, KRX 업종지수 inactive slice, CPI/PPI/M2/CSI monthly macro conservative release policy active 전환까지 완료.

### 12.1 완료된 작업

| 범위 | 상태 | 주요 산출물 |
|---|---|---|
| PR 1 - DDL/domain/storage 기반 | 완료 | `common_feature_series`, `common_feature_observation_raw`, `common_feature_catalog`, `common_feature_catalog_input`, `common_feature_daily_fact` DDL 추가 |
| PR 1 - enum/model | 완료 | `Source` 확장, `RunType.COMMON_FEATURE_SYNC`, `RunType.COMMON_FEATURE_BUILD`, 공통 feature domain dataclass 추가 |
| PR 1 - availability 정책 | 완료 | `domain/availability.py`에 `compute_available_from(...)` 구현 및 정책별 unit test 추가 |
| PR 1 - storage | 완료 | `Storage` protocol과 `PostgresStorage`에 common feature series/raw/catalog/daily fact upsert/query/count 메서드 추가 |
| PR 2-A - provider port | 완료 | `ports/common_features.py`에 `CommonFeatureProvider` protocol 추가 |
| PR 2-A - sync service | 완료 | `service/sync_common_features.py` 구현. active series 조회, provider dispatch, coverage skip, rate limit, partial run, `ingestion_runs` 기록 포함 |
| PR 2-B - seed catalog | 완료 | `service/default_common_feature_catalog.py`에 Phase 1 MVP series/catalog seed 추가 |
| PR 2-C - pykrx provider | 완료 | `adapters/common_features_pykrx/provider.py` 추가. pykrx index OHLCV 종가를 raw observation으로 변환 |
| PR 2-D - FDR provider | 완료 | `adapters/common_features_fdr/provider.py` 추가. FDR DataReader 종가를 raw observation으로 변환. NaN/Inf skip, 요청 범위 필터, end-date 보정 반영 |
| PR 2-E - CLI | 완료 | `krx-collector common seed-catalog`, `krx-collector common sync` 추가 |
| PR 2-E - CLI test | 완료 | common CLI parser/handler unit test 추가 |
| PR 3-A - daily fact builder | 완료 | `service/build_common_feature_daily_facts.py` 구현. PIT as-of join, `level`/`ret_1d`/`ret_5d`/`ret_20d`, stale 한도, vintage 선택, `COMMON_FEATURE_BUILD` run 기록 |
| PR 3-B - build CLI | 완료 | `krx-collector common build-daily` 추가 |
| PR 3-C - build smoke | 완료 | 로컬 DB에서 FDR smoke raw -> daily fact build 경로 검증. PIT 위반 0 확인. builder stale calendar 반복 로딩 보정 |
| PR 3-D - coverage report | 완료 | `service/report_common_feature_coverage.py`, `krx-collector common coverage-report` 추가. fact/non-null/null/missing/coverage/PIT 위반 집계 |
| PR 4-A - FDR smoke 확장 | 완료 | FDR 3개 series 짧은 raw backfill, `global_sp500_ret_1d`/`fx_usdkrw_ret_5d` 실제 계산 확인, FDR end-date 보정 |
| PR 4-B - ECOS provider skeleton | 완료 | `ECOS_API_KEY` 설정, `EcosStatisticSearchClient`, ECOS provider/parser, no-key 비활성화 동작, mock unit test 추가 |
| PR 4-C - ECOS source catalog | 완료 | `source_catalog_00.md` 추가, `rate_kr_gov3y`/`macro_cpi` source series와 `rate_kr_gov3y_level`/`macro_cpi_level` feature를 `active=false`로 seed |
| PR 4-D - ECOS sync wiring | 완료 | `common sync --sources ecos` dispatch 추가, inactive smoke용 `--include-inactive` 추가, `active_only` sync 옵션과 guard/test 추가 |
| PR 4-E - ECOS live smoke | 완료 | `.env`의 `ECOS_API_KEY` 로딩 확인, `rate_kr_gov3y` 단기 live smoke 성공, raw 4 rows upsert, `TIME`/단위/available-from 매핑 확인 |
| PR 4-F - ECOS smoke 확장 | 완료 | `rate_kr_gov3y` 2024년 1월 22/22 rows 확인, `macro_cpi` 2024년 1~3월 3 rows 확인, CPI unit `2020=100` seed/catalog 반영 |
| PR 4-G - inactive fact/coverage 검증 | 완료 | `common build-daily`/`coverage-report`에 `--include-inactive` 추가, `rate_kr_gov3y_level`/`macro_cpi_level` daily fact와 coverage/PIT 검증 |
| PR 4-H - active readiness 기준/리포트 | 완료 | `service/report_common_feature_readiness.py`, `krx-collector common readiness-report` 추가. 기본 기준은 coverage `1.0000`, null/missing/PIT 0 |
| PR 4-I - `rate_kr_gov3y` 운영 범위 확대 검증 | 완료 | 3개월(2026-03-10..2026-06-09)·12개월(2025-06-10..2026-06-09) 범위에서 raw sync -> build -> coverage -> readiness 검증. 두 범위 모두 coverage `1.0000`, null/missing/PIT 위반 0, readiness `true`. catalog active 전환은 별도 PR로 분리(이 PR에서는 미전환) |
| PR 4-J - `rate_kr_gov3y` active 전환 | 완료 | seed에서 `rate_kr_gov3y` series와 `rate_kr_gov3y_level` feature를 `active=true`로 전환. seed 테스트를 active 기대/`macro_cpi` inactive 유지로 분리(unit 226 passed). seed 재적용 후 `--include-inactive` 없이 build/coverage/readiness 동작 확인(12개월 254일, coverage `1.0000`, null/missing/PIT 0, ready). `macro_cpi`는 inactive 유지 |
| transform 확장 - single-input | 완료 | builder에 `change_<N>d`(절대 차분), `vol_<N>d`(N개 1-step 수익률 표본표준편차, ddof=1), `yoy`, `mom` 추가. transform code 파라메트릭 파싱(`ret/change/vol_<N>d`)으로 일반화. `yoy`/`mom`은 캘린더 year-month 정확 매칭(직전 기간 결측 시 NULL). fixture 단위 테스트 6종 추가(unit 232 passed) |
| transform 확장 - multi-input | 완료 | `CommonFeatureCatalogEntry.input_roles` 추가(빈 값=전부 primary, 단일-input 무영향), storage write/read에 role 배선. builder에 `spread`(long-short)/`ratio`(num/denom) 추가. 각 input 독립 as-of join, 결측/stale/0분모 시 NULL, asof=max(available_from). fixture 테스트 8종 추가(unit 240 passed) |
| ECOS 10Y/spread inactive seed | 완료 | ECOS `StatisticItemList`에서 `817Y002`/`010210000`=`국고채(10년)` 확인. `rate_kr_gov10y`, `rate_kr_gov10y_level`, `rate_kr_term_spread_10y_3y`를 `active=false`로 seed 등록. spread input role은 `rate_kr_gov10y=spread_long`, `rate_kr_gov3y=spread_short` |
| ECOS 10Y/spread inactive smoke | 완료 | `rate_kr_gov10y` 2024년 1월 raw 22 rows upsert, `rate_kr_term_spread_10y_3y` build 21 facts/null 0, coverage `1.0000`, PIT 위반 0, readiness `true` |
| ECOS 10Y/spread 운영 범위 확대 검증 | 완료 | 3개월(2026-03-10..2026-06-11)·12개월(2025-06-10..2026-06-11) 범위에서 raw sync -> build -> coverage -> readiness 검증. 두 범위 모두 coverage `1.0000`, null/missing/PIT 위반 0, readiness `true` |
| ECOS 10Y/spread active 전환 | 완료 | seed에서 `rate_kr_gov10y` series와 `rate_kr_gov10y_level`, `rate_kr_term_spread_10y_3y` feature를 `active=true`로 전환. seed 재적용 후 `--include-inactive` 없이 12개월 active-only build/coverage/readiness 통과 |
| FRED provider/live smoke | 완료 | `common_features_fred` client/provider, `FRED_API_KEY` 설정, CLI `--sources fred` 추가. `rate_us2y`/`rate_us10y`/`commodity_wti_fred` raw sync와 후보 feature build/coverage/readiness 검증 |
| FRED US rates active 전환 | 완료 | `rate_us2y`, `rate_us10y`, `rate_us2y_level`, `rate_us10y_level`, `rate_us_term_spread_10y_2y`를 `active=true`로 전환. active-only 12개월 build/coverage/readiness 통과 |
| FRED WTI spot 병행 feature | 완료 | 기존 `commodity_wti_ret_20d`(FDR `CL=F` futures)와 FRED `DCOILWTICO` spot ret20을 236일 paired 비교. correlation 0.991004지만 p95 absolute diff 3.04pp, max 13.48pp라 대체하지 않고 `commodity_wti_spot_ret_20d` active feature를 병행 추가 |
| KRX breadth/liquidity active 전환 | 완료 | `MDCSTAT01501`/`finder_equidx` 403 재검증 성공. `common_features_krx` provider에 `market_breadth` 집계 경로와 row cache 추가. KOSPI/KOSDAQ 상승/하락/보합 종목 수와 거래대금 8개 source/feature를 seed하고, 3개월 raw sync -> build -> coverage/readiness 통과 후 `active=true` 전환 |
| KRX 업종지수 inactive slice | 완료 | `finder_equidx` 후보 중 KRX 반도체, KOSPI 전기전자, KOSPI 금융, KOSDAQ 제약 4개 source와 level/ret_1d 8개 feature를 `active=false`로 seed. provider live smoke, 3개월 raw sync(256 rows), daily build(512 facts), warm-up 제외 coverage/readiness 통과 |
| CPI conservative release policy active 전환 | 완료 | `macro_cpi`는 `period_end_date + 20 calendar days` 후 다음 KRX session부터 사용하도록 보수 정책을 명시. `macro_cpi_yoy_latest`/`macro_cpi_mom_latest`를 seed로 추가하고 2026년 구간 inactive/active-only raw sync -> build -> coverage/readiness 통과 후 active 전환 |
| PPI/M2/CSI monthly macro inactive smoke | 완료 | ECOS metadata에서 PPI `404Y014`/`*AA`, M2 `161Y005`/`BBHS00`, CSI `511Y002`/`FME`/`99988` 확인. 3개 source와 7개 feature를 `active=false`로 seed하고, 2026년 구간 raw sync(48 rows) -> daily build(525 facts/null 0) -> coverage/readiness 1.0000/PIT 0/ready true 확인 |
| PPI/M2/CSI monthly macro active 전환 | 완료 | CPI와 같은 conservative `period_end_date + 20 calendar days` 정책으로 `macro_ppi`, `macro_m2`, `macro_consumer_sentiment`와 7개 feature를 active 전환. active-only sync/build/coverage/readiness 통과(48 raw rows, 525 facts/null 0, coverage 1.0000, PIT 0, ready true) |

### 12.2 현재 seed 범위

현재 active seed는 연구용 MVP에 필요한 최소 시장/금리/거시 series 위주다.

| 구분 | series_id |
|---|---|
| 국내 시장지수(KRX direct, active) | `market_kospi_krx`, `market_kosdaq_krx`, `market_kospi200_krx` |
| 국내 breadth/liquidity(KRX direct, active) | `market_kospi_advancers_krx`, `market_kospi_decliners_krx`, `market_kospi_unchanged_krx`, `market_kospi_turnover_value_krx`, `market_kosdaq_advancers_krx`, `market_kosdaq_decliners_krx`, `market_kosdaq_unchanged_krx`, `market_kosdaq_turnover_value_krx` |
| 글로벌 시장/위험 | `global_sp500`, `global_nasdaq`, `global_vix` |
| 환율/원자재 | `fx_usdkrw_ecos`(active), `fx_usdkrw`(FDR fallback), `commodity_wti`, `commodity_wti_fred` |
| 국내 금리(ECOS, active) | `rate_kr_gov3y`, `rate_kr_gov10y` |
| 미국 금리(FRED, active) | `rate_us2y`, `rate_us10y` |
| 월간 거시(ECOS, active) | `macro_cpi`, `macro_ppi`, `macro_m2`, `macro_consumer_sentiment` |
| inactive 후보 | pykrx fallback 국내 지수 source, KRX 업종지수 4개 source |

현재 active feature code:

```text
market_kospi_close
market_kospi_ret_1d
market_kospi_ret_5d
market_kospi_ret_20d
market_kosdaq_ret_1d
market_kospi200_ret_1d
global_sp500_ret_1d
global_nasdaq_ret_1d
global_vix_level
fx_usdkrw_level
fx_usdkrw_ret_5d
commodity_wti_ret_20d
commodity_wti_spot_ret_20d
rate_kr_gov3y_level
rate_kr_gov10y_level
rate_kr_term_spread_10y_3y
rate_us2y_level
rate_us10y_level
rate_us_term_spread_10y_2y
market_kospi_advancers_count
market_kospi_decliners_count
market_kospi_unchanged_count
market_kospi_turnover_value
market_kosdaq_advancers_count
market_kosdaq_decliners_count
market_kosdaq_unchanged_count
market_kosdaq_turnover_value
macro_cpi_level
macro_cpi_yoy_latest
macro_cpi_mom_latest
macro_ppi_level
macro_ppi_yoy_latest
macro_ppi_mom_latest
macro_m2_level
macro_m2_yoy_latest
macro_m2_mom_latest
macro_consumer_sentiment_level
```

현재 inactive 후보 feature code:

```text
market_kospi_krx_close
market_kospi_krx_ret_1d
market_kospi_krx_ret_5d
market_kospi_krx_ret_20d
market_kosdaq_krx_ret_1d
market_kospi200_krx_ret_1d
fx_usdkrw_ecos_level
fx_usdkrw_ecos_ret_5d
commodity_wti_fred_ret_20d
industry_krx_semiconductor_level
industry_krx_semiconductor_ret_1d
industry_kospi_electronics_level
industry_kospi_electronics_ret_1d
industry_kospi_financials_level
industry_kospi_financials_ret_1d
industry_kosdaq_pharma_level
industry_kosdaq_pharma_ret_1d
```

### 12.3 현재 사용 가능한 CLI

```bash
uv run krx-collector common seed-catalog --init-schema

uv run krx-collector common sync \
  --sources pykrx,fdr \
  --start 2026-06-01 \
  --end 2026-06-08

uv run krx-collector common sync \
  --sources ecos \
  --series rate_kr_gov3y \
  --start 2024-01-02 \
  --end 2024-01-05 \
  --include-inactive \
  --rate-limit-seconds 3

uv run krx-collector common build-daily \
  --feature-codes global_sp500_ret_1d,fx_usdkrw_level \
  --start 2024-01-03 \
  --end 2024-01-15

uv run krx-collector common build-daily \
  --feature-codes rate_kr_gov3y_level \
  --start 2024-01-03 \
  --end 2024-01-31 \
  --include-inactive

uv run krx-collector common coverage-report \
  --feature-codes global_sp500_ret_1d,fx_usdkrw_level \
  --start 2024-01-03 \
  --end 2024-01-15

uv run krx-collector common coverage-report \
  --feature-codes rate_kr_gov3y_level \
  --start 2024-01-03 \
  --end 2024-01-31 \
  --include-inactive

uv run krx-collector common readiness-report \
  --feature-codes rate_kr_gov3y_level \
  --start 2024-01-03 \
  --end 2024-01-31 \
  --include-inactive \
  --required-coverage-ratio 1.0
```

`common sync`는 `--sources pykrx,fdr,ecos,fred`, `--series`, `--force`, `--rate-limit-seconds`, `--include-inactive`, `--init-schema` 옵션을 지원한다. `common build-daily`, `common coverage-report`, `common readiness-report`도 inactive 후보 검증용 `--include-inactive`를 지원한다. inactive row는 broad collection/build/report를 막기 위해 `--include-inactive`와 explicit `--series` 또는 `--feature-codes`를 함께 요구한다.

### 12.4 검증 완료

아래 정적/단위 검증은 모두 로컬 unit/mock 기반이다.

```bash
uv run --extra dev pytest tests/unit
uv run --extra dev ruff check src tests
uv run python -m compileall src/krx_collector
```

마지막 확인 결과:

```text
tests/unit: 279 passed
ruff: passed
compileall: passed
```

### 12.5 smoke run 결과

제한된 API 호출로 로컬 DB에서 아래 경로를 확인했다.

1. seed:
   - PR 4-A smoke 당시: `common_feature_series=8`, `common_feature_catalog=12`, `common_feature_catalog_input=12`
   - 현재 seed 정의: active series 26개 + inactive 후보 7개, active feature 37개 + inactive 후보 17개
2. FDR 단일일 smoke:
   - `US500`, `VIX`, `USD/KRW`, `CL=F` 정상 적재 확인
   - `market_kospi` pykrx는 현재 네트워크에서 KRX auth/JSON parse 실패로 `partial`
3. FDR 확장 smoke(PR 4-A):
   - `global_sp500`, `global_vix`, `fx_usdkrw`
   - 범위: `2024-01-02..2024-01-12`
   - provider 요청 3회, raw upsert 24 rows, sync status `success`
   - build 범위: `2024-01-03..2024-01-15`
   - daily facts 36 rows, build status `success`, PIT 위반 0
4. coverage 결과:
   - `global_vix_level`: coverage `1.0000`, PIT 위반 0
   - `fx_usdkrw_level`: coverage `1.0000`, PIT 위반 0
   - `global_sp500_ret_1d`: coverage `0.8889`, PIT 위반 0
   - `fx_usdkrw_ret_5d`: coverage `0.5556`, PIT 위반 0
5. ECOS smoke(PR 4-E):
   - `ECOS_API_KEY` 로딩 확인: present
   - seed upsert: series 10, catalog 14
   - series: `rate_kr_gov3y`
   - 범위: `2024-01-02..2024-01-05`
   - provider 요청 1회, raw upsert 4 rows, sync status `success`
   - `TIME` -> `observation_date`/`period_end_date` 직접 매핑 확인
   - 단위: `UNIT_NAME=연%`
   - `available_from_date`: `next_krx_session` 정책 확인. `2024-01-05` row는 `2024-01-08`부터 사용 가능
6. ECOS 확장 smoke(PR 4-F):
   - `rate_kr_gov3y`: `2024-01-02..2024-01-31`, provider 요청 1회, raw upsert 22 rows, Mon-Fri 22/22, 결측 0, sync status `success`
   - `rate_kr_gov3y` value range: `3.191..3.313`, unit `연%`
   - `macro_cpi`: `2024-01-01..2024-03-31`, provider 요청 1회, raw upsert 3 rows, sync status `success`
   - `macro_cpi` rows: `202401=113.17`, `202402=113.78`, `202403=113.95`
   - `macro_cpi` unit: `2020=100`
   - `manual_lag_days=20` 및 다음 KRX session 보정 확인: `2024-03-31` 관측값은 `2024-04-22`부터 사용 가능
7. ECOS inactive daily fact/coverage 검증(PR 4-G):
   - `rate_kr_gov3y_level`: build `2024-01-03..2024-01-31`, target 21일, facts 21, null 0, coverage `1.0000`, PIT 위반 0
   - `macro_cpi_level`: build `2024-02-20..2024-04-30`, target 49일, facts 49, null 0, coverage `1.0000`, PIT 위반 0
   - `macro_cpi_level` as-of 전환 확인: `2024-03-19=113.17`, `2024-03-20=113.78`, `2024-04-19=113.78`, `2024-04-22=113.95`
8. ECOS active readiness 리포트(PR 4-H):
   - 기본 readiness 기준: required coverage `1.0000`, null 0, missing 0, PIT 위반 0
   - `rate_kr_gov3y_level`: `2024-01-03..2024-01-31`, target 21일, ready `true`, blockers 없음
   - `macro_cpi_level`: `2024-02-20..2024-04-30`, target 49일, ready `true`, blockers 없음
   - 위 결과는 daily fact의 기계적 품질 판정이다. `macro_cpi_level`은 공식 release calendar 정책이 확정되기 전까지 active 전환하지 않는다.
9. `rate_kr_gov3y` 운영 범위 확대 검증(PR 4-I):
   - 3개월 raw sync: `2026-03-09..2026-06-09`, provider 요청 1회, raw upsert 63 rows, sync status `success`
   - 3개월 build: `2026-03-10..2026-06-09`(첫 관측일 다음 KRX 영업일부터), target 66일, facts 66, null 0, coverage `1.0000`, PIT 위반 0, readiness `true`
   - 12개월 raw sync: `2025-06-06..2026-06-09`(ECOS 반환 범위 `2025-06-09..2026-06-09`), provider 요청 1회, raw upsert 246 rows, value range `2.385..3.940`, unit `연%`, 결측 0, sync status `success`
   - 12개월 build: `2025-06-10..2026-06-09`, target 254일, facts 254, null 0, coverage `1.0000`, PIT 위반 0, readiness `true`
   - 두 범위 모두 통과 기준(coverage `1.0000`, null 0, missing 0, PIT 위반 0, readiness blockers 없음) 충족. catalog `active=false`는 유지(전환은 별도 PR)
10. ECOS 10Y/spread inactive smoke:
   - metadata: ECOS `StatisticItemList`에서 `817Y002`/`010210000`=`국고채(10년)` 확인
   - seed 재적용: `common_feature_series=11`, `common_feature_catalog=16`
   - raw sync: `rate_kr_gov10y,rate_kr_gov3y`, `2024-01-02..2024-01-31`, provider 요청 1회, existing coverage skip 1회, `rate_kr_gov10y` raw upsert 22 rows
   - spread build: `rate_kr_term_spread_10y_3y`, `2024-01-03..2024-01-31`, target 21일, facts 21, null 0
   - coverage/readiness: coverage `1.0000`, missing 0, PIT 위반 0, readiness `true`
11. ECOS 10Y/spread 운영 범위 확대 검증:
   - 3개월 raw sync: `rate_kr_gov10y,rate_kr_gov3y`, `2026-03-09..2026-06-10`, provider 요청 2회, raw upsert 128 rows, sync status `success`
   - 3개월 build: `rate_kr_term_spread_10y_3y`, `2026-03-10..2026-06-11`, target 68일, facts 68, null 0, coverage `1.0000`, PIT 위반 0, readiness `true`
   - 12개월 raw sync: `rate_kr_gov10y,rate_kr_gov3y`, `2025-06-09..2026-06-10`, provider 요청 2회, raw upsert 494 rows, sync status `success`
   - 12개월 build: `rate_kr_term_spread_10y_3y`, `2025-06-10..2026-06-11`, target 256일, facts 256, null 0, coverage `1.0000`, PIT 위반 0, readiness `true`
   - 두 범위 모두 통과 기준(coverage `1.0000`, null 0, missing 0, PIT 위반 0, readiness blockers 없음) 충족. 이후 catalog active 전환 완료
12. ECOS 10Y/spread active-only 검증:
   - seed 재적용: `common_feature_series=11`, `common_feature_catalog=16`
   - active-only sync: `rate_kr_gov10y,rate_kr_gov3y`, `2025-06-09..2026-06-10`, provider 요청 2회, raw upsert 494 rows
   - active-only build: `rate_kr_gov10y_level,rate_kr_term_spread_10y_3y`, `2025-06-10..2026-06-11`, target 256일, features 2, facts 512, null 0
   - coverage/readiness: 두 feature 모두 coverage `1.0000`, missing 0, PIT 위반 0, readiness `true`
   - active-only sync는 raw count 기반 skip 조건을 만족하지 못해 ECOS 요청 2회를 수행했다. build coverage 실패가 아니라 raw 일간 observation count와 KRX target day count의 차이 때문이다.
13. Next-B ECOS USD/KRW 공식 source code 확인 및 inactive seed:
   - metadata: ECOS `StatisticItemList`에서 `731Y001`/`0000001`=`원/미국달러(매매기준율)`, `START_TIME=19640504`, `UNIT_NAME=원` 확인
   - seed: `fx_usdkrw_ecos` source series와 `fx_usdkrw_ecos_level`/`fx_usdkrw_ecos_ret_5d` validation feature를 `active=false`로 추가
   - provider smoke(no DB write): `2024-01-02..2024-01-10` 7 rows, `2025-06-09..2026-06-10` 247 rows, unit `원`, 12개월 value range `1352.6..1546.5`
   - 기존 active `fx_usdkrw_level`/`fx_usdkrw_ret_5d`는 검증 전까지 FDR fallback `fx_usdkrw` input을 유지
14. Next-B-2 ECOS USD/KRW DB coverage/readiness 및 active 전환:
   - 단기 validation: raw `2024-01-02..2024-01-31` 22 rows, build `2024-01-10..2024-01-31`, target 16, facts 32, null 0, coverage `1.0000`, PIT 0, readiness true
   - 3개월 validation: raw `2026-03-09..2026-06-10` 64 rows, build `2026-03-17..2026-06-11`, target 63, facts 126, null 0, coverage `1.0000`, PIT 0, readiness true
   - 12개월 validation: raw `2025-06-09..2026-06-10` 247 rows, build `2025-06-17..2026-06-11`, target 251, facts 502, null 0, coverage `1.0000`, PIT 0, readiness true
   - active 전환: `fx_usdkrw_ecos` source를 active로 전환하고 기존 active feature code `fx_usdkrw_level`/`fx_usdkrw_ret_5d` input을 ECOS series로 변경. FDR `fx_usdkrw`는 fallback source로 유지
   - active-only 검증: same 12개월 범위에서 sync `0 attempted / 1 skipped`, build facts 502/null 0, coverage `1.0000`, readiness true, source trace `["fx_usdkrw_ecos"]`

### 12.6 현재 known issue / 보수적 결정

1. pykrx provider는 mock/unit 기준으로는 동작하지만, 현재 네트워크의 실제 smoke에서 KRX auth/JSON parse 실패가 발생했다. 국내 지수 운영 경로는 PR 6 KRX direct provider로 안정화하고, pykrx는 fallback로 둔다.
2. FDR provider는 MVP/연구용 fallback이다. `US500`, `VIX`, `USD/KRW`, `CL=F`는 제한 smoke에서 동작을 확인했지만, 운영 핵심 데이터는 ECOS/FRED/KRX direct로 교체한다.
3. FDR `DataReader`가 end date를 exclusive처럼 처리하는 사례가 있어 upstream query는 `end + 1 day`로 넓히고, 저장은 원래 `start..end`로 필터링한다.
4. daily fact builder transform 지원 범위:
   - **single-input**: `level`, `ret_<N>d`, `change_<N>d`(절대 차분), `vol_<N>d`(최근 N개 1-step 수익률 표본표준편차, ddof=1), `yoy`, `mom`. transform code는 파라메트릭 파싱(`ret_5d`, `change_20d`, `vol_60d` 등)이라 임의 window를 받는다. `yoy`/`mom`은 positional lag이 아니라 캘린더 year-month 정확 매칭으로 계산하며, 직전 기간이 없으면 잘못된 비교 대신 NULL을 낸다.
   - **multi-input**: `spread`(`spread_long - spread_short`), `ratio`(`numerator / denominator`). `CommonFeatureCatalogEntry.input_roles`로 series별 역할을 지정하고, builder가 각 input을 독립적으로 as-of join한다. 어느 input이라도 결측/stale이거나 ratio 분모가 0이면 fact는 NULL이고, `asof_available_date`는 입력들의 max(available_from)이다. 실제 seed에는 `rate_kr_term_spread_10y_3y`가 active feature로 등록되어 있다.
5. 기존 smoke 과정에서 provider date-range 필터 도입 전 저장된 `fx_usdkrw`의 `2024-01-01` raw row가 로컬 DB에 남아 있다. 삭제는 DB mutation이므로 별도 승인 후 처리한다.
6. `rate_kr_gov3y`/`rate_kr_gov3y_level`은 PR 4-J에서 `active=true`로 전환했다(PR 4-I의 3개월/12개월 검증 통과 근거). `macro_cpi`/`macro_cpi_level`/`macro_cpi_yoy_latest`/`macro_cpi_mom_latest`도 conservative `period_end + 20 calendar days` 정책 검증 후 active 전환했다. 공식 release calendar가 준비되면 CPI policy를 교체한다. inactive 후보 수집/빌드/리포트는 `--include-inactive`와 explicit allowlist를 요구한다.
7. ECOS daily 금리 series는 fact coverage가 1.0000이어도 raw observation count가 KRX target day count보다 작을 수 있다. Next-A에서 비거래소 daily source의 relaxed coverage 기준을 추가했지만, 운영 DB active-only 재실행으로 skip 효과를 한 번 더 확인한다.
8. `fx_usdkrw_ecos`는 DB coverage/readiness 통과 후 active 전환했다. 기존 active FX feature code는 유지하되 input만 ECOS official series로 바뀌었다.
9. FRED `rate_us2y`/`rate_us10y`와 `rate_us_term_spread_10y_2y`는 live smoke와 3개월/12개월 readiness 통과 후 active 전환했다. FRED WTI spot은 기존 FDR WTI futures fallback과 의미 차이가 있어 기존 feature를 대체하지 않고 `commodity_wti_spot_ret_20d`로 병행 active 처리했다.

## 13. 남은 작업과 다음 액션

### 13.1 바로 이어서 할 작업

직전까지 완료된 것(아래 §13.3 커밋 참조):

- PR 4-I/4-J: `rate_kr_gov3y`/`rate_kr_gov3y_level` 운영 범위 검증(3개월/12개월) 통과 후 `active=true` 전환.
- transform 확장(single-input): `change_<N>d`/`vol_<N>d`/`yoy`/`mom`.
- transform 확장(multi-input): `spread`/`ratio` + `input_roles`.
- ECOS 10Y/spread active 전환: `rate_kr_gov10y` source와 `rate_kr_gov10y_level`/`rate_kr_term_spread_10y_3y` feature 등록. 2024년 1월 단기와 최근 3개월/12개월 raw sync/build/coverage/readiness 통과 후 active 전환 완료.
- Next-A active sync skip 기준 보정: ECOS/FDR/FRED 등 비거래소 daily source는 strict raw count 부족 시 관측 밀도와 시작/끝 coverage를 확인해 중복 fetch를 줄인다. KRX/PYKRX strict 기준은 유지한다.
- Next-B ECOS USD/KRW 전환: `fx_usdkrw_ecos` 공식 code 확인, validation feature 단기/3개월/12개월 build/coverage/readiness 통과, 기존 active FX feature input을 ECOS로 전환 완료.
- Next-C FRED provider 1차 구현: `common_features_fred` client/provider, `FRED_API_KEY` 설정, CLI `--sources fred`, US2Y/US10Y/WTI inactive seed와 US 10Y-2Y spread 후보 추가. latest observation 저장만 구현했고 live smoke는 `FRED_API_KEY` 필요.
- Next-C-2 FRED live smoke/readiness 및 US rates active 전환: 단기/3개월/12개월 raw sync/build/coverage/readiness 통과 후 `rate_us2y`, `rate_us10y`, `rate_us2y_level`, `rate_us10y_level`, `rate_us_term_spread_10y_2y`를 active 전환. active-only sync는 `0 attempted / 2 skipped`, 12개월 active build/coverage/readiness 통과. `commodity_wti_fred_ret_20d`는 inactive 유지.
- Next-C-3 FRED WTI vs FDR WTI 비교: `commodity_wti_ret_20d`(FDR `CL=F` futures)와 FRED spot ret20을 236일 paired 비교. 상관은 높지만 차이가 유의해 기존 feature는 유지하고 `commodity_wti_spot_ret_20d`를 별도 active feature로 추가. active-only WTI spot build/coverage/readiness 통과.
- Next-D-1 KRX direct provider 1차 구현: `flows_krx.client.KrxMdcClient`를 `adapters/krx_common/client.py`로 공통화하고, `common_features_krx` provider와 CLI `--sources krx`를 추가. `MDCSTAT00301` 기반 `market_kospi_krx`, `market_kosdaq_krx`, `market_kospi200_krx` source와 validation feature를 inactive seed로 추가. DB write 없는 provider-level live smoke는 3개 지수 모두 3 rows 성공. pykrx active 경로는 DB build/readiness 전까지 유지.
- Next-D-2 KRX direct DB readiness 및 active 전환: 로컬 DB seed/sync/build/coverage/readiness와 pykrx fallback paired 비교 완료. KRX direct active feature 6개 모두 corrected target 43일 coverage 1.0000/readiness true. pykrx 대비 max diff 0. KRX direct source를 active로 전환하고 기존 국내 지수 feature input을 KRX direct로 변경. pykrx source는 fallback inactive로 격하. 2026년 현재까지 KRX 휴장일 9개를 `docs/holidays_krx.csv`에 추가하고, 보정 전 생성된 holiday daily fact 165 rows 삭제. 보정 후 active-only KRX sync는 `0 attempted / 3 skipped`.
- Next-D-3a/3b KRX breadth/liquidity inactive slice: `MDCSTAT01501`/`finder_equidx` HTTP 403은 재검증 시 재현되지 않았고, 기존 `KrxMdcClient` warmup/session/header 경로로 정상 응답했다. `MDCSTAT01501` 기반 KOSPI/KOSDAQ 상승/하락/보합 종목 수와 거래대금 8개 source/feature를 `active=false`로 seed했다. 2026-06-10 raw sync(8 rows) -> 2026-06-11 build(8 facts/null 0) -> coverage/readiness(coverage 1.0000, PIT 0, ready true)까지 완료했다.
- Next-D-3c KRX breadth/liquidity active 전환: provider row cache를 추가한 뒤 3개월 raw sync(`2026-03-09..2026-06-10`, 512 rows), daily build(`2026-03-10..2026-06-11`, 512 facts/null 0), coverage/readiness(coverage 1.0000, PIT 0, ready true)를 통과했다. seed를 active로 전환했고 active-only sync는 `0 attempted / 8 skipped`, active-only build/coverage/readiness도 통과했다. VKOSPI는 `finder_equidx`/pykrx menu catalog에서 현물 지수 endpoint를 찾지 못해 후속 조사로 남겼다.
- Next-D-4 KRX 업종지수 inactive slice: `MDCSTAT00301` 기존 provider 경로로 KRX 반도체(`5044`), KOSPI 전기전자(`1013`), KOSPI 금융(`1021`), KOSDAQ 제약(`2066`) source를 `active=false`로 seed했다. level/ret_1d 8개 feature도 inactive로 두고, provider live smoke와 3개월 DB smoke를 통과했다. `ret_1d` 첫 날 warm-up null 4개는 raw 시작일의 직전 관측값이 없어 발생했으며, warm-up 제외 구간(`2026-03-11..2026-06-11`)은 coverage/readiness `1.0000`, PIT 0, ready true다.
- Next-E CPI conservative release policy active 전환: `macro_cpi`의 1차 release policy를 `period_end_date + 20 calendar days`, KRX 비거래일이면 다음 session으로 문서화했다. `macro_cpi_yoy_latest`/`macro_cpi_mom_latest`를 seed로 추가하고, raw `2025-01-01..2026-05-31` 17 rows -> build `2026-02-20..2026-06-11` 225 facts/null 0 -> coverage/readiness 1.0000/PIT 0/ready true를 확인했다. active 전환 후 `--include-inactive` 없이 sync/build/coverage/readiness도 통과했다. `2026-05-31` CPI는 `available_from=2026-06-22`라 `2026-06-11` feature에는 노출되지 않았다.
- Next-E-3 PPI/M2/CSI monthly macro inactive smoke: ECOS metadata에서 `macro_ppi`=`404Y014`/`*AA`, `macro_m2`=`161Y005`/`BBHS00`, `macro_consumer_sentiment`=`511Y002`/`FME`/`99988`를 확인했다. 3개 source와 7개 feature(`level`, PPI/M2 `yoy`/`mom`)는 `active=false`로 seed했다. raw `2025-01-01..2026-05-31` 48 rows -> build `2026-02-20..2026-06-11` 525 facts/null 0 -> coverage/readiness 1.0000/PIT 0/ready true를 확인했다.
- Next-E-4 PPI/M2/CSI monthly macro active 전환: 공식 release calendar를 새로 붙이지 않고 CPI와 같은 conservative `period_end_date + 20 calendar days` 정책으로 active 전환했다. active-only seed 상태는 active series 26/inactive 7, active features 37/inactive 17. active-only sync/build/coverage/readiness는 raw 48 rows, facts 525, null 0, coverage 1.0000, PIT 0, ready true다.

남은 작업은 "원천을 더 붙이기"보다 "운영 가능한 공통 feature layer로 굳히기" 관점에서 재정렬한다. 구현 단위는 아래 순서로 쪼갠다.

1. **Next-F: 운영화와 ML ETL 연계(중간~큼)**
   - 완료: `deploy/prod/bin/common-features-refresh.sh` wrapper 추가. seed -> daily sync -> monthly macro force sync -> build -> coverage -> readiness 순서로 실행한다.
   - 완료: `common readiness-report --fail-on-not-ready` 옵션 추가. not-ready feature 또는 report error가 있으면 exit code `2`로 종료해 Cronicle 실패 알림과 연결할 수 있다.
   - 완료: `docs/operations.md`, `docs/deploy.md`, `.env.example`에 common feature 운영 env와 Cronicle command를 문서화했다.
   - 완료: v0.8.8 릴리스/배포 후 sj2-server에 `common-features-refresh.sh`를 배포하고 Cronicle `sdc_daily_common_features` 이벤트를 등록/활성화했다. 첫 Cronicle run(`jmq9lrbzp03`)은 운영 `.env`의 `FRED_API_KEY`/`ECOS_API_KEY` 누락과 cold-start lookback 부족으로 readiness exit code 2가 발생했다. 운영 `.env`에 두 key를 추가하고 1회성 backfill(`daily=180d`, `macro=620d`, `build=220d`, `readiness=60d`)을 수행해 active 37개 feature readiness 1.0000/PIT 0/ready true를 확인했다. 이후 Cronicle 기본 설정 재실행(`jmqa0c8mn04`)은 exit code 0으로 성공했다.
   - 남음: ML ETL pivot/gold panel 연계.

2. **Next-G: monthly macro release calendar 보강(중간)**
   - CPI/PPI/M2/CSI는 현재 conservative `period_end + 20 calendar days` 정책으로 active다.
   - 공식 발표 calendar 또는 원천별 release date table이 준비되면 monthly macro availability policy를 교체하고 PIT 회귀 테스트를 추가한다.

공통 원칙은 유지한다. 신규 원천/feature는 `active=false`로 seed → raw sync → daily build → coverage/readiness → 별도 active 전환 순서로 진행한다. 단, Next-A는 신규 원천 추가가 아니라 운영 효율 보정이므로 바로 active 경로 테스트를 대상으로 한다.

### 13.2 아직 남은 구현 범위

| 범위 | 재설계된 남은 작업 |
|---|---|
| sync 운영 효율 | 완료: ECOS/FDR/FRED 등 비거래소 daily source의 relaxed existing coverage skip 기준 보정. KRX/PYKRX strict 기준은 유지 |
| source catalog | 신규 source마다 API parameter/license/API key 필요 여부를 `source_catalog_00.md`에 먼저 기록. KRX breadth/liquidity는 active 전환 완료. KRX 업종지수 4개 후보는 inactive smoke 완료. VKOSPI 현물 endpoint는 미확정 |
| ECOS provider 확장 | 완료: 공식 USD/KRW로 FDR FX fallback 대체, 국고채 3Y/10Y 및 10Y-3Y spread active 전환. 완료: CPI/PPI/M2/CSI monthly macro conservative release policy active 전환. 남은 작업은 공식 monthly release calendar 보강 |
| FRED provider | API client/parser, `FRED_API_KEY` 설정, US2Y/US10Y/WTI seed, US term spread feature 완료. US2Y/US10Y/spread와 WTI spot 병행 feature active 전환 완료. vintage/realtime API는 후속 |
| KRX direct provider | 완료: 공통 `krx_common` client, 지수 provider, CLI `--sources krx`, KRX direct 지수 seed/provider-level smoke/DB readiness/active 전환. 완료: `MDCSTAT01501` 기반 KOSPI/KOSDAQ breadth/liquidity active 전환. 완료: `MDCSTAT00301` 기반 업종지수 4개 후보 inactive smoke. 보류: VKOSPI 현물 endpoint 확인 |
| coverage/reporting | raw stale 상세, source trace 상세, outlier/z-score, Markdown/CSV 출력. 운영 알림과 연결 |
| KOFIA/무역/업종 | 고객예탁금/신용융자잔고, 수출입 10일/월간 지표, `stock_industry_classification`, `hs_sector_mapping` |
| 운영화 | 완료: Cronicle용 wrapper, recent-days/months 증분 실행, readiness 실패 exit code, README/operations/deploy 문서, sj2-server 배포, Cronicle `sdc_daily_common_features` 등록/활성화, cold-start backfill, 기본 설정 첫 성공 run 확인 |
| ML ETL 연계 | `common_feature_daily_fact` pivot, gold panel broadcast join, 종목별 interaction 생성 |

이 순서가 중요한 이유는 원천 수집보다 daily alignment가 더 큰 리스크이기 때문이다. PR 3에서 PIT builder와 coverage report가 닫혔으므로, 이제부터 추가 원천은 raw 수집 후 `common build-daily`와 `common coverage-report`로 실제 노출 품질을 확인한다. 발표일이 불명확한 macro series는 conservative policy가 명시되기 전까지 active feature로 모델에 노출하지 않는다.

### 13.3 세션 인계 메모

다른 세션에서 이어받을 때는 이 문서를 기준 문서로 사용한다. ECOS source code와 후보 판단 세부사항은 `docs/dev/20260608_common_features/source_catalog_00.md`를 함께 확인한다.

git 상태(이 작업들은 main에 커밋되어 있다. 무관 변경 — `remote_sync.py`의 `krx_security_flow_raw` 동기화, deploy 스크립트, README, `temp_prompt.txt` 등 — 은 의도적으로 커밋하지 않고 작업 트리에 남겨뒀다):

```text
87b4451 feat(common-features): support multi-input spread/ratio transforms
816ebe8 feat(common-features): add change/vol/yoy/mom daily fact transforms
4427e07 feat(common-features): add market/macro common feature collection (PR 1~4-J)
```

(주의: 공통 피쳐 기능 전체가 `4427e07` 한 커밋으로 처음 들어갔다. 그 이전에는 untracked 상태였다.)

현재 마지막 완료 단위:

- multi-input transform(`spread`/`ratio`) 완료. `CommonFeatureCatalogEntry.input_roles` 추가(빈 값=전부 primary, single-input 무영향), storage write/read에 role 배선, builder에 spread(long-short)/ratio(num/denom) 구현. 각 input 독립 as-of join, 결측/stale/0분모 시 NULL, asof=max(available_from).
- 그 직전 single-input transform 확장(`change_<N>d`/`vol_<N>d`/`yoy`/`mom`) 완료. `yoy`/`mom`은 캘린더 year-month 정확 매칭(직전 기간 결측 시 NULL).
- KRX direct 국내 지수 전환 완료. `MDCSTAT00301` 기반 KOSPI/KOSDAQ/KOSPI200 source는 로컬 DB readiness와 pykrx paired 비교를 통과했고 active 전환됐다. pykrx 국내 지수 source는 fallback inactive.
- Next-D-3a/3b/3c KRX breadth/liquidity active 전환 완료. `MDCSTAT01501`/`finder_equidx` 403은 재검증에서 재현되지 않았다. provider-level smoke, 하루치 DB smoke, 3개월 DB smoke, active-only skip/build/coverage/readiness가 모두 통과했고, KOSPI/KOSDAQ 상승/하락/보합/거래대금 8개 feature는 `active=true`다.
- Next-D-4 KRX 업종지수 inactive slice 완료. KRX 반도체/KOSPI 전기전자/KOSPI 금융/KOSDAQ 제약 4개 source와 level/ret_1d 8개 feature는 `active=false`로 seed했고, 3개월 raw sync/build와 warm-up 제외 coverage/readiness를 통과했다.
- Next-E CPI conservative release policy active 전환 완료. `macro_cpi_level`, `macro_cpi_yoy_latest`, `macro_cpi_mom_latest`는 `active=true`로 seed했고, 2026년 구간 inactive/active-only raw sync/build/coverage/readiness를 통과했다.
- Next-E-3 PPI/M2/CSI monthly macro inactive smoke 완료. `macro_ppi`, `macro_m2`, `macro_consumer_sentiment` source와 `macro_ppi_*`, `macro_m2_*`, `macro_consumer_sentiment_level` 7개 feature는 inactive 상태에서 2026년 구간 raw sync/build/coverage/readiness를 통과했다.
- Next-E-4 PPI/M2/CSI monthly macro active 전환 완료. 7개 feature는 `active=true`로 seed했고, active-only sync/build/coverage/readiness를 통과했다.
- 검증 상태: Next-E-4 active 전환 후 DB smoke는 raw 48 rows, facts 525, null 0, coverage 1.0000, PIT 0, ready true. `env LOG_LEVEL=WARNING uv run pytest tests/unit` = **279 passed**, `env LOG_LEVEL=WARNING uv run ruff check src tests` 통과, `python3 -m compileall src/krx_collector` 통과.
- Next-F 운영화 1차 완료. `common-features-refresh.sh` wrapper와 `readiness-report --fail-on-not-ready`를 추가했고, 운영/env/deploy 문서에 Cronicle command와 lookback env를 반영했다. `bash -n deploy/prod/bin/common-features-refresh.sh`, `env LOG_LEVEL=WARNING uv run pytest tests/unit` = **281 passed**, `env LOG_LEVEL=WARNING uv run ruff check src tests` 통과, `python3 -m compileall src/krx_collector` 통과.
- Next-F 운영 배포/등록 완료. v0.8.8 이미지 build/push 성공 후 sj2-server compose image를 v0.8.8로 갱신하고 `docker compose pull collector`를 완료했다. `common readiness-report --help`에서 `--fail-on-not-ready` 옵션을 운영 이미지에서 확인했다. Cronicle `sdc_daily_common_features` 이벤트는 `enabled=1`, `target=maingrp`, `plugin=shellplug`, `timezone=Asia/Seoul`, `max_children=1`, `timing=false`로 등록했다. 운영 `.env`에 `FRED_API_KEY`/`ECOS_API_KEY`를 추가한 뒤 cold-start backfill을 수행했고, Cronicle 기본 설정 재실행 job `jmqa0c8mn04`는 2026-06-12 06:38:22 KST에 exit code 0으로 완료했다. 최종 readiness는 active 37개 feature 모두 coverage 1.0000, null 0, missing 0, PIT violation 0, ready true다.
- active 상태: KRX direct 국내 지수 feature, KRX breadth/liquidity 8개 feature, ECOS `rate_kr_gov3y`/`rate_kr_gov10y`/10Y-3Y spread, ECOS `fx_usdkrw_ecos` 기반 FX feature, ECOS CPI/PPI/M2/CSI monthly macro feature, FRED `rate_us2y`/`rate_us10y`/10Y-2Y spread, FRED `commodity_wti_spot_ret_20d`가 active. pykrx 국내 지수 source는 fallback inactive. KRX 업종지수 4개 source/8개 feature, `fx_usdkrw_ecos_*` validation alias, `commodity_wti_fred_ret_20d`, KRX direct 지수 validation feature는 inactive 유지.

`rate_kr_gov3y` active 경로 재현 명령(`--include-inactive` 불필요):

```bash
uv run krx-collector common sync --sources ecos --series rate_kr_gov3y \
  --start 2025-06-06 --end 2026-06-09 --rate-limit-seconds 3
uv run krx-collector common build-daily --feature-codes rate_kr_gov3y_level \
  --start 2025-06-10 --end 2026-06-09
uv run krx-collector common coverage-report --feature-codes rate_kr_gov3y_level \
  --start 2025-06-10 --end 2026-06-09
uv run krx-collector common readiness-report --feature-codes rate_kr_gov3y_level \
  --start 2025-06-10 --end 2026-06-09 --required-coverage-ratio 1.0
```

(`rate_kr_gov3y`는 `next_krx_session` 정책이라 fact 시작일을 raw 첫 관측일 다음 KRX 영업일로 둔다. ECOS는 `--start`가 주말이면 반환 범위가 다음 영업일부터 시작한다.)

다음 구현/검증 단위(권장): `common_feature_daily_fact` pivot/gold panel 연계다. 공식 monthly release calendar 보강은 Next-G로 분리한다.
