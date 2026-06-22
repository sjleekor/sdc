# 피처 프로파일링 자동화 프로그램 최종 통합 계획

본 문서는 [`AUTOMATED_FEATURE_PROFILING_PLAN_A.md`](./AUTOMATED_FEATURE_PROFILING_PLAN_A.md),
[`AUTOMATED_FEATURE_PROFILING_PLAN_B.md`](./AUTOMATED_FEATURE_PROFILING_PLAN_B.md) 두 제안서와,
이를 1차 통합한 [`plan_total_claude.md`](./plan_total_claude.md), [`plan_total_codex.md`](./plan_total_codex.md)
두 문서를 다시 종합한 **단일 실행 계획**이다.

- 작성: 2026-06-19 KST
- 기반 문서: [`PLAN.md`](../table_stat_20260528/PLAN.md), `statistical_profile_*.md` 21개,
  [`feature_profile_summary_for_model_selection.md`](../table_stat_20260528/feature_profile_summary_for_model_selection.md), `sql/postgres_ddl.sql`
- 목표: `PLAN.md` 의 수작업 마크다운 프로파일링을 **카탈로그 기반·1회 명령 재실행·그래프 포함·실행 간 diff 가능한** 프로그램으로 전환한다.
- 대상 DB: 로컬 PostgreSQL `mydb`(`.env DB_DSN`)와 운영 원천 `sj2` 를 모두 지원(`dbq.sh` 의 `local`/`sj2` 개념과 동일).

---

## 0. 문제 정의 (Why)

기존 `PLAN.md` 방식의 한계(두 안 공통 진단):

1. **재현 불가** — 통계치가 사람이 psql 로 돌려 마크다운에 붙여넣은 단일 시점 스냅샷. 수집이 진행되면 매번 손으로 재실행.
2. **시점 정합성 붕괴** — 표마다 집계 시점이 달라 행수·분포·커버리지·NULL 비율이 어긋난다.
3. **stale 가속** — `stock_metric_fact`, DART 재무/XBRL 처럼 백필 후 규모가 바뀐 테이블의 수동 문서가 빠르게 낡고, 자동 감지가 어렵다.
4. **드리프트 추적 불가** — 실행 간 통계 변화를 기계적으로 비교할 수 없다.
5. **long-format 축 미흡** — `metric_code`/`feature_code`/`series_id`/`concept_id` 축이 테이블 단위 문서 하나로는 깊게 안 보인다.
6. **그래프 부재 / 대형 테이블 비용 정책 미고정** — 분포·커버리지 시각 진단이 없고, 수천만 행 테이블의 샘플링/timeout 정책이 코드로 강제되어 있지 않다.

목표: 선언적 카탈로그에 테이블/피처를 추가하면 통계·그래프·아티팩트가 자동 재계산되고, 이전 실행과 diff 가능한 프로그램.
기존 수동 문서는 즉시 폐기하지 않고, 체크리스트와 특화 질문을 실행 가능한 registry/check 로 옮긴 뒤 최신 프로파일을 명령 한 번으로 재생성한다.

---

## 1. 핵심 설계 원칙

### 1.1. 헥사고날 아키텍처 준수 (CLAUDE.md 불변식)

`domain/`·`service/` 는 `adapters/`·`infra/` 를 절대 import 하지 않는다. DB·파일 렌더링·Notebook/HTML 생성은 port 뒤의 구현체로 두고, 와이어링은 `cli/app.py` 합성 루트에서만.
테이블별 analyzer 모듈을 테이블 수만큼 증식시키지 않고, **테이블별 특화 로직은 `domain_checks` 레지스트리(순수 함수 id)로 흡수**한다(§4).

```text
domain/profiling.py                          순수 dataclass/Enum: "무엇을 측정할지"
ports/profiling.py                           Protocol: ProfileQueryRunner, ProfileRenderer
service/profiling/catalog.py                 TableProfileSpec 선언 (테이블당 1개) + domain_checks 매핑
service/profiling/runner.py                  오케스트레이터: 카탈로그 spec → 체크 실행 → ProfileResult 조립
service/profiling/diff.py                    run 간 드리프트 계산
infra/db_postgres/profiling_query_runner.py  CheckKind → SQL 변환 + 실행(+샘플링/timeout/preflight)
adapters/profiling_render/                   notebook/markdown/html/artifact/index/diff 렌더러
```

### 1.2. 선언적 "프로파일 카탈로그" — 기존 metric_catalog 패턴 재사용

이 프로젝트는 이미 `metric_catalog`/`metric_mapping_rule` 로 "무엇을(선언) ↔ 어떻게(매핑)"를 분리한다. 동일 철학 적용.
SQL 을 service 에 박지 않고, **테이블별 프로파일 설정을 선언적 `TableProfileSpec` 1개**로 둔다.

| 필드 | 의미 | 예 (`krx_security_flow_raw`) |
|---|---|---|
| `table` | 테이블명 | `krx_security_flow_raw` |
| `weight` | `full` / `light` | `full` |
| `entity_key` | 엔티티 컬럼 | `ticker` |
| `time_col` | 시간축 | `trade_date` |
| `natural_key` | 자연키(중복검사) | `(trade_date,ticker,market,metric_code,source)` |
| `numeric_cols` | 분위수 대상 | `[value_numeric]` |
| `category_cols` | 분포 대상 | `[market, metric_code, source]` |
| `null_cols` | NULL 비율 대상 | (전 컬럼 기본) |
| `fk_relations` | FK 정합성 | `daily_ohlcv (trade_date,ticker,market)` |
| `drilldown_dim` | long-format 드릴다운 차원 | `metric_code` (7종) |
| `cost_class` | 쿼리 비용 등급 | `cheap` / `expensive` |
| `sampling` | 대형 테이블 샘플 정책 | `TABLESAMPLE SYSTEM (1)` (분위수/Top-N 한정) |
| `domain_checks` | §4 도메인 특화 체크 id 목록 | `[flow_source_dedupe, flow_pit_join_coverage, …]` |

service 는 이 spec 만 보고 표준 체크(C1~C13)를 자동 수행한다. **신규 테이블 = 카탈로그 항목 1개 추가.**
spec 에 해당 컬럼이 없으면 자동 생략(현재 수작업 규칙을 코드가 강제)하고 manifest 에 사유를 기록.

> 카탈로그 형식: **Python dataclass registry** 로 시작(타입체크/리팩터 안전). 비개발자 편집 요구가 생기면 YAML 로더를 추가하되 기본은 Python.

### 1.3. 표준 체크 = PLAN.md §3 의 C1~C13 코드화

`PLAN.md` §3 의 체크리스트를 그대로 `CheckKind` Enum + SQL 빌더로 코드화한다. spec 에 필요한 컬럼이 없으면 해당 체크는 자동 skip 하고 manifest 에 사유를 남긴다.

| 코드 | CheckKind | 코드화된 체크 | 그래프 |
|---|---|---|---|
| C1 | `count_keys_range` | 행수/distinct key/min·max date | 헤더 카드 |
| C2 | `time_distribution` | 연·월·일별 행수·엔티티수 추세 | 라인/area |
| C3 | `category_distribution` | 카테고리 컬럼별 분포 | 막대 / treemap |
| C4 | `null_ratios` | 컬럼별 NULL/빈문자열 비율 | 가로 막대 |
| C5 | `duplicate_groups` | 자연키/PK 중복 | 수치 카드 |
| C6 | `per_entity_distribution` | 엔티티당 행수 분포 | 히스토그램 + box |
| C7 | `entity_time_coverage` | 엔티티×시간 커버리지 | 히트맵 |
| C8 | `numeric_quantiles` | 수치 분위수, zero/negative 비율 | box/violin, log축 히스토그램 |
| C9 | `category_top_n` | 코드/value Top-N | 가로 막대 Top-20 |
| C10 | `ingest_time_trend` | 적재시각/freshness 추세 | 라인 |
| C11 | `unit_scale` | 통화/단위/스케일 분포 | 그룹 막대 |
| C12 | `fk_integrity` | orphan/join 커버리지 | 수치 카드 |
| C13 | `pit_validity` | point-in-time/look-ahead 검증 | 산점/플래그 카드 |

추가로 두 안 공통의 **freshness 체크**(최신 수집일·최신 데이터일·stale 여부)를 `ingestion_runs`/`sync_checkpoints` 연계로 모든 리포트 상단 warning 으로 노출한다.

### 1.4. 대형 테이블 실행 정책

raw row 를 Python 으로 가져오지 않는다. 모든 기본 분석은 DB aggregate SQL 로 수행한다.
`dart_xbrl_fact_raw`(80M), `krx_security_flow_raw`(76M), `dart_financial_statement_raw`(17M) 등:

- **preflight**: `pg_class.reltuples`, 실제 `COUNT(*)`, max date, 인덱스 존재를 먼저 확인.
- 행수·min/max·커버리지 같은 핵심 체크는 **full aggregate** 우선.
- `percentile_cont`, JSONB 파싱, self-join overlap 등 고비용 체크는 `cost_class=expensive` 로 표시.
- `--sample-policy auto` 기본: 고비용 분위수/문자열 Top-N 은 `TABLESAMPLE SYSTEM (1)` 또는 corp/연도 subset 으로 먼저 실행.
- 모든 결과에 `sampled / sample_pct` 메타 자동 표기(현 수작업 표기 규약을 코드가 보장).
- `--query-timeout-sec` 로 쿼리별 timeout. **쿼리 실패는 전체 run 실패가 아니라 해당 섹션 warning** 으로 기록(`ingestion_runs` partial-run 규약과 동일 철학).

---

## 2. 산출물 포맷 & 디렉터리

### 2.1. 포맷 — 두 안 합집합, 역할 분리

Plan A 의 머신판독(diff)과 Plan B 의 Markdown(git-diff/리뷰)을 모두 채택한다. 단일 포맷으로 고정하지 않는다.

| 포맷 | 역할 | 기본 |
|---|---|---|
| **JSON manifest + 요약** | 실행 메타(target, git SHA, run_date, 행수, 샘플비율, lib 버전) + 핵심 metric. **diff/회귀 검사의 기준**. | ✅ |
| **JSON/Parquet artifact** | 행단위 분포/커버리지 집계 원본. diff·외부 분석·후속 피처 카탈로그 입력 재활용. | ✅ |
| **HTML** | 사람이 보는 기본 상세 리포트. Plotly interactive 그래프 포함. 실행된 노트북을 `nbconvert` 로 변환. | ✅ |
| **Jupyter Notebook (.ipynb)** | 재현 가능한 심층 분석. `nbformat` 생성 → `nbclient` 실행 → 그래프 셀 인라인. 사람이 재실행/탐색. | ✅ |
| **Markdown** | git-diff·문서 리뷰용 사람 친화 리포트. `--publish-docs` 대상. | ✅ |
| **PNG/SVG** | Markdown 삽입용 정적 그래프. | 보조 |
| **CSV** | 사람이 바로 볼 작은 표. 대형 집계는 Parquet 우선. | 선택 |

> 그래프 엔진: HTML 은 **Plotly interactive** 우선, Markdown 은 **matplotlib(PNG 정적)**. plotly 셀은 HTML 익스포트 시 인터랙티브로 유지(`--interactive`).
> Notebook 은 `nbformat` 으로 생성하고 `nbclient` 로 실행한다. `papermill` 은 템플릿 기반 파라미터 재실행이 필요해지는 시점에 `analysis` extra 에 추가한다(초기 필수 의존성 아님).

### 2.2. 디렉터리 — docs 누적 금지, publish 분리

대형 산출물(HTML/ipynb/parquet)을 docs 에 직접 누적하지 않는다. 실행별 raw output 은 `reports/` 에 두고,
사람이 검토한 요약만 `docs/features/table_stat_20260528/` 로 publish 한다.

```text
reports/feature_profiles/
  20260619_153000_local/              # <run_id> = <ts>_<target>, 실행 이력 보존(diff 용)
    manifest.json                     # 실행 메타 + 핵심 metric (diff 기준)
    run_summary.md                    # 전체 테이블 상태 요약(행수·결측·stale 한눈에)
    index.html                        # 동일 요약 대시보드
    index.json
    tables/
      daily_ohlcv/
        profile.ipynb
        profile.html
        profile.md
        metrics.json
        data/ { yearly_coverage.parquet, null_rates.csv }
        figures/ { coverage_heatmap.png, value_distribution.html }
      krx_security_flow_raw/
        profile.{ipynb,html,md}
        metrics/                      # 드릴다운(피처별 분리)
          foreign_net_buy_volume.md
          short_selling_value.md
          ...
      common_feature_daily_fact/
        profile.{ipynb,html,md}
        features/                     # active feature_code 별 분리
          market_kospi_ret_20d.md
          ...
    summary/
      model_selection_context.md
      stale_profile_report.md         # 기존 statistical_profile_*.md 작성일·행수 vs 최신 manifest
  latest -> 20260619_153000_local     # 최신 실행 심볼릭(diff 기본 기준)
```

`docs/features/table_stat_20260528/` 에는 `--publish-docs` 로 다음만 반영:
최신 모델 선택 요약(`feature_profile_summary_for_model_selection.md`), 사람이 검토·고정한 테이블별 대표 Markdown, 기존 수동 profile 대비 stale 요약, 최신 실행 manifest 링크/요약.

드릴다운 자동 분리 임계값: `drilldown_dim` 의 distinct 수가 임계(기본 ≥ 5) 이상이면 하위 디렉터리 생성.
대상: `krx_security_flow_raw`(7), `stock_metric_fact`(29), `common_feature_daily_fact`(37), `dart_xbrl_fact_raw`(concept Top-N).

---

## 3. CLI 설계 (`cli/app.py` 합성 루트)

신규 top-level 서브커맨드 **`profile`** 을 추가한다(기존 `db`/`metrics`/`common`/`flows`/`validate` 와 동일 패턴).
`features profile` 보다 top-level `profile` 을 선택한다 — common feature 만이 아니라 OHLCV, DART raw, KRX flow, master, 운영 테이블까지 다루기 때문.

```bash
# 단일 테이블
uv run krx-collector profile table daily_ohlcv --target local

# 전체 카탈로그(가중치별)
uv run krx-collector profile all --target local --weight full,light

# 드릴다운 포함 + 포맷 선택
uv run krx-collector profile table krx_security_flow_raw --drilldown \
  --formats ipynb,md,html,json,parquet --target local

# long-format 피처 필터 / 범위 제한
uv run krx-collector profile table common_feature_daily_fact \
  --feature-codes market_kospi_ret_20d,fx_usdkrw_ret_5d --start 2025-01-01

# 이전 실행 대비 드리프트 리포트
uv run krx-collector profile diff --target local --against latest
uv run krx-collector profile diff \
  --baseline reports/feature_profiles/20260615_090000_local/manifest.json \
  --candidate reports/feature_profiles/20260619_153000_local/manifest.json

# 검토용 문서 publish
uv run krx-collector profile publish --run-id 20260619_153000_local

# sj2 운영 원천 검증 / 로컬 미러 지연 비교
uv run krx-collector profile table daily_ohlcv --target sj2
```

주요 옵션(두 안 옵션 통합):

| 옵션 | 설명 |
|---|---|
| `--target {local,sj2}` | 로컬 미러 또는 운영 원천. 기본 `local`. local=`.env DB_DSN`, sj2=secrets `db_info`(dbq.sh 동일 출처) |
| `--weight full,light` | 가중치별 실행 |
| `--tables` / `--all` | 특정 테이블 / 카탈로그 전체 |
| `--feature-codes` | long-format(`metric_code`/`feature_code`/`series_id`) 필터 |
| `--start`, `--end`, `--years`, `--reprt-codes` | 분석 구간 제한(DART/재무 포함) |
| `--formats` | `ipynb,md,html,json,parquet,csv,png` 중 선택(기본 `ipynb,md,html,json,parquet`) |
| `--drilldown` / `--split-long-features` | 피처별 파일 생성 |
| `--sample-policy {auto,full,sample}`, `--sample-pct`, `--query-timeout-sec` | 대형 테이블 정책 |
| `--out-dir`(기본 `reports/feature_profiles`), `--run-id`/`--run-date`(미지정 시 `YYYYMMDD_HHMMSS_<target>`) | 출력 |
| `--against` / `--compare-run` / `--baseline` `--candidate` | diff 기준 |
| `--publish-docs` | 검토용 요약을 `docs/features/table_stat_20260528/` 로 복사 |
| `--interactive` | 그래프를 plotly 인터랙티브로 |

종료 정책:

- DB 연결 실패는 명확한 실패 exit code 로 종료(기존 partial-run/exit-code 규약 존중).
- 개별 쿼리 timeout/실패는 table section warning 으로 남기고 run 은 partial 로 계속 진행.
- 0행 테이블(`operating_*`)은 `skipped: empty` 또는 `schema-only` 로 기록 후 정상 종료.

---

## 4. 테이블별 최적화 분석 (PLAN.md §4 → `domain_checks` 매핑)

각 테이블의 도메인 특화 항목을 `domain_checks` 체크 id + 노트북 섹션/그래프로 코드화한다.
"피처 특성이 다르다"는 요구를 카탈로그의 테이블별 설정과 전용 체크로 흡수한다(레지스트리 방식 + 테이블별 분석 항목 통합).

| 테이블 | 핵심 domain_checks | 특화 그래프 |
|---|---|---|
| `daily_ohlcv` | OHLC 항등식(`low<=min(o,c) & high>=max(o,c)`), `is_halted=(o=h=l=0)`/`volume=0` 비율, 음수/0 가격, 종목별 상장 span, 수익률·gap return·가격/거래대금 fat-tail | 캔들 정합성 카드, log(close) 히스토그램, 거래대금 box, 거래일 커버리지 히트맵, 시장별 coverage timeline |
| `krx_security_flow_raw` | metric_code 7종 커버리지 비대칭, KRX/PYKRX **source dedupe**(자연키 값충돌), 공매도잔고 2016-06-30 시작, `daily_ohlcv` PIT join 커버리지(연도별), 합계 항등식 **검증 금지** 주석 | metric×연도 히트맵, metric별 value 분포(드릴다운 7), join 커버리지 라인 |
| `dart_xbrl_fact_raw` | **샘플링 필수(auto)**, concept_id/element_id Top-50, unit/decimals 분포, numeric parse 성공률, instant/duration 비율, dimension/context fan-out, `dart_xbrl_document` 고아 fact | concept Top-50 막대, 통화 분포, 연도×corp fact수 히트맵 |
| `dart_xbrl_document` | 보고서유형/연도/분기 분포, `dart_financial_statement_raw.rcept_no` 교집합/차집합, 재공시 횟수 | 보고서유형 막대, 교집합 카드 |
| `dart_financial_statement_raw` | `fs_div=CFS` 단독, `sj_div` 분포, frmtrm/bfefrmtrm NULL률, account_id 표준코드 미사용률, 금액 fat-tail, currency/unit 분포, `stock_metric_fact` 매핑 후보 Top-N | sj_div 막대, NULL 막대, 금액 log 분포 |
| `dart_shareholder_return_raw` | 이벤트유형 분포, `stock_knd` 비표준 50+, value_numeric ~77% NULL(value_text fallback 필요성), 재공시 패턴, 정규화 사전 후보 자동추출 | 이벤트유형 Top-N, NULL률 막대 |
| `dart_share_count_raw` | `se` 139종 비표준, 발행주식총수 시계열 jump 탐지, corp당 변동 빈도 | 변동사유 Top-N, jump 산점 |
| `stock_metric_fact` | metric_code 29 × source_table × bsns_year/reprt_code 커버리지, 종목/연도 커버리지, IS 5종 매핑 희소, 자본잠식, 자연키 중복, core/sparse 분리 | metric별 커버리지 히트맵(드릴다운 29), source_table stacked 막대 |
| `metric_catalog`/`metric_mapping_rule` | active 수, distinct key, 규칙 참조 metric code 유효성, catalog↔fact 불일치, 미사용 XBRL fallback | 경량 표 |
| `dart_corp_master` | 상장/비상장 비율, ticker NULL 97.4%, modify_date 분포, `stock_master` join 일치율, 스냅샷 지연 | 상태 막대, modify_date 라인 |
| `stock_master*` | 시장/상태 분포, 우선주 식별, 스냅샷 간격·추가/제거 수, 부분 스냅샷(2026-05-21 KOSPI만) 경고, PIT universe 재구성 품질 | 스냅샷 종목수 라인, 시장 막대 |
| `common_feature_*` | series source/category/freq active 수, observation 범위·vintage, **PIT 위반(available_from_date/asof_available_date <= feature_date)**, daily fact forward-fill/stale·warm-up·as-of lag, input DAG(catalog_input 1:1/1:N), 카탈로그↔series 고아, daily_ohlcv 거래일 대비 coverage gap | feature별 커버리지 히트맵(드릴다운 37), PIT 위반 카드, stale 구간 라인 |
| `ingestion_runs` | run_type별 success/failed/partial/running, stale running, 평균 소요, 최근 실행 | 상태 stacked 막대, 소요 box |
| `sync_checkpoints` | 채널별 cursor, latest data date vs cursor 차이, stale 여부 | 경량 카드 |
| `operating_*` | 0행 → `skipped: empty`(schema-only). 적재 후 FK 정합성·섹터/문서유형/metric 분포·추출기 성공률·raw snippet 품질(체크 미리 작성, 데이터 없으면 자동 skip) | 적재 후 활성화 |

---

## 5. 모듈별 구현 명세

### 5.1. `domain/profiling.py` (순수)

```python
class CheckKind(StrEnum):
    COUNT_KEYS_RANGE = "count_keys_range"
    TIME_DISTRIBUTION = "time_distribution"
    CATEGORY_DISTRIBUTION = "category_distribution"
    NULL_RATIOS = "null_ratios"
    DUPLICATE_GROUPS = "duplicate_groups"
    PER_ENTITY_DISTRIBUTION = "per_entity_distribution"
    ENTITY_TIME_COVERAGE = "entity_time_coverage"
    NUMERIC_QUANTILES = "numeric_quantiles"
    CATEGORY_TOP_N = "category_top_n"
    INGEST_TIME_TREND = "ingest_time_trend"
    UNIT_SCALE = "unit_scale"
    FK_INTEGRITY = "fk_integrity"
    PIT_VALIDITY = "pit_validity"
    FRESHNESS = "freshness"

class ProfileWeight(StrEnum): FULL = "full"; LIGHT = "light"
class CostClass(StrEnum): CHEAP = "cheap"; EXPENSIVE = "expensive"

@dataclass(frozen=True)
class TableProfileSpec:
    table: str
    weight: ProfileWeight
    entity_key: str | None
    time_col: str | None
    natural_key: tuple[str, ...]
    numeric_cols: tuple[str, ...]
    category_cols: tuple[str, ...]
    null_cols: tuple[str, ...] | None
    fk_relations: tuple[ForeignKeyProfileSpec, ...]
    drilldown_dim: str | None
    cost_class: CostClass
    sampling: SamplingPolicy
    domain_checks: tuple[str, ...]

@dataclass
class CheckResult: kind; rows: list[dict]; sampled: bool; sample_pct; sql; note; warning
@dataclass
class ProfileResult: spec; generated_at; row_count; checks: list[CheckResult]; drilldown: dict[str, list[CheckResult]]
@dataclass
class RunManifest: run_id; target; git_sha; run_date; tables; row_counts; query_ok_fail; sample_policy; artifact_paths; lib_versions
```

### 5.2. `ports/profiling.py` (Protocol)

```python
class ProfileQueryRunner(Protocol):
    def describe_schema(self, table: str) -> list[ColumnInfo]: ...
    def preflight(self, table: str) -> TablePreflight: ...        # reltuples, count, max_date, indexes
    def run_check(self, spec, kind, *, drill_value=None) -> CheckResult: ...

class ProfileRenderer(Protocol):
    def render(self, result: ProfileResult, *, out_dir, formats) -> list[Path]: ...
```

### 5.3. `service/profiling/` (catalog/runner/diff)

- `build_profile(spec, runner) -> ProfileResult`: spec 의 컬럼 존재에 맞춰 적용 가능한 표준 체크 + domain_checks 선택, 드릴다운 차원 distinct 조회 후 임계 초과 시 per-value 체크. **SQL 을 직접 만들지 않는다**(전부 runner 위임) → 헥사고날 순수성 유지.
- `profile_catalog(specs, runner, renderer, ...)`: registry 조회 → 전체 순회 → RunManifest/index 요약 조립.
- `compare_runs(baseline, candidate) -> DriftReport`: 행수·max date·distinct entity·NULL·중복·join coverage·PIT 위반·분위수·feature coverage gap 변화(드리프트 임계 초과 강조).
- 가능하면 기존 `coverage-report`/`readiness` 계산 로직을 재사용 가능한 섹션으로 연결(중복 SQL 방지).

### 5.4. `infra/db_postgres/profiling_query_runner.py`

- `PostgresProfileQueryRunner` — 기존 `get_connection(dsn)` 사용. `CheckKind` → SQL 템플릿 매핑.
- **SQL injection 차단**: 테이블/컬럼 식별자는 카탈로그 출처만 허용하는 화이트리스트 검증, 값은 파라미터 바인딩.
- 대형 테이블: `sampling`/`cost_class` 정책에 따라 분위수/Top-N 만 `TABLESAMPLE`, 행수/커버리지는 본실행. preflight·timeout·실패→warning 처리. 결과에 `sampled/sample_pct` 기록.
- `--target` → DSN 선택: local=`.env DB_DSN`, sj2=secrets `db_info`(dbq.sh 동일 출처).

### 5.5. `adapters/profiling_render/`

- `notebook.py` — nbformat 셀 조립(헤더/스키마/체크별 표+그래프), nbclient 실행. 그래프 matplotlib 기본, plotly 옵션. (papermill 파라미터 실행은 후속 확장)
- `markdown.py` — git-diff 친화 요약 + PNG 삽입(publish 대상).
- `html.py` — nbconvert 로 실행 노트북 → HTML(Plotly interactive 유지).
- `artifacts.py` — JSON(manifest/요약) + Parquet(행단위 분포) via pyarrow.
- `index.py` — 전 테이블 상태 대시보드(run_summary.md / index.html / index.json / manifest.json).
- `diff.py` 렌더 — DriftReport 출력(드리프트 임계 초과 강조) + stale 프로파일 리포트.

### 5.6. 의존성 추가 (`pyproject.toml` `[analysis]` extra)

dev 와 분리한 optional extra — **프로덕션 이미지 미포함**(프로파일링은 분석 도구이지 수집 파이프라인이 아님):

```toml
[project.optional-dependencies]
analysis = [
  "matplotlib>=3.8",
  "plotly>=5.20",
  "nbformat>=5.10",
  "nbclient>=0.10",
  "nbconvert>=7.16",
  "pyarrow>=15",
]
```

`papermill` 은 초기 필수 의존성에 넣지 않는다. Notebook 을 템플릿으로 재실행해야 하는 요구가 생기면 `analysis` extra 에 추가한다.

---

## 6. 검증 전략

1. **기존 수작업 프로파일과 패리티 검증** — `daily_ohlcv` 를 먼저 코드로 생성해, 현재 `statistical_profile_daily_ohlcv.md` 수치(행수, KOSPI/KOSDAQ 비율, zero_open 1.597%, close 분위수 등)와 대조. 설명 가능한 수준의 일치 = 코드 신뢰 확보 후 확장.
2. **단위 테스트(`tests/unit/`, DB 불필요)** — 카탈로그 완전성(public 테이블 누락 없음), SQL 빌더 식별자 화이트리스트, CheckKind→SQL 스냅샷, spec 컬럼 미존재 시 자동 skip, 샘플 정책 결정, long-format split 경로 sanitization, 렌더러 필수 섹션/manifest 생성.
3. **Fixture 테스트** — 작은 in-memory result set 으로 `daily_ohlcv`/`common_feature_daily_fact` 결과 검증.
4. **통합 테스트(`tests/integration/`)** — 기존 규약대로 DB 미도달 시 self-skip. local 소형 테이블(`sync_checkpoints`, `stock_master`, `metric_catalog`) 전체 파이프라인 + `daily_ohlcv` end-to-end smoke.
5. **회귀/멱등성** — 동일 `run_id` 재실행 시 산출물 경로·manifest 구조 안정·diff 0; manifest 비교가 행수 증가·max date 전진·null ratio 악화·중복 증가를 정확히 표시.

---

## 7. Diff 와 품질 게이트

`profile diff` 는 두 run 의 manifest 와 artifact 를 비교한다. 비교 항목:

- row count 증감, max date 전진/후퇴
- distinct entity 수 변화, key duplicate 증가
- NULL ratio 변화, join coverage 악화
- PIT 위반 신규 발생, numeric percentile 급변
- feature/metric coverage gap 변화, stale running ingestion 증가

초기에는 **warning 만 생성**한다. 차단 gate 는 M3 이후 테이블별 임계값을 정한 뒤 적용한다.

---

## 8. 구현 마일스톤 (Plan A M0~M3 + Plan B Phase 통합)

> **구현 상태 (2026-06-19): M0~M3 전부 완료.** 카탈로그 23개 테이블, 표준 체크 C1~C13+freshness,
> 도메인 체크 16종, 5종 포맷 렌더러(ipynb/md/html/json/parquet), `profile table|all|diff|publish` CLI,
> long-format per-value 파일 분리, `operating_*` skip-empty, 단위 40 + 통합 3 테스트.
> 코드: `domain/profiling.py`, `ports/profiling.py`, `service/profiling/{catalog,runner,orchestrate,diff}.py`,
> `infra/db_postgres/{profiling_query_runner,profiling_domain_checks}.py`, `adapters/profiling_render/`,
> `cli/app.py` (`profile` 서브커맨드).

- **M0 — 스캐폴딩 + 레퍼런스(`daily_ohlcv`)** — ✅ 완료
  - domain/ports/service/infra/renderer 골격, `TableProfileSpec`/`RunManifest`, 카탈로그 1개, 표준 체크 C1~C8, 노트북·Markdown·HTML·JSON/Parquet 렌더러, CLI `profile table` 연결.
  - §6.1 패리티 검증 통과까지. 완료 기준: `uv run krx-collector profile table daily_ohlcv --target local` 실행 가능, 핵심 수치가 기존 수동 문서와 설명 가능 수준 일치.
- **M1 — Wave 0 대형/long-format 테이블 + 샘플링 정책** — ✅ 완료
  - `krx_security_flow_raw`, `dart_xbrl_fact_raw`, `dart_financial_statement_raw`, `stock_metric_fact` spec. preflight·TABLESAMPLE·timeout·cost_class·드릴다운(metric_code) 도입, C9~C12 구현.
  - 완료 기준: 대형 테이블 timeout 이 전체 run 을 깨지 않고, metric별 상세 리포트와 sample metadata 가 생성된다. **검증됨**: 70M행 flow 테이블에서 분위수/Top-N 만 1% TABLESAMPLE, timeout→section warning(exit 0), `bsns_year` INT 시간축 타입 인지.
- **M2 — common_feature_* + PIT + 드릴다운 + 그래프/HTML** — ✅ 완료
  - C13 PIT/freshness 체크, feature 드릴다운(37), 커버리지 gap, stale/warm-up/as-of lag, plotly/matplotlib figure, HTML 익스포트, index 대시보드.
  - 완료 기준: active feature 별 coverage·PIT 위반 자동 산출. **검증됨**: `common_feature_daily_fact` PIT 위반 0건(`asof_available_date<=feature_date`), `cf_stale_runs`/`cf_coverage_gap` feature별 산출.
- **M3 — 마스터/카탈로그/운영 경량 + diff + publish + 마감** — ✅ 완료
  - light weight 체크, `profile all`/`profile diff`/`profile publish` 마감, 드리프트·stale 리포트, `operating_*` skip-empty, 전체 단위/통합/회귀 테스트, 문서화.
  - 완료 기준: registry 전체가 한 명령으로 실행, 변화 자동 요약, docs 에는 검토용 산출물만 반영. **검증됨**: 동일 run 재실행 시 diff 0건(멱등성), publish 가 html/ipynb/parquet 제외하고 md+manifest 만 복사.

각 마일스톤 종료 시 `PLAN.md` §7 체크리스트를 "코드 생성본으로 대체됨" 으로 갱신.

---

## 9. 우선순위

1. `daily_ohlcv` — 전체 프로파일러 기준 테이블(패리티 검증), 모델 입력 기본 축, row 증가 잦음.
2. `krx_security_flow_raw` — 모델 입력 핵심, metric별 coverage 와 source dedupe 중요.
3. `common_feature_daily_fact`, `common_feature_observation_raw` — PIT/stale/coverage gap 검증 가치가 가장 큼.
4. `stock_metric_fact` — 다년도 백필 후 stale 가속, metric별 split 필요.
5. DART raw/XBRL — 대형 테이블 sample/fallback 정책 검증 후 적용(M1).
6. master/catalog/operation — lightweight 로 매 run 동반 실행.
7. `operating_*` — 적재 전 schema-only, 적재 후 도메인 analyzer 활성화.

---

## 10. 산출물의 활용 (PLAN.md §8 계승·확장)

- 누적 run 디렉터리 → 모델링 피처 카탈로그(`docs/features/feature_catalog.md`) 1차 입력으로 자동 갱신.
- 아티팩트(JSON/Parquet)는 **데이터 품질 모니터링·드리프트 감지**(`profile diff`)의 재료.
- `common_feature_*` PIT/stale/coverage gap 결과는 종목 패널 결합 시 look-ahead 방지·stale 제한·coverage gate 기준으로 재사용.
- sj2 복구 시 `--target sj2` 동일 코드 실행으로 로컬 미러 지연을 정량 비교.

---

## 11. 완료 기준

- 한 명령으로 대상 테이블의 ipynb·Markdown·HTML·JSON manifest·Parquet 가 생성된다.
- long-format 테이블(`metric_code`/`feature_code`/`series_id`)은 table-level 요약과 feature-level 상세 파일을 모두 만든다.
- `PLAN.md` 공통 체크리스트 C1~C13 이 코드 레지스트리/check 에 매핑된다.
- `local`/`sj2` 실행이 구분되고 manifest 에 DB target·git SHA·실행 시각이 남는다.
- 대형 테이블 쿼리는 timeout/sample/fallback 정책과 결과가 manifest 에 기록되고, 실패는 run 실패가 아니라 섹션 warning 이다.
- `profile diff` 가 coverage·row count·null ratio·max date·PIT 위반 변화를 자동 요약한다.
- `--publish-docs` 는 대형 원본 산출물을 docs 에 복사하지 않고 검토된 요약만 반영한다.
- `daily_ohlcv` 코드 생성본이 기존 수작업 프로파일 수치와 패리티를 만족한다.

---

## 12. 종합 시 확정한 선택 (A vs B)

| 쟁점 | 채택 | 근거 |
|---|---|---|
| 아키텍처 | **A**: 헥사고날 + 선언적 카탈로그 | CLAUDE.md 불변식 준수, 테이블별 analyzer 모듈 증식 방지(B의 특화는 domain_checks 레지스트리로 흡수) |
| 산출 포맷 | **A∪B**: JSON+Parquet+HTML+ipynb(A) **+ Markdown/PNG(B)** | 머신판독 diff(A)와 git-diff 리뷰(B) 모두 필요 |
| 그래프 | HTML=Plotly interactive, Markdown=PNG 정적 | 공유성과 git-diff 친화성 양립 |
| 노트북 실행 | `nbformat`+`nbclient` 기본, `papermill` 은 후속 확장 | 초기 의존성 최소화 |
| 산출 위치 | **B**: `reports/` raw + `--publish-docs` | docs 에 대용량 HTML/ipynb/parquet 누적 방지 |
| CLI 네이밍 | **A**: top-level `profile` | 기존 db/metrics/common/flows/validate 패턴 일치 |
| CLI 옵션 | **A∪B** | A의 diff/drilldown + B의 feature-codes/years/sample-policy/timeout/publish |
| 대형 테이블 정책 | **B 상세를 A spec 에 통합** | preflight·cost_class·timeout·실패→warning 이 더 견고 |
| 카탈로그 형식 | **A**: Python dataclass 시작 | 타입 안전, 필요 시 YAML 로더 추가 |
| 검증 | **A 패리티 + B 회귀/fixture/diff gate** | 신뢰 확보(A) + 변화 추적 정확성(B) |
