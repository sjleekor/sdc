# 파이프라인 리팩터링 계획: sj2 = raw 수집 전용, downstream = parquet/DuckDB

작성일: 2026-06-28 / 대상 디렉터리: `docs/dev/20260728_refactor_pipeline/`

> **진행 상황(2026-06-29 갱신):** P1~P3 + P5(코드)·P6(repo) **구현 완료**. 되돌릴 수 없는 운영 작업
> (P4 Cronicle, P5 실제 `DROP TABLE`)과 README 한글판/릴리스만 남음. **전체 구현 상태·남은 작업·
> 이어가기 가이드는 [`01_implementation_status.md`](01_implementation_status.md) 참고.** 이 문서(00)는
> 설계 레퍼런스로 유지한다.

---

## 0. 목표와 결정사항 (TL;DR)

**목표.** `build-daily`, `coverage-report`, `readiness-report`, `metrics normalize` 같은
*compute-only* 단계를 PostgreSQL row-by-row 처리 대신 **parquet → DuckDB** 위에서 돌려
속도를 끌어올리고, 운영 호스트(sj2-server)의 책임을 **raw 데이터 수집으로 한정**한다.

**확정된 결정 (사용자):**

1. **Compute 구현은 DuckDB SQL** — 기존 `service/` orchestrator(Storage 포트 → Postgres)를
   parquet 어댑터로 재사용하지 않고, `research/etl`의 DuckDB SQL 경로로 **재구현**한다.
2. **Downstream(비-raw) 결과를 테이블에 쓰지 않는다** — normalize/build-daily/coverage/readiness의
   산출물은 parquet/DuckDB 마트로만 존재하고 Postgres canonical 테이블로 되돌려 쓰지 않는다.
3. **파생 *fact* + catalog/rule 4개를 양쪽에서 제거** — `stock_metric_fact`/`common_feature_daily_fact`
   (+`operating_*`) 그리고 **compute 만 읽는 catalog/rule 4개**(`metric_catalog`/`metric_mapping_rule`/
   `common_feature_catalog`/`common_feature_catalog_input`)를 sj2·mydb 양쪽에서 드롭한다. 이 4개는 §3.0 코드
   정의 모듈에서 마트가 직접 읽으므로 DB 가 불필요(결정 7). **단 `common_feature_series`는 유지(결정 7).**
4. **raw 브리지는 기존 `db sync-remote` 재사용** — sj2 raw → 로컬 mydb 미러 → 로컬 export(최소 변경, §2.1).
5. **`operating_*` 파일럿 테이블도 함께 드롭** — `operating_source_document`, `operating_metric_fact` (§5).
6. **freshness 게이트를 compute 노드로 이동** — raw 신선도 체크를 레이크 위 DuckDB 체크로 재구현, sj2 에는 두지 않음 (§4).
7. **`common_feature_series` 1개만 레이크로 흘려 OQ3 분기를 제거한다(최소 구조).** 코드 조사 결과
   **수집측(sj2)이 런타임에 읽는 catalog/rule 테이블은 `common_feature_series` 하나뿐**
   (`sync_common_features.py:93`, 수집 driver). 나머지 catalog/rule 4개는 **compute 만** 읽으므로
   수집↔compute "분기"가 애초에 성립하지 않는다 → 레이크로 흘릴 필요 없이 §3.0 코드 모듈에서 마트가 직접 읽는다.
   - `common_feature_series` → 미러(이미 됨) + **parquet export 추가** → 마트·freshness 가 레이크 뷰로 읽음.
     수집과 compute 가 동일 series 행을 보므로 OQ3 드리프트·checksum 게이트 **불필요**(§8.1 OQ3).
   - catalog/rule 4개 → 코드 정의(§3.0)에서 마트가 직접 사용. DB·미러·export 불필요 → 결정 3 으로 드롭.

**이 리팩터의 핵심(가장 무거운 작업).** 현재 `research/etl`은 두 개의 canonical fact 테이블을
*소비*만 한다. 즉 raw 에서 재계산하지 않는다:

- `research/etl/features/fin_pit.py:72` → `FROM stock_metric_fact` (canonical)
- `research/etl/features/common.py:71` → `FROM common_feature_daily_fact` (canonical)

따라서 canonical 테이블을 그냥 드롭하면 두 모듈이 깨진다. 이 계획의 본체는
**`metrics normalize`와 `common build-daily`의 로직을 DuckDB SQL 마트로 포팅**하여
두 fact 를 raw + 룰/카탈로그에서 다시 만들어내는 것이다. 매핑 룰과 feature 카탈로그(catalog/rule 4개)는
이미 Python 코드로 정의돼 있어(§3.0 순수 모듈로 분리) **마트가 코드 정의에서 직접 읽는다.** 단
`common_feature_series`만은 수집측도 읽는 유일한 공유 입력이라 **레이크 뷰로 흘려 마트가 거기서 읽는다**
(결정 7, OQ3 분기 제거). 코드의 `_default_metric_*`/`default_common_feature_catalog`는 catalog/rule 마트
입력 겸 `common seed`의 series seed 입력으로 남는다.

---

## 1. 현재 상태 (조사 결과)

### 1.1 sj2-server Cronicle 토폴로지 (API `get_schedule/v1` 기준, 2026-06-28)

17개 이벤트, 4개의 timed 체인. `chain` 필드는 "이 이벤트 완료 후 실행할 다음 이벤트"다.

| 체인 시작(timing) | 순서 (→ = chain) | 분류 |
|---|---|---|
| **18:30 평일** | `universe sync` → `prices backfill --incremental` → `flows sync` → `common sync krx` | RAW 4 |
| **20:30 평일** | `common sync fdr` (단독) · `common sync fred` (단독) · `common sync ecos-daily` → `ecos-macro` | RAW 4 |
| **23:30 평일** | `common build-daily` → `common coverage-report` → `common readiness-report` | **COMPUTE 3** |
| **04:00 매일** | `dart sync-corp` → `sync-financials` → `sync-share-info` → `sync-xbrl` → `metrics normalize` | RAW 4 + **COMPUTE 1** |

- 모든 이벤트: `max_children=1`, `multiplex=0`, plugin `shellplug`, 래퍼는 `/home/whi/apps/sdc/bin/*.sh`.
- 래퍼는 `docker compose run --rm collector <cmd>` 형태(이미지 `ghcr.io/sjleekor/sdc:v0.8.14`, `profiles: ["manual"]`).
- **sj2 에서 도는 compute 이벤트는 정확히 4개:**
  `sdc_daily_metrics_normalize`, `sdc_daily_common_build`, `sdc_daily_common_coverage`, `sdc_daily_common_readiness`.
- `common-build-daily.sh`는 build 직전에 `ops assert-common-freshness` 게이트를 호출(raw 기반).
- `validate`, `metrics coverage-report` 래퍼는 존재하나 **스케줄에는 없음**(수동 전용).

### 1.2 테이블 분류 (`sql/postgres_ddl.sql` 기준)

| 그룹 | 테이블 | 비고 |
|---|---|---|
| **RAW (수집, Postgres 유지)** | `stock_master`, `stock_master_snapshot`, `stock_master_snapshot_items`, `daily_ohlcv`, `krx_security_flow_raw`, `dart_corp_master`, `dart_financial_statement_raw`, `dart_share_count_raw`, `dart_shareholder_return_raw`, `dart_xbrl_document`, `dart_xbrl_fact_raw`, `common_feature_observation_raw` | 외부 소스에서 수집 |
| **CONFIG (수집 driver, 유지·미러·export) — 결정 7** | `common_feature_series` | **수집측이 런타임에 읽는 유일한 catalog**(`sync_common_features.py:93`). compute 도 읽음 → sj2 잔류 + 미러 + parquet export 로 양쪽이 같은 행을 읽게 함(OQ3 분기 제거) |
| **AUDIT/OPS (유지)** | `ingestion_runs`, `sync_checkpoints` | 감사/재개 커서 |
| **DERIVED(fact) — 드롭 대상** | `stock_metric_fact`, `common_feature_daily_fact` | normalize / build-daily 산출 fact |
| **CATALOG/RULE(코드 정의로 대체) — 드롭 대상** | `metric_catalog`, `metric_mapping_rule`, `common_feature_catalog`, `common_feature_catalog_input` | **compute 만** 읽음(수집측 미참조) → 분기 없음. §3.0 코드 모듈에서 마트가 직접 읽으므로 DB·미러·export 불필요 |
| **OPERATING 파일럿 — 드롭 대상** | `operating_source_document`, `operating_metric_fact` | 스케줄 없음. 비-raw → 함께 드롭(결정사항 5) |

> 분류 근거(코드 조사): `common_feature_series`만 **수집측**(`sync_common_features.py:93`,
> `freshness.py:92`)이 런타임에 읽으므로 수집↔compute 가 공유하는 유일한 설정 → 레이크로 흘려 분기 제거.
> catalog/rule 4개(`metric_catalog`/`metric_mapping_rule`/`common_feature_catalog(_input)`)는
> **compute 만** 읽으므로(수집측 reader 없음) 분기 자체가 없다 → 코드 정의에서 마트가 직접 읽고 DB 는 드롭.

### 1.3 `research/etl` DuckDB 레이어가 읽는 것 (canonical 의존성)

| canonical 테이블 | research/etl 상태 | 영향 |
|---|---|---|
| `stock_metric_fact` | **소비** (`fin_pit.py:72` `FROM stock_metric_fact`) | 드롭 시 `feat_fin_pit` 붕괴 → 마트로 재생성 |
| `common_feature_daily_fact` | **소비** (`common.py:71` `FROM common_feature_daily_fact`) | 드롭 시 `feat_common` 붕괴 → 마트로 재생성 |
| `common_feature_series` | 현재 미참조 → **레이크 뷰로 등록**(결정 7) | 드롭 안 함; 마트·freshness 입력(수집 공유) |
| `metric_catalog` / `metric_mapping_rule` | 미참조 | 드롭; 마트가 §3.0 코드 정의에서 읽음 |
| `common_feature_catalog` / `common_feature_catalog_input` | 미참조 | 드롭; 마트가 §3.0 코드 정의에서 읽음 |

raw 에서 **이미** 재계산 중인 것들(드롭 무관, 그대로 동작):
`dim_trading_calendar`·`label_daily`·`dim_universe_daily` ← `daily_ohlcv`;
`feat_price` ← `daily_ohlcv`; `feat_flow` ← `krx_security_flow_raw`;
`feat_event` ← `dart_share_count_raw`.

### 1.4 이미 존재하는 도구 (재사용)

- **Rust exporter** `tools/raw-parquet-exporter/` — Postgres → parquet(`data_lake/`), manifest/checkpoint.
  - raw export config `config/export_tables.toml`(13 테이블), 래퍼 `bin/raw-parquet-export-all.sh`.
  - canonical export config `config/export_canonical_tables.toml`(5 테이블), 래퍼 `bin/canonical-parquet-export-all.sh`.
    → **결정 7 후:** canonical export 전체 폐기(파생 fact 는 마트가 생성). 대신 **`common_feature_series` 1개**만
    raw export(`export_tables.toml`)에 추가한다. 현재 raw export 에는 `common_feature_observation_raw`만 있고
    `common_feature_series`는 빠져 있다. catalog/rule 4개는 export 하지 않음(마트가 코드 정의에서 읽음).
  - DSN 은 `DB_DSN`(= 로컬 `local_mydb`)에서 읽음. 레이크 레이아웃:
    `data_lake/{raw,canonical}_postgres/snapshot_date=<DATE>/source=local_mydb/<table>/...`.
- **DuckDB compute** `research/etl/` — `lake.py`(parquet 뷰 등록, `hive_partitioning=false` 불변식),
  `config.py`(`RAW_TABLES`/`CANONICAL_TABLES`, `LakeConfig`, snapshot 핀), `mart.py`(materialize+뷰 등록),
  `features/*`, `labels.py`, `universe.py`, `calendar.py`.
- **raw 미러** `db sync-remote` — sj2 raw → 로컬 mydb 증분/전체 미러(`download_data.sh` = `--full-refresh`).
  미러 대상 목록은 `infra/db_postgres/remote_sync.py:PIPELINE_FULL_REFRESH_TABLE_NAMES`(현재 canonical 포함).

---

## 2. 목표 아키텍처

```
┌─ sj2-server (운영) ────────────────┐      ┌─ 로컬 / compute 노드 ─────────────────────────┐
│ RAW 수집만:                        │      │ (1) 미러:  db sync-remote (raw + series)      │
│  universe / prices / flows         │ ───► │ (2) raw export: bin/raw-parquet-export-all.sh │
│  dart(corp/fin/share/xbrl)         │ raw  │      → data_lake/raw_postgres/snapshot_date=…  │
│  common sync(fdr/fred/ecos/krx)    │ only │ (3) DuckDB compute (신규 마트, §3):           │
│ + ingestion_runs / sync_checkpoints│      │      normalize → stock_metric_fact 마트        │
│ + common_feature_series (수집 driver)│+series│     build-daily → common_feature_daily_fact   │
│ + (선택) raw freshness 게이트      │ ───► │  (입력 = raw + series 레이크 뷰 + 코드 catalog)│
│                                    │      │      coverage / readiness = DuckDB 체크        │
│ ✘ 파생 fact / catalog·rule 테이블  │      │ (4) 기존 feat_* / labels / 모델 ETL 그대로     │
│   없음 (series 만 잔류)            │      │ ✘ 파생 fact 를 Postgres 로 되쓰지 않음        │
└────────────────────────────────────┘      └────────────────────────────────────────────────┘
```

핵심 변화:
- **두 canonical fact 는 더 이상 Postgres 에서 export 하지 않는다.** DuckDB compute 가 raw 에서
  직접 만들어 `data_lake/derived_mart/.../{stock_metric_fact,common_feature_daily_fact}/`(경로명 §8.1 OQ2; 또는 마트
  뷰)로 산출한다. 즉 `bin/canonical-parquet-export-all.sh` + `export_canonical_tables.toml`은 폐기.
- 다운스트림 소비자(`fin_pit.py`, `common.py`)는 **뷰 이름만 신규 마트로 갈아끼우면** 거의 무변경.

### 2.1 raw 브리지 (결정: `db sync-remote` 재사용)

**확정.** 기존 `db sync-remote`를 **raw + `common_feature_series`**로 축소 → 로컬 mydb(미러) →
`bin/raw-parquet-export-all.sh`(이미 `local_mydb` 기준) → DuckDB.
미러 대상 목록(`remote_sync.py:PIPELINE_FULL_REFRESH_TABLE_NAMES`)에서 **파생 fact 2개 + catalog/rule 4개**
(총 6개)를 빼면 된다(§5.2): raw 11개 + `common_feature_series` 1개만 잔류. series 는 이미 미러 spec 이
있으므로 미러 자체엔 신규 코드 0 — exporter(§1.4)에 series 1줄 + `research/etl/config.py`(§3.3)에 series 1개
등록만 추가하면 compute 가 레이크에서 읽는다.

후속 최적화(범위 외, 차기 검토): sj2 에서 직접 raw→parquet export 후 parquet 만 rsync 하여
무거운 Postgres 미러 단계를 제거. 본 리팩터에서는 채택하지 않음.

---

## 3. 본체: normalize / build-daily 를 DuckDB SQL 로 포팅

> 두 작업이 이 리팩터의 80%. 산출물은 raw 에서 재계산되는 두 마트이며, 기존 DuckDB
> 소비자가 동일한 의미로 읽도록 만든다. **parity(동치) 검증**(§7.4)이 합격 기준이다.

### 3.0 (선행) 룰·카탈로그 정의를 순수 definition 모듈로 분리

> **이유.** catalog/rule 4개(`metric_catalog`/`metric_mapping_rule`/`common_feature_catalog(_input)`)는
> **수집측이 안 읽는다 → 분기 없음**(§1.2). 그래서 DB·레이크로 흘릴 필요 없이 **마트가 코드 정의를 직접
> import** 한다. 그런데 이 정의가 지금 §5 에서 제거할 service 모듈(`normalize_metrics.py` 등)에 섞여 있어,
> 마트가 그 모듈을 import 하면 "제거 대상에 새 코드가 의존"하는 모순이 생긴다. → **마트 작성 전에**
> 정의를 실행 로직과 분리한다. (`common_feature_series`만은 수집 공유 입력이라 레이크 뷰로 읽음 — §3.2.)

- `_default_metric_catalog()` + `_default_metric_mapping_rules()` → `research/etl/definitions/metric_rules.py`
  같은 **순수 데이터 모듈**로 이동(Storage/외부 의존 0). **마트가 직접 import**(catalog/rule DB 드롭됨).
- `default_common_feature_catalog.py`의 catalog/catalog_input 정의도 동일하게 순수 모듈로 → 마트가 직접 import.
  **series 정의만은** `common seed`(sj2 잔류)가 DB `common_feature_series`를 채우는 입력으로도 계속 쓰인다.
- 이 단계는 동작 변화 없는 리팩터(순수 이동)이며 P1 의 첫 작업으로 선행한다.
- **데이터 흐름(결정 7):**
  - catalog/rule 4개: `코드 정의 ──import──► 마트` (DB·미러·export 경유 없음 — 가장 단순).
  - series: `코드 정의 ──seed──► common_feature_series(sj2) ──sync──► mydb ──export──► 레이크 뷰 ──► 마트`.
    수집(`common sync`)도 같은 series 테이블을 읽으므로 수집↔compute 가 동일 행 → 분기 불가(OQ3 해소).

### 3.1 `metrics normalize` → DuckDB 마트 (`stock_metric_fact` 대체)

현행 로직(`service/normalize_metrics.py`):
- 매핑 룰 우선순위 매칭. 룰은 4개 소스 테이블을 커버:
  - `dart_financial_statement_raw` — `_matches_financial`: `fs_div`/`sj_div`/`account_id`(필요시 `account_nm`) 일치, 값 = `thstrm_amount`(`value_selector`). (`normalize_metrics.py:418`)
  - `dart_share_count_raw` — `_matches_share_count`: `se == row_name`, 값 = `istc_totqy`/`tesstk_co`. (`:428`)
  - `dart_shareholder_return_raw` — `_matches_shareholder_return`: `statement_type`/`row_name`/`stock_knd`(+`metric_code_match`), 값 = `value_numeric`. (`:434`)
  - `dart_xbrl_fact_raw` — `account_id == concept_id`, 값 = `value_numeric`.
- (target = ticker × bsns_year × reprt_code) 별로 metric_code 마다 **priority 번호가 가장 작은**
  매칭 룰이 승리해 하나의 fact 생성.
- `period_type`/`period_end`는 `reprt_code`에서 추론(`_reprt_code_to_period_type:396`, `_infer_period_end:405`):
  `11013→q1/3-31`, `11012→half/6-30`, `11014→q3/9-30`, `11011→annual/12-31`.

룰·카탈로그 source(결정 7): **§3.0 코드 정의 모듈**(`_default_metric_catalog`/`_default_metric_mapping_rules`).
metric_catalog/metric_mapping_rule 은 수집측이 안 읽어 분기가 없으므로 DB·레이크 경유 없이 마트가 직접 import.

**포팅 산출물:** `research/etl/marts/metrics_normalize.py`(신규)
- 입력 뷰: `dart_financial_statement_raw`, `dart_share_count_raw`, `dart_shareholder_return_raw`,
  `dart_xbrl_fact_raw`(+ ticker/market 보강용 `dart_corp_master`/`stock_master`).
- 룰은 §3.0 정의 모듈을 import 해 **DuckDB 인라인 관계**로 구성:
  `(metric_code, source_table, priority, match-cols…, value_selector)` 튜플 → `VALUES` CTE 또는 소형 seed
  parquet 으로 등록. (Postgres `metric_mapping_rule` 불필요 — 드롭 대상.)
- 소스별 `JOIN` + 매칭 조건 → `value_selector` 컬럼 선택 → `period_type`/`period_end` CASE 부여 →
  `(ticker, metric_code, bsns_year, reprt_code)` 파티션에서 `QUALIFY ROW_NUMBER() ORDER BY priority` = 1.
- **출력 스키마 = 기존 `stock_metric_fact` 호환 스키마(권장).** 소비자 `fin_pit.py`가 쓰는 컬럼은
  `ticker, market, metric_code, period_type, period_end, bsns_year, reprt_code, value_numeric` 뿐이지만,
  **P2 행단위 parity 와 profiling/report/debug 를 위해 provenance 컬럼을 유지**한다:
  `corp_code, fs_div, unit, value_text, source_table, source_key, mapping_rule_code`.
  (리뷰 Medium: 소비 컬럼만 남기면 parity 비교·디버깅이 약해진다.) `fetched_at`/`updated_at` 등
  순수 audit 타임스탬프는 생략 가능(비결정적이라 parity 비교에서 제외).
  > `fin_pit.py`는 `available_from = period_end + 90d/45d`(annual/quarterly) PIT 랙을 자체 계산하므로
  > (`fin_pit.py:53-77`), 마트가 disclosure date 를 별도로 들 필요는 없다.
- `mart.py:materialize()`로 `<derived mart root>/stock_metric_fact/`(경로명은 §8.1 OQ2 참고)에 기록하고
  뷰 `stock_metric_fact`로 등록 → `fin_pit.py`의 `smf_view` 기본값 그대로 동작.

검증 포인트: `dart_shareholder_return_raw`의 `metric_code_match`(예: `thstrm`)와 같은 필드매칭,
XBRL 다중 concept_id 의 priority offset, OFS/CFS 동시 룰의 우선순위(10 vs 20)를 SQL 에서 정확히 재현.

### 3.2 `common build-daily` → DuckDB 마트 (`common_feature_daily_fact` 대체)

> **주의(리뷰 High): 이 작업은 단순 as-of + transform 이 아니다.** 현행 구현
> (`build_common_feature_daily_facts.py`)은 아래 요소를 모두 포함하며, 누락 시 P2 parity 가 깨진다.
> SQL 포팅은 이 전부를 1:1 재현해야 한다.

현행 로직의 **정확한** 동작:
- **입력:** `common_feature_catalog`(+`transform_code`, `unit`), `common_feature_catalog_input`(feature→series,
  `role`), `common_feature_series`(`max_stale_business_days`, `default_transform`),
  `common_feature_observation_raw`(PIT 관측: `available_from_date`/`period_end_date`/`observation_date`/
  `release_date`/`vintage`/`raw_id`).
- **per-period latest-vintage 선택** (`_asof_history:576`, `_observation_sort_key:602`): `available_from_date
  <= feature_date` 인 관측 중, **period(=`period_end_date` 우선, 없으면 `observation_date`)별로**
  `(release_date, available_from_date, fetched_at, vintage, raw_id)` 최대인 1건 선택 → period 정렬된 history 구성.
  (단일 "최신 한 건"이 아니라 **period 별 최신 vintage 의 시계열**이 transform 입력이다.)
- **stale 게이트** (`_is_stale:617`): 선택된 current 관측의 `available_from_date`가 feature_date 로부터
  **영업일(KRX) 기준 `max_stale_business_days` 초과**면 `value_numeric=NULL`(단 `asof_available_date`/
  `selected_vintage`/provenance 는 기록). business-day 카운트는 `stale_calendar`(`_build_stale_calendar:632`)에
  `bisect_right`로.
- **transform** (`_transform_value:437`):
  - `level` — current value.
  - `ret_Nd`/`change_Nd` — **positional lag**(history 인덱스 −N). `ret`은 `base==0`이면 NULL.
  - `vol_Nd` — 최근 N개 1-step return 의 **표본표준편차(ddof=1)** = `variance.sqrt()` (`_rolling_return_volatility:533`).
  - `yoy`/`mom` — **calendar-offset**(정확히 12/1개월 전 같은 period; 없으면 NULL) (`_value_at_calendar_offset:501`).
- **multi-input transforms** (`_MULTI_INPUT_TRANSFORMS:247`): `spread`=`spread_long−spread_short`,
  `ratio`=`numerator/denominator`(분모 0 → NULL). 입력 series 를 **role 별**로 받음(`series_by_role`).
- **provenance 산출:** `source_series_ids`, `source_observation_ids`(transform 에 실제 쓰인 base/used 관측의
  `raw_id` 누적, `_trace:571`), `asof_available_date`, `selected_vintage`, `unit`.
- **incremental baseline** (`:110`): `--incremental` 시 feature 별 `get_common_feature_daily_fact_max_dates`로
  시작점을 잡아 누락분만 빌드.

카탈로그·feature source(결정 7): **catalog/catalog_input 은 §3.0 코드 정의 모듈**에서 마트가 직접 import
(수집측 미참조 → 분기 없음, DB 드롭). **`common_feature_series`만 레이크 뷰**에서 읽는다(수집 driver 공유).

**포팅 산출물:** `research/etl/marts/common_build.py`(신규)
- 입력 뷰: `common_feature_observation_raw`(raw 레이크) **+ `common_feature_series` 레이크 뷰**(결정 7).
  `common_feature_catalog`/`common_feature_catalog_input`은 §3.0 코드 정의에서 import → DuckDB 관계로 등록.
  series 는 sj2 가 수집 driver 로 쓰는 바로 그 행을 compute 가 읽으므로 수집↔compute 분기 없음(OQ3 해소).
- KRX 거래일·영업일 카운트는 `research/etl/calendar.py:dim_trading_calendar`(이미 `daily_ohlcv`에서 생성) 재사용
  → stale business-day 게이트에 동일 calendar 사용.
- **재현 체크리스트(전부 필수):** period-latest-vintage 선택 / stale 게이트(NULL 처리 포함) /
  positional vs calendar-offset lag 구분 / vol 의 ddof=1 sqrt / spread·ratio multi-input(role) /
  source_observation_ids 추적 / incremental baseline.
  - 윈도우/표준편차는 DuckDB window 함수(`LAG`, `STDDEV_SAMP`) 또는 list 집계로 표현하되, **분모 0·
    history 부족 시 NULL** 의미를 Python 구현과 정확히 일치시킨다.
- **출력 스키마 = 기존 `common_feature_daily_fact` 호환(권장).** 소비자 `common.py` 필수는
  `feature_date, feature_code, value_numeric, asof_available_date` 뿐이지만, **P2 parity·디버깅 위해**
  `unit, value_text, source_series_ids, source_observation_ids, selected_vintage` 를 유지한다.
- `<derived mart root>/common_feature_daily_fact/`(경로명 §8.1 OQ2)에 기록, 뷰 등록 →
  `common.py`의 `cfdf_view` 기본값 그대로 동작.

### 3.3 소비자 재배선 (등록 경로를 먼저 정리)

> **순서 주의(리뷰 High).** `stock_metric_fact`/`common_feature_daily_fact`는 `fin_pit.py`/`common.py`만
> 읽는 게 아니다. **모델 빌드와 통합 테스트가 `register_views(..., tables=[...])`로 canonical 뷰를
> 직접 요청**한다:
> - `research/models/_01_20_access_return_rank/build_dataset.py:158-165` — feature group 에 따라
>   `lake_tables`에 `stock_metric_fact`/`common_feature_daily_fact`를 append 후 `register_views`.
> - `tests/integration/test_research_fin_pit_smoke.py:29` — `register_views(con, cfg, tables=["daily_ohlcv","stock_metric_fact"])`.
>
> `register_views`는 **명시적으로 요청된 테이블의 parquet 가 없으면 `FileNotFoundError`**(`lake.py:75`),
> `config.table_glob`은 미등록 이름에 `KeyError`(`config.py:137`). 따라서 canonical 을 `CANONICAL_TABLES`에서
> 빼고 마트로 옮기기 **전에**, 이 두 소비 경로가 "마트 빌드 → 뷰 등록"을 거치도록 먼저 정리해야 한다.
> 안 그러면 canonical 제거 직후 모델 ETL/스모크가 즉시 깨진다.

재배선 순서:
0. **`common_feature_series`를 레이크에 등록(결정 7)** — `research/etl/config.py`의 `RAW_TABLES`에
   `common_feature_series` 1개 추가(또는 작은 `CONFIG_TABLES` 그룹), exporter(§1.4)에도 1줄 추가.
   → §3.2 마트의 series 뷰가 실제로 존재하게 되는 선행 조건. catalog/rule 4개는 레이크 등록 없음(코드 import).
1. **derived mart 등록 헬퍼 도입** — 두 fact 를 §3.1/§3.2 마트로 빌드 후 동일 뷰 이름
   (`stock_metric_fact`, `common_feature_daily_fact`)으로 등록하는 단일 경로(예: `lake.py`의
   `register_derived_marts(con, cfg)` 또는 `mart.py` 확장). `hive_partitioning=false` 불변식 유지.
2. **소비 경로 전환** — `build_dataset.py`는 `lake_tables`에서 두 fact 를 빼고 mart 등록 헬퍼를 호출;
   `test_research_fin_pit_smoke.py` 픽스처도 동일하게 마트 빌드 경로로. `fin_pit.py`/`common.py`의
   `smf_view`/`cfdf_view` 기본값은 그대로 → 모듈 내부 변경 최소.
3. **그 다음에** `research/etl/config.py`의 `CANONICAL_TABLES` 전체를 비운다
   (`stock_metric_fact`/`common_feature_daily_fact`는 마트로 이동, catalog/rule 3개는 애초에 미참조였고 이제
   코드 import 라 레이크 등록 불필요). `common_feature_series`만 step 0 에서 raw/config 로 신규 등록됨.
   (1→2 가 끝난 뒤에만.)
- 검증: 전환 후 `pytest tests/integration` + 모델 build 스모크가 통과해야 §5 진행.

### 3.4 오케스트레이션 엔트리포인트

**사용자가 필요 시 수동으로** 돌리는 단일 스크립트 `bin/parquet-compute-all.sh`(신규, §8.1 OQ1):
**raw + series sync 부터** `sync-remote(raw+series)` → `raw-parquet-export-all.sh`(raw+series) → freshness 게이트 →
DuckDB 마트(normalize/build-daily) → coverage/readiness 체크(§4) → (옵션) feat_*/labels 빌드.
snapshot_date 핀 일관 적용. `--from-step`/`--snapshot-date`/`--skip-sync`/`--features` 로 부분 실행.
자동 스케줄러는 두지 않음(raw 수집은 sj2 가 자동, compute 는 on-demand).

---

## 4. coverage / readiness / freshness 게이트 재정의

현재 sj2 에서 운영 게이트로 동작:
- `common readiness-report --fail-on-not-ready` → 미달 시 exit 1 (Cronicle 알람).
- `common coverage-report` → 커버리지 리포트.
- `ops assert-common-freshness` (build 직전) → raw 신선도 게이트.

리팩터 후:
- **coverage / readiness** 는 `common_feature_daily_fact`(드롭됨)에 의존 → **DuckDB compute 쪽으로 이동**.
  `report_common_feature_coverage.py`(거래일 대비 non-null 비율) / `readiness`(임계 비율 + PIT 위반)
  로직을 §3.2 마트 위 DuckDB 쿼리로 재구현. compute 파이프라인(`bin/parquet-compute-all.sh`)의
  마지막 단계로 두고, 미달 시 **non-zero exit + stderr 에러 요약**(대화형 수동 실행, §8.1 OQ1).
- **freshness 게이트(raw 기반)** 는 **compute 노드로 이동한다**(결정사항 6). 현행 `ops
  assert-common-freshness`는 `ingestion_runs` + `common_feature_observation_raw` max date 를 보는데,
  observation_raw 는 raw 레이크로 export 되고 `ingestion_runs` 도 raw 미러 대상이므로(또는 별도 동기),
  레이크 위 DuckDB 체크로 재구현 가능하다. `bin/parquet-compute-all.sh`에서 normalize/build-daily
  **이전 단계**로 두고, raw 신선도 미달 시 non-zero exit → compute 가 stale raw 위에서 도는 것을 차단.
  → sj2 의 `assert-common-freshness` 호출(현 `common-build-daily.sh` 선행 단계)은 제거.
  - 트레이드오프(수용): "수집 실패"도 이제 compute 노드에서 감지된다. 즉 sj2 raw 수집 장애가
    compute 시점까지 드러나지 않을 수 있음 → §8.Q4 의 보완책(ingestion_runs 동기/알람) 참고.

---

## 5. 비-raw 테이블 디커미션 (sj2 + 로컬)

> 결정 7: 드롭 **8개**(파생 fact 2 + catalog/rule 4 + operating 2). **유지는 `common_feature_series` 1개**
> — 수집측이 런타임에 읽는 유일한 catalog 라 레이크로 흘려 compute 와 공유(OQ3 분기 제거).

드롭 대상(양쪽 호스트) — **총 8개**:
- 파생 fact 2: `stock_metric_fact`, `common_feature_daily_fact`(마트가 재생성).
- catalog/rule 4: `metric_catalog`, `metric_mapping_rule`, `common_feature_catalog`,
  `common_feature_catalog_input`(**compute 만** 읽음 → 분기 없음 → §3.0 코드 정의에서 마트가 직접 import).
- operating 2: `operating_metric_fact`, `operating_source_document`(파일럿, 결정사항 5).
- (FK 드롭 순서 주의: `stock_metric_fact`→`metric_catalog`/`metric_mapping_rule`,
  `common_feature_catalog_input`→catalog/series, `common_feature_daily_fact`→catalog,
  `operating_metric_fact`→`operating_source_document`. 자식 fact·input 먼저, 부모 나중.)

**드롭하지 않고 유지 — `common_feature_series` 1개(결정 7).** 수집(`common sync`)이 읽는 driver 이므로 sj2
잔류 + 미러 + **parquet export 추가**(compute 가 레이크 뷰로 읽어 수집과 공유).

코드/구성 변경:
1. `sql/postgres_ddl.sql` — **드롭 8개**의 `CREATE`/인덱스/FK 제거. `common_feature_series` DDL 은 유지.
   (단 `common_feature_catalog_input`→`common_feature_series` FK 는 input 드롭으로 자연 해소.)
2. `infra/db_postgres/remote_sync.py` — `PIPELINE_FULL_REFRESH_TABLE_NAMES`(`:95`)와
   `SYNC_TABLE_DEPENDENCIES`(`:126`), `SYNC_TABLE_SPECS`에서 **미러 대상 6개 제거**(파생 fact 2 + catalog/rule 4;
   `operating_*`는 애초에 미러 대상 아님). **`common_feature_series`는 미러 유지.**
   → 미러가 raw 11개 + `common_feature_series` 1개를 다룸(결정 7, §2.1).
3. `tools/raw-parquet-exporter/config/export_canonical_tables.toml` + `bin/canonical-parquet-export-all.sh`
   — **폐기**(또는 deprecated). 파생 fact lake 는 DuckDB compute 가 생성. 대신 **`common_feature_series`
   1개를 raw export(`export_tables.toml`)에 추가**(§1.4, §3.3 step 0).
4. `research/etl/config.py` — §3.3대로 `CANONICAL_TABLES` 비우고 `common_feature_series`를 raw/config 로 등록.
5. CLI: `metrics normalize`, `common build-daily`, `common coverage-report`, `common readiness-report`,
   `metrics coverage-report`, `operating process-document`, `ops assert-common-freshness`는 Postgres
   경로에서 **제거 또는 deprecate**(compute 는 DuckDB 로 이동). **`common seed`(series seed)·`db init`은 유지**
   (결정 7: series 테이블을 채우는 유일 경로). catalog/rule seed 는 더 이상 DB 를 채울 필요 없으나 **마트가
   import 할 순수 정의 모듈로 보존**(§3.0). — `cli/app.py` DI 정리. `operating_*` 드롭에 맞춰
   `process_operating_document.py`·`adapters/operating_extractors`·관련 storage 메서드도 정리.
6. `infra/db_postgres/repositories.py` — **드롭 8개** 관련 upsert/get 메서드 및 `ports/storage.py` 프로토콜
   항목 정리. **`common_feature_series` 의 seed/get 메서드는 유지**(seed·미러·`common sync`·freshness 가 사용).
7. 드롭 실행 스크립트(멱등): `DROP TABLE IF EXISTS … CASCADE` 마이그레이션 1회(**8개 테이블**).
   **roll-forward 만; 백업 후 진행**(§7.5).

---

## 6. Cronicle 스케줄 변경 (sj2-server)

> 모두 **mutating** 이라 그 시점에 사용자 승인 후 실행. 본 문서는 계획만.

제거(4개 compute 이벤트):
- `sdc_daily_metrics_normalize`
- `sdc_daily_common_build`, `sdc_daily_common_coverage`, `sdc_daily_common_readiness`

체인 재배선:
- **04:00 체인**: `… → sync-xbrl → metrics-normalize` 에서 tail 제거 →
  `sync-xbrl`의 `chain`을 `''`로. (`sdc_daily_opendart_xbrl.chain` 수정.)
- **23:30 체인**: build→coverage→readiness 전체 제거. 23:30 root 자체 삭제.
- 18:30 / 20:30 raw 체인은 변경 없음.
- freshness 게이트는 compute 노드로 이동(결정사항 6, §4)하므로 **sj2 에 신규 알람 이벤트는 만들지 않는다.**

호스트 정리: `bin/metrics-normalize.sh`, `common-build-daily.sh`, `common-coverage-report.sh`,
`common-readiness-check.sh` 래퍼는 더 이상 스케줄에서 호출 안 됨(파일은 남겨도 무방, 혼선 방지 위해
deprecated 주석 권장). sj2 에 신규 래퍼 추가 없음.

> 시점 주의: 현재 23:30 build 는 20:30 common sync 이후 실행. compute 가 로컬로 가면 "raw 가 그날
> 수집 완료된 뒤" 로컬 compute 를 돌리면 됨(예: 익일 새벽 또는 sync-remote 직후 트리거).

---

## 7. 마이그레이션 순서 (단계적·가역적)

각 단계는 이전 단계 검증 후 진행. **A/B 병행** 구간을 둬 parity 확인 전에는 테이블을 드롭하지 않는다.

1. **P1 — DuckDB 마트 구현 (코드만, 운영 무영향).**
   **(1a)** §3.0 정의 모듈 분리(순수 이동, 동작 무변화) → **(1b)** §3.1 `metrics_normalize.py`,
   §3.2 `common_build.py` → **(1c)** §3.3 소비 경로 재배선(`build_dataset.py`/스모크 → 마트 등록).
   단위테스트 추가. 이 단계에서는 canonical 을 아직 `CANONICAL_TABLES`에서 빼지 않는다(병행 비교용).
2. **P2 — Parity 검증(테이블 드롭 전).**
   현재 Postgres `stock_metric_fact`/`common_feature_daily_fact`를 canonical parquet 로 export 한 것과
   DuckDB 마트 산출을 **키별 행단위 비교**. **허용오차 기준을 값 종류별로 분리(리뷰 Low):**
   - **financial normalize (`stock_metric_fact` value_numeric)** — 룰 선택 결과의 값 복사이므로 **exact(diff=0)**.
     단 NUMERIC 스케일 일치 위해 비교 전 양쪽을 동일 Decimal 스케일로 캐스팅.
   - **common derived (`ret/change/vol/yoy/mom/ratio`)** — 기존 구현은 Python `Decimal`(+`Decimal.sqrt`),
     DuckDB 는 `DOUBLE` 로 흐를 수 있어 마지막 비트 차이 발생 가능. → 두 안 중 택1을 P1 에서 확정:
     (a) DuckDB 측을 `DECIMAL` 로 타입 고정해 exact 비교, 또는 (b) 파생값에 한해 **상대오차 tolerance**
     (예: `1e-9`)로 비교. `level`/`spread`/정수 카운트는 exact.
   - provenance 컬럼(`source_observation_ids`, `selected_vintage`, `mapping_rule_code` 등)도 비교에 포함
     (호환 스키마 유지 결정의 효용 — §3.1/§3.2). 순수 audit 타임스탬프는 비교 제외.
   - coverage/readiness 수치도 기존 Postgres 리포트와 일치 확인. 불일치는 §3 재현 버그 → 수정 후 재검.
3. **P3 — compute 오케스트레이션 전환.**
   `bin/parquet-compute-all.sh`로 로컬 compute 확립. feat_*/labels/모델 ETL 이 새 마트 위에서 정상.
4. **P4 — sj2 스케줄에서 compute 제거(§6).** 사용자 승인 → 04:00 tail·23:30 체인 정리. raw 수집만 가동
   상태로 며칠 관찰(ingestion_runs).
5. **P5 — 테이블 디커미션(§5).** 코드 변경 머지 → 로컬 mydb 및 sj2 `krx_data`에서 **8개 테이블** 드롭
   (파생 fact 2 + catalog/rule 4 + `operating_*` 2). `common_feature_series`는 드롭하지 않음(결정 7).
   **드롭 전 백업(완료 조건):**
   - 파생 fact 2개(`stock_metric_fact`/`common_feature_daily_fact`) — 마트가 재생성하므로 canonical parquet
     export(또는 마트 산출) 1회 보관으로 충분.
   - catalog/rule 4개 — 값이 §3.0 코드 정의에 그대로 있으므로 별도 백업 불필요(코드가 곧 백업).
   - **`operating_source_document`/`operating_metric_fact` — 별도 백업 필수(리뷰 Medium).** `operating_source_document`는
     `content_text`+`raw_payload`(원천 문서 성격, `sql/postgres_ddl.sql:595`)를 담아 canonical parquet
     export 에 포함되지 않을 수 있다. → **`pg_dump`(두 테이블) 또는 별도 parquet archive 완료**를 드롭의
     전제 조건으로 둔다. roll-forward.
6. **P6 — 정리.** canonical exporter/래퍼 폐기, 문서(README·`docs/operations.md`·CLAUDE.md) 갱신,
   `sdc-release`로 버전 범프.

### 7.4 합격 기준
- DuckDB 마트 ≡ 기존 canonical(키별 동치, P2 의 값종류별 tolerance 기준 충족).
- `feat_fin_pit`/`feat_common` 산출이 리팩터 전후 동일(스냅샷 핀 고정 비교).
- canonical 제거 후 `pytest tests/integration` + 모델 build 스모크 통과(§3.3).
- sj2 raw 수집 체인 무변경 동작, compute 이벤트 부재.
- coverage/readiness 게이트가 compute 노드에서 정상 알람.
- `operating_*` 백업(pg_dump/archive) 완료 후에만 드롭.

### 7.5 롤백
- P4 까지는 코드/스케줄만 → 역방향 가능.
- P5 이후 테이블 복구가 필요하면 백업 parquet → `db init` + 적재, 또는 pg_dump 복원.

---

## 8. 위험과 열린 질문

- **Q1 (raw 브리지). [결정됨]** `db sync-remote`(raw + `common_feature_series`, 결정 7) → 로컬 export. §2.1.
  잔여: 미러에서 **비-raw 6개**(파생 fact 2 + catalog/rule 4)를 제거하고 `common_feature_series`만 유지하는지
  §5.2 와 일관 확인.
- **Q2 (normalize/build-daily SQL parity). [최대 리스크]** Python 룰 매칭(특히 shareholder_return
  `metric_code_match`, XBRL multi-concept priority, OFS/CFS 우선순위)과 transform(`ret/change/vol_Nd`,
  `yoy/mom`)을 SQL 로 정확히 재현해야 함. P2 행단위 검증으로 가드.
- **Q3 (operating 파일럿). [결정됨]** `operating_*` 함께 드롭(§5). 잔여: `process_operating_document`
  서비스/어댑터/storage 메서드도 정리해 dead code 잔존 방지(§5.5–5.6).
- **Q4 (freshness 게이트 위치). [결정됨]** compute 노드로 이동(§4). **잔여 리스크:** sj2 raw 수집
  장애가 compute 시점까지 안 드러날 수 있음. 보완책 — `ingestion_runs`를 raw 미러 대상에 포함(이미
  raw 그룹)하고, compute 의 freshness 체크가 "최근 성공 run + 레이크 max date"를 함께 보게 해
  수집 실패도 compute 단계에서 빨리 감지. 운영상 "수집/compute 알람 분리"가 필요해지면 차후 sj2
  경량 알람을 재도입할 수 있음(현 결정에선 미포함).
- **카탈로그 위치(결정 7, 최소 구조).** **`common_feature_series` 1개만** 수집·compute 공통 입력이라
  sj2 유지 + 미러 + export → compute 가 레이크 뷰로 같은 series 를 읽어 드리프트 **구조적 제거**(checksum
  게이트 불필요, §8.1 OQ3). 나머지 catalog/rule 4개는 **수집측이 안 읽어 분기 자체가 없으므로** 레이크로
  흘리지 않고 §3.0 코드 정의에서 마트가 직접 import(DB 드롭). 잔여 관리사항: series 정의 변경 시
  **`common seed` 재실행**으로 `common_feature_series`를 갱신(catalog/rule 변경은 코드 머지로 즉시 반영).
- **스냅샷 일관성.** 마트/feat 빌드는 동일 `snapshot_date` 핀에서. raw export 와 compute 사이
  시점 불일치 방지.

### 8.1 해소된 운영 설계 질문

- **OQ1 (compute 노드 실행 주체). [결정]** **자동 스케줄 없음 — 사용자가 필요할 때 단일 스크립트로 수동 실행.**
  `bin/parquet-compute-all.sh`(신규)를 제공하고, 이 스크립트가 **raw + series sync 부터 끝까지 한 번에** 수행:
  `db sync-remote`(raw + `common_feature_series`) → `raw-parquet-export-all.sh`(raw + series) → freshness 게이트 →
  normalize/build-daily 마트 → coverage/readiness → (옵션) feat_*/labels.
  - 사람이 "데이터를 갱신·분석하고 싶을 때" 한 줄로 돌리는 절차이므로 cron/launchd/Cronicle 같은 자동
    스케줄러는 두지 않는다. (raw 수집 자체는 여전히 sj2 가 자동으로 함 — compute 만 on-demand.)
  - 옵션 인자로 부분 실행을 지원: `--from-step <sync|export|freshness|marts|reports|features>`,
    `--snapshot-date <YYYY-MM-DD>`, `--skip-sync`(이미 미러/export 된 스냅샷 재계산),
    `--features`(feat_*/labels 까지). 백필·재현은 `--snapshot-date` 핀으로.
  - **알람 설계:** 게이트 미달 시 스크립트가 **non-zero exit + 사람이 읽는 에러 요약을 stderr 로 출력**
    → 대화형 실행이므로 사용자가 즉시 확인. 별도 notifier 불필요(자동 스케줄이 아니므로).
  - **문서화(완료 조건):** `README.md`(또는 `docs/operations.md`)에 "raw sync → parquet compute" 수동
    실행 절차, 각 step 의 의미, `--from-step`/`--snapshot-date` 사용 예, 게이트 실패 시 대응을 기재.
  - 추후 무인 갱신이 필요해지면 같은 스크립트를 cron/launchd 로 감싸 재도입 가능(현 결정에선 미포함).
- **OQ2 (레이크 경로명). [결정]** `data_lake/canonical_postgres` → **`data_lake/derived_mart`로 리네임**.
  더 이상 Postgres export 산출물이 아니라 DuckDB 파생 마트이므로 이름을 의미에 맞춘다.
  - 변경점: `research/etl/config.py`의 `CANONICAL_LAKE_NAME`/`canonical_root`(및 `raw`와의 대칭 명명),
    스모크의 `cfg.canonical_root` 참조. P1(1c) 재배선과 함께 한 번에 적용해 혼선 최소화.
  - 본 문서의 `<derived mart root>` 표기는 이 경로를 가리킨다.
- **OQ3 (series ↔ code 카탈로그 드리프트). [결정 — 결정 7: 코드 조사로 범위 최소화]**
  **이전 안(checksum 게이트)은 폐기.** OQ3 드리프트는 **수집과 compute 가 같은 개념을 서로 다른 소스로 볼 때**만
  성립한다. 코드 조사 결과 **수집측(sj2)이 런타임에 읽는 catalog 는 `common_feature_series` 하나뿐**
  (`sync_common_features.py:93`, `freshness.py:92`). 나머지 catalog/rule 4개
  (`metric_catalog`/`metric_mapping_rule`/`common_feature_catalog(_input)`)는 **compute 만** 읽는다.
  - **`common_feature_series`(분기 실재) → 분기 제거:** 미러(이미 됨) + **parquet export 추가**로 레이크에
    흘려, compute(build-daily/freshness)가 **sj2 가 수집 driver 로 쓴 바로 그 series 행**을 읽게 한다.
    수집·compute 가 동일 테이블을 읽으므로 어긋남이 구조적으로 불가능 → checksum 게이트 불필요.
    미재시드 상태면 수집·compute 가 **똑같이 옛 series**로 동작 → 부분적용 상태(=OQ3 위험) 없음.
  - **catalog/rule 4개(분기 없음) → 코드 직접 사용:** 수집측이 안 읽으므로 비교 대상이 없어 드리프트 개념
    자체가 성립 안 함. 레이크로 흘릴 필요 없이 §3.0 코드 정의에서 마트가 직접 import(DB 는 §5 로 드롭).
    compute 의 자기 일관성만 있으면 충분.
  - **잔여 운영 규칙(게이트 아님):** series 정의를 바꾸면 **`common seed` 재실행**으로
    `common_feature_series`를 갱신해야 수집·레이크에 반영된다. catalog/rule 변경은 코드 머지로 즉시 반영.
  - **manifest 기록(선택, 진단용):** 마트는 자신이 읽은 series 스냅샷(snapshot_date)과 코드 catalog 버전을
    manifest 에 적어 두면 사후 추적이 쉽다.

---

## 9. 작업 분해 (파일 단위)

신규:
- `research/etl/marts/metrics_normalize.py` — raw → `stock_metric_fact` 마트(§3.1).
- `research/etl/marts/common_build.py` — observation_raw → `common_feature_daily_fact` 마트(§3.2).
- `research/etl/marts/coverage.py` (또는 기존 리포트 로직 이식) — coverage/readiness DuckDB 체크(§4).
- `research/etl/marts/freshness.py` — raw 레이크 신선도 게이트(compute 이동분, §4). `service/freshness.py`
  + `ingestion_runs`/`common_feature_observation_raw` 로직을 레이크 위로 이식.
- `research/etl/definitions/` — §3.0 분리 대상: metric catalog/rules + common catalog/catalog_input **순수
  정의 모듈**. **마트가 직접 import**(catalog/rule). series 정의는 `common seed`의 입력으로도 쓰임(결정 7).
- `bin/parquet-compute-all.sh` — **사용자가 수동 실행하는 단일 compute 스크립트**(§3.4, §8.1 OQ1):
  sync-remote(raw + series) → raw export(raw + series) → freshness 게이트 → normalize/build-daily 마트 →
  coverage/readiness → (옵션) feat_*/labels. `--from-step`/`--snapshot-date`/`--skip-sync`/`--features` 인자
  지원. 미달 시 non-zero exit + stderr 요약. (자동 스케줄러 없음.)
- `sql/migrations/<date>_drop_derived_tables.sql` — **8개 테이블** 드롭(파생 fact 2 + catalog/rule 4 +
  operating 2, §5). `common_feature_series`는 드롭 안 함.

수정:
- `research/etl/config.py` — `CANONICAL_TABLES` 비움(fact 2 마트 이동, catalog/rule 3 코드 import);
  `common_feature_series`를 `RAW_TABLES`(또는 작은 `CONFIG_TABLES`)에 등록(결정 7, §3.3 step 0);
  `CANONICAL_LAKE_NAME`/`canonical_root` → `derived_mart` 리네임(§8.1 OQ2); derived mart 등록 경로(§3.3).
- `service/default_common_feature_catalog.py` — **순수 정의 모듈로 분리**(§3.0). catalog/catalog_input 은 마트가
  import, series 정의는 `common seed`도 사용. 체크섬 게이트는 폐기(결정 7로 불필요).
- `tools/raw-parquet-exporter/config/export_tables.toml` — **`common_feature_series` 1개** export 추가
  (§1.4, 결정 7). catalog/rule 은 추가하지 않음.
- `research/etl/lake.py` — canonical 뷰 → 마트 빌드 호출 + `common_feature_series` 뷰 등록.
- `research/etl/features/fin_pit.py`, `features/common.py` — 뷰 소스 재배선(최소).
- `infra/db_postgres/remote_sync.py` — 미러 대상에서 **비-raw 6개 제거**(fact 2 + catalog/rule 4, §5.2).
  raw 11개 + `common_feature_series` + `ingestion_runs`(freshness 게이트 입력) 유지.
- `infra/db_postgres/repositories.py`, `ports/storage.py` — **드롭 8개** 메서드/프로토콜 정리(§5.6).
  `common_feature_series` seed/get 메서드는 유지.
- `sql/postgres_ddl.sql` — **8개 테이블** DDL 제거(§5.1). `common_feature_series` DDL 유지.
- `cli/app.py` — compute 서브커맨드(normalize/build-daily/coverage/readiness/metrics coverage-report/
  operating process-document/ops assert-common-freshness) 제거/deprecate, DI 정리(§5.5).
- `service/process_operating_document.py` + `adapters/operating_extractors/` — operating 드롭에 맞춰 정리(§5.5).
- 문서: `README.md`, `docs/operations.md`, `docs/architecture.md`, `CLAUDE.md`.
  특히 **`bin/parquet-compute-all.sh` 수동 실행 절차(raw sync → parquet compute)** 를 runbook 으로
  기재 — step 의미, `--from-step`/`--snapshot-date` 사용 예, 게이트 실패 시 대응(§8.1 OQ1 완료 조건).

폐기:
- `bin/canonical-parquet-export-all.sh`, `tools/raw-parquet-exporter/config/export_canonical_tables.toml`,
  `config/canonical.example.toml`(§5.3).
- (deprecate) sj2 래퍼 `metrics-normalize.sh`, `common-build-daily.sh`,
  `common-coverage-report.sh`, `common-readiness-check.sh`(§6).
