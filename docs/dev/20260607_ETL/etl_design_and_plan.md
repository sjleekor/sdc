# ML 피처셋 ETL 아키텍처 및 구현 계획

- 작성일: 2026-06-07
- 대상 저장소: `stock_data_collector`
- 원천 DB: sj2-server PostgreSQL `krx_data` (source of truth), local `mydb`는 수작업 동기화/검증용 mirror
- 목표: 주가 예측 모델 학습/실험에 바로 투입 가능한 시계열 피처셋을 Parquet 기반으로 생성, 증분 갱신, 검증, 버전 관리한다.

## 0. 수집 데이터 상태와 ETL 기준 시점

2026-06-07에 sj2-server DB를 직접 조회한 기준이다. 단, DART 다년 백필은 아직 진행 중이므로 아래 행수는 **중간 스냅샷**이다. ETL의 실제 source snapshot은 현재 백필이 정상 완료된 뒤 다시 고정한다.

| 영역 | 테이블 | 행수 | 기간/연도 범위 | 종목수 | 비고 |
|---|---|---:|---|---:|---|
| 시세 | `daily_ohlcv` | 6,517,317 | 2007-06-05 ~ 2026-05-21 | 2,780 | 2014년 이후 종목 횡단 학습에 적합 |
| 수급 | `krx_security_flow_raw` | 76,222,905 | 2007-06-05 ~ 2026-05-21 | 2,776 | KRX/PYKRX source 중복 제거 필요 |
| DART 재무 raw | `dart_financial_statement_raw` | 8,086,627 | 2021 ~ 2026 | 2,607 | 연결 재무제표 중심 |
| DART XBRL 문서 | `dart_xbrl_document` | 35,986 | 2022 ~ 2025 | 2,606 | XBRL fact와 join key |
| DART XBRL fact | `dart_xbrl_fact_raw` | 44,772,480 | 2022 ~ 2025 | 2,606 | 현재 최대 단일 테이블 |
| DART 주식수 | `dart_share_count_raw` | 114,318 | 2022 ~ 2025 | 2,650 | 수정주가/주식수 피처 후보 |
| DART 주주환원 | `dart_shareholder_return_raw` | 2,844,285 | 2022 ~ 2025 | 2,650 | 배당/자사주 이벤트 후보 |
| 정규화 metric | `stock_metric_fact` | 411,291 | 2022 ~ 2025 | 2,650 | ML 재무 피처 1차 입력 |
| 마스터 | `stock_master` | 2,780 | 최신 snapshot | 2,780 | KOSPI/KOSDAQ universe |
| 마스터 | `dart_corp_master` | 116,503 | 2017-06-30 ~ 2026-04-17 | 3,959 | `corp_code`-`ticker` 매핑 |

현재 실행 중인 백필:

- 실행 로그: `/home/whi/apps/sdc/backfill_2015_2024_resume_20260606.log`
- 범위: `2024`부터 `2015`까지 역순, 보고서 코드 `11011,11012,11013,11014`, `fs_divs=CFS,OFS`
- 2026-06-07 12:39 KST 기준 진행 상태:
  - `2024`, `2023`, `2022`: financials, share-info, XBRL, metrics normalize 완료
  - `2021`: financials 진행 중
  - `2020`~`2015`: 미진행
- 소진/중단 신호(`OpenDART error 020`, `All OpenDART API keys`, `stopped`)는 해당 시점 로그에 없음

### 0.1 백필 완료 후 ETL baseline

현재 백필이 정상 완료되면 ETL 설계의 재무 데이터 기준은 아래처럼 바뀐다.

| 영역 | 완료 후 기대 연도 범위 | ETL 반영 |
|---|---|---|
| `dart_financial_statement_raw` | 2015~2026 | 2015~2024 과거 raw + 2025/2026 기존 최신 raw 유지 |
| `dart_xbrl_document` / `dart_xbrl_fact_raw` | 2015~2025 | XBRL 기반 fallback metric 후보 범위가 2015년까지 확장 |
| `dart_share_count_raw` / `dart_shareholder_return_raw` | 2015~2025 | 주식수/배당/자사주 이벤트 후보 범위가 2015년까지 확장 |
| `stock_metric_fact` | 2015~2025 | 재무 PIT feature의 학습 가능 시작점을 2022년 이후에서 2015년 이후로 앞당김 |

완료 후에는 아래 검증 쿼리로 6개 DART 테이블의 `bsns_year`가 2015~2024를 모두 포함하는지 확인한 뒤, 그 결과를 `dataset_manifest.json`의 source snapshot으로 기록한다.

```bash
.agents/skills/sdc-db/scripts/dbq.sh sj2 "select bsns_year, count(*) from dart_financial_statement_raw group by bsns_year order by bsns_year;"
.agents/skills/sdc-db/scripts/dbq.sh sj2 "select bsns_year, count(*) from dart_xbrl_document group by bsns_year order by bsns_year;"
.agents/skills/sdc-db/scripts/dbq.sh sj2 "select bsns_year, count(*) from dart_xbrl_fact_raw group by bsns_year order by bsns_year;"
.agents/skills/sdc-db/scripts/dbq.sh sj2 "select bsns_year, count(*) from dart_share_count_raw group by bsns_year order by bsns_year;"
.agents/skills/sdc-db/scripts/dbq.sh sj2 "select bsns_year, count(*) from dart_shareholder_return_raw group by bsns_year order by bsns_year;"
.agents/skills/sdc-db/scripts/dbq.sh sj2 "select bsns_year, count(*) from stock_metric_fact group by bsns_year order by bsns_year;"
```

DB 크기는 2026-06-07 12:39 KST 중간 상태에서 `pg_database_size(krx_data)=123 GB`, PGDATA `124G`다. 백필 완료 후 PGDATA는 대략 `170~210GB`, 가장 가능성 높은 범위는 `180~190GB`로 추정한다.

추정 근거: 현재 `dart_xbrl_fact_raw`가 4년치(2022~2025)에 약 4,477만 행으로 단일 최대 테이블이고, 백필 완료 시 2015~2025의 11년치로 약 2.5~2.75배 성장이 예상된다(2015~2021 신규 7개 연도가 평균 행수에 도달한다는 가정, 단 초기 연도는 공시 종목수가 적어 ~0.7배 가중). 같은 가중을 다른 5개 DART 테이블에 적용하면 DART 군집 합계 약 +50~70GB 증가가 예상되고, 비-DART 테이블은 거의 변하지 않는다. 하한 170GB는 초기 연도 행수 보수 추정, 상한 210GB는 평균치+인덱스/toast 오버헤드 추정. 백필 완료 직후 실측치를 manifest seed에 기록해 본 추정과 차이를 검증한다.

테이블 크기 병목은 `dart_xbrl_fact_raw`와 `krx_security_flow_raw`이며, ETL 설계는 최소 200GB 이상 PostgreSQL 원천과 그에 상응하는 Parquet lake 성장을 전제로 한다.

## 1. ETL 파이프라인 아키텍처 개요

### 1.1 설계 원칙

1. **PostgreSQL은 수집 원장, Parquet은 ML 소비 계층**
   - sj2-server PostgreSQL은 raw 수집/정규화의 source of truth로 유지한다.
   - ML 학습은 DB를 직접 스캔하지 않고 Parquet lake의 gold dataset만 읽는다.
   - local DB mirror는 개발/검증 편의용이며, 최종 ETL 기준값은 sj2-server 또는 sj2에서 복사된 동일 snapshot으로 고정한다.

2. **Pandas 회피, Polars/DuckDB 중심**
   - Polars: 종목별 rolling/window feature, lazy execution, multi-core 연산.
   - DuckDB: Parquet scan/join/write, SQL 기반 검증, partition overwrite.
   - PySpark: 단일 머신에서 Polars/DuckDB로 감당하기 어려운 multi-node 규모가 될 때만 2차 선택지로 둔다.
   - Pandas는 작은 설정/카탈로그 확인 정도에만 제한한다.

3. **Point-in-time(PIT) 우선**
   - 모든 피처는 `feature_date` 또는 `trade_date` 시점에 이미 사용 가능했던 값만 사용한다.
   - 재무/공시 피처는 `period_end`가 아니라 `available_at` 기준으로 as-of join한다.
   - `available_at`을 알 수 없는 재무 데이터는 보수적인 공시 가능일 또는 별도 `report_availability` dimension을 구축하기 전까지 ML 입력에 제한적으로 사용한다.

4. **순수 Parquet의 한계를 인정한 partition overwrite**
   - Parquet 자체는 row-level ACID merge를 제공하지 않는다.
   - 따라서 append-only처럼 보이는 신규 일자도 실제로는 "영향받은 파티션을 임시 경로에 재작성한 뒤 원자적으로 교체"하는 방식으로 멱등성을 보장한다.
   - 추후 동시 쓰기/ACID merge가 필요하면 Iceberg/Delta/DuckLake를 검토하되, 1차 구현은 Hive-style Parquet + manifest로 간다.
   - 전환 트리거(아래 중 하나라도 발생하면 Delta로 전환 검토): (a) 학습 잡과 ETL 잡이 같은 dataset에 동시 쓰기/읽기, (b) gold dataset에 schema evolution(컬럼 추가·타입 변경)이 발생, (c) overwrite 후 backup GC 운영 부담이 ETL 운영 시간의 일정 비율을 넘음. Delta는 `deltalake`(rust 기반 Python 패키지)로 Polars/DuckDB와 호환되어 Spark 의존 없이 도입 가능하다.

### 1.2 전체 데이터 흐름

```text
sj2 PostgreSQL krx_data
  ├─ daily_ohlcv
  ├─ krx_security_flow_raw
  ├─ stock_metric_fact
  ├─ dart_*_raw / dart_xbrl_*
  └─ stock_master / dart_corp_master
        │
        │ incremental extract by date/year/watermark
        ▼
Parquet Bronze Zone
  data_lake/bronze/postgres/<table>/v=1/...
        │
        │ canonicalize, dedupe, type normalization, quality gates
        ▼
Parquet Silver Zone
  ├─ price_daily
  ├─ flow_daily_wide
  ├─ financial_metric_pit
  ├─ corporate_action_factor_v0_heuristic   # 2차에서 v1으로 교체
  ├─ shares_pit_daily
  ├─ tradable_universe_pit
  ├─ report_availability
  └─ trading_calendar
        │
        │ PIT joins, rolling features, labels, split index
        ▼
Parquet Gold Zone
  ├─ ml_panel_daily_v1
  ├─ ml_labels_v1
  ├─ split_manifest_v1
  └─ dataset_manifest.json
        │
        ▼
ML experiments
  ├─ LightGBM/CatBoost tabular panel
  ├─ sequence window dataset for TCN/Transformer
  └─ backtest/evaluation jobs
```

### 1.3 Layer별 책임

#### Bronze: DB snapshot/extract

목표는 PostgreSQL 대형 테이블을 ML 변환과 분리해 재사용 가능한 Parquet 원천으로 저장하는 것이다.

- 입력: sj2 PostgreSQL 또는 local mirror PostgreSQL
- 출력 예:
  - `data_lake/bronze/postgres/daily_ohlcv/v=1/year=2026/month=05/part-*.parquet`
  - `data_lake/bronze/postgres/krx_security_flow_raw/v=1/year=2026/month=05/part-*.parquet`
  - `data_lake/bronze/postgres/stock_metric_fact/v=1/bsns_year=2025/part-*.parquet`
- 원칙:
  - DB 테이블의 자연키/원본 컬럼을 최대한 보존한다.
  - `extracted_at`, `source_db`, `source_table`, `extract_run_id` metadata를 추가한다.
  - `raw_payload` JSONB 처리 원칙: bronze에 string/struct 형태로 보존은 하되, **silver 진입 전에 사용 예정인 key를 numeric/categorical 컬럼으로 평탄화한 `*_flat` bronze view를 함께 생성한다.** Parquet에 JSON 원문을 그대로 두면 row-group 압축률이 떨어지고 silver 변환이 매번 JSON 파싱을 반복하기 때문이다. 평탄화 대상 key는 `schema.py`에 명시한다.

#### Silver: canonical feature component

목표는 모델 조인 전에 각 모달리티를 "한 행의 의미가 명확한" 피처 컴포넌트로 정리하는 것이다.

- `price_daily`
  - key: `(trade_date, ticker, market)`
  - canonical OHLCV, 조정가격, 거래정지 플래그, 수익률/변동성 기초 피처
- `flow_daily_wide`
  - key: `(trade_date, ticker, market)`
  - `krx_security_flow_raw` long format을 metric wide format으로 pivot
  - `KRX` source 우선, `PYKRX`는 fallback
- `financial_metric_pit`
  - key: `(ticker, market, metric_code, available_at)`
  - `stock_metric_fact`를 우선 사용하고, 부족 metric은 raw/XBRL fallback 후보로 분리
  - `period_end`, `bsns_year`, `reprt_code`, `rcept_no`, `available_at`을 함께 유지
- `corporate_action_factor_v0_heuristic`
  - key: `(ticker, market, effective_date)`
  - 액면분할/병합/무상증자/배당 조정 factor
  - **1차 구현은 휴리스틱 기반이라 신뢰도가 낮음을 이름에 명시**한다. 2차에서 SEIBRO/거래소 원천 기반 `corporate_action_factor_v1`로 교체하며, gold dataset은 어떤 버전을 사용했는지 manifest에 기록한다.
- `tradable_universe_pit`
  - key: `(trade_date, ticker, market)`
  - PIT universe(특정 거래일에 *그 시점 기준* 상장·거래 가능했던 종목 집합)
  - 입력: `dart_corp_master`(`corp_code`-`ticker` 매핑 이력 116,503행)과 `daily_ohlcv`의 종목별 first/last `trade_date`를 조합해 상장일·상폐일을 복원한다.
  - **survivorship bias 방지의 핵심 산출물**이다. `stock_master`(현재 2,780행)는 *현재* universe만 담고 있어 상폐 종목이 누락되므로, 학습/백테스트 universe는 항상 본 dimension을 통해 결정한다.
- `shares_pit_daily`
  - key: `(trade_date, ticker)`
  - 일별 발행주식수(보통주/우선주/자사주 분리), 액면분할/유증/소각 이벤트로 *일자 단위 step* 변화 반영
  - 입력: `dart_share_count_raw`(분기/반기 보고)와 corporate action 이벤트(분할·무상증자·소각 등 effective_date)
  - market_cap 같은 일별 ratio 피처는 분기 보고 시점 값을 그대로 forward-fill하면 분기 내 자본행위 발생일에 틀려진다. 이벤트 기준으로 발행주식수를 step-adjust한 본 산출물을 silver에 둔다.
- `report_availability`
  - key: `(rcept_no, corp_code)`
  - 컬럼: `rcept_dt`(접수일), `rcept_time`(접수시각, KST), `available_at`(아래 규칙으로 산출), `is_amended`(정정공시 여부), `amends_rcept_no`
  - 다른 모든 PIT join이 본 dimension의 `available_at`을 source of truth로 사용한다.
- `trading_calendar`
  - key: `(trade_date, market)`
  - `docs/holidays_krx.csv` 및 `infra/calendar/trading_days.py`와 일치시킨 KRX 거래일 달력

#### Gold: ML-ready dataset

목표는 학습 코드가 복잡한 DB/원천 조인을 몰라도 되게 만드는 것이다.

- `ml_panel_daily_v1`
  - key: `(trade_date, ticker, market)`
  - feature columns only
  - label은 별도 파일 또는 `label_*` prefix로 분리하고 feature manifest에서 제외한다.
- `ml_labels_v1`
  - key: `(trade_date, ticker, market, horizon)`
  - 예: `fwd_return_1d`, `fwd_return_5d`, `fwd_return_20d`, `fwd_rank_quantile_20d`
- `split_manifest_v1`
  - chronological split index
  - train/valid/test 기간, purge gap, label horizon을 명시한다.
- `dataset_manifest.json`
  - source DB snapshot, 입력 테이블 row count, feature set version, code git SHA, schema hash, partition list, quality result를 기록한다.

### 1.4 권장 파티셔닝

일자와 종목을 모두 고려하되, `(trade_date, ticker)`를 그대로 path partition으로 쓰면 작은 파일이 폭증한다. 2,780개 종목과 수천 거래일의 곱을 직접 파티셔닝하지 않는다.

권장 기본 레이아웃:

```text
data_lake/gold/ml_panel_daily_v1/
  feature_set_version=20260607/
    year=2024/
      month=01/
        ticker_bucket=00/
          part-000.parquet
        ticker_bucket=01/
          part-000.parquet
```

- `year/month`: 시간 기반 train/valid/test와 증분 overwrite에 유리하다.
- `ticker_bucket`: `hash(ticker) % 64` 또는 `% 128`로 고른 파일 크기 유지.
- 파일 내부 sort: `(ticker, trade_date)`.
- row group: 50k~250k rows 수준부터 측정 후 조정.
- compression: `zstd`.

대체 레이아웃:

- 연구가 "단일 종목 긴 sequence" 중심이면 `market/ticker_bucket/year` 순서를 검토한다.
- 매일 전체 종목 cross-section 학습이 주력이면 `year/month/ticker_bucket`이 더 낫다.
- `ticker=<code>` 직접 파티셔닝은 작은 파일 관리 비용 때문에 피한다.

## 2. 권장하는 프로젝트 디렉토리 구조 (Modular Architecture)

기존 `src/krx_collector` 포트/서비스 구조를 유지하되, ML ETL은 수집 파이프라인과 별도 하위 패키지로 둔다.

```text
stock_data_collector/
  docs/
    dev/
      20260607_ETL/
        etl_design_and_plan.md
    features/
      feature_catalog.md                  # 향후 생성
  src/
    krx_collector/
      ml_etl/
        __init__.py
        cli.py                            # `krx-collector ml-etl ...` wiring
        config.py                         # lake path, versions, split config
        constants.py
        db.py                             # read-only source DB connector helpers
        lake.py                           # Parquet paths, temp write, atomic replace
        manifest.py                       # dataset manifest/checkpoint read/write
        planner.py                        # incremental affected partition planner
        schema.py                         # expected schemas and column groups
        extractors/
          __init__.py
          postgres_extract.py             # DB -> bronze parquet
          daily_ohlcv.py
          krx_flows.py
          stock_metrics.py
          dart_reports.py
        transforms/
          __init__.py
          calendar.py
          price_features.py
          flow_features.py
          financial_pit.py
          adjusted_price.py
          labels.py
          panel_builder.py
        quality/
          __init__.py
          checks.py
          leakage.py
          coverage.py
          reports.py
        splits/
          __init__.py
          chronological.py
          walk_forward.py
  tests/
    unit/
      ml_etl/
        test_planner.py
        test_partition_paths.py
        test_price_features.py
        test_flow_dedupe.py
        test_financial_pit_no_leakage.py
        test_chronological_split.py
        test_available_at_cutoffs.py
        test_universe_pit.py
        test_shares_pit_daily.py
    integration/
      ml_etl/
        test_e2e_synthetic_pipeline.py    # 작은 합성 DB → bronze→silver→gold 한 바퀴
        test_atomic_swap_crash_safety.py  # write 도중 SIGKILL 시 final 손상 없음 검증
        test_idempotent_rerun.py          # 동일 입력/코드 재실행 시 동일 manifest/파티션
  data_lake/                              # 로컬 개발용. gitignore 대상.
    bronze/
    silver/
    gold/
    manifests/
```

운영 환경에서는 `data_lake`를 repo 내부가 아니라 별도 디스크 경로에 둔다. repo에는 코드, 문서, 테스트, schema contract만 커밋한다.

### 2.1 모듈 책임

| 모듈 | 책임 |
|---|---|
| `config.py` | lake root, feature set version, split 기간, lookback/horizon 설정 |
| `db.py` | source DB read-only 연결, SQL template 실행, credential 출력 방지 |
| `lake.py` | Parquet 경로 규칙, temp write, partition overwrite |
| `manifest.py` | dataset manifest, watermark, schema hash, run metadata |
| `planner.py` | 증분 실행 시 영향 날짜/파티션 계산 |
| `extractors/*` | PostgreSQL 테이블을 bronze Parquet으로 추출 |
| `transforms/*` | price/flow/financial/label/split 변환 |
| `quality/*` | 중복, 누설, coverage, numeric sanity 검증 |
| `splits/*` | chronological split, walk-forward split 생성 |

## 3. 룩어헤드 방지 및 금융 시계열 규칙

### 3.1 시간 컬럼 표준

모든 silver/gold 데이터는 아래 시간 컬럼의 의미를 구분한다.

| 컬럼 | 의미 | 예 |
|---|---|---|
| `trade_date` | KRX 거래일 | 2026-05-21 |
| `period_end` | 재무제표/보고서 대상 기간 종료일 | 2025-12-31 |
| `available_at` | 모델이 해당 값을 사용할 수 있게 된 시점 | 공시 접수일 다음 거래일 |
| `feature_date` | 모델 입력 row의 기준일 | 보통 `trade_date`와 동일 |
| `label_end_date` | forward label 종료일 | `trade_date + N trading days` |

기본 예측 시점은 **장마감 후 EOD T에 사용 가능한 데이터로 T+N 수익률을 예측**하는 것으로 둔다. 따라서 `daily_ohlcv`의 T일 종가/거래량은 T일 feature에 포함할 수 있다. 만약 장시작 전 예측으로 바꾸면 가격/수급 feature를 전부 1거래일 shift하는 설정을 별도로 둔다.

### 3.2 재무/PIT join

재무 피처는 아래 규칙을 강제한다.

1. `period_end <= trade_date`만으로는 충분하지 않다.
2. `available_at <= trade_date`인 값 중 가장 최신 값만 선택한다.
3. 개정 공시는 개정 공시의 `available_at` 이후 row에만 반영한다.
4. `rcept_no`만 있고 접수일이 명시되지 않은 raw는 `report_availability` dimension을 먼저 만든다.
5. `available_at`을 확정할 수 없는 metric은 gold feature에서 제외하거나 `conservative_available_at`을 명시한다.

#### 3.2.1 `available_at` 결정 규칙

기본 예측 시점(EOD T → T+N 수익률)과 정합되도록 다음 규칙을 단일 source of truth로 둔다.

1. `rcept_dt`(공시 접수일, KST) + `rcept_time`(접수 시각)을 결합해 KST timestamp `rcept_ts`를 만든다.
2. **장중(09:00~15:30) 접수**: 같은 거래일 EOD를 `available_at`으로 둔다. 장중에 정보가 노출되었더라도 본 ETL의 기본 예측 시점이 EOD T이므로 시점이 정합한다.
3. **장후(15:30~18:00) 접수**: 같은 거래일 EOD를 `available_at`으로 둔다. EOD 마감 직후 시장 참가자가 다음 거래일 시초가 결정 전까지 충분히 반영 가능하다는 가정.
4. **18:00 이후 또는 휴장일/주말 접수**: 직전 정의의 컷오프(18:00 KST)를 넘으면 *다음 KRX 거래일* EOD를 `available_at`으로 둔다. `trading_calendar`로 다음 거래일을 산출한다.
5. **`rcept_time`이 없는 raw**: `rcept_dt` 18:00 KST 이후 접수로 보수적으로 가정해 다음 거래일 EOD를 `available_at`으로 둔다.
6. **정정공시(`is_amended=true`)**: 정정공시의 `available_at` 이후 row에만 반영한다. 원본 공시는 정정 시점 직전까지 사용한다(완전 무효화하지 않음, 단 메트릭별로 정책 다를 수 있어 `report_availability`에 정정 사유 코드 보존).
7. 본 컷오프(15:30, 18:00)는 `config.py`의 `available_at_cutoffs`로 노출하며, 실험을 위해 장시작 전 예측 모드를 도입할 때는 컷오프와 함께 1거래일 추가 shift 옵션을 둔다.

Polars as-of join 예시:

```python
financial = financial.sort(["ticker", "metric_code", "available_at"])
panel = panel.sort(["ticker", "trade_date"])

pit = panel.join_asof(
    financial,
    left_on="trade_date",
    right_on="available_at",
    by=["ticker"],
    strategy="backward",
)
```

실제 구현에서는 `metric_code`별 wide pivot 전후 성능을 비교한다. metric 수가 현재 29개 수준이면 wide pivot 후 join이 단순하다.

### 3.3 비거래일 처리

기본 gold dataset은 KRX 거래일만 가진다.

- `drop`: 거래일이 아닌 날짜는 row를 만들지 않는다. 기본값.
- `ffill_static`: 종목명, market, 재무/PIT metric, 최근 공시 metric은 다음 거래일까지 forward fill한다.
- `ffill_price`: 가격 자체를 비거래일에 채우는 옵션은 기본 비활성화한다. sequence 모델이 calendar-day grid를 요구할 때만 `is_trading_day=false`와 함께 별도 dataset으로 만든다.
- 종목별 결측 거래일:
  - 상장 전/상폐 후는 row 없음.
  - 거래정지/volume=0은 row를 유지하고 `is_halted`, `is_zero_volume` flag를 둔다.

### 3.4 수정주가 플로우

현재 `daily_ohlcv`는 raw OHLCV 성격이므로, ML label과 장기 rolling feature는 조정가격 기준을 우선한다.

1차 구현:

1. `dart_share_count_raw`에서 `distb_stock_co`, `istc_totqy`의 큰 변화를 후보 이벤트로 추출한다.
2. `daily_ohlcv`에서 전일 대비 비정상 가격 jump를 탐지한다.
3. 주식수 변화와 가격 jump가 동시에 발생한 경우 `corporate_action_factor` 후보로 기록한다.
4. 사람이 검증 가능한 report를 만든 뒤 `adj_factor`를 확정한다.
5. `adj_open/high/low/close = raw_open/high/low/close * cumulative_adj_factor`를 생성한다.

2차 구현:

- SEIBRO/거래소 corporate action 원천을 추가해 액면분할, 병합, 무상증자, 배당락 정보를 직접 반영한다.
- 배당까지 total-return label에 반영할지, 가격수익률 label만 쓸지 feature set version으로 분리한다.

## 4. 증분 로드 및 멱등성 설계

### 4.1 Watermark

ETL 상태는 DB 테이블이 아니라 별도 manifest/checkpoint로 관리한다.

```json
{
  "dataset": "ml_panel_daily_v1",
  "feature_set_version": "20260607",
  "source_db": "sj2/krx_data",
  "last_complete_trade_date": "2026-05-21",
  "max_lookback_days": 252,
  "max_label_horizon_days": 20,
  "source_watermarks": {
    "daily_ohlcv": {"max_trade_date": "2026-05-21"},
    "krx_security_flow_raw": {"max_trade_date": "2026-05-21"},
    "stock_metric_fact": {"max_updated_at": "..."}
  }
}
```

### 4.2 영향 범위 계산

새 데이터가 `new_min_date..new_max_date`에 들어오면 feature 계산 범위는 아래처럼 확장한다.

```text
effective_max_lookback = max(lookback_per_feature.values())
effective_max_horizon  = max(label_horizons)
compute_start = previous_trading_day(new_min_date, effective_max_lookback)
compute_end   = next_trading_day(new_max_date, effective_max_horizon)
write_start   = new_min_date
write_end     = new_max_date
```

- rolling feature는 과거 lookback이 필요하므로 `compute_start`부터 읽는다.
- label은 미래 horizon이 필요하므로 `compute_end`까지 읽는다.
- 실제 overwrite는 `write_start..write_end`가 포함된 `year/month/ticker_bucket` 파티션만 대상으로 한다.
- 재무/PIT 값이 새로 들어오면 해당 `available_at` 이후 다음 변경일 전까지의 기간이 영향 범위다.

**lookback은 단일 상수가 아니라 feature별 map으로 관리한다.** `config.py`의 `lookback_per_feature: dict[str, int]`에서 `volatility_60d → 60`, `mom_252d → 252`, `beta_3y → 750` 식으로 관리하고, 영향 범위는 항상 `max(...)`로 산정한다. 단일 상수(`max_lookback_days`)에 의존하면 새 피처 추가 시 lookback이 조용히 잘려 학습 품질이 저하될 수 있다.

### 4.3 Idempotent write

순서:

1. 입력 watermark와 대상 파티션 목록을 산출한다.
2. 대상 파티션을 `data_lake/_tmp/<run_id>/...`에 새로 쓴다.
3. row count, key uniqueness, null threshold, PIT leakage check를 수행한다.
4. 검증 통과 시 기존 파티션을 backup 또는 trash 경로로 이동하고 tmp 파티션을 rename한다.
5. `dataset_manifest.json`을 마지막에 갱신한다.

같은 입력과 같은 code version으로 재실행하면 같은 파티션과 같은 manifest가 생성되어야 한다.

## 5. 핵심 변환 설계

### 5.1 Price features

입력: `daily_ohlcv`, `corporate_action_factor`, `trading_calendar`

기본 피처:

- `adj_close`, `adj_open`, `adj_high`, `adj_low`
- `ret_1d`, `ret_5d`, `ret_20d`, `log_ret_1d`
- `volatility_20d`, `volatility_60d`
- `turnover_value = adj_close * volume`
- `turnover_z_20d`
- `is_halted`, `is_zero_volume`, `limit_move_flag`

`is_halted` 휴리스틱은 단일 패턴(예: OHLC가 모두 0)에 의존하지 않는다. KRX 정지/단기과열 공시 원천을 별도로 붙이기 전까지의 1차 구현은 다음 신호 중 *복수* 만족을 정지 후보로 본다.

1. `volume == 0` 그리고 `(open == high == low == close)` (전일 종가로 가격 4종이 정지된 형태)
2. `(open == 0) and (high == 0) and (low == 0)` (수집기에서 가격 4종을 0으로 표기한 케이스)
3. KRX 거래정지 공시 dimension(2차에서 추가)에 해당 `(trade_date, ticker)` 매치

1차 구현 직전 `daily_ohlcv`의 실제 분포를 한 번 검증해(0 가격 vs 전일 종가 freeze 비율) 본 휴리스틱을 픽스한다. 검증 결과는 `docs/dev/20260607_ETL/halt_distribution.md`에 기록한다.

Polars 예시:

```python
import polars as pl

price_features = (
    prices.lazy()
    .sort(["ticker", "trade_date"])
    .with_columns(
        (pl.col("adj_close") / pl.col("adj_close").shift(1).over("ticker") - 1).alias("ret_1d"),
        (pl.col("adj_close") / pl.col("adj_close").shift(5).over("ticker") - 1).alias("ret_5d"),
        (pl.col("adj_close").log() - pl.col("adj_close").shift(1).over("ticker").log()).alias("log_ret_1d"),
        ((pl.col("open") == 0) & (pl.col("high") == 0) & (pl.col("low") == 0) & (pl.col("close") > 0)).alias("is_halted"),
        (pl.col("volume") == 0).alias("is_zero_volume"),
    )
    .with_columns(
        pl.col("log_ret_1d").rolling_std(window_size=20).over("ticker").alias("volatility_20d"),
        (pl.col("adj_close") * pl.col("volume")).alias("turnover_value"),
    )
)
```

### 5.2 Flow features

입력: `krx_security_flow_raw`

규칙:

- 같은 `(trade_date, ticker, market, metric_code)`에 `KRX`와 `PYKRX`가 공존하면 `KRX` 우선.
- `PYKRX`는 `KRX`가 없는 key의 fallback으로만 사용한다.
- long format을 wide로 pivot한다.

기본 피처:

- `foreign_holding_shares`
- `institution_net_buy_volume`
- `individual_net_buy_volume`
- `foreign_net_buy_volume`
- `short_selling_volume`
- `short_selling_value`
- `short_selling_balance_quantity`
- 가격/거래량과 결합한 ratio:
  - `foreign_net_buy_volume / volume`
  - `short_selling_volume / volume`
  - `short_selling_value / turnover_value`

주의:

- 공매도 거래량이 일 거래량보다 큰 일부 row는 단위/조정 이슈 가능성이 있다. ratio는 cap하되 raw 값을 잃지 않도록 다음을 함께 둔다.
  - `*_outlier_flag` (불리언): 도메인 sanity 위반(예: `short_selling_volume > volume`) 발생 시 true.
  - `*_capped` (불리언): cap이 적용되었음을 알린다.
  - cap 미적용 raw ratio도 `_raw` suffix로 보존해 모델이 outlier-aware 학습/제외를 선택할 수 있게 한다.
  - quality report에 outlier row를 `(trade_date, ticker, metric)` 단위로 적재해 추후 수집기 버그 진단에 사용한다.
- 외국인 보유율은 `raw_payload`에 이미 존재하는 KRX 값에서 파생 가능하지만, gold에는 JSON이 아니라 numeric column으로만 저장한다.

### 5.3 Financial PIT features

입력 우선순위:

1. `stock_metric_fact`: 현재 가장 바로 쓰기 좋은 정규화 metric.
2. `dart_financial_statement_raw`: mapping 보강용.
3. `dart_xbrl_fact_raw`: 부족한 손익/세그먼트 metric fallback.
4. `dart_share_count_raw`, `dart_shareholder_return_raw`: 주식수/배당/자사주 이벤트.

기본 피처:

- `total_assets`, `total_liabilities`, `total_equity`
- `revenue`, `operating_income`, `net_income`
- `operating_cash_flow`, `investing_cash_flow`, `financing_cash_flow`
- `shares_outstanding`, `treasury_shares`, `float_shares`
- ratio:
  - `debt_to_equity`
  - `roe`, `roa`
  - `operating_margin`
  - `market_cap = adj_close * shares_pit_daily.shares_outstanding` (silver `shares_pit_daily`로 일별 정확)
  - `pb`, `per` 후보

주의:

- 음수 자본, 0 분모, 자본잠식은 `ratio_invalid_flag`를 남기고 무한대를 만들지 않는다.
- `reprt_code`별 분기/반기/사업보고서 의미를 유지한다.
- 백필 완료 후 `stock_metric_fact`는 2015~2024 보고서 4종 중심으로 확장되는 것을 ETL baseline으로 둔다. 단, 2025는 현재 coverage가 상대적으로 얕을 수 있으므로 raw/XBRL 경로 보강 전까지 coverage gap을 허용하거나 제외한다.

### 5.4 Label generation

label은 feature와 분리 생성한다.

```python
labels = (
    prices.lazy()
    .sort(["ticker", "trade_date"])
    .with_columns(
        (pl.col("adj_close").shift(-1).over("ticker") / pl.col("adj_close") - 1).alias("fwd_return_1d"),
        (pl.col("adj_close").shift(-5).over("ticker") / pl.col("adj_close") - 1).alias("fwd_return_5d"),
        (pl.col("adj_close").shift(-20).over("ticker") / pl.col("adj_close") - 1).alias("fwd_return_20d"),
    )
    .select(["trade_date", "ticker", "market", "fwd_return_1d", "fwd_return_5d", "fwd_return_20d"])
)
```

품질 규칙:

- label horizon 끝 날짜가 없는 최신 row는 해당 label을 null로 두거나 학습 split에서 제외한다.
- feature 생성 코드가 `fwd_`, `label_`, `target_` prefix column을 입력으로 받으면 실패시킨다.
- walk-forward validation에서 purge gap은 최대 label horizon 이상으로 둔다.

## 6. Chronological split

랜덤 split은 금지한다. 기본 split은 manifest로 관리하고, 모델 학습 시 manifest를 읽어서 필터링한다.

권장 1차 split:

| split | 기간 | 비고 |
|---|---|---|
| train | 2014-01-02 ~ 2023-12-28 | 시세/수급 장기 학습 |
| valid | 2024-01-02 ~ 2024-12-30 | 하이퍼파라미터/피처 선택 |
| test | 2025-01-02 ~ 2026-05-21 | 최종 holdout |

주의:

- 재무 PIT 피처까지 필수로 쓰는 실험은 DART 백필 완료 snapshot을 기준으로 별도 split을 둔다.
- 백필 완료 후 `stock_metric_fact`는 2015년 이후를 기본 범위로 보되, 실제 train 시작일은 `available_at` 검증 결과로 정한다. `report_availability`가 확정되기 전에는 보수적으로 2016년 이후 또는 metric coverage가 충분한 첫 거래일부터 시작한다.
- label horizon이 20거래일이면 train/valid/test 경계에 최소 20거래일 **purge gap**을 둔다. purge는 label horizon이 인접 split의 feature와 겹치지 않도록 train 끝과 valid 시작 사이를 비우는 장치다.
- 추가로 **embargo**를 둔다. embargo는 가장 긴 rolling lookback 피처가 split 경계에서 양쪽 데이터를 동시에 보지 못하도록 한쪽을 추가로 밀어내는 장치다. 보통 `embargo = small`(예: 1~5 거래일)로 두지만, 본 ETL은 `embargo_days = ceil(0.01 * max(lookback_per_feature.values()))`를 기본값으로 두고 manifest에 명시한다.
- 결과적으로 split 경계 비워두기 = `purge_days(=label horizon) + embargo_days`. Lopez de Prado(2018) 표준에 따른 분리. `split_manifest_v1`에 둘을 분리해 기록한다.

## 7. 품질 검증 체크리스트

필수 검증:

- gold key uniqueness: `(trade_date, ticker, market)` 중복 0.
- PIT leakage: 모든 재무 row에 대해 `available_at <= trade_date`.
- source leakage: label column이 feature parquet에 포함되지 않음.
- row count reconciliation:
  - `daily_ohlcv` source row count와 `price_daily` row count 비교.
  - flow dedupe 후 `(trade_date, ticker, market)` coverage 비교.
- date coverage:
  - `trade_date`는 KRX calendar에 존재해야 함.
  - 2014년 이후 연도별 ticker coverage 급감 시 실패 또는 warning.
- null threshold:
  - 핵심 가격 feature null은 상장 초기 lookback 구간을 제외하면 낮아야 함.
  - 재무 feature null은 metric별 coverage report로 분리.
- numeric sanity:
  - `low <= min(open, close) <= max(open, close) <= high`, 단 거래정지 raw 규칙 예외 처리.
  - ratio feature는 `inf`, `-inf`, `nan` 금지.
- partition integrity:
  - manifest partition list와 실제 파일 목록 일치.
  - tmp 경로 잔재 없음.

DuckDB 검증 예:

```sql
SELECT trade_date, ticker, market, COUNT(*) AS n
FROM read_parquet('data_lake/gold/ml_panel_daily_v1/**/*.parquet')
GROUP BY 1,2,3
HAVING COUNT(*) > 1;
```

## 8. CLI 계획

기존 CLI 스타일을 따라 `argparse` 기반으로 추가한다.

```bash
# 원천 DB에서 bronze 추출
krx-collector ml-etl extract \
  --source-db sj2 \
  --tables daily_ohlcv,krx_security_flow_raw,stock_metric_fact \
  --start 2026-05-01 \
  --end 2026-05-21

# silver feature component 생성
krx-collector ml-etl build-silver \
  --components price_daily,flow_daily_wide,financial_metric_pit \
  --start 2026-05-01 \
  --end 2026-05-21

# gold panel 생성
krx-collector ml-etl build-panel \
  --feature-set-version 20260607 \
  --start 2014-01-02 \
  --end 2026-05-21 \
  --horizons 1,5,20

# 품질 검증
krx-collector ml-etl validate \
  --dataset ml_panel_daily_v1 \
  --feature-set-version 20260607
```

## 9. 핵심 코드 스켈레톤

### 9.1 Config

```python
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class AvailableAtCutoffs:
    intraday_close_kst: time = time(15, 30)   # 장중/장후 경계
    daily_cutoff_kst: time = time(18, 0)      # 다음 거래일로 미는 경계


@dataclass(frozen=True)
class VersionRetention:
    keep_last_n: int = 5
    keep_within_days: int = 14


@dataclass(frozen=True)
class MlEtlConfig:
    lake_root: Path
    feature_set_version: str
    source_db: str
    # feature별 lookback (단일 상수 대신 map). 영향 범위는 max(...)로 계산.
    lookback_per_feature: dict[str, int] = field(default_factory=lambda: {
        "ret_1d": 1, "ret_5d": 5, "ret_20d": 20,
        "volatility_20d": 20, "volatility_60d": 60,
        "turnover_z_20d": 20,
    })
    label_horizons: tuple[int, ...] = (1, 5, 20)
    ticker_buckets: int = 64
    compression: str = "zstd"
    available_at_cutoffs: AvailableAtCutoffs = field(default_factory=AvailableAtCutoffs)
    version_retention: VersionRetention = field(default_factory=VersionRetention)

    @property
    def max_lookback_days(self) -> int:
        return max(self.lookback_per_feature.values(), default=0)
```

### 9.2 Partition planner

```python
from dataclasses import dataclass
from datetime import date

@dataclass(frozen=True)
class AffectedRange:
    compute_start: date
    compute_end: date
    write_start: date
    write_end: date

def plan_affected_range(
    new_min_date: date,
    new_max_date: date,
    *,
    calendar: TradingCalendar,
    max_lookback_days: int,
    max_label_horizon_days: int,
) -> AffectedRange:
    """Compute read window required to safely overwrite write_start..write_end.

    compute_start is in the PAST: rolling features need `max_lookback_days` of
    prior trading days to produce correct values starting at write_start.
    compute_end is in the FUTURE: forward-looking labels require
    `max_label_horizon_days` of subsequent trading days to be readable.
    The actual overwrite scope is only write_start..write_end; compute_start
    and compute_end define the READ scope, never the WRITE scope.
    """
    return AffectedRange(
        compute_start=calendar.shift(new_min_date, -max_lookback_days),
        compute_end=calendar.shift(new_max_date, +max_label_horizon_days),
        write_start=new_min_date,
        write_end=new_max_date,
    )
```

### 9.3 Atomic dataset swap

순진한 "final → backup 이동, tmp → final rename" 2단계 패턴은 두 rename 사이에 *파티션이 잠시 존재하지 않는* race window를 만든다. 학습 잡이 panel을 동시에 읽는 시나리오를 가정하면 안전하지 않다.

대신 **dataset 전체를 단일 디렉토리 포인터로 노출**하고, 새 버전을 별도 디렉토리에 다 쓴 뒤 **단 한 번의 atomic rename**으로 전환한다.

레이아웃:

```text
data_lake/gold/ml_panel_daily_v1/
  current -> versions/v_20260607T1432_ab12         # symlink (atomic update target)
  versions/
    v_20260607T1100_8e44/...                       # 이전 dataset
    v_20260607T1432_ab12/...                       # 새 dataset (write 완료)
  manifests/
    runs/<run_id>.json                             # append-only run log
    current.json -> runs/<run_id>.json             # symlink (or pointer)
```

```python
from pathlib import Path
from uuid import UUID
import os

def swap_dataset_version(
    *,
    dataset_root: Path,         # data_lake/gold/ml_panel_daily_v1
    new_version_dir: Path,       # versions/v_20260607T1432_ab12 (already fully written)
    run_id: UUID,
) -> None:
    """Atomically point `current` symlink at the new version.

    Single rename(2) on the symlink target -> readers either see the old
    version or the new version, never a missing partition.
    """
    current = dataset_root / "current"
    tmp_link = dataset_root / f".current.swap.{run_id}"
    os.symlink(new_version_dir.relative_to(dataset_root), tmp_link)
    os.replace(tmp_link, current)  # atomic on POSIX
```

증분 overwrite의 경우에도 영향받은 파티션만 새 version 디렉토리에 복사+덮어쓰기 한 뒤 위 swap을 적용한다(파일시스템 reflink/하드링크 가능 시 비용 최소). 단일 라이터 가정은 유지하되, 동시 reader는 안전하다.

#### 9.3.1 Backup retention / GC

`versions/` 누적 증가는 무한 성장하므로 GC job을 둔다.

- 기본 정책: 최근 `N=5` 버전 또는 14일 이내 생성 버전 보관, 그 외 삭제
- `dataset_manifest.json`에 모든 보관 버전이 등록되어 있어야 GC가 안전하게 삭제 가능
- GC 정책은 `config.py`의 `version_retention`으로 노출

#### 9.3.2 Manifest 동시쓰기 회피

`dataset_manifest.json` 단일 파일에 여러 run이 동시에 쓰면 마지막 writer가 이긴다. 본 ETL은 다음 패턴을 사용한다.

- `manifests/runs/<run_id>.json`을 **append-only**로 쓴다(파일 자체는 한 run이 생성하므로 충돌 없음).
- `manifests/current.json`은 위 atomic symlink swap과 동일한 방식으로 최신 run을 가리킨다.
- `dataset_manifest.json`은 `current.json`이 가리키는 run의 alias로만 취급한다.

### 9.4 DuckDB Parquet write

```python
import duckdb

def write_panel_parquet(con: duckdb.DuckDBPyConnection, query: str, output_path: str) -> None:
    con.execute(
        f"""
        COPY ({query})
        TO '{output_path}'
        (
          FORMAT PARQUET,
          COMPRESSION ZSTD,
          PARTITION_BY (year, month, ticker_bucket),
          ROW_GROUP_SIZE 100000
        )
        """
    )
```

실제 구현에서는 `query`와 `output_path` 문자열을 외부 입력에서 직접 받지 않는다. `output_path`는 `lake.py`의 path builder에서 합성한 값만 통과시키고, 그 외 경로는 거부한다(allowlist 체크). DuckDB의 `COPY ... TO`는 path를 parameter binding으로 받지 않으므로, 안전성은 호출자 측 헬퍼가 보장해야 한다. 본 ETL은 `write_panel_parquet`을 직접 호출하지 않고 `lake.write_partitioned(con, query, dataset, version)` 같은 wrapper만 사용한다.

## 10. 구현 단계

### Phase 0: source snapshot 고정

목표: 현재 진행 중인 DART 백필을 완료하고, ETL 입력으로 사용할 source snapshot을 확정한다.

- `dart-backfill-all-years.sh` 종료 및 `OpenDART backfill completed` 로그 확인
- sj2 `krx_data`에서 DART 6개 테이블의 `bsns_year=2015~2024` coverage 검증
- 기존 2025/2026 row count가 백필 전보다 감소하지 않았는지 확인
- `uv run krx-collector db sync-remote --ssh-host whi@sj2-server`로 local mirror 동기화
- sj2와 local의 DART 연도별 row count 비교
- 최종 `pg_database_size`, 주요 테이블별 `pg_total_relation_size`, source row count를 ETL manifest seed로 기록
- 이 단계가 끝나기 전에는 Phase 1 gold dataset을 최종 산출물로 간주하지 않는다.

### Phase 1: 최소 동작 ETL

목표: 시세 + 수급 + stock_metric_fact 기반 gold panel을 만든다.

- 의존성 추가: `polars`, `duckdb`, `pyarrow`
- `ml_etl/config.py`, `lake.py`, `manifest.py`, `planner.py` 작성
- `daily_ohlcv`, `krx_security_flow_raw`, `stock_metric_fact` bronze extract
- `report_availability` dimension 생성(§3.2.1 규칙 적용)
- `tradable_universe_pit` dimension 생성(survivorship bias 차단)
- `shares_pit_daily` 생성(분기 보고 + corporate action으로 일별 forward-step)
- `daily_ohlcv` 정지 패턴 분포 검증 → `is_halted` 휴리스틱 픽스
- price/flow/financial silver 생성
- `ml_panel_daily_v1` 생성(universe는 항상 `tradable_universe_pit` 통과)
- 기본 chronological split manifest 생성(`purge_days`/`embargo_days` 분리 기록)
- unit test: partition planner, flow source dedupe, PIT leakage, split boundary, available_at cutoffs, universe PIT, shares PIT
- integration test: e2e 합성 파이프라인, atomic swap crash safety, idempotent rerun

### Phase 2: 품질/운영 강화

목표: 반복 실행과 장애 복구를 안전하게 만든다.

- atomic dataset version swap(symlink rename) 도입 + race window 제거
- `manifests/runs/<run_id>.json` append-only run log + `current.json` symlink
- `versions/` GC job(retention `N`개 또는 `T`일)
- DuckDB 기반 validation suite
- `ingestion_runs`와 별도 `ml_etl_runs` 또는 manifest run log 연동
- local mirror와 sj2 source의 row count 비교 report
- KRX 거래정지/단기과열 공시 원천 추가로 `is_halted` 휴리스틱 → 정확 신호 교체

### Phase 3: 수정주가/공시 availability 고도화

목표: 장기 수익률 label과 재무 PIT 정확도를 높인다.

- `report_availability` dimension 정정공시·휴장일 코너케이스 보강(Phase 1에서 만든 1차 버전을 정밀화)
- `corporate_action_factor_v0_heuristic` → `corporate_action_factor_v1`로 SEIBRO/거래소 원천 기반 교체
- adjusted OHLCV와 raw OHLCV를 feature set version으로 분리
- dividend/total return label 실험 dataset 추가

### Phase 4: 모델 소비 최적화

목표: 모델 타입별 입력을 빠르게 공급한다.

- LightGBM/CatBoost용 wide panel
- sequence model용 `(ticker, window_start, window_end)` manifest
- walk-forward split generator
- feature importance/coverage report와 feature catalog 자동 생성

## 11. 1차 산출물 정의

1차 완료 기준:

- `data_lake/gold/ml_panel_daily_v1/feature_set_version=20260607/...` 생성
- key: `(trade_date, ticker, market)` 중복 0
- 기간: 기본 2014-01-02 ~ 최신 source trade_date
- source snapshot: DART 백필 완료 후 sj2 `krx_data` 또는 동일 row count의 local mirror
- 포함 모달리티:
  - adjusted 또는 raw 기반 price feature
  - KRX/PYKRX deduped flow feature
  - `stock_metric_fact` 기반 PIT financial feature
- label:
  - `fwd_return_1d`, `fwd_return_5d`, `fwd_return_20d`
- split:
  - train/valid/test chronological manifest
- manifest:
  - source row count, source date range, feature schema, git SHA, quality check 결과

## 12. 주요 리스크와 대응

| 리스크 | 영향 | 대응 |
|---|---|---|
| `available_at` 부재 | 재무 피처 lookahead 가능 | `report_availability` dimension 전까지 보수적 available date 적용 또는 gold 제외 |
| Parquet row-level merge 부재 | 중복/꼬인 append 가능 | 영향 파티션 overwrite만 허용 |
| ticker 직접 partition | small file 폭증 | `ticker_bucket` 사용 |
| KRX/PYKRX flow 중복 | feature row 중복/값 중복 | source priority dedupe |
| 수정주가 미흡 | 장기 label 왜곡 | `corporate_action_factor` 별도 구축, feature version 분리 |
| 최신 row label null | 학습 중 null target | horizon별 label null row를 split에서 제외 |
| DART 백필 진행 중 snapshot 사용 | 재무 coverage와 row count가 실행 중 변동 | Phase 0 완료 전 최종 ETL/manifest 생성 금지 |
| 백필 완료 후 DB/lake 크기 증가 | extract 시간 및 저장 공간 증가 | 200GB+ PostgreSQL 원천, Parquet lake 용량, partition overwrite 비용을 전제로 설계 |
| 재무 coverage의 실제 `available_at` 불확실 | 장기 재무 PIT 시작점 왜곡 | `report_availability` 검증 후 financial split 시작일 확정 |
| Survivorship bias(현재 `stock_master` 2,780종목만 universe로 사용) | 백테스트 성능 위로 편향 | `tradable_universe_pit` dimension 통과를 모든 학습/평가 universe의 단일 출입구로 강제 |
| `shares_outstanding`을 분기 보고 시점 값으로 forward-fill | 분기 내 자본행위(분할/소각) 발생일에 `market_cap` 등 ratio 왜곡 | `shares_pit_daily` 일별 step-adjust 산출물을 silver에서 사용 |
| `is_halted` 휴리스틱 오탐/미탐 | 거래정지일에 잘못된 ret 계산 또는 정지 누락 | 실제 분포 검증 후 휴리스틱 픽스, 2차에서 KRX 정지 공시 원천으로 교체 |
| Atomic swap 부재 / `_backup` 누적 | 동시 read 시 missing partition, 디스크 무한 증가 | `current` symlink 단일 rename swap + version retention GC |
| `dataset_manifest.json` 단일 파일 동시쓰기 | 마지막 writer가 이전 run 메타 덮어씀 | `manifests/runs/<run_id>.json` append-only + `current.json` symlink |
| 단일 `max_lookback_days` 상수 의존 | 새 장기 lookback 피처 추가 시 조용한 lookback 절삭 | `lookback_per_feature` map + `max(...)`로 영향 범위 산정 |
| Walk-forward에서 purge만 두고 embargo 누락 | 인접 split의 rolling lookback 누설 | `purge_days`(=label horizon) + `embargo_days`(=lookback 비례) 분리 기록 |
