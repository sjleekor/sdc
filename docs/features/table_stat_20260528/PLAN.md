# 수집 테이블 통계적 특성 프로파일링 진행 계획

본 문서는 주가예측모델 피처 엔지니어링을 위한 사전 분석으로, 본 프로젝트가 적재하는 PostgreSQL 테이블들에 대해
이미 완료된 `dart_financial_statement_raw` 와 동일한 형식의 통계적 특성 프로파일링을 일관되게 진행하기 위한 계획서이다.

- 최초 작성: 2026-05-28
- 현재 갱신: 2026-06-15
- 대상 DB: 로컬 PostgreSQL (`.env` 의 `DB_DSN`, DB명 `mydb`)
- 확인 기준: `sql/postgres_ddl.sql`, `src/krx_collector/infra/db_postgres/remote_sync.py`, 관련 service/storage 코드, 로컬 DB `information_schema` 및 집계 쿼리
- 제약: 현재 `sj2-server` 접근 불가. 아래 행수와 커버리지는 `sj2-server` 주요 데이터 테이블이 동기화된 로컬 DB 기준이다.
- 참고(템플릿): [`statistical_profile_dart_financial_statement_raw.md`](./statistical_profile_dart_financial_statement_raw.md)
- 산출물 위치: `docs/features/table_stat_20260528/statistical_profile_<table_name>.md`

로컬 DB 기준 `remote_db_sync` 최근 성공 로그는 2026-06-14이며, `remote_db_sync.daily_ohlcv` 체크포인트는
`trade_date=2026-06-10` 이다. sj2 접근이 복구되면 동일 쿼리를 sj2에도 실행해 로컬 미러 지연 여부를 별도 표기한다.

---

## 1. 대상 테이블 인벤토리

`sql/postgres_ddl.sql` 와 실제 로컬 DB(`information_schema.tables`) 양쪽에서 확인한 적재 테이블 전수.
행수는 2026-06-15 로컬 DB 실측값이다.

### 1.1. 확인 결과 요약

- DDL과 로컬 DB 모두 23개 public 테이블을 가진다.
- 2026-05-28 계획에 없던 `common_feature_*` 5개 테이블이 DDL, remote sync 대상, 로컬 DB에 모두 존재한다.
- `krx_security_flow_raw` 는 더 이상 미적재 테이블이 아니며 7,644만 행 규모의 핵심 피처 후보 테이블이다.
- `operating_source_document`, `operating_metric_fact` 는 스키마와 CLI 처리 경로는 있으나 로컬 DB 기준 0행이고, `remote_sync.py` 의 full-refresh 대상에는 아직 포함되어 있지 않다.
- 완료 프로파일 산출물은 21개이다: 기존 핵심 8개 + Wave 1 `common_feature_*` 5개 + Wave 2 마스터/메타 4개 + Wave 3 설정/운영 4개.

### 1.2. 수집/원시/팩트 테이블 - 프로파일링 우선

| # | 테이블 | 도메인 | 행수(2026-06-15) | 주요 커버리지 | 현재 상태 |
|---|---|---|---:|---|---|
| 1 | `dart_xbrl_fact_raw` | DART XBRL 팩트 원시 | 80,143,928 | 2015-2025, 2,606 corp/ticker | ✅ 완료 |
| 2 | `krx_security_flow_raw` | KRX 투자자/공매도/대차성 수급 원시 | 76,446,601 | 2007-06-05-2026-06-10, 2,779 ticker, 7 metrics | ✅ 완료 |
| 3 | `dart_financial_statement_raw` | DART 재무제표 원시 | 16,887,271 | 2015-2026, 2,608 corp/ticker | ✅ 완료(템플릿) |
| 4 | `dart_shareholder_return_raw` | DART 배당/자기주식 등 주주환원 원시 | 7,831,054 | 2015-2025, 2,650 corp/ticker | ✅ 완료 |
| 5 | `daily_ohlcv` | KRX 일봉 시세 | 6,550,517 | 2007-06-05-2026-06-10, 2,783 ticker | ✅ 완료 |
| 6 | `stock_metric_fact` | 정규화된 종목 지표 팩트 | 765,966 | 2015-2025, 2,650 ticker | ✅ 완료 |
| 7 | `dart_share_count_raw` | DART 주식수 변동 원시 | 312,329 | 2015-2025, 2,650 corp/ticker | ✅ 완료 |
| 8 | `dart_xbrl_document` | DART XBRL 문서 메타 | 81,532 | 2015-2025, 2,606 corp/ticker | ✅ 완료 |
| 9 | `common_feature_daily_fact` | KRX 거래일 정렬 공통 피처 팩트 | 5,550 | 2025-11-03-2026-06-12, 37 active features | ✅ 완료 |
| 10 | `common_feature_observation_raw` | 공통 시장/거시 원천 observation | 2,752 | 2024-09-30-2026-06-12, 26 active series, 4 source | ✅ 완료 |
| 11 | `operating_source_document` | 섹터별 영업 KPI 원천 문서 | 0 | - | 적재 후 프로파일 |
| 12 | `operating_metric_fact` | 섹터별 영업 KPI 추출 팩트 | 0 | - | 적재 후 프로파일 |

### 1.3. 마스터/카탈로그/브리지 테이블 - 보조 프로파일링

| # | 테이블 | 용도 | 행수(2026-06-15) | 주요 커버리지 | 현재 상태 |
|---|---|---|---:|---|---|
| 13 | `dart_corp_master` | DART 기업 마스터 | 116,503 | `modify_date` 2017-2026, 2,657 active listed pairs | ✅ 완료 |
| 14 | `stock_master_snapshot_items` | 종목 마스터 스냅샷 항목 | 56,357 | 21 snapshot, 2,783 ticker | ✅ 완료 |
| 15 | `stock_master` | 종목 마스터 현행 | 2,783 | KOSPI/KOSDAQ, 2,769 active | ✅ 완료 |
| 16 | `stock_master_snapshot` | 종목 마스터 스냅샷 메타 | 21 | 2026-04-10-2026-06-10 | ✅ 완료 |
| 17 | `common_feature_catalog` | 모델 노출 공통 피처 카탈로그 | 54 | 37 active features, 12 categories | ✅ 완료 |
| 18 | `common_feature_catalog_input` | 공통 피처와 원천 series 연결 | 56 | feature-series bridge | ✅ 완료 |
| 19 | `common_feature_series` | 공통 피처 원천 series 카탈로그 | 33 | 26 active series, ECOS/FDR/FRED/KRX active | ✅ 완료 |
| 20 | `metric_catalog` | 재무 지표 카탈로그 | 29 | 29 active metric, fact 사용 29/29 | ✅ 완료 |
| 21 | `metric_mapping_rule` | 재무 지표 매핑 규칙 | 59 | 59 active rule, unused XBRL fallback 6 | ✅ 완료 |

### 1.4. 운영/동기화 테이블 - 경량 점검

| # | 테이블 | 용도 | 행수(2026-06-15) | 주요 커버리지 | 현재 상태 |
|---|---|---|---:|---|---|
| 22 | `ingestion_runs` | 수집/정규화 잡 실행 로그 | 171 | 11 run_type, stale running 16 | ✅ 완료 |
| 23 | `sync_checkpoints` | 증분 동기화 체크포인트 | 1 | `remote_db_sync.daily_ohlcv`, cursor 2026-06-10 | ✅ 완료 |

---

## 2. 진행 우선순위 (Wave)

피처화 가치, 데이터 규모, 적재 완료도, 아직 프로파일되지 않은 활성 계층 여부를 기준으로 재정렬한다.

- **Wave 0 (완료된 핵심 프로파일)**
  - `dart_financial_statement_raw`, `daily_ohlcv`, `dart_xbrl_fact_raw`, `dart_xbrl_document`,
    `dart_shareholder_return_raw`, `dart_share_count_raw`, `stock_metric_fact`, `krx_security_flow_raw`
- **Wave 1 (신규 활성 공통 피처 계층)**
  - `common_feature_observation_raw`, `common_feature_daily_fact`
  - 경량 동반 점검: `common_feature_series`, `common_feature_catalog`, `common_feature_catalog_input`
- **Wave 2 (마스터/메타)**
  - `stock_master`, `stock_master_snapshot`, `stock_master_snapshot_items`, `dart_corp_master`
- **Wave 3 (완료된 설정/운영 - 경량 버전)**
  - `metric_catalog`, `metric_mapping_rule`, `ingestion_runs`, `sync_checkpoints`
- **Wave 4 (데이터 적재 후)**
  - `operating_source_document`, `operating_metric_fact`
  - 현재 0행이므로 본격 통계 프로파일은 적재 후 진행한다. 단, 코드/스키마 기반의 기대 키와 품질 체크 SQL은 미리 작성 가능하다.

---

## 3. 테이블별 공통 SQL 템플릿 (재사용 체크리스트)

`dart_financial_statement_raw` 프로파일 §2 의 SQL 패턴을 표준화한 공통 체크리스트.
각 테이블에 적용할 때, 테이블에 해당 컬럼이 없으면 생략하고 도메인 특화 SQL(§4) 로 보완한다.

| 코드 | 목적 | 핵심 SQL 패턴 |
|---|---|---|
| C1 | 총 행수 / 유일 키 수 / 적재 기간 | `COUNT(*)`, `COUNT(DISTINCT <PK 후보>)`, `MIN/MAX(<time col>)` |
| C2 | 시간축 분포 | `GROUP BY <date / year / month>` |
| C3 | 카테고리 컬럼 분포 | 각 enum 성격 컬럼별 `GROUP BY` (`source`, `market`, `currency`, `*_div`, `*_code` 등) |
| C4 | 컬럼별 NULL 비율 | `ROUND(100.0*SUM((col IS NULL)::int)/COUNT(*),2)` 일괄 |
| C5 | 자연 키 중복 검사 | `GROUP BY <natural key cols> HAVING COUNT(*)>1` |
| C6 | 엔티티(corp/ticker/series/feature)당 행수 분포 | `WITH t AS (... GROUP BY entity) SELECT AVG/MIN/MEDIAN/MAX` |
| C7 | 엔티티 x 시간축 커버리지 | `GROUP BY entity, year/period COUNT(DISTINCT ...)` |
| C8 | 핵심 수치 컬럼 분위수/평균/표준편차 | `percentile_cont(0.01/0.25/0.5/0.75/0.99)` |
| C9 | 코드/카테고리 상위 빈도 Top-N | `GROUP BY <code> ORDER BY COUNT(*) DESC LIMIT 20` |
| C10 | 적재 시각 월별 추세 | `GROUP BY date_trunc('month', fetched_at/created_at/updated_at/generated_at)` |
| C11 | 통화/단위 별 금액 스케일 | `GROUP BY currency/unit, AVG(ABS(amount)), MAX(ABS(amount))` |
| C12 | 외래키 정합성 | `LEFT JOIN ... WHERE r.<fk> IS NULL` |
| C13 | PIT 가용성 검증 | `available_from_date <= feature_date`, `release_date/available_from_date` 기준 look-ahead 여부 |

---

## 4. 테이블별 특화 조사 항목

각 테이블에서 모델링 관점에서 반드시 추가로 확인해야 할 도메인 특화 질문.

### 4.1. `daily_ohlcv` (시계열 라벨/피처 후보의 1순위)
- 거래일 커버리지: 영업일 누락 여부(KRX 영업일 캘린더 대비 결측).
- 종목별 상장/상폐 시점: `MIN(trade_date), MAX(trade_date) GROUP BY ticker`.
- 종목별 시계열 길이 분포.
- `volume=0` 거래정지 일자 비율.
- 가격 이상치(전일 대비 ±30% 초과) 분포: 액면분할/이벤트 검출 단서.
- 시가/고가/저가/종가 일관성: `low <= min(open,close) AND high >= max(open,close)`.
- 시장(KOSPI/KOSDAQ)별 행수 및 종목 수.

### 4.2. `krx_security_flow_raw` (종목별 수급/공매도 핵심 피처)
- `metric_code` 분포: `foreign_holding_shares`, `foreign_net_buy_volume`, `individual_net_buy_volume`,
  `institution_net_buy_volume`, `short_selling_value`, `short_selling_volume`, `short_selling_balance_quantity`.
- 종목/시장/metric별 날짜 커버리지와 누락 구간.
- 동일 `(trade_date, ticker, market, metric_code, source)` 중복 검사.
- 수급 metric 간 합계/부호 일관성: 개인/기관/외국인 순매수 합계의 해석 가능성 검토.
- 공매도 거래량/대금과 대차잔고의 scale 및 극단값 분포.
- `daily_ohlcv` 와의 `(trade_date, ticker, market)` join 커버리지.

### 4.3. `dart_xbrl_fact_raw` (최대 규모 - 샘플링 전략 필수)
- 전체 행수/연도 분포는 본실행, 고비용 분위수/문자열 Top-N은 `TABLESAMPLE SYSTEM (1)` 또는 보고서 subset으로 진행하고 결과에 표기.
- `concept_id` / `element_id` 의 distinct 수 및 Top-50 빈도.
- 단위(`unit_id`, `unit_measure`)와 `decimals` 분포.
- context: duration/instant 비율, `period_start/end`, `instant_date` 분포.
- corp/연도/보고서 별 fact 수 분포(편향 진단).
- `dart_xbrl_document` 와의 join 정합성(고아 fact 비율).

### 4.4. `dart_xbrl_document`
- 보고서 유형/연도/분기 분포 및 corp 커버리지.
- `dart_financial_statement_raw.rcept_no` 와의 교집합/차집합.
- 동일 corp/보고서의 재공시(개정) 횟수.

### 4.5. `dart_shareholder_return_raw`
- 이벤트 유형(배당/자사주취득/소각/처분 등) 코드 분포.
- corp/연도별 이벤트 수, 금액 분포, 단위 분포.
- 자연 키 중복 및 동일 이벤트 재공시 패턴.

### 4.6. `dart_share_count_raw`
- 주식수 변동 사유 코드 분포.
- corp 당 행수 분포(자본금 변동 빈도).
- `발행주식총수` 시계열 연속성 검증(전월 대비 jump 탐지).

### 4.7. `stock_metric_fact`
- `metric_code` 분포와 source table 분포.
- 종목/사업연도/보고서 커버리지.
- 메트릭별 값 분포(분위수), NULL/0 비율.
- 동일 `(ticker, metric_code, bsns_year, reprt_code)` 중복 검사.
- 모델 입력으로 바로 쓰기 어려운 sparse metric과 안정적으로 쓸 수 있는 core metric 분리.

### 4.8. `dart_corp_master`
- 상장/비상장 비율, `ticker` NULL 비율.
- 시장 구분 분포, `modify_date` 분포.
- `stock_master` 와의 join 일치율.

### 4.9. `stock_master` / `stock_master_snapshot` / `stock_master_snapshot_items`
- 시장/상태 분포, 우선주/보통주 식별 가능성.
- 스냅샷 일자 간격, 두 스냅샷 간 종목 추가/제거 수.
- snapshot items와 `dart_corp_master` 의 corp_code 매핑 커버리지.

### 4.10. `common_feature_*` (시장/거시 공통 피처 계층)
- `common_feature_series`: source/category/frequency별 active series 수, `history_start_date`, `max_stale_business_days` 분포.
- `common_feature_observation_raw`: source/series별 observation 범위, `available_from_date`, `release_date`, `vintage` 분포.
- `common_feature_catalog`: category별 active feature 수, transform code 분포.
- `common_feature_catalog_input`: feature와 source series의 1:1/1:N 관계, 고아 feature/input 여부.
- `common_feature_daily_fact`: feature/category별 KRX 거래일 커버리지, forward-fill/stale 구간, `source_observation_ids` 추적 가능성.
- PIT 검증: 모든 daily fact가 해당 `feature_date` 기준 `available_from_date <= feature_date` 인 observation만 참조하는지 확인.
- 기존 종목 패널과 결합 가능성: `daily_ohlcv`의 거래일 범위 대비 common feature coverage gap.

### 4.11. 운영/설정 테이블
- `metric_catalog`, `metric_mapping_rule`: 행수, distinct key, 매핑 규칙이 참조하는 metric code의 유효성.
- `ingestion_runs`: job 별 성공/실패/partial/running 비율, 평균 소요시간, 최근 실행.
- `sync_checkpoints`: 채널별 최신 cursor, stale 여부.

### 4.12. 미적재 operating 테이블
- `operating_source_document`, `operating_metric_fact` 는 적재 후 §3 전 항목과 다음을 추가:
  - source document와 metric fact의 외래키 정합성.
  - 섹터/문서 유형/metric code 분포.
  - 추출기별 성공/실패 패턴 및 raw snippet 품질.

---

## 5. 산출물 표준 포맷

각 테이블당 1개 파일.

- 경로: `docs/features/table_stat_20260528/statistical_profile_<table_name>.md`
- 섹션(템플릿과 동일):
  0. 테이블 스키마 요약 (`information_schema.columns`)
  1. 핵심 결론 (Executive Summary, bullet)
  2. 데이터 특성 조사용 SQL 모음 (§3 공통 + §4 특화)
  3. SQL 실제 실행 결과 (값 포함)
  4. 모델링 시사점 / 후속 조치 권고

경량 프로파일 대상(`*_catalog`, bridge, 운영 테이블)은 2-4쪽 내외로 줄이되, 자연키 중복, FK 정합성, stale 여부는 반드시 포함한다.

---

## 6. 실행 절차 (Runbook)

각 테이블 진행 시 동일하게 반복:

1. **스키마 확인**
   ```sql
   SELECT column_name, data_type, is_nullable
   FROM information_schema.columns
   WHERE table_schema='public' AND table_name='<TABLE>'
   ORDER BY ordinal_position;
   ```
2. **자연 키 후보 식별**: DDL(`sql/postgres_ddl.sql`) 의 `PRIMARY KEY` / `UNIQUE` 제약 우선 사용.
3. **행수/범위 확인**: 로컬 DB에서 `COUNT(*)`, `MIN/MAX(<date/year>)`, `COUNT(DISTINCT <entity>)` 실행.
4. **§3 공통 SQL 체크리스트** 중 해당 컬럼이 있는 항목만 선택해 실행.
5. **§4 도메인 특화 SQL** 실행.
6. **대규모 테이블(수천만 행 이상 또는 매우 느린 쿼리)** 은 `TABLESAMPLE SYSTEM (1)` 또는 1-2개 corp/연도 subset으로 검증 후 본실행하고, 결과에 샘플링 여부를 명시한다.
7. 결과를 §5 포맷에 맞춰 마크다운으로 정리한다.

### 6.1. 표준 실행 환경

- 로컬 DB 조회는 helper 사용:
  ```bash
  .agents/skills/sdc-db/scripts/dbq.sh local "<SQL>"
  ```
- sj2 접근 가능 시에는 같은 SQL을 `sj2` target에도 실행해 local mirror와 비교한다.
- 임시 SQL 파일이 필요하면 `/tmp/` 에 두고, 문서에는 실행 SQL과 결과만 남긴다.
- credentials 또는 DSN 원문은 문서에 남기지 않는다.

---

## 7. 일정 및 체크리스트

진행할 때마다 아래 체크박스를 갱신한다.

### 7.1. 완료

- [x] `dart_financial_statement_raw` - [`statistical_profile_dart_financial_statement_raw.md`](./statistical_profile_dart_financial_statement_raw.md)
- [x] `daily_ohlcv` - [`statistical_profile_daily_ohlcv.md`](./statistical_profile_daily_ohlcv.md)
- [x] `dart_xbrl_fact_raw` - [`statistical_profile_dart_xbrl_fact_raw.md`](./statistical_profile_dart_xbrl_fact_raw.md)
- [x] `dart_xbrl_document` - [`statistical_profile_dart_xbrl_document.md`](./statistical_profile_dart_xbrl_document.md)
- [x] `dart_shareholder_return_raw` - [`statistical_profile_dart_shareholder_return_raw.md`](./statistical_profile_dart_shareholder_return_raw.md)
- [x] `dart_share_count_raw` - [`statistical_profile_dart_share_count_raw.md`](./statistical_profile_dart_share_count_raw.md)
- [x] `stock_metric_fact` - [`statistical_profile_stock_metric_fact.md`](./statistical_profile_stock_metric_fact.md)
- [x] `krx_security_flow_raw` - [`statistical_profile_krx_security_flow_raw.md`](./statistical_profile_krx_security_flow_raw.md)
- [x] `common_feature_observation_raw` *(Wave 1)* - [`statistical_profile_common_feature_observation_raw.md`](./statistical_profile_common_feature_observation_raw.md)
- [x] `common_feature_daily_fact` *(Wave 1)* - [`statistical_profile_common_feature_daily_fact.md`](./statistical_profile_common_feature_daily_fact.md)
- [x] `common_feature_series` *(Wave 1, 경량)* - [`statistical_profile_common_feature_series.md`](./statistical_profile_common_feature_series.md)
- [x] `common_feature_catalog` *(Wave 1, 경량)* - [`statistical_profile_common_feature_catalog.md`](./statistical_profile_common_feature_catalog.md)
- [x] `common_feature_catalog_input` *(Wave 1, 경량)* - [`statistical_profile_common_feature_catalog_input.md`](./statistical_profile_common_feature_catalog_input.md)
- [x] `stock_master` *(Wave 2)* - [`statistical_profile_stock_master.md`](./statistical_profile_stock_master.md)
- [x] `stock_master_snapshot` *(Wave 2)* - [`statistical_profile_stock_master_snapshot.md`](./statistical_profile_stock_master_snapshot.md)
- [x] `stock_master_snapshot_items` *(Wave 2)* - [`statistical_profile_stock_master_snapshot_items.md`](./statistical_profile_stock_master_snapshot_items.md)
- [x] `dart_corp_master` *(Wave 2)* - [`statistical_profile_dart_corp_master.md`](./statistical_profile_dart_corp_master.md)
- [x] `metric_catalog` *(Wave 3, 경량)* - [`statistical_profile_metric_catalog.md`](./statistical_profile_metric_catalog.md)
- [x] `metric_mapping_rule` *(Wave 3, 경량)* - [`statistical_profile_metric_mapping_rule.md`](./statistical_profile_metric_mapping_rule.md)
- [x] `ingestion_runs` *(Wave 3, 경량)* - [`statistical_profile_ingestion_runs.md`](./statistical_profile_ingestion_runs.md)
- [x] `sync_checkpoints` *(Wave 3, 경량)* - [`statistical_profile_sync_checkpoints.md`](./statistical_profile_sync_checkpoints.md)

### 7.2. 다음 작업

- [ ] `operating_source_document` *(Wave 4, 적재 후)*
- [ ] `operating_metric_fact` *(Wave 4, 적재 후)*

---

## 8. 산출물의 활용

- 본 시리즈 문서가 누적되면 모델링 피처 카탈로그 작성(`docs/features/feature_catalog.md`) 의 1차 입력으로 사용한다.
- 결측/중복/이상치 패턴은 데이터 품질 모니터링 SQL 로 재활용한다.
- `common_feature_*` 프로파일 결과는 종목별 패널과 결합할 때 look-ahead bias 방지, stale feature 제한, 공통 피처 coverage gate의 기준으로 사용한다.
