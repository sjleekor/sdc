# 수집 테이블 통계적 특성 프로파일링 — 진행 계획

본 문서는 주가예측모델 피처 엔지니어링을 위한 사전 분석으로, 본 프로젝트가 적재하는 PostgreSQL 테이블들에 대해
이미 완료된 `dart_financial_statement_raw` 와 동일한 형식의 통계적 특성 프로파일링을 일관되게 진행하기 위한 계획서이다.

- 작성 일시: 2026-05-28
- 대상 DB: `.env` 의 `DB_DSN` → PostgreSQL (`mydb`)
- 참고(템플릿): [`statistical_profile_dart_financial_statement_raw.md`](./statistical_profile_dart_financial_statement_raw.md)
- 산출물 위치: `docs/features/table_stat_20260528/statistical_profile_<table_name>.md`

---

## 1. 대상 테이블 인벤토리

`sql/postgres_ddl.sql` 와 실제 DB(`information_schema.tables`) 양쪽에서 확인한 적재 테이블 전수.
행수는 2026-05-28 시점 실측값(0 인 테이블은 적재 미시작).

### 1.1. 수집(원시/팩트) 테이블 — 프로파일링 우선

| # | 테이블 | 도메인 | 행수(2026-05-28) | 비고 |
|---|---|---|---:|---|
| 1 | `dart_financial_statement_raw` | DART 재무제표(원시) | 1,254,634 | ✅ 완료(템플릿) |
| 2 | `dart_xbrl_fact_raw` | DART XBRL 팩트(원시) | 18,696,562 | 최대 규모, 핵심 |
| 3 | `dart_xbrl_document` | DART XBRL 문서 메타 | 8,255 | `dart_xbrl_fact_raw` 와 함께 |
| 4 | `dart_shareholder_return_raw` | DART 배당/자기주식 등 주주환원 | 263,030 | |
| 5 | `dart_share_count_raw` | DART 주식수 변동 | 10,295 | |
| 6 | `dart_corp_master` | DART 기업 마스터 | 116,503 | 사실상 마스터지만 적재 규모 큼 |
| 7 | `daily_ohlcv` | KRX 일봉 시세 | 6,517,317 | 모델 라벨/피처 핵심 |
| 8 | `stock_master_snapshot_items` | 종목 마스터 스냅샷 항목 | 53,588 | |
| 9 | `stock_master_snapshot` | 종목 마스터 스냅샷 | 20 | 작지만 메타 중요 |
| 10 | `stock_master` | 종목 마스터(현행) | 2,780 | |
| 11 | `stock_metric_fact` | 정규화된 종목 지표 팩트 | 34,411 | 피처 직접 후보 |
| 12 | `krx_security_flow_raw` | KRX 투자자별 매매(원시) | 69,682,921 | ✅ 완료 |
| 13 | `operating_source_document` | 영업 원천 문서 | 0 | 〃 |
| 14 | `operating_metric_fact` | 영업 지표 팩트 | 0 | 〃 |

### 1.2. 운영/설정 테이블 — 보조 프로파일링(간단 요약만)

| # | 테이블 | 용도 | 행수 |
|---|---|---|---:|
| 15 | `metric_catalog` | 지표 카탈로그 | 29 |
| 16 | `metric_mapping_rule` | 지표 매핑 규칙 | 59 |
| 17 | `ingestion_runs` | 수집 잡 실행 로그 | 92 |
| 18 | `sync_checkpoints` | 증분 동기화 체크포인트 | 1 |

> 운영/설정 테이블은 모델 피처 후보가 아니므로 §3 의 공통 SQL 중 “스키마 / 행수 / 키 중복 / 시간 범위” 정도만 정리하는 경량 버전으로 진행한다.

---

## 2. 진행 우선순위 (Wave)

피처화 가치 × 데이터 규모 × 적재 완료도를 고려해 3 웨이브로 나누어 순차 진행한다.

- **Wave 1 (핵심 시세/재무 — 즉시 진행)**
  - `daily_ohlcv`, `dart_xbrl_fact_raw`, `dart_xbrl_document`, `dart_shareholder_return_raw`, `dart_share_count_raw`, `stock_metric_fact`
- **Wave 2 (마스터/메타 — Wave 1 이후)**
  - `stock_master`, `stock_master_snapshot`, `stock_master_snapshot_items`, `dart_corp_master`
- **Wave 3 (설정/운영 — 경량 버전만)**
  - `metric_catalog`, `metric_mapping_rule`, `ingestion_runs`, `sync_checkpoints`
- **Wave 4 (데이터 적재 후)**
  - `krx_security_flow_raw`, `operating_source_document`, `operating_metric_fact` — 적재가 시작되어 1개 이상 비어있지 않은 시점부터 동일한 절차로 진행.

---

## 3. 테이블별 공통 SQL 템플릿 (재사용 체크리스트)

`dart_financial_statement_raw` 프로파일 §2 의 18개 SQL 을 표준화한 “공통 SQL 체크리스트”.
각 테이블에 적용할 때, **테이블에 해당 컬럼이 없으면 해당 항목은 생략**하고, 도메인 특화 SQL(§4) 로 보완한다.

| 코드 | 목적 | 핵심 SQL 패턴 |
|---|---|---|
| C1 | 총 행수 / 유일 키 수 / 적재 기간 | `COUNT(*)`, `COUNT(DISTINCT <PK 후보>)`, `MIN/MAX(<time col>)` |
| C2 | 시간축 분포 | `GROUP BY <date / year / month>` |
| C3 | 카테고리 컬럼 분포 | 각 enum 성격 컬럼별 `GROUP BY` (`source`, `market`, `currency`, `*_div`, `*_code` …) |
| C4 | 컬럼별 NULL 비율 | `ROUND(100.0*SUM((col IS NULL)::int)/COUNT(*),2)` 일괄 |
| C5 | 자연 키 중복 검사 | `GROUP BY <natural key cols> HAVING COUNT(*)>1` |
| C6 | 엔티티(corp/ticker)당 행수 분포 | `WITH t AS (... GROUP BY entity) SELECT AVG/MIN/MEDIAN/MAX` |
| C7 | 엔티티 × 시간축 커버리지 | `GROUP BY entity, year/period COUNT(DISTINCT ...)` |
| C8 | 핵심 수치 컬럼 분위수/평균/표준편차 | `percentile_cont(0.01/0.25/0.5/0.75/0.99)` |
| C9 | 코드/카테고리 상위 빈도 Top-N | `GROUP BY <code> ORDER BY COUNT(*) DESC LIMIT 20` |
| C10 | 적재 시각 월별 추세 | `GROUP BY date_trunc('month', fetched_at/created_at)` |
| C11 | 통화/단위 별 금액 스케일 | `GROUP BY currency, AVG(ABS(amount)), MAX(ABS(amount))` |
| C12 | 외래키 정합성 | `LEFT JOIN ... WHERE r.<fk> IS NULL` |

---

## 4. 테이블별 특화 조사 항목

각 테이블에서 모델링 관점에서 반드시 추가로 확인해야 할 도메인 특화 질문.

### 4.1. `daily_ohlcv` (시계열 라벨/피처 후보의 1순위)
- 거래일 커버리지: 영업일 누락 여부 (KRX 영업일 캘린더 대비 결측).
- 종목별 상장/상폐 시점: `MIN(date), MAX(date) GROUP BY ticker`.
- 종목별 시계열 길이 분포(연속 거래일 수).
- `volume=0` 거래정지 일자 비율, `close=NULL` 결측 비율.
- 가격 이상치(전일 대비 ±30% 초과) 분포 → 액면분할/이벤트 검출 단서.
- 시가/고가/저가/종가 일관성: `low <= min(open,close) AND high >= max(open,close)`.
- 시장(KOSPI/KOSDAQ/KONEX)별 행수 및 종목 수.

### 4.2. `dart_xbrl_fact_raw` (최대 규모 — 샘플링 전략 필수)
- **샘플링**: 전체 통계는 `TABLESAMPLE SYSTEM (1)` 또는 `WHERE rcept_no IN (...샘플...)` 로 진행하고 결과에 표기.
- `concept_id` / `element_id` 의 distinct 수 및 Top-50 빈도.
- 단위(`unit_ref`)·통화·decimals 분포.
- context: 기간(duration/instant) 비율, `period_start/end` 의 분포.
- corp/연도/보고서 별 fact 수 분포(편향 진단).
- `dart_xbrl_document` 와의 join 정합성(고아 fact 비율).

### 4.3. `dart_xbrl_document`
- 보고서 유형/연도/분기 분포 및 corp 커버리지.
- `dart_financial_statement_raw.rcept_no` 와의 교집합/차집합.
- 동일 corp/보고서의 재공시(개정) 횟수.

### 4.4. `dart_shareholder_return_raw`
- 이벤트 유형(배당/자사주취득/소각/처분 등) 코드 분포.
- corp/연도별 이벤트 수, 금액 분포 / 통화.
- 자연 키 중복 및 동일 이벤트 재공시 패턴.

### 4.5. `dart_share_count_raw`
- 주식수 변동 사유 코드 분포.
- corp 당 행수 분포(자본금 변동 빈도).
- `발행주식총수` 시계열 연속성 검증(전월 대비 jump 탐지).

### 4.6. `dart_corp_master`
- 상장/비상장 비율, `stock_code` NULL 비율(상장사 식별 가능 비율).
- 시장 구분 분포, `modify_date` 분포.
- `stock_master` 와의 join 일치율.

### 4.7. `stock_master` / `stock_master_snapshot` / `stock_master_snapshot_items`
- 시장/섹터 분포, 우선주/보통주 비율.
- 스냅샷 일자 간격, 두 스냅샷 간 종목 추가/제거 수.
- snapshot_items 와 dart_corp_master 의 corp_code 매핑 커버리지.

### 4.8. `stock_metric_fact`
- 메트릭 종류(`metric_code`) 분포, 종목/일자 커버리지.
- 메트릭별 값 분포(분위수), NULL/0 비율.
- 동일 (ticker, date, metric_code) 중복 검사.

### 4.9. 미적재 테이블 (Wave 4)
- `krx_security_flow_raw`, `operating_source_document`, `operating_metric_fact` 는 적재 후 §3 전 항목 + 다음을 추가:
  - flow: 투자자 구분 코드 분포, 매수/매도/순매수 합계 검증.
  - operating_*: source_document 와 metric_fact 의 외래키 정합성, 메트릭 코드 분포.

### 4.10. 운영/설정 테이블 (Wave 3 — 경량)
- `metric_catalog`, `metric_mapping_rule`: 행수, distinct key, 매핑 규칙이 참조하는 코드의 유효성.
- `ingestion_runs`: job 별 성공/실패 비율, 평균 소요시간, 최근 실행.
- `sync_checkpoints`: 채널별 최신 cursor, stale 여부.

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

---

## 6. 실행 절차 (Runbook)

각 테이블 진행 시 동일하게 반복:

1. **스키마 확인**
   ```sql
   SELECT column_name, data_type, is_nullable
   FROM information_schema.columns
   WHERE table_name='<TABLE>' ORDER BY ordinal_position;
   ```
2. **자연 키 후보 식별** — DDL(`sql/postgres_ddl.sql`) 의 `PRIMARY KEY` / `UNIQUE` 제약 우선 사용.
3. **§3 공통 SQL 체크리스트** 중 해당 컬럼이 있는 항목만 선택해 실행.
4. **§4 도메인 특화 SQL** 실행.
5. **대규모 테이블(>1억 행 또는 매우 느린 쿼리)** 은 `TABLESAMPLE SYSTEM (1)` 또는 1~2개 corp/연도 subset 으로 검증 후 본실행 — 결과에 “샘플링 여부 명시”.
6. 결과를 §5 포맷에 맞춰 마크다운으로 정리, 커밋.

### 6.1. 표준 실행 환경
- `.venv/bin/python` + `psycopg2` (2.9.x 설치 확인됨).
- DSN: `.env` 의 `DB_DSN`.
- 임시 스크립트는 `/tmp/` 에서 실행 후 삭제(이미 적용된 관행 유지).

---

## 7. 일정 및 체크리스트

진행할 때마다 아래 체크박스를 갱신한다.

- [x] `dart_financial_statement_raw`
- [x] `daily_ohlcv` *(Wave 1)* — [`statistical_profile_daily_ohlcv.md`](./statistical_profile_daily_ohlcv.md)
- [x] `dart_xbrl_fact_raw` *(Wave 1, 대용량)* — [`statistical_profile_dart_xbrl_fact_raw.md`](./statistical_profile_dart_xbrl_fact_raw.md)
- [x] `dart_xbrl_document` *(Wave 1)* — [`statistical_profile_dart_xbrl_document.md`](./statistical_profile_dart_xbrl_document.md)
- [x] `dart_shareholder_return_raw` *(Wave 1)* — [`statistical_profile_dart_shareholder_return_raw.md`](./statistical_profile_dart_shareholder_return_raw.md)
- [x] `dart_share_count_raw` *(Wave 1)* — [`statistical_profile_dart_share_count_raw.md`](./statistical_profile_dart_share_count_raw.md)
- [x] `stock_metric_fact` *(Wave 1)* — [`statistical_profile_stock_metric_fact.md`](./statistical_profile_stock_metric_fact.md)
- [ ] `stock_master` *(Wave 2)*
- [ ] `stock_master_snapshot` *(Wave 2)*
- [ ] `stock_master_snapshot_items` *(Wave 2)*
- [ ] `dart_corp_master` *(Wave 2)*
- [ ] `metric_catalog` *(Wave 3, 경량)*
- [ ] `metric_mapping_rule` *(Wave 3, 경량)*
- [ ] `ingestion_runs` *(Wave 3, 경량)*
- [ ] `sync_checkpoints` *(Wave 3, 경량)*
- [x] `krx_security_flow_raw` *(Wave 4)* — [`statistical_profile_krx_security_flow_raw.md`](./statistical_profile_krx_security_flow_raw.md)
- [ ] `operating_source_document` *(Wave 4, 적재 후)*
- [ ] `operating_metric_fact` *(Wave 4, 적재 후)*

---

## 8. 산출물의 활용

- 본 시리즈 문서가 누적되면 → 모델링 피처 카탈로그 작성(`docs/features/feature_catalog.md`) 의 1차 입력으로 사용.
- 결측·중복·이상치 패턴은 곧바로 데이터 품질 모니터링 SQL 로 재활용(예: `ingestion_runs` 검증 단계에 삽입).
