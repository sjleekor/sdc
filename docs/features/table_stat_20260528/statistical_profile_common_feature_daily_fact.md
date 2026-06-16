# `common_feature_daily_fact` 통계적 특성 프로파일

- 작성 일시: 2026-06-15
- 대상 DB: 로컬 PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 확인 방법: `sdc-db` helper로 로컬 DB read-only 집계. 현재 `sj2-server` 접근 불가.
- 적재 규모: **5,550 행** / **37 active feature_code** / **150 feature_date** / feature date **2025-11-03 ~ 2026-06-12**
- 참고: [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트 + §4.10 `common_feature_*` 특화 항목 적용.

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| `feature_date` | date | NO | KRX-session-aligned 날짜, PK |
| `feature_code` | text | NO | FK -> `common_feature_catalog.feature_code`, PK |
| `value_numeric` | numeric | YES | 모델 입력 수치값 |
| `value_text` | text | NO | 모델 입력 텍스트값 |
| `unit` | text | NO | 모델 단위 |
| `source_series_ids` | jsonb | NO | 원천 series 목록 |
| `source_observation_ids` | jsonb | NO | 참조 raw observation id 목록 |
| `asof_available_date` | date | NO | 선택된 원천값의 사용 가능 기준일 |
| `selected_vintage` | text | NO | 선택 vintage |
| `generated_at` | timestamptz | NO | 생성 시각 |
| `generation_run_id` | uuid | YES | 생성 run id |

- PK: `(feature_date, feature_code)`
- FK: `feature_code` -> `common_feature_catalog(feature_code)`
- 주요 인덱스: `(feature_code, feature_date DESC)`, `(generated_at, feature_date, feature_code)`

---

## 1. 핵심 결론

- **규모/범위**: 5,550행 = 37개 active feature x 150개 feature_date. 날짜 범위는 2025-11-03 ~ 2026-06-12.
- **카테고리 구성**: 11개 active category. 큰 축은 market_breadth 6개, market_index 6개, rate 6개, macro_price 6개 feature이며, 나머지는 commodity/fx/global/macro/liquidity 계열이다.
- **무결성**: PK 중복 0건, `feature_code` FK 고아 0건, inactive feature에 생성된 fact 0건, `generation_run_id` NULL 0건.
- **PIT 검증 통과**: `source_observation_ids`가 가리키는 raw reference 6,965건 중 missing raw 0건, `available_from_date > feature_date` 위반 0건.
- **as-of 검증 통과**: `asof_available_date > feature_date` 0건. `feature_date - asof_available_date`는 p50 0일, p95 24일, max 53일.
- **초기 NULL 구간 존재**: `value_numeric` NULL은 913행(16.45%). null 발생일은 2025-11-03 ~ 2026-01-15에만 있고, 2025-12-15 이전 31개 feature_date는 일간 raw 미가용으로 27개 feature가 매일 NULL이다.
- **raw reference 빈 배열**: `source_observation_ids=[]`가 837행. 이것도 주로 초기 일간 raw 미가용 구간과 수익률 lookback 부족 구간이다. 월간 macro 계열은 빈 raw reference가 없다.
- **KRX 거래일 정렬 주의**: feature_date 150일 중 147일은 `daily_ohlcv` 거래일과 겹친다. `daily_ohlcv` 구간에서 feature가 누락된 거래일은 0개지만, common feature에만 있는 날짜가 3개 있다: 2025-12-31, 2026-06-11, 2026-06-12.
- **장기 학습 한계**: 현재 daily fact는 2025-11-03 이후 150일만 존재한다. 공통 피처가 실제 모델 공변량이 되려면 2015년 이후 학습 구간까지 raw/backfill/build를 확장해야 한다.

---

## 2. 데이터 특성 조사용 SQL 모음

### C1. 규모 / 날짜 범위

```sql
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT feature_code) AS features,
       COUNT(DISTINCT feature_date) AS feature_dates,
       MIN(feature_date) AS min_feature_date,
       MAX(feature_date) AS max_feature_date,
       MIN(asof_available_date) AS min_asof_available_date,
       MAX(asof_available_date) AS max_asof_available_date,
       MIN(generated_at) AS min_generated_at,
       MAX(generated_at) AS max_generated_at
FROM common_feature_daily_fact;
```

### C2. category / active 분포

```sql
SELECT c.category, COUNT(DISTINCT f.feature_code) AS features,
       COUNT(*) AS rows, MIN(f.feature_date), MAX(f.feature_date)
FROM common_feature_daily_fact f
JOIN common_feature_catalog c USING (feature_code)
GROUP BY c.category;

SELECT c.active, COUNT(DISTINCT f.feature_code) AS features, COUNT(*) AS rows
FROM common_feature_daily_fact f
JOIN common_feature_catalog c USING (feature_code)
GROUP BY c.active;
```

### C3. 품질 / 중복 / FK

```sql
SELECT COUNT(*) AS rows,
       SUM((value_numeric IS NULL)::int) AS null_value_numeric,
       SUM((value_text='')::int) AS empty_value_text,
       SUM((unit='')::int) AS empty_unit,
       SUM((jsonb_typeof(source_series_ids) <> 'array')::int) AS non_array_source_series_ids,
       SUM((jsonb_typeof(source_observation_ids) <> 'array')::int) AS non_array_source_observation_ids,
       SUM((jsonb_array_length(source_series_ids)=0)::int) AS empty_source_series_ids,
       SUM((jsonb_array_length(source_observation_ids)=0)::int) AS empty_source_observation_ids,
       SUM((generation_run_id IS NULL)::int) AS null_generation_run_id
FROM common_feature_daily_fact;

SELECT COUNT(*) AS duplicate_groups
FROM (
  SELECT feature_date, feature_code
  FROM common_feature_daily_fact
  GROUP BY 1,2
  HAVING COUNT(*) > 1
) d;

SELECT COUNT(*) AS orphan_feature_rows
FROM common_feature_daily_fact f
LEFT JOIN common_feature_catalog c USING (feature_code)
WHERE c.feature_code IS NULL;
```

### C4. PIT raw reference 검증

```sql
WITH refs AS (
  SELECT f.feature_date,
         f.feature_code,
         jsonb_array_elements_text(f.source_observation_ids)::bigint AS raw_id
  FROM common_feature_daily_fact f
), joined AS (
  SELECT r.feature_date, r.feature_code, r.raw_id,
         o.available_from_date, o.series_id
  FROM refs r
  LEFT JOIN common_feature_observation_raw o USING (raw_id)
)
SELECT COUNT(*) AS raw_refs,
       COUNT(*) FILTER (WHERE series_id IS NULL) AS missing_raw_refs,
       COUNT(*) FILTER (WHERE available_from_date > feature_date) AS pit_violations,
       MIN(feature_date - available_from_date) AS min_ref_age_days,
       MAX(feature_date - available_from_date) AS max_ref_age_days,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY feature_date - available_from_date) AS p50_ref_age_days,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY feature_date - available_from_date) AS p95_ref_age_days
FROM joined;
```

### C5. KRX 거래일 정렬

```sql
WITH d AS (SELECT DISTINCT feature_date FROM common_feature_daily_fact),
     o AS (
       SELECT DISTINCT trade_date
       FROM daily_ohlcv
       WHERE trade_date BETWEEN (SELECT MIN(feature_date) FROM d)
                            AND (SELECT MAX(feature_date) FROM d)
     )
SELECT (SELECT COUNT(*) FROM d) AS feature_dates,
       (SELECT COUNT(*) FROM o) AS ohlcv_dates_in_range,
       (SELECT COUNT(*) FROM d JOIN o ON d.feature_date=o.trade_date) AS common_dates,
       (SELECT COUNT(*) FROM d LEFT JOIN o ON d.feature_date=o.trade_date WHERE o.trade_date IS NULL) AS feature_dates_not_in_ohlcv,
       (SELECT COUNT(*) FROM o LEFT JOIN d ON d.feature_date=o.trade_date WHERE d.feature_date IS NULL) AS ohlcv_dates_missing_feature;
```

---

## 3. 실제 실행 결과

### 3.1 규모 / 범위

| rows | features | feature_dates | min feature_date | max feature_date | min asof | max asof |
|---:|---:|---:|---|---|---|---|
| 5,550 | 37 | 150 | 2025-11-03 | 2026-06-12 | 2025-10-20 | 2026-06-12 |

- `generated_at`: 2026-06-11 15:27:55 UTC ~ 2026-06-14 08:22:06 UTC
- 생성 run 수: 3개
  - 2026-06-11 run: 2,627행, 2025-11-03 ~ 2026-02-11
  - 2026-06-11 run: 74행, 2026-02-12 ~ 2026-02-13
  - 2026-06-14 run: 2,849행, 2026-02-19 ~ 2026-06-12

### 3.2 category 분포

| category | features | rows | min feature_date | max feature_date |
|---|---:|---:|---|---|
| commodity | 2 | 300 | 2025-11-03 | 2026-06-12 |
| fx | 2 | 300 | 2025-11-03 | 2026-06-12 |
| global_index | 2 | 300 | 2025-11-03 | 2026-06-12 |
| global_risk | 1 | 150 | 2025-11-03 | 2026-06-12 |
| macro_money | 3 | 450 | 2025-11-03 | 2026-06-12 |
| macro_price | 6 | 900 | 2025-11-03 | 2026-06-12 |
| macro_sentiment | 1 | 150 | 2025-11-03 | 2026-06-12 |
| market_breadth | 6 | 900 | 2025-11-03 | 2026-06-12 |
| market_index | 6 | 900 | 2025-11-03 | 2026-06-12 |
| market_liquidity | 2 | 300 | 2025-11-03 | 2026-06-12 |
| rate | 6 | 900 | 2025-11-03 | 2026-06-12 |

모든 fact row는 active feature에만 연결된다.

### 3.3 품질 결과

| 항목 | 값 |
|---|---:|
| PK duplicate groups | 0 |
| FK orphan rows vs `common_feature_catalog` | 0 |
| `value_numeric` NULL | 913 |
| `value_text=''` | 5,550 |
| `unit=''` | 0 |
| non-array `source_series_ids` | 0 |
| non-array `source_observation_ids` | 0 |
| empty `source_series_ids` | 0 |
| empty `source_observation_ids` | 837 |
| `generation_run_id IS NULL` | 0 |

### 3.4 PIT / as-of 검증

| raw_refs | missing_raw_refs | pit_violations | min_ref_age_days | max_ref_age_days | p50_ref_age_days | p95_ref_age_days |
|---:|---:|---:|---:|---:|---:|---:|
| 6,965 | 0 | 0 | 0 | 417 | 0 | 372 |

`max_ref_age_days`와 높은 p95는 `mom`/`yoy`처럼 오래된 비교 observation을 함께 참조하는 파생 feature 때문이다. 핵심 PIT 조건인 `raw.available_from_date <= feature_date` 위반은 0건이다.

| asof_after_feature_date | min_age_days | max_age_days | p50_age_days | p95_age_days |
|---:|---:|---:|---:|---:|
| 0 | 0 | 53 | 0 | 24 |

### 3.5 feature별 NULL / raw reference 상태

NULL은 일간 raw 시작 전 또는 수익률 lookback 부족 구간에서 집중된다.

| feature_code | rows | null_numeric | empty_observation_refs | max age days |
|---|---:|---:|---:|---:|
| `market_kospi_ret_20d` | 150 | 52 | 31 | 2 |
| `commodity_wti_ret_20d` | 150 | 51 | 31 | 3 |
| `commodity_wti_spot_ret_20d` | 150 | 51 | 31 | 3 |
| `fx_usdkrw_ret_5d` | 150 | 36 | 31 | 0 |
| `market_kospi_ret_5d` | 150 | 36 | 31 | 2 |
| `global_nasdaq_ret_1d` | 150 | 32 | 31 | 3 |
| `global_sp500_ret_1d` | 150 | 32 | 31 | 3 |
| `market_kosdaq_ret_1d` | 150 | 32 | 31 | 2 |
| `market_kospi200_ret_1d` | 150 | 32 | 31 | 2 |
| `market_kospi_ret_1d` | 150 | 32 | 31 | 2 |

31건 NULL이 반복되는 일간 level/count 계열은 2025-11-03 ~ 2025-12-15 구간에 raw observation이 아직 없기 때문이다. 월간 macro 계열 10개 feature는 `null_numeric=0`, `empty_observation_refs=0`이다.

### 3.6 category별 stale/as-of 특성

| category | rows | null_numeric | empty refs | max age days | p50 age days | p95 age days |
|---|---:|---:|---:|---:|---:|---:|
| commodity | 300 | 102 | 62 | 3 | 0 | 0 |
| fx | 300 | 67 | 62 | 0 | 0 | 0 |
| global_index | 300 | 64 | 62 | 3 | 0 | 0 |
| global_risk | 150 | 31 | 31 | 3 | 0 | 0 |
| macro_money | 450 | 0 | 0 | 53 | 17 | 43 |
| macro_price | 900 | 0 | 0 | 30 | 15 | 28 |
| macro_sentiment | 150 | 0 | 0 | 30 | 15 | 28 |
| market_breadth | 900 | 186 | 186 | 0 | 0 | 0 |
| market_index | 900 | 215 | 186 | 2 | 0 | 0 |
| market_liquidity | 300 | 62 | 62 | 0 | 0 | 0 |
| rate | 900 | 186 | 186 | 1 | 0 | 0 |

월간 macro 계열은 forward-fill 특성상 age가 길다. `macro_m2_*`는 max 53일, `macro_cpi/ppi/consumer_sentiment`는 max 30일이다.

### 3.7 raw/series reference 배열

| observation_ref_count | rows |
|---:|---:|
| 0 | 837 |
| 1 | 2,461 |
| 2 | 2,252 |

| series_ref_count | rows |
|---:|---:|
| 1 | 5,250 |
| 2 | 300 |

2개 series를 참조하는 300행은 term spread 2개 feature(`rate_kr_term_spread_10y_3y`, `rate_us_term_spread_10y_2y`)다.

### 3.8 `daily_ohlcv` 거래일 정렬

| feature_dates | ohlcv_dates_in_range | common_dates | feature_dates_not_in_ohlcv | ohlcv_dates_missing_feature |
|---:|---:|---:|---:|---:|
| 150 | 147 | 147 | 3 | 0 |

common feature에만 있는 날짜:

| feature_date |
|---|
| 2025-12-31 |
| 2026-06-11 |
| 2026-06-12 |

2026-06-11, 2026-06-12는 로컬 `daily_ohlcv` 최신일이 2026-06-10인 영향이다. 2025-12-31은 KRX holiday/연말 휴장 가능성이 있어 캘린더 소스 점검이 필요하다.

---

## 4. 모델링 시사점 / 후속 조치

- PIT 조건은 현재 통과한다. `source_observation_ids` raw 참조가 모두 존재하고, 참조 raw의 `available_from_date`가 feature date보다 늦은 사례가 없다.
- 초기 2025-11-03 ~ 2025-12-15 구간은 일간 common raw가 없어서 많은 feature가 NULL이다. 학습 데이터 생성 시 해당 구간을 제외하거나 feature별 valid-start를 적용해야 한다.
- 수익률 feature는 lookback window만큼 추가 NULL이 생긴다. `ret_20d`는 최소 20영업일 warm-up을 모델 feature catalog에 명시해야 한다.
- 월간 macro feature는 NULL 없이 채워져 있지만 stale age가 30~53일까지 간다. `max_stale_business_days` 정책과 실제 forward-fill age를 feature gate로 검증하는 테스트가 필요하다.
- `feature_date`가 `daily_ohlcv`와 완전히 일치하지 않는다. 모델 panel join 전에 KRX 거래일 캘린더 단일화가 필요하며, 특히 2025-12-31과 로컬 OHLCV 최신일 지연을 분리해서 처리해야 한다.
- 장기 모델링을 위해서는 common raw/daily fact를 최소 2015년 이후로 백필하는 것이 다음 데이터 작업의 핵심이다.
