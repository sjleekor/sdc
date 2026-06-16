# `common_feature_observation_raw` 통계적 특성 프로파일

- 작성 일시: 2026-06-15
- 대상 DB: 로컬 PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 확인 방법: `sdc-db` helper로 로컬 DB read-only 집계. 현재 `sj2-server` 접근 불가.
- 적재 규모: **2,752 행** / **26 active series** / **4 source** / observation date **2024-09-30 ~ 2026-06-12**
- 참고: [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트 + §4.10 `common_feature_*` 특화 항목 적용.

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| `raw_id` | bigint | NO | PK, BIGSERIAL |
| `source` | text | NO | ECOS/FDR/FRED/KRX |
| `series_id` | text | NO | FK -> `common_feature_series.series_id`, UQ |
| `observation_date` | date | NO | 원천 observation 기준일, UQ |
| `period_end_date` | date | YES | 월간/기간형 지표의 기간 종료일, UQ |
| `release_date` | date | YES | 발표일, UQ |
| `available_from_date` | date | NO | 모델 사용 가능 시작일 |
| `vintage` | text | NO | revision/vintage 식별자, UQ |
| `value_numeric` | numeric | YES | 원천 숫자값 |
| `value_text` | text | NO | 원천 텍스트값 |
| `unit` | text | NO | 원천 단위 |
| `frequency` | text | NO | D/M |
| `source_updated_at` | timestamptz | YES | 원천 수정시각 |
| `fetched_at` | timestamptz | NO | 수집 시각 |
| `raw_payload` | jsonb | NO | 원본 응답 |

- PK: `raw_id`
- UNIQUE: `(source, series_id, observation_date, period_end_date, release_date, vintage)` with `NULLS NOT DISTINCT`
- FK: `series_id` -> `common_feature_series(series_id)`
- 주요 인덱스: `(series_id, available_from_date DESC, observation_date DESC)`, `(series_id, observation_date DESC)`, `(fetched_at, raw_id)`

---

## 1. 핵심 결론

- **규모/범위**: 2,752행, 26개 active series, 4개 source. `observation_date`는 2024-09-30 ~ 2026-06-12, `available_from_date`는 2024-10-21 ~ 2026-06-22.
- **source 구성**: KRX 1,317행/11 series, FDR 626행/5 series, ECOS 441행/7 series, FRED 368행/3 series.
- **빈도 구성**: 일간(D) 2,671행/22 series, 월간(M) 81행/4 series. 월간은 ECOS macro 계열이다.
- **무결성**: 자연키 중복 0건, `series_id` FK 고아 0건, inactive series에 적재된 raw row 0건.
- **값 품질**: `value_numeric` NULL 0건, `raw_payload` NULL 0건, `raw_payload`는 전부 JSON object. `value_text`, `vintage`, `source_updated_at`은 현재 전부 비어 있거나 NULL이다.
- **PIT 가용성**: `available_from_date < observation_date` 0건. `release_date`가 전부 NULL이라 release 기반 검증은 아직 불가하지만, 현재 정책상 `available_from_date`는 observation date보다 보수적으로 1~22일 뒤다.
- **가용성 lag**: 일간 source는 대부분 다음 KRX 세션 정책으로 p50 1일, p95 3일. ECOS 월간 지표는 manual lag 20일 정책으로 p95 20일, max 22일.
- **모델링 한계**: 일간 공통 피처 raw는 대부분 2025-12-15부터 시작한다. 장기 학습에 쓰려면 공통 피처 과거 백필 범위를 `daily_ohlcv`/`krx_security_flow_raw`의 2015년 이후 학습 구간까지 확장해야 한다.
- **revision 한계**: `release_date`, `vintage`, `source_updated_at`이 비어 있어 현재는 revised macro series의 vintage-aware 학습이 아니라 latest-value 기반 PIT lag 모델이다.

---

## 2. 데이터 특성 조사용 SQL 모음

### C1. 규모 / 날짜 범위

```sql
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT source) AS sources,
       COUNT(DISTINCT series_id) AS series,
       COUNT(DISTINCT observation_date) AS observation_dates,
       MIN(observation_date) AS min_observation_date,
       MAX(observation_date) AS max_observation_date,
       MIN(available_from_date) AS min_available_from_date,
       MAX(available_from_date) AS max_available_from_date,
       MIN(fetched_at) AS min_fetched_at,
       MAX(fetched_at) AS max_fetched_at
FROM common_feature_observation_raw;
```

### C2. source/category/series 커버리지

```sql
SELECT source, COUNT(*) AS rows, COUNT(DISTINCT series_id) AS series,
       MIN(observation_date), MAX(observation_date),
       MIN(available_from_date), MAX(available_from_date)
FROM common_feature_observation_raw
GROUP BY source;

SELECT s.category, o.source, COUNT(*) AS rows, COUNT(DISTINCT o.series_id) AS series,
       MIN(o.observation_date), MAX(o.observation_date)
FROM common_feature_observation_raw o
JOIN common_feature_series s USING (series_id)
GROUP BY s.category, o.source;
```

### C3. 품질 / 중복 / FK

```sql
SELECT SUM((value_numeric IS NULL)::int) AS null_value_numeric,
       SUM((value_text='')::int) AS empty_value_text,
       SUM((unit='')::int) AS empty_unit,
       SUM((vintage='')::int) AS empty_vintage,
       SUM((source_updated_at IS NULL)::int) AS null_source_updated_at,
       SUM((jsonb_typeof(raw_payload) <> 'object')::int) AS non_object_payload
FROM common_feature_observation_raw;

SELECT COUNT(*) AS duplicate_groups
FROM (
  SELECT source, series_id, observation_date, period_end_date, release_date, vintage
  FROM common_feature_observation_raw
  GROUP BY 1,2,3,4,5,6
  HAVING COUNT(*) > 1
) d;

SELECT COUNT(*) AS orphan_series_rows
FROM common_feature_observation_raw o
LEFT JOIN common_feature_series s USING (series_id)
WHERE s.series_id IS NULL;
```

### C4. PIT 가용성 lag

```sql
SELECT source,
       COUNT(*) AS rows,
       MIN(available_from_date - observation_date) AS min_delay_days,
       MAX(available_from_date - observation_date) AS max_delay_days,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY available_from_date - observation_date) AS p50_delay_days,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY available_from_date - observation_date) AS p95_delay_days,
       COUNT(*) FILTER (WHERE available_from_date < observation_date) AS available_before_observation
FROM common_feature_observation_raw
GROUP BY source;
```

---

## 3. 실제 실행 결과

### 3.1 규모 / 범위

| rows | sources | series | observation_dates | min_observation_date | max_observation_date | min_available_from_date | max_available_from_date |
|---:|---:|---:|---:|---|---|---|---|
| 2,752 | 4 | 26 | 156 | 2024-09-30 | 2026-06-12 | 2024-10-21 | 2026-06-22 |

- `fetched_at`: 2026-06-11 14:57:10 UTC ~ 2026-06-14 08:21:10 UTC

### 3.2 source 분포

| source | rows | series | min observation | max observation | min available | max available |
|---|---:|---:|---|---|---|---|
| KRX | 1,317 | 11 | 2025-12-15 | 2026-06-12 | 2025-12-16 | 2026-06-15 |
| FDR | 626 | 5 | 2025-12-15 | 2026-06-12 | 2025-12-16 | 2026-06-15 |
| ECOS | 441 | 7 | 2024-09-30 | 2026-06-12 | 2024-10-21 | 2026-06-22 |
| FRED | 368 | 3 | 2025-12-15 | 2026-06-11 | 2025-12-16 | 2026-06-12 |

### 3.3 category x source 분포

| category | source | rows | series | min date | max date |
|---|---|---:|---:|---|---|
| market_breadth | KRX | 720 | 6 | 2025-12-15 | 2026-06-12 |
| market_index | KRX | 357 | 3 | 2025-12-15 | 2026-06-12 |
| global_index | FDR | 248 | 2 | 2025-12-15 | 2026-06-12 |
| rate | FRED | 248 | 2 | 2025-12-15 | 2026-06-11 |
| market_liquidity | KRX | 240 | 2 | 2025-12-15 | 2026-06-12 |
| rate | ECOS | 240 | 2 | 2025-12-15 | 2026-06-12 |
| fx | FDR | 129 | 1 | 2025-12-15 | 2026-06-12 |
| global_risk | FDR | 125 | 1 | 2025-12-15 | 2026-06-12 |
| commodity | FDR | 124 | 1 | 2025-12-15 | 2026-06-12 |
| commodity | FRED | 120 | 1 | 2025-12-15 | 2026-06-08 |
| fx | ECOS | 120 | 1 | 2025-12-15 | 2026-06-12 |
| macro_price | ECOS | 41 | 2 | 2024-09-30 | 2026-05-31 |
| macro_sentiment | ECOS | 21 | 1 | 2024-09-30 | 2026-05-31 |
| macro_money | ECOS | 19 | 1 | 2024-09-30 | 2026-03-31 |

### 3.4 series별 커버리지

| source | series_id | rows | min observation | max observation | null numeric |
|---|---|---:|---|---|---:|
| ECOS | `fx_usdkrw_ecos` | 120 | 2025-12-15 | 2026-06-12 | 0 |
| ECOS | `macro_consumer_sentiment` | 21 | 2024-09-30 | 2026-05-31 | 0 |
| ECOS | `macro_cpi` | 21 | 2024-09-30 | 2026-05-31 | 0 |
| ECOS | `macro_m2` | 19 | 2024-09-30 | 2026-03-31 | 0 |
| ECOS | `macro_ppi` | 20 | 2024-09-30 | 2026-04-30 | 0 |
| ECOS | `rate_kr_gov10y` | 120 | 2025-12-15 | 2026-06-12 | 0 |
| ECOS | `rate_kr_gov3y` | 120 | 2025-12-15 | 2026-06-12 | 0 |
| FDR | `commodity_wti` | 124 | 2025-12-15 | 2026-06-12 | 0 |
| FDR | `fx_usdkrw` | 129 | 2025-12-15 | 2026-06-12 | 0 |
| FDR | `global_nasdaq` | 124 | 2025-12-15 | 2026-06-12 | 0 |
| FDR | `global_sp500` | 124 | 2025-12-15 | 2026-06-12 | 0 |
| FDR | `global_vix` | 125 | 2025-12-15 | 2026-06-12 | 0 |
| FRED | `commodity_wti_fred` | 120 | 2025-12-15 | 2026-06-08 | 0 |
| FRED | `rate_us10y` | 124 | 2025-12-15 | 2026-06-11 | 0 |
| FRED | `rate_us2y` | 124 | 2025-12-15 | 2026-06-11 | 0 |
| KRX | breadth/liquidity/index 11 series | 119-120 each | 2025-12-15 | 2026-06-12 | 0 |

KRX index 계열 `market_kospi_krx`, `market_kosdaq_krx`, `market_kospi200_krx`는 119행이고, KRX breadth/liquidity 계열은 각 120행이다.

### 3.5 품질 결과

| 항목 | 값 |
|---|---:|
| 자연키 duplicate groups | 0 |
| FK orphan rows vs `common_feature_series` | 0 |
| inactive series raw rows | 0 |
| `value_numeric` NULL | 0 |
| `value_text=''` | 2,752 |
| `unit=''` | 0 |
| `vintage=''` | 2,752 |
| `source_updated_at IS NULL` | 2,752 |
| non-object `raw_payload` | 0 |

### 3.6 frequency / availability lag

| frequency | rows | series |
|---|---:|---:|
| D | 2,671 | 22 |
| M | 81 | 4 |

| source | rows | min delay | max delay | p50 delay | p95 delay | available before observation |
|---|---:|---:|---:|---:|---:|---:|
| ECOS | 441 | 1 | 22 | 1 | 20 | 0 |
| FDR | 626 | 1 | 6 | 1 | 3 | 0 |
| FRED | 368 | 1 | 6 | 1 | 3 | 0 |
| KRX | 1,317 | 1 | 6 | 1 | 3 | 0 |

### 3.7 단위 분포

| source | unit | rows | series |
|---|---|---:|---:|
| ECOS | 연% | 240 | 2 |
| ECOS | 원 | 120 | 1 |
| ECOS | 2020=100 | 41 | 2 |
| ECOS | index | 21 | 1 |
| ECOS | 십억원 | 19 | 1 |
| FDR | index_point | 373 | 3 |
| FDR | KRW | 129 | 1 |
| FDR | USD/bbl | 124 | 1 |
| FRED | pct | 248 | 2 |
| FRED | USD/bbl | 120 | 1 |
| KRX | count | 720 | 6 |
| KRX | index_point | 357 | 3 |
| KRX | KRW | 240 | 2 |

---

## 4. 모델링 시사점 / 후속 조치

- `common_feature_observation_raw` 자체의 키/값 품질은 양호하다. 중복, FK 고아, 숫자 NULL, payload 구조 오류가 없다.
- PIT 관점에서는 `available_from_date`가 observation date보다 빠른 row가 없으므로 기본 가용성 정책은 보수적으로 동작한다.
- 다만 `release_date`와 `vintage`가 아직 비어 있어, 월간 macro 지표 revision을 엄밀히 재현하는 수준은 아니다. 현재는 manual lag를 둔 latest observation 기반으로 봐야 한다.
- 일간 source 대부분이 2025-12-15 이후부터 시작하므로, 2015년 이후 학습 패널에 붙이려면 common feature raw 백필 기간을 크게 늘려야 한다.
- ECOS 월간 지표는 `available_from_date`가 20일 lag 정책을 반영한다. daily fact에서 forward-fill 기간과 `max_stale_business_days` 정책이 과도하게 길어지는지 별도 검증해야 한다.
