# `dart_xbrl_fact_raw` 통계적 특성 프로파일

- 작성 일시: 2026-05-28
- 대상 DB: PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 적재 규모: **18,696,562 행** (현 시점 본 프로젝트 최대 테이블)
- 참고: 본 문서는 [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트(C1~C12) + §4.2 특화 항목(D1~D6, E1~E2) 을 동일 절차로 적용한 결과이다.

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL |
|---|---|---|
| raw_id | bigint | NO (PK, BIGSERIAL) |
| corp_code | text | NO |
| ticker | text | YES |
| bsns_year | integer | NO |
| reprt_code | text | NO |
| rcept_no | text | NO |
| concept_id | text | NO |
| concept_name | text | NO (default `''`) |
| namespace_uri | text | NO (default `''`) |
| context_id | text | NO (default `''`) |
| context_type | text | NO (default `''`) |
| period_start | date | YES |
| period_end | date | YES |
| instant_date | date | YES |
| dimensions | jsonb | NO (default `'[]'::jsonb`) |
| unit_id | text | NO (default `''`) |
| unit_measure | text | NO (default `''`) |
| decimals | text | NO (default `''`) |
| value_numeric | numeric(30,4) | YES |
| value_text | text | NO (default `''`) |
| is_nil | boolean | NO (default FALSE) |
| label_ko | text | NO (default `''`) |
| source | text | NO |
| fetched_at | timestamptz | NO |
| raw_payload | jsonb | NO |

- 자연 키(UNIQUE 제약 `uq_dart_xbrl_fact_raw`): `(corp_code, bsns_year, reprt_code, rcept_no, context_id, concept_id)`
- 보조 인덱스 `ix_dart_xbrl_fact_raw_lookup` 로 일반 조회 보장

---

## 1. 핵심 결론 (Executive Summary)

- **규모**: 1,869만 행 / **2,140 기업** / **8,255 보고서(rcept_no)** / 사업연도 **2025 단일**.
- **자연키 무결성 완벽**: `(corp_code, bsns_year, reprt_code, rcept_no, context_id, concept_id)` 중복 0건.
- **문서 정합성 완벽**: `dart_xbrl_document` 와의 LEFT JOIN orphan 0건. 또한 `dart_xbrl_document` 의 모든 rcept_no(8,255건) 가 `dart_financial_statement_raw` 의 rcept_no(10,370건) 부분집합.
- **수치 가용성 우수**: `is_nil=FALSE` 100%, `value_numeric` 보유율 **90.97%** — 즉 약 9% 만 텍스트/비수치 fact.
- **시간 정보**: `instant`(63.8%) / `duration`(36.2%) 두 종류만, `no_period` 0.
- **단위 분포**: 약 85% `iso4217:KRW` 또는 비어있음(unit_measure 빈문자열 9.5%). 외화 사례 다수 — 다국어/해외법인 통합.
- **개념(concept) 풍부**: TABLESAMPLE 5% 추정 약 64K, 분기보고서(11013) subset 만으로도 201,377 distinct concept 노출 — 실체 distinct 는 수십~수백 K 규모로 매우 sparse 한 long-format.
- **수치 스케일 극단**: TABLESAMPLE 1% 기준 최소 -1.43e16, 최대 6.35e16 (≈ 자산총계 등 원화 raw). p25=0, p50≈2.5억, p75=100억 → 좌측편향, fat-tail. 정규화(로그 + 단위/통화별 분리) 필수.
- **시계열 한계**: bsns_year=2025 단일. `dart_financial_statement_raw` 와 동일하게 연도 백필 전에는 시계열 모델 학습 불가.
- **차원(dimensions)**: 99.4% 가 비어있지 않은 JSONB(평균 길이 ~2). 세그먼트/연결-별도 등 다축 정보가 풍부 → 단순 `concept_id` 만으로는 동일 보고서 내 fact 식별 불가, dimensions 까지 포함한 키 정규화 필요.

---

## 2. 데이터 특성 조사용 SQL 모음

> 18M+ 테이블이므로 `COUNT(DISTINCT col)` 등 sort-heavy 쿼리는 PG 임시디스크가 부족할 수 있다.
> 실행 전 다음을 권장:
> ```sql
> SET work_mem='256MB';
> SET max_parallel_workers_per_gather=0;
> ```
> 또한 거대한 distinct 집계는 `TABLESAMPLE SYSTEM (n)` 으로 근사한다.

### C1. 총 행수 / 유일 키 수 / 시간 범위

```sql
-- (분할 실행 권장)
SELECT COUNT(*) FROM dart_xbrl_fact_raw;
SELECT COUNT(DISTINCT corp_code) FROM dart_xbrl_fact_raw;
SELECT COUNT(DISTINCT rcept_no)  FROM dart_xbrl_fact_raw;
SELECT COUNT(DISTINCT concept_id) FROM dart_xbrl_fact_raw TABLESAMPLE SYSTEM (5);  -- 근사
SELECT MIN(fetched_at), MAX(fetched_at), MIN(bsns_year), MAX(bsns_year)
  FROM dart_xbrl_fact_raw;
```

### C2. 사업연도 분포

```sql
SELECT bsns_year, COUNT(*) FROM dart_xbrl_fact_raw GROUP BY bsns_year ORDER BY 1;
WITH t AS (SELECT corp_code, MIN(bsns_year) y FROM dart_xbrl_fact_raw GROUP BY corp_code)
SELECT y, COUNT(*) corps FROM t GROUP BY y ORDER BY y;
```

### C3. 카테고리 컬럼 분포

```sql
SELECT reprt_code,   COUNT(*) c FROM dart_xbrl_fact_raw GROUP BY 1 ORDER BY c DESC;
SELECT context_type, COUNT(*) c FROM dart_xbrl_fact_raw GROUP BY 1 ORDER BY c DESC;
SELECT source,       COUNT(*) c FROM dart_xbrl_fact_raw GROUP BY 1 ORDER BY c DESC;
SELECT is_nil,       COUNT(*) c FROM dart_xbrl_fact_raw GROUP BY 1;
```

### C4. NULL / 빈 문자열 비율

```sql
SELECT
  ROUND(100.0*SUM((ticker IS NULL)::int)/COUNT(*),2)        AS null_ticker,
  ROUND(100.0*SUM((period_start IS NULL)::int)/COUNT(*),2)  AS null_period_start,
  ROUND(100.0*SUM((period_end IS NULL)::int)/COUNT(*),2)    AS null_period_end,
  ROUND(100.0*SUM((instant_date IS NULL)::int)/COUNT(*),2)  AS null_instant_date,
  ROUND(100.0*SUM((value_numeric IS NULL)::int)/COUNT(*),2) AS null_value_numeric,
  ROUND(100.0*SUM((unit_measure='')::int)/COUNT(*),2)       AS empty_unit_measure,
  ROUND(100.0*SUM((decimals='')::int)/COUNT(*),2)           AS empty_decimals,
  ROUND(100.0*SUM((label_ko='')::int)/COUNT(*),2)           AS empty_label_ko
FROM dart_xbrl_fact_raw;
```

### C5. 자연키 중복 검사

```sql
SELECT COUNT(*) dup_groups, COALESCE(SUM(c-1),0) extra_rows FROM (
  SELECT COUNT(*) c FROM dart_xbrl_fact_raw
  GROUP BY corp_code, bsns_year, reprt_code, rcept_no, context_id, concept_id
  HAVING COUNT(*)>1) t;
```

### C6. 엔티티별 행수 분포

```sql
-- 기업당
WITH t AS (SELECT corp_code, COUNT(*) c FROM dart_xbrl_fact_raw GROUP BY corp_code)
SELECT COUNT(*) corps, MIN(c), MAX(c), AVG(c)::numeric(20,1) avg,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY c) p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY c) p95
  FROM t;

-- 보고서(rcept_no) 당
WITH t AS (SELECT rcept_no, COUNT(*) c FROM dart_xbrl_fact_raw GROUP BY rcept_no)
SELECT COUNT(*) docs, MIN(c), MAX(c), AVG(c)::numeric(20,1) avg,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY c) p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY c) p95,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY c) p99
  FROM t;
```

### C7. 연도별 corp / 보고서 커버리지

```sql
WITH t AS (SELECT rcept_no, MIN(bsns_year) y FROM dart_xbrl_fact_raw GROUP BY rcept_no)
SELECT y, COUNT(*) docs FROM t GROUP BY y ORDER BY y;
```

### C8. `value_numeric` 분위수 (샘플링)

```sql
SELECT COUNT(*) sample_n,
       MIN(value_numeric), MAX(value_numeric),
       AVG(value_numeric)::numeric(30,2) avg,
       percentile_cont(0.01) WITHIN GROUP (ORDER BY value_numeric) p01,
       percentile_cont(0.25) WITHIN GROUP (ORDER BY value_numeric) p25,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY value_numeric) p50,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY value_numeric) p75,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY value_numeric) p99
  FROM dart_xbrl_fact_raw TABLESAMPLE SYSTEM (1)
 WHERE value_numeric IS NOT NULL;
```

### C9. 상위 빈도 코드 Top-N

```sql
SELECT concept_id, MAX(concept_name) name, COUNT(*) c
  FROM dart_xbrl_fact_raw GROUP BY 1 ORDER BY c DESC LIMIT 20;
SELECT unit_measure,  COUNT(*) c FROM dart_xbrl_fact_raw GROUP BY 1 ORDER BY c DESC LIMIT 20;
SELECT decimals,      COUNT(*) c FROM dart_xbrl_fact_raw GROUP BY 1 ORDER BY c DESC LIMIT 20;
SELECT namespace_uri, COUNT(*) c FROM dart_xbrl_fact_raw GROUP BY 1 ORDER BY c DESC LIMIT 10;
```

### C10. 적재 시각(`fetched_at`) 월별 추세

```sql
SELECT date_trunc('month', fetched_at)::date m, COUNT(*) c
  FROM dart_xbrl_fact_raw GROUP BY 1 ORDER BY 1;
```

### C12. 외래키 정합성 (vs `dart_xbrl_document`)

```sql
SELECT COUNT(*) facts_total,
       SUM(CASE WHEN d.document_id IS NULL THEN 1 ELSE 0 END) orphan_facts
  FROM dart_xbrl_fact_raw f
  LEFT JOIN dart_xbrl_document d
    ON d.corp_code=f.corp_code AND d.bsns_year=f.bsns_year
   AND d.reprt_code=f.reprt_code AND d.rcept_no=f.rcept_no;
```

### D1~D6. 특화 항목

```sql
-- D1: instant / duration / no_period 분류
SELECT CASE
         WHEN instant_date IS NOT NULL AND period_start IS NULL THEN 'instant'
         WHEN period_start IS NOT NULL AND period_end IS NOT NULL AND instant_date IS NULL THEN 'duration'
         WHEN instant_date IS NULL AND period_start IS NULL AND period_end IS NULL THEN 'no_period'
         ELSE 'mixed' END AS ptype,
       COUNT(*) c
  FROM dart_xbrl_fact_raw GROUP BY 1 ORDER BY c DESC;

-- D2: ticker 커버리지
SELECT COUNT(*) total,
       SUM((ticker IS NULL)::int) null_ticker,
       COUNT(DISTINCT ticker)     distinct_ticker
  FROM dart_xbrl_fact_raw;

-- D3: dimensions 사용 비율
SELECT
  SUM((dimensions = '[]'::jsonb)::int)  empty_dims,
  SUM((dimensions <> '[]'::jsonb)::int) with_dims,
  COUNT(*) total
FROM dart_xbrl_fact_raw;

-- D4: 보고서당 distinct concept (샘플)
WITH s AS (SELECT rcept_no FROM dart_xbrl_document ORDER BY random() LIMIT 200)
SELECT AVG(c)::numeric(10,1) avg_concepts, MIN(c), MAX(c),
       percentile_cont(0.5) WITHIN GROUP (ORDER BY c) p50
  FROM (SELECT f.rcept_no, COUNT(DISTINCT f.concept_id) c
          FROM dart_xbrl_fact_raw f JOIN s USING (rcept_no)
          GROUP BY f.rcept_no) t;

-- D5: is_nil=FALSE 일 때 value_numeric 가용성
SELECT COUNT(*) non_nil,
       SUM((value_numeric IS NOT NULL)::int) has_numeric,
       ROUND(100.0*SUM((value_numeric IS NOT NULL)::int)/COUNT(*),2) pct_numeric
  FROM dart_xbrl_fact_raw WHERE is_nil=FALSE;

-- D6: dart_financial_statement_raw 와의 rcept_no 교집합
WITH x AS (SELECT DISTINCT rcept_no FROM dart_xbrl_fact_raw),
     y AS (SELECT DISTINCT rcept_no FROM dart_financial_statement_raw)
SELECT (SELECT COUNT(*) FROM x) xbrl_rcepts,
       (SELECT COUNT(*) FROM y) fs_rcepts,
       (SELECT COUNT(*) FROM x JOIN y USING(rcept_no)) both;

-- E2: dimensions 배열 길이 분포 (샘플)
SELECT jsonb_array_length(dimensions) dim_len, COUNT(*) c
  FROM dart_xbrl_fact_raw TABLESAMPLE SYSTEM (1)
 GROUP BY 1 ORDER BY 1;
```

---

## 3. SQL 실제 실행 결과 (2026-05-28)

### C1. 행수 / 유일 키 수 / 시간 범위
- `total_rows` = **18,696,562**
- `distinct corp_code` = **2,140**
- `distinct rcept_no` = **8,255**
- `distinct concept_id`: TABLESAMPLE 5% 추정 = **63,942**; reprt_code='11011' subset distinct = **201,377** (실체값은 수십~수백 K 추정)
- `fetched_at` 범위: 2026-04-19 11:05:42 UTC ~ 2026-05-23 17:15:50 UTC
- `bsns_year` 범위: **2025 ~ 2025** (단일)

### C2. 사업연도 분포

| bsns_year | rows | corps |
|---:|---:|---:|
| 2025 | 18,696,562 | 2,140 |

### C3. 카테고리 분포

**reprt_code** (분기보고서 코드)

| reprt_code | rows | 의미 |
|---:|---:|---|
| 11011 | 7,571,935 | 사업보고서 |
| 11014 | 3,967,588 | 3분기보고서 |
| 11012 | 3,847,216 | 반기보고서 |
| 11013 | 3,309,823 | 1분기보고서 |

**context_type**

| context_type | rows |
|---|---:|
| duration | 11,928,886 |
| instant  |  6,767,676 |

**source**: 전량 `OPENDART` (18,696,562)
**is_nil**: 전량 `FALSE`

### C4. NULL / 빈 문자열 비율 (%)

| 컬럼 | 비율 |
|---|---:|
| `ticker` NULL | 0.00 |
| `period_start` NULL | 36.20 |
| `period_end` NULL   | 36.20 |
| `instant_date` NULL | 63.80 |
| `value_numeric` NULL | 9.03 |
| `concept_name` 빈문자열 | 0.00 |
| `namespace_uri` 빈문자열 | 0.00 |
| `context_id` 빈문자열 | 0.00 |
| `context_type` 빈문자열 | 0.00 |
| `unit_id` 빈문자열 | 9.54 |
| `unit_measure` 빈문자열 | 9.54 |
| `decimals` 빈문자열 | 9.54 |
| `value_text` 빈문자열 | 0.00 |
| `label_ko` 빈문자열 | 3.67 |

> `period_start/end` 36.2% NULL 과 `instant_date` 63.8% NULL 은 context_type=instant/duration 비율과 정확히 상보 — 모순 없음.
> `unit_*`/`decimals` 9.54% 빈문자열은 비수치 fact(`value_text` 전용) 와 거의 일치.

### C5. 자연키 중복

- `dup_groups` = **0**, `extra_rows` = **0** → 자연키 무결성 완벽.

### C6. 엔티티별 행수 분포

기업당:

| corps | min | max | avg | p50 | p95 |
|---:|---:|---:|---:|---:|---:|
| 2,140 | 337 | 85,844 | 8,736.7 | 4,011.5 | 27,196.1 |

보고서(rcept_no) 당:

| docs | min | max | avg | p50 | p95 | p99 |
|---:|---:|---:|---:|---:|---:|---:|
| 8,255 | 300 | 36,733 | 2,264.9 | 777 | 7,254.6 | 11,281.4 |

### C7. 연도별 보고서 수

| bsns_year | docs |
|---:|---:|
| 2025 | 8,255 |

### C8. `value_numeric` 분위수 (TABLESAMPLE 1%, n=170,963)

| min | p01 | p25 | p50 | p75 | p99 | max | avg |
|---:|---:|---:|---:|---:|---:|---:|---:|
| -1.43e16 | -5.17e10 | 0 | 2.51e8 | 1.00e10 | 3.44e12 | 6.35e16 | 1.29e12 |

→ 통화 단위(KRW 등) 혼재된 원시값. 모델 입력 전 통화별 분리 + 로그/스케일링 + 윈저라이즈 필수.

### C9. 상위 빈도 코드

**concept_id Top-20**: `ifrs-full_PropertyPlantAndEquipment`(299,424), `ifrs-full_Equity`(266,635), `ifrs-full_FinancialAssets`(242,917), `dart_EquityAtBeginningOfPeriod`(241,724), `ifrs-full_ProfitLoss`(215,435), `ifrs-full_Revenue`(156,559), `…RelatedPartyTransactions` 계열(129,351 / 121,432 / 84,207), `ifrs-full_DeferredTaxLiabilityAsset`(100,827), `ifrs-full_Assets`(99,639), `ifrs-full_IntangibleAssetsAndGoodwill`(98,525), `ifrs-full_BorrowingsInterestRate`(97,180), `ifrs-full_RightofuseAssets`(96,198), `ifrs-full_DepreciationPropertyPlantAndEquipment`(95,345), `ifrs-full_ComprehensiveIncome`(88,423), `…DefinedBenefitPlans`(88,128), `…AdditionsOtherThan…PropertyPlantAndEquipment`(85,660), `ifrs-full_InvestmentProperty`(83,180), `ifrs-full_DividendsPaid`(82,956).

**unit_measure Top-20**: `iso4217:KRW`(15,979,591), `''`(1,783,143), `xbrli:pure`(435,882), `iso4217:KRW / xbrli:shares`(185,575), `iso4217:USD`(126,955), `xbrli:shares`(107,062), `iso4217:CNY/EUR/JPY/VND/GBP/INR/HKD/CAD/IDR/AUD/SGD/BRL/MYR/SEK` 등 외화 다수.

**decimals Top**: `0`(7.12M), `-3`(6.39M), `-6`(2.65M), `''`(1.78M), `INF`(0.73M), 그 외 소수.

**namespace_uri Top-10**: IFRS-full 2021-03-24(10,554,540), DART IFRS 2024-06-30(4,120,556), DART IFRS GCD 2024-06-30(679,346), 개별 entity 별 분기 taxonomy 다수.

### C10. 적재 시각 월별

| month | rows |
|---|---:|
| 2026-04 | 7,565,927 |
| 2026-05 | 11,130,635 |

→ 4월 19일 시작, 5월에 본격 적재 → 현재도 적재 진행중.

### C12. `dart_xbrl_document` 와의 정합성

- `facts_total` = 18,696,562, `orphan_facts` = **0** → 모든 fact 가 문서에 매칭됨.

### D1. period 유형 (파생)

| ptype | rows |
|---|---:|
| duration | 11,928,886 |
| instant  |  6,767,676 |

→ `no_period`/`mixed` 0. context_type 과 동일한 분포.

### D2. ticker 커버리지

- total=18,696,562, null_ticker=**0**, distinct_ticker=**2,140** → 모든 corp 가 상장 ticker 보유 (corp_code 개수와 동일).

### D3. dimensions 사용

| empty_dims | with_dims | total | with_dims % |
|---:|---:|---:|---:|
| 107,268 | 18,589,294 | 18,696,562 | 99.43% |

### D4. 보고서당 distinct concept (랜덤 200 docs)

- avg=**332.1**, min=121, max=1,262, p50=185.

### D5. `is_nil=FALSE` 일 때 `value_numeric` 가용성

- non_nil=18,696,562 (전체), has_numeric=**17,008,982**, **90.97%**.

### D6. `dart_financial_statement_raw` 와의 rcept_no 교집합

| xbrl rcepts | fs rcepts | both |
|---:|---:|---:|
| 8,255 | 10,370 | **8,255** |

→ XBRL 의 rcept_no 는 모두 FS_raw 에 포함. 반대 방향에 2,115건의 rcept_no 는 XBRL 미수집(차분 백필 대상).

### E2. `dimensions` 배열 길이 분포 (TABLESAMPLE 1%, n=186,088)

| len | rows | 비율 |
|---:|---:|---:|
| 0 | 1,117 | 0.6% |
| 1 | 60,863 | 32.7% |
| 2 | 97,636 | 52.5% |
| 3 | 18,128 | 9.7% |
| 4 | 5,040 | 2.7% |
| 5 | 2,383 | 1.3% |
| 6 | 755 | 0.4% |
| 7 | 162 | 0.1% |
| 8 | 4 | 0.0% |

→ 대다수 dimension 길이 1~2 (연결/별도, 사업부문 등의 단일/2축 분류).

---

## 4. 모델링 시사점 / 후속 조치

1. **시계열 미충족 — 백필 우선**
   - `bsns_year=2025` 단일년. `dart_financial_statement_raw` 는 2025/2026 적재되었으나 XBRL 은 아직 2025만 → 과거 연도(2018~2024) XBRL 백필을 진행해야 시계열·전년동기비 등의 파생 피처 생성 가능.
   - `deploy/prod/bin/dart-backfill-all-years.sh` 와 `dart-sync-xbrl.sh` 활용.

2. **fact-level → 와이드 피처화 전략**
   - long-format 1,869만 행 → (corp_code × rcept_no × concept_id × context_id × dimensions-hash) 키로 정규화 후 모델별 와이드피벗 필요.
   - `dimensions` JSONB 가 99.4% 비어있지 않으므로 동일 `(rcept_no, concept_id)` 만으로는 중복 — dimensions 해시를 키에 포함하거나 `dimensions='[]'` 인 totals 행만 추출.

3. **수치 정규화**
   - `value_numeric` 의 p50=2.5억, p99=3.4조, max≈6.35e16 → 통화(`unit_measure`)별 분리 → `decimals` 스케일 보정 → `log1p` + 윈저라이즈(0.5/99.5%).
   - `xbrli:pure`/`xbrli:shares`/`iso4217:*` 마다 의미가 다르므로 unit별로 독립 피처군 구성.

4. **타깃 concept 식별**
   - 빈도 Top-20 의 IFRS-full 핵심 항목(`Equity`, `Assets`, `Revenue`, `ProfitLoss`, `ComprehensiveIncome`, `DividendsPaid`, `PropertyPlantAndEquipment`, `RightofuseAssets`, `DeferredTaxLiabilityAsset` …) 을 1차 피처 셋으로 우선 추출.
   - 동일 의미가 `ifrs-full_*`, `dart_*`, `entity{corp}_*` 3개 네임스페이스에 흩어져있어 — `metric_catalog`/`metric_mapping_rule` 의 매핑이 필수.

5. **결측/이상치 처리**
   - `value_numeric IS NULL` 9.03% 는 텍스트 fact(주석/문장형) — 정형 피처에서는 제외하거나 `value_text` 만 별도 처리.
   - `unit_measure / decimals` 빈문자열 9.54% 도 동일 텍스트 fact 집합. 정형 파이프라인에서 `unit_measure='' ` 필터로 일관 처리.

6. **데이터 품질 모니터링 SQL (재활용)**
   - 일일/주간 잡으로 `C5(자연키 중복)`, `C12(orphan)`, `D5(numeric 가용성)`, `D6(FS_raw rcept 교집합)` 4개 SQL 을 `ingestion_runs` 검증 후속으로 실행 권장.

7. **PG 운영 상의 주의**
   - 본 테이블 distinct/sort 쿼리는 PG 컨테이너 임시 디스크를 빠르게 소진(현 work_mem=4MB 기본).
   - 분석용 세션에서는 항상 `SET work_mem='256MB'; SET max_parallel_workers_per_gather=0;` 를 먼저 적용하거나, 큰 distinct 는 `TABLESAMPLE` 로 근사.
