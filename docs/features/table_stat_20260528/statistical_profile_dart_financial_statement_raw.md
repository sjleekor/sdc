# `dart_financial_statement_raw` 통계적 특성 프로파일

본 문서는 주가예측모델 피처 엔지니어링을 위한 사전 분석으로,
`dart_financial_statement_raw` 테이블의 데이터 분포·결측·커버리지·중복 등을
SQL 로 직접 조사한 결과를 정리한 것이다.

- 조사 일시: 2026-05-28
- 대상 DB: `.env` 의 `DB_DSN` 으로 접근한 PostgreSQL (`mydb`)
- 분석 대상 테이블: `dart_financial_statement_raw`

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL 허용 | 설명 |
|---|---|---|---|
| `raw_id` | bigint | NO | PK (자동) |
| `corp_code` | text | NO | OpenDART 기업 코드 |
| `ticker` | text | YES | 종목코드 (현재는 100% 채워짐) |
| `bsns_year` | integer | NO | 사업연도 |
| `reprt_code` | text | NO | 보고서 코드(1Q/반기/3Q/사업보고서) |
| `fs_div` | text | NO | 재무제표 구분 (CFS/OFS) |
| `sj_div` | text | NO | 재무제표 종류 (BS/IS/CIS/CF/SCE) |
| `sj_nm` | text | NO | 재무제표 종류명 |
| `account_id` | text | NO | 표준계정코드 (없으면 `-표준계정코드 미사용-`) |
| `account_nm` | text | NO | 계정명 |
| `account_detail` | text | NO | 계정 상세 |
| `thstrm_nm` | text | NO | 당기명 (예: `제 27 기 1분기`) |
| `thstrm_amount` | numeric | YES | 당기금액 |
| `thstrm_add_amount` | numeric | YES | 당기 누적금액 |
| `frmtrm_nm` | text | NO | 전기명 |
| `frmtrm_q_nm` | text | NO | 전기 분기명 |
| `frmtrm_q_amount` | numeric | YES | 전기 분기금액 |
| `frmtrm_amount` | numeric | YES | 전기금액 |
| `frmtrm_add_amount` | numeric | YES | 전기 누적금액 |
| `bfefrmtrm_nm` | text | NO | 전전기명 |
| `bfefrmtrm_amount` | numeric | YES | 전전기금액 |
| `ord` | bigint | NO | 행 순서 |
| `currency` | text | YES | 통화 |
| `rcept_no` | text | NO | DART 접수번호(공시 단위) |
| `source` | text | NO | 적재 출처 (`OPENDART`) |
| `fetched_at` | timestamptz | NO | 적재 시각 |
| `raw_payload` | jsonb | NO | 원본 페이로드 |

---

## 1. 핵심 결론 (Executive Summary)

- **규모**: 총 **1,254,675 행**, **2,151 개 기업(corp_code/ticker)**, **10,370 개 공시(rcept_no)**.
- **기간**: 사업연도 `2025` ~ `2026` 2개년만 적재됨 (`bsns_year`).
  - 시계열 모델링에 쓸 만큼의 장기 시계열이 아직 확보되지 않음 → 과거 연도 백필 필요.
- **소스/통화**: `source` 는 전부 `OPENDART`, 통화는 **KRW 99.2%** 가 압도적, 그 외 USD/CNY/JPY/GBP 는 해외 자회사 관련 소수.
- **재무제표 구분**: `fs_div` 가 **전부 `CFS`(연결재무제표) 단 한 가지** — 별도재무제표(`OFS`)가 적재되어 있지 않음. 모델에서 “연결 기준” 가정 가능.
- **보고서 종류**: 분기/반기/사업 4종 모두 존재.
  - `11013`(1분기) 469,906 행 / `11011`(사업) 279,702 / `11014`(3분기) 256,087 / `11012`(반기) 248,980.
- **재무제표 분포**(`sj_div`): BS 36.2%, CF 34.3%, CIS 20.4%, SCE 7.8%, IS 1.3%.
  - 손익계산서가 `IS` 보다 **포괄손익계산서 `CIS`** 로 적재되는 비중이 훨씬 큼 → 손익 피처는 `CIS` 우선 사용 권장.
- **결측 패턴 (모델링 시 주의)**:
  - `thstrm_amount` NULL: **2.98%** (값 자체는 비교적 충실)
  - `frmtrm_amount` NULL: **50.28%**, `frmtrm_q_amount` NULL: **52.97%**
  - `bfefrmtrm_amount` NULL: **78.49%**, `thstrm_add_amount` / `frmtrm_add_amount` NULL: **83.14%**
  - → **전전기 및 누적금액 컬럼은 결측이 매우 많음**. 시계열 피처는 `thstrm_amount` 위주로 구성하고, 과거 비교는 같은 corp 의 과거 보고서 행을 직접 join 하는 방식이 안전.
- **품질**:
  - 자연 키 `(corp_code, bsns_year, reprt_code, fs_div, sj_div, account_id, ord)` 기준 **중복 0건** — 안전한 그레인.
  - `ticker` NULL **0%** (모든 행에 종목코드 매핑되어 있음).
- **커버리지**:
  - 2025년: 4개 보고서코드 모두 2,030~2,113 개 corp 보유 (양호).
  - 2026년: `11013`(1Q) 만 2,056 corp, 그 외(반기/3Q/사업) 는 아직 미발간 시점이라 한 자리수 ~ 10여개 → **2026 데이터는 1분기 위주**라는 점을 학습 데이터 컷오프 시 반드시 고려.
- **계정코드**:
  - `account_id == '-표준계정코드 미사용-'` 인 행이 **85,359 행 (전체 약 6.8%)** → 표준 코드 매핑 누락 행. 피처화 시 별도 처리 필요.
  - 그 외 상위 빈도 계정은 모두 `ifrs-full_*` 표준 코드로, **당기순이익, 자본, 자산, 부채, 현금흐름** 등 핵심 재무지표가 풍부.

---

## 2. 데이터 특성 조사용 SQL 모음

아래 SQL 들은 동일한 결과를 재현하기 위한 표준 쿼리이며, 결과는 §3 에 첨부.

### 2.1. 기본 요약 (총량/유일 기업/적재 기간)
```sql
SELECT
  COUNT(*)                          AS total_rows,
  COUNT(DISTINCT corp_code)         AS distinct_corps,
  COUNT(DISTINCT ticker)            AS distinct_tickers,
  COUNT(DISTINCT rcept_no)          AS distinct_reports,
  MIN(bsns_year)                    AS min_year,
  MAX(bsns_year)                    AS max_year,
  MIN(fetched_at)                   AS min_fetched_at,
  MAX(fetched_at)                   AS max_fetched_at
FROM dart_financial_statement_raw;
```

### 2.2. 연도별 레코드 / 기업 / 공시 수
```sql
SELECT bsns_year,
       COUNT(*)                    AS rows,
       COUNT(DISTINCT corp_code)   AS corps,
       COUNT(DISTINCT rcept_no)    AS reports
FROM dart_financial_statement_raw
GROUP BY bsns_year
ORDER BY bsns_year;
```

### 2.3. 보고서 코드(`reprt_code`) 분포
```sql
SELECT reprt_code,
       COUNT(*)                  AS rows,
       COUNT(DISTINCT rcept_no)  AS reports
FROM dart_financial_statement_raw
GROUP BY reprt_code
ORDER BY rows DESC;
-- 11011=사업, 11012=반기, 11013=1분기, 11014=3분기
```

### 2.4. `fs_div`(연결/별도) 분포
```sql
SELECT fs_div, COUNT(*) AS rows, COUNT(DISTINCT corp_code) AS corps
FROM dart_financial_statement_raw
GROUP BY fs_div
ORDER BY rows DESC;
```

### 2.5. `sj_div`(재무제표 종류) 분포
```sql
SELECT sj_div, sj_nm, COUNT(*) AS rows
FROM dart_financial_statement_raw
GROUP BY sj_div, sj_nm
ORDER BY rows DESC;
```

### 2.6. 통화 분포
```sql
SELECT currency, COUNT(*) AS rows
FROM dart_financial_statement_raw
GROUP BY currency
ORDER BY rows DESC;
```

### 2.7. 주요 숫자 컬럼의 NULL 비율
```sql
SELECT
  COUNT(*) AS total,
  ROUND(100.0*SUM((ticker            IS NULL)::int)/COUNT(*),2) AS null_ticker_pct,
  ROUND(100.0*SUM((thstrm_amount     IS NULL)::int)/COUNT(*),2) AS null_thstrm_amount_pct,
  ROUND(100.0*SUM((frmtrm_amount     IS NULL)::int)/COUNT(*),2) AS null_frmtrm_amount_pct,
  ROUND(100.0*SUM((bfefrmtrm_amount  IS NULL)::int)/COUNT(*),2) AS null_bfefrmtrm_amount_pct,
  ROUND(100.0*SUM((thstrm_add_amount IS NULL)::int)/COUNT(*),2) AS null_thstrm_add_pct,
  ROUND(100.0*SUM((frmtrm_q_amount   IS NULL)::int)/COUNT(*),2) AS null_frmtrm_q_amount_pct,
  ROUND(100.0*SUM((frmtrm_add_amount IS NULL)::int)/COUNT(*),2) AS null_frmtrm_add_amount_pct,
  ROUND(100.0*SUM((currency          IS NULL)::int)/COUNT(*),2) AS null_currency_pct
FROM dart_financial_statement_raw;
```

### 2.8. 연도 x 보고서코드 기업 커버리지
```sql
SELECT bsns_year, reprt_code, COUNT(DISTINCT corp_code) AS corps
FROM dart_financial_statement_raw
GROUP BY bsns_year, reprt_code
ORDER BY bsns_year, reprt_code;
```

### 2.9. 상위 빈도 `account_id`
```sql
SELECT account_id, MAX(account_nm) AS sample_nm, COUNT(*) AS rows
FROM dart_financial_statement_raw
GROUP BY account_id
ORDER BY rows DESC
LIMIT 20;
```

### 2.10. `thstrm_amount` 분포(요약통계 + 분위수)
```sql
SELECT
  COUNT(thstrm_amount) AS n,
  MIN(thstrm_amount)   AS min,
  percentile_cont(0.01) WITHIN GROUP (ORDER BY thstrm_amount) AS p01,
  percentile_cont(0.25) WITHIN GROUP (ORDER BY thstrm_amount) AS p25,
  percentile_cont(0.50) WITHIN GROUP (ORDER BY thstrm_amount) AS p50,
  percentile_cont(0.75) WITHIN GROUP (ORDER BY thstrm_amount) AS p75,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY thstrm_amount) AS p99,
  MAX(thstrm_amount)   AS max,
  AVG(thstrm_amount)   AS avg,
  STDDEV(thstrm_amount) AS stddev
FROM dart_financial_statement_raw;
```

### 2.11. 자연 키 중복 검사
```sql
SELECT corp_code, bsns_year, reprt_code, fs_div, sj_div, account_id, ord,
       COUNT(*) AS c
FROM dart_financial_statement_raw
GROUP BY corp_code, bsns_year, reprt_code, fs_div, sj_div, account_id, ord
HAVING COUNT(*) > 1
ORDER BY c DESC
LIMIT 10;
```

### 2.12. 기업당 행 수 분포
```sql
WITH t AS (
  SELECT corp_code, COUNT(*) AS rows
  FROM dart_financial_statement_raw
  GROUP BY corp_code
)
SELECT COUNT(*) AS corps,
       AVG(rows)::numeric(12,2) AS avg_rows,
       MIN(rows) AS min_rows,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY rows) AS median_rows,
       MAX(rows) AS max_rows
FROM t;
```

### 2.13. 연도·보고서 별 공시 수
```sql
SELECT bsns_year, reprt_code, COUNT(DISTINCT rcept_no) AS reports
FROM dart_financial_statement_raw
GROUP BY bsns_year, reprt_code
ORDER BY bsns_year, reprt_code;
```

### 2.14. `fetched_at` 월별 적재량
```sql
SELECT date_trunc('month', fetched_at)::date AS month, COUNT(*) AS rows
FROM dart_financial_statement_raw
GROUP BY 1 ORDER BY 1;
```

### 2.15. `sj_div` 별 distinct `account_id` 수 (계정 다양성)
```sql
SELECT sj_div, sj_nm, COUNT(DISTINCT account_id) AS distinct_accounts
FROM dart_financial_statement_raw
GROUP BY sj_div, sj_nm
ORDER BY distinct_accounts DESC;
```

### 2.16. 통화별 금액 스케일
```sql
SELECT currency,
       COUNT(*) AS rows,
       AVG(ABS(thstrm_amount))::numeric(20,2) AS avg_abs_amount,
       MAX(ABS(thstrm_amount))                AS max_abs_amount
FROM dart_financial_statement_raw
WHERE thstrm_amount IS NOT NULL
GROUP BY currency
ORDER BY rows DESC;
```

### 2.17. 상위 `thstrm_nm` (당기명)
```sql
SELECT thstrm_nm, COUNT(*) AS rows
FROM dart_financial_statement_raw
GROUP BY thstrm_nm
ORDER BY rows DESC
LIMIT 15;
```

### 2.18. 기업별 연도 커버리지(몇 년치 데이터가 있는지)
```sql
WITH t AS (
  SELECT corp_code, COUNT(DISTINCT bsns_year) AS years
  FROM dart_financial_statement_raw
  GROUP BY corp_code
)
SELECT years, COUNT(*) AS corps
FROM t GROUP BY years ORDER BY years;
```

---

## 3. 실제 실행 결과 (2026-05-28 기준)

### 3.1. 기본 요약
| total_rows | distinct_corps | distinct_tickers | distinct_reports | min_year | max_year | min_fetched_at | max_fetched_at |
|---:|---:|---:|---:|---:|---:|---|---|
| 1,254,675 | 2,151 | 2,151 | 10,370 | 2025 | 2026 | 2026-04-19 10:24Z | 2026-05-23 15:46Z |

### 3.2. 연도별
| bsns_year | rows | corps | reports |
|---:|---:|---:|---:|
| 2025 | 1,016,063 | 2,141 | 8,291 |
| 2026 | 238,612 | 2,079 | 2,079 |

### 3.3. 보고서 코드 분포
| reprt_code | 보고서 | rows | reports |
|---|---|---:|---:|
| 11013 | 1분기 | 469,906 | 4,086 |
| 11011 | 사업보고서 | 279,702 | 2,117 |
| 11014 | 3분기 | 256,087 | 2,088 |
| 11012 | 반기 | 248,980 | 2,079 |

### 3.4. `fs_div`
| fs_div | rows | corps |
|---|---:|---:|
| CFS | 1,254,675 | 2,151 |

> 현재 적재되는 데이터는 **연결재무제표(CFS)** 단일 구분만 있음. 별도재무제표 비교가 필요한 모델링은 불가.

### 3.5. `sj_div`
| sj_div | sj_nm | rows |
|---|---|---:|
| BS  | 재무상태표 | 454,340 |
| CF  | 현금흐름표 | 430,903 |
| CIS | 포괄손익계산서 | 256,036 |
| SCE | 자본변동표 | 97,664 |
| IS  | 손익계산서 | 15,732 |

### 3.6. `source`
| source | rows |
|---|---:|
| OPENDART | 1,254,675 |

### 3.7. `currency`
| currency | rows |
|---|---:|
| KRW | 1,244,884 |
| USD | 5,325 |
| CNY | 3,774 |
| JPY | 543 |
| GBP | 149 |

### 3.8. 컬럼별 NULL 비율
| 컬럼 | NULL % |
|---|---:|
| ticker | 0.00 |
| currency | 0.00 |
| thstrm_amount | 2.98 |
| frmtrm_amount | 50.28 |
| frmtrm_q_amount | 52.97 |
| bfefrmtrm_amount | 78.49 |
| thstrm_add_amount | 83.14 |
| frmtrm_add_amount | 83.14 |

### 3.9. 연도 x 보고서코드 기업 커버리지
| bsns_year | reprt_code | corps |
|---:|---|---:|
| 2025 | 11011 | 2,113 |
| 2025 | 11012 | 2,067 |
| 2025 | 11013 | 2,030 |
| 2025 | 11014 | 2,081 |
| 2026 | 11011 | 4 |
| 2026 | 11012 | 12 |
| 2026 | 11013 | 2,056 |
| 2026 | 11014 | 7 |

### 3.10. 상위 `account_id` (TOP 10)
| account_id | 예시 명 | rows |
|---|---|---:|
| `-표준계정코드 미사용-` | (계정명 다양) | 85,359 |
| `ifrs-full_ProfitLoss` | 당기순이익(손실) | 27,440 |
| `ifrs-full_Equity` | 자본총계 | 20,731 |
| `ifrs-full_OtherComprehensiveIncome` | 기타포괄손익 | 11,953 |
| `ifrs-full_ComprehensiveIncome` | 총포괄이익(손실) | 11,685 |
| `ifrs-full_CashAndCashEquivalents` | 현금및현금성자산 | 10,478 |
| `ifrs-full_Liabilities` | 부채 합계 | 10,370 |
| `dart_EquityAtBeginningOfPeriod` | 기초자본 | 10,370 |
| `ifrs-full_Assets` | 자산총계 | 10,370 |
| `ifrs-full_EquityAndLiabilities` | 부채와자본총계 | 10,369 |

### 3.11. `thstrm_amount` 분포
| 통계 | 값 |
|---|---:|
| N (non-null) | 1,217,276 |
| min | -68,512,206,000,000 |
| p01 | -46,924,604,176 |
| p25 | 4,550,298 |
| p50 (median) | 1,366,490,295 |
| p75 | 19,212,399,093 |
| p99 | 3,603,056,540,002 |
| max | 829,740,827,000,000 |
| mean | 322,346,772,578 |
| stddev | 7,273,983,123,930 |

> 분포의 꼬리가 매우 두꺼움(좌측 -68조 ~ 우측 +829조). **로그 스케일/표준화** 이전에 outlier winsorize 필요.

### 3.12. 자연 키 중복
- 결과 행 수: **0** (중복 없음).
- `(corp_code, bsns_year, reprt_code, fs_div, sj_div, account_id, ord)` 를 안전한 그레인 키로 사용 가능.

### 3.13. 기업당 행 수
| corps | avg | min | median | max |
|---:|---:|---:|---:|---:|
| 2,151 | 583.30 | 82 | 570 | 1,499 |

### 3.14. `fetched_at` 월별 적재
| month | rows |
|---|---:|
| 2026-04 | 278,549 |
| 2026-05 | 976,126 |

> 데이터 적재가 2026-04 ~ 05 두 달에 집중 — 초기 적재 단계임을 시사.

### 3.15. `sj_div` 별 distinct account 수
| sj_div | distinct_accounts |
|---|---:|
| CF | 1,022 |
| BS | 602 |
| CIS | 549 |
| SCE | 311 |
| IS | 207 |

### 3.16. 통화별 금액 스케일
| currency | rows | avg(\|amount\|) | max(\|amount\|) |
|---|---:|---:|---:|
| KRW | 1,207,712 | 336,273,123,214 | 829,740,827,000,000 |
| USD | 5,237 | 95,777,847 | 8,680,513,000 |
| CNY | 3,693 | 286,483,231 | 11,706,721,032 |
| JPY | 485 | 3,089,938,853 | 38,450,380,071 |
| GBP | 149 | 5,704,489 | 80,823,602 |

### 3.17. 기업별 연도 커버리지
| 보유 연도 수 | 기업 수 |
|---:|---:|
| 1 | 82 |
| 2 | 2,069 |

> 약 96.2% 의 기업이 2025·2026 두 해 모두 보유. 단년도만 있는 기업 82개는 신규 상장/상폐 가능성 → 모델 학습 시 마스킹/제외 검토.

---

## 4. 모델링 관점 시사점 (Action Items)

1. **장기 시계열 부족**: 2025·2026 두 해뿐이므로, DART 백필(`deploy/prod/bin/dart-backfill-all-years.sh`) 로 과거 5~10년치를 확보한 뒤 시계열 피처(YoY/QoQ growth, trend) 를 만드는 것이 우선.
2. **그레인 고정**: `(corp_code, bsns_year, reprt_code, sj_div, account_id, ord)` 기준으로 중복이 없으므로, 피처 추출 시 이 키로 안전하게 pivot/aggregation 가능.
3. **결측 컬럼 대응**:
   - `thstrm_add_amount`, `frmtrm_add_amount`, `bfefrmtrm_amount` 는 결측률이 78~83% → **사용하지 말거나** self-join 으로 직접 보강.
   - 누적치(분기 누적)가 필요한 경우 같은 corp 의 1Q/반기/3Q/사업 보고서 `thstrm_amount` 를 누적해서 계산하는 편이 안전.
4. **손익 계열**: `IS`(손익계산서) 적재는 1.3%로 매우 적음. **`CIS`(포괄손익계산서)** 를 기본으로 사용.
5. **표준 계정 매핑 누락**: `account_id = '-표준계정코드 미사용-'` 6.8% 행은 `account_nm`/`account_detail` 기반의 룰/매핑 테이블을 별도 구축해야 모델 피처로 활용 가능.
6. **통화 정규화**: USD/CNY/JPY/GBP 1만여 행이 KRW 와 섞여 있어 단순 `SUM` 시 단위 혼선 발생 가능 → KRW 환산 또는 통화별 분리.
7. **이상치 처리**: `thstrm_amount` 의 표준편차(7.3조) 가 평균(3,200억) 대비 22배 → **로버스트 스케일링 / log1p / winsorize(p01~p99)** 권장.
8. **2026 부분 적재**: 사업/반기/3Q 보고서는 시점상 아직 거의 없으므로(2026 = 1분기 위주), 학습/평가 분할 시 _as-of_ 시점 누설 방지에 유의.

---

## 5. 재현 가이드

1. `.env` 의 `DB_DSN` 으로 PostgreSQL 접속.
2. §2 의 SQL 들을 순서대로 실행하면 §3 결과를 재현할 수 있다.
3. 본 문서의 통계 값은 `2026-05-28 08:54 KST` 시점의 스냅샷이며,
   수집 파이프라인이 새 공시를 적재할 때마다 갱신되어야 한다.
