# `dart_shareholder_return_raw` 통계적 특성 프로파일

- 작성 일시: 2026-05-28
- 대상 DB: PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 적재 규모: **263,030 행** / **2,647 기업** / **2,677 보고서(rcept_no)**
- 참고: 본 문서는 [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트(C1~C12) + §4 특화 항목을 동일 절차로 적용한 결과이다. 템플릿은 [`statistical_profile_dart_financial_statement_raw.md`](./statistical_profile_dart_financial_statement_raw.md).

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL |
|---|---|---|
| raw_id | bigint | NO (PK, BIGSERIAL) |
| corp_code | text | NO |
| ticker | text | YES |
| bsns_year | int | NO |
| reprt_code | text | NO (default `''`) |
| statement_type | text | NO |
| row_name | text | NO (default `''`) |
| stock_knd | text | NO (default `''`) |
| dim1 / dim2 / dim3 | text | NO (default `''`) |
| metric_code | text | NO |
| metric_name | text | NO |
| value_numeric | numeric(30,4) | YES |
| value_text | text | NO (default `''`) |
| unit | text | YES |
| rcept_no | text | NO (default `''`) |
| stlm_dt | date | YES |
| source | text | NO |
| fetched_at | timestamptz | NO |
| raw_payload | jsonb | NO |

- 자연 키(UNIQUE 제약 `uq_dart_shareholder_return_raw`):
  `(corp_code, bsns_year, reprt_code, statement_type, row_name, stock_knd, dim1, dim2, dim3, metric_code, rcept_no)`
- 보조 인덱스 `ix_dart_shareholder_return_raw_lookup (ticker, bsns_year, reprt_code, statement_type)`
- `statement_type` ∈ {`dividend`, `treasury_stock`} — 한 테이블에 두 도메인을 long-format 으로 함께 적재.

---

## 1. 핵심 결론 (Executive Summary)

- **규모**: 263,030 행 / 2,647 기업 / 2,677 보고서. `dart_financial_statement_raw` 의 기업집합(2,151) 보다 596 기업 더 많음 → DART 사업보고서 본문에 ‘재무제표’ 가 없거나 일부만 있어도 ‘배당/자기주식’ 표는 별도 적재되는 케이스가 다수 존재한다.
- **시계열 한계**: `bsns_year=2025` 단일, `reprt_code=11011`(사업보고서) 단일. 분/반기보고서(11012/11013/11014) 적재는 아직 없음 → 시계열·중간배당 모델 학습 전 백필 필요.
- **자연키 무결성 완벽**: 11개 컬럼 자연키 중복 0건. PK + UNIQUE 제약 정상 동작.
- **두 statement_type 의 비대칭**:
  - `treasury_stock` 155,810 행(59.2%) — `unit='shares'`, `dim1/dim2/dim3` **100% 사용**(취득방법/구분 등 다축), `value_numeric` 가용 **10.60%**(주로 ‘기간/사유’ 같은 텍스트 위주, 변동수량만 수치).
  - `dividend` 107,220 행(40.8%) — `unit=''`, `dim*` 미사용, `value_numeric` 가용 **41.19%**(주당배당금/배당총액/배당성향/시가배당률 등 수치 표).
- **`stock_knd` 표기 비표준**: `보통주`/`보통주식`/`보 통 주`, `우선주`/`우선주식`/`우 선 주`, ‘-’/`기타`/`종류주`/`종류주식`, 각종 `1우선주`/`2우선주`/`제1종 우선주` 등 50+ 종류 혼재 → **사전 정규화(normalization) 필수**.
- **결제일(`stlm_dt`)**: 98.0% 가 `2025-12-31`. 나머지는 6월/3월/9월/11월 등 — 12월 결산법인 위주, 일부 결산월이 다른 법인 포함.
- **`metric_code` 8종 only**: 배당 3종(`thstrm`/`frmtrm`/`lwfr` = 당기/전기/전전기), 자기주식 5종(`bsis_qy`/`trmend_qy`/`change_qy_acqs`/`change_qy_dsps`/`change_qy_incnr` = 기초/기말/취득/처분/소각). 매우 컴팩트한 코드체계.
- **FS_raw 와의 정합**:
  - rcept_no 교집합 2,112 / SR 2,677, FS 10,370 — SR 의 78.9% 가 FS 보고서에 포함됨.
  - corp_code 교집합 2,148 / SR 2,647, FS 2,151 — FS 의 99.86% corp 가 SR 에도 있음(역방향 81.1%). 즉 **SR 기업집합이 FS 의 상위 집합**.
- **수치 스케일**: `value_numeric` n=60,681 기준 min ≈ -3.56e10, max ≈ 2.58e9, p25=25 / p50=954 / p75=32,740 / p99=6,000,000 — 주식수·금액·비율이 한 컬럼에 혼재. `row_name`/`unit` 으로 분리해서 해석해야 의미가 있음.

---

## 2. 데이터 특성 조사용 SQL 모음

> 263K 행 규모로 sort 부담은 크지 않으나, `SET work_mem='256MB'` 권장.

### C1. 총 행수 / 유일 키 / 시간 범위

```sql
SELECT COUNT(*)                       AS total_rows,
       COUNT(DISTINCT corp_code)      AS corps,
       COUNT(DISTINCT ticker)         AS tickers,
       COUNT(DISTINCT rcept_no)       AS rcepts,
       COUNT(DISTINCT metric_code)    AS metric_codes,
       COUNT(DISTINCT statement_type) AS stmt_types,
       MIN(fetched_at), MAX(fetched_at),
       MIN(bsns_year),  MAX(bsns_year),
       MIN(stlm_dt),    MAX(stlm_dt)
  FROM dart_shareholder_return_raw;
```

### C2. 사업연도 분포

```sql
SELECT bsns_year, COUNT(*) c,
       COUNT(DISTINCT corp_code) corps,
       COUNT(DISTINCT rcept_no)  rcepts
  FROM dart_shareholder_return_raw
 GROUP BY bsns_year ORDER BY 1;
```

### C3. 카테고리 컬럼 분포

```sql
SELECT reprt_code,     COUNT(*) c FROM dart_shareholder_return_raw GROUP BY 1 ORDER BY c DESC;
SELECT statement_type, COUNT(*) c FROM dart_shareholder_return_raw GROUP BY 1 ORDER BY c DESC;
SELECT stock_knd,      COUNT(*) c FROM dart_shareholder_return_raw GROUP BY 1 ORDER BY c DESC LIMIT 20;
SELECT source,         COUNT(*) c FROM dart_shareholder_return_raw GROUP BY 1;
SELECT unit,           COUNT(*) c FROM dart_shareholder_return_raw GROUP BY 1 ORDER BY c DESC;
```

### C4. NULL / 빈 문자열 비율

```sql
SELECT ROUND(100.0*SUM((ticker        IS NULL)::int)/COUNT(*),2) null_ticker,
       ROUND(100.0*SUM((value_numeric IS NULL)::int)/COUNT(*),2) null_value_numeric,
       ROUND(100.0*SUM((unit          IS NULL)::int)/COUNT(*),2) null_unit,
       ROUND(100.0*SUM((stlm_dt       IS NULL)::int)/COUNT(*),2) null_stlm_dt,
       ROUND(100.0*SUM((stock_knd='')::int)/COUNT(*),2) empty_stock_knd,
       ROUND(100.0*SUM((dim1=''     )::int)/COUNT(*),2) empty_dim1,
       ROUND(100.0*SUM((dim2=''     )::int)/COUNT(*),2) empty_dim2,
       ROUND(100.0*SUM((dim3=''     )::int)/COUNT(*),2) empty_dim3
  FROM dart_shareholder_return_raw;
```

### C5. 자연키 중복 검사

```sql
SELECT COUNT(*) dup_groups, COALESCE(SUM(c-1),0) extra_rows FROM (
  SELECT COUNT(*) c FROM dart_shareholder_return_raw
   GROUP BY corp_code, bsns_year, reprt_code, statement_type, row_name,
            stock_knd, dim1, dim2, dim3, metric_code, rcept_no
  HAVING COUNT(*)>1) t;
```

### C6. 엔티티별 행수 분포

```sql
WITH t AS (SELECT corp_code, COUNT(*) c FROM dart_shareholder_return_raw GROUP BY corp_code)
SELECT COUNT(*) corps, MIN(c), MAX(c), AVG(c)::numeric(10,1) avg,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY c) p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY c) p95 FROM t;

WITH t AS (SELECT rcept_no, COUNT(*) c FROM dart_shareholder_return_raw
            WHERE rcept_no<>'' GROUP BY rcept_no)
SELECT COUNT(*) rcepts, MIN(c), MAX(c), AVG(c)::numeric(10,1) avg,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY c) p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY c) p95 FROM t;
```

### C8. `value_numeric` 분위수

```sql
SELECT COUNT(*) n, MIN(value_numeric) mn, MAX(value_numeric) mx,
       AVG(value_numeric)::numeric(30,2) avg,
       percentile_cont(0.01) WITHIN GROUP (ORDER BY value_numeric) p01,
       percentile_cont(0.25) WITHIN GROUP (ORDER BY value_numeric) p25,
       percentile_cont(0.5 ) WITHIN GROUP (ORDER BY value_numeric) p50,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY value_numeric) p75,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY value_numeric) p99
  FROM dart_shareholder_return_raw WHERE value_numeric IS NOT NULL;
```

### C9. 상위 빈도 코드

```sql
SELECT metric_code, MAX(metric_name) name, COUNT(*) c
  FROM dart_shareholder_return_raw GROUP BY 1 ORDER BY c DESC LIMIT 30;

SELECT row_name, COUNT(*) c FROM dart_shareholder_return_raw
 WHERE row_name<>'' GROUP BY 1 ORDER BY c DESC LIMIT 20;

SELECT unit, COUNT(*) c FROM dart_shareholder_return_raw GROUP BY 1 ORDER BY c DESC LIMIT 20;
```

### C10. 시간 분포

```sql
SELECT date_trunc('month', fetched_at)::date m, COUNT(*) c
  FROM dart_shareholder_return_raw GROUP BY 1 ORDER BY 1;

SELECT stlm_dt, COUNT(*) c FROM dart_shareholder_return_raw
 WHERE stlm_dt IS NOT NULL GROUP BY 1 ORDER BY c DESC LIMIT 20;
```

### D. 특화 항목

```sql
-- D1: statement_type x reprt_code
SELECT statement_type, reprt_code, COUNT(*) c
  FROM dart_shareholder_return_raw GROUP BY 1,2 ORDER BY 1, c DESC;

-- D2: ticker / corp 커버리지
SELECT COUNT(*) total,
       SUM((ticker IS NULL)::int)  null_ticker,
       COUNT(DISTINCT ticker)      distinct_ticker,
       COUNT(DISTINCT corp_code)   distinct_corp
  FROM dart_shareholder_return_raw;

-- D3: dim1~dim3 사용률
SELECT SUM((dim1<>'')::int) dim1_used,
       SUM((dim2<>'')::int) dim2_used,
       SUM((dim3<>'')::int) dim3_used,
       COUNT(*) total
  FROM dart_shareholder_return_raw;

-- D4: stock_knd × statement_type
SELECT statement_type, stock_knd, COUNT(*) c
  FROM dart_shareholder_return_raw
 GROUP BY 1,2 ORDER BY 1, c DESC LIMIT 40;

-- D5: rcept_no 교집합 vs FS_raw
WITH x AS (SELECT DISTINCT rcept_no FROM dart_shareholder_return_raw WHERE rcept_no<>''),
     y AS (SELECT DISTINCT rcept_no FROM dart_financial_statement_raw)
SELECT (SELECT COUNT(*) FROM x) sr_rcepts,
       (SELECT COUNT(*) FROM y) fs_rcepts,
       (SELECT COUNT(*) FROM x JOIN y USING(rcept_no)) both;

-- D6: corp_code 교집합 vs FS_raw
WITH x AS (SELECT DISTINCT corp_code FROM dart_shareholder_return_raw),
     y AS (SELECT DISTINCT corp_code FROM dart_financial_statement_raw)
SELECT (SELECT COUNT(*) FROM x) sr_corps,
       (SELECT COUNT(*) FROM y) fs_corps,
       (SELECT COUNT(*) FROM x JOIN y USING(corp_code)) both;

-- D7: statement_type 별 value_numeric 가용성
SELECT statement_type, COUNT(*) total,
       SUM((value_numeric IS NOT NULL)::int) has_num,
       ROUND(100.0*SUM((value_numeric IS NOT NULL)::int)/COUNT(*),2) pct_num
  FROM dart_shareholder_return_raw GROUP BY 1 ORDER BY total DESC;
```

---

## 3. 실제 실행 결과 (2026-05-28)

### 3.1 규모 / 키 / 시간 범위 (C1)
- total_rows = **263,030**, corps = **2,647**, tickers = **2,647**, rcepts = **2,677**
- metric_codes = **8**, stmt_types = **2** (`dividend`, `treasury_stock`)
- fetched_at: 2026-04-19 ~ 2026-05-20 (대부분 4월)
- bsns_year: **2025** 단일
- stlm_dt: 2025-01-31 ~ 2025-12-31

### 3.2 분포 (C2/C3)

| bsns_year | 행수 | corps | rcepts |
|---:|---:|---:|---:|
| 2025 | 263,030 | 2,647 | 2,677 |

| reprt_code | 행수 |
|---|---:|
| 11011 (사업보고서) | 263,030 |

| statement_type | 행수 |
|---|---:|
| treasury_stock | 155,810 (59.2%) |
| dividend       | 107,220 (40.8%) |

`stock_knd` Top:

| stock_knd | 행수 |
|---|---:|
| 보통주 | 77,404 |
| (빈문자열) | 73,308 |
| 우선주 | 57,732 |
| - | 33,052 |
| 보통주식 | 8,301 |
| 기타주식 | 6,840 |
| 종류주식 | 2,242 |
| 종류주 | 1,902 |

`unit` Top:

| unit | 행수 |
|---|---:|
| shares | 155,810 (treasury_stock 전체) |
| (빈문자열) | 107,220 (dividend 전체) |

`source`: `OPENDART` 100%.

### 3.3 NULL / 빈 문자열 (C4)

| 컬럼 | 비율 |
|---|---:|
| null_ticker | 0.00% |
| null_value_numeric | 76.93% |
| null_unit | 0.00% |
| null_stlm_dt | 0.00% |
| empty_reprt_code | 0.00% |
| empty_row_name | 0.00% |
| empty_stock_knd | 27.87% |
| empty_dim1 / dim2 / dim3 | 40.76% |
| empty_value_text | 0.00% |
| empty_rcept_no | 0.00% |

### 3.4 자연키 중복 (C5)
- dup_groups = **0**, extra_rows = **0**.

### 3.5 엔티티별 분포 (C6)
- 기업당 행수: corps=2,647 / min=38 / max=225 / avg=99.4 / p50=123 / p95=135
- 보고서(rcept)당 행수: docs=2,677 / min=38 / max=225 / avg=98.3 / p50=120 / p95=135

### 3.6 `value_numeric` 분위수 (C8)
- n = 60,681 (전체의 23.07%)
- min = -3.56e10, max = 2.58e9, avg = -2.59e6
- p01 = -55,501.8 / p25 = 25 / p50 = 954 / p75 = 32,740 / p99 = 6,000,000

> 음수는 자기주식 ‘처분’/‘소각’ 수량(부호 음수)으로 추정. 양수 분포는 ‘주당 배당금’/‘수익률(%)’/‘주식수’ 등이 한 컬럼에 섞여 있어 단일 분포 해석은 의미 제한적이다.

### 3.7 상위 `metric_code` / `row_name` (C9)

`metric_code` (전부):

| metric_code | 한글명 | 행수 |
|---|---|---:|
| lwfr            | 전전기      | 35,740 |
| thstrm          | 당기        | 35,740 |
| frmtrm          | 전기        | 35,740 |
| change_qy_incnr | 소각변동수량 | 31,162 |
| bsis_qy         | 기초수량    | 31,162 |
| trmend_qy       | 기말수량    | 31,162 |
| change_qy_acqs  | 취득변동수량 | 31,162 |
| change_qy_dsps  | 처분변동수량 | 31,162 |

`row_name` Top-10:

| row_name | 행수 |
|---|---:|
| 자기주식 취득 및 처분 현황 | 155,810 |
| 주당 현금배당금(원) | 13,035 |
| 현금배당수익률(%) | 13,029 |
| 주당 주식배당(주) | 12,471 |
| 주식배당수익률(%) | 12,468 |
| (연결)당기순이익(백만원) | 8,031 |
| 주식배당금총액(백만원) | 8,031 |
| (연결)주당순이익(원) | 8,031 |
| 현금배당금총액(백만원) | 8,031 |
| 주당액면가액(원) | 8,031 |

### 3.8 시간 분포 (C10)

`fetched_at` 월별:

| month | 행수 |
|---|---:|
| 2026-04 | 262,388 |
| 2026-05 |     642 |

`stlm_dt` Top:

| stlm_dt | 행수 |
|---|---:|
| 2025-12-31 | 257,869 |
| 2025-06-30 | 1,523 |
| 2025-03-31 | 1,276 |
| 2025-09-30 |   872 |
| 2025-11-30 |   393 |

→ 12월 결산 압도적, 일부 3·6·9월 결산법인 존재.

### 3.9 특화(D)

- **D1**: 두 statement_type 모두 `reprt_code=11011` 만 존재 — 분기/반기 보고서 적재 0.
- **D2**: ticker NULL 0, distinct_ticker = distinct_corp = 2,647 — 종목코드 1:1 매핑 완전.
- **D3**: `dim1=dim2=dim3 = 155,810` 사용 → 정확히 `treasury_stock` 행 전부에만 채워져 있음(`dividend` 행은 모두 빈문자열).
- **D5**: SR ∩ FS rcept_no = 2,112 / SR 2,677 / FS 10,370 — SR 의 78.9% 만 FS rcept_no 와 일치(나머지 약 565 SR rcept 는 FS_raw 에 없음 — FS 적재 누락 가능성 또는 ‘배당/자기주식’ 만 별도 적재된 케이스).
- **D6**: SR ∩ FS corp_code = 2,148 / SR 2,647 / FS 2,151 — FS 기업의 99.9% 가 SR 에 포함, SR 에는 FS 에 없는 499 기업 추가 존재.
- **D7**: value_numeric 가용 — `treasury_stock` 10.60% (대부분 텍스트), `dividend` 41.19%.
- **D8**: `value_text` 는 두 statement_type 모두 100% 채워져 있음 → 원본 표 셀 텍스트가 항상 보존됨(분석 시 fallback 가능).

---

## 4. 모델링 시사점

1. **시계열·중간배당 활용 불가** — bsns_year=2025 & reprt_code=11011 단일. 시계열/이벤트 모델 학습 전, 11012/11013/11014 + 2020~2024 백필 필요.
2. **두 statement_type 분리 모델링 권장** — 단위/차원/수치 비율이 완전히 다름. `treasury_stock` 은 (수량/이벤트) 시계열, `dividend` 는 (금액·비율) 단일 시점 메트릭으로 별도 파이프라인 구성.
3. **`stock_knd` 정규화 사전 필요** — `보통주`/`보통주식`/`보 통 주`/`-`/`기타`, `우선주` 계열 50+ 종류 → 룰베이스 매핑 사전(예: `{보통주, 보통주식, 보 통 주, 보통주(소액주주), 보통주(최대주주)} → COMMON`)을 `metric_mapping_rule` 에 추가하는 것을 권장.
4. **`row_name` 기반 피처 추출** — 배당 도메인의 의미 있는 수치 피처는 row_name 으로 식별: `주당 현금배당금(원)`, `현금배당수익률(%)`, `현금배당성향(%)`, `현금배당금총액(백만원)`, `(연결)당기순이익(백만원)`, `(연결)주당순이익(원)` 등. `metric_code` 만으로는 (당기/전기/전전기)·(기초/기말/취득/처분/소각) 8종에 그쳐, **(row_name × metric_code × stock_knd_normalized)** 가 진짜 피처 키.
5. **자기주식 변동 검증식** — `trmend_qy = bsis_qy + change_qy_acqs - change_qy_dsps - change_qy_incnr` 식을 데이터 품질 체크(이상치 탐지) 룰로 적용 가능.
6. **수치 컬럼 의미 분리** — `value_numeric` 에 수량·원·%가 혼재. (row_name → 단위 타입) 매핑 후 피처로 펼쳐야 함(예: `row_name LIKE '%수익률%' → ratio`).
7. **외부 마스터 매핑** — corp_code/ticker 1:1 매칭 → `daily_ohlcv.ticker`, `stock_master.ticker` 와 직접 조인 가능. `dart_financial_statement_raw` 보다 더 폭넓은 기업집합(특히 FS 누락 499기업)을 커버 — 라벨 누락 기업 보강 소스로도 활용 가능.

---

## 5. 진행 메모 (Operational)

- 전체 SQL 실행은 `.venv/bin/python` + `psycopg2` 로 한 번에 18쿼리 실행(약 30초). 임시디스크 이슈 없음.
- 본 문서는 `dart_financial_statement_raw` / `dart_xbrl_fact_raw` 와 동일 포맷이며, [`PLAN.md`](./PLAN.md) §7 체크리스트의 해당 항목을 완료 처리한다.
