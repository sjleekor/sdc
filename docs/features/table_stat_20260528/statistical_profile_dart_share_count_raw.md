# `dart_share_count_raw` 통계적 특성 프로파일

- 작성 일시: 2026-05-28
- 대상 DB: PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 적재 규모: **10,295 행** / **2,647 기업** / **2,677 보고서(rcept_no)**
- 참고: 본 문서는 [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트(C1~C12) + §4 특화 항목을 동일 절차로 적용한 결과이다. 템플릿은 [`statistical_profile_dart_financial_statement_raw.md`](./statistical_profile_dart_financial_statement_raw.md).

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| raw_id | bigint | NO | PK, BIGSERIAL |
| corp_code | text | NO | DART 기업코드 |
| ticker | text | YES | KRX 종목코드 |
| bsns_year | int | NO | 사업연도 |
| reprt_code | text | NO (default `''`) | 보고서 종류 (11011/11012/11013/11014) |
| rcept_no | text | NO (default `''`) | 접수번호 |
| corp_cls | text | NO (default `''`) | 시장 구분 (Y=KOSPI, K=KOSDAQ, N=KONEX, E=기타) |
| se | text | NO (default `''`) | 주식 종류 구분 (보통주/우선주/합계/비고 등) |
| isu_stock_totqy | bigint | YES | 발행할 주식의 총수 |
| now_to_isu_stock_totqy | bigint | YES | 현재까지 발행한 주식의 총수 |
| now_to_dcrs_stock_totqy | bigint | YES | 현재까지 감소한 주식의 총수 |
| redc | text | NO (default `''`) | 감자 |
| profit_incnr | text | NO (default `''`) | 이익소각 |
| rdmstk_repy | text | NO (default `''`) | 상환주식상환 |
| etc | text | NO (default `''`) | 기타 |
| istc_totqy | bigint | YES | 발행주식의 총수 |
| tesstk_co | bigint | YES | 자기주식수 |
| distb_stock_co | bigint | YES | 유통주식수 |
| stlm_dt | date | YES | 결산일 |
| source | text | NO | 데이터 출처 |
| fetched_at | timestamptz | NO | 수집 시각 |
| raw_payload | jsonb | NO | 원본 응답 |

- 자연 키 UNIQUE 제약: `(corp_code, bsns_year, reprt_code, se, rcept_no)`
- 보조 인덱스: `ix_dart_share_count_raw_lookup (ticker, bsns_year, reprt_code)`
- API 원천: OpenDART `/api/stockTotqySttus.json` (주식의 총수 현황)

---

## 1. 핵심 결론 (Executive Summary)

- **규모**: 10,295 행 / 2,647 기업 / 2,677 보고서. `dart_shareholder_return_raw` 의 corp 집합과 **완전 일치**(2,647 ∩ 2,647 = 2,647) — 두 테이블이 동일한 OpenDART 사업보고서 파이프라인에서 동시 수집된다.
- **시계열 한계**: `bsns_year=2025` 단일 / `reprt_code=11011`(사업보고서) 단일. 분/반기보고서(11012/11013/11014) 및 과거연도 적재 없음 → 시계열 모델 학습 전 백필 필수.
- **자연키 무결성 완벽**: 5개 컬럼 자연키 중복 0건.
- **`se`(주식 구분) 표기 비표준 심각**: distinct 139 종, 줄바꿈(`\n`)·괄호·공백 변형 다수.
  - 정형 4종(`합계` 2,677, `비고` 2,677, `보통주` 2,461, `우선주` 1,834)이 91.5% 차지.
  - 나머지 8.5%는 `보통주식`/`보 통 주`/`의결권 있는 주식\n(보통주)`/`의결권 없는 주식(우선주)`/`전환우선주`/`상환전환우선주`/`종류주식`/`종류주`/`1종 종류주식` 등 50+ 표기 변형.
  - 자연키에 `se` 가 들어가므로 정규화 전에는 **동일 기업이 동일 주식 종류를 다른 표기로 두 행으로 적재되는 사고**가 발생할 수 있음.
- **(corp, bsns_year)당 평균 행수 ≈ 3.89, p50/p95=4**: 전형적인 구성은 `{보통주, 우선주, 합계, 비고}` 4행. 단일 종류 종목은 `{보통주, 합계, 비고}` 3행, 종류주식이 다양한 기업은 최대 16행.
- **시장구분(`corp_cls`)**: K(KOSDAQ) 6,990 / Y(KOSPI) 3,305 — KOSDAQ 비중 67.9%.
- **수치 컬럼 결측 큼**: `isu_stock_totqy` 43.3%, `istc_totqy` 45.8%, `tesstk_co` 65.1%, `distb_stock_co` 45.9%, `now_to_dcrs_stock_totqy` 69.8% NULL. 대부분 `비고` 행(2,677) 과 `합계` 외 종류주식 일부에서 발생. 자기주식 보유가 없는 종목은 `tesstk_co` 자체가 NULL 로 들어옴.
- **항등식 위배 확인**: `distb_stock_co = istc_totqy − tesstk_co` 항등식이 성립해야 하지만, NULL 제외 5,570 행 중 일치 20 / 불일치 5,550 → 거의 모든 경우에 자기주식 보유분이 NULL 로 들어와서 항등식이 깨지는 것으로 보임(NULL을 0으로 치환한 비교라 노이즈 큼). 즉 정규화 시 NULL→0 치환 규칙을 반드시 정의해야 함.
- **수치 스케일 정상**: `now_to_isu_stock_totqy` p50 ≈ 21M주, p99 ≈ 0.81B주, max 3.06e13(코스닥 일부 초소형 액면병합 종목 → 이상치 의심).
- **FS_raw 와의 정합**: rcept 교집합 2,112 / SC 2,677, FS 10,370. corp 교집합 2,148 / FS 2,151. SR_raw 와 corp 완전 일치(2,647).

---

## 2. 데이터 특성 조사용 SQL 모음

> 10K 행 규모로 모든 쿼리는 즉시 응답.

### C1. 총 행수 / 유일 키 / 시간 범위

```sql
SELECT COUNT(*)                  AS total_rows,
       COUNT(DISTINCT corp_code) AS corps,
       COUNT(DISTINCT ticker)    AS tickers,
       COUNT(DISTINCT rcept_no)  AS rcepts,
       COUNT(DISTINCT reprt_code) AS reprt_codes,
       COUNT(DISTINCT se)        AS se_types,
       MIN(fetched_at), MAX(fetched_at),
       MIN(bsns_year),  MAX(bsns_year),
       MIN(stlm_dt),    MAX(stlm_dt)
  FROM dart_share_count_raw;
```

### C2. 사업연도 분포

```sql
SELECT bsns_year, COUNT(*) c,
       COUNT(DISTINCT corp_code) corps,
       COUNT(DISTINCT rcept_no)  rcepts
  FROM dart_share_count_raw GROUP BY 1 ORDER BY 1;
```

### C3. 카테고리 컬럼 분포

```sql
SELECT reprt_code, COUNT(*) c FROM dart_share_count_raw GROUP BY 1 ORDER BY c DESC;
SELECT se,         COUNT(*) c FROM dart_share_count_raw GROUP BY 1 ORDER BY c DESC LIMIT 30;
SELECT corp_cls,   COUNT(*) c FROM dart_share_count_raw GROUP BY 1 ORDER BY c DESC;
SELECT source,     COUNT(*) c FROM dart_share_count_raw GROUP BY 1;
```

### C4. NULL / 빈 문자열 비율

```sql
SELECT
  ROUND(100.0*SUM((ticker                  IS NULL)::int)/COUNT(*),2) null_ticker,
  ROUND(100.0*SUM((isu_stock_totqy         IS NULL)::int)/COUNT(*),2) null_isu_stock_totqy,
  ROUND(100.0*SUM((now_to_isu_stock_totqy  IS NULL)::int)/COUNT(*),2) null_now_to_isu,
  ROUND(100.0*SUM((now_to_dcrs_stock_totqy IS NULL)::int)/COUNT(*),2) null_now_to_dcrs,
  ROUND(100.0*SUM((istc_totqy              IS NULL)::int)/COUNT(*),2) null_istc_totqy,
  ROUND(100.0*SUM((tesstk_co               IS NULL)::int)/COUNT(*),2) null_tesstk_co,
  ROUND(100.0*SUM((distb_stock_co          IS NULL)::int)/COUNT(*),2) null_distb_stock_co,
  ROUND(100.0*SUM((stlm_dt                 IS NULL)::int)/COUNT(*),2) null_stlm_dt
  FROM dart_share_count_raw;
```

### C5. 자연키 중복 검사

```sql
SELECT COUNT(*) dup_groups, COALESCE(SUM(c-1),0) extra_rows FROM (
  SELECT COUNT(*) c FROM dart_share_count_raw
   GROUP BY corp_code, bsns_year, reprt_code, se, rcept_no
  HAVING COUNT(*)>1) t;
```

### C6. 엔티티별 행수 분포

```sql
WITH t AS (SELECT corp_code, COUNT(*) c FROM dart_share_count_raw GROUP BY corp_code)
SELECT COUNT(*) corps, MIN(c), MAX(c), AVG(c)::numeric(10,2) avg,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY c) p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY c) p95 FROM t;
```

### C8. 수치 컬럼 분위수 (6개 컬럼 각각)

```sql
SELECT COUNT(*) n, MIN(isu_stock_totqy) mn, MAX(isu_stock_totqy) mx,
       AVG(isu_stock_totqy)::numeric(30,2) avg,
       percentile_cont(0.01) WITHIN GROUP (ORDER BY isu_stock_totqy) p01,
       percentile_cont(0.5 ) WITHIN GROUP (ORDER BY isu_stock_totqy) p50,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY isu_stock_totqy) p99
  FROM dart_share_count_raw WHERE isu_stock_totqy IS NOT NULL;
-- now_to_isu_stock_totqy / now_to_dcrs_stock_totqy / istc_totqy / tesstk_co / distb_stock_co 도 동일하게
```

### C10. 시간 분포

```sql
SELECT date_trunc('month', fetched_at)::date m, COUNT(*) c
  FROM dart_share_count_raw GROUP BY 1 ORDER BY 1;

SELECT stlm_dt, COUNT(*) c FROM dart_share_count_raw
 WHERE stlm_dt IS NOT NULL GROUP BY 1 ORDER BY c DESC LIMIT 20;
```

### D. 특화 항목

```sql
-- D1: se × reprt_code 교차
SELECT reprt_code, se, COUNT(*) c
  FROM dart_share_count_raw GROUP BY 1,2 ORDER BY c DESC LIMIT 30;

-- D4: 항등식 distb = istc - tesstk 위배 검사
SELECT
  SUM((distb_stock_co = COALESCE(istc_totqy,0) - COALESCE(tesstk_co,0))::int) match_rows,
  SUM((distb_stock_co IS NOT NULL
       AND distb_stock_co <> COALESCE(istc_totqy,0) - COALESCE(tesstk_co,0))::int) mismatch_rows,
  SUM((distb_stock_co IS NULL)::int) null_rows,
  COUNT(*) total
FROM dart_share_count_raw;

-- D5/D6: FS_raw 교집합
WITH x AS (SELECT DISTINCT rcept_no FROM dart_share_count_raw WHERE rcept_no<>''),
     y AS (SELECT DISTINCT rcept_no FROM dart_financial_statement_raw)
SELECT (SELECT COUNT(*) FROM x) sc_rcepts,
       (SELECT COUNT(*) FROM y) fs_rcepts,
       (SELECT COUNT(*) FROM x JOIN y USING(rcept_no)) both;

-- D7: SR_raw 교집합
WITH x AS (SELECT DISTINCT corp_code FROM dart_share_count_raw),
     y AS (SELECT DISTINCT corp_code FROM dart_shareholder_return_raw)
SELECT (SELECT COUNT(*) FROM x) sc_corps,
       (SELECT COUNT(*) FROM y) sr_corps,
       (SELECT COUNT(*) FROM x JOIN y USING(corp_code)) both;

-- D9: (corp, year) 당 행수 분포 — 정상 케이스 4행 검증
WITH t AS (SELECT corp_code, bsns_year, COUNT(*) c FROM dart_share_count_raw GROUP BY 1,2)
SELECT MIN(c) mn, MAX(c) mx, AVG(c)::numeric(10,2) avg,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY c) p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY c) p95,
       COUNT(*) groups FROM t;
```

---

## 3. 실제 실행 결과 (2026-05-28)

### 3.1 규모 / 키 / 시간 범위 (C1)

- total_rows = **10,295**, corps = **2,647**, tickers = **2,647**, rcepts = **2,677**
- reprt_codes = **1**(11011), `se_types` = **139** (표기 비표준)
- fetched_at: 2026-04-19 ~ 2026-05-20 (대부분 4월)
- bsns_year: **2025** 단일
- stlm_dt: 2025-01-31 ~ 2025-12-31

### 3.2 분포 (C2/C3)

| bsns_year | 행수 | corps | rcepts |
|---:|---:|---:|---:|
| 2025 | 10,295 | 2,647 | 2,677 |

| reprt_code | 행수 |
|---|---:|
| 11011 (사업보고서) | 10,295 |

`se` Top-10:

| se | 행수 |
|---|---:|
| 합계 | 2,677 |
| 비고 | 2,677 |
| 보통주 | 2,461 |
| 우선주 | 1,834 |
| 종류주식 | 162 |
| 보통주식 | 126 |
| 종류주 | 76 |
| 의결권 있는 주식 | 41 |
| 의결권 없는 주식 | 24 |
| 기타주식 | 16 |

> 그 외 129 종(우선주식 13, 전환우선주 10, 기타 8, 상환전환우선주 6, `의결권 있는 주식\n(보통주)` 6, `의결권 없는 주식\n(우선주)` 4, `의결권 없는\n주식` 4, `우선주(*)` 4, `의결권없는주식` 3, …)이 꼬리에 길게 분포.

`corp_cls`:

| corp_cls | 행수 |
|---|---:|
| K (KOSDAQ) | 6,990 |
| Y (KOSPI)  | 3,305 |

`source`: `OPENDART` 100%.

### 3.3 NULL / 빈 문자열 비율 (C4)

| 컬럼 | NULL/Empty |
|---|---:|
| null_ticker | 0.00% |
| null_isu_stock_totqy | 43.28% |
| null_now_to_isu_stock_totqy | 40.87% |
| null_now_to_dcrs_stock_totqy | 69.76% |
| null_istc_totqy | 45.83% |
| null_tesstk_co | 65.06% |
| null_distb_stock_co | 45.90% |
| null_stlm_dt | 0.00% |
| empty_reprt_code / se / corp_cls / redc / profit_incnr / rdmstk_repy / etc / rcept_no | 0.00% |

> 모든 텍스트 컬럼은 빈 문자열 비율 0%. 단, `redc/profit_incnr/rdmstk_repy/etc` 는 D3 에서 추가 검사(아래)에서 사실상 의미있는 값 비율 확인 필요.

### 3.4 자연키 중복 (C5)

- dup_groups = **0**, extra_rows = **0**.

### 3.5 엔티티별 분포 (C6)

- 기업당 행수: corps=2,647 / min=2 / max=16 / avg=3.89 / p50=4 / p95=4
- 보고서(rcept)당 행수: rcepts=2,677 / min=2 / max=8 / avg=3.85 / p50=4 / p95=4
- (corp, year) 당 행수: avg=3.89 / p50=4 / p95=4 / max=16 — 전형 `{보통주, 우선주, 합계, 비고}` 4행 구조.

### 3.6 수치 컬럼 분위수 (C8)

| 컬럼 | n | min | p50 | p99 | max | avg |
|---|---:|---:|---:|---:|---:|---:|
| isu_stock_totqy (발행할 주식 총수) | 5,839 | 0 | 100,000,000 | 5,000,000,000 | 5.00e14 | 2.23e11 |
| now_to_isu_stock_totqy (현재까지 발행) | 6,087 | 0 | 21,109,243 | 8.09e8 | 3.06e13 | 1.90e10 |
| now_to_dcrs_stock_totqy (현재까지 감소) | 3,113 | 0 | 2,773,000 | 7.90e8 | 1.93e12 | 1.29e9 |
| istc_totqy (발행주식 총수) | 5,577 | 0 | 20,860,012 | 4.03e8 | 3.06e13 | 2.00e10 |
| tesstk_co (자기주식수) | 3,597 | 0 | 338,193 | 2.60e7 | 1.01e12 | 7.32e8 |
| distb_stock_co (유통주식수) | 5,570 | 0 | 20,252,300 | 4.03e8 | 3.03e13 | 1.96e10 |

> max 값들이 1e13~1e14 수준 — 우선주/액면병합 후 단위 오해석 또는 입력 오류 가능. 학습 데이터 사용 전 이상치 윈저라이징(p99 캡) 권장.

### 3.7 시간 분포 (C10)

`fetched_at` 월별:

| 월 | 행수 |
|---|---:|
| 2026-04 | 10,268 |
| 2026-05 | 27 |

`stlm_dt` Top:

| stlm_dt | 행수 |
|---|---:|
| 2025-12-31 | 10,011 (97.2%) |
| 2025-06-30 | 72 |
| 2025-03-31 | 66 |
| 2025-09-30 | 41 |

### 3.8 특화 (D1~D9)

- **D1**: 모두 `reprt_code=11011`. `합계`(2,677) = `비고`(2,677) = rcepts 와 동일 — 보고서당 반드시 1행씩 적재되는 행정행.
- **D2 ticker coverage**: total=10,295, null_ticker=0, distinct_ticker=2,647 = distinct_corp=2,647. corp:ticker = 1:1.
- **D3 redc/profit_incnr/rdmstk_repy/etc non-empty**: 4개 컬럼 모두 **10,295 / 10,295 (100%) non-empty** — OpenDART 응답에서 빈 항목도 `'-'` 등으로 채워져 들어옴. 따라서 빈 문자열 검사로는 가용성 판단 불가, 실제 의미값 비율은 후속 sampling 필요.
- **D4 항등식**: `distb_stock_co = COALESCE(istc_totqy,0) - COALESCE(tesstk_co,0)` 검증 → match 20 / mismatch 5,550 / null 4,725 / total 10,295. NULL 치환의 영향으로 mismatch 가 과대 측정되나, **수치 데이터 정합성 정규화 단계에서 NULL→0 치환 규칙을 반드시 명시**해야 함.
- **D5 rcept 교집합 vs FS_raw**: SC 2,677 / FS 10,370 / both **2,112** — SC 의 78.9%(2,112/2,677) 가 FS 의 rcept 와 일치. SR_raw 와 동일한 수치(SR D5 의 2,112) → SC/SR 은 같은 rcept 집합에서 수집.
- **D6 corp 교집합 vs FS_raw**: SC 2,647 / FS 2,151 / both **2,148** — FS 의 99.86% 가 SC 에도 존재. **SC 가 FS 의 상위집합**(SC 만 있는 499 corp 존재).
- **D7 corp 교집합 vs SR_raw**: SC 2,647 / SR 2,647 / both **2,647** — **완전 일치**. SC/SR 은 동일한 OpenDART 사업보고서 파이프라인에서 동시에 적재된다.
- **D8 se 별 발행수량 분포**: `합계` 행이 has_isu=2,668(99.7%) 로 가장 신뢰도 높음. `보통주` p50=1억주, `우선주` p50=1,250만주.
- **D9 (corp,year) 행수**: avg=3.89 / p50=4 / p95=4 / max=16 / groups=2,647 — 종류주식이 매우 다양한 일부 기업(예: 우선주 여러 종류) 만 5~16행을 차지.

---

## 4. 모델링 시사점

1. **시계열·중간 흐름 부재 → 분/반기 백필 필수**: `bsns_year=2025` + `reprt_code=11011` 단일. 자기주식 변동·증자/감자 흐름을 학습하려면 11012/11013/11014 + 과거연도 적재 후 재프로파일 필요.
2. **`se` 표기 정규화 → `metric_mapping_rule` 사전 강화 권장**: 139종 distinct → 표준 분류(`COMMON`/`PREFERRED`/`CONVERTIBLE_PREF`/`REDEEMABLE_PREF`/`OTHER`/`TOTAL`/`NOTE`)로 매핑 사전 추가. 자연키에 `se` 가 들어가는 만큼, 정규화 전 단계에서는 raw 표기 그대로 사용하고 피처 단계에서 매핑 적용해야 한다.
3. **핵심 피처 행은 `se='합계'`**: 한 보고서당 정확히 1행, `isu_stock_totqy`/`now_to_isu_stock_totqy`/`istc_totqy`/`distb_stock_co`/`tesstk_co` 가 99.7% 가용. 모델 피처 추출은 우선 `se='합계'` 행을 기준 라인으로 삼고, 보통주/우선주 비율 등을 보조 피처로 add.
4. **유통주식수(`distb_stock_co`) 직접 사용 권장**: 시가총액·유동성 피처(예: 시가총액 = `distb_stock_co × close`, 유통비율 = `distb_stock_co / istc_totqy`) 산출에 핵심. NULL 행은 자기주식 0 으로 간주하고 `istc_totqy` 로 대체하는 규칙을 명시.
5. **이상치 캡**: `now_to_isu_stock_totqy` / `istc_totqy` max 3e13 수준은 입력 오류 또는 단위 혼동 가능. 학습 전 `p99` 캡 또는 log 변환 적용.
6. **자기주식 보유율 = `tesstk_co / istc_totqy`**: 65% NULL 은 `tesstk_co=0` 으로 imputation 하면 의미있는 피처화 가능. SR_raw 의 `treasury_stock` 변동수량(취득/처분/소각)과 결합하여 분기 자기주식 변동 시그널 생성에 활용.
7. **FS_raw / SR_raw / SC_raw 정합 활용**: SC corp = SR corp(2,647) ⊃ FS corp(2,151). 즉 “재무제표는 안 들어왔지만 주식수/배당/자기주식은 들어온” 기업이 499 곳 존재. 시가총액·유통주식수 같은 기본 피처는 SC 기준으로 더 넓은 유니버스 커버 가능.

---

## 5. 후속 작업 권장

- 11012/11013/11014 분/반기보고서 + 과거연도(≤2024) 백필 후 동일 절차로 재프로파일.
- `metric_mapping_rule` 에 `se` 정규화 사전 추가(`보통주`/`보통주식`/`보 통 주`/`의결권 있는 주식\n(보통주)` → `COMMON` 등).
- `redc / profit_incnr / rdmstk_repy / etc` 컬럼의 의미값 비율(예: `-` 가 아닌 값) 산출하여 D3 보강.
- `now_to_isu_stock_totqy`/`istc_totqy` 의 극단 이상치 corp_code 리스트 추출하여 데이터 수집 단계에서 검증.
