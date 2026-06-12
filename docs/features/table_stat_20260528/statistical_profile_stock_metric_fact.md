# `stock_metric_fact` 통계적 특성 프로파일

- 작성 일시: 2026-05-28
- 대상 DB: PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 적재 규모: **34,411 행** / **2,647 종목/기업** / **29 metric_code** / **31 mapping_rule** / 사업연도 **2025** 단일·보고서 **11011(사업보고서)** 단일
- 참고: 본 문서는 [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트(C1~C10) + §4 특화 항목을 동일 절차로 적용한 결과이다. 템플릿은 [`statistical_profile_dart_financial_statement_raw.md`](./statistical_profile_dart_financial_statement_raw.md).

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| fact_id | bigint | NO | PK (BIGSERIAL) |
| ticker | text | NO | UQ |
| market | text | NO | KOSPI/KOSDAQ |
| corp_code | text | NO | DART 8자리 |
| metric_code | text | NO | FK → metric_catalog, UQ |
| period_type | text | NO | `annual` 외 후보 |
| period_end | date | YES | 기간 종료일 |
| bsns_year | int | NO | UQ |
| reprt_code | text | NO | UQ (11011/11012/11013/11014) |
| fs_div | text | NO | CFS/OFS/'' |
| value_numeric | numeric(30,4) | YES | 정량값 |
| value_text | text | NO | 정성값 |
| unit | text | NO | KRW/shares 등 |
| source_table | text | NO | 원천 테이블 |
| source_key | text | NO | 원천 자연키 직렬화 |
| mapping_rule_code | text | NO | FK → metric_mapping_rule |
| fetched_at | timestamptz | NO | 수집 시각 |
| updated_at | timestamptz | NO | 갱신 시각 |

- UNIQUE: `(ticker, metric_code, bsns_year, reprt_code)` — 종목·지표·연도·보고서 1행 보장
- 보조 인덱스: `ix_stock_metric_fact_lookup(ticker, metric_code, bsns_year DESC, reprt_code)`
- 적재 파이프라인: `metric_mapping_rule` 우선순위에 따라 raw 4종(`dart_financial_statement_raw`, `dart_xbrl_fact_raw`, `dart_share_count_raw`, `dart_shareholder_return_raw`)을 canonical metric 으로 정규화

---

## 1. 핵심 결론 (Executive Summary)

- **규모/커버리지**: 34,411행 / 2,647 종목·기업 / 29개 metric_code / 31개 mapping_rule. `bsns_year=2025`·`reprt_code=11011(사업보고서)` 단일, `period_type=annual` 단일.
- **종목 커버리지 완전성**: `(ticker, market)` 2,647 페어가 `stock_master`(2,780) 와 `daily_ohlcv`(2,780) 양쪽에 100% 포함(고아 0). SMF 가 마스터의 95.2% 종목을 커버.
- **무결성 완벽**: UQ `(ticker, metric_code, bsns_year, reprt_code)` 중복 0건, `value_numeric` NULL 0%, `value_text=''` 0%, `period_end` NULL 0%, `unit=''` 0%.
- **시장 구성**: KOSDAQ 21,288(61.9% / 1,812종목) / KOSPI 13,123(38.1% / 835종목) — `daily_ohlcv` 와 거의 동일 분포.
- **원천 분포(source_table)**:
  - `dart_financial_statement_raw` 26,066(75.7%) — 재무제표 21개 metric
  - `dart_share_count_raw` 4,390(12.8%) — `issued_shares`, `treasury_shares`
  - `dart_xbrl_fact_raw` 2,834(8.2%) — `weighted_avg_shares`, `diluted_shares`, `depreciation_expense`, `amortization_intangible_assets`
  - `dart_shareholder_return_raw` 1,121(3.3%) — `dps`
- **단위(unit)**: `KRW` 28,690(83.4%) / `shares` 5,721(16.6%). `metric_catalog` 단위와 100% 일치.
- **metric_catalog 활용도**: 카탈로그 29개 전부 1회 이상 사용(사용률 100%, 미사용 metric 0).
- **종목별 metric 가용성 편차 큼**: ticker–year 페어당 metric 수 min 1 / p05 1 / p50 14 / p95 22 / max 29 / avg 13. **손익계산서 5종(`revenue/cogs/operating_income/net_income/sga`)이 121~165 종목**에만 적재된 “**커버리지 빈약 metric**”. 반대로 `issued_shares` 는 2,647 전 종목.
- **부호 분포 합리적**:
  - 현금흐름 음수 비율 — `financing_cf` 55.1%, `investing_cf` 76.2%, `operating_cf` 30.5% (현실적 신호).
  - `total_equity` 자본잠식 12종(0.6%), `total_liabilities`/`total_assets` 음수·0 없음.
  - `dps` zero 8건(중간배당만 기재된 종목 추정), `issued_shares`/`treasury_shares` 음수 0.
- **DART corp_code 교집합**: SMF 2,647 corp ⊃ FS_raw 2,151 corp 의 2,148(99.86%). FS 에 없지만 SMF 에 있는 corp **499개** — 이는 `dart_share_count_raw`/`dart_shareholder_return_raw` 에서만 적재된 종목(증자/배당 보고만 있고 사업보고서 첨부 미완 추정).
- **시계열 한계**: 2025 사업보고서(11011)만 → 분기·반기보고서(11012/11013/11014) 와 과거 연도(2024 이하) 백필이 시계열 학습 전 선결.
- **수집 시점**: 2026-04 에 34,326행(99.75%) 백필 + 2026-05 증분 85행.

---

## 2. 데이터 특성 조사용 SQL 모음

> 34K 행으로 모든 쿼리는 1초 미만 응답.

### C1. 총 행수 / 키 / 시간 범위

```sql
SELECT COUNT(*) total_rows,
       COUNT(DISTINCT ticker) tickers,
       COUNT(DISTINCT market) markets,
       COUNT(DISTINCT corp_code) corps,
       COUNT(DISTINCT metric_code) metrics,
       COUNT(DISTINCT bsns_year) years,
       COUNT(DISTINCT reprt_code) reprts,
       COUNT(DISTINCT source_table) src_tables,
       COUNT(DISTINCT mapping_rule_code) rules,
       MIN(bsns_year), MAX(bsns_year),
       MIN(period_end), MAX(period_end),
       MIN(fetched_at), MAX(fetched_at)
  FROM stock_metric_fact;
```

### C2. 연도 × 보고서 분포

```sql
SELECT bsns_year, reprt_code, COUNT(*) c
  FROM stock_metric_fact GROUP BY 1,2 ORDER BY 1,2;
```

### C3. 시장 / period_type / fs_div / source_table / unit

```sql
SELECT market, COUNT(*) c, COUNT(DISTINCT ticker) tickers
  FROM stock_metric_fact GROUP BY 1;
SELECT period_type, COUNT(*) c FROM stock_metric_fact GROUP BY 1;
SELECT fs_div, COUNT(*) c FROM stock_metric_fact GROUP BY 1;
SELECT source_table, COUNT(*) c FROM stock_metric_fact GROUP BY 1;
SELECT unit, COUNT(*) c FROM stock_metric_fact GROUP BY 1;
```

### C4. NULL/빈값 비율

```sql
SELECT
  ROUND(100.0*SUM((value_numeric IS NULL)::int)/COUNT(*),3) null_numeric,
  ROUND(100.0*SUM((value_text='')::int)/COUNT(*),3) empty_text,
  ROUND(100.0*SUM((period_end IS NULL)::int)/COUNT(*),3) null_period_end,
  ROUND(100.0*SUM((unit='')::int)/COUNT(*),3) empty_unit
FROM stock_metric_fact;
```

### C5. UNIQUE 중복

```sql
SELECT COUNT(*) dup_groups FROM (
  SELECT ticker, metric_code, bsns_year, reprt_code, COUNT(*) c
  FROM stock_metric_fact GROUP BY 1,2,3,4 HAVING COUNT(*)>1) t;
```

### C6. 종목·지표별 행수 분포

```sql
WITH t AS (SELECT ticker, COUNT(*) c FROM stock_metric_fact GROUP BY ticker)
SELECT COUNT(*) tickers, MIN(c), MAX(c), AVG(c)::numeric(10,2),
       percentile_cont(0.05) WITHIN GROUP (ORDER BY c) p05,
       percentile_cont(0.5 ) WITHIN GROUP (ORDER BY c) p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY c) p95 FROM t;

SELECT metric_code, COUNT(*) c, COUNT(DISTINCT ticker) tickers
FROM stock_metric_fact GROUP BY 1 ORDER BY c DESC;
```

### C8. value_numeric 분위수 (메트릭별)

```sql
SELECT metric_code, COUNT(*) n,
       MIN(value_numeric) mn, MAX(value_numeric) mx,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY value_numeric) p50,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY value_numeric) p99
FROM stock_metric_fact GROUP BY 1 ORDER BY 1;
```

### D. 특화

```sql
-- D1: metric × source_table 매핑
SELECT metric_code, source_table, COUNT(*) c
FROM stock_metric_fact GROUP BY 1,2 ORDER BY 1,2;

-- D5: metric별 음수/0 행
SELECT metric_code,
       SUM((value_numeric<0)::int) neg,
       SUM((value_numeric=0)::int) zero,
       COUNT(*) total
FROM stock_metric_fact GROUP BY 1 ORDER BY 1;

-- D6: stock_master / daily_ohlcv 교집합
WITH x AS (SELECT DISTINCT ticker, market FROM stock_metric_fact),
     sm AS (SELECT DISTINCT ticker, market FROM stock_master),
     oh AS (SELECT DISTINCT ticker, market FROM daily_ohlcv)
SELECT (SELECT COUNT(*) FROM x) smf_pairs,
       (SELECT COUNT(*) FROM x JOIN sm USING(ticker,market)) smf_sm_both,
       (SELECT COUNT(*) FROM x JOIN oh USING(ticker,market)) smf_ohlcv_both;

-- D7: FS_raw corp_code 교집합
WITH x AS (SELECT DISTINCT corp_code FROM stock_metric_fact),
     f AS (SELECT DISTINCT corp_code FROM dart_financial_statement_raw)
SELECT (SELECT COUNT(*) FROM x) smf_corps,
       (SELECT COUNT(*) FROM f) fs_corps,
       (SELECT COUNT(*) FROM x JOIN f USING(corp_code)) both,
       (SELECT COUNT(*) FROM x LEFT JOIN f USING(corp_code) WHERE f.corp_code IS NULL) only_smf;

-- D8: metric_catalog 사용률
WITH c AS (SELECT metric_code FROM metric_catalog),
     f AS (SELECT DISTINCT metric_code FROM stock_metric_fact)
SELECT (SELECT COUNT(*) FROM c) catalog_n,
       (SELECT COUNT(*) FROM f) used_n;

-- D10: bsns_year vs period_end 분포
SELECT bsns_year, reprt_code, MIN(period_end), MAX(period_end),
       COUNT(DISTINCT period_end) distinct_pe, COUNT(*) c
FROM stock_metric_fact GROUP BY 1,2;

-- D11: ticker-year 당 metric 수 분포
WITH t AS (SELECT ticker, bsns_year, COUNT(DISTINCT metric_code) c
           FROM stock_metric_fact GROUP BY 1,2)
SELECT MIN(c), MAX(c), AVG(c)::numeric(10,2),
       percentile_cont(0.5) WITHIN GROUP (ORDER BY c) p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY c) p95,
       COUNT(*) ticker_year_pairs FROM t;
```

---

## 3. 실제 실행 결과 (2026-05-28)

### 3.1 규모 / 키 / 시간 범위 (C1)

- total_rows = **34,411**, tickers = **2,647**, corps = **2,647**, metrics = **29**, rules = **31**, source_tables = **4**
- `bsns_year`: 2025 단일 / `reprt_code`: `11011` 단일 / `period_type`: `annual` 단일
- `period_end` 범위: 2023-12-31 ~ 2025-12-31 (distinct 14개 — 분기/회계연도 변경 종목 포함)
- `fetched_at`: 2026-04-19 ~ 2026-05-20 UTC

### 3.2 연도 × 보고서 (C2)

| bsns_year | reprt_code | 행수 |
|---:|---|---:|
| 2025 | 11011 (사업보고서) | 34,411 |

### 3.3 시장 / 단위 / 원천 (C3)

| market | rows | tickers |
|---|---:|---:|
| KOSDAQ | 21,288 | 1,812 |
| KOSPI | 13,123 | 835 |

| unit | rows |
|---|---:|
| KRW | 28,690 |
| shares | 5,721 |

| source_table | rows |
|---|---:|
| `dart_financial_statement_raw` | 26,066 |
| `dart_share_count_raw` | 4,390 |
| `dart_xbrl_fact_raw` | 2,834 |
| `dart_shareholder_return_raw` | 1,121 |

| fs_div | rows |
|---|---:|
| `CFS` | 26,066 |
| `''` (공란) | 8,345 |

> `fs_div=''` 은 FS_raw 이외 원천(`share_count`/`xbrl`/`shareholder_return`) 에서 적재된 행으로 의도된 공란.

### 3.4 NULL / 무결성 (C4·C5)

| 항목 | 값 |
|---|---:|
| null_numeric | 0.000% |
| empty_text | 0.000% |
| null_period_end | 0.000% |
| empty_unit | 0.000% |
| UNIQUE 중복 | **0** |

### 3.5 종목 분포 (C6)

- 종목당 행수: tickers=2,647 / min=1 / p05=1 / p50=14 / p95=21.7 / max=29 / avg=13.00
- ticker-year 페어당 metric 수 = 종목당 행수 동일 (단일 연도이므로)

### 3.6 metric_code 별 적재량 (C6b, 상위 12)

| metric_code | rows | tickers | 분류 |
|---|---:|---:|---|
| `issued_shares` | 2,647 | 2,647 | 주식수(필수) |
| `total_liabilities` | 2,112 | 2,112 | BS |
| `total_assets` | 2,111 | 2,111 | BS |
| `total_equity` | 2,111 | 2,111 | BS |
| `operating_cash_flow` | 2,108 | 2,108 | CF |
| `investing_cash_flow` | 2,108 | 2,108 | CF |
| `financing_cash_flow` | 2,106 | 2,106 | CF |
| `cash_and_cash_equivalents` | 2,086 | 2,086 | BS |
| `interest_received` | 1,980 | 1,980 | CF |
| `capex_ppe` | 1,910 | 1,910 | CF |
| `interest_paid` | 1,867 | 1,867 | CF |
| `treasury_shares` | 1,743 | 1,743 | 주식수 |

손익계산서(IS) 5종(`revenue`/`cogs`/`operating_income`/`net_income`/`sga`)은 **121~165 종목** 만 — `dart_financial_statement_raw` 의 IS 매핑 룰이 한정적이라 커버리지가 낮음(전체 종목의 ~6%).

### 3.7 value_numeric 분위수 (C8, 전체)

| n | min | p01 | p50 | p99 | max | avg |
|---:|---:|---:|---:|---:|---:|---:|
| 34,411 | -68.5조 | -1,593억 | 23.8억 | 9.8조 | 797.9조 | 7,878억 |

`total_assets` 최대치(797.9조) = 삼성전자 추정. 매트릭별 p50 은 §3.10 참고.

### 3.8 시간 분포 (C10)

| 월 | rows |
|---|---:|
| 2026-04 | 34,326 (99.75%) |
| 2026-05 | 85 (0.25%) |

### 3.9 특화 (D)

- **D1 metric × source_table**: 모든 metric 이 **단일 source_table** 에 매핑(중복 없음). 단, `depreciation_expense` 는 `xbrl.depreciationexpense` 와 `xbrl.depreciationandamortisationexpense` 두 rule 이 동시 적재 → 같은 metric_code 에 우선순위 다른 룰이 모두 사용된 케이스.
- **D5 부호 분포 (음수 비율 상위)**:

  | metric_code | neg | zero | total | neg% |
  |---|---:|---:|---:|---:|
  | `investing_cash_flow` | 1,607 | 1 | 2,108 | 76.2% |
  | `financing_cash_flow` | 1,161 | 1 | 2,106 | 55.1% |
  | `operating_cash_flow` | 643 | 0 | 2,108 | 30.5% |
  | `controlling_net_income` | 39 | 0 | 158 | 24.7% |
  | `net_income` | 37 | 0 | 164 | 22.6% |
  | `total_equity` | 12 | 0 | 2,111 | 0.57% (자본잠식) |

  이외 `revenue`/`cogs`/`total_assets`/`total_liabilities`/`issued_shares`/`treasury_shares` 음수 0(정상).
- **D6 마스터·시세 교집합**: SMF 페어 2,647 = stock_master 교집합 2,647 = daily_ohlcv 교집합 2,647 — **모든 SMF 종목이 시세·마스터에 존재(고아 0)**.
- **D7 FS_raw corp 교집합**: SMF 2,647 corps ⊃ FS_raw 2,151 corps 의 2,148 (FS에만 있는 corp 3개, SMF 에만 있는 corp **499개** — 주식수/배당 보고만으로 적재).
- **D8 metric_catalog 사용률**: 카탈로그 29 = 사용 29 = 100% 활용, 미사용 metric 0.
- **D10 period_end vs bsns_year**: `bsns_year=2025`에 대해 `period_end` distinct 14개. 대부분 2025-12-31 이지만 12월 결산 외 종목(3·6·9월 결산)이 포함되어 다양한 종료일이 섞임. 일부 2023-12-31/2024-12-31 행은 회계연도 변경·정정 보고서 기인 추정.
- **D11 ticker–year 당 metric 수**: 2,647 페어, p50=14, p95=22, max=29 — 종목별 가용 metric 편차가 매우 큼.

### 3.10 metric 별 p50 (D12, KRW/shares)

| metric_code | metric_name | unit | p50 |
|---|---|---|---:|
| `revenue` | 매출액 | KRW | 9,716억 |
| `gross_profit` | 매출총이익 | KRW | 1,349억 |
| `operating_income` | 영업이익 | KRW | 387억 |
| `net_income` | 당기순이익 | KRW | 215억 |
| `total_assets` | 총자산 | KRW | 2,544억 |
| `total_equity` | 총자본 | KRW | 1,460억 |
| `total_liabilities` | 총부채 | KRW | 940억 |
| `cash_and_cash_equivalents` | 현금성자산 | KRW | 238억 |
| `operating_cash_flow` | 영업CF | KRW | 76억 |
| `investing_cash_flow` | 투자CF | KRW | -92억 |
| `financing_cash_flow` | 재무CF | KRW | -6.5억 |
| `capex_ppe` | 유형자산 취득 | KRW | 54억 |
| `dividends_paid` | 배당금 지급 | KRW | 37억 |
| `dps` | 주당현금배당금 | KRW | 225원 |
| `issued_shares` | 발행주식수 | shares | 21,829,074주 |
| `treasury_shares` | 자기주식수 | shares | 363,728주 |
| `weighted_avg_shares` | 가중평균주식수 | shares | 27,385,327주 |

---

## 4. 모델링/피처 엔지니어링 시사점

1. **단일 시점 한계**: 2025 사업보고서만 적재 → 시계열 학습 불가. **분기·반기보고서(11012/11013/11014) + 2024 이전 백필**이 우선 작업. (PLAN.md §4 와 일관.)
2. **종목별 metric 가용성 균일화 필요**: 종목당 metric 수 분포가 1~29 로 광범위. IS 5종(`revenue/cogs/operating_income/net_income/sga`)은 121~165 종목만 적재 — 매핑 룰 (`fin.*.cfs.is.*`) 보강 또는 `dart_xbrl_fact_raw` IS 컨셉으로 폴백 필요. 피처 엔지니어링 시 “이용 가능한 metric 의 subset 으로 학습 가능한 모델”(예: lightgbm 의 missing-aware, MICE 임퓨테이션)을 우선 고려.
3. **`stock_master`·`daily_ohlcv` 와의 1:1 정합**: 고아 0 — SMF 의 `(ticker,market)` 을 그대로 시세 join 키로 사용 가능. 다만 마스터의 2,780 종목 중 133 종목(4.8%) 은 SMF 미수록 → 신규 상장·관리종목 또는 DART 매핑 누락 가능.
4. **FS_raw 외 corp 499개**: `dart_share_count_raw`/`dart_shareholder_return_raw` 에서만 적재된 종목군 → 재무제표가 없으면 metric 수가 1~3 개에 그침. 모델 학습 시 corp 수준의 “재무 가용 여부” 플래그 피처 추가 권장.
5. **현금흐름 부호 정합성 확인 완료**: `investing_cf` 음수 76%, `financing_cf` 음수 55% 는 산업 통계와 일치. 임의 절댓값 적용 금지.
6. **자본잠식 12종(0.57%)**: `total_equity<0` — 모델 학습 시 음수 자본 종목을 별도 분류(부도/관리/스팩) 하거나 log 변환 대신 signed-log(`sign(x)*log1p(|x|)`) 사용 권장.
7. **단위 혼재 → 그룹별 스케일링**: `KRW`(83%)/`shares`(17%) — 단순 평탄화 시 정보 손실. metric_code 별 robust-scaling 또는 비율 피처(예: `treasury_shares/issued_shares`, `cash/total_assets`) 로 변환.
8. **`depreciation_expense` 룰 충돌 가능성**: 2개 rule(`ifrs-full_depreciationexpense` 548 + `ifrs-full_depreciationandamortisationexpense` 419)이 동시 적재되어 일부 종목에서 중복 가능. UQ 가 `(ticker, metric_code, year, reprt)` 라 같은 종목에는 1행이지만, 종목 간 정의 차이가 발생 — `metric_mapping_rule.priority` 와 `mapping_rule_code` 메타로 후처리 시 분리 학습 또는 일관 정의 강제 권장.

---

## 5. 후속 작업 제안

- 분기·반기 보고서 11012/11013/11014 백필(파이프라인 cron 확장).
- 2020~2024 사업보고서 백필 — `daily_ohlcv` 가 2014~ 이므로 최소 2014 이후 매년 적재 시 종목 횡단 학습 가능.
- IS 매핑 룰 보강 — 현행 21개 종목 미만인 `cogs`, `sga`, `gross_profit` 등을 XBRL 폴백 룰로 우선순위 추가.
- `metric_catalog` 에 “필수(required)” 플래그 추가하여 적재 모니터링.
- `mapping_rule_code` 별 적용 종목수 대시보드화 → 룰 품질 모니터링.
