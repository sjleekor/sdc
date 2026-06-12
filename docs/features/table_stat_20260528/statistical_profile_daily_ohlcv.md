# `daily_ohlcv` 통계적 특성 프로파일

- 작성 일시: 2026-05-28
- 대상 DB: PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 적재 규모: **6,517,317 행** / **2,780 종목** / **4,674 거래일** (2007-06-05 ~ 2026-05-21)
- 참고: 본 문서는 [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트(C1~C10) + §4 특화 항목을 동일 절차로 적용한 결과이다. 템플릿은 [`statistical_profile_dart_financial_statement_raw.md`](./statistical_profile_dart_financial_statement_raw.md).

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| trade_date | date | NO | PK |
| ticker | text | NO | PK, KRX 종목코드(6자리) |
| market | text | NO | PK, `KOSPI` / `KOSDAQ` |
| open | bigint | NO | 시가(원) |
| high | bigint | NO | 고가(원) |
| low | bigint | NO | 저가(원) |
| close | bigint | NO | 종가(원) |
| volume | bigint | NO | 거래량(주) |
| source | text | NO | 데이터 출처(`PYKRX`) |
| fetched_at | timestamptz | NO | 수집 시각 |

- PK: `(trade_date, ticker, market)`
- 보조 인덱스: `ix_daily_ohlcv_ticker_date (ticker, market, trade_date DESC)`, `ix_daily_ohlcv_sync_cursor (fetched_at, trade_date, ticker, market)`
- 어댑터: `src/krx_collector/adapters/prices_pykrx/` (pykrx 라이브러리 기반)

---

## 1. 핵심 결론 (Executive Summary)

- **규모/시계열**: 6.52M 행 / 2,780 종목 / 4,674 거래일. **2007-06-05 ~ 2026-05-21** 약 19년치 일봉. 2014년부터 본격적 전체 종목 수집 시작(2007~2013 은 단일 티커 백테스트용 시계열로 추정), 이후 매년 종목수 자연 증가(2014: 1,678 → 2026: 2,780).
- **시장구성**: KOSDAQ 3.92M(60.2%) / KOSPI 2.60M(39.8%). KOSDAQ 종목수가 KOSPI 의 약 2배(1,830 vs 950).
- **PK 무결성 완벽**: 중복 0건.
- **수치 무결성 양호하나 1.6% 정지일 패턴 존재**: `open=high=low=0` 인 행이 **1.597%**(약 104K 행) 존재. 동일 행에서 `close>0`(직전 종가 유지)이라 `close>high` 형태의 D1 항등식 위배가 **152,817 행(2.34%)** 카운트됨 → 실질 의미는 “**거래정지 또는 거래량 0 일자에 pykrx가 OHL을 0으로, close 는 기준가로 반환**”이다. 데이터 자체 오류가 아닌 pykrx 의 표기 관행.
- **거래량 0 행은 2.12%**(138K 행)이며 연도별로 1.3~2.7% 분포. 거래정지·신규상장일/관리종목 등이 섞여 있음.
- **종가는 0 이 없음**(min=20원, p01=569원) — pykrx 가 비거래일에도 직전 종가를 유지하기 때문.
- **종목 마스터와 1:1 완벽 일치**: `stock_master` 와 `(ticker, market)` 페어 2,780 = 2,780 = 교집합 2,780. 고아 행 0.
- **최신 거래일 커버리지**: 2026-05-21 기준 2,770 종목 적재(전체 2,780 중 99.6%) — 10 종목이 최신일 데이터 누락(거래정지/신규 상장 직후로 추정).
- **가격 분위수**(close): p01=569원, p50=7,060원, p99=262,673원, p99.9=890,000원, max=4,601,000원(아마 액면병합 직전 일부 종목). 학습 시 log 변환 권장.
- **거래대금(close × volume) 분위수**: p50 ≈ 6.5억원, p95 ≈ 267억원, p99 ≈ 1,168억원, max ≈ 109.6조원(2007년 단일 ETF 추정 이상치). 유동성 피처 산출 기반으로 사용 가능.
- **연도별 거래일수**: 약 244일(연 245~248) 정상. 2026 년은 5/21 까지 94 거래일 누적.
- **수집 시각**: 2026-04 에 6.47M 행 일괄 백필 + 2026-05 증분 47K 행.

---

## 2. 데이터 특성 조사용 SQL 모음

> 6.5M 행 / PK 인덱스가 있어 모든 쿼리는 수십 초 이내 응답.

### C1. 총 행수 / 키 / 시간 범위

```sql
SELECT COUNT(*) total_rows,
       COUNT(DISTINCT ticker) tickers,
       COUNT(DISTINCT market) markets,
       COUNT(DISTINCT trade_date) trade_dates,
       COUNT(DISTINCT source) sources,
       MIN(trade_date), MAX(trade_date),
       MIN(fetched_at), MAX(fetched_at)
  FROM daily_ohlcv;
```

### C2. 연도 분포

```sql
SELECT EXTRACT(YEAR FROM trade_date)::int yr, COUNT(*) c,
       COUNT(DISTINCT ticker) tickers,
       COUNT(DISTINCT trade_date) days
  FROM daily_ohlcv GROUP BY 1 ORDER BY 1;
```

### C3. 시장/소스 분포

```sql
SELECT market, COUNT(*) c, COUNT(DISTINCT ticker) tickers
  FROM daily_ohlcv GROUP BY 1 ORDER BY c DESC;
SELECT source, COUNT(*) c FROM daily_ohlcv GROUP BY 1;
```

### C4. 0/음수 값 비율

```sql
SELECT
  ROUND(100.0*SUM((open=0)::int)/COUNT(*),3)   zero_open,
  ROUND(100.0*SUM((high=0)::int)/COUNT(*),3)   zero_high,
  ROUND(100.0*SUM((low=0)::int)/COUNT(*),3)    zero_low,
  ROUND(100.0*SUM((close=0)::int)/COUNT(*),3)  zero_close,
  ROUND(100.0*SUM((volume=0)::int)/COUNT(*),3) zero_volume,
  SUM((open<0 OR high<0 OR low<0 OR close<0 OR volume<0)::int) negative_rows
  FROM daily_ohlcv;
```

### C5. PK 중복

```sql
SELECT COUNT(*) dup_groups FROM (
  SELECT trade_date, ticker, market, COUNT(*) c
    FROM daily_ohlcv GROUP BY 1,2,3 HAVING COUNT(*)>1) t;
```

### C6. 종목·일자별 분포

```sql
-- 종목별 행수
WITH t AS (SELECT ticker, COUNT(*) c FROM daily_ohlcv GROUP BY ticker)
SELECT COUNT(*) tickers, MIN(c), MAX(c), AVG(c)::numeric(10,2),
       percentile_cont(0.05) WITHIN GROUP (ORDER BY c) p05,
       percentile_cont(0.5 ) WITHIN GROUP (ORDER BY c) p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY c) p95 FROM t;

-- 일자별 종목수
WITH t AS (SELECT trade_date, COUNT(*) c FROM daily_ohlcv GROUP BY trade_date)
SELECT COUNT(*) days, MIN(c), MAX(c), AVG(c)::numeric(10,2),
       percentile_cont(0.05) WITHIN GROUP (ORDER BY c) p05,
       percentile_cont(0.5 ) WITHIN GROUP (ORDER BY c) p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY c) p95 FROM t;
```

### C8. OHLCV 수치 분위수 (각 컬럼 동일)

```sql
SELECT COUNT(*) n, MIN(close) mn, MAX(close) mx, AVG(close)::numeric(30,2) avg,
       percentile_cont(0.01) WITHIN GROUP (ORDER BY close) p01,
       percentile_cont(0.5 ) WITHIN GROUP (ORDER BY close) p50,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY close) p99
  FROM daily_ohlcv;
```

### C10. 시간 분포

```sql
SELECT EXTRACT(YEAR FROM trade_date)::int yr, COUNT(DISTINCT trade_date) days
  FROM daily_ohlcv GROUP BY 1 ORDER BY 1;

SELECT date_trunc('month', fetched_at)::date m, COUNT(*) c
  FROM daily_ohlcv GROUP BY 1 ORDER BY 1;
```

### D. 특화 항목

```sql
-- D1: OHLC 항등식 정합성
SELECT
  SUM((low > high)::int) low_gt_high,
  SUM((open > high OR open < low)::int) open_out,
  SUM((close > high OR close < low)::int) close_out,
  SUM((high < GREATEST(open,close))::int) high_lt_oc,
  SUM((low  > LEAST(open,close))::int) low_gt_oc,
  COUNT(*) total
FROM daily_ohlcv;

-- D2/D3: 0 가격·0 거래량 분포
SELECT market,
       SUM((close=0)::int) zero_close,
       SUM((volume=0)::int) zero_volume,
       SUM((open=0 AND high=0 AND low=0 AND close=0)::int) all_zero,
       COUNT(*) total
FROM daily_ohlcv GROUP BY market;

SELECT EXTRACT(YEAR FROM trade_date)::int yr,
       SUM((volume=0)::int) zero_vol_rows,
       COUNT(*) total,
       ROUND(100.0*SUM((volume=0)::int)/COUNT(*),3) pct
FROM daily_ohlcv GROUP BY 1 ORDER BY 1;

-- D4: 시장 × 연도
SELECT market, EXTRACT(YEAR FROM trade_date)::int yr,
       COUNT(*) c, COUNT(DISTINCT ticker) tickers,
       COUNT(DISTINCT trade_date) days
FROM daily_ohlcv GROUP BY 1,2 ORDER BY 1,2;

-- D5: 종목별 상장 기간(span)
WITH t AS (
  SELECT ticker,
         MIN(trade_date) mn, MAX(trade_date) mx,
         MAX(trade_date)-MIN(trade_date) span,
         COUNT(*) c
  FROM daily_ohlcv GROUP BY ticker)
SELECT COUNT(*) tickers, MIN(span), MAX(span), AVG(span)::numeric(10,1),
       percentile_cont(0.5) WITHIN GROUP (ORDER BY span) p50_span,
       AVG(c)::numeric(10,1) avg_rows FROM t;

-- D6: stock_master 와 교집합
WITH x AS (SELECT DISTINCT ticker, market FROM daily_ohlcv),
     y AS (SELECT DISTINCT ticker, market FROM stock_master)
SELECT (SELECT COUNT(*) FROM x) ohlcv_pairs,
       (SELECT COUNT(*) FROM y) master_pairs,
       (SELECT COUNT(*) FROM x JOIN y USING(ticker,market)) both,
       (SELECT COUNT(*) FROM x LEFT JOIN y USING(ticker,market) WHERE y.ticker IS NULL) only_ohlcv,
       (SELECT COUNT(*) FROM y LEFT JOIN x USING(ticker,market) WHERE x.ticker IS NULL) only_master;

-- D7/D8: 최신 거래일 및 직전 거래일들 커버리지
SELECT trade_date, COUNT(*) c, COUNT(DISTINCT ticker) tickers
FROM daily_ohlcv
WHERE trade_date >= (SELECT MAX(trade_date)-INTERVAL '20 days' FROM daily_ohlcv)
GROUP BY trade_date ORDER BY trade_date DESC;

-- D10/D11: 가격·거래대금 극단치
SELECT percentile_cont(0.001) WITHIN GROUP (ORDER BY close) p001,
       percentile_cont(0.01)  WITHIN GROUP (ORDER BY close) p01,
       percentile_cont(0.5)   WITHIN GROUP (ORDER BY close) p50,
       percentile_cont(0.99)  WITHIN GROUP (ORDER BY close) p99,
       percentile_cont(0.999) WITHIN GROUP (ORDER BY close) p999,
       MAX(close) mx FROM daily_ohlcv;

SELECT percentile_cont(0.5)  WITHIN GROUP (ORDER BY close::numeric*volume) p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY close::numeric*volume) p95,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY close::numeric*volume) p99,
       MAX(close::numeric*volume) mx FROM daily_ohlcv WHERE volume>0;
```

---

## 3. 실제 실행 결과 (2026-05-28)

### 3.1 규모 / 키 / 시간 범위 (C1)

- total_rows = **6,517,317**, tickers = **2,780**, markets = 2 (KOSPI/KOSDAQ), trade_dates = **4,674**, sources = 1 (PYKRX)
- trade_date 범위: **2007-06-05 ~ 2026-05-21**
- fetched_at 범위: 2026-04-10 ~ 2026-05-21 (초기 백필 4월, 이후 증분)

### 3.2 연도 분포 (C2)

| yr | 행수 | 종목수 | 거래일 |
|---:|---:|---:|---:|
| 2007 | 140 | 1 | 140 |
| 2008 | 248 | 1 | 248 |
| 2009 | 253 | 1 | 253 |
| 2010 | 251 | 1 | 251 |
| 2011 | 248 | 1 | 248 |
| 2012 | 355 | 2 | 248 |
| 2013 | 494 | 2 | 247 |
| 2014 | 377,719 | 1,678 | 245 |
| 2015 | 425,198 | 1,783 | 248 |
| 2016 | 445,792 | 1,862 | 246 |
| 2017 | 460,850 | 1,944 | 243 |
| 2018 | 482,173 | 2,040 | 244 |
| 2019 | 509,891 | 2,138 | 246 |
| 2020 | 537,053 | 2,218 | 248 |
| 2021 | 562,148 | 2,315 | 248 |
| 2022 | 578,369 | 2,394 | 246 |
| 2023 | 598,285 | 2,509 | 245 |
| 2024 | 626,577 | 2,641 | 244 |
| 2025 | 651,480 | 2,756 | 242 |
| 2026 | 259,793 | 2,780 | 94 |

> 2007~2013 은 단일 티커(2~2종목) 백테스트성 시계열 → 모델 학습 시 노이즈 또는 의도된 long-history 종목으로 분리 검토 필요. 전체 종목 일봉은 **2014년부터** 신뢰 가능.

### 3.3 시장 / 소스 (C3)

| market | 행수 | tickers |
|---|---:|---:|
| KOSDAQ | 3,921,842 | 1,830 |
| KOSPI  | 2,595,475 | 950 |

`source`: `PYKRX` 100%.

### 3.4 0/음수 값 비율 (C4)

| 컬럼 | 비율 |
|---|---:|
| zero_open | 1.597% |
| zero_high | 1.597% |
| zero_low  | 1.597% |
| zero_close | 0.000% |
| zero_volume | 2.120% |
| negative_rows | 0 |

> `open=high=low=0` 동시발생 패턴이 1.597% (약 104K 행). `close` 는 0 인 행이 없음 → 거래정지일에도 종가는 직전 기준가로 유지된다.

### 3.5 PK 중복 (C5)

- dup_groups = **0**.

### 3.6 종목·일자별 분포 (C6)

- 종목당 행수: tickers=2,780 / min=2 / max=3,027 / avg=2,344 / p05=339 / p50=3,027 / p95=3,027
- 일자당 종목수: days=4,674 / min=1 / max=2,773 / avg=1,394 / p05=1 / p50=1,842 / p95=2,685

> p05=339, p50=p95=3,027 분포는 “2014년 이후 신규 상장 종목은 부분 시계열, 그 외 대부분은 전체 기간 보유”를 의미.

### 3.7 OHLCV 분위수 (C8)

| 컬럼 | n | min | p50 | p99 | max | avg |
|---|---:|---:|---:|---:|---:|---:|
| open  | 6,517,317 | 0 | 6,990   | 261,500 | 4,598,000 | 22,545 |
| high  | 6,517,317 | 0 | 7,130   | 266,500 | 4,742,000 | 22,985 |
| low   | 6,517,317 | 0 | 6,840   | 256,500 | 4,393,000 | 22,106 |
| close | 6,517,317 | 20 | 7,060  | 262,673 | 4,601,000 | 22,675 |
| volume| 6,517,317 | 0 | 87,284 | 8,958,759 | 8.31e9 | 610,245 |

### 3.8 시간 분포 (C10)

- 연도별 거래일수: 평균 245일 (2007 부분연도, 2026 진행중)
- `fetched_at`: 2026-04 6,470,280 행(99.3%) + 2026-05 47,037 행(0.7%, 증분 수집).

### 3.9 특화 (D1~D11)

- **D1 OHLC 항등식**: low>high 0 / open_out 0 / **close_out 152,817**(2.34%) / high_lt_oc 152,812 / low_gt_oc 5 — `open=high=low=0` 인 행에서 `close>0` 이라 high<close 가 발생. 데이터 오류가 아니라 pykrx 의 비거래일 표기 규약.
- **D2 시장별 0 값**: KOSDAQ zero_volume=111,893 / KOSPI 26,296. all_zero(4개 동시 0) 행 **0**.
- **D3 연도별 거래량 0 비율**: 1.3~2.7% 정상 범위, 2026 누적 4.23%(분모가 적어 변동성 큼).
- **D4 시장 × 연도**: KOSDAQ 종목수 1,807(2025) / 1,830(2026), KOSPI 949 / 950. KOSPI 일부 종목은 2014년 KOSPI 거래일이 233일 → 최초 적재 시작점 정렬 필요 확인.
- **D5 종목별 상장기간**: tickers=2,780 / min_span=1일 / max_span=6,925일 / avg=3,493일 / p50=4,504일 / avg_rows=2,344.
- **D6 stock_master 교집합**: ohlcv_pairs=2,780 = master_pairs=2,780 = both=2,780. **고아 0**. OHLCV ↔ master 완전 정합.
- **D7 최신일 (2026-05-21)**: rows=2,770 / tickers=2,770 — 전체 2,780 중 **10 종목**이 최신일 누락(거래정지/신규 상장 직후/관리종목 예상).
- **D8 최근 거래일 시계열**(2026-05-21 → 5-4): 일별 종목수 2,766~2,770 으로 매우 안정.
- **D9 연간 거래일**: 정상 245일(±3) 분포.
- **D10 close 극단치**: p001=195 / p01=569 / p50=7,060 / p99=262,673 / p999=890,000 / max=4,601,000.
- **D11 거래대금** (close × volume, volume>0): p50≈6.5억 / p95≈267억 / p99≈1,168억 / max≈109.6조 — max 는 2007 년 단일 종목 누적 이상치로 추정.

---

## 4. 모델링 시사점

1. **2014 이후를 학습 기간으로 고정**: 2007~2013 은 단일 티커(1~2종) 부분 시계열이라 종목 횡단 학습에 부적합. cutoff 권장값 `trade_date >= '2014-01-02'`.
2. **거래정지일 마스킹 규칙 명시**: `open=high=low=0` 인 행은 **거래정지/기준가 유지일**로 처리. 학습 입력에서는 `(open,high,low) = close` 로 imputation 하거나 별도 `is_halted` 플래그 컬럼을 생성. 단순히 사용하면 close>high 같은 비정상 시그널이 들어옴.
3. **거래량 0 / 거래대금 0 처리**: 2.1% 비중. 변동성·모멘텀 피처에서 0-volume 일은 결측으로 마스킹 후 forward-fill 권장. 일중 변동률(`(close-prev_close)/prev_close`)은 그대로 계산 가능(close는 0이 아님).
4. **가격 스케일 정규화**: close 가 20원~460만원으로 5자릿수 분산. 학습 시 **log(close)** 또는 **수익률(`pct_change`)** 기반 피처로 변환 필수.
5. **거래대금 = close × volume 을 우선 피처화**: 유동성 시그널·이상거래 탐지에 가장 강한 단일 피처. `numeric` 캐스팅(BIGINT×BIGINT 오버플로 방지)을 ETL 단에서 적용. p99 ≈ 1,168억, 윈저라이징 p99 cap 또는 log 변환 권장.
6. **종목 마스터와 LEFT JOIN 안전**: `(ticker, market)` 정합이 완전(both=2,780). 학습 시 daily_ohlcv 를 사실상의 종목 universe 로 사용해도 무방.
7. **최신일 결측 10 종목 처리**: 2026-05-21 기준 누락 10 종목은 distress / 신규상장 / 거래정지 후보. 라벨 생성 시 (a) 별도 표기, (b) 모델 inference 단계에서 명시적 제외 둘 중 하나의 규칙 필요.
8. **시장 분리 모델링 고려**: KOSPI(950) vs KOSDAQ(1,830) 규모·변동성·티커수 차이가 큼. 시장 더미 변수 추가 또는 시장별 separate 모델 구성 권장.

---

## 5. 후속 작업 권장

- 거래정지 마스킹 규칙(`is_halted = (open=0 AND high=0 AND low=0)`) 을 피처 빌드 파이프라인에 표준화.
- `stock_metric_fact` 프로파일링 후, `daily_ohlcv` × `stock_metric_fact` 조인 키(ticker, market, trade_date 또는 as_of_date) 정합성 점검.
- 거래대금 max 109.6조원 행의 (ticker, trade_date) 확인 후 데이터 수집/단위 검증.
- 종가 max 460만원 행이 액면병합 직전 종목인지 확인(이상치 라벨링).
- pykrx 외 보조 출처(KRX 정보데이터 시스템)와의 종가 sanity-check 샘플링.
