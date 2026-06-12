# `krx_security_flow_raw` 통계적 특성 프로파일

- 작성 일시: 2026-06-01
- 대상 DB: PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 적재 규모: **69,682,921 행** / **2,776 종목** / **4,674 거래일** / **7 metric_code** / **2 source** (`KRX`, `PYKRX`)
- 거래일 범위: **2007-06-05 ~ 2026-05-21**
- 참고: 본 문서는 [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트(C1~C10) + `krx_security_flow_raw` 특화 항목(투자자별 순매수, 외국인 보유, 공매도)을 적용한 결과이다.

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| raw_id | bigint | NO | PK (BIGSERIAL) |
| trade_date | date | NO | 거래일, UQ |
| ticker | text | NO | KRX 6자리 종목코드, UQ |
| market | text | NO | `KOSPI` / `KOSDAQ`, UQ |
| metric_code | text | NO | long-format metric code, UQ |
| metric_name | text | NO | 표시명 |
| value | numeric(30,4) | YES | 정규화 수치값 |
| unit | text | YES | `shares` / `KRW` |
| source | text | NO | `KRX` / `PYKRX`, UQ |
| fetched_at | timestamptz | NO | 수집 시각 |
| raw_payload | jsonb | NO | 원본 응답 row 및 요청 메타 |

- UNIQUE: `(trade_date, ticker, market, metric_code, source)`
- 보조 인덱스: `ix_krx_security_flow_raw_lookup(ticker, market, trade_date DESC)`
- 수집 코드:
  - metric 정의: `src/krx_collector/adapters/flows_common.py`
  - KRX 직접 provider/parser: `src/krx_collector/adapters/flows_krx/provider.py`, `src/krx_collector/adapters/flows_krx/parsers.py`
  - 저장소 upsert: `src/krx_collector/infra/db_postgres/repositories.py`

현재 코드 기준 metric:

| metric_code | metric_name | unit | 출처 endpoint / 의미 |
|---|---|---|---|
| `foreign_holding_shares` | 외국인 보유주식수 | shares | `MDCSTAT03701`, 시장·일자별 전종목 외국인 보유 |
| `institution_net_buy_volume` | 기관 순매수 수량 | shares | `MDCSTAT02302`, `TRDVAL1` |
| `individual_net_buy_volume` | 개인 순매수 수량 | shares | `MDCSTAT02302`, `TRDVAL3` |
| `foreign_net_buy_volume` | 외국인 순매수 수량 | shares | `MDCSTAT02302`, `TRDVAL4` |
| `short_selling_volume` | 공매도 거래량 | shares | `MDCSTAT30001`, `CVSRTSELL_TRDVOL` |
| `short_selling_value` | 공매도 거래대금 | KRW | `MDCSTAT30001`, `CVSRTSELL_TRDVAL` |
| `short_selling_balance_quantity` | 공매도 잔고 수량 | shares | `MDCSTAT30502.BAL_QTY`, 없으면 `MDCSTAT30001.STR_CONST_VAL1` fallback |

`borrow_balance_quantity` 는 코드상 `UNSUPPORTED_FLOW_METRIC_CODES` 에 남아 있고 아직 저장되지 않는다.

---

## 1. 핵심 결론 (Executive Summary)

- **규모/기간**: 69.68M 행, 2,776 종목, 4,674 거래일. 범위는 `daily_ohlcv` 와 같은 2007-06-05 ~ 2026-05-21 이지만, 2007~2013년 `daily_ohlcv` 는 거의 단일 티커만 있어 시세 조인 가능률이 0.07~0.13%에 불과하다. **모델 학습용 조인은 2015년 이후가 가장 안전**하다.
- **시장 구성**: KOSPI 37.89M(54.4%, 949종목) / KOSDAQ 31.79M(45.6%, 1,827종목). `stock_master`·`daily_ohlcv` 와 `(ticker, market)` 고아 0.
- **source 공존**: `KRX` 49.05M행(최신 2026-05-21), `PYKRX` 20.63M행(최신 2026-04-24). 두 source 가 같은 자연키를 갖는 겹침이 **19,915,294건** 있고, 겹치는 값은 **100% 동일**이다. 모델 입력에서는 `source` 를 무시하고 그냥 조회하면 중복된다.
- **source 우선순위 필요**: `(trade_date,ticker,market,metric_code)` 기준 dedupe 후 행수는 **49,767,627**이다. `KRX` 를 우선 사용하고 `PYKRX` 는 KRX 미존재 키 713,040건의 fallback 으로만 쓰는 규칙이 합리적이다.
- **metric 커버리지 차이 큼**:
  - `foreign_holding_shares`: 17.39M행, 2,776종목, 4,674일. 최신일 2,770종목.
  - 투자자별 순매수 3종: 각 11.41M행, 2,396종목, 4,674일. 최신일 KRX 2,079종목.
  - 공매도 거래량/거래대금: 각 6.90M행, 2,156종목, 4,674일. 최신일 1,920종목.
  - 공매도 잔고: 4.27M행, 2,156종목, **2016-06-30 이후 2,426일**. 최신일 916종목.
- **NULL/빈값 없음**: `value`, `unit`, `metric_name`, `source`, `raw_payload` 모두 NULL/빈값 0건. `raw_payload` 는 전부 JSON object.
- **값 분포**:
  - 투자자별 순매수는 음수/양수 모두 정상. p50은 0주, p99는 개인 306K주 / 외국인 224K주 / 기관 159K주.
  - 외국인 보유주식수 p50 398,596주, p99 82.1M주, max 3.46B주. zero 4.71%.
  - 공매도 거래대금 p50 0원, p99 42.8억원, max 8,864억원. 공매도 거래량/대금은 52.27%가 0.
- **외국인 보유율 검증**: `raw_payload.row.FORN_SHR_RT` 가 전 행 존재. `value / LIST_SHRS * 100` 과 payload 지분율은 1bp 기준 mismatch 0건. 외국인 보유율 피처는 별도 재수집 없이 raw_payload 에서 산출 가능하다.
- **투자자 3주체 합계는 0이 아님**: 저장 metric 은 기관/개인/외국인만 있고 `기타법인(TRDVAL2)` 이 없다. KRX 기준 완전한 7,421,547 ticker-day 중 44.63%만 3주체 합계가 0이고, p99 절대 잔차는 69,022주. 잔차는 데이터 오류가 아니라 누락된 기타법인 순매수 효과다.
- **공매도 비율 피처 주의**: `daily_ohlcv` 와 join 가능한 공매도 ticker-day 5.06M건에서 공매도 거래량/일 거래량 p99는 23.2%이나, 0.061%는 공매도 거래량이 일 거래량보다 크다. 일부 종목에서 `daily_ohlcv` 가격/수량 조정과 KRX 공매도 원수량 단위가 어긋난 것으로 보이며 ratio cap 또는 outlier flag 가 필요하다.

---

## 2. 데이터 특성 조사용 SQL 모음

> 69.7M 행 테이블이다. `COUNT(DISTINCT ...)`, source overlap join, JSON 파싱 쿼리는 수십 초~3분 수준으로 실행되었다. 큰 hash join 은 shared memory 오류가 날 수 있어 source overlap 쿼리에서는 `enable_hashjoin=off`, `max_parallel_workers_per_gather=0` 로 실행했다.

### C1. 총 행수 / 키 / 시간 범위

```sql
SELECT COUNT(*) total_rows,
       COUNT(DISTINCT ticker) tickers,
       COUNT(DISTINCT market) markets,
       COUNT(DISTINCT trade_date) trade_dates,
       COUNT(DISTINCT metric_code) metrics,
       COUNT(DISTINCT source) sources,
       MIN(trade_date) min_trade_date,
       MAX(trade_date) max_trade_date,
       MIN(fetched_at) min_fetched_at,
       MAX(fetched_at) max_fetched_at
FROM krx_security_flow_raw;
```

### C2. 시장 / source / unit 분포

```sql
SELECT market, COUNT(*) rows, COUNT(DISTINCT ticker) tickers,
       COUNT(DISTINCT trade_date) trade_dates
FROM krx_security_flow_raw
GROUP BY 1 ORDER BY rows DESC;

SELECT source, COUNT(*) rows, COUNT(DISTINCT ticker) tickers,
       COUNT(DISTINCT trade_date) trade_dates,
       MIN(trade_date), MAX(trade_date),
       MIN(fetched_at), MAX(fetched_at)
FROM krx_security_flow_raw
GROUP BY 1 ORDER BY rows DESC;

SELECT COALESCE(unit,'<NULL>') unit, COUNT(*) rows,
       COUNT(DISTINCT metric_code) metrics
FROM krx_security_flow_raw
GROUP BY 1 ORDER BY rows DESC;
```

### C3. metric별 규모 / 값 분포

```sql
SELECT metric_code, MIN(metric_name) metric_name, COALESCE(unit,'<NULL>') unit,
       COUNT(*) rows,
       COUNT(DISTINCT ticker) tickers,
       COUNT(DISTINCT trade_date) trade_dates,
       COUNT(DISTINCT source) sources,
       MIN(trade_date) min_date,
       MAX(trade_date) max_date,
       ROUND(100.0*SUM((value IS NULL)::int)/COUNT(*),4) null_value_pct,
       ROUND(100.0*SUM((value=0)::int)/NULLIF(SUM((value IS NOT NULL)::int),0),4) zero_value_pct,
       ROUND(100.0*SUM((value<0)::int)/NULLIF(SUM((value IS NOT NULL)::int),0),4) negative_value_pct,
       MIN(value) min_value,
       MAX(value) max_value,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY value) p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY value) p95,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY value) p99
FROM krx_security_flow_raw
GROUP BY metric_code, COALESCE(unit,'<NULL>')
ORDER BY rows DESC;
```

### C4. source × metric 분포와 일자별 row count

```sql
SELECT source, metric_code, COUNT(*) rows,
       MIN(trade_date) min_date, MAX(trade_date) max_date
FROM krx_security_flow_raw
GROUP BY 1,2 ORDER BY source, rows DESC;

WITH d AS (
  SELECT metric_code, source, trade_date, COUNT(*) rows
  FROM krx_security_flow_raw
  GROUP BY 1,2,3
)
SELECT metric_code, source, COUNT(*) days,
       MIN(rows) min_rows_per_day,
       ROUND(AVG(rows),2) avg_rows_per_day,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY rows) p50_rows_per_day,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY rows) p95_rows_per_day,
       MAX(rows) max_rows_per_day
FROM d GROUP BY 1,2 ORDER BY metric_code, source;
```

### C5. NULL/빈값 품질

```sql
SELECT COUNT(*) rows,
       SUM((value IS NULL)::int) null_value,
       SUM((unit IS NULL)::int) null_unit,
       SUM((unit='')::int) empty_unit,
       SUM((metric_name='')::int) empty_metric_name,
       SUM((source='')::int) empty_source,
       SUM((raw_payload IS NULL)::int) null_payload,
       SUM((jsonb_typeof(raw_payload) <> 'object')::int) non_object_payload
FROM krx_security_flow_raw;
```

### D1. source overlap / 값 일치성

```sql
SET work_mem = '32MB';
SET max_parallel_workers_per_gather = 0;
SET enable_hashjoin = off;

SELECT k.metric_code,
       COUNT(*) overlap_keys,
       SUM((k.value = p.value)::int) same_value_keys,
       SUM((k.value <> p.value)::int) diff_value_keys,
       ROUND(100.0 * SUM((k.value <> p.value)::int) / COUNT(*), 6) diff_pct,
       MAX(ABS(k.value - p.value)) max_abs_diff,
       AVG(ABS(k.value - p.value))::numeric(30,4) avg_abs_diff
FROM krx_security_flow_raw k
JOIN krx_security_flow_raw p
  ON p.trade_date = k.trade_date
 AND p.ticker = k.ticker
 AND p.market = k.market
 AND p.metric_code = k.metric_code
 AND p.source = 'PYKRX'
WHERE k.source = 'KRX'
GROUP BY k.metric_code
ORDER BY overlap_keys DESC;
```

### D2. stock_master / daily_ohlcv 정합성

```sql
WITH flow_pairs AS MATERIALIZED (
  SELECT DISTINCT ticker, market FROM krx_security_flow_raw
), master_pairs AS MATERIALIZED (
  SELECT ticker, market FROM stock_master
), daily_pairs AS MATERIALIZED (
  SELECT DISTINCT ticker, market FROM daily_ohlcv
)
SELECT
  (SELECT COUNT(*) FROM flow_pairs) flow_pairs,
  (SELECT COUNT(*) FROM master_pairs) stock_master_pairs,
  (SELECT COUNT(*) FROM daily_pairs) daily_ohlcv_pairs,
  (SELECT COUNT(*) FROM flow_pairs f JOIN master_pairs s USING (ticker, market)) flow_in_master,
  (SELECT COUNT(*) FROM flow_pairs f LEFT JOIN master_pairs s USING (ticker, market) WHERE s.ticker IS NULL) flow_orphans_vs_master,
  (SELECT COUNT(*) FROM master_pairs s LEFT JOIN flow_pairs f USING (ticker, market) WHERE f.ticker IS NULL) master_missing_in_flow,
  (SELECT COUNT(*) FROM flow_pairs f JOIN daily_pairs d USING (ticker, market)) flow_in_daily,
  (SELECT COUNT(*) FROM daily_pairs d LEFT JOIN flow_pairs f USING (ticker, market) WHERE f.ticker IS NULL) daily_missing_in_flow;
```

### D3. 투자자 3주체 순매수 잔차

```sql
WITH trio AS (
  SELECT trade_date, ticker, market,
         SUM(value) FILTER (WHERE metric_code='institution_net_buy_volume') AS inst,
         SUM(value) FILTER (WHERE metric_code='individual_net_buy_volume') AS indiv,
         SUM(value) FILTER (WHERE metric_code='foreign_net_buy_volume') AS foreign_net,
         COUNT(*) AS metric_rows
  FROM krx_security_flow_raw
  WHERE source='KRX'
    AND metric_code IN ('institution_net_buy_volume','individual_net_buy_volume','foreign_net_buy_volume')
  GROUP BY 1,2,3
), complete AS (
  SELECT *, (inst + indiv + foreign_net) AS residual,
            ABS(inst) + ABS(indiv) + ABS(foreign_net) AS gross_abs
  FROM trio WHERE metric_rows=3
)
SELECT COUNT(*) complete_ticker_days,
       SUM((residual=0)::int) zero_residual,
       ROUND(100.0*SUM((residual=0)::int)/COUNT(*),4) zero_residual_pct,
       MIN(residual) min_residual,
       MAX(residual) max_residual,
       AVG(ABS(residual))::numeric(30,2) avg_abs_residual,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY ABS(residual)) p50_abs_residual,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY ABS(residual)) p95_abs_residual,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY ABS(residual)) p99_abs_residual,
       ROUND(100.0*SUM((gross_abs>0 AND ABS(residual)/gross_abs > 0.1)::int)/COUNT(*),4) residual_gt_10pct_gross_pct
FROM complete;
```

### D4. 외국인 보유율 payload 검증

```sql
WITH f AS (
  SELECT value AS holding_shares,
         NULLIF(regexp_replace(raw_payload->'row'->>'FORN_SHR_RT', '[^0-9.\-]', '', 'g'), '')::numeric AS foreign_share_pct,
         NULLIF(regexp_replace(raw_payload->'row'->>'FORN_LMT_EXHST_RT', '[^0-9.\-]', '', 'g'), '')::numeric AS limit_exhaust_pct,
         NULLIF(regexp_replace(raw_payload->'row'->>'LIST_SHRS', '[^0-9.\-]', '', 'g'), '')::numeric AS listed_shares
  FROM krx_security_flow_raw
  WHERE source='KRX' AND metric_code='foreign_holding_shares'
)
SELECT COUNT(*) rows,
       SUM((foreign_share_pct IS NULL)::int) null_foreign_share_pct,
       SUM((limit_exhaust_pct IS NULL)::int) null_limit_exhaust_pct,
       SUM((listed_shares IS NULL)::int) null_listed_shares,
       MIN(foreign_share_pct) min_foreign_share_pct,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY foreign_share_pct) p50_foreign_share_pct,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY foreign_share_pct) p95_foreign_share_pct,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY foreign_share_pct) p99_foreign_share_pct,
       MAX(foreign_share_pct) max_foreign_share_pct,
       SUM((listed_shares > 0 AND ABS((holding_shares / listed_shares * 100) - foreign_share_pct) > 0.01)::int) holding_rate_mismatch_gt_1bp
FROM f;
```

### D5. 공매도 비율 vs daily_ohlcv

```sql
WITH s AS (
  SELECT trade_date, ticker, market,
         MAX(value) FILTER (WHERE metric_code='short_selling_volume') AS short_volume,
         MAX(value) FILTER (WHERE metric_code='short_selling_value') AS short_value
  FROM krx_security_flow_raw
  WHERE source='KRX' AND metric_code IN ('short_selling_volume','short_selling_value')
  GROUP BY 1,2,3
), joined AS (
  SELECT s.trade_date, s.ticker, s.market, s.short_volume, s.short_value,
         d.volume, d.close,
         CASE WHEN d.volume > 0 THEN s.short_volume / d.volume::numeric END AS short_volume_ratio,
         CASE WHEN d.volume > 0 AND d.close > 0 THEN s.short_value / (d.close::numeric * d.volume::numeric) END AS short_value_ratio
  FROM s JOIN daily_ohlcv d USING (trade_date, ticker, market)
)
SELECT COUNT(*) joined_rows,
       SUM((volume=0)::int) zero_ohlcv_volume_rows,
       SUM((short_volume > volume)::int) short_volume_gt_total_volume_rows,
       ROUND(100.0*SUM((short_volume > volume)::int)/COUNT(*),6) short_volume_gt_total_volume_pct,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY short_volume_ratio) p50_short_volume_ratio,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY short_volume_ratio) p95_short_volume_ratio,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY short_volume_ratio) p99_short_volume_ratio,
       MAX(short_volume_ratio) max_short_volume_ratio,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY short_value_ratio) p50_short_value_ratio,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY short_value_ratio) p95_short_value_ratio,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY short_value_ratio) p99_short_value_ratio,
       MAX(short_value_ratio) max_short_value_ratio
FROM joined;
```

---

## 3. SQL 실제 실행 결과 (2026-06-01)

### 3.1 규모 / 키 / 시간 범위 (C1)

| 항목 | 값 |
|---|---:|
| total_rows | 69,682,921 |
| tickers | 2,776 |
| markets | 2 |
| trade_dates | 4,674 |
| metrics | 7 |
| sources | 2 |
| min_trade_date | 2007-06-05 |
| max_trade_date | 2026-05-21 |
| min_fetched_at | 2026-04-25 17:23:23 UTC |
| max_fetched_at | 2026-05-31 13:42:34 UTC |

### 3.2 시장 / source / unit 분포 (C2)

| market | rows | tickers | trade_dates |
|---|---:|---:|---:|
| KOSPI | 37,891,561 | 949 | 4,674 |
| KOSDAQ | 31,791,360 | 1,827 | 4,674 |

| source | rows | tickers | trade_dates | min_date | max_date |
|---|---:|---:|---:|---|---|
| KRX | 49,054,587 | 2,776 | 4,674 | 2007-06-05 | 2026-05-21 |
| PYKRX | 20,628,334 | 2,770 | 4,657 | 2007-06-05 | 2026-04-24 |

| unit | rows | metrics |
|---|---:|---:|
| shares | 62,779,846 | 6 |
| KRW | 6,903,075 | 1 |

### 3.3 metric별 규모 / 값 분포 (C3)

| metric_code | rows | tickers | days | sources | zero% | neg% | min | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `foreign_holding_shares` | 17,388,734 | 2,776 | 4,674 | 2 | 4.709 | 0.000 | 0 | 398,596 | 13,968,696 | 82,114,937 | 3,462,996,814 |
| `foreign_net_buy_volume` | 11,407,365 | 2,396 | 4,674 | 2 | 9.794 | 46.339 | -31,484,373 | 0 | 60,137 | 224,049 | 39,303,395 |
| `individual_net_buy_volume` | 11,407,365 | 2,396 | 4,674 | 2 | 6.152 | 46.463 | -39,440,482 | 0 | 78,662 | 306,085 | 72,146,869 |
| `institution_net_buy_volume` | 11,407,365 | 2,396 | 4,674 | 2 | 29.225 | 36.580 | -46,720,839 | 0 | 35,003 | 158,900 | 57,272,016 |
| `short_selling_value` | 6,903,075 | 2,156 | 4,674 | 1 | 52.270 | 0.000 | 0 | 0 | 645,809,170 | 4,281,448,121 | 886,439,427,500 |
| `short_selling_volume` | 6,903,075 | 2,156 | 4,674 | 1 | 52.270 | 0.000 | 0 | 0 | 22,498 | 98,223 | 10,006,643 |
| `short_selling_balance_quantity` | 4,265,942 | 2,156 | 2,426 | 1 | 49.097 | 0.000 | 0 | 2,633.5 | 859,405 | 3,191,215 | 40,330,909 |

해석:

- `value` NULL 은 모든 metric 에서 0건이다.
- 순매수 3종의 음수는 매도 우위이므로 정상 신호다.
- 공매도 잔고는 2016-06-30 이후만 존재한다.
- 공매도 거래량/거래대금 zero 비율 52.27%는 공매도 미발생일이 많다는 의미다.

### 3.4 source × metric 분포 (C4)

| source | metric_code | rows | min_date | max_date |
|---|---|---:|---|---|
| KRX | `foreign_holding_shares` | 8,717,854 | 2007-06-05 | 2026-05-21 |
| KRX | `foreign_net_buy_volume` | 7,421,547 | 2007-06-05 | 2026-05-21 |
| KRX | `individual_net_buy_volume` | 7,421,547 | 2007-06-05 | 2026-05-21 |
| KRX | `institution_net_buy_volume` | 7,421,547 | 2007-06-05 | 2026-05-21 |
| KRX | `short_selling_value` | 6,903,075 | 2007-06-05 | 2026-05-21 |
| KRX | `short_selling_volume` | 6,903,075 | 2007-06-05 | 2026-05-21 |
| KRX | `short_selling_balance_quantity` | 4,265,942 | 2016-06-30 | 2026-05-21 |
| PYKRX | `foreign_holding_shares` | 8,670,880 | 2007-06-05 | 2026-04-24 |
| PYKRX | `institution_net_buy_volume` | 3,985,818 | 2007-06-05 | 2026-04-24 |
| PYKRX | `foreign_net_buy_volume` | 3,985,818 | 2007-06-05 | 2026-04-24 |
| PYKRX | `individual_net_buy_volume` | 3,985,818 | 2007-06-05 | 2026-04-24 |

일자별 row count 요약:

| metric_code | source | days | min/day | avg/day | p50/day | p95/day | max/day |
|---|---|---:|---:|---:|---:|---:|---:|
| `foreign_holding_shares` | KRX | 4,674 | 1,189 | 1,865.18 | 1,789.5 | 2,679.0 | 2,770 |
| `foreign_holding_shares` | PYKRX | 4,657 | 1,190 | 1,861.90 | 1,783.0 | 2,671.0 | 2,770 |
| `foreign_net_buy_volume` | KRX | 4,674 | 1,040 | 1,587.84 | 1,558.0 | 2,194.35 | 2,248 |
| `foreign_net_buy_volume` | PYKRX | 4,657 | 661 | 855.88 | 849.0 | 1,020.0 | 1,028 |
| `individual_net_buy_volume` | KRX | 4,674 | 1,040 | 1,587.84 | 1,558.0 | 2,194.35 | 2,248 |
| `individual_net_buy_volume` | PYKRX | 4,657 | 661 | 855.88 | 849.0 | 1,020.0 | 1,028 |
| `institution_net_buy_volume` | KRX | 4,674 | 1,040 | 1,587.84 | 1,558.0 | 2,194.35 | 2,248 |
| `institution_net_buy_volume` | PYKRX | 4,657 | 661 | 855.88 | 849.0 | 1,020.0 | 1,028 |
| `short_selling_value` | KRX | 4,674 | 962 | 1,476.91 | 1,443.5 | 2,072.0 | 2,151 |
| `short_selling_volume` | KRX | 4,674 | 962 | 1,476.91 | 1,443.5 | 2,072.0 | 2,151 |
| `short_selling_balance_quantity` | KRX | 2,426 | 916 | 1,758.43 | 1,755.0 | 2,109.0 | 2,151 |

최신 거래일 2026-05-21 KRX coverage:

| metric_code | rows |
|---|---:|
| `foreign_holding_shares` | 2,770 |
| `institution_net_buy_volume` | 2,079 |
| `individual_net_buy_volume` | 2,079 |
| `foreign_net_buy_volume` | 2,079 |
| `short_selling_value` | 1,920 |
| `short_selling_volume` | 1,920 |
| `short_selling_balance_quantity` | 916 |

### 3.5 NULL / raw_payload 품질 (C5)

| 항목 | 값 |
|---|---:|
| null_value | 0 |
| null_unit | 0 |
| empty_unit | 0 |
| empty_metric_name | 0 |
| empty_source | 0 |
| null_payload | 0 |
| non_object_payload | 0 |

`raw_payload` 형태:

| source | source_bld / kind | rows |
|---|---|---:|
| KRX | `dbms/MDC/STAT/standard/MDCSTAT02302` | 22,264,641 |
| KRX | `dbms/MDC/STAT/srt/MDCSTAT30001` | 13,806,150 |
| KRX | `dbms/MDC/STAT/standard/MDCSTAT03701` | 8,717,854 |
| KRX | `dbms/MDC/STAT/srt/MDCSTAT30502` | 4,265,942 |
| PYKRX | `kind=investor_net_volume` | 11,957,454 |
| PYKRX | `kind=foreign_holding_shares` | 8,670,880 |

### 3.6 source overlap / 중복성 (D1)

| metric_code | overlap_keys | same_value_keys | diff_value_keys | diff% | max_abs_diff |
|---|---:|---:|---:|---:|---:|
| `foreign_holding_shares` | 8,670,817 | 8,670,817 | 0 | 0.000000 | 0 |
| `foreign_net_buy_volume` | 3,748,159 | 3,748,159 | 0 | 0.000000 | 0 |
| `individual_net_buy_volume` | 3,748,159 | 3,748,159 | 0 | 0.000000 | 0 |
| `institution_net_buy_volume` | 3,748,159 | 3,748,159 | 0 | 0.000000 | 0 |

- source 를 포함한 UNIQUE 중복은 스키마상 불가능하다.
- source 를 제외하면 위 overlap 이 그대로 중복 그룹이다.
- overlap 합계 = **19,915,294**.
- source dedupe 후 자연키 기준 행수 = `69,682,921 - 19,915,294 = 49,767,627`.
- 값 충돌은 0건이므로 `KRX` 우선, `PYKRX` fallback dedupe 로 일관된 피처 테이블을 만들 수 있다.

### 3.7 stock_master / daily_ohlcv 정합성 (D2)

| 항목 | 값 |
|---|---:|
| flow_pairs | 2,776 |
| stock_master_pairs | 2,780 |
| daily_ohlcv_pairs | 2,780 |
| flow_in_master | 2,776 |
| flow_orphans_vs_master | 0 |
| master_missing_in_flow | 4 |
| flow_in_daily | 2,776 |
| daily_missing_in_flow | 4 |

`stock_master` 에는 있으나 flow 에 없는 4개:

| ticker | market | name | status |
|---|---|---|---|
| 452670 | KOSDAQ | 상상인제4호스팩 | DELISTED |
| 455310 | KOSDAQ | 한화플러스제4호스팩 | DELISTED |
| 457630 | KOSDAQ | 대신밸런스제16호스팩 | DELISTED |
| 138490 | KOSPI | 코오롱ENP | DELISTED |

`daily_ohlcv` 조인 가능률(KRX 기준 distinct ticker-day):

| 연도 | flow ticker-days | daily 조인 가능 | missing | 조인 가능률 |
|---:|---:|---:|---:|---:|
| 2007 | 172,980 | 140 | 172,840 | 0.08% |
| 2008 | 318,314 | 248 | 318,066 | 0.08% |
| 2009 | 336,391 | 253 | 336,138 | 0.08% |
| 2010 | 352,025 | 251 | 351,774 | 0.07% |
| 2011 | 367,039 | 248 | 366,791 | 0.07% |
| 2012 | 378,778 | 355 | 378,423 | 0.09% |
| 2013 | 385,676 | 494 | 385,182 | 0.13% |
| 2014 | 396,247 | 377,069 | 19,178 | 95.16% |
| 2015 | 424,454 | 424,454 | 0 | 100.00% |
| 2016 | 445,053 | 445,053 | 0 | 100.00% |
| 2017 | 460,120 | 460,120 | 0 | 100.00% |
| 2018 | 481,437 | 481,437 | 0 | 100.00% |
| 2019 | 509,153 | 509,153 | 0 | 100.00% |
| 2020 | 536,308 | 536,308 | 0 | 100.00% |
| 2021 | 561,617 | 561,617 | 0 | 100.00% |
| 2022 | 577,877 | 577,877 | 0 | 100.00% |
| 2023 | 597,508 | 597,508 | 0 | 100.00% |
| 2024 | 625,357 | 625,357 | 0 | 100.00% |
| 2025 | 650,272 | 650,272 | 0 | 100.00% |
| 2026 | 259,519 | 259,519 | 0 | 100.00% |

### 3.8 투자자 3주체 순매수 잔차 (D3)

KRX source 에서 기관/개인/외국인 3개 metric 이 모두 있는 ticker-day 기준:

| 항목 | 값 |
|---|---:|
| complete_ticker_days | 7,421,547 |
| zero_residual | 3,312,483 |
| zero_residual_pct | 44.6333% |
| min_residual | -47,359,991 |
| max_residual | 85,310,942 |
| avg_abs_residual | 4,892.70 |
| p50_abs_residual | 21 |
| p95_abs_residual | 15,284 |
| p99_abs_residual | 69,022 |
| residual_gt_10pct_gross_pct | 17.0143% |

큰 잔차 예시:

| trade_date | ticker | market | 기관 | 개인 | 외국인 | 합계 잔차 |
|---|---|---|---:|---:|---:|---:|
| 2021-02-23 | 001440 | KOSPI | -14,653,086 | 72,146,869 | 27,817,159 | 85,310,942 |
| 2021-08-25 | 003530 | KOSPI | 57,272,016 | -705,306 | 185,082 | 56,751,792 |
| 2020-07-31 | 001440 | KOSPI | 7,824,010 | 6,276,862 | 35,822,698 | 49,923,570 |
| 2019-04-18 | 001440 | KOSPI | 22,513,517 | 7,823,195 | 18,814,379 | 49,151,091 |
| 2017-06-27 | 006800 | KOSPI | -46,720,839 | -362,259 | -276,893 | -47,359,991 |

현재 저장 mapping 이 `TRDVAL1/3/4` 만 포함하고 `TRDVAL2(기타법인)` 를 제외하므로, 3주체 합계가 0이 아닌 것은 구조적으로 정상이다.

### 3.9 외국인 보유율 raw_payload 검증 (D4)

KRX `foreign_holding_shares` 8,717,854행 기준:

| 항목 | 값 |
|---|---:|
| null_foreign_share_pct | 0 |
| null_limit_exhaust_pct | 0 |
| null_listed_shares | 0 |
| min_foreign_share_pct | 0.00% |
| p50_foreign_share_pct | 2.29% |
| p95_foreign_share_pct | 33.06% |
| p99_foreign_share_pct | 62.91% |
| max_foreign_share_pct | 100.00% |
| holding_rate_mismatch_gt_1bp | 0 |

시사점:

- 테이블 컬럼 `value` 는 보유주식수만 저장하지만, `raw_payload.row.FORN_SHR_RT`, `LIST_SHRS`, `FORN_LMT_EXHST_RT` 가 모두 존재한다.
- 외국인 보유율(`foreign_holding_ratio`)과 외국인 한도소진율은 raw_payload 에서 안정적으로 파생 가능하다.

### 3.10 공매도 비율 vs daily_ohlcv (D5)

KRX source 의 `short_selling_volume`/`short_selling_value` 를 `(trade_date,ticker,market)` 으로 묶고 `daily_ohlcv` 와 join 한 결과:

| 항목 | 값 |
|---|---:|
| joined_rows | 5,064,231 |
| zero_ohlcv_volume_rows | 101,489 |
| short_volume_gt_total_volume_rows | 3,094 |
| short_volume_gt_total_volume_pct | 0.061095% |
| p50_short_volume_ratio | 0.0134% |
| p95_short_volume_ratio | 10.59% |
| p99_short_volume_ratio | 23.18% |
| max_short_volume_ratio | 8,893.33% |
| p50_short_value_ratio | 0.0138% |
| p95_short_value_ratio | 11.04% |
| p99_short_value_ratio | 24.66% |
| max_short_value_ratio | 7,909.09% |

공매도 거래량이 일 거래량보다 큰 상위 예시:

| trade_date | ticker | market | short_volume | daily_volume | short_volume_ratio | short_value_ratio |
|---|---|---|---:|---:|---:|---:|
| 2019-12-23 | 025560 | KOSPI | 1,182,279 | 13,294 | 88.93 | 0.173 |
| 2018-08-02 | 025560 | KOSPI | 755,473 | 9,129 | 82.76 | 0.170 |
| 2018-07-26 | 025560 | KOSPI | 977,030 | 12,410 | 78.73 | 0.161 |
| 2019-12-11 | 025560 | KOSPI | 597,579 | 9,443 | 63.28 | 0.122 |
| 2019-12-20 | 025560 | KOSPI | 837,107 | 13,566 | 61.71 | 0.120 |

`short_value_ratio` 는 같은 행에서 1보다 훨씬 작으므로 단순 수집 오류라기보다 `daily_ohlcv.volume` 의 수정주식수/거래정지 표기와 KRX 공매도 원수량 단위가 어긋나는 케이스로 보인다. 공매도 volume ratio 는 cap 또는 anomaly flag 와 함께 쓰는 편이 안전하다.

---

## 4. 모델링/피처 엔지니어링 시사점

1. **source dedupe 선행 필수**
   - 조회 시 `source` 를 표시하지 않으면 `KRX`/`PYKRX` 중복이 그대로 노출된다.
   - 권장 규칙: `(trade_date,ticker,market,metric_code)` 별 `KRX` 우선, 없으면 `PYKRX`.
   - 겹치는 19.9M 키는 값이 완전히 같으므로 우선순위 dedupe 로 정보 손실은 거의 없다.

2. **학습 기간은 2015년 이후 권장**
   - `krx_security_flow_raw` 자체는 2007년부터 넓게 존재하지만, `daily_ohlcv` 는 2014년 이전 횡단 커버리지가 거의 없다.
   - 수급 + 가격 기반 모델은 `trade_date >= '2015-01-02'` 를 기본 cutoff 로 두는 것이 가장 일관적이다. 2014년은 조인 가능률 95.16%라 백테스트 보조 구간으로 사용 가능하다.

3. **long-format 에서 wide-format pivot 필요**
   - 모델 입력은 날짜·종목 행 하나에 `foreign_holding_shares`, `foreign_net_buy_volume`, `short_selling_value` 등이 컬럼으로 펼쳐진 wide table 이 적합하다.
   - source dedupe 후 pivot 해야 중복 행으로 label leakage 또는 row explosion 이 생기지 않는다.

4. **순매수 3주체 합산 검증 금지**
   - 저장된 투자자 metric 은 기관/개인/외국인만 포함하고 `기타법인` 이 빠져 있다.
   - `institution + individual + foreign = 0` 을 데이터 품질 규칙으로 쓰면 정상 데이터를 오탐한다.
   - 오히려 잔차 `-(institution + individual + foreign)` 를 `other_corp_net_buy_proxy` 로 파생하는 것이 가능하다.

5. **외국인 보유율 파생 피처 추천**
   - `foreign_holding_shares` 단독보다 `foreign_holding_ratio = FORN_SHR_RT / 100` 이 종목 간 비교 가능성이 높다.
   - payload 의 `LIST_SHRS`, `FORN_SHR_RT`, `FORN_LMT_EXHST_RT` 는 결측 0이고 내부 검증도 통과했다.

6. **공매도 피처는 value ratio 우선**
   - `short_selling_volume / daily_ohlcv.volume` 은 일부 수정주식수 불일치로 극단치가 발생한다.
   - `short_selling_value / (close * volume)` 도 outlier 가 있지만 volume ratio 보다 해석 안정성이 낫다.
   - 둘 다 p99 cap 또는 logit/윈저라이징 후 사용 권장.

7. **공매도 잔고는 2016-06-30 이후만 사용**
   - `short_selling_balance_quantity` 는 시작일이 늦고 최신일 coverage 도 916종목으로 거래량/거래대금보다 낮다.
   - 2016년 이전 모델 입력에서는 구조적 결측으로 처리하고, 단순 0 대체는 피해야 한다.

8. **zero 값은 결측이 아니라 이벤트 부재**
   - 공매도 거래량/대금 zero 52.27%, 외국인 보유 zero 4.71%, 기관 순매수 zero 29.22%.
   - 공매도 zero 는 “공매도 없음”, 순매수 zero 는 “순매수 균형/거래 없음”에 가까우므로 결측으로 바꾸지 않는다.

9. **마스터 조인은 안전**
   - flow 의 2,776 `(ticker,market)` 은 모두 `stock_master` 와 `daily_ohlcv` 에 존재한다.
   - 마스터에만 있는 4종목은 모두 `DELISTED` 상태라 학습 universe 에서 제외해도 무방하다.

---

## 5. 후속 작업 권장

- 피처 빌드 SQL 또는 view 에 `KRX` 우선 dedupe CTE 표준화.
- `TRDVAL2`(`기타법인`) 를 추가 수집하여 투자자별 순매수 합계 검증 가능하게 개선.
- `foreign_holding_ratio`, `foreign_limit_exhaust_ratio` 를 raw_payload 에서 정규 컬럼 또는 feature view 로 승격.
- 공매도 volume ratio outlier 3,094건에 대한 액면분할/수정주식수 영향 검증.
- `short_selling_balance_quantity` coverage monitor 추가: 최신일 916종목은 거래량/거래대금 1,920종목 대비 낮음.
- `metric_code, source, trade_date` 조회가 잦다면 `(metric_code, source, trade_date)` 계열 보조 인덱스 검토. 현재 인덱스는 ticker 중심이라 통계/feature build 쿼리가 매번 전체 스캔에 가깝다.
