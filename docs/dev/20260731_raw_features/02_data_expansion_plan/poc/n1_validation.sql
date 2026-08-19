-- N1-9 검증 V2·V3·V5·V6·V8 — daily_market_cap(KRX Open API) 대 daily_ohlcv(naver).
--
-- 로컬 미러에서 돌린다. prod에 직접 걸면 S-1 백필과 같은 디스크를 두고 다툰다
-- (2026-08-19 실측: 프로덕션에서 단순 count(distinct)가 5분 넘게 걸렸다).
--   uv run krx-collector db sync-remote --full-refresh --tables daily_market_cap,daily_ohlcv
--
-- V4·V7은 여기 없다. DART 쪽(dart_capital_change_raw · dart_share_count_raw)이
-- 필요한데 S-1 잔여 백필이 상폐 법인분을 아직 채우는 중이다.

-- ==========================================================================
-- V6. 일별 커버리지 시계열 — 급락 구간은 수집 실패다
-- ==========================================================================
-- 먼저 본다. 커버리지에 구멍이 있으면 아래 넷의 전제가 통째로 무너진다.

\echo '=== V6a. 월별 종목 수 추이 (완만한 증가여야 한다) ==='
SELECT date_trunc('month', trade_date)::date AS mon,
       market,
       count(DISTINCT trade_date)            AS sessions,
       round(avg(cnt))                       AS avg_tickers,
       min(cnt)                              AS min_tickers,
       max(cnt)                              AS max_tickers
FROM (SELECT trade_date, market, count(*) AS cnt
      FROM daily_market_cap GROUP BY 1, 2) d
GROUP BY 1, 2
ORDER BY 1, 2;

\echo '=== V6b. 직전 세션 대비 5% 이상 급락한 날 (수집 실패 후보) ==='
WITH per_session AS (
    SELECT trade_date, market, count(*) AS cnt
    FROM daily_market_cap GROUP BY 1, 2
), stepped AS (
    SELECT trade_date, market, cnt,
           lag(cnt) OVER (PARTITION BY market ORDER BY trade_date) AS prev
    FROM per_session
)
SELECT trade_date, market, prev, cnt,
       round(100.0 * (cnt - prev) / nullif(prev, 0), 1) AS pct
FROM stepped
WHERE prev IS NOT NULL AND cnt < prev * 0.95
ORDER BY trade_date;

-- ==========================================================================
-- V3. volume 불일치율 — 조정 정책이 아니라 KRX 대 naver 원천 차이다
-- ==========================================================================
-- 어느 쪽을 정본으로 볼지 정하는 것이 목적이다. 목록화까지가 범위.

\echo '=== V3. 연도별 volume 불일치율 ==='
SELECT extract(year FROM m.trade_date)::int AS yr,
       count(*)                                                    AS both,
       count(*) FILTER (WHERE m.volume IS DISTINCT FROM o.volume)  AS mismatch,
       round(100.0 * count(*) FILTER (WHERE m.volume IS DISTINCT FROM o.volume)
             / count(*), 2)                                        AS pct,
       round(avg(abs(m.volume - o.volume))
             FILTER (WHERE m.volume IS DISTINCT FROM o.volume))     AS avg_abs_diff,
       count(*) FILTER (WHERE m.volume > o.volume)                 AS krx_higher,
       count(*) FILTER (WHERE m.volume < o.volume)                 AS naver_higher
FROM daily_market_cap m
JOIN daily_ohlcv o USING (trade_date, ticker)
GROUP BY 1 ORDER BY 1;

-- ==========================================================================
-- V5. 종가 x 거래량 대 실제 거래대금 — 낮으면 px_amihud_20d 재검정
-- ==========================================================================
-- Amihud 비유동성은 |수익률| / 거래대금이다. 거래대금을 종가 x 거래량으로
-- 근사해 왔는데, daily_market_cap이 실제 거래대금(trading_value)을 준다.
-- 순위 상관이 낮으면 기존 피처가 잰 것이 유동성이 아니었다는 뜻이다.

\echo '=== V5. 세션별 Spearman 순위상관 (근사 거래대금 대 실제) ==='
WITH ranked AS (
    SELECT trade_date,
           rank() OVER (PARTITION BY trade_date
                        ORDER BY source_close::numeric * volume) AS rk_proxy,
           rank() OVER (PARTITION BY trade_date
                        ORDER BY trading_value)                  AS rk_actual
    FROM daily_market_cap
    WHERE volume > 0 AND trading_value > 0 AND source_close > 0
), per_session AS (
    SELECT trade_date, corr(rk_proxy, rk_actual) AS rho
    FROM ranked GROUP BY 1
)
SELECT extract(year FROM trade_date)::int AS yr,
       count(*)                           AS sessions,
       round(avg(rho)::numeric, 4)        AS avg_rho,
       round(min(rho)::numeric, 4)        AS min_rho
FROM per_session GROUP BY 1 ORDER BY 1;

\echo '=== V5b. 근사가 실제와 얼마나 벌어지나 (상대오차 분포) ==='
SELECT extract(year FROM trade_date)::int AS yr,
       round(100.0 * avg(abs(source_close::numeric * volume - trading_value)
                         / nullif(trading_value, 0)), 2) AS avg_rel_err_pct,
       round(100.0 * percentile_cont(0.5) WITHIN GROUP (
                 ORDER BY abs(source_close::numeric * volume - trading_value)
                          / nullif(trading_value, 0))::numeric, 2) AS median_rel_err_pct
FROM daily_market_cap
WHERE volume > 0 AND trading_value > 0 AND source_close > 0
GROUP BY 1 ORDER BY 1;

-- ==========================================================================
-- V2. 원주가 대 수정주가 비율 — K-7의 입구
-- ==========================================================================
-- source_close는 KRX 미수정, daily_ohlcv.close는 naver 수정주가다.
-- 한 종목의 비율은 corporate action이 없는 한 일정해야 하고, 계단이 지는
-- 날짜가 곧 조정 이벤트다. 계단을 셀 수 있으면 K-7이 성립한다.

\echo '=== V2a. 비율이 일정한가 — 종목별 서로 다른 비율 값의 개수 ==='
WITH ratios AS (
    SELECT m.ticker,
           round(m.source_close::numeric / nullif(o.close, 0), 6) AS ratio
    FROM daily_market_cap m
    JOIN daily_ohlcv o USING (trade_date, ticker)
    WHERE o.close > 0 AND m.source_close > 0
)
SELECT CASE WHEN n = 1 THEN '1 (조정 이벤트 없음)'
            WHEN n <= 3 THEN '2-3'
            WHEN n <= 10 THEN '4-10'
            ELSE '11+' END AS distinct_ratios,
       count(*) AS tickers
FROM (SELECT ticker, count(DISTINCT ratio) AS n FROM ratios GROUP BY 1) t
GROUP BY 1 ORDER BY 1;

\echo '=== V2b. 비율이 1이 아닌 종목은 수정이 실제로 걸린 것이다 ==='
WITH latest AS (
    SELECT DISTINCT ON (m.ticker) m.ticker, m.trade_date,
           m.source_close::numeric / nullif(o.close, 0) AS ratio
    FROM daily_market_cap m
    JOIN daily_ohlcv o USING (trade_date, ticker)
    WHERE o.close > 0 AND m.source_close > 0
    ORDER BY m.ticker, m.trade_date DESC
)
SELECT count(*)                                        AS tickers,
       count(*) FILTER (WHERE abs(ratio - 1) < 0.001)  AS ratio_is_one,
       count(*) FILTER (WHERE abs(ratio - 1) >= 0.001) AS ratio_differs
FROM latest;

-- ==========================================================================
-- V8. 시장 이전상장 — stock_master 조인이 만들었을 룩어헤드 크기
-- ==========================================================================
-- 2026-08-19에 KONEX 이전상장으로 원인이 규명됐다(04_w1_pit_universe.md §3.6).
-- 여기서는 크기를 센다.

\echo '=== V8. daily_ohlcv에만 있는 (세션, 종목) — KONEX 구간의 크기 ==='
SELECT extract(year FROM o.trade_date)::int AS yr,
       count(*)                            AS ohlcv_only_rows,
       count(DISTINCT o.ticker)            AS ohlcv_only_tickers
FROM daily_ohlcv o
LEFT JOIN daily_market_cap m USING (trade_date, ticker)
WHERE m.ticker IS NULL
GROUP BY 1 ORDER BY 1;
