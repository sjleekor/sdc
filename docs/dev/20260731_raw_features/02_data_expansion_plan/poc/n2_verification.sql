-- N2-10 verification V1..V5 against the prod raw layer.
--
--   .agents/skills/sdc-db/scripts/dbq.sh sj2 -f <this file>
--
-- V6 (industry-wise fin_value_z dispersion) is not here: it needs the research
-- feature pipeline, so it runs against the parquet lake after D-8.
-- V4's market-cap weight half also waits for N1-8 to populate daily_market_cap.

\echo '== V1  induty_code 결측률 (기준 < 2%) =='
SELECT
    count(*)                                                   AS profiled,
    count(*) FILTER (WHERE induty_code IS NULL)                AS missing,
    round(100.0 * count(*) FILTER (WHERE induty_code IS NULL) / nullif(count(*), 0), 3)
                                                               AS missing_pct
FROM dart_corp_master
WHERE profile_fetched_at IS NOT NULL;

\echo ''
\echo '== V1b  induty_code 자릿수 분포 (PoC: 2/3/4/5 혼재) =='
SELECT length(induty_code) AS code_len, count(*) AS n
FROM dart_corp_master
WHERE profile_fetched_at IS NOT NULL AND induty_code IS NOT NULL
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '== V2  2자리 prefix 그룹 크기 분포 (기준: 그룹당 20 이상) =='
WITH g AS (
    SELECT left(induty_code, 2) AS grp, count(*) AS n
    FROM dart_corp_master
    WHERE profile_fetched_at IS NOT NULL AND induty_code IS NOT NULL
    GROUP BY 1
)
SELECT
    count(*)                                  AS groups,
    count(*) FILTER (WHERE n < 20)            AS under_20,
    count(*) FILTER (WHERE n = 1)             AS singletons,
    min(n)                                    AS min_n,
    round(avg(n), 1)                          AS avg_n,
    max(n)                                    AS max_n
FROM g;

\echo ''
\echo '== V2b  20 미만 그룹 목록 (병합 대상) =='
SELECT left(induty_code, 2) AS grp, count(*) AS n
FROM dart_corp_master
WHERE profile_fetched_at IS NOT NULL AND induty_code IS NOT NULL
GROUP BY 1 HAVING count(*) < 20
ORDER BY 2 DESC, 1;

\echo ''
\echo '== V3  corp_cls vs stock_master.market 불일치 (기준 0) =='
-- corp_cls: Y=KOSPI(유가), K=KOSDAQ(코스닥), N=KONEX, E=기타.
-- 상폐 법인은 stock_master에 없으므로 조인에서 자연히 빠진다 (= 살아있는 종목만 비교).
SELECT
    c.corp_cls,
    s.market,
    count(*) AS n
FROM dart_corp_master c
JOIN stock_master s ON s.ticker = c.ticker
WHERE c.profile_fetched_at IS NOT NULL AND c.corp_cls IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;

\echo ''
\echo '== V3b  불일치 건수만 =='
SELECT count(*) AS mismatches
FROM dart_corp_master c
JOIN stock_master s ON s.ticker = c.ticker
WHERE c.profile_fetched_at IS NOT NULL
  AND c.corp_cls IS NOT NULL
  AND NOT (
        (c.corp_cls = 'Y' AND s.market = 'KOSPI')
     OR (c.corp_cls = 'K' AND s.market = 'KOSDAQ')
  );

\echo ''
\echo '== V4  acc_mt != 12 종목 수 (PoC 추정 약 158) =='
SELECT
    coalesce(acc_mt, '(null)') AS acc_mt,
    count(*)                   AS n
FROM dart_corp_master
WHERE profile_fetched_at IS NOT NULL
GROUP BY 1
ORDER BY n DESC;

\echo ''
\echo '== V4b  acc_mt != 12 중 현재 상장 종목 (metric_vintages 영향 범위) =='
SELECT count(*) AS listed_non_december
FROM dart_corp_master c
JOIN stock_master s ON s.ticker = c.ticker
WHERE c.profile_fetched_at IS NOT NULL
  AND c.acc_mt IS NOT NULL
  AND c.acc_mt <> '12';

\echo ''
\echo '== V5  est_dt 결측률·이상치 =='
SELECT
    count(*)                                                          AS profiled,
    count(*) FILTER (WHERE est_dt IS NULL)                            AS missing,
    count(*) FILTER (WHERE est_dt > current_date)                     AS future_dated,
    count(*) FILTER (WHERE est_dt < DATE '1900-01-01')                AS implausibly_old,
    min(est_dt)                                                       AS oldest,
    max(est_dt)                                                       AS newest
FROM dart_corp_master
WHERE profile_fetched_at IS NOT NULL;

\echo ''
\echo '== V5b  설립연도 10년 버킷 (firm age 피쳐 사용 가능성) =='
SELECT (extract(year FROM est_dt)::int / 10) * 10 AS decade, count(*) AS n
FROM dart_corp_master
WHERE profile_fetched_at IS NOT NULL AND est_dt IS NOT NULL
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '== 커버리지: 상폐 포함 대상 전체가 실제로 수집됐는가 =='
SELECT
    count(*) FILTER (WHERE ticker IS NOT NULL)                                AS with_ticker,
    count(*) FILTER (WHERE ticker IS NOT NULL AND profile_fetched_at IS NOT NULL) AS profiled,
    count(*) FILTER (WHERE ticker IS NOT NULL AND profile_fetched_at IS NULL)  AS still_todo
FROM dart_corp_master;
