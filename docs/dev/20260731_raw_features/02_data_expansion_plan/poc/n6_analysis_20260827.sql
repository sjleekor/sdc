-- N6 local-only diagnostics against the 2026-08-23 raw parquet snapshot.
-- Run from the repository root:
--   duckdb -c ".read docs/dev/20260731_raw_features/02_data_expansion_plan/poc/n6_analysis_20260827.sql"

CREATE OR REPLACE TEMP VIEW employee_raw AS
SELECT *
FROM read_parquet(
    'data_lake/raw_postgres/snapshot_date=2026-08-23/source=sj2_remote/dart_employee_raw/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE TEMP VIEW governance_raw AS
SELECT *
FROM read_parquet(
    'data_lake/raw_postgres/snapshot_date=2026-08-23/source=sj2_remote/dart_governance_raw/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE TEMP VIEW filing_raw AS
SELECT *
FROM read_parquet(
    'data_lake/raw_postgres/snapshot_date=2026-08-23/source=sj2_remote/dart_filing_receipt_raw/**/*.parquet',
    hive_partitioning = true
);

-- Corrections can leave more than one receipt in raw. Cross-sectional diagnostics
-- use the latest receipt, while the vintage section measures what was discarded.
CREATE OR REPLACE TEMP VIEW employee_latest_receipt AS
SELECT corp_code, bsns_year, max(rcept_no) AS rcept_no
FROM employee_raw
WHERE statement_type = 'employee'
GROUP BY ALL;

CREATE OR REPLACE TEMP VIEW employee_parsed AS
WITH selected AS (
    SELECT e.*,
           trim(json_extract_string(raw_payload, '$.fo_bbm')) AS division,
           try_cast(regexp_replace(json_extract_string(raw_payload, '$.sm'), '[^0-9.-]', '', 'g') AS DOUBLE) AS headcount,
           try_cast(regexp_replace(json_extract_string(raw_payload, '$.jan_salary_am'), '[^0-9.-]', '', 'g') AS DOUBLE) AS average_pay
    FROM employee_raw e
    JOIN employee_latest_receipt r USING (corp_code, bsns_year, rcept_no)
    WHERE statement_type = 'employee'
), flags AS (
    SELECT *,
           division LIKE '%합계%' AS is_summary,
           max(CASE WHEN division LIKE '%합계%' AND headcount IS NOT NULL THEN 1 ELSE 0 END)
               OVER (PARTITION BY corp_code, bsns_year, rcept_no) AS has_summary_headcount,
           max(CASE WHEN division LIKE '%합계%' AND average_pay IS NOT NULL THEN 1 ELSE 0 END)
               OVER (PARTITION BY corp_code, bsns_year, rcept_no) AS has_summary_pay
    FROM selected
)
SELECT * FROM flags;

CREATE OR REPLACE TEMP VIEW employee_annual AS
SELECT corp_code,
       any_value(ticker) AS ticker,
       bsns_year,
       any_value(rcept_no) AS rcept_no,
       sum(headcount) FILTER (
           WHERE headcount IS NOT NULL
             AND (has_summary_headcount = 0 OR is_summary)
       ) AS headcount,
       sum(average_pay * headcount) FILTER (
           WHERE average_pay IS NOT NULL AND headcount IS NOT NULL
             AND (has_summary_pay = 0 OR is_summary)
       ) / nullif(sum(headcount) FILTER (
           WHERE average_pay IS NOT NULL AND headcount IS NOT NULL
             AND (has_summary_pay = 0 OR is_summary)
       ), 0) AS average_pay
FROM employee_parsed
GROUP BY corp_code, bsns_year;

CREATE OR REPLACE TEMP VIEW structural_change_year AS
SELECT DISTINCT corp_code, year(rcept_dt)::INTEGER AS bsns_year
FROM filing_raw
WHERE regexp_matches(report_nm, '합병등종료보고서\((분할|합병)\)');

CREATE OR REPLACE TEMP VIEW employee_growth AS
WITH lagged AS (
    SELECT *,
           lag(bsns_year) OVER (PARTITION BY corp_code ORDER BY bsns_year) AS previous_year,
           lag(headcount) OVER (PARTITION BY corp_code ORDER BY bsns_year) AS previous_headcount,
           lag(average_pay) OVER (PARTITION BY corp_code ORDER BY bsns_year) AS previous_average_pay
    FROM employee_annual
)
SELECT l.*,
       headcount / nullif(previous_headcount, 0) - 1 AS headcount_growth,
       average_pay / nullif(previous_average_pay, 0) - 1 AS average_pay_growth,
       s.corp_code IS NOT NULL AS structural_change,
       s.corp_code IS NOT NULL
           AND abs(headcount / nullif(previous_headcount, 0) - 1) >= 0.30 AS structural_mask
FROM lagged l
LEFT JOIN structural_change_year s USING (corp_code, bsns_year)
WHERE previous_year = bsns_year - 1;

-- Largest-shareholder group stake. Prefer an explicit total, then a common/voting
-- share total, then any total row, and only then the largest numeric member row.
CREATE OR REPLACE TEMP VIEW major_latest_receipt AS
SELECT corp_code, bsns_year, max(rcept_no) AS rcept_no
FROM governance_raw
WHERE statement_type = 'major_shareholder'
GROUP BY ALL;

CREATE OR REPLACE TEMP VIEW major_rows AS
SELECT g.*,
       trim(json_extract_string(raw_payload, '$.nm')) AS holder_name,
       trim(json_extract_string(raw_payload, '$.stock_knd')) AS stock_kind,
       try_cast(regexp_replace(
           json_extract_string(raw_payload, '$.trmend_posesn_stock_qota_rt'),
           '[^0-9.-]', '', 'g'
       ) AS DOUBLE) AS stake,
       CASE
           WHEN trim(json_extract_string(raw_payload, '$.nm')) = '계'
                AND trim(json_extract_string(raw_payload, '$.stock_knd')) = '합계' THEN 1
           WHEN trim(json_extract_string(raw_payload, '$.nm')) = '계'
                AND (json_extract_string(raw_payload, '$.stock_knd') LIKE '%보통%'
                     OR json_extract_string(raw_payload, '$.stock_knd') LIKE '%의결권%') THEN 2
           WHEN trim(json_extract_string(raw_payload, '$.nm')) = '계' THEN 3
           ELSE 4
       END AS selection_priority
FROM governance_raw g
JOIN major_latest_receipt r USING (corp_code, bsns_year, rcept_no)
WHERE statement_type = 'major_shareholder';

CREATE OR REPLACE TEMP VIEW major_annual AS
WITH ranked AS (
    SELECT *, min(selection_priority) OVER (PARTITION BY corp_code, bsns_year) AS best_priority
    FROM major_rows
    WHERE stake IS NOT NULL AND stake BETWEEN 0 AND 100
)
SELECT corp_code,
       any_value(ticker) AS ticker,
       bsns_year,
       any_value(rcept_no) AS rcept_no,
       max(stake) FILTER (WHERE selection_priority = best_priority) AS major_stake,
       min(best_priority) AS selection_priority
FROM ranked
GROUP BY corp_code, bsns_year;

CREATE OR REPLACE TEMP VIEW major_growth AS
WITH lagged AS (
    SELECT *,
           lag(bsns_year) OVER (PARTITION BY corp_code ORDER BY bsns_year) AS previous_year,
           lag(major_stake) OVER (PARTITION BY corp_code ORDER BY bsns_year) AS previous_stake
    FROM major_annual
)
SELECT *, major_stake - previous_stake AS stake_change_pp
FROM lagged
WHERE previous_year = bsns_year - 1;

-- The audit endpoint labels periods as current/prior/two-years-prior. stlm_dt is
-- the current report date on every row, so it cannot identify the observation year.
CREATE OR REPLACE TEMP VIEW audit_observation AS
WITH parsed AS (
    SELECT corp_code,
           ticker,
           bsns_year AS request_year,
           rcept_no,
           regexp_replace(coalesce(json_extract_string(raw_payload, '$.bsns_year'), ''), '\s', '', 'g') AS period_label,
           regexp_replace(coalesce(json_extract_string(raw_payload, '$.adt_opinion'), ''), '\s', '', 'g') AS opinion,
           CASE
               WHEN json_extract_string(raw_payload, '$.bsns_year') LIKE '%전전기%' THEN 2
               WHEN json_extract_string(raw_payload, '$.bsns_year') LIKE '%전기%' THEN 1
               WHEN json_extract_string(raw_payload, '$.bsns_year') LIKE '%당기%' THEN 0
           END AS year_offset
    FROM governance_raw
    WHERE statement_type = 'audit_opinion'
), classified AS (
    SELECT *,
           request_year - year_offset AS observation_year,
           CASE
               WHEN opinion = '' OR opinion = '-' THEN 'blank'
               WHEN opinion LIKE '%의견거절%' OR opinion = '거절' THEN 'disclaimer'
               WHEN opinion LIKE '%부적정%' THEN 'adverse'
               WHEN opinion LIKE '%한정%' THEN 'qualified'
               WHEN opinion LIKE '%적정%' OR opinion LIKE '%공정%'
                    OR opinion LIKE '%예외사항%없%' OR opinion LIKE '%지적사항%없%' THEN 'proper'
               ELSE 'unknown'
           END AS opinion_class
    FROM parsed
    WHERE year_offset IS NOT NULL
)
-- 2025 responses contain exact duplicate current/prior/two-prior rows. Collapse
-- them without hiding disagreement between classes in the same response.
SELECT corp_code,
       any_value(ticker) AS ticker,
       request_year,
       rcept_no,
       observation_year,
       CASE
           WHEN count(*) FILTER (WHERE opinion_class IN ('disclaimer', 'adverse', 'qualified')) > 0
               THEN 'nonproper'
           WHEN count(*) FILTER (WHERE opinion_class = 'proper') > 0 THEN 'proper'
           WHEN count(*) FILTER (WHERE opinion_class = 'unknown') > 0 THEN 'unknown'
           ELSE 'blank'
       END AS opinion_class,
       count(*) AS source_rows,
       count(DISTINCT opinion_class) AS class_count
FROM classified
GROUP BY corp_code, request_year, rcept_no, observation_year;

CREATE OR REPLACE TEMP VIEW major_change_event AS
WITH parsed AS (
    SELECT corp_code,
           ticker,
           bsns_year AS request_year,
           rcept_no,
           try_strptime(
               json_extract_string(raw_payload, '$.change_on'),
               ['%Y년 %m월 %d일', '%Y-%m-%d', '%Y.%m.%d']
           )::DATE AS change_date,
           trim(json_extract_string(raw_payload, '$.mxmm_shrholdr_nm')) AS holder_name,
           trim(json_extract_string(raw_payload, '$.change_cause')) AS change_cause,
           try_cast(regexp_replace(json_extract_string(raw_payload, '$.qota_rt'), '[^0-9.-]', '', 'g') AS DOUBLE) AS stake
    FROM governance_raw
    WHERE statement_type = 'major_change'
)
SELECT * FROM parsed WHERE change_date IS NOT NULL;

-- 1. Coverage and cross-sectional variation.
SELECT 'employee_coverage' AS section,
       bsns_year,
       count(*) AS companies,
       count(headcount) AS valid_headcount,
       count(average_pay) AS valid_pay
FROM employee_annual
GROUP BY bsns_year
ORDER BY bsns_year;

SELECT 'employee_growth' AS section,
       count(headcount_growth) AS observations,
       avg(headcount_growth) AS mean,
       stddev_samp(headcount_growth) AS stddev,
       quantile_cont(headcount_growth, 0.10) AS p10,
       median(headcount_growth) AS median,
       quantile_cont(headcount_growth, 0.90) AS p90,
       avg((abs(headcount_growth) > 0.05)::INTEGER) AS abs_gt_5pct,
       avg((abs(headcount_growth) >= 0.30)::INTEGER) AS abs_ge_30pct,
       count(*) FILTER (WHERE structural_change) AS evidence_rows,
       count(*) FILTER (WHERE structural_mask) AS masked_rows
FROM employee_growth;

WITH valid AS (
    SELECT headcount_growth
    FROM employee_growth
    WHERE headcount >= 1 AND previous_headcount >= 1 AND isfinite(headcount_growth)
), bounds AS (
    SELECT quantile_cont(headcount_growth, 0.01) AS p01,
           quantile_cont(headcount_growth, 0.99) AS p99
    FROM valid
)
SELECT 'employee_growth_robust' AS section,
       count(*) AS observations,
       min(p01) AS p01,
       quantile_cont(headcount_growth, 0.10) AS p10,
       median(headcount_growth) AS median,
       quantile_cont(headcount_growth, 0.90) AS p90,
       max(p99) AS p99,
       stddev_samp(greatest(p01, least(p99, headcount_growth))) AS winsorized_stddev
FROM valid CROSS JOIN bounds;

SELECT 'employee_mask_effect' AS section,
       structural_mask AS masked,
       count(*) AS observations,
       quantile_cont(headcount_growth, 0.10) AS p10,
       median(headcount_growth) AS median,
       quantile_cont(headcount_growth, 0.90) AS p90
FROM employee_growth
WHERE headcount >= 1 AND previous_headcount >= 1 AND isfinite(headcount_growth)
GROUP BY structural_mask
ORDER BY structural_mask;

SELECT 'employee_growth_by_year' AS section,
       bsns_year,
       count(headcount_growth) AS observations,
       stddev_samp(headcount_growth) AS stddev,
       median(headcount_growth) AS median,
       avg((abs(headcount_growth) > 0.05)::INTEGER) AS abs_gt_5pct,
       count(*) FILTER (WHERE structural_mask) AS masked_rows
FROM employee_growth
GROUP BY bsns_year
ORDER BY bsns_year;

SELECT 'pay_growth' AS section,
       count(average_pay_growth) AS observations,
       stddev_samp(average_pay_growth) AS stddev,
       quantile_cont(average_pay_growth, 0.10) AS p10,
       median(average_pay_growth) AS median,
       quantile_cont(average_pay_growth, 0.90) AS p90,
       avg((abs(average_pay_growth) > 0.05)::INTEGER) AS abs_gt_5pct,
       count(*) FILTER (WHERE abs(average_pay_growth) >= 1) AS abs_ge_100pct,
       count(*) FILTER (WHERE abs(average_pay_growth) >= 5) AS abs_ge_500pct
FROM employee_growth;

SELECT 'pay_quality' AS section,
       count(average_pay) AS values_total,
       count(*) FILTER (WHERE average_pay BETWEEN 1000000 AND 1000000000) AS plausible_krw_values,
       min(average_pay) AS min_value,
       quantile_cont(average_pay, 0.01) AS p01,
       median(average_pay) AS median,
       quantile_cont(average_pay, 0.99) AS p99,
       max(average_pay) AS max_value
FROM employee_annual;

SELECT 'pay_growth_plausible' AS section,
       count(*) AS observations,
       stddev_samp(average_pay_growth) AS stddev,
       quantile_cont(average_pay_growth, 0.10) AS p10,
       median(average_pay_growth) AS median,
       quantile_cont(average_pay_growth, 0.90) AS p90,
       avg((abs(average_pay_growth) > 0.05)::INTEGER) AS abs_gt_5pct,
       count(*) FILTER (WHERE abs(average_pay_growth) >= 1) AS abs_ge_100pct,
       count(*) FILTER (WHERE abs(average_pay_growth) >= 5) AS abs_ge_500pct
FROM employee_growth
WHERE average_pay BETWEEN 1000000 AND 1000000000
  AND previous_average_pay BETWEEN 1000000 AND 1000000000;

WITH revenue AS (
    SELECT ticker,
           bsns_year,
           value_numeric::DOUBLE AS revenue,
           row_number() OVER (
               PARTITION BY ticker, bsns_year
               ORDER BY CASE WHEN fs_basis = 'CFS' THEN 0 ELSE 1 END, rcept_no DESC
           ) AS row_number
    FROM read_parquet(
        'data_lake/feature_mart/snapshot_date=2026-08-23/source=sj2_remote/stock_metric_vintage_fact/*.parquet'
    )
    WHERE metric_code = 'revenue' AND reprt_code = '11011' AND value_numeric > 0
), productivity AS (
    SELECT e.bsns_year,
           e.ticker,
           ln(r.revenue / e.headcount) AS log_revenue_per_employee
    FROM employee_annual e
    JOIN revenue r USING (ticker, bsns_year)
    WHERE r.row_number = 1 AND e.headcount > 0
)
SELECT 'revenue_per_employee' AS section,
       bsns_year,
       count(*) AS companies,
       stddev_samp(log_revenue_per_employee) AS log_stddev,
       quantile_cont(log_revenue_per_employee, 0.10) AS p10_log,
       median(log_revenue_per_employee) AS median_log,
       quantile_cont(log_revenue_per_employee, 0.90) AS p90_log
FROM productivity
GROUP BY bsns_year
ORDER BY bsns_year;

SELECT 'major_stake_coverage' AS section,
       bsns_year,
       count(*) AS valid_companies,
       stddev_samp(major_stake) AS level_stddev_pp,
       quantile_cont(major_stake, 0.10) AS p10,
       median(major_stake) AS median,
       quantile_cont(major_stake, 0.90) AS p90
FROM major_annual
GROUP BY bsns_year
ORDER BY bsns_year;

SELECT 'major_stake_change' AS section,
       count(*) AS observations,
       stddev_samp(stake_change_pp) AS stddev_pp,
       quantile_cont(stake_change_pp, 0.10) AS p10_pp,
       median(stake_change_pp) AS median_pp,
       quantile_cont(stake_change_pp, 0.90) AS p90_pp,
       avg((abs(stake_change_pp) >= 1)::INTEGER) AS abs_ge_1pp,
       avg((abs(stake_change_pp) >= 5)::INTEGER) AS abs_ge_5pp
FROM major_growth;

WITH latest AS (
    SELECT *
    FROM audit_observation
    WHERE request_year = observation_year
    QUALIFY row_number() OVER (
        PARTITION BY corp_code, observation_year ORDER BY rcept_no DESC
    ) = 1
)
SELECT 'audit_current_period' AS section,
       observation_year,
       count(*) FILTER (WHERE opinion_class IN ('proper', 'nonproper')) AS valid_companies,
       count(*) FILTER (WHERE opinion_class = 'nonproper') AS nonproper,
       avg((opinion_class = 'nonproper')::INTEGER)
           FILTER (WHERE opinion_class IN ('proper', 'nonproper')) AS nonproper_rate,
       count(*) FILTER (WHERE opinion_class = 'unknown') AS unknown,
       count(*) FILTER (WHERE opinion_class = 'blank') AS blank
FROM latest
GROUP BY observation_year
ORDER BY observation_year;

SELECT 'major_change' AS section,
       year(change_date) AS event_year,
       count(*) AS response_rows,
       count(DISTINCT (corp_code, change_date, holder_name, change_cause)) AS unique_events,
       count(DISTINCT corp_code) AS companies
FROM major_change_event
WHERE year(change_date) >= 2015
GROUP BY event_year
ORDER BY event_year;

-- 2. Final-vintage and response-shape limitations.
WITH annual_receipts AS (
    SELECT corp_code,
           try_cast(regexp_extract(report_nm, '\(([0-9]{4})\.', 1) AS INTEGER) AS bsns_year,
           count(*) AS filing_receipts
    FROM filing_raw
    WHERE report_nm LIKE '%사업보고서 (%'
    GROUP BY corp_code, bsns_year
), n6_receipts AS (
    SELECT corp_code, bsns_year, count(DISTINCT rcept_no) AS n6_receipts
    FROM employee_raw
    WHERE statement_type = 'employee'
    GROUP BY corp_code, bsns_year
)
SELECT 'employee_vintage_capture' AS section,
       count(*) AS corp_years,
       count(*) FILTER (WHERE filing_receipts > 1) AS revised_corp_years,
       sum(filing_receipts) AS filing_receipts,
       sum(n6_receipts) AS n6_receipts,
       sum(greatest(filing_receipts - n6_receipts, 0)) AS prior_receipts_not_in_n6
FROM n6_receipts
JOIN annual_receipts USING (corp_code, bsns_year);

WITH parsed AS (
    SELECT corp_code,
           bsns_year,
           rcept_no,
           trim(json_extract_string(raw_payload, '$.fo_bbm')) AS division,
           try_cast(regexp_replace(json_extract_string(raw_payload, '$.sm'), '[^0-9.-]', '', 'g') AS DOUBLE) AS headcount
    FROM employee_raw
    WHERE statement_type = 'employee'
), flagged AS (
    SELECT *,
           division LIKE '%합계%' AS is_summary,
           max(CASE WHEN division LIKE '%합계%' AND headcount IS NOT NULL THEN 1 ELSE 0 END)
               OVER (PARTITION BY corp_code, bsns_year, rcept_no) AS has_summary
    FROM parsed
), annual AS (
    SELECT corp_code,
           bsns_year,
           rcept_no,
           sum(headcount) FILTER (WHERE headcount IS NOT NULL AND (has_summary = 0 OR is_summary)) AS headcount
    FROM flagged
    GROUP BY corp_code, bsns_year, rcept_no
), compared AS (
    SELECT corp_code,
           bsns_year,
           count(*) AS receipts,
           count(DISTINCT headcount) AS headcount_values,
           min(headcount) AS min_headcount,
           max(headcount) AS max_headcount
    FROM annual
    GROUP BY corp_code, bsns_year
)
SELECT 'employee_observed_corrections' AS section,
       count(*) FILTER (WHERE receipts > 1) AS multi_receipt_corp_years,
       count(*) FILTER (WHERE receipts > 1 AND headcount_values > 1) AS changed_headcount,
       max((max_headcount - min_headcount) / nullif(min_headcount, 0))
           FILTER (WHERE receipts > 1 AND headcount_values > 1) AS max_relative_change
FROM compared;

SELECT 'raw_multi_receipt' AS section,
       statement_type,
       count(*) AS corp_request_years,
       count(*) FILTER (WHERE receipts > 1) AS multi_receipt_groups,
       max(receipts) AS max_receipts
FROM (
    SELECT statement_type, corp_code, bsns_year, count(DISTINCT rcept_no) AS receipts
    FROM (
        SELECT statement_type, corp_code, bsns_year, rcept_no FROM employee_raw
        UNION ALL
        SELECT statement_type, corp_code, bsns_year, rcept_no FROM governance_raw
    )
    GROUP BY statement_type, corp_code, bsns_year
)
GROUP BY statement_type
ORDER BY statement_type;

WITH comparable AS (
    SELECT corp_code,
           observation_year,
           count(DISTINCT request_year) AS request_years,
           count(DISTINCT opinion_class) FILTER (WHERE opinion_class IN ('proper', 'nonproper')) AS classes
    FROM audit_observation
    GROUP BY corp_code, observation_year
)
SELECT 'audit_carry_forward' AS section,
       count(*) FILTER (WHERE request_years > 1) AS repeated_corp_years,
       count(*) FILTER (WHERE request_years > 1 AND classes > 1) AS binary_disagreements,
       count(*) FILTER (WHERE request_years = 1) AS single_snapshot_corp_years
FROM comparable;

SELECT 'audit_duplicate_shape' AS section,
       request_year,
       count(*) AS observations,
       sum(source_rows) AS raw_rows,
       count(*) FILTER (WHERE source_rows > 1) AS duplicated_observations,
       max(source_rows) AS max_source_rows
FROM audit_observation
GROUP BY request_year
ORDER BY request_year;

SELECT 'major_change_delay' AS section,
       count(*) AS response_rows,
       count(DISTINCT (corp_code, change_date, holder_name, change_cause)) AS unique_events,
       median(date_diff('day', change_date, strptime(substr(rcept_no, 1, 8), '%Y%m%d')::DATE)) AS median_delay_days,
       quantile_cont(date_diff('day', change_date, strptime(substr(rcept_no, 1, 8), '%Y%m%d')::DATE), 0.90) AS p90_delay_days,
       max(date_diff('day', change_date, strptime(substr(rcept_no, 1, 8), '%Y%m%d')::DATE)) AS max_delay_days
FROM major_change_event;

-- 3. Value-up disclosure classification. This remains a collection tag, not a
-- Horizon Scan candidate, because the sample starts in 2024.
SELECT 'value_up_filings' AS section,
       year(rcept_dt) AS receipt_year,
       count(*) AS filings,
       count(DISTINCT corp_code) AS companies,
       min(rcept_dt) AS first_date,
       max(rcept_dt) AS last_date
FROM filing_raw
WHERE regexp_matches(replace(report_nm, ' ', ''), '기업가치제고계획|밸류업')
GROUP BY receipt_year
ORDER BY receipt_year;

SELECT 'value_up_total' AS section,
       count(*) AS filings,
       count(DISTINCT corp_code) AS companies,
       count(*) FILTER (WHERE report_nm LIKE '%제고계획(자율공시)%') AS plan_filings,
       count(DISTINCT corp_code) FILTER (WHERE report_nm LIKE '%제고계획(자율공시)%') AS plan_companies,
       count(*) FILTER (WHERE report_nm LIKE '%예고%') AS announcement_filings
FROM filing_raw
WHERE regexp_matches(replace(report_nm, ' ', ''), '기업가치제고계획|밸류업');
