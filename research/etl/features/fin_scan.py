"""``feat_fin_scan_daily`` — Phase B B-4 (04_specific_plan_B.md §4.1-§4.3, B-4).

Daily PIT materialization of the 5 continuous financial families (size,
value, profitability, asset growth, accruals) from ``fin_quarterly_metric_vintage``
(B-3) interval-joined onto A0's own PIT shares/market-cap
(``dim_stock_pit_daily``) and price-quality (``dim_price_quality_daily``) marts.

Grain: ``(trade_date, ticker, market)``, one row per raw ``daily_ohlcv`` row
(broad/tradable filtering happens downstream, same layering as ``feat_price``/
``feat_flow`` — §5.1: "official sample은 ... broad + common formation + ...").

Shared fs_basis per ticker-date (§3.6): rather than resolving CFS/OFS
independently per metric (which could silently mix bases across a single
day's feature bundle), every metric is interval-joined under *both* bases,
then one ``fs_basis_used`` decision — CFS if ``net_income`` has a CFS value at
this date, else OFS — selects every other metric's value for that date. This
is what makes ``fin_accruals_to_assets``'s net_income/CFO/avg_assets a genuine
"same four-quarter set, same fs basis" computation (§4.3), not three
independently-chosen bases that happen to collide.
"""

from __future__ import annotations

import duckdb

from research.etl.config import LakeConfig
from research.etl.mart import materialize, register_mart_view

FIN_SCAN_TABLE = "feat_fin_scan_daily"

# §1.3 fingerprint, same role as ``EVENT_FEATURE_FORMULA_VERSION`` for the event
# features: the formula/handling rules below are not covered by ``config_hash``
# (the scan YAML) or ``phase_b_code_hash`` (which only hashes
# ``horizon_scan_phase_b*.py``), so a change here would otherwise produce
# different numbers under identical run-spec fingerprints. Bump it whenever the
# ratio definitions, the winsorize/z-score handling, or the fs_basis rule change,
# and do not reuse an existing artifact of the same snapshot across a bump.
#
#   fin_v1 — the 2026-08 rules as first scanned (§4.1-§4.3).
#   fin_v2 — value components keep NULL through the winsorize step, so the
#            ">= 2 valid components" rule actually binds (10_known_issues.md I1).
#   fin_v3 — same-day metric candidates resolve to the latest fiscal period
#            instead of scan order (10_known_issues.md I12).
FIN_FEATURE_FORMULA_VERSION = "fin_v3"

# Metrics interval-joined onto the daily panel; total_assets additionally
# carries its own value_lag_4q (B-3) for avg_assets / asset growth.
_METRICS = (
    "total_equity",
    "controlling_net_income",
    "operating_cash_flow",
    "revenue",
    "cogs",
    "gross_profit",
    "operating_income",
    "net_income",
    "total_assets",
)
_BASES = ("CFS", "OFS")


def _metric_intervals_cte(vintage_view: str) -> str:
    """One row per (ticker, metric_code, fs_basis, quarter) with the value
    B-4 actually wants — TTM for flow metrics, the instant itself otherwise —
    plus the [available_from, next_available_from) interval bound.
    """
    metrics_in = "(" + ", ".join(f"'{m}'" for m in _METRICS) + ")"
    return f"""
    metric_points AS (
        SELECT
            ticker, metric_code, fs_basis, seq_key, rcept_no,
            CASE WHEN metric_kind IN ('direct_interim', 'cumulative_reported')
                 THEN ttm_value ELSE standalone_value END AS daily_value,
            CASE WHEN metric_kind IN ('direct_interim', 'cumulative_reported')
                 THEN ttm_available_from ELSE available_from END AS daily_available_from,
            value_lag_4q
        FROM {vintage_view}
        WHERE metric_code IN {metrics_in}
    ),
    -- fin_v3: a filer catching up files several periods at once, so more than
    -- one value can become available on the same day — 6,024 such groups, e.g.
    -- 038530 on 2020-08-10 carrying net income for 2016 Q4 through 2020 Q1.
    -- ``daily_available_from`` alone is then not a total order and the interval
    -- boundaries (and so the value read on every later date) depended on scan
    -- order. Same-day candidates are resolved to the *latest fiscal period* —
    -- the figure describing the company's most recent state, which is what the
    -- daily feature means — and the losers dropped before LEAD runs, so the
    -- interval chain is built from one row per availability date.
    -- See 10_known_issues.md I12.
    metric_points_ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY ticker, metric_code, fs_basis, daily_available_from
                ORDER BY seq_key DESC, rcept_no DESC
            ) AS same_day_rank
        FROM metric_points
        WHERE daily_available_from IS NOT NULL AND daily_value IS NOT NULL
    ),
    metric_intervals AS (
        SELECT
            ticker, metric_code, fs_basis, daily_value, value_lag_4q,
            daily_available_from,
            LEAD(daily_available_from) OVER (
                PARTITION BY ticker, metric_code, fs_basis ORDER BY daily_available_from
            ) AS next_available_from
        FROM metric_points_ranked
        WHERE same_day_rank = 1
    )
    """


def _metric_join(metric_code: str, basis: str) -> str:
    alias = f"m_{metric_code}_{basis.lower()}"
    return f"""
        LEFT JOIN metric_intervals {alias}
          ON {alias}.ticker = panel.ticker
         AND {alias}.metric_code = '{metric_code}' AND {alias}.fs_basis = '{basis}'
         AND {alias}.daily_available_from <= panel.trade_date
         AND ({alias}.next_available_from IS NULL
              OR panel.trade_date < {alias}.next_available_from)"""


def build_fin_scan_daily_sql(
    *,
    pit_view: str = "dim_stock_pit_daily",
    quality_view: str = "dim_price_quality_daily",
    vintage_view: str = "fin_quarterly_metric_vintage",
) -> str:
    """SQL producing ``feat_fin_scan_daily`` from B-3's quarterly vintage mart."""
    joins = "\n".join(_metric_join(m, b) for m in _METRICS for b in _BASES)

    return f"""
    WITH {_metric_intervals_cte(vintage_view)},
    panel AS (
        SELECT
            pit.trade_date, pit.ticker, pit.market,
            pit.market_cap_pit, pit.issued_shares_pit,
            pit.shares_is_available, pit.shares_invalid_flag, pit.shares_available_from,
            q.is_halted, q.valid_session_idx
        FROM {pit_view} pit
        LEFT JOIN {quality_view} q USING (trade_date, ticker, market)
    ),
    joined AS (
        SELECT panel.*,
            {",\n            ".join(
                f"m_{m}_{b.lower()}.daily_value AS v_{m}_{b.lower()}, "
                f"m_{m}_{b.lower()}.daily_available_from AS a_{m}_{b.lower()}"
                for m in _METRICS for b in _BASES
            )},
            m_total_assets_cfs.value_lag_4q AS v_total_assets_cfs_lag4q,
            m_total_assets_ofs.value_lag_4q AS v_total_assets_ofs_lag4q
        FROM panel
        {joins}
    ),
    resolved AS (
        SELECT
            trade_date, ticker, market,
            market_cap_pit, shares_is_available, shares_invalid_flag,
            is_halted, valid_session_idx,
            CASE WHEN v_net_income_cfs IS NOT NULL THEN 'CFS'
                 WHEN v_net_income_ofs IS NOT NULL THEN 'OFS' END AS fs_basis_used,
            CASE WHEN v_net_income_cfs IS NOT NULL THEN v_total_equity_cfs
                 ELSE v_total_equity_ofs END AS total_equity_selected,
            CASE WHEN v_net_income_cfs IS NOT NULL THEN v_controlling_net_income_cfs
                 ELSE v_controlling_net_income_ofs END AS controlling_net_income_selected,
            CASE WHEN v_net_income_cfs IS NOT NULL THEN v_operating_cash_flow_cfs
                 ELSE v_operating_cash_flow_ofs END AS operating_cash_flow_selected,
            CASE WHEN v_net_income_cfs IS NOT NULL THEN v_revenue_cfs
                 ELSE v_revenue_ofs END AS revenue_selected,
            CASE WHEN v_net_income_cfs IS NOT NULL THEN v_cogs_cfs
                 ELSE v_cogs_ofs END AS cogs_selected,
            CASE WHEN v_net_income_cfs IS NOT NULL THEN v_gross_profit_cfs
                 ELSE v_gross_profit_ofs END AS gross_profit_selected,
            CASE WHEN v_net_income_cfs IS NOT NULL THEN v_operating_income_cfs
                 ELSE v_operating_income_ofs END AS operating_income_selected,
            CASE WHEN v_net_income_cfs IS NOT NULL THEN v_net_income_cfs
                 ELSE v_net_income_ofs END AS net_income_selected,
            CASE WHEN v_net_income_cfs IS NOT NULL THEN v_total_assets_cfs
                 ELSE v_total_assets_ofs END AS total_assets_selected,
            CASE WHEN v_net_income_cfs IS NOT NULL THEN v_total_assets_cfs_lag4q
                 ELSE v_total_assets_ofs_lag4q END AS total_assets_lag4q_selected,
            CASE WHEN v_net_income_cfs IS NOT NULL THEN a_net_income_cfs
                 ELSE a_net_income_ofs END AS net_income_available_from,
            CASE WHEN v_net_income_cfs IS NOT NULL
                 THEN greatest(a_total_equity_cfs, a_net_income_cfs)
                 ELSE greatest(a_total_equity_ofs, a_net_income_ofs) END
                AS value_available_from,
            CASE WHEN v_net_income_cfs IS NOT NULL THEN a_gross_profit_cfs
                 ELSE a_gross_profit_ofs END AS profitability_available_from,
            CASE WHEN v_net_income_cfs IS NOT NULL THEN a_total_assets_cfs
                 ELSE a_total_assets_ofs END AS asset_growth_available_from,
            CASE WHEN v_net_income_cfs IS NOT NULL
                 THEN greatest(a_net_income_cfs, a_operating_cash_flow_cfs, a_total_assets_cfs)
                 ELSE greatest(a_net_income_ofs, a_operating_cash_flow_ofs, a_total_assets_ofs) END
                AS accruals_available_from
        FROM joined
    ),
    scored AS (
        SELECT
            *,
            (market_cap_pit IS NOT NULL AND market_cap_pit > 0
             AND shares_is_available AND NOT shares_invalid_flag
             AND NOT COALESCE(is_halted, TRUE) AND valid_session_idx IS NOT NULL
            ) AS base_ok,
            (total_equity_selected IS NOT NULL AND total_equity_selected <= 0) AS negative_equity,
            CASE WHEN total_assets_selected > 0 AND total_assets_lag4q_selected > 0
                 THEN (total_assets_selected + total_assets_lag4q_selected) / 2 END AS avg_assets,
            CASE WHEN gross_profit_selected IS NOT NULL THEN gross_profit_selected
                 WHEN revenue_selected IS NOT NULL AND cogs_selected IS NOT NULL
                      THEN revenue_selected - cogs_selected
            END AS gross_profit_effective,
            CASE WHEN gross_profit_selected IS NOT NULL THEN 'direct'
                 WHEN revenue_selected IS NOT NULL AND cogs_selected IS NOT NULL
                      THEN 'revenue_minus_cogs_fallback'
            END AS gross_profit_source
        FROM resolved
    ),
    ratios AS (
        SELECT
            trade_date, ticker, market, fs_basis_used, negative_equity,
            gross_profit_source, value_available_from, profitability_available_from,
            asset_growth_available_from, accruals_available_from,
            CASE WHEN base_ok THEN ln(market_cap_pit) END AS fin_log_mcap,
            CASE WHEN base_ok AND total_equity_selected > 0
                 THEN total_equity_selected / market_cap_pit END AS fin_book_to_market,
            CASE WHEN base_ok THEN controlling_net_income_selected / market_cap_pit
            END AS fin_earnings_yield,
            CASE WHEN base_ok THEN operating_cash_flow_selected / market_cap_pit
            END AS fin_cfo_yield,
            CASE WHEN base_ok THEN revenue_selected / market_cap_pit
            END AS fin_sales_to_price,
            CASE WHEN avg_assets > 0 THEN gross_profit_effective / avg_assets END
                AS fin_gross_profitability,
            CASE WHEN avg_assets > 0 THEN operating_income_selected / avg_assets END
                AS fin_operating_profitability,
            CASE WHEN total_assets_lag4q_selected > 0
                 THEN total_assets_selected / total_assets_lag4q_selected - 1
            END AS fin_asset_growth_yoy,
            CASE WHEN avg_assets > 0 AND net_income_selected IS NOT NULL
                      AND operating_cash_flow_selected IS NOT NULL
                 THEN (net_income_selected - operating_cash_flow_selected) / avg_assets
            END AS fin_accruals_to_assets
        FROM scored
    ),
    -- §4.1: winsorize each value component at its own (trade_date, market)
    -- 1st/99th percentile, then z-score the winsorized series.
    --
    -- Each clip is guarded by ``WHEN <ratio> IS NULL THEN NULL``: DuckDB's
    -- GREATEST/LEAST *skip* NULL arguments, so a bare
    -- ``LEAST(GREATEST(NULL, p01), p99)`` returns p01. Without the guard a
    -- company with no financials at all is silently imputed to the market's
    -- 1st percentile on every component, counted as 4 valid components, and
    -- pinned to the "most expensive" end of the cross-section (fin_v1
    -- behaviour: 29.2% of emitted values broke the >= 2 component rule — see
    -- docs/dev/20260731_raw_features/01_feature_candidate/10_known_issues.md I1).
    winsorized AS (
        SELECT
            *,
            CASE WHEN fin_book_to_market IS NULL THEN NULL ELSE
                LEAST(GREATEST(fin_book_to_market,
                    quantile_cont(fin_book_to_market, 0.01)
                        OVER (PARTITION BY trade_date, market)),
                    quantile_cont(fin_book_to_market, 0.99)
                        OVER (PARTITION BY trade_date, market))
            END AS w_bm,
            CASE WHEN fin_earnings_yield IS NULL THEN NULL ELSE
                LEAST(GREATEST(fin_earnings_yield,
                    quantile_cont(fin_earnings_yield, 0.01)
                        OVER (PARTITION BY trade_date, market)),
                    quantile_cont(fin_earnings_yield, 0.99)
                        OVER (PARTITION BY trade_date, market))
            END AS w_ep,
            CASE WHEN fin_cfo_yield IS NULL THEN NULL ELSE
                LEAST(GREATEST(fin_cfo_yield,
                    quantile_cont(fin_cfo_yield, 0.01)
                        OVER (PARTITION BY trade_date, market)),
                    quantile_cont(fin_cfo_yield, 0.99)
                        OVER (PARTITION BY trade_date, market))
            END AS w_cfop,
            CASE WHEN fin_sales_to_price IS NULL THEN NULL ELSE
                LEAST(GREATEST(fin_sales_to_price,
                    quantile_cont(fin_sales_to_price, 0.01)
                        OVER (PARTITION BY trade_date, market)),
                    quantile_cont(fin_sales_to_price, 0.99)
                        OVER (PARTITION BY trade_date, market))
            END AS w_sp
        FROM ratios
    ),
    zscored AS (
        SELECT
            *,
            (w_bm - AVG(w_bm) OVER (PARTITION BY trade_date, market))
                / NULLIF(STDDEV_SAMP(w_bm) OVER (PARTITION BY trade_date, market), 0) AS z_bm,
            (w_ep - AVG(w_ep) OVER (PARTITION BY trade_date, market))
                / NULLIF(STDDEV_SAMP(w_ep) OVER (PARTITION BY trade_date, market), 0) AS z_ep,
            (w_cfop - AVG(w_cfop) OVER (PARTITION BY trade_date, market))
                / NULLIF(STDDEV_SAMP(w_cfop) OVER (PARTITION BY trade_date, market), 0) AS z_cfop,
            (w_sp - AVG(w_sp) OVER (PARTITION BY trade_date, market))
                / NULLIF(STDDEV_SAMP(w_sp) OVER (PARTITION BY trade_date, market), 0) AS z_sp
        FROM winsorized
    ),
    value_combined AS (
        SELECT
            *,
            (CASE WHEN z_bm IS NOT NULL THEN 1 ELSE 0 END
             + CASE WHEN z_ep IS NOT NULL THEN 1 ELSE 0 END
             + CASE WHEN z_cfop IS NOT NULL THEN 1 ELSE 0 END
             + CASE WHEN z_sp IS NOT NULL THEN 1 ELSE 0 END) AS value_component_count
        FROM zscored
    ),
    final AS (
        SELECT
            *,
            CASE WHEN value_component_count >= 2 THEN
                (COALESCE(z_bm, 0) + COALESCE(z_ep, 0) + COALESCE(z_cfop, 0) + COALESCE(z_sp, 0))
                / value_component_count
            END AS fin_value_z
        FROM value_combined
    )
    SELECT
        trade_date, ticker, market,
        fin_log_mcap,
        LAG(fin_log_mcap) OVER (PARTITION BY ticker, market ORDER BY trade_date)
            AS fin_log_mcap_lag1,
        fin_book_to_market, fin_earnings_yield, fin_cfo_yield, fin_sales_to_price,
        negative_equity, value_component_count, fs_basis_used,
        fin_value_z,
        LAG(fin_value_z) OVER (PARTITION BY ticker, market ORDER BY trade_date)
            AS fin_value_z_lag1,
        fin_gross_profitability,
        LAG(fin_gross_profitability) OVER (PARTITION BY ticker, market ORDER BY trade_date)
            AS fin_gross_profitability_lag1,
        gross_profit_source,
        fin_operating_profitability,
        LAG(fin_operating_profitability) OVER (PARTITION BY ticker, market ORDER BY trade_date)
            AS fin_operating_profitability_lag1,
        fin_asset_growth_yoy,
        LAG(fin_asset_growth_yoy) OVER (PARTITION BY ticker, market ORDER BY trade_date)
            AS fin_asset_growth_yoy_lag1,
        fin_accruals_to_assets,
        LAG(fin_accruals_to_assets) OVER (PARTITION BY ticker, market ORDER BY trade_date)
            AS fin_accruals_to_assets_lag1,
        value_available_from,
        (trade_date - value_available_from) AS value_fin_age_days,
        profitability_available_from,
        (trade_date - profitability_available_from) AS profitability_fin_age_days,
        asset_growth_available_from,
        (trade_date - asset_growth_available_from) AS asset_growth_fin_age_days,
        accruals_available_from,
        (trade_date - accruals_available_from) AS accruals_fin_age_days
    FROM final
    """


def register_fin_scan_daily_view(
    con: duckdb.DuckDBPyConnection,
    *,
    view_name: str = FIN_SCAN_TABLE,
    pit_view: str = "dim_stock_pit_daily",
    quality_view: str = "dim_price_quality_daily",
    vintage_view: str = "fin_quarterly_metric_vintage",
) -> str:
    """Register a DuckDB view over the SQL above (no parquet — tests/parity)."""
    sql = build_fin_scan_daily_sql(
        pit_view=pit_view, quality_view=quality_view, vintage_view=vintage_view
    )
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {sql}")
    return view_name


def materialize_fin_scan_daily(
    con: duckdb.DuckDBPyConnection,
    config: LakeConfig,
    *,
    pit_view: str = "dim_stock_pit_daily",
    quality_view: str = "dim_price_quality_daily",
    vintage_view: str = "fin_quarterly_metric_vintage",
    force: bool = False,
) -> str:
    """Build + register ``feat_fin_scan_daily`` as a cached parquet mart.

    Requires ``pit_view``, ``quality_view`` (A0) and ``vintage_view`` (B-3)
    already registered on ``con``.
    """
    materialize(
        con,
        config,
        FIN_SCAN_TABLE,
        build_fin_scan_daily_sql(
            pit_view=pit_view, quality_view=quality_view, vintage_view=vintage_view
        ),
        force=force,
    )
    return register_mart_view(con, config, FIN_SCAN_TABLE)
