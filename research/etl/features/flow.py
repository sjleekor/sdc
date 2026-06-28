"""feat_flow — investor-flow / short-selling features (etl_00 §3.2).

Source: ``krx_security_flow_raw`` (the heaviest table, 76M rows). Pipeline:

  1. KRX-first dedup. KRX/PYKRX carry the same natural key with zero value
     conflicts (etl_00 §3.2). ``QUALIFY ROW_NUMBER() ... ORDER BY (KRX first)``
     keeps the KRX row. Result = 55,918,702 distinct rows (etl_01 §4.2, pinned
     as a regression guard in tests). NOTE: requires the view be read with
     ``hive_partitioning=false`` so the real ``source`` column survives
     (etl_01 §4.2) — the lake reader enforces this.
  2. Wide pivot the 7 metric_codes to one row per (trade_date, ticker, market).
  3. Derive cumulative / change / z-score features (``flow_`` prefix). Net-buy
     volumes are accumulated and z-scored (not used as raw share counts).

Short-vs-volume ratios (``short_selling_volume / daily volume``, etc.) need the
daily traded ``volume``/``turnover`` which live in ``daily_ohlcv``, not here, so
they are computed at panel assembly (P5) where price and flow are joined. This
builder passes through ``flow_short_selling_volume`` / ``flow_short_selling_value``
for that step and keeps only flow-internal derivations.

Coverage asymmetry: ``short_selling_balance_quantity`` starts 2016-06-30 (others
2007-06-05), so balance-derived features are NULL before then -> the preprocess
stage adds ``*_isna`` flags (etl_00 §3.2, L1). The 3-investor net-buy sum is NOT
an identity (excludes 기타법인) so we never derive a "closes-to-zero" feature
from it (etl_00 §3.2).

See ``etl_00`` §3.2 and ``etl_03_implementation_plan.md`` §4 (P3).
"""

from __future__ import annotations

import duckdb

from research.etl.config import LakeConfig
from research.etl.mart import materialize, register_mart_view

FLOW_TABLE = "feat_flow"

# The 7 metric_codes carried by krx_security_flow_raw (verified on the lake).
METRIC_CODES: tuple[str, ...] = (
    "foreign_net_buy_volume",
    "institution_net_buy_volume",
    "individual_net_buy_volume",
    "foreign_holding_shares",
    "short_selling_volume",
    "short_selling_value",
    "short_selling_balance_quantity",
)


def build_dedup_sql(flow_view: str = "krx_security_flow_raw") -> str:
    """SQL for KRX-first dedup of the raw flow view (etl_00 §3.2).

    Emits ``(trade_date, ticker, market, metric_code, value)`` with one row per
    natural key, KRX winning over PYKRX. ``value`` cast to DOUBLE (etl_01 §3).
    """
    return f"""
        SELECT trade_date, ticker, market, metric_code,
               CAST(value AS DOUBLE) AS value
        FROM {flow_view}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY trade_date, ticker, market, metric_code
            ORDER BY CASE source WHEN 'KRX' THEN 0 ELSE 1 END
        ) = 1
    """


def _pivot_expr() -> str:
    """Conditional-aggregation pivot of the 7 metric_codes into wide columns."""
    cols = []
    for code in METRIC_CODES:
        cols.append(f"MAX(CASE WHEN metric_code = '{code}' THEN value END) AS {code}")
    return ",\n            ".join(cols)


def build_flow_sql(flow_view: str = "krx_security_flow_raw") -> str:
    """SQL producing ``feat_flow`` (dedup -> wide pivot -> derived features).

    ``flow_view`` must already be registered on the connection (hive=false).
    """
    dedup = build_dedup_sql(flow_view)
    return f"""
        WITH dedup AS (
            {dedup}
        ),
        wide AS (
            SELECT
                trade_date, ticker, market,
                {_pivot_expr()}
            FROM dedup
            GROUP BY trade_date, ticker, market
        )
        SELECT
            trade_date, ticker, market,
            -- cumulative net-buy (5d / 20d) per investor
            SUM(foreign_net_buy_volume) OVER w5  AS flow_foreign_netbuy_sum_5d,
            SUM(foreign_net_buy_volume) OVER w20 AS flow_foreign_netbuy_sum_20d,
            SUM(institution_net_buy_volume) OVER w5  AS flow_inst_netbuy_sum_5d,
            SUM(institution_net_buy_volume) OVER w20 AS flow_inst_netbuy_sum_20d,
            SUM(individual_net_buy_volume) OVER w5  AS flow_indiv_netbuy_sum_5d,
            SUM(individual_net_buy_volume) OVER w20 AS flow_indiv_netbuy_sum_20d,
            -- foreign holding change (level differences, not the level)
            foreign_holding_shares
                - LAG(foreign_holding_shares, 5)  OVER w AS flow_foreign_holding_chg_5d,
            foreign_holding_shares
                - LAG(foreign_holding_shares, 20) OVER w AS flow_foreign_holding_chg_20d,
            -- short-selling balance change (NULL before 2016-06-30; *_isna in P5)
            short_selling_balance_quantity
                - LAG(short_selling_balance_quantity, 20) OVER w
                AS flow_short_balance_chg_20d,
            -- 20d z-score of net-buy (standardized signal, not raw shares)
            (foreign_net_buy_volume - AVG(foreign_net_buy_volume) OVER w20)
                / NULLIF(STDDEV_SAMP(foreign_net_buy_volume) OVER w20, 0)
                AS flow_foreign_netbuy_z_20d,
            (institution_net_buy_volume - AVG(institution_net_buy_volume) OVER w20)
                / NULLIF(STDDEV_SAMP(institution_net_buy_volume) OVER w20, 0)
                AS flow_inst_netbuy_z_20d,
            -- short avg price (value / volume); flow-internal, no daily_ohlcv needed
            short_selling_value / NULLIF(short_selling_volume, 0) AS flow_short_avg_price,
            -- passthrough levels for P5 panel-stage ratios (vs daily_ohlcv volume)
            short_selling_volume AS flow_short_selling_volume,
            short_selling_value AS flow_short_selling_value,
            short_selling_balance_quantity AS flow_short_balance_qty
        FROM wide
        WINDOW
            w   AS (PARTITION BY ticker, market ORDER BY trade_date),
            w5  AS (PARTITION BY ticker, market ORDER BY trade_date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
            w20 AS (PARTITION BY ticker, market ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    """


def materialize_flow(
    con: duckdb.DuckDBPyConnection,
    config: LakeConfig,
    *,
    flow_view: str = "krx_security_flow_raw",
    force: bool = False,
) -> str:
    """Build + register ``feat_flow`` mart view. Returns the view name.

    Requires ``flow_view`` registered on ``con`` (hive=false — the lake reader
    enforces this so the KRX-first dedup is not neutralized, etl_01 §4.2).
    """
    materialize(con, config, FLOW_TABLE, build_flow_sql(flow_view), force=force)
    return register_mart_view(con, config, FLOW_TABLE)
