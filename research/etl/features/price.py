"""feat_price — derived price/momentum/volatility/liquidity features (etl_00 §3.1).

Source: ``daily_ohlcv`` only (always available). Produces *pre-standardization*
columns at grain ``(trade_date, ticker, market)`` with the ``px_`` prefix. Level
prices are deliberately turned into returns/ratios (etl_00 §3.1, L3/L5); winsor/
log/z-score is the model preprocess step (P5), not here.

Windows use trading-row counts (``ROWS BETWEEN n PRECEDING``) over the per-ticker
ordering, which absorbs halts/holidays consistently with the d_idx label logic.
Halt days (``open=high=low=0``) carry a stale close, so returns spanning a halt
are slightly distorted — flagged via ``px_is_halted`` / ``px_halt_ratio_20d`` so
the model preprocess can mask them (etl_00 §3.1, §1.2).

Conventions (etl_01 §3): all price arithmetic casts BIGINT prices to DOUBLE
before division/log to avoid integer truncation and Decimal overflow.

See ``etl_00`` §3.1 and ``etl_03_implementation_plan.md`` §4 (P3).
"""

from __future__ import annotations

import duckdb

from research.etl.config import LakeConfig
from research.etl.mart import materialize, register_mart_view

PRICE_TABLE = "feat_price"


def build_price_sql(price_view: str = "daily_ohlcv") -> str:
    """SQL producing ``feat_price`` from a daily OHLCV view.

    ``price_view`` must already be registered on the connection. Log returns use
    ``ln(close_t / close_{t-n})``; momentum/vol use day-over-day log returns.
    """
    return f"""
        WITH base AS (
            SELECT
                trade_date, ticker, market,
                CAST(close  AS DOUBLE) AS close_d,
                CAST(high   AS DOUBLE) AS high_d,
                CAST(low    AS DOUBLE) AS low_d,
                CAST(volume AS DOUBLE) AS volume_d,
                (open = 0 AND high = 0 AND low = 0) AS is_halted
            FROM {price_view}
        ),
        lagged AS (
            SELECT
                base.*,
                CAST(close_d AS DOUBLE) * volume_d AS turnover,
                LAG(close_d, 1)  OVER w AS close_lag1,
                LAG(close_d, 5)  OVER w AS close_lag5,
                LAG(close_d, 20) OVER w AS close_lag20,
                LAG(close_d, 60) OVER w AS close_lag60,
                AVG(close_d) OVER (PARTITION BY ticker, market ORDER BY trade_date
                                   ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                MAX(high_d) OVER (PARTITION BY ticker, market ORDER BY trade_date
                                  ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS hi20,
                MIN(low_d) OVER (PARTITION BY ticker, market ORDER BY trade_date
                                 ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS lo20,
                MAX(close_d) OVER (PARTITION BY ticker, market ORDER BY trade_date
                                   ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS hi52w
            FROM base
            WINDOW w AS (PARTITION BY ticker, market ORDER BY trade_date)
        ),
        rets AS (
            SELECT
                lagged.*,
                ln(close_d / NULLIF(close_lag1, 0))  AS ret_1d,
                ln(close_d / NULLIF(close_lag5, 0))  AS ret_5d,
                ln(close_d / NULLIF(close_lag20, 0)) AS ret_20d,
                ln(close_d / NULLIF(close_lag60, 0)) AS ret_60d
            FROM lagged
        )
        SELECT
            trade_date, ticker, market,
            ret_1d  AS px_ret_1d,
            ret_5d  AS px_ret_5d,
            ret_20d AS px_ret_20d,
            ret_60d AS px_ret_60d,
            (ret_20d - ret_60d) AS px_mom_20_60,
            STDDEV_SAMP(ret_1d) OVER (PARTITION BY ticker, market ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS px_vol_20d,
            STDDEV_SAMP(ret_1d) OVER (PARTITION BY ticker, market ORDER BY trade_date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS px_vol_60d,
            (hi20 - lo20) / NULLIF(close_d, 0) AS px_high_low_range_20d,
            turnover AS px_turnover,
            AVG(turnover) OVER (PARTITION BY ticker, market ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS px_turnover_ma20,
            -- Amihud illiquidity: mean(|ret_1d| / turnover) over 20 rows.
            AVG(abs(ret_1d) / NULLIF(turnover, 0)) OVER (PARTITION BY ticker, market
                ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS px_amihud_20d,
            (close_d / NULLIF(ma20, 0) - 1) AS px_gap_vs_ma20,
            (close_d / NULLIF(hi52w, 0) - 1) AS px_dist_52w_high,
            is_halted AS px_is_halted,
            AVG(CASE WHEN is_halted THEN 1.0 ELSE 0.0 END) OVER (PARTITION BY ticker, market
                ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS px_halt_ratio_20d
        FROM rets
    """


def materialize_price(
    con: duckdb.DuckDBPyConnection,
    config: LakeConfig,
    *,
    price_view: str = "daily_ohlcv",
    force: bool = False,
) -> str:
    """Build + register ``feat_price`` mart view. Returns the view name.

    Requires ``price_view`` registered on ``con``.
    """
    materialize(con, config, PRICE_TABLE, build_price_sql(price_view), force=force)
    return register_mart_view(con, config, PRICE_TABLE)
