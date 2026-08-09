"""Shared valid-session panel primitives used by features, labels and quality."""

from __future__ import annotations


def build_valid_session_sql(price_view: str = "daily_ohlcv") -> str:
    """Return one row per real ticker session with a stable valid-session index.

    Halt rows remain available in the source view for audit joins, but never
    participate in lag/lead windows. This is the only place where the session
    index and adjacent simple/log returns are defined.
    """
    return f"""
        SELECT
            trade_date, ticker, market,
            ROW_NUMBER() OVER (
                PARTITION BY ticker, market ORDER BY trade_date
            ) AS valid_session_idx,
            CAST(close AS DOUBLE) AS close_d,
            CAST(volume AS DOUBLE) AS volume_d,
            CAST(close AS DOUBLE) * CAST(volume AS DOUBLE) AS turnover,
            LAG(CAST(close AS DOUBLE)) OVER w AS prev_close_d,
            CAST(close AS DOUBLE) / NULLIF(LAG(CAST(close AS DOUBLE)) OVER w, 0) - 1
                AS simple_ret,
            LN(CAST(close AS DOUBLE) / NULLIF(LAG(CAST(close AS DOUBLE)) OVER w, 0))
                AS log_ret
        FROM {price_view}
        WHERE NOT (open = 0 AND high = 0 AND low = 0)
        WINDOW w AS (PARTITION BY ticker, market ORDER BY trade_date)
    """


def build_full_panel_sql(price_view: str = "daily_ohlcv") -> str:
    """Return all OHLCV keys with valid-session fields left joined."""
    valid = build_valid_session_sql(price_view)
    return f"""
        WITH valid AS ({valid})
        SELECT
            p.trade_date, p.ticker, p.market,
            (p.open = 0 AND p.high = 0 AND p.low = 0) AS is_halted,
            (CAST(p.volume AS DOUBLE) = 0) AS volume_zero,
            v.valid_session_idx, v.close_d, v.volume_d, v.turnover,
            v.prev_close_d, v.simple_ret, v.log_ret
        FROM {price_view} p
        LEFT JOIN valid v USING (trade_date, ticker, market)
    """
