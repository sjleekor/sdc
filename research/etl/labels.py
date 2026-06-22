"""make_label — the single, parameterized label generator (00_shared §3.1).

Model count == label count, so there is ONE generator. The 20d excess-return
rank (etl_00 §2) is just the ``horizon=20, kind="excess", bench="eqw_market",
outputs=("reg","rank","cls")`` instance; 5d/60d and volatility/MDD are other
parameter sets sharing this code.

Mechanics (etl_00 §2):
  - Forward return uses a per-ticker trading-day index that EXCLUDES halt days
    (``open=high=low=0``), so ``t+H`` is exactly H real sessions later and halt/
    holiday gaps are absorbed (etl_00 §2.1; §9 checklist "거래일 인덱스 기준").
  - Benchmark (``eqw_market``): equal-weighted mean forward return within the
    same ``(trade_date, market)`` — robust, PIT-safe, no external index needed.
    ``excess = fwd - bench``; per (date, market) the mean excess is ~0.
    ``bench="index"`` is reserved for a future market-index swap (one-line, the
    prediction-target doc §1 note) and currently raises NotImplementedError.
  - Outputs (etl_00 §2.2): per-date winsorized regression (``y_reg_*``), per-date
    percentile rank in [0,1] (``y_rank_*`` — the main target), and a 3-class
    label (``y_cls_*`` ∈ {-1,0,1}) thresholded at the 0.2/0.8 ranks.
  - Auxiliary (etl_00 §2.3): realized volatility / max-drawdown of the t+1..t+H
    daily-return path, produced by :func:`build_risk_label_sql`.

All ranking/winsorizing is WITHIN ``(trade_date, market)`` so regime/market-
direction is removed and the label matches per-date cross-sectional features
(etl_00 §2.2, §4.3).

See ``etl_00`` §2, ``00_shared`` §3.1, and ``etl_03_implementation_plan.md`` §4 (P4).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import duckdb

from research.etl.config import LakeConfig
from research.etl.mart import materialize, register_mart_view

LABEL_TABLE = "label_daily"

_VALID_KINDS = ("excess", "abs")
_VALID_BENCH = ("eqw_market", "index")
_VALID_OUTPUTS = ("reg", "rank", "cls")


@dataclass(frozen=True)
class LabelSpec:
    """Parameters for one label instance (00_shared §3.1).

    ``horizons`` is the set of forward windows to emit (the first is the
    "primary"); column suffixes are ``_{h}d``. ``cls_top``/``cls_bottom`` are the
    rank thresholds for the 3-class label.
    """

    horizons: Sequence[int] = (20, 5, 60)
    kind: str = "excess"
    bench: str = "eqw_market"
    outputs: Sequence[str] = ("reg", "rank", "cls")
    winsor: tuple[float, float] = (0.005, 0.995)
    cls_top: float = 0.8
    cls_bottom: float = 0.2
    # Risk labels (y_vol/y_mdd, etl_00 §2.3) are a *separate* regression target.
    # Off by default: the MDD path is expensive and the return-rank model does
    # not use them. Set include_risk=True to emit them into label_daily.
    risk_horizons: Sequence[int] = field(default_factory=lambda: (20,))
    include_risk: bool = False

    def __post_init__(self) -> None:
        if self.kind not in _VALID_KINDS:
            raise ValueError(f"kind must be one of {_VALID_KINDS}, got {self.kind!r}")
        if self.bench not in _VALID_BENCH:
            raise ValueError(f"bench must be one of {_VALID_BENCH}, got {self.bench!r}")
        bad = set(self.outputs) - set(_VALID_OUTPUTS)
        if bad:
            raise ValueError(f"unknown outputs {bad}; valid: {_VALID_OUTPUTS}")
        if not self.horizons:
            raise ValueError("horizons must be non-empty")
        lo, hi = self.winsor
        if not (0.0 <= lo < hi <= 1.0):
            raise ValueError(f"winsor must be 0<=lo<hi<=1, got {self.winsor}")


def _forward_cte(price_view: str) -> str:
    """CTE ``px``: per-ticker non-halt trading-day index over a price view."""
    return f"""
        px AS (
            SELECT trade_date, ticker, market,
                   CAST(close AS DOUBLE) AS close_d,
                   ROW_NUMBER() OVER (PARTITION BY ticker, market ORDER BY trade_date) AS d_idx
            FROM {price_view}
            WHERE NOT (open = 0 AND high = 0 AND low = 0)   -- exclude halt-day close distortion
        )
    """


def build_label_sql(spec: LabelSpec, price_view: str = "daily_ohlcv") -> str:
    """SQL producing ``label_daily`` (return labels) from a price view.

    ``price_view`` must already be registered. Grain: (trade_date, ticker,
    market). Emits, per horizon h: ``fwd_ret_{h}d``, optionally ``bench_ret_{h}d``,
    ``raw_label_{h}d`` (the regression source), and the requested outputs
    ``y_reg_{h}d`` / ``y_rank_{h}d`` / ``y_cls_{h}d``.
    """
    if spec.bench == "index":
        raise NotImplementedError(
            "bench='index' not implemented; use 'eqw_market' (prediction_target §1). "
            "Swapping to KOSPI/KOSDAQ index returns is a future one-line change."
        )

    px = _forward_cte(price_view)
    primary = spec.horizons[0]

    # One forward-return CTE per horizon, plus an equal-weight benchmark CTE.
    fwd_ctes: list[str] = []
    bench_ctes: list[str] = []
    for h in spec.horizons:
        fwd_ctes.append(f"""
        fwd{h} AS (
            SELECT a.trade_date, a.ticker, a.market,
                   f.close_d / NULLIF(a.close_d, 0) - 1 AS fwd_ret_{h}d
            FROM px a
            JOIN px f
              ON f.ticker = a.ticker AND f.market = a.market
             AND f.d_idx = a.d_idx + {h}
        )""")
        if spec.kind == "excess":
            bench_ctes.append(f"""
        bench{h} AS (
            SELECT trade_date, market, AVG(fwd_ret_{h}d) AS bench_ret_{h}d
            FROM fwd{h} GROUP BY trade_date, market
        )""")

    # fwd{primary} is the base row set; other horizons/benchmarks LEFT JOIN on.
    all_ctes = ",\n".join([px] + fwd_ctes + bench_ctes)
    select_cols = _build_select_cols(spec)
    risk_join = ""
    if spec.include_risk:
        # risk labels (y_vol/y_mdd) joined on the full key; NULL where no path.
        risk_cols = ", ".join(f"risk.y_vol_{h}d, risk.y_mdd_{h}d" for h in spec.risk_horizons)
        select_cols += ",\n            " + risk_cols
        risk_sql = build_risk_label_sql(spec, price_view)
        risk_join = f"\n        LEFT JOIN ({risk_sql}) AS risk USING (trade_date, ticker, market)"
    return f"""
        WITH {all_ctes}
        SELECT
            {select_cols}
        FROM fwd{primary} AS base{_extra_joins(spec, primary)}{risk_join}
    """


def _extra_joins(spec: LabelSpec, primary: int) -> str:
    """JOIN clauses for non-primary horizons + benchmarks (base is fwd{primary})."""
    parts: list[str] = []
    if spec.kind == "excess":
        parts.append(f"JOIN bench{primary} AS m{primary} USING (trade_date, market)")
    for h in spec.horizons:
        if h == primary:
            continue
        parts.append(f"LEFT JOIN fwd{h} AS f{h} USING (trade_date, ticker, market)")
        if spec.kind == "excess":
            parts.append(f"LEFT JOIN bench{h} AS m{h} USING (trade_date, market)")
    if not parts:
        return ""
    return "\n        " + "\n        ".join(parts)


def _build_select_cols(spec: LabelSpec) -> str:
    """Build the final SELECT column list (raw labels + requested outputs)."""
    primary = spec.horizons[0]
    cols: list[str] = ["base.trade_date", "base.ticker", "base.market"]
    lo, hi = spec.winsor

    for h in spec.horizons:
        fref = "base" if h == primary else f"f{h}"
        cols.append(f"{fref}.fwd_ret_{h}d")
        if spec.kind == "excess":
            cols.append(f"m{h}.bench_ret_{h}d")
            raw = f"({fref}.fwd_ret_{h}d - m{h}.bench_ret_{h}d)"
        else:
            raw = f"{fref}.fwd_ret_{h}d"
        cols.append(f"{raw} AS raw_label_{h}d")

        # per-date (within trade_date, market) ranking window.
        # NOTE: PERCENT_RANK() assigns a value to rows whose ORDER BY key is NULL
        # (they sort last), so a horizon with no forward return (e.g. the last H
        # sessions, or a secondary horizon beyond the panel) would get a bogus
        # rank/class. Guard every output with ``WHEN raw IS NULL THEN NULL`` so a
        # missing forward label stays NULL across reg/rank/cls.
        win = "PARTITION BY base.trade_date, base.market"
        null_guard = f"{raw} IS NOT NULL"
        if "rank" in spec.outputs:
            cols.append(
                f"CASE WHEN {null_guard} "
                f"THEN PERCENT_RANK() OVER ({win} ORDER BY {raw}) END AS y_rank_{h}d"
            )
        if "reg" in spec.outputs:
            # per-date winsorize via quantile clip (NULL passes through cleanly)
            cols.append(
                f"LEAST(GREATEST({raw}, "
                f"QUANTILE_CONT({raw}, {lo}) OVER ({win})), "
                f"QUANTILE_CONT({raw}, {hi}) OVER ({win})) AS y_reg_{h}d"
            )
        if "cls" in spec.outputs:
            r = f"PERCENT_RANK() OVER ({win} ORDER BY {raw})"
            cols.append(
                f"CASE WHEN {null_guard} THEN "
                f"(CASE WHEN {r} >= {spec.cls_top} THEN 1 "
                f"WHEN {r} <= {spec.cls_bottom} THEN -1 ELSE 0 END) END AS y_cls_{h}d"
            )
    return ",\n            ".join(cols)


def build_risk_label_sql(spec: LabelSpec, price_view: str = "daily_ohlcv") -> str:
    """SQL for realized-volatility / max-drawdown labels (etl_00 §2.3).

    For each (t, ticker, market) and horizon h in ``risk_horizons``, over the
    t+1..t+h daily-return path:
      - ``y_vol_{h}d``: stddev of daily log returns (realized volatility)
      - ``y_mdd_{h}d``: max drawdown of the cumulative return path (<= 0)

    Implemented with forward window frames + a single self-join aggregation per
    horizon (not a per-row correlated subquery), so it scales to the full panel.
    Uses the same non-halt d_idx as the return labels.
    """
    px = _forward_cte(price_view)
    rets = """
        rets AS (
            SELECT trade_date, ticker, market, d_idx,
                   ln(close_d / NULLIF(LAG(close_d) OVER (
                       PARTITION BY ticker, market ORDER BY d_idx), 0)) AS r1
            FROM px
        )
    """
    # Forward realized vol per horizon via a FOLLOWING window frame on rets.
    vol_cols = []
    for h in spec.risk_horizons:
        vol_cols.append(
            f"STDDEV_SAMP(r1) OVER (PARTITION BY ticker, market ORDER BY d_idx "
            f"ROWS BETWEEN 1 FOLLOWING AND {h} FOLLOWING) AS y_vol_{h}d"
        )
    vol_cte = f"""
        vol AS (
            SELECT trade_date, ticker, market, d_idx,
                   {", ".join(vol_cols)}
            FROM rets
        )
    """
    # MDD per horizon: join each base row to its forward window rows, build the
    # running cum-sum / peak per (base, horizon), and take the min drawdown.
    # One pass per horizon over the (base x window) pairs — no per-row subquery.
    mdd_ctes = []
    mdd_joins = []
    for h in spec.risk_horizons:
        mdd_ctes.append(f"""
        mdd{h} AS (
            SELECT base_td AS trade_date, ticker, market,
                   MIN(cum - run_max) AS y_mdd_{h}d
            FROM (
                SELECT base_td, ticker, market, w_d_idx,
                       cum,
                       MAX(cum) OVER (PARTITION BY base_td, ticker, market
                                      ORDER BY w_d_idx) AS run_max
                FROM (
                    SELECT a.trade_date AS base_td, a.ticker, a.market, b.d_idx AS w_d_idx,
                           SUM(b.r1) OVER (PARTITION BY a.trade_date, a.ticker, a.market
                                           ORDER BY b.d_idx) AS cum
                    FROM rets a
                    JOIN rets b
                      ON b.ticker = a.ticker AND b.market = a.market
                     AND b.d_idx > a.d_idx AND b.d_idx <= a.d_idx + {h}
                ) s
            ) p
            GROUP BY base_td, ticker, market
        )""")
        mdd_joins.append(f"LEFT JOIN mdd{h} USING (trade_date, ticker, market)")

    all_ctes = ",\n".join([px, rets, vol_cte] + mdd_ctes)
    mdd_select = "".join(f", mdd{h}.y_mdd_{h}d" for h in spec.risk_horizons)
    return f"""
        WITH {all_ctes}
        SELECT vol.trade_date, vol.ticker, vol.market,
               {", ".join(f"vol.y_vol_{h}d" for h in spec.risk_horizons)}{mdd_select}
        FROM vol
        {" ".join(mdd_joins)}
    """


def materialize_label(
    con: duckdb.DuckDBPyConnection,
    config: LakeConfig,
    spec: LabelSpec | None = None,
    *,
    price_view: str = "daily_ohlcv",
    force: bool = False,
) -> str:
    """Build + register ``label_daily`` mart view. Returns the view name.

    Requires ``price_view`` registered on ``con``.
    """
    spec = spec or LabelSpec()
    materialize(con, config, LABEL_TABLE, build_label_sql(spec, price_view), force=force)
    return register_mart_view(con, config, LABEL_TABLE)
