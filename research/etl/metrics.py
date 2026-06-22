"""metrics — ranking-centric evaluation for the return-rank model (etl_00 §6).

For stock selection, "did the names we ranked high actually do well?" matters
more than pointwise RMSE (etl_00 §6). The primary metrics are computed PER DATE
(cross-sectional) and then averaged across dates:

  - rank_ic            : Spearman corr(prediction, realized) within each date,
                         averaged over dates (the headline metric).
  - icir               : mean(rank_ic) / std(rank_ic) — information ratio of IC.
  - top_decile_spread  : mean realized excess of the top-decile-predicted names.
  - top_minus_bottom   : Q-top mean minus Q-bottom mean realized excess (Q5-Q1).
  - hit_ratio_top      : fraction of top-quantile names with positive realized excess.

All functions take per-row arrays/columns plus a date key for the per-date
grouping. Pure numpy/polars; sklearn is not needed here.

See ``etl_00`` §6, ``00_shared`` §3.3, and ``etl_03_implementation_plan.md`` §4 (P6).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import polars as pl


@dataclass(frozen=True)
class RankICReport:
    """Aggregated walk-forward evaluation (etl_00 §6)."""

    n_dates: int
    n_obs: int
    rank_ic_mean: float
    rank_ic_std: float
    icir: float
    rank_ic_tstat: float
    top_decile_spread: float
    top_minus_bottom: float
    hit_ratio_top: float

    def as_dict(self) -> dict:
        return {
            "n_dates": self.n_dates,
            "n_obs": self.n_obs,
            "rank_ic_mean": self.rank_ic_mean,
            "rank_ic_std": self.rank_ic_std,
            "icir": self.icir,
            "rank_ic_tstat": self.rank_ic_tstat,
            "top_decile_spread": self.top_decile_spread,
            "top_minus_bottom": self.top_minus_bottom,
            "hit_ratio_top": self.hit_ratio_top,
        }


def _spearman(pred: np.ndarray, realized: np.ndarray) -> float:
    """Spearman rank correlation = Pearson corr of ranks. NaN if degenerate."""
    if pred.size < 2:
        return float("nan")
    pr = _rankdata(pred)
    rr = _rankdata(realized)
    ps, rs = pr.std(), rr.std()
    if ps == 0 or rs == 0:
        return float("nan")
    return float(np.corrcoef(pr, rr)[0, 1])


def _rankdata(a: np.ndarray) -> np.ndarray:
    """Average-rank of ``a`` (ties share the mean rank), like scipy.rankdata."""
    order = a.argsort()
    ranks = np.empty(a.size, dtype=float)
    ranks[order] = np.arange(1, a.size + 1, dtype=float)
    # resolve ties to average rank
    _, inv, counts = np.unique(a, return_inverse=True, return_counts=True)
    if counts.max() > 1:
        sums = np.zeros(counts.size)
        np.add.at(sums, inv, ranks)
        avg = sums / counts
        ranks = avg[inv]
    return ranks


def _finite_clean(
    df: pl.DataFrame, date_col: str, pred_col: str, realized_col: str
) -> pl.DataFrame:
    """Keep only rows where pred and realized are non-null AND finite.

    ``drop_nulls`` alone leaves NaN/inf (e.g. a NaN realized label or an inf
    prediction), which would poison the rank/quantile stats; filter those too.
    """
    clean = df.select([date_col, pred_col, realized_col]).drop_nulls()
    return clean.filter(pl.col(pred_col).is_finite() & pl.col(realized_col).is_finite())


def per_date_rank_ic(
    df: pl.DataFrame,
    *,
    pred_col: str,
    realized_col: str,
    date_col: str = "trade_date",
) -> pl.DataFrame:
    """Per-date Spearman IC between prediction and realized label.

    Returns a DataFrame with columns ``[date_col, "rank_ic", "n"]`` (one row per
    date). Rows with null/NaN/inf pred or realized are dropped before correlating.
    """
    out_dates = []
    out_ic = []
    out_n = []
    clean = _finite_clean(df, date_col, pred_col, realized_col)
    for (d,), grp in clean.group_by([date_col], maintain_order=True):
        pred = grp[pred_col].to_numpy()
        realized = grp[realized_col].to_numpy()
        out_dates.append(d)
        out_ic.append(_spearman(pred, realized))
        out_n.append(grp.height)
    return pl.DataFrame({date_col: out_dates, "rank_ic": out_ic, "n": out_n})


def _quantile_stats(
    df: pl.DataFrame,
    *,
    pred_col: str,
    realized_col: str,
    date_col: str,
    n_quantiles: int = 5,
) -> tuple[float, float, float, float]:
    """Per-date quantile portfolio stats, averaged across dates.

    Returns (top_decile_spread, top_minus_bottom, hit_ratio_top, _reserved).
    Top decile uses pred rank >= 0.9; top/bottom quantiles use n_quantiles.
    """
    clean = _finite_clean(df, date_col, pred_col, realized_col)
    top_dec, top_q, bot_q, hit = [], [], [], []
    for (_d,), grp in clean.group_by([date_col], maintain_order=True):
        if grp.height < n_quantiles:
            continue
        pred = grp[pred_col].to_numpy()
        realized = grp[realized_col].to_numpy()
        rank = _rankdata(pred) / pred.size  # in (0,1]
        top_mask = rank >= (1 - 1 / n_quantiles)
        bot_mask = rank <= (1 / n_quantiles)
        dec_mask = rank >= 0.9
        if dec_mask.any():
            top_dec.append(float(realized[dec_mask].mean()))
        if top_mask.any():
            top_q.append(float(realized[top_mask].mean()))
            hit.append(float((realized[top_mask] > 0).mean()))
        if bot_mask.any():
            bot_q.append(float(realized[bot_mask].mean()))

    def _m(xs: list[float]) -> float:
        return float(np.mean(xs)) if xs else float("nan")

    tmb = _m(top_q) - _m(bot_q) if top_q and bot_q else float("nan")
    return _m(top_dec), tmb, _m(hit), float("nan")


def evaluate(
    df: pl.DataFrame,
    *,
    pred_col: str,
    realized_col: str,
    date_col: str = "trade_date",
    n_quantiles: int = 5,
) -> RankICReport:
    """Compute the full ranking report (etl_00 §6) over a predictions frame.

    ``realized_col`` is the realized label to rank against — typically the raw
    excess return (``raw_label_20d``) so the top-decile spread is in return
    units, or the rank label for a pure IC check.
    """
    ic_df = per_date_rank_ic(df, pred_col=pred_col, realized_col=realized_col, date_col=date_col)
    ics = ic_df["rank_ic"].drop_nulls().to_numpy()
    mean = float(ics.mean()) if ics.size else float("nan")
    std = float(ics.std(ddof=1)) if ics.size > 1 else float("nan")
    icir = mean / std if std and not np.isnan(std) and std != 0 else float("nan")
    tstat = mean / (std / np.sqrt(ics.size)) if std and ics.size > 1 and std != 0 else float("nan")

    top_dec, tmb, hit, _ = _quantile_stats(
        df,
        pred_col=pred_col,
        realized_col=realized_col,
        date_col=date_col,
        n_quantiles=n_quantiles,
    )

    clean = _finite_clean(df, date_col, pred_col, realized_col)
    return RankICReport(
        n_dates=int(ic_df.height),
        n_obs=int(clean.height),
        rank_ic_mean=mean,
        rank_ic_std=std,
        icir=icir,
        rank_ic_tstat=tstat,
        top_decile_spread=top_dec,
        top_minus_bottom=tmb,
        hit_ratio_top=hit,
    )
