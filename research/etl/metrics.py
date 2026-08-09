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

import math
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


def per_date_market_rank_ic(
    df: pl.DataFrame,
    *,
    pred_col: str,
    realized_col: str,
    date_col: str = "trade_date",
    market_col: str = "market",
    min_names: int = 2,
) -> pl.DataFrame:
    """Spearman IC at the preregistered ``(date, market)`` unit."""
    clean = df.select([date_col, market_col, pred_col, realized_col]).drop_nulls()
    clean = clean.filter(pl.col(pred_col).is_finite() & pl.col(realized_col).is_finite())
    out: list[tuple[object, object, float, int]] = []
    for (d, market), grp in clean.group_by([date_col, market_col], maintain_order=True):
        if grp.height < min_names:
            continue
        out.append(
            (
                d,
                market,
                _spearman(grp[pred_col].to_numpy(), grp[realized_col].to_numpy()),
                grp.height,
            )
        )
    return pl.DataFrame(
        out,
        schema={
            date_col: clean.schema[date_col],
            market_col: clean.schema[market_col],
            "rank_ic": pl.Float64,
            "n": pl.Int64,
        },
        orient="row",
    )


def daily_market_weighted_ic(
    market_ic: pl.DataFrame,
    *,
    date_col: str = "trade_date",
) -> pl.DataFrame:
    """Collapse market ICs to one n-observation-weighted IC per date."""
    if market_ic.is_empty():
        return pl.DataFrame({date_col: [], "rank_ic": [], "n": []})
    return (
        market_ic.with_columns((pl.col("rank_ic") * pl.col("n")).alias("weighted_ic"))
        .group_by(date_col, maintain_order=True)
        .agg(
            (pl.col("weighted_ic").sum() / pl.col("n").sum()).alias("rank_ic"),
            pl.col("n").sum().alias("n"),
        )
    )


def market_weight_means(
    market_ic: pl.DataFrame,
    *,
    date_col: str = "trade_date",
    market_col: str = "market",
) -> dict[str, float]:
    """Mean KOSPI/KOSDAQ share of ``n`` across valid daily_ic dates (§4.1).

    A date where only one market is valid gets weight 1 for that market —
    daily IC is n-weighted, so this discloses how much an "overall" result is
    actually a KOSDAQ (or KOSPI) result in disguise.
    """
    if market_ic.is_empty():
        return {"kospi_weight_mean": float("nan"), "kosdaq_weight_mean": float("nan")}
    wide = market_ic.pivot(on=market_col, index=date_col, values="n").fill_null(0.0)
    kospi = wide["KOSPI"].to_numpy() if "KOSPI" in wide.columns else np.zeros(wide.height)
    kosdaq = wide["KOSDAQ"].to_numpy() if "KOSDAQ" in wide.columns else np.zeros(wide.height)
    total = kospi + kosdaq
    valid = total > 0
    if not valid.any():
        return {"kospi_weight_mean": float("nan"), "kosdaq_weight_mean": float("nan")}
    kospi_weight = kospi[valid] / total[valid]
    return {
        "kospi_weight_mean": float(kospi_weight.mean()),
        "kosdaq_weight_mean": float(1.0 - kospi_weight.mean()),
    }


def per_date_market_quantile_spread(
    df: pl.DataFrame,
    *,
    feature_col: str,
    raw_label_col: str,
    date_col: str = "trade_date",
    market_col: str = "market",
    n_quantiles: int = 5,
    min_names: int = 50,
) -> pl.DataFrame:
    """Per ``(date, market)`` equal-weighted Q-top minus Q-bottom raw spread.

    §4.3 steps 1-2: rank the *raw* feature (average-rank ties, matching
    :func:`per_date_market_rank_ic`'s Spearman convention) within each
    date×market cross-section, then difference the top/bottom quantile's
    equal-weighted raw excess return. A cross-section under ``min_names`` is
    dropped entirely (not zero-filled).
    """
    columns = (
        [date_col, market_col, feature_col]
        if feature_col == raw_label_col
        else [date_col, market_col, feature_col, raw_label_col]
    )
    clean = df.select(columns).drop_nulls()
    clean = clean.filter(pl.col(feature_col).is_finite() & pl.col(raw_label_col).is_finite())
    rows: list[tuple[object, object, float, int]] = []
    for (d, m), grp in clean.group_by([date_col, market_col], maintain_order=True):
        if grp.height < min_names:
            continue
        feature = grp[feature_col].to_numpy()
        realized = grp[raw_label_col].to_numpy()
        rank = _rankdata(feature) / feature.size
        top = realized[rank >= 1 - 1 / n_quantiles]
        bottom = realized[rank <= 1 / n_quantiles]
        if top.size and bottom.size:
            rows.append((d, m, float(top.mean() - bottom.mean()), grp.height))
    return pl.DataFrame(
        rows,
        schema={
            date_col: clean.schema[date_col],
            market_col: clean.schema[market_col],
            "spread": pl.Float64,
            "n": pl.Int64,
        },
        orient="row",
    )


def daily_market_weighted_spread(
    market_spread: pl.DataFrame,
    *,
    date_col: str = "trade_date",
) -> pl.DataFrame:
    """Collapse per-market Q-top-minus-bottom spreads to one n-weighted spread
    per date (§4.3 step 3) — mirrors :func:`daily_market_weighted_ic`."""
    if market_spread.is_empty():
        return pl.DataFrame({date_col: [], "spread": [], "n": []})
    return (
        market_spread.with_columns((pl.col("spread") * pl.col("n")).alias("weighted"))
        .group_by(date_col, maintain_order=True)
        .agg(
            (pl.col("weighted").sum() / pl.col("n").sum()).alias("spread"),
            pl.col("n").sum().alias("n"),
        )
    )


def newey_west_tstat(
    values: np.ndarray | list[float],
    session_index: np.ndarray | list[int],
    lag: int,
) -> float:
    """HAC t-statistic using actual KRX session distances, not array offsets."""
    x = np.asarray(values, dtype=float)
    idx = np.asarray(session_index, dtype=int)
    mask = np.isfinite(x) & np.isfinite(idx)
    x, idx = x[mask], idx[mask]
    n = x.size
    if n < 2:
        return float("nan")
    mean = float(x.mean())
    centered = x - mean
    gamma0 = float(np.dot(centered, centered) / n)
    long_run = gamma0
    if lag > 0:
        for i in range(n):
            distances = idx[i + 1 :] - idx[i]
            valid = (distances > 0) & (distances <= lag)
            for distance, product in zip(distances[valid], centered[i] * centered[i + 1 :][valid]):
                gamma = float(product / n)
                long_run += 2.0 * (1.0 - distance / (lag + 1.0)) * gamma
    variance_mean = long_run / n
    if variance_mean <= 0 or not np.isfinite(variance_mean):
        return float("nan")
    return mean / float(np.sqrt(variance_mean))


def n_hac_pairs(session_index: np.ndarray | list[int], lag: int) -> int:
    """Count of (i, j) pairs within ``lag`` KRX sessions of each other.

    Diagnostic companion to :func:`newey_west_tstat` (§4.2: "각 lag의 pair 수를
    출력해 gap 영향을 진단한다") — a long calendar gap (regime break, delisting)
    starves some lags of pairs even when ``n_dates`` looks adequate.
    """
    idx = np.asarray(session_index, dtype=int)
    idx = idx[np.isfinite(idx)]
    idx.sort()
    if lag <= 0 or idx.size < 2:
        return 0
    total = 0
    for i in range(idx.size):
        distances = idx[i + 1 :] - idx[i]
        total += int(np.count_nonzero((distances > 0) & (distances <= lag)))
    return total


def exact_binomial_sign_test_p(n_success: int, n_trials: int) -> float:
    """One-sided exact binomial sign test: H0 p=0.5, H1 p>0.5 (§A-5).

    Used for non-overlap offset direction checks where ``n_trials`` (dates per
    offset, e.g. ~20-500) is far too small for a normal approximation to be
    trustworthy — ``math.comb`` keeps this exact via Python's arbitrary-
    precision integers rather than pulling in scipy for one test.
    """
    if n_trials <= 0:
        return float("nan")
    if not 0 <= n_success <= n_trials:
        raise ValueError(f"n_success={n_success} must be within [0, n_trials={n_trials}]")
    total = sum(math.comb(n_trials, i) for i in range(n_success, n_trials + 1))
    return total / (2**n_trials)


def two_sided_normal_p(tstat: float) -> float:
    """Two-sided asymptotic-normal p-value for an NW/HAC t-statistic (§4.2:
    ``stats.nw_p_value_distribution: asymptotic_normal``, not a t-distribution)."""
    if not math.isfinite(tstat):
        return float("nan")
    return math.erfc(abs(tstat) / math.sqrt(2.0))


def choose_nw_lag(
    *, scan_type: str, horizon: int | None = None, bucket_width: int | None = None
) -> int:
    """Use cumulative ``h-1`` and bucket-width ``width-1`` preregistered lags."""
    if scan_type == "cum":
        if horizon is None or horizon < 1:
            raise ValueError("cumulative scan requires a positive horizon")
        return horizon - 1
    if scan_type == "bucket":
        if bucket_width is None or bucket_width < 1:
            raise ValueError("bucket scan requires a positive bucket width")
        return bucket_width - 1
    raise ValueError(f"unknown scan type {scan_type!r}")


def benjamini_hochberg(pvalues: list[float] | np.ndarray) -> np.ndarray:
    """Return monotone BH q-values in the original hypothesis order.

    Tied p-values break by their position in ``pvalues`` (``kind="stable"``)
    — callers that must break ties by a specific key (§2.3 rule 5: hypothesis
    id) pre-sort their rows by that key before calling this.
    """
    p = np.asarray(pvalues, dtype=float)
    q = np.full(p.shape, np.nan, dtype=float)
    finite = np.isfinite(p)
    if not finite.any():
        return q
    values = p[finite]
    order = np.argsort(values, kind="stable")
    ranked = values[order]
    adjusted = ranked * len(ranked) / np.arange(1, len(ranked) + 1)
    adjusted = np.minimum.accumulate(adjusted[::-1])[::-1]
    restored = np.empty_like(adjusted)
    restored[order] = np.clip(adjusted, 0.0, 1.0)
    q[finite] = restored
    return q


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
    columns = (
        [date_col, pred_col] if pred_col == realized_col else [date_col, pred_col, realized_col]
    )
    clean = df.select(columns).drop_nulls()
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


def per_date_quantile_spread(
    df: pl.DataFrame,
    *,
    score_col: str,
    realized_col: str,
    date_col: str = "trade_date",
    n_quantiles: int = 5,
    min_names: int = 20,
) -> pl.DataFrame:
    """Return raw-return top-minus-bottom spreads for each date.

    The score may be a model prediction or a precomputed rank.  Invalid rows
    are removed before both the score ordering and the realized-return mean,
    keeping the portfolio population explicit and auditable.
    """
    clean = _finite_clean(df, date_col, score_col, realized_col)
    rows: list[tuple[object, float, int]] = []
    for (d,), grp in clean.group_by([date_col], maintain_order=True):
        if grp.height < max(min_names, n_quantiles):
            continue
        score = grp[score_col].to_numpy()
        realized = grp[realized_col].to_numpy()
        ranks = _rankdata(score) / score.size
        top = realized[ranks >= 1 - 1 / n_quantiles]
        bottom = realized[ranks <= 1 / n_quantiles]
        if top.size and bottom.size:
            rows.append((d, float(top.mean() - bottom.mean()), grp.height))
    return pl.DataFrame(
        rows,
        schema={date_col: clean.schema[date_col], "spread": pl.Float64, "n": pl.Int64},
        orient="row",
    )


def raw_vs_rank_quantile_spread(
    df: pl.DataFrame,
    *,
    rank_col: str,
    raw_col: str,
    date_col: str = "trade_date",
    n_quantiles: int = 5,
    min_names: int = 20,
) -> pl.DataFrame:
    """Compare quantile spreads formed from a raw score and its rank score."""
    raw = per_date_quantile_spread(
        df,
        score_col=raw_col,
        realized_col=raw_col,
        date_col=date_col,
        n_quantiles=n_quantiles,
        min_names=min_names,
    ).rename({"spread": "raw_score_spread"})
    ranked = per_date_quantile_spread(
        df,
        score_col=rank_col,
        realized_col=raw_col,
        date_col=date_col,
        n_quantiles=n_quantiles,
        min_names=min_names,
    ).rename({"spread": "rank_score_spread"})
    return raw.join(ranked, on=[date_col, "n"], how="full", coalesce=True)


@dataclass(frozen=True)
class EconomicReport:
    """Non-overlapping-rebalance economic significance (acceptance gate §6.1 ⑤).

    Daily top-decile membership is not directly comparable across successive
    days because the label horizon overlaps (each day's realized return window
    covers the next ``horizon`` sessions) — turnover computed on daily
    snapshots would double-count the same holding period many times over. This
    report instead re-derives membership only on a grid spaced ``horizon``
    sessions apart (mirroring the non-overlap bucket grid in
    ``research/etl/labels.py``), so each rebalance is a genuinely distinct
    holding period.
    """

    horizon: int
    n_rebalances: int
    grid_top_decile_spread: float
    turnover: float
    cost_bps_roundtrip: float
    cost_adjusted_spread: float

    def as_dict(self) -> dict:
        return {
            "horizon": self.horizon,
            "n_rebalances": self.n_rebalances,
            "grid_top_decile_spread": self.grid_top_decile_spread,
            "turnover": self.turnover,
            "cost_bps_roundtrip": self.cost_bps_roundtrip,
            "cost_adjusted_spread": self.cost_adjusted_spread,
        }


def rebalance_grid(dates: list, horizon: int) -> list:
    """Every ``horizon``-th date of the sorted unique ``dates`` (non-overlap grid)."""
    if horizon < 1:
        raise ValueError(f"horizon must be >= 1, got {horizon}")
    ordered = sorted(set(dates))
    return ordered[::horizon]


def decile_membership(
    df: pl.DataFrame,
    *,
    pred_col: str,
    date_col: str = "trade_date",
    ticker_col: str = "ticker",
    q: float = 0.9,
) -> dict:
    """Per-date set of tickers whose ``pred`` rank is at/above quantile ``q``.

    ``q=0.9`` is the top decile (matches ``_quantile_stats``'s ``top_decile_spread``
    convention); ``q=0.1`` with ``rank <= q`` would give the bottom decile — this
    helper always takes the upper tail, so pass ``1 - q`` and filter externally
    for a bottom-decile membership map if ever needed.
    """
    clean = df.select([date_col, ticker_col, pred_col]).drop_nulls()
    clean = clean.filter(pl.col(pred_col).is_finite())
    out: dict = {}
    for (d,), grp in clean.group_by([date_col], maintain_order=True):
        pred = grp[pred_col].to_numpy()
        if pred.size == 0:
            continue
        rank = _rankdata(pred) / pred.size
        mask = rank >= q
        if mask.any():
            out[d] = set(grp[ticker_col].to_numpy()[mask].tolist())
    return out


def portfolio_turnover(membership_by_date: dict, ordered_keys: list) -> float:
    """Mean pairwise turnover between consecutive membership snapshots.

    ``turnover = 1 - |A ∩ B| / max(|A|, |B|)`` for each consecutive pair present
    in ``membership_by_date`` — 0.0 means identical holdings, 1.0 means fully
    disjoint. Snapshots missing from ``membership_by_date`` (e.g. a rebalance
    date with too few names) are skipped rather than treated as empty.
    """
    present = [k for k in ordered_keys if k in membership_by_date]
    turnovers = []
    for prev, curr in zip(present, present[1:]):
        a, b = membership_by_date[prev], membership_by_date[curr]
        denom = max(len(a), len(b))
        if denom == 0:
            continue
        turnovers.append(1.0 - len(a & b) / denom)
    return float(np.mean(turnovers)) if turnovers else float("nan")


def economic_report(
    df: pl.DataFrame,
    *,
    pred_col: str,
    realized_col: str,
    horizon: int,
    date_col: str = "trade_date",
    ticker_col: str = "ticker",
    q: float = 0.9,
    cost_bps_roundtrip: float = 60.0,
) -> EconomicReport:
    """Grid-based top-decile spread net of an assumed round-trip transaction cost.

    ``cost_bps_roundtrip`` (default 60bp) is a business assumption, not derived
    from data — document it alongside any reported result. The realized spread
    is averaged only over the rebalance grid (not daily), so it is directly
    comparable to the turnover measured on that same grid.
    """
    clean = _finite_clean(df, date_col, pred_col, realized_col)
    grid = rebalance_grid(clean[date_col].to_list(), horizon)

    membership = decile_membership(
        df, pred_col=pred_col, date_col=date_col, ticker_col=ticker_col, q=q
    )
    turnover = portfolio_turnover(membership, grid)

    realized_means = []
    for d in grid:
        day = clean.filter(pl.col(date_col) == d)
        if day.height == 0:
            continue
        pred = day[pred_col].to_numpy()
        rank = _rankdata(pred) / pred.size
        mask = rank >= q
        if mask.any():
            realized_means.append(float(day[realized_col].to_numpy()[mask].mean()))
    grid_spread = float(np.mean(realized_means)) if realized_means else float("nan")

    cost = 0.0 if turnover != turnover else turnover * cost_bps_roundtrip / 10_000.0
    net = grid_spread - cost if grid_spread == grid_spread else float("nan")

    return EconomicReport(
        horizon=horizon,
        n_rebalances=len(grid),
        grid_top_decile_spread=grid_spread,
        turnover=turnover,
        cost_bps_roundtrip=cost_bps_roundtrip,
        cost_adjusted_spread=net,
    )


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
