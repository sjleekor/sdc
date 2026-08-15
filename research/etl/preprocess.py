"""preprocess — missing/outlier/scale handling for the linear model (etl_00 §4).

Ridge/ElasticNet impose hard ETL requirements (etl_00 §0): no NaN (L1), scale
sensitivity (L2), fat-tail fragility (L3). This module implements the §4.2/§4.3
recipe over a Polars panel:

  1. add_isna_flags  — one ``<col>_isna`` per feature (L1).
  2. impute          — z-score/ratio/momentum -> 0 (neutral); level -> per-date
                       cross-sectional median (etl_00 §4.2).
  3. per_date_transform — per-date winsorize -> signed-log (configured cols) ->
                       per-date z-score (L2/L3, etl_00 §4.3). "Per-date" =
                       within ``trade_date`` so only the cross-section matters
                       (regime removed), matching the rank label.

Fold-awareness (etl_00 §4.3, §5): winsorize/log/z-score statistics must be fit
on the TRAIN range only and applied to valid/test. Because the standardization
is per-date (within a single day) there is little cross-fold leakage, but the
winsorize quantiles are global-per-column, so :func:`fit_winsor_bounds` is fit on
the train slice and passed to :func:`per_date_transform` for all slices.

``profile`` toggles model-family behavior (00_shared §3.3): ``"linear"`` does the
full impute+standardize; ``"tree"`` keeps NaNs (native) and skips z-score.

See ``etl_00`` §4.2, §4.3, ``00_shared`` §3.3, ``etl_03_implementation_plan.md`` §4 (P5).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

KEY_COLS = ("trade_date", "ticker", "market")

# Columns imputed to 0 (neutral) vs per-date median. Heuristic by name suffix/
# content: ratios, momentum, z-scores, returns center at 0; absolute levels
# (turnover, holding shares, balances, prices) use the per-date median.
_ZERO_IMPUTE_HINTS = ("_z_", "ret_", "_chg_", "mom_", "_ratio", "rank", "netbuy")
_MEDIAN_IMPUTE_HINTS = ("turnover", "balance", "holding", "amihud", "avg_price", "volume", "value")


@dataclass(frozen=True)
class PreprocessConfig:
    """Preprocessing knobs (etl_00 §4)."""

    profile: str = "linear"  # "linear" | "tree"
    winsor: tuple[float, float] = (0.01, 0.99)  # per-column quantile clip (etl_00 §4.3)
    signed_log_cols: tuple[str, ...] = ()  # heavy-tailed levels to signed-log
    zero_impute_extra: tuple[str, ...] = ()
    median_impute_extra: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.profile not in ("linear", "tree"):
            raise ValueError(f"profile must be 'linear' or 'tree', got {self.profile!r}")
        lo, hi = self.winsor
        if not (0.0 <= lo < hi <= 1.0):
            raise ValueError(f"winsor must be 0<=lo<hi<=1, got {self.winsor}")


def feature_columns(panel: pl.DataFrame, *, extra_exclude: tuple[str, ...] = ()) -> list[str]:
    """Numeric feature columns: everything except keys, labels, and excludes."""
    exclude = set(KEY_COLS) | set(extra_exclude)
    cols = []
    for name, dtype in zip(panel.columns, panel.dtypes):
        if name in exclude or name.startswith("y_") or name.startswith("raw_label"):
            continue
        if name.startswith("fwd_ret_") or name.startswith("bench_ret_"):
            continue
        if dtype.is_numeric() or dtype == pl.Boolean:
            cols.append(name)
    return cols


def add_isna_flags(panel: pl.DataFrame, feature_cols: list[str]) -> pl.DataFrame:
    """Append a ``<col>_isna`` Int8 flag for each feature (L1, etl_00 §4.2)."""
    flags = [panel[c].is_null().cast(pl.Int8).alias(f"{c}_isna") for c in feature_cols]
    return panel.with_columns(flags)


def _cast_bool_features(panel: pl.DataFrame, feature_cols: list[str]) -> pl.DataFrame:
    """Cast Boolean feature columns to Float64 so numeric ops (impute/zscore) work."""
    casts = [
        pl.col(c).cast(pl.Float64).alias(c)
        for c in feature_cols
        if panel.schema.get(c) == pl.Boolean
    ]
    return panel.with_columns(casts) if casts else panel


def _impute_kind(col: str, cfg: PreprocessConfig) -> str:
    """Return 'zero' or 'median' for a feature column (etl_00 §4.2)."""
    if col in cfg.zero_impute_extra:
        return "zero"
    if col in cfg.median_impute_extra:
        return "median"
    lowered = col.lower()
    if any(h in lowered for h in _MEDIAN_IMPUTE_HINTS):
        return "median"
    if any(h in lowered for h in _ZERO_IMPUTE_HINTS):
        return "zero"
    return "zero"  # default neutral


def impute(panel: pl.DataFrame, feature_cols: list[str], cfg: PreprocessConfig) -> pl.DataFrame:
    """Fill nulls: zero-impute or group-based median (etl_00 §4.2).

    Uses ``(trade_date, market)`` as the grouping for median imputation to
    better capture sector/market-specific neutral levels.
    """
    zero_cols = [c for c in feature_cols if _impute_kind(c, cfg) == "zero"]
    median_cols = [c for c in feature_cols if _impute_kind(c, cfg) == "median"]

    exprs = []
    for c in zero_cols:
        exprs.append(pl.col(c).fill_null(0.0).alias(c))

    # Identify grouping columns. 'market' is used if present in the panel.
    group_cols = ["trade_date"]
    if "market" in panel.columns:
        group_cols.append("market")

    for c in median_cols:
        # per-group cross-sectional median, then any residual null -> 0.
        med = pl.col(c).median().over(group_cols)
        exprs.append(pl.col(c).fill_null(med).fill_null(0.0).alias(c))
    return panel.with_columns(exprs)


def fit_winsor_bounds(
    train_panel: pl.DataFrame, feature_cols: list[str], cfg: PreprocessConfig
) -> dict[str, tuple[float, float]]:
    """Fit per-column winsorize bounds on the TRAIN slice only (etl_00 §4.3, §5)."""
    lo_q, hi_q = cfg.winsor
    bounds: dict[str, tuple[float, float]] = {}
    for c in feature_cols:
        col = train_panel[c]
        if not col.dtype.is_numeric():
            continue
        lo = col.quantile(lo_q)
        hi = col.quantile(hi_q)
        if lo is not None and hi is not None:
            bounds[c] = (float(lo), float(hi))
    return bounds


def _signed_log(expr: pl.Expr) -> pl.Expr:
    """signed-log: sign(x) * ln(1+|x|) — tames heavy tails, keeps sign (etl_00 §4.3)."""
    return expr.sign() * (expr.abs() + 1.0).log()


def per_date_transform(
    panel: pl.DataFrame,
    feature_cols: list[str],
    cfg: PreprocessConfig,
    bounds: dict[str, tuple[float, float]],
) -> pl.DataFrame:
    """Winsorize (fitted bounds) -> signed-log (configured) -> per-date z-score.

    Order matters (etl_00 §4.3). For ``profile="tree"`` the z-score step is
    skipped (trees are scale-invariant) but winsorize/log still run.
    """
    # 1) winsorize using train-fitted bounds (clip both tails).
    clip_exprs = []
    for c in feature_cols:
        if c in bounds:
            lo, hi = bounds[c]
            clip_exprs.append(pl.col(c).clip(lo, hi).alias(c))
    panel = panel.with_columns(clip_exprs) if clip_exprs else panel

    # 2) signed-log for configured heavy-tailed level columns.
    if cfg.signed_log_cols:
        log_exprs = [
            _signed_log(pl.col(c)).alias(c) for c in cfg.signed_log_cols if c in feature_cols
        ]
        panel = panel.with_columns(log_exprs) if log_exprs else panel

    # 3) per-date z-score (cross-sectional standardize). Skipped for trees.
    if cfg.profile == "linear":
        z_exprs = []
        for c in feature_cols:
            mean = pl.col(c).mean().over("trade_date")
            std = pl.col(c).std().over("trade_date")
            # std==0 (or null) -> 0 so a constant cross-section becomes neutral.
            z = pl.when(std.is_null() | (std == 0)).then(0.0).otherwise((pl.col(c) - mean) / std)
            z_exprs.append(z.alias(c))
        panel = panel.with_columns(z_exprs)

    return panel


def assert_finite(panel: pl.DataFrame, feature_cols: list[str]) -> None:
    """Assert no NaN/null/inf remain in feature columns (L1 gate, etl_00 §4.2)."""
    for c in feature_cols:
        s = panel[c]
        n_null = s.null_count()
        n_nan = int(s.is_nan().sum()) if s.dtype.is_float() else 0
        n_inf = int(s.is_infinite().sum()) if s.dtype.is_float() else 0
        if n_null or n_nan or n_inf:
            raise AssertionError(
                f"feature {c!r} has non-finite values: null={n_null} nan={n_nan} inf={n_inf}"
            )


@dataclass
class FittedPreprocess:
    """Statistics fit on train, applied to any slice (fold-aware, etl_00 §4.3)."""

    feature_cols: list[str]
    cfg: PreprocessConfig
    bounds: dict[str, tuple[float, float]] = field(default_factory=dict)

    def transform(self, panel: pl.DataFrame) -> pl.DataFrame:
        """Apply isna -> cast-bool -> impute -> winsor/log/zscore to a panel slice."""
        out = add_isna_flags(panel, self.feature_cols)
        # Boolean features (e.g. px_is_halted) -> Float64 so numeric ops apply.
        out = _cast_bool_features(out, self.feature_cols)
        if self.cfg.profile == "linear":
            out = impute(out, self.feature_cols, self.cfg)
        out = per_date_transform(out, self.feature_cols, self.cfg, self.bounds)
        return out


def fit(
    train_panel: pl.DataFrame, cfg: PreprocessConfig, *, extra_exclude: tuple[str, ...] = ()
) -> FittedPreprocess:
    """Fit preprocessing on the train slice (winsor bounds). Returns a transformer.

    The cross-sectional impute median and z-score mean/std are computed per-date
    at transform time (within-day), so only the winsor bounds need fitting here.
    """
    feats = feature_columns(train_panel, extra_exclude=extra_exclude)
    bounds = fit_winsor_bounds(train_panel, feats, cfg)
    return FittedPreprocess(feature_cols=feats, cfg=cfg, bounds=bounds)
