"""§6 B-8 "결합 단면 permutation" — jointly permutes Phase A's 75 continuous
primary cells, Phase B's ready continuous primary cells, and Phase B's ready
SUE event cells under one replicate index, applies one combined BH pass per
replicate, and returns the null distribution of "how many discoveries would
this population produce by chance".

Deliberately does **not** compute the final empirical p-value here —
comparing this null distribution against the *real* combined discovery count
needs ``run_combined_ab``'s own result, and ``run_combined_ab`` is a pure
disk-reader by design (B-PR10: it never reconnects to the live lake so any
two published run directories can be combined without re-touching raw data).
This module's entry point runs inside ``run_phase_b_core`` instead (which
already holds the live connection/panel), and only the true count →
empirical p-value conversion happens later, in ``run_combined_ab``, as a
cheap pure-math step over this module's own persisted output.

Limitation (§6 B-8's own text: "이 permutation은 NW의 시계열 보정을 검증하지
않는다는 제한을 report에 명시한다"): like Phase A's own cross-sectional
permutation (``horizon_scan_permutation.run_cross_sectional_permutation``),
this breaks the feature-label link within each date×market block
(continuous) or within each event cohort (SUE) — it diagnoses join/leakage
and multiple-testing scale, not whether the Newey-West overlap correction
itself under-corrects long-horizon serial dependence.

``combined_continuous_registry`` must already carry ``scan_type``/``h_start``/
``h_end``/``feature``/``hypothesis_id``/``expected_sign`` per cell (Phase A's
own registry already does; Phase B's ``ready_continuous`` readiness rows use
``cell_type`` instead of ``scan_type`` and must be normalized by the caller
first — the same one-line mapping ``run_phase_b_continuous_scan`` already
does).
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import polars as pl

from research.analysis.horizon_scan_permutation import (
    _append_checkpoint,
    _load_checkpoint,
    _registry_target_columns,
    _scan_registry_once,
    derive_replicate_seed,
    fetch_broad_common_survivor_frame,
    permute_within_groups,
)
from research.analysis.horizon_scan_phase_b_scan import (
    _aggregate_cohort_rows,
    _pool_cohort_ranks,
    _pool_qualifying_by_date,
    build_event_cohort_frame_sql,
)
from research.analysis.horizon_scan_runner import apply_global_bh
from research.etl.metrics import choose_nw_lag


def _permute_qualifying_sue_ranks(qualifying: pl.DataFrame, *, seed: int) -> pl.DataFrame:
    """Shuffle ``sue_pctrank`` within each ``(event_formation_date, market)``
    group, leaving ``excess_pctrank`` (and every other column) fixed to its
    original row — the "frozen rank vector" permutation §6 B-8 describes,
    applied directly to the already-computed ranks rather than re-deriving
    them from a resampled/permuted raw SUE value. Equivalent to permuting the
    raw values and re-ranking (rank is invariant to a shared within-group
    permutation of both sides), just without the redundant recompute — the
    real ``qualifying`` frame (from ``_pool_cohort_ranks``) is fetched once
    and reused across every replicate.
    """
    rng = np.random.default_rng(seed)
    parts = []
    for _, grp in qualifying.group_by(["event_formation_date", "market"], maintain_order=True):
        shuffled = grp["sue_pctrank"].to_numpy()[rng.permutation(grp.height)]
        parts.append(grp.with_columns(pl.Series("sue_pctrank", shuffled)))
    return pl.concat(parts, how="vertical")


def _scan_sue_null_row(
    cell: dict[str, Any],
    qualifying: pl.DataFrame | None,
    *,
    seed: int,
    min_events_per_cohort_total: int,
) -> dict[str, Any]:
    """One SUE cell's null row for one replicate — no minimum-cohort-count
    gate (matches the issuer/filing-cycle bootstraps and the event-ordinal
    non-overlap diagnostic: a permutation replicate legitimately wants
    whatever cohort dates happen to qualify, however few)."""
    row = {
        **cell,
        "scan_type": "event_bucket",
        "status": "insufficient",
        "ic_mean": None,
        "p_nw": None,
    }
    if qualifying is None or qualifying.is_empty():
        return row
    permuted = _permute_qualifying_sue_ranks(qualifying, seed=seed)
    cohort_rows = _pool_qualifying_by_date(
        permuted, min_events_per_cohort_total=min_events_per_cohort_total
    )
    if not cohort_rows:
        return row
    lag = choose_nw_lag(scan_type="bucket", bucket_width=cell["h_end"] - cell["h_start"])
    agg = _aggregate_cohort_rows(cohort_rows, lag=lag)
    row.update(agg)
    ic_mean = agg.get("ic_mean")
    row["status"] = "valid" if ic_mean is not None and math.isfinite(ic_mean) else "insufficient"
    return row


def run_combined_cross_sectional_permutation(
    con: duckdb.DuckDBPyConnection,
    *,
    panel_view: str,
    combined_continuous_registry: list[dict[str, Any]],
    ready_sue_cells: list[dict[str, Any]],
    config_hash: str,
    sample_start: str,
    min_names: int,
    min_names_for_spread: int,
    quantile_count: int,
    min_dates_per_cell: int,
    min_events_per_market_contribution: int,
    min_events_per_cohort_total: int,
    n_replicates: int = 100,
    q_threshold: float = 0.10,
    event_view: str = "fin_sue_event",
    calendar_view: str = "daily_ohlcv",
    checkpoint_path: Path | None = None,
) -> dict[str, Any]:
    """§6 B-8: ``n_replicates`` joint permutations across
    ``combined_continuous_registry`` (Phase A's 75 + Phase B's ready
    continuous cells) and ``ready_sue_cells``, one combined BH pass per
    replicate. Returns ``{"replicate_summaries": [...],
    "null_discovery_counts": [...], "n_replicates": ...}`` — no
    ``p_empirical_count`` yet (see module docstring for why).

    Resume-safe via ``checkpoint_path`` (JSONL, one row per replicate),
    same convention as every other null experiment in this repo.
    """
    feature_cols = sorted({hyp["feature"] for hyp in combined_continuous_registry})
    target_cols = _registry_target_columns(combined_continuous_registry)
    base_frame = (
        fetch_broad_common_survivor_frame(
            con,
            panel_view=panel_view,
            extra_cols=feature_cols + target_cols,
            sample_start=sample_start,
        )
        if combined_continuous_registry
        else None
    )

    sue_qualifying_by_hid: dict[str, pl.DataFrame | None] = {}
    for cell in ready_sue_cells:
        sql = build_event_cohort_frame_sql(
            event_view=event_view,
            calendar_view=calendar_view,
            sue_col=cell["feature"],
            h_start=cell["h_start"],
            h_end=cell["h_end"],
            sample_start=sample_start,
        )
        frame = con.execute(sql).pl()
        if not frame.is_empty():
            frame = frame.filter(
                pl.col("sue_value").is_finite() & pl.col("excess_value").is_finite()
            )
        pooled = _pool_cohort_ranks(
            frame,
            min_events_per_market_contribution=min_events_per_market_contribution,
            min_events_per_cohort_total=min_events_per_cohort_total,
        )
        sue_qualifying_by_hid[cell["hypothesis_id"]] = pooled["qualifying"]

    checkpoint = _load_checkpoint(checkpoint_path)
    summaries: list[dict[str, Any]] = [checkpoint[i] for i in sorted(checkpoint)]
    view_name = "_combined_cross_sectional_permuted_panel"
    for i in range(n_replicates):
        if i in checkpoint:
            continue
        continuous_rows: list[dict[str, Any]] = []
        seed = derive_replicate_seed(
            placebo_kind="combined_cross_sectional", replicate_index=i, config_hash=config_hash
        )
        if base_frame is not None:
            permuted = permute_within_groups(
                base_frame,
                group_cols=["trade_date", "market"],
                permute_cols=feature_cols,
                seed=seed,
            )
            con.register(view_name, permuted)
            try:
                continuous_rows = _scan_registry_once(
                    con,
                    combined_continuous_registry,
                    panel_view=view_name,
                    sample_start=sample_start,
                    min_names=min_names,
                    min_names_for_spread=min_names_for_spread,
                    quantile_count=quantile_count,
                    min_dates_per_cell=min_dates_per_cell,
                )
            finally:
                con.unregister(view_name)

        sue_rows = [
            _scan_sue_null_row(
                cell,
                sue_qualifying_by_hid.get(cell["hypothesis_id"]),
                seed=derive_replicate_seed(
                    placebo_kind=f"combined_sue_rank_permutation:{cell['hypothesis_id']}",
                    replicate_index=i,
                    config_hash=config_hash,
                ),
                min_events_per_cohort_total=min_events_per_cohort_total,
            )
            for cell in ready_sue_cells
        ]

        bh_rows = apply_global_bh(continuous_rows + sue_rows, q_threshold=q_threshold)
        n_discoveries = sum(1 for r in bh_rows if r["primary_discovery"])
        summary = {"replicate": i, "seed": seed, "n_discoveries": n_discoveries}
        _append_checkpoint(checkpoint_path, summary)
        summaries.append(summary)

    summaries.sort(key=lambda s: s["replicate"])
    replicate_ids = {s["replicate"] for s in summaries}
    if len(summaries) != n_replicates or len(replicate_ids) != n_replicates:
        raise RuntimeError(
            "combined cross-sectional permutation replicate set is incomplete or "
            f"duplicated: expected {n_replicates} unique replicates, got {len(replicate_ids)}"
        )

    return {
        "replicate_summaries": summaries,
        "null_discovery_counts": [s["n_discoveries"] for s in summaries],
        "n_replicates": n_replicates,
    }
