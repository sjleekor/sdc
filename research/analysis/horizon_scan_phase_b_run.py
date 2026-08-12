"""Phase B real-lake orchestration + the A+B combined BH pass (04_specific_plan_B.md
§6 B-7/B-9 steps 1-2; see also ``08_phase_b_implementation_log.md`` §3 remaining
work item 3, "Phase A 75개 + Phase B ready 개를 실제로 결합해 q_fdr_global_ab
산출").

``horizon_scan_phase_b.py`` (readiness contract) and ``horizon_scan_phase_b_scan.py``
(scan + BH functions) are pure functions already covered by synthetic-row unit
tests. This module is the missing wiring against a real DuckDB lake — the
Phase B analog of ``horizon_scan.py``'s ``run_phase_a`` — plus a second entry
point that reads a *published* Phase A run directory and combines it with a
published Phase B run.

Two run kinds, two directories (§7.1), never conflated:

- ``run_phase_b_core`` writes ``phase=B/.../run_id=<id>/`` — Phase B's own
  readiness freeze + core scan + phase-B-only BH diagnostic. Does not require
  a completed Phase A run (§1.1: "Phase B core scan 자체는 Phase A 실행 완료
  전에도 계산할 수 있다").
- ``run_combined_ab`` writes ``phase=AB/.../run_id=<id>/`` — reads a published
  Phase A run and a published Phase B run (both already on disk, both
  immutable), verifies the Phase A artifact's integrity (§2.3 rule 5), and
  applies the combined BH pass.

Only the subset of the §7.1 directory contract that this item covers is
written here: ``phase_b_run_spec.json``, ``phase_b_readiness_freeze.json``,
``horizon_ic.parquet``, ``event_ic.parquet``, ``phase_b_primary_hypotheses
.parquet`` for Phase B; ``combined_ab_primary_hypotheses.parquet``,
``phase_a_card_overlay.parquet`` for the combined step. The remaining §7.1
artifacts (``readiness_matrix.*``, the ``*_quality``/``*_coverage`` diagnostics,
the B-8 robustness `*_summary.parquet` files, `primary_feature_rank_correlation
.parquet`, `family_cards.md`/`03b_horizon_scan_results.md`) belong to other
still-open remaining-work items (B-8's non-hard-gate diagnostics, B-9's
screen_pass/grade logic, B-10) and are not written by this module.

Directory nesting deliberately follows ``run_phase_a``'s actual on-disk
convention (``phase=<X>/snapshot_date=/source=/config_hash=/run_id=/``) rather
than 04_specific_plan_B.md §7.1's ASCII diagram (``snapshot_date=/source=/
config=<hash>/phase=B/``) — the plan predates `run_phase_a`'s implementation,
and matching the code that already exists on disk keeps ``phase=A``/``phase=B``/
``phase=AB`` siblings under one root instead of introducing a second
inconsistent nesting order.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import replace
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from krx_collector.infra.calendar.trading_days import get_trading_days
from research.analysis.horizon_scan_config import CONFIG_PATH, HorizonScanConfig, load_config
from research.analysis.horizon_scan_permutation import empirical_discovery_count_p
from research.analysis.horizon_scan_phase_b import (
    PHASE_B_CONTENT_HASH_EXCLUDE_NAMES,
    build_phase_b_readiness_rows,
    build_phase_b_run_spec,
    write_phase_b_readiness_freeze,
    write_phase_b_run_spec,
)
from research.analysis.horizon_scan_phase_b_cards import (
    FAMILY_SUMMARY_TABLE,
    build_family_summary_rows,
    render_family_cards_md,
)
from research.analysis.horizon_scan_phase_b_diagnostics import (
    compute_phase_b_rank_correlation,
    run_sue_event_ordinal_nonoverlap,
)
from research.analysis.horizon_scan_phase_b_joint_permutation import (
    run_combined_cross_sectional_permutation,
)
from research.analysis.horizon_scan_phase_b_robustness import (
    evaluate_sue_cluster_confirmation,
    run_filing_cycle_block_bootstrap,
    run_issuer_cluster_bootstrap,
    run_phase_b_continuous_nonoverlap,
    run_phase_b_temporal_placebo,
    select_phase_b_long_horizon_cells,
)
from research.analysis.horizon_scan_phase_b_scan import (
    PHASE_B_PANEL_VIEW,
    apply_combined_ab_bh,
    apply_phase_b_only_bh,
    assemble_phase_b_primary_table,
    assert_scan_matches_ready_population,
    compute_phase_b_evidence_grade,
    compute_phase_b_period_sign_pass,
    compute_phase_b_screen_pass,
    phase_b_primary_stats_rows,
    register_phase_b_panel,
    run_phase_b_continuous_scan,
    run_phase_b_event_scan,
)
from research.analysis.horizon_scan_readiness import build_primary_hypothesis_registry
from research.analysis.horizon_scan_run_spec import (
    REQUIRED_A0_MARTS,
    assert_a0_manifest_matches,
    compute_run_content_hash,
    kst_now_iso,
    phase_a_code_hash,
    publish_run,
)
from research.analysis.horizon_scan_runner import (
    build_period_segment_sql,
    compute_available_direction_pass,
    compute_tradable_pass,
    register_analysis_panel,
    resolve_common_formation_end,
    scan_cell,
)
from research.etl.config import REMOTE_SOURCE, LakeConfig
from research.etl.features.event_scan import (
    EVENT_FEATURE_FORMULA_VERSION,
    materialize_event_scan_daily,
)
from research.etl.features.fin_scan import FIN_SCAN_TABLE, materialize_fin_scan_daily
from research.etl.features.sue_event import SUE_EVENT_TABLE, materialize_sue_event
from research.etl.lake import connect, register_views
from research.etl.mart import mart_root, register_mart_view
from research.etl.marts.financial_quarters import (
    FQMV_TABLE,
    materialize_fin_quarterly_metric_vintage,
)
from research.etl.marts.metric_vintages import (
    SMVF_TABLE,
    materialize_stock_metric_vintage_fact,
)
from research.etl.phase_b_coverage import (
    FEATURE_COVERAGE_SPECS,
    build_event_coverage_sql,
    build_feature_coverage_sql,
    build_quarterly_metric_quality_sql,
    build_receipt_value_pairing_quality_sql,
    build_stock_metric_vintage_quality_sql,
)
from research.etl.phase_b_quality import (
    build_capital_change_quality_sql,
    build_filing_receipt_quality_sql,
)
from research.etl.snapshot import resolve_config

REPO_ROOT = Path(__file__).resolve().parents[2]


def _feature_coverage_sql_for(available_assets: set[str]) -> str:
    """``feature_coverage`` over whichever of the two daily marts exist.

    The full SQL unions both marts, so it cannot run when only one of them
    materialized. Narrowing the spec list keeps the artifact useful in that
    case instead of dropping it entirely — the missing mart's features are
    simply absent rows, which is what every other diagnostic here does too.
    """
    specs = tuple(s for s in FEATURE_COVERAGE_SPECS if s.source_mart in available_assets)
    return build_feature_coverage_sql(specs=specs)


# §7.1 the seven *_quality / *_coverage diagnostics (B-10 Stage 2), each with
# the views it reads. A diagnostic whose inputs are not in this lake is not
# written at all — an absent artifact is a visible gap, the same rule the
# diagnostics themselves follow for absent groups.
_QUALITY_DIAGNOSTICS: tuple[tuple[str, frozenset[str], Any], ...] = (
    (
        "filing_receipt_quality",
        frozenset({"dart_filing_receipt_raw"}),
        lambda _assets: build_filing_receipt_quality_sql(),
    ),
    (
        "capital_change_quality",
        frozenset({"dart_capital_change_raw", "dart_share_count_raw", "dart_corp_master"}),
        lambda _assets: build_capital_change_quality_sql(),
    ),
    (
        "receipt_value_pairing_quality",
        frozenset({SMVF_TABLE}),
        lambda _assets: build_receipt_value_pairing_quality_sql(),
    ),
    (
        "stock_metric_vintage_quality",
        frozenset({SMVF_TABLE}),
        lambda _assets: build_stock_metric_vintage_quality_sql(),
    ),
    (
        "quarterly_metric_quality",
        frozenset({FQMV_TABLE}),
        lambda _assets: build_quarterly_metric_quality_sql(),
    ),
    (
        "feature_coverage",
        frozenset({FIN_SCAN_TABLE}),  # event mart is optional, see _feature_coverage_sql_for
        _feature_coverage_sql_for,
    ),
    (
        "event_coverage",
        frozenset({SUE_EVENT_TABLE}),
        lambda _assets: build_event_coverage_sql(),
    ),
)


def write_phase_b_family_cards(
    config: HorizonScanConfig,
    core_dir: Path,
    *,
    readiness_rows: list[dict[str, Any]],
    assembled_rows: list[dict[str, Any]],
    rank_correlation_rows: list[dict[str, Any]],
    diagnostics_written: list[str],
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    """§7.1 ``family_summary.parquet`` + ``family_cards.md`` (B-10 Stage 4)."""

    def _read(name: str) -> list[dict[str, Any]]:
        if name not in diagnostics_written:
            return []
        return pl.read_parquet(core_dir / f"{name}.parquet").to_dicts()

    rows = build_family_summary_rows(
        config,
        readiness_rows=readiness_rows,
        assembled_rows=assembled_rows,
        feature_coverage_rows=_read("feature_coverage"),
        event_coverage_rows=_read("event_coverage"),
        rank_correlation_rows=rank_correlation_rows,
        # Only the issuance feature carries a fingerprinted formula version
        # today; the others get None rather than a made-up one.
        formula_versions={"ev_net_share_issuance_yoy": EVENT_FEATURE_FORMULA_VERSION},
    )
    pl.DataFrame(rows, infer_schema_length=None).write_parquet(
        core_dir / f"{FAMILY_SUMMARY_TABLE}.parquet"
    )
    (core_dir / "family_cards.md").write_text(
        render_family_cards_md(rows, run_id=run_id), encoding="utf-8"
    )
    return rows


def write_phase_b_quality_diagnostics(
    con: duckdb.DuckDBPyConnection,
    core_dir: Path,
    *,
    available_assets: set[str],
) -> list[str]:
    """Materialize the §7.1 diagnostics into ``core_dir``; return what landed.

    Written unconditionally with respect to *readiness* — unlike the scan
    artifacts, these describe the inputs, and the case where they matter most
    is precisely the one where every candidate is blocked and someone needs to
    see why. An empty result set still produces a parquet with the right
    schema; only a missing input skips the file.
    """
    written: list[str] = []
    for name, required, build_sql in _QUALITY_DIAGNOSTICS:
        if not required <= available_assets:
            continue
        try:
            frame = con.execute(build_sql(available_assets)).pl()
        except duckdb.Error:
            continue
        frame.write_parquet(core_dir / f"{name}.parquet")
        written.append(name)
    return written


def _render_readiness_matrix_md(readiness_rows: list[dict[str, Any]]) -> str:
    """§7.1 ``readiness_matrix.md`` — a flat markdown table over the 38
    candidate cells, sorted for stable diffing across runs of the same
    ``config_hash``. Purely a rendering of ``readiness_rows`` (already
    computed by ``build_phase_b_readiness_rows``); no new data."""
    columns = [
        "family",
        "feature",
        "cell_type",
        "h_start",
        "h_end",
        "role",
        "status",
        "missing_dependencies",
    ]
    rows = sorted(
        readiness_rows, key=lambda r: (r["family"], r["feature"], r["h_start"], r["h_end"])
    )
    lines = [
        "| " + " | ".join(columns) + " |",
        "|" + "|".join("---" for _ in columns) + "|",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(col, "")) for col in columns) + " |")
    return "\n".join(lines) + "\n"


def _scan_kwargs_from_config(config: HorizonScanConfig) -> dict[str, Any]:
    """Same fields as ``horizon_scan.py``'s ``scan_kwargs_from_config``,
    duplicated (not imported) to avoid a circular import — that module in
    turn imports ``run_phase_b_core``/``run_combined_ab`` from here for its
    ``--phase B``/``--phase AB`` CLI dispatch."""
    stats = config.raw["stats"]
    return {
        "sample_start": str(config.raw["sample"]["start"]),
        "min_names": int(stats["min_names_per_date_market"]),
        "min_names_for_spread": int(stats["min_names_for_spread"]),
        "quantile_count": int(stats["quantile_count"]),
        "min_dates_per_cell": int(stats["min_dates_per_cell"]),
    }


def _register_phase_b_period_segment_view(
    con: duckdb.DuckDBPyConnection,
    config: HorizonScanConfig,
    *,
    panel_view: str = PHASE_B_PANEL_VIEW,
    view_name: str = "analysis_panel_phase_b_periods",
) -> str:
    """Phase B analog of ``horizon_scan.py``'s ``register_period_segment_view``
    (duplicated, not imported, for the same circular-import reason as
    ``_scan_kwargs_from_config``) — same generic
    ``build_period_segment_sql``/``resolve_common_formation_end`` primitives
    from ``horizon_scan_runner.py``, pointed at ``analysis_panel_phase_b``
    instead of Phase A's ``analysis_panel``. Only called when that view is
    already registered (i.e. at least one continuous cell was scanned)."""
    common_formation_end = resolve_common_formation_end(con, panel_view)
    period_sql = build_period_segment_sql(
        config.raw["sample"]["period_sets"]["common"],
        {"common_formation_end": common_formation_end},
    )
    con.execute(
        f"CREATE OR REPLACE VIEW {view_name} AS "
        f"SELECT *, ({period_sql}) AS period_id_common FROM {panel_view}"
    )
    return view_name


def _compute_phase_b_period_ics(
    con: duckdb.DuckDBPyConnection,
    *,
    panel_view: str,
    cell: dict[str, Any],
    period_ids: list[str],
    sample_start: str,
    min_names: int,
    min_names_for_spread: int,
    quantile_count: int,
    min_dates_per_cell: int,
) -> list[float | None]:
    """One IC per preregistered common period, at the discovery coordinate —
    Phase B analog of ``horizon_scan.py``'s ``compute_period_ics``."""
    ics: list[float | None] = []
    for period_id in period_ids:
        result = scan_cell(
            con,
            panel_view=panel_view,
            feature_col=cell["feature"],
            scan_type="cum" if cell["cell_type"] == "cumulative" else "bucket",
            h_start=cell["h_start"],
            h_end=cell["h_end"],
            universe="broad",
            sample_kind="common_survivor",
            sample_start=sample_start,
            min_names=min_names,
            min_names_for_spread=min_names_for_spread,
            quantile_count=quantile_count,
            min_dates_per_cell=min_dates_per_cell,
            expected_sign=cell.get("expected_sign"),
            extra_where=f"period_id_common = '{period_id}'",
            compute_spread=False,
        )
        ics.append(result["ic_mean"] if result["status"] == "valid" else None)
    return ics


# --- mart materialization: best-effort, per-mart (§B-0 outcome-blind availability) ---


def register_phase_b_marts(
    con: duckdb.DuckDBPyConnection, lake: LakeConfig, *, force: bool = False
) -> set[str]:
    """Materialize the 5 Phase B marts in dependency order, best-effort.

    ``stock_metric_vintage_fact`` is the sole root every other mart depends on
    directly or transitively; ``feat_event_scan_daily`` also needs
    ``dart_capital_change_raw`` directly. A ``duckdb.Error`` here means a
    referenced raw/mart view is genuinely absent from this lake (e.g.
    ``dart_filing_receipt_raw``/``dart_capital_change_raw`` not collected yet)
    — that mart, and anything depending on it, is simply left out of the
    returned set rather than crashing the run. ``build_phase_b_readiness_rows``
    is what turns "not in this set" into the correct ``blocked_exploratory``
    role for the specific candidate cells that need it (never the whole run).
    """
    available: set[str] = set()

    def _try(name: str, build) -> None:
        try:
            build()
        except duckdb.Error:
            return
        available.add(name)

    bounds = con.execute("SELECT min(trade_date), max(trade_date) FROM daily_ohlcv").fetchone()
    trading_days = list(get_trading_days(bounds[0], bounds[1])) if bounds[0] is not None else []

    _try(
        "stock_metric_vintage_fact",
        lambda: materialize_stock_metric_vintage_fact(
            con, lake, trading_days=trading_days, force=force
        ),
    )
    if "stock_metric_vintage_fact" in available:
        _try(
            "fin_quarterly_metric_vintage",
            lambda: materialize_fin_quarterly_metric_vintage(con, lake, force=force),
        )
    if "fin_quarterly_metric_vintage" in available:
        _try(
            "feat_fin_scan_daily",
            lambda: materialize_fin_scan_daily(con, lake, force=force),
        )
        _try(
            "feat_event_scan_daily",
            lambda: materialize_event_scan_daily(con, lake, trading_days=trading_days, force=force),
        )
        _try(
            "fin_sue_event",
            lambda: materialize_sue_event(con, lake, force=force),
        )
    return available


# --- Phase B core run: readiness freeze + continuous/event scan (§6 B-7) ---


def _build_a0_run_spec_fields(
    config: HorizonScanConfig, manifest: dict[str, Any]
) -> dict[str, Any]:
    """The subset of Phase A's own ``run_spec`` fields that
    ``build_phase_b_run_spec`` needs, derived directly from ``(config,
    manifest)`` the same way Phase A's ``build_run_spec`` does — so Phase B
    never has to wait for an actual completed Phase A statistical run (§1.1).
    """
    return {
        "snapshot_date": manifest.get("snapshot_date"),
        "source": manifest.get("source"),
        "raw_manifest_hash": manifest.get("raw_marker"),
        "a0_manifest_hash": manifest.get("config_hash"),
        "label_policy_version": {
            "holdout_start": config.raw["sample"]["holdout_start"],
            "holdout_boundary": config.raw["sample"]["holdout_boundary"],
        },
        "quality_policy_version": config.raw["quality"],
        "universe_policy_version": config.raw["universe"],
    }


def compute_phase_b_gate_updates(
    con: duckdb.DuckDBPyConnection,
    config: HorizonScanConfig,
    *,
    ready_continuous: list[dict[str, Any]],
    ready_events: list[dict[str, Any]],
    continuous_scanned_rows: list[dict[str, Any]],
    event_scanned_rows: list[dict[str, Any]],
    scan_kwargs: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    """§9 B-9 screen_pass gate inputs (rules 3, 4, 5, 7, 8), one entry per
    ready cell's ``hypothesis_id``. Split out of ``run_phase_b_core`` so it is
    testable against a small synthetic ``analysis_panel_phase_b`` (plus
    ``fin_sue_event``) directly, without needing a real disk-backed
    ``LakeConfig`` — ``con`` must already have the Phase B panel/event views
    registered and ``continuous_scanned_rows``/``event_scanned_rows`` already
    scanned (exactly ``run_phase_b_core``'s own state by this point).

    Every ready cell gets a ``robustness_required``/``robustness_pass`` pair
    (defaulting to ``False``/``None``) even when neither rule 7 nor rule 8
    applies to it, so a caller merging this dict never has to special-case a
    missing key.

    Returns ``(gate_updates, diagnostics)`` — ``diagnostics`` is
    ``{"nonoverlap_rows": [...], "temporal_placebo_rows": [...],
    "issuer_bootstrap_rows": [...], "filing_cycle_bootstrap_rows": [...]}``,
    the full per-cell rows the robustness calls below already compute (§6
    B-10 Stage 1's ``*_summary.parquet`` artifacts — ``gate_updates`` only
    keeps the couple of fields each rule needs, the rest was previously
    discarded).
    """
    phase_b_cfg = config.raw["phase_b"]
    gate_updates: dict[str, dict[str, Any]] = {
        cell["hypothesis_id"]: {"robustness_required": False, "robustness_pass": None}
        for cell in ready_continuous + ready_events
    }
    nonoverlap_rows: list[dict[str, Any]] = []
    temporal_placebo_rows: list[dict[str, Any]] = []
    issuer_bootstrap_rows: list[dict[str, Any]] = []
    filing_cycle_bootstrap_rows: list[dict[str, Any]] = []

    if continuous_scanned_rows:
        combos_by_hid: dict[str, dict[tuple[str, str], dict[str, Any]]] = {}
        for row in continuous_scanned_rows:
            combos_by_hid.setdefault(row["hypothesis_id"], {})[
                (row["universe"], row["sample_kind"])
            ] = row
        tradable_min_retention = float(config.raw["decision"]["tradable_min_abs_ic_retention"])
        for cell in ready_continuous:
            hid = cell["hypothesis_id"]
            combos = combos_by_hid.get(hid, {})
            broad_cs = combos.get(("broad", "common_survivor"))
            tradable_cs = combos.get(("tradable", "common_survivor"))
            broad_av = combos.get(("broad", "available"))
            tradable_gate = compute_tradable_pass(
                ic_broad=broad_cs["ic_mean"] if broad_cs else None,
                ic_tradable=tradable_cs["ic_mean"] if tradable_cs else None,
                min_retention=tradable_min_retention,
            )
            available_gate = compute_available_direction_pass(
                ic_common_survivor=broad_cs["ic_mean"] if broad_cs else None,
                ic_available=broad_av["ic_mean"] if broad_av else None,
            )
            gate_updates[hid].update(
                {
                    "tradable_retention": tradable_gate["tradable_retention"],
                    "tradable_pass": tradable_gate["tradable_pass"],
                    "available_direction_pass": available_gate["available_direction_pass"],
                }
            )

        period_view = _register_phase_b_period_segment_view(con, config)
        period_ids = [p["id"] for p in config.raw["sample"]["period_sets"]["common"]]
        for cell in ready_continuous:
            hid = cell["hypothesis_id"]
            period_ics = _compute_phase_b_period_ics(
                con, panel_view=period_view, cell=cell, period_ids=period_ids, **scan_kwargs
            )
            period_sign_pass = compute_phase_b_period_sign_pass(
                period_ics, expected_sign=cell.get("expected_sign")
            )
            gate_updates[hid].update(period_sign_pass)

        long_cells = select_phase_b_long_horizon_cells(
            ready_continuous, min_nw_lag=int(config.raw["placebo"]["temporal_min_nw_lag"])
        )
        if long_cells:
            nonoverlap_result = run_phase_b_continuous_nonoverlap(
                con,
                long_cells,
                panel_view=PHASE_B_PANEL_VIEW,
                sample_start=scan_kwargs["sample_start"],
                min_names=scan_kwargs["min_names"],
                nonoverlap_min_dates_overrides=dict(phase_b_cfg["nonoverlap_min_dates"]),
                valid_offset_ratio_min=float(phase_b_cfg["nonoverlap_valid_offset_ratio_min"]),
                expected_sign_ratio_min=float(
                    phase_b_cfg["nonoverlap_expected_sign_offset_ratio_min"]
                ),
            )
            nonoverlap_rows.extend(nonoverlap_result)
            nonoverlap_by_id = {r["hypothesis_id"]: r for r in nonoverlap_result}
            real_t_nw_by_id = {
                cell["hypothesis_id"]: combos_by_hid.get(cell["hypothesis_id"], {})
                .get(("broad", "common_survivor"), {})
                .get("t_nw")
                for cell in long_cells
            }
            placebo_result = run_phase_b_temporal_placebo(
                con,
                panel_view=PHASE_B_PANEL_VIEW,
                long_horizon_cells=long_cells,
                real_t_nw_by_id=real_t_nw_by_id,
                config_hash=config.config_hash,
                n_replicates=int(config.raw["placebo"]["temporal_long_cell_repeats"]),
                min_shift_sessions=int(config.raw["placebo"]["temporal_min_shift_sessions"]),
                p_max=float(config.raw["placebo"]["temporal_p_max"]),
                **scan_kwargs,
            )
            for cell in long_cells:
                hid = cell["hypothesis_id"]
                nonoverlap_pass = bool(
                    nonoverlap_by_id.get(hid, {}).get("nonoverlap_robustness_pass")
                )
                temporal = placebo_result["per_cell"].get(hid, {})
                temporal_pass = bool(temporal.get("temporal_null_pass"))
                gate_updates[hid].update(
                    {
                        "robustness_required": True,
                        "robustness_pass": bool(nonoverlap_pass and temporal_pass),
                        "nonoverlap_robustness_pass": nonoverlap_pass,
                        "offset_status": nonoverlap_by_id.get(hid, {}).get("offset_status"),
                        "temporal_null_pass": temporal_pass,
                        "p_temporal_nw": temporal.get("p_temporal_nw"),
                    }
                )
                temporal_placebo_rows.append(
                    {
                        "hypothesis_id": hid,
                        "n_replicates": placebo_result["n_replicates"],
                        "total_sessions": placebo_result["total_sessions"],
                        **temporal,
                    }
                )

    if event_scanned_rows:
        for cell in ready_events:
            hid = cell["hypothesis_id"]
            common_bootstrap_kwargs = dict(
                hypothesis_id=hid,
                config_hash=config.config_hash,
                h_start=cell["h_start"],
                h_end=cell["h_end"],
                sample_start=scan_kwargs["sample_start"],
                min_events_per_market_contribution=int(
                    phase_b_cfg["min_events_per_market_contribution"]
                ),
                min_events_per_cohort_total=int(phase_b_cfg["min_events_per_cohort_total"]),
                expected_sign=cell.get("expected_sign"),
                p_max=float(phase_b_cfg["event_cluster_confirm_p_max"]),
            )
            issuer_result = run_issuer_cluster_bootstrap(
                con,
                n_replicates=int(phase_b_cfg["event_issuer_bootstrap_repeats"]),
                **common_bootstrap_kwargs,
            )
            filing_cycle_result = run_filing_cycle_block_bootstrap(
                con,
                n_replicates=int(phase_b_cfg["event_filing_cycle_bootstrap_repeats"]),
                **common_bootstrap_kwargs,
            )
            confirmation = evaluate_sue_cluster_confirmation(issuer_result, filing_cycle_result)
            gate_updates[hid].update(
                {
                    "robustness_required": True,
                    "robustness_pass": confirmation["sue_cluster_confirm_pass"],
                    "issuer_bootstrap_p": confirmation["issuer_bootstrap_p"],
                    "filing_cycle_bootstrap_p": confirmation["filing_cycle_bootstrap_p"],
                }
            )
            issuer_bootstrap_rows.append(
                {
                    "hypothesis_id": hid,
                    **{k: v for k, v in issuer_result.items() if k != "replicate_ic_means"},
                }
            )
            filing_cycle_bootstrap_rows.append(
                {
                    "hypothesis_id": hid,
                    **{k: v for k, v in filing_cycle_result.items() if k != "replicate_ic_means"},
                }
            )

        # §6 B-8 SUE point 5 — non-blocking diagnostic (never sets
        # robustness_required/robustness_pass; those two remain rules 7/8's
        # cluster-confirmation gate above). Reported for the card and to
        # corroborate n_independent_filing_windows's own grade-A cap.
        ordinal_rows = {
            r["hypothesis_id"]: r
            for r in run_sue_event_ordinal_nonoverlap(
                con,
                ready_events,
                sample_start=scan_kwargs["sample_start"],
                min_events_per_market_contribution=int(
                    phase_b_cfg["min_events_per_market_contribution"]
                ),
                min_events_per_cohort_total=int(phase_b_cfg["min_events_per_cohort_total"]),
                min_event_cohorts=int(phase_b_cfg["min_event_cohorts"]),
                ordinal_stride=int(phase_b_cfg["event_ordinal_nonoverlap_stride"]),
                valid_offset_ratio_min=float(phase_b_cfg["nonoverlap_valid_offset_ratio_min"]),
                expected_sign_ratio_min=float(
                    phase_b_cfg["nonoverlap_expected_sign_offset_ratio_min"]
                ),
            )
        }
        for cell in ready_events:
            hid = cell["hypothesis_id"]
            ordinal = ordinal_rows.get(hid, {})
            gate_updates[hid].update(
                {
                    "event_ordinal_nonoverlap_pass": ordinal.get("nonoverlap_robustness_pass"),
                    "event_ordinal_offset_status": ordinal.get("offset_status"),
                }
            )

    return gate_updates, {
        "nonoverlap_rows": nonoverlap_rows,
        "temporal_placebo_rows": temporal_placebo_rows,
        "issuer_bootstrap_rows": issuer_bootstrap_rows,
        "filing_cycle_bootstrap_rows": filing_cycle_bootstrap_rows,
    }


def run_phase_b_core(
    *,
    snapshot_date: str | None = None,
    source: str = REMOTE_SOURCE,
    data_lake_root: Path | None = None,
    output_root: Path,
    command_line: list[str],
) -> Path:
    config = load_config(CONFIG_PATH)
    base = LakeConfig(source=source, data_lake_root=data_lake_root or LakeConfig().data_lake_root)
    required_raw = list(config.raw["phase_b"]["required_raw_tables"])
    lake, resolution = resolve_config(
        base, required_inputs=required_raw, snapshot_date=snapshot_date
    )
    lake = replace(lake, analysis_config_hash=config.config_hash)

    manifest_path = mart_root(lake) / "_manifests" / "_SUCCESS.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"A0 manifest is required before a Phase B scan: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert_a0_manifest_matches(config, manifest)

    started_at = kst_now_iso()
    a0_run_spec = _build_a0_run_spec_fields(config, manifest)

    con = connect(lake)
    raw_present = set(register_views(con, lake))
    for name in REQUIRED_A0_MARTS:
        register_mart_view(con, lake, name)
    register_analysis_panel(con)
    phase_b_marts = register_phase_b_marts(con, lake)
    available_assets = raw_present | set(REQUIRED_A0_MARTS) | phase_b_marts

    readiness_rows = build_phase_b_readiness_rows(config, available_assets=available_assets)

    run_spec = build_phase_b_run_spec(
        config,
        a0_run_spec=a0_run_spec,
        raw_manifest_tables=raw_present,
        repo_root=REPO_ROOT,
        code_paths=sorted(Path(__file__).parent.glob("horizon_scan_phase_b*.py")),
        command_line=command_line,
        started_at=started_at,
    )

    run_dir_root = (
        output_root
        / "phase=B"
        / f"snapshot_date={resolution.snapshot_date}"
        / f"source={resolution.source}"
        / f"config_hash={config.config_hash}"
    )
    tmp_run_dir = run_dir_root / f"run_id={run_spec['run_id']}.tmp"

    # §B-0: the freeze is written before any scan/label-join stage runs, and
    # is refused a second write for this run — outcome-blind by construction.
    write_phase_b_readiness_freeze(
        config,
        readiness_rows,
        tmp_run_dir / "phase_b_readiness_freeze.json",
        generated_at=started_at,
    )

    continuous_scanned_rows: list[dict[str, Any]] = []
    event_scanned_rows: list[dict[str, Any]] = []
    ready_continuous = [
        c
        for c in readiness_rows
        if c["role"] == "ready_primary" and c["cell_type"] in ("cumulative", "bucket")
    ]
    ready_events = [
        c
        for c in readiness_rows
        if c["role"] == "ready_primary" and c["cell_type"] == "event_bucket"
    ]
    scan_kwargs = _scan_kwargs_from_config(config)
    phase_b_cfg = config.raw["phase_b"]

    if ready_continuous and {"feat_fin_scan_daily", "feat_event_scan_daily"} & phase_b_marts:
        register_phase_b_panel(
            con,
            fin_scan_view="feat_fin_scan_daily" if "feat_fin_scan_daily" in phase_b_marts else None,
            event_scan_view=(
                "feat_event_scan_daily" if "feat_event_scan_daily" in phase_b_marts else None
            ),
        )
        continuous_scanned_rows = run_phase_b_continuous_scan(con, ready_continuous, **scan_kwargs)

    if ready_events and "fin_sue_event" in phase_b_marts:
        event_scanned_rows = run_phase_b_event_scan(
            con,
            ready_events,
            sample_start=scan_kwargs["sample_start"],
            min_events_per_market_contribution=int(
                phase_b_cfg["min_events_per_market_contribution"]
            ),
            min_events_per_cohort_total=int(phase_b_cfg["min_events_per_cohort_total"]),
            min_event_cohorts=int(phase_b_cfg["min_event_cohorts"]),
        )

    scanned_rows = continuous_scanned_rows + event_scanned_rows
    assembled = assemble_phase_b_primary_table(readiness_rows, scanned_rows)
    ready_stats_rows = phase_b_primary_stats_rows(assembled)
    assert_scan_matches_ready_population(ready_stats_rows, readiness_rows)

    q_threshold = float(config.raw["stats"]["global_bh_q"])
    ready_stats_rows = apply_phase_b_only_bh(ready_stats_rows, q_threshold=q_threshold)

    gate_updates, phase_b_diagnostics = compute_phase_b_gate_updates(
        con,
        config,
        ready_continuous=ready_continuous,
        ready_events=ready_events,
        continuous_scanned_rows=continuous_scanned_rows,
        event_scanned_rows=event_scanned_rows,
        scan_kwargs=scan_kwargs,
    )
    ready_stats_rows = [
        {**row, **gate_updates.get(row["hypothesis_id"], {})} for row in ready_stats_rows
    ]
    by_id = {r["hypothesis_id"]: r for r in ready_stats_rows}
    assembled = [by_id.get(row["hypothesis_id"], row) for row in assembled]

    write_phase_b_run_spec(tmp_run_dir, run_spec)
    (tmp_run_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8"
    )
    core_dir = tmp_run_dir / "core"
    core_dir.mkdir(parents=True, exist_ok=True)

    # §7.1 readiness_matrix.{parquet,md} — the full 38-candidate readiness
    # freeze, reformatted for direct inspection. Unlike every other artifact
    # in this function, this is written unconditionally: it's a view over
    # `readiness_rows` itself (ready *and* blocked cells), not a downstream
    # scan/diagnostic result gated on any cell being ready.
    pl.DataFrame(readiness_rows, infer_schema_length=None).write_parquet(
        core_dir / "readiness_matrix.parquet"
    )
    (core_dir / "readiness_matrix.md").write_text(
        _render_readiness_matrix_md(readiness_rows), encoding="utf-8"
    )

    # §7.1 the *_quality/*_coverage diagnostics — same unconditional treatment
    # as readiness_matrix above, and for the same reason: they describe the
    # inputs, not a scan result, so a run where everything is blocked is the
    # run that needs them most.
    diagnostics_written = write_phase_b_quality_diagnostics(
        con, core_dir, available_assets=available_assets
    )

    rank_corr_rows: list[dict[str, Any]] = []

    # §5.5/§7.1 primary_feature_rank_correlation.parquet — every ready Phase A
    # continuous family's primary feature crossed with every ready Phase B
    # continuous family's primary feature (SUE excluded, its grain isn't
    # daily). Only meaningful once at least one Phase B continuous cell was
    # actually scanned (same guard as the panel-dependent gates above).
    if continuous_scanned_rows:
        phase_a_primary_by_family = {
            f["family"]: next(feat["column"] for feat in f["features"] if feat["role"] == "primary")
            for f in config.families
            if f.get("phase") == "A" and f.get("role") == "ready"
        }
        phase_b_primary_by_family = {cell["family"]: cell["feature"] for cell in ready_continuous}
        feature_pairs = [
            (fam_a, feat_a, fam_b, feat_b)
            for fam_a, feat_a in phase_a_primary_by_family.items()
            for fam_b, feat_b in phase_b_primary_by_family.items()
        ]
        rank_corr_rows = compute_phase_b_rank_correlation(
            con,
            panel_view=PHASE_B_PANEL_VIEW,
            feature_pairs=feature_pairs,
            sample_start=scan_kwargs["sample_start"],
            min_names=scan_kwargs["min_names"],
        )
        if rank_corr_rows:
            pl.DataFrame(rank_corr_rows, infer_schema_length=None).write_parquet(
                core_dir / "primary_feature_rank_correlation.parquet"
            )

    # §6 B-8 "결합 단면 permutation" — joint A+B continuous + SUE null
    # discovery-count distribution. Only worth computing when Phase B
    # actually contributed something ready (continuous or SUE) — otherwise
    # this would just reduce to Phase A's own existing A-6a permutation. The
    # *real* discovery count isn't known yet here (that needs
    # run_combined_ab's own combined BH result); only the null distribution
    # is persisted — run_combined_ab converts it to an empirical p-value
    # later as a pure-math step over this file, without reconnecting to the
    # lake (see horizon_scan_phase_b_joint_permutation.py's module docstring).
    if continuous_scanned_rows or event_scanned_rows:
        combined_continuous_registry = build_primary_hypothesis_registry(config) + [
            {**cell, "scan_type": "cum" if cell["cell_type"] == "cumulative" else "bucket"}
            for cell in ready_continuous
        ]
        permutation_result = run_combined_cross_sectional_permutation(
            con,
            panel_view=PHASE_B_PANEL_VIEW,
            combined_continuous_registry=combined_continuous_registry,
            ready_sue_cells=ready_events,
            config_hash=config.config_hash,
            sample_start=scan_kwargs["sample_start"],
            min_names=scan_kwargs["min_names"],
            min_names_for_spread=scan_kwargs["min_names_for_spread"],
            quantile_count=scan_kwargs["quantile_count"],
            min_dates_per_cell=scan_kwargs["min_dates_per_cell"],
            min_events_per_market_contribution=int(
                phase_b_cfg["min_events_per_market_contribution"]
            ),
            min_events_per_cohort_total=int(phase_b_cfg["min_events_per_cohort_total"]),
            n_replicates=int(config.raw["placebo"]["cross_sectional_repeats"]),
            q_threshold=q_threshold,
        )
        pl.DataFrame(
            permutation_result["replicate_summaries"], infer_schema_length=None
        ).write_parquet(core_dir / "permutation_summary.parquet")

    # §7.1 robustness *_summary.parquet — the full per-cell rows
    # `compute_phase_b_gate_updates` already computed (§6 B-10 Stage 1);
    # `gate_updates` only kept the couple of fields each screen_pass rule
    # needs, these are the rest. Written only when non-empty (no ready
    # long-horizon continuous cell / ready SUE cell today → none exist).
    for name, rows in phase_b_diagnostics.items():
        if rows:
            pl.DataFrame(rows, infer_schema_length=None).write_parquet(
                core_dir / f"{name.removesuffix('_rows')}_summary.parquet"
            )

    # §7.1 family_summary.parquet / family_cards.md (B-10 Stage 4). Coverage is
    # read back off the parquet just written rather than re-queried, so a card
    # can never disagree with the artifact it summarizes.
    write_phase_b_family_cards(
        config,
        core_dir,
        readiness_rows=readiness_rows,
        assembled_rows=assembled,
        rank_correlation_rows=rank_corr_rows,
        diagnostics_written=diagnostics_written,
        run_id=run_spec["run_id"],
    )

    continuous_by_id = {
        r["hypothesis_id"]: r for r in ready_stats_rows if r.get("scan_type") in ("cum", "bucket")
    }
    continuous_output_rows = [
        (
            continuous_by_id.get(r["hypothesis_id"], r)
            if r["universe"] == "broad" and r["sample_kind"] == "common_survivor"
            else r
        )
        for r in continuous_scanned_rows
    ]
    event_stats = [r for r in ready_stats_rows if r.get("scan_type") == "event_bucket"]
    if continuous_output_rows:
        pl.DataFrame(continuous_output_rows, infer_schema_length=None).write_parquet(
            core_dir / "horizon_ic.parquet"
        )
    if event_stats:
        pl.DataFrame(event_stats, infer_schema_length=None).write_parquet(
            core_dir / "event_ic.parquet"
        )
    pl.DataFrame(assembled, infer_schema_length=None).write_parquet(
        core_dir / "phase_b_primary_hypotheses.parquet"
    )

    final_run_dir = run_dir_root / f"run_id={run_spec['run_id']}"
    return publish_run(
        tmp_run_dir,
        final_run_dir,
        run_spec=run_spec,
        required_artifacts=("phase_b_run_spec.json", "manifest.json"),
        content_hash_exclude_names=PHASE_B_CONTENT_HASH_EXCLUDE_NAMES,
    )


# --- combined A+B BH (§6 B-9 steps 1-2) ---


def load_phase_a_primary_rows(
    phase_a_run_dir: Path, config: HorizonScanConfig
) -> list[dict[str, Any]]:
    """Read a *published* Phase A run's 75 primary raw rows, verifying §2.3
    rule 5 before trusting them: same config, unmodified content, exact 75-id
    population match against the current preregistered registry.
    """
    run_spec = json.loads((phase_a_run_dir / "run_spec.json").read_text(encoding="utf-8"))
    if run_spec.get("config_hash") != config.config_hash:
        raise ValueError(
            f"Phase A run {phase_a_run_dir} was built with config_hash="
            f"{run_spec.get('config_hash')!r}, current config_hash={config.config_hash!r}"
        )
    success = json.loads((phase_a_run_dir / "_SUCCESS.json").read_text(encoding="utf-8"))
    recomputed_hash = compute_run_content_hash(phase_a_run_dir)
    if recomputed_hash != success.get("content_hash"):
        raise ValueError(
            f"Phase A run {phase_a_run_dir} content hash mismatch: recomputed="
            f"{recomputed_hash!r} != published={success.get('content_hash')!r} "
            "(artifact modified since publish)"
        )

    frame = pl.read_parquet(phase_a_run_dir / "core" / "horizon_ic.parquet")
    # Only the broad/common_survivor primary rows were ever passed through
    # ``apply_global_bh`` (§2.3's discovery coordinate) — short-exploratory
    # rows and the tradable/available combos share this same parquet but
    # never carry ``q_fdr_global``, so this filter alone isolates the 75.
    primary_rows = [
        row
        for row in frame.to_dicts()
        if row.get("universe") == "broad"
        and row.get("sample_kind") == "common_survivor"
        and row.get("q_fdr_global") is not None
    ]
    registry_ids = {c["hypothesis_id"] for c in build_primary_hypothesis_registry(config)}
    row_ids = {r["hypothesis_id"] for r in primary_rows}
    if row_ids != registry_ids:
        missing = registry_ids - row_ids
        extra = row_ids - registry_ids
        raise ValueError(
            f"Phase A run {phase_a_run_dir} primary rows do not match the current "
            f"75-hypothesis registry: missing={missing} extra={extra}"
        )
    return [
        {
            **row,
            "q_fdr_phase_a": row.get("q_fdr_global"),
            "bh_pass_phase_a": row.get("bh_pass"),
            "primary_discovery_phase_a": row.get("primary_discovery"),
        }
        for row in primary_rows
    ]


def run_combined_ab(
    *,
    phase_a_run_dir: Path,
    phase_b_run_dir: Path,
    output_root: Path,
    command_line: list[str],
) -> Path:
    config = load_config(CONFIG_PATH)
    phase_a_rows = load_phase_a_primary_rows(phase_a_run_dir, config)

    phase_a_success = json.loads((phase_a_run_dir / "_SUCCESS.json").read_text(encoding="utf-8"))
    phase_a_spec = json.loads((phase_a_run_dir / "run_spec.json").read_text(encoding="utf-8"))
    phase_b_success = json.loads((phase_b_run_dir / "_SUCCESS.json").read_text(encoding="utf-8"))
    phase_b_spec = json.loads(
        (phase_b_run_dir / "phase_b_run_spec.json").read_text(encoding="utf-8")
    )
    if phase_b_spec.get("config_hash") != config.config_hash:
        raise ValueError(
            f"Phase B run {phase_b_run_dir} was built with config_hash="
            f"{phase_b_spec.get('config_hash')!r}, current config_hash={config.config_hash!r}"
        )
    recomputed_b_hash = compute_run_content_hash(
        phase_b_run_dir, exclude_names=PHASE_B_CONTENT_HASH_EXCLUDE_NAMES
    )
    if recomputed_b_hash != phase_b_success.get("content_hash"):
        raise ValueError(
            f"Phase B run {phase_b_run_dir} content hash mismatch: recomputed="
            f"{recomputed_b_hash!r} != published={phase_b_success.get('content_hash')!r}"
        )

    assembled = pl.read_parquet(
        phase_b_run_dir / "core" / "phase_b_primary_hypotheses.parquet"
    ).to_dicts()
    phase_b_ready_rows = [row for row in assembled if row.get("role") == "ready_primary"]

    q_threshold = float(config.raw["stats"]["global_bh_q"])
    combined = apply_combined_ab_bh(phase_a_rows, phase_b_ready_rows, q_threshold=q_threshold)

    # §9 B-9 screen_pass + evidence_grade — only meaningful for Phase B ready
    # cells (Phase A's 75 already have their own family-level screen_pass on
    # the family card, a different computation this function does not
    # touch). Rules 2-9's inputs ride along on ``combined`` rows already
    # (``apply_global_bh`` copies every input field verbatim) since
    # ``run_phase_b_core`` persisted them onto
    # ``phase_b_primary_hypotheses.parquet``; only rule 1
    # (``q_fdr_global_ab``/expected sign, folded into ``primary_discovery_ab``)
    # is new at this combined step.
    phase_b_ready_ids = {r["hypothesis_id"] for r in phase_b_ready_rows}
    updated_combined: list[dict[str, Any]] = []
    for row in combined:
        if row["hypothesis_id"] not in phase_b_ready_ids:
            updated_combined.append(row)
            continue
        screen = compute_phase_b_screen_pass(
            role=row.get("role", "ready_primary"),
            primary_discovery=bool(row.get("primary_discovery_ab")),
            isolated_spike=bool(row.get("isolated_spike", False)),
            tradable_pass=bool(row.get("tradable_pass", False)),
            period_sign_pass=bool(row.get("period_sign_pass", False)),
            available_direction_pass=row.get("available_direction_pass"),
            robustness_required=bool(row.get("robustness_required", False)),
            robustness_pass=row.get("robustness_pass"),
        )
        all_offsets_evaluable = (
            not row.get("robustness_required") or row.get("offset_status") == "complete"
        )
        grade = compute_phase_b_evidence_grade(
            role=row.get("role", "ready_primary"),
            family=row.get("family"),
            screen_pass=screen["screen_pass"],
            failed_gates=screen["failed_gates"],
            valid_subperiods=int(row.get("valid_subperiods") or 0),
            all_offsets_evaluable=all_offsets_evaluable,
            n_independent_filing_windows=row.get("n_independent_filing_windows"),
            grade_a_min_independent_filing_windows=int(
                config.raw["phase_b"]["grade_a_min_independent_filing_windows"]
            ),
        )
        updated_combined.append({**row, **screen, "evidence_grade": grade})
    combined = updated_combined

    combined_by_id = {r["hypothesis_id"]: r for r in combined}
    phase_a_overlay = []
    for row in phase_a_rows:
        merged = combined_by_id[row["hypothesis_id"]]
        phase_a_overlay.append(
            {
                "hypothesis_id": row["hypothesis_id"],
                "family": row.get("family"),
                "q_fdr_phase_a": row.get("q_fdr_phase_a"),
                "bh_pass_phase_a": row.get("bh_pass_phase_a"),
                "primary_discovery_phase_a": row.get("primary_discovery_phase_a"),
                "q_fdr_global_ab": merged.get("q_fdr_global_ab"),
                "bh_pass_ab": merged.get("bh_pass_ab"),
                "primary_discovery_ab": merged.get("primary_discovery_ab"),
                "discovery_changed_vs_phase_a_only": (
                    bool(row.get("primary_discovery_phase_a"))
                    != bool(merged.get("primary_discovery_ab"))
                ),
            }
        )

    # §6 B-8 "결합 단면 permutation" — the null distribution was already
    # computed in run_phase_b_core (it needed a live lake connection this
    # function deliberately never opens); only the real-vs-null comparison
    # happens here, a pure-math step over that already-published file.
    combined_permutation_path = phase_b_run_dir / "core" / "permutation_summary.parquet"
    combined_permutation_summary = None
    if combined_permutation_path.is_file():
        null_counts = pl.read_parquet(combined_permutation_path)["n_discoveries"].to_list()
        real_discovery_count = sum(1 for row in combined if row.get("primary_discovery_ab"))
        combined_permutation_summary = {
            "real_discovery_count": real_discovery_count,
            "n_replicates": len(null_counts),
            "p_empirical_count": empirical_discovery_count_p(real_discovery_count, null_counts),
        }

    started_at = kst_now_iso()
    code_hash = phase_a_code_hash(sorted(Path(__file__).parent.glob("horizon_scan_phase_b*.py")))
    ab_run_id = f"{started_at[:19].replace(':', '').replace('-', '')}-{code_hash[:8]}"
    manifest = {
        "phase": "AB",
        "generated_at": started_at,
        "config_hash": config.config_hash,
        "q_threshold": q_threshold,
        "m_ab": len(combined),
        "phase_b_screen_pass_count": sum(
            1
            for row in combined
            if row["hypothesis_id"] in phase_b_ready_ids and row.get("screen_pass")
        ),
        "phase_b_evidence_grade_counts": {
            grade: sum(
                1
                for row in combined
                if row["hypothesis_id"] in phase_b_ready_ids and row.get("evidence_grade") == grade
            )
            for grade in ("A", "B", "C", "D")
        },
        "phase_a_run_id": phase_a_spec.get("run_id"),
        "phase_a_content_hash": phase_a_success.get("content_hash"),
        "phase_b_run_id": phase_b_spec.get("run_id"),
        "phase_b_content_hash": phase_b_success.get("content_hash"),
        "command_line": command_line,
        "run_id": ab_run_id,
    }
    if combined_permutation_summary is not None:
        manifest["combined_cross_sectional_permutation"] = combined_permutation_summary

    run_dir_root = (
        output_root
        / "phase=AB"
        / f"snapshot_date={phase_a_spec.get('snapshot_date')}"
        / f"source={phase_a_spec.get('source')}"
        / f"config_hash={config.config_hash}"
    )
    tmp_run_dir = run_dir_root / f"run_id={ab_run_id}.tmp"
    tmp_run_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(combined, infer_schema_length=None).write_parquet(
        tmp_run_dir / "combined_ab_primary_hypotheses.parquet"
    )
    pl.DataFrame(phase_a_overlay, infer_schema_length=None).write_parquet(
        tmp_run_dir / "phase_a_card_overlay.parquet"
    )
    # §7.1: phase=B and phase=AB both carry primary_feature_rank_correlation
    # .parquet — copied verbatim from the (already integrity-verified) Phase B
    # run rather than recomputed, since its content only depends on Phase B's
    # own ready continuous cells, not on the combined BH result.
    rank_corr_src = phase_b_run_dir / "core" / "primary_feature_rank_correlation.parquet"
    if rank_corr_src.is_file():
        shutil.copyfile(rank_corr_src, tmp_run_dir / "primary_feature_rank_correlation.parquet")
    (tmp_run_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8"
    )

    final_run_dir = run_dir_root / f"run_id={ab_run_id}"
    return publish_run(
        tmp_run_dir,
        final_run_dir,
        run_spec=manifest,
        required_artifacts=("manifest.json",),
        content_hash_exclude_names=frozenset({"manifest.json", "_SUCCESS.json"}),
    )
