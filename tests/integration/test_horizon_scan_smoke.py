"""Phase A synthetic end-to-end smoke test (§8.2).

Exercises the *real* production functions built across A-PR1..A-PR5 —
``register_analysis_panel`` (the actual 7-mart join), ``run_registry_scan``/
``apply_global_bh`` (A-2/A-3), the A-4/A-5 robustness/offset gates, the A-6
permutation/placebo replicate loops and look-ahead canary, the A-7/A-8 decay/
pattern/screen/grade/card logic, and the A-9 plot/markdown/atomic-publish
machinery — wired together end to end against a small synthetic 2-family,
12-ticker, 130-session in-memory lake.

This is a vertical-slice proof that every stage *composes*, not a stand-in
for the real 17-family/75-hypothesis official run: the two synthetic
families here are hand-built and not registered in
``horizon_scan_config.yaml``, so they never touch
``build_primary_hypothesis_registry``. The real-lake official run (§8.3:
100+100 replicates over the full ~6.5M-row panel) is a separate, hours-long
manual step this test does not attempt.
"""

from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

import duckdb
import polars as pl
import pytest
from research.analysis.horizon_scan_config import load_config
from research.analysis.horizon_scan_permutation import (
    run_cross_sectional_permutation,
    run_lookahead_canary,
    run_temporal_placebo,
    select_long_horizon_hypotheses,
)
from research.analysis.horizon_scan_report import (
    assign_evidence_grade,
    build_family_card,
    classify_pattern_auto,
    compute_decay_summary,
    compute_screen_pass,
    render_family_plots,
    write_markdown_report,
)
from research.analysis.horizon_scan_run_spec import (
    REQUIRED_A0_MARTS,
    build_run_spec,
    publish_run,
    write_run_spec,
)
from research.analysis.horizon_scan_runner import (
    apply_global_bh,
    compute_available_direction_pass,
    compute_tradable_pass,
    register_analysis_panel,
    run_nonoverlap_offsets,
    run_registry_scan,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

N_SESSIONS = 130
MARKETS = ("KOSPI", "KOSDAQ")
N_TICKERS = 6  # per market; the last ticker index is broad-only (not tradable/survivor)

PRIMARY_REGISTRY = [
    {
        "hypothesis_id": "px_test_family|px_feat|cum|0|5",
        "family": "px_test_family",
        "feature": "px_feat",
        "scan_type": "cum",
        "h_start": 0,
        "h_end": 5,
        "expected_sign": "+",
    },
    {
        "hypothesis_id": "px_test_family|px_feat|cum|0|60",
        "family": "px_test_family",
        "feature": "px_feat",
        "scan_type": "cum",
        "h_start": 0,
        "h_end": 60,
        "expected_sign": "+",
    },
    {
        "hypothesis_id": "flow_test_family|flow_feat|cum|0|20",
        "family": "flow_test_family",
        "feature": "flow_feat",
        "scan_type": "cum",
        "h_start": 0,
        "h_end": 20,
        "expected_sign": "+",
    },
]


def _build_synthetic_a0_lake(con: duckdb.DuckDBPyConnection) -> None:
    base = date(2024, 1, 1)
    mean_t = (N_TICKERS - 1) / 2.0
    label_rows, price_rows, flow_rows = [], [], []
    broad_rows, tradable_rows, pit_rows, quality_rows = [], [], [], []
    for session in range(1, N_SESSIONS + 1):
        d = base + timedelta(days=session - 1)
        for market in MARKETS:
            for t in range(N_TICKERS):
                ticker = f"{market[:1]}{t}"
                is_tradable = t < N_TICKERS - 1
                is_survivor = t < N_TICKERS - 1

                px_feat = float(t) + 0.01 * session
                px_feat_lag1 = float(t) + 0.01 * (session - 1) if session > 1 else None
                flow_feat = float(N_TICKERS - 1 - t) - 0.005 * session
                flow_feat_lag1 = (
                    float(N_TICKERS - 1 - t) - 0.005 * (session - 1) if session > 1 else None
                )

                raw_5 = float(t) * 2.0 + 3.0 * math.sin(t + 0.7 * session)
                raw_20 = float(N_TICKERS - 1 - t) * 2.0 + 3.0 * math.sin(t + 0.3 * session)
                raw_60 = float(t) * 2.0 + 3.0 * math.sin(t + 0.05 * session)
                fwd_ret_1d = float(t) + 0.01 * session

                price_rows.append(
                    {
                        "trade_date": d,
                        "ticker": ticker,
                        "market": market,
                        "px_feat": px_feat,
                        "px_feat_lag1": px_feat_lag1,
                    }
                )
                flow_rows.append(
                    {
                        "trade_date": d,
                        "ticker": ticker,
                        "market": market,
                        "flow_feat": flow_feat,
                        "flow_feat_lag1": flow_feat_lag1,
                        # feat_flow carries its own short_regime/short_balance_is_available;
                        # build_analysis_panel_sql EXCLUDEs them in favor of the quality
                        # view's copy, but they must still exist here to bind.
                        "short_regime": False,
                        "short_balance_is_available": True,
                    }
                )
                label_rows.append(
                    {
                        "trade_date": d,
                        "ticker": ticker,
                        "market": market,
                        "y_rank_5d": raw_5,
                        "raw_label_5d": raw_5,
                        "label_ok_5d": True,
                        "y_rank_20d": raw_20,
                        "raw_label_20d": raw_20,
                        "label_ok_20d": True,
                        "y_rank_60d": raw_60,
                        "raw_label_60d": raw_60,
                        "label_ok_60d": True,
                        "fwd_ret_1d": fwd_ret_1d,
                        "y_rank_1d": float(t) - mean_t,
                        "raw_label_1d": float(t) - mean_t,
                        "label_ok_1d": True,
                        "common_formation_120d": True,
                        "common_survivor_120d": is_survivor,
                    }
                )
                broad_rows.append(
                    {
                        "trade_date": d,
                        "ticker": ticker,
                        "market": market,
                        "in_universe": True,
                        "membership_reconstruction_available": True,
                        "management_filter_available": True,
                    }
                )
                tradable_rows.append(
                    {
                        "trade_date": d,
                        "ticker": ticker,
                        "market": market,
                        "in_universe": is_tradable,
                        "membership_reconstruction_available": True,
                        "management_filter_available": True,
                    }
                )
                pit_rows.append(
                    {
                        "trade_date": d,
                        "ticker": ticker,
                        "market": market,
                        "issued_shares_pit": 1_000_000.0,
                        "treasury_shares_pit": 0.0,
                        "float_shares_pit": 900_000.0,
                        "market_cap_pit": 1_000_000_000.0,
                        "shares_is_available": True,
                        "shares_invalid_flag": False,
                    }
                )
                quality_rows.append(
                    {
                        "trade_date": d,
                        "ticker": ticker,
                        "market": market,
                        "valid_session_idx": session,
                        "is_halted": False,
                        "volume_zero": False,
                        "simple_ret": 0.0,
                        "log_ret": 0.0,
                        "ca_mask": False,
                        "ca_event": False,
                        "ca_event_cumulative": 0,
                        "ca_rule_applicability_unknown": False,
                        "short_regime": False,
                        "short_balance_is_available": True,
                    }
                )

    def _table(name: str, rows: list[dict]) -> None:
        con.register("_src", pl.DataFrame(rows))
        con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM _src")
        con.unregister("_src")

    _table("label_scan", label_rows)
    _table("feat_price", price_rows)
    _table("feat_flow", flow_rows)
    _table("dim_universe_broad_daily", broad_rows)
    _table("dim_universe_tradable_daily", tradable_rows)
    _table("dim_stock_pit_daily", pit_rows)
    _table("dim_price_quality_daily", quality_rows)


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    _build_synthetic_a0_lake(connection)
    register_analysis_panel(connection)  # real A-1 join; raises on any fan-out
    return connection


def test_full_phase_a_pipeline_runs_end_to_end_on_a_synthetic_lake(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    sample_start = "2024-01-01"
    scan_kwargs = dict(
        sample_start=sample_start,
        min_names=3,
        min_names_for_spread=3,
        quantile_count=5,
        min_dates_per_cell=10,
    )

    # --- A-2/A-3: core scan + global BH ---
    rows = run_registry_scan(con, PRIMARY_REGISTRY, **scan_kwargs)
    assert len(rows) == len(PRIMARY_REGISTRY) * 4  # 4 universe x sample_kind combos
    assert all(r["status"] == "valid" for r in rows), rows

    broad_common_survivor = [
        r for r in rows if r["universe"] == "broad" and r["sample_kind"] == "common_survivor"
    ]
    bh_rows = apply_global_bh(broad_common_survivor, q_threshold=0.10)
    assert {r["hypothesis_id"] for r in bh_rows} == {h["hypothesis_id"] for h in PRIMARY_REGISTRY}
    real_discovery_count = sum(1 for r in bh_rows if r["primary_discovery"])
    assert real_discovery_count > 0  # the synthetic signal is strong by construction

    # --- A-4: robustness gates over the already-scanned universe/sample combos ---
    by_key = {(r["hypothesis_id"], r["universe"], r["sample_kind"]): r for r in rows}
    for hyp in PRIMARY_REGISTRY:
        hid = hyp["hypothesis_id"]
        broad = by_key[(hid, "broad", "common_survivor")]
        tradable = by_key[(hid, "tradable", "common_survivor")]
        available = by_key[(hid, "broad", "available")]
        tradable_gate = compute_tradable_pass(
            ic_broad=broad["ic_mean"], ic_tradable=tradable["ic_mean"]
        )
        assert isinstance(tradable_gate["tradable_pass"], bool)
        available_gate = compute_available_direction_pass(
            ic_common_survivor=broad["ic_mean"], ic_available=available["ic_mean"]
        )
        assert available_gate["available_direction_pass"] in (True, False, None)

    # --- A-5: non-overlap offsets for the h=5 primary cell ---
    offset_summary = run_nonoverlap_offsets(
        con,
        feature_col="px_feat",
        scan_type="cum",
        h_start=0,
        h_end=5,
        universe="broad",
        sample_kind="common_survivor",
        sample_start=sample_start,
        min_names=3,
        nonoverlap_min_dates=5,
        alignment_sign=1.0,
    )
    assert offset_summary["n_offsets_total"] == 5
    assert offset_summary["offset_status"] in ("complete", "some_insufficient")

    # --- A-6: replicate loops (small repeat counts — the real official run uses 100) ---
    permutation_result = run_cross_sectional_permutation(
        con,
        panel_view="analysis_panel",
        primary_registry=PRIMARY_REGISTRY,
        real_discovery_count=real_discovery_count,
        config_hash="synthetic-e2e",
        n_replicates=5,
        **scan_kwargs,
    )
    assert len(permutation_result["replicate_summaries"]) == 5
    assert 0 < permutation_result["p_empirical_count"] <= 1.0

    long_registry = select_long_horizon_hypotheses(PRIMARY_REGISTRY)
    assert {h["hypothesis_id"] for h in long_registry} == {
        "px_test_family|px_feat|cum|0|60"
    }
    real_t_nw_by_id = {r["hypothesis_id"]: r["t_nw"] for r in broad_common_survivor}
    temporal_result = run_temporal_placebo(
        con,
        panel_view="analysis_panel",
        long_horizon_registry=long_registry,
        real_t_nw_by_id=real_t_nw_by_id,
        config_hash="synthetic-e2e",
        n_replicates=5,
        min_shift_sessions=15,
        **scan_kwargs,
    )
    assert len(temporal_result["replicate_meta"]) == 5
    long_hid = long_registry[0]["hypothesis_id"]
    assert 0 < temporal_result["per_cell"][long_hid]["p_temporal_nw"] <= 1.0

    canary = run_lookahead_canary(
        con, sample_start=sample_start, min_names=3, min_dates_per_cell=10
    )
    assert canary["canary_pass"] is True

    # --- A-7/A-8: decay summary, pattern, screen_pass, evidence grade, family card ---
    cards = []
    for family, feature in (("px_test_family", "px_feat"), ("flow_test_family", "flow_feat")):
        family_bh_rows = [r for r in bh_rows if r["family"] == family]
        decay = compute_decay_summary(family_bh_rows, [], expected_sign="+")
        pattern = classify_pattern_auto(
            has_primary_discovery=any(r["primary_discovery"] for r in family_bh_rows),
            has_exploratory_significant=False,
            peak_bucket=decay["peak_bucket"],
            sign_flip_bucket=decay["sign_flip_bucket"],
            segment_gates_all_pass=True,
        )
        screen = compute_screen_pass(
            role="ready",
            primary_discovery=any(r["primary_discovery"] for r in family_bh_rows),
            tradable_pass=True,
            period_sign_pass=True,
            isolated_spike=False,
            available_direction_pass=True,
            delay_required=False,
            delay_pass=None,
            temporal_null_required=False,
            temporal_null_pass=None,
        )
        grade = assign_evidence_grade(role="ready", screen_pass=screen["screen_pass"])
        cards.append(
            build_family_card(
                family=family,
                domain="synthetic",
                primary_feature=feature,
                expected_sign="+",
                observed_sign="+",
                decay_summary=decay,
                pattern_auto=pattern,
                primary_discoveries=[
                    r["hypothesis_id"] for r in family_bh_rows if r["primary_discovery"]
                ],
                candidate_horizon_band=None,
                broad_ic=family_bh_rows[0]["ic_mean"],
                tradable_ic=None,
                tradable_retention=None,
                valid_subperiods=0,
                sign_consistent_subperiods=0,
                native_ic=family_bh_rows[0]["ic_mean"],
                lag1_ic=None,
                delay_pass=None,
                common_survivor_ic=family_bh_rows[0]["ic_mean"],
                available_ic=None,
                attrition_warning=False,
                nonoverlap_offset_summary=None,
                kospi_weight_mean=family_bh_rows[0]["kospi_weight_mean"],
                kosdaq_weight_mean=family_bh_rows[0]["kosdaq_weight_mean"],
                p_temporal_nw=None,
                temporal_null_pass=None,
                q_fdr_global=family_bh_rows[0]["q_fdr_global"],
                evidence_grade=grade,
                screen_pass=screen["screen_pass"],
            )
        )

    # --- A-9: plots, markdown report, run_spec/manifest, atomic publish ---
    run_dir_root = tmp_path
    tmp_run_dir = run_dir_root / "run_id.tmp"
    plots_dir = tmp_run_dir / "plots"
    for card in cards:
        render_family_plots(
            family=card["family"],
            output_dir=plots_dir,
            cumulative_curves={
                "broad_common_survivor": [
                    {"h_end": r["h_end"], "ic_mean": r["ic_mean"]}
                    for r in bh_rows
                    if r["family"] == card["family"]
                ]
            },
            bucket_rows=[],
            expected_sign=card["expected_sign"],
            native_rows=[],
            lag1_rows=[],
            period_rows=[],
            segment_rows=[],
            coverage_rows=[],
            offset_summary=(
                offset_summary if card["family"] == "px_test_family" else {"offsets": []}
            ),
        )
    plot_files = list(plots_dir.glob("*.png"))
    assert len(plot_files) == 7 * len(cards)

    config = load_config()
    manifest = {
        "status": "success",
        "config_hash": config.config_hash,
        "smoke_only": False,
        "raw_marker": "synthetic",
        "marts": [
            {"view": name, "row_count": 1000, "schema_hash": f"hash-{name}"}
            for name in REQUIRED_A0_MARTS
        ],
    }
    # smoke_family forces official=False/smoke_only=True — this synthetic fixture run
    # must never be mistaken for a real official run downstream.
    run_spec = build_run_spec(
        config,
        manifest,
        snapshot_date="2024-01-01",
        source="synthetic",
        resolution_auto_selected=True,
        smoke_family="synthetic_fixture_smoke",
        permutation_repeats_override=None,
        include_holdout=False,
        holdout_start_override=None,
        repo_root=REPO_ROOT,
        code_paths=[Path(__file__)],
        command_line=["pytest", "test_horizon_scan_smoke.py"],
        started_at="2024-01-01T00:00:00+09:00",
    )
    assert run_spec["official"] is False
    write_run_spec(tmp_run_dir, run_spec)
    (tmp_run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    core_dir = tmp_run_dir / "core"
    core_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(bh_rows).write_parquet(core_dir / "horizon_ic.parquet")
    cards_dir = tmp_run_dir / "cards"
    cards_dir.mkdir(parents=True, exist_ok=True)
    (cards_dir / "family_cards.json").write_text(json.dumps(cards, default=str), encoding="utf-8")

    report_context = {
        "run_identity": {
            "run_id": run_spec["run_id"],
            "snapshot_date": run_spec["snapshot_date"],
            "source": run_spec["source"],
            "config_hash": run_spec["config_hash"],
            "official": run_spec["official"],
            "started_at": run_spec["started_at"],
            "finished_at": None,
        },
        "preflight": {"status": "ok"},
        "sample_coverage": {
            "holdout_start": config.raw["sample"]["holdout_start"],
            "effective_sample_start": sample_start,
            "effective_sample_end": None,
            "common_formation_end": None,
        },
        "bh_summary": {
            "n_hypotheses": len(PRIMARY_REGISTRY),
            "n_valid": len(bh_rows),
            "n_bh_pass": sum(1 for r in bh_rows if r["bh_pass"]),
            "n_primary_discovery": real_discovery_count,
            "q_threshold": 0.10,
        },
        "short_exploratory_summary": {"n_cells": 0, "n_valid": 0},
        "permutation_summary": {
            "real_discovery_count": real_discovery_count,
            "p_empirical_count": permutation_result["p_empirical_count"],
            "n_replicates": permutation_result["n_replicates"],
        },
        "temporal_summary": {
            "n_replicates": temporal_result["n_replicates"],
            "per_cell": temporal_result["per_cell"],
        },
        "price_cards": [cards[0]],
        "flow_cards": [cards[1]],
        "warnings": [],
        "acceptance_gate": [],
        "deferred_candidates": [],
        "limitations": [],
    }
    write_markdown_report(tmp_run_dir / "03a_horizon_scan_results.md", report_context)

    final_run_dir = run_dir_root / run_spec["run_id"]
    published = publish_run(tmp_run_dir, final_run_dir, run_spec=run_spec)

    assert published == final_run_dir
    assert not tmp_run_dir.exists()
    assert (final_run_dir / "_SUCCESS.json").is_file()
    assert (final_run_dir / "core" / "horizon_ic.parquet").is_file()
    assert (final_run_dir / "cards" / "family_cards.json").is_file()
    assert (final_run_dir / "03a_horizon_scan_results.md").is_file()
    assert len(list((final_run_dir / "plots").glob("*.png"))) == 7 * len(cards)
