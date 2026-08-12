"""Tests for the real-lake Phase B orchestration + A+B combined BH wiring
(``research/analysis/horizon_scan_phase_b_run.py``) — see
``08_phase_b_implementation_log.md`` §3 remaining work item 3.

``register_phase_b_marts``'s dependency-order/graceful-degradation logic is
tested by stubbing the ``materialize_*`` functions it calls (each already has
its own dedicated real-SQL test coverage in ``test_metric_vintages.py`` /
``test_financial_quarters.py`` / ``test_research_fin_scan.py`` /
``test_research_event_scan.py`` / ``test_research_sue_event.py`` — this file
is only responsible for the NEW glue: call order, short-circuiting, and which
mart names end up in ``available_assets``).

``load_phase_a_primary_rows``/``run_combined_ab`` are tested against a
hand-built but *realistic* fake published Phase A run directory — built from
the real 75-cell primary registry and the real ``apply_global_bh`` so the
BH math in the fixture is authentic, not hand-faked.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import polars as pl
import pytest
import research.analysis.horizon_scan_phase_b_run as phase_b_run
from research.analysis.horizon_scan_config import CONFIG_PATH, load_config
from research.analysis.horizon_scan_phase_b import PHASE_B_CONTENT_HASH_EXCLUDE_NAMES
from research.analysis.horizon_scan_phase_b_run import (
    _render_readiness_matrix_md,
    compute_phase_b_gate_updates,
    load_phase_a_primary_rows,
    register_phase_b_marts,
    run_combined_ab,
)
from research.analysis.horizon_scan_readiness import build_primary_hypothesis_registry
from research.analysis.horizon_scan_run_spec import compute_run_content_hash, kst_now_iso
from research.analysis.horizon_scan_runner import apply_global_bh

# --- register_phase_b_marts: dependency order + graceful degradation ---


def _con_with_daily_ohlcv() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE daily_ohlcv (trade_date DATE, ticker VARCHAR, market VARCHAR)")
    con.execute("INSERT INTO daily_ohlcv VALUES (DATE '2024-01-02', 'A', 'KOSPI')")
    return con


def _stub(monkeypatch: pytest.MonkeyPatch, name: str, calls: list[str], *, fail: bool = False):
    def _fn(*_args, **_kwargs):
        calls.append(name)
        if fail:
            raise duckdb.CatalogException(f"{name} unavailable")

    monkeypatch.setattr(phase_b_run, f"materialize_{name}", _fn)


def test_register_phase_b_marts_all_succeed(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    _stub(monkeypatch, "stock_metric_vintage_fact", calls)
    _stub(monkeypatch, "fin_quarterly_metric_vintage", calls)
    _stub(monkeypatch, "fin_scan_daily", calls)
    _stub(monkeypatch, "event_scan_daily", calls)
    _stub(monkeypatch, "sue_event", calls)

    result = register_phase_b_marts(_con_with_daily_ohlcv(), lake=object())

    assert result == {
        "stock_metric_vintage_fact",
        "fin_quarterly_metric_vintage",
        "feat_fin_scan_daily",
        "feat_event_scan_daily",
        "fin_sue_event",
    }
    assert calls == [
        "stock_metric_vintage_fact",
        "fin_quarterly_metric_vintage",
        "fin_scan_daily",
        "event_scan_daily",
        "sue_event",
    ]


def test_register_phase_b_marts_stops_at_root_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """``dart_filing_receipt_raw`` missing today (real local lake state) means
    ``stock_metric_vintage_fact`` itself fails — nothing downstream should
    even be attempted, and the run must not crash."""
    calls: list[str] = []
    _stub(monkeypatch, "stock_metric_vintage_fact", calls, fail=True)
    _stub(monkeypatch, "fin_quarterly_metric_vintage", calls)
    _stub(monkeypatch, "fin_scan_daily", calls)
    _stub(monkeypatch, "event_scan_daily", calls)
    _stub(monkeypatch, "sue_event", calls)

    result = register_phase_b_marts(_con_with_daily_ohlcv(), lake=object())

    assert result == set()
    assert calls == ["stock_metric_vintage_fact"]


def test_register_phase_b_marts_partial_availability_degrades_gracefully(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``dart_capital_change_raw`` missing but ``dart_filing_receipt_raw``
    present: SMVF/FQMV/fin_scan/sue_event all succeed, only
    ``feat_event_scan_daily`` (which needs capital_change directly) fails —
    proving one missing raw source blocks only its dependent families, not
    the whole run."""
    calls: list[str] = []
    _stub(monkeypatch, "stock_metric_vintage_fact", calls)
    _stub(monkeypatch, "fin_quarterly_metric_vintage", calls)
    _stub(monkeypatch, "fin_scan_daily", calls)
    _stub(monkeypatch, "event_scan_daily", calls, fail=True)
    _stub(monkeypatch, "sue_event", calls)

    result = register_phase_b_marts(_con_with_daily_ohlcv(), lake=object())

    assert result == {
        "stock_metric_vintage_fact",
        "fin_quarterly_metric_vintage",
        "feat_fin_scan_daily",
        "fin_sue_event",
    }
    assert set(calls) == {
        "stock_metric_vintage_fact",
        "fin_quarterly_metric_vintage",
        "fin_scan_daily",
        "event_scan_daily",
        "sue_event",
    }


def test_register_phase_b_marts_stops_after_second_stage_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    _stub(monkeypatch, "stock_metric_vintage_fact", calls)
    _stub(monkeypatch, "fin_quarterly_metric_vintage", calls, fail=True)
    _stub(monkeypatch, "fin_scan_daily", calls)
    _stub(monkeypatch, "event_scan_daily", calls)
    _stub(monkeypatch, "sue_event", calls)

    result = register_phase_b_marts(_con_with_daily_ohlcv(), lake=object())

    assert result == {"stock_metric_vintage_fact"}
    assert calls == ["stock_metric_vintage_fact", "fin_quarterly_metric_vintage"]


# --- compute_phase_b_gate_updates: §9 B-9 rule wiring, robustness calls mocked ---


def _base_continuous_cell(hid: str = "fin_log_mcap|fin_log_mcap|cum|0|60") -> dict:
    return {
        "hypothesis_id": hid,
        "feature": "fin_log_mcap",
        "cell_type": "cumulative",
        "h_start": 0,
        "h_end": 60,
        "expected_sign": "-",
    }


def _four_combo_rows(hid: str, *, broad_cs: float, broad_av: float, trad_cs: float) -> list[dict]:
    return [
        {
            "hypothesis_id": hid,
            "universe": "broad",
            "sample_kind": "common_survivor",
            "ic_mean": broad_cs,
            "t_nw": broad_cs * 60,
        },
        {
            "hypothesis_id": hid,
            "universe": "broad",
            "sample_kind": "available",
            "ic_mean": broad_av,
            "t_nw": broad_av * 60,
        },
        {
            "hypothesis_id": hid,
            "universe": "tradable",
            "sample_kind": "common_survivor",
            "ic_mean": trad_cs,
            "t_nw": trad_cs * 60,
        },
        {
            "hypothesis_id": hid,
            "universe": "tradable",
            "sample_kind": "available",
            "ic_mean": trad_cs * 0.9,
            "t_nw": trad_cs * 0.9 * 60,
        },
    ]


_SCAN_KWARGS = {
    "sample_start": "2020-01-01",
    "min_names": 30,
    "min_names_for_spread": 30,
    "quantile_count": 5,
    "min_dates_per_cell": 20,
}


def test_compute_phase_b_gate_updates_wires_continuous_and_sue_gates(
    monkeypatch: pytest.MonkeyPatch, config
) -> None:
    cont_cell = _base_continuous_cell()
    cont_hid = cont_cell["hypothesis_id"]
    ready_continuous = [cont_cell]
    ready_events = [
        {
            "hypothesis_id": "fin_sue|fin_sue|event|0|3",
            "feature": "fin_sue",
            "h_start": 0,
            "h_end": 3,
            "expected_sign": "+",
        }
    ]
    sue_hid = ready_events[0]["hypothesis_id"]
    continuous_scanned_rows = _four_combo_rows(
        cont_hid, broad_cs=-0.05, broad_av=-0.04, trad_cs=-0.03
    )
    event_scanned_rows = [{"hypothesis_id": sue_hid, "scan_type": "event_bucket"}]

    monkeypatch.setattr(
        phase_b_run, "_register_phase_b_period_segment_view", lambda *a, **k: "period_view"
    )
    monkeypatch.setattr(
        phase_b_run, "_compute_phase_b_period_ics", lambda *a, **k: [-0.02, -0.01, 0.03]
    )
    monkeypatch.setattr(
        phase_b_run, "select_phase_b_long_horizon_cells", lambda cells, **k: list(cells)
    )
    monkeypatch.setattr(
        phase_b_run,
        "run_phase_b_continuous_nonoverlap",
        lambda con, cells, **k: [
            {
                "hypothesis_id": c["hypothesis_id"],
                "nonoverlap_robustness_pass": True,
                "offset_status": "complete",
            }
            for c in cells
        ],
    )
    monkeypatch.setattr(
        phase_b_run,
        "run_phase_b_temporal_placebo",
        lambda *a, **k: {
            "per_cell": {cont_hid: {"temporal_null_pass": True, "p_temporal_nw": 0.02}},
            "n_replicates": 100,
            "total_sessions": 500,
        },
    )
    monkeypatch.setattr(
        phase_b_run,
        "run_issuer_cluster_bootstrap",
        lambda *a, **k: {
            "cluster_confirm_pass": True,
            "bootstrap_p": 0.01,
            "bootstrap_mean": 0.02,
        },
    )
    monkeypatch.setattr(
        phase_b_run,
        "run_filing_cycle_block_bootstrap",
        lambda *a, **k: {
            "cluster_confirm_pass": True,
            "bootstrap_p": 0.03,
            "bootstrap_mean": 0.015,
        },
    )
    monkeypatch.setattr(
        phase_b_run,
        "run_sue_event_ordinal_nonoverlap",
        lambda con, cells, **k: [
            {
                "hypothesis_id": c["hypothesis_id"],
                "nonoverlap_robustness_pass": True,
                "offset_status": "complete",
            }
            for c in cells
        ],
    )

    updates, diagnostics = compute_phase_b_gate_updates(
        object(),
        config,
        ready_continuous=ready_continuous,
        ready_events=ready_events,
        continuous_scanned_rows=continuous_scanned_rows,
        event_scanned_rows=event_scanned_rows,
        scan_kwargs=_SCAN_KWARGS,
    )

    cont = updates[cont_hid]
    assert cont["tradable_pass"] is True  # |-0.03|/|-0.05| = 0.6 >= 0.5, same sign
    assert cont["available_direction_pass"] is True  # -0.05 and -0.04 agree
    assert cont["period_sign_pass"] is True  # expected_sign "-": 2 of 3 periods negative
    assert cont["robustness_required"] is True
    assert cont["robustness_pass"] is True  # nonoverlap True and temporal True
    assert cont["offset_status"] == "complete"

    sue = updates[sue_hid]
    assert sue["robustness_required"] is True
    assert sue["robustness_pass"] is True
    assert sue["issuer_bootstrap_p"] == 0.01
    assert sue["filing_cycle_bootstrap_p"] == 0.03
    assert sue["event_ordinal_nonoverlap_pass"] is True
    assert sue["event_ordinal_offset_status"] == "complete"

    # §6 B-10 Stage 1 — the full per-cell robustness rows survive alongside
    # the gate_updates fields, not just the couple of fields folded in above.
    assert [r["hypothesis_id"] for r in diagnostics["nonoverlap_rows"]] == [cont_hid]
    assert diagnostics["nonoverlap_rows"][0]["nonoverlap_robustness_pass"] is True
    assert [r["hypothesis_id"] for r in diagnostics["temporal_placebo_rows"]] == [cont_hid]
    assert diagnostics["temporal_placebo_rows"][0]["p_temporal_nw"] == 0.02
    assert [r["hypothesis_id"] for r in diagnostics["issuer_bootstrap_rows"]] == [sue_hid]
    assert diagnostics["issuer_bootstrap_rows"][0]["bootstrap_mean"] == 0.02
    assert [r["hypothesis_id"] for r in diagnostics["filing_cycle_bootstrap_rows"]] == [sue_hid]
    assert diagnostics["filing_cycle_bootstrap_rows"][0]["bootstrap_mean"] == 0.015


def test_compute_phase_b_gate_updates_short_cell_skips_robustness_gate(
    monkeypatch: pytest.MonkeyPatch, config
) -> None:
    """A continuous cell ``select_phase_b_long_horizon_cells`` does not select
    (nw_lag < 59) must keep the default ``robustness_required=False`` even
    though tradable/period gates were still computed for it."""
    cont_cell = _base_continuous_cell()
    cont_hid = cont_cell["hypothesis_id"]
    continuous_scanned_rows = _four_combo_rows(
        cont_hid, broad_cs=-0.05, broad_av=-0.04, trad_cs=-0.03
    )

    monkeypatch.setattr(
        phase_b_run, "_register_phase_b_period_segment_view", lambda *a, **k: "period_view"
    )
    monkeypatch.setattr(phase_b_run, "_compute_phase_b_period_ics", lambda *a, **k: [-0.02, -0.01])
    monkeypatch.setattr(phase_b_run, "select_phase_b_long_horizon_cells", lambda cells, **k: [])

    updates, diagnostics = compute_phase_b_gate_updates(
        object(),
        config,
        ready_continuous=[cont_cell],
        ready_events=[],
        continuous_scanned_rows=continuous_scanned_rows,
        event_scanned_rows=[],
        scan_kwargs=_SCAN_KWARGS,
    )

    cont = updates[cont_hid]
    assert cont["tradable_pass"] is True
    assert cont["robustness_required"] is False
    assert cont["robustness_pass"] is None
    # no long-horizon cell selected → no robustness diagnostic rows at all
    assert diagnostics["nonoverlap_rows"] == []
    assert diagnostics["temporal_placebo_rows"] == []


def test_compute_phase_b_gate_updates_empty_scans_only_default_robustness_keys(
    config,
) -> None:
    """When nothing was actually scanned (today's real state — see
    ``horizon_scan_phase_b_run``'s module docstring), every ready cell still
    gets a well-formed default entry, never a missing key."""
    ready_continuous = [_base_continuous_cell()]
    ready_events = [{"hypothesis_id": "fin_sue|fin_sue|event|0|3"}]

    updates, diagnostics = compute_phase_b_gate_updates(
        object(),
        config,
        ready_continuous=ready_continuous,
        ready_events=ready_events,
        continuous_scanned_rows=[],
        event_scanned_rows=[],
        scan_kwargs=_SCAN_KWARGS,
    )

    assert updates == {
        ready_continuous[0]["hypothesis_id"]: {
            "robustness_required": False,
            "robustness_pass": None,
        },
        ready_events[0]["hypothesis_id"]: {
            "robustness_required": False,
            "robustness_pass": None,
        },
    }
    assert diagnostics == {
        "nonoverlap_rows": [],
        "temporal_placebo_rows": [],
        "issuer_bootstrap_rows": [],
        "filing_cycle_bootstrap_rows": [],
    }


# --- _render_readiness_matrix_md: §7.1 readiness_matrix.md (B-10 Stage 1) ---


def test_render_readiness_matrix_md_sorts_and_includes_every_row() -> None:
    rows = [
        {
            "family": "fin_value_z",
            "feature": "fin_value_z",
            "cell_type": "cumulative",
            "h_start": 0,
            "h_end": 60,
            "role": "blocked_exploratory",
            "status": "blocked_missing_dependency",
            "missing_dependencies": "dart_filing_receipt_raw",
        },
        {
            "family": "fin_log_mcap",
            "feature": "fin_log_mcap",
            "cell_type": "cumulative",
            "h_start": 0,
            "h_end": 60,
            "role": "ready_primary",
            "status": "ready",
            "missing_dependencies": "",
        },
    ]

    md = _render_readiness_matrix_md(rows)
    lines = md.strip().splitlines()

    assert lines[0].split("|")[1].strip() == "family"
    # sorted by (family, feature, h_start, h_end) — fin_log_mcap before fin_value_z
    data_lines = lines[2:]
    assert "fin_log_mcap" in data_lines[0]
    assert "fin_value_z" in data_lines[1]
    assert "dart_filing_receipt_raw" in data_lines[1]


# --- fake published Phase A / Phase B run directories, built from real registries ---


def _fake_phase_a_run(
    tmp_path: Path, config, *, drop_id: str | None = None, run_id: str = "fake-a-001"
) -> Path:
    registry = build_primary_hypothesis_registry(config)
    rows = []
    for i, cell in enumerate(registry):
        sign = 1.0 if cell["expected_sign"] == "+" else -1.0
        rows.append(
            {
                **cell,
                "universe": "broad",
                "sample_kind": "common_survivor",
                "status": "valid",
                "ic_mean": sign * (0.05 if i % 3 else -0.01),
                "p_nw": 0.001 if i % 3 else 0.9,
            }
        )
    if drop_id is not None:
        rows = [r for r in rows if r["hypothesis_id"] != drop_id]
    bh_rows = apply_global_bh(rows, q_threshold=0.10)

    run_dir = tmp_path / "phase_a" / run_id
    core_dir = run_dir / "core"
    core_dir.mkdir(parents=True)
    pl.DataFrame(bh_rows, infer_schema_length=None).write_parquet(core_dir / "horizon_ic.parquet")

    run_spec = {
        "phase": "A",
        "config_hash": config.config_hash,
        "run_id": run_id,
        "snapshot_date": "2024-01-01",
        "source": "test_source",
    }
    (run_dir / "run_spec.json").write_text(json.dumps(run_spec), encoding="utf-8")

    content_hash = compute_run_content_hash(run_dir)
    success = {
        "status": "success",
        "run_id": run_id,
        "config_hash": config.config_hash,
        "content_hash": content_hash,
        "published_at": kst_now_iso(),
    }
    (run_dir / "_SUCCESS.json").write_text(json.dumps(success), encoding="utf-8")
    return run_dir


def _fake_phase_b_run(
    tmp_path: Path,
    config,
    *,
    ready_rows: list[dict],
    run_id: str = "fake-b-001",
    rank_correlation_rows: list[dict] | None = None,
    permutation_summary_rows: list[dict] | None = None,
) -> Path:
    run_dir = tmp_path / "phase_b" / run_id
    core_dir = run_dir / "core"
    core_dir.mkdir(parents=True)
    pl.DataFrame(ready_rows, infer_schema_length=None).write_parquet(
        core_dir / "phase_b_primary_hypotheses.parquet"
    )
    if rank_correlation_rows is not None:
        pl.DataFrame(rank_correlation_rows, infer_schema_length=None).write_parquet(
            core_dir / "primary_feature_rank_correlation.parquet"
        )
    if permutation_summary_rows is not None:
        pl.DataFrame(permutation_summary_rows, infer_schema_length=None).write_parquet(
            core_dir / "permutation_summary.parquet"
        )
    run_spec = {"phase": "B", "config_hash": config.config_hash, "run_id": run_id}
    (run_dir / "phase_b_run_spec.json").write_text(json.dumps(run_spec), encoding="utf-8")

    content_hash = compute_run_content_hash(
        run_dir, exclude_names=PHASE_B_CONTENT_HASH_EXCLUDE_NAMES
    )
    success = {
        "status": "success",
        "run_id": run_id,
        "config_hash": config.config_hash,
        "content_hash": content_hash,
        "published_at": kst_now_iso(),
    }
    (run_dir / "_SUCCESS.json").write_text(json.dumps(success), encoding="utf-8")
    return run_dir


@pytest.fixture(scope="module")
def config():
    return load_config(CONFIG_PATH)


# --- load_phase_a_primary_rows ---


def test_load_phase_a_primary_rows_happy_path(tmp_path: Path, config) -> None:
    run_dir = _fake_phase_a_run(tmp_path, config)
    rows = load_phase_a_primary_rows(run_dir, config)
    assert len(rows) == 75
    assert all("q_fdr_phase_a" in r and "primary_discovery_phase_a" in r for r in rows)


def test_load_phase_a_primary_rows_rejects_config_hash_mismatch(tmp_path: Path, config) -> None:
    run_dir = _fake_phase_a_run(tmp_path, config)
    spec_path = run_dir / "run_spec.json"
    spec = json.loads(spec_path.read_text())
    spec["config_hash"] = "not-the-real-hash"
    spec_path.write_text(json.dumps(spec), encoding="utf-8")

    with pytest.raises(ValueError, match="config_hash"):
        load_phase_a_primary_rows(run_dir, config)


def test_load_phase_a_primary_rows_rejects_tampered_content(tmp_path: Path, config) -> None:
    run_dir = _fake_phase_a_run(tmp_path, config)
    frame = pl.read_parquet(run_dir / "core" / "horizon_ic.parquet")
    frame = frame.with_columns(pl.lit(999.0).alias("ic_mean"))
    frame.write_parquet(run_dir / "core" / "horizon_ic.parquet")

    with pytest.raises(ValueError, match="content hash mismatch"):
        load_phase_a_primary_rows(run_dir, config)


def test_load_phase_a_primary_rows_rejects_incomplete_population(tmp_path: Path, config) -> None:
    registry = build_primary_hypothesis_registry(config)
    run_dir = _fake_phase_a_run(tmp_path, config, drop_id=registry[0]["hypothesis_id"])

    with pytest.raises(ValueError, match="do not match the current"):
        load_phase_a_primary_rows(run_dir, config)


# --- run_combined_ab ---


def _synthetic_phase_b_ready_rows() -> list[dict]:
    return [
        {
            "hypothesis_id": "fin_log_mcap|fin_log_mcap|cum|0|60",
            "family": "fin_log_mcap",
            "scan_type": "cum",
            "h_end": 60,
            "status": "valid",
            "p_nw": 0.001,
            "expected_sign": "-",
            "ic_mean": -0.04,
            "role": "ready_primary",
            "tradable_pass": True,
            "period_sign_pass": True,
            "valid_subperiods": 3,
            "available_direction_pass": True,
            "robustness_required": False,
            "robustness_pass": None,
        },
        {
            "hypothesis_id": "fin_value_z|fin_value_z|cum|0|60",
            "family": "fin_value_z",
            "scan_type": "cum",
            "h_end": 60,
            "status": "valid",
            "p_nw": 0.8,
            "expected_sign": "+",
            "ic_mean": 0.01,
            "role": "ready_primary",
        },
        {
            "hypothesis_id": "fin_sue|fin_sue|event|0|3",
            "family": "fin_sue",
            "scan_type": "event_bucket",
            "h_end": 3,
            "status": "not_evaluated",
            "role": "blocked_exploratory",
        },
    ]


def test_run_combined_ab_happy_path(tmp_path: Path, config, monkeypatch) -> None:
    phase_a_dir = _fake_phase_a_run(tmp_path, config)
    ready_rows = _synthetic_phase_b_ready_rows()
    phase_b_dir = _fake_phase_b_run(tmp_path, config, ready_rows=ready_rows)

    published = run_combined_ab(
        phase_a_run_dir=phase_a_dir,
        phase_b_run_dir=phase_b_dir,
        output_root=tmp_path / "output",
        command_line=["test"],
    )

    assert (published / "_SUCCESS.json").is_file()
    combined = pl.read_parquet(published / "combined_ab_primary_hypotheses.parquet")
    n_ready_b = sum(1 for r in ready_rows if r["role"] == "ready_primary")
    assert combined.height == 75 + n_ready_b
    assert combined["q_fdr_global_ab"].null_count() == 0

    combined_by_id = {r["hypothesis_id"]: r for r in combined.to_dicts()}
    strong = combined_by_id["fin_log_mcap|fin_log_mcap|cum|0|60"]
    assert strong["screen_pass"] is True
    assert strong["failed_gates"] == []
    assert strong["evidence_grade"] == "A"
    weak = combined_by_id["fin_value_z|fin_value_z|cum|0|60"]
    assert weak["screen_pass"] is False
    assert "primary_discovery" in weak["failed_gates"]
    assert weak["evidence_grade"] == "D"
    # Phase A's 75 never get this function's screen_pass/evidence_grade
    # columns — they carry their own family-level versions elsewhere (the
    # family card).
    phase_a_row = combined_by_id["px_reversal_5d|px_reversal_5d|cum|0|1"]
    assert phase_a_row.get("screen_pass") is None
    assert phase_a_row.get("evidence_grade") is None

    overlay = pl.read_parquet(published / "phase_a_card_overlay.parquet")
    assert overlay.height == 75
    assert overlay["q_fdr_global_ab"].null_count() == 0

    manifest = json.loads((published / "manifest.json").read_text())
    assert manifest["m_ab"] == 75 + n_ready_b
    assert manifest["phase_a_run_id"] == "fake-a-001"
    assert manifest["phase_b_run_id"] == "fake-b-001"
    assert manifest["phase_b_screen_pass_count"] == 1
    assert manifest["phase_b_evidence_grade_counts"] == {"A": 1, "B": 0, "C": 0, "D": 1}

    # Republishing to the same run_id must refuse to overwrite (immutability).
    # ab_run_id is derived from kst_now_iso() at second precision, so pin the
    # clock to the first publish's timestamp — otherwise this assertion only
    # holds when both calls happen to land inside the same wall-clock second.
    monkeypatch.setattr(phase_b_run, "kst_now_iso", lambda: manifest["generated_at"])
    with pytest.raises(FileExistsError):
        run_combined_ab(
            phase_a_run_dir=phase_a_dir,
            phase_b_run_dir=phase_b_dir,
            output_root=tmp_path / "output",
            command_line=["test"],
        )


def test_run_combined_ab_rejects_tampered_phase_b_run(tmp_path: Path, config) -> None:
    phase_a_dir = _fake_phase_a_run(tmp_path, config)
    phase_b_dir = _fake_phase_b_run(tmp_path, config, ready_rows=_synthetic_phase_b_ready_rows())
    frame = pl.read_parquet(phase_b_dir / "core" / "phase_b_primary_hypotheses.parquet")
    frame = frame.with_columns(pl.lit(-1.0).alias("ic_mean"))
    frame.write_parquet(phase_b_dir / "core" / "phase_b_primary_hypotheses.parquet")

    with pytest.raises(ValueError, match="Phase B run .* content hash mismatch"):
        run_combined_ab(
            phase_a_run_dir=phase_a_dir,
            phase_b_run_dir=phase_b_dir,
            output_root=tmp_path / "output",
            command_line=["test"],
        )


def test_run_combined_ab_copies_rank_correlation_file_when_present(tmp_path: Path, config) -> None:
    """§7.1: phase=B and phase=AB both carry
    ``primary_feature_rank_correlation.parquet`` — copied verbatim, not
    recomputed."""
    phase_a_dir = _fake_phase_a_run(tmp_path, config)
    rank_rows = [
        {
            "family_a": "px_reversal_5d",
            "feature_a": "px_reversal_5d",
            "family_b": "fin_log_mcap",
            "feature_b": "fin_log_mcap",
            "n_dates": 100,
            "mean_rank_corr": 0.05,
            "std_rank_corr": 0.1,
            "min_rank_corr": -0.2,
            "max_rank_corr": 0.3,
        }
    ]
    phase_b_dir = _fake_phase_b_run(
        tmp_path,
        config,
        ready_rows=_synthetic_phase_b_ready_rows(),
        rank_correlation_rows=rank_rows,
    )

    published = run_combined_ab(
        phase_a_run_dir=phase_a_dir,
        phase_b_run_dir=phase_b_dir,
        output_root=tmp_path / "output",
        command_line=["test"],
    )

    copied = pl.read_parquet(published / "primary_feature_rank_correlation.parquet")
    assert copied.height == 1
    assert copied["family_b"].to_list() == ["fin_log_mcap"]


def test_run_combined_ab_skips_rank_correlation_file_when_absent(tmp_path: Path, config) -> None:
    phase_a_dir = _fake_phase_a_run(tmp_path, config)
    phase_b_dir = _fake_phase_b_run(tmp_path, config, ready_rows=_synthetic_phase_b_ready_rows())

    published = run_combined_ab(
        phase_a_run_dir=phase_a_dir,
        phase_b_run_dir=phase_b_dir,
        output_root=tmp_path / "output",
        command_line=["test"],
    )

    assert not (published / "primary_feature_rank_correlation.parquet").exists()


def test_run_combined_ab_computes_permutation_p_value_when_summary_present(
    tmp_path: Path, config
) -> None:
    """§6 B-8 결합 단면 permutation — run_combined_ab must do the real-vs-null
    comparison as a pure read of run_phase_b_core's already-persisted
    ``permutation_summary.parquet``, never recomputing it."""
    phase_a_dir = _fake_phase_a_run(tmp_path, config)
    ready_rows = _synthetic_phase_b_ready_rows()
    phase_b_dir = _fake_phase_b_run(
        tmp_path,
        config,
        ready_rows=ready_rows,
        # 10 null replicates with a high, known discovery count so the real
        # (much smaller, from the combined A+B fixture) count is clearly
        # below every one of them -> p_empirical_count == 1.0 exactly.
        permutation_summary_rows=[
            {"replicate": i, "seed": i, "n_discoveries": 999} for i in range(10)
        ],
    )

    published = run_combined_ab(
        phase_a_run_dir=phase_a_dir,
        phase_b_run_dir=phase_b_dir,
        output_root=tmp_path / "output",
        command_line=["test"],
    )

    combined = pl.read_parquet(published / "combined_ab_primary_hypotheses.parquet")
    real_discovery_count = int(combined["primary_discovery_ab"].fill_null(False).sum())

    manifest = json.loads((published / "manifest.json").read_text())
    summary = manifest["combined_cross_sectional_permutation"]
    assert summary["n_replicates"] == 10
    assert summary["real_discovery_count"] == real_discovery_count
    # every null replicate (999) is >= the real count -> p = (1+10)/(10+1) = 1.0
    assert summary["p_empirical_count"] == pytest.approx(1.0)


def test_run_combined_ab_omits_permutation_field_when_summary_absent(
    tmp_path: Path, config
) -> None:
    phase_a_dir = _fake_phase_a_run(tmp_path, config)
    phase_b_dir = _fake_phase_b_run(tmp_path, config, ready_rows=_synthetic_phase_b_ready_rows())

    published = run_combined_ab(
        phase_a_run_dir=phase_a_dir,
        phase_b_run_dir=phase_b_dir,
        output_root=tmp_path / "output",
        command_line=["test"],
    )

    manifest = json.loads((published / "manifest.json").read_text())
    assert "combined_cross_sectional_permutation" not in manifest


# --- write_phase_b_quality_diagnostics: §7.1 Stage 2 artifacts ---


def _con_with_quality_inputs() -> duckdb.DuckDBPyConnection:
    """A lake holding only the two raw tables the raw-side diagnostics read."""
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE dart_filing_receipt_raw ("
        "corp_code VARCHAR, ticker VARCHAR, report_nm VARCHAR, rcept_no VARCHAR, "
        "rcept_dt DATE, rm VARCHAR)"
    )
    con.execute(
        "INSERT INTO dart_filing_receipt_raw VALUES "
        "('00126380','005930','사업보고서 (2023.12)','20240310000001',DATE '2024-03-10','')"
    )
    return con


def test_quality_diagnostics_skip_artifacts_whose_inputs_are_absent(tmp_path: Path) -> None:
    con = _con_with_quality_inputs()

    written = phase_b_run.write_phase_b_quality_diagnostics(
        con, tmp_path, available_assets={"dart_filing_receipt_raw"}
    )

    # A missing input leaves no file at all — an absent artifact is a visible
    # gap, not an empty table that reads as "measured, found nothing".
    assert written == ["filing_receipt_quality"]
    assert (tmp_path / "filing_receipt_quality.parquet").is_file()
    assert not (tmp_path / "capital_change_quality.parquet").exists()
    assert not (tmp_path / "feature_coverage.parquet").exists()


def test_quality_diagnostics_are_written_even_when_the_result_is_empty(tmp_path: Path) -> None:
    con = _con_with_quality_inputs()
    con.execute("DELETE FROM dart_filing_receipt_raw")

    written = phase_b_run.write_phase_b_quality_diagnostics(
        con, tmp_path, available_assets={"dart_filing_receipt_raw"}
    )

    # Present-but-empty is a different fact from absent, and the schema still
    # has to land so a downstream reader does not have to special-case it.
    assert written == ["filing_receipt_quality"]
    frame = pl.read_parquet(tmp_path / "filing_receipt_quality.parquet")
    assert frame.height == 0
    assert "periodic_amendment_receipts" in frame.columns


def test_feature_coverage_degrades_to_the_daily_mart_that_exists(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE feat_fin_scan_daily ("
        "trade_date DATE, ticker VARCHAR, market VARCHAR, "
        "fin_log_mcap DOUBLE, fin_log_mcap_lag1 DOUBLE, "
        "fin_book_to_market DOUBLE, fin_earnings_yield DOUBLE, fin_cfo_yield DOUBLE, "
        "fin_sales_to_price DOUBLE, value_component_count INTEGER, "
        "fin_value_z DOUBLE, fin_value_z_lag1 DOUBLE, "
        "fin_gross_profitability DOUBLE, fin_gross_profitability_lag1 DOUBLE, "
        "fin_operating_profitability DOUBLE, fin_operating_profitability_lag1 DOUBLE, "
        "fin_asset_growth_yoy DOUBLE, fin_asset_growth_yoy_lag1 DOUBLE, "
        "fin_accruals_to_assets DOUBLE, fin_accruals_to_assets_lag1 DOUBLE, "
        "value_fin_age_days BIGINT, profitability_fin_age_days BIGINT, "
        "asset_growth_fin_age_days BIGINT, accruals_fin_age_days BIGINT)"
    )
    con.execute(
        "INSERT INTO feat_fin_scan_daily VALUES "
        "(DATE '2024-03-11','005930','KOSPI',1,1,0.5,0.1,0.2,0.3,4,1,1,"
        "0.4,0.4,0.3,0.3,0.05,0.05,-0.02,-0.02,40,40,40,40)"
    )

    # feat_event_scan_daily never materialized — the union must not reference
    # it rather than the whole artifact being dropped.
    written = phase_b_run.write_phase_b_quality_diagnostics(
        con, tmp_path, available_assets={"feat_fin_scan_daily"}
    )

    assert written == ["feature_coverage"]
    frame = pl.read_parquet(tmp_path / "feature_coverage.parquet")
    assert set(frame["source_mart"].unique()) == {"feat_fin_scan_daily"}
    assert "ev_net_share_issuance_yoy" not in set(frame["feature"])


# --- write_phase_b_family_cards: §7.1 Stage 4 artifacts ---


def test_family_cards_read_coverage_back_off_the_parquet_just_written(
    tmp_path: Path, config
) -> None:
    from research.analysis.horizon_scan_phase_b import build_phase_b_readiness_rows

    pl.DataFrame(
        [
            {
                "feature": "fin_value_z",
                "variant": "native_t",
                "market": "KOSPI",
                "year": 2016,
                "panel_rows": 200,
                "nonnull_rows": 150,
                "first_value_date": date(2016, 3, 2),
                "min_names_per_date": 11,
            }
        ]
    ).write_parquet(tmp_path / "feature_coverage.parquet")

    rows = phase_b_run.write_phase_b_family_cards(
        config,
        tmp_path,
        readiness_rows=build_phase_b_readiness_rows(config, available_assets=set()),
        assembled_rows=[],
        rank_correlation_rows=[],
        diagnostics_written=["feature_coverage"],
        run_id="20260812T090000-abcd1234",
    )

    by_family = {row["family"]: row for row in rows}
    assert by_family["fin_value_z"]["coverage_ratio"] == 0.75
    assert by_family["fin_value_z"]["effective_start"] == date(2016, 3, 2)
    # event_coverage was not written, so the SUE family gets no coverage at all
    # rather than borrowing the continuous table.
    assert by_family["fin_sue"]["coverage_source"] is None

    assert (tmp_path / "family_summary.parquet").is_file()
    markdown = (tmp_path / "family_cards.md").read_text()
    assert "## fin_value_z" in markdown
    assert "20260812T090000-abcd1234" in markdown


def test_family_cards_are_written_when_no_diagnostic_landed(tmp_path: Path, config) -> None:
    from research.analysis.horizon_scan_phase_b import build_phase_b_readiness_rows

    rows = phase_b_run.write_phase_b_family_cards(
        config,
        tmp_path,
        readiness_rows=build_phase_b_readiness_rows(config, available_assets=set()),
        assembled_rows=[],
        rank_correlation_rows=[],
        diagnostics_written=[],
    )

    # The card that says "blocked, here is the missing dependency" is exactly
    # the one a run with nothing collected needs to produce.
    assert len(rows) == 8
    assert all(row["readiness"] == "blocked" for row in rows)
    assert (tmp_path / "family_cards.md").is_file()


# --- B-10 Stage 5: 03b report is required, and does not enter the hash ---


def test_the_phase_b_report_is_excluded_from_the_reproducibility_hash(tmp_path: Path) -> None:
    from research.analysis.horizon_scan_run_spec import compute_run_content_hash

    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text("{}")
    before = compute_run_content_hash(run_dir, exclude_names=PHASE_B_CONTENT_HASH_EXCLUDE_NAMES)

    # The report carries this run's timestamps, so two byte-identical scans
    # would otherwise hash differently.
    (run_dir / phase_b_run.PHASE_B_REPORT_NAME).write_text("# report\n\nstarted 12:00\n")
    after = compute_run_content_hash(run_dir, exclude_names=PHASE_B_CONTENT_HASH_EXCLUDE_NAMES)

    assert before == after


def test_publish_refuses_a_phase_b_run_without_its_report(tmp_path: Path) -> None:
    from research.analysis.horizon_scan_run_spec import publish_run

    tmp_run_dir = tmp_path / "run.tmp"
    tmp_run_dir.mkdir()
    (tmp_run_dir / "phase_b_run_spec.json").write_text("{}")
    (tmp_run_dir / "manifest.json").write_text("{}")

    required = ("phase_b_run_spec.json", "manifest.json", phase_b_run.PHASE_B_REPORT_NAME)
    # A run that published without the report would look complete to
    # _SUCCESS.json while giving a human nothing to read.
    with pytest.raises(RuntimeError, match=phase_b_run.PHASE_B_REPORT_NAME):
        publish_run(
            tmp_run_dir,
            tmp_path / "run",
            run_spec={"run_id": "x"},
            required_artifacts=required,
            content_hash_exclude_names=PHASE_B_CONTENT_HASH_EXCLUDE_NAMES,
        )
