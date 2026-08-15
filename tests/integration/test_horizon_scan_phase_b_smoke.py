"""Integration smoke test for Phase B core scan + A+B combined BH wiring
against the real local lake (``08_phase_b_implementation_log.md`` §3 remaining
work item 3: "Phase A 75개 + Phase B ready 개를 실제로 결합해 q_fdr_global_ab
산출").

Self-skips when no complete ``sj2_remote`` raw snapshot / A0 manifest is
available, matching ``test_horizon_scan_inputs_smoke.py``'s convention.

Does **not** assert a specific ``m_b_ready`` count. The two brand-new raw
tables this session added (``dart_filing_receipt_raw``, ``dart_capital_change_raw``)
have not been collected from prod yet, so every Phase B candidate cell is
expected to freeze ``blocked_exploratory`` today (``m_b_ready == 0``) — this
is correct outcome-blind behavior (§B-0), not a limitation of this test, and
will change to some cells ``ready_primary`` with zero code changes once prod
collects those two raw tables. What this test verifies instead is internal
consistency: the readiness freeze is well-formed and immutable, the published
run's artifacts are structurally sound, and ``len(assembled) == 38`` always
with ``m_b_ready`` of them ``ready_primary``.

Also exercises §2.3 rule 5's integrity checks against the one real published
Phase A run currently on disk, without assuming whether it happens to be
config-fresh (built from the exact current ``horizon_scan_config.yaml``) or
config-stale (predates this session's Phase B additions to that same file,
which is what's actually the case today — see
``horizon_scan_phase_b_run.py``'s module docstring) — either outcome is
correct; only an unexpected exception type would indicate a real bug.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
from research.analysis.horizon_scan_config import CONFIG_PATH, load_config
from research.analysis.horizon_scan_phase_b_robustness import select_phase_b_long_horizon_cells
from research.analysis.horizon_scan_phase_b_run import (
    load_phase_a_primary_rows,
    run_combined_ab,
    run_phase_b_core,
)
from research.etl.config import REMOTE_SOURCE, LakeConfig
from research.etl.snapshot import resolve_config

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def phase_b_run(tmp_path_factory: pytest.TempPathFactory) -> Path:
    config = load_config(CONFIG_PATH)
    base = LakeConfig(source=REMOTE_SOURCE)
    required_raw = list(config.raw["phase_b"]["required_raw_tables"])
    try:
        resolve_config(base, required_inputs=required_raw)
    except FileNotFoundError as exc:
        pytest.skip(f"no complete {REMOTE_SOURCE!r} raw snapshot available: {exc}")

    output_root = tmp_path_factory.mktemp("phase_b_smoke_output")
    try:
        return run_phase_b_core(
            source=REMOTE_SOURCE,
            output_root=output_root,
            command_line=["pytest-smoke"],
        )
    except FileNotFoundError as exc:
        pytest.skip(f"A0 manifest not available for a Phase B scan: {exc}")


def test_publish_writes_required_artifacts(phase_b_run: Path) -> None:
    for name in (
        "phase_b_run_spec.json",
        "phase_b_readiness_freeze.json",
        "manifest.json",
        "_SUCCESS.json",
    ):
        assert (phase_b_run / name).is_file(), f"missing {name}"


def test_readiness_matrix_always_published(phase_b_run: Path) -> None:
    """Unlike every other §7.1 diagnostic artifact this session added,
    ``readiness_matrix.{parquet,md}`` (§6 B-10 Stage 1) is a rendering of the
    full 38-candidate freeze itself — it's written unconditionally, ready
    cells or not."""
    freeze = json.loads((phase_b_run / "phase_b_readiness_freeze.json").read_text())
    matrix_path = phase_b_run / "core" / "readiness_matrix.parquet"
    assert matrix_path.is_file()
    table = pl.read_parquet(matrix_path)
    assert table.height == freeze["max_candidates"] == 38

    md_path = phase_b_run / "core" / "readiness_matrix.md"
    assert md_path.is_file()
    md = md_path.read_text()
    for cell in freeze["cells"]:
        assert cell["family"] in md


def test_robustness_summary_files_match_ready_cell_state(phase_b_run: Path) -> None:
    """§6 B-10 Stage 1's 4 ``*_summary.parquet`` files persist the full
    per-cell rows ``compute_phase_b_gate_updates`` already computes for the
    screen_pass robustness rules (7/8) — nonoverlap/temporal placebo only for
    the ``nw_lag>=59`` long-horizon subset of ready continuous cells, issuer/
    filing-cycle bootstrap for any ready SUE cell. Today's expected state
    (``m_b_ready == 0`` — see module docstring) is every file absent."""
    freeze = json.loads((phase_b_run / "phase_b_readiness_freeze.json").read_text())
    config = load_config(CONFIG_PATH)
    ready_continuous = [
        c
        for c in freeze["cells"]
        if c["role"] == "ready_primary" and c["cell_type"] in ("cumulative", "bucket")
    ]
    ready_events = [
        c
        for c in freeze["cells"]
        if c["role"] == "ready_primary" and c["cell_type"] == "event_bucket"
    ]
    long_cells = select_phase_b_long_horizon_cells(
        ready_continuous, min_nw_lag=int(config.raw["placebo"]["temporal_min_nw_lag"])
    )

    core_dir = phase_b_run / "core"
    for name, expected in (
        ("nonoverlap_summary.parquet", bool(long_cells)),
        ("temporal_placebo_summary.parquet", bool(long_cells)),
        ("issuer_bootstrap_summary.parquet", bool(ready_events)),
        ("filing_cycle_bootstrap_summary.parquet", bool(ready_events)),
    ):
        assert (core_dir / name).is_file() == expected, name


def test_readiness_freeze_is_internally_consistent(phase_b_run: Path) -> None:
    freeze = json.loads((phase_b_run / "phase_b_readiness_freeze.json").read_text())
    assert freeze["max_candidates"] == 38
    assert freeze["m_b_ready"] + freeze["blocked_exploratory_count"] == 38
    assert (
        freeze["combined_ab_hypothesis_count"]
        == freeze["phase_a_primary_count"] + freeze["m_b_ready"]
    )
    if freeze["m_b_ready"] == 0:
        # today's expected state (see module docstring) — not a hardcoded
        # requirement, just documenting why an all-blocked freeze is correct.
        assert all(c["role"] == "blocked_exploratory" for c in freeze["cells"])
    else:
        assert any(c["role"] == "ready_primary" for c in freeze["cells"])


def test_phase_b_primary_hypotheses_row_count_matches_freeze(phase_b_run: Path) -> None:
    freeze = json.loads((phase_b_run / "phase_b_readiness_freeze.json").read_text())
    table = pl.read_parquet(phase_b_run / "core" / "phase_b_primary_hypotheses.parquet")
    assert table.height == 38
    assert int((table["role"] == "ready_primary").sum()) == freeze["m_b_ready"]


def test_rank_correlation_file_matches_ready_continuous_state(phase_b_run: Path) -> None:
    """``primary_feature_rank_correlation.parquet`` is only written once at
    least one Phase B continuous cell is ready (today, none are — see module
    docstring) — its presence must track that, not be unconditional."""
    freeze = json.loads((phase_b_run / "phase_b_readiness_freeze.json").read_text())
    ready_continuous = any(
        c["role"] == "ready_primary" and c["cell_type"] in ("cumulative", "bucket")
        for c in freeze["cells"]
    )
    rank_corr_path = phase_b_run / "core" / "primary_feature_rank_correlation.parquet"
    if ready_continuous:
        assert rank_corr_path.is_file()
    else:
        assert not rank_corr_path.exists()


def test_permutation_summary_file_matches_any_ready_cell_state(phase_b_run: Path) -> None:
    """``core/permutation_summary.parquet`` (§6 B-8 결합 단면 permutation null
    distribution) is only written once at least one Phase B cell — continuous
    or SUE — is ready (today, none are — see module docstring); its presence
    must track that, same convention as
    ``test_rank_correlation_file_matches_ready_continuous_state``."""
    freeze = json.loads((phase_b_run / "phase_b_readiness_freeze.json").read_text())
    any_ready = any(c["role"] == "ready_primary" for c in freeze["cells"])
    permutation_path = phase_b_run / "core" / "permutation_summary.parquet"
    if any_ready:
        assert permutation_path.is_file()
    else:
        assert not permutation_path.exists()


def test_content_hash_is_reproducible(phase_b_run: Path) -> None:
    from research.analysis.horizon_scan_phase_b import PHASE_B_CONTENT_HASH_EXCLUDE_NAMES
    from research.analysis.horizon_scan_run_spec import compute_run_content_hash

    success = json.loads((phase_b_run / "_SUCCESS.json").read_text())
    recomputed = compute_run_content_hash(
        phase_b_run, exclude_names=PHASE_B_CONTENT_HASH_EXCLUDE_NAMES
    )
    assert recomputed == success["content_hash"]


def test_load_phase_a_primary_rows_against_real_published_run() -> None:
    """Proves the §2.3 rule 5 integrity checks run cleanly against a real
    artifact — whichever branch applies today (accept 75 rows, or refuse a
    config-stale run) is correct; anything else is a bug."""
    candidates = sorted(
        Path(f"{REPO_ROOT}/research/output/horizon_scan/phase=A").glob("*/*/*/run_id=*")
    )
    if not candidates:
        pytest.skip("no published Phase A run available on disk")
    run_dir = candidates[-1]
    config = load_config(CONFIG_PATH)
    try:
        rows = load_phase_a_primary_rows(run_dir, config)
    except ValueError as exc:
        assert "config_hash" in str(exc) or "content hash" in str(exc) or "match" in str(exc)
        return
    assert len(rows) == 75


def test_run_combined_ab_against_real_published_phase_a_run(
    phase_b_run: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """Same "whichever branch is correct today" shape as the test above, but
    through the full ``run_combined_ab`` entry point — proves the new §9 B-9
    screen_pass wiring (``phase_b_screen_pass_count`` in the manifest) doesn't
    crash against a real (if today config-stale) Phase A run."""
    candidates = sorted(
        Path(f"{REPO_ROOT}/research/output/horizon_scan/phase=A").glob("*/*/*/run_id=*")
    )
    if not candidates:
        pytest.skip("no published Phase A run available on disk")

    output_root = tmp_path_factory.mktemp("phase_ab_smoke_output")
    try:
        published = run_combined_ab(
            phase_a_run_dir=candidates[-1],
            phase_b_run_dir=phase_b_run,
            output_root=output_root,
            command_line=["pytest-smoke"],
        )
    except ValueError as exc:
        assert "config_hash" in str(exc) or "content hash" in str(exc) or "match" in str(exc)
        return

    manifest = json.loads((published / "manifest.json").read_text())
    assert "phase_b_screen_pass_count" in manifest
    assert manifest["phase_b_screen_pass_count"] >= 0
    assert set(manifest["phase_b_evidence_grade_counts"]) == {"A", "B", "C", "D"}

    source_rank_corr = phase_b_run / "core" / "primary_feature_rank_correlation.parquet"
    copied_rank_corr = published / "primary_feature_rank_correlation.parquet"
    assert copied_rank_corr.is_file() == source_rank_corr.is_file()

    permutation_summary_path = phase_b_run / "core" / "permutation_summary.parquet"
    if permutation_summary_path.is_file():
        combined_permutation = manifest["combined_cross_sectional_permutation"]
        assert combined_permutation["n_replicates"] > 0
        assert combined_permutation["real_discovery_count"] >= 0
        assert 0.0 <= combined_permutation["p_empirical_count"] <= 1.0
    else:
        assert "combined_cross_sectional_permutation" not in manifest
