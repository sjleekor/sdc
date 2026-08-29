from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("plotly")

from research.analysis.feature_performance_report import (
    EXTERNAL_ASSET_RE,
    FEATURE_GUIDES,
    generate_report,
    load_report_bundle,
)

ROOT = Path(__file__).resolve().parents[2]
SNAPSHOT_DATE = "2026-08-23"
SOURCE = "sj2_remote"
CONFIG_HASH = "889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB_RUN_ID = "20260828T165038-4e0ae8b0"
AB_DIR = (
    ROOT
    / "research/output/horizon_scan"
    / "phase=AB"
    / f"snapshot_date={SNAPSHOT_DATE}"
    / f"source={SOURCE}"
    / f"config_hash={CONFIG_HASH}"
    / f"run_id={AB_RUN_ID}"
)


def _bundle():
    if not (AB_DIR / "_SUCCESS.json").exists():
        pytest.skip("canonical local horizon-scan artifacts are not available")
    target = ROOT / "docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json"
    if not target.exists():
        pytest.skip("local model validation artifacts are not available")
    return load_report_bundle(
        ROOT,
        snapshot_date=SNAPSHOT_DATE,
        source=SOURCE,
        config_hash=CONFIG_HASH,
        ab_run_id=AB_RUN_ID,
    )


def test_canonical_report_kpis() -> None:
    kpis = _bundle().kpis
    assert kpis["family_count"] == 35
    assert kpis["a_primary_cells"] == 75
    assert kpis["a_exploratory_cells"] == 28
    assert kpis["b_ready_cells"] == 78
    assert kpis["ab_hypotheses"] == 153
    assert kpis["ab_valid"] == 147
    assert kpis["ab_insufficient"] == 6
    assert kpis["ab_discoveries"] == 87
    assert kpis["phase_b_screen_pass"] == 40
    assert kpis["a_grade_counts"] == {"A": 6, "C": 4, "D": 6, "R": 1}
    assert kpis["phase_b_grade_counts"] == {"A": 23, "B": 17, "C": 35, "D": 3}
    assert kpis["p_empirical_count"] == pytest.approx(0.009900990099009901)
    assert kpis["phase_a_discovery_changes"] == 0
    assert kpis["high_correlation_pairs"] == 2
    assert kpis["t1_decile_h20_delta"] == pytest.approx(-0.002490663366409782)
    assert kpis["t1_topk_h20_delta"] == pytest.approx(-0.004541910085757397)
    assert kpis["t2_status"] == "improved_all_horizons"


def test_report_is_self_contained_and_under_size_limit(tmp_path: Path) -> None:
    index_path, manifest_path = generate_report(_bundle(), tmp_path)
    report = index_path.read_text(encoding="utf-8")
    manifest_text = manifest_path.read_text(encoding="utf-8")
    manifest = json.loads(manifest_text)

    assert index_path.stat().st_size < 10 * 1024 * 1024
    assert report.count("plotly.js v") == 1
    assert EXTERNAL_ASSET_RE.search(report) is None
    assert "/Users/" not in report
    assert "/Users/" not in manifest_text
    assert "FINAL HOLDOUT PENDING" in report
    assert len(FEATURE_GUIDES) == 35
    assert 'data-testid="feature-guide"' in report
    assert report.count("data-testid='feature-guide-card'") == 35
    assert report.count("class='chart-note'") == 7
    assert 'data-testid="family-table"' in report
    assert report.count("class='family-row'") == 35
    assert manifest["row_counts"]["family_table"] == 35
    assert manifest["runs"]["AB"]["run_id"] == AB_RUN_ID
