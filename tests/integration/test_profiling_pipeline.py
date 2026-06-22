"""Integration test for the profiling pipeline.

Requires a reachable PostgreSQL instance with the pipeline schema; skipped
automatically when the DB is unreachable or a table is absent.  Exercises the
full runner → renderer → manifest path on small tables, plus the idempotency /
diff-zero regression contract.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from krx_collector.domain.profiling import RunManifest, SamplePolicy
from krx_collector.infra.config.settings import get_settings
from krx_collector.infra.db_postgres.connection import get_connection


@pytest.fixture()
def dsn() -> str:
    return get_settings().db_dsn


def _require_db(dsn: str) -> None:
    try:
        with get_connection(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Database not reachable: {exc}")


def _runner(dsn: str):
    from krx_collector.infra.db_postgres.profiling_query_runner import (
        PostgresProfileQueryRunner,
    )

    return PostgresProfileQueryRunner(
        dsn, target="local", sample_policy=SamplePolicy.FULL, query_timeout_sec=60
    )


def test_profile_small_table_end_to_end(dsn: str, tmp_path: Path) -> None:
    """metric_catalog → checks → JSON/MD artifacts, all checks clean."""
    _require_db(dsn)
    from krx_collector.adapters.profiling_render.artifact_renderer import ArtifactRenderer
    from krx_collector.adapters.profiling_render.markdown_renderer import MarkdownRenderer
    from krx_collector.service.profiling.catalog import get_spec
    from krx_collector.service.profiling.runner import build_profile

    runner = _runner(dsn)
    spec = get_spec("metric_catalog")
    if not runner.describe_schema(spec.table):
        pytest.skip("metric_catalog table absent")

    result = build_profile(spec, runner, target="local")
    assert result.preflight.exists
    # No warnings expected on a tiny, well-formed catalog table.
    assert result.warnings == []

    written = ArtifactRenderer().render(result, out_dir=tmp_path, formats=["json"])
    written += MarkdownRenderer().render(result, out_dir=tmp_path, formats=["md"])
    assert any(p.suffix == ".json" for p in written)
    data = json.loads((tmp_path / "artifacts" / "metric_catalog.stats.json").read_text())
    assert data["table"] == "metric_catalog"
    assert data["checks"]


def test_profile_run_is_idempotent_and_diff_zero(dsn: str, tmp_path: Path) -> None:
    """Re-profiling the same table twice yields a drift report with 0 changes."""
    _require_db(dsn)
    from krx_collector.adapters.profiling_render.serialize import manifest_to_dict
    from krx_collector.service.profiling.catalog import get_spec
    from krx_collector.service.profiling.diff import compare_manifests
    from krx_collector.service.profiling.runner import build_profile, update_manifest

    runner = _runner(dsn)
    spec = get_spec("sync_checkpoints")
    if not runner.describe_schema(spec.table):
        pytest.skip("sync_checkpoints table absent")

    manifests = []
    for run_id in ("run_a", "run_b"):
        result = build_profile(spec, runner, target="local")
        manifest = RunManifest(
            run_id=run_id,
            target="local",
            run_date="2026-06-19",
            generated_at=datetime(2026, 6, 19),
        )
        update_manifest(manifest, result)
        manifests.append(manifest_to_dict(manifest))

    report = compare_manifests(manifests[0], manifests[1], generated_at=datetime(2026, 6, 19))
    assert report.changed == [], [t.table for t in report.changed]


def test_empty_table_is_skipped(dsn: str) -> None:
    """operating_* tables (0 rows pre-load) record skipped:empty, not a crash."""
    _require_db(dsn)
    from krx_collector.service.profiling.catalog import get_spec
    from krx_collector.service.profiling.runner import build_profile

    runner = _runner(dsn)
    spec = get_spec("operating_metric_fact")
    if not runner.describe_schema(spec.table):
        pytest.skip("operating_metric_fact table absent")

    result = build_profile(spec, runner, target="local")
    # If data has since landed this assertion is intentionally relaxed.
    if result.row_count == 0:
        assert result.skipped_reason == "skipped: empty"
        assert result.checks == []
