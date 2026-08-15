"""Integration smoke test for Phase A0 orchestration against the real lake.

Self-skips when no complete ``sj2_remote`` raw snapshot is available. Runs
``build_a0_inputs`` end-to-end (materialization is idempotent/skip-if-present,
so a repeat run only recomputes the manifest/readiness) and checks the
structural invariants the A0 plan (§A0-9, §6.3) requires before Phase A can
start: all 7 marts materialize with rows, keys are unique, tradable is a
subset of broad, and the manifest carries the expected fields.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from research.etl.config import REMOTE_SOURCE, EngineOptions, LakeConfig
from research.etl.horizon_scan_inputs import REQUIRED_RAW_INPUTS, build_a0_inputs
from research.etl.lake import connect
from research.etl.mart import register_mart_view
from research.etl.snapshot import resolve_config
from research.etl.universe import assert_tradable_subset

EXPECTED_MARTS = {
    "dim_stock_pit_daily",
    "dim_price_quality_daily",
    "dim_universe_broad_daily",
    "dim_universe_tradable_daily",
    "feat_price",
    "feat_flow",
    "label_scan",
}


@pytest.fixture(scope="module")
def a0_manifest() -> tuple[dict, LakeConfig]:
    base = LakeConfig(source=REMOTE_SOURCE, engine=EngineOptions(threads=4, memory_limit="4GB"))
    try:
        resolve_config(base, required_inputs=REQUIRED_RAW_INPUTS)
    except FileNotFoundError as exc:
        pytest.skip(f"no complete {REMOTE_SOURCE!r} raw snapshot available: {exc}")
    target = build_a0_inputs(config=base)
    payload = json.loads(target.read_text(encoding="utf-8"))
    lake = LakeConfig(
        source=REMOTE_SOURCE,
        snapshot_date=payload["snapshot_date"],
        analysis_config_hash=payload["config_hash"],
    )
    return payload, lake


def test_manifest_reports_success(a0_manifest) -> None:
    payload, _ = a0_manifest
    assert payload["status"] == "success"
    assert payload["source"] == REMOTE_SOURCE
    assert isinstance(payload["smoke_only"], bool)
    assert {m["view"] for m in payload["marts"]} == EXPECTED_MARTS


def test_all_marts_materialized_with_rows(a0_manifest) -> None:
    payload, _ = a0_manifest
    for mart in payload["marts"]:
        assert mart["row_count"] > 0, mart["view"]


def test_key_uniqueness_across_marts(a0_manifest) -> None:
    _, lake = a0_manifest
    con = connect(lake)
    marts = (
        "dim_stock_pit_daily",
        "dim_price_quality_daily",
        "feat_price",
        "feat_flow",
        "label_scan",
    )
    for view in marts:
        register_mart_view(con, lake, view)
        total, distinct = con.execute(
            f"SELECT count(*), count(DISTINCT (trade_date, ticker, market)) FROM {view}"
        ).fetchone()
        assert total == distinct, view


def test_tradable_universe_is_subset_of_broad(a0_manifest) -> None:
    _, lake = a0_manifest
    con = connect(lake)
    register_mart_view(con, lake, "dim_universe_broad_daily")
    register_mart_view(con, lake, "dim_universe_tradable_daily")
    assert_tradable_subset(con)


def test_feat_price_has_no_infinite_values(a0_manifest) -> None:
    _, lake = a0_manifest
    con = connect(lake)
    register_mart_view(con, lake, "feat_price")
    numeric_cols = [
        row[0]
        for row in con.execute("DESCRIBE feat_price").fetchall()
        if row[1] in ("DOUBLE", "FLOAT")
    ]
    checks = " + ".join(f"isinf({col})::INT" for col in numeric_cols)
    (n_inf,) = con.execute(f"SELECT sum({checks}) FROM feat_price").fetchone()
    assert not n_inf


def test_readiness_matrix_written(a0_manifest) -> None:
    payload, _ = a0_manifest
    assert Path(payload["readiness_matrix_parquet"]).is_file()
    assert Path(payload["readiness_matrix_markdown"]).is_file()


def test_rerun_is_idempotent(a0_manifest) -> None:
    payload, lake = a0_manifest
    target = build_a0_inputs(config=LakeConfig(source=REMOTE_SOURCE))
    repeat_payload = json.loads(target.read_text(encoding="utf-8"))
    assert repeat_payload["config_hash"] == payload["config_hash"]
    assert repeat_payload["snapshot_date"] == payload["snapshot_date"]
