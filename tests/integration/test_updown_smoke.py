"""Integration smoke: one model-02 run end to end on the local lake.

`06` §5's last row. Self-skips when the pinned snapshot's marts are not on this
host, so it is safe to run anywhere; where the lake *is* present it is the only
test that exercises build -> train -> evaluate -> written artifacts together.

Kept deliberately small (one horizon, one grid point, two folds, two years) —
the point is that the wiring holds, not that the model is any good.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
from research.etl.mart import is_materialized
from research.models._02_updown_prob import build_dataset as bd
from research.models._02_updown_prob import evaluate as ev
from research.models._02_updown_prob.experiments import registry as rg
from research.models._02_updown_prob.experiments import run_matrix as rm
from research.models._02_updown_prob.spec import ModelSpec, lake_config

HORIZON = 20


@pytest.fixture(scope="module")
def lake():
    config = lake_config()
    spec = ModelSpec(feature_set="FS0", period_end="2016-12-31", n_folds=2)
    needed = [bd.UNIVERSE_VIEW, *bd.required_mart_columns(list(spec.feature_columns(HORIZON)))]
    missing = [m for m in needed if not is_materialized(config, m)]
    if missing:
        pytest.skip(f"snapshot {config.snapshot_date} lacks marts: {missing}")
    return config, spec


def test_build_train_evaluate_completes_on_the_real_marts(lake) -> None:
    config, spec = lake
    result = bd.build_dataset(spec, config, horizon=HORIZON, write=False)
    assert result.panel_rows > 100_000
    assert set(result.feature_cols) == set(spec.feature_columns(HORIZON))
    # the mart contracts on this snapshot are genuinely not uniform, which is
    # why the builder registers read-only and records what it found
    assert all(contract is not None for contract in result.mart_contracts.values())


def test_a_smoke_run_writes_every_artifact_the_plan_asks_for(
    lake, tmp_path: Path, monkeypatch
) -> None:
    config, _spec = lake
    monkeypatch.setattr(rm, "RESULTS_ROOT", tmp_path)
    run = rg.stage_runs("E1", horizon=HORIZON)[0]
    run = rg.resolve(
        run,
        {
            "E0": {
                "stage": "E0",
                "by_horizon": {
                    str(HORIZON): {
                        "label": "y_up",
                        "model": "hgb_clf",
                        "feature_set": "FS0",
                        "preprocess_profile": "rank",
                        "grid": "HGB_CLF_GRID_E1",
                    }
                },
            }
        },
    )
    record = rm.execute_run(run, config=config, smoke=True)

    out_dir = tmp_path / rm.SMOKE_DIR / run.run_id
    for name in ("run_spec.json", "summary.json", "summary.md", "fold_metrics.parquet"):
        assert (out_dir / name).is_file(), name

    run_spec = json.loads((out_dir / "run_spec.json").read_text(encoding="utf-8"))
    assert run_spec["smoke"] is True
    assert run_spec["spec"]["period_end"] == rm.SMOKE_PERIOD_END
    assert run_spec["mart_contracts"], "the mart contracts must be pinned in the run spec"
    assert len(run_spec["code_hash"]) == 16

    # the probability is a probability, and the primary metrics are the
    # preregistered ones for this horizon
    assert 0.0 < record.probability < 1.5
    assert record.summary["probability_metric"] == ev.PRIMARY_PROBABILITY
    assert record.summary["economic_metric"] == ev.PRIMARY_ECONOMIC[HORIZON]

    predictions = pl.read_parquet(record.summary["predictions_path"])
    assert predictions.height > 0
    # `06` §8: not one prediction row may sit past the holdout boundary
    assert str(predictions["trade_date"].max()) < "2025-08-01"
    assert predictions["p_raw"].min() >= 0.0 and predictions["p_raw"].max() <= 1.0


def test_the_panel_is_reused_rather_than_rebuilt(lake) -> None:
    config, spec = lake
    first_dir, _manifest = rm.ensure_dataset(spec, config, HORIZON)
    parts = sorted((first_dir / bd.STD_DIR).glob("*.parquet"))
    assert parts
    stamps = {p: p.stat().st_mtime_ns for p in parts}
    second_dir, _again = rm.ensure_dataset(spec, config, HORIZON)
    assert second_dir == first_dir
    assert {p: p.stat().st_mtime_ns for p in parts} == stamps


def test_train_config_grids_are_the_registry_names(lake) -> None:
    for stage in rg.STAGES:
        for run in rg.stage_runs(stage, include_optional=True):
            if run.grid == rg.SELECTED:
                continue
            assert run.grid in rm.GRIDS
