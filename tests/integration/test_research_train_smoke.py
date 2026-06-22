"""Integration smoke test for P6 — build + Ridge walk-forward on the real lake.

Self-skips when the raw lake is absent. Builds a short-window dataset to a temp
dir, runs the walk-forward, and asserts: a Rank IC report is produced, and (the
key guard) shuffling the label within each (fold, date) collapses the IC toward
zero — i.e. there is no structural label leakage (etl_00 §5 embargo/purge works).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest
from research.etl.config import EngineOptions, LakeConfig
from research.models._01_20_access_return_rank import build_dataset as bd
from research.models._01_20_access_return_rank import train as tr
from research.models._01_20_access_return_rank.spec import ModelSpec


@pytest.fixture(scope="module")
def std_panel(tmp_path_factory):
    probe = LakeConfig()
    if not probe.raw_root.exists():
        pytest.skip(f"raw lake not present at {probe.raw_root}")
    tmp = tmp_path_factory.mktemp("ds")
    cfg = LakeConfig(datasets_root=Path(tmp), engine=EngineOptions(threads=4, memory_limit="4GB"))
    spec = ModelSpec(period_start="2023-01-01", period_end="2024-12-31", n_folds=3)
    res = bd.build_dataset(spec, cfg, created_at="x", write=True)
    return pl.read_parquet(res.dataset_dir / "feat_panel_std.parquet")


def test_walk_forward_produces_rank_ic(std_panel) -> None:
    result = tr.walk_forward(std_panel, tr.TrainConfig(model="ridge"))
    assert result.fold_results
    assert "alpha" in result.best_params
    # IC should be finite and bounded in [-1, 1].
    assert -1.0 <= result.mean_rank_ic <= 1.0
    for fr in result.fold_results:
        assert fr.report.n_dates > 0


def test_label_shuffle_collapses_ic(std_panel) -> None:
    """No structural leakage: shuffling labels within (fold, date) kills the IC."""
    real = tr.walk_forward(std_panel, tr.TrainConfig(model="ridge"))

    rng = np.random.default_rng(0)
    parts = []
    for _keys, g in std_panel.group_by(["fold_id", "fold_role", "trade_date"], maintain_order=True):
        y = g["y_rank_20d"].to_numpy().copy()
        rng.shuffle(y)
        parts.append(g.with_columns(pl.Series("y_rank_20d", y)))
    shuffled = pl.concat(parts)
    shuf = tr.walk_forward(shuffled, tr.TrainConfig(model="ridge"))

    # Real signal clearly above shuffled; shuffled near zero.
    assert real.mean_rank_ic > shuf.mean_rank_ic
    assert abs(shuf.mean_rank_ic) < 0.08
