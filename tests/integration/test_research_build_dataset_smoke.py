"""Integration smoke test for P5 — build_dataset end-to-end on the real lake.

Self-skips when the raw lake is absent. Uses a short period and writes to a temp
datasets root (never the repo's data/). Verifies artifacts, per-date z-score, the
NaN/Inf-free gate, manifest contents, and embargo in split_folds.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
from research.etl.config import EngineOptions, LakeConfig
from research.models._01_20_access_return_rank import build_dataset as bd
from research.models._01_20_access_return_rank.spec import ModelSpec


@pytest.fixture()
def built(tmp_path: Path):
    probe = LakeConfig()
    if not probe.raw_root.exists():
        pytest.skip(f"raw lake not present at {probe.raw_root}")
    cfg = LakeConfig(
        datasets_root=tmp_path,
        engine=EngineOptions(threads=4, memory_limit="4GB"),
    )
    spec = ModelSpec(period_start="2024-01-01", period_end="2024-12-31", n_folds=2)
    res = bd.build_dataset(spec, cfg, created_at="2026-06-20T00:00:00Z", write=True)
    return res, spec


def test_artifacts_written(built) -> None:
    res, _ = built
    names = {p.name for p in res.dataset_dir.iterdir()}
    assert {
        "feat_panel.parquet",
        "feat_panel_std.parquet",
        "label_daily.parquet",
        "split_folds.parquet",
        "dataset_manifest.json",
    } <= names
    assert res.panel_rows > 100_000  # ~529k for 2024 in-universe


def test_per_date_zscore_and_finite(built) -> None:
    res, _ = built
    std = pl.read_parquet(res.dataset_dir / "feat_panel_std.parquet")
    # no NaN/null/inf in a standardized feature column (L1 gate).
    col = std["px_ret_20d"]
    assert col.null_count() == 0
    assert int(col.is_nan().sum()) == 0
    assert int(col.is_infinite().sum()) == 0
    # per-date mean ~0 on a sampled date.
    agg = std.group_by("trade_date").agg(pl.col("px_ret_20d").mean().alias("m")).head(3)
    for m in agg["m"].to_list():
        assert abs(m) < 1e-6


def test_manifest_pins_reproducibility(built) -> None:
    res, spec = built
    man = json.loads((res.dataset_dir / "dataset_manifest.json").read_text())
    assert man["model_id"] == spec.model_id
    assert man["feature_groups"] == ["px", "flow"]
    assert man["label_spec"]["primary_target"] == "y_rank_20d"
    assert man["period"] == {"start": "2024-01-01", "end": "2024-12-31"}
    assert man["row_count"] == res.panel_rows


def test_split_folds_have_embargo(built) -> None:
    res, _ = built
    folds = pl.read_parquet(res.dataset_dir / "split_folds.parquet").to_dicts()
    assert folds
    for f in folds:
        # valid_start strictly after train_end (embargo+purge gap in sessions).
        assert f["valid_start"] > f["train_end"]
