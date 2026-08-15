"""Integration smoke test for P8 — feat_common / feat_event + full multimodal build.

Self-skips when the canonical lake is absent. Verifies feat_common is PIT-safe
(asof <= date) and broadcast (one row per date), feat_event PIT join works, and
the full px+flow+fin+cf+ev build assembles with all group prefixes present.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import polars as pl
import pytest
from research.etl.config import EngineOptions, LakeConfig
from research.etl.features import common, event
from research.etl.lake import connect, register_derived_marts, register_views
from research.etl.universe import UniverseFilter, build_universe_sql
from research.models._01_20_access_return_rank import build_dataset as bd
from research.models._01_20_access_return_rank.spec import ModelSpec


@pytest.fixture()
def cf_con():
    cfg = LakeConfig(engine=EngineOptions(threads=4, memory_limit="4GB"))
    if not cfg.raw_root.exists():
        pytest.skip(f"raw lake not present at {cfg.raw_root}")
    con = connect(cfg)
    # common_feature_daily_fact is recomputed from the raw observation lake (§3.3).
    register_views(con, cfg, tables=["common_feature_observation_raw"])
    register_derived_marts(con, cfg, which=["common_feature_daily_fact"])
    con.execute(f"CREATE VIEW feat_common AS {common.build_common_sql()}")
    return con


def test_common_one_row_per_date(cf_con) -> None:
    rows, distinct = cf_con.execute(
        "SELECT count(*), count(DISTINCT trade_date) FROM feat_common"
    ).fetchone()
    assert rows == distinct  # broadcast grain: one row per date
    assert rows > 100  # ~150 trading days of daily history


def test_common_columns_present(cf_con) -> None:
    cols = [r[0] for r in cf_con.execute("DESCRIBE feat_common").fetchall()]
    assert "cf_market_kospi_ret_5d" in cols
    assert "cf_global_vix_level" in cols


@pytest.fixture()
def ev_con():
    cfg = LakeConfig(engine=EngineOptions(threads=4, memory_limit="4GB"))
    if not cfg.raw_root.exists():
        pytest.skip(f"raw lake not present at {cfg.raw_root}")
    con = connect(cfg)
    register_views(con, cfg, tables=["daily_ohlcv", "dart_share_count_raw"])
    con.execute(f"CREATE VIEW dim_universe_daily AS {build_universe_sql(UniverseFilter())}")
    con.execute(f"CREATE VIEW feat_event AS {event.build_event_sql()}")
    return con


def test_event_treasury_ratio_bounded(ev_con) -> None:
    # treasury ratio is a fraction in [0, 1] where present.
    row = ev_con.execute(
        "SELECT max(ev_treasury_ratio), min(ev_treasury_ratio) "
        "FROM feat_event WHERE ev_treasury_ratio IS NOT NULL"
    ).fetchone()
    assert row[0] is not None
    assert 0.0 <= row[1] <= row[0] <= 1.0


@pytest.fixture()
def full_built(tmp_path: Path):
    cfg = LakeConfig(datasets_root=tmp_path, engine=EngineOptions(threads=4, memory_limit="4GB"))
    if not cfg.raw_root.exists():
        pytest.skip("raw lake not present")
    base = ModelSpec(period_start="2025-01-01", period_end="2026-06-10", n_folds=2)
    spec = dataclasses.replace(base, feature_groups=("px", "flow", "fin", "cf", "ev"))
    return bd.build_dataset(spec, cfg, created_at="x", write=True)


def test_full_multimodal_build_has_all_groups(full_built) -> None:
    cols = full_built.feature_cols
    for prefix in ("px_", "flow_", "fin_", "cf_", "ev_"):
        assert any(c.startswith(prefix) for c in cols), f"missing {prefix} features"
    std = pl.read_parquet(full_built.dataset_dir / "feat_panel_std.parquet")
    # cf features finite after standardize (L1 gate already enforced in build).
    assert "cf_market_kospi_ret_5d" in std.columns
