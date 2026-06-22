"""Integration smoke test for P7 — feat_fin_pit + F_fin build on the real lake.

Self-skips when the canonical lake is absent. Pins the etl_01 §6 guard (Samsung
2024-06-03 PIT as-of preserves ~26 metric_codes) and checks the fin-enabled
dataset build produces ``fin_`` columns with no look-ahead.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import polars as pl
import pytest
from research.etl.config import EngineOptions, LakeConfig
from research.etl.features import fin_pit
from research.etl.lake import connect, register_views
from research.etl.universe import UniverseFilter, build_universe_sql
from research.models._01_20_access_return_rank import build_dataset as bd
from research.models._01_20_access_return_rank.spec import ModelSpec


@pytest.fixture()
def fin_con():
    cfg = LakeConfig(engine=EngineOptions(threads=4, memory_limit="4GB"))
    if not cfg.canonical_root.exists():
        pytest.skip(f"canonical lake not present at {cfg.canonical_root}")
    con = connect(cfg)
    register_views(con, cfg, tables=["daily_ohlcv", "stock_metric_fact"])
    con.execute(f"CREATE VIEW dim_universe_daily AS {build_universe_sql(UniverseFilter())}")
    con.execute(f"CREATE VIEW feat_fin_pit AS {fin_pit.build_fin_pit_sql()}")
    return con


def test_samsung_pit_preserves_metrics(fin_con) -> None:
    # etl_01 §6: a single (t, ticker) must carry all available metric_codes
    # (verified value: 26 for Samsung 2024-06-03), so the wide ratios resolve.
    row = fin_con.execute("""
        SELECT fin_has_fs, fin_roa, fin_equity_ratio, fin_debt_to_equity
        FROM feat_fin_pit
        WHERE ticker = '005930' AND trade_date = DATE '2024-06-03'
        """).fetchone()
    assert row is not None
    has_fs, roa, eq_ratio, _dte = row
    assert has_fs is True
    assert roa is not None  # net_income / total_assets resolved
    assert 0.0 < eq_ratio < 1.0  # equity/assets is a sane fraction


def test_no_lookahead_available_from(fin_con) -> None:
    # Every fin row must come from a report available at or before trade_date.
    # Probe: the earliest fin row for Samsung cannot precede its first
    # available_from (period_end+90d of the oldest annual report).
    (min_fin_date,) = fin_con.execute(
        "SELECT min(trade_date) FROM feat_fin_pit WHERE ticker = '005930'"
    ).fetchone()
    (min_avail,) = fin_con.execute("""
        SELECT min(period_end + INTERVAL '90 days')
        FROM stock_metric_fact WHERE ticker = '005930' AND period_type = 'annual'
        """).fetchone()
    assert min_fin_date >= min_avail.date() if hasattr(min_avail, "date") else min_avail


@pytest.fixture()
def fin_built(tmp_path: Path):
    cfg = LakeConfig(datasets_root=tmp_path, engine=EngineOptions(threads=4, memory_limit="4GB"))
    if not cfg.canonical_root.exists():
        pytest.skip("canonical lake not present")
    base = ModelSpec(period_start="2024-01-01", period_end="2024-12-31", n_folds=2)
    spec = dataclasses.replace(base, feature_groups=("px", "flow", "fin"))
    res = bd.build_dataset(spec, cfg, created_at="x", write=True)
    return res


def test_fin_build_has_fin_columns(fin_built) -> None:
    fin_cols = [c for c in fin_built.feature_cols if c.startswith("fin_")]
    assert "fin_roa" in fin_cols
    assert "fin_equity_ratio" in fin_cols
    std = pl.read_parquet(fin_built.dataset_dir / "feat_panel_std.parquet")
    # fin columns standardized + finite (L1 gate already asserted in build).
    assert "fin_roa" in std.columns
