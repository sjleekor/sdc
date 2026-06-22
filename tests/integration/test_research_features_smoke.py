"""Integration smoke test for P3 against the real lake (feat_price / feat_flow).

Self-skips when the raw lake is absent. Builds the feature SQL as in-memory
views (no mart write into the repo) and pins the KRX-first dedup distinct count
to the measured figure (etl_01 §4.2) as a regression guard.
"""

from __future__ import annotations

import pytest
from research.etl.config import EngineOptions, LakeConfig
from research.etl.features import flow, price
from research.etl.lake import connect, register_views

# Measured on the 2026-06-19 lake (etl_01 §4.2 / etl_02 §3 Q1). Regression guard.
EXPECTED_FLOW_DEDUP_DISTINCT = 55_918_702


@pytest.fixture()
def real_con():
    cfg = LakeConfig(engine=EngineOptions(threads=4, memory_limit="4GB"))
    if not cfg.raw_root.exists():
        pytest.skip(f"raw lake not present at {cfg.raw_root}")
    con = connect(cfg)
    register_views(con, cfg, tables=["daily_ohlcv", "krx_security_flow_raw"])
    return con


def test_flow_dedup_distinct_count_matches_measured(real_con) -> None:
    dedup = flow.build_dedup_sql("krx_security_flow_raw")
    (n,) = real_con.execute(f"SELECT count(*) FROM ({dedup})").fetchone()
    assert n == EXPECTED_FLOW_DEDUP_DISTINCT


def test_feat_price_rowcount_matches_daily_ohlcv(real_con) -> None:
    real_con.execute(f"CREATE VIEW feat_price AS {price.build_price_sql('daily_ohlcv')}")
    (fp,) = real_con.execute("SELECT count(*) FROM feat_price").fetchone()
    (p,) = real_con.execute("SELECT count(*) FROM daily_ohlcv").fetchone()
    assert fp == p


def test_feat_flow_one_row_per_key_grain(real_con) -> None:
    real_con.execute(f"CREATE VIEW feat_flow AS {flow.build_flow_sql('krx_security_flow_raw')}")
    # feat_flow grain is (trade_date, ticker, market) after the wide pivot.
    (rows,) = real_con.execute("SELECT count(*) FROM feat_flow").fetchone()
    (distinct_keys,) = real_con.execute(
        "SELECT count(*) FROM (SELECT DISTINCT trade_date, ticker, market FROM feat_flow)"
    ).fetchone()
    assert rows == distinct_keys
    assert rows > 8_000_000  # ~8.9M (trade_date,ticker,market) keys
