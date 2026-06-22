"""Integration smoke test against the real Parquet lake (P1).

Self-skips when the lake is absent, matching the repo's integration-test
convention. When present, it asserts the etl_01 §4.2 dedup-killer invariant on
the real ``krx_security_flow_raw`` (KRX/PYKRX must be distinct, not all the
``source=`` path value) and that the headline source counts match the measured
figures recorded in etl_01 §4.2.
"""

from __future__ import annotations

import pytest
from research.etl import lake
from research.etl.config import LakeConfig

# Measured on the 2026-06-19 lake (etl_01 §4.2). Pinned as a regression guard.
EXPECTED_FLOW_SOURCE_COUNTS = {"KRX": 55_908_238, "PYKRX": 20_628_334}


@pytest.fixture()
def real_lake() -> LakeConfig:
    cfg = LakeConfig()
    if not cfg.raw_root.exists():
        pytest.skip(f"raw lake not present at {cfg.raw_root}")
    return cfg


def test_real_flow_source_preserved_hive_false(real_lake: LakeConfig) -> None:
    con = lake.connect(real_lake)
    created = lake.register_views(con, real_lake, tables=["krx_security_flow_raw"])
    assert created == ["krx_security_flow_raw"]

    rows = dict(
        con.execute("SELECT source, count(*) FROM krx_security_flow_raw GROUP BY source").fetchall()
    )
    # The path partition value must never appear as a data value.
    assert "local_mydb" not in rows
    # Both real sources present and exactly as measured.
    assert rows == EXPECTED_FLOW_SOURCE_COUNTS


def test_real_daily_ohlcv_registers(real_lake: LakeConfig) -> None:
    con = lake.connect(real_lake)
    lake.register_views(con, real_lake, tables=["daily_ohlcv"])
    (n,) = con.execute("SELECT count(*) FROM daily_ohlcv").fetchone()
    assert n > 6_000_000  # ~6.55M rows (etl_01 / feature profile)
