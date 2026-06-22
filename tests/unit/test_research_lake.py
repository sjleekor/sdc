"""Unit tests for the research ETL lake reader (P1).

These build a tiny synthetic lake on disk (no DB, no real ``data_lake/``) that
mimics the exporter layout, then assert the two invariants from etl_01:

1. hive_partitioning=false preserves a real ``source`` data column that
   collides with the path's ``source=`` partition (etl_01 §4.2).
2. Decimal columns are castable to DOUBLE via :func:`cast_double` (etl_01 §3).
"""

from __future__ import annotations

import decimal
from pathlib import Path

import duckdb
import pytest
from research.etl import lake
from research.etl.config import EngineOptions, LakeConfig


def _write_flow_fixture(lake_root: Path, snapshot_date: str, source: str) -> None:
    """Write a krx_security_flow_raw fixture under the canonical lake layout.

    The fixture deliberately gives rows a real ``source`` column value
    (``KRX``/``PYKRX``) that differs from the path partition (``source=...``),
    so a hive-partitioning regression would be caught.
    """
    table_dir = (
        lake_root
        / "raw_postgres"
        / f"snapshot_date={snapshot_date}"
        / f"source={source}"
        / "krx_security_flow_raw"
        / "schema_version=1"
        / "year=2020"
        / "month=01"
    )
    table_dir.mkdir(parents=True, exist_ok=True)
    out = (table_dir / "part-000000.parquet").as_posix()

    d = "DATE '2020-01-02'"
    dec = "CAST(1234.5678 AS DECIMAL(30,4))"
    con = duckdb.connect()
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            ({d}, '005930', 'KOSPI', 'foreign_netbuy', {dec}, 'KRX'),
            ({d}, '005930', 'KOSPI', 'foreign_netbuy', {dec}, 'PYKRX'),
            ({d}, '000660', 'KOSPI', 'short_volume', CAST(10.0 AS DECIMAL(30,4)), 'KRX')
          ) AS t(trade_date, ticker, market, metric_code, value, source)
        ) TO '{out}' (FORMAT PARQUET)
        """)
    con.close()


@pytest.fixture()
def synthetic_lake(tmp_path: Path) -> LakeConfig:
    snapshot_date = "2020-01-01"
    source = "local_mydb"
    _write_flow_fixture(tmp_path, snapshot_date, source)
    return LakeConfig(
        snapshot_date=snapshot_date,
        source=source,
        data_lake_root=tmp_path,
        engine=EngineOptions(threads=2),
    )


def test_register_views_creates_present_table(synthetic_lake: LakeConfig) -> None:
    con = lake.connect(synthetic_lake)
    created = lake.register_views(con, synthetic_lake)

    assert "krx_security_flow_raw" in created
    # Only the fixture table exists on disk; absent tables are skipped silently.
    assert created == ["krx_security_flow_raw"]


def test_hive_partitioning_false_preserves_source_column(synthetic_lake: LakeConfig) -> None:
    """The real KRX/PYKRX source column must survive, not be overwritten by the
    ``source=local_mydb`` path partition (etl_01 §4.2 dedup-killer bug)."""
    con = lake.connect(synthetic_lake)
    lake.register_views(con, synthetic_lake)

    rows = con.execute(
        "SELECT source, count(*) FROM krx_security_flow_raw GROUP BY source ORDER BY source"
    ).fetchall()

    assert rows == [("KRX", 2), ("PYKRX", 1)]
    # And crucially the path partition value never leaks into the data.
    assert "local_mydb" not in {r[0] for r in rows}


def test_value_column_is_decimal_and_casts_to_double(synthetic_lake: LakeConfig) -> None:
    con = lake.connect(synthetic_lake)
    lake.register_views(con, synthetic_lake)

    (raw_value,) = con.execute(
        "SELECT value FROM krx_security_flow_raw WHERE ticker = '005930' LIMIT 1"
    ).fetchone()
    assert isinstance(raw_value, decimal.Decimal)

    (as_double,) = con.execute(
        f"SELECT {lake.cast_double('value')} FROM krx_security_flow_raw "
        "WHERE ticker = '005930' LIMIT 1"
    ).fetchone()
    assert isinstance(as_double, float)
    assert as_double == pytest.approx(1234.5678)


def test_register_unknown_table_raises(synthetic_lake: LakeConfig) -> None:
    con = lake.connect(synthetic_lake)
    with pytest.raises(KeyError):
        lake.register_views(con, synthetic_lake, tables=["not_a_real_table"])


def test_register_requested_but_absent_table_raises(synthetic_lake: LakeConfig) -> None:
    con = lake.connect(synthetic_lake)
    # stock_metric_fact is a known canonical table but absent in this fixture;
    # explicit request must fail loudly rather than skip.
    with pytest.raises(FileNotFoundError):
        lake.register_views(con, synthetic_lake, tables=["stock_metric_fact"])
