"""Unit tests for research ETL config (P0)."""

from __future__ import annotations

from pathlib import Path

import pytest
from research.etl.config import (
    CANONICAL_TABLES,
    RAW_TABLES,
    EngineOptions,
    LakeConfig,
)


def test_lake_roots_follow_exporter_layout() -> None:
    cfg = LakeConfig(
        snapshot_date="2026-06-19",
        source="local_mydb",
        data_lake_root=Path("/lake"),
    )
    assert cfg.raw_root == Path("/lake/raw_postgres/snapshot_date=2026-06-19/source=local_mydb")
    assert cfg.canonical_root == Path(
        "/lake/canonical_postgres/snapshot_date=2026-06-19/source=local_mydb"
    )


def test_table_glob_routes_raw_vs_canonical() -> None:
    cfg = LakeConfig(data_lake_root=Path("/lake"))
    assert cfg.table_glob("daily_ohlcv").startswith(str(cfg.raw_root))
    assert cfg.table_glob("stock_metric_fact").startswith(str(cfg.canonical_root))
    assert cfg.table_glob("daily_ohlcv").endswith("/daily_ohlcv/**/*.parquet")


def test_table_glob_unknown_raises() -> None:
    cfg = LakeConfig(data_lake_root=Path("/lake"))
    with pytest.raises(KeyError):
        cfg.table_glob("operating_source_document")  # schema-only, not registered


def test_table_sets_disjoint_and_expected_counts() -> None:
    assert set(RAW_TABLES).isdisjoint(CANONICAL_TABLES)
    assert len(RAW_TABLES) == 12  # 13th (operating_source_document) is schema-only
    assert len(CANONICAL_TABLES) == 5


def test_engine_options_pragmas() -> None:
    assert EngineOptions().as_pragmas() == {}
    pragmas = EngineOptions(threads=14, memory_limit="2GB", temp_directory="/tmp/x").as_pragmas()
    assert pragmas == {
        "threads": "14",
        "memory_limit": "2GB",
        "temp_directory": "/tmp/x",
    }
