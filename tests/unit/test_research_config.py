"""Unit tests for research ETL config (P0)."""

from __future__ import annotations

from pathlib import Path

import pytest
import research.etl.config as config_module
from research.etl.config import (
    CANONICAL_TABLES,
    CONFIG_TABLES,
    RAW_TABLES,
    REMOTE_SOURCE,
    EngineOptions,
    LakeConfig,
)


def test_remote_source_constant() -> None:
    assert REMOTE_SOURCE == "sj2_remote"


def test_default_source_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    monkeypatch.setenv("SDC_LAKE_SOURCE", REMOTE_SOURCE)
    try:
        importlib.reload(config_module)
        assert config_module.DEFAULT_SOURCE == REMOTE_SOURCE
        assert config_module.LakeConfig().source == REMOTE_SOURCE
    finally:
        monkeypatch.delenv("SDC_LAKE_SOURCE", raising=False)
        importlib.reload(config_module)
    assert config_module.DEFAULT_SOURCE == "local_mydb"


def test_lake_root_supports_remote_source() -> None:
    cfg = LakeConfig(
        snapshot_date="2026-07-30",
        source=REMOTE_SOURCE,
        data_lake_root=Path("/lake"),
    )
    assert cfg.raw_root == Path("/lake/raw_postgres/snapshot_date=2026-07-30/source=sj2_remote")


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
    assert cfg.derived_mart_root == Path(
        "/lake/derived_mart/snapshot_date=2026-06-19/source=local_mydb"
    )


def test_table_glob_routes_raw_vs_canonical() -> None:
    cfg = LakeConfig(data_lake_root=Path("/lake"))
    assert cfg.table_glob("daily_ohlcv").startswith(str(cfg.raw_root))
    assert cfg.table_glob("stock_metric_fact").startswith(str(cfg.canonical_root))
    assert cfg.table_glob("daily_ohlcv").endswith("/daily_ohlcv/**/*.parquet")


def test_table_glob_routes_config_to_raw_root() -> None:
    # Decision 7: common_feature_series lives under the raw lake root.
    cfg = LakeConfig(data_lake_root=Path("/lake"))
    assert cfg.table_glob("common_feature_series").startswith(str(cfg.raw_root))


def test_table_glob_unknown_raises() -> None:
    cfg = LakeConfig(data_lake_root=Path("/lake"))
    with pytest.raises(KeyError):
        cfg.table_glob("operating_source_document")  # schema-only, not registered


def test_table_sets_disjoint_and_expected_counts() -> None:
    assert set(RAW_TABLES).isdisjoint(CANONICAL_TABLES)
    assert set(RAW_TABLES).isdisjoint(CONFIG_TABLES)
    assert len(RAW_TABLES) == 15  # +daily_market_cap (N1)
    assert CONFIG_TABLES == ("common_feature_series",)
    assert len(CANONICAL_TABLES) == 5


def test_engine_options_pragmas() -> None:
    assert EngineOptions().as_pragmas() == {}
    pragmas = EngineOptions(threads=14, memory_limit="2GB", temp_directory="/tmp/x").as_pragmas()
    assert pragmas == {
        "threads": "14",
        "memory_limit": "2GB",
        "temp_directory": "/tmp/x",
    }
