"""ETL configuration — lake roots, snapshot pin, and DuckDB engine options.

Single source of truth for *where* the lake is and *how* the engine reads it.
All other ETL modules import paths/options from here rather than hard-coding.

See:
- ``docs/target/01_20_access_return_rank/etl_01_parquet_data_flow_plan.md`` §0.5, §1, §3
- ``docs/target/01_20_access_return_rank/etl_02_engine_comparison.md`` §6 (engine options)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# --- repository / lake roots -------------------------------------------------

# research/etl/config.py -> repo root is two parents up.
REPO_ROOT: Path = Path(__file__).resolve().parents[2]

# Override with SDC_DATA_LAKE_ROOT for tests / alternate lakes.
DATA_LAKE_ROOT: Path = Path(os.environ.get("SDC_DATA_LAKE_ROOT", REPO_ROOT / "data_lake"))

# Per-model datasets (L2b) live here, separate from the shared lake (etl_01 §7).
DATASETS_ROOT: Path = Path(os.environ.get("SDC_DATASETS_ROOT", REPO_ROOT / "data" / "datasets"))

RAW_LAKE_NAME = "raw_postgres"
# Legacy Postgres-export lake for the derived facts. Kept readable while the
# DuckDB marts run A/B against it (parity, P2). Decommissioned at P6.
CANONICAL_LAKE_NAME = "canonical_postgres"
# New home for DuckDB-produced derived facts (refactor §8.1 OQ2): the marts in
# research/etl/marts recompute stock_metric_fact / common_feature_daily_fact from
# raw, so the output is a *derived mart*, not a Postgres canonical export.
DERIVED_MART_LAKE_NAME = "derived_mart"

# Default export source (exporter writes ``source=<name>`` into the path).
DEFAULT_SOURCE = "local_mydb"

# Default snapshot — the lake export this model was designed against
# (etl_01 §1, §5). Override per run via SDC_SNAPSHOT_DATE or LakeConfig.
DEFAULT_SNAPSHOT_DATE = os.environ.get("SDC_SNAPSHOT_DATE", "2026-06-19")


# --- table -> lake-root mapping (etl_01 §2) ---------------------------------
# raw lake: raw + reference tables exported from Postgres. The derived facts
# (stock_metric_fact / common_feature_daily_fact) are no longer exported — the
# DuckDB marts in research/etl/marts recompute them — so CANONICAL_TABLES is
# being emptied as the consumers move to the marts (refactor §3.3).

RAW_TABLES: tuple[str, ...] = (
    "daily_ohlcv",
    "krx_security_flow_raw",
    "dart_xbrl_fact_raw",
    "dart_financial_statement_raw",
    "dart_shareholder_return_raw",
    "dart_share_count_raw",
    "dart_xbrl_document",
    "dart_corp_master",
    "stock_master",
    "stock_master_snapshot",
    "stock_master_snapshot_items",
    "common_feature_observation_raw",
)

# Decision 7: common_feature_series is the one config table the collector reads at
# runtime AND the compute mart needs, so it is exported to the raw lake and read
# back as a view — the collector and compute see the same rows (no drift branch).
CONFIG_TABLES: tuple[str, ...] = ("common_feature_series",)

# Legacy Postgres-exported derived/catalog tables. Read only during the P2 A/B
# parity window; the marts replace them and this tuple empties at §3.3 step 3.
CANONICAL_TABLES: tuple[str, ...] = (
    "stock_metric_fact",
    "common_feature_daily_fact",
    "metric_catalog",
    "metric_mapping_rule",
    "common_feature_catalog",
)


@dataclass(frozen=True)
class EngineOptions:
    """DuckDB connection knobs (etl_02 §6).

    ``threads`` defaults to DuckDB's own default when None. ``memory_limit``
    (e.g. ``"2GB"``) caps RAM; DuckDB spills the 2.2GB flow dedup to disk under
    a tight limit (etl_02 §3.1). ``temp_directory`` is where spill files land.
    """

    threads: int | None = None
    memory_limit: str | None = None
    temp_directory: str | None = None

    def as_pragmas(self) -> dict[str, str]:
        pragmas: dict[str, str] = {}
        if self.threads is not None:
            pragmas["threads"] = str(self.threads)
        if self.memory_limit is not None:
            pragmas["memory_limit"] = self.memory_limit
        if self.temp_directory is not None:
            pragmas["temp_directory"] = self.temp_directory
        return pragmas


@dataclass(frozen=True)
class LakeConfig:
    """Resolved lake location for one snapshot.

    A ``LakeConfig`` pins reproducibility: same ``snapshot_date`` => same input
    parquet regardless of later DB changes (etl_01 §0.5).
    """

    snapshot_date: str = DEFAULT_SNAPSHOT_DATE
    source: str = DEFAULT_SOURCE
    data_lake_root: Path = DATA_LAKE_ROOT
    datasets_root: Path = DATASETS_ROOT
    engine: EngineOptions = field(default_factory=EngineOptions)

    def lake_root(self, lake_name: str) -> Path:
        """Root for one lake (``raw_postgres`` / ``canonical_postgres``)."""
        return (
            self.data_lake_root
            / lake_name
            / f"snapshot_date={self.snapshot_date}"
            / f"source={self.source}"
        )

    @property
    def raw_root(self) -> Path:
        return self.lake_root(RAW_LAKE_NAME)

    @property
    def canonical_root(self) -> Path:
        return self.lake_root(CANONICAL_LAKE_NAME)

    @property
    def derived_mart_root(self) -> Path:
        return self.lake_root(DERIVED_MART_LAKE_NAME)

    def dataset_dir(self, model_id: str) -> Path:
        """Per-model dataset dir (L2b): ``datasets/<model_id>/snapshot_date=.../``."""
        return self.datasets_root / model_id / f"snapshot_date={self.snapshot_date}"

    def table_glob(self, table: str) -> str:
        """Recursive parquet glob for a table, across the lake roots.

        Raises ``KeyError`` if the table is not a known raw/config/canonical table.
        ``common_feature_series`` (CONFIG_TABLES) lives under the raw lake root
        (decision 7).
        """
        if table in RAW_TABLES or table in CONFIG_TABLES:
            root = self.raw_root
        elif table in CANONICAL_TABLES:
            root = self.canonical_root
        else:
            known = RAW_TABLES + CONFIG_TABLES + CANONICAL_TABLES
            raise KeyError(f"unknown lake table {table!r}; expected one of {known}")
        return str(root / table / "**" / "*.parquet")
