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
CANONICAL_LAKE_NAME = "canonical_postgres"

# Default export source (exporter writes ``source=<name>`` into the path).
DEFAULT_SOURCE = "local_mydb"

# Default snapshot — the lake export this model was designed against
# (etl_01 §1, §5). Override per run via SDC_SNAPSHOT_DATE or LakeConfig.
DEFAULT_SNAPSHOT_DATE = os.environ.get("SDC_SNAPSHOT_DATE", "2026-06-19")


# --- table -> lake-root mapping (etl_01 §2) ---------------------------------
# raw lake: 13 raw/reference tables. canonical lake: 5 derived/catalog tables.
# operating_source_document is schema-only (no partitions) and not read by the
# model ETL, so it is intentionally omitted from the registered views.

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

    def dataset_dir(self, model_id: str) -> Path:
        """Per-model dataset dir (L2b): ``datasets/<model_id>/snapshot_date=.../``."""
        return self.datasets_root / model_id / f"snapshot_date={self.snapshot_date}"

    def table_glob(self, table: str) -> str:
        """Recursive parquet glob for a table, across both lake roots.

        Raises ``KeyError`` if the table is not a known raw/canonical table.
        """
        if table in RAW_TABLES:
            root = self.raw_root
        elif table in CANONICAL_TABLES:
            root = self.canonical_root
        else:
            raise KeyError(
                f"unknown lake table {table!r}; " f"expected one of {RAW_TABLES + CANONICAL_TABLES}"
            )
        return str(root / table / "**" / "*.parquet")
