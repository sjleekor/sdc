"""Feature mart (L2a) materialization helpers.

The mart is the model-agnostic, snapshot-pinned layer between the raw/canonical
lake and per-model datasets (00_shared §1). Heavy computations (flow dedup,
financial PIT as-of) run here once per snapshot and are written to parquet;
models then join the mart cheaply.

Caching is idempotent by directory: a mart that already exists is skipped unless
``force=True`` (00_shared §5), mirroring the exporter scripts' ``--force``.

See ``docs/target/00_shared_etl_platform.md`` §1, §5 and
``docs/target/01_20_access_return_rank/etl_03_implementation_plan.md`` §4 (P2).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb

from research.etl.config import LakeConfig
from research.etl.lake import _sql_str_literal

FEATURE_MART_NAME = "feature_mart"


def mart_root(config: LakeConfig) -> Path:
    """Root for the snapshot's feature mart (``data_lake/feature_mart/...``)."""
    return config.data_lake_root / FEATURE_MART_NAME / f"snapshot_date={config.snapshot_date}"


def mart_table_dir(config: LakeConfig, name: str) -> Path:
    """Directory holding one mart table's parquet part files."""
    return mart_root(config) / name


def mart_glob(config: LakeConfig, name: str) -> str:
    """Recursive parquet glob for a materialized mart table."""
    return str(mart_table_dir(config, name) / "**" / "*.parquet")


def is_materialized(config: LakeConfig, name: str) -> bool:
    """True if the mart table has at least one parquet part on disk."""
    directory = mart_table_dir(config, name)
    return directory.is_dir() and any(directory.rglob("*.parquet"))


def materialize(
    con: duckdb.DuckDBPyConnection,
    config: LakeConfig,
    name: str,
    select_sql: str,
    *,
    force: bool = False,
    partition_by: list[str] | None = None,
) -> Path:
    """Write ``select_sql`` to ``feature_mart/.../<name>/`` as parquet.

    Idempotent: returns early (skips the write) when the table already exists
    and ``force`` is False. With ``force`` the existing directory is removed and
    rebuilt. Returns the table directory.
    """
    table_dir = mart_table_dir(config, name)
    if is_materialized(config, name) and not force:
        return table_dir

    if table_dir.exists():
        shutil.rmtree(table_dir)
    table_dir.mkdir(parents=True, exist_ok=True)

    out = _sql_str_literal(str(table_dir))
    copy_opts = ["FORMAT PARQUET", "COMPRESSION ZSTD"]
    if partition_by:
        cols = ", ".join(partition_by)
        copy_opts.append(f"PARTITION_BY ({cols})")
        # PARTITION_BY writes a directory tree; otherwise a single file.
        target = out
    else:
        target = _sql_str_literal(str(table_dir / "part-000000.parquet"))

    con.execute(f"COPY ({select_sql}) TO {target} ({', '.join(copy_opts)})")
    return table_dir


def register_mart_view(
    con: duckdb.DuckDBPyConnection,
    config: LakeConfig,
    name: str,
    *,
    view_name: str | None = None,
) -> str:
    """Register a DuckDB view over a materialized mart table (hive=false).

    Returns the created view name. Raises ``FileNotFoundError`` if the mart
    table has not been materialized yet.
    """
    if not is_materialized(config, name):
        raise FileNotFoundError(
            f"mart table {name!r} not materialized at {mart_table_dir(config, name)}"
        )
    view = view_name or name
    glob = _sql_str_literal(mart_glob(config, name))
    con.execute(
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT * FROM read_parquet({glob}, hive_partitioning=false)"
    )
    return view
