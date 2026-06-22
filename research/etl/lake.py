"""L1 lake reader — DuckDB connection + view registration over Parquet.

Every ETL query in the ``research`` package runs on top of the views created
here. Two invariants this module enforces (both verified bugs from etl_01):

1. ``hive_partitioning=false`` ALWAYS. The lake path contains ``source=...``
   and ``krx_security_flow_raw`` *also* has a real ``source`` column
   (``KRX``/``PYKRX``). With hive=true the path value silently overwrites the
   data column, neutralizing the KRX-first dedup (etl_01 §4.2). We never enable
   it; partition pruning is driven by real data columns (``trade_date`` etc.),
   which DuckDB still applies from the ``year=/month=`` path automatically.
2. ``numeric`` arrives as ``DECIMAL`` (Decimal128). Ratio/log math should cast
   to ``DOUBLE`` first (etl_01 §3); :func:`cast_double` builds that expression.

See ``docs/target/01_20_access_return_rank/etl_01_parquet_data_flow_plan.md`` §3, §4.
"""

from __future__ import annotations

from collections.abc import Iterable

import duckdb

from research.etl.config import CANONICAL_TABLES, RAW_TABLES, LakeConfig


def connect(config: LakeConfig | None = None) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with the configured engine pragmas."""
    config = config or LakeConfig()
    con = duckdb.connect()
    for key, value in config.engine.as_pragmas().items():
        # Identifiers (key) are from a fixed allowlist; value is quoted.
        con.execute(f"SET {key} = '{value}'")
    return con


def _sql_str_literal(value: str) -> str:
    """Single-quote a string for inline SQL (escape embedded quotes)."""
    return "'" + value.replace("'", "''") + "'"


def _create_view(con: duckdb.DuckDBPyConnection, table: str, glob: str) -> None:
    # hive_partitioning=false is mandatory — see module docstring (etl_01 §4.2).
    # DuckDB rejects prepared parameters inside CREATE VIEW DDL, so the glob
    # (a trusted, config-derived path) is inlined as a quoted literal.
    con.execute(
        f"CREATE OR REPLACE VIEW {table} AS "
        f"SELECT * FROM read_parquet({_sql_str_literal(glob)}, hive_partitioning=false)"
    )


def register_views(
    con: duckdb.DuckDBPyConnection,
    config: LakeConfig | None = None,
    *,
    tables: Iterable[str] | None = None,
) -> list[str]:
    """Register one DuckDB view per lake table, returning the names created.

    By default registers every raw + canonical table that has parquet files on
    disk. Missing tables are skipped (the raw 1st-pass lake may lack canonical
    exports) so callers can register opportunistically. Pass ``tables`` to
    restrict to a specific set (a ``KeyError`` is raised for unknown names).
    """
    config = config or LakeConfig()
    requested = tuple(tables) if tables is not None else RAW_TABLES + CANONICAL_TABLES

    created: list[str] = []
    for table in requested:
        glob = config.table_glob(table)  # raises KeyError on unknown table
        # Probe: read_parquet errors if zero files match. Skip absent tables
        # unless they were explicitly requested.
        if not _glob_has_files(con, glob):
            if tables is not None:
                raise FileNotFoundError(f"no parquet files for table {table!r} at {glob}")
            continue
        _create_view(con, table, glob)
        created.append(table)
    return created


def _glob_has_files(con: duckdb.DuckDBPyConnection, glob: str) -> bool:
    """True if at least one parquet file matches the glob."""
    rows = con.execute("SELECT count(*) FROM glob(?)", [glob]).fetchone()
    return bool(rows and rows[0] > 0)


def cast_double(column: str) -> str:
    """SQL expression casting a (possibly Decimal128) column to DOUBLE.

    Use before ratio/log/z-score math to avoid Decimal overflow/precision
    surprises (etl_01 §3). Keep winsorize/log ordering per etl_00 §4.3.
    """
    return f"CAST({column} AS DOUBLE)"
