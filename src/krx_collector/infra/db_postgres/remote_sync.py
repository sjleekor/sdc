"""Remote-to-local PostgreSQL sync helpers.

This module copies the pipeline tables from a remote PostgreSQL instance
into the local PostgreSQL database in batches. Incremental sync uses a
stable composite cursor of ``(watermark_timestamp, primary_key...)`` so
that rows sharing the same timestamp are not skipped across batch
boundaries.
"""

from __future__ import annotations

import contextlib
import csv
import io
import json
import logging
import socket
import subprocess
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

DAILY_OHLCV_SYNC_NAME = "remote_db_sync.daily_ohlcv"
DAILY_OHLCV_STAGING_TABLE = "staging_daily_ohlcv"
FULL_REFRESH_DAILY_OHLCV_BATCH_SIZE = 200_000


@dataclass(frozen=True, slots=True)
class RemoteDbInfo:
    """Connection details for the remote PostgreSQL instance."""

    host: str
    port: int
    db_name: str
    user: str
    password: str
    container: str | None = None

    def to_dsn(self, host_override: str | None = None, port_override: int | None = None) -> str:
        """Build a PostgreSQL DSN string."""
        host = host_override or self.host
        port = port_override or self.port
        return (
            f"postgresql://{quote(self.user, safe='')}:{quote(self.password, safe='')}"
            f"@{host}:{port}/{quote(self.db_name, safe='')}"
        )


@dataclass(frozen=True, slots=True)
class TableSyncSpec:
    """Metadata describing how to copy a single table."""

    name: str
    select_list: str
    from_clause: str
    order_columns: tuple[str, ...]
    insert_columns: tuple[str, ...]
    conflict_columns: tuple[str, ...]
    update_columns: tuple[str, ...]
    local_cursor_sql: str
    cursor_indexes: tuple[int, ...]


SYNC_TABLE_SPECS: tuple[TableSyncSpec, ...] = (
    TableSyncSpec(
        name="stock_master",
        select_list="ticker, market, name, status, last_seen_date, source, updated_at",
        from_clause="stock_master",
        order_columns=("updated_at", "ticker", "market"),
        insert_columns=(
            "ticker",
            "market",
            "name",
            "status",
            "last_seen_date",
            "source",
            "updated_at",
        ),
        conflict_columns=("ticker", "market"),
        update_columns=("name", "status", "last_seen_date", "source", "updated_at"),
        local_cursor_sql=(
            "SELECT updated_at, ticker, market "
            "FROM stock_master "
            "ORDER BY updated_at DESC, ticker DESC, market DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(6, 0, 1),
    ),
    TableSyncSpec(
        name="stock_master_snapshot",
        select_list="snapshot_id, as_of_date, source, fetched_at, record_count",
        from_clause="stock_master_snapshot",
        order_columns=("fetched_at", "snapshot_id"),
        insert_columns=("snapshot_id", "as_of_date", "source", "fetched_at", "record_count"),
        conflict_columns=("snapshot_id",),
        update_columns=("as_of_date", "source", "fetched_at", "record_count"),
        local_cursor_sql=(
            "SELECT fetched_at, snapshot_id "
            "FROM stock_master_snapshot "
            "ORDER BY fetched_at DESC, snapshot_id DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(3, 0),
    ),
    TableSyncSpec(
        name="stock_master_snapshot_items",
        select_list="i.snapshot_id, i.ticker, i.market, i.name, i.status, s.fetched_at",
        from_clause=(
            "stock_master_snapshot_items i "
            "JOIN stock_master_snapshot s ON s.snapshot_id = i.snapshot_id"
        ),
        order_columns=("s.fetched_at", "i.snapshot_id", "i.ticker", "i.market"),
        insert_columns=("snapshot_id", "ticker", "market", "name", "status"),
        conflict_columns=("snapshot_id", "ticker", "market"),
        update_columns=("name", "status"),
        local_cursor_sql=(
            "SELECT s.fetched_at, i.snapshot_id, i.ticker, i.market "
            "FROM stock_master_snapshot_items i "
            "JOIN stock_master_snapshot s ON s.snapshot_id = i.snapshot_id "
            "ORDER BY s.fetched_at DESC, i.snapshot_id DESC, i.ticker DESC, i.market DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(5, 0, 1, 2),
    ),
    TableSyncSpec(
        name="daily_ohlcv",
        select_list=(
            "trade_date, ticker, market, open, high, low, close, volume, source, fetched_at"
        ),
        from_clause="daily_ohlcv",
        order_columns=("fetched_at", "trade_date", "ticker", "market"),
        insert_columns=(
            "trade_date",
            "ticker",
            "market",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "source",
            "fetched_at",
        ),
        conflict_columns=("trade_date", "ticker", "market"),
        update_columns=("open", "high", "low", "close", "volume", "source", "fetched_at"),
        local_cursor_sql=(
            "SELECT fetched_at, trade_date, ticker, market "
            "FROM daily_ohlcv "
            "ORDER BY fetched_at DESC, trade_date DESC, ticker DESC, market DESC "
            "LIMIT 1"
        ),
        cursor_indexes=(9, 0, 1, 2),
    ),
)


def load_remote_db_info(path: str | Path) -> RemoteDbInfo:
    """Parse the secret metadata file for the remote PostgreSQL instance."""
    info_path = Path(path)
    values: dict[str, str] = {}

    for raw_line in info_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        values[key.strip().lower()] = value.strip()

    missing = [
        key
        for key in (
            "server host",
            "host port",
            "postgres_user",
            "postgres_password",
            "postgres_db",
        )
        if key not in values
    ]
    if missing:
        missing_fields = ", ".join(missing)
        raise ValueError(f"Missing required remote DB fields in {info_path}: {missing_fields}")

    return RemoteDbInfo(
        host=values["server host"],
        port=int(values["host port"]),
        db_name=values["postgres_db"],
        user=values["postgres_user"],
        password=values["postgres_password"],
        container=values.get("container"),
    )


@contextlib.contextmanager
def resolve_remote_dsn(
    *,
    db_info_path: str | Path,
    host_override: str | None = None,
    ssh_host: str | None = None,
    ssh_local_port: int | None = None,
) -> tuple[RemoteDbInfo, str]:
    """Yield the remote DB metadata and a connectable DSN.

    When ``ssh_host`` is provided, an SSH local-port forward is opened and
    the returned DSN points to ``127.0.0.1:<forwarded-port>``.
    """
    info = load_remote_db_info(db_info_path)

    if ssh_host:
        with _open_ssh_tunnel(
            ssh_host=ssh_host,
            remote_port=info.port,
            local_port=ssh_local_port,
        ) as forwarded_port:
            yield info, info.to_dsn(host_override="127.0.0.1", port_override=forwarded_port)
        return

    yield info, info.to_dsn(host_override=host_override)


def sync_remote_tables_to_local(
    *,
    remote_dsn: str,
    local_dsn: str,
    batch_size: int,
    full_refresh: bool,
) -> dict[str, int]:
    """Copy the supported remote tables into the local PostgreSQL database."""
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    results: dict[str, int] = {}
    with contextlib.closing(psycopg2.connect(remote_dsn)) as remote_conn:
        remote_conn.set_session(readonly=True, autocommit=False)
        with contextlib.closing(psycopg2.connect(local_dsn)) as local_conn:
            local_conn.autocommit = False
            if full_refresh:
                _prepare_local_full_refresh_session(local_conn)
                _truncate_target_tables(local_conn)

            for spec in SYNC_TABLE_SPECS:
                if spec.name == "daily_ohlcv":
                    copied = _sync_daily_ohlcv_via_copy(
                        remote_conn=remote_conn,
                        local_conn=local_conn,
                        spec=spec,
                        batch_size=batch_size,
                        full_refresh=full_refresh,
                    )
                else:
                    copied = _sync_table(
                        remote_conn=remote_conn,
                        local_conn=local_conn,
                        spec=spec,
                        batch_size=batch_size,
                        full_refresh=full_refresh,
                    )
                results[spec.name] = copied

    return results


def _truncate_target_tables(local_conn: Any) -> None:
    """Remove previously synced rows before a full refresh."""
    with local_conn.cursor() as cur:
        cur.execute(
            "TRUNCATE TABLE "
            "daily_ohlcv, stock_master_snapshot_items, stock_master_snapshot, stock_master"
        )
        cur.execute(
            "DELETE FROM sync_checkpoints WHERE sync_name = %s",
            (DAILY_OHLCV_SYNC_NAME,),
        )
    local_conn.commit()


def _sync_table(
    *,
    remote_conn: Any,
    local_conn: Any,
    spec: TableSyncSpec,
    batch_size: int,
    full_refresh: bool,
) -> int:
    """Copy one table in batches using a stable incremental cursor."""
    copied_rows = 0
    cursor_values = None if full_refresh else _get_local_cursor(local_conn=local_conn, spec=spec)

    while True:
        rows = _fetch_remote_rows(
            remote_conn=remote_conn,
            spec=spec,
            cursor_values=cursor_values,
            batch_size=batch_size,
        )
        if not rows:
            return copied_rows

        _upsert_rows(local_conn=local_conn, spec=spec, rows=rows)
        copied_rows += len(rows)
        cursor_values = tuple(rows[-1][index] for index in spec.cursor_indexes)


def _sync_daily_ohlcv_via_copy(
    *,
    remote_conn: Any,
    local_conn: Any,
    spec: TableSyncSpec,
    batch_size: int,
    full_refresh: bool,
) -> int:
    """Copy ``daily_ohlcv`` using ``COPY`` into a local temp staging table."""
    _ensure_daily_ohlcv_staging_table(local_conn)
    copied_rows = 0
    batch_number = 0
    cursor_values = None
    effective_batch_size = _effective_daily_ohlcv_batch_size(
        batch_size=batch_size,
        full_refresh=full_refresh,
    )

    if not full_refresh:
        checkpoint_cursor = _load_daily_ohlcv_checkpoint(local_conn)
        local_cursor = _get_local_cursor(local_conn=local_conn, spec=spec)
        cursor_values = _select_resume_cursor(checkpoint_cursor, local_cursor)

    query, params = _build_streaming_query(
        spec=spec,
        cursor_values=cursor_values,
        full_refresh=full_refresh,
    )
    remote_cursor_name = f"daily_ohlcv_sync_{int(time.time())}"

    try:
        with remote_conn.cursor(name=remote_cursor_name) as remote_cur:
            remote_cur.itersize = effective_batch_size
            remote_cur.execute(query, params)

            while True:
                started_at = time.monotonic()
                rows = remote_cur.fetchmany(effective_batch_size)
                if not rows:
                    break

                try:
                    _copy_daily_ohlcv_rows_to_staging(local_conn=local_conn, rows=rows)
                    if full_refresh:
                        _insert_daily_ohlcv_from_staging(local_conn)
                    else:
                        _merge_daily_ohlcv_from_staging(local_conn)
                        cursor_values = tuple(rows[-1][index] for index in spec.cursor_indexes)
                        _save_daily_ohlcv_checkpoint(local_conn, cursor_values)
                    local_conn.commit()
                except Exception:
                    local_conn.rollback()
                    raise

                copied_rows += len(rows)
                batch_number += 1
                elapsed = max(time.monotonic() - started_at, 0.001)
                if full_refresh:
                    logger.info(
                        "daily_ohlcv full-refresh batch=%s rows=%s total=%s rate=%.0f rows/s",
                        batch_number,
                        len(rows),
                        copied_rows,
                        len(rows) / elapsed,
                    )
                else:
                    logger.info(
                        "daily_ohlcv copy-sync batch=%s rows=%s total=%s "
                        "rate=%.0f rows/s cursor=%s",
                        batch_number,
                        len(rows),
                        copied_rows,
                        len(rows) / elapsed,
                        _format_cursor_for_log(cursor_values),
                    )
    finally:
        remote_conn.rollback()

    return copied_rows


def _get_local_cursor(*, local_conn: Any, spec: TableSyncSpec) -> tuple[Any, ...] | None:
    """Return the most recent local cursor state for a table."""
    with local_conn.cursor() as cur:
        cur.execute(spec.local_cursor_sql)
        row = cur.fetchone()

    if row is None:
        return None
    return tuple(row)


def _load_daily_ohlcv_checkpoint(local_conn: Any) -> tuple[Any, ...] | None:
    """Load the saved resume cursor for ``daily_ohlcv``."""
    with local_conn.cursor() as cur:
        cur.execute(
            "SELECT cursor_payload FROM sync_checkpoints WHERE sync_name = %s",
            (DAILY_OHLCV_SYNC_NAME,),
        )
        row = cur.fetchone()

    if row is None:
        return None

    payload = row[0]
    if isinstance(payload, str):
        payload = json.loads(payload)

    return (
        datetime.fromisoformat(payload["fetched_at"]),
        date.fromisoformat(payload["trade_date"]),
        payload["ticker"],
        payload["market"],
    )


def _save_daily_ohlcv_checkpoint(local_conn: Any, cursor_values: tuple[Any, ...]) -> None:
    """Persist the latest successfully merged ``daily_ohlcv`` cursor."""
    payload = _daily_ohlcv_checkpoint_payload(cursor_values)
    with local_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sync_checkpoints (sync_name, cursor_payload, updated_at)
            VALUES (%s, %s::jsonb, now())
            ON CONFLICT (sync_name) DO UPDATE SET
                cursor_payload = EXCLUDED.cursor_payload,
                updated_at = EXCLUDED.updated_at
            """,
            (DAILY_OHLCV_SYNC_NAME, json.dumps(payload)),
        )


def _daily_ohlcv_checkpoint_payload(cursor_values: tuple[Any, ...]) -> dict[str, str]:
    """Serialize a ``daily_ohlcv`` cursor tuple into JSON-friendly form."""
    fetched_at, trade_date, ticker, market = cursor_values
    return {
        "fetched_at": fetched_at.isoformat(),
        "trade_date": trade_date.isoformat(),
        "ticker": ticker,
        "market": market,
    }


def _select_resume_cursor(
    checkpoint_cursor: tuple[Any, ...] | None,
    local_cursor: tuple[Any, ...] | None,
) -> tuple[Any, ...] | None:
    """Choose the furthest-known resume cursor."""
    if checkpoint_cursor is None:
        return local_cursor
    if local_cursor is None:
        return checkpoint_cursor
    return max(checkpoint_cursor, local_cursor)


def _build_streaming_query(
    *,
    spec: TableSyncSpec,
    cursor_values: tuple[Any, ...] | None,
    full_refresh: bool,
) -> tuple[str, list[Any]]:
    """Build a streaming SELECT for named-cursor iteration."""
    if full_refresh:
        return f"SELECT {spec.select_list} FROM {spec.from_clause}", []

    predicate = ""
    params: list[Any] = []
    if cursor_values is not None:
        tuple_expr = ", ".join(spec.order_columns)
        placeholders = ", ".join(["%s"] * len(cursor_values))
        predicate = f"WHERE ({tuple_expr}) > ({placeholders})"
        params.extend(cursor_values)

    query = (
        f"SELECT {spec.select_list} "
        f"FROM {spec.from_clause} "
        f"{predicate} "
        f"ORDER BY {', '.join(spec.order_columns)}"
    )
    return query, params


def _fetch_remote_rows(
    *,
    remote_conn: Any,
    spec: TableSyncSpec,
    cursor_values: tuple[Any, ...] | None,
    batch_size: int,
) -> list[tuple[Any, ...]]:
    """Fetch the next batch from the remote table."""
    predicate = ""
    params: list[Any] = []

    if cursor_values is not None:
        tuple_expr = ", ".join(spec.order_columns)
        placeholders = ", ".join(["%s"] * len(cursor_values))
        predicate = f"WHERE ({tuple_expr}) > ({placeholders})"
        params.extend(cursor_values)

    query = (
        f"SELECT {spec.select_list} "
        f"FROM {spec.from_clause} "
        f"{predicate} "
        f"ORDER BY {', '.join(spec.order_columns)} "
        f"LIMIT %s"
    )
    params.append(batch_size)

    with remote_conn.cursor() as cur:
        cur.execute(query, params)
        return list(cur.fetchall())


def _ensure_daily_ohlcv_staging_table(local_conn: Any) -> None:
    """Create the temp staging table used by ``COPY``."""
    with local_conn.cursor() as cur:
        cur.execute(f"""
            CREATE TEMP TABLE IF NOT EXISTS {DAILY_OHLCV_STAGING_TABLE} (
                trade_date  DATE        NOT NULL,
                ticker      TEXT        NOT NULL,
                market      TEXT        NOT NULL,
                open        BIGINT      NOT NULL,
                high        BIGINT      NOT NULL,
                low         BIGINT      NOT NULL,
                close       BIGINT      NOT NULL,
                volume      BIGINT      NOT NULL,
                source      TEXT        NOT NULL,
                fetched_at  TIMESTAMPTZ NOT NULL
            ) ON COMMIT DELETE ROWS
            """)


def _copy_daily_ohlcv_rows_to_staging(*, local_conn: Any, rows: list[tuple[Any, ...]]) -> None:
    """Bulk load a batch into the temp staging table via ``COPY``."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for row in rows:
        writer.writerow(_serialize_copy_row(row))
    buffer.seek(0)

    copy_sql = f"""
        COPY {DAILY_OHLCV_STAGING_TABLE} (
            trade_date, ticker, market, open, high, low, close, volume, source, fetched_at
        )
        FROM STDIN WITH (FORMAT CSV, NULL '\\N')
    """

    with local_conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {DAILY_OHLCV_STAGING_TABLE}")
        cur.copy_expert(copy_sql, buffer)


def _insert_daily_ohlcv_from_staging(local_conn: Any) -> None:
    """Insert staged rows into an empty ``daily_ohlcv`` target."""
    with local_conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO daily_ohlcv (
                trade_date, ticker, market, open, high, low, close, volume, source, fetched_at
            )
            SELECT
                trade_date, ticker, market, open, high, low, close, volume, source, fetched_at
            FROM {DAILY_OHLCV_STAGING_TABLE}
            """)


def _merge_daily_ohlcv_from_staging(local_conn: Any) -> None:
    """Merge staged ``daily_ohlcv`` rows into the target table."""
    with local_conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO daily_ohlcv (
                trade_date, ticker, market, open, high, low, close, volume, source, fetched_at
            )
            SELECT
                trade_date, ticker, market, open, high, low, close, volume, source, fetched_at
            FROM {DAILY_OHLCV_STAGING_TABLE}
            ON CONFLICT (trade_date, ticker, market) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                source = EXCLUDED.source,
                fetched_at = EXCLUDED.fetched_at
            WHERE daily_ohlcv.fetched_at <= EXCLUDED.fetched_at
            """)


def _serialize_copy_row(row: tuple[Any, ...]) -> list[Any]:
    """Serialize a DB row into CSV-friendly values for ``COPY``."""
    return [_serialize_copy_value(value) for value in row]


def _serialize_copy_value(value: Any) -> Any:
    """Serialize one value for ``COPY FROM STDIN``."""
    if value is None:
        return "\\N"
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _format_cursor_for_log(cursor_values: tuple[Any, ...] | None) -> str:
    """Format cursor values for compact progress logging."""
    if cursor_values is None:
        return "None"
    return ", ".join(str(value) for value in cursor_values)


def _prepare_local_full_refresh_session(local_conn: Any) -> None:
    """Relax durability for this dedicated full-refresh session."""
    with local_conn.cursor() as cur:
        cur.execute("SET synchronous_commit = OFF")


def _effective_daily_ohlcv_batch_size(*, batch_size: int, full_refresh: bool) -> int:
    """Return the effective batch size for ``daily_ohlcv`` sync."""
    if full_refresh:
        return max(batch_size, FULL_REFRESH_DAILY_OHLCV_BATCH_SIZE)
    return batch_size


def _upsert_rows(*, local_conn: Any, spec: TableSyncSpec, rows: list[tuple[Any, ...]]) -> None:
    """Upsert a batch into the local table."""
    assignments = ", ".join(f"{column} = EXCLUDED.{column}" for column in spec.update_columns)
    insert_columns = ", ".join(spec.insert_columns)
    conflict_columns = ", ".join(spec.conflict_columns)
    values = [row[: len(spec.insert_columns)] for row in rows]

    statement = (
        f"INSERT INTO {spec.name} ({insert_columns}) "
        f"VALUES %s "
        f"ON CONFLICT ({conflict_columns}) DO UPDATE SET {assignments}"
    )

    try:
        with local_conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                statement,
                values,
                page_size=min(len(values), 1000),
            )
        local_conn.commit()
    except Exception:
        local_conn.rollback()
        raise


@contextlib.contextmanager
def _open_ssh_tunnel(
    *,
    ssh_host: str,
    remote_port: int,
    local_port: int | None,
) -> int:
    """Open an SSH tunnel to the remote PostgreSQL host."""
    forwarded_port = local_port or _find_free_port()
    cmd = [
        "ssh",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ServerAliveInterval=30",
        "-N",
        "-L",
        f"{forwarded_port}:127.0.0.1:{remote_port}",
        ssh_host,
    ]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        _wait_for_local_port(proc=proc, local_port=forwarded_port)
        yield forwarded_port
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def _wait_for_local_port(
    *,
    proc: subprocess.Popen[str],
    local_port: int,
    timeout_seconds: float = 5.0,
) -> None:
    """Wait until the forwarded local port is accepting connections."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            stderr = proc.stderr.read().strip() if proc.stderr else ""
            raise RuntimeError(f"SSH tunnel process exited early: {stderr or 'no stderr output'}")

        try:
            with socket.create_connection(("127.0.0.1", local_port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)

    raise TimeoutError(f"Timed out waiting for SSH tunnel on local port {local_port}")


def _find_free_port() -> int:
    """Return an available local TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
