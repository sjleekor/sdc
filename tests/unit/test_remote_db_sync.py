from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from krx_collector.infra.db_postgres import remote_sync
from krx_collector.infra.db_postgres.remote_sync import (
    PIPELINE_FULL_REFRESH_TABLES,
    DatabaseTable,
    _copy_status_row_count,
    _daily_ohlcv_checkpoint_payload,
    _effective_daily_ohlcv_batch_size,
    _select_required_public_tables,
    _select_resume_cursor,
    _sort_tables_by_fk_dependencies,
    _validate_full_database_table_sets,
    load_remote_db_info,
    reset_local_public_tables,
    sync_remote_tables_to_local,
)


def test_load_remote_db_info_parses_expected_fields(tmp_path: Path) -> None:
    info_file = tmp_path / "db_info"
    info_file.write_text(
        "\n".join(
            [
                "# sj2-server DB Information",
                "Server Host: sj2-server",
                "Host Port: 5432",
                "Container: sdc-postgres",
                "",
                "# Credentials",
                "POSTGRES_USER: krx_user",
                "POSTGRES_PASSWORD: secret!",
                "POSTGRES_DB: krx_data",
            ]
        ),
        encoding="utf-8",
    )

    info = load_remote_db_info(info_file)

    assert info.host == "sj2-server"
    assert info.port == 5432
    assert info.container == "sdc-postgres"
    assert info.db_name == "krx_data"
    assert info.to_dsn() == "postgresql://krx_user:secret%21@sj2-server:5432/krx_data"


def test_load_remote_db_info_requires_expected_fields(tmp_path: Path) -> None:
    info_file = tmp_path / "db_info"
    info_file.write_text("Server Host: sj2-server\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Missing required remote DB fields"):
        load_remote_db_info(info_file)


def test_sync_remote_tables_requires_positive_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        sync_remote_tables_to_local(
            remote_dsn="postgresql://remote",
            local_dsn="postgresql://local",
            batch_size=0,
            full_refresh=False,
        )


def test_all_tables_sync_requires_full_refresh() -> None:
    with pytest.raises(ValueError, match="all_tables sync requires full_refresh=True"):
        sync_remote_tables_to_local(
            remote_dsn="postgresql://remote",
            local_dsn="postgresql://local",
            batch_size=1000,
            full_refresh=False,
            all_tables=True,
        )


def test_validate_full_database_table_sets_reports_mismatches() -> None:
    remote_tables = (
        DatabaseTable(schema="public", name="stock_master"),
        DatabaseTable(schema="public", name="daily_ohlcv"),
    )
    local_tables = (
        DatabaseTable(schema="public", name="stock_master"),
        DatabaseTable(schema="public", name="ingestion_runs"),
    )

    with pytest.raises(ValueError, match="missing locally: daily_ohlcv"):
        _validate_full_database_table_sets(remote_tables=remote_tables, local_tables=local_tables)


def test_select_required_public_tables_allows_extra_public_tables() -> None:
    required = (
        DatabaseTable(schema="public", name="stock_master"),
        DatabaseTable(schema="public", name="daily_ohlcv"),
    )
    remote_tables = required + (DatabaseTable(schema="public", name="custom_remote"),)
    local_tables = required + (DatabaseTable(schema="public", name="custom_local"),)

    selected = _select_required_public_tables(
        remote_tables=remote_tables,
        local_tables=local_tables,
        required_tables=required,
    )

    assert selected == required


def test_select_required_public_tables_reports_missing_targets() -> None:
    required = (
        DatabaseTable(schema="public", name="stock_master"),
        DatabaseTable(schema="public", name="daily_ohlcv"),
    )

    with pytest.raises(ValueError, match="missing locally: daily_ohlcv"):
        _select_required_public_tables(
            remote_tables=required,
            local_tables=(DatabaseTable(schema="public", name="stock_master"),),
            required_tables=required,
        )


def test_sort_tables_by_fk_dependencies_copies_parents_first() -> None:
    parent = DatabaseTable(schema="public", name="stock_master_snapshot")
    child = DatabaseTable(schema="public", name="stock_master_snapshot_items")
    unrelated = DatabaseTable(schema="public", name="daily_ohlcv")

    ordered = _sort_tables_by_fk_dependencies(
        tables=(child, unrelated, parent),
        dependencies=((child, parent),),
    )

    assert ordered.index(parent) < ordered.index(child)
    assert set(ordered) == {parent, child, unrelated}


def test_copy_status_row_count_parses_copy_status() -> None:
    assert _copy_status_row_count("COPY 123") == 123
    assert _copy_status_row_count("") is None


def test_daily_ohlcv_checkpoint_payload_serializes_cursor() -> None:
    cursor = (
        datetime(2026, 4, 19, 0, 0, tzinfo=UTC),
        date(2026, 4, 18),
        "005930",
        "KOSPI",
    )

    payload = _daily_ohlcv_checkpoint_payload(cursor)

    assert payload == {
        "fetched_at": "2026-04-19T00:00:00+00:00",
        "trade_date": "2026-04-18",
        "ticker": "005930",
        "market": "KOSPI",
    }


def test_select_resume_cursor_prefers_furthest_cursor() -> None:
    checkpoint_cursor = (
        datetime(2026, 4, 19, 0, 0, tzinfo=UTC),
        date(2026, 4, 18),
        "005930",
        "KOSPI",
    )
    local_cursor = (
        datetime(2026, 4, 18, 23, 0, tzinfo=UTC),
        date(2026, 4, 18),
        "000660",
        "KOSPI",
    )

    assert _select_resume_cursor(checkpoint_cursor, local_cursor) == checkpoint_cursor
    assert _select_resume_cursor(None, local_cursor) == local_cursor
    assert _select_resume_cursor(checkpoint_cursor, None) == checkpoint_cursor


class _FakeCursor:
    def __init__(self, table_names: list[str]) -> None:
        self._table_names = table_names
        self.executed: list[tuple[object, object]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def execute(self, statement, params=None) -> None:
        self.executed.append((statement, params))

    def fetchall(self) -> list[tuple[str]]:
        return [(name,) for name in self._table_names]


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.autocommit = False
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


def _identifier_pairs(statement) -> list[tuple[str, ...]]:
    """Pull the wrapped strings out of every Identifier in a Composable."""
    from psycopg2 import sql as psycopg2_sql

    pairs: list[tuple[str, ...]] = []
    if isinstance(statement, psycopg2_sql.Identifier):
        pairs.append(tuple(statement.strings))
    elif isinstance(statement, psycopg2_sql.Composed):
        for part in statement.seq:
            pairs.extend(_identifier_pairs(part))
    return pairs


def test_reset_local_public_tables_drops_only_pipeline_tables(monkeypatch) -> None:
    cursor = _FakeCursor(["stock_master", "daily_ohlcv", "custom_table"])
    connection = _FakeConnection(cursor)

    monkeypatch.setattr(remote_sync.psycopg2, "connect", lambda dsn: connection)

    dropped = reset_local_public_tables("postgresql://local")

    assert dropped == 2
    assert connection.autocommit is True
    assert connection.closed is True

    select_stmt, select_params = cursor.executed[0]
    assert "FROM pg_tables" in select_stmt
    assert select_params == (
        "public",
        [table.name for table in PIPELINE_FULL_REFRESH_TABLES],
    )

    drop_targets = [
        _identifier_pairs(stmt) for stmt, _ in cursor.executed[1:]
    ]
    assert drop_targets == [
        [("public", "stock_master"), ("public", "daily_ohlcv")],
    ]


def test_reset_local_public_tables_handles_empty_db(monkeypatch) -> None:
    cursor = _FakeCursor([])
    connection = _FakeConnection(cursor)

    monkeypatch.setattr(remote_sync.psycopg2, "connect", lambda dsn: connection)

    assert reset_local_public_tables("postgresql://local") == 0
    assert len(cursor.executed) == 1


def test_effective_daily_ohlcv_batch_size_is_boosted_for_full_refresh() -> None:
    assert _effective_daily_ohlcv_batch_size(batch_size=50_000, full_refresh=False) == 50_000
    assert _effective_daily_ohlcv_batch_size(batch_size=50_000, full_refresh=True) == 200_000
    assert _effective_daily_ohlcv_batch_size(batch_size=300_000, full_refresh=True) == 300_000
