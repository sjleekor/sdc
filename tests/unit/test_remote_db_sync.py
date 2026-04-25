from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from krx_collector.infra.db_postgres.remote_sync import (
    DatabaseTable,
    _copy_status_row_count,
    _daily_ohlcv_checkpoint_payload,
    _effective_daily_ohlcv_batch_size,
    _select_resume_cursor,
    _sort_tables_by_fk_dependencies,
    _validate_full_database_table_sets,
    load_remote_db_info,
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


def test_effective_daily_ohlcv_batch_size_is_boosted_for_full_refresh() -> None:
    assert _effective_daily_ohlcv_batch_size(batch_size=50_000, full_refresh=False) == 50_000
    assert _effective_daily_ohlcv_batch_size(batch_size=50_000, full_refresh=True) == 200_000
    assert _effective_daily_ohlcv_batch_size(batch_size=300_000, full_refresh=True) == 300_000
