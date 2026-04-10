"""PostgreSQL storage implementation.

Implements the :class:`~krx_collector.ports.storage.Storage` protocol
using ``psycopg2`` against the schema defined in ``sql/postgres_ddl.sql``.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import psycopg2.extras

from krx_collector.domain.enums import ListingStatus, Market, Source
from krx_collector.domain.models import (
    DailyBar,
    IngestionRun,
    Stock,
    StockUniverseSnapshot,
    UpsertResult,
)
from krx_collector.infra.db_postgres.connection import get_connection

logger = logging.getLogger(__name__)


class PostgresStorage:
    """PostgreSQL-backed storage conforming to the ``Storage`` protocol.

    Args:
        dsn: PostgreSQL connection string.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    # -- Schema management ----------------------------------------------------

    def init_schema(self) -> None:
        """Execute ``sql/postgres_ddl.sql`` to create / update tables."""
        # Find sql file relative to project root
        # Since this code is in src/krx_collector/infra/db_postgres,
        # we can go up 4 levels and into 'sql'.
        sql_path = Path(__file__).parent.parent.parent.parent.parent / "sql" / "postgres_ddl.sql"
        if not sql_path.exists():
            logger.error("DDL file not found at %s", sql_path)
            raise FileNotFoundError(f"DDL file not found at {sql_path}")

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_path.read_text(encoding="utf-8"))
        logger.info("Schema initialized successfully.")

    # -- Stock master ---------------------------------------------------------

    def upsert_stock_master(
        self,
        stocks: list[Stock],
        snapshot: StockUniverseSnapshot,
    ) -> UpsertResult:
        """Upsert stock_master rows and persist the snapshot."""
        if not stocks:
            return UpsertResult()

        result = UpsertResult()

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                # 1. Insert snapshot metadata
                cur.execute(
                    """
                    INSERT INTO stock_master_snapshot (snapshot_id, as_of_date, source, fetched_at, record_count)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        snapshot.snapshot_id,
                        snapshot.as_of_date,
                        snapshot.source.value,
                        snapshot.fetched_at,
                        snapshot.record_count,
                    )
                )

                # 2. Insert snapshot items
                snapshot_items_args = [
                    (
                        snapshot.snapshot_id,
                        s.ticker,
                        s.market.value,
                        s.name,
                        s.listing_date,
                        s.status.value,
                    )
                    for s in snapshot.records
                ]
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO stock_master_snapshot_items (snapshot_id, ticker, market, name, listing_date, status)
                    VALUES %s
                    ON CONFLICT (snapshot_id, ticker, market) DO NOTHING
                    """,
                    snapshot_items_args,
                    page_size=1000
                )

                # 3. Upsert stock_master
                master_args = [
                    (
                        s.ticker,
                        s.market.value,
                        s.name,
                        s.listing_date,
                        s.status.value,
                        s.last_seen_date,
                        s.source.value,
                    )
                    for s in stocks
                ]

                # psycopg2 execute_values doesn't easily tell us inserted vs updated count directly
                # We will approximate or just use rowcount for total affected.
                # Actually, DO UPDATE returns the affected rows if we append RETURNING.

                # To accurately count, we can do a standard execute_values
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO stock_master (ticker, market, name, listing_date, status, last_seen_date, source)
                    VALUES %s
                    ON CONFLICT (ticker, market) DO UPDATE SET
                        name = EXCLUDED.name,
                        listing_date = COALESCE(EXCLUDED.listing_date, stock_master.listing_date),
                        status = EXCLUDED.status,
                        last_seen_date = EXCLUDED.last_seen_date,
                        source = EXCLUDED.source,
                        updated_at = now()
                    """,
                    master_args,
                    page_size=1000
                )
                # Approximation: we can just count total as updated for upsert
                result.updated = cur.rowcount

        return result

    def get_active_stocks(self, market: Market | None = None) -> list[Stock]:
        """Return currently active stocks from stock_master."""
        stocks = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                if market:
                    cur.execute(
                        "SELECT * FROM stock_master WHERE status = 'ACTIVE' AND market = %s",
                        (market.value,)
                    )
                else:
                    cur.execute("SELECT * FROM stock_master WHERE status = 'ACTIVE'")

                for row in cur.fetchall():
                    stocks.append(
                        Stock(
                            ticker=row["ticker"],
                            market=Market(row["market"]),
                            name=row["name"],
                            listing_date=row["listing_date"],
                            status=ListingStatus(row["status"]),
                            last_seen_date=row["last_seen_date"],
                            source=Source(row["source"]),
                        )
                    )
        return stocks

    def get_listing_date(self, ticker: str) -> date | None:
        """Query stock_master for the listing date."""
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT listing_date FROM stock_master WHERE ticker = %s", (ticker,)
                )
                row = cur.fetchone()
                if row:
                    return row[0]
                return None

    # -- Daily OHLCV ----------------------------------------------------------

    def upsert_daily_bars(self, bars: list[DailyBar]) -> UpsertResult:
        """Upsert daily OHLCV bars."""
        if not bars:
            return UpsertResult()

        result = UpsertResult()

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                args = [
                    (
                        b.trade_date,
                        b.ticker,
                        b.market.value,
                        b.open,
                        b.high,
                        b.low,
                        b.close,
                        b.volume,
                        b.source.value,
                        b.fetched_at,
                    )
                    for b in bars
                ]

                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO daily_ohlcv (trade_date, ticker, market, open, high, low, close, volume, source, fetched_at)
                    VALUES %s
                    ON CONFLICT (trade_date, ticker, market) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at
                    """,
                    args,
                    page_size=1000
                )
                result.updated = cur.rowcount

        return result

    # -- Ingestion runs -------------------------------------------------------

    def record_run(self, run: IngestionRun) -> None:
        """Insert or update an ingestion-run audit record."""
        import json

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingestion_runs (run_id, run_type, started_at, ended_at, status, params, counts, error_summary)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id) DO UPDATE SET
                        ended_at = EXCLUDED.ended_at,
                        status = EXCLUDED.status,
                        params = EXCLUDED.params,
                        counts = EXCLUDED.counts,
                        error_summary = EXCLUDED.error_summary
                    """,
                    (
                        run.run_id,
                        run.run_type.value,
                        run.started_at,
                        run.ended_at,
                        run.status.value,
                        json.dumps(run.params) if run.params else None,
                        json.dumps(run.counts) if run.counts else None,
                        run.error_summary,
                    )
                )

    # -- Query helpers --------------------------------------------------------

    def get_daily_bars(self, target_date: date, market: Market | None = None) -> list[DailyBar]:
        """Return all daily bars for a given date."""
        bars = []
        with get_connection(self._dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                if market:
                    cur.execute(
                        "SELECT * FROM daily_ohlcv WHERE trade_date = %s AND market = %s",
                        (target_date, market.value)
                    )
                else:
                    cur.execute(
                        "SELECT * FROM daily_ohlcv WHERE trade_date = %s",
                        (target_date,)
                    )

                for row in cur.fetchall():
                    bars.append(
                        DailyBar(
                            ticker=row["ticker"],
                            market=Market(row["market"]),
                            trade_date=row["trade_date"],
                            open=row["open"],
                            high=row["high"],
                            low=row["low"],
                            close=row["close"],
                            volume=row["volume"],
                            source=Source(row["source"]),
                            fetched_at=row["fetched_at"],
                        )
                    )
        return bars
