"""Port: Storage / repository interface.

Defines the contract for persisting domain objects.  The primary
implementation targets PostgreSQL, but the protocol is storage-agnostic
so that a file-based backend (CSV / Parquet) can be swapped in without
touching core logic.
"""

from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable

from krx_collector.domain.enums import Market
from krx_collector.domain.models import (
    DailyBar,
    IngestionRun,
    Stock,
    StockUniverseSnapshot,
    UpsertResult,
)


@runtime_checkable
class Storage(Protocol):
    """Repository / unit-of-work style storage port.

    Implementations:
        - ``PostgresStorage`` (infra/db_postgres/repositories.py)
        - Future: ``FileStorage`` (CSV / Parquet writer)
    """

    # -- Schema management ----------------------------------------------------

    def init_schema(self) -> None:
        """Create tables / ensure schema is up to date.

        For PostgreSQL this executes the DDL from ``sql/postgres_ddl.sql``.
        For file-based storage this may create directories.
        """
        ...

    # -- Stock master ---------------------------------------------------------

    def upsert_stock_master(
        self,
        stocks: list[Stock],
        snapshot: StockUniverseSnapshot,
    ) -> UpsertResult:
        """Upsert stock master rows and persist the snapshot for audit.

        Args:
            stocks: Current universe records to upsert.
            snapshot: The snapshot metadata (persisted to
                ``stock_master_snapshot`` + ``stock_master_snapshot_items``).

        Returns:
            Counters of inserted / updated / errored rows.
        """
        ...

    def get_active_stocks(self, market: Market | None = None) -> list[Stock]:
        """Return the currently active stocks in the stock master.

        Used to compute diffs (new/delisted) during universe sync.

        Args:
            market: Optional market filter. If None, returns all markets.

        Returns:
            List of active stocks.
        """
        ...

    # -- Daily OHLCV ----------------------------------------------------------

    def upsert_daily_bars(self, bars: list[DailyBar]) -> UpsertResult:
        """Upsert daily OHLCV bars.

        Uses ``INSERT … ON CONFLICT (trade_date, ticker, market) DO UPDATE``
        so that re-fetched data overwrites stale rows (source corrections
        propagate automatically).

        Args:
            bars: Daily bars to persist.

        Returns:
            Counters of inserted / updated / errored rows.
        """
        ...

    # -- Ingestion runs -------------------------------------------------------

    def record_run(self, run: IngestionRun) -> None:
        """Insert or update an ingestion-run audit record.

        Called at the start (status=running) and end (status=success|failed)
        of each pipeline execution.
        """
        ...

    # -- Query helpers --------------------------------------------------------

    def get_daily_bars(self, target_date: date, market: Market | None = None) -> list[DailyBar]:
        """Return all daily bars for a given date.

        Used for validation.

        Args:
            target_date: Date to query.
            market: Optional market filter.

        Returns:
            List of daily bars.
        """
        ...
