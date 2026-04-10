"""Domain models for the KRX data pipeline.

All models are plain dataclasses — no framework dependency — so that the
domain layer remains pure and testable without infrastructure concerns.

Design choices:
    • OHLCV prices use ``int`` (Korean won has no sub-unit for equities).
      If fractional values are ever needed (e.g., index points), switch to
      ``Decimal`` and update the DDL column type accordingly.
    • ``StockUniverseSnapshot.records`` stores the full list so that
      snapshot-items can be persisted for audit/diff purposes.
    • Timestamps (``fetched_at``, ``started_at``, …) are timezone-aware
      ``datetime`` objects in Asia/Seoul.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime

from krx_collector.domain.enums import (
    ListingStatus,
    Market,
    RunStatus,
    RunType,
    Source,
)


@dataclass(frozen=True, slots=True)
class Stock:
    """A single listed stock on KRX.

    Attributes:
        ticker: 6-digit KRX ticker code (e.g. ``"005930"``).
        market: Exchange market segment.
        name: Korean company name.
        listing_date: IPO / listing date (``None`` if unknown).
        status: Current listing status.
        last_seen_date: Date when the stock was last observed in a universe fetch.
        source: Data source that provided this record.
    """

    ticker: str
    market: Market
    name: str
    listing_date: date | None
    status: ListingStatus
    last_seen_date: date
    source: Source


@dataclass(frozen=True, slots=True)
class StockUniverseSnapshot:
    """Point-in-time snapshot of the stock universe.

    Stores metadata about a single universe-fetch run and the full list of
    ``Stock`` records captured.  The ``records`` list is persisted into
    ``stock_master_snapshot_items`` for auditability — enabling diffs between
    snapshots to detect new listings, delistings, or name changes.

    Attributes:
        snapshot_id: Unique identifier (UUID).
        as_of_date: The reference date for the universe.
        source: Data source used.
        fetched_at: KST timestamp when the data was retrieved.
        records: Full list of stocks in this snapshot.
    """

    snapshot_id: str
    as_of_date: date
    source: Source
    fetched_at: datetime
    records: list[Stock]

    @property
    def record_count(self) -> int:
        """Number of stocks in this snapshot."""
        return len(self.records)


@dataclass(frozen=True, slots=True)
class DailyBar:
    """Single daily OHLCV bar for a stock.

    Attributes:
        ticker: 6-digit KRX ticker code.
        market: Exchange market segment.
        trade_date: Trading date.
        open: Opening price (KRW, integer).
        high: High price.
        low: Low price.
        close: Closing price.
        volume: Traded volume.
        source: Data source.
        fetched_at: KST timestamp when the data was retrieved.
    """

    ticker: str
    market: Market
    trade_date: date
    open: int
    high: int
    low: int
    close: int
    volume: int
    source: Source
    fetched_at: datetime


# ---------------------------------------------------------------------------
# Result / aggregate types
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class UpsertResult:
    """Outcome counters for an upsert operation.

    Attributes:
        inserted: Number of newly inserted rows.
        updated: Number of rows updated (conflict resolved).
        errors: Number of rows that failed.
    """

    inserted: int = 0
    updated: int = 0
    errors: int = 0


@dataclass(slots=True)
class UniverseResult:
    """Result of a universe-fetch operation.

    Attributes:
        snapshot: The captured snapshot (may be ``None`` on failure).
        error: Error message if the fetch failed.
    """

    snapshot: StockUniverseSnapshot | None = None
    error: str | None = None


@dataclass(slots=True)
class DailyPriceResult:
    """Result of a daily-price fetch for one ticker.

    Attributes:
        ticker: Ticker that was fetched.
        bars: List of daily bars retrieved.
        error: Error message if the fetch failed.
    """

    ticker: str = ""
    bars: list[DailyBar] = field(default_factory=list)
    error: str | None = None


@dataclass(slots=True)
class SyncResult:
    """Outcome of a universe-sync use-case run.

    Attributes:
        upsert: Aggregated upsert counters.
        new_tickers: Tickers not previously in stock_master.
        delisted_tickers: Tickers no longer in the fetched universe.
        error: Error message if something went wrong.
    """

    upsert: UpsertResult = field(default_factory=UpsertResult)
    new_tickers: list[str] = field(default_factory=list)
    delisted_tickers: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass(slots=True)
class BackfillResult:
    """Outcome of a daily-backfill use-case run.

    Attributes:
        tickers_processed: Number of tickers attempted.
        bars_upserted: Total bars written.
        errors: Per-ticker error messages.
    """

    tickers_processed: int = 0
    bars_upserted: int = 0
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class IngestionRun:
    """Metadata for a single pipeline execution (persisted to ingestion_runs).

    Attributes:
        run_id: UUID string.
        run_type: Category of the run.
        started_at: KST start time.
        ended_at: KST end time (``None`` while running).
        status: Current run status.
        params: Arbitrary run parameters (JSON-serialisable dict).
        counts: Aggregated counters (JSON-serialisable dict).
        error_summary: Human-readable error summary.
    """

    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    run_type: RunType = RunType.UNIVERSE_SYNC
    started_at: datetime | None = None
    ended_at: datetime | None = None
    status: RunStatus = RunStatus.RUNNING
    params: dict[str, object] | None = None
    counts: dict[str, int] | None = None
    error_summary: str | None = None
