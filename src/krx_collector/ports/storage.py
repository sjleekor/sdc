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
    DartCorp,
    DartFinancialStatementLine,
    OperatingMetricFact,
    OperatingSourceDocument,
    SecurityFlowLine,
    DartXbrlDocument,
    DartXbrlFactLine,
    MetricCatalogEntry,
    MetricMappingRule,
    StockMetricFact,
    DartShareCountLine,
    DartShareholderReturnLine,
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

    def upsert_dart_corp_master(self, records: list[DartCorp]) -> UpsertResult:
        """Upsert OpenDART corporation-code master rows.

        Args:
            records: OpenDART corp-code records enriched with optional
                KRX market/name linkage.

        Returns:
            Counters of inserted / updated / errored rows.
        """
        ...

    def get_dart_corp_master(
        self,
        active_only: bool = True,
        tickers: list[str] | None = None,
    ) -> list[DartCorp]:
        """Return OpenDART corp-code rows mapped to local tickers.

        Args:
            active_only: If ``True``, restrict to rows matched to active
                ``stock_master`` records.
            tickers: Optional ticker allowlist.

        Returns:
            List of OpenDART corp-code rows.
        """
        ...

    def upsert_dart_financial_statement_raw(
        self,
        records: list[DartFinancialStatementLine],
    ) -> UpsertResult:
        """Upsert OpenDART financial-statement raw rows."""
        ...

    def upsert_dart_share_count_raw(
        self,
        records: list[DartShareCountLine],
    ) -> UpsertResult:
        """Upsert OpenDART share-count raw rows."""
        ...

    def upsert_dart_shareholder_return_raw(
        self,
        records: list[DartShareholderReturnLine],
    ) -> UpsertResult:
        """Upsert OpenDART dividend / treasury-stock raw rows."""
        ...

    def upsert_dart_xbrl_documents(
        self,
        records: list[DartXbrlDocument],
    ) -> UpsertResult:
        """Upsert OpenDART XBRL document metadata rows."""
        ...

    def upsert_dart_xbrl_fact_raw(
        self,
        records: list[DartXbrlFactLine],
    ) -> UpsertResult:
        """Upsert parsed OpenDART XBRL fact rows."""
        ...

    def upsert_krx_security_flow_raw(
        self,
        records: list[SecurityFlowLine],
    ) -> UpsertResult:
        """Upsert KRX / pykrx security-flow raw rows."""
        ...

    def upsert_operating_source_documents(
        self,
        records: list[OperatingSourceDocument],
    ) -> UpsertResult:
        """Upsert operating KPI source documents."""
        ...

    def upsert_operating_metric_facts(
        self,
        records: list[OperatingMetricFact],
    ) -> UpsertResult:
        """Upsert extracted operating KPI facts."""
        ...

    def upsert_metric_catalog(self, records: list[MetricCatalogEntry]) -> UpsertResult:
        """Upsert canonical metric catalog entries."""
        ...

    def replace_metric_mapping_rules(self, records: list[MetricMappingRule]) -> UpsertResult:
        """Replace the active metric mapping rules with the provided set."""
        ...

    def get_metric_mapping_rules(self) -> list[MetricMappingRule]:
        """Return active metric mapping rules ordered by priority."""
        ...

    def get_dart_financial_statement_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartFinancialStatementLine]:
        """Return financial statement raw rows for normalization."""
        ...

    def get_dart_share_count_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartShareCountLine]:
        """Return share-count raw rows for normalization."""
        ...

    def get_dart_shareholder_return_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartShareholderReturnLine]:
        """Return dividend / treasury-stock raw rows for normalization."""
        ...

    def get_dart_xbrl_fact_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartXbrlFactLine]:
        """Return parsed OpenDART XBRL fact rows for normalization."""
        ...

    def upsert_stock_metric_facts(self, records: list[StockMetricFact]) -> UpsertResult:
        """Upsert normalized canonical metric facts."""
        ...

    def get_metric_catalog_entries(self) -> list[MetricCatalogEntry]:
        """Return active metric catalog entries."""
        ...

    def get_stock_metric_facts(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[StockMetricFact]:
        """Return normalized canonical metric facts for coverage queries."""
        ...

    def get_operating_metric_facts(
        self,
        tickers: list[str] | None = None,
        sector_keys: list[str] | None = None,
    ) -> list[OperatingMetricFact]:
        """Return extracted operating KPI facts."""
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

    def query_missing_days(
        self,
        ticker: str,
        start: date,
        end: date,
    ) -> list[date]:
        """Return trade dates in [start, end] that have no daily bar stored.

        This is an *optional* optimisation for incremental backfill.  A
        minimal implementation may return all dates in the range.

        Args:
            ticker: 6-digit KRX ticker code.
            start: Range start (inclusive).
            end: Range end (inclusive).

        Returns:
            Sorted list of missing dates.
        """
        ...

    def get_min_trade_date(self, ticker: str) -> date | None:
        """Return the earliest stored ``trade_date`` for *ticker*.

        Used by the backfill service to clamp the effective start date
        so that ranges before the ticker's known data start are not
        re-requested on every run.

        Args:
            ticker: 6-digit KRX ticker code.

        Returns:
            The minimum stored ``trade_date`` for the ticker, or ``None``
            if no rows exist yet.
        """
        ...

    def get_max_trade_date(self, ticker: str) -> date | None:
        """Return the latest stored ``trade_date`` for *ticker*.

        Used by the backfill service in incremental mode to fetch only
        days after the ticker's last known trade date.

        Args:
            ticker: 6-digit KRX ticker code.

        Returns:
            The maximum stored ``trade_date`` for the ticker, or ``None``
            if no rows exist yet.
        """
        ...
