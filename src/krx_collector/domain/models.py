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
from decimal import Decimal

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
        status: Current listing status.
        last_seen_date: Date when the stock was last observed in a universe fetch.
        source: Data source that provided this record.
    """

    ticker: str
    market: Market
    name: str
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


@dataclass(frozen=True, slots=True)
class DartCorp:
    """OpenDART corporation-code master row.

    Attributes:
        corp_code: OpenDART corporation code (8 digits).
        corp_name: Legal company name from OpenDART.
        ticker: 6-digit KRX ticker code if the company is listed.
        market: KRX market segment when matched to ``stock_master``.
        stock_name: KRX stock name when available.
        modify_date: Last modified date from the OpenDART master file.
        is_active: Whether the ticker is currently active in ``stock_master``.
        source: Data source.
        fetched_at: KST timestamp when the row was retrieved.
    """

    corp_code: str
    corp_name: str
    ticker: str | None
    market: Market | None
    stock_name: str | None
    modify_date: date | None
    is_active: bool
    source: Source
    fetched_at: datetime


@dataclass(frozen=True, slots=True)
class DartFinancialStatementLine:
    """Single raw account line from OpenDART financial statements."""

    corp_code: str
    ticker: str
    bsns_year: int
    reprt_code: str
    fs_div: str
    sj_div: str
    sj_nm: str
    account_id: str
    account_nm: str
    account_detail: str
    thstrm_nm: str
    thstrm_amount: Decimal | None
    thstrm_add_amount: Decimal | None
    frmtrm_nm: str
    frmtrm_amount: Decimal | None
    frmtrm_q_nm: str
    frmtrm_q_amount: Decimal | None
    frmtrm_add_amount: Decimal | None
    bfefrmtrm_nm: str
    bfefrmtrm_amount: Decimal | None
    ord: int | None
    currency: str
    rcept_no: str
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class DartShareCountLine:
    """Single raw row from OpenDART stockTotqySttus."""

    corp_code: str
    ticker: str
    bsns_year: int
    reprt_code: str
    rcept_no: str
    corp_cls: str
    se: str
    isu_stock_totqy: int | None
    now_to_isu_stock_totqy: int | None
    now_to_dcrs_stock_totqy: int | None
    redc: str
    profit_incnr: str
    rdmstk_repy: str
    etc: str
    istc_totqy: int | None
    tesstk_co: int | None
    distb_stock_co: int | None
    stlm_dt: date | None
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class DartShareholderReturnLine:
    """Flattened metric row from dividend / treasury-stock disclosures."""

    corp_code: str
    ticker: str
    bsns_year: int
    reprt_code: str
    statement_type: str
    row_name: str
    stock_knd: str
    dim1: str
    dim2: str
    dim3: str
    metric_code: str
    metric_name: str
    value_numeric: Decimal | None
    value_text: str
    unit: str
    rcept_no: str
    stlm_dt: date | None
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class DartXbrlDocument:
    """Metadata for one downloaded OpenDART XBRL ZIP document."""

    corp_code: str
    ticker: str
    bsns_year: int
    reprt_code: str
    rcept_no: str
    zip_entry_count: int
    instance_document_name: str
    label_ko_document_name: str
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class DartXbrlFactLine:
    """Single XBRL fact extracted from an OpenDART XBRL instance document."""

    corp_code: str
    ticker: str
    bsns_year: int
    reprt_code: str
    rcept_no: str
    concept_id: str
    concept_name: str
    namespace_uri: str
    context_id: str
    context_type: str
    period_start: date | None
    period_end: date | None
    instant_date: date | None
    dimensions: list[str]
    unit_id: str
    unit_measure: str
    decimals: str
    value_numeric: Decimal | None
    value_text: str
    is_nil: bool
    label_ko: str
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class SecurityFlowLine:
    """Single daily investor/shorting/ownership raw metric."""

    trade_date: date
    ticker: str
    market: Market
    metric_code: str
    metric_name: str
    value: Decimal | None
    unit: str
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class OperatingSourceDocument:
    """Raw source document used for sector-specific operating metric extraction."""

    document_key: str
    ticker: str
    market: Market
    sector_key: str
    document_type: str
    title: str
    document_date: date | None
    period_end: date | None
    source_system: str
    source_url: str
    language: str
    content_text: str
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class OperatingMetricFact:
    """Extracted sector-specific operating metric fact."""

    ticker: str
    market: Market
    sector_key: str
    metric_code: str
    metric_name: str
    period_end: date | None
    value_numeric: Decimal | None
    value_text: str
    unit: str
    document_key: str
    extractor_code: str
    raw_snippet: str
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class MetricCatalogEntry:
    """Canonical metric definition."""

    metric_code: str
    metric_name: str
    category: str
    unit: str
    description: str
    is_active: bool = True


@dataclass(frozen=True, slots=True)
class MetricMappingRule:
    """Rule connecting a raw row shape to a canonical metric."""

    rule_code: str
    metric_code: str
    source_table: str
    value_selector: str
    priority: int
    statement_type: str = ""
    fs_div: str = ""
    sj_div: str = ""
    account_id: str = ""
    account_nm: str = ""
    row_name: str = ""
    stock_knd: str = ""
    dim1: str = ""
    dim2: str = ""
    dim3: str = ""
    metric_code_match: str = ""
    is_active: bool = True


@dataclass(frozen=True, slots=True)
class StockMetricFact:
    """Normalized canonical metric fact for one company and reporting period."""

    ticker: str
    market: Market
    corp_code: str
    metric_code: str
    period_type: str
    period_end: date | None
    bsns_year: int
    reprt_code: str
    fs_div: str
    value_numeric: Decimal | None
    value_text: str
    unit: str
    source_table: str
    source_key: str
    mapping_rule_code: str
    fetched_at: datetime


@dataclass(frozen=True, slots=True)
class CommonFeatureSeries:
    """Source series definition for a common market or macro feature."""

    series_id: str
    source: Source
    source_series_key: str
    category: str
    frequency: str
    name_kr: str
    name_en: str = ""
    unit: str = ""
    country: str = ""
    market: str = ""
    endpoint_params: dict[str, object] = field(default_factory=dict)
    availability_policy: str = "release_date"
    manual_lag_days: int = 0
    source_timezone: str = "Asia/Seoul"
    history_start_date: date | None = None
    max_stale_business_days: int = 5
    default_transform: str = ""
    active: bool = True
    notes: str = ""


@dataclass(frozen=True, slots=True)
class CommonFeatureObservation:
    """Single raw observation for a common feature source series."""

    source: Source
    series_id: str
    observation_date: date
    frequency: str
    fetched_at: datetime
    period_end_date: date | None = None
    release_date: date | None = None
    available_from_date: date | None = None
    vintage: str = ""
    value_numeric: Decimal | None = None
    value_text: str = ""
    unit: str = ""
    source_updated_at: datetime | None = None
    raw_payload: dict[str, object] = field(default_factory=dict)
    raw_id: int | None = None


@dataclass(frozen=True, slots=True)
class CommonFeatureCatalogEntry:
    """Model-facing feature definition derived from one or more source series."""

    feature_code: str
    feature_name_kr: str
    category: str
    frequency: str = "D"
    unit: str = ""
    transform_code: str = ""
    description: str = ""
    input_series_ids: tuple[str, ...] = ()
    active: bool = True


@dataclass(frozen=True, slots=True)
class CommonFeatureDailyFact:
    """KRX-date-aligned common feature value safe for point-in-time joins."""

    feature_date: date
    feature_code: str
    asof_available_date: date
    generated_at: datetime
    value_numeric: Decimal | None = None
    value_text: str = ""
    unit: str = ""
    source_series_ids: list[str] = field(default_factory=list)
    source_observation_ids: list[int] = field(default_factory=list)
    selected_vintage: str = ""
    generation_run_id: str | None = None


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
class DartCorpCodeResult:
    """Result of fetching the OpenDART corporation-code master."""

    records: list[DartCorp] = field(default_factory=list)
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None
    exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartFinancialStatementResult:
    """Result of fetching one OpenDART financial statement payload."""

    corp_code: str = ""
    ticker: str = ""
    bsns_year: int = 0
    reprt_code: str = ""
    fs_div: str = ""
    records: list[DartFinancialStatementLine] = field(default_factory=list)
    no_data: bool = False
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None
    exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartShareCountResult:
    """Result of fetching one OpenDART share-count payload."""

    corp_code: str = ""
    ticker: str = ""
    bsns_year: int = 0
    reprt_code: str = ""
    records: list[DartShareCountLine] = field(default_factory=list)
    no_data: bool = False
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None
    exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartShareholderReturnResult:
    """Result of fetching one OpenDART dividend or treasury-stock payload."""

    corp_code: str = ""
    ticker: str = ""
    bsns_year: int = 0
    reprt_code: str = ""
    statement_type: str = ""
    records: list[DartShareholderReturnLine] = field(default_factory=list)
    no_data: bool = False
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None
    exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartXbrlResult:
    """Result of fetching and parsing one OpenDART XBRL document."""

    corp_code: str = ""
    ticker: str = ""
    bsns_year: int = 0
    reprt_code: str = ""
    rcept_no: str = ""
    document: DartXbrlDocument | None = None
    facts: list[DartXbrlFactLine] = field(default_factory=list)
    no_data: bool = False
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None
    exhaustion_reason: str | None = None


@dataclass(slots=True)
class SecurityFlowFetchResult:
    """Result of fetching a batch of security-flow raw rows."""

    records: list[SecurityFlowLine] = field(default_factory=list)
    no_data: bool = False
    error: str | None = None


@dataclass(slots=True)
class CommonFeatureFetchResult:
    """Result of fetching one common feature source series."""

    records: list[CommonFeatureObservation] = field(default_factory=list)
    no_data: bool = False
    error: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None


@dataclass(slots=True)
class OperatingMetricExtractionResult:
    """Result of extracting metrics from one operating source document."""

    document: OperatingSourceDocument | None = None
    facts: list[OperatingMetricFact] = field(default_factory=list)
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
class DartCorpSyncResult:
    """Outcome of syncing OpenDART corp codes into local storage."""

    upsert: UpsertResult = field(default_factory=UpsertResult)
    total_records: int = 0
    matched_active_tickers: int = 0
    unmatched_active_tickers: list[str] = field(default_factory=list)
    unmatched_dart_tickers: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass(slots=True)
class DartFinancialSyncResult:
    """Outcome of syncing OpenDART financial-statement raw rows."""

    upsert: UpsertResult = field(default_factory=UpsertResult)
    targets_processed: int = 0
    requests_attempted: int = 0
    requests_skipped: int = 0
    rows_upserted: int = 0
    no_data_requests: int = 0
    errors: dict[str, str] = field(default_factory=dict)
    opendart_exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartShareInfoSyncResult:
    """Outcome of syncing share-count and shareholder-return raw rows."""

    share_count_upsert: UpsertResult = field(default_factory=UpsertResult)
    shareholder_return_upsert: UpsertResult = field(default_factory=UpsertResult)
    targets_processed: int = 0
    requests_attempted: int = 0
    requests_skipped: int = 0
    share_count_rows_upserted: int = 0
    shareholder_return_rows_upserted: int = 0
    no_data_requests: int = 0
    errors: dict[str, str] = field(default_factory=dict)
    opendart_exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartXbrlSyncResult:
    """Outcome of syncing parsed OpenDART XBRL documents and facts."""

    document_upsert: UpsertResult = field(default_factory=UpsertResult)
    fact_upsert: UpsertResult = field(default_factory=UpsertResult)
    targets_processed: int = 0
    requests_attempted: int = 0
    requests_skipped: int = 0
    documents_upserted: int = 0
    facts_upserted: int = 0
    no_data_requests: int = 0
    errors: dict[str, str] = field(default_factory=dict)
    opendart_exhaustion_reason: str | None = None


@dataclass(slots=True)
class KrxFlowSyncResult:
    """Outcome of syncing KRX security-flow raw rows."""

    upsert: UpsertResult = field(default_factory=UpsertResult)
    targets_processed: int = 0
    requests_attempted: int = 0
    requests_skipped: int = 0
    rows_upserted: int = 0
    no_data_requests: int = 0
    pending_metrics: list[str] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class CommonFeatureSyncResult:
    """Outcome of syncing common feature raw observations."""

    upsert: UpsertResult = field(default_factory=UpsertResult)
    series_processed: int = 0
    requests_attempted: int = 0
    requests_skipped: int = 0
    rows_upserted: int = 0
    no_data_requests: int = 0
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class CommonFeatureBuildResult:
    """Outcome of building KRX-date-aligned common feature facts."""

    upsert: UpsertResult = field(default_factory=UpsertResult)
    features_processed: int = 0
    feature_dates_processed: int = 0
    facts_built: int = 0
    null_facts: int = 0
    facts_upserted: int = 0
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class CommonFeatureCatalogSeedResult:
    """Outcome of seeding common feature series and catalog rows."""

    series_upsert: UpsertResult = field(default_factory=UpsertResult)
    catalog_upsert: UpsertResult = field(default_factory=UpsertResult)


@dataclass(slots=True)
class OperatingMetricSyncResult:
    """Outcome of processing operating KPI source documents."""

    document_upsert: UpsertResult = field(default_factory=UpsertResult)
    fact_upsert: UpsertResult = field(default_factory=UpsertResult)
    documents_processed: int = 0
    facts_upserted: int = 0
    extracted_metric_codes: list[str] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class MetricNormalizationResult:
    """Outcome of seeding rules and normalizing canonical metrics."""

    catalog_upsert: UpsertResult = field(default_factory=UpsertResult)
    rule_upsert: UpsertResult = field(default_factory=UpsertResult)
    fact_upsert: UpsertResult = field(default_factory=UpsertResult)
    targets_processed: int = 0
    facts_written: int = 0
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MetricCoverageRow:
    """Coverage summary for one canonical metric."""

    metric_code: str
    metric_name: str
    target_count: int
    covered_count: int
    missing_count: int
    coverage_ratio: Decimal


@dataclass(slots=True)
class MetricCoverageReport:
    """Coverage report over normalized canonical metric facts."""

    target_count: int = 0
    rows: list[MetricCoverageRow] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CommonFeatureCoverageRow:
    """Coverage summary for one KRX-date-aligned common feature."""

    feature_code: str
    feature_name_kr: str
    target_count: int
    fact_count: int
    non_null_count: int
    null_count: int
    missing_count: int
    coverage_ratio: Decimal
    pit_violation_count: int


@dataclass(slots=True)
class CommonFeatureCoverageReport:
    """Coverage report over common feature daily facts."""

    target_count: int = 0
    rows: list[CommonFeatureCoverageRow] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CommonFeatureReadinessRow:
    """Active-readiness decision for one KRX-date-aligned common feature."""

    feature_code: str
    feature_name_kr: str
    target_count: int
    fact_count: int
    non_null_count: int
    null_count: int
    missing_count: int
    coverage_ratio: Decimal
    pit_violation_count: int
    required_coverage_ratio: Decimal
    ready: bool
    blockers: tuple[str, ...] = ()


@dataclass(slots=True)
class CommonFeatureReadinessReport:
    """Readiness report for common feature active-transition decisions."""

    target_count: int = 0
    rows: list[CommonFeatureReadinessRow] = field(default_factory=list)
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
