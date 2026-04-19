"""End-to-End Integration tests for the KRX pipeline.

These tests use the real PostgreSQL database but mock the external
data providers (FDR, pykrx) to avoid network flakiness and rate limits.
"""

import uuid
from datetime import date
from pathlib import Path

import pytest

from krx_collector.domain.enums import ListingStatus, Market, Source
from krx_collector.domain.models import (
    DailyBar,
    DailyPriceResult,
    OperatingSourceDocument,
    Stock,
    StockUniverseSnapshot,
    UniverseResult,
)
from krx_collector.infra.config.settings import get_settings
from krx_collector.infra.db_postgres.repositories import PostgresStorage
from krx_collector.service.backfill_daily import backfill_daily_prices
from krx_collector.service.default_operating_registry import build_default_operating_registry
from krx_collector.service.process_operating_document import (
    build_operating_document_key,
    process_operating_document,
)
from krx_collector.service.sync_universe import sync_universe
from krx_collector.service.validate import validate
from krx_collector.util.time import now_kst


OPERATING_FIXTURE_PATH = (
    Path(__file__).resolve().parent.parent / "fixtures" / "operating" / "shipbuilding_defense_sample.txt"
)


class MockUniverseProvider:
    def fetch_universe(
        self, markets: list[Market], as_of: date | None = None
    ) -> UniverseResult:
        records = [
            Stock(
                ticker="000001",
                market=Market.KOSPI,
                name="Mock Corp A",
                status=ListingStatus.ACTIVE,
                last_seen_date=as_of or date.today(),
                source=Source.FDR,
            ),
            Stock(
                ticker="000002",
                market=Market.KOSDAQ,
                name="Mock Corp B",
                status=ListingStatus.ACTIVE,
                last_seen_date=as_of or date.today(),
                source=Source.FDR,
            ),
        ]
        snapshot = StockUniverseSnapshot(
            snapshot_id=str(uuid.uuid4()),
            as_of_date=as_of or date.today(),
            source=Source.FDR,
            fetched_at=now_kst(),
            records=records,
        )
        return UniverseResult(snapshot=snapshot)


class MockPriceProvider:
    def fetch_daily_ohlcv(
        self, ticker: str, market: Market, start: date, end: date
    ) -> DailyPriceResult:
        bars = [
            DailyBar(
                ticker=ticker,
                market=market,
                trade_date=start,
                open=1000,
                high=1100,
                low=900,
                close=1050,
                volume=5000,
                source=Source.PYKRX,
                fetched_at=now_kst(),
            )
        ]
        return DailyPriceResult(ticker=ticker, bars=bars)


@pytest.fixture(scope="session")
def storage() -> PostgresStorage:
    settings = get_settings()
    store = PostgresStorage(settings.db_dsn)
    try:
        store.init_schema()
    except Exception as e:
        pytest.skip(f"Could not initialize DB: {e}")
    return store


@pytest.fixture(autouse=True)
def clean_db(storage: PostgresStorage) -> None:
    """Clean the tables before and after the tests."""
    from krx_collector.infra.db_postgres.connection import get_connection

    def _truncate() -> None:
        with get_connection(storage._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "TRUNCATE TABLE operating_metric_fact, operating_source_document, "
                    "daily_ohlcv, stock_master, "
                    "stock_master_snapshot_items, stock_master_snapshot, "
                    "ingestion_runs CASCADE;"
                )

    _truncate()
    yield
    _truncate()


def test_end_to_end_pipeline(storage: PostgresStorage) -> None:
    """Test the full pipeline: sync universe -> backfill prices -> validate."""
    test_date = date(2024, 1, 10)

    # 1. Sync Universe
    universe_provider = MockUniverseProvider()
    sync_result = sync_universe(
        provider=universe_provider,
        storage=storage,
        markets=[Market.KOSPI, Market.KOSDAQ],
        as_of=test_date,
    )
    assert sync_result.error is None
    assert sync_result.upsert.updated > 0

    # Verify stock is in DB
    active_stocks = storage.get_active_stocks()
    assert len(active_stocks) == 2
    assert active_stocks[0].ticker in ("000001", "000002")

    # 2. Backfill Daily Prices
    price_provider = MockPriceProvider()
    backfill_result = backfill_daily_prices(
        provider=price_provider,
        storage=storage,
        market=Market.KOSPI,  # Only backfill KOSPI
        start=test_date,
        end=test_date,
        rate_limit_seconds=0.0,
    )

    assert len(backfill_result.errors) == 0
    assert backfill_result.tickers_processed == 1  # Only 1 KOSPI stock mock
    assert backfill_result.bars_upserted == 1

    # Verify price is in DB
    bars = storage.get_daily_bars(target_date=test_date, market=Market.KOSPI)
    assert len(bars) == 1
    assert bars[0].ticker == "000001"
    assert bars[0].close == 1050

    # 3. Validate
    # Running validation should log missing days for the KOSDAQ stock (since we didn't backfill it)
    # but the KOSPI stock should pass.
    # Validation service doesn't return anything, but it shouldn't crash.
    validate(storage=storage, market=Market.KOSPI, target_date=test_date)


def test_operating_document_pipeline(storage: PostgresStorage) -> None:
    """Test the operating KPI pipeline: persist source doc -> extract -> query facts."""
    content_text = OPERATING_FIXTURE_PATH.read_text(encoding="utf-8")
    document = OperatingSourceDocument(
        document_key=build_operating_document_key(
            ticker="009540",
            sector_key="shipbuilding_defense",
            document_type="manual_text",
            title="조선 방산 수주 샘플",
            period_end="2025-12-31",
            content_text=content_text,
        ),
        ticker="009540",
        market=Market.KOSPI,
        sector_key="shipbuilding_defense",
        document_type="manual_text",
        title="조선 방산 수주 샘플",
        document_date=date(2026, 4, 19),
        period_end=date(2025, 12, 31),
        source_system="LOCAL",
        source_url="",
        language="ko",
        content_text=content_text,
        fetched_at=now_kst(),
        raw_payload={},
    )

    result = process_operating_document(
        storage=storage,
        registry=build_default_operating_registry(),
        document=document,
    )

    assert result.errors == {}
    assert result.documents_processed == 1
    assert result.facts_upserted == 2

    facts = storage.get_operating_metric_facts(tickers=["009540"], sector_keys=["shipbuilding_defense"])
    fact_map = {fact.metric_code: fact for fact in facts}
    assert sorted(fact_map) == ["order_backlog_amount", "order_intake_amount"]
    assert str(fact_map["order_intake_amount"].value_numeric) == "3250000000000.0000"
    assert str(fact_map["order_backlog_amount"].value_numeric) == "24130000000000.0000"
