from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from krx_collector.cli.app import build_parser
from krx_collector.domain.enums import ListingStatus, Market, RunStatus, RunType, Source
from krx_collector.domain.models import (
    IngestionRun,
    SecurityFlowFetchResult,
    SecurityFlowLine,
    Stock,
    UpsertResult,
)
from krx_collector.service.sync_krx_flows import sync_krx_security_flows

KST = timezone(timedelta(hours=9))


class MockFlowProvider:
    def __init__(self, source: Source = Source.KRX) -> None:
        self._source = source
        self.investor_calls = 0
        self.shorting_calls = 0
        self.foreign_calls = 0

    def source(self) -> Source:
        return self._source

    def fetch_investor_net_volume(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        del end
        self.investor_calls += 1
        return SecurityFlowFetchResult(
            records=[
                SecurityFlowLine(
                    trade_date=start,
                    ticker=ticker,
                    market=market,
                    metric_code="institution_net_buy_volume",
                    metric_name="기관 순매수 수량",
                    value=Decimal("10"),
                    unit="shares",
                    source=self._source,
                    fetched_at=datetime(2026, 4, 19, tzinfo=KST),
                    raw_payload={},
                )
            ]
        )

    def fetch_shorting_metrics(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        del end
        self.shorting_calls += 1
        return SecurityFlowFetchResult(
            records=[
                SecurityFlowLine(
                    trade_date=start,
                    ticker=ticker,
                    market=market,
                    metric_code="short_selling_volume",
                    metric_name="공매도 거래량",
                    value=Decimal("20"),
                    unit="shares",
                    source=self._source,
                    fetched_at=datetime(2026, 4, 19, tzinfo=KST),
                    raw_payload={},
                )
            ]
        )

    def fetch_foreign_holding_shares(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        self.foreign_calls += 1
        ticker = tickers[0] if tickers else "005930"
        return SecurityFlowFetchResult(
            records=[
                SecurityFlowLine(
                    trade_date=trade_date,
                    ticker=ticker,
                    market=market,
                    metric_code="foreign_holding_shares",
                    metric_name="외국인 보유주식수",
                    value=Decimal("30"),
                    unit="shares",
                    source=self._source,
                    fetched_at=datetime(2026, 4, 19, tzinfo=KST),
                    raw_payload={},
                )
            ]
        )

    def unsupported_metric_codes(self) -> list[str]:
        return ["borrow_balance_quantity"]


class MockFlowStorage:
    def __init__(self) -> None:
        self.runs: list[IngestionRun] = []
        self.records: list[SecurityFlowLine] = []
        self.foreign_counts: dict[tuple[date, str], int] = {}
        self.investor_counts: dict[str, int] = {}
        self.shorting_counts: dict[str, int] = {}
        self.count_sources: list[Source] = []

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)

    def get_active_stocks(self, market: Market | None = None) -> list[Stock]:
        stocks = [
            Stock(
                ticker="005930",
                market=Market.KOSPI,
                name="삼성전자",
                status=ListingStatus.ACTIVE,
                last_seen_date=date(2026, 4, 17),
                source=Source.KRX,
            )
        ]
        if market is None:
            return stocks
        return [stock for stock in stocks if stock.market == market]

    def upsert_krx_security_flow_raw(self, records: list[SecurityFlowLine]) -> UpsertResult:
        self.records.extend(records)
        return UpsertResult(updated=len(records))

    def count_krx_security_flow_daily_market_tickers(
        self,
        start: date,
        end: date,
        tickers: list[str],
        metric_code: str,
        source: Source,
    ) -> dict[tuple[date, str], int]:
        del start, end, tickers, metric_code
        self.count_sources.append(source)
        return self.foreign_counts

    def count_krx_security_flow_ticker_metric_dates(
        self,
        start: date,
        end: date,
        tickers: list[str],
        metric_codes: list[str],
        source: Source,
    ) -> dict[str, int]:
        del start, end, tickers
        self.count_sources.append(source)
        if "institution_net_buy_volume" in metric_codes:
            return self.investor_counts
        return self.shorting_counts

    def get_daily_price_date_range(
        self,
        tickers: list[str] | None = None,
    ) -> tuple[date, date] | None:
        del tickers
        return date(2026, 4, 16), date(2026, 4, 17)


def test_sync_krx_security_flows_writes_rows_and_pending_metrics() -> None:
    storage = MockFlowStorage()

    result = sync_krx_security_flows(
        provider=MockFlowProvider(),  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 4, 17),
        end=date(2026, 4, 17),
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert result.targets_processed == 1
    assert result.requests_attempted == 3
    assert result.rows_upserted == 3
    assert result.pending_metrics == ["borrow_balance_quantity"]
    assert len(storage.records) == 3
    assert storage.runs[0].run_type == RunType.KRX_FLOW_SYNC
    assert storage.runs[-1].status == RunStatus.SUCCESS


def test_sync_krx_security_flows_skips_complete_existing_requests() -> None:
    storage = MockFlowStorage()
    storage.foreign_counts[(date(2026, 4, 17), Market.KOSPI.value)] = 1
    storage.investor_counts["005930"] = 3
    storage.shorting_counts["005930"] = 3
    provider = MockFlowProvider()

    result = sync_krx_security_flows(
        provider=provider,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 4, 17),
        end=date(2026, 4, 17),
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert result.requests_attempted == 0
    assert result.requests_skipped == 3
    assert result.rows_upserted == 0
    assert len(storage.records) == 0
    assert provider.foreign_calls == 0
    assert provider.investor_calls == 0
    assert provider.shorting_calls == 0
    assert storage.runs[-1].status == RunStatus.SUCCESS


def test_sync_krx_security_flows_uses_provider_source_for_existing_counts() -> None:
    storage = MockFlowStorage()
    provider = MockFlowProvider(source=Source.KRX)

    result = sync_krx_security_flows(
        provider=provider,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 4, 17),
        end=date(2026, 4, 17),
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert storage.count_sources == [Source.KRX, Source.KRX, Source.KRX]
    assert storage.records
    assert {record.source for record in storage.records} == {Source.KRX}
    assert storage.runs[0].params["provider_source"] == Source.KRX.value


def test_flows_sync_parser_supports_price_range_mode_without_provider_selection() -> None:
    args = build_parser().parse_args(["flows", "sync", "--use-price-range"])

    assert args.use_price_range is True
    assert args.start is None
    assert args.end is None
    assert not hasattr(args, "provider")


def test_flows_sync_parser_rejects_provider_selection() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["flows", "sync", "--provider", "krx"])
