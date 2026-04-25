from datetime import date
from decimal import Decimal

import pandas as pd

from krx_collector.adapters.flows_pykrx.provider import (
    parse_foreign_holding_frame,
    parse_investor_net_volume_frame,
    parse_shorting_frames,
)
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


def test_parse_investor_net_volume_frame() -> None:
    df = pd.DataFrame(
        [
            {"기관합계": -100, "개인": 40, "외국인합계": 60, "전체": 0},
        ],
        index=pd.to_datetime(["2026-04-17"]),
    )

    records = parse_investor_net_volume_frame(df, "005930", Market.KOSPI)

    assert len(records) == 3
    facts = {record.metric_code: record for record in records}
    assert facts["institution_net_buy_volume"].value == Decimal("-100")
    assert facts["individual_net_buy_volume"].value == Decimal("40")
    assert facts["foreign_net_buy_volume"].value == Decimal("60")


def test_parse_foreign_holding_frame() -> None:
    df = pd.DataFrame(
        [
            {"상장주식수": 1000, "보유수량": 250, "지분율": 25.0},
        ],
        index=["005930"],
    )

    records = parse_foreign_holding_frame(df, Market.KOSPI, date(2026, 4, 17), ["005930"])

    assert len(records) == 1
    assert records[0].metric_code == "foreign_holding_shares"
    assert records[0].value == Decimal("250")


def test_parse_shorting_frames() -> None:
    status_df = pd.DataFrame(
        [
            {"거래량": 123, "거래대금": 4567, "잔고수량": 999},
        ],
        index=pd.to_datetime(["2026-04-17"]),
    )
    balance_df = pd.DataFrame(
        [
            {"공매도잔고": 1000, "공매도금액": 50000},
        ],
        index=pd.to_datetime(["2026-04-17"]),
    )

    records = parse_shorting_frames(status_df, balance_df, "005930", Market.KOSPI)

    assert len(records) == 3
    facts = {record.metric_code: record for record in records}
    assert facts["short_selling_volume"].value == Decimal("123")
    assert facts["short_selling_value"].value == Decimal("4567")
    assert facts["short_selling_balance_quantity"].value == Decimal("1000")


class MockFlowProvider:
    def __init__(self) -> None:
        self.investor_calls = 0
        self.shorting_calls = 0
        self.foreign_calls = 0

    def fetch_investor_net_volume(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
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
                    source=Source.PYKRX,
                    fetched_at=pd.Timestamp("2026-04-19T00:00:00+09:00").to_pydatetime(),
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
                    source=Source.PYKRX,
                    fetched_at=pd.Timestamp("2026-04-19T00:00:00+09:00").to_pydatetime(),
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
                    source=Source.PYKRX,
                    fetched_at=pd.Timestamp("2026-04-19T00:00:00+09:00").to_pydatetime(),
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
                source=Source.PYKRX,
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
        del start, end, tickers, metric_code, source
        return self.foreign_counts

    def count_krx_security_flow_ticker_metric_dates(
        self,
        start: date,
        end: date,
        tickers: list[str],
        metric_codes: list[str],
        source: Source,
    ) -> dict[str, int]:
        del start, end, tickers, source
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


def test_flows_sync_parser_supports_price_range_mode() -> None:
    args = build_parser().parse_args(["flows", "sync", "--use-price-range"])

    assert args.use_price_range is True
    assert args.start is None
    assert args.end is None
