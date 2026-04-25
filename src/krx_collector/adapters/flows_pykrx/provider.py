"""pykrx-based security-flow provider."""

from __future__ import annotations

import logging
import signal
from datetime import date
from decimal import Decimal

import pandas as pd
from pykrx import stock

from krx_collector.domain.enums import Market, Source
from krx_collector.domain.models import SecurityFlowFetchResult, SecurityFlowLine
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)


class PykrxCallTimeoutError(RuntimeError):
    """Raised when a pykrx network call exceeds the configured timeout."""


def _json_safe(value: object) -> object:
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


def _parse_decimal(value: object) -> Decimal | None:
    if value is None or pd.isna(value):
        return None
    return Decimal(str(value))


def _parse_trade_date(value: object) -> date:
    if hasattr(value, "date"):
        return value.date()  # type: ignore[no-any-return]
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def parse_investor_net_volume_frame(
    df: pd.DataFrame,
    ticker: str,
    market: Market,
) -> list[SecurityFlowLine]:
    """Parse pykrx investor net-buy volume frame into raw metric rows."""
    if df is None or df.empty:
        return []

    metric_specs = {
        "기관합계": ("institution_net_buy_volume", "기관 순매수 수량"),
        "개인": ("individual_net_buy_volume", "개인 순매수 수량"),
        "외국인합계": ("foreign_net_buy_volume", "외국인 순매수 수량"),
    }
    fetched_at = now_kst()
    records: list[SecurityFlowLine] = []

    for trade_date, row in df.iterrows():
        trade_date_value = _parse_trade_date(trade_date)
        row_dict = {key: _json_safe(value) for key, value in row.to_dict().items()}
        for column_name, (metric_code, metric_name) in metric_specs.items():
            if column_name not in row_dict:
                continue
            records.append(
                SecurityFlowLine(
                    trade_date=trade_date_value,
                    ticker=ticker,
                    market=market,
                    metric_code=metric_code,
                    metric_name=metric_name,
                    value=_parse_decimal(row_dict.get(column_name)),
                    unit="shares",
                    source=Source.PYKRX,
                    fetched_at=fetched_at,
                    raw_payload={
                        "kind": "investor_net_volume",
                        "row": row_dict,
                    },
                )
            )
    return records


def parse_foreign_holding_frame(
    df: pd.DataFrame,
    market: Market,
    trade_date: date,
    tickers: list[str] | None = None,
) -> list[SecurityFlowLine]:
    """Parse pykrx foreign ownership frame into raw metric rows."""
    if df is None or df.empty:
        return []

    ticker_filter = set(tickers or [])
    fetched_at = now_kst()
    records: list[SecurityFlowLine] = []

    for ticker_value, row in df.iterrows():
        ticker = str(ticker_value).zfill(6)
        if ticker_filter and ticker not in ticker_filter:
            continue
        row_dict = {key: _json_safe(value) for key, value in row.to_dict().items()}
        records.append(
            SecurityFlowLine(
                trade_date=trade_date,
                ticker=ticker,
                market=market,
                metric_code="foreign_holding_shares",
                metric_name="외국인 보유주식수",
                value=_parse_decimal(row_dict.get("보유수량")),
                unit="shares",
                source=Source.PYKRX,
                fetched_at=fetched_at,
                raw_payload={
                    "kind": "foreign_holding_shares",
                    "row": row_dict,
                },
            )
        )
    return records


def parse_shorting_frames(
    status_df: pd.DataFrame,
    balance_df: pd.DataFrame,
    ticker: str,
    market: Market,
) -> list[SecurityFlowLine]:
    """Parse pykrx shorting status/balance frames into raw metric rows."""
    if (status_df is None or status_df.empty) and (balance_df is None or balance_df.empty):
        return []

    fetched_at = now_kst()
    records: list[SecurityFlowLine] = []
    by_date: dict[date, dict[str, object]] = {}

    if status_df is not None and not status_df.empty:
        for trade_date, row in status_df.iterrows():
            by_date.setdefault(_parse_trade_date(trade_date), {}).update(
                {f"status:{key}": _json_safe(value) for key, value in row.to_dict().items()}
            )

    if balance_df is not None and not balance_df.empty:
        for trade_date, row in balance_df.iterrows():
            by_date.setdefault(_parse_trade_date(trade_date), {}).update(
                {f"balance:{key}": _json_safe(value) for key, value in row.to_dict().items()}
            )

    for trade_date_value, merged_row in sorted(by_date.items()):
        status_volume = merged_row.get("status:거래량")
        status_value = merged_row.get("status:거래대금")
        balance_quantity = merged_row.get("balance:공매도잔고")
        if balance_quantity is None:
            balance_quantity = merged_row.get("status:잔고수량")

        specs = [
            ("short_selling_volume", "공매도 거래량", status_volume, "shares"),
            ("short_selling_value", "공매도 거래대금", status_value, "KRW"),
            ("short_selling_balance_quantity", "공매도 잔고 수량", balance_quantity, "shares"),
        ]
        for metric_code, metric_name, raw_value, unit in specs:
            if raw_value is None or pd.isna(raw_value):
                continue
            records.append(
                SecurityFlowLine(
                    trade_date=trade_date_value,
                    ticker=ticker,
                    market=market,
                    metric_code=metric_code,
                    metric_name=metric_name,
                    value=_parse_decimal(raw_value),
                    unit=unit,
                    source=Source.PYKRX,
                    fetched_at=fetched_at,
                    raw_payload={
                        "kind": "shorting",
                        "row": merged_row,
                    },
                )
            )
    return records


class PykrxFlowProvider:
    """Fetch security-flow metrics via pykrx."""

    def __init__(self, call_timeout_seconds: float = 20.0) -> None:
        self._call_timeout_seconds = call_timeout_seconds

    def _call_with_timeout(self, func, *args):
        if self._call_timeout_seconds <= 0:
            return func(*args)

        def _timeout_handler(signum, frame):  # type: ignore[no-untyped-def]
            raise PykrxCallTimeoutError(
                f"pykrx call timed out after {self._call_timeout_seconds} seconds"
            )

        previous_handler = signal.getsignal(signal.SIGALRM)
        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.setitimer(signal.ITIMER_REAL, self._call_timeout_seconds)
        try:
            return func(*args)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous_handler)

    def fetch_investor_net_volume(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        try:
            df = self._call_with_timeout(
                stock.get_market_trading_volume_by_date,
                start.strftime("%Y%m%d"),
                end.strftime("%Y%m%d"),
                ticker,
            )
            return SecurityFlowFetchResult(
                records=parse_investor_net_volume_frame(df, ticker, market),
                no_data=df is None or df.empty,
            )
        except Exception as exc:
            logger.exception("Failed to fetch investor net volume for %s", ticker)
            return SecurityFlowFetchResult(error=str(exc))

    def fetch_shorting_metrics(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        try:
            start_str = start.strftime("%Y%m%d")
            end_str = end.strftime("%Y%m%d")
            status_df = self._call_with_timeout(
                stock.get_shorting_status_by_date, start_str, end_str, ticker
            )
            balance_df = self._call_with_timeout(
                stock.get_shorting_balance_by_date, start_str, end_str, ticker
            )
            records = parse_shorting_frames(status_df, balance_df, ticker, market)
            return SecurityFlowFetchResult(
                records=records,
                no_data=(status_df is None or status_df.empty)
                and (balance_df is None or balance_df.empty),
            )
        except Exception as exc:
            logger.exception("Failed to fetch shorting metrics for %s", ticker)
            return SecurityFlowFetchResult(error=str(exc))

    def fetch_foreign_holding_shares(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        try:
            df = self._call_with_timeout(
                stock.get_exhaustion_rates_of_foreign_investment_by_ticker,
                trade_date.strftime("%Y%m%d"),
                market.value,
            )
            records = parse_foreign_holding_frame(df, market, trade_date, tickers)
            return SecurityFlowFetchResult(
                records=records,
                no_data=df is None or df.empty,
            )
        except Exception as exc:
            logger.exception("Failed to fetch foreign holdings for %s", market.value)
            return SecurityFlowFetchResult(error=str(exc))

    def unsupported_metric_codes(self) -> list[str]:
        return ["borrow_balance_quantity"]
