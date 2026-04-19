"""Port: security flow provider interface."""

from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable

from krx_collector.domain.enums import Market
from krx_collector.domain.models import SecurityFlowFetchResult


@runtime_checkable
class FlowProvider(Protocol):
    """Fetch daily investor/shorting/ownership flow metrics."""

    def fetch_investor_net_volume(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        """Fetch investor net-buy volume metrics for one ticker/date range."""
        ...

    def fetch_shorting_metrics(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        """Fetch short-selling metrics for one ticker/date range."""
        ...

    def fetch_foreign_holding_shares(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        """Fetch foreign ownership shares for one market/date."""
        ...

    def unsupported_metric_codes(self) -> list[str]:
        """Return metric codes intentionally left unsupported by this provider."""
        ...
