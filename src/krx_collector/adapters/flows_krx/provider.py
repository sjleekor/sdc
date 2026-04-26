"""Direct KRX MDC security-flow provider."""

from __future__ import annotations

import logging
from datetime import date

from krx_collector.adapters.flows_common import UNSUPPORTED_FLOW_METRIC_CODES
from krx_collector.adapters.flows_krx.client import KrxMdcClient, KrxMdcResponseError
from krx_collector.adapters.flows_krx.codes import KrxStockCodeResolver
from krx_collector.adapters.flows_krx.parsers import (
    FOREIGN_HOLDING_BLD,
    INVESTOR_BLD,
    SHORTING_BALANCE_BLD,
    SHORTING_STATUS_BLD,
    parse_foreign_holding_rows,
    parse_investor_net_volume_rows,
    parse_shorting_rows,
)
from krx_collector.domain.enums import Market, Source
from krx_collector.domain.models import SecurityFlowFetchResult

logger = logging.getLogger(__name__)

MARKET_TO_KRX_ID = {
    Market.KOSPI: "STK",
    Market.KOSDAQ: "KSQ",
}


class KrxDirectFlowProvider:
    """Fetch security-flow metrics directly from KRX MDC JSON endpoints."""

    def __init__(
        self,
        *,
        client: KrxMdcClient | None = None,
        resolver: KrxStockCodeResolver | None = None,
        timeout_seconds: float = 20.0,
        login_id: str = "",
        login_pw: str = "",
    ) -> None:
        self._client = client or KrxMdcClient(
            timeout_seconds=timeout_seconds,
            login_id=login_id,
            login_pw=login_pw,
        )
        self._resolver = resolver or KrxStockCodeResolver(self._client)

    def source(self) -> Source:
        return Source.KRX

    def fetch_investor_net_volume(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        try:
            isin = self._resolver.resolve_isin(ticker, market)
            rows = self._client.post_rows(
                INVESTOR_BLD,
                {
                    "strtDd": start.strftime("%Y%m%d"),
                    "endDd": end.strftime("%Y%m%d"),
                    "isuCd": isin,
                    "inqTpCd": "2",
                    "trdVolVal": "1",
                    "askBid": "3",
                },
                output_key="output",
            )
            records = parse_investor_net_volume_rows(rows, ticker, market)
            return SecurityFlowFetchResult(records=records, no_data=not records)
        except Exception as exc:
            logger.exception("Failed to fetch KRX investor net volume for %s", ticker)
            return SecurityFlowFetchResult(error=str(exc))

    def fetch_shorting_metrics(
        self,
        ticker: str,
        market: Market,
        start: date,
        end: date,
    ) -> SecurityFlowFetchResult:
        try:
            isin = self._resolver.resolve_isin(ticker, market)
            common_params = {
                "strtDd": start.strftime("%Y%m%d"),
                "endDd": end.strftime("%Y%m%d"),
                "isuCd": isin,
            }
            status_rows = self._client.post_rows(
                SHORTING_STATUS_BLD,
                common_params,
                output_key="OutBlock_1",
            )
            balance_rows = self._client.post_rows(
                SHORTING_BALANCE_BLD,
                common_params,
                output_key="OutBlock_1",
            )
            records = parse_shorting_rows(status_rows, balance_rows, ticker, market)
            return SecurityFlowFetchResult(records=records, no_data=not records)
        except Exception as exc:
            logger.exception("Failed to fetch KRX shorting metrics for %s", ticker)
            return SecurityFlowFetchResult(error=str(exc))

    def fetch_foreign_holding_shares(
        self,
        trade_date: date,
        market: Market,
        tickers: list[str] | None = None,
    ) -> SecurityFlowFetchResult:
        try:
            mkt_id = MARKET_TO_KRX_ID.get(market)
            if mkt_id is None:
                raise KrxMdcResponseError(
                    f"Unsupported market for KRX MDC foreign holding: {market.value}"
                )
            rows = self._client.post_rows(
                FOREIGN_HOLDING_BLD,
                {
                    "searchType": "1",
                    "mktId": mkt_id,
                    "trdDd": trade_date.strftime("%Y%m%d"),
                    "isuLmtRto": "0",
                },
                output_key="output",
            )
            records = parse_foreign_holding_rows(rows, market, trade_date, tickers)
            return SecurityFlowFetchResult(records=records, no_data=not records)
        except Exception as exc:
            logger.exception("Failed to fetch KRX foreign holdings for %s", market.value)
            return SecurityFlowFetchResult(error=str(exc))

    def unsupported_metric_codes(self) -> list[str]:
        return list(UNSUPPORTED_FLOW_METRIC_CODES)
