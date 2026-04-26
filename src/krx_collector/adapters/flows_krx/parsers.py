"""Parsers for raw KRX MDC security-flow JSON rows."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from krx_collector.adapters.flows_common import (
    FOREIGN_HOLDING_SHARES,
    KRX_INVESTOR_COLUMNS,
    SHORT_SELLING_BALANCE_QUANTITY,
    SHORT_SELLING_VALUE,
    SHORT_SELLING_VOLUME,
    FlowMetricSpec,
)
from krx_collector.adapters.flows_krx.client import KrxMdcRow
from krx_collector.domain.enums import Market, Source
from krx_collector.domain.models import SecurityFlowLine
from krx_collector.util.time import now_kst

INVESTOR_BLD = "dbms/MDC/STAT/standard/MDCSTAT02302"
FOREIGN_HOLDING_BLD = "dbms/MDC/STAT/standard/MDCSTAT03701"
SHORTING_STATUS_BLD = "dbms/MDC/STAT/srt/MDCSTAT30001"
SHORTING_BALANCE_BLD = "dbms/MDC/STAT/srt/MDCSTAT30502"


def parse_investor_net_volume_rows(
    rows: Iterable[KrxMdcRow],
    ticker: str,
    market: Market,
) -> list[SecurityFlowLine]:
    """Parse MDCSTAT02302 rows into investor net-buy volume metrics."""
    fetched_at = now_kst()
    records: list[SecurityFlowLine] = []
    normalized_ticker = str(ticker).zfill(6)

    for item in rows:
        row = item.row
        trade_date = parse_trade_date(row.get("TRD_DD"))
        for column_name, spec in KRX_INVESTOR_COLUMNS.items():
            value = parse_decimal(row.get(column_name))
            if value is None:
                continue
            records.append(
                _build_line(
                    trade_date=trade_date,
                    ticker=normalized_ticker,
                    market=market,
                    spec=spec,
                    value=value,
                    fetched_at=fetched_at,
                    source_bld=INVESTOR_BLD,
                    request=item.request,
                    row=row,
                )
            )
    return records


def parse_foreign_holding_rows(
    rows: Iterable[KrxMdcRow],
    market: Market,
    trade_date: date,
    tickers: list[str] | None = None,
) -> list[SecurityFlowLine]:
    """Parse MDCSTAT03701 rows into foreign holding share metrics."""
    ticker_filter = {str(ticker).zfill(6) for ticker in tickers or []}
    fetched_at = now_kst()
    records: list[SecurityFlowLine] = []

    for item in rows:
        row = item.row
        ticker = str(row.get("ISU_SRT_CD", "")).strip().zfill(6)
        if not ticker or (ticker_filter and ticker not in ticker_filter):
            continue
        value = parse_decimal(row.get("FORN_HD_QTY"))
        if value is None:
            continue
        records.append(
            _build_line(
                trade_date=trade_date,
                ticker=ticker,
                market=market,
                spec=FOREIGN_HOLDING_SHARES,
                value=value,
                fetched_at=fetched_at,
                source_bld=FOREIGN_HOLDING_BLD,
                request=item.request,
                row=row,
            )
        )
    return records


def parse_shorting_rows(
    status_rows: Iterable[KrxMdcRow],
    balance_rows: Iterable[KrxMdcRow],
    ticker: str,
    market: Market,
) -> list[SecurityFlowLine]:
    """Parse short-selling status and balance rows with balance fallback."""
    fetched_at = now_kst()
    normalized_ticker = str(ticker).zfill(6)
    status_by_date: dict[date, KrxMdcRow] = {}
    balance_by_date: dict[date, KrxMdcRow] = {}

    for item in status_rows:
        status_by_date[parse_trade_date(item.row.get("TRD_DD"))] = item
    for item in balance_rows:
        balance_by_date[parse_trade_date(item.row.get("RPT_DUTY_OCCR_DD"))] = item

    records: list[SecurityFlowLine] = []
    for trade_date in sorted(status_by_date.keys() | balance_by_date.keys()):
        status_item = status_by_date.get(trade_date)
        balance_item = balance_by_date.get(trade_date)

        if status_item is not None:
            status_row = status_item.row
            for column_name, spec in [
                ("CVSRTSELL_TRDVOL", SHORT_SELLING_VOLUME),
                ("CVSRTSELL_TRDVAL", SHORT_SELLING_VALUE),
            ]:
                value = parse_decimal(status_row.get(column_name))
                if value is None:
                    continue
                records.append(
                    _build_line(
                        trade_date=trade_date,
                        ticker=normalized_ticker,
                        market=market,
                        spec=spec,
                        value=value,
                        fetched_at=fetched_at,
                        source_bld=SHORTING_STATUS_BLD,
                        request=status_item.request,
                        row=status_row,
                    )
                )

        balance_source_item = balance_item
        balance_source_bld = SHORTING_BALANCE_BLD
        balance_value = (
            parse_decimal(balance_item.row.get("BAL_QTY")) if balance_item is not None else None
        )
        if balance_value is None and status_item is not None:
            balance_value = parse_decimal(status_item.row.get("STR_CONST_VAL1"))
            balance_source_item = status_item
            balance_source_bld = SHORTING_STATUS_BLD

        if balance_value is not None and balance_source_item is not None:
            records.append(
                _build_line(
                    trade_date=trade_date,
                    ticker=normalized_ticker,
                    market=market,
                    spec=SHORT_SELLING_BALANCE_QUANTITY,
                    value=balance_value,
                    fetched_at=fetched_at,
                    source_bld=balance_source_bld,
                    request=balance_source_item.request,
                    row=balance_source_item.row,
                )
            )

    return records


def parse_decimal(value: object) -> Decimal | None:
    """Parse a KRX numeric string into Decimal."""
    if value is None:
        return None
    text = str(value).strip()
    if text in {"", "-", "--", "N/A", "nan", "None"}:
        return None
    text = text.replace(",", "")
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid KRX numeric value: {value!r}") from exc


def parse_trade_date(value: object) -> date:
    """Parse KRX date values in YYYY/MM/DD, YYYY-MM-DD, or YYYYMMDD format."""
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        raise ValueError("Missing KRX trade date")
    if "/" in text:
        text = text.replace("/", "-")
    if "-" in text:
        return date.fromisoformat(text)
    if len(text) == 8 and text.isdigit():
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    raise ValueError(f"Invalid KRX trade date: {value!r}")


def _build_line(
    *,
    trade_date: date,
    ticker: str,
    market: Market,
    spec: FlowMetricSpec,
    value: Decimal,
    fetched_at,
    source_bld: str,
    request: dict[str, Any],
    row: dict[str, Any],
) -> SecurityFlowLine:
    return SecurityFlowLine(
        trade_date=trade_date,
        ticker=ticker,
        market=market,
        metric_code=spec.metric_code,
        metric_name=spec.metric_name,
        value=value,
        unit=spec.unit,
        source=Source.KRX,
        fetched_at=fetched_at,
        raw_payload={
            "source_bld": source_bld,
            "request": dict(request),
            "row": dict(row),
        },
    )
