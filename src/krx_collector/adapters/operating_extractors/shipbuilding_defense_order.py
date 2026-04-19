"""Pilot extractor for shipbuilding/defense order metrics."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

from krx_collector.domain.models import (
    OperatingMetricExtractionResult,
    OperatingMetricFact,
    OperatingSourceDocument,
)
from krx_collector.util.time import now_kst

_AMOUNT_VALUE_PATTERN = (
    r"(?:\d[\d,]*(?:\.\d+)?)\s*조(?:\s*(?:\d[\d,]*(?:\.\d+)?)\s*억)?\s*원?"
    r"|(?:\d[\d,]*(?:\.\d+)?)\s*억\s*원?"
    r"|(?:\d[\d,]*(?:\.\d+)?)\s*백만원"
    r"|(?:\d[\d,]*(?:\.\d+)?)\s*원"
)
_AMOUNT_PATTERN = re.compile(rf"(?P<amount>{_AMOUNT_VALUE_PATTERN})")


def parse_korean_amount_to_won(text: str) -> Decimal | None:
    """Parse common Korean monetary expressions into KRW."""
    normalized = text.replace(" ", "").replace(",", "")
    if not normalized:
        return None

    total = Decimal("0")
    if "조" in normalized:
        parts = normalized.split("조", 1)
        try:
            total += Decimal(parts[0]) * Decimal("1000000000000")
        except InvalidOperation:
            return None
        normalized = parts[1]
    if "억" in normalized:
        parts = normalized.split("억", 1)
        if parts[0]:
            try:
                total += Decimal(parts[0]) * Decimal("100000000")
            except InvalidOperation:
                return None
        normalized = parts[1]
    if "백만원" in normalized:
        value = normalized.replace("백만원", "")
        try:
            total += Decimal(value) * Decimal("1000000")
        except InvalidOperation:
            return None
        normalized = ""
    elif normalized.endswith("원"):
        value = normalized[:-1]
        if value:
            try:
                total += Decimal(value)
            except InvalidOperation:
                return None

    return total if total != 0 else Decimal("0")


def _extract_amount_snippet(text: str, keywords: list[str]) -> tuple[Decimal | None, str]:
    patterns = []
    for keyword in keywords:
        patterns.append(
            re.compile(
                rf"{keyword}\s*(?:은|는|:|기준|으로)?\s*(?P<amount>{_AMOUNT_VALUE_PATTERN})"
            )
        )

    for pattern in patterns:
        match = pattern.search(text)
        if match is None:
            continue
        amount_text = match.group("amount")
        amount_value = parse_korean_amount_to_won(amount_text)
        if amount_value is not None:
            return amount_value, match.group(0).strip()
    return None, ""


class ShipbuildingDefenseOrderExtractor:
    """Extract order intake/backlog metrics from shipbuilding/defense text."""

    sector_key = "shipbuilding_defense"
    extractor_code = "shipbuilding_defense_order_v1"

    def extract(self, document: OperatingSourceDocument) -> OperatingMetricExtractionResult:
        text = document.content_text
        fetched_at = now_kst()
        facts: list[OperatingMetricFact] = []

        metric_specs = [
            (
                "order_intake_amount",
                "수주금액",
                ["신규수주금액", "수주금액", "수주액", "신규수주"],
            ),
            (
                "order_backlog_amount",
                "수주잔고",
                ["수주잔고", "수주총잔고"],
            ),
        ]

        for metric_code, metric_name, keywords in metric_specs:
            value_numeric, snippet = _extract_amount_snippet(text, keywords)
            if value_numeric is None:
                continue
            facts.append(
                OperatingMetricFact(
                    ticker=document.ticker,
                    market=document.market,
                    sector_key=document.sector_key,
                    metric_code=metric_code,
                    metric_name=metric_name,
                    period_end=document.period_end,
                    value_numeric=value_numeric,
                    value_text=str(value_numeric),
                    unit="KRW",
                    document_key=document.document_key,
                    extractor_code=self.extractor_code,
                    raw_snippet=snippet,
                    fetched_at=fetched_at,
                    raw_payload={
                        "keywords": keywords,
                    },
                )
            )

        return OperatingMetricExtractionResult(document=document, facts=facts)
