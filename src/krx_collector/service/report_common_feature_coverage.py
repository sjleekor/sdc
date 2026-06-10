"""Use-case: report common feature daily fact coverage."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date
from decimal import Decimal

from krx_collector.domain.models import (
    CommonFeatureCoverageReport,
    CommonFeatureCoverageRow,
    CommonFeatureDailyFact,
)
from krx_collector.infra.calendar.trading_days import get_trading_days
from krx_collector.ports.storage import Storage

KrxTradingDayProvider = Callable[[date, date], Sequence[date]]


def build_common_feature_coverage_report(
    storage: Storage,
    start: date,
    end: date,
    feature_codes: list[str] | None = None,
    active_only: bool = True,
    krx_trading_days: KrxTradingDayProvider | None = None,
) -> CommonFeatureCoverageReport:
    """Build a coverage report for common feature daily facts."""
    calendar = krx_trading_days or get_trading_days
    target_count = len(calendar(start, end))
    report = CommonFeatureCoverageReport(target_count=target_count)

    catalog_rows = storage.get_common_feature_catalog(
        feature_codes=feature_codes,
        active_only=active_only,
    )
    facts = storage.get_common_feature_daily_facts(
        start=start,
        end=end,
        feature_codes=feature_codes,
    )
    facts_by_feature = _group_facts(facts)

    for entry in catalog_rows:
        feature_facts = facts_by_feature.get(entry.feature_code, [])
        fact_count = len(feature_facts)
        non_null_count = sum(1 for fact in feature_facts if fact.value_numeric is not None)
        null_count = sum(1 for fact in feature_facts if fact.value_numeric is None)
        missing_count = max(target_count - fact_count, 0)
        pit_violation_count = sum(
            1
            for fact in feature_facts
            if fact.asof_available_date > fact.feature_date
        )
        coverage_ratio = Decimal("0")
        if target_count > 0:
            coverage_ratio = (
                Decimal(non_null_count) / Decimal(target_count)
            ).quantize(Decimal("0.0001"))

        report.rows.append(
            CommonFeatureCoverageRow(
                feature_code=entry.feature_code,
                feature_name_kr=entry.feature_name_kr,
                target_count=target_count,
                fact_count=fact_count,
                non_null_count=non_null_count,
                null_count=null_count,
                missing_count=missing_count,
                coverage_ratio=coverage_ratio,
                pit_violation_count=pit_violation_count,
            )
        )

    report.rows.sort(key=lambda row: (row.coverage_ratio, row.feature_code), reverse=True)
    return report


def _group_facts(
    facts: list[CommonFeatureDailyFact],
) -> dict[str, list[CommonFeatureDailyFact]]:
    grouped: dict[str, list[CommonFeatureDailyFact]] = {}
    for fact in facts:
        grouped.setdefault(fact.feature_code, []).append(fact)
    return grouped
