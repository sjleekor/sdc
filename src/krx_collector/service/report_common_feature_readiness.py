"""Use-case: report common feature active-transition readiness."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date
from decimal import Decimal

from krx_collector.domain.models import (
    CommonFeatureCoverageRow,
    CommonFeatureReadinessReport,
    CommonFeatureReadinessRow,
)
from krx_collector.ports.storage import Storage
from krx_collector.service.report_common_feature_coverage import (
    build_common_feature_coverage_report,
)

KrxTradingDayProvider = Callable[[date, date], Sequence[date]]


def build_common_feature_readiness_report(
    storage: Storage,
    start: date,
    end: date,
    feature_codes: list[str] | None = None,
    active_only: bool = True,
    required_coverage_ratio: Decimal = Decimal("1.0000"),
    krx_trading_days: KrxTradingDayProvider | None = None,
) -> CommonFeatureReadinessReport:
    """Build a strict readiness report for common feature active transition."""
    coverage_report = build_common_feature_coverage_report(
        storage=storage,
        start=start,
        end=end,
        feature_codes=feature_codes,
        active_only=active_only,
        krx_trading_days=krx_trading_days,
    )
    report = CommonFeatureReadinessReport(
        target_count=coverage_report.target_count,
        errors=dict(coverage_report.errors),
    )

    for coverage_row in coverage_report.rows:
        blockers = _readiness_blockers(
            coverage_row,
            required_coverage_ratio=required_coverage_ratio,
        )
        report.rows.append(
            CommonFeatureReadinessRow(
                feature_code=coverage_row.feature_code,
                feature_name_kr=coverage_row.feature_name_kr,
                target_count=coverage_row.target_count,
                fact_count=coverage_row.fact_count,
                non_null_count=coverage_row.non_null_count,
                null_count=coverage_row.null_count,
                missing_count=coverage_row.missing_count,
                coverage_ratio=coverage_row.coverage_ratio,
                pit_violation_count=coverage_row.pit_violation_count,
                required_coverage_ratio=required_coverage_ratio,
                ready=not blockers,
                blockers=tuple(blockers),
            )
        )

    report.rows.sort(key=lambda row: (not row.ready, row.feature_code))
    return report


def _readiness_blockers(
    row: CommonFeatureCoverageRow,
    *,
    required_coverage_ratio: Decimal,
) -> list[str]:
    blockers: list[str] = []
    if row.target_count == 0:
        blockers.append("target_count=0")
    if row.coverage_ratio < required_coverage_ratio:
        blockers.append(
            f"coverage_ratio={row.coverage_ratio} < required={required_coverage_ratio}"
        )
    if row.null_count > 0:
        blockers.append(f"null_count={row.null_count}")
    if row.missing_count > 0:
        blockers.append(f"missing_count={row.missing_count}")
    if row.pit_violation_count > 0:
        blockers.append(f"pit_violation_count={row.pit_violation_count}")
    return blockers
