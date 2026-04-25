"""Use-case: Report normalized metric coverage by target period."""

from __future__ import annotations

from decimal import Decimal

from krx_collector.domain.models import MetricCoverageReport, MetricCoverageRow
from krx_collector.ports.storage import Storage


def build_metric_coverage_report(
    storage: Storage,
    bsns_years: list[int],
    reprt_codes: list[str],
    tickers: list[str] | None = None,
) -> MetricCoverageReport:
    """Build a simple coverage report over normalized canonical metric facts."""
    report = MetricCoverageReport()
    catalog = storage.get_metric_catalog_entries()
    facts = storage.get_stock_metric_facts(bsns_years, reprt_codes, tickers)
    financial_rows = storage.get_dart_financial_statement_raw(bsns_years, reprt_codes, tickers)

    targets = {(row.ticker, row.bsns_year, row.reprt_code) for row in financial_rows if row.ticker}
    report.target_count = len(targets)

    covered_by_metric: dict[str, set[tuple[str, int, str]]] = {}
    for fact in facts:
        covered_by_metric.setdefault(fact.metric_code, set()).add(
            (fact.ticker, fact.bsns_year, fact.reprt_code)
        )

    for entry in catalog:
        covered = len(covered_by_metric.get(entry.metric_code, set()))
        missing = max(report.target_count - covered, 0)
        ratio = Decimal("0")
        if report.target_count > 0:
            ratio = (Decimal(covered) / Decimal(report.target_count)).quantize(Decimal("0.0001"))
        report.rows.append(
            MetricCoverageRow(
                metric_code=entry.metric_code,
                metric_name=entry.metric_name,
                target_count=report.target_count,
                covered_count=covered,
                missing_count=missing,
                coverage_ratio=ratio,
            )
        )

    report.rows.sort(key=lambda row: (row.coverage_ratio, row.metric_code), reverse=True)
    return report
