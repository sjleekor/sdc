"""Read-only freshness status report for the ``ops freshness-report`` command.

Raw-collection status only. The freshness *gate* that used to live here
(``assert_common_freshness``) moved to the compute node as
``research.etl.marts.reports.freshness_violations`` (refactor §4, decision 6), and
the derived tables it referenced (``stock_metric_fact`` /
``common_feature_daily_fact``) are recomputed by the DuckDB marts, so they are no
longer surfaced here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from krx_collector.domain.enums import Source
from krx_collector.domain.models import IngestionRun
from krx_collector.ports.storage import Storage
from krx_collector.service.sync_krx_flows import FLOW_METRIC_GROUPS
from krx_collector.util.time import now_kst


@dataclass(frozen=True, slots=True)
class YearRangeFreshness:
    table_name: str
    min_year: int | None = None
    max_year: int | None = None
    rows: int = 0


@dataclass(frozen=True, slots=True)
class CommonSeriesFreshness:
    series_id: str
    source: Source
    latest_observation_date: date | None


@dataclass(frozen=True, slots=True)
class FreshnessReport:
    price_latest_date: date | None
    flow_metric_latest_dates: dict[str, date] = field(default_factory=dict)
    flow_group_latest_dates: dict[str, date | None] = field(default_factory=dict)
    common_series: list[CommonSeriesFreshness] = field(default_factory=list)
    dart_year_ranges: list[YearRangeFreshness] = field(default_factory=list)
    running_runs: list[IngestionRun] = field(default_factory=list)
    generated_at: datetime | None = None


def build_freshness_report(storage: Storage, *, running_limit: int = 20) -> FreshnessReport:
    """Build a read-only summary of latest stored RAW data by collector domain."""
    metric_codes = sorted({metric for metrics in FLOW_METRIC_GROUPS.values() for metric in metrics})
    flow_metric_latest_dates = storage.get_krx_security_flow_metric_max_dates(
        metric_codes=metric_codes,
        source=Source.KRX,
    )
    flow_group_latest_dates: dict[str, date | None] = {}
    for group, metrics in FLOW_METRIC_GROUPS.items():
        dates = [flow_metric_latest_dates.get(metric) for metric in metrics]
        present_dates = [item for item in dates if item is not None]
        flow_group_latest_dates[group] = min(present_dates) if present_dates else None

    series_rows = storage.get_common_feature_series(active_only=True)
    observation_latest = storage.get_common_feature_observation_max_dates(
        series_ids=[series.series_id for series in series_rows]
    )
    common_series = [
        CommonSeriesFreshness(
            series_id=series.series_id,
            source=series.source,
            latest_observation_date=observation_latest.get(series.series_id),
        )
        for series in series_rows
    ]

    dart_year_ranges: list[YearRangeFreshness] = []
    for table_name in (
        "dart_financial_statement_raw",
        "dart_share_count_raw",
        "dart_shareholder_return_raw",
        "dart_xbrl_document",
        "dart_xbrl_fact_raw",
    ):
        year_range = storage.get_table_bsns_year_range(table_name)
        if year_range is None:
            dart_year_ranges.append(YearRangeFreshness(table_name=table_name))
        else:
            min_year, max_year, rows = year_range
            dart_year_ranges.append(
                YearRangeFreshness(
                    table_name=table_name,
                    min_year=min_year,
                    max_year=max_year,
                    rows=rows,
                )
            )

    return FreshnessReport(
        price_latest_date=storage.get_latest_daily_price_date(),
        flow_metric_latest_dates=flow_metric_latest_dates,
        flow_group_latest_dates=flow_group_latest_dates,
        common_series=common_series,
        dart_year_ranges=dart_year_ranges,
        running_runs=storage.get_running_ingestion_runs(limit=running_limit),
        generated_at=now_kst(),
    )
