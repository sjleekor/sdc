"""Freshness status for ``ops freshness-report``, and the staleness gate on it.

Raw-collection status only. The freshness gate over the *derived* tables moved to
the compute node as ``research.etl.marts.reports.freshness_violations`` (refactor
§4, decision 6); what is here covers the raw layer the collectors write.

The report alone is a human-read artifact that always succeeds, which is why
:func:`evaluate_staleness` exists. A collection run that never happened leaves no
``ingestion_runs`` row and no error anywhere — sj2-server was down over the
2026-08-14 evening window and the whole KRX chain plus the common sync simply did
not run. Every raw table sat a day behind and nothing said so. Only the data
itself carries that evidence, so the check has to be "is the newest row as new as
the calendar says it should be", not "did any run fail".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from krx_collector.domain.enums import Source
from krx_collector.domain.models import IngestionRun
from krx_collector.infra.calendar.trading_days import get_trading_days
from krx_collector.ports.storage import Storage
from krx_collector.service.sync_krx_flows import FLOW_METRIC_GROUPS
from krx_collector.util.time import now_kst, today_kst

# Both sources write the same security-flow metric codes: KRX until 2026-08,
# KIS after. Asking only about KRX would report the flow domain as frozen on
# changeover day — the gate would fire every morning on data that is current.
FLOW_SOURCES: tuple[Source, ...] = (Source.KRX, Source.KIS)


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
    market_cap_latest_date: date | None = None
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
        sources=FLOW_SOURCES,
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
        market_cap_latest_date=storage.get_latest_market_cap_date(),
        flow_metric_latest_dates=flow_metric_latest_dates,
        flow_group_latest_dates=flow_group_latest_dates,
        common_series=common_series,
        dart_year_ranges=dart_year_ranges,
        running_runs=storage.get_running_ingestion_runs(limit=running_limit),
        generated_at=now_kst(),
    )


# Sources whose observations land on KRX trading days. Everything else (ECOS,
# FRED) publishes on its own calendar with a release lag, so a trading-day
# budget would flag it every single day and the gate would be ignored.
TRADING_DAY_SOURCES = frozenset({Source.KRX, Source.KIS, Source.FDR, Source.PYKRX})

DEFAULT_MAX_LAG_TRADING_DAYS = 1
DEFAULT_MAX_LAG_CALENDAR_DAYS = 14


@dataclass(frozen=True, slots=True)
class StaleFinding:
    """One domain whose newest row is older than its budget allows."""

    domain: str
    latest: date | None
    required_at_or_after: date
    cadence: str  # "trading" | "calendar"

    def describe(self) -> str:
        latest = self.latest.isoformat() if self.latest else "(empty)"
        return (
            f"{self.domain}: latest={latest} "
            f"but needs >= {self.required_at_or_after.isoformat()} ({self.cadence})"
        )


def _trading_day_threshold(
    as_of: date,
    max_lag_trading_days: int,
    holidays: set[date] | None,
) -> date | None:
    """The oldest trading day a collector may still be sitting on.

    ``max_lag_trading_days=1`` means the most recent session must be stored, so
    this check belongs *after* the evening collection window, not before it.
    """
    lag = max(1, max_lag_trading_days)
    # 60 calendar days covers the longest KRX holiday cluster with room to spare.
    sessions = get_trading_days(as_of - timedelta(days=60), as_of, holidays=holidays)
    if not sessions:
        return None
    return sessions[-lag] if len(sessions) >= lag else sessions[0]


def evaluate_staleness(
    report: FreshnessReport,
    *,
    as_of: date | None = None,
    max_lag_trading_days: int = DEFAULT_MAX_LAG_TRADING_DAYS,
    max_lag_calendar_days: int = DEFAULT_MAX_LAG_CALENDAR_DAYS,
    holidays: set[date] | None = None,
) -> list[StaleFinding]:
    """Return the domains in *report* that are behind their freshness budget.

    An empty list means every domain is current. A domain with no rows at all
    counts as stale rather than as "nothing to check" — an empty table is the
    loudest possible version of the problem, not an exemption from it.
    """
    as_of = as_of or today_kst()
    findings: list[StaleFinding] = []

    trading_threshold = _trading_day_threshold(as_of, max_lag_trading_days, holidays)
    calendar_threshold = as_of - timedelta(days=max(1, max_lag_calendar_days))

    def check(domain: str, latest: date | None, threshold: date | None, cadence: str) -> None:
        if threshold is None:
            return
        if latest is None or latest < threshold:
            findings.append(
                StaleFinding(
                    domain=domain,
                    latest=latest,
                    required_at_or_after=threshold,
                    cadence=cadence,
                )
            )

    check("daily_ohlcv", report.price_latest_date, trading_threshold, "trading")
    check("daily_market_cap", report.market_cap_latest_date, trading_threshold, "trading")

    for group, latest in sorted(report.flow_group_latest_dates.items()):
        check(f"krx_security_flow_raw:{group}", latest, trading_threshold, "trading")

    for row in sorted(report.common_series, key=lambda item: item.series_id):
        if row.source in TRADING_DAY_SOURCES:
            check(
                f"common:{row.series_id}",
                row.latest_observation_date,
                trading_threshold,
                "trading",
            )
        else:
            check(
                f"common:{row.series_id}",
                row.latest_observation_date,
                calendar_threshold,
                "calendar",
            )

    return findings
