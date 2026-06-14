"""Read-only freshness reporting for scheduler and smoke checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from krx_collector.domain.enums import RunStatus, RunType, Source
from krx_collector.domain.models import CommonFeatureSeries, IngestionRun
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
    common_fact_latest_dates: dict[str, date] = field(default_factory=dict)
    dart_year_ranges: list[YearRangeFreshness] = field(default_factory=list)
    running_runs: list[IngestionRun] = field(default_factory=list)
    generated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class CommonFreshnessViolation:
    """One failed common-feature freshness check."""

    source: Source
    check: str
    message: str
    series_id: str | None = None


@dataclass(frozen=True, slots=True)
class CommonSourceRunFreshness:
    """Latest successful common sync run observed for one source."""

    source: Source
    run_id: str | None
    ended_at: datetime | None
    age_hours: float | None


@dataclass(frozen=True, slots=True)
class CommonFreshnessAssertResult:
    """Machine-readable result for common-feature freshness gating."""

    sources: list[Source]
    end: date
    checked_series: int
    run_freshness: list[CommonSourceRunFreshness] = field(default_factory=list)
    violations: list[CommonFreshnessViolation] = field(default_factory=list)
    generated_at: datetime = field(default_factory=now_kst)

    @property
    def ok(self) -> bool:
        """Return ``True`` when every required freshness check passed."""
        return not self.violations


def build_freshness_report(storage: Storage, *, running_limit: int = 20) -> FreshnessReport:
    """Build a read-only summary of latest stored data by collector domain."""
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
        "stock_metric_fact",
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
        common_fact_latest_dates=storage.get_common_feature_daily_fact_max_dates(),
        dart_year_ranges=dart_year_ranges,
        running_runs=storage.get_running_ingestion_runs(limit=running_limit),
        generated_at=now_kst(),
    )


def assert_common_freshness(
    storage: Storage,
    *,
    sources: list[Source],
    end: date,
    max_run_age_hours: int = 30,
    daily_max_lag_days: int = 2,
    macro_max_lag_days: int = 45,
    series_ids: list[str] | None = None,
    now: datetime | None = None,
) -> CommonFreshnessAssertResult:
    """Check whether required common-feature raw inputs are fresh enough.

    The check is intentionally stricter than the human-oriented freshness
    report: it returns a structured result that wrappers can map directly to a
    non-zero scheduler exit before running ``common build-daily``.
    """
    if not sources:
        raise ValueError("at least one common feature source is required")
    if max_run_age_hours <= 0:
        raise ValueError("max_run_age_hours must be positive")
    if daily_max_lag_days < 0:
        raise ValueError("daily_max_lag_days must be >= 0")
    if macro_max_lag_days < 0:
        raise ValueError("macro_max_lag_days must be >= 0")

    generated_at = now or now_kst()
    series_rows = storage.get_common_feature_series(
        sources=sources,
        series_ids=series_ids,
        active_only=True,
    )
    latest_by_series = storage.get_common_feature_observation_max_dates(
        sources=sources,
        series_ids=[series.series_id for series in series_rows],
    )

    violations: list[CommonFreshnessViolation] = []
    source_series_counts = {source: 0 for source in sources}
    for series in series_rows:
        if series.source in source_series_counts:
            source_series_counts[series.source] += 1

        latest = latest_by_series.get(series.series_id)
        max_lag_days = _common_series_max_lag_days(
            series,
            daily_max_lag_days=daily_max_lag_days,
            macro_max_lag_days=macro_max_lag_days,
        )
        if latest is None:
            violations.append(
                CommonFreshnessViolation(
                    source=series.source,
                    series_id=series.series_id,
                    check="latest_observation",
                    message="no raw observations found for active common source series",
                )
            )
            continue

        lag_days = (end - latest).days
        if lag_days > max_lag_days:
            violations.append(
                CommonFreshnessViolation(
                    source=series.source,
                    series_id=series.series_id,
                    check="latest_observation",
                    message=(
                        f"latest observation {latest.isoformat()} is {lag_days} days "
                        f"behind {end.isoformat()} (allowed {max_lag_days})"
                    ),
                )
            )

    for source, count in source_series_counts.items():
        if count == 0:
            violations.append(
                CommonFreshnessViolation(
                    source=source,
                    check="series_catalog",
                    message="no active common source series found for required source",
                )
            )

    run_freshness = _build_common_run_freshness(
        storage=storage,
        sources=sources,
        max_run_age_hours=max_run_age_hours,
        now=generated_at,
        violations=violations,
    )

    return CommonFreshnessAssertResult(
        sources=sources,
        end=end,
        checked_series=len(series_rows),
        run_freshness=run_freshness,
        violations=violations,
        generated_at=generated_at,
    )


def _common_series_max_lag_days(
    series: CommonFeatureSeries,
    *,
    daily_max_lag_days: int,
    macro_max_lag_days: int,
) -> int:
    if series.frequency.upper() == "D":
        return max(
            daily_max_lag_days,
            series.manual_lag_days,
            series.max_stale_business_days,
        )
    return max(
        macro_max_lag_days,
        series.manual_lag_days,
        series.max_stale_business_days,
    )


def _build_common_run_freshness(
    *,
    storage: Storage,
    sources: list[Source],
    max_run_age_hours: int,
    now: datetime,
    violations: list[CommonFreshnessViolation],
) -> list[CommonSourceRunFreshness]:
    recent_runs = storage.get_recent_ingestion_runs(
        run_type=RunType.COMMON_FEATURE_SYNC,
        limit=100,
    )
    run_freshness: list[CommonSourceRunFreshness] = []
    for source in sources:
        run = _latest_successful_common_run_for_source(recent_runs, source)
        if run is None:
            run_freshness.append(
                CommonSourceRunFreshness(
                    source=source,
                    run_id=None,
                    ended_at=None,
                    age_hours=None,
                )
            )
            violations.append(
                CommonFreshnessViolation(
                    source=source,
                    check="last_successful_run",
                    message="no successful common feature sync run found for required source",
                )
            )
            continue

        completed_at = run.ended_at or run.started_at
        age_hours = _age_hours(now, completed_at) if completed_at is not None else None
        run_freshness.append(
            CommonSourceRunFreshness(
                source=source,
                run_id=run.run_id,
                ended_at=completed_at,
                age_hours=age_hours,
            )
        )
        if age_hours is None or age_hours > max_run_age_hours:
            age_text = "-" if age_hours is None else f"{age_hours:.1f}"
            violations.append(
                CommonFreshnessViolation(
                    source=source,
                    check="last_successful_run",
                    message=(
                        f"latest successful common sync run age {age_text}h exceeds "
                        f"{max_run_age_hours}h"
                    ),
                )
            )
    return run_freshness


def _latest_successful_common_run_for_source(
    runs: list[IngestionRun],
    source: Source,
) -> IngestionRun | None:
    for run in runs:
        if run.status != RunStatus.SUCCESS:
            continue
        run_sources = _run_common_sources(run)
        if run_sources is not None and source not in run_sources:
            continue
        return run
    return None


def _run_common_sources(run: IngestionRun) -> set[Source] | None:
    params = run.params or {}
    raw_sources = params.get("sources")
    if raw_sources is None:
        return None
    if not isinstance(raw_sources, list):
        raw_sources = [raw_sources]

    sources: set[Source] = set()
    for raw_source in raw_sources:
        try:
            sources.add(Source(str(raw_source).upper()))
        except ValueError:
            continue
    return sources


def _age_hours(now: datetime, then: datetime) -> float:
    if then.tzinfo is None:
        then = then.replace(tzinfo=now.tzinfo)
    else:
        then = then.astimezone(now.tzinfo)
    return max(0.0, (now - then).total_seconds() / 3600.0)
