from datetime import date, datetime, timedelta

from krx_collector.domain.enums import RunStatus, RunType, Source
from krx_collector.domain.models import CommonFeatureSeries, IngestionRun
from krx_collector.service.freshness import assert_common_freshness
from krx_collector.util.time import KST


def _series(
    series_id: str,
    source: Source,
    *,
    frequency: str = "D",
    manual_lag_days: int = 0,
    max_stale_business_days: int = 5,
) -> CommonFeatureSeries:
    return CommonFeatureSeries(
        series_id=series_id,
        source=source,
        source_series_key=series_id,
        category="market",
        frequency=frequency,
        name_kr=series_id,
        unit="point",
        manual_lag_days=manual_lag_days,
        max_stale_business_days=max_stale_business_days,
    )


def _run(
    source: Source,
    *,
    ended_at: datetime,
    status: RunStatus = RunStatus.SUCCESS,
) -> IngestionRun:
    return IngestionRun(
        run_type=RunType.COMMON_FEATURE_SYNC,
        started_at=ended_at - timedelta(minutes=5),
        ended_at=ended_at,
        status=status,
        params={"sources": [source.value]},
    )


class MockFreshnessStorage:
    def __init__(
        self,
        *,
        series: list[CommonFeatureSeries],
        latest_by_series: dict[str, date],
        runs: list[IngestionRun],
    ) -> None:
        self.series = series
        self.latest_by_series = latest_by_series
        self.runs = runs

    def get_common_feature_series(
        self,
        sources: list[Source] | None = None,
        series_ids: list[str] | None = None,
        active_only: bool = True,
    ) -> list[CommonFeatureSeries]:
        return [
            series
            for series in self.series
            if (not sources or series.source in sources)
            and (not series_ids or series.series_id in series_ids)
            and (not active_only or series.active)
        ]

    def get_common_feature_observation_max_dates(
        self,
        sources: list[Source] | None = None,
        series_ids: list[str] | None = None,
    ) -> dict[str, date]:
        selected_ids = set(series_ids or self.latest_by_series)
        return {
            series_id: latest
            for series_id, latest in self.latest_by_series.items()
            if series_id in selected_ids
        }

    def get_recent_ingestion_runs(
        self,
        run_type: RunType,
        limit: int = 20,
    ) -> list[IngestionRun]:
        return [run for run in self.runs if run.run_type == run_type][:limit]


def test_assert_common_freshness_passes_with_daily_and_macro_lag_policies() -> None:
    now = datetime(2026, 6, 13, 10, 0, tzinfo=KST)
    storage = MockFreshnessStorage(
        series=[
            _series("global_sp500", Source.FDR),
            _series("macro_cpi", Source.FRED, frequency="M"),
        ],
        latest_by_series={
            "global_sp500": date(2026, 6, 12),
            "macro_cpi": date(2026, 5, 20),
        },
        runs=[
            _run(Source.FDR, ended_at=now - timedelta(hours=2)),
            _run(Source.FRED, ended_at=now - timedelta(hours=3)),
        ],
    )

    result = assert_common_freshness(
        storage=storage,  # type: ignore[arg-type]
        sources=[Source.FDR, Source.FRED],
        end=date(2026, 6, 13),
        daily_max_lag_days=2,
        macro_max_lag_days=45,
        max_run_age_hours=30,
        now=now,
    )

    assert result.ok is True
    assert result.checked_series == 2
    assert result.violations == []


def test_assert_common_freshness_reports_stale_series_and_missing_run() -> None:
    now = datetime(2026, 6, 13, 10, 0, tzinfo=KST)
    storage = MockFreshnessStorage(
        series=[_series("market_kospi", Source.KRX)],
        latest_by_series={"market_kospi": date(2026, 6, 1)},
        runs=[],
    )

    result = assert_common_freshness(
        storage=storage,  # type: ignore[arg-type]
        sources=[Source.KRX],
        end=date(2026, 6, 13),
        daily_max_lag_days=2,
        max_run_age_hours=30,
        now=now,
    )

    assert result.ok is False
    assert [(item.check, item.series_id) for item in result.violations] == [
        ("latest_observation", "market_kospi"),
        ("last_successful_run", None),
    ]


def test_assert_common_freshness_uses_daily_lag_for_daily_fred_and_ecos_series() -> None:
    now = datetime(2026, 6, 13, 10, 0, tzinfo=KST)
    storage = MockFreshnessStorage(
        series=[
            _series("rate_us10y", Source.FRED, max_stale_business_days=5),
            _series("fx_usdkrw_ecos", Source.ECOS, max_stale_business_days=10),
            _series(
                "macro_cpi",
                Source.ECOS,
                frequency="M",
                manual_lag_days=20,
                max_stale_business_days=45,
            ),
        ],
        latest_by_series={
            "rate_us10y": date(2026, 6, 1),
            "fx_usdkrw_ecos": date(2026, 6, 1),
            "macro_cpi": date(2026, 5, 20),
        },
        runs=[
            _run(Source.FRED, ended_at=now - timedelta(hours=2)),
            _run(Source.ECOS, ended_at=now - timedelta(hours=2)),
        ],
    )

    result = assert_common_freshness(
        storage=storage,  # type: ignore[arg-type]
        sources=[Source.FRED, Source.ECOS],
        end=date(2026, 6, 13),
        daily_max_lag_days=2,
        macro_max_lag_days=45,
        max_run_age_hours=30,
        now=now,
    )

    latest_observation_violations = [
        item
        for item in result.violations
        if item.check == "latest_observation"
    ]
    assert [item.series_id for item in latest_observation_violations] == [
        "rate_us10y",
        "fx_usdkrw_ecos",
    ]
    assert all("allowed 45" not in item.message for item in latest_observation_violations)


def test_assert_common_freshness_filters_runs_by_source_params() -> None:
    now = datetime(2026, 6, 13, 10, 0, tzinfo=KST)
    storage = MockFreshnessStorage(
        series=[_series("market_kospi", Source.KRX)],
        latest_by_series={"market_kospi": date(2026, 6, 12)},
        runs=[
            _run(Source.FDR, ended_at=now - timedelta(hours=1)),
            _run(Source.KRX, ended_at=now - timedelta(hours=40)),
        ],
    )

    result = assert_common_freshness(
        storage=storage,  # type: ignore[arg-type]
        sources=[Source.KRX],
        end=date(2026, 6, 13),
        max_run_age_hours=30,
        now=now,
    )

    assert result.ok is False
    assert result.run_freshness[0].source == Source.KRX
    assert result.run_freshness[0].age_hours == 40
    assert result.violations[-1].check == "last_successful_run"
