from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from krx_collector.domain.enums import RunStatus, RunType, Source
from krx_collector.domain.models import (
    CommonFeatureFetchResult,
    CommonFeatureObservation,
    CommonFeatureSeries,
    IngestionRun,
    UpsertResult,
)
from krx_collector.service.sync_common_features import sync_common_features


def _krx_days(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _series(
    series_id: str,
    source: Source = Source.PYKRX,
    policy: str = "next_krx_session",
    source_timezone: str = "Asia/Seoul",
) -> CommonFeatureSeries:
    return CommonFeatureSeries(
        series_id=series_id,
        source=source,
        source_series_key=series_id,
        category="market",
        frequency="D",
        name_kr=series_id,
        unit="point",
        availability_policy=policy,
        source_timezone=source_timezone,
    )


def _observation(
    series_id: str,
    source: Source = Source.PYKRX,
    observation_date: date = date(2026, 6, 8),
    value: str = "2910.42",
) -> CommonFeatureObservation:
    return CommonFeatureObservation(
        source=source,
        series_id=series_id,
        observation_date=observation_date,
        frequency="D",
        fetched_at=datetime(2026, 6, 8, 18, 30, tzinfo=UTC),
        value_numeric=Decimal(value),
        raw_payload={"value": value},
    )


class MockCommonFeatureProvider:
    def __init__(
        self,
        source: Source,
        results: dict[str, CommonFeatureFetchResult],
    ) -> None:
        self._source = source
        self.results = results
        self.calls: list[tuple[str, date, date]] = []

    def source(self) -> Source:
        return self._source

    def fetch_series(
        self,
        series: CommonFeatureSeries,
        start: date,
        end: date,
    ) -> CommonFeatureFetchResult:
        self.calls.append((series.series_id, start, end))
        return self.results[series.series_id]


class MockCommonFeatureStorage:
    def __init__(
        self,
        series: list[CommonFeatureSeries],
        observation_counts: dict[str, int] | None = None,
    ) -> None:
        self.series = series
        self.observation_counts = observation_counts or {}
        self.observations: list[CommonFeatureObservation] = []
        self.runs: list[IngestionRun] = []
        self.series_query: tuple[list[Source] | None, list[str] | None, bool] | None = None
        self.count_queries: list[
            tuple[list[str] | None, date | None, date | None, Source | None]
        ] = []

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)

    def get_common_feature_series(
        self,
        sources: list[Source] | None = None,
        series_ids: list[str] | None = None,
        active_only: bool = True,
    ) -> list[CommonFeatureSeries]:
        self.series_query = (sources, series_ids, active_only)
        return list(self.series)

    def upsert_common_feature_observations(
        self,
        records: list[CommonFeatureObservation],
    ) -> UpsertResult:
        self.observations.extend(records)
        return UpsertResult(updated=len(records))

    def count_common_feature_observations(
        self,
        series_ids: list[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        source: Source | None = None,
    ) -> dict[str, int]:
        self.count_queries.append((series_ids, start, end, source))
        if series_ids:
            return {
                series_id: self.observation_counts.get(series_id, 0)
                for series_id in series_ids
            }
        return dict(self.observation_counts)


def test_sync_common_features_writes_observations_with_service_availability() -> None:
    storage = MockCommonFeatureStorage([_series("market_kospi")])
    provider = MockCommonFeatureProvider(
        Source.PYKRX,
        {
            "market_kospi": CommonFeatureFetchResult(
                records=[_observation("market_kospi")]
            )
        },
    )

    result = sync_common_features(
        providers=[provider],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.errors == {}
    assert result.series_processed == 1
    assert result.requests_attempted == 1
    assert result.rows_upserted == 1
    assert storage.observations[0].available_from_date == date(2026, 6, 9)
    assert storage.observations[0].unit == "point"
    assert storage.runs[-1].run_type == RunType.COMMON_FEATURE_SYNC
    assert storage.runs[-1].status == RunStatus.SUCCESS
    assert storage.runs[-1].counts["rows_upserted"] == 1


def test_sync_common_features_records_partial_run_for_series_error() -> None:
    storage = MockCommonFeatureStorage([_series("market_kospi"), _series("market_kosdaq")])
    provider = MockCommonFeatureProvider(
        Source.PYKRX,
        {
            "market_kospi": CommonFeatureFetchResult(
                records=[_observation("market_kospi")]
            ),
            "market_kosdaq": CommonFeatureFetchResult(error="provider failed"),
        },
    )

    result = sync_common_features(
        providers=[provider],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.rows_upserted == 1
    assert result.requests_attempted == 2
    assert result.errors == {"market_kosdaq": "provider failed"}
    assert storage.runs[-1].status == RunStatus.PARTIAL
    assert storage.runs[-1].counts["partial_failure_count"] == 1


def test_sync_common_features_applies_source_and_series_filters() -> None:
    storage = MockCommonFeatureStorage(
        [
            _series("market_kospi", source=Source.PYKRX),
            _series(
                "global_sp500",
                source=Source.FDR,
                policy="same_krx_session_morning",
                source_timezone="America/New_York",
            ),
            _series(
                "global_vix",
                source=Source.FDR,
                policy="same_krx_session_morning",
                source_timezone="America/New_York",
            ),
        ]
    )
    pykrx_provider = MockCommonFeatureProvider(
        Source.PYKRX,
        {"market_kospi": CommonFeatureFetchResult(records=[_observation("market_kospi")])},
    )
    fdr_provider = MockCommonFeatureProvider(
        Source.FDR,
        {
            "global_sp500": CommonFeatureFetchResult(
                records=[
                    _observation(
                        "global_sp500",
                        source=Source.FDR,
                        observation_date=date(2026, 6, 5),
                    )
                ]
            )
        },
    )

    result = sync_common_features(
        providers=[pykrx_provider, fdr_provider],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 5),
        end=date(2026, 6, 8),
        sources=[Source.FDR],
        series_ids=["global_sp500"],
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.errors == {}
    assert result.series_processed == 1
    assert pykrx_provider.calls == []
    assert fdr_provider.calls == [("global_sp500", date(2026, 6, 5), date(2026, 6, 8))]
    assert storage.series_query == ([Source.FDR], ["global_sp500"], True)
    assert storage.observations[0].source == Source.FDR
    assert storage.observations[0].available_from_date == date(2026, 6, 8)


def test_sync_common_features_can_include_inactive_series_for_explicit_smoke() -> None:
    storage = MockCommonFeatureStorage([_series("rate_kr_gov3y", source=Source.ECOS)])
    provider = MockCommonFeatureProvider(
        Source.ECOS,
        {
            "rate_kr_gov3y": CommonFeatureFetchResult(
                records=[
                    _observation(
                        "rate_kr_gov3y",
                        source=Source.ECOS,
                        observation_date=date(2026, 6, 8),
                        value="3.25",
                    )
                ]
            )
        },
    )

    result = sync_common_features(
        providers=[provider],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
        sources=[Source.ECOS],
        series_ids=["rate_kr_gov3y"],
        active_only=False,
        force=True,
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.errors == {}
    assert result.series_processed == 1
    assert provider.calls == [("rate_kr_gov3y", date(2026, 6, 8), date(2026, 6, 8))]
    assert storage.series_query == ([Source.ECOS], ["rate_kr_gov3y"], False)
    assert storage.runs[-1].params["active_only"] is False
    assert storage.observations[0].source == Source.ECOS


def test_sync_common_features_skips_series_with_existing_coverage() -> None:
    storage = MockCommonFeatureStorage(
        [_series("market_kospi")],
        observation_counts={"market_kospi": 1},
    )
    provider = MockCommonFeatureProvider(
        Source.PYKRX,
        {
            "market_kospi": CommonFeatureFetchResult(
                records=[_observation("market_kospi")]
            )
        },
    )

    result = sync_common_features(
        providers=[provider],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.errors == {}
    assert result.requests_attempted == 0
    assert result.requests_skipped == 1
    assert result.rows_upserted == 0
    assert provider.calls == []
    assert storage.count_queries == [
        (["market_kospi"], date(2026, 6, 8), date(2026, 6, 8), Source.PYKRX)
    ]
    assert storage.runs[-1].status == RunStatus.SUCCESS


def test_sync_common_features_force_fetches_even_with_existing_coverage() -> None:
    storage = MockCommonFeatureStorage(
        [_series("market_kospi")],
        observation_counts={"market_kospi": 1},
    )
    provider = MockCommonFeatureProvider(
        Source.PYKRX,
        {
            "market_kospi": CommonFeatureFetchResult(
                records=[_observation("market_kospi")]
            )
        },
    )

    result = sync_common_features(
        providers=[provider],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
        force=True,
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.requests_attempted == 1
    assert result.requests_skipped == 0
    assert result.rows_upserted == 1
    assert provider.calls == [("market_kospi", date(2026, 6, 8), date(2026, 6, 8))]
    assert storage.count_queries == []


def test_sync_common_features_records_missing_provider_as_partial() -> None:
    storage = MockCommonFeatureStorage([_series("rate_kr_gov3y", source=Source.ECOS)])

    result = sync_common_features(
        providers=[],
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
        rate_limit_seconds=0.0,
        krx_trading_days=_krx_days,
    )

    assert result.requests_attempted == 0
    assert result.requests_skipped == 1
    assert result.rows_upserted == 0
    assert "rate_kr_gov3y" in result.errors
    assert "source ECOS" in result.errors["rate_kr_gov3y"]
    assert storage.runs[-1].status == RunStatus.PARTIAL
