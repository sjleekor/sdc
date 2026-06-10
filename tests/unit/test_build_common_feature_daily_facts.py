from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from krx_collector.domain.enums import RunStatus, RunType, Source
from krx_collector.domain.models import (
    CommonFeatureCatalogEntry,
    CommonFeatureDailyFact,
    CommonFeatureObservation,
    CommonFeatureSeries,
    IngestionRun,
    UpsertResult,
)
from krx_collector.service.build_common_feature_daily_facts import (
    build_common_feature_daily_facts,
)


def _krx_days(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _series(
    series_id: str = "market_kospi",
    *,
    source: Source = Source.PYKRX,
    max_stale_business_days: int = 5,
    active: bool = True,
) -> CommonFeatureSeries:
    return CommonFeatureSeries(
        series_id=series_id,
        source=source,
        source_series_key=series_id,
        category="market",
        frequency="D",
        name_kr=series_id,
        unit="point",
        availability_policy="next_krx_session",
        max_stale_business_days=max_stale_business_days,
        active=active,
    )


def _feature(
    feature_code: str = "market_kospi_close",
    *,
    transform_code: str = "level",
    series_id: str = "market_kospi",
    active: bool = True,
) -> CommonFeatureCatalogEntry:
    return CommonFeatureCatalogEntry(
        feature_code=feature_code,
        feature_name_kr=feature_code,
        category="market",
        unit="point",
        transform_code=transform_code,
        input_series_ids=(series_id,),
        active=active,
    )


def _obs(
    raw_id: int,
    series_id: str,
    observation_date: date,
    available_from_date: date,
    value: str,
    *,
    source: Source = Source.PYKRX,
    period_end_date: date | None = None,
    release_date: date | None = None,
    vintage: str = "",
) -> CommonFeatureObservation:
    return CommonFeatureObservation(
        raw_id=raw_id,
        source=source,
        series_id=series_id,
        observation_date=observation_date,
        period_end_date=period_end_date or observation_date,
        release_date=release_date,
        available_from_date=available_from_date,
        vintage=vintage,
        value_numeric=Decimal(value),
        unit="point",
        frequency="D",
        fetched_at=datetime(2026, 6, 9, 8, 0, tzinfo=UTC),
    )


class MockCommonFeatureBuildStorage:
    def __init__(
        self,
        *,
        series: list[CommonFeatureSeries],
        catalog: list[CommonFeatureCatalogEntry],
        observations: list[CommonFeatureObservation],
    ) -> None:
        self.series = series
        self.catalog = catalog
        self.observations = observations
        self.facts: list[CommonFeatureDailyFact] = []
        self.runs: list[IngestionRun] = []
        self.observation_query: tuple[
            list[str] | None,
            date | None,
            date | None,
            Source | None,
            date | None,
        ] | None = None
        self.catalog_query: tuple[list[str] | None, bool] | None = None
        self.series_query: tuple[list[Source] | None, list[str] | None, bool] | None = None

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)

    def get_common_feature_catalog(
        self,
        feature_codes: list[str] | None = None,
        active_only: bool = True,
    ) -> list[CommonFeatureCatalogEntry]:
        self.catalog_query = (feature_codes, active_only)
        return [
            feature
            for feature in self.catalog
            if (not feature_codes or feature.feature_code in feature_codes)
            and (not active_only or feature.active)
        ]

    def get_common_feature_series(
        self,
        sources: list[Source] | None = None,
        series_ids: list[str] | None = None,
        active_only: bool = True,
    ) -> list[CommonFeatureSeries]:
        self.series_query = (sources, series_ids, active_only)
        return [
            series
            for series in self.series
            if (not sources or series.source in sources)
            and (not series_ids or series.series_id in series_ids)
            and (not active_only or series.active)
        ]

    def get_common_feature_observations(
        self,
        series_ids: list[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        source: Source | None = None,
        available_from_end: date | None = None,
    ) -> list[CommonFeatureObservation]:
        self.observation_query = (series_ids, start, end, source, available_from_end)
        return [
            observation
            for observation in self.observations
            if (not series_ids or observation.series_id in series_ids)
            and (start is None or observation.observation_date >= start)
            and (end is None or observation.observation_date <= end)
            and (source is None or observation.source == source)
            and (
                available_from_end is None
                or observation.available_from_date is None
                or observation.available_from_date <= available_from_end
            )
        ]

    def upsert_common_feature_daily_facts(
        self,
        records: list[CommonFeatureDailyFact],
    ) -> UpsertResult:
        self.facts = records
        return UpsertResult(updated=len(records))


def test_build_daily_facts_prevents_same_day_krx_close_leakage() -> None:
    storage = MockCommonFeatureBuildStorage(
        series=[_series("market_kospi")],
        catalog=[_feature("market_kospi_close", series_id="market_kospi")],
        observations=[
            _obs(
                1,
                "market_kospi",
                observation_date=date(2026, 6, 8),
                available_from_date=date(2026, 6, 9),
                value="2910.42",
            )
        ],
    )

    result = build_common_feature_daily_facts(
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 9),
        krx_trading_days=_krx_days,
    )

    assert result.errors == {}
    assert [(fact.feature_date, fact.value_numeric) for fact in storage.facts] == [
        (date(2026, 6, 8), None),
        (date(2026, 6, 9), Decimal("2910.42")),
    ]
    assert storage.facts[0].source_observation_ids == []
    assert storage.facts[1].source_observation_ids == [1]
    assert storage.runs[-1].run_type == RunType.COMMON_FEATURE_BUILD
    assert storage.runs[-1].status == RunStatus.SUCCESS


def test_build_daily_facts_uses_same_morning_global_observation() -> None:
    storage = MockCommonFeatureBuildStorage(
        series=[_series("global_sp500", source=Source.FDR)],
        catalog=[_feature("global_sp500_level", series_id="global_sp500")],
        observations=[
            _obs(
                10,
                "global_sp500",
                observation_date=date(2026, 6, 5),
                available_from_date=date(2026, 6, 8),
                value="6010.25",
                source=Source.FDR,
            )
        ],
    )

    build_common_feature_daily_facts(
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
        krx_trading_days=_krx_days,
    )

    assert [(fact.feature_date, fact.value_numeric) for fact in storage.facts] == [
        (date(2026, 6, 8), Decimal("6010.25"))
    ]
    assert storage.facts[0].asof_available_date == date(2026, 6, 8)


def test_build_daily_facts_computes_return_from_asof_history() -> None:
    storage = MockCommonFeatureBuildStorage(
        series=[_series("market_kospi")],
        catalog=[
            _feature(
                "market_kospi_ret_1d",
                transform_code="ret_1d",
                series_id="market_kospi",
            )
        ],
        observations=[
            _obs(1, "market_kospi", date(2026, 6, 8), date(2026, 6, 9), "100"),
            _obs(2, "market_kospi", date(2026, 6, 9), date(2026, 6, 10), "110"),
        ],
    )

    result = build_common_feature_daily_facts(
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 10),
        end=date(2026, 6, 10),
        krx_trading_days=_krx_days,
    )

    assert result.errors == {}
    assert len(storage.facts) == 1
    assert storage.facts[0].value_numeric == Decimal("0.1")
    assert storage.facts[0].source_observation_ids == [2, 1]


def test_build_daily_facts_writes_null_when_stale_limit_exceeded() -> None:
    storage = MockCommonFeatureBuildStorage(
        series=[_series("market_kospi", max_stale_business_days=1)],
        catalog=[_feature("market_kospi_close", series_id="market_kospi")],
        observations=[
            _obs(1, "market_kospi", date(2026, 6, 8), date(2026, 6, 8), "100")
        ],
    )

    build_common_feature_daily_facts(
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 10),
        krx_trading_days=_krx_days,
    )

    assert [(fact.feature_date, fact.value_numeric) for fact in storage.facts] == [
        (date(2026, 6, 8), Decimal("100")),
        (date(2026, 6, 9), Decimal("100")),
        (date(2026, 6, 10), None),
    ]
    assert storage.facts[2].asof_available_date == date(2026, 6, 8)
    assert storage.facts[2].source_observation_ids == [1]


def test_build_daily_facts_selects_latest_vintage_asof_feature_date() -> None:
    storage = MockCommonFeatureBuildStorage(
        series=[_series("macro_cpi")],
        catalog=[_feature("macro_cpi_level", series_id="macro_cpi")],
        observations=[
            _obs(
                1,
                "macro_cpi",
                observation_date=date(2024, 1, 31),
                period_end_date=date(2024, 1, 31),
                release_date=date(2024, 2, 6),
                available_from_date=date(2024, 2, 6),
                value="100",
                vintage="v1",
            ),
            _obs(
                2,
                "macro_cpi",
                observation_date=date(2024, 1, 31),
                period_end_date=date(2024, 1, 31),
                release_date=date(2024, 2, 7),
                available_from_date=date(2024, 2, 7),
                value="110",
                vintage="v2",
            ),
        ],
    )

    build_common_feature_daily_facts(
        storage=storage,  # type: ignore[arg-type]
        start=date(2024, 2, 6),
        end=date(2024, 2, 7),
        krx_trading_days=_krx_days,
    )

    assert [
        (fact.feature_date, fact.value_numeric, fact.selected_vintage)
        for fact in storage.facts
    ] == [
        (date(2024, 2, 6), Decimal("100"), "v1"),
        (date(2024, 2, 7), Decimal("110"), "v2"),
    ]


def test_build_daily_facts_can_include_inactive_explicit_features() -> None:
    storage = MockCommonFeatureBuildStorage(
        series=[_series("rate_kr_gov3y", source=Source.ECOS, active=False)],
        catalog=[
            _feature(
                "rate_kr_gov3y_level",
                series_id="rate_kr_gov3y",
                active=False,
            )
        ],
        observations=[
            _obs(
                100,
                "rate_kr_gov3y",
                observation_date=date(2024, 1, 2),
                available_from_date=date(2024, 1, 3),
                value="3.24",
                source=Source.ECOS,
            )
        ],
    )

    result = build_common_feature_daily_facts(
        storage=storage,  # type: ignore[arg-type]
        start=date(2024, 1, 3),
        end=date(2024, 1, 3),
        feature_codes=["rate_kr_gov3y_level"],
        active_only=False,
        krx_trading_days=_krx_days,
    )

    assert result.errors == {}
    assert storage.catalog_query == (["rate_kr_gov3y_level"], False)
    assert storage.series_query == (None, ["rate_kr_gov3y"], False)
    assert storage.runs[-1].params["active_only"] is False
    assert [(fact.feature_code, fact.value_numeric) for fact in storage.facts] == [
        ("rate_kr_gov3y_level", Decimal("3.24"))
    ]


def test_build_daily_facts_records_partial_for_unsupported_transform() -> None:
    storage = MockCommonFeatureBuildStorage(
        series=[_series("market_kospi")],
        catalog=[
            _feature(
                "market_kospi_zscore",
                transform_code="zscore_20d",
                series_id="market_kospi",
            )
        ],
        observations=[],
    )

    result = build_common_feature_daily_facts(
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 8),
        end=date(2026, 6, 8),
        krx_trading_days=_krx_days,
    )

    assert result.facts_built == 0
    assert "market_kospi_zscore" in result.errors
    assert storage.runs[-1].status == RunStatus.PARTIAL


def test_build_daily_facts_computes_change_as_absolute_difference() -> None:
    storage = MockCommonFeatureBuildStorage(
        series=[_series("rate_kr_gov3y", source=Source.ECOS)],
        catalog=[
            _feature(
                "rate_kr_gov3y_change_1d",
                transform_code="change_1d",
                series_id="rate_kr_gov3y",
            )
        ],
        observations=[
            _obs(
                1, "rate_kr_gov3y", date(2026, 6, 8), date(2026, 6, 9), "3.25", source=Source.ECOS
            ),
            _obs(
                2, "rate_kr_gov3y", date(2026, 6, 9), date(2026, 6, 10), "3.31", source=Source.ECOS
            ),
        ],
    )

    build_common_feature_daily_facts(
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 10),
        end=date(2026, 6, 10),
        krx_trading_days=_krx_days,
    )

    assert len(storage.facts) == 1
    assert storage.facts[0].value_numeric == Decimal("0.06")
    assert storage.facts[0].source_observation_ids == [2, 1]


def test_build_daily_facts_computes_rolling_return_volatility() -> None:
    # 1-step returns over 3 windows: +0.10, -0.10/1.10, ... use simple values.
    # values 100, 110, 99 -> returns 0.1 and -0.1 ; sample std (ddof=1) of {0.1, -0.1}
    storage = MockCommonFeatureBuildStorage(
        series=[_series("market_kospi")],
        catalog=[
            _feature(
                "market_kospi_vol_2d",
                transform_code="vol_2d",
                series_id="market_kospi",
            )
        ],
        observations=[
            _obs(1, "market_kospi", date(2026, 6, 8), date(2026, 6, 9), "100"),
            _obs(2, "market_kospi", date(2026, 6, 9), date(2026, 6, 10), "110"),
            _obs(3, "market_kospi", date(2026, 6, 10), date(2026, 6, 11), "99"),
        ],
    )

    build_common_feature_daily_facts(
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 11),
        end=date(2026, 6, 11),
        krx_trading_days=_krx_days,
    )

    assert len(storage.facts) == 1
    # returns: 110/100-1 = 0.1 ; 99/110-1 = -0.1 ; mean 0 ; var = (0.1^2 + 0.1^2)/1 = 0.02
    # std = sqrt(0.02)
    expected = Decimal("0.02").sqrt()
    assert storage.facts[0].value_numeric == expected
    # current obs (3) is traced first, then the window's base observations (1, 2).
    assert storage.facts[0].source_observation_ids == [3, 1, 2]


def test_build_daily_facts_volatility_is_null_when_window_too_short() -> None:
    storage = MockCommonFeatureBuildStorage(
        series=[_series("market_kospi")],
        catalog=[
            _feature(
                "market_kospi_vol_20d",
                transform_code="vol_20d",
                series_id="market_kospi",
            )
        ],
        observations=[
            _obs(1, "market_kospi", date(2026, 6, 8), date(2026, 6, 9), "100"),
            _obs(2, "market_kospi", date(2026, 6, 9), date(2026, 6, 10), "110"),
        ],
    )

    build_common_feature_daily_facts(
        storage=storage,  # type: ignore[arg-type]
        start=date(2026, 6, 10),
        end=date(2026, 6, 10),
        krx_trading_days=_krx_days,
    )

    assert len(storage.facts) == 1
    assert storage.facts[0].value_numeric is None


def test_build_daily_facts_computes_yoy_from_calendar_month_match() -> None:
    storage = MockCommonFeatureBuildStorage(
        series=[_series("macro_cpi", source=Source.ECOS)],
        catalog=[
            _feature(
                "macro_cpi_yoy",
                transform_code="yoy",
                series_id="macro_cpi",
            )
        ],
        observations=[
            _obs(
                1,
                "macro_cpi",
                observation_date=date(2024, 1, 31),
                period_end_date=date(2024, 1, 31),
                available_from_date=date(2024, 2, 20),
                value="100",
                source=Source.ECOS,
            ),
            _obs(
                2,
                "macro_cpi",
                observation_date=date(2025, 1, 31),
                period_end_date=date(2025, 1, 31),
                available_from_date=date(2025, 2, 20),
                value="103",
                source=Source.ECOS,
            ),
        ],
    )

    build_common_feature_daily_facts(
        storage=storage,  # type: ignore[arg-type]
        start=date(2025, 2, 20),
        end=date(2025, 2, 20),
        krx_trading_days=_krx_days,
    )

    assert len(storage.facts) == 1
    assert storage.facts[0].value_numeric == Decimal("0.03")
    assert storage.facts[0].source_observation_ids == [2, 1]


def test_build_daily_facts_yoy_is_null_when_prior_year_period_missing() -> None:
    storage = MockCommonFeatureBuildStorage(
        series=[_series("macro_cpi", source=Source.ECOS, max_stale_business_days=400)],
        catalog=[
            _feature(
                "macro_cpi_yoy",
                transform_code="yoy",
                series_id="macro_cpi",
            )
        ],
        observations=[
            # prior period is 2024-02, not 2024-01: no exact 12-month match for 2025-01.
            _obs(
                1,
                "macro_cpi",
                observation_date=date(2024, 2, 29),
                period_end_date=date(2024, 2, 29),
                available_from_date=date(2024, 3, 20),
                value="100",
                source=Source.ECOS,
            ),
            _obs(
                2,
                "macro_cpi",
                observation_date=date(2025, 1, 31),
                period_end_date=date(2025, 1, 31),
                available_from_date=date(2025, 2, 20),
                value="103",
                source=Source.ECOS,
            ),
        ],
    )

    build_common_feature_daily_facts(
        storage=storage,  # type: ignore[arg-type]
        start=date(2025, 2, 20),
        end=date(2025, 2, 20),
        krx_trading_days=_krx_days,
    )

    assert len(storage.facts) == 1
    assert storage.facts[0].value_numeric is None


def test_build_daily_facts_computes_mom_from_prior_month() -> None:
    storage = MockCommonFeatureBuildStorage(
        series=[_series("macro_cpi", source=Source.ECOS, max_stale_business_days=400)],
        catalog=[
            _feature(
                "macro_cpi_mom",
                transform_code="mom",
                series_id="macro_cpi",
            )
        ],
        observations=[
            _obs(
                1,
                "macro_cpi",
                observation_date=date(2024, 12, 31),
                period_end_date=date(2024, 12, 31),
                available_from_date=date(2025, 1, 20),
                value="100",
                source=Source.ECOS,
            ),
            _obs(
                2,
                "macro_cpi",
                observation_date=date(2025, 1, 31),
                period_end_date=date(2025, 1, 31),
                available_from_date=date(2025, 2, 20),
                value="102",
                source=Source.ECOS,
            ),
        ],
    )

    build_common_feature_daily_facts(
        storage=storage,  # type: ignore[arg-type]
        start=date(2025, 2, 20),
        end=date(2025, 2, 20),
        krx_trading_days=_krx_days,
    )

    assert len(storage.facts) == 1
    assert storage.facts[0].value_numeric == Decimal("0.02")
    assert storage.facts[0].source_observation_ids == [2, 1]
