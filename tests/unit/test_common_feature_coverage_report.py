from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from krx_collector.domain.models import (
    CommonFeatureCatalogEntry,
    CommonFeatureDailyFact,
)
from krx_collector.service.report_common_feature_coverage import (
    build_common_feature_coverage_report,
)


def _krx_days(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _feature(feature_code: str, *, active: bool = True) -> CommonFeatureCatalogEntry:
    return CommonFeatureCatalogEntry(
        feature_code=feature_code,
        feature_name_kr=feature_code,
        category="market",
        input_series_ids=("series",),
        active=active,
    )


def _fact(
    feature_code: str,
    feature_date: date,
    value: str | None,
    *,
    asof_available_date: date | None = None,
) -> CommonFeatureDailyFact:
    return CommonFeatureDailyFact(
        feature_date=feature_date,
        feature_code=feature_code,
        value_numeric=Decimal(value) if value is not None else None,
        asof_available_date=asof_available_date or feature_date,
        generated_at=datetime(2026, 6, 9, 8, 0, tzinfo=UTC),
    )


class MockCommonFeatureCoverageStorage:
    def __init__(
        self,
        *,
        catalog: list[CommonFeatureCatalogEntry],
        facts: list[CommonFeatureDailyFact],
    ) -> None:
        self.catalog = catalog
        self.facts = facts
        self.catalog_query: tuple[list[str] | None, bool] | None = None
        self.fact_query: tuple[date, date, list[str] | None] | None = None

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

    def get_common_feature_daily_facts(
        self,
        start: date,
        end: date,
        feature_codes: list[str] | None = None,
    ) -> list[CommonFeatureDailyFact]:
        self.fact_query = (start, end, feature_codes)
        return [
            fact
            for fact in self.facts
            if start <= fact.feature_date <= end
            and (not feature_codes or fact.feature_code in feature_codes)
        ]


def test_common_feature_coverage_report_counts_fact_null_missing_and_pit() -> None:
    storage = MockCommonFeatureCoverageStorage(
        catalog=[
            _feature("global_vix_level"),
            _feature("global_sp500_ret_1d"),
            _feature("fx_usdkrw_level"),
        ],
        facts=[
            _fact("global_vix_level", date(2024, 1, 2), None),
            _fact("global_vix_level", date(2024, 1, 3), "13.2"),
            _fact(
                "global_vix_level",
                date(2024, 1, 4),
                "13.3",
                asof_available_date=date(2024, 1, 5),
            ),
            _fact("global_sp500_ret_1d", date(2024, 1, 2), None),
            _fact("global_sp500_ret_1d", date(2024, 1, 3), None),
            _fact("fx_usdkrw_level", date(2024, 1, 2), "1293.5"),
            _fact("fx_usdkrw_level", date(2024, 1, 3), "1293.6"),
            _fact("fx_usdkrw_level", date(2024, 1, 4), "1293.6"),
        ],
    )

    report = build_common_feature_coverage_report(
        storage=storage,  # type: ignore[arg-type]
        start=date(2024, 1, 2),
        end=date(2024, 1, 5),
        feature_codes=["global_vix_level", "global_sp500_ret_1d", "fx_usdkrw_level"],
        krx_trading_days=_krx_days,
    )

    assert report.target_count == 4
    rows = {row.feature_code: row for row in report.rows}

    assert rows["fx_usdkrw_level"].fact_count == 3
    assert rows["fx_usdkrw_level"].non_null_count == 3
    assert rows["fx_usdkrw_level"].null_count == 0
    assert rows["fx_usdkrw_level"].missing_count == 1
    assert rows["fx_usdkrw_level"].coverage_ratio == Decimal("0.7500")
    assert rows["fx_usdkrw_level"].pit_violation_count == 0

    assert rows["global_vix_level"].fact_count == 3
    assert rows["global_vix_level"].non_null_count == 2
    assert rows["global_vix_level"].null_count == 1
    assert rows["global_vix_level"].missing_count == 1
    assert rows["global_vix_level"].coverage_ratio == Decimal("0.5000")
    assert rows["global_vix_level"].pit_violation_count == 1

    assert rows["global_sp500_ret_1d"].fact_count == 2
    assert rows["global_sp500_ret_1d"].non_null_count == 0
    assert rows["global_sp500_ret_1d"].null_count == 2
    assert rows["global_sp500_ret_1d"].missing_count == 2
    assert rows["global_sp500_ret_1d"].coverage_ratio == Decimal("0.0000")
    assert rows["global_sp500_ret_1d"].pit_violation_count == 0

    assert storage.catalog_query == (
        ["global_vix_level", "global_sp500_ret_1d", "fx_usdkrw_level"],
        True,
    )
    assert storage.fact_query == (
        date(2024, 1, 2),
        date(2024, 1, 5),
        ["global_vix_level", "global_sp500_ret_1d", "fx_usdkrw_level"],
    )


def test_common_feature_coverage_report_handles_empty_trading_day_range() -> None:
    storage = MockCommonFeatureCoverageStorage(
        catalog=[_feature("global_vix_level")],
        facts=[],
    )

    report = build_common_feature_coverage_report(
        storage=storage,  # type: ignore[arg-type]
        start=date(2024, 1, 6),
        end=date(2024, 1, 7),
        krx_trading_days=_krx_days,
    )

    assert report.target_count == 0
    assert len(report.rows) == 1
    assert report.rows[0].coverage_ratio == Decimal("0")
    assert report.rows[0].missing_count == 0


def test_common_feature_coverage_report_can_include_inactive_explicit_features() -> None:
    storage = MockCommonFeatureCoverageStorage(
        catalog=[_feature("rate_kr_gov3y_level", active=False)],
        facts=[
            _fact("rate_kr_gov3y_level", date(2024, 1, 3), "3.24"),
        ],
    )

    report = build_common_feature_coverage_report(
        storage=storage,  # type: ignore[arg-type]
        start=date(2024, 1, 3),
        end=date(2024, 1, 3),
        feature_codes=["rate_kr_gov3y_level"],
        active_only=False,
        krx_trading_days=_krx_days,
    )

    assert storage.catalog_query == (["rate_kr_gov3y_level"], False)
    assert len(report.rows) == 1
    assert report.rows[0].feature_code == "rate_kr_gov3y_level"
    assert report.rows[0].coverage_ratio == Decimal("1.0000")
