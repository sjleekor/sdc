from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from krx_collector.domain.models import (
    CommonFeatureCatalogEntry,
    CommonFeatureDailyFact,
)
from krx_collector.service.report_common_feature_readiness import (
    build_common_feature_readiness_report,
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
        generated_at=datetime(2026, 6, 10, 9, 0, tzinfo=UTC),
    )


class MockCommonFeatureReadinessStorage:
    def __init__(
        self,
        *,
        catalog: list[CommonFeatureCatalogEntry],
        facts: list[CommonFeatureDailyFact],
    ) -> None:
        self.catalog = catalog
        self.facts = facts
        self.catalog_query: tuple[list[str] | None, bool] | None = None

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
        return [
            fact
            for fact in self.facts
            if start <= fact.feature_date <= end
            and (not feature_codes or fact.feature_code in feature_codes)
        ]


def test_common_feature_readiness_report_marks_ready_and_blocked_features() -> None:
    storage = MockCommonFeatureReadinessStorage(
        catalog=[
            _feature("rate_kr_gov3y_level"),
            _feature("global_sp500_ret_1d"),
        ],
        facts=[
            _fact("rate_kr_gov3y_level", date(2024, 1, 2), "3.25"),
            _fact("rate_kr_gov3y_level", date(2024, 1, 3), "3.24"),
            _fact("rate_kr_gov3y_level", date(2024, 1, 4), "3.27"),
            _fact("global_sp500_ret_1d", date(2024, 1, 2), None),
            _fact(
                "global_sp500_ret_1d",
                date(2024, 1, 4),
                "0.012",
                asof_available_date=date(2024, 1, 5),
            ),
        ],
    )

    report = build_common_feature_readiness_report(
        storage=storage,  # type: ignore[arg-type]
        start=date(2024, 1, 2),
        end=date(2024, 1, 4),
        feature_codes=["rate_kr_gov3y_level", "global_sp500_ret_1d"],
        required_coverage_ratio=Decimal("1.0000"),
        krx_trading_days=_krx_days,
    )

    rows = {row.feature_code: row for row in report.rows}

    assert rows["rate_kr_gov3y_level"].ready is True
    assert rows["rate_kr_gov3y_level"].blockers == ()
    assert rows["rate_kr_gov3y_level"].coverage_ratio == Decimal("1.0000")

    assert rows["global_sp500_ret_1d"].ready is False
    assert rows["global_sp500_ret_1d"].coverage_ratio == Decimal("0.3333")
    assert rows["global_sp500_ret_1d"].blockers == (
        "coverage_ratio=0.3333 < required=1.0000",
        "null_count=1",
        "missing_count=1",
        "pit_violation_count=1",
    )


def test_common_feature_readiness_report_can_include_inactive_explicit_features() -> None:
    storage = MockCommonFeatureReadinessStorage(
        catalog=[_feature("macro_cpi_level", active=False)],
        facts=[_fact("macro_cpi_level", date(2024, 2, 20), "113.17")],
    )

    report = build_common_feature_readiness_report(
        storage=storage,  # type: ignore[arg-type]
        start=date(2024, 2, 20),
        end=date(2024, 2, 20),
        feature_codes=["macro_cpi_level"],
        active_only=False,
        krx_trading_days=_krx_days,
    )

    assert storage.catalog_query == (["macro_cpi_level"], False)
    assert len(report.rows) == 1
    assert report.rows[0].ready is True
    assert report.rows[0].coverage_ratio == Decimal("1.0000")
