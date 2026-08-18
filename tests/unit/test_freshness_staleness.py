"""The staleness gate over the raw freshness report.

``ops freshness-report`` prints and exits 0 no matter what it finds, so putting
it on a schedule produces a green job every day whether or not anything was
collected. That is exactly what happened on 2026-08-14: sj2-server was down
across the 18:30 KRX chain and the 20:30 common sync, every raw table stayed a
day behind, no run failed because no run started, and nothing surfaced it.

A missed run leaves no trace in ``ingestion_runs``. The only evidence is the data
itself, so the gate asks whether the newest row is as new as the calendar says it
should be.
"""

from __future__ import annotations

from datetime import date

import pytest

from krx_collector.domain.enums import Source
from krx_collector.service.freshness import (
    FLOW_SOURCES,
    TRADING_DAY_SOURCES,
    CommonSeriesFreshness,
    FreshnessReport,
    build_freshness_report,
    evaluate_staleness,
)

# 2026-08-15 is a Saturday; the sessions before it are Fri 08-14 and Thu 08-13.
SATURDAY = date(2026, 8, 15)
FRIDAY = date(2026, 8, 14)
THURSDAY = date(2026, 8, 13)

NO_HOLIDAYS: set[date] = set()


def _report(
    *,
    price: date | None = FRIDAY,
    market_cap: date | None = FRIDAY,
    flows: dict[str, date | None] | None = None,
    common: list[CommonSeriesFreshness] | None = None,
) -> FreshnessReport:
    return FreshnessReport(
        price_latest_date=price,
        market_cap_latest_date=market_cap,
        flow_group_latest_dates={"investor_net_buy": FRIDAY} if flows is None else flows,
        common_series=common or [],
    )


def _evaluate(report: FreshnessReport, **kwargs):
    kwargs.setdefault("as_of", SATURDAY)
    kwargs.setdefault("holidays", NO_HOLIDAYS)
    return evaluate_staleness(report, **kwargs)


def test_current_data_produces_no_findings() -> None:
    assert _evaluate(_report()) == []


def test_reproduces_the_2026_08_14_outage() -> None:
    # Everything one session behind, which is what the host being down over the
    # evening window looked like in the database.
    report = _report(
        price=THURSDAY,
        market_cap=THURSDAY,
        flows={"investor_net_buy": THURSDAY, "short_selling": THURSDAY},
        common=[
            CommonSeriesFreshness("market_kospi_close_krx", Source.KRX, THURSDAY),
            CommonSeriesFreshness("fx_usdkrw_fdr", Source.FDR, THURSDAY),
        ],
    )

    findings = _evaluate(report)

    assert {finding.domain for finding in findings} == {
        "daily_ohlcv",
        "daily_market_cap",
        "krx_security_flow_raw:investor_net_buy",
        "krx_security_flow_raw:short_selling",
        "common:market_kospi_close_krx",
        "common:fx_usdkrw_fdr",
    }
    assert all(finding.required_at_or_after == FRIDAY for finding in findings)
    assert all(finding.cadence == "trading" for finding in findings)


def test_weekend_does_not_move_the_threshold_past_the_last_session() -> None:
    # Run the same check on Sunday: Friday is still the newest session, so data
    # that was current on Saturday must not turn stale overnight.
    assert _evaluate(_report(), as_of=date(2026, 8, 16)) == []


def test_a_holiday_run_does_not_demand_data_from_a_closed_market() -> None:
    # 08-17 is the substitute holiday for Liberation Day. The newest session is
    # still Friday, and asking for Monday's data would be asking for data that
    # cannot exist.
    monday = date(2026, 8, 17)

    assert _evaluate(_report(), as_of=monday, holidays={monday}) == []


def test_an_empty_table_is_stale_rather_than_skipped() -> None:
    findings = _evaluate(_report(price=None))

    assert [finding.domain for finding in findings] == ["daily_ohlcv"]
    assert findings[0].latest is None


def test_market_cap_is_gated_alongside_prices() -> None:
    # daily_market_cap becomes a daily job, so leaving it out of the gate would
    # recreate the exact hole the gate exists to close. It is empty until the
    # N1 backfill lands, and empty is stale -- so schedule the gate after the
    # backfill, do not carve out an exemption for it.
    findings = _evaluate(_report(market_cap=None))

    assert [finding.domain for finding in findings] == ["daily_market_cap"]


def test_release_lagged_sources_get_a_calendar_budget() -> None:
    # FRED and ECOS publish on their own schedule. Holding them to the trading
    # calendar would flag them every day, and a gate that always fires is a gate
    # nobody reads.
    common = [
        CommonSeriesFreshness("us_cpi_fred", Source.FRED, date(2026, 8, 5)),
        CommonSeriesFreshness("kr_base_rate_ecos", Source.ECOS, date(2026, 8, 5)),
    ]

    assert _evaluate(_report(common=common)) == []

    stale = [
        CommonSeriesFreshness("us_cpi_fred", Source.FRED, date(2026, 7, 1)),
    ]
    findings = _evaluate(_report(common=stale))
    assert [finding.domain for finding in findings] == ["common:us_cpi_fred"]
    assert findings[0].cadence == "calendar"


def test_a_wider_trading_budget_tolerates_one_missed_session() -> None:
    report = _report(price=THURSDAY, flows={"investor_net_buy": THURSDAY})

    assert _evaluate(report, max_lag_trading_days=2) == []


@pytest.mark.parametrize("lag", [0, -1])
def test_a_nonsensical_budget_falls_back_to_the_latest_session(lag: int) -> None:
    # 0 would otherwise index sessions[-0] == sessions[0], i.e. 60 days back,
    # silently turning the gate off.
    report = _report(price=THURSDAY, flows={"investor_net_buy": THURSDAY})

    findings = _evaluate(report, max_lag_trading_days=lag)

    assert {finding.required_at_or_after for finding in findings} == {FRIDAY}


def test_describe_names_the_domain_and_both_dates() -> None:
    findings = _evaluate(_report(price=THURSDAY, flows={}))

    message = findings[0].describe()
    assert "daily_ohlcv" in message
    assert "2026-08-13" in message
    assert "2026-08-14" in message


def test_the_flow_cursor_asks_about_both_krx_and_kis() -> None:
    # `flows` moved from KRX scraping to KIS on 2026-08. A KRX-scoped cursor
    # would report the flow domain as frozen on changeover day and fire this
    # gate every morning on data that is in fact current.
    class RecordingStorage:
        def __init__(self) -> None:
            self.flow_sources: tuple[Source, ...] = ()

        def get_krx_security_flow_metric_max_dates(self, metric_codes, sources):
            self.flow_sources = tuple(sources)
            return {metric: FRIDAY for metric in metric_codes}

        def get_common_feature_series(self, active_only=True):
            return []

        def get_common_feature_observation_max_dates(self, series_ids):
            return {}

        def get_table_bsns_year_range(self, table_name):
            return None

        def get_latest_daily_price_date(self):
            return FRIDAY

        def get_latest_market_cap_date(self):
            return FRIDAY

        def get_running_ingestion_runs(self, limit=20):
            return []

    storage = RecordingStorage()
    report = build_freshness_report(storage)

    assert storage.flow_sources == FLOW_SOURCES
    assert Source.KRX in storage.flow_sources
    assert Source.KIS in storage.flow_sources
    assert report.flow_group_latest_dates["investor"] == FRIDAY


def test_kis_is_treated_as_a_trading_day_source() -> None:
    # KIS publishes on KRX sessions, so it gets the trading-day budget rather
    # than the 14-day calendar budget that ECOS/FRED need.
    assert Source.KIS in TRADING_DAY_SOURCES

    stale = [
        CommonSeriesFreshness(
            series_id="kospi_flow", source=Source.KIS, latest_observation_date=THURSDAY
        )
    ]
    findings = _evaluate(_report(common=stale))
    assert [finding.cadence for finding in findings] == ["trading"]
