"""Use-case: build KRX-date-aligned common feature daily facts."""

from __future__ import annotations

import logging
import re
from bisect import bisect_right
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from krx_collector.domain.enums import RunStatus, RunType
from krx_collector.domain.models import (
    CommonFeatureBuildResult,
    CommonFeatureCatalogEntry,
    CommonFeatureDailyFact,
    CommonFeatureObservation,
    CommonFeatureSeries,
    IngestionRun,
)
from krx_collector.infra.calendar.trading_days import get_trading_days
from krx_collector.ports.storage import Storage
from krx_collector.util.pipeline import build_run_counts, complete_run, fail_run
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)

KrxTradingDayProvider = Callable[[date, date], Sequence[date]]

# Transforms that take no window argument.
_SCALAR_TRANSFORMS = {"level", "yoy", "mom"}
# Transforms parameterized by a positive integer window, e.g. ret_5d, change_20d, vol_60d.
_WINDOWED_TRANSFORM = re.compile(r"^(ret|change|vol)_(\d+)d$")


@dataclass(frozen=True, slots=True)
class _Transform:
    kind: str  # level | ret | change | vol | yoy | mom
    window: int  # observation count; 0 for scalar transforms


def _parse_transform(transform_code: str) -> _Transform | None:
    if transform_code in _SCALAR_TRANSFORMS:
        return _Transform(kind=transform_code, window=0)
    match = _WINDOWED_TRANSFORM.match(transform_code)
    if match is None:
        return None
    window = int(match.group(2))
    if window <= 0:
        return None
    return _Transform(kind=match.group(1), window=window)


@dataclass(frozen=True, slots=True)
class _SeriesValue:
    period_date: date
    observation: CommonFeatureObservation


def build_common_feature_daily_facts(
    storage: Storage,
    start: date,
    end: date,
    feature_codes: list[str] | None = None,
    active_only: bool = True,
    krx_trading_days: KrxTradingDayProvider | None = None,
) -> CommonFeatureBuildResult:
    """Build point-in-time safe common feature facts for KRX trading days."""
    run = IngestionRun(
        run_type=RunType.COMMON_FEATURE_BUILD,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "start": start.isoformat(),
            "end": end.isoformat(),
            "feature_codes": feature_codes,
            "active_only": active_only,
        },
    )
    storage.record_run(run)

    result = CommonFeatureBuildResult()
    calendar = krx_trading_days or get_trading_days
    generated_at = now_kst()

    try:
        feature_dates = list(calendar(start, end))
        catalog_rows = storage.get_common_feature_catalog(
            feature_codes=feature_codes,
            active_only=active_only,
        )
        result.features_processed = len(catalog_rows)
        result.feature_dates_processed = len(feature_dates)

        input_series_ids = sorted(
            {
                series_id
                for feature in catalog_rows
                for series_id in feature.input_series_ids
            }
        )
        series_by_id = {
            series.series_id: series
            for series in storage.get_common_feature_series(
                series_ids=input_series_ids,
                active_only=active_only,
            )
        }
        observations = storage.get_common_feature_observations(
            series_ids=input_series_ids,
            end=end,
            available_from_end=end,
        )
        observations_by_series = _group_observations(observations)
        stale_calendar = _build_stale_calendar(
            observations=observations,
            feature_dates=feature_dates,
            krx_trading_days=calendar,
        )

        facts: list[CommonFeatureDailyFact] = []
        for feature in catalog_rows:
            feature_facts = _build_feature_facts(
                feature=feature,
                series_by_id=series_by_id,
                observations_by_series=observations_by_series,
                feature_dates=feature_dates,
                generated_at=generated_at,
                generation_run_id=run.run_id,
                stale_calendar=stale_calendar,
            )
            if isinstance(feature_facts, str):
                result.errors[feature.feature_code] = feature_facts
                continue
            facts.extend(feature_facts)

        if facts:
            upsert = storage.upsert_common_feature_daily_facts(facts)
            result.upsert.updated += upsert.updated
            result.upsert.errors += upsert.errors
            result.facts_upserted += upsert.updated
        result.facts_built = len(facts)
        result.null_facts = sum(1 for fact in facts if fact.value_numeric is None)

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                features_processed=result.features_processed,
                feature_dates_processed=result.feature_dates_processed,
                facts_built=result.facts_built,
                null_facts=result.null_facts,
                facts_upserted=result.facts_upserted,
            ),
            errors=result.errors,
            partial_subject="common feature daily fact features",
        )
        return result
    except Exception as exc:
        logger.exception("Common feature daily fact build failed")
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result


def _build_feature_facts(
    *,
    feature: CommonFeatureCatalogEntry,
    series_by_id: dict[str, CommonFeatureSeries],
    observations_by_series: dict[str, list[CommonFeatureObservation]],
    feature_dates: list[date],
    generated_at: datetime,
    generation_run_id: str,
    stale_calendar: list[date],
) -> list[CommonFeatureDailyFact] | str:
    if len(feature.input_series_ids) != 1:
        return "PR 3-A supports only single-input common feature transforms"

    series_id = feature.input_series_ids[0]
    series = series_by_id.get(series_id)
    if series is None:
        return f"Missing active common feature series: {series_id}"

    transform_code = feature.transform_code or series.default_transform or "level"
    transform = _parse_transform(transform_code)
    if transform is None:
        return f"Unsupported common feature transform: {transform_code}"

    observations = observations_by_series.get(series_id, [])
    facts: list[CommonFeatureDailyFact] = []
    for feature_date in feature_dates:
        history = _asof_history(observations, feature_date)
        current = history[-1] if history else None
        value_numeric: Decimal | None = None
        source_observation_ids: list[int] = []
        asof_available_date = feature_date
        selected_vintage = ""

        if current is not None:
            current_observation = current.observation
            asof_available_date = current_observation.available_from_date or feature_date
            selected_vintage = current_observation.vintage
            if current_observation.raw_id is not None:
                source_observation_ids.append(current_observation.raw_id)

            if not _is_stale(
                current_observation,
                feature_date,
                max_stale_business_days=series.max_stale_business_days,
                stale_calendar=stale_calendar,
            ):
                value_numeric = _transform_value(
                    transform=transform,
                    history=history,
                    current=current,
                    source_observation_ids=source_observation_ids,
                )

        facts.append(
            CommonFeatureDailyFact(
                feature_date=feature_date,
                feature_code=feature.feature_code,
                value_numeric=value_numeric,
                value_text="",
                unit=feature.unit,
                source_series_ids=[series_id],
                source_observation_ids=source_observation_ids,
                asof_available_date=asof_available_date,
                selected_vintage=selected_vintage,
                generated_at=generated_at,
                generation_run_id=generation_run_id,
            )
        )
    return facts


def _transform_value(
    *,
    transform: _Transform,
    history: list[_SeriesValue],
    current: _SeriesValue,
    source_observation_ids: list[int],
) -> Decimal | None:
    current_value = current.observation.value_numeric
    if current_value is None:
        return None
    if transform.kind == "level":
        return current_value

    current_index = history.index(current)

    if transform.kind in {"ret", "change"}:
        base = _value_at_lag(history, current_index, transform.window)
        if base is None:
            return None
        base_observation, base_value = base
        if transform.kind == "ret" and base_value == 0:
            return None
        _trace(source_observation_ids, base_observation)
        if transform.kind == "change":
            return current_value - base_value
        return (current_value / base_value) - Decimal("1")

    if transform.kind == "vol":
        return _rolling_return_volatility(
            history=history,
            current_index=current_index,
            window=transform.window,
            source_observation_ids=source_observation_ids,
        )

    if transform.kind in {"yoy", "mom"}:
        months = 12 if transform.kind == "yoy" else 1
        base = _value_at_calendar_offset(history, current, months_back=months)
        if base is None:
            return None
        base_observation, base_value = base
        if base_value == 0:
            return None
        _trace(source_observation_ids, base_observation)
        return (current_value / base_value) - Decimal("1")

    return None


def _value_at_lag(
    history: list[_SeriesValue],
    current_index: int,
    lag: int,
) -> tuple[CommonFeatureObservation, Decimal] | None:
    base_index = current_index - lag
    if base_index < 0:
        return None
    base_observation = history[base_index].observation
    base_value = base_observation.value_numeric
    if base_value is None:
        return None
    return base_observation, base_value


def _value_at_calendar_offset(
    history: list[_SeriesValue],
    current: _SeriesValue,
    *,
    months_back: int,
) -> tuple[CommonFeatureObservation, Decimal] | None:
    """Match the same period exactly N months before current.period_date.

    Used for yoy/mom on monthly/quarterly series, where positional lags are
    unreliable if any period is missing. Returns None when the prior period
    is absent so gaps surface as NULL rather than a wrong comparison.
    """
    target_year, target_month = _shift_months(
        current.period_date.year, current.period_date.month, months_back
    )
    for candidate in history:
        if (
            candidate.period_date.year == target_year
            and candidate.period_date.month == target_month
        ):
            base_value = candidate.observation.value_numeric
            if base_value is None:
                return None
            return candidate.observation, base_value
    return None


def _shift_months(year: int, month: int, months_back: int) -> tuple[int, int]:
    index = (year * 12 + (month - 1)) - months_back
    return index // 12, (index % 12) + 1


def _rolling_return_volatility(
    *,
    history: list[_SeriesValue],
    current_index: int,
    window: int,
    source_observation_ids: list[int],
) -> Decimal | None:
    """Sample standard deviation (ddof=1) of the last `window` 1-step returns.

    Needs window+1 consecutive observations ending at current_index. Returns
    None if history is too short or any base value is zero.
    """
    if window < 2:
        return None
    first_index = current_index - window
    if first_index < 0:
        return None

    returns: list[Decimal] = []
    used: list[CommonFeatureObservation] = []
    for index in range(first_index + 1, current_index + 1):
        prev_observation = history[index - 1].observation
        curr_observation = history[index].observation
        prev_value = prev_observation.value_numeric
        curr_value = curr_observation.value_numeric
        if prev_value is None or curr_value is None or prev_value == 0:
            return None
        returns.append((curr_value / prev_value) - Decimal("1"))
        used.append(prev_observation)

    count = len(returns)
    mean = sum(returns, Decimal("0")) / count
    variance = sum(((value - mean) ** 2 for value in returns), Decimal("0")) / (count - 1)
    for observation in used:
        _trace(source_observation_ids, observation)
    return variance.sqrt()


def _trace(source_observation_ids: list[int], observation: CommonFeatureObservation) -> None:
    if observation.raw_id is not None and observation.raw_id not in source_observation_ids:
        source_observation_ids.append(observation.raw_id)


def _asof_history(
    observations: list[CommonFeatureObservation],
    feature_date: date,
) -> list[_SeriesValue]:
    latest_by_period: dict[date, CommonFeatureObservation] = {}
    for observation in observations:
        if observation.available_from_date is None:
            continue
        if observation.available_from_date > feature_date:
            continue
        if observation.value_numeric is None:
            continue

        period_date = observation.period_end_date or observation.observation_date
        existing = latest_by_period.get(period_date)
        if existing is None or _observation_sort_key(observation) > _observation_sort_key(
            existing
        ):
            latest_by_period[period_date] = observation

    return [
        _SeriesValue(period_date=period_date, observation=observation)
        for period_date, observation in sorted(latest_by_period.items())
    ]


def _observation_sort_key(
    observation: CommonFeatureObservation,
) -> tuple[date, date, str, str, int]:
    available_from = observation.available_from_date or date.min
    release_date = observation.release_date or available_from
    raw_id = observation.raw_id or -1
    return (
        release_date,
        available_from,
        observation.fetched_at.isoformat(),
        observation.vintage,
        raw_id,
    )


def _is_stale(
    observation: CommonFeatureObservation,
    feature_date: date,
    *,
    max_stale_business_days: int,
    stale_calendar: list[date],
) -> bool:
    if observation.available_from_date is None:
        return True
    available_index = bisect_right(stale_calendar, observation.available_from_date)
    feature_index = bisect_right(stale_calendar, feature_date)
    age = max(0, feature_index - available_index)
    return age > max_stale_business_days


def _build_stale_calendar(
    *,
    observations: list[CommonFeatureObservation],
    feature_dates: list[date],
    krx_trading_days: KrxTradingDayProvider,
) -> list[date]:
    if not feature_dates:
        return []
    first_feature_date = feature_dates[0]
    first_available_date = min(
        (
            observation.available_from_date
            for observation in observations
            if observation.available_from_date is not None
        ),
        default=first_feature_date,
    )
    if first_available_date >= first_feature_date:
        return feature_dates
    return list(krx_trading_days(min(first_feature_date, first_available_date), feature_dates[-1]))


def _group_observations(
    observations: list[CommonFeatureObservation],
) -> dict[str, list[CommonFeatureObservation]]:
    grouped: dict[str, list[CommonFeatureObservation]] = {}
    for observation in observations:
        grouped.setdefault(observation.series_id, []).append(observation)
    for rows in grouped.values():
        rows.sort(key=lambda row: (row.period_end_date or row.observation_date, row.raw_id or -1))
    return grouped
