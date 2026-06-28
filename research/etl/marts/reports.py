"""Coverage / readiness / freshness checks on the lake (refactor §4).

These move the Postgres operational gates onto the parquet/DuckDB compute node:

- :func:`coverage_report` / :func:`readiness_report` — ports of
  ``service/report_common_feature_coverage.py`` and ``report_common_feature_readiness.py``,
  computed over the ``common_feature_daily_fact`` mart and the KRX feature-date
  calendar instead of the dropped canonical table.
- :func:`freshness_violations` — port of ``service/freshness.assert_common_freshness``
  raw-input gate, run on the lake (``common_feature_observation_raw`` +
  ``common_feature_series`` + ``ingestion_runs``) so the compute pipeline can refuse
  to run on stale raw (decision 6). Catches collector failures at compute time
  (plan §8 Q4) since ``ingestion_runs`` rides the raw mirror.

All functions take a DuckDB connection with the needed views already registered
and return plain dataclasses the orchestrator turns into a non-zero exit + stderr
summary (no Postgres writes).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date

import duckdb


@dataclass(frozen=True, slots=True)
class CoverageRow:
    feature_code: str
    target_count: int
    fact_count: int
    non_null_count: int
    null_count: int
    missing_count: int
    coverage_ratio: float
    pit_violation_count: int


@dataclass(frozen=True, slots=True)
class ReadinessRow:
    feature_code: str
    coverage_ratio: float
    ready: bool
    blockers: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FreshnessViolation:
    series_id: str | None
    check: str
    message: str


@dataclass(frozen=True, slots=True)
class FreshnessResult:
    end: date
    checked_series: int
    violations: list[FreshnessViolation] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations


def coverage_report(
    con: duckdb.DuckDBPyConnection,
    *,
    feature_dates: Sequence[date],
    cfdf_view: str = "common_feature_daily_fact",
) -> list[CoverageRow]:
    """Per-feature coverage over the KRX feature-date calendar.

    ``target_count`` = number of feature dates; ``coverage_ratio`` =
    non_null / target. Mirrors the Postgres coverage report 1:1.
    """
    target_count = len(feature_dates)
    rows = con.execute(f"""
        SELECT
            feature_code,
            count(*) AS fact_count,
            count(*) FILTER (WHERE value_numeric IS NOT NULL) AS non_null_count,
            count(*) FILTER (WHERE value_numeric IS NULL) AS null_count,
            count(*) FILTER (WHERE asof_available_date > feature_date) AS pit_violation_count
        FROM {cfdf_view}
        GROUP BY feature_code
        """).fetchall()
    out: list[CoverageRow] = []
    for feature_code, fact_count, non_null, null_count, pit_violations in rows:
        missing = max(target_count - fact_count, 0)
        ratio = round(non_null / target_count, 4) if target_count > 0 else 0.0
        out.append(
            CoverageRow(
                feature_code=feature_code,
                target_count=target_count,
                fact_count=fact_count,
                non_null_count=non_null,
                null_count=null_count,
                missing_count=missing,
                coverage_ratio=ratio,
                pit_violation_count=pit_violations,
            )
        )
    out.sort(key=lambda r: (r.coverage_ratio, r.feature_code), reverse=True)
    return out


def readiness_report(
    con: duckdb.DuckDBPyConnection,
    *,
    feature_dates: Sequence[date],
    required_coverage_ratio: float = 1.0,
    cfdf_view: str = "common_feature_daily_fact",
) -> list[ReadinessRow]:
    """Strict readiness: a feature is ready only with full coverage, no nulls,
    no missing dates, and no PIT violations. Mirrors the Postgres readiness report."""
    out: list[ReadinessRow] = []
    for row in coverage_report(con, feature_dates=feature_dates, cfdf_view=cfdf_view):
        blockers: list[str] = []
        if row.target_count == 0:
            blockers.append("target_count=0")
        if row.coverage_ratio < required_coverage_ratio:
            blockers.append(
                f"coverage_ratio={row.coverage_ratio} < required={required_coverage_ratio}"
            )
        if row.null_count > 0:
            blockers.append(f"null_count={row.null_count}")
        if row.missing_count > 0:
            blockers.append(f"missing_count={row.missing_count}")
        if row.pit_violation_count > 0:
            blockers.append(f"pit_violation_count={row.pit_violation_count}")
        out.append(
            ReadinessRow(
                feature_code=row.feature_code,
                coverage_ratio=row.coverage_ratio,
                ready=not blockers,
                blockers=tuple(blockers),
            )
        )
    out.sort(key=lambda r: (not r.ready, r.feature_code))
    return out


def freshness_violations(
    con: duckdb.DuckDBPyConnection,
    *,
    end: date,
    daily_max_lag_days: int = 2,
    macro_max_lag_days: int = 45,
    obs_view: str = "common_feature_observation_raw",
    series_view: str = "common_feature_series",
) -> FreshnessResult:
    """Raw-input freshness gate (port of ``assert_common_freshness``).

    For each active series, the latest observation must be within the per-series
    allowed lag (max of the frequency default, manual_lag_days, and
    max_stale_business_days). Series with no observations violate. Run BEFORE the
    marts so compute refuses stale raw (decision 6).
    """
    rows = con.execute(f"""
        WITH latest AS (
            SELECT series_id, max(observation_date) AS latest_obs
            FROM {obs_view}
            GROUP BY series_id
        )
        SELECT
            s.series_id,
            s.frequency,
            s.manual_lag_days,
            s.max_stale_business_days,
            l.latest_obs
        FROM {series_view} s
        LEFT JOIN latest l USING (series_id)
        WHERE s.active = TRUE
        """).fetchall()

    violations: list[FreshnessViolation] = []
    for series_id, frequency, manual_lag, max_stale, latest_obs in rows:
        base = daily_max_lag_days if (frequency or "").upper() == "D" else macro_max_lag_days
        max_lag = max(base, int(manual_lag or 0), int(max_stale or 0))
        if latest_obs is None:
            violations.append(
                FreshnessViolation(
                    series_id=series_id,
                    check="latest_observation",
                    message="no raw observations found for active common source series",
                )
            )
            continue
        lag_days = (end - latest_obs).days
        if lag_days > max_lag:
            violations.append(
                FreshnessViolation(
                    series_id=series_id,
                    check="latest_observation",
                    message=(
                        f"latest observation {latest_obs.isoformat()} is {lag_days} days "
                        f"behind {end.isoformat()} (allowed {max_lag})"
                    ),
                )
            )

    return FreshnessResult(end=end, checked_series=len(rows), violations=violations)
