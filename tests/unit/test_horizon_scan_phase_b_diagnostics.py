"""Tests for ``research/analysis/horizon_scan_phase_b_diagnostics.py`` — the
rank-correlation diagnostic (§5.5) and the SUE event-formation-ordinal
non-overlap diagnostic (§6 B-8 SUE point 5). Both reuse already-tested
primitives (``per_date_market_rank_ic``/``daily_market_weighted_ic``,
``_pool_cohort_ranks``/``_aggregate_cohort_rows``,
``compute_nonoverlap_robustness_pass``) so these tests focus on the new
wiring — SQL eligibility filtering, ordinal-stride subsampling — not on
re-verifying that underlying math.
"""

from __future__ import annotations

from datetime import date, timedelta

import duckdb
from research.analysis.horizon_scan_phase_b_diagnostics import (
    compute_phase_b_rank_correlation,
    run_sue_event_ordinal_nonoverlap,
)

# --- rank correlation ---


def _seed_panel(con: duckdb.DuckDBPyConnection, rows: list[tuple]) -> None:
    con.execute("""
        CREATE TABLE analysis_panel_phase_b (
            trade_date DATE, ticker VARCHAR, market VARCHAR,
            in_broad BOOLEAN, ca_mask BOOLEAN,
            common_formation_120d BOOLEAN, common_survivor_120d BOOLEAN,
            feat_a DOUBLE, feat_b DOUBLE
        )
    """)
    if rows:
        con.executemany("INSERT INTO analysis_panel_phase_b VALUES (?,?,?,?,?,?,?,?,?)", rows)


def _weekdays(start: date, count: int) -> list[date]:
    days: list[date] = []
    current = start
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def test_compute_phase_b_rank_correlation_perfectly_aligned_features() -> None:
    con = duckdb.connect()
    days = _weekdays(date(2024, 1, 1), 5)
    rows = []
    for d in days:
        for i in range(6):
            rows.append((d, f"T{i}", "KOSPI", True, False, True, True, float(i), float(i)))
    _seed_panel(con, rows)

    out = compute_phase_b_rank_correlation(
        con,
        panel_view="analysis_panel_phase_b",
        feature_pairs=[("fam_a", "feat_a", "fam_b", "feat_b")],
        sample_start="2020-01-01",
        min_names=2,
    )

    assert len(out) == 1
    row = out[0]
    assert row["family_a"] == "fam_a" and row["feature_b"] == "feat_b"
    assert row["n_dates"] == 5
    assert abs(row["mean_rank_corr"] - 1.0) < 1e-9
    assert abs(row["min_rank_corr"] - 1.0) < 1e-9


def test_compute_phase_b_rank_correlation_perfectly_inverted_features() -> None:
    con = duckdb.connect()
    days = _weekdays(date(2024, 2, 1), 5)
    rows = []
    for d in days:
        for i in range(6):
            rows.append((d, f"T{i}", "KOSPI", True, False, True, True, float(i), float(-i)))
    _seed_panel(con, rows)

    out = compute_phase_b_rank_correlation(
        con,
        panel_view="analysis_panel_phase_b",
        feature_pairs=[("fam_a", "feat_a", "fam_b", "feat_b")],
        sample_start="2020-01-01",
        min_names=2,
    )

    assert abs(out[0]["mean_rank_corr"] - (-1.0)) < 1e-9


def test_compute_phase_b_rank_correlation_excludes_ineligible_rows() -> None:
    """``in_broad=False``, ``ca_mask=True``, and non-common-survivor rows must
    never enter the correlation — the diagnostic shares the same discovery
    coordinate as every other Phase B primary statistic, not a looser one."""
    con = duckdb.connect()
    days = _weekdays(date(2024, 3, 1), 3)
    rows = []
    for d in days:
        for i in range(6):
            rows.append((d, f"T{i}", "KOSPI", True, False, True, True, float(i), float(i)))
        # ineligible rows with an anti-correlated pattern that would flip the
        # sign if wrongly included
        rows.append((d, "X1", "KOSPI", False, False, True, True, 1.0, -1.0))
        rows.append((d, "X2", "KOSPI", True, True, True, True, 2.0, -2.0))
        rows.append((d, "X3", "KOSPI", True, False, False, True, 3.0, -3.0))
    _seed_panel(con, rows)

    out = compute_phase_b_rank_correlation(
        con,
        panel_view="analysis_panel_phase_b",
        feature_pairs=[("fam_a", "feat_a", "fam_b", "feat_b")],
        sample_start="2020-01-01",
        min_names=2,
    )

    assert abs(out[0]["mean_rank_corr"] - 1.0) < 1e-9


def test_compute_phase_b_rank_correlation_empty_result_when_no_valid_dates() -> None:
    con = duckdb.connect()
    _seed_panel(con, [])

    out = compute_phase_b_rank_correlation(
        con,
        panel_view="analysis_panel_phase_b",
        feature_pairs=[("fam_a", "feat_a", "fam_b", "feat_b")],
        sample_start="2020-01-01",
        min_names=2,
    )

    assert out == [
        {
            "family_a": "fam_a",
            "feature_a": "feat_a",
            "family_b": "fam_b",
            "feature_b": "feat_b",
            "n_dates": 0,
            "mean_rank_corr": None,
            "std_rank_corr": None,
            "min_rank_corr": None,
            "max_rank_corr": None,
        }
    ]


# --- SUE event-formation-ordinal non-overlap ---


def _seed_calendar(con: duckdb.DuckDBPyConnection, days: list[date]) -> None:
    con.execute("CREATE TABLE daily_ohlcv (trade_date DATE)")
    con.executemany("INSERT INTO daily_ohlcv VALUES (?)", [(d,) for d in days])


def _seed_event_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE fin_sue_event (
            ticker VARCHAR, market VARCHAR, event_formation_date DATE,
            bsns_year INTEGER, reprt_code VARCHAR,
            fin_sue DOUBLE, bucket_0_3_excess DOUBLE,
            is_primary_constant_sample BOOLEAN
        )
    """)


def _cohort_events(
    formation_date: date, market: str, n: int, *, reversed_rank: bool
) -> list[tuple]:
    out = []
    for i in range(n):
        sue = float(i)
        excess = float(n - 1 - i) if reversed_rank else float(i)
        ticker = f"{market[:1]}{i}"
        out.append((ticker, market, formation_date, 2020, "11013", sue, excess, True))
    return out


_ORDINAL_KWARGS = dict(
    sample_start="2020-01-01",
    min_events_per_market_contribution=10,
    min_events_per_cohort_total=16,
    min_event_cohorts=8,
    ordinal_stride=2,
)


def _ready_sue_cell(expected_sign: str = "+") -> dict:
    return {
        "hypothesis_id": "fin_sue|fin_sue|event|0|3",
        "feature": "fin_sue",
        "h_start": 0,
        "h_end": 3,
        "expected_sign": expected_sign,
    }


def test_run_sue_event_ordinal_nonoverlap_all_subsamples_pass() -> None:
    con = duckdb.connect()
    days = _weekdays(date(2020, 1, 6), 20)  # 20 dates / stride 2 -> 10 each
    _seed_calendar(con, days)
    _seed_event_table(con)
    events: list[tuple] = []
    for d in days:
        events += _cohort_events(d, "KOSPI", 16, reversed_rank=False)
    con.executemany("INSERT INTO fin_sue_event VALUES (?,?,?,?,?,?,?,?)", events)

    out = run_sue_event_ordinal_nonoverlap(con, [_ready_sue_cell()], **_ORDINAL_KWARGS)

    assert len(out) == 1
    row = out[0]
    assert row["hypothesis_id"] == "fin_sue|fin_sue|event|0|3"
    assert row["n_offsets_total"] == 2
    assert row["n_offsets_valid"] == 2
    assert row["offset_status"] == "complete"
    assert row["offset_sign_agreement_ratio"] == 1.0
    assert row["nonoverlap_robustness_pass"] is True


def test_run_sue_event_ordinal_nonoverlap_insufficient_cohorts_per_subsample() -> None:
    con = duckdb.connect()
    days = _weekdays(date(2020, 2, 3), 10)  # 10 dates / stride 2 -> 5 each (< min 8)
    _seed_calendar(con, days)
    _seed_event_table(con)
    events: list[tuple] = []
    for d in days:
        events += _cohort_events(d, "KOSPI", 16, reversed_rank=False)
    con.executemany("INSERT INTO fin_sue_event VALUES (?,?,?,?,?,?,?,?)", events)

    out = run_sue_event_ordinal_nonoverlap(con, [_ready_sue_cell()], **_ORDINAL_KWARGS)

    row = out[0]
    assert row["n_offsets_valid"] == 0
    assert row["offset_status"] == "some_insufficient"
    assert row["nonoverlap_robustness_pass"] is False


def test_run_sue_event_ordinal_nonoverlap_sign_disagreement_fails_gate() -> None:
    con = duckdb.connect()
    days = _weekdays(date(2020, 3, 2), 20)
    _seed_calendar(con, days)
    _seed_event_table(con)
    events: list[tuple] = []
    for i, d in enumerate(days):
        # subsample 0 (even ordinal) aligned, subsample 1 (odd ordinal) reversed
        events += _cohort_events(d, "KOSPI", 16, reversed_rank=(i % 2 == 1))
    con.executemany("INSERT INTO fin_sue_event VALUES (?,?,?,?,?,?,?,?)", events)

    out = run_sue_event_ordinal_nonoverlap(con, [_ready_sue_cell()], **_ORDINAL_KWARGS)

    row = out[0]
    assert row["n_offsets_valid"] == 2
    assert row["offset_status"] == "complete"
    assert row["offset_sign_agreement_ratio"] == 0.5
    assert row["nonoverlap_robustness_pass"] is False
