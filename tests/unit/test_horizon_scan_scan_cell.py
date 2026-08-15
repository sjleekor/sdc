from __future__ import annotations

import math

import duckdb
import pytest
from research.analysis.horizon_scan_runner import build_formation_sql, run_registry_scan, scan_cell


def _seed_scan_panel(con: duckdb.DuckDBPyConnection, *, n_sessions: int = 30) -> None:
    """A panel where feature and label are strongly rank-correlated within each
    date×market cross-section but not identically so every session (a small
    deterministic wobble keeps daily IC's variance nonzero, as real data would
    have it — a perfectly constant daily IC degenerately zeros ic_std and
    would make t_nw NaN for a reason unrelated to what these tests check)."""
    rows = []
    for session in range(1, n_sessions + 1):
        d = f"2024-01-01' + {session - 1}"
        for market in ("KOSPI", "KOSDAQ"):
            for t in range(10):
                ticker = f"{market[:1]}{t}"
                feature = float(t) + 0.01 * session  # ticker rank dominates
                # amplitude > the t-spacing (2.0) so some sessions actually
                # invert a couple of adjacent ranks, not just perturb magnitude
                wobble = 3.0 * math.sin(t + 0.7 * session)
                raw_label = float(t) * 2.0 + wobble  # mostly monotone in feature
                # per_date_market_rank_ic re-ranks its inputs internally, so
                # reusing the same wobbled value here (rather than a fixed
                # t/9 rank) lets the day-to-day wobble actually reach the IC.
                rank_label = raw_label
                in_broad = True
                in_tradable = t < 8  # two names per market fall outside tradable
                common_survivor = t != 9  # one name per market is not a survivor
                ca_mask = False
                rows.append(
                    f"(DATE '{d}, '{ticker}', '{market}', {session}, "
                    f"{feature}, {rank_label}, {raw_label}, true, "
                    f"{str(in_broad).lower()}, {str(in_tradable).lower()}, "
                    f"true, {str(common_survivor).lower()}, {str(ca_mask).lower()})"
                )
    con.execute(
        "CREATE TABLE analysis_panel AS SELECT "
        "trade_date, ticker, market, formation_session_idx, "
        "CAST(px_feature AS DOUBLE) AS px_feature, "
        "CAST(y_rank_5d AS DOUBLE) AS y_rank_5d, "
        "CAST(raw_label_5d AS DOUBLE) AS raw_label_5d, "
        "label_ok_5d, in_broad, in_tradable, common_formation_120d, "
        "common_survivor_120d, ca_mask "
        "FROM (VALUES "
        + ",".join(rows)
        + ") t(trade_date, ticker, market, formation_session_idx, "
        "px_feature, y_rank_5d, raw_label_5d, label_ok_5d, "
        "in_broad, in_tradable, common_formation_120d, common_survivor_120d, ca_mask)"
    )
    # bucket columns for the (0,5] bucket, reusing the same 5d values
    con.execute("""
        ALTER TABLE analysis_panel ADD COLUMN y_rank_bucket_0_5d DOUBLE;
        UPDATE analysis_panel SET y_rank_bucket_0_5d = y_rank_5d;
    """)
    con.execute("""
        ALTER TABLE analysis_panel ADD COLUMN raw_bucket_label_0_5d DOUBLE;
        UPDATE analysis_panel SET raw_bucket_label_0_5d = raw_label_5d;
    """)
    con.execute("""
        ALTER TABLE analysis_panel ADD COLUMN bucket_ok_0_5d BOOLEAN;
        UPDATE analysis_panel SET bucket_ok_0_5d = label_ok_5d;
    """)


def test_build_formation_sql_extra_where_restricts_to_one_market() -> None:
    con = duckdb.connect()
    _seed_scan_panel(con)
    sql = build_formation_sql(
        panel_view="analysis_panel",
        feature_col="px_feature",
        scan_type="cum",
        h_start=0,
        h_end=5,
        universe="broad",
        sample_kind="available",
        sample_start="2024-01-01",
        extra_where="market = 'KOSPI'",
    )
    markets = {r[0] for r in con.execute(f"SELECT DISTINCT market FROM ({sql})").fetchall()}
    assert markets == {"KOSPI"}


def test_scan_cell_extra_where_restricts_the_scanned_population() -> None:
    con = duckdb.connect()
    _seed_scan_panel(con)
    both_markets = scan_cell(
        con,
        feature_col="px_feature",
        scan_type="cum",
        h_start=0,
        h_end=5,
        universe="broad",
        sample_kind="available",
        sample_start="2024-01-01",
        min_names=5,
        min_names_for_spread=5,
        quantile_count=5,
        min_dates_per_cell=5,
    )
    kospi_only = scan_cell(
        con,
        feature_col="px_feature",
        scan_type="cum",
        h_start=0,
        h_end=5,
        universe="broad",
        sample_kind="available",
        sample_start="2024-01-01",
        min_names=5,
        min_names_for_spread=5,
        quantile_count=5,
        min_dates_per_cell=5,
        extra_where="market = 'KOSPI'",
    )
    assert kospi_only["n_obs"] < both_markets["n_obs"]
    assert kospi_only["kosdaq_weight_mean"] == pytest.approx(0.0)


def test_build_formation_sql_common_survivor_excludes_non_survivors() -> None:
    con = duckdb.connect()
    _seed_scan_panel(con)
    sql = build_formation_sql(
        panel_view="analysis_panel",
        feature_col="px_feature",
        scan_type="cum",
        h_start=0,
        h_end=5,
        universe="broad",
        sample_kind="common_survivor",
        sample_start="2024-01-01",
    )
    (n,) = con.execute(f"SELECT count(*) FROM ({sql})").fetchone()
    # 30 sessions x 2 markets x 9 survivors (t=9 excluded) = 540
    assert n == 30 * 2 * 9


def test_build_formation_sql_available_keeps_non_survivors() -> None:
    con = duckdb.connect()
    _seed_scan_panel(con)
    sql = build_formation_sql(
        panel_view="analysis_panel",
        feature_col="px_feature",
        scan_type="cum",
        h_start=0,
        h_end=5,
        universe="broad",
        sample_kind="available",
        sample_start="2024-01-01",
    )
    (n,) = con.execute(f"SELECT count(*) FROM ({sql})").fetchone()
    assert n == 30 * 2 * 10


def test_build_formation_sql_tradable_universe_excludes_flagged_names() -> None:
    con = duckdb.connect()
    _seed_scan_panel(con)
    sql = build_formation_sql(
        panel_view="analysis_panel",
        feature_col="px_feature",
        scan_type="cum",
        h_start=0,
        h_end=5,
        universe="tradable",
        sample_kind="available",
        sample_start="2024-01-01",
    )
    (n,) = con.execute(f"SELECT count(*) FROM ({sql})").fetchone()
    assert n == 30 * 2 * 8


def test_scan_cell_reports_valid_status_with_strong_positive_ic() -> None:
    con = duckdb.connect()
    _seed_scan_panel(con)
    result = scan_cell(
        con,
        feature_col="px_feature",
        scan_type="cum",
        h_start=0,
        h_end=5,
        universe="broad",
        sample_kind="common_survivor",
        sample_start="2024-01-01",
        min_names=5,
        min_names_for_spread=5,
        quantile_count=5,
        min_dates_per_cell=5,
        expected_sign="+",
    )
    assert result["status"] == "valid"
    assert result["ic_mean"] > 0.9
    assert result["ic_std"] > 0  # the wobble keeps daily IC from being degenerate
    assert result["n_dates"] == 30
    assert result["t_nw"] > 0
    assert result["p_nw"] < 0.01
    assert result["q5_spread_raw"] is not None
    assert result["q5_spread_aligned"] == pytest.approx(result["q5_spread_raw"])
    assert result["kospi_weight_mean"] == pytest.approx(0.5)


def test_scan_cell_negative_expected_sign_flips_aligned_spread() -> None:
    con = duckdb.connect()
    _seed_scan_panel(con)
    result = scan_cell(
        con,
        feature_col="px_feature",
        scan_type="cum",
        h_start=0,
        h_end=5,
        universe="broad",
        sample_kind="common_survivor",
        sample_start="2024-01-01",
        min_names=5,
        min_names_for_spread=5,
        quantile_count=5,
        min_dates_per_cell=5,
        expected_sign="-",
    )
    assert result["q5_spread_aligned"] == pytest.approx(-result["q5_spread_raw"])


def test_scan_cell_marks_insufficient_dates_explicitly() -> None:
    con = duckdb.connect()
    _seed_scan_panel(con, n_sessions=30)
    result = scan_cell(
        con,
        feature_col="px_feature",
        scan_type="cum",
        h_start=0,
        h_end=5,
        universe="broad",
        sample_kind="common_survivor",
        sample_start="2024-01-01",
        min_names=5,
        min_names_for_spread=5,
        quantile_count=5,
        min_dates_per_cell=60,  # far above the 30 sessions actually seeded
        expected_sign="+",
    )
    assert result["status"] == "insufficient"
    assert result["status_reason"].startswith("insufficient_dates:30<60")


def test_scan_cell_reports_no_formation_rows_for_impossible_universe() -> None:
    con = duckdb.connect()
    _seed_scan_panel(con)
    con.execute("UPDATE analysis_panel SET in_broad = false")
    result = scan_cell(
        con,
        feature_col="px_feature",
        scan_type="cum",
        h_start=0,
        h_end=5,
        universe="broad",
        sample_kind="common_survivor",
        sample_start="2024-01-01",
        min_names=5,
        min_names_for_spread=5,
        quantile_count=5,
        min_dates_per_cell=5,
    )
    assert result["status"] == "insufficient"
    assert result["status_reason"] == "no_formation_rows"


def test_scan_cell_bucket_scan_type_uses_bucket_columns() -> None:
    con = duckdb.connect()
    _seed_scan_panel(con)
    result = scan_cell(
        con,
        feature_col="px_feature",
        scan_type="bucket",
        h_start=0,
        h_end=5,
        universe="broad",
        sample_kind="common_survivor",
        sample_start="2024-01-01",
        min_names=5,
        min_names_for_spread=5,
        quantile_count=5,
        min_dates_per_cell=5,
        expected_sign="+",
    )
    assert result["status"] == "valid"
    assert result["width"] == 5
    assert result["ic_mean"] > 0.9


def test_run_registry_scan_covers_every_universe_sample_combo() -> None:
    con = duckdb.connect()
    _seed_scan_panel(con)
    registry = [
        {
            "hypothesis_id": "fam|px_feature|cum|0|5",
            "family": "fam",
            "feature": "px_feature",
            "scan_type": "cum",
            "h_start": 0,
            "h_end": 5,
            "expected_sign": "+",
        }
    ]
    rows = run_registry_scan(
        con,
        registry,
        sample_start="2024-01-01",
        min_names=5,
        min_names_for_spread=5,
        quantile_count=5,
        min_dates_per_cell=5,
    )
    assert len(rows) == 4
    combos = {(r["universe"], r["sample_kind"]) for r in rows}
    assert combos == {
        ("broad", "common_survivor"),
        ("broad", "available"),
        ("tradable", "common_survivor"),
        ("tradable", "available"),
    }
    assert all(r["hypothesis_id"] == "fam|px_feature|cum|0|5" for r in rows)
    assert all(r["status"] == "valid" for r in rows)
    tradable_available = next(
        r for r in rows if r["universe"] == "tradable" and r["sample_kind"] == "available"
    )
    assert tradable_available["n_obs"] > 0
