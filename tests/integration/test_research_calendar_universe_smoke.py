"""Integration smoke test for P2 against the real lake (calendar + universe).

Self-skips when the raw lake is absent. To avoid writing mart parquet into the
repo's ``data_lake/``, this builds the calendar/universe SQL as in-memory views
rather than materializing — the SQL is identical, only the COPY-to-disk step is
skipped. Materialization itself is covered by the unit tests (tmp_path).
"""

from __future__ import annotations

import pytest
from research.etl import calendar, universe
from research.etl.config import EngineOptions, LakeConfig
from research.etl.lake import connect, register_views


@pytest.fixture()
def real_con():
    cfg = LakeConfig(engine=EngineOptions(threads=4))
    if not cfg.raw_root.exists():
        pytest.skip(f"raw lake not present at {cfg.raw_root}")
    con = connect(cfg)
    register_views(con, cfg, tables=["daily_ohlcv"])
    cal_sql = calendar.build_calendar_sql("daily_ohlcv")
    uni_sql = universe.build_universe_sql(universe.UniverseFilter())
    con.execute(f"CREATE VIEW dim_trading_calendar AS {cal_sql}")
    con.execute(f"CREATE VIEW dim_universe_daily AS {uni_sql}")
    return con


def test_calendar_d_idx_monotonic_per_market(real_con) -> None:
    # d_idx must be dense 1..N per market (max == count, no gaps/dups).
    rows = real_con.execute(
        "SELECT market, count(*), max(d_idx), min(d_idx) "
        "FROM dim_trading_calendar GROUP BY market"
    ).fetchall()
    assert rows
    for _market, cnt, dmax, dmin in rows:
        assert dmin == 1
        assert dmax == cnt


def test_universe_rowcount_matches_daily_ohlcv(real_con) -> None:
    # One universe row per price row (flags annotate, never drop rows).
    (u,) = real_con.execute("SELECT count(*) FROM dim_universe_daily").fetchone()
    (p,) = real_con.execute("SELECT count(*) FROM daily_ohlcv").fetchone()
    assert u == p


def test_universe_2015_plus_breadth_is_reasonable(real_con) -> None:
    # ~1,900-2,500 in-universe names/day in the 2015+ cross-section (etl_00 §1.1).
    (avg_names,) = real_con.execute("""
        SELECT avg(c) FROM (
          SELECT trade_date, count(*) AS c FROM dim_universe_daily
          WHERE in_universe AND trade_date >= DATE '2015-01-02'
          GROUP BY trade_date
        )
        """).fetchone()
    assert 1_000 < avg_names < 2_800
