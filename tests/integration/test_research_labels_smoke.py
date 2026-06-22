"""Integration smoke test for P4 against the real lake (make_label).

Self-skips when the raw lake is absent. Pins the label row count to the measured
figure (etl_02 §3 Q2/Q3 = 6,408,188) and checks the eqw-excess mean is ~0 and
ranks lie in [0,1] on the 2015+ cross-section.
"""

from __future__ import annotations

import pytest
from research.etl import labels
from research.etl.config import EngineOptions, LakeConfig
from research.etl.lake import connect, register_views

# Measured on the 2026-06-19 lake (etl_02 §3 Q2/Q3 row count). Regression guard.
EXPECTED_LABEL_ROWS = 6_408_188


@pytest.fixture()
def label_con():
    cfg = LakeConfig(engine=EngineOptions(threads=4, memory_limit="4GB"))
    if not cfg.raw_root.exists():
        pytest.skip(f"raw lake not present at {cfg.raw_root}")
    con = connect(cfg)
    register_views(con, cfg, tables=["daily_ohlcv"])
    spec = labels.LabelSpec()  # (20,5,60), excess, eqw, all outputs
    con.execute(f"CREATE VIEW label_daily AS {labels.build_label_sql(spec)}")
    return con


def test_label_rowcount_matches_measured(label_con) -> None:
    (n,) = label_con.execute("SELECT count(*) FROM label_daily").fetchone()
    assert n == EXPECTED_LABEL_ROWS


def test_eqw_excess_mean_is_zero_and_rank_bounded(label_con) -> None:
    row = label_con.execute("""
        SELECT AVG(raw_label_20d), MIN(y_rank_20d), MAX(y_rank_20d)
        FROM label_daily
        WHERE trade_date >= DATE '2015-01-02' AND raw_label_20d IS NOT NULL
        """).fetchone()
    avg_excess, rmin, rmax = row
    assert abs(avg_excess) < 1e-9  # per (date, market) excess sums to ~0
    assert rmin == pytest.approx(0.0)
    assert rmax == pytest.approx(1.0)


def test_cls_top_fraction_near_threshold(label_con) -> None:
    (top_frac,) = label_con.execute("""
        SELECT AVG(CASE WHEN y_cls_20d = 1 THEN 1.0 ELSE 0 END)
        FROM label_daily
        WHERE trade_date >= DATE '2015-01-02' AND raw_label_20d IS NOT NULL
        """).fetchone()
    # cls_top = 0.8 -> roughly the top 20% are class 1.
    assert 0.18 < top_frac < 0.22
