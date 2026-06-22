"""Unit tests for P6 — metrics (Rank IC / ICIR / top-decile / spread)."""

from __future__ import annotations

import datetime

import polars as pl
import pytest
from research.etl import metrics


def _df(rows: list[tuple]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": [r[0] for r in rows],
            "pred": [r[1] for r in rows],
            "realized": [r[2] for r in rows],
        }
    )


D1 = datetime.date(2020, 1, 1)
D2 = datetime.date(2020, 1, 2)


def test_perfect_rank_agreement_ic_is_one() -> None:
    # pred ranks == realized ranks within the date -> IC 1.0.
    df = _df([(D1, 1.0, 0.1), (D1, 2.0, 0.2), (D1, 3.0, 0.3), (D1, 4.0, 0.4)])
    ic = metrics.per_date_rank_ic(df, pred_col="pred", realized_col="realized")
    assert ic["rank_ic"][0] == pytest.approx(1.0)


def test_reversed_rank_ic_is_minus_one() -> None:
    df = _df([(D1, 1.0, 0.4), (D1, 2.0, 0.3), (D1, 3.0, 0.2), (D1, 4.0, 0.1)])
    ic = metrics.per_date_rank_ic(df, pred_col="pred", realized_col="realized")
    assert ic["rank_ic"][0] == pytest.approx(-1.0)


def test_rankdata_handles_ties() -> None:
    import numpy as np

    r = metrics._rankdata(np.array([10.0, 10.0, 20.0]))
    # tied first two -> average rank 1.5; third -> 3.
    assert list(r) == pytest.approx([1.5, 1.5, 3.0])


def test_evaluate_aggregates_over_dates() -> None:
    df = _df(
        [
            (D1, 1.0, 0.1),
            (D1, 2.0, 0.2),
            (D1, 3.0, 0.3),
            (D2, 1.0, 0.3),
            (D2, 2.0, 0.2),
            (D2, 3.0, 0.1),
        ]
    )
    rep = metrics.evaluate(df, pred_col="pred", realized_col="realized")
    assert rep.n_dates == 2
    assert rep.n_obs == 6
    # D1 IC=+1, D2 IC=-1 -> mean 0.
    assert rep.rank_ic_mean == pytest.approx(0.0)


def test_top_minus_bottom_positive_when_pred_orders_realized() -> None:
    # one date, 5 names, pred perfectly orders realized excess.
    rows = [(D1, float(i), float(i) / 100) for i in range(1, 6)]
    rep = metrics.evaluate(df := _df(rows), pred_col="pred", realized_col="realized", n_quantiles=5)
    assert rep.top_minus_bottom > 0  # top quantile realized > bottom quantile
    assert rep.hit_ratio_top == pytest.approx(1.0)  # top name has positive excess
    assert df.height == 5


def test_degenerate_single_obs_ic_is_nan() -> None:
    import math

    df = _df([(D1, 1.0, 0.5)])
    ic = metrics.per_date_rank_ic(df, pred_col="pred", realized_col="realized")
    assert math.isnan(ic["rank_ic"][0])


def test_nan_inf_rows_dropped_not_poisoning() -> None:
    """Fix #5: NaN/inf pred or realized are filtered, not left in the stats."""
    nan = float("nan")
    inf = float("inf")
    # D1: 4 clean perfectly-aligned rows + 1 NaN realized + 1 inf pred.
    rows = [
        (D1, 1.0, 0.1),
        (D1, 2.0, 0.2),
        (D1, 3.0, 0.3),
        (D1, 4.0, 0.4),
        (D1, 5.0, nan),  # NaN realized -> must be dropped
        (D1, inf, 0.5),  # inf pred -> must be dropped
    ]
    rep = metrics.evaluate(_df(rows), pred_col="pred", realized_col="realized", n_quantiles=2)
    # The 4 clean rows are perfectly rank-aligned -> IC 1.0, not NaN.
    assert rep.rank_ic_mean == pytest.approx(1.0)
    assert rep.n_obs == 4  # NaN + inf rows excluded
    # top-decile / spread must be finite (would be NaN if the NaN leaked in).
    assert rep.top_decile_spread == rep.top_decile_spread  # not NaN
    assert rep.top_minus_bottom == rep.top_minus_bottom
