from __future__ import annotations

import numpy as np
import polars as pl
import pytest
from research.analysis.horizon_scan_permutation import (
    apply_circular_feature_shift,
    circular_shift_session_index,
    derive_replicate_seed,
    empirical_discovery_count_p,
    permute_within_groups,
    select_circular_shift_distance,
    temporal_placebo_p,
)


def test_derive_replicate_seed_is_deterministic() -> None:
    a = derive_replicate_seed(placebo_kind="cross_sectional", replicate_index=0, config_hash="abc")
    b = derive_replicate_seed(placebo_kind="cross_sectional", replicate_index=0, config_hash="abc")
    assert a == b


def test_derive_replicate_seed_varies_with_each_input() -> None:
    base = derive_replicate_seed(
        placebo_kind="cross_sectional", replicate_index=0, config_hash="abc"
    )
    diff_index = derive_replicate_seed(
        placebo_kind="cross_sectional", replicate_index=1, config_hash="abc"
    )
    diff_kind = derive_replicate_seed(placebo_kind="temporal", replicate_index=0, config_hash="abc")
    diff_hash = derive_replicate_seed(
        placebo_kind="cross_sectional", replicate_index=0, config_hash="xyz"
    )
    assert len({base, diff_index, diff_kind, diff_hash}) == 4


def _sample_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": [1, 1, 1, 1, 2, 2, 2, 2],
            "market": ["KOSPI"] * 4 + ["KOSPI"] * 4,
            "ticker": ["A", "B", "C", "D", "A", "B", "C", "D"],
            "feat_1": [10.0, 20.0, 30.0, None, 11.0, 21.0, 31.0, 41.0],
            "feat_2": [1.0, 2.0, 3.0, None, 1.1, 2.1, 3.1, 4.1],
            "label": [100, 200, 300, 400, 101, 201, 301, 401],
        }
    )


def test_permute_within_groups_is_deterministic_for_a_fixed_seed() -> None:
    df = _sample_frame()
    a = permute_within_groups(
        df, group_cols=["trade_date", "market"], permute_cols=["feat_1", "feat_2"], seed=7
    )
    b = permute_within_groups(
        df, group_cols=["trade_date", "market"], permute_cols=["feat_1", "feat_2"], seed=7
    )
    assert a.equals(b)


def test_permute_within_groups_preserves_group_membership_and_row_vectors() -> None:
    df = _sample_frame()
    out = permute_within_groups(
        df, group_cols=["trade_date", "market"], permute_cols=["feat_1", "feat_2"], seed=3
    )
    # feat_1/feat_2 stay paired (moved together as one row vector) and the
    # multiset of values within each date is unchanged, just reassigned.
    for (_d,), grp in df.group_by(["trade_date"], maintain_order=True):
        out_grp = out.filter(pl.col("trade_date") == grp["trade_date"][0])
        assert sorted(grp["feat_1"].to_list(), key=str) == sorted(
            out_grp["feat_1"].to_list(), key=str
        )
        # the (feat_1, feat_2) pairing from the original rows is preserved as a set
        original_pairs = set(zip(grp["feat_1"].to_list(), grp["feat_2"].to_list()))
        out_pairs = set(zip(out_grp["feat_1"].to_list(), out_grp["feat_2"].to_list()))
        assert original_pairs == out_pairs


def test_permute_within_groups_keeps_labels_fixed_to_their_original_row() -> None:
    df = _sample_frame()
    out = permute_within_groups(
        df, group_cols=["trade_date", "market"], permute_cols=["feat_1", "feat_2"], seed=3
    )
    # ticker/label/trade_date/market are untouched — same values, same row order.
    assert out["ticker"].to_list() == df["ticker"].to_list()
    assert out["label"].to_list() == df["label"].to_list()
    assert out["trade_date"].to_list() == df["trade_date"].to_list()


def test_permute_within_groups_does_not_leak_across_groups() -> None:
    df = _sample_frame()
    out = permute_within_groups(
        df, group_cols=["trade_date", "market"], permute_cols=["feat_1", "feat_2"], seed=11
    )
    day1_values = set(df.filter(pl.col("trade_date") == 1)["feat_1"].drop_nulls().to_list())
    day2_out_values = set(out.filter(pl.col("trade_date") == 2)["feat_1"].drop_nulls().to_list())
    assert day1_values.isdisjoint(day2_out_values)


def test_permute_within_groups_is_independent_of_input_row_order() -> None:
    df = _sample_frame()
    shuffled = df.reverse()
    a = permute_within_groups(
        df, group_cols=["trade_date", "market"], permute_cols=["feat_1", "feat_2"], seed=42
    ).sort(["trade_date", "ticker"])
    b = permute_within_groups(
        shuffled, group_cols=["trade_date", "market"], permute_cols=["feat_1", "feat_2"], seed=42
    ).sort(["trade_date", "ticker"])
    assert a.equals(b)


def test_permute_within_groups_rejects_unknown_columns() -> None:
    df = _sample_frame()
    with pytest.raises(ValueError, match="not present"):
        permute_within_groups(
            df, group_cols=["trade_date"], permute_cols=["does_not_exist"], seed=1
        )


def test_select_circular_shift_distance_is_deterministic_and_in_band() -> None:
    for seed in range(10):
        shift = select_circular_shift_distance(seed=seed, total_sessions=1000, min_shift=120)
        assert 120 <= shift <= 880
        again = select_circular_shift_distance(seed=seed, total_sessions=1000, min_shift=120)
        assert shift == again


def test_select_circular_shift_distance_rejects_too_short_calendar() -> None:
    with pytest.raises(ValueError, match="too short"):
        select_circular_shift_distance(seed=1, total_sessions=200, min_shift=120)


def test_circular_shift_session_index_wraps_around() -> None:
    idx = np.array([1, 2, 3, 998, 999, 1000])
    shifted = circular_shift_session_index(idx, shift=5, total_sessions=1000)
    # sessions near the end wrap back to the beginning of the range
    assert shifted.tolist() == [6, 7, 8, 3, 4, 5]


def test_circular_shift_session_index_by_full_calendar_is_identity() -> None:
    idx = np.array([1, 500, 1000])
    shifted = circular_shift_session_index(idx, shift=1000, total_sessions=1000)
    assert shifted.tolist() == idx.tolist()


def test_apply_circular_feature_shift_relabels_session_and_preserves_rows() -> None:
    df = pl.DataFrame(
        {
            "session_idx": [1, 2, 3],
            "ticker": ["A", "B", "C"],
            "feature": [10.0, 20.0, 30.0],
        }
    )
    shifted = apply_circular_feature_shift(df, session_col="session_idx", shift=2, total_sessions=5)
    assert shifted["session_idx"].to_list() == [3, 4, 5]
    # only the session label changes — each ticker keeps its own feature value
    assert shifted["ticker"].to_list() == df["ticker"].to_list()
    assert shifted["feature"].to_list() == df["feature"].to_list()


def test_apply_circular_feature_shift_join_pairs_labels_with_a_different_dates_feature() -> None:
    # This is the actual placebo mechanic: relabel the feature frame's session
    # index by the shift, then join it onto the *original* label frame by
    # session_idx — every name's label now sees a different date's feature.
    labels = pl.DataFrame({"session_idx": [1, 2, 3], "ticker": ["A", "A", "A"], "label": [1, 2, 3]})
    features = pl.DataFrame(
        {"session_idx": [1, 2, 3], "ticker": ["A", "A", "A"], "feature": [100.0, 200.0, 300.0]}
    )
    shifted_features = apply_circular_feature_shift(
        features, session_col="session_idx", shift=1, total_sessions=3
    )
    placebo_panel = labels.join(shifted_features, on=["session_idx", "ticker"], how="inner")
    paired = dict(zip(placebo_panel["session_idx"].to_list(), placebo_panel["feature"].to_list()))
    # session 2's label now pairs with session 1's original feature (100.0),
    # not session 2's own (200.0) — the calendar alignment is broken as intended.
    assert paired[2] == pytest.approx(100.0)
    assert paired[1] == pytest.approx(300.0)  # session 1 wraps to session 3's feature


def test_empirical_discovery_count_p_matches_hand_computation() -> None:
    # 5 replicates; 3 of them have a null discovery count >= the real one (5)
    p = empirical_discovery_count_p(5, [3, 4, 5, 6, 7])
    assert p == pytest.approx((1 + 3) / (5 + 1))


def test_empirical_discovery_count_p_is_one_when_every_null_matches_or_exceeds() -> None:
    p = empirical_discovery_count_p(0, [0, 0, 0])
    assert p == pytest.approx(1.0)


def test_empirical_discovery_count_p_rejects_empty_input() -> None:
    with pytest.raises(ValueError):
        empirical_discovery_count_p(5, [])


def test_temporal_placebo_p_matches_hand_computation_and_threshold() -> None:
    result = temporal_placebo_p(3.0, [1.0, 2.0, 3.0, 4.0, 5.0])
    assert result["p_temporal_nw"] == pytest.approx((1 + 3) / (5 + 1))
    assert result["temporal_null_pass"] is False  # 0.667 is not < 0.10


def test_temporal_placebo_p_passes_when_real_t_is_extreme_under_the_null() -> None:
    result = temporal_placebo_p(10.0, [1.0] * 99)
    assert result["p_temporal_nw"] == pytest.approx(1 / 100)
    assert result["temporal_null_pass"] is True
