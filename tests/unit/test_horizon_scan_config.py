from __future__ import annotations

from copy import deepcopy

import pytest
from research.analysis.horizon_scan_config import (
    _canonical_hash,
    bucket_primary_cells,
    load_config,
    validate_config,
)
from research.analysis.horizon_scan_readiness import build_readiness_rows


def test_preregistered_registry_and_hypothesis_count() -> None:
    config = load_config()
    assert len(config.families) == 25
    assert sum(f["phase"] == "A" for f in config.families) == 17
    assert config.primary_hypothesis_count == 75
    assert sum(f["role"] == "exploratory_short_regime" for f in config.families) == 4


def test_bucket_cells_are_grid_intersection_not_cumulative_count() -> None:
    """px_reversal_5d's cumulative {1,2,3,5,10} only meets the bucket grid at
    {5,10} — a naive len(primary_horizon_set) bucket count would wrongly give 5
    (04_specific_plan_A.md §2.1 table: 5 cumulative + 2 bucket = 7 BH cells)."""
    config = load_config()
    buckets = config.buckets
    by_family = {f["family"]: f for f in config.families}

    reversal = by_family["px_reversal_5d"]
    assert bucket_primary_cells(reversal, buckets) == [(0, 5), (5, 10)]
    assert len(reversal["primary_horizon_set"]) + len(bucket_primary_cells(reversal, buckets)) == 7

    holding_chg = by_family["flow_foreign_holding_ratio_chg"]
    assert bucket_primary_cells(holding_chg, buckets) == [(10, 20), (20, 40), (40, 60)]

    short_turnover = by_family["flow_short_turnover"]
    assert (
        len(short_turnover["primary_horizon_set"])
        + len(bucket_primary_cells(short_turnover, buckets))
        == 10
    )


def test_missing_marts_and_unresolved_lag_are_explicit() -> None:
    config = load_config()
    rows = build_readiness_rows(
        config,
        columns_by_mart={"feat_price": {"px_reversal_5d"}},
        publication_lag_verified=False,
    )
    by_family = {row["family"]: row for row in rows}
    assert by_family["px_reversal_5d"]["status"] == "blocked_missing_dependency"
    assert by_family["flow_short_interest"]["status"] == "exploratory_blocked_publication_lag"
    assert by_family["fin_sue"]["status"] == "phase_b_blocked"


def test_validator_rejects_overlapping_horizons_and_missing_sign() -> None:
    raw = deepcopy(load_config().raw)
    raw["families"][0]["exploratory_horizon_set"].append(
        raw["families"][0]["primary_horizon_set"][0]
    )
    with pytest.raises(ValueError, match="overlap"):
        validate_config(raw)

    raw = deepcopy(load_config().raw)
    del raw["families"][0]["expected_sign"]
    with pytest.raises(ValueError, match="expected_sign"):
        validate_config(raw)


def test_phase_a_discovery_population_is_fixed() -> None:
    config = load_config()
    assert config.raw["discovery"]["universe"] == "broad"
    assert config.raw["discovery"]["sample_kind"] == "common_survivor"

    raw = deepcopy(config.raw)
    raw["discovery"]["universe"] = "tradable"
    with pytest.raises(ValueError, match="discovery.universe"):
        validate_config(raw)


def test_phase_a_missing_section_is_rejected() -> None:
    raw = deepcopy(load_config().raw)
    del raw["evidence_grade"]
    with pytest.raises(ValueError, match="missing keys"):
        validate_config(raw)


def test_evidence_grade_rubric_and_evaluation_order() -> None:
    config = load_config()
    assert config.raw["evidence_grade"]["evaluation_order"] == ["R", "C", "A", "B", "D"]

    raw = deepcopy(config.raw)
    del raw["evidence_grade"]["B"]
    with pytest.raises(ValueError, match="evidence_grade missing keys"):
        validate_config(raw)

    raw = deepcopy(config.raw)
    raw["evidence_grade"]["evaluation_order"] = ["A", "B", "C", "D", "R"]
    with pytest.raises(ValueError, match="R/C before"):
        validate_config(raw)


def test_sparse_primary_grid_families_must_be_registered() -> None:
    raw = deepcopy(load_config().raw)
    raw["stats"]["sparse_primary_grid_families"] = ["does_not_exist"]
    with pytest.raises(ValueError, match="unknown families"):
        validate_config(raw)


def test_bh_missing_p_value_is_fixed_at_one() -> None:
    raw = deepcopy(load_config().raw)
    raw["stats"]["bh_missing_p_value"] = 0.5
    with pytest.raises(ValueError, match="bh_missing_p_value"):
        validate_config(raw)


def test_scan_engine_is_runtime_only_for_config_hash() -> None:
    raw = {
        "execution": {"scan_engine": "legacy", "mapping_contract_version": "joint_cs_v2"},
        "sample": {"start": "2014-06-01"},
    }
    native = {
        **raw,
        "execution": {**raw["execution"], "scan_engine": "polars_native_v1"},
    }
    changed_contract = {
        **raw,
        "execution": {**raw["execution"], "mapping_contract_version": "v1"},
    }

    assert _canonical_hash(raw) == _canonical_hash(native)
    assert _canonical_hash(raw) != _canonical_hash(changed_contract)
