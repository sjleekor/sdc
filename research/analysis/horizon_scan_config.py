"""Load and validate the preregistered horizon-scan contract."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml

CONFIG_PATH = Path(__file__).with_name("horizon_scan_config.yaml")


@dataclass(frozen=True)
class HorizonScanConfig:
    raw: dict[str, Any]
    config_hash: str
    path: Path

    @property
    def families(self) -> list[dict[str, Any]]:
        return list(self.raw["families"])

    @property
    def horizons(self) -> list[int]:
        return list(self.raw["horizons"])

    @property
    def buckets(self) -> list[list[int]]:
        return list(self.raw["buckets"])

    @property
    def primary_hypothesis_count(self) -> int:
        return sum(
            len(f["primary_horizon_set"]) + len(bucket_primary_cells(f, self.buckets))
            for f in self.families
            if f.get("fdr_include", False)
        )


def bucket_primary_cells(family: dict[str, Any], buckets: list[list[int]]) -> list[tuple[int, int]]:
    """Buckets whose end falls in the family's primary cumulative horizon set.

    §2.1: "bucket은 h_end가 family의 primary horizon에 포함될 때만 해당 family의 주
    검정 셀로 간주한다" — bucket count is NOT simply the cumulative horizon count;
    it is the intersection of the fixed bucket grid's ends with the family's
    preregistered primary horizons (e.g. px_reversal_5d's {1,2,3,5,10} only meets
    the grid at {5,10}, giving 2 bucket cells, not 5).
    """
    if not family.get("include_bucket_primary", True):
        return []
    horizon_set = set(family["primary_horizon_set"])
    return [(b[0], b[1]) for b in buckets if b[1] in horizon_set]


def _canonical_hash(raw: dict[str, Any]) -> str:
    def jsonable(value: Any) -> Any:
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, dict):
            return {str(k): jsonable(v) for k, v in value.items()}
        if isinstance(value, list):
            return [jsonable(v) for v in value]
        return value

    encoded = json.dumps(
        jsonable(raw), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


_PHASE_A_FIXED_PROTOCOL_VALUES = {
    ("discovery", "universe"): "broad",
    ("discovery", "sample_kind"): "common_survivor",
    ("stats", "nw_p_value_distribution"): "asymptotic_normal",
    ("stats", "bh_missing_p_value"): 1.0,
    ("stats", "nonoverlap_all_offsets"): True,
    ("stats", "nonoverlap_inference"): "exact_sign_test",
    ("placebo", "cross_sectional_block"): "date_market",
    ("placebo", "temporal_min_nw_lag"): 59,
}
_EVIDENCE_GRADE_KEYS = {"evaluation_order", "A", "B", "C", "D", "R"}

# 04_specific_plan_B.md §2.1/§2.4 — frozen before any Phase B outcome is computed.
_PHASE_B_FIXED_PROTOCOL_VALUES = {
    "primary_candidate_count_max": 38,
    "phase_a_primary_count": 75,
    "readiness_freeze_before_label_join": True,
    "preflight_blocked_role": "blocked_exploratory",
    "post_freeze_blocked_p_for_bh": 1.0,
    "receipt_value_pairing_required": "verified_same_receipt",
    "receipt_value_pairing_error_tolerance": 0,
}
_PHASE_B_EVENT_BUCKETS = [[0, 3], [3, 5], [5, 10], [10, 20], [20, 40], [40, 60]]


def validate_config(raw: dict[str, Any]) -> None:
    required = {
        "schema_version",
        "horizons",
        "buckets",
        "families",
        "quality",
        "sample",
        "stats",
        "decision",
        "placebo",
        "discovery",
        "evidence_grade",
        "phase_b",
    }
    missing = required - raw.keys()
    if missing:
        raise ValueError(f"horizon scan config missing keys: {sorted(missing)}")
    if raw["horizons"] != [1, 2, 3, 5, 10, 20, 40, 60, 120]:
        raise ValueError("horizon grid differs from the preregistered A0 grid")
    if raw["buckets"] != [[0, 5], [5, 10], [10, 20], [20, 40], [40, 60], [60, 120]]:
        raise ValueError("bucket grid differs from the preregistered A0 grid")
    for (section, key), expected in _PHASE_A_FIXED_PROTOCOL_VALUES.items():
        actual = raw.get(section, {}).get(key)
        if actual != expected:
            raise ValueError(
                f"{section}.{key} must be the fixed Phase A protocol value {expected!r}, "
                f"got {actual!r}"
            )
    missing_grade_keys = _EVIDENCE_GRADE_KEYS - raw["evidence_grade"].keys()
    if missing_grade_keys:
        raise ValueError(f"evidence_grade missing keys: {sorted(missing_grade_keys)}")
    if raw["evidence_grade"]["evaluation_order"] != ["R", "C", "A", "B", "D"]:
        raise ValueError("evidence_grade.evaluation_order must check R/C before A/B/D")
    for key, expected in _PHASE_B_FIXED_PROTOCOL_VALUES.items():
        actual = raw["phase_b"].get(key)
        if actual != expected:
            raise ValueError(
                f"phase_b.{key} must be the fixed Phase B protocol value {expected!r}, "
                f"got {actual!r}"
            )
    if raw["phase_b"].get("event_buckets") != _PHASE_B_EVENT_BUCKETS:
        raise ValueError("phase_b.event_buckets differs from the preregistered event grid")
    family_names = {f.get("family") for f in raw["families"]}
    sparse_families = raw["stats"].get("sparse_primary_grid_families", [])
    unknown_sparse = [f for f in sparse_families if f not in family_names]
    if unknown_sparse:
        raise ValueError(
            f"sparse_primary_grid_families references unknown families: {unknown_sparse}"
        )
    families = raw["families"]
    if not isinstance(families, list) or len(families) != 25:
        raise ValueError("exactly 25 families must be registered")
    names = [f.get("family") for f in families]
    if len(set(names)) != len(names) or any(not n for n in names):
        raise ValueError("family names must be unique and non-empty")
    for family in families:
        features = family.get("features", [])
        primary = [f for f in features if f.get("role") == "primary"]
        if len(primary) != 1:
            raise ValueError(f"{family['family']}: exactly one primary feature is required")
        for key in ("primary_horizon_set", "exploratory_horizon_set", "official_feature_variant"):
            if key not in family:
                raise ValueError(f"{family['family']}: missing {key}")
        if "expected_sign" not in family:
            raise ValueError(f"{family['family']}: expected_sign must be explicit")
        if set(family["primary_horizon_set"]) & set(family["exploratory_horizon_set"]):
            raise ValueError(f"{family['family']}: primary and exploratory horizons overlap")
        all_h = set(family["primary_horizon_set"]) | set(family["exploratory_horizon_set"])
        if not all_h.issubset(set(raw["horizons"])):
            raise ValueError(f"{family['family']}: horizon outside fixed grid")
        if family.get("fdr_include") and family.get("role") == "reference_only":
            raise ValueError(f"{family['family']}: reference family cannot enter FDR")
        mapping = family.get("variant_columns", {})
        if not {"native_t", "lag1"}.issubset(mapping):
            raise ValueError(f"{family['family']}: native_t and lag1 mappings are required")
        if "event_buckets" in family and family["event_buckets"] != _PHASE_B_EVENT_BUCKETS:
            raise ValueError(f"{family['family']}: event_buckets differs from the frozen grid")
        if (
            family.get("role") == "ready"
            and family.get("family") != "flow_individual_netbuy_to_volume"
            and family.get("expected_sign") not in {"+", "-"}
        ):
            raise ValueError(f"{family['family']}: ready families require expected_sign '+' or '-'")
    short = [f for f in families if f.get("role") == "exploratory_short_regime"]
    if len(short) != 4:
        raise ValueError("exactly four short-selling families must be exploratory")
    if sum(1 for f in families if f.get("phase") == "A") != 17:
        raise ValueError("Phase A must contain 17 families")
    if sum(1 for f in families if f.get("phase") == "B") != 8:
        raise ValueError("Phase B must contain 8 families")
    fdr_count = sum(
        len(f["primary_horizon_set"]) + len(bucket_primary_cells(f, raw["buckets"]))
        for f in families
        if f.get("fdr_include", False)
    )
    if fdr_count != 75:
        raise ValueError(f"global primary hypothesis count must be 75, got {fdr_count}")
    short_count = sum(
        len(f["primary_horizon_set"]) + len(bucket_primary_cells(f, raw["buckets"])) for f in short
    )
    if short_count != 28:
        raise ValueError(f"short exploratory cell count must be 28, got {short_count}")
    phase_b_families = [f for f in families if f.get("phase") == "B"]
    phase_b_count = sum(
        (
            len(f["event_buckets"])
            if "event_buckets" in f
            else len(f["primary_horizon_set"]) + len(bucket_primary_cells(f, raw["buckets"]))
        )
        for f in phase_b_families
    )
    max_candidates = raw["phase_b"]["primary_candidate_count_max"]
    if phase_b_count != max_candidates:
        raise ValueError(f"Phase B candidate count must be {max_candidates}, got {phase_b_count}")


def load_config(path: Path | str = CONFIG_PATH) -> HorizonScanConfig:
    path = Path(path)
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("horizon scan YAML root must be an object")
    validate_config(raw)
    return HorizonScanConfig(raw=raw, config_hash=_canonical_hash(raw), path=path)
