"""Unit tests for KSIC industry grouping (N2).

The codes here are real ``induty_code`` values observed on 2026-08-15. They are
the reason the grouping rule takes a PREFIX rather than a fixed width: lengths
of 2, 3, 4 and 5 all occur in the same response set.
"""

from __future__ import annotations

from krx_collector.definitions.industry_groups import (
    MIN_GROUP_SIZE,
    UNKNOWN_GROUP,
    UNKNOWN_SECTION,
    group_sizes,
    industry_group,
    industry_section,
    is_financial,
    resolve_groups,
)


def test_prefix_rule_handles_every_observed_code_length() -> None:
    # 3, 4 and 5 digits, all from the same live sample.
    assert industry_group("264") == "26"  # 삼성전자
    assert industry_group("5821") == "58"  # 크래프톤
    assert industry_group("21100") == "21"  # 셀트리온
    assert industry_group("65121") == "65"  # 삼성화재
    assert industry_group("50112") == "50"  # HMM
    # A 2-digit code already IS the middle category.
    assert industry_group("26") == "26"


def test_missing_or_junk_code_becomes_unknown() -> None:
    assert industry_group(None) == UNKNOWN_GROUP
    assert industry_group("") == UNKNOWN_GROUP
    assert industry_group("  ") == UNKNOWN_GROUP
    assert industry_group("2") == UNKNOWN_GROUP
    assert industry_group("A1") == UNKNOWN_GROUP


def test_code_is_stripped_before_slicing() -> None:
    assert industry_group(" 264 ") == "26"


def test_section_lookup_covers_the_ksic_ranges() -> None:
    assert industry_section("26") == "C"  # 제조업
    assert industry_section("58") == "J"  # 정보통신업
    assert industry_section("64") == "K"  # 금융 및 보험업
    assert industry_section("50") == "H"  # 운수 및 창고업
    assert industry_section("68") == "L"  # 부동산업
    # 43 falls between the F (41-42) and G (45-47) ranges.
    assert industry_section("43") == UNKNOWN_SECTION
    assert industry_section(UNKNOWN_GROUP) == UNKNOWN_SECTION


def test_financial_groups_are_flagged() -> None:
    # LG (holding) and KB Financial Group (bank holding) both return 64992, so
    # this flag marks "cannot be separated here", not "is a bank".
    assert is_financial(industry_group("64992")) is True
    assert is_financial("65") is True
    assert is_financial("66") is True
    assert is_financial("26") is False


def test_large_groups_are_kept_and_small_groups_fold_into_their_section() -> None:
    codes = {f"big{i}": "26100" for i in range(MIN_GROUP_SIZE)}
    codes["small1"] = "5821"  # section J
    codes["small2"] = "5822"

    resolved = resolve_groups(codes)

    assert resolved["big0"] == "26"
    assert resolved["small1"] == "J"
    assert resolved["small2"] == "J"


def test_unknown_codes_stay_their_own_bucket() -> None:
    # A missing code is not evidence of any industry, so it must not be folded
    # into a real section.
    codes = {f"big{i}": "26100" for i in range(MIN_GROUP_SIZE)}
    codes["missing"] = None

    resolved = resolve_groups(codes)

    assert resolved["missing"] == UNKNOWN_GROUP


def test_fold_up_threshold_is_configurable() -> None:
    codes = {"a": "26100", "b": "26200", "c": "5821"}

    lenient = resolve_groups(codes, min_group_size=2)
    strict = resolve_groups(codes, min_group_size=3)

    assert lenient["a"] == "26"
    assert lenient["c"] == "J"  # only 1 member
    assert strict["a"] == "C"  # 2 members < 3


def test_group_sizes_counts_members() -> None:
    assert group_sizes(["26", "26", "58"]) == {"26": 2, "58": 1}
