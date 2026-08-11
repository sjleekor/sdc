from __future__ import annotations

from datetime import date

import duckdb
from research.etl.marts.metric_vintages import (
    SMVF_TABLE,
    register_stock_metric_vintage_fact_view,
)


def _weekdays_in(year: int, month: int) -> list[date]:
    days = []
    day = 1
    while True:
        try:
            d = date(year, month, day)
        except ValueError:
            break
        if d.weekday() < 5:
            days.append(d)
        day += 1
    return days


_TRADING_DAYS = [d for month in range(1, 7) for d in _weekdays_in(2026, month)]


def _base_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE dart_corp_master (ticker VARCHAR, market VARCHAR, corp_code VARCHAR)")
    con.execute(
        "CREATE TABLE dart_financial_statement_raw ("
        "corp_code VARCHAR, ticker VARCHAR, bsns_year INTEGER, reprt_code VARCHAR, "
        "fs_div VARCHAR, sj_div VARCHAR, account_id VARCHAR, account_nm VARCHAR, "
        "ord INTEGER, thstrm_amount DECIMAL(30,4), currency VARCHAR, rcept_no VARCHAR, "
        "thstrm_add_amount DECIMAL(30,4), frmtrm_q_amount DECIMAL(30,4))"
    )
    con.execute(
        "CREATE TABLE dart_share_count_raw ("
        "corp_code VARCHAR, ticker VARCHAR, bsns_year INTEGER, reprt_code VARCHAR, "
        "rcept_no VARCHAR, se VARCHAR, istc_totqy BIGINT, tesstk_co BIGINT, stlm_dt DATE)"
    )
    con.execute(
        "CREATE TABLE dart_shareholder_return_raw ("
        "corp_code VARCHAR, ticker VARCHAR, bsns_year INTEGER, reprt_code VARCHAR, "
        "rcept_no VARCHAR, statement_type VARCHAR, row_name VARCHAR, stock_knd VARCHAR, "
        "dim1 VARCHAR, dim2 VARCHAR, dim3 VARCHAR, metric_code VARCHAR, "
        "value_numeric DECIMAL(30,4), stlm_dt DATE)"
    )
    con.execute(
        "CREATE TABLE dart_xbrl_fact_raw ("
        "corp_code VARCHAR, ticker VARCHAR, bsns_year INTEGER, reprt_code VARCHAR, "
        "rcept_no VARCHAR, concept_id VARCHAR, concept_name VARCHAR, context_id VARCHAR, "
        "context_type VARCHAR, period_start DATE, period_end DATE, instant_date DATE, "
        "dimensions VARCHAR, value_numeric DECIMAL(30,4), label_ko VARCHAR)"
    )
    con.execute(
        "CREATE TABLE dart_filing_receipt_raw "
        "(corp_code VARCHAR, rcept_no VARCHAR, report_nm VARCHAR)"
    )
    con.execute("INSERT INTO dart_corp_master VALUES ('005930', 'KOSPI', '00126380')")
    return con


def _register(con: duckdb.DuckDBPyConnection) -> None:
    register_stock_metric_vintage_fact_view(con, trading_days=_TRADING_DAYS)


def test_baseline_identity_transform_no_receipt_history() -> None:
    """Pre-B-1 state: single captured rcept_no per filing, no receipt list at
    all — every row must land as captured_vintages_only, not blocked."""
    con = _base_con()
    con.execute(
        "INSERT INTO dart_financial_statement_raw "
        "(corp_code, ticker, bsns_year, reprt_code, fs_div, sj_div, account_id, "
        "account_nm, ord, thstrm_amount, currency, rcept_no) VALUES "
        "('00126380','005930',2025,'11011','CFS','IS','ifrs-full_Revenue','매출액',1,"
        "1000.0000,'KRW','20260310000001')"
    )
    con.execute(
        "INSERT INTO dart_xbrl_fact_raw VALUES "
        "('00126380','005930',2025,'11011','20260310000001','ifrs-full_Revenue','매출액',"
        "'ctx1','duration',DATE '2025-01-01',DATE '2025-12-31',NULL,'[]',1000.0000,'매출액')"
    )
    con.execute(
        "INSERT INTO dart_share_count_raw VALUES "
        "('00126380','005930',2025,'11011','20260310000001','합계',5000,100,DATE '2025-12-31')"
    )
    _register(con)

    rows = con.execute(
        f"SELECT metric_code, rcept_no, disclosed_date, available_from, availability_source, "
        f"captured_vintage_status, receipt_value_pairing_status, period_end_source, "
        f"statement_period_end FROM {SMVF_TABLE} ORDER BY metric_code"
    ).fetchall()
    by_metric = {r[0]: r for r in rows}

    revenue = by_metric["revenue"]
    assert revenue[1] == "20260310000001"
    assert revenue[2] == date(2026, 3, 10)
    assert revenue[3] == date(2026, 3, 11)  # next KRX session after 2026-03-10 (Tue)
    assert revenue[4] == "rcept_no"
    assert revenue[5] == "captured_vintages_only"
    assert revenue[6] == "verified_same_receipt"
    assert revenue[7] == "xbrl"
    assert revenue[8] == date(2025, 12, 31)

    issued_shares = by_metric["issued_shares"]
    assert issued_shares[5] == "captured_vintages_only"
    assert issued_shares[6] == "not_applicable"


def test_missing_rcept_no_uses_synthetic_fallback() -> None:
    con = _base_con()
    con.execute(
        "INSERT INTO dart_share_count_raw VALUES "
        "('00126380','005930',2025,'11011','','합계',5000,100,DATE '2025-12-31')"
    )
    _register(con)

    row = con.execute(
        f"SELECT availability_source, available_from, statement_period_end "
        f"FROM {SMVF_TABLE} WHERE metric_code = 'issued_shares'"
    ).fetchone()
    assert row[0] == "synthetic_fallback"
    # annual report -> period_end + 90 days
    assert row[1] == date(2025, 12, 31) + __import__("datetime").timedelta(days=90)
    assert row[2] == date(2025, 12, 31)


def test_multi_vintage_preserves_both_receipts_after_backfill() -> None:
    """B-1 backfill scenario: original + a later correction, both linked via
    dart_filing_receipt_raw. Grain must NOT collapse to one winner."""
    con = _base_con()
    con.execute(
        "INSERT INTO dart_financial_statement_raw "
        "(corp_code, ticker, bsns_year, reprt_code, fs_div, sj_div, account_id, "
        "account_nm, ord, thstrm_amount, currency, rcept_no) VALUES "
        "('00126380','005930',2025,'11011','CFS','IS','ifrs-full_Revenue','매출액',1,"
        "1000.0000,'KRW','20260310000001'),"
        "('00126380','005930',2025,'11011','CFS','IS','ifrs-full_Revenue','매출액',1,"
        "1100.0000,'KRW','20260405000002')"
    )
    con.execute(
        "INSERT INTO dart_filing_receipt_raw VALUES "
        "('00126380','20260310000001','사업보고서'),"
        "('00126380','20260405000002','[기재정정]사업보고서')"
    )
    _register(con)

    rows = con.execute(
        f"SELECT rcept_no, disclosed_date, available_from, is_revision, original_rcept_no, "
        f"captured_vintage_status FROM {SMVF_TABLE} "
        f"WHERE metric_code = 'revenue' ORDER BY rcept_no"
    ).fetchall()
    assert len(rows) == 2

    original, revision = rows
    assert original[0] == "20260310000001"
    assert original[3] is False
    assert original[4] == "20260310000001"
    assert original[5] == "original_confirmed_revisions_partial"

    assert revision[0] == "20260405000002"
    assert revision[3] is True
    assert revision[4] is None
    assert revision[5] == "original_confirmed_revisions_partial"

    # Each vintage's own availability, independently computed.
    assert original[2] == date(2026, 3, 11)
    assert revision[2] == date(2026, 4, 6)


def test_row_without_receipt_match_is_unlinked_when_siblings_are_matched() -> None:
    con = _base_con()
    con.execute(
        "INSERT INTO dart_financial_statement_raw "
        "(corp_code, ticker, bsns_year, reprt_code, fs_div, sj_div, account_id, "
        "account_nm, ord, thstrm_amount, currency, rcept_no) VALUES "
        "('00126380','005930',2025,'11011','CFS','IS','ifrs-full_Revenue','매출액',1,"
        "1000.0000,'KRW','20260310000001'),"
        "('00126380','005930',2025,'11011','CFS','IS','ifrs-full_Revenue','매출액',1,"
        "1100.0000,'KRW','20260405000002')"
    )
    # Only the first receipt is in the receipt list; the second is unlinkable.
    con.execute(
        "INSERT INTO dart_filing_receipt_raw VALUES ('00126380','20260310000001','사업보고서')"
    )
    _register(con)

    rows = con.execute(
        f"SELECT rcept_no, captured_vintage_status FROM {SMVF_TABLE} "
        f"WHERE metric_code = 'revenue' ORDER BY rcept_no"
    ).fetchall()
    status_by_rcept = dict(rows)
    assert status_by_rcept["20260310000001"] == "original_confirmed_revisions_partial"
    assert status_by_rcept["20260405000002"] == "unlinked_receipt"


def test_same_day_filings_collapse_to_latest_rcept_no() -> None:
    con = _base_con()
    con.execute(
        "INSERT INTO dart_share_count_raw VALUES "
        "('00126380','005930',2025,'11011','20260310000001','합계',5000,100,DATE '2025-12-31'),"
        "('00126380','005930',2025,'11011','20260310000009','합계',5000,100,DATE '2025-12-31')"
    )
    _register(con)

    rows = con.execute(
        f"SELECT rcept_no, same_day_effective_rcept_no FROM {SMVF_TABLE} "
        f"WHERE metric_code = 'issued_shares' ORDER BY rcept_no"
    ).fetchall()
    assert len(rows) == 2
    assert all(effective == "20260310000009" for _rcept, effective in rows)


def test_period_end_conflict_is_flagged_and_xbrl_wins() -> None:
    con = _base_con()
    con.execute(
        "INSERT INTO dart_financial_statement_raw "
        "(corp_code, ticker, bsns_year, reprt_code, fs_div, sj_div, account_id, "
        "account_nm, ord, thstrm_amount, currency, rcept_no) VALUES "
        "('00126380','005930',2025,'11011','CFS','IS','ifrs-full_Revenue','매출액',1,"
        "1000.0000,'KRW','20260310000001')"
    )
    # XBRL says period end 2025-12-31; share_count's stlm_dt disagrees (2025-12-30).
    con.execute(
        "INSERT INTO dart_xbrl_fact_raw VALUES "
        "('00126380','005930',2025,'11011','20260310000001','ifrs-full_Revenue','매출액',"
        "'ctx1','duration',DATE '2025-01-01',DATE '2025-12-31',NULL,'[]',1000.0000,'매출액')"
    )
    con.execute(
        "INSERT INTO dart_share_count_raw VALUES "
        "('00126380','005930',2025,'11011','20260310000001','합계',5000,100,DATE '2025-12-30')"
    )
    _register(con)

    row = con.execute(
        f"SELECT period_end_source, period_end_conflict, statement_period_end "
        f"FROM {SMVF_TABLE} WHERE metric_code = 'revenue'"
    ).fetchone()
    assert row[0] == "xbrl"
    assert row[1] is True
    assert row[2] == date(2025, 12, 31)


def test_cfs_and_ofs_are_preserved_as_distinct_rows() -> None:
    con = _base_con()
    con.execute(
        "INSERT INTO dart_financial_statement_raw "
        "(corp_code, ticker, bsns_year, reprt_code, fs_div, sj_div, account_id, "
        "account_nm, ord, thstrm_amount, currency, rcept_no) VALUES "
        "('00126380','005930',2025,'11011','CFS','IS','ifrs-full_Revenue','매출액',1,"
        "1000.0000,'KRW','20260310000001'),"
        "('00126380','005930',2025,'11011','OFS','IS','ifrs-full_Revenue','매출액',1,"
        "900.0000,'KRW','20260310000001')"
    )
    _register(con)

    rows = con.execute(
        f"SELECT fs_basis, value_numeric FROM {SMVF_TABLE} "
        f"WHERE metric_code = 'revenue' ORDER BY fs_basis"
    ).fetchall()
    assert rows == [("CFS", 1000.0), ("OFS", 900.0)]


def test_xbrl_value_mismatch_is_flagged_not_silently_accepted() -> None:
    con = _base_con()
    con.execute(
        "INSERT INTO dart_financial_statement_raw "
        "(corp_code, ticker, bsns_year, reprt_code, fs_div, sj_div, account_id, "
        "account_nm, ord, thstrm_amount, currency, rcept_no) VALUES "
        "('00126380','005930',2025,'11011','CFS','IS','ifrs-full_Revenue','매출액',1,"
        "1000.0000,'KRW','20260310000001')"
    )
    con.execute(
        "INSERT INTO dart_xbrl_fact_raw VALUES "
        "('00126380','005930',2025,'11011','20260310000001','ifrs-full_Revenue','매출액',"
        "'ctx1','duration',DATE '2025-01-01',DATE '2025-12-31',NULL,'[]',999.0000,'매출액')"
    )
    _register(con)

    row = con.execute(
        f"SELECT receipt_value_pairing_status, pairing_tolerance FROM {SMVF_TABLE} "
        f"WHERE metric_code = 'revenue'"
    ).fetchone()
    assert row[0] == "value_mismatch"
    assert row[1] == 0.0


def test_no_is_active_dependency_on_corp_master() -> None:
    """corp_view is only consulted for ticker/market/corp_code — passing a
    corp master without an is_active column at all must not raise (§3.4)."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE dart_corp_master (ticker VARCHAR, market VARCHAR, corp_code VARCHAR)")
    con.execute(
        "CREATE TABLE dart_financial_statement_raw ("
        "corp_code VARCHAR, ticker VARCHAR, bsns_year INTEGER, reprt_code VARCHAR, "
        "fs_div VARCHAR, sj_div VARCHAR, account_id VARCHAR, account_nm VARCHAR, "
        "ord INTEGER, thstrm_amount DECIMAL(30,4), currency VARCHAR, rcept_no VARCHAR, "
        "thstrm_add_amount DECIMAL(30,4), frmtrm_q_amount DECIMAL(30,4))"
    )
    con.execute(
        "CREATE TABLE dart_share_count_raw ("
        "corp_code VARCHAR, ticker VARCHAR, bsns_year INTEGER, reprt_code VARCHAR, "
        "rcept_no VARCHAR, se VARCHAR, istc_totqy BIGINT, tesstk_co BIGINT, stlm_dt DATE)"
    )
    con.execute(
        "CREATE TABLE dart_shareholder_return_raw ("
        "corp_code VARCHAR, ticker VARCHAR, bsns_year INTEGER, reprt_code VARCHAR, "
        "rcept_no VARCHAR, statement_type VARCHAR, row_name VARCHAR, stock_knd VARCHAR, "
        "dim1 VARCHAR, dim2 VARCHAR, dim3 VARCHAR, metric_code VARCHAR, "
        "value_numeric DECIMAL(30,4), stlm_dt DATE)"
    )
    con.execute(
        "CREATE TABLE dart_xbrl_fact_raw ("
        "corp_code VARCHAR, ticker VARCHAR, bsns_year INTEGER, reprt_code VARCHAR, "
        "rcept_no VARCHAR, concept_id VARCHAR, concept_name VARCHAR, context_id VARCHAR, "
        "context_type VARCHAR, period_start DATE, period_end DATE, instant_date DATE, "
        "dimensions VARCHAR, value_numeric DECIMAL(30,4), label_ko VARCHAR)"
    )
    con.execute(
        "CREATE TABLE dart_filing_receipt_raw "
        "(corp_code VARCHAR, rcept_no VARCHAR, report_nm VARCHAR)"
    )
    con.execute("INSERT INTO dart_corp_master VALUES ('005930', 'KOSPI', '00126380')")
    con.execute(
        "INSERT INTO dart_share_count_raw VALUES "
        "('00126380','005930',2025,'11011','20260310000001','합계',5000,100,DATE '2025-12-31')"
    )
    _register(con)
    # istc_totqy and tesstk_co are both non-null on the one raw row, so two
    # metric candidates (issued_shares, treasury_shares) are correctly emitted.
    row_count = con.execute(f"SELECT COUNT(*) FROM {SMVF_TABLE}").fetchone()[0]
    assert row_count == 2
