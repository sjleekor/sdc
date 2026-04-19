from datetime import date
from decimal import Decimal

from krx_collector.adapters.opendart_financials.provider import (
    parse_fnltt_singl_acnt_all_response,
)
from krx_collector.domain.enums import Market, RunStatus, RunType, Source
from krx_collector.domain.models import (
    DartCorp,
    DartFinancialStatementLine,
    DartFinancialStatementResult,
    IngestionRun,
    UpsertResult,
)
from krx_collector.service.sync_dart_financials import sync_dart_financial_statements
from krx_collector.util.time import now_kst


def _sample_corp() -> DartCorp:
    return DartCorp(
        corp_code="00126380",
        corp_name="삼성전자",
        ticker="005930",
        market=Market.KOSPI,
        stock_name="삼성전자",
        modify_date=date(2024, 4, 1),
        is_active=True,
        source=Source.OPENDART,
        fetched_at=now_kst(),
    )


def test_parse_fnltt_singl_acnt_all_response_success() -> None:
    corp = _sample_corp()
    payload = {
        "status": "000",
        "message": "정상",
        "list": [
            {
                "rcept_no": "20240312000123",
                "sj_div": "BS",
                "sj_nm": "재무상태표",
                "account_id": "ifrs-full_Assets",
                "account_nm": "자산총계",
                "account_detail": "",
                "thstrm_nm": "제 56 기말",
                "thstrm_amount": "455,905,980,000,000",
                "thstrm_add_amount": "",
                "frmtrm_nm": "제 55 기말",
                "frmtrm_amount": "448,424,507,000,000",
                "frmtrm_q_nm": "",
                "frmtrm_q_amount": "",
                "frmtrm_add_amount": "",
                "bfefrmtrm_nm": "제 54 기말",
                "bfefrmtrm_amount": "426,621,158,000,000",
                "ord": "1",
                "currency": "KRW",
            }
        ],
    }

    result = parse_fnltt_singl_acnt_all_response(payload, corp, 2025, "11011", "CFS")

    assert result.error is None
    assert result.no_data is False
    assert len(result.records) == 1
    line = result.records[0]
    assert line.ticker == "005930"
    assert line.sj_div == "BS"
    assert line.account_id == "ifrs-full_Assets"
    assert line.thstrm_amount == Decimal("455905980000000")
    assert line.frmtrm_amount == Decimal("448424507000000")
    assert line.bfefrmtrm_amount == Decimal("426621158000000")
    assert line.ord == 1


def test_parse_fnltt_singl_acnt_all_response_no_data() -> None:
    corp = _sample_corp()
    payload = {"status": "013", "message": "조회된 데이타가 없습니다."}

    result = parse_fnltt_singl_acnt_all_response(payload, corp, 2025, "11011", "CFS")

    assert result.no_data is True
    assert result.error is None
    assert result.records == []


class MockFinancialProvider:
    def fetch_financial_statement(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
        fs_div: str,
    ) -> DartFinancialStatementResult:
        if fs_div == "OFS":
            return DartFinancialStatementResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                fs_div=fs_div,
                no_data=True,
            )
        return DartFinancialStatementResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            fs_div=fs_div,
            records=[
                DartFinancialStatementLine(
                    corp_code=corp.corp_code,
                    ticker=corp.ticker or "",
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    fs_div=fs_div,
                    sj_div="IS",
                    sj_nm="손익계산서",
                    account_id="ifrs-full_Revenue",
                    account_nm="매출액",
                    account_detail="",
                    thstrm_nm="제 56 기",
                    thstrm_amount=Decimal("300"),
                    thstrm_add_amount=Decimal("1200"),
                    frmtrm_nm="제 55 기",
                    frmtrm_amount=Decimal("250"),
                    frmtrm_q_nm="",
                    frmtrm_q_amount=None,
                    frmtrm_add_amount=Decimal("1000"),
                    bfefrmtrm_nm="제 54 기",
                    bfefrmtrm_amount=Decimal("900"),
                    ord=1,
                    currency="KRW",
                    rcept_no="20240312000123",
                    source=Source.OPENDART,
                    fetched_at=now_kst(),
                    raw_payload={"account_nm": "매출액"},
                )
            ],
        )


class MockFinancialStorage:
    def __init__(self) -> None:
        self.runs: list[IngestionRun] = []
        self.upserts: list[DartFinancialStatementLine] = []

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)

    def get_dart_corp_master(
        self,
        active_only: bool = True,
        tickers: list[str] | None = None,
    ) -> list[DartCorp]:
        records = [_sample_corp()]
        if tickers is None:
            return records
        return [record for record in records if record.ticker in tickers]

    def upsert_dart_financial_statement_raw(
        self,
        records: list[DartFinancialStatementLine],
    ) -> UpsertResult:
        self.upserts.extend(records)
        return UpsertResult(updated=len(records))


def test_sync_dart_financial_statements_counts_success_and_no_data() -> None:
    storage = MockFinancialStorage()
    result = sync_dart_financial_statements(
        provider=MockFinancialProvider(),
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        fs_divs=["CFS", "OFS"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert result.targets_processed == 1
    assert result.requests_attempted == 2
    assert result.rows_upserted == 1
    assert result.no_data_requests == 1
    assert len(storage.upserts) == 1
    assert storage.runs[0].run_type == RunType.DART_FINANCIAL_SYNC
    assert storage.runs[-1].status == RunStatus.SUCCESS
