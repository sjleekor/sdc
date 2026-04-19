from dataclasses import dataclass
from datetime import date

from krx_collector.domain.enums import Market, RunStatus, RunType, Source
from krx_collector.domain.models import (
    DartCorp,
    DartFinancialStatementLine,
    DartFinancialStatementResult,
    IngestionRun,
    UpsertResult,
)
from krx_collector.service.sync_dart_financials import sync_dart_financial_statements
from krx_collector.util.pipeline import call_with_retry, complete_run
from krx_collector.util.time import now_kst


@dataclass(slots=True)
class RetryProbe:
    error: str | None = None


class RecordingStorage:
    def __init__(self) -> None:
        self.runs: list[IngestionRun] = []

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)


class PartialFinancialProvider:
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
                error="temporary upstream failure",
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
                    thstrm_amount=None,
                    thstrm_add_amount=None,
                    frmtrm_nm="",
                    frmtrm_amount=None,
                    frmtrm_q_nm="",
                    frmtrm_q_amount=None,
                    frmtrm_add_amount=None,
                    bfefrmtrm_nm="",
                    bfefrmtrm_amount=None,
                    ord=1,
                    currency="KRW",
                    rcept_no="20260419000001",
                    source=Source.OPENDART,
                    fetched_at=now_kst(),
                    raw_payload={"account_nm": "매출액"},
                )
            ],
        )


class PartialFinancialStorage(RecordingStorage):
    def __init__(self) -> None:
        super().__init__()
        self.upserts: list[DartFinancialStatementLine] = []

    def get_dart_corp_master(
        self,
        active_only: bool = True,
        tickers: list[str] | None = None,
    ) -> list[DartCorp]:
        corp = DartCorp(
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
        return [corp]

    def upsert_dart_financial_statement_raw(
        self,
        records: list[DartFinancialStatementLine],
    ) -> UpsertResult:
        self.upserts.extend(records)
        return UpsertResult(updated=len(records))


def test_call_with_retry_retries_result_errors_until_success() -> None:
    attempts = {"count": 0}
    sleeps: list[float] = []

    def operation() -> RetryProbe:
        attempts["count"] += 1
        if attempts["count"] < 3:
            return RetryProbe(error="try again")
        return RetryProbe(error=None)

    result = call_with_retry(
        operation,
        request_label="retry-probe",
        sleep_fn=sleeps.append,
    )

    assert result.error is None
    assert attempts["count"] == 3
    assert sleeps == [0.5, 1.0]


def test_complete_run_marks_partial_and_extends_counts() -> None:
    storage = RecordingStorage()
    run = IngestionRun(run_type=RunType.DART_FINANCIAL_SYNC, started_at=now_kst())

    complete_run(
        storage,  # type: ignore[arg-type]
        run,
        counts={"requests_attempted": 5, "rows_upserted": 4},
        errors={"005930:2025:11011:OFS": "temporary upstream failure"},
        partial_subject="financial sync requests",
    )

    assert storage.runs[-1].status == RunStatus.PARTIAL
    assert storage.runs[-1].counts == {
        "requests_attempted": 5,
        "rows_upserted": 4,
        "error_count": 1,
        "partial_failure_count": 1,
        "completed_request_count": 4,
    }
    assert "financial sync requests had errors" in (storage.runs[-1].error_summary or "")


def test_sync_dart_financial_statements_marks_partial_run() -> None:
    storage = PartialFinancialStorage()

    result = sync_dart_financial_statements(
        provider=PartialFinancialProvider(),
        storage=storage,  # type: ignore[arg-type]
        bsns_years=[2025],
        reprt_codes=["11011"],
        fs_divs=["CFS", "OFS"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.requests_attempted == 2
    assert result.rows_upserted == 1
    assert result.errors == {"005930:2025:11011:OFS": "temporary upstream failure"}
    assert storage.runs[-1].status == RunStatus.PARTIAL
    assert storage.runs[-1].counts == {
        "targets_processed": 1,
        "requests_attempted": 2,
        "rows_upserted": 1,
        "no_data_requests": 0,
        "error_count": 1,
        "partial_failure_count": 1,
        "completed_request_count": 1,
    }
