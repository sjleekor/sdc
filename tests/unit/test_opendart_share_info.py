from datetime import date
from decimal import Decimal

from krx_collector.adapters.opendart_share_info.provider import (
    OpenDartShareInfoProvider,
    parse_dividend_response,
    parse_stock_count_response,
    parse_treasury_stock_response,
)
from krx_collector.domain.enums import Market, RunStatus, Source
from krx_collector.domain.models import (
    DartCorp,
    DartShareCountLine,
    DartShareCountResult,
    DartShareholderReturnLine,
    DartShareholderReturnResult,
    IngestionRun,
    UpsertResult,
)
from krx_collector.service.sync_dart_share_info import sync_dart_share_info
from krx_collector.util.time import now_kst
from tests.helpers.fake_opendart_executor import FakeOpenDartExecutor


def _sample_corp() -> DartCorp:
    return DartCorp(
        corp_code="00126380",
        corp_name="삼성전자",
        ticker="005930",
        market=Market.KOSPI,
        stock_name="삼성전자",
        modify_date=date(2026, 3, 10),
        is_active=True,
        source=Source.OPENDART,
        fetched_at=now_kst(),
    )


def test_parse_stock_count_response() -> None:
    corp = _sample_corp()
    payload = {
        "status": "000",
        "message": "정상",
        "list": [
            {
                "rcept_no": "20260310002820",
                "corp_cls": "Y",
                "corp_code": "00126380",
                "corp_name": "삼성전자",
                "se": "보통주",
                "isu_stock_totqy": "20,000,000,000",
                "now_to_isu_stock_totqy": "7,780,466,850",
                "now_to_dcrs_stock_totqy": "1,860,828,928",
                "redc": "-",
                "profit_incnr": "1,860,828,928",
                "rdmstk_repy": "-",
                "etc": "-",
                "istc_totqy": "5,919,637,922",
                "tesstk_co": "91,828,987",
                "distb_stock_co": "5,827,808,935",
                "stlm_dt": "2025-12-31",
            }
        ],
    }

    result = parse_stock_count_response(payload, corp, 2025, "11011")

    assert result.error is None
    assert len(result.records) == 1
    row = result.records[0]
    assert row.se == "보통주"
    assert row.isu_stock_totqy == 20000000000
    assert row.tesstk_co == 91828987
    assert row.stlm_dt == date(2025, 12, 31)


def test_parse_dividend_response() -> None:
    corp = _sample_corp()
    payload = {
        "status": "000",
        "message": "정상",
        "list": [
            {
                "rcept_no": "20260310002820",
                "corp_cls": "Y",
                "corp_code": "00126380",
                "corp_name": "삼성전자",
                "se": "주당 현금배당금(원)",
                "stock_knd": "보통주",
                "thstrm": "1,668",
                "frmtrm": "1,446",
                "lwfr": "1,444",
                "stlm_dt": "2025-12-31",
            }
        ],
    }

    result = parse_dividend_response(payload, corp, 2025, "11011")

    assert result.error is None
    assert len(result.records) == 3
    current = next(row for row in result.records if row.metric_code == "thstrm")
    assert current.row_name == "주당 현금배당금(원)"
    assert current.stock_knd == "보통주"
    assert current.value_numeric == Decimal("1668")


def test_parse_treasury_stock_response() -> None:
    corp = _sample_corp()
    payload = {
        "status": "000",
        "message": "정상",
        "list": [
            {
                "rcept_no": "20260310002820",
                "corp_cls": "Y",
                "corp_code": "00126380",
                "corp_name": "삼성전자",
                "stock_knd": "보통주",
                "acqs_mth1": "총계",
                "acqs_mth2": "총계",
                "acqs_mth3": "총계",
                "bsis_qy": "29,700,000",
                "change_qy_acqs": "118,314,495",
                "change_qy_dsps": "6,040,880",
                "change_qy_incnr": "50,144,628",
                "trmend_qy": "91,828,987",
                "rm": "-",
                "stlm_dt": "2025-12-31",
            }
        ],
    }

    result = parse_treasury_stock_response(payload, corp, 2025, "11011")

    assert result.error is None
    assert len(result.records) == 5
    ending = next(row for row in result.records if row.metric_code == "trmend_qy")
    assert ending.stock_knd == "보통주"
    assert ending.dim1 == "총계"
    assert ending.value_numeric == Decimal("91828987")


def test_open_dart_share_info_provider_maps_no_data_result() -> None:
    corp = _sample_corp()
    provider = OpenDartShareInfoProvider(
        request_executor=FakeOpenDartExecutor(
            [
                '{"status":"013","message":"조회된 데이타가 없습니다."}'.encode("utf-8"),
            ]
        )
    )

    result = provider.fetch_share_count(corp, 2025, "11011")

    assert result.no_data is True
    assert result.status_code == "013"
    assert result.error is None


class MockShareInfoProvider:
    def fetch_share_count(self, corp: DartCorp, bsns_year: int, reprt_code: str) -> DartShareCountResult:
        return DartShareCountResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            records=[
                DartShareCountLine(
                    corp_code=corp.corp_code,
                    ticker=corp.ticker or "",
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    rcept_no="20260310002820",
                    corp_cls="Y",
                    se="보통주",
                    isu_stock_totqy=20000000000,
                    now_to_isu_stock_totqy=7780466850,
                    now_to_dcrs_stock_totqy=1860828928,
                    redc="-",
                    profit_incnr="1860828928",
                    rdmstk_repy="-",
                    etc="-",
                    istc_totqy=5919637922,
                    tesstk_co=91828987,
                    distb_stock_co=5827808935,
                    stlm_dt=date(2025, 12, 31),
                    source=Source.OPENDART,
                    fetched_at=now_kst(),
                    raw_payload={"se": "보통주"},
                )
            ],
        )

    def fetch_dividend(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareholderReturnResult:
        return DartShareholderReturnResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            statement_type="dividend",
            records=[
                DartShareholderReturnLine(
                    corp_code=corp.corp_code,
                    ticker=corp.ticker or "",
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    statement_type="dividend",
                    row_name="주당 현금배당금(원)",
                    stock_knd="보통주",
                    dim1="",
                    dim2="",
                    dim3="",
                    metric_code="thstrm",
                    metric_name="당기",
                    value_numeric=Decimal("1668"),
                    value_text="1,668",
                    unit="",
                    rcept_no="20260310002820",
                    stlm_dt=date(2025, 12, 31),
                    source=Source.OPENDART,
                    fetched_at=now_kst(),
                    raw_payload={"se": "주당 현금배당금(원)"},
                )
            ],
        )

    def fetch_treasury_stock(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareholderReturnResult:
        return DartShareholderReturnResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            statement_type="treasury_stock",
            no_data=True,
        )


class MockShareInfoStorage:
    def __init__(self) -> None:
        self.runs: list[IngestionRun] = []
        self.share_count_rows: list[DartShareCountLine] = []
        self.return_rows: list[DartShareholderReturnLine] = []

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

    def upsert_dart_share_count_raw(self, records: list[DartShareCountLine]) -> UpsertResult:
        self.share_count_rows.extend(records)
        return UpsertResult(updated=len(records))

    def upsert_dart_shareholder_return_raw(
        self,
        records: list[DartShareholderReturnLine],
    ) -> UpsertResult:
        self.return_rows.extend(records)
        return UpsertResult(updated=len(records))


def test_sync_dart_share_info_counts_results() -> None:
    storage = MockShareInfoStorage()
    provider = MockShareInfoProvider()

    result = sync_dart_share_info(
        share_count_provider=provider,
        shareholder_return_provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert result.targets_processed == 1
    assert result.requests_attempted == 3
    assert result.share_count_rows_upserted == 1
    assert result.shareholder_return_rows_upserted == 1
    assert result.no_data_requests == 1
    assert storage.runs[-1].status == RunStatus.SUCCESS
