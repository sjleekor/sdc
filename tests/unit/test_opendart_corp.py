import io
import zipfile
from datetime import date

from krx_collector.adapters.opendart_corp.provider import parse_corp_code_zip_bytes
from krx_collector.domain.enums import ListingStatus, Market, RunStatus, RunType, Source
from krx_collector.domain.models import (
    DartCorp,
    DartCorpCodeResult,
    IngestionRun,
    Stock,
    UpsertResult,
)
from krx_collector.service.sync_dart_corp import sync_dart_corp_master
from krx_collector.util.time import now_kst


def _make_corp_code_zip(xml_text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("CORPCODE.xml", xml_text)
    return buffer.getvalue()


def test_parse_corp_code_zip_bytes() -> None:
    payload = _make_corp_code_zip(
        """
        <result>
          <list>
            <corp_code>00126380</corp_code>
            <corp_name>삼성전자</corp_name>
            <stock_code>005930</stock_code>
            <modify_date>20240401</modify_date>
          </list>
          <list>
            <corp_code>00987654</corp_code>
            <corp_name>비상장예시</corp_name>
            <stock_code></stock_code>
            <modify_date>20240315</modify_date>
          </list>
        </result>
        """
    )

    records = parse_corp_code_zip_bytes(payload)

    assert len(records) == 2
    assert records[0].corp_code == "00126380"
    assert records[0].corp_name == "삼성전자"
    assert records[0].ticker == "005930"
    assert records[0].modify_date == date(2024, 4, 1)
    assert records[0].source == Source.OPENDART
    assert records[1].ticker is None
    assert records[1].modify_date == date(2024, 3, 15)


class MockCorpCodeProvider:
    def fetch_corp_codes(self) -> DartCorpCodeResult:
        fetched_at = now_kst()
        return DartCorpCodeResult(
            records=[
                DartCorp(
                    corp_code="00126380",
                    corp_name="삼성전자",
                    ticker="005930",
                    market=None,
                    stock_name="삼성전자",
                    modify_date=date(2024, 4, 1),
                    is_active=False,
                    source=Source.OPENDART,
                    fetched_at=fetched_at,
                ),
                DartCorp(
                    corp_code="00999999",
                    corp_name="상장폐지예시",
                    ticker="999999",
                    market=None,
                    stock_name="상장폐지예시",
                    modify_date=date(2024, 4, 1),
                    is_active=False,
                    source=Source.OPENDART,
                    fetched_at=fetched_at,
                ),
            ]
        )


class MockStorage:
    def __init__(self) -> None:
        self.runs: list[IngestionRun] = []
        self.saved_records: list[DartCorp] = []

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)

    def get_active_stocks(self, market: Market | None = None) -> list[Stock]:
        stocks = [
            Stock(
                ticker="005930",
                market=Market.KOSPI,
                name="삼성전자",
                status=ListingStatus.ACTIVE,
                last_seen_date=date(2024, 4, 19),
                source=Source.FDR,
            ),
            Stock(
                ticker="000660",
                market=Market.KOSPI,
                name="SK하이닉스",
                status=ListingStatus.ACTIVE,
                last_seen_date=date(2024, 4, 19),
                source=Source.FDR,
            ),
        ]
        if market is None:
            return stocks
        return [stock for stock in stocks if stock.market == market]

    def upsert_dart_corp_master(self, records: list[DartCorp]) -> UpsertResult:
        self.saved_records = records
        return UpsertResult(updated=len(records))


def test_sync_dart_corp_master_matches_active_tickers_and_reports_gaps() -> None:
    storage = MockStorage()
    result = sync_dart_corp_master(provider=MockCorpCodeProvider(), storage=storage)

    assert result.error is None
    assert result.total_records == 2
    assert result.matched_active_tickers == 1
    assert result.unmatched_active_tickers == ["000660"]
    assert result.unmatched_dart_tickers == ["999999"]

    matched_record = next(record for record in storage.saved_records if record.ticker == "005930")
    assert matched_record.market == Market.KOSPI
    assert matched_record.stock_name == "삼성전자"
    assert matched_record.is_active is True

    assert storage.runs[0].run_type == RunType.DART_CORP_SYNC
    assert storage.runs[-1].status == RunStatus.SUCCESS
