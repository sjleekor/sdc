"""OpenDART corporation-code master adapter."""

from __future__ import annotations

import io
import logging
import zipfile
from datetime import date
from xml.etree import ElementTree as ET

from krx_collector.adapters.opendart_common import (
    CORP_CODE_POLICY,
    OpenDartCallResult,
    OpenDartRequestExecutor,
    apply_call_result_meta,
)
from krx_collector.domain.enums import Source
from krx_collector.domain.models import DartCorp, DartCorpCodeResult
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)

OPENDART_CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"


def parse_corp_code_zip_bytes(payload: bytes) -> list[DartCorp]:
    """Parse OpenDART corpCode zip bytes into ``DartCorp`` rows."""
    fetched_at = now_kst()

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        xml_names = [name for name in archive.namelist() if name.lower().endswith(".xml")]
        if not xml_names:
            raise ValueError("OpenDART corpCode payload contained no XML file.")

        xml_bytes = archive.read(xml_names[0])

    root = ET.fromstring(xml_bytes)
    records: list[DartCorp] = []

    for item in root.findall(".//list"):
        corp_code = (item.findtext("corp_code") or "").strip()
        corp_name = (item.findtext("corp_name") or "").strip()
        ticker = (item.findtext("stock_code") or "").strip() or None
        modify_date_raw = (item.findtext("modify_date") or "").strip()

        if not corp_code or not corp_name:
            continue

        modify_date = None
        if modify_date_raw:
            modify_date = date.fromisoformat(
                f"{modify_date_raw[:4]}-{modify_date_raw[4:6]}-{modify_date_raw[6:8]}"
            )

        records.append(
            DartCorp(
                corp_code=corp_code,
                corp_name=corp_name,
                ticker=ticker,
                market=None,
                stock_name=corp_name,
                modify_date=modify_date,
                is_active=False,
                source=Source.OPENDART,
                fetched_at=fetched_at,
            )
        )

    return records


class OpenDartCorpCodeProvider:
    """Fetch the OpenDART corporation-code master zip file."""

    def __init__(
        self,
        request_executor: OpenDartRequestExecutor,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._request_executor = request_executor
        self._timeout_seconds = timeout_seconds

    @property
    def request_executor(self) -> OpenDartRequestExecutor:
        """Expose the shared executor for run-level metrics."""
        return self._request_executor

    def _parse_corp_code_payload(self, payload: bytes) -> OpenDartCallResult:
        return CORP_CODE_POLICY.classify_xml_zip_payload(payload)

    def fetch_corp_codes(self) -> DartCorpCodeResult:
        """Download and parse the OpenDART corp-code master."""
        try:
            call_result = self._request_executor.fetch_bytes(
                endpoint_url=OPENDART_CORP_CODE_URL,
                params={},
                request_label="corp_code_master",
                parser=self._parse_corp_code_payload,
                timeout_seconds=self._timeout_seconds,
            )
            if call_result.error:
                return apply_call_result_meta(DartCorpCodeResult(), call_result)

            records = parse_corp_code_zip_bytes(call_result.payload or b"")
            return apply_call_result_meta(DartCorpCodeResult(records=records), call_result)
        except zipfile.BadZipFile:
            return DartCorpCodeResult(
                error=f"OpenDART returned an invalid ZIP payload: {(call_result.payload or b'')[:120]!r}"
            )
        except Exception as exc:
            return DartCorpCodeResult(error=str(exc))
