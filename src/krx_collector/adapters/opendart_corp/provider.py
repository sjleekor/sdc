"""OpenDART corporation-code master adapter."""

from __future__ import annotations

import io
import logging
import zipfile
from datetime import date
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
from xml.etree import ElementTree as ET

from krx_collector.domain.enums import Source
from krx_collector.domain.models import DartCorp, DartCorpCodeResult
from krx_collector.util.retry import retry
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


def parse_opendart_error_message(payload: bytes) -> str:
    """Best-effort extraction of an OpenDART status/message error payload."""
    try:
        root = ET.fromstring(payload.decode("utf-8", errors="ignore"))
    except ET.ParseError:
        return "OpenDART returned a non-zip response that could not be parsed."

    status = (root.findtext(".//status") or "").strip()
    message = (root.findtext(".//message") or "").strip()

    if status or message:
        return f"OpenDART error {status}: {message}".strip()
    return "OpenDART returned a non-zip response without a structured error message."


class OpenDartCorpCodeProvider:
    """Fetch the OpenDART corporation-code master zip file."""

    def __init__(self, api_key: str, timeout_seconds: float = 30.0) -> None:
        self._api_key = api_key.strip()
        self._timeout_seconds = timeout_seconds

    @retry(max_attempts=3, base_delay=1.0, backoff_factor=2.0)
    def _download(self) -> bytes:
        if not self._api_key:
            raise RuntimeError("OPENDART_API_KEY is required for corp code sync.")

        query = urlencode({"crtfc_key": self._api_key})
        url = f"{OPENDART_CORP_CODE_URL}?{query}"
        logger.info("Downloading OpenDART corporation-code master.")

        with urlopen(url, timeout=self._timeout_seconds) as response:
            return response.read()

    def fetch_corp_codes(self) -> DartCorpCodeResult:
        """Download and parse the OpenDART corp-code master."""
        payload = b""
        try:
            payload = self._download()
            records = parse_corp_code_zip_bytes(payload)
            return DartCorpCodeResult(records=records)
        except zipfile.BadZipFile:
            return DartCorpCodeResult(error=parse_opendart_error_message(payload))
        except HTTPError as exc:
            return DartCorpCodeResult(error=f"OpenDART HTTP error: {exc.code} {exc.reason}")
        except URLError as exc:
            return DartCorpCodeResult(error=f"OpenDART network error: {exc.reason}")
        except Exception as exc:
            return DartCorpCodeResult(error=str(exc))
