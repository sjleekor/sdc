"""OpenDART single-company full financial-statement adapter."""

from __future__ import annotations

import json
import logging
from decimal import Decimal, InvalidOperation
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from krx_collector.domain.enums import Source
from krx_collector.domain.models import (
    DartCorp,
    DartFinancialStatementLine,
    DartFinancialStatementResult,
)
from krx_collector.util.retry import retry
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)

OPENDART_FINANCIAL_STATEMENT_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
OPENDART_NO_DATA_STATUS = "013"
OPENDART_OK_STATUS = "000"


def _parse_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "-":
        return None
    normalized = text.replace(",", "")
    try:
        return Decimal(normalized)
    except InvalidOperation:
        return None


def _parse_int(value: object) -> int | None:
    decimal_value = _parse_decimal(value)
    if decimal_value is None:
        return None
    return int(decimal_value)


def parse_fnltt_singl_acnt_all_response(
    payload: dict[str, object],
    corp: DartCorp,
    bsns_year: int,
    reprt_code: str,
    fs_div: str,
) -> DartFinancialStatementResult:
    """Parse OpenDART financial-statement JSON into raw rows."""
    status = str(payload.get("status", "")).strip()
    message = str(payload.get("message", "")).strip()

    if status == OPENDART_NO_DATA_STATUS:
        return DartFinancialStatementResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            fs_div=fs_div,
            no_data=True,
        )

    if status != OPENDART_OK_STATUS:
        return DartFinancialStatementResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            fs_div=fs_div,
            error=f"OpenDART error {status}: {message}",
        )

    fetched_at = now_kst()
    records: list[DartFinancialStatementLine] = []

    for row in payload.get("list", []):
        if not isinstance(row, dict):
            continue
        raw_payload = dict(row)
        records.append(
            DartFinancialStatementLine(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                fs_div=fs_div,
                sj_div=str(row.get("sj_div", "")).strip(),
                sj_nm=str(row.get("sj_nm", "")).strip(),
                account_id=str(row.get("account_id", "")).strip(),
                account_nm=str(row.get("account_nm", "")).strip(),
                account_detail=str(row.get("account_detail", "")).strip(),
                thstrm_nm=str(row.get("thstrm_nm", "")).strip(),
                thstrm_amount=_parse_decimal(row.get("thstrm_amount")),
                thstrm_add_amount=_parse_decimal(row.get("thstrm_add_amount")),
                frmtrm_nm=str(row.get("frmtrm_nm", "")).strip(),
                frmtrm_amount=_parse_decimal(row.get("frmtrm_amount")),
                frmtrm_q_nm=str(row.get("frmtrm_q_nm", "")).strip(),
                frmtrm_q_amount=_parse_decimal(row.get("frmtrm_q_amount")),
                frmtrm_add_amount=_parse_decimal(row.get("frmtrm_add_amount")),
                bfefrmtrm_nm=str(row.get("bfefrmtrm_nm", "")).strip(),
                bfefrmtrm_amount=_parse_decimal(row.get("bfefrmtrm_amount")),
                ord=_parse_int(row.get("ord")) or 0,
                currency=str(row.get("currency", "")).strip(),
                rcept_no=str(row.get("rcept_no", "")).strip(),
                source=Source.OPENDART,
                fetched_at=fetched_at,
                raw_payload=raw_payload,
            )
        )

    return DartFinancialStatementResult(
        corp_code=corp.corp_code,
        ticker=corp.ticker or "",
        bsns_year=bsns_year,
        reprt_code=reprt_code,
        fs_div=fs_div,
        records=records,
    )


class OpenDartFinancialStatementProvider:
    """Fetch single-company full financial statements from OpenDART."""

    def __init__(self, api_key: str, timeout_seconds: float = 30.0) -> None:
        self._api_key = api_key.strip()
        self._timeout_seconds = timeout_seconds

    @retry(max_attempts=3, base_delay=1.0, backoff_factor=2.0)
    def _download(self, corp_code: str, bsns_year: int, reprt_code: str, fs_div: str) -> bytes:
        if not self._api_key:
            raise RuntimeError("OPENDART_API_KEY is required for financial sync.")

        query = urlencode(
            {
                "crtfc_key": self._api_key,
                "corp_code": corp_code,
                "bsns_year": str(bsns_year),
                "reprt_code": reprt_code,
                "fs_div": fs_div,
            }
        )
        url = f"{OPENDART_FINANCIAL_STATEMENT_URL}?{query}"
        logger.info(
            "Downloading OpenDART financial statement: corp_code=%s year=%s reprt=%s fs_div=%s",
            corp_code,
            bsns_year,
            reprt_code,
            fs_div,
        )

        with urlopen(url, timeout=self._timeout_seconds) as response:
            return response.read()

    def fetch_financial_statement(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
        fs_div: str,
    ) -> DartFinancialStatementResult:
        payload_bytes = b""
        try:
            payload_bytes = self._download(corp.corp_code, bsns_year, reprt_code, fs_div)
            payload = json.loads(payload_bytes.decode("utf-8"))
            if not isinstance(payload, dict):
                raise RuntimeError("OpenDART returned an unexpected JSON payload.")
            return parse_fnltt_singl_acnt_all_response(payload, corp, bsns_year, reprt_code, fs_div)
        except HTTPError as exc:
            return DartFinancialStatementResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                fs_div=fs_div,
                error=f"OpenDART HTTP error: {exc.code} {exc.reason}",
            )
        except URLError as exc:
            return DartFinancialStatementResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                fs_div=fs_div,
                error=f"OpenDART network error: {exc.reason}",
            )
        except json.JSONDecodeError:
            return DartFinancialStatementResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                fs_div=fs_div,
                error=f"OpenDART returned invalid JSON: {payload_bytes[:200]!r}",
            )
        except Exception as exc:
            return DartFinancialStatementResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                fs_div=fs_div,
                error=str(exc),
            )
