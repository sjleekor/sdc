"""OpenDART share-count / dividend / treasury-stock adapter."""

from __future__ import annotations

import json
import logging
from datetime import date
from decimal import Decimal, InvalidOperation
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from krx_collector.domain.enums import Source
from krx_collector.domain.models import (
    DartCorp,
    DartShareCountLine,
    DartShareCountResult,
    DartShareholderReturnLine,
    DartShareholderReturnResult,
)
from krx_collector.util.retry import retry
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)

OPENDART_STOCK_COUNT_URL = "https://opendart.fss.or.kr/api/stockTotqySttus.json"
OPENDART_DIVIDEND_URL = "https://opendart.fss.or.kr/api/alotMatter.json"
OPENDART_TREASURY_STOCK_URL = "https://opendart.fss.or.kr/api/tesstkAcqsDspsSttus.json"
OPENDART_OK_STATUS = "000"
OPENDART_NO_DATA_STATUS = "013"

DIVIDEND_METRICS = {
    "thstrm": "당기",
    "frmtrm": "전기",
    "lwfr": "전전기",
}

TREASURY_STOCK_METRICS = {
    "bsis_qy": "기초수량",
    "change_qy_acqs": "취득변동수량",
    "change_qy_dsps": "처분변동수량",
    "change_qy_incnr": "소각변동수량",
    "trmend_qy": "기말수량",
}


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


def _parse_date(value: object) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "-":
        return None
    return date.fromisoformat(text)


def _extract_status(payload: dict[str, object]) -> tuple[str, str]:
    return str(payload.get("status", "")).strip(), str(payload.get("message", "")).strip()


def parse_stock_count_response(
    payload: dict[str, object],
    corp: DartCorp,
    bsns_year: int,
    reprt_code: str,
) -> DartShareCountResult:
    status, message = _extract_status(payload)
    if status == OPENDART_NO_DATA_STATUS:
        return DartShareCountResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            no_data=True,
        )
    if status != OPENDART_OK_STATUS:
        return DartShareCountResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            error=f"OpenDART error {status}: {message}",
        )

    fetched_at = now_kst()
    records: list[DartShareCountLine] = []
    for row in payload.get("list", []):
        if not isinstance(row, dict):
            continue
        records.append(
            DartShareCountLine(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                rcept_no=str(row.get("rcept_no", "")).strip(),
                corp_cls=str(row.get("corp_cls", "")).strip(),
                se=str(row.get("se", "")).strip(),
                isu_stock_totqy=_parse_int(row.get("isu_stock_totqy")),
                now_to_isu_stock_totqy=_parse_int(row.get("now_to_isu_stock_totqy")),
                now_to_dcrs_stock_totqy=_parse_int(row.get("now_to_dcrs_stock_totqy")),
                redc=str(row.get("redc", "")).strip(),
                profit_incnr=str(row.get("profit_incnr", "")).strip(),
                rdmstk_repy=str(row.get("rdmstk_repy", "")).strip(),
                etc=str(row.get("etc", "")).strip(),
                istc_totqy=_parse_int(row.get("istc_totqy")),
                tesstk_co=_parse_int(row.get("tesstk_co")),
                distb_stock_co=_parse_int(row.get("distb_stock_co")),
                stlm_dt=_parse_date(row.get("stlm_dt")),
                source=Source.OPENDART,
                fetched_at=fetched_at,
                raw_payload=dict(row),
            )
        )

    return DartShareCountResult(
        corp_code=corp.corp_code,
        ticker=corp.ticker or "",
        bsns_year=bsns_year,
        reprt_code=reprt_code,
        records=records,
    )


def parse_dividend_response(
    payload: dict[str, object],
    corp: DartCorp,
    bsns_year: int,
    reprt_code: str,
) -> DartShareholderReturnResult:
    status, message = _extract_status(payload)
    if status == OPENDART_NO_DATA_STATUS:
        return DartShareholderReturnResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            statement_type="dividend",
            no_data=True,
        )
    if status != OPENDART_OK_STATUS:
        return DartShareholderReturnResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            statement_type="dividend",
            error=f"OpenDART error {status}: {message}",
        )

    fetched_at = now_kst()
    records: list[DartShareholderReturnLine] = []
    for row in payload.get("list", []):
        if not isinstance(row, dict):
            continue
        row_payload = dict(row)
        row_name = str(row.get("se", "")).strip()
        stock_knd = str(row.get("stock_knd", "")).strip()
        rcept_no = str(row.get("rcept_no", "")).strip()
        stlm_dt = _parse_date(row.get("stlm_dt"))

        for metric_code, metric_name in DIVIDEND_METRICS.items():
            raw_value = row.get(metric_code)
            text_value = "" if raw_value is None else str(raw_value).strip()
            numeric_value = _parse_decimal(raw_value)
            records.append(
                DartShareholderReturnLine(
                    corp_code=corp.corp_code,
                    ticker=corp.ticker or "",
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    statement_type="dividend",
                    row_name=row_name,
                    stock_knd=stock_knd,
                    dim1="",
                    dim2="",
                    dim3="",
                    metric_code=metric_code,
                    metric_name=metric_name,
                    value_numeric=numeric_value,
                    value_text=text_value,
                    unit="",
                    rcept_no=rcept_no,
                    stlm_dt=stlm_dt,
                    source=Source.OPENDART,
                    fetched_at=fetched_at,
                    raw_payload=row_payload,
                )
            )

    return DartShareholderReturnResult(
        corp_code=corp.corp_code,
        ticker=corp.ticker or "",
        bsns_year=bsns_year,
        reprt_code=reprt_code,
        statement_type="dividend",
        records=records,
    )


def parse_treasury_stock_response(
    payload: dict[str, object],
    corp: DartCorp,
    bsns_year: int,
    reprt_code: str,
) -> DartShareholderReturnResult:
    status, message = _extract_status(payload)
    if status == OPENDART_NO_DATA_STATUS:
        return DartShareholderReturnResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            statement_type="treasury_stock",
            no_data=True,
        )
    if status != OPENDART_OK_STATUS:
        return DartShareholderReturnResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            statement_type="treasury_stock",
            error=f"OpenDART error {status}: {message}",
        )

    fetched_at = now_kst()
    records: list[DartShareholderReturnLine] = []
    for row in payload.get("list", []):
        if not isinstance(row, dict):
            continue
        row_payload = dict(row)
        stock_knd = str(row.get("stock_knd", "")).strip()
        dim1 = str(row.get("acqs_mth1", "")).strip()
        dim2 = str(row.get("acqs_mth2", "")).strip()
        dim3 = str(row.get("acqs_mth3", "")).strip()
        row_name = "자기주식 취득 및 처분 현황"
        rcept_no = str(row.get("rcept_no", "")).strip()
        stlm_dt = _parse_date(row.get("stlm_dt"))

        for metric_code, metric_name in TREASURY_STOCK_METRICS.items():
            raw_value = row.get(metric_code)
            text_value = "" if raw_value is None else str(raw_value).strip()
            numeric_value = _parse_decimal(raw_value)
            records.append(
                DartShareholderReturnLine(
                    corp_code=corp.corp_code,
                    ticker=corp.ticker or "",
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    statement_type="treasury_stock",
                    row_name=row_name,
                    stock_knd=stock_knd,
                    dim1=dim1,
                    dim2=dim2,
                    dim3=dim3,
                    metric_code=metric_code,
                    metric_name=metric_name,
                    value_numeric=numeric_value,
                    value_text=text_value,
                    unit="shares",
                    rcept_no=rcept_no,
                    stlm_dt=stlm_dt,
                    source=Source.OPENDART,
                    fetched_at=fetched_at,
                    raw_payload=row_payload,
                )
            )

    return DartShareholderReturnResult(
        corp_code=corp.corp_code,
        ticker=corp.ticker or "",
        bsns_year=bsns_year,
        reprt_code=reprt_code,
        statement_type="treasury_stock",
        records=records,
    )


class OpenDartShareInfoProvider:
    """Fetch share-count and shareholder-return disclosures from OpenDART."""

    def __init__(self, api_key: str, timeout_seconds: float = 30.0) -> None:
        self._api_key = api_key.strip()
        self._timeout_seconds = timeout_seconds

    @retry(max_attempts=3, base_delay=1.0, backoff_factor=2.0)
    def _download(
        self,
        endpoint_url: str,
        corp_code: str,
        bsns_year: int,
        reprt_code: str,
    ) -> bytes:
        if not self._api_key:
            raise RuntimeError("OPENDART_API_KEY is required for share info sync.")

        query = urlencode(
            {
                "crtfc_key": self._api_key,
                "corp_code": corp_code,
                "bsns_year": str(bsns_year),
                "reprt_code": reprt_code,
            }
        )
        url = f"{endpoint_url}?{query}"
        logger.info(
            "Downloading OpenDART share info: endpoint=%s corp_code=%s year=%s reprt=%s",
            endpoint_url.rsplit("/", 1)[-1],
            corp_code,
            bsns_year,
            reprt_code,
        )
        with urlopen(url, timeout=self._timeout_seconds) as response:
            return response.read()

    def _fetch_json(
        self,
        endpoint_url: str,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> dict[str, object]:
        payload_bytes = self._download(endpoint_url, corp.corp_code, bsns_year, reprt_code)
        payload = json.loads(payload_bytes.decode("utf-8"))
        if not isinstance(payload, dict):
            raise RuntimeError("OpenDART returned an unexpected JSON payload.")
        return payload

    def fetch_share_count(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareCountResult:
        try:
            payload = self._fetch_json(OPENDART_STOCK_COUNT_URL, corp, bsns_year, reprt_code)
            return parse_stock_count_response(payload, corp, bsns_year, reprt_code)
        except HTTPError as exc:
            return DartShareCountResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                error=f"OpenDART HTTP error: {exc.code} {exc.reason}",
            )
        except URLError as exc:
            return DartShareCountResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                error=f"OpenDART network error: {exc.reason}",
            )
        except Exception as exc:
            return DartShareCountResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                error=str(exc),
            )

    def fetch_dividend(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareholderReturnResult:
        try:
            payload = self._fetch_json(OPENDART_DIVIDEND_URL, corp, bsns_year, reprt_code)
            return parse_dividend_response(payload, corp, bsns_year, reprt_code)
        except HTTPError as exc:
            return DartShareholderReturnResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                statement_type="dividend",
                error=f"OpenDART HTTP error: {exc.code} {exc.reason}",
            )
        except URLError as exc:
            return DartShareholderReturnResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                statement_type="dividend",
                error=f"OpenDART network error: {exc.reason}",
            )
        except Exception as exc:
            return DartShareholderReturnResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                statement_type="dividend",
                error=str(exc),
            )

    def fetch_treasury_stock(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareholderReturnResult:
        try:
            payload = self._fetch_json(OPENDART_TREASURY_STOCK_URL, corp, bsns_year, reprt_code)
            return parse_treasury_stock_response(payload, corp, bsns_year, reprt_code)
        except HTTPError as exc:
            return DartShareholderReturnResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                statement_type="treasury_stock",
                error=f"OpenDART HTTP error: {exc.code} {exc.reason}",
            )
        except URLError as exc:
            return DartShareholderReturnResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                statement_type="treasury_stock",
                error=f"OpenDART network error: {exc.reason}",
            )
        except Exception as exc:
            return DartShareholderReturnResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                statement_type="treasury_stock",
                error=str(exc),
            )
