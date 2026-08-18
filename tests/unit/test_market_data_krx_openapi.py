"""KRX Open API client and providers (K-4).

Fixtures are the live 2026-08-18 responses, not invented shapes, so a field
rename upstream fails here instead of turning into a column of NULLs that
reads as sparse upstream data.

The tests that matter most are the ones pinning behaviour that differs from
the pykrx path this replaces: a non-trading day is zero rows rather than
zero-filled rows, and a 401 means two different things depending on the body.
"""

from __future__ import annotations

from datetime import date

import pytest

from krx_collector.adapters.market_data_krx_openapi.client import (
    KrxOpenApiClient,
    KrxOpenApiEndpointNotApprovedError,
)
from krx_collector.adapters.market_data_krx_openapi.provider import (
    KrxOpenApiHistoricalUniverseProvider,
    KrxOpenApiMarketCapProvider,
    parse_int,
)
from krx_collector.domain.enums import Market, Source
from krx_collector.util.pipeline import SourceAuthError, SourceQuotaExhaustedError

# Verbatim from sto/stk_bydd_trd, basDd=20260814.
KOSPI_ROW = {
    "BAS_DD": "20260814",
    "ISU_CD": "095570",
    "ISU_NM": "AJ네트웍스",
    "MKT_NM": "KOSPI",
    "SECT_TP_NM": "",
    "TDD_CLSPRC": "4520",
    "CMPPREVDD_PRC": "100",
    "FLUC_RT": "2.26",
    "TDD_OPNPRC": "4435",
    "TDD_HGPRC": "4545",
    "TDD_LWPRC": "4435",
    "ACC_TRDVOL": "74511",
    "ACC_TRDVAL": "334258460",
    "MKTCAP": "204542470680",
    "LIST_SHRS": "45252759",
}

TRADE_DATE = date(2026, 8, 14)


class _FakeResponse:
    def __init__(self, status_code: int, payload: object) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self) -> object:
        if isinstance(self._payload, Exception):
            raise ValueError("not json")
        return self._payload


class _FakeSession:
    """Returns queued responses and records the key each request carried."""

    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, dict[str, str], str]] = []

    def get(self, url, headers=None, params=None, timeout=None):  # noqa: ANN001
        self.calls.append((url, dict(params or {}), (headers or {}).get("AUTH_KEY", "")))
        if not self._responses:
            raise AssertionError(f"unexpected extra request to {url}")
        return self._responses.pop(0)


def _client(responses: list[_FakeResponse], keys: tuple[str, ...] = ("key-a",)) -> KrxOpenApiClient:
    return KrxOpenApiClient(
        keys,
        session=_FakeSession(responses),
        requests_per_second=0,
        sleep_fn=lambda _s: None,
    )


def _ok(rows: list[dict[str, str]]) -> _FakeResponse:
    return _FakeResponse(200, {"OutBlock_1": rows})


# --------------------------------------------------------------------------
# client
# --------------------------------------------------------------------------


def test_a_missing_key_is_rejected_at_construction() -> None:
    # Failing here beats failing 6,000 requests into a backfill.
    with pytest.raises(ValueError, match="AUTH_KEYS"):
        KrxOpenApiClient([])
    with pytest.raises(ValueError, match="AUTH_KEYS"):
        KrxOpenApiClient(["", "   "])


def test_the_auth_key_goes_in_a_header_and_the_date_in_the_query() -> None:
    client = _client([_ok([KOSPI_ROW])])

    client.fetch_rows("sto", "stk_bydd_trd", {"basDd": "20260814"})

    url, params, key = client._session.calls[0]  # type: ignore[attr-defined]
    assert url.endswith("/sto/stk_bydd_trd")
    assert params == {"basDd": "20260814"}
    assert key == "key-a"


def test_an_unapproved_endpoint_is_not_reported_as_an_auth_failure() -> None:
    # Measured 2026-08-18: a valid key on an endpoint with no 이용 신청 returns
    # 401 "Unauthorized API Call", while a bad key returns "Unauthorized Key".
    # Collapsing the two sends an operator hunting for a key problem that does
    # not exist; the fix is a portal application.
    client = _client([_FakeResponse(401, {"respMsg": "Unauthorized API Call", "respCode": "401"})])

    with pytest.raises(KrxOpenApiEndpointNotApprovedError, match="이용 신청"):
        client.fetch_rows("sto", "stk_bydd_trd", {"basDd": "20260814"})


def test_a_rejected_key_is_an_auth_failure() -> None:
    client = _client([_FakeResponse(401, {"respMsg": "Unauthorized Key", "respCode": "401"})])

    with pytest.raises(SourceAuthError):
        client.fetch_rows("sto", "stk_bydd_trd", {"basDd": "20260814"})


def test_quota_exhaustion_rotates_to_the_next_key_before_giving_up() -> None:
    client = _client(
        [
            _FakeResponse(429, {"respMsg": "Limit Exceeded"}),
            _ok([KOSPI_ROW]),
        ],
        keys=("key-a", "key-b"),
    )

    rows = client.fetch_rows("sto", "stk_bydd_trd", {"basDd": "20260814"})

    assert len(rows) == 1
    assert client.counters.key_rotations == 1
    assert [key for _, _, key in client._session.calls] == ["key-a", "key-b"]  # type: ignore[attr-defined]


def test_quota_exhaustion_on_every_key_ends_the_run_cleanly() -> None:
    # KRX does not say whether the 10,000/day budget is per key or per account,
    # so rotation is best effort and running out has to be a clean stop rather
    # than an error per remaining slice.
    client = _client(
        [
            _FakeResponse(429, {"respMsg": "Limit Exceeded"}),
            _FakeResponse(429, {"respMsg": "Limit Exceeded"}),
        ],
        keys=("key-a", "key-b"),
    )

    with pytest.raises(SourceQuotaExhaustedError):
        client.fetch_rows("sto", "stk_bydd_trd", {"basDd": "20260814"})


def test_a_server_error_is_retried_and_counted() -> None:
    client = _client([_FakeResponse(503, {"respMsg": "busy"}), _ok([KOSPI_ROW])])

    rows = client.fetch_rows("sto", "stk_bydd_trd", {"basDd": "20260814"})

    assert len(rows) == 1
    assert client.counters.http_requests == 2
    assert client.counters.http_retries == 1
    assert client.counters.http_errors == 1


def test_counters_are_real_http_counts_not_logical_ones() -> None:
    client = _client([_ok([KOSPI_ROW]), _ok([KOSPI_ROW])])

    client.fetch_rows("sto", "stk_bydd_trd", {"basDd": "20260814"})
    client.fetch_rows("sto", "ksq_bydd_trd", {"basDd": "20260814"})

    assert client.counters.http_requests == 2
    assert client.counters.as_dict()["http_requests"] == 2
    # Zero-valued counters are dropped so ingestion_runs.counts stays readable.
    assert "key_rotations" not in client.counters.as_dict()


# --------------------------------------------------------------------------
# parsing
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("4520", 4520),
        ("204,542,470,680", 204542470680),
        ("", None),
        ("-", None),
        (None, None),
        ("abc", None),
        ("2.26", 2),
    ],
)
def test_blank_values_parse_to_none_never_zero(raw: object, expected: int | None) -> None:
    # A zero that cannot be told apart from a real zero is the bug the pykrx
    # adapter has to work around; this path refuses to create one.
    assert parse_int(raw) == expected


# --------------------------------------------------------------------------
# market-cap provider
# --------------------------------------------------------------------------


def test_the_live_response_shape_maps_onto_the_row() -> None:
    provider = KrxOpenApiMarketCapProvider(_client([_ok([KOSPI_ROW])]))

    result = provider.fetch_by_date(TRADE_DATE, Market.KOSPI)

    assert result.error is None
    assert result.response_rows == 1
    (row,) = result.rows
    assert row.ticker == "095570"
    assert row.market is Market.KOSPI
    assert row.trade_date == TRADE_DATE
    assert row.source_close == 4520
    assert row.market_cap == 204542470680
    assert row.trading_value == 334258460
    assert row.listed_shares == 45252759
    assert row.volume == 74511
    assert row.source is Source.KRX_OPENAPI


def test_the_unadjusted_ohlc_comes_along_for_free() -> None:
    # This is what makes K-7 cost nothing extra: the adjustment factor becomes
    # computable here instead of inherited from naver's retroactive rewrites.
    provider = KrxOpenApiMarketCapProvider(_client([_ok([KOSPI_ROW])]))

    (row,) = provider.fetch_by_date(TRADE_DATE, Market.KOSPI).rows

    assert (row.source_open, row.source_high, row.source_low) == (4435, 4545, 4435)


def test_a_non_trading_day_is_zero_rows_not_zero_filled_rows() -> None:
    # The opposite of pykrx, which returns every ticker with the price columns
    # zeroed and needs alternative=False plus a "close == 0 is not real" rule.
    # Absence is expressed as absence here, so neither guard applies.
    provider = KrxOpenApiMarketCapProvider(_client([_ok([])]))

    result = provider.fetch_by_date(date(2026, 8, 17), Market.KOSPI)

    assert result.rows == []
    assert result.response_rows == 0
    assert result.error is None


def test_a_renamed_field_fails_loudly_instead_of_nulling_a_column() -> None:
    renamed = {**KOSPI_ROW}
    renamed["MKT_CAP"] = renamed.pop("MKTCAP")
    provider = KrxOpenApiMarketCapProvider(_client([_ok([renamed])]))

    result = provider.fetch_by_date(TRADE_DATE, Market.KOSPI)

    assert result.rows == []
    assert result.error is not None
    assert "MKTCAP" in result.error


def test_market_comes_from_the_call_argument_and_picks_the_endpoint() -> None:
    # MKT_NM exists in the response, but the endpoint is already market-
    # specific, so the argument is authoritative — and it is what keeps a
    # stock_master join (which would leak today's market backwards) out.
    client = _client([_ok([{**KOSPI_ROW, "MKT_NM": "KOSPI"}])])
    provider = KrxOpenApiMarketCapProvider(client)

    (row,) = provider.fetch_by_date(TRADE_DATE, Market.KOSDAQ).rows

    assert row.market is Market.KOSDAQ
    url, _, _ = client._session.calls[0]  # type: ignore[attr-defined]
    assert url.endswith("/sto/ksq_bydd_trd")


def test_an_upstream_failure_becomes_an_error_not_an_exception() -> None:
    provider = KrxOpenApiMarketCapProvider(
        _client([_FakeResponse(401, {"respMsg": "Unauthorized Key"})])
    )

    result = provider.fetch_by_date(TRADE_DATE, Market.KOSPI)

    assert result.rows == []
    assert result.error is not None


# --------------------------------------------------------------------------
# historical universe provider
# --------------------------------------------------------------------------


def test_the_universe_snapshot_reuses_the_market_cap_call() -> None:
    # One request per market, versus two on the pykrx path, because ISU_NM
    # rides along with the trade data.
    client = _client([_ok([KOSPI_ROW])])
    provider = KrxOpenApiHistoricalUniverseProvider(client)

    result = provider.fetch_universe([Market.KOSPI], as_of=TRADE_DATE)

    assert result.error is None
    assert result.snapshot is not None
    assert result.snapshot.as_of_date == TRADE_DATE
    (record,) = result.snapshot.records
    assert record.ticker == "095570"
    assert record.name == "AJ네트웍스"
    assert len(client._session.calls) == 1  # type: ignore[attr-defined]


def test_snapshots_are_tagged_as_backfill_so_the_live_diff_ignores_them() -> None:
    provider = KrxOpenApiHistoricalUniverseProvider(_client([_ok([KOSPI_ROW])]))

    result = provider.fetch_universe([Market.KOSPI], as_of=TRADE_DATE)

    assert result.snapshot is not None
    assert result.snapshot.source is Source.KRX_OPENAPI_BACKFILL
    assert all(r.source is Source.KRX_OPENAPI_BACKFILL for r in result.snapshot.records)


def test_an_empty_universe_is_an_error_not_a_market_wide_delisting() -> None:
    provider = KrxOpenApiHistoricalUniverseProvider(_client([_ok([])]))

    result = provider.fetch_universe([Market.KOSPI], as_of=TRADE_DATE)

    assert result.snapshot is None
    assert result.error is not None
    assert "refusing to record an empty universe" in result.error
