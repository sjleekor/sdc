"""KRX Open API market-cap and historical-universe providers.

One request returns a whole market-day.  Verified live on 2026-08-18
(``basDd=20260814``): KOSPI 942 rows, KOSDAQ 1,821 rows, 15 fields::

    {"BAS_DD": "20260814", "ISU_CD": "095570", "ISU_NM": "AJ네트웍스",
     "MKT_NM": "KOSPI", "SECT_TP_NM": "", "TDD_CLSPRC": "4520",
     "CMPPREVDD_PRC": "100", "FLUC_RT": "2.26", "TDD_OPNPRC": "4435",
     "TDD_HGPRC": "4545", "TDD_LWPRC": "4435", "ACC_TRDVOL": "74511",
     "ACC_TRDVAL": "334258460", "MKTCAP": "204542470680",
     "LIST_SHRS": "45252759"}

Three properties of that response drive the code below, and each of them
differs from the pykrx path this replaces.

1. **A non-trading day returns zero rows.**  ``basDd=20260817`` (a substitute
   holiday) returned an empty ``OutBlock_1``.  pykrx instead returns every
   ticker with the price columns zeroed, which is why the pykrx adapter has to
   pass ``alternative=False`` and treat ``종가 == 0`` as "not a real row".
   Neither guard is needed here: absence is expressed as absence.

2. **Every value is a string**, including the numeric ones, and a field can be
   an empty string.  Parsing is explicit and an unparseable value becomes
   ``None`` rather than zero — a zero that cannot be told from a real zero is
   the bug the pykrx adapter had to work around.

3. **``MKT_NM`` exists but is not used to fill ``market``.**  The endpoint is
   already market-specific, so the call argument is authoritative and cheaper
   to trust.  ``SECT_TP_NM`` is *not* an industry: it is the KOSDAQ 소속부
   (벤처기업부 / 중견기업부) and is blank for KOSPI.  It is deliberately not
   mapped anywhere near N2's ``induty_code``.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date

from krx_collector.adapters.market_data_krx_openapi.client import KrxOpenApiClient
from krx_collector.domain.enums import ListingStatus, Market, Source
from krx_collector.domain.models import (
    DailyMarketCapResult,
    DailyMarketCapRow,
    Stock,
    StockUniverseSnapshot,
    UniverseResult,
)
from krx_collector.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)

#: One daily-trade endpoint per market.  There is no combined endpoint, and
#: that is fine: the split is what fills ``market`` without a stock_master join.
DAILY_TRADE_ENDPOINTS: dict[Market, tuple[str, str]] = {
    Market.KOSPI: ("sto", "stk_bydd_trd"),
    Market.KOSDAQ: ("sto", "ksq_bydd_trd"),
}

#: Issue base info, same market split.  Carries LIST_DD / SECUGRP_NM /
#: KIND_STKCERT_TP_NM, which is what a universe filter policy needs.
ISSUE_BASE_ENDPOINTS: dict[Market, tuple[str, str]] = {
    Market.KOSPI: ("sto", "stk_isu_base_info"),
    Market.KOSDAQ: ("sto", "ksq_isu_base_info"),
}

_TICKER = "ISU_CD"
_NAME = "ISU_NM"
_CLOSE = "TDD_CLSPRC"
_OPEN = "TDD_OPNPRC"
_HIGH = "TDD_HGPRC"
_LOW = "TDD_LWPRC"
_VOLUME = "ACC_TRDVOL"
_TRADING_VALUE = "ACC_TRDVAL"
_MARKET_CAP = "MKTCAP"
_LISTED_SHARES = "LIST_SHRS"

REQUIRED_COLUMNS: tuple[str, ...] = (
    _TICKER,
    _NAME,
    _CLOSE,
    _OPEN,
    _HIGH,
    _LOW,
    _VOLUME,
    _TRADING_VALUE,
    _MARKET_CAP,
    _LISTED_SHARES,
)


def parse_int(value: object) -> int | None:
    """Parse a KRX Open API numeric string.

    Values arrive as strings, sometimes empty, sometimes comma-grouped.  A
    value that cannot be parsed becomes ``None``, never ``0``.

    Args:
        value: Raw cell from the response.

    Returns:
        The integer value, or ``None`` when it is blank or unparseable.
    """
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text == "-":
        return None
    try:
        return int(text)
    except ValueError:
        try:
            return int(float(text))
        except ValueError:
            return None


def _missing_columns(row: dict[str, object]) -> tuple[str, ...]:
    return tuple(column for column in REQUIRED_COLUMNS if column not in row)


class KrxOpenApiMarketCapProvider:
    """Fetches one ``(trade_date, market)`` market-cap slice from the Open API.

    Conforms to :class:`~krx_collector.ports.market_cap.MarketCapProvider`.
    """

    def __init__(self, client: KrxOpenApiClient) -> None:
        self._client = client

    def fetch_by_date(self, trade_date: date, market: Market) -> DailyMarketCapResult:
        """Retrieve every listed stock's market-cap row for one date and market.

        Args:
            trade_date: Trading date to fetch.
            market: Market segment; fills the ``market`` column.

        Returns:
            ``DailyMarketCapResult`` with normalised rows, the raw response row
            count, or an error message.  Never raises for an upstream failure.
        """
        endpoint = DAILY_TRADE_ENDPOINTS.get(market)
        if endpoint is None:
            return DailyMarketCapResult(
                trade_date=trade_date,
                market=market,
                error=f"KRX Open API has no daily-trade endpoint for market {market.value}",
            )

        group, name = endpoint
        try:
            raw_rows = self._client.fetch_rows(
                group, name, {"basDd": trade_date.strftime("%Y%m%d")}
            )
        except Exception as exc:
            logger.exception("KRX Open API market-cap fetch failed: %s", exc)
            return DailyMarketCapResult(trade_date=trade_date, market=market, error=str(exc))

        if raw_rows:
            missing = _missing_columns(raw_rows[0])
            if missing:
                # A renamed field would otherwise turn into a column of NULLs
                # that looks like sparse upstream data.
                return DailyMarketCapResult(
                    trade_date=trade_date,
                    market=market,
                    response_rows=len(raw_rows),
                    error=f"KRX Open API response is missing columns: {', '.join(missing)}",
                )

        fetched_at = now_kst()
        rows: list[DailyMarketCapRow] = []
        for raw in raw_rows:
            ticker = str(raw.get(_TICKER, "")).strip()
            if not ticker:
                continue
            rows.append(
                DailyMarketCapRow(
                    ticker=ticker,
                    market=market,
                    trade_date=trade_date,
                    source_close=parse_int(raw.get(_CLOSE)),
                    market_cap=parse_int(raw.get(_MARKET_CAP)),
                    trading_value=parse_int(raw.get(_TRADING_VALUE)),
                    listed_shares=parse_int(raw.get(_LISTED_SHARES)),
                    volume=parse_int(raw.get(_VOLUME)),
                    source=Source.KRX_OPENAPI,
                    fetched_at=fetched_at,
                    source_open=parse_int(raw.get(_OPEN)),
                    source_high=parse_int(raw.get(_HIGH)),
                    source_low=parse_int(raw.get(_LOW)),
                )
            )

        return DailyMarketCapResult(
            trade_date=trade_date,
            market=market,
            rows=rows,
            response_rows=len(raw_rows),
        )


class KrxOpenApiHistoricalUniverseProvider:
    """Reconstructs a past-dated universe snapshot from the Open API.

    Conforms to :class:`~krx_collector.ports.universe.UniverseProvider`.

    The same daily-trade call that fills market cap already lists exactly the
    tickers that traded that day, with their names, so a snapshot costs one
    request per market rather than the two the pykrx path needed.  Snapshots
    carry ``Source.KRX_OPENAPI_BACKFILL`` so the live ``sync_universe`` diff
    never reads a reconstructed snapshot as a newly observed one.
    """

    def __init__(self, client: KrxOpenApiClient) -> None:
        self._client = client

    def fetch_universe(
        self,
        markets: list[Market],
        as_of: date | None = None,
    ) -> UniverseResult:
        """Retrieve the listed universe as of a date.

        Args:
            markets: Market segments to query.
            as_of: Reference date.  ``None`` means today (Asia/Seoul).

        Returns:
            ``UniverseResult`` with a ``Source.KRX_OPENAPI_BACKFILL`` snapshot,
            or an error.  Never raises for an upstream failure.
        """
        reference_date = as_of or today_kst()
        fetched_at = now_kst()
        records: list[Stock] = []

        for market in markets:
            endpoint = DAILY_TRADE_ENDPOINTS.get(market)
            if endpoint is None:
                return UniverseResult(
                    error=f"KRX Open API has no daily-trade endpoint for market {market.value}"
                )
            group, name = endpoint
            try:
                raw_rows = self._client.fetch_rows(
                    group, name, {"basDd": reference_date.strftime("%Y%m%d")}
                )
            except Exception as exc:
                logger.exception("KRX Open API universe fetch failed: %s", exc)
                return UniverseResult(error=str(exc))

            if not raw_rows:
                # A trading day that returns nothing is an upstream problem,
                # not an empty market.  Reporting it as a snapshot would look
                # like every listing was delisted at once.
                return UniverseResult(
                    error=(
                        f"KRX Open API returned no rows for {market.value} on "
                        f"{reference_date.isoformat()}; refusing to record an empty universe"
                    )
                )

            for raw in raw_rows:
                ticker = str(raw.get(_TICKER, "")).strip()
                if not ticker:
                    continue
                records.append(
                    Stock(
                        ticker=ticker,
                        market=market,
                        # The daily-trade response carries ISU_NM, so unlike
                        # the pykrx path there is no second call and no
                        # ticker-as-name fallback to spend requests avoiding.
                        name=str(raw.get(_NAME, "")).strip() or ticker,
                        status=ListingStatus.ACTIVE,
                        last_seen_date=reference_date,
                        source=Source.KRX_OPENAPI_BACKFILL,
                    )
                )

        snapshot = StockUniverseSnapshot(
            snapshot_id=str(uuid.uuid4()),
            as_of_date=reference_date,
            source=Source.KRX_OPENAPI_BACKFILL,
            fetched_at=fetched_at,
            records=records,
        )
        return UniverseResult(snapshot=snapshot)
