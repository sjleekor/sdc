"""pykrx universe provider — stub.

Uses ``pykrx.stock.get_market_ticker_list(date, market)`` and
``pykrx.stock.get_market_ticker_name(ticker)`` to build the stock universe.

Mapping notes (for implementation):
    • ``get_market_ticker_list`` returns a list of ticker strings.
    • Company names must be fetched individually or via
      ``get_market_ohlcv_by_ticker`` with a name column.
    • Listing date is NOT directly available from pykrx — may need to be
      inferred from the earliest date with OHLCV data, or left as ``None``.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date

from krx_collector.adapters.pykrx_auth import get_pykrx_stock_module
from krx_collector.domain.enums import ListingStatus, Market, Source
from krx_collector.domain.models import Stock, StockUniverseSnapshot, UniverseResult
from krx_collector.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)


class PykrxUniverseProvider:
    """Fetches the KOSPI / KOSDAQ stock universe via pykrx.

    Conforms to :class:`~krx_collector.ports.universe.UniverseProvider`.
    """

    def fetch_universe(
        self,
        markets: list[Market],
        as_of: date | None = None,
    ) -> UniverseResult:
        """Retrieve listed stocks from pykrx.

        Args:
            markets: Market segments to query.
            as_of: Reference date.  ``None`` means today (Asia/Seoul).

        Returns:
            ``UniverseResult`` with the snapshot.
        """
        try:
            stock = get_pykrx_stock_module()
            records: list[Stock] = []
            reference_date = as_of or today_kst()
            fetched_at = now_kst()

            # pykrx requires date in 'YYYYMMDD' format
            date_str = reference_date.strftime("%Y%m%d")

            for market in markets:
                logger.info(
                    "Fetching pykrx universe for market: %s as_of: %s", market.value, date_str
                )
                # market argument expects 'KOSPI' or 'KOSDAQ'
                tickers = stock.get_market_ticker_list(date_str, market=market.value)

                if not tickers:
                    logger.warning("pykrx returned empty ticker list for market: %s", market.value)
                    continue

                for ticker in tickers:
                    name = stock.get_market_ticker_name(ticker)

                    records.append(
                        Stock(
                            ticker=ticker,
                            market=market,
                            name=name,
                            status=ListingStatus.ACTIVE,
                            last_seen_date=reference_date,
                            source=Source.PYKRX,
                        )
                    )
            snapshot = StockUniverseSnapshot(
                snapshot_id=str(uuid.uuid4()),
                as_of_date=reference_date,
                source=Source.PYKRX,
                fetched_at=fetched_at,
                records=records,
            )
            return UniverseResult(snapshot=snapshot)

        except Exception as exc:
            logger.exception("Failed to fetch pykrx universe: %s", exc)
            return UniverseResult(error=str(exc))
