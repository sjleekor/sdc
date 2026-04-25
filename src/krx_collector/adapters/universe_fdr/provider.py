"""FDR universe provider — stub.

Uses ``FinanceDataReader.StockListing('KOSPI')`` and
``FinanceDataReader.StockListing('KOSDAQ')`` to fetch the current stock
universe.

Mapping notes (for implementation):
    • FDR returns a DataFrame with columns like ``Symbol``, ``Name``,
      ``Market``, ``ListingDate``, etc.
    • Map ``Symbol`` → ``Stock.ticker``, ``Name`` → ``Stock.name``.
    • ``ListingDate`` may be NaT — handle as ``None``.
    • FDR does not provide historical delisting info; all returned rows
      are assumed ``ACTIVE``.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date

import FinanceDataReader as fdr

from krx_collector.domain.enums import ListingStatus, Market, Source
from krx_collector.domain.models import Stock, StockUniverseSnapshot, UniverseResult
from krx_collector.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)


class FdrUniverseProvider:
    """Fetches the KOSPI / KOSDAQ stock universe via FinanceDataReader.

    Conforms to :class:`~krx_collector.ports.universe.UniverseProvider`.
    """

    def fetch_universe(
        self,
        markets: list[Market],
        as_of: date | None = None,
    ) -> UniverseResult:
        """Retrieve listed stocks from FinanceDataReader.

        Args:
            markets: Market segments to query.
            as_of: Reference date (ignored by FDR — always returns current
                listing).

        Returns:
            ``UniverseResult`` with the snapshot.
        """
        try:
            records: list[Stock] = []
            reference_date = as_of or today_kst()
            fetched_at = now_kst()

            for market in markets:
                logger.info("Fetching FDR universe for market: %s", market.value)
                df = fdr.StockListing(market.value)

                if df.empty:
                    logger.warning("FDR returned empty DataFrame for market: %s", market.value)
                    continue

                for _, row in df.iterrows():
                    # FDR columns have changed: 'Code' instead of 'Symbol', 'Name' is still there.
                    # 'ListingDate' seems to be removed in the new FDR output.
                    ticker = str(row.get("Code", ""))
                    if not ticker:
                        continue

                    name = str(row.get("Name", ""))

                    stock = Stock(
                        ticker=ticker,
                        market=market,
                        name=name,
                        status=ListingStatus.ACTIVE,  # FDR current listing only returns active
                        last_seen_date=reference_date,
                        source=Source.FDR,
                    )
                    records.append(stock)

            snapshot = StockUniverseSnapshot(
                snapshot_id=str(uuid.uuid4()),
                as_of_date=reference_date,
                source=Source.FDR,
                fetched_at=fetched_at,
                records=records,
            )
            return UniverseResult(snapshot=snapshot)

        except Exception as exc:
            logger.exception("Failed to fetch FDR universe: %s", exc)
            return UniverseResult(error=str(exc))
