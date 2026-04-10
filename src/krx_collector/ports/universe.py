"""Port: Universe provider interface.

Any adapter that can supply a list of listed stocks for KOSPI / KOSDAQ
must conform to the ``UniverseProvider`` protocol.
"""

from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable

from krx_collector.domain.enums import Market
from krx_collector.domain.models import UniverseResult


@runtime_checkable
class UniverseProvider(Protocol):
    """Fetches the stock universe (listed tickers) for one or more markets.

    Implementations:
        - ``FdrUniverseProvider``  (FinanceDataReader)
        - ``PykrxUniverseProvider`` (pykrx)
    """

    def fetch_universe(
        self,
        markets: list[Market],
        as_of: date | None = None,
    ) -> UniverseResult:
        """Retrieve the stock universe.

        Args:
            markets: Market segments to include.
            as_of: Reference date for the universe snapshot.  ``None``
                means "today" (Asia/Seoul).

        Returns:
            ``UniverseResult`` containing the snapshot or an error.
        """
        ...
