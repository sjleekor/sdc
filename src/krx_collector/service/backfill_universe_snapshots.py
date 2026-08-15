"""Use-case: Backfill historical stock-universe snapshots.

``stock_master`` holds only 28 DELISTED rows because ``sync_universe`` infers
delistings by diffing consecutive snapshots — so it can only see what happened
after snapshot collection began.  Everything delisted before that is simply
absent from the sample, which inflates any transition or distress feature
(``fin_turn_to_profit`` keeps the companies that recovered and drops the ones
that were delisted).

``get_market_ticker_list`` accepts a date, so the past listings can be
reconstructed.  Two things bound what this service claims:

**It never writes ``stock_master``.**  It calls
``insert_stock_master_snapshot_only``; a 2016 ticker list must not overwrite
who is listed today.

**It is an audit path, not the daily universe.**  Month-end resolution cannot
see a stock that listed and delisted inside one month, and misses an intra-month
listing by up to a month.  The daily PIT universe comes from ``daily_market_cap``
rows instead — the two ticker sets were verified identical at four sampled dates
(``04_w1_pit_universe.md`` 3.5).
"""

from __future__ import annotations

import calendar
import logging
import random
import time
from datetime import date

from krx_collector.domain.enums import Market, RunStatus, RunType, Source
from krx_collector.domain.models import IngestionRun, UniverseSnapshotBackfillResult
from krx_collector.infra.calendar.trading_days import get_trading_days
from krx_collector.ports.storage import Storage
from krx_collector.ports.universe import UniverseProvider
from krx_collector.util.pipeline import build_run_counts, complete_run, fail_run
from krx_collector.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)

DEFAULT_MARKETS: tuple[Market, ...] = (Market.KOSPI, Market.KOSDAQ)
DEFAULT_START = date(2014, 6, 1)


def month_end_trading_days(start: date, end: date) -> list[date]:
    """Return the last trading day of each month intersecting ``[start, end]``.

    A calendar month-end is often a weekend or a holiday, so the last actual
    session of the month is used.  Months with no session in range are skipped.
    """
    days: list[date] = []
    year, month = start.year, start.month

    while (year, month) <= (end.year, end.month):
        last_day = date(year, month, calendar.monthrange(year, month)[1])
        window_start = max(start, date(year, month, 1))
        window_end = min(end, last_day)

        if window_start <= window_end:
            sessions = get_trading_days(window_start, window_end)
            if sessions:
                days.append(sessions[-1])

        month += 1
        if month > 12:
            year, month = year + 1, 1

    return days


def backfill_universe_snapshots(
    provider: UniverseProvider,
    storage: Storage,
    markets: list[Market] | None = None,
    start: date | None = None,
    end: date | None = None,
    rate_limit_seconds: float = 0.5,
    force: bool = False,
) -> UniverseSnapshotBackfillResult:
    """Backfill month-end universe snapshots from *provider* into *storage*.

    Args:
        provider: Universe provider that honours ``as_of``
            (``PykrxHistoricalUniverseProvider``).
        storage: Target storage.
        markets: Markets to include in each snapshot.  Defaults to KOSPI+KOSDAQ.
        start: First date to consider.  Defaults to ``DEFAULT_START``.
        end: Last date to consider.  Defaults to today (KST).
        rate_limit_seconds: Base delay between snapshots.
        force: Re-fetch dates that already have a backfilled snapshot.

    Returns:
        ``UniverseSnapshotBackfillResult`` with per-snapshot counters.
    """
    resolved_markets = list(markets) if markets else list(DEFAULT_MARKETS)
    resolved_start = start or DEFAULT_START
    resolved_end = end or today_kst()

    run = IngestionRun(
        run_type=RunType.UNIVERSE_SNAPSHOT_BACKFILL,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "markets": [m.value for m in resolved_markets],
            "start": str(resolved_start),
            "end": str(resolved_end),
            "rate_limit": rate_limit_seconds,
            "force": force,
            "source": Source.PYKRX_BACKFILL.value,
        },
    )
    storage.record_run(run)

    result = UniverseSnapshotBackfillResult()

    try:
        if resolved_start > resolved_end:
            raise ValueError(f"start ({resolved_start}) must be <= end ({resolved_end})")

        targets = month_end_trading_days(resolved_start, resolved_end)
        existing = set() if force else storage.get_existing_snapshot_dates(Source.PYKRX_BACKFILL)
        logger.info(
            "Universe snapshot backfill %s..%s: %d month-ends (%d already stored)",
            resolved_start,
            resolved_end,
            len(targets),
            len(existing),
        )

        for as_of in targets:
            result.snapshots_attempted += 1

            if as_of in existing:
                result.snapshots_skipped += 1
                continue

            fetch = provider.fetch_universe(markets=resolved_markets, as_of=as_of)

            if rate_limit_seconds > 0:
                jitter = random.uniform(-0.2, 0.2) * rate_limit_seconds
                time.sleep(max(0.0, rate_limit_seconds + jitter))

            if fetch.error or fetch.snapshot is None:
                message = fetch.error or "provider returned no snapshot"
                result.errors[as_of.isoformat()] = message
                logger.warning("Snapshot %s failed: %s", as_of, message)
                continue

            snapshot = fetch.snapshot
            if not snapshot.records:
                result.errors[as_of.isoformat()] = "provider returned an empty universe"
                logger.warning("Snapshot %s returned no records", as_of)
                continue

            upsert = storage.insert_stock_master_snapshot_only(snapshot)

            result.snapshots_written += 1
            result.items_written += upsert.inserted
            logger.info("Snapshot %s stored: %d tickers", as_of, snapshot.record_count)

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                snapshots_attempted=result.snapshots_attempted,
                snapshots_skipped=result.snapshots_skipped,
                snapshots_written=result.snapshots_written,
                items_written=result.items_written,
            ),
            errors=result.errors,
            partial_subject="universe snapshots",
        )
        return result

    except Exception as exc:
        logger.exception("Universe snapshot backfill failed")
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result
