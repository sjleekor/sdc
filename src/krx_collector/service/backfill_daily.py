"""Use-case: Backfill daily OHLCV prices.

Responsibilities:
    1. Determine the set of tickers to process (all active, or a subset).
    2. For each ticker, determine the start date (defaults to 2000-01-01).
    3. Chunk the date range into manageable batches to avoid memory issues
       and enable resume/checkpointing.
    4. Fetch daily bars from ``PriceProvider`` with rate limiting.
    5. Idempotent upsert via ``Storage.upsert_daily_bars``.
    6. Record the ingestion run for auditability.

Resume / checkpointing design (not yet implemented):
    - Each ticker+date-range chunk writes to ``ingestion_runs`` on completion.
    - On restart, the service queries the last successful chunk and resumes
      from the next date.
"""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta

from krx_collector.domain.enums import Market, RunStatus, RunType
from krx_collector.domain.models import BackfillResult, IngestionRun, Stock
from krx_collector.ports.prices import PriceProvider
from krx_collector.ports.storage import Storage
from krx_collector.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)


def backfill_daily_prices(
    provider: PriceProvider,
    storage: Storage,
    market: Market | None = None,
    tickers: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    rate_limit_seconds: float = 0.2,
) -> BackfillResult:
    """Backfill daily OHLCV bars from *provider* into *storage*."""
    run = IngestionRun(
        run_type=RunType.DAILY_BACKFILL,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "market": market.value if market else None,
            "tickers": tickers,
            "start": str(start) if start else None,
            "end": str(end) if end else None,
            "rate_limit": rate_limit_seconds,
        },
    )
    storage.record_run(run)

    result = BackfillResult()

    try:
        # 1. Resolve ticker list
        target_stocks: list[Stock] = []
        if tickers:
            all_active = storage.get_active_stocks()
            ticker_set = set(tickers)
            target_stocks = [s for s in all_active if s.ticker in ticker_set]
            if not target_stocks:
                logger.warning("None of the provided tickers were found as ACTIVE in stock_master.")
        else:
            target_stocks = storage.get_active_stocks(market)

        if not target_stocks:
            logger.info("No active stocks found to backfill.")
            run.ended_at = now_kst()
            run.status = RunStatus.SUCCESS
            storage.record_run(run)
            return result

        # 2. Resolve end date
        resolved_end = end or today_kst()

        # 3. For each ticker
        for stock in target_stocks:
            ticker = stock.ticker
            result.tickers_processed += 1

            # Determine start date
            resolved_start = start or date(2000, 1, 1) # arbitrary early date for pykrx

            if resolved_start > resolved_end:
                logger.warning("Start date > end date for %s. Skipping.", ticker)
                continue
            try:
                # To be robust, we could query missing days, or chunk by year.
                # For pykrx, querying long ranges at once is fine but chunking is safer.
                # Let's just fetch the whole range for now as a single chunk to simplify,
                # or chunk by 1-year blocks.

                current_start = resolved_start
                while current_start <= resolved_end:
                    current_end = min(
                        current_start + timedelta(days=365),
                        resolved_end
                    )

                    fetch_res = provider.fetch_daily_ohlcv(
                        ticker=ticker,
                        market=stock.market,
                        start=current_start,
                        end=current_end
                    )

                    if fetch_res.error:
                        result.errors[ticker] = fetch_res.error
                        break

                    if fetch_res.bars:
                        upsert_res = storage.upsert_daily_bars(fetch_res.bars)
                        result.bars_upserted += upsert_res.updated

                    # Rate limiting
                    time.sleep(rate_limit_seconds)

                    current_start = current_end + timedelta(days=1)

            except Exception as exc:
                logger.exception("Error backfilling ticker %s", ticker)
                result.errors[ticker] = str(exc)

        # 4. Record IngestionRun
        run.ended_at = now_kst()
        run.status = RunStatus.SUCCESS if not result.errors else RunStatus.FAILED
        run.counts = {
            "tickers_processed": result.tickers_processed,
            "bars_upserted": result.bars_upserted,
            "error_count": len(result.errors),
        }
        if result.errors:
            run.error_summary = f"{len(result.errors)} tickers had errors."

        storage.record_run(run)
        return result

    except Exception as exc:
        logger.exception("Backfill pipeline failed")
        run.ended_at = now_kst()
        run.status = RunStatus.FAILED
        run.error_summary = str(exc)
        storage.record_run(run)
        return result
