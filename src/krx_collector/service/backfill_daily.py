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
import random
import time
from datetime import date, timedelta

from krx_collector.domain.enums import Market, RunStatus, RunType
from krx_collector.domain.models import BackfillResult, DailyPriceResult, IngestionRun, Stock
from krx_collector.ports.prices import PriceProvider
from krx_collector.ports.storage import Storage
from krx_collector.util.retry import retry
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
    long_rest_interval: int = 100,
    long_rest_seconds: float = 10.0,
    incremental: bool = False,
    lookback_days: int = 0,
    max_auto_range_days: int | None = None,
    new_ticker_start: date | None = None,
    allow_new_ticker_backfill: bool = False,
    allow_large_range: bool = False,
) -> BackfillResult:
    """Backfill daily OHLCV bars from *provider* into *storage*.

    Args:
        incremental: If ``True``, skip per-day gap detection and instead
            fetch a single contiguous range starting from
            ``MAX(trade_date) + 1`` for each ticker. This trusts that
            historical data is already complete and is intended for
            fast daily catch-up runs. Tickers with no stored rows fall
            back to ``start`` (or the default early date).
    """
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
            "long_rest_interval": long_rest_interval,
            "long_rest_seconds": long_rest_seconds,
            "incremental": incremental,
            "lookback_days": lookback_days,
            "max_auto_range_days": max_auto_range_days,
            "new_ticker_start": str(new_ticker_start) if new_ticker_start else None,
            "allow_new_ticker_backfill": allow_new_ticker_backfill,
            "allow_large_range": allow_large_range,
        },
    )
    storage.record_run(run)

    result = BackfillResult()
    api_requests_count = 0
    no_work_tickers = 0
    baseline_missing_tickers = 0
    range_too_large_tickers = 0

    @retry(max_attempts=4, base_delay=0.5, backoff_factor=2.0)
    def _fetch_with_retry(t: str, m: Market, s: date, e: date) -> DailyPriceResult:
        res = provider.fetch_daily_ohlcv(ticker=t, market=m, start=s, end=e)
        if res.error:
            # Raise an exception so that the @retry decorator can catch it and backoff.
            raise RuntimeError(res.error)
        return res

    try:
        if lookback_days < 0:
            raise ValueError("lookback_days must be >= 0")
        if max_auto_range_days is not None and max_auto_range_days <= 0:
            raise ValueError("max_auto_range_days must be positive")

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
            resolved_start = start or date(2000, 1, 1)  # arbitrary early date for pykrx

            if incremental:
                # Incremental mode: start strictly after the last stored
                # trade date. Skips gap detection entirely — trusts that
                # historical data is already complete.
                max_stored = storage.get_max_trade_date(ticker)
                if max_stored:
                    next_date = max_stored + timedelta(days=1)
                    if lookback_days > 0:
                        next_date = min(next_date, resolved_end - timedelta(days=lookback_days))
                    if next_date > resolved_start:
                        logger.debug(
                            "Incremental: %s starts at %s (after last stored %s)",
                            ticker,
                            next_date,
                            max_stored,
                        )
                        resolved_start = next_date
                elif start is None and not allow_new_ticker_backfill:
                    if new_ticker_start is None:
                        baseline_missing_tickers += 1
                        result.errors[ticker] = (
                            "No stored daily_ohlcv baseline for incremental backfill. "
                            "Run explicit backfill or pass --new-ticker-start."
                        )
                        logger.warning("Skipping %s: %s", ticker, result.errors[ticker])
                        continue
                    resolved_start = new_ticker_start
            else:
                # Clamp start to the ticker's earliest stored trade date (if any).
                # This avoids re-requesting pre-listing / pre-data-start ranges
                # that the provider will never return on subsequent runs.
                min_stored = storage.get_min_trade_date(ticker)
                if min_stored and min_stored > resolved_start:
                    logger.debug(
                        "Clamping start for %s from %s to %s (earliest stored trade date)",
                        ticker,
                        resolved_start,
                        min_stored,
                    )
                    resolved_start = min_stored

            if resolved_start > resolved_end:
                logger.info(
                    "Nothing to fetch for %s (start=%s > end=%s). Skipping.",
                    ticker,
                    resolved_start,
                    resolved_end,
                )
                no_work_tickers += 1
                continue

            auto_range_days = (resolved_end - resolved_start).days + 1
            if (
                incremental
                and max_auto_range_days is not None
                and auto_range_days > max_auto_range_days
                and not allow_large_range
            ):
                range_too_large_tickers += 1
                result.errors[ticker] = (
                    f"Resolved incremental range is too large "
                    f"({auto_range_days} days > {max_auto_range_days})."
                )
                logger.warning("Skipping %s: %s", ticker, result.errors[ticker])
                continue
            try:
                ranges: list[tuple[date, date]] = []
                if incremental:
                    # Single contiguous range from resolved_start to resolved_end.
                    ranges.append((resolved_start, resolved_end))
                else:
                    # 1. Query missing days to optimize fetching
                    missing_days = storage.query_missing_days(ticker, resolved_start, resolved_end)

                    if not missing_days:
                        logger.debug("No missing days for %s. Skipping.", ticker)
                        continue

                    # 2. Group missing days into continuous date ranges
                    current_range_start = missing_days[0]
                    current_range_end = missing_days[0]

                    for d in missing_days[1:]:
                        if d == current_range_end + timedelta(days=1):
                            current_range_end = d
                        else:
                            ranges.append((current_range_start, current_range_end))
                            current_range_start = d
                            current_range_end = d
                    ranges.append((current_range_start, current_range_end))

                # 3. Fetch and upsert for each range
                for r_start, r_end in ranges:
                    current_start = r_start
                    while current_start <= r_end:
                        # Chunk by 1 year to avoid overloading the pykrx API
                        current_end = min(current_start + timedelta(days=365), r_end)

                        logger.info(
                            "Backfilling %s from %s to %s", ticker, current_start, current_end
                        )

                        try:
                            fetch_res = _fetch_with_retry(
                                ticker, stock.market, current_start, current_end
                            )
                        except Exception as e:
                            fetch_res = DailyPriceResult(ticker=ticker, error=str(e))

                        api_requests_count += 1

                        if long_rest_interval > 0 and api_requests_count % long_rest_interval == 0:
                            logger.info(
                                "Reached %d requests. Taking a long rest for %.1f seconds...",
                                api_requests_count,
                                long_rest_seconds,
                            )
                            time.sleep(long_rest_seconds)

                        if fetch_res.error:
                            result.errors[ticker] = fetch_res.error
                            break

                        if fetch_res.bars:
                            upsert_res = storage.upsert_daily_bars(fetch_res.bars)
                            result.bars_upserted += upsert_res.updated

                        # Rate limiting with jitter (+/- 20%)
                        if rate_limit_seconds > 0:
                            jitter = random.uniform(-0.2, 0.2) * rate_limit_seconds
                            time.sleep(max(0.0, rate_limit_seconds + jitter))

                        current_start = current_end + timedelta(days=1)

                    if ticker in result.errors:
                        break

            except Exception as exc:
                logger.exception("Error backfilling ticker %s", ticker)
                result.errors[ticker] = str(exc)

        # 4. Record IngestionRun
        run.ended_at = now_kst()
        run.status = RunStatus.SUCCESS if not result.errors else RunStatus.FAILED
        run.counts = {
            "tickers_processed": result.tickers_processed,
            "bars_upserted": result.bars_upserted,
            "no_work_tickers": no_work_tickers,
            "baseline_missing_tickers": baseline_missing_tickers,
            "range_too_large_tickers": range_too_large_tickers,
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
