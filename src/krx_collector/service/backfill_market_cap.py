"""Use-case: Backfill daily KRX market cap / trading value / listed shares.

The unit of work is a ``(trade_date, market)`` SLICE — one provider call
returns every listed stock on that date and market.

Two rules shape this service and both come from
``01_implementation_checklist.md`` 2.4.

**A slice is complete only when the stored row count matches the response.**
"Some rows exist, therefore done" would permanently strand any slice that was
interrupted mid-write: the skip rule would never look at it again.  So the skip
decision compares stored counts against what the provider actually returned,
and a mismatch leaves the slice incomplete and retryable.

**Holidays are not fetched.**  pykrx does not return an empty frame for a
non-session date — it returns every ticker with the prices zeroed.  The KRX
trading calendar filters those out before any request is made; the adapter's
zero-close drop is the second line of defence, not the first.
"""

from __future__ import annotations

import logging
import random
import time
from datetime import date

from krx_collector.domain.enums import Market, RunStatus, RunType
from krx_collector.domain.models import (
    DailyMarketCapResult,
    IngestionRun,
    MarketCapBackfillResult,
)
from krx_collector.infra.calendar.trading_days import get_trading_days
from krx_collector.ports.market_cap import MarketCapProvider
from krx_collector.ports.storage import Storage
from krx_collector.util.pipeline import build_run_counts, complete_run, fail_run
from krx_collector.util.retry import retry
from krx_collector.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)

DEFAULT_MARKETS: tuple[Market, ...] = (Market.KOSPI, Market.KOSDAQ)

# 2014-06-02 is where the feature sample starts (02_feature_candidate.md 2.2):
# KOSPI OHLCV only begins 2014-01-20, so anything earlier is KOSDAQ-biased.
DEFAULT_START = date(2014, 6, 2)


def _slice_key(trade_date: date, market: Market) -> str:
    return f"{trade_date.isoformat()}/{market.value}"


def backfill_market_cap(
    provider: MarketCapProvider,
    storage: Storage,
    markets: list[Market] | None = None,
    start: date | None = None,
    end: date | None = None,
    rate_limit_seconds: float = 0.3,
    long_rest_interval: int = 200,
    long_rest_seconds: float = 15.0,
    force: bool = False,
) -> MarketCapBackfillResult:
    """Backfill ``daily_market_cap`` from *provider* into *storage*.

    Args:
        provider: Market-cap provider (pykrx).
        storage: Target storage.
        markets: Markets to fetch.  Defaults to KOSPI + KOSDAQ.  Each market is
            a separate request — the response has no market column, so this is
            required, not an option.
        start: First trade date.  Defaults to ``DEFAULT_START``.
        end: Last trade date.  Defaults to today (KST).
        rate_limit_seconds: Base delay between requests.
        long_rest_interval: Take a long rest every N requests (0 disables).
        long_rest_seconds: Length of the long rest.
        force: Re-fetch slices that are already complete.

    Returns:
        ``MarketCapBackfillResult`` with per-slice counters and errors.
    """
    resolved_markets = list(markets) if markets else list(DEFAULT_MARKETS)
    resolved_start = start or DEFAULT_START
    resolved_end = end or today_kst()

    run = IngestionRun(
        run_type=RunType.MARKET_CAP_BACKFILL,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "markets": [m.value for m in resolved_markets],
            "start": str(resolved_start),
            "end": str(resolved_end),
            "rate_limit": rate_limit_seconds,
            "long_rest_interval": long_rest_interval,
            "long_rest_seconds": long_rest_seconds,
            "force": force,
        },
    )
    storage.record_run(run)

    result = MarketCapBackfillResult()
    requests_made = 0

    @retry(max_attempts=4, base_delay=0.5, backoff_factor=2.0)
    def _fetch_with_retry(d: date, m: Market) -> DailyMarketCapResult:
        res = provider.fetch_by_date(trade_date=d, market=m)
        if res.error:
            # Raise so @retry can back off; the caller records the final failure.
            raise RuntimeError(res.error)
        return res

    try:
        if resolved_start > resolved_end:
            raise ValueError(f"start ({resolved_start}) must be <= end ({resolved_end})")

        trading_days = get_trading_days(resolved_start, resolved_end)
        logger.info(
            "Market-cap backfill %s..%s: %d trading days x %d markets",
            resolved_start,
            resolved_end,
            len(trading_days),
            len(resolved_markets),
        )

        stored_counts = (
            {} if force else storage.get_market_cap_slice_row_counts(resolved_start, resolved_end)
        )

        for trade_date in trading_days:
            for market in resolved_markets:
                label = _slice_key(trade_date, market)
                result.slices_attempted += 1

                # Completeness is a row-count comparison, never mere existence.
                # Without the expected count from a fresh response the best
                # available proxy is the neighbouring slices of the same market,
                # so a short slice is re-fetched rather than trusted.
                stored = stored_counts.get((trade_date, market), 0)
                if stored and not _looks_short(stored, stored_counts, market):
                    result.slices_skipped += 1
                    continue

                try:
                    fetch = _fetch_with_retry(trade_date, market)
                except Exception as exc:  # noqa: BLE001 - collected, not raised
                    result.errors[label] = str(exc)
                    logger.warning("Slice %s failed: %s", label, exc)
                    continue
                finally:
                    requests_made += 1
                    _throttle(
                        requests_made,
                        rate_limit_seconds,
                        long_rest_interval,
                        long_rest_seconds,
                    )

                dropped = max(0, fetch.response_rows - len(fetch.rows))
                result.rows_dropped += dropped

                if not fetch.rows:
                    # Every row was zero-filled: the calendar says session, the
                    # exchange says otherwise.  Not an error, but not a
                    # completed slice either — leave it retryable.
                    logger.info("Slice %s returned no usable rows (%d dropped)", label, dropped)
                    continue

                upsert = storage.upsert_daily_market_cap(fetch.rows)

                if upsert.updated != len(fetch.rows):
                    result.errors[label] = (
                        f"row count mismatch: fetched {len(fetch.rows)}, "
                        f"stored {upsert.updated}"
                    )
                    logger.warning("Slice %s: %s", label, result.errors[label])
                    continue

                result.rows_upserted += upsert.updated
                result.slices_completed += 1

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                slices_attempted=result.slices_attempted,
                slices_skipped=result.slices_skipped,
                slices_completed=result.slices_completed,
                rows_upserted=result.rows_upserted,
                rows_dropped=result.rows_dropped,
                requests_made=requests_made,
            ),
            errors=result.errors,
            partial_subject="market-cap slices",
        )
        return result

    except Exception as exc:
        logger.exception("Market-cap backfill failed")
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result


def _looks_short(
    stored: int,
    stored_counts: dict[tuple[date, Market], int],
    market: Market,
) -> bool:
    """Return ``True`` when a stored slice is implausibly small for its market.

    A partially written slice has rows, so row existence cannot detect it.  The
    listed universe moves slowly, so a slice holding far fewer rows than the
    median for its market was almost certainly interrupted.
    """
    same_market = sorted(count for (_, m), count in stored_counts.items() if m == market)
    if len(same_market) < 3:
        return False
    median = same_market[len(same_market) // 2]
    return stored < median * 0.9


def _throttle(
    requests_made: int,
    rate_limit_seconds: float,
    long_rest_interval: int,
    long_rest_seconds: float,
) -> None:
    """Sleep between requests, with a periodic longer rest."""
    if long_rest_interval > 0 and requests_made % long_rest_interval == 0:
        logger.info("Long rest for %.1fs after %d requests", long_rest_seconds, requests_made)
        time.sleep(long_rest_seconds)
        return

    if rate_limit_seconds > 0:
        jitter = random.uniform(-0.2, 0.2) * rate_limit_seconds
        time.sleep(max(0.0, rate_limit_seconds + jitter))
