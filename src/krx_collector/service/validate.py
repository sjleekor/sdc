"""Use-case: Data validation.

Planned validations (design only — no execution logic yet):

1. **OHLC sanity rules**
   - low <= open <= high
   - low <= close <= high
   - All prices > 0 (unless delisted/halted — volume == 0 allowed)
   - Volume >= 0

2. **Missing-day checks**
   - Compare stored trade dates against the trading calendar.
   - Flag tickers with gaps (excluding holidays and weekends).
   - Use ``infra.calendar.trading_days`` for the calendar strategy.

3. **Universe count drift checks**
   - Compare today's snapshot record_count against previous snapshots.
   - Alert if the count changes by more than a configurable threshold
     (e.g., ±5%), which may indicate data-source issues.

4. **Cross-source consistency** (future)
   - Compare FDR vs pykrx universe snapshots for the same date.
   - Flag discrepancies in ticker sets or names.
"""

from __future__ import annotations

import logging
from datetime import date

from krx_collector.domain.enums import Market, RunStatus, RunType
from krx_collector.domain.models import IngestionRun
from krx_collector.infra.calendar.trading_days import get_trading_days, load_holidays
from krx_collector.ports.storage import Storage
from krx_collector.util.time import now_kst, today_kst

logger = logging.getLogger(__name__)


def validate(
    storage: Storage,
    market: Market | None = None,
    target_date: date | None = None,
) -> None:
    """Run data-quality validations."""
    resolved_date = target_date or today_kst()

    run = IngestionRun(
        run_type=RunType.VALIDATE,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params={
            "market": market.value if market else None,
            "target_date": str(resolved_date),
        },
    )
    storage.record_run(run)

    try:
        # 1. Load trading calendar
        holidays = load_holidays()
        trading_days = get_trading_days(resolved_date, resolved_date, holidays)
        is_trading_day = len(trading_days) > 0

        if not is_trading_day:
            logger.info("%s is not a trading day. Skipping missing-day check.", resolved_date)

        # 2. Query daily_ohlcv
        bars = storage.get_daily_bars(resolved_date, market)
        logger.info("Found %d bars for %s.", len(bars), resolved_date)

        # 3. Apply OHLC sanity rules
        violations = []
        for bar in bars:
            if not (bar.low <= bar.open <= bar.high):
                violations.append(f"{bar.ticker}: open ({bar.open}) out of bounds [{bar.low}, {bar.high}]")
            if not (bar.low <= bar.close <= bar.high):
                violations.append(f"{bar.ticker}: close ({bar.close}) out of bounds [{bar.low}, {bar.high}]")
            if bar.volume < 0:
                violations.append(f"{bar.ticker}: negative volume ({bar.volume})")

        if violations:
            logger.warning("Found %d OHLC violations.", len(violations))
            for v in violations[:10]:
                logger.warning("  - %s", v)
            if len(violations) > 10:
                logger.warning("  - ... and %d more", len(violations) - 10)

        # 4. Check for missing trading days per ticker
        missing_tickers = []
        if is_trading_day:
            active_stocks = storage.get_active_stocks(market)
            fetched_tickers = {b.ticker for b in bars}

            for stock in active_stocks:
                # If listed after target_date, skip
                if stock.listing_date and stock.listing_date > resolved_date:
                    continue

                if stock.ticker not in fetched_tickers:
                    missing_tickers.append(stock.ticker)

            if missing_tickers:
                logger.warning("Missing daily bars for %d active tickers on %s.", len(missing_tickers), resolved_date)

        # 5. Check universe count drift (skipped in this iteration)
        # TODO: Implement universe count drift check

        # 6. Complete the run
        run.ended_at = now_kst()
        run.status = RunStatus.SUCCESS
        run.counts = {
            "bars_checked": len(bars),
            "ohlc_violations": len(violations),
            "missing_tickers": len(missing_tickers),
        }

        if violations or missing_tickers:
            run.error_summary = f"Violations: {len(violations)}, Missing: {len(missing_tickers)}"

        storage.record_run(run)
        logger.info("Validation completed.")

    except Exception as exc:
        logger.exception("Validation failed")
        run.ended_at = now_kst()
        run.status = RunStatus.FAILED
        run.error_summary = str(exc)
        storage.record_run(run)
