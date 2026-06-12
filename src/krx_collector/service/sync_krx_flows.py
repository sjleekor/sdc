"""Use-case: Sync KRX security-flow raw rows."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import date, timedelta

from krx_collector.domain.enums import ListingStatus, RunStatus, RunType, Source
from krx_collector.domain.models import IngestionRun, KrxFlowSyncResult, Stock
from krx_collector.infra.calendar.trading_days import get_trading_days
from krx_collector.ports.flows import FlowProvider
from krx_collector.ports.storage import Storage
from krx_collector.util.pipeline import (
    build_run_counts,
    call_with_retry,
    complete_run,
    fail_run,
    sleep_with_jitter,
)
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)

FOREIGN_HOLDING_METRIC = "foreign_holding_shares"
INVESTOR_METRICS = [
    "institution_net_buy_volume",
    "individual_net_buy_volume",
    "foreign_net_buy_volume",
]
SHORTING_METRICS = [
    "short_selling_volume",
    "short_selling_value",
    "short_selling_balance_quantity",
]
FLOW_METRIC_GROUPS: dict[str, tuple[str, ...]] = {
    "foreign_holding": (FOREIGN_HOLDING_METRIC,),
    "investor": tuple(INVESTOR_METRICS),
    "shorting": tuple(SHORTING_METRICS),
}
DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS = 30.0
DEFAULT_PROGRESS_LOG_EVERY_ITEMS = 100
SLOW_FLOW_REQUEST_WARNING_SECONDS = 30.0


@dataclass(frozen=True, slots=True)
class FlowIncrementalRange:
    """Resolved date range and audit details for incremental flow sync."""

    start: date
    end: date
    latest_price_date: date
    group_latest_dates: dict[str, date]
    group_lag_days: dict[str, int]
    excluded_groups: list[str]
    lookback_days: int
    latest_flow_date: date

    @property
    def auto_range_days(self) -> int:
        """Inclusive calendar-day size of the resolved range."""
        return (self.end - self.start).days + 1

    @property
    def no_work(self) -> bool:
        return self.start > self.end

    def as_run_params(self) -> dict[str, object]:
        """Return JSON-serialisable params for ingestion_runs auditing."""
        return {
            "incremental": True,
            "resolved_start": self.start.isoformat(),
            "resolved_end": self.end.isoformat(),
            "lookback_days": self.lookback_days,
            "excluded_groups": self.excluded_groups,
            "latest_price_date": self.latest_price_date.isoformat(),
            "latest_flow_date": self.latest_flow_date.isoformat(),
            "group_latest_dates": {
                group: latest.isoformat()
                for group, latest in sorted(self.group_latest_dates.items())
            },
            "group_lag_days": dict(sorted(self.group_lag_days.items())),
            "auto_range_days": self.auto_range_days,
        }


class _FlowProgressLogger:
    """Emit bounded progress logs for long-running flow sync phases."""

    def __init__(
        self,
        *,
        interval_seconds: float,
        every_items: int,
    ) -> None:
        self._interval_seconds = interval_seconds
        self._every_items = every_items
        self._phase = ""
        self._phase_total = 0
        self._phase_started_at = time.monotonic()
        self._last_log_at = self._phase_started_at
        self._last_log_processed = 0

    def start_phase(self, phase: str, total: int, details: str = "") -> None:
        now = time.monotonic()
        self._phase = phase
        self._phase_total = total
        self._phase_started_at = now
        self._last_log_at = now
        self._last_log_processed = 0
        suffix = f" {details}" if details else ""
        logger.info("Flow sync phase started: phase=%s total=%d%s", phase, total, suffix)

    def tick(
        self,
        *,
        processed: int,
        attempted: int,
        skipped: int,
        rows_upserted: int,
        no_data: int,
        errors: int,
        current: str,
    ) -> None:
        if self._phase_total <= 0:
            return

        now = time.monotonic()
        should_log = processed >= self._phase_total
        if self._every_items > 0 and processed - self._last_log_processed >= self._every_items:
            should_log = True
        if self._interval_seconds > 0 and now - self._last_log_at >= self._interval_seconds:
            should_log = True
        if not should_log:
            return

        logger.info(
            "Flow sync progress: phase=%s processed=%d/%d attempted=%d skipped=%d "
            "rows_upserted=%d no_data=%d errors=%d elapsed=%.1fs current=%s",
            self._phase,
            processed,
            self._phase_total,
            attempted,
            skipped,
            rows_upserted,
            no_data,
            errors,
            now - self._phase_started_at,
            current,
        )
        self._last_log_at = now
        self._last_log_processed = processed


def _log_flow_request_result(request_key: str, elapsed_seconds: float, result: object) -> None:
    records = getattr(result, "records", None)
    record_count = len(records) if records is not None else 0
    no_data = bool(getattr(result, "no_data", False))
    error = getattr(result, "error", None)
    if elapsed_seconds >= SLOW_FLOW_REQUEST_WARNING_SECONDS:
        logger.warning(
            "Slow flow request: request=%s elapsed=%.1fs records=%d no_data=%s error=%s",
            request_key,
            elapsed_seconds,
            record_count,
            no_data,
            error,
        )
        return

    logger.debug(
        "Flow request completed: request=%s elapsed=%.1fs records=%d no_data=%s error=%s",
        request_key,
        elapsed_seconds,
        record_count,
        no_data,
        error,
    )


def _filter_targets(stocks: list[Stock], tickers: list[str] | None) -> list[Stock]:
    if tickers is None:
        return stocks
    ticker_filter = set(tickers)
    return [stock for stock in stocks if stock.ticker in ticker_filter]


def _load_targets(storage: Storage, tickers: list[str] | None) -> list[Stock]:
    stocks = _filter_targets(storage.get_active_stocks(), tickers)
    if stocks:
        return stocks

    dart_rows = storage.get_dart_corp_master(active_only=True, tickers=tickers)
    return [
        Stock(
            ticker=row.ticker or "",
            market=row.market,
            name=row.stock_name or row.corp_name,
            status=ListingStatus.ACTIVE,
            last_seen_date=row.modify_date or date.today(),
            source=Source.OPENDART,
        )
        for row in dart_rows
        if row.ticker and row.market is not None
    ]


def resolve_incremental_flow_range(
    *,
    latest_price_date: date | None,
    metric_latest_dates: dict[str, date],
    lookback_days: int,
    exclude_groups: list[str] | None = None,
) -> FlowIncrementalRange:
    """Resolve the automatic incremental ``flows sync`` date range.

    ``metric_latest_dates`` is keyed by metric_code.  For a group that contains
    multiple metrics, the group latest date is the minimum of its metric latest
    dates so one advanced metric cannot hide another lagging one.
    """
    if lookback_days < 0:
        raise ValueError("lookback_days must be >= 0")
    if latest_price_date is None:
        raise ValueError("Cannot resolve incremental flow range: daily_ohlcv has no rows.")

    excluded = sorted(set(exclude_groups or []))
    unknown_groups = sorted(set(excluded) - set(FLOW_METRIC_GROUPS))
    if unknown_groups:
        supported = ", ".join(sorted(FLOW_METRIC_GROUPS))
        raise ValueError(
            f"Unknown flow metric group(s): {', '.join(unknown_groups)} "
            f"(supported: {supported})"
        )

    active_groups = {
        group: metrics
        for group, metrics in FLOW_METRIC_GROUPS.items()
        if group not in set(excluded)
    }
    if not active_groups:
        raise ValueError("Cannot resolve incremental flow range: all metric groups are excluded.")

    group_latest_dates: dict[str, date] = {}
    missing: dict[str, list[str]] = {}
    for group, metrics in active_groups.items():
        group_dates: list[date] = []
        for metric in metrics:
            metric_latest = metric_latest_dates.get(metric)
            if metric_latest is None:
                missing.setdefault(group, []).append(metric)
            else:
                group_dates.append(metric_latest)
        if group_dates:
            group_latest_dates[group] = min(group_dates)

    if missing:
        details = "; ".join(
            f"{group}={','.join(metrics)}" for group, metrics in sorted(missing.items())
        )
        raise ValueError(
            "Cannot resolve incremental flow range: missing baseline latest date "
            f"for metric(s): {details}. Run explicit flow backfill first."
        )
    if not group_latest_dates:
        raise ValueError(
            "Cannot resolve incremental flow range: no baseline krx_security_flow_raw rows. "
            "Run explicit flow backfill first."
        )

    latest_flow_date = min(group_latest_dates.values())
    next_flow_trading_day = _next_trading_day_after(latest_flow_date, latest_price_date)
    lookback_start = latest_price_date - timedelta(days=lookback_days)
    resolved_start = min(next_flow_trading_day, lookback_start)
    group_lag_days = {
        group: (latest_price_date - group_latest).days
        for group, group_latest in group_latest_dates.items()
    }

    return FlowIncrementalRange(
        start=resolved_start,
        end=latest_price_date,
        latest_price_date=latest_price_date,
        group_latest_dates=group_latest_dates,
        group_lag_days=group_lag_days,
        excluded_groups=excluded,
        lookback_days=lookback_days,
        latest_flow_date=latest_flow_date,
    )


def _next_trading_day_after(anchor: date, end: date) -> date:
    if anchor >= end:
        return end + timedelta(days=1)
    trading_days = get_trading_days(anchor + timedelta(days=1), end)
    if not trading_days:
        return end + timedelta(days=1)
    return trading_days[0]


def sync_krx_security_flows(
    provider: FlowProvider,
    storage: Storage,
    start: date,
    end: date,
    tickers: list[str] | None = None,
    rate_limit_seconds: float = 0.2,
    progress_log_interval_seconds: float = DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS,
    progress_log_every_items: int = DEFAULT_PROGRESS_LOG_EVERY_ITEMS,
    randomize_request_order: bool = True,
    run_params_extra: dict[str, object] | None = None,
    enabled_flow_groups: list[str] | None = None,
) -> KrxFlowSyncResult:
    """Synchronise daily investor/shorting/ownership raw metrics."""
    provider_source = provider.source()
    enabled_groups = sorted(set(enabled_flow_groups or FLOW_METRIC_GROUPS.keys()))
    unknown_enabled_groups = sorted(set(enabled_groups) - set(FLOW_METRIC_GROUPS))
    if unknown_enabled_groups:
        supported = ", ".join(sorted(FLOW_METRIC_GROUPS))
        raise ValueError(
            f"Unknown enabled flow group(s): {', '.join(unknown_enabled_groups)} "
            f"(supported: {supported})"
        )
    run_params: dict[str, object] = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "tickers": tickers,
        "rate_limit_seconds": rate_limit_seconds,
        "progress_log_interval_seconds": progress_log_interval_seconds,
        "progress_log_every_items": progress_log_every_items,
        "randomize_request_order": randomize_request_order,
        "provider_source": provider_source.value,
        "enabled_flow_groups": enabled_groups,
    }
    if run_params_extra:
        run_params.update(run_params_extra)
    run = IngestionRun(
        run_type=RunType.KRX_FLOW_SYNC,
        started_at=now_kst(),
        status=RunStatus.RUNNING,
        params=run_params,
    )
    storage.record_run(run)

    result = KrxFlowSyncResult(pending_metrics=provider.unsupported_metric_codes())

    try:
        targets = _load_targets(storage, tickers)
        if not targets:
            raise RuntimeError("No active stocks found for the requested flow sync.")

        result.targets_processed = len(targets)
        trading_days = get_trading_days(start, end)
        if not trading_days:
            raise RuntimeError("No trading days found in the requested range.")

        stocks_by_market: dict[str, list[Stock]] = {}
        for stock in targets:
            stocks_by_market.setdefault(stock.market.value, []).append(stock)
        market_values = sorted(stocks_by_market)

        logger.info(
            "Flow sync started: range=%s..%s targets=%d trading_days=%d markets=%s "
            "source=%s rate_limit=%.3fs",
            start.isoformat(),
            end.isoformat(),
            len(targets),
            len(trading_days),
            ",".join(market_values),
            provider_source.value,
            rate_limit_seconds,
        )
        logger.info("Flow sync existing coverage check started.")

        target_tickers = [stock.ticker for stock in targets]
        foreign_ticker_counts = (
            storage.count_krx_security_flow_daily_market_tickers(
                start=start,
                end=end,
                tickers=target_tickers,
                metric_code=FOREIGN_HOLDING_METRIC,
                source=provider_source,
            )
            if "foreign_holding" in enabled_groups
            else {}
        )
        investor_metric_counts = (
            storage.count_krx_security_flow_ticker_metric_dates(
                start=start,
                end=end,
                tickers=target_tickers,
                metric_codes=INVESTOR_METRICS,
                source=provider_source,
            )
            if "investor" in enabled_groups
            else {}
        )
        shorting_metric_counts = (
            storage.count_krx_security_flow_ticker_metric_dates(
                start=start,
                end=end,
                tickers=target_tickers,
                metric_codes=SHORTING_METRICS,
                source=provider_source,
            )
            if "shorting" in enabled_groups
            else {}
        )
        investor_expected_count = len(trading_days) * len(INVESTOR_METRICS)
        shorting_expected_count = len(trading_days) * len(SHORTING_METRICS)
        completed_investor_tickers = {
            ticker
            for ticker, count in investor_metric_counts.items()
            if count >= investor_expected_count
        }
        completed_shorting_tickers = {
            ticker
            for ticker, count in shorting_metric_counts.items()
            if count >= shorting_expected_count
        }
        foreign_total_market_days = (
            len(trading_days) * len(stocks_by_market)
            if "foreign_holding" in enabled_groups
            else 0
        )
        foreign_completed_market_days = sum(
            1
            for trade_date in trading_days
            for market_stocks in stocks_by_market.values()
            if foreign_ticker_counts.get((trade_date, market_stocks[0].market.value), 0)
            >= len(market_stocks)
        )

        logger.info(
            "Flow sync existing coverage loaded: foreign_complete_market_days=%d/%d "
            "investor_complete_tickers=%d/%d shorting_complete_tickers=%d/%d",
            foreign_completed_market_days,
            foreign_total_market_days,
            len(completed_investor_tickers),
            len(targets),
            len(completed_shorting_tickers),
            len(targets),
        )

        progress = _FlowProgressLogger(
            interval_seconds=progress_log_interval_seconds,
            every_items=progress_log_every_items,
        )
        rng = random.SystemRandom()

        foreign_processed = 0
        foreign_work = (
            [
                (trade_date, market_stocks)
                for trade_date in trading_days
                for market_stocks in stocks_by_market.values()
            ]
            if "foreign_holding" in enabled_groups
            else []
        )
        if randomize_request_order and len(foreign_work) > 1:
            rng.shuffle(foreign_work)
        progress.start_phase(
            "foreign_holding",
            foreign_total_market_days,
            details=f"trading_days={len(trading_days)} markets={len(stocks_by_market)}",
        )
        for trade_date, market_stocks in foreign_work:
            market = market_stocks[0].market
            market_tickers = [stock.ticker for stock in market_stocks]
            current = f"{trade_date.isoformat()}:{market.value}"
            if foreign_ticker_counts.get((trade_date, market.value), 0) >= len(market_tickers):
                logger.debug(
                    "Skipping existing foreign holding flow request %s:%s",
                    trade_date.isoformat(),
                    market.value,
                )
                result.requests_skipped += 1
                foreign_processed += 1
                progress.tick(
                    processed=foreign_processed,
                    attempted=result.requests_attempted,
                    skipped=result.requests_skipped,
                    rows_upserted=result.rows_upserted,
                    no_data=result.no_data_requests,
                    errors=len(result.errors),
                    current=current,
                )
                continue

            result.requests_attempted += 1
            request_key = f"foreign:{trade_date.isoformat()}:{market.value}"
            logger.debug("Fetching flow request: request=%s", request_key)
            request_started_at = time.monotonic()
            foreign_result = call_with_retry(
                lambda: provider.fetch_foreign_holding_shares(
                    trade_date=trade_date,
                    market=market,
                    tickers=market_tickers,
                ),
                request_label=request_key,
                logger_instance=logger,
            )
            _log_flow_request_result(
                request_key,
                time.monotonic() - request_started_at,
                foreign_result,
            )
            if foreign_result.error:
                logger.warning(
                    "Foreign holding sync failed for %s: %s",
                    request_key,
                    foreign_result.error,
                )
                result.errors[request_key] = foreign_result.error
            elif foreign_result.no_data:
                result.no_data_requests += 1
            elif foreign_result.records:
                upsert = storage.upsert_krx_security_flow_raw(foreign_result.records)
                result.upsert.updated += upsert.updated
                result.upsert.errors += upsert.errors
                result.rows_upserted += upsert.updated
            sleep_with_jitter(rate_limit_seconds, jitter_ratio=0.4)
            foreign_processed += 1
            progress.tick(
                processed=foreign_processed,
                attempted=result.requests_attempted,
                skipped=result.requests_skipped,
                rows_upserted=result.rows_upserted,
                no_data=result.no_data_requests,
                errors=len(result.errors),
                current=current,
            )

        ticker_metric_groups = [
            group for group in ("investor", "shorting") if group in enabled_groups
        ]
        ticker_metric_total = len(targets) * len(ticker_metric_groups)
        ticker_metric_processed = 0
        ticker_metric_work = []
        if "investor" in enabled_groups:
            ticker_metric_work.extend(
                (stock, "investor", provider.fetch_investor_net_volume) for stock in targets
            )
        if "shorting" in enabled_groups:
            ticker_metric_work.extend(
                (stock, "shorting", provider.fetch_shorting_metrics) for stock in targets
            )
        if randomize_request_order and len(ticker_metric_work) > 1:
            rng.shuffle(ticker_metric_work)
        progress.start_phase(
            "ticker_metrics",
            ticker_metric_total,
            details=f"targets={len(targets)} metric_groups=2",
        )
        for stock, fetch_kind, fetch_fn in ticker_metric_work:
            if fetch_kind == "investor" and stock.ticker in completed_investor_tickers:
                logger.debug("Skipping existing investor flow request %s", stock.ticker)
                result.requests_skipped += 1
                ticker_metric_processed += 1
                progress.tick(
                    processed=ticker_metric_processed,
                    attempted=result.requests_attempted,
                    skipped=result.requests_skipped,
                    rows_upserted=result.rows_upserted,
                    no_data=result.no_data_requests,
                    errors=len(result.errors),
                    current=f"{fetch_kind}:{stock.ticker}",
                )
                continue
            if fetch_kind == "shorting" and stock.ticker in completed_shorting_tickers:
                logger.debug("Skipping existing shorting flow request %s", stock.ticker)
                result.requests_skipped += 1
                ticker_metric_processed += 1
                progress.tick(
                    processed=ticker_metric_processed,
                    attempted=result.requests_attempted,
                    skipped=result.requests_skipped,
                    rows_upserted=result.rows_upserted,
                    no_data=result.no_data_requests,
                    errors=len(result.errors),
                    current=f"{fetch_kind}:{stock.ticker}",
                )
                continue

            result.requests_attempted += 1
            request_key = f"{fetch_kind}:{stock.ticker}:{start.isoformat()}:{end.isoformat()}"
            logger.debug("Fetching flow request: request=%s", request_key)
            request_started_at = time.monotonic()
            fetch_result = call_with_retry(
                lambda: fetch_fn(stock.ticker, stock.market, start, end),
                request_label=request_key,
                logger_instance=logger,
            )
            _log_flow_request_result(
                request_key,
                time.monotonic() - request_started_at,
                fetch_result,
            )
            if fetch_result.error:
                logger.warning("Flow sync failed for %s: %s", request_key, fetch_result.error)
                result.errors[request_key] = fetch_result.error
            elif fetch_result.no_data:
                result.no_data_requests += 1
            elif fetch_result.records:
                upsert = storage.upsert_krx_security_flow_raw(fetch_result.records)
                result.upsert.updated += upsert.updated
                result.upsert.errors += upsert.errors
                result.rows_upserted += upsert.updated
            sleep_with_jitter(rate_limit_seconds, jitter_ratio=0.4)
            ticker_metric_processed += 1
            progress.tick(
                processed=ticker_metric_processed,
                attempted=result.requests_attempted,
                skipped=result.requests_skipped,
                rows_upserted=result.rows_upserted,
                no_data=result.no_data_requests,
                errors=len(result.errors),
                current=f"{fetch_kind}:{stock.ticker}",
            )

        complete_run(
            storage,
            run,
            counts=build_run_counts(
                targets_processed=result.targets_processed,
                requests_attempted=result.requests_attempted,
                requests_skipped=result.requests_skipped,
                rows_upserted=result.rows_upserted,
                no_data_requests=result.no_data_requests,
                pending_metric_count=len(result.pending_metrics),
            ),
            errors=result.errors,
            partial_subject="flow sync requests",
        )
        return result
    except Exception as exc:
        logger.exception("KRX flow sync failed")
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result
