"""Shared helpers for pipeline retries, throttling, and run finalization."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Mapping
from typing import Any

from krx_collector.domain.enums import RunStatus
from krx_collector.domain.models import IngestionRun
from krx_collector.ports.storage import Storage
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)


class OpenDartKeyExhaustedError(RuntimeError):
    """Raised when every configured OpenDART key has hit the daily limit."""


def is_opendart_daily_limit_exhausted(result: object) -> bool:
    """Return whether an OpenDART result means all keys are rate-limited."""
    return getattr(result, "exhaustion_reason", None) == "all_rate_limited"


def sleep_with_jitter(
    rate_limit_seconds: float,
    jitter_ratio: float = 0.2,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> None:
    """Sleep for ``rate_limit_seconds`` with small symmetric jitter."""
    if rate_limit_seconds <= 0:
        return

    jitter = random.uniform(-jitter_ratio, jitter_ratio) * rate_limit_seconds
    sleep_fn(max(0.0, rate_limit_seconds + jitter))


def should_retry_opendart_result(result: object) -> bool:
    """Standard OpenDART retry predicate for ``call_with_retry``.

    Retries transient per-call failures, but not ``all_rate_limited``. Once
    every configured key has hit the OpenDART limit, the scheduler should stop
    this run and continue on the next daily execution after OpenDART resets
    usage.
    """
    if is_opendart_daily_limit_exhausted(result):
        return False
    return bool(getattr(result, "retryable", False))


def call_with_retry[T](
    operation: Callable[[], T],
    *,
    request_label: str,
    max_attempts: int = 3,
    base_delay_seconds: float = 0.5,
    backoff_factor: float = 2.0,
    sleep_fn: Callable[[float], None] = time.sleep,
    logger_instance: logging.Logger | None = None,
    should_retry_result: Callable[[T], bool] | None = None,
) -> T:
    """Execute one provider call with retry on exceptions or ``result.error``."""
    active_logger = logger_instance or logger
    delay = base_delay_seconds
    last_exception: BaseException | None = None
    last_result: T | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            result = operation()
        except Exception as exc:
            last_exception = exc
            if attempt == max_attempts:
                raise
            active_logger.warning(
                "Request %s raised %s on attempt %d/%d; retrying in %.1fs",
                request_label,
                exc,
                attempt,
                max_attempts,
                delay,
            )
            sleep_fn(delay)
            delay *= backoff_factor
            continue

        last_result = result
        error = getattr(result, "error", None)
        retry_result = (
            should_retry_result(result) if should_retry_result is not None else bool(error)
        )
        if retry_result and attempt < max_attempts:
            retry_delay_seconds = getattr(result, "retry_after_seconds", None)
            effective_delay = (
                float(retry_delay_seconds)
                if isinstance(retry_delay_seconds, int | float) and retry_delay_seconds > 0
                else delay
            )
            active_logger.warning(
                "Request %s returned error on attempt %d/%d; retrying in %.1fs: %s",
                request_label,
                attempt,
                max_attempts,
                effective_delay,
                error,
            )
            sleep_fn(effective_delay)
            delay = max(delay * backoff_factor, effective_delay * backoff_factor)
            continue

        return result

    if last_result is not None:
        return last_result
    raise RuntimeError(
        f"Retry loop for {request_label} exhausted without returning a result."
    ) from last_exception


def extend_counts_with_errors(
    counts: Mapping[str, int],
    errors: Mapping[str, str],
) -> dict[str, int]:
    """Attach common partial-failure counters to pipeline audit counts."""
    result = dict(counts)
    error_count = len(errors)
    result["error_count"] = error_count
    result["partial_failure_count"] = error_count

    requests_attempted = result.get("requests_attempted")
    if requests_attempted is not None:
        result["completed_request_count"] = max(0, requests_attempted - error_count)

    return result


def summarize_errors(
    errors: Mapping[str, str],
    *,
    subject: str,
    sample_size: int = 3,
) -> str | None:
    """Build a compact error summary suitable for ``ingestion_runs``."""
    if not errors:
        return None

    sample_keys = ", ".join(list(errors)[:sample_size])
    suffix = f" Sample keys: {sample_keys}" if sample_keys else ""
    return f"{len(errors)} {subject} had errors.{suffix}"


def complete_run(
    storage: Storage,
    run: IngestionRun,
    *,
    counts: Mapping[str, int],
    errors: Mapping[str, str] | None = None,
    partial_subject: str = "requests",
) -> None:
    """Persist a finished run, using ``partial`` when recoverable errors exist."""
    normalized_errors = errors or {}
    run.ended_at = now_kst()
    run.counts = extend_counts_with_errors(counts, normalized_errors)
    run.status = RunStatus.PARTIAL if normalized_errors else RunStatus.SUCCESS
    run.error_summary = summarize_errors(normalized_errors, subject=partial_subject)
    storage.record_run(run)


def fail_run(storage: Storage, run: IngestionRun, exc: Exception) -> None:
    """Persist a terminal pipeline failure."""
    run.ended_at = now_kst()
    run.status = RunStatus.FAILED
    run.error_summary = str(exc)
    storage.record_run(run)


def build_run_counts(**counts: Any) -> dict[str, int]:
    """Normalize run-count payloads to integer-valued counters."""
    normalized: dict[str, int] = {}
    for key, value in counts.items():
        normalized[key] = int(value)
    return normalized
