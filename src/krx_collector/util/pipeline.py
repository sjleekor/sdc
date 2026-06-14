"""Shared helpers for pipeline retries, throttling, and run finalization."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any

from krx_collector.domain.enums import RunStatus, RunType
from krx_collector.domain.models import IngestionRun
from krx_collector.ports.storage import Storage
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class HumanThrottlePolicy:
    """Throttle policy for KRX-facing HTTP requests."""

    min_delay_seconds: float = 0.0
    max_delay_seconds: float = 0.0
    long_rest_every: int = 0
    long_rest_min_seconds: float = 0.0
    long_rest_max_seconds: float = 0.0
    auth_cooldown_seconds: float = 0.0
    error_backoff_min_seconds: float = 0.0
    error_backoff_max_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.min_delay_seconds < 0 or self.max_delay_seconds < 0:
            raise ValueError("HumanThrottlePolicy delays must be non-negative.")
        if self.min_delay_seconds > self.max_delay_seconds:
            raise ValueError("HumanThrottlePolicy min_delay_seconds must be <= max_delay_seconds.")
        if self.long_rest_every < 0:
            raise ValueError("HumanThrottlePolicy long_rest_every must be non-negative.")
        if self.long_rest_min_seconds < 0 or self.long_rest_max_seconds < 0:
            raise ValueError("HumanThrottlePolicy long rest durations must be non-negative.")
        if self.long_rest_min_seconds > self.long_rest_max_seconds:
            raise ValueError(
                "HumanThrottlePolicy long_rest_min_seconds must be <= long_rest_max_seconds."
            )
        if self.auth_cooldown_seconds < 0:
            raise ValueError("HumanThrottlePolicy auth_cooldown_seconds must be non-negative.")
        if self.error_backoff_min_seconds < 0 or self.error_backoff_max_seconds < 0:
            raise ValueError("HumanThrottlePolicy error backoff durations must be non-negative.")
        if self.error_backoff_min_seconds > self.error_backoff_max_seconds:
            raise ValueError(
                "HumanThrottlePolicy error_backoff_min_seconds must be <= "
                "error_backoff_max_seconds."
            )

    def enabled(self) -> bool:
        return any(
            (
                self.max_delay_seconds > 0,
                self.long_rest_every > 0 and self.long_rest_max_seconds > 0,
                self.auth_cooldown_seconds > 0,
                self.error_backoff_max_seconds > 0,
            )
        )


class HumanThrottle:
    """Stateful human-like request throttling."""

    def __init__(
        self,
        policy: HumanThrottlePolicy,
        *,
        sleep_fn: Callable[[float], None] = time.sleep,
        monotonic_fn: Callable[[], float] = time.monotonic,
        rng: random.Random | None = None,
        logger_instance: logging.Logger | None = None,
    ) -> None:
        self._policy = policy
        self._sleep_fn = sleep_fn
        self._monotonic_fn = monotonic_fn
        self._rng = rng or random.Random()
        self._logger = logger_instance or logger
        self._completed_requests = 0
        self._last_request_finished_at: float | None = None

    def before_request(self, label: str) -> None:
        if not self._policy.enabled():
            return

        if (
            self._policy.long_rest_every > 0
            and self._completed_requests > 0
            and self._completed_requests % self._policy.long_rest_every == 0
        ):
            self._sleep_random(
                self._policy.long_rest_min_seconds,
                self._policy.long_rest_max_seconds,
                reason=f"KRX long rest before {label}",
            )

        self._sleep_to_spacing(
            self._policy.min_delay_seconds,
            self._policy.max_delay_seconds,
            reason=f"KRX request spacing before {label}",
        )

    def after_request(self) -> None:
        self._completed_requests += 1
        self._last_request_finished_at = self._monotonic_fn()

    def cooldown_after_auth(self, label: str) -> None:
        if self._policy.auth_cooldown_seconds <= 0:
            return
        self._sleep_exact(
            self._policy.auth_cooldown_seconds,
            reason=f"KRX auth cooldown after {label}",
        )

    def backoff_after_error(self, label: str) -> None:
        if self._policy.error_backoff_max_seconds <= 0:
            return
        self._sleep_random(
            self._policy.error_backoff_min_seconds,
            self._policy.error_backoff_max_seconds,
            reason=f"KRX error backoff after {label}",
        )

    def _sleep_to_spacing(self, minimum: float, maximum: float, *, reason: str) -> None:
        if maximum <= 0:
            return

        target_spacing = self._rng.uniform(minimum, maximum)
        if self._last_request_finished_at is None:
            sleep_for = target_spacing
        else:
            elapsed = self._monotonic_fn() - self._last_request_finished_at
            sleep_for = max(0.0, target_spacing - elapsed)
        self._sleep_exact(sleep_for, reason=reason)

    def _sleep_random(self, minimum: float, maximum: float, *, reason: str) -> None:
        if maximum <= 0:
            return
        self._sleep_exact(self._rng.uniform(minimum, maximum), reason=reason)

    def _sleep_exact(self, seconds: float, *, reason: str) -> None:
        if seconds <= 0:
            return
        self._logger.debug("%s: sleeping %.2fs", reason, seconds)
        self._sleep_fn(seconds)
        self._last_request_finished_at = self._monotonic_fn()


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


def record_terminal_run(
    storage: Storage,
    *,
    run_type: RunType,
    status: RunStatus,
    params: Mapping[str, Any],
    counts: Mapping[str, int] | None = None,
    error_summary: str | None = None,
) -> IngestionRun:
    """Persist a run that finishes before the normal service path starts."""
    now = now_kst()
    run = IngestionRun(
        run_type=run_type,
        started_at=now,
        ended_at=now,
        status=status,
        params=_jsonable_mapping(params),
        counts=dict(counts or {}),
        error_summary=error_summary,
    )
    storage.record_run(run)
    return run


def _jsonable_mapping(values: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _jsonable(value) for key, value in values.items()}


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple | set):
        return [_jsonable(item) for item in value]
    return value


def build_run_counts(**counts: Any) -> dict[str, int]:
    """Normalize run-count payloads to integer-valued counters."""
    normalized: dict[str, int] = {}
    for key, value in counts.items():
        normalized[key] = int(value)
    return normalized
