"""Shared helpers for pykrx import-time KRX authentication.

Session lifetime
----------------
pykrx logs into KRX once at module import and reuses that session. It re-logs in
only when its own clock says the session expired (one hour, minus a five-minute
buffer). It has no notion of the server having dropped the session first.

KRX does drop it first. Measured 2026-08-15/16 on the N3 snapshot backfill: a
process would work for two to four minutes and then every response would come
back as an error page — ``JSONDecodeError`` on a JSON endpoint, or an empty
ticker list — while pykrx kept sending the dead cookies because its clock still
said the session was good. Two runs died this way, and the slower-paced one got
*fewer* rows before dying, so the pace was not what was being punished. A fresh
process succeeded every time, because a fresh process logs in again.

So the retry has to key on the response, not on the clock. Both ``Get`` and
``Post`` in ``pykrx.website.comm.webio`` resolve their session through the
module-global the auth module owns, and ``KRXSession.refresh`` mutates that
object in place, so re-logging in there fixes every pykrx call at once.
"""

from __future__ import annotations

import contextlib
import io
import logging
import os
from collections.abc import Callable
from functools import lru_cache
from types import ModuleType

from krx_collector.infra.config.settings import configure_krx_credentials_from_settings

logger = logging.getLogger(__name__)


class KrxLoginUnavailableError(RuntimeError):
    """KRX's login endpoint did not answer with JSON, so pykrx cannot be used."""


@lru_cache(maxsize=1)
def get_pykrx_stock_module() -> ModuleType:
    """Import pykrx.stock after loading KRX credentials, suppressing auth chatter.

    Raises:
        KrxLoginUnavailableError: KRX's login endpoint returned something other
            than JSON. ``pykrx.website.comm.webio`` logs in at *import* time, so
            this takes the whole library down, not one call.
    """
    configure_krx_credentials_from_settings()
    captured_output = io.StringIO()
    try:
        with (
            contextlib.redirect_stdout(captured_output),
            contextlib.redirect_stderr(captured_output),
        ):
            from pykrx import stock
    except ValueError as exc:
        # requests raises JSONDecodeError, a ValueError subclass, from
        # `resp.json()` inside login_krx. Unwrapped, this surfaces as forty
        # lines of traceback ending in "Expecting value: line 13 column 1",
        # repeated once per target, with nothing naming the actual cause —
        # which is what a KRX maintenance window looks like from in here.
        # Observed 2026-08-16 from roughly 23:54 KST onward.
        raise KrxLoginUnavailableError(
            "KRX login returned a non-JSON response, so `import pykrx` fails and "
            "every pykrx collector is blocked. This is an upstream condition "
            "(maintenance window or a block), not a credential problem — verify "
            "with a KRX MDC collector, which authenticates separately. Re-run "
            f"when KRX answers again. Underlying error: {exc}"
        ) from exc

    output = captured_output.getvalue()
    if "KRX 로그인 실패" in output:
        logger.warning("pykrx KRX login failed; check KRX_ID/KRX_PW.")
    elif "KRX 로그인 완료" in output:
        logger.info("pykrx KRX login completed.")

    return stock


def refresh_pykrx_session() -> bool:
    """Force pykrx to log in to KRX again, regardless of its expiry clock.

    Returns:
        ``True`` when a new session was established.
    """
    configure_krx_credentials_from_settings()
    login_id = os.getenv("KRX_ID")
    login_pw = os.getenv("KRX_PW")
    if not (login_id and login_pw):
        logger.warning("Cannot refresh the KRX session: KRX_ID/KRX_PW are not set.")
        return False

    try:
        from pykrx.website.comm import auth
    except ImportError:  # pragma: no cover - pykrx is a hard dependency
        logger.exception("pykrx auth module is unavailable")
        return False

    session = auth.get_auth_session()
    if session is None:
        logger.warning("pykrx has no KRX session to refresh.")
        return False

    captured_output = io.StringIO()
    with contextlib.redirect_stdout(captured_output), contextlib.redirect_stderr(captured_output):
        refreshed = bool(session.refresh(login_id, login_pw))

    if refreshed:
        logger.info("KRX session re-established after a failed response.")
    else:
        logger.warning("KRX session refresh failed.")
    return refreshed


# A dropped session cannot be told apart from "no data" by exception, because
# pykrx's `dataframe_empty_handler` swallows JSONDecodeError and returns an
# empty frame. It CAN be told apart by clustering: a dead session empties every
# call, while genuinely absent data is scattered. So the trigger is consecutive
# empties, not the first one.
_INITIAL_EMPTY_STREAK_TRIGGER = 3
_MAX_EMPTY_STREAK_TRIGGER = 200

_consecutive_empty = 0
_empty_streak_trigger = _INITIAL_EMPTY_STREAK_TRIGGER


def reset_session_retry_state() -> None:
    """Forget the empty-streak state. For tests and for starting a fresh run."""
    global _consecutive_empty, _empty_streak_trigger
    _consecutive_empty = 0
    _empty_streak_trigger = _INITIAL_EMPTY_STREAK_TRIGGER


def call_with_session_retry[T](
    fetch: Callable[[], T],
    *,
    is_empty: Callable[[T], bool],
    label: str,
) -> T:
    """Run *fetch*, re-logging in and retrying when empties start clustering.

    The backoff is on the *trigger*, not on the call. If a refresh does not turn
    the empty into data, the session was fine and the data is genuinely absent,
    so the streak required to try again grows fourfold. A run of nothing but
    legitimate empties — the delisted-ticker price backfill, where most tickers
    have no rows in most ranges — therefore pays at most a handful of wasted
    logins instead of one per few tickers.

    Args:
        fetch: The pykrx call, already bound to its arguments.
        is_empty: Whether a result counts as empty.
        label: What is being fetched, for the log line.

    Returns:
        The retried result when a retry happened, otherwise the first result.
    """
    global _consecutive_empty, _empty_streak_trigger

    result = fetch()
    if not is_empty(result):
        _consecutive_empty = 0
        _empty_streak_trigger = _INITIAL_EMPTY_STREAK_TRIGGER
        return result

    _consecutive_empty += 1
    if _consecutive_empty < _empty_streak_trigger:
        return result

    logger.warning(
        "pykrx %s: %d consecutive empty responses — refreshing the KRX session and retrying.",
        label,
        _consecutive_empty,
    )
    _consecutive_empty = 0
    if not refresh_pykrx_session():
        return result

    retried = fetch()
    if is_empty(retried):
        # The session was not the problem. Make the next attempt cost more.
        _empty_streak_trigger = min(_empty_streak_trigger * 4, _MAX_EMPTY_STREAK_TRIGGER)
        logger.info(
            "pykrx %s: still empty after re-login, so the data is absent; "
            "next refresh needs %d consecutive empties.",
            label,
            _empty_streak_trigger,
        )
    else:
        _empty_streak_trigger = _INITIAL_EMPTY_STREAK_TRIGGER
    return retried
