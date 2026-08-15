"""Re-login when KRX drops the session out from under pykrx.

pykrx logs in once at import and re-logs in only when its own clock says the
session expired. KRX drops it sooner than that. Measured on the N3 snapshot
backfill (2026-08-15/16): a process worked for two to four minutes and then
every response came back empty, while pykrx kept sending dead cookies because
its clock still said the session was good. Two runs died that way, and the
slower-paced run got *fewer* rows before dying — so pacing was not the thing
being punished, process lifetime was. A fresh process succeeded every time.

The trigger cannot be a single empty response: ``dataframe_empty_handler``
swallows ``JSONDecodeError`` and returns an empty frame, so a dead session and a
delisted ticker with no rows are indistinguishable one call at a time. They are
distinguishable in aggregate — a dead session empties *everything*.
"""

from __future__ import annotations

import pytest

from krx_collector.adapters import pykrx_auth


@pytest.fixture(autouse=True)
def _fresh_state():
    pykrx_auth.reset_session_retry_state()
    yield
    pykrx_auth.reset_session_retry_state()


def _fetcher(results: list[object]):
    """Return (callable, calls) yielding `results` in order, repeating the last."""
    calls: list[int] = []

    def fetch() -> object:
        index = min(len(calls), len(results) - 1)
        calls.append(index)
        return results[index]

    return fetch, calls


def _call(fetch, label: str = "probe"):
    return pykrx_auth.call_with_session_retry(fetch, is_empty=lambda value: not value, label=label)


def test_a_good_response_never_touches_the_session(monkeypatch) -> None:
    refreshes = []
    monkeypatch.setattr(pykrx_auth, "refresh_pykrx_session", lambda: refreshes.append(1) or True)

    fetch, calls = _fetcher(["data"])

    assert _call(fetch) == "data"
    assert len(calls) == 1
    assert refreshes == []


def test_a_single_empty_response_is_not_enough_to_re_login(monkeypatch) -> None:
    # One empty is ordinary: a ticker with no rows in the requested range.
    refreshes = []
    monkeypatch.setattr(pykrx_auth, "refresh_pykrx_session", lambda: refreshes.append(1) or True)

    assert _call(_fetcher([[]])[0]) == []
    assert _call(_fetcher([[]])[0]) == []

    assert refreshes == []


def test_a_streak_of_empties_re_logs_in_and_retries(monkeypatch) -> None:
    refreshes = []
    monkeypatch.setattr(pykrx_auth, "refresh_pykrx_session", lambda: refreshes.append(1) or True)

    _call(_fetcher([[]])[0])
    _call(_fetcher([[]])[0])

    # Third consecutive empty: refresh, then the retry succeeds.
    fetch, calls = _fetcher([[], "data"])
    assert _call(fetch) == "data"

    assert refreshes == [1]
    assert len(calls) == 2


def test_a_success_resets_the_streak(monkeypatch) -> None:
    refreshes = []
    monkeypatch.setattr(pykrx_auth, "refresh_pykrx_session", lambda: refreshes.append(1) or True)

    _call(_fetcher([[]])[0])
    _call(_fetcher([[]])[0])
    _call(_fetcher(["data"])[0])
    _call(_fetcher([[]])[0])
    _call(_fetcher([[]])[0])

    assert refreshes == []


def test_a_refresh_that_does_not_help_makes_the_next_one_cost_more(monkeypatch) -> None:
    # The delisted-price backfill is mostly legitimate empties. Paying one login
    # per three tickers there would be worse than the problem being solved, so
    # a refresh that fails to produce data raises the bar fourfold.
    refreshes = []
    monkeypatch.setattr(pykrx_auth, "refresh_pykrx_session", lambda: refreshes.append(1) or True)

    for _ in range(3):
        _call(_fetcher([[]])[0])
    assert refreshes == [1]

    # Trigger is now 12, so the next 11 empties must not refresh.
    for _ in range(11):
        _call(_fetcher([[]])[0])
    assert refreshes == [1]

    _call(_fetcher([[]])[0])
    assert refreshes == [1, 1]


def test_a_refresh_that_helps_restores_the_original_trigger(monkeypatch) -> None:
    refreshes = []
    monkeypatch.setattr(pykrx_auth, "refresh_pykrx_session", lambda: refreshes.append(1) or True)

    # Raise the bar once with an unhelpful refresh.
    for _ in range(3):
        _call(_fetcher([[]])[0])
    assert refreshes == [1]

    # Now a refresh that does produce data.
    for _ in range(11):
        _call(_fetcher([[]])[0])
    assert _call(_fetcher([[], "data"])[0]) == "data"
    assert refreshes == [1, 1]

    # Back to a trigger of 3.
    for _ in range(3):
        _call(_fetcher([[]])[0])
    assert refreshes == [1, 1, 1]


def test_a_failed_refresh_returns_the_empty_result_rather_than_raising(monkeypatch) -> None:
    # Losing the credentials mid-run must not turn into a crash; the caller's
    # circuit breaker is what decides to stop.
    monkeypatch.setattr(pykrx_auth, "refresh_pykrx_session", lambda: False)

    for _ in range(2):
        _call(_fetcher([[]])[0])

    fetch, calls = _fetcher([[], "data"])
    assert _call(fetch) == []
    assert len(calls) == 1  # no retry after a failed refresh


def test_refresh_without_credentials_reports_failure(monkeypatch) -> None:
    monkeypatch.delenv("KRX_ID", raising=False)
    monkeypatch.delenv("KRX_PW", raising=False)
    monkeypatch.setattr(pykrx_auth, "configure_krx_credentials_from_settings", lambda: None)

    assert pykrx_auth.refresh_pykrx_session() is False


def test_a_failed_krx_login_is_reported_as_such_not_as_a_json_error(monkeypatch) -> None:
    """The import-time login failure has to name itself.

    `pykrx.website.comm.webio` logs into KRX at import time, so when KRX answers
    the login endpoint with an HTML page the whole library becomes unimportable.
    Raw, that surfaced as forty lines of traceback ending in "Expecting value:
    line 13 column 1", repeated once per target date, with nothing anywhere
    naming the cause. It took reading pykrx's source to work out that the
    collectors were fine and the login was down.
    """
    import builtins
    import json

    get_pykrx_stock_module = pykrx_auth.get_pykrx_stock_module
    get_pykrx_stock_module.cache_clear()
    monkeypatch.setattr(pykrx_auth, "configure_krx_credentials_from_settings", lambda: None)

    real_import = builtins.__import__

    def _import(name, *args, **kwargs):
        if name == "pykrx":
            raise json.JSONDecodeError("Expecting value", "<html>", 25)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _import)

    with pytest.raises(pykrx_auth.KrxLoginUnavailableError) as excinfo:
        get_pykrx_stock_module()

    message = str(excinfo.value)
    assert "KRX login returned a non-JSON response" in message
    assert "not a credential problem" in message
    assert isinstance(excinfo.value.__cause__, json.JSONDecodeError)

    get_pykrx_stock_module.cache_clear()
