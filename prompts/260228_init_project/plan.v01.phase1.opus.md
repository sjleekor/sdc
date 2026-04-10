# Phase 1 — Foundation & Infrastructure: Ultra-Detailed Implementation Plan

> **Purpose**: This document provides extremely granular, copy-paste-ready instructions
> so that even a lower-capability coding agent can execute Phase 1 without ambiguity.
> Every file path, every function signature, every expected output is spelled out.

---

## Pre-requisites & Current State

Before starting, understand what **already exists** and what **needs to be done**:

| Component | File | Status |
|---|---|---|
| Settings | `src/krx_collector/infra/config/settings.py` | ✅ Fully implemented |
| DB Connection | `src/krx_collector/infra/db_postgres/connection.py` | ❌ Stub — raises `NotImplementedError` |
| Trading Days | `src/krx_collector/infra/calendar/trading_days.py` | ✅ Fully implemented |
| Holidays CSV | `docs/holidays_krx.csv` | ❌ Does not exist yet |
| Logging Setup | `src/krx_collector/infra/logging/setup.py` | ✅ Fully implemented |
| Retry Decorator | `src/krx_collector/util/retry.py` | ✅ Fully implemented |
| Time Utilities | `src/krx_collector/util/time.py` | ✅ Fully implemented |
| CLI (logging wiring) | `src/krx_collector/cli/app.py` | ✅ Already calls `setup_logging()` |
| Unit tests | `tests/unit/test_placeholder.py` | Only placeholder exists |
| Integration tests | `tests/integration/test_placeholder.py` | Only placeholder exists |

**Summary of work**:
1. Implement `connection.py` (replace the stub with real psycopg2 pooling)
2. Create `docs/holidays_krx.csv` (holiday master data)
3. Create 3 test files: `test_db_connection.py`, `test_calendar.py`, `test_retry.py`
4. Verify logging works (no code change needed — already wired)

---

## Task 1: Implement DB Connection Pool

### 1.1 Edit file: `src/krx_collector/infra/db_postgres/connection.py`

**Action**: Replace the entire file content with the implementation below.

**Complete file content** (replace everything from line 1 to line 49):

```python
"""PostgreSQL connection management with thread-safe connection pooling.

Provides a thin wrapper around ``psycopg2`` using
``ThreadedConnectionPool`` for obtaining and releasing database connections.

Usage::

    from krx_collector.infra.db_postgres.connection import get_connection

    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Generator
from typing import Any

import psycopg2
import psycopg2.pool

logger = logging.getLogger(__name__)

# Module-level connection pool (singleton).
_POOL: psycopg2.pool.ThreadedConnectionPool | None = None


def _get_pool(dsn: str) -> psycopg2.pool.ThreadedConnectionPool:
    """Return the global connection pool, creating it on first call.

    Args:
        dsn: PostgreSQL connection string.

    Returns:
        The singleton ``ThreadedConnectionPool``.
    """
    global _POOL
    if _POOL is None or _POOL.closed:
        logger.info("Creating new connection pool for DSN: %s", dsn[:dsn.rfind("@") + 1] + "***")
        _POOL = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=dsn,
        )
    return _POOL


@contextlib.contextmanager
def get_connection(dsn: str) -> Generator[Any, None, None]:
    """Yield a ``psycopg2`` connection, committing on success / rolling back on error.

    The connection is obtained from a ``ThreadedConnectionPool`` and returned
    to the pool in the ``finally`` block.

    Args:
        dsn: PostgreSQL connection string
            (e.g. ``postgresql://user:pass@host:5432/db``).

    Yields:
        A ``psycopg2.extensions.connection`` object.
    """
    pool = _get_pool(dsn)
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)
```

### 1.2 Verification checklist for Task 1

After editing, confirm:
- [ ] The file imports `psycopg2` and `psycopg2.pool` (no `NotImplementedError` anywhere).
- [ ] `_POOL` is a module-level variable initialized to `None`.
- [ ] `_get_pool(dsn)` creates a `ThreadedConnectionPool` with `minconn=1, maxconn=10`.
- [ ] `get_connection(dsn)` is a context manager that: gets conn from pool → yields → commits on success → rollback on exception → putconn in finally.

---

## Task 2: Create Holidays Master Data CSV

### 2.1 Create file: `docs/holidays_krx.csv`

**Action**: Create a new file at path `docs/holidays_krx.csv` with the exact content below.

```csv
date,name
2024-01-01,New Year's Day
2024-02-09,Lunar New Year
2024-02-10,Lunar New Year
2024-02-11,Lunar New Year
2024-02-12,Lunar New Year (Substitute)
2024-03-01,Samiljeol
2024-04-10,General Election
2024-05-01,Labor Day
2024-05-06,Children's Day (Substitute)
2024-05-15,Buddha's Birthday
2024-06-06,Memorial Day
2024-08-15,Liberation Day
2024-09-16,Chuseok
2024-09-17,Chuseok
2024-09-18,Chuseok
2024-10-03,National Foundation Day
2024-10-09,Hangeul Day
2024-12-25,Christmas Day
2024-12-31,Market Closing Day
2025-01-01,New Year's Day
2025-01-28,Lunar New Year
2025-01-29,Lunar New Year
2025-01-30,Lunar New Year
2025-03-01,Samiljeol
2025-03-03,Samiljeol (Substitute)
2025-05-01,Labor Day
2025-05-05,Children's Day
2025-05-06,Buddha's Birthday
2025-06-06,Memorial Day
2025-08-15,Liberation Day
2025-10-03,National Foundation Day
2025-10-05,Chuseok
2025-10-06,Chuseok
2025-10-07,Chuseok
2025-10-08,Chuseok (Substitute)
2025-10-09,Hangeul Day
2025-12-25,Christmas Day
```

### 2.2 Verification checklist for Task 2

- [ ] File exists at `docs/holidays_krx.csv`.
- [ ] First line is exactly `date,name` (CSV header).
- [ ] All dates use `YYYY-MM-DD` format.
- [ ] Includes both 2024 and 2025 holidays.

---

## Task 3: Create Unit Test — Calendar / Trading Days

### 3.1 Create file: `tests/unit/test_calendar.py`

**Action**: Create a new file at path `tests/unit/test_calendar.py` with the exact content below.

```python
"""Unit tests for krx_collector.infra.calendar.trading_days."""

from datetime import date
from pathlib import Path

from krx_collector.infra.calendar.trading_days import get_trading_days, load_holidays


class TestLoadHolidays:
    """Tests for load_holidays()."""

    def test_load_holidays_from_existing_file(self) -> None:
        """Loading the project's holidays CSV should return a non-empty set."""
        holidays = load_holidays(Path("docs/holidays_krx.csv"))
        assert len(holidays) > 0
        # Spot-check: 2024-01-01 (New Year) should be present
        assert date(2024, 1, 1) in holidays

    def test_load_holidays_missing_file_returns_empty(self, tmp_path: Path) -> None:
        """When the CSV does not exist, return an empty set (no crash)."""
        holidays = load_holidays(tmp_path / "nonexistent.csv")
        assert holidays == set()


class TestGetTradingDays:
    """Tests for get_trading_days()."""

    def test_new_year_excluded(self) -> None:
        """2024-01-01 (New Year, Monday) must be excluded from trading days."""
        holidays = load_holidays(Path("docs/holidays_krx.csv"))
        result = get_trading_days(date(2024, 1, 1), date(2024, 1, 5), holidays=holidays)
        # 2024-01-01 is Mon (holiday), 01-02 Tue, 01-03 Wed, 01-04 Thu, 01-05 Fri
        assert date(2024, 1, 1) not in result
        assert date(2024, 1, 2) in result
        assert date(2024, 1, 3) in result
        assert date(2024, 1, 4) in result
        assert date(2024, 1, 5) in result
        assert len(result) == 4

    def test_weekends_excluded(self) -> None:
        """Saturday and Sunday should never appear in trading days."""
        # 2024-01-06 is Saturday, 2024-01-07 is Sunday
        holidays: set[date] = set()
        result = get_trading_days(date(2024, 1, 6), date(2024, 1, 7), holidays=holidays)
        assert result == []

    def test_full_week_no_holidays(self) -> None:
        """A full Mon-Fri week with no holidays should return 5 days."""
        # 2024-01-08 (Mon) to 2024-01-12 (Fri) — no holidays this week
        holidays: set[date] = set()
        result = get_trading_days(date(2024, 1, 8), date(2024, 1, 12), holidays=holidays)
        assert len(result) == 5

    def test_holidays_none_loads_default(self) -> None:
        """When holidays=None, the function loads from default CSV path."""
        # This test just verifies it runs without error.
        # It may use the real CSV if present, or weekends-only if not.
        result = get_trading_days(date(2024, 1, 2), date(2024, 1, 3), holidays=None)
        assert isinstance(result, list)

    def test_empty_range(self) -> None:
        """When start > end, return an empty list."""
        holidays: set[date] = set()
        result = get_trading_days(date(2024, 1, 5), date(2024, 1, 1), holidays=holidays)
        assert result == []
```

### 3.2 Run the test

Execute this command from the project root:

```bash
cd /Users/whishaw/wss_p/stock_data_collector && python -m pytest tests/unit/test_calendar.py -v
```

**Expected output**: All 6 tests pass (5 in `TestGetTradingDays` + 1 in `TestLoadHolidays` that loads the CSV + 1 for missing file).

---

## Task 4: Create Unit Test — Retry Decorator

### 4.1 Create file: `tests/unit/test_retry.py`

**Action**: Create a new file at path `tests/unit/test_retry.py` with the exact content below.

```python
"""Unit tests for krx_collector.util.retry."""

import pytest

from krx_collector.util.retry import retry


class TestRetry:
    """Tests for the retry decorator."""

    def test_success_on_first_attempt(self) -> None:
        """Function succeeds immediately — no retry needed."""
        calls = 0

        @retry(max_attempts=3, base_delay=0.01)
        def good_func() -> str:
            nonlocal calls
            calls += 1
            return "ok"

        assert good_func() == "ok"
        assert calls == 1

    def test_success_after_transient_failure(self) -> None:
        """Function fails once then succeeds — should return the successful result."""
        calls = 0

        @retry(max_attempts=3, base_delay=0.01)
        def flaky_func() -> str:
            nonlocal calls
            calls += 1
            if calls < 2:
                raise ValueError("transient")
            return "recovered"

        assert flaky_func() == "recovered"
        assert calls == 2

    def test_exhaustion_raises_last_exception(self) -> None:
        """All attempts fail — should raise the original exception."""

        @retry(max_attempts=2, base_delay=0.01)
        def always_fails() -> None:
            raise ValueError("permanent")

        with pytest.raises(ValueError, match="permanent"):
            always_fails()

    def test_specific_exception_filter(self) -> None:
        """Only the specified exception type triggers a retry."""
        calls = 0

        @retry(max_attempts=3, base_delay=0.01, exceptions=(TypeError,))
        def wrong_exception() -> None:
            nonlocal calls
            calls += 1
            raise ValueError("not retryable")

        with pytest.raises(ValueError):
            wrong_exception()

        # Should have been called only once — ValueError is not in the retry list
        assert calls == 1

    def test_backoff_increases_delay(self) -> None:
        """Verify the function is called the expected number of times with backoff."""
        calls = 0

        @retry(max_attempts=3, base_delay=0.01, backoff_factor=2.0)
        def fail_twice() -> str:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise RuntimeError("not yet")
            return "done"

        assert fail_twice() == "done"
        assert calls == 3
```

### 4.2 Run the test

```bash
cd /Users/whishaw/wss_p/stock_data_collector && python -m pytest tests/unit/test_retry.py -v
```

**Expected output**: All 5 tests pass.

---

## Task 5: Create Integration Test — DB Connection

### 5.1 Create file: `tests/integration/test_db_connection.py`

**Action**: Create a new file at path `tests/integration/test_db_connection.py` with the exact content below.

> **Note**: This test requires a running PostgreSQL instance. It will be skipped automatically
> if the database is not reachable — this is by design so that unit tests can still pass in CI
> without a DB.

```python
"""Integration test for database connectivity.

Requires a running PostgreSQL instance. Skipped automatically if the
database is unreachable.
"""

import pytest

from krx_collector.infra.config.settings import get_settings
from krx_collector.infra.db_postgres.connection import get_connection


@pytest.fixture()
def db_dsn() -> str:
    """Return the DSN from settings."""
    return get_settings().db_dsn


def test_database_select_one(db_dsn: str) -> None:
    """Verify basic connectivity: execute SELECT 1 and check the result."""
    try:
        with get_connection(db_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                result = cur.fetchone()
                assert result is not None
                assert result[0] == 1
    except Exception as exc:
        pytest.skip(f"Database not reachable: {exc}")


def test_connection_rollback_on_error(db_dsn: str) -> None:
    """Verify that an exception inside the context triggers a rollback (not a commit)."""
    try:
        with pytest.raises(ZeroDivisionError):
            with get_connection(db_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                raise ZeroDivisionError("intentional")
    except Exception as exc:
        pytest.skip(f"Database not reachable: {exc}")
```

### 5.2 Run the test

```bash
cd /Users/whishaw/wss_p/stock_data_collector && python -m pytest tests/integration/test_db_connection.py -v
```

**Expected output**:
- If PostgreSQL is running and `.env` has valid credentials: 2 tests pass.
- If PostgreSQL is NOT running: 2 tests skipped (not failed).

---

## Task 6: Verify Logging Configuration (No Code Changes Needed)

### 6.1 Explanation

The logging infrastructure is **already fully wired**:
- `src/krx_collector/infra/logging/setup.py` implements `setup_logging()` with plain and JSON formatters.
- `src/krx_collector/cli/app.py` line 202-207 already calls `setup_logging()` using values from `Settings`.

**No code changes are required.** Just verify it works.

### 6.2 Manual Verification Steps

**Step A** — Verify plain text logging (default):

```bash
cd /Users/whishaw/wss_p/stock_data_collector && python -m krx_collector universe sync 2>&1 | head -5
```

Expected: You should see a log line like:
```
YYYY-MM-DD HH:MM:SS [WARNING ] ... Command not yet implemented: ...
```
(The `NotImplementedError` is expected — we're only checking log format.)

**Step B** — Verify JSON logging:

```bash
cd /Users/whishaw/wss_p/stock_data_collector && LOG_FORMAT=json python -m krx_collector universe sync 2>&1 | head -5
```

Expected: You should see a JSON log line like:
```json
{"ts": "...", "level": "WARNING", "logger": "...", "message": "Command not yet implemented: ..."}
```

---

## Task 7: Run All Tests Together

### 7.1 Execute full test suite

```bash
cd /Users/whishaw/wss_p/stock_data_collector && python -m pytest tests/unit/ -v
```

**Expected**: All unit tests pass (test_calendar.py + test_retry.py).

```bash
cd /Users/whishaw/wss_p/stock_data_collector && python -m pytest tests/ -v
```

**Expected**: All tests pass (unit tests pass, integration tests pass or skip).

---

## Definition of Done — Phase 1 Checklist

All of the following must be true before Phase 1 is considered complete:

| # | Criterion | How to verify |
|---|---|---|
| 1 | `connection.py` implements real psycopg2 pooling | Open the file — no `NotImplementedError`, has `ThreadedConnectionPool` |
| 2 | `docs/holidays_krx.csv` exists with 2024+2025 data | `wc -l docs/holidays_krx.csv` returns > 20 lines |
| 3 | `pytest tests/unit/test_calendar.py -v` — all pass | Run command, 0 failures |
| 4 | `pytest tests/unit/test_retry.py -v` — all pass | Run command, 0 failures |
| 5 | `pytest tests/integration/test_db_connection.py -v` — pass or skip | Run command, 0 failures (skips OK) |
| 6 | JSON logging works | `LOG_FORMAT=json python -m krx_collector universe sync` shows JSON |
| 7 | No existing tests broken | `pytest tests/ -v` — 0 failures |

---

## File Change Summary

| Action | File Path |
|---|---|
| **REPLACE** (full rewrite) | `src/krx_collector/infra/db_postgres/connection.py` |
| **CREATE** (new) | `docs/holidays_krx.csv` |
| **CREATE** (new) | `tests/unit/test_calendar.py` |
| **CREATE** (new) | `tests/unit/test_retry.py` |
| **CREATE** (new) | `tests/integration/test_db_connection.py` |
| No change needed | `src/krx_collector/infra/logging/setup.py` |
| No change needed | `src/krx_collector/cli/app.py` |
| No change needed | `src/krx_collector/util/retry.py` |
| No change needed | `src/krx_collector/infra/calendar/trading_days.py` |

**Total files to modify: 1** | **Total files to create: 4** | **Total files unchanged: 5**
