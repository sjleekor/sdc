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
