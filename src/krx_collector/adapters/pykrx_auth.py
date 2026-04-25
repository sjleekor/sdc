"""Shared helpers for pykrx import-time KRX authentication."""

from __future__ import annotations

import contextlib
import io
import logging
from functools import lru_cache
from types import ModuleType

from krx_collector.infra.config.settings import configure_krx_credentials_from_settings

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_pykrx_stock_module() -> ModuleType:
    """Import pykrx.stock after loading KRX credentials, suppressing auth chatter."""
    configure_krx_credentials_from_settings()
    captured_output = io.StringIO()
    with contextlib.redirect_stdout(captured_output), contextlib.redirect_stderr(captured_output):
        from pykrx import stock

    output = captured_output.getvalue()
    if "KRX 로그인 실패" in output:
        logger.warning("pykrx KRX login failed; check KRX_ID/KRX_PW.")
    elif "KRX 로그인 완료" in output:
        logger.info("pykrx KRX login completed.")

    return stock
