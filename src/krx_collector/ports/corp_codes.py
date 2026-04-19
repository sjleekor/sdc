"""Port: OpenDART corporation-code provider interface."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from krx_collector.domain.models import DartCorpCodeResult


@runtime_checkable
class CorpCodeProvider(Protocol):
    """Fetches the OpenDART corporation-code master file."""

    def fetch_corp_codes(self) -> DartCorpCodeResult:
        """Retrieve all corporation-code rows from OpenDART."""
        ...
