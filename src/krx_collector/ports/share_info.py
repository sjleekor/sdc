"""Ports for share-count and shareholder-return disclosures."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from krx_collector.domain.models import (
    DartCorp,
    DartShareCountResult,
    DartShareholderReturnResult,
)


@runtime_checkable
class ShareCountProvider(Protocol):
    """Fetches OpenDART stock count disclosures."""

    def fetch_share_count(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareCountResult:
        """Retrieve stockTotqySttus raw rows for one company/report."""
        ...


@runtime_checkable
class ShareholderReturnProvider(Protocol):
    """Fetches dividend and treasury-stock disclosures."""

    def fetch_dividend(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareholderReturnResult:
        """Retrieve alotMatter rows for one company/report."""
        ...

    def fetch_treasury_stock(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareholderReturnResult:
        """Retrieve tesstkAcqsDspsSttus rows for one company/report."""
        ...
