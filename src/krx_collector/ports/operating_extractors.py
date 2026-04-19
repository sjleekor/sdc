"""Port: sector-specific operating metric extractors."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from krx_collector.domain.models import OperatingMetricExtractionResult, OperatingSourceDocument


@runtime_checkable
class OperatingMetricExtractor(Protocol):
    """Extract sector-specific KPI facts from one source document."""

    @property
    def sector_key(self) -> str:
        """Sector key supported by this extractor."""
        ...

    @property
    def extractor_code(self) -> str:
        """Stable extractor version/code."""
        ...

    def extract(self, document: OperatingSourceDocument) -> OperatingMetricExtractionResult:
        """Extract zero or more facts from the document."""
        ...
