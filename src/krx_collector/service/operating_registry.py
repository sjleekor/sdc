"""Registry for sector-specific operating metric extractors."""

from __future__ import annotations

from krx_collector.ports.operating_extractors import OperatingMetricExtractor


class OperatingMetricExtractorRegistry:
    """Maps sector keys to extractor implementations."""

    def __init__(self) -> None:
        self._extractors: dict[str, OperatingMetricExtractor] = {}

    def register(self, extractor: OperatingMetricExtractor) -> None:
        self._extractors[extractor.sector_key] = extractor

    def get(self, sector_key: str) -> OperatingMetricExtractor | None:
        return self._extractors.get(sector_key)

    def supported_sector_keys(self) -> list[str]:
        return sorted(self._extractors)
