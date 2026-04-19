"""Factory for the default operating KPI extractor registry."""

from __future__ import annotations

from krx_collector.adapters.operating_extractors.shipbuilding_defense_order import (
    ShipbuildingDefenseOrderExtractor,
)
from krx_collector.service.operating_registry import OperatingMetricExtractorRegistry


def build_default_operating_registry() -> OperatingMetricExtractorRegistry:
    registry = OperatingMetricExtractorRegistry()
    registry.register(ShipbuildingDefenseOrderExtractor())
    return registry
