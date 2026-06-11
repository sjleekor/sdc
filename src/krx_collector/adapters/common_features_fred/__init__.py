"""FRED common feature provider adapter."""

from krx_collector.adapters.common_features_fred.client import (
    FRED_SERIES_OBSERVATIONS_URL,
    FredSeriesObservationResult,
    FredSeriesObservationsClient,
)
from krx_collector.adapters.common_features_fred.provider import FredCommonFeatureProvider

__all__ = [
    "FRED_SERIES_OBSERVATIONS_URL",
    "FredCommonFeatureProvider",
    "FredSeriesObservationResult",
    "FredSeriesObservationsClient",
]
