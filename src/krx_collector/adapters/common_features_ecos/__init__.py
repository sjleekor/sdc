"""Bank of Korea ECOS common feature provider adapter."""

from krx_collector.adapters.common_features_ecos.client import (
    ECOS_STATISTIC_SEARCH_BASE_URL,
    EcosStatisticSearchClient,
    EcosStatisticSearchResult,
)
from krx_collector.adapters.common_features_ecos.provider import EcosCommonFeatureProvider

__all__ = [
    "ECOS_STATISTIC_SEARCH_BASE_URL",
    "EcosCommonFeatureProvider",
    "EcosStatisticSearchClient",
    "EcosStatisticSearchResult",
]
