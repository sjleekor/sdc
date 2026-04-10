"""Domain enumerations for the KRX data pipeline.

These enums represent core business concepts and are used throughout the
domain models, ports, and adapters.
"""

from enum import StrEnum


class Market(StrEnum):
    """Korean stock exchange market segment."""

    KOSPI = "KOSPI"
    KOSDAQ = "KOSDAQ"


class Source(StrEnum):
    """Data source identifier.

    Only FDR and PYKRX are implemented today. KIS and KIWOOM are reserved
    for future broker-API integrations.
    """

    FDR = "FDR"
    PYKRX = "PYKRX"
    # Future sources (not implemented):
    # KIS = "KIS"
    # KIWOOM = "KIWOOM"


class ListingStatus(StrEnum):
    """Stock listing status on KRX."""

    ACTIVE = "ACTIVE"
    DELISTED = "DELISTED"
    UNKNOWN = "UNKNOWN"


class RunType(StrEnum):
    """Pipeline run type recorded in ingestion_runs."""

    UNIVERSE_SYNC = "universe_sync"
    DAILY_BACKFILL = "daily_backfill"
    VALIDATE = "validate"


class RunStatus(StrEnum):
    """Pipeline run execution status."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
