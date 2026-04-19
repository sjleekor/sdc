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

    FDR and PYKRX are implemented today. OPENDART and KRX are reserved
    for the upcoming account/flow ingestion phases. KIS and KIWOOM remain
    reserved for future broker-API integrations.
    """

    FDR = "FDR"
    PYKRX = "PYKRX"
    OPENDART = "OPENDART"
    KRX = "KRX"
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
    REMOTE_DB_SYNC = "remote_db_sync"
    DART_CORP_SYNC = "dart_corp_sync"
    DART_FINANCIAL_SYNC = "dart_financial_sync"
    DART_SHARE_COUNT_SYNC = "dart_share_count_sync"
    DART_SHAREHOLDER_RETURN_SYNC = "dart_shareholder_return_sync"
    DART_SHARE_INFO_SYNC = "dart_share_info_sync"
    METRIC_NORMALIZE = "metric_normalize"
    KRX_FLOW_SYNC = "krx_flow_sync"
    XBRL_PARSE = "xbrl_parse"
    OPERATING_METRIC_SYNC = "operating_metric_sync"


class RunStatus(StrEnum):
    """Pipeline run execution status."""

    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
