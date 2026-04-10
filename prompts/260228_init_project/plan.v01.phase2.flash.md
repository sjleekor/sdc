# Phase 2 — Stock Universe Sync: Ultra-Detailed Implementation Plan

> **Purpose**: This document provides granular, copy-paste-ready instructions for Phase 2: Stock Universe Sync. It covers the implementation of Universe Providers (FDR, pykrx), the PostgreSQL repository for stock master data, and the orchestration service.

---

## Pre-requisites & Current State

Before starting Phase 2, ensure Phase 1 (Foundation) is complete:
- [ ] `psycopg2` connection pooling is implemented in `src/krx_collector/infra/db_postgres/connection.py`.
- [ ] `Asia/Seoul` time utilities are ready in `src/krx_collector/util/time.py`.
- [ ] PostgreSQL is running and reachable via the DSN in `.env`.

| Component | File | Status |
|---|---|---|
| FDR Provider | `src/krx_collector/adapters/universe_fdr/provider.py` | ❌ Stub |
| pykrx Provider | `src/krx_collector/adapters/universe_pykrx/provider.py` | ❌ Stub |
| Postgres Repo | `src/krx_collector/infra/db_postgres/repositories.py` | ❌ Stub |
| Sync Service | `src/krx_collector/service/sync_universe.py` | ❌ Stub |
| CLI Wiring | `src/krx_collector/cli/app.py` | ❌ Command not implemented |

---

## Task 1: Implement FDR Universe Provider

### 1.1 Edit file: `src/krx_collector/adapters/universe_fdr/provider.py`

**Action**: Implement `FdrUniverseProvider` using `FinanceDataReader`.

**Logic Details**:
1. Iterate over requested `markets` (KOSPI, KOSDAQ).
2. Call `fdr.StockListing(market_name)`.
3. Map DataFrame columns: `Symbol` -> `ticker`, `Name` -> `name`, `ListingDate` -> `listing_date`.
4. Convert `ListingDate` to `datetime.date` (handle `NaT` as `None`).
5. Set `status = ListingStatus.ACTIVE`, `source = Source.FDR`.
6. Set `last_seen_date = as_of`.

**Complete file content**:

```python
"""FDR universe provider implementation."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import FinanceDataReader as fdr
import pandas as pd

from krx_collector.domain.enums import ListingStatus, Market, Source
from krx_collector.domain.models import Stock, StockUniverseSnapshot, UniverseResult
from krx_collector.ports.universe import UniverseProvider
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)


class FdrUniverseProvider(UniverseProvider):
    """Fetches the stock universe using FinanceDataReader."""

    def fetch_universe(
        self,
        markets: list[Market],
        as_of: date | None = None,
    ) -> UniverseResult:
        """Retrieve the stock universe from FDR."""
        target_date = as_of or now_kst().date()
        all_stocks: list[Stock] = []

        try:
            for market in markets:
                logger.info("Fetching %s universe from FDR...", market.value)
                # FDR uses 'KOSPI', 'KOSDAQ' strings
                df = fdr.StockListing(market.value)
                
                if df is None or df.empty:
                    logger.warning("No data returned from FDR for %s", market.value)
                    continue

                for _, row in df.iterrows():
                    listing_date = None
                    if "ListingDate" in row and pd.notna(row["ListingDate"]):
                        listing_date = pd.to_datetime(row["ListingDate"]).date()

                    stock = Stock(
                        ticker=str(row["Symbol"]),
                        market=market,
                        name=str(row["Name"]),
                        listing_date=listing_date,
                        status=ListingStatus.ACTIVE,
                        last_seen_date=target_date,
                        source=Source.FDR,
                    )
                    all_stocks.append(stock)

            snapshot = StockUniverseSnapshot(
                snapshot_id=str(pd.Timestamp.now().value), # Simple ID for now
                as_of_date=target_date,
                source=Source.FDR,
                fetched_at=now_kst(),
                records=all_stocks,
            )
            return UniverseResult(snapshot=snapshot)

        except Exception as e:
            logger.exception("Failed to fetch universe from FDR")
            return UniverseResult(error=str(e))
```

---

## Task 2: Implement pykrx Universe Provider

### 2.1 Edit file: `src/krx_collector/adapters/universe_pykrx/provider.py`

**Action**: Implement `PykrxUniverseProvider` using `pykrx`.

**Logic Details**:
1. Use `stock.get_market_ohlcv` for a specific date to get all tickers and names in one call (efficient).
2. Map `ticker` and `name` (column labels in pykrx).
3. `ListingDate` is not easily available in the bulk call, so it can be `None` for now or fetched individually (bulk is preferred for the universe sync).

**Complete file content**:

```python
"""pykrx universe provider implementation."""

from __future__ import annotations

import logging
import uuid
from datetime import date

from pykrx import stock

from krx_collector.domain.enums import ListingStatus, Market, Source
from krx_collector.domain.models import Stock, StockUniverseSnapshot, UniverseResult
from krx_collector.ports.universe import UniverseProvider
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)


class PykrxUniverseProvider(UniverseProvider):
    """Fetches the stock universe using pykrx."""

    def fetch_universe(
        self,
        markets: list[Market],
        as_of: date | None = None,
    ) -> UniverseResult:
        """Retrieve the stock universe from pykrx."""
        target_date = as_of or now_kst().date()
        date_str = target_date.strftime("%Y%m%d")
        all_stocks: list[Stock] = []

        try:
            for market in markets:
                logger.info("Fetching %s universe from pykrx as of %s...", market.value, date_str)
                # get_market_ohlcv returns DataFrame with Tickers as Index
                df = stock.get_market_ohlcv(date_str, market=market.value)
                
                if df is None or df.empty:
                    # Fallback if no OHLCV (e.g. non-trading day), try ticker list
                    tickers = stock.get_market_ticker_list(date_str, market=market.value)
                    for t in tickers:
                        name = stock.get_market_ticker_name(t)
                        all_stocks.append(self._make_stock(t, name, market, target_date))
                else:
                    # pykrx v1.0.x: Index is Ticker, columns include '종목명'
                    for ticker, row in df.iterrows():
                        name = str(row.get("종목명", "Unknown"))
                        all_stocks.append(self._make_stock(str(ticker), name, market, target_date))

            snapshot = StockUniverseSnapshot(
                snapshot_id=str(uuid.uuid4()),
                as_of_date=target_date,
                source=Source.PYKRX,
                fetched_at=now_kst(),
                records=all_stocks,
            )
            return UniverseResult(snapshot=snapshot)

        except Exception as e:
            logger.exception("Failed to fetch universe from pykrx")
            return UniverseResult(error=str(e))

    def _make_stock(self, ticker: str, name: str, market: Market, last_seen: date) -> Stock:
        return Stock(
            ticker=ticker,
            market=market,
            name=name,
            listing_date=None,  # pykrx bulk doesn't provide this
            status=ListingStatus.ACTIVE,
            last_seen_date=last_seen,
            source=Source.PYKRX,
        )
```

---

## Task 3: Implement Postgres Storage (Repository)

### 3.1 Edit file: `src/krx_collector/infra/db_postgres/repositories.py`

**Action**: Implement `PostgresStorage` to handle `stock_master` and `ingestion_runs`.

**Complete file content**:

```python
"""PostgreSQL repository implementation."""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

from krx_collector.domain.models import (
    DailyBar,
    IngestionRun,
    Stock,
    StockUniverseSnapshot,
    UpsertResult,
)
from krx_collector.infra.db_postgres.connection import get_connection
from krx_collector.ports.storage import Storage

logger = logging.getLogger(__name__)


class PostgresStorage(Storage):
    """PostgreSQL implementation of the Storage port."""

    def __init__(self, dsn: str):
        self._dsn = dsn

    def init_schema(self) -> None:
        """Execute DDL to ensure schema exists."""
        import os
        ddl_path = os.path.join(os.getcwd(), "sql", "postgres_ddl.sql")
        with open(ddl_path, "r") as f:
            ddl = f.read()
        
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
        logger.info("Database schema initialized.")

    def upsert_stock_master(
        self,
        stocks: list[Stock],
        snapshot: StockUniverseSnapshot,
    ) -> UpsertResult:
        """Upsert stock master and save snapshot audit records."""
        result = UpsertResult()

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                # 1. Save Snapshot Metadata
                cur.execute(
                    """
                    INSERT INTO stock_master_snapshot (snapshot_id, as_of_date, source, fetched_at, record_count)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (snapshot_id) DO NOTHING;
                    """,
                    (snapshot.snapshot_id, snapshot.as_of_date, snapshot.source.value, 
                     snapshot.fetched_at, snapshot.record_count)
                )

                # 2. Bulk Upsert Stock Master & Snapshot Items
                for s in stocks:
                    try:
                        # stock_master
                        cur.execute(
                            """
                            INSERT INTO stock_master (ticker, market, name, listing_date, status, last_seen_date, source, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                            ON CONFLICT (ticker, market) DO UPDATE SET
                                name = EXCLUDED.name,
                                listing_date = COALESCE(stock_master.listing_date, EXCLUDED.listing_date),
                                status = EXCLUDED.status,
                                last_seen_date = EXCLUDED.last_seen_date,
                                source = EXCLUDED.source,
                                updated_at = now();
                            """,
                            (s.ticker, s.market.value, s.name, s.listing_date, s.status.value, 
                             s.last_seen_date, s.source.value)
                        )
                        
                        # stock_master_snapshot_items
                        cur.execute(
                            """
                            INSERT INTO stock_master_snapshot_items (snapshot_id, ticker, market, name, listing_date, status)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (snapshot_id, ticker, market) DO NOTHING;
                            """,
                            (snapshot.snapshot_id, s.ticker, s.market.value, s.name, s.listing_date, s.status.value)
                        )
                        result.inserted += 1 # Simplified counter
                    except Exception:
                        logger.exception("Failed to upsert ticker %s", s.ticker)
                        result.errors += 1
        
        return result

    def get_listing_date(self, ticker: str) -> date | None:
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT listing_date FROM stock_master WHERE ticker = %s LIMIT 1;", (ticker,))
                row = cur.fetchone()
                return row[0] if row else None

    def upsert_daily_bars(self, bars: list[DailyBar]) -> UpsertResult:
        # To be implemented in Phase 3
        raise NotImplementedError("upsert_daily_bars is for Phase 3")

    def record_run(self, run: IngestionRun) -> None:
        """Insert or update ingestion run audit record."""
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingestion_runs (run_id, run_type, started_at, ended_at, status, params, counts, error_summary)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id) DO UPDATE SET
                        ended_at = EXCLUDED.ended_at,
                        status = EXCLUDED.status,
                        counts = EXCLUDED.counts,
                        error_summary = EXCLUDED.error_summary;
                    """,
                    (run.run_id, run.run_type.value, run.started_at, run.ended_at, run.status.value,
                     json.dumps(run.params) if run.params else None,
                     json.dumps(run.counts) if run.counts else None,
                     run.error_summary)
                )

    def query_missing_days(self, ticker: str, start: date, end: date) -> list[date]:
        # Optional optimization for Phase 3
        return []
```

---

## Task 4: Implement Sync Universe Service

### 4.1 Edit file: `src/krx_collector/service/sync_universe.py`

**Action**: Implement the `SyncUniverseService` that orchestrates providers and storage.

**Complete file content**:

```python
"""Universe sync service implementation."""

from __future__ import annotations

import logging
from datetime import date

from krx_collector.domain.enums import Market, RunStatus, RunType, Source
from krx_collector.domain.models import IngestionRun, SyncResult
from krx_collector.ports.storage import Storage
from krx_collector.ports.universe import UniverseProvider
from krx_collector.util.time import now_kst

logger = logging.getLogger(__name__)


class SyncUniverseService:
    """Orchestrates fetching the stock universe and updating the master record."""

    def __init__(
        self,
        fdr_provider: UniverseProvider,
        pykrx_provider: UniverseProvider,
        storage: Storage,
    ):
        self._fdr = fdr_provider
        self._pykrx = pykrx_provider
        self._storage = storage

    def sync(
        self,
        markets: list[Market] | None = None,
        as_of: date | None = None,
    ) -> SyncResult:
        """Perform a full universe synchronization."""
        markets = markets or [Market.KOSPI, Market.KOSDAQ]
        target_date = as_of or now_kst().date()
        
        run = IngestionRun(
            run_type=RunType.UNIVERSE_SYNC,
            started_at=now_kst(),
            params={"markets": [m.value for m in markets], "as_of": str(target_date)},
        )
        self._storage.record_run(run)

        try:
            # 1. Fetch from providers
            # We use FDR as primary and Pykrx as secondary
            fdr_res = self._fdr.fetch_universe(markets, as_of=target_date)
            pykrx_res = self._pykrx.fetch_universe(markets, as_of=target_date)

            if fdr_res.error and pykrx_res.error:
                raise RuntimeError(f"Both providers failed. FDR: {fdr_res.error}, pykrx: {pykrx_res.error}")

            # 2. Merge (Union of tickers)
            stocks_dict = {}
            if pykrx_res.snapshot:
                for s in pykrx_res.snapshot.records:
                    stocks_dict[(s.ticker, s.market)] = s
            
            if fdr_res.snapshot:
                for s in fdr_res.snapshot.records:
                    # FDR preferred for listing_date
                    stocks_dict[(s.ticker, s.market)] = s

            all_stocks = list(stocks_dict.values())
            logger.info("Merged universe contains %d unique stocks.", len(all_stocks))

            # 3. Create a combined snapshot for audit
            from krx_collector.domain.models import StockUniverseSnapshot
            import uuid
            
            final_snapshot = StockUniverseSnapshot(
                snapshot_id=str(uuid.uuid4()),
                as_of_date=target_date,
                source=Source.FDR, # Mark as FDR-led merge
                fetched_at=now_kst(),
                records=all_stocks
            )

            # 4. Persistence
            upsert_res = self._storage.upsert_stock_master(all_stocks, final_snapshot)

            # 5. Finalize run
            run.status = RunStatus.SUCCESS
            run.ended_at = now_kst()
            run.counts = {
                "total_fetched": len(all_stocks),
                "upserted": upsert_res.inserted,
                "errors": upsert_res.errors
            }
            self._storage.record_run(run)

            return SyncResult(upsert=upsert_res)

        except Exception as e:
            logger.exception("Universe sync failed")
            run.status = RunStatus.FAILED
            run.ended_at = now_kst()
            run.error_summary = str(e)
            self._storage.record_run(run)
            return SyncResult(error=str(e))
```

---

## Task 5: Wire CLI Command

### 5.1 Edit file: `src/krx_collector/cli/app.py`

**Action**: Connect the `SyncUniverseService` to the `universe sync` CLI command.

**Partial file change** (look for `universe_sync` function):

```python
@universe_app.command("sync")
def universe_sync(
    market: list[str] = typer.Option(
        ["KOSPI", "KOSDAQ"],
        "--market", "-m",
        help="Markets to sync (can be specified multiple times)."
    ),
    as_of: str = typer.Option(
        None,
        "--as-of",
        help="Reference date (YYYY-MM-DD). Default is today."
    ),
) -> None:
    """Fetch the latest stock list from KRX and update the local database."""
    from krx_collector.domain.enums import Market
    from krx_collector.infra.config.settings import get_settings
    from krx_collector.infra.db_postgres.repositories import PostgresStorage
    from krx_collector.adapters.universe_fdr.provider import FdrUniverseProvider
    from krx_collector.adapters.universe_pykrx.provider import PykrxUniverseProvider
    from krx_collector.service.sync_universe import SyncUniverseService
    from datetime import datetime

    settings = get_settings()
    markets = [Market(m.upper()) for m in market]
    target_date = datetime.strptime(as_of, "%Y-%m-%d").date() if as_of else None

    # Dependency Injection
    storage = PostgresStorage(settings.db_dsn)
    # Ensure tables exist
    storage.init_schema()

    service = SyncUniverseService(
        fdr_provider=FdrUniverseProvider(),
        pykrx_provider=PykrxUniverseProvider(),
        storage=storage
    )

    logger.info("Starting universe sync for markets: %s", [m.value for m in markets])
    result = service.sync(markets=markets, as_of=target_date)

    if result.error:
        logger.error("Sync failed: %s", result.error)
        raise typer.Exit(code=1)
    
    logger.info(
        "Sync completed. Upserted: %d, Errors: %d",
        result.upsert.inserted,
        result.upsert.errors
    )
```

---

## Task 6: Verification

### 6.1 Initialize Database Schema

```bash
python -m krx_collector universe sync --market KOSPI --as-of 2024-01-02
```

### 6.2 Check Data in PostgreSQL

```sql
-- Connect to your DB and run:
SELECT count(*) FROM stock_master;
SELECT * FROM stock_master_snapshot;
SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT 1;
```

---

## Definition of Done — Phase 2 Checklist

| # | Criterion | How to verify |
|---|---|---|
| 1 | `FdrUniverseProvider` returns `Stock` objects | Unit test or CLI run |
| 2 | `PykrxUniverseProvider` returns `Stock` objects | Unit test or CLI run |
| 3 | `PostgresStorage` handles UPSERT correctly | `SELECT count(*)` in `stock_master` increases |
| 4 | `SyncUniverseService` records audit trails | `ingestion_runs` has a `universe_sync` record |
| 5 | CLI command `universe sync` works | Command exits with code 0 |

---

## File Change Summary

| Action | File Path |
|---|---|
| **REPLACE** | `src/krx_collector/adapters/universe_fdr/provider.py` |
| **REPLACE** | `src/krx_collector/adapters/universe_pykrx/provider.py` |
| **REPLACE** | `src/krx_collector/infra/db_postgres/repositories.py` |
| **REPLACE** | `src/krx_collector/service/sync_universe.py` |
| **MODIFY** | `src/krx_collector/cli/app.py` |

**Total files to modify: 5**
