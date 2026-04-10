# Architecture

## Overview

The KRX data pipeline follows a **ports & adapters** (hexagonal) architecture.
Domain logic is isolated from infrastructure concerns, making it easy to swap
data sources or storage backends without modifying core business rules.

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                          CLI (argparse)                         │
│  krx-collector universe sync / prices backfill / validate       │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Service Layer (use-cases)                  │
│  sync_universe() │ backfill_daily_prices() │ validate()         │
└────────┬─────────────────────┬─────────────────────┬────────────┘
         │                     │                     │
         ▼                     ▼                     ▼
┌─────────────────┐  ┌─────────────────┐   ┌─────────────────────┐
│  Ports           │  │  Ports           │   │  Ports              │
│  UniverseProvider│  │  PriceProvider   │   │  Storage            │
│  (Protocol)      │  │  (Protocol)      │   │  (Protocol)         │
└────────┬─────────┘  └────────┬─────────┘   └──────────┬──────────┘
         │                     │                        │
         ▼                     ▼                        ▼
┌─────────────────────────────────────────┐   ┌─────────────────────┐
│           Adapters                      │   │  Infra / DB         │
│  FdrUniverseProvider                    │   │  PostgresStorage    │
│  PykrxUniverseProvider                  │   │  (future: FileStore)│
│  PykrxDailyPriceProvider                │   │                     │
└─────────────────────────────────────────┘   └─────────────────────┘
         │                     │                        │
         ▼                     ▼                        ▼
   FinanceDataReader       pykrx API               PostgreSQL
```

## Ports & Adapters Rationale

### Why Protocols instead of ABCs?

- **Structural typing**: Adapters don't need to inherit from a base class.
  Any class with the right method signatures automatically satisfies the
  protocol — enabling easier testing with mocks/fakes.
- **No runtime import coupling**: The domain and service layers never import
  adapter code.  Dependency wiring happens at the CLI / composition root.

### Why separate Universe and Price ports?

- **Single Responsibility**: Universe fetching (list of tickers) and price
  fetching (OHLCV bars per ticker) are fundamentally different operations
  with different rate-limiting, error-handling, and caching strategies.
- **Source flexibility**: Universe can come from FDR or pykrx; prices come
  from pykrx only (for now).  Keeping them separate avoids coupling.

### Storage abstraction

The `Storage` protocol is designed so that:

1. **PostgreSQL** is the primary backend (via `PostgresStorage`).
2. A future **file-based backend** (CSV / Parquet writer) can implement
   the same protocol and be swapped in via dependency injection at the
   CLI layer — no changes to services or domain.

## Domain Layer

Pure Python dataclasses with no framework dependencies:

- `Stock`, `DailyBar`, `StockUniverseSnapshot` — immutable value objects.
- `IngestionRun` — mutable audit record.
- `UpsertResult`, `SyncResult`, `BackfillResult` — operation outcomes.
- Enums: `Market`, `Source`, `ListingStatus`, `RunType`, `RunStatus`.

## Configuration

- `pydantic-settings` loads from `.env` / environment variables.
- Timezone is fixed to `Asia/Seoul` (not configurable).
- Settings are cached as a singleton via `get_settings()`.

## Future: Intraday Extension

An `IntradayPriceProvider` protocol is sketched in `ports/prices.py`
(commented out).  When implemented:

1. Add the protocol method `fetch_intraday_bars(ticker, date, interval)`.
2. Add `intraday_ohlcv` table (see DDL comments).
3. Add a new service use-case `backfill_intraday`.
4. Add a CLI subcommand `prices backfill-intraday`.
