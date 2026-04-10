# Database Schema

Full DDL is in [`sql/postgres_ddl.sql`](../sql/postgres_ddl.sql).

## Tables

### 1. `stock_master`

Latest state of each listed stock.

| Column         | Type          | Notes                          |
|----------------|---------------|--------------------------------|
| `ticker`       | TEXT NOT NULL  | 6-digit KRX code (PK part 1)  |
| `market`       | TEXT NOT NULL  | KOSPI \| KOSDAQ (PK part 2)   |
| `name`         | TEXT NOT NULL  | Korean company name            |
| `listing_date` | DATE NULL      | IPO date (NULL if unknown)     |
| `status`       | TEXT NOT NULL  | ACTIVE \| DELISTED \| UNKNOWN  |
| `last_seen_date` | DATE NOT NULL | Last universe fetch that included this ticker |
| `source`       | TEXT NOT NULL  | FDR \| PYKRX                   |
| `updated_at`   | TIMESTAMPTZ   | Auto-set on insert/update      |

**Primary key:** `(ticker, market)`

### 2. `stock_master_snapshot`

Point-in-time universe fetch metadata.

| Column         | Type          | Notes                        |
|----------------|---------------|------------------------------|
| `snapshot_id`  | UUID PK       | Unique snapshot identifier   |
| `as_of_date`   | DATE NOT NULL  | Reference date               |
| `source`       | TEXT NOT NULL  | FDR \| PYKRX                 |
| `fetched_at`   | TIMESTAMPTZ   | When data was retrieved      |
| `record_count` | INT NOT NULL   | Number of stocks in snapshot |

### 3. `stock_master_snapshot_items`

Individual stocks captured in a snapshot — enables diffing between any two
snapshots to detect new listings, delistings, or name changes.

| Column         | Type          | Notes                        |
|----------------|---------------|------------------------------|
| `snapshot_id`  | UUID FK       | References `stock_master_snapshot` |
| `ticker`       | TEXT NOT NULL  | 6-digit KRX code             |
| `market`       | TEXT NOT NULL  | KOSPI \| KOSDAQ              |
| `name`         | TEXT NOT NULL  | Company name at snapshot time |
| `listing_date` | DATE NULL      | IPO date                     |
| `status`       | TEXT NOT NULL  | ACTIVE \| DELISTED \| UNKNOWN |

**Unique:** `(snapshot_id, ticker, market)`

### 4. `daily_ohlcv`

Daily OHLCV price bars.

| Column       | Type           | Notes                         |
|--------------|----------------|-------------------------------|
| `trade_date` | DATE NOT NULL   | Trading date (PK part 1)     |
| `ticker`     | TEXT NOT NULL   | 6-digit KRX code (PK part 2) |
| `market`     | TEXT NOT NULL   | KOSPI \| KOSDAQ (PK part 3)  |
| `open`       | BIGINT NOT NULL | Opening price (KRW)          |
| `high`       | BIGINT NOT NULL | High price                   |
| `low`        | BIGINT NOT NULL | Low price                    |
| `close`      | BIGINT NOT NULL | Closing price                |
| `volume`     | BIGINT NOT NULL | Traded volume                |
| `source`     | TEXT NOT NULL   | PYKRX (currently only source)|
| `fetched_at` | TIMESTAMPTZ    | When data was retrieved       |

**Primary key:** `(trade_date, ticker, market)`
**Index:** `(ticker, market, trade_date DESC)` for per-ticker queries.

### 5. `ingestion_runs`

Audit log for every pipeline execution.

| Column          | Type           | Notes                          |
|-----------------|----------------|--------------------------------|
| `run_id`        | UUID PK        | Unique run identifier          |
| `run_type`      | TEXT NOT NULL   | universe_sync \| daily_backfill \| validate |
| `started_at`    | TIMESTAMPTZ    | Run start time                 |
| `ended_at`      | TIMESTAMPTZ    | Run end time (NULL while running) |
| `status`        | TEXT NOT NULL   | running \| success \| failed   |
| `params`        | JSONB          | Run parameters                 |
| `counts`        | JSONB          | Aggregated counters            |
| `error_summary` | TEXT           | Human-readable error           |

## Upsert Strategy

### `daily_ohlcv`

```sql
INSERT INTO daily_ohlcv (trade_date, ticker, market, open, high, low, close, volume, source, fetched_at)
VALUES (...)
ON CONFLICT (trade_date, ticker, market) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    source = EXCLUDED.source,
    fetched_at = EXCLUDED.fetched_at;
```

**Rationale:** `DO UPDATE` is preferred over `DO NOTHING` because KRX
occasionally issues price corrections.  Re-fetching should overwrite stale
data automatically.

### `stock_master`

```sql
INSERT INTO stock_master (ticker, market, name, listing_date, status, last_seen_date, source, updated_at)
VALUES (...)
ON CONFLICT (ticker, market) DO UPDATE SET
    name = EXCLUDED.name,
    listing_date = COALESCE(EXCLUDED.listing_date, stock_master.listing_date),
    status = EXCLUDED.status,
    last_seen_date = EXCLUDED.last_seen_date,
    source = EXCLUDED.source,
    updated_at = now();
```

**Note:** `COALESCE` on `listing_date` preserves a previously known date if
the new source doesn't provide one.

## Future: `intraday_ohlcv`

See commented-out DDL in `sql/postgres_ddl.sql`.  Primary key would be
`(trade_ts, ticker, market, interval)`.
