# Phase 2 Execution Plan for Codex (Ultra-Explicit, Low-Capability-Agent Friendly)

## 0. Objective

Implement **Phase 2: Stock Universe Sync** from `plan.v01.md` with a deterministic workflow that a weaker coding agent can execute without ambiguity.

Phase 2 scope:
1. FDR universe provider
2. pykrx universe provider
3. Stock master storage (PostgreSQL)
4. Sync universe service
5. CLI wiring for `universe sync` (and `db init` needed to run it)

Output expectation:
- No Phase 2 stubs remain in the target files.
- `krx-collector universe sync ...` can run end-to-end (if DB/network are available).

---

## 1. Hard Scope Boundaries

Follow these rules strictly:
1. Do not modify Phase 3 logic (`prices backfill`) except import-side compile fixes if absolutely necessary.
2. Keep public signatures unchanged for:
   - `FdrUniverseProvider.fetch_universe(...)`
   - `PykrxUniverseProvider.fetch_universe(...)`
   - `PostgresStorage.*`
   - `sync_universe(...)`
3. Leave `PostgresStorage.upsert_daily_bars(...)` and `query_missing_days(...)` as `NotImplementedError` (Phase 3 work).
4. Use `Asia/Seoul` utilities from `krx_collector.util.time`.
5. Keep output types aligned with domain dataclasses and enums.

---

## 2. Files to Modify in This Phase

1. `src/krx_collector/adapters/universe_fdr/provider.py`
2. `src/krx_collector/adapters/universe_pykrx/provider.py`
3. `src/krx_collector/infra/db_postgres/repositories.py`
4. `src/krx_collector/service/sync_universe.py`
5. `src/krx_collector/cli/app.py`

Recommended test files to add:
1. `tests/unit/test_universe_fdr_provider.py`
2. `tests/unit/test_universe_pykrx_provider.py`
3. `tests/unit/test_sync_universe.py`
4. `tests/integration/test_postgres_storage_stock_master.py`

---

## 3. Execution Rules for a Weak Agent

Use this strict sequence:
1. Edit exactly one file.
2. Run only the targeted checks for that file.
3. Fix errors in that file before moving on.
4. Do not batch-edit multiple files at once.

After each file:
1. Run `python -m compileall src/krx_collector/<path_to_file>`
2. Run `rg -n "NotImplementedError|TODO" <that_file>`
3. Confirm only expected TODOs remain.

---

## 4. Step-by-Step Implementation

## Step 1: Implement `FdrUniverseProvider`

File: `src/krx_collector/adapters/universe_fdr/provider.py`

### 1.1 Required imports

Ensure these imports exist:
- `uuid`
- `pandas as pd`
- `FinanceDataReader as fdr`
- domain enums: `ListingStatus`, `Market`, `Source`
- domain models: `Stock`, `StockUniverseSnapshot`, `UniverseResult`
- `now_kst` from `krx_collector.util.time`

### 1.2 Functional behavior

Implement `fetch_universe(markets, as_of=None)`:
1. Resolve `target_date = as_of or now_kst().date()`.
2. For each `market` in `markets`:
   - Call `fdr.StockListing(market.value)`.
   - If dataframe is empty, continue and log warning.
3. For each row:
   - `ticker` from `Symbol` (string, stripped; keep leading zeros).
   - `name` from `Name` (string, stripped).
   - `listing_date` from `ListingDate`:
     - if null/NaT -> `None`
     - else convert to `date`.
   - Create `Stock` with:
     - `status=ListingStatus.ACTIVE`
     - `last_seen_date=target_date`
     - `source=Source.FDR`
4. Build `StockUniverseSnapshot`:
   - `snapshot_id = str(uuid.uuid4())`
   - `as_of_date = target_date`
   - `source = Source.FDR`
   - `fetched_at = now_kst()`
   - `records = all_stocks`
5. Return `UniverseResult(snapshot=snapshot)`.
6. On exception: `logger.exception(...)` and return `UniverseResult(error=str(exc))`.

### 1.3 Guardrails

1. Do not raise exceptions to caller for normal fetch failures; return `UniverseResult(error=...)`.
2. Do not hardcode markets.
3. Keep the class name `FdrUniverseProvider`.

### 1.4 Local check

Commands:
```bash
python -m compileall src/krx_collector/adapters/universe_fdr/provider.py
rg -n "NotImplementedError|TODO" src/krx_collector/adapters/universe_fdr/provider.py
```

Expected:
- compile succeeds
- no TODO/NotImplementedError left in this file

---

## Step 2: Implement `PykrxUniverseProvider`

File: `src/krx_collector/adapters/universe_pykrx/provider.py`

### 2.1 Required imports

Ensure these imports exist:
- `uuid`
- `from pykrx import stock as pykrx_stock`
- domain enums: `ListingStatus`, `Market`, `Source`
- domain models: `Stock`, `StockUniverseSnapshot`, `UniverseResult`
- `now_kst` from `krx_collector.util.time`

### 2.2 Functional behavior

Implement `fetch_universe(markets, as_of=None)`:
1. Resolve `target_date = as_of or now_kst().date()`.
2. Convert date string: `as_of_str = target_date.strftime("%Y%m%d")`.
3. For each `market`:
   - `tickers = pykrx_stock.get_market_ticker_list(as_of_str, market=market.value)`
4. For each ticker:
   - `name = pykrx_stock.get_market_ticker_name(ticker)`
   - create `Stock` with:
     - `listing_date=None` (pykrx universe endpoint does not provide listing date directly)
     - `status=ListingStatus.ACTIVE`
     - `last_seen_date=target_date`
     - `source=Source.PYKRX`
5. Build snapshot using `Source.PYKRX` and `uuid4`.
6. Return `UniverseResult(snapshot=snapshot)`.
7. On exception, return `UniverseResult(error=str(exc))` after logging.

### 2.3 Guardrails

1. Do not call OHLCV APIs in this Phase 2 universe adapter.
2. Do not infer listing date in this phase.
3. Keep `as_of` behavior deterministic via KST default.

### 2.4 Local check

Commands:
```bash
python -m compileall src/krx_collector/adapters/universe_pykrx/provider.py
rg -n "NotImplementedError|TODO" src/krx_collector/adapters/universe_pykrx/provider.py
```

Expected:
- compile succeeds
- no TODO/NotImplementedError left in this file

---

## Step 3: Implement `PostgresStorage` methods needed by Phase 2

File: `src/krx_collector/infra/db_postgres/repositories.py`

### 3.1 Required imports

Add:
- `json`
- `Path` from `pathlib`
- `get_connection` from `krx_collector.infra.db_postgres.connection`

Keep existing domain model imports.

### 3.2 Implement `init_schema`

Behavior:
1. Resolve project root from current file location:
   - from `repositories.py`, go up to repo root and locate `sql/postgres_ddl.sql`.
2. Read SQL as UTF-8 text.
3. Execute SQL with:
   - `with get_connection(self._dsn) as conn:`
   - `with conn.cursor() as cur: cur.execute(ddl_sql)`
4. Log completion.

Important:
- Do not use `os.getcwd()` for DDL lookup (fragile when command runs from another directory).

### 3.3 Implement `upsert_stock_master`

Functional sequence:
1. Initialize `result = UpsertResult()`.
2. Insert snapshot metadata into `stock_master_snapshot`:
   - columns: `snapshot_id, as_of_date, source, fetched_at, record_count`
   - `ON CONFLICT (snapshot_id) DO NOTHING`
3. For each stock in `stocks`, execute two SQL statements:
   - Upsert into `stock_master`:
     - conflict key `(ticker, market)`
     - update fields: `name`, `listing_date` (with `COALESCE` to preserve known value), `status`, `last_seen_date`, `source`, `updated_at=now()`
   - Insert into `stock_master_snapshot_items`:
     - columns: `snapshot_id, ticker, market, name, listing_date, status`
     - conflict handling: `DO NOTHING`
4. Counter policy:
   - On successful stock processing, increment `result.inserted` by 1.
   - On per-stock exception, increment `result.errors` and continue.
   - `result.updated` may remain 0 in Phase 2 (acceptable).
5. Return `result`.

### 3.4 Implement `get_listing_date`

Behavior:
1. Query `stock_master` by ticker:
   - `SELECT listing_date FROM stock_master WHERE ticker = %s LIMIT 1`
2. Return the `date` value or `None`.

### 3.5 Implement `record_run`

Behavior:
1. Insert or update row in `ingestion_runs`:
   - Insert: `run_id, run_type, started_at, ended_at, status, params, counts, error_summary`
   - On conflict `(run_id)`, update: `ended_at, status, params, counts, error_summary`.
2. Serialize dict fields with `json.dumps(...)` when not `None`.

### 3.6 Keep Phase 3 methods untouched

Do not implement these now:
- `upsert_daily_bars(...)`
- `query_missing_days(...)`

Leave `NotImplementedError` there.

### 3.7 Local check

Commands:
```bash
python -m compileall src/krx_collector/infra/db_postgres/repositories.py
rg -n "NotImplementedError|TODO" src/krx_collector/infra/db_postgres/repositories.py
```

Expected:
- compile succeeds
- only Phase 3 methods may still have `NotImplementedError`

---

## Step 4: Implement `sync_universe` service

File: `src/krx_collector/service/sync_universe.py`

### 4.1 Required imports

Add imports for:
- `RunStatus`, `RunType` enums
- `IngestionRun`, `SyncResult`
- `now_kst`

### 4.2 Functional behavior

Implement `sync_universe(provider, storage, markets, as_of=None, full_refresh=False)`:
1. Determine `target_date = as_of or now_kst().date()`.
2. Create an `IngestionRun`:
   - `run_type=RunType.UNIVERSE_SYNC`
   - `started_at=now_kst()`
   - `status=RunStatus.RUNNING`
   - `params` should include source-independent values:
     - `markets`: market string list
     - `as_of`: ISO date string
     - `full_refresh`: bool
3. Persist run start with `storage.record_run(run)`.
4. Fetch data: `provider_result = provider.fetch_universe(markets, as_of=target_date)`.
5. If provider returns error or no snapshot:
   - mark run failed
   - set `ended_at`, `status`, `error_summary`
   - write run again via `storage.record_run(run)`
   - return `SyncResult(error=...)`
6. Persist snapshot and stock records:
   - `upsert = storage.upsert_stock_master(snapshot.records, snapshot)`
7. Finalize success run:
   - set run `ended_at`, `status=RunStatus.SUCCESS`
   - set `counts` with at least:
     - `snapshot_records`
     - `upsert_inserted`
     - `upsert_updated`
     - `upsert_errors`
   - persist run
8. Return `SyncResult(upsert=upsert, new_tickers=[], delisted_tickers=[])`.

### 4.3 About `full_refresh`, `new_tickers`, `delisted_tickers`

In current interfaces, there is no storage read API for master diffing.
Phase 2 implementation rule:
1. Accept `full_refresh` argument for compatibility.
2. Log that full refresh diff behavior is reserved.
3. Return empty diff lists now.
4. Do not add new Storage protocol methods in this phase.

### 4.4 Error handling

Wrap the workflow in `try/except`:
1. On any exception, update run as failed (if run already created).
2. Return `SyncResult(error=str(exc))`.

### 4.5 Local check

Commands:
```bash
python -m compileall src/krx_collector/service/sync_universe.py
rg -n "NotImplementedError|TODO" src/krx_collector/service/sync_universe.py
```

Expected:
- compile succeeds
- no TODO/NotImplementedError left in this file

---

## Step 5: Wire CLI handlers for DB init and Universe sync

File: `src/krx_collector/cli/app.py`

### 5.1 Implement `_handle_db_init`

Sequence:
1. `settings = get_settings()`
2. Instantiate `PostgresStorage(settings.db_dsn)`
3. Call `init_schema()`
4. Print a concise success message

### 5.2 Implement `_handle_universe_sync`

Sequence:
1. Read settings and resolve source:
   - if `args.source` provided, use it
   - else use `settings.universe_source_default.value`
2. Parse `args.markets` (comma-separated) into enum list:
   - accepted tokens: `kospi`, `kosdaq` (case-insensitive)
   - convert to `Market` enum values
   - on invalid token, raise `ValueError` with clear message
3. Instantiate provider by source:
   - `fdr` -> `FdrUniverseProvider()`
   - `pykrx` -> `PykrxUniverseProvider()`
4. Instantiate storage: `PostgresStorage(settings.db_dsn)`.
5. Optionally call `storage.init_schema()` before sync (recommended for first-run safety).
6. Call:
   - `result = sync_universe(provider, storage, markets, as_of=args.as_of, full_refresh=args.full_refresh)`
7. If `result.error` exists:
   - log and `sys.exit(1)`
8. On success:
   - print upsert counters
   - include diff counts (`len(new_tickers)`, `len(delisted_tickers)`)

### 5.3 Keep other handlers unchanged

Do not implement in this phase:
- `_handle_prices_backfill`
- `_handle_validate`

They may continue to raise `NotImplementedError`.

### 5.4 Improve top-level `main()` exception handling

Current code only handles `NotImplementedError`.
Add a second `except Exception as exc` block:
1. Log exception with traceback.
2. Print user-facing error to stderr.
3. Exit with code 1.

This prevents raw tracebacks for operational failures.

### 5.5 Local check

Commands:
```bash
python -m compileall src/krx_collector/cli/app.py
rg -n "NotImplementedError|TODO" src/krx_collector/cli/app.py
```

Expected:
- compile succeeds
- TODOs remain only for Phase 3/4 handlers

---

## Step 6: Add Focused Tests (Strongly Recommended)

## 6.1 Unit test: FDR provider mapping

File: `tests/unit/test_universe_fdr_provider.py`

Test requirements:
1. Monkeypatch `fdr.StockListing` to return a small DataFrame with:
   - one valid listing date
   - one null listing date
2. Assert:
   - `UniverseResult.error is None`
   - snapshot exists
   - `source == Source.FDR`
   - two `Stock` records mapped correctly

## 6.2 Unit test: pykrx provider mapping

File: `tests/unit/test_universe_pykrx_provider.py`

Test requirements:
1. Monkeypatch:
   - `pykrx_stock.get_market_ticker_list`
   - `pykrx_stock.get_market_ticker_name`
2. Assert:
   - records created for all tickers
   - `listing_date is None`
   - `source == Source.PYKRX`

## 6.3 Unit test: sync service

File: `tests/unit/test_sync_universe.py`

Create fake classes:
1. `FakeProvider` returning a valid `UniverseResult`
2. `FakeStorage` storing calls in memory

Assertions:
1. `record_run` called at least twice (start/end).
2. `upsert_stock_master` called once with snapshot records.
3. Successful `SyncResult.error is None`.
4. Failure path returns `SyncResult.error` and final run status failed.

## 6.4 Integration test: repository stock master flow

File: `tests/integration/test_postgres_storage_stock_master.py`

Behavior:
1. Use real DB DSN from settings.
2. Skip test if DB connection unavailable.
3. Steps:
   - `init_schema()`
   - create sample snapshot + stocks
   - call `upsert_stock_master`
   - assert row exists in `stock_master`
   - assert snapshot metadata row exists
   - assert snapshot item row exists

---

## Step 7: End-to-End Manual Verification

Run from repo root:

```bash
uv run krx-collector db init
uv run krx-collector universe sync --source fdr --markets kospi,kosdaq
uv run krx-collector universe sync --source pykrx --markets kospi
```

Expected behavior:
1. `db init` exits 0 and creates schema.
2. `universe sync` exits 0 on success.
3. Console logs include upsert counters.

SQL verification examples:

```sql
SELECT COUNT(*) FROM stock_master;
SELECT COUNT(*) FROM stock_master_snapshot;
SELECT COUNT(*) FROM stock_master_snapshot_items;
SELECT run_id, run_type, status, started_at, ended_at
FROM ingestion_runs
ORDER BY started_at DESC
LIMIT 5;
```

Expected:
1. Counts are greater than 0 after successful sync.
2. At least one `universe_sync` run with `status='success'`.

---

## 8. Completion Checklist (Definition of Done)

Mark all items before closing Phase 2:

1. `FdrUniverseProvider.fetch_universe` implemented and returns `UniverseResult`.
2. `PykrxUniverseProvider.fetch_universe` implemented and returns `UniverseResult`.
3. `PostgresStorage.init_schema` implemented.
4. `PostgresStorage.upsert_stock_master` implemented.
5. `PostgresStorage.get_listing_date` implemented.
6. `PostgresStorage.record_run` implemented.
7. `sync_universe(...)` implemented with ingestion run start/end recording.
8. `_handle_db_init` and `_handle_universe_sync` wired in CLI.
9. Targeted unit tests pass.
10. Manual `universe sync` run verified against DB.

---

## 9. Known Deferred Items (Explicitly Not Phase 2)

These are intentionally deferred:
1. `upsert_daily_bars` implementation
2. `query_missing_days` implementation
3. `prices backfill` handler wiring
4. `validate` handler wiring
5. advanced diffing of `new_tickers` and `delisted_tickers` from DB state

Keep these deferred to avoid scope creep and unstable partial implementations.
