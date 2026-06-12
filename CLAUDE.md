# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`krx-data-pipeline` (package `krx_collector`, image/repo alias `sdc`) is a Korean stock data pipeline. It syncs KOSPI/KOSDAQ universe, daily OHLCV, OpenDART financials/share-info/XBRL, KRX security flows, sector-specific operating KPIs, and market/macro common features — normalizing them into canonical metric tables in PostgreSQL. Python ≥ 3.12, managed with `uv`. The README is in Korean; this file is the English working summary.

## Commands

```bash
uv sync --extra dev               # install with dev deps (ruff, black, pytest)
uv run pytest                     # run all tests
uv run pytest tests/unit          # unit tests only (no DB needed)
uv run pytest path::test_name     # run a single test
uv run ruff check src/ tests/     # lint (E, F, I, W, UP; line-length 100)
uv run black src/ tests/          # format
```

- Integration tests in `tests/integration/` self-skip when the DB is unreachable. Live-API tests gate on env vars `RUN_LIVE_FDR_TEST=1` / `RUN_LIVE_PYKRX_TEST=1`.
- The CLI is `uv run krx-collector <command>` (or `uv run python -m krx_collector ...`). `pyproject.toml` also defines a `dart` console script that is just `krx-collector dart ...`.
- Releasing (version bump, tag, update prod compose) is handled by the **sdc-release** skill; prod host/schedule inspection by **sj2-server** / **sdc-db** skills. Prefer these over manual steps.

## CLI command tree

Top-level subcommands (see `src/krx_collector/cli/app.py`, ~1800 lines, where the whole argparse tree and DI wiring live):

- `db init` / `db sync-remote` — schema init; pull prod DB → local (incremental, `--full-refresh`, `--all-tables`, `--ssh-host` tunnel).
- `universe sync` — sync stock master from `fdr` or `pykrx`.
- `prices backfill` — daily OHLCV; default = gap-detection backfill, `--incremental` = only after each ticker's `MAX(trade_date)`.
- `dart sync-corp | sync-financials | sync-share-info | sync-xbrl` — OpenDART raw ingestion.
- `metrics normalize | coverage-report` — raw → `stock_metric_fact` canonical normalization (no external calls).
- `common seed | sync | build-daily | coverage | readiness` — market/macro common features.
- `flows sync` — KRX MDC security-flow raw (investor net-buy, short-selling).
- `operating process-document` — sector KPI extractor pilot (e.g. `shipbuilding_defense`).
- `validate` — data-quality checks.

## Architecture

Strict **ports & adapters (hexagonal)**. The dependency rule is the key invariant: **`domain/` and `service/` never import `adapters/` or `infra/`**. Wiring happens only in the CLI composition root (`cli/app.py`).

- **`domain/`** — pure dataclasses + `StrEnum`s, no framework deps. `enums.py` defines `Source`, `Market`, `RunType`, `RunStatus` (`running`/`success`/`partial`/`failed`) — central to audit logging.
- **`ports/`** — `typing.Protocol` interfaces (structural typing, not ABCs), one per concern: `universe`, `prices`, `storage`, `corp_codes`, `financials`, `share_info`, `xbrl`, `flows`, `operating_extractors`, `common_features`.
- **`adapters/`** — provider implementations grouped by source: `universe_fdr`/`universe_pykrx`, `prices_pykrx`, `opendart_*`, `flows_krx`, `operating_extractors`, `common_features_{pykrx,fdr,krx,ecos,fred}`.
- **`service/`** — one use-case orchestrator per file (`sync_universe`, `backfill_daily`, `sync_dart_*`, `normalize_metrics`, `sync_common_features`, `build_common_feature_daily_facts`, `sync_local_db`, …). These take ports as arguments.
- **`infra/`** — `db_postgres/` (PostgresStorage + `remote_sync.py`), `calendar/` (KRX trading-day calendar, uses `docs/holidays_krx.csv`), `config/` (pydantic-settings singleton via `get_settings()`), `logging/`.
- **`util/pipeline.py`** — shared retry/jitter/throttle (`HumanThrottlePolicy`) and the **partial-run finalizer**. Read this before touching any ingestion flow.

### Cross-cutting patterns you must preserve

- **Raw vs canonical two-layer model.** Each source writes immutable `*_raw` tables. A separate normalization layer (`metric_catalog` / `metric_mapping_rule` → `stock_metric_fact`; analogously for common features) produces canonical metrics. Don't blur these layers.
- **Idempotent + skip-if-present.** Every sync uses `ON CONFLICT … DO UPDATE` and skips re-fetching when the equivalent row already exists (keyed per source — see README "중복 실행 방지" section). New ingestion code must follow this.
- **Audit via `ingestion_runs`.** Every run records a `RunType` and ends in one of the four `RunStatus`. On partial external-API failure the pipeline still exits cleanly with `status=partial` and per-failure counts. OpenDART runs additionally record multi-key rotation/rate-limit/status-code metrics. See `docs/operations.md` for interpretation.
- **OpenDART multi-key.** `OPENDART_API_KEY` (single) and/or `OPENDART_API_KEYS` (comma-separated) — a shared executor in `opendart_common` rotates keys on rate-limit/error. When all keys hit the daily limit the CLI exits with code `75` and resumes (skipping stored raw) next run.
- **Timezone is fixed `Asia/Seoul`** (`util/time.py`, `now_kst()`) — not configurable.
- **Schema source of truth** is `sql/postgres_ddl.sql` (applied by `db init` and `db sync-remote --all-tables`).

## Config & secrets

Settings load from `.env` (template: `.env.example`) via pydantic-settings. DB via `DB_DSN` or `DB_HOST/PORT/NAME/USER/PASSWORD`. OpenDART keys as above. KRX MDC login fallback uses `KRX_ID`/`KRX_PW`. `db sync-remote` reads remote DB creds from `/Users/whishaw/wss_p/stock_data_collector_secrets/db_info` by default.

## Docs & deploy

- `docs/architecture.md`, `docs/database.md`, `docs/operations.md` (cron schedule, runbook, partial-run recovery). `docs/dev/` holds dated design/implementation plans.
- `deploy/prod/bin/` holds the host-side wrapper scripts that run each pipeline stage in prod (Cronicle on sj2-server). `bin/dart-backfill-all-years.sh` runs the multi-year OpenDART backfill (default 2015→last year).
- CI: pushing to `main` builds/pushes `ghcr.io/sjleekor/sdc` via `.github/workflows/docker.yml`.

## Scope exclusions

Intraday (minute/hour) bars are out of scope (extension points stubbed in `ports/prices.py`). Selenium is intentionally not used.
