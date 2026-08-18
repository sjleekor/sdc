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

The CLI is **raw-ingestion only** after the 2026-07 refactor (see
`docs/dev/20260728_refactor_pipeline/00_refactor_plan.md`). The *compute* steps
(`metrics normalize`, `common build-daily/coverage/readiness`, `metrics
coverage-report`, `ops assert-common-freshness`, `operating process-document`)
were removed from the CLI — they now run as DuckDB marts via
`bin/parquet-compute-all.sh` (see "Parquet compute pipeline" below).

Top-level subcommands (see `src/krx_collector/cli/app.py`, where the argparse tree and DI wiring live):

- `db init` / `db sync-remote` — schema init; pull prod DB → local (incremental, `--full-refresh`, `--all-tables`, `--ssh-host` tunnel).
- `universe sync` — sync stock master; `--source krx-openapi` (official, needs `AUTH_KEYS`), `fdr`, or `pykrx`. Prod still passes `--source fdr` because prod has no `AUTH_KEYS` yet.
  `universe backfill-snapshots` — month-end PIT snapshots; `--source krx-openapi` (default) or `pykrx`.
- `prices backfill` — daily OHLCV; default = gap-detection backfill, `--incremental` = only after each ticker's `MAX(trade_date)`. `--source naver` (default) reads Naver's chart endpoint directly; `--source pykrx` is the old wrapper over *the same* endpoint and is gated by `ALLOW_KRX_SCRAPING` (K-5) because importing pykrx logs in to KRX.
  `prices market-cap-backfill` — daily market cap / trading value / listed shares, plus the unadjusted OHLC the Open API returns in the same response. `--source krx-openapi` (default, needs `AUTH_KEYS`) or `pykrx` (scrapes; kept only for comparison until K-5).
- `dart sync-corp | sync-financials | sync-share-info | sync-xbrl` — OpenDART raw ingestion.
  `sync-share-info` also collects `dart_capital_change_raw` (irdsSttus) in the same run.
  `dart sync-filings` (raw disclosure-receipt history, list.json) runs as the last stage of
  `bin/dart-backfill-all-years.sh`, on its own *calendar receipt year* range (up to the current
  year, not `end_year`). `dart backfill-xbrl-receipts` (explicit receipt-targeted XBRL refetch,
  `--targets-file`) stays manual — picking the target receipts is a separate analysis.
- `common seed | sync` — seed `common_feature_series` config + sync market/macro raw observations.
- `flows sync` — KRX MDC security-flow raw (investor net-buy, short-selling).
  `flows sync-kis` — the same metric codes from KIS Developers (K-6f), covering 6 of the 7
  (`short_selling_balance_quantity` is KRX-only). Per-ticker/date-range shaped rather than
  per-market-day, so it has its own service, its own `RunType`, and per-ticker checkpoints.
  `--plan-only` resolves the work without issuing a token or sending a request.
- `ops freshness-report` — read-only raw freshness status.
- `validate` — data-quality checks.

## Parquet compute pipeline (downstream, on-demand)

The derived layer (`stock_metric_fact`, `common_feature_daily_fact`, coverage /
readiness / freshness gates) is recomputed from the raw lake by DuckDB marts in
`research/etl/marts/` — **not** in Postgres. A human runs `bin/parquet-compute-all.sh`
(db sync-remote → raw parquet export → freshness gate → normalize/build-daily
marts → coverage/readiness → optional feat_*/labels). Mapping rules + the feature
catalog are pure code in `krx_collector.definitions/` (the marts import them);
only `common_feature_series` remains a Postgres table the collector seeds and the
mart shares via the lake (decision 7). Parity with the old Postgres path is frozen
in `tests/unit/golden/*.json` (differential mart tests). See `docs/operations.md`
"Parquet compute 파이프라인". Raw can come via either `--route local` (default;
`db sync-remote --full-refresh` → mirror → export) or `--route remote` (direct
sj2 capture through `db with-remote-dsn`, no local mirror) — see `bin/README.md`
and `docs/dev/20260730_refactor_dump/00_dual_route_raw_export_plan.md`.

## Architecture

Strict **ports & adapters (hexagonal)**. The dependency rule is the key invariant: **`domain/` and `service/` never import `adapters/` or `infra/`**. Wiring happens only in the CLI composition root (`cli/app.py`).

- **`domain/`** — pure dataclasses + `StrEnum`s, no framework deps. `enums.py` defines `Source`, `Market`, `RunType`, `RunStatus` (`running`/`success`/`partial`/`failed`) — central to audit logging.
- **`ports/`** — `typing.Protocol` interfaces (structural typing, not ABCs), one per concern: `universe`, `prices`, `storage`, `corp_codes`, `financials`, `share_info`, `xbrl`, `flows`, `common_features`.
- **`adapters/`** — provider implementations grouped by source: `universe_fdr`/`universe_pykrx`, `prices_pykrx`, `opendart_*`, `flows_krx`, `common_features_{pykrx,fdr,krx,ecos,fred}`.
- **`definitions/`** — pure data definitions (metric catalog/mapping rules, common-feature catalog/series) with no `Storage` dep, imported by both the `common seed` path and the DuckDB compute marts (refactor §3.0).
- **`service/`** — one use-case orchestrator per file (`sync_universe`, `backfill_daily`, `sync_dart_*`, `sync_common_features`, `sync_local_db`, …). These take ports as arguments. (The compute orchestrators `normalize_metrics`/`build_common_feature_daily_facts` were removed — recomputed by DuckDB marts.)
- **`infra/`** — `db_postgres/` (PostgresStorage + `remote_sync.py`), `calendar/` (KRX trading-day calendar, uses `docs/holidays_krx.csv`), `config/` (pydantic-settings singleton via `get_settings()`), `logging/`.
- **`util/pipeline.py`** — shared retry/jitter/throttle (`HumanThrottlePolicy`) and the **partial-run finalizer**. Read this before touching any ingestion flow.

### Cross-cutting patterns you must preserve

- **Raw vs derived two-layer model.** Each source writes immutable `*_raw` tables in Postgres. The derived layer (`stock_metric_fact`, `common_feature_daily_fact`) is **no longer a Postgres table** — it is recomputed from raw by the DuckDB marts in `research/etl/marts/` using the code-defined mapping rules / feature catalog. Keep ingestion (raw, Postgres) and compute (derived, parquet/DuckDB) separate.
- **Idempotent + skip-if-present.** Every sync uses `ON CONFLICT … DO UPDATE` and skips re-fetching when the equivalent row already exists (keyed per source — see README "중복 실행 방지" section). New ingestion code must follow this.
- **Audit via `ingestion_runs`.** Every run records a `RunType` and ends in one of the four `RunStatus`. On partial external-API failure the pipeline still exits cleanly with `status=partial` and per-failure counts. OpenDART runs additionally record multi-key rotation/rate-limit/status-code metrics. See `docs/operations.md` for interpretation.
- **OpenDART multi-key.** `OPENDART_API_KEY` (single) and/or `OPENDART_API_KEYS` (comma-separated) — a shared executor in `opendart_common` rotates keys on rate-limit/error. When all keys hit the daily limit the CLI exits with code `75` and resumes (skipping stored raw) next run.
- **Timezone is fixed `Asia/Seoul`** (`util/time.py`, `now_kst()`) — not configurable.
- **Schema source of truth** is `sql/postgres_ddl.sql` (applied by `db init` and `db sync-remote --all-tables`).

## Config & secrets

Settings load from `.env` (template: `.env.example`) via pydantic-settings. DB via `DB_DSN` or `DB_HOST/PORT/NAME/USER/PASSWORD`. OpenDART keys as above. KRX MDC login fallback uses `KRX_ID`/`KRX_PW`. `db sync-remote` reads remote DB creds from `/Users/whishaw/wss_p/stock_data_collector_secrets/db_info` by default.

`ALLOW_KRX_SCRAPING` (default `false`) gates the pykrx login path — the collection path KRX restricted this host for. The replacements are wired and verified, so leave it off; set it only for a deliberate one-off comparison. Two doors are deliberately *not* gated because they still have no replacement in prod: MDC direct (`flows sync`, `common sync --sources krx`) and FDR anonymous (`universe sync --source fdr`, which calls `data.krx.co.kr` twice per invocation just to read `max_work_dt` while reading its actual rows from a GitHub CSV cache).

`KIS_APP_KEY`/`KIS_APP_SECRET` (한국투자증권 오픈API) are in the **local `.env` only** as of 2026-08-16 — **prod does not have them yet**, so `flows sync-kis` cannot run there until someone adds them. They serve the KRX-replacement work (K 묶음): KRX restricted this host's IP on 2026-08-16 for a ToS violation, so the scraping path is being replaced by KRX Open API (N1/N3) plus KIS (`flows`). See `docs/operations.md` "KRX 접근 제한". Note the secrets directory holds **connection metadata** (`db_info`, `cronicle_info`) — API keys always go in `.env`, not there.

`AUTH_KEYS` (KRX Open API, comma-separated — two keys as of 2026-08-18) and `DATAGO_KEY` (공공데이터포털) are also local-only. Both are wired into `Settings` (`krx_openapi_auth_keys`, `datago_api_key`) but **no adapter reads them yet** — that adapter is K-4, the next piece of work. The KRX keys plus 16 approved endpoints were verified live on 2026-08-18; the response spec lives in `docs/dev/20260731_raw_features/02_data_expansion_plan/poc/krx_open_api.md` §4.1c. Two things to know before writing that adapter: a key alone is not enough (each endpoint needs its own 이용 신청, and an unapproved one returns `401 Unauthorized API Call` — distinct from a bad key's `Unauthorized Key`), and one `sto/stk_bydd_trd` call returns a whole market-day including **unadjusted OHLC**, so N1, N3, and K-7 collapse into a single request. Anything added to `.env` must get a `Settings` field: pydantic-settings forbids extras, so an undeclared key does not fail at first use — it fails *every* command.

Two KIS facts that are easy to get wrong, both measured live rather than read from docs:
**every access-token issuance sends the account holder a KakaoTalk notification**, so the token is cached on a host volume (`./state:/state`, `KIS_TOKEN_CACHE_PATH`) — the only volume the `collector` service has; and the **effective rate limit is 1 req/s, not the documented 20/s**, with throttle rejections arriving as *HTTP 500 carrying `EGW00201`* rather than 429.

## Docs & deploy

- `docs/architecture.md`, `docs/database.md`, `docs/operations.md` (cron schedule, runbook, partial-run recovery). `docs/dev/` holds dated design/implementation plans.
- `deploy/prod/bin/` holds the host-side wrapper scripts that run each pipeline stage in prod (Cronicle on sj2-server). `bin/dart-backfill-all-years.sh` runs the multi-year OpenDART backfill (default 2015→last year).
- CI: pushing to `main` builds/pushes `ghcr.io/sjleekor/sdc` via `.github/workflows/docker.yml`.

## Scope exclusions

Intraday (minute/hour) bars are out of scope (extension points stubbed in `ports/prices.py`). Selenium is intentionally not used.
