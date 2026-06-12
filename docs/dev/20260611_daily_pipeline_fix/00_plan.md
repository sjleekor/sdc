# Daily Pipeline Long-Run Fix Plan

## Summary

`sdc_daily_pipeline` ran for more than 24 hours because the daily KRX flow step is configured as a full-range sync:

```bash
docker compose run --rm collector flows sync --use-price-range
```

`--use-price-range` resolves the flow sync range from all stored `daily_ohlcv` rows. On sj2 production this became:

- price range: `2007-06-05` to `2026-06-10`
- trading days used by the job: `4929`
- markets: `KOSPI`, `KOSDAQ`
- `foreign_holding` work: `4929 * 2 = 9858` date-market requests

The job was not stuck. It was making slow KRX MDC requests and reprocessing a large historical range.

## Incident Notes

- Cronicle job: `jmq85vi9b02`
- Event: `sdc_daily_pipeline`
- Host: `sj2-server` / `192.168.0.11`
- Start time: `2026-06-10 23:25:48 KST`
- Long-running stage: `flows-sync.sh`
- Observed progress before abort: `foreign_holding processed=6107/9858`
- Observed errors: `0`
- Abort action on `2026-06-11`:
  - Cronicle API `abort_job` returned `{"code":0}`
  - The collector container still remained alive
  - Directly stopped the stale `ghcr.io/sjleekor/sdc:v0.8.7` collector container running `krx-collector flows sync --use-price-range`

## Root Cause

### 1. Daily flow sync uses the full price date range

Current production wrapper:

```bash
# deploy/prod/bin/flows-sync.sh
docker compose run --rm collector flows sync --use-price-range
```

The CLI resolves this to the full `daily_ohlcv` min/max date range when `--start` and `--end` are not supplied.

This makes the daily job behave like a historical backfill job.

### 2. Foreign holding skip logic compares historical rows with current active ticker counts

`sync_krx_security_flows()` treats a date-market request as complete only when the stored row count for that date and market is at least the number of current active tickers in that market.

For old dates, many current active tickers were not listed yet. KRX cannot return rows for those tickers on those dates, so the request is repeatedly considered incomplete.

Observed examples from sj2 production:

| date | market | stored KRX foreign holding tickers | current active tickers | gap |
|---|---:|---:|---:|---:|
| `2007-06-05` | `KOSDAQ` | `575` | `1822` | `1247` |
| `2007-06-05` | `KOSPI` | `614` | `947` | `333` |
| `2026-06-10` | `KOSDAQ` | `1822` | `1822` | `0` |
| `2026-06-10` | `KOSPI` | `947` | `947` | `0` |

The job log showed:

```text
foreign_complete_market_days=54/9858
```

This means most historical date-market pairs are classified as incomplete even after prior collection.

### 3. The daily event mixes operational freshness and historical repair

`sdc_daily_pipeline` currently tries to do three different jobs in one run:

1. refresh the listed universe
2. update latest prices incrementally
3. make KRX flow data match the full historical price range

The first two are daily maintenance. The third is a long-running backfill/repair task and should be scheduled separately.

## Goals

1. Make `sdc_daily_pipeline` finish predictably within a short daily window.
2. Preserve the ability to repair historical KRX flow coverage.
3. Avoid repeatedly re-fetching impossible historical date-market combinations.
4. Keep operational behavior visible through logs and DB coverage queries.
5. Avoid destructive data changes.

## Non-Goals

- Do not delete existing KRX/PYKRX rows.
- Do not rewrite the full flow ingestion schema in this fix.
- Do not make `sdc_daily_pipeline` responsible for historical completeness.
- Do not manually edit production files under `/home/whi/apps/sdc`; update `deploy/prod/` and deploy from the repo.

## Proposed Design

### A. Split daily freshness from historical backfill

Change the daily wrapper so it only syncs a bounded recent window.

Recommended default:

```bash
docker compose run --rm collector flows sync --start "$FLOW_START" --end "$FLOW_END"
```

Where:

- `FLOW_END`: latest available `daily_ohlcv.trade_date` or KST yesterday
- `FLOW_START`: `FLOW_END - 7 calendar days` by default

Rationale:

- A 7-day window catches market holidays, late KRX availability, and short outages.
- Daily runtime becomes proportional to recent dates instead of all dates since 2007.
- Existing service skip logic can still avoid already complete recent requests.

Implementation options:

1. Compute the range in `deploy/prod/bin/flows-sync.sh` with a small SQL query against `daily_ohlcv`.
2. Add CLI options such as `flows sync --recent-days 7 --use-price-range-end`.

Prefer option 2 if the code change is small, because it keeps date range resolution in the application rather than shell.

### B. Add a separate backfill wrapper/event

Create a dedicated production wrapper:

```text
deploy/prod/bin/flows-backfill-range.sh
```

Expected parameters:

```bash
FLOW_START=2026-05-01 FLOW_END=2026-06-10 /home/whi/apps/sdc/bin/flows-backfill-range.sh
```

Wrapper behavior:

- require explicit `FLOW_START` and `FLOW_END`
- fail fast if either is missing
- run `collector flows sync --start "$FLOW_START" --end "$FLOW_END"`
- optionally support `FLOW_TICKERS`

Cronicle:

- keep daily event bounded
- add/manual-run a separate backfill event only when needed
- do not schedule full-range backfill every day

### C. Fix foreign holding completeness semantics

Current check:

```text
stored row count for date/market >= current active ticker count for market
```

This is not valid for historical dates.

Better options, in preferred order:

1. Use listing-aware expected ticker counts if listing dates are available or can be inferred.
2. Track request completion by date/market/metric/source separately from row count.
3. Treat successful no-error KRX response as complete for that date/market even if row count is below current active ticker count.

Recommended near-term fix:

- Add a lightweight request-completion table or reuse ingestion-run detail records if available.
- Mark `foreign_holding_shares` date-market requests complete when the provider returns successfully.
- Skip completed requests by `(trade_date, market, metric_code, source)`.

This avoids repeated historical calls where KRX legitimately returns fewer rows than current active ticker count.

### D. Guard against accidental full-range daily runs

Add one or both safeguards:

1. CLI guard: if `flows sync --use-price-range` resolves to more than a configured maximum number of days, require an explicit flag such as `--allow-large-range`.
2. Wrapper guard: production daily wrapper refuses to run when the resolved window exceeds a small threshold.

Suggested defaults:

- daily max: `14` calendar days
- manual backfill max without override: `90` calendar days

## Implementation Plan

### Phase 1. Stop daily long-run behavior

1. Update `deploy/prod/bin/flows-sync.sh`.
2. Make it run only a recent bounded window.
3. Add logging that prints the resolved `FLOW_START` and `FLOW_END`.
4. Update `docs/deploy.md` to document that daily flow sync is recent-window only.
5. Deploy to sj2 with `./deploy/deploy_to_sj2.sh`.
6. Update Cronicle event if needed so it continues calling `/home/whi/apps/sdc/bin/flows-sync.sh`.

Acceptance criteria:

- `sdc_daily_pipeline` no longer invokes `flows sync --use-price-range`.
- A normal daily run targets at most 7 to 14 days.
- Logs include the exact date window.

### Phase 2. Add manual backfill path

1. Add `deploy/prod/bin/flows-backfill-range.sh`.
2. Require `FLOW_START` and `FLOW_END`.
3. Document manual Cronicle/API usage.
4. Optionally add a disabled Cronicle event for operator-triggered backfills.

Acceptance criteria:

- Historical repair can be run without changing the daily event.
- Missing date ranges can be repaired in controlled chunks.

### Phase 3. Fix skip/completeness logic

1. Add storage support for completed flow requests, or identify an existing run-detail table to reuse.
2. Mark foreign holding date-market requests complete after a successful provider response.
3. Check completion before row-count checks.
4. Keep row-count checks as a fallback only for recent data or for databases without completion records.
5. Add unit tests covering old dates where KRX returns fewer rows than current active tickers.

Acceptance criteria:

- A historical date-market request that successfully returned rows is skipped on the next run.
- `foreign_complete_market_days` is no longer tied only to current active ticker counts.
- Re-running a completed historical range mostly skips instead of refetching.

### Phase 4. Add safety guardrails

1. Add max-range validation to `flows sync`.
2. Allow large ranges only with an explicit option.
3. Add tests for accidental full-range runs.
4. Log a clear error message when the guard blocks a run.

Acceptance criteria:

- Daily wrapper cannot accidentally start a 2007-to-today sync.
- Manual backfill remains possible with explicit operator intent.

## Verification Queries

Run against sj2 production using the DB helper.

### Price range

```bash
.agents/skills/sdc-db/scripts/dbq.sh sj2 "
select min(trade_date) as price_min,
       max(trade_date) as price_max,
       count(distinct trade_date) as price_dates,
       count(distinct ticker) as price_tickers,
       count(*) as price_rows
from daily_ohlcv;"
```

### KRX flow coverage

```bash
.agents/skills/sdc-db/scripts/dbq.sh sj2 "
select source,
       metric_code,
       min(trade_date) as min_date,
       max(trade_date) as max_date,
       count(*) as rows,
       count(distinct trade_date) as dates,
       count(distinct ticker) as tickers
from krx_security_flow_raw
where source = 'KRX'
group by source, metric_code
order by metric_code;"
```

### Daily job window smoke check

After deployment, inspect the next `sdc_daily_pipeline` log and confirm:

```text
flows sync: start=<recent date>, end=<latest price date>
```

It must not show:

```text
Price range resolved: start=2007-06-05
```

## Operational Runbook

### If the daily job runs too long again

1. Inspect the Cronicle job log.
2. Identify the current phase and progress line.
3. If it is a full-range flow sync, abort the job from Cronicle.
4. Verify the collector container stopped.
5. If Cronicle abort leaves the container running, stop only the matching one-off collector container.

Read-only checks:

```bash
ssh whi@sj2-server 'ps -eo pid,ppid,etime,cmd | grep -E "flows sync|docker compose run|krx-collector" | grep -v grep || true'
ssh whi@sj2-server 'docker ps --format "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Command}}" | grep sdc-collector || true'
```

Mutation, only with explicit operator approval:

```bash
ssh whi@sj2-server 'docker stop <collector-container-id>'
```

### Backfill recommendation

Run backfills in small chunks, for example one month at a time:

```bash
FLOW_START=2026-05-01 FLOW_END=2026-05-31 /home/whi/apps/sdc/bin/flows-backfill-range.sh
```

Avoid running backfills at the same time as:

- `sdc_daily_pipeline`
- `sdc_daily_common_features`
- other DB-heavy jobs

## Risks

- A recent-window daily flow sync will not automatically repair old gaps.
- Adding request-completion tracking requires careful migration and tests.
- If KRX returns transient partial data for a recent date, a completion marker could hide a short-lived upstream issue. Mitigate by applying completion markers only after no provider error and by keeping a recent-window daily retry.

## Open Questions

1. Should the daily window be 7 days or 14 days?
2. Should the backfill wrapper be added as a disabled Cronicle event or kept as an operator script only?
3. Is there reliable listing-date data available in `stock_master`, or should request-completion tracking be the primary fix?
4. Should `investor` and `shorting` phases get the same request-completion treatment?

