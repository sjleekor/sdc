#!/usr/bin/env bash
set -euo pipefail

# N3 — month-end historical universe snapshots (survivorship audit).
#
# Writes only stock_master_snapshot / _items; stock_master is never touched, so
# this cannot disturb the live universe or sync_universe's delisting diff.
# Snapshots are tagged Source.PYKRX_BACKFILL to keep them distinguishable.
#
# Idempotent on (as_of_date, source): re-running skips dates already captured.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  universe backfill-snapshots
  --markets "${UNIVERSE_SNAPSHOT_MARKETS:-kospi,kosdaq}"
  --rate-limit-seconds "${UNIVERSE_SNAPSHOT_RATE_LIMIT_SECONDS:-0.5}"
  --max-consecutive-failures "${UNIVERSE_SNAPSHOT_MAX_CONSECUTIVE_FAILURES:-5}"
)

if [[ -n "${UNIVERSE_SNAPSHOT_START:-}" ]]; then
  args+=(--start "$UNIVERSE_SNAPSHOT_START")
fi

if [[ -n "${UNIVERSE_SNAPSHOT_END:-}" ]]; then
  args+=(--end "$UNIVERSE_SNAPSHOT_END")
fi

if [[ "${UNIVERSE_SNAPSHOT_FORCE:-0}" == "1" ]]; then
  args+=(--force)
fi

sdc_run_daily_collector krx_marketdata "${args[@]}"
