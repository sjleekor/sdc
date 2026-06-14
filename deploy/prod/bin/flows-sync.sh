#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  flows sync
  --incremental
  --lookback-days "${FLOW_LOOKBACK_DAYS:-14}"
  --max-auto-range-days "${FLOW_MAX_AUTO_RANGE_DAYS:-30}"
)

if [[ "${FLOW_ALLOW_LARGE_RANGE:-0}" == "1" ]]; then
  args+=(--allow-large-range)
fi

if [[ -n "${FLOW_EXCLUDE_GROUPS:-}" ]]; then
  args+=(--exclude-groups "$FLOW_EXCLUDE_GROUPS")
fi

sdc_use_daily_lock_defaults
sdc_run_collector_with_lock krx_marketdata "${args[@]}"
