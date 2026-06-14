#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  metrics normalize
  --incremental
  --lookback-years "${SDC_METRICS_NORMALIZE_LOOKBACK_YEARS:-2}"
)

if [[ -n "${SDC_METRICS_NORMALIZE_BATCH_SIZE:-}" ]]; then
  args+=(--batch-size "$SDC_METRICS_NORMALIZE_BATCH_SIZE")
fi

sdc_run_collector "${args[@]}"
