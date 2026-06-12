#!/usr/bin/env bash
set -euo pipefail
cd "$HOME/apps/sdc"

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

docker compose run --rm collector "${args[@]}"
