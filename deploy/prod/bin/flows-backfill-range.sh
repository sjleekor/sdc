#!/usr/bin/env bash
set -euo pipefail
cd "$HOME/apps/sdc"

if [[ -z "${FLOW_START:-}" ]]; then
  echo "FLOW_START is required (YYYY-MM-DD)." >&2
  exit 2
fi

if [[ -z "${FLOW_END:-}" ]]; then
  echo "FLOW_END is required (YYYY-MM-DD)." >&2
  exit 2
fi

args=(
  flows sync
  --start "$FLOW_START"
  --end "$FLOW_END"
)

if [[ -n "${FLOW_TICKERS:-}" ]]; then
  args+=(--tickers "$FLOW_TICKERS")
fi

if [[ -n "${FLOW_MAX_PRICE_RANGE_DAYS:-}" ]]; then
  args+=(--max-price-range-days "$FLOW_MAX_PRICE_RANGE_DAYS")
fi

if [[ "${FLOW_ALLOW_LARGE_RANGE:-0}" == "1" ]]; then
  args+=(--allow-large-range)
fi

docker compose run --rm collector "${args[@]}"
