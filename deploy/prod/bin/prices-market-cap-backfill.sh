#!/usr/bin/env bash
set -euo pipefail

# N1 — daily KRX market cap / trading value / listed shares.
#
# Uses the krx_marketdata source lock: this hits the same KRX endpoints as the
# price and flow collectors, and running them concurrently is what produced the
# exit-75 lock conflicts before.
#
# The service resolves trading days from the KRX calendar and skips slices it
# has already completed, so re-running a range is cheap and idempotent.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  prices market-cap-backfill
  --market "${MARKET_CAP_MARKET:-all}"
)

if [[ -n "${MARKET_CAP_START:-}" ]]; then
  args+=(--start "$MARKET_CAP_START")
fi

if [[ -n "${MARKET_CAP_END:-}" ]]; then
  args+=(--end "$MARKET_CAP_END")
fi

if [[ -n "${MARKET_CAP_RATE_LIMIT_SECONDS:-}" ]]; then
  args+=(--rate-limit-seconds "$MARKET_CAP_RATE_LIMIT_SECONDS")
fi

if [[ -n "${MARKET_CAP_LONG_REST_INTERVAL:-}" ]]; then
  args+=(--long-rest-interval "$MARKET_CAP_LONG_REST_INTERVAL")
fi

if [[ -n "${MARKET_CAP_LONG_REST_SECONDS:-}" ]]; then
  args+=(--long-rest-seconds "$MARKET_CAP_LONG_REST_SECONDS")
fi

if [[ "${MARKET_CAP_FORCE:-0}" == "1" ]]; then
  args+=(--force)
fi

sdc_run_daily_collector krx_marketdata "${args[@]}"
