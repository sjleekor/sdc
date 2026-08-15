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

# Each call returns a whole market (~1,700 rows), which is a heavier ask than
# the per-ticker price calls the 0.1s default was tuned for. Pace the bulk
# backfill more conservatively unless the operator overrides it; the daily
# incremental run is only ~2 slices and can use whatever is configured.
args=(
  prices market-cap-backfill
  --market "${MARKET_CAP_MARKET:-all}"
  --rate-limit-seconds "${MARKET_CAP_RATE_LIMIT_SECONDS:-0.4}"
  --long-rest-interval "${MARKET_CAP_LONG_REST_INTERVAL:-200}"
  --long-rest-seconds "${MARKET_CAP_LONG_REST_SECONDS:-15}"
  --max-consecutive-failures "${MARKET_CAP_MAX_CONSECUTIVE_FAILURES:-5}"
)

if [[ -n "${MARKET_CAP_START:-}" ]]; then
  args+=(--start "$MARKET_CAP_START")
fi

if [[ -n "${MARKET_CAP_END:-}" ]]; then
  args+=(--end "$MARKET_CAP_END")
fi

if [[ "${MARKET_CAP_FORCE:-0}" == "1" ]]; then
  args+=(--force)
fi

sdc_run_daily_collector krx_marketdata "${args[@]}"
