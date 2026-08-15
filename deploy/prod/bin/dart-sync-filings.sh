#!/usr/bin/env bash
set -euo pipefail

# Raw disclosure-receipt history (list.json), standalone.
#
# sync-filings previously only ran as the last stage of dart-backfill-all-years.sh,
# so there was no way to run it on its own target set. S-1 needs exactly that:
# 1,302 of the 3,959 corps that ever had a ticker have no receipts at all, and
# they are the delisted ones -- the survivorship gap in the disclosure-activity
# features (N5-7) is precisely this set.
#
# Skip-if-present on (corp_code, year), so re-running costs a DB lookup per
# window and no OpenDART call. Exit 75 means every key hit its daily limit; the
# next run resumes.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

args=(
  dart sync-filings
  --universe-scope "${DART_FILINGS_UNIVERSE_SCOPE:-historical}"
  --rate-limit-seconds "${DART_FILINGS_RATE_LIMIT_SECONDS:-0.2}"
)

if [[ -n "${DART_FILINGS_YEARS:-}" ]]; then
  args+=(--years "$DART_FILINGS_YEARS")
fi

if [[ -n "${DART_FILINGS_TICKERS:-}" ]]; then
  args+=(--tickers "$DART_FILINGS_TICKERS")
fi

if [[ "${DART_FILINGS_FORCE:-0}" == "1" ]]; then
  args+=(--force)
fi

sdc_run_daily_collector opendart "${args[@]}"
