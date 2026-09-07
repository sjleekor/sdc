#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
app_dir="${SDC_APP_DIR:-$(cd "$script_dir/.." && pwd)}"

cd "$app_dir"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*"
}

is_positive_year() {
  [[ "$1" =~ ^[0-9]{4}$ ]]
}

start_year="${SDC_DART_BACKFILL_START_YEAR:-2015}"
default_end_year=$(( $(date +%Y) - 1 ))
if [[ "${SDC_DART_BACKFILL_INCLUDE_CURRENT_YEAR:-0}" == "1" ]]; then
  default_end_year="$(date +%Y)"
fi
end_year="${SDC_DART_BACKFILL_END_YEAR:-$default_end_year}"

if ! is_positive_year "$start_year"; then
  printf 'Invalid SDC_DART_BACKFILL_START_YEAR: %s\n' "$start_year" >&2
  exit 2
fi
if ! is_positive_year "$end_year"; then
  printf 'Invalid SDC_DART_BACKFILL_END_YEAR: %s\n' "$end_year" >&2
  exit 2
fi
if (( start_year > end_year )); then
  printf 'Invalid backfill year range: start_year=%s end_year=%s\n' "$start_year" "$end_year" >&2
  exit 2
fi

reprt_codes="${SDC_DART_BACKFILL_REPRT_CODES:-11011,11012,11013,11014}"
# Which universe the three per-business-year stages target. The CLI default is
# `current`, and this script never overrode it — which is the whole reason the
# S-1 gap exists: every DART raw table covers ~2% of the 1,330 delisted names,
# and a 2016 cross-section is missing 13.9% of its rows
# (poc/survivorship_gap.md). `historical` targets every corp that ever carried
# a ticker (3,959 vs 2,657); corpCode.xml keeps stock_code after delisting, so
# the mapping was always there and only the filter was in the way.
#
# Left at the CLI default so no scheduled job changes behaviour by upgrading;
# the S-1 remainder backfill (F-9.3) sets it to `historical` explicitly.
universe_scope="${SDC_DART_BACKFILL_UNIVERSE_SCOPE:-current}"
fs_divs="${SDC_DART_BACKFILL_FS_DIVS:-CFS,OFS}"
collector_service="${SDC_DART_BACKFILL_COLLECTOR_SERVICE:-collector}"
pull_image="${SDC_DART_BACKFILL_PULL_IMAGE:-1}"
compose_cmd="${SDC_DOCKER_COMPOSE_CMD:-docker compose}"

# dart_filing_receipt_raw is keyed by *calendar receipt* year, not business
# year — a FY2025 annual report is filed in calendar 2026 — so the filing stage
# runs its own range, up to the current calendar year rather than to end_year.
collect_filings="${SDC_DART_BACKFILL_FILINGS:-1}"
filings_rate="${SDC_DART_BACKFILL_FILINGS_RATE_LIMIT:-0.5}"
current_year="$(date +%Y)"
filings_end_year="${SDC_DART_BACKFILL_FILINGS_END_YEAR:-$current_year}"

if ! is_positive_year "$filings_end_year"; then
  printf 'Invalid SDC_DART_BACKFILL_FILINGS_END_YEAR: %s\n' "$filings_end_year" >&2
  exit 2
fi

read -r -a compose <<< "$compose_cmd"

log "OpenDART backfill starting in $app_dir"
log "Range: ${end_year} down to ${start_year}; reprt_codes=${reprt_codes}; fs_divs=${fs_divs}"
log "Universe scope: ${universe_scope}"
log "Filing receipts: enabled=${collect_filings} range=${filings_end_year} down to ${start_year}"
log "This script collects OpenDART raw only; derived metric marts are recomputed by bin/parquet-compute-all.sh"

if [[ "$pull_image" == "1" ]]; then
  log "Pulling collector image"
  "${compose[@]}" pull "$collector_service"
fi

log "Syncing OpenDART corp master"
"${compose[@]}" run --rm "$collector_service" dart sync-corp

for year in $(seq "$end_year" -1 "$start_year"); do
  log "Backfilling OpenDART financials for ${year}"
  "${compose[@]}" run --rm "$collector_service" dart sync-financials \
    --bsns-years "$year" \
    --reprt-codes "$reprt_codes" \
    --fs-divs "$fs_divs" \
    --universe-scope "$universe_scope"

  log "Backfilling OpenDART share info for ${year}"
  "${compose[@]}" run --rm "$collector_service" dart sync-share-info \
    --bsns-years "$year" \
    --reprt-codes "$reprt_codes" \
    --universe-scope "$universe_scope"

  log "Backfilling OpenDART XBRL for ${year}"
  "${compose[@]}" run --rm "$collector_service" dart sync-xbrl \
    --bsns-years "$year" \
    --reprt-codes "$reprt_codes" \
    --universe-scope "$universe_scope"

done

# Filing receipts run after the three per-business-year stages: they feed the
# Phase B SUE original-filing source only, so on a quota exit the metrics every
# other consumer needs are already in.
if [[ "$collect_filings" == "1" ]]; then
  filings_years=()
  for year in $(seq "$filings_end_year" -1 "$start_year"); do
    # A stored past year is skipped forever; the current year is re-fetched on
    # every run by design, so it goes last and ends up the freshest.
    if (( year != current_year )); then
      filings_years+=("$year")
    fi
  done
  if (( filings_end_year >= current_year && start_year <= current_year )); then
    filings_years+=("$current_year")
  fi

  for year in ${filings_years[@]+"${filings_years[@]}"}; do
    log "Backfilling OpenDART filing receipts for ${year}"
    "${compose[@]}" run --rm "$collector_service" dart sync-filings \
      --years "$year" \
      --rate-limit-seconds "$filings_rate"
  done
fi

log "OpenDART backfill completed; run bin/parquet-compute-all.sh when derived marts are needed"
