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
fs_divs="${SDC_DART_BACKFILL_FS_DIVS:-CFS,OFS}"
collector_service="${SDC_DART_BACKFILL_COLLECTOR_SERVICE:-collector}"
pull_image="${SDC_DART_BACKFILL_PULL_IMAGE:-1}"
compose_cmd="${SDC_DOCKER_COMPOSE_CMD:-docker compose}"

read -r -a compose <<< "$compose_cmd"

log "OpenDART backfill starting in $app_dir"
log "Range: ${end_year} down to ${start_year}; reprt_codes=${reprt_codes}; fs_divs=${fs_divs}"

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
    --fs-divs "$fs_divs"

  log "Backfilling OpenDART share info for ${year}"
  "${compose[@]}" run --rm "$collector_service" dart sync-share-info \
    --bsns-years "$year" \
    --reprt-codes "$reprt_codes"

  log "Backfilling OpenDART XBRL for ${year}"
  "${compose[@]}" run --rm "$collector_service" dart sync-xbrl \
    --bsns-years "$year" \
    --reprt-codes "$reprt_codes"

  log "Normalizing metrics for ${year}"
  "${compose[@]}" run --rm "$collector_service" metrics normalize \
    --bsns-years "$year" \
    --reprt-codes "$reprt_codes"
done

log "OpenDART backfill completed"
