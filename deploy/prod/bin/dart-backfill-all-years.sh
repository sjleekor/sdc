#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

is_positive_year() {
  [[ "$1" =~ ^[0-9]{4}$ ]]
}

run_dart_sync() {
  if [[ "${SDC_DART_BACKFILL_EXCLUSIVE:-1}" == "1" ]]; then
    sdc_run_collector "$@"
  else
    sdc_run_collector_with_lock opendart "$@"
  fi
}

run_backfill() {
  local start_year end_year default_end_year reprt_codes fs_divs pull_image
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
  pull_image="${SDC_DART_BACKFILL_PULL_IMAGE:-1}"

  sdc_log "OpenDART backfill starting"
  sdc_log "app_dir=$SDC_APP_DIR"
  sdc_log "range=${end_year}..${start_year} reprt_codes=$reprt_codes fs_divs=$fs_divs"
  sdc_log "exclusive=${SDC_DART_BACKFILL_EXCLUSIVE:-1}"

  if [[ "$pull_image" == "1" ]]; then
    sdc_cd_app
    sdc_log "pull: $SDC_DOCKER_COMPOSE_CMD pull $SDC_COLLECTOR_SERVICE"
    sdc_compose pull "$SDC_COLLECTOR_SERVICE"
  fi

  sdc_log "Syncing OpenDART corp master"
  run_dart_sync dart sync-corp

  local year
  for year in $(seq "$end_year" -1 "$start_year"); do
    sdc_log "Backfilling OpenDART financials for ${year}"
    run_dart_sync dart sync-financials \
      --bsns-years "$year" \
      --reprt-codes "$reprt_codes" \
      --fs-divs "$fs_divs"

    sdc_log "Backfilling OpenDART share info for ${year}"
    run_dart_sync dart sync-share-info \
      --bsns-years "$year" \
      --reprt-codes "$reprt_codes"

    sdc_log "Backfilling OpenDART XBRL for ${year}"
    run_dart_sync dart sync-xbrl \
      --bsns-years "$year" \
      --reprt-codes "$reprt_codes"

    sdc_log "Normalizing metrics for ${year}"
    sdc_run_collector metrics normalize \
      --bsns-years "$year" \
      --reprt-codes "$reprt_codes"
  done

  sdc_log "OpenDART backfill completed"
}

if [[ "${SDC_DART_BACKFILL_EXCLUSIVE:-1}" == "1" ]]; then
  sdc_with_source_lock opendart run_backfill
else
  run_backfill
fi
