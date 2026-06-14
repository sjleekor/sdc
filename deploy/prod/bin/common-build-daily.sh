#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

END_DATE="${SDC_COMMON_END_DATE:-$(TZ=Asia/Seoul date +%F)}"
BUILD_LOOKBACK_DAYS="${SDC_COMMON_BUILD_LOOKBACK_DAYS:-120}"
REQUIRED_SOURCES="${SDC_COMMON_REQUIRED_SOURCES:-fdr,fred,ecos,krx}"

sdc_run_collector ops assert-common-freshness \
  --sources "$REQUIRED_SOURCES" \
  --end "$END_DATE" \
  --max-run-age-hours "${SDC_COMMON_SOURCE_MAX_AGE_HOURS:-30}" \
  --daily-max-lag-days "${SDC_COMMON_DAILY_MAX_LAG_DAYS:-2}" \
  --macro-max-lag-days "${SDC_COMMON_MACRO_MAX_LAG_DAYS:-45}"

sdc_run_collector common build-daily \
  --end "$END_DATE" \
  --incremental \
  --lookback-days "$BUILD_LOOKBACK_DAYS" \
  --max-auto-range-days "${SDC_COMMON_BUILD_MAX_AUTO_RANGE_DAYS:-240}"
