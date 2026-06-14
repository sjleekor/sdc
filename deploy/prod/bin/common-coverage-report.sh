#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

END_DATE="${SDC_COMMON_END_DATE:-$(TZ=Asia/Seoul date +%F)}"
READINESS_LOOKBACK_DAYS="${SDC_COMMON_READINESS_LOOKBACK_DAYS:-60}"
READINESS_START_DATE="${SDC_COMMON_READINESS_START_DATE:-$(sdc_date_minus_days "$END_DATE" "$READINESS_LOOKBACK_DAYS")}"

sdc_run_collector common coverage-report \
  --start "$READINESS_START_DATE" \
  --end "$END_DATE"
