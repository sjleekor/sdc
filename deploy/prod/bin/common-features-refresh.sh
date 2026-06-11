#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${SDC_APP_DIR:-$HOME/apps/sdc}"
END_DATE="${SDC_COMMON_END_DATE:-$(TZ=Asia/Seoul date +%F)}"
DAILY_LOOKBACK_DAYS="${SDC_COMMON_DAILY_LOOKBACK_DAYS:-45}"
MACRO_LOOKBACK_DAYS="${SDC_COMMON_MACRO_LOOKBACK_DAYS:-540}"
BUILD_LOOKBACK_DAYS="${SDC_COMMON_BUILD_LOOKBACK_DAYS:-120}"
READINESS_LOOKBACK_DAYS="${SDC_COMMON_READINESS_LOOKBACK_DAYS:-60}"
RATE_LIMIT_SECONDS="${SDC_COMMON_RATE_LIMIT_SECONDS:-0.2}"
SYNC_SOURCES="${SDC_COMMON_SYNC_SOURCES:-fdr,fred,ecos,krx}"
MACRO_SERIES="${SDC_COMMON_MACRO_SERIES:-macro_cpi,macro_ppi,macro_m2,macro_consumer_sentiment}"
REQUIRED_COVERAGE_RATIO="${SDC_COMMON_REQUIRED_COVERAGE_RATIO:-1.0}"

date_minus_days() {
  python3 - "$1" "$2" <<'PY'
from datetime import date, timedelta
import sys

end = date.fromisoformat(sys.argv[1])
days = int(sys.argv[2])
print((end - timedelta(days=days)).isoformat())
PY
}

run_collector() {
  docker compose run --rm collector "$@"
}

cd "$APP_DIR"

DAILY_START_DATE="${SDC_COMMON_DAILY_START_DATE:-$(date_minus_days "$END_DATE" "$DAILY_LOOKBACK_DAYS")}"
MACRO_START_DATE="${SDC_COMMON_MACRO_START_DATE:-$(date_minus_days "$END_DATE" "$MACRO_LOOKBACK_DAYS")}"
BUILD_START_DATE="${SDC_COMMON_BUILD_START_DATE:-$(date_minus_days "$END_DATE" "$BUILD_LOOKBACK_DAYS")}"
READINESS_START_DATE="${SDC_COMMON_READINESS_START_DATE:-$(date_minus_days "$END_DATE" "$READINESS_LOOKBACK_DAYS")}"

echo "== common feature refresh =="
echo "app_dir=$APP_DIR"
echo "end_date=$END_DATE"
echo "daily_sync=$DAILY_START_DATE..$END_DATE sources=$SYNC_SOURCES"
echo "macro_sync=$MACRO_START_DATE..$END_DATE series=$MACRO_SERIES"
echo "build=$BUILD_START_DATE..$END_DATE"
echo "readiness=$READINESS_START_DATE..$END_DATE required=$REQUIRED_COVERAGE_RATIO"

run_collector common seed-catalog --init-schema

run_collector common sync \
  --sources "$SYNC_SOURCES" \
  --start "$DAILY_START_DATE" \
  --end "$END_DATE" \
  --rate-limit-seconds "$RATE_LIMIT_SECONDS"

run_collector common sync \
  --sources ecos \
  --series "$MACRO_SERIES" \
  --start "$MACRO_START_DATE" \
  --end "$END_DATE" \
  --rate-limit-seconds "$RATE_LIMIT_SECONDS" \
  --force

run_collector common build-daily \
  --start "$BUILD_START_DATE" \
  --end "$END_DATE"

run_collector common coverage-report \
  --start "$READINESS_START_DATE" \
  --end "$END_DATE"

run_collector common readiness-report \
  --start "$READINESS_START_DATE" \
  --end "$END_DATE" \
  --required-coverage-ratio "$REQUIRED_COVERAGE_RATIO" \
  --fail-on-not-ready

echo "== common feature refresh complete =="
