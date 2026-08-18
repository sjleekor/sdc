#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

# Still --source fdr, and that is a pending item rather than a choice (K-5).
#
# fdr reads its listing rows from a GitHub CSV cache, but calls
# data.krx.co.kr/comm/bldAttendant/executeForResourceBundle.cmd twice per
# invocation — the second one a duplicate — purely to read max_work_dt. Four
# anonymous MDC requests a day, outside our throttle and outside our
# accounting.
#
# `--source krx-openapi` replaces it with the official endpoint, and brings a
# real LIST_DD instead of FDR's best-effort listing-date column. It needs
# AUTH_KEYS in this host's .env, which prod does not have yet. Flip this line
# in the same change that adds the key.
sdc_run_daily_collector fdr universe sync --source fdr --markets kospi,kosdaq
