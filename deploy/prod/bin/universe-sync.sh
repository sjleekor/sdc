#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

sdc_run_daily_collector fdr universe sync --source fdr --markets kospi,kosdaq
