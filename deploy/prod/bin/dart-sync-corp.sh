#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/sdc-wrapper.sh"

sdc_use_daily_lock_defaults
sdc_run_collector_with_lock opendart dart sync-corp
