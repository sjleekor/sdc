#!/usr/bin/env bash
set -euo pipefail

# On-demand compute pipeline (refactor §3.4, §8.1 OQ1).
#
# Runs the whole "refresh raw -> recompute derived" chain a human invokes when
# they want fresh derived data. sj2 still collects raw automatically; only this
# compute half is on-demand (no scheduler).
#
#   1) db sync-remote     pull sj2 raw + common_feature_series into local mydb
#   2) raw-parquet-export pg mydb -> data_lake/raw_postgres/<snapshot>/...
#   3) compute_all        freshness gate -> normalize/build-daily marts ->
#                         coverage/readiness gate -> (optional) feat_*/labels
#
# Gate failures exit non-zero with a stderr summary (interactive run; no notifier).

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
app_dir="${SDC_APP_DIR:-$(cd "$script_dir/.." && pwd)}"
cd "$app_dir"

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*"; }

usage() {
  cat <<'EOF'
Usage: bin/parquet-compute-all.sh [options]

Refresh the raw lake and recompute the derived marts + gates on DuckDB.

Options:
  --snapshot-date YYYY-MM-DD   Lake snapshot to write/read. Defaults to today.
  --from-step STEP             Start at sync|export|freshness|marts|reports|features.
                               (default: sync)
  --skip-sync                  Alias for --from-step export (reuse the mirror).
  --features                   Also build feat_*/labels marts after the gates.
  --required-coverage-ratio R  Readiness threshold (default 1.0).
  -h, --help                   Show this help.

The compute half (freshness/marts/reports/features) runs research/etl/compute_all.py.
EOF
}

snapshot_date="$(date '+%Y-%m-%d')"
from_step="sync"
features=0
coverage_ratio="1.0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot-date) snapshot_date="$2"; shift 2 ;;
    --from-step) from_step="$2"; shift 2 ;;
    --skip-sync) from_step="export"; shift ;;
    --features) features=1; shift ;;
    --required-coverage-ratio) coverage_ratio="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 2 ;;
  esac
done

# Ordered step gate: returns 0 when $1 should run given --from-step.
step_index() {
  case "$1" in
    sync) echo 0 ;; export) echo 1 ;; freshness) echo 2 ;;
    marts) echo 3 ;; reports) echo 4 ;; features) echo 5 ;;
    *) echo 99 ;;
  esac
}
start_idx="$(step_index "$from_step")"
should_run() { [[ "$(step_index "$1")" -ge "$start_idx" ]]; }

if should_run sync; then
  log "step 1/3: db sync-remote (raw + common_feature_series)"
  uv run krx-collector db sync-remote
fi

if should_run export; then
  log "step 2/3: raw-parquet export -> snapshot ${snapshot_date}"
  bin/raw-parquet-export-all.sh --snapshot-date "${snapshot_date}"
fi

# The compute half maps its own --from-step (freshness/marts/reports/features).
compute_from="freshness"
case "$from_step" in
  marts|reports|features) compute_from="$from_step" ;;
esac

log "step 3/3: compute_all (from ${compute_from}, snapshot ${snapshot_date})"
compute_args=(--snapshot-date "${snapshot_date}" --from-step "${compute_from}"
              --required-coverage-ratio "${coverage_ratio}")
[[ "$features" -eq 1 ]] && compute_args+=(--features)
uv run python -m research.etl.compute_all "${compute_args[@]}"

log "parquet-compute-all complete"
