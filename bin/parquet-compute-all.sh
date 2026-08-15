#!/usr/bin/env bash
set -euo pipefail

# On-demand compute pipeline (refactor §3.4, §8.1 OQ1; dual-route §3.6 of
# docs/dev/20260730_refactor_dump/00_dual_route_raw_export_plan.md).
#
# Runs the whole "refresh raw -> recompute derived" chain a human invokes when
# they want fresh derived data. sj2 still collects raw automatically; only this
# compute half is on-demand (no scheduler). Raw can come from either route:
#
#   --route local (default)              --route remote
#   1) db sync-remote --full-refresh      1) (skipped -- no local mirror)
#      pull sj2 raw -> local mydb
#   2) raw-parquet-export                 2) db with-remote-dsn --
#      local mydb -> data_lake/...           raw-parquet-export-all.sh --route remote
#                                             (direct sj2 capture -> data_lake/...)
#   3) compute_all --source local_mydb    3) compute_all --source sj2_remote
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
  --route local|remote          Raw source (default: local).
                               local  -> db sync-remote --full-refresh, then export
                                         the local mirror (source=local_mydb).
                               remote -> skip the mirror entirely; capture directly
                                         from sj2 (source=sj2_remote, ~0 GB local
                                         disk, no --full-refresh wait). See the
                                         dual-route plan doc for the tradeoffs.
  --ssh-host HOST               SSH host for the remote route's DSN tunnel
                               (passthrough to `db with-remote-dsn`).
  --jobs N                      Export parallelism passthrough (default: 1 local /
                               3 remote; clamped to [1,4]).
  --from-step STEP             Start at sync|export|freshness|marts|reports|features.
                               (default: sync; --route remote has no sync step and
                               promotes a default/explicit "sync" to "export".)
  --skip-sync                  Alias for --from-step export (reuse the mirror).
  --features                   Also build feat_*/labels marts after the gates.
                               Feature and dataset caches are separated by source=.
  --end YYYY-MM-DD             Freshness reference date. Defaults to compute-time KST today.
  --required-coverage-ratio R  Readiness threshold (default 1.0).
  -h, --help                   Show this help.

The compute half (freshness/marts/reports/features) runs research/etl/compute_all.py.
EOF
}

snapshot_date="$(date '+%Y-%m-%d')"
route="local"
ssh_host=""
jobs=""
from_step="sync"
features=0
coverage_ratio="1.0"
end_date=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot-date) snapshot_date="$2"; shift 2 ;;
    --route) route="$2"; shift 2 ;;
    --ssh-host) ssh_host="$2"; shift 2 ;;
    --jobs) jobs="$2"; shift 2 ;;
    --from-step) from_step="$2"; shift 2 ;;
    --skip-sync) from_step="export"; shift ;;
    --features) features=1; shift ;;
    --end) end_date="$2"; shift 2 ;;
    --required-coverage-ratio) coverage_ratio="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 2 ;;
  esac
done

case "$route" in
  local|remote) ;;
  *) echo "Invalid --route: ${route} (expected local or remote)" >&2; exit 2 ;;
esac

# Ordered step gate: returns 0 when $1 should run given --from-step.
step_index() {
  case "$1" in
    sync) echo 0 ;; export) echo 1 ;; freshness) echo 2 ;;
    marts) echo 3 ;; reports) echo 4 ;; features) echo 5 ;;
    *) echo 99 ;;
  esac
}

if [[ "$route" == "remote" && "$from_step" == "sync" ]]; then
  log "route=remote has no sync step; promoting --from-step sync to export"
  from_step="export"
fi

start_idx="$(step_index "$from_step")"
should_run() { [[ "$(step_index "$1")" -ge "$start_idx" ]]; }

if should_run sync; then
  log "step 1/3: db sync-remote --full-refresh (raw + common_feature_series)"
  uv run krx-collector db sync-remote --full-refresh
fi

if should_run export; then
  log "step 2/3: raw-parquet export -> snapshot ${snapshot_date} (route=${route})"
  export_args=(--snapshot-date "${snapshot_date}" --route "${route}")
  [[ -n "$jobs" ]] && export_args+=(--jobs "$jobs")
  if [[ "$route" == "remote" ]]; then
    with_remote_dsn_args=(db with-remote-dsn)
    [[ -n "$ssh_host" ]] && with_remote_dsn_args+=(--ssh-host "$ssh_host")
    uv run krx-collector "${with_remote_dsn_args[@]}" -- \
      bin/raw-parquet-export-all.sh "${export_args[@]}"
  else
    bin/raw-parquet-export-all.sh "${export_args[@]}"
  fi
fi

# The compute half maps its own --from-step (freshness/marts/reports/features).
compute_from="freshness"
case "$from_step" in
  marts|reports|features) compute_from="$from_step" ;;
esac

compute_source="local_mydb"
[[ "$route" == "remote" ]] && compute_source="sj2_remote"

log "step 3/3: compute_all (from ${compute_from}, snapshot ${snapshot_date}, source ${compute_source})"
compute_args=(--snapshot-date "${snapshot_date}" --source "${compute_source}"
              --from-step "${compute_from}" --required-coverage-ratio "${coverage_ratio}")
[[ "$features" -eq 1 ]] && compute_args+=(--features)
[[ -n "$end_date" ]] && compute_args+=(--end "${end_date}")
uv run python -m research.etl.compute_all "${compute_args[@]}"

log "parquet-compute-all complete"
