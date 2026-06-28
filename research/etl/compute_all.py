"""On-demand compute orchestrator: raw lake -> derived marts -> gates (§3.4, OQ1).

Single entrypoint a human runs to refresh + recompute the derived layer from the
raw lake, replacing the sj2 compute Cronicle events. Steps (each gated):

    freshness -> marts (normalize + build-daily) -> coverage/readiness -> features

The raw mirror + parquet export run in the wrapping shell (db sync-remote +
raw-parquet-export-all.sh); this module assumes the raw lake for ``--snapshot-date``
already exists. Gate failures print a human-readable summary to stderr and exit
non-zero (no notifier — interactive run, OQ1). Nothing is written back to Postgres.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from research.etl.config import EngineOptions, LakeConfig
from research.etl.lake import (
    _common_feature_calendars,
    connect,
    register_derived_marts,
    register_views,
)
from research.etl.marts import reports

_SMF_RAW_INPUTS = [
    "daily_ohlcv",
    "dart_financial_statement_raw",
    "dart_share_count_raw",
    "dart_shareholder_return_raw",
    "dart_xbrl_fact_raw",
    "dart_corp_master",
]
_CF_RAW_INPUTS = ["common_feature_observation_raw"]

_STEPS = ("freshness", "marts", "reports", "features")


def _eprint(*args: object) -> None:
    print(*args, file=sys.stderr)


def _register_series_if_present(con, cfg: LakeConfig) -> None:
    """Register common_feature_series opportunistically (decision 7 fallback)."""
    from research.etl.lake import _glob_has_files

    glob = cfg.table_glob("common_feature_series")
    if _glob_has_files(con, glob):
        register_views(con, cfg, tables=["common_feature_series"])


def run(
    *,
    snapshot_date: str | None,
    from_step: str = "freshness",
    end: date | None = None,
    required_coverage_ratio: float = 1.0,
    threads: int = 4,
    memory_limit: str = "4GB",
    with_features: bool = False,
) -> int:
    """Run the compute pipeline from ``from_step``. Returns a process exit code."""
    if from_step not in _STEPS:
        _eprint(f"unknown --from-step {from_step!r}; expected one of {_STEPS}")
        return 2
    start_idx = _STEPS.index(from_step)

    cfg = LakeConfig(
        snapshot_date=snapshot_date or LakeConfig().snapshot_date,
        engine=EngineOptions(threads=threads, memory_limit=memory_limit),
    )
    if not cfg.raw_root.exists():
        _eprint(f"raw lake not present at {cfg.raw_root}; run sync + export first")
        return 1

    con = connect(cfg)
    register_views(con, cfg, tables=_SMF_RAW_INPUTS + _CF_RAW_INPUTS)
    _register_series_if_present(con, cfg)
    _, feature_dates = _common_feature_calendars(con)
    gate_end = end or (feature_dates[-1] if feature_dates else date.today())

    # 1) freshness gate (raw inputs fresh enough?)
    if start_idx <= _STEPS.index("freshness"):
        fr = reports.freshness_violations(con, end=gate_end)
        if not fr.ok:
            _eprint(f"FRESHNESS GATE FAILED ({len(fr.violations)} violations):")
            for v in fr.violations:
                _eprint(f"  - [{v.series_id or v.check}] {v.message}")
            return 1
        print(f"freshness OK: {fr.checked_series} series fresh as of {gate_end}")

    # 2) derived marts (normalize + build-daily)
    if start_idx <= _STEPS.index("marts"):
        created = register_derived_marts(con, cfg)
        smf_n = con.execute("SELECT count(*) FROM stock_metric_fact").fetchone()[0]
        cfdf_n = con.execute("SELECT count(*) FROM common_feature_daily_fact").fetchone()[0]
        print(f"marts built: {created} (smf={smf_n}, cfdf={cfdf_n})")
    else:
        register_derived_marts(con, cfg)  # need the views for later steps

    # 3) coverage + readiness gates
    if start_idx <= _STEPS.index("reports"):
        rdy = reports.readiness_report(
            con, feature_dates=feature_dates, required_coverage_ratio=required_coverage_ratio
        )
        not_ready = [r for r in rdy if not r.ready]
        cov = reports.coverage_report(con, feature_dates=feature_dates)
        ready_n = len(rdy) - len(not_ready)
        print(f"coverage: {len(cov)} features; readiness: {ready_n}/{len(rdy)} ready")
        if not_ready:
            _eprint(f"READINESS GATE: {len(not_ready)} features not ready:")
            for r in not_ready[:20]:
                _eprint(
                    f"  - {r.feature_code} (coverage={r.coverage_ratio}): {', '.join(r.blockers)}"
                )
            return 1

    # 4) optional downstream feature/label marts
    if with_features and start_idx <= _STEPS.index("features"):
        _build_features(con, cfg)
        print("feature/label marts built")

    print("compute pipeline OK")
    return 0


def _build_features(con, cfg: LakeConfig) -> None:
    """Build the feat_*/labels marts on top of the derived marts (optional step)."""
    from research.etl.calendar import materialize_calendar
    from research.etl.features import common as cf_feat
    from research.etl.features import fin_pit
    from research.etl.universe import UniverseFilter, build_universe_sql

    register_views(con, cfg, tables=["krx_security_flow_raw"])
    materialize_calendar(con, cfg)
    con.execute(
        f"CREATE OR REPLACE VIEW dim_universe_daily AS {build_universe_sql(UniverseFilter())}"
    )
    con.execute(f"CREATE OR REPLACE VIEW feat_fin_pit AS {fin_pit.build_fin_pit_sql()}")
    con.execute(f"CREATE OR REPLACE VIEW feat_common AS {cf_feat.build_common_sql()}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date", default=None)
    parser.add_argument("--from-step", default="freshness", choices=_STEPS)
    parser.add_argument("--end", default=None, help="freshness gate end date YYYY-MM-DD")
    parser.add_argument("--required-coverage-ratio", type=float, default=1.0)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="4GB")
    parser.add_argument("--features", action="store_true", help="also build feat_*/labels")
    args = parser.parse_args(argv)

    end = date.fromisoformat(args.end) if args.end else None
    return run(
        snapshot_date=args.snapshot_date,
        from_step=args.from_step,
        end=end,
        required_coverage_ratio=args.required_coverage_ratio,
        threads=args.threads,
        memory_limit=args.memory_limit,
        with_features=args.features,
    )


if __name__ == "__main__":
    raise SystemExit(main())
