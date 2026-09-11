"""Integration check: every model-02 feature column exists in its mart.

`02` §6's last-but-one item. Self-skips when the pinned snapshot's marts are not
on this host. It reads schemas only — one ``DESCRIBE`` per mart, no scan — so it
is cheap enough to run beside the unit suite.

The mapping in ``features.COLUMN_MART`` is hand-written from the feature
inventory, and a column that moved mart (or was renamed) would otherwise only
surface as a DuckDB binder error in the middle of a 20-minute panel build.
"""

from __future__ import annotations

import duckdb
import pytest
from research.etl.mart import is_materialized, mart_glob
from research.models._02_updown_prob import features as fx
from research.models._02_updown_prob.spec import lake_config

# FS3's two marts on this snapshot are the pre-v2 build (`02` §1.5): the columns
# are there, which is what this test checks, but E5 still needs them rebuilt.
_FS3_MARTS = {"feat_relation_stat", "feat_fin_risk"}


def _columns(con: duckdb.DuckDBPyConnection, glob: str) -> set[str]:
    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}', hive_partitioning=false)")
    return {row[0] for row in rows.fetchall()}


@pytest.fixture(scope="module")
def mart_columns() -> dict[str, set[str]]:
    config = lake_config()
    marts = sorted(set(fx.COLUMN_MART.values()))
    missing = [m for m in marts if not is_materialized(config, m)]
    if missing:
        pytest.skip(f"snapshot {config.snapshot_date} lacks marts: {missing}")
    con = duckdb.connect()
    return {m: _columns(con, mart_glob(config, m)) for m in marts}


def test_every_mapped_column_exists_in_its_mart(mart_columns) -> None:
    absent: list[str] = []
    for column, mart in fx.COLUMN_MART.items():
        if column not in mart_columns[mart]:
            absent.append(f"{mart}.{column}")
    assert absent == []


def test_the_mart_lag1_twins_the_builder_reads_exist(mart_columns) -> None:
    flow = mart_columns[fx.GROUP_VIEW["flow"]]
    for source in fx.FLOW_MART_LAG1_SOURCE.values():
        assert source in flow


def test_fs0_and_fs2_are_available_without_the_pre_v2_marts(mart_columns) -> None:
    """FS0-FS2 must not depend on the two marts E5 has to rebuild first."""
    used = {fx.COLUMN_MART[c] for c in fx.FS2_COLS if c in fx.COLUMN_MART}
    assert not used & _FS3_MARTS
