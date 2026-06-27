---
name: sdc-db
description: Use when inspecting the stock_data_collector / sdc PostgreSQL data — the local DB (mydb, a manual mirror) and the sj2-server production DB (krx_data, the real collector host). Covers connecting with psql via the dbq.sh helper, listing tables, checking row counts and date/year coverage (daily_ohlcv, krx_security_flow_raw, dart_*_raw, stock_metric_fact, etc.), and comparing local vs sj2-server. Trigger on requests to query the database, verify collected data ranges/coverage, count rows, compare local and production data, or answer "어떤 기간/연도까지 수집되어 있나" questions.
---

# SDC Database Inspection

Read-only inspection of the two SDC PostgreSQL databases. Default to `SELECT`/`\d` only; treat any write (`INSERT/UPDATE/DELETE/DDL/TRUNCATE`) as a mutation that needs explicit current-turn user authorization.

There are two databases:

| Target | DB | Where it lives | Role |
|---|---|---|---|
| `local` | `mydb` @ `localhost:5432` | repo `.env` (`DB_DSN`) | Manual mirror, synced by hand from sj2-server |
| `sj2`   | `krx_data` @ `sj2-server:5432` (container `sdc-postgres`) | `…/stock_data_collector_secrets/db_info` | Real collection host (source of truth) |

`sj2-server` resolves to `192.168.0.11` via `/etc/hosts` / `~/.ssh/config`. The local DB usually lags sj2-server by the manual sync interval, so for the latest data prefer `sj2`; use `local` for fast iteration or when offline.

## Credentials

Never copy secret values into committed files, skill files, logs, or final responses.

- `local`: connection string is in the repo `.env` line `DB_DSN=...`.
- `sj2`: credentials are in the secrets file (outside the repo):

  ```text
  /Users/whishaw/wss_p/stock_data_collector_secrets/db_info
  ```

  It contains `Server Host`, `Host Port`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`.

If a credentials file is missing, stop and ask the user. Do not guess or hard-code values.

## Quick start — use the helper

A helper resolves credentials at call time and passes them to `psql` without printing them:

```bash
.agents/skills/sdc-db/scripts/dbq.sh <local|sj2> "<SQL>"
```

Examples:

```bash
# Row count on each DB.
.agents/skills/sdc-db/scripts/dbq.sh local "select count(*) from daily_ohlcv;"
.agents/skills/sdc-db/scripts/dbq.sh sj2   "select count(*) from daily_ohlcv;"

# Run a .sql file.
.agents/skills/sdc-db/scripts/dbq.sh sj2 -f /tmp/range_check.sql

# Pipe SQL via stdin.
echo "select now();" | .agents/skills/sdc-db/scripts/dbq.sh local
```

The helper auto-adds the keg-only `libpq` psql (`/opt/homebrew/opt/libpq/bin`) to `PATH` if needed. Override the secrets location with `SDC_SECRETS_DIR` if the repo is checked out elsewhere.

### Manual psql (fallback, if you must)

```bash
export PATH="/opt/homebrew/opt/libpq/bin:$PATH"
# local
psql "$(grep '^DB_DSN=' .env | cut -d= -f2-)" -c '\dt'
# sj2 (read creds from db_info into PGPASSWORD; do not echo)
```

## Common inspection queries

```sql
-- Time-series coverage (siseong/flow).
select min(trade_date), max(trade_date), count(*),
       count(distinct trade_date), count(distinct ticker) from daily_ohlcv;
select min(trade_date), max(trade_date), count(*), count(distinct ticker)
  from krx_security_flow_raw;

-- Per-year ticker coverage.
select extract(year from trade_date)::int yr,
       count(distinct ticker), count(*) from daily_ohlcv group by 1 order by 1;

-- DART business-year distribution (financials / xbrl / share / metrics).
select bsns_year, count(*), count(distinct corp_code)
  from dart_financial_statement_raw group by bsns_year order by bsns_year;
select bsns_year, count(*) from dart_xbrl_document       group by bsns_year order by bsns_year;
select bsns_year, count(*) from dart_xbrl_fact_raw        group by bsns_year order by bsns_year;
select bsns_year, count(*) from dart_share_count_raw      group by bsns_year order by bsns_year;
select bsns_year, count(*) from dart_shareholder_return_raw group by bsns_year order by bsns_year;
select bsns_year, count(*) from stock_metric_fact         group by bsns_year order by bsns_year;
```

To compare `local` vs `sj2`, run the same query against both targets and diff the results. See the worked example in
`docs/dev/20260606_data_year_range_verify/data_year_range_verification.md`, and `docs/database.md` for the schema.

## Safety rules

- Read-only by default: `SELECT`, `\d`, `\dt`, `EXPLAIN` (without `ANALYZE` on writes).
- Any `INSERT/UPDATE/DELETE/TRUNCATE/ALTER/DROP/CREATE` requires explicit current-turn user approval; state the statement in plain text first.
- Prefer aggregate/`LIMIT`ed queries; avoid dumping full large tables.
- Never print credentials or full connection strings containing passwords.

## Reporting

1. Name the target used (`local` or `sj2`) and that sj2 is the source of truth.
2. Report the concrete numbers asked for (ranges, counts, per-year coverage).
3. If `local` and `sj2` differ, call out the gap (likely manual-sync lag).
4. Mention any missing secret/blocked connection and what was needed.
