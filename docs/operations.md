# Operations Guide

## Daily Schedule

KRX market hours: 09:00–15:30 KST.  Run the pipeline **after market close**
to ensure complete data for the day.

### Sample cron schedule (KST)

```cron
# ┌───── min  ┌───── hour  ┌───── day  ┌───── month  ┌───── dow
# │           │             │           │              │
# Universe sync — once daily at 16:00 KST (weekdays)
  0  16  *  *  1-5  cd /opt/krx-data-pipeline && krx-collector universe sync --source fdr

# Daily OHLCV backfill — at 16:30 KST (weekdays)
  30 16  *  *  1-5  cd /opt/krx-data-pipeline && krx-collector prices backfill --market all --since-listing

# Validation — at 17:00 KST (weekdays)
  0  17  *  *  1-5  cd /opt/krx-data-pipeline && krx-collector validate --market all
```

> **Tip:** Set `TZ=Asia/Seoul` in the crontab or use systemd timers with
> `OnCalendar=` to avoid UTC confusion.

## Runbook

### Re-running a backfill safely

The backfill is **idempotent** thanks to `ON CONFLICT … DO UPDATE` upserts.
Re-running with the same parameters will overwrite existing rows with fresh
data — no duplicates are created.

```bash
# Re-backfill a specific ticker from a specific date
krx-collector prices backfill --tickers 005930 --start 2024-01-01 --end 2024-12-31

# Re-backfill all tickers for a single market
krx-collector prices backfill --market kospi --since-listing
```

### Full universe refresh

If stock_master is suspected to be stale or inconsistent:

```bash
krx-collector universe sync --source fdr --full-refresh
```

This replaces all rows rather than computing an incremental diff.

### Validating data quality

```bash
# Validate a specific date
krx-collector validate --date 2024-06-15 --market all

# Validate today (default)
krx-collector validate
```

Validation checks (when implemented):
1. **OHLC sanity**: low ≤ open ≤ high, low ≤ close ≤ high, prices > 0.
2. **Missing days**: Gaps vs. the trading calendar (weekdays minus holidays).
3. **Universe drift**: Record count changed by > 5% vs. previous snapshot.

### Database initialisation

```bash
# Create tables (idempotent — uses CREATE TABLE IF NOT EXISTS)
krx-collector db init
```

### Checking ingestion history

```sql
-- Last 10 runs
SELECT run_id, run_type, started_at, ended_at, status, counts
FROM ingestion_runs
ORDER BY started_at DESC
LIMIT 10;

-- Failed runs
SELECT * FROM ingestion_runs WHERE status = 'failed' ORDER BY started_at DESC;
```

## Monitoring

### Key metrics to track

- `ingestion_runs.status = 'failed'` count per day.
- `stock_master` row count (should be stable ± 5%).
- `daily_ohlcv` row count growth per day (~2,500 new rows on a trading day).
- Backfill duration (wall-clock time).

### Alerting suggestions

- Alert if any `ingestion_runs` row has `status = 'failed'`.
- Alert if universe sync record_count drops by > 10%.
- Alert if no new `daily_ohlcv` rows appear on a weekday (non-holiday).

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `NotImplementedError` on any command | Adapter stubs not yet implemented | Implement the adapter (see TODO comments) |
| Connection refused on DB | PostgreSQL not running or wrong DSN | Check `.env` DB settings, verify `pg_isready` |
| Rate-limited by KRX | Too many requests | Increase `RATE_LIMIT_SECONDS` in `.env` |
| Missing holidays in validation | `docs/holidays_krx.csv` not populated | Add KRX holiday dates to the CSV |
| `JSONDecodeError` during sync | KRX website changed or blocked your IP | Use an alternative proxy, or wait for pykrx/FDR library updates to support new KRX authentication changes |
