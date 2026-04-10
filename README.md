# krx-data-pipeline

Maintainable, production-oriented Korean stock data pipeline that:

1. **Syncs the KOSPI / KOSDAQ stock universe** (stock master) using both
   [FinanceDataReader](https://github.com/financedata-org/FinanceDataReader)
   and [pykrx](https://github.com/sharebook-kr/pykrx).
2. **Collects per-ticker daily OHLCV history** from listing date using pykrx.
3. **Stores everything in PostgreSQL** with a clean ports/adapters architecture
   that allows future file-based storage (CSV / Parquet) without refactoring
   core logic.

## Non-goals (current scope)

- **Intraday** (minute / hourly) collection is out of scope — extension points
  are designed but not implemented.
- **Selenium** is explicitly not used.

## Quickstart

### Prerequisites

- Python ≥ 3.12
- [uv](https://docs.astral.sh/uv/) package manager
- PostgreSQL (for production use)

### Setup

```bash
# 1. Install dependencies
uv sync

# 2. Configure environment
cp .env.example .env
# Edit .env with your database credentials and preferences

# 3. Initialise the database schema
krx-collector db init

# 4. Sync the stock universe
krx-collector universe sync --source fdr --markets kospi,kosdaq

# 5. Backfill daily OHLCV data
krx-collector prices backfill --market all --since-listing

# 6. Run validations
krx-collector validate --date 2025-01-15 --market all
```

> **Note:** All commands currently print "Not implemented yet" — this is a
> skeleton repository.  Adapter implementations will be added in subsequent
> iterations.

### Running via `python -m`

```bash
python -m krx_collector universe sync --source pykrx
```

### Development

```bash
# Install dev dependencies
uv sync --extra dev

# Run tests
uv run pytest

# Lint
uv run ruff check src/ tests/

# Format
uv run black src/ tests/
```

## Project structure

```
krx-data-pipeline/
├── .env.example                  # Environment template
├── pyproject.toml                # Project metadata & dependencies (uv)
├── sql/
│   └── postgres_ddl.sql          # Database schema
├── docs/
│   ├── architecture.md           # Architecture & data flow
│   ├── database.md               # Schema documentation
│   └── operations.md             # Runbook & cron examples
├── src/krx_collector/
│   ├── cli/app.py                # argparse CLI with subcommands
│   ├── domain/                   # Pure models & enums (no dependencies)
│   ├── ports/                    # Protocol interfaces (universe, prices, storage)
│   ├── adapters/                 # Concrete provider implementations (stubs)
│   ├── service/                  # Use-case orchestration
│   ├── infra/                    # Config, logging, calendar, DB
│   └── util/                     # Retry, timezone helpers
└── tests/
    ├── unit/
    └── integration/
```

## Architecture

See [docs/architecture.md](docs/architecture.md) for the full data-flow
diagram and ports/adapters rationale.

## License

MIT
