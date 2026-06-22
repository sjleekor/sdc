# Raw Parquet Exporter Benchmark - 2026-06-19

## Scope

- Target DB: local `mydb` mirror. The sj2 PostgreSQL database remains the source of truth.
- Exporter: `tools/raw-parquet-exporter`
- Table: `dart_xbrl_fact_raw`
- Range: `raw_id >= 1 AND raw_id < 1000001`
- Rows: 1,000,000
- Snapshot date: `2026-06-19`

## Command

```bash
/usr/bin/time -l cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  --log-level error \
  export \
  --tables dart_xbrl_fact_raw \
  --start-raw-id 1 \
  --chunk-rows 1000000 \
  --batch-rows 65536 \
  --max-rows-per-file 200000 \
  --force
```

## Result

| Metric | Value |
|---|---:|
| Exported rows | 1,000,000 |
| Parquet files | 5 |
| Wall time | 22.68 sec |
| Throughput | ~44,100 rows/sec |
| User CPU | 23.01 sec |
| System CPU | 2.66 sec |
| Max RSS | 573,292,544 bytes |
| Total Parquet bytes | 46,126,237 bytes |
| Min file bytes | 9,040,353 bytes |
| Max file bytes | 9,582,006 bytes |

All five files landed under:

```text
data_lake/raw_postgres/snapshot_date=2026-06-19/source=local_mydb/dart_xbrl_fact_raw/schema_version=1/bsns_year=2025/reprt_code=11011/
```

This run used `--max-rows-per-file 200000`, so the file split was row-count driven rather than size driven.

## Row Count Validation

Manifest validation:

```text
manifest_rows = 1,000,000
parquet_rows = 1,000,000
files_checked = 5
passed = true
```

DuckDB Parquet count:

```text
rows = 1,000,000
files = 5
min_raw_id = 1
max_raw_id = 1,000,000
```

PostgreSQL source count:

```text
rows = 1,000,000
min_raw_id = 1
max_raw_id = 1,000,000
```

## Sample Value Validation

Compared PostgreSQL source rows and Parquet rows for `raw_id in (1, 500000, 1000000)`.

Checked columns:

- `raw_id`
- `corp_code`
- `ticker`
- `bsns_year`
- `reprt_code`
- `rcept_no`
- `concept_id`
- `context_id`
- `unit_measure`
- `decimals`
- `value_numeric`
- `value_text`
- `is_nil`
- `md5(raw_payload)`

All sampled values matched, including `raw_payload` MD5:

| raw_id | raw_payload_md5 |
|---:|---|
| 1 | `00dacf8737269748692ca237e6e20fdc` |
| 500000 | `39a8de52c67bcba76b05a0308a5ab846` |
| 1000000 | `10ca04c552aed7b0b448965ac27630ba` |

The same check is now available through the exporter CLI:

```bash
cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  --log-level error \
  validate-samples \
  --manifest data_lake/raw_postgres/snapshot_date=2026-06-19/source=local_mydb/_manifests/table_manifests/dart_xbrl_fact_raw.json
```

Result:

```text
requested_raw_ids = [1, 500000, 1000000]
columns_checked = 25
compared_rows = 3
missing_in_source = []
missing_in_parquet = []
mismatches = []
passed = true
```

## DuckDB Hive Partition Note

DuckDB auto-detects Hive partition columns from path segments. With default `read_parquet(...)`, `reprt_code=11011` can be exposed as a partition column with numeric type inference. The file-internal schema is correct:

```sql
DESCRIBE
SELECT raw_id, bsns_year, reprt_code
FROM read_parquet(
  'data_lake/raw_postgres/snapshot_date=2026-06-19/source=local_mydb/dart_xbrl_fact_raw/schema_version=1/**/*.parquet',
  hive_partitioning = false
)
LIMIT 0;
```

Result:

```text
raw_id      bigint
bsns_year   integer
reprt_code  varchar
```

Use `hive_partitioning=false` when validating file-internal column types for columns that are also path partitions.

## 10M Follow-Up Benchmark

### Scope

- Target DB: local `mydb` mirror.
- Table: `dart_xbrl_fact_raw`
- Range: `raw_id >= 1 AND raw_id < 10000001`
- Source rows in range: 9,999,997
- Note: this raw_id interval has 3 missing raw_id values, so the row count is not exactly 10,000,000.

### Command

```bash
/usr/bin/time -l cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  --log-level error \
  export \
  --tables dart_xbrl_fact_raw \
  --start-raw-id 1 \
  --chunk-rows 10000000 \
  --batch-rows 65536 \
  --max-rows-per-file 1000000 \
  --force
```

### Result

| Metric | Value |
|---|---:|
| Exported rows | 9,999,997 |
| Parquet files | 11 |
| Wall time | 289.47 sec |
| Throughput | ~34,550 rows/sec |
| User CPU | 221.78 sec |
| System CPU | 22.11 sec |
| Max RSS | 780,533,760 bytes |
| Total Parquet bytes | 437,661,633 bytes |
| Min file bytes | 23,094,817 bytes |
| Max file bytes | 46,325,735 bytes |

The 10M run produced files under these partitions:

```text
bsns_year=2025/reprt_code=11011/
bsns_year=2025/reprt_code=11012/
bsns_year=2025/reprt_code=11013/
bsns_year=2025/reprt_code=11014/
```

File sizes remained below the 256MB target range even with
`--max-rows-per-file 1000000`; most files were roughly 22MB to 44MB.

### Row Count Validation

Manifest validation:

```text
manifest_rows = 9,999,997
parquet_rows = 9,999,997
files_checked = 11
passed = true
```

DuckDB Parquet count:

```text
rows = 9,999,997
files = 11
min_raw_id = 1
max_raw_id = 10,000,000
```

PostgreSQL source count:

```text
rows = 9,999,997
min_raw_id = 1
max_raw_id = 10,000,000
```

### Sample Validation

```bash
cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  --log-level error \
  validate-samples \
  --manifest data_lake/raw_postgres/snapshot_date=2026-06-19/source=local_mydb/_manifests/table_manifests/dart_xbrl_fact_raw.json
```

Result:

```text
requested_raw_ids = [1, 5000000, 10000000]
columns_checked = 25
compared_rows = 3
missing_in_source = []
missing_in_parquet = []
mismatches = []
passed = true
```

### Observations

- Throughput dropped from ~44,100 rows/sec in the 1M run to ~34,550 rows/sec in the 10M run.
- Max RSS increased from 573MB to 781MB, which is not fully flat but still bounded for this single-table run.
- The current row-count rotation is conservative for this table; 1M rows per file still produces sub-50MB files.
- Before full-table export, test `--max-rows-per-file 5000000` or implement file-size-based rotation.

## 10M File-Size Policy Benchmark

### Scope

- Target DB: local `mydb` mirror.
- Table: `dart_xbrl_fact_raw`
- Range: `raw_id >= 1 AND raw_id < 10000001`
- Source rows in range: 9,999,997
- Change from prior 10M run: `--max-rows-per-file 5000000`

### Command

```bash
/usr/bin/time -l cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  --log-level error \
  export \
  --tables dart_xbrl_fact_raw \
  --start-raw-id 1 \
  --chunk-rows 10000000 \
  --batch-rows 65536 \
  --max-rows-per-file 5000000 \
  --force
```

### Result

| Metric | Value |
|---|---:|
| Exported rows | 9,999,997 |
| Parquet files | 5 |
| Wall time | 286.43 sec |
| Throughput | ~34,910 rows/sec |
| User CPU | 223.01 sec |
| System CPU | 23.18 sec |
| Max RSS | 919,666,688 bytes |
| Total Parquet bytes | 437,841,849 bytes |
| Min file bytes | 27,771,847 bytes |
| Max file bytes | 226,903,363 bytes |

File sizes:

```text
bsns_year=2025/reprt_code=11011/part-000000.parquet  216M
bsns_year=2025/reprt_code=11011/part-000001.parquet  106M
bsns_year=2025/reprt_code=11012/part-000000.parquet   32M
bsns_year=2025/reprt_code=11013/part-000000.parquet   26M
bsns_year=2025/reprt_code=11014/part-000000.parquet   37M
```

### Validation

Manifest validation:

```text
manifest_rows = 9,999,997
parquet_rows = 9,999,997
files_checked = 5
passed = true
```

DuckDB and source counts both returned:

```text
rows = 9,999,997
min_raw_id = 1
max_raw_id = 10,000,000
```

Sample validation:

```text
requested_raw_ids = [1, 5000000, 10000000]
columns_checked = 25
compared_rows = 3
missing_in_source = []
missing_in_parquet = []
mismatches = []
passed = true
```

### Comparison

| Setting | Files | Max file | Wall time | Throughput | Max RSS |
|---|---:|---:|---:|---:|---:|
| 1M rows/file | 11 | 46.3MB | 289.47 sec | ~34,550 rows/sec | 780.5MB |
| 5M rows/file | 5 | 226.9MB | 286.43 sec | ~34,910 rows/sec | 919.7MB |

The 5M rows/file setting is better for file count and gets the largest file close to the 256MB target range. Runtime is effectively unchanged. RSS increased by about 139MB, which is acceptable for this local run but should be watched on larger ranges.

## Follow-Up Queue

- Use `--max-rows-per-file 5000000` as the provisional setting for `dart_xbrl_fact_raw`.
- Test whether the same row cap is reasonable for other wide P0 tables.
- Consider file-size-based rotation if table-specific row caps become hard to manage.
- Run P0 date-month smoke benchmarks for `krx_security_flow_raw` and `daily_ohlcv`.
- Extend `validate-samples` beyond raw-id manifests if full-table/date-month sample checks become necessary.

## P0 Smoke Benchmark: `dart_financial_statement_raw`

### Scope

- Target DB: local `mydb` mirror.
- Table: `dart_financial_statement_raw`
- Total local rows: 16,887,271
- Range: `raw_id >= 1 AND raw_id < 1000001`
- Source rows in range: 1,000,000

The tested range spans these partitions:

| bsns_year | reprt_code | rows |
|---:|---|---:|
| 2025 | `11011` | 279,407 |
| 2025 | `11012` | 182,690 |
| 2025 | `11013` | 174,774 |
| 2025 | `11014` | 187,517 |
| 2026 | `11011` | 60 |
| 2026 | `11012` | 771 |
| 2026 | `11013` | 174,037 |
| 2026 | `11014` | 744 |

### Command

```bash
/usr/bin/time -l cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  --log-level error \
  export \
  --tables dart_financial_statement_raw \
  --start-raw-id 1 \
  --chunk-rows 1000000 \
  --batch-rows 65536 \
  --max-rows-per-file 1000000 \
  --force
```

### Result

| Metric | Value |
|---|---:|
| Exported rows | 1,000,000 |
| Parquet files | 8 |
| Wall time | 25.52 sec |
| Throughput | ~39,185 rows/sec |
| User CPU | 23.27 sec |
| System CPU | 1.75 sec |
| Max RSS | 646,725,632 bytes |
| Total Parquet bytes | 61,136,137 bytes |
| Min file bytes | 15,088 bytes |
| Max file bytes | 18,222,879 bytes |

File sizes:

```text
bsns_year=2025/reprt_code=11011/part-000000.parquet   17M
bsns_year=2025/reprt_code=11012/part-000000.parquet   11M
bsns_year=2025/reprt_code=11013/part-000000.parquet  9.8M
bsns_year=2025/reprt_code=11014/part-000000.parquet   11M
bsns_year=2026/reprt_code=11011/part-000000.parquet   15K
bsns_year=2026/reprt_code=11012/part-000000.parquet   62K
bsns_year=2026/reprt_code=11013/part-000000.parquet  9.7M
bsns_year=2026/reprt_code=11014/part-000000.parquet   61K
```

### Validation

Manifest validation:

```text
manifest_rows = 1,000,000
parquet_rows = 1,000,000
files_checked = 8
passed = true
```

DuckDB and source counts both returned:

```text
rows = 1,000,000
min_raw_id = 1
max_raw_id = 1,000,000
```

Sample validation:

```text
requested_raw_ids = [1, 500000, 1000000]
columns_checked = 27
compared_rows = 3
missing_in_source = []
missing_in_parquet = []
mismatches = []
passed = true
```

### Observations

- Decimal/JSON/timestamp paths passed sample validation on this table.
- Throughput was ~39K rows/sec, broadly similar to `dart_xbrl_fact_raw` 10M throughput.
- Partition skew creates very small files for low-row `2026` partitions.
- Row-count rotation alone does not address tiny partition files; small-partition compaction or a minimum-file-size policy may be needed before operational full exports.

## P0 Smoke Benchmark: `dart_shareholder_return_raw`

### Scope

- Target DB: local `mydb` mirror.
- Table: `dart_shareholder_return_raw`
- Total local rows: 8,647,588
- Range: `raw_id >= 1 AND raw_id < 1000001`
- Source rows in range: 1,000,000

The tested range spans these partitions:

| bsns_year | reprt_code | rows |
|---:|---|---:|
| 2024 | `11011` | 208,031 |
| 2024 | `11012` | 190,731 |
| 2024 | `11013` | 168,076 |
| 2024 | `11014` | 170,132 |
| 2025 | `11011` | 263,030 |

### Command

```bash
/usr/bin/time -l cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  --log-level error \
  export \
  --tables dart_shareholder_return_raw \
  --start-raw-id 1 \
  --chunk-rows 1000000 \
  --batch-rows 65536 \
  --max-rows-per-file 1000000 \
  --force
```

### Result

| Metric | Value |
|---|---:|
| Exported rows | 1,000,000 |
| Parquet files | 5 |
| Wall time | 16.90 sec |
| Throughput | ~59,172 rows/sec |
| User CPU | 16.41 sec |
| System CPU | 0.87 sec |
| Max RSS | 483,491,840 bytes |
| Total Parquet bytes | 11,293,854 bytes |
| Min file bytes | 1,896,258 bytes |
| Max file bytes | 2,987,590 bytes |

File sizes:

```text
bsns_year=2024/reprt_code=11011/part-000000.parquet  2.3M
bsns_year=2024/reprt_code=11012/part-000000.parquet  2.0M
bsns_year=2024/reprt_code=11013/part-000000.parquet  1.8M
bsns_year=2024/reprt_code=11014/part-000000.parquet  1.8M
bsns_year=2025/reprt_code=11011/part-000000.parquet  2.8M
```

### Validation

Manifest validation:

```text
manifest_rows = 1,000,000
parquet_rows = 1,000,000
files_checked = 5
passed = true
```

DuckDB and source counts both returned:

```text
rows = 1,000,000
min_raw_id = 1
max_raw_id = 1,000,000
```

Sample validation:

```text
requested_raw_ids = [1, 500000, 1000000]
columns_checked = 21
compared_rows = 3
missing_in_source = []
missing_in_parquet = []
mismatches = []
passed = true
```

### Observations

- JSON/date/timestamp paths passed sample validation on this table.
- Throughput was higher than the wider DART tables, at about 59K rows/sec.
- The output files are small even with 1M exported rows because the table is comparatively narrow and the first chunk is split across five report partitions.
- This table may need larger chunk ranges or file-size-based rotation/compaction before operational full exports if the desired target is 128-256MB files.
