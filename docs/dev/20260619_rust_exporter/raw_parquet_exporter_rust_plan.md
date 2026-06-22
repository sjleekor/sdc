# Rust Raw Parquet Exporter 구현 계획

- 작성일: 2026-06-19
- 대상 저장소: `stock_data_collector`
- 대상 DB: local PostgreSQL `mydb` mirror 우선, 운영 기준 source of truth는 sj2-server PostgreSQL `krx_data`
- 목표: DB에 보관된 원천/raw 성격 테이블을 가공하지 않은 상태로 Parquet lake에 안정적으로 export하여, 이후 분석/ETL이 PostgreSQL 대형 scan에 의존하지 않게 한다.

## 1. 언어 선택

### 1.1 선택: Rust

현재 프로젝트 주 언어인 Python은 고려하지 않고, raw export 성능과 운영 안정성을 기준으로 Rust를 선택한다.

이유:

1. **PostgreSQL 대용량 stream 처리에 적합**
   - `COPY (SELECT ...) TO STDOUT` 결과를 async stream으로 받아 writer에 흘릴 수 있다.
   - Python보다 object allocation/GC 비용을 낮게 유지하기 쉽다.

2. **Parquet writer 제어가 좋음**
   - Apache Arrow Rust / Parquet Rust를 직접 사용해 `RecordBatch`, row group, compression, dictionary encoding, memory cap을 명시적으로 제어할 수 있다.
   - JSON/NUMERIC/timestamp 같은 타입 정책을 중앙에서 고정할 수 있다.

3. **대형 테이블 재시작/검증/병렬화 제어가 쉬움**
   - bounded channel, worker pool, semaphore 기반으로 DB connection 수와 writer memory를 분리 제어한다.
   - panic/에러 전파와 임시 파일 정리 정책을 명확히 구현할 수 있다.

4. **C++ 대비 유지보수 비용이 낮음**
   - C++/Arrow C++가 절대 성능 후보이지만, 이 작업의 병목은 DB read, 디스크 write, ZSTD 압축이다.
   - Rust는 성능을 크게 잃지 않으면서 메모리 안전성과 배포 안정성이 높다.

### 1.2 대안 평가

| 후보 | 장점 | 제외/보류 이유 |
|---|---|---|
| C++ + Arrow C++ | 최고 수준의 native 성능 | 빌드/배포/메모리 안전 비용이 크다. 이 작업의 병목이 C++로만 해결되는 성격이 아니다. |
| Java/Scala + Spark | 분산 처리와 Parquet 생태계 강함 | 현재는 단일 PostgreSQL 원천 export가 병목이고, Spark cluster 운영 비용이 과하다. |
| Go | stream I/O와 배포가 단순 | Arrow/Parquet 타입 제어와 Decimal/JSON 처리에서 Rust 쪽이 더 적합하다. |
| DuckDB SQL | PostgreSQL attach 후 Parquet export가 매우 단순 | 재시작성, manifest, chunk별 검증, writer memory cap, 테이블별 특수 타입 정책 제어가 제한적이다. 단, benchmark baseline으로 유지한다. |
| Python + PyArrow | 기존 프로젝트와 통합 쉬움 | 80M+ row raw export에서 Python row path를 밟으면 병목이 커진다. Arrow-native path를 써도 운영 제어가 Rust보다 약하다. |

## 2. Export 범위

### 2.1 Strict raw 테이블

아래 테이블은 원천 API/문서/관측값을 보존하는 raw export 1차 대상이다.

| 테이블 | 행수 기준 | 성격 | 우선순위 |
|---|---:|---|---|
| `dart_xbrl_fact_raw` | 83,944,757 | OpenDART XBRL instance fact raw | P0 |
| `krx_security_flow_raw` | 76,536,572 | KRX 수급/공매도 raw | P0 |
| `dart_financial_statement_raw` | 16,887,271 | OpenDART 재무제표 raw | P0 |
| `dart_shareholder_return_raw` | 8,647,588 | 배당/자사주 raw | P0 |
| `dart_share_count_raw` | 347,217 | 주식수 raw | P1 |
| `dart_xbrl_document` | 83,611 | XBRL ZIP/document metadata raw | P1 |
| `common_feature_observation_raw` | 2,839 | 거시/시장 공통 feature observation raw | P2 |
| `operating_source_document` | 0 | 비정형 사업 KPI 원문 document | P2 |

### 2.2 Raw에 준하는 reference/source 테이블

아래 테이블은 `*_raw` 이름은 아니지만 raw 분석과 조인 재현성에 필요하므로 함께 export한다.

| 테이블 | 행수 기준 | 성격 | 우선순위 |
|---|---:|---|---|
| `daily_ohlcv` | 6,567,124 | 원천 가격/거래량 시계열 | P0 |
| `dart_corp_master` | 116,503 | DART corp/ticker mapping source master | P1 |
| `stock_master_snapshot` | 26 | universe snapshot metadata | P1 |
| `stock_master_snapshot_items` | 70,196 | universe snapshot items | P1 |
| `stock_master` | 2,783 | 현재 universe reference | P1 |

### 2.3 Export 제외

아래는 raw export 대상이 아니다. 필요하면 별도 `derived` 또는 `metadata` export로 다룬다.

| 테이블 | 제외 이유 |
|---|---|
| `stock_metric_fact` | raw를 정규화한 canonical metric fact |
| `common_feature_daily_fact` | observation raw를 가공한 일별 feature fact |
| `operating_metric_fact` | 원문 document에서 extractor가 추출한 KPI fact |
| `metric_catalog`, `metric_mapping_rule` | 정규화 규칙/카탈로그 |
| `common_feature_catalog`, `common_feature_catalog_input`, `common_feature_series` | feature 정의/설정 |
| `ingestion_runs`, `sync_checkpoints` | 운영/audit/checkpoint |

## 3. 핵심 원칙

### 3.1 Raw 보존

Exporter는 분석 가능한 포맷으로 옮기는 도구이지, 의미 변환 ETL이 아니다.

허용되는 변환:

- PostgreSQL type을 Parquet logical type으로 손실 없이 mapping.
- `jsonb`를 PostgreSQL이 저장한 text 표현(`col::text`) 그대로 string으로 저장. Rust에서 `serde_json::Value`로 round-trip하지 않는다(key 재정렬·숫자 표기 변경으로 raw가 바뀔 수 있다). PostgreSQL jsonb는 저장 시 이미 정규화되므로 `::text`가 사실상의 canonical raw 표현이다.
- `timestamptz`를 UTC epoch micros + timezone metadata로 저장.
- export metadata 컬럼 추가: `__extract_run_id`, `__source_db`, `__source_table`, `__extracted_at`.

금지되는 변환:

- metric pivot, dedupe, KRX/PYKRX source 우선순위 적용.
- currency 환산.
- numeric을 float로 변환.
- XBRL dimensions flattening.
- raw payload 내부 key 추출.
- null/empty 값 의미 변경.

### 3.2 PostgreSQL 부하 제어

PostgreSQL은 source of truth이며 export 때문에 수집 DB를 방해하면 안 된다.

- 기본 DB read connection: 2개.
- 최대 DB read connection: 4개.
- 대형 테이블 동시 export는 기본 1개만 허용한다.
- 작은 reference 테이블은 대형 테이블 작업 사이 또는 별도 small-table worker에서 처리한다.
- `statement_timeout`과 `idle_in_transaction_session_timeout`을 exporter connection에 명시한다.
- long-running snapshot이 vacuum을 방해하지 않도록 테이블별 chunk transaction으로 끊는다.

### 3.3 추출 키와 저장 파티션 키 분리

DB에서 빠르게 읽기 위한 key와 Parquet 분석에 좋은 path partition key는 다르다.

예:

- `dart_xbrl_fact_raw`
  - extract: `raw_id` range, primary key index 사용.
  - output: `bsns_year=/reprt_code=/part-*.parquet`.
- `krx_security_flow_raw`
  - extract: `trade_date` month range 또는 `raw_id` range.
  - output: `year=/month=/part-*.parquet`.

## 4. 출력 레이아웃

기본 root:

```text
data_lake/
  raw_postgres/
    snapshot_date=2026-06-19/
      source=local_mydb/
        <table>/
          schema_version=1/
            ...
      _manifests/
        raw_export_manifest.json
        table_manifests/
          <table>.json
```

운영에서는 repo 내부가 아니라 별도 디스크 경로를 기본값으로 둔다. repo 내부 `data_lake/`는 개발용이며 `.gitignore` 대상이어야 한다.

### 4.1 테이블별 path partition

| 테이블 | 출력 partition | 파일 내부 sort |
|---|---|---|
| `dart_xbrl_fact_raw` | `bsns_year=<YYYY>/reprt_code=<CODE>/` | `(corp_code, rcept_no, context_id, concept_id, raw_id)` |
| `dart_xbrl_document` | `bsns_year=<YYYY>/reprt_code=<CODE>/` | `(corp_code, rcept_no)` |
| `dart_financial_statement_raw` | `bsns_year=<YYYY>/reprt_code=<CODE>/` | `(ticker, corp_code, fs_div, sj_div, account_id, ord)` |
| `dart_shareholder_return_raw` | `bsns_year=<YYYY>/reprt_code=<CODE>/` | `(ticker, statement_type, row_name, metric_code)` |
| `dart_share_count_raw` | `bsns_year=<YYYY>/reprt_code=<CODE>/` | `(ticker, se, rcept_no)` |
| `krx_security_flow_raw` | `year=<YYYY>/month=<MM>/` | `(trade_date, ticker, market, metric_code, source)` |
| `daily_ohlcv` | `year=<YYYY>/month=<MM>/` | `(trade_date, ticker, market)` |
| `common_feature_observation_raw` | `source=<SOURCE>/` | `(series_id, observation_date, vintage)` |
| `operating_source_document` | `source_system=<SOURCE>/` | `(ticker, document_date, document_key)` |
| `dart_corp_master` | single file | `(ticker, corp_code, modify_date)` |
| `stock_master` | single file | `(market, ticker)` |
| `stock_master_snapshot` | single file | `(as_of_date, snapshot_id)` |
| `stock_master_snapshot_items` | `snapshot_date=<YYYY-MM-DD>/` | `(snapshot_id, market, ticker)` |

위 "파일 내부 sort"는 **분석 친화적 최종 정렬 목표**이며, `raw_id_range` 추출은 raw_id 순으로 stream되므로 partition writer 안에서 이 순서가 자동으로 보장되지 않는다. 두 가지 정책 중 하나를 명시적으로 택한다.

1. (기본) **sort 보장하지 않음.** Parquet column statistics(min/max per row group)로 filter pushdown은 충분하고, raw 보존 단계에서 전역 sort는 불필요한 메모리/비용을 유발한다. 이 경우 위 컬럼은 "권장 sort"일 뿐 강제하지 않으며, 파일은 추출 순서(raw_id 또는 trade_date) 그대로 기록한다.
2. (옵션) partition cardinality가 충분히 낮고 partition별 데이터가 메모리에 들어오는 작은 테이블(`dart_share_count_raw`, reference 테이블)에 한해, writer close 직전 in-memory sort를 적용한다.

대형 P0 테이블(`dart_xbrl_fact_raw`, `krx_security_flow_raw`, `dart_financial_statement_raw`, `dart_shareholder_return_raw`)은 1번을 기본으로 한다.

빈 문자열 partition 값 주의:

- `reprt_code`는 `NOT NULL DEFAULT ''`이고 `dart_shareholder_return_raw`는 `reprt_code`가 빈 값일 수 있다. partition path가 `reprt_code=`(빈 값)이 되면 일부 reader에서 모호하므로, 빈 값은 `reprt_code=__empty__` 같은 sentinel로 인코딩하고 그 매핑을 table manifest에 기록한다. `bsns_year`는 모든 대상 테이블에서 `INT NOT NULL`이라 빈 값 문제는 없다.

### 4.2 파일 크기 목표

- Target Parquet file size: 256MB ~ 1GB compressed.
- Row group size: 기본 128K rows, 대형 wide/string-heavy 테이블은 64K부터 benchmark.
- Compression: `zstd`.
- Dictionary encoding: text cardinality가 낮은 컬럼에만 활성화. `raw_payload`, `value_text`, `content_text`는 dictionary 비활성 후보.

## 5. Rust 프로젝트 구조

위치는 Python 패키지와 분리한다.

```text
tools/
  raw-parquet-exporter/
    Cargo.toml
    README.md
    config/
      export_tables.toml
      local.example.toml
    src/
      main.rs
      cli.rs
      config.rs
      db.rs
      pg_copy.rs
      schema.rs
      table_specs.rs
      planner.rs
      partition_router.rs
      parquet_writer.rs
      manifest.rs
      validate.rs
      metrics.rs
      error.rs
    tests/
      schema_mapping_tests.rs
      partition_router_tests.rs
      manifest_tests.rs
```

### 5.1 주요 crate 후보

| 역할 | crate 후보 |
|---|---|
| CLI | `clap` |
| async runtime | `tokio` |
| PostgreSQL | `tokio-postgres` |
| stream utils | `futures`, `tokio-util` |
| Arrow/Parquet | `arrow`, `parquet` |
| Decimal | `rust_decimal` 또는 Arrow Decimal128 직접 변환 |
| config | `serde`, `toml` |
| JSON | `serde_json` |
| logging | `tracing`, `tracing-subscriber` |
| error | `thiserror`, `anyhow` |
| checksum | `xxhash-rust` 또는 `sha2` |
| progress | `indicatif` optional |

버전은 구현 시점의 호환 조합으로 lock한다. exporter는 repo Python dependency와 분리된 Cargo lock을 가진다.

## 6. Config 설계

`config/export_tables.toml` 예시:

```toml
[defaults]
compression = "zstd"
row_group_rows = 131072
target_file_bytes = 536870912
db_read_connections = 2
writer_workers = 4

[[tables]]
name = "dart_xbrl_fact_raw"
priority = "P0"
extract_strategy = "raw_id_range"
extract_key = "raw_id"
chunk_rows = 1000000
output_partitions = ["bsns_year", "reprt_code"]
order_by = ["raw_id"]
jsonb_columns = ["dimensions", "raw_payload"]

[[tables]]
name = "krx_security_flow_raw"
priority = "P0"
extract_strategy = "date_month"
date_column = "trade_date"
output_partitions = ["year(trade_date)", "month(trade_date)"]
order_by = ["trade_date", "ticker", "market", "metric_code", "source"]
jsonb_columns = ["raw_payload"]

[[tables]]
name = "daily_ohlcv"
priority = "P0"
extract_strategy = "date_month"
date_column = "trade_date"
output_partitions = ["year(trade_date)", "month(trade_date)"]
order_by = ["trade_date", "ticker", "market"]
```

Local runtime config:

```toml
[source]
name = "local_mydb"
dsn_env = "DB_DSN"
schema = "public"
read_only = true

[output]
root = "/path/to/data_lake/raw_postgres"
snapshot_date = "2026-06-19"
tmp_root = "/path/to/data_lake/_tmp/raw_export"
```

Secrets/DSN은 config file에 직접 저장하지 않는다. `.env` 또는 환경 변수에서 읽는다.

## 7. Module별 책임

### 7.1 `db.rs`

- DSN resolve.
- read-only connection 생성.
- session setting 적용:
  - `SET statement_timeout = '6h'`
  - `SET idle_in_transaction_session_timeout = '30min'`
  - `SET DateStyle = ISO`
  - `SET IntervalStyle = iso_8601`
- table schema introspection.

### 7.2 `pg_copy.rs`

- `COPY (SELECT ...) TO STDOUT WITH (FORMAT BINARY)` 또는 CSV/text COPY stream 처리.
- 1차 구현은 안정성을 위해 `query_raw` row stream 방식과 `COPY CSV` 방식 중 benchmark로 선택한다.
- 최종 목표는 PostgreSQL binary COPY를 Arrow batch로 직접 decode하는 경로다.

실용적 단계:

1. MVP: `query_raw` + typed row decode.
2. 성능 개선: `COPY ... WITH (FORMAT CSV, NULL '\N')` + fast parser.
3. 고성능 경로: binary COPY decoder 구현.

MVP가 너무 느리면 2단계를 먼저 구현한다.

### 7.3 `schema.rs`

PostgreSQL type -> Arrow/Parquet mapping을 고정한다.

| PostgreSQL | Arrow/Parquet | 정책 |
|---|---|---|
| `text` | `LargeUtf8` | 원문 보존 |
| `date` | `Date32` | days since epoch |
| `timestamp with time zone` | `Timestamp(Microsecond, UTC)` | KST 표시는 metadata에만 보존 |
| `timestamp without time zone` | `Timestamp(Microsecond, None)` | timezone 추정 금지 |
| `bigint` | `Int64` | 그대로 |
| `integer` | `Int32` | 그대로 |
| `boolean` | `Boolean` | 그대로 |
| `numeric(30,4)` | `Decimal128(30,4)` | float 변환 금지. DART raw 금액/주식수 컬럼 |
| `numeric(30,8)` | `Decimal128(30,8)` | float 변환 금지. `common_feature_observation_raw.value_numeric` 전용 (scale=8) |
| `uuid` | `Utf8` 또는 fixed binary | MVP는 string. export 대상 raw 테이블에는 uuid 컬럼 없음(snapshot_id는 `stock_master_snapshot`/`_items`에만 존재) |
| `bigserial` (`raw_id` 등) | `Int64` | PK는 그대로 보존(resume 키이자 추출 키) |
| `jsonb` | `LargeUtf8` | PostgreSQL `col::text` 표현 그대로 보존. round-trip 직렬화 금지 |

### 7.4 `planner.rs`

테이블별 job list를 만든다.

Job 예:

```text
ExportJob {
  table: "dart_xbrl_fact_raw",
  extract_predicate: "raw_id >= 10000001 AND raw_id < 11000001",
  expected_min_key: 10000001,
  expected_max_key: 11000000,
  output_partition_policy: ["bsns_year", "reprt_code"]
}
```

전략:

- `raw_id_range`: 대형 raw 테이블 기본. primary key 또는 `raw_id` index를 사용한다.
- `date_month`: `daily_ohlcv`, `krx_security_flow_raw`처럼 날짜 index가 좋은 테이블.
- `full_table`: 작은 reference table.
- `snapshot_items`: `stock_master_snapshot_items`에는 날짜 컬럼이 없으므로 `stock_master_snapshot`과 `snapshot_id`로 join해 `as_of_date`를 가져와 `snapshot_date` partition을 만든다.
- `empty_table`: row가 0인 테이블(`operating_source_document` 등)은 데이터 파일 없이 schema-only manifest entry(`rows_exported=0`, `files=0`)만 기록한다. 빈 디렉터리/파일을 만들지 않는다.

### 7.5 `partition_router.rs`

RecordBatch를 output partition별로 나눠 writer에 전달한다.

주의:

- `raw_id_range`로 읽더라도 output은 `bsns_year/reprt_code`로 나뉠 수 있다.
- 대형 테이블에서 열린 writer 수를 제한해야 한다.
- writer LRU close/reopen 정책 또는 partition별 single writer worker를 둔다.

1차 구현은 테이블별 partition cardinality가 낮은 경우만 동시에 open한다.

- DART: `bsns_year * reprt_code` 대략 48개.
- KRX/daily: `year * month` 대략 240개.

### 7.6 `parquet_writer.rs`

- Arrow `RecordBatch` -> Parquet file write.
- row group flush.
- target file size 도달 시 file rotation.
- `_tmp` 경로에 write 후 close 성공 시 final path로 rename.
- writer별 row count, byte size, min/max key를 반환.

### 7.7 `manifest.rs`

최상위 manifest:

```json
{
  "run_id": "...",
  "created_at": "2026-06-19T...",
  "source": {
    "name": "local_mydb",
    "database": "mydb",
    "schema": "public",
    "snapshot_policy": "per_chunk_read_committed"
  },
  "tables": {
    "dart_xbrl_fact_raw": {
      "rows_exported": 83944757,
      "files": 184,
      "partitions": ["bsns_year", "reprt_code"],
      "schema_hash": "...",
      "validation": "passed"
    }
  }
}
```

Table manifest:

- source table.
- selected columns.
- PostgreSQL type list.
- Arrow schema.
- file list.
- row count per file.
- min/max partition values.
- checksum per file.
- failed/retried jobs.

### 7.8 `validate.rs`

검증 단계:

1. DB row count vs Parquet row count.
2. Partition별 row count.
3. Nullability sanity.
4. Primary/natural key duplicate check는 Parquet 쪽에서 sample 또는 full DuckDB query로 실행.
5. `raw_payload` parse spot check.
6. Decimal round-trip spot check.

대형 테이블 검증은 full duplicate check를 기본으로 하지 않는다. row count와 min/max/checksum 중심으로 시작하고, 필요 시 DuckDB full validation을 별도 command로 둔다.

## 8. CLI 설계

```bash
raw-parquet-exporter plan \
  --config config/export_tables.toml \
  --runtime local.toml

raw-parquet-exporter export \
  --tables dart_xbrl_fact_raw,krx_security_flow_raw \
  --config config/export_tables.toml \
  --runtime local.toml

raw-parquet-exporter validate \
  --manifest /path/to/raw_export_manifest.json

raw-parquet-exporter resume \
  --run-id <run_id> \
  --manifest /path/to/raw_export_manifest.json
```

옵션:

```text
--tables              comma-separated table names
--priority            P0/P1/P2
--dry-run             query/job plan only
--max-db-connections  override default DB connection limit
--writer-workers      override writer worker count
--chunk-rows          override raw_id chunk size
--since-date          date_month table incremental start
--until-date          date_month table incremental end
--snapshot-date       output snapshot partition
--force               overwrite existing tmp/final partitions
--resume-run-id       resume failed run
```

## 9. 테이블별 추출 상세

### 9.1 `dart_xbrl_fact_raw`

문제:

- 83.9M rows, 117GB total size.
- `bsns_year` 단독 집계가 매우 느리다.
- 전체 exact aggregation은 3분 이상 걸릴 수 있다.

계획:

- `raw_id` range chunk로 읽는다.
- chunk size는 1M rows부터 시작해 benchmark 후 2M/5M으로 조정한다.
- stream row를 `bsns_year/reprt_code` partition writer로 route한다.
- `dimensions`, `raw_payload`는 JSON text로 저장한다.
- output:

```text
dart_xbrl_fact_raw/schema_version=1/bsns_year=2025/reprt_code=11011/part-000123.parquet
```

검증:

- source row count: export 시작 직전 `SELECT count(*)`로 1회 baseline 측정(대형 테이블이라 느릴 수 있으나 검증 기준값으로 1회만 사용). `ingestion_runs.remote_db_sync.counts`는 마지막 sync의 **증분 동기화 row 수**일 뿐 전체 행수가 아니므로 검증 기준으로 쓰지 않는다.
- raw_id min/max per exported file.
- sampled source_key reconstruction 가능성 확인:
  - `(corp_code, bsns_year, reprt_code, rcept_no, context_id, concept_id)`.

### 9.2 `krx_security_flow_raw`

계획:

- `trade_date` monthly range가 기본.
- unique index가 `trade_date`로 시작하므로 month predicate가 유리하다.
- output:

```text
krx_security_flow_raw/schema_version=1/year=2026/month=06/part-000000.parquet
```

주의:

- KRX/PYKRX dedupe 금지.
- source별 metric row를 그대로 보존한다.

### 9.3 `dart_financial_statement_raw`

계획:

- extract는 `raw_id` range가 기본.
- output은 `bsns_year/reprt_code`.
- `raw_payload` 보존.
- `numeric(30,4)` 금액 컬럼은 Decimal128.

### 9.4 `dart_shareholder_return_raw`

계획:

- extract는 `raw_id` range.
- output은 `bsns_year/reprt_code`.
- `statement_type`은 path partition에 넣지 않는다. cardinality는 낮지만 query에서 filter pushdown은 Parquet column stats로 충분하다.

### 9.5 `dart_share_count_raw`

계획:

- table size가 작으므로 `bsns_year/reprt_code` partition query도 가능하다.
- output은 `bsns_year/reprt_code`.

### 9.6 `dart_xbrl_document`

계획:

- `dart_xbrl_fact_raw`와 같은 `bsns_year/reprt_code` partition.
- XBRL fact 분석의 document dimension으로 항상 함께 export한다.

### 9.7 `daily_ohlcv`

계획:

- `trade_date` monthly range.
- output은 `year/month`.
- 가격/거래량은 원천 그대로. 수정주가 계산 금지.

### 9.8 Reference tables

작은 테이블은 단일 Parquet로 export한다.

- `dart_corp_master`
- `stock_master`
- `stock_master_snapshot`

`stock_master_snapshot_items`는 `stock_master_snapshot`과 join하여 `snapshot_date` partition을 만들 수 있다. 단, raw 보존을 엄격히 해석하면 join 없이 단일 파일로 저장하는 옵션도 제공한다.

기본은 두 벌을 만든다.

```text
stock_master_snapshot_items/schema_version=1/part-000000.parquet
stock_master_snapshot_items_by_date/schema_version=1/snapshot_date=2026-06-18/part-000000.parquet
```

## 10. Snapshot 정책

초기 구현은 per-chunk `READ COMMITTED`를 사용한다.

이유:

- local mirror는 full refresh 후 정적에 가까운 개발 DB다.
- single repeatable-read transaction을 수 시간 유지하면 vacuum과 DB 운영에 부담이 될 수 있다.

운영 sj2 직접 export가 필요하면 아래 중 하나를 선택한다.

1. export 직전 remote sync로 local mirror를 고정하고 local에서 export.
2. PostgreSQL `REPEATABLE READ READ ONLY DEFERRABLE` snapshot을 table 단위로 짧게 사용.
3. physical backup/snapshot 후 offline export.

권장 기본은 1번이다. 즉, source of truth는 sj2이지만 대형 raw Parquet export는 local mirror에서 수행한다.

## 11. 성능 Benchmark 계획

### 11.1 Benchmark 대상

1. `dart_xbrl_fact_raw`
   - 1M rows.
   - 10M rows.
   - full table.
2. `krx_security_flow_raw`
   - 1개월.
   - 1년.
   - full table.
3. `dart_financial_statement_raw`
   - 1M rows.
   - full table.

### 11.2 비교군

1. Rust exporter MVP.
2. Rust exporter COPY CSV path.
3. DuckDB PostgreSQL attach + `COPY ... TO parquet`.
4. psql `COPY TO CSV` + external conversion, 참고용.

### 11.3 측정 항목

- rows/sec.
- uncompressed MB/sec.
- compressed output MB/sec.
- output file size.
- DB CPU/IO load.
- exporter CPU.
- peak RSS.
- writer backpressure 발생 횟수.
- failed/resumed job 처리 시간.

## 12. 구현 단계

### Phase 0: 설계 고정

- 본 문서 리뷰.
- raw export 대상 table 확정.
- output root와 disk 여유 공간 확정.
- `.gitignore`에 local lake 경로 확인.

완료 조건:

- `export_tables.toml` 초안 확정.
- 테이블별 schema mapping 정책 확정.

### Phase 1: Rust scaffold

- `tools/raw-parquet-exporter` Cargo project 추가.
- CLI skeleton 구현.
- config load.
- DB connection 및 schema introspection.
- dry-run plan 출력.

완료 조건:

- `raw-parquet-exporter plan --tables dart_xbrl_fact_raw`가 job list를 출력한다.

### Phase 2: Single-table MVP

- `dart_xbrl_fact_raw` 1개 테이블 지원.
- `raw_id` range planner.
- row stream decode.
- Parquet write.
- manifest write.
- row count validation.

완료 조건:

- `dart_xbrl_fact_raw` 1M row export 성공.
- DuckDB로 Parquet row count 확인.
- source sample row와 Parquet sample row 값 일치.

### Phase 3: P0 테이블 확장

지원 테이블:

- `dart_xbrl_fact_raw`
- `krx_security_flow_raw`
- `dart_financial_statement_raw`
- `dart_shareholder_return_raw`
- `daily_ohlcv`

완료 조건:

- P0 full export 성공.
- manifest table별 row count가 source count와 일치.
- 재실행 시 기존 성공 job skip 또는 resume 가능.

### Phase 4: P1/P2 테이블 확장

지원 테이블:

- `dart_share_count_raw`
- `dart_xbrl_document`
- `common_feature_observation_raw`
- `operating_source_document`
- `dart_corp_master`
- `stock_master`
- `stock_master_snapshot`
- `stock_master_snapshot_items`

완료 조건:

- 모든 raw/reference table export 성공.
- table별 schema manifest 생성.

### Phase 5: 성능 개선

- COPY CSV path 또는 binary COPY path 도입.
- partition writer LRU 개선.
- file size based rotation.
- compression/thread tuning.
- benchmark 결과 문서화.

완료 조건:

- `dart_xbrl_fact_raw` full export 시간이 운영상 허용 범위에 들어온다.
- exporter peak RSS가 설정된 cap 안에 머문다.

## 13. 검증 SQL/쿼리 예시

DuckDB validation 예:

```sql
SELECT count(*)
FROM read_parquet('/lake/raw_postgres/.../dart_xbrl_fact_raw/**/*.parquet');

SELECT bsns_year, reprt_code, count(*)
FROM read_parquet('/lake/raw_postgres/.../dart_xbrl_fact_raw/**/*.parquet')
GROUP BY 1, 2
ORDER BY 1, 2;
```

PostgreSQL source 비교:

```sql
SELECT count(*) FROM dart_xbrl_fact_raw;
SELECT bsns_year, reprt_code, count(*)
FROM dart_xbrl_fact_raw
GROUP BY 1, 2
ORDER BY 1, 2;
```

대형 테이블에서 두 번째 source query는 느릴 수 있으므로 full validation command는 선택적으로 실행한다. 기본 manifest 검증은 export 중 집계한 partition count를 사용한다.

## 14. 리스크와 대응

| 리스크 | 영향 | 대응 |
|---|---|---|
| `bsns_year` 단독 scan이 느림 | DART table partition count 검증이 오래 걸림 | extract 중 partition count를 누적하고, DB full aggregation은 optional로 둔다. |
| JSONB 직렬화 비용 | CPU와 output size 증가 | raw 보존을 위해 text 보존. 추후 분석용 flat view는 별도 silver 단계에서 생성한다. |
| Decimal 처리 오류 | 금액/주식수 값 손상 | Decimal128 mapping test와 sample round-trip test를 필수화한다. |
| 많은 partition writer open | file descriptor/memory 증가 | writer open limit와 LRU close 정책. |
| local mirror와 sj2 차이 | snapshot 재현성 문제 | manifest에 source sync run, row count, started/ended timestamp 기록. |
| 장시간 export 중 실패 | 재실행 비용 증가 | job manifest와 part temp file 기반 resume. |
| DB 부하 과다 | 수집/운영 영향 | local mirror export 기본, sj2 직접 export 금지에 가깝게 제한. |

## 15. 결정 사항

1. Exporter 언어는 Rust로 한다.
2. Python 프로젝트와 분리된 `tools/raw-parquet-exporter`로 둔다.
3. `dart_xbrl_fact_raw`를 첫 MVP/benchmark 대상으로 한다.
4. raw table은 의미 변환 없이 Parquet으로 옮긴다.
5. `jsonb`는 MVP에서 JSON text로 보존한다.
6. `numeric(30,4)`는 Decimal128로 보존한다.
7. 대형 테이블 추출은 DB index 친화적인 key로 수행하고, output partition은 분석 친화적으로 별도 route한다.
8. 운영 source는 sj2지만 대형 export 실행은 local mirror 기준을 기본으로 한다.
