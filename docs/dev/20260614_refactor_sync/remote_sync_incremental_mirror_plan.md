# Remote Sync Incremental Mirror Refactor Plan

- 작성일: 2026-06-14
- 대상: `krx-collector db sync-remote`
- 목적: sj2-server PostgreSQL `krx_data`를 로컬 DB에 학습 재현 가능한 mirror 형태로 동기화한다.

## 0. 구현 상태

2026-06-14 기준 Phase 1 구현을 반영했다.

- 기본 `db sync-remote` 대상은 19개 source-data mirror table로 확장했다.
- `ingestion_runs`와 `sync_checkpoints`는 remote mirror 대상에서 제외하고 local audit/checkpoint로 유지한다.
- raw/update 가능 테이블은 `(fetched_at, surrogate_key)` 또는 `(updated_at, surrogate_key)` cursor를 사용한다.
- `--tables` 옵션을 추가했고, `--all-tables`와 상호배타로 처리하며 FK parent dependency closure를 자동 포함한다.
- sync 옵션 검증은 local table reset보다 먼저 수행한다.
- 부분 `--full-refresh`나 prune에서 선택한 parent를 참조하는 child table이 빠져 있으면 실행을 거부한다.
- `common_feature_observation_raw`는 `ON CONFLICT ON CONSTRAINT`를 지원한다.
- raw/fact table의 natural-key conflict 시 remote surrogate id도 보존한다.
- 작은 mirror table은 full scan 후 FK child-first 순서로 remote에 없는 local row를 prune하며, prune key는 temp table을 사용한다.
- cursor index DDL과 remote sync 단위 테스트를 추가했다.

## 1. 배경

현재 `db sync-remote`는 sj2-server의 데이터를 로컬 PostgreSQL로 복사하기 위한 개발/분석용 명령이다. 하지만 동기화 기능 구현 이후 common feature 계층이 추가되었고, DART 백필과 metric 정규화 범위도 확장되었다. 기존 sync 대상은 이 변화를 모두 반영하지 못한다.

현재 구현은 두 경로로 나뉜다.

1. 기본 증분 sync
   - `SYNC_TABLE_SPECS`에 정의된 일부 테이블만 처리한다.
   - 현재 대상: `stock_master`, `stock_master_snapshot`, `stock_master_snapshot_items`, `daily_ohlcv`, `krx_security_flow_raw`
2. `--full-refresh --all-tables`
   - `PIPELINE_FULL_REFRESH_TABLE_NAMES`에 정의된 관리 테이블을 truncate/copy 방식으로 복제한다.
   - 현재 대상은 DART/metric까지 포함하지만 common feature 계층은 빠져 있다.

학습 데이터 ETL은 `daily_ohlcv`, `krx_security_flow_raw`, `stock_metric_fact`, `common_feature_daily_fact` 등을 조합해야 하므로, 로컬 mirror는 수집/정규화/공통 feature 계층을 함께 재현할 수 있어야 한다.

## 2. 목표

### 2.1 학습용 전체 재현 가능한 mirror

`db sync-remote`는 아래 19개 source-data 테이블을 관리 대상 mirror sync set으로 삼는다.

```text
stock_master
stock_master_snapshot
stock_master_snapshot_items
daily_ohlcv
krx_security_flow_raw
dart_corp_master
dart_financial_statement_raw
dart_share_count_raw
dart_shareholder_return_raw
dart_xbrl_document
dart_xbrl_fact_raw
metric_catalog
metric_mapping_rule
stock_metric_fact
common_feature_series
common_feature_catalog
common_feature_catalog_input
common_feature_observation_raw
common_feature_daily_fact
```

`sync_checkpoints`는 제외한다. 이 테이블은 remote source 데이터가 아니라 로컬 sync 진행 상태를 저장하는 관리 테이블이다.

`ingestion_runs`도 기본 mirror 대상에서 제외한다. 이 테이블은 sj2 collector 실행 감사 로그이면서 동시에 `db sync-remote` 자체가 로컬 DB에 실행 row를 기록하는 테이블이다. remote row와 local sync row를 같은 테이블에 섞으면 mirror semantics가 깨지고, full refresh 중 현재 실행 row를 truncate하는 문제가 생긴다. remote 감사 로그가 필요하면 별도 옵션으로 `remote_ingestion_runs` 같은 분리 테이블에 복제하는 방식을 후속 과제로 둔다.

### 2.2 없는 데이터만 증분 sync

기본 실행은 로컬 DB 상태를 먼저 확인한 뒤, 이미 로컬에 있는 데이터는 다시 복사하지 않는다.

요구되는 동작:

1. 로컬 테이블 존재 여부와 컬럼 스키마를 확인한다.
2. 로컬 테이블의 현재 row count와 cursor를 계산한다.
3. sj2-server에서 해당 cursor 이후 데이터만 조회한다.
4. 로컬에 batch upsert한다.
5. correction 가능성이 있는 raw/derived 테이블은 단순 append가 아니라 update-aware cursor와 upsert를 사용한다.

## 3. 비목표

- sj2-server 수집 스케줄을 변경하지 않는다.
- 로컬 DB를 학습용 Parquet lake로 변환하지 않는다.
- `sync_checkpoints`를 sj2에서 로컬로 복제하지 않는다.
- `ingestion_runs`를 기본 mirror 대상에 섞지 않는다.
- 운영 DB의 대형 테이블을 매 실행마다 full refresh하지 않는다.

## 4. 테이블 분류와 증분 전략

### 4.1 Small replace 테이블

작고 기준/카탈로그 성격이 강한 테이블이다. mirror semantics를 지키려면 단순 upsert가 아니라 remote 전체 상태로 로컬을 교체해야 한다. upsert만 하면 remote에서 삭제되거나 비활성화 과정에서 사라진 row가 로컬에 남을 수 있다.

```text
stock_master
stock_master_snapshot
stock_master_snapshot_items
metric_catalog
metric_mapping_rule
common_feature_series
common_feature_catalog
common_feature_catalog_input
```

주의:

- `stock_master_snapshot_items`는 상대적으로 작지만 FK상 `stock_master_snapshot` 뒤에 처리해야 한다.
- `metric_mapping_rule`은 `metric_catalog` 뒤에 처리해야 한다.
- `common_feature_catalog_input`은 `common_feature_series`, `common_feature_catalog` 뒤에 처리해야 한다.

권장 구현:

1. remote rows를 임시 staging table 또는 메모리 batch로 읽는다.
2. FK 순서상 child 테이블을 먼저 truncate/delete한다.
3. parent -> child 순서로 insert/upsert한다.
4. full replace가 부담되지 않는 크기임을 유지하기 위해 row count guard를 둔다.

대안으로 delete-prune 방식을 사용할 수 있다. 이 경우 remote key set에 없는 local row를 삭제해야 하며, FK child table은 parent pruning 전에 먼저 정리해야 한다.

### 4.2 Update-aware cursor 기반 대형 raw 테이블

대형 raw/fact 테이블은 로컬 cursor 이후만 읽되, `raw_id` 또는 `document_id` 단독 cursor를 사용하지 않는다. 현재 저장 로직은 natural key 충돌 시 기존 row를 update할 수 있으므로, 원격에서 과거 surrogate key row가 보정되면 `WHERE raw_id > local_max_raw_id` 방식은 correction을 놓친다.

기본 원칙:

- source row의 correction을 반영할 수 있도록 `fetched_at` 또는 source update timestamp를 cursor의 첫 번째 컬럼으로 사용한다.
- surrogate key는 tie-breaker로만 사용한다.
- source가 기존 row를 update하면서 `fetched_at`을 갱신한다는 전제가 필요하다. 이 전제가 깨지는 테이블은 정기 overlap reconciliation 또는 partition refresh 대상이다.
- 모든 update-aware cursor에는 remote/local 양쪽에 cursor index를 둔다.

| 테이블 | 권장 cursor | conflict key |
|---|---|---|
| `daily_ohlcv` | `(fetched_at, trade_date, ticker, market)` | `(trade_date, ticker, market)` |
| `krx_security_flow_raw` | `(fetched_at, raw_id)` | `(trade_date, ticker, market, metric_code, source)` |
| `dart_financial_statement_raw` | `(fetched_at, raw_id)` | DDL의 `uq_dart_financial_statement_raw` |
| `dart_share_count_raw` | `(fetched_at, raw_id)` | DDL의 `uq_dart_share_count_raw` |
| `dart_shareholder_return_raw` | `(fetched_at, raw_id)` | DDL의 `uq_dart_shareholder_return_raw` |
| `dart_xbrl_document` | `(fetched_at, document_id)` | `(corp_code, bsns_year, reprt_code, rcept_no)` |
| `dart_xbrl_fact_raw` | `(fetched_at, raw_id)` | DDL의 `uq_dart_xbrl_fact_raw` |
| `common_feature_observation_raw` | `(fetched_at, raw_id)` | DDL의 `uq_common_feature_observation_raw` |

추가 correction guard:

- 최근 N일 또는 최근 N개 사업연도에 해당하는 partition/key range는 주기적으로 overlap refresh한다.
- DART raw처럼 과거 보정 가능성이 낮지만 row 수가 큰 테이블은 기본 증분 + 수동 `--tables ... --refresh-window ...` 옵션을 둔다.
- common feature raw는 row 수가 작으므로 필요하면 최근 전체 lookback window를 매번 재조회해도 된다.

### 4.3 Update-aware 파생 테이블

정규화나 daily build를 다시 실행하면 기존 key의 값이 보정될 수 있다. 이 테이블들은 "없는 행만 append"보다 update-aware upsert가 안전하다.

| 테이블 | 권장 cursor | conflict key | 비고 |
|---|---|---|---|
| `stock_metric_fact` | `(updated_at, fact_id)` | `(ticker, metric_code, bsns_year, reprt_code)` | metric rule 보정 시 기존 값 변경 가능 |
| `common_feature_daily_fact` | `(generated_at, feature_date, feature_code)` | `(feature_date, feature_code)` | build 재실행 시 기존 feature 값 변경 가능 |

`stock_metric_fact`는 정확성을 우선하면 `(updated_at, fact_id)` cursor가 기본이다. 단, 기존 row의 `updated_at`이 로컬 cursor보다 뒤인 경우만 읽어야 하므로 `WHERE (updated_at, fact_id) > (...)` 형태의 composite cursor가 필요하다.

`common_feature_daily_fact`도 `generated_at` 기준으로 보정분을 반영해야 한다. 단순 `feature_date` max 기준은 과거 파티션 재빌드 결과를 놓칠 수 있다.

필요 index:

```sql
CREATE INDEX IF NOT EXISTS ix_stock_metric_fact_sync_cursor
    ON stock_metric_fact (updated_at, fact_id);

CREATE INDEX IF NOT EXISTS ix_common_feature_daily_fact_sync_cursor
    ON common_feature_daily_fact (generated_at, feature_date, feature_code);
```

대형 raw 테이블도 `(fetched_at, surrogate_key)` 형태의 sync cursor index를 추가한다. 이미 `daily_ohlcv`에는 `(fetched_at, trade_date, ticker, market)` index가 있으므로 같은 패턴을 확장한다.

예상 raw cursor index:

```sql
CREATE INDEX IF NOT EXISTS ix_krx_security_flow_raw_sync_cursor
    ON krx_security_flow_raw (fetched_at, raw_id);
CREATE INDEX IF NOT EXISTS ix_dart_financial_statement_raw_sync_cursor
    ON dart_financial_statement_raw (fetched_at, raw_id);
CREATE INDEX IF NOT EXISTS ix_dart_share_count_raw_sync_cursor
    ON dart_share_count_raw (fetched_at, raw_id);
CREATE INDEX IF NOT EXISTS ix_dart_shareholder_return_raw_sync_cursor
    ON dart_shareholder_return_raw (fetched_at, raw_id);
CREATE INDEX IF NOT EXISTS ix_dart_xbrl_document_sync_cursor
    ON dart_xbrl_document (fetched_at, document_id);
CREATE INDEX IF NOT EXISTS ix_dart_xbrl_fact_raw_sync_cursor
    ON dart_xbrl_fact_raw (fetched_at, raw_id);
CREATE INDEX IF NOT EXISTS ix_common_feature_observation_raw_sync_cursor
    ON common_feature_observation_raw (fetched_at, raw_id);
```

## 5. CLI 동작 제안

### 5.1 기본 동작

```bash
uv run krx-collector db sync-remote --ssh-host whi@sj2-server
```

기본 동작은 19개 source-data mirror 테이블 증분 sync로 변경한다.

### 5.2 Full refresh

```bash
uv run krx-collector db sync-remote --ssh-host whi@sj2-server --full-refresh
```

19개 source-data mirror 테이블을 truncate 후 재적재한다. 로컬 DB에 있는 같은 테이블만 대상으로 하며, `sync_checkpoints`와 local `ingestion_runs`는 유지한다.

### 5.3 선택 테이블 sync

추가 옵션을 검토한다.

```bash
uv run krx-collector db sync-remote \
  --ssh-host whi@sj2-server \
  --tables daily_ohlcv,krx_security_flow_raw,stock_metric_fact
```

대형 테이블을 부분적으로 확인하거나 재동기화할 때 유용하다.

부분 sync는 FK dependency closure를 자동 포함해야 한다. 예를 들어 `--tables common_feature_daily_fact`는 최소한 `common_feature_catalog`를 함께 포함해야 하며, 재빌드 검증까지 의도하면 `common_feature_series`, `common_feature_catalog_input`, `common_feature_observation_raw`도 함께 포함해야 한다.

정책:

- 기본값은 `--include-dependencies=true`이다.
- 사용자가 child table만 지정하면 필요한 parent tables를 자동으로 sync plan 앞쪽에 추가한다.
- parent를 자동 포함할 수 없거나 로컬 parent가 missing/stale로 판단되면 실행을 거부한다.
- `--no-include-dependencies` 같은 unsafe 옵션은 초기 구현에서 제공하지 않는다.
- full refresh에서 선택 테이블을 truncate할 때는 참조하는 child table까지 포함하지 않으면 거부한다.

### 5.4 `--all-tables` 호환성

`--all-tables`는 기존 사용자 호환을 위해 deprecated alias로 둔다.

권장 해석:

- 기존: `--full-refresh --all-tables`만 허용
- 변경 후: `--all-tables`는 "managed source-data mirror 19 tables" 의미
- 문서에는 `--all-tables` 대신 기본 sync 또는 `--full-refresh` 사용을 권장한다.

## 6. 구현 설계

### 6.1 Sync 대상 상수 재정의

기존 `PIPELINE_FULL_REFRESH_TABLE_NAMES`를 19개 source-data mirror 대상 상수로 바꾼다.

예상 이름:

```python
MANAGED_MIRROR_TABLE_NAMES: tuple[str, ...] = (...)
MANAGED_MIRROR_TABLES: tuple[DatabaseTable, ...] = (...)
```

기존 이름은 호환 wrapper로 남겨도 된다.

### 6.2 `TableSyncSpec` 확장

현재 spec는 일부 테이블에만 수동 정의되어 있다. 19개 mirror 테이블을 같은 추상화로 다루려면 필드를 확장한다.

```python
@dataclass(frozen=True, slots=True)
class TableSyncSpec:
    name: str
    mode: Literal["small_replace", "incremental_upsert", "update_aware_upsert"]
    cursor_columns: tuple[str, ...]
    conflict_columns: tuple[str, ...] = ()
    conflict_constraint: str | None = None
    json_columns: tuple[str, ...] = ()
    select_columns: tuple[str, ...] | None = None
    do_nothing_when_no_update_columns: bool = False
    requires_parent_tables: tuple[str, ...] = ()
```

`select_columns=None`이면 remote/local physical insertable columns를 information_schema에서 읽어 사용한다.

`conflict_constraint`는 `UNIQUE NULLS NOT DISTINCT` 같은 constraint 기반 conflict 처리를 위해 필요하다. 예를 들어 `common_feature_observation_raw`는 column list보다 `ON CONFLICT ON CONSTRAINT uq_common_feature_observation_raw`를 직접 사용하는 편이 안전하다.

`do_nothing_when_no_update_columns`는 `common_feature_catalog_input`처럼 PK 외 mutable column이 없는 link table에서 `DO UPDATE SET`가 비는 문제를 피하기 위한 옵션이다.

### 6.3 Generic incremental query

Composite cursor를 지원하는 SQL builder를 만든다.

```sql
SELECT <columns>
FROM <table>
WHERE (<cursor_columns...>) > (<local_cursor_values...>)
ORDER BY <cursor_columns...>
LIMIT <batch_size>
```

cursor가 없거나 local row count가 0이면 `WHERE` 없이 처음부터 읽는다.

PostgreSQL row comparison을 쓰되, nullable cursor column은 피한다. 필요한 cursor 컬럼은 모두 NOT NULL이거나, `COALESCE` 정책을 명시한다.

raw table의 cursor는 `raw_id` 단독이 아니라 `(fetched_at, raw_id)`처럼 update-aware timestamp를 앞에 둔다. 단, source correction 시 해당 timestamp가 갱신되지 않는 테이블은 cursor 방식만으로 mirror 정합성을 보장할 수 없으므로 refresh window를 별도로 적용한다.

### 6.4 Upsert SQL

대부분 테이블은 `INSERT ... ON CONFLICT (...) DO UPDATE SET ...`를 사용한다.

원칙:

- conflict key는 자연키/DDL constraint와 일치시킨다.
- identity/surrogate key는 remote 값을 보존한다.
- update 대상은 conflict key를 제외한 모든 mutable column이다.
- JSONB 컬럼은 `psycopg2.extras.Json` 또는 COPY-compatible 직렬화를 사용한다.

### 6.5 Sequence sync

`raw_id`, `fact_id`, `document_id` 등 sequence-backed key를 remote 값 그대로 복사하면, local sequence가 뒤처질 수 있다.

각 테이블 sync 후 `_sync_owned_sequences()`를 호출한다. 대상은 19개 managed mirror table 전체로 확장한다.

### 6.6 FK 처리 순서

처리 순서는 FK 부모를 먼저 둔다.

```text
stock_master
stock_master_snapshot
stock_master_snapshot_items
daily_ohlcv
dart_corp_master
dart_financial_statement_raw
dart_share_count_raw
dart_shareholder_return_raw
dart_xbrl_document
dart_xbrl_fact_raw
metric_catalog
metric_mapping_rule
stock_metric_fact
krx_security_flow_raw
common_feature_series
common_feature_catalog
common_feature_catalog_input
common_feature_observation_raw
common_feature_daily_fact
```

`_sort_tables_by_fk_dependencies()`를 재사용할 수 있으면 수동 순서와 dependency sort를 함께 검증한다.

부분 sync도 같은 dependency graph를 사용한다. 지정된 테이블 set에 대해 transitive parent closure를 계산하고, closure를 포함한 뒤 dependency sort를 수행한다.

## 7. Local state inspection

각 테이블 sync 시작 전에 아래를 로그에 남긴다.

- local row count
- remote estimated row count 또는 remote cursor max
- local cursor
- remote cursor max
- sync mode
- copied/upserted row count

예:

```text
table=dart_xbrl_fact_raw mode=incremental_upsert
local_rows=80400000 local_cursor=(2026-06-14T01:20:00Z, 80400000)
remote_cursor=(2026-06-14T02:10:00Z, 80436160) copied=36160
```

대형 테이블에서 `count(*)`는 비쌀 수 있으므로, 필요하면 local count만 정확히 보고 remote는 `pg_stat_user_tables.n_live_tup`를 사용한다.

## 8. 테스트 계획

### 8.1 Unit tests

1. managed mirror sync set이 정확히 19개인지 검증한다.
2. `sync_checkpoints`가 managed mirror sync set에 없는지 검증한다.
3. `ingestion_runs`가 기본 mirror sync set에 없는지 검증한다.
4. common feature 5개 테이블이 sync set에 포함되는지 검증한다.
5. FK 순서에서 parent가 child보다 먼저 처리되는지 검증한다.
6. `--tables` 입력에 대해 transitive parent dependency closure가 자동 포함되는지 검증한다.
7. composite cursor SQL이 올바르게 생성되는지 검증한다.
8. raw table cursor가 `raw_id` 단독이 아니라 `(fetched_at, raw_id)` 계열로 생성되는지 검증한다.
9. local cursor가 없는 경우 full initial copy query로 떨어지는지 검증한다.
10. update-aware 테이블의 `ON CONFLICT DO UPDATE` SQL이 생성되는지 검증한다.
11. `conflict_constraint` 지정 시 `ON CONFLICT ON CONSTRAINT ...` SQL이 생성되는지 검증한다.
12. update column이 없는 link table은 `DO NOTHING`으로 처리되는지 검증한다.

### 8.2 Integration tests

작은 synthetic remote/local PostgreSQL 또는 fake connection layer를 사용한다.

1. local empty -> remote rows 전체 복사
2. local partial -> missing rows만 복사
3. local existing row correction -> `stock_metric_fact` 값 update 반영
4. local existing raw correction -> `(fetched_at, raw_id)` cursor로 common raw 값 update 반영
5. small replace 대상에서 remote에 없는 local row가 prune되는지 검증
6. common feature catalog/input FK 순서 검증
7. `--tables common_feature_daily_fact`가 필요한 parent table을 포함하는지 검증
8. sequence sync 후 local insert가 collision 없이 동작하는지 검증

### 8.3 Manual verification

sj2/local에서 다음을 비교한다.

```sql
SELECT count(*), max(trade_date) FROM daily_ohlcv;
SELECT count(*), max(fetched_at), max(raw_id) FROM krx_security_flow_raw;
SELECT count(*), max(fetched_at), max(raw_id) FROM dart_xbrl_fact_raw;
SELECT count(*), max(updated_at) FROM stock_metric_fact;
SELECT count(*), max(generated_at) FROM common_feature_daily_fact;
```

대형 테이블은 처음에는 `--tables`로 작은 테이블부터 검증한다. 단, child table을 지정할 때 dependency closure가 자동 포함되는지 함께 확인한다. 이후 `daily_ohlcv`, `stock_metric_fact`, `common_feature_*`, 마지막에 `krx_security_flow_raw`, `dart_xbrl_fact_raw` 순으로 확인한다.

## 9. 구현 단계

### Phase 1. 대상 set 정리

- `MANAGED_MIRROR_TABLE_NAMES` 19개 정의
- `sync_checkpoints` 제외 테스트 추가
- `ingestion_runs` 제외 테스트 추가
- 기존 `PIPELINE_FULL_REFRESH_TABLE_NAMES` 참조 정리
- 19개 테이블의 FK dependency graph 정의
- cursor index DDL 추가

### Phase 2. Common copier 확장

- `TableSyncSpec` 확장
- 19개 테이블 spec 추가
- generic column discovery 적용
- JSONB 직렬화 경로 정리
- `conflict_constraint`와 `do_nothing_when_no_update_columns` 지원
- small replace의 delete/prune 또는 truncate/reload 경로 구현

### Phase 3. Incremental sync 구현

- local cursor 조회
- remote cursor 이후 batch fetch
- batch upsert
- refresh window/overlap sync 옵션 추가
- sequence sync
- table별 progress log

### Phase 4. CLI/문서 정리

- `--tables` 옵션 추가
- `--tables` dependency closure 자동 포함
- `--all-tables` deprecated alias 처리
- README의 대상 테이블 목록 갱신
- 운영 문서에 19개 source-data mirror set과 `ingestion_runs` 제외 사유 명시

### Phase 5. 검증

- unit/integration test 실행
- local DB에서 `--tables common_feature_daily_fact` dependency closure 포함 검증
- `stock_metric_fact` 증분 검증
- 대형 raw 테이블 순차 검증

## 10. 리스크와 대응

| 리스크 | 영향 | 대응 |
|---|---|---|
| 대형 테이블 cursor가 부정확함 | 누락 또는 중복 sync | natural key conflict upsert와 cursor test 강화 |
| raw source correction을 놓침 | 학습 데이터 불일치 | `fetched_at` 기반 update-aware cursor와 refresh window 사용 |
| derived source correction을 놓침 | 학습 데이터 불일치 | `updated_at/generated_at` 기반 update-aware cursor 사용 |
| `count(*)` 비용 과다 | sync 시작 지연 | remote는 pg_stat 추정치 사용, cursor max만 정확 조회 |
| FK 순서 오류 | insert 실패 | dependency sort와 고정 순서 테스트 |
| 부분 sync가 parent 없이 실행됨 | insert 실패 또는 mirror 불일치 | dependency closure 자동 포함, 불가능하면 reject |
| small table upsert가 삭제 row를 남김 | mirror 불일치 | replace/prune semantics 구현 |
| constraint 기반 unique를 표현 못함 | upsert 실패 또는 중복 | `conflict_constraint` spec 지원 |
| local schema drift | sync 실패 또는 잘못된 insert | sync 전 column match 검증 |
| 기존 `--all-tables` 사용자 혼란 | 운영 실수 | deprecation message와 README 갱신 |

## 11. 완료 기준

- `db sync-remote` 기본 실행이 19개 source-data mirror table을 대상으로 동작한다.
- local에 이미 있는 row는 재복사하지 않는다.
- raw table correction은 `fetched_at` 기반 cursor 또는 refresh window로 반영된다.
- `stock_metric_fact`, `common_feature_daily_fact`의 기존 row correction은 update로 반영된다.
- `sync_checkpoints`는 remote mirror 대상에서 제외된다.
- `ingestion_runs`는 기본 remote mirror 대상에서 제외되고 로컬 sync audit으로 남는다.
- `common_feature_*` 5개 테이블이 local mirror에 포함된다.
- 테스트가 19개 대상, cursor, FK dependency closure, update-aware upsert, small replace prune을 검증한다.
