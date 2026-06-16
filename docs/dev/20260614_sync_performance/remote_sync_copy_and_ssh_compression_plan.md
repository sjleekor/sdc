# Remote Sync COPY Fast Path and SSH Compression Plan

- 작성일: 2026-06-14
- 대상: `krx-collector db sync-remote`
- 범위: 이전 성능 제안 중 2, 3, 5번만 적용한다.

## 구현 상태

2026-06-14에 본 계획의 주요 구현을 반영했다.

- 대형 update-aware 테이블에 `copy_merge_enabled`를 추가하고 incremental sync에서 remote `COPY (SELECT ...)` -> local temp staging -> `INSERT ... SELECT ... ON CONFLICT` merge 경로를 사용한다.
- `daily_ohlcv`는 generic COPY merge 경로로 옮기되 기존 `daily_ohlcv.fetched_at <= EXCLUDED.fetched_at` stale update guard와 checkpoint 저장을 유지한다.
- selected `--full-refresh`는 `--all-tables`가 아니어도 managed table을 no-commit truncate 후 binary `COPY`로 적재하고, truncate/copy/checkpoint를 먼저 commit한 뒤 sequence state를 별도 단계로 복제한다.
- SSH tunnel 압축은 `REMOTE_DB_SSH_COMPRESSION`, `--ssh-compression`, `--no-ssh-compression`으로 제어한다.

검증:

```bash
uv run ruff check src/krx_collector/infra/db_postgres/remote_sync.py src/krx_collector/service/sync_local_db.py src/krx_collector/cli/app.py src/krx_collector/infra/config/settings.py tests/unit/test_remote_db_sync.py tests/unit/test_cli_entrypoints.py
uv run pytest tests/unit
```

## 0. 적용 범위

이번 계획에서 다루는 항목은 세 가지다.

1. 대형 non-daily 테이블에도 staging + `COPY` 기반 incremental merge 경로를 적용한다.
2. `--full-refresh`가 `--all-tables`가 아니어도 선택된 managed mirror 테이블은 truncate 후 direct COPY fast path로 적재한다.
3. SSH tunnel 사용 시 선택적으로 `ssh -C` 압축을 켤 수 있게 한다.

이번 계획에서 제외하는 항목:

- 기본 sync 대상 축소 또는 분석용 preset 추가
- 테이블 단위 병렬 sync
- DB 인덱스/파라미터 튜닝
- sj2-server 수집 스케줄 변경
- schema나 managed mirror table set 변경

## 0.1 리뷰 반영 결정

| 리뷰 | 판단 | 반영 내용 |
|---|---|---|
| `daily_ohlcv` generic COPY merge 시 stale update guard가 빠질 수 있음 | 반영 | table별 conflict update predicate를 계획에 추가하고 `daily_ohlcv.fetched_at <= EXCLUDED.fetched_at` 조건을 수용 기준과 테스트에 명시 |
| `COPY (SELECT ...) TO STDOUT`에서 cursor/LIMIT parameter 처리 방식 누락 | 반영 | `copy_expert()`가 bind params를 받지 않는 점을 명시하고 `mogrify()` 기반 literal quoting helper를 추가 |
| selected full-refresh fast path에서 `_truncate_database_tables()`의 내부 commit으로 partial truncate가 확정될 수 있음 | 반영 | no-commit truncate variant를 요구하고 selected full-refresh 전체를 하나의 transaction으로 묶도록 수정 |
| FK dependency closure 표현이 모호함 | 반영 | "parent dependency closure + external child validation"으로 표현을 고정 |

## 1. 현재 병목 요약

현재 기본 증분 sync는 `daily_ohlcv`만 staging + `COPY`를 쓰고, 나머지 테이블은 `_fetch_remote_rows()`로 Python tuple list를 만든 뒤 `_upsert_rows()`에서 `execute_values()`로 upsert한다.

문제가 되는 지점:

- `src/krx_collector/infra/db_postgres/remote_sync.py`
  - `_sync_table()`은 batch마다 remote rows를 Python list로 materialize한다.
  - `_fetch_remote_rows()`는 `SELECT ... ORDER BY ... LIMIT %s` 결과를 `fetchall()`로 가져온다.
  - `_upsert_rows()`는 batch를 큰 SQL `VALUES` 문자열로 변환한다.
  - `_sync_daily_ohlcv_via_copy()`만 local temp staging + `COPY` merge를 사용한다.
  - `_copy_database_table()`에는 이미 full table binary COPY 스트리밍 구현이 있다.
  - `_open_ssh_tunnel()`은 현재 `ssh -C`를 지원하지 않는다.

sj2 기준 대형 테이블 규모:

| table | rows | total size |
|---|---:|---:|
| `dart_xbrl_fact_raw` | 80,143,928 | 107 GB |
| `krx_security_flow_raw` | 76,446,601 | 47 GB |
| `dart_financial_statement_raw` | 16,887,271 | 15 GB |
| `dart_shareholder_return_raw` | 7,831,054 | 5963 MB |
| `daily_ohlcv` | 6,550,517 | 1884 MB |

따라서 Python row materialization과 `execute_values()` SQL 생성 비용을 줄이는 것이 DB 성능 튜닝보다 먼저 할 가치가 있다.

## 2. 설계 목표

### 2.1 Generic Incremental COPY Merge

`daily_ohlcv` 전용 경로를 일반화해서 아래 조건을 만족하는 대형 테이블에도 적용한다.

- `spec.select_list`가 `spec.insert_columns`와 같은 column count/order로 stage에 적재 가능하다.
- `spec.order_columns`가 local staging table에서 다시 조회 가능한 실제 insert column이다.
- natural key conflict upsert semantics는 기존 `_upsert_rows()`와 동일하게 유지한다.
- surrogate id 보존 동작은 `preserve_remote_surrogate_columns`를 그대로 사용한다.
- table별 conflict update guard를 유지한다. 특히 `daily_ohlcv`는 기존 `WHERE daily_ohlcv.fetched_at <= EXCLUDED.fetched_at` 조건을 절대 제거하지 않는다.

우선 적용 후보:

- `daily_ohlcv`
- `krx_security_flow_raw`
- `dart_financial_statement_raw`
- `dart_share_count_raw`
- `dart_shareholder_return_raw`
- `dart_xbrl_document`
- `dart_xbrl_fact_raw`
- `stock_metric_fact`
- `common_feature_observation_raw`
- `common_feature_daily_fact`

작은 full-scan/prune 테이블은 기존 `_sync_table()` 경로를 유지한다. 이 테이블들은 row 수가 작고 pruning key set 수집이 필요하므로 COPY 경로의 이점이 작다.

### 2.2 Selected Full Refresh COPY Fast Path

현재 `--full-refresh --all-tables`는 `_sync_pipeline_public_tables_to_local()`에서 binary COPY를 사용하지만, `--full-refresh`만 사용하면 selected specs를 truncate한 뒤 `_sync_table()` 경로로 다시 upsert한다.

변경 후에는 `full_refresh=True`이고 `all_tables=False`인 경우에도 다음 순서로 동작하게 한다.

1. 선택된 specs와 FK parent dependency closure를 계산한다.
2. local/remote column compatibility를 검증한다.
3. 선택된 parent를 참조하는 omitted child가 있는지 external child validation으로 거부한다.
4. FK parent-first order로 truncate한다.
5. 같은 order로 `_copy_database_table()`을 사용해 binary COPY한다.
6. `daily_ohlcv`가 포함된 경우 checkpoint를 local max cursor 기준으로 재정렬한다.
7. truncate/copy/checkpoint transaction을 commit한다.
8. owned sequence state를 별도 단계로 복제한다.

이 경로는 target table이 비어 있으므로 conflict upsert가 필요 없다.

### 2.3 Optional SSH Compression

SSH tunnel을 사용할 때 `ssh -C`를 선택적으로 켤 수 있게 한다.

기본값은 `False`로 둔다. LAN에서는 압축이 CPU 병목이 될 수 있고, 원격/로컬 CPU 상황에 따라 더 느려질 수 있기 때문이다.

## 3. 구현 계획

### Phase 1. COPY merge 공통 유틸 분리

`remote_sync.py`에 기존 `_upsert_rows()`의 conflict SQL 생성을 재사용 가능한 helper로 분리한다.

예상 helper:

```python
def _build_conflict_action(spec: TableSyncSpec) -> str:
    ...

def _build_insert_select_from_stage_statement(
    *,
    spec: TableSyncSpec,
    stage_table: str,
) -> str:
    ...
```

기존 `_upsert_rows()`는 이 helper를 사용하게 바꿔 중복을 막는다.

conflict action helper는 table별 update predicate를 표현할 수 있어야 한다.

예상 field:

```python
conflict_update_where_sql: str | None = None
```

`daily_ohlcv` spec에는 아래 조건을 설정한다.

```sql
daily_ohlcv.fetched_at <= EXCLUDED.fetched_at
```

이 조건은 stale remote batch가 더 최신 local row를 덮는 것을 막는 현재 동작이므로 generic COPY merge로 흡수하더라도 보존해야 한다. 다른 테이블은 현재 `_upsert_rows()`와 같은 unconditional update를 기본값으로 둔다.

### Phase 2. Generic staging table 생성

대형 table용 temp stage 생성 함수를 추가한다.

권장 방식:

```sql
CREATE TEMP TABLE IF NOT EXISTS remote_sync_stage_<table> AS
SELECT <insert_columns>
FROM <table>
WHERE FALSE;
```

이 방식은 local target의 column type을 그대로 가져오며 PK/FK/unique/index를 만들지 않으므로 staging load가 빠르다.

주의:

- stage table name은 internal constant로만 조합하고, table name은 allowlisted `TableSyncSpec.name`만 사용한다.
- batch마다 `TRUNCATE TABLE <stage>` 후 `COPY FROM STDIN`한다.
- stage table은 `ON COMMIT DELETE ROWS` 또는 batch 단위 명시 truncate를 사용한다. commit 후에도 table 자체는 유지되어야 다음 batch에서 재사용할 수 있다.

### Phase 3. Remote COPY SELECT to local staging

대형 table batch를 Python row list로 가져오지 않고 PostgreSQL `COPY` stream으로 옮긴다.

권장 SQL:

```sql
COPY (
  SELECT <select_list>
  FROM <from_clause>
  WHERE (<order_columns>) > (...)
  ORDER BY <order_columns>
  LIMIT <batch_size>
) TO STDOUT WITH (FORMAT CSV, NULL '\N')
```

`copy_expert()`는 `execute(query, params)`처럼 parameter list를 받지 않으므로 cursor 값과 `LIMIT` 값은 안전하게 SQL에 반영해야 한다.

권장 방식:

- table/column/from/order SQL 조각은 `TableSyncSpec`에 정의된 allowlisted 값만 사용한다.
- cursor 값과 `batch_size`는 remote cursor의 `mogrify()`로 literal quoting한 뒤 COPY SQL에 삽입한다.
- 직접 문자열 포맷으로 사용자 입력 값을 넣지 않는다.
- 대안으로 `psycopg2.sql.Literal`을 사용할 수 있지만, 기존 psycopg2 connection의 adaptation을 확실히 쓰기 위해 `mogrify()`를 우선한다.

예상 helper:

```python
def _build_copy_select_sql(
    *,
    remote_cur: Any,
    spec: TableSyncSpec,
    cursor_values: tuple[Any, ...] | None,
    batch_size: int,
) -> str:
    ...
```

local 쪽:

```sql
COPY remote_sync_stage_<table> (<insert_columns>)
FROM STDIN WITH (FORMAT CSV, NULL '\N')
```

CSV COPY를 우선 사용한다. PostgreSQL이 JSONB, array, timestamp escaping을 직접 처리하므로 Python serializer를 새로 만들 필요가 없고, binary COPY의 cross-version 호환성 리스크도 줄어든다.

batch 처리 후:

1. `INSERT INTO target (...) SELECT ... FROM stage ON CONFLICT ...`로 merge한다.
2. stage에서 마지막 cursor를 조회한다.
3. commit한다.
4. 다음 batch로 진행한다.

마지막 cursor 조회:

```sql
SELECT <order_columns>
FROM remote_sync_stage_<table>
ORDER BY <order_columns> DESC
LIMIT 1
```

### Phase 4. TableSyncSpec에 COPY merge opt-in 추가

`TableSyncSpec`에 opt-in field를 추가한다.

예상 field:

```python
copy_merge_enabled: bool = False
```

우선 대형 update-aware 테이블에만 `True`를 설정한다. 작은 full-scan/prune 테이블은 `False`를 유지한다.

`sync_remote_tables_to_local()`의 일반 incremental loop는 다음 분기로 바꾼다.

1. `spec.copy_merge_enabled`이면 `_sync_table_via_copy_merge()`
2. 아니면 기존 `_sync_table()`

기존 `daily_ohlcv` 전용 `_sync_daily_ohlcv_via_copy()`는 generic 함수로 흡수하거나 thin wrapper로 남긴다. 다만 특수 처리는 checkpoint 저장만이 아니다. 기존 stale update guard인 `WHERE daily_ohlcv.fetched_at <= EXCLUDED.fetched_at`도 `daily_ohlcv` 전용 conflict update predicate로 반드시 유지한다.

### Phase 5. Selected full refresh fast path

`sync_remote_tables_to_local()`에서 `full_refresh=True`이고 `all_tables=False`인 경우 기존 spec loop 대신 새 helper를 호출한다.

예상 helper:

```python
def _sync_selected_public_tables_to_local(
    *,
    remote_conn: Any,
    local_conn: Any,
    specs: tuple[TableSyncSpec, ...],
) -> dict[str, int]:
    ...
```

내부 순서:

1. `_prepare_local_full_refresh_session(local_conn)`
2. `_database_tables_for_specs(specs)`
3. `_validate_no_external_fk_children(...)`
4. `_validate_full_database_columns(...)`
5. `_sort_tables_by_fk_dependencies(...)`
6. no-commit truncate helper로 target tables를 truncate한다.
7. table order대로 `_copy_database_table(...)`
8. `daily_ohlcv` 포함 시 `_reset_daily_ohlcv_checkpoint_from_local(...)`
9. truncate/copy/checkpoint transaction commit
10. `_sync_owned_sequences(...)`
11. sequence sync transaction commit

`--all-tables` 경로는 기존 `_sync_pipeline_public_tables_to_local()`을 유지한다. 차이는 `--all-tables`는 schema reset/drop 후 init까지 수행한다는 점이다.

현재 `_truncate_database_tables()`는 내부에서 commit한다. selected full-refresh fast path는 copy 중 실패했을 때 truncate만 확정되는 것을 피해야 하므로 다음 중 하나로 구현한다.

- `_truncate_database_tables(commit: bool = True)` 옵션을 추가하고 selected full-refresh에서는 `commit=False`로 호출한다.
- 또는 `_truncate_database_tables_no_commit()` helper를 별도로 추가한다.

`--all-tables` 경로의 기존 commit semantics는 이번 변경에서 건드리지 않는다. selected full-refresh fast path는 truncate, copy, checkpoint reset을 하나의 transaction으로 묶고 먼저 commit한다. PostgreSQL sequence state changes are not rolled back, so `_sync_owned_sequences()`는 table/checkpoint commit 이후 별도 단계로 수행한다.

### Phase 6. SSH compression 옵션 추가

설정:

- `Settings.remote_db_ssh_compression: bool = False`
- env: `REMOTE_DB_SSH_COMPRESSION=false`

CLI:

- `krx-collector db sync-remote --ssh-compression`
- 가능하면 `argparse.BooleanOptionalAction`을 사용해 `--no-ssh-compression`도 허용한다.
- CLI default는 `None`으로 두고, 미지정 시 settings 값을 사용한다.

service/API thread-through:

- `sync_remote_db_to_local(..., ssh_compression: bool = False)`
- `resolve_remote_dsn(..., ssh_compression: bool = False)`
- `_open_ssh_tunnel(..., compression: bool = False)`

SSH command 변경:

```text
ssh -C -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -N -L ...
```

`compression=False`이면 현재 command와 동일해야 한다.

## 4. 테스트 계획

### Unit tests

`tests/unit/test_remote_db_sync.py`

- `_build_conflict_action()`이 기존 `_upsert_rows()`와 같은 conflict SQL을 만든다.
- `daily_ohlcv` conflict SQL에 `WHERE daily_ohlcv.fetched_at <= EXCLUDED.fetched_at` stale update guard가 포함된다.
- `_build_copy_select_sql()`이 cursor 값과 `LIMIT` 값을 `mogrify()`로 quoting한다.
- COPY merge 대상 spec만 `copy_merge_enabled=True`인지 확인한다.
- `_sync_table_via_copy_merge()`가 remote `COPY (SELECT ...) TO STDOUT`와 local `COPY stage FROM STDIN`을 호출하는지 fake cursor로 검증한다.
- stage merge SQL에 `preserve_remote_surrogate_columns` assignment가 포함되는지 확인한다.
- `daily_ohlcv` COPY merge 후 checkpoint 저장 helper가 호출되는지 확인한다.
- selected full-refresh helper가 `_sync_table()`을 호출하지 않고 `_copy_database_table()`을 호출하는지 monkeypatch로 검증한다.
- selected full-refresh helper가 commit하는 truncate helper를 직접 호출하지 않거나 `commit=False`로 호출하는지 검증한다.
- selected full-refresh에서 `daily_ohlcv` 포함 시 checkpoint reset이 호출되는지 확인한다.
- partial selected full-refresh는 parent dependency closure를 포함하되, omitted child가 있으면 기존처럼 거부되는지 확인한다.
- `_open_ssh_tunnel(compression=True)` command에 `-C`가 포함되는지 확인한다.
- `_open_ssh_tunnel(compression=False)` command가 기존 command shape를 유지하는지 확인한다.

`tests/unit/test_cli_entrypoints.py`

- `db sync-remote --ssh-compression` parse가 성공한다.
- `db sync-remote --no-ssh-compression` parse가 성공한다.
- 옵션 미지정 시 settings 값이 service로 전달된다.

### Local smoke tests

작은 테이블로 빠르게 검증한다.

```bash
uv run krx-collector db sync-remote \
  --ssh-host whi@sj2-server \
  --tables dart_xbrl_document \
  --full-refresh
```

COPY merge incremental smoke:

```bash
uv run krx-collector db sync-remote \
  --ssh-host whi@sj2-server \
  --tables stock_metric_fact \
  --batch-size 10000
```

SSH compression smoke:

```bash
uv run krx-collector db sync-remote \
  --ssh-host whi@sj2-server \
  --ssh-compression \
  --tables dart_xbrl_document
```

### Regression checks

```bash
uv run pytest tests/unit/test_remote_db_sync.py tests/unit/test_cli_entrypoints.py
```

가능하면 before/after elapsed time을 같은 table set으로 기록한다.

권장 비교:

- `--tables dart_xbrl_document --full-refresh`
- `--tables stock_metric_fact`
- `--tables krx_security_flow_raw --batch-size 50000`는 운영 시간에 주의해서 별도 실행

## 5. 리스크와 대응

### COPY CSV escaping

원격 PostgreSQL이 CSV를 생성하고 로컬 PostgreSQL이 그대로 읽으므로 Python escaping보다 안전하다. 그래도 JSONB/text payload가 큰 테이블에서 smoke test를 반드시 수행한다.

### temp staging disk 사용량

batch 단위 staging이므로 temp table 크기는 `batch_size`에 비례한다. 기본 `REMOTE_DB_BATCH_SIZE=50000`은 유지하고, 대형 payload 테이블에서 문제가 있으면 table별 effective batch size를 낮출 수 있게 후속 옵션을 검토한다.

### full-refresh selected path의 FK 안정성

기존 `_validate_no_external_fk_children()`를 재사용한다. parent dependency closure는 자동 포함하지만 child closure는 자동 포함하지 않는다. parent만 truncate하고 child를 남기는 unsafe subset은 계속 거부한다.

### selected full-refresh transaction 경계

`TRUNCATE` 자체는 PostgreSQL transaction 안에서 rollback 가능하지만, 현재 `_truncate_database_tables()`는 내부 commit을 수행한다. selected full-refresh fast path에서는 no-commit truncate variant를 사용해 copy 실패 시 truncate도 rollback되도록 한다.

### COPY SELECT parameter quoting

`copy_expert()`는 bind parameter list를 받지 않는다. cursor 값과 `LIMIT` 값을 직접 문자열 보간하면 quoting bug나 SQL injection risk가 생길 수 있으므로, allowlisted SQL 조각과 `mogrify()`로 quoting한 literal만 조합한다.

### stale update guard 누락

`daily_ohlcv`는 현재 stale update guard를 갖고 있다. generic COPY merge로 옮기는 과정에서 이 조건이 빠지면 오래된 remote batch가 더 최신 local row를 덮을 수 있다. table별 conflict update predicate를 수용 기준과 unit test로 고정한다.

### SSH compression 역효과

압축은 기본 off다. `dart_xbrl_fact_raw`처럼 text/JSON payload가 큰 경우에는 효과가 있을 수 있지만, LAN 환경이나 CPU가 부족한 상황에서는 느려질 수 있다. 옵션으로만 제공하고 문서에 benchmark 권장을 남긴다.

### 기존 결과 count 의미

기존 `table_counts`는 "remote에서 읽어 merge 시도한 row 수"에 가깝다. COPY merge에서도 local COPY row count를 같은 의미로 사용한다. 실제 insert/update affected row 수로 의미를 바꾸지 않는다.

## 6. 수용 기준

- 기본 incremental sync에서 COPY merge enabled 테이블은 Python `fetchall()` + `execute_values()` 경로를 타지 않는다.
- `daily_ohlcv` COPY merge는 기존 stale update guard를 유지한다.
- remote `COPY (SELECT ...)` SQL은 cursor 값과 `LIMIT` 값을 안전하게 quoting한다.
- `--full-refresh` without `--all-tables`가 managed table truncate 후 direct COPY로 적재한다.
- selected full-refresh fast path는 truncate, copy, checkpoint reset을 하나의 transaction으로 묶으며 copy 실패 시 truncate가 확정되지 않는다.
- selected full-refresh sequence sync는 table/checkpoint commit 이후 별도 단계로 수행한다.
- selected full-refresh는 parent dependency closure와 external child validation semantics를 기존과 동일하게 유지한다.
- `--all-tables`의 schema reset semantics는 변경되지 않는다.
- `--ssh-compression`이 SSH tunnel command에 `-C`를 추가한다.
- `--no-ssh-compression` 또는 기본값은 기존 tunnel command와 동일하게 동작한다.
- 기존 remote sync unit tests와 CLI parser tests가 통과한다.
- `dart_xbrl_document` 또는 `stock_metric_fact` smoke sync 후 local/sj2 count가 일치한다.

## 7. 권장 구현 순서

1. conflict SQL builder를 분리하고 기존 `_upsert_rows()` 테스트를 유지한다.
2. selected full-refresh COPY fast path를 먼저 구현한다. 이미 `_copy_database_table()`가 있어 변경 범위가 작고 효과 확인이 쉽다.
3. SSH compression 옵션을 settings, CLI, service, tunnel까지 thread-through한다.
4. generic COPY merge를 구현하고 `stock_metric_fact` 같은 중간 크기 테이블로 smoke test한다.
5. `krx_security_flow_raw`, `dart_xbrl_fact_raw` 같은 초대형 테이블은 작은 `batch_size`부터 운영 시간 외에 검증한다.
