# `ingestion_runs` 경량 통계 프로파일

- 작성 일시: 2026-06-15
- 대상 DB: 로컬 PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 확인 방법: `sdc-db` helper로 로컬 DB read-only 집계. 현재 `sj2-server` 접근 불가.
- 적재 규모: **171 run** / **11 run_type** / **4 status** / 기간 **2026-04-10-2026-06-14 UTC**
- 참고: [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트 + §4.11 운영/설정 테이블 적용.

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| `run_id` | uuid | NO | PK |
| `run_type` | text | NO | job 종류 |
| `started_at` | timestamptz | NO | 시작 시각 |
| `ended_at` | timestamptz | YES | 종료 시각 |
| `status` | text | NO | running/success/failed/partial |
| `params` | jsonb | YES | 실행 파라미터 |
| `counts` | jsonb | YES | 처리 건수/요약 |
| `error_summary` | text | YES | 오류 요약 |

- PK: `run_id`
- DDL 주석에는 `running | success | failed`만 언급되어 있으나, 실제 데이터에는 `partial`도 존재한다.

---

## 1. 핵심 결론

- **규모**: 2026-04-10부터 2026-06-14까지 171개 run이 기록되어 있다.
- **상태 분포**: success 144개, running 16개, failed 7개, partial 4개다.
- **stale running**: `running` 16개는 모두 `ended_at`이 없고 2026-04-10-2026-05-21에 시작된 오래된 실행이다. 2026-06-15 기준 25-66일 경과했다.
- **최근 정상 실행**: `remote_db_sync`는 2026-06-14 15:44:52 UTC에 성공했다. local DB mirror의 최신 동기화 근거로 볼 수 있다.
- **주의 run type**: `krx_flow_sync`는 success가 없고 partial/running만 있다. 다만 `krx_security_flow_raw` 자체는 7,644만 행까지 적재되어 있어, 실행 로그 상태와 테이블 적재 상태를 함께 해석해야 한다.
- **JSON 품질**: `params`/`counts`가 object가 아닌 row는 0건이다. 다만 `params` NULL 3건, `counts` NULL 23건이 있다.
- **상태/종료시각 정합성**: running인데 `ended_at`이 있는 row 0건, non-running인데 `ended_at`이 없는 row 0건이다.

---

## 2. 조사 SQL

```sql
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT run_type) AS run_types,
       COUNT(DISTINCT status) AS statuses,
       MIN(started_at) AS min_started_at,
       MAX(started_at) AS max_started_at,
       MIN(ended_at) AS min_ended_at,
       MAX(ended_at) AS max_ended_at
FROM ingestion_runs;

SELECT status, COUNT(*) AS rows
FROM ingestion_runs
GROUP BY status
ORDER BY rows DESC;

SELECT run_type, status, COUNT(*) AS rows,
       MIN(started_at) AS first_started_at,
       MAX(started_at) AS last_started_at,
       COUNT(*) FILTER (WHERE ended_at IS NULL) AS null_ended_at
FROM ingestion_runs
GROUP BY run_type, status
ORDER BY run_type, status;

SELECT run_type,
       COUNT(*) AS running_rows,
       MIN(started_at) AS oldest_started_at,
       MAX(started_at) AS newest_started_at
FROM ingestion_runs
WHERE status = 'running'
GROUP BY run_type
ORDER BY oldest_started_at;

SELECT run_type,
       AVG(EXTRACT(EPOCH FROM ended_at - started_at)) AS avg_seconds,
       MAX(ended_at - started_at) AS max_duration
FROM ingestion_runs
WHERE ended_at IS NOT NULL
GROUP BY run_type
ORDER BY run_type;
```

---

## 3. 실제 실행 결과

### 3.1 전체 규모

| rows | run_types | statuses | min_started_at | max_started_at | max_ended_at |
|---:|---:|---:|---|---|---|
| 171 | 11 | 4 | 2026-04-10 14:24:30 UTC | 2026-06-14 14:43:29 UTC | 2026-06-14 15:44:52 UTC |

### 3.2 status 분포

| status | rows |
|---|---:|
| success | 144 |
| running | 16 |
| failed | 7 |
| partial | 4 |

### 3.3 run_type별 상태 요약

| run_type | rows | success | partial | failed | running |
|---|---:|---:|---:|---:|---:|
| `common_feature_build` | 40 | 40 | 0 | 0 | 0 |
| `common_feature_sync` | 35 | 34 | 1 | 0 | 0 |
| `daily_backfill` | 24 | 17 | 0 | 1 | 6 |
| `dart_corp_sync` | 9 | 8 | 0 | 1 | 0 |
| `dart_financial_sync` | 7 | 7 | 0 | 0 | 0 |
| `dart_share_info_sync` | 6 | 6 | 0 | 0 | 0 |
| `krx_flow_sync` | 8 | 0 | 2 | 0 | 6 |
| `metric_normalize` | 7 | 3 | 0 | 0 | 4 |
| `remote_db_sync` | 5 | 4 | 0 | 1 | 0 |
| `universe_sync` | 24 | 20 | 0 | 4 | 0 |
| `xbrl_parse` | 6 | 5 | 1 | 0 | 0 |

### 3.4 오래된 running row

| run_type | running_rows | oldest_started_at | newest_started_at | 2026-06-15 기준 경과 |
|---|---:|---|---|---|
| `daily_backfill` | 6 | 2026-04-10 14:25:09 UTC | 2026-04-12 10:57:45 UTC | 64-66일 |
| `metric_normalize` | 4 | 2026-04-19 11:53:09 UTC | 2026-04-26 10:16:13 UTC | 50-57일 |
| `krx_flow_sync` | 6 | 2026-04-25 14:48:06 UTC | 2026-05-21 08:31:03 UTC | 25-51일 |

| 정합성 항목 | 값 |
|---|---:|
| `status='running' AND ended_at IS NOT NULL` | 0 |
| `status<>'running' AND ended_at IS NULL` | 0 |

### 3.5 종료된 run 소요시간

| run_type | avg seconds | max duration |
|---|---:|---|
| `common_feature_build` | 0.05 | 00:00:00.158 |
| `common_feature_sync` | 23.23 | 00:03:23 |
| `daily_backfill` | 1,575.32 | 03:20:25 |
| `dart_financial_sync` | 1,204.17 | 01:50:00 |
| `krx_flow_sync` | 28,362.88 | 15:45:19 |
| `remote_db_sync` | 1,733.62 | 01:05:32 |
| `xbrl_parse` | 1,379.41 | 01:29:12 |

### 3.6 run_type별 최근 성공

| run_type | latest success ended_at |
|---|---|
| `common_feature_build` | 2026-06-11 14:14:18 UTC |
| `common_feature_sync` | 2026-06-11 14:14:06 UTC |
| `daily_backfill` | 2026-05-21 08:30:55 UTC |
| `dart_corp_sync` | 2026-05-23 13:56:23 UTC |
| `dart_financial_sync` | 2026-05-23 15:46:26 UTC |
| `dart_share_info_sync` | 2026-05-23 15:46:35 UTC |
| `metric_normalize` | 2026-05-23 17:16:18 UTC |
| `remote_db_sync` | 2026-06-14 15:44:52 UTC |
| `universe_sync` | 2026-05-21 08:24:04 UTC |
| `xbrl_parse` | 2026-05-23 17:15:51 UTC |

`krx_flow_sync`는 success row가 없다. 최신 row는 2026-05-21 08:31:03 UTC 시작 `running`이다.

### 3.7 오류/partial 하이라이트

| run_type | status | started_at | 요약 |
|---|---|---|---|
| `common_feature_sync` | partial | 2026-06-08 | 1 series error, `market_kospi` |
| `remote_db_sync` | failed | 2026-05-31 | no space left on device |
| `universe_sync` | failed | 2026-05-20/21 | FDR/pykrx KRX endpoint errors |
| `krx_flow_sync` | partial | 2026-04-26 | 4,051 errors, 7,391,599 rows upserted, pending metric 1 |
| `xbrl_parse` | partial | 2026-04-19 | 4 errors, 2,099 docs, 1,207,125 facts |
| `daily_backfill` | failed | 2026-04-12 | 1 ticker error, 5,736,753 bars upserted |
| `dart_corp_sync` | failed | 2026-04 | target table 생성 전 실행 실패 |

### 3.8 JSON 필드 품질

| 항목 | 값 |
|---|---:|
| `params IS NULL` | 3 |
| `counts IS NULL` | 23 |
| empty `error_summary` | 160 |
| non-object `params` | 0 |
| non-object `counts` | 0 |

---

## 4. 모델링 시사점 / 후속 조치

- 실행 로그는 수집 상태 판단에 유용하지만, 오래된 `running` row가 남아 있어 단순 status 집계만으로 현재 실행 중이라고 해석하면 안 된다.
- `remote_db_sync`의 2026-06-14 성공 로그와 `sync_checkpoints`의 2026-06-10 cursor를 함께 보면 로컬 mirror는 주요 테이블 기준 최근 동기화가 수행된 상태다.
- `krx_flow_sync`는 success가 없지만 원천 테이블 적재는 충분히 존재한다. 수급 데이터 품질 판단은 `krx_security_flow_raw` 프로파일을 우선하고, ingestion log는 운영 cleanup 대상으로 분리하는 것이 맞다.
- 오래된 running row 16개는 운영성 지표를 왜곡하므로, 실제 job heartbeat 또는 timeout 정책을 추가하고 stale 상태로 전환하는 관리 작업이 필요하다.
- `partial` status가 실제로 쓰이고 있으므로 DDL 주석, 운영 문서, 모니터링 쿼리의 status enum을 `partial`까지 반영해야 한다.
