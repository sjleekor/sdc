# `sync_checkpoints` 경량 통계 프로파일

- 작성 일시: 2026-06-15
- 대상 DB: 로컬 PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 확인 방법: `sdc-db` helper로 로컬 DB read-only 집계. 현재 `sj2-server` 접근 불가.
- 적재 규모: **1 checkpoint** / sync name **`remote_db_sync.daily_ohlcv`**
- 참고: [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트 + §4.11 운영/설정 테이블 적용.

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| `sync_name` | text | NO | PK, sync cursor 이름 |
| `cursor_payload` | jsonb | NO | 증분 cursor payload |
| `updated_at` | timestamptz | NO | checkpoint 갱신 시각 |

- PK: `sync_name`

---

## 1. 핵심 결론

- **규모**: checkpoint는 1개이며 `remote_db_sync.daily_ohlcv`만 존재한다.
- **cursor 위치**: cursor payload의 `trade_date`는 2026-06-10, `market`은 KOSDAQ, `ticker`는 `032685`다.
- **최신성**: checkpoint의 `trade_date=2026-06-10`은 로컬 DB의 `daily_ohlcv.MAX(trade_date)` 및 `krx_security_flow_raw.MAX(trade_date)`와 같다.
- **갱신 시각**: checkpoint `updated_at`은 2026-06-14 15:44:52 UTC이고, `ingestion_runs.remote_db_sync` 최신 성공 종료시각과 거의 같다.
- **품질**: 빈 `sync_name`, NULL cursor, non-object cursor 모두 0건이다.
- **해석 주의**: 현재 explicit checkpoint row는 `daily_ohlcv` 하나뿐이다. 다른 full-refresh/동기화 테이블이 미동기화라는 뜻은 아니며, 일부 sync는 테이블별 cursor query 또는 full-refresh 전략으로 동작한다.

---

## 2. 조사 SQL

```sql
SELECT COUNT(*) AS rows,
       MIN(updated_at) AS min_updated_at,
       MAX(updated_at) AS max_updated_at
FROM sync_checkpoints;

SELECT sync_name,
       cursor_payload,
       cursor_payload->>'trade_date' AS trade_date,
       cursor_payload->>'ticker' AS ticker,
       cursor_payload->>'market' AS market,
       cursor_payload->>'fetched_at' AS fetched_at,
       updated_at
FROM sync_checkpoints
ORDER BY sync_name;

SELECT COUNT(*) FILTER (WHERE sync_name = '') AS empty_sync_name,
       COUNT(*) FILTER (WHERE cursor_payload IS NULL) AS null_cursor_payload,
       COUNT(*) FILTER (WHERE jsonb_typeof(cursor_payload) <> 'object') AS non_object_cursor_payload
FROM sync_checkpoints;

SELECT (SELECT MAX(trade_date) FROM daily_ohlcv) AS max_daily_ohlcv_trade_date,
       (SELECT MAX(trade_date) FROM krx_security_flow_raw) AS max_flow_trade_date,
       (SELECT cursor_payload->>'trade_date'
          FROM sync_checkpoints
         WHERE sync_name = 'remote_db_sync.daily_ohlcv') AS checkpoint_trade_date;
```

---

## 3. 실제 실행 결과

### 3.1 전체 규모

| rows | min_updated_at | max_updated_at |
|---:|---|---|
| 1 | 2026-06-14 15:44:52 UTC | 2026-06-14 15:44:52 UTC |

### 3.2 checkpoint row

| sync_name | trade_date | market | ticker | fetched_at | updated_at |
|---|---|---|---|---|---|
| `remote_db_sync.daily_ohlcv` | 2026-06-10 | KOSDAQ | `032685` | 2026-06-10 14:32:22 UTC | 2026-06-14 15:44:52 UTC |

Cursor payload:

```json
{
  "market": "KOSDAQ",
  "ticker": "032685",
  "fetched_at": "2026-06-10T14:32:22.243217+00:00",
  "trade_date": "2026-06-10"
}
```

### 3.3 품질 결과

| 항목 | 값 |
|---|---:|
| empty `sync_name` | 0 |
| `cursor_payload IS NULL` | 0 |
| non-object `cursor_payload` | 0 |

### 3.4 주요 테이블 최신 trade date 비교

| 기준 | max/checkpoint trade_date |
|---|---|
| `daily_ohlcv.MAX(trade_date)` | 2026-06-10 |
| `krx_security_flow_raw.MAX(trade_date)` | 2026-06-10 |
| `remote_db_sync.daily_ohlcv` checkpoint | 2026-06-10 |

---

## 4. 모델링 시사점 / 후속 조치

- checkpoint는 로컬 mirror의 가격/수급 최신 거래일이 2026-06-10까지 맞춰져 있음을 확인하는 운영 근거로 쓸 수 있다.
- checkpoint row가 1개뿐이므로 데이터 freshness 대시보드는 `sync_checkpoints`만 보지 말고 각 핵심 테이블의 `MAX(trade_date)`, `MAX(updated_at)`, `ingestion_runs` 최신 성공을 함께 봐야 한다.
- `remote_db_sync.daily_ohlcv` cursor의 `fetched_at`은 2026-06-10이고 checkpoint update는 2026-06-14다. 수집 시각과 로컬 동기화 시각을 분리해 해석해야 한다.
- sj2 접근이 복구되면 동일 cursor와 sj2의 `MAX(trade_date)`를 비교해 local mirror lag를 명시해야 한다.
