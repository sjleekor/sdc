# 데이터베이스 스키마

전체 DDL은 [`sql/postgres_ddl.sql`](../sql/postgres_ddl.sql)에서 확인할 수 있습니다.

## 테이블

### 1. `stock_master`

각 상장 종목의 최신 상태를 저장합니다.

| 컬럼명         | 타입          | 비고                           |
|----------------|---------------|--------------------------------|
| `ticker`       | TEXT NOT NULL  | 6자리 KRX 종목 코드 (PK part 1) |
| `market`       | TEXT NOT NULL  | KOSPI \| KOSDAQ (PK part 2)   |
| `name`         | TEXT NOT NULL  | 종목명 (한글)                  |
| `listing_date` | DATE NULL      | 상장일 (모를 경우 NULL)        |
| `status`       | TEXT NOT NULL  | ACTIVE \| DELISTED \| UNKNOWN  |
| `last_seen_date` | DATE NOT NULL | 이 종목이 마지막으로 확인된 스냅샷 날짜 |
| `source`       | TEXT NOT NULL  | FDR \| PYKRX                   |
| `updated_at`   | TIMESTAMPTZ   | Insert/Update 시 자동 갱신     |

**기본키(Primary key):** `(ticker, market)`

### 2. `stock_master_snapshot`

특정 시점의 종목 유니버스 수집 메타데이터를 저장합니다.

| 컬럼명         | 타입          | 비고                         |
|----------------|---------------|------------------------------|
| `snapshot_id`  | UUID PK       | 스냅샷 고유 식별자           |
| `as_of_date`   | DATE NOT NULL  | 기준 날짜                    |
| `source`       | TEXT NOT NULL  | FDR \| PYKRX                 |
| `fetched_at`   | TIMESTAMPTZ   | 데이터를 수집한 시간         |
| `record_count` | INT NOT NULL   | 스냅샷에 포함된 종목 수      |

### 3. `stock_master_snapshot_items`

스냅샷에 캡처된 개별 종목들입니다. 임의의 두 스냅샷을 비교하여 신규 상장, 상장 폐지, 종목명 변경 등을 찾아낼 수 있습니다.

| 컬럼명         | 타입          | 비고                         |
|----------------|---------------|------------------------------|
| `snapshot_id`  | UUID FK       | `stock_master_snapshot` 참조 |
| `ticker`       | TEXT NOT NULL  | 6자리 KRX 종목 코드          |
| `market`       | TEXT NOT NULL  | KOSPI \| KOSDAQ              |
| `name`         | TEXT NOT NULL  | 스냅샷 당시의 종목명         |
| `listing_date` | DATE NULL      | 상장일                       |
| `status`       | TEXT NOT NULL  | ACTIVE \| DELISTED \| UNKNOWN |

**고유키(Unique):** `(snapshot_id, ticker, market)`

### 4. `daily_ohlcv`

일봉 OHLCV 가격 데이터입니다.

| 컬럼명       | 타입           | 비고                          |
|--------------|----------------|-------------------------------|
| `trade_date` | DATE NOT NULL   | 거래일 (PK part 1)            |
| `ticker`     | TEXT NOT NULL   | 6자리 KRX 종목 코드 (PK part 2)|
| `market`     | TEXT NOT NULL   | KOSPI \| KOSDAQ (PK part 3)   |
| `open`       | BIGINT NOT NULL | 시가 (KRW)                    |
| `high`       | BIGINT NOT NULL | 고가                          |
| `low`        | BIGINT NOT NULL | 저가                          |
| `close`      | BIGINT NOT NULL | 종가                          |
| `volume`     | BIGINT NOT NULL | 거래량                        |
| `source`     | TEXT NOT NULL   | PYKRX (현재 유일한 소스)      |
| `fetched_at` | TIMESTAMPTZ    | 데이터를 수집한 시간          |

**기본키(Primary key):** `(trade_date, ticker, market)`
**인덱스(Index):** `(ticker, market, trade_date DESC)` - 특정 종목별 조회를 위한 인덱스.

### 5. `ingestion_runs`

파이프라인의 모든 실행 이력을 기록하는 감사(Audit) 로그입니다.

| 컬럼명          | 타입           | 비고                           |
|-----------------|----------------|--------------------------------|
| `run_id`        | UUID PK        | 고유 실행 식별자               |
| `run_type`      | TEXT NOT NULL   | universe_sync \| daily_backfill \| validate |
| `started_at`    | TIMESTAMPTZ    | 실행 시작 시간                 |
| `ended_at`      | TIMESTAMPTZ    | 실행 종료 시간 (실행 중엔 NULL)|
| `status`        | TEXT NOT NULL   | running \| success \| failed   |
| `params`        | JSONB          | 실행 파라미터                  |
| `counts`        | JSONB          | 집계 카운터 (성공 건수 등)     |
| `error_summary` | TEXT           | 사람이 읽기 쉬운 에러 요약     |

## Upsert 전략

### `daily_ohlcv`

```sql
INSERT INTO daily_ohlcv (trade_date, ticker, market, open, high, low, close, volume, source, fetched_at)
VALUES (...)
ON CONFLICT (trade_date, ticker, market) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    source = EXCLUDED.source,
    fetched_at = EXCLUDED.fetched_at;
```

**설계 이유:** KRX에서 가끔 가격 데이터를 수정하는 경우가 있기 때문에, `DO NOTHING` 대신 `DO UPDATE`를 사용하여 재수집 시 기존(수정 전) 데이터를 덮어쓰도록 했습니다.

### `stock_master`

```sql
INSERT INTO stock_master (ticker, market, name, listing_date, status, last_seen_date, source)
VALUES (...)
ON CONFLICT (ticker, market) DO UPDATE SET
    name = EXCLUDED.name,
    listing_date = COALESCE(EXCLUDED.listing_date, stock_master.listing_date),
    status = EXCLUDED.status,
    last_seen_date = EXCLUDED.last_seen_date,
    source = EXCLUDED.source;
```

**참고:** `COALESCE`를 사용하여, 새로운 소스에서 상장일을 제공하지 않더라도 기존에 저장된 상장일 정보가 보존되도록 했습니다.

## 향후 확장: `intraday_ohlcv`

`sql/postgres_ddl.sql`에 주석 처리된 DDL을 참고하세요. 기본키(Primary key)는 `(trade_ts, ticker, market, interval)`이 될 예정입니다.