# `stock_master_snapshot_items` 통계적 특성 프로파일

- 작성 일시: 2026-06-15
- 대상 DB: 로컬 PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 확인 방법: `sdc-db` helper로 로컬 DB read-only 집계. 현재 `sj2-server` 접근 불가.
- 적재 규모: **56,357 행** / **21 snapshot** / **2,783 distinct ticker** / latest snapshot **2,769 rows**
- 참고: [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트 + §4.9 마스터/메타 특화 항목 적용.

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| `snapshot_id` | uuid | NO | FK -> `stock_master_snapshot.snapshot_id`, UQ |
| `ticker` | text | NO | KRX 종목코드, UQ |
| `market` | text | NO | KOSPI/KOSDAQ, UQ |
| `name` | text | NO | snapshot 당시 종목명 |
| `status` | text | NO | snapshot 당시 상태 |

- UNIQUE: `(snapshot_id, ticker, market)`
- FK: `snapshot_id` -> `stock_master_snapshot(snapshot_id)`

---

## 1. 핵심 결론

- **규모**: 56,357행, 21개 snapshot, 2,783 distinct ticker, 2개 market.
- **상태**: snapshot items의 `status`는 전부 ACTIVE다. DELISTED는 `stock_master`에는 남지만 snapshot items에는 없다.
- **품질**: `(snapshot_id, ticker, market)` 중복 0건, snapshot FK 고아 0건, ticker/market/name/status 빈값 0건.
- **시장 구성 누적**: KOSDAQ 36,428행/1,833 ticker, KOSPI 19,929행/950 ticker.
- **최신 snapshot**: 2026-06-10 기준 2,769개. `daily_ohlcv`와 `krx_security_flow_raw`는 최신 snapshot 전부를 커버한다.
- **재무/DART 커버리지**: 최신 snapshot 2,769개 중 `stock_metric_fact` 매칭 2,640개(누락 129), `dart_corp_master` 매칭 2,646개(누락 123).
- **주의 snapshot**: 2026-05-21 02:17 UTC snapshot은 KOSPI 948개만 포함한 partial snapshot이다. items 테이블만 보면 정상적인 ACTIVE row처럼 보이므로 snapshot 메타의 `record_count`/market 구성과 함께 필터해야 한다.

---

## 2. 조사 SQL

```sql
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT snapshot_id) AS snapshots,
       COUNT(DISTINCT ticker) AS tickers,
       COUNT(DISTINCT market) AS markets,
       COUNT(DISTINCT status) AS statuses
FROM stock_master_snapshot_items;

SELECT COUNT(*) AS duplicate_groups
FROM (
  SELECT snapshot_id, ticker, market
  FROM stock_master_snapshot_items
  GROUP BY 1,2,3
  HAVING COUNT(*) > 1
) d;

SELECT COUNT(*) AS orphan_snapshot_items
FROM stock_master_snapshot_items i
LEFT JOIN stock_master_snapshot s USING(snapshot_id)
WHERE s.snapshot_id IS NULL;

WITH latest AS (
  SELECT snapshot_id
  FROM stock_master_snapshot
  ORDER BY as_of_date DESC, fetched_at DESC
  LIMIT 1
), li AS (
  SELECT ticker, market
  FROM stock_master_snapshot_items
  JOIN latest USING(snapshot_id)
)
SELECT COUNT(*) AS latest_pairs,
       ...
FROM li;
```

---

## 3. 실제 실행 결과

### 3.1 규모

| rows | snapshots | tickers | markets | statuses |
|---:|---:|---:|---:|---:|
| 56,357 | 21 | 2,783 | 2 | 1 |

### 3.2 품질 결과

| 항목 | 값 |
|---|---:|
| duplicate `(snapshot_id,ticker,market)` groups | 0 |
| orphan snapshot items | 0 |
| empty ticker | 0 |
| empty market | 0 |
| empty name | 0 |
| empty status | 0 |

### 3.3 누적 시장 분포

| market | status | rows | snapshots | distinct tickers |
|---|---|---:|---:|---:|
| KOSDAQ | ACTIVE | 36,428 | 20 | 1,833 |
| KOSPI | ACTIVE | 19,929 | 21 | 950 |

KOSDAQ이 20개 snapshot에만 등장하는 이유는 2026-05-21 02:17 UTC partial snapshot이 KOSPI만 포함했기 때문이다.

### 3.4 snapshot별 row count 정합성

모든 snapshot에서 `stock_master_snapshot.record_count = stock_master_snapshot_items` 행수 = distinct pair 수다.

| as_of_date | fetched_at UTC | record_count | KOSDAQ | KOSPI |
|---|---|---:|---:|---:|
| 2026-04-10 | 2026-04-10 14:24:58 | 2,773 | 1,823 | 950 |
| 2026-04-19 | 2026-04-18 19:00:07 | 2,770 | 1,821 | 949 |
| 2026-04-22 | 2026-04-21 19:00:05 | 2,769 | 1,820 | 949 |
| 2026-04-24 | 2026-04-23 19:00:07 | 2,770 | 1,821 | 949 |
| 2026-05-21 02:17 UTC | 2026-05-21 02:17:55 | 948 | 0 | 948 |
| 2026-05-21 08:24 UTC | 2026-05-21 08:24:03 | 2,770 | 1,822 | 948 |
| 2026-06-10 | 2026-06-10 14:25:51 | 2,769 | 1,822 | 947 |

### 3.5 최신 snapshot의 주요 테이블 교집합

최신 snapshot은 2026-06-10, 2,769개 ACTIVE 종목이다.

| 기준 | 최신 snapshot 2,769개 중 매칭 | 누락 |
|---|---:|---:|
| `daily_ohlcv` | 2,769 | 0 |
| `krx_security_flow_raw` | 2,769 | 0 |
| `stock_metric_fact` | 2,640 | 129 |
| `dart_corp_master` | 2,646 | 123 |

`stock_master` 전체 기준보다 최신 ACTIVE snapshot 기준이 수급 커버리지는 더 깔끔하다. DELISTED 14개가 빠지면서 flow 누락 4개도 사라진다.

---

## 4. 모델링 시사점 / 후속 조치

- 모델 학습용 현재 거래가능 universe는 최신 snapshot 2,769개를 우선 기준으로 삼는 것이 좋다. 이 기준은 OHLCV와 flow가 모두 커버한다.
- 재무 피처를 결합하면 최신 snapshot에서도 129개가 빠진다. 이 누락은 별도 feature availability mask로 관리해야 한다.
- snapshot items는 snapshot 메타와 함께 사용해야 한다. partial snapshot 하나가 포함되어 있으므로 `record_count` 또는 market별 count gate 없이 diff를 만들면 잘못된 상장/상폐 이벤트가 생긴다.
- DELISTED 이력을 포함한 historical universe는 `stock_master`만으로는 부족하고, snapshot sequence에서 품질 gate를 거친 시점별 ACTIVE universe를 만들어야 한다.
