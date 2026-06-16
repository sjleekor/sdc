# `stock_master_snapshot` 통계적 특성 프로파일

- 작성 일시: 2026-06-15
- 대상 DB: 로컬 PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 확인 방법: `sdc-db` helper로 로컬 DB read-only 집계. 현재 `sj2-server` 접근 불가.
- 적재 규모: **21 snapshots** / **12 as_of_date** / source **FDR**
- 참고: [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트 + §4.9 마스터/메타 특화 항목 적용.

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| `snapshot_id` | uuid | NO | PK |
| `as_of_date` | date | NO | snapshot 기준일 |
| `source` | text | NO | 현재 FDR |
| `fetched_at` | timestamptz | NO | 수집 시각 |
| `record_count` | integer | NO | snapshot row count |

- PK: `snapshot_id`
- 참조 테이블: `stock_master_snapshot_items.snapshot_id`
- remote sync cursor index: `(fetched_at, snapshot_id)`

---

## 1. 핵심 결론

- **규모**: 21개 snapshot, 12개 기준일. 기준일 범위는 2026-04-10 ~ 2026-06-10.
- **source**: 전부 FDR.
- **record_count**: 보통 2,769~2,773개 수준이나, 2026-05-21 02:17 UTC snapshot 하나는 948개뿐이다.
- **item count 정합성**: 모든 snapshot에서 `record_count = stock_master_snapshot_items` 실제 행수이며, distinct `(ticker, market)` 수도 동일하다.
- **중복 실행**: 2026-04-12 3회, 2026-04-25 3회, 2026-04-26 5회, 2026-05-21 2회 snapshot이 존재한다.
- **부분 snapshot 리스크**: 2026-05-21 02:17 UTC snapshot은 KOSPI 948개만 포함하고 KOSDAQ이 빠져 있다. 직후 08:24 UTC에 2,770개 전체 snapshot이 다시 들어왔다.
- **최신 snapshot**: 2026-06-10, 2,769개. `stock_master` ACTIVE 2,769개와 정확히 일치한다.
- **변화 추적**: snapshot 간 추가/제거는 대부분 소수지만, 부분 snapshot 때문에 2026-05-21 02:17 UTC에는 1,822개 removed, 직후 snapshot에는 1,822개 added로 보이는 잡음이 발생한다.

---

## 2. 조사 SQL

```sql
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT snapshot_id) AS snapshots,
       COUNT(DISTINCT as_of_date) AS as_of_dates,
       COUNT(DISTINCT source) AS sources,
       MIN(as_of_date) AS min_as_of_date,
       MAX(as_of_date) AS max_as_of_date,
       MIN(fetched_at) AS min_fetched_at,
       MAX(fetched_at) AS max_fetched_at,
       MIN(record_count) AS min_record_count,
       MAX(record_count) AS max_record_count
FROM stock_master_snapshot;

SELECT s.as_of_date, s.source, s.fetched_at, s.record_count,
       COUNT(i.*) AS item_rows,
       COUNT(DISTINCT (i.ticker, i.market)) AS distinct_pairs
FROM stock_master_snapshot s
LEFT JOIN stock_master_snapshot_items i USING(snapshot_id)
GROUP BY s.snapshot_id, s.as_of_date, s.source, s.fetched_at, s.record_count
ORDER BY s.as_of_date, s.fetched_at;
```

---

## 3. 실제 실행 결과

### 3.1 규모 / 범위

| snapshots | as_of_dates | sources | min_as_of_date | max_as_of_date | min_record_count | max_record_count |
|---:|---:|---:|---|---|---:|---:|
| 21 | 12 | 1 | 2026-04-10 | 2026-06-10 | 948 | 2,773 |

- `fetched_at`: 2026-04-10 14:24:58 UTC ~ 2026-06-10 14:25:51 UTC

### 3.2 기준일별 snapshot 수

| as_of_date | snapshots | min_record_count | max_record_count |
|---|---:|---:|---:|
| 2026-04-10 | 1 | 2,773 | 2,773 |
| 2026-04-12 | 3 | 2,773 | 2,773 |
| 2026-04-19 | 1 | 2,770 | 2,770 |
| 2026-04-20 | 1 | 2,770 | 2,770 |
| 2026-04-21 | 1 | 2,770 | 2,770 |
| 2026-04-22 | 1 | 2,769 | 2,769 |
| 2026-04-23 | 1 | 2,769 | 2,769 |
| 2026-04-24 | 1 | 2,770 | 2,770 |
| 2026-04-25 | 3 | 2,770 | 2,770 |
| 2026-04-26 | 5 | 2,770 | 2,770 |
| 2026-05-21 | 2 | 948 | 2,770 |
| 2026-06-10 | 1 | 2,769 | 2,769 |

### 3.3 snapshot별 item count 정합성

모든 snapshot에서 `record_count`, `item_rows`, `distinct_pairs`가 일치했다. 주요 구간:

| as_of_date | fetched_at UTC | record_count | item_rows | 비고 |
|---|---|---:|---:|---|
| 2026-04-10 | 2026-04-10 14:24:58 | 2,773 | 2,773 | 최초 확인 |
| 2026-04-19 | 2026-04-18 19:00:07 | 2,770 | 2,770 | 3개 감소 |
| 2026-04-22 | 2026-04-21 19:00:05 | 2,769 | 2,769 | 1개 감소 |
| 2026-04-24 | 2026-04-23 19:00:07 | 2,770 | 2,770 | 1개 증가 |
| 2026-05-21 | 2026-05-21 02:17:55 | 948 | 948 | KOSPI only partial snapshot |
| 2026-05-21 | 2026-05-21 08:24:03 | 2,770 | 2,770 | 전체 snapshot 복구 |
| 2026-06-10 | 2026-06-10 14:25:51 | 2,769 | 2,769 | 최신 |

### 3.4 market 구성

일반 snapshot은 KOSDAQ/KOSPI를 모두 포함한다. 2026-05-21 02:17 UTC partial snapshot만 KOSPI 948개만 포함했다.

| snapshot | KOSDAQ rows | KOSPI rows |
|---|---:|---:|
| 2026-04-10 | 1,823 | 950 |
| 2026-04-19 | 1,821 | 949 |
| 2026-05-21 02:17 UTC | 0 | 948 |
| 2026-05-21 08:24 UTC | 1,822 | 948 |
| 2026-06-10 | 1,822 | 947 |

### 3.5 인접 snapshot 변화량

| as_of_date | fetched_at UTC | record_count | prev_record_count | added_pairs | removed_pairs | changed_pairs |
|---|---|---:|---:|---:|---:|---:|
| 2026-04-19 | 2026-04-18 19:00:07 | 2,770 | 2,773 | 0 | 3 | 12 |
| 2026-04-21 | 2026-04-20 19:00:04 | 2,770 | 2,770 | 0 | 0 | 5 |
| 2026-04-22 | 2026-04-21 19:00:05 | 2,769 | 2,770 | 0 | 1 | 4 |
| 2026-04-24 | 2026-04-23 19:00:07 | 2,770 | 2,769 | 1 | 0 | 1 |
| 2026-05-21 02:17 UTC | 2026-05-21 02:17:55 | 948 | 2,770 | 0 | 1,822 | 2 |
| 2026-05-21 08:24 UTC | 2026-05-21 08:24:03 | 2,770 | 948 | 1,822 | 0 | 0 |
| 2026-06-10 | 2026-06-10 14:25:51 | 2,769 | 2,770 | 3 | 4 | 2 |

부분 snapshot을 제외하면 universe 변화는 매우 작다.

---

## 4. 모델링 시사점 / 후속 조치

- 특정 시점 universe를 재현하려면 `as_of_date`만으로 snapshot을 고르면 안 된다. 같은 `as_of_date`에 여러 snapshot이 있고, 2026-05-21처럼 부분 snapshot도 있다.
- PIT universe를 만들 때는 `as_of_date`별 최신 `fetched_at`을 선택하되, `record_count`가 비정상적으로 작은 snapshot은 품질 gate로 제외해야 한다.
- 최신 2026-06-10 snapshot은 `stock_master` ACTIVE universe와 일치하므로 현재 기준 universe로 적합하다.
- snapshot diff 기반 상장/상폐 이벤트를 만들려면 2026-05-21 02:17 UTC partial snapshot을 제외하지 않으면 1,822개 제거/추가라는 거짓 이벤트가 생긴다.
