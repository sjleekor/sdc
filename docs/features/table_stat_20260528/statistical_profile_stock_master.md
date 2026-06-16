# `stock_master` 통계적 특성 프로파일

- 작성 일시: 2026-06-15
- 대상 DB: 로컬 PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 확인 방법: `sdc-db` helper로 로컬 DB read-only 집계. 현재 `sj2-server` 접근 불가.
- 적재 규모: **2,783 행** / **2,783 ticker** / **2 market** / latest active 기준일 **2026-06-10**
- 참고: [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트 + §4.9 마스터/메타 특화 항목 적용.

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| `ticker` | text | NO | KRX 종목코드, PK |
| `market` | text | NO | KOSPI/KOSDAQ, PK |
| `name` | text | NO | 종목명 |
| `status` | text | NO | ACTIVE/DELISTED |
| `last_seen_date` | date | NO | 마지막 확인 일자 |
| `source` | text | NO | 현재 FDR |
| `updated_at` | timestamptz | NO | 갱신 시각 |

- PK: `(ticker, market)`
- remote sync cursor index: `(updated_at, ticker, market)`

---

## 1. 핵심 결론

- **현재 universe**: 2,783개 `(ticker, market)` pair. KOSDAQ 1,833개, KOSPI 950개.
- **상태 구성**: ACTIVE 2,769개, DELISTED 14개. ACTIVE 전부 `last_seen_date=2026-06-10`.
- **source**: 전 row가 FDR 원천이다.
- **품질**: ticker/market/name/status/source 빈값 0건, ticker 중복 group 0건.
- **최신 snapshot 정합성**: 최신 `stock_master_snapshot`(2026-06-10, 2,769개)과 ACTIVE universe가 정확히 맞는다. `stock_master`에만 있는 14개는 모두 DELISTED다.
- **OHLCV 커버리지**: `daily_ohlcv`와 2,783개 전부 매칭된다. 가격 패널 기준 universe로 쓰기 좋다.
- **수급 커버리지**: `krx_security_flow_raw`와 2,779개 매칭, 4개 누락. 누락 4개는 전부 DELISTED/SPAC 성격이다.
- **재무/지표 커버리지**: `stock_metric_fact`와 2,650개 매칭, 133개 누락. 누락 중 ACTIVE 129개, DELISTED 4개다.
- **DART 매핑 커버리지**: `dart_corp_master`와 2,657개 매칭, 126개 누락. 누락은 우선주/특수 ticker/스팩/일부 active 종목명이 섞여 있으므로 모델 universe를 재무 피처와 결합할 때 명시적 필터가 필요하다.

---

## 2. 조사 SQL

```sql
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT ticker) AS tickers,
       COUNT(DISTINCT market) AS markets,
       COUNT(DISTINCT status) AS statuses,
       COUNT(DISTINCT source) AS sources,
       MIN(last_seen_date) AS min_last_seen_date,
       MAX(last_seen_date) AS max_last_seen_date,
       MIN(updated_at) AS min_updated_at,
       MAX(updated_at) AS max_updated_at
FROM stock_master;

SELECT market, status, source, COUNT(*) AS rows,
       MIN(last_seen_date), MAX(last_seen_date)
FROM stock_master
GROUP BY market, status, source;

WITH sm AS (SELECT DISTINCT ticker, market FROM stock_master),
     oh AS (SELECT DISTINCT ticker, market FROM daily_ohlcv),
     fl AS (SELECT DISTINCT ticker, market FROM krx_security_flow_raw),
     mf AS (SELECT DISTINCT ticker, market FROM stock_metric_fact),
     dc AS (
       SELECT DISTINCT ticker, market
       FROM dart_corp_master
       WHERE ticker IS NOT NULL AND ticker <> ''
         AND market IS NOT NULL AND market <> ''
     )
SELECT (SELECT COUNT(*) FROM sm) AS stock_master_pairs,
       (SELECT COUNT(*) FROM sm JOIN oh USING(ticker, market)) AS in_daily_ohlcv,
       (SELECT COUNT(*) FROM sm JOIN fl USING(ticker, market)) AS in_flow,
       (SELECT COUNT(*) FROM sm JOIN mf USING(ticker, market)) AS in_stock_metric_fact,
       (SELECT COUNT(*) FROM sm JOIN dc USING(ticker, market)) AS in_dart_corp_master;
```

---

## 3. 실제 실행 결과

### 3.1 규모 / 범위

| rows | tickers | markets | statuses | sources | min_last_seen_date | max_last_seen_date |
|---:|---:|---:|---:|---:|---|---|
| 2,783 | 2,783 | 2 | 2 | 1 | 2026-04-12 | 2026-06-10 |

- `updated_at`: 2026-04-18 19:00:07 UTC ~ 2026-06-10 14:25:51 UTC

### 3.2 시장 / 상태 분포

| market | status | source | rows | min_last_seen_date | max_last_seen_date |
|---|---|---|---:|---|---|
| KOSDAQ | ACTIVE | FDR | 1,822 | 2026-06-10 | 2026-06-10 |
| KOSDAQ | DELISTED | FDR | 11 | 2026-04-12 | 2026-05-21 |
| KOSPI | ACTIVE | FDR | 947 | 2026-06-10 | 2026-06-10 |
| KOSPI | DELISTED | FDR | 3 | 2026-04-12 | 2026-05-21 |

| last_seen_date | rows |
|---|---:|
| 2026-04-12 | 3 |
| 2026-04-21 | 1 |
| 2026-04-26 | 6 |
| 2026-05-21 | 4 |
| 2026-06-10 | 2,769 |

### 3.3 품질 결과

| 항목 | 값 |
|---|---:|
| empty ticker | 0 |
| empty market | 0 |
| empty name | 0 |
| empty status | 0 |
| empty source | 0 |
| ticker duplicate groups | 0 |

### 3.4 최신 snapshot 정합성

최신 snapshot: 2026-06-10, FDR, `record_count=2,769`.

| latest_item_rows | stock_master_rows | both_pairs | latest_only_pairs | stock_master_only_pairs | name_mismatches | status_mismatches |
|---:|---:|---:|---:|---:|---:|---:|
| 2,769 | 2,783 | 2,769 | 0 | 14 | 0 | 0 |

`stock_master`에만 있는 14개는 모두 DELISTED다.

| market | tickers |
|---|---|
| KOSDAQ | `139050`, `217620`, `230980`, `451700`, `452670`, `452980`, `455310`, `457630`, `462020`, `464680`, `466910` |
| KOSPI | `138490`, `140910`, `152550` |

### 3.5 주요 수집 테이블과의 교집합

| 기준 | stock_master 2,783개 중 매칭 | 누락 |
|---|---:|---:|
| `daily_ohlcv` | 2,783 | 0 |
| `krx_security_flow_raw` | 2,779 | 4 |
| `stock_metric_fact` | 2,650 | 133 |
| `dart_corp_master` | 2,657 | 126 |

다른 테이블에만 있고 `stock_master`에 없는 pair는 모두 0개다.

수급 누락 4개:

| ticker | market | name | status |
|---|---|---|---|
| `452670` | KOSDAQ | 상상인제4호스팩 | DELISTED |
| `455310` | KOSDAQ | 한화플러스제4호스팩 | DELISTED |
| `457630` | KOSDAQ | 대신밸런스제16호스팩 | DELISTED |
| `138490` | KOSPI | 코오롱ENP | DELISTED |

`stock_metric_fact` 누락 133개 구성:

| market | status | rows |
|---|---|---:|
| KOSDAQ | ACTIVE | 16 |
| KOSDAQ | DELISTED | 2 |
| KOSPI | ACTIVE | 113 |
| KOSPI | DELISTED | 2 |

DART 매핑 누락 126개 구성:

| market | status | missing_dart |
|---|---|---:|
| KOSDAQ | ACTIVE | 13 |
| KOSDAQ | DELISTED | 2 |
| KOSPI | ACTIVE | 110 |
| KOSPI | DELISTED | 1 |

티커 패턴상 DART 누락 126개 중 89개는 우선주/특수 ticker 후보(`5`, `7`, `K`, `L` 등), 23개는 문자 suffix, 14개는 일반 숫자 ticker다.

---

## 4. 모델링 시사점 / 후속 조치

- 가격 기반 모델의 기본 universe는 `stock_master` ACTIVE 2,769개 또는 최신 snapshot 2,769개를 기준으로 삼는 것이 가장 일관적이다.
- `stock_master` 전체 2,783개를 그대로 쓰면 DELISTED 14개가 포함된다. 학습/추론 목적에 따라 ACTIVE filter를 명시해야 한다.
- 재무 피처 결합 시 `stock_metric_fact` coverage가 2,650개로 줄어든다. 특히 KOSPI 우선주/특수 ticker 다수가 재무/DART 매핑에서 빠지므로, 보통주 중심 universe와 전체 거래가능 universe를 분리해야 한다.
- `daily_ohlcv`는 `stock_master` 전체를 커버하지만, `krx_security_flow_raw`는 DELISTED 4개가 빠져 있다. 수급 피처를 필수로 둘 경우 해당 4개는 제외된다.
- DART 누락 126개는 매핑 오류라기보다 우선주/스팩/특수 ticker 구조의 영향이 커 보인다. 그래도 일반 숫자 ticker 14개는 별도 확인 대상으로 남긴다.
