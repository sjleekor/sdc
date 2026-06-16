# `dart_corp_master` 통계적 특성 프로파일

- 작성 일시: 2026-06-15
- 대상 DB: 로컬 PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 확인 방법: `sdc-db` helper로 로컬 DB read-only 집계. 현재 `sj2-server` 접근 불가.
- 적재 규모: **116,503 corp_code** / **2,657 active listed ticker-market pair** / source **OPENDART**
- 참고: [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트 + §4.8 DART 기업 마스터 특화 항목 적용.

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| `corp_code` | text | NO | PK, OpenDART 기업 고유번호 |
| `ticker` | text | YES | KRX 종목코드 |
| `corp_name` | text | NO | DART 회사명 |
| `market` | text | YES | KOSPI/KOSDAQ |
| `stock_name` | text | YES | DART 종목명 |
| `modify_date` | date | YES | DART corp code 파일 수정일 |
| `is_active` | boolean | NO | 현재 상장 매핑 활성 여부 |
| `source` | text | NO | OPENDART |
| `fetched_at` | timestamptz | NO | 수집 시각 |
| `updated_at` | timestamptz | NO | 갱신 시각 |

- PK: `corp_code`
- 보조 인덱스: `ticker`
- remote sync cursor index: `(updated_at, corp_code)`

---

## 1. 핵심 결론

- **전체 규모**: 116,503 corp_code. source는 전부 OPENDART.
- **활성 상장 매핑**: `is_active=true`는 2,657행이며 전부 non-empty ticker/market을 가진다. KOSDAQ 1,818개, KOSPI 839개.
- **전체 DART corp와 상장 universe의 차이**: 113,846행은 inactive이고, 이 중 112,544행은 ticker가 비어 있다. DART 전체 법인 목록이므로 자연스러운 구조다.
- **키 품질**: `corp_code` 중복 0건, non-empty `(ticker, market)` 중복 0건, ticker 중복 0건. listed 매핑 키로는 안정적이다.
- **stock_master 교집합**: DART listed pair 2,657개는 모두 `stock_master`에 존재한다. 반대로 `stock_master` 2,783개 중 126개는 DART 매핑이 없다.
- **최신 snapshot 교집합**: 최신 ACTIVE snapshot 2,769개 중 DART 매핑은 2,646개, 누락은 123개.
- **재무 팩트 교집합**: DART listed pair 2,657개 중 `stock_metric_fact`에 2,650개가 존재한다. DART 매핑이 된 종목은 정규화 재무 지표 커버리지가 거의 완전하다.
- **누락 유형**: `stock_master` 대비 DART 누락 126개 중 89개는 우선주/특수 ticker 후보, 23개는 문자 suffix ticker, 14개는 일반 숫자 ticker다.
- **종목명 drift**: `stock_master.name`과 `dart_corp_master.stock_name`이 다른 매칭 row가 32개 있다. 이름은 mutable하므로 join key로 쓰면 안 되고 `(ticker, market)` 또는 corp_code를 써야 한다.

---

## 2. 조사 SQL

```sql
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT corp_code) AS corps,
       COUNT(DISTINCT ticker) FILTER (WHERE ticker IS NOT NULL AND ticker <> '') AS nonempty_tickers,
       COUNT(DISTINCT market) FILTER (WHERE market IS NOT NULL AND market <> '') AS markets,
       COUNT(*) FILTER (WHERE is_active) AS active_rows,
       MIN(modify_date) AS min_modify_date,
       MAX(modify_date) AS max_modify_date,
       MIN(fetched_at) AS min_fetched_at,
       MAX(fetched_at) AS max_fetched_at
FROM dart_corp_master;

SELECT market, is_active, COUNT(*) AS rows,
       COUNT(DISTINCT ticker) FILTER (WHERE ticker IS NOT NULL AND ticker <> '') AS tickers,
       MIN(modify_date), MAX(modify_date)
FROM dart_corp_master
GROUP BY market, is_active;

WITH dc AS (
  SELECT DISTINCT ticker, market
  FROM dart_corp_master
  WHERE ticker IS NOT NULL AND ticker <> ''
    AND market IS NOT NULL AND market <> ''
), sm AS (
  SELECT DISTINCT ticker, market FROM stock_master
)
SELECT (SELECT COUNT(*) FROM dc) AS dart_pairs,
       (SELECT COUNT(*) FROM dc JOIN sm USING(ticker, market)) AS dart_in_stock_master,
       (SELECT COUNT(*) FROM sm LEFT JOIN dc USING(ticker, market) WHERE dc.ticker IS NULL) AS stock_master_missing_dart;
```

---

## 3. 실제 실행 결과

### 3.1 규모 / 범위

| rows | corps | nonempty_tickers | markets | active_rows | min_modify_date | max_modify_date |
|---:|---:|---:|---:|---:|---|---|
| 116,503 | 116,503 | 3,959 | 2 | 2,657 | 2017-06-30 | 2026-04-17 |

- `fetched_at`: 2026-04-19 10:24:14 UTC 단일
- `updated_at`: 2026-04-19 10:24:17 UTC 단일

### 3.2 market / active 분포

| market | is_active | rows | distinct tickers | min_modify_date | max_modify_date |
|---|---|---:|---:|---|---|
| `<empty>` | false | 113,846 | 1,302 | 2017-06-30 | 2026-04-17 |
| KOSDAQ | true | 1,818 | 1,818 | 2018-11-22 | 2026-04-17 |
| KOSPI | true | 839 | 839 | 2020-07-15 | 2026-04-16 |

Inactive row에도 ticker가 남아 있는 경우가 있으나 market이 비어 있어 listed pair로는 사용하지 않는다.

### 3.3 품질 결과

| 항목 | 값 |
|---|---:|
| empty ticker | 112,544 |
| empty market | 113,846 |
| empty stock_name | 0 |
| null modify_date | 0 |
| empty corp_name | 0 |
| corp_code duplicate groups | 0 |
| non-empty `(ticker, market)` duplicate groups | 0 |
| non-empty ticker duplicate groups | 0 |

`stock_name` duplicate group은 5,405개다. DART 전체 법인 목록의 이름은 key가 아니므로 `corp_code`를 기준으로 써야 한다.

### 3.4 modify_date 연도 분포

| modify_year | rows | listed_like_rows | active_rows |
|---:|---:|---:|---:|
| 2017 | 33,727 | 576 | 0 |
| 2018 | 3,863 | 36 | 1 |
| 2019 | 6,098 | 33 | 1 |
| 2020 | 5,315 | 41 | 2 |
| 2021 | 4,269 | 43 | 15 |
| 2022 | 16,472 | 349 | 261 |
| 2023 | 20,763 | 463 | 374 |
| 2024 | 11,111 | 619 | 478 |
| 2025 | 9,565 | 783 | 659 |
| 2026 | 5,320 | 1,016 | 866 |

### 3.5 주요 테이블과의 교집합

| 기준 | 값 |
|---|---:|
| DART listed pairs | 2,657 |
| active DART listed pairs | 2,657 |
| DART listed pairs in `stock_master` | 2,657 |
| `stock_master` pairs missing DART | 126 |
| DART listed pairs in latest snapshot | 2,646 |
| latest snapshot pairs missing DART | 123 |
| DART listed pairs in `daily_ohlcv` | 2,657 |
| DART listed pairs in `krx_security_flow_raw` | 2,656 |
| DART listed pairs in `stock_metric_fact` | 2,650 |

`dart_corp_master`에만 있고 `stock_master`에 없는 listed pair는 0개다.

### 3.6 `stock_master` 대비 DART 누락

| market | status | missing_dart |
|---|---|---:|
| KOSDAQ | ACTIVE | 13 |
| KOSDAQ | DELISTED | 2 |
| KOSPI | ACTIVE | 110 |
| KOSPI | DELISTED | 1 |

티커 패턴:

| pattern | rows |
|---|---:|
| likely_preferred_or_special | 89 |
| ends_with_letter | 23 |
| plain_numeric | 14 |

KOSPI 누락이 많은 이유는 우선주/특수 ticker가 DART corp master의 active listed pair로 잡히지 않기 때문이다.

### 3.7 이름 drift

`stock_master`와 DART가 같은 `(ticker, market)`으로 join되는 2,657개 중:

| 항목 | rows |
|---|---:|
| exact `stock_master.name = dart.stock_name` | 2,625 |
| stock_name mismatch | 32 |
| exact `stock_master.name = dart.corp_name` | 2,580 |

이름 mismatch 예:

| ticker | market | stock_master_name | dart_stock_name |
|---|---|---|---|
| `007820` | KOSDAQ | 엠엑스로보틱스 | 에스엠코어 |
| `018700` | KOSDAQ | 졸스 | 바른손 |
| `041930` | KOSDAQ | SY동아 | 동아화성 |
| `054050` | KOSDAQ | NH농우바이오 | 농우바이오 |
| `090470` | KOSDAQ | 제이스로보틱스 | 제이스텍 |
| `150900` | KOSDAQ | 파수AI | 파수 |
| `000390` | KOSPI | SP삼화 | 삼화페인트 |
| `005610` | KOSPI | 삼립 | SPC삼립 |

이름 변경/브랜드명 변경이 반영되는 시점 차이로 보이며, 이름 기반 join은 금지해야 한다.

---

## 4. 모델링 시사점 / 후속 조치

- DART 재무 피처와 결합할 universe는 `dart_corp_master` active listed pair 2,657개 또는 `stock_metric_fact` 2,650개를 기준으로 보는 것이 안전하다.
- 최신 거래가능 universe 2,769개와 재무 가능 universe 2,650개 사이에는 구조적 gap이 있다. 전체 종목 모델과 재무 포함 모델을 분리하거나, 재무 feature missing mask를 명시해야 한다.
- `stock_master` 대비 DART 누락 126개 중 다수는 우선주/특수 ticker다. 일반 숫자 ticker 14개는 실제 매핑 누락인지 별도 확인이 필요하다.
- DART `stock_name`과 현재 `stock_master.name`이 다를 수 있으므로, 문서/ETL에서 종목명은 표시용으로만 쓰고 join에는 `ticker`, `market`, `corp_code`를 사용해야 한다.
- DART corp master는 2026-04-19 수집본이다. universe 최신일 2026-06-10과 시점 차이가 있으므로, sj2 접근 복구 후 DART corp sync 최신 여부를 확인하는 것이 좋다.
