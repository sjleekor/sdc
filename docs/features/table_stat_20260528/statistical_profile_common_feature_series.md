# `common_feature_series` 경량 통계 프로파일

- 작성 일시: 2026-06-15
- 대상 DB: 로컬 PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 확인 방법: `sdc-db` helper로 로컬 DB read-only 집계. 현재 `sj2-server` 접근 불가.
- 적재 규모: **33 series** / **26 active series** / **5 source** / **12 category**

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| `series_id` | text | NO | PK |
| `source` | text | NO | ECOS/FDR/FRED/KRX/PYKRX |
| `source_series_key` | text | NO | 원천 API key |
| `category` | text | NO | feature category |
| `frequency` | text | NO | D/M |
| `name_kr` | text | NO | 한글명 |
| `name_en` | text | NO | 영문명 |
| `unit` | text | NO | 원천 단위 |
| `country` | text | NO | 국가 |
| `market` | text | NO | 시장 |
| `endpoint_params` | jsonb | NO | 원천 호출 파라미터 |
| `availability_policy` | text | NO | PIT 가용성 정책 |
| `manual_lag_days` | integer | NO | 수동 lag |
| `source_timezone` | text | NO | 원천 timezone |
| `history_start_date` | date | YES | 백필 시작 기준 |
| `max_stale_business_days` | integer | NO | stale 허용 기준 |
| `default_transform` | text | NO | 기본 변환 |
| `active` | boolean | NO | 수집/사용 활성 여부 |
| `notes` | text | NO | 비고 |
| `updated_at` | timestamptz | NO | 갱신 시각 |

- PK: `series_id`
- 참조 테이블: `common_feature_observation_raw.series_id`, `common_feature_catalog_input.series_id`

---

## 1. 핵심 결론

- **규모**: 33개 series 중 26개 active, 7개 inactive.
- **source 구성**: active series는 ECOS 7, FDR 5, FRED 3, KRX 11. PYKRX 3개는 모두 inactive.
- **inactive 구성**: KRX industry_index 4개, PYKRX market_index 3개. 업종지수 계열은 catalog/input도 있으나 inactive 상태다.
- **빈도**: D/M 두 종류. 월간(M)은 ECOS macro 4개(`macro_cpi`, `macro_ppi`, `macro_m2`, `macro_consumer_sentiment`)뿐이다.
- **가용성 정책**: 일간 국내/KRX 계열은 `next_krx_session`, 미국/FDR/FRED 계열 일부는 `same_krx_session_morning`, ECOS 월간 macro는 `manual_lag_days=20`.
- **품질**: `source_series_key`, `name_kr`, `endpoint_params`, `default_transform` 누락 0건. `endpoint_params`는 모두 JSON object.
- **브리지 주의**: `common_feature_catalog_input`에 연결되지 않은 series가 4개 있다. 이 중 `fx_usdkrw`는 active이고 raw observation도 존재하지만 현재 daily fact 생성에는 쓰이지 않는다.

---

## 2. 조사 SQL

```sql
SELECT COUNT(*) AS series_rows,
       COUNT(*) FILTER (WHERE active) AS active_series,
       COUNT(DISTINCT source) AS sources,
       COUNT(DISTINCT category) AS categories,
       COUNT(DISTINCT frequency) AS frequencies,
       MIN(history_start_date) AS min_history_start_date,
       MAX(history_start_date) AS max_history_start_date,
       MIN(updated_at) AS min_updated_at,
       MAX(updated_at) AS max_updated_at
FROM common_feature_series;

SELECT source, COUNT(*) AS series, COUNT(*) FILTER (WHERE active) AS active_series
FROM common_feature_series
GROUP BY source;

SELECT source, frequency, availability_policy, source_timezone,
       COUNT(*) AS series, COUNT(*) FILTER (WHERE active) AS active_series,
       MIN(manual_lag_days) AS min_manual_lag_days,
       MAX(manual_lag_days) AS max_manual_lag_days,
       MIN(max_stale_business_days) AS min_stale_days,
       MAX(max_stale_business_days) AS max_stale_days
FROM common_feature_series
GROUP BY source, frequency, availability_policy, source_timezone;

SELECT s.series_id, s.source, s.category, s.active
FROM common_feature_series s
LEFT JOIN common_feature_catalog_input i USING (series_id)
WHERE i.series_id IS NULL;
```

---

## 3. 실제 실행 결과

### 3.1 전체 규모

| series_rows | active_series | sources | categories | frequencies | min_history_start_date | max_history_start_date |
|---:|---:|---:|---:|---:|---|---|
| 33 | 26 | 5 | 12 | 2 | 1962-01-02 | 2008-07-01 |

`updated_at`은 전 row가 2026-06-14 08:06:13 UTC로 동일하다.

### 3.2 source별 series

| source | series | active_series |
|---|---:|---:|
| ECOS | 7 | 7 |
| FDR | 5 | 5 |
| FRED | 3 | 3 |
| KRX | 15 | 11 |
| PYKRX | 3 | 0 |

### 3.3 category별 series

| category | series | active_series |
|---|---:|---:|
| commodity | 2 | 2 |
| fx | 2 | 2 |
| global_index | 2 | 2 |
| global_risk | 1 | 1 |
| industry_index | 4 | 0 |
| macro_money | 1 | 1 |
| macro_price | 2 | 2 |
| macro_sentiment | 1 | 1 |
| market_breadth | 6 | 6 |
| market_index | 6 | 3 |
| market_liquidity | 2 | 2 |
| rate | 4 | 4 |

### 3.4 availability policy

| source | frequency | availability_policy | source_timezone | series | active_series | lag days | stale days |
|---|---|---|---|---:|---:|---:|---:|
| ECOS | D | next_krx_session | Asia/Seoul | 3 | 3 | 0 | 5-10 |
| ECOS | M | manual_lag_days | Asia/Seoul | 4 | 4 | 20 | 45-90 |
| FDR | D | next_krx_session | Asia/Seoul | 1 | 1 | 0 | 10 |
| FDR | D | same_krx_session_morning | America/New_York | 4 | 4 | 0 | 5 |
| FRED | D | same_krx_session_morning | America/New_York | 3 | 3 | 0 | 5-10 |
| KRX | D | next_krx_session | Asia/Seoul | 15 | 11 | 0 | 5 |
| PYKRX | D | next_krx_session | Asia/Seoul | 3 | 0 | 0 | 5 |

### 3.5 catalog input 미연결 series

| series_id | source | category | active |
|---|---|---|---|
| `fx_usdkrw` | FDR | fx | true |
| `market_kosdaq` | PYKRX | market_index | false |
| `market_kospi` | PYKRX | market_index | false |
| `market_kospi200` | PYKRX | market_index | false |

`fx_usdkrw`는 active raw source이지만 현재 active FX feature는 `fx_usdkrw_ecos`를 참조한다.

---

## 4. 모델링 시사점 / 후속 조치

- active series 기준 카탈로그는 작고 명확하지만, `fx_usdkrw`처럼 active인데 feature input에 쓰이지 않는 series가 있다. fallback 의도인지 설정 누락인지 결정해야 한다.
- inactive KRX industry series는 이미 catalog/input이 있으므로, 업종지수 수집이 가능해지는 시점에 active 전환 후보가 될 수 있다.
- ECOS 월간 series의 `max_stale_business_days`가 45-90일로 길다. daily fact 문서의 stale age와 함께 모델 feature gate에서 별도 관리해야 한다.
- 미국 원천의 `same_krx_session_morning` 정책은 look-ahead 검증의 핵심이므로, timezone/holiday 처리 테스트를 유지해야 한다.
