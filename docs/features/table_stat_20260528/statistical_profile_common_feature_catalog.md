# `common_feature_catalog` 경량 통계 프로파일

- 작성 일시: 2026-06-15
- 대상 DB: 로컬 PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 확인 방법: `sdc-db` helper로 로컬 DB read-only 집계. 현재 `sj2-server` 접근 불가.
- 적재 규모: **54 feature** / **37 active feature** / **12 category** / **7 transform_code**

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| `feature_code` | text | NO | PK |
| `feature_name_kr` | text | NO | 표시명 |
| `category` | text | NO | feature category |
| `frequency` | text | NO | D/M |
| `unit` | text | NO | 모델 노출 단위 |
| `transform_code` | text | NO | level/return/spread/mom/yoy |
| `description` | text | NO | 설명 |
| `active` | boolean | NO | 모델 노출 활성 여부 |
| `updated_at` | timestamptz | NO | 갱신 시각 |

- PK: `feature_code`
- 참조 테이블: `common_feature_catalog_input.feature_code`, `common_feature_daily_fact.feature_code`

---

## 1. 핵심 결론

- **규모**: catalog 54개 중 37개 active. daily fact는 active 37개 feature에 대해서만 생성되어 있다.
- **category 구성**: 12개 category. active가 0개인 category는 `industry_index`뿐이다.
- **transform 구성**: `level` 25개, `ret_1d` 12개, `ret_20d` 5개, `ret_5d` 4개, `mom` 3개, `yoy` 3개, `spread` 2개.
- **품질**: `feature_name_kr`, `unit`, `transform_code`, `description` 빈값 0건.
- **비활성 feature**: 17개. 업종지수 8개, market_index KRX direct alias 6개, FX ECOS alias 2개, FRED WTI 1개다.
- **모델링 관점**: active catalog는 시장지수/수급 breadth/금리/거시/원자재/환율을 포함하지만, 기간은 현재 2025-11-03 이후 daily fact에 한정된다.

---

## 2. 조사 SQL

```sql
SELECT COUNT(*) AS catalog_rows,
       COUNT(*) FILTER (WHERE active) AS active_features,
       COUNT(DISTINCT category) AS categories,
       COUNT(DISTINCT frequency) AS frequencies,
       COUNT(DISTINCT transform_code) AS transforms,
       MIN(updated_at) AS min_updated_at,
       MAX(updated_at) AS max_updated_at
FROM common_feature_catalog;

SELECT category, COUNT(*) AS features, COUNT(*) FILTER (WHERE active) AS active_features
FROM common_feature_catalog
GROUP BY category;

SELECT transform_code, COUNT(*) AS features, COUNT(*) FILTER (WHERE active) AS active_features
FROM common_feature_catalog
GROUP BY transform_code;

SELECT COUNT(*) FILTER (WHERE feature_name_kr='') AS empty_feature_name_kr,
       COUNT(*) FILTER (WHERE unit='') AS empty_unit,
       COUNT(*) FILTER (WHERE transform_code='') AS empty_transform_code,
       COUNT(*) FILTER (WHERE description='') AS empty_description
FROM common_feature_catalog;
```

---

## 3. 실제 실행 결과

### 3.1 전체 규모

| catalog_rows | active_features | categories | frequencies | transforms |
|---:|---:|---:|---:|---:|
| 54 | 37 | 12 | 2 | 7 |

`updated_at`은 전 row가 2026-06-14 08:06:13 UTC로 동일하다.

### 3.2 category 분포

| category | features | active_features |
|---|---:|---:|
| commodity | 3 | 2 |
| fx | 4 | 2 |
| global_index | 2 | 2 |
| global_risk | 1 | 1 |
| industry_index | 8 | 0 |
| macro_money | 3 | 3 |
| macro_price | 6 | 6 |
| macro_sentiment | 1 | 1 |
| market_breadth | 6 | 6 |
| market_index | 12 | 6 |
| market_liquidity | 2 | 2 |
| rate | 6 | 6 |

### 3.3 transform 분포

| transform_code | features | active_features |
|---|---:|---:|
| level | 25 | 19 |
| ret_1d | 12 | 5 |
| ret_20d | 5 | 3 |
| ret_5d | 4 | 2 |
| mom | 3 | 3 |
| yoy | 3 | 3 |
| spread | 2 | 2 |

### 3.4 품질 결과

| 항목 | 값 |
|---|---:|
| `feature_name_kr=''` | 0 |
| `unit=''` | 0 |
| `transform_code=''` | 0 |
| `description=''` | 0 |

### 3.5 active feature 구성

Active feature는 다음 축으로 구성된다.

| category | active feature 예 |
|---|---|
| commodity | WTI 20일 수익률 2종 |
| fx | USD/KRW level, 5일 수익률 |
| global_index/risk | S&P500, NASDAQ, VIX |
| macro_* | CPI/PPI/M2/소비심리 level, MoM, YoY |
| market_breadth/liquidity | KOSPI/KOSDAQ 상승/하락/보합, 거래대금 |
| market_index | KOSPI/KOSDAQ/KOSPI200 수익률, KOSPI 종가 |
| rate | 한국/미국 금리 level, term spread |

---

## 4. 모델링 시사점 / 후속 조치

- active catalog 37개는 현재 daily fact와 1:1로 맞는다. 모델 입력 후보 목록은 이 37개를 기준으로 시작하면 된다.
- `ret_20d`, `ret_5d`, `ret_1d` 계열은 warm-up NULL이 생기므로 feature별 valid-start date를 catalog summary에 추가하는 것이 좋다.
- inactive `industry_index` 8개는 업종 노출 피처로 가치가 크지만 아직 raw/fact가 비활성이다. 업종별 종목 mapping이 준비되면 별도 Wave로 켜는 것이 적절하다.
- FDR/ECOS 중복 성격의 FX/WTI alias가 일부 inactive다. source 우선순위와 fallback 정책을 명시해야 한다.
