# `common_feature_catalog_input` 경량 통계 프로파일

- 작성 일시: 2026-06-15
- 대상 DB: 로컬 PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 확인 방법: `sdc-db` helper로 로컬 DB read-only 집계. 현재 `sj2-server` 접근 불가.
- 적재 규모: **56 input rows** / **54 feature_code** / **29 series_id** / **3 role**

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| `feature_code` | text | NO | PK, FK -> `common_feature_catalog.feature_code` |
| `series_id` | text | NO | PK, FK -> `common_feature_series.series_id` |
| `role` | text | NO | PK, primary/spread_long/spread_short |

- PK: `(feature_code, series_id, role)`
- FK: `feature_code` -> `common_feature_catalog(feature_code)` with `ON DELETE CASCADE`
- FK: `series_id` -> `common_feature_series(series_id)`

---

## 1. 핵심 결론

- **규모**: 56행, 54개 feature, 29개 series, 3개 role.
- **매핑 구조**: 52행은 `primary`, 4행은 spread용(`spread_long` 2, `spread_short` 2).
- **무결성**: PK duplicate 0건, feature FK 고아 0건, series FK 고아 0건.
- **coverage**: catalog feature 54개는 모두 input을 가진다. series 33개 중 input에 쓰이지 않는 series는 4개다.
- **active mapping**: active feature와 active series 조합 39행이 실제 모델 입력 후보다. active feature 37개 중 spread 2개가 각각 2개 series를 참조하므로 input row가 39개다.
- **inactive mapping**: inactive feature input 17행이 남아 있다. 이 중 9행은 active series를 참조하고, 8행은 inactive industry series를 참조한다. daily fact는 active feature만 생성하므로 현재 모델 입력에는 들어가지 않는다.
- **주의점**: active `fx_usdkrw` FDR series는 input에 쓰이지 않고, active FX feature 2개는 ECOS `fx_usdkrw_ecos`를 참조한다.

---

## 2. 조사 SQL

```sql
SELECT COUNT(*) AS input_rows,
       COUNT(DISTINCT feature_code) AS features_with_input,
       COUNT(DISTINCT series_id) AS series_used,
       COUNT(DISTINCT role) AS roles
FROM common_feature_catalog_input;

SELECT role, COUNT(*) AS rows
FROM common_feature_catalog_input
GROUP BY role;

SELECT c.active AS feature_active, s.active AS series_active,
       COUNT(*) AS rows,
       COUNT(DISTINCT i.feature_code) AS features,
       COUNT(DISTINCT i.series_id) AS series
FROM common_feature_catalog_input i
JOIN common_feature_catalog c USING (feature_code)
JOIN common_feature_series s USING (series_id)
GROUP BY c.active, s.active;

SELECT COUNT(*) AS catalog_without_input
FROM common_feature_catalog c
LEFT JOIN common_feature_catalog_input i USING (feature_code)
WHERE i.feature_code IS NULL;

SELECT COUNT(*) AS series_without_input
FROM common_feature_series s
LEFT JOIN common_feature_catalog_input i USING (series_id)
WHERE i.series_id IS NULL;
```

---

## 3. 실제 실행 결과

### 3.1 전체 규모

| input_rows | features_with_input | series_used | roles |
|---:|---:|---:|---:|
| 56 | 54 | 29 | 3 |

### 3.2 role 분포

| role | rows |
|---|---:|
| primary | 52 |
| spread_long | 2 |
| spread_short | 2 |

Spread input:

| feature_code | role | series_id | source |
|---|---|---|---|
| `rate_kr_term_spread_10y_3y` | spread_long | `rate_kr_gov10y` | ECOS |
| `rate_kr_term_spread_10y_3y` | spread_short | `rate_kr_gov3y` | ECOS |
| `rate_us_term_spread_10y_2y` | spread_long | `rate_us10y` | FRED |
| `rate_us_term_spread_10y_2y` | spread_short | `rate_us2y` | FRED |

### 3.3 active/inactive 조합

| feature_active | series_active | rows | features | series |
|---|---|---:|---:|---:|
| true | true | 39 | 37 | 25 |
| false | true | 9 | 9 | 5 |
| false | false | 8 | 8 | 4 |

Inactive feature가 active series를 참조하는 9행은 alias/fallback 성격의 feature다. 예: FRED WTI 수익률, ECOS FX alias, KRX direct market index alias.

### 3.4 FK / coverage 품질

| 항목 | 값 |
|---|---:|
| PK duplicate groups | 0 |
| feature FK orphan rows | 0 |
| series FK orphan rows | 0 |
| catalog features without input | 0 |
| series without input | 4 |

Input에 쓰이지 않는 series:

| series_id | source | category | active |
|---|---|---|---|
| `fx_usdkrw` | FDR | fx | true |
| `market_kosdaq` | PYKRX | market_index | false |
| `market_kospi` | PYKRX | market_index | false |
| `market_kospi200` | PYKRX | market_index | false |

### 3.5 category별 input

| category | active | input_rows | features | series |
|---|---|---:|---:|---:|
| commodity | true | 2 | 2 | 2 |
| commodity | false | 1 | 1 | 1 |
| fx | true | 2 | 2 | 1 |
| fx | false | 2 | 2 | 1 |
| global_index | true | 2 | 2 | 2 |
| global_risk | true | 1 | 1 | 1 |
| industry_index | false | 8 | 8 | 4 |
| macro_money | true | 3 | 3 | 1 |
| macro_price | true | 6 | 6 | 2 |
| macro_sentiment | true | 1 | 1 | 1 |
| market_breadth | true | 6 | 6 | 6 |
| market_index | true | 6 | 6 | 3 |
| market_index | false | 6 | 6 | 3 |
| market_liquidity | true | 2 | 2 | 2 |
| rate | true | 8 | 6 | 4 |

---

## 4. 모델링 시사점 / 후속 조치

- active feature 37개와 active input 39행은 daily fact 생성 범위와 일치한다. 모델 입력 lineage 추적에는 이 테이블을 신뢰해도 된다.
- active raw series `fx_usdkrw`가 input에서 빠져 있다. FDR USD/KRW를 fallback으로 쓸 의도라면 feature/input alias를 추가하거나, ECOS 우선/FDR 미사용 정책을 명시해야 한다.
- inactive feature input이 남아 있는 것은 문제는 아니지만, UI/ETL에서 active filter를 빠뜨리면 비활성 후보가 섞일 수 있다.
- spread feature는 2개 source observation을 참조하므로 lineage와 PIT 검증 시 배열 길이 2를 정상 케이스로 처리해야 한다.
