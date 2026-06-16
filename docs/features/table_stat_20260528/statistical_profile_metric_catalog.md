# `metric_catalog` 경량 통계 프로파일

- 작성 일시: 2026-06-15
- 대상 DB: 로컬 PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 확인 방법: `sdc-db` helper로 로컬 DB read-only 집계. 현재 `sj2-server` 접근 불가.
- 적재 규모: **29 metric** / **29 active metric** / **4 category** / **2 unit**
- 참고: [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트 + §4.11 운영/설정 테이블 적용.

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| `metric_code` | text | NO | PK, 표준 재무/주식수 metric code |
| `metric_name` | text | NO | 표시명 |
| `category` | text | NO | financial/share_count/shareholder_return/xbrl |
| `unit` | text | NO | KRW/shares |
| `description` | text | NO | metric 설명 |
| `is_active` | boolean | NO | 정규화 대상 활성 여부 |
| `updated_at` | timestamptz | NO | 갱신 시각 |

- PK: `metric_code`
- 참조 테이블: `metric_mapping_rule.metric_code`, `stock_metric_fact.metric_code`
- remote sync cursor index: `(updated_at, metric_code)`

---

## 1. 핵심 결론

- **규모**: metric 29개가 모두 active다.
- **사용 여부**: catalog 29개 전부 `stock_metric_fact`에 사용된다. unused catalog metric 0건, fact orphan metric 0건.
- **분류**: category는 4개다. `financial` 22개, `share_count` 2개, `shareholder_return` 1개, `xbrl` 4개.
- **단위**: `KRW` 25개, `shares` 4개다. share count/XBRL 주식수 계열은 `shares`, 나머지는 `KRW`다.
- **품질**: `metric_code`, `metric_name`, `category`, `unit`, `description` 빈값 0건.
- **갱신 시각**: 29개 전 row가 2026-06-10 15:26:24 UTC로 동일하다. 카탈로그 seed가 한 번에 반영된 상태다.
- **모델링 관점**: core 재무/현금흐름/자본 metric은 2015-2025 범위에서 넓게 존재하지만, XBRL 기반 감가상각/희석주식수/가중평균주식수 계열은 2023-2025 중심으로 sparse하다.

---

## 2. 조사 SQL

```sql
SELECT COUNT(*) AS rows,
       COUNT(*) FILTER (WHERE is_active) AS active_rows,
       COUNT(DISTINCT category) AS categories,
       COUNT(DISTINCT unit) AS units,
       MIN(updated_at) AS min_updated_at,
       MAX(updated_at) AS max_updated_at
FROM metric_catalog;

SELECT category, unit, COUNT(*) AS metrics,
       COUNT(*) FILTER (WHERE is_active) AS active_metrics
FROM metric_catalog
GROUP BY category, unit
ORDER BY category, unit;

WITH fact_metric AS (
  SELECT metric_code,
         COUNT(*) AS fact_rows,
         COUNT(DISTINCT ticker) AS tickers,
         MIN(bsns_year) AS min_year,
         MAX(bsns_year) AS max_year
  FROM stock_metric_fact
  GROUP BY metric_code
)
SELECT c.metric_code, c.category, c.unit,
       COALESCE(f.fact_rows, 0) AS fact_rows,
       COALESCE(f.tickers, 0) AS tickers,
       f.min_year, f.max_year
FROM metric_catalog c
LEFT JOIN fact_metric f USING (metric_code)
ORDER BY fact_rows DESC, c.metric_code;

SELECT COUNT(*) AS orphan_fact_metric_rows
FROM stock_metric_fact f
LEFT JOIN metric_catalog c USING (metric_code)
WHERE c.metric_code IS NULL;

SELECT COUNT(*) AS unused_catalog_metrics
FROM metric_catalog c
LEFT JOIN stock_metric_fact f USING (metric_code)
WHERE f.metric_code IS NULL;
```

---

## 3. 실제 실행 결과

### 3.1 전체 규모

| rows | active_rows | categories | units | min_updated_at | max_updated_at |
|---:|---:|---:|---:|---|---|
| 29 | 29 | 4 | 2 | 2026-06-10 15:26:24 UTC | 2026-06-10 15:26:24 UTC |

### 3.2 category / unit 분포

| category | unit | metrics | active_metrics |
|---|---|---:|---:|
| financial | KRW | 22 | 22 |
| share_count | shares | 2 | 2 |
| shareholder_return | KRW | 1 | 1 |
| xbrl | KRW | 2 | 2 |
| xbrl | shares | 2 | 2 |

### 3.3 품질 / 정합성

| 항목 | 값 |
|---|---:|
| empty `metric_code` | 0 |
| empty `metric_name` | 0 |
| empty `category` | 0 |
| empty `unit` | 0 |
| empty `description` | 0 |
| fact orphan metric rows | 0 |
| unused catalog metrics | 0 |

### 3.4 `stock_metric_fact` 사용량

| metric_code | fact_rows | tickers | year range |
|---|---:|---:|---|
| `issued_shares` | 81,061 | 2,650 | 2015-2025 |
| `treasury_shares` | 56,291 | 2,039 | 2015-2025 |
| `total_liabilities` | 49,891 | 2,604 | 2015-2025 |
| `total_assets` | 49,885 | 2,604 | 2015-2025 |
| `total_equity` | 49,826 | 2,604 | 2015-2025 |
| `cash_and_cash_equivalents` | 49,655 | 2,573 | 2015-2025 |
| `operating_cash_flow` | 49,564 | 2,598 | 2015-2025 |
| `investing_cash_flow` | 49,535 | 2,598 | 2015-2025 |
| `financing_cash_flow` | 49,431 | 2,598 | 2015-2025 |
| `interest_received` | 45,550 | 2,458 | 2015-2025 |
| `interest_paid` | 41,724 | 2,334 | 2015-2025 |
| `capex_ppe` | 41,198 | 2,330 | 2015-2025 |
| `capex_intangible` | 35,117 | 2,314 | 2015-2025 |
| `borrowing_proceeds_long_term` | 25,497 | 1,810 | 2015-2025 |
| `dividends_paid` | 21,380 | 1,617 | 2015-2025 |
| `treasury_share_acquisition_amount` | 16,278 | 1,565 | 2015-2025 |
| `dps` | 15,391 | 1,649 | 2015-2025 |
| `operating_income` | 6,874 | 234 | 2015-2025 |
| `sga` | 6,229 | 217 | 2015-2025 |
| `net_income` | 4,183 | 213 | 2015-2025 |
| `revenue` | 4,170 | 213 | 2015-2025 |
| `cogs` | 3,942 | 200 | 2015-2025 |
| `gross_profit` | 3,840 | 195 | 2015-2025 |
| `controlling_net_income` | 3,387 | 187 | 2015-2025 |
| `depreciation_expense` | 2,127 | 1,011 | 2023-2025 |
| `weighted_avg_shares` | 1,988 | 1,048 | 2023-2025 |
| `amortization_intangible_assets` | 1,260 | 594 | 2023-2025 |
| `diluted_shares` | 579 | 343 | 2023-2025 |
| `borrowing_repayments_long_term` | 113 | 57 | 2023-2025 |

---

## 4. 모델링 시사점 / 후속 조치

- catalog와 fact의 metric universe는 완전히 일치한다. metric code FK 정합성 관점에서 즉시 모델 feature catalog의 기준으로 쓸 수 있다.
- 전체 coverage가 높은 core metric은 주식수, 자산/부채/자본, 현금흐름 계열이다. 이들은 재무 피처의 1차 stable set으로 분리하는 것이 좋다.
- 손익계산서 계열(`revenue`, `gross_profit`, `operating_income`, `net_income` 등)은 ticker coverage가 200개대라 전체 universe 모델의 공통 입력으로 쓰기 어렵다. coverage mask 또는 sector/sub-universe 전용 피처로 다뤄야 한다.
- XBRL 기반 감가상각/가중평균주식수/희석주식수 계열은 2023년 이후만 존재한다. 긴 시계열 학습에서는 결측률이 높으므로 recent-window 모델에서만 우선 평가하는 것이 적절하다.
- `metric_catalog` 자체는 정합성이 좋지만 sparsity 판단은 반드시 `stock_metric_fact`의 metric별 coverage와 함께 해야 한다.
