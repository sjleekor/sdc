# `metric_mapping_rule` 경량 통계 프로파일

- 작성 일시: 2026-06-15
- 대상 DB: 로컬 PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 확인 방법: `sdc-db` helper로 로컬 DB read-only 집계. 현재 `sj2-server` 접근 불가.
- 적재 규모: **59 rule** / **59 active rule** / **29 metric_code** / **4 source_table**
- 참고: [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트 + §4.11 운영/설정 테이블 적용.

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| `rule_code` | text | NO | PK |
| `metric_code` | text | NO | FK -> `metric_catalog.metric_code` |
| `source_table` | text | NO | 원천 테이블 |
| `value_selector` | text | NO | 원천 값 선택 기준 |
| `priority` | integer | NO | 같은 metric 내 우선순위 |
| `statement_type` | text | NO | DART 보고서/계산서 계열 조건 |
| `fs_div` | text | NO | CFS/OFS 등 연결/별도 조건 |
| `sj_div` | text | NO | 재무제표 구분 조건 |
| `account_id` | text | NO | XBRL/DART account id 조건 |
| `account_nm` | text | NO | account name 조건 |
| `row_name` | text | NO | 주주환원 row 조건 |
| `stock_knd` | text | NO | 주식 종류 조건 |
| `dim1` | text | NO | XBRL dimension 조건 |
| `dim2` | text | NO | XBRL dimension 조건 |
| `dim3` | text | NO | XBRL dimension 조건 |
| `metric_code_match` | text | NO | 주주환원/공시 metric code 조건 |
| `is_active` | boolean | NO | 규칙 활성 여부 |
| `updated_at` | timestamptz | NO | 갱신 시각 |

- PK: `rule_code`
- FK: `metric_code` -> `metric_catalog(metric_code)`
- 참조 테이블: `stock_metric_fact.mapping_rule_code`
- 보조 인덱스: `(metric_code, source_table, priority)`, `(updated_at, rule_code)`

---

## 1. 핵심 결론

- **규모**: 59개 rule이 모두 active이고, catalog의 29개 metric을 모두 커버한다.
- **원천 구성**: `dart_financial_statement_raw` 44개, `dart_xbrl_fact_raw` 11개, `dart_share_count_raw` 2개, `dart_shareholder_return_raw` 2개다.
- **정합성**: rule이 참조하는 metric orphan 0건, fact가 참조하는 `mapping_rule_code` orphan 0건, `rule_code` 중복 0건이다.
- **우선순위 구조**: 재무제표 원천 22개 metric은 CFS priority 10, OFS priority 20의 2단 구조다. XBRL 4개 metric은 account id fallback을 포함해 2-3개 rule을 가진다.
- **미사용 rule**: 6개 rule은 현재 `stock_metric_fact`에서 사용되지 않았다. 모두 XBRL fallback account id 성격이며 오류라기보다 향후 공시 taxonomy 대응용 후보로 보인다.
- **충돌**: active `(metric_code, source_table, priority)` 충돌 0건이다.
- **품질**: 핵심 필드 `rule_code`, `metric_code`, `source_table`, `value_selector` 빈값 0건. source별로 무관한 selector 컬럼이 빈값인 것은 정상이다.

---

## 2. 조사 SQL

```sql
SELECT COUNT(*) AS rows,
       COUNT(*) FILTER (WHERE is_active) AS active_rows,
       COUNT(DISTINCT metric_code) AS metrics,
       COUNT(DISTINCT source_table) AS source_tables,
       MIN(priority) AS min_priority,
       MAX(priority) AS max_priority,
       MIN(updated_at) AS min_updated_at,
       MAX(updated_at) AS max_updated_at
FROM metric_mapping_rule;

SELECT source_table,
       COUNT(*) AS rules,
       COUNT(DISTINCT metric_code) AS metrics,
       MIN(priority) AS min_priority,
       MAX(priority) AS max_priority
FROM metric_mapping_rule
GROUP BY source_table
ORDER BY rules DESC;

SELECT r.rule_code, r.metric_code, r.source_table, r.priority, r.account_id
FROM metric_mapping_rule r
LEFT JOIN stock_metric_fact f ON f.mapping_rule_code = r.rule_code
WHERE f.mapping_rule_code IS NULL
ORDER BY r.source_table, r.metric_code, r.priority, r.rule_code;

SELECT metric_code, source_table, priority, COUNT(*) AS rules
FROM metric_mapping_rule
WHERE is_active
GROUP BY metric_code, source_table, priority
HAVING COUNT(*) > 1;
```

---

## 3. 실제 실행 결과

### 3.1 전체 규모

| rows | active_rows | metrics | source_tables | priority range | min_updated_at | max_updated_at |
|---:|---:|---:|---:|---|---|---|
| 59 | 59 | 29 | 4 | 10-20 | 2026-06-10 15:26:24 UTC | 2026-06-10 15:26:24 UTC |

### 3.2 source table 분포

| source_table | rules | metrics | priority range |
|---|---:|---:|---|
| `dart_financial_statement_raw` | 44 | 22 | 10-20 |
| `dart_xbrl_fact_raw` | 11 | 4 | 10-12 |
| `dart_share_count_raw` | 2 | 2 | 10 |
| `dart_shareholder_return_raw` | 2 | 1 | 10-20 |

### 3.3 규칙 패턴

| 패턴 | rule 수 | 설명 |
|---|---:|---|
| 재무제표 CFS/OFS | 44 | 22개 financial metric 각각 CFS priority 10, OFS priority 20 |
| 주식수 | 2 | `issued_shares`, `treasury_shares` 각각 1개 |
| 주주환원 | 2 | `treasury_share_acquisition_amount` 1개 metric에 2개 rule |
| XBRL | 11 | 감가상각/무형자산상각/가중평균주식수/희석주식수 fallback 포함 |

### 3.4 FK / 중복 / 충돌 품질

| 항목 | 값 |
|---|---:|
| rule -> metric orphan rows | 0 |
| fact -> mapping rule orphan rows | 0 |
| duplicate `rule_code` groups | 0 |
| active `(metric_code, source_table, priority)` conflict groups | 0 |

### 3.5 빈 selector 필드

| 항목 | 값 |
|---|---:|
| empty `rule_code` | 0 |
| empty `metric_code` | 0 |
| empty `source_table` | 0 |
| empty `value_selector` | 0 |
| empty `statement_type` | 57 |
| empty `fs_div` | 15 |
| empty `sj_div` | 15 |
| empty `account_id` | 4 |
| empty `account_nm` | 59 |
| empty `row_name` | 55 |
| empty `metric_code_match` | 57 |

대부분의 selector 컬럼은 source-specific 조건이다. 예를 들어 재무제표 rule은 `fs_div`/`sj_div`/`account_id`를 쓰고, 주식수 rule은 `stock_knd`, 주주환원 rule은 `row_name` 또는 `metric_code_match`, XBRL rule은 `account_id`/dimension을 쓴다.

### 3.6 현재 미사용 rule

| rule_code | metric_code | priority | account_id |
|---|---|---:|---|
| `xbrl.amortization_intangible_assets.dart_amortizationofintangibleassetsexpense` | `amortization_intangible_assets` | 11 | `dart_AmortizationOfIntangibleAssetsExpense` |
| `xbrl.depreciation_expense.ifrs-full_depreciationamortisationandimpairmentexpense` | `depreciation_expense` | 12 | `ifrs-full_DepreciationAmortisationAndImpairmentExpense` |
| `xbrl.diluted_shares.ifrs-full_weightedaveragenumberofordinarysharesoutstandingdiluted` | `diluted_shares` | 11 | `ifrs-full_WeightedAverageNumberOfOrdinarySharesOutstandingDiluted` |
| `xbrl.diluted_shares.ifrs-full_weightedaveragenumberofsharesoutstandingdiluted` | `diluted_shares` | 12 | `ifrs-full_WeightedAverageNumberOfSharesOutstandingDiluted` |
| `xbrl.weighted_avg_shares.ifrs-full_weightedaveragenumberofordinarysharesoutstandingbasic` | `weighted_avg_shares` | 11 | `ifrs-full_WeightedAverageNumberOfOrdinarySharesOutstandingBasic` |
| `xbrl.weighted_avg_shares.ifrs-full_weightedaveragenumberofsharesoutstandingbasic` | `weighted_avg_shares` | 12 | `ifrs-full_WeightedAverageNumberOfSharesOutstandingBasic` |

---

## 4. 모델링 시사점 / 후속 조치

- `metric_mapping_rule`은 현재 fact lineage를 추적하기에 충분히 정합적이다. `stock_metric_fact.mapping_rule_code`를 통해 원천 rule까지 거슬러 올라갈 수 있다.
- XBRL fallback rule 6개가 아직 사용되지 않았으므로, 이들을 실패로 보지 말고 taxonomy 확장 대비 rule로 유지하되 사용 여부 모니터링을 붙이는 것이 좋다.
- 재무제표 CFS/OFS priority 구조가 명확하다. 모델 설명 문서에서는 CFS 우선, OFS fallback이라는 해석을 명시해야 한다.
- 빈 selector 필드는 source-specific 구조에서 자연스럽다. 품질 체크는 전체 빈값 여부보다 source별 필수 컬럼 조건으로 작성해야 한다.
- 향후 rule 추가 시 `(metric_code, source_table, priority)` 충돌과 fact orphan 여부를 regression check로 고정하는 것이 적절하다.
