# metrics normalize 저메모리 구현 제안

## 배경

`metrics normalize`는 전체 사업연도 백필 범위를 한 번에 넘기면 메모리를 크게 사용한다. 현재 병목은 OpenDART raw 테이블 자체보다도, raw row를 Python 객체로 전부 올린 뒤 canonical fact 후보를 다시 모아 한 번에 upsert하는 구조에 있다.

주요 원인:

- `normalize_stock_metrics()`가 financial/share-count/shareholder-return/xbrl raw를 모두 한 번에 조회한다.
  - `src/krx_collector/service/normalize_metrics.py:453`
  - `src/krx_collector/service/normalize_metrics.py:458`
- `candidate_builders`에서 조회 결과를 다시 `list(...)`로 감싼다.
  - `src/krx_collector/service/normalize_metrics.py:475`
  - `src/krx_collector/service/normalize_metrics.py:507`
- repository가 `SELECT *` + `fetchall()`로 전체 row를 누적하고, normalize에 필요 없는 `raw_payload`까지 도메인 객체에 담는다.
  - `src/krx_collector/infra/db_postgres/repositories.py:1394`
  - `src/krx_collector/infra/db_postgres/repositories.py:1405`
  - `src/krx_collector/infra/db_postgres/repositories.py:1548`
  - `src/krx_collector/infra/db_postgres/repositories.py:1559`
- 최종 후보도 `selected_facts`에 전체 범위만큼 쌓고, upsert에서 다시 dedupe dict와 `execute_values` args list를 만든다.
  - `src/krx_collector/service/normalize_metrics.py:518`
  - `src/krx_collector/service/normalize_metrics.py:576`
  - `src/krx_collector/infra/db_postgres/repositories.py:1596`
  - `src/krx_collector/infra/db_postgres/repositories.py:1602`

보존해야 할 의미:

- canonical fact unique key는 `(ticker, metric_code, bsns_year, reprt_code)`이다.
- 같은 fact key 안에서는 mapping rule priority가 낮은 후보가 우선이다. 현재 CFS rule priority는 10, OFS는 20이라 CFS가 우선된다.
- XBRL은 `dimensions` 기반 candidate rank를 추가로 비교한다.
- catalog/rule seed는 normalize 실행마다 유지되어야 한다.

## 추천 순위

| 순위 | 구현안 | 메모리 절감 | 구현 리스크 | 추천 판단 |
| --- | --- | --- | --- | --- |
| 1 | 청크 단위 Python normalize + skinny row streaming | 높음 | 중간 | 현 코드 의미를 가장 잘 보존하면서 운영 메모리를 안정적으로 낮춘다. |
| 2 | Postgres set-based `INSERT ... SELECT` normalize | 매우 높음 | 높음 | 장기적으로 가장 효율적이나 SQL 복잡도와 회귀 위험이 크다. |
| 3 | 최소 변경 skinny filtered fetch + batched upsert | 중간 | 낮음 | 빠른 완화책으로 좋지만 전체 범위 후보 dict는 남는다. |

## 1순위: 청크 단위 Python normalize + skinny row streaming

### 개요

normalize 대상을 `(bsns_year, reprt_code, ticker batch)` 단위로 잘라 처리한다. 각 청크 안에서만 financial/share-count/shareholder-return/xbrl raw를 조회하고, 같은 청크 안에서 후보 우선순위를 계산한 뒤 즉시 upsert한다.

핵심은 “같은 unique key 후보들이 반드시 같은 청크 안에 들어오게” 하는 것이다. unique key에 ticker/year/report가 모두 포함되어 있으므로, ticker batch + 단일 year/report 청크는 현재 선택 의미를 보존할 수 있다.

### 구현 방향

1. `normalize_stock_metrics()`를 청크 루프로 변경한다.
   - catalog/rule seed는 기존처럼 시작 시 한 번 수행한다.
   - corp master는 전체를 읽어도 크기가 작으므로 유지 가능하다.
   - 루프 형태:

```python
for bsns_year in bsns_years:
    for reprt_code in reprt_codes:
        for ticker_batch in chunked(target_tickers, batch_size):
            selected_facts = normalize_one_chunk(bsns_year, reprt_code, ticker_batch)
            storage.upsert_stock_metric_facts(selected_facts)
```

2. normalize 전용 raw 조회 API를 추가한다.
   - 기존 `get_dart_*_raw()`는 다른 호출자 호환을 위해 유지한다.
   - 신규 API는 normalize에 필요한 컬럼만 조회한다.
   - `raw_payload`는 조회하지 않는다.
   - 가능하면 server-side cursor 또는 `fetchmany(page_size)`를 사용한다.

3. mapping rule로 raw 조회 조건을 선필터링한다.
   - financial: `account_id`, `fs_div`, `sj_div` 조건을 SQL에 반영한다.
   - xbrl: `concept_id`를 rule의 `account_id` 목록으로 제한한다.
   - share-count/shareholder-return: `row_name`, `statement_type`, `metric_code_match`, `stock_knd`를 SQL에 반영한다.

4. upsert도 청크 단위로 수행한다.
   - `selected_facts`는 한 ticker batch의 fact만 가진다.
   - `upsert_stock_metric_facts()` 내부 dedupe dict와 args list도 청크 크기만큼만 생성된다.

### 예상 변경 범위

- `src/krx_collector/service/normalize_metrics.py`
  - `normalize_one_chunk()` 또는 `_normalize_candidates()` helper 분리
  - `batch_size` 기본값 도입
- `src/krx_collector/ports/storage.py`
  - normalize 전용 iterator/fetch API 추가
- `src/krx_collector/infra/db_postgres/repositories.py`
  - skinny select + page fetch 구현
- `tests/unit/test_metric_normalization.py`
  - 기존 선택 우선순위 테스트 유지
  - 청크 경계가 결과를 바꾸지 않는 테스트 추가

### 장점

- Python matcher와 ranking 로직을 대부분 유지하므로 회귀 위험이 낮다.
- 메모리 사용량이 `전체 raw row 수`가 아니라 `batch_size 안의 filtered raw row 수`에 비례한다.
- 운영 중간 실패 시 이미 upsert된 chunk는 보존되므로 재실행에도 자연스럽다.
- batch size를 환경 변수나 CLI 옵션으로 조정할 수 있다.

### 단점 / 주의점

- source별 raw를 독립적으로 streaming만 하면 안 된다. 같은 fact key의 CFS/OFS, XBRL 후보 비교가 끝난 뒤 upsert해야 한다.
- 너무 작은 batch size는 DB round trip을 늘린다. 기본은 `100` 또는 `200` tickers로 시작하고 운영 RSS/시간을 보고 조정한다.
- `tickers=None`일 때 corp master active ticker 전체를 batch로 나누는 코드가 필요하다.

### 추천 기본값

```text
SDC_METRICS_NORMALIZE_BATCH_SIZE=100
```

전체 백필에서 메모리가 여전히 크면 50으로 낮추고, 실행 시간이 과하게 늘면 200으로 올린다.

## 2순위: Postgres set-based INSERT ... SELECT normalize

### 개요

Python에서 raw row를 만들지 않고 Postgres에서 후보를 계산한 뒤 바로 `stock_metric_fact`에 upsert한다. 각 source별 후보 SELECT를 `UNION ALL`하고, `row_number()`로 `(ticker, metric_code, bsns_year, reprt_code)`별 최우선 후보를 선택한다.

### 구현 방향

1. catalog/rule seed는 Python에서 기존처럼 수행한다.
2. repository에 `normalize_stock_metric_facts_sql(bsns_years, reprt_codes, tickers)`를 추가한다.
3. SQL 구조:

```sql
WITH candidates AS (
  SELECT ... FROM dart_financial_statement_raw raw
  JOIN metric_mapping_rule rule ON ...
  JOIN dart_corp_master corp ON ...
  UNION ALL
  SELECT ... FROM dart_share_count_raw raw
  JOIN metric_mapping_rule rule ON ...
  UNION ALL
  SELECT ... FROM dart_shareholder_return_raw raw
  JOIN metric_mapping_rule rule ON ...
  UNION ALL
  SELECT ... FROM dart_xbrl_fact_raw raw
  JOIN metric_mapping_rule rule ON ...
),
ranked AS (
  SELECT *,
         row_number() OVER (
           PARTITION BY ticker, metric_code, bsns_year, reprt_code
           ORDER BY priority, candidate_rank
         ) AS rn
  FROM candidates
)
INSERT INTO stock_metric_fact (...)
SELECT ...
FROM ranked
WHERE rn = 1
ON CONFLICT (ticker, metric_code, bsns_year, reprt_code)
DO UPDATE SET ...;
```

4. XBRL candidate rank는 SQL에서 계산한다.
   - `jsonb_array_length(dimensions) * 10`
   - dimensions text에 `ConsolidatedMember`, `SeparateMember`, `ReportedAmountMember`, `OperatingSegmentsMember` 포함 여부를 반영한다.

### 장점

- 애플리케이션 RSS는 거의 증가하지 않는다.
- DB optimizer가 index를 활용할 수 있어 전체 처리 시간이 줄 가능성이 있다.
- raw DTO 생성, Python matcher loop, 대량 args list 생성을 제거한다.

### 단점 / 주의점

- 현재 Python matcher 의미를 SQL로 옮기는 과정에서 회귀 위험이 크다.
- `period_type`, `period_end`, `source_key`, XBRL rank 계산이 SQL에 중복 구현된다.
- 테스트는 unit만으로 부족하고 Postgres integration test가 필요하다.
- SQL이 길어지면 mapping rule 확장 시 유지보수가 어려워질 수 있다.

### 적합한 시점

1순위 구현으로 메모리 문제를 안정화한 뒤에도 normalize 시간이 너무 길거나, canonical metric 수가 크게 늘어 Python loop 비용이 병목이 될 때 전환한다.

## 3순위: 최소 변경 skinny filtered fetch + batched upsert

### 개요

현재 함수 구조는 크게 유지하되, repository 조회만 먼저 줄인다. `SELECT *`와 `raw_payload` 적재를 제거하고, mapping rule에 걸릴 가능성이 있는 row만 가져오게 한다. 마지막 upsert는 일정 크기로 나눠 호출한다.

### 구현 방향

1. 기존 `get_dart_*_raw()` 대신 normalize 전용 `get_dart_*_raw_for_normalization()`을 추가한다.
   - 필요한 컬럼만 SELECT한다.
   - `raw_payload`는 제외한다.
   - financial/xbrl은 rule의 account/concept 목록으로 WHERE 조건을 좁힌다.
2. 서비스 함수는 기존처럼 source별 list를 만들되, row 수와 row 폭을 줄인다.
3. `facts = [...]` 생성 뒤 `upsert_stock_metric_facts()`를 page 단위로 호출한다.

### 장점

- 구현이 가장 작고 빠르다.
- `raw_payload` 제거만으로도 XBRL fact row의 메모리 폭을 크게 줄일 수 있다.
- 기존 unit test 대부분을 그대로 재사용할 수 있다.

### 단점 / 주의점

- 전체 범위 list와 `selected_facts` dict가 여전히 남는다.
- 연도 여러 개와 모든 보고서 코드를 한 번에 넘기는 백필에서는 근본 해결이 아닐 수 있다.
- `fetchall()`을 유지하면 DB 결과 set은 여전히 한 번에 클라이언트로 온다.

### 적합한 시점

운영 장애를 빠르게 완화해야 하고, 1순위 리팩터링을 바로 적용하기 어렵다면 임시 단계로 적용한다. 다만 최종 목표는 1순위로 두는 것이 맞다.

## 권장 실행 계획

1. 1순위 구현을 목표로 잡는다.
2. 첫 PR은 behavior-preserving refactor로 쪼갠다.
   - candidate 선택 로직을 helper로 분리
   - `unit_by_metric_code` dict를 만들어 catalog lookup 반복 제거
   - 기존 테스트가 그대로 통과하는지 확인
3. 두 번째 PR에서 청크 루프와 normalize 전용 skinny fetch API를 도입한다.
4. 세 번째 PR에서 운영 옵션을 추가한다.
   - CLI: `metrics normalize --batch-size 100`
   - env fallback: `SDC_METRICS_NORMALIZE_BATCH_SIZE`
5. 운영 검증 후 필요하면 2순위 SQL set-based 구현을 별도 실험 브랜치로 진행한다.

## 검증 항목

- 기존 unit test:

```bash
uv run pytest tests/unit/test_metric_normalization.py
```

- 청크 동등성 테스트:
  - 같은 fixture를 batch size `1`, `2`, `100`으로 normalize했을 때 `stock_metric_fact` 결과가 같아야 한다.
- 선택 우선순위 테스트:
  - CFS/OFS가 동시에 있으면 CFS가 선택되어야 한다.
  - XBRL dimensions rank가 낮은 후보가 선택되어야 한다.
- 운영 관찰:
  - normalize 시작/종료 시 RSS 로그를 남긴다.
  - chunk마다 `year`, `reprt_code`, `ticker_count`, `facts_written`, elapsed를 로그로 남긴다.

## 결론

추천은 1순위 구현이다. 현재 코드의 Python matcher와 priority semantics를 유지하면서도, 메모리를 전체 백필 크기가 아니라 ticker batch 크기에 묶을 수 있다. 2순위는 더 낮은 메모리와 잠재적으로 빠른 실행 시간을 제공하지만 SQL 이식 리스크가 크므로 장기 최적화로 두고, 3순위는 빠른 완화책으로만 보는 것이 적절하다.
