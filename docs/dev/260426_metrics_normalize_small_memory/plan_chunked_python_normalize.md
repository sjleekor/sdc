# 1순위 구현 상세 계획: 청크 단위 Python normalize + skinny row streaming

`proposal.md` 1순위(청크 단위 Python normalize)를 실행하기 위한 단계별 구현 계획. 검토 피드백을 반영하여 다음을 추가했다.

- **사전 측정 단계**(가설 검증)를 PR 0으로 추가
- **tiebreaker 도입 PR**을 청크화보다 먼저 실행 (비결정성 제거)
- 청크 차원을 `ticker batch` **단일 차원**으로 단순화 (year/report 곱연산 round trip 회피)
- mapping rule wildcard 단서, `share-count.se` 컬럼명, batch_size env 우선 도입 등 review 피드백 반영
- 운영 옵션은 첫 도입부터 env로 노출, CLI flag는 후행

---

## 의미 보존 invariants (모든 PR에서 유지)

| Invariant | 근거 |
| --- | --- |
| canonical fact unique key는 `(ticker, metric_code, bsns_year, reprt_code)` | `normalize_metrics.py:568`, `repositories.py:1597` |
| 같은 fact key 안에서 `(rule.priority, candidate_rank)`이 더 작은 후보가 선택됨 | `normalize_metrics.py:571` |
| CFS rule priority 10, OFS rule priority 20 → CFS 우선 | `normalize_metrics.py:242-243` |
| XBRL는 `_xbrl_candidate_rank`로 dimensions 기반 추가 비교 | `normalize_metrics.py:407-418` |
| catalog/rule seed는 normalize 실행마다 재적용 | `normalize_metrics.py:444-445` |
| corp master active filter (`market is not None`) | `normalize_metrics.py:448-450` |

테스트는 위 invariant를 fixture로 못박은 뒤, 청크 경계가 결과를 바꾸지 않음을 추가 검증한다.

---

## PR 0 — 메모리 진단 (실행 전 1회, 변경 없음)

가설은 “raw row 자체보다도 Python 객체화 + `raw_payload` 적재가 주범”이다. 이걸 측정으로 확정해야 batch_size 기본값과 PR 우선순위가 근거를 갖는다.

### 작업

1. 운영 DB(또는 prod 스냅샷)에서 다음을 측정:

   ```sql
   -- 평균/최대 raw_payload 크기 (테이블별)
   SELECT
     'dart_xbrl_fact_raw' AS tbl,
     count(*)                                  AS rows,
     avg(pg_column_size(raw_payload))::bigint  AS avg_payload_bytes,
     max(pg_column_size(raw_payload))::bigint  AS max_payload_bytes,
     avg(pg_column_size(t.*))::bigint          AS avg_row_bytes
   FROM dart_xbrl_fact_raw t
   WHERE bsns_year IN (2024) AND reprt_code = '11011';
   -- financial / share_count / shareholder_return도 동일하게
   ```

2. 1년치 normalize 실행 시 RSS를 한 번 캡처:
   - `psutil.Process().memory_info().rss`를 normalize 시작 / 각 source fetch 후 / candidate 누적 후 / upsert 직전에 로그
   - 각 source의 `rows` 길이도 같이 로그
3. 결과를 `docs/dev/260426_metrics_normalize_small_memory/measurement.md` (또는 본 문서 부록)으로 남긴다.

### 산출물 → 후속 PR에 미치는 영향

- `raw_payload`가 row 폭의 압도적 비중이면 → 3순위(skinny select)만으로도 큰 효과 → 1순위 batch_size를 더 크게 잡아도 안전
- XBRL row 수 vs financial row 수 비율 → 청크 batch_size 산정 근거
- 측정값을 PR 설명에 인용한다.

---

## PR 1 — Behavior-preserving refactor (의미 보존 단순화)

청크 도입 전에 반드시 깔아둘 평지작업. **외부 동작 변화 없음.**

### 1-A. unit lookup O(1) 화 (필수)

`normalize_metrics.py:555-562`의 `next(... for entry in catalog ...)`는 매 fact마다 catalog 전체 선형 스캔이다. catalog 진입 직후 dict화한다.

```python
unit_by_metric_code = {entry.metric_code: entry.unit for entry in catalog}
# fact 생성 시
unit=unit_by_metric_code.get(rule.metric_code, ""),
```

### 1-B. candidate 선택 helper 분리 (필수)

청크 루프 도입을 위해 inner loop body를 함수로 빼낸다. 시그니처는 청크화 PR에서 그대로 재사용된다.

```python
def _collect_candidates(
    builders: Sequence[CandidateBuilder],
    rules_by_source: dict[str, list[MetricMappingRule]],
    corp_by_ticker: dict[str, DartCorpMaster],
    unit_by_metric_code: dict[str, str],
) -> dict[tuple[str, str, int, str], tuple[int, int, int, StockMetricFact]]:
    selected: dict[...] = {}
    for builder in builders:
        for row in builder.rows:
            # 기존 inner loop, 그러나 (priority, candidate_rank, tiebreaker) tuple로 비교
            ...
    return selected
```

### 1-C. **tiebreaker 도입 (필수, 청크화 전제조건)**

현재 575줄은 `(priority, candidate_rank) < current` 비교라 동률 시 first-wins, 즉 fetch 순서 의존이다. 청크 경계가 row 순서를 바꾸면 동률 후보 선택이 달라질 수 있어 청크 동등성 테스트가 깨진다.

추가 tiebreaker 키를 명시:

```python
def _tiebreaker(row, source_key: str) -> tuple[int, str]:
    # 결정성을 보장하는 보조 키. 의미 변화는 없도록 source_key를 사용.
    return source_key  # 또는 (rcept_no, ord) 등 source별로 정의
```

비교 튜플을 `(priority, candidate_rank, tiebreaker)`로 확장한다. 기존 fixture가 동률을 만드는지 확인하고, 만들지 않는 fixture라면 동작 변화 없음. 만든다면 fixture를 명시적 priority 차이로 보강.

### 1-D. corp filter 이중 체크 정리 (선택)

`corp_by_ticker`는 이미 `market is not None`로 필터됐는데 532줄에서 또 None 체크. 한 곳으로 통합.

### 산출물

- `tests/unit/test_metric_normalization.py` 모두 통과
- 동률 후보 선택이 결정적임을 확인하는 단위 테스트 1건 추가

---

## PR 2 — Repository skinny fetch + ticker chunk iterator

청크 루프의 **repository 측 인프라**를 먼저 도입한다. service는 아직 변경하지 않고, 기존 호출자 호환을 위해 `get_dart_*_raw()`도 그대로 둔다.

### 2-A. normalize 전용 fetch API 추가

`ports/storage.py`에 신규 메서드 추가 (기존 메서드는 유지):

```python
def iter_dart_financial_statement_for_normalize(
    self,
    bsns_years: list[int],
    reprt_codes: list[str],
    tickers: list[str],          # 청크 단위, None 불허
    rule_account_ids: list[str] | None,  # wildcard rule 있으면 None 전달
    page_size: int = 5000,
) -> Iterator[DartFinancialStatementLine]:
    ...
```

핵심 차이점:

| 항목 | 기존 `get_dart_*_raw` | 신규 `iter_*_for_normalize` |
| --- | --- | --- |
| SELECT 컬럼 | `*` (raw_payload 포함) | normalize에서 실제 사용하는 컬럼만 |
| `raw_payload` | 적재 | **미적재** |
| ticker 필터 | optional | **필수** (청크 단위) |
| 반환 형태 | `list[...]` | `Iterator[...]` (generator) |
| fetch 전략 | `fetchall()` | server-side cursor + `fetchmany(page_size)` |

### 2-B. server-side cursor 사용

psycopg2 named cursor + `itersize` 패턴:

```python
with get_connection(self._dsn) as conn:
    with conn.cursor(name="normalize_financial", cursor_factory=DictCursor) as cur:
        cur.itersize = page_size
        cur.execute(sql, params)
        for row in cur:        # internally fetchmany(itersize)
            yield DartFinancialStatementLine(...)
```

주의:
- named cursor는 트랜잭션 내에서만 유효. 청크 루프 동안 connection을 유지해야 한다.
- autocommit 환경에서는 명시 `BEGIN` 필요. `psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED` 기본 사용.
- 호출자(service)는 `with conn:` 블록 안에서 iterator를 소비해야 한다 → repository 메서드를 **context manager**로 노출하거나 service가 connection lifecycle을 알게 하지 않도록 service 면에서는 `list[...]` 받는 wrapper를 우선 제공하고, 내부에서 generator 소비를 한 번에 끝내는 구조로 시작한다.

### 2-C. mapping rule 기반 SQL 선필터 (조건부)

review 지적: `_matches_financial`/`_matches_xbrl`는 `rule.account_id`가 비어 있으면 wildcard로 동작. seed에 wildcard rule이 하나라도 있으면 SQL `WHERE account_id = ANY(%s)`은 데이터를 누락시킨다.

따라서:

```python
def _build_account_filter(rules: list[MetricMappingRule]) -> list[str] | None:
    """모든 rule이 account_id를 명시할 때만 IN 필터로 좁힌다."""
    if any(not r.account_id for r in rules):
        return None
    return sorted({r.account_id for r in rules})
```

`None`이면 SQL에서 해당 조건을 생략. 현재 seed 기준 `_default_metric_mapping_rules()`을 점검해 wildcard 유무를 PR 본문에 명시한다.

share-count 필터는 **`row.se` 컬럼**으로 좁힌다 (rule field는 `row_name`이지만 실제 비교 대상 컬럼은 `se`). 컬럼명을 SQL에 정확히 반영.

```sql
-- share_count
WHERE bsns_year = ANY(%s) AND reprt_code = ANY(%s)
  AND ticker = ANY(%s)
  AND se = ANY(%s)        -- rule.row_name 목록을 그대로 전달
```

shareholder_return은 필드 다양성이 커서 **선필터 생략**, 그대로 가져온다 (XBRL/financial이 메모리 상위라는 PR 0 측정 결과를 인용).

### 2-D. ticker batch helper

```python
def chunked(seq: Iterable[T], size: int) -> Iterator[list[T]]:
    buf: list[T] = []
    for item in seq:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf
```

`tickers=None` (전체) 케이스는 service에서 `corp_by_ticker.keys()`를 sort해 chunk한다.

### 산출물

- repository 신규 메서드 4개 + 단위 테스트(스키마/누락 컬럼 검증)
- 기존 메서드 호출자는 변동 없음

---

## PR 3 — Service 청크 루프 적용

여기서 비로소 service가 **청크 단위로 동작**하기 시작한다.

### 3-A. 청크 차원: ticker만 (단일 차원)

unique key에 ticker가 포함된 이상, **year/report는 한 번에 넘겨도 의미 보존**된다. round trip을 줄이기 위해 ticker만 batch 차원으로 둔다.

```python
def normalize_stock_metrics(
    storage: Storage,
    bsns_years: list[int],
    reprt_codes: list[str],
    tickers: list[str] | None = None,
    *,
    batch_size: int | None = None,
) -> MetricNormalizationResult:
    batch_size = batch_size or _resolve_batch_size()  # env fallback

    # 1. catalog/rule seed (한 번)
    # 2. corp_by_ticker (한 번, 전체)
    # 3. rules_by_source (한 번)

    target_tickers = sorted(corp_by_ticker.keys()) if tickers is None else tickers
    total_written = 0
    for ticker_batch in chunked(target_tickers, batch_size):
        chunk_facts = _normalize_chunk(
            storage=storage,
            bsns_years=bsns_years,
            reprt_codes=reprt_codes,
            ticker_batch=ticker_batch,
            corp_by_ticker=corp_by_ticker,
            rules_by_source=rules_by_source,
            unit_by_metric_code=unit_by_metric_code,
            catalog=catalog,
        )
        upsert_result = storage.upsert_stock_metric_facts(chunk_facts)
        total_written += upsert_result.updated
        _log_chunk_metrics(ticker_batch, upsert_result, ...)
    result.facts_written = total_written
```

### 3-B. `_normalize_chunk` 구조

```python
def _normalize_chunk(...) -> list[StockMetricFact]:
    rule_accounts_fin = _build_account_filter(rules_by_source.get("dart_financial_statement_raw", []))
    rule_accounts_xbrl = _build_account_filter(rules_by_source.get("dart_xbrl_fact_raw", []))
    rule_se_share = _build_se_filter(rules_by_source.get("dart_share_count_raw", []))

    builders = [
        CandidateBuilder(
            source_table="dart_financial_statement_raw",
            rows=storage.iter_dart_financial_statement_for_normalize(
                bsns_years, reprt_codes, ticker_batch, rule_accounts_fin
            ),
            ...  # PR 1에서 분리한 키 builder들 재사용
        ),
        ...  # share_count, shareholder_return, xbrl
    ]
    selected = _collect_candidates(builders, rules_by_source, corp_by_ticker, unit_by_metric_code)
    return [fact for _, _, _, fact in selected.values()]
```

`selected` dict는 **한 청크 안에서만** 살아 있다. 산식:

> 청크 dict 크기 ≈ batch_size × |metric_code| × |years| × |reports|
> 예: 100 × 50 × 1 × 4 ≈ 20k entries → 무시 가능

### 3-C. batch_size 결정과 env

review 지적대로 첫 도입부터 env로 분리:

```python
def _resolve_batch_size() -> int:
    raw = os.environ.get("SDC_METRICS_NORMALIZE_BATCH_SIZE")
    if not raw:
        return 100
    value = int(raw)
    if value <= 0:
        raise ValueError(...)
    return value
```

PR 0 측정 결과로 100/200/50 중 정한다. CLI flag (`--batch-size`)는 PR 4에서.

### 3-D. 청크별 로깅

운영 관찰을 위해 청크마다 다음을 한 줄로 남긴다:

```
chunk start=00100 end=00200 years=[2024] reports=[11011] candidates=18234 facts=8421 elapsed=2.31s rss_mb=412
```

`logger.info` 레벨, 구조화된 dict로 남기면 좋다.

### 산출물

- `tests/unit/test_metric_normalization.py`에 청크 동등성 테스트 추가:
  - 동일 fixture를 `batch_size=1`, `2`, `len(tickers)`로 normalize → `selected_facts` 결과가 set 비교로 동일
  - tiebreaker가 결정성을 보장하는지 명시 검증
- 기존 단위 테스트 모두 통과

---

## PR 4 — 운영 옵션 노출 (CLI flag)

env는 PR 3에서 이미 동작. 사용자 편의를 위한 flag 추가.

```python
metrics_normalize.add_argument(
    "--batch-size",
    type=int,
    default=None,
    help="ticker batch size (env: SDC_METRICS_NORMALIZE_BATCH_SIZE, default 100).",
)
```

`_handle_metrics_normalize`에서 `batch_size=args.batch_size`를 그대로 전달, `None`이면 env fallback.

---

## 검증 항목

### 단위 테스트

```bash
uv run pytest tests/unit/test_metric_normalization.py
```

- 기존 선택 우선순위 테스트 그대로 통과
- 신규: 동일 fixture를 batch_size `1`, `2`, full로 돌렸을 때 결과 동일
- 신규: 동률 후보가 있는 fixture에서도 결정적으로 같은 후보 선택

### Integration smoke

prod 스냅샷의 1년치(예: 2024 + 11011)를 dev DB로 복제한 뒤:

```bash
SDC_METRICS_NORMALIZE_BATCH_SIZE=100 uv run krx-collector metrics normalize \
  --bsns-years 2024 --reprt-codes 11011
```

- 결과 row count가 PR 0 baseline과 일치
- 무작위 ticker 20개에 대해 (`metric_code`, value)가 baseline과 동일
- 청크별 로그에 RSS가 batch 안에서 안정적으로 유지되는지 확인

### 운영 관찰

배포 후 1주:
- 전 백필 normalize의 peak RSS가 baseline 대비 줄었는가
- 실행 시간이 과도하게 늘지 않았는가 (batch_size가 너무 작으면 round trip 증가)
- 청크 실패 시 이미 upsert된 청크는 보존되는가 (재실행 idempotency)

---

## 후속 옵션 (이 계획의 범위 밖)

1. **Hybrid SQL set-based** — 1순위 안정화 후 financial/xbrl만 SQL set-based로 전환하고 share-count/shareholder-return은 Python에 남기는 부분 적용. 회귀 위험을 격리하면서 메모리/시간 모두 추가 절감 가능.
2. **wildcard rule 추가 시 대응** — 새 mapping rule이 wildcard로 들어올 때 `_build_account_filter` 로직이 자동으로 `None`을 반환하므로 silently 메모리 절감이 약해진다. lint성 알람 (`logger.warning`) 한 줄 권장.
3. **rule 변경 감지 캐시** — `replace_metric_mapping_rules`가 변경 없을 때 catalog/rule upsert를 skip해 normalize 사이드이펙트를 줄이는 것은 직교한 개선. 현재 계획에는 포함하지 않는다.

---

## 작업 순서 요약

| PR | 목적 | 외부 동작 변화 | blocking |
| --- | --- | --- | --- |
| 0 | 메모리 측정 | 없음 (관측만) | — |
| 1 | refactor + tiebreaker | 없음 | PR 0 권장 |
| 2 | repository skinny iter | 없음 (신규 API만) | PR 1 |
| 3 | service 청크 루프 + env | **있음** (peak RSS 감소) | PR 1, 2 |
| 4 | CLI flag | 없음 (편의) | PR 3 |

순차 의존성이 명확하므로 PR을 병합 순서대로 묶으면 된다.
