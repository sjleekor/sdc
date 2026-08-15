# 01. 공통 구현 규약 — 새 원천을 붙일 때 매번 하는 것

- 작성일: 2026-08-15
- 상위 문서: [`00_new_collection_plan.md`](00_new_collection_plan.md)
- 이 문서는 **N1~N9 전부에 공통으로 적용되는 규약**이다. 각 작업 패키지 문서는 이걸 참조하고
  자기 고유의 것만 적는다.

---

## 1. 새 테이블 하나는 6곳에 등록해야 한다

**이게 이 문서에서 가장 중요한 부분이다.** 실제로 `08_phase_b_implementation_log.md` §4.1.2에서
`dart_filing_receipt_raw`·`dart_capital_change_raw`가 exporter 설정에 빠져 있어 raw export가 한 번
막혔다. 같은 실수를 반복하지 않는다.

| # | 파일 | 넣을 것 | 빠뜨리면 |
|---|---|---|---|
| 1 | `sql/postgres_ddl.sql` | `CREATE TABLE` + PK + **sync cursor 인덱스** | `db init`이 테이블을 안 만든다 |
| 2 | `src/krx_collector/infra/db_postgres/remote_sync.py` | `PIPELINE_FULL_REFRESH_TABLE_NAMES` + `SYNC_TABLE_SPECS` (+ FK 있으면 `SYNC_TABLE_DEPENDENCIES`) | `db sync-remote`가 prod → local 미러링을 건너뛴다 |
| 3 | **`src/krx_collector/service/profiling/catalog.py`** | 프로파일 spec | **유닛 테스트가 깨진다**(아래) |
| 4 | `tools/raw-parquet-exporter/config/export_tables.toml` | `[[tables]]` 블록 | 익스포터가 테이블을 모른다 |
| 5 | `bin/raw-parquet-export-all.sh` | `raw_id_tables` / `date_month_tables` / `non_resumable_tables` 중 하나 | 배치 스크립트가 호출하지 않는다 |
| 6 | `research/etl/config.py` | `RAW_TABLES` 튜플 | 마트에서 `table_glob()`이 `KeyError` |

**3번은 선택이 아니라 강제다.** `tests/unit/test_profiling.py`의
`test_catalog_covers_all_pipeline_tables`가 `PIPELINE_FULL_REFRESH_TABLE_NAMES`의 모든 테이블이
profile catalog에 있어야 한다고 단언한다. 2번만 하고 3번을 빠뜨리면 **테스트가 즉시 실패한다.**
(역설적으로 이건 안전장치다 — 조용히 누락되지 않는다.)

문서도 같이 고친다: **`docs/database.md`**에 테이블 설명을 추가한다. 테스트가 강제하지는 않지만
이 저장소의 스키마 문서 관례다.

**컬럼만 추가하는 경우에도 2번을 반드시 본다.** `SYNC_TABLE_SPECS`의 SELECT/INSERT 컬럼 목록은
명시적이라, 추가하지 않으면 **미러링에서 조용히 누락된다.** 3번의 테스트 같은 안전장치가 없다.

### 1.1 sync cursor 인덱스

`db sync-remote`의 증분 동기화는 커서 컬럼 인덱스에 의존한다. 기존 패턴을 그대로 따른다.

```sql
CREATE INDEX IF NOT EXISTS ix_<table>_sync_cursor
    ON <table> (fetched_at, <pk columns...>);
```

### 1.2 익스포터 전략 고르기

`export_tables.toml`의 `extract_strategy`가 셋이다. 테이블 모양에 맞는 걸 고른다.

| 전략 | 조건 | 파티션 예 | 기존 예 |
|---|---|---|---|
| `date_month` | 일자 컬럼이 있는 시계열 | `["year(trade_date)", "month(trade_date)"]` | `daily_ohlcv`, `krx_security_flow_raw` |
| `raw_id_range` | `BIGSERIAL raw_id`가 있는 대용량 raw | `["bsns_year", "reprt_code"]` | `dart_*_raw` 다수 |
| (기본/비재개) | 작은 참조·마스터 테이블 | 없음 | `stock_master`, `dart_corp_master` |

`raw_id_range`를 쓰려면 **DDL에 `raw_id BIGSERIAL PRIMARY KEY`가 있어야 한다.** JSONB 컬럼은
`jsonb_columns`에 반드시 적는다 — 안 적으면 문자열로 나간다.

`bin/raw-parquet-export-all.sh`의 배열 선택은 위 전략과 짝이 맞아야 한다
(`raw_id_tables` ↔ `raw_id_range`, `date_month_tables` ↔ `date_month`, 나머지 ↔ `non_resumable_tables`).

---

## 2. 코드 구조 — ports & adapters 규율

**깨면 안 되는 불변식 하나: `domain/`과 `service/`는 `adapters/`·`infra/`를 import하지 않는다.**
결선은 `cli/app.py` 합성 루트에서만 한다.

새 원천 하나를 붙일 때 손대는 곳은 정해져 있다.

```
domain/models.py          ← 레코드 dataclass + <X>SyncResult dataclass
domain/enums.py           ← RunType에 항목 추가 (필요 시 Source에도)
ports/<concern>.py        ← Protocol 하나 (runtime_checkable)
ports/storage.py          ← upsert_<table>() / get_existing_<...>() 시그니처
adapters/<source>_<x>/    ← provider.py (+ client.py) — 실제 호출·파싱
infra/db_postgres/repositories.py  ← ON CONFLICT DO UPDATE upsert 구현
service/sync_<x>.py       ← 유스케이스 오케스트레이터 (포트만 인자로 받는다)
cli/app.py                ← add_parser + set_defaults(handler=_handle_<x>) + 결선
```

### 2.1 서비스 골격

`service/sync_dart_filings.py`가 가장 최근이자 가장 깔끔한 참조 구현이다. 골격은 항상 같다.

```python
def sync_<x>(provider: <X>Provider, storage: Storage, ..., force: bool = False) -> <X>SyncResult:
    run = IngestionRun(run_type=RunType.<X>_SYNC, started_at=now_kst(),
                       status=RunStatus.RUNNING, params={...})
    storage.record_run(run)
    result = <X>SyncResult()
    try:
        targets = storage.get_<targets>(...)
        existing = set() if force else storage.get_existing_<x>_keys(...)   # skip-if-present
        for t in targets:
            if key(t) in existing:
                result.requests_skipped += 1
                continue
            result.requests_attempted += 1
            fetch = call_with_retry(lambda: provider.fetch_<x>(t), request_label=..., ...)
            if fetch.error:
                result.errors[label] = fetch.error          # 개별 실패는 모아만 둔다
            elif fetch.records:
                up = storage.upsert_<table>(fetch.records)
                result.rows_upserted += up.updated
            sleep_with_jitter(rate_limit_seconds)
        complete_run(storage, run, counts=build_run_counts(...), errors=result.errors,
                     partial_subject="<x> requests")        # 부분 실패 → status=partial
        return result
    except Exception as exc:
        fail_run(storage, run, exc)
        result.errors["pipeline"] = str(exc)
        return result
```

**핵심은 세 가지다.**

1. **개별 요청 실패로 죽지 않는다.** `result.errors`에 모으고 `complete_run`이 `partial`로 마감한다.
2. **skip-if-present 키를 반드시 정의한다.** 재실행이 idempotent해야 한다.
3. **`force` 플래그로 skip을 끌 수 있어야 한다.**

### 2.2 OpenDART 원천이면 추가로

`opendart_common`의 다중키 실행기를 쓴다. `sync_dart_filings.py`의 `_get_executor` 패턴 그대로다.

- `run.params["opendart_key_count"] = executor.configured_key_count`
- `is_opendart_daily_limit_exhausted(fetch_result)` → `OpenDartKeyExhaustedError` 발생 →
  **CLI 종료 코드 75**로 나가고 다음 실행에서 저장된 raw를 건너뛰며 재개
- `should_retry_opendart_result`를 `call_with_retry`에 넘긴다
- `complete_run`의 counts에 `**executor.snapshot_metrics()`를 합친다 (키 회전·레이트리밋·상태코드)

### 2.3 pykrx / KRX 원천이면 추가로

`HumanThrottlePolicy`(`util/pipeline.py`)를 쓴다. `prices backfill`의 `--rate-limit-seconds` /
`--long-rest-interval` / `--long-rest-seconds` 인자 3종을 같은 이름으로 노출한다.

### 2.4 "행이 하나라도 있으면 완료"로 skip하면 안 된다

**흔한 함정이다.** `(trade_date, market)`에 행이 있으면 그 슬라이스를 건너뛰는 규칙은,
응답 일부만 저장된 뒤 중단되면 **그 슬라이스를 영구히 불완전하게 남긴다.** 다시는 안 채워진다.

**최소 요건 두 개.**

1. **슬라이스 단위 원자성.** 한 API 응답(= 한 날짜 · 한 시장, 또는 한 corp × 한 연도)의 전체
   행을 **하나의 트랜잭션**으로 upsert한다. 배치를 나눠 쓰면 그 사이에 죽었을 때 부분 저장이
   남는다. `psycopg2`의 connection 컨텍스트가 종료 시 커밋하므로, **upsert 호출 한 번 = 한
   슬라이스**를 지킨다.
2. **기대 행 수 대조.** 응답 행 수와 저장 행 수를 비교하고, 다르면 슬라이스를 성공으로
   기록하지 않는다.

**대규모·다중 run 백필(N6처럼 16만 호출)에는 이것만으로 부족하다.** 현재 OpenDART 경로의
완료 상태 추적에는 두 가지 한계가 있다.

- `no_data_request_keys`가 run당 **1,000개로 잘린다** (`sync_dart_financials.py:167`,
  `sync_dart_share_info.py:335`, `sync_dart_xbrl.py:178`)
- negative cache가 **최근 20개 run만** 읽는다 (`dart_target_plan.py:104`)

즉 며칠에 걸친 백필의 완료 상태를 `ingestion_runs.params`로 복원할 수 없다. 그런 작업에는
**지속 ledger 테이블**이 필요하다.

```sql
CREATE TABLE IF NOT EXISTS collection_slice_state (
    source        TEXT NOT NULL,       -- PYKRX | OPENDART | ...
    endpoint      TEXT NOT NULL,       -- market_cap | empSttus | ...
    slice_key     TEXT NOT NULL,       -- '2024-01-02|KOSPI' | '00126380|2020|11011'
    status        TEXT NOT NULL,       -- running | success | no_data | failed
    expected_rows INT,
    actual_rows   INT,
    attempt_count INT NOT NULL DEFAULT 0,
    last_error    TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, endpoint, slice_key)
);
```

**선례가 있다.** `sync_checkpoints`(`sync_name` → `cursor_payload` JSONB)가 같은 발상인데
`remote_sync`의 미러링 커서 전용이고 sync당 1행이라, 수만 개 슬라이스에는 맞지 않는다.

**적용 범위를 구분한다.**

| 작업 | 요건 |
|---|---|
| N1 · N4 · N7 (날짜 슬라이스, 단일 run) | §2.4의 1·2번이면 충분 |
| **N6** (16만 호출, 며칠, 다중 run) | **ledger 필수** |
| N5 | PoC 결과에 따라 (볼륨이 크면 ledger) |

ledger를 도입한다면 **N6 착수 전에 공통 컴포넌트로 먼저 만든다.** 작업 패키지마다 따로
만들면 안 된다.

---

## 3. PoC 먼저 — 전량 백필은 그 다음

**pykrx 계열(N1·N3·N4·N7)은 이번 조사에서 실호출 검증을 못 했다.** 네트워크가 막혀
`Expecting value: line 1 column 1`로 실패했다. 시그니처와 docstring만 확인한 상태다.

그래서 각 작업 패키지의 **1번 PR은 항상 PoC**다.

```
PoC에서 확인할 것
├─ 실제 반환 컬럼명 (docstring과 다를 수 있다)
├─ 결측·0 값 패턴
├─ 과거 구간 응답 여부 (오래된 날짜에서 빈 값이 나오는지)
├─ 호출 1회 소요 시간 → 전량 백필 시간 추정
└─ 스로틀 없이 몇 회까지 견디는지
```

PoC 산출물은 **1개월치 raw 응답 덤프 + 관측 요약**이고, 스크래치패드가 아니라
`docs/dev/20260731_raw_features/02_data_expansion_plan/poc/` 아래에 남긴다.

---

## 4. 테스트

| 층 | 위치 | 규칙 |
|---|---|---|
| 유닛 | `tests/unit/test_<x>.py` | DB 불필요. 파서·매핑·skip 로직·부분실패 경로 |
| 통합 | `tests/integration/test_<x>.py` | DB 없으면 **자기 스킵** |
| 라이브 | `tests/integration/test_<x>_live.py` | `RUN_LIVE_<SRC>_TEST=1` 환경변수 게이트 |

**픽스처 주의.** `00_status.md` §4c에서 B-2 결함 3건을 놓친 원인이 "픽스처가 filing당 컨텍스트
하나뿐이었던 것"이었다. **픽스처는 실제 데이터의 모양(다중 종류·다중 기간·다중 축)을 반영해야
한다.** 단순화한 픽스처는 결함을 통과시킨다.

기준선: `uv run pytest tests/unit -q` 939개 통과(2026-08-12). 새 작업은 이 수를 늘리기만 한다.

```bash
uv run pytest tests/unit -q
uv run ruff check src/ tests/          # E,F,I,W,UP / line-length 100
uv run black src/ tests/
```

---

## 5. 배포와 운영

1. **릴리즈는 `sdc-release` 스킬**로 한다 — 버전 범프 → lint+unit → 커밋 → `vX.Y.Z` 태그 →
   sj2 compose 이미지 태그 갱신. 수동으로 하지 않는다.
2. **prod 래퍼 스크립트**는 `deploy/prod/bin/` 아래에 둔다. 백필은 기존
   `bin/dart-backfill-all-years.sh` 패턴을 따른다.
3. **Cronicle 일회성 백필 이벤트는 끝나면 지운다.** 상시 이벤트로 남겨 두면 혼란이 생긴다
   (과거 `common-backfill-2015` 사례).
4. **KRX 계열은 exit 75 lock 충돌에 주의**한다. 기존 KRX 잡과 시간대가 겹치지 않게 배치한다.

---

## 6. 수집과 검정을 섞지 않는다

수집 자체는 지금 시작해도 된다. **검정은 다르다.**

- 새 원천으로 family를 늘리면 **BH 모집단이 커져 기존 발견의 문턱이 올라간다.**
  현재 `m_ab=113`, discovery 45, 강등 0건이다.
- **새 config로 별도 사전등록**한다. 기존 `config_hash=e55c3046…`를 건드리지 않는다.
- **재실행 비용**: Phase B 5시간 30분, Phase A 4시간 41분(실측). 수집할 때마다 재검정하지 않고
  묶어서 한 번에 돌린다.
- 각 작업 패키지 문서의 **"결과 보기 전에 고정할 것"** 절을 수집 착수 전에 채운다.

---

## 7. 작업 패키지별 완료 기준 (공통 DoD)

각 문서의 고유 조건에 더해 아래를 전부 만족해야 완료다.

- [ ] §1의 등록 **6곳** 전부 반영 (+ `docs/database.md`)
- [ ] `uv run pytest tests/unit/test_profiling.py -q` 통과 (catalog 누락 검출)
- [ ] §2.4 — 슬라이스 단위 원자성 + 행 수 대조. 대규모 백필이면 ledger
- [ ] `db init` → 새 테이블 생성 확인
- [ ] `db sync-remote --full-refresh` → prod → local 미러링 확인
- [ ] `bin/raw-parquet-export-all.sh` → lake에 파티션 생성 확인
- [ ] 마트에서 `LakeConfig.table_glob("<table>")`이 `KeyError` 없이 동작
- [ ] 유닛 테스트 추가, `ruff` / `black` 통과
- [ ] 같은 명령 두 번 실행 시 `requests_skipped`가 늘고 `rows_upserted`가 0 (idempotent)
- [ ] 부분 실패 시 `ingestion_runs.status = partial`로 마감되고 종료 코드가 0
- [ ] 문서: 이 디렉터리에 **수집 결과 관측 요약** 추가 (행 수·기간·커버리지·결측)

---

## 문서 지도

| 파일 | 대상 | 차수 |
|---|---|---|
| [`00_new_collection_plan.md`](00_new_collection_plan.md) | 전체 계획·우선순위 | — |
| **`01_implementation_checklist.md`** | **이 문서 — 공통 규약** | — |
| [`02_w1_daily_market_cap.md`](02_w1_daily_market_cap.md) | N1 일별 시총·거래대금·상장주식수 | 1차 |
| [`03_w1_company_profile.md`](03_w1_company_profile.md) | N2 업종 코드·설립일·결산월 | 1차 |
| [`04_w1_pit_universe.md`](04_w1_pit_universe.md) | N3 PIT 유니버스 | 1차 |
| [`05_w2_industry_index.md`](05_w2_industry_index.md) | N4 업종 지수 구성종목 | 2차 |
| [`06_w2_ownership_disclosure.md`](06_w2_ownership_disclosure.md) | N5 임원·주요주주 / 대량보유 | 2차 |
| [`07_w3_periodic_report_extras.md`](07_w3_periodic_report_extras.md) | N6 직원·임원·최대주주·감사의견 | 3차 |
| [`08_w3_valuation_and_macro.md`](08_w3_valuation_and_macro.md) | N7 KRX 밸류에이션 · N8 고용 | 3차 |
| [`09_w4_filing_text.md`](09_w4_filing_text.md) | N9 공시 원문 텍스트 | 후순위 |
