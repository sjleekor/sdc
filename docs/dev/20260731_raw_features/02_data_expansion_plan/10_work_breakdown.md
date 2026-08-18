# 10. 작업 분해 — 구현·배포 체크리스트

- 작성일: 2026-08-15
- 상위 문서: [`00_new_collection_plan.md`](00_new_collection_plan.md) · 공통 규약: [`01_implementation_checklist.md`](01_implementation_checklist.md)
- **이 문서는 진행 상태를 추적하는 곳이다.** 왜 하는가는 각 작업 패키지 문서에 있고,
  여기에는 **무엇을 언제 하는가**만 둔다.
- 체크박스는 작업이 **끝났고 검증됐을 때** 채운다. 착수만으로 채우지 않는다.

---

## 진행 요약

| 차수 | 대상 | 상태 | 비고 |
|---|---|---|---|
| 0단계 | 착수 전 공통 | **완료** (2026-08-15) | pykrx 실호출 확인됨 |
| 1차 | N1 · N2 · N3 | N2 완료 · **N1·N3 차단 해제** (2026-08-18) | K-4 어댑터가 선행 |
| 2차 | N4 · N5 | **N4 목적 A 종결** (2026-08-18) | 대체안 crosswalk 실패 → 측정 안 하고 한계 명시 |
| 3차 | ledger · N6 · N7 · N8 | **N7 축소** (K-6d) | 6,000 호출 백필 취소 → KIS 횡단면 대조 |
| 후순위 | N9 · DS005 | 착수 안 함 | 조건 미충족 |

> **2026-08-16 — KRX를 쓰는 모든 작업이 멈췄다.**
> 페이싱 문제가 아니라 **이용약관 위반**이다(제10조 제2호: 자동화 수단 수집 금지).
> 느리게 해서 풀리는 종류가 아니므로 **공식 경로로 옮기는 것이 유일한 해결**이다.
> → [`K 묶음`](#k-묶음--krx-접근-경로-전환-신규-축-2026-08-16) ·
> 조사: [`poc/krx_open_api.md`](poc/krx_open_api.md)
>
> 멈춘 것: **N1-8 · N3-5b · N4 · S-2 · V-1b**.
> N7은 **축소돼 KIS로 진행 가능**해졌고(K-6d), `flows`는 **KIS로 교체 완료**다(K-6f).
> 매일 도는 `common krx`·`universe sync`는 **계속 돈다** —
> 하루 수십 요청이고 차단은 백필 6,000요청에서 났다.
> 약관 대상인 건 맞으므로 **한시적**이라는 전제로 둔다(K-6a).
>
> KRX와 무관해 지금 진행 가능한 것은 아래 절에 모았다.
> 최장 사슬은 **`S-1 잔여 → L-1 → N6`**이고, 가장 급한 건 **`flows` MDC 문 닫기**다.

> **2026-08-18 — 공식 경로가 열렸다.** 인증키(2개)와 엔드포인트 16건이 모두 승인됐고
> 실호출 10건이 전부 200이다. **위 문단의 "멈춘 것" 다섯 중 N4를 뺀 넷이 풀린다.**
> N1·N3는 **한 호출로 합쳐지고**, KRX 원주가까지 같이 와서 **K-7 재료가 따라온다.**
> 남은 단계는 **K-4 어댑터 하나**다. 사양은 [`poc/krx_open_api.md`](poc/krx_open_api.md) §4.1c.
>
> 다만 **약관 게이트(K-0c)는 그대로 남는다.** 공식 엔드포인트라는 사실이
> 장기 보관과 파생물 이용 권한을 주지 않는다 — 이번에도 같은 실수를 하지 않는다.

**전제는 해소됐다.** 조사 시점에 pykrx 실호출이 실패했던 것은 네트워크 차단이 아니라
**KRX 인증 없이 `pykrx.stock`을 직접 import한 탓**이었다.
`krx_collector.adapters.pykrx_auth.get_pykrx_stock_module()`을 거치면 정상 동작한다.

**PoC에서 나온 주요 결과** ([`poc/n1_pykrx_market_cap.md`](poc/n1_pykrx_market_cap.md) ·
[`poc/n3_pit_universe.md`](poc/n3_pit_universe.md))

| 결과 | 영향 |
|---|---|
| 응답 5컬럼(종가 포함), 시장 구분 없음, `ALL` = KOSPI+KOSDAQ+**KONEX 129** | 시장별 호출 확정 |
| **휴장일 = 빈 응답이 아니라 0으로 채워진 전종목 행** | `alternative=False` 명시 + 캘린더 선필터 |
| 정상 거래일 종가 0 = **0건**, 0은 거래량·거래대금에만 | 결측 방침 확정 |
| **V1 항등성 6,904행 100% 일치** | V1 완료. N1의 실익은 `listed_shares`·`trading_value` |
| **`get_market_ticker_list` ≡ `get_market_cap_by_ticker` 종목 집합 (차집합 0)** | **일별 유니버스를 N1 행으로 확정** (`04` §3.5) |
| **`daily_ohlcv`는 naver 수정주가 확정** (삼성전자 50:1 분할로 검증) | `02_feature_candidate.md` §2.3 항목 닫힘 |
| `by_date` 폴백에는 **종가가 없다**(4컬럼) | 정상 경로에서 배제 |
| 무휴식 40회 성공, 평균 0.206s → 6,000회 ≈ 21분 | 스로틀 여유 있음. 정책은 유지 |

---

## KRX 경로가 열렸다 (2026-08-18) — 이제 남은 건 K-4 하나다

> **엔드포인트 16건 승인 완료. 실호출 10건 전부 200이다.**
> 사양은 [`poc/krx_open_api.md`](poc/krx_open_api.md) **§4.1c**에 원본으로 있다.
> 인증키는 로컬 `.env` `AUTH_KEYS`에 **2개**(콤마 구분), `settings.krx_openapi_auth_keys`가 읽는다.
>
> **하루 만에 두 번 바뀌었다.** 아침엔 키가 유효한데 401(`Unauthorized API Call`)이었고 —
> 가짜 키의 `Unauthorized Key`와 본문이 달라 **서비스 권한 문제**로 갈라냈다 —
> 신청이 승인되면서 그대로 열렸다.
>
> **확정된 것 중 계획을 바꾸는 셋.**
>
> 1. **N1 · N3 · 원주가가 한 호출에 다 온다.** N1+N3 합쳐지는 건 예상했는데
>    **`TDD_OPNPRC`·`HGPRC`·`LWPRC`·`CLSPRC`(KRX 미수정 원주가)까지 같이 온다.**
>    → **K-7이 별도 수집을 요구하지 않는다.** N1-8을 돌리면 재료가 따라온다
> 2. **휴장일이 `rows=0`이다 — pykrx와 정반대다.** pykrx의 "0으로 채워진 전종목 행"을
>    막으려고 넣은 `alternative=False` 방어가 **이 경로엔 필요 없다**
> 3. **하루 안에 끝난다.** 무휴식 10요청 거부 0, 응답 1.3~1.7초 →
>    **6,000 호출 ≈ 2.5시간**, 한도는 키 2개로 하루 20,000회
>
> **막혀 있던 것이 전부 풀렸다** — K-2·K-4·K-7 · N1-8/N1-9 · N3-5b/N3-6/N3-7 ·
> S-2/S-3/S-4 · V-1b~V-4 · K-5의 pykrx·FDR 두 문.
> **N4만 예외였고, 그것도 같은 날 닫혔다** — 지수 구성종목은 Open API에 없고
> 대체안의 crosswalk가 실패했다(K-6e). **차단이 아니라 종결이다.**

아래 표는 **K-4 구현과 병행 가능한 작업**이다(초판은 "키 없이 가능한 것"이었다).
초판(08-16)의 K-6b·K-6f는 완료됐고, I6은 해결 작업이 아니라 등급 임계값 결정이라 빠졌다.

**K-4와 무관한 가장 긴 사슬은 `S-1 잔여 → L-1 → N6`이다.**
N6-5의 대상 집합이 N3에서 **S-1**으로 바뀌었으므로 **N6은 KRX와 무관하다.**
3차 전체가 이 사슬에 걸려 있다.

| # | 작업 | 왜 지금 되나 | 남은 선행 |
|---|---|---|---|
| 1 | **`flows` 교체 완주** — prod 키 → 전량 1회 → MDC 문 닫기 | KIS만 쓴다 | prod `.env` (**사람**) |
| 2 | **S-1 잔여** (financials·share-info·xbrl) | OpenDART만. **약 1,300 상폐 법인.** `--include-delisted` 구현돼 있어 실행만 남았다 | — |
| 3 | **L-1 ledger** | 순수 코드. 지금의 tombstone이 임시판이다 | — |
| 4 | **N6** (스키마 2 + 어댑터 + 8.4만 호출) | OpenDART만 | S-1 · L-1 |
| 5 | **I7** XBRL fallback 보강 | ~~순수 코드~~ → **definitions + 마트 둘 다 고쳐야 한다** (2026-08-18 재측정). **N7 축소안의 유일한 선행** | 결정 2건 |
| 6 | ~~공공데이터포털로 K-2를 우회~~ → **필요 없어졌다** (08-18 오후). KRX 승인이 났고 이력이 2014-06까지 닿는다. `DATAGO_KEY`는 대조 검증용으로만 남긴다 | — | — |
| 7 | **D-8 lake 갱신 → N2-10 V6** | 로컬 | — |
| 8 | **N2-9** 업종 중립 variant | 로컬, 진단 전용 | — |
| 9 | **N5-7** + 후순위 공시활동 피쳐 | 신규 수집 없음. 같은 원천이라 한 묶음 사전등록 | — |
| 10 | **N8** (ECOS) | KRX와 무관 | N8-2 readiness 창 정렬 |
| 11 | **N7 축소안** 횡단면 대조 | KIS `per`·`pbr`·`eps`·`bps` | I7 · C4/C5 처리 방침 |
| 12 | **O-8/D-5 freshness 알람** | `daily_market_cap` **제외하고 등록하면 된다** | — |
| 13 | 검정 트랙 `0-4` Phase C 인계 판단 | 수집과 독립 | — |
| 14 | **K-0c 약관 게이트** · K-6c KIS 약관 | 문서 판단 | 사람 |

**우선순위 셋.**

1. **`flows` 교체 완주가 먼저다.** K-0b 안 B가 "교체가 끝난 경로는 **즉시** 끈다,
   병행 운영 기간을 두지 않는다"인데 지금 양쪽이 다 돈다. 조건의 시계가 이미 돌고 있다.
   그리고 **K-6f 코드가 아직 커밋도 안 됐다** — 워킹트리에만 있다
2. **K-4가 그 다음이다.** 사양이 실호출로 다 나왔고 **포트 변경이 없다** —
   `MarketCapProvider`·`HistoricalUniverseProvider`가 이미 `(trade_date, market)`
   슬라이스 단위라 어댑터만 갈아끼운다. **뒤에 걸린 게 가장 많다**
3. **`S-1 → L-1 → N6`은 K-4와 병행한다.** OpenDART만 쓰므로 서로 안 막는다

**K-4 이후 순서에 걸린 것.** N1-8 → N1-9 → (N3-5b는 N1-8에 흡수) → S-2 → S-3/S-4.
**V 묶음 전에 K-7을 먼저 본다** — 원주가가 N1-8에 따라오므로
`listed_shares` 차분과 합치면 **V-1b 전량 재수집이 통째로 불필요**해진다.

**N4는 막힌 게 아니라 닫혔다** (2026-08-18 오후). 업종분류 현황 대체안의 분기점 ②
(KSIC↔KRX crosswalk)를 실측으로 판정했고 **실패했다** — 안정적 1:1 대응이 종목의 36%뿐이다.
→ **목적 A는 측정하지 않고 N2의 PIT 약점을 한계로 명시한다.**
앞으로의 변경만이라도 잡으려면 **`induty_code` 버저닝**이 더 낫다(K-6e).

---

## 크리티컬 패스

```
pykrx 실호출 확인 ─┬→ N1 ──┬→ N3 §3.5 판정 (일별 유니버스를 N1 행으로 쓸 수 있는가)
                   │        └→ N5 정규화 분모 (listed_shares)
                   ├→ N3 ───→ N6 대상 집합 (역사적 상장사)
                   ├→ N4
                   └→ N7
N2 ────────────────→ N4 대조 (업종 변경 비율)
ledger ────────────→ N6 (16만 호출 재개)
```

가장 긴 사슬은 **N1 → N3 → N6**이다. N6이 3차인 이유는 볼륨만이 아니라 이 의존성이다.

**2026-08-18 갱신 — 원천이 바뀌면서 사슬이 짧아졌다.**

```
K-4 어댑터 ──→ N1-8 (= N3 스냅샷 + 원주가 동시 확보)
                 ├→ N1-9 검증 V2~V8
                 ├→ S-2 상폐 master 복구 → S-3 → S-4 편향 크기
                 └→ K-7 원주가 전환 → V-1b·V-2·V-3 불필요 판정
S-1 잔여 ────→ L-1 ledger ──→ N6          (OpenDART만. K-4와 병행)
```

**N3가 N1의 선행이 아니라 결과가 됐다.** 한 호출이 둘을 다 준다.
N6은 대상 집합이 N3에서 S-1으로 바뀌었으므로(N6-5) **이제 KRX 사슬 밖이다.**

---

## 0단계 — 착수 전 (공통)

- [x] **0-1** `02_data_expansion_plan/poc/` 디렉터리 생성 — PoC 산출물 자리 (`01` §3)
- [x] **0-2** **pykrx 실호출 가능 여부 확인** — 정상. `pykrx_auth.get_pykrx_stock_module()` 경유 필수
- [x] **0-3** 기준선 확인 — `uv run pytest tests/unit -q` **952 통과** (문서 기재 939에서 증가), 23.5s
- [x] **0-4** Phase C·acceptance gate 인계 상태 확인 (`00_status.md` §5-1b)
      — **미착수.** 산출물 11종은 나와 있고 판단 두 가지가 남았다:
      ① `screen_pass` 7셀 중 temporal placebo를 거친 건 `fin_log_mcap` 3셀뿐 — 인계 범위 결정
      ② Phase C를 열 만한 family가 이번 run에 있는가.
      **수집과 독립이므로 병행한다. 재검정 순서만 인계 이후에 정한다**

---

## 1차 — N1 · N2 · N3

세 패키지는 **서로 다른 API·테이블이라 병행 가능**하다.
단 **N1 PoC와 N3 PoC는 하나로 묶는다** — 같은 날짜의 두 응답을 대조해야 한다(`04` §3.5).

### N1. 일별 시총·거래대금·상장주식수 — `daily_market_cap`

상세: [`02_w1_daily_market_cap.md`](02_w1_daily_market_cap.md) · 약 6,000 호출 · 8–13M행

- [x] **N1-1 PoC** — [`poc/n1_pykrx_market_cap.md`](poc/n1_pykrx_market_cap.md)
  - [x] 5개 컬럼(**종가 포함**)이 전종목으로 온다 → **날짜×시장 루프 채택**
  - [x] 결측 방침 확정 — `source_close == 0` 행 결측, 거래량·거래대금 0은 **진짜 0**
  - [x] `ALL` = KOSPI + KOSDAQ + **KONEX 129** (정확히 일치) → `ALL` 사용하지 않는다
  - [x] **휴장일은 0으로 채워진 전종목 행** → `alternative=False` 명시 + 캘린더 선필터
  - [x] `by_date` 폴백에 **종가 없음** → 정상 경로에서 배제
  - [x] 스로틀 — 무휴식 40회 성공, 평균 0.206s (6,000회 ≈ 21분)
- [x] **N1-2 스키마 + 등록 6곳** (2026-08-15)
  - [x] `sql/postgres_ddl.sql` — `4b) daily_market_cap` + PK + 조회/sync cursor 인덱스
  - [x] `infra/db_postgres/remote_sync.py` — `PIPELINE_FULL_REFRESH_TABLE_NAMES` + `SYNC_TABLE_SPECS`
        (`copy_merge_enabled`, `conflict_update_where_sql`은 `daily_ohlcv`와 같은 모양)
  - [x] `service/profiling/catalog.py` — `DAILY_MARKET_CAP` spec + `_CATALOG` 등록
  - [x] `tools/raw-parquet-exporter/config/export_tables.toml` — `date_month`, P0
  - [x] `bin/raw-parquet-export-all.sh` — `date_month_tables` 배열
  - [x] `research/etl/config.py` — `RAW_TABLES`
  - [x] `docs/database.md` — `4b` 절 + upsert 전략 + 결측 규칙·항등식
  - [x] **개수 단언 테스트 3건 갱신** — `test_research_config`(14→15),
        `test_remote_db_sync`(15→16, copy_merge 집합)
  - [x] **신규 테스트 2건 추가** — spec 모양·커서 인덱스 고정, 등록 3곳 동시 단언
  - [x] 배관 검증: `uv run pytest tests/unit -q` **954 통과** · `ruff check src/ tests/` clean
  - [x] `db init` → 테이블·인덱스 3개 생성 확인 (컬럼 10개, nullable 정확)
  - [x] exporter `plan --tables daily_market_cap` → `strategy=date_month columns=10`,
        `warning: source table is empty` (데이터 없이 통과)
  - [x] `db sync-remote` 미러링 확인 (2026-08-15, D-2 이후) — 오류 0
- [x] **N1-3 도메인 + 포트 + 어댑터** (2026-08-15)
  - [x] `DailyMarketCapRow` · `DailyMarketCapResult` · `MarketCapBackfillResult`
  - [x] `RunType.MARKET_CAP_BACKFILL`
  - [x] `ports/market_cap.py` — 작업 단위가 ticker가 아니라 **`(trade_date, market)` 슬라이스**
  - [x] `adapters/market_cap_pykrx/provider.py`
  - [x] `market`은 **호출 인자에서만** 채운다 — `stock_master` 조인 없음
  - [x] **`alternative=False`를 명시적으로 넘긴다** — `True`면 휴장일에 전 영업일 데이터가
        그 날짜로 저장된다
  - [x] `source_close == 0` 행 드롭, 거래량·거래대금 0은 **진짜 0으로 보존**
- [x] **N1-4 스토리지** (2026-08-15)
  - [x] `upsert_daily_market_cap` — **한 슬라이스 = 한 트랜잭션**
  - [x] `get_market_cap_slice_row_counts` — boolean이 아니라 **행 수**를 준다
  - [x] **`execute_values` rowcount 버그 수정** — `page_size` 분할 시 `cur.rowcount`가
        마지막 페이지만 반환해 1,704행 슬라이스가 704로 잡혔다. 명시적 페이징으로 합산
- [x] **N1-5 서비스 + CLI** (2026-08-15)
  - [x] `service/backfill_market_cap.py` — 거래일 캘린더 선필터, 부분실패 finalizer
  - [x] `cli/app.py` — `prices market-cap-backfill`, 인자 이름을 `prices backfill`과 맞춤
  - [x] `--market ALL`은 KOSPI·KOSDAQ **2회 호출**로 풀린다. pykrx에 `ALL`을 넘기지 않는다
- [x] **N1-6 테스트** (2026-08-15) — 유닛 16 + 라이브 6. 전체 **970 통과**
  - [x] `test_backfill_market_cap.py` 9건 — 거래일만 호출, 슬라이스 원자성,
        **행 수 미달 슬라이스 재수집**, 행 수 불일치 시 미완료 처리, 부분실패 → `partial`
  - [x] `test_market_cap_pykrx_provider.py` 7건 — **PoC 실측 응답 모양을 픽스처로**
        (휴장일 0 채움 · 거래정지 진짜 0 · 컬럼명 변경 감지 · `alternative=False` 고정)
  - [x] `tests/integration/test_market_cap_live.py` 6건 — `RUN_LIVE_PYKRX_TEST=1` 게이트
- [x] **N1 end-to-end 로컬 검증** (2024-01 한 달, 2026-08-15)
  - [x] 44/44 슬라이스 완료, **58,435행**, 오류 0. 휴장일 2024-01-01은 호출 자체가 없음
  - [x] **V1 항등성 위반 0** · `source_close` 0/NULL 0 · 거래대금 0 = 1,847건(거래정지)
  - [x] **idempotent** — 재실행 시 44 skipped / 0 upserted
  - [x] `ingestion_runs`에 `market_cap_backfill` 감사 기록 2건 (`success`)
  - [x] exporter 실데이터 export → `year=2024/month=01` 파티션, 58,435행, 컬럼 10개
- [x] **N1-7 결정 고정 (결과 보기 전)** — `02` §7 확정본. **근본 해결 방향으로 결정**
  - [x] **결정 1** 기존 ID는 **정의 동결**, 새 정의는 **새 ID**(`px_illiq_20d`·`mcap_krx_log`),
        **새 config에는 새 ID만 사전등록**. 근사값을 후보로 남기면 측정 오차를 알파 후보로
        사전등록하는 셈이다. V5는 진단으로만
  - [x] **결정 2** **상장주식수 차분을 정본으로.** DART 문자열 매칭 폐기(정의 동결 후 교차검증만).
        기계적 변경은 `listed_shares × source_close ≈ 불변`으로 분리 — **N1 데이터만으로 닫힌다**
  - [x] **결정 3** 회전율 = **`거래대금 / 시가총액`**. 같은 응답·같은 원천이라 정합성이 구조적
  - [x] **결정 4 (신규)** `daily_ohlcv` corporate action 재수집 —
        [`poc/n1_adjusted_price_vintage.md`](poc/n1_adjusted_price_vintage.md)
- [ ] **N1-8 백필 실행** — 2024~현재 먼저, 검증 후 2014-06-02~2023.
      **원천이 pykrx가 아니라 KRX Open API다**(K-4 선행). 약 6,000 호출 ≈ **2.5시간**,
      하루 한도 안에서 끝난다. **N3-5b 잔여가 여기에 흡수된다** — 같은 호출이다
- [ ] **N1-9 검증 V1~V8** → `poc/n1_validation.md`
  - [x] **V1** `market_cap` vs `source_close × listed_shares` — **PoC에서 6,904행 100% 일치**
  - [ ] V2 `source_close`(KRX 미수정) vs `daily_ohlcv.close`(naver 수정) 비율 — 전 종목
  - [ ] V3 · V4 · V6
  - [ ] V5 `종가×거래량` vs 실제 거래대금 rank 상관 — **낮으면 `px_amihud_20d` 재검정**
  - [ ] V7 `listed_shares` vs DART `issued_shares` — **`fin_log_mcap` 개선폭의 상한**
  - [ ] V8 시장 이전상장 종목 수 = `stock_master` 조인이 만들었을 룩어헤드 크기
- [x] **N1-10 조정주가 확인** (`02` §6) — **PoC에서 확정.** 삼성전자 2018-05-04 50:1 분할에서
      naver 종가 52,140 = KRX 2,607,000 ÷ 50. **`daily_ohlcv`는 수정주가가 맞다.**
      `02_feature_candidate.md` §2.3 미해결 항목이 닫혔다

### N2. 업종 코드·설립일·결산월 — `dart_corp_master` 확장

상세: [`03_w1_company_profile.md`](03_w1_company_profile.md) · 약 3,959 호출 · 단일 실행

- [x] **N2-1 PoC** (158 호출) → [`poc/n2_company_profile.md`](poc/n2_company_profile.md)
  - [x] **`induty_code` 자릿수가 2·3·4·5로 섞인다** (150 표본: 3/52/21/74). 결측 0, 오류 0
  - [x] 2자리 prefix 36그룹, **1종목 그룹 15개** → 병합 규칙이 실제로 필요
  - [x] **`acc_mt != '12'`가 4%** (150 중 6건 → 3,959 환산 약 158 법인)
  - [x] **LG와 KB금융이 똑같이 `64992`** → 금융·지주 override 불가
- [x] **N2-2 ALTER + 등록 4곳** (2026-08-15)
  - [x] `sql/postgres_ddl.sql` — `7b)` 블록, 6개 컬럼 + `induty_code` 인덱스
  - [x] `remote_sync.py` — 신규 6컬럼을 `updated_at` **뒤에** 붙여 커서 인덱스 9 유지
  - [x] `profiling/catalog.py` — `corp_cls`는 `category_cols`, `induty_code`는 `top_n_cols`,
        결측률 추적용 `null_cols` 5개 추가
  - [x] `export_tables.toml` — `jsonb_columns = ["profile_raw"]`
  - [x] `docs/database.md` — 컬럼 설명 + 자릿수·금융/지주·`acc_mt` 한계
- [x] **N2-3 어댑터 + 포트** — `COMPANY_PROFILE_POLICY`, `fetch_company_profile`,
      `CorpProfileProvider`. 다중키 실행기 + exit 75 재개
- [x] **N2-4 서비스 + CLI** — `dart sync-corp-profile`, 대상은 **ticker 매핑된 법인만**
- [x] **N2-5 테스트** — 유닛 18 (프로필 9 + 그룹 9). 픽스처는 **실측 코드**
      (`264`/`5821`/`21100`/`64992`, `acc_mt='03'`)
- [x] **N2-6 결정 3개 고정 (결과 보기 전)** — **PoC 실측에 맞춰 초안을 뒤집었다**
  - [x] 매핑: **2자리 prefix.** 원본 길이가 2~5로 섞이므로 "N자리로 자른다"가 아니라
        **prefix**여야 한다. 길이에 무관하게 KSIC 중분류가 나온다
  - [x] 최소 그룹 크기 **20**, 미달 시 **KSIC 대분류(알파벳)로 병합**. 결측은 병합하지 않는다
  - [x] 금융: **override 불가**(LG=KB금융=64992). 64·65·66을 별도 그룹으로 두되
        **지주회사가 섞인다는 사실을 명시.** 실제 금융 판정은 계정 구조 = `metric_rules` 영역
- [x] **N2-7 로컬 검증** — 40건 시드 → 32건 수집(결측 0), 재실행 32 skip,
      자릿수 3/4/5 혼재 그대로 저장, 그룹 병합 동작 확인
- [x] **N2-7b 전량 실행 완료** (2026-08-15, 이벤트 `emsugdoe907`, 1,301초) —
      요청 3,959 / 저장 3,959 / skip 0 / 무응답 0 / 오류 0.
      **상폐 법인 1,330건 포함.** `--universe-scope historical`이 없었으면 2,629건만 받았다
- [x] **N2-8 `definitions/industry_groups.py`** — 순수 코드, Storage 의존 없음
- [ ] **N2-9 `fin_scan.py` 업종 중립 variant** — 기존 경로 유지, **진단 전용**
- [x] **N2-10 검증 V1~V5 완료** → [`poc/n2_validation.md`](poc/n2_validation.md)
  - [x] **V1 결측률 0.000%** (기준 2% 미만). 자릿수 2/3/4/5 = 44/1,387/481/2,047
  - [x] **V2 기준 미달 → 규칙을 고쳤다.** 섹션으로 접어도 43개 중 11개가 20 미만(최소 2).
        섹션 자체가 얇아서다(A는 상장사 4개). **구성원 2명 z-score는 항상 ±0.707 —
        약한 신호가 아니라 만들어낸 신호다.** 병합을 기준 충족까지 반복(→`OTHER`).
        결과 최소 20/23, 위반 0. **`MIN_GROUP_SIZE=20`은 그대로 — 메커니즘만 고쳤다**
  - [x] **V3 ACTIVE 불일치 0.** 처음 잡힌 31건은 전부 `corp_cls='E'`였고
        **`status='DELISTED'`와 정확히 같은 집합**(양방향 0건). 독립인 두 소스가 일치한다 →
        상폐 diff 검증 수단으로 쓸 수 있고, **`corp_cls`는 현재 상태 필드**임이 재확인됐다
  - [x] **V4 `acc_mt != '12'` 142건**(상장 53건). 시총 비중은 N1-8 이후
  - [x] **V5 결측 0·미래 0.** 1900년 이전 3건은 실제 값(신한은행 1897·동화약품 1897·
        우리은행 1899) — **임계값이 틀렸지 데이터가 틀린 게 아니다.** firm age 사용 가능
  - [ ] V6 업종별 `fin_value_z` 중앙값 분산 — **이 작업의 핵심 산출물.** D-8 이후
  - [x] **PIT 금지선 유지** — 진단 전용, scored backtest·acceptance gate·holdout 금지

### N3. PIT 유니버스 백필 — 기존 스냅샷 테이블

상세: [`04_w1_pit_universe.md`](04_w1_pit_universe.md) · 약 290 호출 · **등록 6곳 해당 없음**

- [x] **N3-1 PoC** (N1 PoC와 묶음) → [`poc/n3_pit_universe.md`](poc/n3_pit_universe.md)
  - [x] 과거 시점 응답 — **2014-06까지 정상**
  - [x] **`get_market_ticker_list` vs `get_market_cap_by_ticker` 차집합 = 네 시점 모두 0**
  - [x] `market='ALL'`은 시장 구분을 주지 않는다 → 시장별 호출
  - [x] 우선주·리츠·스팩 혼입 여부 → 필터 정책 — **재료 확보** (2026-08-18).
        `stk_isu_base_info`의 `SECUGRP_NM`(주권 여부)·`KIND_STKCERT_TP_NM`(보통주/우선주).
        공공데이터포털 표본에 스팩이 섞여 있던 것도 같이 확인됐다. **필터는 우리가 건다**
  - [ ] 종목명 조회 비용 → 이름 채우기 방식 결정 (N3-3 구현 시)
- [x] **N3-2 enum + 스토리지 메서드** (2026-08-15)
  - [x] `Source.PYKRX_BACKFILL` · `RunType.UNIVERSE_SNAPSHOT_BACKFILL`
  - [x] `insert_stock_master_snapshot_only` — `upsert_stock_master`의 1·2단계만 하고
        **3단계(`stock_master` upsert)는 하지 않는다**
  - [x] `get_existing_snapshot_dates(source)` — skip 키, source로 스코프
- [x] **N3-2b 어댑터** — `PykrxHistoricalUniverseProvider` (2026-08-15)
  - [x] **종목명 조회 비용 문제를 근본 해결.** `get_market_ticker_name`은 개당 0.354s
        (실측) → 145개월 × 2시장 × 2,700종목 = **약 38시간**.
        `get_market_price_change_by_ticker(d, d, market)`가 **전종목 종목명을 1회 호출**로 준다
  - [x] 네 시점(2014·2016·2020·2024) 종목 집합 완전 일치, 빈 이름 0건 확인
  - [x] 이름 조회 실패 시 ticker로 폴백 — 스냅샷을 잃지 않는다
- [x] **N3-3 서비스 + CLI** (2026-08-15) — `universe backfill-snapshots`,
      월말 **거래일** 산출(달력 말일이 휴장이면 그 달 마지막 세션)
- [x] **N3-4 테스트** — 유닛 17 + 통합 3
  - [x] `FakeStorage.upsert_stock_master`가 **호출되면 즉시 실패**하도록 단언
  - [x] **통합 테스트로 SQL 레벨 검증** — `stock_master` 행을 심고 그와 모순되는 과거
        스냅샷을 써도 master 행이 그대로인가. 이게 실제 위험 지점이다
  - [x] `sync_universe`는 `stock_master`와만 diff한다는 것을 확인 —
        `stock_master`를 안 건드리면 오염이 원천 차단된다
  - [x] 빈 유니버스는 **에러로 처리** (시장 전체 상폐로 보이면 안 된다)
- [x] **N3-5 로컬 검증** (2016-01~06, 2026-08-15)
  - [x] 스냅샷 6개 / items 12,293행, 종목명 정상, 폴백 0건
  - [x] **`stock_master` 행 수 0 유지** — 백필이 전혀 건드리지 않았다
  - [x] idempotent — 재실행 6 skipped / 0 written
- [ ] **N3-5b 전량 실행** — 2014-06 ~ 현재 월말 (약 145회). **prod 배포 후**
  - [x] **1차 실행 (2026-08-15 23:53, 127초): 23/146 저장 후 서킷 브레이커 정지.**
        2014-06-30 ~ 2016-04-29. KOSPI 885~907 · KOSDAQ 1,010→1,165으로
        월별 추세가 자연스럽다 — **데이터 품질 문제가 아니라 실행이 끊긴 것**
  - [x] 정지 원인은 **pykrx의 KRX 세션 로그인이 non-JSON을 받는 것**
        (`build_krx_session` → `login_krx` → `JSONDecodeError`). 23:54경부터 시작됐고
        직전까지(23:22~23:50) 같은 소스로 prices·flows·common이 모두 성공했다.
        같은 자격증명을 쓰는 `common-sync-krx`도 23:47에 성공 → **자격증명 문제 아님**
  - [x] **서킷 브레이커가 정확히 의도대로 작동했다** — 5연속 실패에서 정지.
        없었으면 나머지 118개 날짜를 거부하는 서버에 계속 던졌다
  - [x] **2차 실행 (00:11): 14개 추가 → 38개** (2016-06 ~ 2017-07). 같은 방식으로 정지.
        **`_exit_if_run_aborted`가 적용돼 이번엔 exit 1**로 끝났다 (O-12)
  - [x] **3차 실행 (00:19, 연 단위 청크): 0개.** 13개 연도 전부 실패
  - [x] **최종 원인 규명 — 두 가지 실패 방식이 겹쳐 있었다**
    - [x] ① **세션이 실행 중에 죽는다.** pykrx는 자기 시계(1시간)로만 재로그인하고
          서버가 먼저 끊은 건 모른다. 1차 0.5s에 23개(127초) / 2차 1.5s에 14개(253초) —
          **느린 쪽이 더 적게 성공했다.** 페이스가 아니라 프로세스 수명이다
    - [x] ② **로그인 엔드포인트 자체가 죽는다.** `webio.py`가 **import 시점에** 로그인해서,
          로그인이 HTML을 받으면 `import pykrx`가 예외를 던진다. 호출 단위 재시도로는
          닿을 수 없다 — 호출이 없고 라이브러리가 안 올라온다
    - [x] 23:22~23:50 정상 → **23:54부터 실패** → 00:11 프로브 1건 통과 → 00:19 이후 전면 실패.
          같은 자격증명을 쓰지만 **별도로 인증하는 KRX MDC 수집기는 계속 정상**이었다
          → 자격증명 문제가 아니라 **자정 무렵 상류 조건**이다
  - [x] **v0.9.7 `call_with_session_retry`** — ①에 대한 수정. 시계가 아니라 응답을 본다.
        `dataframe_empty_handler`가 `JSONDecodeError`를 삼키고 빈 프레임을 주므로
        **연속 빈 응답**으로 판정한다. 죽은 세션은 전부를 비우고 없는 데이터는 흩어져 있다.
        재로그인해도 여전히 비면 문턱을 4배로 올린다(상폐 가격 백필이 로그인 비용을 반복해
        물지 않도록). pykrx 프로바이더 3개 전부에 적용
  - [x] **v0.9.9 `KrxLoginUnavailableError`** — ②에 대한 수정. 고칠 수 있는 게 아니라
        **읽을 수 있게** 만드는 것이다. 날것으로는 대상 날짜마다 40줄 트레이스백이
        `Expecting value: line 13 column 1`로 끝나며 원인을 아무 데서도 말하지 않았다
  - [x] **4차 실행 (2026-08-16 09:06, v0.9.10 새 페이싱): 40 → 60.**
        2014-01 ~ 2018-12 완주. 그리고 **약 5분 / 약 95요청 만에 다시 차단됐다**
    - [x] 09:07~09:11 성공(데이터 80요청 + 로그인 15요청, **0.32 req/s**) →
          09:12부터 로그인 non-JSON. 어젯밤 0.9 req/s보다 3배 느린 페이스인데도 걸렸다
    - [x] 1차 차단은 약 9시간 만에 풀렸는데(23:54 → 08:46 이전),
          **재차단은 5분 만에 왔다.** sj2 IP가 훨씬 짧은 고삐에 있다는 뜻이다
    - [x] 로그인 실패 쿨다운은 설계대로 작동 — 청크당 로그인 시도 1회로 억제됐다
  - [x] **판단 완료 (2026-08-16): 스크래핑으로는 재개하지 않는다.**
        차단 사유가 페이싱이 아니라 **약관 위반**이다(K-0). 현재 **60/152에서 정지**,
        2014-01 ~ 2018-12 확보. **나머지는 Open API 경로(K-4)로 채운다** —
        그때는 N1과 같은 호출이라 N3 단독 실행 자체가 없어진다
  - [x] **경로 확정 (2026-08-18)** — `sto/stk_bydd_trd`가 `basDd=20140602`에
        KOSPI 907 · KOSDAQ 1,009행을 준다. **1차 실행 실측과 맞는 수다.**
        → **N3-5b는 독립 작업으로 남지 않고 N1-8에 흡수된다**
- [ ] **N3-6 상폐 종목 데이터 실태 조사** — **이게 이 패키지의 진짜 결론**
  - [ ] 연도별 상폐 추정 건수 vs KRX 공표치
  - [ ] 상폐 종목 중 `daily_ohlcv` 보유 비율 — **낮으면 편향이 남는다**
  - [ ] 상폐 직전 60거래일 데이터 보유율
  - [ ] 후속 판단 기록: 상폐 종목 가격 백필이 필요한가
- [ ] **N3-7 마트 PIT 유니버스 variant** — 기존 경로 유지.
      **PoC 판정에 따라 `04` §6 안 1(일별, `daily_market_cap` 행 기준)로 간다**
- [x] **N3-8 §3.5 판정 기록** — **일별 유니버스를 N1 행으로 쓸 수 있다.**
      네 시점 차집합 0. N3는 감사·교차검증 역할로 축소 확정

---

## 배포 절차 (각 차수 끝에 한 번씩)

차수마다 반복한다. 체크박스는 차수별로 복제해 쓴다.

- [x] **D-0 (신규) main 정합화** — v0.9.0~v0.9.2가 **브랜치에서만 태깅돼 main에 없었다.**
      prod가 main에 없는 코드로 돌던 상태. PR #1로 58커밋 머지
- [x] **D-1 릴리즈 v0.9.3** (2026-08-15) — main에서 태그, GHCR 빌드 성공, 이미지 pull 완료
  - [x] **부수 결함 수정** — 릴리즈는 원격 compose만 갱신하고 저장소 파일은 그대로 둔다.
        `deploy_to_sj2.sh`가 그걸 덮어쓰므로 **문서대로 배포하면 prod가 조용히 롤백된다.**
        실제로 원격 v0.9.3 / 저장소 v0.8.16으로 **네 릴리즈만큼 벌어져 있었다**
- [x] **D-2 prod DDL 적용** — `daily_market_cap`(인덱스 3개) + `dart_corp_master` 컬럼 6개.
      `db sync-remote` 미러링도 통과 → **등록 6곳이 실제 환경에서 끝까지 확인됐다**
- [x] **D-3 `deploy/prod/bin/` 래퍼 3개** — market-cap / backfill-snapshots / corp-profile.
      pykrx 계열은 `krx_marketdata` 락 공유(exit 75 회피), DART는 `opendart`
- [ ] **D-4** Cronicle **일회성 백필 이벤트** 등록 → **끝나면 삭제** (`common-backfill-2015` 사례)
  - [x] `SDC Backfill DART Corp Profile (one-time)` — `emsugdoe907` (N2-7b)
  - [x] `SDC Backfill N3 Universe Snapshots (one-time)` — `emsugmrjp0a` (N3-5b)
  - [ ] V-1b / N1-8 / S-2 이벤트
  - [ ] **검증 끝나면 전부 삭제.** 선례 `emr0r4xgb0h`가 2026-07-04 완료 후 아직 남아 있다
- [ ] **D-5** Cronicle **정기 증분 이벤트** 추가 — N1 일별, N4 월별, N2·N5 주기적 refresh
- [x] **D-6** **KRX 계열 동시 실행 차단** (2026-08-15) — 계획은 "시간대 분리"였으나
      **락 자체가 꺼져 있었다.** `SDC_DAILY_USE_SOURCE_LOCK`이 opt-in이고 prod은 켠 적이
      없다 — `.env`에도, 호스트 env에도, Cronicle 이벤트 스크립트에도 없다.
      `/tmp/sdc-locks`가 아예 존재하지 않았다. 락을 잡는 것처럼 읽히는 래퍼 7개가
      **아무것도 잡고 있지 않았다**
  - [x] 기본값을 `:-0` → `:-1`로. **안전장치는 기본이 ON이고 우회가 명시적이어야 한다**
  - [x] 시간대 분리는 대체재가 아니다 — 창을 넘기는 run(flows 실측 775s, 백필은 구조적으로
        수 시간)이 조용히 동시 실행을 되살린다
  - [x] 호스트 실검증 — flock 백엔드, 두 번째 호출 exit 75, 해제 후 재획득, 명시적 OFF 동작
- [x] **D-7** `docs/operations.md` — cron 표에 신규 잡 3개, `--universe-scope` 표,
      `--refetch`, as-of 검증 설명
- [x] **D-1b 릴리즈 v0.9.4** (2026-08-15) — O-9 정지 조건. GHCR 빌드 성공, sj2 pull 완료,
      배포된 이미지에서 `--max-consecutive-failures` 노출 확인.
      **릴리즈 스크립트가 로컬·원격 compose를 함께 올리도록 수정한 뒤 첫 릴리즈**
- [x] **D-1c 릴리즈 v0.9.5** (2026-08-15) — O-10 CLI 인자 결함 수정. **v0.9.4가
      `prices backfill`과 `prices market-cap-backfill`을 깨진 채로 배포했다** (아래 O-10)
- [ ] **D-8** 로컬 반영 — `db sync-remote --full-refresh` → `bin/raw-parquet-export-all.sh`
      → `bin/parquet-compute-all.sh`

---

## 2차 — N4 · N5

### N4. 업종 지수 구성종목 — `krx_index_constituent`

상세: [`05_w2_industry_index.md`](05_w2_industry_index.md) · 약 3,600 호출 · 선행: N2

> **2026-08-16 재정의 판정** (K-6d) → [`poc/n7_n4_alternatives.md`](poc/n7_n4_alternatives.md) §2
>
> 지수 구성종목은 Open API에 없고 스크래핑은 약관 위반이다. 조사 결과 **목적 둘 중
> 하나는 이미 충족됐고, 남은 하나는 더 나은 원천이 있다.**
>
> **목적 B(관계 피쳐의 업종 집합)는 N2로 이미 돌아간다** — 3,959 법인 /
> `induty_code` 630종을 `industry_groups.py`가 쓰고 있다. 지수 구성종목이 아니어도 된다.
>
> **목적 A는 `업종분류 현황`(`MDC0201020506`) 전종목 CSV가 더 맞는다.** 화면 다운로드는
> 허용된 경로고, **지수 편입/제외에 섞이는 신규상장·유동성·방법론 개편 잡음이 없다** —
> §4가 스스로 경고한 그 문제다.
>
> → **N4-2~N4-8 취소.** 업종분류 현황 월별 스냅샷으로 대체한다.
> **분기점 하나: 그 화면이 과거 기준일 조회를 받는가**(K-6e).
> 받으면 목적 A가 소급으로 풀리고, 안 받으면 앞으로만 쌓인다.

- [x] **N4-1 PoC** → [`poc/n4_index_constituent.md`](poc/n4_index_constituent.md)
  - [x] **업종 지수 목록 확정** — KOSPI 22 · KOSDAQ 21, **중복 소속 0건**
  - [x] **상위 개념 지수 발견** — 제조(496)⊃하위, 금융(100)⊃증권·보험.
        제외하되 잔여 87종목은 **코드 차집합 그룹 2개**로 메운다
  - [x] `date` 포맷 = **`YYYYMMDD`.** 틀리면 예외가 아니라 **조용히 빈 값**
  - [x] 하한 **2014-05-01** 확인 (명시적 에러 메시지)
  - [x] **커버리지 KOSPI 78.0% · KOSDAQ 66.2%** → 폴백 비율 리포트 필수
- [ ] **N4-2 스키마 + 등록 6곳** — `date_month` 전략
- [ ] **N4-3 어댑터 + 포트 + 서비스 + CLI** — `universe sync-index-constituents`
- [ ] **N4-4 테스트** — 하한 밖 요청을 **아예 만들지 않는지**
- [ ] **N4-5 결정 3개 고정 (결과 보기 전)**
  - [x] 업종 지수 코드 목록 — **PoC §3에 고정**
  - [ ] 업종 변경 판정 4개 조건 + 3% / 10% 임계값 — **운영 규칙임을 명시**
  - [x] 미편입 폴백 = N2 + `industry_source` 노출. **커버리지가 22~34% 비므로
        폴백 비율 리포트를 필수로 둔다**
- [ ] **N4-6 실행** — 2014-06 ~ 현재 월말
- [ ] **N4-7 N2 대조 3종** — 커버리지 / 일치도 / **업종 변경 비율**
- [ ] **N4-8 inactive 산업지수 4종 처리** — 활성화 / 재정의 / 삭제 중 택일

### N5. 임원·주요주주 소유 / 대량보유

상세: [`06_w2_ownership_disclosure.md`](06_w2_ownership_disclosure.md) · **볼륨 PoC 전 미정**

- [x] **N5-1 PoC ★** → [`poc/n5_ownership.md`](poc/n5_ownership.md)
  - [x] **응답은 최근 2년 롤링 윈도.** 삼성전자(1975 상장)도 2024-08-26부터
  - [x] 요청 인자가 `corp_code`뿐 → **연도 루프 우회 불가**
  - [x] **강등이 아니라 경로 교체** — `dart_filing_receipt_raw`에 지분공시가
        **2015-01 ~ 2026-08 전 구간, 139,697건 / 2,607 법인** 이미 있다
  - [x] 커버리지 **98.1%**, 법인당 연 8건, 60거래일 윈도당 1.93건
- [x] **N5-2~N5-5 취소** — 수집 자체를 하지 않는다. 스키마·어댑터·서비스 불필요
- [x] **N5-6 결정** — **수량을 포기하고 빈도로 간다.**
      elestock을 다 받아도 도달점은 `ins_holding_increase`(방향 혼재)이고
      학습 구간이 1년이다. receipt 경로는 **10.5년**
  - [x] 후보: `own_insider_filing_60d` · `own_insider_filing_burst` ·
        `own_major_filing_60d` · `own_amendment_ratio_1y`. 윈도 {60,120}, 부호 미고정
  - [x] 노출 시점 = `rcept_dt`의 **다음 거래일**
- [ ] **N5-7 (신규) receipt 기반 지분공시 피쳐 구현** — 신규 수집 없음.
      **`09` §3의 공시 활동 피쳐와 같은 원천이므로 한 묶음으로 사전등록**
- [ ] **N5-8 (보류) `elestock` 정기 수집** — 지금 만들지 않는다.
      **다만 지연 1개월 = 과거 1개월 영구 손실**(2년 윈도가 흘러간다)
      (`fin_sue` coverage 0.0000 사례를 반복하지 않는다)

---

## 3차 — ledger · N6 · N7 · N8

- [x] **L-1 `collection_slice_state` ledger 공통 컴포넌트 — 완료** (2026-08-18, `01` §2.4).
      **N6 착수 전에 만든다**는 조건을 지켰다. 패키지마다 따로 만들지 않는다
  - [x] 현재 한계 확인: `no_data_request_keys`는 run당 절단되고 negative cache는 최근
        20 run만 읽는다 → **며칠짜리 백필이 자기 진행 상태를 복원하지 못한다.**
        N6이 정확히 그 경우다(8.4만 호출 · 약 3일)
  - [x] **`(source, endpoint, slice_key)` PK.** `endpoint`가 키에 있어야 한 원천을
        쓰는 두 수집기가 slice_key에서 충돌하지 않는다
  - [x] **`expected_rows` vs `actual_rows` 대조.** "행이 있으면 완료"가 아니다 —
        응답이 반만 저장된 채 죽으면 그 슬라이스는 영구히 skip된다.
        불일치는 `failed`로 기록해 다음 run이 다시 받는다
  - [x] **`running`은 완료가 아니다** — 죽은 프로세스가 남긴 행이 영구 구멍이 되지 않는다
  - [x] **`no_data`만 TTL로 만료된다.** 거래정지 종목은 살아나고 정정 공시도 나온다.
        `success`는 만료되지 않는다
  - [x] `attempt_count`는 **SQL에서 증가**시킨다 — 같은 endpoint를 도는 두 run이
        서로의 시도 횟수를 덮어쓰면 "계속 실패하는 슬라이스"를 못 본다
  - [x] `SliceStatus`를 `RunStatus`와 분리 — run은 `partial`일 수 있지만 슬라이스는 아니다
  - [x] `--force`도 ledger를 쓴다. 안 쓰면 escape hatch가 영구 우회가 된다
  - [x] 테스트 20건 + 로컬 DB 실검증(재실패 시 `attempt_count=2`, plan 4분류). 전체 **1,188 통과**

### N6. 직원·임원·최대주주·감사의견

상세: [`07_w3_periodic_report_extras.md`](07_w3_periodic_report_extras.md) · 약 16만 호출 · 3일 · 선행: N3, L-1

- [x] **N6-1 PoC** → [`poc/n6_periodic_extras.md`](poc/n6_periodic_extras.md)
  - [x] **변동 폭 확인 — 패키지 살아 있다.** 직원 수 YoY 표준편차 **13.7%**,
        |변화|>5%가 **50%**. 범위 −37.7% ~ +61.3%
  - [x] 행 순서 **안정** → `row_ordinal`. payload hash 불필요
  - [x] **`hyslrChgSttus`에 `change_on`이 있다** → 분기 수집 예외 불필요(+97,200 절약).
        게다가 **과거 변동을 누적으로 준다**(2023 요청에 2019~2023의 14건)
  - [x] 감사의견은 **3개년씩** 온다 → 12년에 4회
  - [x] 2015년 데이터 **정상**
  - [x] **호출량 162,000 → 83,700** (`exctvSttus` 1차 제외 + 누적/3개년 응답 활용)
- [x] **N6-2 스키마 2개 + 등록 6곳** — `dart_employee_raw`·`dart_governance_raw`.
      `rcept_no`를 **NOT NULL + UNIQUE에 포함**(vintage 보존). 등록 6곳 전부 반영했고
      **안전장치 4개가 실제로 걸렸다**(profiling catalog · mirror 2건 · RAW_TABLES 개수)
- [x] **N6-3 어댑터 + 포트** — `opendart_periodic_extras`.
      포트는 **엔드포인트별 메서드가 아니라 statement enum 하나**로 받는다 —
      5개가 인자도 응답 모양도 같아서 메서드 5개는 URL만 다른 복사본이 된다.
      `rcept_no`가 빈 행은 **응답 전체를 실패시킨다** — 빈 값으로 저장하면
      그 보고서의 모든 vintage가 고유키에서 한 행으로 합쳐진다
- [x] **N6-4 서비스 + CLI** — `dart sync-periodic-extras`, 기본 `--reprt-codes 11011`.
      **연도 씨닝이 서비스에 들어갔다**(162,000 → 83,700). 기본 `--universe-scope historical` —
      감사 비적정은 대부분 나중에 상폐된 기업 얘기라 current로 잡으면 이 패키지가
      존재하는 이유가 편향된다
- [ ] **N6-5 대상 집합 구성** — ~~N3 + filing_receipt~~ → **S-1이 선행 조건**.
      `dart_filing_receipt_raw`가 `active_only=True`로 수집돼 **그 자체가 편향돼 있다**
      ([`poc/survivorship_gap.md`](poc/survivorship_gap.md))
      (현재 상장사로 잡으면 부실 신호에 생존편향이 그대로 들어온다)
- [x] **N6-6 테스트** — 41건
  - [x] 정정본 시나리오 — 다른 `rcept_no`가 오면 덮어쓰지 않고 행이 는다
  - [x] **ledger 기반 exit 75 재개** — 1차 run이 1건 뒤 중단돼도 **끝난 슬라이스는
        flush돼 남고**, 2차 run이 남은 것만 정확히 집는다
  - [x] **연도 씨닝이 무손실인지** — 요청 목록을 고정하는 대신
        "원하는 연도가 전부 어떤 요청에 덮이는가"를 단언한다(statement × 구간 조합)
  - [x] 실 DB 검증 — 005930 FY2023 4호출로 34행(감사의견이 **제55·54·53기 3개년**),
        ledger 4슬라이스 전부 `expected==actual`, 재실행 시 4건 전부 skip
- [x] **N6-7 결정 5개 고정 (결과 보기 전)** — 2026-08-18, 권고안대로.
      확정본은 [`07` §6](07_w3_periodic_report_extras.md#6-결과-보기-전에-고정할-것--2026-08-18-확정-n6-7)
  - [x] 직원 수 정의 → **`sm`(합계)**. 정규직/계약직 구분 기준이 기업마다 다르다
  - [x] `hc_revenue_per_employee` 분모 → **기말 직원 수**. 평균은 전년 값을 요구해
        상장 첫 해와 공시 누락 연도가 전부 빈 값이 된다
  - [x] 감사의견 인코딩 → **이진**(적정/비적정)
  - [x] 부호 — 감사 비적정 `−` 고정, 최대주주 지분·직원 증가는 **미고정임을 사전에 적는다**
  - [x] **(신규) 합병·분할 보정 → 2게이트.** 증거(`합병등종료보고서(분할|합병)`가 해당
        사업연도 안) **AND** 크기(\|YoY\| ≥ 30%, 표준편차 13.7%의 약 2σ)면 결측.
        증거만 있으면 플래그 `hc_structural_change=1`만 남겨 되돌릴 수 있게 한다.
        **정정 — `dart_capital_change_raw`는 못 쓴다.** 물적분할은 모회사 주식 수를
        안 바꿔서 **동기가 된 LG화학 사례를 그 방법이 놓친다**(lake 실측: 2019~2021
        분할 흔적 0, `isu_dcrs_stle` 15종에 합병·회사분할 없음). 적용 규모는
        2015~2025 corp-year 43,549 중 증거 **1,023건(2.35%)**.
        상폐 기업은 `dart_filing_receipt_raw`의 `active_only` 편향 때문에 보정 밖 →
        N6-5에서 재적용
- [ ] **N6-8 백필** — 연도 분할, `dart-backfill-all-years.sh` **마지막 단계**
- [ ] **N6-9 횡단면 변동 측정** + **final-vintage 한계를 피쳐 문서·evidence grade에 명시**
- [ ] **N6-10 밸류업 공시 분류 가능성 확인** — 수집·분류만, 검정은 나중

### N7. KRX 공식 밸류에이션 — `daily_market_fundamental`

상세: [`08_w3_valuation_and_macro.md`](08_w3_valuation_and_macro.md) 파트 A · 약 6,000 호출

**PR3~PR6은 N1을 끝낸 뒤면 거의 복사다.**

> **2026-08-16 축소 판정** (K-6d) → [`poc/n7_n4_alternatives.md`](poc/n7_n4_alternatives.md) §1
>
> BPS/PER/PBR/EPS/DIV/DPS는 Open API에 없다(K-1). 그런데 조사 중에 **더 근본적인 게
> 나왔다** — A1이 판 진단 근거 셋이 전부 다른 작업으로 이미 귀속돼 있다.
> **I1은 SQL 버그**(`fin_scan.py:216-231`), **I7은 DART canonical 매핑**,
> **I6은 정정분 재수집**이다. 독립 PER/PBR이 셋 중 어디에도 기여하지 않는다.
>
> **남는 건 매핑 검증 하나고, 그건 횡단면 문제라 6,000일 이력이 필요 없다.**
> → **N7-2~N7-7 취소.** KIS `inquire_price` 횡단면 대조로 대체한다.
> **I1·I7·I6 수정이 선행이다** — 고치기 전에 대조하면 이미 아는 결함을 다시 발견할 뿐이다.

- [x] **N7-1 PoC** → [`poc/n7_fundamental.md`](poc/n7_fundamental.md)
  - [x] 컬럼 6개(BPS/PER/PBR/EPS/DIV/DPS), 과거 구간 정상, 휴장일은 전 종목 0
  - [x] **`PER == 0` 314건(34.0%)이 전부 EPS ≤ 0** — 적자기업과 정확히 대응
  - [x] **I1의 29.2%와 근접** → C3 검증 재료 확보
  - [x] **`DIV`는 0 → NULL 하지 않는다** — 무배당은 진짜 0(DPS와 259=259로 짝)
  - [x] **`BPS`를 NULL 대상에 추가** — PBR과 123=123으로 짝
  - [x] 행 수가 시총보다 **29·20종목 적다** → 조인 시 결측 확인 필요
- [ ] **N7-2 스키마 + 등록 6곳** — `date_month`. **N1과 합치지 않는다**
- [ ] **N7-3 어댑터 + 포트 + 도메인** — **`PER`·`PBR`·`BPS` 0 → NULL.
      `DIV`·`DPS`·`EPS`는 0 유지** (PoC §2에서 계획 수정)
- [ ] **N7-4 스토리지** — 슬라이스 `(trade_date, market)`, 원자적 upsert + 행 수 대조
- [ ] **N7-5 서비스 + CLI** — `prices fundamental-backfill`
- [ ] **N7-6 테스트**
- [ ] **N7-7 백필** — 최근 → 과거
- [ ] **N7-8 대조 분석 C1~C5** — **이 작업의 주 산출물**
  - [ ] C3 `PER == 0` 집합 vs I1의 "1분위 몰림" 집합 — **I1의 직접 검증**
  - [ ] C5 IC는 **exploratory로 표기.** 승격하면 새 config로 confirmatory run
  - [ ] `10_known_issues.md` I1·I7 반영 여부 판단

### N8. 고용 지표 (ECOS)

상세: [`08_w3_valuation_and_macro.md`](08_w3_valuation_and_macro.md) 파트 B · **새 테이블 없음**

- [x] **N8-1** → [`poc/n8_employment.md`](poc/n8_employment.md)
  - [x] 통계표 **`901Y027`**(경제활동인구, 월간). 실업률 `I61BC`·고용률 `I61E`·
        취업자 `I61BA`, **1999-06부터**
  - [x] **vintage 조회 미지원** → final-vintage 명시
  - [x] **원계열/계절조정이 `ITEM_CODE2`로 갈린다** (`I28A`/`I28B`) → **원계열 채택**
  - [x] **(신규) KSIC 대분류별 취업자 존재** — `industry_groups.py` 체계와 매칭.
        업종 단위 매크로 조건화 재료. 수집만, 검정은 나중
  - [ ] ECOS 어댑터가 `ITEM_CODE2`를 지정할 수 있는가 — 안 되면 어댑터 확장 선행
- [ ] **N8-2** **readiness 창 정렬 문제 먼저 정리** — 37개 중 4개만 ready인 상태에서
      시리즈를 늘리면 게이트가 더 어지러워진다
- [ ] **N8-3** `definitions/common_features.py`에 시리즈 추가 + 유닛 테스트
- [ ] **N8-4** `common seed` → `common sync` 실행, 커버리지 확인
- [ ] **N8-5 중복 측정 게이트** — **변환 후·availability 정렬 후** 상관.
      0.8 초과면 **추가하지 않는다** (raw level 상관은 게이트로 쓰지 않는다)
- [ ] **N8-6** inactive pykrx 폴백 3종 처리

---

## K 묶음 — KRX 접근 경로 전환 (신규 축, 2026-08-16)

조사: [`poc/krx_open_api.md`](poc/krx_open_api.md) ·
경로 인벤토리: [`poc/krx_access_inventory.md`](poc/krx_access_inventory.md)

**O-14를 정정하는 축이다.** 우리는 IP 차단을 페이싱 문제로 읽고 스로틀을 통일했다.
그 뒤 **0.32 req/s**(어젯밤의 1/3)로 돌렸는데 **95요청 만에 다시 차단**됐다.
1차 차단은 9시간 만에 풀렸는데 재차단은 5분 만에 왔다.

KRX 안내문이 이유를 명시한다 — **약관 제10조 제2호는 자동화 수단에 의한 수집 자체를 금지**한다.
속도 조건이 붙어 있지 않다. **느리게 하면 허용되는 종류의 규칙이 아니다.**
그래서 스로틀링은 여기서 멈추고, 남은 질문은 **공식 경로가 우리 수요를 덮는가** 하나다.

- [x] **K-0b 운영 지속 여부 — 결정됨: 안 B** (2026-08-16, 사용자 결정)
  - [x] **"하루 38요청이라 계속 돌려도 된다"는 K-0과 충돌한다.**
        약관 제10조 제2호에 **속도 조건이 없다.** 38회와 6,000회 사이에 경계가 없고,
        **호출량은 탐지 위험을 바꿀 뿐 권한을 만들지 않는다**
  - [x] **결정: 안 B — 계속 돌리되 한시적 위반임을 명시하고 교체를 최우선으로 한다.**

        > **이 판단을 숨기지 않고 그대로 적는다.**
        > 논리적으로 일관된 것은 안 A(전면 중단)다. 그런데도 B를 택한 이유는
        > **요청량이 작아서가 아니다.** 매일 끊기면 `daily_ohlcv`·`flows`·`common`에
        > **되메울 수 없는 공백**이 생기고(원천이 과거를 다시 주지 않는 구간이 있다),
        > 교체 기간이 며칠~몇 주로 예상되기 때문이다.
        > **공백과 위반 지속을 견주어 후자를 감수한 것이다.**

  - [x] **B의 조건 — 이걸 안 지키면 B가 아니라 방치다**
    - [x] 교체(K-4·K-6f)를 **다른 모든 작업보다 앞에 둔다**
    - [ ] **새 백필을 시작하지 않는다.** 중단된 N1·N3는 인증키가 나올 때까지 그대로 둔다.
          일 증분만 유지한다 — 차단을 부른 게 백필이었다
    - [ ] `universe sync --source pykrx`·`common-sync-pykrx.sh`·live test 등
          **조건부·수동 경로는 실행하지 않는다**
    - [ ] 교체가 끝난 경로는 **즉시** 스크래핑을 끈다. 병행 운영 기간을 두지 않는다
- [ ] **K-0c (신규) 새 원천도 약관 게이트를 통과시킨다** —
      **KRX Open API를 "공식이니까 허용"으로 통과시킨 게 같은 실수다.**
      공식 약관에 **비상업 목적 · 제3자 제공 제한 · 출처 표시 · 계약 종료 후 이용 제한**이 있다.
      **엔드포인트가 공식이라는 사실이 장기 보관과 파생물 이용 권한을 주지 않는다**
  - [ ] 대상 셋 — **KRX Open API · 공공데이터포털 · KIS**
  - [ ] 질문 넷 — 비상업 목적 해당 여부 / 장기 보관 / 파생물(피쳐·마트) 지위 / 계약 종료 시 처리
- [x] **K-0 차단 성격 확정** (2026-08-16) — 추정이 아니라 안내문 전문으로 확인
  - [x] 더미 자격증명 프로브로 **계정 문제가 아님** 확인
  - [x] raw `requests` + Chrome UA도 같은 `blockError_01.jsp` →
        **pykrx 고유 문제가 아니다.** 라이브러리를 바꿔도 안 풀린다
  - [x] **사실 정정**: 일별 가격 수집(2,763건/저녁)은 KRX가 아니라 **naver**로 간다
        (`adjusted=True` → `naver.get_market_ohlcv_by_date`)
- [x] **K-1 Open API 서비스 목록 대조** (2026-08-16) — **핵심은 되고, 일부는 안 된다**
  - [x] 사양 확인 — 베이스 `data-dbg.krx.co.kr/svc/apis/{그룹}/{엔드포인트}`,
        `AUTH_KEY` 헤더, `basDd` 하루 단위, 응답 `OutBlock_1`.
        **지금 MDC와 같은 모양이라 어댑터 교체 비용이 낮다**
  - [x] 한도 **하루 10,000회**, 무료, 키 1년
  - [x] **함정 — 키만으로는 못 쓴다.** 엔드포인트마다 이용 신청이 따로 필요하다
  - [x] **된다**: 유가증권·코스닥 일별매매정보 / 종목기본정보, KOSPI·KOSDAQ 시리즈 일별시세정보,
        ETF·ETN·ELW, 채권·파생·일반상품·ESG. 유가증권 일별매매정보는 **2010-01-04부터**
        (우리 표본 시작 2014-06보다 앞선다)
  - [x] **안 된다**: **투자자별 거래실적 · 공매도 · PER/PBR/배당수익률 · 지수 구성종목.**
        각각 `flows` · `flows` · N7 · N4에 해당한다
  - [x] **호출량이 오히려 유리하다** — 한 호출이 그날 그 시장 전종목을 주므로
        **N1과 N3가 같은 호출로 합쳐진다.** 2014-06~현재 2시장 ≈ 6,000회 =
        **하루 한도 안에서 전량 백필이 끝난다**
- [x] **K-1b 접근 경로 인벤토리** (2026-08-16) —
      [`poc/krx_access_inventory.md`](poc/krx_access_inventory.md).
      **무엇을 교체해야 하는지가 먼저 있어야 한다**
  - [x] **외부 리뷰 반영 — 초판의 사실 주장 3건이 틀렸다** (2026-08-16)
    - [x] **`prices backfill`도 KRX를 친다.** 데이터는 naver가 맞지만
          `get_pykrx_stock_module()` → pykrx import → `build_krx_session()`이
          **실행당 로그인 3~4요청**을 보낸다. **매일 도는 건 셋이 아니라 넷이다.**
          O-14에 이어 이 항목을 **두 번 틀렸다**
    - [x] **FDR 경로 분석이 틀렸다.** 설치본은 `KrxMarcapListingCache`를 쓰고
          이 클래스는 **GitHub CSV 캐시**를 읽는다. `MDCSTAT01501`을 치는
          `KrxMarcapListing`은 **이 경로에서 안 쓰인다.**
          → "N1 데이터를 받아 버린다"·"`OutBlock_1` 간접 근거" **둘 다 철회**
    - [x] **"하루 53요청"은 HTTP 수가 아니라 논리 작업 수다.**
          투자자 bulk 1건이 내부에서 4 엔드포인트를 친다. warmup·login·retry 미계수
    - [x] **누락 경로 추가** — `universe sync --source pykrx`(CLI),
          `common-sync-pykrx.sh`(`SDC_COMMON_ENABLE_PYKRX`), opt-in live test 3종,
          그리고 **`universe_fdr`의 pykrx 자동 폴백**(`provider.py:80`)
  - [x] **`universe sync`가 KRX 수집기였다 — 회계에 없던 항목이다.**
        `fdr.StockListing('KOSPI')`가 FDR 라이브러리 안에서
        `data.krx.co.kr/comm/bldAttendant/getJsonData.cmd`를 친다.
        **우리 `krx_common/client.py`의 `KRX_MDC_URL`과 같은 주소다**
  - [x] **문이 셋이다** — MDC 로그인(`flows`·`common krx`) / pykrx 로그인(N1·N3) /
        **FDR 익명**(`universe sync`). **하나를 고쳐도 나머지가 남는다.**
        FDR 경로는 로그인도 안 하고 **우리 `HumanThrottle`도 안 걸린다**
  - [x] 일 요청 실측(`ingestion_runs.counts`, 2026-08-15) —
        `flows` **38** · `common krx` **11** · `universe sync` ~4 = **약 53건/일.**
        **차단을 부른 건 이쪽이 아니라 백필 둘**(N1 ~6,000 / N3 ~290)이다
  - [x] **`prices backfill`은 KRX가 아니다** — pykrx 소스로 분기 확인.
        `adjusted=True`(기본값) → `naver.get_market_ohlcv_by_date`.
        O-14에서 한 번 틀렸던 부분이라 근거를 남겼다
  - [x] 비활성으로 묶여 있는 KRX 요청 — pykrx 지수 폴백 3종 + 산업지수 4종.
        **켜면 요청이 는다**는 걸 명시
- [x] **K-2 응답 필드 확정 — 닫혔다** (2026-08-16, 외부 리뷰)
  - [x] **주식 서비스 상세 페이지의 embedded schema에 `MKTCAP`·`LIST_SHRS`·`ACC_TRDVAL`이 있다.**
        **공식 스키마이지 정황이 아니다.** N1이 받으려는 것 그대로다
  - [x] **키 발급 blocker가 아니었다.** 남은 live 검증은 **값 품질과 과거일자 coverage**뿐
  - [x] **live 검증 완료** (2026-08-18, 10요청) → [`poc/krx_open_api.md`](poc/krx_open_api.md) §4.1c.
        embedded schema 판독이 맞았고, **필드가 예상보다 많다** — `stk_bydd_trd` 15필드
    - [x] 값 품질 — `MKTCAP`·`LIST_SHRS`·`ACC_TRDVAL` 실제 값 수신.
          **전부 문자열**이므로 숫자 변환은 우리가 한다
    - [x] 과거일자 coverage — `basDd=20140602`가 **KOSPI 907 · KOSDAQ 1,009행**.
          **N3-5b 1차 실행 실측(2014-06 KOSPI 885~907 · KOSDAQ 1,010)과 맞는다.**
          독립된 두 경로가 같은 수를 준다
    - [x] **휴장일은 `rows=0`** (`20260817` 대체공휴일). pykrx의 "0으로 채워진 전종목 행"과
          정반대라 **`alternative=False` 방어가 이 경로엔 불필요하다**
    - [x] **원주가 4필드가 딸려 온다** — `TDD_OPNPRC`·`HGPRC`·`LWPRC`·`CLSPRC`.
          **K-7의 재료가 N1-8에 무료로 따라온다**
    - [x] **`SECT_TP_NM`은 업종이 아니다** — KOSDAQ 소속부(벤처기업부·중견기업부),
          KOSPI는 빈 값. N2 `induty_code`·N4 KRX업종과 **다른 축이다**
  - [x] **철회** — 초판이 든 간접 근거(FDR의 `MDCSTAT01501` / `OutBlock_1`)는
        **경로 분석 자체가 틀려서 성립하지 않는다**(K-1b 정정)
- [ ] ~~K-2 응답 필드 확정~~ — 아래는 기록 (초판)
  - [x] 확인된 필드는 `BAS_DD`·`ISU_CD`·`ISU_NM`·`TDD_CLSPRC`뿐.
        공식 상세 페이지가 필드 표를 JS로 렌더링해 WebFetch로 안 잡히고,
        서드파티 구현체도 문서화하지 않았다
  - [ ] **`시가총액`·`상장주식수`·`거래대금`이 오는가** — MDC 전종목시세(`MDCSTAT01501`)에
        셋 다 있고 Open API는 같은 통계의 API 형태라 가능성은 높지만 **단정하지 않는다**
  - [x] **간접 근거가 하나 늘었다** (K-1b) — FDR이 치는 `MDCSTAT01501`의 응답 키가
        **`OutBlock_1`**이고 파싱 필드가 `MKTCAP`·`LIST_SHRS`·`ACC_TRDVAL`이다.
        Open API 일별매매정보와 **응답 키가 같다.** 정황은 강해졌지만 여전히 정황이다
  - [ ] 확정 방법: 키 승인 후 상세 페이지 TEST 버튼 또는 개발명세서 다운로드
- [x] **K-3 엔드포인트 이용 신청 — 승인 완료** (2026-08-18). 인증키와 **별개 절차였고**
      하루 만에 둘 다 끝났다. 계획했던 6건이 아니라 **16건을 신청했다**
  - [x] **인증키 발급** — 로컬 `.env` `AUTH_KEYS`에 **2개**를 콤마로 이어 뒀다.
        40자 영숫자로 OpenDART 키와 같은 모양이고, `settings.krx_openapi_auth_keys`가 읽는다
  - [x] **승인 전 상태를 남겨둔다 — 이 진단법은 다음에도 쓰인다** (오전, 8요청)
    - [x] 키가 유효한데도 5개 엔드포인트 전부 **401**이었다. 그런데 **본문이 갈린다** —
          헤더 없음·가짜 키는 `Unauthorized Key`, **우리 키는 `Unauthorized API Call`**.
          **메시지만으로 "키가 틀렸다"와 "엔드포인트가 안 열렸다"를 구분할 수 있다.**
          K-1 §2.1의 함정 그대로였다
    - [x] **차단 페이지가 아니다.** `data-dbg.krx.co.kr`는 JSON으로 답한다 —
          스크래핑 차단(`data.krx.co.kr`)과 다른 호스트고 IP 제한과 무관하다
  - [x] **승인 후 실호출 200 확인** (오후, 10요청). 키 2개 모두 같은 응답
    - [x] 유가증권 일별매매정보 `sto/stk_bydd_trd` — 942행(20260814)
    - [x] 코스닥 일별매매정보 `sto/ksq_bydd_trd` — 1,821행. **둘이 2,763으로
          우리 유니버스 크기와 맞는다**
    - [x] 유가증권 종목기본정보 `sto/stk_isu_base_info` — 12필드
    - [x] KOSPI 시리즈 일별시세정보 `idx/kospi_dd_trd` — 지수 51종
  - [x] 신청 범위는 더 넓다 — KRX·KOSDAQ 시리즈, 채권지수, 파생상품지수,
        코넥스 일별매매정보·종목기본정보, 신주인수권 증권·증서, ETF·ETN·ELW.
        **당장 쓰는 건 위 넷이고 나머지는 열려만 있다**
  - [x] ~~공공데이터포털 우회 경로~~ — **필요 없어졌다.** 이력이 2020-01-02부터라
        54%만 덮었는데, KRX가 2014-06을 준다. `DATAGO_KEY`는 대조 검증용으로만 남긴다
  - [ ] **한도가 키 단위인지 계정 단위인지는 모른다.** 실측하려면 10,000회를 써야 한다.
        **키당으로 가정하지 않고** 소진 응답을 만나면 그때 확정한다 (K-4)
- [ ] **K-4 `adapters/market_data_krx_openapi/` 구현** — K-2 확정 후
  - [ ] **포트 변경이 없다.** `MarketCapProvider`·`HistoricalUniverseProvider`가 이미
        `(trade_date, market)` 슬라이스 단위라 어댑터만 갈아끼우면 된다
  - [x] 인증키는 `.env` → `get_settings()`. `KRX_ID`/`KRX_PW`와 다른 축이다
    - [x] **배선 완료** (2026-08-18) — `AUTH_KEYS` → `settings.krx_openapi_auth_keys`
          (콤마 구분 tuple, OpenDART 다중키와 같은 모양). `DATAGO_KEY` →
          `settings.datago_api_key`도 같이 넣었다. 어댑터는 아직 없다
  - [ ] **다중키 로테이션** — 키가 2개다. OpenDART `opendart_common` 실행기가
        하는 일과 같으므로 그쪽을 복제한다
  - [ ] 한도 소진 시 종료 코드 — OpenDART의 `75` 전례를 따른다.
        **한도가 키 단위인지 계정 단위인지 모르므로**(K-3) 로테이션이 소진을
        못 피할 수 있다. 그 경우도 정상 종료 경로여야 한다
  - [ ] **파서가 지킬 것 셋** (§4.1c 실측) — 값이 전부 **문자열**이고,
        `idx` 응답에는 **빈 문자열**이 섞이며(`코스피 (외국주포함)`의 `CLSPRC_IDX`),
        **휴장일은 `rows=0`**이다. 마지막 항목 덕에 `alternative=False` 방어는 불필요하다
  - [ ] **N1과 N3를 한 서비스로 합친다** — 같은 호출이 시총·상장주식수·거래대금과
        그날의 종목 집합을 함께 준다. `backfill_market_cap`과
        `backfill_universe_snapshots`를 따로 돌릴 이유가 없어진다
  - [ ] **원주가 4필드를 어디에 쓸지 정한다** — `TDD_OPNPRC`·`HGPRC`·`LWPRC`·`CLSPRC`가
        같이 온다. `daily_market_cap`에 넣을지 별도 테이블로 뺄지가 **K-7의 입구다**
- [~] **K-5 스크래핑 경로 폐기** — 2026-08-18 **pykrx 문은 닫았다.** 남은 둘은 prod 자격증명 대기
  - [x] **(K-6f에서 발견) `shorting` 그룹 freshness를 먼저 손봐야 한다.**
        그룹 최신일은 metric 3개의 최솟값인데 그중 `short_selling_balance_quantity`는
        KIS가 못 채운다. KRX를 끄는 순간 **이 그룹이 매일 stale로 뜬다.**
        → `DISCONTINUED_FLOW_METRICS`로 **수집 중단을 선언**해 예산에서만 뺐다.
        `ops freshness-report`에는 마지막 수집일과 사유가 계속 보인다.
        **모르는 그룹은 계속 검사한다** — "모른다"가 게이트를 끄는 방법이 되면 안 된다
  - [~] **문이 셋이라 셋 다 닫아야 한다**(K-1b).
    - [x] **pykrx 문 — 닫았다.** `ALLOW_KRX_SCRAPING` 기본 off.
          import·세션 refresh 두 입구 모두 게이트
    - [ ] **MDC 직접**(`flows`·`common krx`) — prod KIS 키 대기.
          `common krx`는 대체재 자체가 없다(Open API 지수 엔드포인트가 후보)
    - [ ] **FDR 익명**(`universe sync`) — prod `AUTH_KEYS` 대기.
          `universe-sync.sh`에 전환 조건을 주석으로 박아뒀다
  - [x] `universe sync`는 종목기본정보(`stk_isu_base_info`)로 **대체 가능하다** —
        2026-08-18 실호출로 12필드 확인. `ISU_SRT_CD`(단축코드)·`ISU_ABBRV`(약명)·
        `MKT_TP_NM`(시장)·`LIST_DD`(상장일)로 `stock_master`가 채워진다.
        **FDR을 계속 쓰면 우리 스로틀 밖의 KRX 트래픽이 남는다**
    - [x] **덤으로 N3-3의 미해결 질문이 풀린다** — `SECUGRP_NM`(주권 여부)과
          `KIND_STKCERT_TP_NM`(보통주/우선주)이 있다.
          "우선주·리츠·스팩 혼입 → 필터 정책"의 재료가 여기 다 있다
  - [x] **`prices backfill`의 pykrx import를 끊는다** — `adapters/prices_naver/`.
        prod 스크립트가 `--source`를 안 넘기므로 **기본값 전환만으로 닫힌다**(prod 변경 0).
        실측으로 함정 셋이 나왔다 — ① 선언부 앞에 **빈 줄**이 와서 그냥 파싱하면 깨진다
        ② EUC-KR 선언이 붙은 **bytes는 ElementTree가 거부**한다("multi-byte encodings
        are not supported") ③ **에러 페이지도 well-formed라** 파싱만으로는 "행 없음"과
        구분이 안 된다 → **root 태그**로 가른다(없는 종목도 `<protocol />`은 준다)
  - [x] **`universe_fdr`의 pykrx 폴백 제거** — 이제 FDR 실패는 그냥 실패다.
        어댑터가 `PykrxUniverseProvider`를 **import조차 하지 않는지** 테스트로 고정
  - [x] **조건부·수동·테스트 경로도 범위에 넣는다** — `ALLOW_KRX_SCRAPING` 하나로 덮는다.
        에러 메시지가 **대안 명령과 재활성화 방법을 같이 말한다** —
        "안 된다"만 말하는 게이트는 결국 플래그를 켜는 것으로 우회된다
  - [~] **실 HTTP·page·retry·login 계수를 도입한다** — 신규 경로는 실계수다
        (`KrxOpenApiCounters`, `NaverDailyPriceProvider.http_requests`).
        **남은 스크래핑 어댑터의 `requests_attempted`는 여전히 논리 작업 수다**
- [ ] **K-6 빈 항목을 어떻게 덮을 것인가.** `flows` · N7 · N4
  - [x] **K-6a `flows` 대체 조사 완료** (2026-08-16) →
        [`poc/flows_alternatives.md`](poc/flows_alternatives.md).
        **7 metric 중 5개는 KIS Developers로 대체 가능, 2개는 갈 곳이 없다**
    - [x] **문제가 생각보다 작다** — `krx_security_flow_raw`가 **2007-06-05 ~ 2026-08-14를
          이미 갖고 있다.** 필요한 건 백필이 아니라 **forward-fill**이다.
          이력이 얕은 후보도 쓸 수 있다는 뜻이라 판단이 달라졌다
    - [x] **공공데이터포털은 없다** — 금융위 라인업이 KRX Open API와 같다.
          "공매도"로 나오는 건 전부 **이노핀(민간) 연계데이터 = 파생 스코어**이지 원본이 아니다.
          예탁결제원 증권대차서비스는 **대차이지 공매도가 아니다**
    - [x] **KIS가 덮는 것** — 종목별 투자자매매동향(일별) `FHPTJ04160001`(개인·외국인·기관 순매수),
          국내주식 공매도 일별추이 `FHPST04830000`(공매도 거래량·거래대금).
          근거는 공식 샘플 코드(`open-trading-api`)
    - [x] **구조가 정반대다** — KRX는 `(날짜×시장)→전종목`, KIS는 `(종목)→기간`.
          일 호출이 38 → **약 5,526**으로 늘지만 실전계좌 초당 20건이라 **약 4.6분**이다.
          반대로 **백필은 KIS가 유리하다**(종목당 1호출로 날짜 구간을 준다)
    - [x] **갈 곳이 없는 둘** — **외국인 보유주식수**(17.5M행, 가장 큰 테이블)와
          **공매도 잔고 수량**. KIS의 `외국계 순매수추이`는 **순매수이지 보유잔고가 아니다.**
          KIS는 공매도 **거래**만 주고 잔고는 없다. **KRX만 만드는 데이터다**
    - [x] **급하지 않다** — 하루 38요청은 차단을 부른 규모가 아니다(백필이 6,000이었다).
          약관 대상인 건 맞으므로 **한시적이라는 걸 인지하고 가는 것**이지 안전하다는 뜻은 아니다
- [x] **K-6b 실호출 확인 완료** (2026-08-16) —
      [`flows_alternatives.md`](poc/flows_alternatives.md) §5b. 발급된 키로 직접 호출
  - [x] **`flows`는 5/7이 아니라 6/7이다.** `inquire_price`에 **`frgn_hldn_qty`가 직접 있다**
        (005930 = 2,736,287,683). `lstn_stcn`으로 검산 시 `hts_frgn_ehrt` 46.80%와 일치.
        **비율 곱셈 불필요**
  - [x] **공매도 잔고는 없다 — 확정.** `output2` 21필드에 거래량·거래대금·누적·비중만 있다.
        **남은 미해결은 이 하나뿐이다**
  - [x] **`DIV`도 없다** → **N7의 C4는 수행 불가 확정** (폐기 대상)
  - [x] **이력 깊이 2014-04까지 확인** — 우리 표본 시작(2014-06)보다 앞선다.
        페이지 크기는 공매도 **100행**, 투자자 **30행**
  - [x] **매일 돌 필요가 없다** — 투자자매매동향이 **호출당 30 거래일**을 준다.
        주 1회로도 창이 겹쳐 안전하고 **놓친 날을 자동으로 메운다.**
        "일 5,526 호출"은 매일 돌린다는 가정에서 나온 수다
  - [x] **확장 기회 발견** — KIS `output2`가 **101필드**로 기관을
        **증권·투신·은행·보험·사모·기금·기타법인**으로 쪼갠다.
        현재 우리는 기관 통합 하나뿐이다. **별도 판단 대상**
  - [ ] 남은 것 — 공매도 잔고 대체 경로 · **KIS 일일 quota 미확인** · 2020-01 HTTP 500 재확인
  - [ ] ~~K-6b `flows` 실행 전 확인 4개~~ — [`flows_alternatives.md`](poc/flows_alternatives.md) §6
    - [ ] `FHPST04830000`의 output1/output2 필드 — 잔고가 섞여 있는가
    - [ ] KIS 주식현재가에 `hts_frgn_ehrt`(외국인 소진율)가 있는가.
          있으면 **`상장주식수 × 소진율`로 외국인 보유주식수를 복원**할 수 있다 → **6/7이 된다**
    - [ ] KIS 이력 깊이 — 문서에 없다
    - [ ] KRX 데이터 상품 가격·조건 — **판매 페이지가 500 에러다.** 전화 문의(1577-0088)
  - [ ] **K-6c KIS 약관 확인** — 증권사 오픈API는 일반적으로 **본인 이용 범위**다.
        연구용 저장은 대체로 그 안이지만 **재배포는 아니다**
    - [x] **자격증명 발급 완료** (2026-08-16) — `KIS_APP_KEY`·`KIS_APP_SECRET`·
          `KIS_BASE_URL`·`KIS_TIMEOUT_SECONDS`가 **로컬 `.env`에 있다.**
          `.env.example`·`CLAUDE.md`·`docs/operations.md`에 기록.
          **prod `.env`에는 아직 없고 읽는 코드도 없다**
    - [ ] 약관 본문 확인
- [x] **K-6f KIS 어댑터 구현 완료** (2026-08-16). 실호출로 검증했다.
      **어댑터보다 감사 모델 정비가 컸다는 예측이 맞았다** — 어댑터 2파일,
      전환 조건 쪽이 그 몇 배다
  - [x] **결정: 안 (a) — 기존 6 metric만 채운다.** `output2` 101필드의 기관 세분
        (증권·투신·은행·보험·사모·기금·기타법인)은 **수집하지 않는다.**
        교체와 확장을 한 PR에 섞으면 회귀 원인을 가릴 수 없다. 확장은 별건으로 남긴다
  - [x] **포트를 나눴다** — `TickerFlowProvider`(신규) vs 기존 `FlowProvider`.
        KRX는 `(날짜×시장)→전종목`, KIS는 `(종목)→기간`이라 **작업 단위 자체가 다르다.**
        한 포트에 욱여넣으면 호출당 30~100 거래일이라는 KIS의 이점을 버리고
        실패 단위도 거짓이 된다. 서비스도 `service/sync_kis_flows.py`로 분리
  - [x] **전환 조건 6개 — 전부 처리** ([`flows_alternatives.md`](poc/flows_alternatives.md) §3.1b)
    - [x] **source 전환 cursor** — `get_krx_security_flow_metric_max_dates`와 신규
          `..._ticker_metric_coverage`가 **`sources` 리스트**를 받는다.
          커서는 provenance가 아니라 **metric**을 추적한다 → KIS 첫 실행이 KRX가
          멈춘 자리에서 정확히 이어진다
    - [x] **종목·날짜 checkpoint** — 종목×metric별 `(세션 수, 최신일)`을 읽어
          종목 단위로 창을 계산한다. 실패는 `params.failed_request_keys`에 **전량** 남긴다
          (sample 3건이 아니다). **최신일만 보지 않는다** — 커서 뒤에 구멍이 있으면
          창 전체를 다시 받는다. 최신일만 보는 게 종목 단위 수집기가 구멍을 숨기는 방식이다
    - [x] **no-data tombstone** — `params.no_data_request_keys` + **TTL 7일**.
          영구가 아닌 이유는 거래정지 종목이 다시 살아나기 때문이다.
          DART가 쓰던 negative cache를 `util/pipeline.py`로 올려 **한 구현을 공유**한다
    - [x] **실 HTTP·retry 계수** — 클라이언트가 요청·재시도·페이지·유량거부·
          토큰발급·스로틀 대기를 세고 `ingestion_runs.counts`에 `http_*`로 들어간다.
          실측 예: 논리 요청 6건 = **실 HTTP 11건 · 재시도 5건**. 기존 지표로는 안 보인다
    - [x] 전역 auth·rate-limit 서킷 브레이커 — `SourceAuthError` /
          `SourceQuotaExhaustedError`로 run을 **FAILED로 끝낸다.**
          **`call_with_retry`가 이 둘을 재시도하던 결함도 같이 고쳤다** —
          인증 재시도 1회 = 알림톡 1회라 3회 시도는 알림톡 3회였다
    - [x] **source-aware freshness** — `FLOW_SOURCES = (KRX, KIS)`.
          `Source.KRX` 고정이었으면 전환 첫날부터 매일 오탐이었다
  - [x] CLI `flows sync-kis` 신설 (`Source.KRX` 하드코딩과 무관하게 별도 경로).
        **`--plan-only`는 요청 0건·토큰 발급 0건**으로 계획만 출력한다
  - [x] **토큰 캐시** — `state/kis_token.json`, 원자적 쓰기, 0600, 갱신 여유 1시간.
        `deploy/prod/compose.yaml`에 `./state:/state` + `KIS_TOKEN_CACHE_PATH` 추가.
        **`collector`에 볼륨이 붙은 유일한 이유가 이것이다.**
        이번 세션 전체에서 **토큰 발급 0회** (이전 세션 캐시 재사용)
  - [x] `settings.py` `kis_*` 8필드 추가
  - [ ] **prod `.env`에 키 반영** — 아직 안 했다. 사람이 넣어야 `flows sync-kis`가 prod에서 돈다
  - [x] 유량 제한을 `HumanThrottle`이 아니라 **토큰 버킷**(`util/rate_limit.py`)으로.
        KRX용은 탐지 회피 랜덤 지연이라 성격이 다르다
  - [x] **실측이 문서를 뒤집었다 — 유량은 초당 20건이 아니라 초당 1건이다**
    - [x] 1.0/s에서 20/20 성공·거부 0. **1.2/s에서 이미 3건 거부**, 1.5/s는 6건.
          20/s·10/s·5/s 전부 절반 이상 거부. **어떤 rate에서도 실효 처리량 1.1/s를 못 넘었다**
          → 기본값 `KIS_REQUESTS_PER_SECOND=1.0`, burst 1
    - [x] **유량 초과가 HTTP 500으로 온다** (429가 아니다). 상태 코드로만 분기하면
          일반 서버 오류로 묻혀 **rate-limit 카운터가 0이 되고 서킷 브레이커가 영영 안 걸린다.**
          첫 실호출에서 실제로 그렇게 나왔고(`http_rate_limited=0` / 실제 5건), 본문
          `msg_cd`를 상태 코드보다 먼저 보도록 고쳤다
    - [x] **운영 계산이 바뀐다** — 2,763종목 기준 `foreign_holding` 하루치 **약 46분**,
          `investor`+`shorting` 1회 **약 1.5시간**, 전체 **약 2.3시간**.
          초당 20건 가정의 "4.6분"은 성립하지 않는다.
          → **`foreign_holding` 매일 · 나머지 주 1회**로 나눈다
  - [x] **`foreign_holding`은 최신 거래일에만 수집한다** — `inquire-price`는 기준일이 없는
        현재값이다. 과거 날짜로 요청하면 **오늘 값이 과거 날짜로 저장된다.**
        그래서 최신 세션이 아니면 그룹째 건너뛰고 이유를 run에 남긴다.
        완결 판정도 다른 그룹과 다르다 — "최신 세션 행이 있는가" 하나뿐이다
        (창 전체를 요구하면 채울 수 없는 날 때문에 매번 전 종목을 다시 친다)
  - [x] **테스트 51건 추가** — 파서(실응답 픽스처·필드 rename 감지), 페이징,
        토큰 캐시(재발급 없음·0600·손상 내성), 토큰버킷, 전환 조건 6개 각각,
        HTTP 500 유량거부, 재실행 요청 0건. 전체 **1,138 통과**
  - [x] **로컬 실검증** — 3종목 × 3그룹 = 9요청 → **78행**, 재시도 0·유량거부 0.
        값이 프로브 실측과 정확히 일치(`frgn_hldn_qty` 2,736,287,683 등).
        재실행 시 **계획 0건**(완전 idempotent)
  - [x] **K-6d N7 · N4 조사 완료** (2026-08-16) →
        [`poc/n7_n4_alternatives.md`](poc/n7_n4_alternatives.md).
        **둘 다 폐기가 아니라 축소다**
    - [x] **N7은 대체재보다 근거가 먼저 문제였다.** `08` A1이 판 진단 근거 셋을
          `10_known_issues.md` 원문에서 확인하니 **전부 다른 작업으로 이미 귀속돼 있다**
      - [x] **I1은 데이터 문제가 아니라 SQL 버그다** — DuckDB의 `GREATEST`/`LEAST`가
            NULL을 건너뛰어 `LEAST(GREATEST(NULL, p01), p99)`가 `p01`을 준다.
            원인·위치(`fin_scan.py:216-231`)·수정 SQL이 §2에 이미 있다.
            **독립 PER/PBR이 기여할 게 없다**
      - [x] **I7은 DART canonical 매핑 문제다** — `revenue` 8,103행 vs
            `net_income` 141,011행. §5.5.3이 **"XBRL fallback 보강이 정답"**이라 결론냈다
      - [x] ~~등급 B 상한(I6)은 정정분 재수집이 답~~ — **거꾸로 인용했다** (2026-08-16 정정).
            `10_known_issues.md` §5.5.2 제목이 **"I6 — 재수집으로 해결되지 않는다"**이고
            §3단계가 **"정정분 재수집(I6)은 빠지고"**라 명시했다. 정정 비율 ~10%는
            **이 원천의 기본값**이다. **I6은 해결 작업이 아니라 등급 임계값 결정 사항**이라
            **N7 선행조건에서 뺀다 — 남은 선행은 I7 하나다**
      - [x] **축소안의 검증 범위는 B/M·E/P뿐이다** (2026-08-16 추가).
            KIS는 `per`·`pbr`·`eps`·`bps`만 준다 → **CFO/P·S/P는 검증 못 한다.**
            그리고 **I7이 바로 `revenue`·`gross_profit` 문제라 I7 수정 결과를 못 본다**
      - [ ] **C4·C5 상태 미결** — 축소안으로 **수행 불가능**해졌다.
            C4는 KIS 현재가에 `DIV`가 없고, C5는 현재값 횡단면으로 IC를 못 낸다.
            **폐기인지 다른 시계열로 이전인지 명시할 것**
    - [x] **N7에 남는 건 매핑 검증 하나다** — I1·I7 수정 후에도 매핑이 맞는지는 모른다.
          **다만 횡단면 문제라 6,000일 이력이 필요 없다.**
          KIS `inquire_price`(`FHKST01010100`)가 `per`·`pbr`·`eps`·`bps`를 준다
    - [x] **N4는 목적 둘 중 하나가 이미 충족됐다** — 목적 B(관계 피쳐의 업종 집합)는
          N2-7b 완료로 **3,959 법인 / `induty_code` 630종**이 있고
          `industry_groups.py`가 이미 쓴다. **지수 구성종목이 아니어도 된다**
    - [x] 목적 A 후보는 KRX `업종분류 현황`(`MDC0201020506`) 전종목 CSV.
          화면 다운로드는 허용된 경로고, 지수 편입/제외에 섞이는
          **신규상장·유동성·방법론 개편 잡음이 없다**
    - [x] **그런데 "더 나은 원천"이라는 결론은 과했다** (2026-08-16 정정).
          목적 A는 **DART `induty_code` = KSIC**의 PIT 약점 측정인데
          **KRX 업종분류는 KSIC이 아니다.** `05` §5가 이미 표로 적어뒀다
          ("N2는 표준산업분류, N4는 KRX 업종").
          **cross-taxonomy 문제를 그대로 물려받는다** —
          KSIC 변경과 KRX 업종 변경을 구분 못 하고,
          **KRX 분류 기준 개정도 기업의 업종 변경처럼 보인다**
    - [ ] **분기점이 하나가 아니라 둘이다** — ① 과거 기준일 조회 가능 여부
          ② **KSIC ↔ KRX 업종 crosswalk 가능 여부.**
          **②가 없으면 대체안이 아니다.** 둘 다 안 되면 목적 A를 **측정 못 한 채**
          N2의 PIT 약점을 한계로 명시하고 간다
    - [x] **별건 — 공공데이터포털이 K-2를 우회한다.**
          `getStockPriceInfo`가 `mrktTotAmt`·`lstgStCnt`·`trPrc`를 주고
          **`beginBasDt`/`endBasDt` 범위 조회를 지원**한다. 하루 10,000회.
          ~~출처가 서드파티 문서라 공식 명세 대조가 남았다~~ → **실호출로 종결** (아래)
    - [x] **키 발급·실호출 확인 완료** (2026-08-18, 2요청) →
          [`n7_n4_alternatives.md`](poc/n7_n4_alternatives.md) §3.2
      - [x] **자동승인이라 대기가 없었다.** `DATAGO_KEY`가 로컬 `.env`에 있다.
            인증 통과(`resultCode=00`), 전체 4,391,760행
      - [x] **필드 15개가 서드파티 목록과 정확히 일치** → 명세 대조 항목 종결
      - [x] **`mrktCtg`(시장 구분)가 있다 — pykrx보다 낫다.** pykrx는 시장 구분이 없어
            시장별 호출로 확정했었는데 이쪽은 한 번에 온다. **호출 수가 절반이 된다**
      - [x] **Encoding 키는 URL에 raw로 넣는다.** `params=`로 넘기면 `%2B`가 `%252B`가 돼
            **서버가 다른 키를 받는다.** 어댑터 구현 시 이 함정을 피한다
      - [x] 최신 기준일 20260813, `basDt=20260814`는 0건 — **키 문제가 아니라 갱신 지연이다**
            (영업일 익일 13시 이후 + 8/17 대체공휴일)
      - [x] **범위 조회 동작 확인** — `beginBasDt`/`endBasDt`가 먹는다
            (파라미터가 틀렸으면 무시돼 무필터 총계가 나왔을 것이다).
            **날짜별 6,000회로 돌아가지 않는다**
      - [x] **그런데 이력이 짧다 — 2014-06이 0건이다.** 우리 표본 시작(2014-06-02)이 이력 밖이다
      - [x] **이력 시작일 = 2020-01-02 확정** — 마지막 페이지 100행이 전부 `20200102`다.
            2020년 첫 거래일이라 **연 단위로 끊긴 경계**고, 행 수 역산(하루 2,700종목)도 맞는다
      - [x] **구간 분담이 확정됐다** — 거래일 기준 **약 54%를 인증키 없이 덮는다**

        | 구간 | 원천 |
        |---|---|
        | 2020-01-02 ~ 현재 (약 1,630거래일) | 공공데이터포털 — **지금 가능** |
        | 2014-06-02 ~ 2019-12-30 (약 1,370거래일) | **KRX 인증키만** |

      - [x] **곁가지 — 스팩이 포함된다** (표본이 `엔에이치스팩14호`).
            N3-3의 "우선주·리츠·스팩 혼입 여부 → 필터 정책" 질문 일부가 답이 됐다.
            **필터는 우리가 건다**
  - [ ] **K-6e 남은 확인 3건** — [`n7_n4_alternatives.md`](poc/n7_n4_alternatives.md) §5
    - [x] KIS `inquire_price`에 `DIV`(배당수익률)가 있는가
          — **K-6b에서 "없다"로 이미 확인됐다.** 중복 항목이라 닫는다
    - [ ] **KRX 업종분류 현황이 과거 기준일 조회를 받는가** — 분기점 ①.
          자동 요청은 금지이므로 **사람이 화면 다운로드로 확인**한다
      - [x] **전종목 CSV 실물 확보** (2026-08-18, 사람이 수동 다운로드).
            KOSPI 942 + KOSDAQ 1,821 = **2,763행**으로 Open API 행 수와 정확히 같다.
            컬럼 8개 — `종목코드·종목명·시장구분·업종명·종가·대비·등락률·시가총액`
      - [x] **함정 — 파일명 날짜와 데이터 기준일이 다르다.** 파일명은 `20260818`인데
            값은 **2026-08-14 종가와 정확히 일치**한다(AJ네트웍스 4520·100·2.26·
            204,542,470,680). Open API `basDd=20260818`은 **0행**이다.
            **월별 스냅샷을 파일명으로 라벨링하면 조용히 잘못 붙는다**
      - [x] **다만 검증 수단이 생겼다** — 같은 날짜를 Open API로 조회해 종가를 대조하면
            **그 파일의 진짜 기준일을 확정할 수 있다.** 과거 기준일 조회를 시험할 때
            "정말 그 날짜 데이터가 왔는가"를 눈이 아니라 대조로 판정한다
      - [ ] **남은 것은 화면에 날짜 입력칸이 있는가 하나다** — CSV에 날짜 컬럼이 없어
            파일만으로는 알 수 없다
    - [x] **KSIC ↔ KRX 업종 crosswalk — 분기점 ② 판정 완료: 못 만든다** (2026-08-18).
          **연관은 강한데 목적 A가 요구하는 개별 종목 대응이 안 된다.**
          sj2가 막혀 있어 `dart sync-corp`(1호출) + `dart sync-corp-profile`
          **층화 표본 682건**(업종당 최대 30, 29종 전량)으로 로컬에서 직접 받았다.
          받은 건 668 · skip 14 · 오류 0, 교차표 표본 **700건**
      - [x] **총량 지표만 보면 통과처럼 보인다** — Cramer's V **0.936**,
            KRX 업종을 알 때 KSIC 불확실성 **85.8% 감소**.
            **두 분류가 같은 것을 보고 있는 건 맞다**
      - [x] **그런데 개별 종목에서 깨진다.** KRX 업종을 알 때 KSIC 최빈값이 맞을 확률이
            **76.9%**고, **양방향 80% 이상으로 안정적인 1:1 쌍에 드는 종목은 36.4%뿐이다.**
            나머지 **63.6%**가 애매한 구간이다
      - [x] **KRX 한 업종이 KSIC 둘을 반씩 담는 사례가 핵심이다** —
            `전기·전자` = 26(전자부품) 16 / 28(전기장비) 14,
            `운송장비·부품` = 30 / 31, `유통` = 46(도매) / 47(소매),
            `금속` = 24 / 25, `건설` = 41 / 42.
            **이 경계를 넘는 KSIC 변경은 KRX 업종에서 아예 안 보인다**
      - [x] **`일반서비스`는 KSIC 8종을 담고 최빈이 45%다.** 잔여 범주에 가깝다.
            `운송·창고` 4종/36%, `오락·문화` 5종/47%도 같은 부류
      - [x] **시장별 어휘 문제가 데이터로 확인됐다** — `전기·가스`(KOSPI)와
            `전기·가스·수도`(KOSDAQ)가 **둘 다 KSIC 35다.** 라벨만 다르다.
            금융도 KOSPI는 `은행`(64)·`증권`(66)·`기타금융`(64)로 쪼개는데
            KOSDAQ `금융`은 64와 66을 **같이** 담는다
      - [x] **판정** — 목적 A는 "업종이 실제로 바뀐 기업이 몇 개인가"를 재는 것이다.
            **가짜 변경(시장 이전상장)을 더하고 진짜 변경(같은 KRX 업종 안의 KSIC 이동)을
            빼는 자로는 그 수를 못 잰다.** ①(과거 기준일 조회)은 볼 필요가 없어졌다
      - [ ] **유일하게 남는 선택지** — 1:1이 성립하는 11개 업종(표본의 36.4%)에서만
            변경률을 재고 한계를 명시한다. **다만 그 부분집합이 편향돼 있다** —
            깨끗한 쪽은 제약·비금속·부동산·통신·보험처럼 **단일 업종 기업**이고,
            업종 변경이 실제로 잦을 다각화 제조·서비스가 애매한 쪽에 몰려 있다.
            **과소 추정이 구조적이다**
      - [x] **교차표 이전에 나온 구조적 결함 둘** (CSV 실측)
        - [x] **① 업종 어휘가 시장마다 다르다.** KOSPI 26종 · KOSDAQ 24종 · 공통 21종.
              **KOSPI는 금융을 `은행`·`증권`·`보험`·`기타금융` 넷으로 쪼개는데
              KOSDAQ은 `금융` 하나다.** `전기·가스`(KOSPI) vs `전기·가스·수도`(KOSDAQ)도
              라벨만 다르다. 한쪽에만 있는 업종에 **274종목(9.9%)**이 들어 있다 →
              **시장 이전상장이 업종 변경으로 오인된다.** 목적 A가 재려는 바로 그 수를 오염시킨다
        - [x] **② KRX 업종이 KSIC보다 거칠다.** 2,763종목에 **29종**뿐이고
              `전기·전자` 400 · `IT 서비스` 254처럼 큰 덩어리가 있다.
              KSIC 2자리 중분류는 43그룹이다 → **KSIC 중분류 변경의 일부는
              KRX 업종에서 아예 안 보인다.** 목적 A를 KRX로 재면 **과소 측정**이다
        - [x] 곁가지 — **우선주 113건이 섞여 있다**(끝자리 5·7·9·K·L).
              법인 단위인 `induty_code`와 조인할 때 걸러야 한다
      - [x] ~~교차표는 sj2 복구 후~~ → **로컬 표본으로 끝냈다** (위 판정)
    - [ ] **(신규) 대안 — KRX를 거치지 말고 `induty_code` 자체를 버저닝한다.**
          목적 A의 앞으로 절반은 **crosswalk 없이** 풀린다.
          `dart_corp_master.induty_code`는 upsert라 **덮어써서 이력이 없다.**
          `profile_raw`에 vintage를 남기거나 이력 테이블을 두면
          **KSIC 원본 그대로** 변경을 잡는다 — 분류 체계 불일치가 원천적으로 없고,
          이미 도는 `dart sync-corp-profile`을 주기 실행하는 것 말고 추가 수집이 없다.
          한계는 KRX 안과 같다 — **소급이 안 되고 앞으로만 쌓인다**
    - [ ] KRX 데이터 상품 / Koscom datamall — **두 페이지 다 500.** 전화 문의(1577-0088)
  - [ ] `common sync --sources krx` 11요청 중 지수 3건은 Open API로 옮길 수 있다.
        **등락종목수·거래대금 8건(`MDCSTAT01501`)은 대응 여부 불명** — K-2와 같이 확인
- [ ] **K-7 (부수) 원주가 전환 검토** — [`poc/krx_open_api.md`](poc/krx_open_api.md) §5.
      Open API는 **KRX 원주가**를 준다. naver 소급 재작성이 결정 4 vintage 결함의 원인이므로
      **원주가 + `listed_shares` 차분으로 조정 계수를 우리가 PIT 계산하면 결함이 사라진다.**
      V-1b 전량 재수집이 **불필요**해진다 → **V 묶음 실행 전에 이걸 먼저 본다**
  - [x] **비용이 0에 가깝다** (2026-08-18) — 원주가가 **별도 엔드포인트가 아니다.**
        `stk_bydd_trd` 응답에 `TDD_OPNPRC`·`TDD_HGPRC`·`TDD_LWPRC`·`TDD_CLSPRC`가
        시총·상장주식수와 **같이 온다.** N1-8을 돌리면 원주가 시계열이 따라온다
  - [ ] 결정 — 원주가를 `daily_market_cap`에 함께 넣을지 별도 테이블로 뺄지.
        **K-4 스키마를 정할 때 같이 정해야 한다** (나중에 바꾸면 재백필이다)

---

## O 묶음 — 탐지 (신규 축, 2026-08-15)

이번 세션 결함이 **전부 값싸게 탐지 가능한데 아무도 안 보고 있었다.** 개별 결함을 고치는
것보다 근본적인 건 11번째 결함이 사람이 우연히 보지 않아도 드러나게 하는 것이다.

- [x] **O-1 `impossible_daily_return`** — 가격제한폭 ±30% 초과. `daily_ohlcv`에 등록.
      실행 확인: 2016 구간 1건 검출
- [x] **O-2 `pit_universe_coverage`** — as-of 스냅샷 대비 커버리지.
      `daily_ohlcv`·`daily_market_cap`·`krx_security_flow_raw`에 등록.
      **손으로 잰 14.216%~13.911%를 정확히 재현**
- [x] **O-3 `validate`를 as-of 유니버스 기준으로** — `get_universe_as_of()` 추가.
      오늘 날짜는 as-of가 오늘일 뿐인 특수 케이스가 됐고, 스냅샷이 없으면 active로 폴백.
      **실행 확인: 2016-06-30에 286/2,056 검출** (이전 코드는 0을 보고했다)
  - [x] `universe_size`와 `universe_source`를 counts/params에 기록 —
        없으면 `missing=0`이 "완전"인지 "빈 유니버스"인지 구분이 안 된다
- [x] **O-4 universe count drift** — `validate.py`의 `# TODO` 구현.
      **source별로 비교**한다(백필 스냅샷과 라이브는 수집 경로가 다르다). `--universe-drift-pct`
- [x] **O-5 `run_zero_yield`** — `success`인데 rows 0인 run 비율. `ingestion_runs`에 등록.
      요청을 시도한 run만 센다(idle skip 제외)
- [x] **O-6 `_execute_values_counted`** — `execute_values(page_size)` 뒤 `cur.rowcount`가
      **마지막 페이지만** 반환하는 버그. **19개 호출 지점 + 손으로 페이징하던 3곳**을
      공용 헬퍼로 교체. 헬퍼 불변식 테스트 4건
- [x] **O-7 `capital_change_direction_balance`** — I3 부류.
      "카탈로그가 조용히 매칭 실패"와 "그 범주가 실제로 없음"이 구분되게
- [ ] **O-8 탐지를 스케줄에 편입** — **D 그룹 의존**
  - [x] **`ops freshness-report --fail-if-stale`** (2026-08-15) — 게이트 신설.
        기존 명령은 출력만 하고 **무조건 exit 0**이라 스케줄에 넣어도 알람이 되지 않았다.
        `evaluate_staleness`가 각 raw 도메인의 최신 행을 KRX 거래일 달력과 비교한다
  - [x] 거래일 예산(`daily_ohlcv`·`daily_market_cap`·flow 그룹·KRX/FDR/PYKRX common)과
        달력 예산(ECOS·FRED)을 분리. **매일 걸리는 게이트는 아무도 안 본다**
  - [x] `daily_market_cap`도 게이트에 포함 — 일별 잡이 될 예정이라 빼면 같은 구멍을
        한 칸 옆에 다시 만든다. 빈 테이블은 stale로 센다 → **N1-8 이후에 스케줄**
  - [ ] Cronicle 이벤트 등록 (23:00, 저녁 수집 창 종료 후)
  - [ ] `profile`을 정기 스케줄에 편입 — 지금 수동 wrapper뿐
- [x] **O-9 원천 차단 정지 조건 `ConsecutiveFailureGuard`** (2026-08-15, v0.9.4) —
      탐지가 아니라 **정지**다. 모든 수집기가 실패를 항목 오류로 기록하고 넘어가서,
      원천이 거부하기 시작해도 대상 목록을 끝까지 때렸다. N1은 6,000 슬라이스 × `@retry` 4회
      = **24,000 요청**. 임계 5에서 **20 요청**에 멈춘다
  - [x] 연속 실패만 세고 **성공 한 번에 리셋** — 흩어진 실패로는 안 걸린다
  - [x] 연속 실패 사이 백오프 증가 + 상한. `@retry`는 항목 안에서만 백오프하고 리셋되므로
        이게 없으면 전부 실패하는 동안 페이스가 그대로다
  - [x] 차단 시 run은 `FAILED` + `source_blocked` — 스케줄러가 절반을 완료로 보지 않는다
  - [x] `--max-consecutive-failures` (기본 5, 0이면 비활성) — market-cap / snapshots / prices
  - [x] **자체 결함 1건**: `rate_limit_seconds=0`일 때 백오프가 1초로 기본값이 붙어
        "스로틀 끔"이 조용히 "최대 60초 대기"가 됐다. 설정된 페이스에 비례시켜 수정
  - [x] 대량 백필 래퍼 기본값을 prod의 `0.1s`가 아닌 **`0.4s`**로. 한 호출이 시장 전체
        (~1,700행)를 반환해 종목당 호출 기준으로 튜닝된 값보다 무겁다
- [x] **O-10 CLI 핸들러 인자 정적 검사** (2026-08-15, v0.9.5) —
      v0.9.4의 `prices backfill`과 `prices market-cap-backfill`이 **첫 줄에서 죽었다.**
      `AttributeError: 'Namespace' object has no attribute 'include_delisted'`.
      T 묶음이 `--include-delisted`를 `--universe-scope`로 바꾸면서 f-string 안에 남은
      참조다. **파이프라인에서 물량이 가장 큰 명령 둘**이고, 갭 복구로 일별 체인을
      돌려보고 나서야 드러났다
  - [x] 원인은 오타가 아니라 **핸들러가 파서에 없는 인자를 읽는 걸 막는 게 없다**는 것.
        유닛 테스트는 서비스를 직접 부르므로 CLI 배선을 한 번도 통과하지 않는다
  - [x] 파서 트리를 걸어 각 leaf 명령의 dest 집합을 모으고, 핸들러를 AST로 훑어
        `args.X` 참조가 그 부분집합인지 검사. **수정 전 소스에서 실패하는 것 확인**
  - [x] market-cap에는 scope 개념이 없다 — 슬라이스가 특정일의 시장 전체라
        `UniverseScope`가 고를 종목 집합이 없다
- [x] **O-12 중단된 백필이 exit 0으로 끝났다** (2026-08-15, v0.9.6) —
      N3 1차 실행이 5연속 실패에서 **설계대로 정지**했고, 146개 중 23개만 쓰고,
      이유를 출력하고, **exit 0으로 끝났다.** Cronicle은 성공으로 기록했다.
      **스케줄러가 볼 수 없는 정지 조건은 정지 조건이 아니다**
  - [x] 가드가 걸린 3개 명령 중 종료 경로가 있던 건 `prices backfill` 하나뿐이었고
        그것도 `pipeline` 키만 봤다
  - [x] `_exit_if_run_aborted` — `source_blocked`·`pipeline`은 **partial이 아니다.**
        남은 작업을 아예 시도하지 않았기 때문이다. 항목별 실패는 그대로 정상 종료
        (종목 하나 실패가 장애로 읽히면 안 된다)
  - [x] 가드 테스트 — 중단 가능한 핸들러가 이 함수를 부르는지 AST로 검사.
        **이번에 빠뜨린 방식 그대로 다음 명령이 빠뜨리는 걸 막는다**
- [x] **O-14 KRX가 IP를 제한했다 — 방어 장치를 다시 설계했다** (2026-08-16, v0.9.10)
  - [x] **KRX 안내 그대로**: `자동화 수단을 통한 비정상 대량 조회가 감지되어 해당 IP의
        접속이 일시적으로 제한되었습니다`. 추정이 아니라 확인된 사실이다.
        더미 자격증명으로 확인 — 계정 문제가 아니다. 약 9시간 뒤 자동 해제됐다
  - [x] **사실 정정**: 일별 가격 수집(2,763건/저녁)은 KRX가 아니라 **naver**로 간다
        (`adjusted=True` → `naver.get_market_ohlcv_by_date`). KRX 트래픽이 아니다
  - [x] **① 실패가 로그인을 증폭시켰다 (가장 나쁜 것).** `webio`가 import 시점에
        로그인하고 `lru_cache`는 예외를 캐시하지 않는다 → 호출마다 import가 다시 돌고
        import마다 로그인 플로우 3요청이 새로 나갔다. **20분 실행 하나에서 실패 32건 →
        로그인 96요청, 유효 작업 0.** 막힌 상태에서 로그인을 두드리는 건 자동화 탐지가
        가장 민감하게 보는 행동이다.
        → 실패를 캐시하고 쿨다운을 60s→300s→900s→3600s로 키운다.
        일시적 장애는 1분이면 회복하고, 실제 차단은 시간당 4회로 줄어든다.
        세션 refresh도 같은 쿨다운을 공유한다 — 같은 엔드포인트로 가는 다른 문이다
  - [x] **② B등급에 에러 백오프가 없었다.** A등급(MDC)은 에러 후 45~180초 쉬는데
        pykrx 경로는 그냥 진행했다. 게다가 `@retry(4회, 0.5초 간격)` — 원천이 힘들어하는
        바로 그 순간에 0.5초 간격으로 4번 때린다
  - [x] **③ 같은 호스트에 두 개의 페이스가 있었다.** A등급 1.5~4.0초 vs
        B등급 배포 래퍼 0.4초(.env 기본 0.1초). **148배 차이**
  - [x] **②③ 통합 수정**: `backfill_market_cap`·`backfill_universe_snapshots`가
        A등급과 **같은 `HumanThrottle`**을 쓴다. 같은 `KRX_*` 설정에서 CLI가 만든다.
        재시도는 4회 즉시 → **1회, 에러 백오프 이후**. 차단된 원천 비용이 가드당
        12요청 → 6요청. 거래일의 빈 유니버스도 에러로 보고 백오프한다
        (`dataframe_empty_handler`가 차단과 "데이터 없음"을 같게 만들기 때문)
  - [x] 래퍼가 페이스를 고정하지 않는다. per-run override는
        `--min-delay-seconds` / `--max-delay-seconds`뿐
  - [x] 실환경 검증 — 단일 날짜 5초 → **10.3초**, 스냅샷 정상 저장
- [x] **O-15 청크 루프가 서킷 브레이커를 무시했다** (2026-08-16) —
      연 단위 청크는 각각 별도 프로세스라 **로그인도 로그인 실패 쿨다운도 새로 시작한다.**
      한 청크가 `source_blocked`로 죽어도 루프는 다음 연도로 갔다.
      09:06 실행에서 2014~2018은 성공하고 **2019~2024가 각각 8분씩 실패**하며
      연도당 로그인 1회씩, 수집 0. **서킷 브레이커는 run을 멈추는데 run들의 루프를
      멈추는 건 아무것도 없었다** — 같은 결함이 한 계층 위에 있었다
  - [x] 첫 실패 청크에서 루프 종료. 저장된 연도는 skip되므로 재실행이 그 지점부터 이어진다
- [x] **O-16 `.env`에 키를 추가했더니 CLI 전체가 죽었다** (2026-08-18) —
      `AUTH_KEYS`·`DATAGO_KEY`를 `.env`에만 넣고 `Settings`에 필드를 안 만들었다.
      **pydantic-settings는 모르는 키를 무시하지 않고 거부한다**(`extra_forbidden`) →
      `get_settings()`가 예외를 던지고 **모든 명령이 첫 줄에서 죽는다.**
      유닛 테스트도 10건 실패했다 — 정확히 O-10과 같은 모양이다
  - [x] **이 동작 자체는 옳다.** 오타 난 설정이 조용히 무시되는 것보다 낫다.
        결함은 **자격증명을 `.env`에 넣는 것과 코드에 선언하는 것이 따로 논다**는 것이고,
        **아직 안 쓰는 키도 반드시 선언해야 한다**는 뜻이다
  - [x] `krx_openapi_auth_keys_raw`(alias `AUTH_KEYS`) + `datago_api_key`
        (alias `DATAGO_KEY`) 추가. 파싱은 OpenDART 다중키와 공유(`_split_key_list`).
        전체 **1,143 통과**
  - [x] 회귀 테스트 3건 — `AUTH_KEYS` 콤마 파싱·중복 제거, `DATAGO_KEY` 퍼센트 이스케이프
        보존, **모르는 env 이름이 거부되는지**(이 결함을 만든 규칙을 명시적으로 고정)
- [x] **O-13 중단된 래퍼가 컨테이너를 남긴다** (2026-08-16) —
      S-1을 04:00 체인 전에 멈추려고 중단했더니 **락은 풀렸는데 컨테이너가 20분을 더 돌았다.**
      락은 호스트 프로세스에, 작업은 컨테이너에 있어서다. 두 번째 수집기가 그 옆에서 시작됐다
  - [x] 컨테이너에 락 도메인 이름을 붙였다. **락을 잡았다는 건 형제가 없다는 증명**이므로
        그 이름의 컨테이너가 있으면 고아가 확정 → 회수. TERM/INT trap은 정상 중단을,
        회수는 SIGKILL을 덮는다(trap으로는 불가능하다)
  - [x] 락 없는 경로는 익명 유지 — 락이 없으면 배타성 증명이 없어 고정 이름이 틀린다
- [x] **O-11 (발견) 호스트 다운이 무탐지로 지나갔다** — sj2-server가 2026-08-14
      16:37~00:08 다운. **18:30 KRX 체인과 20:30 common이 통째로 누락**됐고
      `daily_ohlcv`·`krx_security_flow_raw`·`common_feature_observation_raw`가
      전부 08-13에 멈춰 있었다. 아무도 몰랐다
  - [x] 재부팅은 systemd 정상 종료(장애 아님). 현재 22시간 연속 가동
  - [ ] **탐지가 없다는 게 결함이다.** `ops freshness-report`는 있는데 스케줄에 없다 → O-8/D-5

---

## T 묶음 — 대상 해석 구조 (신규 축, 2026-08-15)

플래그를 opt-in으로 두면 다음 수집기가 또 잊는다. 진짜 문제는 **"수집 대상이 무엇인가"를
정하는 단일 지점이 없다**는 것이었다 — 여섯 서비스가 각자, 그리고 전부 같은 방향으로 틀렸다.

- [x] **T-1 `UniverseScope` + `service/collection_targets.py`** —
      `resolve_dart_targets` / `resolve_price_targets`. 대상 해석을 한 곳으로
  - [x] `CURRENT`(일 sync) / `HISTORICAL`(백테스트용 백필)를 **이름으로** 구분.
        `include_delisted` bool을 대체했다 — bool은 메커니즘을, enum은 의도를 말한다
- [x] **T-2 서비스 7개를 resolver로 교체** — DART 5 + prices + flows.
      CLI는 `--universe-scope current|historical`로 통일
- [x] **T-3 가드 테스트** — `service/` 아래에서 유니버스 accessor를 직접 부르면 실패.
      **작성하자마자 내가 못 본 4곳을 더 찾아냈다**(flows, xbrl 접수타깃 경로, validate 폴백,
      그리고 오탐 1건). 예외 3곳은 테스트에 근거와 함께 명시
- [x] **T-4 `01_implementation_checklist.md` §1에 7번째 등록 지점 추가**

---

## S 묶음 — 생존편향 (신규 축, 2026-08-15 발견)

N6 대상 집합을 확인하다 나왔다. 측정: [`poc/survivorship_gap.md`](poc/survivorship_gap.md)

**신규 원천이 아니라 기존 수집의 대상 집합 결함 수정이다.**
`dart_corp_master`의 ticker 매핑 법인 3,959개 중 **1,330개가 상폐 법인**인데,
모든 raw 테이블에서 커버리지가 **2.0~2.3%**다. 원인은 모든 DART 서비스가
`get_dart_corp_master(active_only=True)`를, `backfill_daily_prices`가
`get_active_stocks()`를 쓰기 때문이다.

**복구 경로가 둘 다 열려 있다** — corpCode.xml이 상폐 법인의 `stock_code`를 유지하고,
pykrx가 상폐 종목 OHLCV를 상폐일까지 준다(5종목 중 4종목 확인).

- [x] **S-1 DART 대상 확장 — 구현 완료** (2026-08-15)
  - [x] `get_dart_corp_master(include_delisted=True)` — **`active_only=False`가 아니다.**
        전자는 ticker 없는 11만 법인까지 연다. 이건 ticker를 가진 적 있는 3,959건
  - [x] `--include-delisted`를 DART 명령 5개에 노출 (corp-profile / financials /
        share-info / xbrl / filings)
  - [x] **DS001/DS002가 상폐 법인에도 응답한다** — 5종목 전부 `status=000`
  - [x] 테스트 추가 + fake storage 7곳 갱신. 전체 **1,010 통과**
  - [x] **`sync-filings` 실행 중** (2026-08-16 00:05 시작, 이벤트 `emsuia7tg0e`).
        측정: ticker 매핑 3,959 법인 중 **1,302개가 접수 이력 0건**이고 전부 상폐 법인이다.
        `dart_filing_receipt_raw`는 120만 행 · 2,657 법인 · 2015-01-02 ~ 2026-08-12
  - [x] `deploy/prod/bin/dart-sync-filings.sh` 신규 — 기존에는 `dart-backfill-all-years.sh`의
        마지막 단계로만 돌아서 **자기 대상 집합으로 돌릴 방법이 없었다**
  - [ ] `sync-financials` / `sync-share-info` / `sync-xbrl`
    - [x] **잔여 규모 측정** (2026-08-16) — 대상 3,959 법인 대비 현재 커버리지다.
          **약 1,300 법인이 빠져 있고 그게 정확히 상폐 법인 수(1,302)와 맞는다**

      | 테이블 | 법인 수 |
      |---|---:|
      | `dart_corp_master` (ticker 보유) | 3,959 |
      | 〃 그중 상폐 | 1,302 |
      | `dart_filing_receipt_raw` | **3,490** ← S-1 완료분 |
      | `dart_share_count_raw` | 2,653 |
      | `dart_financial_statement_raw` | 2,608 |
      | `dart_xbrl_fact_raw` | 2,608 |

    - [ ] **KRX와 무관하다.** OpenDART만 쓰므로 **지금 바로 실행 가능**
  - [ ] **N6보다 먼저다.** 편향된 대상으로 8.4만 호출을 돌리면 의미가 없다
- [ ] **S-2 상폐 종목 가격 백필** — `daily_ohlcv`도 `get_active_stocks()`로 대상을 잡으므로
      같은 확장이 필요. **결정 4의 재수집 PR과 같은 작업**이라 한 번에 푼다
  - [x] **진짜 blocker를 찾았다 — 플래그 문제가 아니었다** (2026-08-16).
        `UniverseScope.HISTORICAL`은 "stock_master의 모든 행"을 정확히 준다. 그런데
        **`stock_master`가 2026-04부터의 것만 안다.** 그 전에 상폐된 종목은 행이 없다
    - [x] ticker를 가졌던 3,959 법인 중 **1,299개가 `stock_master`에 아예 없다**
    - [x] 스냅샷 24개만으로도 **372 ticker가 master에 없고, 정확히 그 372개가
          `daily_ohlcv` 0행이다.** 두 수가 일치한다 →
          **`stock_master` 소속 여부가 가격 커버리지를 결정한다**
    - [x] **`--tickers`로도 못 닿는다** — allowlist가 같은 테이블에 필터링된다.
          어떤 플래그도 master 너머로 못 간다
  - [x] **`universe backfill-master`** (v0.9.8) — master가 자기 정의대로 채워지게 한다.
        N3 월말 스냅샷이 소스다(가격과 같은 원천, ticker별 market 보유).
        복구 행은 항상 `DELISTED` — master에 없다는 건 라이브 sync가 본 적 없다는
        적극적 증거다. `sync_universe`는 `get_active_stocks`로 diff하므로 영향 없다
    - [x] 한계 명시: 월말 스냅샷은 **한 달 안에 상장·상폐된 종목을 못 본다**
  - [ ] 실행 — **N3 완료가 선행 조건.** N3가 60/152에서 멈췄으므로 **K-4 → N1-8까지 대기**한다.
        지금 돌리면 2019년 이후 상폐 종목이 통째로 빠진 master가 만들어진다.
        **2026-08-18: 그 대기가 "무기한"에서 "K-4 구현 기간"으로 바뀌었다**
- [ ] **S-3 상폐 시점 확정** — N3 월말 스냅샷 diff + `daily_ohlcv` 마지막 거래일
- [ ] **S-4 편향 크기 측정** — 같은 피쳐를 편향 표본과 복구 표본으로 각각 돌린 IC 차이.
      **이게 생존편향의 크기 그 자체다**

---

## 결정 4 후속 — `daily_ohlcv` 조정 vintage 복구

N1 PoC 중에 드러난 별건이다. 측정: [`poc/n1_adjusted_price_vintage.md`](poc/n1_adjusted_price_vintage.md)

`daily_ohlcv`는 일관된 조정주가 시계열이 아니다. 백필 이후 발생한 분할이 과거 행에 소급
반영되지 않아 **분할일에 가짜 수익률**이 박힌다(2026-04-11 이후 279건 / 252종목,
비율 5.0·10.0·2.0). **발행된 결과는 안전하지만 앞으로는 매달 ~70건씩 쌓인다.**

- [x] **V-0 (신규) 선행 PR** — `prices backfill`에 재수집 경로가 **없었다.**
      비증분 경로가 gap detection이라 **구멍만 메우고 기존 행은 갱신하지 못한다**
  - [x] `--refetch` — 구간 전체 재수집. `--incremental`과 상호 배타(조합 시 아무것도
        재수집하지 않으면서 성공처럼 보인다)
  - [x] `--include-delisted` + `storage.get_stocks()` — **S-2도 같이 푼다.**
        `--tickers`가 active 결과를 거르므로 상폐 종목은 이름을 대도 못 잡았다
  - [x] **부수 결함 수정** — 예외 처리가 `result.errors`를 안 채워 검증 실패인데도
        CLI가 성공 메시지 + exit 0. 스케줄러가 초록불로 본다
  - [x] 테스트 4건 추가, 전체 **1,009 통과**
- [x] **V-1 로컬 검증** — 오염 20종목 12,700행을 심고 `--refetch` 실행:
      **불가능 수익률 21 → 1** (20/21 해소)
  - [x] 잔여 1건(003060)은 **15:1 액면병합**. 재수집해도 남는다 =
        naver가 조정하지 않는 이벤트. **vintage 문제가 아니다**
  - [x] 그리고 그 케이스가 **결정 2의 `k × p ≈ 1` 판정에 정확히 걸린다**(0.925)
- [ ] **V-1b 전량 실행** — 300종목(백필 이후 252). **prod 배포 후**
  - [x] **KRX 차단과 무관하다** — `prices backfill`은 `adjusted=True`라 naver로 간다
  - [ ] **다만 K-7을 먼저 본다.** Open API 원주가로 옮기면 조정을 우리가 하게 되고
        **소급 재작성 자체가 없어져 V-1b·V-2·V-3이 통째로 불필요해진다.**
        지금 300종목을 재수집해도 다음 분할에 또 오염된다 — 증상 처리다
  - [x] **2026-08-18: 이 판단이 더 강해졌다.** 원주가가 N1-8에 딸려 오므로
        K-7의 추가 수집 비용이 사실상 없다. **V-1b를 지금 돌리는 근거가 약하다**
- [ ] **V-2 재측정** — 모집단은 **상승 방향(병합·감자)이 다수**(279 중 229)라
      표본의 95% 해소율을 그대로 외삽하지 않는다. 전량 실행 후 확정
- [ ] **V-3 정식 트리거** — `listed_shares` 일별 차분 → 해당 종목 이력 재수집. **N1 백필 후**
- [ ] **V-4 스케줄 편입** — D-5. 증분 수집이 corporate action을 모르는 구조적 결함을 닫는다

---

## 후순위 — 지금 하지 않는 것

- [ ] **N9 공시 원문 텍스트** — [`09_w4_filing_text.md`](09_w4_filing_text.md) §4 착수 조건 4개 미충족
- [ ] **DS005 주요사항보고서** — N9 검토 시점이 오면 **그 전에 먼저 본다**
- [ ] `dart_filing_receipt_raw` 기반 공시 활동 피쳐 4종
      — **신규 수집이 아니므로 이 디렉터리 범위 밖.** `11_feature_taxonomy.md` §9.4

---

## 검정 트랙 (수집과 분리)

수집과 섞으면 안 되는 것들이다. `01` §6 규율.

- [ ] 새 원천 피쳐는 **새 config로 별도 사전등록** — 기존 `config_hash=e55c3046…`를 건드리지 않는다
- [ ] BH 모집단 증가가 기존 발견 45개(`m_ab=113`)의 문턱을 올린다는 점을 감안해 family 추가를 묶는다
- [ ] **재실행은 묶어서 한 번** — Phase B 5시간 30분, Phase A 4시간 41분(실측)
- [ ] holdout은 여전히 **한 번만** — feature·horizon·variant·interaction 선택이 전부 끝난 뒤

---

## 개정 이력

| 날짜 | 내용 |
|---|---|
| 2026-08-15 | 최초 작성. `00`~`09` 계획을 실행 체크리스트로 분해 |
| 2026-08-16 | **K 묶음 신설.** KRX 차단이 페이싱이 아니라 약관 위반임을 확인하고(K-0), Open API 서비스 목록을 대조했다(K-1). N1·N3는 옮길 수 있고 `flows`·N4·N7은 갈 곳이 없다. N3-5b는 60/152에서 종료 판정, N4·N7에 차단 표기, S-2·V-1b에 선행 조건 갱신 |
| 2026-08-16 | **K-1b 경로 인벤토리.** `universe sync`가 FDR 라이브러리 안에서 같은 MDC 엔드포인트를 치고 있었다 — 회계에 없던 KRX 수집기다. **문이 셋**(MDC 로그인 / pykrx 로그인 / FDR 익명)이라 K-5 폐기 범위를 넓혔다. `MDCSTAT01501` 응답 키가 `OutBlock_1`이라는 K-2 간접 근거도 기록 |
| 2026-08-16 | **외부 리뷰 반영.** 사실 주장 3건이 틀렸다 — ① **`prices backfill`도 KRX를 친다**(pykrx import가 로그인 발생) ② **FDR 경로 분석 오류**(GitHub CSV 캐시를 읽는다) → K-2 간접 근거 철회 ③ **"53요청"은 논리 작업 수**. 판단 4건 수정 — **"flows는 급하지 않다"가 K-0과 충돌**(K-0b 신설) · **N4 cross-taxonomy 미해결** · **N7 검증 범위는 B/M·E/P뿐이고 I7을 못 본다** · **I6은 재수집 대상이 아니다**(선행조건에서 제외). **K-2는 공식 스키마로 닫혔다**(`MKTCAP`·`LIST_SHRS`·`ACC_TRDVAL`). 신설 — K-0c 약관 게이트 · K-5 확장(pykrx import·폴백·조건부 경로) · K-6f 전환 조건 6개 |
| 2026-08-16 | **KRX 인증키 미발급 — 8/18~19 예상**(대체공휴일 8/17까지). "키 없이 가능한 것" 절 신설. **정정: I1은 이미 끝나 있었다** — 수정·결정성 게이트·Phase B 재실행·결과 갱신까지 완료돼 `fin_value_z` IC가 +0.0348 → +0.0692로 두 배가 됐다. K-6d의 "I1·I7·I6 선행"에서 **남은 선행은 I7·I6 둘**이다. S-1 잔여 규모도 측정: 3,959 대상 중 financials·xbrl **2,608**, share_count **2,653** — 약 1,300 법인 누락 |
| 2026-08-16 | **K-6d N7·N4 조사 — 둘 다 축소.** N7은 대체재보다 근거가 먼저 문제였다: 진단 근거 셋(I1=SQL 버그 / I7=canonical 매핑 / I6=정정분 재수집)이 **전부 다른 작업으로 이미 귀속돼 있다.** 남는 매핑 검증은 횡단면이라 KIS로 된다. N4는 **목적 B가 N2로 이미 돌아가고**, 목적 A는 **업종분류 현황 CSV가 지수 구성종목보다 맞는다**(편입/제외 잡음이 없다). N7-2~7 · N4-2~8 취소. 별건으로 **공공데이터포털 `getStockPriceInfo`가 K-2를 우회**한다 |
| 2026-08-16 | **K-6a `flows` 대체 조사.** 7 metric 중 5개는 KIS Developers로 대체 가능, **외국인 보유주식수·공매도 잔고 2개는 갈 곳이 없다.** 공공데이터포털은 답이 아니었다(민간 파생 스코어뿐). 그리고 **급하지 않다** — 2007년부터의 이력을 이미 갖고 있어 필요한 건 forward-fill이고, 하루 38요청은 차단 규모가 아니다 |
| 2026-08-18 | **N4 분기점 ② 판정 — crosswalk는 못 만든다. 목적 A를 접는다.** sj2가 막혀 있어 로컬에서 `dart sync-corp`(1호출) + 층화 표본 **682건**(업종당 최대 30, 29종 전량)으로 `induty_code`를 직접 받아 교차표를 돌렸다(표본 700, 오류 0). **총량은 통과처럼 보인다** — Cramer's V **0.936**, 불확실성 **85.8% 감소**. **개별 종목에서 깨진다** — KRX 업종으로 KSIC 최빈값을 맞힐 확률 **76.9%**, 안정적 1:1 쌍에 드는 종목 **36.4%뿐**. 결정적인 건 **KRX 한 업종이 KSIC 둘을 반씩 담는 구조다**(`전기·전자`=26/28, `운송장비·부품`=30/31, `유통`=46/47, `금속`=24/25, `건설`=41/42) — **그 경계를 넘는 KSIC 변경이 KRX에서 안 보인다.** `일반서비스`는 KSIC 8종에 최빈 45%로 잔여 범주다. 시장별 어휘 문제도 데이터로 확인 — **`전기·가스`(KOSPI)와 `전기·가스·수도`(KOSDAQ)가 둘 다 KSIC 35**다. **가짜 변경을 더하고 진짜 변경을 빼는 자로는 "업종이 바뀐 기업 수"를 못 잰다** → ①(과거 기준일 조회)은 확인 불필요. **신규 대안** — KRX를 거치지 말고 **`induty_code`를 버저닝**하면 앞으로의 변경은 crosswalk 없이 KSIC 원본으로 잡힌다 |
| 2026-08-18 | **N4 — 업종분류 현황 CSV 실물 확인 (사람 다운로드).** 2,763행으로 Open API와 행 수가 맞는다. **교차표는 못 돌렸다** — `induty_code`가 prod에만 있고 sj2가 막혔으며 lake 최신 스냅샷은 컬럼 추가 이전이다. **그런데 교차표 없이 구조적 결함 둘이 먼저 나왔다.** ① **업종 어휘가 시장마다 다르다** — KOSPI는 금융을 은행·증권·보험·기타금융 넷으로 쪼개고 KOSDAQ은 `금융` 하나다. 한쪽에만 있는 업종에 **274종목(9.9%)** → **시장 이전상장이 업종 변경으로 오인된다.** ② **KRX 29종 < KSIC 43그룹** → KSIC 변경의 일부가 안 보여 **과소 측정**된다. 둘 다 crosswalk 품질과 무관하게 목적 A를 깎는다. 곁가지 — **파일명 날짜(20260818)와 데이터 기준일(20260814)이 다르다**(Open API `basDd=20260818`은 0행). 스냅샷을 파일명으로 라벨링하면 조용히 틀린다. 다만 **Open API가 진짜 기준일을 대조해줄 오라클이 됐다** |
| 2026-08-18 | **KRX 경로가 열렸다 — K 묶음의 목적이 달성됐다.** 엔드포인트 **16건 승인**, 실호출 10건 전부 200. **K-2 live 종결** — `MKTCAP`·`LIST_SHRS`·`ACC_TRDVAL`이 실제 값으로 오고 이력이 **2014-06-02까지 닿는다**(1차 N3 실행 실측과 행 수가 맞는다). **계획을 바꾸는 발견 셋** — ① **원주가 4필드가 같은 응답에 딸려 온다** → K-7의 추가 수집 비용이 0이고 V-1b를 지금 돌릴 근거가 약해졌다 ② **휴장일이 `rows=0`**이라 pykrx 때문에 넣은 `alternative=False` 방어가 이 경로엔 불필요하다 ③ **`stk_isu_base_info`가 `SECUGRP_NM`·`KIND_STKCERT_TP_NM`을 준다** → N3-3의 우선주·스팩 필터 질문과 K-5의 `universe sync` 대체 가능성이 같이 풀린다. 무휴식 10요청 거부 0 · 응답 1.3~1.7초 → **6,000 호출 ≈ 2.5시간.** **남은 차단은 N4 하나뿐이고**, 공공데이터포털 우회는 필요 없어졌다 |
| 2026-08-18 | **KRX Open API 인증키 발급 — 그런데 blocker가 안 풀렸다.** 키 2개가 `.env` `AUTH_KEYS`에 있고 **키 자체는 유효하다.** 실호출 5개 엔드포인트가 전부 401인데 본문이 `Unauthorized API Call`이고, 가짜 키의 `Unauthorized Key`와 **다르다** → 키는 통과했고 **엔드포인트 이용 신청(K-3)이 승인되지 않았다.** K-1 §2.1의 함정 그대로다. **K 묶음의 단일 blocker가 "인증키 대기"에서 "이용 신청 승인 대기"로 바뀌었고 사람이 포털에서 신청해야 한다.** 키가 둘이라 한도는 하루 20,000회. 곁가지로 **O-16** — `.env`에 키만 넣고 `Settings`에 필드를 안 만들어 `get_settings()`가 `extra_forbidden`으로 죽었고 **모든 CLI 명령과 유닛 테스트 10건이 실패했다.** 배선(`krx_openapi_auth_keys`·`datago_api_key`)과 회귀 테스트 3건으로 해결 |
| 2026-08-18 | **공공데이터포털 키 발급·실호출 확인 (4요청).** 신청이 **자동승인**이라 대기가 없었고 `DATAGO_KEY`가 동작한다. **필드 15개가 서드파티 목록과 정확히 일치** → 명세 대조 종결. **`mrktCtg`가 있어 pykrx와 달리 시장별 호출이 불필요**하다(호출 절반). **범위 조회 동작 확인**(`beginBasDt`/`endBasDt`) → 날짜별 6,000회 시나리오 회피. **그런데 이력이 2020-01-02부터다** — N1-8 목표 구간의 거래일 **약 54%**만 덮는다. **정정: "지연 자체를 무력화한다"는 절반만 맞다.** 다만 백필 순서가 "2024~현재 먼저"라 **1단계 전체 + 2020~2023까지 인증키 없이 가능**하고 V1~V8 검증도 전부 돌린다. 남는 건 2014-06~2019 뒷구간. 함정 기록 — **Encoding 키는 URL에 raw로** 넣는다(`params=`로 넘기면 `%2B`→`%252B` 이중 인코딩). 곁가지로 **스팩 포함 확인**(N3-3 필터 정책 질문 일부 해소) |
| 2026-08-18 | **N6-7 결정 5개 고정 — 권고안대로.** 직원 수는 `sm`(합계), `hc_revenue_per_employee` 분모는 기말, 감사의견은 이진, 부호는 감사 비적정만 `−` 고정. **⑤ 합병·분할 보정은 규칙을 새로 세웠다** — 증거(`합병등종료보고서(분할\|합병)`) AND 크기(\|YoY\| ≥ 30% = 2σ)면 결측, 증거만 있으면 플래그. **그 과정에서 PoC 권고안이 틀린 원천을 지목했다는 게 드러났다** — `dart_capital_change_raw`로는 **동기가 된 LG화학 물적분할을 못 잡는다**(모회사 주식 수가 안 바뀐다). lake 실측: LG화학 2019~2021 분할 흔적 0, `isu_dcrs_stle` 전체 15종에 합병·회사분할 없음(`주식분할` 7,067건은 액면분할). `dart_filing_receipt_raw`의 `합병등종료보고서(분할)`은 2020-12-04로 정확히 찍힌다. 적용 규모 2015~2025 corp-year 43,549 중 **1,023건(2.35%)**. **N6에 남은 blocker는 N6-5 대상 집합(S-1 → sj2) 하나다** |
| 2026-08-18 | **K-5 pykrx 문을 닫았다.** `prices backfill`이 가장 컸다 — 데이터는 원래 naver인데 **모듈 import만으로 KRX 로그인이 나갔다**(`webio.py` 모듈 레벨 `build_krx_session()`). naver 직접 어댑터로 끊었고 **prod 스크립트가 `--source`를 안 넘겨 기본값 전환만으로 닫힌다**. 실측으로 함정 셋 — 선언부 앞 빈 줄 · EUC-KR bytes를 ElementTree가 거부 · **에러 페이지도 well-formed라 root 태그로 갈라야 한다**(없는 종목도 `<protocol />`은 준다). 검증: naver 종가가 **Open API 원종가와 3종목 전부 일치**. `universe sync --source krx-openapi` 신설(2,763종목 실행 성공) — **`ISU_CD`가 여기서는 ISIN이라 `ISU_SRT_CD`를 써야 하고**, **오늘자 파일이 종가 뒤에도 한동안 안 올라와서**(16:04 KST 실측 0행) 기본 as_of는 마지막 게시일로 물러난다. FDR 기록 정정 — **KRX를 두 번 친다(중복 호출)이면서 목록은 GitHub CSV**라, 기존 두 기록이 각각 반만 맞았다. **pykrx 폴백 제거**, **`ALLOW_KRX_SCRAPING` 기본 off**. `shorting` freshness는 `DISCONTINUED_FLOW_METRICS`로 선행 처리. **남은 문 둘은 prod 자격증명 대기**(`flows`=KIS 키, `universe sync`=`AUTH_KEYS`) |
| 2026-08-18 | **N6 코드 완성** — 스키마 2 + 등록 6곳 + 어댑터/포트 + 서비스/CLI + 테스트 41건. **L-1 ledger를 처음 실제로 쓰는 수집기다** — 8.4만 호출이 키 한도로 여러 번 끊기는데 `ingestion_runs.params`는 그걸 못 담는다(no-data 목록이 run당 상한, 최근 run만 읽는다). 포트를 엔드포인트별 메서드가 아니라 **statement enum 하나**로 받았다 — 5개가 인자도 응답도 같아서 메서드 5개는 URL만 다른 복사본이 된다. **연도 씨닝을 서비스에 넣었다**(162,000 → 83,700): `hyslrChgSttus`는 누적, 감사의견은 3개년씩 오므로 요청 연도를 건너뛴다. 테스트는 요청 목록이 아니라 **무손실 여부**를 단언한다. 실 DB 검증 — 005930 FY2023 4호출 34행, 감사의견이 실제로 **제55·54·53기 3개년**으로 왔고, ledger 4슬라이스 `expected==actual`, 재실행 시 전부 skip. **남은 건 N6-5 대상 집합(S-1 → sj2)과 N6-8 백필뿐이다** |
