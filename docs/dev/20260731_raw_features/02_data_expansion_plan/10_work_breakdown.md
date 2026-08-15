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
| 1차 | N1 · N2 · N3 | **진행 중** — PoC 완료 | 병행 가능 |
| 2차 | N4 · N5 | 대기 | N5는 PoC 결과에 따라 강등 가능 |
| 3차 | ledger · N6 · N7 · N8 | 대기 | ledger가 N6 선행 |
| 후순위 | N9 · DS005 | 착수 안 함 | 조건 미충족 |

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
  - [ ] `db sync-remote --full-refresh` 미러링 확인 — **prod에 테이블이 생긴 뒤**(D-2) 가능
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
- [ ] **N1-8 백필 실행** — 2024~현재 먼저, 검증 후 2014-06-02~2023
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

- [ ] **N2-1 PoC** (20 호출) → `poc/n2_company_profile.md`
  - [ ] **`induty_code` 실제 자릿수 분포** — §5 결정에 직결
  - [ ] 2자리 prefix 그룹 수와 그룹당 종목 수
  - [ ] `acc_mt != '12'` 사례가 실제로 나오는가
  - [ ] 지주회사·금융지주가 받는 코드
- [ ] **N2-2 ALTER + 등록 4곳** (새 테이블이 아니라 컬럼 추가)
  - [ ] `sql/postgres_ddl.sql` — 6개 컬럼 + `induty_code` 인덱스
  - [ ] `remote_sync.py` — **`SYNC_TABLE_SPECS` 컬럼 목록에 추가.** 빠뜨리면 조용히 누락되고 테스트가 안 잡는다
  - [ ] `profiling/catalog.py` — `category_cols` / `null_cols`
  - [ ] `export_tables.toml` — `jsonb_columns = ["profile_raw"]`
  - [ ] `docs/database.md`
- [ ] **N2-3 어댑터 + 포트** — `opendart_corp`에 메서드 추가, 다중키 실행기 (`01` §2.2)
- [ ] **N2-4 서비스 + CLI** — `dart sync-corp-profile`, 대상은 **ticker 매핑 3,959건만**
- [ ] **N2-5 테스트** — 픽스처에 `acc_mt='03'`·업종 결측·필드 결측 포함
- [ ] **N2-6 결정 3개 고정 (결과 보기 전)**
  - [ ] 매핑안 A/B/C — **권고: B** (2자리 prefix + 금융·지주 override)
  - [ ] 최소 그룹 크기 — **권고: 20**
  - [ ] 금융·지주 취급 — **권고: 제외가 아니라 별도 그룹**
- [ ] **N2-7 실행** — `dart sync-corp-profile`
- [ ] **N2-8 `definitions/industry_groups.py`** — 순수 코드, Storage 의존 없음
- [ ] **N2-9 `fin_scan.py` 업종 중립 variant** — 기존 경로 유지, **진단 전용**
- [ ] **N2-10 검증 V1~V6 + 후속 판단**
  - [ ] V6 업종별 `fin_value_z` 중앙값 분산 — **이 작업의 핵심 산출물**
  - [ ] V4 `acc_mt != '12'` 종목 수 → **`metric_vintages.py` period_end 하드코딩 수정 여부**
  - [ ] **PIT 금지선이 문서에 남았는가** — scored backtest·acceptance gate·holdout 금지

### N3. PIT 유니버스 백필 — 기존 스냅샷 테이블

상세: [`04_w1_pit_universe.md`](04_w1_pit_universe.md) · 약 290 호출 · **등록 6곳 해당 없음**

- [x] **N3-1 PoC** (N1 PoC와 묶음) → [`poc/n3_pit_universe.md`](poc/n3_pit_universe.md)
  - [x] 과거 시점 응답 — **2014-06까지 정상**
  - [x] **`get_market_ticker_list` vs `get_market_cap_by_ticker` 차집합 = 네 시점 모두 0**
  - [x] `market='ALL'`은 시장 구분을 주지 않는다 → 시장별 호출
  - [ ] 우선주·리츠·스팩 혼입 여부 → 필터 정책 (N3-3 구현 시)
  - [ ] 종목명 조회 비용 → 이름 채우기 방식 결정 (N3-3 구현 시)
- [ ] **N3-2 enum + 스토리지 메서드**
  - [ ] `Source.PYKRX_BACKFILL` — 현재 enum에 없다
  - [ ] `RunType.UNIVERSE_SNAPSHOT_BACKFILL`
  - [ ] `insert_stock_master_snapshot_only` — **기존 `upsert_stock_master`는 `stock_master`도
        갱신한다. 그대로 쓰면 현재 유니버스가 오염된다**
- [ ] **N3-3 서비스 + CLI** — `universe backfill-snapshots`, skip 키 `(as_of_date, source)`
- [ ] **N3-4 테스트**
  - [ ] `stock_master`를 갱신하지 않는다는 명시적 단언
  - [ ] **기존 `sync_universe` diff 회귀 테스트** — 이 PR에서 가장 위험한 부분
- [ ] **N3-5 실행** — 2014-06 ~ 현재 월말
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

- [ ] **D-1** **`sdc-release` 스킬**로 릴리즈 — 버전 범프 → lint+unit → 커밋 → 태그 → sj2 compose
- [ ] **D-2** prod DDL 적용 (`db init`) — 새 테이블·컬럼
- [ ] **D-3** `deploy/prod/bin/` 래퍼 스크립트 추가
- [ ] **D-4** Cronicle **일회성 백필 이벤트** 등록 → **끝나면 삭제** (`common-backfill-2015` 사례)
- [ ] **D-5** Cronicle **정기 증분 이벤트** 추가 — N1 일별, N4 월별, N2·N5 주기적 refresh
- [ ] **D-6** **KRX 계열 exit 75 lock 회피** — 기존 `sdc_daily_krx_*` 스케줄과 시간대 분리
- [ ] **D-7** `docs/operations.md` cron 표·런북 갱신
- [ ] **D-8** 로컬 반영 — `db sync-remote --full-refresh` → `bin/raw-parquet-export-all.sh`
      → `bin/parquet-compute-all.sh`

---

## 2차 — N4 · N5

### N4. 업종 지수 구성종목 — `krx_index_constituent`

상세: [`05_w2_industry_index.md`](05_w2_industry_index.md) · 약 3,600 호출 · 선행: N2

- [ ] **N4-1 PoC** → `poc/n4_index_constituent.md`
  - [ ] **업종 지수 코드 목록 확정** — 규모별·테마 지수는 뺀다
  - [ ] `date` 포맷 (`YYMMDD` vs `YYYYMMDD`)
  - [ ] 2014-05-02 하한이 실제로 걸리는가
  - [ ] 구성종목 합집합의 전체 상장 종목 커버리지
- [ ] **N4-2 스키마 + 등록 6곳** — `date_month` 전략
- [ ] **N4-3 어댑터 + 포트 + 서비스 + CLI** — `universe sync-index-constituents`
- [ ] **N4-4 테스트** — 하한 밖 요청을 **아예 만들지 않는지**
- [ ] **N4-5 결정 3개 고정 (결과 보기 전)**
  - [ ] 업종 지수 코드 목록 — 상수로 박고 나중에 고치지 않는다
  - [ ] 업종 변경 판정 4개 조건 + 3% / 10% 임계값 — **운영 규칙임을 명시**
  - [ ] 미편입 종목 폴백 정책 + `industry_source` 노출
- [ ] **N4-6 실행** — 2014-06 ~ 현재 월말
- [ ] **N4-7 N2 대조 3종** — 커버리지 / 일치도 / **업종 변경 비율**
- [ ] **N4-8 inactive 산업지수 4종 처리** — 활성화 / 재정의 / 삭제 중 택일

### N5. 임원·주요주주 소유 / 대량보유

상세: [`06_w2_ownership_disclosure.md`](06_w2_ownership_disclosure.md) · **볼륨 PoC 전 미정**

- [ ] **N5-1 PoC ★ 다른 무엇보다 먼저** → `poc/n5_ownership.md`
  - [ ] **응답이 전체 이력인가 최근분인가** — 볼륨 100배 갈림길
  - [ ] 최근분만이면 **연도 루프 우회가 불가능**하므로 **3차로 강등**하고 여기서 멈춘다
  - [ ] 한 `rcept_no`에 몇 행이 오는가
  - [ ] 같은 corp 2회 호출 시 행 순서 유지 → `row_ordinal` vs payload hash
  - [ ] `sp_stock_lmp_irds_cnt` 부호 분포 (음수가 실제로 오는가)
- [ ] **N5-2 스키마 2개 + 등록 6곳** — `raw_id_range`, 파티션 `["year(rcept_dt)"]`
- [ ] **N5-3 어댑터 + 포트** — `adapters/opendart_ownership/`, exit 75 재개
- [ ] **N5-4 서비스 + CLI** — `dart sync-ownership`, skip 키 `(corp_code)` + `--refresh-older-than`
- [ ] **N5-5 테스트** — 한 접수에 보고자 3명·증권 2종이 섞인 픽스처
- [ ] **N5-6 결정 4개 고정 (결과 보기 전)**
  - [ ] 피쳐명 `ins_holding_increase` — **"매수"라고 부르지 않는다**
  - [ ] 윈도 {60, 120} 두 개만 사전등록
  - [ ] 정규화 분모 `listed_shares` (N1 선행)
  - [ ] `majorstock`의 `report_resn` — **권고: 경영참여/단순투자 분리**
- [ ] **N5-7 실행 + 검정력 판단** — 관측 수를 먼저 세고, 안 나오면 **피쳐 개발로 넘어가지 않는다**
      (`fin_sue` coverage 0.0000 사례를 반복하지 않는다)

---

## 3차 — ledger · N6 · N7 · N8

- [ ] **L-1 `collection_slice_state` ledger 공통 컴포넌트** (`01` §2.4)
      — **N6 착수 전에 만든다.** 패키지마다 따로 만들지 않는다
  - [ ] 현재 한계 확인: `no_data_request_keys` run당 1,000개 절단, negative cache 최근 20 run

### N6. 직원·임원·최대주주·감사의견

상세: [`07_w3_periodic_report_extras.md`](07_w3_periodic_report_extras.md) · 약 16만 호출 · 3일 · 선행: N3, L-1

- [ ] **N6-1 PoC** (50 호출 미만) → `poc/n6_periodic_extras.md`
  - [ ] **10개 종목 5년치 변동 폭 사전 확인** — 변동이 없으면 이 패키지를 접거나 축소
  - [ ] 같은 요청 2회의 행 순서 안정성 → `row_ordinal` vs payload hash
  - [ ] `hyslrChgSttus`에 변동일자가 있는가 → 분기 수집 예외 판단
  - [ ] 감사의견 코드 체계, 직원 수 필드 구분, 2015년 데이터 존재 여부
- [ ] **N6-2 스키마 2개 + 등록 6곳** — `rcept_no`를 **NOT NULL + UNIQUE에 포함**(vintage 보존)
- [ ] **N6-3 어댑터 + 포트** — `opendart_share_info` 구조 복제
- [ ] **N6-4 서비스 + CLI** — `dart sync-periodic-extras`, 기본 `--reprt-codes 11011`
- [ ] **N6-5 대상 집합 구성** — N3 PIT 유니버스 + `dart_filing_receipt_raw`로 **역사적 상장사**
      (현재 상장사로 잡으면 부실 신호에 생존편향이 그대로 들어온다)
- [ ] **N6-6 테스트**
  - [ ] 정정본 시나리오 — 다른 `rcept_no`가 오면 덮어쓰지 않고 행이 는다
  - [ ] **ledger 기반 exit 75 재개** — 이 패키지에서 가장 중요한 테스트
- [ ] **N6-7 결정 4개 고정 (결과 보기 전)**
  - [ ] 직원 수 정의 (정규직 vs 합계) — 하나로 고정
  - [ ] `hc_revenue_per_employee` 분모 시점
  - [ ] 감사의견 인코딩 — **권고: 이진**
  - [ ] 부호 — 감사 비적정 `−` 고정, 최대주주 지분·직원 증가는 **미고정임을 사전에 적는다**
- [ ] **N6-8 백필** — 연도 분할, `dart-backfill-all-years.sh` **마지막 단계**
- [ ] **N6-9 횡단면 변동 측정** + **final-vintage 한계를 피쳐 문서·evidence grade에 명시**
- [ ] **N6-10 밸류업 공시 분류 가능성 확인** — 수집·분류만, 검정은 나중

### N7. KRX 공식 밸류에이션 — `daily_market_fundamental`

상세: [`08_w3_valuation_and_macro.md`](08_w3_valuation_and_macro.md) 파트 A · 약 6,000 호출

**PR3~PR6은 N1을 끝낸 뒤면 거의 복사다.**

- [ ] **N7-1 PoC** — 컬럼명, **`0` sentinel 실측**, 과거 구간, 시장별 호출 → `poc/n7_fundamental.md`
- [ ] **N7-2 스키마 + 등록 6곳** — `date_month`. **N1과 합치지 않는다**
- [ ] **N7-3 어댑터 + 포트 + 도메인** — `PER/PBR/DIV == 0` → NULL 정규화 (**EPS·DPS는 제외**)
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

- [ ] **N8-1** ECOS 통계표 코드 확인 + **vintage 조회 지원 여부** + 원계열/계절조정 선택
- [ ] **N8-2** **readiness 창 정렬 문제 먼저 정리** — 37개 중 4개만 ready인 상태에서
      시리즈를 늘리면 게이트가 더 어지러워진다
- [ ] **N8-3** `definitions/common_features.py`에 시리즈 추가 + 유닛 테스트
- [ ] **N8-4** `common seed` → `common sync` 실행, 커버리지 확인
- [ ] **N8-5 중복 측정 게이트** — **변환 후·availability 정렬 후** 상관.
      0.8 초과면 **추가하지 않는다** (raw level 상관은 게이트로 쓰지 않는다)
- [ ] **N8-6** inactive pykrx 폴백 3종 처리

---

## 결정 4 후속 — `daily_ohlcv` 조정 vintage 복구

N1 PoC 중에 드러난 별건이다. 측정: [`poc/n1_adjusted_price_vintage.md`](poc/n1_adjusted_price_vintage.md)

`daily_ohlcv`는 일관된 조정주가 시계열이 아니다. 백필 이후 발생한 분할이 과거 행에 소급
반영되지 않아 **분할일에 가짜 수익률**이 박힌다(2026-04-11 이후 279건 / 252종목,
비율 5.0·10.0·2.0). **발행된 결과는 안전하지만 앞으로는 매달 ~70건씩 쌓인다.**

- [ ] **V-1 임시 방어** — 하루 |수익률| > 50%인 **300종목 이력 재수집**
      (`prices backfill --force`, 종목 단위 ~300 호출). **N1과 무관하게 지금 가능**
- [ ] **V-2 재측정** — 재수집 후 잔여 건수가 학습 구간 수준(73건)으로 떨어지는가.
      떨어지면 원인 진단 확정
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
