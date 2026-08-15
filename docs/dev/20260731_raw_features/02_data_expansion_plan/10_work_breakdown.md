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
- [ ] **N2-7b 전량 실행** — 3,959 호출 단일 run. **prod 배포 후**
- [x] **N2-8 `definitions/industry_groups.py`** — 순수 코드, Storage 의존 없음
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
  - [ ] 재개 — 소스가 회복되면 idempotent skip으로 24번째부터 이어간다
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

- [ ] **L-1 `collection_slice_state` ledger 공통 컴포넌트** (`01` §2.4)
      — **N6 착수 전에 만든다.** 패키지마다 따로 만들지 않는다
  - [ ] 현재 한계 확인: `no_data_request_keys` run당 1,000개 절단, negative cache 최근 20 run

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
- [ ] **N6-2 스키마 2개 + 등록 6곳** — `rcept_no`를 **NOT NULL + UNIQUE에 포함**(vintage 보존)
- [ ] **N6-3 어댑터 + 포트** — `opendart_share_info` 구조 복제
- [ ] **N6-4 서비스 + CLI** — `dart sync-periodic-extras`, 기본 `--reprt-codes 11011`
- [ ] **N6-5 대상 집합 구성** — ~~N3 + filing_receipt~~ → **S-1이 선행 조건**.
      `dart_filing_receipt_raw`가 `active_only=True`로 수집돼 **그 자체가 편향돼 있다**
      ([`poc/survivorship_gap.md`](poc/survivorship_gap.md))
      (현재 상장사로 잡으면 부실 신호에 생존편향이 그대로 들어온다)
- [ ] **N6-6 테스트**
  - [ ] 정정본 시나리오 — 다른 `rcept_no`가 오면 덮어쓰지 않고 행이 는다
  - [ ] **ledger 기반 exit 75 재개** — 이 패키지에서 가장 중요한 테스트
- [ ] **N6-7 결정 4개 고정 (결과 보기 전)**
  - [ ] 직원 수 정의 (정규직 vs 합계) — 하나로 고정
  - [ ] `hc_revenue_per_employee` 분모 시점
  - [ ] 감사의견 인코딩 — **권고: 이진**
  - [ ] 부호 — 감사 비적정 `−` 고정, 최대주주 지분·직원 증가는 **미고정임을 사전에 적는다**
  - [ ] **(신규) 합병·분할 보정** — 직원 수 점프가 구조 변경인지 갈라낸다
        (LG화학 −37.7% = 물적분할). `listed_shares` 급변과 대조
- [ ] **N6-8 백필** — 연도 분할, `dart-backfill-all-years.sh` **마지막 단계**
- [ ] **N6-9 횡단면 변동 측정** + **final-vintage 한계를 피쳐 문서·evidence grade에 명시**
- [ ] **N6-10 밸류업 공시 분류 가능성 확인** — 수집·분류만, 검정은 나중

### N7. KRX 공식 밸류에이션 — `daily_market_fundamental`

상세: [`08_w3_valuation_and_macro.md`](08_w3_valuation_and_macro.md) 파트 A · 약 6,000 호출

**PR3~PR6은 N1을 끝낸 뒤면 거의 복사다.**

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
  - [ ] **N6보다 먼저다.** 편향된 대상으로 8.4만 호출을 돌리면 의미가 없다
- [ ] **S-2 상폐 종목 가격 백필** — `daily_ohlcv`도 `get_active_stocks()`로 대상을 잡으므로
      같은 확장이 필요. **결정 4의 재수집 PR과 같은 작업**이라 한 번에 푼다
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
