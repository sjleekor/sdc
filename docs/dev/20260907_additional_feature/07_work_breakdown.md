# 07. 작업 분해 — 체크리스트·순서·규모

- 작성일: 2026-09-07
- 이 문서가 진행 상태를 추적한다. 체크는 **끝나고 검증됐을 때** 채운다. 왜 하는가는 `01`~`06`에 있다.
- 모델 스트림 접점: FS3 인계(F-HS `screen_pass` 뒤), FS0' 통보(F-9.1 뒤).

---

## 0. 상태표

| 패키지 | 내용 | 새 수집 | 상태 | 다음 행동 |
|---|---|---|---|---|
| F-1 | 업종 `induty_code` 버저닝 | OpenDART 월 3,959 | F-1.1~F-1.3 완료(2026-09-07) | **F-1.4 prod 첫 실행 승인 대기** |
| F-2 | 통계적 peer 관계 피쳐 | 없음 | **완료**(2026-09-07, snapshot 2026-08-23) | F-HS-1 대기 |
| F-3 | 업종 관계 피쳐 | 없음(F-1 선행) | 대기(D-F1) | 3개월 변경률 측정 뒤 |
| F-4 | 재무위험·생애주기·전이 | 없음 | **완료**(2026-09-07, snapshot 2026-08-23) | F-HS-1 대기. 5 family는 2020~ 표본 |
| F-5 | `metric_rules` 확장 | 없음(매핑) | 미착수 | **CF 4종 XBRL fallback 먼저**(F-4.6) → 그 다음 태그 커버리지 PoC |
| F-6 | `fin_sue` XBRL 백필 | OpenDART(측정 뒤) | 미착수 | 대상 역산 |
| F-7 | DS005 이벤트·elestock | 있음 | 미착수 (D-F2 확정: `elestock` 시작 / D-F3: PoC 뒤 6종) | DS005 PoC + `elestock` 스키마 |
| F-8 | 매크로 2단계 시리즈 | 시리즈 정의 | 미착수 (D-F5 확정: 2단계 먼저) | ECOS item_code 확정 |
| F-9 | `fin_pit` strict·휴장일·상폐·유니버스·Cronicle | 일부 | F-9.3 명령 준비 완료 | **F-9.3 실행 승인 대기** |
| F-HS | 새 config 사전등록·A→B→AB→C | — | 대기 | F-2·F-4 마트 완료 뒤 |

---

## 1. 순서

```
주차 1~2   F-2 (peer 계산·마트)  ‖  F-4 (fin_risk 마트)  ‖  F-1 스키마·시드·첫 스냅샷  ‖  F-9.3 S-1 잔여 실행 시작(prod, 며칠)
주차 2~3   F-5 PoC  ‖  F-6 대상 측정  ‖  F-7 DS005 PoC  ‖  F-8 item_code 확정·시리즈 추가·백필  ‖  F-9.1 fin_ratios_pit
주차 3~4   F-HS-1: F-2·F-4 사전등록 config → (S-1 잔여·F-6 백필·holidays 보강이 끝났으면 같은 snapshot) A0→A→B→AB
주차 4~    F-8 국면 사전 계산 → Phase C 2라운드(F-4 필요)  ‖  F-7 수집  ‖  F-1 월별 스냅샷 누적
+3개월     D-F1 판정(변경률) → F-3 마트 → F-HS-2
FS3 인계   F-HS-1 screen_pass → 모델 E5
```

- `‖`는 병행 가능. 병목은 **prod OpenDART 호출**(S-1 잔여 4.3만 + F-6 + F-7)이라 키 예산을 순서대로 배정한다: S-1 잔여 → F-6 → F-7.
- 장비: F-2 peer 계산·마트 빌드와 Horizon Scan·모델 run이 겹치지 않게.

---

## 2. 체크리스트

### F-1 업종 PIT (`01`)

- [x] **F-1.1** DDL `dart_corp_profile_history` + 7곳 등록 + `docs/database.md`. `db init` → 테이블 + 인덱스 3개 생성 확인(local `mydb`)
- [x] **F-1.2** 서비스: `sync-corp-profile` 결과를 history에 append(`(corp_code, observed_month)` skip). `RunType` 추가(`dart_corp_profile_history_seed`), 결과 카운터 `history_rows_appended`
      — 옵션 이름은 `--refresh`가 아니라 기존 `--force`다(이 저장소에 `--refresh`는 없다)
      — 함께 고친 것: `CompanyProfileResult`가 다른 OpenDART 결과와 달리 `all_rate_limited`를 들고 있어 `is_opendart_daily_limit_exhausted`가 이 경로에서 **한 번도 걸리지 않았다.** 키 소진 시 법인당 3회 재시도하며 3,959건을 끝까지 돌았고 exit 75가 안 났다. `exhaustion_reason`으로 통일
- [x] **F-1.3** 시드: `dart_corp_master` 현재 행 → `observed_month`, `is_seed=true`. CLI `dart seed-corp-profile-history --observed-month`
      — local `mydb`에서 실행 확인: 1회차 700행, 2회차 0행(idempotent), `observed_at`은 corp master의 `profile_fetched_at`(2026-08-15~18) 유지
- [ ] **F-1.4** 첫 실행(prod): 3,959 법인, 오류 0, 행 수 = 법인 수 — **명령 준비 완료, 사용자 확인 대기** → [`results/prod_runbook_f1_4_f9_3.md`](results/prod_runbook_f1_4_f9_3.md) §1. prod에 테이블이 없으므로 릴리즈 + `db init`이 선행. 시드 월을 2026-08(관측 실제 시각)로 할지 2026-09(문서)로 할지 결정 필요
- [ ] **F-1.5** 마트 `dim_industry_pit_daily`(`ind_ksic_code`, `ind_group`, `ind_is_backcast`, `ind_changed_recent_252`) + 테스트(backcast 경계, as-of, fold-up 규칙 동일)
- [ ] **F-1.6** Cronicle 월 이벤트 + freshness 월 예산
- [ ] **F-1.7** 변경률 리포트 스크립트 + 첫 회(2026-10)
- [ ] **F-1.8** KIS 종목 기본정보 업종 필드 PoC(`poc/kis_stock_info_industry.md`) → L2 채택 여부
- [ ] **F-1.9** D-F1 판정(2026-12, 3개월) — 문턱 0.3% 고정

### F-2 통계적 peer (`02` §1)

- [x] **F-2.1** `resid_ret` CTE를 `trading_panel`/`price.py`에서 함수로 공유(`feat_price` SQL 불변 확인 — A0 해시). 이미 `trading_panel.build_market_model_sql`로 빠져 있어 `relation_stat`이 그대로 재사용한다. `feat_price` 해시 두 개를 `test_relation_stat.py`에서 다시 고정
- [x] **F-2.2** `dim_peer_monthly`: 월말 252세션 잔차 상관, 유효 189, K=20, numpy → parquet. PIT 테스트. 저장은 K=40(진단 `_k10`·`_k40`을 rank 필터로 얻는다), 컬럼명은 `rank` → `peer_rank`(DuckDB 예약어 회피)
- [x] **F-2.3** `feat_relation_stat`: 5 primary + 진단 `_k10/_k40`, `rel_peer_corr_mean`, `rel_peer_ksic_agree`. **연도별** 파티션 materialize(`mart.materialize_in_parts`) — 월별보다 파트 수가 적고 한 파트가 여전히 수백만 행이라 조인 working set이 작다
- [x] **F-2.4** 마트 검증 리포트: 커버리지·분포·자기상관·규모 축 상관 → [`results/f2_relation_stat_verification.md`](results/f2_relation_stat_verification.md)
- [x] **F-2.5** 사전등록 항목 4 family(부호·horizon·fdr_family `relation`) 확정 → 같은 리포트 §6. YAML은 F-HS-1에서 만든다

### F-3 업종 관계 (`02` §2) — D-F1 뒤

- [ ] **F-3.1** `feat_relation_ind` 6컬럼 + `ind_group_id`. `require_pit` 처리
- [ ] **F-3.2** `feat_fin_scan_daily_ind` 컬럼을 family로 등록(표지 포함)
- [ ] **F-3.3** F-HS-2 config

### F-4 재무위험·생애주기·전이 (`03`)

- [x] **F-4.1** `fin_scan.py`의 vintage 선택·`base_ok`·`available_from`을 `features/fin_vintage.py`로 공유. `feat_fin_scan_daily` SQL 해시 2개 불변 확인(`9656de5e…`/`2615e77d…` — 2026-08-23 snapshot의 `_cache_metadata.json`과 일치)
- [x] **F-4.2** `feat_fin_risk` 9 family(`fin_risk_v1`), TTM·직전 vintage·`dps` 노출 규칙. 7,211,785행
      — 조인 형태를 바꿔야 했다: metric 10 × basis 2 = 20개의 equality+range 조인은 7.2M 세션 × 1.17M 인터벌에서 **완료되지 않았다**(20분 무출력). ASOF 20개도 마찬가지. interval을 먼저 wide로 피벗해 **ASOF 1개**로 읽으면 100초에 끝난다. `feat_fin_scan_daily`는 range 조인을 그대로 둔다(SQL 텍스트가 A0 캐시 키). 두 형태가 같은 행을 고른다는 것을 엇갈린 접수일 fixture로 테스트
- [x] **F-4.3** 생애주기 8조합 매핑 테스트(8개 전수 parametrize), 전이 이벤트 빈도 리포트 → **`fin_lifecycle_transition`·`fin_profit_turn` = continuous**(연평균 0.69/0.27, 문턱 0.05), 나머지 둘은 event cohort 유지
- [x] **F-4.4** 상관 진단 → **경고 2건**: `fin_interest_coverage` × `fin_operating_profitability` ρ=0.88(분자 공유), `fin_net_debt_to_mcap` × `fin_book_to_market` ρ=0.51(분모 공유). 상폐 커버리지 9.2%(`stock_master` 기준) → Decline 판정 보류
- [x] **F-4.5** 사전등록 9 family 확정 → [`results/f4_fin_risk_verification.md`](results/f4_fin_risk_verification.md) §8
- [ ] **F-4.6**(새로 생긴 항목) **5개 family가 2020년부터만 존재한다.** `interest_paid`·`investing_cash_flow`·`financing_cash_flow`·`cash_and_cash_equivalents` 넷에 XBRL fallback 규칙이 없어 2018년 이전 행이 100건 안팎이다. F-5.1의 **최우선 대상**이고, 붙이면 `fin_risk_v2` 범프 + 재검정이 따라온다. 그때까지 사전등록 표본은 2020~2025로 읽는다

### F-5 `metric_rules` 확장 (`03` §3)

- [ ] **F-5.1** 태그 커버리지 PoC(`current_assets`, `current_liabilities`, `borrowings`, `rnd_expense`) → `poc/metric_rules_ext.md`
- [ ] **F-5.0**(선행, F-4.6에서 나왔다) 기존 4 metric에 XBRL fallback 추가: `interest_paid`, `investing_cash_flow`, `financing_cash_flow`, `cash_and_cash_equivalents`. 새 metric이 아니라 **기존 metric의 원천 확장**이라 golden 영향 범위가 다르다 — `operating_cash_flow`만 기존 family(`fin_accruals_to_assets`)가 쓰므로 그 넷은 현재 소비자가 `feat_fin_risk`뿐이다
- [ ] **F-5.2** catalog·매핑 규칙 추가, 기존 29 metric golden 불변
- [ ] **F-5.3** vintage 재빌드, 역산 비율 기록
- [ ] **F-5.4** 파생 family(`fin_current_ratio`, `fin_borrowings_to_mcap`, `fin_altman_z`, `fin_rnd_to_sales`, `fin_bm_intangible_adj`) → F-HS-2 또는 3

### F-6 `fin_sue` 백필 (`04` §1)

- [ ] **F-6.1** 대상 역산 스크립트 + 측정 문서(법인·접수·호출 수)
- [ ] **F-6.2** prod 백필 실행(`dart backfill-xbrl-receipts --targets-file`), exit 75 재개, 완료 기록
- [ ] **F-6.3** 새 snapshot 재빌드 → `effective_start`·coverage 확인 → 기존 사전등록으로 재판정(F-HS-1과 같은 snapshot)

### F-7 구조화 이벤트 (`04` §2·§3)

- [ ] **F-7.1** DS005 PoC: endpoint 목록·인자·응답·볼륨 실측 → `poc/ds005_events.md`, D-F3 확정
- [ ] **F-7.2** `dart_major_event_raw` DDL + 7곳 + 어댑터(statement enum) + 서비스·CLI + 테스트
- [ ] **F-7.3** 백필 2015~2025 + 일 증분 이벤트(23:45)
- [ ] **F-7.4** `feat_event_decision` 4 family → F-HS-2
- [ ] **F-7.5** `elestock` 월 수집 시작(`dart_insider_holding_raw`) — D-F2 확정 2026-09-07. DDL + 7곳 + 어댑터 + 월 이벤트, 첫 실행 기록

### F-8 매크로 2·3단계 (`05`)

- [ ] **F-8.1** ECOS item_code 실호출 확정(`817Y002` AA-/BBB-/CD91, `722Y001`, `513Y001`) → `poc/ecos_stage2_items.md`
- [ ] **F-8.2** 시리즈 10 + 파생 feature 추가(`common_features.py`), golden 불변 확인, `common seed`
- [ ] **F-8.3** 백필(`common sync --sources ecos,fred`), readiness·freshness 통과
- [ ] **F-8.4** inactive 시리즈 정리(주석·`active=False` 유지)
- [ ] **F-8.5** 국면 R7~R11 사전 계산(점유율·지속·G2) → 기록
- [ ] **F-8.6** Phase C 2라운드 사전등록(Q1~Q6 + X3) — F-4 완료 뒤 → 실행
- [ ] F-8.7 3단계 어댑터: 관세청 수출·SOX → `macro_beta_export`·`macro_beta_sox` — D-F5 확정: **F-HS-C2에서 2단계 국면이 하나 이상 `screen_pass`한 뒤에만** 착수

### F-9 인프라·PIT (`06`)

- [ ] **F-9.1** `feat_fin_ratios_pit`(`_pit` 접미) + `fin_pit` 대비 차이 리포트 → 모델 스트림 통보(E3-b)
- [ ] **F-9.2** `holidays_krx.csv` 2014~2023 보강(`daily_market_cap` 무행 평일) → golden 재생성·diff 기록 → fact 재빌드(F-HS-1 snapshot에 묶음)
- [ ] **F-9.3** S-1 잔여: `sync-financials/share-info/xbrl` **`--universe-scope historical`**(`--include-delisted`는 없는 플래그다) 2015~2025 — **명령 준비 완료, 사용자 확인 대기** → [`results/prod_runbook_f1_4_f9_3.md`](results/prod_runbook_f1_4_f9_3.md) §2
      — 실측: 재무 866 / 주식수 529 / XBRL 866 법인 누락. 그중 764는 `stock_master`에 행이 **아예 없는** 조기 상폐 법인이다
      — 호출 상한 약 **13.7만**(4.3만 추정은 보고서·fs_div 축을 빼먹었다)
      — `bin/dart-backfill-all-years.sh`가 `--universe-scope`를 안 넘기고 있었다(기본 `current`) = S-1 갭의 직접 원인. `SDC_DART_BACKFILL_UNIVERSE_SCOPE` 추가, 기본값은 `current` 유지
- [ ] **F-9.4** S-2: `universe backfill-master` → 상폐 종목 `prices backfill`
- [ ] **F-9.5** S-3: `delisted_date` 확정, 상폐 종목 라벨 정책 문서화
- [ ] **F-9.6** `dim_universe_daily_krx`(`daily_market_cap` 기준) + 집합 차이 리포트
- [ ] **F-9.7** Cronicle 이벤트 등록(월 corp-profile·insider, 일 major-events), N3 백필 이벤트 삭제
- [ ] **F-9.8** freshness 월 예산 항목

### F-HS Horizon Scan 사전등록·실행

- [ ] **F-HS-1** overlay config(`horizon_scan_expansion_2026MM.yaml`): F-2 4 + F-4 9 family, fdr_family `relation`·`financial_risk`·`lifecycle`·`transition`. `registered_at` 실제 날짜, hash 기록(`05_preregistration_record` 형식)
- [ ] **F-HS-1 실행** 새 snapshot(S-1 잔여·F-6·F-9.2 반영) → A0 → A → B → AB. 기존 discovery 변화 0 확인, 단계 0 exact match
- [ ] **F-HS-1 결과 문서** → `screen_pass` 컬럼 목록 = **FS3** → 모델 스트림 `05` E5에 기록
- [ ] **F-HS-C2** Phase C 2라운드(F-8.6)
- [ ] **F-HS-2** F-3·F-5·F-7 family(D-F1·PoC 결과 뒤)

---

## 3. 규모 추정

| 항목 | 값 | 근거 |
|---|---|---|
| F-1 월 호출 | 3,959 × 0.2s ≈ 13분 | N2-7b 1,301초/3,959 |
| F-2 peer 계산 | 월 2,800² 상관 × 130개월, 수 분 | numpy |
| F-2 마트 | 7M행 × peer 20 조인 → 월별 파티션 | DuckDB |
| F-4 마트 | `feat_fin_scan_daily`(7.2M행)와 같은 규모 | 같은 vintage 입력 |
| F-6 백필 | 측정 뒤 | — |
| F-7 백필 | PoC 뒤. N6급이면 8만 호출 | N6 83,700 |
| F-8 백필 | 시리즈 10 × 12년 일별, 수 분 | ECOS/FRED |
| S-1 잔여 | 약 1,300 법인 × 11년 × 3 ≈ 4.3만 호출, 키 2개 며칠 | `10_work_breakdown.md` S-1 |
| F-HS-1 실행 | A0 + A(60분) + B(45분) + AB(1초) ≈ 2시간, Phase C +30분 | 08-23 실측 |

---

## 4. DoD (패키지 공통)

- 마트: `FORMULA_VERSION` 명시, PIT 규칙·유효 시작일 docstring, 커버리지·분포 리포트, 단위 테스트(PIT·NULL·정의), 기존 마트 SQL 불변(A0 해시).
- raw: `06` §6 체크리스트 전부.
- 사전등록: 부호·horizon·fdr_family·스캔 경로가 결과를 보기 전에 YAML에 고정, hash 기록.
- 문서: 이 디렉터리 `results/`에 PoC·측정·실행 기록. 상태표(§0) 갱신.

---

## 5. 개정 이력

| 날짜 | 내용 |
|---|---|
| 2026-09-07 | 최초 작성. F-1~F-9·F-HS 정의, 결정 D-F1~D-F5 제시 |
| 2026-09-07 | **D-F1~D-F5 권고안 채택 확정.** D-F1은 판정 규칙(3개월 변경률, 문턱 0.3%)이 확정된 것이고 판정 자체는 2026-12. `elestock` 시작, S-1 잔여 실행, DS005 6종 PoC, 3단계는 2단계 국면 통과 뒤 |
