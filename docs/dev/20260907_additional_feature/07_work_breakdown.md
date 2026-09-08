# 07. 작업 분해 — 체크리스트·순서·규모

- 작성일: 2026-09-07
- 이 문서가 진행 상태를 추적한다. 체크는 **끝나고 검증됐을 때** 채운다. 왜 하는가는 `01`~`06`에 있다.
- 모델 스트림 접점: FS3 인계(F-HS `screen_pass` 뒤), FS0' 통보(F-9.1 뒤).

---

## 0. 상태표

| 패키지 | 내용 | 새 수집 | 상태 | 다음 행동 |
|---|---|---|---|---|
| F-1 | 업종 `induty_code` 버저닝 | OpenDART 월 3,959 | F-1.1~F-1.4·F-1.6 완료(2026-09-08) | F-1.5 마트 / F-1.7 변경률 스크립트 |
| F-2 | 통계적 peer 관계 피쳐 | 없음 | **완료**(2026-09-07, snapshot 2026-08-23) | F-HS-1 대기 |
| F-3 | 업종 관계 피쳐 | 없음(F-1 선행) | 대기(D-F1) | 3개월 변경률 측정 뒤 |
| F-4 | 재무위험·생애주기·전이 | 없음 | **완료**(`fin_risk_v2`, 2026-09-08) | F-HS-1 대기. 표본 시작은 family별 2016~2019 |
| F-5 | `metric_rules` 확장 | 없음(매핑) | **F-5.0 완료**(2026-09-08) | F-5.1 태그 커버리지 PoC(신규 4 metric) |
| F-6 | `fin_sue` XBRL 백필 | OpenDART(측정 뒤) | 미착수 | 대상 역산 |
| F-7 | DS005 이벤트·elestock | 있음 | 미착수 (D-F2 확정: `elestock` 시작 / D-F3: PoC 뒤 6종) | DS005 PoC + `elestock` 스키마 |
| F-8 | 매크로 2단계 시리즈 | 시리즈 정의 | 미착수 (D-F5 확정: 2단계 먼저) | ECOS item_code 확정 |
| F-9 | `fin_pit` strict·휴장일·상폐·유니버스·Cronicle | 일부 | **F-9.3 종결**(2026-09-08, 더 받을 것 없음) | F-9.1 / F-9.2 |
| F-HS | 새 config 사전등록·A→B→AB→C | — | 대기 | F-2·F-4 마트 완료 뒤 |

---

## 1. 순서

```
주차 1~2   F-2 (peer 계산·마트)  ‖  F-4 (fin_risk 마트)  ‖  F-1 스키마·시드·첫 스냅샷  ‖  F-9.3 S-1 잔여 → 2026-09-08 종결(수확 0)
주차 2~3   F-5 PoC  ‖  F-6 대상 측정  ‖  F-7 DS005 PoC  ‖  F-8 item_code 확정·시리즈 추가·백필  ‖  F-9.1 fin_ratios_pit
주차 3~4   F-HS-1: F-2·F-4 사전등록 config → (S-1 잔여·F-6 백필·holidays 보강이 끝났으면 같은 snapshot) A0→A→B→AB
주차 4~    F-8 국면 사전 계산 → Phase C 2라운드(F-4 필요)  ‖  F-7 수집  ‖  F-1 월별 스냅샷 누적
+3개월     D-F1 판정(변경률) → F-3 마트 → F-HS-2
FS3 인계   F-HS-1 screen_pass → 모델 E5
```

- `‖`는 병행 가능. 병목은 **prod OpenDART 호출**이다. ~~S-1 잔여~~는 2026-09-08에 종결됐으므로(더 받을 것이 없다) 키 예산은 **F-6 → F-7** 순이다.
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
- [x] **F-1.4** 첫 실행(prod) **완료 2026-09-08** → [`results/f1_4_first_snapshot_20260908.md`](results/f1_4_first_snapshot_20260908.md)
      — v0.12.0 릴리즈·배포 → `db init`(4분 33초) → 시드 2026-08-01 **3,959행** → 첫 수집 스냅샷 2026-09 **3,959/3,959/3,959, 오류 0, 19분 27초, `status=success`**
      — 시드 재실행 0행으로 prod `ON CONFLICT DO NOTHING` 확인
      — 월 호출 실측 **20분**(3.27 req/s). §3의 13분 추정은 호출당 0.2s만 계산한 값이라 틀렸다
      — 첫 변경률(판정 아님): 코드 0.0505%, **그룹 0.0253%**(문턱 0.3%). 코드 2건 중 1건은 업종 변경이 아니라 정밀도 변경(262→26293)이다
      — 부수 확인: `corp_cls`→`E` 6건 중 5건이 `stock_master` 상폐와 일치하고 `last_seen_date`가 두 스냅샷 사이에 들어간다. 6번째는 `stock_master`에 행이 없는 KONEX 종목이다. `corp_cls`가 `is_active`보다 신선하다
- [ ] **F-1.5** 마트 `dim_industry_pit_daily`(`ind_ksic_code`, `ind_group`, `ind_is_backcast`, `ind_changed_recent_252`) + 테스트(backcast 경계, as-of, fold-up 규칙 동일)
- [x] **F-1.6** Cronicle 월 이벤트 등록 완료 2026-09-08: `sdc_monthly_corp_profile_history`, 매월 1일 **05:30 KST**, `max_children=1`, `timeout=5400`, **`catch_up=1`**(1일에 호스트가 내려가 있어도 같은 달로 채운다)
      — `DART_PROFILE_FORCE=1`이 **필수**다. 없으면 skip-if-present가 전 법인을 건너뛰고 history에 아무것도 안 들어간다 — 옛 일회성 이벤트를 월 스냅샷으로 못 쓴 이유가 이것이다
      — 05:30을 고른 근거: 04:00 `sdc_daily_opendart_corp`이 실측 **2~4초**에 끝난다. 다음 opendart 잡은 23:30이라 20분 런과 겹치지 않는다
      — 중단된 달을 다시 돌리면 **빈 곳만 채운다**(이미 행이 있는 법인은 DO NOTHING)
      — freshness 월 예산 항목은 아직 안 넣었다 → F-9.8
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
- [x] **F-4.4** 상관 진단 → **경고 2건**: `fin_interest_coverage` × `fin_operating_profitability` ρ=0.88(분자 공유), `fin_net_debt_to_mcap` × `fin_book_to_market` ρ=0.51(분모 공유). 상폐 커버리지 9.2%(`stock_master` 기준)
      — **2026-09-08 갱신:** F-9.3이 종결돼 Decline 판정 보류는 "백필 대기"가 아니라 **영구 한계**다. 상폐 법인 재무 37%가 상한이다(F-9.3 결과 §5.1). 카드 문구를 그렇게 적는다
- [x] **F-4.5** 사전등록 9 family 확정 → [`results/f4_fin_risk_verification.md`](results/f4_fin_risk_verification.md) §8
- [x] **F-4.6 해소 2026-09-08** → [`poc/metric_rules_ext.md`](poc/metric_rules_ext.md). XBRL fallback 16규칙 추가 → `fin_risk_v2`
      — 다섯 family가 **2~4년 앞당겨졌다**: `fin_net_debt_to_mcap` 2020→**2016**, `fin_ext_finance`·`fin_lifecycle_stage`·`fin_lifecycle_transition` 2021→**2018**, `fin_interest_coverage` 2021→**2019**
      — 핵심은 철자였다. DART가 2019년경 taxonomy 접두를 바꿔서 2018년 이전을 덮는 건 `ifrs_` 쪽이다(`ifrs-full_Liabilities` ≤2018 364행 vs `ifrs_Liabilities` 112,827행). `ifrs-full_`만 붙였다면 아무것도 안 늘었다
      — 덧붙이기만 했다: `feat_fin_scan_daily` 입력 9 metric은 행 수 +0이고 **값이 다른 행 0개**. 유닛 1,609개 통과

### F-5 `metric_rules` 확장 (`03` §3)

- [ ] **F-5.1** 태그 커버리지 PoC(`current_assets`, `current_liabilities`, `borrowings`, `rnd_expense`) → `poc/metric_rules_ext.md`
- [x] **F-5.0 완료 2026-09-08** 기존 4 metric에 XBRL fallback 16규칙 추가 → [`poc/metric_rules_ext.md`](poc/metric_rules_ext.md)
      — 판정 규칙 두 개를 **측정 전에 커밋**했다(`05a1b3e`). 처음 제안한 "행 수 50%"는 틀린 양이라 폐기 — TTM은 연속 4분기를 요구하므로 분기의 50%를 무작위로 가지면 사용 가능 확률이 6%다
      — vintage 행 증가: `interest_paid` +38,957 / `investing_cash_flow` +51,668 / `financing_cash_flow` +49,446 / `cash_and_cash_equivalents` +50,335
      — **canonical 마트는 안 건드렸다.** 별도 lake(`data_lake_f50/`, raw·A0 마트는 심볼릭 링크)에서 측정했다. F-HS-1은 `07` §7대로 새 snapshot에서 돈다
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
- [x] **F-9.3** S-1 잔여 **종결 2026-09-08 — 더 받을 것이 없다** → [`results/f9_3_s1_remainder_20260908.md`](results/f9_3_s1_remainder_20260908.md)
      — 176 유닛 전수 완주(5시간 45분), 실패 0, 요청 **131,292건**, 저장 **0행**. 커버리지 변화 없음(재무 3,093 / 주식수 3,430 / XBRL 3,093)
      — 원인: 상장 법인은 이미 99.7% 완결이고 공백은 전부 상폐(E 1,230중 455=37%)·코넥스(N 105중 19)에 있다. 남은 866중 356은 정기보고서를 냈는데도 OpenDART가 `013 조회된 데이타가 없습니다`로 답한다
      — PoC(양성 대조 포함): `fnlttSinglAcnt`(주요계정)도 같은 013. 삼성전자는 000/229행 → 키·요청 형태는 정상. **재무제표 API가 현재 규제 대상 법인만 서빙하는 것**으로 보인다
      — **생존편향은 수집으로 닫히지 않는다.** F-4의 "S-1 잔여 뒤 판정"은 **영구 한계**로 문구를 바꿔야 한다(§5.1)
      — Cronicle 이벤트는 삭제했다. 드라이버 스크립트는 남긴다. 이유: skip-if-present가 *저장된* 행만 건너뛰므로 회수 불가 861개는 매 실행 13.1만 요청을 반복한다(스크립트에 "no-op가 된다"고 적은 것은 틀렸다)
      — `06` §3 정정: 누락은 1,300이 아니라 866, 호출은 4.3만이 아니라 13.1만, `--include-delisted`는 없는 플래그
- [ ] **F-9.4** S-2: `universe backfill-master` → 상폐 종목 `prices backfill`
- [ ] **F-9.5** S-3: `delisted_date` 확정, 상폐 종목 라벨 정책 문서화
- [ ] **F-9.6** `dim_universe_daily_krx`(`daily_market_cap` 기준) + 집합 차이 리포트
- [~] **F-9.7** Cronicle 정리 — 부분 완료 2026-09-08
      — [x] 월 corp-profile 이벤트 등록(F-1.6과 같은 항목)
      — [x] `emsugdoe907`("SDC Backfill DART Corp Profile (one-time)") **삭제**. 2026-08-15에 1회 1,300.6초 실행 code=0으로 목적 달성, `--force`가 없어 월 스냅샷으로는 못 쓴다
      — [x] `sdc_backfill_s1_remainder` **삭제**(F-9.3 종결, 이유는 그쪽 §4)
      — [ ] `emsugmrjp0a`("SDC Backfill N3 Universe Snapshots (one-time)") 삭제 — `timing=manual`이라 자동으로 안 돌고 급하지 않다. `06` §4가 삭제 대상으로 지목
      — [ ] `emr0r4xgb0h`("SDC Common Backfill 2015 (one-time)") — 검증 후 삭제 대상(별건)
      — 월 insider(F-7.5)·일 major-events(F-7)는 수집기가 아직 없어 해당 없음
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
| F-1 월 호출 | **약 20분** (3.27 req/s 실측, 2026-09-08) | N2-7b 1,301초와 같은 급. 0.2s × 3,959 = 13분은 응답 지연을 뺀 값이라 틀렸다 |
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
