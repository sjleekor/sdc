# 매크로 exposure 베타 · Phase C 조건부 IC 사전등록 — 2026-08-29

- config: `research/analysis/horizon_scan_macro_20260829.yaml`
- config hash: `236d0d3515043e44e280f0c2c2707ca2cc486aa44b638eb893a7095ddac1110f`
- base config: `horizon_scan_expansion_20260827.yaml`, `889c3e83…` (그 base는 `horizon_scan_config.yaml`, `ab0de634…`)
- 등록 시점: **매크로 베타와 조건부 IC의 label·IC·p-value 계산 전**
- 설계 문서: `docs/dev/20260829_macro_features/01_design/` (`00`~`04`, 리뷰 `06`·반영 `07`)

## 결론

기존 25 + 10 family와 Phase A 75개 가설은 바꾸지 않는다. 이 층이 더하는 것은 둘이다.

1. **Phase B에 매크로 exposure 베타 6 family × 4 cell = 24 candidate cell.** Phase B는 78 → **102**,
   결합 BH 모집단은 75 + 102 = **177**.
2. **Phase C 조건부 IC 계약을 처음으로 연다.** primary 15쌍 + reference 2쌍. Phase C BH 모집단은 **15**.

매크로 level·Δ·YoY는 같은 날짜의 모든 종목에 같은 값이라 횡단면 rank IC에 넣지 않는다(`00_survey/00` §3).
매크로가 이 틀에 들어오는 길은 **종목별 rolling exposure 베타**(횡단면에서 변한다)와 **국면별 조건부 IC**뿐이고,
이 층은 그 둘을 각각 등록한다.

interaction(국면 × 종목특성)은 scan family로 등록하지 **않는다.** rank IC 아래에서
`IC_t(s·x) = sign(s_t)·IC_t(x)`이므로 interaction family의 평균 IC는 어떤 코딩으로도 `δ = E[IC|s=1] − E[IC|s=0]`가
아니다(`00_overview` §1.1). 같은 가설을 Phase C의 조건부 차이로 등록한다.

## 신규 Phase B registry — 매크로 exposure 베타

전 family 공통: `phase: B`, `fdr_family: macro_exposure`, `role: phase_b_blocked`, `fdr_include: false`,
`primary_horizon_set: [20, 60]`, `include_bucket_primary: true`, `official_feature_variant: native_t`,
`readiness_dependencies: [feat_macro_exposure, common_feature_daily_fact, label_scan]`.

| family | primary feature | secondary | expected sign | cell | 근거 |
|---|---|---|:---:|---:|---|
| `macro_beta_usdkrw` | `macro_beta_usdkrw` | `macro_rawbeta_usdkrw`, `macro_semibeta_usdkrw_up` | 양방향 | 4 | Chu 2022 세미베타 프리미엄, 고강석 2019 부호 불안정 |
| `macro_beta_wti` | `macro_beta_wti` | `macro_rawbeta_wti` | 양방향 | 4 | 한국 섹터 비대칭 반응(RIBAF 2025) |
| `macro_beta_kr10y` | `macro_beta_kr10y` | `macro_rawbeta_kr10y` | 양방향 | 4 | 한국 횡단면 검정 공백 |
| `macro_beta_sp500_lag` | `macro_beta_sp500_lag` | `macro_rawbeta_sp500_lag` | 양방향 | 4 | RSZ 2013 (표본에 한국 없음) |
| `macro_beta_vix` | `macro_beta_vix` | `macro_rawbeta_vix` | **−** | 4 | Ang et al. 2006 FVIX, Bali-Brown-Tang 2017 |
| `px_market_beta` | `px_market_beta` | — | 양방향 | 4 | Frazzini-Pedersen 2014 (한국 재현 근거 없음) |

**primary는 market-model 잔차의 요인 민감도다.** 라벨이 `(거래일, 시장)` 동일가중 평균 초과수익률이라
시장 평균 중립이지 베타 중립이 아니고, 원수익률 요인 베타는 `β_{i,m}·Cov(r_m, f)`에 크게 물들어
`px_market_beta`의 복사본이 된다. `macro_rawbeta_*`는 secondary로 같이 내고 둘의 IC 차이를 진단으로 본다.

horizon [20, 60]을 고른 이유: 베타는 252세션 창의 느린 변수라 1~10일 예측을 주장할 근거가 없고,
120일은 NW lag 119로 유효 관측이 적다. cum 2 + bucket 2(`[10,20]`, `[40,60]`) = family당 4 cell.

## Phase C 조건부 IC 계약

`contract: conditional_ic_v1`, `open_policy: preregistered_pairs`, `grid: krx_sessions`,
`sample_start: 2015-06-16`, discovery 좌표 `broad × common_survivor`.

### 국면 7 (primary 4, exploratory 3)

| id | 연속 변수 `z_t` | `s_t = 1[z_t > 0]` | 역할 |
|---|---|---|---|
| `vix_up` | `VIX_t − VIX_{t−20}` | 20세션 VIX 상승 | primary |
| `vix_high` | `VIX_t − median_252(VIX)` | VIX 수준이 1년 중앙값 위 | primary |
| `market_up` | `ln(KOSPI_t / KOSPI_{t−252})` | 12개월 KOSPI 상승 | primary |
| `liq_high` | `ln(mean_20(TV) / median_252(TV))`, TV = KOSPI+KOSDAQ 거래대금 | 시장 거래대금 높음 | primary |
| `term_steep` | `TS_t − median_252(TS)` | 기간 스프레드 가파름 | exploratory |
| `kosdaq_rel_up` | `Σ20 ret_kosdaq − Σ20 ret_kospi` | 코스닥 상대 강세 | exploratory |
| `krw_weak_20d` | `ln(USDKRW_t / USDKRW_{t−20})` | 원화 20일 약세 | exploratory |

### 검정 쌍 15 + reference 2

| id | family | cell | 국면 | 기대 `δ` |
|---|---|---|---|:---:|
| P1 | `px_idio_vol_60d` | cum 0→60 | `vix_up` | + |
| P2 | `px_maxret_20d` | cum 0→60 | `vix_up` | + |
| P3 | `px_reversal_5d` | cum 0→5 | `vix_high` | + |
| P4 | `px_mom_12_1` | cum 0→60 | `market_up` | + |
| P5 | `px_mom_12_1` | cum 0→60 | `liq_high` | + |
| P6 | `px_amihud_20d` | cum 0→60 | `liq_high` | − |
| P7 | `flow_foreign_netbuy_to_volume` | cum 0→20 | `market_up` | + |
| P8 | `flow_individual_netbuy_to_volume` | cum 0→20 | `market_up` | − |
| P9 | `px_turnover_shock` | cum 0→20 | `liq_high` | 양방향 |
| P10 | `flow_inst_netbuy_to_volume` | cum 0→20 | `market_up` | 양방향 |
| P11 | `flow_foreign_netbuy_to_volume` | cum 0→20 | `vix_up` | 양방향 |
| P12 | `flow_foreign_netbuy_to_volume` | cum 0→20 | `liq_high` | 양방향 |
| P13 | `flow_inst_netbuy_to_volume` | cum 0→20 | `liq_high` | 양방향 |
| P14 | `flow_individual_netbuy_to_volume` | cum 0→20 | `liq_high` | 양방향 |
| P15 | `px_market_beta` | cum 0→20 | `market_up` | 양방향 |
| X1 | `fin_log_mcap` | cum 0→60 | `market_up` | reference, `δ ≈ 0` |
| X2 | `fin_log_mcap` | cum 0→60 | `kosdaq_rel_up` | reference, `δ ≈ 0` |

각 cell은 그 family의 `primary_horizon_set` 안에서 골랐다(validator가 강제한다). 결과를 보고 옮기지 않는다.

### 게이트

`screen_pass = discovery ∧ G1 ∧ G2 ∧ G3 ∧ G4`. G4(국면 circular-shift placebo, 100회, 최소 이동 120세션,
`p ≤ 0.10`, seed `20260829`)는 **모든 쌍에 필수**다 — 국면과 IC가 둘 다 지속 계열이라 관계가 없어도 `δ̂`가 크게
나올 수 있고, HAC는 국면 길이가 창을 넘으면 못 잡는다.

## 국면 사전 계산 — hash 전에 확정한 값

`03` §2.3의 지시대로 overlay 커밋 전에 계산했다. 국면 시계열은 국면 변수만으로 정해지므로 결과와 무관하다.

- 구현: `research/analysis/horizon_scan_phase_c_regimes.py`
- 실행: `uv run python -m research.analysis.horizon_scan_phase_c_regimes --snapshot-date 2026-08-23 --source sj2_remote`
- 산출: `research/output/horizon_scan/phase_c_regimes/`
- 입력: `label_scan` 세션 격자(4,736 세션) ⨝ `common_feature_daily_fact` (snapshot 2026-08-23, derived_mart persist본)
- **판정 창 `2015-06-16 ~ 2025-02-05`, 2,368 세션.** 15쌍이 전부 `common_survivor` cell이라 daily IC가
  `common_formation_end`(2025-02-05)에서 끝난다. 계열 자체는 2026-08-21까지 저장한다.

### G1 국면 점유율

| regime | role | 세션 | s=1 | s=0 | 점유율 | 시작 | 끝 | G1 |
|---|---|---|---|---|---|---|---|---|
| `vix_up` | primary | 2,368 | 1,107 | 1,261 | 0.467 | 2015-06-16 | 2025-02-05 | 통과 |
| `vix_high` | primary | 2,368 | 1,097 | 1,271 | 0.463 | 2015-06-16 | 2025-02-05 | 통과 |
| `market_up` | primary | 2,362 | 1,317 | 1,045 | 0.558 | 2015-06-24 | 2025-02-05 | 통과 |
| `liq_high` | primary | 2,368 | 1,040 | 1,328 | 0.439 | 2015-06-16 | 2025-02-05 | 통과 |
| `term_steep` | exploratory | 2,368 | 1,109 | 1,259 | 0.468 | 2015-06-16 | 2025-02-05 | 통과 |
| `kosdaq_rel_up` | exploratory | 2,368 | 1,143 | 1,225 | 0.483 | 2015-06-16 | 2025-02-05 | 통과 |
| `krw_weak_20d` | exploratory | 2,368 | 1,324 | 1,044 | 0.559 | 2015-06-16 | 2025-02-05 | 통과 |

### 국면 지속 (§6.5)

| regime | 전환 횟수 | 평균 지속 s=1 | 평균 지속 s=0 |
|---|---|---|---|
| `vix_up` | 329 | 6.7 | 7.6 |
| `vix_high` | 170 | 12.8 | 15.0 |
| `market_up` | 64 | 39.9 | 32.7 |
| `liq_high` | 39 | 52.0 | 66.4 |
| `term_steep` | 100 | 21.7 | 25.2 |
| `kosdaq_rel_up` | 246 | 9.2 | 10.0 |
| `krw_weak_20d` | 225 | 11.7 | 9.2 |

### G2 구간별 유효 여부 (양쪽 ≥ 40일)

| regime | 2014_2016 | 2017_2019 | 2020_2021 | 2022_2023_10 | 2023_11_common_end | 유효 구간 |
|---|---|---|---|---|---|---|
| `vix_up` | 184/199 ✅ | 320/413 ✅ | 237/259 ✅ | 213/237 ✅ | 153/153 ✅ | **5/5** |
| `vix_high` | 182/201 ✅ | 342/391 ✅ | 225/271 ✅ | 205/245 ✅ | 143/163 ✅ | **5/5** |
| `market_up` | 154/223 ✅ | 393/340 ✅ | 422/74 ✅ | 94/356 ✅ | 254/52 ✅ | **5/5** |
| `liq_high` | 138/245 ✅ | 329/404 ✅ | 314/182 ✅ | 160/290 ✅ | 99/207 ✅ | **5/5** |
| `term_steep` | 106/277 ✅ | 293/440 ✅ | 365/131 ✅ | 123/327 ✅ | 222/84 ✅ | **5/5** |
| `kosdaq_rel_up` | 171/212 ✅ | 347/386 ✅ | 281/215 ✅ | 219/231 ✅ | 125/181 ✅ | **5/5** |
| `krw_weak_20d` | 241/142 ✅ | 361/372 ✅ | 249/247 ✅ | 285/165 ✅ | 188/118 ✅ | **5/5** |

**여기서 확정되는 것 셋.**

- **G1은 7개 전부 통과한다.** 점유율 0.44~0.56. G1 `insufficient`로 빠질 쌍은 없다. P15만 1a 산출 여부로 갈린다.
- **G2는 35구간 전부 유효하다.** `03` §2.3이 걱정한 `market_up`의 `2023_11_common_end`는 **254/52**로 40 문턱을
  12일 차이로 넘겼다. 표에서 가장 빠듯한 칸이므로 결과 해석에서 다시 본다. `2020_2021`의 422/74와
  `2022_2023_10`의 94/356도 크게 기울어 있어, 그 구간 `δ`는 적은 쪽 표본이 좌우한다.
- **지속이 G4를 정당화한다.** `liq_high`는 평균 52~66세션 지속에 전환 39회뿐이고 `market_up`은 33~40세션이다.
  HAC lag는 `h_end−1`(P5·P6은 59)이라 60세션짜리 블록을 잡지 못한다. `vix_up`(6.7~7.6세션)만 HAC 안에 들어온다.

## 사전 조건 — 이 층으로 run하기 전에

- A0 마트를 **overlay hash로 다시 만들어야 한다**: `horizon_scan_inputs --force --config <overlay>`.
  `mart.materialize`는 `analysis_config_hash`가 다르면 에러를 낸다. 2026-08-23의 A0 마트는 `889c3e83`으로 캐시돼 있다.
- `common_feature_daily_fact`(실행 1)와 `feat_macro_exposure`(실행 2)는 2026-08-23에 이미 만들어 두었다
  (`04` §3.1). overlay hash 재빌드 때 `feat_macro_exposure`도 같이 다시 만들어진다.

## 보존 규칙

- `horizon_scan_config.yaml`(`ab0de634…`)·`horizon_scan_expansion_20260827.yaml`(`889c3e83…`)과 그 산출물은 그대로 둔다.
  두 hash가 움직이지 않는다는 것을 테스트로 고정했다(`test_horizon_scan_config.py`).
- overlay는 `extends` 체인으로 읽되 hash는 merge된 전체 계약에서 계산한다.
- 양방향 family·쌍은 첫 결과의 관측 부호로 방향을 고정하고 이후 바꾸지 않는다. 카드에는
  `direction_preregistered: null`이 그대로 남는다.
- Phase C가 읽을 A·B run_id는 config가 아니라 CLI 인자이고 `phase_c_run_spec.json`에 sha와 함께 기록한다.
- N8 고용 regime 후보는 dormant 그대로다. 이번 국면 목록에 넣지 않는다.
- holdout(2025-08-01~)은 feature·horizon·variant·Phase C 선택을 모두 끝낸 뒤 한 번만 연다.
- 바꿀 수 없는 것과 바꿀 수 있는 것의 전체 목록은 `01_design/04_preregistration_overlay.md` §5.

## 실행 결과 — 2026-08-30

이 절은 사전등록 계약을 바꾸지 않고 실행 lineage와 판정만 덧붙인다.

| phase | run_id | 결과 |
|---|---|---|
| A | `20260830T085718-efd35e70` | 412 cell 전부 valid, `bh_pass` 57, discovery 32 — **canonical A와 exact match** |
| B | `20260830T100518-efd35e70` | 102 cell 전부 `ready_primary`·valid (매크로 24 포함) |
| AB | `20260830T122850-efd35e70` | 177 가설, discovery 103, `screen_pass` 53, B-cell A23·B30·C40·D9 |
| C | `20260830T122850-phasec` | primary 15쌍 전부 유효, discovery 4, `screen_pass` 4 (A4·D11·R2) |

결합 permutation `p = 0.0099`. **기존 Phase A discovery 변화 0개**, 공통 153 가설의 등급 변화 0개.

- 단계 1a 해설: [`05_results_stage1a_20260830.md`](05_results_stage1a_20260830.md)
- 단계 1b 해설: [`05_results_stage1b_20260830.md`](05_results_stage1b_20260830.md)

**Phase C 개방 조건이 처음으로 충족됐다.** P3(`px_reversal_5d` × `vix_high`),
P9(`px_turnover_shock` × `liq_high`), P12(`flow_foreign_netbuy_to_volume` × `liq_high`),
P15(`px_market_beta` × `market_up`) 넷이 `screen_pass`이고 등급 A다. G4 국면 placebo는 15쌍에서
BH와 한 번도 엇갈리지 않았다 — discovery 4쌍 전부 `placebo_p = 0.0099`, 나머지 11쌍 전부 `> 0.10`.

### 단계 0 검증 (`01` §4.2·§4.3)

- **§4.2 통과.** Phase A `horizon_ic.parquet` 412행 × 40컬럼을 canonical `20260827T221729-4e0ae8b0`과
  전 컬럼 대조해 **max |Δ| = 1.388e-17**. `bh_pass`·`primary_discovery`·family 등급 전부 동일.
- **`daily_ic_reconciled: true`, 최대 차이 0.0** (Phase A·B 양쪽 `_SUCCESS.json`).
- **§4.3 legacy/native parity — 통과.** legacy Phase B `20260830T041554-db50d0ff`와 대조:
  `daily_spread` 완전 일치(0.000e+00), `daily_ic` **868,400행 행 집합까지 정확히 일치**,
  공통 행 **max |Δ| = 2.776e-16**.

  1차 실행에서는 `daily_ic`의 행 집합이 474행 어긋났는데, 단계 0이 아니라 **native 엔진이 상수
  횡단면에서 가짜 상관을 내는 결함**(I13) 때문이었다. 그것을 고치고 계보 전체를 다시 돌린 것이
  위 run들이다 — 아래 참조.

### 실행 중 확인한 것 셋

- **A0 재빌드는 장부 정리였다.** overlay hash로 다시 만든 7개 A0 마트의 행 수·schema hash가
  `889c3e83` 것과 전부 같다. A0에 들어가는 config 섹션(`quality`·`universe`·`sample`·`horizons`·`buckets`)이
  두 층에서 동일하기 때문이다.
- **`permutation_cell_stats`는 canonical과 달라야 한다.** `mapping_seed_sequence`가 `config_hash`를
  seed 입력으로 받으므로 사전등록 층이 다르면 복제 매핑이 반드시 달라진다. `01` §4.2가 지정한
  세 artifact에 이것이 없는 이유다.
- **N6 4 family의 IC가 canonical과 미세하게 다르다**(max |Δ| 4.7e-04, 16 가설). `feat_periodic_extras`를
  읽는 family 전부이자 그것뿐이고, 그 파일은 canonical Phase B run(2026-08-28 12:33)보다 **뒤인
  19:41 커밋 `7f5cc6f`에서 추가**됐다. 판정(`primary_discovery_ab`·`screen_pass`·`evidence_grade`)은
  16 가설 전부 동일하다.

### 발견하고 고친 결함 — native 엔진의 상수 횡단면 처리 (I13)

`per_date_market_rank_ic(engine="polars_native_v1")`가 **예측자가 완전히 상수인 횡단면에서 NaN 대신
가짜 상관을 낸다.** legacy `_spearman`은 `ps == 0` 가드로 정확히 NaN을 낸다.

- 실측: 2014-06-16 KOSPI, `own_major_filing_60d` 고유값 **1개**(745종목 전부 동일), rank std = 0.
  legacy `nan` / native `−0.014574`.
- 최소 재현: 길이 745 상수 열에서 polars가 seed마다 −0.049 ~ +0.022를 낸다. 같은 입력에는 결정적이지만
  길이 50·100·…·500·1000에서는 NaN이 나온다 — **크기·데이터 의존**이다. polars 1.41.2.
- 영향: 이번 Phase B `daily_ic` 868,874행 중 **474행**(0.05%), 전부 `own_major_filing_activity`,
  472행이 2014년(그 count feature의 이력 시작 구간)이다. `ic_mean`이 약 5e-4 움직이고 **판정 변화는 0**이다.
- **매크로 6 family와 Phase C 15쌍은 영향받지 않는다** — rolling 베타와 가격·수급 피쳐는 횡단면이
  상수가 되지 않는다.
- 이 결함은 **이번 작업이 만든 것이 아니고**, 기본 엔진(`polars_native_v1`)을 쓰는 기존 canonical run에도
  들어 있다. `04_engine_parity_20260829.md`가 같은 family의 parity 실패를 "engine 차이가 아니라 두 기준
  시점 사이 feature 정의 변경"으로 돌린 것은 **적어도 불완전한 귀속이다** — 같은 코드·같은 마트로 돌린
  이번 두 run에서도 차이가 그대로 남는다.
**고쳤다 (2026-08-30).** `04` §5는 구현 버그 수정을 허용하되 "수정 전후 canonical A 요약 exact match가
유지돼야 하고, 수치가 바뀌면 버그 수정 전 값은 판정에 쓰지 않는다고 결과 문서에 적는다"고 정한다.
그대로 따랐다.

1. `_per_date_market_rank_ic_native`에 그룹 내 고유값 검사를 넣어 상수 열이면 NaN으로 만든다
   (legacy `_spearman`의 `ps == 0` 가드와 같은 규약). docstring의 잘못된 주장도 고쳤다.
2. **영향 범위를 먼저 측정했다.** Phase A 16개 feature의 상수 횡단면 **0개**, Phase B는
   `own_major_filing_60d` 288개·`own_amendment_ratio_1y` 1개, 나머지 21개 feature 0개.
   매크로 6 family와 Phase C 15쌍은 영향 없음이 이 측정으로 확정된다.
3. **계보 전체를 수정된 코드로 다시 돌렸다** — Phase A는 결과가 같을 것을 알면서도 다시 돌렸다.
   `--phase-a-reuse-run-dir`가 `analysis_kernel_hash` 일치를 요구하기 때문이고(맞는 동작),
   published 계보가 한 코드에 묶이는 이득도 있다.
4. **수정 후 결과**: Phase A는 canonical과 여전히 **max |Δ| = 1.388e-17**(수정 전과 같은 값),
   `daily_ic` legacy/native parity는 **행 집합까지 일치**. AB 177 가설과 Phase C 17쌍의
   `primary_discovery_ab`·`screen_pass`·`evidence_grade`가 **수정 전후 하나도 바뀌지 않았다**
   (AB `ic_mean` max |Δ| 5.0e-04는 `own_major_filing_activity` 4 cell, Phase C는 δ̂ 차이 0.000e+00).
5. 회귀 테스트 3개를 `test_research_metrics.py`에 넣었다. 기존 edge-case 테스트에도 "constant group"이
   있었지만 **3행짜리**였고 그 크기에서는 polars도 NaN을 낸다 — 그래서 결함이 빠져나갔다.
   새 테스트는 실제 시장일 크기(50~1,581)를 다룬다.

**따라서 이 문서의 모든 수치는 수정된 코드로 낸 값이다.** 1차 실행(run_id 접미사 `db50d0ff`)은
디스크에 남아 있으나 판정에 쓰지 않는다.
