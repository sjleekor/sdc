# 03. 단계 1b — 국면별 조건부 IC (Phase C)

- 작성일: 2026-08-29 (리뷰 `06_review_20260829.md` M3·M5·M6·M7, §6 반영: KRX 세션 격자, G5 제거, `vix_high` 국면과 P3,
  run_id를 CLI 인자로, 국면 지속 진단, seed 표현, 종속성 해석 규칙)
- 성격: **사전등록 검정.** 국면 정의, 검정 쌍, 기대 방향, 통계량, 게이트, placebo를 결과 전에 고정한다.
- 입력: 단계 0의 `daily_ic.parquet`(`01`), `common_feature_daily_fact`(국면 변수 원천), overlay config의 `phase_c` 블록(`04`).
- `12`의 Phase C 규율을 그대로 잇는다 — "regime cut을 결과에 맞춰 고르지 않는다", "실제 조건화 계약은 사전등록된
  변환만 사용해 고정한다". 이 문서가 그 계약이다.

---

## 1. 질문

**같은 피쳐의 IC가 국면에 따라 다른가.** 구체적으로, family `x`의 일별 IC `IC_t`와 formation 시점 t에 관측
가능한 이진 국면 `s_t ∈ {0, 1}`에 대해

```
δ = E[IC_t | s_t = 1] − E[IC_t | s_t = 0]
```

가 0인지, 0이 아니면 사전등록한 방향인지 검정한다. `00_overview` §1.1이 보인 대로 이것이 interaction
`s · x`의 올바른 검정이다.

무엇이 **아닌지**도 적어 둔다.
- 국면이 시장 방향을 예측하는지 묻지 않는다(라벨이 시장 중립).
- 국면 변수 자체의 IC는 정의되지 않는다(날짜 상수).
- 결과가 좋아도 "국면에서 피쳐를 켜고 끄는 전략"의 채택이 아니다. 채택은 acceptance gate의 일이다.

---

## 2. 국면 변수 — formation 시점 t에 관측 가능한 정의만

### 2.1 격자 — KRX 세션 위에서 계산한다

`common_feature_daily_fact`의 `feature_date`는 2014~2023년에 KRX 세션이 아니라 **평일 격자**다. `docs/holidays_krx.csv`가
2024~2026년만 담고 있어 그 이전은 KRX 휴장 평일(연 13~17일)이 전 세션 값 복사로 들어 있다(리뷰 M3 실측: 2015년 261행 중
비세션 13, 2023년 260행 중 15, 2024년 245행 중 1). 그 격자에서 `t−20`·`t−252`를 세면 창 길이가 2024년 전후로 달라진다.

그래서 국면은 다음 순서로 만든다. **fact를 다시 만들지 않는다**(만들면 `feat_common`·`00_survey/00` §4 재현값이 같이 움직인다).

1. KRX 세션 목록 = `label_scan`(또는 `daily_ohlcv`)의 distinct `trade_date`.
2. fact를 `feature_date = trade_date`로 세션에 join한다. 비세션 행은 빠진다. `asof_available_date <= trade_date`는 이미 성립.
3. LAG·rolling 창은 **세션 index 위에서** 잡는다. 아래 `t−k`는 전부 KRX 세션 k개 전이다.

### 2.2 국내 계열은 한 세션 stale이다

`market_kospi_close`·거래대금·`fx_usdkrw_level`은 `next_krx_session` 정책이라 세션 t의 fact 값은 **관측일 t−1**이다
(`02` §2.2). 국면 `s_t`는 그 값으로 만들므로 "t−1 종가까지의 정보로 정한 국면"이다. formation t에 이미 알고 있는
값이라 PIT는 지켜지고, 정보가 한 세션 오래됐을 뿐이다. 해외 계열(VIX)은 NY t−1 종가로 KRX 세션 t 아침 정보다.

### 2.3 정의

모두 과거 창이다. 전체 표본 중앙값·사후 확정 변수(침체 날짜 등)는 쓰지 않는다(`00_survey/01` §7).

| id | 이름 | 연속 변수 `z_t` | 이진 `s_t = 1[z_t > 0]` 뜻 | 원천 feature_code | 역할 |
|---|---|---|---|---|---|
| **R1** | `vix_up` | `VIX_t − VIX_{t−20}` | 지난 20세션 VIX 상승 | `global_vix_level` | primary |
| **R1b** | `vix_high` | `VIX_t − median_252(VIX)_t` | VIX 수준이 지난 1년 중앙값보다 높음 | `global_vix_level` | primary |
| **R2** | `market_up` | `ln(KOSPI_t / KOSPI_{t−252})` | 지난 12개월 KOSPI 상승 (CGH 2004의 12개월 판) | `market_kospi_close` | primary |
| **R3** | `liq_high` | `ln( mean_20(TV)_t / median_252(TV)_t )`, `TV = KOSPI + KOSDAQ 거래대금` | 최근 시장 거래대금이 1년 중앙값보다 큼 | `market_kospi_turnover_value`, `market_kosdaq_turnover_value` | primary |
| R4 | `term_steep` | `TS_t − median_252(TS)_t` | 기간 스프레드가 1년 중앙값보다 가파름 | `rate_kr_term_spread_10y_3y` | exploratory |
| R5 | `kosdaq_rel_up` | `Σ_{20} ret_kosdaq − Σ_{20} ret_kospi` | 코스닥 20일 상대 강세 | `market_kosdaq_ret_1d`, `market_kospi_ret_1d` | exploratory / reference용 |
| R6 | `krw_weak_20d` | `ln(USDKRW_t / USDKRW_{t−20})` | 원화 20일 약세 | `fx_usdkrw_level` | exploratory |

- `vix_high`(R1b)를 둔 이유: Nagel(2012)의 결과는 VIX **수준**과 반전 수익의 관계다. `vix_up`(변화)은 Kim-Park-Ok(2019)의
  ΔVIX와 맞고, 두 문헌이 다른 변수를 말하므로 둘 다 둔다(리뷰 M6). 중앙값은 과거 252세션이다.
- CGH(2004)는 36개월이 기본이고 12·24개월에도 강건하다. daily 계열이 2014-06-16부터라 24·36개월은 표본을 줄인다.
  **12개월을 primary로 고정**하고 24개월은 진단으로만 낸다.
- R3에서 두 시장 거래대금을 합치는 이유: `IC_t`가 두 시장을 종목 수로 가중한 값이라 국면도 시장 전체여야 한다.
- 연속 변수 `z_t`는 §6.2 대안 cut과 §6.4 연속 회귀 진단에만 쓴다. 판정은 이진 `s_t`로만 한다.

**Phase C 공통 표본.** R1b·R2·R3이 252세션 창을 필요로 해 국면은 2014-06-16에서 252세션 뒤인 **2015-06-16** 전후부터
정의된다(`phase_c.sample_start: 2015-06-16`). 끝은 각 cell의 `common_formation_end`.

**hash 전에 미리 계산해 둘 것 — 완료 (2026-08-29).** 국면 시계열은 결과와 무관하게 국면 변수만으로 정해지므로
overlay 커밋 전에 계산했다. 구현은 `research/analysis/horizon_scan_phase_c_regimes.py`, 결과는 `04` §3.2와
`research/output/horizon_scan/phase_c_regimes/`.

- **G1: 7개 국면 전부 통과.** 판정 창 2015-06-16~2025-02-05(2,368 세션)에서 점유율 0.44~0.56.
- **G2: 7×5 = 35구간 전부 유효.** 걱정했던 `market_up`의 `2023_11_common_end`는 **254/52** — 40 문턱을 12일 차이로
  넘겼다. 표에서 가장 빠듯한 칸이다.
- **지속:** `liq_high` 평균 52~66세션(전환 39회), `market_up` 33~40세션(64회), `vix_up` 6.7~7.6세션(329회).
  앞의 둘은 HAC lag(P5·P6은 59) 밖이라 §5.1의 G4 필수 논거를 데이터가 그대로 확인한다.

리뷰가 fact 격자로 잰 참고값(`vix_up` 0.47·328회·≈8세션, `market_up` 0.57·86회·≈29세션, `liq_high` 0.42·40회·≈63세션)과
같은 자릿수다. 차이는 격자(평일 → 세션)와 표본 창 때문이다.

---

## 3. 검정 쌍 — 사전등록

### 3.1 cell 선택 규칙

family 하나에 cell **하나**를 고정한다. 좌표는 discovery 좌표(`broad × common_survivor`), variant는 family의
`official_feature_variant`(수급은 `lag1`), scan_type `cum`. horizon은 아래 표에 적은 값이고 **family의
`primary_horizon_set` 안에서 골랐다.** 결과를 보고 다른 horizon으로 옮기지 않는다.

### 3.2 쌍

**방향 고정 8쌍 (P1~P8).** 방향은 문헌에서 나온다. `δ`의 부호를 적는다.

| id | family | cell | 국면 | 기대 `δ` | 근거 |
|---|---|---|---|---|---|
| P1 | `px_idio_vol_60d` | cum 0→60 | R1 `vix_up` | **+** (IC가 음수이므로 VIX 상승 뒤 약해짐) | Kim, Park & Ok 2019: VIX 하락 다음 달에만 저IVOL 효과. 국면 변수도 ΔVIX(1개월) |
| P2 | `px_maxret_20d` | cum 0→60 | R1 | **+** (같은 논리) | 같음; MAX와 IVOL은 같은 축(`09` §4) |
| P3 | `px_reversal_5d` | cum 0→5 | **R1b `vix_high`** | **+** (IC 양수, VIX 수준 높을 때 강해짐) | Nagel 2012: 유동성 공급 수익은 VIX **수준**이 높을 때 |
| P4 | `px_mom_12_1` | cum 0→60 | R2 `market_up` | **+** | CGH 2004: 모멘텀은 UP 뒤에만. 한국 기준 IC가 음수(반전)이므로 UP에서 덜 음수 |
| P5 | `px_mom_12_1` | cum 0→60 | R3 `liq_high` | **+** | 이창준·김창하 2018: 시장 유동성 높을수록 모멘텀 이익 |
| P6 | `px_amihud_20d` | cum 0→60 | R3 | **−** (IC 양수, 시장 유동성 낮을 때 프리미엄 커짐) | Amihud 2002 |
| P7 | `flow_foreign_netbuy_to_volume` | cum 0→20 (`lag1`) | R2 | **+** | Kang, Kwon & Park 2014: 외국인 가격영향은 상승장에서만 |
| P8 | `flow_individual_netbuy_to_volume` | cum 0→20 (`lag1`) | R2 | **−** | P7의 거울상(`09` §5) |

**방향 미고정 7쌍 (P9~P15).** 국면 의존을 예상할 근거는 있으나 방향 문헌이 없다. `12`의 양방향 규칙 —
관측 부호를 기록하고 이후 바꾸지 않는다.

| id | family | cell | 국면 | 근거 |
|---|---|---|---|---|
| P9 | `px_turnover_shock` | cum 0→20 | R3 | 박종원 2020: 반전은 고회전율 종목·전환시장에서. 반대 부호 5/5 일관(`08_px_turnover_shock.md`)의 국면 구조 |
| P10 | `flow_inst_netbuy_to_volume` | cum 0→20 (`lag1`) | R2 | 수급 3종 거울상의 세 번째 |
| P11 | `flow_foreign_netbuy_to_volume` | cum 0→20 (`lag1`) | R1 | 원화·VIX와 외국인 수급 공동 움직임(`00_survey/00` §4.3) |
| P12 | `flow_foreign_netbuy_to_volume` | cum 0→20 (`lag1`) | R3 | 수급 IQR ↔ 거래대금 ρ −0.54~−0.72 (`00_survey/00` §4.3) |
| P13 | `flow_inst_netbuy_to_volume` | cum 0→20 (`lag1`) | R3 | 같음 (ρ −0.72) |
| P14 | `flow_individual_netbuy_to_volume` | cum 0→20 (`lag1`) | R3 | 같음 (ρ −0.65) |
| P15 | `px_market_beta` (단계 1a) | cum 0→20 | R2 | `02_feature_candidate.md` §3.6 `regime × beta`. 1a가 돌기 전에는 `insufficient` |

**Reference 2쌍 (X1·X2).** BH 모집단에 넣지 않는다. "국면 의존이 없어야 한다"는 대조군이다.

| id | family | cell | 국면 | 기대 |
|---|---|---|---|---|
| X1 | `fin_log_mcap` | cum 0→60 | R2 | `δ ≈ 0` — 엄철준 외 2024: 규모효과는 시장 상태와 무관 |
| X2 | `fin_log_mcap` | cum 0→60 | R5 `kosdaq_rel_up` | `δ ≈ 0` 기대. 0이 아니면 `cf_small_growth_regime`(`02` §3.6) 가설의 첫 단서 |

**Exploratory (BH 밖, 보고만).** 15개 family 전부 × R4 `term_steep`; P3의 family를 R1 `vix_up`에, P1·P2의 family를
R1b `vix_high`에(국면 변수 교차 확인); `macro_beta_usdkrw` × R6; `macro_beta_vix` × R1; `px_amihud_20d` × R1;
P1·P2·P4의 24개월 R2 변형. 이 결과로 다음 사전등록의 **후보를 고를 수는 있지만** 이번 run에서 판정하지 않는다.

BH 모집단은 **P1~P15의 15쌍**이다. 1a가 아직 없으면 P15는 `insufficient`로 모집단에 남고 p=1.0 처리한다
(`phase_b.post_freeze_blocked_p_for_bh`와 같은 규칙).

---

## 4. 통계량

### 4.1 추정

cell의 일별 IC `IC_t`(`daily_ic.rank_ic`, n_obs 가중 일별 값)와 국면 `s_t`를 `trade_date`로 맞춘다(inner join —
국면은 세션 격자라 빠지는 날이 없다). `s_t`는 formation date t의 값이다 — 예측 시점에 아는 정보만 쓴다.

```
IC_t = α + δ · s_t + ε_t        (OLS)
δ̂ = mean(IC_t | s_t=1) − mean(IC_t | s_t=0)
```

### 4.2 분산 — cell과 같은 HAC, 그리고 그 한계

`IC_t`는 창이 겹쳐 자기상관이 크다. cell의 `t_nw`와 같은 규약으로 Newey-West를 쓴다.

- lag `L = h_end − 1` (cum cell; `stats.nw_lag_cumulative: h_minus_1`)
- gap 정책 `calendar_session_distance`: `formation_session_idx` 차이가 `L` 이하인 쌍만 자기공분산에 넣는다
  (`newey_west_tstat`, `metrics.py:267`의 규약 그대로). 구현은 `research/etl/metrics.py`에 회귀 버전
  `newey_west_ols(y, X, sessions, lag)`를 추가하고, `X`가 상수 하나일 때 `newey_west_tstat`과 일치함을 테스트로 고정한다.
- `t_nw = δ̂ / se_nw`, `p_nw`는 양측 정규.

**HAC는 lag 이내의 자기상관만 보정한다.** `liq_high`의 평균 지속은 약 63세션인데 그 국면을 쓰는 P9·P12~P14는 h=20 cell이라
lag 19다. 국면 지속이 HAC 창을 세 배 넘는다. `market_up`은 연 단위다. 즉 §4.2의 HAC만으로는 국면 지속성 문제를 막지
못하고, **G4 국면 placebo가 유일한 방어선**이다(리뷰 §6.1). 그래서 G4는 모든 쌍에 필수다.

### 4.3 다중검정

- BH q = 0.10, 모집단 15쌍 (`stats.global_bh_q`와 같은 값).
- **discovery** = `q ≤ 0.10` ∧ (방향 고정 쌍이면 `sign(δ̂)` 일치).
- 방향 미고정 쌍은 `q ≤ 0.10`이면 discovery이고 관측 부호를 카드에 고정한다.

---

## 5. 게이트

| 게이트 | 규칙 | 실패 시 |
|---|---|---|
| **G1 점유율** | 각 국면 날짜 수 ≥ 250 이고 전체의 ≥ 20% | `insufficient` (p=1.0으로 BH 모집단 유지) |
| **G2 기간 일관성** | 5개 `common` 구간 중 두 국면이 각각 ≥ 40일인 구간을 유효로 보고, 유효 구간 `δ` 부호가 전체 `δ̂`와 같은 구간이 **⌈유효/2⌉ 이상** | `screen_pass` 실패 |
| **G3 tradable 유지** | 같은 cell의 `tradable × common_survivor` 일별 IC로 `δ̂_tradable`을 구해 `\|δ̂_tradable\| / \|δ̂_broad\| ≥ 0.50` (`run_registry_scan`이 이 combo를 항상 스캔하므로 daily_ic에 있다) | `screen_pass` 실패 |
| **G4 국면 placebo** | §6.1. `p_placebo ≤ 0.10` | `screen_pass` 실패 — **모든 쌍에 필수** |
| G6 대안 cut (진단) | §6.2 | 두 cut의 부호가 다르면 비치명 경고 |

**lag1 유지 게이트는 두지 않는다.** lag1 variant cell은 `run_registry_scan` 밖에서 family 대표 cell 하나만 스캔되고
(`horizon_scan.py:216`·`:423`), 쌍의 cell과 horizon이 다르다. 단계 0의 `daily_ic`에 없고, Phase C는 panel을 다시 만들지
않는다(§7.1)는 원칙과 충돌하므로 뺀다(리뷰 M5). 수급 3 family는 official variant가 이미 `lag1`이다.

**`screen_pass` = discovery ∧ G1 ∧ G2 ∧ G3 ∧ G4.**

등급은 Phase A/B 문자와 뜻을 맞춘다.

| 등급 | 조건 |
|---|---|
| A | screen_pass ∧ 유효 구간 ≥ 4 ∧ G6 부호 일치 |
| B | screen_pass ∧ 비치명 경고(유효 구간 ≤ 3, 또는 G6 부호 불일치 "cut 민감") |
| C | exploratory 쌍, 또는 G1 `insufficient` |
| D | discovery 아님 또는 G2~G4 실패 |
| R | reference 쌍 (X1·X2) — `δ`와 p만 보고 |

### 5.1 왜 G4가 필수인가

`11` §8.4의 경고가 이 검정의 급소다. 국면은 수십~수백 세션 동안 같은 값이고 `IC_t`도 자기상관이 크다. 두 지속
계열 사이에는 아무 관계가 없어도 `δ̂`가 크게 나올 수 있다. HAC가 일부를 잡지만 국면 길이가 창을 넘으면 못
잡는다(§4.2). Phase A의 temporal placebo가 긴 horizon cell을 사실상 다 걸러 냈다는 사실(`00_읽는_법` §9.5)이 그
증거다. 그래서 여기서는 horizon과 무관하게 **모든 쌍**에 국면 placebo를 요구한다.

---

## 6. Placebo와 진단

### 6.1 국면 circular-shift placebo (G4)

- `s_t`만 세션 축에서 `k`만큼 원형 이동한다(`IC_t`는 고정). 두 계열의 자기상관이 그대로 보존되므로 이 목적에 맞는 null이다.
  `k`는 `[120, T−120]`에서 균등 추출, 100회. Phase A `temporal_min_shift_sessions: 120`과 같은 최소 이동이다.
- 복제마다 §4의 `t_nw`를 같은 lag로 다시 계산한다.
- `p_placebo = (1 + #{ |t_k| ≥ |t_real| }) / 101`. Phase A의 `0.0099` 최솟값 규약과 같다.
- seed `20260829`. seed sequence는 `horizon_scan_mapping.mapping_seed_sequence`와 **같은 방식의 새 키**
  `(contract_version, replicate_index, config_hash, pair_id, universe)`로 만든다(기존 키는 `trade_date`·`market`이라 재사용이
  아니다 — 리뷰 §6.6).
- 15 쌍 + 2 reference + exploratory 전부에 돌린다(exploratory는 보고용).

### 6.2 대안 cut (G6, 진단)

이진 cut `z_t > 0`이 자의적이지 않은지 본다. `z_t`를 **과거 252세션 중앙값** 기준으로 다시 이진화해 `δ̂'`를
구하고 `sign(δ̂') == sign(δ̂)`와 `|δ̂'| / |δ̂|`를 기록한다. 부호가 다르면 "cut 민감" 비치명 경고(등급 B).

### 6.3 시장별 분해 (진단)

`daily_ic.rank_ic_kospi`·`rank_ic_kosdaq`로 시장별 `δ̂`를 따로 낸다. Han, Lee & Kang(2020)이 "KOSDAQ 제외 시
모멘텀 급감"을 보고했으므로 P4·P5는 이 분해가 해석에 중요하다. 판정에는 쓰지 않는다.

### 6.4 연속 국면 회귀 (진단)

rank IC는 `|z_t|`를 버리므로 "국면 강도에 비례하는 효과"는 family 스캔으로는 표현할 수 없다(`00_overview` §1.1). 대신
`IC_t = α + γ·z_t + ε_t`를 같은 HAC로 추정해 `γ`의 부호·t를 기록한다. 이번 계약은 이진 `s_t`로 판정하므로 이 값은
진단이고, 다음 사전등록의 후보 재료다.

### 6.5 국면 지속 진단

쌍마다 표본 안의 국면 **전환 횟수**와 **평균 지속 세션 수**를 `conditional_ic.parquet`에 넣는다. 유효 표본이 얼마나
작은지를 독자가 바로 볼 수 있게 하기 위해서다(리뷰 §6.1 권고).

---

## 7. 구현

### 7.1 모듈

`research/analysis/horizon_scan_phase_c.py` (신규). 입력은 파일과 config만이다 — **panel을 다시 만들지 않는다.**
daily_ic를 읽을 A·B run은 config가 아니라 **CLI 인자** `--phase-a-run-id`·`--phase-b-run-id`로 받는다. run_id는 계약이
아니라 실행 인자이고, config에 두면 hash 제외 규칙이 필요해진다(리뷰 §7.4). 대신 `phase_c_run_spec.json`에 두 run_id와
각 `daily_ic` sha256을 적는다.

```
load phase_c block from config (regimes, pairs, stats, placebo)
load daily_ic (phase A run, phase B run)  → 쌍마다 cell 행 필터
build regime_series on the KRX session grid (§2.1)          → regime_series.parquet
for each pair (and universe in broad, tradable):
    join IC_t ⨝ s_t on trade_date, filter sample_start ≤ t
    G1 → δ̂, se_nw, t_nw, p_nw
    subperiod δ̂ (G2), tradable retention (G3)
    placebo (G4), alt cut (G6), market split (§6.3), continuous γ (§6.4), persistence (§6.5)
BH over P1..P15 → q, discovery, screen_pass, grade
write conditional_ic.parquet, regime_placebo_summary.parquet, subperiod_conditional_ic.parquet,
      03c_conditional_ic_results.md, manifest.json, _SUCCESS.json
```

`phase_c_run_spec.json`은 첫 산출물이고 불변이다(`run_spec.json` 규약): config_hash, `phase_c` 블록 해시, 입력
A·B run_id와 그 `daily_ic` sha256, `common_feature_daily_fact` snapshot, KRX 세션 목록의 해시, seed.

### 7.2 산출물 스키마 — `conditional_ic.parquet`

한 행 = 쌍 × universe.

`pair_id, family, feature, scan_type, h_start, h_end, universe, sample_kind, regime_id, regime_role
(primary|reference|exploratory), direction_preregistered (+|-|null), sample_start, sample_end,
n_dates, n_dates_s1, n_dates_s0, share_s1, n_regime_transitions, mean_run_length_s1, mean_run_length_s0,
ic_mean_s1, ic_mean_s0, delta, se_nw, t_nw, p_nw, nw_lag,
q_fdr_phase_c, discovery, valid_subperiods, sign_consistent_subperiods, tradable_delta, tradable_retention,
placebo_p, placebo_repeats, alt_cut_delta, alt_cut_sign_agree, delta_kospi, delta_kosdaq,
continuous_gamma, continuous_t_nw, screen_pass, evidence_grade, status, status_reason`

### 7.3 디렉터리

`research/output/horizon_scan/phase=C/snapshot_date=<…>/source=sj2_remote/config_hash=<overlay>/run_id=<…>/`
— 기존 A/B/AB와 같은 nesting.

### 7.4 테스트

| 파일 | 내용 |
|---|---|
| `tests/unit/test_metrics_newey_west_ols.py` (신규) | 상수만 있을 때 `newey_west_tstat`과 일치; 알려진 AR(1) 데이터의 HAC 분산 근사; gap 정책이 세션 거리로 쌍을 제외 |
| `tests/unit/test_horizon_scan_phase_c.py` (신규) | synthetic daily_ic + regime: (a) `IC_t = a + δ s_t + noise`를 심고 `δ̂` 복원, (b) 무관한 지속 국면에서 placebo p가 큼, (c) G1 점유율 실패 → `insufficient`, (d) BH 모집단이 정확히 15, reference·exploratory 제외, (e) 방향 고정 쌍의 부호 불일치 → discovery false, (f) run_spec 불변, (g) **fact에 비세션 행을 심어도 국면 창이 세션 개수로 잡힘**, (h) CLI run_id가 run_spec에 sha와 함께 기록 |
| `tests/unit/test_horizon_scan_config.py` | `phase_c` 블록 검증: regime id 유일, `pairs[*].regime ∈ regimes[*].id`, 모든 pair의 family·cell이 registry의 primary horizon 안, `exploratory_grid.extra`의 family가 registry에 있음, primary pair 15, reference 2, `sample_start` 형식 |
| synthetic end-to-end | 디렉터리·manifest·`_SUCCESS` — 합성 lake(label_scan + fact)와 합성 daily_ic로 `run_phase_c`를 끝까지 돌려 17행 `conditional_ic.parquet`·보고서·`_SUCCESS.json` 생성 확인 |

**PR-1b 구현 메모 (2026-08-29).**

- `newey_west_ols(y, x, sessions, lag)`는 `x=None`이면 상수만 적합한다. 그때 절편의 t가
  `newey_west_tstat`과 **정확히 같다**(1e-12 이내, gap 있는 세션 축 포함). §4.2가 "cell과 같은 규약"이라고
  적은 것을 코드에서 증명하는 방식이다. 이진 회귀변수에서 `delta`가 두 조건부 평균의 차이와 부동소수점
  수준까지 같다는 것도 테스트로 고정했다.
- **G4가 실제로 작동하는 것을 테스트로 보였다.** 국면과 IC가 둘 다 지속(AR 0.97)이지만 **서로 무관한**
  합성 표본에서 lag 59 HAC로도 `|t|`가 크게 남는데 circular-shift placebo의 `p > 0.10`이 그것을 걸러 낸다.
  반대로 국면이 실제로 IC를 움직이는 표본에서는 `p ≤ 0.10`이다. §5.1의 논거가 말로만이 아니다.
- placebo seed는 `(regime_shift_v1, base_seed, replicate, config_hash, pair_id, universe)` 키다.
  `mapping_seed_sequence`와 같은 구성이되 자기 키를 쓴다 — 그쪽은 `(trade_date, market)` 키라
  날짜 단위 국면 이동에는 뜻이 없다(리뷰 §6.6).
- G6(대안 cut)의 부호 비교는 `finalize_rows`에서 한다. BH 뒤에야 등록된 `delta`가 확정되기 때문이다.

---

## 8. 결과를 어떻게 읽을 것인가 — 미리 적는 해석 규칙

- `screen_pass` 쌍이 하나 이상이면 `12`의 Phase C 개방 조건("경제적으로 설명할 수 있는 조건부 패턴")이
  **처음으로 충족**된다. 그 다음 단계는 (a) 해당 interaction을 acceptance gate의 모델 입력 후보로 올리는 것,
  (b) 국면 변수 자체의 확장(단계 2의 회사채 스프레드 등)이다. 이 문서에서 정하지 않는다.
- P1·P2가 통과하고 P4·P5가 실패하면 "변동성 축은 VIX 국면 의존, 모멘텀 축은 국면 무관"으로 읽는다. 반대면 반대다.
  **둘을 합쳐 "국면 조건화가 통했다/안 통했다"로 뭉뚱그리지 않는다.**
- **쌍들은 강하게 종속적이다.** cell을 공유하는 쌍(P4·P5, P7·P11·P12)과 국면을 공유하는 쌍(`market_up` 5쌍, `liq_high`
  6쌍)이 있다. BH는 PRDS 아래에서 FDR을 지키므로 판정은 유효하지만, 한 국면이 우연히 IC 추세와 겹치면 여러 쌍이 **함께**
  통과하거나 함께 떨어진다. 같은 국면의 쌍이 여럿 통과했다고 그 수만큼 독립된 발견으로 세지 않는다. G4가 이 위험을 쌍
  단위로 재고, §6.5의 지속 진단이 그 크기를 보여 준다.
- X1·X2에서 `|t_nw| > 2`가 나오면 규모 축에 국면 의존이 있다는 뜻이고, 다음 사전등록의 후보가 된다. 이번 판정은 아니다.
- 모든 쌍이 실패해도 결론은 "이 7개 국면 정의, 이 15개 쌍에서는 조건부 패턴이 없다"이지 "매크로는 쓸모없다"가 아니다.
  결과 문서에 이 문장을 그대로 쓴다.

---

## 9. 완료 기준

- [x] `newey_west_ols` + 테스트 (PR-1b). `research/etl/metrics.py`. `x=None`(상수만)일 때 `newey_west_tstat`과
  1e-12 안에서 일치함을 gap 있는 세션 축까지 포함해 고정했다
- [x] `horizon_scan_phase_c.py` + `horizon_scan_phase_c_report.py` + 테스트 27개 (PR-1b)
- [x] config `phase_c` 검증 (PR-1a-3, `_validate_phase_c`)
- [x] hash 전 G1·G2 사전 계산을 `05_preregistration_record.md`에 기록 (실행 3a)
- [x] Phase C run (overlay hash) — `20260830T122850-phasec`, `_SUCCESS.json`, 17쌍 결과
- [x] `03c_conditional_ic_results.md` 자동 생성 + 해설 [`05_results_stage1b_20260830.md`](05_results_stage1b_20260830.md)
- [x] `00_status.md`에 Phase C 개방 조건 **충족** 기록 — `screen_pass` 4쌍(P3·P9·P12·P15)
