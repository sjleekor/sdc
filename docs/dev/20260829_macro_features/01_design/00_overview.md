# 00. 매크로 피쳐 단계 0·1 설계 — 개요와 결정

- 작성일: 2026-08-29
- 입력: `../00_survey/` 세 문서. 특히 `02_candidate_indicators_and_sources.md` §8의 단계표.
- 목적: **새 수집 없이** 할 수 있는 두 단계를 사전등록 계약으로 못 박는다.
  - **단계 0** — Horizon Scan이 계산하고 버리는 일별 IC 시계열을 저장한다. 가설이 아니라 산출물 계약이다.
  - **단계 1a** — 종목별 매크로 exposure 베타 6 family를 Phase B에 추가한다. 보통의 횡단면 가설이다.
  - **단계 1b** — 국면별 조건부 IC(Phase C)를 연다. 기존 family의 IC가 국면에 따라 달라지는지 검정한다.
- 이 디렉터리의 문서는 **결과를 보기 전에** 쓴 것이다. 사전등록 규율은 `04_preregistration_overlay.md` §5.

| 문서 | 내용 |
|---|---|
| [01_stage0_daily_ic_persistence.md](01_stage0_daily_ic_persistence.md) | `daily_ic.parquet` 산출물 계약, 코드 변경 지점, 정합성 검증 |
| [02_stage1a_exposure_beta_families.md](02_stage1a_exposure_beta_families.md) | `feat_macro_exposure` 마트 산식, 6 family 사전등록 |
| [03_stage1b_conditional_ic_phase_c.md](03_stage1b_conditional_ic_phase_c.md) | 국면 정의, 검정 쌍과 기대 방향, 통계량, 게이트, placebo |
| [04_preregistration_overlay.md](04_preregistration_overlay.md) | config overlay 초안, 실행 순서, PR 분할, 검증 목록, 규율 |
| [05_preregistration_record.md](05_preregistration_record.md) | **사전등록 기록** — 확정 hash `236d0d35…`, 국면 사전 계산값, 보존 규칙 |

---

## 1. 결정 — 한 장

### 1.1 interaction(②)은 이 검증 틀에서 조건부 IC(③)와 같은 것이다

조사 문서(`00_survey/00` §3)는 매크로를 쓰는 길을 셋으로 나눴다 — ① exposure 베타, ② 국면 × 종목
특성 interaction, ③ 국면별 조건부 IC. 설계하면서 ②와 ③이 **rank IC 아래에서는 같은 검정**이라는
것이 확인됐다.

날짜 상수 `s_t`(국면 변수)와 종목 특성 `x_{i,t}`의 곱 `s_t · x_{i,t}`를 날짜 안에서 순위 매기면, `s_t > 0`인
날은 `x`의 순위 그대로이고 `s_t < 0`인 날은 순위가 뒤집힌다. 따라서

```
IC_t(s · x) = sign(s_t) · IC_t(x)
```

이다(`s_t = 0`인 날은 피쳐가 상수가 되어 그 횡단면이 통째로 빠진다). 그래서 interaction family의 평균 IC는
**어느 코딩이든 `δ = E[IC | s=1] − E[IC | s=0]`가 아니다** (리뷰 `06` §3.2).

- `s ∈ {0, 1}` 코딩: `s=0`인 날이 전부 탈락해 `ic_mean(s·x) = E[IC | s=1]` — 한쪽 조건부 평균이다.
- `s ∈ {−1, +1}` 코딩: `P(s>0)·E[IC|s>0] − P(s<0)·E[IC|s<0] = (P₁ − P₀)·E[IC] + 2·P₁·P₀·δ` — **국면 점유율이
  불균형이면 `δ = 0`이어도 0이 아니다.**

즉 interaction을 보통 family로 스캔하면 "국면에 따라 달라지는가"를 재지 못한다. 바른 검정은 `δ`이고,
그것은 일별 IC 시계열(단계 0)이 있으면 바로 계산할 수 있다.

두 가지를 더 적어 둔다. **연속 국면 가중은 이 틀에서 표현 자체가 불가능하다** — rank IC는 `|s_t|`를 완전히
버리므로 `z_t · x`를 등록해도 `sign(z_t)·IC_t(x)`일 뿐이다(크기 가중은 Phase C의 연속 회귀 진단 `03` §6.4로만
본다). 그리고 interaction family를 Phase A/B로 스캔하면 **temporal placebo가 `s·x`를 통째로 밀어** 국면 지속성
때문에 약해지는데(`11` §8.4), 조건부 `δ` + 국면 circular-shift placebo(`03` G4)는 그 문제를 정면으로 다루는
더 좋은 검정이다.

> **결정 1.** interaction은 Horizon Scan family로 등록하지 않는다. 같은 가설을 단계 1b(Phase C)의 조건부
> IC 차이 검정으로 등록한다. interaction을 **모델 입력**으로 쓰는 문제(walk-forward에서 트리·선형 모형이
> `x × s`를 학습)는 acceptance gate 단계의 일이며, 이 문서 범위 밖이다.

같은 논리로 "종목 exposure × 매크로 충격"(`beta_i × Δmacro_t`)도 beta family의 조건부 IC로 환원된다.
그래서 단계 1a의 베타 family들은 보통 family로 스캔하고, 그 국면 의존은 1b의 쌍으로 등록한다.

### 1.2 단계 0이 먼저다

- Phase C(1b)는 일별 IC 없이는 존재할 수 없다.
- 단계 0은 **기존 결과를 하나도 바꾸지 않아야** 한다. 요약 통계(`ic_mean`, `t_nw`, `q_fdr_*`, 등급)가 canonical
  run과 정확히 같아야 하고, 저장된 일별 IC의 평균이 `ic_mean`과 1e-12 안에서 맞아야 한다. 이것이 단계 0의
  유일한 합격 기준이다.
- 단계 0은 `00_읽는_법.md` §7(a)와 `08_phase_b_implementation_log.md` §4.3 Stage 3이 미뤄 둔 바로 그 일이다.
  부수 효과로 `00_읽는_법.md` §9.2·9.3의 A×A·B×B 중복 진단(일별 IC 상관)과 연도별 IC가 같이 열린다.

### 1.3 단계 1a와 1b는 서로 독립이다

| | 1a exposure 베타 | 1b 조건부 IC |
|---|---|---|
| 새 가설 | 6 family × 4 cell = 24 (Phase B 78 → 102, 결합 BH 153 → 177) | 15 검정 쌍 + 2 reference |
| 새 마트 | `feat_macro_exposure` | 없음 (`daily_ic.parquet` + `common_feature_daily_fact`를 KRX 세션 격자에서 읽기) |
| 새 config | overlay `horizon_scan_macro_20260829.yaml` (`families_append` 6) | 같은 overlay의 `phase_c` 블록 |
| 선행 조건 | scan snapshot에 `common_feature_daily_fact` 빌드 | 단계 0 + overlay Phase A·B run(daily_ic 보유) |
| 결과가 바꾸는 것 | 기존 cell의 `q_fdr_global_ab` (모집단 증가) | 기존 등급은 그대로. 별도 Phase C 판정 추가 |

실행은 한 lineage다(`04` §3): overlay hash로 A → B → AB → C. 1b의 15쌍 중 14쌍은 기존 family라 A·B run의
daily_ic만 있으면 되고, `px_market_beta × market_up`(P15)만 1a의 결과를 기다린다.

### 1.3a 리뷰 반영 (2026-08-29)

`06_review_20260829.md`의 지적 중 계약에 들어가는 것 일곱(M1~M7)과 정확성 항목(§8)을 반영했다. 항목별 반영 여부는
`07_review_response_20260829.md`에 있다. 결정이 필요했던 둘은 이렇게 정했다 — **M1** 국내 요인(환율·국고채)은 fact에서
한 세션 지연된 값이므로 `resid_ret_τ`를 τ+1에 공개된 변화와 짝짓고 창을 직전 세션에서 끝내 동시 exposure를 만든다
(`02` §2.2~2.3). **M3** fact의 날짜 격자가 2014~2023년에 평일이므로 Phase C 국면은 KRX 세션 격자에서 계산하고 fact는
다시 만들지 않는다(`03` §2.1).

### 1.4 하지 않는 것

- 매크로 level·Δ·YoY를 Phase B family로 등록하지 않는다 (`12`, `00_survey/00` §3).
- N8 고용 regime은 dormant 그대로 둔다. 이번 Phase C 국면 변수는 VIX 변화·VIX 수준·시장 상태·유동성 네 개(+ 탐색 셋)다.
  `12`가 사전등록한 `macro_unemployment_rate_level`·`macro_employment_rate_level`은 이번 계약의 국면
  목록에 넣지 않는다 — 넣으려면 `12`의 "부호 반전 또는 경제적으로 설명 가능한 조건부 패턴" 조건을 먼저
  만족해야 하고, 그 판단 재료가 바로 1b의 결과다.
- 새 수집(ECOS 회사채·기준금리, 관세청 수출, SOX)은 단계 2·3이다. 여기서 다루지 않는다.
- SUE의 `cohort_ic.parquet`(event 코호트 IC 시계열)는 `fin_sue`에 표본이 없어(`22_fin_sue.md`) 이번에 만들지 않는다.
- holdout(2025-08-01~)은 열지 않는다.

---

## 2. 왜 이 순서인가 — 조사 문서에서 넘어온 근거

| 근거 | 출처 | 이 설계에서의 자리 |
|---|---|---|
| 일별 IC를 계산하고 평균만 남긴다 | `00_읽는_법` §4.2, §7 | 단계 0 |
| 수급 3종 IQR ↔ 시장 거래대금 ρ −0.65~−0.72 | `00_survey/00` §4.3 | 1b 유동성 국면 쌍 |
| VIX 하락 뒤 달에만 저변동성 효과 (Kim, Park & Ok 2019) | `00_survey/01` §5.7 | 1b `px_idio_vol_60d`·`px_maxret_20d × vix_up` |
| 모멘텀은 상승장 뒤에만 (CGH 2004), 한국 반전은 전환시장·고회전율에서 강함 (박종원 2020) | `00_survey/01` §3.2, §5.7 | 1b `px_mom_12_1 × market_up`, `× liq_high`, `px_turnover_shock × liq_high` |
| 외국인 가격영향은 상승장에서만 (Kang, Kwon & Park 2014) | `00_survey/01` §5.7 | 1b `flow_foreign_* × market_up` |
| 반전 전략 수익은 VIX 높을 때 (Nagel 2012) | 이 문서 §2.1 | 1b `px_reversal_5d × vix_up` |
| 규모효과는 국면 무관 (엄철준 외 2024) | `00_survey/01` §5.7 | 1b reference 쌍 `fin_log_mcap` |
| FX 세미베타 프리미엄 (Chu 2022), 환율 베타 부호 불안정 (고강석 2019) | `00_survey/01` §5.8 | 1a `macro_beta_usdkrw` 양방향 + 하방 세미베타 secondary |
| ΔVIX 민감도 높은 종목의 낮은 수익 (Ang et al. 2006; Bali, Brown & Tang 2017) | `00_survey/01` §3.2 | 1a `macro_beta_vix` 기대 부호 `−` |
| 금리 베타의 한국 횡단면 검정 공백 | `00_survey/01` §5.8 | 1a `macro_beta_kr10y` 양방향 |
| exposure는 종목별로 달라 temporal placebo에 가장 안전 | `11` §8.4 | 1a를 보통 family로 |
| 국면 변수는 오래 같은 값 → 시계열을 밀어도 비슷한 신호 | `11` §8.4 | 1b의 **국면 circular-shift placebo를 필수 게이트**로 |

### 2.1 이 문서에서 새로 인용한 것

- Nagel, S. (2012) "Evaporating Liquidity", *RFS* 25(7). 단기 반전(유동성 공급) 전략의 기대수익이 VIX가
  높을 때 크다. https://academic.oup.com/rfs/article-abstract/25/7/2005/1602153
- Ang, Hodrick, Xing & Zhang (2006) "The Cross-Section of Volatility and Expected Returns", *JF* 61(1).
  ΔVIX 민감도(FVIX 로딩)가 높은 종목의 평균 수익이 낮다. https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2006.00836.x
- Frazzini & Pedersen (2014) "Betting Against Beta", *JFE* 111(1). 저베타 이례현상 — `px_market_beta`의
  무조건부 방향을 고정하지 않은 이유(한국 재현 근거 없음). https://www.sciencedirect.com/science/article/pii/S0304405X13002675

---

## 3. 산출물이 어디에 생기나

```
research/output/horizon_scan/
  phase=A/…/config_hash=889c3e83…/run_id=<재실행>/core/daily_ic.parquet        ← 단계 0
  phase=B/…/config_hash=889c3e83…/run_id=<재실행>/daily_ic.parquet             ← 단계 0
  phase=C/…/config_hash=<macro overlay>/run_id=<…>/conditional_ic.parquet     ← 단계 1b
  phase=B/…/config_hash=<macro overlay>/run_id=<…>/horizon_ic.parquet          ← 단계 1a (6 family 추가)
  phase=AB/…/config_hash=<macro overlay>/run_id=<…>/combined_ab_primary_hypotheses.parquet

data_lake/feature_mart/snapshot_date=<…>/source=sj2_remote/feat_macro_exposure/   ← 단계 1a 마트
research/analysis/horizon_scan_macro_20260829.yaml                                 ← overlay
research/analysis/horizon_scan_phase_c.py                                          ← 단계 1b 모듈
```

---

## 4. 완료 기준 (요약)

**전부 완료 (2026-08-30).** 기록은 [`05_preregistration_record.md`](05_preregistration_record.md) §실행 결과.

- [x] 단계 0: Phase A 요약이 canonical과 exact match(max |Δ| 1.388e-17), `daily_ic` 정합성 검사 통과
  (`daily_ic_reconciled: true`, 차이 0.0), legacy/native parity는 공통 행 2.776e-16 통과 + 행 집합 474행
  차이(native 엔진 결함 I13, 단계 0과 무관)
- [x] 단계 1a: overlay hash `236d0d35…` 기록 → 마트 빌드 → Phase B·AB → **기존 Phase A discovery 변화 0개** →
  24 cell 중 discovery 16·`screen_pass` 13
- [x] 단계 1b: Phase C run — **`screen_pass` 4쌍**(P3·P9·P12·P15), G4가 15쌍에서 BH와 완전 일치
- [x] `00_status.md` 반영. **`12`의 "조건부 패턴" 조건은 충족됐다** — 근거는 `05_results_stage1b_20260830.md`
