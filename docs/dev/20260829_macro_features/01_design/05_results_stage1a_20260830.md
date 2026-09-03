# 05. 단계 1a 결과 — 매크로 exposure 베타 6 family

- 실행일: 2026-08-30
- config: `horizon_scan_macro_20260829.yaml`, hash `236d0d3515043e44…` (사전등록 `05_preregistration_record.md`)
- run: A `20260830T085718-efd35e70` → B `20260830T100518-efd35e70` → AB `20260830T122850-efd35e70`
- snapshot `2026-08-23` / `sj2_remote`
- **판정은 이 run_id의 산출물에서만 읽는다.** 사전등록 계약은 결과를 보기 전에 고정됐고, 이 문서는 계약을 바꾸지 않는다.

---

## 1. 한 줄

**6 family 24 cell 전부 스캔됐고, 16 cell이 discovery·13 cell이 `screen_pass`다.**
`macro_beta_vix`·`macro_beta_wti`·`macro_beta_sp500_lag`·`px_market_beta` 넷이 통과했고,
`macro_beta_usdkrw`·`macro_beta_kr10y` 둘은 전 cell 실패다. **등급 A는 하나도 없다** — 6 family 전부
`fdr_family: macro_exposure`이고, 통과한 13 cell은 모두 **B**다(비치명 경고).

기대 부호를 고정한 유일한 family `macro_beta_vix`는 **부호까지 맞았다**(IC 음수).

---

## 2. cell별 결과

| family | cell | IC | t_nw | q_ab | discovery | screen | 등급 |
|---|---|---:|---:|---:|:-:|:-:|:-:|
| `macro_beta_vix` | cum 0→20 | −0.0133 | −3.24 | 0.0022 | ✅ | ✅ | B |
| `macro_beta_vix` | cum 0→60 | −0.0248 | −3.26 | 0.0020 | ✅ | ✅ | B |
| `macro_beta_vix` | bucket 10→20 | −0.0089 | −3.12 | 0.0030 | ✅ | ✅ | B |
| `macro_beta_vix` | bucket 40→60 | −0.0164 | **−4.16** | **0.00007** | ✅ | ✅ | B |
| `macro_beta_wti` | cum 0→20 | +0.0153 | 3.45 | 0.0011 | ✅ | ✅ | B |
| `macro_beta_wti` | cum 0→60 | +0.0238 | 3.16 | 0.0027 | ✅ | ✗ | C |
| `macro_beta_wti` | bucket 10→20 | +0.0104 | 3.48 | 0.0010 | ✅ | ✅ | B |
| `macro_beta_wti` | bucket 40→60 | +0.0147 | 3.48 | 0.0010 | ✅ | ✅ | B |
| `px_market_beta` | cum 0→20 | −0.0334 | −3.06 | 0.0037 | ✅ | ✅ | B |
| `px_market_beta` | cum 0→60 | −0.0492 | −3.46 | 0.0010 | ✅ | ✗ | C |
| `px_market_beta` | bucket 10→20 | −0.0299 | −3.71 | 0.0004 | ✅ | ✅ | B |
| `px_market_beta` | bucket 40→60 | −0.0352 | −3.44 | 0.0011 | ✅ | ✅ | B |
| `macro_beta_sp500_lag` | cum 0→20 | +0.0125 | 2.80 | 0.0083 | ✅ | ✅ | B |
| `macro_beta_sp500_lag` | cum 0→60 | +0.0211 | 2.58 | 0.0150 | ✅ | ✗ | C |
| `macro_beta_sp500_lag` | bucket 10→20 | +0.0068 | 2.10 | 0.0509 | ✅ | ✅ | B |
| `macro_beta_sp500_lag` | bucket 40→60 | +0.0127 | 2.75 | 0.0094 | ✅ | ✅ | B |
| `macro_beta_usdkrw` | 4 cell 전부 | +0.0003 ~ +0.0038 | 0.06 ~ 1.38 | 0.21 ~ 1.00 | ✗ | ✗ | C/D |
| `macro_beta_kr10y` | 4 cell 전부 | +0.0034 ~ +0.0064 | 0.75 ~ 1.40 | 0.21 ~ 0.52 | ✗ | ✗ | C/D |

---

## 3. 읽는 법 — family별

### `macro_beta_vix` — 기대 부호 `−`, 맞았다

사전등록에서 방향을 고정한 **유일한** family다(Ang et al. 2006의 FVIX 로딩, Bali-Brown-Tang 2017).
4 cell 전부 IC가 음수이고 전부 `screen_pass`다. bucket 40→60의 `q = 0.00007`은 24 cell 중 가장 작다.

**VIX 상승에 같이 오르는 종목의 이후 수익이 낮다** — 헤지 자산으로서 프리미엄을 덜 요구받는다는
해석이 한국 횡단면에서 재현됐다. 사전에 방향을 못 박았고 그대로 나왔다는 점에서 이 결과의
증거력이 가장 높다.

### `macro_beta_wti` — 양방향 등록, 관측 부호 `+`로 고정

4 cell 전부 IC 양수, t 3.16~3.48. **유가 민감도가 높은 종목의 이후 수익이 높다.**
`12`의 양방향 규칙대로 **관측 부호 `+`를 이제 고정하고, 이후 바꾸지 않는다.**
방향 문헌이 없어 양방향으로 등록했으므로(RIBAF 2025의 섹터 비대칭만 근거), 이 부호는 이번 관측이
정한 것이지 예측이 아니다. 카드에 `direction_preregistered: null`이 그대로 남는다.

### `px_market_beta` — 양방향 등록, 관측 부호 `−`

4 cell 전부 IC 음수. **저베타 이례현상(BAB, Frazzini-Pedersen 2014)의 한국판이 재현됐다.**
사전등록 시점에 "한국 재현 근거 없음"이라 방향을 고정하지 않았는데, 결과는 BAB 방향이다.
관측 부호 `−`를 고정한다.

절대값이 가장 크다(cum 0→60에서 −0.049). 다만 `px_market_beta`는 규모·변동성 축과 겹칠 수 있으므로
`02` §4의 상관 진단을 카드에서 반드시 같이 본다.

### `macro_beta_sp500_lag` — 양방향 등록, 관측 부호 `+`

4 cell 전부 양수, t 2.10~2.80으로 통과 넷 중 가장 약하다. bucket 10→20의 `q = 0.051`은 임계 0.10을
넘지만 여유가 크지 않다. **전일 S&P500에 민감한 종목의 이후 수익이 높다** — RSZ 2013의 정보 점진
확산과 부호가 맞지만, 그 표본에 한국이 없었으므로 이번이 첫 관측이다.

### `macro_beta_usdkrw` — 전 cell 실패

IC가 사실상 0이다(+0.0003 ~ +0.0038, t 최대 1.38). `00_survey/01` §5.8이 정리한 "환율 베타 부호
불안정"(고강석 2019)과 일관된다. **환노출이 횡단면 수익을 가르지 못한다**는 것이 이번 표본의 답이다.

`02` §2.2가 국내 요인의 짝짓기를 세심하게 정한 것(창 B, `(resid_ret_τ, g_τ)`)을 감안하면, 시점 정렬
문제로 신호가 사라진 것은 아니다. 다만 매매기준율 산출 방식이 미검증이라는 한계(`04` §5)는 그대로 남는다.

### `macro_beta_kr10y` — 전 cell 실패

t 0.75~1.40. `00_survey/01` §5.8이 "한국 횡단면 검정 공백"이라 한 자리를 메운 결과는 **공백이 아니라
효과 없음**이다. 듀레이션·성장주 할인율 대용으로서의 금리 베타는 이 표본에서 작동하지 않는다.

---

## 4. 결합 BH에 미친 영향

| | canonical (`889c3e83`) | 이번 (`236d0d35`) |
|---|---|---|
| Phase B cell | 78 | **102** |
| 결합 BH 모집단 `m_ab` | 153 | **177** |
| discovery | 87 | **103** |
| `screen_pass` | 40 | **53** |
| Phase B 등급 | A23·B17·C35·D3 | A23·**B30**·**C40**·**D9** |
| 결합 permutation `p` | 0.0099 | 0.0099 |

**기존 Phase A discovery 변화 0개**(`phase_a_card_overlay.parquet`, 75행). `00_overview` §1.3이
"1a가 바꾸는 것은 기존 cell의 `q_fdr_global_ab`뿐"이라 예고한 그대로다.

**공통 153 가설의 등급 변화도 0개다.** `q_fdr_global_ab`는 모집단이 153 → 177로 커져 145행이
움직였고 최대 변화는 0.0074인데, 어느 것도 판정을 뒤집지 않았다.

---

## 5. 마트와 커버리지

`feat_macro_exposure` (snapshot 2026-08-23, config hash `236d0d35…`): 7,024,118행, 빌드 14.4초.

| 컬럼 | 첫 값 | 종목 |
|---|---|---|
| `macro_beta_usdkrw`·`_kr10y` | 2014-12-17 | 2,976 |
| `macro_beta_wti`·`_sp500_lag`·`_vix` | 2014-12-22 | ~2,975 |
| `px_market_beta` | 2007-12-07 | 3,137 |
| `macro_semibeta_usdkrw_up` | 2014-11-27 | 2,972 |

2024년 횡단면 분포로 본 온전성: `px_market_beta` 중앙값 **0.99**(5–95%: 0.32–1.75). 자세한 것은
`02` §2.5a·§2.5b.

---

## 6. §4 진단 — residual 정의가 옳았다는 증거

`core/secondary_feature_diagnostics.parquet` (discovery 좌표 broad × common_survivor, cum 20·60).
`02` §1이 primary를 원수익률이 아니라 **market-model 잔차**로 정의한 근거를 이 표가 직접 검증한다.

| family | h | primary IC (잔차) | secondary IC (원수익률) | 차이 |
|---|---:|---:|---:|---:|
| `macro_beta_vix` | 20 | **−0.0133** | **+0.0077** | +0.0210 |
| `macro_beta_vix` | 60 | **−0.0248** | **+0.0088** | +0.0336 |
| `macro_beta_sp500_lag` | 20 | **+0.0125** | **−0.0075** | −0.0200 |
| `macro_beta_sp500_lag` | 60 | **+0.0211** | **−0.0102** | −0.0313 |
| `macro_beta_wti` | 20 | +0.0152 | +0.0089 | −0.0063 |
| `macro_beta_wti` | 60 | +0.0238 | +0.0119 | −0.0119 |
| `macro_beta_usdkrw` | 20 | +0.0008 | +0.0072 | +0.0064 |
| `macro_beta_usdkrw` | 60 | +0.0003 | +0.0117 | +0.0113 |
| `macro_beta_kr10y` | 20 | +0.0034 | −0.0009 | −0.0044 |
| `macro_beta_kr10y` | 60 | +0.0058 | +0.0004 | −0.0055 |

**`macro_beta_vix`와 `macro_beta_sp500_lag`은 원수익률로 재면 부호가 뒤집힌다.**
`02` §1이 예측한 그대로다 — `Cov(r_i, f) = β_{i,m}·Cov(r_m, f) + Cov(e_i, f)`에서 원수익률 베타는
**시장 베타 × (시장–요인 공분산)에 물든다.** `px_market_beta`의 IC가 음수(BAB)이고 고베타 종목이
VIX 상승에 더 크게 빠지므로, 원수익률 VIX 베타는 시장 베타 신호를 부호만 뒤집어 물려받는다.
잔차로 재야 진짜 exposure가 보인다.

**만약 primary를 원수익률로 정의했다면 `macro_beta_vix`의 사전등록 기대 부호 `−`는 틀린 것으로
판정됐을 것이다.** 사전등록 시점에 정한 정의가 결과를 갈랐다.

`macro_beta_usdkrw`는 반대 방향의 사례다 — 잔차로는 0(+0.0008)인데 원수익률로는 +0.0072~+0.0117로
커진다. **환율 베타로 보이던 것의 실체가 시장 베타였다**는 뜻이고, `00_survey/01` §5.8의
"환율 베타 부호 불안정"과 맞는다. 이 family가 실패한 것은 신호가 시장 축에 흡수됐기 때문이다.

세미베타(`macro_semibeta_usdkrw_up`)는 −0.0064 ~ −0.0098로 primary와 부호가 반대다.
**secondary는 진단이다** — `04` §5의 "실패한 family를 secondary 결과로 대신 판정하지 않는다"에 따라
`macro_beta_usdkrw`의 판정은 실패 그대로다.

### 규모·변동성 축 중복 — 경고 없음

`core/primary_feature_rank_correlation.parquet` (Phase A ready family × 매크로 family).

| 쌍 | 평균 rank ρ |
|---|---:|
| `px_near_52w_high` × `px_market_beta` | −0.345 |
| `px_idio_vol_60d` × `px_market_beta` | +0.295 |
| `px_maxret_20d` × `px_market_beta` | +0.282 |
| `px_amihud_20d` × `px_market_beta` | −0.244 |
| 그 외 매크로 베타 5종의 최대 | \|ρ\| ≤ 0.093 |

**`|ρ| ≥ 0.7`인 쌍은 0개이고, `≥ 0.5`도 0개다.** `02` §4가 "`|ρ| ≥ 0.7`이면 카드에 '규모 축 중복'
경고를 단다"고 정한 조건에 걸리는 것이 없다. `px_market_beta`가 변동성 축(`px_idio_vol_60d`
+0.295, `px_maxret_20d` +0.282)과 가장 겹치지만 0.3 미만이고, **매크로 요인 베타 5종은 기존
어느 축과도 사실상 독립이다**(최대 0.093). 새 축을 더한 것이 맞다.

---

## 7. 남은 일

- 양방향 4 family의 **관측 부호를 고정 기록**했다(§3). 이후 바꾸지 않는다.
- holdout(2025-08-01~)은 열지 않았다.
- acceptance gate 후보 승격은 이 문서에서 정하지 않는다.
