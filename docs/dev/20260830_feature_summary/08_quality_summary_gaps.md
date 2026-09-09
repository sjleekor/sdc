# 08. 품질 종합·공백·horizon별 배치 — 모델 개발 전에 알아야 할 것

- 작성일: 2026-09-03
- 이 문서는 `02`~`06` 카드에 흩어진 품질 정보를 모델 설계 관점에서 한곳에 모은다. 새 판정을 내리지 않는다.

---

## 1. 41 family 판정 종합 (결합 AB 기준)

| 그룹 | family | A | B | C | D | R | screen_pass family |
|---|---|---:|---:|---:|---:|---:|---|
| 가격·유동성 (`02`) | 9 | 5 | 0 | 0 | 3 | 1 | `px_reversal_5d`, `px_near_52w_high`, `px_maxret_20d`, `px_idio_vol_60d`, `px_amihud_20d` |
| 수급·공매도 (`03`) | 8 | 1 | 0 | 4(보류) | 3 | 0 | `flow_individual_netbuy_to_volume` |
| 재무·규모·밸류·인적자본 (`04`) | 9 | 2 | 4 | 3 | 0 | 0 | `fin_log_mcap`, `mcap_krx_log`(2 cell), `fin_value_z`(1), `fin_gross_profitability`(5), `hc_employee_growth`(1), `hc_productivity`(1) |
| 공시·이벤트·지분 (`05`) | 9 | 5 | 3 | 1 | 0 | 0 | `ev_amendment_ratio`, `ev_filing_activity`, `ev_net_share_issuance_yoy`(1), `ev_payout_yield`(3), `own_amendment_ratio`, `own_major_filing_activity`, `own_major_stake_change`, `own_major_stake_level`(2) |
| 매크로 exposure (`06`) | 6 | 0 | 4 | 2 | 0 | 0 | `macro_beta_vix`, `macro_beta_wti`(3), `macro_beta_sp500_lag`(3), `px_market_beta`(3) |
| **합** | **41** | **13** | **11** | **10** | **6** | **1** | |

- cell이 갈리는 family(`A1/C3` 등)는 상위 등급으로 셌다. 정확한 cell 분포는 `01_feature_inventory.md`.
- 통과 family는 축 넷으로 모인다: **규모·유동성·변동성**(idio_vol·amihud·maxret·log_mcap·mcap_krx·market_beta), **단기 반전과 개인 수급**(reversal·individual netbuy), **공시 활동·주주환원·밸류**(amendment·filing·payout·value_z·gross_profitability·issuance), **매크로 exposure**(vix·wti·sp500). 축 사이 상관은 낮다(A×B 84쌍 대부분 |ρ| ≤ 0.15, 매크로×A 최대 0.345).
- **모멘텀 계열은 한국에서 반대다.** `px_mom_12_1`, `px_resid_mom_12_1`, `px_turnover_shock`, `flow_inst_*`, `flow_foreign_*`가 기대와 반대 부호. Han-Lee-Kang(2020)의 한국 복제율(수익성 5%, 투자 24%, 밸류 69%, 거래마찰 48%)이 이번 등급을 카테고리 단위로 예측했다 — 데이터 결함이 아니라 시장 특성일 가능성이 크다(`11_feature_taxonomy` §4).
- Phase C: 15쌍 중 4쌍 통과(P3·P9·P12·P15), 독립 발견은 셋(유동성 국면·VIX 수준·시장 상태).

---

## 2. 횡단 품질 이슈 — 모델에 넣기 전에

### 2.1 IC와 5분위 spread가 자주 어긋난다

| 유형 | 사례 | 모델에서 |
|---|---|---|
| 부호까지 반대 | `ev_net_share_issuance_yoy` IC −0.038 / spread +11.3%p, `hc_productivity` 네 cell 전부, `px_near_52w_high` 여섯 cell 전부, `px_turnover_shock` | 순위 모델 입력이면 IC, 분위 롱숏이면 spread. **상승 확률 모델은 양 끝 분위의 실현 비율을 보므로 spread 쪽이 더 가깝다.** 이 피쳐들은 단조 가정이 깨졌을 수 있다 — 트리 모델이 낫고, 선형에는 넣지 않는다 |
| 크기 어긋남 | `ev_payout_yield` \|IC\| 0.10 / spread 0.49%p, `px_idio_vol_60d` \|IC\| 1위 / spread 4위 | 순위 신호. 확률 모델에서 극단 분위 기여가 작을 수 있다 |
| 계산 불가 | `own_amendment_ratio_1y` 동점으로 NaN | 0 과다·동점형. indicator + 크기 분리 |

원인은 IC가 순위 상관이고 spread는 양 끝 평균이라는 데 있다. **분위별 평균수익률·중앙값 spread를 아직 산출하지 않았다.** 이걸 풀 자료가 없다(§3-2).

### 2.2 A×A·B×B 상관이 없다 — 독립성을 말할 수 없다

A×B 교차 204쌍과 매크로×A만 계산했다. 같은 마트 형제 family의 중복은 구조적으로만 안다.

| 마트 | family | 관계 |
|---|---|---|
| `feat_filing_activity` | 18·19·31·32·33 | **전체 대 부분집합** |
| `feat_fin_scan_daily` | 23·24·25·26·27 | 분모 공유 |
| `feat_periodic_extras` | 29·30·34·35 | 분자 공유, 수준 대 차분 |
| `feat_event_scan_daily` | 20·21 | 같은 축의 반대편 |
| `feat_price` | 05×06 (MAX·idio_vol: 신호 모양·제3 피쳐 상관 부호까지 같음), 10·11·12 (거의 거울상), 15×16 (분자 같음), 17⊃15 | 사전 설계가 05×06 ablation을 요구했는데 안 했다 |

**T2 14개 안의 확인된 중복 셋**: `fin_log_mcap`·`mcap_krx_log`, `ev_amendment_ratio_1y`·`own_amendment_ratio_1y`, `own_major_stake`·`own_major_stake_chg`. 14개가 14개의 독립 정보가 아니다. 단계 0으로 `daily_ic.parquet`이 생겨 일별 IC 상관으로 A×A·B×B를 잴 수 있게 됐다 — 아직 안 했다.

### 2.3 `|ρ| ≥ 0.7`은 두 쌍뿐인데 둘 다 규모 축이다

`px_amihud_20d` × `mcap_krx_log` **−0.754**, × `fin_log_mcap` **−0.721**. 그리고 5분위 spread 상위 셋(+11.83 / +11.21 / +11.19%p)이 전부 이 축이다. **비유동성 프리미엄과 소형주 효과를 구분하지 못한다.** 규모를 통제한 증분 IC가 없다. 모델에서는 이 셋을 다 넣으면 하나의 축을 세 번 세는 것이고, 소형주 편중이 top-k를 지배한다. `tradable_retention`(amihud 0.85, mcap_krx_log 0.56~0.69)을 같이 봐야 한다.

### 2.4 시간 placebo가 긴 구간을 거의 다 걸러 낸다 — 그리고 짧은 구간은 받지 않았다

NW lag ≥ 59 cell만 대상. 받은 cell 대부분이 떨어졌다.

| 통과 | `px_maxret_20d`, `px_idio_vol_60d`, `px_amihud_20d`, `ev_amendment_ratio`, `fin_log_mcap`, `own_amendment_ratio`, `own_major_filing_activity`, `own_major_stake_change`, `ev_filing_activity`(0.069) |
|---|---|
| **실패** | `px_mom_12_1`, `px_resid_mom_12_1`, `px_near_52w_high`, `flow_foreign_holding_ratio_chg`, `ev_net_share_issuance_yoy`, `ev_payout_yield`, `fin_accruals_to_assets`, `fin_asset_growth_yoy`, `fin_gross_profitability`, `fin_value_z`, `hc_employee_growth`, `hc_productivity`, `own_insider_filing_activity`, `own_major_stake_level` |

h40–60 bucket(width 20)으로 screen_pass한 A·B는 이 검사를 **받지 않은** 것이다(`robustness_required=False`). `fin_value_z`·`fin_gross_profitability`·`ev_payout_yield`·`hc_*`의 통과 cell이 그렇다. 긴 h 실험에 이 피쳐들을 넣을 때 시간 placebo 실패 이력을 기억해야 한다 — 국면 지속성이 만든 가짜 신호일 수 있다.

### 2.5 부호가 반대인 것과 신호가 없는 것은 다르다

| 유형 | 기간 일치 | family | 모델에서 |
|---|---|---|---|
| **반대 방향이 안정적** | 0/5 (전 구간 반대) | `px_turnover_shock`, `flow_foreign_netbuy_to_volume`, `flow_inst_netbuy_to_volume` | **부호를 뒤집어 넣을 수 있는 정보.** 단 `flow_foreign_*`는 지연 게이트(`delay_pass`, h≤5 cell `p_nw` 0.053 > 0.05) 실패, `px_turnover_shock`는 `liq_high` 국면 의존(Phase C P9) |
| 방향 불안정 | 2/5 | `px_mom_12_1`, `px_resid_mom_12_1`, `flow_foreign_holding_ratio_chg` | 근거 없음 |
| **신호 자체 없음** | 절반 안팎 | `fin_asset_growth_yoy`(\|IC\| 0.004), `own_insider_filing_activity`, `macro_beta_usdkrw`, `macro_beta_kr10y` | 넣지 않는다 |

### 2.6 `|IC|`가 0에 가까우면 비율 지표가 폭주한다

`tradable_retention = |tradable IC| / |broad IC|`. `fin_asset_growth_yoy` 0.110~14.533, `own_insider_filing_activity` 0.628~3.143, `own_major_stake_level` 0.935~1.430. 유지율은 분모의 절대 크기와 같이 읽는다.

### 2.7 업종 중립화가 없다 — 35개 전부의 한계

횡단 정규화 그룹이 KOSPI·KOSDAQ 둘뿐이다. 특히 취약한 축: `fin_gross_profitability`(소프트웨어와 유통이 한 풀), `hc_productivity`(1인당 매출은 업종이 지배), `fin_value_z`(은행 B/M과 소프트웨어 B/M을 같이 비교). 업종 중립 variant `feat_fin_scan_daily_ind`는 **현재 업종 소급이라 PIT가 아니다** — 진단 전용. N2 V6: 업종 중립화로 업종 간 중앙값 분산이 65.4% 줄었고, 중립화 전후 값의 일별 rank corr 중앙값은 0.8305다(`poc/n2_validation.md` V6).

### 2.8 커버리지가 높은 게 항상 좋은 건 아니다

| family | coverage | tradable 유지율 |
|---|---:|---:|
| `own_major_filing_activity` | 1.000 | 0.83~0.90 |
| `mcap_krx_log` | 0.999 | **0.56~0.69** |
| `fin_log_mcap` | 0.779 | 0.79~0.85 |
| `fin_value_z` | 0.564 | 1.00~1.02 |

커버리지가 높으면 거래 거의 없는 초소형주까지 값을 갖고, 유동성 필터를 걸면 그만큼 빠진다. **신호의 상당 부분이 실행 불가 종목에서 나올 수 있다.** 반대로 커버리지가 낮은 피쳐(`ev_net_share_issuance_yoy` 0.48, `own_insider_*` 0.25)는 `*_isna`가 "어떤 회사인가"의 정보가 되고, 커버리지가 시간에 따라 늘면 모델이 연도를 학습한다.

### 2.9 건수·플래그형 피쳐는 이력 초기에 횡단면이 상수다 (I13)

`own_major_filing_60d`는 2014년 288개 `(거래일, 시장)` 횡단면이 통째로 0이었다. native 엔진이 그런 날에 NaN 대신 가짜 상관을 냈고(2026-08-30 수정, 판정 변화 0), 같은 성질이 `ev_filing_activity`·`own_insider_filing_activity`·`own_amendment_ratio`에 있다. 모델에서는 (i) 이력 시작 구간을 `*_isna`가 아니라 "관측 0"으로 잘못 학습하지 않도록 유효 시작일 이전을 NULL로 두고, (ii) rank 변환 시 동점 처리를 명시한다.

### 2.10 PIT 규칙이 마트마다 다르다 — 모델에 섞어 넣기 전에 통일

| 마트 | PIT 규칙 | 쓰는 곳 |
|---|---|---|
| `feat_price`, `feat_macro_exposure` | 당일 종가 = 관측 시점 (`native_t`; 국내 매크로 요인은 한 세션 지연) | 검증·모델 둘 다 |
| `feat_flow` | 세션 t의 KRX 투자자별 집계. **당일 사용 가능 여부가 검증되지 않아 scan 정본은 `lag1`(직전 유효 세션 값)**이다(`flow_unverified_same_day_variant: lag1`). 당일 값 대비 손실은 미측정 | 검증은 `lag1`, baseline 모델은 당일 값 — **불일치** |
| `feat_fin_pit` (baseline `fin_*` 10) | `stock_metric_fact` + **보수적 지연 period_end+90일(연)/+45일(분기)** | 모델 baseline만 |
| `feat_fin_scan_daily` (검증 `fin_*` 5) | **strict PIT vintage** (`DEFAULT_VINTAGE_POLICY`, 접수일 기준 `available_from`) | 검증만 |
| `feat_event_scan_daily` | strict PIT capital-change vintage | 검증 |
| `feat_filing_activity` | 접수일 **다음 거래일** 노출 (위반 0 확인) | 검증·T2 |
| `feat_periodic_extras` | `*_available_from` (DS002 접수 기준) | 검증·T2 |
| `feat_event` (`ev_treasury_*`) | bsns_year 말 +90일 | 모델(꺼짐) |
| `common_feature_daily_fact` | `asof_available_date` (국내 next session / 해외 same morning / 월간 +20일) | 검증·국면 |
| `daily_market_cap` 원천 | KRX Open API **T+1** | `mcap_krx_log` |

같은 `fin_` 접두어로 두 규칙이 공존한다. 모델에 `fin_pit` 10 + `fin_scan` 5를 같이 넣으면 같은 회계 정보가 두 시점에 두 번 들어간다. **하나로 맞추는 것이 첫 작업이다** — strict PIT 쪽이 검증 결과와 맞는다.

### 2.11 정의가 바뀐 이력이 있다

- `FIN_FEATURE_FORMULA_VERSION` `fin_v3 → fin_v4`(I7 XBRL fallback, 2026-08-18): S/P 매핑 5%→79%, `fin_gross_profitability` coverage 0.03→0.81. 그 전 run의 재무 결과는 지금 정의와 다르다.
- `FILING_ACTIVITY_FORMULA_VERSION` `filing_v1 → v3`: 08-29 parity에서 `own_major_filing_activity` 16/288행 차이의 원인으로 귀속됐다가, I13 발견으로 "불완전한 귀속"이 됐다(`10_known_issues` §9.9).
- flow 원천이 2026-08부터 KRX MDC → KIS(6지표)로 바뀌었다. 병행 대조(`docs/operations.md` "KRX ↔ KIS 병행 대조"): 순매수 3종 불일치 0건, **공매도 거래량·거래대금은 KIS가 총량 −1.19% 낮은 정의 차이**, `foreign_holding_shares`는 477/2,763(17.3%) 양방향 불일치(대형주 1% 안, 소형주 최대 5배). 학습 표본(~2025-02)은 전부 KRX 값이고, 교체일 이후는 정의가 다른 값이 이어진다 — 교체일에 계단이 생긴다.
- `common_feature_daily_fact` 격자가 2014~2023년 평일(비세션 포함)이다(`06` §3.2).

### 2.12 `fin_sue`는 표본이 없다

8분기 EPS 이력 + 접수일 기준이라 유효 시작이 2025-05-02, coverage 0.00. 6 cell 전부 `insufficient`. 실적 서프라이즈 축은 **아직 측정되지 않았다**(나쁜 게 아니다). receipt-targeted XBRL 백필 대상을 골라야 한다.

---

## 3. 모델 개발에서의 공백과 우선순위

`00_읽는_법.md` §10과 `11_feature_taxonomy.md` §10을 이 모델 목표에 맞춰 다시 순서 매겼다.

| 순위 | 작업 | 왜 이 모델에 필요한가 | 비용 | 새 수집 |
|---|---|---|---|---|
| 1 | **PIT 규칙 통일** (`fin_pit` vs `fin_scan`) | 같은 정보가 두 시점에 두 번 들어가는 것을 막는다 (§2.10) | 코드 | 없음 |
| 2 | **A×A·B×B 일별 IC 상관** (`daily_ic.parquet` 이용) | 41개 중 몇 개가 독립 정보인지 알아야 피쳐 수를 정한다 (§2.2) | 분석 | 없음 |
| 3 | **분위별 평균수익률·중앙값 spread** | IC–spread 어긋남을 풀고, 상승 확률 라벨의 양 끝 행동을 안다 (§2.1) | 분석 | 없음 |
| 4 | **규모 통제 증분 IC** | 규모 축 3개의 중복을 정리 (§2.3) | 분석 | 없음 |
| 5 | **명시적 interaction 4개 + 증분성 gate** | 거시를 "한 번에" 넣는 검증된 길 (`06` §5) | 코드·gate | 없음 |
| 6 | 반대 부호 안정 3 family 재등록 | D등급에 묻힌 정보 회수 (§2.5) | 새 config | 없음 |
| 7 | **PIT 업종 코드 수집 → 업종 중립·업종 더미** | "다른 종목과의 조합"의 첫 블록. Barra·GKX 표준의 industry 블록이 통째로 없다 | **수집·가장 오래 걸림** | **있음** (`induty_code` 버저닝, K-6e) |
| 8 | 관계 피쳐 (peer 상대 위치, 업종 내 순위, 리드래그) | 7이 선행 | 코드 | 7 뒤 |
| 9 | 상태 전이·Δ 피쳐 (적자→흑자, 무배당→배당), 레버리지, 생애주기 | canonical 29 metric에 원천이 이미 있다. 축 T3·C2 공백 | 코드·새 config | 없음 |
| 10 | `fin_sue` XBRL 백필 대상 선정 | 서프라이즈 축 첫 측정 (§2.12) | 백필 | OpenDART |
| 11 | `holidays_krx.csv` 2014~2023 보강 | fact 격자 문제 원천 해결. golden·재현값이 바뀌므로 별도 과제 | 데이터 | 없음 |
| 12 | 매크로 단계 2·3 (회사채 스프레드·기준금리·ESI, 수출 잠정치, SOX) | 국면 변수 확장 | 새 어댑터 | 있음 |

1~6은 새 수집 없이 지금 코드와 산출물로 할 수 있다. 7이 사용자가 말한 "다른 종목과의 조합"의 진짜 병목이다.

---

## 4. horizon별 피쳐 배치 제안 — 출발점

사전등록 primary horizon과 관측 신호 밴드(onset·peak·half-life)를 근거로 했다. **결과를 보고 옮기지 않는다**는 규율의 대상은 검증이고, 모델 실험은 새 사전등록이므로 이 표를 실험 계획에 고정하고 시작한다.

| h | 넣을 피쳐 (근거) | 넣지 않을 것 | 주 지표 |
|---|---|---|---|
| **5** (단기) | `px_reversal_5d`(밴드 1~10, peak 3, lag1 0.686) · `px_turnover_shock`(부호 뒤집어, 밴드 5~20; ×`liq_high`) · `flow_individual_netbuy_to_volume_5d/_20d` · `flow_inst_netbuy_to_volume_20d`(부호 뒤집어) · `ev_filing_burst_60d`(exploratory 1·5·10에 값 있음) · baseline px/flow 30 | 재무·지분·매크로 베타(사전등록 20 이상), `flow_foreign_*`(지연 게이트 실패) | **비용 반영 spread, turnover.** IC 개선만으로 채택 금지(T1 교훈). 수급 피쳐는 검증 정본이 `lag1`이므로 모델에서도 하루 지연값으로 시작한다 |
| **20** (중기, 기존 배포 대상) | `px_idio_vol_60d` · `px_maxret_20d` · `px_near_52w_high`(spread 음수 주의) · `flow_individual_netbuy_to_volume_20d` · `ev_filing_burst_60d` · `own_major_filing_60d` · `macro_beta_vix/wti/sp500_lag` · `px_market_beta`(×`market_up`) · `flow_foreign_netbuy_to_volume_20d`×`liq_high` · baseline 40 | `fin_*` 대부분(사전등록 60·120), 공매도 4개(표본 단절) | Rank IC + 비용 반영 spread + k=100 |
| **60** (중장기) | `fin_log_mcap` 또는 `mcap_krx_log` 중 하나 · `px_amihud_20d`(규모 통제 뒤) · `fin_value_z`·`fin_gross_profitability`(placebo 미통과 이력 유의) · `ev_amendment_ratio_1y`(또는 `own_`) · `ev_payout_yield` · `ev_net_share_issuance_yoy`(spread 부호 반대) · `own_major_stake_chg` · `hc_employee_growth_yoy` · 매크로 베타 4 · baseline 40 | 반전·회전율 충격(half-life 5~10) | Rank IC, 기간 일관성, **holdout은 2026-10~11** |
| **120** (장기) | `px_amihud_20d` · `fin_log_mcap` · `ev_net_share_issuance_yoy` · `fin_value_z`(placebo 실패) · `fin_asset_growth_yoy`는 신호 없음 | 일별 롤링 단기 피쳐 전부 | 기간 일관성, `available` 부호. 표본 22개 독립 창 |

- **모든 h 공통**: 규모 축(`px_amihud_20d`·`fin_log_mcap`·`mcap_krx_log`)은 하나만, 또는 규모 통제 뒤. 중복 쌍(`ev_`·`own_amendment`, `own_major_stake`·`_chg`)은 하나만.
- **국면 변수** 4개(이진)를 모든 h에 보조 입력으로 넣되, 검증된 interaction 4개는 명시적 컬럼으로 따로 둔다.
- **`cf_*` 직접 투입**은 라벨이 절대 상승일 때만 검토한다(`07` §1.2).

### 4.1 조합 후보 — 검증됐거나 사전 설계가 요구한 것

| 조합 | 근거 | 상태 |
|---|---|---|
| `px_reversal_5d × vix_high` | Phase C P3 통과 (δ̂ +0.0205) | **검증됨** |
| `px_turnover_shock × liq_high` | P9 (+0.0269) | **검증됨** |
| `flow_foreign_netbuy_to_volume × liq_high` | P12 (+0.0163) | **검증됨** |
| `px_market_beta × market_up` | P15 (−0.0579) | **검증됨** |
| `px_reversal_5d × {px_turnover, px_amihud_20d, px_maxret_20d}` | `02_feature_candidate` P2 사전 설계가 요구 | 미검정 |
| `px_maxret_20d` vs `px_idio_vol_60d` ablation | 사전 설계 요구, 신호 모양 동일 | 미검정 |
| `fin_log_mcap × cf_small_growth_regime`, 레버리지 × 금리 변화 | `00_survey/00` §4.4 | 미검정 |
| `flow_foreign_* × (usdkrw, ΔVIX)` | 문헌·수준 상관 근거 | P11(`vix_up`) 실패, 환율은 미검정 |
| 업종 더미 × 밸류·수익성 | 표준 모델 구조 | 업종 코드 없음 |

---

## 5. 규율 재확인

1. holdout(2025-08-01~)을 열지 않는다. 2026-10~11 한 번.
2. 등급 ≠ 채택. 묶음 단위 증분성·비용 반영 지표로 판정한다.
3. 새 피쳐·interaction·라벨은 새 config로 사전등록한다. 기존 `config_hash` YAML을 고치지 않는다.
4. 숫자를 다른 run과 섞지 않는다. 이 묶음은 `00` §4의 run만 인용한다.
5. PIT를 깨는 지름길(현재 업종 소급, 최신 vintage, T+1 원천의 당일 사용)을 모델 입력에 쓰지 않는다.
