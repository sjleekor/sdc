# 01. 피쳐 인벤토리 — 마트별 전체 컬럼과 상태

- 작성일: 2026-09-03
- 범위: `research/etl/features/*.py`가 만드는 feature 마트 12개의 컬럼 전부. 라벨과 국면 변수는 따로 적었다.
- 등급은 **결합 AB 기준**(`00_읽는_법.md` §8). cell 단위 등급이 갈리면 `A2/C2`처럼 적었다.
  매크로 6 family는 `236d0d35…` 계보의 AB 결과다.
- `모델 사용` 열: `baseline 40` = 현재 acceptance gate baseline(`feature_groups=("px","flow","fin")` → `feat_price` 15 + `feat_flow` 15 + `feat_fin_pit` 10).
  `T1` = Grade A 후보 5개 묶음(비채택). `T2` = Phase B 후보 14개 묶음(validation 개선, holdout 대기). `—` = 어느 모델에도 아직 안 들어갔다.

---

## 0. 집계

| 구분 | 수 |
|---|---|
| feature 마트 | 12 (`feat_price`, `feat_flow`, `feat_fin_scan_daily`, `feat_fin_scan_daily_ind`, `feat_fin_pit`, `fin_sue_event`, `feat_market_cap`, `feat_periodic_extras`, `feat_event_scan_daily`, `feat_filing_activity`, `feat_event`, `feat_macro_exposure`) + 공통 `feat_common` |
| Horizon Scan family | **41** (Phase A 17 + Phase B 18 + 매크로 6) |
| family 대표 등급 (41, cell이 갈리면 상위 등급으로) | **A 13 · B 11 · C 10 · D 6 · R 1** (cell 분포는 §1~§6 표의 표기) |
| Horizon Scan primary 피쳐 컬럼 | 41 |
| secondary 피쳐 컬럼 | 21 |
| baseline 모델 컬럼 (단변량 검정 없음) | 39 (`px_amihud_20d`는 양쪽에 다 있다) |
| 그 밖의 보조·진단 컬럼 | `feat_event` 3, `fin_value_z` 구성요소 4, `mcap_krx`·`mcap_unreliable`, `sue_history_count`, `*_available_from` 4, `macro_*_n`, `flow_*` 잔여 4 |
| `*_lag1` 변형 | primary·secondary 대부분에 하나씩 (게이트용, 모델 입력 아님) |
| 공통(거시·시장) feature_code | 55 (active 38) → `feat_common` 기본 12 |
| Phase C 국면 변수 | 7 (primary 4, exploratory 3) |
| 라벨 | `y_rank/y_reg/y_cls_{5,20,60}d`, `label_scan` 누적 9 + bucket 6, 리스크 `y_vol/y_mdd`(꺼짐) |

---

## 1. `feat_price` — 가격·유동성 (원천 `daily_ohlcv`) → [02](02_price_liquidity.md)

| 컬럼 | family | 역할 | 등급(AB) | screen_pass | 모델 사용 | 메모 |
|---|---|---|---|---|---|---|
| `px_reversal_5d` | `px_reversal_5d` | primary | **A** 7/7 | 통과 | T1 | q 최소. 하루 늦으면 31% 손실 |
| `px_mom_12_1` | `px_mom_12_1` | primary | **D** | — | — | 부호 반대, 그 반대도 2/5 불안정 |
| `px_mom_6_1` | `px_mom_12_1` | secondary | — | — | — | 스캔 안 됨 |
| `px_resid_mom_12_1` | `px_resid_mom_12_1` | primary | **D** | — | — | BH 통과 cell 0 |
| `px_near_52w_high` | `px_near_52w_high` | primary | **A** 3/6 | 통과 | baseline 40 (`px_dist_52w_high`로) | IC 양수인데 q5 spread 음수. T1 후보에서 제외된 이유가 이것 |
| `px_maxret_20d` | `px_maxret_20d` | primary | **A** 6/6 | 통과 | T1 | 시간 placebo 통과 |
| `px_idio_vol_60d` | `px_idio_vol_60d` | primary | **A** 6/6 | 통과 | T1 | \|IC\| 최대 0.143 |
| `px_amihud_20d` | `px_amihud_20d` | primary | **A** 4/4 | 통과 | **baseline 40** | q5 spread 최대 +11.2%p. 규모와 ρ −0.72 |
| `px_turnover_shock` | `px_turnover_shock` | primary | **D** | — | — | **반대 부호 5/5 일관.** Phase C `liq_high` 통과 |
| `px_zero_ret_ratio_20d` | `px_zero_ret_ratio_20d` | primary | **R** | — | — | 기준용, 검정 안 함 |
| `px_ret_1d`, `px_ret_5d`, `px_ret_20d`, `px_ret_60d` | — | baseline | — | — | baseline 40 | 로그수익률 |
| `px_mom_20_60` | — | baseline | — | — | baseline 40 | `ret_20d − ret_60d` |
| `px_vol_20d`, `px_vol_60d` | — | baseline | — | — | baseline 40 | 실현변동성 |
| `px_high_low_range_20d` | — | baseline | — | — | baseline 40 | 20일 고저폭/종가 |
| `px_turnover`, `px_turnover_ma20` | — | baseline | — | — | baseline 40 | 거래대금·20일 평균 |
| `px_gap_vs_ma20` | — | baseline | — | — | baseline 40 | 이격도 |
| `px_dist_52w_high` | — | baseline | — | — | baseline 40 | `px_near_52w_high`와 **같은 산식**(`close / MAX(close) OVER 252 − 1`). 기업행동 마스킹만 없다 |
| `px_is_halted`, `px_halt_ratio_20d` | — | baseline | — | — | baseline 40 | 거래정지 플래그·비율 |
| `px_*_lag1` (9개) | 각 family | lag1 variant | — | — | — | 지연 게이트용 |

## 2. `feat_flow` — 투자자 수급·공매도 (원천 `krx_security_flow_raw`, 2026-08부터 KIS 6지표) → [03](03_investor_flow_short.md)

| 컬럼 | family | 역할 | 등급(AB) | screen_pass | 모델 사용 | 메모 |
|---|---|---|---|---|---|---|
| `flow_foreign_netbuy_to_volume_20d` | `flow_foreign_netbuy_to_volume` | primary | **D** | — | — | 반대 부호 안정. 지연 게이트 실패. Phase C `liq_high` 통과 |
| `flow_foreign_netbuy_to_volume_5d`, `_60d` | 같음 | secondary | — | — | — | |
| `flow_inst_netbuy_to_volume_20d` | `flow_inst_netbuy_to_volume` | primary | **D** | — | — | 반대 방향이 가장 강하고 깨끗(q 1.3e-13) |
| `flow_inst_netbuy_to_volume_5d`, `_60d` | 같음 | secondary | — | — | — | |
| `flow_individual_netbuy_to_volume_20d` | `flow_individual_netbuy_to_volume` | primary | **A** 6/6 | 통과 | T1 (`_5d`·`_20d`) | 부호 미고정, 관측 `+` |
| `flow_individual_netbuy_to_volume_5d` | 같음 | secondary | — | — | T1 | |
| `flow_individual_netbuy_to_volume_60d` | — | 보조 | — | — | — | 스캔 밖 |
| `flow_foreign_holding_ratio_chg_20d` | `flow_foreign_holding_ratio_chg` | primary | **D** | — | — | 수급 4개 중 가장 약함 |
| `flow_foreign_holding_ratio`, `_chg_5d`, `_chg_60d` | — | 보조 | — | — | — | |
| `flow_short_turnover_20d` | `flow_short_turnover` | primary | **C**(보류) | — | — | 표본 2016-07~2020-03 |
| `flow_short_interest_ratio` | `flow_short_interest` | primary | **C**(보류) | — | — | 공매도 4개 중 \|IC\| 최대 |
| `flow_short_interest_ratio_chg_20d` | 같음 | secondary | — | — | — | |
| `flow_days_to_cover` | `flow_days_to_cover` | primary | **C**(보류) | — | — | `flow_short_interest`와 분자 같음 |
| `flow_nat_proxy_20d` | `flow_nat_proxy_20d` | primary | **C**(보류) | — | — | `flow_short_interest`를 재료로 포함 |
| `flow_foreign_netbuy_sum_5d`, `_20d` | — | baseline | — | — | baseline 40 | 순매수량 누적 |
| `flow_inst_netbuy_sum_5d`, `_20d` | — | baseline | — | — | baseline 40 | |
| `flow_indiv_netbuy_sum_5d`, `_20d` | — | baseline | — | — | baseline 40 | |
| `flow_foreign_holding_chg_5d`, `_20d` | — | baseline | — | — | baseline 40 | 보유주식수 차분 |
| `flow_short_balance_chg_20d` | — | baseline | — | — | baseline 40 | 2016-06-30 이전 NULL |
| `flow_foreign_netbuy_z_20d`, `flow_inst_netbuy_z_20d` | — | baseline | — | — | baseline 40 | 20일 z-score |
| `flow_short_avg_price` | — | baseline | — | — | baseline 40 | `short_value / short_volume` |
| `flow_short_selling_volume`, `flow_short_selling_value`, `flow_short_balance_qty` | — | baseline | — | — | baseline 40 | 패널 passthrough 레벨 |
| `flow_*_lag1` (8개) | 각 family | lag1 variant | — | — | — | **수급은 이것이 scan 정본이다**(`flow_unverified_same_day_variant: lag1`). 당일 집계 사용 가능 여부 미검증 |

## 3. 재무·규모·밸류·인적자본 → [04](04_financial_size_value.md)

### 3.1 `feat_fin_scan_daily` (strict PIT vintage, `fin_v4`)

| 컬럼 | family | 역할 | 등급(AB) | screen_pass | 모델 사용 | 메모 |
|---|---|---|---|---|---|---|
| `fin_log_mcap` | `fin_log_mcap` | primary | **A** 4/4 | 통과 | T2 | Phase B 최고. 긴 cell 3개가 시간 placebo 실제 통과 |
| `fin_value_z` | `fin_value_z` | primary | **B1/C3** | 1 cell | T2 | Phase B \|IC\| 최대 0.122. 긴 cell 전부 placebo 실패 |
| `fin_book_to_market`, `fin_earnings_yield`, `fin_cfo_yield`, `fin_sales_to_price` | `fin_value_z` | 구성요소 | — | — | — | 스캔 안 됨 |
| `fin_gross_profitability` | `fin_gross_profitability` | primary | **B5/C3** | 5 cell | T2 | 매출총이익 94.4%가 역산값 |
| `fin_operating_profitability` | 같음 | secondary | — | — | — | |
| `fin_asset_growth_yoy` | `fin_asset_growth_yoy` | primary | **C4** | — | — | 신호 없음(\|IC\| 0.004) |
| `fin_accruals_to_assets` | `fin_accruals_to_assets` | primary | **C3/D1** | — | — | 부호 반대, 불안정 |
| `fin_*_lag1` (5개) | 각 family | lag1 | — | — | — | |

### 3.2 `feat_fin_scan_daily_ind` — 업종 중립 variant (진단 전용)

같은 컬럼을 업종 그룹(`definitions/industry_groups.py`, KSIC 2자리 prefix, 최소 20 → 섹션 → `OTHER`) 안에서 정규화한 것. **현재 업종 코드를 과거에 소급한 값이라 PIT가 아니다.** scored backtest·acceptance gate·holdout 사용 금지. 모델 입력 불가.

### 3.3 `feat_fin_pit` — baseline 모델 재무 10 (원천 `stock_metric_fact`, 보수적 지연 +90/+45일)

| 컬럼 | 뜻 | 모델 사용 |
|---|---|---|
| `fin_roa`, `fin_roe`, `fin_operating_margin` | 수익성 비율 | baseline 40 |
| `fin_debt_to_equity`, `fin_equity_ratio` | 레버리지·자본구조 | baseline 40 |
| `fin_ocf_to_assets`, `fin_cash_ratio`, `fin_asset_turnover` | 현금흐름·효율 | baseline 40 |
| `fin_is_negative_equity` | 자본잠식 플래그(비율 클리핑 동반) | baseline 40 |
| `fin_has_fs` | 재무제표 존재 플래그 | baseline 40 |

Horizon Scan 검정은 없다. **PIT 규칙이 §3.1과 다르다**(`08` §2.10).

### 3.4 그 밖의 재무·규모 마트

| 마트 | 컬럼 | family | 등급(AB) | screen_pass | 모델 사용 | 메모 |
|---|---|---|---|---|---|---|
| `fin_sue_event` | `fin_sue` | `fin_sue` | **C**(insufficient) | — | — | 표본 0. 8분기 EPS 이력 + 접수일 → 2025-05부터 |
| `fin_sue_event` | `sue_history_count` | — | — | — | — | 진단 |
| `feat_market_cap` | `mcap_krx_log` | `mcap_krx_log` | **A2/C2** | 2 cell | T2 | `fin_log_mcap`과 같은 개념·다른 원천(KRX `daily_market_cap`). 커버리지 0.999 |
| `feat_market_cap` | `mcap_krx`, `mcap_unreliable` | — | — | — | — | 원값, 무상증자 권리락 창 마스크 |
| `feat_periodic_extras` | `hc_employee_growth_yoy` | `hc_employee_growth` | **B1/C3** | 1 cell | T2 | q 0.093으로 겨우 통과 |
| `feat_periodic_extras` | `hc_revenue_per_employee` | `hc_productivity` | **B1/C3** | 1 cell | T2 | 네 cell 전부 IC와 spread 부호 반대 |
| `feat_periodic_extras` | `own_major_stake` | `own_major_stake_level` | **B2/C2** | 2 cell | T2 | 변화(`_chg`)가 모든 지표에서 낫다 |
| `feat_periodic_extras` | `own_major_stake_chg` | `own_major_stake_change` | **B** 4/4 | 통과 | T2 | 게이트 전부 통과, 상한 때문에 B |
| `feat_periodic_extras` | `*_available_from` (4) | — | — | — | — | PIT 시작일 |

## 4. 공시·자본정책 이벤트·지분 → [05](05_filing_event_ownership.md)

### 4.1 `feat_event_scan_daily` (strict PIT capital-change vintage)

| 컬럼 | family | 등급(AB) | screen_pass | 모델 사용 | 메모 |
|---|---|---|---|---|---|
| `ev_net_share_issuance_yoy` | `ev_net_share_issuance_yoy` | **A1/C3** | 1 cell | T2 | **IC −0.038 대 spread +11.3%p, 정반대** |
| `ev_payout_yield` | `ev_payout_yield` | **B3/C1** | 3 cell | T2 | \|IC\| 0.10인데 spread 0.49%p |

### 4.2 `feat_filing_activity` (원천 `dart_filing_receipt_raw`, 접수일 다음 거래일 노출, `filing_v3`)

| 컬럼 | family | 역할 | 등급(AB) | screen_pass | 모델 사용 | 메모 |
|---|---|---|---|---|---|---|
| `ev_amendment_ratio_1y` | `ev_amendment_ratio` | primary | **A** 4/4 | 통과 | T2 | Phase B 최고 수준 |
| `ev_filing_burst_60d` | `ev_filing_activity` | primary | **A** 4/4 | 통과 | T2 | 양방향, 크기는 amendment의 1/4 |
| `ev_filing_count_60d`, `ev_filing_count_120d`, `ev_filing_burst_120d` | 같음 | secondary | — | — | — | |
| `own_amendment_ratio_1y` | `own_amendment_ratio` | primary | **A** 4/4 | 통과 | T2 | q5 spread 계산 불가(동점). `ev_amendment_ratio_1y`의 부분집합 |
| `own_insider_filing_burst_60d` | `own_insider_filing_activity` | primary | **C2/D2** | — | — | 신호 없음, 커버리지 0.25 |
| `own_insider_filing_60d`, `_120d`, `own_insider_filing_burst_120d` | 같음 | secondary | — | — | — | |
| `own_major_filing_60d` | `own_major_filing_activity` | primary | **A** 4/4 | 통과 | T2 | 커버리지 1.00. 2014년 상수 횡단면(I13) |
| `own_major_filing_120d` | 같음 | secondary | — | — | — | |
| `own_filings_*`, `own_amendments_*` | — | 중간 집계 | — | — | — | |
| `*_lag1` | 각 family | lag1 | — | — | — | |

### 4.3 `feat_event` — 모델용 기업행위 플래그 (원천 `dart_share_count_raw`, bsns_year 말 +90일)

| 컬럼 | 뜻 | 모델 사용 | 검증 |
|---|---|---|---|
| `ev_treasury_ratio` | 자사주 / 발행주식 | — (spec `ev` 그룹 꺼짐) | 없음 |
| `ev_has_treasury` | 자사주 보유 플래그 | — | 없음 |
| `ev_shares_chg_yoy` | 발행주식수 YoY | — | 없음. `ev_net_share_issuance_yoy`와 개념 겹침 |

## 5. `feat_macro_exposure` — 매크로 exposure 베타 → [06](06_macro_regime.md)

| 컬럼 | family | 역할 | 등급(AB, `236d0d35`) | screen_pass | 모델 사용 | 메모 |
|---|---|---|---|---|---|---|
| `macro_beta_vix` | `macro_beta_vix` | primary | **B** 4/4 | 4 cell | — | 기대 부호 `−` 사전등록, 맞았다 |
| `macro_beta_wti` | `macro_beta_wti` | primary | **B3/C1** | 3 cell | — | 관측 부호 `+` 고정 |
| `macro_beta_sp500_lag` | `macro_beta_sp500_lag` | primary | **B3/C1** | 3 cell | — | 관측 `+`, 넷 중 가장 약함 |
| `px_market_beta` | `px_market_beta` | primary | **B3/C1** | 3 cell | — | 관측 `−`(BAB). Phase C `market_up` 통과 |
| `macro_beta_usdkrw` | `macro_beta_usdkrw` | primary | **C/D** | — | — | IC ≈ 0 |
| `macro_beta_kr10y` | `macro_beta_kr10y` | primary | **C/D** | — | — | 효과 없음 |
| `macro_rawbeta_{usdkrw,wti,kr10y,sp500_lag,vix}` | 각 family | secondary(진단) | — | — | — | 원수익률 베타. vix·sp500은 부호가 뒤집힘 |
| `macro_semibeta_usdkrw_up`, `macro_semibeta_n_usdkrw_up` | `macro_beta_usdkrw` | secondary | — | — | — | 원화 약세일 세미베타 |
| `macro_beta_n_*`, `px_market_beta_n` | — | 진단 | — | — | — | REGR_COUNT |
| `*_lag1` | 각 family | lag1 | — | — | — | |

## 6. `feat_common` / `common_feature_daily_fact` — 시장·거시 공통 (날짜 broadcast) → [06](06_macro_regime.md) §3

- `common_feature_daily_fact`: feature_code 55개(active 38). 카테고리 `market_index`·`market_breadth`·`market_liquidity`·`global_index`·`global_risk`·`fx`·`commodity`·`rate`·`macro_price`·`macro_money`·`macro_employment`·`macro_sentiment`·`industry_index`(inactive).
- `feat_common` 기본 12개(`cf_` 접두어): `market_kospi_ret_5d`·`ret_20d`, `market_kosdaq_ret_1d`, `market_kospi200_ret_1d`, `rate_kr_gov3y_level`, `rate_kr_gov10y_level`, `rate_kr_term_spread_10y_3y`, `rate_us_term_spread_10y_2y`, `fx_usdkrw_ret_5d`, `global_vix_level`, `global_sp500_ret_1d`, `commodity_wti_ret_20d`.
- 모델 사용: **없음**(spec `cf` 그룹 꺼짐). Horizon Scan 검정: 구조적으로 불가(날짜 상수). 국면 변수 7개의 원천.

## 7. Phase C 국면 변수 (피쳐가 아니라 조건 변수) → [06](06_macro_regime.md) §4

`vix_up`, `vix_high`, `market_up`, `liq_high`(primary) / `term_steep`, `kosdaq_rel_up`, `krw_weak_20d`(exploratory). `research/analysis/horizon_scan_phase_c_regimes.py`가 KRX 세션 격자에서 만든다.

## 8. 라벨 → [07](07_labels_sample_validation.md)

| 테이블 | 컬럼 | 뜻 |
|---|---|---|
| `label_daily` | `raw_label_{h}d` | h거래일 뒤까지 초과수익률(시장 동일가중 대비). 기본 h = 20·5·60 |
| | `y_rank_{h}d` | `(거래일, 시장)` 내 백분위 순위 [0,1] — 기존 모델의 주 타깃 |
| | `y_reg_{h}d` | winsor(0.5%, 99.5%) 회귀 라벨 |
| | `y_cls_{h}d` | 3-class {−1,0,1}, 순위 0.2/0.8 임계 |
| | `y_vol_{h}d`, `y_mdd_{h}d` | 실현변동성·최대낙폭 (`include_risk=True`일 때만) |
| `label_scan` | `raw_label_{h}d`, `y_rank_{h}d` (h ∈ 1,2,3,5,10,20,40,60,120) | Horizon Scan 격자 |
| | `raw_bucket_label_{a}_{b}d` | (0,5],(5,10],(10,20],(20,40],(40,60],(60,120] 구간 초과수익률 |

---

## 9. 이 표를 읽을 때

- **`baseline 40`은 단변량 검정을 거치지 않았다.** walk-forward Rank IC(h20 0.1436)는 40개 묶음의 결과이고 개별 기여도는 없다.
- **T2 14개 안에 확인된 중복 쌍이 셋 있다.** `fin_log_mcap`·`mcap_krx_log`(같은 개념), `ev_amendment_ratio_1y`·`own_amendment_ratio_1y`(전체 대 부분집합), `own_major_stake`·`own_major_stake_chg`(수준 대 차분).
- **등급 D여도 버릴 정보가 아닌 것이 있다.** `px_turnover_shock`, `flow_foreign_netbuy_to_volume_20d`, `flow_inst_netbuy_to_volume_20d`는 기대와 반대 부호가 5/5 구간 일관됐다. 모델에는 부호만 뒤집어 넣을 수 있다. 다만 `flow_foreign_*`는 지연 게이트에 실패했다.
- `*_lag1`은 모델 입력용이 아니라 "하루 늦게 써도 남는가"를 재는 게이트용이다. 모델에서 실행 지연을 흡수하려면 피쳐 계산 시점을 옮기는 것이 맞다.
