# 06. 매크로·국면 — exposure 베타, 공통 피쳐 카탈로그, 국면 변수와 Phase C

- 작성일: 2026-09-03
- 대상: `feat_macro_exposure` 6 family, `common_feature_daily_fact`/`feat_common` 55 feature_code, Phase C 국면 변수 7개와 검정 쌍 15+2
- 기준 run: A `20260830T085718-efd35e70` → B `20260830T100518-efd35e70` → AB `20260830T122850-efd35e70` → C `20260830T122850-phasec`
  (config `horizon_scan_macro_20260829.yaml`, hash `236d0d35…`, snapshot `2026-08-23`)
- 원문: `docs/dev/20260829_macro_features/` (조사 `00_survey/`, 설계·결과 `01_design/`)

---

## 1. 매크로가 이 검증 틀에 들어오는 세 가지 길

라벨 `raw_label_{h}d`는 같은 `(거래일, 시장)` 안의 동일가중 평균수익률을 뺀 값이고, IC는 그 안의 순위 상관이다. **그날 모든 종목에 같은 값인 변수는 횡단면 분산이 0이라 상관이 정의되지 않는다.** VIX level이든 CPI YoY든 "VIX > 25" 더미든 마찬가지다. 매크로가 시장 전체를 올리거나 내리는 효과는 라벨 정의에서 이미 빠져 있다. 이 프로젝트가 예측하려는 것은 "어느 종목이 시장보다 나은가"이기 때문이다(`00_survey/00` §3).

| 길 | 형태 | 횡단면에서 변하나 | 이번에 한 것 | 모델에서 |
|---|---|---|---|---|
| ① exposure | `rolling_beta(resid_ret_i, Δmacro)` — 종목별 민감도 | 변한다 | **단계 1a**: 6 family 사전등록·스캔 완료 (§2) | 보통 피쳐로 투입 |
| ② interaction | `국면_t × 특성_i` | 변한다 | Horizon Scan family로는 등록 안 함 — rank IC 아래에서는 ③과 같은 검정임을 증명 (`01_design/00` §1.1) | **명시적 interaction 컬럼**으로 만들어 투입 (§5) |
| ③ 조건부 IC | 국면별로 IC를 나눠 δ 검정 | — | **단계 1b(Phase C)**: 15쌍 중 4쌍 통과 (§4) | ②의 근거 |

pooled 모델(트리)은 날짜를 섞어 학습하므로 `cf_*` 컬럼(날짜 상수)을 넣으면 분기 조건으로 쓰여 **암묵적 interaction**이 된다. 어떤 interaction이 학습됐는지 사후 해석이 어려우므로, 검증된 쌍을 ②로 명시적으로 만드는 것이 먼저다.

interaction을 rank IC로 스캔하면 안 되는 이유는 수식 하나다. 날짜 상수 `s_t`와 특성 `x`의 곱을 날짜 안에서 순위 매기면 `IC_t(s·x) = sign(s_t)·IC_t(x)`이고, `s∈{0,1}`이면 `s=0`인 날이 통째로 탈락해 한쪽 조건부 평균만 남고, `s∈{−1,+1}`이면 국면 점유율이 불균형할 때 `δ=0`이어도 0이 아니다. 연속 국면 가중은 rank IC가 크기를 버려 표현 자체가 안 된다.

---

## 2. `feat_macro_exposure` — 매크로 exposure 베타 6 family

### 2.1 산식과 규칙 (6 family 공통)

파일 `research/etl/features/macro_exposure.py`. grain `(trade_date, ticker, market)`, valid session만. 7,024,118행, 빌드 14.4초(snapshot 2026-08-23).

```sql
-- 창 A: 현재 세션 포함 252 valid session (해외 요인·시장 요인)
wA = PARTITION BY ticker, market ORDER BY trade_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
-- 창 B: 직전 세션까지 252 valid session (국내 요인 — 짝이 τ+1에 완성되므로)
wB = PARTITION BY ticker, market ORDER BY trade_date ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING

macro_beta_<f> = CASE WHEN REGR_COUNT(resid_ret, f) OVER w >= 126 THEN REGR_SLOPE(resid_ret, f) OVER w END
px_market_beta = CASE WHEN REGR_COUNT(log_ret, market_ret) OVER wA >= 126 THEN REGR_SLOPE(log_ret, market_ret) OVER wA END
macro_rawbeta_<f> = REGR_SLOPE(log_ret, f) (secondary, 진단)
macro_semibeta_usdkrw_up = 원화 약세일(g_usdkrw > 0)만의 REGR_SLOPE, REGR_COUNT >= 60 (secondary)
```

- **y는 원수익률이 아니라 market-model 잔차 `resid_ret`**이다(252세션 회귀, `price.py`의 `residuals` CTE 그대로). 원수익률로 재면 `Cov(r_i,f) = β_{i,m}·Cov(r_m,f) + Cov(e_i,f)`라 시장 베타에 물든다. §2.5의 진단이 이 선택이 옳았음을 보였다.
- 창 252·최소 유효 짝 126은 이 설계의 선택이다(`price.py`의 126과 다른 뜻).
- 단위는 요인마다 다르다(수익률 vs %p). rank IC는 척도 불변이라 표준화하지 않았다. **모델에서는 날짜×시장 내 rank로 넣는 것이 맞다.**
- 비양수 레벨의 로그 변화는 NULL(WTI 2020-04-20 −36.98 → 2020-04-21·22 두 세션).
- 원천 관측이 갱신되지 않은 세션(fact `asof_available_date` 같음)의 요인은 **NULL**이다. 0으로 두면 가짜 무변동일이 회귀에 들어가고 `f_sp500_lag`처럼 값을 그대로 쓰는 요인은 같은 수익률이 두 번 들어간다(3,137세션 중 115세션).

### 2.2 요인 정의와 가용 시점 — 두 부류

| 부류 | 요인 | 원천 feature_code | 짝 | `x_t`의 뜻 | 베타 갱신 |
|---|---|---|---|---|---|
| **국내 (한 세션 지연)** | `f_usdkrw` = `ln(fx_{τ+1}/fx_τ)` | `fx_usdkrw_level`(ECOS 매매기준율) | `resid_ret_τ` | 세션 τ에 고시된 환율의 전일 대비 변화 | 해외보다 **한 세션 늦다** (창 B) |
| | `f_kr10y` = `kr10y_{τ+1} − kr10y_τ` (%p) | `rate_kr_gov10y_level` | `resid_ret_τ` | 같음 | 같음 |
| **해외 (밤사이 값)** | `f_wti` = `ln(wti_t/wti_{t−1})` | `commodity_wti_spot_level`(FRED, 이번에 카탈로그 추가) | `resid_ret_t` | NY t−1 종가 변화 → KRX 세션 t. spillover | 당일 |
| | `f_sp500_lag` = `global_sp500_ret_1d` | `global_sp500_ret_1d` | `resid_ret_t` | NY t−1 일간 수익률 | 당일 |
| | `f_vix` = `ln(vix_t/vix_{t−1})` | `global_vix_level` | `resid_ret_t` | spillover | 당일 |
| 시장 | `f_mkt` = `market_ret_t` | 마트 내부(시장별 동일가중 평균) | `log_ret_t` | 동시 | 당일 |

국내 계열은 `availability_policy="next_krx_session"`이라 세션 t의 fact 값이 관측일 t−1의 값이다(예: 2024-07-02 행 = 07-01 고시 1382.4). 그래서 짝 `(resid_ret_τ, g_τ)`는 τ+1에 완성되고, 세션 t의 베타는 τ ≤ t−1까지로 만든다. look-ahead는 없다. 매매기준율이 전 영업일 은행간 거래 가중평균이라는 도메인 지식은 데이터로 확인하지 않은 한계로 남겼다.

### 2.3 예측력 — 24 cell 전체 (broad × common_survivor × native_t)

**16 cell discovery, 13 cell `screen_pass`, 등급은 전부 B**(fdr_family `macro_exposure`, 비치명 경고). A는 없다.

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

(`01_design/05_results_stage1a_20260830.md` §2.) 사전등록 primary horizon은 6 family 모두 `[20, 60]`(bucket 포함), exploratory `[1,2,3,5,10,40,120]`.

### 2.4 family별 읽기

| family | 사전등록 부호 | 관측 부호 | 해석 | 근거 문헌 | 모델 메모 |
|---|---|---|---|---|---|
| `macro_beta_vix` | **`−` 고정** | `−` (맞았다) | VIX 상승에 같이 오르는 종목의 이후 수익이 낮다. 방향을 미리 못 박고 그대로 나온 유일한 family — 증거력 최고 | Ang et al. 2006, Bali-Brown-Tang 2017 | h20·h60 모두. 4 cell 전부 통과 |
| `macro_beta_wti` | 양방향 | `+` 고정 | 유가 민감도 높은 종목의 이후 수익이 높다. 이 부호는 관측이 정한 것이지 예측이 아니다 | 섹터 비대칭만(RIBAF 2025) | cum 0→60은 screen 실패(C) |
| `px_market_beta` | 양방향 | `−` 고정 | 저베타 이례현상(BAB)의 한국판. 절대값 최대(cum 0→60 −0.049) | Frazzini-Pedersen 2014 | 변동성 축과 ρ 0.28~0.30 겹침. Phase C `market_up` 통과(§4) |
| `macro_beta_sp500_lag` | 양방향 | `+` | 전일 S&P500에 민감한 종목의 이후 수익이 높다. 넷 중 가장 약함, bucket 10→20 q 0.051 | RSZ 2013(한국 표본 없음, 첫 관측) | 여유 없음 |
| `macro_beta_usdkrw` | 양방향 | 없음 | IC ≈ 0. 환노출이 횡단면 수익을 가르지 못한다. 원수익률로 재면 +0.007~+0.012로 커지는데 그 실체는 시장 베타였다 | 고강석 2019 "환율 베타 부호 불안정"과 일관 | 투입 근거 없음 |
| `macro_beta_kr10y` | 양방향 | 없음 | t 0.75~1.40. 금리 베타는 이 표본에서 작동하지 않는다 | 한국 횡단면 검정 공백 → 효과 없음 | 투입 근거 없음 |

### 2.5 진단 — 잔차 정의가 옳았다는 증거

`core/secondary_feature_diagnostics.parquet`(discovery 좌표, cum 20·60).

| family | h | primary IC (잔차) | secondary IC (원수익률) |
|---|---:|---:|---:|
| `macro_beta_vix` | 20 | **−0.0133** | **+0.0077** |
| `macro_beta_vix` | 60 | **−0.0248** | **+0.0088** |
| `macro_beta_sp500_lag` | 20 | **+0.0125** | **−0.0075** |
| `macro_beta_sp500_lag` | 60 | **+0.0211** | **−0.0102** |
| `macro_beta_wti` | 20 / 60 | +0.0152 / +0.0238 | +0.0089 / +0.0119 |
| `macro_beta_usdkrw` | 20 / 60 | +0.0008 / +0.0003 | +0.0072 / +0.0117 |
| `macro_beta_kr10y` | 20 / 60 | +0.0034 / +0.0058 | −0.0009 / +0.0004 |

**`vix`·`sp500_lag`은 원수익률로 재면 부호가 뒤집힌다.** 고베타 종목이 VIX 상승에 더 크게 빠지므로 원수익률 VIX 베타는 시장 베타 신호를 부호만 뒤집어 물려받는다. primary를 원수익률로 정의했다면 `macro_beta_vix`의 사전등록 부호는 틀린 것으로 판정됐을 것이다. **모델에 `macro_rawbeta_*`를 넣지 않는다** — 그건 `px_market_beta`의 변장이다.

### 2.6 강건성·중복성·커버리지

- 등급 B의 이유는 fdr_family 단위 비치명 경고다. 시간 placebo는 cum 0→60·bucket 40→60만 대상(NW lag 59·19 — 후자는 대상 아님). 상세는 AB run의 `combined_ab_primary_hypotheses.parquet`.
- 상관(`primary_feature_rank_correlation.parquet`, Phase A ready family × 매크로): `|ρ| ≥ 0.5` 쌍 0개. 가장 큰 것은 `px_near_52w_high`×`px_market_beta` −0.345, `px_idio_vol_60d`×`px_market_beta` +0.295, `px_maxret_20d`×`px_market_beta` +0.282, `px_amihud_20d`×`px_market_beta` −0.244. **매크로 요인 베타 5종은 기존 어느 축과도 사실상 독립**(최대 |ρ| 0.093). 새 축이 맞다.
- 기존 Phase A discovery 변화 0개, 공통 153 가설 등급 변화 0개(`q_fdr_global_ab`는 모집단 153→177로 145행이 최대 0.0074 움직였지만 판정은 그대로).

| 컬럼 | 첫 값 | 종목 수 | 비고 |
|---|---|---:|---|
| `macro_beta_usdkrw`, `_kr10y` | 2014-12-17 | 2,976 | 국내 요인, 창 B |
| `macro_beta_wti`, `_sp500_lag`, `_vix` | 2014-12-22 | ~2,975 | |
| `px_market_beta` | 2007-12-07 | 3,137 | 2024년 중앙값 0.99 (5–95%: 0.32–1.75) |
| `macro_semibeta_usdkrw_up` | 2014-11-27 | 2,972 | |

결측 원인은 warm-up(252세션 중 유효 짝 126 미만)과 요인 NULL(해외 휴장)이다. 2014-06-17 이후 2,990세션에서 `f_vix` 2,899, `f_wti` 2,882, `f_sp500_lag` 2,897, `g_usdkrw`·`g_kr10y` 2,989개 세션에 값이 있다.

### 2.7 모델 투입 메모

- **변환**: 날짜×시장 내 rank. 단위가 요인마다 달라 원값을 그대로 섞으면 안 된다.
- **국면과 함께**: `px_market_beta`는 `market_up` 국면에서 IC가 −0.058 더 음수다(§4). `px_market_beta × 1[market_up]`이 첫 interaction 후보다.
- **horizon**: 사전등록 20·60. 4 family 모두 bucket 40→60이 살아 있어 h60 실험에 맞다. h5·h10은 exploratory였고 판정 대상이 아니다.
- **지속성**: 252세션 롤링 베타라 하루하루 거의 안 바뀐다. 회전율 부담이 작고, `lag1` 손실도 구조적으로 작다(수치는 AB 산출물에서 확인).
- **버리는 것**: `macro_beta_usdkrw`·`_kr10y`(신호 없음), `macro_rawbeta_*`(시장 베타 변장), `macro_semibeta_usdkrw_up`(primary와 부호 반대, 진단용).
- **채택 상태**: 어느 acceptance gate에도 아직 넣지 않았다. 후보 승격은 결과 문서가 정하지 않았다(`05_results_stage1a` §7).

---

## 3. `common_feature_daily_fact` / `feat_common` — 시장·거시 공통 피쳐

### 3.1 무엇이 있나

`src/krx_collector/definitions/common_features.py`의 카탈로그가 정본이다. 시리즈 36개(active 29) → feature_code 55개(active 38). DuckDB 마트 `common_build`가 `common_feature_daily_fact`를 만들고, `feat_common`은 그중 기본 12개를 `cf_` 접두어로 피벗해 날짜마다 모든 종목에 broadcast한다.

| 카테고리 | active feature_code | 원천 | availability | 비고 |
|---|---|---|---|---|
| `market_index` | `market_kospi_close`, `market_kospi_ret_1d/5d/20d`, `market_kosdaq_ret_1d`, `market_kospi200_ret_1d` | KRX MDC (`*_krx` 시리즈) | `next_krx_session` | `pykrx` 시리즈 3개는 inactive |
| `market_breadth` | KOSPI·KOSDAQ `advancers/decliners/unchanged_count` (6) | KRX MDCSTAT01501 | `next_krx_session` | 시장 폭 |
| `market_liquidity` | `market_kospi_turnover_value`, `market_kosdaq_turnover_value` | KRX | `next_krx_session` | 국면 `liq_high`의 원천 |
| `global_index` | `global_sp500_ret_1d`, `global_nasdaq_ret_1d` | FDR | `same_krx_session_morning` | NY t−1 종가 |
| `global_risk` | `global_vix_level` | FDR | `same_krx_session_morning` | 국면 `vix_up`·`vix_high` 원천 |
| `fx` | `fx_usdkrw_level`, `fx_usdkrw_ret_5d` | ECOS 매매기준율 | `next_krx_session` | FDR 시리즈는 inactive 대조용 |
| `commodity` | `commodity_wti_ret_20d`(FDR 선물), `commodity_wti_spot_ret_20d`, `commodity_wti_spot_level`(FRED 현물) | FDR / FRED | `same_krx_session_morning` | 선물·현물이 12개월 창에서 유의하게 달라 둘 다 유지 |
| `rate` | `rate_kr_gov3y_level`, `rate_kr_gov10y_level`, `rate_kr_term_spread_10y_3y`, `rate_us2y_level`, `rate_us10y_level`, `rate_us_term_spread_10y_2y` | ECOS / FRED | 국내 `next_krx_session`, 미국 `same_krx_session_morning` | 국면 `term_steep` 원천 |
| `macro_price` | `macro_cpi_level/yoy_latest/mom_latest`, `macro_ppi_*` (6) | ECOS 월간 | `manual_lag_days` (기간말 +20일) | 공식 발표 캘린더 미연결 |
| `macro_money` | `macro_m2_level/yoy_latest/mom_latest` | ECOS 월간 | 같음 | |
| `macro_employment` | `macro_unemployment_rate`, `macro_employment_rate` | ECOS 월간 | 같음 | N8. `12`가 Phase C regime 후보로 사전등록만 해 둠(dormant) |
| `macro_sentiment` | `macro_consumer_sentiment_level` | ECOS 월간 | 같음 | |
| `industry_index` | KRX 반도체·KOSPI 전기전자·금융·KOSDAQ 제약 (8) | KRX | — | **inactive, 데이터 0건** |

### 3.2 시작일·격자·PIT

- **일별 계열은 2014-06-16, 월별 매크로는 2013-06-20부터** 있다(snapshot 2026-08-23 실측, `common.py` docstring). 2026-06의 모델 문서가 적은 "2025-12-15부터"는 백필 전 값이다. 2015+ 학습 패널에서 `cf_*`는 채워져 있고, `*_isna`는 warm-up 공백만 남는다.
- **격자 주의**: fact의 `feature_date`는 2014~2023년에 KRX 세션이 아니라 **평일**이다(`docs/holidays_krx.csv`가 2024~2026만 담고 있어 연 13~17개 휴장 평일이 전 세션 값 복사로 들어 있다). 종목 패널에 `trade_date = feature_date`로 join하면 비세션 행은 빠지므로 피쳐 값에는 영향이 없지만, **fact 격자에서 `t−20`·`t−252`를 세면 창 길이가 2024년 전후로 달라진다.** 국면 변수는 이 때문에 KRX 세션 격자에서 다시 계산한다(§4.1). 모델에서 `cf_*`의 롤링 변환을 직접 만들 때 같은 함정이 있다.
- PIT는 `asof_available_date <= feature_date` 필터로 보장된다(look-ahead 위반 0 검증). 국내 계열은 한 세션 stale, 해외 계열은 NY t−1, 월간은 기간말 +20일 보수 지연이다.
- prod 갱신: KRX 계열 매일 18:30 체인, ECOS/FRED/FDR 20:30, freshness 게이트 23:00.

### 3.3 모델에서의 자리

- 현재 모델 spec은 `cf` 그룹이 **꺼져 있다.** 켜면 `build_dataset`이 `feat_common`을 날짜로 broadcast한다.
- **순위 라벨 모델에서 `cf_*` 단독은 정보가 0이다**(§1). 의미가 생기는 길은 (a) 명시적 interaction 컬럼, (b) 트리 모델의 분기 조건(암묵적 interaction), (c) 절대 상승 확률 라벨(`kind="abs"`)을 쓸 때의 시장 방향 정보 — 이 경우 라벨이 시장 방향을 포함하므로 `cf_*`가 직접 예측 변수가 된다. 목표 모델이 "시장 대비"인지 "절대 상승"인지가 여기서 갈린다(`07` §1.3).
- **구조적 경고**(`11_feature_taxonomy` §8.4): 2014~2025년 사이 금리·물가·통화 사이클이 두세 번뿐이라 어떤 매크로 변수를 넣어도 시대(era)와 거의 구분되지 않는다. 월말 134개 표본의 |ρ| 0.5~0.6은 추세끼리의 상관이다. walk-forward의 뒤쪽 fold에서 앞쪽 fold에 없던 국면이 나오면 외삽이다.
- 실증(`00_survey/00` §4.3, 월말 Spearman): 피쳐의 날짜별 **수준·IQR은 국면과 같이 움직인다.** `px_near_52w_high` 중앙값 ↔ 미국 기간 스프레드 +0.67, 외국인 순매수 중앙값 ↔ 시장 폭 +0.63, **수급 3종 IQR ↔ KOSPI 거래대금 −0.65~−0.72**(거래가 활발할수록 수급 피쳐의 횡단면 정보가 줄어든다). 이 마지막 것이 Phase C `liq_high` 쌍의 동기였고, P12에서 확인됐다.

---

## 4. 국면 변수와 Phase C 결과

### 4.1 국면 변수 7개 — formation 시점 t에 관측 가능한 정의만

`research/analysis/horizon_scan_phase_c_regimes.py`. KRX 세션 격자(`label_scan`의 distinct `trade_date`)에 fact를 join한 뒤 세션 index 위에서 LAG·창을 잡는다. 전체 표본 중앙값·사후 확정 변수(침체 날짜)는 쓰지 않는다. 판정은 이진 `s_t = 1[z_t > 0]`으로만 한다.

| id | 이름 | 연속 변수 `z_t` | `s_t=1`의 뜻 | 원천 | 역할 | 점유율 | 평균 지속(세션) / 전환 |
|---|---|---|---|---|---|---:|---|
| R1 | `vix_up` | `VIX_t − VIX_{t−20}` | 20세션 VIX 상승 | `global_vix_level` | primary | 0.44~0.56 | 6.7~7.6 / 329회 |
| R1b | `vix_high` | `VIX_t − median_252(VIX)` | VIX 수준이 1년 중앙값보다 높음 | `global_vix_level` | primary | | (Nagel 2012는 수준 — 리뷰 M6로 추가) |
| R2 | `market_up` | `ln(KOSPI_t / KOSPI_{t−252})` | 12개월 KOSPI 상승 | `market_kospi_close` | primary | | 33~40 / 64회 |
| R3 | `liq_high` | `ln(mean_20(TV) / median_252(TV))`, TV = KOSPI+KOSDAQ 거래대금 | 최근 거래대금이 1년 중앙값보다 큼 | `market_*_turnover_value` | primary | | **52~66 / 39회** |
| R4 | `term_steep` | `TS_t − median_252(TS)` | 기간 스프레드가 1년 중앙값보다 가파름 | `rate_kr_term_spread_10y_3y` | exploratory | | |
| R5 | `kosdaq_rel_up` | `Σ_20 ret_kosdaq − Σ_20 ret_kospi` | 코스닥 20일 상대 강세 | `market_*_ret_1d` | exploratory / reference | | |
| R6 | `krw_weak_20d` | `ln(USDKRW_t / USDKRW_{t−20})` | 원화 20일 약세 | `fx_usdkrw_level` | exploratory | | |

- 판정 창 `2015-06-16 ~ 2025-02-05`(252세션 warm-up 뒤), 세션 2,745개(Phase C run), 국면 사전 계산 창 2,368세션. G1(점유율 0.44~0.56) 7개 전부 통과, G2(5구간 × 7 = 35칸 양쪽 ≥ 40일) 전부 유효 — 가장 빠듯한 칸은 `market_up`의 `2023_11_common_end` 254/52.
- 국내 계열(KOSPI·거래대금·환율)은 `next_krx_session`이라 `s_t`는 "t−1 종가까지의 정보로 정한 국면"이다. PIT는 지켜지고 한 세션 오래됐을 뿐이다. VIX는 NY t−1 종가로 세션 t 아침 정보다.
- **`liq_high`·`market_up`의 지속이 HAC lag(59) 밖**이라, 무관한 두 계열도 큰 δ̂를 낼 수 있다. 그래서 국면 circular-shift placebo(G4)를 필수 게이트로 뒀다.

### 4.2 검정 — δ = E[IC | s=1] − E[IC | s=0]

Phase A·B run이 저장한 `daily_ic.parquet`(단계 0)에서 cell의 일별 IC를 읽어 국면별 평균 차이를 Newey-West OLS(`research/etl/metrics.py` `newey_west_ols`)로 검정한다. BH는 primary 15쌍 모집단, 게이트는 G1 점유율 · G2 기간 일관성 · G3 tradable 유지 · G4 국면 placebo(100회 circular shift, p ≤ 0.10) · G6 대안 cut 부호 일치(진단).

### 4.3 결과 — 15쌍 중 4쌍 `screen_pass`

| id | 가설 | 사전등록 방향 | δ̂ | t_nw | q | placebo p | 기간 | tradable | 등급 |
|---|---|---|---:|---:|---:|---:|---|---:|:-:|
| **P3** | `px_reversal_5d`(cum 0→5) × `vix_high` | `+` | **+0.0205** | 3.03 | 0.0185 | 0.0099 | 3/5 | 1.02 | **A** |
| **P9** | `px_turnover_shock`(cum 0→20) × `liq_high` | 없음 → `+` 고정 | **+0.0269** | 3.61 | 0.0046 | 0.0099 | 4/5 | 1.13 | **A** |
| **P12** | `flow_foreign_netbuy_to_volume`(cum 0→20) × `liq_high` | 없음 → `+` 고정 | **+0.0163** | 2.89 | 0.0193 | 0.0099 | 5/5 | 0.86 | **A** |
| **P15** | `px_market_beta`(cum 0→20) × `market_up` | 없음 → `−` 고정 | **−0.0579** | −2.55 | 0.0407 | 0.0099 | 5/5 | 0.96 | **A** |
| P1 | `px_idio_vol_60d`(cum 0→60) × `vix_up` | `+` | +0.0115 | 1.23 | — | 0.1089 | — | — | D (부호는 맞았다, 다음 후보) |
| P2 | `px_maxret_20d`(cum 0→60) × `vix_up` | `+` | | | | > 0.10 | | | D |
| P4 / P5 | `px_mom_12_1`(cum 0→60) × `market_up` / `liq_high` | `+` | | | | > 0.10 | | | D |
| P6 | `px_amihud_20d`(cum 0→60) × `liq_high` | `−` | | | | > 0.10 | | | D |
| P7 / P11 | `flow_foreign_netbuy_to_volume` × `market_up` / `vix_up` | `+` / 없음 | | | | > 0.10 | | | D |
| P8 / P14 | `flow_individual_netbuy_to_volume` × `market_up` / `liq_high` | `−` / 없음 | | | | > 0.10 | | | D |
| P10 / P13 | `flow_inst_netbuy_to_volume` × `market_up` / `liq_high` | 없음 | | | | > 0.10 | | | D |
| X1 (reference) | `fin_log_mcap`(cum 0→60) × `market_up` | — | +0.0200 | 0.93 | p 0.353 | 0.772 | | | R |
| X2 (reference) | `fin_log_mcap`(cum 0→60) × `kosdaq_rel_up` | — | −0.0049 | −0.42 | p 0.677 | 0.663 | | | R |

(`01_design/05_results_stage1b_20260830.md`. 실패 쌍의 δ̂·t는 그 문서에 없어 비워 뒀다. `conditional_ic.parquet`에 있다.) 게이트별 실패: discovery 11, G4 11, G2 4(P1·P2·P5·P7), G3 2(P2·P7). G2·G3 단독 실패는 없다.

**G4가 BH와 15쌍에서 한 번도 엇갈리지 않았다.** discovery 4쌍은 placebo p가 전부 최솟값 0.0099(100회 중 `|t_shift| ≥ |t_real|` 0회), 나머지 11쌍은 전부 > 0.10. reference 2쌍은 `|t| < 2`로 대조군답게 실패했다 — 규모효과는 국면 무관(엄철준 외 2024).

### 4.4 통과 4쌍 해석과 독립성

- **P3** VIX 수준이 높을 때 단기 반전이 +0.0205 더 크다. Nagel(2012) "Evaporating Liquidity"의 한국 재현. 국면을 `vix_up`(변화)이 아니라 `vix_high`(수준)로 바꾼 것이 결정적이었다 — 같은 family의 `vix_up` exploratory 쌍은 실패.
- **P9** 거래대금이 많을 때 회전율 충격의 IC가 더 크다. 박종원(2020) "반전은 고회전율·전환시장에서 강함"과 부호 일치. `08_px_turnover_shock.md`의 "반대 부호 5/5 일관"에 국면 구조가 있다.
- **P12** 거래대금이 많을 때 외국인 순매수 비율의 IC가 더 크다. 시장 방향(P7)·위험선호(P11) 축은 실패 — **외국인 수급의 국면 의존은 유동성 축**이다. Kang-Kwon-Park(2014) "상승장에서만"은 재현되지 않았다.
- **P15** 상승장에서 시장 베타의 IC가 더 음수다. 1a의 무조건부 BAB 효과가 상승장에서 강해진다.
- **독립된 발견은 넷이 아니라 셋 정도다.** P9·P12가 `liq_high`를 공유한다. 유동성 국면 하나 + VIX 수준 하나 + 시장 상태 하나로 읽는다(`03` §8 해석 규칙).
- 변동성 축(P1·P2)과 모멘텀 축(P4·P5)은 국면 조건화가 통하지 않았다. 재무·이벤트 18 family는 Phase C에 넣지 않았다.

---

## 5. 모델 투입 메모 — 거시를 "한 번에" 넣는 순서

1. **명시적 interaction 4개**를 먼저 만든다. `s_t`는 §4.1 정의(KRX 세션 격자), `x`는 날짜×시장 rank.
   - `rank(px_reversal_5d) × 1[vix_high]` — h5 실험
   - `rank(px_turnover_shock) × 1[liq_high]` — h20 (부호는 관측 `+`, 단변량 D등급이었던 반대 부호와 방향을 맞춘다)
   - `rank(flow_foreign_netbuy_to_volume_20d) × 1[liq_high]` — h20 (지연 게이트 실패 family라 실행 지연에 취약)
   - `rank(px_market_beta) × 1[market_up]` — h20·h60
   증분성은 acceptance gate 방식(baseline + 특성 단독 vs + interaction)으로 따로 봐야 한다. 이건 아직 하지 않았다.
2. **exposure 베타 4 family**(`vix`·`wti`·`sp500_lag`·`px_market_beta`)를 보통 피쳐로 넣는다. h20·h60. `usdkrw`·`kr10y`·`rawbeta`·`semibeta`는 넣지 않는다.
3. **`cf_*`를 직접 넣는 것은 라벨 선택에 달렸다.** 시장 대비 순위 라벨이면 정보가 0이고 트리에서만 암묵적 interaction으로 산다. 절대 상승 라벨이면 시장 방향 예측 변수가 되지만, 사이클 2~3회 표본에서 외삽 위험이 크다(§3.3).
4. **국면 변수를 피쳐로 넣을 때도 이진·연속 둘 다 KRX 세션 격자에서 만든다.** fact 격자에서 만든 20·252 창은 2024년 전후로 길이가 다르다.
5. **N8 고용 regime**(`macro_unemployment_rate_level`·`macro_employment_rate_level`)은 사전등록만 있고 검정하지 않았다. 넣으려면 새 Phase C 사전등록이 먼저다.

---

## 6. 한계

- 국면 정의 7개, 쌍 15개에서 나온 결과다. "매크로 조건화 일반이 통한다"는 뜻이 아니다.
- 실패 쌍 P1(`px_idio_vol_60d × vix_up`, δ̂ +0.0115, placebo 0.1089)은 아깝지만 판정은 실패다. 국면 cut을 옮기거나 horizon을 바꾸지 않는다(`04` §5 "결과를 보고 하지 않는 것").
- 매매기준율 산출 시점 미검증, `docs/holidays_krx.csv` 2014~2023 미보강(보강하면 fact 격자 문제가 원천에서 사라지지만 golden·재현값이 바뀐다).
- 단계 2(ECOS 회사채 스프레드·기준금리·ESI, FRED HY OAS·breakeven·EPU)와 단계 3(관세청 수출 잠정치, SOX)은 새 수집이라 이번 범위 밖이다(`00_survey/02` §8).
- holdout(2025-08-01~)은 열지 않았다.
