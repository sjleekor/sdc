# 02. 가격·유동성 — 모델 개발용 피쳐 카드

- 작성일: 2026-09-03
- 대상: `px_reversal_5d`, `px_mom_12_1`(secondary `px_mom_6_1`), `px_resid_mom_12_1`, `px_near_52w_high`,
  `px_maxret_20d`, `px_idio_vol_60d`, `px_amihud_20d`, `px_turnover_shock`, `px_zero_ret_ratio_20d`
- 기준 run: Phase A `20260827T221729-4e0ae8b0` / Phase B `20260828T123313-4e0ae8b0` / AB `20260828T165038-4e0ae8b0`
  (snapshot `2026-08-23`, config `889c3e83…`). 매크로·Phase C는 `236d0d35…` 계보(`20260830T085718-efd35e70`,
  `20260830T100518-efd35e70`, `20260830T122850-phasec`).
- 항목 정의와 읽는 법은 [00_reading_guide.md](00_reading_guide.md)를 먼저 본다.
- 원문 해설: `docs/dev/20260829_raw_features_explain/` (이 문서의 `(01 §5.1)` 같은 괄호는 그 묶음의 문서 번호와 절이다)

이 문서는 검증 결과를 **모델 개발자 시점**으로 다시 편 것이다. 판정의 정본은 보고서와 `00_status.md`이고, 숫자의
정본은 위 run의 parquet이다. 여기 없는 숫자는 만들지 않았다.

---

## 0. 그룹 한눈에 보기

| # | family | primary feature | 무엇을 재나 | 기대→관측 부호 | 등급 | screen_pass | 대표 cell IC | 대표 q5 spread | 모델 사용 | 한 줄 메모 |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `px_reversal_5d` | `px_reversal_5d` | 최근 5거래일 로그수익률 합의 음수 | `+`→`+` | A | 통과 | cum 0→3 **+0.0533** | cum 0→5 +0.38%p | T1 후보(비채택) | 35개 중 q 최소(3.29e-95). onset 1·peak 3·half-life 5~10. 하루 늦으면 31% 사라짐 |
| 2 | `px_mom_12_1` | `px_mom_12_1` | 252→21거래일 전 로그수익률 | `+`→**`−`** | D | 실패 | cum 0→120 −0.0326 | −1.60%p | 미투입 | 반대 부호도 기간 2/5·placebo p 0.297로 불안정. `px_mom_6_1`은 미실행 |
| 3 | `px_resid_mom_12_1` | `px_resid_mom_12_1` | 시장모형 잔차의 21~252일 전 누적 | `+`→**`−`** | D | 실패 | cum 0→20 −0.0100 | −0.09%p | 미투입 | BH 통과 cell 0개. 표본 2,207일로 그룹 최단 |
| 4 | `px_near_52w_high` | `px_near_52w_high` | 종가 / 252일 최고 종가 − 1 | `+`→`+` | A | 통과 | bucket 40→60 +0.0312 | −0.07%p | baseline 40에 포함(`px_dist_52w_high` 동일 산식) | bucket만 3/6 discovery. **IC 양수인데 spread 여섯 cell 전부 음수** |
| 5 | `px_maxret_20d` | `px_maxret_20d` | 최근 20거래일 최대 단순수익률 | `−`→`−` | A | 통과 | cum 0→60 −0.1133 | +1.79%p(정렬) | T1 후보(비채택) | 6/6·기간 5/5·시간 placebo 0.0099 통과. IVOL과 중복 미확인 |
| 6 | `px_idio_vol_60d` | `px_idio_vol_60d` | 시장모형 잔차의 60일 표준편차 | `−`→`−` | A | 통과 | cum 0→60 **−0.1433** | +2.99%p(정렬) | T1 후보(비채택) | 35개 중 \|IC\| 최대. MAX와 중복 미확인. 표본 2,334일 |
| 7 | `px_amihud_20d` | `px_amihud_20d` | mean(\|수익률\| / 거래대금) 20일 | `+`→`+` | A | 통과 | cum 0→120 +0.1343 | **+11.21%p** | baseline 40에 포함 | spread 35개 중 최대. 규모와 ρ −0.754/−0.721. tradable 유지율 0.852 |
| 8 | `px_turnover_shock` | `px_turnover_shock` | ln(당일 거래대금 / 직전 60일 중앙값) | `+`→**`−`** | D | 실패 | cum 0→20 −0.0264 | +0.13%p | 미투입 | 반대 방향이 5구간 전부 일관, q 3.18e-11. Phase C P9(`liq_high`) 통과 |
| 9 | `px_zero_ret_ratio_20d` | `px_zero_ret_ratio_20d` | 20일 중 무변동·무거래일 비율 | 없음→— | R | 대상 아님 | — | — | 기준용 | 애초에 재지 않았다. `—`는 0이 아니다 |

`대표 cell IC`는 원문 해설(`889c3e83…` run)의 값이다. `09_all_feature_results.md` §1(config `ab0de634…`, 25 family)의
대표 IC는 일부 다르다 — `px_idio_vol_60d` −0.1524, `px_maxret_20d` −0.1162, `px_near_52w_high` +0.0325,
`px_mom_12_1` −0.0333, `px_turnover_shock` −0.0342, `px_resid_mom_12_1` −0.0113. 두 run의 config가 다르므로 이
문서는 기준 run인 해설 값을 쓴다. `px_reversal_5d` +0.0533과 `px_amihud_20d` +0.1343은 두 문서가 같다.

이 그룹에는 T1의 C(판정 보류)도 T2의 C(강건성 실패)도 없다. D 셋은 전부 "부호 반대"이고, R 하나는 "재지 않음"이다.

### 0.1 이 그룹 공통

- 원천은 전부 `daily_ohlcv` 하나다. prod는 매일 18:30 Cronicle 체인(FDR Universe → Prices, `prices backfill --market all --incremental`)으로 갱신한다
  (`docs/operations.md`). 마트 `feat_price`는 `bin/parquet-compute-all.sh`를 사람이 돌릴 때 다시 만든다.
- 거래정지일(`open=high=low=0`)은 유효 세션 패널에서 빠진다(`research/etl/trading_panel.py` `build_valid_session_sql`).
  창은 달력이 아니라 **유효 거래행 수**다. `log_ret = ln(close / 직전 유효세션 close)`, `simple_ret = close / 직전 − 1`,
  `turnover = close × volume`(실제 체결대금이 아니다).
- 패널 편입 warm-up: 직전 60행 중 유효 세션 40 이상(`universe.warmup_window: 60`, `warmup_min_valid: 40`).
- 기업행동 마스킹: 창 안에 `ca_event`가 한 건이라도 있으면 값 전체 NULL. 판정 기준 `ca_abs_share_change: 0.25`,
  `ca_ratio_product_range: [0.8, 1.25]`. `px_amihud_20d`만 마스킹이 없다.
- 표준화는 마트에서 하지 않는다("winsor/log/z-score is the model preprocess step", `price.py` docstring).
- 라벨은 `(거래일, 시장)` 동일가중 평균을 뺀 초과수익률의 시장 내 백분위 순위다. **시장 중립이지 업종 중립이 아니다**
  (00 §4.1). 횡단 그룹은 KOSPI·KOSDAQ 둘뿐이다.
- 표본 2014-06-01~, holdout 2025-08-01~(열지 않았다). `common_survivor`는 120일 라벨 때문에 2025-02-05에서 끝난다.
- 산식 줄 번호는 **현재 `research/etl/features/price.py`** 기준이다. 원문 해설이 인용한 줄 번호(`:97` 등)는 시장모형이
  `trading_panel.build_market_model_sql`로 옮겨지기 전의 것이라 지금 파일과 다르다.
- `px_market_beta`는 `feat_macro_exposure` 마트에 있고 [06_macro_regime.md](06_macro_regime.md)가 다룬다.

---

## 1. `px_reversal_5d` — 5일 단기 반전

| 항목 | 값 |
|---|---|
| primary feature | `px_reversal_5d` |
| secondary / variant | secondary — · `px_reversal_5d_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_price` / `research/etl/features/price.py:70` (마스킹 `:117`, lag1 `:139`) |
| 원천 raw 테이블 | `daily_ohlcv` (+ `dim_price_quality_daily`의 `ca_event`) |
| prod 갱신 | 매일 18:30 Cronicle 체인(`prices backfill --incremental`) |
| 분류 좌표 | C1 × T1 × U (11_feature_taxonomy §3) |
| 검증 phase / fdr_family / role | A / price / ready |
| 기대 부호 → 관측 부호 | `+` → `+` |
| 사전등록 primary horizon | [1, 2, 3, 5, 10] · bucket 포함(0→5, 5→10) · exploratory [20, 40, 60, 120] |
| 관측 신호 밴드 · 모양 | 1~10일 · onset 1 / peak_h_cum 3 / peak_bucket [0, 5] / half-life bucket [5, 10] / pattern `immediate` (01 §4.3) |
| 등급 / screen_pass / discovery | A / 통과 / 7/7 cell (AB 결합에서도 7/7) |
| 모델 검증 이력 | T1 후보 5개 묶음(baseline 40 + 5) → walk-forward Rank IC h5/20/60 전부 개선 → k=100 h20 비용반영 Δ **−0.0045** → 묶음 비채택. 개별 기여도 미측정 (01 §9) |

### 산식과 규칙

```sql
-SUM(log_ret) OVER w5 AS px_reversal_5d
-- w5 = PARTITION BY ticker, market ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
CASE WHEN ca_count_5 > 0 THEN NULL ELSE px_reversal_5d END
```

- 최근 5거래행 로그수익률 합에 마이너스를 붙였다. **값이 클수록 최근 많이 떨어진 종목**이다.
- 창은 유효 거래행 5개다. 정지일을 건너뛰므로 달력으로는 5일보다 길 수 있다 (01 §2.2).
- 5일 창 안에 기업행동이 있으면 NULL. 가격 계열 중 마스킹 창이 가장 짧아 버려지는 행이 가장 적다 (01 §2.3).
- 단위는 로그수익률 합(부호 반전). 표준화 없음.

### 가용 시점(PIT)과 지연

- 당일 종가로 확정된다. 정본 variant는 `native_t`(`execution.price_default_official_variant`).
- lag1 유지율 **0.686** (native 0.0470 → lag1 0.0323, cum 0→1). 게이트 0.50은 넘지만 **하루 늦으면 31%가 사라진다.**
  검증한 A등급 중 지연 손실이 큰 편이다 (01 §5.1).
- 원천 지연은 당일 18:30 수집 한 번이다. 장 마감 후 계산해 다음 세션 시초에 쓰는 것까지가 이 유지율의 범위다.

### 예측력 — broad × common_survivor × native_t

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→1 | +0.0470 | 0.408 | 20.91 | +0.11%p | ~0 | discovery |
| cum | 0→2 | +0.0514 | 0.471 | 20.25 | +0.22%p | ~0 | discovery |
| cum | 0→3 | **+0.0533** | 0.498 | 18.69 | +0.30%p | ~0 | discovery |
| cum | 0→5 | +0.0491 | 0.480 | 15.26 | **+0.38%p** | ~0 | discovery |
| cum | 0→10 | +0.0362 | 0.376 | 9.76 | +0.36%p | ~0 | discovery |
| bucket | 0→5 | +0.0491 | 0.480 | 15.26 | +0.38%p | ~0 | discovery |
| bucket | 5→10 | +0.0080 | 0.093 | 2.81 | **−0.01%p** | 0.0079 | discovery |

(01 §4.1) family 최소 q: Phase A 3.29e-95, AB 6.72e-95 — 35개 family 중 가장 작다.

- IC 정점은 3일, spread 정점은 5일이다. 보유가 길수록 누적이 커지기 때문이고 둘은 다른 최대점이다 (01 §4.2).
- `bucket 5→10`은 IC +0.0080으로 유의(q 0.0079)하지만 spread는 −0.01%p로 사실상 0이다. **유의성과 경제성이 갈린다.**
  순위 모델 입력이면 IC를, 분위 롱숏이면 spread를 본다 (00 §9.1).
- 일별 IC는 2,622일 중 1,772일(67.6%)에서 양수, 부호검정 p 3.75e-74 (01 §4.4).

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 0.954 (broad 0.0470 / tradable 0.0449) | 통과, 기준 0.50 |
| lag1 유지율 | 0.686 (`delay_pass = true`) | 통과, 기준 0.50 |
| available 표본 부호 | available 0.0460 vs common 0.0470, 같은 부호, `attrition_warning = false` | 통과 |
| 기간 일관성 | **5/5** | 통과 |
| 시간 placebo | **대상 아님** — primary cell의 NW lag 최대 9 < 59 (`placebo.temporal_min_nw_lag: 59`). `robustness_required` 아님. **받지 않았다** | — |
| 비중첩 offset | `offset_status = complete`, offset 1개, 부호 일치율 1.0. 짧은 horizon이라 격자가 좁다 | 통과 |
| source 경고 (Phase B) | 대상 아님 (Phase A family) | — |
| 시장 구성 | KOSPI 41.2% / KOSDAQ 58.8% | — |

(01 §5)

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | 2,622일 (`available` 2,741일) |
| 날짜당 종목 수 | 1,098개 |
| 전체 관측 | 약 288만 행 (cum 0→1 기준) |
| coverage_ratio (Phase B) | 대상 아님 — Phase A는 계산하지 않는다 |
| KOSPI/KOSDAQ 비중 | 41.2% / 58.8% |

(01 §6) 35개 중 표본이 가장 긴 축이다.

### 중복성

- A×B 교차 상관 상위: `mcap_krx_log` −0.033, `ev_payout_yield` −0.027, `fin_log_mcap` −0.019, `fin_gross_profitability` −0.018.
  전부 0.05 미만 — Phase B 재무·공시 계열과 사실상 독립 (01 §7).
- 같은 마트 형제와의 구조: `px_mom_12_1`은 21일을 skip해 시간축에서 겹치지 않도록 설계됐다 (02 §2.2).
- **미확인 중복**: A×A 상관 산출물이 없다. `px_maxret_20d`·`px_idio_vol_60d`·`px_turnover_shock`와 겹칠 여지가 있다
  (급락 종목은 변동성도 크다) (01 §7).

### 국면 의존 (Phase C)

| 쌍 | 국면 | cell | 기대 δ | δ̂ | t_nw | q | placebo p | 판정 |
|---|---|---|---|---:|---:|---:|---:|---|
| P3 | `vix_high` (VIX − 252세션 중앙값 > 0) | cum 0→5 | `+` | **+0.0205** | 3.03 | 0.0185 | 0.0099 | **screen_pass, 등급 A** |

- VIX 수준이 지난 1년 중앙값보다 높은 국면에서 단기 반전 IC가 +0.0205 더 크다. Nagel(2012)의 방향과 같다.
  G2 유효 구간 5개 중 부호 일치 3개(⌈5/2⌉ = 3, 간신히 통과), G3 tradable 유지율 1.02 (05_results §4).
- 같은 family를 `vix_up`(20세션 변화)에 건 exploratory 쌍은 통과하지 못했다 — **수준 변수여야 했다** (05_results §4).
- 매크로 수준 동행(참고): 날짜별 중앙값이 `breadth_20d`와 ρ −0.45, `vix_chg_20d`와 +0.35. 시장이 빠진 뒤 값이 올라가는
  정의상 결과다 (00_survey §4.3).

### 모델 투입 메모

- **변환**: `(date, market)` 내 rank. 검증 IC가 순위 기반이라 rank 입력이 검증 결과와 가장 가깝다. 원값(로그수익률 합)을
  그대로 쓰려면 급락 극단값 winsor가 필요하다.
- **결측**: 5일 창 기업행동 마스킹과 warm-up(60행 중 40)뿐이라 결측이 가장 적다. `*_isna` indicator로 충분하다.
- **유의점**: 회전이 빠르다. 5일마다 갈아타면 연 50회, T1 실측 turnover는 h5에서 0.68~0.78(게이트 0.696/0.682,
  k=100 0.780/0.767)이었다. 왕복 60bp를 빼면 0.38%p에서 남는 게 거의 없다 (01 §4.2). 급락 종목은 스프레드가 벌어져
  bid-ask bounce에 노출된다 (01 §3.4). tradable 유지율 0.954라 유동성 편중은 아니다.
- **horizon**: 사전등록 1~10일과 관측 밴드가 정확히 일치한다. **h=5 실험의 주력 후보**다. h=20 이상은 exploratory에서
  신호가 새지 않았으므로 h=20/60/120 실험에는 단독 기여가 작다. 다만 T1 묶음은 h60에서도 Rank IC +0.0087 개선을 냈다
  (grade_a_acceptance_gate_results).
- **조합 후보**: 사전 설계(`02_feature_candidate` §3.1 P2)가 요구한 회전율·Amihud·MAX와의 상호작용은 아직 보지 않았다.
  Phase C P3(`vix_high`)가 통과했으므로 `s_vix_high × px_reversal_5d`는 acceptance gate 후보로 올릴 수 있는 첫 interaction이다
  (채택 여부는 05_results §7이 이 문서 밖으로 미뤘다).

### 한계

- 거래비용 반영 성과가 없다. 5일 회전 신호에서 이것이 결정적 공백이다 (01 §8).
- 회전율·Amihud·MAX 상호작용과 같은 가격 계열과의 A×A 상관이 미확인이다.
- 업종 중립화가 없어 업종 동반 급락일의 신호가 업종 효과인지 구분하지 못한다.
- 등급 A지만 시간 placebo는 받지 않았다(대상 아님).
- holdout(2025-08-01~)을 열지 않았다.

---

## 2. `px_mom_12_1` — 12개월-1개월 모멘텀

| 항목 | 값 |
|---|---|
| primary feature | `px_mom_12_1` |
| secondary / variant | `px_mom_6_1`(secondary, **이번 run에 행 없음**) · `px_mom_12_1_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_price` / `research/etl/features/price.py:73` (`px_mom_6_1` `:71`, 마스킹 `:119`·`:118`) |
| 원천 raw 테이블 | `daily_ohlcv` (+ `ca_event`) |
| prod 갱신 | 매일 18:30 Cronicle 체인 |
| 분류 좌표 | C1 × T1 × U |
| 검증 phase / fdr_family / role | A / price / ready |
| 기대 부호 → 관측 부호 | `+` → **`−`** |
| 사전등록 primary horizon | [20, 40, 60, 120] · bucket 포함(10→20, 20→40, 40→60, 60→120) · exploratory [1, 2, 3, 5, 10] |
| 관측 신호 밴드 · 모양 | **없음**(`candidate_horizon_band = null`) · pattern `no_signal` · peak_h_cum 120(음수 방향) · peak_bucket [40, 60] (02 §4.4) |
| 등급 / screen_pass / discovery | D / 실패 / 0/8 (BH 통과는 4 cell, `expected_sign_pass = false`) |
| 모델 검증 이력 | T1·T2 후보 아님. baseline 40의 `px_mom_20_60`은 다른 산식이다 (02 §9) |

### 산식과 규칙

```sql
LN(LAG(close_d, 21) OVER w / NULLIF(LAG(close_d, 252) OVER w, 0)) AS px_mom_12_1
LN(LAG(close_d, 21) OVER w / NULLIF(LAG(close_d, 126) OVER w, 0)) AS px_mom_6_1
CASE WHEN ca_count_252_skip21 > 0 THEN NULL ELSE px_mom_12_1 END   -- w252_skip21 = 252 PRECEDING AND 21 PRECEDING
```

- 252거래행 전부터 21거래행 전까지의 로그수익률. 최근 21행은 단기 반전과 섞이지 않게 뺐다 (02 §2.2).
- `LAG(close_d, 252)`가 반드시 있어야 하므로 **상장 후 252행 미만이면 NULL**이다. 마스킹 창도 231행이라 버려지는 행이 많다.
  같은 원천인데 `px_reversal_5d`보다 표본이 162일 짧다 (02 §2.3).
- `px_mom_6_1`도 마스킹은 `ca_count_252_skip21`(6개월 창이 아니라 12개월 창)을 쓴다 (`price.py:118`).

### 가용 시점(PIT)과 지연

- 당일 종가 기준 `native_t`. 이미 21일을 skip한 신호라 하루 지연의 뜻이 없다.
- native −0.0326 / lag1 −0.0336 (cum 0→120). `delay_pass = null` — 방향 게이트를 통과하지 못한 family에는 지연 게이트를
  적용하지 않는다. **떨어진 것이 아니라 대상이 아니다** (02 §5.6).

### 예측력 — broad × common_survivor × native_t

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | −0.0165 | −0.160 | −2.14 | −0.33%p | 0.049 | BH 통과, 부호 반대 → discovery 아님 |
| cum | 0→40 | −0.0215 | −0.208 | −1.97 | −0.50%p | 0.068 | BH 통과, discovery 아님 |
| cum | 0→60 | −0.0248 | −0.247 | −1.97 | −0.64%p | 0.068 | BH 통과, discovery 아님 |
| cum | 0→120 | **−0.0326** | −0.369 | −2.05 | **−1.60%p** | 0.059 | BH 통과, discovery 아님 |
| bucket | 10→20 | −0.0082 | −0.083 | −1.58 | −0.17%p | 0.141 | BH 실패 |
| bucket | 20→40 | −0.0077 | −0.078 | −1.04 | −0.22%p | 0.339 | BH 실패 |
| bucket | 40→60 | −0.0073 | −0.077 | −1.02 | −0.20%p | 0.342 | BH 실패 |
| bucket | 60→120 | −0.0190 | −0.214 | −1.67 | −0.93%p | 0.122 | BH 실패 |

(02 §4.1) IC와 spread의 부호는 여덟 cell 전부 같다(둘 다 음수). 어긋남은 없다.

- 반대 부호를 신호로 쓰려면 새 config로 다시 사전등록해야 한다. 사후에 부호를 고르면 사전등록이 무의미해진다 (02 §4.2).
- `peak_h_cum = 120`은 "음수 방향으로 가장 컸다"는 뜻이다. 성과로 읽으면 안 된다 (02 §4.4).

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 1.023 (broad −0.0326 / tradable −0.0333) — 유동성 좋은 종목에서 반대 부호가 조금 더 강하다 | 기준 통과(방향은 반대) |
| lag1 유지율 | native −0.0326 / lag1 −0.0336. `delay_pass = null` | 대상 아님 |
| available 표본 부호 | 0→120에서 −0.0326 = −0.0326, 0→20에서 −0.0143 vs −0.0165. 부호 같음. card `limitations: survival_bias_unresolved` | 뒤집힘 없음 |
| 기간 일관성 | **2/5** — 세 구간에서 부호가 반대였다 | 불안정 |
| 시간 placebo | p **0.297**, `temporal_null_pass = false` (cum 0→120, NW lag 119 ≥ 59 → 대상) | **실패** |
| 비중첩 offset | 120개 전부 유효, 부호 일치율 **0.0**, 부호검정 p 중앙값 0.868, offset IC 범위 −0.043 ~ −0.024 | 실패 |
| source 경고 (Phase B) | 대상 아님 | — |
| 시장 구성 | KOSPI 41.9% / KOSDAQ 58.1% | — |

(02 §5) offset IC가 전부 음수인데 부호검정을 통과하지 못한 것은 offset 하나당 유효일이 21일 안팎으로 짧아 **날짜 단위로는
방향이 흩어져 있기 때문**이다 (02 §5.3).

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | **2,460일** (`available` 2,560일, 0→20 cell) |
| 날짜당 종목 수 | 1,058~1,061개 |
| coverage_ratio (Phase B) | 대상 아님 |
| KOSPI/KOSDAQ 비중 | 41.9% / 58.1% |

(02 §6)

### 중복성

- A×B 상위: `mcap_krx_log` **+0.161**(범위 −0.12 ~ +0.49), `fin_asset_growth_yoy` +0.157, `fin_gross_profitability` +0.132,
  `fin_log_mcap` +0.116. 35개 중 A×B 상관이 큰 축이다. 1년간 많이 오른 종목은 시가총액도 커져 있다는 기계적 연결이고,
  범위가 넓어 날짜에 따라 크게 흔들린다 (02 §7). `|ρ| ≥ 0.7` 경고 대상은 아니다.
- **미확인 중복**: `px_resid_mom_12_1`과 같은 창을 쓰므로 강하게 겹칠 것이 분명한데 A×A 상관이 없다. `px_near_52w_high`도
  252일 경로를 공유한다 (02 §7, 04 §7).

### 국면 의존 (Phase C)

| 쌍 | 국면 | cell | 기대 δ | δ̂ | t_nw | q | placebo p | G2 / G3 / G4 | 판정 |
|---|---|---|---|---:|---:|---:|---|---|---|
| P4 | `market_up` (ln KOSPI_t/KOSPI_{t−252} > 0) | cum 0→60 | `+` | −0.0272 | −1.21 | 0.3481 | > 0.10 | ✅ / ✅ / ✗ | D |
| P5 | `liq_high` (20일 시장 거래대금 / 252일 중앙값 > 1) | cum 0→60 | `+` | −0.0267 | −1.14 | 0.3481 | > 0.10 | ✗ / ✅ / ✗ | D |

(run 보고서 `03c_conditional_ic_results.md`; discovery 아닌 11쌍의 placebo p는 전부 > 0.10, 05_results §2)

- 두 쌍 모두 δ̂ 부호가 사전등록(`+`)과 반대이고 유의하지 않다. "상승장·고유동성 국면에서 모멘텀이 살아난다"는 문헌
  가설이 이 표본에서는 재현되지 않았다. 05_results §6은 이를 "모멘텀 축은 국면 조건화가 통하지 않았다"로 읽는다.
- 매크로 수준 동행(참고): 중앙값이 `kr_term_spread`·`us_term_spread`와 +0.55. "모멘텀 수준 = 지난 1년 시장"이다
  (00_survey §4.3).

### 모델 투입 메모

- **변환**: `(date, market)` 내 rank. 원값은 로그수익률이라 극단 winsor 후 z도 가능하다.
- **결측**: 252행 LAG 요구 + 231행 마스킹 창 → 상장 1년 미만 종목은 통째로 NULL이다. NULL이 상장 연차와 강하게 얽히므로
  `*_isna` indicator가 사실상 "신규 상장" 플래그가 된다는 점을 알고 써야 한다.
- **유의점**: 부호를 고정한 선형 alpha로 넣을 근거가 없다. 기간 2/5·placebo 실패·offset 일치율 0.0이 모두 "방향이
  불안정하다"고 말한다. 비선형 모델이 부호를 스스로 배우게 하더라도 fold 간 뒤집힘을 예상해야 한다. 규모(+0.16)와의
  상관 때문에 규모 피쳐와 함께 넣으면 일부가 규모 효과로 흡수된다.
- **horizon**: 사전등록 20~120일 어디에도 후보 밴드가 없다. h=60/120 실험에서 "모멘텀이 없다"는 사실 자체를 확인하는
  참고 컬럼 이상의 자리는 없다.
- **조합 후보**: `px_mom_x_volume = rank(mom_12_1) × rank(volume_trend)` (P8, Lee & Swaminathan 2000) 미구현. 잔차·수급
  결합(P1 사전 메모) 미확인. 규모를 통제한 증분 IC 없음. `px_mom_6_1`은 등록만 되고 실행되지 않았다.

### 한계

- 반대 부호를 결론으로 쓸 수 없다 — 세 검사 모두 "안정적이지 않다" (02 §8).
- `px_mom_6_1`을 보지 않았고, 규모 효과와 분리하지 않았고, 잔차 모멘텀과의 상관을 재지 않았다.
- 어느 구간에서 부호가 뒤집혔는지는 기간별 heatmap(`plots/px_mom_12_1_subperiod_heatmap.png`)에서만 볼 수 있다.
- 업종 중립화가 없다. holdout을 열지 않았다.

---

## 3. `px_resid_mom_12_1` — 잔차 모멘텀

| 항목 | 값 |
|---|---|
| primary feature | `px_resid_mom_12_1` |
| secondary / variant | secondary — · `px_resid_mom_12_1_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_price` / `research/etl/features/price.py:75` (마스킹 `:120`); 잔차는 `research/etl/trading_panel.py` `build_market_model_sql` |
| 원천 raw 테이블 | `daily_ohlcv` (+ `ca_event`) |
| prod 갱신 | 매일 18:30 Cronicle 체인 |
| 분류 좌표 | C1 × T1 × U |
| 검증 phase / fdr_family / role | A / price / ready |
| 기대 부호 → 관측 부호 | `+` → **`−`** (0에 가깝다) |
| 사전등록 primary horizon | [20, 40, 60, 120] · bucket 포함 · exploratory [1, 2, 3, 5, 10] |
| 관측 신호 밴드 · 모양 | **없음** · pattern `no_signal` · sign_flip_bucket **[40, 60]** · peak_h_cum 20(음수 방향) · half_life_bucket [60, 120](참고값) (03 §4.4) |
| 등급 / screen_pass / discovery | D / 실패 / 0/8 · **BH 통과 cell도 0개** (최소 q 0.139) |
| 모델 검증 이력 | T1·T2 후보 아님 |

### 산식과 규칙

```sql
-- trading_panel.build_market_model_sql: 동일가중 시장수익률 → 252세션 rolling 시장모형(당일 제외) → 잔차
AVG(log_ret) OVER (PARTITION BY trade_date, market) AS market_ret
REGR_SLOPE(log_ret, market_ret)     OVER (... ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING) AS beta_252
REGR_INTERCEPT(log_ret, market_ret) OVER (... ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING) AS alpha_252
CASE WHEN model_n_252 >= 252 THEN log_ret - (alpha_252 + beta_252 * market_ret) END AS resid_ret
-- price.py:75
CASE WHEN COUNT(resid_ret) OVER (... ROWS BETWEEN 252 PRECEDING AND 21 PRECEDING) = 232
     THEN SUM(resid_ret) OVER (... ROWS BETWEEN 252 PRECEDING AND 21 PRECEDING) END AS px_resid_mom_12_1
CASE WHEN ca_count_252_skip21 > 0 THEN NULL ELSE px_resid_mom_12_1 END
```

- 세 겹 조건: 잔차 하나에 회귀 표본 252개, 피쳐 하나에 잔차 **정확히 232개**(부등호가 아니다), 그 232개 각각이 다시 252개
  회귀 표본. 결국 **연속 484거래일(약 2년)** 이력이 필요하다 (03 §2.2). config `resid_mom_model_min_valid: 252`,
  `resid_mom_require_complete_history: true`.
- 벤치마크는 **같은 날 같은 시장의 동일가중 평균 로그수익률**이다. 라벨과 같은 벤치마크다 (03 §2.4).
- 같은 `resid_ret`을 `px_idio_vol_60d`와 `feat_macro_exposure`의 `macro_beta_*`가 공유한다 (`trading_panel.py` docstring).

### 가용 시점(PIT)과 지연

- 당일 종가 기준 `native_t`. native −0.0100 / lag1 −0.0096 (cum 0→20). 21일 skip 신호라 하루 차이는 뜻이 없다.
- `delay_pass = null` — 방향 게이트 실패로 대상 아님 (03 §5.6).

### 예측력 — broad × common_survivor × native_t

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | **−0.0100** | −0.126 | −1.60 | −0.09%p | 0.139 | BH 실패, 부호 반대 |
| cum | 0→40 | −0.0092 | −0.116 | −1.10 | **+0.02%p** | 0.313 | BH 실패 |
| cum | 0→60 | −0.0071 | −0.096 | −0.77 | **+0.21%p** | 0.484 | BH 실패 |
| cum | 0→120 | −0.0051 | −0.076 | −0.45 | **+0.26%p** | 0.702 | BH 실패 |
| bucket | 10→20 | −0.0055 | −0.072 | −1.29 | −0.03%p | 0.238 | BH 실패 |
| bucket | 20→40 | −0.0014 | −0.019 | −0.24 | +0.09%p | 0.844 | BH 실패 |
| bucket | 40→60 | **+0.0022** | +0.028 | +0.35 | +0.15%p | 0.764 | BH 실패, 기대 방향 |
| bucket | 60→120 | +0.0006 | +0.008 | +0.06 | +0.06%p | 0.952 | BH 실패, 기대 방향 |

(03 §4.1)

- **IC와 spread가 어긋난다**: 0→40·0→60·0→120에서 IC는 음수, spread는 양수다. 관계가 단조롭지 않다는 뜻이지만 어느 값도
  유의하지 않아(t −0.45 ~ −1.10) 잡음 안의 흔들림과 구분할 수 없다 (03 §4.2).
- 원래 모멘텀과 나란히 놓으면 (03 §4.3): IC(0→120) −0.0326 → −0.0051, BH 통과 4 → 0, 최소 q 0.059 → 0.139.
  시장 요인을 빼자 반대 신호가 거의 사라졌다. **"모멘텀 역방향의 상당 부분이 시장·베타 쪽에서 왔다"는 해석은 아직
  가설이다** — 두 피쳐의 상관도, 표본 차이(2,460 vs 2,207일)의 기여도 재지 않았다.

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 1.131 (broad −0.0100 / tradable −0.0113) — 크기가 작아 대조 의미 제한 | 기준 통과(방향 반대) |
| lag1 유지율 | native −0.0100 / lag1 −0.0096. `delay_pass = null` | 대상 아님 |
| available 표본 부호 | −0.0099 vs −0.0100 (0→20). `attrition_warning = false`. card `survival_bias_unresolved` | 뒤집힘 없음 |
| 기간 일관성 | **2/5** | 불안정 |
| 시간 placebo | p **0.614**, `temporal_null_pass = false` (100번 중 61번이 관측값만큼 극단) | **실패** |
| 비중첩 offset | 20개 전부 유효, 부호 일치율 **0.0**, p 중앙값 0.665, IC 범위 −0.0135 ~ −0.0083 | 실패 |
| source 경고 (Phase B) | 대상 아님 | — |
| 시장 구성 | KOSPI 42.4% / KOSDAQ 57.6% | — |

(03 §5)

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | **2,207일** — 가격 계열 최단(`px_reversal_5d`보다 415일 적다) |
| 날짜당 종목 수 | 1,022~1,025개 — 상장 2년 미만 종목이 통째로 빠진다 |
| coverage_ratio (Phase B) | 대상 아님 |
| KOSPI/KOSDAQ 비중 | 42.4% / 57.6% |

(03 §6)

### 중복성

- A×B 상위: `own_major_stake_change` +0.078, `ev_filing_activity` +0.061, `mcap_krx_log` +0.048, `hc_employee_growth` −0.047.
  전부 0.08 미만. `px_mom_12_1`이 `mcap_krx_log`와 +0.161이었던 것과 대조되며, **시장 요인을 빼면서 규모와의 연결도 끊겼다**
  (03 §7).
- **미확인 중복**: `px_mom_12_1`과의 상관(같은 창), `px_idio_vol_60d`와의 관계(같은 `resid_ret` 공유). A×A 산출물 없음.

### 모델 투입 메모

- **변환**: rank 또는 z. 잔차 합은 로그수익률 척도라 극단이 덜하지만 winsor는 필요하다.
- **결측**: 484거래일 요구로 결측이 가장 많다. `*_isna`가 "상장 2년 미만" 플래그로 작동한다. 이 컬럼을 넣는 것만으로
  신규 상장 여부를 모델에 알려 주는 셈이 된다.
- **유의점**: 통계적으로 신호가 없다. 단독 투입 근거가 없다. 벤치마크가 동일가중이라 시가총액 가중 지수 대비 잔차와는
  다른 값이다 (03 §8).
- **horizon**: 어느 horizon에도 후보 밴드가 없다.
- **조합 후보**: `px_mom_12_1`과 둘 중 하나만 쓰거나 차분(원 모멘텀 − 잔차 모멘텀 = 시장 기여분)을 보는 것이 설계 취지에
  맞지만 검정한 적이 없다. 시가총액 가중 벤치마크 변형, 업종 잔차 변형(업종 코드 없어 불가) 미확인.

### 한계

- BH 통과 0, placebo 실패, offset 일치율 0.0 — 신호가 없다는 것 말고 할 말이 없다 (03 §8).
- 원 모멘텀과의 상관·표본 차이 기여를 분리하지 않았다.
- 벤치마크를 동일가중 하나만 썼다. 업종 요인은 그대로 남아 있다.
- holdout을 열지 않았다.

---

## 4. `px_near_52w_high` — 52주 고점 근접도

| 항목 | 값 |
|---|---|
| primary feature | `px_near_52w_high` |
| secondary / variant | secondary — · `px_near_52w_high_lag1`(lag1) · baseline 40의 `px_dist_52w_high`(`price.py:189`)가 **같은 산식** |
| 마트 / 산식 위치 | `feat_price` / `research/etl/features/price.py:82` (마스킹 `:121`) |
| 원천 raw 테이블 | `daily_ohlcv` (+ `ca_event`) |
| prod 갱신 | 매일 18:30 Cronicle 체인 |
| 분류 좌표 | C1 × **T0**(수준) × U |
| 검증 phase / fdr_family / role | A / price / ready |
| 기대 부호 → 관측 부호 | `+` → `+` |
| 사전등록 primary horizon | [20, 40, 60] · bucket 포함(10→20, 20→40, 40→60) · exploratory [1, 2, 3, 5, 10, 120] |
| 관측 신호 밴드 · 모양 | [20, 60] · pattern **`delayed`** · onset 없음(누적 앞 구간 신호 없음) / peak_h_cum 60 / peak_bucket **[40, 60]** / half-life 없음(관측 범위 60일 끝) (04 §4.4) |
| 등급 / screen_pass / discovery | A / 통과 / **3/6 — 전부 bucket** (cum 세 cell은 discovery 아님) |
| 모델 검증 이력 | **baseline 40에 이미 포함**(`px_dist_52w_high`) → T1 신규 후보에서 제외. 개별 기여도 미측정 (04 §9) |

### 산식과 규칙

```sql
close_d / NULLIF(MAX(close_d) OVER w252, 0) - 1 AS px_near_52w_high
-- w252 = ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
CASE WHEN ca_count_252 > 0 THEN NULL ELSE px_near_52w_high END
```

- 값은 **항상 0 이하**다. 0이면 오늘이 52주 신고가, −0.9면 고점 대비 90% 하락. 기준점이 최고가 하나라 1년 전 하루의
  급등이 이후 1년 내내 분모를 붙잡는다 (04 §2.1~2.2).
- `MAX`는 **부분 창으로도 계산**된다. 그래서 252일 창인데도 표본이 2,622일로 `px_reversal_5d`와 같다. 대신 상장 초기
  종목은 분모가 짧은 기간의 최고가라 값이 0에 가깝게 나오는 편향이 있는데, **이번에 확인하지 않았다** (04 §2.3).
- 마스킹 창은 최근 21일까지 포함하는 `ca_count_252`로 `px_mom_12_1`보다 조금 넓다.
- baseline의 `px_dist_52w_high`는 `legacy` CTE가 `base`(정지 행 포함) 위에서 계산하고 **기업행동 마스킹이 없다**
  (`price.py:161~`, `:189`). 산식은 같지만 값이 완전히 같지는 않다.

### 가용 시점(PIT)과 지연

- 당일 종가 기준 `native_t`.
- native 0.0151 / lag1 **0.0160** (cum 0→60) — **하루 늦추면 오히려 조금 강하다.** 지연형 신호라 실행 여유가 크다 (04 §2.4, §5.5).
- `delay_pass = null` — 지연 게이트는 h ≤ 5 cell에만 적용하는데 이 family의 discovery는 전부 10일 이후 구간이다.

### 예측력 — broad × common_survivor × native_t

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | +0.0010 | 0.007 | 0.11 | −0.34%p | 0.940 | — |
| cum | 0→40 | +0.0067 | 0.053 | 0.56 | −0.37%p | 0.629 | — |
| cum | 0→60 | +0.0151 | 0.128 | 1.15 | −0.16%p | 0.294 | — |
| bucket | 10→20 | +0.0129 | 0.101 | 1.97 | −0.19%p | 0.068 | **discovery** |
| bucket | 20→40 | +0.0226 | 0.191 | 2.69 | −0.19%p | 0.011 | **discovery** |
| bucket | 40→60 | **+0.0312** | 0.282 | 3.97 | **−0.07%p** | **0.00012** | **discovery** |

(04 §4.1)

- **누적은 안 되고 구간만 된다.** 앞 10일에 신호가 없어 cum이 희석된다. `cum 0→60` IC 0.0151(t 1.15)이 `bucket 40→60`
  에서는 0.0312(t 3.97)다. 누적 지표만 봤다면 "신호 없음"으로 분류됐을 family다 (04 §4.2).
- **IC는 양수인데 spread는 여섯 cell 전부 음수다.** 35개 중 어긋남이 가장 큰 사례다. 하위 20%(고점에서 가장 멀리 떨어진
  폭락주)의 수익률 분포가 오른쪽으로 치우쳐 소수의 대박이 평균을 끌어올린다. **어느 쪽이 맞는지 판정할 자료(중앙값
  spread, 분위별 평균수익률)가 없다** (04 §4.3, 00 §9.1). 순위 모델 입력이면 IC, 분위 롱숏이면 spread를 믿어야 한다.

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 0.934 (broad 0.0151 / tradable 0.0141, cum 0→60) | 통과 |
| lag1 유지율 | native 0.0151 / lag1 0.0160 (수치상 손해 없음). `delay_pass = null` | 대상 아님 |
| available 표본 부호 | available **0.0179** vs common 0.0151 — 상장폐지 종목을 넣으면 강해진다. `attrition_warning = false`. card `survival_bias_unresolved` | 통과 |
| 기간 일관성 | **4/5** — 한 구간에서 뒤집혔다(어느 구간인지는 heatmap에서만) | 통과 |
| 시간 placebo | card 값 p **0.772**, `temporal_null_pass = false` — 그러나 이 값은 `cum 0→60`(NW lag 59)의 것이고 그 cell은 discovery가 아니다. discovery인 bucket 10→20·20→40·40→60은 NW lag 9·19·19로 **대상 아님**. 등급 A의 `robustness_required` 관점에서 **discovery cell은 받지 않았고, 긴 구간은 받아서 실패했다** | 대상 아님 / 긴 구간 실패 |
| 비중첩 offset | 60개 전부 유효, 부호 일치율 **1.0**, p 중앙값 0.087, IC 범위 +0.0073 ~ +0.0201 | 통과 |
| source 경고 (Phase B) | 대상 아님 | — |
| 시장 구성 | KOSPI 41.2% / KOSDAQ 58.8% | — |

(04 §5) 시간 placebo 행을 특히 주의해서 읽어야 한다. 이 family의 60일 이상 신호는 시간 placebo와 구분되지 않는다 (04 §5.6).

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | 2,622일 (`available` 2,682~2,722일) |
| 날짜당 종목 수 | 1,096~1,098개 |
| coverage_ratio (Phase B) | 대상 아님 |
| KOSPI/KOSDAQ 비중 | 41.2% / 58.8% |

(04 §6)

### 중복성

- **A×B 교차 상관이 35개 중 가장 큰 축이다** (04 §7):

| 상대 family | ρ | 범위 | 읽기 |
|---|---:|---|---|
| `ev_payout_yield` | **+0.223** | +0.05 ~ +0.35 | 범위가 전부 양수 — 날짜와 무관하게 안정적으로 겹친다 |
| `fin_value_z` | +0.167 | −0.11 ~ +0.40 | 날짜에 따라 뒤집힌다 |
| `mcap_krx_log` | +0.152 | −0.08 ~ +0.45 | |
| `fin_gross_profitability` | +0.149 | −0.05 ~ +0.28 | |
| `ev_amendment_ratio` | **−0.137** | −0.22 ~ −0.01 | 범위가 전부 음수 — 안정적 |
| `own_major_stake_change` | +0.127 | +0.03 ~ +0.51 | |

- `|ρ| ≥ 0.7` 경고 대상은 아니다. 다만 `ev_payout_yield`와 함께 넣으면 정보가 상당 부분 중복될 수 있다.
- 같은 마트: baseline `px_dist_52w_high`와 **사실상 같은 컬럼**이다(위 산식 절). 둘을 같이 넣을 이유가 없다.
- **미확인 중복**: `px_mom_12_1`(252일 경로 공유). A×A 없음.

### 국면 의존

Phase C 사전등록 쌍이 없다. 매크로 수준 동행(참고, 00_survey §4.3): 날짜별 중앙값이 `us_term_spread` **+0.67**,
`kr_term_spread` +0.65, `kospi_ret_60d` +0.52, `vix` −0.40 — 35개 중 매크로와 가장 많이 같이 움직이는 피쳐다. IQR은
`us_kr_10y_diff` +0.61, `m2_yoy` −0.54. 순위 변환이 수준 동행은 전부 지우지만, 횡단면 폭이 국면을 따라 달라진다는 점은
남는다. 국면 조건화 후보로 아직 등록된 적은 없다.

### 모델 투입 메모

- **변환**: `(date, market)` 내 rank. 원값이 [−1, 0]에 갇혀 있고 **신고가 종목이 정확히 0에서 동점**이 되므로 rank의
  동점 처리를 확인해야 한다. 상장 초기 종목의 0 근접 편향(04 §2.3)도 동점 쪽으로 쏠린다.
- **결측**: 부분 창 허용이라 warm-up 결측은 거의 없다. 결측은 252일 창 기업행동 마스킹이 대부분이다.
- **유의점**: **IC 방향과 spread 방향이 반대**다. 순위 손실을 쓰는 랭킹 모델에는 검증 결과가 맞지만, 상·하위 분위
  롱숏으로 평가하면 반대로 나올 수 있다. 하위 분위(폭락주)의 소수 대박이 원인이므로 분위별 평균수익률을 먼저 뽑아야
  한다. 지연형(onset 없음, peak 40~60)이라 lag1 손해가 없고 실행 여유가 크다.
- **horizon**: h=20 cum은 IC +0.0010으로 신호가 없다. h=60 cum도 t 1.15로 비유의. **신호는 20~60일 뒤 구간 라벨에서만
  잡힌다.** h=60 실험에서는 "40일 뒤에 사서 20일 보유" 형태의 구간 라벨(`raw_bucket_label_40_60d`)이 이 피쳐에 맞는다.
  h=120은 exploratory여서 미검정이고, 60일 이상은 placebo 실패다.
- **조합 후보**: 모멘텀 계열과의 관계(A×A) 미확인. `ev_payout_yield`와의 공선성(+0.223, 항상 양수) 처리 필요.
  baseline `px_dist_52w_high`와 중복 투입 금지.

### 한계

- IC와 spread의 부호가 반대인데 이를 풀 자료가 없다. **이 family를 실제로 쓸지 판단하려면 이게 먼저다** (04 §8).
- 60일 이상은 시간 placebo를 통과하지 못한다.
- 누적이 아니라 구간 신호라 "오늘 사서 계속 들고 있는" 전략과 맞지 않는다.
- 상장 초기 편향, 모멘텀 중복, 업종 중립화 미확인. holdout 미개방.

---

## 5. `px_maxret_20d` — 20일 최대 일간수익률 (복권주 지표)

| 항목 | 값 |
|---|---|
| primary feature | `px_maxret_20d` |
| secondary / variant | secondary — · `px_maxret_20d_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_price` / `research/etl/features/price.py:83` (마스킹 `:122`); `simple_ret`은 `trading_panel.py` |
| 원천 raw 테이블 | `daily_ohlcv` (+ `ca_event`) |
| prod 갱신 | 매일 18:30 Cronicle 체인 |
| 분류 좌표 | C1 × T0 × U |
| 검증 phase / fdr_family / role | A / price / ready |
| 기대 부호 → 관측 부호 | `−` → `−` |
| 사전등록 primary horizon | [20, 40, 60] · bucket 포함(10→20, 20→40, 40→60) · exploratory [1, 2, 3, 5, 10, 120] |
| 관측 신호 밴드 · 모양 | [20, 60] · pattern `delayed` · onset **20** / peak_h_cum 60 / peak_bucket [20, 40] / half-life 없음(60일 끝) (05 §4.4) |
| 등급 / screen_pass / discovery | A / 통과 / **6/6** |
| 모델 검증 이력 | T1 후보 5개 묶음 → h20 k=100 비용반영 Δ −0.0045 → 묶음 비채택. 같은 묶음에 `px_idio_vol_60d`가 함께 있었다 (05 §9) |

### 산식과 규칙

```sql
MAX(simple_ret) OVER w20 AS px_maxret_20d      -- simple_ret = close / 직전 유효세션 close − 1
-- w20 = ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
CASE WHEN ca_count_20 > 0 THEN NULL ELSE px_maxret_20d END
```

- 가격 계열 중 유일하게 **단순수익률**을 쓴다. 30% 상승이면 단순 0.30, 로그 0.262 — 최댓값 지표라 이 차이가 그대로
  값에 들어간다 (05 §2.2).
- **상하한가에 값이 뭉친다.** 가격제한폭이 2015-06-14까지 0.15, 이후 0.30이다(`quality.price_limit_regimes`). 상한가를 친
  종목은 한 점에 모여 대량 동점이 되고, 2015년 6월을 경계로 척도가 바뀐다. Spearman은 평균 순위로 동점을 처리하지만
  분해능이 떨어진다 (05 §2.3).
- 20일 창 기업행동 마스킹. 무상증자·액면분할이 만든 점프를 "크게 튄 하루"로 오인하지 않으려는 장치라 이 피쳐에서
  특히 중요하다 (05 §2.4).

### 가용 시점(PIT)과 지연

- 당일 종가 기준 `native_t`. native −0.0896 / lag1 −0.0880 (cum 0→20). 하루 지연 손실 1.8%. **실행 여유가 크다** (05 §5.6).
- `delay_pass = null` — discovery가 전부 10일 이후 구간이라 지연 게이트 대상 아님.

### 예측력 — broad × common_survivor × native_t

기대 부호가 `−`라 spread는 정렬값(`q5_spread_aligned`)이다. 양수면 기대대로다.

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | −0.0896 | −0.930 | −13.64 | +0.44%p | ~0 | discovery |
| cum | 0→40 | −0.1032 | **−1.121** | −11.90 | +1.16%p | ~0 | discovery |
| cum | 0→60 | **−0.1133** | **−1.309** | −11.15 | **+1.79%p** | ~0 | discovery |
| bucket | 10→20 | −0.0643 | −0.640 | −13.11 | +0.21%p | ~0 | discovery |
| bucket | 20→40 | −0.0710 | −0.709 | −10.53 | +0.56%p | ~0 | discovery |
| bucket | 40→60 | −0.0623 | −0.666 | −9.98 | +0.40%p | ~0 | discovery |

(05 §4.1) family 최소 q: Phase A 1.83e-41, AB 3.73e-41 — 35개 중 세 번째로 작다.

- IC와 spread의 방향은 여섯 cell 전부 일치한다. 어긋남 없음.
- |ICIR|이 1을 넘는다(0→40 1.121, 0→60 1.309). 35개 중 몇 안 되는 경우다 (05 §4.2).
- `px_reversal_5d`와 비교하면 대표 보유기간 60일 vs 5일, spread +1.79%p vs +0.38%p, 대략 연 4회 vs 50회 리밸런싱이다.
  **회전이 12분의 1인데 회당 수익은 5배**다 (05 §4.3).

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | **1.019** (broad −0.0896 / tradable −0.0913, cum 0→20; 0→60은 −0.1133 vs −0.1135) — 유동성 좋은 종목에서 오히려 강함 | 통과 |
| lag1 유지율 | native −0.0896 / lag1 −0.0880. `delay_pass = null` | 대상 아님 |
| available 표본 부호 | available **−0.0931** vs common −0.0896 — 상장폐지 포함 시 강해짐. `attrition_warning = false`. card `survival_bias_unresolved` | 통과 |
| 기간 일관성 | **5/5** | 통과 |
| 시간 placebo | p **0.0099**, `temporal_null_pass = true` — 100번 중 관측값만큼 극단적인 것이 없었다(가능한 최솟값). cum 0→60(NW lag 59)이 대상. **`robustness_required` 대상이었고 실제로 받아서 통과했다** | **통과** |
| 비중첩 offset | 20개 전부 유효, 부호 일치율 **1.0**, p 중앙값 **5.2e-14**, p 최댓값 1.1e-10, IC 범위 −0.094 ~ −0.085 | 통과 |
| source 경고 (Phase B) | 대상 아님 | — |
| 시장 구성 | KOSPI 41.2% / KOSDAQ 58.8% | — |

(05 §5) 35개 중 강건성 검사를 가장 깨끗하게 통과한 피쳐다.

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | 2,622일 (`available` 2,682~2,722일) |
| 날짜당 종목 수 | 1,096~1,098개 |
| coverage_ratio (Phase B) | 대상 아님 |
| KOSPI/KOSDAQ 비중 | 41.2% / 58.8% |

(05 §6)

### 중복성

- A×B 상위 (05 §7): `fin_value_z` **−0.251**(범위 −0.44 ~ −0.06, 전부 음수), `ev_payout_yield` **−0.241**(−0.37 ~ −0.09,
  전부 음수), `fin_log_mcap` +0.118, `own_major_filing_activity` +0.103. 크게 튀는 종목은 밸류가 비싸고 주주환원이 적다.
  두 관계 모두 방향이 안정적이라 함께 넣을 때 증분이 줄 수 있다. `|ρ| ≥ 0.7` 대상은 아니다.
- **미확인 중복 — 이 family의 가장 큰 공백**: `px_idio_vol_60d`와 "크게 흔들리는 종목"이라는 **같은 경제적 축**을 잰다.
  onset 20·peak_bucket [20, 40]·pattern `delayed`·기간 5/5·placebo 0.0099·제3 피쳐와의 상관 부호까지 같다. 사전 설계
  (`02_feature_candidate` §3.1 P5)가 ablation을 명시적으로 요구했는데 **A×A 상관을 재지 않았다** (05 §7, 06 §7, 00 §9.3).

### 국면 의존 (Phase C)

| 쌍 | 국면 | cell | 기대 δ | δ̂ | t_nw | q | placebo p | G2 / G3 / G4 | 판정 |
|---|---|---|---|---:|---:|---:|---|---|---|
| P2 | `vix_up` (VIX_t − VIX_{t−20} > 0) | cum 0→60 | `+` (IC 음수 → VIX 상승 뒤 약해짐) | +0.0071 | 0.84 | 0.5005 | > 0.10 | ✗ / ✗ / ✗ | D |

(run 보고서 `03c_conditional_ic_results.md`; 05_results §2·§3)

- 방향은 사전등록과 맞지만 유의하지 않고 G2·G3·G4를 모두 통과하지 못했다. Kim-Park-Ok(2019)의 ΔVIX 의존은 이 표본에서
  재현되지 않았다. 같은 family를 `vix_high`에 건 쌍은 exploratory라 보고만 하고 판정하지 않았다.
- 매크로 수준 동행(참고): 중앙값이 `vix`와 **+0.46**. 복권형 수익률의 빈도는 VIX가 높을 때 늘어난다 (00_survey §4.3).

### 모델 투입 메모

- **변환**: `(date, market)` 내 rank. **상한가 동점**이 대량이고 2015-06-15에 상한이 0.15→0.30으로 바뀌므로 원값 z-score는
  날짜 간 척도가 다르다. 날짜 내 rank가 두 문제를 모두 흡수한다. 원값을 쓰려면 제도 변경 전후를 나눠야 한다.
- **결측**: 20일 창 마스킹만. 결측 적음.
- **유의점**: 회전이 느리고(60일 대표) lag1 손실 1.8%라 실행 부담이 작다. tradable에서 오히려 강하다(1.019) — "동전주
  효과"가 아니다. 다만 `fin_value_z`·`ev_payout_yield`와 −0.25 수준으로 겹친다.
- **horizon**: onset 20이라 **h=20·h=60 실험에 맞는다.** h=5 실험에서는 exploratory(1~10일)여서 미검정이다. h=120은
  exploratory. `half_life`가 없는 것은 관측 범위가 60일에서 끝나기 때문이다.
- **조합 후보**: `px_idio_vol_60d`와의 ablation(둘 중 하나 또는 증분 확인) — 가장 시급하다. `px_limit_up_count_20d`를
  만들어 "상한가를 쳤다" 한 가지로 신호가 얼마나 설명되는지 비교(P5 사전 지시, 미실행). `fin_value_z`·`ev_payout_yield`
  통제. 2015-06 제도 변경 전후 분할 검정.

### 한계

- IVOL과의 중복이 미확인 — 사전 설계가 요구한 ablation이다 (05 §8).
- 상한가 뭉침 문제와 2015년 6월 제도 변경 전후를 나눠 보지 않았다.
- 밸류·주주환원 통제 후에도 남는지 모른다. 거래비용 반영 성과가 없다(회전은 느리다).
- 60일 이후 미검정. holdout 미개방.

---

## 6. `px_idio_vol_60d` — 60일 고유변동성 (IVOL)

| 항목 | 값 |
|---|---|
| primary feature | `px_idio_vol_60d` |
| secondary / variant | secondary — · `px_idio_vol_60d_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_price` / `research/etl/features/price.py:84` (마스킹 `:123`); 잔차는 `trading_panel.py` `build_market_model_sql` |
| 원천 raw 테이블 | `daily_ohlcv` (+ `ca_event`) |
| prod 갱신 | 매일 18:30 Cronicle 체인 |
| 분류 좌표 | C1 × T0 × U |
| 검증 phase / fdr_family / role | A / price / ready |
| 기대 부호 → 관측 부호 | `−` → `−` (이론은 `+`, 실증 IVOL 퍼즐을 따라 `−`로 사전등록) |
| 사전등록 primary horizon | [20, 40, 60] · bucket 포함 · exploratory [1, 2, 3, 5, 10, 120] |
| 관측 신호 밴드 · 모양 | [20, 60] · pattern `delayed` · onset **20** / peak_h_cum 60 / peak_bucket [20, 40] / half-life 없음 (06 §4.4) — `px_maxret_20d`와 전부 같다 |
| 등급 / screen_pass / discovery | A / 통과 / **6/6** |
| 모델 검증 이력 | T1 후보 5개 묶음 → h20 k=100 Δ −0.0045 → 묶음 비채택. 사전 설계는 "독립 alpha가 아닌 B등급 risk/quality feature"로 예상했다 (06 §3.4, §9) |

### 산식과 규칙

```sql
CASE WHEN COUNT(resid_ret) OVER w126 >= 126
     THEN STDDEV_SAMP(resid_ret) OVER w60 END AS px_idio_vol_60d
-- w60 = 59 PRECEDING AND CURRENT ROW, w126 = 125 PRECEDING AND CURRENT ROW
-- resid_ret = log_ret − (alpha_252 + beta_252 × market_ret), 252세션 완전 창에서만 (build_market_model_sql)
CASE WHEN ca_count_60 > 0 THEN NULL ELSE px_idio_vol_60d END
```

- 시장모형 잔차의 최근 60거래일 표준편차. 베타 위험과 고유 위험을 분리한다 — 라벨이 시장 초과수익률이라 이 분리가 라벨
  축과 맞는다 (06 §2.2).
- 세 겹 조건(회귀 252 + 잔차 126 완전 + 표준편차 60) → **연속 378거래일(약 1년 반)** 이력 필요. config `market_model_window: 252`,
  `idio_model_min_valid: 126`, `idio_vol_window: 60` (06 §2.3).
- `px_resid_mom_12_1`과 **같은 `resid_ret`**을 쓴다.

### 가용 시점(PIT)과 지연

- 당일 종가 기준 `native_t`. native −0.1103 / lag1 −0.1084 (cum 0→20). 손실 1.7% (06 §2.5).
- `delay_pass = null` — discovery가 전부 10일 이후 구간.

### 예측력 — broad × common_survivor × native_t

spread는 정렬값이다.

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | −0.1103 | −1.020 | −14.43 | +0.94%p | ~0 | discovery |
| cum | 0→40 | −0.1304 | −1.376 | −14.06 | +1.98%p | ~0 | discovery |
| cum | 0→60 | **−0.1433** | **−1.642** | −13.25 | **+2.99%p** | ~0 | discovery |
| bucket | 10→20 | −0.0811 | −0.718 | −14.00 | +0.41%p | ~0 | discovery |
| bucket | 20→40 | −0.0916 | −0.863 | −12.22 | +0.87%p | ~0 | discovery |
| bucket | 40→60 | −0.0805 | −0.793 | −11.26 | +0.73%p | ~0 | discovery |

(06 §4.1) family 최소 q: Phase A 4.38e-46, AB 8.94e-46 — `px_reversal_5d` 다음으로 작다. **|IC| 0.1433은 35개 전체 최댓값**,
|ICIR| 1.642도 최대다.

- IC와 spread 방향은 전부 일치한다.
- **|IC| 1위인데 spread 1위는 아니다.** 60일 +2.99%p는 `px_amihud_20d` 120일 +11.21%p의 4분의 1이고, 같은 60일로 맞춰도
  `px_amihud_20d` 0→60은 IC +0.1085 / spread +6.06%p로 두 배 크다. IC는 horizon과 무관하게 비교할 수 있지만 spread는
  같은 horizon에서만 비교할 수 있다 (06 §4.3).

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | **1.042** (broad −0.1103 / tradable −0.1149, cum 0→20; 0→60은 −0.1433 vs −0.1493) — 유동성 좋은 종목에서 강함 | 통과 |
| lag1 유지율 | native −0.1103 / lag1 −0.1084. `delay_pass = null` | 대상 아님 |
| available 표본 부호 | available **−0.1145** vs common −0.1103 — 강해짐. `attrition_warning = false`. card `survival_bias_unresolved` | 통과 |
| 기간 일관성 | **5/5** | 통과 |
| 시간 placebo | p **0.0099**, `temporal_null_pass = true` (cum 0→60, NW lag 59). **`robustness_required` 대상이었고 받아서 통과했다** | **통과** |
| 비중첩 offset | 20개 전부 유효, 부호 일치율 **1.0**, p 중앙값 **2.0e-16**, p 최댓값 4.4e-11, IC 범위 −0.114 ~ −0.106 | 통과 |
| source 경고 (Phase B) | 대상 아님 | — |
| 시장 구성 | KOSPI 42.1% / KOSDAQ 57.9% | — |

(06 §5)

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | **2,334일** (`available` 2,394~2,434일) — `px_maxret_20d`보다 288일 짧다 |
| 날짜당 종목 수 | 1,042개 — `px_maxret_20d`보다 55개 적다 |
| coverage_ratio (Phase B) | 대상 아님 |
| KOSPI/KOSDAQ 비중 | 42.1% / 57.9% |

(06 §6) MAX와 비교할 때 **같은 종목·같은 날짜를 보고 있는 게 아니라는 점**을 빼놓으면 안 된다.

### 중복성

- **204개 A×B 쌍 중 절대값 1·2위가 이 family에 있다** (06 §7): `fin_value_z` **−0.351**(범위 −0.49 ~ +0.02),
  `ev_payout_yield` **−0.347**(−0.43 ~ −0.23, 폭이 좁고 전부 음수), `own_major_filing_activity` +0.155,
  `own_major_stake_level` −0.141, `ev_amendment_ratio` +0.140. `|ρ| ≥ 0.7` 대상은 아니지만 방향이 안정적이라 증분 기여가
  줄 수 있다.
- **미확인 중복 — 이 문서 전체에서 가장 중요한 공백**: `px_maxret_20d`와의 A×A 상관. 재는 방식(60일 잔차 표준편차 vs
  20일 최대 하루 수익률)만 다르고 기대 부호·horizon·신호 모양·기간 일관성·placebo·제3 피쳐 상관 부호가 전부 같다.
  `02_feature_candidate` §3.1 P4("IVOL 부호는 MAX 통제에 민감")와 `09_all_feature_results` §4("둘 다 쓰면 중복")가 이미
  지적했다. 상관을 재려면 공통 표본으로 맞춰야 한다(2,334 vs 2,622일).
- 같은 마트: `px_resid_mom_12_1`과 `resid_ret` 공유. `feat_macro_exposure`의 `macro_beta_*`도 같은 잔차다.

### 국면 의존 (Phase C)

| 쌍 | 국면 | cell | 기대 δ | δ̂ | t_nw | q | placebo p | G2 / G3 / G4 | 판정 |
|---|---|---|---|---:|---:|---:|---:|---|---|
| P1 | `vix_up` | cum 0→60 | `+` | **+0.0115** | 1.23 | 0.3481 | **0.1089** | ✗ / ✅ / ✗ | D |

(run 보고서 `03c_conditional_ic_results.md`; 05_results §2)

- **아깝게 떨어졌다.** δ̂ 부호는 사전등록과 맞지만 t 1.23으로 BH를 못 넘고, placebo p 0.1089로 G4 임계 0.10도 아슬아슬하게
  넘겼다. 05_results §2가 "다음 사전등록의 후보"로 적어 뒀다. **이번 판정은 실패다.** 결과를 보고 국면 cut을 옮기지
  않는다(05_results §7).
- 매크로 수준 동행(참고): 중앙값 `kr_term_spread` +0.52, `cpi_yoy` −0.46, `vix` +0.31; IQR `kospi_turnover_log` **+0.52**,
  `vix` +0.37 — 거래가 활발할수록 고유변동성이 종목 간에 더 벌어진다 (00_survey §4.3).

### 모델 투입 메모

- **변환**: `(date, market)` 내 rank. 원값은 양수·우측 꼬리인 표준편차라 log 후 z가 자연스럽다.
- **결측**: 378거래일 warm-up → 상장 1년 반 미만 NULL. `*_isna`가 상장 연차 정보를 담는다. 60일 창 마스킹.
- **유의점**: 단변량 |IC| 1위가 곧 모델 기여 1위는 아니다. T1 묶음에서 `px_maxret_20d`와 함께 들어갔는데 묶음 증분이
  기대만큼 나오지 않았을 가능성을 이번 설계로는 확인할 수 없다 (06 §9). `fin_value_z`·`ev_payout_yield`와 −0.35다.
  tradable에서 강해 "미세소형주 효과"가 아니다.
- **horizon**: onset 20 — **h=20·h=60 실험에 맞는다.** h=5는 exploratory(미검정), h=120은 exploratory.
- **조합 후보**: `px_maxret_20d` ablation(둘 중 하나). 베타 통제 — `px_market_beta`가 `feat_macro_exposure`에 생겼으므로
  이제 가능하다([06_macro_regime.md](06_macro_regime.md)). `vix_up` 국면 P1은 다음 사전등록 후보. 사전 설계가 말한
  "risk/quality feature" 역할(alpha가 아닌 위험 통제 변수)로 쓰는 실험은 없었다.

### 한계

- `px_maxret_20d`와의 중복 미확인 — 가장 시급하다 (06 §8).
- |IC| 1위를 성과 1위로 읽으면 안 된다. 밸류·환원·베타 통제 후에도 남는지 모른다.
- 표본이 다른 가격 피쳐와 다르다(2,334일). 업종별 고유변동성 수준 차이를 시장 두 그룹 안에서만 순위 매겼다.
- 60일 이후 미검정. holdout 미개방.

---

## 7. `px_amihud_20d` — Amihud 비유동성

| 항목 | 값 |
|---|---|
| primary feature | `px_amihud_20d` |
| secondary / variant | secondary — · `px_amihud_20d_lag1`(lag1, `price.py:203`) |
| 마트 / 산식 위치 | `feat_price` / `research/etl/features/price.py:205~209` — `variants` CTE 뒤 별도 서브쿼리, **기업행동 마스킹 없음** |
| 원천 raw 테이블 | `daily_ohlcv` (`turnover = close × volume` 근사) |
| prod 갱신 | 매일 18:30 Cronicle 체인 |
| 분류 좌표 | **C1 / C8**(실행·거래비용) × T0 × U — 두 카테고리에 걸친 유일한 축 |
| 검증 phase / fdr_family / role | A / price / ready · `sparse_primary_grid: true` |
| 기대 부호 → 관측 부호 | `+` → `+` |
| 사전등록 primary horizon | **[60, 120]** · bucket 포함(40→60, 60→120) · exploratory [1, 2, 3, 5, 10, 20, 40] — 35개 중 격자가 가장 성기다 |
| 관측 신호 밴드 · 모양 | [60, 120] · pattern `delayed` · onset **60**(35개 중 가장 늦다) / peak_h_cum 120 / peak_bucket [60, 120] / half-life 없음 — **peak가 관측 범위 끝에 있다** (07 §4.4) |
| 등급 / screen_pass / discovery | A / 통과 / **4/4** |
| 모델 검증 이력 | **baseline 40에 같은 이름으로 이미 포함** → T1 신규 후보에서 제외. 개별 기여도 미측정. T2 묶음에는 `fin_log_mcap`·`mcap_krx_log`가 둘 다 들어 있다 (07 §9) |

### 산식과 규칙

```sql
AVG(ABS(ret_1d) / NULLIF(turnover, 0)) OVER (
    PARTITION BY ticker, market ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
) AS px_amihud_20d
-- turnover = CAST(close AS DOUBLE) * CAST(volume AS DOUBLE)   (trading_panel.py)
```

- "거래대금 1원당 가격이 얼마나 움직이나". 값이 클수록 비유동적이다. Amihud(2002) 원 정의 (07 §2.1).
- **거래대금이 근사값이다.** 종가 × 거래량이라 장중 변동이 크면 실제 체결대금과 벌어진다. 변동성이 큰 종목일수록
  분자·분모가 같은 방향으로 틀어질 수 있는데 편향 크기를 확인하지 않았다. `daily_market_cap`에 KRX Open API의 실제
  거래대금이 있지만 쓰지 않았다 (07 §2.2).
- **NULL 규칙이 다른 가격 피쳐와 다르다** (07 §2.3): `volume = 0`인 날은 NULL이 되어 `AVG`에서 빠진다 → 20일 중 3일만
  거래됐어도 그 3일 평균으로 값이 나온다. **기업행동 마스킹이 없다** → 액면분할·무상증자가 만든 큰 수익률이 분자에 그대로
  들어간다.
- 단위는 |수익률| / 원. 규모에 따라 수십 배 차이가 나는 매우 작은 수다.

### 가용 시점(PIT)과 지연

- 당일 종가 기준 `native_t`. native 0.1085 / lag1 0.1077 (cum 0→60). 20일 평균이라 하루 차이가 거의 없다 (07 §2.5).
- `delay_pass = null` — discovery가 전부 60일 이후 구간.

### 예측력 — broad × common_survivor × native_t

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | +0.1085 | 1.105 | 8.97 | **+6.06%p** | ~0 | discovery |
| cum | 0→120 | **+0.1343** | **1.334** | 7.88 | **+11.21%p** | ~0 | discovery |
| bucket | 40→60 | +0.0504 | 0.515 | 7.37 | +1.91%p | ~0 | discovery |
| bucket | 60→120 | +0.0761 | 0.776 | 6.42 | +5.10%p | ~0 | discovery |

(07 §4.1) family 최소 q: Phase A 1.14e-18, AB 1.77e-18.

- **spread +11.21%p는 35개 중 최대**다. |IC|는 `px_idio_vol_60d`(0.1433)보다 작은데 spread는 3.75배다. 60일로 맞춰도
  두 배(+6.06 vs +2.99%p) — horizon 효과만으로 설명되지 않는다. 비유동 종목은 수익률 분산 자체가 커서 같은 순위 정확도로도
  벌어지는 폭이 크다 (07 §4.2).
- **+11.21%p를 실현 가능한 수익으로 읽으면 안 된다.** 이 신호가 고르는 종목은 정의상 거래비용이 가장 비싸고, 매수 자체가
  가격을 밀어 올린다. 시장충격은 spread에 반영되지 않았다 (07 §4.3).
- primary cell이 4개뿐이다. 4/4 통과를 8개 중 6개 통과와 같은 무게로 읽으면 안 된다 (07 §3.3).

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | **0.852** (broad 0.1085 / tradable 0.0925, cum 0→60) — **A등급 중 가장 낮다.** 비유동성을 재는 지표에 유동성 필터를 걸면 줄어드는 것이 정상이다. 결함이 아니라 성격이다 | 통과(기준 0.50) |
| lag1 유지율 | native 0.1085 / lag1 0.1077. `delay_pass = null` | 대상 아님 |
| available 표본 부호 | 0.1081 vs 0.1085. `attrition_warning = false`. 다만 card `survival_bias_unresolved` — `available` 표본 자체가 상장폐지 이력을 충분히 담지 못하므로 **생존편향이 없다고 결론 내리면 안 된다** | 뒤집힘 없음 |
| 기간 일관성 | **5/5** | 통과 |
| 시간 placebo | p **0.0099**, `temporal_null_pass = true`. **`robustness_required` 대상이었고 받아서 통과했다** | **통과** |
| 비중첩 offset | 60개 전부 유효, 부호 일치율 **1.0**, p 중앙값 7.0e-08, p 최댓값 2.6e-06, IC 범위 +0.100 ~ +0.116 | 통과 |
| source 경고 (Phase B) | 대상 아님 | — |
| 시장 구성 | KOSPI 41.3% / KOSDAQ 58.7% | — |

(07 §5)

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | 2,622일 — 가격 계열 최장. **마스킹이 없어서 길다.** 기업행동이 낀 값이 그대로 들어 있다 |
| 날짜당 종목 수 | 1,091~1,094개 |
| coverage_ratio (Phase B) | 대상 아님 |
| KOSPI/KOSDAQ 비중 | 41.3% / 58.7% |

(07 §6)

### 중복성

- **이번 검증에서 `|ρ| ≥ 0.7`인 상관 쌍은 204쌍 중 딱 둘이고, 둘 다 이 family다** (07 §7, 00 §9.4):

| 쌍 | ρ | 범위 |
|---|---:|---|
| × `mcap_krx_log` | **−0.754** | −0.85 ~ −0.63 |
| × `fin_log_mcap` | **−0.721** | −0.79 ~ −0.55 |

- 범위가 관측 기간 전체에서 한 번도 −0.6 위로 올라오지 않았다. 구조적 관계다. 분모(거래대금)가 규모를 그대로 반영하기
  때문이다. **비유동성 프리미엄과 소형주 효과를 구분하지 못한다** — 규모를 통제한 증분 IC가 없다. spread 상위 셋
  (+11.83 / +11.21 / +11.19%p)이 전부 이 규모 축이다 (00 §9.4).
- 다른 상관: `own_major_filing_activity` −0.226, `fin_asset_growth_yoy` −0.224, `fin_value_z` +0.203, `fin_gross_profitability`
  −0.161. 전부 규모를 경유했을 수 있다.
- **미확인 중복**: `px_zero_ret_ratio_20d`(같은 C8, `volume = 0`인 날의 여집합을 본다), `px_turnover_shock`(같은 `turnover`).

### 국면 의존 (Phase C)

| 쌍 | 국면 | cell | 기대 δ | δ̂ | t_nw | q | placebo p | G2 / G3 / G4 | 판정 |
|---|---|---|---|---:|---:|---:|---|---|---|
| P6 | `liq_high` | cum 0→60 | `−` (시장 유동성 낮을 때 프리미엄 커짐) | +0.0078 | 0.34 | 0.7835 | > 0.10 | ✅ / ✅ / ✗ | D |

(run 보고서 `03c_conditional_ic_results.md`; 05_results §2·§3)

- δ̂이 0에 가깝고 부호도 사전등록과 반대다. 시장 유동성 국면이 비유동성 프리미엄의 크기를 바꾼다는 Amihud(2002)의
  시계열 함의는 이 표본의 횡단면 IC에서 재현되지 않았다. `px_amihud_20d × vix_up`은 exploratory라 보고만 했다.
- 매크로 수준 동행(참고): 중앙값 `kospi_turnover_log` **−0.46**, `wti` −0.45, `kospi_ret_60d` −0.41, `m2_yoy` −0.41;
  IQR `kospi_turnover_log` −0.52. 비유동성 수준은 시장 거래대금의 함수다 (00_survey §4.3).

### 모델 투입 메모

- **변환**: **log 후 rank 또는 z.** 원값은 분모(거래대금)가 0에 가까운 종목에서 폭주하고 규모에 따라 수십 배 차이 난다.
  winsor 없이 원값을 쓰면 안 된다. 날짜 내 rank가 척도 문제를 흡수한다.
- **결측**: `volume = 0`인 날이 평균에서 빠지므로 20일 전부 거래가 없어야만 NULL이다. 즉 결측이 거의 없는 대신
  **거래일이 적은 종목이 적은 표본으로 높은 값을 갖는다**. 기업행동 마스킹이 없어 액면분할 왜곡 값이 그대로 들어간다 —
  `dim_price_quality_daily`의 `ca_event`로 모델 전처리에서 따로 마스킹하는 것을 고려해야 한다.
- **유의점**: `fin_log_mcap`·`mcap_krx_log`와 −0.72~−0.75다. 규모 피쳐와 함께 넣으면 공선성이 생기고, 둘 중 어느 쪽이
  기여했는지 트리 모델의 중요도로도 갈라내기 어렵다. 유동성 편중(tradable 0.852) — 신호의 15%가 tradable 밖에서 나온다.
  거래비용·시장충격이 가장 비싼 종목을 고른다. C8(실행 필터) 역할과 alpha 역할이 한 값에 섞여 있다 (07 §2.4).
- **horizon**: onset 60 — **h=60·h=120 실험에 맞는다.** peak가 120일(관측 범위 끝)이라 더 긴 horizon에서 더 강할 수 있는데
  보지 않았다. h=5·h=20은 exploratory(미검정).
- **조합 후보**: 규모(`fin_log_mcap`)를 통제한 증분 IC — 이 피쳐를 독립 신호로 부르려면 반드시 필요하다. 실제 거래대금
  (`daily_market_cap`) 기반 변형. `px_zero_ret_ratio_20d`와의 관계. 거래비용 모형 입력으로서의 역할(alpha 아닌 필터) 실험.

### 한계

- 규모와의 중복이 미해결이다(`|ρ| = 0.75`, 이번 검증에서 유일하게 경고 기준을 넘었다) (07 §8).
- 거래대금이 근사값이고 기업행동 마스킹이 없다.
- 거래비용·시장충격 반영 성과가 없다 — 하필 거래비용이 가장 비싼 종목을 고르는 신호다.
- primary cell 4개뿐(`sparse_primary_grid`). 120일 이후 미검정. 20일 창이 다 차지 않아도 값이 나온다.
- holdout 미개방.

---

## 8. `px_turnover_shock` — 회전율 충격 (거래량 급증)

| 항목 | 값 |
|---|---|
| primary feature | `px_turnover_shock` |
| secondary / variant | secondary — · `px_turnover_shock_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_price` / `research/etl/features/price.py:86` (마스킹 `:124`) |
| 원천 raw 테이블 | `daily_ohlcv` (`turnover = close × volume` 근사, + `ca_event`) |
| prod 갱신 | 매일 18:30 Cronicle 체인 |
| 분류 좌표 | C1 × **T2**(놀라움) × U — 35개 중 T2는 셋뿐 |
| 검증 phase / fdr_family / role | A / price / ready |
| 기대 부호 → 관측 부호 | `+` → **`−`** |
| 사전등록 primary horizon | **[5, 10, 20]** · bucket 포함(0→5, 5→10, 10→20) · exploratory [1, 2, 3, 40, 60, 120] — 가격 계열 중 가장 짧다 |
| 관측 신호 밴드 · 모양 | **없음**(`candidate_horizon_band = null`) · pattern `no_signal` · peak_h_cum 20(음수 방향) · peak_bucket [5, 10] · sign_flip 없음 (08 §4.3) |
| 등급 / screen_pass / discovery | D / 실패 / 0/6 (BH는 **6/6 통과**, `expected_sign_pass = false`) |
| 모델 검증 이력 | T1·T2 후보 아님. 다만 "신호가 없어서 D"가 아니라 "사전등록 방향과 반대여서 D"다 (08 §9) |

### 산식과 규칙

```sql
LN(NULLIF(turnover, 0) / NULLIF(
    QUANTILE_CONT(turnover, 0.5) OVER (
        PARTITION BY ticker, market ORDER BY trade_date ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
    ), 0)) AS px_turnover_shock
CASE WHEN ca_count_60_prior > 0 THEN NULL ELSE px_turnover_shock END   -- 마스킹 창 = 분모 창
```

- 오늘 거래대금 / 직전 60거래일 **중앙값**의 로그. 0이면 평소, 0.69(= ln 2)면 두 배, −0.69면 절반 (08 §2.1).
- 세 가지 설계 선택 (08 §2.2): **수준이 아니라 변화**(raw turnover level은 규모와 섞여 "단독 입력 금지", P7),
  **평균이 아니라 중앙값**(과거 한 번의 폭발이 분모를 끌어올리는 것을 막음), **당일을 분모에서 제외**
  (`turnover_shock_include_current: false`).
- 거래대금 근사 오차는 같은 종목의 과거와 비교하는 비율이라 상당 부분 상쇄된다 (08 §2.3).
- `volume = 0`인 날은 `NULLIF`로 NULL. 분모가 중앙값이라 창이 다 차지 않아도 계산돼 표본이 최장(2,622일)이다 (08 §6).

### 가용 시점(PIT)과 지연

- 당일 종가·거래량 기준 `native_t`.
- native −0.0264 / lag1 **−0.0298** (cum 0→20). **`delay_pass = true`** — primary에 h ≤ 5 cell(cum 0→5, bucket 0→5)이 있어
  지연 게이트가 실제로 적용됐고 통과했다. lag1이 오히려 조금 더 강하다 (08 §2.5, §5.5). A등급 가격 피쳐들이 전부 `null`
  (대상 아님)이었던 것과 다르다.

### 예측력 — broad × common_survivor × native_t

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→5 | −0.0185 | −0.264 | −7.79 | **+0.23%p** | ~0 | BH 통과, 부호 반대 → discovery 아님 |
| cum | 0→10 | −0.0199 | −0.297 | −6.51 | **+0.26%p** | ~0 | BH 통과, discovery 아님 |
| cum | 0→20 | **−0.0264** | −0.416 | −6.74 | **+0.13%p** | ~0 | BH 통과, discovery 아님 |
| bucket | 0→5 | −0.0185 | −0.264 | −7.79 | +0.23%p | ~0 | BH 통과, discovery 아님 |
| bucket | 5→10 | −0.0163 | −0.260 | −7.75 | +0.04%p | ~0 | BH 통과, discovery 아님 |
| bucket | 10→20 | −0.0216 | −0.369 | −8.17 | **−0.12%p** | ~0 | BH 통과, discovery 아님 |

(08 §4.1) family 최소 q: Phase A **3.18e-11** — D등급 중 압도적으로 작다. t값도 −6.5 ~ −8.2다.

- **IC와 spread의 부호가 어긋난다.** 여섯 cell 중 다섯에서 IC 음수·spread 양수. `px_near_52w_high`와 같은 구조인데 방향이
  반대다. 상위 20%(거래 폭발 종목)는 대부분 이후 부진하지만(IC −) 대형 호재로 폭발한 소수가 평균을 끌어올린다(spread +).
  풀 자료(중앙값 spread, 분위별 평균)가 없다 (08 §4.2). 순위 모델 입력이면 IC(음수)를 믿는다.

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | **1.268** (broad −0.0264 / tradable −0.0335) — **35개 중 최고.** 거래 활발한 종목에서 반대 신호가 27% 더 강하다 | 기준 통과(방향 반대) |
| lag1 유지율 | native −0.0264 / lag1 −0.0298, **`delay_pass = true`** | 통과(실제로 받았다) |
| available 표본 부호 | −0.0271 vs −0.0264. `attrition_warning = false`. card `limitations` **비어 있음** — 가격 계열 중 드물게 `survival_bias_unresolved`가 없다 | 뒤집힘 없음 |
| 기간 일관성 | **0/5** = 기대 방향 일치 0 = **반대 방향 5/5 완벽 일관.** 0과 5는 둘 다 "일관적"이고 2~3이 "불안정"이다 (08 §5.1) | 반대 방향 안정 |
| 시간 placebo | **대상 아님** — 사전등록 최대 horizon 20이라 NW lag 최대 19 < 59. 떨어진 것이 아니다 | — |
| 비중첩 offset | 20개 전부 유효, 기대 방향 부호 일치율 **0.0**, p 중앙값 **0.9996**, p 최솟값 0.973, IC 범위 −0.034 ~ −0.020. 부호검정이 기대 방향 기준 단측이라 **p가 1에 붙어 있다 = 반대 방향으로 강하게 유의** | 반대 방향 안정 |
| source 경고 (Phase B) | 대상 아님 | — |
| 시장 구성 | KOSPI 41.4% / KOSDAQ 58.6% | — |

(08 §5) `px_mom_12_1`(2/5, p 중앙값 0.868, "방향 불분명")과 성격이 다르다. 00 §9.6이 "반대 방향이 안정적인 유형"으로
따로 분류했고 **새 config로 재등록할 가치가 있다**고 적었다.

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | 2,622일 |
| 날짜당 종목 수 | 1,090개 |
| coverage_ratio (Phase B) | 대상 아님 |
| KOSPI/KOSDAQ 비중 | 41.4% / 58.6% |

(08 §6)

### 중복성

- A×B 상위: `mcap_krx_log` +0.048(범위 −0.34 ~ +0.32), `fin_log_mcap` +0.038, `ev_filing_activity` +0.017, `fin_value_z` −0.015.
  **전부 0.05 미만, 35개 중 A×B 상관이 가장 작은 축이다.** 같은 `turnover`를 쓰는 `px_amihud_20d`가 규모와 −0.754인데
  이 피쳐는 +0.048 — 분모를 자기 과거 중앙값으로 잡아 규모를 상쇄한 설계가 그대로 작동했다 (08 §7).
- **미확인 중복**: `px_amihud_20d`·`px_zero_ret_ratio_20d`(거래량 계열 셋). `px_mom_12_1`과의 상호작용(§3.4).

### 국면 의존 (Phase C)

| 쌍 | 국면 | cell | 기대 δ | δ̂ | t_nw | q | placebo p | G2 / G3 | 판정 |
|---|---|---|---|---:|---:|---:|---:|---|---|
| P9 | `liq_high` (20일 시장 거래대금 / 252일 중앙값 > 1) | cum 0→20 | 미고정 → **관측 `+` 고정** | **+0.0269** | 3.61 | **0.0046** | 0.0099 | 유효 5 중 4 일치 / 1.13 | **screen_pass, 등급 A** |

(05_results §1·§4; run 보고서 `03c_conditional_ic_results.md`)

- **15쌍 중 가장 강한 결과다.** 시장 거래대금이 많은 국면에서 회전율 충격의 IC가 +0.0269 더 크다. 박종원(2020)의
  "반전은 고회전율 종목·전환시장에서 강하다"와 부호가 맞고, 08이 기록한 "반대 부호 5/5 일관"에 국면 구조가 있다는
  것이 확인됐다 (05_results §4).
- 무조건부 IC가 음수(−0.0264)이므로 δ̂ +0.0269는 "고유동성 국면에서 음의 IC가 그만큼 약해진다"는 뜻이다. 국면별 조건부
  평균 자체는 이 문서에 없다 — `conditional_ic.parquet`에서 확인해야 한다.
- P9와 P12(`flow_foreign_netbuy_to_volume × liq_high`)가 같은 국면을 공유한다. 독립된 발견 둘이 아니라 "유동성 국면 하나"로
  읽어야 한다 (05_results §6).
- 매크로 동행(참고): IQR이 `breadth_20d`와 +0.41 (00_survey §4.3).

### 모델 투입 메모

- **변환**: 이미 log 비율이라 z 또는 rank. 분모 0(`NULLIF`)은 NULL로 빠지므로 폭주는 없다.
- **결측**: `volume = 0`인 날, 직전 60일 기업행동. 분모가 중앙값이라 warm-up 결측은 적다.
- **유의점**: **선형 모델에 `+` 부호를 가정하고 넣으면 검증 결과와 반대다.** 반대 방향이 다섯 구간·20 offset 전부에서
  안정적이므로, 부호를 자유롭게 배우는 모델에서는 그대로 정보다. IC −/spread + 어긋남 — 랭킹 손실이면 IC를 따른다.
  tradable에서 27% 강하므로 유동성 편중 걱정은 없다. lag1이 더 강해 실행 여유가 있다.
- **horizon**: 사전등록 5~20일. **h=5·h=20 실험에 맞는다.** |IC| 최대는 cum 0→20. 40일 이후는 exploratory(미검정)라
  h=60/120 자리는 없다.
- **조합 후보**: **가장 먼저 검정할 것은 `s_liq_high × px_turnover_shock`** — Phase C에서 A등급으로 통과했다. `px_mom_x_volume
  = rank(mom_12_1) × rank(volume_trend)` (P8, Lee & Swaminathan 2000)과 `px_volume_trend`는 미구현. 반대 부호(`−`)로 새
  config에 재등록하는 것(00 §9.6, §10 5번).

### 한계

- 반대 부호를 discovery로 세지 않았다 — 규율상 맞지만 D등급 한 줄에 정보가 묻힌다 (08 §8).
- IC와 spread의 부호가 어긋나는데 풀 자료가 없다.
- 상호작용(모멘텀 × 거래량)을 보지 않았다 — 원 문헌의 핵심 구조다.
- 시간 placebo를 돌릴 긴 cell이 없었다. 40일 이후를 primary로 올리면 가능해진다.
- holdout 미개방.

---

## 9. `px_zero_ret_ratio_20d` — 무변동일 비율 (reference 전용)

| 항목 | 값 |
|---|---|
| primary feature | `px_zero_ret_ratio_20d` |
| secondary / variant | secondary — · `px_zero_ret_ratio_20d_lag1`(lag1, IC 없어 비교값 없음) |
| 마트 / 산식 위치 | `feat_price` / `research/etl/features/price.py:91` (마스킹 `:125`) |
| 원천 raw 테이블 | `daily_ohlcv` (+ `ca_event`) |
| prod 갱신 | 매일 18:30 Cronicle 체인 |
| 분류 좌표 | C1 / **C8** × T0 × U |
| 검증 phase / fdr_family / role | A / **`reference`** / **`reference_only`** · `fdr_include: false` |
| 기대 부호 → 관측 부호 | **없음**(`expected_sign: null`) → — |
| 사전등록 primary horizon | **[]** (비어 있다) · exploratory [1, 5, 20, 60, 120] · `include_bucket_primary: false` |
| 관측 신호 밴드 · 모양 | — (`horizon_ic.parquet`에 행 0개) |
| 등급 / screen_pass / discovery | **R** / 대상 아님(`role != ready`) / 0 |
| 모델 검증 이력 | T1·T2 후보 아님. baseline 40의 px 15 목록(model_features §1.1)에도 없다 |

### 산식과 규칙

```sql
AVG(CASE WHEN log_ret = 0 OR volume_d = 0 THEN 1.0 ELSE 0.0 END) OVER w20 AS px_zero_ret_ratio_20d
-- w20 = ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
CASE WHEN ca_count_20 > 0 THEN NULL ELSE px_zero_ret_ratio_20d END
```

- 최근 20거래일 중 "가격이 안 움직였거나(`log_ret = 0`) 거래가 없었던(`volume = 0`) 날"의 비율. 0~1 (09 §2.1).
- **거래정지일은 패널에서 아예 빠지므로 여기 잡히지 않는다.** 이 피쳐는 "거래는 가능했는데 안 일어난 날"을 잰다.
  거래정지는 baseline의 `px_is_halted`·`px_halt_ratio_20d`가 따로 잰다 (09 §2.4).
- 두 조건을 OR로 합쳐 어느 쪽이 얼마나 기여하는지 구분하지 않는다 (09 §8).
- 사전 설계(P10)는 `20~63d`로 적었는데 구현은 20일 하나다.

### 가용 시점(PIT)과 지연

- 당일 종가·거래량 기준 `native_t`. lag1 컬럼도 있지만 IC를 계산하지 않아 유지율은 **미측정**이다.

### 예측력 — broad × common_survivor × native_t

**재지 않았다.** Phase A `horizon_ic.parquet`에 이 family의 행은 0개다. `broad_ic`·`q_fdr_global`·`candidate_horizon_band`·
`peak_*`·`onset_h` 전부 `null`, discovery 0 (09 §4.1). AB 결합 30 family에도, 153개 결합 BH 모집단에도 없다 (09 §4.3).

`—`를 0으로 읽으면 안 된다. `reference_only`(애초에 계산하지 않기로 함)는 `insufficient`(계산하려 했는데 표본이 없음,
`fin_sue`)와도, `exploratory_short_regime`(진단용으로만 계산, 공매도 4 family)과도 다르다 (09 §4.2, 00 §9.10).

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | `null` — broad IC가 없어 비율을 못 만든다 | 대상 아님 |
| lag1 유지율 | `null` | 대상 아님 |
| available 표본 부호 | `null` | 대상 아님 |
| 기간 일관성 | `valid_subperiods = 0` | 대상 아님 |
| 시간 placebo 
| `null` | 대상 아님 |
| 비중첩 offset | `null`. card `warnings: ["insufficient_offset_coverage"]` — 품질 문제가 아니라 검사 대상이 아니라는 자동 표시 | 대상 아님 |
| source 경고 (Phase B) | 대상 아님 | — |

(09 §5) 등급 R은 `evidence_grade.evaluation_order: [R, C, A, B, D]`에서 R을 가장 먼저 보기 때문에 다른 등급으로 채점되기 전에
확정된다.

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 미측정 |
| 유효 거래일 · 날짜당 종목 수 | 미측정 — IC cell이 없어 `n_dates`·`n_obs_mean`이 없다 |
| coverage_ratio (Phase B) | 대상 아님 |

(09 §6) 산식이 20일 창에 마스킹만 걸리므로 `px_maxret_20d`(2,622일, 1,097종목)와 비슷할 것으로 보이지만 **이번 산출물로는
확인되지 않는다.**

### 중복성

- A×B 상관 산출물에 이 family가 없다(primary가 아니라서) (09 §7).
- **미확인 중복 — 가장 아쉬운 부분**: `px_amihud_20d`는 `volume = 0`인 날을 **제외한** 나머지의 평균이고 이 피쳐는 그 날의
  **비율**이다. 서로의 여집합을 본다. `px_amihud_20d`가 규모와 −0.754인 만큼 이 피쳐도 규모와 강하게 얽혀 있을 가능성이
  높은데 재지 않았다. `fin_log_mcap`과의 관계도 없다.

### 모델 투입 메모

- **역할**: alpha가 아니라 **필터·마스크·데이터 품질 지표**다. 무변동일이 많은 종목에서는 다른 가격 피쳐가 왜곡된다 —
  `px_idio_vol_60d` 과소 추정(0이 많이 섞임), `px_maxret_20d`가 우연에 좌우, `px_amihud_20d`가 소수 날짜로 평균,
  `px_reversal_5d`의 5일 합이 며칠치만 반영 (09 §2.3). 이 왜곡을 실제로 측정한 적은 없다.
- **변환**: 값 [0, 1]. 유동성 좋은 종목이 0에 뭉치므로 동점이 많다. rank보다 원값·구간 플래그(예: 상위 분위 마스크)가
  용도에 맞다.
- **결측**: 20일 창 마스킹만.
- **유의점**: **`tradable` universe 정의(20일 평균 거래대금 1억원·종가 1,000원)에 이 피쳐는 들어가지 않는다.** 필터로
  등록했는데 필터로 쓰이지 않았다 (09 §8). 모델 전처리에서 "무변동일 비율 상위 종목 제외" 같은 마스크로 쓰려면 그 임계값
  효과를 먼저 재야 한다.
- **horizon**: 해당 없음.
- **조합 후보**: "무변동일 비율이 높은 종목을 빼면 `px_idio_vol_60d`의 IC가 어떻게 되나" 같은 대조 — reference 피쳐의 본래
  용도인데 쓰이지 않았다. `log_ret = 0`과 `volume = 0` 분리. 20~63일 창 변형.

### 한계

- 필터로 실제로 쓰지 않았고, 다른 피쳐의 왜곡을 측정하지 않았다 (09 §8).
- `px_amihud_20d`·규모와의 상관이 없다.
- 커버리지 통계가 없다. 두 조건을 OR로 합쳤다. 20일 창만 만들었다.

---

## Horizon Scan 미검정 피쳐

같은 `feat_price` 마트에 있지만 Horizon Scan 단변량 검정을 거치지 않은 컬럼이다. baseline 40 = `feat_price` px 15 +
`feat_flow` flow 15 + `feat_fin_pit` fin 10(T1 게이트 기준, spec `feature_groups=("px","flow","fin")`). `px_amihud_20d`도
baseline 15개 중 하나지만 카드 7에서 다뤘다.

**`legacy` 컬럼 공통 주의**: `px_ret_*`·`px_mom_20_60`·`px_high_low_range_20d`·`px_turnover_ma20`·`px_gap_vs_ma20`·
`px_dist_52w_high`·`px_halt_ratio_20d`는 `legacy` CTE가 **`base`(정지 행 포함) 위에서** 계산하고 **기업행동 마스킹이 없다**
(`price.py:161~191`). 카드 1~9의 `variants` 컬럼과 창 정의가 다르다. `px_ret_1d`·`px_vol_*`·`px_turnover`는 유효 세션의
`ret_1d`/`turnover`를 쓴다.

| 컬럼 | 뜻(산식 요약) | 마트 | 모델 사용 | 검증 상태 | 메모 |
|---|---|---|---|---|---|
| `px_ret_1d` | `log_ret` (직전 유효세션 대비 로그수익률) | `feat_price` | baseline 40에 포함 | baseline 모델 포함(단변량 검정 없음, walk-forward Rank IC는 묶음 결과만) | `px_reversal_5d`의 재료 |
| `px_ret_5d` | `LN(close / LAG(close, 5))` | `feat_price` | baseline 40에 포함 | 같음 | `px_reversal_5d`와 부호만 반대인 근사 관계(5행 vs 5 유효세션, 마스킹 없음) |
| `px_ret_20d` | `LN(close / LAG(close, 20))` | `feat_price` | baseline 40에 포함 | 같음 | |
| `px_ret_60d` | `LN(close / LAG(close, 60))` | `feat_price` | baseline 40에 포함 | 같음 | |
| `px_mom_20_60` | `px_ret_20d − px_ret_60d` | `feat_price` | baseline 40에 포함 | 같음 | 모멘텀 스프레드. `px_mom_12_1`(D)과 창이 다르다 — 그 결과를 여기에 옮겨 읽으면 안 된다 |
| `px_vol_20d` | `STDDEV_SAMP(ret_1d)` 20행 | `feat_price` | baseline 40에 포함 | 같음 | 실현변동성(총변동성). `px_idio_vol_60d`(고유변동성)와 축이 다르다 |
| `px_vol_60d` | `STDDEV_SAMP(ret_1d)` 60행 | `feat_price` | baseline 40에 포함 | 같음 | |
| `px_high_low_range_20d` | `(MAX(high, 20) − MIN(low, 20)) / close` | `feat_price` | baseline 40에 포함 | 같음 | 고저폭. `px_maxret_20d`와 겹칠 여지, 미확인 |
| `px_turnover` | `close × volume` | `feat_price` | baseline 40에 포함 | 같음 | **거래대금 수준 — 사전 설계 P7이 "단독 입력 금지"(규모와 섞임)로 적은 값**이다. baseline에는 들어 있다 |
| `px_turnover_ma20` | `AVG(turnover)` 20행 | `feat_price` | baseline 40에 포함 | 같음 | 위와 같은 주의 |
| `px_gap_vs_ma20` | `close / AVG(close, 20) − 1` | `feat_price` | baseline 40에 포함 | 같음 | 이격도 |
| `px_dist_52w_high` | `close / MAX(close, 252) − 1` | `feat_price` | baseline 40에 포함 | **`px_near_52w_high`(A)와 동일 산식** — 카드 4의 검증 결과가 사실상 적용된다. 단 마스킹 없음·정지 행 포함 | T1이 `px_near_52w_high`를 신규 후보에서 뺀 이유 (04 §9) |
| `px_is_halted` | `open = high = low = 0` 플래그 | `feat_price` | baseline 40에 포함 | 진단용·마스킹용 | 유효 세션 패널에서는 이 행이 빠지므로 다른 `px_*`는 정지일에 계산되지 않는다 |
| `px_halt_ratio_20d` | `AVG(is_halted)` 20행 | `feat_price` | baseline 40에 포함 | 진단용·마스킹용 | `px_zero_ret_ratio_20d`(거래 가능했는데 안 일어난 날)와 구분 |
| `px_mom_6_1` | `LN(LAG(close, 21) / LAG(close, 126))`, 마스킹 `ca_count_252_skip21` | `feat_price` | 미투입 | **사전등록 secondary, 이번 run 미실행**(`horizon_ic.parquet`에 행 없음) | 6개월 모멘텀도 반대인지 확인하지 않았다 (02 §2.5) |
| `px_*_lag1` (10개) | 마스킹된 값의 `LAG(·, 1)` — 직전 유효 세션 값 | `feat_price` | 미투입 | 지연 게이트 전용 variant. family 대표 cell 하나만 스캔, `daily_ic`에 저장 안 됨 (00 §7) | `px_reversal_5d_lag1`, `px_mom_6_1_lag1`, `px_mom_12_1_lag1`, `px_resid_mom_12_1_lag1`, `px_near_52w_high_lag1`, `px_maxret_20d_lag1`, `px_idio_vol_60d_lag1`, `px_turnover_shock_lag1`, `px_zero_ret_ratio_20d_lag1`, `px_amihud_20d_lag1` |
| `px_market_beta` | — | `feat_macro_exposure` | 단계 1a | [06_macro_regime.md](06_macro_regime.md) 참조. Phase C P15(`× market_up`) 통과 | 카드 3·6의 `resid_ret`과 같은 시장모형이다 |

baseline 15개의 walk-forward 성과는 묶음 결과로만 있다 — T1 게이트 baseline(40/80) Rank IC h5 0.1155 / h20 0.1436 /
h60 0.1753(hgb), milestone A의 px+flow 30개 Ridge는 h20 Rank IC 0.128 (model_features §2). 컬럼별 기여도는 어디에도 없다.
