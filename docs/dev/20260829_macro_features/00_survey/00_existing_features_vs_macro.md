# 00. 기존 35개 피쳐와 거시 지표는 겹치는가

- 작성일: 2026-08-29
- 대상: `docs/dev/20260731_raw_features`에서 검증한 35 feature family
  (해설: `docs/dev/20260829_raw_features_explain/`)
- 목적: 매크로 피쳐 발굴을 시작하기 전에, **지금 있는 피쳐에 매크로 정보가 이미 들어가 있는지**를
  계보·구조·실증 세 방향에서 확인한다.
- 판정이 아니라 점검 기록이다. 새 피쳐의 사전등록은 `01_`·`02_` 문서와 이후 설계 문서에서 한다.

---

## 1. 결론

**직접 겹치는 것은 없다. 그러나 "관계가 없다"는 뜻은 아니다.**

| 점검 | 결과 |
|---|---|
| 계보 — 35 family 중 매크로 raw(`common_feature_observation_raw`)를 읽는 것 | **0개** |
| 계보 — walk-forward baseline 모델(px 15 + flow 15 + fin 10)에 공통 피쳐 그룹(`cf`) 포함 | **없음** (`feature_groups=("px","flow","fin")`) |
| 구조 — 현재 검증 틀(날짜×시장 내 rank IC)에서 매크로 변수가 직접 신호로 잡힐 수 있는가 | **불가능** (날짜 내 상수) |
| 실증 — 피쳐의 날짜별 횡단면 중앙값·분산이 매크로 상태와 같이 움직이는가 | **움직인다** (§4) |

세 줄로 줄이면 이렇다.

1. 지금까지 나온 IC·등급에는 매크로 정보가 **한 조각도 직접 들어가지 않았다.** 검증 틀이 날짜 안의
   순위만 보기 때문에, 그날 모든 종목에 같은 값인 변수는 형태(level·Δ·YoY·더미)와 무관하게 소거된다.
2. 그런데 피쳐 값의 **날짜별 수준과 흩어짐은 매크로 상태를 따라 움직인다.** 52주 신고가 근접도
   중앙값은 기간 스프레드와 ρ 0.65, 외국인 순매수 중앙값은 시장 폭과 0.63·원화 약세와 −0.30이다.
   순위 변환이 이 부분을 통째로 지우고 있는 것이다.
3. 따라서 매크로 작업은 **"매크로를 피쳐로 추가"가 아니라 "지워진 조건부 구조를 되살리는 일"**이다.
   종목별 exposure(민감도), 매크로 × 종목 특성 interaction, 국면별 조건부 IC의 세 갈래이고,
   기존 문서(`02` §3.6, `11` §8.3)가 이미 그렇게 정식화했다. 그중 구현된 것은 아직 없다.

---

## 2. 계보 점검 — 코드가 말하는 것

### 2.1 35 family의 입력 raw

`09_all_feature_results.md` §3의 계보에 Phase B 확장 10 family를 덧붙였다. 매크로 raw는 어디에도 없다.

| 마트 (`research/etl/features/`) | family | 입력 raw |
|---|---|---|
| `feat_price` (`price.py`) | px_* 9 | `daily_ohlcv` |
| `feat_flow` (`flow.py`) | flow_* 8 | `krx_security_flow_raw`, `dart_share_count_raw` |
| `feat_fin_scan_daily` (`fin_scan.py`) | fin_* 5 | `stock_metric_vintage_fact` ← DART XBRL/재무 raw, `daily_ohlcv` |
| `feat_event_scan_daily` (`event_scan.py`) | ev_net_share_issuance_yoy, ev_payout_yield | `dart_capital_change_raw`, `dart_shareholder_return_raw`, `daily_ohlcv` |
| `feat_filing_activity` (`filing_activity.py`) | ev_filing_activity, ev_amendment_ratio, own_*_filing_activity, own_amendment_ratio | `dart_filing_receipt_raw` |
| `feat_periodic_extras` (`periodic_extras.py`) | hc_* 2, own_major_stake_* 2 | `dart_employee_raw`, `dart_governance_raw` |
| `feat_market_cap` (`market_cap.py`) | mcap_krx_log | `daily_market_cap` |
| `fin_sue_event` (`sue_event.py`) | fin_sue | `dart_filing_receipt_raw`, `dart_xbrl_fact_raw` |

각 파일의 import를 확인했다. `research/etl/features/common.py`(공통 피쳐 브로드캐스트 마트
`feat_common`)를 import하는 피쳐 마트는 없다. `horizon_scan_config.yaml`과
`horizon_scan_expansion_20260827.yaml`의 `families:`에도 `cf_`·`macro_` family는 없다.

### 2.2 매크로가 "있는데 안 쓰이는" 자리 둘

**① `feat_common` 마트.** `common_feature_daily_fact`를 PIT 필터(`asof_available_date <= feature_date`)로
날짜별 한 행으로 피벗해 12개 `cf_` 컬럼을 만든다(KOSPI 5·20일 수익률, 한/미 기간 스프레드,
USD/KRW 5일, VIX level, S&P500 1일, WTI 20일 등). 이 마트를 읽는 곳은
`research/models/_01_20_access_return_rank/build_dataset.py` 하나이고, 그것도
`spec.feature_groups`에 `"cf"`가 들어 있을 때만이다. 기본값은 `("px", "flow", "fin")`이라
**acceptance gate(`07`)의 baseline 40개 피쳐에 공통 피쳐는 없다.**

~~같은 파일 docstring이 "daily history starts 2025-12-15"라고 적고 있는데 **오래된 문장이다.**~~
→ **고침 (2026-08-29, PR-docs).** 실제 `common_feature_daily_fact`는 일별 계열이 2014-06-16,
월별 매크로가 2013-06-20부터 있다(snapshot 2026-08-23에서 재확인). docstring도 그 값으로 바꿨다.

**② N8 고용 시리즈.** `definitions/common_features.py`에 `macro_unemployment_rate`·
`macro_employment_rate`·`macro_employed_persons`가 카탈로그에 올라 있고, `12_expansion_preregistration_20260827.md`가
level 두 개를 **Phase C 조건부 regime 후보**로 사전등록만 해 뒀다. Phase C는 열리지 않았다(dormant).

### 2.3 시장 정보가 이미 내장된 피쳐 — 어디까지 내장됐나

세 family는 산식 안에 "시장"이 들어 있어 매크로와 겹치는 것처럼 보일 수 있다. 실제 범위는 좁다.

| family | 내장된 시장 정보 | 내장되지 **않은** 것 |
|---|---|---|
| `px_idio_vol_60d` | 252일 market-model 잔차. `market_ret = AVG(log_ret) OVER (PARTITION BY trade_date, market)` — **같은 시장의 동일가중 평균수익률**이다. KOSPI 지수가 아니다 | 환율·금리·유가·글로벌 지수 exposure. 시장 요인 하나만 걷어낸다 |
| `px_resid_mom_12_1` | 같은 market-model의 잔차 누적 | 같음 |
| `flow_foreign_*` 3종 | 외국인 순매수 자체가 글로벌 위험선호·환율의 함수라 **간접적으로** 내장 | 명시적 매크로 변수는 없음. §4에서 공동 움직임만 확인 |

즉 "시장 중립"은 동일가중 시장 요인 하나에 대한 중립이고, **매크로 요인에 대한 중립은 아니다.**
`px_beta_252d`(시장 beta)는 `02` §3.1에 후보로 있지만 매크로 beta는 하나도 정의돼 있지 않다.

---

## 3. 구조 점검 — 왜 매크로가 현재 결과에 들어갈 수 없나

`00_읽는_법.md` §4.1~4.2의 정의를 그대로 따르면 자명하다.

- 라벨 `raw_label_{h}d`는 같은 `(거래일, 시장)` 안의 동일가중 평균수익률을 뺀 초과수익률이고,
  `y_rank`는 그 안의 백분위 순위다.
- IC는 `(거래일, 시장)`마다 피쳐와 `y_rank`의 Spearman 상관이다.

그날 모든 종목에 같은 값인 변수는 횡단면 분산이 0이라 상관이 정의되지 않는다. VIX level이든
ΔVIX든 CPI YoY든 "VIX > 25" 더미든 마찬가지다. **매크로가 시장 전체를 올리거나 내리는 효과는
라벨 정의 단계에서 이미 빼 버렸다.** 이건 결함이 아니라 설계다 — 이 프로젝트가 예측하려는
것은 "어느 종목이 시장보다 나은가"이고, 시장 자체의 방향은 아니다.

그래서 매크로가 이 틀에 들어올 수 있는 길은 셋뿐이다(`11` §8.3 재정리).

| 길 | 형태 | 횡단면에서 변하나 | 현재 틀에 그대로 들어가나 |
|---|---|---|---|
| ① exposure | `rolling_beta(r_i, Δmacro)` — 종목별 민감도 | **변한다** | **그대로 들어간다.** 보통의 Phase family로 사전등록 가능 |
| ② interaction | `regime_t × characteristic_i` | 변한다 | 들어간다. 단 `characteristic` 단독과의 증분성을 따로 봐야 한다 |
| ③ 조건부 IC | 국면별로 IC를 나눠서 본다 | — | 피쳐가 아니라 **검증 설계**다. 일별 IC 시계열 저장이 선행 조건 |

한 가지 예외를 적어 둔다. walk-forward 모델은 날짜를 pooled로 학습하므로, 트리 모델에 `cf_`
컬럼을 넣으면 날짜 상수라도 **분기 조건으로 쓰여 암묵적 interaction이 된다.** 지금은 `cf` 그룹이
꺼져 있어 일어나지 않는다. 이 길을 열면 어떤 interaction이 학습됐는지 사후에 해석하기 어려우므로
②를 명시적으로 만드는 것이 먼저다.

---

## 4. 실증 점검 — 횡단면 통계는 매크로와 같이 움직인다

### 4.1 무엇을 잰 것인가

"매크로가 IC에 못 들어간다"는 구조 논증만으로는 "그래서 매크로 조건화가 의미 있는가"에 답할 수
없다. 조건화가 의미 있으려면 **피쳐의 횡단면 분포 자체가 국면에 따라 달라져야** 한다. 그걸 봤다.

- 피쳐: `feat_price`·`feat_flow` (snapshot `2026-08-23`, source `sj2_remote`) 의 px 9·flow 8 family
  primary feature. 날짜마다 **중앙값(수준)**과 **IQR(흩어짐)**을 구했다.
- 기간: 2014-06-02 ~ 2025-07-31, 2,742 거래일. holdout(2025-08-01~)은 열지 않았다.
- 매크로: `common_feature_daily_fact` (snapshot `2026-08-12`) 를 PIT 필터로 피벗해 16개 변수를
  만들었다 — VIX level·20일 변화, USD/KRW 20일 수익률, KOSPI 20·60일 수익률, 한/미 10년 금리 20일
  변화, 한/미 기간 스프레드, 미-한 10년 금리차, WTI 20일, CPI YoY, M2 YoY, 소비자심리 level,
  KOSPI 거래대금(log), 20일 시장 폭 `(adv−dec)/(adv+dec)`.
- 월말 표본(134개)의 Spearman 상관을 정본으로 쓰고 일별(2,742개)은 참고로 뒀다.
  전체 표: `appendix/feat_macro_spearman_monthend_20260829.csv`, `…_daily_20260829.csv`.

### 4.2 읽기 전에 — 이 숫자의 한계

- **추세끼리의 상관이 섞여 있다.** 기간 스프레드·CPI YoY·M2 YoY는 2014~2025년 사이 사이클이
  두세 번뿐이라, 같은 방향으로 흐른 다른 계열과도 상관이 높게 나온다. |ρ| 0.5~0.6이 나와도
  인과나 예측력이 아니다.
- 134개 월말 관측은 자기상관이 크다. 유효 표본은 이보다 훨씬 적다. 유의성 검정을 하지 않았다.
- 공매도 4개는 표본이 50~74개 월말이고 2016~2020 한 국면이다. 아래 표에서 뺐다.
- `common_feature_daily_fact`의 날짜 격자는 2014~2023년에 KRX 세션이 아니라 평일이다(`docs/holidays_krx.csv`가 2024~2026만
  있어 연 13~17개 휴장 평일이 전 세션 값 복사로 들어 있다 — `01_design/06_review_20260829.md` M3). 피쳐 집계(KRX 세션)와
  `trade_date`로 inner join했으므로 비세션 행은 빠졌고, 월말 표본에는 영향이 없다. 20일 변화·20일 시장 폭 같은 창은 fact
  격자에서 셌으므로 2024년 이전은 약 19 KRX 세션에 해당한다. 서술 통계라 결론은 바뀌지 않는다.
- 이 표는 **공동 움직임의 서술**이다. "이 매크로가 이 피쳐의 IC를 바꾼다"는 주장은 §4.4의
  일별 IC 시계열이 있어야 할 수 있다.

### 4.3 결과 — |ρ| 0.40 이상만 (월말, Spearman)

**수준(중앙값).** 피쳐 값의 시장 전체 수준이 어느 국면 변수를 따라가는가.

| 피쳐 중앙값 | 같이 움직이는 매크로 (ρ) | 읽기 |
|---|---|---|
| `px_near_52w_high` | us_term_spread **+0.67**, kr_term_spread +0.65, kospi_ret_60d +0.52, vix −0.40 | 경기·시장 국면을 거의 그대로 반영한다. 순위 변환이 이 부분을 전부 지운다 |
| `px_mom_12_1` | kr_term_spread +0.55, us_term_spread +0.55 | 같은 성격. 모멘텀 수준 = 지난 1년 시장 |
| `px_idio_vol_60d` | kr_term_spread +0.52, cpi_yoy **−0.46**, vix +0.31 | 변동성 수준이 물가·금리 국면과 연동 |
| `px_maxret_20d` | vix **+0.46** | 복권형 수익률의 빈도는 VIX가 높을 때 늘어난다 |
| `px_amihud_20d` | kospi_turnover_log **−0.46**, wti −0.45, kospi_ret_60d −0.41, m2_yoy −0.41 | 비유동성 수준은 시장 거래대금의 함수. 유동성 국면 변수가 이미 카탈로그에 있다 |
| `px_reversal_5d` | breadth_20d **−0.45**, vix_chg_20d +0.35 | 정의상 시장이 빠진 뒤 값이 올라간다 |
| `flow_foreign_netbuy_to_volume_20d` | breadth_20d **+0.63**, kospi_ret_20d +0.43, usdkrw_ret_20d **−0.30**, vix_chg_20d −0.29 | 원화 약세·VIX 상승 국면에 외국인 순매도. 국내 실증과 부호가 같다 |
| `flow_foreign_holding_ratio_chg_20d` | breadth_20d +0.55, kospi_ret_20d +0.39, usdkrw_ret_20d −0.35 | 위와 같은 축 |
| `flow_individual_netbuy_to_volume_20d` | breadth_20d **−0.53** | 외국인의 거울상 |

**흩어짐(IQR).** 횡단면이 얼마나 벌어지는가 — 신호 강도가 국면 의존일 가능성을 시사하는 쪽이다.

| 피쳐 IQR | 같이 움직이는 매크로 (ρ) | 읽기 |
|---|---|---|
| `flow_inst_netbuy_to_volume_20d` | kospi_turnover_log **−0.72**, cpi_yoy −0.50, vix −0.43 | 거래가 활발할수록 기관 순매수 비율의 횡단면이 **좁아진다** |
| `flow_individual_netbuy_to_volume_20d` | kospi_turnover_log −0.65, m2_yoy −0.48, us_kr_10y_diff +0.48 | 같음 |
| `flow_foreign_netbuy_to_volume_20d` | m2_yoy −0.58, us_kr_10y_diff +0.58, term spreads −0.54 | 유동성 국면·금리차에 따라 외국인 수급 분산이 달라진다 |
| `px_near_52w_high` | us_kr_10y_diff **+0.61**, m2_yoy −0.54, kr_term_spread −0.42 | 미-한 금리차가 벌어질 때 신고가 근접도의 횡단면이 벌어진다 |
| `px_idio_vol_60d` | kospi_turnover_log **+0.52**, vix +0.37 | 거래가 활발할수록 특이변동성이 종목 간에 더 벌어진다 |
| `px_amihud_20d` | kospi_turnover_log −0.52, wti −0.41, kospi_ret_60d −0.38 | 유동성 국면 |
| `px_mom_12_1` | m2_yoy +0.44, kr_term_spread +0.41, vix +0.35 | |
| `px_turnover_shock` | breadth_20d +0.41 | |

일별 표본(참고)에서도 부호와 크기가 거의 같다. 예: `px_near_52w_high_med`–kr_term_spread 0.66,
`flow_foreign_netbuy_20d_med`–breadth 0.62, `flow_foreign_netbuy_20d_med`–usdkrw_ret_20d −0.30.

### 4.4 이 결과가 말하는 것과 말하지 않는 것

**말하는 것.**
- 35개 중 "변장한 매크로 피쳐"는 없다. 매크로와 가장 많이 같이 움직이는 `px_near_52w_high`도
  종목 간 순위는 매크로가 정하지 않는다.
- 그러나 **수준과 흩어짐 모두 국면을 따라 움직인다.** 특히 수급 3종의 IQR과 시장 거래대금의
  ρ −0.65~−0.72는 "거래가 활발한 국면에서 수급 피쳐의 횡단면 정보가 줄어든다"는 뜻이라,
  **수급 피쳐의 IC가 유동성 국면에 따라 달라질 가능성**을 강하게 시사한다.
- 외국인 수급과 원화·VIX의 관계는 부호가 문헌과 같다. `flow_foreign_* × (usdkrw, VIX)` interaction은
  근거가 있는 첫 후보다.

**말하지 않는 것.**
- **어느 피쳐의 IC가 어느 국면에서 강한지는 아직 모른다.** `per_date_market_rank_ic`가 일별 IC를
  계산하고도 평균만 남기고 버린다(`00_읽는_법.md` §7). 그것을 저장하면 이 표의 매크로 변수와
  일별 IC의 상관을 바로 잴 수 있다. **매크로 작업에서 비용 대비 가장 값이 큰 첫 조치다.**
- 재무·이벤트·지분·인적자본 18 family는 이 점검에 넣지 않았다. 회계 기반이라 날짜별 중앙값이
  분기 단위로만 움직이고, `fin_value_z`처럼 날짜 내 z-score인 것은 중앙값이 정의상 0이다.
  이들에게 매크로가 붙는 길은 ①·②이고 (예: `fin_log_mcap × cf_small_growth_regime`, 레버리지 ×
  금리 변화), 수준 공동 움직임 점검은 의미가 적다.

---

## 5. 새 작업에 주는 제약

1. **매크로 level을 Phase B family로 등록하지 않는다.** `12` 가 이미 그렇게 결정했고 §3이 이유다.
2. **① exposure beta를 먼저 한다.** 종목별 rolling beta는 횡단면에서 변하고 시계열 상수도 아니어서
   현재 검증 틀과 temporal placebo 게이트를 그대로 통과할 수 있는 유일한 형태다(`11` §8.4).
   업종 코드가 없는 지금, `beta_usdkrw`(수출/내수 대용)·`beta_wti`(에너지 노출)는 사업영역
   이질성의 우회로이기도 하다(`11` §8.3).
3. **② interaction은 §4.3에서 근거가 확인된 쌍부터 사전등록한다.** `flow_foreign_* × usdkrw_ret`,
   `flow_foreign_* × ΔVIX`, `px_maxret_20d`/`px_idio_vol_60d × VIX 국면`, 수급 3종 × 거래대금 국면,
   `px_amihud_20d × 거래대금 국면`, `fin_log_mcap × (kosdaq−kospi 20d)`.
4. **③ 조건부 IC는 일별 IC 시계열을 저장한 뒤에 한다.** 국면 cut을 결과를 보고 고르지 않는다 —
   `12`의 Phase C 규율 그대로다.
5. **월간 매크로(CPI·M2·심리·고용)는 interaction의 국면 변수로만 쓴다.** 자기상관이 극도로 높아
   temporal placebo를 통과하기 어렵고(`11` §8.4), `available_from_date`(월말 + 20일)를 반드시 쓴다.

---

## 6. 재현

```bash
# 1) 날짜별 횡단면 통계 (feat_price · feat_flow, snapshot 2026-08-23) — 각 2초 안팎
# 2) 매크로 피벗 (common_feature_daily_fact, snapshot 2026-08-12, PIT 필터)
# 3) 월말 표본 Spearman
uv run --extra analysis python - <<'PY'
import duckdb, pandas as pd, numpy as np
con = duckdb.connect()
fp = "data_lake/feature_mart/snapshot_date=2026-08-23/source=sj2_remote/feat_price/**/*.parquet"
cf = "data_lake/derived_mart/snapshot_date=2026-08-12/source=sj2_remote/common_feature_daily_fact/**/*.parquet"
px = con.sql(f"""
  SELECT trade_date, median(px_near_52w_high) med
  FROM read_parquet('{fp}') WHERE trade_date BETWEEN '2014-06-01' AND '2025-07-31'
  GROUP BY 1""").df().set_index("trade_date")
m = con.sql(f"""
  WITH pit AS (SELECT feature_date, feature_code, CAST(value_numeric AS DOUBLE) v
               FROM read_parquet('{cf}') WHERE asof_available_date <= feature_date
               QUALIFY ROW_NUMBER() OVER (PARTITION BY feature_date, feature_code
                                          ORDER BY asof_available_date DESC) = 1)
  SELECT feature_date trade_date,
         MAX(CASE WHEN feature_code='rate_kr_term_spread_10y_3y' THEN v END) kr_ts
  FROM pit GROUP BY 1""").df().set_index("trade_date")
df = px.join(m, how="inner"); df.index = pd.to_datetime(df.index)
me = df.groupby([df.index.year, df.index.month]).tail(1)
print(me["med"].corr(me["kr_ts"], method="spearman"))   # ≈ 0.65
PY
```

전체 17 family × 16 변수 표를 만든 스크립트는 위 구조를 그대로 확장한 것이다. 산출 CSV는
`appendix/`에 있다.

---

## 7. 참고한 문서

- `docs/dev/20260731_raw_features/01_feature_candidate/02_feature_candidate.md` §3.6 — 조건부(interaction) 원칙
- `docs/dev/20260731_raw_features/01_feature_candidate/09_all_feature_results.md` §3 — 원천 계보
- `docs/dev/20260731_raw_features/01_feature_candidate/11_feature_taxonomy.md` §8 — 매크로 원천 현황·사용법 셋·구조적 경고
- `docs/dev/20260731_raw_features/01_feature_candidate/12_expansion_preregistration_20260827.md` — N8 Phase C 사전등록(dormant)
- `docs/dev/20260731_raw_features/02_data_expansion_plan/08_w3_valuation_and_macro.md` 파트 B — N8 고용 시리즈 계획
- `docs/dev/20260829_raw_features_explain/00_읽는_법.md` §4, §7 — 라벨·IC 정의, 일별 IC 미저장
- `research/etl/features/price.py:66` — market-model의 `market_ret` 정의
- `research/etl/features/common.py` — `feat_common` 마트 (docstring의 시작일 문장은 오래됨)
- `research/models/_01_20_access_return_rank/spec.py:26` — baseline `feature_groups`
