# 02. 단계 1a — 매크로 exposure 베타 6 family

- 작성일: 2026-08-29 (리뷰 `06_review_20260829.md` M1·M2·M4, §5 반영: 요인 시점 정렬, 시작일, `f_sp500_lag` NULL 규칙,
  126의 근거, 라벨 문장, `_feature` 인자, secondary 진단 경로)
- 성격: **사전등록 가설.** 결과를 보기 전에 산식·기대 부호·horizon을 못 박는다. 실제 계약 파일은
  `research/analysis/horizon_scan_macro_20260829.yaml`(`04_preregistration_overlay.md`)이고 이 문서는 그 근거다.
- 왜 이것이 첫 매크로 피쳐인가: 종목별 rolling beta는 **횡단면에서 변하고 날짜 상수가 아니다.** 그래서 현재
  rank IC 검증 틀과 temporal placebo 게이트를 그대로 통과할 수 있는 유일한 매크로 형태다(`00_survey/00` §3,
  `11` §8.4). 업종 코드가 없는 지금 `beta_usdkrw`·`beta_wti`는 수출/내수·에너지 노출을 데이터로 근사하는
  우회로이기도 하다(`11` §8.3).

---

## 1. 가설

| family | 무엇을 재나 | 메커니즘 | 기대 부호 | 근거 |
|---|---|---|---|---|
| `macro_beta_usdkrw` | 시장 요인을 뺀 수익률의 원/달러 변화 민감도 | 수출주(원화 약세 수혜) vs 내수·외화부채 기업. 프리미엄 부호는 국면 의존 | **양방향** | Chu 2022(세미베타 프리미엄), 고강석 2019(부호 불안정), 김진웅 2024(달러 베타 −) |
| `macro_beta_wti` | 유가 변화 민감도 | 에너지·화학·운송 노출 | **양방향** | 한국 섹터 비대칭 반응(RIBAF 2025), 방향 근거 없음 |
| `macro_beta_kr10y` | 국고채 10년 금리 변화 민감도 | 듀레이션·레버리지·성장주 할인율 대용 | **양방향** | 한국 횡단면 검정 공백(`00_survey/01` §5.8) |
| `macro_beta_sp500_lag` | 전일 S&P500 수익률 민감도 | 글로벌 연동 강도. 정보 점진 확산(RSZ 2013) | **양방향** | RSZ 2013 표본에 한국 없음 |
| `macro_beta_vix` | VIX 변화 민감도 | VIX 상승 시 오르는 종목은 헤지 자산 → 낮은 기대수익 | **`−`** | Ang et al. 2006(FVIX), Bali-Brown-Tang 2017(불확실성 베타 −) |
| `px_market_beta` | 동일가중 시장수익률 베타 | 저베타 이례현상(BAB) vs 국면 의존. Phase C의 `beta × market_up` 앵커 | **양방향** | Frazzini-Pedersen 2014; 한국 재현 근거 없음 |

양방향 family는 `12`의 규칙을 따른다 — 관측 부호를 따라 period·robustness 방향을 정하고, 결과가 나온 뒤 기대
부호를 바꾸지 않는다. `macro_beta_vix`만 방향을 고정한다. 문헌이 한 방향이기 때문이고, 한국에서 반대로
나오면 그것 자체가 정보다(`00_읽는_법` §9.6).

**왜 residual 기준인가.** 라벨은 `(거래일, 시장)` 동일가중 평균을 뺀 초과수익률이다 — **시장 평균 중립이지
베타 중립은 아니다.** `β_i = 1`인 종목에서만 시장 요인이 사라지고, `(β_i − 1)·r_m`은 라벨에 남는다(그래서
`px_market_beta`가 IC를 가질 수 있다). 한편 원수익률로 잰 요인 베타는 `Cov(r_i, f) = β_{i,m}·Cov(r_m, f) + Cov(e_i, f)`라
시장 베타 × (시장–요인 공분산)에 크게 물든다. 환율처럼 시장과 강하게 같이 움직이는 요인에서는 `px_market_beta`의
복사본이 된다. 그래서 primary는 **market-model 잔차의 요인 민감도**(`Cov(e_i, f)` 성분)로 정의하고, 원수익률
베타는 secondary로 같이 낸다. 둘의 IC 차이가 곧 "시장 요인을 뺀 뒤 남는 exposure의 값"이다.

---

## 2. 마트 `feat_macro_exposure`

파일 `research/etl/features/macro_exposure.py`. grain `(trade_date, ticker, market)`, valid session만.
`compute_all._build_features`에 `materialize_price` 다음에 넣는다. `feat_price`는 **건드리지 않는다**
(마트 SQL hash가 바뀌면 A0 입력 계보가 흔들린다).

### 2.1 입력

| 입력 | 어디서 | 규약 |
|---|---|---|
| 종목 로그수익률 `log_ret`, valid session | `daily_ohlcv` + `build_valid_session_sql` + quality join | `price.py`와 같은 CTE를 재사용한다(함수로 뽑아 공유) |
| 시장수익률 `market_ret` | `AVG(log_ret) OVER (PARTITION BY trade_date, market)` | `price.py:66` 정의 그대로 — **동일가중, 시장별** |
| market-model 잔차 `resid_ret` | 252 세션(현재 제외) `REGR_SLOPE/INTERCEPT`, `model_n_252 >= 252` | `price.py`의 `modeled`·`residuals` CTE 그대로. `feat_price`에서 2008-12부터 존재 |
| 매크로 요인 | `common_feature_daily_fact` (derived mart, 같은 snapshot) | 종목 panel(KRX 세션)에 `trade_date = feature_date`로 join. fact는 이미 as-of 피벗 상태라(`(feature_date, feature_code)`당 한 행, `asof_available_date <= feature_date` 항상 성립 — 리뷰 §5.2) 추가 PIT 필터는 항등이다 |

`common_feature_daily_fact`가 scan snapshot에 없으면 마트를 만들지 않고 family는 `blocked`가 된다(Phase B의
정상 경로). 2026-08-23 snapshot에는 아직 없다 — `04` §3 실행 순서 1번.

fact의 `feature_date` 격자는 2014~2023년에 KRX 세션이 아니라 평일이다(`docs/holidays_krx.csv`가 2024~2026만 있어
연 13~17개 KRX 휴장 평일이 전 세션 값 복사로 들어 있다 — 리뷰 M3 실측). **이 마트는 fact를 종목 panel에 join하므로
비세션 행은 자연히 빠지고**, 아래 LAG·창은 전부 ticker별 valid session 위에서 잡는다. 영향이 없다.

### 2.2 요인 정의 — fact에 들어 있는 값의 시점으로 정의한다

`x_t`는 "KRX 세션 t에 fact가 갖는 값"이다. **시점이 두 부류다.** 리뷰 §5.3에서 raw와 대조해 실측했다.

| 부류 | 시리즈 | availability_policy | `x_t`의 뜻 |
|---|---|---|---|
| **국내 (한 세션 지연)** | `fx_usdkrw_level`(← `fx_usdkrw_ecos`), `rate_kr_gov10y_level`, `market_kospi_close`, 거래대금 | `next_krx_session` | **관측일 t−1의 값.** 예: 2024-07-02 행 = 07-01 고시 1382.4 |
| **해외 (밤사이 값)** | `global_vix_level`, `commodity_wti_spot_level`, `global_sp500_ret_1d` | `same_krx_session_morning` | **NY t−1 종가.** KRX 세션 t 아침에 알 수 있는 최신 값 |

그래서 두 부류의 짝짓기가 다르다.

| 요인 | 산식 | 원천 feature_code | 짝짓는 종목 수익률 | 뜻 |
|---|---|---|---|---|
| `f_usdkrw` | `g_τ = ln(fx_{τ+1} / fx_τ)` | `fx_usdkrw_level` | `resid_ret_τ` | 세션 τ에 고시된 환율의 전일 대비 변화(fact 기준 τ+1 행 − τ 행). **동시(same-session) exposure** |
| `f_kr10y` | `g_τ = kr10y_{τ+1} − kr10y_τ` (%p) | `rate_kr_gov10y_level` | `resid_ret_τ` | 같음 |
| `f_wti` | `ln(wti_t / wti_{t−1})` | **`commodity_wti_spot_level`** (신규, §2.5) | `resid_ret_t` | NY t−1 종가 변화 → KRX 세션 t. **spillover exposure** |
| `f_sp500_lag` | `global_sp500_ret_1d` 그대로 | `global_sp500_ret_1d` | `resid_ret_t` | 이미 NY t−1의 일간 수익률. spillover |
| `f_vix` | `ln(vix_t / vix_{t−1})` | `global_vix_level` | `resid_ret_t` | spillover |
| `f_mkt` | `market_ret_t` | (마트 내부) | `log_ret_t` | 동시 |

**국내 요인의 짝 `(resid_ret_τ, g_τ)`는 세션 τ+1에 완성된다.** `g_τ`의 분자 `fx_{τ+1}`이 τ+1에야 fact에 들어오기
때문이다. 따라서 세션 t의 베타는 **τ ≤ t−1까지의 짝**으로 만든다(§2.3). 세션 t에 아는 정보만 쓰므로 look-ahead가
없다. 대신 국내 요인 베타는 해외 요인 베타보다 한 세션 늦게 갱신된다 — 카드에 표기한다.

**적어 두는 한계.** 매매기준율이 전 영업일 은행간 거래의 가중평균이라는 도메인 지식(리뷰 §10)은 데이터로 확인하지
않았다. 그것이 맞으면 `g_τ`는 τ−1 거래의 변화이고 "동시"는 한 세션 어긋난다. 사전등록은 **fact에 있는 값의 시점**으로
정의하고, 이 불확실성은 결과 해석에서 다룬다. 실측 없이 짝을 한 세션 더 옮기지 않는다.

**NULL 규칙 — 여섯 요인 전부에 적용.** 두 연속 세션 사이에 원천 관측이 갱신되지 않았으면(fact의 `asof_available_date`가
같으면) 그 세션의 요인은 **NULL**이다. 0으로 두면 가짜 무변동일이 회귀에 들어가고, `f_sp500_lag`처럼 값을 그대로 쓰는
요인은 **같은 수익률이 두 번** 들어간다(3,137세션 중 115세션 — 리뷰 §5.2). asof가 갱신됐는데 값이 같은 것은 진짜
무변동이라 0으로 둔다. REGR 계열 함수는 NULL 쌍을 자동으로 제외한다.

### 2.3 베타 정의

```sql
-- 창 A: 현재 세션 포함 252 valid session (해외 요인·시장 요인)
wA = PARTITION BY ticker, market ORDER BY trade_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
-- 창 B: 직전 세션까지 252 valid session (국내 요인 — 짝이 τ+1에 완성되므로)
wB = PARTITION BY ticker, market ORDER BY trade_date ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING

-- 국내 요인: 짝 (resid_ret_τ, g_τ). g_τ는 세션 τ 행에 "다음 세션 값 − 이번 세션 값"으로 미리 계산해 둔다.
macro_beta_usdkrw   = CASE WHEN REGR_COUNT(resid_ret, g_usdkrw) OVER wB >= 126
                           THEN REGR_SLOPE(resid_ret, g_usdkrw) OVER wB END          -- primary
macro_beta_kr10y    = (같은 꼴, g_kr10y)
-- 해외 요인·시장 요인: 짝 (resid_ret_t, f_t)
macro_beta_wti      = CASE WHEN REGR_COUNT(resid_ret, f_wti) OVER wA >= 126
                           THEN REGR_SLOPE(resid_ret, f_wti) OVER wA END
macro_beta_sp500_lag, macro_beta_vix = (같은 꼴)
px_market_beta      = CASE WHEN REGR_COUNT(log_ret, f_mkt) OVER wA >= 126
                           THEN REGR_SLOPE(log_ret, f_mkt) OVER wA END

-- secondary: 원수익률 베타 (같은 창·같은 짝, y = log_ret)
macro_rawbeta_<f>   = REGR_SLOPE(log_ret, <요인>) OVER <해당 창>, REGR_COUNT >= 126
-- secondary: 원화 약세일 세미베타 (Chu 2022 하방 공변동의 한국판)
macro_semibeta_usdkrw_up
                    = CASE WHEN REGR_COUNT(CASE WHEN g_usdkrw > 0 THEN resid_ret END,
                                           CASE WHEN g_usdkrw > 0 THEN g_usdkrw END) OVER wB >= 60
                           THEN REGR_SLOPE(CASE WHEN g_usdkrw > 0 THEN resid_ret END,
                                           CASE WHEN g_usdkrw > 0 THEN g_usdkrw END) OVER wB END
```

- **창 252·최소 유효 짝 126은 이 설계의 선택이다.** `price.py`의 `idio_model_min_valid=126`은 "126세션 창이 완전히
  채워졌을 때"라는 다른 뜻이고, market model은 완전 252 창을 요구한다(리뷰 §5.5). 여기서는 해외 휴장·국내 휴장 NULL을
  감안해 252 창 안에 유효 짝이 절반 이상이면 낸다. 세미베타는 표본이 절반이라 60으로 둔다.
- `price.py`의 `beta_252`는 현재 세션을 제외한 완전 252 창을 요구하는 잔차 계산용이다. `px_market_beta`는 피쳐이므로
  현재 세션을 포함하고 126 이상이면 낸다. **다른 정의**라는 것을 컬럼명으로 구분한다.
- 단위는 요인마다 다르다(수익률 vs %p). rank IC는 척도에 불변이므로 표준화하지 않는다.
- `_lag1` 컬럼: `LAG(x) OVER (PARTITION BY ticker, market ORDER BY trade_date)` — valid session 기준 직전 세션 값.
  `price.py:167~`과 같은 방식.

### 2.4 컬럼 목록

`trade_date, ticker, market, valid_session_idx,`
`macro_beta_usdkrw, macro_beta_wti, macro_beta_kr10y, macro_beta_sp500_lag, macro_beta_vix, px_market_beta,`
`macro_rawbeta_usdkrw, macro_rawbeta_wti, macro_rawbeta_kr10y, macro_rawbeta_sp500_lag, macro_rawbeta_vix,`
`macro_semibeta_usdkrw_up,`
`macro_beta_n_usdkrw, …, px_market_beta_n` (REGR_COUNT — 진단·커버리지용),
`(위 primary·secondary 전부)_lag1`, `snapshot_date, source`.

### 2.5 카탈로그 변경 — `commodity_wti_spot_level`

`common_feature_daily_fact`에 WTI는 20일 수익률(`commodity_wti_ret_20d`, `commodity_wti_spot_ret_20d`)만 있고
level이 없다. `definitions/common_features.py`의 catalog에 `_feature("commodity_wti_spot_level", "WTI 현물 레벨",
"commodity", "USD/bbl", "level", "commodity_wti_fred")`를 추가한다 — `_feature`는 위치 인자
`(feature_code, name_kr, category, unit, transform_code, series_id)`다(`common_features.py:1577`).

**PR-1a-1 구현 시 확인한 것 두 가지(2026-08-29).**

- **unit은 `USD/bbl`이다.** 초안은 `USD`였으나, fact의 `unit` 컬럼은 catalog 항목의 값을 그대로 쓰고
  (`marts/common_build.py:170`), 기존 `level` feature는 예외 없이 원천 series의 unit을 그대로 쓴다
  (`fx_usdkrw_level`=KRW, `global_vix_level`=index_point, `rate_us10y_level`=pct). `commodity_wti_fred`의
  series unit이 `USD/bbl`이므로 그 규칙을 따랐다. 자유 TEXT 컬럼이라 검증에 걸리는 값은 아니고, 베타 산식은
  `value_numeric`만 읽으므로 계산에 영향이 없다.
- **golden은 바뀌지 않는다.** 초안은 "파생 마트가 catalog를 import하므로 golden이 바뀐다"고 했으나 사실이
  아니다. `test_common_build_mart.py`는 시나리오마다 자기 catalog를 손으로 만들어 쓰고
  (`catalog = [_feature(...)]`), `test_reports_mart.py`는 `default_common_feature_catalog`를 monkeypatch한다.
  두 golden 다 실제 카탈로그를 읽지 않으므로 항목을 더해도 움직이지 않는다 — 실행해서 확인했다. 대신 "행 추가만"을
  구조적으로 고정하는 테스트를 `test_common_build_mart.py`에 넣었다(feature 하나를 더해도 기존 feature의 행이
  값·unit·asof까지 그대로인지).

**실 lake 검증(snapshot 2026-08-23, 평일 격자 2014-06-13~2026-08-23).** fact 행 117,672 → 120,852(+3,180),
추가된 3,180행은 전부 `commodity_wti_spot_level`이고 **기존 행 중 바뀌거나 사라진 것 0개**. 새 feature는
2014-06-16~2026-08-21, NULL 0개(3,180/3,180)로 `00_survey/00` §2.2의 시작일과 맞는다. freshness 게이트는
series 단위라(`marts/reports.py:freshness_violations`) 모집단이 그대로다 — `commodity_wti_fred`는 이미
`commodity_wti_spot_ret_20d`가 쓰는 active series다.

### 2.5a PR-1a-2 구현에서 확정·수정한 것 (2026-08-29)

설계 초안과 다르게 간 곳 셋과, 실측으로 드러난 것 둘이다.

- **`valid_session_idx`를 산출 컬럼에서 뺐다** (§2.4 목록과 다름). Phase B panel이 이 마트를 daily view로
  LEFT JOIN하는데 panel에는 이미 `dim_price_quality_daily`에서 온 `valid_session_idx`가 있다. DuckDB는 에러를
  내지 않고 둘째 컬럼을 `valid_session_idx_1`로 조용히 바꾼다 — 나중에 물릴 종류의 일이다. `feat_price`도
  같은 이유로 이 컬럼을 내지 않는다. `ROW_NUMBER() OVER (PARTITION BY ticker, market ORDER BY trade_date)`로
  언제든 복원된다. `snapshot_date`·`source`도 뺐다 — 다른 feature 마트와 같이 경로에 있다.
- **비양수 레벨에서 로그 변화는 NULL이다.** 설계는 `f_wti = ln(wti_t / wti_{t−1})`만 적었는데, WTI 현물은
  **2020-04-20에 −36.98**로 마감했고 그 값이 2020-04-21 KRX 세션 행에 들어온다. 로그가 정의되지 않아 빌드가
  그대로 죽는다(실측). 양끝이 모두 양수일 때만 계산하고 아니면 NULL로 둔다 — 휴장과 같은 처리이고 같은 이유다
  (값이 0인 게 아니라 값이 없다). 영향은 `f_wti`의 2020-04-21·04-22 두 세션뿐이다.
- **`*_n`은 primary(잔차) 회귀의 짝 수다.** family당 하나만 두는 §2.4 목록을 유지하되, `macro_rawbeta_*`는
  같은 창에서 **더 많은** 짝을 본다 — `resid_ret`은 252세션 시장모형이 채워져야 존재하고 `log_ret`은 아니다.
  그래서 이 수는 secondary에게는 하한이지 자기 값이 아니다.

실측 둘:

- **요인 NULL 밀도가 국내/해외에서 갈린다.** 2014-06-17 이후 2,990 세션에서 `f_vix` 2,899, `f_wti` 2,882,
  `f_sp500_lag` 2,897, `g_usdkrw`·`g_kr10y` 2,989(마지막 세션은 LEAD가 없어 NULL). 해외 계열은 미국 휴장으로
  3%가량 빠진다. 그래서 §2.6의 "국내 요인은 창 B라 한 세션 더 늦다"가 실제로는 뒤집힌다 — 창 B가 한 세션 더
  뒤로 닿는 이점보다 해외 계열 NULL이 커서 국내 베타가 오히려 며칠 빠르다.
- **`macro_beta_*`는 창 252에서 평균 223~238쌍을 쓴다**(최소 126). 여유가 있다.

시장모형 CTE(`market`/`modeled`/`residuals`)는 `research/etl/trading_panel.build_market_model_sql`로 뽑아
`feat_price`와 공유한다. `feat_price`의 SQL 텍스트는 A0 마트 캐시 키(`mart._sql_hash`)라 **바이트 단위로 같아야**
하고, 그 hash 두 개를 `test_research_features.py`에 상수로 못 박았다.

---

### 2.6 warm-up과 표본

시작을 정하는 것은 잔차가 아니라 **요인**이다. `feat_price`는 2007-06-05부터 있고 `resid_ret`(= `px_idio_vol_60d` 존재)은
2008-12-16부터 있다(리뷰 M2 실측). daily 공통 계열은 2014-06-16 시작이다.

- `macro_beta_*` primary·`macro_rawbeta_*`: 첫 요인 변화 2014-06-17 → 유효 짝 126개는 **약 2014-12 중순**(휴장 NULL 감안).
  ~~국내 요인은 창 B라 한 세션 더 늦다.~~ → **실측은 반대다**(§2.5a). 2026-08-23 snapshot에서
  `macro_beta_usdkrw`·`macro_beta_kr10y`는 **2014-12-17**, `macro_beta_wti`·`_sp500_lag`·`_vix`는 **2014-12-22**,
  `macro_semibeta_usdkrw_up`은 하한이 60이라 **2014-11-27**부터다.
- `px_market_beta`: 요인이 마트 내부라 panel 시작부터 있다. 실측 **2007-12-07**(패널 첫 세션 2007-06-05 + 126세션).
- `2014_2016` 구간은 약 2년치다. 기간 일관성 구간 수는 다른 Phase B family와 같은 조건이다.
- holdout 경계 2025-08-01은 그대로다.

---

### 2.5b 커버리지 (snapshot 2026-08-23, 실측)

`macro_beta_*`가 비어 있지 않은 종목 수.

| 연도 | `macro_beta_usdkrw` | `macro_beta_wti` | `px_market_beta` | 평균 유효 짝(wti) |
|---|---|---|---|---|
| 2014 | 1 | 1 | 1,899 | – |
| 2015 | 1,823 | 1,822 | 2,020 | 86 |
| 2016 | 1,960 | 1,957 | 2,101 | 218 |
| 2017 | 2,070 | 2,070 | 2,177 | 223 |
| 2018 | 2,127 | 2,125 | 2,216 | 227 |
| 2019 | 2,161 | 2,159 | 2,268 | 226 |
| 2020 | 2,236 | 2,236 | 2,332 | 226 |
| 2021 | 2,272 | 2,270 | 2,385 | 228 |
| 2022 | 2,359 | 2,358 | 2,444 | 228 |
| 2023 | 2,417 | 2,416 | 2,510 | 229 |
| 2024 | 2,467 | 2,465 | 2,598 | 226 |
| 2025 | 2,604 | 2,604 | 2,739 | 223 |
| 2026 | 2,617 | 2,611 | 2,729 | 227 |

2024년 횡단면 분포(5% / 중앙 / 95%)로 본 온전성: `px_market_beta` 0.32 / **0.99** / 1.75,
`macro_beta_sp500_lag` −0.42 / −0.02 / 0.47, `macro_beta_usdkrw` −0.60 / −0.00 / 0.61.
시장 베타 중앙값이 1 근처라는 것이 시장모형 배선이 맞다는 가장 단순한 증거다.

---

## 3. 사전등록 — registry 항목

전 family 공통: `phase: B`, `fdr_family: macro_exposure`, `role: phase_b_blocked`, `fdr_include: false`,
`primary_horizon_set: [20, 60]`, `exploratory_horizon_set: [1, 2, 3, 5, 10, 40, 120]`, `include_bucket_primary:
true`, `readiness_dependencies: [feat_macro_exposure, common_feature_daily_fact, label_scan]`,
`official_feature_variant: native_t`.

- horizon [20, 60]을 고른 이유: 베타는 252일 창의 느린 변수라 1~10일 예측을 주장할 근거가 없고, 120일은 NW lag
  119로 유효 관측이 적다. cum 2 + bucket 2(`[10,20]`, `[40,60]`) = **family당 4 cell, 6 family 24 cell.**
- `fdr_family`를 새로 둔다(`macro_exposure`). Phase B 안의 BH는 `fdr_family`가 아니라 전체 모집단 기준이므로 판정에
  영향은 없고 카드 분류에만 쓰인다.
- `common_feature_daily_fact`를 readiness dependency로 명시한다. `build_readiness_rows`(`horizon_scan_readiness.py:136~`)는
  `DESCRIBE`가 성공한 dependency만 "있음"으로 보므로, Phase B run이 그 view를 등록하는 한 줄이 필요하다.

  **PR-1a-2 결정: snapshot의 persist된 derived parquet를 읽는다(재계산하지 않는다).** 새 헬퍼
  `research/etl/lake.register_persisted_derived_mart(con, config, name)`가
  `derived_mart/snapshot_date=…/source=…/common_feature_daily_fact/`를 view로 바인딩한다. 이유 셋 —
  (a) 그 parquet가 `compute_all --from-step marts`가 쓰고 coverage/readiness 게이트가 통과시킨 바로 그 산출물이라
  "마트가 읽은 fact = readiness가 본 fact"가 정의상 성립한다. (b) `register_derived_marts`의 재계산 경로는
  `common_feature_observation_raw`·`common_feature_series` 등록과 `_common_feature_calendars`의 날짜 범위에 의존해
  게이트가 본 것과 다른 fact를 조용히 만들 수 있다. (c) parquet가 없으면 `FileNotFoundError`이고,
  `register_phase_b_marts`의 `_try`가 그것을 잡아 두 이름을 `available`에서 빼므로 6 family가 정상 경로로 blocked가 된다.

| family | primary column | secondary | expected_sign |
|---|---|---|---|
| `macro_beta_usdkrw` | `macro_beta_usdkrw` | `macro_rawbeta_usdkrw`, `macro_semibeta_usdkrw_up` | null |
| `macro_beta_wti` | `macro_beta_wti` | `macro_rawbeta_wti` | null |
| `macro_beta_kr10y` | `macro_beta_kr10y` | `macro_rawbeta_kr10y` | null |
| `macro_beta_sp500_lag` | `macro_beta_sp500_lag` | `macro_rawbeta_sp500_lag` | null |
| `macro_beta_vix` | `macro_beta_vix` | `macro_rawbeta_vix` | `"-"` |
| `px_market_beta` | `px_market_beta` | — | null |

`variant_columns: {native_t: <primary>, lag1: <primary>_lag1}`.

Phase B 모집단: 78 → **102**. 결합 BH 모집단: 153 → **177**. 기존 cell의 `q_fdr_global_ab`는 모집단이 늘어
바뀔 수 있다 — `12`의 확장 때와 같은 성질이고, 기존 Phase A discovery가 바뀌지 않는지 확인하는 것이 완료
기준이다.

---

## 4. 게이트와 진단

기존 Phase B 게이트를 그대로 받는다 — BH q 0.10, 기대 부호(양방향은 관측 부호 고정), 기간 부호 일관성,
tradable 유지율 0.50, lag1 유지율 0.50, available 부호 뒤집힘, non-overlap, `nw_lag ≥ 59`인 cell의 temporal
placebo. 추가 진단(판정 아님):

| 진단 | 왜 | 어디서 나오나 |
|---|---|---|
| primary vs `fin_log_mcap`·`mcap_krx_log` 일별 rank 상관 | 베타 추정은 소형·고변동 종목에서 잡음이 커서 규모의 대리변수가 될 수 있다. `\|ρ\| ≥ 0.5`면 카드에 경고 | 기존 `primary_feature_rank_correlation` |
| primary vs `px_market_beta`·`px_idio_vol_60d` 상관 | residual 정의가 실제로 시장 베타와 갈라졌는지 | 같음 |
| `macro_beta_*` vs `macro_rawbeta_*`·세미베타 IC 차이 | "시장을 뺀 exposure의 값" (§1) | **secondary cell은 본 스캔에서 스캔되지 않는다**(`01` §3.1). `horizon_scan_phase_b_diagnostics.py`에 secondary 컬럼을 primary cell 좌표(broad × common_survivor, cum 20·60)로 스캔하는 **진단 전용 경로**를 추가한다. BH 밖, 카드에만 |
| 날짜별 베타 횡단면 IQR 시계열 | 송민규·조성빈(2011): 변동성 큰 기간에 환노출 기업이 급증. Phase C 국면 변수와의 관계를 나중에 볼 재료 | 마트에서 직접 |
| `*_n` 커버리지 | 요인 NULL(휴장)이 많아 126 미만으로 떨어지는 구간이 있는지 | 마트에서 직접 |

---

## 5. 테스트

| 파일 | 내용 |
|---|---|
| `tests/unit/test_research_macro_exposure.py` (신규) | synthetic `daily_ohlcv` + synthetic `common_feature_daily_fact`로 (a) 요인 산식·NULL 규칙(asof 미갱신 → NULL, 여섯 요인 전부), (b) 국내 요인 짝 `(resid_ret_τ, g_τ)`가 τ+1 값을 쓰고 세션 t 베타가 τ ≤ t−1까지만 포함(look-ahead 검사), (c) `REGR_COUNT < 126` → NULL, (d) 알려진 선형 관계를 심은 종목의 베타 복원(오차 1e-9), (e) 세미베타는 `g > 0` 행만 사용, (f) `_lag1`이 직전 valid session, (g) fact의 비세션 행이 join에서 빠짐, (h) `common_feature_daily_fact` view 부재 시 명확한 에러 |
| `tests/unit/test_default_common_feature_catalog.py`, `test_common_build_mart.py` | `commodity_wti_spot_level` 등록(active·level·`commodity_wti_fred`·unit이 series와 일치), 기존 WTI feature 불변, catalog 항목 추가가 다른 feature의 fact 행을 건드리지 않음 |
| `tests/unit/test_horizon_scan_config.py` | overlay 로드 시 family 41개(17 A + 24 B), Phase B cell 102, Phase A primary 75 유지; base 두 hash 불변; `phase_c` 검증 실패 경로 6종 |
| `tests/unit/test_horizon_scan_phase_b.py` | 매크로 6 family가 두 dependency가 다 있을 때만 `ready_primary`(24 cell), 하나만 있으면 blocked |
| `tests/unit/test_horizon_scan_phase_b_run.py` | `register_phase_b_marts`가 fact→마트 순서로 등록하고, persist된 fact가 없으면 두 이름을 `available`에서 빼며 나머지 마트에는 영향이 없음 |
| `tests/unit/test_research_lake.py` | `register_persisted_derived_mart`가 snapshot parquet를 바인딩하고, 없으면 `FileNotFoundError` |
| `tests/unit/test_research_features.py` | `feat_price` SQL 텍스트 hash 고정(A0 계보), 두 마트가 같은 시장모형 CTE를 씀 |
| `tests/unit/test_horizon_scan_phase_b_diagnostics.py` | secondary 진단 스캔 경로 — 셀 생성, 발견 좌표 스캔, 컬럼 부재 보고, `daily_sink` 미전달 |
| 실 lake smoke (`tests/integration`) | 2026-08-23 snapshot에서 마트 빌드, 커버리지(종목 수 by 연도) 출력, primary 시작일 ≈ 2014-12 확인 |

---

## 6. 완료 기준

- [x] `commodity_wti_spot_level` 카탈로그 반영 (PR-1a-1). golden은 바뀌지 않았고, 실 lake diff로 기존 행 0개 변경 확인 — §2.5
- [x] `feat_macro_exposure` 마트 구현 (PR-1a-2) + snapshot 2026-08-23에 persist (실행 1·2, `04` §3.1). 7,024,118행 / 14.4초 / 1.0G, 커버리지 표 §2.5b. **overlay 커밋(PR-1a-3) 전이라 IC는 계산하지 않았다**
- [x] overlay 커밋과 hash 기록 (PR-1a-3). `horizon_scan_macro_20260829.yaml`, hash `236d0d35…`, 기록 `05_preregistration_record.md`. **이후에만** Phase B run
- [x] Phase B·AB run (2026-08-30): 24 cell 전부 valid, **discovery 16·`screen_pass` 13·등급 B13·C5·D6**. **기존 Phase A discovery 변화 0개**, 공통 153 가설 등급 변화 0개 — `05_results_stage1a_20260830.md`
- [x] §4 진단 (2026-08-30). secondary 진단은 **잔차 정의가 옳았음을 직접 보였다** — `macro_beta_vix`·`macro_beta_sp500_lag`은 원수익률로 재면 부호가 뒤집힌다. 상관 진단은 `|ρ| ≥ 0.7` 쌍 0개(최대 0.345). `05_results_stage1a_20260830.md` §6
- [x] 결과 문서 [`05_results_stage1a_20260830.md`](05_results_stage1a_20260830.md)
