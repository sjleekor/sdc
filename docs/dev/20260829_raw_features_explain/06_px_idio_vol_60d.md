# 06. `px_idio_vol_60d` — 60일 고유변동성 (IVOL)

- 작성일: 2026-08-29
- family: `px_idio_vol_60d` · primary feature: `px_idio_vol_60d` · domain: `price`
- Phase A · fdr_family `price` · 기대 부호 `−` · 관측 부호 `−`
- 등급 **A** · `screen_pass` 통과 · discovery **6/6 cell 전부**
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**시장 요인으로 설명되지 않는 변동성이 큰 종목이 이후 20~60일 동안 부진했다.**
**검증한 35개 family 중 |IC|가 가장 크다** (cum 0→60 IC −0.1433, q = 4.4e-46).

강건성도 `px_maxret_20d`와 나란히 가장 깨끗한 축이다 — 6개 cell 전부 discovery, 5기간 전부
같은 방향, offset 20개 전부 일치, 시간 placebo 통과.

**다만 세 가지를 함께 봐야 한다.**

1. 60일 보유 기준 5분위 수익률 차이는 **+2.99%p**다. |IC| 순위 1위지만, 120일을 보는
   `px_amihud_20d`의 +11.21%p보다는 작다. **horizon이 다르면 IC 순위와 수익 순위가 다르다.**
2. **`px_maxret_20d`와 경제적으로 같은 축이다.** 둘 다 "크게 흔들리는 종목"을 잰다. 그런데
   둘의 상관을 재지 않았다 (§7).
3. 산식이 무거워 **표본이 2,334일로 가격 계열 중 짧은 편**이다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/price.py:111
CASE WHEN COUNT(resid_ret) OVER w126 >= 126
     THEN STDDEV_SAMP(resid_ret) OVER w60 END AS px_idio_vol_60d

-- w60  = ROWS BETWEEN  59 PRECEDING AND CURRENT ROW
-- w126 = ROWS BETWEEN 125 PRECEDING AND CURRENT ROW
```

**시장모형 잔차의 최근 60거래일 표준편차**다.

잔차 `resid_ret`은 [03_px_resid_mom_12_1.md](03_px_resid_mom_12_1.md) §2.1과 **같은 값을
쓴다.**

```sql
-- research/etl/features/price.py:88
CASE WHEN model_n_252 >= 252
     THEN log_ret - (alpha_252 + beta_252 * market_ret) END AS resid_ret
```

같은 날 같은 시장의 동일가중 평균수익률을 벤치마크로 252일 rolling 회귀를 돌려 알파·베타를
추정하고, 그 예측을 뺀 나머지다.

### 2.2 "고유"가 무슨 뜻인가

전체 변동성이 아니라 **시장이 설명하지 못하는 부분의 변동성**이다.

종목 수익률은 이렇게 나뉜다.

```
종목 수익률 = 알파 + 베타 × 시장수익률 + 잔차
                              ↑              ↑
                        시장 요인       고유 요인 ← 이 부분의 변동성을 잰다
```

베타가 높아 시장과 함께 크게 움직이는 종목은 전체 변동성이 커도 IVOL은 낮을 수 있다.
**베타 위험과 고유 위험을 분리하는 게 이 피처의 핵심이다.**

이 분리가 중요한 이유는 라벨 때문이다. 라벨이 이미 시장 초과수익률이라
([00_읽는_법.md](00_읽는_법.md) §4.1) 시장 요인이 섞인 지표는 라벨과 축이 어긋난다.

### 2.3 warm-up이 무겁다 — 표본이 짧아지는 이유

세 겹의 조건이 걸린다.

1. 잔차 하나를 만들려면 직전 **252거래일** 회귀 표본이 다 있어야 한다 (`model_n_252 >= 252`).
2. 피처 값을 만들려면 최근 **126거래일 잔차가 전부** 있어야 한다 (`COUNT(...) >= 126`).
3. 표준편차는 그중 최근 **60거래일**로 계산한다.

결국 종목 하나가 값을 가지려면 **연속 378거래일(약 1년 반)의 이력**이 필요하다.

설정에도 같은 값이 있다.

```yaml
# horizon_scan_config.yaml
price:
  market_model_window: 252
  idio_model_min_valid: 126
  idio_vol_window: 60
```

### 2.4 무엇이 NULL이 되는가

```sql
-- research/etl/features/price.py:151
CASE WHEN ca_count_60 > 0 THEN NULL ELSE px_idio_vol_60d END
```

| 조건 | 결과 |
|---|---|
| 60일 창에 기업행동 | NULL |
| 회귀 표본 252개 미만 | 잔차 NULL |
| 126일 창 잔차가 126개 미만 | NULL |
| 상장 1년 반 미만 | 사실상 NULL |

**표본은 2,334일**로 `px_maxret_20d`(2,622일)보다 288일 짧고, 날짜당 종목 수도 1,043개로
55개 적다. 같은 "변동성" 축인데 커버리지가 다르다는 점이 §7의 비교에서 중요해진다.

### 2.5 변형

| variant | 컬럼 | IC (cum 0→20) |
|---|---|---:|
| `native_t` (정본) | `px_idio_vol_60d` | −0.1103 |
| `lag1` | `px_idio_vol_60d_lag1` | −0.1084 |

60일 표준편차라 하루 밀린다고 거의 변하지 않는다. 손실 1.7%.

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 시장모형·잔차 | `research/etl/features/price.py:65`, `:88` |
| 산식 | `research/etl/features/price.py:111` (마스킹 `:151`) |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:251` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘 — 부호가 이론과 반대라는 게 출발점이다

**교과서 이론은 `+`를 예측한다.** 위험이 크면 그 대가로 기대수익이 높아야 한다.

그런데 Ang, Hodrick, Xing & Zhang (2006)이 실제로는 **반대**임을 보고했다. 고유변동성이
높은 종목이 오히려 낮은 수익을 냈다. 이후 "IVOL 퍼즐"로 불린다.

설명은 여러 갈래다.

- **차익거래 제약** — 고평가된 종목을 공매도하려 해도 IVOL이 높으면 위험해서 못 한다.
  그래서 고평가가 오래 유지된다.
- **복권 선호** — 변동성이 큰 종목은 대박 가능성이 커 보여 과대평가된다. 이건
  [05_px_maxret_20d.md](05_px_maxret_20d.md)와 **정확히 같은 설명**이다.
- **저변동성 이례현상** — Baker, Bradley & Wurgler (2011). 기관의 벤치마크 제약 때문에
  저변동 종목이 저평가된 채 남는다.

**둘째 설명이 §7의 중복 문제를 낳는다.** 같은 메커니즘이라면 두 피처가 같은 것을 재고 있을
수 있다.

### 3.2 기대 부호

`−`. 이론이 아니라 **실증 결과를 따라 사전등록했다.** 고유변동성이 클수록 이후 초과수익률
순위가 낮다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_config.yaml:257
primary_horizon_set: [20, 40, 60]
exploratory_horizon_set: [1, 2, 3, 5, 10, 120]
include_bucket_primary: true
```

`px_maxret_20d`와 동일하다. 같은 축이라고 봤다는 뜻이기도 하다.

| | 사전등록 primary | 실제 관측 (`candidate_horizon_band`) |
|---|---|---|
| 밴드 | 20~60일 | **20~60일** |
| 부호 | `−` | `−` |
| onset | (20일 이상 예측) | **20** |

**정확히 맞았다.**

### 3.4 한국 시장 단서

`02_feature_candidate.md` §3.1 P4가 검증 전에 적어 둔 내용이다.

> IVOL 부호는 시장·표본·**MAX 통제**에 민감 → **독립 alpha가 아닌 B등급 risk/quality
> feature**

두 가지가 예고돼 있었다.

| 사전 예고 | 이번 결과 |
|---|---|
| MAX를 통제하면 부호가 흔들릴 수 있다 | **확인 안 함.** A×A 상관도, 통제 회귀도 없다 |
| 독립 alpha가 아니라 risk/quality 성격 | 단변량으로는 최강. **독립성은 미확인** |

**사전 설계는 이 피처를 "B등급 risk feature"로 예상했는데 단변량 결과는 A등급으로 나왔다.**
이 간극이 §7·§8의 핵심이다. 단변량 강도와 증분 기여는 다른 문제다.

분류 좌표는 C1 × T0(수준) × U다.

### 3.5 근거 문헌

Ang, Hodrick, Xing & Zhang (2006), *The Cross-Section of Volatility and Expected Returns*.
Baker, Bradley & Wurgler (2011), *Benchmarks as Limits to Arbitrage*. 등급 A/B.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

부호가 `−`이므로 5분위 차이는 방향 정렬값으로 적는다. 양수면 기대대로다.

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이(정렬) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | −0.1103 | −1.020 | −14.43 | +0.94%p | ~0 | **discovery** |
| cum | 0→40 | −0.1304 | −1.376 | −14.06 | +1.98%p | ~0 | **discovery** |
| cum | 0→60 | **−0.1433** | **−1.642** | −13.25 | **+2.99%p** | ~0 | **discovery** |
| bucket | 10→20 | −0.0811 | −0.718 | −14.00 | +0.41%p | ~0 | **discovery** |
| bucket | 20→40 | −0.0916 | −0.863 | −12.22 | +0.87%p | ~0 | **discovery** |
| bucket | 40→60 | −0.0805 | −0.793 | −11.26 | +0.73%p | ~0 | **discovery** |

- family 최소 q: Phase A **4.38e-46**, 결합 AB **8.94e-46**. `px_reversal_5d` 다음으로 작다.
- **6개 cell 전부 discovery.**
- **|IC| 0.1433은 35개 family 전체 최대값이다.**

### 4.2 ICIR −1.64 — 안정성도 최고

| family | 최대 \|IC\| | 그때 \|ICIR\| |
|---|---:|---:|
| **`px_idio_vol_60d`** | **0.1433** | **1.642** |
| `px_amihud_20d` | 0.1343 | 1.334 |
| `px_maxret_20d` | 0.1133 | 1.309 |
| `px_reversal_5d` | 0.0533 | 0.498 |

IC 평균이 일별 IC의 흔들림보다 1.6배 크다. 35개 중 가장 안정적이다.

### 4.3 IC 1위인데 수익 1위는 아니다

**이 문서에서 가장 중요한 대목이다.**

| family | horizon | \|IC\| | 5분위 차이 |
|---|---|---:|---:|
| `px_idio_vol_60d` | 60일 | **0.1433** (1위) | +2.99%p |
| `px_amihud_20d` | 120일 | 0.1343 (2위) | **+11.21%p** |

|IC|는 이 family가 앞서는데 5분위 수익률 차이는 `px_amihud_20d`가 3.75배 크다.

**주된 이유는 horizon이 다르기 때문이다.** 60거래일과 120거래일은 보유 기간이 두 배 차이다.
누적 수익률이니 긴 쪽이 큰 게 당연하다.

그래서 **두 숫자를 이렇게 읽어야 한다.**

- **IC**는 "이 신호가 순위를 얼마나 잘 맞히나"를 잰다. horizon과 무관하게 비교할 수 있다.
- **5분위 차이**는 "그 신호로 얼마를 벌 수 있나"를 잰다. **horizon이 같을 때만 비교할 수
  있다.**

같은 60일 기준으로 맞춰 비교하려면 `px_amihud_20d`의 60일 cell을 봐야 한다 — IC +0.1085,
5분위 차이 +6.06%p다. 그래도 이 family의 +2.99%p보다 두 배 크다.

**|IC| 1위라는 사실만으로 "가장 좋은 피처"라고 부르면 안 된다.** 보고서가 IC만 보여줬을 때
생기는 오해가 바로 이것이다.

### 4.4 신호의 모양 — `delayed`

| 항목 | 값 | 읽는 법 |
|---|---|---|
| `pattern_auto` | **`delayed`** | 지연 반응형 |
| `onset_h` | **20** | 20일부터 신호가 잡힘 |
| `candidate_horizon_band` | [20, 60] | 후보 구간 |
| `peak_h_cum` | 60 | 누적 최대 |
| `peak_bucket` | [20, 40] | 구간 최대 |
| `half_life_bucket` | 없음 | 60일 안에서 반감점 없음 |
| `sign_flip_bucket` | 없음 | 부호 뒤집힘 없음 |

`px_maxret_20d`와 **onset·peak·pattern이 전부 같다.** 같은 축을 재고 있다는 또 하나의 정황이다.

`half_life`가 없는 건 사전등록 범위가 60일에서 끝나기 때문이다. 120일은 exploratory로 내렸다.

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 5구간 전부

- `valid_subperiods` = 5
- `sign_consistent_subperiods` = **5**

다섯 기간 전부 같은 방향이다.

### 5.2 시간 placebo — 통과, 최솟값

| 항목 | 값 |
|---|---|
| `p_temporal_nw` | **0.0099** |
| `temporal_null_pass` | **true** |

100번의 시간 이동 placebo 중 관측값만큼 극단적인 게 하나도 없었다. `px_maxret_20d`와 같은
최솟값이다.

### 5.3 비중첩 offset — 20개 전부 일치

| 항목 | 값 |
|---|---|
| 총 offset | 20개 (전부 유효) |
| **부호 일치율** | **1.0** |
| 부호 검정 p 중앙값 | **2.0e-16** |
| 부호 검정 p 최댓값 | 4.4e-11 |
| offset IC 범위 | −0.114 ~ −0.106 |

가장 나쁜 offset의 p도 4.4e-11이다. IC 범위도 좁다. 어느 시작점에서 봐도 같다.

### 5.4 거래 가능한 종목만 남겨도 — 오히려 강해진다

| universe | IC (cum 0→20) | 유지율 |
|---|---:|---:|
| `broad` | −0.1103 | — |
| `tradable` | −0.1149 | **1.042** |

cum 0→60에서도 broad −0.1433, tradable −0.1493으로 tradable이 강하다.

**중요한 결과다.** IVOL 효과는 "미세소형주에서만 나온다"는 의심을 자주 받는데 이번 표본에서는
유동성 좋은 종목에서 오히려 강했다.

### 5.5 생존편향 — 오히려 강해진다

| sample_kind | IC (cum 0→20) |
|---|---:|
| `common_survivor` | −0.1103 |
| `available` | **−0.1145** |

상장폐지 종목을 넣으면 더 강하다. 방향이 같으므로 게이트 통과. 고유변동성이 컸다가
상장폐지된 종목은 가설이 예측하는 사례라 자연스럽다.

card에는 `survival_bias_unresolved` 한계가 붙어 있다.

### 5.6 지연

native −0.1103, lag1 −0.1084. `delay_pass`는 `null`이다 — discovery가 전부 10일 이후
구간이라 지연 게이트 대상이 아니다. 수치상 손실은 1.7%로 무시할 수준이다.

### 5.7 시장 구성

KOSPI 42.1% / KOSDAQ 57.9%.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | **2,334일** |
| 날짜당 평균 종목 수 | 1,042개 |
| `available` 기준 | 2,394~2,434일 |

### 가격 계열 표본 비교

| family | 유효 거래일 | 날짜당 종목 | warm-up 요구 |
|---|---:|---:|---|
| `px_reversal_5d` | 2,622 | 1,098 | 5일 |
| `px_maxret_20d` | 2,622 | 1,097 | 20일 |
| `px_near_52w_high` | 2,622 | 1,097 | 252일(부분 창 허용) |
| `px_mom_12_1` | 2,460 | 1,061 | 252일(완전) |
| **`px_idio_vol_60d`** | **2,334** | **1,042** | 252일 회귀 + 126일 완전 |
| `px_resid_mom_12_1` | 2,207 | 1,024 | 252일 회귀 + 232개 완전 |

**`px_maxret_20d`와 288일, 종목 55개 차이가 난다.** §7에서 두 피처를 비교할 때 이 차이를
빼놓으면 안 된다. 같은 종목·같은 날짜를 보고 있는 게 아니다.

---

## 7. 중복성 — 이 family의 가장 큰 미해결 문제

### A×B 교차 상관 — 35개 중 절대값 최대

| 상대 family | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `fin_value_z` | **−0.351** | 1,928 | −0.49 ~ +0.02 |
| `ev_payout_yield` | **−0.347** | 2,175 | −0.43 ~ −0.23 |
| `own_major_filing_activity` | +0.155 | 2,334 | +0.06 ~ +0.26 |
| `own_major_stake_level` | −0.141 | 2,234 | −0.33 ~ −0.02 |
| `ev_amendment_ratio` | +0.140 | 2,334 | +0.01 ~ +0.23 |

**전체 204개 A×B 쌍 중 절대값 1·2위가 이 family에 있다.**

- **`ev_payout_yield`와 −0.347.** 범위가 −0.43~−0.23으로 **전부 음수이고 폭도 좁다.**
  배당·자사주 환원을 하는 회사는 고유변동성이 낮다. 관계가 매우 안정적이다.
- **`fin_value_z`와 −0.351.** 고유변동성이 큰 종목은 밸류가 비싸다. 범위가 −0.49~+0.02로
  거의 전부 음수다.

`|ρ| ≥ 0.7` 경고 기준에는 못 미친다. 다만 **두 관계 모두 방향이 안정적이라 모델에 함께
넣을 때 증분 기여가 줄어들 가능성이 있다.**

### 확인하지 않은 중복 — `px_maxret_20d`와의 관계

**이 문서 전체에서 가장 중요한 공백이다.**

두 피처는 이렇게 겹친다.

| | `px_idio_vol_60d` | `px_maxret_20d` |
|---|---|---|
| 재는 것 | 잔차의 60일 표준편차 | 20일 중 최대 하루 수익률 |
| 경제적 축 | 크게 흔들리는 종목 | 크게 흔들리는 종목 |
| 기대 부호 | `−` | `−` |
| 사전등록 horizon | 20~60일 | 20~60일 |
| `pattern_auto` | `delayed` | `delayed` |
| `onset_h` | 20 | 20 |
| `peak_bucket` | [20, 40] | [20, 40] |
| 기간 일관성 | 5/5 | 5/5 |
| 시간 placebo | 통과 (0.0099) | 통과 (0.0099) |
| `fin_value_z` 상관 | −0.351 | −0.251 |
| `ev_payout_yield` 상관 | −0.347 | −0.241 |

**신호의 모양이 거의 동일하고, 제3의 피처와 맺는 관계의 부호까지 같다.** 서로 다른 정보를
담고 있다고 보기 어렵다.

두 문서가 이 문제를 이미 지적했다.

- `02_feature_candidate.md` §3.1 P4: "IVOL 부호는 시장·표본·**MAX 통제**에 민감"
- `09_all_feature_results.md` §4: "경제적으로 같은 축이라 **둘 다 쓰면 중복**이다. 채택
  단계에서 하나만 고르거나 증분성을 따로 확인해야 한다."

**그런데 A×A 상관을 재지 않았다.** 이번 산출물은 A×B 교차만 담는다
(`13_..._plan.md` §7.2 차트 5).

게다가 §6에서 본 대로 **두 피처는 표본도 다르다** (2,334일 대 2,622일). 상관을 재려면
공통 표본으로 맞춰야 한다.

---

## 8. 한계와 확인 못 한 것

1. **`px_maxret_20d`와의 중복이 미확인이다** (§7). 사전 설계가 명시적으로 요구한
   ablation인데 A×A 상관이 없다. **가장 시급한 후속 작업이다.**
2. **|IC| 1위를 성과 1위로 읽으면 안 된다** (§4.3). horizon이 다르면 5분위 차이는 비교할
   수 없다.
3. **밸류·주주환원과의 관계를 통제하지 않았다** (§7). 단변량 IC라 `fin_value_z`,
   `ev_payout_yield`를 통제한 뒤에도 남는지 모른다. 상관이 −0.35 수준이라 무시할 수 없다.
4. **베타를 통제하지 않았다.** 잔차를 쓰므로 시장 노출은 뺐지만, 베타 자체가 IVOL과 어떻게
   얽히는지는 안 봤다. `px_beta_252d`·`px_downside_beta`는 만들지 않았다.
5. **60일 이후를 안 봤다.** 120일은 exploratory다.
6. **표본이 다른 피처와 다르다** (§6). 비교할 때 공통 표본으로 맞추지 않았다.
7. **업종 중립화가 없다.** 업종별로 고유변동성 수준이 다른데 시장(KOSPI/KOSDAQ) 두 그룹
   안에서만 순위를 매겼다.
8. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
9. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T1

**T1 후보 5개 중 하나로 들어갔다** (`px_reversal_5d`, `px_maxret_20d`, **`px_idio_vol_60d`**,
`flow_individual_netbuy_to_volume_{5,20}d`).

### walk-forward (2026-08-24)

| horizon | baseline Rank IC | candidate Rank IC | baseline 비용반영 spread | candidate 비용반영 spread |
|---|---:|---:|---:|---:|
| 5 | 0.1155 | **0.1202** | −0.00018 | **+0.00075** |
| 20 | 0.1436 | **0.1521** | +0.01258 | +0.01009 |
| 60 | 0.1753 | **0.1840** | +0.02035 | **+0.02073** |

### k=100 비용 확인 (2026-08-12)

| horizon | baseline | candidate | Δ |
|---|---:|---:|---:|
| 20 | +0.01999 | +0.01545 | **−0.00454** |

사전 조건을 통과하지 못해 **묶음 전체가 비채택**이다.

**여기서 §7이 다시 걸린다.** 같은 묶음에 `px_maxret_20d`가 함께 들어 있었다. 두 피처가
같은 정보를 담고 있다면 묶음의 증분이 각각의 단변량 강도만큼 나오지 않는다.

**단변량 |IC| 1위인 피처가 모델 증분에서는 기대만큼 기여하지 못했을 가능성**을 이번 설계로는
확인할 수 없다. 개별 기여도를 측정하지 않았기 때문이다.

T2 14-feature bundle에는 없다 (Phase B 전용 묶음이다).

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
A=f"research/output/horizon_scan/phase=A/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260827T221729-4e0ae8b0"
# IVOL 과 MAX 를 나란히 — 표본 길이 차이도 함께 확인
print(duckdb.sql(f"""
  select family, scan_type, h_start, h_end, ic_mean, icir,
         q5_spread_aligned, n_dates, n_obs_mean
  from '{A}/core/horizon_ic.parquet'
  where family in ('px_idio_vol_60d','px_maxret_20d')
    and universe='broad' and sample_kind='common_survivor'
    and hypothesis_role='primary'
  order by scan_type, h_end, family
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| cell 전체 | `phase=A/…/run_id=20260827T221729-4e0ae8b0/core/horizon_ic.parquet` |
| 강건성·offset 20개 | 같은 run의 `cards/family_cards.json` |
| 차트 7종 | 같은 run의 `plots/px_idio_vol_60d_*.png` |
| 시간 placebo 상세 | 같은 run의 `core/permutation_cell_stats.parquet` |
| 잔차 산식 | `research/etl/features/price.py:65`, `:88` |
| MAX 통제 경고 | `01_feature_candidate/02_feature_candidate.md` §3.1 P4 |
| 중복 지적 | `01_feature_candidate/09_all_feature_results.md` §4 |
| T1 결과 | `docs/target/01_20_access_return_rank/grade_a_acceptance_gate_results.json`, `topk_cost_check.json` |
