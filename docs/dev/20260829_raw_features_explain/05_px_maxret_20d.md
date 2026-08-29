# 05. `px_maxret_20d` — 20일 최대 일간수익률 (복권주 지표)

- 작성일: 2026-08-29
- family: `px_maxret_20d` · primary feature: `px_maxret_20d` · domain: `price`
- Phase A · fdr_family `price` · 기대 부호 `−` · 관측 부호 `−`
- 등급 **A** · `screen_pass` 통과 · discovery **6/6 cell 전부**
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**최근 20일 안에 하루 크게 튄 적이 있는 종목은 이후 20~60일 동안 시장 대비 부진했다.**
검증한 35개 family 중 **강건성 검사를 가장 깨끗하게 통과한 피처**다.

- 사전등록 6개 cell **전부 discovery** (q = 1.8e-41)
- 5기간 **전부 같은 방향**
- 비중첩 offset 20개 **전부 방향 일치** (p 중앙값 5.2e-14)
- **시간 placebo 통과** (p = 0.0099, 가능한 최솟값)
- 유동성 좋은 종목에서 **오히려 더 강함** (유지율 1.019)

60일 보유 기준 5분위 수익률 차이는 **+1.79%p**다. `px_reversal_5d`(5일 0.38%p)보다 회전이
훨씬 느리면서 크기는 크다. 실행 측면에서 중요한 차이다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/price.py:110
MAX(simple_ret) OVER w20 AS px_maxret_20d

-- w20 = PARTITION BY ticker, market ORDER BY trade_date
--       ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
```

**최근 20거래일 중 가장 높았던 하루 수익률**이다.

### 2.2 로그수익률이 아니라 단순수익률이다

다른 가격 피처들이 `log_ret`을 쓰는 것과 달리 이 피처만 **`simple_ret`**을 쓴다.

```sql
-- research/etl/trading_panel.py:23
close / LAG(close) - 1  AS simple_ret     -- 이 피처가 쓰는 값
LN(close / LAG(close))  AS log_ret        -- 나머지 가격 피처가 쓰는 값
```

의도적인 선택이다. "그날 몇 % 올랐나"를 재는 지표이므로 사람이 체감하는 단위와 맞춰야 한다.
큰 수익률에서 두 값이 벌어진다 — 30% 상승이면 단순수익률 0.30, 로그수익률 0.262다.
**최댓값을 재는 지표라 이 차이가 그대로 값에 들어간다.**

### 2.3 상하한가에 값이 뭉친다 — 이 피처의 구조적 특징

한국 시장에는 일간 가격제한폭이 있다. 설정에 그대로 기록돼 있다.

```yaml
# horizon_scan_config.yaml
quality:
  price_limit_regimes:
    - {start: 2014-06-01, end: 2015-06-14, limit: 0.15}
    - {start: 2015-06-15, end: null,       limit: 0.30}
```

즉 `simple_ret`의 최댓값이 **2015년 6월 15일 이전에는 +15%, 이후에는 +30%로 잘린다.**

두 가지 결과가 따라온다.

1. **상한가를 친 종목들의 값이 한 점에 뭉친다.** 상한가는 상한가일 뿐 그 이상을 구분하지
   못한다. 횡단면 순위를 매길 때 동점이 대량으로 생긴다. Spearman 상관은 평균 순위로
   동점을 처리하므로(`metrics.py:436` `_rankdata`) 계산은 되지만, **분해능이 그만큼
   떨어진다.**
2. **2015년 6월을 기준으로 값의 척도가 바뀐다.** 앞 구간은 상한이 0.15, 뒤는 0.30이다.
   횡단면 순위만 쓰므로 IC 계산 자체는 영향이 적지만, 표본 앞부분(2014-06~2015-06)은
   구조가 다르다는 점을 알고 봐야 한다.

`02_feature_candidate.md` §3.1 P5가 검증 전에 이 문제를 지적하고 `px_limit_up_count_20d`와
비교한 뒤 대표 신호 하나만 고르라고 적어 뒀다. **그 비교는 이번에 하지 않았다** (§8).

### 2.4 무엇이 NULL이 되는가

```sql
-- research/etl/features/price.py:150
CASE WHEN ca_count_20 > 0 THEN NULL ELSE px_maxret_20d END
```

20일 창에 기업행동이 있으면 버린다. 무상증자·액면분할이 만든 가격 점프를 "크게 튄 하루"로
오인하지 않으려는 장치다. **이 피처에서 특히 중요한 마스킹이다.**

표본은 2,622일로 `px_reversal_5d`·`px_near_52w_high`와 같다. 20일 창이라 손실이 작다.

### 2.5 변형

| variant | 컬럼 | IC (cum 0→20) |
|---|---|---:|
| `native_t` (정본) | `px_maxret_20d` | −0.0896 |
| `lag1` | `px_maxret_20d_lag1` | −0.0880 |

거의 차이가 없다. 20일 창의 최댓값이라 하루 밀린다고 값이 크게 변하지 않는다.

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/price.py:110` (마스킹 `:150`) |
| 단순수익률 정의 | `research/etl/trading_panel.py:23` |
| 가격제한 설정 | `research/analysis/horizon_scan_config.yaml:6` |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:238` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**복권 선호(lottery preference)다.**

Bali, Cakici & Whitelaw (2011)의 설명은 이렇다. 일부 투자자는 "크게 오를 수도 있다"는
가능성 자체에 값을 지불한다. 복권을 사는 심리와 같다. 최근에 하루 크게 튄 종목은 그런
기대를 자극하므로 **과대평가되고, 결과적으로 이후 수익률이 낮다.**

위험 프리미엄이 아니라 **행태 편향에 기반한 과대평가 가설**이다. 그래서 부호가 음수다.
위험이 커서 수익이 높은 게 아니라, 사람들이 좋아해서 비싸진 것이다.

### 3.2 기대 부호

`−`. 최근 최대 일간수익률이 클수록 이후 초과수익률 순위가 **낮다**.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_config.yaml:244
primary_horizon_set: [20, 40, 60]
exploratory_horizon_set: [1, 2, 3, 5, 10, 120]
include_bucket_primary: true
```

과대평가가 풀리는 데는 몇 주에서 몇 달이 걸린다고 봤다. 며칠 단위(1~10일)와 120일은
exploratory로 내렸다.

| | 사전등록 primary | 실제 관측 (`candidate_horizon_band`) |
|---|---|---|
| 밴드 | 20~60일 | **20~60일** |
| 부호 | `−` | `−` |

**정확히 맞았다.** 게다가 `onset_h = 20`으로 시작점이 사전등록 하한과 정확히 일치한다.

### 3.4 한국 시장 단서

`02_feature_candidate.md` §3.1 P5가 적어 둔 내용이다.

> 개인 비중 높은 한국·대만에서 강함. 상하한가 ±30%로 MAX가 상한가에 뭉침 →
> `px_limit_up_count_20d`와 비교 후 **대표 신호 1개만** 채택. IVOL과 중복 ablation

세 가지가 예고돼 있었고 각각의 결과는 이렇다.

| 사전 예고 | 이번 결과 |
|---|---|
| 한국에서 강할 것 | **맞았다.** IC −0.113으로 35개 중 상위권 |
| 상한가 뭉침 문제 | **확인 안 함.** `px_limit_up_count_20d`를 만들지 않았다 |
| IVOL과 중복 | **확인 안 함.** A×A 상관이 없다 (§7) |

개인 투자자 비중이 높은 시장에서 복권 선호가 강하게 나타난다는 예측은 그대로 재현됐다.

분류 좌표는 C1 × T0(수준) × U다.

### 3.5 근거 문헌

Bali, Cakici & Whitelaw (2011), *Maxing Out: Stocks as Lotteries and the Cross-Section
of Expected Returns*. 등급 A/B.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

부호가 `−`이므로 5분위 차이는 **방향 정렬값**(`q5_spread_aligned`)으로 적는다. 양수면
기대대로다.

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이(정렬) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | −0.0896 | −0.930 | −13.64 | +0.44%p | ~0 | **discovery** |
| cum | 0→40 | −0.1032 | −1.121 | −11.90 | +1.16%p | ~0 | **discovery** |
| cum | 0→60 | **−0.1133** | **−1.309** | −11.15 | **+1.79%p** | ~0 | **discovery** |
| bucket | 10→20 | −0.0643 | −0.640 | −13.11 | +0.21%p | ~0 | **discovery** |
| bucket | 20→40 | −0.0710 | −0.709 | −10.53 | +0.56%p | ~0 | **discovery** |
| bucket | 40→60 | −0.0623 | −0.666 | −9.98 | +0.40%p | ~0 | **discovery** |

- family 최소 q: Phase A **1.83e-41**, 결합 AB **3.73e-41**. 35개 중 세 번째로 작다
  (`px_reversal_5d`, `px_idio_vol_60d` 다음).
- **사전등록 6개가 전부 통과했다.** 일부만 통과한 다른 family들과 다르다.

### 4.2 ICIR이 1을 넘는다

**35개 family 중 |ICIR| > 1을 넘긴 몇 안 되는 경우다.**

| cell | IC | ICIR |
|---|---:|---:|
| cum 0→40 | −0.1032 | **−1.121** |
| cum 0→60 | −0.1133 | **−1.309** |

ICIR은 일별 IC의 평균을 그 변동성으로 나눈 값이다. 1을 넘는다는 건 **IC의 평균이 그
흔들림보다 크다**는 뜻이다. 참고로 `px_reversal_5d`는 0.41~0.50, `px_near_52w_high`는
0.10~0.28이다.

IC 평균만 보면 `px_reversal_5d`(0.053)와 `px_maxret_20d`(0.113)의 차이는 두 배지만,
**안정성까지 보면 격차가 훨씬 크다.**

### 4.3 IC를 수익률로 읽으면

| horizon | 5분위 차이 | 기간당 환산 |
|---|---:|---|
| 20일 | +0.44%p | 약 1개월 |
| 40일 | +1.16%p | 약 2개월 |
| 60일 | **+1.79%p** | 약 3개월 |

60거래일에 1.79%p는 연율로 대략 7%대다. **거래비용 차감 전**이고 창이 매일 겹치는 측정이라
그대로 운용 성과가 되지는 않는다.

다만 `px_reversal_5d`와 비교하면 실행 관점의 차이가 분명하다.

| | `px_reversal_5d` | `px_maxret_20d` |
|---|---|---|
| 대표 보유기간 | 5일 | 60일 |
| 5분위 차이 | +0.38%p | **+1.79%p** |
| 연 리밸런싱 횟수(대략) | 50회 | 4회 |

**회전이 12분의 1인데 회당 수익은 5배다.** 거래비용을 감안하면 격차가 더 벌어진다.
이 대비는 IC만 보던 보고서에서는 보이지 않던 것이다.

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

**`px_near_52w_high`와 같은 `delayed` 형태인데 다른 점이 있다.** 이쪽은 누적 cell도 전부
discovery다. 구간 신호가 20~60일에 고르게 퍼져 있어 누적해도 희석되지 않았다.

`half_life_bucket`이 없는 건 효과가 영원해서가 아니라 **사전등록 범위가 60일에서 끝나기
때문이다.** 120일은 exploratory로 내렸다.

---

## 5. 진짜인가 — 강건성

**35개 중 가장 깨끗하다.**

### 5.1 기간 일관성 — 5구간 전부

- `valid_subperiods` = 5
- `sign_consistent_subperiods` = **5**

다섯 기간 **전부에서 같은 방향**이었다. 2014~2016, 2017~2019, 2020~2021, 2022~2023.10,
2023.11~ 어디서도 뒤집히지 않았다.

### 5.2 시간 placebo — 통과, 그것도 최솟값

| 항목 | 값 |
|---|---|
| `p_temporal_nw` | **0.0099** |
| `temporal_null_pass` | **true** |

기준은 0.10이다. 0.0099는 **100번의 시간 이동 placebo 중 관측값만큼 극단적인 게 하나도
없었다**는 뜻이다 (계산식이 `(1 + 0) / 101`이므로 이게 최솟값이다).

`px_mom_12_1`(0.297), `px_resid_mom_12_1`(0.614), `px_near_52w_high`(0.772)와 비교하면
차이가 확연하다. **이 검사를 실제로 통과한 가격 피처다.**

### 5.3 비중첩 offset — 20개 전부 일치

| 항목 | 값 |
|---|---|
| 총 offset | 20개 (전부 유효) |
| **부호 일치율** | **1.0** |
| 부호 검정 p 중앙값 | **5.2e-14** |
| 부호 검정 p 최댓값 | 1.1e-10 |
| offset IC 범위 | −0.094 ~ −0.085 |

창이 겹치지 않게 나눠 검정해도 **20개 전부 통과했고, 가장 나쁜 offset의 p도 1.1e-10**이다.
IC 범위도 −0.094~−0.085로 좁다. 어느 시작점에서 보든 같은 결과가 나온다.

`px_mom_12_1`(120개 offset, 일치율 0.0, p 중앙값 0.868)과 정반대다.

### 5.4 거래 가능한 종목만 남겨도 — 오히려 강해진다

| universe | IC (cum 0→20) | 유지율 |
|---|---:|---:|
| `broad` | −0.0896 | — |
| `tradable` | −0.0913 | **1.019** |

유지율이 1을 넘는다. 거래대금 1억원 이상, 종가 1,000원 이상으로 좁혀도 신호가 유지된다.

**중요한 결과다.** 복권주 효과는 흔히 "동전주에서만 나타나는 현상"으로 의심받는데, 이번
표본에서는 그렇지 않았다. cum 0→60에서도 broad −0.1133 대 tradable −0.1135로 같다.

### 5.5 생존편향 — 오히려 강해진다

| sample_kind | IC (cum 0→20) |
|---|---:|
| `common_survivor` | −0.0896 |
| `available` | **−0.0931** |

상장폐지 종목을 포함하면 더 강하다. 방향이 같으므로 게이트 통과
(`attrition_warning = false`).

해석은 자연스럽다. 크게 튀었다가 상장폐지된 종목은 이 가설이 예측하는 바로 그 사례다.
빼면 신호가 약해지는 게 맞다. **생존편향이 결과를 부풀린 게 아니다.**

card에는 `survival_bias_unresolved` 한계가 여전히 붙어 있다.

### 5.6 지연

| variant | IC |
|---|---:|
| `native_t` | −0.0896 |
| `lag1` | −0.0880 |

`delay_pass`는 `null`이다. 지연 게이트는 h ≤ 5인 짧은 cell에만 적용하는데 이 family의
discovery는 전부 10일 이후 구간이라 대상이 아니다.

수치상 하루 지연 손실은 1.8%에 불과하다. 20일 창의 최댓값이라 하루로는 잘 안 변한다.
**실행 여유가 큰 신호다.**

### 5.7 시장 구성

KOSPI 41.2% / KOSDAQ 58.8%. 다른 가격 피처와 같다.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | **2,622일** |
| 날짜당 평균 종목 수 | 1,096~1,098개 |
| `available` 기준 | 2,682~2,722일 |

가격 계열 최장 표본이다. 20일 마스킹 창이라 손실이 작다.

---

## 7. 중복성

### A×B 교차 상관

| 상대 family | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `fin_value_z` | **−0.251** | 1,928 | −0.44 ~ −0.06 |
| `ev_payout_yield` | **−0.241** | 2,175 | −0.37 ~ −0.09 |
| `fin_log_mcap` | +0.118 | 2,392 | −0.05 ~ +0.46 |
| `own_major_filing_activity` | +0.103 | 2,517 | −0.03 ~ +0.21 |

**35개 family 중 A×B 교차 상관 절대값이 가장 큰 축이다.**

- **`fin_value_z`와 −0.251.** 크게 튀는 종목은 밸류가 비싸다. 범위가 −0.44~−0.06으로
  전부 음수라 **날짜와 무관하게 안정적으로 겹친다.** 복권주 효과와 밸류 효과가 상당 부분
  같은 종목군을 가리킨다는 뜻이다.
- **`ev_payout_yield`와 −0.241.** 배당·자사주 환원이 적은 회사가 크게 튄다. 이것도 범위가
  전부 음수다.

`|ρ| ≥ 0.7` 경고 기준에는 못 미치므로 공식 중복 판정 대상은 아니다. 다만 **두 관계 모두
방향이 안정적이라 모델에 함께 넣을 때 증분 기여가 줄어들 가능성이 있다.**

### 확인하지 않은 중복 — 이 family에서 가장 큰 공백

`09_all_feature_results.md` §4가 직접 지적했다.

> 위 특이변동성과 경제적으로 같은 축이라 **둘 다 쓰면 중복**이다. 채택 단계에서 하나만
> 고르거나 증분성을 따로 확인해야 한다.

[06_px_idio_vol_60d.md](06_px_idio_vol_60d.md)와 이 family는 **"크게 흔들리는 종목"이라는
같은 축**을 다른 방식으로 잰다. 하나는 최댓값 하나로, 하나는 잔차의 표준편차로.

그런데 **A×A 상관을 재지 않았다.** 학계에서도 MAX와 IVOL의 관계는 오래된 쟁점이다
(`02_feature_candidate.md` §3.1 P4: "IVOL 부호는 시장·표본·MAX 통제에 민감"). 이 둘을
분리하지 않으면 어느 쪽이 진짜인지 말할 수 없다.

---

## 8. 한계와 확인 못 한 것

1. **IVOL과의 중복을 확인하지 않았다** (§7). 사전 설계가 명시적으로 요구한 ablation인데
   A×A 상관 산출물이 없다. **이 family에서 가장 시급한 후속 작업이다.**
2. **상한가 뭉침 문제를 확인하지 않았다** (§2.3). `px_limit_up_count_20d`를 만들어
   비교하라는 사전 지시를 따르지 않았다. 신호의 얼마만큼이 "상한가를 쳤다"는 사실 하나로
   설명되는지 모른다.
3. **2015년 6월 제도 변경 전후를 나눠 보지 않았다.** 가격제한폭이 15%에서 30%로 바뀐
   지점인데 별도 검사가 없다. 기간 일관성 5/5는 이 경계를 가로지르는 구간 구분이 아니다.
4. **밸류·주주환원과의 관계를 통제하지 않았다** (§7). 단변량 IC라 `fin_value_z`를 통제한
   뒤에도 남는지 모른다.
5. **거래비용 반영 성과가 없다.** 다만 회전이 느려 `px_reversal_5d`보다는 여유가 있다.
6. **60일 이후를 안 봤다.** 120일은 exploratory로 내렸다. `half_life`가 없는 건 그 때문이다.
7. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
8. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T1

**T1 후보 5개 중 하나로 들어갔다** (`07_phase1_acceptance_gate.md` §1:
`px_reversal_5d`, `px_maxret_20d`, `px_idio_vol_60d`,
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

사전 조건 `Δ(h20) > 0`을 통과하지 못해 **묶음 전체가 비채택**이다.

**주의.** 이건 5개 묶음의 결과이고 `px_maxret_20d` 개별 기여도는 측정하지 않았다. 단변량
근거만 보면 이 family가 5개 중 가장 강하지만(§5), 묶음 안에서 다른 피처와 겹쳐 증분이
작았을 수도 있다. **§7의 IVOL 중복 문제가 바로 그 의심의 근거다** — 같은 묶음에
`px_idio_vol_60d`가 함께 들어 있었다.

T2 14-feature bundle에는 없다 (Phase B 전용 묶음이다).

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
A=f"research/output/horizon_scan/phase=A/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260827T221729-4e0ae8b0"
print(duckdb.sql(f"""
  select scan_type, h_start, h_end, universe, sample_kind, ic_mean, icir, t_nw,
         q5_spread_aligned, q_fdr_global, primary_discovery, n_dates
  from '{A}/core/horizon_ic.parquet'
  where family='px_maxret_20d' and hypothesis_role='primary'
  order by universe, sample_kind, scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| cell 전체 | `phase=A/…/run_id=20260827T221729-4e0ae8b0/core/horizon_ic.parquet` |
| 강건성·offset 20개 | 같은 run의 `cards/family_cards.json` |
| 차트 7종 | 같은 run의 `plots/px_maxret_20d_*.png` |
| 시간 placebo 상세 | 같은 run의 `core/permutation_cell_stats.parquet` |
| 가격제한 설정 | `research/analysis/horizon_scan_config.yaml:6` |
| 사전 경고 (상한가 뭉침) | `01_feature_candidate/02_feature_candidate.md` §3.1 P5 |
| IVOL 중복 지적 | `01_feature_candidate/09_all_feature_results.md` §4 |
| T1 결과 | `docs/target/01_20_access_return_rank/grade_a_acceptance_gate_results.json`, `topk_cost_check.json` |
