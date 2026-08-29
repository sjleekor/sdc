# 08. `px_turnover_shock` — 회전율 충격 (거래량 급증)

- 작성일: 2026-08-29
- family: `px_turnover_shock` · primary feature: `px_turnover_shock` · domain: `price`
- Phase A · fdr_family `price` · 기대 부호 `+` · **관측 부호 `−`**
- 등급 **D** · `screen_pass` 실패 · discovery 0/6 cell
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**기대와 정반대인데, 그 반대 방향이 매우 안정적이다.** 거래가 갑자기 몰린 종목이 이후
5~20일 동안 더 오른 게 아니라 **덜 올랐다** (0→20일 IC −0.0264, q = 3.2e-11).

**이 family가 다른 D등급과 결정적으로 다른 점이 여기다.**

| | `px_mom_12_1` | **`px_turnover_shock`** |
|---|---|---|
| 기대 부호 대비 | 반대 | 반대 |
| 기대 방향 일치 구간 | 2/5 | **0/5** |
| 실제 의미 | 방향이 뒤죽박죽 | **5구간 전부 반대 방향으로 일치** |
| 최소 BH q | 0.059 | **3.2e-11** |
| tradable 유지율 | 1.023 | **1.268** |

`px_mom_12_1`의 2/5는 "부호가 오락가락한다"는 뜻이지만, 이 family의 **0/5는 "다섯 구간
전부에서 기대와 반대였다"**는 뜻이다. 반대 방향이 오히려 완벽하게 일관됐다.

**그래도 discovery로 세지 않는다.** 사전등록한 부호가 `+`이기 때문이다. 반대 방향을 신호로
쓰려면 새 config로 다시 사전등록해야 한다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/price.py:114
LN(NULLIF(turnover, 0) / NULLIF(
    QUANTILE_CONT(turnover, 0.5) OVER (
        PARTITION BY ticker, market ORDER BY trade_date
        ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
    ), 0)) AS px_turnover_shock
```

**오늘 거래대금을 직전 60거래일 거래대금 중앙값으로 나눈 뒤 로그를 취한 값**이다.

- 값이 0이면 평소와 같은 거래량
- 값이 0.69(= ln 2)면 평소의 두 배
- 값이 −0.69면 평소의 절반

### 2.2 세 가지 설계 선택

**첫째, 수준이 아니라 변화를 잰다.**

`02_feature_candidate.md` §3.1 P7이 명시적으로 금지한 게 있다.

> raw turnover level은 가격·규모와 섞임 → **단독 입력 금지**

거래대금 그 자체는 시가총액과 거의 같은 값이 된다
([07_px_amihud_20d.md](07_px_amihud_20d.md) §7이 그 문제를 보여 준다). 그래서 이 피처는
**같은 종목의 과거 자신과 비교**한다. 분모가 그 종목의 60일 중앙값이므로 규모가 상쇄된다.

실제로 §7에서 보듯 규모와의 상관이 +0.048에 그친다. `px_amihud_20d`의 −0.754와 대조된다.
**설계 의도가 그대로 작동했다.**

**둘째, 평균이 아니라 중앙값으로 나눈다.**

`QUANTILE_CONT(turnover, 0.5)`다. 평균을 쓰면 과거 창 안의 한 번의 거래 폭발이 분모를
끌어올려 이후 60일 내내 값을 눌러 버린다. 중앙값은 그 영향을 받지 않는다.

**셋째, 당일을 분모에서 뺀다.**

창이 `ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING`이다. 오늘은 분자에만 들어간다.

```yaml
# horizon_scan_config.yaml
price:
  turnover_shock_window: 60
  turnover_shock_include_current: false
```

당일을 분모에 넣으면 급증한 날의 값이 스스로 분모를 키워 신호가 희석된다.

### 2.3 거래대금이 근사값이다

`turnover`는 `종가 × 거래량`이다 (`trading_panel.py:21`). 실제 체결대금이 아니다.
[07_px_amihud_20d.md](07_px_amihud_20d.md) §2.2와 같은 한계다.

다만 이 피처는 **같은 종목의 과거와 비교하는 비율**이라 근사 오차가 상당 부분 상쇄된다.
Amihud처럼 절대 수준을 쓰는 지표보다 영향이 작다.

### 2.4 무엇이 NULL이 되는가

```sql
-- research/etl/features/price.py:152
CASE WHEN ca_count_60_prior > 0 THEN NULL ELSE px_turnover_shock END
```

**직전 60거래일(당일 제외)에 기업행동이 있으면 버린다.** 마스킹 창이 분모 창과 정확히
일치한다 — `w60_prior`가 둘 다 `60 PRECEDING AND 1 PRECEDING`이다.

거래량이 없는 날(`volume = 0`)은 `NULLIF(turnover, 0)`으로 NULL이 된다.

표본은 **2,622일**로 가격 계열 최장이다. 날짜당 종목 수는 1,090개다.

### 2.5 변형

| variant | 컬럼 | IC (cum 0→20) |
|---|---|---:|
| `native_t` (정본) | `px_turnover_shock` | −0.0264 |
| `lag1` | `px_turnover_shock_lag1` | −0.0298 |

**`delay_pass = true`다.** 이 family는 discovery cell에 h ≤ 5가 포함돼 지연 게이트가 실제로
적용됐고, 통과했다. lag1이 오히려 조금 더 강하다.

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/price.py:114` (마스킹 `:152`) |
| 거래대금 근사 | `research/etl/trading_panel.py:21` |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:277` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**주목(attention)이 가격을 밀어 올린다는 가설이다.**

Gervais, Kaniel & Mingelgrin (2001)의 "high-volume return premium"이다. 거래량이 갑자기
늘면 그 종목이 투자자 눈에 띄고, 새로 들어오는 매수세가 가격을 밀어 올린다. 효과는
**단기(1~4주)**에 나타난다.

행태 편향 설명이라 **주목이 식으면 사라진다**는 예측이 따라온다.

### 3.2 기대 부호

`+`. 거래가 평소보다 많이 몰릴수록 이후 초과수익률 순위가 높다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_config.yaml:283
primary_horizon_set: [5, 10, 20]
exploratory_horizon_set: [1, 2, 3, 40, 60, 120]
```

**가격 피처 중 가장 짧은 primary 밴드다.** 주목 효과가 몇 주 안에 끝난다고 봤다.
40일 이후는 전부 exploratory로 내렸다.

| | 사전등록 primary | 실제 관측 |
|---|---|---|
| 밴드 | 5~20일 | **없음** (`candidate_horizon_band = null`) |
| 부호 | `+` | `−` |
| 패턴 | 단기 상승 | `no_signal` |

**부호가 반대라 후보 밴드가 잡히지 않았다.**

### 3.4 한국 시장 단서

`02_feature_candidate.md` §3.1 P8이 적어 둔 내용이다.

> 단기(1~4주) 예측용. `px_mom_x_volume = rank(mom_12_1)×rank(volume_trend)` interaction
> 후보 (Lee & Swaminathan 2000)

**상호작용 후보로 지정돼 있었는데 만들지 않았다.** 이번 scan은 단변량만 본다
(`11_feature_taxonomy.md` §2.3: 조건부 피처 0개).

Lee & Swaminathan (2000)의 원래 발견은 **거래량과 모멘텀의 상호작용**이다. 거래량이 많은
승자와 적은 승자가 다르게 움직인다는 것이다. 단변량으로는 그 구조를 잡을 수 없다.

분류 좌표는 C1 × **T2(놀라움)** × U다. 35개 중 T2로 분류된 셋 중 하나다
(나머지는 `fin_sue`, `px_turnover_shock`와 함께 묶인 항목).

### 3.5 근거 문헌

Gervais, Kaniel & Mingelgrin (2001), *The High-Volume Return Premium*.
Lee & Swaminathan (2000), *Price Momentum and Trading Volume*.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 | BH q | 부호 | 판정 |
|---|---|---:|---:|---:|---:|---:|---|---|
| cum | 0→5 | −0.0185 | −0.264 | −7.79 | **+0.23%p** | ~0 | 반대 | BH 통과, discovery 아님 |
| cum | 0→10 | −0.0199 | −0.297 | −6.51 | **+0.26%p** | ~0 | 반대 | BH 통과, discovery 아님 |
| cum | 0→20 | **−0.0264** | −0.416 | −6.74 | **+0.13%p** | ~0 | 반대 | BH 통과, discovery 아님 |
| bucket | 0→5 | −0.0185 | −0.264 | −7.79 | +0.23%p | ~0 | 반대 | BH 통과, discovery 아님 |
| bucket | 5→10 | −0.0163 | −0.260 | −7.75 | +0.04%p | ~0 | 반대 | BH 통과, discovery 아님 |
| bucket | 10→20 | −0.0216 | −0.369 | −8.17 | **−0.12%p** | ~0 | 반대 | BH 통과, discovery 아님 |

- family 최소 q: Phase A **3.18e-11**. **D등급 중 압도적으로 작다.**
- **6개 cell 전부 BH를 통과했다.** t값도 −6.5~−8.2로 크다.
- 그런데 `expected_sign_pass = false`이므로 discovery는 0개다.

### 4.2 IC와 5분위 차이의 부호가 어긋난다

**여섯 cell 중 다섯에서 IC는 음수인데 5분위 차이는 양수다.**

| cell | Rank IC | 5분위 차이 |
|---|---:|---:|
| cum 0→5 | −0.0185 | **+0.23%p** |
| cum 0→10 | −0.0199 | **+0.26%p** |
| cum 0→20 | −0.0264 | **+0.13%p** |
| bucket 10→20 | −0.0216 | −0.12%p |

[04_px_near_52w_high.md](04_px_near_52w_high.md) §4.3과 같은 종류의 어긋남인데 **방향이
반대**다. 저쪽은 IC 양수·spread 음수였고, 여기는 IC 음수·spread 양수다.

원인도 같은 구조다. IC는 순위 상관이라 횡단면 전체를 보고, 5분위 차이는 양 끝 **평균**이라
극단값에 끌려간다.

이 피처에서 상위 20%는 **거래가 폭발한 종목**이다. 대부분은 이후 부진하지만(그래서 IC가
음수), 일부가 크게 오르면서 평균을 끌어올린다. 거래 폭발은 대형 호재 때도 일어나기 때문이다.

정리하면 이렇다.

> **순위로 보면 거래 급증 종목이 이후 나쁘다(IC −). 그런데 상위 20% 평균만 보면 소수의
> 대박이 그 손해를 덮는다(spread +).**

**이 어긋남을 풀 자료가 없다.** 중앙값 기반 spread나 분위별 평균수익률을 산출하지 않았다.

### 4.3 신호의 모양

| 항목 | 값 |
|---|---|
| `pattern_auto` | `no_signal` |
| `candidate_horizon_band` | 없음 |
| `onset_h` | 없음 |
| `peak_h_cum` | 20 (음수 방향 최대) |
| `peak_bucket` | [5, 10] |
| `half_life_bucket` | 없음 |
| `sign_flip_bucket` | 없음 |

`sign_flip_bucket`이 없다는 게 §5.1과 이어진다. **관측 구간 전체에서 부호가 한 번도
뒤집히지 않았다.**

---

## 5. 진짜인가 — 강건성

**여기가 이 문서의 핵심이다. 반대 방향이 매우 안정적이다.**

### 5.1 기간 일관성 — 기대 방향 0/5 = 반대 방향 5/5

- `valid_subperiods` = 5
- `sign_consistent_subperiods` = **0**

`sign_consistent_subperiods`는 **기대 부호와 일치한 구간 수**다
(`horizon_scan_runner.py:945`: `sign = -1.0 if expected_sign == "-" else 1.0`,
`consistent = sum(1 for ic in valid if sign * ic > 0)`).

기대 부호가 `+`인데 0이라는 건 **다섯 구간 전부에서 IC가 음수였다**는 뜻이다.

**이 숫자를 "가장 불안정하다"로 읽으면 정반대로 오해하게 된다.**

| family | 기대 | 일치 구간 | 실제 의미 |
|---|---|---:|---|
| `px_reversal_5d` | + | 5/5 | 기대 방향으로 완벽하게 일관 |
| `px_mom_12_1` | + | 2/5 | **방향이 오락가락** |
| **`px_turnover_shock`** | + | **0/5** | **반대 방향으로 완벽하게 일관** |

0과 5는 둘 다 "일관적"이고, 중간값인 2~3이 "불안정"이다.

### 5.2 비중첩 offset — 20개 전부, 반대 방향으로

| 항목 | 값 |
|---|---|
| 총 offset | 20개 (전부 유효) |
| 기대 방향 부호 일치율 | **0.0** |
| 부호 검정 p 중앙값 | **0.9996** |
| 부호 검정 p 최솟값 | 0.973 |
| offset IC 범위 | −0.034 ~ −0.020 |

**p가 1에 붙어 있다는 게 핵심이다.** 부호 검정이 기대 방향(`+`) 기준 단측이므로, p가
1에 가깝다는 건 **반대 방향으로 강하게 유의하다**는 뜻이다.

`px_mom_12_1`의 p 중앙값 0.868과 비교하면 차이가 있다. 저쪽은 "방향이 불분명"에 가깝고
여기는 "반대 방향이 확실"에 가깝다. offset IC 범위도 −0.034~−0.020으로 전부 음수이고
폭이 좁다.

### 5.3 거래 가능한 종목만 남기면 — 27% 더 강해진다

| universe | IC (cum 0→20) | 유지율 |
|---|---:|---:|
| `broad` | −0.0264 | — |
| `tradable` | −0.0335 | **1.268** |

**35개 family 중 유지율이 가장 높다.** 유동성 좋은 종목에서 반대 신호가 훨씬 강하다.

소형주·동전주가 만든 착시라는 설명을 배제한다. 오히려 **거래가 활발한 종목에서 "거래 급증
뒤 부진"이 뚜렷하다.**

### 5.4 생존편향

| sample_kind | IC (cum 0→20) |
|---|---:|
| `common_survivor` | −0.0264 |
| `available` | −0.0271 |

차이 없음. `attrition_warning = false`. card에 `limitations`도 비어 있다 — 가격 계열 중
드물게 `survival_bias_unresolved`가 붙지 않았다.

### 5.5 지연 — 실제로 검사했고 통과했다

| variant | IC (cum 0→20) |
|---|---:|
| `native_t` | −0.0264 |
| `lag1` | −0.0298 |

**`delay_pass = true`.** 이 family는 primary cell에 h ≤ 5(`cum 0→5`, `bucket 0→5`)가 있어
지연 게이트가 실제로 적용됐다. 앞선 A등급 가격 피처들이 전부 `null`(대상 아님)이었던 것과
다르다.

하루 늦춰도 신호가 오히려 강해진다. 실행 여유가 있다.

### 5.6 시간 placebo — 대상이 아니다

`p_temporal_nw`, `temporal_null_pass` 모두 `null`이다. **검사에서 떨어진 게 아니다.**
사전등록 최대 horizon이 20일이라 NW lag가 19에 그쳐 기준 59에 못 미친다. 긴 cell이 없어
placebo를 돌릴 대상이 없었다.

### 5.7 시장 구성

KOSPI 41.4% / KOSDAQ 58.6%.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | **2,622일** |
| 날짜당 평균 종목 수 | 1,090개 |

60일 창을 쓰는데도 표본이 최장이다. 분모가 중앙값이라 창이 다 차지 않아도 계산되기 때문이다.

---

## 7. 중복성

| 상대 family | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `mcap_krx_log` | +0.048 | 2,622 | −0.34 ~ +0.32 |
| `fin_log_mcap` | +0.038 | 2,392 | −0.24 ~ +0.28 |
| `ev_filing_activity` | +0.017 | 2,354 | −0.11 ~ +0.13 |
| `fin_value_z` | −0.015 | 1,928 | −0.33 ~ +0.23 |

**전부 0.05 미만이다. 35개 중 A×B 교차 상관이 가장 작은 축이다.**

**§2.2의 설계 의도가 확인된 자리다.** 같은 거래대금을 쓰는 `px_amihud_20d`가 규모와
−0.754인데 이 피처는 +0.048이다. 분모를 "같은 종목의 과거 중앙값"으로 잡아 규모를 상쇄한
효과가 숫자로 나타났다.

**즉 이 피처가 담은 정보는 규모가 아니다.** 반대 방향이긴 하지만 독립적인 정보를 담고 있다.

### 확인하지 않은 중복

`px_amihud_20d`, `px_zero_ret_ratio_20d`와의 A×A 상관이 없다. 셋 다 거래량 계열이다.

`px_mom_12_1`과의 상호작용도 안 봤다 (§3.4).

---

## 8. 한계와 확인 못 한 것

1. **반대 부호를 discovery로 세지 않았다.** 규율상 맞지만, 이 family는 §5.1·§5.2에서 보듯
   반대 방향이 매우 안정적이다. **반대 부호 가설로 새로 사전등록할 가치가 있는 후보다.**
   현재 상태로는 D등급으로만 기록돼 있어 이 정보가 묻힌다.
2. **IC와 5분위 차이의 부호가 어긋난다** (§4.2). 풀 자료가 없다.
3. **상호작용을 안 봤다** (§3.4). 원래 문헌의 핵심인 모멘텀 × 거래량 구조를 단변량으로는
   잡을 수 없다. `px_mom_x_volume`이 후보로 등록만 되고 만들어지지 않았다.
4. **거래대금이 근사값이다** (§2.3). 비율 형태라 영향이 작지만 확인은 안 했다.
5. **`px_volume_trend`를 안 만들었다.** 같은 P8 항목의 짝인데 이번 scan에 없다.
6. **시간 placebo를 못 돌렸다** (§5.6). 긴 horizon cell이 없어서다. 40일 이후를 primary로
   올리면 이 검사가 가능해진다.
7. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
8. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나

**T1·T2 어느 후보에도 안 들어갔다.** 등급 D라 screening에서 걸러졌다.

다만 §8의 1번을 다시 짚어 둘 만하다. 이 family는 **"신호가 없어서 D"가 아니라 "사전등록한
방향과 반대여서 D"**다. 최소 q 3.2e-11, 5구간 전부 같은 방향, tradable에서 27% 강화라는
조합은 A등급 피처들과 비교해도 약하지 않다.

`09_all_feature_results.md` §4가 탈락 3개를 묶어 "전부 부호가 반대다"로 처리했는데,
**모멘텀 둘(2/5, 방향 불안정)과 이 피처(0/5, 반대 방향 안정)는 성격이 다르다.**

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
A=f"research/output/horizon_scan/phase=A/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260827T221729-4e0ae8b0"
print(duckdb.sql(f"""
  select scan_type, h_start, h_end, universe, ic_mean, icir, t_nw,
         q5_spread_raw, q_fdr_global, bh_pass, expected_sign_pass,
         primary_discovery, n_dates
  from '{A}/core/horizon_ic.parquet'
  where family='px_turnover_shock' and sample_kind='common_survivor'
    and hypothesis_role='primary'
  order by universe, scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| cell 전체 | `phase=A/…/run_id=20260827T221729-4e0ae8b0/core/horizon_ic.parquet` |
| 강건성·offset 20개 | 같은 run의 `cards/family_cards.json` |
| 기간별 IC | 같은 run의 `plots/px_turnover_shock_subperiod_heatmap.png` |
| 기간 부호 판정 코드 | `research/analysis/horizon_scan_runner.py:929` |
| turnover level 금지 지침 | `01_feature_candidate/02_feature_candidate.md` §3.1 P7 |
| 상호작용 후보 등록 | 같은 문서 §3.1 P8 |
| 서술 대조 | `01_feature_candidate/09_all_feature_results.md` §4 「탈락 3개」 |
