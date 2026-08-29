# 25. `fin_gross_profitability` — 매출총이익률 (총자산 대비)

- 작성일: 2026-08-29
- family: `fin_gross_profitability` · primary feature: 동명 · domain: financial
- **Phase B** · fdr_family `financial` · 기대 부호 `+` · 관측 부호 `+`
- **discovery 8/8 · screen-pass 5/8** · 등급 **B 5개 / C 3개** · source quality `warn`
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**매출총이익률이 높은 회사가 이후 더 올랐다** (cum 0→120 IC +0.0360). 기대 방향과 일치하고
**사전등록 8개 cell 전부 discovery**다. Phase B에서 cell 수가 가장 많은 family다.

**세 가지를 함께 봐야 한다.**

1. **짧은 구간만 screen-pass했다.** 60일 이상 세 cell이 시간 placebo에서 떨어졌다
   (p = 0.168 ~ 0.297).
2. **등급이 A가 아니라 B다.** 매출총이익의 **94.4%가 매핑 대체 경로**로 채워졌다 — `매출액 −
   매출원가`로 역산한 값이다 (§5.6).
3. **쓰는 값이 오래됐다.** 평균 105일, **95분위 706일**. 5%는 2년 가까이 묵은 값이다 (§2.5).

크기는 작다. |IC| 0.036, 5분위 수익률 차이 +0.66%p(120일 기준)다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/fin_scan.py:276
CASE WHEN avg_assets > 0
     THEN gross_profit_effective / avg_assets
END AS fin_gross_profitability
```

**매출총이익 ÷ 평균 총자산**이다.

- 0.30이면 자산 100원으로 매출총이익 30원을 만든다
- 값이 클수록 자산을 효율적으로 쓴다

### 2.2 왜 순이익이 아니라 매출총이익인가

**Novy-Marx (2013)의 핵심 주장이다.**

순이익은 감가상각·이자·세금·일회성 항목을 거치며 회계 재량이 많이 개입한다. **매출총이익
(매출액 − 매출원가)은 손익계산서에서 가장 위에 있어 조작 여지가 작다.**

그래서 "가장 깨끗한 수익성 지표"라는 것이 이 피처의 근거다.

**그런데 이번 구현에서는 그 장점이 상당 부분 약해진다** — §2.3이 이유다.

### 2.3 94.4%가 역산값이다 — 이 family의 핵심 한계

```sql
-- research/etl/features/fin_scan.py:252
CASE WHEN gross_profit_selected IS NOT NULL THEN gross_profit_selected
     WHEN revenue_selected IS NOT NULL AND cogs_selected IS NOT NULL
          THEN revenue_selected - cogs_selected
END AS gross_profit_effective,
CASE WHEN gross_profit_selected IS NOT NULL THEN 'direct'
     WHEN revenue_selected IS NOT NULL AND cogs_selected IS NOT NULL
          THEN 'revenue_minus_cogs_fallback'
END AS gross_profit_source
```

**매출총이익 계정을 직접 찾지 못하면 `매출액 − 매출원가`로 만든다.**

산출물에 `gross_profit_source`가 함께 저장되고, 원천 품질 지표가 그 비율을 잰다.

| 항목 | 값 |
|---|---|
| `mapping_fallback_ratio` | **0.9440** |
| `mapping_fallback_worst_metric` | **`gross_profit`** |
| 임계값 | 0.50 |

**94.4%가 역산 경로다.** 임계 0.50을 크게 넘는다. Phase B 구현 로그가 이 값을 명시했다.

> `fin_gross_profitability`와 `fin_value_z`는 `mapping_fallback_ratio`도 각각
> **0.9440·0.9417**로 임계 0.50을 넘었다.

**역산 자체가 틀린 계산은 아니다** — 회계 정의상 매출총이익은 매출액에서 매출원가를 뺀
값이다. 다만 두 가지가 걸린다.

1. **매출원가 계정을 못 찾는 회사는 통째로 빠진다.** 금융업처럼 매출원가 개념이 없는
   업종이 그렇다. 커버리지 0.584의 한 원인이다.
2. **§2.2의 "조작 여지가 작다"는 논거가 약해진다.** 두 계정을 각각 매핑해 빼는 과정에서
   원천 표기 차이가 들어온다.

### 2.4 분모가 평균 총자산이다

`avg_assets = (total_assets + total_assets_lag4q) / 2`다
(`fin_scan.py:250`). 분자가 기간 흐름이므로 분모도 기간 평균이다.

[23_fin_accruals_to_assets.md](23_fin_accruals_to_assets.md)와 **분모를 공유한다.**

### 2.5 vintage 나이가 가장 오래됐다

| 시장 | 평균 나이 | **95분위 나이** |
|---|---:|---:|
| KOSDAQ | 105.2일 | **705.9일** |
| KOSPI | 102.9일 | **531.0일** |

**35개 중 가장 오래된 값을 쓴다.** 다른 재무 family가 평균 74일 / 95분위 175일 안팎인데
이쪽은 평균 105일 / 95분위 706일이다.

**20번에 한 번은 2년 가까이 묵은 값**으로 오늘의 순위를 매긴다는 뜻이다.

원인은 §2.3의 매핑 조건이다. 매출총이익(또는 매출액·매출원가 쌍)을 찾지 못한 분기는
건너뛰고 **더 오래된 값을 계속 쓰게 된다.**

**PIT 원칙 위반은 아니다** — 그 시점에 실제로 알 수 있었던 값만 쓴다. 다만 **정보가
얼마나 신선한가**는 별개 문제이고, 이 family는 그 점에서 가장 취약하다.

### 2.6 secondary가 하나 있다

```yaml
features:
  - {column: fin_gross_profitability,     role: primary}
  - {column: fin_operating_profitability, role: secondary}
```

`fin_operating_profitability`(영업이익 / 평균총자산)가 secondary로 등록돼 있는데
**이번 run에는 primary 8개 cell만 있다.** 매출총이익과 영업이익 중 어느 쪽이 나은지
확인하지 않았다.

### 2.7 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/fin_scan.py:276` |
| 역산 fallback | `research/etl/features/fin_scan.py:252` |
| 평균 총자산 | `research/etl/features/fin_scan.py:250` |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:444` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**"질 좋은 회사가 더 오른다"는 quality 가설이다.**

Novy-Marx (2013)의 발견이다. 총자산 대비 매출총이익이 높은 회사가 이후 수익률이 높다.
설명은 이렇다.

- 수익성이 높은 회사는 **경쟁 우위가 있다.** 그 우위가 지속되는데 시장이 충분히 반영하지
  않는다.
- 밸류 지표와 **음의 상관**이 있어(수익성 좋은 회사는 비싸다) 밸류와 함께 쓰면 서로를
  보완한다.

### 3.2 기대 부호

`+`. 매출총이익률이 높을수록 이후 초과수익률 순위가 높다.

### 3.3 사전등록 horizon — Phase B 중 가장 넓다

```yaml
# horizon_scan_config.yaml:450
primary_horizon_set: [20, 40, 60, 120]
exploratory_horizon_set: [1, 5, 10]
include_bucket_primary: true
```

**primary가 네 개다.** 다른 Phase B family가 [60, 120] 둘인 것과 다르다. cell이
누적 4 + 구간 4 = **8개**로 Phase B 최다다.

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 20~120일 | **8개 cell 전부 discovery, screen-pass 5개** |
| 부호 | `+` | **`+` (일치)** |

**넓게 잡은 게 결과적으로 유리하게 작용했다.** 짧은 구간이 살아남았기 때문이다(§4.2).

### 3.4 한국 시장 단서 — 예고와 결과가 갈린다

`02_feature_candidate.md` §1의 9번 항목(`Q1, Q2`)이다.

> 수익성/quality | `fin_gross_profitability`, `fin_operating_profitability` | Q1, Q2 |
> `+` | A | R1

그런데 `11_feature_taxonomy.md` §4의 한국 복제 연구 대조는 이렇게 적었다.

> **수익성 5.0%(엄격 기준 0.0%)**, 투자 24.1% 복제율이 **`fin_gross_profitability`**와
> `fin_asset_growth_yoy`의 **무신호**를 … 예측한다.

**Han, Lee & Kang (2020)에서 수익성 카테고리 복제율이 5.0%였다.** 한국에서 거의 재현되지
않는 영역이라는 뜻이다.

**그런데 이번에는 신호가 나왔다.** 8개 cell 전부 discovery다.

두 가지로 읽을 수 있고 **이 자료로는 못 가른다.**

1. 복제율이 낮다는 건 "대부분 안 된다"는 뜻이지 "전부 안 된다"가 아니다. 이 산식이
   되는 5%에 속할 수 있다.
2. §5.2에서 보듯 긴 구간은 시간 placebo를 통과하지 못했다. **짧은 구간의 신호가 진짜인지는
   아직 확인되지 않았다.**

`11_feature_taxonomy.md`는 이 문서보다 앞선 run을 기준으로 쓰였다. **당시에는 커버리지가
0.0315로 표본이 없었고, I7 수정 뒤 0.5835로 늘면서 짧은 horizon 5개 cell이 새로 살아났다**
(`08_phase_b_implementation_log.md`).

분류 좌표는 C2(재무 기반 상태) × T0(수준) × U다.

### 3.5 근거 문헌

Novy-Marx (2013), *The Other Side of Value: The Gross Profitability Premium*. 등급 A.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 | AB q | 등급 | 판정 |
|---|---|---:|---:|---:|---:|---:|---|---|
| cum | 0→20 | +0.0233 | 0.346 | 4.07 | +0.10%p | 0.00009 | **B** | **screen-pass** |
| cum | 0→40 | +0.0275 | 0.375 | 3.18 | +0.29%p | 0.0024 | **B** | **screen-pass** |
| cum | 0→60 | +0.0293 | 0.391 | 2.68 | +0.46%p | 0.0112 | C | robustness 실패 |
| cum | 0→120 | **+0.0360** | 0.453 | 2.29 | **+0.66%p** | 0.0317 | C | robustness 실패 |
| bucket | 10→20 | +0.0196 | 0.306 | 5.09 | +0.04%p | ~0 | **B** | **screen-pass** |
| bucket | 20→40 | +0.0226 | 0.333 | 3.91 | +0.11%p | 0.00017 | **B** | **screen-pass** |
| bucket | 40→60 | +0.0211 | 0.312 | 3.62 | +0.10%p | 0.00053 | **B** | **screen-pass** |
| bucket | 60→120 | +0.0267 | 0.360 | 2.47 | +0.21%p | 0.0201 | C | robustness 실패 |

**8개 전부 discovery, 5개가 screen-pass다.**

### 4.2 긴 구간만 떨어졌다 — 규칙적인 패턴

`failed_gates`가 세 cell에서 `[robustness_pass]`다.

| cell | NW lag | `robustness_required` | `p_temporal_nw` | 판정 |
|---|---:|---|---:|---|
| cum 0→20 | 19 | **False** | — | 대상 아님 → 통과 |
| cum 0→40 | 39 | **False** | — | 대상 아님 → 통과 |
| bucket 10→20 | 9 | False | — | 대상 아님 → 통과 |
| bucket 20→40 | 19 | False | — | 대상 아님 → 통과 |
| bucket 40→60 | 19 | False | — | 대상 아님 → 통과 |
| **cum 0→60** | **59** | **True** | **0.2277** | **실패** |
| **cum 0→120** | **119** | **True** | **0.2970** | **실패** |
| **bucket 60→120** | **59** | **True** | **0.1683** | **실패** |

**NW lag가 59 이상인 세 cell만 시간 placebo를 받았고, 셋 다 떨어졌다.**

Phase B 구현 로그가 같은 구조를 기록했다.

> 12셀 중 temporal placebo가 요구된 것은 `fin_log_mcap` 3셀뿐이며 모두 p=0.0099로 통과했다.
> **나머지 9셀은 horizon 폭이 짧아 `robustness_required=false`다.**

**즉 screen-pass한 5개 cell은 시간 placebo를 통과한 게 아니라 받지 않았다.**
[04_px_near_52w_high.md](04_px_near_52w_high.md) §5.6과 같은 구조다.

**긴 구간에서 검사를 받으면 전부 떨어진다는 사실이 이 family의 실제 상태를 말해 준다.**

### 4.3 크기는 작다

|IC| 0.036, 5분위 차이 +0.66%p(120일)다.

| family | horizon | \|IC\| | 5분위 차이 |
|---|---|---:|---:|
| `fin_value_z` | — | 0.122 | +4.52%p |
| `fin_log_mcap` | — | 0.115 | +11.83%p |
| **`fin_gross_profitability`** | **120일** | **0.036** | **+0.66%p** |
| `fin_accruals_to_assets` | 120일 | 0.018 | +0.10%p |

Phase B 재무 계열에서 중간 정도다.

### 4.4 신호의 모양

| 관찰 | 값 |
|---|---|
| `peak_cell` | `cum 0→120` |
| `peak_ic_mean` | +0.0360 |
| 누적 IC 추이 | 20일 0.023 → 40일 0.028 → 60일 0.029 → 120일 0.036 |
| 구간 IC 추이 | 10~20일 0.020 → 20~40일 0.023 → 40~60일 0.021 → 60~120일 0.027 |

**누적은 완만하게 증가하고 구간은 거의 평평하다.** 신호가 특정 시점에 몰려 있지 않고
20~120일에 고르게 퍼져 있다는 뜻이다.

다만 t값은 반대로 간다 — 짧을수록 크다(5.09 → 2.29). horizon이 길수록 중첩 보정이
커지기 때문이다.

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 4구간 중 3~4구간

| cell | `valid_subperiods` | `sign_consistent_subperiods` | `period_sign_pass` |
|---|---:|---:|---|
| cum 0→20 | 4 | **4** | True |
| bucket 10→20 | 4 | **4** | True |
| bucket 20→40 | 4 | **4** | True |
| 나머지 5개 | 4 | 3 | True |

**전부 통과했다.** 짧은 구간이 4/4로 특히 안정적이다.

표본이 2017-02-27부터라 구간이 5개가 아니라 **4개**다. `2014_2016`이 통째로 비었다.

### 5.2 시간 placebo — 검사받은 세 cell 전부 실패

§4.2에 정리했다. p = 0.168 ~ 0.297로 기준 0.10을 넘는다.

**이 family의 유일한 실패 지점이다.** 다른 게이트는 전부 통과했다.

### 5.3 비중첩 offset — `complete` 통과

세 긴 cell 모두 `offset_status = complete`, `nonoverlap_robustness_pass = True`다.
창 중첩 보정에서는 문제가 없었다. **떨어진 건 시간 placebo 하나다.**

### 5.4 거래 가능한 종목만 남기면 — 오히려 강해진다

| cell | `tradable_retention` |
|---|---:|
| cum 0→120 | **1.228** |
| cum 0→60 | 1.218 |
| cum 0→40 | 1.203 |
| 나머지 | 1.151 ~ 1.193 |

**여덟 cell 전부 1.15를 넘는다.** 유동성 좋은 종목에서 15~23% 더 강하다. 소형주 착시가
아니다.

### 5.5 생존편향

`available_direction_pass` = **True** (8개 cell 모두).

### 5.6 source quality — `warn` 두 가지

**등급이 A가 아니라 B인 이유다.**

| 항목 | 값 | 임계 | 판정 |
|---|---|---:|---|
| `source_quality_reasons` | **`mapping_fallback,revision`** | — | 두 가지 |
| `mapping_fallback_ratio` | **0.9440** (`gross_profit`) | 0.50 | **초과** |
| `revision_ratio` | 0.1014 (`total_assets`) | 0.10 | **근소 초과** |
| `pairing_mismatch_ratio` | 0.000125 | 0.01 | 통과 |

**35개 중 유일하게 두 가지 경고가 동시에 걸린 family는 이것과 `fin_value_z` 둘이다.**

- **매핑 대체 94.4%** (§2.3) — 매출총이익을 직접 못 찾아 역산한 비율
- **정정 10.1%** — 총자산이 사후 정정된 비율

등급 규칙상 `B: screen_pass_with_nonfatal_warning`이므로, screen-pass한 다섯 cell이
A가 아니라 **B**가 됐다.

Phase B 구현 로그가 이를 명시했다.

> **B등급 7셀은 통계 때문이 아니라 source 품질 경고로 A 상한이 막혔다.**

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2017-02-27 ~ 2025-02-05** |
| 유효 거래일 | **1,927일** |
| 날짜당 평균 종목 수 | **855~856개** |
| `coverage_ratio` | **0.584** |
| 관측 행 수 | 4,208,192 |

**패널의 42%에 값이 없다.** §2.3의 매핑 조건이 주된 원인이다 — 매출총이익도, 매출액·매출원가
쌍도 못 찾으면 NULL이다.

시장별로는 KOSDAQ 0.613 / KOSPI 0.538이다.

### 6.1 이전 run과 크게 달라진 family다

Phase B 구현 로그가 기록했다.

> I7 뒤 `fin_gross_profitability` coverage가 **0.0315 → 0.5835**로 늘면서 짧은 horizon
> **5셀이 새 B등급으로 살아난 것이 이전 run과 가장 큰 차이다.**

**이전 run에서는 커버리지가 3%였고 사실상 표본이 없었다.** 지금 결과는 그 수정 이후의
것이다. **이전 문서(`09_all_feature_results.md`, `11_feature_taxonomy.md`)의 "무신호"
서술은 수정 전 기준이다.**

---

## 7. 중복성

### A×B 교차 상관

| 상대 family | 평균 순위상관 | 유효일 |
|---|---:|---:|
| `px_amihud_20d` | **−0.161** | 1,927 |
| `px_mom_12_1` | +0.132 | 1,927 |
| `px_near_52w_high` | +0.149 | 1,927 |
| `px_maxret_20d` | (§7 원본 참조) | — |

- **`px_amihud_20d`와 −0.161.** 수익성 좋은 회사는 비유동성이 낮다 = 규모가 크다.
  `px_amihud_20d`가 사실상 규모 지표라는 점을 생각하면
  ([07_px_amihud_20d.md](07_px_amihud_20d.md) §7) **수익성 신호가 규모와 얽힐 수 있다.**
- **`px_near_52w_high`와 +0.149.** 수익성 좋은 회사가 고점 근처에 있다.

`|ρ| ≥ 0.7` 경고 기준에는 못 미친다.

### 확인하지 않은 중복

1. **`fin_value_z`와의 관계.** §3.1의 가설이 "밸류와 음의 상관"을 전제하는데
   **B×B 상관 산출물이 없다.** 이 family의 핵심 논거를 확인하지 못했다.
2. **`fin_accruals_to_assets`와 분모가 같다** (`avg_assets`).
3. **`fin_operating_profitability`(secondary)를 안 돌렸다** (§2.6).

---

## 8. 한계와 확인 못 한 것

1. **긴 구간이 전부 시간 placebo에서 떨어졌다** (§4.2, §5.2). screen-pass한 다섯은
   검사를 받지 않았다.
2. **매출총이익의 94.4%가 역산값이다** (§2.3). §2.2의 "조작 여지가 작다"는 논거가 약해진다.
3. **쓰는 값이 가장 오래됐다** (§2.5). 평균 105일, 95분위 706일.
4. **총자산의 10.1%가 사후 정정됐다** (§5.6).
5. **표본이 2017년부터라 기간 검정이 4구간뿐이다** (§5.1, §6).
6. **`fin_value_z`와의 상관이 없다** (§7). 이 family의 핵심 가설을 검증하지 못했다.
7. **`fin_operating_profitability`를 안 돌렸다** (§2.6).
8. **한국 복제율 5.0%와의 관계가 미해결이다** (§3.4). 신호가 나온 게 예외인지, 표본이
   짧아서인지 알 수 없다.
9. **업종 중립화가 없다.** 매출총이익률은 업종에 따라 근본적으로 다르다 — 소프트웨어와
   유통이 한 풀에서 비교된다. **이 family가 업종 중립화 부재에 가장 취약한 축이다.**
10. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
11. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T2

**T2 14-feature bundle에 들어갔다** (`fin_gross_profitability`).

| horizon | Rank IC Δ | 비용 반영 spread Δ |
|---|---:|---:|
| 5 | +0.0031 | +0.0017 |
| 20 | +0.0011 | +0.0030 |
| 60 | +0.0003 | +0.0080 |

세 horizon 전부 개선됐다(`improved_all_horizons`). **14개를 함께 넣은 결과라 개별 기여도는
측정하지 않았다.**

같은 묶음에 `fin_value_z`·`fin_log_mcap`·`mcap_krx_log`가 들어 있어 §7의 미확인 중복이
여기서도 걸린다.

**최종 h60 holdout은 아직 열지 않았다.**

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# robustness_required 가 켜진 cell 만 시간 placebo 를 받는다
print(duckdb.sql(f"""
  select scan_type, h_start, h_end, ic_mean, q5_spread_aligned, q_fdr_global_ab,
         robustness_required, p_temporal_nw, temporal_null_pass,
         screen_pass, evidence_grade, failed_gates, tradable_retention
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  where family='fin_gross_profitability' order by scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 원천 품질 (매핑 대체 94.4%) | 같은 B run의 `core/quarterly_metric_quality.parquet` |
| 커버리지·vintage 나이 | 같은 B run의 `core/feature_coverage.parquet` |
| 산식·역산 fallback | `research/etl/features/fin_scan.py:276`, `:252` |
| 커버리지 회복 이력 | `01_feature_candidate/08_phase_b_implementation_log.md` §3.0 |
| 한국 복제 연구 대조 | `01_feature_candidate/11_feature_taxonomy.md` §4 |
| T2 결과 | `docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json` |
