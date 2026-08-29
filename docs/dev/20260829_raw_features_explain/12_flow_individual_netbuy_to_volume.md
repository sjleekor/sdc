# 12. `flow_individual_netbuy_to_volume` — 개인 순매수 강도

- 작성일: 2026-08-29
- family: `flow_individual_netbuy_to_volume` · primary feature: **`flow_individual_netbuy_to_volume_20d`**
- Phase A · fdr_family `flow` · **기대 부호 없음(양방향)** · 관측 부호 `+`
- 등급 **A** · `screen_pass` 통과 · discovery **6/6 cell 전부**
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**개인이 순매수한 종목이 이후 5~20일 동안 시장 대비 더 올랐다** (0→20일 IC +0.0241,
q = 1.1e-12, 5분위 수익률 차이 +0.70%p). **"개인은 틀린다"는 통념과 반대다.**

**35개 중 방향 가설을 걸지 않은 유일한 Phase A family다.** 연구 결과가 갈려 있어
`expected_sign: null`로 열어 둔 채 검증했다. 그 덕에 살아남았다 — 만약 통념대로 `−`를
걸었다면 이 family도 D등급이 됐을 것이다.

강건성은 깨끗하다. 5기간 전부 같은 방향, 비중첩 offset 5개 전부 일치, 유동성 좋은 종목에서
오히려 강함(유지율 1.009), 지연 게이트 통과.

**다만 §7이 중요하다.** 이 결과는 기관·외국인의 반대 부호와 **같은 현상의 뒷면**일 가능성이
높다. 독립된 발견으로 세면 안 된다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/flow.py:229
CASE WHEN COUNT(individual_net_buy_volume) OVER w20 = 20
          AND COUNT(total_volume) OVER w20 = 20
     THEN SUM(individual_net_buy_volume) OVER w20
          / NULLIF(SUM(total_volume) OVER w20, 0) END
    AS flow_individual_netbuy_to_volume_20d
```

**최근 20거래일 개인 순매수 주식 수를 같은 기간 총 거래량으로 나눈 값**이다.

[10_flow_foreign_netbuy_to_volume.md](10_flow_foreign_netbuy_to_volume.md) §2와 **투자자
유형만 다르고 산식이 동일하다.** 거래량으로 나눠 규모를 상쇄하고, 20일 창을 전부 채운 날만
계산하며, 거래정지일은 먼저 제거하고, **정본 변형이 `lag1`이다.**

산출물 24행이 전부 `feature = flow_individual_netbuy_to_volume_20d_lag1`이다.

### 2.2 secondary는 `_5d` 하나뿐이다

```yaml
# horizon_scan_config.yaml:337
features:
  - {column: flow_individual_netbuy_to_volume_20d, role: primary}
  - {column: flow_individual_netbuy_to_volume_5d, role: secondary}
```

외국인·기관이 `_5d`와 `_60d` 둘을 secondary로 둔 것과 달리 여기는 `_5d`만이다. 마트에는
`_60d`도 있지만 등록하지 않았다.

이번 run에는 20일 것만 있다. **secondary는 안 돌렸다.**

### 2.3 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/flow.py:229` |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:333` |

---

## 3. 왜 방향을 열어 뒀나 — 이 family의 핵심 설계

### 3.1 사전등록에 기대 부호가 없다

```yaml
# horizon_scan_config.yaml:336
- family: flow_individual_netbuy_to_volume
  expected_sign: null          # ← 외국인·기관은 "+"
```

**Phase A 17개 중 방향을 안 건 건 이것 하나다** (reference인 `px_zero_ret_ratio_20d` 제외).

### 3.2 왜 열어 뒀나

`09_all_feature_results.md` §5가 이유를 적었다.

> **부호를 사전등록하지 않은 유일한 피쳐다.** 연구별로 price pressure·momentum chasing·반전
> 결과가 섞여 있어 방향을 고정하지 않고 열어둔 채 검증했다.

세 갈래 예측이 서로 반대 방향을 가리킨다.

| 가설 | 메커니즘 | 예측 부호 |
|---|---|---|
| **가격 압력** | 개인 매수가 일시적으로 가격을 밀어 올림 | `+` (단기) |
| **추세 추종** | 개인이 이미 오른 종목을 뒤늦게 삼 | `−` (이후 되돌림) |
| **유동성 공급** | 개인이 기관 매도를 받아 주고 대가를 받음 | `+` |
| **정보 열위** | 개인이 정보 없이 사서 손해 | `−` |

**어느 쪽이 맞는지 사전에 정할 근거가 없었다.**

### 3.3 양방향 가설은 어떻게 판정하나

`expected_sign`이 없으면 두 가지가 달라진다.

**첫째, 방향 게이트를 적용하지 않는다.** 산출물에서 `expected_sign_pass`가 `<NA>`다
(다른 family는 `True`/`False`). 게이트 목록에서 아예 빠진다.

**둘째, 기간 일관성은 관측 부호를 기준으로 잰다.**

```python
# research/analysis/horizon_scan_runner.py:936
# Two-sided families (``expected_sign is None``) use the *observed* sign of
# the aligned IC directly
```

즉 `sign_consistent_subperiods = 5`는 **"관측된 방향(+)과 다섯 구간이 전부 일치했다"**는
뜻이다. 기대 부호와의 일치가 아니다.

### 3.4 이게 규율을 훼손하지 않는 이유

방향을 열어 두면 "어느 쪽이 나와도 발견"이 되는 것 아닌가 하는 의심이 생긴다. 그렇지 않다.

- **결과를 보기 전에 열어 뒀다.** 사후에 부호를 고른 게 아니다.
- **양측 검정을 쓴다.** `p_nw`는 양측이므로(`two_sided_normal_p`) 한쪽만 볼 때보다 문턱이
  높다.
- **나머지 게이트는 그대로다.** BH, 기간 일관성, tradable, 지연, offset을 전부 통과해야
  한다.

`09_all_feature_results.md`의 표현이 정확하다.

> 부호를 고정하지 않은 덕에 살아남았다. `+`로 걸었어도 통과했겠지만, 나머지 셋을 보면
> **그건 결과를 보고 나서 하는 말이다.**

### 3.5 사전등록 horizon

```yaml
primary_horizon_set: [5, 10, 20]
exploratory_horizon_set: [1, 2, 3, 40, 60, 120]
```

외국인·기관과 동일하다.

| | 사전등록 primary | 실제 관측 (`candidate_horizon_band`) |
|---|---|---|
| 밴드 | 5~20일 | **5~20일** |
| onset | (5일 이상) | **5** |

**정확히 맞았다.**

분류 좌표는 C3 × T1 × U다.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `lag1`)

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→5 | +0.0116 | 0.255 | 7.22 | +0.18%p | ~0 | **discovery** |
| cum | 0→10 | +0.0179 | 0.386 | 7.87 | +0.37%p | ~0 | **discovery** |
| cum | 0→20 | **+0.0241** | 0.498 | 7.56 | **+0.70%p** | ~0 | **discovery** |
| bucket | 0→5 | +0.0116 | 0.255 | 7.22 | +0.18%p | ~0 | **discovery** |
| bucket | 5→10 | +0.0136 | 0.303 | 8.43 | +0.19%p | ~0 | **discovery** |
| bucket | 10→20 | +0.0173 | 0.380 | 7.60 | +0.32%p | ~0 | **discovery** |

- family 최소 q: Phase A **1.07e-12**, 결합 AB **1.54e-16**.
- **6개 cell 전부 discovery.**

### 4.2 IC와 5분위 차이가 같은 방향이다

여섯 cell 전부 IC 양수·spread 양수다. 관계가 단조롭다.

### 4.3 크기

20일 기준 +0.70%p다. 기관 쪽의 −0.68%p와 거의 대칭이다 — §7의 거울상 관계를 숫자로 보여
주는 대목이다.

| family | horizon | IC | 5분위 차이 |
|---|---|---:|---:|
| **`flow_individual_netbuy_to_volume`** | 20일 | **+0.0241** | **+0.70%p** |
| `flow_inst_netbuy_to_volume` | 20일 | −0.0209 | −0.68%p |
| `flow_foreign_netbuy_to_volume` | 20일 | −0.0131 | −0.23%p |

가격 계열 A등급(1.79~2.99%p)과 비교하면 여전히 작다.

### 4.4 신호의 모양 — `delayed`

| 항목 | 값 | 읽는 법 |
|---|---|---|
| `pattern_auto` | **`delayed`** | 즉시 반응이 아니다 |
| `onset_h` | **5** | 5일부터 신호가 잡힘 |
| `candidate_horizon_band` | [5, 20] | 후보 구간 |
| `peak_h_cum` | 20 | 누적 최대 (관측 범위 끝) |
| `peak_bucket` | [10, 20] | 구간 최대 |
| `half_life_bucket` | 없음 | 20일 안에서 반감점 없음 |
| `sign_flip_bucket` | 없음 | 부호 뒤집힘 없음 |

**즉시 나타나지 않고 5~20일에 걸쳐 커진다.** 가격 압력 가설이 맞다면 매수 당일에 가장
강하고 이후 되돌아와야 하는데, 그 모양이 아니다.

`peak_h_cum = 20`이 관측 범위 끝이라 **40일 이후가 궁금한데 exploratory로 내려 확인하지
않았다.** 되돌림이 그 뒤에 오는지 알 수 없다.

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 5/5

- `valid_subperiods` = 5
- `sign_consistent_subperiods` = **5**

양방향 family이므로 **관측 부호(+) 기준**이다 (§3.3). 다섯 구간 전부에서 양수였다.

### 5.2 비중첩 offset — 5개 전부 일치

| 항목 | 값 |
|---|---|
| 총 offset | **5개** (전부 유효) |
| 부호 일치율 | **1.0** |
| 부호 검정 p 중앙값 | 5.9e-05 |
| 부호 검정 p 최댓값 | 5.2e-04 |
| offset IC 범위 | +0.0107 ~ +0.0126 |

**offset이 5개뿐이다.** 대표 cell의 horizon이 5일이라 격자가 좁다. `px_amihud_20d`의 60개,
`px_maxret_20d`의 20개와 비교하면 검정 강도가 약하다.

그래도 5개 전부 통과했고 가장 나쁜 p도 5.2e-04다.

### 5.3 거래 가능한 종목만 남겨도

| universe | IC (cum 0→5) | 유지율 |
|---|---:|---:|
| `broad` | 0.0116 | — |
| `tradable` | 0.0117 | **1.009** |

cum 0→20에서도 broad 0.0241 대 tradable 0.0233으로 거의 같다.

**개인 매매가 소형주에 몰린다는 통념을 생각하면 뜻밖이다.** 유동성 좋은 종목에서도 같은
신호가 나온다.

### 5.4 생존편향

| sample_kind | IC (cum 0→5) |
|---|---:|
| `common_survivor` | 0.0116 |
| `available` | 0.0109 |

차이가 작고 방향이 같다. `attrition_warning = false`.

### 5.5 지연 게이트 — 통과

| 항목 | 값 |
|---|---|
| `native_ic` | 0.011624 |
| `lag1_ic` | 0.011624 |
| `delay_pass` | **true** |

정본이 `lag1`이라 자기 자신과 비교하므로 유지율 1.0이고, h ≤ 5 cell의 `p_nw`가 0에 가까워
`p_nw < 0.05` 조건도 통과했다.

외국인이 이 지점에서 실패한 것(5일 cell p_nw = 0.053)과 대조된다.

### 5.6 시간 placebo — 대상이 아니다

`null`이다. 최대 horizon 20일이라 NW lag 19로 기준 59에 못 미친다.

### 5.7 시장 구성

KOSPI 41.5% / KOSDAQ 58.5%.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | **2,622일** |
| 날짜당 평균 종목 수 | **1,019개** |
| `tradable` 기준 종목 수 | 874개 |

수급 세 family가 전부 같다. 같은 원천·같은 완전 창 요구를 쓴다.

---

## 7. 중복성 — 세 유형은 하나의 발견일 수 있다

### A×B 교차 상관

| 상대 family | 평균 순위상관 | 유효일 |
|---|---:|---:|
| `own_major_filing_activity` | +0.025 | 2,514 |
| `fin_value_z` | +0.019 | 1,928 |
| `mcap_krx_log` | −0.015 | 2,622 |
| `fin_gross_profitability` | −0.015 | 1,927 |

**전부 0.03 미만이다.** Phase B 계열과 사실상 완전히 독립이다.

### 확인하지 않은 중복 — 이 계열의 핵심 문제

`09_all_feature_results.md` §5의 지적을 그대로 옮긴다.

> **세 개가 모두 반대로 나왔고 개인만 정방향이다.** 개인 순매수와 기관·외국인 순매수는
> 거의 거울상이라(기타법인 제외로 완전한 항등식은 아니지만), 사실상 **같은 현상의 양면**을
> 보고 있을 가능성이 높다. 즉 독립된 네 개의 발견이 아니라 하나의 발견으로 읽어야 한다.

§4.3의 대칭이 그 정황이다. 개인 +0.70%p, 기관 −0.68%p. 거의 부호만 뒤집힌 값이다.

**주의할 점.** 세 투자자 순매수의 합은 **항등식이 아니다.** 기타법인이 빠져 있다
(`flow.py` 모듈 docstring: "The 3-investor net-buy sum is NOT an identity (excludes 기타법인)
so we never derive a 'closes-to-zero' feature from it").

그래서 완전한 거울상은 아니고, **얼마나 겹치는지가 실측 대상**이다. 그런데
**A×A 상관 산출물이 없어 재지 않았다.**

이 질문에 답하지 않으면 다음을 판단할 수 없다.

- 세 family를 모델에 다 넣어야 하는가, 하나면 되는가
- 개인 순매수의 `+`가 독립 정보인가, 기관·외국인 `−`의 기계적 뒷면인가
- 결합 BH에서 세 family를 별도 가설로 센 것이 다중검정 보정을 느슨하게 만들지 않았는가

---

## 8. 한계와 확인 못 한 것

1. **세 유형이 하나의 발견일 수 있다** (§7). 가장 중요한 미확인 사항이다.
2. **크기가 작다** (§4.3). 20일 +0.70%p, 거래비용 차감 전이다.
3. **offset이 5개뿐이다** (§5.2). 검정 강도가 다른 A등급 family보다 약하다.
4. **40일 이후를 안 봤다** (§4.4). 가격 압력 가설이라면 되돌림이 와야 하는데 관측 범위
   끝에서 신호가 최대다.
5. **어느 메커니즘인지 모른다** (§3.2). 네 갈래 가설 중 어느 것인지 구분하는 검정을 하지
   않았다. 부호만으로는 가격 압력과 유동성 공급을 구분할 수 없다.
6. **`_5d` secondary를 안 돌렸다** (§2.2).
7. **당일 사용 가능성을 검증하지 않았다.** `lag1`이 정본인 건 보수적 선택이다.
8. **업종 중립화가 없다.**
9. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
10. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T1

**T1 후보 5개 중 둘이 이 family에서 나왔다.**

`07_phase1_acceptance_gate.md` §1의 후보 목록이다.

> baseline + `px_reversal_5d`, `px_maxret_20d`, `px_idio_vol_60d`,
> **`flow_individual_netbuy_to_volume_5d`, `flow_individual_netbuy_to_volume_20d`**
> (family 카드에 등록된 primary+secondary만; `_60d` 변형은 미등록이라 제외) = 45개

**5개 후보 중 2개가 이 family의 두 변형이다.** `_60d`가 빠진 이유는 §2.2에서 본 대로
사전등록에 없기 때문이다 — 등록 범위가 모델 후보 범위를 그대로 결정했다.

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

**주의.** 5개 묶음의 결과이고 개별 기여도는 측정하지 않았다. 이 family가 두 자리를 차지했다는
사실은 후보 구성에서의 비중을 보여줄 뿐, 성과 기여를 뜻하지 않는다.

T2 14-feature bundle에는 없다 (Phase B 전용 묶음이다).

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
A=f"research/output/horizon_scan/phase=A/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260827T221729-4e0ae8b0"
# 양방향 family 는 expected_sign_pass 가 NULL 이다
print(duckdb.sql(f"""
  select family, scan_type, h_end, ic_mean, q5_spread_raw,
         expected_sign, expected_sign_pass, primary_discovery
  from '{A}/core/horizon_ic.parquet'
  where family like 'flow_%netbuy_to_volume' and scan_type='cum'
    and universe='broad' and sample_kind='common_survivor'
    and hypothesis_role='primary'
  order by h_end, family
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| cell 전체 | `phase=A/…/run_id=20260827T221729-4e0ae8b0/core/horizon_ic.parquet` |
| 강건성·offset 5개 | 같은 run의 `cards/family_cards.json` |
| 기간별 IC | 같은 run의 `plots/flow_individual_netbuy_to_volume_subperiod_heatmap.png` |
| 산식 | `research/etl/features/flow.py:229` |
| 양방향 판정 코드 | `research/analysis/horizon_scan_runner.py:936` |
| 부호를 연 근거 | `01_feature_candidate/09_all_feature_results.md` §5 |
| T1 후보 구성 | `01_feature_candidate/07_phase1_acceptance_gate.md` §1 |
