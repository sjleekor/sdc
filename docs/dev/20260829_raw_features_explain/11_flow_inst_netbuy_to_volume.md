# 11. `flow_inst_netbuy_to_volume` — 기관 순매수 강도

- 작성일: 2026-08-29
- family: `flow_inst_netbuy_to_volume` · primary feature: **`flow_inst_netbuy_to_volume_20d`**
- Phase A · fdr_family `flow` · 기대 부호 `+` · **관측 부호 `−`**
- 등급 **D** · `screen_pass` 실패 · discovery 0/6 cell
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**기관이 순매수한 종목이 이후 5~20일 동안 시장 대비 부진했다** (0→20일 IC −0.0209,
q = 1.3e-13, 5분위 수익률 차이 −0.68%p).

**수급 계열 세 개 중 반대 신호가 가장 강하고 가장 깨끗하다.**

| | 외국인 | **기관** | 개인 |
|---|---:|---:|---:|
| IC (0→20) | −0.0131 | **−0.0209** | +0.0241 |
| 최소 BH q | 2.2e-05 | **1.3e-13** | — |
| 5분위 차이 | −0.23%p | **−0.68%p** | — |
| tradable 유지율 | 0.707 | **1.073** | — |
| 지연 게이트 | **실패** | **통과** | — |

기대 방향 일치 구간이 **0/5**, 즉 다섯 구간 전부에서 반대 방향으로 일관됐고, 비중첩 offset
20개도 전부 반대 방향이다 (p 중앙값 0.99996). 유동성 좋은 종목에서 오히려 강해지고
(유지율 1.073) 하루 늦춰도 유효하다.

**그래도 discovery가 아니다.** 사전등록한 부호가 `+`이기 때문이다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/flow.py:217
CASE WHEN COUNT(institution_net_buy_volume) OVER w20 = 20
          AND COUNT(total_volume) OVER w20 = 20
     THEN SUM(institution_net_buy_volume) OVER w20
          / NULLIF(SUM(total_volume) OVER w20, 0) END
    AS flow_inst_netbuy_to_volume_20d
```

**최근 20거래일 기관 순매수 주식 수를 같은 기간 총 거래량으로 나눈 값**이다.

[10_flow_foreign_netbuy_to_volume.md](10_flow_foreign_netbuy_to_volume.md)와 **투자자 유형만
다르고 산식이 동일하다.** 따라서 다음이 그대로 적용된다.

- 거래량으로 나눠 규모를 상쇄한다 (§2.2 참조)
- 20일 창의 20행이 **전부** 있어야 계산한다 (`= 20`)
- 거래정지일은 창에 들어가기 전에 제거된다
- **정본 변형이 `lag1`이다** — 당일 사용 가능 여부가 검증되지 않아 하루 늦춘 값을 쓴다

산출물의 24행이 전부 `feature = flow_inst_netbuy_to_volume_20d_lag1`이다.

### 2.2 "기관"이 무엇을 포함하는가

원천 `krx_security_flow_raw`의 `institution_net_buy_volume`을 그대로 쓴다. KRX가 집계하는
기관 범주(금융투자·투신·연기금·보험 등)의 **합계**다.

**세부 유형을 나누지 않았다.** 연기금과 증권사 자기매매는 목적도 기간도 다른데 한 값으로
묶여 있다. 이번 검증에서는 그 안을 들여다보지 않았다.

### 2.3 secondary는 안 돌렸다

`_5d`, `_60d`가 secondary로 등록돼 있지만 이번 run에는 20일 것만 있다.

### 2.4 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/flow.py:217` |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:317` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**정보 우위 가설이다.** 외국인 쪽과 같다.

기관 투자자는 리서치 조직과 정보 접근성이 개인보다 낫다. 그들의 순매수는 앞으로 오를 종목을
가리킨다는 설명이다.

### 3.2 기대 부호

`+`. 기관 순매수 비중이 높을수록 이후 초과수익률 순위가 높다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_config.yaml:323
primary_horizon_set: [5, 10, 20]
exploratory_horizon_set: [1, 2, 3, 40, 60, 120]
```

외국인과 동일하다.

| | 사전등록 primary | 실제 관측 |
|---|---|---|
| 밴드 | 5~20일 | **없음** |
| 부호 | `+` | `−` |

### 3.4 한국 시장 단서

`02_feature_candidate.md` §3.2가 이 도메인 전체를 "한국 특화 연구에서 예측력이 가장 일관되게
보고되는 영역"으로 적었고, `00_raw_feature_inventory.md`가 "완전 미사용 7,700만 행"으로
지목했다.

**기대가 가장 컸던 영역인데 세 유형 중 둘이 반대로 나왔다.**

분류 좌표는 C3 × T1 × U다.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `lag1`)

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 | BH q | 부호 | 판정 |
|---|---|---:|---:|---:|---:|---:|---|---|
| cum | 0→5 | −0.0141 | −0.308 | −8.72 | −0.21%p | ~0 | 반대 | BH 통과, discovery 아님 |
| cum | 0→10 | −0.0177 | −0.396 | −8.24 | −0.40%p | ~0 | 반대 | BH 통과, discovery 아님 |
| cum | 0→20 | **−0.0209** | −0.483 | −7.51 | **−0.68%p** | ~0 | 반대 | BH 통과, discovery 아님 |
| bucket | 0→5 | −0.0141 | −0.308 | −8.72 | −0.21%p | ~0 | 반대 | BH 통과, discovery 아님 |
| bucket | 5→10 | −0.0114 | −0.266 | −7.58 | −0.19%p | ~0 | 반대 | BH 통과, discovery 아님 |
| bucket | 10→20 | −0.0138 | −0.324 | −6.63 | −0.27%p | ~0 | 반대 | BH 통과, discovery 아님 |

- family 최소 q: **1.34e-13**. 외국인(2.2e-05)보다 여덟 자릿수 작다.
- **6개 cell 전부 BH를 통과했다.** 외국인은 4개였다.
- 그런데 `expected_sign_pass = false`이므로 discovery는 0개다.

### 4.2 IC와 5분위 차이의 부호가 일치한다

수급 계열에서 드물게 **두 지표가 같은 방향**이다. 여섯 cell 전부 IC 음수·spread 음수다.

[08_px_turnover_shock.md](08_px_turnover_shock.md) §4.2, [04_px_near_52w_high.md](04_px_near_52w_high.md)
§4.3에서 본 어긋남이 여기서는 없다. **관계가 단조롭다**는 뜻이다. 상위 20%와 하위 20%의
평균 차이가 순위 상관과 같은 이야기를 한다.

### 4.3 크기

20일 기준 −0.68%p다. 외국인(−0.23%p)의 세 배지만 가격 계열 A등급과 비교하면 여전히 작다.

| family | horizon | 5분위 차이 |
|---|---|---:|
| `px_idio_vol_60d` | 60일 | +2.99%p |
| `px_maxret_20d` | 60일 | +1.79%p |
| **`flow_inst_netbuy_to_volume`** | **20일** | **−0.68%p** |
| `px_reversal_5d` | 5일 | +0.38%p |
| `flow_foreign_netbuy_to_volume` | 20일 | −0.23%p |

### 4.4 신호의 모양

| 항목 | 값 |
|---|---|
| `pattern_auto` | `no_signal` |
| `candidate_horizon_band` | 없음 |
| `peak_h_cum` | 20 (음수 방향 최대, 관측 범위 끝) |
| `peak_bucket` | [5, 10] |
| `half_life_bucket` | 없음 |
| `sign_flip_bucket` | 없음 |

누적 |IC|가 5일 → 10일 → 20일로 단조 증가한다 (0.014 → 0.018 → 0.021). 외국인과 같은
모양이고, **관측 범위 끝에서 최대라 40일 이후가 궁금한데 exploratory로 내려 확인하지
않았다.**

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 기대 방향 0/5 = 반대 방향 5/5

- `valid_subperiods` = 5
- `sign_consistent_subperiods` = **0**

다섯 구간 전부에서 IC가 음수였다. 반대 방향이 완벽하게 일관됐다.

### 5.2 비중첩 offset — 20개 전부, 반대 방향으로

| 항목 | 값 |
|---|---|
| 총 offset | 20개 (전부 유효) |
| 기대 방향 부호 일치율 | **0.0** |
| 부호 검정 p 중앙값 | **0.99996** |
| 부호 검정 p 최솟값 | 0.9992 |
| offset IC 범위 | −0.0248 ~ −0.0191 |

**가장 낮은 p조차 0.9992다.** 20개 offset 전부가 반대 방향으로 강하게 유의하다. IC 범위도
좁고 전부 음수다.

### 5.3 거래 가능한 종목만 남기면 — 7% 강해진다

| universe | IC (cum 0→20) | 유지율 |
|---|---:|---:|
| `broad` | −0.0209 | — |
| `tradable` | −0.0224 | **1.073** |

**외국인(0.707)과 결정적으로 다른 대목이다.** 유동성 좋은 종목에서 오히려 강해진다.

기관이 주로 거래하는 종목이 애초에 유동성 좋은 종목이라는 점을 생각하면 자연스럽다.
**소형주 착시가 아니라는 뜻이고, 반대 신호의 신뢰도를 높인다.**

### 5.4 생존편향

| sample_kind | IC (cum 0→20) |
|---|---:|
| `common_survivor` | −0.0209 |
| `available` | −0.0198 |

차이가 작고 방향이 같다. `attrition_warning = false`.

### 5.5 지연 게이트 — 통과

| 항목 | 값 |
|---|---|
| `native_ic` | −0.02089 |
| `lag1_ic` | −0.02089 |
| `delay_pass` | **true** |

정본이 이미 `lag1`이라 자기 자신과 비교하므로 유지율은 1.0이다. 게이트의 나머지 조건인
`p_nw < 0.05`도 통과했다 — h ≤ 5 cell(`cum 0→5`)의 `p_nw`가 0에 가깝다.

**외국인은 여기서 실패했다** (5일 cell의 p_nw = 0.053). 기관 쪽은 5일 구간에서도 신호가
뚜렷하다.

### 5.6 시간 placebo — 대상이 아니다

`null`이다. 최대 horizon이 20일이라 NW lag 19로 기준 59에 못 미친다. **검사에서 떨어진 게
아니다.**

### 5.7 시장 구성

KOSPI 41.5% / KOSDAQ 58.5%. 외국인과 같다 — 같은 표본을 쓰기 때문이다.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | **2,622일** |
| 날짜당 평균 종목 수 | **1,019개** |

외국인 family와 정확히 같다. 같은 원천·같은 완전 창 요구를 쓰기 때문이다.

---

## 7. 중복성

### A×B 교차 상관 — 35개 중 가장 작다

| 상대 family | 평균 순위상관 | 유효일 |
|---|---:|---:|
| `own_major_filing_activity` | −0.035 | 2,514 |
| `own_insider_filing_activity` | −0.019 | 2,354 |
| `mcap_krx_log` | −0.017 | 2,622 |
| `fin_value_z` | −0.014 | 1,928 |

**전부 0.04 미만이다.** Phase B 계열과 사실상 완전히 독립이다. 규모와도 −0.017로 무관하다.

### 확인하지 않은 중복 — 이 계열의 핵심 문제

`09_all_feature_results.md` §5가 정확히 지적했다.

> **세 개가 모두 반대로 나왔고 개인만 정방향이다.** 개인 순매수와 기관·외국인 순매수는
> 거의 거울상이라(기타법인 제외로 완전한 항등식은 아니지만), 사실상 **같은 현상의 양면**을
> 보고 있을 가능성이 높다. 즉 독립된 네 개의 발견이 아니라 하나의 발견으로 읽어야 한다.

수급 4개 family의 결과는 이렇게 배열된다.

| family | 부호 | IC (0→20) |
|---|---|---:|
| `flow_individual_netbuy_to_volume` | **+** | +0.0241 |
| `flow_inst_netbuy_to_volume` | − | −0.0209 |
| `flow_foreign_netbuy_to_volume` | − | −0.0131 |
| `flow_foreign_holding_ratio_chg` | − | −0.0080 |

**개인이 사면 기관·외국인이 판다.** 셋의 합이 완전한 항등식은 아니지만(기타법인 제외)
거의 거울상이다.

**A×A 상관이 없어 이 관계를 숫자로 확인할 수 없다.** 네 family를 별도 신호로 세는 게
타당한지가 미해결이다. 지금 상태로는 **하나의 발견을 네 번 세고 있을 가능성**을 배제하지
못한다.

---

## 8. 한계와 확인 못 한 것

1. **네 개의 발견이 아니라 하나일 수 있다** (§7). 가장 중요한 미확인 사항이다.
2. **반대 부호를 discovery로 세지 않았다.** 규율상 맞지만 §5.1·§5.2·§5.3이 전부
   "반대 방향이 안정적"이라고 말한다. 수급 계열 중 재등록 가치가 가장 높은 후보다.
3. **기관 세부 유형을 나누지 않았다** (§2.2). 연기금과 증권사 자기매매가 한 값에 섞여 있다.
   따로 보면 부호가 갈릴 가능성이 있는데 확인하지 않았다.
4. **당일 사용 가능성을 검증하지 않았다.** `lag1`이 정본인 건 보수적 선택이다.
5. **`_5d`·`_60d` 변형을 안 돌렸다** (§2.3).
6. **40일 이후를 안 봤다** (§4.4). |IC|가 관측 범위 끝에서 최대다.
7. **업종 중립화가 없다.** 기관 매매는 업종 단위로 움직이는 경향이 있다.
8. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
9. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나

**T1·T2 어느 후보에도 안 들어갔다.** 등급 D다.

기존 baseline 40개에는 flow 계열 15개(레거시)가 들어 있는데
(`07_phase1_acceptance_gate.md` §1) 이 산식이 그 안에 있는지는 확인되지 않는다.

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
A=f"research/output/horizon_scan/phase=A/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260827T221729-4e0ae8b0"
print(duckdb.sql(f"""
  select family, universe, scan_type, h_start, h_end, ic_mean, icir, t_nw,
         q5_spread_raw, q_fdr_global, n_dates
  from '{A}/core/horizon_ic.parquet'
  where family='flow_inst_netbuy_to_volume'
    and sample_kind='common_survivor' and hypothesis_role='primary'
  order by universe, scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| cell 전체 | `phase=A/…/run_id=20260827T221729-4e0ae8b0/core/horizon_ic.parquet` |
| 강건성·offset 20개 | 같은 run의 `cards/family_cards.json` |
| 기간별 IC | 같은 run의 `plots/flow_inst_netbuy_to_volume_subperiod_heatmap.png` |
| 산식 | `research/etl/features/flow.py:217` |
| 네 family를 하나로 읽으라는 지적 | `01_feature_candidate/09_all_feature_results.md` §5 |
| 한국 연구 근거 | `01_feature_candidate/02_feature_candidate.md` §3.2 |
