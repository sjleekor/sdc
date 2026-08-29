# 10. `flow_foreign_netbuy_to_volume` — 외국인 순매수 강도

- 작성일: 2026-08-29
- family: `flow_foreign_netbuy_to_volume` · primary feature: **`flow_foreign_netbuy_to_volume_20d`**
- Phase A · fdr_family `flow` · 기대 부호 `+` · **관측 부호 `−`**
- 등급 **D** · `screen_pass` 실패 · discovery 0/6 cell
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**한국 시장에서 가장 강할 것으로 기대했던 축인데 방향이 반대로 나왔다.** 최근 20일 외국인
순매수 비중이 높은 종목이 이후 더 오른 게 아니라 **덜 올랐다** (0→20일 IC −0.0131,
q = 2.2e-05).

`px_turnover_shock`와 같은 형태다 — **기대 방향 일치 구간 0/5**, 즉 **다섯 구간 전부에서
반대 방향으로 일관**됐다. 우연한 잡음이 아니다.

다만 **크기가 작다.** |IC| 0.013은 가격 계열 A등급(0.05~0.14)의 10분의 1 수준이고,
20일 5분위 수익률 차이는 **−0.23%p**다.

거래가능 유지율도 **0.707**로 낮은 편이고, **지연 게이트를 실제로 적용해 통과하지 못했다**
(`delay_pass = false`). 수급 계열에서 이 게이트가 실제로 작동한 사례다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/flow.py:205
CASE WHEN COUNT(foreign_net_buy_volume) OVER w20 = 20
          AND COUNT(total_volume) OVER w20 = 20
     THEN SUM(foreign_net_buy_volume) OVER w20
          / NULLIF(SUM(total_volume) OVER w20, 0) END
    AS flow_foreign_netbuy_to_volume_20d

-- w20 = PARTITION BY ticker, market ORDER BY trade_date
--       ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
```

**최근 20거래일 외국인 순매수 주식 수를 같은 기간 총 거래량으로 나눈 값**이다.

- 양수면 외국인이 순매수했다
- 0.05면 20일 총거래량의 5%만큼을 외국인이 순매수했다
- 음수면 순매도했다

### 2.2 왜 거래량으로 나누는가

순매수 주식 수 자체를 쓰면 **거래가 많은 대형주가 항상 큰 값**을 갖는다. 규모 축과
뒤섞인다 — [07_px_amihud_20d.md](07_px_amihud_20d.md) §7이 보여 준 문제와 같다.

같은 기간 총거래량으로 나누면 **"그 종목 거래의 몇 %가 외국인 순매수였나"**가 되어 규모가
상쇄된다. 실제로 §7에서 규모 상관이 +0.033에 그친다.

### 2.3 완전 창을 요구한다

```sql
CASE WHEN COUNT(foreign_net_buy_volume) OVER w20 = 20
          AND COUNT(total_volume) OVER w20 = 20
```

**20일 창의 20행이 전부 있어야 한다.** 부등호가 아니라 `= 20`이다. 설정에도 있다.

```yaml
# horizon_scan_config.yaml
flow:
  require_complete_window: true
```

가격 피처 중 `px_amihud_20d`가 부분 창을 허용했던 것과 다르다. 수급 데이터는 결측 구간이
있어 부분 창을 허용하면 며칠치만으로 만든 값이 20일치 값과 섞인다.

### 2.4 거래정지일을 먼저 걸러낸다

`sessioned` CTE가 `WHERE q.valid_session_idx IS NOT NULL`로 거래정지일을 창에 들어가기 전에
제거한다 (`flow.py:126`). 코드 주석이 이유를 적어 뒀다.

> flow's windows must match, or a halt-day row silently consumes one of the N rolling slots
> and both the native ratio and its `_lag1` stop meaning "N valid sessions."

가격 피처와 같은 세션 정의를 쓴다.

### 2.5 정본 변형이 `lag1`이다 — 가격 계열과 결정적으로 다르다

```yaml
# horizon_scan_config.yaml:315
official_feature_variant: lag1
variant_columns:
  native_t: flow_foreign_netbuy_to_volume_20d
  lag1: flow_foreign_netbuy_to_volume_20d_lag1
```

**수급 4개 family는 전부 `lag1`이 정본이다.** 가격 계열이 `native_t`인 것과 반대다.

```yaml
execution:
  price_default_official_variant: native_t
  flow_unverified_same_day_variant: lag1
```

이유는 **당일 사용 가능 여부가 확인되지 않았기 때문이다.** 가격은 장 마감에 확정되지만
투자자별 순매수 집계는 KRX가 언제 공표하는지, 그날 장 마감 시점에 쓸 수 있는지가 검증되지
않았다. 그래서 **하루 늦춘 값을 정본으로 쓴다.**

산출물에서도 확인된다 — 이 family의 `horizon_ic.parquet` 24행이 전부
`feature = flow_foreign_netbuy_to_volume_20d_lag1`, `feature_variant = lag1`이다.

**보수적 선택이고, 그만큼 실제 성능을 과소평가할 수 있다.** 당일 사용이 가능하다면 신호가
더 강할 수 있는데 확인하지 않았다.

### 2.6 등록됐지만 이번에 안 돌린 것

secondary로 `_5d`와 `_60d`가 등록돼 있는데 이번 run에는 20일 것만 있다. **다른 관측 창에서
결과가 다른지 확인하지 않았다.**

### 2.7 원천과 중복 제거

원천은 `krx_security_flow_raw`다. 7,600만 행으로 이 프로젝트에서 가장 큰 테이블이고,
KRX와 pykrx가 같은 자연키를 갖고 있어 **KRX 우선 중복 제거**를 거친다. 결과는 55,918,702
고유 행이고 회귀 테스트에 고정돼 있다 (`flow.py` 모듈 docstring).

주의할 점 하나. 세 투자자(외국인·기관·개인) 순매수 합은 **항등식이 아니다.** 기타법인이
빠져 있다. 그래서 "셋을 더하면 0"이라는 성질을 쓰는 피처는 만들지 않았다.

### 2.8 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/flow.py:205` |
| 세션 필터 | `research/etl/features/flow.py:126` |
| 중복 제거 | `research/etl/features/flow.py:53` |
| lag1 정본 규칙 | `research/analysis/horizon_scan_config.yaml`의 `execution` |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:301` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**정보 우위 가설이다.**

외국인 투자자가 국내 개인보다 정보력·분석력이 낫고, 그들의 매수는 앞으로 오를 종목을
가리킨다는 설명이다. 한국처럼 외국인 비중이 크고 투자자 유형별 집계가 매일 공표되는
시장에서는 이 신호를 **누구나 볼 수 있다는 점**이 특징이다.

### 3.2 왜 이 축을 최우선으로 뒀나

`02_feature_candidate.md` §1의 우선순위 표에서 이 축이 **2위**다. 그리고 §3.2가 이렇게
적었다.

> 한국 특화 연구에서 **예측력이 가장 일관되게 보고되는 영역** (Hong & Lee 2011; Bae, Min …)

`00_raw_feature_inventory.md`도 이 도메인을 "완전 미사용 7,700만 행"으로 지목했다. 데이터는
쌓여 있는데 안 쓰고 있던 영역이라 기대가 컸다.

### 3.3 기대 부호

`+`. 외국인 순매수 비중이 높을수록 이후 초과수익률 순위가 높다.

### 3.4 사전등록 horizon

```yaml
# horizon_scan_config.yaml:307
primary_horizon_set: [5, 10, 20]
exploratory_horizon_set: [1, 2, 3, 40, 60, 120]
```

수급 정보는 **몇 주 안에 가격에 반영된다**고 봤다. 40일 이후는 exploratory로 내렸다.

| | 사전등록 primary | 실제 관측 |
|---|---|---|
| 밴드 | 5~20일 | **없음** |
| 부호 | `+` | `−` |
| 패턴 | 단기 반영 | `no_signal` |

### 3.5 근거 문헌

Hong & Lee (2011), Bae & Min 등 한국 시장 대상 연구.

분류 좌표는 **C3(수급·소유·내부자)** × T1(변화) × U다.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `lag1`)

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 | BH q | 부호 | 판정 |
|---|---|---:|---:|---:|---:|---:|---|---|
| cum | 0→5 | −0.0030 | −0.067 | −1.94 | +0.004%p | 0.070 | 반대 | BH 실패 |
| cum | 0→10 | −0.0076 | −0.166 | −3.45 | −0.06%p | 0.0009 | 반대 | BH 통과, discovery 아님 |
| cum | 0→20 | **−0.0131** | −0.278 | −4.36 | **−0.23%p** | 2.2e-05 | 반대 | BH 통과, discovery 아님 |
| bucket | 0→5 | −0.0030 | −0.067 | −1.94 | +0.004%p | 0.070 | 반대 | BH 실패 |
| bucket | 5→10 | −0.0089 | −0.198 | −5.63 | −0.06%p | ~0 | 반대 | BH 통과, discovery 아님 |
| bucket | 10→20 | −0.0124 | −0.279 | −5.66 | −0.16%p | ~0 | 반대 | BH 통과, discovery 아님 |

- family 최소 q: **2.25e-05**.
- 6개 중 4개가 BH를 통과했지만 부호가 반대라 **discovery는 0개**다.

### 4.2 크기가 작다

|IC| 0.0131은 통계적으로는 뚜렷하지만 **경제적으로는 작다.**

| family | 대표 \|IC\| | 5분위 차이 |
|---|---:|---:|
| `px_idio_vol_60d` | 0.143 (60일) | +2.99%p |
| `px_maxret_20d` | 0.113 (60일) | +1.79%p |
| `px_reversal_5d` | 0.053 (3일) | +0.30%p |
| **`flow_foreign_netbuy_to_volume`** | **0.013 (20일)** | **−0.23%p** |

t값이 −4.4로 큰 것은 표본이 2,622일로 길기 때문이다. **표본이 크면 작은 효과도 유의해진다.**
유의성과 크기를 분리해서 봐야 한다.

### 4.3 IC가 커질수록 방향이 뚜렷해진다

horizon이 길어질수록 |IC|가 단조 증가한다 (0.003 → 0.008 → 0.013). bucket도 마찬가지다
(0.003 → 0.009 → 0.012).

**반대 방향 신호가 시간이 갈수록 강해진다**는 뜻이다. 5일에서는 거의 없고 20일에서 가장
크다. `peak_h_cum = 20`으로 사전등록 최대치에서 정점이다. **더 길게 보면 더 강해질 수도
있는데 40일 이후는 exploratory로 내려 확인하지 않았다.**

### 4.4 신호의 모양

| 항목 | 값 |
|---|---|
| `pattern_auto` | `no_signal` |
| `candidate_horizon_band` | 없음 |
| `onset_h` | 없음 |
| `peak_h_cum` | 20 (음수 방향 최대, 관측 범위 끝) |
| `peak_bucket` | [0, 5] |
| `half_life_bucket` | 없음 |
| `sign_flip_bucket` | 없음 |

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 기대 방향 0/5 = 반대 방향 5/5

- `valid_subperiods` = 5
- `sign_consistent_subperiods` = **0**

기대 부호가 `+`인데 0이므로 **다섯 구간 전부에서 IC가 음수**였다
([08_px_turnover_shock.md](08_px_turnover_shock.md) §5.1과 같은 읽기다).

**반대 방향이 매우 안정적이다.** 특정 국면의 현상이 아니다.

### 5.2 비중첩 offset — 20개 전부, 반대 방향으로

| 항목 | 값 |
|---|---|
| 총 offset | 20개 (전부 유효) |
| 기대 방향 부호 일치율 | **0.0** |
| 부호 검정 p 중앙값 | **0.9992** |
| offset IC 범위 | −0.0166 ~ −0.0092 |

p가 1에 붙어 있다 = **반대 방향으로 강하게 유의하다.** IC 범위도 전부 음수다.

### 5.3 거래 가능한 종목만 남기면 — 29% 줄어든다

| universe | IC (cum 0→20) | 유지율 |
|---|---:|---:|
| `broad` | −0.0131 | — |
| `tradable` | −0.0093 | **0.707** |

게이트 기준 0.50은 넘지만 **가격 계열 A등급(0.85~1.04)보다 확연히 낮다.**

유동성이 좋은 종목에서 반대 신호가 약해진다. 외국인 순매수 비중이라는 지표 자체가
거래가 적은 종목에서 더 극단적인 값을 갖기 때문일 수 있는데, 확인하지 않았다.

### 5.4 생존편향

| sample_kind | IC (cum 0→20) |
|---|---:|
| `common_survivor` | −0.0131 |
| `available` | −0.0120 |

차이가 작고 방향이 같다. `attrition_warning = false`.

### 5.5 지연 게이트 — 실제로 적용했고 실패했다

**수급 계열에서 이 게이트가 실제로 작동한 사례다.**

| 항목 | 값 |
|---|---|
| `native_ic` | −0.01310 |
| `lag1_ic` | −0.01310 |
| `delay_pass` | **false** |

두 값이 같다. 이 family는 **정본이 이미 `lag1`이라 자기 자신과 비교**한다
(`horizon_scan_runner.py:980`: "a family whose official variant is already lag1 evaluates
this against itself"). 유지율은 1.0이다.

**그런데 실패했다.** 게이트에 조건이 하나 더 있기 때문이다.

```python
# research/analysis/horizon_scan_runner.py:991
p_ok = p_nw_lag1 is not None and math.isfinite(p_nw_lag1) and p_nw_lag1 < p_max
```

기준은 `decision.delay_confirm_p_nw: 0.05`이고, 지연 게이트가 적용되는 h ≤ 5 cell
(`cum 0→5`)의 `p_nw`가 **0.05294**다. 0.05를 넘어 통과하지 못했다.

**5일 구간에서는 반대 신호조차 유의하지 않다**는 뜻이다. §4.3에서 본 대로 짧은 구간에는
신호가 거의 없다.

### 5.6 시간 placebo — 대상이 아니다

`p_temporal_nw`, `temporal_null_pass` 모두 `null`이다. 최대 horizon이 20일이라 NW lag가
19로 기준 59에 못 미친다. **검사에서 떨어진 게 아니다.**

### 5.7 시장 구성

KOSPI 41.5% / KOSDAQ 58.5%.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | **2,622일** |
| 날짜당 평균 종목 수 | **1,019개** |

가격 계열과 표본 길이는 같지만 **날짜당 종목 수가 78개 적다** (`px_maxret_20d` 1,097개
대비). §2.3의 완전 창 요구 때문이다.

원천 자체는 2007-06-05부터 있지만 scan 표본 시작은 2014-06-01로 공통 고정돼 있다.

---

## 7. 중복성

| 상대 family | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `fin_value_z` | −0.051 | 1,928 | −0.21 ~ +0.24 |
| `ev_payout_yield` | −0.042 | 2,175 | −0.21 ~ +0.13 |
| `mcap_krx_log` | +0.033 | 2,622 | −0.25 ~ +0.25 |
| `fin_log_mcap` | +0.032 | 2,391 | −0.47 ~ +0.24 |

**전부 0.06 미만이다.** Phase B 계열과 사실상 독립이다.

**규모 상관이 +0.033에 그친 게 §2.2 설계가 작동한 증거다.** 거래량으로 나누지 않았다면
대형주 편향이 그대로 들어왔을 것이다.

### 확인하지 않은 중복

같은 Phase A 안의 `flow_inst_netbuy_to_volume`, `flow_individual_netbuy_to_volume`과의
상관이 없다. **세 투자자 순매수는 서로의 거울상에 가깝다** — 한쪽이 사면 다른 쪽이 판다.
합이 항등식은 아니지만(§2.7) 강한 음의 상관이 예상된다.

**A×A 상관이 없어 이 관계를 확인할 수 없다.** 세 family를 별도 신호로 세는 게 타당한지가
미해결이다.

---

## 8. 한계와 확인 못 한 것

1. **반대 부호를 discovery로 세지 않았다.** 규율상 맞지만 §5.1·§5.2에서 보듯 반대 방향이
   매우 안정적이다. 새 config로 재등록할 가치가 있는 후보다.
2. **크기가 작다** (§4.2). 통계적 유의성이 표본 크기에서 나온다.
3. **당일 사용 가능성을 검증하지 않았다** (§2.5). `lag1`을 정본으로 쓴 건 보수적 선택이고,
   당일 값이 쓸 수 있다면 결과가 달라질 수 있다.
4. **`_5d`·`_60d` 변형을 안 돌렸다** (§2.6).
5. **세 투자자 유형 간 상관이 없다** (§7). 가장 중요한 미확인 중복이다.
6. **40일 이후를 안 봤다** (§4.3). |IC|가 관측 범위 끝에서 최대인데 그 너머는 exploratory다.
7. **tradable 유지율이 0.707로 낮다** (§5.3). 원인을 분석하지 않았다.
8. **업종 중립화가 없다.** 외국인 매수는 업종 단위로 움직이는 경향이 있는데 시장 두 그룹
   안에서만 순위를 매겼다.
9. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
10. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나

**T1·T2 어느 후보에도 안 들어갔다.** 등급 D다.

같은 수급 계열에서는 `flow_individual_netbuy_to_volume`만 T1 후보 5개에 들어갔다
([12_flow_individual_netbuy_to_volume.md](12_flow_individual_netbuy_to_volume.md) 참조).

**기존 baseline 40개 피처에는 flow 계열 15개(레거시)가 이미 들어 있다**
(`07_phase1_acceptance_gate.md` §1). 그 15개에 이 산식이 포함되는지는 이번 문서로 확인되지
않는다.

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
A=f"research/output/horizon_scan/phase=A/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260827T221729-4e0ae8b0"
# 세 투자자 유형을 나란히 본다
print(duckdb.sql(f"""
  select family, scan_type, h_start, h_end, ic_mean, icir, t_nw,
         q5_spread_raw, q_fdr_global, bh_pass, expected_sign_pass, n_dates
  from '{A}/core/horizon_ic.parquet'
  where family like 'flow_%netbuy_to_volume' and universe='broad'
    and sample_kind='common_survivor' and hypothesis_role='primary'
  order by scan_type, h_end, family
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| cell 전체 | `phase=A/…/run_id=20260827T221729-4e0ae8b0/core/horizon_ic.parquet` |
| 강건성·offset 20개 | 같은 run의 `cards/family_cards.json` |
| 기간별 IC | 같은 run의 `plots/flow_foreign_netbuy_to_volume_subperiod_heatmap.png` |
| 지연 대조 차트 | 같은 run의 `plots/flow_foreign_netbuy_to_volume_native_vs_lag1.png` |
| 산식 | `research/etl/features/flow.py:205` |
| 지연 게이트 코드 | `research/analysis/horizon_scan_runner.py:969` |
| 한국 연구 근거 | `01_feature_candidate/02_feature_candidate.md` §3.2 |
| 서술 대조 | `01_feature_candidate/09_all_feature_results.md` §5 |
