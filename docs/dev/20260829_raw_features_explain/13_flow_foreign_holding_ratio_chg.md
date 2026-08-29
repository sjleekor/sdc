# 13. `flow_foreign_holding_ratio_chg` — 외국인 지분율 변화

- 작성일: 2026-08-29
- family: `flow_foreign_holding_ratio_chg` · primary feature: **`flow_foreign_holding_ratio_chg_20d`**
- Phase A · fdr_family `flow` · 기대 부호 `+` · **관측 부호 `−`**
- 등급 **D** · `screen_pass` 실패 · discovery 0/6 cell
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**외국인 지분율이 오른 종목이 이후 덜 올랐다** (0→20일 IC −0.0080, q = 0.010).
[10_flow_foreign_netbuy_to_volume.md](10_flow_foreign_netbuy_to_volume.md)와 같은 방향인데
**크기가 절반이고 안정성은 훨씬 나쁘다.**

**수급 4개 중 가장 약한 family다.**

| 검사 | 결과 |
|---|---|
| 기간 일관성 | **2/5** — 방향이 오락가락 (다른 수급 셋은 0/5 또는 5/5) |
| 시간 placebo | **실패** (p = 0.347) |
| 비중첩 offset | 20개 전부 부호 검정 실패 (p 중앙값 0.948) |
| tradable 유지율 | **0.617** — 수급 계열 최저 |
| 유효 거래일 | **2,370일** — 시작이 2015-05-18로 늦다 |
| BH 통과 cell | 6개 중 3개 |

**반대 부호마저 신뢰할 수 없다.** `flow_inst_netbuy_to_volume`(0/5, 반대 방향 완벽 일관)과
성격이 전혀 다르다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/flow.py:241
foreign_holding_shares / NULLIF(float_shares_pit, 0)
    - LAG(foreign_holding_shares / NULLIF(float_shares_pit, 0), 20) OVER w
    AS flow_foreign_holding_ratio_chg_20d

-- w = PARTITION BY ticker, market ORDER BY trade_date
```

**외국인 지분율의 20거래일 전 대비 변화**다.

지분율 자체는 `외국인 보유주식수 / 유통주식수(PIT)`다. 그 값을 20거래일 전 값에서 뺀다.
단위는 **비율의 차이**다 — 0.01이면 지분율이 1%p 올랐다는 뜻이다.

### 2.2 순매수와 무엇이 다른가

같은 외국인을 보는데 재는 방식이 다르다.

| | `flow_foreign_netbuy_to_volume` | **`flow_foreign_holding_ratio_chg`** |
|---|---|---|
| 분자 | 20일 순매수 **주식 수** | 지분율의 **변화** |
| 분모 | 20일 총 **거래량** | (지분율 안에) 유통주식수 |
| 재는 것 | 거래 중 외국인 비중 | 보유 잔고의 이동 |
| 원천 | 순매수 집계 | 보유 잔고 집계 |

**거래 흐름(flow)과 보유 잔고(stock)의 차이다.** 이론상 순매수를 누적하면 보유 잔고 변화와
비슷해야 하지만, 실제로는 장외 거래·주식 발행·전환 등이 끼어 두 값이 갈린다.

### 2.3 분모가 외부 데이터라 표본이 짧아진다

**이 family만 유통주식수(`float_shares_pit`)를 쓴다.** 다른 수급 세 family는 거래량으로만
나누므로 `daily_ohlcv` 안에서 끝난다.

```yaml
# horizon_scan_config.yaml:352
readiness_dependencies: [feat_flow, dim_stock_pit_daily, label_scan]
```

`dim_stock_pit_daily`는 DART 지분 공시에서 만든 시점 정확(PIT) 유통주식수다. 그 커버리지가
표본을 결정한다.

**결과가 표본에 나타난다.**

| family | 유효 시작 | 유효 거래일 | 날짜당 종목 |
|---|---|---:|---:|
| `flow_foreign_netbuy_to_volume` | 2014-06-02 | 2,622 | 1,019 |
| `flow_inst_netbuy_to_volume` | 2014-06-02 | 2,622 | 1,019 |
| `flow_individual_netbuy_to_volume` | 2014-06-02 | 2,622 | 1,019 |
| **`flow_foreign_holding_ratio_chg`** | **2015-05-18** | **2,370** | **919** |

**시작이 1년 가까이 늦고 종목도 100개 적다.** 다른 셋과 같은 기간을 비교하는 게 아니다.

`horizon_scan_config.yaml`의 `shares` 블록이 PIT 규칙을 정한다.

```yaml
shares:
  disclosed_date_source: rcept_no_yyyymmdd      # 공시 접수번호의 날짜
  availability: next_krx_session                # 다음 거래일부터 사용 가능
  period_end_source: stlm_dt
  fallback_lag_days: {annual: 90, quarterly: 45}
```

**공시 접수 다음 거래일부터 쓴다.** look-ahead를 막는 처리다.

### 2.4 분모가 바뀌면 값이 튄다 — 구조적 약점

산식이 **지분율의 차분**이라 분자와 분모가 둘 다 움직인다.

```
지분율(t) − 지분율(t−20) = 외국인주식(t)/유통주식(t) − 외국인주식(t−20)/유통주식(t−20)
```

**유상증자·무상증자로 유통주식수가 늘면 외국인이 아무것도 안 해도 지분율이 떨어진다.**
그러면 이 피처가 "외국인이 팔았다"고 잘못 읽는다.

가격 피처들이 `ca_count_*` 마스킹으로 기업행동을 걸러내는 것과 달리 **수급 계열에는 그
마스킹이 없다.** 이 family가 특히 취약한 지점인데 이번에 확인하지 않았다.

### 2.5 정본 변형은 `lag1`

다른 수급 family와 같다. 산출물 24행이 전부
`feature = flow_foreign_holding_ratio_chg_20d_lag1`이다.

secondary(`_5d`, `_60d`)는 마트에 있지만 **사전등록에 없다.** 이 family만 `features`가
primary 하나뿐이다.

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/flow.py:241` |
| PIT 유통주식수 | `dim_stock_pit_daily` (`research/etl/stock_pit.py`) |
| PIT 규칙 | `research/analysis/horizon_scan_config.yaml`의 `shares` |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:344` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**정보 우위 가설인데, 거래가 아니라 보유를 본다.**

순매수는 며칠짜리 매매일 수 있지만 지분율 상승은 **실제로 들고 있는 양이 늘었다**는 뜻이다.
단기 트레이딩과 중장기 투자 판단을 구분하려는 의도다.

그래서 사전등록 horizon도 순매수 계열(5~20일)보다 길다.

### 3.2 기대 부호

`+`. 외국인 지분율이 오를수록 이후 초과수익률 순위가 높다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_config.yaml:350
primary_horizon_set: [20, 40, 60]
exploratory_horizon_set: [1, 2, 3, 5, 10, 120]
```

**수급 4개 중 유일하게 20~60일이다.** 나머지 셋은 5~20일이다. 보유 잔고 변화는 더 긴
시야의 신호라고 봤다는 뜻이다.

| | 사전등록 primary | 실제 관측 |
|---|---|---|
| 밴드 | 20~60일 | **없음** |
| 부호 | `+` | `−` |

**그런데 §4.1에서 보듯 20일에서 가장 강하고 60일로 갈수록 0에 수렴한다.** 사전에 예상한
"긴 시야" 구조와 맞지 않는다.

### 3.4 한국 시장 단서

`02_feature_candidate.md` §1의 우선순위 표에서 `foreign_holding_ratio/chg`는 §3.2 수급
도메인 4번 항목이다. 같은 절의 "한국 특화 연구에서 예측력이 가장 일관되게 보고되는 영역"
서술이 적용된다.

분류 좌표는 C3 × T1 × U다.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `lag1`)

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 | BH q | 부호 | 판정 |
|---|---|---:|---:|---:|---:|---:|---|---|
| cum | 0→20 | **−0.0080** | −0.176 | −2.73 | −0.03%p | 0.010 | 반대 | BH 통과, discovery 아님 |
| cum | 0→40 | −0.0063 | −0.144 | −1.95 | +0.09%p | 0.070 | 반대 | BH 통과, discovery 아님 |
| cum | 0→60 | −0.0042 | −0.093 | −1.34 | +0.29%p | 0.221 | 반대 | BH 실패 |
| bucket | 10→20 | −0.0079 | −0.180 | −3.59 | −0.04%p | 0.0006 | 반대 | BH 통과, discovery 아님 |
| bucket | 20→40 | −0.0030 | −0.075 | −1.18 | +0.12%p | 0.283 | 반대 | BH 실패 |
| bucket | 40→60 | −0.0002 | −0.006 | −0.09 | +0.16%p | 0.940 | 반대 | BH 실패 |

- family 최소 q: **0.0100**. 수급 4개 중 가장 크다(= 가장 약하다).
- 6개 중 **3개만** BH를 통과했다.

### 4.2 horizon이 길어질수록 사라진다

|IC|가 20일 0.0080 → 40일 0.0063 → 60일 0.0042로 **단조 감소**한다. bucket도
0.0079 → 0.0030 → 0.0002로 40~60일 구간에서는 사실상 0이다.

**사전등록 방향과 반대다.** §3.3에서 본 대로 이 family는 "긴 시야 신호"라고 보고 20~60일에
걸었는데, 실제로는 **밴드의 가장 짧은 끝에서만 무언가 있고 뒤로 갈수록 없어진다.**

같은 외국인 순매수 계열은 5~20일 밴드 안에서 |IC|가 증가했다. 두 결과를 합치면 **외국인
관련 신호는 20일 안팎에 몰려 있다**는 그림이 되는데, 이 family의 사전등록은 그 지점을
하한으로만 잡았다.

### 4.3 IC와 5분위 차이의 부호가 어긋난다

여섯 cell 중 넷에서 IC는 음수인데 5분위 차이는 양수다.

| cell | Rank IC | 5분위 차이 |
|---|---:|---:|
| cum 0→60 | −0.0042 | **+0.29%p** |
| cum 0→40 | −0.0063 | **+0.09%p** |
| bucket 40→60 | −0.0002 | **+0.16%p** |

[08_px_turnover_shock.md](08_px_turnover_shock.md) §4.2와 같은 종류의 어긋남이다. 다만
여기서는 **어느 값도 유의하지 않아** 잡음 안에서 부호가 흔들리는 것과 구분할 수 없다.

### 4.4 신호의 모양

| 항목 | 값 |
|---|---|
| `pattern_auto` | `no_signal` |
| `candidate_horizon_band` | 없음 |
| `onset_h` | 없음 |
| `peak_h_cum` | 20 (음수 방향 최대, 사전등록 밴드의 하한) |
| `peak_bucket` | [40, 60] |
| `half_life_bucket` | 없음 |
| `sign_flip_bucket` | 없음 |

---

## 5. 진짜인가 — 강건성

**수급 4개 중 유일하게 반대 방향조차 불안정하다.**

### 5.1 기간 일관성 — 2/5

- `valid_subperiods` = 5
- `sign_consistent_subperiods` = **2**

기대 부호가 `+`인데 2이므로 **두 구간은 양수, 세 구간은 음수**였다. 방향이 오락가락한다.

**같은 수급 계열과 비교하면 차이가 분명하다.**

| family | 기대 방향 일치 | 실제 의미 |
|---|---:|---|
| `flow_individual_netbuy_to_volume` | 5/5 | 기대 방향으로 완벽 일관 |
| `flow_inst_netbuy_to_volume` | 0/5 | **반대 방향으로 완벽 일관** |
| `flow_foreign_netbuy_to_volume` | 0/5 | **반대 방향으로 완벽 일관** |
| **`flow_foreign_holding_ratio_chg`** | **2/5** | **방향이 불안정** |

[02_px_mom_12_1.md](02_px_mom_12_1.md)와 같은 유형이다. 반대 부호를 결론으로 쓸 수 없다.

### 5.2 시간 placebo — 실패

| 항목 | 값 |
|---|---|
| `p_temporal_nw` | **0.347** |
| `temporal_null_pass` | **false** |

기준은 0.10이다. 시간축을 밀어 만든 가짜 신호가 관측값만큼 극단적인 결과를 **100번 중 약
35번** 만들어 냈다.

**수급 4개 중 이 검사를 받은 유일한 family다.** 나머지 셋은 최대 horizon이 20일이라 NW lag가
기준 59에 못 미쳐 대상이 아니었다. 이 family만 60일 cell이 있어 검사를 받았고, 떨어졌다.

### 5.3 비중첩 offset — 20개 전부 실패

| 항목 | 값 |
|---|---|
| 총 offset | 20개 (전부 유효) |
| 기대 방향 부호 일치율 | **0.0** |
| 부호 검정 p 중앙값 | **0.948** |
| 부호 검정 p 최솟값 | 0.677 |
| offset IC 범위 | −0.0123 ~ −0.0039 |

p 중앙값 0.948은 반대 방향 쪽이지만 **최솟값이 0.677로 1에서 멀다.** `flow_inst`의
최솟값 0.9992와 비교하면 훨씬 흐릿하다. offset IC 범위도 −0.012~−0.004로 넓다.

**반대 방향이 확실한 게 아니라 방향이 불분명하다.**

### 5.4 거래 가능한 종목만 남기면 — 38% 줄어든다

| universe | IC (cum 0→20) | 유지율 |
|---|---:|---:|
| `broad` | −0.0080 | — |
| `tradable` | −0.0049 | **0.617** |

**수급 계열 최저이고, 게이트 기준 0.50에 가깝다.**

| family | 유지율 |
|---|---:|
| `flow_inst_netbuy_to_volume` | 1.073 |
| `flow_individual_netbuy_to_volume` | 1.009 |
| `flow_foreign_netbuy_to_volume` | 0.707 |
| **`flow_foreign_holding_ratio_chg`** | **0.617** |

유동성이 좋은 종목에서 신호가 크게 약해진다.

### 5.5 생존편향

| sample_kind | IC (cum 0→20) |
|---|---:|
| `common_survivor` | −0.0080 |
| `available` | −0.0071 |

차이가 작고 방향이 같다. `attrition_warning = false`. card에는
`survival_bias_unresolved` 한계가 붙어 있다.

### 5.6 지연 — 대상이 아니다

`native_ic`와 `lag1_ic`가 같고 `delay_pass`는 `null`이다. 사전등록 최소 horizon이 20일이라
h ≤ 5 cell이 없어 게이트 대상이 아니다.

같은 수급 계열이라도 5~20일 밴드인 셋은 이 게이트를 받았고
([10](10_flow_foreign_netbuy_to_volume.md)은 실패, [11](11_flow_inst_netbuy_to_volume.md)·
[12](12_flow_individual_netbuy_to_volume.md)는 통과), 이 family만 안 받았다.

### 5.7 시장 구성 — KOSDAQ 쏠림이 가장 크다

- KOSPI **37.3%** / KOSDAQ **62.7%**

35개 family 중 KOSDAQ 비중이 가장 높은 축이다. 다른 수급 셋이 41.5%/58.5%인 것과 다르다.

**§2.3의 PIT 유통주식수 커버리지 때문일 가능성이 있다.** 어느 시장의 종목이 더 많이
빠졌는지에 따라 구성이 달라진다. 확인하지 않았다.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2015-05-18** ~ 2025-02-05 |
| 유효 거래일 | **2,370일** |
| 날짜당 평균 종목 수 | **919개** |

§2.3에서 본 대로 **수급 4개 중 유일하게 시작일이 다르다.** 다른 셋은 2014-06-02이고 종목도
100개 많다.

**다른 수급 family와 결과를 비교할 때 이 차이를 빼놓으면 안 된다.** 같은 기간·같은 종목을
보고 있지 않다.

---

## 7. 중복성

| 상대 family | 평균 순위상관 | 유효일 |
|---|---:|---:|
| `mcap_krx_log` | +0.024 | 2,371 |
| `fin_value_z` | −0.018 | 1,928 |
| `fin_accruals_to_assets` | −0.016 | 1,927 |
| `ev_payout_yield` | −0.016 | 2,175 |

**전부 0.03 미만이다.** Phase B 계열과 사실상 독립이다.

### 확인하지 않은 중복

`flow_foreign_netbuy_to_volume`과의 상관이 없다. **같은 외국인을 거래 기준과 잔고 기준으로
재는 두 지표**라 강하게 겹칠 것으로 보이는데 A×A 상관 산출물이 없다.

§2.2에서 본 대로 두 값이 이론상 연결돼 있으므로, 이 관계를 재지 않으면 **외국인 관련 신호를
두 번 세고 있는지** 알 수 없다.

---

## 8. 한계와 확인 못 한 것

1. **반대 부호를 결론으로 쓸 수 없다.** 기간 2/5, 시간 placebo 실패, offset 부호 흐릿함.
   세 검사가 모두 불안정을 가리킨다.
2. **기업행동 처리가 없다** (§2.4). 증자로 유통주식수가 늘면 외국인이 아무것도 안 해도
   지분율이 떨어진다. 수급 계열에는 `ca_count_*` 마스킹이 없다. **이 family가 가장 취약한
   지점인데 확인하지 않았다.**
3. **표본이 다른 수급 family와 다르다** (§6). 시작이 1년 늦고 종목이 100개 적다.
4. **외국인 순매수와의 상관이 없다** (§7).
5. **사전등록 밴드가 신호 위치와 어긋난다** (§4.2). 20일에서 최대인데 하한이 20일이다.
   짧은 쪽은 exploratory로 내려 확인하지 않았다.
6. **KOSDAQ 쏠림 원인을 모른다** (§5.7).
7. **secondary 변형이 등록되지 않았다** (§2.5). `_5d`·`_60d`가 마트에 있는데 안 썼다.
8. **업종 중립화가 없다.**
9. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
10. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나

**T1·T2 어느 후보에도 안 들어갔다.** 등급 D다.

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
A=f"research/output/horizon_scan/phase=A/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260827T221729-4e0ae8b0"
# 수급 4개의 표본 차이를 함께 확인한다
print(duckdb.sql(f"""
  select family, min(effective_sample_start) as start, max(n_dates) as n_dates,
         round(avg(n_obs_mean)) as names_per_date
  from '{A}/core/horizon_ic.parquet'
  where family like 'flow_%' and universe='broad'
    and sample_kind='common_survivor' and hypothesis_role='primary'
  group by family order by family
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| cell 전체 | `phase=A/…/run_id=20260827T221729-4e0ae8b0/core/horizon_ic.parquet` |
| 강건성·offset 20개 | 같은 run의 `cards/family_cards.json` |
| 기간별 IC | 같은 run의 `plots/flow_foreign_holding_ratio_chg_subperiod_heatmap.png` |
| 커버리지 차트 | 같은 run의 `plots/flow_foreign_holding_ratio_chg_coverage.png` |
| 산식 | `research/etl/features/flow.py:241` |
| PIT 유통주식수 규칙 | `research/analysis/horizon_scan_config.yaml`의 `shares` |
| 서술 대조 | `01_feature_candidate/09_all_feature_results.md` §5 |
