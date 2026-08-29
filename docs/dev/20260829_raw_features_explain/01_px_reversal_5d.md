# 01. `px_reversal_5d` — 5일 단기 반전

- 작성일: 2026-08-29
- family: `px_reversal_5d` · primary feature: `px_reversal_5d` · domain: `price`
- Phase A · fdr_family `price` · 기대 부호 `+` · 관측 부호 `+`
- 등급 **A** · `screen_pass` 통과 · AB discovery 7/7 cell
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**최근 5거래일 많이 떨어진 종목이 이후 며칠 동안 시장 대비 더 오른다.** 검증한 35개 family
중 통계적으로 가장 뚜렷하다 (BH q ≈ 3.3e-95, 2,622일 중 1,772일에서 IC가 양수). 다만 효과는
3일에 정점을 찍고 5~10일 구간에서 거의 사라지며, 5일 보유 기준 5분위 수익률 차이는
**0.38%p**에 그친다. 회전이 빠른 만큼 거래비용에 그대로 노출된다.

T1 모델 후보 5개 중 하나로 들어갔지만, k=100 비용 반영 수익이 h20에서 baseline보다
낮아 묶음 전체가 **비채택**됐다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/price.py:97
-SUM(log_ret) OVER w5 AS px_reversal_5d

-- w5 = PARTITION BY ticker, market ORDER BY trade_date
--      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
```

최근 5거래일 로그수익률 합에 **마이너스를 붙인 값**이다. 부호를 뒤집었으므로
**값이 클수록 최근에 많이 떨어진 종목**이다.

`log_ret`은 `ln(종가 / 직전 유효세션 종가)`다 (`research/etl/trading_panel.py:25`).

### 2.2 창과 세션 정의

- 창은 **달력일이 아니라 거래행 5개**다 (`ROWS BETWEEN 4 PRECEDING AND CURRENT ROW`).
- 거래정지일(`open=high=low=0`)은 패널에서 아예 빠진다. 창이 정지 구간을 건너뛰므로
  정지를 사이에 둔 5일이 달력으로는 훨씬 길 수 있다.
- 종목이 패널에 들어오려면 warm-up을 통과해야 한다 — 직전 60행 중 유효 세션이 40일 이상
  (`horizon_scan_config.yaml`의 `universe.warmup_window: 60`, `warmup_min_valid: 40`).
  신규 상장·장기 정지 종목을 걸러 낸다.

### 2.3 무엇이 NULL이 되는가

```sql
-- research/etl/features/price.py:146
CASE WHEN ca_count_5 > 0 THEN NULL ELSE px_reversal_5d END
```

**5일 창 안에 기업행동이 한 건이라도 있으면 값을 통째로 버린다.** 액면분할·무상증자 같은
사건이 섞인 수익률을 반전 신호로 오해하지 않으려는 장치다. 기업행동 판정 기준은
`quality.ca_abs_share_change: 0.25`, `ca_ratio_product_range: [0.8, 1.25]`다.

가격 계열 중 마스킹 창이 가장 짧아서(5일) 버려지는 행이 적다. 모멘텀 계열은 252일 창을 쓰기
때문에 훨씬 많이 버린다 — 같은 가격 원천인데 커버리지가 달라지는 이유다.

### 2.4 변형

| variant | 컬럼 | 뜻 |
|---|---|---|
| `native_t` (정본) | `px_reversal_5d` | 당일 종가까지 반영한 값 |
| `lag1` | `px_reversal_5d_lag1` | 직전 유효 세션 값 |

정본은 `native_t`다 (`execution.price_default_official_variant`). 가격은 장 마감 시점에
확정되므로 당일 값을 쓰는 데 PIT 문제가 없다. `lag1`은 "하루 늦게 써도 신호가 남는가"를 보는
게이트 용도다 (§5.1).

### 2.5 표준화

이 단계에서는 **표준화하지 않는다.** 원값 그대로 저장한다 (`price.py` 모듈 docstring:
"winsor/log/z-score is the model preprocess step, not here"). IC 계산은 `(거래일, 시장)`
안의 순위만 쓰므로 표준화 여부와 무관하다.

**횡단 정규화 그룹은 KOSPI·KOSDAQ 둘뿐이다.** 업종 중립화는 없다.

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/price.py:97` (마스킹 `:146`, lag1 `:171`) |
| 유효세션·수익률 | `research/etl/trading_panel.py:6` |
| 패널 편입 규칙 | `research/etl/universe.py:143` |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:183` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**행태 편향과 유동성 공급의 대가다.** 단기 급락은 정보가 아니라 매도 압력 때문에 생기는
경우가 많고, 그 압력이 풀리면 가격이 되돌아온다. 급락한 주식을 받아 주는 쪽이 그 대가를
받는다는 설명이다 (Jegadeesh 1990).

위험 프리미엄 설명이 아니라 **가격 압력·과잉반응 설명**이라는 점이 중요하다. 그래서
"며칠 안에 되돌아온다"는 예측이 따라 나온다. 되돌림이 몇 달 걸린다면 이 가설이 아니다.

### 3.2 기대 부호

`+`. 산식이 이미 부호를 뒤집어 놨으므로, **값이 클수록(많이 떨어졌을수록) 이후 초과수익률
순위가 높다**는 뜻이다.

### 3.3 사전등록 horizon — 이것이 곧 가설이다

```yaml
# horizon_scan_config.yaml:190
primary_horizon_set: [1, 2, 3, 5, 10]
exploratory_horizon_set: [20, 40, 60, 120]
include_bucket_primary: true
```

가격 압력 해소 가설이 맞다면 **1~10일에서 신호가 나야 하고 20일 이후에는 없어야 한다.**
20일 이상을 exploratory로 내려둔 것이 그 예측이다.

| | 사전등록 primary | 실제 관측 (`candidate_horizon_band`) |
|---|---|---|
| 밴드 | 1~10일 | **1~10일** |

**정확히 맞았다.** 예측한 구간에서 신호가 났고 그 밖으로 새지 않았다.

### 3.4 한국 시장 단서

`02_feature_candidate.md` §3.1 P2가 미리 적어 둔 내용이다.

- 한국은 개인 비중이 높아 **반전이 모멘텀보다 강한 편**이다. 이번 결과가 그대로 재현했다 —
  모멘텀 계열 2개는 부호가 반대로 나왔는데 반전만 살아남았다.
- **거래비용과 bid-ask bounce에 민감하다.** 5일 창은 회전이 빠르고, 급락 종목은 스프레드가
  벌어져 있다. 측정된 IC가 그대로 수익이 되지 않는다.
- 회전율·Amihud·MAX와의 상호작용을 확인하라고 적혀 있었는데, 이번 scan은 단변량만
  봤으므로 **아직 확인하지 않았다.**

분류 좌표는 C1(시장가격 기반 기업상태) × T1(변화) × U(무조건부)다
(`11_feature_taxonomy.md` §3).

### 3.5 근거 문헌

Jegadeesh (1990), *Evidence of Predictable Behavior of Security Returns*. 등급 A/B
(널리 복제된 결과).

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

| scan | horizon | Rank IC | ICIR | t(NW) | **5분위 수익률 차이** | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→1 | +0.0470 | 0.408 | 20.91 | +0.11%p | ~0 | discovery |
| cum | 0→2 | +0.0514 | 0.471 | 20.25 | +0.22%p | ~0 | discovery |
| cum | 0→3 | **+0.0533** | 0.498 | 18.69 | +0.30%p | ~0 | discovery |
| cum | 0→5 | +0.0491 | 0.480 | 15.26 | **+0.38%p** | ~0 | discovery |
| cum | 0→10 | +0.0362 | 0.376 | 9.76 | +0.36%p | ~0 | discovery |
| bucket | 0→5 | +0.0491 | 0.480 | 15.26 | +0.38%p | ~0 | discovery |
| bucket | 5→10 | +0.0080 | 0.093 | 2.81 | **−0.01%p** | 0.0079 | discovery |

- family 최소 q: Phase A global BH **3.29e-95**, 결합 AB BH **6.72e-95**.
  두 기준 모두에서 35개 family 중 가장 작다 (다음이 `px_idio_vol_60d` 8.9e-46).
- 7개 cell 전부 BH를 통과했고 결합 AB에서도 전부 discovery로 남았다.
- `bucket 0→5`가 `cum 0→5`와 같은 값인 것은 정의상 당연하다 (시작이 0이면 두 라벨이 같다).

### 4.2 IC를 수익률로 읽으면

**여기서 그림이 바뀐다.**

IC는 0→3일이 제일 크지만(0.0533) 수익률 차이는 0→5일이 제일 크다(0.38%p). 보유 기간이
길수록 누적 수익이 커지기 때문이다. IC 최대점과 수익 최대점이 다르다.

그리고 **0.38%p는 5거래일치 시장 대비 초과수익이고 거래비용 차감 전이다.**

- 왕복 60bp(T1 게이트 가정)를 빼면 남는 게 거의 없다.
- 5일마다 갈아타면 연 50회 회전이다. T1 실측 회전율은 5일 리밸런싱에서 68~78%였다.
- `bucket 5→10`은 IC가 +0.008로 통계적으로는 유의하지만(q=0.0079) 수익률 차이가
  **−0.01%p로 사실상 0**이다. **유의성과 경제성이 갈라지는 교과서적인 사례**다.

### 4.3 신호의 모양

`cards/family_cards.json` 기록이다.

| 항목 | 값 | 읽는 법 |
|---|---|---|
| `onset_h` | 1 | 다음 거래일부터 바로 나온다 |
| `peak_h_cum` | 3 | 누적 IC는 3일에 정점 |
| `peak_bucket` | [0, 5] | 구간 기준으로는 0~5일이 가장 강함 |
| `half_life_bucket` | [5, 10] | 5~10일 구간에서 절반 아래로 떨어짐 |
| `sign_flip_bucket` | 없음 | 부호가 뒤집히는 구간이 없다 |
| `pattern_auto` | `immediate` | 지연 없이 즉시 반응하는 형태 |

**onset 1일 · peak 3일 · half-life 5~10일.** 이 세 숫자가 "단기 반전"이라는 이름보다
피처를 정확하게 설명한다. 신호를 쓰려면 늦어도 며칠 안에 실행해야 한다는 뜻이다.

### 4.4 일별 IC의 방향 일치

family card의 비중첩 offset 기록에서:

- 유효 거래일 2,622일 중 **1,772일(67.6%)에서 IC가 양수**
- 부호 검정 p = 3.75e-74

3분의 2 이상의 날에 같은 방향이었다. 평균만 보고 "몇몇 날 크게 맞아서 생긴 평균"이라고
의심할 여지가 작다.

---

## 5. 진짜인가 — 강건성

### 5.1 하루 늦게 써도 남는가 (delay)

| variant | IC | 유지율 |
|---|---:|---:|
| `native_t` | 0.0470 | — |
| `lag1` | 0.0323 | **0.686** |

게이트 기준 0.50을 넘어 `delay_pass = true`. 다만 **하루 늦으면 31%가 사라진다.**
검증한 A등급 피처 중 지연 손실이 큰 편이다. 장 마감 직후 계산해 다음 날 시초에 실행하는
정도는 되지만, 하루 이상 밀리면 곤란하다.

### 5.2 거래 가능한 종목만 남겨도 남는가 (tradable)

| universe | IC | 유지율 |
|---|---:|---:|
| `broad` | 0.0470 | — |
| `tradable` | 0.0449 | **0.954** |

`tradable`은 20일 평균 거래대금 1억원 이상, 종가 1,000원 이상이다. 유지율 0.954로
**거의 손실이 없다.** 이 신호는 동전주·거래 없는 종목에 몰려 있지 않다.

`px_amihud_20d`의 유지율 0.85와 대조된다. 반전은 실행 가능성 쪽 문제가 작은 편이다.

### 5.3 생존편향 (survivorship)

| sample_kind | IC |
|---|---:|
| `common_survivor` | 0.0470 |
| `available` | 0.0460 |

차이가 2% 수준이고 부호도 같다. `attrition_warning = false`. 상장폐지 종목을 포함해도
결과가 바뀌지 않는다.

### 5.4 기간 일관성

- `valid_subperiods` = 5
- `sign_consistent_subperiods` = **5**

2014~2016, 2017~2019, 2020~2021, 2022~2023.10, 2023.11~ 다섯 구간 **전부에서 같은
방향**이었다. 특정 국면에만 나타난 신호가 아니다.

다만 **구간별 크기는 이 문서로 알 수 없다.** 부호 일치 여부만 저장돼 있다. 크기 변화는
Phase A run의 `plots/px_reversal_5d_subperiod_heatmap.png`에서 봐야 한다.

### 5.5 창 중첩 보정 (non-overlap)

`offset_status = complete`. 등록된 offset이 1개이고 유효 판정을 받았다. 부호 일치율 1.0.

짧은 horizon이라 offset 격자가 좁다. 긴 horizon 피처만큼 이 검사가 엄하지 않다는 점은
감안해야 한다.

### 5.6 시장 구성

- KOSPI 비중 **41.2%** / KOSDAQ 비중 **58.8%**

코스닥 쪽이 조금 더 무겁다. 개인 비중이 높은 시장에서 반전이 강하다는 가설과 방향이 맞지만,
KOSDAQ 종목 수가 원래 더 많다는 점도 함께 작용한다. 이 비중만으로 시장별 차이를 결론지을
수 없다.

### 5.7 시간 placebo

`p_temporal_nw`, `temporal_null_pass` 모두 `null`이다. temporal placebo는 긴 horizon
cell(NW lag 59 이상)에만 적용하므로 이 family는 대상이 아니다
(`placebo.temporal_min_nw_lag: 59`). **검사에서 떨어진 게 아니라 대상이 아니다.**

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | 2,622일 |
| 날짜당 평균 종목 수 | 1,098개 |
| 전체 관측 | 약 288만 행 (`cum 0→1` 기준) |

- 35개 family 중 표본이 가장 긴 축이다. 원천이 `daily_ohlcv` 하나뿐이라 결측이 거의 없다.
- 종료일이 2025-02-05인 것은 `common_survivor` 표본이 120일 라벨을 요구하기 때문이다.
  holdout 시작(2025-08-01)과는 다른 이유다.
- `available` 표본으로 넓히면 2,741일까지 늘어난다.
- Phase A는 `coverage_ratio`를 계산하지 않는다. 커버리지 비율은 Phase B 전용 지표다.

---

## 7. 중복성

A×B 교차 상관에서 이 family와 가장 크게 겹치는 상대는 다음과 같다 (일별 횡단 순위상관의
기간 평균).

| 상대 family | 평균 순위상관 | 유효일 |
|---|---:|---:|
| `mcap_krx_log` | −0.033 | 2,622 |
| `ev_payout_yield` | −0.027 | 2,175 |
| `fin_log_mcap` | −0.019 | 2,392 |
| `fin_gross_profitability` | −0.018 | 1,927 |

**전부 0.05 미만이다.** Phase B의 재무·공시 계열과 사실상 독립이다. 묶어 써도 정보가
겹치지 않는다.

### 확인하지 않은 중복

**같은 Phase A 안의 가격 피처끼리는 비교하지 않았다.** 상관 산출물이 A×B 교차만 담고 있기
때문이다 (`13_..._plan.md` §7.2 차트 5).

반전은 단기 가격 변화이므로 `px_maxret_20d`, `px_idio_vol_60d`, `px_turnover_shock`와
겹칠 여지가 있다. 특히 급락 종목은 변동성도 크다. **A×A 상관은 아직 답이 없는 질문이다.**

---

## 8. 한계와 확인 못 한 것

1. **거래비용을 반영한 성과가 없다.** §4.2의 0.38%p는 비용 차감 전이다. 5일 회전 신호에서
   이건 결정적인 공백이다.
2. **어떤 종목이 언제 좋았는지 모른다.** 일별 IC 시계열도, 상위 분위 종목 목록도 저장하지
   않는다. 지금 답할 수 있는 건 "다섯 구간 전부에서 부호가 같았다"까지다
   ([00_읽는_법.md](00_읽는_법.md) §7).
3. **상호작용을 안 봤다.** 사전 설계가 요구한 회전율·Amihud·MAX와의 상호작용은 단변량
   scan 범위 밖이다.
4. **같은 가격 계열과의 중복이 미확인**이다 (§7).
5. **업종 중립화가 없다.** 특정 업종이 동반 급락한 날의 신호가 업종 효과인지 개별 종목
   효과인지 구분하지 못한다.
6. **holdout을 열지 않았다.** 2025-08-01 이후 구간은 그대로 남아 있다.

---

## 9. 모델에서는 어땠나 — T1

이 피처는 T1 후보 5개 중 하나로 들어갔다
(`07_phase1_acceptance_gate.md` §1: `px_reversal_5d`, `px_maxret_20d`, `px_idio_vol_60d`,
`flow_individual_netbuy_to_volume_{5,20}d`). 기존 40개 raw 피처에 5개를 더한 45개 구성이다.

### walk-forward (2026-08-24)

| horizon | baseline Rank IC | candidate Rank IC | baseline 비용반영 spread | candidate 비용반영 spread |
|---|---:|---:|---:|---:|
| 5 | 0.1155 | **0.1202** | −0.00018 | **+0.00075** |
| 20 | 0.1436 | **0.1521** | +0.01258 | **+0.01009** |
| 60 | 0.1753 | **0.1840** | +0.02035 | **+0.02073** |

Rank IC는 세 horizon 전부 개선됐다. 증분성은 확실하다.

### k=100 비용 확인 (2026-08-12)

| horizon | baseline 비용반영 수익 | candidate 비용반영 수익 | Δ |
|---|---:|---:|---:|
| 20 | +0.01999 | +0.01545 | **−0.00454** |

**사전에 정한 조건은 `k=100 비용 반영 수익 Δ(h20) > 0`이었다.** −0.0045로 조건을 통과하지
못했다. 그래서 묶음 전체가 **비채택**이다.

주의할 점 셋.

- 이건 **5개 묶음의 결과**다. `px_reversal_5d` 개별 기여도는 따로 측정하지 않았다.
- 두 JSON은 실행 시점과 lineage가 다르다. 숫자를 한 계열로 이어 붙이면 안 된다.
- **screening 통과와 모델 채택은 다른 판정이다.** 등급 A는 "단변량 근거가 좋다"는 뜻이지
  "모델에 넣으면 이득이다"가 아니다. 이 family가 그 차이를 보여 주는 대표 사례다.

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
A=f"research/output/horizon_scan/phase=A/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260827T221729-4e0ae8b0"
print(duckdb.sql(f"""
  select scan_type, h_start, h_end, universe, sample_kind,
         ic_mean, icir, t_nw, q5_spread_aligned, q_fdr_global, n_dates, n_obs_mean
  from '{A}/core/horizon_ic.parquet'
  where family='px_reversal_5d' and hypothesis_role='primary'
  order by universe, sample_kind, scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| cell 전체 | `phase=A/…/run_id=20260827T221729-4e0ae8b0/core/horizon_ic.parquet` |
| 신호 모양·강건성 요약 | 같은 run의 `cards/family_cards.json` |
| 차트 7종 | 같은 run의 `plots/px_reversal_5d_*.png` |
| 결합 BH 결과 | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| A×B 상관 | 같은 AB run의 `primary_feature_rank_correlation.parquet` |
| T1 walk-forward | `docs/target/01_20_access_return_rank/grade_a_acceptance_gate_results.json` |
| T1 k=100 | `docs/target/01_20_access_return_rank/topk_cost_check.json` |
| 서술 대조 | `01_feature_candidate/09_all_feature_results.md` §4 |
