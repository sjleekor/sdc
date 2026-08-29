# 19. `ev_filing_activity` — 공시 건수 급증 (filing burst)

- 작성일: 2026-08-29
- family: `ev_filing_activity` · primary feature: **`ev_filing_burst_60d`** · domain: event
- **Phase B** · fdr_family `event` · **기대 부호 없음(양방향)** · 관측 부호 `−`
- 등급 **A** · discovery 4/4 · screen-pass 4/4 · 실패한 게이트 없음
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**최근 60일 공시 건수가 평소보다 많이 늘어난 회사가 이후 부진했다**
(cum 0→60 IC −0.0142, 5분위 수익률 차이 −0.58%p).

**Phase B 18개 중 [12_flow_individual_netbuy_to_volume.md](12_flow_individual_netbuy_to_volume.md)처럼
방향을 열어 둔 family다.** 공시가 늘면 호재일 수도 악재일 수도 있어 `expected_sign: null`로
등록했다. 결과는 `−`로 나왔다.

4개 cell 전부 discovery이면서 screen-pass이고 등급 A다. 기간 5/5, 거래가능 유지율
**1.09~1.20**(유동성 좋은 종목에서 오히려 강함).

**다만 크기가 작다.** |IC| 0.0142, 5분위 차이 0.58%p는 같은 마트에서 나온
[18_ev_amendment_ratio.md](18_ev_amendment_ratio.md)(0.0568 / 2.65%p)의 4분의 1이다.
시간 placebo도 **p = 0.0693으로 기준 0.10에 가깝다.**

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의 — 두 단계

**1단계 — 60거래일 공시 건수**

```sql
-- research/etl/features/filing_activity.py:118
SUM(filings) OVER (
    PARTITION BY ticker, market ORDER BY trade_date
    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
) AS ev_filing_count_60d
```

**2단계 — 그 회사의 평소 수준으로 나눈다**

```sql
-- research/etl/features/filing_activity.py:136
CASE WHEN session_ordinal >= 250 THEN
    ev_filing_count_60d / NULLIF(
        quantile_cont(ev_filing_count_60d, 0.5) OVER (
            PARTITION BY ticker, market ORDER BY trade_date
            ROWS BETWEEN 249 PRECEDING AND CURRENT ROW
        ), 0)
END AS ev_filing_burst_60d
```

**최근 60일 공시 건수를 직전 250거래일 동안의 그 값 중앙값으로 나눈 값**이다.

- 1이면 평소와 같은 속도로 공시를 내고 있다
- 2면 평소의 두 배다
- 0.5면 절반이다

### 2.2 왜 건수가 아니라 비율인가

건수 자체를 쓰면 **큰 회사가 항상 큰 값**을 갖는다. 사업이 많으면 공시도 많다. 규모 축과
뒤섞인다 — [07_px_amihud_20d.md](07_px_amihud_20d.md) §7이 보여 준 문제와 같다.

분모를 **그 회사 자신의 250일 중앙값**으로 잡으면 규모가 상쇄된다. 남는 건 "이 회사가
평소보다 바쁜가"다.

[08_px_turnover_shock.md](08_px_turnover_shock.md) §2.2와 정확히 같은 설계다. 그쪽은
거래대금을 자기 과거 중앙값으로 나눴고, 이쪽은 공시 건수를 나눴다. **평균이 아니라
중앙값을 쓰는 이유도 같다** — 과거의 한 번의 폭증이 분모를 오래 눌러 두지 않게 하려는 것이다.

### 2.3 250일 baseline이 표본 시작을 늦춘다

```python
# research/etl/features/filing_activity.py:87
BURST_BASELINE = 250
```

주석이 근거를 적었다.

> Sessions of history the burst ratio's baseline needs. Below this the median
> is being taken over a partial window and the ratio is not comparable.

`session_ordinal >= 250` 조건 때문에 **패널 시작 후 250거래일이 지나야 값이 나온다.**

그 결과 유효 시작이 **2015-07-06**이다. 같은 마트에서 나온 `ev_amendment_ratio`(2015-01-05)
보다 6개월 늦다. 연도별 커버리지에서도 2015년이 **0.376**으로 크게 낮다(§6.2).

### 2.4 창 두 개를 미리 등록했다

```python
# research/etl/features/filing_activity.py:78
#: Trailing session counts. Both are pre-registered (N5-6): 60 as the primary
#: window, 120 as the declared variant, so choosing between them after seeing
#: results is not available.
WINDOWS = (60, 120)
```

**결과를 보고 60일과 120일 중 좋은 쪽을 고르는 걸 막으려고 미리 역할을 정해 뒀다.**
60일이 primary, 120일이 선언된 변형이다.

사전등록에 secondary 셋이 올라 있다.

```yaml
features:
  - {column: ev_filing_burst_60d,  role: primary}
  - {column: ev_filing_count_60d,  role: secondary}
  - {column: ev_filing_count_120d, role: secondary}
  - {column: ev_filing_burst_120d, role: secondary}
```

**이번 run에는 primary 4개 cell만 있다. secondary 셋은 안 돌렸다.**
비율(burst)과 건수(count) 중 어느 쪽이 신호인지, 60일과 120일 중 어느 창이 나은지
확인하지 않았다.

### 2.5 PIT와 0 채우기

[18_ev_amendment_ratio.md](18_ev_amendment_ratio.md) §2.4·§2.5와 같다.

- 접수일 D의 공시는 **D+1 거래일부터** 노출한다
- 공시가 없는 날을 0으로 채워 `ROWS` 창이 거래일을 세게 한다

정본 변형은 **`native_t`**다. PIT가 산식 안에 있어 추가 지연이 필요 없다.

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 건수 산식 | `research/etl/features/filing_activity.py:118` |
| burst 산식 | `research/etl/features/filing_activity.py:136` |
| 창 상수 | `research/etl/features/filing_activity.py:78`, `:87` |
| 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |

---

## 3. 왜 방향을 열어 뒀나

### 3.1 두 갈래가 반대를 가리킨다

```yaml
- family: ev_filing_activity
  expected_sign: null       # ← 방향을 걸지 않았다
```

| 가설 | 메커니즘 | 예측 부호 |
|---|---|---|
| **활동성** | 사업이 활발해 공시할 일이 많다 | `+` |
| **주목** | 공시가 늘면 투자자 관심이 몰린다 | `+` (단기) |
| **문제 신호** | 유상증자·소송·경영권 분쟁이 몰리면 공시가 는다 | `−` |
| **불확실성** | 공시가 잦으면 상황이 유동적이라는 뜻 | `−` |

**어느 쪽이 우세한지 사전에 정할 근거가 없었다.** [12](12_flow_individual_netbuy_to_volume.md)
§3.2와 같은 상황이다.

### 3.2 양방향 판정 규칙

`expected_sign`이 없으면 두 가지가 달라진다
([12](12_flow_individual_netbuy_to_volume.md) §3.3과 동일).

- **방향 게이트를 적용하지 않는다.** 산출물에서 `expected_sign_pass`가 `<NA>`다.
- **기간 일관성은 관측 부호를 기준으로 잰다.** `sign_consistent_subperiods = 5`는
  "관측된 `−` 방향과 다섯 구간이 전부 일치했다"는 뜻이다.
- **`q5_spread_aligned`가 원값과 같다.** 곱할 부호가 없으므로 `+1`이 적용된다.

양측 검정(`p_nw`)을 쓰고 나머지 게이트는 그대로이므로 규율이 느슨해지지 않는다.

### 3.3 사전등록 horizon

```yaml
primary_horizon_set: [20, 60]
exploratory_horizon_set: [1, 5, 10, 40, 120]
include_bucket_primary: true
```

**Phase B 확장분 중 이 family와 지분 활동 계열만 [20, 60]이다.** 나머지는 [60, 120]이다.

이유는 §2.1의 창 길이다. 60일 창의 급증 지표라 반응이 빠를 것으로 봤다. 다만 250일
baseline 때문에 완전히 빠른 지표는 아니다.

cell은 누적 2개(0→20, 0→60) + 구간 2개(10→20, 40→60) = **4개**다.

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 20~60일 | **4개 cell 전부 discovery** |
| 부호 | 없음 | **`−`** |

### 3.4 사전등록 시점

2026-08-27 확장 등록분이다 (`outcome_blind: true`).

분류 좌표는 C4(이벤트·공시) × **T2(놀라움)** × U다. 평소 대비 변화를 재므로 `fin_sue`,
`px_turnover_shock`와 같은 T2다.

### 3.5 근거 문헌

없다. 접수 이력에서 만든 신규 지표다.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

양방향 family이므로 `q5_spread_aligned`가 원값과 같다. **음수면 상위 20%가 하위 20%보다
덜 올랐다는 뜻이다.**

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 | AB q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | −0.0089 | −0.239 | −3.13 | −0.28%p | 0.0029 | **discovery + screen-pass** |
| cum | 0→60 | **−0.0142** | −0.373 | −3.23 | **−0.58%p** | 0.0021 | **discovery + screen-pass** |
| bucket | 10→20 | −0.0054 | −0.157 | −2.91 | −0.12%p | 0.0057 | **discovery + screen-pass** |
| bucket | 40→60 | −0.0067 | −0.222 | −3.16 | −0.16%p | 0.0026 | **discovery + screen-pass** |

**4개 전부 통과했고 `failed_gates`가 비어 있다.**

### 4.2 크기가 작다

|IC| 0.0142는 Phase B 중 작은 축이다. 같은 마트에서 나온 형제와 비교하면 확연하다.

| family | 대표 \|IC\| | 5분위 차이 | ICIR |
|---|---:|---:|---:|
| `ev_amendment_ratio` | 0.0568 (120일) | +2.65%p | −1.357 |
| **`ev_filing_activity`** | **0.0142 (60일)** | **−0.58%p** | **−0.373** |

**공시를 몇 건 냈나보다 그중 몇 건을 고쳤나가 훨씬 강한 신호다.** 건수는 사업 규모·업종에
따라 자연스럽게 다르지만, 정정 비율은 품질을 직접 가리킨다는 §3.1의 「문제 신호」 해석과
맞는 결과다.

t값이 −2.9~−3.2로 유의한 것은 표본이 2,354일로 길기 때문이다. **유의성과 크기를 분리해서
봐야 한다.**

### 4.3 IC와 5분위 차이가 같은 방향이다

네 cell 전부 IC 음수·spread 음수다. 관계가 단조롭다.

### 4.4 신호의 모양

| 관찰 | 값 |
|---|---|
| `peak_cell` | `cum 0→60` |
| `peak_ic_mean` | −0.0142 |
| 누적 \|IC\| 추이 | 20일 0.009 → 60일 0.014 (증가) |
| 구간 \|IC\| 추이 | 10~20일 0.005 → 40~60일 0.007 (증가) |

**관측 범위 끝에서 최대다.** 120일은 exploratory로 내려 확인하지 않았다.

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 4개 cell 전부 5/5

- `valid_subperiods` = 5, `sign_consistent_subperiods` = **5**, `period_sign_pass` = True
  (4개 cell 모두)

양방향 family이므로 **관측 부호(`−`) 기준**이다 (§3.2).

### 5.2 시간 placebo — 통과하지만 문턱에 가깝다

| cell | `p_temporal_nw` | 판정 |
|---|---:|---|
| cum 0→60 | **0.0693** | **통과** (기준 0.10) |
| 나머지 셋 | — | 대상 아님 (NW lag < 59) |

**0.0693은 통과이지만 여유가 크지 않다.** [18](18_ev_amendment_ratio.md)의 0.0099~0.0198,
`px_maxret_20d`·`px_idio_vol_60d`의 0.0099와 비교하면 확연히 높다.

100번의 시간 이동 placebo 중 **여섯 번**이 관측값만큼 극단적이었다는 뜻이다.

### 5.3 비중첩 offset

| cell | `robustness_required` | `offset_status` | `nonoverlap_robustness_pass` |
|---|---|---|---|
| cum 0→60 | True | **complete** | **True** |
| cum 0→20 | False | — | 대상 아님 |
| bucket 10→20 | False | — | 대상 아님 |
| bucket 40→60 | False | — | 대상 아님 |

**강건성 검사를 받은 cell이 하나뿐이다.** 나머지 셋은 horizon이 짧아
`robustness_required = false`다.

### 5.4 거래 가능한 종목만 남기면 — 오히려 강해진다

| cell | `tradable_retention` | `tradable_pass` |
|---|---:|---|
| cum 0→20 | 1.095 | True |
| cum 0→60 | **1.147** | True |
| bucket 10→20 | 1.123 | True |
| bucket 40→60 | **1.202** | True |

**네 cell 전부 1을 넘는다.** 유동성 좋은 종목에서 20% 더 강하다.

**35개 중에서도 높은 축이다.** 소형주 착시가 아니고, 오히려 거래가 활발한 종목에서
"공시 급증 뒤 부진"이 뚜렷하다.

### 5.5 생존편향

`available_direction_pass` = **True** (4개 cell 모두).

### 5.6 source quality — 대상 아님

`source_quality_status` = `not_applicable`. 접수 이력은 사후 수정되지 않는 사실 기록이다.

### 5.7 등급 A

`evidence_grade` = **A** (4개 cell 모두), `failed_gates` = `[]`.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2015-07-06 ~ 2025-02-05** |
| 유효 거래일 | **2,354일** |
| 날짜당 평균 종목 수 | 998~999개 |
| `coverage_ratio` | **0.821** |
| 관측 행 수 | 5,647,906 |

### 6.1 시장별 커버리지

| 시장 | 커버리지 | 값이 있는 종목 | 날짜당 중앙값 |
|---|---:|---:|---:|
| KOSDAQ | 0.858 | 1,694 | 1,081 |
| KOSPI | **0.766** | 826 | 776 |

[18](18_ev_amendment_ratio.md)과 같이 **KOSPI가 9%p 낮다.** 원인을 분석하지 않았다.

### 6.2 연도별 커버리지 — 2015년이 낮다

| 연도 | 커버리지 |
|---|---:|
| 2015 | **0.376** |
| 2016 | 0.874 |
| 2017~2026 | 0.89 ~ 0.92 |

§2.3의 250일 baseline 요구 때문이다. 2016년부터 안정된다.

`ev_amendment_ratio`(0.894)보다 전체 커버리지가 7%p 낮은 것도 같은 이유다. 250일 창을
**두 번**(60일 건수 + 250일 중앙값) 거치기 때문이다.

---

## 7. 중복성

### A×B 교차 상관

| 상대 family | 평균 순위상관 | 유효일 |
|---|---:|---:|
| `px_idio_vol_60d` | +0.071 | 2,334 |
| `px_resid_mom_12_1` | +0.061 | 2,207 |
| `px_mom_12_1` | +0.054 | 2,354 |

**전부 0.08 미만이다.** Phase A 계열과 거의 독립이다.

`ev_amendment_ratio`가 `px_idio_vol_60d`와 +0.140이었던 것보다 낮다. **건수 급증은 정정
비율보다 가격 계열과 덜 얽힌다.**

### 확인하지 않은 중복 — 같은 마트의 형제들

`feat_filing_activity` 하나가 다섯 family를 만든다
([18](18_ev_amendment_ratio.md) §7 참조).

**특히 [32_own_insider_filing_activity.md](32_own_insider_filing_activity.md)와 구조가
동일하다.** 둘 다 `_burst_60d` 형태이고 산식이 같은 CASE 문에서 나온다
(`filing_activity.py:130`의 `burst_columns` 루프가 `ev_filing`과 `own_insider_filing`
둘을 같이 만든다).

차이는 **분자에 세는 공시 종류**뿐이다.

| family | 세는 공시 |
|---|---|
| **`ev_filing_activity`** | **전체 공시** |
| `own_insider_filing_activity` | 임원·주요주주 소유상황보고서만 |

**전체가 부분을 포함한다.** 부분집합 관계인데 별도 family로 세었고,
**B×B 상관 산출물이 없다.**

`ev_filing_count_60d`·`ev_filing_count_120d`·`ev_filing_burst_120d`(secondary 셋)와의
관계도 안 봤다 (§2.4).

---

## 8. 한계와 확인 못 한 것

1. **크기가 작다** (§4.2). |IC| 0.0142, 5분위 차이 0.58%p.
2. **시간 placebo가 문턱에 가깝다** (§5.2). p = 0.0693.
3. **강건성 검사를 받은 cell이 하나뿐이다** (§5.3).
4. **같은 마트 다섯 family 간 중복이 미확인이다** (§7). 특히
   `own_insider_filing_activity`와 부분집합 관계다.
5. **secondary 셋을 안 돌렸다** (§2.4). 건수와 비율 중, 60일과 120일 중 어느 쪽이 나은지
   모른다.
6. **공시의 종류를 구분하지 않는다.** 유상증자 공시와 단순 정기보고서를 같은 1건으로
   센다. `ev_material_event_flag`는 의도적으로 만들지 않았다 — 중요도 목록을 정하는
   판단이 아직 없기 때문이다 (`filing_activity.py` docstring).
7. **KOSPI 커버리지가 9%p 낮다** (§6.1).
8. **60일 너머를 안 봤다** (§4.4).
9. **업종 중립화가 없다.** 업종마다 공시 빈도가 다르다.
10. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
11. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T2

**T2 14-feature bundle에 들어갔다** (`ev_filing_burst_60d`).

| horizon | Rank IC Δ | 비용 반영 spread Δ |
|---|---:|---:|
| 5 | +0.0031 | +0.0017 |
| 20 | +0.0011 | +0.0030 |
| 60 | +0.0003 | +0.0080 |

세 horizon 전부 개선됐다(`improved_all_horizons`). **다만 14개를 함께 넣은 결과라 이
피처의 개별 기여도는 측정하지 않았다.**

§4.2에서 본 대로 단변량 크기가 작은 축이라 묶음 안에서의 비중도 작았을 수 있다. 확인할
방법이 없다.

**최종 h60 holdout은 아직 열지 않았다.**

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# 양방향 family 는 expected_sign_pass 가 NULL 이다
print(duckdb.sql(f"""
  select family, feature, scan_type, h_start, h_end, ic_mean, q5_spread_aligned,
         expected_sign, expected_sign_pass, q_fdr_global_ab, screen_pass,
         evidence_grade, tradable_retention, p_temporal_nw
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  where family in ('ev_filing_activity','ev_amendment_ratio')
  order by family, scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 커버리지 | 같은 B run의 `core/feature_coverage.parquet` |
| burst 산식 | `research/etl/features/filing_activity.py:136` |
| 창 상수와 사전등록 의도 | `research/etl/features/filing_activity.py:78` |
| 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |
| T2 결과 | `docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json` |
