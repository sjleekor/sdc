# 29. `hc_employee_growth` — 직원 수 증가율

- 작성일: 2026-08-29
- family: `hc_employee_growth` · primary feature: **`hc_employee_growth_yoy`** · domain: human_capital
- **Phase B** · fdr_family `human_capital` · **기대 부호 없음(양방향)** · 관측 부호 `+`
- **discovery 1/4 · screen-pass 1/4** · 등급 **B 1개 / C 3개** · source quality `warn` · **등급 상한 B**
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**직원 수를 늘린 회사가 이후 조금 더 올랐다** (cum 0→120 IC +0.0157). **35개 중 유일하게
비재무·비가격 정보를 쓰는 축이다** — 사업보고서의 직원 현황이 원천이다.

**그런데 근거가 약하다.**

| 항목 | 값 |
|---|---|
| 최소 BH q | **0.093** — 기준 0.10을 겨우 통과 |
| discovery | 4개 중 **1개**(`bucket 40→60`) |
| t(NW) | 1.16 ~ 1.81 — **어느 cell도 2를 넘지 않는다** |
| 시간 placebo | **전부 실패** (p = 0.554 ~ 0.713) |
| **등급 상한** | **B** — `final_vintage` 경고로 A를 받을 수 없다 |
| vintage 나이 | 평균 **182일**, 95분위 **484일** |

**등급 상한이 사전에 걸려 있는 유일한 계열이다.** 원천이 최종본만 주기 때문이다(§5.6).

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/periodic_extras.py:99
CASE
    WHEN e.previous_year = e.bsns_year - 1
     AND e.headcount >= 1 AND e.previous_headcount >= 1
     AND NOT (
         s.corp_code IS NOT NULL
         AND abs(e.headcount / nullif(e.previous_headcount, 0) - 1) >= 0.30
     )
    THEN e.headcount / nullif(e.previous_headcount, 0) - 1
END AS hc_employee_growth_yoy
```

**직원 수의 전년 대비 증가율**이다.

- +0.10이면 1년 동안 직원이 10% 늘었다
- 사업보고서 「직원 현황」의 인원 수를 쓴다

### 2.2 구조 변경이 있는 해에 30% 이상 변하면 버린다

**가장 눈에 띄는 조건이다.**

```sql
AND NOT (s.corp_code IS NOT NULL
         AND abs(headcount / previous_headcount - 1) >= 0.30)
```

`structural_change_year`에 걸린 회사(합병·분할 등)가 **그 해에 30% 이상 인원이 변하면 값을
버린다.**

**직원이 늘어난 게 채용 때문인지 합병 때문인지 구분하려는 장치다.** 합병으로 직원 1,000명이
붙은 걸 "고용을 늘렸다"고 읽으면 안 된다.

두 조건이 **AND**로 묶여 있다는 점이 중요하다. 구조 변경이 없으면 30%가 넘어도 버리지
않고, 구조 변경이 있어도 30% 미만이면 버리지 않는다.

### 2.3 연 1회 값이고 늦게 온다

```sql
-- research/etl/features/periodic_extras.py:98
greatest(e.employee_available_from, e.previous_available_from) AS available_from
```

**올해 값과 작년 값 중 늦게 공개된 쪽을 기준으로 쓴다.** 둘 다 있어야 증가율이 나오므로
당연한 처리인데, 결과적으로 노출이 더 늦어진다.

| 시장 | 평균 나이 | 95분위 나이 |
|---|---:|---:|
| KOSDAQ | **181.3일** | **470일** |
| KOSPI | **182.5일** | **484일** |

**평균 182일 된 정보다.** 분기 재무 계열(74일)의 2.5배이고, 20번에 한 번은 **1년 3개월**
묵은 값을 쓴다.

연 1회 공시라는 구조적 한계다. 사업보고서는 결산 후 3개월 안에 나오고, 증가율을 만들려면
그 전해 보고서까지 있어야 한다.

### 2.4 원천은 DS002이고 최종본만 준다

```python
# research/etl/features/periodic_extras.py 모듈 docstring
"""Daily PIT features from OpenDART periodic employee/governance responses (N6).

The DS002 endpoints expose **final-vintage values**. Every feature therefore
starts only after the retained receipt becomes public; annual values are never
backdated to the business-year end."""
```

**DS002(정기보고서 주요정보)는 최종 확정본만 준다.** 처음 공시된 값과 나중에 정정된 값을
구분해서 주지 않는다.

그래서 상수가 하나 박혀 있다.

```python
FINAL_VINTAGE_CAPTURE_RATIO = 0.0184
```

**PIT 원칙 자체는 지켰다** — 해당 접수가 공개된 뒤에야 값을 쓴다. 다만 **그 값이 나중에
고쳐졌다면 고쳐진 값을 쓰게 된다.** 이게 §5.6의 등급 상한 근거다.

원천 수집은 `dart sync-periodic-extras` 명령이 담당한다 (`CLAUDE.md`).

### 2.5 정본 변형은 `native_t`

PIT 처리가 산식 안에 있어 추가 지연이 필요 없다. `formula_version: periodic_extras_v2`.

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/periodic_extras.py:99` |
| 최종본 한계 | 같은 파일 모듈 docstring, `FINAL_VINTAGE_CAPTURE_RATIO` |
| 원천 수집 | `krx-collector dart sync-periodic-extras` |
| 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |

---

## 3. 왜 방향을 열어 뒀나

### 3.1 두 갈래가 반대를 가리킨다

```yaml
- family: hc_employee_growth
  expected_sign: null       # ← 방향을 걸지 않았다
```

| 가설 | 메커니즘 | 예측 부호 |
|---|---|---|
| **성장 신호** | 사람을 뽑는다 = 사업이 잘 된다 | `+` |
| **인적자본 투자** | 무형자산 축적이 미래 수익으로 | `+` |
| **비용 증가** | 인건비가 늘어 이익률이 나빠진다 | `−` |
| **과잉 확장** | 자산성장 이례현상과 같은 논리 | `−` |

**네 번째가 특히 걸린다.** [24_fin_asset_growth_yoy.md](24_fin_asset_growth_yoy.md)의
투자 이례현상이 맞다면 **인력 확장도 같은 방향**이어야 한다. 그래서 사전에 정할 수 없었다.

양방향 판정 규칙은 [12_flow_individual_netbuy_to_volume.md](12_flow_individual_netbuy_to_volume.md)
§3.3과 같다 — 방향 게이트를 적용하지 않고(`expected_sign_pass`가 `<NA>`), 기간 일관성은
관측 부호 기준으로 잰다.

### 3.2 왜 이 축을 넣었나 — 빈 칸을 메우려는 시도다

`11_feature_taxonomy.md` §1이 현재 구조의 빈 칸을 지목했고, C7(시장구조·비재무)의
**인적자본**이 그중 하나였다.

같은 문서 §2.4가 배경을 준다. Han, Lee & Kang (2020)의 6개 카테고리 중 **intangible
assets가 0개**였다.

**이 family가 그 방향의 첫 시도다.** 새 수집 없이 이미 받아 둔 DS002 응답을 썼다.

### 3.3 사전등록 horizon

```yaml
primary_horizon_set: [60, 120]
exploratory_horizon_set: [20, 40]
include_bucket_primary: true
source_quality: {status: warn, warning: final_vintage, grade_cap: B}
```

연 1회 갱신되는 가장 느린 지표이므로 긴 horizon에 걸었다. cell은 4개다.

**`source_quality` 블록이 사전등록에 직접 들어 있다.** 결과를 보기 전에 등급 상한을
못 박았다는 뜻이다 (§5.6).

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 60~120일 | **discovery 1개** |
| 부호 | 없음 | **`+`** |

### 3.4 사전등록 시점

2026-08-27 확장 등록분이다 (`outcome_blind: true`).

분류 좌표는 **C7(시장구조·비재무 — 인적자본)** × T1(변화) × U다.
`11_feature_taxonomy.md` 기준으로 **비어 있던 칸을 처음 채운 항목**이다.

### 3.5 근거 문헌

없다. 신규 축이다. Eisfeldt & Papanikolaou (2013)의 organization capital 계열이 가장
가깝지만 직접 근거로 등록되지는 않았다.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

양방향 family이므로 `q5_spread_aligned`가 원값과 같다.

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 | AB q | 등급 | 판정 |
|---|---|---:|---:|---:|---:|---:|---|---|
| cum | 0→60 | +0.0154 | 0.207 | 1.52 | +0.17%p | 0.162 | C | BH 실패 |
| cum | 0→120 | **+0.0157** | 0.221 | 1.27 | +0.48%p | 0.248 | C | BH 실패 |
| bucket | 40→60 | +0.0084 | 0.149 | **1.81** | −0.01%p | **0.093** | **B** | **discovery + screen-pass** |
| bucket | 60→120 | +0.0079 | 0.151 | 1.16 | +0.02%p | 0.297 | C | BH 실패 |

### 4.2 통과한 하나가 아슬아슬하다

**`bucket 40→60`의 q가 0.093이다.** BH 기준이 0.10이므로 **0.007 차이로 통과했다.**

그리고 그 cell이 **|IC|가 가장 작다**(0.0084). 네 cell 중 t값이 가장 큰 것(1.81)이 통과
사유인데, 그것도 2를 넘지 않는다.

**t가 큰 이유는 IC가 커서가 아니라 중첩 보정이 작아서다.** bucket 40→60은 폭이 20일이라
NW lag가 19다. 누적 0→120은 lag가 119라 같은 IC라도 t가 훨씬 작아진다.

**"짧은 구간이 통과했다"를 "짧은 구간에서 신호가 강하다"로 읽으면 안 된다.**

### 4.3 5분위 차이가 사실상 0이다

통과한 `bucket 40→60`의 5분위 차이가 **−0.006%p**다. 부호까지 IC와 반대다.

| cell | Rank IC | 5분위 차이 |
|---|---:|---:|
| cum 0→120 | +0.0157 | +0.48%p |
| **bucket 40→60** | **+0.0084** | **−0.01%p** |
| bucket 60→120 | +0.0079 | +0.02%p |

**discovery로 뽑힌 cell의 경제적 크기가 0이다.** 통계적 통과와 경제적 의미가 갈리는
사례다 ([01_px_reversal_5d.md](01_px_reversal_5d.md) §4.2의 `bucket 5→10`과 같은 구조).

### 4.4 신호의 모양

| 관찰 | 값 |
|---|---|
| `peak_cell` | `cum 0→120` |
| `peak_ic_mean` | +0.0157 |
| 누적 IC 추이 | 60일 0.0154 → 120일 0.0157 (거의 평평) |
| 구간 IC 추이 | 40~60일 0.0084 → 60~120일 0.0079 (감소) |

**누적이 거의 평평하다.** 60일 이후 새로 더해지는 신호가 거의 없다는 뜻이다.

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 4구간 중 3~4구간

| cell | `valid_subperiods` | `sign_consistent_subperiods` | `period_sign_pass` |
|---|---:|---:|---|
| cum 0→60 | 4 | **4** | True |
| bucket 40→60 | 4 | **4** | True |
| cum 0→120 | 4 | 3 | True |
| bucket 60→120 | 4 | 3 | True |

**전부 통과했다.** 양방향 family이므로 관측 부호(`+`) 기준이다.

표본이 2016-06-27부터라 구간이 **4개**다.

### 5.2 시간 placebo — 전부 실패

| cell | `p_temporal_nw` | `temporal_null_pass` |
|---|---:|---|
| cum 0→60 | **0.5545** | **False** |
| cum 0→120 | **0.5743** | **False** |
| bucket 60→120 | **0.7129** | **False** |
| bucket 40→60 | — | 대상 아님 (NW lag 19) |

기준은 0.10이다. **55~71% 확률로 시간 이동 placebo가 관측값만큼 극단적인 결과를 만들었다.**
사실상 무작위와 구분되지 않는다.

**discovery로 뽑힌 `bucket 40→60`은 이 검사를 받지 않았다.** 검사를 받은 세 cell은 전부
떨어졌다.

### 5.3 비중첩 offset — `complete` 통과

세 긴 cell 모두 `offset_status = complete`, `nonoverlap_robustness_pass = True`다.
떨어진 건 시간 placebo 하나다.

### 5.4 거래 가능한 종목만 남겨도

| cell | `tradable_retention` |
|---|---:|
| cum 0→60 | 0.925 |
| cum 0→120 | 1.062 |
| bucket 40→60 | 1.035 |
| bucket 60→120 | 1.078 |

**0.93 ~ 1.08로 안정적이다.** 유동성 필터가 신호를 크게 바꾸지 않는다.

### 5.5 생존편향

`available_direction_pass` = **True** (4개 cell 모두).

### 5.6 source quality — 등급 상한 B가 사전에 걸려 있다

**35개 중 `source_quality_grade_cap`이 실제로 설정된 계열은 넷뿐이고 전부 N6 확장분이다.**

| 항목 | 값 |
|---|---|
| `source_quality_status` | **`warn`** |
| `source_quality_reasons` | **`final_vintage`** |
| **`source_quality_grade_cap`** | **`B`** |

사전등록 yaml에 직접 적혀 있다.

```yaml
source_quality: {status: warn, warning: final_vintage, grade_cap: B}
```

그리고 확장 등록분 전체에 대한 메모도 있다.

```yaml
notes:
  - N6 final-vintage families have an evidence-grade cap of B.
```

**결과를 보기 전에 "이 계열은 아무리 잘 나와도 A를 못 받는다"고 못 박았다.** §2.4의 원천
한계를 알고 등록했다는 뜻이다.

같은 상한이 걸린 넷은 이 family, [30_hc_productivity.md](30_hc_productivity.md),
[34_own_major_stake_change.md](34_own_major_stake_change.md),
[35_own_major_stake_level.md](35_own_major_stake_level.md)다.

**`revision_ratio`나 `mapping_fallback_ratio`는 NaN이다.** 분기 재무 vintage 대상 지표라
이 계열에는 적용되지 않는다. 경고 사유가 다르다.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2016-06-27 ~ 2025-02-05** |
| 유효 거래일 | **1,985일** |
| 날짜당 평균 종목 수 | **944~947개** |
| `coverage_ratio` | **0.672** |
| 관측 행 수 | 4,620,549 |

시장별로는 KOSDAQ 0.711 / KOSPI 0.613이다.

**패널의 33%가 빈다.** §2.1·§2.2의 조건들 때문이다 — 전년 값이 있어야 하고, 인원이 1명
이상이어야 하고, 구조 변경 + 30% 조건에 안 걸려야 한다.

---

## 7. 중복성

### A×B 교차 상관

| 상대 family | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `px_amihud_20d` | **−0.128** | 1,985 | −0.19 ~ **+0.42** |
| `px_resid_mom_12_1` | −0.047 | 1,985 | −0.53 ~ +0.12 |
| `px_near_52w_high` | +0.022 | 1,985 | −0.14 ~ +0.43 |

**`px_amihud_20d`와 −0.128이 가장 크다.** 직원이 많이 늘어난 회사는 비유동성이 낮다 =
규모가 크다. 다만 범위가 −0.19~+0.42로 **날짜에 따라 부호가 뒤집힌다.**

전체적으로 Phase A 계열과 거의 독립이다.

### 확인하지 않은 중복

1. **[30_hc_productivity.md](30_hc_productivity.md)와 분자를 공유한다.** 둘 다
   `headcount`를 쓴다. 하나는 그 변화율, 하나는 매출을 그것으로 나눈 값이다.
   **B×B 상관 산출물이 없다.**
2. **[24_fin_asset_growth_yoy.md](24_fin_asset_growth_yoy.md)와의 관계.** §3.1의 네 번째
   가설이 두 family를 같은 축으로 본다. 상관을 재지 않았다.

---

## 8. 한계와 확인 못 한 것

1. **근거가 약하다** (§4). 최소 q 0.093, t 최대 1.81. discovery 하나가 문턱을 0.007 차이로
   넘었다.
2. **discovery cell의 경제적 크기가 0이다** (§4.3). 5분위 차이 −0.006%p.
3. **시간 placebo를 받은 세 cell이 전부 떨어졌다** (§5.2). 통과한 하나는 받지 않았다.
4. **등급 상한이 B다** (§5.6). 원천이 최종본만 주기 때문이고, 이건 데이터를 다시 받아도
   해결되지 않는 구조적 한계다.
5. **쓰는 값이 평균 182일 묵었다** (§2.3). 20번에 한 번은 1년 3개월 지난 값이다.
6. **직원 구성을 구분하지 않는다.** 정규직·계약직, 사무직·생산직을 한 숫자로 센다.
   원천(`dart_employee_raw`)에는 구분이 있을 수 있는데 쓰지 않았다.
7. **구조 변경 필터가 완전한지 모른다** (§2.2). `structural_change_year`에 안 잡힌 합병이
   있으면 그대로 통과한다.
8. **`hc_productivity`와의 상관이 없다** (§7).
9. **업종 중립화가 없다.** 업종마다 고용 증가 속도가 근본적으로 다르다 — 제조업과
   플랫폼 기업을 한 풀에서 비교한다.
10. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
11. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T2

**T2 14-feature bundle에 들어갔다** (`hc_employee_growth_yoy`).

| horizon | Rank IC Δ | 비용 반영 spread Δ |
|---|---:|---:|
| 5 | +0.0031 | +0.0017 |
| 20 | +0.0011 | +0.0030 |
| 60 | +0.0003 | +0.0080 |

세 horizon 전부 개선됐다(`improved_all_horizons`). **14개를 함께 넣은 결과라 개별 기여도는
측정하지 않았다.**

**§4를 생각하면 이 family의 몫은 작을 가능성이 높다.** 단변량 근거가 14개 중 가장 약한
축이다. 확인할 방법은 없다.

**최종 h60 holdout은 아직 열지 않았다.**

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# 등급 상한이 걸린 N6 확장분 네 family
print(duckdb.sql(f"""
  select family, scan_type, h_start, h_end, ic_mean, q5_spread_aligned,
         q_fdr_global_ab, primary_discovery_ab, screen_pass,
         evidence_grade, source_quality_status, source_quality_grade_cap,
         p_temporal_nw
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  where source_quality_grade_cap = 'B'
  order by family, scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 커버리지·vintage 나이 | 같은 B run의 `core/feature_coverage.parquet` |
| 산식 | `research/etl/features/periodic_extras.py:99` |
| 최종본 한계 | 같은 파일 모듈 docstring |
| 등급 상한 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |
| 빈 칸 지목 | `01_feature_candidate/11_feature_taxonomy.md` §1, §2.4 |
| T2 결과 | `docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json` |
