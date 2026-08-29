# 30. `hc_productivity` — 1인당 매출 (노동생산성)

- 작성일: 2026-08-29
- family: `hc_productivity` · primary feature: **`hc_revenue_per_employee`** · domain: human_capital
- **Phase B** · fdr_family `human_capital` · **기대 부호 없음(양방향)** · 관측 부호 `+`
- **discovery 2/4 · screen-pass 1/4** · 등급 **B 1개 / C 3개** · source quality `warn` · **등급 상한 B**
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**직원 1인당 매출이 높은 회사가 이후 조금 더 올랐다** (cum 0→120 IC +0.0181).
[29_hc_employee_growth.md](29_hc_employee_growth.md)와 짝을 이루는 인적자본 축의 두 번째
피처다.

**같은 계열이지만 근거가 조금 더 낫다.**

| | `hc_employee_growth` | **`hc_productivity`** |
|---|---:|---:|
| 최소 BH q | 0.093 | **0.0029** |
| discovery | 1/4 | **2/4** |
| 최대 t(NW) | 1.81 | **3.12** |
| 기간 일관성 | 4구간 중 3~4 | **5구간 중 4** |
| 유효 거래일 | 1,985 | **2,176** |

**그런데 두 가지가 걸린다.**

1. **IC와 5분위 수익률 차이의 부호가 정반대다.** 네 cell 전부 IC는 양수인데 spread는
   음수다 (§4.3).
2. **등급 상한이 B다.** `final_vintage` 경고가 사전에 걸려 있다 (§5.6).

시간 placebo를 받은 두 cell은 전부 떨어졌다 (p = 0.238, 0.366).

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/periodic_extras.py:127
SELECT
    e.ticker, e.bsns_year,
    greatest(e.employee_available_from, r.revenue_available_from) AS available_from,
    ln(r.revenue / e.headcount) AS hc_revenue_per_employee
FROM employee_annual e
JOIN annual_revenue r USING (ticker, bsns_year)
WHERE e.headcount > 0
  AND e.employee_available_from IS NOT NULL
  AND r.revenue_available_from IS NOT NULL
```

**연간 매출액을 직원 수로 나눈 뒤 로그를 취한 값**이다.

로그를 씌우는 이유는 분포가 심하게 치우쳐 있기 때문이다 — 반도체 회사와 노동집약 제조업의
1인당 매출은 자릿수가 다르다. **Rank IC는 순위만 쓰므로 로그 여부가 IC 값을 바꾸지 않는다.**

### 2.2 두 원천을 합친다 — 늦은 쪽을 기준으로

**이 family의 구조적 특징이다.**

| 입력 | 원천 |
|---|---|
| 직원 수 (`headcount`) | **DS002 정기보고서 주요정보** → `dart_employee_raw` |
| 연간 매출 (`revenue`) | **재무제표 vintage** → `stock_metric_vintage_fact` |

```sql
greatest(e.employee_available_from, r.revenue_available_from) AS available_from
```

**둘 중 늦게 공개된 쪽부터 쓴다.** 하나라도 없으면 값이 없다.

모듈 docstring이 이를 명시했다.

> Growth/change values use the later of the current and previous receipt, and
> **productivity also waits for the annual revenue vintage.**

### 2.3 매출은 연간 보고서(11011)만 쓴다

```sql
-- research/etl/features/periodic_extras.py:112
WHERE metric_code = 'revenue' AND reprt_code = '11011' AND value_numeric > 0
QUALIFY row_number() OVER (
    PARTITION BY ticker, bsns_year
    ORDER BY CASE WHEN fs_basis = 'CFS' THEN 0 ELSE 1 END,
             available_from DESC NULLS LAST, rcept_no DESC
) = 1
```

세 가지가 규정돼 있다.

1. **`reprt_code = '11011'`** — 사업보고서(연간)만이다. 분기 매출을 합치지 않는다.
   직원 수가 연 1회 값이므로 분자도 연간이어야 단위가 맞는다.
2. **연결(CFS) 우선.** `CASE WHEN fs_basis = 'CFS' THEN 0 ELSE 1 END`로 정렬한다.
3. **전순서 tiebreaker.** `available_from DESC`, `rcept_no DESC`까지 붙여
   **scan 순서에 따라 값이 달라지지 않게** 고정했다. 같은 문제를
   [20_ev_net_share_issuance_yoy.md](20_ev_net_share_issuance_yoy.md) §2.3의 v4가 고쳤다.

**주의할 점.** 직원 수는 DS002 최종본이고 매출은 재무 vintage인데, **`fs_basis` 통일이
없다.** 매출은 연결 기준일 수 있고 직원 수는 별도 기준 인원일 수 있다. 이 불일치는 이번에
확인하지 않았다.

### 2.4 값이 가장 늦게 온다 — 35개 중 최장

| 시장 | 평균 나이 | 95분위 나이 |
|---|---:|---:|
| KOSDAQ | **189.1일** | **489일** |
| KOSPI | **189.0일** | **574일** |

**평균 189일 된 정보이고, 20번에 한 번은 1년 7개월 묵은 값을 쓴다.**
35개 family 중 가장 오래됐다.

`hc_employee_growth`(182일)보다도 늦은 이유는 §2.2다. **두 원천을 기다려야 하고, 늦은 쪽을
기준으로 잡는다.**

### 2.5 최종본만 준다는 한계는 같다

`hc_employee_growth`와 같은 DS002 원천을 쓰므로
[29_hc_employee_growth.md](29_hc_employee_growth.md) §2.4의 한계가 그대로 적용된다.

- DS002는 최종 확정본만 준다
- `FINAL_VINTAGE_CAPTURE_RATIO = 0.0184`
- PIT 원칙 자체는 지켰지만, 값이 나중에 고쳐졌다면 고쳐진 값을 쓰게 된다

`formula_version: periodic_extras_v2`. 정본 변형은 `native_t`다.

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/periodic_extras.py:127` |
| 매출 선택 규칙 | `research/etl/features/periodic_extras.py:112` |
| 최종본 한계 | 같은 파일 모듈 docstring |
| 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |

---

## 3. 왜 방향을 열어 뒀나

### 3.1 세 갈래가 갈린다

```yaml
- family: hc_productivity
  expected_sign: null       # ← 방향을 걸지 않았다
```

| 가설 | 메커니즘 | 예측 부호 |
|---|---|---|
| **효율성** | 적은 인원으로 많은 매출 = 잘 경영한다 | `+` |
| **자본집약도** | 1인당 매출이 높은 건 업종 특성일 뿐 | 0 |
| **인적자본 부족** | 사람이 부족해 과부하 상태 | `−` |

**두 번째가 특히 걸린다.** 유통업과 소프트웨어의 1인당 매출은 근본적으로 다르다.
**업종 중립화 없이 재면 업종 더미를 재는 셈이 될 수 있다** (§8).

그래서 방향을 걸지 않았다. 양방향 판정 규칙은
[12_flow_individual_netbuy_to_volume.md](12_flow_individual_netbuy_to_volume.md) §3.3과 같다.

### 3.2 사전등록 horizon

```yaml
primary_horizon_set: [60, 120]
exploratory_horizon_set: [20, 40]
include_bucket_primary: true
source_quality: {status: warn, warning: final_vintage, grade_cap: B}
```

연 1회 갱신되는 가장 느린 지표이므로 긴 horizon에 걸었다. cell은 4개다.

**`source_quality` 블록이 사전등록에 직접 들어 있다** (§5.6).

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 60~120일 | **discovery 2개** |
| 부호 | 없음 | **`+`** |

### 3.3 사전등록 시점

2026-08-27 확장 등록분이다 (`outcome_blind: true`).

분류 좌표는 **C7(시장구조·비재무 — 인적자본)** × T0(수준) × U다.
`hc_employee_growth`가 T1(변화)인 것과 대비된다 — **같은 원천으로 수준과 변화를 각각
만들었다.**

### 3.4 근거 문헌

없다. 신규 축이다. `11_feature_taxonomy.md` §1이 지목한 빈 칸(인적자본)을 메우려는 시도다.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

양방향 family이므로 `q5_spread_aligned`가 원값과 같다.

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 | AB q | 등급 | 판정 |
|---|---|---:|---:|---:|---:|---:|---|---|
| cum | 0→60 | +0.0136 | 0.223 | 1.69 | **−0.55%p** | 0.120 | C | BH 실패 |
| cum | 0→120 | **+0.0181** | 0.285 | 1.48 | **−0.80%p** | 0.174 | C | BH 실패 |
| bucket | 40→60 | +0.0144 | 0.237 | **3.12** | **−0.12%p** | **0.0029** | **B** | **discovery + screen-pass** |
| bucket | 60→120 | +0.0170 | 0.281 | 2.11 | **−0.24%p** | 0.0495 | C | discovery, robustness 실패 |

**discovery 2개, screen-pass 1개다.**

### 4.2 짧은 구간이 통과하고 긴 구간이 떨어지는 구조

[29_hc_employee_growth.md](29_hc_employee_growth.md) §4.2와 같은 패턴이다.

`bucket 40→60`이 |IC|는 중간인데 t가 3.12로 가장 크다. **NW lag가 19라 중첩 보정이 작기
때문이다.** 누적 0→120은 lag가 119라 같은 IC로도 t가 1.48까지 떨어진다.

**"짧은 구간이 통과했다"를 "짧은 구간에서 신호가 강하다"로 읽으면 안 된다.**

### 4.3 IC와 5분위 차이가 정반대다 — 네 cell 전부

**이 family의 가장 중요한 특징이다.**

| cell | Rank IC | 5분위 차이 | 방향 |
|---|---:|---:|---|
| cum 0→60 | **+0.0136** | **−0.55%p** | 반대 |
| cum 0→120 | **+0.0181** | **−0.80%p** | 반대 |
| bucket 40→60 | **+0.0144** | **−0.12%p** | 반대 |
| bucket 60→120 | **+0.0170** | **−0.24%p** | 반대 |

**예외 없이 전부 어긋난다.** 다른 family에서는 일부 cell만 갈렸는데
([04_px_near_52w_high.md](04_px_near_52w_high.md), [08_px_turnover_shock.md](08_px_turnover_shock.md))
여기는 네 개 전부다.

읽으면 이렇다.

> **순위로 보면 1인당 매출이 높은 종목이 낫다(IC +). 그런데 상위 20% 평균과 하위 20%
> 평균을 비교하면 오히려 낮다(spread −).**

원인은 분포에 있을 가능성이 높다. 1인당 매출 하위 20%는 **인원 대비 매출이 적은 회사**들,
즉 바이오·플랫폼처럼 아직 매출이 안 나오는 성장 기업이 섞인다. 이 집단은 수익률 분포가
오른쪽으로 심하게 치우쳐 있어 **소수의 대박이 평균을 끌어올린다.**

**어느 쪽이 맞다고 이 자료로 판정할 수 없다.** 중앙값 기반 spread나 분위별 평균수익률을
산출하지 않았다.

**§3.1의 두 번째 가설(업종 특성)과도 이어진다.** 업종이 통제되지 않으면 이런 어긋남이
생기기 쉽다.

### 4.4 신호의 모양

| 관찰 | 값 |
|---|---|
| `peak_cell` | `cum 0→120` |
| `peak_ic_mean` | +0.0181 |
| 누적 IC 추이 | 60일 0.0136 → 120일 0.0181 (증가) |
| 구간 IC 추이 | 40~60일 0.0144 → 60~120일 0.0170 (증가) |

관측 범위 끝에서 최대다. `hc_employee_growth`가 평평했던 것과 다르다.

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 5구간 중 4구간

| cell | `valid_subperiods` | `sign_consistent_subperiods` | `period_sign_pass` |
|---|---:|---:|---|
| 4개 전부 | **5** | **4** | True |

**구간이 5개다.** 표본이 2015-06-29부터라 `2014_2016`이 부분적으로 잡힌다.
`hc_employee_growth`(4구간)보다 검정력이 낫다.

### 5.2 시간 placebo — 받은 두 cell 전부 실패

| cell | `p_temporal_nw` | `temporal_null_pass` |
|---|---:|---|
| cum 0→60 | **0.3663** | **False** |
| cum 0→120 | **0.2376** | **False** |
| bucket 60→120 | **0.2376** | **False** |
| bucket 40→60 | — | 대상 아님 (NW lag 19) |

기준은 0.10이다. **24~37% 확률로 시간 이동 placebo가 관측값만큼 극단적인 결과를 만들었다.**

`hc_employee_growth`(0.55~0.71)보다는 낫지만 여전히 통과하지 못한다.

**screen-pass한 `bucket 40→60`은 이 검사를 받지 않았다.**

### 5.3 비중첩 offset — `complete` 통과

세 긴 cell 모두 `offset_status = complete`, `nonoverlap_robustness_pass = True`다.
떨어진 건 시간 placebo 하나다.

### 5.4 거래 가능한 종목만 남겨도

| cell | `tradable_retention` |
|---|---:|
| cum 0→60 | **1.208** |
| cum 0→120 | 1.143 |
| bucket 40→60 | 1.042 |
| bucket 60→120 | 1.025 |

**네 cell 전부 1을 넘는다.** 유동성 좋은 종목에서 더 강하다.
`hc_employee_growth`(0.93~1.08)보다 높다.

### 5.5 생존편향

`available_direction_pass` = **True** (4개 cell 모두).

### 5.6 source quality — 등급 상한 B

[29_hc_employee_growth.md](29_hc_employee_growth.md) §5.6과 **완전히 같다.**

| 항목 | 값 |
|---|---|
| `source_quality_status` | **`warn`** |
| `source_quality_reasons` | **`final_vintage`** |
| **`source_quality_grade_cap`** | **`B`** |

사전등록 yaml에 직접 적혀 있고, 확장 등록분 메모에도 있다.

```yaml
notes:
  - N6 final-vintage families have an evidence-grade cap of B.
```

**결과를 보기 전에 상한을 못 박았다.** screen-pass한 cell이 A가 아니라 B인 이유다.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2015-06-29 ~ 2025-02-05** |
| 유효 거래일 | **2,176일** |
| 날짜당 평균 종목 수 | **904~906개** |
| `coverage_ratio` | **0.704** |
| 관측 행 수 | 4,842,808 |

`hc_employee_growth`(1,985일, 0.672)보다 표본이 길고 커버리지가 높다.

**전년 값이 필요 없기 때문이다.** 증가율은 두 해가 있어야 하지만 수준은 한 해면 된다.

시장별로는 KOSDAQ 0.753 / KOSPI 0.631이다.

---

## 7. 중복성

### A×B 교차 상관

| 상대 family | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `px_idio_vol_60d` | **−0.109** | 2,176 | −0.19 ~ +0.03 |
| `px_near_52w_high` | +0.083 | 2,176 | −0.07 ~ **+0.57** |
| `px_amihud_20d` | −0.081 | 2,176 | −0.19 ~ +0.30 |
| `px_maxret_20d` | −0.069 | 2,176 | −0.19 ~ +0.12 |

**전부 0.11 미만이다.** Phase A 계열과 거의 독립이다.

`px_idio_vol_60d`와 −0.109는 방향이 안정적이다(범위가 거의 전부 음수). 1인당 매출이 높은
회사는 고유변동성이 낮다.

### 확인하지 않은 중복

1. **[29_hc_employee_growth.md](29_hc_employee_growth.md)와 `headcount`를 공유한다.**
   하나는 그 변화율, 하나는 매출을 그것으로 나눈 값이다. **B×B 상관 산출물이 없다.**
2. **[27_fin_value_z.md](27_fin_value_z.md)의 S/P 요소와 분자를 공유한다.** 둘 다 연간
   매출을 쓴다. 분모가 시가총액이냐 직원 수냐만 다르다.
3. **업종과의 관계.** §3.1의 두 번째 가설을 확인하려면 업종별 분포를 봐야 하는데
   **업종 코드가 수집돼 있지 않다** (`11_feature_taxonomy.md` §1).

---

## 8. 한계와 확인 못 한 것

1. **IC와 5분위 차이가 네 cell 전부 반대다** (§4.3). 35개 중 가장 일관된 어긋남이다.
   풀 자료가 없다. **이 family를 쓸지 판단하려면 이게 먼저다.**
2. **업종 효과를 분리하지 못한다** (§3.1). 1인당 매출은 업종 특성이 지배적일 수 있는데
   업종 중립화가 없다. **이 family가 업종 부재에 가장 취약한 축이다.**
3. **시간 placebo를 받은 세 cell이 전부 떨어졌다** (§5.2).
4. **등급 상한이 B다** (§5.6). 원천이 최종본만 주는 구조적 한계다.
5. **쓰는 값이 35개 중 가장 오래됐다** (§2.4). 평균 189일, 95분위 574일.
6. **`fs_basis` 통일이 없다** (§2.3). 매출은 연결일 수 있고 직원 수는 별도 기준일 수 있다.
7. **직원 구성을 구분하지 않는다.** 정규직·계약직을 한 숫자로 센다.
8. **`hc_employee_growth`와의 상관이 없다** (§7). 분자를 공유하는데 재지 않았다.
9. **`fin_value_z`의 S/P와의 관계를 안 쟀다** (§7).
10. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
11. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T2

**T2 14-feature bundle에 들어갔다** (`hc_revenue_per_employee`).

| horizon | Rank IC Δ | 비용 반영 spread Δ |
|---|---:|---:|
| 5 | +0.0031 | +0.0017 |
| 20 | +0.0011 | +0.0030 |
| 60 | +0.0003 | +0.0080 |

세 horizon 전부 개선됐다(`improved_all_horizons`). **14개를 함께 넣은 결과라 개별 기여도는
측정하지 않았다.**

**§4.3을 생각하면 주의가 필요하다.** 단변량 IC는 양수인데 5분위 수익률 차이는 음수다.
모델이 이 피처를 어느 방향으로 쓰는지, 그게 도움이 됐는지는 이번 설계로 알 수 없다.

**최종 h60 holdout은 아직 열지 않았다.**

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# IC 와 5분위 차이의 부호가 갈리는 cell 을 전체에서 찾는다
print(duckdb.sql(f"""
  select family, scan_type, h_start, h_end, ic_mean, q5_spread_aligned
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  where status='valid'
    and sign(ic_mean) <> sign(q5_spread_raw)
  order by family, scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 커버리지·vintage 나이 | 같은 B run의 `core/feature_coverage.parquet` |
| 산식 | `research/etl/features/periodic_extras.py:127` |
| 매출 선택 규칙 | `research/etl/features/periodic_extras.py:112` |
| 등급 상한 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |
| 업종 부재 지적 | `01_feature_candidate/11_feature_taxonomy.md` §1 |
| T2 결과 | `docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json` |
