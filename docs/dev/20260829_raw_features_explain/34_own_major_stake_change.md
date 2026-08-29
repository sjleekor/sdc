# 34. `own_major_stake_change` — 최대주주 지분율 변화

- 작성일: 2026-08-29
- family: `own_major_stake_change` · primary feature: **`own_major_stake_chg`** · domain: ownership
- **Phase B** · fdr_family `ownership` · **기대 부호 없음(양방향)** · 관측 부호 `+`
- **discovery 4/4 · screen-pass 4/4 · 실패한 게이트 없음** · 등급 **B 4개** · **등급 상한 B**
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**최대주주 지분율이 오른 회사가 이후 60~120일 동안 더 올랐다** (cum 0→120 IC +0.0416,
ICIR 1.013, 5분위 수익률 차이 +2.13%p).

**게이트를 전부 통과한 몇 안 되는 family다.** 4개 cell 전부 discovery + screen-pass,
`failed_gates` 비어 있음, 기간 **4/4**, 시간 placebo **통과**(p = 0.020 ~ 0.089),
거래가능 유지율 1.04~1.13.

**그런데 등급이 A가 아니라 B다.** 원천이 최종본만 주기 때문에 **사전등록 단계에서 상한
B가 걸려 있다** (§5.6). 통계 때문이 아니다.

[35_own_major_stake_level.md](35_own_major_stake_level.md)와 짝이다 — 저쪽은 지분율 수준,
이쪽은 그 **변화**다. 그리고 **변화 쪽이 더 강하다** (§4.2).

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/periodic_extras.py:181
CASE WHEN previous_year = bsns_year - 1
     THEN own_major_stake - previous_stake END AS own_major_stake_chg
```

**최대주주 지분율의 전년 대비 변화(%p)**다.

- +2.5면 1년 동안 지분율이 2.5%p 올랐다
- 음수면 줄었다

**비율의 비율이 아니라 차이다.** 30%에서 32.5%로 오른 것과 60%에서 62.5%로 오른 것이
같은 값이 된다.

### 2.2 최대주주 지분율은 어떻게 고르나

```sql
-- research/etl/features/periodic_extras.py:170
max(stake) FILTER (WHERE selection_priority = best_priority) AS own_major_stake
```

두 단계다.

1. **`selection_priority`가 가장 좋은(작은) 행만 남긴다.** 같은 사업연도에 여러 보고서가
   있으면 우선순위로 하나를 고른다.
2. **그중 지분율 최댓값을 쓴다.** 최대주주가 여럿 보고되면 가장 큰 값이 최대주주다.

그리고 범위 필터가 있다.

```sql
-- research/etl/features/periodic_extras.py:165
WHERE stake BETWEEN 0 AND 100
```

**0~100 밖의 값은 버린다.** 단위 오류나 입력 오류를 걸러 낸다.

### 2.3 두 해가 다 있어야 한다

```sql
CASE WHEN previous_year = bsns_year - 1 THEN ...
```

**바로 전 사업연도 값이 있어야 계산한다.** 한 해라도 건너뛰면 NULL이다.

노출 시점도 늦은 쪽을 기준으로 잡는다.

```sql
-- research/etl/features/periodic_extras.py:184
CASE WHEN previous_year = bsns_year - 1
     THEN greatest(stake_available_from, previous_available_from) END
    AS change_available_from
```

[29_hc_employee_growth.md](29_hc_employee_growth.md) §2.3과 같은 처리다.

### 2.4 값이 오래됐다

| 시장 | 평균 나이 | 95분위 나이 |
|---|---:|---:|
| KOSDAQ | **181.7일** | **475일** |
| KOSPI | **182.1일** | **490일** |

**평균 182일 된 정보다.** 연 1회 공시이고 두 해를 기다려야 해서다.
`hc_employee_growth`(182일)와 거의 같다.

### 2.5 원천은 DS002이고 최종본만 준다

[29_hc_employee_growth.md](29_hc_employee_growth.md) §2.4와 같다.

- `dart_governance_raw`(최대주주 현황/변동)가 원천이다
- **DS002는 최종 확정본만 준다.** 처음 공시된 값과 정정된 값을 구분해 주지 않는다
- `FINAL_VINTAGE_CAPTURE_RATIO = 0.0184`
- **PIT 원칙 자체는 지켰다** — 접수가 공개된 뒤에야 쓴다

`formula_version: periodic_extras_v2`, 정본 변형은 `native_t`다.

수집은 `krx-collector dart sync-periodic-extras`가 담당한다.

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/periodic_extras.py:181` |
| 최대주주 선택 | `research/etl/features/periodic_extras.py:170` |
| 범위 필터 | `research/etl/features/periodic_extras.py:165` |
| 최종본 한계 | 같은 파일 모듈 docstring |
| 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |

---

## 3. 왜 방향을 열어 뒀나

### 3.1 두 갈래가 반대를 가리킨다

```yaml
- family: own_major_stake_change
  expected_sign: null       # ← 방향을 걸지 않았다
```

| 가설 | 메커니즘 | 예측 부호 |
|---|---|---|
| **내부자 신호** | 최대주주가 지분을 늘린다 = 저평가라고 본다 | `+` |
| **지배력 강화** | 경영권이 안정되면 장기 투자가 가능해진다 | `+` |
| **유통주식 감소** | 최대주주 지분이 늘면 유동성이 나빠진다 | `−` |
| **소액주주 이익 침해** | 지배력이 세지면 견제가 약해진다 | `−` |

**어느 쪽이 우세한지 사전에 정할 근거가 없었다.**

양방향 판정 규칙은
[12_flow_individual_netbuy_to_volume.md](12_flow_individual_netbuy_to_volume.md) §3.3과 같다 —
방향 게이트를 적용하지 않고(`expected_sign_pass`가 `<NA>`), 기간 일관성은 관측 부호
기준으로 잰다.

### 3.2 사전등록 horizon

```yaml
primary_horizon_set: [60, 120]
exploratory_horizon_set: [20, 40]
include_bucket_primary: true
source_quality: {status: warn, warning: final_vintage, grade_cap: B}
```

연 1회 갱신되는 느린 지표이므로 긴 horizon에 걸었다. cell은 4개다.

**`source_quality` 블록이 사전등록에 직접 들어 있다** (§5.6).

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 60~120일 | **4개 cell 전부 discovery + screen-pass** |
| 부호 | 없음 | **`+` (네 cell 전부)** |

### 3.3 사전등록 시점

2026-08-27 확장 등록분이다 (`outcome_blind: true`).

분류 좌표는 **C3(수급·소유·내부자)** × T1(변화) × U다.
`11_feature_taxonomy.md` §2.1이 지목한 C3의 빈 칸 **「내부자·최대주주」**를 메우는 항목이다.

### 3.4 근거 문헌

없다. 신규 축이다. 지배구조와 수익률 연구(Gompers, Ishii & Metrick 2003 등)가 가장 가까운
배경이지만 직접 근거로 등록되지는 않았다.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

양방향 family이므로 `q5_spread_aligned`가 원값과 같다.

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 | AB q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | +0.0321 | 0.793 | 6.17 | +1.18%p | ~0 | **discovery + screen-pass (B)** |
| cum | 0→120 | **+0.0416** | **1.013** | 5.65 | **+2.13%p** | ~0 | **discovery + screen-pass (B)** |
| bucket | 40→60 | +0.0218 | 0.518 | 6.93 | +0.36%p | ~0 | **discovery + screen-pass (B)** |
| bucket | 60→120 | +0.0334 | 0.732 | 5.95 | +1.00%p | ~0 | **discovery + screen-pass (B)** |

**4개 전부 통과했고 `failed_gates`가 비어 있다.**

**Phase B에서 4/4 완전 통과는 다섯 family뿐이다** — `fin_log_mcap`, `ev_amendment_ratio`,
`own_amendment_ratio`, `own_major_filing_activity`, 그리고 이 family.

### 4.2 수준보다 변화가 강하다

**[35_own_major_stake_level.md](35_own_major_stake_level.md)와 나란히 놓으면 차이가 뚜렷하다.**

| | **`own_major_stake_change`** (변화) | `own_major_stake_level` (수준) |
|---|---:|---:|
| IC (cum 0→120) | **+0.0416** | +0.0335 |
| ICIR (cum 0→120) | **1.013** | 0.428 |
| t(NW) (cum 0→120) | **5.65** | 2.60 |
| 5분위 차이 (cum 0→120) | **+2.13%p** | +1.78%p |
| screen-pass | **4/4** | 2/4 |
| 시간 placebo | **전부 통과** | 절반 실패 |

**변화 쪽이 모든 지표에서 낫다.** 특히 ICIR이 2.4배다.

**해석은 자연스럽다.** 최대주주 지분율 수준은 회사마다 구조적으로 고정돼 있다 —
오너 기업은 늘 높고 소유분산 기업은 늘 낮다. 그건 **회사의 성격이지 새 정보가 아니다.**

반면 지분율 변화는 **최대주주가 올해 무엇을 했는가**를 담는다. 그게 신호에 가깝다.

`11_feature_taxonomy.md` §2.2의 축 B(시간적 형태)로 보면 **T0(수준)보다 T1(변화)이 나았다**는
결과다.

### 4.3 IC와 5분위 차이가 같은 방향이다

네 cell 전부 IC 양수·spread 양수다. 관계가 단조롭다.

**ownership 계열에서 경제적 크기가 뚜렷한 두 family 중 하나다**
([33_own_major_filing_activity.md](33_own_major_filing_activity.md)와 함께).

| family | \|IC\| | 5분위 차이 |
|---|---:|---:|
| `own_major_filing_activity` | 0.043 (60일) | −4.24%p |
| **`own_major_stake_change`** | **0.042 (120일)** | **+2.13%p** |
| `own_major_stake_level` | 0.034 (120일) | +1.78%p |
| `own_amendment_ratio` | 0.036 (120일) | 계산 불가 |
| `own_insider_filing_activity` | 0.003 | +0.07%p |

### 4.4 신호의 모양

| 관찰 | 값 |
|---|---|
| `peak_cell` | `cum 0→120` |
| `peak_ic_mean` | +0.0416 |
| 누적 IC 추이 | 60일 0.032 → 120일 0.042 (증가) |
| 구간 IC 추이 | 40~60일 0.022 → 60~120일 0.033 (증가) |

관측 범위 끝에서 최대다. 120일 너머는 확인하지 않았다.

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 4개 cell 전부 4/4

`valid_subperiods` = **4**, `sign_consistent_subperiods` = **4**, `period_sign_pass` = True.

**모든 유효 구간에서 같은 방향이었다.** 양방향 family이므로 관측 부호(`+`) 기준이다.

표본이 2016-06-27부터라 구간이 **4개**다. `2014_2016`이 비었다.

### 5.2 시간 placebo — 통과

| cell | `p_temporal_nw` | 판정 |
|---|---:|---|
| cum 0→60 | **0.0198** | **통과** |
| bucket 60→120 | **0.0792** | **통과** |
| cum 0→120 | **0.0891** | **통과** |
| bucket 40→60 | — | 대상 아님 (NW lag 19) |

기준은 0.10이다. **세 cell 전부 통과했다.**

다만 `cum 0→120`의 0.0891은 여유가 작다. 짧은 쪽(0.0198)이 훨씬 안전하다.

**N6 계열 넷 중 이 검사를 통과한 유일한 family다.**
[29_hc_employee_growth.md](29_hc_employee_growth.md)(0.55~0.71),
[30_hc_productivity.md](30_hc_productivity.md)(0.24~0.37),
[35_own_major_stake_level.md](35_own_major_stake_level.md)(절반 실패)와 갈린다.

### 5.3 비중첩 offset — `complete` 통과

세 cell 모두 `offset_status = complete`, `nonoverlap_robustness_pass = True`다.

### 5.4 거래 가능한 종목만 남겨도 — 오히려 강해진다

| cell | `tradable_retention` |
|---|---:|
| cum 0→60 | **1.127** |
| cum 0→120 | **1.121** |
| bucket 40→60 | **1.134** |
| bucket 60→120 | 1.041 |

**네 cell 전부 1을 넘는다.** 유동성 좋은 종목에서 4~13% 더 강하다.

§3.1의 세 번째 가설(유통주식 감소로 유동성 악화)이 주된 메커니즘이라면 유지율이 낮아야
하는데 그렇지 않다. **그 설명은 약해진다.**

### 5.5 생존편향

`available_direction_pass` = **True** (4개 cell 모두).

### 5.6 source quality — 등급 상한 B가 사전에 걸려 있다

**등급이 A가 아닌 유일한 이유다. 통계 때문이 아니다.**

| 항목 | 값 |
|---|---|
| `source_quality_status` | **`warn`** |
| `source_quality_reasons` | **`final_vintage`** |
| **`source_quality_grade_cap`** | **`B`** |
| `failed_gates` | **`[]`** (비어 있음) |

**게이트는 전부 통과했는데 상한 때문에 B다.**

사전등록 yaml에 직접 적혀 있고, 확장 등록분 메모에도 있다.

```yaml
notes:
  - N6 final-vintage families have an evidence-grade cap of B.
```

**결과를 보기 전에 못 박았다.** §2.5의 원천 한계를 알고 등록했다는 뜻이다.

같은 상한이 걸린 넷은 [29](29_hc_employee_growth.md), [30](30_hc_productivity.md),
이 family, [35](35_own_major_stake_level.md)다.

**이 상한은 데이터를 다시 받아도 해결되지 않는다.** DS002가 최종본만 주는 한 그대로다.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2016-06-27 ~ 2025-02-05** |
| 유효 거래일 | **1,985일** |
| 날짜당 평균 종목 수 | **978~981개** |
| `coverage_ratio` | **0.695** |
| 관측 행 수 | 4,784,037 |

시장별로는 KOSDAQ 0.737 / KOSPI 0.634다.

**패널의 30%가 빈다.** §2.3의 "두 해가 다 있어야 한다" 조건과 §2.2의 범위 필터 때문이다.

[35_own_major_stake_level.md](35_own_major_stake_level.md)(0.795, 2,234일)보다 커버리지가
10%p 낮고 표본이 249일 짧다. **변화를 만들려면 두 해가 필요하기 때문이다.**

---

## 7. 중복성

### A×B 교차 상관

| 상대 family | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `px_near_52w_high` | **+0.127** | 1,985 | **+0.03 ~ +0.51** |
| `px_idio_vol_60d` | **−0.103** | 1,985 | **−0.42 ~ −0.01** |
| `px_resid_mom_12_1` | +0.078 | 1,985 | −0.04 ~ +0.45 |
| `px_maxret_20d` | −0.076 | 1,985 | −0.41 ~ +0.03 |
| `px_amihud_20d` | +0.072 | 1,985 | −0.22 ~ +0.14 |

**두 관계가 방향이 안정적이다** (범위가 한쪽 부호로만 몰림).

- **`px_near_52w_high`와 +0.127.** 최대주주가 지분을 늘린 회사가 고점 근처에 있다.
  두 family의 기대·관측 부호가 둘 다 `+`이므로 **같은 방향으로 작동한다.**
- **`px_idio_vol_60d`와 −0.103.** 지분을 늘린 회사는 고유변동성이 낮다. 부호를 맞추면
  이것도 같은 방향이다.

`|ρ| ≥ 0.7` 경고 기준에는 한참 못 미친다.

### 확인하지 않은 중복

1. **[35_own_major_stake_level.md](35_own_major_stake_level.md)와 같은 값에서 나온다.**
   `own_major_stake`의 차분이 이 family다. **B×B 상관 산출물이 없다.**
   §4.2에서 결과를 비교했지만 두 값의 횡단 상관은 모른다.
2. **[33_own_major_filing_activity.md](33_own_major_filing_activity.md)와 원천이 겹친다.**
   지분 5% 이상 보유자의 공시 건수와 최대주주 지분율 변화는 같은 사건을 다르게 잰다.
3. **ownership 계열 다섯 간 B×B 상관이 전부 없다.**

---

## 8. 한계와 확인 못 한 것

1. **등급 상한이 B다** (§5.6). 게이트는 전부 통과했는데 원천이 최종본만 주기 때문이다.
   **데이터를 다시 받아도 해결되지 않는 구조적 한계다.**
2. **`own_major_stake_level`과의 상관이 없다** (§7). 한쪽이 다른 쪽의 차분인데 재지 않았다.
3. **쓰는 값이 평균 182일 묵었다** (§2.4). 20번에 한 번은 1년 3개월 지난 값이다.
4. **지분율 변화의 원인을 구분하지 않는다.** 최대주주가 직접 샀는지, 유상증자에 참여했는지,
   다른 주주가 팔아서 상대적으로 오른 건지 알 수 없다. §3.1의 네 가설을 가를 수 없다.
5. **최대주주가 바뀐 경우를 구분하지 않는다.** 최대주주 교체는 지분율 변화로만 나타나는데
   경제적 의미가 전혀 다르다.
6. **표본이 2016년부터라 기간 검정이 4구간뿐이다** (§5.1, §6).
7. **120일 너머를 안 봤다** (§4.4).
8. **업종 중립화가 없다.** 업종마다 지배구조 형태가 다르다.
9. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
10. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T2

**T2 14-feature bundle에 들어갔다** (`own_major_stake_chg`).

| horizon | Rank IC Δ | 비용 반영 spread Δ |
|---|---:|---:|
| 5 | +0.0031 | +0.0017 |
| 20 | +0.0011 | +0.0030 |
| 60 | +0.0003 | +0.0080 |

세 horizon 전부 개선됐다(`improved_all_horizons`). **14개를 함께 넣은 결과라 개별 기여도는
측정하지 않았다.**

**§4.2를 생각하면 이 family의 몫이 클 가능성이 있다.** 14개 중 게이트를 전부 통과하고
시간 placebo까지 넘긴 몇 안 되는 축이다.

다만 같은 묶음에 **`own_major_stake`(수준)가 함께 들어 있다.** §7에서 본 대로 한쪽이
다른 쪽의 차분이므로 정보가 겹칠 수 있다.

**최종 h60 holdout은 아직 열지 않았다.**

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# 게이트를 전부 통과한 (failed_gates 가 빈) cell 을 family 별로 센다
print(duckdb.sql(f"""
  select family,
         count(*) as cells,
         sum(case when screen_pass then 1 else 0 end) as screen_pass,
         max(evidence_grade) as grade,
         max(source_quality_grade_cap) as grade_cap
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  where phase='B' or family like 'own_%' or family like 'hc_%'
  group by family order by screen_pass desc, family
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 커버리지·vintage 나이 | 같은 B run의 `core/feature_coverage.parquet` |
| 산식 | `research/etl/features/periodic_extras.py:181` |
| 최대주주 선택 규칙 | `research/etl/features/periodic_extras.py:170` |
| 최종본 한계 | 같은 파일 모듈 docstring |
| 등급 상한 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |
| C3 빈 칸 지목 | `01_feature_candidate/11_feature_taxonomy.md` §2.1 |
| T2 결과 | `docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json` |
