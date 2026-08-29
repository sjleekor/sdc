# 35. `own_major_stake_level` — 최대주주 지분율 수준

- 작성일: 2026-08-29
- family: `own_major_stake_level` · primary feature: **`own_major_stake`** · domain: ownership
- **Phase B** · fdr_family `ownership` · **기대 부호 없음(양방향)** · 관측 부호 `+`
- **discovery 4/4 · screen-pass 2/4** · 등급 **B 2개 / C 2개** · **등급 상한 B**
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**최대주주 지분율이 높은 회사가 이후 조금 더 올랐다** (cum 0→120 IC +0.0335,
5분위 수익률 차이 +1.78%p).

**[34_own_major_stake_change.md](34_own_major_stake_change.md)의 짝이고, 그쪽이 더 강하다.**
이건 지분율 **수준**, 저건 그 **변화**다. 같은 값에서 나오는데 모든 지표에서 변화 쪽이 낫다.

| | `own_major_stake_level` (수준) | `own_major_stake_change` (변화) |
|---|---:|---:|
| IC (cum 0→120) | +0.0335 | **+0.0416** |
| **ICIR (cum 0→120)** | **0.428** | **1.013** |
| t(NW) (cum 0→120) | 2.60 | **5.65** |
| screen-pass | **2/4** | **4/4** |
| 시간 placebo | **절반 실패** | **전부 통과** |
| 커버리지 | **0.795** | 0.695 |

**커버리지만 이쪽이 낫다** — 한 해만 있으면 되기 때문이다.

등급이 A가 아닌 이유는 [34](34_own_major_stake_change.md) §5.6과 같다. 원천이 최종본만
주기 때문에 **사전등록 단계에서 상한 B가 걸려 있다.**

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/periodic_extras.py:170
max(stake) FILTER (WHERE selection_priority = best_priority) AS own_major_stake
```

**최대주주 지분율(%) 그 자체**다.

- 45.3이면 최대주주가 지분 45.3%를 갖고 있다
- 오너 기업은 높고 소유분산 기업은 낮다

선택 규칙은 [34_own_major_stake_change.md](34_own_major_stake_change.md) §2.2와 같다.

1. `selection_priority`가 가장 좋은 행만 남긴다 (같은 사업연도의 여러 보고서 중 하나 선택)
2. 그중 지분율 최댓값을 쓴다
3. `WHERE stake BETWEEN 0 AND 100`으로 범위 밖 값을 버린다

### 2.2 한 해만 있으면 된다 — 커버리지가 더 높은 이유

**변화(`_chg`)와 결정적으로 다른 점이다.**

```sql
-- 수준: 조건 없음
own_major_stake

-- 변화: 전년 값이 있어야 함
CASE WHEN previous_year = bsns_year - 1
     THEN own_major_stake - previous_stake END
```

노출 시점도 다르다.

```sql
-- research/etl/features/periodic_extras.py:180
stake_available_from AS level_available_from          -- 수준: 그 해 공시만 기다림
greatest(stake_available_from, previous_available_from)  -- 변화: 늦은 쪽을 기다림
```

**결과가 커버리지와 표본에 나타난다.**

| | 수준 | 변화 |
|---|---:|---:|
| `coverage_ratio` | **0.795** | 0.695 |
| 유효 시작 | **2015-06-25** | 2016-06-27 |
| 유효 거래일 | **2,234** | 1,985 |
| 날짜당 종목 | **1,003** | 979 |

**한 해분이 덜 필요하니 1년 일찍 시작하고 10%p 더 넓다.**

### 2.3 값이 오래됐다

| 시장 | 평균 나이 | 95분위 나이 |
|---|---:|---:|
| KOSDAQ | **182.3일** | **461일** |
| KOSPI | **186.4일** | **559일** |

**평균 182~186일 된 정보다.** 연 1회 공시라 변화 쪽(182일)과 거의 같다. 한 해만 기다려도
공시 주기 자체가 1년이라 나이가 크게 줄지 않는다.

### 2.4 원천은 DS002이고 최종본만 준다

[34_own_major_stake_change.md](34_own_major_stake_change.md) §2.5와 같다.

- `dart_governance_raw`(최대주주 현황)가 원천이다
- **DS002는 최종 확정본만 준다**
- `FINAL_VINTAGE_CAPTURE_RATIO = 0.0184`
- PIT 원칙 자체는 지켰다 — 접수가 공개된 뒤에야 쓴다

`formula_version: periodic_extras_v2`, 정본 변형은 `native_t`다.

### 2.5 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/periodic_extras.py:170` |
| 범위 필터 | `research/etl/features/periodic_extras.py:165` |
| 노출 시점 | `research/etl/features/periodic_extras.py:180` |
| 최종본 한계 | 같은 파일 모듈 docstring |
| 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |

---

## 3. 왜 방향을 열어 뒀나

### 3.1 지배구조 문헌이 양쪽으로 갈린다

```yaml
- family: own_major_stake_level
  expected_sign: null       # ← 방향을 걸지 않았다
```

| 가설 | 메커니즘 | 예측 부호 |
|---|---|---|
| **이해 일치** | 최대주주 지분이 크면 소액주주와 이해가 같아진다 | `+` |
| **장기 경영** | 경영권이 안정되면 장기 투자가 가능하다 | `+` |
| **참호 구축** | 지배력이 세면 견제가 안 되고 사익 추구가 는다 | `−` |
| **유통물량 부족** | 유통주식이 적어 유동성이 나쁘다 | `−` |
| **코리아 디스카운트** | 지배구조 문제가 밸류에이션을 눌러 왔다 | `−` |

**한국 시장에서 특히 갈리는 축이다.** 지배구조 할인 논의는 오래됐지만, 그것이 **횡단면
수익률 예측력**으로 이어지는지는 별개 문제다.

방향을 정할 근거가 없어 열어 뒀다. 양방향 판정 규칙은
[12_flow_individual_netbuy_to_volume.md](12_flow_individual_netbuy_to_volume.md) §3.3과 같다.

### 3.2 사전등록 horizon

```yaml
primary_horizon_set: [60, 120]
exploratory_horizon_set: [20, 40]
include_bucket_primary: true
source_quality: {status: warn, warning: final_vintage, grade_cap: B}
```

연 1회 갱신되는 느린 지표이므로 긴 horizon에 걸었다. cell은 4개다.

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 60~120일 | **4개 cell 전부 discovery, screen-pass 2개** |
| 부호 | 없음 | **`+` (네 cell 전부)** |

### 3.3 사전등록 시점

2026-08-27 확장 등록분이다 (`outcome_blind: true`).

분류 좌표는 **C3(수급·소유·내부자)** × **T0(수준)** × U다.
[34](34_own_major_stake_change.md)가 T1(변화)인 것과 대비된다 — **같은 원천으로 수준과
변화를 각각 만들어 어느 형태가 나은지 본 설계다.**

### 3.4 근거 문헌

없다. 신규 축이다. Gompers, Ishii & Metrick (2003) 계열의 지배구조 연구가 배경이지만
직접 근거로 등록되지는 않았다.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

양방향 family이므로 `q5_spread_aligned`가 원값과 같다.

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 | AB q | 등급 | 판정 |
|---|---|---:|---:|---:|---:|---:|---|---|
| cum | 0→60 | +0.0301 | 0.484 | 3.93 | +0.80%p | 0.00016 | **B** | **screen-pass** |
| cum | 0→120 | **+0.0335** | 0.428 | 2.60 | **+1.78%p** | 0.0139 | C | robustness 실패 |
| bucket | 40→60 | +0.0191 | 0.274 | 3.59 | +0.28%p | 0.00059 | **B** | **screen-pass** |
| bucket | 60→120 | +0.0213 | 0.239 | 1.86 | +0.95%p | 0.0841 | C | robustness 실패 |

**4개 전부 discovery이고 2개가 screen-pass다.**

`bucket 60→120`의 q가 0.084로 기준 0.10에 가깝다.

### 4.2 수준보다 변화가 나은 이유

§1의 표를 다시 보면 **ICIR 차이가 가장 크다** (0.428 대 1.013).

**IC 평균은 비슷한데 일별 IC의 흔들림이 두 배 이상 크다는 뜻이다.**

해석은 자연스럽다. **최대주주 지분율 수준은 회사의 구조적 성격이다.** 오너 기업은 몇 년째
높고 소유분산 기업은 몇 년째 낮다. 값이 거의 안 변하므로 **횡단 순위도 거의 안 변한다.**

그러면 IC가 시장 국면에 통째로 좌우된다 — 어떤 시기에는 오너 기업이 좋고 어떤 시기에는
나쁘다. **평균은 양수인데 흔들림이 크다.**

변화 쪽은 "올해 최대주주가 무엇을 했는가"를 담으므로 매년 순위가 새로 짜인다.

**`11_feature_taxonomy.md` §2.2의 축 B로 보면 T0(수준)보다 T1(변화)이 나았다는 결과다.**
같은 원천에서 두 형태를 만들어 비교한 설계가 값을 했다.

### 4.3 IC와 5분위 차이가 같은 방향이다

네 cell 전부 IC 양수·spread 양수다. 관계가 단조롭다.

120일 기준 +1.78%p다. ownership 계열에서 세 번째로 크다
([33](33_own_major_filing_activity.md) −4.24%p, [34](34_own_major_stake_change.md) +2.13%p 다음).

### 4.4 신호의 모양

| 관찰 | 값 |
|---|---|
| `peak_cell` | `cum 0→120` |
| `peak_ic_mean` | +0.0335 |
| 누적 IC 추이 | 60일 0.030 → 120일 0.034 (완만한 증가) |
| 구간 IC 추이 | 40~60일 0.019 → 60~120일 0.021 (거의 평평) |

**누적이 60일 이후 거의 평평하다.** 새로 더해지는 신호가 적다는 뜻이고, §4.2의 "값이 거의
안 변한다"와 이어진다.

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 5구간 중 4~5구간

| cell | `valid_subperiods` | `sign_consistent_subperiods` | `period_sign_pass` |
|---|---:|---:|---|
| cum 0→60 | 5 | **5** | True |
| bucket 40→60 | 5 | **5** | True |
| cum 0→120 | 5 | 4 | True |
| bucket 60→120 | 5 | 4 | True |

**전부 통과했고 짧은 두 cell은 5/5다.**

**구간이 5개다.** 표본이 2015-06-25부터라 `2014_2016`이 부분적으로 잡힌다.
[34](34_own_major_stake_change.md)(4구간)보다 검정력이 낫다.

### 5.2 시간 placebo — 하나 통과, 둘 실패

| cell | `p_temporal_nw` | 판정 |
|---|---:|---|
| cum 0→60 | **0.0297** | **통과** |
| cum 0→120 | **0.2475** | **실패** |
| bucket 60→120 | **0.4257** | **실패** |
| bucket 40→60 | — | 대상 아님 (NW lag 19) |

기준은 0.10이다.

**긴 구간 둘이 떨어졌다.** [34](34_own_major_stake_change.md)가 세 cell 전부 통과한 것과
갈린다.

`failed_gates`가 두 cell에서 `[robustness_pass]`이고, 비중첩 offset은 `complete`로
통과했으므로 **떨어진 이유는 시간 placebo 하나다.**

### 5.3 비중첩 offset — `complete` 통과

세 cell 모두 `offset_status = complete`, `nonoverlap_robustness_pass = True`다.

### 5.4 거래가능 유지율이 cell마다 크게 다르다

| cell | `tradable_retention` | `tradable_pass` |
|---|---:|---|
| cum 0→60 | **0.935** | True |
| bucket 40→60 | 1.023 | True |
| cum 0→120 | **1.213** | True |
| bucket 60→120 | **1.430** | True |

**0.94에서 1.43까지 흩어진다.** 네 cell 전부 게이트는 통과하지만 값이 안정적이지 않다.

|IC|가 0.019~0.034로 작아 비율의 분모가 작기 때문이다
([24_fin_asset_growth_yoy.md](24_fin_asset_growth_yoy.md) §5.4의 극단 사례보다는 덜하지만
같은 성질이다).

**유지율을 읽을 때는 분모의 절대 크기를 함께 봐야 한다.**

### 5.5 생존편향

`available_direction_pass` = **True** (4개 cell 모두).

### 5.6 source quality — 등급 상한 B

[34_own_major_stake_change.md](34_own_major_stake_change.md) §5.6과 **완전히 같다.**

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

**결과를 보기 전에 못 박았다.** screen-pass한 두 cell이 A가 아니라 B인 이유다.

이 상한이 걸린 넷은 [29](29_hc_employee_growth.md), [30](30_hc_productivity.md),
[34](34_own_major_stake_change.md), 이 family다.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2015-06-25 ~ 2025-02-05** |
| 유효 거래일 | **2,234일** |
| 날짜당 평균 종목 수 | **1,001~1,004개** |
| `coverage_ratio` | **0.795** |
| 관측 행 수 | 5,466,249 |

**N6 계열 넷 중 커버리지가 가장 높다.**

| family | 커버리지 | 유효 거래일 |
|---|---:|---:|
| **`own_major_stake_level`** | **0.795** | **2,234** |
| `hc_productivity` | 0.704 | 2,176 |
| `own_major_stake_change` | 0.695 | 1,985 |
| `hc_employee_growth` | 0.672 | 1,985 |

§2.2에서 본 대로 **한 해만 있으면 되기 때문이다.** 변화·증가율 형태 둘은 두 해가 필요해
커버리지와 표본을 잃는다.

시장별로는 KOSDAQ 0.847 / KOSPI 0.716이다.

---

## 7. 중복성

### A×B 교차 상관

| 상대 family | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `px_amihud_20d` | **+0.174** | 2,234 | **−0.64 ~ +0.29** |
| `px_idio_vol_60d` | **−0.141** | 2,234 | **−0.33 ~ −0.02** |
| `px_maxret_20d` | −0.089 | 2,234 | −0.40 ~ +0.12 |

- **`px_amihud_20d`와 +0.174.** 최대주주 지분율이 높은 회사는 비유동적이다. §3.1의
  「유통물량 부족」 가설과 방향이 맞다. **다만 범위가 −0.64 ~ +0.29로 매우 넓다** —
  날짜에 따라 관계가 크게 뒤집힌다.
- **`px_idio_vol_60d`와 −0.141.** 지분율이 높은 회사는 고유변동성이 낮다. 범위가 거의
  전부 음수라 안정적이다.

`|ρ| ≥ 0.7` 경고 기준에는 못 미친다.

### 확인하지 않은 중복

1. **[34_own_major_stake_change.md](34_own_major_stake_change.md)와 같은 값에서 나온다.**
   이 family의 차분이 저 family다. **B×B 상관 산출물이 없다.** §4.2에서 결과를 비교했지만
   두 값의 횡단 상관은 모른다.
2. **[33_own_major_filing_activity.md](33_own_major_filing_activity.md)와 원천이 겹친다.**
   5% 대량보유 공시와 최대주주 지분율은 같은 사건을 다르게 잰다.
3. **규모와의 관계.** `px_amihud_20d`와 +0.174이므로 규모와도 얽힐 수 있는데
   `fin_log_mcap`·`mcap_krx_log`와의 상관이 없다 (B×B 부재).

---

## 8. 한계와 확인 못 한 것

1. **변화 형태보다 약하다** (§4.2). ICIR 0.428 대 1.013. 같은 원천에서 만든 두 형태 중
   수준 쪽이 뒤진다.
2. **등급 상한이 B다** (§5.6). 원천이 최종본만 주기 때문이고, 데이터를 다시 받아도
   해결되지 않는다.
3. **긴 두 cell이 시간 placebo에서 떨어졌다** (§5.2).
4. **`own_major_stake_change`와의 상관이 없다** (§7). 한쪽이 다른 쪽의 차분인데 재지 않았다.
   **T2 묶음에 둘 다 들어갔으므로 이 공백이 실질적 문제다** (§9).
5. **거래가능 유지율이 흩어진다** (§5.4). 0.94~1.43.
6. **쓰는 값이 평균 182~186일 묵었다** (§2.3).
7. **최대주주가 누구인지 구분하지 않는다.** 창업자 일가, 지주회사, 국민연금, 외국계 펀드가
   같은 "최대주주"로 묶인다. §3.1의 다섯 가설을 가르려면 이 구분이 필요하다.
8. **특수관계인 합산 여부를 확인하지 않았다.** 원천이 어느 기준으로 보고하는지에 따라
   값의 의미가 달라진다.
9. **업종 중립화가 없다.** 업종마다 지배구조 형태가 다르다.
10. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
11. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T2

**T2 14-feature bundle에 들어갔다** (`own_major_stake`).

| horizon | Rank IC Δ | 비용 반영 spread Δ |
|---|---:|---:|
| 5 | +0.0031 | +0.0017 |
| 20 | +0.0011 | +0.0030 |
| 60 | +0.0003 | +0.0080 |

세 horizon 전부 개선됐다(`improved_all_horizons`). **14개를 함께 넣은 결과라 개별 기여도는
측정하지 않았다.**

**같은 묶음에 `own_major_stake_chg`가 함께 들어 있다.** §7에서 본 대로 한쪽이 다른 쪽의
차분이므로 정보가 겹칠 수 있다.

T2 14개 안에는 이런 쌍이 셋 있다.

| 쌍 | 관계 |
|---|---|
| `fin_log_mcap` · `mcap_krx_log` | 같은 개념, 다른 원천 |
| `ev_amendment_ratio_1y` · `own_amendment_ratio_1y` | 전체 대 부분집합 |
| **`own_major_stake` · `own_major_stake_chg`** | **수준 대 그 차분** |

**14개가 14개의 독립된 정보가 아니라는 뜻이다.** 묶음 개선분을 개별 기여로 나눠 읽으면
안 된다.

**최종 h60 holdout은 아직 열지 않았다.**

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# 같은 원천에서 만든 수준과 변화를 cell 단위로 대조한다
print(duckdb.sql(f"""
  select scan_type, h_start, h_end,
         max(case when family='own_major_stake_level'  then ic_mean end) as ic_level,
         max(case when family='own_major_stake_change' then ic_mean end) as ic_change,
         max(case when family='own_major_stake_level'  then icir end)    as icir_level,
         max(case when family='own_major_stake_change' then icir end)    as icir_change,
         max(case when family='own_major_stake_level'  then p_temporal_nw end) as p_level,
         max(case when family='own_major_stake_change' then p_temporal_nw end) as p_change
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  where family in ('own_major_stake_level','own_major_stake_change')
  group by scan_type, h_start, h_end order by scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 커버리지·vintage 나이 | 같은 B run의 `core/feature_coverage.parquet` |
| 산식 | `research/etl/features/periodic_extras.py:170` |
| 노출 시점 규칙 | `research/etl/features/periodic_extras.py:180` |
| 최종본 한계 | 같은 파일 모듈 docstring |
| 등급 상한 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |
| 수준/변화 축 구분 | `01_feature_candidate/11_feature_taxonomy.md` §2.2 |
| T2 결과 | `docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json` |
