# 20. `ev_net_share_issuance_yoy` — 순주식발행 증가율

- 작성일: 2026-08-29
- family: `ev_net_share_issuance_yoy` · primary feature: 동명 · domain: event
- **Phase B** · fdr_family `event` · 기대 부호 `−` · 관측 부호 `−`
- **discovery 4/4 · screen-pass 1/4** · 등급 **A 1개 / C 3개** · 실패 게이트: `robustness_pass`
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**주식을 새로 발행한 회사가 이후 부진했다** (cum 0→120 IC −0.0376). 기대 방향과 일치하고
4개 cell 전부 BH를 통과했다.

**그런데 세 cell이 강건성 게이트에서 떨어졌다.** 시간 placebo p가 0.139~0.238로 기준 0.10을
넘었다. 살아남은 건 짧은 구간 하나(`bucket 40→60`)뿐이다.

**두 가지가 이 family를 특히 조심해서 읽어야 하게 만든다.**

1. **IC와 5분위 수익률 차이의 부호가 정반대다.** IC는 −0.0376인데 상위 20%가 하위 20%보다
   **11.3%p 더 올랐다.** 35개 중 이 어긋남이 가장 크다 (§4.3).
2. **커버리지가 0.479로 Phase B 최저 수준이다.** 날짜당 종목이 668개뿐이다 (§6).

산식 자체는 이번 검증에서 가장 공들인 축에 속한다. **기계적 주식 수 증가(액면분할·무상증자)와
경제적 발행(유상증자·전환·행사)을 분리**하려고 자본변동 이력을 항등식으로 대조한다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/event_scan.py:436
CASE
    WHEN has_prior_year AND unclassified_1y = 0
     AND ABS((istc_totqy_prior + economic_increase_1y + mechanical_increase_1y
              - economic_decrease_1y - mechanical_decrease_1y) - istc_totqy)
         <= {identity_tolerance}
     AND istc_totqy_prior > 0
    THEN (economic_increase_1y - economic_decrease_1y) / istc_totqy_prior
END AS ev_net_share_issuance_yoy
```

**최근 1년의 경제적 순증 주식 수를 1년 전 발행주식수로 나눈 값**이다.

- +0.10이면 1년 동안 경제적 발행으로 주식이 10% 늘었다
- 음수면 유상감자 등으로 줄었다

### 2.2 핵심 설계 — 기계적 증가를 빼낸다

**발행주식수의 단순 YoY를 쓰면 안 된다.** 모듈 docstring이 이유를 적었다.

> using `dart_capital_change_raw` (B-PR2) to separate economic issuance (paid-in
> capital increases, conversions, option exercises) from **mechanical actions
> (splits, bonus issues, stock dividends) that a naive share-count YoY would
> misread as issuance**.

액면분할을 하면 주식 수가 두 배가 되지만 **자본이 들어온 게 아니다.** 무상증자·주식배당도
마찬가지다. 단순 YoY는 이것들을 "발행"으로 오독한다.

그래서 자본변동 사유(`isu_dcrs_stle`)를 **경제적 증가 / 기계적 증가 / 경제적 감소 /
기계적 감소** 네 묶음으로 분류하고, **경제적 항목만** 분자에 넣는다.

### 2.3 분류는 정확 일치만 한다 — 추측하지 않는다

```python
# research/etl/features/event_scan.py:62
# Matching stays EXACT. No normalization, no substring rule: a rule loose enough
# to absorb 무상감자 into 감자(무상) is also loose enough to silently absorb the
# next unseen reason, and §4.4 step 3 forbids inferring a reason this source did
# not give. Every string below was added by reading it, not by pattern.
```

**부분 문자열 매칭도, 정규화도 하지 않는다.** 사유 문자열을 하나씩 읽어서 목록에 넣었다.

목록에 없는 사유가 하나라도 있으면 `unclassified`가 되고, `unclassified_1y = 0` 조건 때문에
**그 1년 창의 값이 통째로 NULL이 된다.**

이 규율이 커버리지를 크게 깎았다(§6). 그리고 목록이 세 번 늘었다.

| 버전 | 변경 |
|---|---|
| v1 | 2026-08 최초 정의 |
| **v2** | 실제 데이터에 나오는 사유 4개 추가. **연간 vintage 이벤트의 22.4%가 목록이 짧아서 `unclassified`로 빠지고 있었다** |
| **v3** | `유상감자`를 경제적 감소에 추가. **v3 전에는 경제적 감소 집합이 아무것도 매칭하지 않았다** (원천이 `감자(유상)`로 쓰지 않는다). 699행 / 88개 발행사 |
| **v4** | 목록 변경이 아니라 정렬 순서 고정. position·vintage 선택자에 total-order tiebreaker를 넣어 **scan 순서에 따라 값이 달라지던 문제**를 고쳤다 |

`formula_version: issuance_v4`가 이 이력을 가리킨다. **v3 이전 결과와 지금 결과는 다른
숫자다.**

### 2.4 항등식으로 검증한다

```sql
ABS((istc_totqy_prior + economic_increase_1y + mechanical_increase_1y
     - economic_decrease_1y - mechanical_decrease_1y) - istc_totqy) <= tolerance
```

**"1년 전 발행주식수 + 모든 증가 − 모든 감소 = 지금 발행주식수"**가 성립해야 값을 만든다.
안 맞으면 분류가 빠진 것이므로 NULL이다.

산출물에 `issuance_identity_ok`, `issuance_classification_complete` 두 플래그가 함께
저장된다.

**자유 텍스트 사유 칸(`redc`, `profit_incnr`, `rdmstk_repy`, `etc`)은 읽지 않는다.**
원천이 주지 않은 사유를 추정하지 않는다는 원칙이다.

### 2.5 PIT

`available_from` 기준 interval join이다. 공시가 나온 뒤부터 쓴다. 정본 변형은 **`native_t`**다.

커버리지 통계에 **vintage 나이**가 기록돼 있다.

| 항목 | KOSDAQ | KOSPI |
|---|---:|---:|
| 평균 나이 | 52.8일 | 53.9일 |
| 95분위 나이 | 139일 | 133일 |

**쓰고 있는 값이 평균 53일 된 정보**라는 뜻이다. 연 1회 공시라 자연스럽다.

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/event_scan.py:436` |
| 사유 분류 목록 | `research/etl/features/event_scan.py:100` 이하 |
| 버전 이력 | `research/etl/features/event_scan.py:58` ~ `:82` |
| 알려진 문제 | `01_feature_candidate/10_known_issues.md` I3, I12 |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:497` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**주식 발행이 고평가 신호라는 가설이다.**

경영진은 자기 회사 주가가 비쌀 때 주식을 발행한다(market timing). 그리고 발행으로 들어온
돈이 잘 쓰인다는 보장이 없다. 어느 쪽이든 **발행 뒤 수익률이 낮다**는 예측이 나온다.

반대편은 자사주 매입이다. 주가가 싸다고 볼 때 사들이므로 이후 수익률이 높다.

**이 피처는 그 축의 양방향을 한 값으로 담는다** — 양수는 발행, 음수는 감자다.

### 3.2 기대 부호

`−`. 순발행이 클수록 이후 초과수익률 순위가 낮다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_config.yaml:503
primary_horizon_set: [60, 120]
exploratory_horizon_set: [20, 40]
include_bucket_primary: true
```

연 1회 갱신되는 느린 지표이므로 긴 horizon에 걸었다. cell은 누적 2 + 구간 2 = 4개다.

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 60~120일 | 4개 cell 전부 discovery, **screen-pass는 1개** |
| 부호 | `−` | **`−` (일치)** |

### 3.4 한국 시장 단서

`02_feature_candidate.md` §1의 13번 항목(`Q5`)이다.

> 주식 발행/희석 | `ev_net_share_issuance_yoy` | 발행 증가 `-` | A | R1

분류 좌표는 C4(이벤트·공시) × T1(변화) × U다.

### 3.5 근거 문헌

Pontiff & Woodgate (2008), Daniel & Titman (2006) 계열의 net share issuance 이례현상.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

부호가 `−`이므로 5분위 차이는 방향 정렬값이다. **양수면 기대대로인데, 전부 음수다.**

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이(정렬) | AB q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | −0.0300 | −0.646 | −5.09 | **−3.22%p** | ~0 | discovery, **screen 실패** (등급 C) |
| cum | 0→120 | **−0.0376** | −0.757 | −4.39 | **−11.33%p** | 0.00002 | discovery, **screen 실패** (등급 C) |
| bucket | 40→60 | −0.0221 | −0.514 | −6.60 | **−2.20%p** | ~0 | **discovery + screen-pass (등급 A)** |
| bucket | 60→120 | −0.0254 | −0.564 | −4.26 | **−6.19%p** | 0.00004 | discovery, **screen 실패** (등급 C) |

### 4.2 세 cell이 시간 placebo에서 떨어졌다

`failed_gates`가 셋 다 `[robustness_pass]`다. 내역은 이렇다.

| cell | `p_temporal_nw` | `temporal_null_pass` | `offset_status` |
|---|---:|---|---|
| cum 0→60 | **0.1386** | **False** | complete |
| cum 0→120 | **0.2178** | **False** | complete |
| bucket 60→120 | **0.2376** | **False** | complete |
| bucket 40→60 | — | 대상 아님 | — |

기준은 0.10이다. **시간축을 밀어 만든 가짜 신호가 관측값만큼 극단적인 결과를 14~24% 확률로
만들어 냈다.**

비중첩 offset은 세 cell 모두 `complete`로 통과했으므로, 떨어진 이유는 **시간 placebo
하나다.**

살아남은 `bucket 40→60`은 NW lag가 19라 애초에 placebo 대상이 아니었다. **검사를 통과한 게
아니라 받지 않았다.** 등급 A가 붙었지만 이 사정을 알고 읽어야 한다.

### 4.3 IC와 5분위 차이가 정반대다 — 35개 중 가장 크다

**이 문서에서 가장 중요한 대목이다.**

방향 정렬값이 전부 음수라는 건 **원값 기준으로는 기대와 반대**라는 뜻이다.

| cell | Rank IC | 5분위 차이(원값) | 읽는 법 |
|---|---:|---:|---|
| cum 0→120 | −0.0376 | **+11.33%p** | 순위로는 발행 많은 쪽이 나쁜데, **상위 20% 평균은 11.3%p 더 올랐다** |
| bucket 60→120 | −0.0254 | +6.19%p | 같은 방향의 어긋남 |
| cum 0→60 | −0.0300 | +3.22%p | 같은 방향의 어긋남 |

[04_px_near_52w_high.md](04_px_near_52w_high.md) §4.3, [08_px_turnover_shock.md](08_px_turnover_shock.md)
§4.2에서 본 것과 같은 구조인데 **크기가 압도적으로 크다.**

원인은 분포다. 순발행 상위 20%는 **대규모 유상증자를 한 회사들**이다. 이 집단은 수익률 분포가
극단적으로 오른쪽으로 치우친다 — 대부분 희석으로 부진하지만, 자금 조달에 성공해 크게 오르는
소수가 평균을 통째로 끌어올린다.

정리하면 이렇다.

> **순위로 보면 발행이 많은 종목이 나쁘다(IC −). 그런데 상위 20% 평균만 보면 소수의 대박이
> 그 손해를 압도한다(spread +11.3%p).**

**어느 쪽이 맞다고 이 자료로 판정할 수 없다.** 중앙값 기반 spread나 분위별 평균수익률을
산출하지 않았다. **이 family를 실제로 쓸지 판단하려면 이게 먼저다.**

### 4.4 신호의 모양

| 관찰 | 값 |
|---|---|
| `peak_cell` | `cum 0→120` |
| `peak_ic_mean` | −0.0376 |
| 누적 \|IC\| 추이 | 60일 0.030 → 120일 0.038 (증가) |
| 구간 \|IC\| 추이 | 40~60일 0.022 → 60~120일 0.025 (증가) |

관측 범위 끝에서 최대다.

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 4개 cell 전부 5/5

`valid_subperiods` = 5, `sign_consistent_subperiods` = **5**, `period_sign_pass` = True.

**기간 안정성은 문제가 없다.** 떨어진 건 시간 placebo다.

### 5.2 시간 placebo — 세 cell 실패

§4.2에 정리했다. **이 family가 screen-pass를 놓친 유일한 이유다.**

### 5.3 비중첩 offset — 전부 `complete` 통과

세 cell 모두 `offset_status = complete`, `nonoverlap_robustness_pass = True`다.
창 중첩 보정에서는 문제가 없었다.

### 5.4 거래 가능한 종목만 남기면 — 오히려 강해진다

| cell | `tradable_retention` | `tradable_pass` |
|---|---:|---|
| cum 0→60 | 1.081 | True |
| cum 0→120 | **1.115** | True |
| bucket 40→60 | 1.033 | True |
| bucket 60→120 | 1.093 | True |

네 cell 전부 1을 넘는다. **유동성 좋은 종목에서 더 강하다.** 소형주 착시가 아니다.

### 5.5 생존편향

`available_direction_pass` = **True** (4개 cell 모두).

### 5.6 source quality — 대상 아님

`source_quality_status` = `not_applicable`, `source_quality_grade_cap` = `None`.

**다만 §2.3의 v2·v3 이력이 남긴 경고는 별개다.** 사유 분류 목록이 짧아 22.4%가 누락되던
시기가 있었고, `유상감자`는 v3 전까지 아예 매칭되지 않았다. **지금 값은 그 수정 이후
버전이지만, 목록이 여전히 완전한지는 알 수 없다.**

---

## 6. 표본과 커버리지 — Phase B 최저 수준

| 항목 | 값 |
|---|---|
| 유효 표본 | **2016-04-15 ~ 2025-02-05** |
| 유효 거래일 | **2,145일** |
| 날짜당 평균 종목 수 | **668개** |
| `coverage_ratio` | **0.479** |
| 관측 행 수 | 3,452,470 |

### 6.1 절반이 NULL이다

**패널의 52%에 값이 없다.** §2.3·§2.4의 조건들이 겹친 결과다.

- 1년 창 안에 `unclassified` 사유가 하나라도 있으면 NULL
- 항등식이 안 맞으면 NULL
- 직전 연도 데이터가 없으면 NULL
- `istc_totqy_prior > 0`이어야 함

**규율을 지킨 대가다.** 사유를 추측했다면 커버리지는 올라갔겠지만 값의 신뢰도가 떨어졌을
것이다.

시장별 커버리지는 KOSDAQ 0.473 / KOSPI 0.488로 거의 같다. 다른 event family가 KOSPI 쪽이
낮았던 것과 다르다.

### 6.2 날짜당 668종목이 뜻하는 것

`ev_amendment_ratio`(1,040종목), `ev_payout_yield`(988종목)와 비교하면 3분의 2 수준이다.

횡단면이 작으면 순위의 분해능이 떨어지고 5분위 각 칸에 130여 종목만 들어간다.
**§4.3의 극단값 문제가 더 크게 작동할 수 있는 조건이다.**

---

## 7. 중복성

### A×B 교차 상관

| 상대 family | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `px_near_52w_high` | **−0.111** | 2,145 | −0.25 ~ +0.19 |
| `px_idio_vol_60d` | **+0.103** | 2,145 | +0.00 ~ +0.20 |
| `px_maxret_20d` | +0.074 | 2,145 | −0.04 ~ +0.24 |
| `px_mom_12_1` | −0.065 | 2,145 | −0.22 ~ +0.33 |

- **`px_idio_vol_60d`와 +0.103.** 범위가 전부 양수라 안정적이다. 주식을 많이 발행한
  회사는 고유변동성이 크다.
- **`px_near_52w_high`와 −0.111.** 발행한 회사는 고점에서 멀다.

`|ρ| ≥ 0.7` 경고 기준에는 한참 못 미친다.

### 확인하지 않은 중복

같은 `feat_event_scan_daily` 마트에서 나온 [21_ev_payout_yield.md](21_ev_payout_yield.md)와의
관계를 재지 않았다. **둘은 같은 축의 양 끝**이다 — 하나는 주식을 발행하고, 하나는 배당·
자사주로 돌려준다. 경제적으로 반대 방향이므로 음의 상관이 예상된다.

**B×B 상관 산출물이 없다.**

---

## 8. 한계와 확인 못 한 것

1. **IC와 5분위 차이가 정반대다** (§4.3). 35개 중 가장 큰 어긋남이다. 풀 자료(중앙값
   spread, 분위별 수익률)가 없다. **이 family에서 가장 시급하다.**
2. **세 cell이 시간 placebo에서 떨어졌다** (§4.2). 살아남은 하나는 검사 대상이 아니었다.
3. **커버리지가 0.479로 절반이 빈다** (§6.1). 날짜당 668종목뿐이다.
4. **사유 분류 목록이 완전한지 모른다** (§2.3). v2에서 22.4% 누락, v3에서 `유상감자`
   추가라는 이력이 있다. 다음 미분류가 없다고 보장할 수 없다.
5. **`ev_payout_yield`와의 상관이 없다** (§7). 같은 축의 반대편인데 재지 않았다.
6. **120일 너머를 안 봤다** (§4.4).
7. **평균 53일 된 정보를 쓴다** (§2.5). 연 1회 공시의 구조적 한계다.
8. **업종 중립화가 없다.** 업종마다 자금 조달 관행이 다르다.
9. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
10. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T2

**T2 14-feature bundle에 들어갔다** (`ev_net_share_issuance_yoy`).

| horizon | Rank IC Δ | 비용 반영 spread Δ |
|---|---:|---:|
| 5 | +0.0031 | +0.0017 |
| 20 | +0.0011 | +0.0030 |
| 60 | +0.0003 | +0.0080 |

세 horizon 전부 개선됐다(`improved_all_horizons`). **14개를 함께 넣은 결과라 개별 기여도는
측정하지 않았다.**

**주의.** 이 family는 4개 cell 중 3개가 screen-pass를 놓쳤는데도 T2 후보에 들어갔다.
T2 후보 선정은 cell 단위 screen-pass가 아니라 **family 단위 후보 목록**으로 정해졌기
때문이다 (`phase_b_acceptance_gate_results.json`의 `candidate_features` 14개).

**최종 h60 holdout은 아직 열지 않았다.**

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# IC 와 5분위 차이의 부호가 갈리는 cell 을 찾는다
print(duckdb.sql(f"""
  select family, feature, scan_type, h_start, h_end, ic_mean,
         q5_spread_raw, q5_spread_aligned, expected_sign,
         screen_pass, evidence_grade, failed_gates, p_temporal_nw
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  where family='ev_net_share_issuance_yoy'
  order by scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 커버리지·vintage 나이 | 같은 B run의 `core/feature_coverage.parquet` |
| 자본변동 품질 | 같은 B run의 `core/capital_change_quality.parquet` |
| 산식 | `research/etl/features/event_scan.py:436` |
| 사유 분류 이력 | `research/etl/features/event_scan.py:58` ~ `:82` |
| 알려진 문제 | `01_feature_candidate/10_known_issues.md` I3, I12 |
| T2 결과 | `docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json` |
