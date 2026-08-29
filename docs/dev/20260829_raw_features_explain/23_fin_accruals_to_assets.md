# 23. `fin_accruals_to_assets` — 발생액 (이익과 현금흐름의 차이)

- 작성일: 2026-08-29
- family: `fin_accruals_to_assets` · primary feature: 동명 · domain: financial
- **Phase B** · fdr_family `financial` · 기대 부호 `−` · **관측 부호 `+`**
- **discovery 0/4 · screen-pass 0/4** · 등급 **C 3개 / D 1개** · source quality `warn`
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**기대와 반대로 나왔고, 그 반대 방향마저 불안정하다.** 발생액이 큰 회사가 이후 부진해야
하는데 오히려 **조금 더 올랐다** (cum 0→120 IC **+0.0180**).

**세 검사가 전부 걸린다.**

| 검사 | 결과 |
|---|---|
| 기대 부호 | **불일치** (`−` 예상, `+` 관측) → discovery 0개 |
| 기간 일관성 | **4구간 중 0~1구간만 일치** → `period_sign_pass = False` |
| 시간 placebo | **실패** (p = 0.406 / 0.564 / 0.792) |
| 원천 품질 | **`warn`** — 총자산의 10.1%가 사후 정정, 순이익의 31.8%가 매핑 대체 |

BH는 4개 cell 중 3개가 통과했지만(최소 q 0.00043) **부호가 반대라 discovery로 세지 않는다.**

**표본도 짧다.** 2017-02-27 시작, 1,927거래일, 날짜당 865종목, 커버리지 0.596. 기간 검정이
5구간이 아니라 **4구간**이다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/fin_scan.py:283
CASE WHEN avg_assets > 0
          AND net_income_selected IS NOT NULL
          AND operating_cash_flow_selected IS NOT NULL
     THEN (net_income_selected - operating_cash_flow_selected) / avg_assets
END AS fin_accruals_to_assets
```

**(순이익 − 영업현금흐름) ÷ 평균 총자산**이다.

- 양수면 이익이 현금보다 많다 = 발생액이 크다
- 음수면 현금이 이익보다 많다

발생액은 **회계 이익 중 아직 현금으로 들어오지 않은 부분**이다. 매출채권이 늘거나 재고가
쌓이면 이익은 잡히는데 현금은 안 들어온다.

### 2.2 세 값이 같은 회계 기준을 쓴다 — 이 마트의 핵심 설계

모듈 docstring이 설명한다.

> Shared fs_basis per ticker-date (§3.6): rather than resolving CFS/OFS
> independently per metric (which could silently mix bases across a single
> day's feature bundle), every metric is interval-joined under *both* bases,
> then one `fs_basis_used` decision — CFS if `net_income` has a CFS value at
> this date, else OFS — selects every other metric's value for that date.
> **This is what makes `fin_accruals_to_assets`'s net_income/CFO/avg_assets a
> genuine "same four-quarter set, same fs basis" computation, not three
> independently-chosen bases that happen to collide.**

연결(CFS)과 별도(OFS)를 지표마다 따로 고르면, 순이익은 연결인데 현금흐름은 별도인 값이
섞일 수 있다. **그러면 빼기가 의미를 잃는다.**

그래서 `net_income`의 CFS 존재 여부로 **하루의 기준을 하나로 정하고** 나머지를 그 기준에
맞춘다. 산출물에 `fs_basis_used`가 함께 저장된다.

**이 family가 그 설계의 이유가 된 지표다.**

### 2.3 분모가 평균 총자산이다

`avg_assets`는 기말이 아니라 **평균** 총자산이다. 분자가 기간 흐름(1년치 이익·현금흐름)이므로
분모도 기간 평균이어야 단위가 맞는다.

### 2.4 PIT와 vintage 나이

`accruals_available_from` 기준 interval join이다. 정본 변형은 **`native_t`**다.

커버리지 통계에 나이가 기록돼 있다.

| 시장 | 평균 나이 | 95분위 나이 |
|---|---:|---:|
| KOSDAQ | **74.5일** | **182일** |
| KOSPI | 73.6일 | 160일 |

**쓰고 있는 값이 평균 74일 된 정보이고, 20번에 한 번은 반년 가까이 묵은 값이다.**
분기 공시 주기의 구조적 한계다.

### 2.5 산식 버전

`formula_version: fin_v4`. 같은 마트의 다섯 family가 공유한다.

`fin_v1`에는 심각한 버그가 있었다 — winsorize 단계에서 NULL 가드가 없어 재무제표가 아예
없는 회사가 시장 1분위로 채워졌다. **발행 값의 29.2%가 규칙을 어겼다**
(`10_known_issues.md` I1). 지금은 고쳐졌지만, **v1 시절 결과와 지금 결과는 다른 숫자다.**

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/fin_scan.py:283` |
| fs_basis 통일 설계 | 같은 파일 모듈 docstring |
| 알려진 문제 | `01_feature_candidate/10_known_issues.md` I1 |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:473` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**발생액 이례현상(accrual anomaly)이다.**

Sloan (1996)의 발견이다. 회계 이익은 현금 부분과 발생액 부분으로 나뉘는데, **발생액 부분이
덜 지속된다.** 투자자는 그 차이를 구분하지 못하고 이익 전체를 같은 무게로 본다. 그래서
발생액이 큰 회사는 이후 실적이 실망스럽고 주가도 부진하다.

**과대평가 가설**이다. 회계 조작까지 갈 필요도 없다 — 매출채권이 늘어난 정상적인 성장도
발생액을 키운다.

### 3.2 기대 부호

`−`. 발생액이 클수록 이후 초과수익률 순위가 낮다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_config.yaml:479
primary_horizon_set: [60, 120]
exploratory_horizon_set: [20, 40]
include_bucket_primary: true
```

분기 공시 기반의 느린 지표이므로 긴 horizon에 걸었다. cell은 4개다.

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 60~120일 | **discovery 0개** |
| 부호 | `−` | **`+` (반대)** |

### 3.4 한국 시장 단서

`02_feature_candidate.md` §1의 11번 항목(`Q3`)이다.

> 발생액 | `fin_accruals_to_assets` | Q3 | `-` | A | R1

그리고 `11_feature_taxonomy.md` §4가 인용한 Han, Lee & Kang (2020)의 한국 복제 결과가
배경을 준다 — **수익성 카테고리 복제율 5.0%(엄격 기준 0.0%)**로 한국에서 재현이 잘 안 되는
영역이다.

분류 좌표는 C2(재무 기반 상태) × T0(수준) × U다.

### 3.5 근거 문헌

Sloan (1996), *Do Stock Prices Fully Reflect Information in Accruals and Cash Flows
About Future Earnings?* 등급 A. 미국에서는 가장 널리 복제된 회계 이례현상이다.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

부호가 `−` 기대이므로 5분위 차이는 방향 정렬값이다. **양수면 기대대로인데, 전부 양수다.**
그런데 IC도 양수다 — 이 조합이 뜻하는 걸 §4.3에서 본다.

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이(정렬) | AB q | 부호 | 판정 |
|---|---|---:|---:|---:|---:|---:|---|---|
| cum | 0→60 | +0.0152 | 0.440 | 3.23 | +0.08%p | 0.0021 | **반대** | BH 통과, discovery 아님 (C) |
| cum | 0→120 | **+0.0180** | 0.501 | 2.50 | +0.10%p | 0.0185 | **반대** | BH 통과, discovery 아님 (C) |
| bucket | 40→60 | +0.0109 | 0.294 | 3.70 | +0.03%p | 0.0004 | **반대** | BH 통과, discovery 아님 (**D**) |
| bucket | 60→120 | +0.0136 | 0.388 | 2.72 | +0.15%p | 0.0100 | **반대** | BH 통과, discovery 아님 (C) |

**4개 전부 `expected_sign_pass = False`다.** BH를 통과해도 discovery가 아니다
([02_px_mom_12_1.md](02_px_mom_12_1.md) §4.2와 같은 규율이다).

### 4.2 `failed_gates`가 cell마다 다르다

| cell | 등급 | `failed_gates` |
|---|---|---|
| bucket 40→60 | **D** | `primary_discovery`, `period_sign_pass` |
| cum 0→60 | C | `primary_discovery`, `period_sign_pass`, `robustness_pass` |
| cum 0→120 | C | `primary_discovery`, `period_sign_pass`, `robustness_pass` |
| bucket 60→120 | C | `primary_discovery`, `period_sign_pass`, `robustness_pass` |

`bucket 40→60`이 D인 건 강건성 검사 대상이 아니어서 실패 목록이 짧기 때문이 아니라,
등급 규칙상 **신호 없음·부호 반대·강건성 실패**가 D이기 때문이다
(`evidence_grade.D: no_signal_or_wrong_sign_or_robustness_fail`).

나머지 셋이 C인 건 `robustness_pass`까지 실패해 다른 분기로 빠졌기 때문이다.

### 4.3 부호가 반대인데 5분위 차이도 "정렬 후 양수"인 이유

혼동하기 쉬운 대목이다.

`q5_spread_aligned`는 **기대 부호를 곱한 값**이다. 기대가 `−`이므로 `aligned = −1 × raw`다.
표의 +0.10%p는 **원값이 −0.10%p**라는 뜻이다.

| cell | `q5_spread_raw` | `q5_spread_aligned` | 읽는 법 |
|---|---:|---:|---|
| cum 0→120 | −0.0010 | +0.0010 | 발생액 상위 20%가 하위 20%보다 **0.10%p 덜 올랐다** |

**여기서 IC와 5분위 차이가 갈린다.** IC는 +0.0180(발생액 큰 쪽이 좋다)인데 5분위 원값 차이는
−0.10%p(발생액 큰 쪽이 나쁘다)다.

[04_px_near_52w_high.md](04_px_near_52w_high.md) §4.3에서 본 것과 같은 구조다. 순위 상관과
양 끝 평균이 다른 이야기를 한다.

**다만 크기가 워낙 작아** (IC 0.018, spread 0.1%p) 어느 쪽도 경제적으로 의미 있는 수준이
아니다.

### 4.4 크기

|IC| 0.018은 Phase B 재무 계열에서 가장 작은 축이다. 5분위 차이 0.1%p는 사실상 0이다.

t값이 2.5~3.7로 유의한 건 표본이 1,927일로 길기 때문이다. **유의성과 크기를 분리해서 봐야
한다.**

---

## 5. 진짜인가 — 강건성

**세 검사가 전부 걸린다.**

### 5.1 기간 일관성 — 4구간 중 0~1구간

| cell | `valid_subperiods` | `sign_consistent_subperiods` | `period_sign_pass` |
|---|---:|---:|---|
| cum 0→60 | 4 | **0** | **False** |
| cum 0→120 | 4 | **1** | **False** |
| bucket 40→60 | 4 | **0** | **False** |
| bucket 60→120 | 4 | **1** | **False** |

**두 가지를 함께 봐야 한다.**

**첫째, 구간이 5개가 아니라 4개다.** 표본이 2017-02-27부터라 `2014_2016` 구간이 통째로
비었다. 검정력이 그만큼 낮다.

**둘째, 0~1은 애매한 값이다.** 기대 부호가 `−`인데 0이면 "네 구간 전부 양수"이므로
[08_px_turnover_shock.md](08_px_turnover_shock.md)처럼 **반대 방향이 일관**된 것이고,
1이면 **한 구간만 기대 방향**이었다는 뜻이다.

cell마다 0과 1이 섞여 있다. **반대 방향이 완벽하게 일관되지도 않다.**

### 5.2 시간 placebo — 전부 실패

| cell | `p_temporal_nw` | `temporal_null_pass` |
|---|---:|---|
| cum 0→60 | **0.4059** | **False** |
| cum 0→120 | **0.7921** | **False** |
| bucket 60→120 | **0.5644** | **False** |
| bucket 40→60 | — | 대상 아님 |

기준은 0.10이다. **시간축을 밀어 만든 가짜 신호가 관측값만큼 극단적인 결과를 41~79% 확률로
만들어 냈다.** 사실상 무작위와 구분되지 않는다.

Phase A의 `px_resid_mom_12_1`(0.614)과 비슷한 수준이다.

### 5.3 비중첩 offset — `complete`인데 통과 못 했다

`offset_status = complete`이지만 `nonoverlap_robustness_pass = False`다.

**검정을 돌릴 수 있었는데 통과하지 못했다는 뜻이다.** `insufficient`(검정 자체를 못 함)와
다르다 — [15_flow_short_interest.md](15_flow_short_interest.md) §5.2와 대조된다.

### 5.4 거래 가능한 종목만 남기면 — 오히려 강해진다

| cell | `tradable_retention` |
|---|---:|
| cum 0→60 | **1.208** |
| cum 0→120 | **1.238** |
| bucket 40→60 | 1.129 |
| bucket 60→120 | 1.172 |

네 cell 전부 1을 크게 넘는다. **유동성 좋은 종목에서 반대 신호가 20% 이상 강하다.**

소형주 착시가 아니라는 뜻이지만, §5.1·§5.2를 생각하면 **"안정적으로 반대"라고 말하기에는
근거가 부족하다.**

### 5.5 생존편향

`available_direction_pass` = **True** (4개 cell 모두).

### 5.6 source quality — `warn`

| 항목 | 값 |
|---|---|
| `source_quality_status` | **`warn`** |
| `source_quality_reasons` | **`revision`** |
| `revision_ratio` | **0.1014** |
| `revision_worst_metric` | **`total_assets`** |
| `mapping_fallback_ratio` | **0.3176** |
| `mapping_fallback_worst_metric` | **`net_income`** |
| `pairing_mismatch_ratio` | 0.000125 |

**두 가지가 걸린다.**

1. **총자산의 10.1%가 사후 정정됐다.** 기준이 0.10이므로 근소하게 넘는다
   (`08_phase_b_implementation_log.md`: "즉 `warn`을 만든 건 정정 비율 하나다.
   0.10을 근소하게 넘는다").
2. **순이익의 31.8%가 매핑 대체 경로로 채워졌다.** 표준 계정명으로 직접 못 찾아
   다른 규칙으로 채운 비율이다.

이번 scan은 **최종본(final vintage)**을 쓰므로 그 시점에 실제로 알 수 있었던 값보다 정확한
값을 쓴 셈이다.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2017-02-27 ~ 2025-02-05** |
| 유효 거래일 | **1,927일** |
| 날짜당 평균 종목 수 | **865개** |
| `coverage_ratio` | **0.596** |
| 관측 행 수 | 4,296,914 |

**패널의 40%에 값이 없다.** 순이익과 영업현금흐름이 둘 다 있어야 하고, 같은 fs_basis로
맞춰야 하고(§2.2), 평균 총자산이 양수여야 한다.

시장별로는 KOSDAQ 0.630 / KOSPI 0.544다. **KOSPI 쪽 커버리지가 낮은데 날짜당 종목 수는
KOSPI가 662개로 KOSDAQ 377개보다 많다** — 두 지표가 다른 것을 재기 때문이다(비율 대 절대수).

표본 시작이 2017년이라 **기간 검정이 4구간뿐이다** (§5.1).

---

## 7. 중복성

### A×B 교차 상관

| 상대 family | 평균 순위상관 | 유효일 |
|---|---:|---:|
| `px_mom_12_1` | +0.074 | 1,927 |
| `px_near_52w_high` | +0.073 | 1,927 |
| `px_idio_vol_60d` | −0.068 | 1,927 |
| `px_maxret_20d` | −0.040 | 1,927 |

**전부 0.08 미만이다.** Phase A 계열과 거의 독립이다.

### 확인하지 않은 중복

같은 `feat_fin_scan_daily` 마트가 다섯 family를 만든다.

| family | 산식 |
|---|---|
| **`fin_accruals_to_assets`** | (순이익 − 영업현금흐름) / 평균총자산 |
| [24_fin_asset_growth_yoy](24_fin_asset_growth_yoy.md) | 총자산 YoY |
| [25_fin_gross_profitability](25_fin_gross_profitability.md) | 매출총이익 / 평균총자산 |
| [26_fin_log_mcap](26_fin_log_mcap.md) | ln(시가총액) |
| [27_fin_value_z](27_fin_value_z.md) | 4개 밸류 지표 z-score 평균 |

**`fin_gross_profitability`와 분모가 같다**(`avg_assets`). 그리고 발생액과 자산성장은
경제적으로 얽혀 있다 — 자산이 늘면 대개 발생액도 는다.

**B×B 상관 산출물이 없다.** A×B 교차만 계산했다.

---

## 8. 한계와 확인 못 한 것

1. **반대 부호를 결론으로 쓸 수 없다.** 기간 0~1/4, 시간 placebo 전부 실패, offset 통과
   실패. 세 검사가 모두 불안정을 가리킨다.
2. **크기가 사실상 0이다** (§4.4). IC 0.018, 5분위 차이 0.1%p.
3. **표본이 2017년부터라 기간 검정이 4구간뿐이다** (§5.1, §6).
4. **순이익의 31.8%가 매핑 대체다** (§5.6). 산식의 핵심 입력인데 3분의 1이 간접 경로다.
5. **총자산의 10.1%가 사후 정정됐다** (§5.6).
6. **평균 74일 된 정보를 쓴다** (§2.4). 20번에 한 번은 반년 가까이 묵었다.
7. **같은 마트 다섯 family 간 중복이 미확인이다** (§7).
8. **업종 중립화가 없다.** 업종마다 발생액 수준이 크게 다르다. 은행·바이오·조선이 한
   KOSPI 풀에서 비교된다 (`fin_scan.py`의 `CROSS_SECTION_WITH_INDUSTRY` 주석).
   업종 중립 변형(`feat_fin_scan_daily_ind`)이 만들어져 있지만 **PIT가 아니라 진단용이고
   이번 검증에서 제외됐다** (`13_..._plan.md` §4.2).
9. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
10. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나

**T2 14-feature bundle에 안 들어갔다.** discovery 0개라 후보에서 빠졌다.

같은 마트의 `fin_gross_profitability`, `fin_log_mcap`, `fin_value_z` 셋은 들어갔다.

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# BH 는 통과했는데 부호가 반대라 discovery 가 아닌 cell 들
print(duckdb.sql(f"""
  select family, scan_type, h_start, h_end, ic_mean, expected_sign,
         expected_sign_pass, q_fdr_global_ab, primary_discovery_ab,
         valid_subperiods, sign_consistent_subperiods, period_sign_pass,
         p_temporal_nw, evidence_grade, failed_gates
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  where family='fin_accruals_to_assets' order by scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 원천 품질 | 같은 B run의 `core/quarterly_metric_quality.parquet` |
| 커버리지·vintage 나이 | 같은 B run의 `core/feature_coverage.parquet` |
| 산식 | `research/etl/features/fin_scan.py:283` |
| fs_basis 통일 설계 | 같은 파일 모듈 docstring |
| 알려진 문제 | `01_feature_candidate/10_known_issues.md` I1 |
| 한국 복제 연구 대조 | `01_feature_candidate/11_feature_taxonomy.md` §4 |
