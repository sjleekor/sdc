# 31. `own_amendment_ratio` — 지분 공시 정정 비율

- 작성일: 2026-08-29
- family: `own_amendment_ratio` · primary feature: **`own_amendment_ratio_1y`** · domain: ownership
- **Phase B** · fdr_family `ownership` · 기대 부호 `−` · 관측 부호 `−`
- 등급 **A** · **discovery 4/4 · screen-pass 4/4 · 실패한 게이트 없음**
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**최근 1년 지분 공시 중 정정 비율이 높은 회사가 이후 60~120일 동안 부진했다**
(cum 0→120 IC −0.0358, **ICIR −1.417**).

**Phase B에서 가장 깨끗한 셋 중 하나다.** 4개 cell 전부 discovery이면서 전부 screen-pass,
`failed_gates` 비어 있음, 기간 **5/5**, 시간 placebo **전부 최솟값 0.0099로 통과**,
거래가능 유지율 1.01~1.05, 원천 품질 경고 없음.

[18_ev_amendment_ratio.md](18_ev_amendment_ratio.md)의 **지분 공시 한정 버전**이다. 저쪽은
전체 공시의 정정 비율, 이쪽은 **임원·주요주주 소유상황보고서와 5% 대량보유보고서만**의
정정 비율이다.

**한 가지가 특이하다. 5분위 수익률 차이가 계산되지 않았다(NaN).** AB의 유효 147개 cell 중
**이 family의 4개만 그렇다** (§4.3).

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/filing_activity.py:245
own_amendments_250d / NULLIF(own_filings_250d, 0) AS own_amendment_ratio_1y
```

**최근 250거래일(약 1년) 지분 공시 중 정정공시의 비율**이다.

분자·분모의 정의가 [18](18_ev_amendment_ratio.md)과 다르다.

```sql
-- research/etl/features/filing_activity.py:157
COUNT(*) FILTER (WHERE is_insider OR is_major_holder)  AS ownership_filings
COUNT(*) FILTER (
    WHERE is_amendment AND (is_insider OR is_major_holder)
)                                                       AS ownership_amendments
```

**지분 관련 공시만 세고, 그중 정정만 다시 센다.**

### 2.2 어떤 공시를 "지분 공시"로 세는가

두 종류다. 문자열 상수로 고정돼 있다.

```python
# research/etl/features/filing_activity.py:69
#: 임원ㆍ주요주주 특정증권등 소유상황보고서 — the ``elestock`` event, as a receipt.
#: The separator is U+318D (ㆍ), not a middle dot, which is why this matches on a
#: substring rather than on equality: 139,697 receipts across 2,607 tickers.
INSIDER_MARKER = "주요주주특정증권등소유상황보고서"

#: 주식등의 대량보유상황보고서 — the 5% rule. 101,656 receipts (일반 + 약식).
MAJOR_HOLDER_MARKER = "주식등의대량보유상황보고서"
```

**주석에 실측 건수가 적혀 있다.** 임원·주요주주 13.9만 건(2,607종목), 5% 대량보유 10.2만
건이다.

구분자 문제도 기록돼 있다 — 보고서명의 `ㆍ`가 **가운뎃점이 아니라 U+318D**라서 완전 일치
대신 부분 문자열로 매칭한다.

### 2.3 정정 판정은 [18](18_ev_amendment_ratio.md)과 같다

```python
# research/etl/phase_b_quality.py:40
AMENDMENT_MARKERS = ("기재정정", "첨부정정", "첨부추가", "변경등록")
```

같은 마커를 쓴다. `is_amendment AND (is_insider OR is_major_holder)`로 교집합을 센다.

### 2.4 PIT·0 채우기·창 길이

[18_ev_amendment_ratio.md](18_ev_amendment_ratio.md) §2.3~§2.5와 동일하다.

- 창은 **250거래일**(`RATIO_WINDOW`) — 비율이 비율이 되려면 분모에 충분한 건수가 필요하다
- 접수일 D의 공시는 **D+1 거래일부터** 노출
- 공시가 없는 날을 0으로 채워 `ROWS` 창이 거래일을 세게 한다

정본 변형은 **`native_t`**, `formula_version: filing_v3`이다.

### 2.5 분모가 작다 — 이 family의 구조적 특징

전체 공시는 회사당 연 8건 안팎인데, **지분 공시는 그보다 훨씬 적다.**

그 결과 두 가지가 따라온다.

1. **분모가 0인 회사가 많다.** 커버리지가 0.754로 [18](18_ev_amendment_ratio.md)(0.894)보다
   14%p 낮고, 날짜당 종목도 879개로 160개 적다.
2. **값에 0이 많이 몰린다.** 지분 공시를 몇 건 냈는데 정정이 하나도 없으면 값이 정확히 0이다.
   **§4.3의 NaN 문제가 여기서 나올 가능성이 크다.**

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/filing_activity.py:245` |
| 지분 공시 분류 | `research/etl/features/filing_activity.py:157` |
| 공시 종류 상수 | `research/etl/features/filing_activity.py:69`, `:73` |
| 정정 마커 | `research/etl/phase_b_quality.py:40` |
| 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**지배구조 품질의 대리변수라는 가설이다.**

[18_ev_amendment_ratio.md](18_ev_amendment_ratio.md) §3.1의 "공시 품질 = 경영 품질" 논리에
한 겹이 더해진다.

**지분 공시는 임원과 대주주 본인이 내는 공시다.** 회사 실무부서가 처리하는 일반 공시와
달리, 여기서 정정이 잦다는 건 **지배구조 쪽 관리가 허술하거나 지분 변동이 복잡하다**는
신호로 볼 수 있다.

경영권 분쟁, 담보 제공, 반복적인 지분 조정이 있으면 정정이 늘어난다.

### 3.2 기대 부호

`−`. 지분 공시 정정 비율이 높을수록 이후 초과수익률 순위가 낮다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_expansion_20260827.yaml
- family: own_amendment_ratio
  expected_sign: "-"
  features: [{column: own_amendment_ratio_1y, role: primary}]
  primary_horizon_set: [60, 120]
  exploratory_horizon_set: [20, 40]
  include_bucket_primary: true
```

250일 창의 느린 지표이므로 긴 horizon에 걸었다. [18](18_ev_amendment_ratio.md)과 동일하다.
cell은 4개다.

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 60~120일 | **4개 cell 전부 discovery + screen-pass** |
| 부호 | `−` | **`−` (일치)** |

### 3.4 사전등록 시점

2026-08-27 확장 등록분이다 (`outcome_blind: true`).

분류 좌표는 **C3(수급·소유·내부자)** × T0(수준) × U다.
`11_feature_taxonomy.md` §2.1이 C3의 빈 칸으로 지목한 **「내부자·최대주주」**를 메우는
항목이다.

### 3.5 근거 문헌

없다. 접수 이력에서 만든 신규 지표다.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 | AB q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | −0.0273 | −0.996 | −9.06 | **—** | ~0 | **discovery + screen-pass (A)** |
| cum | 0→120 | **−0.0358** | **−1.417** | **−10.28** | **—** | ~0 | **discovery + screen-pass (A)** |
| bucket | 40→60 | −0.0176 | −0.597 | −8.55 | **—** | ~0 | **discovery + screen-pass (A)** |
| bucket | 60→120 | −0.0257 | −0.921 | −8.02 | **—** | ~0 | **discovery + screen-pass (A)** |

**4개 전부 통과했고 `failed_gates`가 비어 있다.**

### 4.2 ICIR −1.42는 Phase B 최고 수준이다

| family | 최대 \|ICIR\| |
|---|---:|
| `ev_amendment_ratio` | 1.357 |
| **`own_amendment_ratio`** | **1.417** |
| `fin_value_z` | 1.153 |
| `ev_payout_yield` | 1.212 |
| `fin_log_mcap` | 1.105 |

**|IC|는 0.036으로 작은데 ICIR은 가장 크다.** 일별 IC의 흔들림이 그만큼 작다는 뜻이다.

t(NW)도 −10.28로 Phase B에서 가장 크다.

### 4.3 5분위 수익률 차이가 계산되지 않았다 — 147개 중 이 넷뿐

**AB의 유효 147개 cell 중 `q5_spread`가 NaN인 건 이 family의 4개가 전부다.**

산출물에는 이유가 기록돼 있지 않다. 산식을 보면 가장 그럴듯한 설명은 **동점(tie)**이다.

```python
# research/etl/metrics.py:230
rank = _rankdata(feature) / feature.size
top = realized[rank >= 1 - 1/5]
bottom = realized[rank <= 1/5]
if top.size and bottom.size:
    rows.append(...)      # ← 둘 중 하나라도 비면 그날은 통째로 버린다
```

`_rankdata`는 동점에 **평균 순위**를 준다. §2.5에서 본 대로 이 피처는 **정확히 0인 값이
대량으로 몰린다.** 회사의 80% 이상이 0이면 그 동점 블록의 평균 순위가 0.2를 넘어
`bottom`이 **빈 배열**이 된다. 그러면 그날 횡단면이 통째로 버려지고, 모든 날이 버려지면
결과가 NaN이 된다.

**확인된 사실이 아니라 산식에서 유도한 추론이다.** 값 분포를 직접 보지 않았다.

**실무적으로 중요한 함의가 있다.**

- **IC는 유효하다.** Spearman은 동점을 평균 순위로 처리하므로 계산이 된다.
- **5분위 전략은 성립하지 않는다.** 하위 20%를 뽑을 수 없다.
- 즉 **"신호는 있는데 5분위 롱숏으로는 구현할 수 없는 피처"**다.

**등급 A와 이 사실을 함께 읽어야 한다.** 통계적 근거는 Phase B 최상위인데
경제적 크기를 측정할 수 없다.

### 4.4 신호의 모양

| 관찰 | 값 |
|---|---|
| `peak_cell` | `cum 0→120` |
| `peak_ic_mean` | −0.0358 |
| 누적 \|IC\| 추이 | 60일 0.027 → 120일 0.036 (증가) |
| 구간 \|IC\| 추이 | 40~60일 0.018 → 60~120일 0.026 (증가) |

관측 범위 끝에서 최대다. [18](18_ev_amendment_ratio.md)과 같은 모양이다.

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 4개 cell 전부 5/5

`valid_subperiods` = **5**, `sign_consistent_subperiods` = **5**, `period_sign_pass` = True.

**구간이 5개다.** 표본이 2015-01-05부터라 `2014_2016`이 잡힌다.

### 5.2 시간 placebo — 전부 통과, 최솟값

| cell | `p_temporal_nw` | 판정 |
|---|---:|---|
| cum 0→60 | **0.0099** | **통과** |
| cum 0→120 | **0.0099** | **통과** |
| bucket 60→120 | **0.0099** | **통과** |
| bucket 40→60 | — | 대상 아님 (NW lag 19) |

**세 cell 전부 최솟값이다.** Phase B에서 이 검사를 받고 전부 최솟값으로 통과한 family는
`fin_log_mcap`, `ev_amendment_ratio`, 이 family 셋뿐이다.

### 5.3 비중첩 offset — `complete` 통과

세 cell 모두 `offset_status = complete`, `nonoverlap_robustness_pass = True`다.

### 5.4 거래 가능한 종목만 남겨도

| cell | `tradable_retention` |
|---|---:|
| cum 0→60 | 1.010 |
| cum 0→120 | 1.036 |
| bucket 40→60 | 1.014 |
| bucket 60→120 | 1.049 |

**네 cell 전부 1.01~1.05다.** 유동성 필터가 신호를 거의 바꾸지 않는다.

### 5.5 생존편향

`available_direction_pass` = **True** (4개 cell 모두).

### 5.6 source quality — 경고 없음

`source_quality_status` = **`not_applicable`**, `source_quality_grade_cap` = `None`.

**접수 이력은 사후 수정되지 않는 사실 기록이다.** 그래서 등급이 **A**다.

같은 원천을 쓰는 filing-activity 계열 다섯 family가 전부 `not_applicable`이다.
N6 계열(`hc_*`, `own_major_stake_*`)이 `final_vintage` 경고로 상한 B에 걸린 것과 갈린다.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2015-01-05 ~ 2025-02-05** |
| 유효 거래일 | **2,478일** |
| 날짜당 평균 종목 수 | **877~880개** |
| `coverage_ratio` | **0.754** |
| 관측 행 수 | 5,187,210 |

시장별로는 KOSDAQ 0.790 / KOSPI 0.700이다.

[18_ev_amendment_ratio.md](18_ev_amendment_ratio.md)(0.894, 1,040종목)보다 낮다.
§2.5에서 본 대로 **지분 공시를 한 건도 안 낸 회사는 분모가 0이라 값이 없기 때문이다.**

---

## 7. 중복성

### A×B 교차 상관

| 상대 family | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `px_idio_vol_60d` | +0.094 | 2,334 | **+0.02 ~ +0.15** |
| `px_near_52w_high` | −0.066 | 2,478 | −0.15 ~ +0.01 |
| `px_maxret_20d` | +0.058 | 2,478 | −0.08 ~ +0.15 |
| `px_amihud_20d` | −0.049 | 2,478 | −0.15 ~ +0.001 |

**전부 0.10 미만이다.** Phase A 계열과 거의 독립이다.

`ev_amendment_ratio`가 `px_idio_vol_60d`와 +0.140, `px_near_52w_high`와 −0.137이었던 것보다
낮다. **지분 공시로 좁히면 가격 계열과 덜 얽힌다.**

### 확인하지 않은 중복 — 가장 중요한 공백

**[18_ev_amendment_ratio.md](18_ev_amendment_ratio.md)와 부분집합 관계다.**

```
ev_amendment_ratio  = 전체 정정 / 전체 공시
own_amendment_ratio = 지분 정정 / 지분 공시
                      ↑ 분자·분모가 모두 위쪽의 부분집합
```

**분자도 분모도 겹친다.** 두 값이 얼마나 다른지는 지분 공시가 전체에서 차지하는 비중과
정정 성향의 차이에 달려 있는데, **B×B 상관 산출물이 없다.**

두 family의 결과도 비슷하다.

| | `ev_amendment_ratio` | `own_amendment_ratio` |
|---|---:|---:|
| IC (cum 0→120) | −0.0568 | −0.0358 |
| ICIR | −1.357 | −1.417 |
| discovery / screen-pass | 4/4 · 4/4 | 4/4 · 4/4 |
| 등급 | A | A |
| 시간 placebo | 0.0099 | 0.0099 |

**두 발견을 독립된 것으로 세면 안 된다.** 결합 BH에서 각각 4개씩 8개 가설로 세어졌다.

같은 마트의 다른 셋([19](19_ev_filing_activity.md), [32](32_own_insider_filing_activity.md),
[33](33_own_major_filing_activity.md))과의 관계도 재지 않았다.

---

## 8. 한계와 확인 못 한 것

1. **5분위 수익률 차이를 계산하지 못했다** (§4.3). 147개 유효 cell 중 이 넷뿐이다.
   **경제적 크기를 알 수 없고, 5분위 롱숏으로 구현할 수도 없다.** 원인 추정은 했지만
   값 분포를 직접 확인하지 않았다.
2. **`ev_amendment_ratio`와 부분집합 관계다** (§7). B×B 상관이 없다. 두 family가 사실상
   같은 발견일 수 있다.
3. **정정의 종류를 구분하지 않는다.** `기재정정`·`첨부정정`·`첨부추가`·`변경등록`을
   한 묶음으로 센다.
4. **누가 정정했는지 모른다.** 임원 공시 정정과 5% 대량보유 정정을 합쳐 센다. 둘은 성격이
   다를 수 있다.
5. **분모가 0인 회사가 25%다** (§6). 지분 공시를 안 내는 회사는 이 신호로 판단할 수 없다.
6. **120일 너머를 안 봤다** (§4.4).
7. **업종 중립화가 없다.**
8. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
9. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T2

**T2 14-feature bundle에 들어갔다** (`own_amendment_ratio_1y`).

| horizon | Rank IC Δ | 비용 반영 spread Δ |
|---|---:|---:|
| 5 | +0.0031 | +0.0017 |
| 20 | +0.0011 | +0.0030 |
| 60 | +0.0003 | +0.0080 |

세 horizon 전부 개선됐다(`improved_all_horizons`). **14개를 함께 넣은 결과라 개별 기여도는
측정하지 않았다.**

**§7이 여기서 걸린다.** 같은 묶음에 `ev_amendment_ratio_1y`가 함께 들어 있다. 부분집합
관계인 두 값을 한 묶음에 넣었을 때의 증분은 하나만 넣었을 때보다 크지 않을 가능성이 높다.

**§4.3도 함께 봐야 한다.** 5분위 롱숏으로 구현되지 않는 피처가 모델 입력으로는 작동할 수
있다 — 모델은 5분위가 아니라 전체 순위를 쓰기 때문이다. **그 차이가 이 family를 모델
입력으로 쓸 근거이기도 하다.**

**최종 h60 holdout은 아직 열지 않았다.**

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# q5_spread 가 NULL 인 유효 cell 은 이 family 뿐이다
print(duckdb.sql(f"""
  select family, feature, scan_type, h_start, h_end, ic_mean, icir, t_nw,
         q5_spread_raw, n_obs_mean, evidence_grade
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  where status='valid' and q5_spread_raw is null
  order by scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 커버리지 | 같은 B run의 `core/feature_coverage.parquet` |
| 산식 | `research/etl/features/filing_activity.py:245` |
| 지분 공시 상수·실측 건수 | `research/etl/features/filing_activity.py:69` |
| 5분위 계산 규칙 | `research/etl/metrics.py:200` |
| C3 빈 칸 지목 | `01_feature_candidate/11_feature_taxonomy.md` §2.1 |
| T2 결과 | `docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json` |
