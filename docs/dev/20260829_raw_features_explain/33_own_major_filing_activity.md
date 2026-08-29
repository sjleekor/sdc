# 33. `own_major_filing_activity` — 5% 대량보유 공시 건수

- 작성일: 2026-08-29
- family: `own_major_filing_activity` · primary feature: **`own_major_filing_60d`** · domain: ownership
- **Phase B** · fdr_family `ownership` · **기대 부호 없음(양방향)** · 관측 부호 `−`
- 등급 **A** · **discovery 4/4 · screen-pass 4/4 · 실패한 게이트 없음**
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**최근 60일 5% 대량보유 공시가 많았던 회사가 이후 부진했다** (cum 0→60 IC −0.0428,
ICIR −1.031, **5분위 수익률 차이 −4.24%p**).

**Phase B에서 가장 깨끗한 축이면서 커버리지가 완벽하다.**

| 항목 | 값 |
|---|---|
| discovery / screen-pass | **4/4 · 4/4**, `failed_gates` 비어 있음 |
| 등급 | **A 4개** |
| 기간 일관성 | **5구간 전부 5/5** |
| 시간 placebo | **통과** (p = 0.0099, 최솟값) |
| **`coverage_ratio`** | **1.00000** |
| 날짜당 종목 수 | **1,107개** — ownership 계열 최다 |

**커버리지 100%의 이유가 설계에 있다.** [32](32_own_insider_filing_activity.md)가 비율(burst)
형태라 분모 0인 회사가 4분의 3이었던 것과 달리, 이 family는 **건수(count) 그 자체**를 쓴다.
공시를 한 건도 안 낸 회사는 값이 NULL이 아니라 **0**이다.

**같은 마트, 같은 설계 철학인데 형태 하나가 커버리지 25%와 100%를 갈랐다.**

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의 — 비율이 아니라 건수다

```sql
-- research/etl/features/filing_activity.py:126
SUM(major_filings) OVER (
    PARTITION BY ticker, market ORDER BY trade_date
    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
) AS own_major_filing_60d
```

**최근 60거래일 동안 나온 5% 대량보유보고서 건수**다. 그대로 쓴다.

**burst 비율로 나누지 않는다.** 코드를 보면 명확하다.

```python
# research/etl/features/filing_activity.py:130
for prefix, column in (
    ("ev_filing",          f"ev_filing_count_{window}d"),
    ("own_insider_filing", f"own_insider_filing_{window}d"),
):
    burst_columns.append(...)      # ← own_major 는 이 루프에 없다
```

**`burst_columns` 루프에 `own_major`가 없다.** 의도적으로 건수만 만들었다.

### 2.2 그 선택이 커버리지를 갈랐다

| family | primary 형태 | 분모 | 커버리지 | 날짜당 종목 |
|---|---|---|---:|---:|
| `ev_filing_activity` | burst 비율 | 250일 중앙값 | 0.821 | 999 |
| `own_insider_filing_activity` | burst 비율 | 250일 중앙값 | **0.253** | **312** |
| **`own_major_filing_activity`** | **건수** | **없음** | **1.00000** | **1,107** |

비율 형태는 `NULLIF(중앙값, 0)` 때문에 **평소에 공시를 안 내는 회사가 통째로 빠진다**
([32](32_own_insider_filing_activity.md) §2.3).

건수 형태는 그런 조건이 없다. 공시가 없으면 0이고, 0도 유효한 값이다.

**대신 규모 편향이 들어온다.** 큰 회사에 대량보유자가 많으면 공시도 많다. 실제로 §7에서
`px_amihud_20d`와 −0.226으로 얽힌다.

**비율은 커버리지를 잃고 규모를 상쇄하고, 건수는 커버리지를 얻고 규모를 안고 간다.**
이 family는 후자를 택했다.

### 2.3 어떤 공시를 세는가

```python
# research/etl/features/filing_activity.py:73
#: 주식등의 대량보유상황보고서 — the 5% rule. 101,656 receipts (일반 + 약식).
MAJOR_HOLDER_MARKER = "주식등의대량보유상황보고서"
```

**지분 5% 이상을 보유하게 되거나 1% 이상 변동하면 내야 하는 공시**다. 원천에
**101,656건**이 있고 일반보고서와 약식보고서를 모두 센다.

### 2.4 0 채우기가 여기서 결정적이다

```sql
-- research/etl/features/filing_activity.py:196
-- Every universe row, with zeros on days nothing was filed. Without
-- the zero rows a ROWS window would count filing days rather than
-- sessions, and a quiet company's window would silently stretch
-- across years.
```

다른 family에서는 창 길이를 지키려는 처리였는데, **이 family에서는 커버리지 100%의
직접적 근거이기도 하다.** 공시가 없는 날이 0으로 채워지므로 모든 (거래일, 종목)에 값이 있다.

### 2.5 PIT와 창

- 접수일 D의 공시는 **D+1 거래일부터** 노출
- 창은 **60거래일**, secondary로 120거래일이 등록돼 있다
- `WINDOWS = (60, 120)`은 사전등록된 값이라 결과를 보고 고를 수 없다
  ([19_ev_filing_activity.md](19_ev_filing_activity.md) §2.4)
- 정본 변형은 **`native_t`**, `formula_version: filing_v3`

**secondary(`own_major_filing_120d`)는 이번 run에 없다.**

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/filing_activity.py:126` |
| burst 루프 (여기 없음) | `research/etl/features/filing_activity.py:130` |
| 공시 종류 상수 | `research/etl/features/filing_activity.py:73` |
| 0 채우기 | `research/etl/features/filing_activity.py:196` |
| 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |

---

## 3. 왜 방향을 열어 뒀나

### 3.1 대량보유 공시는 좋은 소식일 수도 나쁜 소식일 수도 있다

```yaml
- family: own_major_filing_activity
  expected_sign: null       # ← 방향을 걸지 않았다
```

| 가설 | 메커니즘 | 예측 부호 |
|---|---|---|
| **행동주의 매수** | 큰손이 지분을 모은다 = 저평가 신호 | `+` |
| **경영권 경쟁** | 지분 경쟁이 붙으면 주가가 오른다 | `+` |
| **대주주 이탈** | 5% 보유자가 지분을 줄인다 | `−` |
| **지분 불안정** | 공시가 잦다 = 지배구조가 흔들린다 | `−` |

**§2.1의 건수는 이 넷을 구분하지 못한다.** 매수 공시도 1건, 매도 공시도 1건이다.

[32_own_insider_filing_activity.md](32_own_insider_filing_activity.md) §3.1과 같은 한계인데,
**이 family에서는 그럼에도 신호가 나왔다.** 방향이 `−`로 뚜렷하다.

**해석은 §4.2에서 다시 본다.**

### 3.2 사전등록 horizon

```yaml
primary_horizon_set: [20, 60]
exploratory_horizon_set: [1, 5, 10, 40, 120]
include_bucket_primary: true
```

filing-activity 계열 공통이다. 60일 창의 지표라 반응이 빠를 것으로 봤다. cell은 4개다.

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 20~60일 | **4개 cell 전부 discovery + screen-pass** |
| 부호 | 없음 | **`−` (네 cell 전부)** |

### 3.3 사전등록 시점

2026-08-27 확장 등록분이다 (`outcome_blind: true`).

분류 좌표는 **C3(수급·소유·내부자)** × T0(수준) × U다.
`11_feature_taxonomy.md` §2.1이 지목한 C3의 빈 칸 **「내부자·최대주주」**를 메우는 항목이고,
**그중 유일하게 A등급을 받았다.**

### 3.4 근거 문헌

없다. 접수 이력에서 만든 신규 지표다.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

양방향 family이므로 `q5_spread_aligned`가 원값과 같다. **음수면 상위 20%(공시 많은 쪽)가
덜 올랐다는 뜻이다.**

| scan | horizon | Rank IC | ICIR | t(NW) | **5분위 차이** | AB q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | −0.0273 | −0.676 | −9.60 | −1.73%p | ~0 | **discovery + screen-pass (A)** |
| cum | 0→60 | **−0.0428** | **−1.031** | −8.67 | **−4.24%p** | ~0 | **discovery + screen-pass (A)** |
| bucket | 10→20 | −0.0182 | −0.468 | −9.28 | −0.84%p | ~0 | **discovery + screen-pass (A)** |
| bucket | 40→60 | −0.0228 | −0.607 | −8.60 | −2.56%p | ~0 | **discovery + screen-pass (A)** |

**4개 전부 통과했고 `failed_gates`가 비어 있다.**

t(NW)가 −8.6 ~ −9.6으로 크다. |ICIR|도 1.03까지 간다.

### 4.2 IC와 5분위 차이가 같은 방향이고 크기도 상당하다

**ownership 계열에서 유일하게 경제적 크기가 뚜렷한 family다.**

| family | \|IC\| | 5분위 차이 |
|---|---:|---:|
| **`own_major_filing_activity`** | **0.043** | **−4.24%p** |
| `own_major_stake_change` | 0.042 | (§34 참조) |
| `own_amendment_ratio` | 0.036 | **계산 불가(NaN)** |
| `own_insider_filing_activity` | 0.003 | +0.07%p |

**60거래일에 −4.24%p**다. 같은 60일 horizon에서 `px_maxret_20d`(+1.79%p),
`px_idio_vol_60d`(+2.99%p)보다 크다.

**부호를 뒤집어 읽으면 이렇다.** 5% 대량보유 공시가 적은 회사가 많은 회사보다 60거래일
동안 시장 대비 4.24%p 더 올랐다.

§3.1의 네 가설 중 **「대주주 이탈」과 「지분 불안정」 쪽이 우세했다**는 뜻이지만,
**건수만으로는 어느 쪽인지 구분할 수 없다** (§8).

### 4.3 신호의 모양

| 관찰 | 값 |
|---|---|
| `peak_cell` | `cum 0→60` |
| `peak_ic_mean` | −0.0428 |
| 누적 \|IC\| 추이 | 20일 0.027 → 60일 0.043 (증가) |
| 구간 \|IC\| 추이 | 10~20일 0.018 → 40~60일 0.023 (증가) |

**관측 범위 끝에서 최대다.** 120일은 exploratory로 내려 확인하지 않았다.

### 4.4 cell마다 유효일이 조금씩 다르다

| cell | `n_dates` |
|---|---:|
| cum 0→20 | 2,527 |
| bucket 10→20 | 2,525 |
| bucket 40→60 | 2,518 |
| cum 0→60 | 2,514 |

**horizon이 길수록 유효일이 줄어든다.** 라벨을 만들려면 앞으로 그만큼의 거래일이 필요하기
때문이다. 다른 family에서도 마찬가지인데, 커버리지가 100%라 이 차이가 그대로 드러난다.

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 4개 cell 전부 5/5

`valid_subperiods` = **5**, `sign_consistent_subperiods` = **5**, `period_sign_pass` = True.

양방향 family이므로 관측 부호(`−`) 기준이다. **다섯 구간 전부에서 같은 방향이었다.**

### 5.2 시간 placebo — 통과, 최솟값

| cell | `p_temporal_nw` | 판정 |
|---|---:|---|
| cum 0→60 | **0.0099** | **통과** |
| 나머지 셋 | — | 대상 아님 (NW lag < 59) |

**Phase B에서 이 검사를 받고 최솟값으로 통과한 네 번째 family다**
(`fin_log_mcap`, `ev_amendment_ratio`, `own_amendment_ratio`에 이어).

### 5.3 비중첩 offset — `complete` 통과

`cum 0→60`이 `offset_status = complete`, `nonoverlap_robustness_pass = True`다.

### 5.4 거래 가능한 종목만 남기면 — 10~17% 감소

| cell | `tradable_retention` | `tradable_pass` |
|---|---:|---|
| cum 0→20 | 0.847 | True |
| cum 0→60 | 0.877 | True |
| bucket 10→20 | **0.828** | True |
| bucket 40→60 | 0.901 | True |

**네 cell 전부 1 미만이다.** ownership 계열에서 유일하다.

§2.2에서 본 **건수 형태의 규모 편향**과 방향이 맞는 정황이다. 유동성 필터를 걸면 규모가
큰 종목만 남고, 규모와 얽힌 신호가 그만큼 약해진다.

게이트 기준 0.50은 넉넉히 넘는다.

### 5.5 생존편향

`available_direction_pass` = **True** (4개 cell 모두).

### 5.6 source quality — 경고 없음

`source_quality_status` = **`not_applicable`**. 접수 이력은 사후 수정되지 않는다.
그래서 등급이 **A**다.

---

## 6. 표본과 커버리지 — 완벽하다

| 항목 | 값 |
|---|---|
| 유효 표본 | **2014-06-02 ~ 2025-02-05** |
| 유효 거래일 | **2,514 ~ 2,527일** |
| 날짜당 평균 종목 수 | **1,107개** |
| **`coverage_ratio`** | **1.00000** |
| 관측 행 수 | 6,879,703 |

**KOSPI·KOSDAQ 모두 1.00000이다.** 결측이 하나도 없다.

`family_summary`의 `effective_start`는 **2007-08-01**로 적혀 있다 — 원천 접수 이력이
거기서 시작한다는 뜻이다. 실제 scan 표본은 공통 규칙에 따라 2014-06-02부터다.

**커버리지 1.0인 Phase B family는 이것 하나다.** `mcap_krx_log`가 0.99862로 다음이다.

**커버리지가 완벽한 게 항상 좋은 건 아니다.** [28_mcap_krx_log.md](28_mcap_krx_log.md)
§5.4에서 본 대로 커버리지가 높으면 실행 불가능한 종목까지 포함하게 된다. 이 family도
§5.4에서 유지율이 1 미만이다.

---

## 7. 중복성

**A×B 상관 표에 이 family의 행이 출력되지 않았다.** `family_summary`의
`top_rank_correlation_pair`는 `px_amihud_20d`, 값은 **−0.226**이다.

**규모와 얽힌다는 뜻이다.** `px_amihud_20d`가 사실상 규모 지표라는 점을
([07_px_amihud_20d.md](07_px_amihud_20d.md) §7) 생각하면, §2.2에서 예상한 건수 형태의
규모 편향이 숫자로 확인된 셈이다.

전체 상관 표에서 이 family가 걸린 값들은 이렇다.

| 상대 family | 평균 순위상관 |
|---|---:|
| `px_amihud_20d` | **−0.226** |
| `px_idio_vol_60d` | +0.155 |
| `px_maxret_20d` | +0.103 |
| `flow_inst_netbuy_to_volume` | −0.035 |

**`px_idio_vol_60d`와 +0.155도 방향이 안정적이다** (범위 +0.06 ~ +0.26). 대량보유 공시가
잦은 회사는 고유변동성이 크다.

### 확인하지 않은 중복

1. **[32_own_insider_filing_activity.md](32_own_insider_filing_activity.md)와 형제다.**
   하나는 임원 공시, 하나는 5% 대량보유 공시를 센다. 같은 마트에서 나온다.
2. **[31_own_amendment_ratio.md](31_own_amendment_ratio.md)의 분모에 포함된다.**
   저쪽의 `ownership_filings`가 임원 + 대량보유이므로 이 family의 분자를 포함한다.
3. **[19_ev_filing_activity.md](19_ev_filing_activity.md)의 부분집합이다.** 전체 공시 건수에
   대량보유 공시가 들어 있다.
4. **[34_own_major_stake_change.md](34_own_major_stake_change.md)·[35](35_own_major_stake_level.md)와
   원천이 겹친다.** 저쪽은 지분율 자체, 이쪽은 공시 건수다.

**B×B 상관 산출물이 없다.** ownership 계열 다섯을 독립된 발견으로 세면 안 된다.

---

## 8. 한계와 확인 못 한 것

1. **매수와 매도를 구분하지 못한다** (§3.1, §4.2). 신호가 `−`로 나왔지만 그게
   「대주주 이탈」인지 「지분 불안정」인지 알 수 없다. **건수 기반 설계의 근본 한계다.**
2. **규모 편향이 있다** (§2.2, §5.4, §7). `px_amihud_20d`와 −0.226이고 유지율이 0.83~0.90이다.
   규모를 통제한 증분 IC를 재지 않았다.
3. **ownership 계열 다섯 간 중복이 미확인이다** (§7). B×B 상관이 없다.
4. **secondary(120일 창)를 안 돌렸다** (§2.5).
5. **60일 너머를 안 봤다** (§4.3). |IC|가 관측 범위 끝에서 최대다.
6. **공시 종류를 구분하지 않는다.** 일반보고서와 약식보고서를 같은 1건으로 센다.
7. **업종 중립화가 없다.**
8. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
9. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T2

**T2 14-feature bundle에 들어갔다** (`own_major_filing_60d`).

| horizon | Rank IC Δ | 비용 반영 spread Δ |
|---|---:|---:|
| 5 | +0.0031 | +0.0017 |
| 20 | +0.0011 | +0.0030 |
| 60 | +0.0003 | +0.0080 |

세 horizon 전부 개선됐다(`improved_all_horizons`). **14개를 함께 넣은 결과라 개별 기여도는
측정하지 않았다.**

**§4.2를 생각하면 이 family의 몫이 클 가능성이 있다.** 14개 중 5분위 수익률 차이가 뚜렷한
몇 안 되는 축이고, 커버리지가 100%라 모든 종목에 값이 있다.

다만 §7의 규모 중복이 걸린다. 같은 묶음에 `fin_log_mcap`·`mcap_krx_log`가 들어 있다.

**최종 h60 holdout은 아직 열지 않았다.**

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
B=f"research/output/horizon_scan/phase=B/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T123313-4e0ae8b0"
# filing-activity 계열 다섯 family 의 커버리지를 나란히 본다
print(duckdb.sql(f"""
  select family, primary_feature, coverage_ratio, observations
  from '{B}/core/family_summary.parquet'
  where readiness_dependencies like '%feat_filing_activity%'
  order by coverage_ratio desc
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 커버리지 (1.00000) | 같은 B run의 `core/feature_coverage.parquet` |
| 산식 | `research/etl/features/filing_activity.py:126` |
| burst 루프 (여기 없음) | `research/etl/features/filing_activity.py:130` |
| 공시 종류 상수 | `research/etl/features/filing_activity.py:73` |
| C3 빈 칸 지목 | `01_feature_candidate/11_feature_taxonomy.md` §2.1 |
| T2 결과 | `docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json` |
