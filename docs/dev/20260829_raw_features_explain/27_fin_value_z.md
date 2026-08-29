# 27. `fin_value_z` — 밸류 종합 (B/M · E/P · CFO/P · S/P)

- 작성일: 2026-08-29
- family: `fin_value_z` · primary feature: 동명 · domain: financial
- **Phase B** · fdr_family `financial` · 기대 부호 `+` · 관측 부호 `+`
- **discovery 4/4 · screen-pass 1/4** · 등급 **B 1개 / C 3개** · source quality `warn`
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**싼 회사가 이후 60~120일 동안 더 올랐다.** cum 0→120 IC **+0.1220**으로 **Phase B 18개 중
|IC|가 가장 크다.** 5분위 수익률 차이는 +4.52%p다.

**기간 안정성도 완벽하다** — 네 cell 전부 4/4 구간 일치. **그런데 세 cell이 시간 placebo에서
떨어졌다** (p = 0.198 ~ 0.366). 살아남은 건 짧은 구간 하나(`bucket 40→60`)뿐이고, 그것도
검사를 받지 않았기 때문이다.

**등급이 A가 아니라 B인 이유는 원천 품질이다.** 매출액의 **94.2%가 매핑 대체 경로**로
채워졌고 자본총계의 10.1%가 사후 정정됐다 (§5.6).

**35개 중 유일하게 네 개 지표를 합성한 피처다.** 그 합성 규칙에 과거 버그가 있었고
(발행 값의 29.2%가 규칙 위반), 지금은 고쳐졌다 (§2.4).

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 네 단계로 만든다

**1단계 — 네 개의 밸류 비율**

```sql
-- research/etl/features/fin_scan.py:268
CASE WHEN base_ok AND total_equity_selected > 0
     THEN total_equity_selected / market_cap_pit END       AS fin_book_to_market  -- B/M
CASE WHEN base_ok THEN controlling_net_income_selected / market_cap_pit
END                                                        AS fin_earnings_yield  -- E/P
CASE WHEN base_ok THEN operating_cash_flow_selected / market_cap_pit
END                                                        AS fin_cfo_yield       -- CFO/P
CASE WHEN base_ok THEN revenue_selected / market_cap_pit
END                                                        AS fin_sales_to_price   -- S/P
```

**네 개 모두 분모가 시가총액이다.** 값이 클수록 싸다.

**2단계 — 각각을 `(거래일, 시장)` 안에서 winsorize**

1분위·99분위에서 자른다.

**3단계 — winsorize한 값을 z-score**

```sql
-- research/etl/features/fin_scan.py:334
(w_bm - AVG(w_bm) OVER (PARTITION BY trade_date, market))
    / NULLIF(STDDEV_SAMP(w_bm) OVER (PARTITION BY trade_date, market), 0) AS z_bm
```

네 지표의 단위가 달라 그냥 더할 수 없다. 같은 횡단면 안에서 표준화해 대등하게 만든다.

**4단계 — 있는 것만 평균, 최소 2개 요구**

```sql
-- research/etl/features/fin_scan.py:357
CASE WHEN value_component_count >= 2 THEN
    (COALESCE(z_bm,0) + COALESCE(z_ep,0) + COALESCE(z_cfop,0) + COALESCE(z_sp,0))
    / value_component_count
END AS fin_value_z
```

**네 개가 다 있어야 하는 게 아니라 최소 두 개만 있으면 만든다.** 있는 것끼리 평균낸다.

### 2.2 왜 하나가 아니라 넷인가

밸류 지표마다 잡는 게 다르다.

| 지표 | 강점 | 약점 |
|---|---|---|
| **B/M** (자본/시총) | 가장 오래된 표준 | 무형자산 많은 회사에서 왜곡 |
| **E/P** (순이익/시총) | 직관적 | 적자 회사에서 음수, 일회성 항목에 민감 |
| **CFO/P** (영업현금흐름/시총) | 회계 조작에 강함 | 투자가 큰 시기에 왜곡 |
| **S/P** (매출/시총) | 적자 회사에도 적용 | 수익성을 무시 |

**하나에 의존하면 그 지표의 약점이 그대로 신호에 들어온다.** 넷을 평균내면 서로를 보완한다.

`02_feature_candidate.md` §1의 8번 항목이 이 설계를 그대로 적었다.

> 가치 composite | `fin_value_z` (B/M·E/P·CFO/P·S/P) | V1~V4 | 쌀수록 `+` | A | R1

### 2.3 winsorize를 z-score보다 먼저 한다

순서가 중요하다. **극단값을 자른 뒤에 표준화한다.**

반대로 하면 극단값이 표준편차를 부풀려 나머지 값들이 전부 0 근처로 눌린다. 밸류 지표는
분모가 시가총액이라 초소형주에서 극단값이 잘 생긴다.

### 2.4 v1에 심각한 버그가 있었다 — NULL 가드

```sql
-- research/etl/features/fin_scan.py:288 주석
-- Each clip is guarded by ``WHEN <ratio> IS NULL THEN NULL``: DuckDB's
-- GREATEST/LEAST *skip* NULL arguments, so a bare
-- ``LEAST(GREATEST(NULL, p01), p99)`` returns p01. Without the guard a
-- company with no financials at all is silently imputed to the market's
-- 1st percentile on every component, counted as 4 valid components, and
-- pinned to the "most expensive" end of the cross-section (fin_v1
-- behaviour: 29.2% of emitted values broke the >= 2 component rule ...)
```

**DuckDB의 `GREATEST`/`LEAST`가 NULL 인자를 건너뛴다.** 그래서 가드 없이 winsorize하면
재무제표가 **아예 없는 회사**가 시장 1분위 값으로 채워진다.

결과가 셋이었다.

1. 값이 없는 회사가 **네 개 요소 전부 유효**로 세어졌다
2. `value_component_count >= 2` 규칙이 무력화됐다 — **발행 값의 29.2%가 이 규칙을 어겼다**
3. 그 회사들이 **횡단면에서 "가장 비싼 쪽"에 고정**됐다

**지금은 고쳐졌다** (`formula_version: fin_v4`). 다만 **v1 시절 결과와 지금 결과는 다른
숫자다** (`10_known_issues.md` I1).

### 2.5 PIT와 vintage 나이

`value_available_from` 기준 interval join이다. 정본 변형은 **`native_t`**다.

| 시장 | 평균 나이 | 95분위 나이 |
|---|---:|---:|
| KOSDAQ | 74.9일 | 179일 |
| KOSPI | 73.6일 | 159일 |

평균 74일 된 정보다. `fin_gross_profitability`(105일 / 706일)보다는 낫다.

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 네 비율 | `research/etl/features/fin_scan.py:268` |
| winsorize 가드 | `research/etl/features/fin_scan.py:288` |
| z-score | `research/etl/features/fin_scan.py:334` |
| 합성 | `research/etl/features/fin_scan.py:357` |
| 알려진 문제 | `01_feature_candidate/10_known_issues.md` I1 |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:430` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**밸류 프리미엄이다.** 가장 널리 검증된 이례현상 중 하나다.

Fama & French (1992)의 HML 요인이 이 축이다. 설명은 두 갈래로 갈린다.

- **위험 프리미엄.** 싼 회사는 재무적으로 취약해서 그 위험의 대가를 받는다.
- **과잉반응.** 시장이 나쁜 소식에 과도하게 반응해 싸졌고, 시간이 지나면 되돌아온다.

**어느 쪽이든 부호는 `+`다.**

### 3.2 기대 부호

`+`. 밸류 z-score가 높을수록(쌀수록) 이후 초과수익률 순위가 높다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_config.yaml:436
primary_horizon_set: [60, 120]
exploratory_horizon_set: [20, 40]
include_bucket_primary: true
```

분기 재무 기반의 느린 지표이므로 긴 horizon에 걸었다. cell은 4개다.

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 60~120일 | **4개 cell 전부 discovery, screen-pass 1개** |
| 부호 | `+` | **`+` (일치)** |

### 3.4 한국 시장 단서 — 예고가 맞았다

`11_feature_taxonomy.md` §4의 한국 복제 연구 대조가 이 family를 명시적으로 예측했다.

> 수익성 5.0%(엄격 기준 0.0%), 투자 24.1% 복제율이 `fin_gross_profitability`와
> `fin_asset_growth_yoy`의 무신호를, **밸류 69.2%**·거래마찰 48.1%가 **`fin_value_z`**와
> px 계열 A등급을 **예측한다.**

**Han, Lee & Kang (2020)에서 밸류 카테고리 복제율이 69.2%였다.** 한국에서 가장 잘 재현되는
영역이다.

**이번 결과가 그 예측과 맞았다.** Phase B 최대 |IC|가 밸류에서 나왔다.

같은 문서 §1이 이를 첫 번째 결론으로 삼았다.

> **이번 결과는 한국 시장에서 이미 알려진 패턴을 거의 그대로 재현했다.** …
> **데이터 결함이 아니라 시장 특성일 가능성이 크게 올라간다.**

분류 좌표는 C2(재무 기반 상태) × T0(수준) × U다.

### 3.5 근거 문헌

Fama & French (1992, 1993). Lakonishok, Shleifer & Vishny (1994). 등급 A.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 | AB q | 등급 | 판정 |
|---|---|---:|---:|---:|---:|---:|---|---|
| cum | 0→60 | +0.0978 | 1.053 | 6.96 | +2.13%p | ~0 | C | robustness 실패 |
| cum | 0→120 | **+0.1220** | **1.153** | 5.41 | **+4.52%p** | ~0 | C | robustness 실패 |
| bucket | 40→60 | +0.0599 | 0.650 | 8.00 | +0.60%p | ~0 | **B** | **screen-pass** |
| bucket | 60→120 | +0.0865 | 0.938 | 6.29 | +2.00%p | ~0 | C | robustness 실패 |

**Phase B에서 |IC|가 가장 크다.** 35개 전체로는 세 번째다
(`px_idio_vol_60d` 0.143, `px_amihud_20d` 0.134 다음).

|ICIR|이 1.05~1.15로 안정적이다.

### 4.2 세 cell이 시간 placebo에서 떨어졌다

`failed_gates`가 셋 다 `[robustness_pass]`다.

| cell | NW lag | `robustness_required` | `p_temporal_nw` | 판정 |
|---|---:|---|---:|---|
| cum 0→60 | 59 | True | **0.1980** | **실패** |
| cum 0→120 | 119 | True | **0.3663** | **실패** |
| bucket 60→120 | 59 | True | **0.2574** | **실패** |
| bucket 40→60 | 19 | **False** | — | 대상 아님 → 통과 |

기준은 0.10이다.

**살아남은 하나는 검사를 받지 않았다.** [25_fin_gross_profitability.md](25_fin_gross_profitability.md)
§4.2와 정확히 같은 구조다 — **긴 구간에서 검사를 받으면 전부 떨어진다.**

비중첩 offset은 세 cell 모두 `complete`로 통과했으므로 **떨어진 이유는 시간 placebo
하나다.**

### 4.3 IC와 5분위 차이의 비율

|IC| 0.122에 5분위 차이 +4.52%p다. 같은 120일 horizon에서 비교하면 중간이다.

| family | \|IC\| | 5분위 차이 | 차이 / IC |
|---|---:|---:|---:|
| `fin_log_mcap` | 0.115 | +11.83%p | 103 |
| `px_amihud_20d` | 0.134 | +11.21%p | 84 |
| **`fin_value_z`** | **0.122** | **+4.52%p** | **37** |
| `ev_payout_yield` | 0.102 | +0.49%p | 5 |

**규모·유동성 축보다는 낮고 주주환원보다는 훨씬 높다.**

밸류 상위 20%(싼 회사)와 하위 20%(비싼 회사)의 수익률 분산이 규모 축만큼 벌어지지는
않는다는 뜻이다.

### 4.4 신호의 모양

| 관찰 | 값 |
|---|---|
| `peak_cell` | `cum 0→120` |
| `peak_ic_mean` | +0.1220 |
| 누적 IC 추이 | 60일 0.098 → 120일 0.122 (증가) |
| 구간 IC 추이 | 40~60일 0.060 → 60~120일 0.086 (증가) |

관측 범위 끝에서 최대다. 120일 너머는 확인하지 않았다.

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 4개 cell 전부 4/4

| cell | `valid_subperiods` | `sign_consistent_subperiods` | `period_sign_pass` |
|---|---:|---:|---|
| 4개 전부 | 4 | **4** | **True** |

**모든 유효 구간에서 같은 방향이었다.** Phase B에서 이 조합은 드물다.

표본이 2017-02-15부터라 구간이 5개가 아니라 **4개**다. `2014_2016`이 비었다.

### 5.2 시간 placebo — 검사받은 세 cell 전부 실패

§4.2에 정리했다. **이 family의 유일한 실패 지점이다.**

**§5.1과 나란히 놓으면 흥미롭다.** 기간을 넷으로 잘라 보면 전부 같은 방향인데, 시간축을
밀어 만든 가짜 신호와는 구분되지 않는다. 두 검사가 다른 것을 잡는다.

### 5.3 비중첩 offset — `complete` 통과

세 긴 cell 모두 `offset_status = complete`, `nonoverlap_robustness_pass = True`다.

### 5.4 거래 가능한 종목만 남겨도 — 거의 그대로

| cell | `tradable_retention` |
|---|---:|
| cum 0→60 | 1.017 |
| cum 0→120 | 1.019 |
| bucket 40→60 | **1.002** |
| bucket 60→120 | 1.008 |

**네 cell 전부 1.00~1.02다.** 35개 중 가장 1에 가깝다.

**유동성 필터가 이 신호에 전혀 영향을 주지 않는다.** 밸류 효과가 소형주에만 있는 게
아니라는 뜻이고, `fin_log_mcap`(0.79~0.85)이나 `px_amihud_20d`(0.85)와 대조된다.

**실행 가능성 측면에서 규모 축보다 유리한 조건이다.**

### 5.5 생존편향

`available_direction_pass` = **True** (4개 cell 모두).

### 5.6 source quality — `warn` 두 가지

**등급이 A가 아니라 B인 이유다.**

| 항목 | 값 | 임계 | 판정 |
|---|---|---:|---|
| `source_quality_reasons` | **`mapping_fallback,revision`** | — | 두 가지 |
| `mapping_fallback_ratio` | **0.9417** (`revenue`) | 0.50 | **초과** |
| `revision_ratio` | 0.1014 (`total_equity`) | 0.10 | **근소 초과** |
| `pairing_mismatch_ratio` | 0.000125 | 0.01 | 통과 |

**매출액의 94.2%가 매핑 대체 경로로 채워졌다.** S/P 요소의 분자가 그것이다.

Phase B 구현 로그가 명시했다.

> `fin_gross_profitability`와 `fin_value_z`는 `mapping_fallback_ratio`도 각각
> **0.9440·0.9417**로 임계 0.50을 넘었다.
> **B등급 7셀은 통계 때문이 아니라 source 품질 경고로 A 상한이 막혔다.**

**§2.1의 네 요소 중 하나가 특히 불안정한 원천을 쓴다는 뜻이다.** 그런데
`value_component_count` 분포를 보지 않아 **실제로 몇 개 요소로 만들어진 값이 많은지
모른다** (§8).

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2017-02-15 ~ 2025-02-05** |
| 유효 거래일 | **1,928일** |
| 날짜당 평균 종목 수 | **867~868개** |
| `coverage_ratio` | **0.564** |
| 관측 행 수 | 4,068,096 |

**Phase B 재무 계열 중 커버리지가 가장 낮다.** §2.1의 "최소 2개 요소" 조건과 각 요소의
매핑 조건이 겹친 결과다.

시장별로는 KOSDAQ 0.604 / KOSPI 0.503이다.

---

## 7. 중복성 — 가격 계열과 안정적으로 겹친다

### A×B 교차 상관

| 상대 family | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `px_idio_vol_60d` | **−0.351** | 1,928 | **−0.49 ~ +0.02** |
| `px_maxret_20d` | **−0.251** | 1,928 | **−0.44 ~ −0.06** |
| `px_amihud_20d` | +0.203 | 1,928 | −0.13 ~ +0.39 |
| `px_near_52w_high` | +0.167 | 1,928 | −0.11 ~ +0.40 |

**전체 204쌍 중 절대값 3위와 5위가 이 family의 것이다.**

- **`px_idio_vol_60d`와 −0.351.** 싼 회사는 고유변동성이 낮다. 두 family의 기대 부호가
  각각 `+`와 `−`이므로 **같은 방향으로 작동한다.** 범위가 −0.49~+0.02로 거의 전부 음수다.
- **`px_maxret_20d`와 −0.251.** 같은 구조이고 범위가 전부 음수다.
- **`px_amihud_20d`와 +0.203.** 싼 회사가 비유동적이다 = 작다.

**세 관계 모두 모델에 함께 넣을 때 증분 기여를 줄일 수 있다.** `|ρ| ≥ 0.7` 경고 기준에는
못 미치지만 방향이 안정적이다.

### 확인하지 않은 중복

1. **`fin_gross_profitability`와의 관계.** §3.1의 quality 가설이 "밸류와 음의 상관"을
   전제하는데 **B×B 상관 산출물이 없다.**
   [25_fin_gross_profitability.md](25_fin_gross_profitability.md) §7과 같은 공백이다.
2. **`ev_payout_yield`와의 관계.** 둘 다 분모가 시가총액이라 겹칠 수밖에 없다
   ([21_ev_payout_yield.md](21_ev_payout_yield.md) §3.1의 세 번째 메커니즘).
3. **네 요소 간 상관.** B/M·E/P·CFO/P·S/P가 서로 얼마나 겹치는지 재지 않았다. 넷을 평균내는
   설계(§2.2)의 근거를 확인하지 못했다.

---

## 8. 한계와 확인 못 한 것

1. **긴 구간이 전부 시간 placebo에서 떨어졌다** (§4.2, §5.2). screen-pass한 하나는 검사를
   받지 않았다.
2. **매출액의 94.2%가 매핑 대체다** (§5.6). 네 요소 중 S/P가 특히 불안정한 원천을 쓴다.
3. **`value_component_count` 분포를 안 봤다.** 실제로 두 개로 만든 값과 네 개로 만든 값이
   섞여 있는데 비율을 모른다. **두 개짜리와 네 개짜리는 의미가 다른데 같은 값으로 취급된다.**
4. **v1 버그의 잔재를 확인하지 않았다** (§2.4). 지금은 고쳐졌지만 다른 유사 문제가 없는지
   별도로 보지 않았다.
5. **커버리지가 Phase B 재무 계열 최저다** (§6). 43%가 빈다.
6. **자본총계의 10.1%가 사후 정정됐다** (§5.6).
7. **네 요소 간 상관을 안 쟀다** (§7).
8. **`fin_gross_profitability`와의 상관이 없다** (§7). quality-value 조합의 근거를
   확인하지 못했다.
9. **업종 중립화가 없다.** 밸류 지표는 업종에 따라 수준이 크게 다르다 — 은행의 B/M과
   소프트웨어의 B/M을 한 풀에서 비교한다.
10. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
11. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T2

**T2 14-feature bundle에 들어갔다** (`fin_value_z`).

| horizon | Rank IC Δ | 비용 반영 spread Δ |
|---|---:|---:|
| 5 | +0.0031 | +0.0017 |
| 20 | +0.0011 | +0.0030 |
| 60 | +0.0003 | +0.0080 |

세 horizon 전부 개선됐다(`improved_all_horizons`). **14개를 함께 넣은 결과라 개별 기여도는
측정하지 않았다.**

**단변량 |IC|가 14개 중 가장 크다.** 다만 §7에서 본 대로 같은 묶음의
`fin_gross_profitability`·`ev_payout_yield`·`fin_log_mcap`과 겹칠 여지가 있다.

**최종 h60 holdout은 아직 열지 않았다.**

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# Phase B 재무 계열을 |IC| 순으로
print(duckdb.sql(f"""
  select family, scan_type, h_end, ic_mean, q5_spread_aligned, icir,
         tradable_retention, p_temporal_nw, evidence_grade, failed_gates
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  where family like 'fin_%' and scan_type='cum'
  order by abs(ic_mean) desc
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| A×B 상관 | 같은 AB run의 `primary_feature_rank_correlation.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 원천 품질 (매핑 대체 94.2%) | 같은 B run의 `core/quarterly_metric_quality.parquet` |
| 커버리지·vintage 나이 | 같은 B run의 `core/feature_coverage.parquet` |
| 산식 (네 단계) | `research/etl/features/fin_scan.py:268`, `:288`, `:334`, `:357` |
| v1 버그 | `01_feature_candidate/10_known_issues.md` I1 |
| 한국 복제 연구 대조 | `01_feature_candidate/11_feature_taxonomy.md` §1, §4 |
| T2 결과 | `docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json` |
