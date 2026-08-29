# 26. `fin_log_mcap` — 로그 시가총액 (규모)

- 작성일: 2026-08-29
- family: `fin_log_mcap` · primary feature: 동명 · domain: financial
- **Phase B** · fdr_family `financial` · 기대 부호 `−` · 관측 부호 `−`
- **discovery 4/4 · screen-pass 4/4 · 등급 A 4개** · source quality `not_applicable`
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**시가총액이 작은 회사가 이후 60~120일 동안 시장 대비 더 올랐다** (cum 0→120 IC −0.1149,
**5분위 수익률 차이 +11.83%p**).

**Phase B 18개 중 가장 강하고 가장 깨끗하다.**

- 4개 cell **전부 discovery이면서 전부 screen-pass**, `failed_gates` 비어 있음
- **등급 A 4개** — Phase B에서 원천 품질 경고 없이 A를 받은 유일한 재무 family
- 기간 **5/5**, 시간 placebo **전부 통과**(p = 0.0099, 최솟값), 비중첩 offset `complete`
- **5분위 수익률 차이 +11.83%p는 35개 중 두 번째로 크다** (`px_amihud_20d` +11.21%p와
  같은 급)

**그런데 이 신호는 새로운 것이 아니다.** 소형주 효과는 1981년부터 알려진 가장 오래된
이례현상이고, [07_px_amihud_20d.md](07_px_amihud_20d.md)와 **평균 순위상관 −0.721**로
얽혀 있다. 이번 검증에서 `|ρ| ≥ 0.7`로 걸린 두 쌍 중 하나다 (§7).

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/fin_scan.py:267
CASE WHEN base_ok THEN ln(market_cap_pit) END AS fin_log_mcap
```

**시점 정확 시가총액의 자연로그**다. 35개 중 가장 단순한 산식이다.

로그를 씌우는 이유는 시가총액 분포가 심하게 치우쳐 있기 때문인데, **Rank IC는 순위만 쓰므로
로그 여부가 IC 값을 바꾸지 않는다.** 로그는 모델 입력으로 쓸 때를 위한 처리다.

### 2.2 `base_ok`가 품질 게이트다

```sql
-- research/etl/features/fin_scan.py:244
(market_cap_pit IS NOT NULL AND market_cap_pit > 0
 AND shares_is_available AND NOT shares_invalid_flag
 AND NOT COALESCE(is_halted, TRUE) AND valid_session_idx IS NOT NULL
) AS base_ok
```

다섯 조건을 모두 만족해야 값이 나온다.

- 시가총액이 있고 양수
- **PIT 주식수를 그 시점에 알 수 있었고** (`shares_is_available`)
- 주식수가 유효 플래그를 통과했고
- 거래정지일이 아니고
- 유효 세션

같은 마트의 다섯 family가 이 게이트를 공유한다.

### 2.3 vintage 나이가 없다 — 유일하게 신선한 재무 family

커버리지 통계에서 `mean_age_days`와 `p95_age_days`가 **NaN**이다.

**분기 재무제표가 아니라 일별 시가총액을 쓰기 때문이다.** 다른 재무 family는 분기 공시를
기다려야 해서 평균 74~105일 묵은 값을 쓰는데
([25_fin_gross_profitability.md](25_fin_gross_profitability.md) §2.5), 이 family는 **매일
갱신된다.**

| family | 평균 나이 | 95분위 나이 |
|---|---:|---:|
| `fin_gross_profitability` | 105일 | 706일 |
| `fin_accruals_to_assets` | 75일 | 182일 |
| `fin_asset_growth_yoy` | 74일 | 180일 |
| **`fin_log_mcap`** | **—** | **—** |

**§5에서 강건성이 압도적으로 좋은 것과 무관하지 않다.** 신선한 정보와 묵은 정보의 차이가
그대로 나타난다.

### 2.4 PIT 주식수에 의존한다

`market_cap_pit`은 `dim_stock_pit_daily`에서 온다. DART 지분 공시로 만든 시점 정확
주식수에 종가를 곱한 값이다.

`horizon_scan_config.yaml`의 `shares` 블록이 규칙을 정한다.

```yaml
shares:
  disclosed_date_source: rcept_no_yyyymmdd    # 공시 접수번호의 날짜
  availability: next_krx_session              # 다음 거래일부터 사용 가능
  fallback_lag_days: {annual: 90, quarterly: 45}
```

**주식수 자체는 공시 다음 거래일부터 쓴다.** 종가는 당일 것이므로, 시가총액은 "오늘 종가 ×
가장 최근에 알 수 있었던 주식수"다.

### 2.5 `mcap_krx_log`와 무엇이 다른가

**같은 개념을 다른 원천으로 만든 두 family가 나란히 등록돼 있다.**

| | **`fin_log_mcap`** | [28_mcap_krx_log](28_mcap_krx_log.md) |
|---|---|---|
| 원천 | DART 지분 공시 → `dim_stock_pit_daily` | **KRX Open API** → `feat_market_cap` |
| 마트 | `feat_fin_scan_daily` | `feat_market_cap` |
| 사전등록 | 기존 config | 2026-08-27 확장분 |
| `readiness_dependencies` | `feat_fin_scan_daily` | `feat_market_cap` |

**의도적인 이중 측정이다.** 같은 것을 두 경로로 재서 결과가 일치하는지 보는 설계다.
§7에서 두 결과를 비교한다.

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/fin_scan.py:267` |
| 품질 게이트 | `research/etl/features/fin_scan.py:244` |
| PIT 주식수 규칙 | `research/analysis/horizon_scan_config.yaml`의 `shares` |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:417` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**소형주 효과(size effect)다. 가장 오래된 이례현상이다.**

Banz (1981)의 발견이다. 시가총액이 작은 회사가 이후 수익률이 높다. 설명은 여러 갈래다.

- **위험 프리미엄.** 소형주는 도산 위험·유동성 위험이 커서 그 대가를 받는다.
- **유동성 대가.** 거래가 어려운 만큼 할인돼 거래된다 — 이건
  [07_px_amihud_20d.md](07_px_amihud_20d.md) §3.1과 **같은 설명**이다.
- **정보 비대칭.** 애널리스트 커버리지가 적어 정보 우위를 얻기 쉽다.

**두 번째 설명이 §7의 중복 문제를 낳는다.** 규모와 비유동성이 같은 메커니즘의 두 표현이라면
두 피처는 같은 것을 재고 있다.

### 3.2 기대 부호

`−`. 로그 시가총액이 클수록 이후 초과수익률 순위가 낮다. **작을수록 좋다.**

### 3.3 사전등록 horizon

```yaml
# horizon_scan_config.yaml:423
primary_horizon_set: [60, 120]
exploratory_horizon_set: [20, 40]
include_bucket_primary: true
```

소형주 프리미엄은 길게 보유해야 실현된다고 봤다. cell은 4개다.

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 60~120일 | **4개 cell 전부 discovery + screen-pass** |
| 부호 | `−` | **`−` (일치)** |

### 3.4 한국 시장 단서

`02_feature_candidate.md` §1의 우선순위 표에 규모가 독립 항목으로 올라 있지는 않다.
`11_feature_taxonomy.md` §2.2가 `fin_log_mcap`을 **T0(수준)**의 대표 예로 든다.

`11_feature_taxonomy.md` §2.4가 인용한 Barra 표준에서는 size가 style 블록의 핵심 요소이고,
Gu, Kelly & Xiu (2020)의 94개 characteristic에도 당연히 들어 있다.

**표준 리스크 모델이면 반드시 넣는 축이다.** 그래서 이 신호가 강하게 나온 것 자체는
놀랍지 않다.

분류 좌표는 C2(재무 기반 상태) × T0(수준) × U다.

### 3.5 근거 문헌

Banz (1981), *The Relationship Between Return and Market Value of Common Stocks*.
Fama & French (1993)의 SMB 요인. 등급 A.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

부호가 `−`이므로 5분위 차이는 방향 정렬값이다. 양수면 기대대로다.

| scan | horizon | Rank IC | ICIR | t(NW) | **5분위 차이(정렬)** | AB q | 등급 | 판정 |
|---|---|---:|---:|---:|---:|---:|---|---|
| cum | 0→60 | −0.0868 | −0.947 | −7.02 | **+6.29%p** | ~0 | **A** | **screen-pass** |
| cum | 0→120 | **−0.1149** | **−1.105** | −5.84 | **+11.83%p** | ~0 | **A** | **screen-pass** |
| bucket | 40→60 | −0.0413 | −0.475 | −6.30 | +2.03%p | ~0 | **A** | **screen-pass** |
| bucket | 60→120 | −0.0679 | −0.738 | −5.63 | +5.59%p | ~0 | **A** | **screen-pass** |

**4개 전부 통과했고 `failed_gates`가 비어 있다.** Phase B에서 이 조합은
[18_ev_amendment_ratio.md](18_ev_amendment_ratio.md)와 이 family 둘뿐이다.

### 4.2 5분위 수익률 차이가 35개 중 두 번째로 크다

**+11.83%p.** 120거래일(약 6개월) 동안 하위 20%(소형주)가 상위 20%(대형주)보다 시장 대비
11.83%p 더 올랐다는 뜻이다.

| family | horizon | \|IC\| | 5분위 차이 |
|---|---|---:|---:|
| `px_amihud_20d` | 120일 | 0.134 | +11.21%p |
| **`fin_log_mcap`** | **120일** | **0.115** | **+11.83%p** |
| `mcap_krx_log` | 120일 | 0.093 | +11.19%p |
| `fin_value_z` | — | 0.122 | +4.52%p |
| `ev_payout_yield` | 120일 | 0.102 | +0.49%p |

**상위 세 개가 전부 같은 축이다** — Amihud 비유동성, DART 기반 시가총액, KRX 기반
시가총액. §7이 그 이유다.

`ev_payout_yield`와 비교하면 극명하다. |IC|는 비슷한데 5분위 차이가 **24배** 난다
([21_ev_payout_yield.md](21_ev_payout_yield.md) §4.3).

### 4.3 이 수익을 가져갈 수 있는가

세 가지가 걸린다.

1. **거래비용 차감 전이다.** 소형주는 스프레드와 시장충격이 크다.
2. **거래가능 종목만 남기면 15~21%가 사라진다** (§5.4). 다른 A등급 Phase B family와 달리
   유지율이 1 미만이다.
3. **자금을 넣으면 자기가 가격을 밀어 올린다.** 5분위 차이는 그 영향을 반영하지 않는다.

[07_px_amihud_20d.md](07_px_amihud_20d.md) §4.3과 같은 경고다. **두 family가 같은 종목을
가리키므로 같은 실행 문제를 공유한다.**

### 4.4 신호의 모양

| 관찰 | 값 |
|---|---|
| `peak_cell` | `cum 0→120` |
| `peak_ic_mean` | −0.1149 |
| 누적 \|IC\| 추이 | 60일 0.087 → 120일 0.115 (증가) |
| 구간 \|IC\| 추이 | 40~60일 0.041 → 60~120일 0.068 (증가) |

**관측 범위 끝에서 최대다.** 120일 너머가 궁금한데 사전등록 최대치가 120이다.

`px_amihud_20d`도 정확히 같은 모양이었다 (onset 60일, peak 120일).

---

## 5. 진짜인가 — 강건성

**Phase B에서 가장 깨끗하다.**

### 5.1 기간 일관성 — 4개 cell 전부 5/5

`valid_subperiods` = **5**, `sign_consistent_subperiods` = **5**, `period_sign_pass` = True.

**구간이 5개다.** 표본이 2015-03-17부터라 `2014_2016`이 부분적으로라도 잡힌다. 다른 재무
family가 4구간인 것과 다르다.

### 5.2 시간 placebo — 전부 통과, 최솟값

| cell | `p_temporal_nw` | 판정 |
|---|---:|---|
| cum 0→60 | **0.0099** | **통과** |
| cum 0→120 | **0.0099** | **통과** |
| bucket 60→120 | **0.0099** | **통과** |
| bucket 40→60 | — | 대상 아님 (NW lag 19) |

**세 cell 전부 최솟값 0.0099다.** 100번의 시간 이동 placebo 중 관측값만큼 극단적인 게
하나도 없었다.

Phase B 구현 로그가 이를 명시했다.

> 12셀 중 temporal placebo가 요구된 것은 **`fin_log_mcap` 3셀뿐이며 모두 p=0.0099로
> 통과했다.**

**Phase B에서 이 검사를 실제로 받고 통과한 유일한 family다.**
[25_fin_gross_profitability.md](25_fin_gross_profitability.md)(0.168~0.297),
[21_ev_payout_yield.md](21_ev_payout_yield.md)(0.178),
[20_ev_net_share_issuance_yoy.md](20_ev_net_share_issuance_yoy.md)(0.139~0.238)는 전부
떨어졌다.

### 5.3 비중첩 offset — `complete` 통과

세 cell 모두 `offset_status = complete`, `nonoverlap_robustness_pass = True`다.

### 5.4 거래 가능한 종목만 남기면 — 15~21% 사라진다

| cell | `tradable_retention` | `tradable_pass` |
|---|---:|---|
| cum 0→60 | 0.816 | True |
| cum 0→120 | 0.847 | True |
| bucket 40→60 | **0.788** | True |
| bucket 60→120 | 0.849 | True |

**Phase B A등급 중 유일하게 1 미만이다.** 게이트 기준 0.50은 넉넉히 넘지만, 다른 Phase B
family가 1.0~1.23인 것과 대조된다.

**당연한 결과다.** 소형주 효과를 재는 지표에서 유동성 필터를 걸면 신호가 줄어드는 게
정상이다. `px_amihud_20d`(0.852)와 거의 같은 수준이다 —
[07_px_amihud_20d.md](07_px_amihud_20d.md) §5.4와 같은 이야기다.

**결함이 아니라 이 축의 성격이다.** 다만 실행 가능성 측면의 경고이기도 하다.

### 5.5 생존편향

`available_direction_pass` = **True** (4개 cell 모두).

### 5.6 source quality — 경고 없음

| 항목 | 값 |
|---|---|
| `source_quality_status` | **`not_applicable`** |
| `source_quality_grade_cap` | `None` |

**같은 마트의 다른 네 family가 전부 `warn`인데 이것만 깨끗하다.**

이유는 §2.3이다. **분기 재무 vintage를 쓰지 않기 때문이다.** `revision_ratio`와
`mapping_fallback_ratio`는 분기 재무 지표를 대상으로 재는데, 시가총액은 그 대상이 아니다.

그래서 등급이 **A**다. `fin_gross_profitability`·`fin_value_z`가 B에 갇힌 것과 갈린다.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2015-03-17 ~ 2025-02-05** |
| 유효 거래일 | **2,392일** |
| 날짜당 평균 종목 수 | **999~1,002개** |
| `coverage_ratio` | **0.779** |
| 관측 행 수 | 5,616,215 |

**같은 마트의 다른 재무 family보다 표본이 길고 커버리지가 높다.**

| family | 시작 | 거래일 | 커버리지 |
|---|---|---:|---:|
| **`fin_log_mcap`** | **2015-03-17** | **2,392** | **0.779** |
| `fin_asset_growth_yoy` | 2016-06-27 | 1,927 | 0.631 |
| `fin_accruals_to_assets` | 2017-02-27 | 1,927 | 0.596 |
| `fin_gross_profitability` | 2017-02-27 | 1,927 | 0.584 |

분기 재무제표 항목을 요구하지 않고 PIT 주식수만 있으면 되기 때문이다.

시장별로는 KOSDAQ 0.828 / KOSPI 0.703이다.

---

## 7. 중복성 — `|ρ| ≥ 0.7` 두 쌍 중 하나

### `px_amihud_20d`와 −0.721

**이번 검증에서 경고 기준을 넘은 상관 쌍은 전체 204쌍 중 둘뿐이고, 이 family가 그중 하나다.**

| 쌍 | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `px_amihud_20d` × **`fin_log_mcap`** | **−0.721** | 2,392 | **−0.79 ~ −0.55** |
| `px_amihud_20d` × `mcap_krx_log` | −0.754 | 2,622 | −0.85 ~ −0.63 |

**범위가 −0.79 ~ −0.55로 관측 기간 전체에서 −0.55 위로 올라오지 않는다.** 구조적 관계다.

이유는 산식에 있다 — Amihud 비유동성의 분모가 거래대금이고, 거래대금은 시가총액에 비례한다
([07_px_amihud_20d.md](07_px_amihud_20d.md) §7).

**§4.2의 결과와 합치면 그림이 분명해진다.**

| family | 5분위 차이(120일) |
|---|---:|
| `px_amihud_20d` | +11.21%p |
| `fin_log_mcap` | +11.83%p |
| `mcap_krx_log` | +11.19%p |

**세 값이 거의 같다.** 세 피처가 사실상 같은 것을 재고 있다는 가장 직접적인 증거다.

**세 가지 해석이 가능하고 이 자료로는 못 가른다.**

1. 규모가 근본 축이고 비유동성은 그 표현이다
2. 비유동성이 근본이고 규모는 대리변수다 (Amihud 2002의 주장)
3. 둘 다 제3의 요인(정보 비대칭 등)의 표현이다

**구분하려면 하나를 통제한 뒤의 증분 IC가 필요한데 산출하지 않았다.**

### `mcap_krx_log`와의 관계 — 이중 측정의 결과

§2.5에서 본 대로 같은 개념을 두 원천으로 만들었다.

| | `fin_log_mcap` | `mcap_krx_log` |
|---|---:|---:|
| IC (cum 0→120) | −0.1149 | −0.0929 |
| 5분위 차이 | +11.83%p | +11.19%p |
| 유효 거래일 | 2,392 | 2,622 |
| 커버리지 | 0.779 | (§28 참조) |
| `px_amihud_20d` 상관 | −0.721 | −0.754 |

**결과가 서로 일치한다.** 두 경로로 재도 같은 신호가 나온다는 뜻이고, 이중 측정 설계가
값을 했다.

**다만 두 family의 직접 상관은 재지 않았다.** B×B 상관 산출물이 없다. 둘 다 시가총액이므로
0.95 이상일 것이 거의 확실한데 **숫자가 없다.**

---

## 8. 한계와 확인 못 한 것

1. **`px_amihud_20d`와 `|ρ| = 0.72`다** (§7). 규모를 통제한 증분 IC 없이는 두 피처를
   독립 신호로 부를 수 없다. **가장 시급한 후속 작업이다.**
2. **`mcap_krx_log`와의 직접 상관이 없다** (§7). B×B 산출물이 없다. 같은 개념을 두 번
   세고 있는지 숫자로 확인하지 못했다.
3. **거래비용을 반영한 성과가 없다** (§4.3). 소형주는 실행 비용이 가장 비싸다.
4. **거래가능 유지율이 0.79~0.85다** (§5.4). Phase B A등급 중 유일하게 1 미만이다.
5. **120일 너머를 안 봤다** (§4.4). peak가 관측 범위 끝에 있다.
6. **새로운 발견이 아니다.** 1981년부터 알려진 축이고, 표준 리스크 모델이면 반드시 넣는다
   (§3.4). **기존 모델에 이미 있을 가능성이 높은데 확인하지 않았다.**
7. **업종 중립화가 없다.** 업종마다 평균 규모가 다르다.
8. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
9. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T2

**T2 14-feature bundle에 들어갔다** (`fin_log_mcap`).

| horizon | Rank IC Δ | 비용 반영 spread Δ |
|---|---:|---:|
| 5 | +0.0031 | +0.0017 |
| 20 | +0.0011 | +0.0030 |
| 60 | +0.0003 | +0.0080 |

세 horizon 전부 개선됐다(`improved_all_horizons`).

**주의가 특히 필요하다.** 같은 묶음에 `mcap_krx_log`가 **함께** 들어 있다. §7에서 본 대로
둘은 같은 개념이다. **거의 동일한 두 피처를 한 묶음에 넣었을 때의 증분은 하나만 넣었을
때보다 크지 않을 가능성이 높은데, 개별 기여도를 측정하지 않았다.**

그리고 기존 baseline 40개에 규모 지표가 이미 있는지도 확인되지 않았다
(`build_dataset.py`의 `feature_cols_override` 목록을 봐야 한다).

**최종 h60 holdout은 아직 열지 않았다.**

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# 같은 축을 재는 세 family 를 나란히 본다
print(duckdb.sql(f"""
  select family, scan_type, h_start, h_end, ic_mean, q5_spread_aligned,
         tradable_retention, p_temporal_nw, evidence_grade
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  where family in ('fin_log_mcap','mcap_krx_log','px_amihud_20d')
  order by scan_type, h_end, family
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| A×B 상관 204쌍 | 같은 AB run의 `primary_feature_rank_correlation.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 커버리지 | 같은 B run의 `core/feature_coverage.parquet` |
| 산식 | `research/etl/features/fin_scan.py:267` |
| PIT 주식수 규칙 | `research/analysis/horizon_scan_config.yaml`의 `shares` |
| temporal placebo 통과 기록 | `01_feature_candidate/08_phase_b_implementation_log.md` §3.0b |
| T2 결과 | `docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json` |
