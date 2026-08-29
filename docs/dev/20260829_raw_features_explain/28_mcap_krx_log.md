# 28. `mcap_krx_log` — 로그 시가총액 (KRX 상장주식수 기준)

- 작성일: 2026-08-29
- family: `mcap_krx_log` · primary feature: 동명 · domain: financial
- **Phase B** · fdr_family `financial` · 기대 부호 `−` · 관측 부호 `−`
- **discovery 4/4 · screen-pass 2/4** · 등급 **A 2개 / C 2개** · source quality `not_applicable`
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**[26_fin_log_mcap.md](26_fin_log_mcap.md)와 같은 개념을 다른 원천으로 만든 피처다.**
저쪽은 DART 지분 공시의 발행주식수, 이쪽은 **KRX가 발표하는 상장주식수**를 쓴다.

**의도적인 이중 측정이고, 결과가 일치했다.**

| | `fin_log_mcap` (DART) | **`mcap_krx_log` (KRX)** |
|---|---:|---:|
| IC (cum 0→120) | −0.1149 | **−0.0929** |
| 5분위 수익률 차이 | +11.83%p | **+11.19%p** |
| `px_amihud_20d` 상관 | −0.721 | **−0.754** |
| 유효 거래일 | 2,392 | **2,622** |
| **커버리지** | 0.779 | **0.99862** |
| 유효 시작 | 2015-03-17 | **2014-06-02** |

**커버리지 99.9%는 Phase B 18개 중 최고이고, 표본도 가장 길다.** KRX 값이 매일 갱신되고
공시 지연이 없기 때문이다.

**그런데 거래가능 유지율이 0.558~0.685로 Phase B 최저다** (§5.4). 그리고 두 cell이
시간 placebo에서 떨어져 C다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/market_cap.py:71
CASE WHEN NOT mcap_unreliable AND m.market_cap > 0
     THEN ln(CAST(m.market_cap AS DOUBLE)) END AS mcap_krx_log
```

**KRX가 발표하는 시가총액의 자연로그**다. `daily_market_cap.market_cap`을 그대로 쓴다.

### 2.2 왜 같은 것을 두 번 만들었나 — 공시 지연 문제

**모듈 docstring이 이유를 정확히 적었다.**

> The existing size feature is `fin_log_mcap` in `fin_scan`, built as
> `close_d × issued_raw` where `issued_raw` is DART's issued-share count.
> **That pairs a price which reflects a corporate action immediately with a
> share count that does not move until the next periodic report is *disclosed*
> — months, not days.** Nothing gates it: `base_ok` checks availability and
> validity, and `shares_age_days` is computed in `stock_pit` and **never read**.
>
> `daily_market_cap.listed_shares` is the exchange's number and changes on the
> **listing** date. That collapses the mismatch from months to about three weeks
> and **removes the disclosure-lag error class entirely** — the reason N1 was
> collected.

**문제의 구조는 이렇다.**

유상증자로 주식 수가 늘면 가격은 그날 바로 반응한다. 그런데 DART 발행주식수는 **다음 정기
보고서가 공시될 때까지 안 움직인다.**

```
시가총액 = 오늘 가격(사건 반영됨) × 몇 달 전 주식수(사건 미반영)
```

**분자와 분모의 시점이 어긋난다.** 그 오차가 몇 달 단위다.

KRX 상장주식수는 **상장일에 바뀐다.** 어긋남이 몇 달에서 **약 3주**로 줄어든다.

### 2.3 새 id를 만들었지 기존 것을 고치지 않았다

> Because the definition changes, this lands on a **new id** rather than moving
> `fin_log_mcap` (N1-7 decision 1: **frozen ids keep their definition, new
> definitions get new ids**). `fin_log_mcap` stays as published, with its lag
> recorded as a limitation.

**사전등록 규율의 핵심이다.** 기존 피처의 정의를 바꾸면 이전 결과와 비교할 수 없게 된다.
그래서 `fin_log_mcap`은 그대로 두고 **새 이름으로 등록**했다.

그 덕에 §1의 이중 측정 비교가 가능해졌다.

### 2.4 남은 3주는 고치지 않고 마스킹한다

```sql
-- research/etl/features/market_cap.py:67 주석
-- Masked, not corrected. A wrong value concentrated on bonus-issue
-- names is worse in a factor test than a missing one: it moves the
-- cross-sectional rank of exactly the companies whose event is
-- correlated with past returns and small size.
```

무상증자 등으로 남는 3주 창은 **추정해서 고치지 않고 값을 버린다.**

이유가 중요하다. **틀린 값이 무상증자 종목에 몰리면, 하필 과거 수익률·소형주와 상관된
집단의 횡단 순위가 움직인다.** 결측보다 나쁘다.

`mcap_unreliable` 플래그가 마스킹을 결정하고, **원값 `mcap_krx`는 그대로 남겨 감사할 수
있게 한다.** 모델용 컬럼 `mcap_krx_log`만 NULL이 된다.

추정을 거부한 근거는 `poc/n1_validation.md` §5.5에 있다.

### 2.5 원천

`daily_market_cap`은 KRX Open API의 `sto/stk_bydd_trd`에서 온다. 한 번의 호출이
**하루치 시장 전체**를 준다 — 시가총액·거래대금·상장주식수에 미조정 OHLC까지 함께 온다
(`CLAUDE.md`).

**T+1 공표다.** 어제 장의 값이 오늘 저녁에 들어온다. 그래서 `daily_market_cap`은
`daily_ohlcv`보다 한 세션 뒤처지는 게 정상 상태다.

**이번 검증에는 영향이 없다.** 정본 변형이 `native_t`이고 표본이 2025-02-05에서 끝난다.

### 2.6 산식 버전

`formula_version: market_cap_v2`. 이 family 전용이다.

### 2.7 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/market_cap.py:71` |
| 설계 근거 | 같은 파일 모듈 docstring |
| 마스킹 창 정의 | `research/etl/corporate_actions.py` |
| 추정 거부 근거 | `02_data_expansion_plan/poc/n1_validation.md` §5.5 |
| 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**소형주 효과다.** [26_fin_log_mcap.md](26_fin_log_mcap.md) §3.1과 완전히 같다 —
위험 프리미엄, 유동성 대가, 정보 비대칭 세 갈래.

**다른 건 가설이 아니라 측정 방법이다.**

### 3.2 기대 부호

`−`. 로그 시가총액이 클수록 이후 초과수익률 순위가 낮다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_expansion_20260827.yaml
- family: mcap_krx_log
  expected_sign: "-"
  primary_horizon_set: [60, 120]
  exploratory_horizon_set: [20, 40]
  include_bucket_primary: true
  readiness_dependencies: [feat_market_cap, label_scan]
```

`fin_log_mcap`과 동일하다. **같은 것을 재므로 같은 격자를 써야 비교가 성립한다.**

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 60~120일 | **4개 cell 전부 discovery, screen-pass 2개** |
| 부호 | `−` | **`−` (일치)** |

### 3.4 사전등록 시점

**2026-08-27 확장 등록분이다** (`outcome_blind: true`). `fin_log_mcap`은 기존 config에 있고
이건 overlay로 추가됐다.

분류 좌표는 C2(재무 기반 상태) × T0(수준) × U다.

### 3.5 근거 문헌

Banz (1981), Fama & French (1993). [26](26_fin_log_mcap.md) §3.5와 같다.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이(정렬) | AB q | 등급 | 판정 |
|---|---|---:|---:|---:|---:|---:|---|---|
| cum | 0→60 | −0.0692 | −0.593 | −4.61 | +6.19%p | 0.00001 | **A** | **screen-pass** |
| cum | 0→120 | **−0.0929** | −0.717 | −3.96 | **+11.19%p** | 0.00014 | C | robustness 실패 |
| bucket | 40→60 | −0.0269 | −0.264 | −3.60 | +2.07%p | 0.00057 | **A** | **screen-pass** |
| bucket | 60→120 | −0.0480 | −0.419 | −3.32 | +5.25%p | 0.00156 | C | robustness 실패 |

### 4.2 `fin_log_mcap`과 나란히 놓으면

**두 결과가 같은 이야기를 한다.**

| cell | `fin_log_mcap` IC | **`mcap_krx_log` IC** | `fin_log_mcap` 5분위 | **`mcap_krx_log` 5분위** |
|---|---:|---:|---:|---:|
| cum 0→60 | −0.0868 | **−0.0692** | +6.29%p | **+6.19%p** |
| cum 0→120 | −0.1149 | **−0.0929** | +11.83%p | **+11.19%p** |
| bucket 40→60 | −0.0413 | **−0.0269** | +2.03%p | **+2.07%p** |
| bucket 60→120 | −0.0679 | **−0.0480** | +5.59%p | **+5.25%p** |

**5분위 수익률 차이가 거의 같다.** 네 cell 전부 0.1%p 안쪽이다.

**IC는 KRX 쪽이 일관되게 작다** (0.069 대 0.087, 0.093 대 0.115). 두 가지로 읽을 수 있다.

1. **표본이 다르다.** KRX는 2,622일 / 1,077종목, DART는 2,392일 / 1,000종목이다. KRX 쪽이
   더 많은 종목을 담고, 그중에는 신호가 약한 구간이 섞여 있을 수 있다.
2. **측정 오차의 방향이 다르다.** §2.2에서 본 DART의 공시 지연 오차가 **우연히 신호를
   키웠을** 가능성이 있다. 주식 수가 안 움직이는 동안 가격만 움직이면 시가총액 변화가
   가격 변화를 그대로 반영하는데, 그건 규모가 아니라 **최근 수익률**을 재는 셈이 된다.

**어느 쪽인지 이 자료로는 못 가른다.** 두 피처의 직접 상관을 재지 않았고(§7), 공통 표본으로
맞춘 비교도 하지 않았다.

**다만 5분위 수익률 차이가 일치한다는 사실은 중요하다.** 경제적 크기는 측정 방법과 무관하게
같다는 뜻이고, **소형주 효과 자체는 측정 오차의 산물이 아니다.**

### 4.3 5분위 수익률 차이가 35개 중 세 번째로 크다

+11.19%p다. `fin_log_mcap`(+11.83%p), `px_amihud_20d`(+11.21%p) 다음이다.

**상위 셋이 전부 같은 축이다** ([26](26_fin_log_mcap.md) §4.2). §7이 그 이유다.

### 4.4 신호의 모양

| 관찰 | 값 |
|---|---|
| `peak_cell` | `cum 0→120` |
| `peak_ic_mean` | −0.0929 |
| 누적 \|IC\| 추이 | 60일 0.069 → 120일 0.093 (증가) |
| 구간 \|IC\| 추이 | 40~60일 0.027 → 60~120일 0.048 (증가) |

관측 범위 끝에서 최대다. `fin_log_mcap`·`px_amihud_20d`와 같은 모양이다.

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 5구간 중 3~4구간

| cell | `valid_subperiods` | `sign_consistent_subperiods` | `period_sign_pass` |
|---|---:|---:|---|
| cum 0→60 | 5 | **4** | True |
| cum 0→120 | 5 | **4** | True |
| bucket 40→60 | 5 | **4** | True |
| bucket 60→120 | 5 | **3** | True |

**전부 통과했지만 5/5는 아니다.** `fin_log_mcap`이 네 cell 전부 5/5였던 것과 다르다.

**구간이 5개다.** 표본이 2014-06-02부터라 `2014_2016`이 온전히 잡힌다. Phase B에서
5구간을 채운 family는 이것과 `fin_log_mcap`, `ev_amendment_ratio`, `ev_filing_activity`,
`ev_payout_yield`, `ev_net_share_issuance_yoy` 정도다.

### 5.2 시간 placebo — 하나 통과, 둘 실패

| cell | `p_temporal_nw` | 판정 |
|---|---:|---|
| cum 0→60 | **0.0891** | **통과** (기준 0.10) |
| cum 0→120 | **0.1683** | **실패** |
| bucket 60→120 | **0.2178** | **실패** |
| bucket 40→60 | — | 대상 아님 (NW lag 19) |

**0.0891은 통과이지만 여유가 작다.** `fin_log_mcap`이 세 cell 전부 0.0099(최솟값)로
통과한 것과 확연히 다르다.

**같은 개념인데 강건성이 갈린다.** §4.2의 두 번째 해석(측정 오차가 신호를 키웠을 가능성)과
방향이 맞는 정황이지만, 확정하려면 공통 표본 비교가 필요하다.

### 5.3 비중첩 offset — `complete` 통과

세 긴 cell 모두 `offset_status = complete`, `nonoverlap_robustness_pass = True`다.
떨어진 이유는 시간 placebo 하나다.

### 5.4 거래 가능한 종목만 남기면 — Phase B 최저

| cell | `tradable_retention` | `tradable_pass` |
|---|---:|---|
| cum 0→60 | 0.660 | True |
| cum 0→120 | 0.685 | True |
| bucket 40→60 | **0.558** | True |
| bucket 60→120 | 0.632 | True |

**Phase B 18개 중 가장 낮다.** 게이트 기준 0.50을 겨우 넘는 cell도 있다.

같은 축의 세 family를 나란히 놓으면 이렇다.

| family | 유지율 | 커버리지 |
|---|---:|---:|
| `fin_value_z` | 1.00 ~ 1.02 | 0.564 |
| `px_amihud_20d` | 0.852 | — |
| `fin_log_mcap` | 0.79 ~ 0.85 | 0.779 |
| **`mcap_krx_log`** | **0.56 ~ 0.69** | **0.999** |

**커버리지가 높을수록 유지율이 낮다는 패턴이 보인다.** 이 family는 거래가 거의 없는
초소형주까지 값을 갖기 때문에(커버리지 99.9%), 유동성 필터를 걸면 그만큼 많이 빠진다.

**커버리지가 높다는 게 항상 좋은 건 아니라는 사례다.** 신호의 상당 부분이 실행 불가능한
종목에서 나온다.

### 5.5 생존편향

`available_direction_pass` = **True** (4개 cell 모두).

### 5.6 source quality — 경고 없음

`source_quality_status` = **`not_applicable`**. 분기 재무 vintage를 쓰지 않기 때문이다.

그래서 screen-pass한 두 cell이 **등급 A**를 받았다.

---

## 6. 표본과 커버리지 — Phase B 최고

| 항목 | 값 |
|---|---|
| 유효 표본 | **2014-06-02 ~ 2025-02-05** |
| 유효 거래일 | **2,622일** |
| 날짜당 평균 종목 수 | **1,076~1,079개** |
| **`coverage_ratio`** | **0.99862** |
| 관측 행 수 | **7,056,361** |

**Phase B 18개 중 커버리지·표본·관측 수 모두 1위다.**

| 시장 | 커버리지 |
|---|---:|
| KOSPI | **1.00000** |
| KOSDAQ | 0.99773 |

**KOSPI는 결측이 하나도 없다.** KOSDAQ의 0.23%가 §2.4의 무상증자 마스킹 창이다.

연도별로도 2014년 1.000부터 계속 0.998 이상이다. **Phase A 가격 계열과 같은 수준의
커버리지를 재무 계열에서 확보한 유일한 사례다.**

원인은 §2.5다. KRX가 매일 시장 전체를 주므로 공시를 기다릴 필요가 없다.

---

## 7. 중복성 — `|ρ| ≥ 0.7` 두 쌍 중 하나

### `px_amihud_20d`와 −0.754

**이번 검증에서 경고 기준을 넘은 상관 쌍 두 개 중 절대값이 더 큰 쪽이다.**

| 쌍 | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `px_amihud_20d` × **`mcap_krx_log`** | **−0.754** | 2,622 | **−0.85 ~ −0.63** |
| `px_amihud_20d` × `fin_log_mcap` | −0.721 | 2,392 | −0.79 ~ −0.55 |

**범위가 −0.85 ~ −0.63으로 한 번도 −0.63 위로 올라오지 않는다.**

`fin_log_mcap`(−0.55까지 올라감)보다 더 강하게 얽혀 있다. 표본이 겹치는 구간이 더 길고
(2,622일), KRX 시가총액이 거래대금과 같은 원천에서 오기 때문일 수 있다 — 둘 다
`daily_market_cap` 계열이다. **확인하지는 않았다.**

해석은 [26_fin_log_mcap.md](26_fin_log_mcap.md) §7과 같다. 규모가 근본인지 비유동성이
근본인지 이 자료로는 못 가른다.

### `fin_log_mcap`과의 직접 상관이 없다

**같은 개념을 두 원천으로 만든 두 family인데 서로의 상관을 재지 않았다.**

A×B 상관 산출물은 **A primary 12 × B primary 17 교차만** 담는다
(`13_..._plan.md` §7.2 차트 5). 둘 다 Phase B이므로 대상이 아니다.

§4.2에서 5분위 수익률 차이가 거의 같다는 걸 확인했지만, **횡단 순위상관은 모른다.**
0.95 이상일 것이 거의 확실한데 숫자가 없다.

**T2 묶음에 둘 다 들어갔다는 점에서 이 공백이 실질적 문제가 된다** (§9).

---

## 8. 한계와 확인 못 한 것

1. **`px_amihud_20d`와 `|ρ| = 0.75`다** (§7). 이번 검증에서 가장 큰 상관이다. 규모를
   통제한 증분 IC가 없다.
2. **`fin_log_mcap`과의 직접 상관이 없다** (§7). 같은 개념인데 숫자로 확인하지 못했다.
   **T2에 둘 다 들어갔으므로 가장 시급하다.**
3. **거래가능 유지율이 Phase B 최저다** (§5.4). 0.558~0.685. 신호의 상당 부분이 실행
   불가능한 종목에서 나온다.
4. **긴 두 cell이 시간 placebo에서 떨어졌다** (§5.2). 통과한 하나도 0.089로 여유가 작다.
5. **`fin_log_mcap`보다 IC가 작은 이유를 모른다** (§4.2). 표본 차이인지 측정 오차 차이인지
   가르지 않았다.
6. **거래비용을 반영한 성과가 없다.** 소형주는 실행 비용이 가장 비싸다.
7. **3주 마스킹 창의 크기를 재지 않았다** (§2.4). KOSDAQ 결측 0.23%가 그 창인데, 마스킹을
   끈 값(`distortion_view=None`)과 비교하는 진단을 이번에 돌리지 않았다.
8. **120일 너머를 안 봤다** (§4.4).
9. **업종 중립화가 없다.**
10. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
11. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T2

**T2 14-feature bundle에 들어갔다** (`mcap_krx_log`).

| horizon | Rank IC Δ | 비용 반영 spread Δ |
|---|---:|---:|
| 5 | +0.0031 | +0.0017 |
| 20 | +0.0011 | +0.0030 |
| 60 | +0.0003 | +0.0080 |

세 horizon 전부 개선됐다(`improved_all_horizons`).

**주의가 필요한 대목이다.** 같은 묶음에 **`fin_log_mcap`이 함께 들어 있다.**

§4.2에서 본 대로 두 피처는 같은 개념이고 5분위 수익률 차이가 거의 같다. **거의 동일한 두
값을 한 묶음에 넣으면 증분이 하나만 넣었을 때보다 크지 않을 가능성이 높은데, 개별 기여도를
측정하지 않았다.**

`ev_payout_yield`·`fin_value_z`도 §7의 규모 축과 얽혀 있어(각각 −0.35, +0.20) 14개 묶음
안에 규모 관련 정보가 여러 번 들어간 셈이다.

**최종 h60 holdout은 아직 열지 않았다.**

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# 같은 개념을 두 원천으로 잰 두 family 를 cell 단위로 대조한다
print(duckdb.sql(f"""
  select scan_type, h_start, h_end,
         max(case when family='fin_log_mcap' then ic_mean end)  as ic_dart,
         max(case when family='mcap_krx_log' then ic_mean end)  as ic_krx,
         max(case when family='fin_log_mcap' then q5_spread_aligned end) as q5_dart,
         max(case when family='mcap_krx_log' then q5_spread_aligned end) as q5_krx
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  where family in ('fin_log_mcap','mcap_krx_log')
  group by scan_type, h_start, h_end order by scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| A×B 상관 (`|ρ|≥0.7` 두 쌍) | 같은 AB run의 `primary_feature_rank_correlation.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 커버리지 (0.99862) | 같은 B run의 `core/feature_coverage.parquet` |
| 산식·설계 근거 | `research/etl/features/market_cap.py:71`, 모듈 docstring |
| 마스킹 창 | `research/etl/corporate_actions.py` |
| 추정 거부 근거 | `02_data_expansion_plan/poc/n1_validation.md` §5.5 |
| KRX Open API 응답 스펙 | `02_data_expansion_plan/poc/krx_open_api.md` §4.1c |
| 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |
| T2 결과 | `docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json` |
