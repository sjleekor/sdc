# 21. `ev_payout_yield` — 주주환원 수익률 (배당 + 자사주)

- 작성일: 2026-08-29
- family: `ev_payout_yield` · primary feature: 동명 · domain: event
- **Phase B** · fdr_family `event` · 기대 부호 `+` · 관측 부호 `+`
- **discovery 4/4 · screen-pass 3/4** · 등급 **B 3개 / C 1개** · source quality **warn**
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**배당과 자사주 매입으로 주주에게 많이 돌려주는 회사가 이후 60~120일 동안 더 올랐다**
(cum 0→120 IC **+0.1022**, ICIR 1.212).

**|IC| 0.1022는 35개 family 중 여섯 번째로 크다.** Phase B 이벤트 계열에서는 압도적 1위다.

**그런데 5분위 수익률 차이는 +0.49%p뿐이다.** IC가 비슷한 다른 family와 비교하면 극단적으로
작다.

| family | \|IC\| | 5분위 차이 |
|---|---:|---:|
| `px_amihud_20d` | 0.134 | **+11.21%p** |
| `fin_log_mcap` | 0.115 | +11.83%p |
| `px_maxret_20d` | 0.113 | +1.78%p |
| **`ev_payout_yield`** | **0.102** | **+0.49%p** |

**IC 순위와 수익 순위가 크게 어긋나는 대표 사례다** (§4.3).

등급이 A가 아니라 **B**인 이유는 원천 품질 경고 때문이다 — 자사주 취득금액의 **11.2%가
사후 정정**됐다 (§5.6). 네 cell 중 `cum 0→120` 하나는 시간 placebo에서 떨어져 C다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/event_scan.py:710
CASE WHEN base_ok
          AND (cash_dividends_total IS NOT NULL OR buyback_cash_ttm IS NOT NULL)
          AND (COALESCE(cash_dividends_total, 0) + COALESCE(buyback_cash_ttm, 0))
              / market_cap_pit <= {PAYOUT_YIELD_MAX}
     THEN (COALESCE(cash_dividends_total, 0) + COALESCE(buyback_cash_ttm, 0))
          / market_cap_pit
END AS ev_payout_yield
```

**(현금배당 총액 + 자사주 매입 현금) ÷ 시점 정확 시가총액**이다.

- 0.03이면 시가총액의 3%를 1년 동안 주주에게 돌려줬다
- 배당만 있는 회사, 자사주만 하는 회사, 둘 다 하는 회사를 **한 값으로 비교**한다

### 2.2 배당수익률이 아니라 총주주환원이다

**배당만 보면 절반을 놓친다.** 한국 기업은 배당보다 자사주 매입을 늘려 왔고, 둘은 경제적으로
같은 일이다.

분자가 두 항목의 합인 게 이 피처의 핵심 설계다.

| 항목 | 출처 |
|---|---|
| 현금배당 총액 | 직접 총액 행 우선, 없으면 **DPS × 배당대상주식수** 대체 |
| 자사주 매입 현금 | B-3이 이미 TTM 처리한 `treasury_share_acquisition_amount` |

### 2.3 배당 총액 행은 확인된 적이 없다

**정직하게 기록된 한계다.** 모듈 docstring이 적었다.

> Dividend row-name matching (best-effort, see `CASH_DIVIDEND_TOTAL_ROW_NAME`):
> **this repository has never captured a live OpenDART alotMatter payload with a
> total cash-dividend-amount row**, so the row_name below is an unverified best
> guess. If it never matches, the code does exactly what §4.5 prescribes when
> the direct total is unavailable: **falls back to the DPS proxy** — it does not
> silently produce a wrong number.

즉 **실제로는 대부분 DPS 대체 경로로 계산됐을 가능성이 높다.** 산출물에 `dividend_source`
칼럼이 함께 저장되므로 어느 경로였는지 추적은 가능한데, **이번 검증에서 그 분포를 보지
않았다.**

### 2.4 단위 오류를 자르지 않고 버린다

```sql
AND (배당 + 자사주) / market_cap_pit <= PAYOUT_YIELD_MAX
```

주석이 이유를 적었다.

> a yield above `PAYOUT_YIELD_MAX` means the company returned more than 10x its
> own market value in one year, which no filer does; it is a unit slip the source
> did not give us the means to resolve. **Rejected, not clipped**, for the same
> reason as upstream.

**클리핑하지 않고 NULL로 만든다.** 이상값을 상한에 붙여 두면 그 종목이 항상 최상위 분위에
들어가 신호를 오염시킨다.

`payout_v2`에서는 직접 배당 총액을 **DPS로 역산한 금액과 단위 대조**하고 음수 총액을
거부하는 처리도 들어갔다 (`10_known_issues.md` I5).

### 2.5 산식 버전이 따로 관리된다

```python
# research/etl/features/event_scan.py:87
PAYOUT_FEATURE_FORMULA_VERSION = "payout_v3"
```

같은 모듈에 있는 [20_ev_net_share_issuance_yoy.md](20_ev_net_share_issuance_yoy.md)와
**버전을 분리했다.** 주석이 이유를 적었다.

> Fingerprinting it separately keeps a catalog bump from implying that payout
> changed (and vice versa) — `ev_payout_yield`'s card would otherwise read
> "issuance_vN", which is not the rule it was built with.

| 버전 | 변경 |
|---|---|
| v1 | 2026-08 최초 규칙 |
| v2 | 직접 배당 총액을 DPS 역산액과 단위 대조, 음수 거부 (I5) |
| **v3** | **배당 행을 보통주(또는 미분할 공시)로 제한**, 모든 승자 선택 `ORDER BY`를 전순서로 고정해 scan 순서 의존 제거 (I12) |

### 2.6 PIT

`payout_available_from` 기준 interval join이다. 정본 변형은 **`native_t`**다.
분모 `market_cap_pit`도 시점 정확 시가총액을 쓴다.

### 2.7 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/event_scan.py:710` |
| 버전 이력 | `research/etl/features/event_scan.py:87` |
| 배당 행 한계 | `research/etl/features/event_scan.py` 모듈 docstring |
| 알려진 문제 | `01_feature_candidate/10_known_issues.md` I5, I12 |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:512` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**세 갈래가 같은 방향을 가리킨다.**

1. **현금흐름의 증거.** 배당과 자사주는 실제 현금이 나가야 가능하다. 회계 이익은 조정할 수
   있어도 현금 지급은 못 한다. **이익의 질을 보증하는 신호**다.
2. **경영진의 신호.** 자사주를 사는 건 주가가 싸다고 본다는 뜻이다.
3. **밸류 대리변수.** 분모가 시가총액이므로 같은 배당을 하는 회사 중 싼 회사가 높은 값을
   갖는다. **사실상 밸류 지표의 성격이 섞인다** — §7의 상관이 이를 보여 준다.

### 3.2 기대 부호

`+`. 주주환원 수익률이 높을수록 이후 초과수익률 순위가 높다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_config.yaml:518
primary_horizon_set: [60, 120]
exploratory_horizon_set: [20, 40]
include_bucket_primary: true
```

연 1회 갱신되는 느린 지표이므로 긴 horizon에 걸었다. cell은 4개다.

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 60~120일 | 4개 cell 전부 discovery, **screen-pass 3개** |
| 부호 | `+` | **`+` (일치)** |

### 3.4 한국 시장 단서

`02_feature_candidate.md` §1의 14번 항목이다.

> 순주주환원 | `ev_payout_yield` → `ev_net_payout_yield` | `+` | A | R1/R2

**화살표가 붙어 있다.** 최종 목표는 `ev_net_payout_yield`(주주환원에서 신규 발행을 뺀 순액)
인데, 이번에는 총액 형태만 만들었다.
[20_ev_net_share_issuance_yoy.md](20_ev_net_share_issuance_yoy.md)와 합치면 순액이 되는데
**그 조합은 만들지 않았다.**

분류 좌표는 C4(이벤트·공시) × T0(수준) × U다.

### 3.5 근거 문헌

Boudoukh et al. (2007) 계열의 total payout yield 연구.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 | AB q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | +0.0798 | 1.023 | 7.58 | +0.09%p | ~0 | **discovery + screen-pass (B)** |
| cum | 0→120 | **+0.1022** | **1.212** | 6.36 | **+0.49%p** | ~0 | discovery, **screen 실패 (C)** |
| bucket | 40→60 | +0.0550 | 0.660 | 8.88 | **−0.08%p** | ~0 | **discovery + screen-pass (B)** |
| bucket | 60→120 | +0.0795 | 1.075 | 7.93 | +0.22%p | ~0 | **discovery + screen-pass (B)** |

|ICIR|이 1.0~1.2다. Phase A의 `px_maxret_20d`(1.31), `px_amihud_20d`(1.33)와 같은 급으로
**일별 IC가 안정적이다.**

### 4.2 `cum 0→120`이 떨어진 이유

`failed_gates` = `[robustness_pass]`, `p_temporal_nw` = **0.1782**, `temporal_null_pass` =
**False**.

| cell | `p_temporal_nw` | 판정 |
|---|---:|---|
| cum 0→60 | 0.0792 | **통과** |
| bucket 60→120 | 0.0792 | **통과** |
| cum 0→120 | **0.1782** | **실패** |
| bucket 40→60 | — | 대상 아님 |

**|IC|가 가장 큰 cell이 시간 placebo에서 떨어졌다.** 긴 누적 구간일수록 시간 이동 placebo와
구분하기 어려워지는 패턴은 [20](20_ev_net_share_issuance_yoy.md) §4.2에서도 같았다.

통과한 두 cell도 0.0792로 기준 0.10에 여유가 크지 않다.

### 4.3 IC가 큰데 수익률 차이가 작다 — 이 문서의 핵심

**35개 중 이 어긋남이 가장 극단적이다.**

|IC| 0.102로 6위인데 5분위 차이는 +0.49%p다. 같은 120일 horizon에서 비교하면 이렇다.

| family | horizon | \|IC\| | 5분위 차이 | 배율 |
|---|---|---:|---:|---:|
| `px_amihud_20d` | 120일 | 0.134 | +11.21%p | — |
| `fin_log_mcap` | 120일 | 0.115 | +11.83%p | — |
| **`ev_payout_yield`** | **120일** | **0.102** | **+0.49%p** | **약 1/23** |

IC는 비슷한데 수익률 차이가 **20배 넘게 작다.**

**왜 이런 일이 생기나.**

- **IC**는 횡단면 전체의 순위 상관이다. "순서를 얼마나 잘 맞히나"를 잰다.
- **5분위 차이**는 상위 20%와 하위 20%의 **평균 수익률 차이**다. "양 끝이 얼마나 벌어지나"를
  잰다.

**순서를 잘 맞혀도 양 끝이 안 벌어질 수 있다.** 주주환원 수익률이 높은 종목은 대체로
안정적인 대형 배당주다. 이들은 **수익률 분산 자체가 작다.** 순위는 일관되게 위쪽인데
실제 수익률 차이는 크지 않다.

반대로 `px_amihud_20d`가 고르는 비유동 종목은 수익률 분산이 크다. 같은 순위 정확도로도
벌어지는 폭이 훨씬 크다.

**실무적으로 이렇게 읽는다.**

> 이 신호는 **순위를 매기는 데는 강하지만 그 자체로 큰 수익을 만들지는 않는다.**
> 다른 신호와 결합해 순위를 다듬는 용도에 맞고, 단독 롱숏 전략의 알파원으로는 약하다.

`bucket 40→60`은 IC +0.055인데 5분위 차이가 **−0.08%p로 음수**다. 부호까지 갈린다.

**IC만 보던 보고서에서는 이 사실이 전혀 보이지 않았다.**

### 4.4 신호의 모양

| 관찰 | 값 |
|---|---|
| `peak_cell` | `cum 0→120` |
| `peak_ic_mean` | +0.1022 |
| 누적 IC 추이 | 60일 0.080 → 120일 0.102 (증가) |
| 구간 IC 추이 | 40~60일 0.055 → 60~120일 0.080 (증가) |

관측 범위 끝에서 최대다. **다만 그 cell이 시간 placebo에서 떨어졌다** (§4.2).

---

## 5. 진짜인가 — 강건성

### 5.1 기간 일관성 — 4개 cell 전부 5/5

`valid_subperiods` = 5, `sign_consistent_subperiods` = **5**, `period_sign_pass` = True.

### 5.2 시간 placebo — 셋 통과, 하나 실패

§4.2에 정리했다. 통과한 둘도 0.0792로 여유가 크지 않다.

### 5.3 비중첩 offset — `complete` 통과

`cum 0→60`, `cum 0→120`, `bucket 60→120` 세 cell이 `offset_status = complete`이고
`nonoverlap_robustness_pass = True`다.

`cum 0→120`이 떨어진 건 offset이 아니라 **시간 placebo 하나 때문이다.**

### 5.4 거래 가능한 종목만 남기면 — 오히려 강해진다

| cell | `tradable_retention` |
|---|---:|
| cum 0→60 | **1.096** |
| cum 0→120 | **1.091** |
| bucket 40→60 | 1.071 |
| bucket 60→120 | 1.068 |

네 cell 전부 1을 넘는다. 유동성 좋은 종목에서 더 강하다.

**§4.3의 해석과 맞는다** — 배당·자사주를 하는 회사는 애초에 유동성 좋은 대형주가 많다.

### 5.5 생존편향

`available_direction_pass` = **True** (4개 cell 모두).

### 5.6 source quality — `warn` 때문에 등급이 B다

**이 family가 A가 아닌 이유다.**

| 항목 | 값 |
|---|---|
| `source_quality_status` | **`warn`** |
| `source_quality_reasons` | **`revision`** |
| `revision_ratio` | **0.1116** |
| `revision_worst_metric` | **`treasury_share_acquisition_amount`** |
| `mapping_fallback_ratio` | 0.000 (`issued_shares`) |
| `pairing_mismatch_ratio` | 0.000125 |

**자사주 취득금액의 11.2%가 나중에 정정됐다.** 즉 처음 공시된 값과 최종 값이 다른 경우가
9건 중 1건꼴이다.

이번 scan은 **최종본(final vintage)**을 쓰므로, 그 시점에 실제로 알 수 있었던 값보다
정확한 값을 쓴 셈이다. 미래 정보가 섞였을 여지가 있다.

등급 규칙이 이를 반영한다.

```yaml
evidence_grade:
  A: screen_pass_and_no_core_warning_and_all_offsets_evaluable
  B: screen_pass_with_nonfatal_warning     # ← 여기
```

screen-pass한 세 cell이 A가 아니라 **B**인 건 이 경고 때문이다. `source_quality_grade_cap`은
`None`이지만(강제 상한은 없음) 경고 자체가 등급을 한 단계 내렸다.

`mapping_fallback_ratio`가 0.000인 건 좋은 신호다 — 재무 계열
([25_fin_gross_profitability.md](25_fin_gross_profitability.md) 0.944,
[27_fin_value_z.md](27_fin_value_z.md) 0.942)과 대조된다.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2015-12-28 ~ 2025-02-05** |
| 유효 거래일 | **2,175일** |
| 날짜당 평균 종목 수 | 987~990개 |
| `coverage_ratio` | **0.709** |
| 관측 행 수 | 5,113,246 |

**패널의 29%에 값이 없다.** 배당도 자사주도 하지 않은 회사, 단위 오류로 거부된 값(§2.4),
공시가 아직 안 나온 구간이 빠진다.

[20](20_ev_net_share_issuance_yoy.md)의 0.479보다는 훨씬 높다.

---

## 7. 중복성 — 밸류·변동성 축과 안정적으로 겹친다

### A×B 교차 상관

| 상대 family | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `px_idio_vol_60d` | **−0.347** | 2,175 | **−0.43 ~ −0.23** |
| `px_maxret_20d` | **−0.241** | 2,175 | **−0.37 ~ −0.09** |
| `px_near_52w_high` | **+0.223** | 2,175 | **+0.05 ~ +0.35** |
| `px_reversal_5d` | −0.027 | 2,175 | −0.30 ~ +0.39 |

**앞의 셋은 전체 204쌍 중 절대값 상위권이고, 범위가 한쪽 부호로만 몰려 있다.**
날짜와 무관하게 안정적으로 겹친다는 뜻이다.

- **`px_idio_vol_60d`와 −0.347.** 주주환원을 많이 하는 회사는 고유변동성이 낮다.
  `px_idio_vol_60d`의 기대 부호가 `−`이고 이 family가 `+`이므로 **두 신호가 같은 방향으로
  작동한다.**
- **`px_maxret_20d`와 −0.241.** 같은 구조다.
- **`px_near_52w_high`와 +0.223.** 주주환원 회사가 고점 근처에 있다.

**셋 다 모델에 함께 넣으면 증분 기여가 줄어들 가능성이 크다.** `|ρ| ≥ 0.7` 경고 기준에는
못 미치지만 방향이 안정적이라는 점이 중요하다.

§3.1의 세 번째 메커니즘(밸류 대리변수)이 이 상관 구조로 뒷받침된다.

### 확인하지 않은 중복

- [20_ev_net_share_issuance_yoy.md](20_ev_net_share_issuance_yoy.md)와의 관계.
  §3.4에서 본 대로 둘을 합치면 `ev_net_payout_yield`가 되는데 **조합도, 상관도 없다.**
- [27_fin_value_z.md](27_fin_value_z.md)와의 관계. 분모가 시가총액이라 밸류 지표와 겹칠
  수밖에 없는데 **B×B 상관 산출물이 없다.**

---

## 8. 한계와 확인 못 한 것

1. **IC가 큰데 수익률 차이가 작다** (§4.3). 35개 중 이 어긋남이 가장 크다. 순위 신호로는
   강하지만 단독 알파원으로는 약하다는 뜻인데, 이를 확인할 분위별 수익률 자료가 없다.
2. **자사주 취득금액의 11.2%가 사후 정정됐다** (§5.6). 최종본을 써서 미래 정보가 섞였을
   여지가 있다.
3. **배당 총액 행이 실제로 매칭된 적이 없다** (§2.3). 대부분 DPS 대체 경로일 가능성이
   높은데 `dividend_source` 분포를 보지 않았다.
4. **|IC| 최대 cell이 시간 placebo에서 떨어졌다** (§4.2). 통과한 둘도 여유가 작다.
5. **밸류·변동성 축과 안정적으로 겹친다** (§7). 통제 후 증분을 재지 않았다.
6. **`ev_net_payout_yield`를 만들지 않았다** (§3.4). 원래 목표 형태다.
7. **120일 너머를 안 봤다** (§4.4).
8. **업종 중립화가 없다.** 업종마다 배당 성향이 크게 다르다.
9. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
10. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T2

**T2 14-feature bundle에 들어갔다** (`ev_payout_yield`).

| horizon | Rank IC Δ | 비용 반영 spread Δ |
|---|---:|---:|
| 5 | +0.0031 | +0.0017 |
| 20 | +0.0011 | +0.0030 |
| 60 | +0.0003 | +0.0080 |

세 horizon 전부 개선됐다(`improved_all_horizons`). **14개를 함께 넣은 결과라 개별
기여도는 측정하지 않았다.**

**§4.3과 §7을 함께 읽어야 한다.** 단변량 |IC|는 14개 중 가장 큰 축이지만 5분위 수익률
차이는 가장 작은 축이다. 그리고 같은 묶음에 `fin_value_z`·`fin_log_mcap`·`mcap_krx_log`가
들어 있어 밸류·규모 축과 겹친다. **묶음 개선분 중 이 피처의 몫이 얼마인지는 알 수 없다.**

**최종 h60 holdout은 아직 열지 않았다.**

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# IC 대비 5분위 차이가 작은 순으로 본다
print(duckdb.sql(f"""
  select family, max(abs(ic_mean)) as max_abs_ic,
         max(q5_spread_aligned)   as max_q5,
         max(q5_spread_aligned) / nullif(max(abs(ic_mean)),0) as q5_per_ic
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  group by family order by max_abs_ic desc limit 10
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 원천 품질 (정정 비율) | 같은 B run의 `core/stock_metric_vintage_quality.parquet`, `core/quarterly_metric_quality.parquet` |
| 커버리지 | 같은 B run의 `core/feature_coverage.parquet` |
| 산식 | `research/etl/features/event_scan.py:710` |
| 버전 이력·배당 행 한계 | `research/etl/features/event_scan.py:87`, 모듈 docstring |
| 알려진 문제 | `01_feature_candidate/10_known_issues.md` I5, I12 |
| 등급 규칙 | `research/analysis/horizon_scan_config.yaml`의 `evidence_grade` |
| T2 결과 | `docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json` |
