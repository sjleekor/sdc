# 15. `flow_short_interest` — 공매도 잔고 비율

- 작성일: 2026-08-29
- family: `flow_short_interest` · primary feature: **`flow_short_interest_ratio`**
- Phase A · fdr_family `flow` · role **`exploratory_short_regime`** · 기대 부호 `−` · 관측 부호 `−`
- 등급 **C** · `screen_pass` 실패 · **BH·discovery 대상이 아님** (`fdr_include: false`)
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**공매도 4개 중 |IC|가 가장 크다** (0→60일 IC −0.0874, 5분위 수익률 차이 +2.60%p).
기대 방향과 일치한다.

**그런데 판정은 보류다.** 표본이 **909일**뿐이고 2020-03-24에서 끝난다. 기간 검정은 2구간,
비중첩 offset 검정은 **`insufficient`로 완결되지 못했다.**

[14_flow_short_turnover.md](14_flow_short_turnover.md)와 짝이다. 저쪽은 **거래량**(그날 얼마나
공매도됐나), 이쪽은 **잔고**(지금 얼마나 공매도된 채 남아 있나)를 잰다. 잔고는 2016-06-30부터만
있고 **2거래일 공표 지연**을 반영해야 해서 표본이 511일 더 짧다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/flow.py:253
CASE WHEN short_regime = 'allowed' AND short_balance_is_available
     THEN short_selling_balance_quantity / NULLIF(float_shares_pit, 0) END
    AS flow_short_interest_ratio
```

**공매도 잔고 주식 수를 유통주식수(PIT)로 나눈 값**이다.

- 0.02면 유통주식의 2%가 공매도된 채 남아 있다
- 값이 클수록 미청산 공매도 포지션이 크다

### 2.2 거래량이 아니라 잔고다 — 재는 것이 다르다

| | `flow_short_turnover` | **`flow_short_interest`** |
|---|---|---|
| 원천 필드 | `short_selling_volume` | `short_selling_balance_quantity` |
| 재는 것 | 20일간 공매도 **거래량** 비중 | 현재 미청산 **잔고** 비율 |
| 성격 | flow (기간 누적) | **stock (시점 잔액)** |
| 분모 | 총 거래량 | **유통주식수(PIT)** |
| 데이터 시작 | 2014-06 (원천은 2007-06) | **2016-06-30** |
| 공표 지연 | 없음 | **2거래일** |

거래량이 많아도 당일 청산하면 잔고는 늘지 않는다. 반대로 거래가 없어도 과거 포지션이
남아 있으면 잔고는 크다. **두 값이 같은 것을 재지 않는다.**

### 2.3 잔고에는 2거래일 공표 지연이 있다

**이 family가 다른 수급 계열과 결정적으로 다른 지점이다.**

```sql
-- research/etl/quality.py:112
cutoff AS (
    SELECT MIN(session_idx) AS base_idx FROM calendar
    WHERE trade_date >= DATE '2016-06-30'
),
availability AS (
    SELECT MIN(c.trade_date) AS short_balance_available_from
    FROM calendar c, cutoff x
    WHERE c.session_idx >= x.base_idx + 2      -- ← short_balance_lag_sessions
)
```

`short_balance_is_available`이 이 규칙으로 정해지고, 그 이전은 값이 NULL이다.

**왜 2거래일인가.** 실측과 규정 양쪽에서 확인했다
(`research/output/horizon_scan/short_balance_publication_lag.json`, `status: verified`).

- `short_selling_balance_quantity`는 KRX의 `RPT_DUTY_OCCR_DD`(보고의무 발생일)를 키로
  삼는다. **공시일이 아니라 측정일**이다.
- 같은 fetch에서 잔고의 `max(trade_date)`가 거래량 계열보다 **일정한 세션 수만큼 뒤처진다**
  (2026-08-19 대 2026-08-21).
- 자본시장법 시행령 제14조의3이 순보유잔고 보고를 **보고의무 발생일 + 2영업일**까지로
  정한다.

**측정일 기준 값을 그날 알 수 있다고 가정하면 look-ahead가 된다.** 그래서 2거래일을
강제로 밀었다.

`readiness_dependencies`에도 명시돼 있다.

```yaml
# horizon_scan_config.yaml:381
readiness_dependencies: [feat_flow, dim_stock_pit_daily, short_balance_publication_lag, label_scan]
```

`flow_short_turnover`에는 `short_balance_publication_lag`도 `dim_stock_pit_daily`도 없다.

### 2.4 분모가 PIT 유통주식수다

[13_flow_foreign_holding_ratio_chg.md](13_flow_foreign_holding_ratio_chg.md) §2.3·§2.4와
같은 구조다.

- `dim_stock_pit_daily`에 의존하므로 그 커버리지가 표본을 제한한다
- **증자로 유통주식수가 늘면 공매도 잔고가 그대로여도 비율이 떨어진다.** 수급 계열에는
  기업행동 마스킹이 없다

### 2.5 제도 구간 필터

`short_regime = 'allowed'` 조건은 [14](14_flow_short_turnover.md) §2.2와 같다.
`allowed` 구간이 아니면 NULL이다.

**두 조건이 겹쳐 표본이 2016-07-01 ~ 2020-03-24, 909일이 된다.**

### 2.6 등록됐지만 안 돌린 것

```yaml
# horizon_scan_config.yaml:376
features:
  - {column: flow_short_interest_ratio, role: primary}
  - {column: flow_short_interest_ratio_chg_20d, role: secondary}
```

secondary로 **20일 변화율**이 등록돼 있는데 이번 run에는 primary 24행만 있다.
**수준과 변화 중 어느 쪽이 신호인지 확인하지 않았다.**

### 2.7 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/flow.py:253` |
| 잔고 공표 지연 | `research/etl/quality.py:112` |
| 지연 근거 문서 | `research/output/horizon_scan/short_balance_publication_lag.json` |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:371` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**공매도자의 정보 우위 가설이다.** [14](14_flow_short_turnover.md) §3.1과 같다.

다만 잔고 형태는 한 가지를 더 담는다. **지금 이 순간에도 포지션을 들고 있다**는 사실이다.
거래량은 하루의 판단이지만 잔고는 **유지되고 있는 판단**이다. 확신의 강도를 더 잘 반영한다는
논리다.

반대 방향 설명도 있다. 잔고가 크면 **숏 스퀴즈** 위험이 커져 오히려 가격이 튈 수 있다.
이 가설이라면 부호가 `+`가 된다. **사전등록은 정보 우위 쪽을 택해 `−`로 걸었다.**

### 3.2 기대 부호

`−`. 공매도 잔고 비율이 높을수록 이후 초과수익률 순위가 낮다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_config.yaml:379
primary_horizon_set: [20, 40, 60]
exploratory_horizon_set: [1, 2, 3, 5, 10, 120]
fdr_include: false
```

잔고는 며칠 단위로 잘 안 변하므로 짧은 horizon을 exploratory로 내렸다.
`flow_short_turnover`가 5일부터 본 것과 다르다.

| | 사전등록 primary | 실제 관측 |
|---|---|---|
| 밴드 | 20~60일 | **`null`** (`exploratory_only`) |
| 부호 | `−` | **`−` (일치)** |

### 3.4 왜 exploratory로 내렸나

[14](14_flow_short_turnover.md) §3.4와 같다. 제도 때문에 표본이 잘려 다중검정 모집단에
넣으면 다른 피처의 문턱까지 왜곡한다.

분류 좌표는 C3 × T0(수준) × U다.

### 3.5 근거 문헌

`02_feature_candidate.md` §3.2 F7. 공매도 잔고와 수익률 관계 연구.

---

## 4. 얼마나 효과가 있었나 — 진단값이다

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `lag1`)

**q·discovery·screen-pass는 전부 `—`다.** 아래는 진단값이다.

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이(정렬) |
|---|---|---:|---:|---:|---:|
| cum | 0→20 | −0.0524 | −0.510 | −4.24 | +0.79%p |
| cum | 0→40 | −0.0735 | −0.683 | −3.84 | +1.75%p |
| cum | 0→60 | **−0.0874** | −0.775 | −3.57 | **+2.60%p** |
| bucket | 10→20 | −0.0331 | −0.326 | −3.80 | +0.37%p |
| bucket | 20→40 | −0.0456 | −0.438 | −3.64 | +0.95%p |
| bucket | 40→60 | −0.0393 | −0.383 | −3.20 | +0.81%p |

### 4.2 공매도 4개 중 가장 강하다

| family | 대표 \|IC\| (0→60) | 5분위 차이 |
|---|---:|---:|
| **`flow_short_interest`** | **0.0874** | +2.60%p |
| `flow_days_to_cover` | 0.0672 | +3.46%p |
| `flow_short_turnover` | 0.0668 | **+4.40%p** |
| `flow_nat_proxy_20d` | 0.0644 | +2.53%p |

|IC|는 이 family가 1위인데 **5분위 수익률 차이는 3위다.** 같은 60일 horizon인데 순위가
다르다 — [06_px_idio_vol_60d.md](06_px_idio_vol_60d.md) §4.3에서 본 것과 같은 종류의
어긋남이다.

원인은 표본이 다르기 때문일 수 있다. `flow_short_turnover`는 1,420일, 이 family는 909일이다.
**같은 기간을 비교하는 게 아니다.**

### 4.3 horizon이 길수록 커진다

|IC|가 20일 0.052 → 60일 0.087로 단조 증가한다. 관측 범위 끝에서 최대다. 120일은
exploratory로 내려 확인하지 않았다.

### 4.4 신호의 모양

| 항목 | 값 |
|---|---|
| `pattern_auto` | **`exploratory_only`** |
| `candidate_horizon_band` | `null` |
| `peak_h_cum` | 60 (관측 범위 끝) |
| `onset_h` / `half_life_bucket` / `sign_flip_bucket` | 전부 `null` |

---

## 5. 진짜인가 — 강건성

### 5.1 기간 검정이 2구간뿐이다

- `valid_subperiods` = **2**
- `sign_consistent_subperiods` = **2**

2/2로 일치했지만 **두 구간밖에 없다.** 표본이 2016-07부터 2020-03까지라
`2017_2019` 중심이고 나머지는 부분적이다.

### 5.2 비중첩 offset — `insufficient`

| 항목 | 값 |
|---|---|
| 총 offset | 60개 |
| **`offset_status`** | **`insufficient`** |
| 부호 일치율 | **`null`** |

**검정이 완결되지 못했다.** 60일 horizon의 offset 격자를 909일 표본에 나누면 offset 하나당
유효일이 최소 기준(`stats.nonoverlap_min_dates: 20`)에 못 미친다.

`flow_short_turnover`(1,420일, `complete`, 일치율 1.0)와 갈리는 지점이다. **표본 길이가
검정 가능 여부를 결정했다.**

설정에는 이 상태의 등급 상한도 정해져 있다.

```yaml
decision:
  insufficient_offset_max_grade: B
```

### 5.3 거래 가능한 종목만 남겨도

| universe | IC (cum 0→60) | 유지율 |
|---|---:|---:|
| `broad` | −0.0874 | — |
| `tradable` | −0.0744 | **0.851** |

**공매도 4개 중 가장 높다.** `flow_short_turnover`의 0.571과 대조된다.

잔고 비율은 유통주식수로 나누므로 거래량 기반 지표보다 유동성 편향이 작은 것으로 보인다.
**같은 공매도 축이라도 산식에 따라 실행 가능성이 크게 다르다.**

### 5.4 생존편향

| sample_kind | IC (cum 0→60) |
|---|---:|
| `common_survivor` | −0.0874 |
| `available` | −0.0858 |

차이가 작고 방향이 같다.

### 5.5 지연 — 대상이 아니다

`native_ic`와 `lag1_ic`가 같고 `delay_pass`는 `null`이다. 사전등록 최소 horizon이 20일이라
h ≤ 5 cell이 없어 게이트 대상이 아니다.

**단, §2.3의 2거래일 공표 지연은 이미 데이터에 반영돼 있다.** 지연 게이트가 `null`인 것과
별개다. 이 family는 **실제로는 하루가 아니라 최소 사흘 늦은 정보**를 쓴다 (공표 2일 + lag1
1일).

### 5.6 시간 placebo — 대상이 아니다

`null`이다. exploratory 역할이라 placebo 대상 목록에 없다.

### 5.7 시장 구성

KOSPI 42.5% / KOSDAQ 57.5%. `flow_short_turnover`(46.7%)보다 KOSPI 비중이 낮다.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2016-07-01 ~ 2020-03-24** |
| 유효 거래일 | **909일** |
| 날짜당 평균 종목 수 | 803개 |

**3년 9개월치다.** 다른 34개 family가 10년 이상을 보는 것과 비교하면 매우 짧다.

시작이 2016-07-01인 이유가 둘이다.

1. 원천 `short_selling_balance_quantity` 자체가 2016-06-30부터 시작한다
   (`flow.py` 모듈 docstring: "Coverage asymmetry")
2. 거기에 2거래일 공표 지연을 더한다 (§2.3)

끝이 2020-03-24인 이유는 공매도 전면 금지다 (§2.5).

---

## 7. 중복성

**A×B 상관 산출물에 없다.** exploratory라 A primary 12 family에 안 들어간다.

### 확인하지 않은 중복

- `flow_short_turnover`와의 관계 — 같은 공매도 활동의 stock/flow 두 측면 (§2.2)
- **`flow_days_to_cover`와의 관계 — 산식상 분자가 같다.**
  `flow_short_interest = 잔고 / 유통주식수`, `flow_days_to_cover = 잔고 / 평균거래량`.
  **분자가 동일하고 분모만 다르다.** 강하게 겹칠 수밖에 없는데 재지 않았다.
- `flow_nat_proxy_20d`는 산식에 **이 피처를 그대로 포함한다**
  ([17](17_flow_nat_proxy_20d.md) §2.1). 사실상 부분집합 관계인데 별도 family로 세었다.

**공매도 4개를 독립된 네 개의 관찰로 세면 안 된다.**

---

## 8. 한계와 확인 못 한 것

1. **표본이 909일뿐이고 2020-03-24에서 끝난다** (§6).
2. **비중첩 offset 검정이 완결되지 못했다** (§5.2).
3. **기간 검정이 2구간뿐이다** (§5.1).
4. **BH·discovery 판정이 없다** (§4.1).
5. **잔고 계열 셋의 중복이 미확인이다** (§7). 특히 `flow_days_to_cover`와 분자가 같다.
6. **secondary(20일 변화)를 안 돌렸다** (§2.6). 수준과 변화 중 어느 쪽이 신호인지 모른다.
7. **숏 스퀴즈 가설을 검정하지 않았다** (§3.1). 잔고가 극단적으로 큰 구간만 따로 보면
   부호가 뒤집힐 수 있는데 확인하지 않았다.
8. **증자 등 기업행동 처리가 없다** (§2.4).
9. **실제 정보 지연이 사흘 이상이다** (§5.5). 실행 설계에서 반영해야 한다.
10. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
11. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나

**T1·T2 어느 후보에도 안 들어갔다.** exploratory 역할이라 대상이 아니다.

레짐이 안정된 구간이 쌓이면 새 config로 재등록한다
(`09_all_feature_results.md` §6).

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb, json
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
A=f"research/output/horizon_scan/phase=A/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260827T221729-4e0ae8b0"
print(duckdb.sql(f"""
  select family, scan_type, h_start, h_end, ic_mean, icir, t_nw,
         q5_spread_aligned, n_dates, effective_sample_start, effective_sample_end
  from '{A}/core/horizon_ic.parquet'
  where family='flow_short_interest' and universe='broad'
    and sample_kind='common_survivor'
  order by scan_type, h_end
""").df().to_string())
# 잔고 공표 지연 근거
print(json.dumps(json.load(open("research/output/horizon_scan/short_balance_publication_lag.json")),
                 ensure_ascii=False, indent=1))
PY
```

| 항목 | 위치 |
|---|---|
| cell 전체 | `phase=A/…/run_id=20260827T221729-4e0ae8b0/core/horizon_ic.parquet` |
| card (offset `insufficient`) | 같은 run의 `cards/family_cards.json` |
| 차트 7종 | 같은 run의 `plots/flow_short_interest_*.png` |
| 산식 | `research/etl/features/flow.py:253` |
| 공표 지연 규칙 | `research/etl/quality.py:112` |
| 공표 지연 근거 | `research/output/horizon_scan/short_balance_publication_lag.json` |
| 제도 구간 | `research/etl/quality.py:26` |
| exploratory 근거 | `01_feature_candidate/09_all_feature_results.md` §6 |
