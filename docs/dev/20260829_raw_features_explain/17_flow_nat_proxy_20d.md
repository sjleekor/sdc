# 17. `flow_nat_proxy_20d` — 외국인 매수·공매도 잔고 순위차 (NAT proxy)

- 작성일: 2026-08-29
- family: `flow_nat_proxy_20d` · primary feature: **`flow_nat_proxy_20d`**
- Phase A · fdr_family `flow` · role **`exploratory_short_regime`** · 기대 부호 `+` · 관측 부호 `+`
- 등급 **C** · `screen_pass` 실패 · **BH·discovery 대상이 아님** (`fdr_include: false`)
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**외국인이 사고 있으면서 공매도 잔고는 적은 종목을 고른다** (0→60일 IC +0.0644,
5분위 수익률 차이 +2.53%p). 기대 방향과 일치한다.

**35개 중 유일하게 두 신호를 조합해 만든 피처다.** 다른 34개가 하나의 원천에서 하나의 값을
뽑는 데 비해, 이건 **두 지표의 횡단면 백분위 순위를 빼서** 만든다. `02_feature_candidate.md`가
"(신규)"로 표시한 유일한 후보이기도 하다.

**그런데 구성 요소 중 하나가 이미 별도 family다.** 분모 쪽 `nat_asi_20`이
[15_flow_short_interest.md](15_flow_short_interest.md)의 `flow_short_interest_ratio`와
정의가 같다. **부분집합 관계인데 독립 family로 세었다.**

판정은 보류다. 표본 **889일**로 35개 중 가장 짧고, 2020-03-24에서 끝나며, 기간 검정 2구간,
비중첩 offset **`insufficient`**.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의 — 두 단계로 만든다

**1단계 — 두 재료를 만든다**

```sql
-- research/etl/features/flow.py:265
CASE WHEN short_regime = 'allowed'
          AND short_balance_is_available
          AND LAG(short_balance_is_available, 20) OVER w
     THEN foreign_holding_shares / NULLIF(float_shares_pit, 0)
          - LAG(foreign_holding_shares / NULLIF(float_shares_pit, 0), 20) OVER w
END AS nat_ahf_20,              -- 외국인 지분율 20일 변화

CASE WHEN short_regime = 'allowed' AND short_balance_is_available
     THEN short_selling_balance_quantity / NULLIF(float_shares_pit, 0)
END AS nat_asi_20               -- 공매도 잔고 비율
```

**2단계 — 날짜×시장 안에서 백분위 순위를 매겨 뺀다**

```sql
-- research/etl/features/flow.py:280
PERCENT_RANK() OVER (PARTITION BY trade_date, market ORDER BY nat_ahf_20)
- PERCENT_RANK() OVER (PARTITION BY trade_date, market ORDER BY nat_asi_20)
    AS flow_nat_proxy_20d
```

**"외국인 매수 순위 − 공매도 잔고 순위"**다. 범위는 −1 ~ +1이다.

- **+1에 가까우면** 외국인이 가장 많이 사고 있고 공매도 잔고는 가장 적다
- **−1에 가까우면** 외국인이 팔고 있고 공매도 잔고는 많다
- 0이면 두 순위가 같다

### 2.2 왜 이렇게 만드나

**두 정보가 같은 방향을 가리킬 때만 신호로 본다는 설계다.**

외국인 매수만 보면 그들이 틀릴 수 있다. 공매도 잔고만 보면 헤지 목적일 수 있다. **두
신호가 반대 방향을 가리키는 종목**(외국인도 사고 공매도도 많은 종목)은 값이 0 근처가 되어
중립으로 처리된다.

`02_feature_candidate.md` §1의 우선순위 표에 **4위**로 올라 있고, 근거 칸이 "(신규)"다.
기존 문헌을 옮긴 게 아니라 **이 프로젝트에서 만든 조합**이라는 뜻이다.

### 2.3 이 피처만 원시값이 아니라 순위다

**35개 중 산식 안에 이미 횡단면 순위 변환이 들어간 유일한 피처다.**

다른 피처는 원값을 저장하고 IC 계산 단계에서 순위를 매긴다
([00_읽는_법.md](00_읽는_법.md) §4.2). 이건 마트 단계에서 이미 `PERCENT_RANK()`를 쓴다.

결과가 둘이다.

1. **두 재료의 척도 차이가 상쇄된다.** 지분율 변화(−0.05~+0.05 수준)와 잔고 비율(0~0.1
   수준)은 단위가 달라 그냥 빼면 한쪽이 지배한다. 순위로 바꾸면 대등해진다.
2. **원래 크기 정보가 사라진다.** 외국인이 아주 많이 샀는지 조금 샀는지는 순위에 안 남는다.
   순서만 남는다.

그리고 순위를 매기는 그룹이 `PARTITION BY trade_date, market`이다. **IC 계산 때 쓰는 그룹과
같다.** 척도 처리가 두 번 겹치지만 순위의 순위는 순위이므로 결과는 바뀌지 않는다.

### 2.4 재료가 다른 family와 겹친다

| 재료 | 정의 | 대응하는 별도 family |
|---|---|---|
| `nat_ahf_20` | 외국인 지분율 20일 변화 | [13_flow_foreign_holding_ratio_chg.md](13_flow_foreign_holding_ratio_chg.md) |
| `nat_asi_20` | 공매도 잔고 / 유통주식수 | **[15_flow_short_interest.md](15_flow_short_interest.md)와 정의가 같다** |

**`nat_asi_20`은 `flow_short_interest_ratio`와 SQL이 동일하다.** 조건절
(`short_regime = 'allowed' AND short_balance_is_available`)까지 같다.

`nat_ahf_20`은 `flow_foreign_holding_ratio_chg_20d`에 조건이 하나 더 붙은 형태다
(`LAG(short_balance_is_available, 20)` — 20일 전에도 잔고를 알 수 있었어야 한다).

**즉 이 family는 다른 두 family의 조합이다.** 그런데 셋을 각각 별도 family로 세었고,
A×A 상관을 재지 않았다.

### 2.5 조건이 겹쳐 표본이 가장 짧다

세 조건이 모두 걸린다.

1. `short_regime = 'allowed'` — 2020-03-13에서 끊김
2. `short_balance_is_available` — 2016-06-30 + 2거래일 공표 지연
3. `LAG(short_balance_is_available, 20)` — **20일 전에도 잔고를 알 수 있었어야 함**
4. `float_shares_pit` 필요 — PIT 유통주식수 커버리지
5. 두 재료가 **모두 NOT NULL**이어야 함 (`WHERE nat_ahf_20 IS NOT NULL AND nat_asi_20 IS NOT NULL`)

결과가 **2016-07-29 ~ 2020-03-24, 889일, 날짜당 781종목**이다. **35개 family 중 유효
거래일과 날짜당 종목 수가 모두 가장 적다.**

`flow_short_interest`(2016-07-01 시작)보다 시작이 4주 늦은 게 3번 조건 때문이다.

### 2.6 정본 변형은 `lag1`

수급 계열 공통이다. 산출물 24행이 전부 `feature = flow_nat_proxy_20d_lag1`이다.
**실제 정보 지연은 사흘 이상이다** (공표 2일 + lag1 1일).

### 2.7 코드 위치

| 대상 | 경로 |
|---|---|
| 재료 산식 | `research/etl/features/flow.py:265` |
| 순위차 산식 | `research/etl/features/flow.py:280` |
| 공표 지연 | `research/etl/quality.py:112` |
| 제도 구간 | `research/etl/quality.py:26` |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:398` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**두 정보 우위 주체가 반대 방향을 볼 때를 걸러낸다는 설계다.**

- 외국인 지분율 상승 → 정보 있는 매수 → `+`
- 공매도 잔고 → 정보 있는 매도 → `−`

둘을 빼면 **"정보 있는 매수는 강하고 정보 있는 매도는 약한 종목"**이 최상위에 온다.

이름의 NAT는 net-attention 성격의 조합 지표를 뜻한다. 기존 문헌의 정형화된 지표가 아니라
**이 프로젝트가 한국 데이터 구조에 맞춰 만든 조합**이다 — 투자자 유형별 집계와 공매도 잔고가
둘 다 매일 공표되는 시장이라 가능한 형태다.

### 3.2 기대 부호

`+`. 순위차가 클수록 이후 초과수익률 순위가 높다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_config.yaml:404
primary_horizon_set: [20, 40, 60]
exploratory_horizon_set: [1, 2, 3, 5, 10, 120]
fdr_include: false
```

잔고 계열과 같다.

| | 사전등록 primary | 실제 관측 |
|---|---|---|
| 밴드 | 20~60일 | **`null`** (`exploratory_only`) |
| 부호 | `+` | **`+` (일치)** |

### 3.4 왜 exploratory로 내렸나

[14](14_flow_short_turnover.md) §3.4와 같다. 제도로 잘린 표본을 다중검정 모집단에 넣으면
다른 피처의 문턱까지 왜곡한다.

**공매도 잔고를 재료로 쓰기 때문에 제도 제약을 그대로 물려받는다.** 외국인 쪽 재료만 보면
2025년까지 볼 수 있는데, 조합했기 때문에 2020년에서 끊긴다.

분류 좌표는 C3 × T1 × U다.

### 3.5 근거 문헌

없다. `02_feature_candidate.md` §1의 근거 칸이 **"(신규)"**다. 신규 조합이라 사전 문헌이
없고, 그만큼 **검증 부담이 크다.**

---

## 4. 얼마나 효과가 있었나 — 진단값이다

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `lag1`)

**q·discovery·screen-pass는 전부 `—`다.**

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 |
|---|---|---:|---:|---:|---:|
| cum | 0→20 | +0.0315 | 0.397 | 3.34 | +0.72%p |
| cum | 0→40 | +0.0491 | 0.573 | 3.27 | +1.58%p |
| cum | 0→60 | **+0.0644** | 0.700 | 3.29 | **+2.53%p** |
| bucket | 10→20 | +0.0182 | 0.237 | 2.79 | +0.34%p |
| bucket | 20→40 | +0.0328 | 0.392 | 3.20 | +0.89%p |
| bucket | 40→60 | +0.0357 | 0.426 | 3.50 | +1.02%p |

### 4.2 조합이 재료보다 나은가

**이 질문에 답하는 것이 이 family의 존재 이유인데, 답할 수 없다.**

표본이 달라 직접 비교가 성립하지 않는다.

| family | IC (0→60) | 표본 기간 | 거래일 |
|---|---:|---|---:|
| **`flow_nat_proxy_20d`** (조합) | **+0.0644** | 2016-07-29 ~ 2020-03-24 | 889 |
| `flow_short_interest` (재료 2) | −0.0874 | 2016-07-01 ~ 2020-03-24 | 909 |
| `flow_foreign_holding_ratio_chg` (재료 1 계열) | −0.0042 | **2015-05-18 ~ 2025-02-05** | **2,370** |

부호를 맞춰 읽으면 재료 2(공매도 잔고)가 |0.087|로 조합(0.064)보다 강하다. 하지만
표본이 20일 다르고, 재료 1은 아예 다른 기간을 본다.

**조합이 재료 하나보다 나은지 확인하려면 같은 표본에서 셋을 나란히 재야 하는데 그 산출물이
없다.** §7의 A×A 상관 부재와 같은 문제다.

### 4.3 안정성은 공매도 4개 중 가장 좋다

| family | \|ICIR\| (0→60) | \|t(NW)\| 범위 |
|---|---:|---|
| `flow_short_interest` | 0.775 | 3.20 ~ 4.24 |
| **`flow_nat_proxy_20d`** | **0.700** | **2.79 ~ 3.50** |
| `flow_short_turnover` | 0.765 | 3.33 ~ 4.59 |
| `flow_days_to_cover` | 0.652 | 2.78 ~ 3.23 |

절대값은 중간이지만 **bucket cell에서 t가 horizon과 함께 커진다** (2.79 → 3.20 → 3.50).
다른 셋은 뒤로 갈수록 t가 줄어든다. 긴 구간에서 신호가 유지된다는 뜻이다.

### 4.4 신호의 모양

| 항목 | 값 |
|---|---|
| `pattern_auto` | **`exploratory_only`** |
| `candidate_horizon_band` | `null` |
| `peak_h_cum` | 60 (관측 범위 끝) |
| `onset_h` / `half_life_bucket` / `sign_flip_bucket` | 전부 `null` |

|IC|가 20일 0.031 → 60일 0.064로 단조 증가한다. 120일은 exploratory로 내려 확인하지 않았다.

---

## 5. 진짜인가 — 강건성

### 5.1 기간 검정이 2구간뿐이다

- `valid_subperiods` = **2**
- `sign_consistent_subperiods` = **2**

2/2 일치이지만 두 구간뿐이다.

### 5.2 비중첩 offset — `insufficient`

| 항목 | 값 |
|---|---|
| 총 offset | 60개 |
| **`offset_status`** | **`insufficient`** |
| 부호 일치율 | **`null`** |

889일 표본에 60일 offset 격자를 나누면 offset당 유효일이 최소 기준(20일)에 못 미친다.
**35개 중 표본이 가장 짧으므로 당연한 결과다.**

### 5.3 거래 가능한 종목만 남겨도

| universe | IC (cum 0→60) | 유지율 |
|---|---:|---:|
| `broad` | +0.0644 | — |
| `tradable` | +0.0546 | **0.847** |

공매도 4개 중 두 번째로 높다. **분모가 유통주식수인 두 피처
(이 family, `flow_short_interest`)의 유지율이 나란히 0.85 수준**이라는
[16_flow_days_to_cover.md](16_flow_days_to_cover.md) §5.3의 관찰과 맞는다.

### 5.4 생존편향

| sample_kind | IC (cum 0→60) |
|---|---:|
| `common_survivor` | +0.0644 |
| `available` | +0.0631 |

차이가 작고 방향이 같다.

### 5.5 지연 — 대상이 아니다

`native_ic`와 `lag1_ic`가 같고 `delay_pass`는 `null`이다. 최소 horizon이 20일이라 h ≤ 5
cell이 없다.

**실제 정보 지연은 사흘 이상이다** (§2.6).

### 5.6 시간 placebo — 대상이 아니다

`null`이다. exploratory 역할이라 placebo 대상 목록에 없다.

### 5.7 시장 구성

KOSPI 42.9% / KOSDAQ 57.1%.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2016-07-29 ~ 2020-03-24** |
| 유효 거래일 | **889일 (35개 중 최소)** |
| 날짜당 평균 종목 수 | **781개 (35개 중 최소)** |

**3년 8개월, 하루 781종목.** 조건 다섯 개가 겹친 결과다 (§2.5).

`flow_nat_proxy_20d`는 **조합 피처가 치르는 대가**를 보여 준다. 두 재료 중 어느 하나라도
없으면 값이 없다. 재료 각각의 커버리지 교집합만 남는다.

---

## 7. 중복성 — 부분집합을 별도 family로 세었다

**A×B 상관 산출물에 없다.** exploratory라 제외됐다.

### 확인하지 않은 중복 — 구조적으로 가장 심각하다

§2.4에서 본 대로 이 family는 **다른 두 family의 조합**이다.

```
flow_nat_proxy_20d = pctrank(nat_ahf_20) − pctrank(nat_asi_20)
                                              ↑
                          flow_short_interest_ratio 와 정의가 동일
```

**`nat_asi_20`은 `flow_short_interest_ratio`와 SQL이 같다.** 순위 변환만 거쳤다.
Spearman 상관은 단조 변환에 불변이므로, 다른 항이 없다면 두 피처의 IC는 부호만 뒤집힌
같은 값이어야 한다.

실제로는 `nat_ahf_20` 항이 더해져 값이 달라진다. 그 차이가 **조합의 순수 기여**인데,
**A×A 상관이 없어 얼마인지 모른다.**

이건 다중검정 관점에서도 문제다. 결합 BH가 이 family를 별도 가설로 세면
**같은 정보를 두 번 세는 셈**이 된다. 다만 공매도 4개는 `fdr_include: false`라 실제 BH
모집단에는 안 들어갔으므로, **이번 결과에 한해서는 문턱을 왜곡하지 않았다.**

**재등록할 때는 이 중복을 먼저 정리해야 한다.**

---

## 8. 한계와 확인 못 한 것

1. **표본이 889일로 35개 중 가장 짧고 2020-03-24에서 끝난다** (§6).
2. **`flow_short_interest`를 재료로 포함한다** (§2.4, §7). 부분집합을 별도 family로 센
   구조인데 상관을 재지 않았다. **가장 시급한 후속 작업이다.**
3. **조합이 재료보다 나은지 확인하지 못했다** (§4.2). 표본이 달라 직접 비교가 성립하지
   않는다. 같은 표본에서 재료·조합을 나란히 재는 실험이 필요하다.
4. **비중첩 offset 검정이 완결되지 못했다** (§5.2).
5. **기간 검정이 2구간뿐이다** (§5.1).
6. **BH·discovery 판정이 없다** (§4.1).
7. **사전 문헌이 없다** (§3.5). 신규 조합이라 외부 검증 기준이 없다. 그만큼 자체 검증
   부담이 큰데 표본이 가장 짧다.
8. **순위 변환으로 크기 정보를 버린다** (§2.3). 두 재료를 원값 가중합으로 조합했을 때와
   비교하지 않았다.
9. **가중치를 1:1로 고정했다** — 두 순위를 그냥 뺀다. 다른 비율이 나은지 확인하지 않았다.
10. **실제 정보 지연이 사흘 이상이다** (§5.5).
11. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
12. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나

**T1·T2 어느 후보에도 안 들어갔다.** exploratory 역할이라 대상이 아니다.

`09_all_feature_results.md` §6의 결론이 그대로 적용된다.

> 레짐이 안정된 구간이 충분히 쌓이면 새 config로 사전등록해서 다시 본다.
> **지금 숫자가 좋아 보인다고 그대로 쓰면 안 된다.**

이 family는 여기에 §7의 중복 정리까지 더해야 한다.

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
A=f"research/output/horizon_scan/phase=A/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260827T221729-4e0ae8b0"
# 공매도 4 family 의 표본 차이 — 조합 피처가 가장 짧다
print(duckdb.sql(f"""
  select family,
         min(effective_sample_start) as start,
         max(effective_sample_end)   as "end",
         max(n_dates) as n_dates,
         round(avg(n_obs_mean)) as names_per_date
  from '{A}/core/horizon_ic.parquet'
  where hypothesis_role='exploratory_short_regime'
    and universe='broad' and sample_kind='common_survivor'
  group by family order by n_dates
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| cell 전체 | `phase=A/…/run_id=20260827T221729-4e0ae8b0/core/horizon_ic.parquet` |
| card (offset `insufficient`) | 같은 run의 `cards/family_cards.json` |
| 차트 7종 | 같은 run의 `plots/flow_nat_proxy_20d_*.png` |
| 재료 산식 | `research/etl/features/flow.py:265` |
| 순위차 산식 | `research/etl/features/flow.py:280` |
| 신규 후보 등록 | `01_feature_candidate/02_feature_candidate.md` §1 (4위, 근거 "(신규)") |
| exploratory 근거 | `01_feature_candidate/09_all_feature_results.md` §6 |
