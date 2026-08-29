# 16. `flow_days_to_cover` — 공매도 잔고 청산 소요일

- 작성일: 2026-08-29
- family: `flow_days_to_cover` · primary feature: **`flow_days_to_cover`**
- Phase A · fdr_family `flow` · role **`exploratory_short_regime`** · 기대 부호 `−` · 관측 부호 `−`
- 등급 **C** · `screen_pass` 실패 · **BH·discovery 대상이 아님** (`fdr_include: false`)
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**공매도 잔고를 평소 거래량으로 다 사려면 며칠 걸리는가**를 잰다 (0→60일 IC −0.0672,
5분위 수익률 차이 +3.46%p). 기대 방향과 일치한다.

**[15_flow_short_interest.md](15_flow_short_interest.md)와 분자가 같다.** 둘 다 공매도 잔고를
쓰고 **분모만 다르다** — 저쪽은 유통주식수, 이쪽은 20일 평균 거래량. 그래서 두 값은 구조적으로
겹칠 수밖에 없는데 **상관을 재지 않았다.**

판정은 보류다. 표본 909일, 2020-03-24에서 끝남, 기간 검정 2구간, 비중첩 offset
**`insufficient`**.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/flow.py:259
CASE WHEN short_regime = 'allowed' AND short_balance_is_available
     THEN short_selling_balance_quantity / NULLIF(AVG(total_volume) OVER w20, 0) END
    AS flow_days_to_cover

-- w20 = ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
```

**공매도 잔고 주식 수를 최근 20거래일 평균 거래량으로 나눈 값**이다.

단위가 **일(day)**이다.

- 3이면 평소 거래량으로 잔고를 다 되사는 데 3일 걸린다
- 값이 클수록 청산이 오래 걸린다 = 포지션이 거래량에 비해 무겁다

영어권에서 "days to cover" 또는 "short ratio"로 부르는 지표다.

### 2.2 잔고 비율과 무엇이 다른가 — 분모만 다르다

```
flow_short_interest = 공매도잔고 / 유통주식수
flow_days_to_cover  = 공매도잔고 / 20일 평균거래량
                       ↑ 분자가 같다
```

**분자가 완전히 동일하다.** 차이는 "무엇에 비해 큰가"뿐이다.

| | `flow_short_interest` | **`flow_days_to_cover`** |
|---|---|---|
| 분모 | 유통주식수 (PIT) | 20일 평균 거래량 |
| 단위 | 비율 (%) | **일(day)** |
| 재는 것 | 발행주식 대비 공매도 규모 | **청산 난이도** |
| 성격 | 포지션 크기 | **유동성 대비 부담** |

거래가 활발한 종목은 잔고가 커도 며칠이면 청산된다. 거래가 적은 종목은 작은 잔고도 오래
걸린다. **이 피처는 유동성을 분모에 넣어 그 차이를 잡는다.**

**그래서 유동성 축과 얽힌다.** 분모가 거래량이므로
[07_px_amihud_20d.md](07_px_amihud_20d.md)가 겪은 문제 — 거래량 기반 지표가 규모를 대리하는
현상 — 이 여기서도 나타날 수 있다. `11_feature_taxonomy.md` §2.1이 이 family를
`px_amihud_20d`·`px_zero_ret_ratio_20d`와 함께 **C8(실행·거래비용)**에 배치한 이유다.

**그런데 규모와의 상관을 재지 않았다.**

### 2.3 잔고 계열 공통 제약

[15_flow_short_interest.md](15_flow_short_interest.md) §2.3·§2.5와 동일하다.

- **2거래일 공표 지연** (`short_balance_is_available`) — 근거는
  `research/output/horizon_scan/short_balance_publication_lag.json` (`status: verified`)
- **`short_regime = 'allowed'` 구간만** 계산
- 원천 잔고가 **2016-06-30부터** 시작

세 조건이 겹쳐 표본이 **2016-07-01 ~ 2020-03-24, 909일**이 된다.

`readiness_dependencies`가 `flow_short_interest`보다 하나 짧다.

```yaml
# horizon_scan_config.yaml:392
readiness_dependencies: [feat_flow, short_balance_publication_lag, label_scan]
```

**`dim_stock_pit_daily`가 없다.** 분모가 유통주식수가 아니라 거래량이라 PIT 주식수가
필요 없기 때문이다. 그 덕에 날짜당 종목 수가 856개로 `flow_short_interest`(803개)보다 53개
많다.

### 2.4 정본 변형은 `lag1`

수급 계열 공통이다. 산출물 24행이 전부 `feature = flow_days_to_cover_lag1`이다.

**실제 정보 지연은 사흘 이상이다** — 공표 2일 + lag1 1일.

### 2.5 secondary가 없다

```yaml
features: [{column: flow_days_to_cover, role: primary}]
```

이 family만 primary 하나뿐이다. 변화율 형태도, 다른 창 길이도 등록하지 않았다.

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/flow.py:259` |
| 공표 지연 | `research/etl/quality.py:112` |
| 제도 구간 | `research/etl/quality.py:26` |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:386` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**두 갈래가 겹친다.**

**첫째, 공매도자의 정보 우위**다. [14](14_flow_short_turnover.md) §3.1과 같다.

**둘째, 청산 부담이다.** days to cover가 크면 공매도자가 빠져나오기 어렵다. 그만큼 확신이
있어야 그 포지션을 잡는다는 논리다. 잔고 비율보다 "얼마나 진지한 포지션인가"를 잘 잡는다는
주장이다.

**반대 가설도 있다.** days to cover가 크면 숏 스퀴즈가 났을 때 청산 매수가 거래량 대비
과도해져 가격이 급등한다. 이 가설이라면 부호가 `+`다. 실제로 영어권에서 days to cover는
**스퀴즈 위험 지표로 더 자주 쓰인다.**

**사전등록은 정보 우위 쪽을 택해 `−`로 걸었다.**

### 3.2 기대 부호

`−`. 청산 소요일이 길수록 이후 초과수익률 순위가 낮다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_config.yaml:390
primary_horizon_set: [20, 40, 60]
exploratory_horizon_set: [1, 2, 3, 5, 10, 120]
fdr_include: false
```

`flow_short_interest`와 같다. 잔고 계열은 며칠 단위로 잘 안 변한다.

| | 사전등록 primary | 실제 관측 |
|---|---|---|
| 밴드 | 20~60일 | **`null`** (`exploratory_only`) |
| 부호 | `−` | **`−` (일치)** |

### 3.4 왜 exploratory로 내렸나

[14](14_flow_short_turnover.md) §3.4와 같다. 제도로 잘린 표본을 다중검정 모집단에 넣으면
다른 피처의 문턱까지 왜곡한다.

분류 좌표는 C3 / **C8** × T0 × U다. `px_amihud_20d`처럼 두 카테고리에 걸친다.

### 3.5 근거 문헌

`02_feature_candidate.md` §3.2 F7 계열.

---

## 4. 얼마나 효과가 있었나 — 진단값이다

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `lag1`)

**q·discovery·screen-pass는 전부 `—`다.**

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이(정렬) |
|---|---|---:|---:|---:|---:|
| cum | 0→20 | −0.0355 | −0.398 | −3.23 | +1.12%p |
| cum | 0→40 | −0.0535 | −0.548 | −3.09 | +2.30%p |
| cum | 0→60 | **−0.0672** | −0.652 | −3.04 | **+3.46%p** |
| bucket | 10→20 | −0.0217 | −0.253 | −2.92 | +0.62%p |
| bucket | 20→40 | −0.0338 | −0.380 | −3.08 | +1.36%p |
| bucket | 40→60 | −0.0299 | −0.342 | −2.78 | +1.32%p |

### 4.2 |IC|는 3위인데 수익률 차이는 2위다

| family | \|IC\| (0→60) | 5분위 차이 | 표본일 |
|---|---:|---:|---:|
| `flow_short_interest` | 0.0874 | +2.60%p | 909 |
| **`flow_days_to_cover`** | **0.0672** | **+3.46%p** | 909 |
| `flow_short_turnover` | 0.0668 | +4.40%p | 1,420 |
| `flow_nat_proxy_20d` | 0.0644 | +2.53%p | 889 |

**`flow_short_interest`와 표본이 완전히 같은데(909일) 순위가 뒤바뀐다.** |IC|는 저쪽이
30% 크고, 5분위 수익률 차이는 이쪽이 33% 크다.

두 지표가 다른 것을 잰다는 증거다. 분자가 같으므로 **차이는 전적으로 분모에서 온다.**
거래량으로 나눈 쪽(이 family)이 극단 분위에서 더 큰 수익 차이를 만든다.

**해석은 두 갈래이고 이 자료로는 못 가른다.**

- 유동성 대비 부담이 실제로 더 중요한 정보다
- 분모가 거래량이라 유동성 프리미엄(`px_amihud_20d` 계열)이 섞여 들어왔다

§2.2에서 지적한 대로 **규모·유동성 축과의 상관을 재지 않아** 후자를 배제할 수 없다.

### 4.3 t값이 가장 작다

공매도 4개 중 t(NW) 절대값이 가장 작다 (−2.78 ~ −3.23). `flow_short_turnover`는
−3.33 ~ −4.59다.

표본은 `flow_short_interest`와 같은데 t가 더 작다는 건 **일별 IC의 변동이 크다**는 뜻이다.
ICIR도 −0.65로 `flow_short_interest`(−0.77)보다 낮다.

### 4.4 신호의 모양

| 항목 | 값 |
|---|---|
| `pattern_auto` | **`exploratory_only`** |
| `candidate_horizon_band` | `null` |
| `peak_h_cum` | 60 (관측 범위 끝) |
| `onset_h` / `half_life_bucket` / `sign_flip_bucket` | 전부 `null` |

|IC|가 20일 → 60일로 단조 증가한다. 120일은 exploratory로 내려 확인하지 않았다.

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

909일 표본에 60일 offset 격자를 나누면 offset당 유효일이 최소 기준
(`stats.nonoverlap_min_dates: 20`)에 못 미친다.

`flow_short_turnover`(1,420일)만 `complete`다.

### 5.3 거래 가능한 종목만 남기면 — 32% 사라진다

| universe | IC (cum 0→60) | 유지율 |
|---|---:|---:|
| `broad` | −0.0672 | — |
| `tradable` | −0.0460 | **0.684** |

공매도 4개 중 두 번째로 낮다.

| family | 유지율 |
|---|---:|
| `flow_short_interest` | 0.851 |
| `flow_nat_proxy_20d` | 0.847 |
| **`flow_days_to_cover`** | **0.684** |
| `flow_short_turnover` | 0.571 |

**분모가 거래량인 두 피처(이 family, `flow_short_turnover`)의 유지율이 나란히 낮다.**
분모가 유통주식수인 둘(`flow_short_interest`, `flow_nat_proxy_20d`)은 0.85 수준이다.

§2.2·§4.2에서 제기한 유동성 혼입 의심과 방향이 맞는 정황이다. **거래량을 분모에 넣은
지표는 거래가 적은 종목에서 극단값을 갖고, 그 종목들이 tradable 필터에 걸려 빠진다.**

### 5.4 생존편향

| sample_kind | IC (cum 0→60) |
|---|---:|
| `common_survivor` | −0.0672 |
| `available` | −0.0644 |

차이가 작고 방향이 같다.

### 5.5 지연 — 대상이 아니다

`native_ic`와 `lag1_ic`가 같고 `delay_pass`는 `null`이다. 최소 horizon이 20일이라 h ≤ 5
cell이 없다.

**실제 정보 지연은 사흘 이상이다** (§2.4).

### 5.6 시간 placebo — 대상이 아니다

`null`이다. exploratory 역할이라 placebo 대상 목록에 없다.

### 5.7 시장 구성

KOSPI 45.4% / KOSDAQ 54.6%. `flow_short_turnover`(46.7%) 다음으로 KOSPI 비중이 높다.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2016-07-01 ~ 2020-03-24** |
| 유효 거래일 | **909일** |
| 날짜당 평균 종목 수 | **856개** |

`flow_short_interest`와 기간이 같고 **종목이 53개 많다.** §2.3에서 본 대로 PIT 유통주식수에
의존하지 않기 때문이다.

---

## 7. 중복성

**A×B 상관 산출물에 없다.** exploratory라 제외됐다.

### 확인하지 않은 중복 — 이 family에서 가장 큰 공백

**`flow_short_interest`와 분자가 완전히 같다** (§2.2). 두 피처의 상관은 결국
"유통주식수와 20일 평균 거래량이 횡단면에서 얼마나 비슷하게 움직이나"로 결정된다.
그 값을 재지 않았다.

**규모·유동성 축과의 관계도 모른다.** §5.3의 유지율 패턴이 혼입을 시사하는데
`px_amihud_20d`·`fin_log_mcap`과의 상관이 없다.

`flow_nat_proxy_20d`는 산식에 `flow_short_interest`를 포함하므로 이 family와도 간접적으로
얽힌다 ([17](17_flow_nat_proxy_20d.md) §2.1).

**공매도 4개를 독립된 관찰로 세면 안 된다.**

---

## 8. 한계와 확인 못 한 것

1. **표본이 909일이고 2020-03-24에서 끝난다** (§6).
2. **비중첩 offset 검정이 완결되지 못했다** (§5.2).
3. **기간 검정이 2구간뿐이다** (§5.1).
4. **BH·discovery 판정이 없다** (§4.1).
5. **`flow_short_interest`와 분자가 같은데 상관을 안 쟀다** (§7). 가장 시급하다.
6. **유동성 혼입 가능성을 배제하지 못한다** (§2.2, §4.2, §5.3). 분모가 거래량이라
   `px_amihud_20d` 계열과 얽힐 수 있는데 확인하지 않았다.
7. **숏 스퀴즈 가설을 검정하지 않았다** (§3.1). days to cover는 영어권에서 스퀴즈 지표로
   더 자주 쓰이는데 반대 방향 검정을 하지 않았다.
8. **secondary 변형이 없다** (§2.5).
9. **실제 정보 지연이 사흘 이상이다** (§5.5).
10. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
11. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나

**T1·T2 어느 후보에도 안 들어갔다.** exploratory 역할이라 대상이 아니다.

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
A=f"research/output/horizon_scan/phase=A/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260827T221729-4e0ae8b0"
# 분자가 같은 두 피처를 나란히 본다
print(duckdb.sql(f"""
  select family, scan_type, h_end, ic_mean, icir, t_nw, q5_spread_aligned,
         n_dates, n_obs_mean
  from '{A}/core/horizon_ic.parquet'
  where family in ('flow_short_interest','flow_days_to_cover')
    and universe='broad' and sample_kind='common_survivor' and scan_type='cum'
  order by h_end, family
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| cell 전체 | `phase=A/…/run_id=20260827T221729-4e0ae8b0/core/horizon_ic.parquet` |
| card (offset `insufficient`) | 같은 run의 `cards/family_cards.json` |
| 차트 7종 | 같은 run의 `plots/flow_days_to_cover_*.png` |
| 산식 | `research/etl/features/flow.py:259` |
| 공표 지연 규칙·근거 | `research/etl/quality.py:112`, `research/output/horizon_scan/short_balance_publication_lag.json` |
| C8 분류 근거 | `01_feature_candidate/11_feature_taxonomy.md` §2.1 |
| exploratory 근거 | `01_feature_candidate/09_all_feature_results.md` §6 |
