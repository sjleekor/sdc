# 14. `flow_short_turnover` — 공매도 거래 비중

- 작성일: 2026-08-29
- family: `flow_short_turnover` · primary feature: **`flow_short_turnover_20d`**
- Phase A · fdr_family `flow` · role **`exploratory_short_regime`** · 기대 부호 `−` · 관측 부호 `−`
- 등급 **C** · `screen_pass` 실패 · **BH·discovery 대상이 아님** (`fdr_include: false`)
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**기대 방향으로 나왔고 크기도 작지 않은데, 판정을 보류했다** (0→60일 IC −0.0668,
5분위 수익률 차이 +4.40%p).

**탈락이 아니다. 애초에 후보 풀에 넣지 않았다.**

이유는 제도다. 2020년 3월 공매도 금지로 표본이 **2020-03-24에서 잘렸다.** 유효 거래일이
1,420일이고 기간 검정도 5구간이 아니라 **2구간**밖에 안 나온다. 이 상태로 다중검정 모집단에
넣으면 **다른 피처들의 문턱까지 왜곡한다.**

거래가능 유지율 **0.571**은 35개 family 중 최저다. 공매도가 활발한 종목이 거래가능 universe와
어긋난다는 뜻이다.

**지금 숫자가 좋아 보인다고 그대로 쓰면 안 된다.**

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/flow.py:248
CASE WHEN short_regime = 'allowed'
          AND COUNT(total_volume) OVER w20 = 20
          AND COUNT(short_selling_volume) OVER w20 = 20
     THEN SUM(short_selling_volume) OVER w20
          / NULLIF(SUM(total_volume) OVER w20, 0) END
    AS flow_short_turnover_20d
```

**최근 20거래일 공매도 거래량을 같은 기간 총 거래량으로 나눈 값**이다.

- 0.05면 그 종목 거래의 5%가 공매도였다
- 값이 클수록 공매도 압력이 강하다

`flow_*_netbuy_to_volume` 계열과 같은 형태(거래량 대비 비율)라 규모가 상쇄된다.

### 2.2 `short_regime = 'allowed'` 조건이 표본을 결정한다

**이 family의 모든 것이 이 한 줄에서 나온다.**

공매도 제도 구간은 코드에 고정돼 있다.

```python
# research/etl/quality.py:26
SHORT_REGIMES = (
    ShortRegime("2014-06-01", "2020-03-13", "allowed"),
    ShortRegime("2020-03-16", "2021-05-02", "banned"),    # 코로나 전면 금지
    ShortRegime("2021-05-03", "2023-11-03", "partial"),   # 대형주만 부분 재개
    ShortRegime("2023-11-06", "2025-03-28", "banned"),    # 전면 금지 재시행
    ShortRegime("2025-03-31", None,         "allowed"),   # 재개
)
```

**`allowed`가 아닌 구간은 값이 NULL이다.** `partial`(부분 허용)도 제외한다 — 대형주만
공매도가 가능한 구간이라 횡단면 비교가 성립하지 않기 때문이다.

그 결과 유효 표본이 **2014-06-03 ~ 2020-03-24, 1,420일**이다. scan 전체 표본
(2014-06 ~ 2025-02)의 **절반 남짓**이다.

2025-03-31부터 다시 `allowed`인데 **이번 표본에 거의 안 잡힌다.** `common_survivor` 표본이
120일 라벨을 요구해 formation이 2025-02-05에서 끝나기 때문이다.

### 2.3 완전 창을 요구한다

`COUNT(total_volume) = 20 AND COUNT(short_selling_volume) = 20`이다. 20일 창이 다 차야
계산한다. 제도 전환 직후 며칠은 창이 못 차서 추가로 빠진다.

### 2.4 정본 변형은 `lag1`

수급 계열 공통 규칙이다. 산출물 행이 전부
`feature = flow_short_turnover_20d_lag1`이다.

### 2.5 잔고 계열과 다른 점 — 공표 지연이 없다

이 family는 `short_selling_volume`(당일 공매도 거래량)을 쓴다. **거래 체결량이라 잔고와
달리 공표 지연 문제가 없다.**

같은 공매도 묶음의 나머지 셋([15](15_flow_short_interest.md), [16](16_flow_days_to_cover.md),
[17](17_flow_nat_proxy_20d.md))은 **잔고**를 쓰기 때문에 2거래일 공표 지연을 따로 처리해야
하고, 그래서 `readiness_dependencies`에 `short_balance_publication_lag`가 붙는다. 이
family에는 없다.

```yaml
# horizon_scan_config.yaml:365
readiness_dependencies: [feat_flow, label_scan]   # 잔고 계열보다 짧다
```

**그 덕에 표본이 1,420일로 나머지 셋(889~909일)보다 훨씬 길다.**

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/flow.py:248` |
| 제도 구간 정의 | `research/etl/quality.py:26` |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:358` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**공매도자는 정보가 있다는 가설이다.**

공매도는 비용도 위험도 크다. 빌린 주식에 이자를 내야 하고, 주가가 오르면 손실이 무한하다.
그 부담을 감수하고 파는 쪽은 **근거가 있다**고 본다. 그래서 공매도가 몰린 종목은 이후
부진하다.

### 3.2 기대 부호

`−`. 공매도 거래 비중이 높을수록 이후 초과수익률 순위가 낮다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_config.yaml:363
primary_horizon_set: [5, 10, 20, 40, 60]
exploratory_horizon_set: [1, 2, 3, 120]
include_bucket_primary: true
fdr_include: false            # ← BH 모집단 제외
```

**primary가 5개로 공매도 4개 중 가장 넓다.** 나머지 셋은 [20, 40, 60]이다. 표본이 길어
짧은 horizon도 볼 수 있었기 때문이다.

`fdr_include: false`가 핵심이다 — cell을 계산하되 **결합 BH 모집단에는 넣지 않는다.**

| | 사전등록 primary | 실제 관측 |
|---|---|---|
| 밴드 | 5~60일 | **`null`** (`exploratory_only`) |
| 부호 | `−` | **`−` (일치)** |

부호는 맞았는데 `candidate_horizon_band`가 `null`이다. **exploratory 역할이라 후보 밴드를
아예 만들지 않는다.**

### 3.4 왜 exploratory로 내렸나

`09_all_feature_results.md` §6이 이유를 적었다.

> **네 개 전부 기대 부호와 일치하고 크기도 작지 않다.** 그런데 등급은 C다. 탈락이 아니라
> **애초에 후보 풀에 넣지 않았기 때문**이다. …
> 이 상태로 FDR 모집단에 넣으면 **다른 21개 피쳐의 문턱까지 왜곡**한다.

BH는 p값들을 함께 정렬해 문턱을 정한다. 표본 조건이 다른 cell을 섞으면 **다른 family의
판정까지 흔들린다.** 그래서 계산은 하되 모집단에서 뺐다.

분류 좌표는 C3(수급·소유) × T0(수준) × U다.

### 3.5 근거 문헌

Diether, Lee & Werner (2009) 등 공매도 정보성 연구. `02_feature_candidate.md` §3.2 F6.

---

## 4. 얼마나 효과가 있었나 — 진단값이다

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `lag1`)

**q·discovery·screen-pass는 전부 `—`다.** BH 모집단에 없으므로 계산되지 않았다.
아래 숫자는 **진단값으로만** 읽어야 한다.

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이(정렬) |
|---|---|---:|---:|---:|---:|
| cum | 0→5 | −0.0151 | −0.184 | −3.85 | +0.42%p |
| cum | 0→10 | −0.0222 | −0.274 | −4.04 | +0.82%p |
| cum | 0→20 | −0.0348 | −0.427 | −4.45 | +1.58%p |
| cum | 0→40 | −0.0526 | −0.630 | −4.59 | +3.03%p |
| cum | 0→60 | **−0.0668** | −0.765 | −4.54 | **+4.40%p** |
| bucket | 0→5 | −0.0151 | −0.184 | −3.85 | +0.42%p |
| bucket | 5→10 | −0.0130 | −0.159 | −3.33 | +0.40%p |
| bucket | 10→20 | −0.0200 | −0.246 | −3.62 | +0.78%p |
| bucket | 20→40 | −0.0341 | −0.429 | −4.45 | +1.51%p |
| bucket | 40→60 | −0.0325 | −0.406 | −4.19 | +1.51%p |

### 4.2 크기가 작지 않다

60일 기준 5분위 차이 **+4.40%p**는 같은 60일 horizon에서 `px_idio_vol_60d`(+2.99%p)나
`px_maxret_20d`(+1.79%p)보다 크다.

**그래서 더 조심해야 한다.** 이 숫자는 **공매도가 정상적으로 허용되던 2014~2020년 구간의
값**이다. 그 구간이 다른 피처들과 같은 조건인지 확인되지 않았다.

### 4.3 horizon이 길수록 커진다

|IC|가 5일 0.015 → 60일 0.067로 단조 증가한다. 관측 범위 끝에서 최대다. 120일은
exploratory로 내려 확인하지 않았다.

### 4.4 신호의 모양

| 항목 | 값 |
|---|---|
| `pattern_auto` | **`exploratory_only`** |
| `candidate_horizon_band` | `null` |
| `onset_h` | `null` |
| `peak_h_cum` | 60 (관측 범위 끝) |
| `half_life_bucket` | `null` |
| `sign_flip_bucket` | `null` |

`exploratory_only`는 **신호가 없다는 뜻이 아니라 후보 판정을 하지 않았다는 뜻이다.**
[02_px_mom_12_1.md](02_px_mom_12_1.md)의 `no_signal`과 구분해야 한다.

---

## 5. 진짜인가 — 강건성

### 5.1 기간 검정이 2구간뿐이다

- `valid_subperiods` = **2**
- `sign_consistent_subperiods` = **2**

**2/2로 일치했지만 2구간밖에 없다.** 다른 family가 5구간인 것과 다르다.

표본이 2020-03-24에서 끝나므로 다섯 기간 중 `2014_2016`, `2017_2019`만 유효하고
`2020_2021` 이후는 거의 비어 있다.

**"두 구간 전부 일치"를 "다섯 구간 전부 일치"와 같은 무게로 읽으면 안 된다.** 검정력이
절반 이하다.

### 5.2 비중첩 offset — 60개 전부 일치

| 항목 | 값 |
|---|---|
| 총 offset | 60개 (전부 유효) |
| **부호 일치율** | **1.0** |
| `offset_status` | **`complete`** |

**공매도 4개 중 유일하게 offset 검정이 완결됐다.** 나머지 셋은 `insufficient`다(§7 비교).
표본이 1,420일로 길어 60개 offset을 다 만들 수 있었다.

### 5.3 거래 가능한 종목만 남기면 — 43% 사라진다

| universe | IC (cum 0→60) | 유지율 |
|---|---:|---:|
| `broad` | −0.0668 | — |
| `tradable` | −0.0381 | **0.571** |

**35개 family 중 최저다.** 게이트 기준 0.50을 겨우 넘는다.

`09_all_feature_results.md` §6의 지적 그대로다.

> `flow_short_turnover`의 tradable 유지율이 0.57로 특히 낮다. 공매도가 활발한 종목이
> 거래가능 universe와 어긋난다는 뜻이다.

거래대금 1억원·종가 1,000원 필터를 걸면 신호의 절반 가까이가 사라진다. **실행 가능성 측면의
경고다.**

### 5.4 생존편향

| sample_kind | IC (cum 0→60) |
|---|---:|
| `common_survivor` | −0.0668 |
| `available` | −0.0645 |

차이가 작고 방향이 같다.

### 5.5 지연 — 통과

`native_ic`와 `lag1_ic`가 같고 `delay_pass = true`다. 정본이 `lag1`이라 자기 자신과
비교하며, h ≤ 5 cell의 `p_nw`가 0.05 미만이라 조건을 만족했다.

**공매도 4개 중 유일하게 이 게이트를 받았다.** 나머지 셋은 사전등록 최소 horizon이 20일이라
대상이 아니다.

### 5.6 시간 placebo — 대상이 아니다

`null`이다. exploratory 역할이라 placebo 대상 목록에 들어가지 않는다.

### 5.7 시장 구성 — KOSPI 비중이 높다

- KOSPI **46.7%** / KOSDAQ 53.3%

35개 중 KOSPI 비중이 가장 높은 축이다. 다른 수급 family(41.5%)보다 5%p 높다.
**공매도는 대형주에 집중된다**는 사실이 반영된 것으로 보이지만, 확인하지 않았다.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2014-06-03 ~ 2020-03-24** |
| 유효 거래일 | **1,420일** |
| 날짜당 평균 종목 수 | 814개 |

**끝나는 날짜가 2020-03-24다.** 다른 34개 family가 2025-02-05까지 가는 것과 결정적으로
다르다.

공매도 4개 표본을 나란히 놓으면 이렇다.

| family | 유효 시작 | 유효 끝 | 거래일 | 날짜당 종목 |
|---|---|---|---:|---:|
| **`flow_short_turnover`** | **2014-06-03** | 2020-03-24 | **1,420** | 814 |
| `flow_short_interest` | 2016-07-01 | 2020-03-24 | 909 | 803 |
| `flow_days_to_cover` | 2016-07-01 | 2020-03-24 | 909 | 856 |
| `flow_nat_proxy_20d` | 2016-07-29 | 2020-03-24 | 889 | 781 |

**이 family만 2014년부터 시작한다.** §2.5에서 본 대로 잔고가 아니라 거래량을 쓰기 때문이다.
공매도 잔고(`short_selling_balance_quantity`)는 2016-06-30부터만 있다.

---

## 7. 중복성

**A×B 상관 산출물에 이 family가 없다.** `primary_feature_rank_correlation.parquet`는
A primary 12 family만 담는데 공매도 4개는 exploratory라 제외됐다.

### 확인하지 않은 중복

같은 공매도 묶음 셋과의 관계를 재지 않았다. 특히:

- `flow_short_interest`(잔고 비율)와 이 피처(거래 비중)는 **같은 공매도 활동의 stock/flow
  두 측면**이다.
- `flow_nat_proxy_20d`는 산식에 공매도 잔고 비율을 직접 포함한다.

`px_amihud_20d`·`fin_log_mcap` 같은 규모 축과의 관계도 모른다. 공매도가 대형주에 몰린다면
(§5.7) 규모와 얽혀 있을 가능성이 있다.

---

## 8. 한계와 확인 못 한 것

1. **표본이 2020-03-24에서 끝난다** (§6). 이후 5년의 데이터가 없다. 최근 시장에서도 같은지
   모른다.
2. **기간 검정이 2구간뿐이다** (§5.1).
3. **거래가능 유지율이 0.571로 최저다** (§5.3).
4. **BH·discovery 판정이 없다** (§4.1). q값이 아예 계산되지 않았다.
5. **`partial` 구간(2021-05~2023-11)을 통째로 뺐다** (§2.2). 대형주만 허용된 구간이라
   제외한 건 타당하지만, 그 구간의 대형주만이라도 볼 수 있는지 검토하지 않았다.
6. **2025-03-31 재개 이후를 못 봤다.** `common_survivor` 표본이 2025-02-05에서 끝나서다.
7. **같은 공매도 묶음 내부의 중복이 미확인**이다 (§7).
8. **규모와의 관계를 모른다** (§5.7, §7).
9. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
10. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나

**T1·T2 어느 후보에도 안 들어갔다.** exploratory 역할이라 대상이 아니다.

`09_all_feature_results.md` §6의 결론이 남은 계획이다.

> 레짐이 안정된 구간이 충분히 쌓이면 **새 config로 사전등록해서 다시 본다.**

2025-03-31 재개 이후 구간이 쌓이면 재등록 대상이다. **지금 숫자를 근거로 채택하면 안 된다.**

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
A=f"research/output/horizon_scan/phase=A/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260827T221729-4e0ae8b0"
# exploratory 4 family 는 hypothesis_role 로 걸러야 한다. q_fdr_global 은 NULL 이다.
print(duckdb.sql(f"""
  select family, scan_type, h_start, h_end, ic_mean, icir, t_nw,
         q5_spread_aligned, q_fdr_global, n_dates,
         effective_sample_start, effective_sample_end
  from '{A}/core/horizon_ic.parquet'
  where hypothesis_role='exploratory_short_regime'
    and universe='broad' and sample_kind='common_survivor'
  order by family, scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| cell 전체 (28개) | `phase=A/…/run_id=20260827T221729-4e0ae8b0/core/horizon_ic.parquet` (`hypothesis_role='exploratory_short_regime'`) |
| card | 같은 run의 `cards/family_cards.json` |
| 차트 7종 | 같은 run의 `plots/flow_short_turnover_*.png` |
| 산식 | `research/etl/features/flow.py:248` |
| 제도 구간 정의 | `research/etl/quality.py:26` |
| exploratory로 내린 근거 | `01_feature_candidate/09_all_feature_results.md` §6 |
| 보고서 표기 규칙 | `01_feature_candidate/13_feature_performance_html_report_plan.md` §4.1, §6.1 |
