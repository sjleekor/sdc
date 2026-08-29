# 09. `px_zero_ret_ratio_20d` — 무변동일 비율 (reference 전용)

- 작성일: 2026-08-29
- family: `px_zero_ret_ratio_20d` · primary feature: `px_zero_ret_ratio_20d` · domain: `reference`
- Phase A · fdr_family **`reference`** · role **`reference_only`** · 기대 부호 **없음**
- 등급 **R** · IC를 계산하지 않았음 · **`horizon_ic.parquet`에 행이 0개**
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**성능을 재지 않은 유일한 family다.** 애초에 예측력을 묻는 대상이 아니라 **유동성 필터이자
데이터 품질 지표**로 등록했다.

35개 중 이것만 `role: reference_only`이고, 사전등록 primary horizon이 **빈 목록**이며, Phase A
`horizon_ic.parquet`에 **행이 하나도 없다.** IC·q·discovery가 전부 `null`이다.

**보고서에서 `—`로 표시된 칸들은 계산 결과가 0이라는 뜻이 아니라 계산 자체를 하지 않았다는
뜻이다.** 이 문서의 목적은 그 구분을 분명히 하는 것이다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/price.py:117
AVG(CASE WHEN log_ret = 0 OR volume_d = 0 THEN 1.0 ELSE 0.0 END) OVER w20
    AS px_zero_ret_ratio_20d

-- w20 = PARTITION BY ticker, market ORDER BY trade_date
--       ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
```

**최근 20거래일 중 "가격이 안 움직였거나 거래가 없었던 날"의 비율**이다.

- 0이면 20일 내내 가격이 움직였다
- 0.5면 절반이 무변동일이다
- 1이면 20일 내내 정지 상태였다

두 조건을 OR로 묶는다.

| 조건 | 의미 |
|---|---|
| `log_ret = 0` | 종가가 전날과 같음 — 거래는 있었지만 가격이 안 변함 |
| `volume_d = 0` | 아예 거래가 없었음 |

### 2.2 무엇을 잡아내는가

**"이 종목의 가격이 진짜 가격인가"**를 잰다.

거래가 거의 없는 종목은 종가가 며칠씩 그대로 있다. 그 종가로 계산한 수익률·변동성·모멘텀은
실제 시장 평가가 아니라 **거래 부재의 흔적**이다.

Lesmond, Ogden & Trzcinka (1999)의 착상이 여기 있다. 거래비용이 높으면 정보가 있어도
거래가 일어나지 않고, 그 결과 수익률이 0으로 관측된다. **무변동일 비율이 곧 거래비용의
간접 측정치**라는 것이다.

### 2.3 이 값이 다른 피처에 미치는 영향

무변동일이 많은 종목에서는 다른 가격 피처들이 왜곡된다.

| 피처 | 왜곡 방향 |
|---|---|
| `px_idio_vol_60d` | 0이 많이 섞여 변동성이 **과소** 추정 |
| `px_maxret_20d` | 움직인 날이 적어 최댓값이 우연에 좌우 |
| `px_amihud_20d` | `volume=0` 행이 빠져 남은 소수 날짜로 평균 계산 |
| `px_reversal_5d` | 5일 합이 실제로는 며칠치만 반영 |

**이 피처를 reference로 둔 이유가 이것이다.** 자기가 알파를 내는 게 아니라 **다른 피처의
신뢰도를 판단하는 잣대** 역할이다.

`11_feature_taxonomy.md` §2.1이 `px_amihud_20d`, `flow_days_to_cover`와 함께 C8
(실행·거래비용)에 배치한 근거다.

### 2.4 무엇이 NULL이 되는가

```sql
-- research/etl/features/price.py:153
CASE WHEN ca_count_20 > 0 THEN NULL ELSE px_zero_ret_ratio_20d END
```

20일 창에 기업행동이 있으면 버린다.

거래정지일(`open=high=low=0`)은 **패널에서 아예 빠지므로** 이 비율에 잡히지 않는다
(`trading_panel.py:28`). 즉 이 피처가 잡는 건 **거래정지가 아니라 "거래는 가능한데 안
일어난 날"**이다. 둘을 구분해야 한다.

### 2.5 변형

| variant | 컬럼 |
|---|---|
| `native_t` (정본) | `px_zero_ret_ratio_20d` |
| `lag1` | `px_zero_ret_ratio_20d_lag1` |

두 변형 모두 등록돼 있지만 **IC를 계산하지 않았으므로 비교값이 없다.**

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/price.py:117` (마스킹 `:153`) |
| 거래정지 제외 | `research/etl/trading_panel.py:28` |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:290` |

---

## 3. 왜 예측을 묻지 않았나 — 설계 의도

### 3.1 사전등록이 명시적으로 비워져 있다

```yaml
# horizon_scan_config.yaml:290
- family: px_zero_ret_ratio_20d
  phase: A
  fdr_family: reference          # price 가 아니다
  role: reference_only           # ready 가 아니다
  expected_sign: null            # 기대 부호 없음
  primary_horizon_set: []        # 비어 있다
  exploratory_horizon_set: [1, 5, 20, 60, 120]
  include_bucket_primary: false
  fdr_include: false             # BH 모집단에서 제외
```

**다섯 군데가 다른 family와 다르다.** 우연이 아니라 설계다.

- `fdr_family: reference` — 다중검정 묶음이 따로다
- `role: reference_only` — screen_pass 대상이 아니다
  (`horizon_scan_report.py:188`: `if role != "ready": screen_pass = False`)
- `expected_sign: null` — 방향 가설이 없다
- `primary_horizon_set: []` — **검정할 cell이 없다**
- `fdr_include: false` — 153개 결합 BH 모집단에 안 들어간다

### 3.2 왜 그렇게 정했나

`02_feature_candidate.md` §3.1 P10이 적어 둔 역할이다.

> `px_zero_ret_ratio_20d` | `mean(r==0 or volume==0, 20~63d)` | **필터** |
> (Lesmond et al. 1999) | 소형주 유동성 필터 겸용, **거래정지 처리와 일관**

방향이 `필터`로 적혀 있다. `+`도 `-`도 아니다.

**이유는 두 가지다.**

**첫째, 부호를 정할 수 없다.** 무변동일이 많다는 건 유동성이 나쁘다는 뜻이고, 유동성
프리미엄 가설대로면 기대수익이 높아야 한다(`+`). 그런데 동시에 거래가 안 되는 종목은
실행이 불가능하다(`-`). [07_px_amihud_20d.md](07_px_amihud_20d.md) §2.4의 이중 역할과 같은
긴장인데, 이쪽은 훨씬 극단이라 alpha 쪽 주장을 아예 접었다.

**둘째, 사후에 부호를 고르는 걸 막는다.** 부호 가설 없이 검정하면 어느 방향이 나오든
"발견"이라고 말할 수 있게 된다. 그래서 아예 검정 대상에서 뺐다.

분류 좌표는 C1 / **C8** × T0 × U다.

### 3.3 근거 문헌

Lesmond, Ogden & Trzcinka (1999), *A New Estimate of Transaction Costs*.

---

## 4. 얼마나 효과가 있었나 — 재지 않았다

### 4.1 산출물에 행이 없다

Phase A `horizon_ic.parquet`에서 이 family의 행은 **0개**다.

| 항목 | 값 |
|---|---|
| A `horizon_ic` 행 수 | **0** |
| `broad_ic` | `null` |
| `q_fdr_global` | `null` |
| `candidate_horizon_band` | `null` |
| `peak_h_cum` / `peak_bucket` / `onset_h` | 전부 `null` |
| discovery 수 | 0 |

`primary_horizon_set`이 비어 있으므로 만들 cell이 없었다.

### 4.2 `—`와 `0`은 다르다

`13_feature_performance_html_report_plan.md` §6.1이 이 구분을 규정했다.

> AB에 없는 공매도 4 family와 reference 1 family의 primary 성능 칸은 …
> **`primary cell 없음 (reference_only)`**로 표시한다.

같은 문서 §7.3도 못 박았다.

> 계산하지 않은 값은 `—`로 표시하며 **0으로 바꾸지 않는다.**

**IC가 0이라는 것과 IC를 안 쟀다는 것은 전혀 다르다.** 전자는 "신호가 없다"는 측정 결과이고
후자는 "질문하지 않았다"는 설계 결정이다.

이 family는 후자다. 같은 `—`라도 `fin_sue`의 `insufficient`(계산하려 했는데 데이터가 없어
실패)와도 다르다.

| 상태 | 뜻 | 해당 family |
|---|---|---|
| 값 있음 | 계산했고 결과가 나옴 | 대부분 |
| `insufficient` | **계산하려 했는데** formation row가 없어 실패 | `fin_sue` 6 cell |
| `reference_only` | **애초에 계산하지 않기로 함** | **이 family** |
| `exploratory_short_regime` | 진단용으로만 계산, BH·채택 대상 아님 | 공매도 4 family |

### 4.3 결합 AB에도 없다

AB `combined_ab_primary_hypotheses.parquet`의 30개 family에 이 family는 없다. 153개 결합
BH 모집단에도 안 들어갔다 (`fdr_include: false`).

35 family 목록에 이름이 남아 있는 이유는 정본 규칙 때문이다.

```text
Phase A cards/family_cards.json 17개 ∪ Phase B core/family_summary.parquet 18개 = 35개
```

card는 발행되므로 목록에 들어가고, cell이 없으니 성능 칸은 비어 있다.

---

## 5. 진짜인가 — 해당 없음

강건성 검사 전체가 대상이 아니다.

| 검사 | 값 | 이유 |
|---|---|---|
| 기간 일관성 | `valid_subperiods` = 0 | 계산할 cell이 없음 |
| 시간 placebo | `null` | 대상 cell 없음 |
| 비중첩 offset | `null` | 대상 cell 없음 |
| tradable 유지율 | `null` | broad IC가 없어 비율을 못 만듦 |
| 생존편향 | `null` | 대조할 IC 없음 |
| 지연 | `null` | 대조할 IC 없음 |

card에 경고가 하나 붙어 있다.

```json
"warnings": ["insufficient_offset_coverage"]
```

offset 검정을 돌릴 cell이 없어서 자동으로 찍힌 값이다. **품질 문제를 뜻하는 게 아니라
검사 대상이 아니라는 표시다.**

`evidence_grade`가 `R`인 것도 같은 맥락이다. 등급 평가 순서가 `[R, C, A, B, D]`로 R을
가장 먼저 보게 돼 있어서, reference는 다른 등급으로 채점되기 전에 R로 확정된다
(`horizon_scan_config.yaml`의 `evidence_grade.evaluation_order`).

---

## 6. 표본과 커버리지

**Phase A 산출물에 커버리지 통계가 없다.**

- Phase A는 `coverage_ratio`를 계산하지 않는다 (Phase B 전용 지표다).
- 이 family는 IC cell도 없어 `n_dates`·`n_obs_mean`도 없다.

피처 값 자체는 `feat_price` 마트에 있다. 산식이 20일 창에 마스킹만 걸리므로 커버리지는
`px_maxret_20d`(2,622일, 날짜당 1,097종목)와 비슷할 것으로 보이지만 **이번 산출물로는
확인되지 않는다.**

---

## 7. 중복성

**A×B 상관 산출물에 이 family가 없다.** `primary_feature_rank_correlation.parquet`는
A primary 12 family × B primary 17 family 교차만 담는데, 이 family는 primary가 아니다.

### 확인하지 않은 중복 — 이 family에서 가장 아쉬운 부분

`px_amihud_20d`와의 관계를 재지 않았다. 둘 다 C8(실행·거래비용) 축이고, 산식도 겹친다.

- `px_zero_ret_ratio_20d`: `volume = 0`인 날의 비율
- `px_amihud_20d`: `volume = 0`인 날을 **제외한** 나머지의 평균

**서로의 여집합을 보고 있다.** `px_amihud_20d`가 규모와 −0.754로 얽힌 게
([07_px_amihud_20d.md](07_px_amihud_20d.md) §7) 확인된 만큼, 이 피처도 규모와 강하게
얽혀 있을 가능성이 높다. **그런데 상관을 재지 않았다.**

원래 설계는 이 피처를 필터로 쓰려던 것이므로, 필터로서 얼마나 유효한지를 보려면
`px_amihud_20d`·`fin_log_mcap`과의 관계가 필요하다.

---

## 8. 한계와 확인 못 한 것

1. **필터로 실제로 쓰지 않았다.** `tradable` universe 정의는 거래대금 1억원·종가 1,000원
   기준이고 (`horizon_scan_config.yaml`의 `universe.tradable_*`), **이 피처는 그 정의에
   들어가지 않는다.** 필터 용도로 등록했는데 필터로 쓰이지 않았다.
2. **`px_amihud_20d`·규모와의 상관이 없다** (§7).
3. **다른 피처의 왜곡을 실제로 측정하지 않았다** (§2.3). "무변동일 비율이 높은 종목을 빼면
   `px_idio_vol_60d`의 IC가 어떻게 되나" 같은 대조를 하지 않았다. 이게 reference 피처의
   본래 용도인데 쓰이지 않았다.
4. **커버리지 통계가 없다** (§6).
5. **20일 창만 만들었다.** `02_feature_candidate.md` P10은 `20~63d`로 적었는데 구현은 20일
   하나다.
6. **`log_ret = 0`과 `volume = 0`을 구분하지 않는다** (§2.1). 둘은 다른 현상인데 OR로
   합쳤다. 어느 쪽이 얼마나 기여하는지 모른다.

---

## 9. 모델에서는 어땠나

**T1·T2 어느 후보에도 안 들어갔다.** reference 역할이므로 애초에 대상이 아니다.

기존 baseline 40개 피처에 포함돼 있는지는 이번 문서로 확인되지 않는다
(`build_dataset.py`의 `feature_cols_override` 목록을 봐야 한다).

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb, json
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
A=f"research/output/horizon_scan/phase=A/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260827T221729-4e0ae8b0"
# 행이 0개임을 확인한다
print(duckdb.sql(f"""
  select count(*) as rows
  from '{A}/core/horizon_ic.parquet' where family='px_zero_ret_ratio_20d'
""").df().to_string())
card = [c for c in json.load(open(f"{A}/cards/family_cards.json"))
        if c["family"] == "px_zero_ret_ratio_20d"][0]
print(json.dumps(card, ensure_ascii=False, indent=1))
PY
```

| 항목 | 위치 |
|---|---|
| card (IC 전부 null) | `phase=A/…/run_id=20260827T221729-4e0ae8b0/cards/family_cards.json` |
| 산식 | `research/etl/features/price.py:117` |
| reference 사전등록 | `research/analysis/horizon_scan_config.yaml:290` |
| role 게이트 코드 | `research/analysis/horizon_scan_report.py:188` |
| 등급 평가 순서 | `research/analysis/horizon_scan_config.yaml`의 `evidence_grade.evaluation_order` |
| `—` 표기 규칙 | `01_feature_candidate/13_feature_performance_html_report_plan.md` §6.1, §7.3 |
| 필터 역할 지정 | `01_feature_candidate/02_feature_candidate.md` §3.1 P10 |
| 서술 대조 | `01_feature_candidate/09_all_feature_results.md` §4 「기준용 1개」 |
