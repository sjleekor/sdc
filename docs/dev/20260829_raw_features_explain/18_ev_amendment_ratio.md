# 18. `ev_amendment_ratio` — 정정공시 비율

- 작성일: 2026-08-29
- family: `ev_amendment_ratio` · primary feature: **`ev_amendment_ratio_1y`** · domain: event
- **Phase B** · fdr_family `event` · 기대 부호 `−` · 관측 부호 `−`
- 등급 **A** · **discovery 4/4 · screen-pass 4/4 · 실패한 게이트 없음**
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**최근 1년 공시 중 정정공시 비율이 높은 회사가 이후 60~120일 동안 시장 대비 부진했다**
(cum 0→120 IC −0.0568, ICIR −1.357, 5분위 수익률 차이 +2.65%p).

**Phase B 18개 중 가장 깨끗한 축이다.** 사전등록 4개 cell이 **전부 discovery이면서 전부
screen-pass**이고 `failed_gates`가 비어 있다. 기간 5/5, 시간 placebo 통과, 비중첩 offset
`complete`, 거래가능 유지율 0.98~1.01.

**새로 수집한 데이터가 아니다.** 이미 갖고 있던 공시 접수 이력
(`dart_filing_receipt_raw`, 120만 행)에서 뽑았다. 알파 피처가 하나도 읽지 않던 테이블이다.

**주의 하나.** Phase B `family_summary.parquet`에는 이 family의 discovery가 **0개, 등급
NE**로 적혀 있다. 그건 AB 결합 전 placeholder다 (§4.2).

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/filing_activity.py:243
amendments_250d / NULLIF(filings_250d, 0) AS ev_amendment_ratio_1y
```

**최근 250거래일(약 1년) 공시 중 정정공시가 차지하는 비율**이다.

- 0.1이면 공시 10건 중 1건이 정정이었다
- 값이 클수록 처음 낸 공시를 자주 고쳤다

### 2.2 무엇을 "정정"으로 세는가

`report_nm`(보고서명) 문자열에 다음 넷 중 하나가 들어가면 정정으로 센다.

```python
# research/etl/phase_b_quality.py:40
AMENDMENT_MARKERS = ("기재정정", "첨부정정", "첨부추가", "변경등록")
```

**주의할 구분이 하나 있다.** list.json의 `rm` 칼럼에 있는 `정` 플래그는 **"이 공시가 나중에
정정됐다"**는 뜻이고, 위 마커는 **"이 공시가 정정본이다"**는 뜻이다. 서로 다르다.
이 피처는 **후자**를 센다.

정의를 새로 만들지 않고 `phase_b_quality`가 이미 `revision_ratio` 계산에 쓰던 것을 그대로
가져왔다. 모듈 docstring이 이유를 적어 뒀다.

> a second definition of the same concept is worse than either one

### 2.3 창이 250거래일인 이유

```python
# research/etl/features/filing_activity.py:83
RATIO_WINDOW = 250
```

주석이 근거를 적었다.

> A ratio needs enough filings underneath it to be a ratio at all, and filings
> run about 8 per company-year.

회사당 연간 공시가 8건 안팎이다. 창이 짧으면 분모가 한두 건이 되어 비율이 0 아니면 1로
튄다. 1년치를 모아야 비율이 의미를 갖는다.

**대가는 반응 속도다.** 오늘 정정 하나가 나도 분모 8건 중 1건이라 값이 크게 안 움직인다.
**천천히 변하는 지표**이고, 그래서 사전등록 horizon도 60~120일로 길다.

### 2.4 PIT — 접수 다음 거래일부터 쓴다

```sql
-- research/etl/features/filing_activity.py:170
-- A receipt filed on D is exposed on the first session AFTER D.
-- DART publishes through the day, so exposing it on D itself would
-- let an evening filing predict that afternoon's return.
SELECT rd.rcept_dt, MIN(s.trade_date) AS available_date
FROM (SELECT DISTINCT rcept_dt FROM receipts) rd
JOIN trading_days s ON s.trade_date > rd.rcept_dt
GROUP BY rd.rcept_dt
```

**D일에 접수된 공시는 D+1 거래일부터 쓴다.** DART는 하루 종일 공시를 받으므로, 당일에
노출하면 저녁 공시가 그날 오후 수익률을 예측하는 꼴이 된다.

그래서 이 family의 정본 변형은 **`native_t`**다 — PIT 처리가 이미 산식 안에 들어 있어
추가 지연이 필요 없다. 수급 계열이 `lag1`을 정본으로 쓰는 것과 다르다.

### 2.5 0인 날도 패널에 채운다

```sql
-- research/etl/features/filing_activity.py:196
-- Every universe row, with zeros on days nothing was filed. Without
-- the zero rows a ROWS window would count filing days rather than
-- sessions, and a quiet company's window would silently stretch
-- across years.
```

공시가 없는 날을 0으로 채우지 않으면 `ROWS BETWEEN 249 PRECEDING`이 **거래일 250일이 아니라
공시일 250건**을 세게 된다. 공시가 드문 회사는 창이 몇 년으로 늘어난다. **의도한 창을
지키려는 처리다.**

### 2.6 원천 — 새로 수집하지 않았다

```
dart_filing_receipt_raw
  1,201,866 행 · 2015-01 ~ 2026-08 · 2,657 종목
```

모듈 docstring이 이 선택의 배경을 적었다.

> **Nothing new is collected here.** … not one alpha feature reads it.
> … The DS004 ownership APIs answer with a rolling two-year window — even
> Samsung Electronics, listed in 1975, starts at 2024-08-26 …
> The receipts carry the same events over **10.5 years** at 98.1% coverage.
> The trade is quantity for frequency … **A ten-year sample of a weaker measure
> beats a one-year sample of a stronger one.**

지분 변동의 **내용**을 주는 API는 2년치밖에 안 주고, 접수 이력은 **건수**만 주지만 10.5년치를
준다. 후자를 택했다.

### 2.7 formula_version이 붙어 있다

```
formula_version: filing_v3
```

Phase B family에는 산식 버전이 기록된다. v1은 최초 정의, v2는 1세션 지연 변형 노출,
v3는 등록된 broad universe 마트 사용이다.

`config_hash`나 코드 해시로는 산식 변경이 안 잡히기 때문에 따로 둔 필드다
(`filing_activity.py:60` 주석).

### 2.8 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/filing_activity.py:243` |
| 정정 마커 | `research/etl/phase_b_quality.py:40` |
| PIT 노출 규칙 | `research/etl/features/filing_activity.py:170` |
| 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**공시 품질이 경영 품질의 대리변수라는 가설이다.**

공시를 자주 고치는 회사는 내부 통제나 회계 처리에 문제가 있을 가능성이 크다. 그 자체가
직접 주가를 떨어뜨리는 게 아니라, **앞으로 드러날 문제의 조기 신호**로 본다.

**정보 우위 가설도, 위험 프리미엄 가설도 아니다.** 공개된 사실(정정을 몇 번 했나)을 세는
것뿐인데, 시장이 그 정보를 충분히 반영하지 않는다는 **과소반응 가설**에 가깝다.

### 3.2 기대 부호

`−`. 정정공시 비율이 높을수록 이후 초과수익률 순위가 낮다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_expansion_20260827.yaml
- family: ev_amendment_ratio
  expected_sign: "-"
  features: [{column: ev_amendment_ratio_1y, role: primary}]
  primary_horizon_set: [60, 120]
  exploratory_horizon_set: [20, 40]
  include_bucket_primary: true
```

**§2.3에서 본 대로 250일 창의 느린 지표이므로 긴 horizon에 걸었다.** 20~40일은 exploratory로
내렸다.

primary가 [60, 120] 둘이라 cell은 누적 2개 + 구간 2개 = **4개**다.

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 60~120일 | **4개 cell 전부 discovery** |
| 부호 | `−` | **`−` (일치)** |

### 3.4 사전등록 시점

이 family는 **2026-08-27 확장 등록분**이다.

```yaml
preregistration:
  id: expansion_20260827
  registered_at: 2026-08-27
  outcome_blind: true
```

`outcome_blind: true`는 **결과를 보기 전에 등록했다**는 선언이다. 기존 config의
`config_hash`는 그대로 두고 overlay로만 추가했다.

분류 좌표는 **C4(이벤트·공시)** × T0(수준) × U다.

### 3.5 근거 문헌

없다. 접수 이력에서 만든 신규 지표다. `11_feature_taxonomy.md` §2.1이 C4를 "공시 텍스트"가
비어 있는 영역으로 지목했고, 이 family가 그 방향의 첫 시도다.

---

## 4. 얼마나 효과가 있었나

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

부호가 `−`이므로 5분위 차이는 방향 정렬값이다. 양수면 기대대로다.

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이(정렬) | AB q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | −0.0435 | −1.065 | −8.98 | +1.05%p | ~0 | **discovery + screen-pass** |
| cum | 0→120 | **−0.0568** | **−1.357** | −8.96 | **+2.65%p** | ~0 | **discovery + screen-pass** |
| bucket | 40→60 | −0.0285 | −0.700 | −9.93 | +0.33%p | ~0 | **discovery + screen-pass** |
| bucket | 60→120 | −0.0431 | −1.044 | −8.59 | +1.37%p | ~0 | **discovery + screen-pass** |

**4개 전부 통과했고 `failed_gates`가 비어 있다.**

|ICIR|이 1.36까지 간다. Phase A의 `px_idio_vol_60d`(1.64), `px_maxret_20d`(1.31)와 같은
급이다.

### 4.2 `family_summary`의 0과 NE를 믿으면 안 된다

**보고서를 읽을 때 가장 헷갈리는 지점이다.**

Phase B `core/family_summary.parquet`에서 이 family를 보면 이렇다.

| 필드 | 값 |
|---|---|
| `primary_discovery_cells` | **0** |
| `screen_pass_cells` | **0** |
| `evidence_grade` | **NE** |
| `next_step` | `no primary discovery; keep as exploratory` |

**전부 placeholder다.** 이 파일은 AB 결합 **전에** 만들어졌다.

실제 판정은 AB `combined_ab_primary_hypotheses.parquet`에 있고 **4/4 discovery,
4/4 screen-pass, 등급 A**다.

보고서 설계 문서가 이 함정을 명시했다.

> `family_summary.parquet`는 AB 전 산출물이라 18 family의 `evidence_grade`가 모두 `NE`이고
> AB q·discovery·screen-pass가 placeholder다. 이 파일에서는 `coverage_ratio`,
> `effective_start`, `observations`, `formula_version`, readiness·blocker와
> `top_rank_correlation`만 읽는다. **grade와 AB 판정은 쓰지 않는다.**
> — `13_feature_performance_html_report_plan.md` §5.2

### 4.3 IC를 수익률로 읽으면

120거래일(약 6개월) 기준 **+2.65%p**다. 같은 120일 horizon인 `px_amihud_20d`(+11.21%p)보다는
작지만, Phase B 계열에서는 큰 축이다.

거래비용 차감 전이고 창이 매일 겹친다. 다만 250일 창의 느린 지표라 **회전이 낮을 것으로
보인다** — 회전율을 실제로 재지는 않았다.

### 4.4 신호의 모양

Phase B는 Phase A의 `family_cards.json` 같은 decay 요약(`onset_h`, `peak_h_cum`,
`half_life_bucket`)을 만들지 않는다. cell 값으로 읽어야 한다.

| 관찰 | 값 |
|---|---|
| `peak_cell` | `cum 0→120` |
| `peak_ic_mean` | −0.0568 |
| 누적 \|IC\| 추이 | 60일 0.043 → 120일 0.057 (증가) |
| 구간 \|IC\| 추이 | 40~60일 0.029 → 60~120일 0.043 (증가) |

**관측 범위 끝에서 최대다.** 120일 너머가 궁금한데 사전등록 최대치가 120이라 확인하지 않았다.

---

## 5. 진짜인가 — 강건성

**Phase B에서 가장 깨끗하다.**

### 5.1 기간 일관성 — 4개 cell 전부 5/5

- `valid_subperiods` = 5
- `sign_consistent_subperiods` = **5** (4개 cell 모두)
- `period_sign_pass` = **True** (4개 cell 모두)

### 5.2 시간 placebo — 통과

| cell | `p_temporal_nw` | 판정 |
|---|---:|---|
| cum 0→60 | **0.0198** | **통과** |
| cum 0→120 | **0.0099** | **통과** |
| bucket 60→120 | **0.0099** | **통과** |
| bucket 40→60 | — | 대상 아님 (NW lag 19 < 59) |

기준은 0.10이다. 세 cell 모두 통과했고 두 개는 최솟값(0.0099)이다.

### 5.3 비중첩 offset — `complete`

| cell | `offset_status` | `nonoverlap_robustness_pass` |
|---|---|---|
| cum 0→60 | **complete** | **True** |
| cum 0→120 | **complete** | **True** |
| bucket 60→120 | **complete** | **True** |
| bucket 40→60 | — | 대상 아님 (`robustness_required = false`) |

`robustness_required`는 cell마다 다르다. 폭이 좁은 bucket 40→60은 강건성 검사 대상이 아니다.

### 5.4 거래 가능한 종목만 남겨도

| cell | `tradable_retention` | `tradable_pass` |
|---|---:|---|
| cum 0→60 | 0.992 | True |
| cum 0→120 | **1.011** | True |
| bucket 40→60 | 0.977 | True |
| bucket 60→120 | **1.009** | True |

**거의 손실이 없고 두 cell은 오히려 강해진다.** 소형주 착시가 아니다.

### 5.5 생존편향

`available_direction_pass` = **True** (4개 cell 모두). 상장폐지 종목을 포함해도 부호가
같다.

### 5.6 source quality — 대상 아님

`source_quality_status` = `not_applicable`, `source_quality_grade_cap` = `None`.

Phase B 확장분 중 인적자본·지분 계열 4개는 `final_vintage` 경고와 등급 상한 B가 걸려 있는데
(§9 참조) 이 family는 해당 없다. **접수 이력은 사후 수정되지 않는 사실 기록이기 때문이다.**

### 5.7 등급 A

`evidence_grade` = **A** (4개 cell 모두), `failed_gates` = `[]`.

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2015-01-05 ~ 2025-02-05** |
| 유효 거래일 | **2,478일** |
| 날짜당 평균 종목 수 | 1,038~1,041개 |
| `coverage_ratio` | **0.894** |
| 관측 행 수 | 6,150,482 |

### 6.1 시장별 커버리지가 다르다

| 시장 | 커버리지 | 값이 있는 종목 | 날짜당 중앙값 |
|---|---:|---:|---:|
| KOSDAQ | **0.946** | 1,773 | 1,178 |
| KOSPI | **0.817** | 838 | 789 |

**KOSPI가 13%p 낮다.** 원인을 이번에 분석하지 않았다.

횡단 순위를 `(거래일, 시장)` 안에서 매기므로 시장별 커버리지 차이 자체가 IC를 왜곡하지는
않는다. 다만 **KOSPI 쪽 표본이 상대적으로 얇다**는 점은 알고 봐야 한다.

### 6.2 연도별 커버리지

2015년 0.872에서 시작해 2016년부터 0.94~0.95로 안정된다. 250일 창을 채우는 데 1년이
걸리기 때문이다.

원천은 2026-08까지 있지만 scan 표본은 `common_survivor` 규칙에 따라 2025-02-05에서 끝난다.

---

## 7. 중복성

### A×B 교차 상관

| 상대 family | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `px_idio_vol_60d` | **+0.140** | 2,334 | +0.01 ~ +0.23 |
| `px_near_52w_high` | **−0.137** | 2,478 | −0.22 ~ −0.01 |
| `px_maxret_20d` | +0.094 | 2,478 | −0.01 ~ +0.21 |
| `px_mom_12_1` | −0.049 | 2,460 | −0.17 ~ +0.06 |

**두 관계가 안정적이다.** 범위가 한쪽 부호로만 몰려 있다.

- **`px_idio_vol_60d`와 +0.140.** 정정공시가 잦은 회사는 고유변동성도 크다. 둘 다 부호가
  `−`이므로 같은 방향으로 작동한다. 모델에 함께 넣으면 증분이 줄어들 수 있다.
- **`px_near_52w_high`와 −0.137.** 정정이 잦은 회사는 52주 고점에서 멀다. 이것도 부호를
  맞추면 같은 방향이다.

`|ρ| ≥ 0.7` 경고 기준에는 한참 못 미친다.

### 확인하지 않은 중복 — 같은 마트에서 나온 형제들

`feat_filing_activity` 하나가 다섯 family를 만든다.

| family | 산식 |
|---|---|
| **`ev_amendment_ratio`** | 정정 / 전체 공시 (250일) |
| [19_ev_filing_activity](19_ev_filing_activity.md) | 공시 건수 급증 비율 |
| [31_own_amendment_ratio](31_own_amendment_ratio.md) | **지분 공시 중 정정 비율** |
| [32_own_insider_filing_activity](32_own_insider_filing_activity.md) | 임원·주요주주 공시 급증 |
| [33_own_major_filing_activity](33_own_major_filing_activity.md) | 5% 대량보유 공시 건수 |

**특히 `own_amendment_ratio`와는 부분집합 관계다.** 이쪽은 전체 공시의 정정 비율,
저쪽은 **지분 공시만의** 정정 비율이다. 분자·분모가 겹친다.

**B×B 상관 산출물이 없다.** A×B 교차만 계산했다
(`13_..._plan.md` §7.2 차트 5). **다섯을 독립된 발견으로 세면 안 된다.**

---

## 8. 한계와 확인 못 한 것

1. **같은 마트의 다섯 family 간 중복이 미확인이다** (§7). 특히 `own_amendment_ratio`와
   부분집합 관계다. B×B 상관이 없다.
2. **KOSPI 커버리지가 13%p 낮다** (§6.1). 원인을 분석하지 않았다.
3. **120일 너머를 안 봤다** (§4.4). |IC|가 관측 범위 끝에서 최대다.
4. **회전율·거래비용을 재지 않았다** (§4.3). 느린 지표라 회전이 낮을 것으로 보이지만
   측정값이 없다.
5. **정정의 종류를 구분하지 않았다** (§2.2). `기재정정`·`첨부정정`·`첨부추가`·`변경등록`을
   한 묶음으로 센다. 단순 첨부 추가와 내용 정정은 의미가 다를 수 있다.
6. **공시의 중요도를 구분하지 않았다.** `ev_material_event_flag`는 의도적으로 만들지
   않았다 — 어떤 `report_nm`이 중요한지 정하는 판단이 아직 없기 때문이다
   (`filing_activity.py` docstring).
7. **업종 중립화가 없다.** 업종마다 공시 빈도와 정정 관행이 다를 수 있다.
8. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
9. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나 — T2

**T2 14-feature bundle에 들어갔다** (`ev_amendment_ratio_1y`).

T2는 Phase B 후보 14개를 **한꺼번에** baseline에 더해 walk-forward로 평가한 실험이다.

| horizon | Rank IC Δ | 비용 반영 spread Δ |
|---|---:|---:|
| 5 | **+0.0031** | **+0.0017** |
| 20 | **+0.0011** | **+0.0030** |
| 60 | **+0.0003** | **+0.0080** |

**세 horizon 전부, 두 지표 전부 개선됐다.** T2 status는 `improved_all_horizons`다.

**주의 — 이게 이 피처의 기여를 뜻하지 않는다.** 14개를 함께 넣은 결과다. 개별 기여도는
측정하지 않았다. 단변량 근거만 보면 이 family가 14개 중 강한 축이지만, 묶음 안에서 다른
피처와 겹쳐 증분이 작았을 수도 있다.

T2 후보 14개는 다음과 같다.

```
ev_amendment_ratio_1y, ev_filing_burst_60d, ev_net_share_issuance_yoy, ev_payout_yield,
fin_gross_profitability, fin_log_mcap, fin_value_z, hc_employee_growth_yoy,
hc_revenue_per_employee, mcap_krx_log, own_amendment_ratio_1y, own_major_filing_60d,
own_major_stake, own_major_stake_chg
```

**최종 h60 holdout은 아직 열지 않았다.** 2026년 10~11월 이후 새 구간으로 한 번 평가한다.

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# 판정은 family_summary 가 아니라 AB parquet 에서 읽는다
print(duckdb.sql(f"""
  select feature, scan_type, h_start, h_end, ic_mean, q5_spread_aligned,
         q_fdr_global_ab, primary_discovery_ab, screen_pass, evidence_grade,
         failed_gates, temporal_null_pass, p_temporal_nw, tradable_retention
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  where family='ev_amendment_ratio'
  order by scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 커버리지 | 같은 B run의 `core/feature_coverage.parquet` |
| family 요약 (판정 제외) | 같은 B run의 `core/family_summary.parquet` |
| 산식 | `research/etl/features/filing_activity.py:243` |
| 정정 마커 | `research/etl/phase_b_quality.py:40` |
| 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |
| placeholder 주의 | `01_feature_candidate/13_feature_performance_html_report_plan.md` §5.2 |
| T2 결과 | `docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json` |
