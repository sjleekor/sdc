# F-5.0 PoC — 현금흐름 4 metric의 XBRL fallback

- 작성: 2026-09-08
- 배경: F-4.6. `feat_fin_risk`의 9 family 중 **5개가 2020년부터만 존재한다.**
  `interest_paid`·`investing_cash_flow`·`financing_cash_flow`·`cash_and_cash_equivalents`
  넷이 `dart_financial_statement_raw` 규칙만 가지고 XBRL fallback이 없어서, 2018년 이전
  행이 100건 안팎이다(측정: [`../results/f4_fin_risk_verification.md`](../results/f4_fin_risk_verification.md) §5).
- 목적: fallback을 붙이면 그 5개 family의 표본이 2016년까지 내려가는지 재고, 판정한다.

---

## 0. 판정 규칙 — 측정 전에 고정 (사용자 승인 2026-09-08)

**이 절은 아래 §1의 측정을 하기 전에 쓰고 커밋했다.** 커밋 순서가 그것을 증명한다.

```
결정 A: XBRL fallback 추가 판정
  다섯 family 중 하나라도 유효 시작 연도가 1년 이상 앞당겨지면 규칙을 추가하고
  FORMULA_VERSION을 fin_risk_v2로 올린다.

결정 B: 사전등록 표본 시작 (되돌릴 수 없음)
  family별로 횡단면 커버리지가 처음 0.5 이상이 되는 연도를 표본 시작으로 삼는다.
  재빌드된 마트에서 IC 계산 이전에 측정하고, 연도별 커버리지 표를 결과 문서에 남긴다.
  근거: fin_debt_to_assets가 0.545로 시작한 2016년이 03 §2의 기대와 일치한다.
  0.5는 결과를 보기 전에 고정하며 이후 바꾸지 않는다.
```

### 왜 "행 수 비율"이 아니라 커버리지인가

처음에 "`total_liabilities` 행 수 대비 50%"를 제안했다가 폐기했다. **틀린 양을 재는
문턱이었다.** 다섯 family는 TTM을 쓰고 TTM은 **연속 4분기**를 요구한다. 어떤 metric이
분기의 50%를 무작위로 가지면 연속 4분기 확률은 0.5⁴ = 6%다. 많으냐가 아니라 법인별로
이어져 있느냐가 문제다.

실측이 이를 뒷받침한다: `fin_net_debt_to_mcap`은 2020년에 0.782에 닿는데 나머지 넷은
2021년까지 간다. 전자는 현금(instant, 1분기)만 필요하고 후자는 TTM(4분기)이 필요해
**한 해가 더 든다.**

### 왜 0.5인가

| 근거 | 내용 |
|---|---|
| 이미 쓰는 값이다 | 정상 작동하는 `fin_debt_to_assets`의 커버리지가 2015년 0.005 → **2016년 0.545** → 2017년 0.760이고, `03` §2가 독립적으로 "유효 시작 2016~2017"을 기대했다. 새 숫자가 아니라 이미 함축된 값이다 |
| 아래로 내려가면 모집단이 바뀐다 | 커버리지 0.1 구간의 행은 XBRL을 일찍 도입한 대형·유자원 법인이다. 0.78 커버리지의 2021년 이후에 이어 붙이면 패널 구성이 표본 중간에 달라진다. 횡단면 rank IC에서는 초기 연도가 **다른 유니버스를 재는 것**이고, 짧고 깨끗한 표본보다 나쁘다 |
| 위로 올리면 멀쩡한 연도를 버린다 | 0.7이면 `fin_debt_to_assets`가 2017년 시작이 되어 한 해를 잃는다. 0.545 → 0.76의 대표성 개선이 horizon 120 관측 1년치 손실보다 작다 |

### 왜 family별인가

공통 시작일은 멀쩡한 family를 잘라내거나 약한 family를 희석한다. 그리고 이 저장소는 이미
family별로 다르다 — F-4 리포트가 컬럼별 유효 시작일을 적고, 스캔의 `available` 표본도 셀
단위이며, `fin_sue`와 `px_*`가 이미 다른 표본을 쓴다.

---

## 1. 측정 — XBRL 개념 커버리지 (lake snapshot 2026-08-23, 로컬, 수집 0)

### 1.1 철자가 전부였다

DART가 2019년경 taxonomy 접두를 바꿨다. **2018년 이전을 덮는 것은 `ifrs_` 쪽이다.**

| concept_id | 전체 행 | 법인 | 기간 | **≤2018 행** |
|---|---|---|---|---|
| `ifrs-full_Liabilities` (참조) | 354,982 | 3,008 | 2015-2026 | **364** |
| `ifrs_Liabilities` (참조) | 127,467 | 2,143 | 2015-2020 | **112,827** |
| `ifrs-full_CashAndCashEquivalents` | 331,578 | 3,008 | 2015-2026 | 364 |
| `ifrs_CashAndCashEquivalents` | 127,155 | 2,139 | 2015-2020 | **112,523** |
| `ifrs-full_CashFlowsFromUsedInInvestingActivities` | 273,215 | 3,001 | 2015-2026 | 361 |
| `ifrs_CashFlowsFromUsedInInvestingActivities` | 136,758 | 2,124 | 2015-2020 | **122,264** |
| `ifrs-full_CashFlowsFromUsedInFinancingActivities` | 272,297 | 3,001 | 2015-2026 | 358 |
| `ifrs_CashFlowsFromUsedInFinancingActivities` | 133,167 | 2,126 | 2015-2020 | **119,213** |
| `ifrs-full_InterestPaidClassifiedAsOperatingActivities` | 224,733 | 2,625 | 2015-2026 | 264 |
| `ifrs_InterestPaidClassifiedAsOperatingActivities` | 103,027 | 1,839 | 2015-2020 | **91,781** |
| `ifrs-full_InterestPaid` / `ifrs_InterestPaid` | **데이터 없음** | | | |

`total_liabilities`가 2016년부터 작동하는 이유가 이것이다 — **두 철자를 모두** 매핑하고
있고, 초기 연도를 채우는 것은 `ifrs_` 쪽이다. **`ifrs-full_`만 붙였다면 메우려던 그 공백에
아무것도 안 늘었다.** `InterestPaid` 단축형은 사실이 하나도 없어 제외했다.

### 1.2 TTM 가능한 법인-연도 (연속 4분기)

§0이 정한 대로 행 수가 아니라 **4분기를 채운 법인 수**를 셌다.

| metric | 2016 | 2017 | 2018 | 참조 대비 |
|---|---|---|---|---|
| `total_liabilities` (참조, 도달 상한) | 1,742 | 1,813 | 1,908 | — |
| `cash_and_cash_equivalents` | 1,735 | 1,806 | 1,905 | ~100% |
| `investing_cash_flow` | 1,691 | 1,761 | 1,884 | ~97% |
| `financing_cash_flow` | 1,690 | 1,739 | 1,813 | ~96% |
| `interest_paid` | 1,281 | 1,295 | 1,485 | 71~78% |

2015년은 참조까지 포함해 전부 0이다 — 자료의 첫 해라 4분기 창이 닫히지 않는다.
`fin_debt_to_assets`가 2015년 0.005, 2016년 0.545인 것과 같은 이유다.

---

## 2. 적용 — 규칙 16개

`definitions/metric_rules.py`의 `xbrl_fallback_specs`에 4 metric을 추가했다.
metric 4 × 철자 2 × 기준(CFS/OFS) 2 = **16 규칙**, `total_liabilities` 패턴 그대로.
`xbrlfb.` 접두 / `fs_div` 지정 / `priority ≥ 100` 불변식 테스트 통과.

### 2.1 덧붙이기만 했다는 증거

vintage 마트를 v1(기존 규칙)과 v2(새 규칙)로 각각 빌드해 대조했다.

| metric | v1 행 | v2 행 | 증감 |
|---|---|---|---|
| `total_equity` | 159,047 | 159,047 | **+0** |
| `controlling_net_income` | 59,423 | 59,423 | **+0** |
| `operating_cash_flow` | 155,996 | 155,996 | **+0** |
| `revenue` | 148,645 | 148,645 | **+0** |
| `cogs` | 140,819 | 140,819 | **+0** |
| `gross_profit` | 140,160 | 140,160 | **+0** |
| `operating_income` | 153,154 | 153,154 | **+0** |
| `net_income` | 152,970 | 152,970 | **+0** |
| `total_assets` | 158,817 | 158,817 | **+0** |
| `interest_paid` | 85,872 | 124,829 | +38,957 |
| `investing_cash_flow` | 104,096 | 155,764 | +51,668 |
| `financing_cash_flow` | 103,721 | 153,167 | +49,446 |
| `cash_and_cash_equivalents` | 108,383 | 158,718 | +50,335 |

`feat_fin_scan_daily`가 쓰는 9 metric은 행 수뿐 아니라 **값이 다른 행이 0개**다
(`(ticker, metric, fs_basis, year, quarter, standalone_value, ttm_value)` EXCEPT 양방향).
즉 기존 검정 결과에 영향이 없다. 유닛 테스트 1,609개도 통과한다.

**canonical 마트는 건드리지 않았다.** raw와 규칙 무관 A0 마트 두 개를 심볼릭 링크로 재사용하는
별도 lake(`data_lake_f50/`)에서 빌드했다. 2026-08-23 스냅샷의 얼어 있는 Phase A/B 계보를
덮어쓰지 않기 위해서다.

---

## 3. 판정

### 결정 A: **통과.** 네 family가 2~4년 앞당겨졌다

| family | v1 시작 | v2 시작 | 이동 |
|---|---|---|---|
| `fin_net_debt_to_mcap` | 2020 | **2016** | **4년** |
| `fin_ext_finance_to_assets` | 2021 | **2018** | **3년** |
| `fin_lifecycle_stage` | 2021 | **2018** | **3년** |
| `fin_lifecycle_transition` | 2021 | **2018** | **3년** |
| `fin_interest_coverage` | 2021 | **2019** | **2년** |
| `fin_debt_to_assets` | 2016 | 2016 | 0 |
| `fin_profit_turn` | 2018 | 2018 | 0 |
| `fin_dividend_initiation` | 2019 | 2019 | 0 |
| `fin_negative_equity_exit` | 2017 | 2017 | 0 |

문턱은 "하나라도 1년 이상"이었고 실제로는 다섯 개가 2~4년 움직였다.
`FORMULA_VERSION`을 **`fin_risk_v2`** 로 올렸다.

### 결정 B: family별 사전등록 표본 시작 (확정, 되돌리지 않는다)

커버리지 0.5 규칙을 적용한 결과다. **IC를 계산하기 전에 측정했다.**

| family | 표본 시작 | 2017 | 2018 | 2019 | 2020 | 2021 |
|---|---|---|---|---|---|---|
| `fin_debt_to_assets` | **2016** | 0.760 | 0.785 | 0.798 | 0.815 | 0.837 |
| `fin_net_debt_to_mcap` | **2016** | 0.744 | 0.778 | 0.784 | 0.797 | 0.815 |
| `fin_negative_equity_exit` | **2017** | 0.750 | 0.775 | 0.788 | 0.805 | 0.825 |
| `fin_ext_finance_to_assets` | **2018** | 0.442 | 0.652 | 0.715 | 0.747 | 0.770 |
| `fin_lifecycle_stage` | **2018** | 0.439 | 0.647 | 0.712 | 0.745 | 0.770 |
| `fin_lifecycle_transition` | **2018** | 0.345 | 0.603 | 0.674 | 0.726 | 0.753 |
| `fin_profit_turn` | **2018** | 0.372 | 0.635 | 0.690 | 0.726 | 0.746 |
| `fin_interest_coverage` | **2019** | 0.324 | 0.476 | 0.551 | 0.587 | 0.623 |
| `fin_dividend_initiation` | **2019** | 0.000 | 0.001 | 0.533 | 0.762 | 0.791 |

2020~2025(약 5.4년)였던 표본이 **2016~2019 시작**으로 바뀌었다. 다섯 family는 holdout
경계(2025-08) 기준으로 표본이 대략 두 배가 된다.

---

## 4. 부수 관측 — 이번 변경과 무관한 것

`fin_net_debt_to_mcap`의 커버리지가 2022년 0.818에서 2026년 0.674까지 **내려간다.**
같은 기간 `fin_debt_to_assets`는 0.863 → 0.95로 올라간다. 차이는 분자의
`cash_and_cash_equivalents`뿐이다.

**v1에서도 같은 모양이었다**(2022 0.811 → 2026 0.665). 이번 변경이 만든 것이 아니라
원래 있던 것이고, 최근 연도에 현금 태그 커버리지가 떨어진다는 뜻이다. 다른 개념
이름으로 옮겨갔을 가능성이 있다 — F-5의 다음 항목 후보로 남긴다. 이번 판정에는 영향이
없다(2016년 시작은 초기 연도로 결정됐다).

---

## 5. 다음

- 이 lake(`data_lake_f50/`)는 **측정 전용이다. canonical로 승격하지 않는다.**
  F-HS-1은 `07` §7 규약대로 **새 snapshot**에서 돌고, 그때 새 규칙이 자연히 반영된다.
- F-HS-1 사전등록 카드에 family별 표본 시작(§3 결정 B 표)을 적는다.
- `../results/f4_fin_risk_verification.md`의 "5개 family가 2020년부터만 존재한다"는
  이 변경으로 해소됐다. 재생성은 새 snapshot에서 한다.
