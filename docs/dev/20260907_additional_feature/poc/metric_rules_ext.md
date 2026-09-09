# F-5 PoC — `metric_rules` 확장

- §0~§5: **F-5.0** 현금흐름 4 metric의 XBRL fallback (2026-09-08, 완료)
- §6~§9: **F-5.1** 신규 계정 태그 커버리지 (2026-09-09, 완료)

---

# F-5.0 — 현금흐름 4 metric의 XBRL fallback

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

---

# F-5.1 — 신규 계정 태그 커버리지

- 작성: 2026-09-09
- 대상: `03_financial_risk_lifecycle_transition.md` §3의 계정 넷 — `current_assets`,
  `current_liabilities`, `borrowings`, `rnd_expense`. **수집은 0이다.** lake snapshot
  2026-09-08(`sj2_remote`)의 `dart_xbrl_fact_raw`·`dart_financial_statement_raw`만 읽었다.
- 판정 규칙은 **F-5.0 §0의 것을 그대로 쓴다**(횡단면 커버리지 0.5, family별,
  2026-09-08 고정). 이번 측정을 위해 새로 고른 문턱이 아니다.

## 6. 측정

분모는 그 해에 자산총계(`ifrs-full_Assets`/`ifrs_Assets`)를 보고한 법인 수다 — 횡단면
피쳐가 실제로 채점될 모집단이다. 분자는 두 원천(`dart_financial_statement_raw`의
`account_id`, `dart_xbrl_fact_raw`의 `concept_id`) 합집합에서 값이 있는 법인 수다.

| 후보 | 2015 | 2016 | 2017 | 2018 | 2020 | 2022 | 2025 | 첫 0.5 |
|---|---|---|---|---|---|---|---|---|
| `total_liabilities` (참조) | 0.998 | 0.999 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 2015 |
| **`current_assets`** | 0.994 | 0.997 | 0.998 | 0.999 | 0.999 | 0.999 | 0.970 | **2015** |
| **`current_liabilities`** | 0.993 | 0.996 | 0.998 | 0.998 | 0.998 | 0.999 | 0.968 | **2015** |
| **`retained_earnings`** (추가) | 0.959 | 0.968 | 0.997 | 0.996 | 0.996 | 0.997 | 0.999 | **2015** |
| `borrowings_short_term` (2 철자) | 0.003 | 0.008 | 0.764 | 0.781 | 0.780 | 0.777 | 0.627 | 2017 |
| **`borrowings_short_term` (3 철자)** | 0.823 | 0.834 | 0.850 | 0.781 | 0.780 | 0.777 | 0.627 | **2015** |
| **`borrowings_long_term`** | 0.731 | 0.735 | 0.745 | 0.669 | 0.674 | 0.684 | 0.555 | **2015** |
| `borrowings_current_portion` | 0.427 | 0.444 | 0.466 | 0.402 | 0.394 | 0.410 | 0.368 | **없음** |
| `rnd_expense` | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.136 | **없음** |

### 6.1 또 철자였다

F-5.0의 교훈이 그대로 반복된다. 두 철자(`ifrs-full_` / `ifrs_`)만으로는 단기차입금의
2015~2016이 **0.003·0.008**이다. 그 두 해를 채우는 것은 세 번째 철자
`dart_ShortTermBorrowings`(48,251행, 1,673 법인, 2015~2017)이고, 붙이면 같은 해가
**0.823·0.834**가 된다. 2015년 커버리지가 260배 늘어난 것이 규칙 하나다.

### 6.2 `current_assets`는 분기까지 참조 metric과 같다

이 둘은 instant(BS) 값이라 TTM 연속 4분기를 요구하지 않는다 — F-5.0에서
`fin_net_debt_to_mcap`이 나머지 넷보다 한 해 빨랐던 것과 같은 이유다. 분기 보고서별
법인-연도 수도 참조 metric과 사실상 같다.

| reprt_code | `current_assets` | `total_liabilities` (참조) |
|---|---|---|
| 11011 (사업보고서) | 24,653 | 24,946 |
| 11012 (반기) | 23,926 | 24,171 |
| 11013 (1분기) | 23,657 | 23,906 |
| 11014 (3분기) | 22,113 | 22,423 |

### 6.3 차입금 합산은 이중계상이 없다

`borrowings`는 단일 태그가 없어 합산이 필요하다. 합이 유효한지 먼저 확인했다 —
사업보고서(11011)·연결(CFS)에서 세 값이 다 있는 **10,012 법인-연도** 기준:

- `단기 + 장기 > 부채총계`인 경우 **0건** (0.00%). 중위 비율은 0.411이다.
- 유동성장기차입금까지 더해도 초과 **0건**.

즉 이중계상은 없다. 반대로 **빠지는 쪽**은 있다: 유동성장기차입금을 뺀 합은
그 값이 있는 5,017 법인-연도에서 중위 **10.2%**, 90분위 **59.7%** 만큼 이자부부채를
과소계상한다.

## 7. 판정

### 결정 1: `current_assets` · `current_liabilities` — **통과**

2015년부터 0.99다. 참조 metric과 같은 급이고 분기까지 같다. `total_assets`·
`total_liabilities`와 똑같은 형태(주 규칙 = `dart_financial_statement_raw`, fallback =
`dart_xbrl_fact_raw` 두 철자)로 등록한다.

### 결정 2: `retained_earnings` — **추가하고 통과**

원래 목록에 없었다. `03` §3이 계정 넷만 적고 `fin_altman_z`를 "축소판이 아닌 완전판이
되면"이라는 조건부로 뒀는데, 그 조건을 정하는 값이 이익잉여금이라 같이 쟀다. 2015년
0.959다. 따라서 **Altman Z는 완전판으로 만들 수 있다** — 다섯 항이 모두 있다.

| Altman 항 | 재료 | 상태 |
|---|---|---|
| WC/TA | `current_assets` − `current_liabilities`, `total_assets` | 신규 2 + 기존 1 |
| RE/TA | `retained_earnings`, `total_assets` | **신규 1** + 기존 1 |
| EBIT/TA | `operating_income`, `total_assets` | 기존 |
| MVE/TL | `mcap_krx`, `total_liabilities` | 기존 |
| Sales/TA | `revenue`, `total_assets` | 기존 |

### 결정 3: `borrowings` — **단일 metric으로는 불가. 성분 2개로 등록한다**

매핑 규칙 모델은 `(metric, corp, period, basis)`마다 우선순위로 **하나**를 고른다.
합산을 표현할 수 없으므로 `borrowings` 하나를 등록하는 것은 애초에 불가능하다. 대신
성분을 각각 metric으로 등록하고 합은 `feat_fin_risk`에서 한다(마트는 이미 metric 간
산술을 한다).

- `borrowings_short_term` — 세 철자 필수. 통과(2015년 0.823)
- `borrowings_long_term` — `dart_LongTermBorrowingsGross` + `ifrs-full_LongtermBorrowings`.
  통과(2015년 0.731)
- `borrowings_current_portion` — **탈락**(최대 0.466, 문턱 0.5 미달). 필수 성분으로
  쓰지 않는다.

따라서 `fin_borrowings_to_mcap`의 분자는 `단기 + 장기`이고, 카드에 **"유동성장기차입금
제외 — 중위 10.2%, 90분위 59.7% 과소계상"**을 적는다. 문턱을 낮춰 성분을 살리는 쪽은
택하지 않았다: 0.4 커버리지 구간의 법인은 만기 구조를 자세히 공시하는 대형주에 쏠려
있어, 그 성분을 넣으면 분자의 정의가 종목 크기에 따라 달라진다.

### 결정 4: `rnd_expense` — **탈락**

- XBRL `ifrs-full_ResearchAndDevelopmentExpense`는 **2023년부터만** 있다(10,471행,
  447 법인). 2015~2022는 0.000이고 2025년에도 0.136이다.
- `dart_Capitalised...` 계열은 바이오 임상단계별 **자본화** 개발비 주석이지 경상연구개발비가
  아니다.
- `dart_financial_statement_raw` 쪽은 `account_id = '-표준계정코드 미사용-'` /
  `account_nm = '연구개발비'`가 5,639행 **203 법인**뿐이다. 표준계정코드를 안 쓴 값을
  계정명으로 잡는 것이라 원천이 바뀌면 조용히 깨진다.

그래서 F-5.4의 다섯 family 중 **둘은 만들지 않는다**: `fin_rnd_to_sales`,
`fin_bm_intangible_adj`(Peters-Taylor는 R&D 이력 자체가 재료다). 남는 것은 셋이다 —
`fin_current_ratio`, `fin_borrowings_to_mcap`, `fin_altman_z`(완전판).

## 8. 등록할 metric — 5개

| metric_code | 주 규칙(account_id) | XBRL fallback(concept_id) | 유효 시작 |
|---|---|---|---|
| `current_assets` | `ifrs-full_CurrentAssets` (BS) | `ifrs-full_CurrentAssets`, `ifrs_CurrentAssets` | 2015 |
| `current_liabilities` | `ifrs-full_CurrentLiabilities` (BS) | `ifrs-full_CurrentLiabilities`, `ifrs_CurrentLiabilities` | 2015 |
| `retained_earnings` | `ifrs-full_RetainedEarnings` (BS) | `ifrs-full_RetainedEarnings`, `ifrs_RetainedEarnings` | 2015 |
| `borrowings_short_term` | `ifrs-full_ShorttermBorrowings` (BS) | 위 + `ifrs_ShorttermBorrowings`, `dart_ShortTermBorrowings` | 2015 |
| `borrowings_long_term` | `dart_LongTermBorrowingsGross` (BS) | 위 + `ifrs-full_LongtermBorrowings` | 2015 |

`fs_div`는 CFS·OFS 둘 다 등록한다(F-5.0의 `XBRL_FALLBACK_CFS_PRIORITY` /
`XBRL_FALLBACK_OFS_PRIORITY` 구조를 그대로 쓴다).

## 9. 다음 (F-5.2 ~ F-5.4)

- F-5.2: `definitions/metric_rules.py`에 catalog 5 + 규칙 등록. **기존 29 metric의 규칙과
  golden은 건드리지 않는다.** 29 → 34가 된다.
- F-5.3: vintage 재빌드, 연도별 커버리지 실측(위 표는 raw 수준 대리 측정이다 — vintage는
  strict PIT 접수일을 거치므로 값이 조금 낮아질 수 있다)
- F-5.4: 파생 family **3개**(위 결정 4). F-4와 같은 config에 넣지 않는다 — 다음 F-HS
  config다.
