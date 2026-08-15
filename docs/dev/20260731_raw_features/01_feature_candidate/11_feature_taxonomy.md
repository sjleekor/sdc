# 11. 피쳐 카테고리 체계와 빈 칸 — 지금 25개는 어디에 있나

- 작성일: 2026-08-15 (갱신: 2026-08-15, 학계·업계 자료 조사 반영)
- 대상: `01_feature_candidate/` 에서 검증한 25 family (Phase A 17 + Phase B 8)
- 목적: 주가 예측에 쓰이는 정보를 카테고리로 나누고, **현재 25개가 그중 어디를 덮고
  어디가 비어 있는지** 좌표로 찍는다. 기존 문서는 손대지 않았다.
- 이 문서는 **지도이지 계약이 아니다.** 여기 적힌 후보를 실제로 검증하려면 `02` §6.2와
  `04_specific_plan_B.md` §12의 규율대로 **새 config로 따로 사전등록**해야 한다(§11).

---

## 1. 한 장 요약

**현재 25개는 "한 회사의 지금 상태"에 몰려 있다.** 축으로 보면 넷이 통째로 비어 있다.

| 비어 있는 것 | 현재 | 왜 |
|---|---|---|
| **상태 전이** (적자→흑자, 무배당→배당 개시) | **0** | 설계에는 있었으나(`02` Q2·Q6·Q8) 구현 안 됨 |
| **기업 간·산업 간 관계** | **0** | 업종 코드가 수집돼 있지 않다 |
| **거시·시장 상태의 조건화** | **0** | Phase 4 미착수 |
| **재무위험·레버리지** | **0** | **원천은 이미 canonical에 있다 — 그냥 안 만들었다** |

조사로 확인한 것 중 값이 큰 게 셋이다.

**① 이번 결과는 한국 시장에서 이미 알려진 패턴을 거의 그대로 재현했다.**
Han, Lee & Kang (2020)이 148개 이례현상을 한국에서 복제한 결과와 이번 등급이 카테고리 단위로
맞아떨어진다. 수익성 5.0%(엄격 기준 0.0%), 투자 24.1% 복제율이 `fin_gross_profitability`와
`fin_asset_growth_yoy`의 무신호를, 밸류 69.2%·거래마찰 48.1%가 `fin_value_z`와 px 계열 A등급을
예측한다. **데이터 결함이 아니라 시장 특성일 가능성이 크게 올라간다.** → §4

**② 업계·학계 표준 모델은 산업(industry)을 1급 블록으로 명시한다.** Barra는 country ·
**industry** · style 세 블록으로 구성되고, Gu, Kelly & Xiu (2020)는 94개 characteristic에
**74개 industry dummy**를 함께 넣는다. 현재 구조에는 그 블록이 통째로 없다. 확인한 사실이다.

- `stock_master`: ticker, market, name, status, listing_date … — **업종 없음**
- `dart_corp_master`: corp_code, ticker, corp_name, market, stock_name, modify_date … — **업종 없음**
- `common_feature_series`의 KRX 산업지수 4종은 **inactive, 데이터 0건**
- `operating_source_document` / `operating_metric_fact`(`sector_key` 보유)는 `repositories.py`에만
  남아 있고 `sql/postgres_ddl.sql`에도 lake 16개 테이블에도 없다 — 사실상 사문화
- 그 결과 `fin_scan.py`의 횡단 정규화 그룹이 전부 `PARTITION BY trade_date, market`이다.
  **그룹이 KOSPI/KOSDAQ 둘뿐이다.**

**③ 새 수집 없이 지금 당장 되는 카테고리가 둘 더 있다.** canonical metric 29종에
`total_liabilities`·`interest_paid`와 **3대 현금흐름이 전부 들어 있다.** 그런데

- 레버리지·재무위험 피쳐가 **0개**다(§9.1)
- Dickinson (2011)의 **기업 생애주기**(현금흐름 부호 패턴)도 **0개**다(§9.2).
  이건 사용자가 물은 "이익이 없다가 생기는" 전이를 정면으로 다루는 축이다

유가·물가 원천은 있고 **취업률·고용은 없다**(§8). 우선순위 결론은 §10에 있다.

---

## 2. 카테고리 체계

### 2.1 축 A — 정보의 범위 (누구에 관한 정보인가)

`02_feature_candidate.md`의 분류는 **원천 기준**(가격/수급/재무/이벤트/매크로)이다. 수집·구현
계획에는 맞지만 "무엇을 놓쳤나"를 묻기엔 맞지 않다. 같은 원천에서 전혀 다른 종류의 정보가
나오기 때문이다.

| ID | 카테고리 | 내용 | 현재 |
|---|---|---|---|
| **C1** | 기업 상태 — 시장가격 기반 | 모멘텀·반전·변동성·유동성 | **9** |
| **C2** | 기업 상태 — 재무 기반 | 규모·밸류·수익성·성장 / **레버리지 · 생애주기 · 무형** | **5** |
| **C3** | 수급·소유·내부자 | 투자자별 순매수, 공매도, 지분 / **내부자·최대주주** | **8** |
| **C4** | 이벤트·공시 | 발행·배당·자사주·실적 / **공시 텍스트** | **3** |
| **C5** | **관계 (기업 간·산업 간)** | peer 상대 위치, 리드래그, 공급망 | **0** |
| **C6** | **시장·거시 상태** | 지수·금리·유가·물가·환율·시장 폭 | **0** |
| **C7** | **시장구조·비재무** | 지수 편입·계절성·옵션 / 인적자본·지배구조 | **0** |
| (C8) | 실행·거래비용 | 알파가 아니라 비용 모형·필터 입력 | (겸업 3) |

굵게 표시한 것이 이번 조사로 새로 들어온 블록이다. C8은 별도 역할이다 —
`px_amihud_20d`(tradable 유지율 0.85), `px_zero_ret_ratio_20d`, `flow_days_to_cover`가
C1/C3이면서 동시에 여기 속한다.

### 2.2 축 B — 시간적 형태 (무엇을 재는가)

질문 1("이익이 발생하지 않다가 이익 시 증가")에 직접 걸리는 축이다.

| ID | 형태 | 질문 | 예 | 현재 |
|---|---|---|---|---|
| **T0** | 수준 (level) | 지금 얼마인가 | `fin_log_mcap`, `fin_value_z` | **12** |
| **T1** | 변화 (Δ) | 얼마나 변했나 | `px_mom_12_1`, `fin_asset_growth_yoy` | **10** |
| **T2** | 놀라움 (surprise) | 기대 대비 얼마나 다른가 | `px_turnover_shock`, `fin_sue` | **3** |
| **T3** | **상태 전이 (regime)** | **상태가 바뀌었는가** | 적자→흑자, 생애주기 단계 이동 | **0** |

### 2.3 축 C — 조건화

| ID | 형태 | 현재 |
|---|---|---|
| U | 무조건부 — 그 값 자체가 신호 | **25** |
| **X** | **조건부 — 다른 변수와의 상호작용에서만 신호** | **0** |

C6(거시)은 구조적으로 X에서만 살 수 있다. 라벨이 날짜×시장 내 rank이므로 같은 날 모든 종목에
같은 값이 broadcast되면 횡단 z-score 후 0이 된다. `02` §3.6의 진단이 정확하다.

### 2.4 이 체계를 업계·학계 표준과 대조하면

| 기준 | 구조 | 현재 커버 |
|---|---|---|
| **Barra / MSCI** (업계 표준 리스크 모델) | **country · industry · style** 3블록. style은 size, value, momentum, volatility, growth, **leverage**, liquidity 등 | style 일부만. **industry 블록 전무, leverage 전무** |
| **Gu, Kelly & Xiu (2020)** (ML 자산가격 표준 실험) | 94 characteristic + 8 macro 시계열 + **74 industry dummy** (900+ 신호) | characteristic 25. **industry dummy 0, macro 0** |
| **Green, Hand & Zhang (2017)** | 94 characteristic 동시 회귀. 미세소형주 과대가중 방지 + data-snooping 보정 | 규모·유동성 통제 설계는 유사(broad/tradable 이원 보고) |
| **Han, Lee & Kang (2020)** (한국 복제) | 148 이례현상을 6 카테고리로 — value, momentum, investment, profitability, trading friction, **intangible assets** | intangible 0 (§9.3) |

세 표준이 공통으로 두는 블록 중 **산업과 레버리지가 현재 구조에 없다.** 이게 §6·§9.1의
근거다. 국내 단일시장 모델에서는 country 블록이 상수라 빠지는 게 정상이지만, **industry는
단일시장이기 때문에 오히려 남는 유일한 그룹 축**이다.

---

## 3. 현재 25 family의 좌표

| # | family | 축 A | 축 B | 등급 |
|---|---|---|---|---|
| 1 | `px_idio_vol_60d` | C1 | T0 | A |
| 2 | `px_amihud_20d` | C1 / C8 | T0 | A |
| 3 | `px_maxret_20d` | C1 | T0 | A |
| 4 | `px_near_52w_high` | C1 | T0 | A |
| 5 | `px_zero_ret_ratio_20d` | C1 / C8 | T0 | R |
| 6 | `px_mom_12_1` | C1 | T1 | D |
| 7 | `px_resid_mom_12_1` | C1 | T1 | D |
| 8 | `px_reversal_5d` | C1 | T1 | A |
| 9 | `px_turnover_shock` | C1 | **T2** | D |
| 10 | `fin_log_mcap` | C2 | T0 | A |
| 11 | `fin_value_z` | C2 | T0 | B |
| 12 | `fin_gross_profitability` | C2 | T0 | C |
| 13 | `fin_accruals_to_assets` | C2 | T0 | C |
| 14 | `fin_asset_growth_yoy` | C2 | T1 | C |
| 15 | `flow_individual_netbuy_to_volume` | C3 | T1 | A |
| 16 | `flow_inst_netbuy_to_volume` | C3 | T1 | D |
| 17 | `flow_foreign_netbuy_to_volume` | C3 | T1 | D |
| 18 | `flow_foreign_holding_ratio_chg` | C3 | T1 | D |
| 19 | `flow_short_turnover` | C3 | T1 | C(보류) |
| 20 | `flow_short_interest` | C3 | T0 | C(보류) |
| 21 | `flow_days_to_cover` | C3 / C8 | T0 | C(보류) |
| 22 | `flow_nat_proxy_20d` | C3 | **T2** | C(보류) |
| 23 | `ev_payout_yield` | C4 | T0 | B |
| 24 | `ev_net_share_issuance_yoy` | C4 | T1 | A |
| 25 | `fin_sue` | C4 | **T2** | C(표본 0) |

집계하면 **C1 9 · C2 5 · C3 8 · C4 3 · C5 0 · C6 0 · C7 0**,
**T0 12 · T1 10 · T2 3 · T3 0**, **조건부 0**이다.

한 가지 더 읽힌다. **T2 세 개 중 둘(9·22)은 D 또는 보류, 하나(25)는 표본이 0이다.** 즉
"기대 대비 놀라움"을 재는 축은 지금 사실상 검증된 적이 없다. 결과가 나쁘다는 뜻이 아니라
**아직 측정되지 않았다**는 뜻이다.

---

## 4. 먼저 짚을 것 — 한국 복제율이 이번 결과를 거의 그대로 예측했다

조사에서 가장 값이 큰 자료다. Han, Lee & Kang (2020)은 문헌의 **148개 이례현상을 한국 시장
(KOSPI+KOSDAQ, 가치가중)에서 복제**하고 6개 카테고리로 나눠 복제율을 보고했다.

| 카테고리 | 개수 | 복제율 (t≥1.96) | 엄격 (t≥2.78) | 이번 검증에서 대응하는 것 | 이번 결과 |
|---|---:|---:|---:|---|---|
| Value | 13 | **69.2%** | 53.8% | `fin_value_z` | **B, 4셀 전부 유의** |
| Momentum | 15 | 66.7% | **26.7%** | `px_mom_12_1`, `px_resid_mom_12_1` | **D, 둘 다 부호 반대** |
| Trading friction | 54 | **48.1%** | 42.6% | `px_idio_vol`, `px_amihud`, `px_maxret`, `px_reversal` | **전부 A** |
| Intangible assets | 17 | 23.5% | 23.5% | — | **미개발 (§9.3)** |
| Investment | 29 | 24.1% | **10.3%** | `fin_asset_growth_yoy` | **q 0.85~1.00, 무신호** |
| Profitability | 20 | **5.0%** | **0.0%** | `fin_gross_profitability` | **q 0.30, 무신호** |

전체 복제율은 t≥1.96에서 37.8%, 다중검정 보정(t≥2.78) 후 27.7%다.

**읽는 방법이 셋이다.**

**① `fin_gross_profitability`와 `fin_asset_growth_yoy`의 무신호는 놀랄 일이 아니다.**
수익성 카테고리는 한국에서 20개 중 1개만 복제되고 엄격 기준으로는 **0개**다. 투자 카테고리도
29개 중 3개다. 지금 `09` §7은 gross profitability의 무신호를 coverage 0.0315 탓으로,
`10_known_issues.md` I7은 매핑 병목으로 돌린다. **둘 다 맞을 수 있지만, 매핑을 다 고쳐도
신호가 안 나올 확률이 상당히 높다.** I7 백필의 기대값을 이 숫자와 함께 다시 계산하는 게 좋다.

**② 반대로 살아남은 축은 한국에서 복제율이 높은 카테고리와 정확히 겹친다.** 밸류 69.2%,
거래마찰 48.1%. 이번에 A등급을 받은 `px_idio_vol_60d`·`px_amihud_20d`·`px_maxret_20d`·
`px_reversal_5d`가 전부 거래마찰이고, `fin_value_z`가 밸류다. **우연으로 보기 어렵다.**

**③ 모멘텀 전멸도 이 표와 어긋나지 않는다.** 모멘텀은 완화 기준에서 66.7%지만 엄격 기준에서
26.7%로 떨어진다. 같은 논문이 **KOSPI 단독 표본에서는 모멘텀이 거의 유의하지 않다**고
보고한다. `09` §4가 "우연으로 보기 어렵다"고 적은 판단을 외부 자료가 뒷받침한다.

**다만 같은 자료가 경고도 준다.** 저자들의 결론은 "이례현상 초과수익의 상당 부분을 data
mining이 설명한다"다. 148개 중 다중검정 후 살아남은 게 27.7%다. **이번 트랙이 사전등록·BH·
temporal placebo를 고집한 설계가 옳았다는 뜻이고, 동시에 새 카테고리를 붙일 때도 같은 잣대를
써야 한다는 뜻이다.**

---

## 5. 질문 1 — 피쳐 변화를 반영하는 구조인가

**부분적이다. 변화(T1)는 있고, 전이(T3)는 하나도 없다.** 형태적 이유가 셋 있다.

### 5.1 비율형 YoY는 부호가 바뀌는 순간 무의미해진다

`fin_asset_growth_yoy = 총자산 / 총자산(t−4q) − 1`은 분모가 항상 양수라 문제가 없다. 그런데
같은 형태를 이익 계열에 쓰면 깨진다. 순이익이 −100억에서 +50억이 되면 YoY는 −1.5다. 부호도
크기도 해석이 안 된다. −10억에서 −5억이 되면 −0.5로, **개선인데 음수**다.

**처방은 분모를 양수 기준으로 바꾸는 것이다.**

```text
fin_scaled_earnings_chg_4q = (net_income_ttm − net_income_ttm[t−4q]) / avg_assets
fin_op_margin_delta_4q     = op_margin − op_margin[t−4q]        # 분모가 매출액
fin_roa_delta_4q           = roa − roa[t−4q]
```

이 형태는 적자→흑자 전환에서 크고 양수인 값을 낸다. 지금 25개에는 이런 scaled change가
**한 개도 없다.**

### 5.2 재무는 수준만 있고 수준의 변화가 없다

`fin_scan.py`가 만드는 것은 `fin_log_mcap`, `fin_value_z`, `fin_gross_profitability`,
`fin_operating_profitability`, `fin_asset_growth_yoy`, `fin_accruals_to_assets`와 각각의
`_lag1`이다. 이 `_lag1`은 **지연 노출 검정(delay_pass)용**이지 변화 피쳐가 아니다.

`02` §3.4 Q2는 "level + YoY(Δ) 둘 다"라고 적었다. Q8 `fin_fscore_partial_7`은 ΔROA>0,
Δmargin>0, Δturnover>0 같은 **전형적인 전이 지표 묶음**이다. 둘 다 검증된 8개에 없다.

### 5.3 이산 전이가 없다

연속값 Δ만으로는 "없다가 생겼다"를 못 잡는다. 0에서 양수로 가는 첫 사건은 크기가 아니라
**사건 자체**가 정보다.

| 후보 | 정의 초안 | 원천 | 비고 |
|---|---|---|---|
| `fin_turn_to_profit` | 직전 4분기 TTM 영업이익 ≤ 0 이었다가 최신 TTM > 0 이 되는 첫 접수일부터 N일간 1 | canonical | 흑자 전환 |
| `fin_profit_streak_q` | 연속 흑자 분기 수 (상한 8) | canonical | 지속성 |
| `fin_improve_streak_q` | ΔROA > 0 연속 분기 수 | canonical | 개선의 누적 |
| `fin_earnings_vol_8q` | 8분기 ROA 표준편차 | canonical | 이익 안정성 |
| `ev_dividend_initiation` | 무배당 → 배당 개시 | shareholder_return | 신호 강도 큼 |
| `ev_retirement_initiation` | 자사주 최초 소각 | treasury_stock dim | `02` D4와 연결 |
| `fin_news_jump` | as-of join 계단이 점프한 날의 피쳐 변화량 | 기존 마트 | **새 정보의 크기 자체** |

마지막 `fin_news_jump`가 개념적으로 가장 흥미롭다. 현재 재무 피쳐는 접수일에 계단식으로
점프하는데, 그 **점프 크기**를 따로 노출하지 않는다. 점프가 곧 "그날 시장에 새로 들어온 정보의
양"이고, analyst forecast 없이 T2를 근사하는 우회로다. `fin_sue`가 표본 0으로 막혀 있는 지금
특히 값이 있다.

### 5.4 그리고 전이를 정면으로 다루는 표준 축이 하나 있다 — 생애주기

조사에서 나온 가장 직접적인 답이다. **Dickinson (2011)**은 영업·투자·재무 세 현금흐름의
**부호 조합**으로 기업을 Introduction / Growth / Mature / Shake-out / Decline 다섯 단계로
나눈다. 분포 가정이 필요 없고 계산이 단순하다.

**이건 사용자가 물은 전이를 그대로 표현한다.** "이익이 안 나다가 나기 시작하는" 기업은
Introduction(CFO−, CFI−, CFF+) → Growth(CFO+, CFI−, CFF+)로 **단계가 이동한다.** 단계 자체가
T0이고, 단계 이동이 T3다.

**그리고 지금 당장 계산된다.** canonical metric에 `operating_cash_flow`·`investing_cash_flow`·
`financing_cash_flow`가 전부 있다. 새 수집이 전혀 필요 없다. → §9.2

### 5.5 여기에 붙는 경고 — 생존편향

전이 피쳐는 생존편향에 **특히** 취약하다. `stock_master`의 DELISTED가 28개뿐이다
(`00_raw_feature_inventory.md` §2.7). 적자 기업 중 상장폐지된 쪽이 표본에서 빠지면 "적자에서
흑자로 돌아선 기업"만 남는다. `fin_turn_to_profit`의 성과가 구조적으로 부풀려진다.

**T3와 재무위험(§9.1)·생애주기 Decline 단계는 T0/T1 계열보다 PIT universe 보강
(`02` Phase 0-7)이 훨씬 강하게 선행 조건**이다. 지금 미해결이다.

### 5.6 부분적으로는 이미 반영돼 있다

공정하게 적으면 수급 쪽은 형태가 낫다. `flow_*_netbuy_to_volume`은 윈도 누적 변화이고
`flow_foreign_holding_ratio_chg`는 명시적 차분이다. `02` F4가 "외국인 보유 level 단독 비권장,
변화분 우선"이라고 적고 실제로 level을 뺀 것은 맞는 판단이었다.

---

## 6. 질문 2 — 사업영역별로 다르게 동작하는 것을 반영하나

**전혀 반영하지 않는다. 그리고 지금 데이터로는 반영할 방법이 없다.** 횡단 정규화 그룹이
`(trade_date, market)` 하나다. **은행·바이오·조선·게임이 전부 같은 KOSPI 풀에서 z-score를 받는다.**

### 6.1 업계·학계 기준으로 보면 이건 블록 하나가 빠진 것이다

§2.4에서 봤듯 Barra는 **industry를 country·style과 나란한 독립 블록**으로 두고 GICS 기준
45개 산업 팩터를 쓴다. Gu, Kelly & Xiu (2020)는 94개 characteristic에 **74개 industry
dummy**를 붙인다. 즉 산업은 "있으면 좋은 통제 변수"가 아니라 **표준 구성 요소**다.

단일 국가 모델이라 country 블록은 상수로 빠지는 게 정상이다. **그래서 industry가 남는 유일한
그룹 축인데, 그게 없다.**

### 6.2 이게 실제 결과를 어떻게 흐리고 있나

추측이 아니라 이번 run의 관측과 붙여 읽을 수 있는 지점이 넷 있다.

**① `fin_value_z`가 업종 더미를 재고 있을 수 있다.** B/M은 업종 중앙값 차이가 크다. 은행·지주는
구조적으로 높고 제약·소프트웨어는 낮다. 업종 통제 없이 시장 내 z를 쓰면 "싼 종목"이 아니라
"원래 B/M이 높은 업종"이 상위 분위를 채운다. `fin_value_z` ↔ `px_idio_vol_60d` 상관 −0.187도
그 방향과 어긋나지 않는다.

**② `fin_gross_profitability`는 금융업에서 개념이 성립하지 않는다.** 매출총이익이 정의되지
않거나 의미가 다르다. `02` §3.4 마지막 줄이 "지주회사·금융업은 계정 구조가 다름 — 섹터 더미
또는 제외"라고 적어뒀는데 미이행이다.

**③ `fin_accruals_to_assets`의 부호 반전은 업종 혼재를 1순위로 의심할 만하다.** 기대 −,
실제 +, 4개 셀 전부 통계적으로 뚜렷하다. Sloan(1996)의 발생액 이례현상은 제조업 중심 결과다.
금융·지주가 섞이면 발생액의 의미 자체가 달라진다. 지금은 "한국에서 반대인가, 매핑 문제인가"를
못 가른다고 적혀 있는데(`09` §7), **세 번째 후보가 빠져 있다 — 업종 혼재다.**

**④ `ev_payout_yield`도 업종별 배당 성향 차이가 크다.** 금융·통신·유틸리티가 구조적으로 높다.

**업종 통제 부재가 T2 트랙 해석의 최대 교란 요인이다.** 통계 게이트로는 안 걸러진다.
temporal placebo는 시점 정렬을 묻지 업종 혼재를 묻지 않는다.

### 6.3 수집 방법 세 가지

| 방법 | 비용 | PIT | 비고 |
|---|---|---|---|
| **A. DART `기업개황`(company.json)의 `induty_code`** | **가장 낮음** | ✗ (현재 시점 값) | 이미 `dart_corp_master`를 수집 중 — 컬럼 추가 수준 |
| B. KRX 업종분류 / 산업지수 구성종목 | 중간 | 스냅샷을 시점별로 쌓으면 준-PIT | 카탈로그의 inactive 산업지수 4종을 살리는 경로이기도 하다 |
| C. 수익률 상관 기반 통계적 클러스터 | 계산만 | **자연스럽게 PIT** | 해석이 어렵지만 룩어헤드가 원천적으로 없다 |

A는 싸지만 **과거 업종 변경 이력이 없다.** 사업 전환 기업에서 미래 정보가 샌다. 그래서 A로
시작하되 **`02` §0.2 원칙 2(룩어헤드 원천 차단)를 어기는 지점임을 명시하고, 업종을 alpha
피쳐가 아니라 정규화 그룹·진단 축으로만** 쓰는 게 맞다. 알파로 쓰려면 B나 C가 필요하다.

### 6.4 업종이 생기면 즉시 되는 것

1. **업종 중립화** — `(trade_date, market, industry)` 그룹 z-score. 기존 재무 피쳐를 그대로
   다시 돌리기만 하면 된다. ①②③④가 한 번에 검증된다.
2. **업종 내 상대 순위** — `fin_value_z_ind_rank` 등.
3. **업종 제외 정책** — 금융·지주를 뺀 표본에서 `fin_accruals` 부호를 재확인.
4. **업종별 조건부 IC** — 어떤 피쳐가 어떤 업종에서만 사는지. 피쳐가 아니라 진단이다.
5. §7의 관계 피쳐 전부.

**1번은 새 피쳐를 하나도 안 만들고 기존 결과의 해석을 바꾼다.** 비용 대비 효과가 가장 크다.

---

## 7. 질문 3 — 기업 간·산업 간 상호작용

**카테고리 C5가 통째로 비어 있다.** 현재 25개는 전부 "그 종목 자기 시계열 + 시장 내 순위"다.
`px_resid_mom_12_1`이 유일하게 남의 정보를 쓰는데, **시장(market) 잔차만** 걷어내고 업종은
안 걷어낸다.

### 7.1 두 갈래

**(a) 상대 위치** — peer 대비 얼마나 싼가·좋은가. §6.4의 업종 중립화가 여기다.

**(b) 전이·리드래그** — 한쪽 정보가 다른 쪽에 늦게 반영되는 것. 문헌 축이 두텁다.

- Moskowitz & Grinblatt (1999) — 산업 모멘텀
- Hou (2007) — 산업 내 대형주 → 소형주 리드래그
- Menzly & Ozbas (2010) — 공급망으로 연결된 산업 간 리드래그
- Cohen & Frazzini (2008) — customer-supplier 모멘텀
- Ali & Hirshleifer (2020) — 애널리스트를 공유하는 기업 간 모멘텀

**한국에서 특히 값이 있을 이유가 있다.** 이번에 개별 종목 모멘텀이 둘 다 반대 부호로 나왔고
(§4), 한국 복제 연구도 모멘텀이 KOSPI 단독에서 거의 유의하지 않다고 보고한다. 개별 수준에서
모멘텀이 죽은 시장에서 산업 수준 모멘텀은 살아 있는 경우가 문헌에 흔하다 — 개별 노이즈가
집계에서 상쇄되기 때문이다. **모멘텀 전멸이 관계 축을 볼 이유를 오히려 키운다.**

### 7.2 지금 데이터로 가능한 것 / 불가능한 것

| 후보 | 정의 초안 | 필요한 것 |
|---|---|---|
| `rel_industry_mom_20d` | 같은 업종 타 종목(자기 제외) 시총가중 평균 수익률 | 업종 코드 |
| `rel_mom_ex_industry` | 개별 누적수익 − 업종 누적수익 (업종 중립 모멘텀) | 업종 코드 |
| `rel_bigcap_lag_ret` | 같은 업종 시총 상위 K개의 lag 수익률 | 업종 코드 |
| `rel_industry_flow_20d` | **업종 단위 외국인·기관 순매수 강도** | 업종 코드 (flows raw는 이미 있다) |
| `rel_industry_dispersion` | 업종 내 수익률 분산 | 업종 코드 |
| `rel_peer_corr_lag_ret` | 과거 252일 수익률 상관 상위 K peer의 lag 수익률 | **업종 코드 불필요** |
| `rel_peer_corr_resid` | 통계적 peer 대비 잔차 수익률 | **업종 코드 불필요** |

`rel_industry_flow_20d`가 실용적으로 눈에 띈다. 개별 종목 수급은 이미 8개를 봤고 개인만
정방향이었다. **외국인·기관이 업종 단위로 움직이는 쏠림**은 개별 종목 순매수보다 선행할 수
있다. 원천(`krx_security_flow_raw`, 77M행)은 이미 있고 업종 코드만 붙이면 된다.

통계적 peer 두 개는 업종 코드 없이 지금 당장 된다. 2,800 × 2,800 상관행렬을 rolling으로
계산하는 비용이 있지만 DuckDB로 감당 가능하다. **PIT가 자연스럽다**는 장점도 있다.

**불가능한 것도 분명히 적는다.**

- 공급망(customer-supplier): DART 사업보고서 본문에 주요 매출처가 있으나 텍스트 파싱이 필요하다.
  `operating_source_document`가 원래 그 자리였는데 사문화됐다.
- 지분·계열 관계: DART 지분공시 API 미수집(§9.5).
- 애널리스트 커버리지: 원천 자체가 없다(§9.9).

### 7.3 왜 C5가 C6보다 우선인가

라벨이 **날짜×시장 내 rank**다. peer 평균·업종 rank는 같은 날짜 안에서 **종목마다 값이 다르다.**
즉 라벨을 직접 설명할 수 있다. 반면 거시 지표는 날짜 내 상수라 그대로는 못 쓴다.
**C5는 무조건부로 바로 쓸 수 있고, C6은 조건화를 거쳐야만 쓸 수 있다.**

---

## 8. 질문 4 — 거시경제 지표

### 8.1 원천 현황 — 유가·물가는 있고 고용은 없다

`common_feature_series` 33개 중 active 26개다.

| 그룹 | series | 빈도 | 시작 |
|---|---|---|---|
| 유가 | `commodity_wti`(FDR 선물), `commodity_wti_fred`(현물) | D | 2014-06 |
| 물가 | `macro_cpi`, `macro_ppi` | M | 2013-05 |
| 통화·심리 | `macro_m2`, `macro_consumer_sentiment` | M | 2013-05 |
| 금리 | `rate_kr_gov3y`, `rate_kr_gov10y`, `rate_us2y`, `rate_us10y` | D | 2014-06 |
| 환율 | `fx_usdkrw`, `fx_usdkrw_ecos` | D | 2014-06 |
| 글로벌 | `global_sp500`, `global_nasdaq`, `global_vix` | D | 2014-06 |
| 시장 상태 | 지수 3 + breadth 6 + turnover 2 | D | 2014-06 |
| **산업지수** | `industry_*` 4종 | — | **inactive, 데이터 0** |

**고용·취업률은 카탈로그에 없다.** ECOS로 수집은 가능하다.

### 8.2 원천은 있는데 왜 피쳐가 0개인가

`02` §3.6의 판단이 옳다. 날짜 내 상수는 횡단 z-score 후 0이 된다. 문제는 그 결론이
**"interaction으로만 쓴다"에서 멈췄고, 그 interaction이 한 개도 구현되지 않았다**는 것이다
(Phase 4 미착수). 현재 상태는 "매크로가 쓸모없다"가 아니라 **"아직 아무도 안 봤다"**이다.

### 8.3 빠져 있는 사용법 — exposure를 종목 피쳐로 만든다

`02` §3.6은 매크로 사용법을 **interaction 하나**로만 정식화했다. 방법은 셋이고, 가장 싸고
라벨과 정합적인 첫 번째가 빠져 있다.

| 방법 | 형태 | 날짜 내 상수인가 | 현재 |
|---|---|---|---|
| **① exposure(민감도)를 종목 피쳐로** | `rolling_beta(종목수익률, 매크로수익률)` | **아니다 — 종목마다 다르다** | **없음** |
| ② regime × exposure interaction | `cf_vix_high × px_downside_beta` | 아니다 | 없음 (설계만) |
| ③ 레짐별 조건부 IC | 피쳐가 아니라 검증 설계 | — | 없음 |

```text
macro_beta_wti_252d     = rolling_beta(r_i, wti_ret, 252d)
macro_beta_usdkrw_252d  = rolling_beta(r_i, usdkrw_ret, 252d)      # 수출/내수 대용
macro_beta_rate10y_252d = rolling_beta(r_i, Δrate_kr_gov10y, 252d) # 듀레이션·레버리지 대용
macro_beta_sp500_252d   = rolling_beta(r_i, lagged_sp500_ret, 252d)
```

**여기가 질문 2와 질문 4가 만나는 지점이다.** `macro_beta_usdkrw`는 업종 코드 없이 "수출주냐
내수주냐"를 데이터로 근사한다. `macro_beta_wti`는 에너지 노출을 근사한다. **업종 코드 수집
전에도 사업영역 이질성의 일부를 잡는 우회로**다.

주의: `px_beta_252d`는 `02` §3.1에 있지만 **시장 beta 하나뿐**이고 매크로 beta는 없다.

### 8.4 매크로 계열에 붙는 구조적 경고

이번 검증에서 유의한 신호를 실제로 떨어뜨린 관문은 사실상 **temporal placebo 하나**였다
(`09` §9.2). 24개 C 셀 전부가 그 문으로 왔다. 매크로 조건부 피쳐는 이 게이트를 **훨씬 통과하기
어렵다.**

- 월간 시리즈(CPI·PPI·M2·심리)는 자기상관이 극도로 높다. 유효 표본이 관측 수보다 훨씬 적다.
- 레짐 변수는 정의상 긴 구간 동안 같은 값을 유지한다. 시계열을 밀어놔도 비슷한 신호가 나온다.
- `ev_payout_yield`가 IC·ICIR·NW t 전부 좋았는데 placebo p 0.27로 떨어진 것과 **같은 실패
  모드에 정면으로 노출된다.**

반대로 **exposure(①)는 종목별 rolling beta라 횡단 변동이 크고 시계열 상수도 아니다.**
①을 먼저 하고 ②를 나중에 하는 근거다.

### 8.5 고용 지표는 우선순위가 낮다

취업률·고용률은 ECOS로 추가 수집이 가능하지만 순위는 뒤다. 월간이고 발표 지연이 커서 §8.4의
문제를 그대로 안고, 이미 있는 `macro_consumer_sentiment`·`macro_m2`와 정보가 상당히 겹칠 것이다.
**레짐 변수의 종류를 늘리는 것보다 조건화 구조를 한 번이라도 돌려보는 게 먼저다.**

---

## 9. 조사로 새로 나온 카테고리

사용자가 제시한 네 범위 밖에서, 학계·업계가 표준으로 쓰는데 현재 0개인 블록들이다.
**수집 비용 순으로 적었다.**

### 9.1 재무위험·레버리지 — 새 수집 없이 지금 당장 (비용 0)

**현재 25개에 부채 관련 피쳐가 한 개도 없다.** Barra는 **Leverage를 style factor로 명시**하고,
Campbell, Hilscher & Szilagyi (2008)의 distress risk는 "부실 위험이 높은 종목이 오히려 낮은
수익을 낸다"는 잘 알려진 이례현상이다.

**그런데 원천이 이미 canonical에 있다.** metric 29종을 확인했다.

| 후보 | 산식 | 필요한 metric | 상태 |
|---|---|---|---|
| `fin_debt_to_equity` | `total_liabilities / total_equity` | 둘 다 있음 | **즉시 가능** |
| `fin_debt_to_assets` | `total_liabilities / total_assets` | 둘 다 있음 | **즉시 가능** |
| `fin_net_debt_to_mcap` | `(total_liabilities − cash) / market_cap` | 있음 | **즉시 가능** (이자부부채 아닌 총부채 근사 — `02` V6과 같은 한계) |
| `fin_interest_coverage` | `operating_income_ttm / interest_paid_ttm` | 둘 다 있음 | **즉시 가능** |
| `fin_ext_finance_dependence` | `(차입조달 − 차입상환 + 발행) / avg_assets` | `borrowing_proceeds/repayments_long_term` 있음 | **즉시 가능** |
| `fin_altman_z_partial` | Altman Z 축소판 | **운전자본(유동자산·유동부채) 미매핑** | 부분만 |

마지막 줄이 `02` Q8이 지적한 것과 같은 병목이다. 유동자산/유동부채가 canonical에 없어
Piotroski 9항목도, Altman Z 완전판도 안 된다. **`metric_rules` 확장의 우선순위 1번은 여기다.**

한국 복제 표(§4)에 레버리지 카테고리는 따로 없다 — Han et al.의 6분류에서 investment·
trading friction에 흩어져 있다. 즉 **기대값을 밸류만큼 높게 잡을 근거는 없다.** 다만
**비용이 0에 가깝고, `macro_beta_rate10y`(§8.3)의 fundamental 대응물**이라 같이 보면 값이 있다.

### 9.2 기업 생애주기 — 새 수집 없이 지금 당장 (비용 0)

Dickinson (2011)의 현금흐름 부호 패턴이다. 세 현금흐름 부호 조합 8개를 5단계로 매핑한다.

| 단계 | CFO | CFI | CFF |
|---|:--:|:--:|:--:|
| Introduction | − | − | + |
| Growth | + | − | + |
| Mature | + | − | − |
| Shake-out | (나머지 조합) | | |
| Decline | − | + | +/− |

**canonical에 `operating_cash_flow`·`investing_cash_flow`·`financing_cash_flow`가 전부 있다.**
분포 가정도 추정도 필요 없고 부호만 보면 된다.

값이 큰 이유가 셋이다.

1. **§5의 전이 질문에 정면으로 답한다.** 단계는 T0이고, **단계 이동은 T3**다.
   `fin_lifecycle_stage`(범주형) + `fin_lifecycle_transition`(직전 대비 이동 flag) 두 개가 나온다.
2. **성숙기 저평가**가 원 논문의 결론이다 — 투자자가 현금흐름 패턴 정보를 완전히 반영하지 않아
   mature 기업이 저평가된다는 보고다.
3. **조건화 축으로 쓸 수 있다.** 밸류·수익성 피쳐가 생애주기 단계별로 다르게 작동한다는 것이
   이 문헌의 기본 전제다. 즉 §6의 업종 이질성과 **다른 종류의 이질성**을 잡는다. 업종 코드가
   없는 지금 특히 값이 있다.

주의: Decline 단계는 상장폐지 직전 기업이 많다. §5.5의 생존편향이 여기 직격한다.

### 9.3 무형자산·혁신 — metric_rules 확장 필요

Han et al.의 6분류 중 **intangible assets 17개(복제율 23.5%)**가 현재 0개다. 밸류·수익성보다는
낮지만 투자(10.3%)·수익성(0.0%) 카테고리보다 엄격 기준 복제율이 **높다.**

문헌 축은 무형자산이 장부가에 안 잡혀 **B/M 자체가 오측정된다**는 것이다. Peters & Taylor
(2017)는 R&D와 SG&A를 자본화해 지식자본·조직자본을 추정하고, Eisfeldt, Kim & Papanikolaou는
이를 반영한 `HML_INT`가 고전 HML보다 낫다고 보고한다.

지금 상태:

- `sga`(판매비와관리비)는 **canonical에 있다** → 조직자본 근사의 절반은 된다
- **R&D(경상연구개발비)는 미매핑**이다. `dart_financial_statement_raw`·`dart_xbrl_fact_raw`에
  계정은 있을 가능성이 높다(인벤토리 §2.4가 "판관비 세부 미매핑"을 3순위로 지목)
- 특허 데이터는 원천 없음

즉 **`metric_rules`에 R&D를 추가하면 `fin_intangible_adj_bm`이 열린다.** §9.1의 운전자본과
같은 작업 묶음이다.

### 9.4 공시 텍스트 — 원천 일부 있음, 파싱 필요

Cohen, Malloy & Nguyen (2020) "Lazy Prices"가 대표다. 정기보고서의 **문구가 바뀌었는지**만
봐도 예측력이 나온다는 결과다 — 바뀐 기업을 팔고 안 바뀐 기업을 사는 포트폴리오가 월 최대
188bp 알파를 냈고, 변화는 미래 이익·부도까지 예측했다. **공시 시점에 반응이 없고 나중에
드러난다**는 점에서 지연 반영 패턴이다.

지금 상태:

- `dart_filing_receipt_raw`(2015~2026)가 **수집돼 있는데 알파 피쳐로는 한 개도 안 쓰인다.**
  현재 용도는 `fin_sue_event`와 Phase B 품질 진단뿐이다
- 본문 원문은 DART `공시서류원본파일` API로 받을 수 있으나 **미수집**이다. 용량이 크다

**텍스트 없이 접수 메타데이터만으로 되는 것부터 하는 게 맞다.**

| 후보 | 정의 | 원천 |
|---|---|---|
| `ev_filing_count_60d` | 60일 공시 건수 (정보 흐름 강도) | filing_receipt |
| `ev_filing_burst` | 공시 건수의 60일 중앙값 대비 급증 | filing_receipt |
| `ev_amendment_ratio_1y` | 정정공시 비율 (보고 품질) | filing_receipt |
| `ev_material_event_flag` | `report_nm` 분류 — 유상증자 결정, 단일판매·공급계약, 소송 등 | filing_receipt |

`ev_amendment_ratio_1y`는 이미 Phase B가 `revision_ratio`로 **품질 지표로만** 쓰고 있다
(`fin_value_z`·`ev_payout_yield`의 A등급을 막은 그 값이다). **같은 숫자를 피쳐로도 볼 수
있다** — 정정이 잦은 기업은 보고 품질이 낮다는 가설이다.

### 9.5 내부자·지분구조 — DART API 있음, 미수집

문헌은 일관된다. 내부자 매수는 미래 수익을 예측하고, **매수가 매도보다 정보량이 크다**
(매도는 유동성·분산 목적이 섞인다). 최근 연구는 내부자 거래가 이례현상 롱숏 종목에서 미래
이례현상 수익까지 예측한다고 보고한다.

**DART에 API가 이미 있다. 수집만 안 하고 있다.**

| API | 내용 | 그룹 |
|---|---|---|
| `elestock` | **임원·주요주주 특정증권등 소유상황보고** | DS004 지분공시 |
| `majorstock` | **대량보유 상황보고**(5% rule) | DS004 지분공시 |
| `hyslr_sttus` | 최대주주 현황 | DS002 정기보고서 |
| `hyslr_chg_sttus` | 최대주주 변동 현황 | DS002 정기보고서 |

`elestock`이 특히 값이 크다. **보고 의무가 있어 이벤트 시점이 명확하고, PIT 처리가 깨끗하다.**
현재 `dart_filing_receipt_raw`를 이미 수집하고 있으니 수집 파이프라인 패턴도 그대로 쓸 수 있다.

후보: `ins_net_buy_90d`(내부자 순매수 / 시총), `ins_buy_count_90d`, `own_major_stake_chg`,
`own_5pct_holder_chg`.

### 9.6 인적자본·지배구조 — DART API 있음, 미수집

| API | 내용 |
|---|---|
| `emp_sttus` | **직원 현황** — 직원 수, 평균 근속연수, 1인 평균 급여 |
| `exctv_sttus` | 임원 현황 |
| `hmv_audit_indvdl_by_sttus` | 개인별 보수 |
| `accnut_adtor_nm_nd_adt_opinion` | 감사인·감사의견 |

업계에서 workforce analytics(고용 추이·이직률·채용공고)를 대체데이터로 비싸게 사는데,
**한국은 직원 수와 1인당 급여가 정기보고서에 공시된다.** 연 단위·저빈도지만 무료다.

후보: `hc_employee_growth_yoy`, `hc_revenue_per_employee`, `hc_avg_pay_growth`,
`gov_audit_opinion_flag`(비적정 의견 = 강한 부실 신호).

**여기에 시의성 있는 축이 하나 더 있다 — 밸류업.** 2024년 정부가 기업가치 제고 계획 공시를
도입했고 2026년 7월 기준 749개사가 공시했다. 2024-09에는 밸류업 지수 100종목이 출시됐다.
**`ev_payout_yield`·`ev_net_share_issuance_yoy`가 이 레짐 변화 위에 놓여 있다.**
`02` D3이 "2025~ 밸류업 정책으로 소각 증가 — 레짐 변화 유의"라고 이미 적었는데, 밸류업 공시
여부 자체가 피쳐가 될 수 있다. 다만 표본이 2024년 이후뿐이라 **지금 검정력이 거의 없다.**

### 9.7 옵션·파생 시장 — 원천 미수집

옵션 시장 정보가 주식 수익률을 선행한다는 결과가 두텁다. IV skew(OTM put − ATM call)는
음(−)의 예측력, IV spread(ATM call − ATM put)는 양(+)의 예측력을 보이고, 정보거래자가 옵션
시장을 먼저 쓰기 때문이라는 해석이 표준이다.

**한국은 KOSPI200 옵션 시장이 크지만, 개별 종목 옵션은 사실상 없다.** 즉 **횡단면 피쳐로는
못 쓰고 시장 레짐 변수로만 쓸 수 있다** — C6에 속하고 §8.4의 경고를 그대로 받는다.
우선순위는 낮다.

### 9.8 계절성·시장구조 — 원천 불필요 (비용 0)

Heston & Sadka (2008)는 **어떤 종목이 매년 같은 달에 상대적으로 높은(낮은) 수익을 낸다**는
패턴을 보고했다. 산업·규모·실적발표와 독립이고, 캐나다·일본·유럽 12개국에서도 확인됐다.

**추가 수집이 전혀 필요 없다.** `daily_ohlcv`만으로 계산된다.

```text
seas_same_month_ret = mean(과거 k년 같은 달의 초과수익)      # k = 5~20
seas_same_month_rank = 그 값의 (trade_date, market) 내 rank
```

같은 자리에 캘린더 축이 더 있다 — 분기말·연말 window dressing, 배당락일, 지수 정기변경일,
공시 시즌. 전부 `docs/holidays_krx.csv`와 거래일 캘린더로 만들 수 있다.

**단, `02` §7의 규율과 충돌할 소지가 있다.** k(과거 몇 년)를 여러 개 시도하면 window grid
search다. **k를 하나로 사전등록**해야 한다.

### 9.9 원천이 아예 없는 것 — 하지 않는다고 적어둔다

| 카테고리 | 왜 못 하나 | 대체 |
|---|---|---|
| 애널리스트 컨센서스·추정치 수정 | 원천 없음(유료) | `fin_sue`가 seasonal random walk로 근사 중 |
| 투자자 주의(Google 검색량 등) | 원천 없음 | `px_turnover_shock`·거래량 급증이 부분 대용 |
| 뉴스·소셜 감성 | 원천 없음 | 공시 텍스트(§9.4)가 가장 가까운 무료 대체재 |
| 대체데이터(위성·카드결제·웹트래픽·채용공고) | 비용이 크고 개인 접근이 어렵다 | 직원 현황(§9.6)이 workforce의 무료 근사 |
| 일중 미시구조(호가·체결) | **스코프 제외**(`ports/prices.py` 스텁) | — |
| ESG 등급 | 원천 없음(유료) | 감사의견·지배구조 공시가 부분 대용 |

이걸 적어두는 이유는, **없는 것을 없다고 알고 있어야 대체재를 찾기 때문**이다. 실제로
§9.4·§9.6이 유료 대체데이터의 무료 근사에 해당한다.

---

## 10. 우선순위 제안

기대 정보량 ÷ 구현 비용, 그리고 **기존 결과의 해석을 얼마나 바꾸는가**로 매겼다.
1~3순위는 **새 수집이 전혀 없다.**

### 1순위 — 업종 코드 수집과 업종 중립 재정규화 (§6)

- 비용: DART `기업개황`의 `induty_code`. 이미 `dart_corp_master`를 수집 중이라 컬럼 추가 수준
- 효과: **새 피쳐를 하나도 안 만들고** §6.2의 ①②③④를 전부 검증한다.
  `fin_accruals` 부호 반전, `fin_gross_profitability` 무신호, `fin_value_z`의 정체가 갈린다
- 업계·학계 표준 모델이 공통으로 두는 블록이다(§2.4). C5(§7) 전체의 선행 조건이기도 하다
- 한계: PIT가 아니다. **정규화 그룹·진단 축으로만 쓰고 alpha 피쳐로 쓰지 않는다**는 선을
  처음부터 긋는다

### 2순위 — 생애주기 + 재무위험 (§9.2, §9.1)

- 비용: **0. canonical metric만으로 된다**
- 생애주기는 §5의 전이 질문에 정면으로 답하고, **업종 코드 없이 잡을 수 있는 다른 종류의
  이질성**을 준다
- 레버리지는 Barra style factor인데 현재 0개다
- 선행: `10_known_issues.md` I1 수정, 그리고 Decline 단계·부실 종목은 **PIT universe 보강**(§5.5)

### 3순위 — 재무 상태 전이·Δ 피쳐군과 매크로 exposure (§5.3, §8.3)

- 둘 다 새 원천 불필요
- Δ·전이는 기존 `feat_fin_scan_daily` 위에 얹는다. `fin_news_jump`가 `fin_sue` 표본 0을 우회한다
- `macro_beta_*`는 라벨 정합적이고 무조건부로 바로 검증된다. 업종 코드 없이도 사업영역
  이질성의 일부를 잡는다

### 4순위 — 관계 피쳐 (§7)

- 업종 코드가 생긴 뒤: 산업 모멘텀, 대형주 리드래그, **업종 단위 수급 쏠림**
- 업종 코드 없이 지금 가능: 통계적 peer 리드래그
- 개별 모멘텀 전멸이 이 축의 기대값을 오히려 높인다

### 5순위 — 공시 메타데이터와 내부자 (§9.4, §9.5)

- `dart_filing_receipt_raw`는 **이미 수집돼 있고 알파로 미사용**이다 → 여기부터
- `elestock`·`majorstock`은 새 수집이지만 이벤트 시점이 명확해 PIT가 깨끗하다

### 6순위 — metric_rules 확장 (운전자본 · R&D)

- 운전자본이 열리면 Piotroski 9항목과 Altman Z가 완성된다(§9.1)
- R&D가 열리면 무형자산 조정 밸류가 열린다(§9.3)
- 단, §4의 복제율(수익성 0.0%, 투자 10.3%)을 보면 **기대값을 낮게 잡는 게 맞다**

### 7순위 — 계절성 (§9.8)

원천 불필요라 비용은 0인데, `02` §7의 window grid search 금지와 충돌 소지가 있어 사전등록을
특히 조심해야 한다.

### 8순위 이하 — 인적자본·밸류업(§9.6), 옵션(§9.7), 고용 지표(§8.5)

표본이 짧거나(밸류업 2024~), 횡단면에 못 쓰거나(옵션), 기존 매크로와 겹친다(고용).

---

## 11. 규율 — 이걸 지금 다 붙이면 안 되는 이유

**BH 모집단이 커지면 기존 발견의 문턱이 올라간다.** 이번에 `m_ab=113`에서 discovery 45개가
나왔고 Phase A 31 + Phase B 14가 하나도 강등되지 않았다(`00_status.md` §4). family를 늘리면
그 안전 여유가 줄어든다.

§4의 자료가 이 규율을 정확히 뒷받침한다. Han et al.은 148개 이례현상 중 **다중검정 보정 후
27.7%만 살아남았고**, 결론을 "data mining이 초과수익의 상당 부분을 설명한다"로 냈다.
**카테고리를 늘리는 것은 곧 검정 수를 늘리는 것**이다.

그래서 새 카테고리는 반드시 아래를 지킨다.

1. **새 config로 별도 사전등록.** 기존 run의 config_hash `e55c3046…`를 건드리지 않는다.
2. **family당 대표 산식 하나 + 사전 등록된 window만.** 업종 정의를 여러 개 만들어 놓고 제일 잘
   나오는 걸 고르는 건 window grid search와 같은 위반이다. 계절성의 k(§9.8), 생애주기 5단계
   대 8패턴 선택(§9.2)이 같은 함정이다.
3. **부호를 먼저 고정한다.** 예외는 근거가 갈리는 경우뿐이고, 그때는
   `flow_individual_netbuy_to_volume`처럼 **미고정임을 사전에 적는다.** 생애주기 단계별 방향과
   distress risk 부호(문헌은 음(−)이지만 위험 보상 논리로는 양(+))가 여기 해당한다.
4. **holdout은 여전히 한 번만.** feature·horizon·variant·interaction 선택이 전부 끝난 뒤다.
5. 1순위(업종 중립 재정규화)는 예외적으로 **새 발견이 아니라 기존 셀의 재해석**이다. 그래도
   기존 판정을 덮어쓰지 않고 별도 run으로 남긴다.
6. **T3·재무위험·생애주기 Decline은 PIT universe 보강 없이 결론 내지 않는다**(§5.5).

그리고 **지금 크리티컬 패스는 Phase C·acceptance gate 인계**(`00_status.md` §5-1b)다. 이 문서의
후보들은 그 판단을 대체하지 않는다. 인계가 끝난 뒤에 여는 게 맞다.

---

## 12. 한 장 정리

| 축 | 현재 | 진단 |
|---|---|---|
| C1 기업 상태 — 가격 | 9 | 가장 두텁다. 한국 복제율이 높은 거래마찰 카테고리와 겹친다 |
| C2 기업 상태 — 재무 | 5 | 밸류만 살았다. **레버리지·생애주기·무형이 전부 0인데 앞 둘은 비용 0** |
| C3 수급·소유·내부자 | 8 | 한국 특화 축. **내부자·최대주주는 DART API가 있는데 미수집** |
| C4 이벤트·공시 | 3 | `fin_sue` 표본 0. **`dart_filing_receipt_raw` 수집돼 있는데 알파로 미사용** |
| **C5 관계** | **0** | 업종 코드가 막고 있다. 통계적 peer는 지금도 가능 |
| **C6 거시** | **0** | 원천은 있다. exposure(①)가 빠졌고 조건화는 미착수 |
| **C7 시장구조·비재무** | **0** | 계절성은 원천 불필요. 인적자본은 DART에 있다 |
| T0 수준 | 12 | |
| T1 변화 | 10 | 수급 쪽 형태가 낫다 |
| T2 놀라움 | 3 | 셋 다 검증 결과 없음(D·보류·표본 0) |
| **T3 전이** | **0** | 비율형 YoY로는 부호 전환을 표현 못 한다. **생애주기가 가장 싼 진입로** |
| **조건부** | **0** | C6이 살 수 있는 유일한 형태인데 비어 있다 |

관련 문서: 후보 정의 `02`, 검증 설계 `03`, 사전등록 계약 `04_*`, 결과 요약 `09`,
알려진 결함 `10`, 현재 진행 상태 `00_status.md`.

---

## 부록. 이 문서에서 새로 인용한 자료

**한국 시장 복제 (§4의 근거)**

- [Han, Lee & Kang (2020), "Market anomalies in the Korean stock market", *Journal of Derivatives and Quantitative Studies* 28(2)](https://www.emerald.com/jdqs/article/28/2/3/206237/Market-anomalies-in-the-Korean-stock-market) — 148 이례현상 · 6 카테고리 복제율
- [Eom (2022), "Empirical Asset Pricing in Korean Stock Markets: A Review of Models and Anomalies"](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4207303)

**표준 모델 구조 (§2.4의 근거)**

- [MSCI Barra Equity Factor Models](https://www.msci.com/data-and-analytics/factor-investing/equity-factor-models) — country · industry · style 3블록, GICS 45 산업 팩터
- [Gu, Kelly & Xiu (2020), "Empirical Asset Pricing via Machine Learning", *RFS* 33(5)](https://academic.oup.com/rfs/article/33/5/2223/5758276) — 94 characteristic + 8 macro + 74 industry dummy
- [Green, Hand & Zhang (2017), *RFS* 30(12)](https://academic.oup.com/rfs/article-abstract/30/12/4389/3091648) — 94 characteristic 동시 회귀

**새 카테고리 (§9)**

- [Dickinson (2011), "Cash Flow Patterns as a Proxy for Firm Life Cycle", *The Accounting Review* 86](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=755804) — 생애주기
- [Campbell, Hilscher & Szilagyi (2008), "In Search of Distress Risk", *JF*](https://www.nber.org/system/files/working_papers/w12362/w12362.pdf) — 부실 위험
- [Eisfeldt, Kim & Papanikolaou, "Intangible Value"](https://www.nber.org/system/files/working_papers/w28056/revisions/w28056.rev1.pdf); Peters & Taylor (2017) — 무형자산 조정 밸류
- [Cohen, Malloy & Nguyen (2020), "Lazy Prices", *JF* 75(3)](https://onlinelibrary.wiley.com/doi/abs/10.1111/jofi.12885) — 공시 문구 변화
- [Heston & Sadka (2008), "Seasonality in the cross-section of stock returns", *JFE*](https://www.sciencedirect.com/science/article/abs/pii/S0304405X0700195X) — 계절성
- Xing, Zhang & Zhao (2010); Cremers & Weinbaum (2010) — 옵션 IV skew / spread
- Da, Engelberg & Gao (2011), "In Search of Attention" — 투자자 주의

**관계 피쳐 (§7)**

- Moskowitz & Grinblatt (1999) — 산업 모멘텀
- Hou (2007) — 산업 내 대형주-소형주 리드래그
- Menzly & Ozbas (2010) — 공급망 연결 산업 간 리드래그
- Cohen & Frazzini (2008) — customer-supplier 모멘텀
- Ali & Hirshleifer (2020) — 애널리스트 공유 기업 간 모멘텀

**데이터 원천 (§9.5, §9.6)**

- [OpenDART 오픈API 목록](https://opendart.fss.or.kr/intro/infoApiList.do) — DS001~DS006 6개 그룹
- [dart-fss API 문서](https://dart-fss.readthedocs.io/en/latest/dart_api.html) — `elestock`, `majorstock`, `emp_sttus`, `exctv_sttus`, `hyslr_sttus` 등
- [KRX 밸류업 지수 출시 (2024-09)](https://www.kedglobal.com/korean-stock-market/newsView/ked202409250005)
