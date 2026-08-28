# 09. 전체 피쳐 검증 결과 — 25개 family 한눈에 보기

- 작성일: 2026-08-13 (갱신: 2026-08-27)
- 대상 실행: snapshot `2026-08-23` / source `sj2_remote` / config `ab0de634…`
  - Phase A `20260823T210913-b649a460` (px·flow 17 family, content `46ccf585…`)
  - Phase B `20260823T221441-b649a460` (fin·ev 8 family, content `f556dd3d…`)
  - 결합 AB `20260823T225913-b649a460` (content `e380d931…`, 등급 확정)
- 이 실행은 I7 `fin_v4`, native scan과 Phase A permutation artifact 재사용을 반영했다.
  기존 25 family·113가설을 다시 검정했으며 새 원천 후보는 추가하지 않았다.
- 8월 27일 같은 입력으로 A `20260827T082015-b649a460` → B
  `20260827T092909-b649a460` → AB `20260827T101418-b649a460`를 다시 돌렸다. primary 표와
  permutation·판정은 정확히 같았다. non-overlap·rank correlation의 최대 차이는
  `1.11e-16`이라 canonical 결과표와 등급은 8월 23일 run을 그대로 유지한다.
- 이 문서는 **결과 요약본**이다. 왜 그렇게 판정했는지의 계약은 `04_specific_plan_A.md` /
  `04_specific_plan_B.md`, 실행 기록은 `08_phase_b_implementation_log.md`에 있다.
  T1 6개의 일반 해설은 `05_phase_a_results_explained.md`가 더 자세하다.

---

## 1. 한 장 요약

25개 family를 전부 넣었다. IC는 **등급이 확정된 셀** 기준이고, 부호까지 그대로 적었다.
대부분은 |IC|가 가장 큰 셀과 같지만 T2 4개(`ev_payout_yield`·`fin_value_z`·
`fin_gross_profitability`·`ev_net_share_issuance_yoy`)는 다르다 — 이들은 |IC| 최대 셀이
누적 0–120일인데
강건성에서 떨어져 C이고, 등급이 붙은 것은 40–60일 셀이다(`10_known_issues.md` I8).

| # | family | 무엇을 재나 | 원천 | 기대 | 실제 | 대표 구간 | IC | 등급 |
|---|---|---|---|---|---|---|---|---|
| 1 | `px_idio_vol_60d` | 60일 특이변동성 | OHLCV | − | − | 0–60일 | **−0.1524** | **A** |
| 2 | `px_amihud_20d` | Amihud 비유동성 | OHLCV | + | + | 0–120일 | **+0.1343** | **A** |
| 3 | `fin_log_mcap` | 규모(로그 시총) | DART+OHLCV | − | − | 0–120일 | **−0.1149** | **A** |
| 4 | `px_maxret_20d` | 20일 최대 일간수익률 | OHLCV | − | − | 0–60일 | **−0.1162** | **A** |
| 5 | `fin_value_z` | 밸류 종합 z | DART+OHLCV | + | + | 40–60일 | **+0.0599** | **B** |
| 6 | `px_reversal_5d` | 5일 단기반전 | OHLCV | + | + | 0–3일 | **+0.0533** | **A** |
| 7 | `ev_payout_yield` | 주주환원 수익률 | DART | + | + | 40–60일 | +0.0550 | **B** |
| 8 | `px_near_52w_high` | 52주 신고가 근접도 | OHLCV | + | + | 40–60일 | +0.0325 | **A** |
| 9 | `flow_individual_netbuy_to_volume` | 개인 순매수 강도 | KRX flow | 미고정 | + | 0–20일 | +0.0241 | **A** |
| 10 | `ev_net_share_issuance_yoy` | 순주식발행 | DART | − | − | 40–60일 | −0.0221 | **A** |
| 11 | `fin_gross_profitability` | 매출총이익성 | DART | + | + | 0–40일 | +0.0275 | **B** |
| — | | | | | | | | |
| 12 | `flow_short_interest` | 공매도 잔고비율 | KRX flow | − | − | 0–60일 | −0.0874 | C(보류) |
| 13 | `flow_days_to_cover` | 상환소요일수 | KRX flow | − | − | 0–60일 | −0.0672 | C(보류) |
| 14 | `flow_short_turnover` | 공매도 회전율 | KRX flow | − | − | 0–60일 | −0.0668 | C(보류) |
| 15 | `flow_nat_proxy_20d` | NAT proxy | KRX flow | + | + | 0–60일 | +0.0644 | C(보류) |
| — | | | | | | | | |
| 16 | `fin_accruals_to_assets` | 발생액 비중 | DART | − | **+** | 0–120일 | +0.0180 | C |
| 17 | `fin_asset_growth_yoy` | 자산성장률 | DART | − | − | 0–120일 | −0.0044 | C |
| 18 | `fin_sue` | 실적 서프라이즈 | DART | + | (표본 없음) | — | — | C |
| — | | | | | | | | |
| 19 | `px_mom_12_1` | 12-1개월 모멘텀 | OHLCV | + | **−** | — | −0.0333 | D |
| 20 | `px_turnover_shock` | 회전율 충격 | OHLCV | + | **−** | — | −0.0342 | D |
| 21 | `flow_inst_netbuy_to_volume` | 기관 순매수 강도 | KRX flow | + | **−** | — | −0.0224 | D |
| 22 | `flow_foreign_netbuy_to_volume` | 외국인 순매수 강도 | KRX flow | + | **−** | — | −0.0131 | D |
| 23 | `px_resid_mom_12_1` | 잔차 모멘텀 | OHLCV | + | **−** | — | −0.0113 | D |
| 24 | `flow_foreign_holding_ratio_chg` | 외국인 지분율 변화 | KRX flow | + | **−** | — | −0.0080 | D |
| — | | | | | | | | |
| 25 | `px_zero_ret_ratio_20d` | 무변동일 비율 | OHLCV | (기준용) | — | — | — | R |

family 대표 등급으로 집계하면 **A 8 · B 3 · C 7 · D 6 · R 1**이다. 트랙별로는
T1(px·flow 17개)이 A 6 · C 4 · D 6 · R 1이고, T2(fin·ev 8개)가 A 2 · B 3 · C 3이다.

### 세 줄 결론

1. **살아남은 축은 세 개다** — 규모·유동성·변동성(1~4번), 단기 반전과 개인 수급(6·9번),
   주주환원·밸류·수익성(5·7·11번). 서로 다른 정보를 보고 있다(§9.3).
2. **모멘텀 계열은 한국에서 반대로 나온다.** 12-1개월 모멘텀, 잔차 모멘텀, 회전율 충격,
   기관·외국인 순매수가 전부 기대와 반대 부호다(19~24번). 우연이 아니라 일관된 패턴이다.
3. **공매도 4개는 탈락이 아니라 판정 보류다.** 2020-03 공매도 금지로 표본이 끊겨
   FDR 모집단에 넣지 않았다.

---

## 2. 등급이 무슨 뜻인가 — **T1과 T2에서 C의 의미가 다르다**

여기가 이 문서에서 가장 오해하기 쉬운 곳이다. 두 트랙이 같은 글자를 쓰지만 판정 규칙이 다르다.

| 등급 | T1 (px·flow) | T2 (fin·ev) |
|---|---|---|
| **A** | screen_pass + 비치명 경고 없음 + 모든 offset 평가 가능 | 좌동 + source 품질 ok + (SUE는) 독립 filing window 20개 이상 |
| **B** | screen_pass인데 비치명 경고가 붙음 | 좌동 (`revision_ratio` 초과가 A를 막음) |
| **C** | **애초에 후보가 아닌 역할**(탐색용·보조피쳐) 또는 available 표본 부호 뒤집힘 | **강건성 확인 실패** 또는 available 방향 실패 |
| **D** | 신호 없음 / 부호 반대 | 좌동 |
| **R** | 기준용(reference) | — |

즉 **T1의 C는 "판정 보류", T2의 C는 "판정했는데 강건성에서 떨어짐"**이다. 표 11~14번(공매도)과
15~18번(재무)을 같은 칸에 놓고 읽으면 안 된다.

읽을 때 쓰는 숫자는 세 개다.

- **IC** — 그날 피쳐 순위와 이후 수익률 순위의 상관. 시장 내 rank 기준이라 0.02만 돼도
  주식에서는 의미 있는 크기다. 부호가 가설과 맞는지가 크기보다 먼저다.
- **q** — 여러 가설을 한꺼번에 검정한 뒤의 오탐률(BH FDR). 임계는 0.10이고, 이번 결합
  모집단은 113개다.
- **게이트** — 유의해도 통과 못 하는 관문. tradable 유지, 소기간 부호 일관성, 지연 노출,
  그리고 **시계열 placebo**. 이번 검증에서 실제로 문턱 역할을 한 건 마지막 하나다(§9.2).

---

## 3. 원천 계보 — raw에서 피쳐까지

수집(Postgres raw) → 마트(DuckDB parquet) → 피쳐 순이다. 피쳐는 **Postgres에 저장되지 않는다.**

```
daily_ohlcv ─────────────────┬─► feat_price ──────────► px_* 9개
                             │
krx_security_flow_raw ───────┼─► feat_flow ───────────► flow_* 8개
dart_share_count_raw ────────┘   (float_shares_pit)

dart_xbrl_fact_raw ──┐
dart_financial_*_raw ─┼─► stock_metric_vintage_fact ─► feat_fin_scan_daily ─► fin_* 5개
dart_share_count_raw ─┤        (PIT vintage)
daily_ohlcv ─────────┘        (market_cap_pit)

dart_capital_change_raw ──┐
dart_shareholder_return_raw ┼─► feat_event_scan_daily ─► ev_* 2개
daily_ohlcv ──────────────┘

dart_filing_receipt_raw ──┬─► fin_sue_event ──────────► fin_sue
dart_xbrl_fact_raw ───────┘
```

**PIT 규칙이 두 트랙에서 다르다.** px/flow는 거래일 종가 시점이 곧 관측 시점이라 지연이 없다.
재무·이벤트는 **접수일(available_from) 이후에만** 노출한다. 그래서 T2 피쳐에는 유효 시작일이
따로 있고, 그게 아래 표본 시작일 차이를 만든다.

| family | 유효 시작 | coverage | 왜 늦나 |
|---|---|---|---|
| `fin_log_mcap` | 2015-03-17 | 0.7788 | 시총만 있으면 되므로 가장 빠르다 |
| `ev_payout_yield` | 2015-12-28 | 0.7090 | 배당 공시 연 1회 → 첫 TTM 확보 시점 |
| `ev_net_share_issuance_yoy` | 2016-04-15 | 0.4787 | 전기 대비이므로 vintage 2년치 필요 |
| `fin_value_z` | 2017-02-15 | 0.5641 | 4개 구성요소 중 2개 이상 유효해야 산출 |
| `fin_asset_growth_yoy` | 2016-06-27 | 0.6307 | 4분기 전 자산이 있어야 함 |
| `fin_accruals_to_assets` | 2017-02-27 | 0.5958 | 순이익·영업현금흐름 동시 필요 |
| `fin_gross_profitability` | 2017-02-27 | **0.5835** | I7 XBRL fallback으로 S/P 매핑이 복구됐다 |
| `fin_sue` | 2025-05-02 | **0.00** | 8분기 EPS 이력 + 접수일 기준 → 사실상 표본 없음 |

**표본 구간.** px/flow는 2014-06-02 ~ 2025-02-05(공통생존 기준)이고, 공매도 4개만
2016-07-01 ~ 2020-03-24로 짧다. 끝이 2025년 초인 것은 **holdout 경계 2025-08-01**을
넘지 않기 위해서다. 최근 데이터는 일부러 안 쓴다.

---

## 4. 가격 피쳐 9개 — 원천 `daily_ohlcv`

전부 조정주가·거래량만으로 만든다. 표본이 가장 길고 결측이 가장 적다.

### 통과 5개 — 전부 A등급

**`px_idio_vol_60d` — 60일 특이변동성 · A · IC −0.1524 (0–60일)**

252일 rolling market-model 잔차의 60일 표준편차다. 유효 잔차가 126일 이상일 때만 계산한다.

기대는 `−`였다. 변동성이 크면 나중에 수익률이 낮다는 저변동성 이례현상(Ang et al. 2006)이
근거다. 실제로 −0.1524, 전체 25개 중 |IC|가 가장 크다. 소기간 5구간 중 5구간에서 부호가 같다.

**`px_amihud_20d` — Amihud 비유동성 · A · IC +0.1343 (0–120일)**

`mean(|일간수익률| / 거래대금, 20일)`. 거래대금 미수집이라 `종가×거래량`으로 근사한다.

기대는 `+`. 유동성이 나쁘면 그 대가로 기대수익이 높다(Amihud 2002). **60~120일까지 효과가
이어지는 유일한 가격 피쳐**다. 다만 tradable 유지율이 0.85로 통과 피쳐 중 가장 낮다 —
비유동 종목에 신호가 몰려 있으니 당연한 결과이고, 실제 거래 가능성은 별도로 봐야 한다.

**`px_maxret_20d` — 20일 최대 일간수익률 · A · IC −0.1162 (0–60일)**

`max(일간수익률, 20일)`. 기대는 `−`. "복권 같은 주식"을 개인이 과대평가해 나중에 손해라는
가설(Bali et al. 2011)이고, 개인 비중이 높은 한국·대만에서 강하게 보고된다. 그대로 나왔다.

위 특이변동성과 경제적으로 같은 축이라 **둘 다 쓰면 중복**이다. 채택 단계에서 하나만 고르거나
증분성을 따로 확인해야 한다.

**`px_reversal_5d` — 5일 단기반전 · A · IC +0.0533 (0–3일)**

`−sum(log수익률, 5일)`. 즉 최근 5일 많이 떨어진 종목에 높은 점수를 준다.

기대는 `+`. 25개 중 **통계적으로 가장 뚜렷하다**(q ≈ 6.5e-98). 다만 효과가 3일에 정점을 찍고
5~10일 구간에서 반감된다. 회전이 빠른 만큼 거래비용에 취약하다.

**`px_near_52w_high` — 52주 신고가 근접도 · A · IC +0.0325 (40–60일)**

`종가 / max(종가, 252일) − 1`. 기대는 `+`(George & Hwang 2004). 크기는 작지만 40~60일
구간에서 꾸준하다. 모멘텀 계열이 전멸한 가운데 **유일하게 살아남은 추세성 피쳐**다.

### 탈락 3개 — 전부 부호가 반대다

| family | 산식 | 기대 | 실제 IC | 해석 |
|---|---|---|---|---|
| `px_mom_12_1` | `ln(close[t−21]/close[t−252])` | + | −0.0333 | 12개월 오른 종목이 오히려 덜 오른다 |
| `px_resid_mom_12_1` | 252일 market-model 잔차 누적(t−252~t−21) | + | −0.0113 | 시장 효과를 걷어내도 마찬가지 |
| `px_turnover_shock` | `ln(회전율 / median(회전율, t−60~t−1))` | + | −0.0342 | 거래가 갑자기 몰린 종목이 이후 부진하다 |

(가격 쪽 탈락은 이 셋이고, 수급 쪽 탈락 3개는 §5에 있다. 합쳐서 D등급 6개다.)

모멘텀 두 개가 같이 반대로 나온 건 우연으로 보기 어렵다. `02_feature_candidate.md` §3.1이
이미 "한국은 모멘텀 약화/부재 보고 다수"라고 적어뒀는데, 이번 측정이 그걸 재확인했다.
**부호가 반대라는 것 자체가 정보**이기는 하지만, 사전등록한 가설과 반대이므로 규율상
discovery로 세지 않는다. 반대 부호를 쓰고 싶으면 새 config로 다시 사전등록해야 한다.

### 기준용 1개

**`px_zero_ret_ratio_20d`** — `mean(수익률==0 또는 거래량==0, 20일)`. 소형주 유동성 필터
겸용이라 애초에 예측력을 묻지 않는 reference 역할이다. 등급 R.

---

## 5. 투자자 수급 피쳐 4개 — 원천 `krx_security_flow_raw`

`Σ순매수수량(h일) / Σ거래량(h일)`이 기본형이다. 금액 순매수는 수집하지 않아 수량 기준이고,
h ∈ {5, 20, 60}일 창을 전부 채운 날만 계산한다.

**`flow_individual_netbuy_to_volume` — 개인 순매수 강도 · A · IC +0.0241 (0–20일)**

**부호를 사전등록하지 않은 유일한 피쳐다.** 연구별로 price pressure·momentum chasing·반전
결과가 섞여 있어 방향을 고정하지 않고 열어둔 채 검증했다.

결과는 `+`, 즉 **개인이 순매수한 종목이 이후 더 오른다.** "개인은 틀린다"는 통념과 반대다.
다만 크기가 0.0241로 작고, 효과가 즉시 나타나지 않고 5~20일에 걸쳐 나온다(pattern: delayed).

부호를 고정하지 않은 덕에 살아남았다. `+`로 걸었어도 통과했겠지만, 나머지 셋을 보면
그건 결과를 보고 나서 하는 말이다.

**탈락 3개**

| family | 산식 | 기대 | 실제 IC | 해석 |
|---|---|---|---|---|
| `flow_inst_netbuy_to_volume` | 기관 순매수 / 거래량 | + | −0.0224 | 기관이 산 종목이 이후 부진 |
| `flow_foreign_netbuy_to_volume` | 외국인 순매수 / 거래량 | + | −0.0131 | 외국인도 마찬가지. 지연 노출 검정(delay_pass)도 실패 |
| `flow_foreign_holding_ratio_chg` | (외국인보유/유통주식) 의 h일 차분 | + | −0.0080 | 지분율 변화도 반대 |

**세 개가 모두 반대로 나왔고 개인만 정방향이다.** 개인 순매수와 기관·외국인 순매수는
거의 거울상이라(기타법인 제외로 완전한 항등식은 아니지만), 사실상 **같은 현상의 양면**을
보고 있을 가능성이 높다. 즉 독립된 네 개의 발견이 아니라 하나의 발견으로 읽어야 한다.
채택 단계에서 중복 축으로 따로 확인해야 할 지점이다.

---

## 6. 공매도 피쳐 4개 — **판정 보류(C)**

| family | 산식 | 기대 | 실제 IC |
|---|---|---|---|
| `flow_short_interest` | 공매도잔고 / 유통주식수 | − | −0.0874 |
| `flow_days_to_cover` | 공매도잔고 / mean(거래량, 20일) | − | −0.0672 |
| `flow_short_turnover` | Σ공매도량(20일) / Σ거래량(20일) | − | −0.0668 |
| `flow_nat_proxy_20d` | pctrank(외국인지분 20일 변화) − pctrank(공매도잔고비율) | + | +0.0644 |

**네 개 전부 기대 부호와 일치하고 크기도 작지 않다.** 그런데 등급은 C다. 탈락이 아니라
**애초에 후보 풀에 넣지 않았기 때문**이다(`exploratory_short_regime` 역할).

이유는 제도다. 표본이 2016-07-01 ~ 2020-03-24, 겨우 909 거래일이다. 2020년 3월 공매도 금지로
잘렸고, 그 뒤 재개·재금지가 반복돼 레짐이 균질하지 않다. 소기간 검정도 5구간이 아니라
2구간밖에 안 나온다. 이 상태로 FDR 모집단에 넣으면 **다른 21개 피쳐의 문턱까지 왜곡**한다.

`flow_short_turnover`의 tradable 유지율이 0.57로 특히 낮다. 공매도가 활발한 종목이
거래가능 universe와 어긋난다는 뜻이다.

레짐이 안정된 구간이 충분히 쌓이면 새 config로 사전등록해서 다시 본다. **지금 숫자가
좋아 보인다고 그대로 쓰면 안 된다.**

---

## 7. 재무 피쳐 5개 — 원천 DART canonical

전부 접수일 이후 as-of join이고 `fin_age_days`를 같이 들고 다닌다.

**`fin_log_mcap` — 규모 · A · IC −0.1149 (0–120일)**

`ln(market_cap_pit)`. 기대는 `−`, 즉 소형주가 더 오른다는 고전적 size 효과(Fama & French 1992).

**이번 검증에서 가장 단단한 결과다.** 25개 중 유일하게 요구 게이트 3개(강건성, tradable,
소기간 부호)를 전부 통과했다. 4개 셀이 모두 screen_pass이고 등급 A다. 0–120일까지
단조롭게 강해지는 것도 size 효과의 알려진 모습과 맞는다.

tradable 유지율이 0.78~0.85로 낮은데, 소형주에 신호가 몰려 있으니 구조적으로 그렇다.

**`fin_value_z` — 밸류 종합 · B · IC +0.0599 (40–60일)**

B/M, E/P, CFO/P, S/P 네 개를 각각 (거래일, 시장)별 1/99% 윈저라이즈 → z-score → **유효한
것만 평균**한다. 2개 이상 유효할 때만 값을 낸다. 적자기업을 버리지 않으려고 단일 지표 대신
종합 z를 대표로 세웠다.

기대 `+` 그대로 나왔고 4개 셀 전부 통계적으로 유의하다. 누적 0–120일에서는 IC가
**0.1220**까지 올라가 T2에서 `fin_log_mcap` 다음으로 크다. **A로 못 간 이유는 통계가 아니라
원천 품질이다** — 정정 비율
`revision_ratio`가 0.1014로 임계 0.10을 근소하게 넘어 등급 상한이 B로 묶였다.

**`fin_accruals_to_assets` — 발생액 · C · IC +0.0180 — 부호가 반대다**

`(순이익_ttm − 영업현금흐름_ttm) / 평균자산`. 기대는 `−`(Sloan 1996: 발생액이 크면 이익의
질이 낮아 이후 수익률이 낮다).

4개 셀은 Phase B 단독 BH를 통과했지만 기대 부호와 기간 부호를 통과하지 못해 discovery는
0개다. 한국에서 발생액 이례현상이 반대로 나타나는 것인지, canonical 매핑 문제인지는 이
실험만으로 못 가른다. 사전등록 규율상 discovery로 세지 않는다.

**`fin_gross_profitability` — 매출총이익성 · B · IC +0.0275 (0–40일)**

`매출총이익_ttm / 평균자산`(Novy-Marx 2013). I7이 XBRL S/P fallback을 고친 뒤 coverage가
0.0315에서 **0.5835**로 늘었다. 8셀 모두 Phase B discovery이고, 짧은 horizon 5셀
(10–20·20–40·40–60 bucket, 0–20·0–40 cumulative)이 `screen_pass`를 통과했다. 긴 3셀은
temporal placebo에서 떨어졌다. 통과 5셀이 A가 아닌 이유는 `revision_ratio=0.1014`와 높은
mapping fallback에 따른 source 품질 경고다.

**`fin_asset_growth_yoy` — 자산성장률 · C · IC −0.0044**

`총자산 / 총자산(t−4분기) − 1`. 기대 `−`(Cooper et al. 2008). 대표 부호는 맞지만
q가 0.92~1.00이라 **신호가 없다.** 25개 중 가장 결과가 없는 축이다.

**`fin_sue` — 실적 서프라이즈 · C · 측정 불가**

`(분기EPS − 4분기전 EPS) / rolling_std(8분기)`. PEAD를 노린 유일한 이벤트 피쳐다.

**coverage 0.0000, 관측 0건.** 유효 시작일이 2025-05-02인데 holdout 경계가 2025-08-01이라
쓸 수 있는 구간이 사실상 없다. 8분기 이력이 필요한 데다 접수일 기준 노출이라 표본이 가장
늦게 열린다. 정기보고서 정정분을 receipt 단위로 다시 받아오는 백필(B-1 6항)이 선행 조건이다.

---

## 8. 이벤트 피쳐 2개 — 원천 DART 지분·주주환원

**`ev_payout_yield` — 주주환원 수익률 · B · IC +0.0550 (40–60일)**

`(현금배당총액 + 자사주매입현금)_ttm / 시가총액`. 배당만 보는 것보다 자사주매입을 합친
쪽이 낫다는 근거(Boudoukh et al. 2007)를 따랐다.

기대 `+` 그대로다. 4개 셀 전부 유의하고 누적 0–120일 IC가 0.1021이다. 40–60일 셀만
`screen_pass`를 통과했고, 긴 3셀은 temporal placebo에서 떨어졌다. `revision_ratio=0.1116`이라
통과 셀의 등급은 B다.

**`ev_net_share_issuance_yoy` — 순주식발행 · A · IC −0.0221 (40–60일)**

`(경제적 증가 − 경제적 감소) / 전기 발행주식수`.

**이 피쳐가 이번 트랙에서 손이 가장 많이 갔다.** 액면분할·무상증자 같은 기계적 주식수 변경을
유상증자 같은 경제적 발행과 갈라내야 하기 때문이다. `isu_dcrs_stle` 카탈로그를 v2로 고쳐
미분류를 22.4% → 4.3%로 줄였고, 증가·감소 항등식이 맞는 행만 값을 낸다.

여기에 **PIT 정책 판정**이 하나 더 붙었다. 발행 이력은 보고서 vintage마다 상장 이후 전체를
다시 준다. 최신본을 쓰면(latest) 미래 정보가 새고, 당시 본을 쓰면(strict PIT) 데이터가
적어진다. 사전등록한 probe로 재봤더니 9년 거리에서 피쳐가 바뀌는 비율이 **0.1824**로
임계 5%를 크게 넘었다. 그래서 **strict PIT을 채택**하고 부족한 8개 연도 vintage를 추가
수집했다(`dart_capital_change_raw` 71,535 → 245,120행).

기대 `-` 그대로 나왔고 등급 A다. 다만 **IC가 −0.0221로 T2 4개 중 가장 작다.** 방향은
맞지만 크기는 약하다.

2026-08-15 재실행에서 IC가 2.7배가 됐다. 감소 사유 카탈로그에 `유상감자`가 빠져 있어
경제적 감소가 한 번도 매칭되지 않았고, 그 탓에 미분류로 버려지던 창 31만 개가 함께
돌아왔기 때문이다(`06_grade_a_deep_dive/09_ev_net_share_issuance_yoy.md` §4).

---

## 9. 전체를 관통하는 것

### 9.1 A등급 8개가 다 같은 무게는 아니다

**요구 게이트 3개를 전부 통과한 건 `fin_log_mcap` 하나뿐이다.**

T2에서 `screen_pass`가 붙은 12개 셀 중 시계열 placebo를 실제로 거친 건 3개(전부
`fin_log_mcap`)다. 나머지 9개는 horizon 폭이 짧아 `nw_lag < 59`가 되고
**강건성 요구 대상이 아니었다**(`placebo.temporal_min_nw_lag`, rule 7). 사전등록된 설계지만,
등급표만 보고 모든 통과 셀을 같은 무게로 읽으면 안 된다.

`ev_net_share_issuance_yoy`의 A가 여기 해당한다. 40–60일 셀은 A인데, 같은 family의 다른
세 셀(60–120, 0–60, 0–120)은 강건성 요구를 받고 **전부 실패해 C**다.
`fin_gross_profitability`의 B 5셀도 같은 이유로 placebo 미요구 구간이다.

### 9.2 유의한 신호를 떨어뜨리는 관문은 사실상 하나다

T2 등급 규칙상 C는 "강건성 실패 또는 available 방향 실패"로 라우팅된다. 최신 실행의 C
25셀 중 **24셀은 강건성 실패**, 나머지 1셀은 `fin_asset_growth_yoy`의 available 방향 실패다.
그중 12셀은 통계적 유의성과 부호를 통과하고 **오직 시계열 placebo에서만** 떨어졌다.

family로는 `ev_payout_yield` 3 · `fin_value_z` 3 · `fin_gross_profitability` 3 ·
`ev_net_share_issuance_yoy` 3이다.
IC가 작아서가 아니다. 강건성 요구를 받은 셀들의 placebo p값(임계 0.10)을 나란히 놓으면
차이가 분명하다.

| family | 셀 | IC | ICIR | NW t | placebo p | 통과 |
|---|---|---|---|---|---|---|
| `fin_log_mcap` | cum 0–120 | −0.1149 | −1.11 | −5.84 | **0.0099** | ○ |
| `fin_log_mcap` | cum 0–60 | −0.0868 | −0.95 | −7.02 | **0.0099** | ○ |
| `fin_log_mcap` | bucket 60–120 | −0.0679 | −0.74 | −5.63 | **0.0099** | ○ |
| `ev_payout_yield` | cum 0–120 | +0.1021 | +1.21 | +6.36 | 0.2673 | ✗ |
| `ev_payout_yield` | cum 0–60 | +0.0798 | +1.02 | +7.58 | 0.1287 | ✗ |
| `fin_value_z` | cum 0–120 | +0.1220 | +1.15 | +5.41 | 0.4851 | ✗ |
| `fin_gross_profitability` | cum 0–120 | +0.0360 | +0.45 | +2.29 | 0.2673 | ✗ |

`ev_payout_yield`도 `fin_value_z`도 IC·ICIR이 `fin_log_mcap`에 밀리지 않는다. NW t는
오히려 더 크다.
그런데 긴 horizon에서는 **시계열을 통째로 밀어놓고 만든 가짜 신호와 구분되지 않는다**.
반면 `fin_log_mcap`은 0.0099로 100회 복제에서 나올 수 있는 최솟값이다. 이 게이트는 신호의
크기가 아니라 **시점 정렬이 진짜인지**를 묻는다.

### 9.3 두 트랙은 서로 다른 정보를 본다

Phase A와 Phase B의 대표 피쳐 84쌍의 일별 rank correlation을 재봤다. 대부분 절댓값 0.15
이하다.

| 쌍 | 상관 |
|---|---|
| `fin_log_mcap` ↔ `px_amihud_20d` | **−0.7213** |
| `ev_payout_yield` ↔ `px_idio_vol_60d` | −0.3468 |
| `fin_value_z` ↔ `px_idio_vol_60d` | −0.3506 |
| `px_mom_12_1` ↔ `fin_asset_growth_yoy` | +0.1568 |
| `px_reversal_5d` ↔ `fin_log_mcap` | −0.0186 |

**예외가 하나 뚜렷하다** — 규모와 Amihud 비유동성이 −0.72다. 큰 회사일수록 유동성이 좋으니
당연하지만, 그만큼 **1번과 3번은 사실상 같은 축**이라는 뜻이다. 둘 다 채택하면 중복이다.

BH 모집단을 75개(Phase A만)에서 113개(A+B)로 넓혔는데 강등된 가설이 하나도 없는 것도
이 낮은 상관과 일관된다. 결합 단면 permutation은 100회 복제에서
`p_empirical_count = 0.0099`로, 발견 56개가 우연히 나올 수준이 아니다.

### 9.4 아직 아무것도 채택하지 않았다

이 문서의 등급은 **선별 결과이지 채택 결론이 아니다.** 채택은 `02_feature_candidate.md`
§6.1의 acceptance gate에서 증분성·purged walk-forward OOS·turnover·거래비용을 따로 보고
정한다.

T1은 8월 24일 새 A0를 재사용해 walk-forward를 다시 돌렸다. A등급 6개 중 baseline에 이미
있던 둘을 뺀 4개를 20일 모델에 넣었을 때 평균 IC는 0.1436→0.1521로 늘었지만,
비용 반영 spread는 **−0.0025** 나빠졌다. 그래서 채택하지 않았다. holdout은 열지 않았다.

**등급 A가 곧 쓸 수 있는 피쳐라는 뜻이 아니다.** T2도 같은 관문을 아직 안 거쳤다.

---

## 10. 원본을 보고 싶으면

경로는 전부
`research/output/horizon_scan/phase=<X>/snapshot_date=2026-08-23/source=sj2_remote/config_hash=ab0de634…/run_id=<id>/`.

| 보고 싶은 것 | 파일 |
|---|---|
| Phase A 전체 보고서(17 family card 포함) | `phase=A/…/03a_horizon_scan_results.md` |
| Phase A family별 전체 숫자 | `phase=A/…/cards/family_cards.json` |
| Phase A 그래프 119장 | `phase=A/…/plots/*.png` |
| Phase B 전체 보고서 | `phase=B/…/03b_horizon_scan_results.md` |
| Phase B family card 8개 | `phase=B/…/core/family_cards.md` |
| 셀 단위 최하위 통계 (A) | `phase=A/…/core/horizon_ic.parquet` |
| **셀 단위 최종 판정 (등급·게이트)** | `phase=AB/…/combined_ab_primary_hypotheses.parquet` |
| A/B 상관 매트릭스 84쌍 | `phase=AB/…/primary_feature_rank_correlation.parquet` |

`combined_ab_primary_hypotheses.parquet`를 읽을 때는 `evidence_grade`와 함께
**`robustness_required`를 반드시 같이 본다**(§9.1). 안 보면 A등급을 과대평가한다.

관련 문서: 후보 정의는 `02`, 검증 설계는 `03`, 사전등록 계약은 `04_*`, T1 해설은 `05`와
`06_grade_a_deep_dive/`, T1 채택 판정은 `07`, T2 구현·실행 기록은 `08`, 현재 진행 상태는
`00_status.md`.

A/B 등급 10개는 `06_grade_a_deep_dive/`에 family당 파일 하나로 상세 조사가 있다 —
종목 단위 decile 표와 실제 편입 종목까지 본다. T1 6개는 `01`~`06`(Phase A run 기준),
이 문서의 T2 4개는 `08`~`11`(이 문서와 같은 Phase B + AB run 기준)이다. 각 파일 맨 앞
`§0 쉬운 설명`이 통계 배경 없이 읽는 요약이고, 용어는 `07_glossary.md`에 있다.

T2 상세 조사에서 찾은 과거 결함은 `10_known_issues.md`에 있다. `fin_value_z` 결측 처리(I1),
`ev_net_share_issuance_yoy` 감소 사유 카탈로그와 I7 XBRL fallback은 최신 run 전에 고쳤다.
이번 결과는 그 수정 뒤 `fin_v4` 기준이다. 반면 `mcap_krx_log`, 공시 활동, N8 후보와
N6에서 고른 `hc_employee_growth_yoy`·`hc_revenue_per_employee`·`own_major_stake`
level/change는 아직 이 config에 들어 있지 않다. N2 업종 중립 variant는 변동성 진단을
통과했지만 현재 업종 소급이라는 look-ahead 때문에 diagnostic-only로 남겼다.
  정의는 순발행인데 실제로는 총발행을 재고 있다.

§1 표 머리말의 "IC는 |IC|가 가장 큰 primary 셀 기준"도 정확하지 않다. 실제로는
등급이 확정된 셀을 실었고, T2 3개 행에서 두 기준이 갈린다(`10_known_issues.md` I8).
