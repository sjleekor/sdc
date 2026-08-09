# Feature Candidates — 주가 예측용 피쳐 후보 정리

- 작성일: 2026-07-31
- 선행 문서: `00_raw_feature_inventory.md` (raw 레이어 전수 조사)
- 목적: 현재 raw 레이어에서 **즉시 계산 가능**하고, 학계(asset pricing anomaly 문헌)·현업(퀀트 팩터)에서 예측력이 검증된 피쳐 후보를 카테고리별로 정리. 각 피쳐에 계산식·원천 테이블·근거·한국시장 주의사항을 명시.

## 0. 선정 원칙

1. **계산 가능성**: 인벤토리에 있는 raw 테이블만으로 유도 가능해야 함 (외부 데이터 추가 수집 불요).
2. **PIT(point-in-time) 정합성**: 재무 피쳐는 공시일 기준 lag, 매크로는 `available_from_date` 사용. 룩어헤드가 원천적으로 불가능한 정의만 채택.
3. **근거**: 대표 문헌 또는 현업 표준(팩터 라이브러리, 증권사 퀀트 리포트에서 통용)을 병기. 한국시장에서 효과가 다르게 나타나는 경우(모멘텀 약화, 외국인 수급 유효 등) 별도 표기.
4. **횡단면(cross-sectional) 우선**: 종목 간 상대 순위 예측 피쳐를 우선하고, 시계열(마켓 타이밍) 피쳐는 레짐/조건 변수로 분류.

시가총액 근사: `mktcap ≈ close × issued_shares` (share_count는 연/분기 단위라 계단형 근사임 — 정규화 분모로는 충분, 정밀 시총 필요 피쳐는 주의). 유통시총은 `close × (issued_shares − treasury_shares)`.

---

## 1. 가격/거래량 기반 (원천: `daily_ohlcv`)

가장 표본이 길고(KOSDAQ 2007~, KOSPI 2014~) 결측이 적은 도메인. 일봉만으로 계산되는 피쳐들.

| # | 피쳐 | 정의(계산식) | 근거 | 비고 |
|---|---|---|---|---|
| P1 | 중기 모멘텀 (12-1M) | `close[t-21] / close[t-252] − 1` (최근 1개월 제외 누적수익률) | Jegadeesh & Titman (1993), Carhart UMD | **한국시장에서는 효과가 약하거나 부재**하다는 연구 다수 (일본과 함께 대표적 모멘텀 실패 시장). 단독보다 잔차 모멘텀·수급 결합으로 사용 권장 |
| P2 | 단기 반전 (1M reversal) | `−(close[t] / close[t-21] − 1)` | Jegadeesh (1990), Lehmann (1990) | 한국은 반전이 모멘텀보다 강한 편 — 개인 비중 높은 시장 특성. 소형주에서 특히 강하나 거래비용 주의 |
| P3 | 52주 신고가 근접도 | `close / max(high, 252d)` | George & Hwang (2004) | 모멘텀 대비 한국 포함 아시아에서 상대적으로 견조하다는 보고. 계산 단순 |
| P4 | 저변동성 | `−std(daily return, 60d 또는 252d)` | Ang et al. (2006), Baker et al. (2011) low-vol anomaly | 한국에서도 저변동성 효과 보고 다수. 시장수익률 회귀 잔차 기반 idio-vol 버전은 지수 시리즈(`market_kospi_krx` 등)와 결합해 계산 |
| P5 | MAX (복권 수요) | `max(daily return, 21d)` — 높을수록 향후 저성과 | Bali, Cakici & Whitelaw (2011) | 개인 투자자 비중 높은 한국·대만에서 강하게 나타남. 상하한가 제도(±30%)로 극단값이 잘리는 점 참고 |
| P6 | Amihud 비유동성 | `mean( \|ret\| / (close×volume), 21d )` | Amihud (2002) | 유동성 프리미엄. 거래대금 컬럼이 없어 `close×volume` 근사 사용. volume=0(거래정지) 행 제외 필수 |
| P7 | 회전율 (turnover) | `volume / issued_shares` (21d 평균) 및 그 변화율 | Datar et al. (1998), Lee & Swaminathan (2000) | 고회전율 → 저성과(과열), 회전율 급증은 정보 이벤트 신호. issued_shares는 DART 결합 |
| P8 | 거래량 충격 (abnormal volume) | `volume / mean(volume, 60d)` | Gervais, Kaniel & Mingelgrin (2001) high-volume premium | 단기(1~4주) 예측용 |
| P9 | 변동성 구조 (HL range) | Parkinson/Garman-Klass 변동성: `ln(high/low)` 기반 | Parkinson (1980) | close-to-close std보다 효율적 추정. P4의 대체 입력 |
| P10 | zero-return days 비율 | `count(ret==0 or volume==0) / 63d` | Lesmond et al. (1999) 유동성 프록시 | 소형주 유동성 필터 겸용. 인벤토리 §4-5의 거래정지 처리와 일관되게 |

**구현 노트**: 수익률 계산 전 volume=0/open=0 행 처리 정책(제외 vs forward-fill)을 마트 공통 유틸로 고정할 것. KOSPI 2014-01-20 이전 구간은 KOSDAQ 편향이므로 학습 구간 시작을 2014-06(공통 피쳐 시작과 정렬) 이후로 두는 것이 안전.

---

## 2. 수급/공매도 (원천: `krx_security_flow_raw`) — **최우선 후보군**

인벤토리 §5에서 "완전 미사용 77M행"으로 지목된 도메인. 한국시장 특화 연구에서 예측력이 가장 일관되게 보고되는 영역이며, 글로벌 팩터로 복제 불가능한 로컬 엣지.

| # | 피쳐 | 정의(계산식) | 근거 | 비고 |
|---|---|---|---|---|
| F1 | 외국인 순매수 강도 | `Σ(foreign_net_buy_volume × close, 5/20/60d) / mktcap` | Froot et al. (2001), Richards (2005), 국내 다수 연구 — 외국인 순매수는 한국에서 양(+)의 단기 예측력 | **금액 기준 순매수 미수집** → `수량 × 당일 close` 근사 (인벤토리 §2.2). 시총 정규화 필수 |
| F2 | 기관 순매수 강도 | F1과 동일 계산, `institution_net_buy_volume` | 국내 실증: 기관(특히 연기금)도 양의 예측력, 외국인보다 약함 | 투신/연기금 세분류는 미수집 — 합산 기관만 가능 |
| F3 | 개인 순매수 강도 (역지표) | F1과 동일 계산, `individual_net_buy_volume`, 부호 반전 | Barber & Odean 계열 + 국내 실증: 개인 순매수는 음(−)의 예측력 | 세 주체 합≈0이므로 F1·F2와 공선성 — 모델에는 2개만 넣거나 residualize |
| F4 | 외국인 보유비율 | `foreign_holding_shares / issued_shares` (level) | Ferreira & Matos (2008), 국내 연구: 외국인 지분율 높은 기업의 초과성과·저변동성 | level은 quality proxy 성격 |
| F5 | 외국인 보유비율 변화 | F4의 20/60d 변화분 | 국내 실증에서 level보다 **변화분(Δ)의 예측력이 강함** | F1의 저빈도·저노이즈 버전 |
| F6 | 공매도 비중 (short ratio) | `short_selling_volume / volume` (5/20d 평균) | Diether, Lee & Werner (2009): 공매도 급증 → 음의 예측력 | 한국 공매도 규제 이력 주의 (아래 노트) |
| F7 | 공매도 잔고 비율 | `short_selling_balance_quantity / issued_shares` | Asquith et al. (2005), Rapach et al. (2016) — short interest는 가장 강한 음(−) 예측 변수 중 하나 | 2016-06-30 이후만 존재 |
| F8 | 잔고 회전일수 (days-to-cover) | `short_balance_quantity / mean(volume, 21d)` | Hong et al. (2016) DTC — 숏스퀴즈 위험 프록시 | F7과 상호보완 |
| F9 | 잔고 변화 | F7의 20d 변화분 | 잔고 증가 → 음의 신호, 급감 → 스퀴즈성 반등 | |
| F10 | 수급-가격 발산 | 예: 주가 하락 & 외국인 순매수 지속 (F1>0, P2 구간) 교차항 | 현업 스타일 (스마트머니 divergence) | 단독 피쳐보다 교차항으로 |

**한국 공매도 제도 주의 (F6~F9 공통)**: 전면 금지 구간(2020-03~2021-05 부분 재개, 2023-11~2025-03 전면 금지 후 2025-03-31 전면 재개)이 표본에 포함됨. 금지 구간은 값이 0/결측이 되어 **레짐 더미 없이 학습하면 왜곡** — 금지기간 플래그 피쳐 또는 구간 제외 정책 필요.

**구현 노트**: metric_rules에 flow 매핑을 추가하기보다, 인벤토리 §5 방향대로 별도 flow feature 마트(`research/etl/marts/`)로 두는 것이 raw/derived 분리 원칙에 부합. 최신일 2026-07-24 정지(KRX 비밀번호 만료)는 수동 조치 후 백필 확인 필요.

---

## 3. 밸류에이션 (원천: DART 재무 + `daily_ohlcv` + `share_count`)

canonical 29개 metric(인벤토리 §3, `metric_rules.py`) 위에서 즉시 조합 가능. 모든 피쳐는 **공시일 이후 사용** (보고서 접수일 기준 lag — 보수적으로 사업보고서 +90일, 분기 +45일, 또는 raw의 접수일 컬럼 활용).

| # | 피쳐 | 정의(계산식) | 근거 | 비고 |
|---|---|---|---|---|
| V1 | Book-to-Market (B/M) | `total_equity / mktcap` | Fama & French (1992, 1993) HML | 한국에서 밸류는 장기간 유효하다는 실증이 두터움. 지배주주지분 기준 사용 검토 |
| V2 | Earnings Yield (E/P) | `net_income(TTM) / mktcap` | Basu (1977) | 2016년부터 분기 수집이므로 TTM은 2017~ 계산 가능. 적자 기업 처리(음수 E/P 유지 vs 더미) 정책 필요 |
| V3 | Cash-Flow Yield (CFO/P) | `cash_flow_operating(TTM) / mktcap` | Lakonishok, Shleifer & Vishny (1994) | 한국은 이익조정 여지가 커서 E/P보다 CFO/P 선호하는 실무 관행 |
| V4 | Sales-to-Price (S/P) | `revenue(TTM) / mktcap` | Barbee et al. (1996) | 금융업 제외 필요 |
| V5 | FCF Yield | `(cash_flow_operating − capex_ppe − capex_intangible)(TTM) / mktcap` | 현업 표준 (quality-value 결합) | canonical에 capex 2종 모두 존재 |
| V6 | EV/EBITDA 근사 | `(mktcap + total_liabilities − cash_and_cash_equivalents) / (operating_income + depreciation + amortization)` | Loughran & Wellman (2011) EV multiple | 순차입금 정밀 계산(이자부부채)은 미매핑 → 총부채 근사임을 명시. depreciation/amortization은 XBRL rule 기존 매핑 활용 |

---

## 4. 수익성/퀄리티 (원천: DART 재무)

| # | 피쳐 | 정의(계산식) | 근거 | 비고 |
|---|---|---|---|---|
| Q1 | Gross Profitability | `gross_profit(TTM) / total_assets` | Novy-Marx (2013) — "the other side of value" | 밸류와 결합 시 상호보완. gross_profit canonical 존재 |
| Q2 | 영업이익률/ROA/ROE | `operating_income/revenue`, `net_income/total_assets`, `net_income/total_equity` | Fama & French (2015) RMW, Hou-Xue-Zhang q-factor | level + YoY 변화 둘 다 |
| Q3 | Accruals (역지표) | `(net_income − cash_flow_operating)(TTM) / total_assets`, 부호 반전 | Sloan (1996) — 가장 재현성 높은 anomaly 중 하나 | canonical만으로 계산 가능한 balance-sheet-free 정의 |
| Q4 | 자산성장 (역지표) | `total_assets YoY 증가율`, 부호 반전 | Cooper, Gulen & Schill (2008), FF(2015) CMA | 한국 포함 국제 표본에서 유효 (Titman et al. 2013) |
| Q5 | 순주식발행 (역지표) | `issued_shares YoY 변화` (증가=희석 → 음의 신호) | Pontiff & Woodgate (2008) | share_count raw에서 직접. 무상증자/액면분할 조정 필요 — XBRL 주식수 rule과 교차검증 |
| Q6 | 이익 모멘텀 (SUE) | `(분기 net_income − 4분기 전 net_income) / std(동차, 8Q)` | Ball & Brown (1968) PEAD, Bernard & Thomas (1989) | 분기 재무 2016~ 필요 → 2018년경부터 계산 가능. 공시일 lag가 핵심 (PEAD는 공시 직후 드리프트) |
| Q7 | 운전자본 변화 | `Δ(재고자산 + 매출채권) / total_assets` (역지표) | Thomas & Zhang (2002), accruals 세분화 | 재고는 canonical 매핑 있음, 매출채권은 **미매핑 계정 → metric_rules 확장 필요** (인벤토리 §5-3) |
| Q8 | F-Score (단순화) | ROA>0, CFO>0, ΔROA>0, accruals<0, Δleverage<0, Δ유동성>0, 무증자, Δmargin>0, Δturnover>0 중 충족 개수 | Piotroski (2000) | 구성요소 대부분 canonical로 계산 가능 (유동자산/유동부채는 미매핑 → 7~8개 항목 축소판으로 시작) |

**공통 주의**: 2025년부터 OFS(별도) 없음 → **CFS(연결) 기준으로 통일**. 2015년은 연간만 존재. 지주회사·금융업은 계정 구조가 달라 섹터 더미 또는 제외 처리.

---

## 5. 주주환원 (원천: `dart_shareholder_return_raw`, `share_count`)

인벤토리 §5-2 지목 영역. 현재 canonical에는 `dps` 하나만 매핑 — raw에 이미 값이 있어 확장 비용이 낮다.

| # | 피쳐 | 정의(계산식) | 근거 | 비고 |
|---|---|---|---|---|
| D1 | 배당수익률 | raw의 `현금배당수익률` 직접 사용 또는 `dps / close` | Fama & French (1988) 등 | raw에 이미 존재 (dividend statement). 연 단위 → 일 단위 브로드캐스트 |
| D2 | 배당성향 | raw의 `현금배당성향(연결)` 직접 사용 | Skinner (2008) payout 문헌 | 극단값(적자 기업) 윈저라이즈 |
| D3 | 자사주 매입 강도 | `treasury_stock 취득 변동수량 / issued_shares` (연간) 또는 canonical `treasury_share_acquisition_amount / mktcap` | Ikenberry et al. (1995) buyback anomaly | 한국은 2025~ 밸류업 정책으로 자사주 소각 증가 — 레짐 변화 유의 |
| D4 | 소각 강도 | treasury_stock의 `소각 변동수량 / issued_shares` | 소각은 매입보다 강한 신호 (한국: 매입 후 재매각 관행 때문) | dim(취득방법/목적) 파싱 필요 |
| D5 | 총주주환원율 (net payout yield) | `(배당총액 + 자사주순취득 − 신주발행) / mktcap` | Boudoukh et al. (2007) — dividend yield 단독보다 예측력 우수 | D1~D4 + Q5 조합 |

---

## 6. 시장/매크로 조건 변수 (원천: `common_feature_observation_raw`)

횡단면 알파보다는 **레짐 분류·조건부 게이팅**(시장 익스포저 조절, 팩터 스위칭)용. active 37개 카탈로그 피쳐 위에 파생 (인벤토리 §5-5).

| # | 피쳐 | 정의(계산식) | 근거 | 비고 |
|---|---|---|---|---|
| M1 | 기간 스프레드 | `rate_kr_gov10y − rate_kr_gov3y`, `rate_us10y − rate_us2y` (카탈로그 기존재) + 부호/변화 | Estrella & Hardouvelis (1991) 경기 선행 | level은 이미 카탈로그에 있음 — 부호 더미·Δ 추가 |
| M2 | VIX 레짐 | `global_vix` level + 20d 변화, 임계(예: >25) 더미 | Whaley (2000), risk-on/off 표준 | 한국 시장 수익률과 외국인 수급의 조건 변수로 특히 유효 |
| M3 | USD/KRW 모멘텀 | `fx_usdkrw` 20/60d 수익률 | 원화 약세 → 외국인 순매도 → 약세 전이 (국내 실증) | F1(외국인 수급)과 교차항 후보 |
| M4 | Advance-Decline 비율 | `advancers / (advancers + decliners)` (kospi/kosdaq 각각, 10/20d 평균) | Brown & Cliff (2004) breadth-sentiment | breadth raw 6종 기존재 — 비율화만 필요 |
| M5 | 시장 회전율 레짐 | `turnover_value / mean(turnover_value, 252d)` | 과열/침체 프록시 | |
| M6 | 미-한 금리차 | `rate_us10y − rate_kr_gov10y` | 자본유출입 압력 프록시 — M3·F1과 연동 | |
| M7 | 매크로 서프라이즈 방향 | CPI/PPI/M2 YoY (기존재) + 전월 대비 가속/감속 더미 | 인플레 레짐별 팩터 성과 차이 (Neville et al. 2021) | 반드시 `available_from_date` 기준 사용 |

---

## 7. 우선순위 및 다음 단계 제안

인벤토리 §5의 미개발 순위와 각 피쳐군의 (a) 구현 비용, (b) 한국시장 근거 강도, (c) 다른 데이터로 복제 불가능한 정도를 종합:

| 순위 | 피쳐군 | 이유 |
|---|---|---|
| 1 | **F1~F9 수급/공매도** | 77M행 완전 미사용, 한국 특화 엣지, 계산 단순 (조인 + 롤링합). 공매도 금지 레짐 더미 동반 필수 |
| 2 | **P1~P10 가격/거래량** | 원천 하나로 완결, 표본 최장, 모든 모델의 베이스라인. 저비용 |
| 3 | **V1~V6 + Q1~Q6 밸류/퀄리티** | canonical 29 metric 위에서 조합만으로 가능. 공시일 lag 정책 확정이 선결 과제 |
| 4 | **D1~D5 주주환원** | raw에 값이 이미 있고 밸류업 레짐과 시의성 부합. dps 외 매핑 추가 필요 |
| 5 | **M1~M7 매크로 레짐** | 알파원이 아닌 조건 변수 — 위 피쳐들이 갖춰진 뒤 게이팅용 |
| 6 | Q7~Q8 미매핑 계정 확장 | metric_rules 확장(매출채권, 유동자산/부채 등) 필요 — 3번 검증 후 |

**공통 인프라 선결 과제** (피쳐 정의보다 먼저 확정할 것):

1. **수익률/거래정지 처리 유틸**: volume=0 (2.2%) 행 정책을 마트 공통 함수로 고정.
2. **시총 근사 테이블**: `close × issued_shares` 일별 브로드캐스트 (share_count 공시일 기준 PIT 결합) — F, V, D군의 공통 분모.
3. **재무 공시일 lag 정책**: raw 접수일 기반 available_from 규칙을 재무 피쳐 전체에 일괄 적용 (매크로의 `available_from_date` 패턴과 동일 철학).
4. **레이블 정의**: 예측 대상(예: 5/20d forward return, 시장 초과수익률) 및 상하한가·거래정지 종목의 진입 불가 처리 — 백테스트 전 확정.
5. **생존 편향 완화**: DELISTED 28개뿐인 유니버스(인벤토리 §4-7) — `stock_master_snapshot` 기반 PIT 유니버스를 스냅샷 존재 구간에서라도 사용.

## 부록: 대표 참고문헌

- Jegadeesh & Titman (1993) "Returns to Buying Winners and Selling Losers" — 모멘텀
- George & Hwang (2004) "The 52-Week High and Momentum Investing"
- Amihud (2002) "Illiquidity and Stock Returns"
- Bali, Cakici & Whitelaw (2011) "Maxing Out: Stocks as Lotteries"
- Ang, Hodrick, Xing & Zhang (2006) "The Cross-Section of Volatility and Expected Returns"
- Sloan (1996) "Do Stock Prices Fully Reflect Information in Accruals and Cash Flows?"
- Novy-Marx (2013) "The Other Side of Value: The Gross Profitability Premium"
- Cooper, Gulen & Schill (2008) "Asset Growth and the Cross-Section of Stock Returns"
- Piotroski (2000) "Value Investing: The Use of Historical Financial Statement Information"
- Pontiff & Woodgate (2008) "Share Issuance and Cross-Sectional Returns"
- Bernard & Thomas (1989) "Post-Earnings-Announcement Drift"
- Fama & French (1993, 2015) 3-factor / 5-factor
- Diether, Lee & Werner (2009) "Short-Sale Strategies and Return Predictability"
- Asquith, Pathak & Ritter (2005); Rapach, Ringgenberg & Zhou (2016) — short interest
- Boudoukh, Michaely, Richardson & Roberts (2007) "On the Importance of Measuring Payout Yield"
- Ikenberry, Lakonishok & Vermaelen (1995) — buyback
- Froot, O'Connell & Seasholes (2001); Richards (2005) — 신흥시장 외국인 플로우
- Estrella & Hardouvelis (1991) — term spread; Whaley (2000) — VIX
