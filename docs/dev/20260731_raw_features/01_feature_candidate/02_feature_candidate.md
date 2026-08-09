# Feature Candidate 통합안 — 주가 흐름 예측 피쳐 후보

- 작성일: 2026-07-31
- 입력 문서:
  - [01_feature_candidate_F.md](../01_feature_candidate_F.md) — 문헌·카테고리 중심 정리 (P/F/V/Q/D/M 번호 체계)
  - [01_feature_candidate_X.md](../01_feature_candidate_X.md) — 예측 문제·구현 명세 중심 정리 (컬럼명, 등급, Phase, 검증 설계)
- 선행 조사: [00_raw_feature_inventory.md](../00_raw_feature_inventory.md)
- 데이터 기준: `snapshot_date=2026-07-30/source=sj2_remote`

이 문서는 두 입력 문서를 종합한 단일 기준 문서다. 피쳐 정의는 X의 컬럼명·산식을
표준으로 하고, F의 카테고리 ID·문헌 근거·한국시장 주의사항을 병합했다. 두 문서가
상충하는 지점은 §4에서 명시적으로 결정했다.

## 0. 예측 문제와 선정 원칙

### 0.1 예측 문제 정의

- **기본**: EOD `t`까지 알려진 정보로 `t+1`부터 향후 **20거래일의 날짜×시장 내
  초과수익 순위(rank)** 예측.
- 보조 horizon: 5거래일, 60거래일.
- 횡단면(cross-sectional) 우선 — 종목 간 상대 순위 피쳐를 우선하고, 시계열(마켓
  타이밍) 피쳐는 레짐/조건 변수로만 사용한다 (§3.6).

### 0.2 선정 원칙

1. **계산 가능성**: 인벤토리의 raw 테이블만으로 유도 가능 (외부 데이터 추가 수집 불요).
2. **PIT(point-in-time) 정합성**: 재무는 접수일 기준 lag, 매크로는
   `available_from_date`. 룩어헤드가 원천적으로 불가능한 정의만 채택.
3. **근거 병기**: 대표 문헌 또는 현업 표준. 한국시장에서 효과가 다른 경우(모멘텀
   약화, 외국인 수급 유효, 개인 비중 등) 별도 표기.
4. **과최적화 경계**: 논문 유의성 ≠ 미래 한국시장 예측력. 출판 후 predictor 수익
   평균 58% 감소(McLean & Pontiff 2016), anomaly 65%가 microcap 완화·가치가중
   조건에서 탈락(Hou, Xue & Zhang 2020). 아래 등급은 **구현 우선순위**이지 수익
   보장 점수가 아니며, 최종 채택은 §6 acceptance gate로 판정한다.

### 0.3 등급 체계

| 등급 | 의미 |
|---|---|
| **근거 A** | 고전적·반복 검증된 학술 factor 또는 현업 지수 채택 축 |
| **근거 A-KR** | A + 한국시장 직접 연구 존재 |
| **근거 B** | 결과는 강하지만 horizon·시장·유동성에 따라 부호/크기 민감 |
| **근거 B-KR** | 한국 직접 연구지만 단일/최근 연구 의존 |
| **R0** | `daily_ohlcv` + `krx_security_flow_raw`만으로 즉시 계산 |
| **R1** | canonical metric 또는 단순 raw 정규화/PIT join 보강 후 계산 |
| **R2** | XBRL concept 표준화, 공시시각, corporate action 정제 선행 필요 |

## 1. 최우선 shortlist (통합)

| 순위 | Feature family | 대표 컬럼 | F ID | 방향 | 근거 | 준비도 |
|---:|---|---|---|---|---|---|
| 1 | 중기 모멘텀 (+잔차) | `px_mom_12_1`, `px_mom_6_1`, `px_resid_mom_12_1` | P1 | `+` | A | R0 |
| 2 | 정규화 투자자 수급 | `flow_foreign_netbuy_to_volume_20d`, `flow_inst_netbuy_to_volume_20d` | F1, F2 | 대체로 `+` | A-KR | R0 |
| 3 | 공매도 강도 | `flow_short_turnover_20d`, `flow_short_interest_ratio` | F6, F7 | `-` | A | R0/R1 |
| 4 | 외국인 long–공매도 short (NAT proxy) | `flow_nat_proxy_20d` | (신규) | `+` | B-KR | R1 |
| 5 | 52주 고점 근접도 | `px_near_52w_high` | P3 | `+` | A | R0 |
| 6 | 단기 반전 | `px_reversal_5d` | P2 | 최근 하락일수록 `+` | A/B | R0 |
| 7 | MAX/저변동성 | `px_maxret_20d`, `px_idio_vol_60d` | P5, P4 | 대체로 `-` | A/B | R0 |
| 8 | 가치 composite | `fin_value_z` (B/M·E/P·CFO/P·S/P) | V1~V4 | 쌀수록 `+` | A | R1 |
| 9 | 수익성/quality | `fin_gross_profitability`, `fin_operating_profitability` | Q1, Q2 | `+` | A | R1 |
| 10 | 투자/자산성장 | `fin_asset_growth_yoy` | Q4 | `-` | A | R1 |
| 11 | 발생액 | `fin_accruals_to_assets` | Q3 | `-` | A | R1 |
| 12 | 실적 서프라이즈/PEAD | `fin_sue`, `fin_earnings_drift` | Q6 | 서프라이즈 방향 | A-KR | R1 |
| 13 | 주식 발행/희석 | `ev_net_share_issuance_yoy` | Q5 | 발행 증가 `-` | A | R1 |
| 14 | 순주주환원 | `ev_payout_yield` → `ev_net_payout_yield` | D1~D5 | `+` | A | R1/R2 |
| 15 | 유동성/거래비용 | `px_amihud_20d`, `px_zero_ret_ratio_20d` | P6, P10 | 기대수익 `+` / 실행가능성 `-` | A/B | R0 |

수급/공매도(2·3·4위)는 인벤토리 §5에서 "완전 미사용 77M행"으로 지목된 도메인이자
글로벌 팩터로 복제 불가능한 한국 특화 엣지 — **구현 비용 대비 기대가치 최우선 후보군**.

## 2. 공통 계산 규칙·PIT

### 2.1 표기와 정규화

```text
r[i,t]       = ln(close[i,t] / close[i,t-1])
turnover     = close * volume                  # 거래대금 미수집 → 근사
shares_pit   = t 시점에 알려진 최신 발행주식수
float_pit    = 최신 유통주식수(distb_stock_co) 또는 issued_shares - treasury_shares
market_cap   = close * shares_pit              # 계단형 근사 — 분모용으론 충분
market_ret   = 같은 market의 일별 동일가중 수익률
```

- window는 달력일이 아닌 **유효 KRX 거래 row** 기준.
- 모든 비율 분모에 `NULLIF(denominator, 0)`.
- 극단치는 mart에서 버리지 않고 모델 전처리에서 날짜×시장별 winsorize/rank 또는
  robust z-score.
- 횡단 표준화는 기본 `(trade_date, market)` 안에서 수행 (KOSPI/KOSDAQ 구조 차이 완충).
- raw level보다 `ratio / change / rank / z-score`를 모델에 노출.
- volume=0(거래정지, 전체 2.2%) 행 처리 정책을 **마트 공통 유틸로 고정** (제외 vs
  forward-fill — 인벤토리 §4-5와 일관).

### 2.2 예측 시점과 PIT 규칙

기본 예측 시점은 **장 마감 후 EOD `t`** — `t`의 OHLCV는 사용 가능, label은 `t+1` 시작.

| 원천 | 사용 가능 시점 |
|---|---|
| OHLCV | `trade_date=t` 장 마감 후 |
| 투자자 수급 | KRX 게시 완료 시각 확인 전에는 live 경로에서 보수적 1거래일 lag도 비교 |
| 공매도 거래량/대금 | 일반 수급과 동일 원칙 |
| 공매도 잔고 | 측정일 ≠ 공개일. raw 최신일이 다른 flow보다 약 2일 늦음 → 공개 지연을 별도 `available_from`으로 모델링 |
| DART 재무 | `rcept_no`/접수일 이후. 접수시각 dimension 확보 전에는 분기 `period_end+45d`, 연간 `+90d` 보수 lag |
| 공통 시장/매크로 | `available_from_date <= feature_date` 필수; 해외 시장은 한국 EOD보다 늦게 끝나는 같은 달력일 값 사용 금지 |

- 재무 분기값은 누적을 그대로 섞지 않는다: `Q2=half−Q1`, `Q3=q3−half`,
  `Q4=annual−q3`로 standalone quarter 생성 후 TTM/YoY 계산. TTM은 분기 수집
  시작(2016)으로 인해 2017~ 계산 가능.
- 2025년부터 OFS(별도) 없음 → **CFS(연결) 기준 통일**. 2015년은 연간만 존재.
- KOSPI 2014-01-20 이전 구간은 KOSDAQ 편향 → 학습 구간 시작은 2014-06(공통 피쳐
  시작과 정렬) 이후 권장.

### 2.3 조정주가 선행 조건 (Phase 1 진입 조건)

현재 `daily_ohlcv`만으로 계산한 장기 수익률은 액면분할·병합·무상증자 시 가짜
급등락을 만든다. 아래 중 하나를 반드시 병행한다.

1. 신뢰 가능한 adjusted close/corporate-action factor 구축
2. `dart_share_count_raw`의 급격한 주식수 변화 + 비정상 OHLC jump로 해당 window 결측 처리
3. 최소한 `abs(r) > 가격제한폭+허용오차`와 주식수 급변을 quality flag로 기록

이 없이 `12-1 momentum`, `MAX`, 변동성, SUE announcement return을 구현하면 모델이
corporate action 오류를 학습한다.

### 2.4 공매도 제도 레짐 (F6~F9, NAT 공통)

전면 금지 구간이 표본에 포함된다: **2020-03 금지 → 2021-05 부분 재개,
2023-11 전면 금지 → 2025-03-31 전면 재개**. 금지 구간은 값이 0/결측이 되어 레짐
더미 없이 학습하면 시계열 단절을 alpha로 오인한다 → **금지기간 flag 피쳐 또는 구간
제외 정책 필수**. 공매도 잔고는 2016-06-30 이후만 존재 — `is_available` flag를 두고
이전 구간을 0으로 채우지 않는다.

## 3. 피쳐 카탈로그 (도메인별 통합)

### 3.1 가격·거래량 (원천: `daily_ohlcv` — 표본 최장, R0)

| F ID | 컬럼 | 정의 | 방향 | 근거 | 비고 |
|---|---|---|---|---|---|
| P1 | `px_mom_12_1`, `px_mom_6_1` | `ln(close[t-21]/close[t-252])`, `ln(close[t-21]/close[t-126])` — 최근 1개월 skip | `+` | A (Jegadeesh & Titman 1993) | **한국은 모멘텀 약화/부재 보고 다수** → 단독보다 잔차·수급 결합. 조정주가 선결(§2.3) |
| — | `px_resid_mom_12_1`, `px_rel_mom_12_1` | 252d rolling market-model residual 누적 (최소 유효 126일) / 시장 상대 | `+` | A (Blitz, Huij & Martens 2011) | label(시장 내 rank)과 정합적 — 클래식보다 우선 검증. 변형 `px_mom_12_1_risk_adj = mom/vol_252d`는 ablation으로 증분 확인 |
| P2 | `px_reversal_5d`, `px_reversal_1d` | `−sum(r, 5d)`, `−r[t]` | 하락 후 `+` | A/B (Jegadeesh 1990) | 한국은 반전이 모멘텀보다 강한 편(개인 비중). 거래비용·bid-ask bounce 민감 → turnover/Amihud/MAX interaction 평가. 20d label엔 5d 주 후보, 1d는 5d label용 |
| P3 | `px_near_52w_high` | `close / max(close, 252d) − 1` | `+` | A (George & Hwang 2004) | 아시아에서 모멘텀 대비 견조. 기존 `px_dist_52w_high`와 동일 계열 — 이름/부호 고정 + warm-up 252d 강제 |
| P5 | `px_maxret_20d`, `px_top5ret_20d` | `max(r, 20d)`, 상위 5개 평균 | `-` | A/B (Bali, Cakici & Whitelaw 2011) | 개인 비중 높은 한국·대만에서 강함. 상하한가 ±30%로 MAX가 상한가에 뭉침 → `px_limit_up_count_20d`와 비교 후 **대표 신호 1개만** 채택. IVOL과 중복 ablation |
| P4 | `px_realized_vol_20d`, `px_idio_vol_60d`, `px_beta_252d`, `px_downside_beta` | 실현변동성 / market-model residual std / beta | 저변동 `+`, IVOL 대체로 `-` | A/B (Ang et al. 2006; Baker et al. 2011) | IVOL 부호는 시장·표본·MAX 통제에 민감 → **독립 alpha가 아닌 B등급 risk/quality feature**. beta는 단독 방향성보다 `regime × beta` 조건부 exposure용(§3.6) |
| P9 | (range vol) | Parkinson/Garman-Klass `ln(high/low)` 기반 | — | (Parkinson 1980) | close-to-close std보다 효율적 추정 — alpha보다 risk forecast/control 보조 입력 |
| P6 | `px_amihud_20d` | `mean(abs(r) / (close×volume), 20)` | 기대수익 `+` / 실행 `-` | A (Amihud 2002) | 거래대금 미수집 → `close×volume` 근사. volume=0 행 제외 필수. alpha이자 **거래가능성 필터/비용모형 입력** |
| P7 | 회전율 변화 (level 금지) | `volume/issued_shares` 21d 평균의 **변화율만** | 고회전 `-` | (Datar et al. 1998; Lee & Swaminathan 2000) | raw turnover level은 가격·규모와 섞임 → 단독 입력 금지(§7). issued_shares는 DART PIT 결합 |
| P8 | `px_turnover_shock`, `px_volume_trend` | `log(turnover / median(turnover, 60d))`, `mean(vol,20)/mean(vol,120)−1` | 단기 `+` | (Gervais, Kaniel & Mingelgrin 2001) | 단기(1~4주) 예측용. `px_mom_x_volume = rank(mom_12_1)×rank(volume_trend)` interaction 후보 (Lee & Swaminathan 2000) |
| P10 | `px_zero_ret_ratio_20d` | `mean(r==0 or volume==0, 20~63d)` | 필터 | (Lesmond et al. 1999) | 소형주 유동성 필터 겸용, 거래정지 처리와 일관 |

### 3.2 투자자 수급·외국인 보유·공매도 (원천: `krx_security_flow_raw` — **최우선**)

한국 특화 연구에서 예측력이 가장 일관되게 보고되는 영역 (Hong & Lee 2011; Bae, Min
& Jung 2011; Froot et al. 2001; Richards 2005). 단, 연구별로 price pressure·momentum
chasing·반전 결과가 섞이므로 **raw 순매수 부호를 곧바로 alpha로 간주하지 않고
정규화·innovation·horizon을 분리**한다.

| F ID | 컬럼 | 정의 | 방향 | 근거 | 준비도 | 비고 |
|---|---|---|---|---|---|---|
| F1, F2, F3 | `flow_{g}_netbuy_to_volume_{5,20,60}d`, `flow_{g}_netbuy_to_float_{h}d` | `sum(net_buy_volume_g, h) / sum(volume, h)` 및 `/ float_pit`, g ∈ {foreign, inst, individual} | foreign/inst 대체로 `+`, **individual 부호 미고정** | A-KR | R0 | 거래량 분모=단기 체결 강도, float 분모=ownership 이동 강도. 금액 순매수 미수집 → `수량×close` 시총 정규화는 보조 버전. 단순 `sum(shares)`는 규모 비교 불가로 폐기 |
| — | `flow_{g}_unexpected_20d`, `flow_{g}_z_60d` | rolling model(`netbuy ~ lagged_return + lagged_flow + turnover + vol`) residual / 60d z-score | — | B (Campbell, Ramadorai & Schwartz 2009) | R1 | 기관 flow는 자기상관·수익률 반응 → 누적치보다 unexpected가 순수 후보. 모델은 과거 데이터만으로 매일 재추정 |
| F4 | `flow_foreign_holding_ratio` | `foreign_holding_shares / float_pit` | level `+` (quality proxy) | A-KR (Ferreira & Matos 2008) | R0 | **단독 사용 비권장**(§7) — size/index 편입 효과와 혼재. 0 급락·주식수 불일치 시 결측 처리, forward-fill 금지 |
| F5 | `flow_foreign_holding_chg_{5,20,60}d` | ratio의 h일 변화분 | `+` | A-KR | R0/R1 | **level보다 변화분의 예측력이 강함** (국내 실증). corporate action 근처 stale flag |
| F10 | `flow_smart_agreement`, `flow_smart_retail_divergence` | `rank(foreign)+rank(inst)` / `−rank(individual)` 추가 | `+` | B | R0 | 세 주체 합은 기타법인 제외로 0이 아님 → balance identity 아님(§4). "외국인은 옳다" 가정이 아니라 방향 합의의 증분 정보 검증 |
| F6 | `flow_short_turnover_{h}d`, `flow_short_value_ratio_{h}d` | `sum(short_volume,h)/sum(volume,h)`, 대금 버전 | `-` | A (Diether, Lee & Werner 2009; Boehmer, Jones & Zhang 2008) | R0 | share/value 상관 높으면 하나만 유지. **레짐 flag 필수**(§2.4) |
| F7 | `flow_short_interest_ratio` | `short_balance_quantity / float_pit` | `-` | A (Asquith et al. 2005; Rapach et al. 2016) | R1 | 2016-06-30 이후만. `is_available` flag, 공개 지연 `available_from` 이후 as-of join |
| F8 | `flow_days_to_cover` | `short_balance_quantity / mean(volume, 20)` | `-` (스퀴즈 위험 프록시) | (Hong et al. 2016) | R1 | F7과 상호보완 — 거래량은 flow, 잔고는 position이므로 별도 family |
| F9 | `flow_short_interest_chg_20d` | ratio의 20d 변화분 | 증가 `-`, 급감은 스퀴즈성 반등 | — | R1 | |
| — | `flow_nat_proxy_20d` (Net Arbitrage Trading proxy) | `percentile_rank(AHF_20) − percentile_rank(ASI_20)` | `+` | B-KR (Jeong, Eo & Kang 2026) | R1 | 외국인 abnormal long − 공매도 abnormal short의 A0 고정 proxy. 원 논문 formation/ranking과 대조해 replication 버전 별도 추가 |
| — | 수급-가격 발산 교차항 | 예: 주가 하락 & 외국인 순매수 지속 (F1>0 × P2 구간) | `+` | 현업 스타일 | R0 | 단독보다 interaction으로 |

구현 위치: metric_rules 확장이 아니라 별도 flow feature 마트(`research/etl/marts/`)
— raw/derived 분리 원칙 부합. 최신일 2026-07-24 정지(KRX 비밀번호 만료)는 수동
조치 후 백필 확인 필요.

### 3.3 규모·밸류에이션 (원천: DART canonical + `daily_ohlcv` + `share_count`, R1)

모든 재무 피쳐는 접수일 이후 as-of join, `fin_age_days` 동반 제공.

| F ID | 컬럼 | 정의 | 근거 | 비고 |
|---|---|---|---|---|
| — | `fin_log_mcap` | `ln(market_cap)` | A (Fama & French 1992) | size — regime interaction의 기본 축이기도 함 |
| V1 | `fin_book_to_market` | `total_equity / market_cap` | A (Fama & French 1992, 1993) | 한국 밸류 장기 유효 실증 두터움. `total_equity<=0`이면 NULL + `negative_equity` flag. 지배주주지분 기준 검토 |
| V2 | `fin_earnings_yield` | `controlling_net_income_ttm / market_cap` | A (Basu 1977) | 적자기업은 composite로 보존 — P/E 단독 금지(§7) |
| V3 | `fin_cfo_yield` | `operating_cash_flow_ttm / market_cap` | A (Lakonishok, Shleifer & Vishny 1994) | 한국은 이익조정 여지로 E/P보다 CFO/P 선호 실무 관행 |
| V4 | `fin_sales_to_price` | `revenue_ttm / market_cap` | (Barbee et al. 1996) | 금융업 제외 필요 (industry code 미수집 → 당분간 극단치·market별 검증으로 대체) |
| V5 | FCF yield | `(CFO − capex_ppe − capex_intangible)_ttm / market_cap` | 현업 표준 | canonical에 capex 2종 존재 |
| V6 | EV/EBITDA 근사 | `(mktcap + total_liabilities − cash) / (op_income + D&A)` | (Loughran & Wellman 2011) | 이자부부채 미매핑 → **총부채 근사임을 명시**. D&A는 기존 XBRL rule 활용 |
| — | **`fin_value_z`** | B/M·E/P·CFO/P·S/P의 시장 상대 z-score 평균 (유효값만) | A | **대표 밸류 신호로 우선 채택** — 적자기업 보존 |

### 3.4 수익성·quality·투자·실적 (원천: DART canonical, R1~R2)

| F ID | 컬럼 | 정의 | 방향 | 근거 | 준비도 | 비고 |
|---|---|---|---|---|---|---|
| Q1 | `fin_gross_profitability` | `gross_profit_ttm / avg_assets` | `+` | A (Novy-Marx 2013) | R1 | 밸류와 상보적. `revenue/cogs/gross_profit/sga/operating_income` canonical coverage 실측 선행 — 낮으면 XBRL fallback 보강 후 활성화 |
| Q2 | `fin_operating_profitability`, `fin_roa`, `fin_roe`, `fin_operating_margin`, `fin_asset_turnover` | TTM / avg_assets·avg_equity | `+` | A (Fama & French 2015; HXZ q-factor) | R1 | level + YoY(Δ) 둘 다. `avg = (t + t-4q)/2` |
| Q3 | `fin_accruals_to_assets` | `(net_income_ttm − CFO_ttm) / avg_assets` | `-` | A (Sloan 1996) | R1 | balance-sheet-free 정의 — canonical만으로 가능. 보조: `fin_cash_earnings_quality = CFO/abs(NI)` (clipped) + `cfo_positive`/`ni_positive` flag |
| Q4 | `fin_asset_growth_yoy` | `total_assets_t / total_assets_t-4q − 1` | `-` | A (Cooper, Gulen & Schill 2008; 국제: Titman et al. 2013) | R1 | 보조: `fin_capex_to_assets`, `fin_inventory_growth_yoy`. 분모 음수/극소 → NULL, level diff 대안 |
| Q5 | `ev_net_share_issuance_yoy` | `issued_shares_pit / lag_1y − 1` | 발행 증가 `-` | A (Pontiff & Woodgate 2008) | R1→R2 | 액면분할·무상증자도 증가로 보임 → `redc`/`profit_incnr`/`rdmstk_repy`/`etc` dim + 가격 jump로 경제적 issuance와 기계적 변경 구분 후 활성화. 그 전에는 quality flag 필수 |
| Q6 | `fin_sue`, `fin_earnings_drift` | seasonal random-walk SUE: `(EPS_q − EPS_q-4) / rolling_std(8Q)` (standalone 분기, analyst forecast 부재) | 서프라이즈 방향 | A-KR (Ball & Brown 1968; Bernard & Thomas 1989; 한국: Eom, Hahn & Sohn 2019) | R1 | 분기 2016~ → 8Q history로 2018년경부터. **접수일 이후 노출이 핵심** (PEAD는 공시 직후 드리프트). interaction: `fin_pead_retail_opposition = −z(SUE) × z(개인 순매수, 공시 후 5d)` — 개인 역방향 거래 시 drift 강화 검증, event window는 접수일 이후로 절단 |
| — | `fin_announcement_abret_3d`, `fin_sue_minus_price_reaction` | 공시 event window 초과수익 / SUE 대비 미반영도 | underreaction `+` | B | **R2** | DART `rcept_time`(장중/장후) dimension 필요 |
| Q7 | 운전자본 변화 | `Δ(재고 + 매출채권) / total_assets` | `-` | (Thomas & Zhang 2002) | **R2** | 매출채권 미매핑 → metric_rules 확장 필요 |
| Q8 | `fin_fscore_partial_7` + components | ROA>0, CFO>0, ΔROA>0, CFO>NI, Δmargin>0, Δturnover>0, 무증자 (7항목) | `+` | A (Piotroski 2000) | R1/R2 | current ratio·장기부채 변화는 XBRL 보강 후 9항목 완성. **원형 0~9 F-score로 가장하지 않고 `partial_7` 명명**, 각 component도 개별 노출 |

공통: 지주회사·금융업은 계정 구조가 다름 — 섹터 더미 또는 제외 (industry code 수집 전에는 검증으로 대체).

### 3.5 주주환원·발행 (원천: `dart_shareholder_return_raw`, `share_count`, treasury_stock)

현재 canonical에는 `dps` 하나만 매핑 — raw에 값이 이미 있어 확장 비용이 낮다.

| F ID | 컬럼 | 정의 | 근거 | 준비도 | 비고 |
|---|---|---|---|---|---|
| D1 | `ev_dividend_yield` | `dps_ttm / close` 또는 raw `현금배당수익률` 직접 | A (Fama & French 1988) | R1 | 연 단위 → 일 단위 브로드캐스트 |
| D2 | 배당성향 | raw `현금배당성향(연결)` 직접 | (Skinner 2008) | R1 | 적자기업 극단값 윈저라이즈 |
| D3 | `ev_buyback_yield`, `ev_treasury_share_change` | `treasury_share_acquisition_amount_ttm / mktcap`, `−Δtreasury_shares_1y / issued_shares` | A (Ikenberry, Lakonishok & Vermaelen 1995) | R1 | 기말 자기주식비율보다 **취득·소각 flow가 우선**. 현금흐름표 취득금액 ↔ 수량 변화 교차검증. 2025~ 밸류업 정책으로 소각 증가 — 레짐 변화 유의 |
| D4 | `ev_retirement_intensity` | `소각 변동수량 / issued_shares` | 소각은 매입보다 강한 신호 (한국: 매입 후 재매각 관행) | R2 | 취득방법/목적 dim 파싱 필요 |
| D5 | `ev_payout_yield` → `ev_net_payout_yield` | `(배당총액 + buyback)_ttm / mktcap` → `− issuance` 추가 | A (Boudoukh et al. 2007 — dividend 단독보다 우수) | R1 → R2 | 배당총액은 shareholder_return raw 우선, 없으면 `DPS×유통주식수` 근사. issuance 추정은 corporate action 정제 전 신뢰도 낮음 → net payout은 R2 |
| — | `ev_free_float_ratio`, `ev_dilution_ratio` | `float/issued`, `diluted/weighted_avg − 1` | — | R1 | 보조 |

### 3.6 시장 폭·글로벌·매크로 — **조건부(interaction) 피쳐 원칙**

main label이 날짜×시장 내 rank이므로, 같은 날짜에 broadcast되는 VIX·금리·지수
수익률은 **당일 횡단면 차이를 설명하지 못한다** (날짜별 z-score 시 상수 → 0).
따라서 공통 피쳐는 단독 alpha가 아니라 **종목 exposure와의 interaction**으로만
사용하며, macro 수를 늘리기보다 경제적으로 해석 가능한 interaction만 사전 등록한다
(equity-premium predictor OOS 불안정: Welch & Goyal 2008; Goyal, Welch & Zafirov 2024).

| F ID | 파생 series | 정의 | 대표 interaction | 비고 |
|---|---|---|---|---|
| M4, M5 | `cf_breadth_balance`, `cf_ad_line_20d`, `cf_breadth_divergence`, `cf_market_turnover_shock` | `(adv−dec)/(adv+dec)`, 20d 합, 지수수익 z − AD z, `log(turnover/median60)` | `px_beta_252d ×`, `px_amihud_20d ×`, `flow_foreign_netbuy ×` | breadth raw 6종 기존재 — 비율화만 필요. C등급 market-state feature (Brown & Cliff 2004) |
| — | `cf_small_growth_regime` | `kosdaq_ret_20d − kospi_ret_20d` | `× (−fin_log_mcap)` 또는 `× is_kosdaq` | 어떤 종목이 regime에 민감한지 표현 |
| M2 | VIX: `ΔVIX`, percentile, high-regime flag (예: >25) | level보다 변화/flag | `× px_downside_beta`, `× flow_foreign_*` | risk-on/off 표준 (Whaley 2000). 외국인 수급 조건 변수로 유효 |
| M3, M6 | `usdkrw_return_{5,20}d`, 미-한 금리차 `rate_us10y − rate_kr_gov10y` | FX 모멘텀, 자본유출입 압력 | `× foreign_holding_ratio`, `× foreign_netbuy` | 원화 약세 → 외국인 순매도 전이 (국내 실증) |
| M1 | 기간 스프레드 `rate_kr_gov10y − 3y`, `us10y − 2y` level/Δ/부호 더미 | 경기 선행 (Estrella & Hardouvelis 1991) | `× beta`, `× leverage` | level은 카탈로그 기존재 — Δ·더미만 추가 |
| M7 | CPI/PPI/M2 YoY 가속/감속 더미, `wti_return_20d`, 소비자심리 변화 | 인플레 레짐별 팩터 성과 차이 (Neville et al. 2021) | `× size`, `× liquidity`, `× leverage` | 반드시 `available_from_date` 기준. 해외 값은 같은 달력일 사용 금지(§2.2) |
| — | `global_risk` composite, `px_global_beta` | `z(ΔVIX) − z(sp500_ret) + z(usdkrw_ret)`, `rolling_beta(r, lagged_sp500)` | `× foreign_holding`, `× foreign_netbuy_5d` | |

## 4. 두 문서 간 차이와 통합 결정

| 항목 | F안 | X안 | 통합 결정 |
|---|---|---|---|
| 수급 정규화 분모 | 시총 (`수량×close / mktcap`) | 거래량 / 유통주식수 이원화 | **X 채택** (거래량=체결 강도, float=ownership 이동). 시총 정규화는 보조 버전. 금액 순매수 미수집 제약은 공통 |
| 개인 순매수 부호 | 역지표(−) 고정 | 부호 미고정 (유동성 공급 반전 + 추격매매 혼재) | **부호 미고정** — divergence·interaction(`flow_smart_retail_divergence`)으로 사용 |
| 3주체 순매수 합 | 합≈0 → 공선성, 2개만 투입 | 기타법인 제외로 합≠0 | **X 정정 채택** (balance identity 아님). 단 다중공선성 점검 자체는 유지 |
| 52주 고점 분자 | `max(high, 252d)` | `max(close, 252d)` | **`max(close)`** — 기존 `px_dist_52w_high` 구현과 정합 |
| 단기 반전 window | 21d (1M) | 1d / 5d | family 사전 등록: **5d 주 후보(20d label)**, 1d(5d label), 21d 보조 — window grid search 금지(§6.2) |
| 중기 모멘텀 | 한국 약화 경고, 잔차·수급 결합 권장 | shortlist 1위 + residual momentum 권장 | **클래식(6-1/12-1)과 residual/relative 둘 다 구현**, 한국 약화 가능성은 acceptance gate에서 판정. 두 문서 모두 잔차 버전을 지지 — 실질 이견 없음 |
| 저변동성/IVOL | alpha 후보 (P4) | 부호 민감 → B등급 risk feature | **risk/quality feature로 취급**, MAX와 중복 기여 ablation |
| 밸류 개별 vs composite | V1~V6 개별 나열 | `fin_value_z` composite 우선 | **composite 대표 채택**, 개별(V1~V6)은 component로 유지·노출 |
| 매크로 사용법 | 레짐 분류·게이팅 | 날짜 내 상수 소거 → interaction 원칙 | 같은 취지 — **X의 정식화 채택** (§3.6). F의 M1~M7은 interaction 재료 목록으로 흡수 |
| F-Score | 7~8개 축소판으로 시작 | `fin_fscore_partial_7` 명명 + component 개별 노출 | X 채택 (원형으로 가장하지 않기) |
| EV/EBITDA | 후보 포함 (V6) | 미언급 | 포함하되 총부채 근사 명시, `fin_value_z`보다 후순위 |
| 재무 lag | 보수 lag(Q+45/Y+90) 또는 접수일 | 접수일 이후, 접수시각 확보 전 보수 lag | 동일 — **접수일 raw 기반 `available_from` 규칙을 재무 전체에 일괄 적용**, 접수시각 dimension은 Phase 3 |

## 5. 구현 로드맵

### Phase 0 — 공통 인프라 선결 과제 (피쳐 정의보다 먼저)

1. **조정주가/corporate-action anomaly flag** (§2.3) — momentum/MAX/SUE의 진입 조건.
2. **수익률·거래정지 처리 유틸**: volume=0(2.2%) 행 정책을 마트 공통 함수로 고정.
3. **PIT 시총/주식수 daily 테이블**: `close × issued_shares` 일별 브로드캐스트
   (share_count 접수일 기준 PIT 결합) — flow/value/payout 군의 공통 분모.
4. **재무 접수일 lag 정책** 일괄 적용 (매크로 `available_from_date`와 동일 철학).
5. **공매도 레짐 더미** (§2.4) + 잔고 `is_available` flag.
6. **레이블 정의 확정**: `t+1`~`t+20` 시장 내 초과수익 rank (보조 5/60d),
   상하한가·거래정지 종목의 진입 불가 처리.
7. **생존편향 완화**: DELISTED 28개뿐인 master → `stock_master_snapshot` 기반 PIT
   universe를 스냅샷 존재 구간에서라도 사용. 개선 전 장기 성과에는 경고 부착.

### Phase 1 — 가격 + 수급 (R0, 즉시)

1. `px_mom_6_1`, `px_mom_12_1`, `px_resid_mom_12_1`, `px_reversal_5d`, `px_near_52w_high`
2. `px_maxret_20d`, `px_idio_vol_60d`, `px_turnover_shock`, `px_amihud_20d`, `px_zero_ret_ratio_20d`
3. 투자자별 `netbuy_to_volume_{5,20,60}d` (+ float 분모 버전)
4. `foreign_holding_ratio/chg`, `short_turnover`, `short_value_ratio`
5. `short_interest_ratio/chg`, `days_to_cover`, `flow_nat_proxy_20d`

ablation group: `P1-A` classic price / `P1-B` risk·liquidity / `P1-C` investor
net-buy / `P1-D` short selling / `P1-E` NAT·interactions.

### Phase 2 — canonical 재무 (R1)

분기 standalone/TTM mart → daily PIT shares·mktcap → `fin_value_z` →
gross/operating profitability → asset growth/capex → accruals/cash quality →
issuance·basic payout. (공시일 lag 정책 확정이 선결.)

### Phase 3 — 공시 이벤트·XBRL·미매핑 계정 (R2)

접수일·접수시각 dimension → SUE·announcement abnormal return → PEAD × 개인
역방향 수급 → full F-Score용 유동자산/부채·장기부채·매출채권 metric_rules 확장 →
자사주 취득·처분·소각 dim 정규화 → net payout yield.

### Phase 4 — regime interaction

breadth/turnover 파생 series → beta·downside beta·foreign-flow와 공통 피쳐
interaction → KOSPI/KOSDAQ·VIX·USD/KRW regime별 conditional IC.

## 6. 검증 설계

### 6.1 Feature acceptance gate (core set 승격 조건 — 전부 통과)

1. **Coverage**: 목표 학습기간 non-null coverage·종목 수 충분.
2. **PIT**: `available_from <= feature_date`, 수정공시는 수정 접수 이후만.
3. **Sanity**: 산식 항등성, corporate action, 0-volume/정지일, 공매도 공개지연.
4. **단변량 IC**: 날짜×시장별 Spearman Rank-IC 평균, ICIR, 부호 안정성.
5. **경제성**: decile spread, turnover, 거래비용 차감 후 spread.
6. **증분성**: baseline 대비 purged walk-forward OOS Rank-IC/NDCG/Top-K 개선.
7. **안정성**: KOSPI/KOSDAQ, 대/중/소형, 고/저유동성, bull/bear subperiod.
8. **단일 holdout**: feature 선택에 쓰지 않은 마지막 holdout 1회 평가.

### 6.2 Multiple testing 통제

- family당 대표 산식 + 5/20/60d(또는 6/12개월) window만 **사전 등록**.
- RSI 7/9/14, MA 5/10/20/30/60 식 window grid search 금지.
- 후보가 늘면 FDR 또는 block bootstrap reality check.
- 신규 factor 통계 기준은 `t>1.96`보다 높게 (Harvey, Liu & Zhu 2016) — 단변량
  발견보다 반복 OOS 안정성 우선.

### 6.3 거래가능 universe 이원 보고

1. 연구용 broad universe
2. 실행가능 universe: 최근 20일 거래대금 하한, 가격 하한, 정지/관리종목 제외

anomaly 다수가 microcap·비유동주에서만 성립할 수 있으므로 broad 결과만으로
채택하지 않는다.

## 7. 우선 개발 제외 목록

계산 가능하지만 지금 우선 개발하지 않는다.

- **RSI/MACD/stochastic/다수 MA crossover**: 동일 OHLCV의 중복 변환 + window 자유도
  → multiple-testing 위험. classic momentum/reversal/52주 고점/MAX가 먼저.
- **raw 가격·거래량·순매수 shares level**: 기업규모·액면가 혼입.
- **95만 XBRL concept 전체 투입**: 기업 확장 태그·context 중복 암기 위험. 표준
  concept + 경제적 가설 먼저.
- **standalone VIX/금리/CPI broadcast**: 횡단면 rank label에서 날짜 내 상수 (§3.6).
- **P/E 단독**: 적자기업 결측/부호 문제 → `fin_value_z` composite 우선.
- **공매도 평균체결가/종가 괴리 단독**: 집계·동시적 가격효과 해석 불명확.
- **외국인 보유 level 단독**: size·liquidity·index 편입 효과 혼재 → 변화율·
  unexpected·NAT 우선.

## 8. v1 추천 feature set (~30개)

첫 버전은 100개 대신 아래 30개 안팎으로 시작한다. 재무 coverage가 낮은 초기
run은 price+flow 20개로 먼저 OOS baseline을 만들고 재무는 family 단위로 추가한다.

```text
price (9):
  mom_6_1, mom_12_1, resid_mom_12_1, reversal_5d, near_52w_high,
  maxret_20d, idio_vol_60d, amihud_20d, turnover_shock_20d

flow (11):
  foreign/inst/individual_netbuy_to_volume_5d,
  foreign/inst/individual_netbuy_to_volume_20d,
  foreign_holding_chg_20d,
  short_turnover_20d, short_interest_ratio, short_interest_chg_20d,
  days_to_cover, nat_20d

financial/event (9):
  log_mcap, value_z, gross_profitability, operating_profitability,
  asset_growth_yoy, accruals_to_assets, sue,
  net_share_issuance_yoy, payout_yield
```

이 구성은 F안의 우선순위(수급 > 가격 > 밸류/퀄리티 > 주주환원 > 매크로)와 X안의
준비도(R0 → R1 → R2)·검증 설계를 동시에 만족한다.

## 부록. 대표 참고문헌 (통합)

가격/모멘텀/변동성/유동성:

- [Jegadeesh & Titman (1993)](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.1993.tb04702.x) — 모멘텀
- [Jegadeesh (1990)](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.1990.tb05110.x); Lehmann (1990) — 단기 반전
- [Blitz, Huij & Martens (2011)](https://www.sciencedirect.com/science/article/pii/S0927539811000041) — residual momentum
- [George & Hwang (2004)](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2004.00695.x) — 52주 고점
- [Bali, Cakici & Whitelaw (2011)](https://www.sciencedirect.com/science/article/pii/S0304405X1000190X) — MAX
- [Ang, Hodrick, Xing & Zhang (2006)](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2006.00836.x); Baker et al. (2011) — IVOL/low-vol
- [Amihud (2002)](https://www.sciencedirect.com/science/article/pii/S1386418101000246) — 비유동성
- [Lee & Swaminathan (2000)](https://onlinelibrary.wiley.com/doi/10.1111/0022-1082.00280); Datar et al. (1998); Gervais, Kaniel & Mingelgrin (2001); Lesmond et al. (1999); Parkinson (1980)

수급/공매도 (한국 포함):

- [Hong & Lee (2011)](https://www.sciencedirect.com/science/article/pii/S0922142511000405); [Bae, Min & Jung (2011)](https://onlinelibrary.wiley.com/doi/10.1111/j.2041-6156.2011.01037.x) — 한국 투자자별 거래
- Froot, O'Connell & Seasholes (2001); Richards (2005) — 신흥시장 외국인 플로우
- [Campbell, Ramadorai & Schwartz (2009)](https://www.sciencedirect.com/science/article/pii/S0304405X09000026/pdf) — 기관 flow 분해
- Ferreira & Matos (2008) — 외국인 지분
- [Diether, Lee & Werner (2009)](https://academic.oup.com/rfs/article/22/2/575/1596032); [Boehmer, Jones & Zhang (2008)](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2008.01324.x) — 공매도 거래
- Asquith, Pathak & Ritter (2005); Rapach, Ringgenberg & Zhou (2016); Hong et al. (2016) — short interest / DTC
- [Jeong, Eo & Kang (2026)](https://www.sciencedirect.com/science/article/pii/S0927538X26000855) — 한국 Net Arbitrage Trading

재무/quality/이벤트:

- [Fama & French (1992)](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.1992.tb04398.x), (1993), [(2015)](https://www.sciencedirect.com/science/article/pii/S0304405X14002323/pdf) — size/value/5-factor
- Basu (1977); Lakonishok, Shleifer & Vishny (1994); Barbee et al. (1996); Loughran & Wellman (2011)
- [Novy-Marx (2013)](https://www.sciencedirect.com/science/article/pii/S0304405X13000044) — gross profitability
- [Sloan (1996)](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2598); Thomas & Zhang (2002) — accruals
- [Cooper, Gulen & Schill (2008)](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2008.01370.x); Titman et al. (2013) — asset growth
- [Pontiff & Woodgate (2008)](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2008.01335.x) — share issuance
- [Piotroski (2000)](https://www.jstor.org/stable/2672906) — F-Score
- Ball & Brown (1968); Bernard & Thomas (1989); [Eom, Hahn & Sohn (2019)](https://www.sciencedirect.com/science/article/pii/S0927538X17305930) — PEAD (한국 포함)
- [Ikenberry, Lakonishok & Vermaelen (1995)](https://www.sciencedirect.com/science/article/pii/0304405X9500826Z); Skinner (2008); [Boudoukh et al. (2007)](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2007.01226.x) — buyback/payout

매크로/방법론:

- Estrella & Hardouvelis (1991) — term spread; Whaley (2000) — VIX; Brown & Cliff (2004) — breadth; Neville et al. (2021) — 인플레 레짐
- [Welch & Goyal (2008)](https://academic.oup.com/rfs/article-abstract/21/4/1455/1565737); [Goyal, Welch & Zafirov (2024)](https://academic.oup.com/rfs/article/37/11/3490/7749383) — 매크로 predictor OOS
- [McLean & Pontiff (2016)](https://onlinelibrary.wiley.com/doi/10.1111/jofi.12365); [Hou, Xue & Zhang (2020)](https://academic.oup.com/rfs/article/33/5/2019/5236964); [Harvey, Liu & Zhu (2016)](https://academic.oup.com/rfs/article-abstract/29/1/5/1843824) — replication/multiple testing
- [MSCI Factor Indexes](https://www.msci.com/indexes/factor-indexes/msci-factor-indexes) — 현업 factor 분류
