# 주가 흐름 예측 Feature Candidate

- 작성일: 2026-07-31
- 입력 조사: [00_raw_feature_inventory.md](./00_raw_feature_inventory.md)
- 데이터 기준: `snapshot_date=2026-07-30/source=sj2_remote`
- 기본 예측 문제: **EOD `t`까지 알려진 정보로 `t+1`부터 향후 20거래일의 시장 내 초과수익 순위 예측**
- 보조 horizon: 5거래일, 60거래일

## 0. 결론

현재 raw에서 먼저 만들 가치가 큰 피쳐는 아래 네 묶음이다.

1. **가격의 중기 모멘텀·단기 반전·52주 고점·MAX/저변동성**
2. **외국인/기관 수급 강도와 공매도 강도를 거래량·유통주식수로 정규화한 피쳐**
3. **가치·수익성·투자·발생액·실적 서프라이즈**
4. **증자/자사주/배당을 합친 순주주환원과 외국인 long–공매도 short 복합 피쳐**

학계와 현업에서 공통으로 널리 쓰이는 큰 축도 대체로 **Value, Momentum, Quality,
Low Volatility, Size, Yield**다. MSCI도 이들을 장기간 관측된 systematic factor로
분류하며, momentum은 6/12개월 성과를 변동성 조정하고 value는 book-to-price와
earnings yield, quality는 수익성·투자·이익의 질·레버리지·이익 변동성을 함께 본다
([MSCI Factor Indexes](https://www.msci.com/indexes/factor-indexes/msci-factor-indexes),
[MSCI factor definition methodology](https://www.msci.com/eqb/methodology/meth_docs/MSCI_Diversified_Multiple_Factor_With_Low_Volatility_Indexes_Methodology_May2018.pdf)).

다만 “논문에서 유의했다”는 “향후 한국 시장에서 거래비용 후에도 예측된다”는 뜻이
아니다. 97개 predictor의 논문 밖 수익은 평균 26% 감소했고, 출판 후에는 58% 감소했다
([McLean & Pontiff, 2016](https://onlinelibrary.wiley.com/doi/10.1111/jofi.12365)),
452개 anomaly의 65%가 microcap 완화·가치가중 조건에서 통상적인 유의성 기준도
통과하지 못했다
([Hou, Xue & Zhang, 2020](https://academic.oup.com/rfs/article/33/5/2019/5236964)).
아래의 등급은 이 점을 반영한 **구현 우선순위**이지 수익을 보장하는 점수가 아니다.

### 0.1 최우선 shortlist

| 순위 | Feature family | 대표 컬럼 | 기대 방향 | 근거 | 준비도 |
|---:|---|---|---|---|---|
| 1 | 중기 모멘텀 | `px_mom_12_1`, `px_mom_6_1` | 높을수록 `+` | A | R0 |
| 2 | 정규화 투자자 수급 | `flow_foreign_netbuy_to_volume_20d`, `flow_inst_netbuy_to_volume_20d` | 대체로 `+` | A-KR | R0 |
| 3 | 공매도 강도 | `flow_short_turnover_20d`, `flow_short_interest_ratio` | 높을수록 `-` | A | R0/R1 |
| 4 | 외국인 long–공매도 short | `flow_nat_20d` | 높을수록 `+` | B-KR | R1 |
| 5 | 52주 고점 근접도 | `px_near_52w_high` | 높을수록 `+` | A | R0 |
| 6 | 단기 반전 | `px_reversal_5d` | 최근 하락일수록 `+` | A/B | R0 |
| 7 | MAX/저변동성 | `px_maxret_20d`, `px_idio_vol_60d` | 높을수록 대체로 `-` | A/B | R0 |
| 8 | 가치 composite | `fin_value_z` | 쌀수록 `+` | A | R1 |
| 9 | 수익성/quality | `fin_gross_profitability`, `fin_operating_profitability` | 높을수록 `+` | A | R1 |
| 10 | 투자/자산성장 | `fin_asset_growth_yoy` | 높을수록 `-` | A | R1 |
| 11 | 발생액/현금이익 | `fin_accruals_to_assets` | 높을수록 `-` | A | R1 |
| 12 | 실적 서프라이즈/PEAD | `fin_sue`, `fin_earnings_drift` | 서프라이즈 방향과 동일 | A-KR | R1 |
| 13 | 주식 발행/희석 | `ev_net_share_issuance_yoy` | 발행 증가일수록 `-` | A | R1 |
| 14 | 순주주환원 | `ev_net_payout_yield` | 높을수록 `+` | A | R1/R2 |
| 15 | 유동성/거래비용 | `px_amihud_20d`, `px_zero_ret_ratio_20d` | 기대수익에는 `+`, 실현가능성에는 `-` | A/B | R0 |

- **근거 A**: 고전적이고 반복 검토된 학술 factor 또는 현업 지수에 채택된 축.
- **근거 A-KR**: 위 조건에 더해 한국 시장 직접 연구가 있음.
- **근거 B**: 결과가 강하지만 horizon·시장·유동성에 따라 부호나 크기가 민감함.
- **근거 B-KR**: 한국 시장 직접 연구지만 아직 단일/최근 연구 의존도가 큼.
- **R0**: `daily_ohlcv`와 `krx_security_flow_raw`만으로 즉시 계산.
- **R1**: 기존 canonical metric 또는 단순한 raw 정규화/PIT join 보강 후 계산.
- **R2**: XBRL concept 표준화, 공시시각, corporate action 정제가 선행돼야 함.

## 1. 공통 계산 규칙

### 1.1 표기

```text
r[i,t]       = ln(close[i,t] / close[i,t-1])
turnover     = close * volume
shares_pit   = t 시점에 알려진 최신 발행주식수
float_pit    = 최신 유통주식수(distb_stock_co)
               또는 issued_shares - treasury_shares
market_cap   = close * shares_pit
market_ret   = 같은 market의 일별 동일가중 수익률
```

- window는 달력일이 아닌 **유효 KRX 거래 row**로 계산한다.
- 모든 비율의 분모에는 `NULLIF(denominator, 0)`을 적용한다.
- 극단치는 feature mart에서 버리지 않고, 모델 전처리에서 날짜×시장별 winsorize/rank
  또는 robust z-score를 적용한다.
- KOSPI와 KOSDAQ의 구조 차이를 줄이기 위해 종목 횡단 표준화는 기본적으로
  `(trade_date, market)` 안에서 수행한다.
- raw level보다는 `ratio`, `change`, `rank`, `z-score`를 모델에 노출한다.

### 1.2 예측 시점과 PIT

기본 예측 시점은 **장 마감 후 EOD `t`**다. 따라서 `t`의 OHLCV는 사용할 수 있고,
label은 `t+1`부터 시작한다.

| 원천 | 사용 가능 시점 |
|---|---|
| OHLCV | `trade_date=t` 장 마감 후 |
| 투자자 수급 | KRX 게시 완료 시각을 확인하기 전에는 live 경로에서 보수적으로 1거래일 lag도 비교 |
| 공매도 거래량/대금 | 일반 수급과 같은 원칙 |
| 공매도 잔고 | 측정일과 공개일이 다를 수 있음. raw 최신일이 다른 flow보다 약 2일 늦으므로 공개 지연을 별도 `available_from`으로 모델링 |
| DART | `rcept_no`/접수일 이후. 정확한 접수시각 dimension 전에는 현재 코드처럼 분기 `period_end+45d`, 연간 `+90d` 보수 lag |
| 공통 시장/매크로 | 반드시 `available_from_date <= feature_date`; 해외 시장은 한국 EOD보다 뒤에 끝나는 같은 달력일 값을 쓰지 않음 |

재무 분기값은 누적 금액을 그대로 섞지 않는다. `Q2=half-Q1`,
`Q3=q3-half`, `Q4=annual-q3`로 standalone quarter를 만든 후 TTM과 YoY를 계산한다.

### 1.3 조정주가 선행 조건

현재 `daily_ohlcv`만으로 계산한 장기 수익률은 액면분할·병합·무상증자 시 가짜
급등락을 만들 수 있다. 아래 중 하나를 반드시 병행한다.

1. 신뢰 가능한 adjusted close/corporate-action factor 구축
2. `dart_share_count_raw`의 급격한 주식수 변화와 비정상 OHLC jump를 이용해 해당
   window를 결측 처리
3. 최소한 `abs(r)>가격제한폭+허용오차`와 주식수 급변을 quality flag로 남김

이 조건 없이 `12-1 momentum`, `MAX`, 변동성, SUE announcement return을 구현하면
모델이 corporate action 오류를 학습할 수 있다.

## 2. 가격·모멘텀·위험·유동성

### 2.1 중기 모멘텀 — 최우선

```text
px_mom_12_1 = ln(close[t-21] / close[t-252])
px_mom_6_1  = ln(close[t-21] / close[t-126])
```

- 최근 1개월을 건너뛰어 단기 반전과 microstructure 영향을 분리한다.
- 현재 `px_ret_60d`, `px_mom_20_60`보다 학술 momentum 정의에 가깝다.
- 3~12개월 과거 winner가 이후에도 강한 현상은
  [Jegadeesh & Titman (1993)](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.1993.tb04702.x)의
  대표 결과이며, 현업 momentum 지수도 6/12개월 성과를 핵심 입력으로 사용한다.
- 20일 label에는 `6-1`, `12-1`을 둘 다 두되 window를 더 늘리지 않는다.

추가 후보:

```text
px_mom_12_1_risk_adj = px_mom_12_1 / realized_vol_252d
```

변동성 조정 momentum은 crash exposure를 줄이는 실무적 변형이다. 단, raw momentum과
동시에 넣고 ablation으로 증분 IC를 확인한다.

### 2.2 시장 상대/잔차 모멘텀

```text
residual[t]          = r[i,t] - alpha[i] - beta[i] * market_ret[t]
px_resid_mom_12_1   = sum(residual[t-252 : t-21])
px_rel_mom_12_1     = stock_mom_12_1 - market_mom_12_1
```

- 현재 label이 같은 날짜·시장 내 초과수익 rank이므로 raw momentum보다 target과
  더 정합적이다.
- 최소 구현은 시장 상대수익, 권장 구현은 252일 rolling market model residual이다.
- residual momentum은 conventional momentum보다 factor exposure가 낮고 더 안정적인
  위험조정 성과를 보였다는 연구가 있다
  ([Blitz, Huij & Martens, 2011](https://www.sciencedirect.com/science/article/pii/S0927539811000041)).
- 추정 window의 최소 유효 관측 수는 126일로 제한한다.

### 2.3 단기 반전

```text
px_reversal_1d = -r[t]
px_reversal_5d = -sum(r[t-4:t])
```

- 단기 급등 종목의 되돌림, 급락 종목의 반등을 포착한다.
- 개별 종목 월수익률의 유의한 음의 1차 자기상관은
  [Jegadeesh (1990)](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.1990.tb05110.x)의
  고전적 결과다.
- 반전은 거래비용과 bid–ask bounce에 민감하므로 `turnover`, `Amihud`, `halt`,
  `MAX`와 interaction을 평가한다.
- 20일 label에는 `reversal_5d`가 주 후보이고, `reversal_1d`는 5일 label에서 더
  중요한 후보로 둔다.

### 2.4 52주 고점 근접도

```text
px_near_52w_high = close[t] / max(close[t-251:t]) - 1
```

- 0에 가까울수록 최근 52주 고점에 가깝다.
- 52주 고점 근접도는 과거수익률보다 future return 설명력을 더했다는 대표적인
  anchoring signal이다
  ([George & Hwang, 2004](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2004.00695.x)).
- 현재 구현된 `px_dist_52w_high`와 동일 계열이므로 이름/부호만 명시적으로 고정하고
  warm-up 252일을 강제하면 된다.

### 2.5 MAX와 lottery 성향

```text
px_maxret_20d  = max(r[t-19:t])
px_top5ret_20d = mean(largest 5 r in t-19:t)
```

- 최근 한 달에 극단적으로 큰 양의 수익률을 기록한 종목은 이후 수익률이 낮은 경향이
  보고됐다
  ([Bali, Cakici & Whitelaw, 2011](https://www.sciencedirect.com/science/article/pii/S0304405X1000190X)).
- 한국처럼 일일 가격제한폭이 있는 시장에서는 MAX가 상한가에 뭉친다. `MAX`와
  `top5 average`, 상한가 일수(`px_limit_up_count_20d`)를 같이 비교하되 대표 신호는
  하나만 채택한다.
- `MAX`는 idiosyncratic volatility와 강하게 겹치므로 두 family의 중복 기여를
  ablation한다.

### 2.6 저변동성·idiosyncratic volatility·beta

```text
px_realized_vol_20d = std(r, 20)
px_idio_vol_60d     = std(residual_from_market_model, 60)
px_beta_252d        = cov(r, market_ret, 252) / var(market_ret, 252)
px_downside_beta    = beta estimated only when market_ret < 0
```

- 높은 idiosyncratic volatility 종목의 낮은 평균수익은
  [Ang et al. (2006)](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2006.00836.x)의
  대표 anomaly이며, low-volatility는 현업 factor index에도 채택돼 있다.
- 다만 IVOL의 부호는 시장·표본·MAX 통제에 민감하므로 **독립 alpha A가 아니라
  B등급 risk/quality feature**로 취급한다.
- `beta`는 단독 방향성보다 `market regime × beta`, `VIX shock × downside_beta`의
  조건부 exposure에 더 유용하다.
- OHLC를 활용한 Parkinson/range volatility는 realized volatility의 보조 추정치로
  만들 수 있으나 alpha라기보다 risk forecast/control이다.

### 2.7 유동성·거래활동

```text
px_amihud_20d       = mean(abs(r) / turnover, 20)
px_zero_ret_ratio   = mean(r == 0, 20)
px_turnover_shock   = log(turnover[t] / median(turnover[t-59:t]))
px_volume_trend     = mean(volume, 20) / mean(volume, 120) - 1
px_mom_x_volume     = rank(px_mom_12_1) * rank(px_volume_trend)
```

- Amihud ratio는 일별 절대수익률/거래대금으로 가격충격을 근사하는 고전적 측정치다
  ([Amihud, 2002](https://www.sciencedirect.com/science/article/pii/S1386418101000246)).
- 비유동성 premium이 있더라도 실제 매매비용은 반대 방향으로 커진다. 따라서
  `px_amihud_20d`는 alpha이면서 동시에 **거래가능성 필터/비용모형 입력**이다.
- momentum과 거래량의 결합도 널리 연구됐다
  ([Lee & Swaminathan, 2000](https://onlinelibrary.wiley.com/doi/10.1111/0022-1082.00280)).
- raw `px_turnover` level은 가격·기업규모와 섞이므로 단독 입력하지 않는다.

## 3. 투자자 수급·외국인 보유·공매도

한국 시장에서는 이 도메인이 가장 차별화된 raw다. 한국 일별 투자자별 자료 연구는
외국인·기관 거래가 개인보다 정보주도적일 수 있음을 보고했고
([Hong & Lee, 2011](https://www.sciencedirect.com/science/article/pii/S0922142511000405)),
외국인이 산 종목이 판 종목보다 수익률과 영업성과에서 우월했다는 결과도 있다
  ([Bae, Min & Jung, 2011](https://onlinelibrary.wiley.com/doi/10.1111/j.2041-6156.2011.01037.x)).
동시에 연구별로 단기 price pressure·momentum chasing·반전 결과가 섞여 있으므로
**raw 순매수량의 부호를 곧바로 alpha로 간주하지 않고 정규화·innovation·horizon을
분리**해야 한다.

### 3.1 투자자별 순매수 강도

각 `g ∈ {foreign, institution, individual}`에 대해:

```text
flow_g_netbuy_to_volume_hd =
    sum(net_buy_volume_g, h) / sum(volume, h)

flow_g_netbuy_to_float_hd =
    sum(net_buy_volume_g, h) / float_shares_pit

h ∈ {5, 20, 60}
```

- 현재 `feat_flow`의 단순 `sum(shares)`는 삼성전자와 소형주를 비교할 수 없다.
- 거래량 분모는 단기 체결 강도, 유통주식수 분모는 ownership 이동 강도를 뜻한다.
- core window는 5/20/60 세 개로 제한한다.
- `individual`은 예상 부호를 고정하지 않는다. 단기 유동성 공급에 따른 반전과
  정보 없는 추격매매가 섞일 수 있으므로 외국인/기관과의 divergence로도 사용한다.

### 3.2 예상된 flow와 예상 밖 flow 분리

```text
expected_flow =
    rolling_model(netbuy_intensity ~ lagged_return + lagged_flow + turnover + volatility)

flow_g_unexpected_20d = actual_flow - expected_flow
```

단순 대안:

```text
flow_g_z_60d = (netbuy_intensity[t] - mean_60d) / std_60d
```

- 기관 거래는 자기상관이 강하고 최근 수익률에 반응하므로, 단순 누적 순매수에는
  정보와 feedback trading이 섞인다.
- 일별 기관거래가 단기에는 가격압력/반전, 장기에는 수익성을 보인 연구가 있다
  ([Campbell, Ramadorai & Schwartz, 2009](https://www.sciencedirect.com/science/article/pii/S0304405X09000026/pdf)).
- `unexpected_flow`가 raw 누적치보다 더 순수한 후보이며, rolling model은 과거
  데이터만으로 매일 재추정한다.

### 3.3 외국인 보유비율과 변화

```text
flow_foreign_holding_ratio =
    foreign_holding_shares / float_shares_pit

flow_foreign_holding_chg_hd =
    foreign_holding_ratio[t] - foreign_holding_ratio[t-h]
```

- level보다 5/20/60일 변화가 우선이다.
- 분모 주식수의 공시 빈도가 낮아 corporate action 근처에는 stale flag를 둔다.
- `foreign_holding_shares`가 0으로 급락하거나 주식수 변동과 불일치하면 결측으로
  처리하고 forward-fill하지 않는다.

### 3.4 투자자 간 합의와 divergence

```text
flow_smart_agreement =
    rank(foreign_netbuy_to_volume_20d)
    + rank(inst_netbuy_to_volume_20d)

flow_smart_retail_divergence =
    rank(foreign_netbuy_to_volume_20d)
    + rank(inst_netbuy_to_volume_20d)
    - rank(individual_netbuy_to_volume_20d)
```

- 세 투자자 순매수 합은 기타법인이 빠져 0이 아니므로 balance identity로 쓰지 않는다.
- 이 피쳐는 “외국인/기관은 항상 옳고 개인은 항상 틀리다”는 가정이 아니라, 서로 다른
  투자자군의 방향 합의가 증분 정보를 갖는지 검증하는 interaction이다.

### 3.5 공매도 거래 강도

```text
flow_short_turnover_hd =
    sum(short_selling_volume, h) / sum(volume, h)

flow_short_value_ratio_hd =
    sum(short_selling_value, h) / sum(close * volume, h)
```

- share 비율과 value 비율을 둘 다 계산하되 상관이 매우 높으면 하나만 유지한다.
- 높은 일별 short-selling activity가 향후 음의 수익률을 예측한다는 결과가 있다
  ([Diether, Lee & Werner, 2009](https://academic.oup.com/rfs/article/22/2/575/1596032),
  [Boehmer, Jones & Zhang, 2008](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2008.01324.x)).
- 공매도 금지·재개 같은 제도 regime을 별도 flag로 두지 않으면 시계열 단절을 alpha로
  오인할 수 있다.

### 3.6 공매도 잔고와 days-to-cover

```text
flow_short_interest_ratio =
    short_balance_quantity / float_shares_pit

flow_days_to_cover =
    short_balance_quantity / mean(volume, 20)

flow_short_interest_chg_20d =
    short_interest_ratio[t] - short_interest_ratio[t-20]
```

- 거래량은 flow, 잔고는 누적 position이므로 둘은 별도 family다.
- 잔고 데이터는 2016-06-30 이후만 존재한다. `is_available` flag를 추가하고 이전
  구간을 0으로 채우지 않는다.
- 공개 지연을 반영한 `available_from` 이후에만 as-of join한다.

### 3.7 Net Arbitrage Trading — 한국시장 특화 복합 피쳐

```text
AHF_20 = foreign_holding_ratio[t] - mean(foreign_holding_ratio, 20)
ASI_20 = short_interest_ratio[t]   - mean(short_interest_ratio, 20)

flow_nat_20d =
    percentile_rank(AHF_20, within date×market)
    - percentile_rank(ASI_20, within date×market)
```

외국인 보유의 비정상 증가(long)와 공매도 잔고의 비정상 증가(short)를 한 축에 놓는다.
2026년 한국시장 연구는 이 두 abnormal position의 차이인 NAT가 미래 횡단면 수익률을
양(+)으로 예측한다고 보고했다
([Jeong, Eo & Kang, 2026](https://www.sciencedirect.com/science/article/pii/S0927538X26000855)).

- 원 논문의 정확한 formation window/ranking을 appendix와 대조해 replication
  version을 별도로 고정한다.
- 위 산식은 현재 raw에 맞춘 `v0` 제안이며, 논문식과 다르면 이름에 `_proxy`를 붙인다.
- 최근 단일 한국 연구에 의존하므로 B-KR로 두지만, 현재 raw의 두 희소 원천을 함께
  쓰는 매우 유망한 후보다.

## 4. 재무·가치·quality·실적

재무 피쳐는 20일마다 바뀌지 않지만, 가격 기반 분모가 매일 바뀌고 공시 후 정보의
효력이 지속된다. 매일 panel에 as-of join하되 `fin_age_days`를 함께 제공한다.

### 4.1 규모와 가치

```text
fin_log_mcap          = ln(market_cap)
fin_book_to_market    = total_equity / market_cap
fin_earnings_yield    = controlling_net_income_ttm / market_cap
fin_cfo_yield         = operating_cash_flow_ttm / market_cap
fin_sales_to_price    = revenue_ttm / market_cap

fin_value_z =
    mean(valid market-relative z-scores of B/M, E/P, CFO/P, Sales/P)
```

- size와 book-to-market은 평균수익률 횡단면을 설명하는 가장 오래된 대표
  characteristic다
  ([Fama & French, 1992](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.1992.tb04398.x)).
- 단일 P/E보다 적자기업을 보존하는 composite를 우선한다.
- `total_equity <= 0`이면 B/M을 억지로 계산하지 않고 `negative_equity` flag와 NULL을
  유지한다.
- 금융업은 leverage와 일부 accounting ratio의 의미가 다르지만 현재 industry code가
  없다. 업종 분류를 수집하기 전에는 최소한 극단치와 market별 검증이 필요하다.

### 4.2 수익성과 operating quality

```text
avg_assets = (assets_t + assets_t-4q) / 2
avg_equity = (equity_t + equity_t-4q) / 2

fin_gross_profitability     = gross_profit_ttm / avg_assets
fin_operating_profitability = operating_income_ttm / avg_assets
fin_roa                     = net_income_ttm / avg_assets
fin_roe                     = controlling_net_income_ttm / avg_equity
fin_operating_margin        = operating_income_ttm / revenue_ttm
fin_asset_turnover          = revenue_ttm / avg_assets
```

- gross profits/assets는 book-to-market과 비슷한 수준의 횡단면 설명력을 보였고
  value와 상보적이었다
  ([Novy-Marx, 2013](https://www.sciencedirect.com/science/article/pii/S0304405X13000044)).
- profitability와 investment는 Fama–French 5-factor의 핵심 축이다
  ([Fama & French, 2015](https://www.sciencedirect.com/science/article/pii/S0304405X14002323/pdf)).
- 현재 canonical에서 `revenue/cogs/gross_profit/sga/operating_income` coverage를
  먼저 실측해야 한다. coverage가 낮으면 표준 XBRL concept fallback을 보강한 뒤
  활성화한다.

### 4.3 투자와 자산성장

```text
fin_asset_growth_yoy =
    total_assets_t / total_assets_t-4q - 1

fin_capex_to_assets =
    (capex_ppe_ttm + capex_intangible_ttm) / lagged_total_assets

fin_inventory_growth_yoy =
    inventory_t / inventory_t-4q - 1
```

- 자산성장이 높은 기업의 이후 수익률이 낮은 investment anomaly는 강한 대표
  characteristic다
  ([Cooper, Gulen & Schill, 2008](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2008.01370.x)).
- `asset_growth`는 R1, 세부 working-capital growth는 표준 XBRL concept mapping이
  필요한 R2다.
- 성장률 분모가 음수/매우 작으면 NULL 처리하고 level difference/avg assets 대안을
  사용한다.

### 4.4 발생액과 현금이익

```text
fin_accruals_to_assets =
    (net_income_ttm - operating_cash_flow_ttm) / avg_assets

fin_cash_earnings_quality =
    operating_cash_flow_ttm / abs(net_income_ttm)

fin_cfo_positive = operating_cash_flow_ttm > 0
```

- 이익 중 accrual 비중이 높을수록 지속성이 낮고 시장이 이를 늦게 반영한다는
  [Sloan (1996)](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2598)의
  대표 결과를 현재 canonical metric만으로 근사할 수 있다.
- `cash_earnings_quality`는 순이익이 0에 가까울 때 폭발하므로 clipped ratio와
  `cfo_positive`, `net_income_positive` flag를 함께 쓴다.
- balance-sheet accrual은 current assets/current liabilities 표준화 후 R2로 추가한다.

### 4.5 재무 개선도와 Piotroski 계열

```text
fin_delta_roa
fin_delta_gross_margin
fin_delta_asset_turnover
fin_delta_leverage
fin_no_new_shares
fin_fscore
```

원형 F-score는 profitability, leverage/liquidity, operating efficiency의 9개
binary signal을 합친다
([Piotroski, 2000](https://www.jstor.org/stable/2672906)).

- 현재 바로 가능한 항목: ROA>0, CFO>0, ΔROA>0, CFO>NI, Δmargin>0,
  Δasset_turnover>0, no issuance.
- current ratio와 장기부채 변화는 XBRL/FS 표준 계정 보강 후 추가한다.
- 7개만 구현할 때는 `fin_fscore_partial_7`처럼 원형과 다른 이름을 사용하고, 0~9
  F-score로 가장하지 않는다.
- binary 합계뿐 아니라 각 component도 모델에 남겨 어느 항목이 한국 시장에서
  기여하는지 확인한다.

### 4.6 실적 서프라이즈와 PEAD — 강한 이벤트 후보

```text
quarterly_eps =
    standalone_controlling_net_income / weighted_avg_shares

fin_sue =
    (quarterly_eps_t - quarterly_eps_t-4q)
    / rolling_std(quarterly_eps_q - quarterly_eps_q-4, previous 8 quarters)

fin_earnings_growth_yoy =
    standalone_net_income_t / abs(standalone_net_income_t-4q) - 1
```

- 공시 후 주가가 earnings surprise 방향으로 수주~수개월 더 움직이는 PEAD는 가장
  오래 연구된 anomaly 중 하나다.
- 한국 직접 연구에서도 PEAD가 확인됐고, 개인이 실적 뉴스 반대 방향으로 거래할 때
  drift가 강했다
  ([Eom, Hahn & Sohn, 2019](https://www.sciencedirect.com/science/article/pii/S0927538X17305930)).
- analyst forecast가 없으므로 seasonal random-walk SUE
  (`EPS_q - EPS_q-4`)를 사용한다.
- 최소 8분기 history가 없으면 NULL이며, 실제 접수일 이후에만 노출한다.

추가 interaction:

```text
fin_pead_retail_opposition =
    -z(fin_sue) * z(individual_netbuy_to_volume_5d_after_announcement)
```

양의 SUE 뒤 개인 순매도, 음의 SUE 뒤 개인 순매수처럼 뉴스 반대 거래가 클 때
drift가 강화되는지를 직접 표현한다. 공시 전 수급을 섞지 않도록 event window를
접수일 이후로 자른다.

### 4.7 공시일 가격반응과 미반영 정보

```text
fin_announcement_abret_3d =
    stock_return[event_window] - market_return[event_window]

fin_sue_minus_price_reaction =
    rank(fin_sue) - rank(fin_announcement_abret_3d)
```

- 큰 실적 서프라이즈에 비해 announcement return이 작으면 underreaction 후보로 본다.
- DART 접수시각이 장중/장후인지 알아야 event window가 정확해진다.
- 현재 raw에는 명시적 `rcept_time` dimension이 없으므로 이 피쳐는 R2다.

## 5. 발행주식수·배당·자사주·주주환원

### 5.1 순주식 발행과 희석

```text
ev_net_share_issuance_yoy =
    issued_shares_pit / issued_shares_pit_lag_1y - 1

ev_dilution_ratio =
    diluted_shares / weighted_avg_shares - 1

ev_free_float_ratio =
    float_shares / issued_shares
```

- 주식 발행 증가는 이후 낮은 수익률을 예측하는 강한 characteristic로 보고됐다
  ([Pontiff & Woodgate, 2008](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2008.01335.x)).
- 그러나 액면분할·무상증자도 shares 증가로 보일 수 있다. `redc`, `profit_incnr`,
  `rdmstk_repy`, `etc`와 가격 jump를 이용해 경제적 issuance와 기계적 주식수 변경을
  구분한 뒤 활성화한다.
- 현재 `ev_shares_chg_yoy`는 이 구분이 없으므로 quality flag가 필요하다.

### 5.2 자사주 매입·소각

```text
ev_buyback_yield =
    treasury_share_acquisition_amount_ttm / market_cap

ev_treasury_share_change =
    -(treasury_shares_t - treasury_shares_t-1y) / issued_shares

ev_retirement_intensity =
    retired_treasury_shares / issued_shares
```

- 자사주 매입 발표 후 장기 underreaction은 고전적으로 보고됐다
  ([Ikenberry, Lakonishok & Vermaelen, 1995](https://www.sciencedirect.com/science/article/pii/0304405X9500826Z)).
- 한국 raw의 `treasury_stock`는 취득·처분·소각과 목적 dimension을 보유하므로
  단순 기말 자기주식비율보다 **취득과 소각 flow**가 더 좋은 후보다.
- 현금흐름표 취득금액과 수량 변화가 일치하는지 교차검증한다.

### 5.3 배당·payout·net payout

```text
ev_dividend_yield =
    dps_ttm / close

ev_payout_yield =
    (cash_dividends_ttm + buyback_amount_ttm) / market_cap

ev_net_payout_yield =
    (cash_dividends_ttm + buyback_amount_ttm - estimated_equity_issuance_ttm)
    / market_cap
```

- dividend만 보지 않고 repurchase와 issuance까지 합친 payout/net payout이 더 많은
  예측 정보를 가진다는 연구가 있다
  ([Boudoukh et al., 2007](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2007.01226.x)).
- `cash_dividends_ttm`은 `dart_shareholder_return_raw`의 배당금총액을 우선하고,
  없을 때만 `DPS × 보통주 유통주식수`로 근사한다.
- `estimated_equity_issuance`는 corporate action 정제 전에는 신뢰도가 낮으므로
  `payout_yield`를 R1, `net_payout_yield`를 R2로 둔다.

## 6. 시장 폭·글로벌·매크로는 “조건부 피쳐”로 사용

현재 main label은 날짜×시장 안에서 순위를 매긴다. 같은 날짜의 모든 종목에 동일한
`VIX`, 금리, KOSPI 수익률을 broadcast하면 그 값 자체는 **당일 횡단면 차이를 설명하지
못한다**. linear 전처리에서 날짜별 z-score를 하면 상수가 되어 0으로 사라진다.
따라서 공통 피쳐는 단독 alpha가 아니라 종목 exposure와 interaction하는 것이 원칙이다.

### 6.1 시장 breadth와 유동성 regime

```text
cf_breadth_balance =
    (advancers - decliners) / (advancers + decliners)

cf_ad_line_20d =
    sum(advancers - decliners, 20)

cf_breadth_divergence =
    z(market_return_20d) - z(cf_ad_line_20d)

cf_market_turnover_shock =
    log(market_turnover / median(market_turnover, 60))
```

종목 interaction:

```text
px_beta_252d * cf_breadth_balance
px_amihud_20d * cf_market_turnover_shock
flow_foreign_netbuy_to_volume_20d * cf_breadth_balance
```

- 지수 상승과 breadth 악화의 divergence, 거래대금 급감 같은 regime을 포착한다.
- breadth는 학술 factor만큼 확립된 단독 predictor라기보다 market-state feature이므로
  C등급 interaction으로 검증한다.

### 6.2 KOSPI–KOSDAQ 상대강도

```text
cf_small_growth_regime =
    kosdaq_return_20d - kospi_return_20d

interaction =
    cf_small_growth_regime * (-fin_log_mcap or stock_market_is_kosdaq)
```

시장별 index return 자체보다 어떤 종목이 해당 regime에 민감한지를 표현한다.

### 6.3 글로벌 risk와 외국인 수급

```text
global_risk =
    z(delta_vix) - z(sp500_return) + z(usdkrw_return)

flow_foreign_risk_interaction =
    foreign_holding_ratio * global_risk

flow_foreign_shock_interaction =
    foreign_netbuy_to_volume_5d * global_risk

px_global_beta =
    rolling_beta(stock_return, lagged_sp500_return)
```

- 미국 장 마감 시각은 한국보다 늦으므로, 한국 EOD `t`에서 같은 미국 달력일
  종가를 쓰지 않는다. raw `available_from_date` 기준으로 lag한다.
- VIX level보다 `ΔVIX`, percentile, high-regime flag를 비교한다.

### 6.4 금리·환율·원자재·월간 매크로

후보:

```text
term_spread_level / change
usdkrw_return_5d / 20d
wti_return_20d
cpi_yoy_change
m2_yoy_change
consumer_sentiment_change
```

이들은 다음과 interaction한다.

- `beta`, `downside_beta`
- 외국인 보유비율·외국인 순매수
- leverage·interest-paid/assets
- size·liquidity

시장 전체 equity-premium predictor는 OOS가 불안정하다는 대규모 재검토 결과가 있다
([Welch & Goyal, 2008](https://academic.oup.com/rfs/article-abstract/21/4/1455/1565737),
[Goyal, Welch & Zafirov, 2024](https://academic.oup.com/rfs/article/37/11/3490/7749383)).
따라서 macro feature 수를 늘리기보다 위의 경제적으로 해석 가능한 interaction만
사전 등록한다.

## 7. 현재 구현과의 gap

| 영역 | 현재 구현 | 보강할 핵심 |
|---|---|---|
| 가격 | 1/5/20/60일 수익률, 20-60 momentum, 변동성, range, turnover, Amihud, MA gap, 52주 고점 | 6-1/12-1, 최근 1개월 skip, market-relative/residual momentum, reversal, MAX, idio vol, turnover shock |
| 수급 | 투자자별 5/20일 raw 수량 합, 보유/잔고 수량 변화, raw z-score | volume/float 정규화, 60일, unexpected flow, investor divergence, short ratio, short interest, days-to-cover, NAT |
| 재무 | ROA, D/E, equity ratio, CFO/assets, cash/assets, ROE, asset turnover, operating margin | market-cap 기반 value, TTM/standalone quarter, gross profitability, asset growth, accrual, F-score components, SUE |
| 이벤트 | 연간 treasury ratio, 보유 여부, issued shares YoY | 분기 PIT shares, issuance/corporate-action 구분, buyback flow, retirement, dividend/payout/net payout |
| 공통 | 지수·금리·FX·VIX 등 level/return broadcast | breadth/turnover regime과 종목 exposure interaction |

## 8. 구현 순서

### Phase 1 — 즉시 구현, 가격+수급

1. adjusted-price/corporate-action anomaly flag
2. `px_mom_6_1`, `px_mom_12_1`, `px_reversal_5d`, `px_near_52w_high`
3. `px_maxret_20d`, `px_idio_vol_60d`, `px_turnover_shock`, `px_amihud_20d`
4. 투자자별 `netbuy_to_volume_{5,20,60}d`
5. `foreign_holding_ratio/chg`, `short_turnover`, `short_value_ratio`
6. `short_interest_ratio/chg`, `days_to_cover`, `flow_nat_20d`

권장 ablation group:

```text
P1-A: classic price
P1-B: risk/liquidity
P1-C: investor net-buy
P1-D: short selling
P1-E: NAT/interactions
```

### Phase 2 — canonical 재무 보강

1. 분기 standalone/TTM metric mart
2. daily PIT shares와 approximate market cap
3. value composite
4. gross/operating profitability
5. asset growth/capex
6. accruals/cash earnings quality
7. issuance와 basic payout

### Phase 3 — 공시 이벤트와 XBRL

1. 접수일·접수시각 source-of-truth dimension
2. seasonal-random-walk SUE와 announcement abnormal return
3. 한국 PEAD × 개인 역방향 수급
4. full Piotroski F-score에 필요한 current assets/liabilities, long-term debt mapping
5. 자사주 취득·처분·소각 dimension 정규화
6. net payout yield

### Phase 4 — regime interaction

1. breadth/turnover derived series
2. beta/downside beta/foreign-flow와 공통 피쳐 interaction
3. KOSPI/KOSDAQ, VIX·USD/KRW regime별 conditional IC

## 9. 검증 설계

### 9.1 Feature acceptance gate

개별 후보는 아래를 모두 통과해야 core set으로 승격한다.

1. **Coverage**: 목표 학습기간에서 non-null coverage와 종목 수가 충분함.
2. **PIT**: `available_from <= feature_date`, 수정공시는 수정 접수 이후에만 반영.
3. **Sanity**: 산식 항등성, corporate action, 0-volume/정지일, 공매도 공개지연 검증.
4. **단변량 IC**: 날짜×시장별 Spearman Rank-IC의 평균, ICIR, 부호 안정성.
5. **경제성**: decile spread, turnover, 거래비용 차감 후 spread.
6. **증분성**: 현재 baseline 대비 purged walk-forward OOS Rank-IC/NDCG/Top-K 수익 개선.
7. **안정성**: KOSPI/KOSDAQ, 대/중/소형, 고/저유동성, bull/bear subperiod에서
   부호와 성능 확인.
8. **단일 holdout**: feature 선택에는 사용하지 않은 마지막 holdout을 한 번만 평가.

### 9.2 Multiple testing 통제

- family당 대표 산식과 5/20/60 또는 6/12개월 window만 사전 등록한다.
- 동일 개념의 RSI 7/9/14, MA 5/10/20/30/60 같은 window grid search를 하지 않는다.
- 후보 수가 늘면 FDR 또는 block bootstrap reality check를 적용한다.
- 새로운 factor의 통계 기준은 단순 `t>1.96`보다 높아야 한다는 제안
  ([Harvey, Liu & Zhu, 2016](https://academic.oup.com/rfs/article-abstract/29/1/5/1843824))을
  고려해, 단변량 발견보다 반복 OOS 안정성을 우선한다.

### 9.3 거래가능 universe

다음 두 결과를 모두 보고한다.

1. 연구용 broad universe
2. 실행가능 universe: 최근 20일 거래대금 하한, 가격 하한, 정지/관리 이슈 제외

많은 anomaly가 microcap·비유동주에서만 보일 수 있으므로 broad universe 결과만으로
채택하지 않는다. 상폐 종목이 28개뿐인 현재 master의 생존편향도 장기 성과를
과대평가할 수 있으므로 PIT universe 개선 전 성과에는 별도 경고를 붙인다.

## 10. 우선순위에서 제외할 피쳐

아래는 계산 가능하지만 지금 우선 개발하지 않는다.

- **RSI, MACD, stochastic, 다수 이동평균 crossover**: 같은 OHLCV의 중복 변환이고
  window 선택 자유도가 커 multiple-testing 위험이 높다. classic momentum/reversal,
  52주 고점, MAX가 먼저다.
- **raw 가격·거래량·순매수 shares level**: 기업규모와 액면가가 섞인다.
- **95만 XBRL concept 전체 투입**: 기업 확장 태그와 context/dimension 중복을 모델이
  외우게 된다. 표준 concept과 경제적 가설을 먼저 고정한다.
- **standalone VIX/금리/CPI broadcast**: 현재 횡단면 rank label에서는 날짜 내 상수다.
- **P/E 단독**: 적자기업 결측/부호 문제가 크므로 value composite가 우선이다.
- **공매도 평균체결가/종가 괴리 단독**: 집계 방식과 동시적 가격효과의 해석이 불명확하다.
- **현재 외국인 보유 level 단독**: size, liquidity, index 편입 효과와 강하게 섞인다.
  변화율·unexpected holding·NAT가 우선이다.

## 11. 추천 feature set 버전

첫 구현 버전은 넓게 100개를 만들기보다 아래 30개 안팎으로 시작한다.

```text
price:
  mom_6_1, mom_12_1, resid_mom_12_1, reversal_5d, near_52w_high,
  maxret_20d, idio_vol_60d, amihud_20d, turnover_shock_20d

flow:
  foreign/inst/individual_netbuy_to_volume_5d,
  foreign/inst/individual_netbuy_to_volume_20d,
  foreign_holding_chg_20d,
  short_turnover_20d, short_interest_ratio, short_interest_chg_20d,
  days_to_cover, nat_20d

financial/event:
  log_mcap, value_z, gross_profitability, operating_profitability,
  asset_growth_yoy, accruals_to_assets, sue,
  net_share_issuance_yoy, payout_yield
```

재무 coverage가 낮은 초기 run은 price+flow 20개 안팎으로 먼저 OOS baseline을 만들고,
재무는 family 단위로 추가한다. 이 순서가 현재 raw의 강점, PIT 난이도, 학술 근거를
가장 균형 있게 반영한다.
