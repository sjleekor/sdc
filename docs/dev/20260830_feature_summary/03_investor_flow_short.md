# 03. 투자자 수급·공매도 — 모델 개발용 피쳐 카드

- 작성일: 2026-09-03
- 대상: `flow_foreign_netbuy_to_volume` · `flow_inst_netbuy_to_volume` · `flow_individual_netbuy_to_volume` ·
  `flow_foreign_holding_ratio_chg` · `flow_short_turnover` · `flow_short_interest` · `flow_days_to_cover` · `flow_nat_proxy_20d`
- 기준 run: Phase A `20260827T221729-4e0ae8b0` / Phase B `20260828T123313-4e0ae8b0` / AB `20260828T165038-4e0ae8b0`
  (snapshot `2026-08-23`, config `889c3e83…`). 매크로·Phase C는 `236d0d35…` 계보(`20260830T085718-efd35e70`, `20260830T122850-phasec`).
- 항목 정의와 읽는 법은 [00_reading_guide.md](00_reading_guide.md)를 먼저 본다.
- 원문 해설: `docs/dev/20260829_raw_features_explain/` 10~17번. 아래 `(12 §5.1)` 식 인용은 그 문서의 절 번호다.

## 0. 그룹 한눈에 보기

| # | family | primary feature | 무엇을 재나 | 기대→관측 부호 | 등급 | screen_pass | 대표 cell IC | 대표 q5 spread | 모델 사용 | 한 줄 메모 |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `flow_foreign_netbuy_to_volume` | `flow_foreign_netbuy_to_volume_20d` | 20일 외국인 순매수 주식 수 / 20일 총거래량 | `+` → `−` | D | 실패 | cum 0→20 −0.0131 | −0.23%p | 미투입 | 반대 방향 5/5 안정. 지연 게이트 실패. Phase C `liq_high` 쌍은 A |
| 2 | `flow_inst_netbuy_to_volume` | `flow_inst_netbuy_to_volume_20d` | 20일 기관 순매수 / 20일 총거래량 | `+` → `−` | D | 실패 | cum 0→20 −0.0209 | −0.68%p | 미투입 | 반대 방향이 가장 강하고 깨끗(q 1.3e-13). tradable에서 강해짐 |
| 3 | `flow_individual_netbuy_to_volume` | `flow_individual_netbuy_to_volume_20d` | 20일 개인 순매수 / 20일 총거래량 | 미고정 → `+` | A | 통과 | cum 0→20 +0.0241 | +0.70%p | T1 후보(비채택) | 부호를 안 건 유일한 Phase A family. 1·2의 거울상일 가능성 |
| 4 | `flow_foreign_holding_ratio_chg` | `flow_foreign_holding_ratio_chg_20d` | 외국인 지분율(보유/유통 PIT)의 20일 차분 | `+` → `−` | D | 실패 | cum 0→20 −0.0080 | −0.03%p | 미투입 | 수급 4개 중 최약. 기간 2/5, 시간 placebo 실패 |
| 5 | `flow_short_turnover` | `flow_short_turnover_20d` | 20일 공매도 거래량 / 20일 총거래량 | `−` → `−` | C(보류) | 대상 아님 | cum 0→60 −0.0668 | +4.40%p(정렬) | 미투입 | exploratory. 표본 2020-03-24 끝. tradable 유지율 0.571 최저 |
| 6 | `flow_short_interest` | `flow_short_interest_ratio` | 공매도 잔고 / 유통주식수 PIT | `−` → `−` | C(보류) | 대상 아님 | cum 0→60 −0.0874 | +2.60%p(정렬) | 미투입 | exploratory. 공매도 4개 중 \|IC\| 최대. 잔고 공표 2세션 지연 반영 |
| 7 | `flow_days_to_cover` | `flow_days_to_cover` | 공매도 잔고 / 20일 평균 거래량 (일) | `−` → `−` | C(보류) | 대상 아님 | cum 0→60 −0.0672 | +3.46%p(정렬) | 미투입 | exploratory. 6과 분자가 같다. 유동성 혼입 의심 |
| 8 | `flow_nat_proxy_20d` | `flow_nat_proxy_20d` | pctrank(외국인 지분율 20일 변화) − pctrank(공매도 잔고 비율) | `+` → `+` | C(보류) | 대상 아님 | cum 0→60 +0.0644 | +2.53%p | 미투입 | exploratory. 6을 재료로 포함하는 조합. 표본 889일로 35개 중 최소 |

### 0.1 여덟 family에 공통으로 걸리는 것

- **원천과 마트.** 전부 `krx_security_flow_raw`(7,600만 행, KRX 우선 중복 제거 후 55,918,702행)를 `feat_flow`로 만든 것이다. 분모 `total_volume`은 `daily_ohlcv.volume`, `float_shares_pit`는 `dim_stock_pit_daily`에서 온다 (10 §2.7, `flow.py` docstring).
- **정본 variant가 `lag1`이다.** 가격 계열(`native_t`)과 반대다. 투자자별 집계를 그날 장 마감 시점에 쓸 수 있는지 검증되지 않아 하루 늦춘 값을 정본으로 썼다 (`execution.flow_unverified_same_day_variant: lag1`, 10 §2.5). 그래서 카드의 "lag1 유지율 1.0"은 **자기 자신과의 비교**이고, 당일 값 대비 손실은 여덟 개 전부 `미측정`이다.
- **완전 창.** 창 길이 N의 N행이 전부 있어야 계산한다(`= N`, `flow.require_complete_window: true`). 거래정지 세션은 창에 들어가기 전에 제거된다(`flow.py:127`, 10 §2.3·§2.4).
- **기업행동 마스킹이 없다.** 가격 계열의 `ca_count_*` 같은 처리가 수급 계열에는 없다. 분모나 재료에 유통주식수가 들어가는 4·6·8이 취약하다 (13 §2.4).
- **공매도 4개(5~8)의 C는 T1의 C = 판정 보류다.** role `exploratory_short_regime`, `fdr_include: false`라 BH·discovery·screen_pass를 계산하지 않았다. 2020-03 공매도 금지로 표본이 2020-03-24에서 잘려 다중검정 모집단에 넣으면 다른 family의 문턱을 왜곡하기 때문이다. **강건성 실패(T2의 C)가 아니다** (14 §3.4, `09_all_feature_results` §2·§6).
- **A×A 상관이 없다.** 1·2·3은 거의 거울상, 6과 7은 분자가 같고, 8은 6을 재료로 포함한다. 여덟 개를 독립된 여덟 신호로 세면 안 된다 (`00_읽는_법` §9.3).
- **운영 원천이 2026-08에 바뀌었다.** KRX MDC 스크래핑이 차단돼 순매수 3종·공매도 거래량·거래대금·외국인 보유주식수 6개는 KIS(`flows sync-kis`)로 옮겼고, `short_selling_balance_quantity`만 KRX에 남았다. 병행 대조에서 순매수 3종은 0건 불일치, 공매도 거래량·거래대금은 KIS가 총량 −1.19% 낮은 정의 차이, `foreign_holding_shares`는 477/2,763(17.3%) 양방향 불일치였다 (`docs/operations.md` "KRX ↔ KIS 병행 대조"). 학습 표본(~2025-02)은 전부 KRX 값이고, 교체일 이후는 정의가 다른 값이 이어진다.
- **숫자 계보.** `09_all_feature_results.md`(`ab0de634…`)는 `flow_inst_netbuy_to_volume` IC를 −0.0224로 적었다. 본 문서는 해설 문서 계보(`889c3e83…`)의 −0.0209(broad × common_survivor cum 0→20)를 쓴다. 나머지 대표 IC는 두 계보가 같다.

---

## 1. `flow_foreign_netbuy_to_volume` — 외국인 순매수 강도

| 항목 | 값 |
|---|---|
| primary feature | `flow_foreign_netbuy_to_volume_20d` (scan 정본은 `_20d_lag1`) |
| secondary / variant | `flow_foreign_netbuy_to_volume_5d`, `_60d`(secondary 등록, 이번 run 미실행 — 10 §2.6), `flow_foreign_netbuy_to_volume_20d_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_flow` / `research/etl/features/flow.py:204-207` |
| 원천 raw 테이블 | `krx_security_flow_raw`(`foreign_net_buy_volume`) + `daily_ohlcv`(분모 `volume`) + `dim_price_quality_daily`(세션 필터) |
| prod 갱신 | 매일. KRX MDC 체인 18:30(잔고 보완 때문에 유지) + KIS `investor` 그룹 주 1회 월요일 20:30(호출당 30거래일). KRX↔KIS 순매수 값 불일치 0건 (`operations.md` "KIS flows 수집", "병행 대조") |
| 분류 좌표 | C3 × T1 × U (11 §3) |
| 검증 phase / fdr_family / role | A / flow / ready |
| 기대 부호 → 관측 부호 | `+` → `−` |
| 사전등록 primary horizon | [5, 10, 20] (bucket 포함, `include_bucket_primary: true`) |
| 관측 신호 밴드 · 모양 | 없음 · `no_signal`, onset 없음, `peak_h_cum` 20(음수 방향, 관측 범위 끝), half-life 없음 (10 §4.4) |
| 등급 / screen_pass / discovery | D / 실패 / 0/6 cell (BH 통과 4/6이지만 부호 반대) |
| 모델 검증 이력 | T1·T2 미투입 (10 §9). baseline flow 15개 목록에 이 산식 없음(`model_features_performance_and_candidates` §1.2). Phase C P12 `× liq_high` A (§국면 의존) |

### 산식과 규칙

```sql
CASE WHEN COUNT(foreign_net_buy_volume) OVER w20 = 20
          AND COUNT(total_volume) OVER w20 = 20
     THEN SUM(foreign_net_buy_volume) OVER w20 / NULLIF(SUM(total_volume) OVER w20, 0) END
-- w20 = PARTITION BY ticker, market ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
```

- 창은 유효 세션 20행(ROWS). 거래정지 세션은 `sessioned` CTE에서 먼저 빠진다 (10 §2.4).
- 20행이 전부 있어야 한다(`= 20`). 부분 창 불허. 분모 합이 0이면 NULL.
- 단위는 비율. 0.05면 20일 총거래량의 5%를 외국인이 순매수했다는 뜻이다. 표준화는 없다 — 원값을 저장하고 IC 단계에서 `(trade_date, market)` 내 순위를 매긴다.
- 세 투자자(외국인·기관·개인) 순매수 합은 항등식이 아니다(기타법인 제외). "합이 0" 성질을 쓰는 피쳐는 만들지 않았다 (10 §2.7).

### 가용 시점(PIT)과 지연

- 관측 시점은 세션 t의 KRX 집계이지만 당일 사용 가능 여부가 검증되지 않아 **정본이 직전 유효 세션 값(`lag1`)**이다 (10 §2.5). `09_all_feature_results` §3은 "px/flow는 지연이 없다"고 적었는데, scan 정본은 그 가정을 쓰지 않았다.
- lag1 유지율 1.0 — 정본이 이미 `lag1`이라 자기 자신과 비교한 값이다. 당일 값 대비 하루 지연 손실은 `미측정` (10 §5.5).
- 운영 지연: KIS `investor` 그룹은 주 1회 수집이라 주중 최신 세션은 KRX 체인이 채운다. KRX가 꺼지면 최신 세션 값이 주 단위로 늦어진다 (`operations.md` "K-5 전에 처리해야 할 것").

### 예측력 — broad × common_survivor × lag1

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→5 | −0.0030 | −0.067 | −1.94 | +0.004%p | 0.070 | BH 실패, 부호 반대 |
| cum | 0→10 | −0.0076 | −0.166 | −3.45 | −0.06%p | 0.0009 | BH 통과, discovery 아님 |
| cum | 0→20 | **−0.0131** | −0.278 | −4.36 | **−0.23%p** | 2.2e-05 | BH 통과, discovery 아님 |
| bucket | 0→5 | −0.0030 | −0.067 | −1.94 | +0.004%p | 0.070 | BH 실패, 부호 반대 |
| bucket | 5→10 | −0.0089 | −0.198 | −5.63 | −0.06%p | ~0 | BH 통과, discovery 아님 |
| bucket | 10→20 | −0.0124 | −0.279 | −5.66 | −0.16%p | ~0 | BH 통과, discovery 아님 |

(10 §4.1. family 최소 q 2.25e-05.) exploratory horizon(1·2·3·40·60·120)은 `daily_ic`에 없고 해설 문서에도 없다 → `미측정`.

- IC와 q5 spread 어긋남: 0→5 cell만 IC 음수·spread 양수(+0.004%p)다. 둘 다 유의하지 않아 잡음이다. 10·20일은 방향이 같다.
- |IC|가 5→10→20일로 단조 증가하고 관측 범위 끝에서 최대다. 40일 이후는 확인하지 않았다 (10 §4.3).
- t가 −4.4로 큰 것은 표본 2,622일 때문이다. |IC| 0.013은 가격 계열 A등급(0.05~0.14)의 10분의 1이다 (10 §4.2).

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 0.707 (cum 0→20: broad −0.0131 → tradable −0.0093) | 기준 0.50 통과. 가격 A등급(0.85~1.04)보다 낮다 |
| lag1 유지율 | 1.0 (자기 비교) | 실질 `미측정` |
| available 표본 부호 | available −0.0120 vs common_survivor −0.0131, 같은 부호, `attrition_warning = false` | 뒤집히지 않음 |
| 기간 일관성 | 기대 방향 0/5 = **반대 방향 5/5** | 반대 방향이 안정적 |
| 시간 placebo | 대상 아님 — 최대 horizon 20일, NW lag 19 < 59 | 받지 않았다 |
| 비중첩 offset | 20개 전부 유효, 기대 방향 일치율 0.0, 부호 검정 p 중앙값 0.9992, offset IC −0.0166 ~ −0.0092 | 반대 방향으로 강하게 유의 |
| 지연 게이트 | `delay_pass = false` — h ≤ 5 cell(cum 0→5) `p_nw` 0.05294 > 0.05 | 실패. 수급 계열에서 실제로 작동한 사례 (10 §5.5) |
| source 경고 (Phase B) | 대상 아님 (Phase A family) | — |

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | 2,622일 |
| 날짜당 종목 수 | 1,019개 (`px_maxret_20d` 1,097개보다 78개 적다 — 완전 창 요구 때문) |
| coverage_ratio(Phase B) | 대상 아님 (Phase A) |
| KOSPI / KOSDAQ | 41.5% / 58.5% |

(10 §5.7·§6.) 원천 자체는 2007-06-05부터 있으나 scan 시작은 2014-06-01로 고정이다.

### 중복성

- A×B 상관(10 §7): `fin_value_z` −0.051(1,928일), `ev_payout_yield` −0.042, `mcap_krx_log` +0.033, `fin_log_mcap` +0.032. 전부 |ρ| < 0.06. 규모 상관 +0.033은 거래량으로 나눈 설계가 작동한 증거다.
- **미확인 중복:** `flow_inst_netbuy_to_volume`·`flow_individual_netbuy_to_volume`(거울상, A×A 없음), `flow_foreign_holding_ratio_chg`(같은 외국인을 거래 기준·잔고 기준으로 재는 두 지표, 13 §7).
- 매크로 공동 움직임(`00_existing_features_vs_macro` §4.3, 월말 Spearman): 중앙값은 breadth_20d **+0.63**, kospi_ret_20d +0.43, usdkrw_ret_20d −0.30, vix_chg_20d −0.29. IQR은 m2_yoy −0.58, us_kr_10y_diff +0.58, term spreads −0.54.

### 국면 의존 (Phase C, `05_results_stage1b_20260830.md` · `03c_conditional_ic_results.md`)

| 쌍 | 국면 | 방향 | δ̂ | t_nw | p | q | placebo p | G2 / G3 / G4 | 등급 |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| P7 | `market_up`(지난 12개월 KOSPI 상승) | `+` 사전등록 | +0.0002 | 0.03 | 0.9748 | 0.9748 | > 0.10 | ✗ / ✗ / ✗ | D |
| P11 | `vix_up`(VIX 20세션 변화 > 0) | 양방향 | −0.0027 | −0.51 | 0.6127 | 0.7070 | > 0.10 | ✅ / ✅ / ✗ | D |
| **P12** | **`liq_high`**(20일 평균 시장 거래대금 > 252세션 중앙값) | 양방향 | **+0.0163** | **2.89** | 0.0039 | **0.0193** | **0.0099** | ✅ 5/5 / ✅ 0.86 / ✅ | **A** |

- 국면 정의는 `03_stage1b_conditional_ic_phase_c` §2.3. 국내 계열 국면은 t−1 종가 정보다 (§2.2).
- P12는 관측 부호 `+`로 고정됐다. δ̂의 정의가 `mean(IC | s=1) − mean(IC | s=0)`이므로, 거래대금이 적은 국면의 IC가 많은 국면보다 0.0163 낮다(더 음수다). 국면별 IC 평균 자체는 결과 문서에 없다 → `미측정`. `liq_high` 평균 지속 52.0/66.4세션, 전환 39회.
- 05 §4의 읽기: **외국인 수급의 국면 의존은 유동성 축이지 시장 방향(P7)이나 위험선호(P11) 축이 아니다.** Kang, Kwon & Park(2014)의 "상승장에서만"은 재현되지 않았다.
- P9(`px_turnover_shock` × `liq_high`)와 국면을 공유하므로 독립된 발견으로 세지 않는다 (05 §6). 모델 입력 후보 승격은 아직 정하지 않았다 (05 §7).

### 모델 투입 메모

- **부호.** 관측 `−`가 기간 5/5·offset 20/20으로 안정적이다(`00_읽는_법` §9.6 "반대 방향이 안정적" 유형). 분류·랭킹 모델은 부호를 학습하므로 D등급이라고 버릴 정보가 아니다. 단 |IC| 0.013으로 작다.
- **변환.** 비율이라 규모는 이미 상쇄됐다. `(trade_date, market)` 내 rank가 IC 정의와 같고 안전하다. 거래가 적은 종목에서 극단값이 나올 가능성이 있어(10 §5.3 추정, 미확인) 원값을 쓰면 winsor가 필요하다.
- **결측.** 유효 세션 20행 warm-up(신규 상장·거래정지 뒤 20세션), 분모 0. 국면 마스킹은 없다. 학습 패널 규약대로 `*_isna` 플래그를 붙인다.
- **동점·0.** 외국인 거래가 없는 종목은 20일 합이 정확히 0이 되어 동점이 생길 수 있다. 빈도는 `미측정`.
- **유동성 편중.** tradable 유지율 0.707 → 신호의 약 29%가 거래가능 필터 밖 종목에서 나온다. k=100 리스트처럼 좁힐수록 불리해질 수 있다.
- **회전율.** 20일 합 비율이라 하루에 1/20씩 바뀌어 자기상관이 높고 회전율이 낮다. 정본이 `lag1`이라 하루 지연 비용은 없다(당일 대비 손실은 미측정).
- **horizon.** 5일은 신호가 거의 없다(p_nw 0.053). 20일에서 최대이고 40일 이후는 미확인이다. h20 실험에 맞고 h5에는 안 맞는다. h60·h120은 `exploratory` cell이 실행되지 않아 근거가 없다.
- **조합 후보.** `× liq_high`(Phase C A, 승격 미결정). `× usdkrw_ret_20d`(survey §4.4가 첫 후보로 지목, 미검정 — `vix_up` 쪽 P11은 실패). 기관·개인 순매수와의 축약(하나로 합치기)은 A×A 상관 뒤에 정한다. 업종 중립 변형은 업종 코드가 없어 불가 (10 §8.8).

### 한계

- 반대 부호를 discovery로 세지 않았다. 재등록 가치가 있는 후보다 (10 §8.1).
- 당일 사용 가능성을 검증하지 않아 성능을 과소평가할 수 있다 (10 §8.3).
- `_5d`·`_60d` 변형과 40일 이후 horizon을 안 봤다 (10 §8.4·§8.6).
- 세 투자자 유형 간 상관이 없다. 가장 중요한 미확인 중복이다 (10 §8.5).
- tradable 유지율 0.707의 원인을 분석하지 않았다 (10 §8.7). 업종 중립화 없음, holdout 미개봉.

---

## 2. `flow_inst_netbuy_to_volume` — 기관 순매수 강도

| 항목 | 값 |
|---|---|
| primary feature | `flow_inst_netbuy_to_volume_20d` (scan 정본은 `_20d_lag1`) |
| secondary / variant | `flow_inst_netbuy_to_volume_5d`, `_60d`(secondary 등록, 미실행 — 11 §2.3), `flow_inst_netbuy_to_volume_20d_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_flow` / `research/etl/features/flow.py:216-219` |
| 원천 raw 테이블 | `krx_security_flow_raw`(`institution_net_buy_volume`) + `daily_ohlcv` + `dim_price_quality_daily` |
| prod 갱신 | 1번과 같다. KRX↔KIS 순매수 불일치 0건 |
| 분류 좌표 | C3 × T1 × U (11 §3) |
| 검증 phase / fdr_family / role | A / flow / ready |
| 기대 부호 → 관측 부호 | `+` → `−` |
| 사전등록 primary horizon | [5, 10, 20] (bucket 포함) |
| 관측 신호 밴드 · 모양 | 없음 · `no_signal`, `peak_h_cum` 20(음수 방향, 관측 범위 끝), `peak_bucket` [5, 10], half-life 없음 (11 §4.4) |
| 등급 / screen_pass / discovery | D / 실패 / 0/6 cell (BH 통과 6/6, 전부 부호 반대) |
| 모델 검증 이력 | T1·T2 미투입 (11 §9). baseline flow 15개에 없음. Phase C P10·P13 실패 |

### 산식과 규칙

```sql
CASE WHEN COUNT(institution_net_buy_volume) OVER w20 = 20
          AND COUNT(total_volume) OVER w20 = 20
     THEN SUM(institution_net_buy_volume) OVER w20 / NULLIF(SUM(total_volume) OVER w20, 0) END
```

- 1번과 투자자 유형만 다르고 규칙이 같다(완전 창, 세션 필터, `lag1` 정본, 비율, 표준화 없음).
- "기관"은 KRX 집계 기관 범주(금융투자·투신·연기금·보험 등)의 **합계**다. 세부 유형을 나누지 않았다 (11 §2.2).

### 가용 시점(PIT)과 지연

- 1번과 같다. 정본 `lag1`, 당일 값 대비 손실 `미측정`.
- 지연 게이트는 통과했다 — h ≤ 5 cell의 `p_nw`가 0에 가깝다 (11 §5.5). 하루 늦춰도 5일 구간에서 신호가 뚜렷하다.

### 예측력 — broad × common_survivor × lag1

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→5 | −0.0141 | −0.308 | −8.72 | −0.21%p | ~0 | BH 통과, discovery 아님 |
| cum | 0→10 | −0.0177 | −0.396 | −8.24 | −0.40%p | ~0 | BH 통과, discovery 아님 |
| cum | 0→20 | **−0.0209** | −0.483 | −7.51 | **−0.68%p** | ~0 | BH 통과, discovery 아님 |
| bucket | 0→5 | −0.0141 | −0.308 | −8.72 | −0.21%p | ~0 | BH 통과, discovery 아님 |
| bucket | 5→10 | −0.0114 | −0.266 | −7.58 | −0.19%p | ~0 | BH 통과, discovery 아님 |
| bucket | 10→20 | −0.0138 | −0.324 | −6.63 | −0.27%p | ~0 | BH 통과, discovery 아님 |

(11 §4.1. family 최소 q 1.34e-13.) exploratory horizon은 `미측정`.

- IC와 q5 spread가 여섯 cell 전부 같은 방향이다. 관계가 단조롭다 (11 §4.2).
- |IC|가 5→10→20일로 단조 증가(0.014 → 0.018 → 0.021)한다. 40일 이후 미확인 (11 §4.4).
- −0.68%p는 외국인(−0.23%p)의 세 배지만 가격 A등급(1.79~2.99%p)보다 작다 (11 §4.3).

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | **1.073** (cum 0→20: broad −0.0209 → tradable −0.0224) | 유동성 좋은 종목에서 오히려 강해진다. 소형주 착시가 아니다 (11 §5.3) |
| lag1 유지율 | 1.0 (자기 비교) | 실질 `미측정` |
| available 표본 부호 | available −0.0198 vs −0.0209, 같은 부호, `attrition_warning = false` | 뒤집히지 않음 |
| 기간 일관성 | 기대 방향 0/5 = **반대 방향 5/5** | 반대 방향 완벽 일관 |
| 시간 placebo | 대상 아님 — NW lag 19 < 59 | 받지 않았다 |
| 비중첩 offset | 20개 전부 유효, 일치율 0.0, p 중앙값 0.99996, p 최솟값 0.9992, offset IC −0.0248 ~ −0.0191 | 20개 전부 반대 방향으로 강하게 유의 |
| 지연 게이트 | `delay_pass = true` | 통과 |
| source 경고 (Phase B) | 대상 아님 | — |

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | 2,622일 |
| 날짜당 종목 수 | 1,019개 |
| coverage_ratio(Phase B) | 대상 아님 |
| KOSPI / KOSDAQ | 41.5% / 58.5% |

(11 §5.7·§6. 1번과 같은 표본.)

### 중복성

- A×B 상관(11 §7): `own_major_filing_activity` −0.035, `own_insider_filing_activity` −0.019, `mcap_krx_log` −0.017, `fin_value_z` −0.014. **35개 중 가장 작다.** 규모와도 무관하다.
- **미확인 중복:** 개인(+0.0241)·외국인(−0.0131)·외국인 지분율 변화(−0.0080)와의 A×A 상관이 없다. "하나의 발견을 네 번 세고 있을 가능성"을 배제하지 못한다 (11 §7).
- 매크로 공동 움직임(survey §4.3): IQR이 kospi_turnover_log **−0.72**, cpi_yoy −0.50, vix −0.43. 거래가 활발할수록 기관 순매수 비율의 횡단면이 좁아진다. 중앙값 행은 |ρ| 0.40 이상이 없어 표에 없다.

### 국면 의존 (Phase C)

| 쌍 | 국면 | 방향 | δ̂ | t_nw | p | q | placebo p | G2 / G3 / G4 | 등급 |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| P10 | `market_up` | 양방향 | +0.0062 | 1.14 | 0.2535 | 0.3481 | > 0.10 | ✅ / ✅ / ✗ | D |
| P13 | `liq_high` | 양방향 | −0.0075 | −1.42 | 0.1563 | 0.3481 | > 0.10 | ✅ / ✅ / ✗ | D |

- survey §4.3의 IQR ρ −0.72가 IC 차이로는 이어지지 않았다. 외국인(P12)만 `liq_high`에서 통과했다.

### 모델 투입 메모

- **부호.** 관측 `−`가 기간 5/5·offset 20/20·tradable 1.073으로 수급 계열 중 가장 안정적이다. 모델에서는 그대로 쓸 수 있는 정보다. 재등록 가치가 가장 높은 후보로 지목됐다 (11 §8.2).
- **변환.** 1번과 같다. `(trade_date, market)` 내 rank. 규모 상관 −0.017로 통제가 필요 없다.
- **결측.** 완전 창 warm-up 20세션, 분모 0. `*_isna` 플래그.
- **동점·0.** 기관 거래가 없는 소형주의 20일 합 0 동점 가능. 빈도 `미측정`.
- **유동성.** tradable에서 강해지므로 좁힌 리스트에서도 신호가 남을 가능성이 크다. 다만 모델 단위로는 확인된 바 없다.
- **회전율.** 1번과 같다(20일 합, 낮은 회전율, lag1 정본).
- **horizon.** 5일부터 유의하고 20일에서 최대다. h5·h20 실험 모두에 맞다. 40일 이후 미확인.
- **조합 후보.** 기관 세부 유형 분해(연기금 vs 증권사 자기매매 — 부호가 갈릴 가능성, 11 §8.3)는 원천에 세부 컬럼이 없어 지금은 불가. 개인·외국인과의 축약. 국면 조합은 P10·P13이 실패했으므로 근거가 없다.

### 한계

- 네 개의 발견이 아니라 하나일 수 있다 (11 §8.1).
- 반대 부호를 discovery로 세지 않았다 (11 §8.2).
- 기관 세부 유형을 나누지 않았다 (11 §8.3). 당일 사용 가능성 미검증, `_5d`·`_60d` 미실행, 40일 이후 미확인.
- 업종 중립화 없음, holdout 미개봉.

---

## 3. `flow_individual_netbuy_to_volume` — 개인 순매수 강도

| 항목 | 값 |
|---|---|
| primary feature | `flow_individual_netbuy_to_volume_20d` (scan 정본은 `_20d_lag1`) |
| secondary / variant | `flow_individual_netbuy_to_volume_5d`(secondary 등록, 미실행 — 12 §2.2; T1 후보 묶음에는 포함), `flow_individual_netbuy_to_volume_20d_lag1`(lag1). `_60d`는 마트에만 있고 미등록 |
| 마트 / 산식 위치 | `feat_flow` / `research/etl/features/flow.py:228-231` |
| 원천 raw 테이블 | `krx_security_flow_raw`(`individual_net_buy_volume`) + `daily_ohlcv` + `dim_price_quality_daily` |
| prod 갱신 | 1번과 같다. KRX↔KIS 순매수 불일치 0건 |
| 분류 좌표 | C3 × T1 × U (11 §3) |
| 검증 phase / fdr_family / role | A / flow / ready |
| 기대 부호 → 관측 부호 | **미고정(`expected_sign: null`)** → `+` |
| 사전등록 primary horizon | [5, 10, 20] (bucket 포함) |
| 관측 신호 밴드 · 모양 | **5~20일** · `delayed`, onset 5, `peak_h_cum` 20(관측 범위 끝), `peak_bucket` [10, 20], half-life 없음, 부호 뒤집힘 없음 (12 §4.4) |
| 등급 / screen_pass / discovery | **A** / 통과 / **6/6 cell** |
| 모델 검증 이력 | **T1 후보(비채택)** — `_5d`·`_20d`가 후보 5개 중 2개. walk-forward 증분 확인, k=100 비용 반영 h20 Δ −0.0045로 묶음 비채택 (12 §9, `07` §6.1). Phase C P8·P14 실패 |

### 산식과 규칙

```sql
CASE WHEN COUNT(individual_net_buy_volume) OVER w20 = 20
          AND COUNT(total_volume) OVER w20 = 20
     THEN SUM(individual_net_buy_volume) OVER w20 / NULLIF(SUM(total_volume) OVER w20, 0) END
```

- 1·2번과 투자자 유형만 다르다. 완전 창, 세션 필터, `lag1` 정본, 비율, 표준화 없음.
- **방향 게이트를 적용하지 않았다.** `expected_sign_pass`가 `<NA>`이고, 기간 일관성은 관측 부호 기준으로 잰다 (`horizon_scan_runner.py:936`, 12 §3.3). 양측 검정을 쓰고 나머지 게이트(BH·기간·tradable·지연·offset)는 그대로다 (12 §3.4).

### 가용 시점(PIT)과 지연

- 1번과 같다. 정본 `lag1`, 당일 값 대비 손실 `미측정`.
- 지연 게이트 통과 — h ≤ 5 cell `p_nw` ≈ 0 (12 §5.5).

### 예측력 — broad × common_survivor × lag1

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→5 | +0.0116 | 0.255 | 7.22 | +0.18%p | ~0 | **discovery** |
| cum | 0→10 | +0.0179 | 0.386 | 7.87 | +0.37%p | ~0 | **discovery** |
| cum | 0→20 | **+0.0241** | 0.498 | 7.56 | **+0.70%p** | ~0 | **discovery** |
| bucket | 0→5 | +0.0116 | 0.255 | 7.22 | +0.18%p | ~0 | **discovery** |
| bucket | 5→10 | +0.0136 | 0.303 | 8.43 | +0.19%p | ~0 | **discovery** |
| bucket | 10→20 | +0.0173 | 0.380 | 7.60 | +0.32%p | ~0 | **discovery** |

(12 §4.1. family 최소 q Phase A 1.07e-12, 결합 AB 1.54e-16.) 양방향이라 aligned = raw. exploratory horizon은 `미측정`.

- IC와 q5 spread가 여섯 cell 전부 같은 방향이다 (12 §4.2).
- 20일 +0.70%p는 기관 −0.68%p와 거의 대칭이다. 거울상 관계의 정황이다 (12 §4.3).
- 사전등록 밴드 5~20일과 관측 밴드 [5, 20]이 정확히 맞았다 (12 §3.5). 모양은 `delayed` — 즉시 반응이 아니라 5~20일에 걸쳐 커진다. 가격 압력 가설의 모양(당일 최대, 이후 되돌림)이 아니다 (12 §4.4).

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 1.009 (cum 0→5: broad 0.0116 → tradable 0.0117; cum 0→20은 0.0241 → 0.0233) | 통과. 소형주 통념과 달리 유동성 좋은 종목에서도 같다 (12 §5.3) |
| lag1 유지율 | 1.0 (자기 비교) | 실질 `미측정` |
| available 표본 부호 | available 0.0109 vs 0.0116 (cum 0→5), 같은 부호, `attrition_warning = false` | 뒤집히지 않음 |
| 기간 일관성 | 5/5 (관측 부호 `+` 기준) | 통과 |
| 시간 placebo | 대상 아님 — NW lag 19 < 59 | **받지 않았다.** A등급이지만 `robustness_required` 대상이 아니었다 |
| 비중첩 offset | **5개**(전부 유효), 일치율 1.0, p 중앙값 5.9e-05, p 최댓값 5.2e-04, offset IC +0.0107 ~ +0.0126 | 통과. 대표 cell horizon이 5일이라 offset이 5개뿐 — 검정 강도가 약하다 (12 §5.2) |
| 지연 게이트 | `delay_pass = true` | 통과 |
| source 경고 (Phase B) | 대상 아님 | — |

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | 2,622일 |
| 날짜당 종목 수 | 1,019개 (`tradable` 기준 874개) |
| coverage_ratio(Phase B) | 대상 아님 |
| KOSPI / KOSDAQ | 41.5% / 58.5% |

(12 §5.7·§6.)

### 중복성

- A×B 상관(12 §7): `own_major_filing_activity` +0.025, `fin_value_z` +0.019, `mcap_krx_log` −0.015, `fin_gross_profitability` −0.015. 전부 |ρ| < 0.03.
- **미확인 중복 — 이 family의 핵심 문제.** 기관(−0.0209)·외국인(−0.0131)과 거의 거울상이다(합은 항등식이 아니다). A×A 상관이 없어 개인 `+`가 독립 정보인지 기계적 뒷면인지, 결합 BH에서 셋을 별도 가설로 센 것이 보정을 느슨하게 했는지 판단할 수 없다 (12 §7).
- 매크로 공동 움직임(survey §4.3): 중앙값은 breadth_20d **−0.53**(외국인의 거울상). IQR은 kospi_turnover_log −0.65, m2_yoy −0.48, us_kr_10y_diff +0.48.

### 국면 의존 (Phase C)

| 쌍 | 국면 | 방향 | δ̂ | t_nw | p | q | placebo p | G2 / G3 / G4 | 등급 |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| P8 | `market_up` | `−` 사전등록(P7의 거울상) | −0.0103 | −1.58 | 0.1136 | 0.3409 | > 0.10 | ✅ / ✅ / ✗ | D |
| P14 | `liq_high` | 양방향 | −0.0075 | −1.23 | 0.2191 | 0.3481 | > 0.10 | ✅ / ✅ / ✗ | D |

- P8은 방향은 맞았지만 BH·placebo를 넘지 못했다. 외국인 쪽 P12가 `liq_high`에서 통과한 것과 달리 개인 쪽은 유동성 국면 의존이 확인되지 않았다.

### 모델 투입 메모

- **T1 이력을 먼저 본다.** 후보 묶음(baseline 40 + `px_reversal_5d`, `px_maxret_20d`, `px_idio_vol_60d`, 이 family `_5d`·`_20d` = 45개)의 walk-forward Rank IC는 h5 0.1155→0.1202, h20 0.1436→0.1521, h60 0.1753→0.1840으로 올랐다. 비용 반영 spread 차이는 +0.0009 / **−0.0025** / +0.0004, k=100 리스트 h20은 baseline 0.0200 → candidate 0.0155(Δ −0.0045)로 배포 horizon에서 비채택이다 (`grade_a_acceptance_gate_results.md`, `topk_cost_check.md`, `00_status` §3). **개별 기여도는 측정하지 않았다** — 5개 묶음의 결과다 (12 §9).
- **변환.** `(trade_date, market)` 내 rank. 원값 winsor도 가능하나 IC 정의와 맞추려면 rank가 낫다.
- **결측.** 완전 창 warm-up 20세션, 분모 0. `*_isna`. 국면 마스킹 없음.
- **동점·0.** 개인 순매수는 거의 모든 종목에서 0이 아니므로 동점 문제는 1·2번보다 작을 것으로 보이지만 `미측정`.
- **유동성·회전율.** tradable 1.009라 좁힌 universe에서도 순위 신호는 남는다. 그런데 T1 k=100에서 비용 반영 개선이 사라졌다 — 순위 신호와 실매수 리스트 경제성은 다르다. 20일 합 비율이라 회전율은 낮고, `lag1` 정본이라 하루 지연 비용은 없다.
- **horizon.** onset 5, peak 20. h5·h20에 맞다. h60은 T1 k=100에서 유일하게 개선(+0.0092)이었지만 3개 중 1개라 단독 근거로 쓰지 않는다 (`07` §6.1). 40일 이후 되돌림 여부는 미확인 (12 §8.4).
- **조합 후보.** 기관·외국인과 하나로 축약(예: 개인 − 기관 순매수 비율)은 A×A 상관 없이 정하지 않는다. 메커니즘 구분 검정(가격 압력 vs 유동성 공급 vs 추세 추종, 12 §3.2)이 없어 어떤 조건에서 부호가 바뀌는지 모른다. 국면 조합은 P8·P14가 실패했다.

### 한계

- 세 유형이 하나의 발견일 수 있다 (12 §8.1).
- 크기가 작다(20일 +0.70%p, 비용 차감 전). offset 5개로 검정 강도가 약하다 (12 §8.2·§8.3).
- 40일 이후를 안 봤다. 네 갈래 가설 중 어느 것인지 모른다 (12 §8.4·§8.5).
- `_5d` secondary 미실행, 당일 사용 가능성 미검증, 업종 중립화 없음, holdout 미개봉.

---

## 4. `flow_foreign_holding_ratio_chg` — 외국인 지분율 변화

| 항목 | 값 |
|---|---|
| primary feature | `flow_foreign_holding_ratio_chg_20d` (scan 정본은 `_20d_lag1`) |
| secondary / variant | secondary 없음 — 이 family만 `features`가 primary 하나다. `_5d`·`_60d`는 마트에만 있다 (13 §2.5). `flow_foreign_holding_ratio_chg_20d_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_flow` / `research/etl/features/flow.py:240-243` |
| 원천 raw 테이블 | `krx_security_flow_raw`(`foreign_holding_shares`) + `dim_stock_pit_daily`(`float_shares_pit`, DART 지분 공시 기반) + `dim_price_quality_daily` |
| prod 갱신 | `foreign_holding_shares`: KIS `foreign_holding` 매일 19:00(현재값 스냅샷 1건, 과거 조회 불가 — 놓친 날은 영구 손실) + KRX 체인 18:30. KRX↔KIS 477/2,763(17.3%) 양방향 불일치, 스냅샷 정의 차이, 교체일 계단 (`operations.md` "병행 대조"). `float_shares_pit`: 공시 접수 다음 거래일부터 반영 |
| 분류 좌표 | C3 × T1 × U (11 §3) |
| 검증 phase / fdr_family / role | A / flow / ready |
| 기대 부호 → 관측 부호 | `+` → `−` |
| 사전등록 primary horizon | **[20, 40, 60]** (bucket 포함) — 수급 4개 중 유일하게 긴 밴드 (13 §3.3) |
| 관측 신호 밴드 · 모양 | 없음 · `no_signal`, `peak_h_cum` 20(음수 방향, 밴드 하한), `peak_bucket` [40, 60], half-life 없음 (13 §4.4) |
| 등급 / screen_pass / discovery | D / 실패 / 0/6 cell (BH 통과 3/6, 부호 반대) |
| 모델 검증 이력 | T1·T2 미투입 (13 §9). baseline에는 레벨 차분 `flow_foreign_holding_chg_5d/_20d`가 있다(분모 없음 — 다른 산식). Phase C 대상 아님 |

### 산식과 규칙

```sql
foreign_holding_shares / NULLIF(float_shares_pit, 0)
  - LAG(foreign_holding_shares / NULLIF(float_shares_pit, 0), 20) OVER w
-- w = PARTITION BY ticker, market ORDER BY trade_date  (유효 세션만)
```

- 단위는 비율의 차이. 0.01이면 지분율이 1%p 올랐다 (13 §2.1).
- 완전 창 조건(`COUNT = N`)이 없다. `LAG(…, 20)`이 NULL이거나 어느 한쪽 유통주식수가 없으면 NULL이다.
- **분자와 분모가 둘 다 움직인다.** 증자로 유통주식수가 늘면 외국인이 아무것도 안 해도 지분율이 떨어져 "팔았다"로 읽힌다. 기업행동 마스킹이 없다 (13 §2.4).
- 표준화 없음. 1번(거래 흐름)과 달리 보유 잔고(stock)의 이동을 잰다 (13 §2.2).

### 가용 시점(PIT)과 지연

- 정본 `lag1`. 당일 값 대비 손실 `미측정`. 지연 게이트는 최소 horizon이 20일이라 대상이 아니다(`delay_pass = null`, 13 §5.6).
- `float_shares_pit` PIT 규칙(`shares` 블록): 공시일 `rcept_no_yyyymmdd`, 가용 `next_krx_session`, 기말 `stlm_dt`, fallback lag 연 90일·분기 45일 (13 §2.3).
- 운영: KIS `foreign_holding`은 최신 거래일이 아닌 날짜로는 수집하지 않는다. 학습 표본(~2025-02)은 KRX 값이다.

### 예측력 — broad × common_survivor × lag1

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | **−0.0080** | −0.176 | −2.73 | −0.03%p | 0.010 | BH 통과, discovery 아님 |
| cum | 0→40 | −0.0063 | −0.144 | −1.95 | +0.09%p | 0.070 | BH 통과, discovery 아님 |
| cum | 0→60 | −0.0042 | −0.093 | −1.34 | +0.29%p | 0.221 | BH 실패 |
| bucket | 10→20 | −0.0079 | −0.180 | −3.59 | −0.04%p | 0.0006 | BH 통과, discovery 아님 |
| bucket | 20→40 | −0.0030 | −0.075 | −1.18 | +0.12%p | 0.283 | BH 실패 |
| bucket | 40→60 | −0.0002 | −0.006 | −0.09 | +0.16%p | 0.940 | BH 실패 |

(13 §4.1. family 최소 q 0.0100 — 수급 4개 중 가장 약하다.) exploratory horizon(5·10일 등)은 `미측정`.

- **IC와 q5 spread가 여섯 cell 중 넷에서 어긋난다**(cum 0→40·0→60, bucket 20→40·40→60: IC 음수, spread 양수). 어느 값도 유의하지 않아 잡음 안의 흔들림과 구분되지 않는다 (13 §4.3). 20일 cell은 둘 다 음수다.
- |IC|가 20 → 40 → 60일로 단조 감소한다. "긴 시야 신호"라고 보고 20~60일에 걸었는데 밴드의 가장 짧은 끝에서만 무언가 있다 (13 §4.2).

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | **0.617** (cum 0→20: broad −0.0080 → tradable −0.0049) | 기준 0.50에 가깝다. 수급 계열 최저 (13 §5.4) |
| lag1 유지율 | 1.0 (자기 비교) | 실질 `미측정` |
| available 표본 부호 | available −0.0071 vs −0.0080, 같은 부호, `attrition_warning = false`, card에 `survival_bias_unresolved` 한계 표기 | 뒤집히지 않음 |
| 기간 일관성 | **2/5** — 두 구간 양수, 세 구간 음수 | 방향 불안정. 반대 부호를 결론으로 쓸 수 없다 (13 §5.1) |
| 시간 placebo | **실패** — `p_temporal_nw` 0.347 > 0.10 | 수급 4개 중 유일하게 받았고(60일 cell, NW lag 59) 떨어졌다 (13 §5.2) |
| 비중첩 offset | 20개 전부 유효, 기대 방향 일치율 0.0, p 중앙값 0.948, **p 최솟값 0.677**, offset IC −0.0123 ~ −0.0039 | 반대 방향이 확실한 게 아니라 방향이 불분명 (13 §5.3) |
| 지연 게이트 | 대상 아님 (h ≤ 5 cell 없음) | — |
| source 경고 (Phase B) | 대상 아님 | — |

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | **2015-05-18** ~ 2025-02-05 (다른 수급 셋은 2014-06-02) |
| 유효 거래일 | **2,370일** |
| 날짜당 종목 수 | **919개** (다른 셋보다 100개 적다 — `dim_stock_pit_daily` 커버리지) |
| coverage_ratio(Phase B) | 대상 아님 |
| KOSPI / KOSDAQ | **37.3% / 62.7%** — 35개 중 KOSDAQ 비중 최고. PIT 유통주식수 커버리지 때문일 가능성, 미확인 (13 §5.7) |

(13 §2.3·§6.) 다른 수급 family와 같은 기간·같은 종목을 보고 있지 않다.

### 중복성

- A×B 상관(13 §7): `mcap_krx_log` +0.024, `fin_value_z` −0.018, `fin_accruals_to_assets` −0.016, `ev_payout_yield` −0.016. 전부 |ρ| < 0.03.
- **미확인 중복:** `flow_foreign_netbuy_to_volume`(같은 외국인을 거래 기준·잔고 기준으로 재는 두 지표. 이론상 순매수 누적 ≈ 잔고 변화지만 장외·발행·전환으로 갈린다, 13 §2.2·§7). `flow_nat_proxy_20d`의 재료 `nat_ahf_20`은 이 산식에 조건 하나(`LAG(short_balance_is_available, 20)`)를 더한 것이다 (17 §2.4).
- 매크로 공동 움직임(survey §4.3): 중앙값이 breadth_20d +0.55, kospi_ret_20d +0.39, usdkrw_ret_20d −0.35 — 1번과 같은 축. IQR 행은 없다.

### 국면 의존

Phase C 쌍에 들지 않았다. 절 생략.

### 모델 투입 메모

- **부호를 정보로 쓰기 어렵다.** 관측 `−`는 기간 2/5·placebo 실패·offset p 최솟값 0.677로 불안정하다(`00_읽는_법` §9.6 "방향이 불안정" 유형). 1·2번과 달리 반대 부호를 학습 신호로 기대할 근거가 없다.
- **변환.** 비율 차이라 척도는 작다(±0.05 수준, 17 §2.3). `(trade_date, market)` 내 rank. 증자 종목에서 큰 음수 극단값이 나올 수 있어 원값 사용 시 winsor 필수.
- **결측.** `float_shares_pit` 없음(2015-05 이전, PIT 커버리지 밖 종목), `LAG 20` warm-up, 유통주식수 0. 다른 수급 셋보다 종목당 결측이 많다. `*_isna` 플래그.
- **기업행동 오염.** 유상·무상증자 구간은 외국인 매매와 무관한 값이 된다. `ca_count_*`(가격 마트)로 마스킹하는 변형은 아직 없다 — 모델 투입 전 만들 가치가 있다.
- **유동성·시장 편중.** tradable 유지율 0.617, KOSDAQ 62.7%. 좁힌 universe에서 신호의 38%가 사라진다.
- **회전율.** 20일 차분이라 회전율은 낮다. 지연 비용 없음(lag1 정본).
- **horizon.** 20일에서만 무언가 있고 40·60일은 0에 수렴한다. 사전등록 밴드(20~60)의 하한이 신호 위치다. h60·h120 실험에는 근거가 없고, 5·10일은 exploratory로 미실행이다.
- **조합 후보.** 1번(외국인 순매수)과의 상관 → 둘 중 하나로 축약. `flow_nat_proxy_20d` 재료로서의 기여 분리(17 §4.2). 기업행동 마스킹 변형.

### 한계

- 반대 부호를 결론으로 쓸 수 없다 — 기간 2/5, placebo 실패, offset 흐릿함 (13 §8.1).
- 기업행동 처리가 없다. 이 family가 가장 취약한 지점이다 (13 §8.2).
- 표본이 다른 수급 family와 다르다(시작 1년 늦고 종목 100개 적음) (13 §8.3).
- 외국인 순매수와의 상관 없음, 사전등록 밴드와 신호 위치 어긋남, KOSDAQ 쏠림 원인 미확인, secondary 미등록 (13 §8.4~§8.7).

---

## 5. `flow_short_turnover` — 공매도 거래 비중

**등급 C는 판정 보류다(T1의 C).** role `exploratory_short_regime`, `fdr_include: false`라 BH·discovery·screen_pass를 계산하지 않았다. 강건성 실패가 아니다. 2020-03 공매도 금지로 표본이 2020-03-24에서 잘려 다중검정 모집단에 넣으면 다른 family의 문턱을 왜곡하기 때문에 애초에 후보 풀에 넣지 않았다 (14 §3.4, `09` §6). 아래 예측력 숫자는 **진단값**이다.

| 항목 | 값 |
|---|---|
| primary feature | `flow_short_turnover_20d` (scan 정본은 `_20d_lag1`) |
| secondary / variant | secondary 없음. `flow_short_turnover_20d_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_flow` / `research/etl/features/flow.py:247-251`; 제도 구간 `research/etl/quality.py:26` |
| 원천 raw 테이블 | `krx_security_flow_raw`(`short_selling_volume`) + `daily_ohlcv` + `dim_price_quality_daily`(`short_regime`) |
| prod 갱신 | `short_selling_volume`: KIS `shorting` 그룹 주 1회 월요일 20:30(호출당 100거래일) + KRX 체인 18:30. KRX↔KIS 210/2,763(7.6%) 불일치, KIS가 항상 낮고 총량 −1.19%(삼성전자 −6%, KT −18%) — 정의 차이, 교체일 계단 (`operations.md` "병행 대조") |
| 분류 좌표 | C3 × T1 × U (11 §3; 해설 문서 14 §3.4는 T0로 적었다 — 좌표 표기가 갈린다) |
| 검증 phase / fdr_family / role | A / flow / **exploratory_short_regime** |
| 기대 부호 → 관측 부호 | `−` → `−` (일치) |
| 사전등록 primary horizon | **[5, 10, 20, 40, 60]** (bucket 포함) — 공매도 4개 중 가장 넓다. 120은 exploratory |
| 관측 신호 밴드 · 모양 | `null` · `exploratory_only`(후보 밴드를 만들지 않는다), `peak_h_cum` 60(관측 범위 끝) (14 §4.4) |
| 등급 / screen_pass / discovery | C(보류) / 대상 아님 / 대상 아님 |
| 모델 검증 이력 | 미투입 (14 §9). baseline에는 레벨 passthrough `flow_short_selling_volume`·`_value`·`flow_short_avg_price`가 있다(다른 산식) |

### 산식과 규칙

```sql
CASE WHEN short_regime = 'allowed'
          AND COUNT(total_volume) OVER w20 = 20
          AND COUNT(short_selling_volume) OVER w20 = 20
     THEN SUM(short_selling_volume) OVER w20 / NULLIF(SUM(total_volume) OVER w20, 0) END
```

- `short_regime = 'allowed'`가 아니면 NULL. 제도 구간(`quality.py:26`): `allowed` 2014-06-01~2020-03-13 → `banned` 2020-03-16~2021-05-02 → `partial` 2021-05-03~2023-11-03(대형주만, 횡단면 비교 불성립이라 제외) → `banned` 2023-11-06~2025-03-28 → `allowed` 2025-03-31~ (14 §2.2).
- 완전 창 20행. 제도 전환 직후 며칠은 창이 못 차서 추가로 빠진다 (14 §2.3).
- 단위 비율(0.05 = 거래의 5%가 공매도). 규모 상쇄. 표준화 없음.
- 거래 체결량이라 잔고 계열(6~8)과 달리 공표 지연 처리가 없다 (14 §2.5).

### 가용 시점(PIT)과 지연

- 정본 `lag1`. 당일 값 대비 손실 `미측정`. 지연 게이트는 통과했다(`delay_pass = true`, h ≤ 5 cell `p_nw` < 0.05) — 공매도 4개 중 유일하게 받았다 (14 §5.5).
- `readiness_dependencies: [feat_flow, label_scan]` — 잔고 계열보다 짧다. 그 덕에 표본이 1,420일로 나머지 셋(889~909일)보다 길다.
- 운영: KIS `shorting`은 주 1회. 교체 뒤 값은 KRX 대비 −1.19% 낮은 정의다.

### 예측력 — broad × common_survivor × lag1 (진단값)

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→5 | −0.0151 | −0.184 | −3.85 | +0.42%p | — | 대상 아님 |
| cum | 0→10 | −0.0222 | −0.274 | −4.04 | +0.82%p | — | 대상 아님 |
| cum | 0→20 | −0.0348 | −0.427 | −4.45 | +1.58%p | — | 대상 아님 |
| cum | 0→40 | −0.0526 | −0.630 | −4.59 | +3.03%p | — | 대상 아님 |
| cum | 0→60 | **−0.0668** | −0.765 | −4.54 | **+4.40%p** | — | 대상 아님 |
| bucket | 0→5 | −0.0151 | −0.184 | −3.85 | +0.42%p | — | 대상 아님 |
| bucket | 5→10 | −0.0130 | −0.159 | −3.33 | +0.40%p | — | 대상 아님 |
| bucket | 10→20 | −0.0200 | −0.246 | −3.62 | +0.78%p | — | 대상 아님 |
| bucket | 20→40 | −0.0341 | −0.429 | −4.45 | +1.51%p | — | 대상 아님 |
| bucket | 40→60 | −0.0325 | −0.406 | −4.19 | +1.51%p | — | 대상 아님 |

(14 §4.1. aligned = raw × (−1). q는 계산되지 않았다.)

- IC와 spread(정렬)가 전 cell에서 같은 방향이다.
- 60일 +4.40%p는 같은 horizon의 `px_idio_vol_60d`(+2.99%p)보다 크다. **그래서 더 조심해야 한다** — 공매도가 정상 허용된 2014~2020년 구간만의 값이다 (14 §4.2).
- |IC|가 5→60일로 단조 증가(0.015 → 0.067), 관측 범위 끝에서 최대. 120일 미확인.

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | **0.571** (cum 0→60: broad −0.0668 → tradable −0.0381) | **35개 family 중 최저.** 기준 0.50을 겨우 넘는다. 공매도 활발 종목이 거래가능 universe와 어긋난다 (14 §5.3) |
| lag1 유지율 | 1.0 (자기 비교) | 실질 `미측정` |
| available 표본 부호 | available −0.0645 vs −0.0668, 같은 부호 | 뒤집히지 않음 |
| 기간 일관성 | **2/2** — `2014_2016`·`2017_2019`만 유효 | 일치했지만 두 구간뿐. 5/5와 같은 무게로 읽으면 안 된다 (14 §5.1) |
| 시간 placebo | 대상 아님 — exploratory 역할은 placebo 대상 목록에 없다 | 받지 않았다 |
| 비중첩 offset | 60개 전부 유효, 일치율 **1.0**, `offset_status = complete` | 공매도 4개 중 유일하게 완결 (14 §5.2) |
| 지연 게이트 | `delay_pass = true` | 통과 |
| source 경고 (Phase B) | 대상 아님 | — |

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | **2014-06-03 ~ 2020-03-24** |
| 유효 거래일 | **1,420일** (scan 전체 표본의 절반 남짓) |
| 날짜당 종목 수 | 814개 |
| coverage_ratio(Phase B) | 대상 아님 |
| KOSPI / KOSDAQ | **46.7% / 53.3%** — 35개 중 KOSPI 비중 최고. 공매도의 대형주 집중으로 보이나 미확인 (14 §5.7) |

(14 §6.) 2025-03-31 재개 이후는 `common_survivor` 표본이 2025-02-05에서 끝나 잡히지 않는다.

### 중복성

- A×B 상관 산출물에 없다(exploratory라 A primary 12 family에서 제외) → `미측정`.
- **미확인 중복:** `flow_short_interest`(같은 공매도 활동의 flow/stock 두 측면), `flow_nat_proxy_20d`(잔고 비율을 재료로 포함), 규모·유동성 축(`px_amihud_20d`·`fin_log_mcap` — 공매도가 대형주에 몰린다면 얽힌다) (14 §7).
- 매크로 공동 움직임: survey §4.3은 공매도 4개를 표본(월말 50~74개, 2016~2020 한 국면) 때문에 표에서 뺐다 → `대상 아님`.

### 국면 의존

Phase C 쌍에 들지 않았다. 절 생략.

### 모델 투입 메모

- **판정 보류를 그대로 물려받는다.** 지금 숫자가 좋아 보여도 채택 근거로 쓰지 않는다 (14 §9). 재개(2025-03-31) 이후 구간이 쌓이면 새 config로 재등록한다.
- **결측 구조가 학습 패널을 지배한다.** `allowed`가 아닌 2020-03-16~2025-03-28 전체가 NULL이다. 2015~2026 학습 패널에서 5년 이상이 통째로 빈다. `*_isna` 플래그로는 "제도 때문에 없음"과 "종목 때문에 없음"을 구분하지 못한다 — 국면 플래그(`short_regime`)를 따로 넣거나 `allowed` 하위표본으로만 학습하는 두 갈래가 있고, 어느 쪽도 검증되지 않았다.
- **분포 이동.** 2025-03-31 재개 구간의 제도(대상 종목·규제)가 2014~2020과 같다는 확인이 없다. `partial` 구간은 산식이 통째로 NULL 처리해 대형주 정보도 버린다 (14 §8.5).
- **변환.** 비율, `(trade_date, market)` 내 rank. 공매도가 0인 종목(특히 KOSDAQ 소형주)이 많으면 하위 분위에 동점 덩어리가 생긴다. 빈도 `미측정`.
- **유동성 편중.** tradable 유지율 0.571. 신호의 43%가 거래가능 필터 밖에서 나온다. 좁힌 리스트에서는 절반 가까이 사라질 수 있다.
- **회전율.** 20일 합 비율, 낮은 회전율. lag1 정본.
- **horizon.** 5일부터 유의하고 60일에서 최대다. h60에 가장 맞고 h20도 쓸 수 있다. 120일 미확인.
- **조합 후보.** 잔고 계열과의 상관 → 축약 여부. 규모 통제 증분 IC. 재개 이후 `allowed` 구간의 재검정이 선행 조건이다.

### 한계

- 표본이 2020-03-24에서 끝난다. 이후 5년 데이터가 없다 (14 §8.1).
- 기간 검정 2구간, tradable 유지율 0.571 최저, BH·q 없음 (14 §8.2~§8.4).
- `partial` 구간을 통째로 뺐고 2025-03-31 재개 이후를 못 봤다 (14 §8.5·§8.6).
- 공매도 묶음 내부 중복과 규모와의 관계가 미확인이다 (14 §8.7·§8.8).

---

## 6. `flow_short_interest` — 공매도 잔고 비율

**등급 C는 판정 보류다(T1의 C).** 5번과 같은 이유로 후보 풀에 넣지 않았다. 강건성 실패가 아니다 (15 §3.4). 아래 예측력 숫자는 **진단값**이다.

| 항목 | 값 |
|---|---|
| primary feature | `flow_short_interest_ratio` (scan 정본은 `flow_short_interest_ratio_lag1`) |
| secondary / variant | `flow_short_interest_ratio_chg_20d`(secondary 등록, 미실행 — 수준과 변화 중 어느 쪽이 신호인지 미확인, 15 §2.6), `flow_short_interest_ratio_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_flow` / `research/etl/features/flow.py:252-254`; 공표 지연 `research/etl/quality.py:112` |
| 원천 raw 테이블 | `krx_security_flow_raw`(`short_selling_balance_quantity`) + `dim_stock_pit_daily`(`float_shares_pit`) + `dim_price_quality_daily`(`short_regime`, `short_balance_is_available`) |
| prod 갱신 | **KRX 전용.** KIS에 endpoint가 없어 `short_selling_balance_quantity`는 KRX 체인 18:30만 채운다. KRX가 꺼지면 이 metric만 멈추고 `shorting` freshness 그룹이 매일 stale로 뜬다 (`operations.md` "K-5 전에 처리해야 할 것") |
| 분류 좌표 | C3 × T0 × U (11 §3) |
| 검증 phase / fdr_family / role | A / flow / **exploratory_short_regime** |
| 기대 부호 → 관측 부호 | `−` → `−` (일치) |
| 사전등록 primary horizon | [20, 40, 60] (bucket 포함). 잔고는 며칠 단위로 잘 안 변해 짧은 horizon은 exploratory (15 §3.3) |
| 관측 신호 밴드 · 모양 | `null` · `exploratory_only`, `peak_h_cum` 60(관측 범위 끝) (15 §4.4) |
| 등급 / screen_pass / discovery | C(보류) / 대상 아님 / 대상 아님 |
| 모델 검증 이력 | 미투입 (15 §9). baseline에는 레벨 `flow_short_balance_qty`·차분 `flow_short_balance_chg_20d`가 있다(분모·국면 마스킹·공표 지연 처리 없는 다른 산식) |

### 산식과 규칙

```sql
CASE WHEN short_regime = 'allowed' AND short_balance_is_available
     THEN short_selling_balance_quantity / NULLIF(float_shares_pit, 0) END
```

- 시점 잔액(stock)이다. 창이 없다. 0.02면 유통주식의 2%가 공매도된 채 남아 있다 (15 §2.1·§2.2).
- `short_balance_is_available`: 원천이 2016-06-30부터 시작하고, 거기에 **2거래일 공표 지연**(`quality.short_balance_lag_sessions: 2`)을 더한 날부터 참이다. 그 전은 NULL (15 §2.3).
- `short_regime = 'allowed'` 조건은 5번과 같다.
- 분모가 PIT 유통주식수라 4번과 같은 취약점이 있다 — 증자로 유통주식수가 늘면 잔고가 그대로여도 비율이 떨어진다. 기업행동 마스킹 없음 (15 §2.4).
- 표준화 없음.

### 가용 시점(PIT)과 지연

- **잔고에는 2거래일 공표 지연이 있다.** `short_selling_balance_quantity`의 키는 KRX `RPT_DUTY_OCCR_DD`(보고의무 발생일 = 측정일)이고, 자본시장법 시행령 제14조의3이 보고를 발생일 + 2영업일까지로 정한다. 같은 fetch에서 잔고 `max(trade_date)`가 거래량 계열보다 일정 세션 뒤처지는 것을 실측했다(2026-08-19 대 2026-08-21, `research/output/horizon_scan/short_balance_publication_lag.json`, `status: verified`). 측정일 값을 그날 아는 것으로 두면 look-ahead다 (15 §2.3).
- 정본 `lag1`. 따라서 **실제 정보 지연은 최소 사흘**(공표 2일 + lag1 1일)이다 (15 §5.5). 지연 게이트는 h ≤ 5 cell이 없어 대상 아님(`delay_pass = null`).
- `float_shares_pit` PIT 규칙은 4번과 같다.

### 예측력 — broad × common_survivor × lag1 (진단값)

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | −0.0524 | −0.510 | −4.24 | +0.79%p | — | 대상 아님 |
| cum | 0→40 | −0.0735 | −0.683 | −3.84 | +1.75%p | — | 대상 아님 |
| cum | 0→60 | **−0.0874** | −0.775 | −3.57 | **+2.60%p** | — | 대상 아님 |
| bucket | 10→20 | −0.0331 | −0.326 | −3.80 | +0.37%p | — | 대상 아님 |
| bucket | 20→40 | −0.0456 | −0.438 | −3.64 | +0.95%p | — | 대상 아님 |
| bucket | 40→60 | −0.0393 | −0.383 | −3.20 | +0.81%p | — | 대상 아님 |

(15 §4.1.) exploratory horizon(1·2·3·5·10·120)은 `미측정`.

- 공매도 4개 중 |IC| 1위(0.0874)인데 5분위 차이는 3위(+2.60%p)다. `flow_short_turnover`는 |IC| 0.0668·spread +4.40%p. 표본이 다르다(909일 대 1,420일) (15 §4.2).
- |IC|가 20 → 60일로 단조 증가, 관측 범위 끝에서 최대. 120일 미확인.

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | **0.851** (cum 0→60: broad −0.0874 → tradable −0.0744) | 공매도 4개 중 최고. 분모가 유통주식수라 유동성 편향이 작은 것으로 보인다 (15 §5.3) |
| lag1 유지율 | 1.0 (자기 비교) | 실질 `미측정` |
| available 표본 부호 | available −0.0858 vs −0.0874, 같은 부호 | 뒤집히지 않음 |
| 기간 일관성 | **2/2** (`2017_2019` 중심, 나머지는 부분적) | 두 구간뿐 (15 §5.1) |
| 시간 placebo | 대상 아님 — exploratory | 받지 않았다 |
| 비중첩 offset | 60개, **`offset_status = insufficient`**, 일치율 `null` | 909일 표본에 60일 격자를 나누면 offset당 유효일이 `nonoverlap_min_dates: 20`에 못 미친다. `insufficient_offset_max_grade: B` 규칙 (15 §5.2) |
| 지연 게이트 | 대상 아님 | — |
| source 경고 (Phase B) | 대상 아님 | — |

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | **2016-07-01 ~ 2020-03-24** |
| 유효 거래일 | **909일** (3년 9개월) |
| 날짜당 종목 수 | 803개 |
| coverage_ratio(Phase B) | 대상 아님 |
| KOSPI / KOSDAQ | 42.5% / 57.5% |

(15 §5.7·§6.) 시작이 2016-07-01인 이유는 원천 시작(2016-06-30) + 공표 지연 2세션, 끝은 공매도 전면 금지다.

### 중복성

- A×B 상관 산출물에 없다 → `미측정`.
- **미확인 중복 — 구조적으로 확정적인 것들:** `flow_days_to_cover`와 **분자가 완전히 같다**(분모만 유통주식수 대 20일 평균 거래량). `flow_nat_proxy_20d`의 재료 `nat_asi_20`은 이 산식과 조건절까지 SQL이 동일하다 — 사실상 부분집합 관계 (15 §7, 17 §2.4). `flow_short_turnover`와는 stock/flow 관계.
- 매크로 공동 움직임: survey §4.3 표에서 제외 → `대상 아님`.

### 국면 의존

Phase C 쌍에 들지 않았다. 절 생략.

### 모델 투입 메모

- **판정 보류를 물려받는다.** 재등록 전 채택 근거로 쓰지 않는다 (15 §9).
- **결측이 세 겹이다.** ① 2016-06-30 이전 원천 없음(+2세션), ② `allowed` 아닌 구간(2020-03-16~2025-03-28) NULL, ③ `float_shares_pit` 없는 종목. 학습 패널 2015~2026 중 값이 있는 구간이 2016-07~2020-03과 2025-03-31 이후뿐이다. `*_isna` 하나로는 세 원인을 구분할 수 없다.
- **정보 지연 사흘 이상.** 실행 설계에서 formation t의 값이 t−3 측정치라는 점을 반영해야 한다 (15 §8.9). 매일 리밸런싱 모델에서 이 피쳐만 다른 시계에 있다.
- **변환.** 비율, `(trade_date, market)` 내 rank. 잔고 0 종목이 많으면 하위 동점 덩어리. 빈도 `미측정`. 증자 종목 극단값 → 원값 사용 시 winsor.
- **부호의 양면.** 사전등록은 정보 우위(`−`)를 택했지만 숏 스퀴즈 가설은 `+`다. 잔고가 극단적으로 큰 구간만 따로 보면 부호가 뒤집힐 수 있는데 검정하지 않았다 (15 §3.1·§8.7). 비선형 모델은 이를 학습할 수 있으나 표본이 909일이다.
- **유동성.** tradable 0.851 — 공매도 4개 중 실행 가능성이 가장 좋다.
- **회전율.** 잔고는 며칠 단위로 잘 안 변한다. 회전율은 낮다.
- **horizon.** 20일부터 유의, 60일에서 최대. h60에 맞다. h5·h10은 미실행, 120일 미확인.
- **조합 후보.** `flow_days_to_cover`와의 상관(둘 중 하나로 축약), `_chg_20d`(수준 vs 변화), `flow_nat_proxy_20d` 안에서의 순수 기여 분리.

### 한계

- 표본 909일, 2020-03-24 끝. offset 검정 미완결, 기간 2구간, BH 없음 (15 §8.1~§8.4).
- 잔고 계열 셋의 중복 미확인 — 특히 `flow_days_to_cover`와 분자가 같다 (15 §8.5).
- secondary(20일 변화) 미실행, 숏 스퀴즈 가설 미검정, 기업행동 처리 없음, 실제 정보 지연 사흘 이상 (15 §8.6~§8.9).

---

## 7. `flow_days_to_cover` — 공매도 잔고 청산 소요일

**등급 C는 판정 보류다(T1의 C).** 5·6번과 같은 이유로 후보 풀에 넣지 않았다. 강건성 실패가 아니다 (16 §3.4). 예측력 숫자는 **진단값**이다.

| 항목 | 값 |
|---|---|
| primary feature | `flow_days_to_cover` (scan 정본은 `flow_days_to_cover_lag1`) |
| secondary / variant | secondary 없음 — 변화율도 다른 창도 등록하지 않았다 (16 §2.5). `flow_days_to_cover_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_flow` / `research/etl/features/flow.py:259-261` |
| 원천 raw 테이블 | `krx_security_flow_raw`(`short_selling_balance_quantity`) + `daily_ohlcv`(분모 20일 평균 `volume`) + `dim_price_quality_daily` |
| prod 갱신 | 잔고는 KRX 전용(6번과 같다). 분모 거래량은 `prices backfill`(naver) 매일 |
| 분류 좌표 | C3 / **C8**(실행·거래비용) × T0 × U (11 §2.1·§3) |
| 검증 phase / fdr_family / role | A / flow / **exploratory_short_regime** |
| 기대 부호 → 관측 부호 | `−` → `−` (일치) |
| 사전등록 primary horizon | [20, 40, 60] (bucket 포함) |
| 관측 신호 밴드 · 모양 | `null` · `exploratory_only`, `peak_h_cum` 60(관측 범위 끝) (16 §4.4) |
| 등급 / screen_pass / discovery | C(보류) / 대상 아님 / 대상 아님 |
| 모델 검증 이력 | 미투입 (16 §9) |

### 산식과 규칙

```sql
CASE WHEN short_regime = 'allowed' AND short_balance_is_available
     THEN short_selling_balance_quantity / NULLIF(AVG(total_volume) OVER w20, 0) END
-- w20 = ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
```

- 단위는 **일(day)**. 3이면 평소 거래량으로 잔고를 다 되사는 데 3일 걸린다 (16 §2.1).
- 분자는 6번과 완전히 같고 분모만 20일 평균 거래량이다. 분모 창에는 완전 창 조건이 없다(`AVG`만).
- 국면·공표 지연 조건은 6번과 같다. `float_shares_pit`가 필요 없어 `readiness_dependencies`에 `dim_stock_pit_daily`가 없고, 그 덕에 날짜당 종목이 6번보다 53개 많다 (16 §2.3).
- **분모가 거래량이라 유동성 축과 얽힌다.** `px_amihud_20d`가 겪은 "거래량 지표가 규모를 대리하는 현상"이 나타날 수 있다. 그래서 C8에도 속한다 (16 §2.2). 표준화 없음.

### 가용 시점(PIT)과 지연

- 6번과 같다. 잔고 공표 지연 2세션 + `lag1` = **실제 정보 지연 사흘 이상** (16 §2.4). 지연 게이트 대상 아님(`delay_pass = null`).

### 예측력 — broad × common_survivor × lag1 (진단값)

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | −0.0355 | −0.398 | −3.23 | +1.12%p | — | 대상 아님 |
| cum | 0→40 | −0.0535 | −0.548 | −3.09 | +2.30%p | — | 대상 아님 |
| cum | 0→60 | **−0.0672** | −0.652 | −3.04 | **+3.46%p** | — | 대상 아님 |
| bucket | 10→20 | −0.0217 | −0.253 | −2.92 | +0.62%p | — | 대상 아님 |
| bucket | 20→40 | −0.0338 | −0.380 | −3.08 | +1.36%p | — | 대상 아님 |
| bucket | 40→60 | −0.0299 | −0.342 | −2.78 | +1.32%p | — | 대상 아님 |

(16 §4.1.) exploratory horizon은 `미측정`.

- 6번과 표본이 완전히 같은데(909일) |IC|는 3위(0.0672 < 0.0874), 5분위 차이는 2위(+3.46%p > +2.60%p)다. 분자가 같으므로 차이는 전적으로 분모에서 온다. "유동성 대비 부담이 더 중요한 정보"인지 "유동성 프리미엄 혼입"인지 이 자료로는 못 가른다 (16 §4.2).
- 공매도 4개 중 |t_nw|가 가장 작다(2.78~3.23). 일별 IC 변동이 크다 (16 §4.3).

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | **0.684** (cum 0→60: broad −0.0672 → tradable −0.0460) | 공매도 4개 중 두 번째로 낮다. 분모가 거래량인 둘(5번 0.571, 7번 0.684)이 나란히 낮고 유통주식수인 둘(6번 0.851, 8번 0.847)은 0.85 수준 — 유동성 혼입 정황 (16 §5.3) |
| lag1 유지율 | 1.0 (자기 비교) | 실질 `미측정` |
| available 표본 부호 | available −0.0644 vs −0.0672, 같은 부호 | 뒤집히지 않음 |
| 기간 일관성 | 2/2 | 두 구간뿐 |
| 시간 placebo | 대상 아님 — exploratory | 받지 않았다 |
| 비중첩 offset | 60개, **`insufficient`**, 일치율 `null` | 표본 909일 (16 §5.2) |
| 지연 게이트 | 대상 아님 | — |
| source 경고 (Phase B) | 대상 아님 | — |

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2016-07-01 ~ 2020-03-24 |
| 유효 거래일 | 909일 |
| 날짜당 종목 수 | **856개** (6번 803개보다 53개 많다 — PIT 유통주식수 불필요) |
| coverage_ratio(Phase B) | 대상 아님 |
| KOSPI / KOSDAQ | 45.4% / 54.6% |

(16 §5.7·§6.)

### 중복성

- A×B 상관 산출물에 없다 → `미측정`.
- **미확인 중복 — 가장 큰 공백:** `flow_short_interest`와 분자가 같다. 두 피쳐의 상관은 "유통주식수와 20일 평균 거래량이 횡단면에서 얼마나 비슷하게 움직이나"로 결정되는데 재지 않았다. 규모·유동성 축(`px_amihud_20d`·`fin_log_mcap`)과의 상관도 없다. `flow_nat_proxy_20d`와는 6번을 거쳐 간접적으로 얽힌다 (16 §7).
- 매크로 공동 움직임: survey §4.3 표에서 제외 → `대상 아님`.

### 국면 의존

Phase C 쌍에 들지 않았다. 절 생략.

### 모델 투입 메모

- **판정 보류를 물려받는다.** 재등록 전 채택 근거로 쓰지 않는다.
- **결측.** 6번과 같은 두 겹(원천 시작 + 공표 지연, 국면 NULL). `float_shares_pit`는 필요 없어 그 결측은 없다.
- **정보 지연 사흘 이상** (16 §5.5).
- **변환.** 단위가 일수라 우측 꼬리가 길다(거래가 적은 종목의 작은 잔고도 큰 값). rank 또는 log가 맞다. 원값은 폭주한다.
- **유동성 혼입.** 분모가 거래량이라 `px_amihud_20d`류와 같은 정보를 섞어 담을 수 있다. 규모 통제 증분 IC 없이는 독립 기여를 말할 수 없다 (16 §8.6). tradable 유지율 0.684.
- **부호의 양면.** 영어권에서 days to cover는 숏 스퀴즈 지표로 더 자주 쓰인다(`+`). 반대 방향 검정을 하지 않았다 (16 §3.1·§8.7).
- **horizon.** 20일부터 유의, 60일에서 최대. h60에 맞다. 120일 미확인.
- **조합 후보.** 6번과의 축약(둘 중 하나), `px_amihud_20d`·`fin_log_mcap` 통제, 변화율 변형(등록된 것이 없다).

### 한계

- 표본 909일, 2020-03-24 끝, offset 미완결, 기간 2구간, BH 없음 (16 §8.1~§8.4).
- 6번과 분자가 같은데 상관을 안 쟀다 — 가장 시급하다 (16 §8.5).
- 유동성 혼입 가능성을 배제하지 못한다. 숏 스퀴즈 가설 미검정, secondary 없음 (16 §8.6~§8.8).

---

## 8. `flow_nat_proxy_20d` — 외국인 매수·공매도 잔고 순위차 (NAT proxy)

**등급 C는 판정 보류다(T1의 C).** 공매도 잔고를 재료로 쓰기 때문에 제도 제약을 그대로 물려받아 후보 풀에 넣지 않았다. 강건성 실패가 아니다 (17 §3.4). 예측력 숫자는 **진단값**이다.

| 항목 | 값 |
|---|---|
| primary feature | `flow_nat_proxy_20d` (scan 정본은 `flow_nat_proxy_20d_lag1`) |
| secondary / variant | secondary 없음. `flow_nat_proxy_20d_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_flow` / 재료 `research/etl/features/flow.py:262-270`, 순위차 `:280-284` |
| 원천 raw 테이블 | `krx_security_flow_raw`(`foreign_holding_shares`, `short_selling_balance_quantity`) + `dim_stock_pit_daily`(`float_shares_pit`) + `dim_price_quality_daily` |
| prod 갱신 | 잔고는 KRX 전용, 외국인 보유주식수는 KIS 19:00 매일 + KRX 체인(4번 참고 — KRX↔KIS 17.3% 불일치). 두 재료의 원천 전환이 모두 걸린다 |
| 분류 좌표 | C3 × **T2** × U (11 §3; 해설 문서 17 §3.4는 T1로 적었다 — 표기가 갈린다) |
| 검증 phase / fdr_family / role | A / flow / **exploratory_short_regime** (`flow.nat_feature: flow_nat_proxy_20d`) |
| 기대 부호 → 관측 부호 | `+` → `+` (일치) |
| 사전등록 primary horizon | [20, 40, 60] (bucket 포함) |
| 관측 신호 밴드 · 모양 | `null` · `exploratory_only`, `peak_h_cum` 60(관측 범위 끝) (17 §4.4) |
| 등급 / screen_pass / discovery | C(보류) / 대상 아님 / 대상 아님 |
| 모델 검증 이력 | 미투입 (17 §9) |

### 산식과 규칙

```sql
-- 1단계: 재료
nat_ahf_20 = CASE WHEN short_regime = 'allowed' AND short_balance_is_available
                    AND LAG(short_balance_is_available, 20) OVER w
             THEN 외국인지분율(t) − 외국인지분율(t−20) END          -- 13번 산식 + 조건 1개
nat_asi_20 = CASE WHEN short_regime = 'allowed' AND short_balance_is_available
             THEN short_selling_balance_quantity / NULLIF(float_shares_pit, 0) END  -- 15번 산식과 동일
-- 2단계: 날짜×시장 안에서 백분위 순위를 빼기
PERCENT_RANK() OVER (PARTITION BY trade_date, market ORDER BY nat_ahf_20)
  - PERCENT_RANK() OVER (PARTITION BY trade_date, market ORDER BY nat_asi_20)
-- 두 재료가 모두 NOT NULL인 행만 순위 계산에 들어간다
```

- 범위 −1 ~ +1. +1이면 외국인 매수 순위 최상위·공매도 잔고 순위 최하위 (17 §2.1).
- **35개 중 유일하게 산식 안에 횡단면 순위 변환이 들어간 피쳐**다. 두 재료의 척도 차이는 상쇄되지만 크기 정보는 사라진다. 가중치는 1:1 고정 (17 §2.3·§8.9).
- 순위 그룹이 IC 계산 그룹(`trade_date, market`)과 같다.
- 조건 다섯 개(`allowed`, 잔고 가용, 20일 전 잔고 가용, `float_shares_pit`, 두 재료 NOT NULL)가 겹쳐 표본이 35개 중 가장 짧다 (17 §2.5).

### 가용 시점(PIT)과 지연

- 6번과 같다. 잔고 공표 지연 2세션 + `lag1` = **사흘 이상** (17 §2.6). 지연 게이트 대상 아님.
- `float_shares_pit` PIT 규칙은 4번과 같다.

### 예측력 — broad × common_survivor × lag1 (진단값)

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | +0.0315 | 0.397 | 3.34 | +0.72%p | — | 대상 아님 |
| cum | 0→40 | +0.0491 | 0.573 | 3.27 | +1.58%p | — | 대상 아님 |
| cum | 0→60 | **+0.0644** | 0.700 | 3.29 | **+2.53%p** | — | 대상 아님 |
| bucket | 10→20 | +0.0182 | 0.237 | 2.79 | +0.34%p | — | 대상 아님 |
| bucket | 20→40 | +0.0328 | 0.392 | 3.20 | +0.89%p | — | 대상 아님 |
| bucket | 40→60 | +0.0357 | 0.426 | 3.50 | +1.02%p | — | 대상 아님 |

(17 §4.1.) exploratory horizon은 `미측정`.

- **조합이 재료보다 나은지 답할 수 없다.** 재료 2(`flow_short_interest`, |IC| 0.0874, 909일)가 조합(0.0644, 889일)보다 강하지만 표본이 다르고, 재료 1 계열(`flow_foreign_holding_ratio_chg`, −0.0042, 2,370일)은 아예 다른 기간이다 (17 §4.2).
- bucket cell의 t가 horizon과 함께 커진다(2.79 → 3.20 → 3.50). 다른 공매도 셋은 뒤로 갈수록 줄어든다 (17 §4.3).

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | **0.847** (cum 0→60: broad +0.0644 → tradable +0.0546) | 공매도 4개 중 두 번째로 높다. 분모 유통주식수 계열 (17 §5.3) |
| lag1 유지율 | 1.0 (자기 비교) | 실질 `미측정` |
| available 표본 부호 | available +0.0631 vs +0.0644, 같은 부호 | 뒤집히지 않음 |
| 기간 일관성 | 2/2 | 두 구간뿐 |
| 시간 placebo | 대상 아님 — exploratory | 받지 않았다 |
| 비중첩 offset | 60개, **`insufficient`**, 일치율 `null` | 표본 889일 — 35개 중 최소이므로 당연한 결과 (17 §5.2) |
| 지연 게이트 | 대상 아님 | — |
| source 경고 (Phase B) | 대상 아님 | — |

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 시작~끝 | **2016-07-29 ~ 2020-03-24** (6번보다 4주 늦다 — `LAG(short_balance_is_available, 20)`) |
| 유효 거래일 | **889일 (35개 중 최소)** |
| 날짜당 종목 수 | **781개 (35개 중 최소)** |
| coverage_ratio(Phase B) | 대상 아님 |
| KOSPI / KOSDAQ | 42.9% / 57.1% |

(17 §5.7·§6.) 조합 피쳐가 치르는 대가다 — 두 재료 커버리지의 교집합만 남는다.

### 중복성

- A×B 상관 산출물에 없다 → `미측정`.
- **구조적으로 가장 심각한 미확인 중복.** `nat_asi_20`은 `flow_short_interest_ratio`와 SQL이 같다. Spearman은 단조 변환에 불변이므로 `nat_ahf_20` 항이 없다면 두 피쳐의 IC는 부호만 뒤집힌 같은 값이어야 한다. 그 항의 순수 기여가 조합의 존재 이유인데 A×A 상관이 없어 얼마인지 모른다 (17 §7). `nat_ahf_20`은 4번 산식에 조건 하나를 더한 것이다 (17 §2.4).
- 공매도 4개가 `fdr_include: false`라 이번 BH 문턱은 왜곡하지 않았다. **재등록할 때는 이 중복을 먼저 정리해야 한다.**
- 매크로 공동 움직임: survey §4.3 표에서 제외 → `대상 아님`.

### 국면 의존

Phase C 쌍에 들지 않았다. 절 생략.

### 모델 투입 메모

- **판정 보류를 물려받는다.** 여기에 §중복성의 부분집합 문제까지 더해진다 (17 §9).
- **이미 순위다.** 마트 단계에서 `PERCENT_RANK` 차이로 나오므로 `(trade_date, market)` 내 rank를 다시 걸어도 결과가 바뀌지 않는다. 원값 −1~+1을 그대로 넣어도 척도 문제가 없다. 단 크기 정보는 이미 버려졌다.
- **조합을 모델에 맡기는 대안.** 재료 둘(4번 계열, 6번)을 따로 넣으면 비선형 모델이 상호작용을 학습한다. 1:1 순위차로 고정한 조합이 그보다 나은지는 검증되지 않았다 (17 §8.8·§8.9).
- **결측.** 조건 다섯 개의 교집합. 학습 패널에서 값이 있는 구간이 2016-07-29~2020-03-24와 2025-03-31 이후(단 20일 전 잔고 가용 조건으로 4주 더 늦다)뿐이다. `*_isna` 플래그 필수.
- **정보 지연 사흘 이상** (17 §5.5).
- **동점.** 두 재료 중 하나라도 동점 덩어리(잔고 0 종목)가 있으면 순위차도 덩어리가 생긴다. 빈도 `미측정`.
- **유동성.** tradable 0.847 — 실행 가능성은 공매도 4개 중 상위.
- **horizon.** 20일부터 유의, 60일에서 최대, bucket t가 뒤로 갈수록 커진다. h60에 가장 맞다. 120일 미확인.
- **조합 후보.** 같은 표본에서 재료·조합을 나란히 재는 실험(17 §8.3)이 선행돼야 한다. 다른 가중치, 원값 가중합 변형.

### 한계

- 표본 889일로 35개 중 가장 짧고 2020-03-24에서 끝난다 (17 §8.1).
- `flow_short_interest`를 재료로 포함한다. 부분집합을 별도 family로 센 구조 — 가장 시급한 후속 작업 (17 §8.2).
- 조합이 재료보다 나은지 확인하지 못했다. offset 미완결, 기간 2구간, BH 없음 (17 §8.3~§8.6).
- 사전 문헌이 없는 신규 조합("(신규)")이라 외부 검증 기준이 없다. 순위 변환으로 크기 정보를 버렸고 가중치를 1:1로 고정했다 (17 §8.7~§8.9).

---

## Horizon Scan 미검정 피쳐

`feat_flow`에 있지만 Horizon Scan 단변량 검정을 거치지 않은 컬럼이다. 산식은 `research/etl/features/flow.py`, 뜻은 `model_features_performance_and_candidates.md` §1.2를 따른다. `w5`/`w20`은 `ROWS BETWEEN 4/19 PRECEDING AND CURRENT ROW`, `w`는 `PARTITION BY ticker, market ORDER BY trade_date`다. baseline 40 = `feat_price` px 15 + `feat_flow` flow 15 + `feat_fin_pit` fin 10.

| 컬럼 | 뜻(산식 요약) | 마트 | 모델 사용 | 검증 상태 | 메모 |
|---|---|---|---|---|---|
| `flow_foreign_netbuy_sum_5d`, `_20d` | `SUM(foreign_net_buy_volume) OVER w5/w20` — 외국인 순매수 주식 수 누적 | `feat_flow` | baseline 40에 포함 | baseline 모델 포함(단변량 검정 없음, walk-forward Rank IC는 묶음 결과만) | 완전 창 조건 없음(부분 창 허용). 거래량으로 나누지 않아 규모와 뒤섞인다(10 §2.2). 학습 전 fold별 winsor/z 표준화 |
| `flow_inst_netbuy_sum_5d`, `_20d` | `SUM(institution_net_buy_volume) OVER w5/w20` | `feat_flow` | baseline 40에 포함 | 같음 | 같음 |
| `flow_indiv_netbuy_sum_5d`, `_20d` | `SUM(individual_net_buy_volume) OVER w5/w20` | `feat_flow` | baseline 40에 포함 | 같음 | 같음. 세 합은 항등식이 아니다(기타법인 제외) |
| `flow_foreign_holding_chg_5d`, `_20d` | `foreign_holding_shares − LAG(…, 5/20) OVER w` — 보유주식수 레벨 차분 | `feat_flow` | baseline 40에 포함 | 같음 | 4번(지분율 차분)과 다른 산식. 분모 없음이라 유통주식수 변화에는 노출되지 않지만 규모 미상쇄 |
| `flow_short_balance_chg_20d` | `short_selling_balance_quantity − LAG(…, 20) OVER w` | `feat_flow` | baseline 40에 포함 | 같음 | 2016-06-30 이전 NULL → `*_isna`. 산식에 `short_regime`·`short_balance_is_available` 조건이 없다 — 국면 마스킹·공표 지연 2세션 미반영(look-ahead 점검 필요) |
| `flow_foreign_netbuy_z_20d`, `flow_inst_netbuy_z_20d` | `(x − AVG(x) OVER w20) / NULLIF(STDDEV_SAMP(x) OVER w20, 0)` — 종목 내 시계열 z | `feat_flow` | baseline 40에 포함 | 같음 | 개인 z는 없다. 20행 미만 warm-up에서 표준편차 불안정 |
| `flow_short_avg_price` | `short_selling_value / NULLIF(short_selling_volume, 0)` — 공매도 평균가(원) | `feat_flow` | baseline 40에 포함 | 같음 | 가격 레벨. 종가 대비 비율이 아니라 종목 간 비교 의미가 약하다. 국면 마스킹 없음 |
| `flow_short_selling_volume`, `flow_short_selling_value`, `flow_short_balance_qty` | 원천 값 passthrough(레벨) | `feat_flow` | baseline 40에 포함 | 같음 | "패널 단계 비율 계산용 passthrough"(§1.2). 규모 미상쇄. 잔고는 2016-06-30 이전 NULL |
| `flow_foreign_holding_ratio` | `foreign_holding_shares / NULLIF(float_shares_pit, 0)` — 지분율 수준 | `feat_flow` | 미투입 | 미검정 | `02` F4 "외국인 보유 level 단독 비권장"(11 §5.6). 4번의 수준형 |
| `flow_foreign_holding_ratio_chg_5d`, `_60d` | 4번 산식의 5·60일 창 | `feat_flow` | 미투입 | 미검정(사전등록 없음, 13 §2.5) | 마트에만 있다 |
| `flow_individual_netbuy_to_volume_60d` | 3번 산식의 60일 창 | `feat_flow` | 미투입 | 미검정(미등록, 12 §2.2) | T1 후보 묶음에서도 미등록이라 제외(`07` §1) |
| `flow_foreign_netbuy_to_volume_5d`, `_60d` · `flow_inst_netbuy_to_volume_5d`, `_60d` · `flow_individual_netbuy_to_volume_5d` · `flow_short_interest_ratio_chg_20d` | 각 family의 다른 창 / 20일 변화 | `feat_flow` | 개인 `_5d`만 T1 후보(비채택), 나머지 미투입 | secondary 등록, 이번 run 미실행 | 카드 1·2·3·6 참고 |
| `*_lag1` (비율 17개 컬럼 각각) | `LAG(c) OVER w` — 직전 유효 세션 값 | `feat_flow` | 미투입(`07` §1: 자동 제외) | scan 정본 variant | 모델에서 당일 값을 쓰려면 카드의 "당일 사용 가능 여부 미검증"을 먼저 푼다 |
| `short_balance_is_available`, `short_regime` | 잔고 가용 플래그, 제도 구간 | `feat_flow` | 미투입 | 보조 컬럼(패널 계산용) | 5~8번 NULL의 원인 구분에 쓸 수 있다 |
| `nat_ahf_20`, `nat_asi_20` | 8번 재료 | (중간 CTE, 출력 안 됨) | — | 보조 컬럼 | 마트 출력에 없다. 8번 재료 분리 실험을 하려면 출력에 추가해야 한다 |
