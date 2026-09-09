# 04. 재무·규모·밸류·인적자본 — 모델 개발용 피쳐 카드

- 작성일: 2026-09-03
- 대상: `fin_log_mcap` · `mcap_krx_log` · `fin_value_z` · `fin_gross_profitability` ·
  `fin_asset_growth_yoy` · `fin_accruals_to_assets` · `fin_sue` · `hc_employee_growth` ·
  `hc_productivity`
- 기준 run: Phase A `20260827T221729-4e0ae8b0` / Phase B `20260828T123313-4e0ae8b0` / AB `20260828T165038-4e0ae8b0`
  (snapshot `2026-08-23`, config `889c3e83…`). 매크로·Phase C는 `236d0d35…` 계보(`20260830T…-efd35e70`, `20260830T122850-phasec`).
- 항목 정의와 읽는 법은 [00_reading_guide.md](00_reading_guide.md)를 먼저 본다.
- 원문 해설: `docs/dev/20260829_raw_features_explain/` (22~30번)

이 그룹은 **전부 Phase B(T2)**다. 세 가지를 먼저 알아야 숫자가 제대로 읽힌다.

1. **T2의 C는 "판정 보류"가 아니다.** 강건성(시간 placebo·비중첩 offset) 또는 available 표본
   방향 게이트에서 떨어졌다는 뜻이다 (`09_all_feature_results.md` §2, `00_읽는_법.md` §4.5).
   `fin_sue`의 C만 예외다 — 계산 자체를 못 해서 기본값으로 떨어진 등급이다 (22 §6.2).
2. **B는 통계가 아니라 원천 품질 때문이다.** Phase B "source 비치명 경고"는 세 지표로 잰다 —
   `revision_ratio` ≥ 0.10, `mapping_fallback_ratio` ≥ 0.50, `value_mismatch_ratio`
   (`pairing_mismatch_ratio`) ≥ 0.01 중 하나라도 걸리면 `warn`이고 등급 상한이 B다
   (`08_phase_b_implementation_log.md`, `horizon_scan_phase_b_source_quality.py`).
   N6 확장분(`hc_*`)은 원천이 최종본만 주어 사전등록에서 상한 B를 미리 박았다.
3. **시간 placebo는 NW lag ≥ 59인 cell만 받는다.** `bucket 40→60`(lag 19)과 `cum 0→20/0→40`은
   `robustness_required=false`라 검사를 **받지 않았다**. 이 그룹에서 A·B를 받은 cell 대부분이
   그 경우다. 실제로 받고 통과한 것은 `fin_log_mcap` 3 cell(p 0.0099)과 `mcap_krx_log`
   `cum 0→60`(p 0.0891) 둘뿐이다 (00 §9.5).

PIT 규칙도 Phase A와 다르다. 값은 **접수일 다음 KRX 세션부터** 노출되고(`available_from`),
분기·연간 공시 주기를 따라 **계단식**으로 바뀐다. 그래서 family마다 유효 시작일이 다르고
(2014-06 ~ 2017-02), 같은 값이 수십~수백 세션 이어진다 (09 §3).

---

## 0. 그룹 한눈에 보기

대표 cell은 원문 해설 §1이 든 peak cell(대부분 `cum 0→120`)이다. q5 spread는 기대 부호로 정렬한 값이다.

| # | family | primary feature | 무엇을 재나 | 기대→관측 부호 | 등급 | screen_pass | 대표 cell IC | 대표 q5 spread | 모델 사용 | 한 줄 메모 |
|---|---|---|---|---|---|---|---:|---:|---|---|
| 1 | `fin_log_mcap` | `fin_log_mcap` | ln(종가 × DART PIT 주식수) | `−` → `−` | A4 | 4/4 | −0.1149 (cum 0→120) | +11.83%p | T2 후보 14(validation 개선, holdout 대기) | Phase B 최고. 시간 placebo 3 cell 실제 통과(0.0099). `px_amihud_20d` ρ −0.721 |
| 2 | `mcap_krx_log` | `mcap_krx_log` | ln(KRX 발표 시총) | `−` → `−` | A2/C2 | 2/4 | −0.0929 (cum 0→120) | +11.19%p | T2 후보 14(validation 개선, holdout 대기) | 커버리지 0.999인데 tradable 유지율 0.56~0.69 최저. 긴 cell placebo 실패 |
| 3 | `fin_value_z` | `fin_value_z` | B/M·E/P·CFO/P·S/P z-score 평균 | `+` → `+` | B1/C3 | 1/4 | +0.1220 (cum 0→120) | +4.52%p | T2 후보 14(validation 개선, holdout 대기) | Phase B \|IC\| 최대. 긴 cell placebo 전부 실패. S/P 94.2% 매핑 대체 |
| 4 | `fin_gross_profitability` | `fin_gross_profitability` | 매출총이익 / 평균총자산 | `+` → `+` | B5/C3 | 5/8 | +0.0360 (cum 0→120) | +0.66%p | T2 후보 14(validation 개선, holdout 대기) | 94.4%가 역산값. 짧은 cell만 B(placebo 미수행) |
| 5 | `fin_asset_growth_yoy` | `fin_asset_growth_yoy` | 총자산 YoY | `−` → `−`(≈0) | C4 | 0/4 | −0.00438 (cum 0→120) | +1.27%p | 미투입 | 신호 없음(\|IC\| 35개 중 최소). 유지율 0.11~14.5 폭주 |
| 6 | `fin_accruals_to_assets` | `fin_accruals_to_assets` | (순이익 − CFO) / 평균총자산 | `−` → `+` | C3/D1 | 0/4 | +0.0180 (cum 0→120) | +0.10%p (raw −0.10%p) | 미투입 | 부호 반대이고 그 반대도 불안정(기간 0~1/4) |
| 7 | `fin_sue` | `fin_sue` | 표준화 실적 서프라이즈(event-time) | `+` → 없음 | C(insufficient) | —/6 | — | — | 미투입 | 표본 0. 8분기 이력 요구 + XBRL 백필 미완 |
| 8 | `hc_employee_growth` | `hc_employee_growth_yoy` | 직원 수 YoY | 없음 → `+` | B1/C3 | 1/4 | +0.0157 (cum 0→120) | +0.48%p | T2 후보 14(validation 개선, holdout 대기) | q 0.093으로 문턱 겨우. 상한 B(final_vintage). placebo 전부 실패 |
| 9 | `hc_productivity` | `hc_revenue_per_employee` | ln(연매출 / 직원 수) | 없음 → `+` | B1/C3 | 1/4 | +0.0181 (cum 0→120) | −0.80%p | T2 후보 14(validation 개선, holdout 대기) | 네 cell 전부 IC와 q5 부호 반대. 상한 B. vintage 나이 35개 중 최장 |

세 마트가 이 그룹을 만든다.

| 마트 | family | PIT 규칙 | 갱신 주기 |
|---|---|---|---|
| `feat_fin_scan_daily` (`fin_v4`) | 1·3·4·5·6 | `fin_quarterly_metric_vintage` strict PIT — 접수 다음 KRX 세션부터, 같은 날 후보는 최신 회계기간 우선(I12), fs_basis는 `net_income` 기준 하루 하나 | 분기 공시(계단). 시총 요소만 매일 |
| `feat_market_cap` (`market_cap_v2`) | 2 | KRX 일별 발표값, T+1 공표. 무상증자 권리락~신주상장 창은 마스킹 | 매일(`sdc_daily_market_cap` 평일 20:00) |
| `feat_periodic_extras` (`periodic_extras_v2`) | 8·9 | DS002 최종본. `*_available_from` = 접수 다음 세션, 증가율·생산성은 두 입력 중 늦은 쪽 | 연 1회(사업보고서) |
| `fin_sue_event` | 7 | event-time. `event_formation_date` = 원본 접수의 `available_from` | 분기 실적 발표 건마다 |

T2 후보 14 묶음의 validation 결과는 여섯 family가 같은 숫자를 공유한다 (`phase_b_acceptance_gate_results.md`).

| horizon | baseline Rank IC → 후보 | Rank IC Δ | 비용 반영 spread (baseline → 후보) | spread Δ |
|---|---:|---:|---:|---:|
| 5 | 0.1155 → 0.1186 | +0.0031 | −0.0002 → 0.0015 | +0.0017 |
| 20 | 0.1436 → 0.1447 | +0.0011 | 0.0126 → 0.0155 | +0.0030 |
| 60 | 0.1753 → 0.1755 | +0.0003 | 0.0204 → 0.0283 | +0.0080 |

14개를 한 번에 넣은 결과(raw 40 → 54, design 80 → 108)라 **개별 기여도는 없다.** 최종 holdout은 h60 라벨이 성숙하는 2026년 10~11월에 한 번만 연다.

---

## 1. `fin_log_mcap` — 로그 시가총액 (DART PIT 주식수)

| 항목 | 값 |
|---|---|
| primary feature | `fin_log_mcap` |
| secondary / variant | `fin_log_mcap_lag1`(lag1). secondary 없음 |
| 마트 / 산식 위치 | `feat_fin_scan_daily` / `research/etl/features/fin_scan.py:267` (산식), `:244` (`base_ok`) |
| 원천 raw 테이블 | `daily_ohlcv`(종가), `dart_share_count_raw` → `dim_stock_pit_daily`(`market_cap_pit`) |
| prod 갱신 | 종가는 매일(`sdc_daily_pykrx_prices` 체인). 주식수는 DART 정기보고서 접수 시(`sdc_daily_opendart_share_info` 체인, 04:00) |
| 분류 좌표 | C2 × T0 × U (11 §3 #10) |
| 검증 phase / fdr_family / role | B / financial / ready |
| 기대 부호 → 관측 부호 | `−` → `−` |
| 사전등록 primary horizon | [60, 120], bucket 포함(`include_bucket_primary: true`) → cell 4개. exploratory [20, 40] |
| 관측 신호 밴드 · 모양 | 60~120일. 누적 \|IC\| 60일 0.087 → 120일 0.115, 구간 40~60 0.041 → 60~120 0.068. **관측 범위 끝에서 최대**, peak `cum 0→120` (26 §4.4). onset·half-life는 미측정 |
| 등급 / screen_pass / discovery | A 4개 / 4/4 통과 / 4/4 cell. `failed_gates` 비어 있음 |
| 모델 검증 이력 | T2 후보 14 → `improved_all_horizons`(§0 표). `mcap_krx_log`와 함께 들어가 개별 몫 미측정. holdout 대기 |

### 산식과 규칙

```sql
-- fin_scan.py:244, :267
(market_cap_pit IS NOT NULL AND market_cap_pit > 0
 AND shares_is_available AND NOT shares_invalid_flag
 AND NOT COALESCE(is_halted, TRUE) AND valid_session_idx IS NOT NULL) AS base_ok
CASE WHEN base_ok THEN ln(market_cap_pit) END AS fin_log_mcap
```

- `market_cap_pit` = 당일 종가 × 그 시점에 알 수 있었던 DART 발행주식수. 자연로그.
- 창·warm-up 없음. `base_ok` 다섯 조건 중 하나라도 어긋나면 NULL. 같은 마트 다섯 family가 이 게이트를 공유한다.
- 횡단면 표준화 없음(로그만). 단위는 ln(원).
- vintage 나이 `mean_age_days`/`p95_age_days`가 **NaN** — 분기 재무를 쓰지 않는 유일한 재무 family다 (26 §2.3).

### 가용 시점(PIT)과 지연

- 종가는 당일. 주식수는 `shares` 규칙 — `disclosed_date_source: rcept_no_yyyymmdd`, `availability: next_krx_session`, `fallback_lag_days: {annual: 90, quarterly: 45}` (`horizon_scan_config.yaml`).
- **주식수가 다음 정기보고서 공시까지 안 움직인다.** 유상증자 뒤 가격은 즉시 반응하는데 주식수는 몇 달 뒤에 반영된다 — 분자·분모 시점 어긋남 (28 §2.2). `shares_age_days`는 계산되지만 어디서도 읽지 않는다.
- lag1 유지율: 미측정 — 원문 해설 26에 `lag1_ic` 수치가 없다.

### 예측력 — broad × common_survivor × native_t

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | −0.0868 | −0.947 | −7.02 | +6.29%p | ~0 | A · screen-pass |
| cum | 0→120 | **−0.1149** | −1.105 | −5.84 | **+11.83%p** | ~0 | A · screen-pass |
| bucket | 40→60 | −0.0413 | −0.475 | −6.30 | +2.03%p | ~0 | A · screen-pass |
| bucket | 60→120 | −0.0679 | −0.738 | −5.63 | +5.59%p | ~0 | A · screen-pass |

(26 §4.1) IC와 q5 spread가 같은 방향이고 크기도 맞는다. +11.83%p는 35개 중 **두 번째**(1위 `px_amihud_20d` +11.21%p와 사실상 같은 급, 3위 `mcap_krx_log` +11.19%p) — 상위 셋이 전부 규모 축이다 (26 §4.2). exploratory [20, 40] 결과는 원문에 없다.

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 0.816 / 0.847 / 0.788 / 0.849 (cum 60 / cum 120 / b40-60 / b60-120) | 통과(기준 0.50). **Phase B A등급 중 유일하게 1 미만** |
| lag1 유지율 | 미측정 | — |
| available 표본 부호 | 4/4 일치 | 통과 |
| 기간 일관성 | 5/5 (4 cell 전부) | 통과. 5구간을 채운 Phase B family 중 하나 |
| 시간 placebo | cum 0→60 **0.0099** / cum 0→120 **0.0099** / bucket 60→120 **0.0099** — 셋 다 최솟값. bucket 40→60은 대상 아님(NW lag 19) | **실제로 받고 통과.** Phase B에서 유일 |
| 비중첩 offset | 3 cell `complete`, pass | 통과 |
| source 경고 (Phase B) | `not_applicable` (분기 vintage 미사용), grade_cap None | 경고 없음 → A |

(26 §5) tradable 유지율이 1 미만인 것은 결함이 아니라 이 축의 성격이다 — 소형주 효과 지표에 유동성 필터를 걸면 신호가 줄어드는 게 정상이다. `px_amihud_20d`(0.852)와 같은 수준.

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | 2015-03-17 ~ 2025-02-05 |
| 유효 거래일 | 2,392일 |
| 날짜당 종목 수 | 999~1,002개 |
| coverage_ratio | 0.779 (KOSDAQ 0.828 / KOSPI 0.703) |
| 관측 행 수 | 5,616,215 |

(26 §6) 같은 마트의 분기 재무 family(0.58~0.63, 2016~2017 시작)보다 길고 넓다. PIT 주식수만 있으면 되기 때문이다.

### 중복성

- **`px_amihud_20d` × `fin_log_mcap` 평균 순위상관 −0.721** (유효일 2,392, 범위 −0.79 ~ −0.55). 이번 검증에서 `|ρ| ≥ 0.7`을 넘은 두 쌍 중 하나 (26 §7). Amihud 분모가 거래대금이고 거래대금은 시총에 비례한다. 규모가 근본인지 비유동성이 근본인지 **규모 통제 증분 IC가 없어 못 가른다.**
- `mcap_krx_log`와의 **직접 상관 미측정** — B×B 산출물이 없다. 네 cell의 q5 spread가 0.1%p 안에서 일치한다(28 §4.2)는 게 유일한 간접 증거다.
- 같은 규모 축과 얽힌 T2 형제: `ev_payout_yield` −0.35, `fin_value_z` +0.20 (`px_amihud_20d` 기준, 28 §9).
- `feat_fin_scan_daily` 형제 네 개(3~6번)와의 상관 전부 미확인.

### 국면 의존 (Phase C)

Phase C에서 **reference 쌍(R)**으로 쓰였다 — 규모 축에 국면 의존이 없어야 나머지 결과를 믿을 수 있다는 대조군이다 (`05_results_stage1b_20260830.md` §5).

| 쌍 id | 국면 | δ̂ | t_nw | p | placebo p | 판정 |
|---|---|---:|---:|---:|---:|---|
| X1 | `market_up` | +0.0200 | 0.93 | 0.353 | 0.772 | R — 국면 의존 없음 |
| X2 | `kosdaq_rel_up` | −0.0049 | −0.42 | 0.677 | 0.663 | R — 국면 의존 없음 |

둘 다 `|t_nw| < 2`. **규모효과는 시장 방향·KOSDAQ 상대 강세 국면과 무관하다.** `cf_small_growth_regime × (−fin_log_mcap)` 같은 국면 상호작용(02 §231)은 이 결과상 근거가 없다.

### 모델 투입 메모

- **변환**: 이미 로그. Rank IC는 순위만 쓰므로 로그 여부가 IC를 바꾸지 않는다(26 §2.1). 모델 입력은 fold별 winsor/z(baseline 파이프라인) 또는 `(date, market)` 내 rank. 두 시장의 시총 분포가 달라 시장별 표준화가 맞다.
- **결측**: `base_ok` 실패(주식수 미가용·무효 플래그·거래정지·비유효 세션). 커버리지 0.779, KOSPI 쪽(0.703)이 더 비어 있다 — 대형 시장이 더 비는 비대칭이라 `isna` 지시자를 함께 넣어야 결측이 "소형"으로 읽히지 않는다. 생존편향은 available 표본 부호 4/4로 통과.
- **유의점**: 하위 분위가 초소형주라 거래비용·시장충격이 가장 크다. 5분위 차이 +11.83%p는 비용 차감 전이고 자기 가격 영향도 없다(26 §4.3). DART 주식수 지연이 유상증자 직후 값을 틀리게 한다 — `mcap_krx_log`가 그 대안이다. 값이 매일 바뀌지만 순위는 느리게 움직인다(회전율 낮음).
- **horizon**: 사전등록 60/120에서 전부 통과. 관측 peak가 120(범위 끝)이라 h120 실험에서 가장 강하고, h60도 A. h5/h20은 단변량 근거가 없다(exploratory 20/40 결과 미기록) — T2 묶음의 h5 Δ +0.0031은 묶음 결과다.
- **조합 후보**: (a) `px_amihud_20d` 통제 뒤 증분 IC — 가장 시급(26 §8.1). (b) size × value 상호작용(Fama-French 계열 문헌 표준) — 미검정. (c) `mcap_krx_log`와 둘 중 하나만 넣거나 ablation. (d) 국면 조건화는 X1·X2 결과상 후순위.

### 한계

- `px_amihud_20d`와 ρ −0.72. 규모 통제 증분 IC 없이는 독립 신호라 부를 수 없다.
- `mcap_krx_log`와의 직접 상관이 없고, 둘 다 T2 묶음에 들어갔다.
- 거래비용 반영 성과 없음. tradable 유지율 0.79~0.85.
- 120일 너머를 안 봤다. peak가 관측 범위 끝.
- 1981년부터 알려진 축이다. baseline 40에 규모 지표가 이미 있는지 확인하지 않았다(26 §9). baseline `fin_pit` 10개에는 시총 컬럼이 없다(§10).
- 업종 중립화 없음. holdout 미개방.

---

## 2. `mcap_krx_log` — 로그 시가총액 (KRX 상장주식수)

| 항목 | 값 |
|---|---|
| primary feature | `mcap_krx_log` |
| secondary / variant | `mcap_krx`(원값, 감사용), `mcap_unreliable`(마스크 플래그), `mcap_krx_log_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_market_cap` / `research/etl/features/market_cap.py:71`. 마스크 `research/etl/corporate_actions.py`(`dim_mcap_distortion`) |
| 원천 raw 테이블 | `daily_market_cap`(KRX Open API `sto/stk_bydd_trd`), 마스크용 `dart_filing_receipt_raw`(권리락 접수) |
| prod 갱신 | 매일. `sdc_daily_market_cap` 평일 20:00, 최근 30일 gap scan. **원천 T+1** |
| 분류 좌표 | C2 × T0 × U |
| 검증 phase / fdr_family / role | B / financial / ready. **2026-08-27 확장 등록분**(`outcome_blind: true`) |
| 기대 부호 → 관측 부호 | `−` → `−` |
| 사전등록 primary horizon | [60, 120], bucket 포함 → cell 4개. exploratory [20, 40]. `fin_log_mcap`과 같은 격자 |
| 관측 신호 밴드 · 모양 | 60~120일. 누적 \|IC\| 60일 0.069 → 120일 0.093, 구간 0.027 → 0.048. 범위 끝 최대, peak `cum 0→120` (28 §4.4) |
| 등급 / screen_pass / discovery | A 2개 · C 2개 / 2/4 / 4/4 cell |
| 모델 검증 이력 | T2 후보 14 → `improved_all_horizons`(§0 표). `fin_log_mcap`과 함께 들어감 |

### 산식과 규칙

```sql
-- market_cap.py:71
CASE WHEN NOT mcap_unreliable AND m.market_cap > 0
     THEN ln(CAST(m.market_cap AS DOUBLE)) END AS mcap_krx_log
```

- `daily_market_cap.market_cap`(= 종가 × 상장주식수, 항등식 검증 7,060,600행 0 위반)의 자연로그.
- **마스킹, 보정 아님.** 무상증자는 권리락일에 가격이 조정되고 신주는 약 3주 뒤 상장된다. 그 사이 시총이 대략 `1/f`로 읽힌다. prod 2026-08-19 측정: **553건, 평균 23일, 최장 85일, 12,739 ticker-day = 패널의 0.18%** (`corporate_actions.py` docstring). 창 상한 `MAX_WINDOW_DAYS = 120`, 주식수 변화 임계 `SHARE_CHANGE_THRESHOLD = 0.05`. 무상증자만 대상(유상증자·전환권행사는 미측정).
- 틀린 값이 무상증자 종목(과거 수익률·소형주와 상관된 집단)에 몰리면 결측보다 나쁘다는 판단이다 (28 §2.4, `poc/n1_validation.md` §5.5).
- 정의가 다르므로 `fin_log_mcap`을 고치지 않고 **새 id**로 등록했다(N1-7 결정 1: frozen id는 정의를 지킨다).

### 가용 시점(PIT)과 지연

- KRX 상장주식수는 **상장일**에 바뀐다. DART 주식수의 몇 달 지연이 약 3주로 줄고, 그 3주는 마스킹한다.
- **T+1 공표.** 어제 장의 값이 오늘 저녁에 들어온다. 이번 검증은 `native_t`이고 표본이 2025-02-05에서 끝나 영향이 없지만, **실운용에서는 당일 값이 없다** — lag1로 써야 한다.
- lag1 유지율: 미측정.

### 예측력 — broad × common_survivor × native_t

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | −0.0692 | −0.593 | −4.61 | +6.19%p | 0.00001 | A · screen-pass |
| cum | 0→120 | **−0.0929** | −0.717 | −3.96 | **+11.19%p** | 0.00014 | C · robustness 실패 |
| bucket | 40→60 | −0.0269 | −0.264 | −3.60 | +2.07%p | 0.00057 | A · screen-pass |
| bucket | 60→120 | −0.0480 | −0.419 | −3.32 | +5.25%p | 0.00156 | C · robustness 실패 |

(28 §4.1) IC와 q5 방향 일치. `fin_log_mcap`과 나란히 놓으면 **q5 spread는 네 cell 전부 0.1%p 안에서 같고, IC는 KRX 쪽이 일관되게 작다**(0.069 대 0.087, 0.093 대 0.115). 표본 차이(2,622일/1,077종목 대 2,392일/1,000종목)인지 DART 지연 오차가 우연히 신호를 키웠는지 못 가른다 (28 §4.2).

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 0.660 / 0.685 / **0.558** / 0.632 | 통과이지만 **Phase B 18개 중 최저** |
| lag1 유지율 | 미측정 | — |
| available 표본 부호 | 4/4 일치 | 통과 |
| 기간 일관성 | 5구간 중 4 / 4 / 4 / 3 | 통과(5/5는 아님) |
| 시간 placebo | cum 0→60 **0.0891 통과**(기준 0.10, 여유 작음) / cum 0→120 **0.1683 실패** / bucket 60→120 **0.2178 실패**. bucket 40→60 대상 아님(NW lag 19) | A 2개 중 하나(cum 0→60)는 실제로 받고 통과, 하나(bucket 40→60)는 받지 않았다 |
| 비중첩 offset | 3 cell `complete`, pass | 통과. 떨어진 이유는 placebo 하나 |
| source 경고 (Phase B) | `not_applicable` | 경고 없음 |

(28 §5) 커버리지가 높을수록 유지율이 낮다 — 거래가 거의 없는 초소형주까지 값을 갖기 때문이다.

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | 2014-06-02 ~ 2025-02-05 |
| 유효 거래일 | 2,622일 |
| 날짜당 종목 수 | 1,076~1,079개 |
| coverage_ratio | **0.99862** (KOSPI 1.00000 / KOSDAQ 0.99773) |
| 관측 행 수 | 7,056,361 |

(28 §6) Phase B 18개 중 커버리지·표본·관측 수 전부 1위. KOSDAQ 결측 0.23%가 무상증자 마스킹 창이다. 마스킹을 끈 값(`distortion_view=None`)과의 대조 진단은 돌리지 않았다.

### 중복성

- **`px_amihud_20d` × `mcap_krx_log` −0.754** (2,622일, 범위 −0.85 ~ −0.63). 이번 검증 최대 |ρ|. 둘 다 `daily_market_cap` 계열 원천이라 더 강할 수 있다 — 확인하지 않았다 (28 §7).
- `fin_log_mcap`과의 직접 상관 **미측정**. 0.95 이상일 것이 거의 확실한데 숫자가 없다. **둘 다 T2 묶음에 들어가 실질 문제다.**

### 모델 투입 메모

- **변환**: 로그 완료. `fin_log_mcap`과 같은 처리(fold별 winsor/z 또는 시장 내 rank).
- **결측**: 마스킹 창(`mcap_unreliable = TRUE`) 또는 `market_cap ≤ 0`. 원값 `mcap_krx`는 감사용이다 — 모델에 넣으면 마스킹 이유가 무너진다. 결측이 무상증자 종목에 몰리므로 `isna`가 "무상증자 직후"라는 사건 정보를 갖는다. 그 자체가 피쳐가 될 수 있지만 검정된 바 없다.
- **유의점**: T+1이라 실운용은 `mcap_krx_log_lag1`이 현실 값이다. tradable 유지율 0.56~0.69 — 신호의 상당 부분이 실행 불가 종목에서 나온다. 긴 cell이 placebo에서 떨어졌다는 점에서 `fin_log_mcap`보다 강건성이 약하다.
- **horizon**: 60/120 사전등록. `cum 0→60`이 placebo까지 통과한 유일한 A라 h60이 가장 근거가 있다. h120은 C.
- **조합 후보**: `fin_log_mcap` 대체(커버리지 0.999, 지연 오차 없음) 또는 병존 ablation — 둘 중 하나는 빼야 한다. `ln(mcap_krx) − fin_log_mcap` 차이가 DART 지연 오차의 프록시가 될 수 있다는 건 아이디어일 뿐 미검정.

### 한계

- `px_amihud_20d`와 ρ −0.75, `fin_log_mcap`과 상관 미측정.
- tradable 유지율 Phase B 최저. 긴 두 cell placebo 실패, 통과한 하나도 0.089.
- `fin_log_mcap`보다 IC가 작은 이유 미판정. 3주 마스킹 창 크기 진단 미실행.
- 거래비용 미반영, 120일 너머 미관측, 업종 중립화 없음, holdout 미개방.

---

## 3. `fin_value_z` — 밸류 종합 (B/M · E/P · CFO/P · S/P)

| 항목 | 값 |
|---|---|
| primary feature | `fin_value_z` |
| secondary / variant | 구성요소 `fin_book_to_market` · `fin_earnings_yield` · `fin_cfo_yield` · `fin_sales_to_price`(Horizon Scan 미검정), 보조 `value_component_count` · `negative_equity` · `fs_basis_used` · `value_available_from` · `value_fin_age_days`, `fin_value_z_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_fin_scan_daily` / `fin_scan.py:268`(비율) · `:288`(winsorize 가드) · `:334`(z-score) · `:357`(합성) |
| 원천 raw 테이블 | `dart_financial_statement_raw`, `dart_xbrl_fact_raw`(I7 fallback) → `stock_metric_vintage_fact` → `fin_quarterly_metric_vintage`; `dart_share_count_raw`, `daily_ohlcv`(`market_cap_pit`) |
| prod 갱신 | 분기 공시(계단). `sdc_daily_opendart_corp` 04:00 → financials → share_info → xbrl 체인 |
| 분류 좌표 | C2 × T0 × U |
| 검증 phase / fdr_family / role | B / financial / ready |
| 기대 부호 → 관측 부호 | `+` → `+` |
| 사전등록 primary horizon | [60, 120], bucket 포함 → cell 4개. exploratory [20, 40] |
| 관측 신호 밴드 · 모양 | 60~120일. 누적 IC 0.098 → 0.122, 구간 0.060 → 0.086, 범위 끝 최대, peak `cum 0→120` (27 §4.4) |
| 등급 / screen_pass / discovery | B 1개 · C 3개 / 1/4 / 4/4 cell |
| 모델 검증 이력 | T2 후보 14 → `improved_all_horizons`(§0 표). 단변량 \|IC\|는 14개 중 최대 |

### 산식과 규칙

네 단계다 (27 §2.1).

```sql
-- 1) 비율 (fin_scan.py:268) — 분모 전부 market_cap_pit, base_ok 필요
total_equity_selected / market_cap_pit            AS fin_book_to_market   -- total_equity > 0 일 때만
controlling_net_income_selected / market_cap_pit  AS fin_earnings_yield
operating_cash_flow_selected / market_cap_pit     AS fin_cfo_yield
revenue_selected / market_cap_pit                 AS fin_sales_to_price
-- 2) (trade_date, market) 내 1%/99% winsorize — NULL 가드 필수 (:288)
-- 3) winsorize 값을 (trade_date, market) 내 z-score (:334)
-- 4) 합성 (:357)
CASE WHEN value_component_count >= 2 THEN
  (COALESCE(z_bm,0)+COALESCE(z_ep,0)+COALESCE(z_cfop,0)+COALESCE(z_sp,0)) / value_component_count
END AS fin_value_z
```

- 분자는 flow metric이면 TTM(`ttm_value`), instant면 그 값. fs_basis는 하루 하나(`net_income`에 CFS가 있으면 CFS, 없으면 OFS).
- **최소 2개 요소**만 있으면 만든다. 2개짜리와 4개짜리가 같은 값으로 취급된다.
- **v1 버그**: DuckDB `GREATEST/LEAST`가 NULL을 건너뛰어 재무제표가 없는 회사가 시장 1분위로 채워졌고, 발행 값의 **29.2%**가 ≥2 규칙을 어겼다. `fin_v2`에서 고쳤고 현재 `fin_v4` (`10_known_issues.md` I1). v1 시절 숫자와 지금 숫자는 다르다.
- 단위: z-score 평균(무차원). 이미 시장 내 표준화된 값이다.

### 가용 시점(PIT)과 지연

- `value_available_from = greatest(a_total_equity, a_net_income)` 기준 interval join. 정본 `native_t`.
- vintage 나이: KOSDAQ 평균 74.9일 / 95분위 179일, KOSPI 73.6일 / 159일 (27 §2.5).
- 유효 시작 2017-02-15 — 2개 이상 요소가 유효해지는 시점이 늦다(09 §3, I7 전에는 2019년 ≥2 충족률 6.8%).
- lag1 유지율: 미측정. 구조상 값이 바뀌는 날은 새 접수가 유효해지는 세션과 시총 변동일이다.

### 예측력 — broad × common_survivor × native_t

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | +0.0978 | 1.053 | 6.96 | +2.13%p | ~0 | C · robustness 실패 |
| cum | 0→120 | **+0.1220** | **1.153** | 5.41 | **+4.52%p** | ~0 | C · robustness 실패 |
| bucket | 40→60 | +0.0599 | 0.650 | 8.00 | +0.60%p | ~0 | **B · screen-pass** |
| bucket | 60→120 | +0.0865 | 0.938 | 6.29 | +2.00%p | ~0 | C · robustness 실패 |

(27 §4.1) Phase B \|IC\| 최대, 35개 전체 3위(`px_idio_vol_60d` 0.143, `px_amihud_20d` 0.134 다음). IC와 q5 방향은 일치하지만 **크기 비율이 다르다** — 120일 기준 spread/IC가 37로, `fin_log_mcap` 103·`px_amihud_20d` 84보다 낮고 `ev_payout_yield` 5보다 훨씬 높다 (27 §4.3). 순위 신호로는 규모 축과 같은 급인데 양 끝 분위 수익률 차이는 그 40% 수준이다.

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 1.017 / 1.019 / 1.002 / 1.008 | 통과. **35개 중 1에 가장 가깝다** — 유동성 필터가 신호를 안 바꾼다 |
| lag1 유지율 | 미측정 | — |
| available 표본 부호 | 4/4 일치 | 통과 |
| 기간 일관성 | 4/4 (4 cell 전부, 2017 시작이라 4구간) | 통과 |
| 시간 placebo | cum 0→60 **0.1980 실패**(NW 59) / cum 0→120 **0.3663 실패**(119) / bucket 60→120 **0.2574 실패**(59). bucket 40→60 대상 아님(19) | **받은 세 cell 전부 실패. B를 받은 하나는 받지 않았다** |
| 비중첩 offset | 3 cell `complete`, pass | 통과 |
| source 경고 (Phase B) | `warn` — `mapping_fallback_ratio` **0.9417**(`revenue`) > 0.50, `revision_ratio` **0.1014**(`total_equity`) > 0.10, `pairing_mismatch_ratio` 0.000125 < 0.01 | 두 경고 → 상한 B |

(27 §5) 기간 4/4인데 placebo에서 떨어진다 — 두 검사가 다른 것을 잡는다. B 등급은 통계가 아니라 원천 품질 상한이다.

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | 2017-02-15 ~ 2025-02-05 |
| 유효 거래일 | 1,928일 |
| 날짜당 종목 수 | 867~868개 |
| coverage_ratio | **0.564** (KOSDAQ 0.604 / KOSPI 0.503) — Phase B 재무 계열 최저 |
| 관측 행 수 | 4,068,096 |

### 중복성

A×B 교차 상관 (27 §7). 204쌍 중 절대값 3위·5위가 이 family 것이다.

| 상대 | ρ | 범위 | 뜻 |
|---|---:|---|---|
| `px_idio_vol_60d` | **−0.351** | −0.49 ~ +0.02 | 싼 회사는 고유변동성이 낮다. 두 기대 부호(`+`/`−`)가 같은 방향으로 작동 |
| `px_maxret_20d` | **−0.251** | −0.44 ~ −0.06 | 같은 구조 |
| `px_amihud_20d` | +0.203 | −0.13 ~ +0.39 | 싼 회사가 비유동적 = 작다 |
| `px_near_52w_high` | +0.167 | −0.11 ~ +0.40 | |

미확인: `fin_gross_profitability`(quality-value 가설의 전제인 음의 상관, B×B 없음), `ev_payout_yield`(분모 시총 공유), 네 요소 간 상관, `hc_productivity`(S/P와 연매출 분자 공유, 30 §7).

**업종 중립 진단(`feat_fin_scan_daily_ind`, N2 V6)**: 같은 산식을 `(trade_date, market, industry_group)`에서 다시 만들면 일별 횡단면 rank correlation 중앙값 **0.8305**(p10 0.7704, p90 0.8859), 업종 중앙값 사이 표준편차 중앙값 **0.2832 → 0.0958(−65.4%)**. plain 값의 업종 중앙값은 그룹 `70` −0.6564 ~ `30` +0.3095까지 벌어진다 — **plain `fin_value_z`는 종목의 상대 밸류뿐 아니라 업종의 구조적 밸류 수준도 읽고 있다** (`poc/n2_validation.md` V6). 다만 이 변형은 **현재 업종 코드를 과거에 소급한 look-ahead**라 진단 전용이고, scored backtest·acceptance gate·holdout에 **금지**다.

### 모델 투입 메모

- **변환**: 이미 `(date, market)` z-score 평균이다. 추가 z는 불필요하고 rank는 가능하다. baseline 파이프라인이 fold별 winsor/z를 다시 적용하면 이중 표준화가 된다 — 해롭지는 않지만 알고 있어야 한다.
- **결측**: `value_component_count < 2` 또는 `base_ok` 실패. **43%가 빈다.** 시작이 2017-02-15라 2015~2016 패널은 전부 NULL(vintage 2년 필요). KOSPI 0.503으로 더 비어 있다. `isna`와 함께 `value_component_count`(2~4)를 별도 입력으로 넣는 게 맞다 — 2개짜리와 4개짜리는 의미가 다른데 지금은 같은 값이다(27 §8.3). 원천 결측(재무제표 없음)은 부실·신규상장과 상관될 수 있어 `isna` 자체가 정보를 갖는다.
- **유의점**: S/P 분자(`revenue`)의 94.2%가 매핑 대체 경로, 자본총계 10.1%가 사후 정정(최종본 vintage 사용). 업종 노출이 가장 큰 축 중 하나(은행 B/M 대 소프트웨어 B/M). 분기 계단이라 회전율이 낮고 tradable 유지율 ≈1 — **실행 조건은 규모 축보다 유리**하다.
- **horizon**: 사전등록 60/120. 순위 신호는 120에서 최대이지만 B는 `bucket 40→60`뿐이고 그것도 placebo 미수행. h60/h120 모델에서 기여를 기대할 수 있으나 **시간축 강건성이 확인되지 않았다** — walk-forward에서 연도별 IC(`daily_ic.parquet`)를 꼭 봐야 한다. h5/h20 근거 없음.
- **조합 후보**: quality × value(`fin_gross_profitability`와의 음의 상관이 전제인데 미측정), size × value, 업종 중립 버전(PIT 업종 N4 대기). 요소별 개별 투입 대 composite 비교(설계 `02` §249는 composite 대표 채택).

### 한계

- 긴 구간 세 cell 전부 placebo 실패, 통과한 하나는 미수행.
- 매출 94.2% 매핑 대체, 자본총계 10.1% 정정. `value_component_count` 분포 미확인.
- 커버리지 0.564. 네 요소 간·`fin_gross_profitability`와의 상관 없음.
- 업종 중립화 없음(V6가 크다고 확인). holdout 미개방.

---

## 4. `fin_gross_profitability` — 매출총이익률 (총자산 대비)

| 항목 | 값 |
|---|---|
| primary feature | `fin_gross_profitability` |
| secondary / variant | `fin_operating_profitability`(secondary, **이번 run 미스캔**), 보조 `gross_profit_source` · `profitability_available_from` · `profitability_fin_age_days`, `fin_gross_profitability_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_fin_scan_daily` / `fin_scan.py:276`(산식) · `:252`(역산 fallback) · `:250`(`avg_assets`) |
| 원천 raw 테이블 | 3번과 같음(`dart_financial_statement_raw`, `dart_xbrl_fact_raw` → vintage) |
| prod 갱신 | 분기 공시(계단). 3번과 같은 체인 |
| 분류 좌표 | C2 × T0 × U |
| 검증 phase / fdr_family / role | B / financial / ready |
| 기대 부호 → 관측 부호 | `+` → `+` |
| 사전등록 primary horizon | **[20, 40, 60, 120]**, bucket 포함 → cell 8개(Phase B 최다). exploratory [1, 5, 10] |
| 관측 신호 밴드 · 모양 | 20~120일에 고르게. 누적 0.023 → 0.028 → 0.029 → 0.036 완만 증가, 구간 0.020 / 0.023 / 0.021 / 0.027 거의 평평. peak `cum 0→120`. t는 짧을수록 크다(5.09 → 2.29) (25 §4.4) |
| 등급 / screen_pass / discovery | B 5개 · C 3개 / 5/8 / 8/8 cell |
| 모델 검증 이력 | T2 후보 14 → `improved_all_horizons`(§0 표) |

### 산식과 규칙

```sql
-- fin_scan.py:250, :252, :276
(total_assets_selected + total_assets_lag4q_selected) / 2          AS avg_assets   -- 둘 다 > 0
CASE WHEN gross_profit_selected IS NOT NULL THEN gross_profit_selected
     WHEN revenue_selected IS NOT NULL AND cogs_selected IS NOT NULL
          THEN revenue_selected - cogs_selected END                 AS gross_profit_effective
CASE WHEN avg_assets > 0 THEN gross_profit_effective / avg_assets END AS fin_gross_profitability
```

- 분자 TTM 매출총이익, 분모 평균 총자산(당기 + 4분기 전). `fin_accruals_to_assets`와 분모를 공유한다.
- **`gross_profit_source`가 `revenue_minus_cogs_fallback`인 비율 94.4%.** 역산이 틀린 계산은 아니지만 매출원가 계정이 없는 업종(금융)은 통째로 빠지고, "손익계산서 최상단이라 조작 여지가 작다"는 Novy-Marx 논거가 약해진다 (25 §2.3).
- 횡단면 표준화 없음. 단위: 비율.

### 가용 시점(PIT)과 지연

- `profitability_available_from`(= `gross_profit`의 `available_from`) 기준 interval join. `native_t`.
- vintage 나이 **35개 중 최장**: KOSDAQ 평균 105.2일 / 95분위 **705.9일**, KOSPI 102.9일 / 531.0일 (25 §2.5). 매핑을 못 찾은 분기를 건너뛰고 더 오래된 값을 계속 쓰기 때문이다. PIT 위반은 아니지만 20번에 한 번은 2년 묵은 값이다.
- lag1 유지율: 미측정.

### 예측력 — broad × common_survivor × native_t

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | +0.0233 | 0.346 | 4.07 | +0.10%p | 0.00009 | B · screen-pass |
| cum | 0→40 | +0.0275 | 0.375 | 3.18 | +0.29%p | 0.0024 | B · screen-pass |
| cum | 0→60 | +0.0293 | 0.391 | 2.68 | +0.46%p | 0.0112 | C · robustness 실패 |
| cum | 0→120 | **+0.0360** | 0.453 | 2.29 | **+0.66%p** | 0.0317 | C · robustness 실패 |
| bucket | 10→20 | +0.0196 | 0.306 | 5.09 | +0.04%p | ~0 | B · screen-pass |
| bucket | 20→40 | +0.0226 | 0.333 | 3.91 | +0.11%p | 0.00017 | B · screen-pass |
| bucket | 40→60 | +0.0211 | 0.312 | 3.62 | +0.10%p | 0.00053 | B · screen-pass |
| bucket | 60→120 | +0.0267 | 0.360 | 2.47 | +0.21%p | 0.0201 | C · robustness 실패 |

(25 §4.1) IC와 q5 방향 일치, 크기는 작다 — \|IC\| 0.036에 +0.66%p. Phase B 재무 계열 중간(3번 0.122/+4.52%p, 6번 0.018/+0.10%p 사이). exploratory [1, 5, 10] 결과는 원문에 없다.

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | cum 0→120 **1.228**, cum 0→60 1.218, cum 0→40 1.203, 나머지 1.151~1.193 | 통과. 유동성 좋은 종목에서 15~23% 강하다 |
| lag1 유지율 | 미측정 | — |
| available 표본 부호 | 8/8 일치 | 통과 |
| 기간 일관성 | cum 0→20 · bucket 10→20 · bucket 20→40 **4/4**, 나머지 5개 3/4 | 통과(4구간) |
| 시간 placebo | cum 0→60 **0.2277**(NW 59) / cum 0→120 **0.2970**(119) / bucket 60→120 **0.1683**(59) 전부 실패. 나머지 5 cell 대상 아님(NW 9~39) | **B 5개 전부 검사를 받지 않았다.** 받은 셋은 전부 실패 |
| 비중첩 offset | 3 cell `complete`, pass | 통과 |
| source 경고 (Phase B) | `warn` — `mapping_fallback_ratio` **0.9440**(`gross_profit`) > 0.50, `revision_ratio` **0.1014**(`total_assets`) > 0.10, `pairing_mismatch_ratio` 0.000125 | 두 경고 → 상한 B |

(25 §4.2) "긴 구간에서 검사를 받으면 전부 떨어진다"는 사실이 이 family의 실제 상태다. 짧은 cell이 살아남은 것은 넓게 잡은 사전등록의 결과다.

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | 2017-02-27 ~ 2025-02-05 |
| 유효 거래일 | 1,927일 |
| 날짜당 종목 수 | 855~856개 |
| coverage_ratio | 0.584 (KOSDAQ 0.613 / KOSPI 0.538) |
| 관측 행 수 | 4,208,192 |

**이력**: I7(XBRL fallback) 전에는 커버리지 **0.0315**로 사실상 표본이 없었고 `11_feature_taxonomy.md` §3은 이 family를 C로 적었다. I7 뒤 **0.5835**로 늘면서 짧은 horizon 5 cell이 새 B로 살아났다 (25 §6.1, `08_phase_b_implementation_log.md`). `09_all_feature_results.md`·`11_feature_taxonomy.md`의 "무신호" 서술은 수정 전 기준이다.

### 중복성

| 상대 | ρ | 뜻 |
|---|---:|---|
| `px_amihud_20d` | −0.161 | 수익성 좋은 회사는 크다 — 규모와 얽힘 |
| `px_near_52w_high` | +0.149 | 고점 근처 |
| `px_mom_12_1` | +0.132 | |
| `px_maxret_20d` | 미측정(원문 §7이 원본 참조로 남김) | |

미확인: `fin_value_z`(핵심 가설의 전제), `fin_accruals_to_assets`(`avg_assets` 공유), `fin_operating_profitability`(secondary 미스캔). 한국 복제율은 수익성 5.0%(엄격 0.0%)인데 신호가 나왔다 — 예외인지 표본 문제인지 미해결 (25 §3.4).

### 모델 투입 메모

- **변환**: 비율. 분모 `avg_assets > 0`이라 부호 문제는 없지만 초소형 자산에서 극단값이 난다 — fold별 winsor 뒤 z 또는 rank. **업종 중립화 부재에 가장 취약한 축**(소프트웨어와 유통이 한 풀) — 업종 rank가 가능해지면 우선 적용 대상.
- **결측**: `gross_profit`도 `revenue − cogs`도 없으면 NULL. 42%가 빈다. 금융업이 구조적으로 빠지므로 `isna`가 업종 프록시를 겸한다.
- **유의점**: 94.4% 역산, 총자산 10.1% 정정, 95분위 vintage 706일. 값이 오래된 종목이 섞여 있으므로 `profitability_fin_age_days`를 함께 넣어 신선도를 모델이 보게 하는 게 맞다(미검정).
- **horizon**: 사전등록 20~120. B는 10~60일 구간(placebo 미수행), 60 이상은 C. h20/h60 실험 자리이고, h120은 단변량 근거가 약하다. 구간 IC가 평평해 특정 시점에 몰리지 않는다.
- **조합 후보**: quality × value(3번과 상관 미측정), `fin_operating_profitability` 비교, `fin_accruals_to_assets`와 분모 공유 조합. 업종 중립 진단 변형은 진단 전용(§3 참조).

### 한계

- 긴 구간 전부 placebo 실패, B 5개는 미수행.
- 94.4% 역산, 95분위 706일, 총자산 10.1% 정정.
- 2017 시작으로 4구간. `fin_value_z`와 상관 없음. secondary 미스캔.
- 업종 중립화 없음(가장 취약). holdout 미개방.

---

## 5. `fin_asset_growth_yoy` — 총자산 증가율

| 항목 | 값 |
|---|---|
| primary feature | `fin_asset_growth_yoy` |
| secondary / variant | 보조 `asset_growth_available_from` · `asset_growth_fin_age_days`, `fin_asset_growth_yoy_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_fin_scan_daily` / `fin_scan.py:280` |
| 원천 raw 테이블 | 3번과 같음 |
| prod 갱신 | 분기 공시(계단) |
| 분류 좌표 | C2 × T1(변화) × U |
| 검증 phase / fdr_family / role | B / financial / ready |
| 기대 부호 → 관측 부호 | `−` → `−`(크기 0) |
| 사전등록 primary horizon | [60, 120], bucket 포함 → cell 4개. exploratory [20, 40] |
| 관측 신호 밴드 · 모양 | 없음. `peak_ic_mean` −0.00438, `q_fdr_phase_b_min` 0.831 (24 §4.4) |
| 등급 / screen_pass / discovery | C 4개 / 0/4 / 0/4 cell |
| 모델 검증 이력 | 미투입 — discovery 0개로 후보에서 빠짐 |

### 산식과 규칙

```sql
-- fin_scan.py:280
CASE WHEN total_assets_lag4q_selected > 0
     THEN total_assets_selected / total_assets_lag4q_selected - 1 END AS fin_asset_growth_yoy
```

- 분모는 B-3 `value_lag_4q` — "1년 전에 실제로 보고됐던 총자산"(사후 정정 미반영, PIT에 맞다). 분자·분모가 같은 fs_basis.
- 분모가 항상 양수라 비율형 YoY의 부호 문제가 없다(11 §5.1). 단위: 비율.

### 가용 시점(PIT)과 지연

- `asset_growth_available_from` 기준. `native_t`. vintage 나이 KOSDAQ 74.0일 / 180일, KOSPI 74.2일 / 174일 (24 §2.4).
- lag1 유지율: 미측정.

### 예측력 — broad × common_survivor × native_t

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | **−0.00032** | −0.005 | **−0.04** | +0.56%p | 1.000 | C · BH 실패 |
| cum | 0→120 | −0.00438 | −0.069 | −0.35 | +1.27%p | 0.797 | C · BH 실패 |
| bucket | 40→60 | −0.00040 | −0.007 | −0.08 | +0.35%p | 0.992 | C · BH 실패 |
| bucket | 60→120 | −0.00293 | −0.051 | −0.36 | +0.89%p | 0.797 | C · BH 실패 |

(24 §4.1) 35개 중 \|IC\| 최소. **IC는 0인데 q5 spread는 +0.56 ~ +1.27%p** — 해석하면 안 된다. t가 −0.04 ~ −0.36이고 §5.4의 비율 지표가 전부 불안정하다. "IC는 0인데 수익은 난다"로 읽지 않는다 (24 §4.2). `09_all_feature_results.md` §7은 q를 "0.92~1.00"으로 적었는데 해설 24의 AB q는 0.797~1.000이다(run 계보 차이).

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | **14.533 / 0.410 / 4.791 / 0.110** | `tradable_pass` 전부 False. 분모 \|broad IC\|가 0.0003이라 비율이 폭주 — 성능 지표로 읽지 않는다 |
| lag1 유지율 | 미측정 | — |
| available 표본 부호 | cum 0→60, bucket 40→60 **뒤집힘** | 실패 → 상한 C (`available_sign_flip_max_grade: C`) |
| 기간 일관성 | 1 / 2 / 2 / 2 (4구간) | 실패 — 동전 던지기 수준 |
| 시간 placebo | **0.9901 / 0.9208 / 0.8713** (bucket 40→60 대상 아님) | 실패. 35개 중 최악(0.99는 사실상 최댓값) |
| 비중첩 offset | cum 0→120 · bucket 60→120 pass, cum 0→60 fail | 신호가 없으면 통과 여부가 무작위 |
| source 경고 (Phase B) | `warn` — `revision_ratio` 0.1014(`total_assets`), `mapping_fallback_ratio` 0.3176(`net_income`) 통과 | 총자산이 유일한 입력이라 정정 경고가 직접 걸린다 |

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | 2016-06-27 ~ 2025-02-05 |
| 유효 거래일 | 1,927일 |
| 날짜당 종목 수 | 919~921개 |
| coverage_ratio | 0.631 (KOSDAQ 0.663 / KOSPI 0.581) |
| 관측 행 수 | 4,548,653 |

### 중복성

| 상대 | ρ | 범위 |
|---|---:|---|
| `px_amihud_20d` | **−0.224** | −0.30 ~ +0.12 |
| `px_mom_12_1` | +0.157 | −0.23 ~ +0.28 |
| `px_near_52w_high` | +0.087 | −0.11 ~ +0.26 |

신호가 없는데 규모·모멘텀과 상당히 겹친다 — 정보는 있지만 미래 수익률과 연결되지 않는다 (24 §7). 미확인: `ev_net_share_issuance_yoy`(자산 증가 = 자금 조달), `hc_employee_growth`(29 §3.1 네 번째 가설), 형제 family.

### 모델 투입 메모

- **변환**: 비율, 분모 양수. 극단값은 있다(급성장). winsor/rank.
- **결측**: 4분기 전 총자산 필요 → 2016-06-27 시작, 커버리지 0.631.
- **유의점**: 단변량으로는 **투입 근거 없음.** 한국 복제율 24.1%(투자 카테고리)와 맞는 결과지만 한 번의 측정으로 "한국에서 안 된다"고 확정하지 않는다 (24 §8.2).
- **horizon**: 해당 없음.
- **조합 후보**: 유형자산·재고·매출채권으로 분해(미확인), 규모 통제, `hc_employee_growth`와 같은 "확장" 축인지 확인. 다변량 모델에서 상호작용 항으로만 시도할 가치.

### 한계

- 신호 없음(최소 q 0.797, t −0.04 ~ −0.36). 유지율 무의미.
- 4구간, 총자산 10.1% 정정, 평균 74일 묵은 값.
- 분해·`ev_net_share_issuance_yoy` 상관 미실행. 업종 중립화 없음.

---

## 6. `fin_accruals_to_assets` — 발생액 (이익과 현금흐름의 차이)

| 항목 | 값 |
|---|---|
| primary feature | `fin_accruals_to_assets` |
| secondary / variant | 보조 `fs_basis_used` · `accruals_available_from` · `accruals_fin_age_days`, `fin_accruals_to_assets_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_fin_scan_daily` / `fin_scan.py:283` |
| 원천 raw 테이블 | 3번과 같음 |
| prod 갱신 | 분기 공시(계단) |
| 분류 좌표 | C2 × T0 × U |
| 검증 phase / fdr_family / role | B / financial / ready |
| 기대 부호 → 관측 부호 | `−` → **`+`** |
| 사전등록 primary horizon | [60, 120], bucket 포함 → cell 4개. exploratory [20, 40] |
| 관측 신호 밴드 · 모양 | 반대 부호, 크기 사실상 0(\|IC\| 0.011~0.018). 신호 모양 미기록 |
| 등급 / screen_pass / discovery | C 3개 · D 1개 / 0/4 / 0/4 cell (BH는 3/4 통과했으나 부호 반대) |
| 모델 검증 이력 | 미투입 |

### 산식과 규칙

```sql
-- fin_scan.py:283
CASE WHEN avg_assets > 0 AND net_income_selected IS NOT NULL
          AND operating_cash_flow_selected IS NOT NULL
     THEN (net_income_selected - operating_cash_flow_selected) / avg_assets END
```

- 세 입력이 **같은 fs_basis·같은 4분기 세트**여야 빼기가 성립한다 — 이 family가 마트의 "하루 하나의 fs_basis" 설계 이유다 (23 §2.2, 모듈 docstring). 분모는 4번과 같은 `avg_assets`.
- 단위: 비율. 양수면 이익 > 현금.

### 가용 시점(PIT)과 지연

- `accruals_available_from = greatest(net_income, operating_cash_flow, total_assets의 available_from)`. `native_t`. vintage 나이 KOSDAQ 74.5일 / 182일, KOSPI 73.6일 / 160일.
- lag1 유지율: 미측정.

### 예측력 — broad × common_survivor × native_t

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | +0.0152 | 0.440 | 3.23 | +0.08%p | 0.0021 | C · 부호 반대(discovery 아님) |
| cum | 0→120 | **+0.0180** | 0.501 | 2.50 | +0.10%p | 0.0185 | C · 부호 반대 |
| bucket | 40→60 | +0.0109 | 0.294 | 3.70 | +0.03%p | 0.0004 | **D** · 부호 반대 |
| bucket | 60→120 | +0.0136 | 0.388 | 2.72 | +0.15%p | 0.0100 | C · 부호 반대 |

(23 §4.1) **IC와 q5가 어긋난다.** `q5_spread_aligned`는 기대 부호(`−`)를 곱한 값이라 표의 +0.10%p는 **원값 −0.10%p** — 발생액 상위 20%가 하위 20%보다 0.10%p 덜 올랐다(기대대로). 그런데 IC는 +0.0180(발생액 큰 쪽이 좋다). 순위 상관과 양 끝 평균이 다른 이야기를 한다. 다만 크기가 워낙 작아 어느 쪽도 경제적 의미가 없다 (23 §4.3). t 2.5~3.7이 유의한 것은 1,927일 표본 때문 — 유의성과 크기를 분리해서 본다.

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 1.208 / 1.238 / 1.129 / 1.172 | 통과 — 유동성 좋은 종목에서 반대 신호가 20% 이상 강하다 |
| lag1 유지율 | 미측정 | — |
| available 표본 부호 | 4/4 일치 | 통과 |
| 기간 일관성 | **0 / 1 / 0 / 1** (4구간) | 실패. 반대 방향이 완벽히 일관되지도 않다 |
| 시간 placebo | **0.4059 / 0.7921 / 0.5644** (bucket 40→60 대상 아님) | 실패. 무작위와 구분 안 됨 |
| 비중첩 offset | `complete`인데 `nonoverlap_robustness_pass = False` | 실패(검정은 돌았다) |
| source 경고 (Phase B) | `warn` — `revision_ratio` **0.1014**(`total_assets`), `mapping_fallback_ratio` 0.3176(`net_income`), `pairing_mismatch_ratio` 0.000125 | 정정 하나로 warn |

(23 §4.2) `bucket 40→60`이 D인 것은 등급 규칙(`D: no_signal_or_wrong_sign_or_robustness_fail`)대로다. 나머지 셋은 `robustness_pass`까지 실패해 C 분기로 빠졌다.

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | 2017-02-27 ~ 2025-02-05 |
| 유효 거래일 | 1,927일 |
| 날짜당 종목 수 | 865개 (KOSPI 662 / KOSDAQ 377 — 비율은 KOSPI가 낮은데 절대수는 많다) |
| coverage_ratio | 0.596 (KOSDAQ 0.630 / KOSPI 0.544) |
| 관측 행 수 | 4,296,914 |

### 중복성

Phase A 계열과 거의 독립 — `px_mom_12_1` +0.074, `px_near_52w_high` +0.073, `px_idio_vol_60d` −0.068, `px_maxret_20d` −0.040 (23 §7). 미확인: `fin_gross_profitability`(`avg_assets` 공유), `fin_asset_growth_yoy`(자산이 늘면 발생액도 는다), 형제 전부.

### 모델 투입 메모

- **변환**: 비율. winsor/rank. 업종별 발생액 수준이 다르다(은행·바이오·조선).
- **결측**: 순이익·영업현금흐름 둘 다 있고 같은 fs_basis여야 한다. 40%가 빈다. 2017 시작.
- **유의점**: **부호를 정할 수 없다.** 기대 `−`, 관측 `+`, 기간 0~1/4, placebo 실패. 반대 방향이 안정적인 유형(`px_turnover_shock`)과 다르다 — 재등록 가치도 낮다. 순이익의 31.8%가 매핑 대체.
- **horizon**: 해당 없음.
- **조합 후보**: 설계 `02` Q3의 보조 `fin_cash_earnings_quality = CFO/|NI|`와 `cfo_positive`/`ni_positive` 플래그(미구현). 다변량에서 quality 묶음의 부속으로만.

### 한계

- 반대 부호를 결론으로 쓸 수 없다(세 검사 전부 불안정). 크기 사실상 0.
- 순이익 31.8% 매핑 대체, 총자산 10.1% 정정, 4구간.
- 형제 상관 미확인. 업종 중립화 없음.

---

## 7. `fin_sue` — 실적 서프라이즈 (SUE, event-time)

| 항목 | 값 |
|---|---|
| primary feature | `fin_sue` |
| secondary / variant | 보조 `sue_history_count` · `comparative_policy`(`'as_was_lag4q'` 상수) · `seasonal_change` · `quarterly_eps` · `comparative_eps` · `is_primary_constant_sample` · `revision_within_60_sessions`, `fin_sue_lag1`(공식 variant가 **lag1**) |
| 마트 / 산식 위치 | `fin_sue_event` / `research/etl/features/sue_event.py:252`, `MIN_SUE_HISTORY = 8` (`:68`) |
| 원천 raw 테이블 | `dart_filing_receipt_raw`(원본 접수 판정), `dart_xbrl_fact_raw`(`weighted_avg_shares`), `dart_financial_statement_raw`(`controlling_net_income`) → vintage, `daily_ohlcv` |
| prod 갱신 | 분기 실적 발표 건마다(event-time). XBRL 백필은 `dart backfill-xbrl-receipts --targets-file`(수동) |
| 분류 좌표 | C2 × **T2(놀라움)** × U (22 §4.4). `11_feature_taxonomy.md` §3 표는 C4로 적었다 — 두 문서가 다르다 |
| 검증 phase / fdr_family / role | B / event / ready(6/6) — **그러나 `insufficient` 6/6** |
| 기대 부호 → 관측 부호 | `+` → 관측 없음 |
| 사전등록 primary horizon | `primary_horizon_set: []`. **event_buckets [[0,3],[3,5],[5,10],[10,20],[20,40],[40,60]]**, `cell_type = event_bucket` |
| 관측 신호 밴드 · 모양 | 대상 아님 — IC·q5·q 전부 NaN |
| 등급 / screen_pass / discovery | C 6개(기본값) / —/6 / —/6. **"재보니 0"이 아니라 "안 쟀다"** |
| 모델 검증 이력 | 미투입 — 값이 없다 |

### 산식과 규칙

```sql
-- sue_event.py:252
CASE WHEN ewf.history_count >= 8 AND ewf.history_stddev > 0
     THEN ewf.seasonal_change / ewf.history_stddev END AS fin_sue
-- seasonal_change = quarterly_eps − comparative_eps
-- quarterly_eps   = controlling_net_income(standalone) / weighted_avg_shares
-- comparative_eps = value_lag_4q / shares_lag4q  ('as_was_lag4q')
-- history_stddev/count = 직전 8개 seasonal_change (ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING)
```

- grain `(ticker, original_rcept_no, event_formation_date, market)`. Phase B 유일의 **event-time** 마트 — 발표 뒤 60세션 창의 시장 반응을 발표 건마다 잰다 (22 §3.2).
- 원본 이벤트만 센다: `captured_vintage_status = 'original_confirmed_revisions_partial' AND is_revision = FALSE`. 같은 날 정정은 `same_day_effective_rcept_no`로 합친다.
- **비교 EPS는 대체 방법**이다. 같은 공시 안의 전년 동기 XBRL 컨텍스트를 읽는 원래 방법은 다중 컨텍스트 파싱이 없어 못 만들고, 4분기 전에 실제로 보고됐던 값을 쓴다 (22 §3.4).
- `is_primary_constant_sample`: 6개 bucket 수익률 전부 있고, CA 오염 없고, 60세션 내 정정 없음.

### 가용 시점(PIT)과 지연

- `event_formation_date` = B-2 `available_from`(공시 다음 KRX 세션). 공식 variant `lag1`.
- **8분기 이력 요구**가 결정적이다. 2년치 분기 XBRL이 다 있어야 한 건이 나온다. 신규 상장사는 2년간 값이 없다.

### 예측력 — broad × common_survivor × lag1

| scan | bucket | status | Rank IC | q5 spread | BH q | 등급 | 판정 |
|---|---|---|---:|---:|---:|---|---|
| event | 0→3 | insufficient | — | — | 1.000 | C | `no_formation_rows` |
| event | 3→5 | insufficient | — | — | 1.000 | C | 동일 |
| event | 5→10 | insufficient | — | — | 1.000 | C | 동일 |
| event | 10→20 | insufficient | — | — | 1.000 | C | 동일 |
| event | 20→40 | insufficient | — | — | 1.000 | C | 동일 |
| event | 40→60 | insufficient | — | — | 1.000 | C | 동일 |

(22 §6.1) q 1.000은 `bh_missing_p_value: 1.0` — "유의하지 않다"가 아니라 "p값이 없다". `failed_gates`에 게이트 네 개가 적혀 있지만 계산할 값이 없어 자동 미통과된 것이다. Phase B 78 cell 중 이 6개를 빼야 evaluable 72 cell이다.

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable · lag1 · available · 기간 · placebo · offset | 대상 아님 — formation row 0 | — |
| source 경고 (Phase B) | `warn` — `revision_ratio` **0.1270**(`weighted_avg_shares`) > 0.10, `mapping_fallback_ratio` 0.3424(`controlling_net_income`) | 표본이 생겨도 남는다. `grade_cap` None이지만 A는 어렵다 |

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| `effective_start` | **2025-05-02** |
| coverage_ratio | **0.00000** (`event_coverage.parquet`, 다른 17개와 출처가 다르다) |
| observations | 0 |
| 평가 가능 cell | 0 / 6 |

`common_survivor` 표본은 120일 라벨 때문에 formation이 2025-02-05에서 끝난다. **데이터 구간(2025-05-02~)과 표본 구간이 겹치지 않는다** (22 §5.2). 원인은 XBRL 백필 미완(B-1 6항) — 코드가 아니라 수집 범위 문제다.

### 중복성

A×B 상관 없음(`top_rank_correlation_pair = None`). 표본이 생기면 확인할 것: `fin_gross_profitability`·`fin_value_z`와 `controlling_net_income` 공유, `ev_amendment_ratio`·`ev_filing_activity`와 `dart_filing_receipt_raw` 공유.

### 모델 투입 메모

- **지금은 투입 불가.** `sue_history_count`가 8 미만이면 NULL이라 값 자체가 없다.
- 표본이 생기면: event-time 값을 일별 패널에 붙이려면 발표일 뒤 N세션 동안 broadcast하는 별도 변환이 필요하다(현재 마트에 없음). PEAD 가설상 h5~h20이 자리다(bucket 앞이 촘촘). `daily_ic`도 event cell은 저장하지 않는다(00 §7).
- **다음 순서** (22 §11): 8분기 이력을 만들 XBRL 접수 대상을 역산 → `dart backfill-xbrl-receipts` → 마트 재생성 → `effective_start`가 표본 안으로 들어왔는지 확인 → **같은 사전등록(event bucket 6개, `+`)으로 재실행.** 결과를 보고 격자를 바꾸면 사전등록이 무의미해진다.
- 설계상 짝인 `fin_earnings_drift`와 상호작용 `fin_pead_retail_opposition = −z(SUE) × z(개인 순매수, 공시 후 5d)`(02 Q6)는 만들지 않았다.

### 한계

- 표본 0. 비교 EPS가 대체 방법(`as_was_lag4q`). 가중평균주식수 12.7% 정정.
- 8분기 요구가 커버리지를 구조적으로 제한한다. `fin_earnings_drift` 미구현. holdout 미개방.

---

## 8. `hc_employee_growth` — 직원 수 증가율

| 항목 | 값 |
|---|---|
| primary feature | `hc_employee_growth_yoy` |
| secondary / variant | 보조 `hc_employee_growth_available_from` · `periodic_source_warning`(`'final_vintage'`) · `vintage_capture_ratio`(0.0184), `hc_employee_growth_yoy_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_periodic_extras` / `research/etl/features/periodic_extras.py:99`, `available_from` `:98` |
| 원천 raw 테이블 | `dart_employee_raw`(DS002 직원 현황, `statement_type='employee'`), `dart_filing_receipt_raw`(접수일·`structural_change_year`) |
| prod 갱신 | 연 1회(사업보고서). 수집은 `dart sync-periodic-extras`(슬라이스 ledger 재개). 정기 Cronicle 이벤트는 `deploy/prod/README.md` 표에 없다 |
| 분류 좌표 | **C7(시장구조·비재무 — 인적자본)** × T1(변화) × U. `11_feature_taxonomy.md` §1이 지목한 빈 칸을 처음 채운 항목 |
| 검증 phase / fdr_family / role | B / human_capital / ready. 2026-08-27 확장 등록분 |
| 기대 부호 → 관측 부호 | **없음(양방향)** → `+` |
| 사전등록 primary horizon | [60, 120], bucket 포함 → cell 4개. exploratory [20, 40]. `source_quality: {status: warn, warning: final_vintage, grade_cap: B}` 사전 명시 |
| 관측 신호 밴드 · 모양 | 누적 0.0154 → 0.0157 **거의 평평**(60일 이후 추가 신호 없음), 구간 0.0084 → 0.0079 감소. peak `cum 0→120` (29 §4.4) |
| 등급 / screen_pass / discovery | B 1개 · C 3개 / 1/4 / 1/4 cell |
| 모델 검증 이력 | T2 후보 14 → `improved_all_horizons`(§0 표). 단변량 근거는 14개 중 가장 약하다 |

### 산식과 규칙

```sql
-- periodic_extras.py:98-99
greatest(e.employee_available_from, e.previous_available_from) AS available_from,
CASE WHEN e.previous_year = e.bsns_year - 1
      AND e.headcount >= 1 AND e.previous_headcount >= 1
      AND NOT (s.corp_code IS NOT NULL
               AND abs(e.headcount / nullif(e.previous_headcount, 0) - 1) >= 0.30)
     THEN e.headcount / nullif(e.previous_headcount, 0) - 1 END AS hc_employee_growth_yoy
```

- `headcount`는 `raw_payload.sm`을 숫자로 파싱해 합계행(`fo_bbm LIKE '%합계%'`)이 있으면 그것만, 없으면 구분행 합.
- **구조 변경 연도(`합병등종료보고서(분할|합병)`)에 30% 이상 변하면 버린다.** 채용과 합병을 구분하는 장치. 두 조건은 AND다 (29 §2.2).
- 연속된 사업연도가 있어야 한다. 단위: 비율. 정규직·계약직 구분 없음.

### 가용 시점(PIT)과 지연

- **올해 값과 작년 값 중 늦게 공개된 쪽부터** 쓴다. DS002는 최종본만 주므로 나중에 정정된 값이면 정정값을 쓰게 된다 — PIT 시점은 지켰지만 vintage가 없다(29 §2.4). `FINAL_VINTAGE_CAPTURE_RATIO = 0.0184`.
- vintage 나이 KOSDAQ 평균 **181.3일** / 95분위 470일, KOSPI 182.5일 / 484일. 분기 재무(74일)의 2.5배.
- lag1 유지율: 미측정. 연 1회 계단이라 값이 바뀌는 날은 1년에 한 세션이다.

### 예측력 — broad × common_survivor × native_t

양방향이라 `q5_spread_aligned`가 원값과 같다.

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | +0.0154 | 0.207 | 1.52 | +0.17%p | 0.162 | C · BH 실패 |
| cum | 0→120 | **+0.0157** | 0.221 | 1.27 | +0.48%p | 0.248 | C · BH 실패 |
| bucket | 40→60 | +0.0084 | 0.149 | **1.81** | **−0.01%p** | **0.093** | **B · discovery + screen-pass** |
| bucket | 60→120 | +0.0079 | 0.151 | 1.16 | +0.02%p | 0.297 | C · BH 실패 |

(29 §4.1) 통과한 하나가 **q 0.093으로 문턱(0.10)을 0.007 차이**로 넘었고, 그 cell이 \|IC\| 최소(0.0084)다. t가 큰 것은 IC가 커서가 아니라 NW lag 19로 중첩 보정이 작아서다 (29 §4.2). **IC와 q5가 어긋난다** — discovery cell의 q5는 −0.006%p, 부호까지 반대. 경제적 크기 0 (29 §4.3).

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 0.925 / 1.062 / 1.035 / 1.078 | 통과 |
| lag1 유지율 | 미측정 | — |
| available 표본 부호 | 4/4 일치 | 통과 |
| 기간 일관성 | 4 / 3 / 4 / 3 (4구간, 관측 부호 `+` 기준) | 통과 |
| 시간 placebo | **0.5545 / 0.5743 / 0.7129** 전부 실패. bucket 40→60 대상 아님(NW lag 19) | **B cell은 받지 않았다.** 받은 셋 전부 실패 |
| 비중첩 offset | 3 cell `complete`, pass | 통과 |
| source 경고 (Phase B) | `warn` — `final_vintage`, **`source_quality_grade_cap = B`**(사전등록). `revision_ratio`·`mapping_fallback_ratio`는 NaN(분기 vintage 대상 아님) | 상한 B. 데이터를 다시 받아도 해결되지 않는 구조적 한계 |

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | 2016-06-27 ~ 2025-02-05 |
| 유효 거래일 | 1,985일 |
| 날짜당 종목 수 | 944~947개 |
| coverage_ratio | 0.672 (KOSDAQ 0.711 / KOSPI 0.613) |
| 관측 행 수 | 4,620,549 |

### 중복성

| 상대 | ρ | 범위 |
|---|---:|---|
| `px_amihud_20d` | −0.128 | −0.19 ~ **+0.42** (부호가 날짜에 따라 뒤집힌다) |
| `px_resid_mom_12_1` | −0.047 | −0.53 ~ +0.12 |
| `px_near_52w_high` | +0.022 | −0.14 ~ +0.43 |

Phase A와 거의 독립. 미확인: `hc_productivity`(**`headcount` 분자 공유**, 수준 대 변화), `fin_asset_growth_yoy`(같은 "확장" 축 가설).

### 모델 투입 메모

- **변환**: 비율. 30% 필터가 구조 변경 연도에만 걸리므로 그 밖의 극단값은 남아 있다 — winsor/rank.
- **결측**: 전년 값 없음, 인원 1 미만, 구조변경+30%. 33%가 빈다. 두 해 값이 필요해 시작이 2016-06-27.
- **유의점**: 연 1회 계단 — 1년에 한 번 바뀌고 평균 182일 묵었다. 회전율은 거의 0이지만 정보도 느리다. 상한 B는 최종본 원천 때문이라 표본이 늘어도 A는 없다. 단변량 근거가 T2 14개 중 가장 약하다 — 묶음 개선에서 이 family의 몫은 작을 가능성이 높다(29 §9).
- **horizon**: 사전등록 60/120. B는 `bucket 40→60`(placebo 미수행, q5 −0.01%p). 60일 이후 누적 IC가 평평하다 → h60 너머로 늘려도 얻는 게 없다.
- **조합 후보**: `hc_productivity`와 함께 넣을 때 분자 공유 확인 필요. `fin_asset_growth_yoy`와 "확장" 축 공동 검정. 정규직·계약직 분리(원천에 구분 가능성, 미사용).

### 한계

- 근거 약함(q 0.093, t ≤ 1.81), discovery cell q5 0, placebo 전부 실패, 상한 B.
- 평균 182일 묵은 값. 직원 구성 미구분. 구조 변경 필터 완전성 미확인.
- `hc_productivity` 상관 없음. 업종 중립화 없음(제조업 대 플랫폼).

---

## 9. `hc_productivity` — 1인당 매출 (노동생산성)

| 항목 | 값 |
|---|---|
| primary feature | `hc_revenue_per_employee` |
| secondary / variant | 보조 `hc_productivity_available_from`, `hc_revenue_per_employee_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_periodic_extras` / `periodic_extras.py:127`, 매출 선택 규칙 `:112` |
| 원천 raw 테이블 | `dart_employee_raw`(DS002) + `stock_metric_vintage_fact`(`revenue`, `reprt_code='11011'`) ← `dart_financial_statement_raw`/`dart_xbrl_fact_raw` |
| prod 갱신 | 연 1회(사업보고서·연간 재무). 8번과 같은 수집 경로 |
| 분류 좌표 | **C7(인적자본)** × T0(수준) × U — 8번(T1)과 같은 원천으로 수준과 변화를 각각 만들었다 |
| 검증 phase / fdr_family / role | B / human_capital / ready. 2026-08-27 확장 등록분 |
| 기대 부호 → 관측 부호 | **없음(양방향)** → `+` |
| 사전등록 primary horizon | [60, 120], bucket 포함 → cell 4개. exploratory [20, 40]. `grade_cap: B` 사전 명시 |
| 관측 신호 밴드 · 모양 | 누적 0.0136 → 0.0181, 구간 0.0144 → 0.0170 증가. 범위 끝 최대, peak `cum 0→120` (30 §4.4) |
| 등급 / screen_pass / discovery | B 1개 · C 3개 / 1/4 / **2/4** cell |
| 모델 검증 이력 | T2 후보 14 → `improved_all_horizons`(§0 표). 모델이 이 피쳐를 어느 방향으로 쓰는지 알 수 없다(30 §9) |

### 산식과 규칙

```sql
-- periodic_extras.py:112, :127
-- annual_revenue: metric_code='revenue' AND reprt_code='11011' AND value_numeric > 0,
--   (ticker, bsns_year)당 1행 — CFS 우선, available_from DESC, rcept_no DESC (전순서)
greatest(e.employee_available_from, r.revenue_available_from) AS available_from,
ln(r.revenue / e.headcount) AS hc_revenue_per_employee
WHERE e.headcount > 0 AND e.employee_available_from IS NOT NULL
  AND r.revenue_available_from IS NOT NULL
```

- 연간 매출만 쓴다(분기 합산 없음) — 직원 수가 연 1회 값이라 단위를 맞춘다. 로그는 분포 때문(반도체와 노동집약 제조업이 자릿수가 다르다). Rank IC는 로그 무관.
- **fs_basis 통일이 없다.** 매출은 연결(CFS 우선)일 수 있고 직원 수는 별도 기준 인원일 수 있다. 이 불일치는 확인하지 않았다 (30 §2.3).
- 단위: ln(원/명).

### 가용 시점(PIT)과 지연

- 직원 수와 연간 매출 vintage **둘 중 늦은 쪽**부터. 그래서 8번보다도 늦다.
- vintage 나이 **35개 중 최장**: KOSDAQ 평균 **189.1일** / 95분위 489일, KOSPI 189.0일 / **574일** (30 §2.4). 20번에 한 번은 1년 7개월 묵은 값.
- DS002 최종본 한계는 8번과 같다. lag1 유지율: 미측정.

### 예측력 — broad × common_survivor × native_t

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | +0.0136 | 0.223 | 1.69 | **−0.55%p** | 0.120 | C · BH 실패 |
| cum | 0→120 | **+0.0181** | 0.285 | 1.48 | **−0.80%p** | 0.174 | C · BH 실패 |
| bucket | 40→60 | +0.0144 | 0.237 | **3.12** | **−0.12%p** | **0.0029** | **B · discovery + screen-pass** |
| bucket | 60→120 | +0.0170 | 0.281 | 2.11 | **−0.24%p** | 0.0495 | C · discovery, robustness 실패 |

(30 §4.1) **네 cell 전부 IC는 양수, q5 spread는 음수 — 35개 중 가장 일관된 어긋남이다** (30 §4.3, 00 §9.1). 순위로는 1인당 매출이 높은 종목이 낫고, 상위 20% 평균 대 하위 20% 평균은 반대다. 하위 20%에 매출이 아직 안 나오는 바이오·플랫폼 성장 기업이 섞여 소수의 대박이 평균을 끌어올린다는 해석이 가능하지만, **중앙값 spread·분위별 평균수익률을 산출하지 않아 판정할 수 없다.** 순위 모델 입력이면 IC, 분위 롱숏이면 spread를 본다 — 이 family는 둘이 정반대다.

### 강건성

| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 1.208 / 1.143 / 1.042 / 1.025 | 통과 |
| lag1 유지율 | 미측정 | — |
| available 표본 부호 | 4/4 일치 | 통과 |
| 기간 일관성 | **5구간 중 4** (4 cell 전부) | 통과. 8번(4구간)보다 검정력이 낫다 |
| 시간 placebo | cum 0→60 **0.3663** / cum 0→120 **0.2376** / bucket 60→120 **0.2376** 전부 실패. bucket 40→60 대상 아님(NW lag 19) | **B cell은 받지 않았다.** 해설 30 §1은 "두 cell"이라 적었지만 §5.2 표는 세 cell이다 |
| 비중첩 offset | 3 cell `complete`, pass | 통과 |
| source 경고 (Phase B) | `warn` — `final_vintage`, **`grade_cap = B`**(사전등록) | 상한 B |

### 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | 2015-06-29 ~ 2025-02-05 |
| 유효 거래일 | 2,176일 |
| 날짜당 종목 수 | 904~906개 |
| coverage_ratio | 0.704 (KOSDAQ 0.753 / KOSPI 0.631) |
| 관측 행 수 | 4,842,808 |

8번보다 길고 넓다 — 수준은 한 해 값으로 되기 때문이다 (30 §6).

### 중복성

| 상대 | ρ | 범위 |
|---|---:|---|
| `px_idio_vol_60d` | −0.109 | −0.19 ~ +0.03 (방향 안정) |
| `px_near_52w_high` | +0.083 | −0.07 ~ +0.57 |
| `px_amihud_20d` | −0.081 | −0.19 ~ +0.30 |
| `px_maxret_20d` | −0.069 | −0.19 ~ +0.12 |

Phase A와 거의 독립. 미확인: `hc_employee_growth`(**`headcount` 공유**), `fin_value_z`의 S/P(**연매출 분자 공유**, 분모만 시총 대 직원 수), 업종(업종별 분포 미확인 — 코드는 `dart_corp_master.induty_code`로 2026-08-15부터 있지만 PIT가 아니다).

### 모델 투입 메모

- **변환**: 이미 로그. fold별 z 또는 rank. **업종 부재에 가장 취약한 축** — 유통과 소프트웨어의 1인당 매출은 근본적으로 다르다. 업종 중립 없이는 업종 더미를 재는 셈이 될 수 있다(30 §3.1). 업종 rank가 가능해지면 최우선 적용.
- **결측**: 직원 수 또는 연간 매출 vintage 없음. 30%가 빈다. 2015-06-29 시작.
- **유의점**: **IC와 spread가 정반대**라 모델이 어느 방향으로 쓸지, 그게 도움이 되는지 알 수 없다. 상한 B. 평균 189일 묵은 값. fs_basis 불일치 미확인. 연 1회 계단(회전율 ≈ 0).
- **horizon**: 사전등록 60/120. B는 `bucket 40→60`(placebo 미수행). discovery는 60~120 구간에도 있지만 C. IC가 범위 끝에서 최대라 h120 후보이지만 spread는 h120에서 가장 음수(−0.80%p).
- **조합 후보**: `hc_employee_growth`(수준 × 변화), `fin_value_z` S/P와 중복 확인, 업종 중립 버전. 이 family를 쓸지 판단하려면 **분위별 평균수익률이 먼저다**(30 §8.1).

### 한계

- 네 cell IC·q5 반대 — 풀 자료 없음. 업종 효과 분리 불가.
- placebo 세 cell 실패, 상한 B, 95분위 574일.
- fs_basis 통일 없음, 직원 구성 미구분, `hc_employee_growth`·S/P 상관 없음.

---

## 10. Horizon Scan 미검정 피쳐

같은 마트에 있지만 Horizon Scan 단변량 검정을 거치지 않은 컬럼이다.

| 컬럼 | 뜻(산식 요약) | 마트 | 모델 사용 | 검증 상태 | 메모 |
|---|---|---|---|---|---|
| `fin_roa` | `net_income / total_assets` | `feat_fin_pit` | **baseline 40에 포함** | baseline 모델 포함(단변량 검정 없음, walk-forward Rank IC는 묶음 결과만) | 아래 PIT 규칙 차이 참조 |
| `fin_roe` | `COALESCE(controlling_net_income, net_income) / total_equity` | `feat_fin_pit` | baseline 40에 포함 | 같음 | 자본잠식이면 부호가 뒤집힘 — `fin_is_negative_equity`와 같이 봐야 한다 |
| `fin_operating_margin` | `operating_income / revenue` | `feat_fin_pit` | baseline 40에 포함 | 같음 | 매출 매핑이 얇은 구간(I7 이전 8,103행)은 NULL |
| `fin_debt_to_equity` | `total_liabilities / total_equity` (equity > 0일 때만) | `feat_fin_pit` | baseline 40에 포함 | 같음 | 11 §9.1이 "부채 피쳐 0개"라 한 것은 scan 25개 기준. baseline에는 있다 |
| `fin_equity_ratio` | `total_equity / total_assets` | `feat_fin_pit` | baseline 40에 포함 | 같음 | |
| `fin_ocf_to_assets` | `operating_cash_flow / total_assets` | `feat_fin_pit` | baseline 40에 포함 | 같음 | `fin_accruals_to_assets`의 CFO 항과 원천 같음 |
| `fin_cash_ratio` | `cash_and_cash_equivalents / total_assets` | `feat_fin_pit` | baseline 40에 포함 | 같음 | |
| `fin_asset_turnover` | `revenue / total_assets` | `feat_fin_pit` | baseline 40에 포함 | 같음 | |
| `fin_is_negative_equity` | `total_equity <= 0` 플래그 | `feat_fin_pit` | baseline 40에 포함 | 같음 | ~12개 종목. 상수에 가까운 횡단면 |
| `fin_has_fs` | `total_assets IS NOT NULL` | `feat_fin_pit` | baseline 40에 포함 | 같음 | 재무제표 유무 플래그 |
| `fin_book_to_market` · `fin_earnings_yield` · `fin_cfo_yield` · `fin_sales_to_price` | `fin_value_z` 네 요소(비율, winsorize 전 원값) | `feat_fin_scan_daily` | 미투입 | 보조 컬럼(합성 입력) | 요소 간 상관 미측정. S/P는 94.2% 매핑 대체 |
| `fin_operating_profitability` | `operating_income / avg_assets` | `feat_fin_scan_daily` | 미투입 | secondary 등록됐으나 **미스캔** | 매출총이익 대 영업이익 비교 미실행 |
| `value_component_count` · `negative_equity` · `fs_basis_used` · `gross_profit_source` | 합성 요소 수 / 자본잠식 / 회계기준 / 역산 여부 | `feat_fin_scan_daily` | 미투입 | 보조 컬럼(품질·감사용) | `value_component_count`는 모델 입력 후보(§3 메모) |
| `*_available_from` · `*_fin_age_days` (value·profitability·asset_growth·accruals) | PIT 시작일 / 값의 나이 | `feat_fin_scan_daily` | 미투입 | 보조 컬럼(패널 계산·커버리지 통계용) | 신선도 입력 후보(§4 메모) |
| `sue_history_count` | 직전 8분기 중 `seasonal_change` 개수 | `fin_sue_event` | 미투입 | 보조 컬럼 | 8 미만이면 `fin_sue` NULL |
| `comparative_policy` · `quarterly_eps` · `comparative_eps` · `seasonal_change` · `is_primary_constant_sample` · `revision_within_60_sessions` | SUE 구성 요소·표본 규칙 | `fin_sue_event` | 미투입 | 보조 컬럼 | `comparative_policy`는 상수 `'as_was_lag4q'` |
| `mcap_krx` | KRX 시총 원값(마스킹 없음) | `feat_market_cap` | 미투입 | 감사용 | 모델에 넣지 않는다 — 마스킹 이유 |
| `mcap_unreliable` | 무상증자 권리락~신주상장 창 플래그 | `feat_market_cap` | 미투입 | 진단용 | 패널의 0.18%. 사건 정보를 갖는 결측 지시자 |
| `hc_employee_growth_available_from` · `hc_productivity_available_from` · `periodic_source_warning` · `vintage_capture_ratio` | PIT 시작일 / `'final_vintage'` / 0.0184 | `feat_periodic_extras` | 미투입 | 보조 컬럼 | |
| `*_lag1` (9개 primary 전부) | 직전 유효 세션 값 (`LAG … OVER (PARTITION BY ticker, market ORDER BY trade_date)`) | 각 마트 | 미투입 | **지연 노출 검정용(delay gate)** | 변화 피쳐가 아니다(11 §5.2). `fin_sue`만 lag1이 공식 variant. lag1 유지율은 이 그룹 9개 모두 원문에 수치가 없다 |

**`feat_fin_pit`의 PIT 규칙은 scan 피쳐와 다르다 — 모델 개발자가 알아야 한다.**

| | `feat_fin_pit` (baseline 40) | `feat_fin_scan_daily` (T2 후보) |
|---|---|---|
| 원천 | `stock_metric_fact` (canonical 최종본) | `fin_quarterly_metric_vintage` (접수별 vintage) |
| available_from | **`period_end + 90d`(연간) / `+45d`(분기)** 고정 지연 | **실제 접수일 다음 KRX 세션** |
| 값 | 최종본 — 사후 정정이 그대로 반영 | 그 시점에 실제로 알 수 있었던 값(`fin_v4`) |
| 분자 | 보고된 원값(TTM 아님) | flow metric은 TTM |
| fs_basis | 통일 없음(metric_code별 pivot) | 하루 하나(`net_income` 기준) |
| 같은 available_from 충돌 | 최신 `period_end` 우선 | 최신 회계기간 우선(I12) |

baseline의 `fin_*` 10개와 T2의 `fin_*` 3개가 한 모델에 들어가면 **두 가지 PIT 규칙이 섞인다.** `fin_pit`의 고정 지연은 공시 기한이라 대체로 실제 접수보다 늦지만(보수적), 값은 최종본이라 정정 look-ahead가 있다. `fin_roa`(baseline)와 `fin_gross_profitability`(T2)처럼 분모가 같은 계열은 규칙 차이만큼 값이 어긋난다.

---

## 11. 이 그룹에서 아직 없는 것 — 마무리 메모

**빠진 축.** `11_feature_taxonomy.md`가 짚은 대로 재무 계열은 수준(T0)과 YoY(T1)만 있고 **수준의 변화(ΔROA, Δmargin, scaled earnings change)와 이산 전이(흑자 전환, 연속 흑자 분기 수, `fin_news_jump`)가 없다**(§5.2·§5.3). **레버리지**는 scan 25개에 0개인데 baseline `fin_pit`에 `fin_debt_to_equity`·`fin_equity_ratio`가 있다 — 단변량 검정을 안 받은 상태로 모델에 들어가 있다(§9.1). **생애주기**(Dickinson 현금흐름 부호 조합)는 canonical에 CFO·CFI·CFF가 다 있어 새 수집 없이 만들 수 있고, 밸류·수익성이 단계별로 다르게 작동한다는 조건화 축이기도 하다(§9.2). 전이·생애주기 Decline은 생존편향(`DELISTED` 28개)에 특히 취약하다(§5.5).

**업종.** 이 그룹의 3·4·9번이 업종 중립화 부재에 가장 취약하다(00 §9.8). `feat_fin_scan_daily_ind`(`fin_scan.py` `industry_view`, `research/etl/industry.py`, `definitions/industry_groups.py` — KSIC 2자리 접두, 20 미만은 section → `OTHER`로 접음)가 V6에서 업종 효과가 크다는 것을 확인했다(rank corr 중앙값 0.8305, between-industry 분산 −65.4%). 다만 **현재 업종을 과거에 소급한 look-ahead**라 진단 전용이고 scored backtest·acceptance gate·holdout에 금지다. PIT 업종(N4)이 선행 조건이다.

**중복.** B×B 상관이 없어 이 그룹 9개 중 몇 개가 독립 정보인지 말할 수 없다. 구조적으로 `feat_fin_scan_daily` 형제는 분모(`market_cap_pit`, `avg_assets`)를, `feat_periodic_extras` 형제는 분자(`headcount`)를 공유한다(00 §9.2). `|ρ| ≥ 0.7` 두 쌍이 전부 규모 축(`px_amihud_20d` × 1·2번)이고, T2 묶음 안에 `fin_log_mcap`·`mcap_krx_log`가 같은 개념으로 둘 다 들어가 있다. 규모 통제 증분 IC, B×B 상관, 분위별 평균수익률(9번의 IC·spread 어긋남 해소)이 이 그룹의 다음 작업이다(00 §10).

**문서 간 차이.** `09_all_feature_results.md`(`ab0de634…`, 08-23 run)와 해설 22~30(`889c3e83…`)의 대표 IC·커버리지는 일치한다. 다른 곳은 셋이다 — 09 §7은 `fin_value_z`의 B 사유를 `revision_ratio`만 들었지만 해설 27은 `mapping_fallback_ratio` 0.9417도 든다; 09 §7의 `fin_asset_growth_yoy` q "0.92~1.00"은 해설 24의 AB q 0.797~1.000과 다르다; `11_feature_taxonomy.md` §3은 `fin_gross_profitability`를 C(I7 전), `fin_sue`를 C4로 적었고 해설은 각각 B5/C3, C2다. 이 문서는 해설 값을 따랐다.
