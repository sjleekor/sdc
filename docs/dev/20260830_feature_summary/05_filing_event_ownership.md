# 05. 공시·자본정책 이벤트·지분 — 모델 개발용 피쳐 카드

- 작성일: 2026-09-03
- 대상: `ev_amendment_ratio`, `ev_filing_activity`, `ev_net_share_issuance_yoy`, `ev_payout_yield`,
  `own_amendment_ratio`, `own_insider_filing_activity`, `own_major_filing_activity`,
  `own_major_stake_change`, `own_major_stake_level` — 9 family, 전부 Phase B(T2)
- 기준 run: Phase A `20260827T221729-4e0ae8b0` / Phase B `20260828T123313-4e0ae8b0` / AB `20260828T165038-4e0ae8b0`
  (snapshot `2026-08-23`, config `889c3e83…`). 매크로·Phase C는 `236d0d35…` 계보(`20260830T…-efd35e70`, `20260830T122850-phasec`).
- 항목 정의와 읽는 법은 [00_reading_guide.md](00_reading_guide.md)를 먼저 본다.
- 원문 해설: `docs/dev/20260829_raw_features_explain/` (18·19·20·21·31·32·33·34·35)

이 그룹을 읽기 전에 알아야 할 것 네 가지다.

- **Phase B의 C는 강건성(`robustness_pass`) 또는 available 방향 게이트 실패다.** T1의 C(판정 보류)와
  뜻이 다르다 (09 §2, 00_읽는_법 §4.5). 이 그룹의 C는 전부 이 뜻이다.
- **정본 변형은 전부 `native_t`다.** PIT가 산식 안에 있다 — 접수일 **다음 거래일** 노출
  (`feat_filing_activity`, `feat_periodic_extras`) 또는 `available_from` interval join
  (`feat_event_scan_daily`). 수급 계열처럼 `lag1`을 정본으로 쓰지 않는다 (18 §2.4, 20 §2.5, 34 §2.5).
- **등급 A는 `robustness_required`와 함께 읽어야 한다.** NW lag 59 미만 cell(cum 0→20, bucket 10→20,
  bucket 40→60)은 시간 placebo를 통과한 것이 아니라 **받지 않았다** (00_읽는_법 §9.5).
- **B×B 상관이 없다.** 세 마트(`feat_filing_activity` 5 family, `feat_event_scan_daily` 2 family,
  `feat_periodic_extras` 2 family) 안의 형제 관계(전체/부분집합, 수준/차분, 같은 축의 양 끝)를 하나도
  수치로 확인하지 않았다 (00_읽는_법 §9.2). T2 묶음 14개 안에도 확인된 중복 쌍이 이 그룹에 둘 있다
  (`ev_amendment_ratio_1y`·`own_amendment_ratio_1y`, `own_major_stake`·`own_major_stake_chg`).

## 0. 그룹 한눈에 보기

대표 cell은 원문 해설 §4.4의 `peak_cell`이다. 등급이 cell마다 갈리면 대표 cell의 등급을 괄호에 적었다.
q5 spread는 기대 부호로 정렬한 값(`q5_spread_aligned`)이고, 양방향 family는 원값과 같다.

| # | family | primary feature | 무엇을 재나 | 기대→관측 부호 | 등급 | screen_pass | 대표 cell IC | 대표 q5 spread | 모델 사용 | 한 줄 메모 |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `ev_amendment_ratio` | `ev_amendment_ratio_1y` | 250거래일 공시 중 정정공시 비율 | `−` → `−` | A | 4/4 | −0.0568 (cum 0→120) | +2.65%p | T2 후보 14(validation 개선, holdout 대기) | Phase B 가장 깨끗. 5와 부분집합 관계 |
| 2 | `ev_filing_activity` | `ev_filing_burst_60d` | 60일 공시 건수 ÷ 자기 250일 중앙값 | 양방향 → `−` | A | 4/4 | −0.0142 (cum 0→60) | −0.58%p | T2 후보 14 | 크기 작음. placebo p 0.069 문턱 |
| 3 | `ev_net_share_issuance_yoy` | 동명 | 경제적 순발행 ÷ 1년 전 발행주식수 | `−` → `−` | A1/C3 | 1/4 | −0.0376 (cum 0→120, C) | −11.33%p(정렬) = **+11.33%p(원값)** | T2 후보 14 | IC와 spread 부호 정반대. coverage 0.479 |
| 4 | `ev_payout_yield` | 동명 | (현금배당 + 자사주 매입) ÷ PIT 시총 | `+` → `+` | B3/C1 | 3/4 | +0.1022 (cum 0→120, C) | +0.49%p | T2 후보 14 | \|IC\| 6위인데 spread 극소. revision 0.1116 |
| 5 | `own_amendment_ratio` | `own_amendment_ratio_1y` | 250거래일 지분 공시 중 정정 비율 | `−` → `−` | A | 4/4 | −0.0358 (cum 0→120) | NaN | T2 후보 14 | ICIR −1.417 최고. q5 계산 불가(동점) |
| 6 | `own_insider_filing_activity` | `own_insider_filing_burst_60d` | 60일 임원·주요주주 공시 ÷ 250일 중앙값 | 양방향 → 불명 | C2/D2 | 0/4 | −0.00022 (cum 0→60) | +0.26%p | 미투입 | 신호 없음. coverage 0.253 최저 |
| 7 | `own_major_filing_activity` | `own_major_filing_60d` | 60일 5% 대량보유 공시 건수 | 양방향 → `−` | A | 4/4 | −0.0428 (cum 0→60) | −4.24%p | T2 후보 14 | coverage 1.000. 규모 편향. I13 대상 |
| 8 | `own_major_stake_change` | `own_major_stake_chg` | 최대주주 지분율 전년 대비 %p 변화 | 양방향 → `+` | B(상한) | 4/4 | +0.0416 (cum 0→120) | +2.13%p | T2 후보 14 | 게이트 전부 통과, 원천 상한으로 B |
| 9 | `own_major_stake_level` | `own_major_stake` | 최대주주 지분율(%) | 양방향 → `+` | B2/C2 | 2/4 | +0.0335 (cum 0→120, C) | +1.78%p | T2 후보 14 | 8(변화)이 모든 지표에서 낫다 |

baseline 40 = `feat_price` px 15개 + `feat_flow` flow 15개 + `feat_fin_pit` fin 10개
(spec `feature_groups=("px","flow","fin")`). 이 그룹은 baseline에 하나도 없다.

T2 후보 14 묶음의 validation 결과는 아홉 카드에 공통이다 (`phase_b_acceptance_gate_results.md`).

| horizon | baseline Rank IC → 후보 | Rank IC Δ | 비용 반영 spread Δ |
|---|---|---:|---:|
| 5 | 0.1155 → 0.1186 | **+0.0031** | **+0.0017** |
| 20 | 0.1436 → 0.1447 | **+0.0011** | **+0.0030** |
| 60 | 0.1753 → 0.1755 | **+0.0003** | **+0.0080** |

status `improved_all_horizons`. 14개를 **한꺼번에** 넣은 결과라 개별 기여도는 측정하지 않았다.
최종 h60 holdout은 2026년 10~11월 이후 한 번만 연다 (00_status §8.7).

---

## 1. `ev_amendment_ratio` — 정정공시 비율

| 항목 | 값 |
|---|---|
| primary feature | `ev_amendment_ratio_1y` |
| secondary / variant | `—`(secondary 없음), `ev_amendment_ratio_1y_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_filing_activity` / `research/etl/features/filing_activity.py:243` |
| 원천 raw 테이블 | `dart_filing_receipt_raw` (1,201,866행 · 2015-01~2026-08 · 2,657종목, 18 §2.6) |
| prod 갱신 | 매일 23:30 KST `sdc_daily_opendart_filings` (v0.11.5, current universe + 14일 lookback, 00_status §8.5) |
| 분류 좌표 | C4(이벤트·공시) × T0(수준) × U (18 §3.4) |
| 검증 phase / fdr_family / role | B / event / `phase_b_blocked` — 사전등록 `expansion_20260827` (`outcome_blind: true`) |
| 기대 부호 → 관측 부호 | `−` → `−` |
| 사전등록 primary horizon | [60, 120] (bucket 포함), exploratory [20, 40] |
| 관측 신호 밴드 · 모양 | 60~120일 · peak `cum 0→120` · 누적·구간 \|IC\| 모두 관측 범위 끝까지 증가 (Phase B는 decay 요약 없음, 18 §4.4) |
| 등급 / screen_pass / discovery | A / 통과 4/4 / 4/4 cell, `failed_gates = []` |
| 모델 검증 이력 | T2 후보 14 묶음 → `improved_all_horizons` (§0 표) → h60 holdout 대기. 개별 기여도 미측정 (18 §9) |

### 산식과 규칙
```sql
-- filing_activity.py:243
amendments_250d / NULLIF(filings_250d, 0) AS ev_amendment_ratio_1y
```
- 창 `RATIO_WINDOW = 250` 거래일(`ROWS BETWEEN 249 PRECEDING`). 회사당 공시가 연 8건 안팎이라 창이 짧으면
  비율이 0 아니면 1로 튄다 (18 §2.3).
- 정정 판정은 `report_nm`에 `("기재정정", "첨부정정", "첨부추가", "변경등록")` 중 하나가 들어가면 참
  (`phase_b_quality.py:40`의 `AMENDMENT_MARKERS`를 그대로 공유). list.json `rm`의 `정` 플래그(나중에
  정정됨)와 다르다 — 이 피쳐는 "이 공시가 정정본이다"를 센다 (18 §2.2).
- 공시가 없는 날을 0으로 채워 `ROWS` 창이 거래일을 세게 한다 (18 §2.5).
- 분모 0이면 NULL. "정정 안 함"이 아니라 "모른다"다 (코드 주석).
- 단위: 비율(0~1). 표준화 없음. `formula_version: filing_v3`.

### 가용 시점(PIT)과 지연
- 접수일 D의 공시는 **D+1 거래일부터** 노출한다. DART가 하루 종일 공시를 받으므로 당일 노출은 저녁 공시가
  그날 오후 수익률을 예측하는 꼴이 된다 (18 §2.4, `filing_activity.py:170`).
- lag1 유지율: `미측정`(해설 문서에 기록 없음. 정본이 `native_t`라 PIT 지연이 이미 산식 안에 있다).
- 운영 지연: 원천이 매일 23:30 갱신되므로 D일 공시가 D+1 장중 추론에 쓰이려면 그날 밤 배치까지 기다린다.
  2026-08-29 이전에는 `dart_filing_receipt_raw`가 일회성 백필로만 채워졌다 (00_status §8.5).

### 예측력 — broad × common_survivor × native_t
부호가 `−`이므로 q5 spread는 정렬값이다. 양수면 기대대로다.

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | −0.0435 | −1.065 | −8.98 | +1.05%p | ~0 | discovery + screen-pass (A) |
| cum | 0→120 | **−0.0568** | **−1.357** | −8.96 | **+2.65%p** | ~0 | discovery + screen-pass (A) |
| bucket | 40→60 | −0.0285 | −0.700 | −9.93 | +0.33%p | ~0 | discovery + screen-pass (A) |
| bucket | 60→120 | −0.0431 | −1.044 | −8.59 | +1.37%p | ~0 | discovery + screen-pass (A) |

(18 §4.1) IC와 spread가 네 cell 전부 같은 방향이다. 어긋남 없음.
Phase B `family_summary.parquet`의 discovery 0·등급 NE는 AB 결합 전 placeholder다. 판정은 AB
`combined_ab_primary_hypotheses.parquet`에서 읽는다 (18 §4.2).

### 강건성
| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 0.992 / **1.011** / 0.977 / **1.009** (cum60 / cum120 / b40→60 / b60→120) | 기준 0.50 통과 |
| lag1 유지율 | 미측정 | 기준 0.50 |
| available 표본 부호 | `available_direction_pass = True` (4 cell) | 유지 |
| 기간 일관성 | 5/5 (4 cell 전부) | 통과 |
| 시간 placebo | cum60 **0.0198**, cum120 **0.0099**, b60→120 **0.0099** 통과 · b40→60 대상 아님(NW lag 19) | `robustness_required` 3 cell 받았다 |
| 비중첩 offset | `complete` · `nonoverlap_robustness_pass = True` (3 cell) · b40→60 대상 아님 | 통과 |
| source 경고 (Phase B) | `not_applicable` — 접수 이력은 사후 수정되지 않는 사실 기록 | 없음 |

(18 §5) Phase B에서 시간 placebo를 받고 전부 최솟값 근처로 통과한 family는 `fin_log_mcap`,
`ev_amendment_ratio`, `own_amendment_ratio` 셋이다 (31 §5.2).

### 표본과 커버리지
| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2015-01-05 ~ 2025-02-05 |
| 유효 거래일 | 2,478일 |
| 날짜당 종목 수 | 1,038~1,041개 |
| coverage_ratio(Phase B) | **0.894** (관측 6,150,482행) |
| KOSPI/KOSDAQ | KOSDAQ 0.946 (1,773종목, 날짜당 중앙값 1,178) / KOSPI 0.817 (838종목, 789) — KOSPI가 13%p 낮고 원인 미분석 |
| 연도별 | 2015 0.872 → 2016부터 0.94~0.95 (250일 창 warm-up) |

(18 §6)

### 중복성
- A×B 교차: `px_idio_vol_60d` **+0.140**(유효일 2,334, 범위 +0.01~+0.23), `px_near_52w_high` **−0.137**
  (2,478, −0.22~−0.01), `px_maxret_20d` +0.094, `px_mom_12_1` −0.049. 앞의 둘은 범위가 한쪽 부호로만 몰려
  안정적이고, 부호를 맞추면 같은 방향으로 작동한다 (18 §7).
- 구조적 관계: `feat_filing_activity` 다섯 family(1·2·5·6·7)의 형제. **5(`own_amendment_ratio`)와는
  분자·분모가 모두 부분집합 관계**다 (18 §7, 31 §7).
- **미확인 중복**: B×B 상관 산출물이 없다. 다섯을 독립 발견으로 세면 안 된다.

### 모델 투입 메모
- 변환: `(거래일, 시장)` 내 rank. 분모가 연 8건 안팎이라 값이 1/8, 2/8 같은 이산값으로 몰리고 0이 많다 —
  rank 동점이 크다. 필요하면 `정정 있음(>0)` indicator + rank를 분리한다.
- 결측: 분모 0(250일 공시 없음)이면 NULL이고 0과 뜻이 다르다. 2015년은 warm-up으로 coverage 0.872.
  indicator를 두고 drop하지 않는다.
- 유의점: 250일 창의 느린 지표라 회전이 낮을 것으로 보이지만 **회전율은 재지 않았다** (18 §4.3).
  KOSPI 쪽 표본이 얇다.
- horizon: primary [60, 120]에서 \|IC\|가 120에서 최대. **h60·h120 실험용**이다. h5·h20은 exploratory
  [20, 40]로만 등록됐고 검정하지 않았다. 120 너머는 안 봤다.
- 조합 후보: 규모 통제 증분(미측정), `px_idio_vol_60d`와의 중복 통제, 정정 종류(기재/첨부/변경등록) 분리,
  공시 중요도 플래그(`ev_material_event_flag`는 의도적으로 미구현), 업종 중립화. **5와 함께 넣기 전에
  B×B 상관을 먼저 본다.**

### 한계
- 형제 다섯의 중복 미확인, 특히 5와 부분집합 (18 §8.1).
- KOSPI 커버리지 13%p 낮음, 원인 미분석 (18 §8.2). 120일 너머 미관측 (18 §8.3).
- 회전율·거래비용 미측정 (18 §8.4). 정정 종류·공시 중요도 미구분 (18 §8.5~8.6).
- 업종 중립화 없음. 종목·시점 귀속 없음. holdout 미개봉 (18 §8.7~8.9).

---

## 2. `ev_filing_activity` — 공시 건수 급증 (filing burst)

| 항목 | 값 |
|---|---|
| primary feature | `ev_filing_burst_60d` |
| secondary / variant | `ev_filing_count_60d`, `ev_filing_count_120d`, `ev_filing_burst_120d`(secondary, 등록됐지만 이번 run 미실행) · `ev_filing_burst_60d_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_filing_activity` / `filing_activity.py:118`(건수), `:136`(burst) |
| 원천 raw 테이블 | `dart_filing_receipt_raw` |
| prod 갱신 | 매일 23:30 KST (카드 1과 같다) |
| 분류 좌표 | C4(이벤트·공시) × T2(놀라움) × U (19 §3.4) |
| 검증 phase / fdr_family / role | B / event / `phase_b_blocked` — `expansion_20260827` |
| 기대 부호 → 관측 부호 | 없음(`expected_sign: null`, 양방향) → `−` |
| 사전등록 primary horizon | [20, 60] (bucket 포함), exploratory [1, 5, 10, 40, 120] |
| 관측 신호 밴드 · 모양 | 20~60일 · peak `cum 0→60` · 누적 0.009→0.014, 구간 0.005→0.007로 증가 (19 §4.4) |
| 등급 / screen_pass / discovery | A / 통과 4/4 / 4/4 cell, `failed_gates = []` |
| 모델 검증 이력 | T2 후보 14 묶음 → `improved_all_horizons` → holdout 대기. 단변량 크기가 작아 묶음 안 비중도 작았을 수 있다 (19 §9) |

### 산식과 규칙
```sql
-- filing_activity.py:118 / :136
SUM(filings) OVER (PARTITION BY ticker, market ORDER BY trade_date
                   ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ev_filing_count_60d;
CASE WHEN session_ordinal >= 250 THEN
    ev_filing_count_60d / NULLIF(quantile_cont(ev_filing_count_60d, 0.5) OVER (
        PARTITION BY ticker, market ORDER BY trade_date
        ROWS BETWEEN 249 PRECEDING AND CURRENT ROW), 0)
END AS ev_filing_burst_60d
```
- 60일 건수를 **그 회사 자신의 250일 중앙값**으로 나눈다. 1이면 평소 속도, 2면 두 배. 규모 축을 상쇄하려는
  설계로 `px_turnover_shock`와 같다 (19 §2.2).
- `BURST_BASELINE = 250`: `session_ordinal < 250`이면 NULL. 중앙값이 0이면 `NULLIF`로 NULL.
- `WINDOWS = (60, 120)`은 사전등록 값이라 결과를 보고 고를 수 없다 (19 §2.4).
- 공시 종류를 구분하지 않는다. 유상증자 공시와 정기보고서가 같은 1건이다.
- 단위: 비율(무차원). `formula_version: filing_v3`.

### 가용 시점(PIT)과 지연
- 접수일 D → D+1 거래일 노출, 0 채우기 (카드 1과 동일, 19 §2.5).
- lag1 유지율: `미측정`.
- 250일 baseline 때문에 유효 시작이 2015-07-06으로 카드 1보다 6개월 늦다 (19 §2.3).

### 예측력 — broad × common_survivor × native_t
양방향이라 `q5_spread_aligned`가 원값과 같다. 음수면 상위 20%(공시 급증)가 덜 올랐다.

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | −0.0089 | −0.239 | −3.13 | −0.28%p | 0.0029 | discovery + screen-pass (A) |
| cum | 0→60 | **−0.0142** | −0.373 | −3.23 | **−0.58%p** | 0.0021 | discovery + screen-pass (A) |
| bucket | 10→20 | −0.0054 | −0.157 | −2.91 | −0.12%p | 0.0057 | discovery + screen-pass (A) |
| bucket | 40→60 | −0.0067 | −0.222 | −3.16 | −0.16%p | 0.0026 | discovery + screen-pass (A) |

(19 §4.1) IC와 spread가 네 cell 전부 같은 방향(둘 다 음수)이다. 어긋남 없음.
\|IC\| 0.0142는 카드 1(0.0568)의 4분의 1이다. t가 −2.9~−3.2로 유의한 것은 표본이 2,354일로 길기 때문이다.
**유의성과 크기를 분리해서 봐야 한다** (19 §4.2).

### 강건성
| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 1.095 / **1.147** / 1.123 / **1.202** (cum20 / cum60 / b10→20 / b40→60) — 네 cell 전부 1 초과 | 통과 |
| lag1 유지율 | 미측정 | 기준 0.50 |
| available 표본 부호 | `True` (4 cell) | 유지 |
| 기간 일관성 | 5/5 (4 cell, 관측 부호 `−` 기준) | 통과 |
| 시간 placebo | cum60 **0.0693** 통과(기준 0.10에 근접) · 나머지 셋 대상 아님(NW lag < 59) | `robustness_required` 1 cell만 받았다 |
| 비중첩 offset | cum60 `complete`·`True` · 나머지 셋 `robustness_required = false` | 통과 |
| source 경고 (Phase B) | `not_applicable` | 없음 |

(19 §5) 강건성 검사를 받은 cell이 하나뿐이고 그 p가 0.0693이다. 100번 placebo 중 여섯 번이 관측값만큼
극단적이었다.

### 표본과 커버리지
| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2015-07-06 ~ 2025-02-05 |
| 유효 거래일 | 2,354일 |
| 날짜당 종목 수 | 998~999개 |
| coverage_ratio(Phase B) | **0.821** (관측 5,647,906행) |
| KOSPI/KOSDAQ | KOSDAQ 0.858 (1,694종목, 중앙값 1,081) / KOSPI 0.766 (826, 776) — KOSPI 9%p 낮음 |
| 연도별 | **2015 0.376**, 2016 0.874, 2017~2026 0.89~0.92 |

(19 §6) 카드 1보다 7%p 낮은 이유는 250일 창을 두 번(60일 건수 + 250일 중앙값) 거치기 때문이다.

### 중복성
- A×B 교차: `px_idio_vol_60d` +0.071 (2,334), `px_resid_mom_12_1` +0.061 (2,207), `px_mom_12_1` +0.054
  (2,354). 전부 0.08 미만 — Phase A와 거의 독립 (19 §7).
- 구조적 관계: 카드 6(`own_insider_filing_activity`)과 **같은 CASE 문**(`filing_activity.py:130`의
  `burst_columns` 루프)에서 나온다. 차이는 분자에 세는 공시 종류뿐이고 전체가 부분을 포함한다 (19 §7).
- **미확인 중복**: B×B 없음. secondary 셋(건수 형태·120일 창)과의 관계도 안 봤다 (19 §2.4).

### 모델 투입 메모
- 변환: 1을 중심으로 오른콝 꼬리가 긴 비율. `(거래일, 시장)` 내 rank. 60일 건수가 0이면 값이 정확히 0으로
  몰린다 — rank 동점이 크다.
- 결측: (a) `session_ordinal < 250` warm-up, (b) 250일 중앙값 0인 조용한 회사. 둘 다 NULL이고 "평소와
  같다(=1)"와 다르다. indicator를 둔다.
- 유의점: **I13** — 건수·비율형 피쳐는 이력 시작 구간에서 횡단면이 통째로 0이 될 수 있다.
  `10_known_issues.md` §9.9가 이 family를 같은 성질로 지목했다(이번 표본에서는 차이 없음). 2015년 초 표본을
  쓸 때 상수 횡단면 여부를 확인한다.
- horizon: primary [20, 60], peak 60. **h20·h60 실험용**. h5·h10은 exploratory로만 등록. 120은 미검정.
- 조합 후보: 건수 형태(`ev_filing_count_60d`)와 비율 형태 중 어느 쪽이 신호인지 확인(미실행), 급증 ×
  정정 비율(카드 1) 상호작용, 공시 중요도 분류(미구현), 업종 중립화.

### 한계
- 크기가 작다 (19 §8.1). placebo가 문턱 근접, 강건성 검사 cell이 하나 (19 §8.2~8.3).
- 형제 다섯 중복 미확인, 특히 카드 6과 부분집합 (19 §8.4). secondary 셋 미실행 (19 §8.5).
- 공시 종류 미구분, KOSPI 커버리지 낮음, 60일 너머 미관측, 업종 중립화 없음, holdout 미개봉 (19 §8.6~8.11).

---

## 3. `ev_net_share_issuance_yoy` — 순주식발행 증가율

| 항목 | 값 |
|---|---|
| primary feature | `ev_net_share_issuance_yoy` |
| secondary / variant | `—`(secondary 없음) · `ev_net_share_issuance_yoy_lag1`(lag1) · 보조 `issuance_identity_ok`, `issuance_classification_complete`, `issuance_available_from` |
| 마트 / 산식 위치 | `feat_event_scan_daily` / `research/etl/features/event_scan.py:436` |
| 원천 raw 테이블 | `dart_capital_change_raw`(irdsSttus, `dart sync-share-info`가 함께 수집), `dart_share_count_raw`(`istc_totqy`, 누적 증가·감소 총량) |
| prod 갱신 | 04:00 OpenDART 체인(`sync-share-info`) — 연 1회 정기보고서 기준 (docs/operations.md) |
| 분류 좌표 | C4(이벤트·공시) × T1(변화) × U (11 §3 #24) |
| 검증 phase / fdr_family / role | B / event / `phase_b_blocked` — 기본 config `horizon_scan_config.yaml:498` |
| 기대 부호 → 관측 부호 | `−` → `−` |
| 사전등록 primary horizon | [60, 120] (bucket 포함), exploratory [20, 40] |
| 관측 신호 밴드 · 모양 | 60~120일 · peak `cum 0→120` · 누적 0.030→0.038, 구간 0.022→0.025 증가 (20 §4.4) |
| 등급 / screen_pass / discovery | **A 1 / C 3** / 통과 1/4 / 4/4 cell · `failed_gates = [robustness_pass]` (3 cell) — Phase B의 C(강건성 실패) |
| 모델 검증 이력 | T2 후보 14 묶음 → `improved_all_horizons` → holdout 대기. **3/4 cell이 screen 실패인데도 후보에 들어갔다** — 후보 선정이 family 단위(`screen_pass` 1개 이상)였기 때문 (20 §9) |

### 산식과 규칙
```sql
-- event_scan.py:436
CASE WHEN has_prior_year AND unclassified_1y = 0
      AND ABS((istc_totqy_prior + economic_increase_1y + mechanical_increase_1y
               - economic_decrease_1y - mechanical_decrease_1y) - istc_totqy) <= {identity_tolerance}
      AND istc_totqy_prior > 0
     THEN (economic_increase_1y - economic_decrease_1y) / istc_totqy_prior
END AS ev_net_share_issuance_yoy
```
- 자본변동 사유(`isu_dcrs_stle`)를 경제적 증가(유상증자 4종·전환권·신주인수권·주식매수선택권·출자전환) /
  경제적 감소(`감자(유상)`, `유상감자`) / 기계적 증가(무상증자·주식배당·주식분할) / 기계적 감소(주식병합·
  `감자(무상)`·무상감자·소각)로 **정확 일치**만으로 분류한다. 부분 문자열·정규화 없음 (20 §2.3).
- 목록에 없는 사유가 1년 창에 하나라도 있으면 `unclassified` → 그 창 전체 NULL. 항등식이 맞지 않으면 NULL.
  `DEFAULT_ISSUANCE_IDENTITY_TOLERANCE = 0` (20 §2.4).
- 자유 텍스트 사유 칸(`redc`, `profit_incnr`, `rdmstk_repy`, `etc`)은 읽지 않는다.
- 단위: 비율(양수 = 발행, 음수 = 유상감자). `formula_version: issuance_v4`.
  v2에서 사유 4개 추가(그 전에는 연간 vintage 이벤트의 22.4%가 `unclassified`, v2 이후 4.3% — 09 §8),
  v3에서 `유상감자` 추가(**v3 전에는 경제적 감소가 아무것도 매칭되지 않았다**, 699행/88개 발행사),
  v4는 전순서 tiebreaker(I12) (20 §2.3).

### 가용 시점(PIT)과 지연
- `available_from` 기준 interval join (`iss.available_from <= trade_date < next_available_from`).
  발행 이력은 보고서 vintage마다 상장 이후 전체를 다시 주므로 **strict PIT vintage**를 쓴다 — 최신본을 쓰면
  9년 거리에서 값이 바뀌는 비율이 0.1824(임계 5%)였다. 이를 위해 `dart_capital_change_raw`를 71,535 →
  245,120행으로 추가 수집했다 (09 §8).
- vintage 나이: KOSDAQ 평균 52.8일 / 95분위 139일, KOSPI 53.9일 / 133일. **평균 53일 된 정보**다 (20 §2.5).
- lag1 유지율: `미측정`.

### 예측력 — broad × common_survivor × native_t
부호 `−`이므로 aligned 양수가 기대대로인데, **전부 음수**다. 원값은 괄호에 적었다.

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | −0.0300 | −0.646 | −5.09 | **−3.22%p** (원값 +3.22%p) | ~0 | discovery, screen 실패 (C) |
| cum | 0→120 | **−0.0376** | −0.757 | −4.39 | **−11.33%p** (원값 +11.33%p) | 0.00002 | discovery, screen 실패 (C) |
| bucket | 40→60 | −0.0221 | −0.514 | −6.60 | −2.20%p (원값 +2.20%p) | ~0 | **discovery + screen-pass (A)** |
| bucket | 60→120 | −0.0254 | −0.564 | −4.26 | −6.19%p (원값 +6.19%p) | 0.00004 | discovery, screen 실패 (C) |

(20 §4.1) **IC와 q5 spread의 부호가 정반대다. 35개 중 가장 큰 어긋남이다** (20 §4.3, 00_읽는_법 §9.1).
순위로는 발행 많은 쪽이 나쁘다(IC −). 그런데 상위 20% 평균은 11.3%p 더 올랐다 — 대규모 유상증자 집단의
수익률 분포가 오른쪽으로 극단적으로 치우쳐 소수의 대박이 평균을 끌어올린다는 해석이다. **중앙값 spread나
분위별 평균수익률을 산출하지 않아 어느 쪽이 맞는지 이 자료로 판정할 수 없다.** 순위 모델 입력이면 IC,
분위 롱숏이면 spread를 본다.

09 §8(`ab0de634…` 계보)도 대표 cell을 bucket 40→60 IC −0.0221 등급 A로 적었다. 값이 같다.

### 강건성
| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 1.081 / **1.115** / 1.033 / 1.093 (cum60 / cum120 / b40→60 / b60→120) | 통과 |
| lag1 유지율 | 미측정 | 기준 0.50 |
| available 표본 부호 | `True` (4 cell) | 유지 |
| 기간 일관성 | 5/5 (4 cell) | 통과 |
| 시간 placebo | cum60 **0.1386 실패**, cum120 **0.2178 실패**, b60→120 **0.2376 실패** · b40→60 대상 아님(NW lag 19) | 3 cell 실패 |
| 비중첩 offset | `complete`·`True` (3 cell) — 떨어진 이유는 offset이 아니라 placebo 하나 | 통과 |
| source 경고 (Phase B) | `not_applicable`, `grade_cap = None`. 단, v2·v3 사유 목록 이력이 남긴 경고는 별개 — 목록이 지금도 완전한지 알 수 없다 (20 §5.6) | 없음 |

(20 §4.2, §5) **살아남은 `bucket 40→60`은 placebo를 통과한 게 아니라 받지 않았다.** 등급 A를 이 사정을 알고
읽어야 한다.

### 표본과 커버리지
| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2016-04-15 ~ 2025-02-05 |
| 유효 거래일 | 2,145일 |
| 날짜당 종목 수 | **668개** (카드 1의 3분의 2, 5분위 칸당 130여 종목) |
| coverage_ratio(Phase B) | **0.479** (관측 3,452,470행) — Phase B 최저 수준 |
| KOSPI/KOSDAQ | KOSDAQ 0.473 / KOSPI 0.488 — 다른 event family와 달리 거의 같다 |

(20 §6) 09 §3(`ab0de634…`)의 2016-04-15 / 0.4787과 일치한다. 패널의 52%가 NULL이다 — `unclassified`,
항등식 불일치, 직전 연도 없음, `istc_totqy_prior <= 0`이 겹친 결과다. 규율을 지킨 대가다.

### 중복성
- A×B 교차: `px_near_52w_high` **−0.111** (2,145, −0.25~+0.19), `px_idio_vol_60d` **+0.103** (2,145,
  +0.00~+0.20, 전부 양수로 안정), `px_maxret_20d` +0.074, `px_mom_12_1` −0.065 (20 §7).
- 구조적 관계: 카드 4(`ev_payout_yield`)와 **같은 마트, 같은 축의 양 끝**(발행 대 환원). 음의 상관이
  예상되나 **재지 않았다**. 둘을 합친 `ev_net_payout_yield`가 원래 목표 형태였는데 만들지 않았다 (21 §3.4).
- **미확인 중복**: B×B 없음.

### 모델 투입 메모
- 변환: 대부분 0(1년간 경제적 발행 없음)이고 양쪽 꼬리가 있는 분포다. `(거래일, 시장)` 내 rank로 넣되
  **`발행 있음(>0)`/`감자 있음(<0)` indicator와 크기를 분리**하는 편이 맞다 — 0 동점 블록이 rank 중앙을
  차지한다.
- 결측: 52%가 NULL이고 원인이 넷이다(위 §표본). NULL을 0으로 채우면 "발행 없음"과 "분류 불가"가 섞인다.
  indicator 필수. `issuance_classification_complete`·`issuance_identity_ok`를 결측 원인 플래그로 함께 쓸 수 있다.
- 유의점: 연 1회 갱신되는 **계단형** 값(평균 53일 묵음). 상위 20%의 극단 분포(§예측력) — 랭킹 모델은 IC를
  따르지만, 확률 캘리브레이션에서 대규모 유상증자 종목의 꼬리를 따로 본다. 횡단면이 668종목이라 rank
  분해능이 낮다.
- horizon: primary [60, 120]. 통과 cell은 bucket 40→60 하나. **h60 실험에서 보조 피쳐로**, h120은 placebo
  실패를 알고 쓴다. h5·h20은 미검정.
- 조합 후보: `ev_payout_yield`와 합쳐 `ev_net_payout_yield`(사전 설계가 요구, 미구현), 규모 통제,
  `px_idio_vol_60d` 통제, 유상증자 종류(주주배정/제3자배정/일반공모) 분리.

### 한계
- IC·spread 정반대이고 풀 자료(중앙값 spread, 분위별 수익률)가 없다 — **가장 시급** (20 §8.1).
- 3 cell placebo 실패, 통과 cell은 미검사 (20 §8.2). coverage 0.479 (20 §8.3).
- 사유 목록 완전성 미확인(v2 22.4% 누락, v3 `유상감자` 추가 이력) (20 §8.4). 카드 4와 상관 없음 (20 §8.5).
- 평균 53일 묵은 정보, 업종 중립화 없음, holdout 미개봉 (20 §8.7~8.10).

---

## 4. `ev_payout_yield` — 주주환원 수익률 (배당 + 자사주)

| 항목 | 값 |
|---|---|
| primary feature | `ev_payout_yield` |
| secondary / variant | `—` · `ev_payout_yield_lag1`(lag1) · 보조 `cash_dividends_total`, `dividend_source`, `buyback_cash_ttm`, `payout_available_from` |
| 마트 / 산식 위치 | `feat_event_scan_daily` / `event_scan.py:710` |
| 원천 raw 테이블 | `dart_shareholder_return_raw`(alotMatter 배당 행), `stock_metric_vintage_fact`·`fin_quarterly_metric_vintage`(`dps`, TTM `treasury_share_acquisition_amount`), `market_cap_pit`(`daily_ohlcv` + `dart_share_count_raw`) |
| prod 갱신 | 04:00 OpenDART 체인(`sync-share-info`) — 연 1회 정기보고서 기준 |
| 분류 좌표 | C4(이벤트·공시) × T0(수준) × U (11 §3 #23) |
| 검증 phase / fdr_family / role | B / event / `phase_b_blocked` — 기본 config `horizon_scan_config.yaml:511` |
| 기대 부호 → 관측 부호 | `+` → `+` |
| 사전등록 primary horizon | [60, 120] (bucket 포함), exploratory [20, 40] |
| 관측 신호 밴드 · 모양 | 60~120일 · peak `cum 0→120` · 누적 0.080→0.102, 구간 0.055→0.080 증가 (21 §4.4) |
| 등급 / screen_pass / discovery | **B 3 / C 1** / 통과 3/4 / 4/4 cell · source quality `warn`(B 이유) · cum 0→120 `failed_gates = [robustness_pass]`(C) |
| 모델 검증 이력 | T2 후보 14 묶음 → `improved_all_horizons` → holdout 대기. 같은 묶음에 `fin_value_z`·`fin_log_mcap`·`mcap_krx_log`가 있어 밸류·규모 축과 겹친다 (21 §9) |

### 산식과 규칙
```sql
-- event_scan.py:710
CASE WHEN base_ok AND (cash_dividends_total IS NOT NULL OR buyback_cash_ttm IS NOT NULL)
      AND (COALESCE(cash_dividends_total,0) + COALESCE(buyback_cash_ttm,0)) / market_cap_pit <= {PAYOUT_YIELD_MAX}
     THEN (COALESCE(cash_dividends_total,0) + COALESCE(buyback_cash_ttm,0)) / market_cap_pit
END AS ev_payout_yield
```
- 현금배당 총액은 직접 총액 행(`CASH_DIVIDEND_TOTAL_ROW_NAME = "현금배당금총액(백만원)"`) 우선, 없으면
  **DPS × 배당대상주식수** 대체. **이 저장소는 총액 행이 실제로 있는 live payload를 한 번도 받아 본 적이
  없다** — 대부분 DPS 대체 경로일 가능성이 높은데 `dividend_source` 분포를 보지 않았다 (21 §2.3).
- 자사주 매입 현금은 B-3이 TTM 처리한 `treasury_share_acquisition_amount`.
- `PAYOUT_YIELD_MAX = 10`: 시총의 10배를 넘는 값은 단위 오류로 **클리핑하지 않고 NULL**로 버린다 (21 §2.4).
- `base_ok`: `market_cap_pit > 0`, 주식수 유효, 거래정지 아님, 유효 세션.
- 단위: 비율(≥0). `formula_version: payout_v3` (v2 DPS 역산 단위 대조·음수 거부 I5, v3 보통주 한정·전순서
  I12). 카드 3의 `issuance_v4`와 버전을 분리해 관리한다 (21 §2.5).

### 가용 시점(PIT)과 지연
- `payout_available_from` = 배당(총액 또는 DPS)과 자사주 `available_from` 중 **늦은 쪽** 기준 interval join.
  분모 `market_cap_pit`도 시점 정확 시총이다 (21 §2.6).
- lag1 유지율: `미측정`.
- **source 경고**: 자사주 취득금액의 `revision_ratio = 0.1116` — 9건 중 1건꼴로 사후 정정됐다. 이번 scan은
  최종본을 쓰므로 그 시점에 알 수 있었던 값보다 정확한 값을 쓴 셈이다 (21 §5.6).

### 예측력 — broad × common_survivor × native_t
| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | +0.0798 | 1.023 | 7.58 | +0.09%p | ~0 | discovery + screen-pass (B) |
| cum | 0→120 | **+0.1022** | **1.212** | 6.36 | **+0.49%p** | ~0 | discovery, screen 실패 (C) |
| bucket | 40→60 | +0.0550 | 0.660 | 8.88 | **−0.08%p** | ~0 | discovery + screen-pass (B) |
| bucket | 60→120 | +0.0795 | 1.075 | 7.93 | +0.22%p | ~0 | discovery + screen-pass (B) |

(21 §4.1) **\|IC\| 0.1022는 35개 중 6위인데 q5 spread는 +0.49%p다.** 같은 120일의 `px_amihud_20d`(0.134 /
+11.21%p), `fin_log_mcap`(0.115 / +11.83%p)와 비교하면 약 1/23이다. `bucket 40→60`은 IC +0.055인데 spread가
**−0.08%p로 부호까지 갈린다** (21 §4.3, 00_읽는_법 §9.1). 해석: 주주환원이 큰 종목은 안정적 대형 배당주라
수익률 분산 자체가 작다 — **순위 신호로는 강하지만 단독 롱숏 알파원으로는 약하다.**

09 §8(`ab0de634…`)은 대표 cell을 bucket 40→60 IC +0.0550 B로 적어 같고, 누적 0→120 IC를 0.1021로 적었다
(해설 문서는 +0.1022). 이 문서는 해설 문서 값을 따른다.

### 강건성
| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | **1.096** / **1.091** / 1.071 / 1.068 (cum60 / cum120 / b40→60 / b60→120) | 통과 |
| lag1 유지율 | 미측정 | 기준 0.50 |
| available 표본 부호 | `True` (4 cell) | 유지 |
| 기간 일관성 | 5/5 (4 cell) | 통과 |
| 시간 placebo | cum60 **0.0792** 통과, b60→120 **0.0792** 통과, cum120 **0.1782 실패** · b40→60 대상 아님 | 통과 둘도 여유 작음 |
| 비중첩 offset | `complete`·`True` (3 cell) | 통과 |
| source 경고 (Phase B) | **`warn`** · reason `revision` · `revision_ratio 0.1116` (`treasury_share_acquisition_amount`) · `mapping_fallback_ratio 0.000`(`issued_shares`) · `pairing_mismatch_ratio 0.000125` · `grade_cap None` | 비치명 → screen-pass cell이 A 아닌 **B** |

(21 §5) `mapping_fallback_ratio 0.000`은 재무 계열(`fin_gross_profitability` 0.944)과 대조되는 좋은 신호다.

### 표본과 커버리지
| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2015-12-28 ~ 2025-02-05 |
| 유효 거래일 | 2,175일 |
| 날짜당 종목 수 | 987~990개 |
| coverage_ratio(Phase B) | **0.709** (관측 5,113,246행) |
| KOSPI/KOSDAQ | 미측정(해설 문서에 시장별 값 없음) |

(21 §6) 09 §3(`ab0de634…`)의 2015-12-28 / 0.7090과 일치한다. 패널의 29%가 NULL — 배당도 자사주도 없는 회사,
단위 오류 거부, 공시 전 구간.

### 중복성
- A×B 교차: `px_idio_vol_60d` **−0.347** (2,175, **−0.43~−0.23**), `px_maxret_20d` **−0.241** (−0.37~−0.09),
  `px_near_52w_high` **+0.223** (+0.05~+0.35), `px_reversal_5d` −0.027. 앞의 셋은 204쌍 중 절대값 상위권이고
  범위가 한쪽 부호로만 몰려 **날짜와 무관하게 안정적으로 겹친다** (21 §7). 부호를 맞추면 같은 방향으로
  작동하므로 함께 넣으면 증분이 줄어들 가능성이 크다.
- 구조적 관계: 카드 3과 같은 축의 반대편(상관 미측정). 분모가 시총이라 `fin_value_z`와 겹칠 수밖에
  없는데 **B×B 없음** (21 §7).
- **미확인 중복**: `fin_value_z`, `fin_log_mcap`, `mcap_krx_log`, 카드 3.

### 모델 투입 메모
- 변환: 비양수 없음(≥0), 오른콝 꼬리. `(거래일, 시장)` 내 rank로 충분하다. 로그는 rank 뒤에는 불필요.
- 결측: 29% NULL. **배당·자사주 행이 하나도 없는 회사는 0이 아니라 NULL**이다(산식 조건). "환원 안 함"을
  0으로 채우고 싶다면 그 판단을 별도 규칙으로 두고 indicator를 남긴다. 단위 오류 거부(>10배)도 NULL이다.
- 유의점: 연 1회 갱신 **계단형**. 밸류·저변동성 축과 안정적으로 겹친다(−0.347) — `fin_value_z`·규모를
  넣은 뒤의 증분을 먼저 본다. 자사주 금액 11.2% 사후 정정 → 최종본 사용의 미래 정보 여지.
- horizon: primary [60, 120]. **h60 실험**에 맞다(cum 0→60 B, placebo 0.0792). h120은 \|IC\| 최대 cell이
  placebo 실패. h5·h20 미검정.
- 조합 후보: 카드 3과 합친 `ev_net_payout_yield`(사전 설계 목표, 미구현), `fin_value_z` 통제 후 증분,
  배당 대 자사주 분리(`dividend_source` 분포 확인부터), 업종 중립화(업종별 배당 성향 차이).

### 한계
- IC 크고 spread 작음 — 분위별 수익률 자료 없음 (21 §8.1). `revision_ratio 0.1116` (21 §8.2).
- 배당 총액 행 매칭 실적 없음, `dividend_source` 분포 미확인 (21 §8.3). \|IC\| 최대 cell placebo 실패 (21 §8.4).
- 밸류·변동성 축과 겹침, 통제 후 증분 미측정 (21 §8.5). `ev_net_payout_yield` 미구현 (21 §8.6).
- 업종 중립화 없음, holdout 미개봉 (21 §8.8~8.10).

---

## 5. `own_amendment_ratio` — 지분 공시 정정 비율

| 항목 | 값 |
|---|---|
| primary feature | `own_amendment_ratio_1y` |
| secondary / variant | `—` · `own_amendment_ratio_1y_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_filing_activity` / `filing_activity.py:245`(산식), `:157`(지분 공시 분류), `:69`·`:73`(공시 종류 상수) |
| 원천 raw 테이블 | `dart_filing_receipt_raw` |
| prod 갱신 | 매일 23:30 KST (카드 1과 같다) |
| 분류 좌표 | C3(수급·소유·내부자) × T0(수준) × U (31 §3.4) — 11 §2.1이 C3의 빈 칸으로 지목한 「내부자·최대주주」 |
| 검증 phase / fdr_family / role | B / ownership / `phase_b_blocked` — `expansion_20260827` |
| 기대 부호 → 관측 부호 | `−` → `−` |
| 사전등록 primary horizon | [60, 120] (bucket 포함), exploratory [20, 40] |
| 관측 신호 밴드 · 모양 | 60~120일 · peak `cum 0→120` · 누적 0.027→0.036, 구간 0.018→0.026 증가 (31 §4.4) |
| 등급 / screen_pass / discovery | A / 통과 4/4 / 4/4 cell, `failed_gates = []` |
| 모델 검증 이력 | T2 후보 14 묶음 → `improved_all_horizons` → holdout 대기. **같은 묶음에 카드 1이 함께 들어 있다**(부분집합 관계) (31 §9) |

### 산식과 규칙
```sql
-- filing_activity.py:245 / :157
COUNT(*) FILTER (WHERE is_insider OR is_major_holder)                     AS ownership_filings,
COUNT(*) FILTER (WHERE is_amendment AND (is_insider OR is_major_holder))  AS ownership_amendments;
own_amendments_250d / NULLIF(own_filings_250d, 0) AS own_amendment_ratio_1y
```
- 지분 공시 = `report_nm`에 `INSIDER_MARKER = "주요주주특정증권등소유상황보고서"`(139,697건, 2,607종목) 또는
  `MAJOR_HOLDER_MARKER = "주식등의대량보유상황보고서"`(101,656건, 일반+약식)가 포함. 보고서명의 `ㆍ`가
  U+318D라 부분 문자열 매칭 (31 §2.2).
- 정정 마커는 카드 1과 같다. 창 250거래일, 0 채우기, 분모 0이면 NULL. `formula_version: filing_v3`.
- 분모가 카드 1보다 훨씬 작다 — 분모 0 회사가 많고(coverage 0.754) **정확히 0인 값이 대량으로 몰린다**
  (31 §2.5).

### 가용 시점(PIT)과 지연
- 접수일 D → D+1 거래일 노출 (카드 1과 동일).
- lag1 유지율: `미측정`.

### 예측력 — broad × common_survivor × native_t
| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | −0.0273 | −0.996 | −9.06 | **NaN** | ~0 | discovery + screen-pass (A) |
| cum | 0→120 | **−0.0358** | **−1.417** | **−10.28** | **NaN** | ~0 | discovery + screen-pass (A) |
| bucket | 40→60 | −0.0176 | −0.597 | −8.55 | **NaN** | ~0 | discovery + screen-pass (A) |
| bucket | 60→120 | −0.0257 | −0.921 | −8.02 | **NaN** | ~0 | discovery + screen-pass (A) |

(31 §4.1) **AB 유효 147개 cell 중 q5 spread가 NaN인 것은 이 family의 4개가 전부다** (31 §4.3). 산출물에
이유가 기록돼 있지 않다. 산식(`metrics.py:230`)에서 유도한 추론은 **동점**이다 — 값의 80% 이상이 정확히
0이면 평균 순위 동점 블록이 하위 20%를 삼켜 `bottom`이 빈 배열이 되고 그날 횡단면이 통째로 버려진다.
**확인된 사실이 아니라 추론이다.** IC는 Spearman이 동점을 평균 순위로 처리하므로 유효하다.
→ "신호는 있는데 5분위 롱숏으로는 구현할 수 없는 피쳐"다. ICIR −1.417, t −10.28은 Phase B 최고 수준이다 (31 §4.2).

### 강건성
| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 1.010 / 1.036 / 1.014 / 1.049 (cum60 / cum120 / b40→60 / b60→120) | 통과 |
| lag1 유지율 | 미측정 | 기준 0.50 |
| available 표본 부호 | `True` (4 cell) | 유지 |
| 기간 일관성 | 5/5 (4 cell) | 통과 |
| 시간 placebo | cum60 **0.0099**, cum120 **0.0099**, b60→120 **0.0099** 전부 최솟값 통과 · b40→60 대상 아님(NW lag 19) | `robustness_required` 3 cell 받았다 |
| 비중첩 offset | `complete`·`True` (3 cell) | 통과 |
| source 경고 (Phase B) | `not_applicable` | 없음 |

(31 §5)

### 표본과 커버리지
| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2015-01-05 ~ 2025-02-05 |
| 유효 거래일 | 2,478일 |
| 날짜당 종목 수 | 877~880개 (카드 1보다 160개 적다) |
| coverage_ratio(Phase B) | **0.754** (관측 5,187,210행) |
| KOSPI/KOSDAQ | KOSDAQ 0.790 / KOSPI 0.700 |

(31 §6) 지분 공시를 한 건도 안 낸 회사는 분모 0으로 값이 없다 — 패널의 25%.

### 중복성
- A×B 교차: `px_idio_vol_60d` +0.094 (2,334, +0.02~+0.15), `px_near_52w_high` −0.066, `px_maxret_20d` +0.058,
  `px_amihud_20d` −0.049. 전부 0.10 미만 — 지분 공시로 좁히면 카드 1보다 가격 계열과 덜 얽힌다 (31 §7).
- 구조적 관계: **카드 1의 분자·분모 모두의 부분집합**이다. 두 family의 결과도 비슷하다(IC −0.0568 vs
  −0.0358, ICIR −1.357 vs −1.417, 둘 다 4/4 A, placebo 0.0099). 결합 BH에서 8개 가설로 따로 세어졌다 (31 §7).
  카드 6·7의 분자(임원 공시, 5% 공시)가 이 family의 분모 `ownership_filings`에 들어간다 (32 §7, 33 §7).
- **미확인 중복**: B×B 없음. 두 발견이 사실상 하나일 수 있다.

### 모델 투입 메모
- 변환: `(거래일, 시장)` 내 rank. **0 동점 블록이 지배적**이라 rank만으로는 정보가 상위 꼬리에만 있다.
  `지분 공시 정정 있음(>0)` indicator + (>0 구간의) rank로 나누는 것이 맞다. 5분위 컷은 성립하지 않으므로
  분위 기반 후처리(분위 더미 등)를 쓰지 않는다.
- 결측: 25%가 분모 0 NULL. "지분 공시 없음"은 정보일 수 있으니 indicator로 남긴다.
- 유의점: 모델은 5분위가 아니라 전체 순위를 쓰므로 spread NaN이 투입 자체를 막지는 않는다 (31 §9).
  **I13**: 상수 횡단면이 측정에서 1개 확인됐다(`own_amendment_ratio_1y` 1, 10 §9.9). 판정 영향 없음.
- horizon: primary [60, 120], peak 120. **h60·h120 실험용**. h5·h20 미검정.
- 조합 후보: **카드 1과 둘 중 하나만 넣거나 차이(전체 정정 비율 − 지분 정정 비율)를 본다** — B×B 상관을
  먼저 계산한다. 임원 공시 정정과 5% 공시 정정 분리(누가 정정했는지 미구분), 규모 통제.

### 한계
- q5 spread 계산 불가 — 경제적 크기를 알 수 없고 분위 롱숏 구현 불가. 원인은 추정 (31 §8.1).
- 카드 1과 부분집합, B×B 없음 (31 §8.2). 정정 종류·정정 주체 미구분 (31 §8.3~8.4).
- 분모 0 회사 25% (31 §8.5). 120일 너머 미관측, 업종 중립화 없음, holdout 미개봉 (31 §8.6~8.9).

---

## 6. `own_insider_filing_activity` — 임원·주요주주 공시 급증

| 항목 | 값 |
|---|---|
| primary feature | `own_insider_filing_burst_60d` |
| secondary / variant | `own_insider_filing_60d`, `own_insider_filing_120d`, `own_insider_filing_burst_120d`(secondary, 미실행) · `own_insider_filing_burst_60d_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_filing_activity` / `filing_activity.py:122`(건수), `:136`(burst) |
| 원천 raw 테이블 | `dart_filing_receipt_raw` — `elestock` 이벤트를 접수 건수로 대체 (32 §2.2) |
| prod 갱신 | 매일 23:30 KST (카드 1과 같다) |
| 분류 좌표 | C3(수급·소유·내부자) × T2(놀라움) × U (32 §3.3) |
| 검증 phase / fdr_family / role | B / ownership / `phase_b_blocked` — `expansion_20260827` |
| 기대 부호 → 관측 부호 | 없음(양방향) → **불명** (누적 `−`, 구간 `+`) |
| 사전등록 primary horizon | [20, 60] (bucket 포함), exploratory [1, 5, 10, 40, 120] |
| 관측 신호 밴드 · 모양 | 없음 — 최대 \|IC\| 0.0032 (32 §4) |
| 등급 / screen_pass / discovery | **C 2 / D 2** / 통과 0/4 / 0/4 cell — C는 available 부호 뒤집힘 상한(`available_sign_flip_max_grade: C`), D는 신호 없음. **C가 D보다 나은 판정이 아니다** (32 §4.3) |
| 모델 검증 이력 | **미투입.** discovery 0개라 T2 후보에서 빠졌다 (32 §9) |

### 산식과 규칙
```sql
-- filing_activity.py:122 / :136
SUM(insider_filings) OVER (... ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS own_insider_filing_60d;
CASE WHEN session_ordinal >= 250 THEN own_insider_filing_60d / NULLIF(
    quantile_cont(own_insider_filing_60d, 0.5) OVER (... ROWS BETWEEN 249 PRECEDING AND CURRENT ROW), 0)
END AS own_insider_filing_burst_60d
```
- 카드 2와 **완전히 같은 CASE 문**에서 나온다. 차이는 분자가 `INSIDER_MARKER` 공시(임원·주요주주 특정증권등
  소유상황보고서, 139,697건)만 센다는 것 (32 §2.1).
- **건수는 매수와 매도를 구분하지 않는다.** DS004 `elestock` API가 2년치만 주기 때문에 접수 건수로 대체한
  설계의 근본 한계다 (32 §2.2, §3.1).
- 250일 중앙값이 0인 회사(임원 공시를 거의 안 내는 회사)는 `NULLIF`로 NULL — **coverage 0.253의 직접 원인**
  (32 §2.3). `formula_version: filing_v3`.

### 가용 시점(PIT)과 지연
- 접수일 D → D+1 거래일 노출. 250일 baseline으로 유효 시작 2015-07-06 (32 §2.4).
- lag1 유지율: `미측정`.

### 예측력 — broad × common_survivor × native_t
| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | −0.00036 | −0.008 | −0.12 | +0.05%p | 0.984 | C |
| cum | 0→60 | **−0.00022** | −0.005 | **−0.04** | +0.26%p | **1.000** | C |
| bucket | 10→20 | +0.00173 | 0.039 | 0.76 | +0.05%p | 0.511 | D |
| bucket | 40→60 | +0.00322 | 0.069 | 0.99 | +0.07%p | 0.371 | D |

(32 §4.1) IC 부호가 누적(−)과 구간(+)에서 갈리고 spread는 네 cell 다 양수다 — 0 근처에서 부호가 쉽게
뒤집히는 전형적인 "신호 없음" 모습이다. `failed_gates`: b10→20·b40→60 `primary_discovery`; cum 0→20
`primary_discovery, period_sign_pass, available_direction_pass`; cum 0→60 여기에 `tradable_pass`,
`robustness_pass` 추가 (32 §4.3).

### 강건성
| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 0.628 / **3.143(F)** / 1.158 / 1.592 — 분모 \|IC\| 0.0002라 비율이 폭주 | 무의미 (32 §5.2) |
| lag1 유지율 | 미측정 | 기준 0.50 |
| available 표본 부호 | cum 0→20·cum 0→60 **False** (부호 뒤집힘) | 상한 C |
| 기간 일관성 | cum 2/5 · bucket 4/5 | cum 실패 |
| 시간 placebo | cum60 **0.9802** · 나머지 대상 아님 | 실패 |
| 비중첩 offset | cum60 `complete`이지만 `nonoverlap_robustness_pass = False` | 실패 |
| source 경고 (Phase B) | `not_applicable` — 원천이 깨끗한 것과 신호가 있는 것은 별개 (32 §5.6) | 없음 |

### 표본과 커버리지
| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2015-07-06 ~ 2025-02-05 |
| 유효 거래일 | 2,354일 |
| 날짜당 종목 수 | **312개** (5분위 칸당 62종목) |
| coverage_ratio(Phase B) | **0.253** (관측 1,741,331행) — 35개 중 최저 |
| KOSPI/KOSDAQ | KOSDAQ 0.230 / KOSPI 0.288 — 드물게 KOSPI가 높다(원인 미확인) |

(32 §6)

### 중복성
- A×B 상관 표에 이 family의 행이 없다(유효 교집합 부족으로 보이나 미확인). `family_summary`의
  `top_rank_correlation_pair`는 `px_amihud_20d` **−0.084** (32 §7).
- 구조적 관계: 카드 2의 부분집합(같은 CASE 문), 카드 7과 형제(임원 vs 5% 공시), 카드 5의 분모에 포함 (32 §7).
- **미확인 중복**: B×B 없음.

### 모델 투입 메모
- **현 형태로는 넣지 않는다.** 신호가 없고(최대 \|IC\| 0.0032, 최소 q 0.371) 75%가 NULL이라 indicator가
  피쳐를 지배한다.
- 다시 시도한다면: (a) 건수 형태(secondary `own_insider_filing_60d`)는 분모 0이어도 0으로 정의돼
  커버리지 문제를 피한다 — 미실행 (32 §2.5, §10); (b) `elestock` 2년치로 매수·매도 방향을 담은 지표를
  exploratory로 새 config에 사전등록; (c) 카드 7과 합쳐 임원/5% 구분이 의미 있는지 확인.
- **I13**: 건수·비율형이라 이력 시작 구간의 상수 횡단면 위험이 같다(10 §9.9 지목, 이번 표본 차이 없음).
- horizon: 등록은 [20, 60]. 검정 결과 어느 horizon에도 맞지 않는다.

### 한계
- 매수·매도 미구분 — 신호가 없는 가장 그럴듯한 이유 (32 §8.1). coverage 25% (32 §8.2).
- 유지율 무의미 (32 §8.4). secondary 미실행 — 건수 형태가 커버리지를 살릴 수 있었다 (32 §8.5).
- 금액·지분율을 안 봄, 형제 중복 미확인, holdout 미개봉 (32 §8.6~8.10).

---

## 7. `own_major_filing_activity` — 5% 대량보유 공시 건수

| 항목 | 값 |
|---|---|
| primary feature | `own_major_filing_60d` |
| secondary / variant | `own_major_filing_120d`(secondary, 미실행) · `own_major_filing_60d_lag1`(lag1) |
| 마트 / 산식 위치 | `feat_filing_activity` / `filing_activity.py:126` — `burst_columns` 루프(`:130`)에 **없다**, 건수 그대로 |
| 원천 raw 테이블 | `dart_filing_receipt_raw` (`MAJOR_HOLDER_MARKER`, 101,656건, 일반+약식) |
| prod 갱신 | 매일 23:30 KST (카드 1과 같다) |
| 분류 좌표 | C3(수급·소유·내부자) × T0(수준) × U (33 §3.3) — C3 「내부자·최대주주」 빈 칸 중 유일한 A |
| 검증 phase / fdr_family / role | B / ownership / `phase_b_blocked` — `expansion_20260827` |
| 기대 부호 → 관측 부호 | 없음(양방향) → `−` (4 cell 전부) |
| 사전등록 primary horizon | [20, 60] (bucket 포함), exploratory [1, 5, 10, 40, 120] |
| 관측 신호 밴드 · 모양 | 20~60일 · peak `cum 0→60` · 누적 0.027→0.043, 구간 0.018→0.023 증가 (33 §4.3) |
| 등급 / screen_pass / discovery | A / 통과 4/4 / 4/4 cell, `failed_gates = []` |
| 모델 검증 이력 | T2 후보 14 묶음 → `improved_all_horizons` → holdout 대기. 14개 중 spread가 뚜렷하고 coverage 1.0이라 몫이 클 가능성이 있으나 규모 중복(`fin_log_mcap`·`mcap_krx_log`)이 걸린다 (33 §9) |

### 산식과 규칙
```sql
-- filing_activity.py:126
SUM(major_filings) OVER (PARTITION BY ticker, market ORDER BY trade_date
                         ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS own_major_filing_60d
```
- **비율이 아니라 건수다.** 공시가 없으면 NULL이 아니라 **0**이다 — 0 채우기(`filing_activity.py:196`)가
  coverage 1.0의 직접 근거다 (33 §2.2, §2.4).
- 비율은 커버리지를 잃고 규모를 상쇄하고, 건수는 커버리지를 얻고 규모를 안고 간다. 이 family는 후자다.
- 일반보고서와 약식보고서를 같은 1건으로 센다. 매수·매도 공시를 구분하지 않는다.
- 단위: 건수(정수). `formula_version: filing_v3`.

### 가용 시점(PIT)과 지연
- 접수일 D → D+1 거래일 노출 (33 §2.5).
- lag1 유지율: `미측정`.

### 예측력 — broad × common_survivor × native_t
양방향이라 aligned = 원값. 음수면 상위 20%(공시 많은 쪽)가 덜 올랐다.

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | −0.0273 | −0.676 | −9.60 | −1.73%p | ~0 | discovery + screen-pass (A) |
| cum | 0→60 | **−0.0428** | **−1.031** | −8.67 | **−4.24%p** | ~0 | discovery + screen-pass (A) |
| bucket | 10→20 | −0.0182 | −0.468 | −9.28 | −0.84%p | ~0 | discovery + screen-pass (A) |
| bucket | 40→60 | −0.0228 | −0.607 | −8.60 | −2.56%p | ~0 | discovery + screen-pass (A) |

(33 §4.1) IC와 spread가 같은 방향이고 **크기도 상당하다** — 60거래일 −4.24%p는 같은 horizon의
`px_maxret_20d`(+1.79%p), `px_idio_vol_60d`(+2.99%p)보다 크다. ownership 계열에서 경제적 크기가 뚜렷한
family는 이것과 카드 8뿐이다 (33 §4.2). 어긋남 없음.

cell별 `n_dates`: cum 0→20 2,527 / b10→20 2,525 / b40→60 2,518 / cum 0→60 2,514 (33 §4.4).

### 강건성
| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | 0.847 / 0.877 / **0.828** / 0.901 — **네 cell 전부 1 미만**, ownership 계열에서 유일 (규모 편향 정황) | 기준 0.50 통과 |
| lag1 유지율 | 미측정 | 기준 0.50 |
| available 표본 부호 | `True` (4 cell) | 유지 |
| 기간 일관성 | 5/5 (4 cell, 관측 부호 `−` 기준) | 통과 |
| 시간 placebo | cum60 **0.0099** 최솟값 통과 · 나머지 셋 대상 아님(NW lag < 59) | `robustness_required` 1 cell 받았다 |
| 비중첩 offset | cum60 `complete`·`True` | 통과 |
| source 경고 (Phase B) | `not_applicable` | 없음 |

(33 §5)

### 표본과 커버리지
| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2014-06-02 ~ 2025-02-05 (`family_summary`의 `effective_start`는 2007-08-01 — 원천 시작) |
| 유효 거래일 | 2,514~2,527일 |
| 날짜당 종목 수 | **1,107개** — ownership 계열 최다 |
| coverage_ratio(Phase B) | **1.00000** (관측 6,879,703행) — Phase B 유일, 다음이 `mcap_krx_log` 0.99862 |
| KOSPI/KOSDAQ | 둘 다 1.00000 |

(33 §6) 커버리지가 완벽하면 실행 불가능한 초소형주까지 값을 갖는다 — 유지율 1 미만과 이어진다
(00_읽는_법 §9.9).

**I13 주의.** 이 카드의 숫자는 canonical run(2026-08-28, native 엔진 수정 전)의 값이다. native 엔진이
예측자가 완전히 상수인 `(거래일, 시장)` 횡단면에서 NaN 대신 가짜 상관을 냈고, 영향은 **전부 이 family**
였다 — 2014년 288개 date×market 그룹(예: 2014-06-16 KOSPI 745종목 고유값 1개, legacy NaN / native −0.014574),
`daily_ic` 868,874행 중 474행(472행이 2014년). 4 cell의 `n_dates`가 2,478 → 2,514~2,527, `ic_mean`이 약 5e-4
움직였고 **판정 변화는 0**이다. 2026-08-30 수정 뒤 `236d0d35…` 계보로 재실행해 `evidence_grade`·`screen_pass`
변화 없음을 확인했다 (10 §9.9). 건수형 피쳐의 이력 시작 구간은 횡단면이 통째로 0이다 — 2014년 표본을 쓰면
이 점을 안다.

### 중복성
- A×B: `family_summary` `top_rank_correlation_pair` = `px_amihud_20d` **−0.226**. 전체 표에서 `px_idio_vol_60d`
  +0.155 (+0.06~+0.26, 안정), `px_maxret_20d` +0.103, `flow_inst_netbuy_to_volume` −0.035 (33 §7).
  `px_amihud_20d`가 사실상 규모 지표라 **건수 형태의 규모 편향이 숫자로 확인된 셈**이다.
- 구조적 관계: 카드 6과 형제, 카드 5의 분모에 포함, 카드 2의 부분집합, 카드 8·9와 원천 사건이 겹친다
  (지분율 자체 vs 공시 건수) (33 §7).
- **미확인 중복**: B×B 없음. 규모(`fin_log_mcap`·`mcap_krx_log`)와의 직접 상관도 없다.

### 모델 투입 메모
- 변환: 0이 대부분인 정수 건수. `(거래일, 시장)` 내 rank. 0 동점 블록이 크므로 `공시 있음(>0)` indicator +
  건수 rank 분리를 권한다. `log1p`는 rank 뒤에는 불필요.
- 결측: 없다(coverage 1.0). 단, **2014년 초 표본은 전부 0(상수 횡단면)**이라 정보가 없다 — 그 구간을 빼거나
  기간 더미로 표시한다.
- 유의점: **규모 편향**(`px_amihud_20d` −0.226, 유지율 0.83~0.90). `fin_log_mcap`/`mcap_krx_log`를 넣은
  뒤의 증분을 먼저 본다. 초소형주 비중이 크다. 매수·매도 방향 미구분.
- horizon: primary [20, 60], peak 60. **h20·h60 실험용**. 120은 exploratory, 미검정.
- 조합 후보: 규모 통제 증분, 발행주식수 대비 건수(규모 정규화 — burst 형태는 카드 6처럼 커버리지를 잃는다),
  카드 8(지분율 변화)과의 사건 교차 확인, `px_turnover_shock`와의 동시성, 일반/약식 분리.

### 한계
- 매수·매도 미구분 — `−`가 「대주주 이탈」인지 「지분 불안정」인지 모른다 (33 §8.1).
- 규모 편향, 규모 통제 증분 미측정 (33 §8.2). ownership 다섯 중복 미확인 (33 §8.3).
- secondary(120일) 미실행, 60일 너머 미관측, 일반/약식 미구분, 업종 중립화 없음, holdout 미개봉 (33 §8.4~8.9).

---

## 8. `own_major_stake_change` — 최대주주 지분율 변화

| 항목 | 값 |
|---|---|
| primary feature | `own_major_stake_chg` |
| secondary / variant | `—` · `own_major_stake_chg_lag1`(lag1) · 보조 `own_major_stake_chg_available_from`, `periodic_source_warning`, `vintage_capture_ratio` |
| 마트 / 산식 위치 | `feat_periodic_extras` / `research/etl/features/periodic_extras.py:181`(산식), `:170`(최대주주 선택), `:165`(범위 필터), `:184`(노출 시점) |
| 원천 raw 테이블 | `dart_governance_raw`(DS002 `hyslrSttus`, `statement_type = 'major_shareholder'`), `dart_filing_receipt_raw`(접수일 → 다음 거래일) |
| prod 갱신 | **일회성 백필**(N6, 2015~2025: 122,729 슬라이스, raw 419,160행, 오류 0). 정기 Cronicle 이벤트 없음 (00_status §0, docs/operations.md) |
| 분류 좌표 | C3(수급·소유·내부자) × T1(변화) × U (34 §3.3) |
| 검증 phase / fdr_family / role | B / ownership / `phase_b_blocked` — `expansion_20260827`, `source_quality: {status: warn, warning: final_vintage, grade_cap: B}` |
| 기대 부호 → 관측 부호 | 없음(양방향) → `+` (4 cell 전부) |
| 사전등록 primary horizon | [60, 120] (bucket 포함), exploratory [20, 40] |
| 관측 신호 밴드 · 모양 | 60~120일 · peak `cum 0→120` · 누적 0.032→0.042, 구간 0.022→0.033 증가 (34 §4.4) |
| 등급 / screen_pass / discovery | **B**(상한) / 통과 4/4 / 4/4 cell, `failed_gates = []` — 게이트는 전부 통과, 원천 상한으로 B |
| 모델 검증 이력 | T2 후보 14 묶음 → `improved_all_horizons` → holdout 대기. **같은 묶음에 카드 9(`own_major_stake`)가 함께 들어 있다**(수준 대 차분) (34 §9) |

### 산식과 규칙
```sql
-- periodic_extras.py:170 / :181
max(stake) FILTER (WHERE selection_priority = best_priority) AS own_major_stake;   -- WHERE stake BETWEEN 0 AND 100
CASE WHEN previous_year = bsns_year - 1 THEN own_major_stake - previous_stake END AS own_major_stake_chg
```
- 같은 사업연도의 여러 행 중 `selection_priority`(`nm='계'`·`stock_knd='합계'` 1 → 보통주/의결권 2 → 계 3 →
  기타 4)가 가장 좋은 행만 남기고 그중 지분율 최댓값을 최대주주로 본다 (34 §2.2).
- **바로 전 사업연도가 있어야 계산한다.** 한 해라도 건너뛰면 NULL (34 §2.3).
- 단위: **%p 차이**(비율의 비율이 아님). 30→32.5와 60→62.5가 같은 값이다.
- `formula_version: periodic_extras_v2`, `FINAL_VINTAGE_CAPTURE_RATIO = 0.0184`.

### 가용 시점(PIT)과 지연
- `change_available_from = greatest(stake_available_from, previous_available_from)` — 두 해 접수 중 **늦은 쪽**의
  다음 거래일부터 ASOF join (34 §2.3).
- vintage 나이: KOSDAQ 평균 **181.7일** / 95분위 475일, KOSPI 182.1일 / 490일. 20번에 한 번은 1년 3개월
  지난 값이다 (34 §2.4).
- **DS002는 최종 확정본만 준다.** 처음 공시된 값과 정정된 값을 구분할 수 없다. PIT 원칙(접수 공개 뒤 사용)
  자체는 지켰다. 이 한계가 사전등록 단계에서 등급 상한 B로 못 박혔다 (34 §2.5, §5.6).
- lag1 유지율: `미측정`.

### 예측력 — broad × common_survivor × native_t
양방향이라 aligned = 원값.

| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | +0.0321 | 0.793 | 6.17 | +1.18%p | ~0 | discovery + screen-pass (B) |
| cum | 0→120 | **+0.0416** | **1.013** | 5.65 | **+2.13%p** | ~0 | discovery + screen-pass (B) |
| bucket | 40→60 | +0.0218 | 0.518 | 6.93 | +0.36%p | ~0 | discovery + screen-pass (B) |
| bucket | 60→120 | +0.0334 | 0.732 | 5.95 | +1.00%p | ~0 | discovery + screen-pass (B) |

(34 §4.1) IC와 spread가 네 cell 전부 같은 방향이다. 어긋남 없음. Phase B에서 4/4 완전 통과는 다섯 family뿐이다
(`fin_log_mcap`, 카드 1, 카드 5, 카드 7, 이 family).

**수준(카드 9)보다 변화가 모든 지표에서 낫다** (34 §4.2): IC +0.0416 vs +0.0335, ICIR 1.013 vs 0.428, t 5.65 vs
2.60, spread +2.13%p vs +1.78%p, screen 4/4 vs 2/4, placebo 전부 통과 vs 절반 실패. 수준은 회사의 구조적
성격이고 변화가 "올해 최대주주가 무엇을 했는가"를 담는다는 해석이다.

### 강건성
| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | **1.127** / **1.121** / **1.134** / 1.041 (cum60 / cum120 / b40→60 / b60→120) | 통과 |
| lag1 유지율 | 미측정 | 기준 0.50 |
| available 표본 부호 | `True` (4 cell) | 유지 |
| 기간 일관성 | **4/4** (4 cell) — 표본이 2016-06-27부터라 `2014_2016` 구간이 비어 4구간 | 통과 |
| 시간 placebo | cum60 **0.0198**, b60→120 **0.0792**, cum120 **0.0891** 통과 · b40→60 대상 아님(NW lag 19) | 3 cell 받았다. cum120은 여유 작음 |
| 비중첩 offset | `complete`·`True` (3 cell) | 통과 |
| source 경고 (Phase B) | **`warn`** · reason `final_vintage` · **`grade_cap = B`** (사전등록 yaml에 직접 기재) | 상한 B — 데이터를 다시 받아도 해결되지 않는다 |

(34 §5) N6 계열 넷(`hc_employee_growth`, `hc_productivity`, 카드 8, 카드 9) 중 시간 placebo를 통과한 유일한
family다 (34 §5.2).

### 표본과 커버리지
| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2016-06-27 ~ 2025-02-05 |
| 유효 거래일 | 1,985일 |
| 날짜당 종목 수 | 978~981개 |
| coverage_ratio(Phase B) | **0.695** (관측 4,784,037행) |
| KOSPI/KOSDAQ | KOSDAQ 0.737 / KOSPI 0.634 |

(34 §6) 카드 9(0.795, 2,234일)보다 10%p 낮고 249일 짧다 — 변화를 만들려면 두 해가 필요하기 때문이다.

### 중복성
- A×B 교차: `px_near_52w_high` **+0.127** (1,985, +0.03~+0.51), `px_idio_vol_60d` **−0.103** (−0.42~−0.01),
  `px_resid_mom_12_1` +0.078, `px_maxret_20d` −0.076, `px_amihud_20d` +0.072. 앞의 둘은 방향이 안정적이고
  부호를 맞추면 같은 방향으로 작동한다 (34 §7).
- 구조적 관계: **카드 9(`own_major_stake`)의 차분**이다. 카드 7과 원천 사건이 겹친다(지분 5% 이상 보유자의
  공시 건수 vs 최대주주 지분율 변화) (34 §7).
- **미확인 중복**: B×B 없음. 카드 9와의 횡단 상관을 모른다 — **T2 묶음에 둘 다 들어갔으므로 실질적 공백**이다.

### 모델 투입 메모
- 변환: %p 차이, 양쪽 꼬리, **0(변화 없음)에 큰 질량**. `(거래일, 시장)` 내 rank + `변화 있음(≠0)` indicator,
  필요하면 부호 indicator(증가/감소)도 분리한다.
- 결측: 30% NULL — 전년 값 없음(상장 2년 미만, 공시 누락), 범위 필터(0~100 밖), 첫 해. indicator를 둔다.
  2016-06-27 이전은 표본이 없다.
- 유의점: 연 1회 **계단형**, 평균 182일 묵음. 두 해의 접수를 모두 기다리므로 카드 9보다 노출이 늦다.
  등급 상한 B는 통계가 아니라 원천 구조 때문 — 모델 투입을 막는 이유는 아니다. 최대주주 교체·유상증자
  참여·타 주주 매도로 인한 상대적 상승을 구분하지 않는다 (34 §8.4~8.5).
- horizon: primary [60, 120], peak 120. **h60·h120 실험용**. h5·h20 미검정.
- 조합 후보: **카드 9와 둘 중 하나 또는 직교화** — B×B 상관을 먼저 잰다. 카드 7(5% 공시 건수)과의 사건
  일치 여부, 규모 통제, 최대주주 유형(창업자/지주회사/기관) 분리(원천에 구분 없음).

### 한계
- 등급 상한 B, 구조적 (34 §8.1). 카드 9와의 상관 없음 (34 §8.2). 평균 182일 묵음 (34 §8.3).
- 변화 원인·최대주주 교체 미구분 (34 §8.4~8.5). 표본 2016~, 기간 검정 4구간 (34 §8.6).
- 120일 너머 미관측, 업종 중립화 없음, holdout 미개봉 (34 §8.7~8.10).

---

## 9. `own_major_stake_level` — 최대주주 지분율 수준

| 항목 | 값 |
|---|---|
| primary feature | `own_major_stake` |
| secondary / variant | `—` · `own_major_stake_lag1`(lag1) · 보조 `own_major_stake_available_from` |
| 마트 / 산식 위치 | `feat_periodic_extras` / `periodic_extras.py:170`(산식), `:165`(범위 필터), `:180`(노출 시점) |
| 원천 raw 테이블 | `dart_governance_raw`(DS002 `hyslrSttus`), `dart_filing_receipt_raw` |
| prod 갱신 | 일회성 백필(N6, 2015~2025). 정기 이벤트 없음 (카드 8과 같다) |
| 분류 좌표 | C3(수급·소유·내부자) × T0(수준) × U (35 §3.3) — 카드 8(T1)과 같은 원천으로 수준/변화를 각각 만든 설계 |
| 검증 phase / fdr_family / role | B / ownership / `phase_b_blocked` — `expansion_20260827`, `source_quality: {warn, final_vintage, grade_cap: B}` |
| 기대 부호 → 관측 부호 | 없음(양방향) → `+` (4 cell 전부) |
| 사전등록 primary horizon | [60, 120] (bucket 포함), exploratory [20, 40] |
| 관측 신호 밴드 · 모양 | 60~120일 · peak `cum 0→120` · 누적 0.030→0.034 완만, 구간 0.019→0.021 거의 평평 — 60일 이후 새로 더해지는 신호가 적다 (35 §4.4) |
| 등급 / screen_pass / discovery | **B 2 / C 2** / 통과 2/4 / 4/4 cell — 긴 두 cell `failed_gates = [robustness_pass]`(Phase B의 C) |
| 모델 검증 이력 | T2 후보 14 묶음 → `improved_all_horizons` → holdout 대기. 카드 8과 함께 들어가 있다 (35 §9) |

### 산식과 규칙
```sql
-- periodic_extras.py:170
max(stake) FILTER (WHERE selection_priority = best_priority) AS own_major_stake   -- WHERE stake BETWEEN 0 AND 100
```
- 최대주주 지분율(%) 그 자체. 선택 규칙은 카드 8과 같다 (35 §2.1).
- **한 해만 있으면 된다.** 카드 8과 결정적으로 다른 점이고 커버리지·표본 차이의 원인이다 (35 §2.2).
- 단위: %(0~100). `formula_version: periodic_extras_v2`.

### 가용 시점(PIT)과 지연
- `level_available_from = stake_available_from` — 그 해 접수의 다음 거래일부터 ASOF join (35 §2.2).
- vintage 나이: KOSDAQ 평균 **182.3일** / 95분위 461일, KOSPI 186.4일 / 559일. 한 해만 기다려도 공시 주기가
  1년이라 카드 8과 나이가 거의 같다 (35 §2.3).
- DS002 최종본 한계·상한 B는 카드 8과 완전히 같다 (35 §2.4, §5.6).
- lag1 유지율: `미측정`.

### 예측력 — broad × common_survivor × native_t
| scan | horizon | Rank IC | ICIR | t_nw | q5 spread(aligned) | BH q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | +0.0301 | 0.484 | 3.93 | +0.80%p | 0.00016 | discovery + screen-pass (B) |
| cum | 0→120 | **+0.0335** | 0.428 | 2.60 | **+1.78%p** | 0.0139 | discovery, robustness 실패 (C) |
| bucket | 40→60 | +0.0191 | 0.274 | 3.59 | +0.28%p | 0.00059 | discovery + screen-pass (B) |
| bucket | 60→120 | +0.0213 | 0.239 | 1.86 | +0.95%p | 0.0841 | discovery, robustness 실패 (C) |

(35 §4.1) IC와 spread가 같은 방향이다. 어긋남 없음. `bucket 60→120`의 q 0.0841은 기준 0.10에 가깝다.
ICIR 0.428은 카드 8(1.013)의 절반 이하 — IC 평균은 비슷한데 **일별 IC의 흔들림이 두 배 이상**이다. 지분율
수준은 회사마다 구조적으로 고정돼 횡단 순위가 거의 안 변하고, 그래서 IC가 시장 국면에 통째로 좌우된다는
해석이다 (35 §4.2).

### 강건성
| 검사 | 값 | 판정 |
|---|---|---|
| tradable 유지율 | **0.935** / **1.213** / 1.023 / **1.430** (cum60 / cum120 / b40→60 / b60→120) — 0.94~1.43으로 흩어짐. \|IC\| 0.019~0.034로 분모가 작다 | 통과(불안정) |
| lag1 유지율 | 미측정 | 기준 0.50 |
| available 표본 부호 | `True` (4 cell) | 유지 |
| 기간 일관성 | cum60 5/5, b40→60 5/5, cum120 4/5, b60→120 4/5 — 표본 2015-06-25부터라 5구간 | 통과 |
| 시간 placebo | cum60 **0.0297** 통과 · cum120 **0.2475 실패** · b60→120 **0.4257 실패** · b40→60 대상 아님 | 2 cell 실패 |
| 비중첩 offset | `complete`·`True` (3 cell) — 떨어진 이유는 placebo 하나 | 통과 |
| source 경고 (Phase B) | **`warn`** · `final_vintage` · **`grade_cap = B`** | 상한 B |

(35 §5)

### 표본과 커버리지
| 항목 | 값 |
|---|---|
| 유효 시작~끝 | 2015-06-25 ~ 2025-02-05 |
| 유효 거래일 | 2,234일 |
| 날짜당 종목 수 | 1,001~1,004개 |
| coverage_ratio(Phase B) | **0.795** (관측 5,466,249행) — N6 계열 넷 중 최고 |
| KOSPI/KOSDAQ | KOSDAQ 0.847 / KOSPI 0.716 |

(35 §6) 한 해만 있으면 되니 카드 8보다 1년 일찍 시작하고 10%p 넓다.

### 중복성
- A×B 교차: `px_amihud_20d` **+0.174** (2,234, **−0.64~+0.29** — 범위가 매우 넓어 날짜에 따라 관계가 뒤집힌다),
  `px_idio_vol_60d` **−0.141** (−0.33~−0.02, 안정), `px_maxret_20d` −0.089 (35 §7).
- 구조적 관계: **카드 8이 이 값의 차분**이다. 카드 7과 원천 사건이 겹친다. `px_amihud_20d`와 +0.174이므로
  규모와도 얽힐 수 있는데 `fin_log_mcap`·`mcap_krx_log`와의 상관이 없다 (35 §7).
- **미확인 중복**: B×B 없음.

### 모델 투입 메모
- 변환: 0~100 %, 회사별로 거의 상수인 시계열. `(거래일, 시장)` 내 rank. 시계열 변화가 거의 없어 **알파
  피쳐보다 조건 변수(오너 기업 vs 소유분산)나 세그먼트 변수로 쓰는 것이 성격에 맞다.**
- 결측: 20% NULL(공시 전, 범위 필터, 원천 없음). indicator.
- 유의점: 연 1회 계단형, 평균 182~186일 묵음. ICIR 0.428 — IC가 국면에 좌우된다. 이 그룹은 Phase C(국면
  의존) 대상이 아니었으므로 어느 국면에서 맞는지 모른다. 창업자 일가·지주회사·국민연금·외국계 펀드가 같은
  "최대주주"로 묶이고 특수관계인 합산 여부도 미확인 (35 §8.7~8.8).
- horizon: primary [60, 120]. 통과 cell은 cum 0→60·bucket 40→60. **h60 실험**에 맞고 h120은 placebo 실패.
- 조합 후보: **카드 8과 둘 중 하나** 또는 직교화(B×B 먼저), 국면 × 지분율 상호작용(사전등록 필요),
  규모 통제, 최대주주 유형 분리.

### 한계
- 변화 형태보다 약하다 (35 §8.1). 등급 상한 B (35 §8.2). 긴 두 cell placebo 실패 (35 §8.3).
- 카드 8과 상관 없음 — T2 묶음에 둘 다 들어가 실질적 문제 (35 §8.4). 유지율 흩어짐 (35 §8.5).
- 최대주주 유형·특수관계인 합산 미확인, 업종 중립화 없음, holdout 미개봉 (35 §8.7~8.11).

---

## Horizon Scan 미검정 피쳐

같은 마트에 있지만 Horizon Scan 단변량 검정을 거치지 않은 컬럼이다.

| 컬럼 | 뜻(산식 요약) | 마트 | 모델 사용 | 검증 상태 | 메모 |
|---|---|---|---|---|---|
| `ev_treasury_ratio` | `tesstk_co / istc_totqy` — 자사주 ÷ 발행주식수 (`se='합계'` 행) | `feat_event` (`event.py`) | `ev` feature group 토글로 투입 가능. **baseline 40에 없음** | P8 ablation만 있음(`px_flow_fin_ev` mean Rank IC 0.1343 vs `px_flow_fin` 0.1336, `feature_ablation_results.json`). 단변량 검정 없음 | PIT는 `bsns_year` 말 + 90일 고정 지연(`ANNUAL_LAG_DAYS`), `available_from`이 아니다. universe는 `dim_universe_daily`(broad 아님) |
| `ev_has_treasury` | `treasury_shares > 0` 플래그 | `feat_event` | 같음 | 같음 | 플래그형 — 상수 횡단면 위험(I13 성질) |
| `ev_shares_chg_yoy` | `issued_shares / prev_issued_shares − 1` — 발행주식수 단순 YoY | `feat_event` | 같음 | 같음 | 카드 3이 "쓰면 안 된다"고 한 **기계적 증가 미분리 YoY**(20 §2.2). 카드 3의 조악한 대체물로만 의미 |
| `ev_filing_count_60d`, `ev_filing_count_120d`, `ev_filing_burst_120d` | 카드 2의 secondary — 건수 형태·120일 창 | `feat_filing_activity` | 미투입 | 사전등록 secondary, **이번 run 미실행** (19 §2.4) | 건수 vs 비율, 60 vs 120 중 어느 쪽이 나은지 모른다 |
| `own_insider_filing_60d`, `own_insider_filing_120d`, `own_insider_filing_burst_120d` | 카드 6의 secondary | `feat_filing_activity` | 미투입 | secondary, 미실행 (32 §2.5) | 건수 형태는 분모 0 문제를 피해 coverage 0.253을 크게 올릴 수 있다 — 미확인 |
| `own_major_filing_120d` | 카드 7의 secondary — 120일 창 건수 | `feat_filing_activity` | 미투입 | secondary, 미실행 (33 §2.5) | — |
| `amendments_250d`, `filings_250d`, `own_amendments_250d`, `own_filings_250d` | 카드 1·5의 분자·분모 | `feat_filing_activity` **CTE 내부** | — | 보조 컬럼(패널 계산용) | `windowed` CTE에만 있고 **마트 출력 컬럼이 아니다**(`features` SELECT에 없음). 분모 크기를 피쳐로 쓰려면 SQL을 고쳐야 한다 |
| `session_ordinal` | 종목별 누적 세션 번호 | `feat_filing_activity` CTE 내부 | — | 보조(warm-up 판정) | 출력 컬럼 아님 |
| `issuance_available_from`, `issuance_identity_ok`, `issuance_classification_complete` | 카드 3의 노출 시점·항등식·분류 완결 플래그 | `feat_event_scan_daily` | 미투입 | 보조 컬럼(품질 플래그) | 결측 원인 indicator로 쓸 수 있다 |
| `cash_dividends_total`, `dividend_source`, `buyback_cash_ttm`, `payout_available_from` | 카드 4의 분자 구성과 출처 | `feat_event_scan_daily` | 미투입 | 보조 컬럼 | `dividend_source` 분포를 아직 아무도 보지 않았다 (21 §2.3) |
| `own_major_stake_available_from`, `own_major_stake_chg_available_from` | 카드 8·9의 노출 시점 | `feat_periodic_extras` | 미투입 | 보조 컬럼 | 값 나이(vintage age) 계산용 |
| `periodic_source_warning`, `vintage_capture_ratio` | 상수 `'final_vintage'`, `0.0184` | `feat_periodic_extras` | — | 진단용 | 모든 행에 같은 값 — 피쳐가 아니다 |
| `ev_filing_burst_60d_lag1`, `ev_amendment_ratio_1y_lag1`, `own_insider_filing_burst_60d_lag1`, `own_major_filing_60d_lag1`, `own_amendment_ratio_1y_lag1` | 직전 세션 값 (`lag(...) OVER (PARTITION BY ticker, market ORDER BY trade_date)`) | `feat_filing_activity` | 미투입 | lag1 variant — `daily_ic`에 저장되지 않고 family 대표 cell만 스캔 (00_읽는_법 §7) | 정본이 `native_t`라 게이트용. lag1 유지율은 해설 문서에 기록 없음 |
| `ev_net_share_issuance_yoy_lag1`, `ev_payout_yield_lag1` | 같음 | `feat_event_scan_daily` | 미투입 | 같음 | 같음 |
| `own_major_stake_lag1`, `own_major_stake_chg_lag1` | 같음 | `feat_periodic_extras` | 미투입 | 같음 | 같음 |

---

## 이 그룹을 함께 쓸 때 — 닫는 메모

1. **14개가 14개의 독립 정보가 아니다.** T2 묶음 안에 이 그룹에서 확인된 중복 쌍이 둘 있다 —
   `ev_amendment_ratio_1y`·`own_amendment_ratio_1y`(전체 대 부분집합), `own_major_stake`·`own_major_stake_chg`
   (수준 대 차분). 그 밖에 `ev_filing_burst_60d`⊃`own_insider_filing_burst_60d`⊃…, `own_major_filing_60d`⊂
   `own_amendment_ratio_1y`의 분모, `ev_net_share_issuance_yoy`↔`ev_payout_yield`(같은 축의 양 끝)가 전부
   **B×B 상관 미계산** 상태다 (00_읽는_법 §9.2). 모델에 넣기 전에 이 상관을 먼저 계산한다.
2. **IC와 q5 spread를 항상 나란히 본다.** 카드 3(IC −0.038 / spread +11.3%p 정반대), 카드 4(\|IC\| 0.10 /
   spread 0.49%p), 카드 5(NaN)가 교과서적 사례다. 랭킹 모델은 IC를 따르되, 카드 3의 극단 꼬리와 카드 5의 동점
   블록은 분류 확률 캘리브레이션에서 따로 본다.
3. **시간 placebo 통과·실패 지도.** 통과: 카드 1·5·7(0.0099), 8(0.020~0.089), 2(0.069, 문턱). 실패: 카드
   3(3 cell), 4(1 cell), 6, 9(2 cell) (00_읽는_법 §9.5). 통과한 짧은 cell(bucket 40→60 등)은 대부분 검사를
   **받지 않았다**.
4. **건수·플래그형의 상수 횡단면(I13).** 카드 7이 실제 영향(2014년 288 그룹, 판정 변화 0), 카드 2·5·6이 같은
   성질이다. 2014~2015년 초 표본을 쓰면 횡단면이 통째로 0인 날을 확인한다. native 엔진은 2026-08-30에 고쳐졌고
   이 문서의 숫자는 수정 전 canonical run 값이다 (10 §9.9).
5. **갱신 주기가 셋으로 갈린다.** 접수 이력(카드 1·2·5·6·7)은 매일 23:30 갱신되는 rolling window라 값이 매일
   조금씩 움직인다. `feat_event_scan_daily`(카드 3·4)는 연 1회 계단형이고 04:00 OpenDART 체인이 채운다.
   `feat_periodic_extras`(카드 8·9)는 **일회성 백필**이라 2025년 사업연도 이후 값은 다시 수집해야 생긴다.
6. **horizon 자리.** h20·h60 실험은 카드 2·7(primary [20, 60]). h60·h120 실험은 카드 1·3·4·5·8·9(primary
   [60, 120]). **h5는 이 그룹 어느 family도 primary로 검정하지 않았다** — h5 실험에 넣으려면 exploratory
   결과를 새로 봐야 하는데 secondary·exploratory cell은 이번 run에서 스캔되지 않았다.
7. **남은 판정은 하나다.** T2 h60 one-shot holdout, 2026년 10~11월 이후 (00_status §8.7). 그 전까지 이 그룹의
   "채택"은 없다.
