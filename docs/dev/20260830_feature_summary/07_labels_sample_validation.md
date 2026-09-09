# 07. 라벨·표본·검증 틀 — 다중 horizon 모델 실험의 바탕

- 작성일: 2026-09-03
- 정본: `research/etl/labels.py`(라벨), `research/etl/splits.py`(walk-forward), `research/analysis/horizon_scan_config.yaml`(표본·유니버스·게이트),
  `docs/target/01_20_access_return_rank/`(예측 대상 설계, acceptance gate 결과)

---

## 1. 라벨

### 1.1 지금 있는 것 (`labels.py`)

```
raw_label_{h}d  = fwd_ret_{h}d(ticker) − bench_ret_{h}d          # 초과수익률. bench = 같은 (거래일, 시장)의 동일가중 평균 forward 수익률
y_rank_{h}d     = PERCENT_RANK(raw_label_{h}d) OVER (PARTITION BY trade_date, market)   # [0,1] — 기존 모델 주 타깃
y_reg_{h}d      = winsor(raw_label_{h}d, 0.5%~99.5%, 날짜별)                              # 회귀 라벨
y_cls_{h}d      = 1 if y_rank ≥ 0.8, −1 if ≤ 0.2, else 0                                    # 3-class
y_vol_{h}d, y_mdd_{h}d = t+1..t+h 일간 수익률 경로의 실현변동성·최대낙폭                 # include_risk=True일 때만
```

| 항목 | 값 | 비고 |
|---|---|---|
| `LabelSpec.horizons` 기본 | `(20, 5, 60)` — 첫 값이 primary | `label_daily` 컬럼 접미 `_{h}d` |
| `kind` | `"excess"`(기본) / `"abs"` | **절대 수익률 라벨도 지원한다** |
| `bench` | `"eqw_market"`(구현) / `"index"`(예약, `NotImplementedError`) | 지수 종가로 바꾸는 건 한 줄 토글 설계 |
| `label_scan` (Horizon Scan) | h ∈ {1,2,3,5,10,20,40,60,120} 누적 + bucket (0,5],(5,10],(10,20],(20,40],(40,60],(60,120] | `build_label_scan_sql` |
| t+h 계산 | 거래정지일을 제외한 **종목별 거래일 index** | halt·휴일 갭 흡수 |

날짜 안에서 순위·winsor를 하므로 **시장 방향과 국면은 라벨에서 제거된다.** 이 프로젝트의 검증 결과 전부가 이 정의 위에 있다.

### 1.2 목표 모델의 라벨 — 정해야 할 것

목표는 "특정 시점 뒤 주가가 오를 확률"이다. 지금 정의로 만들 수 있는 후보가 셋이다.

| 후보 | 정의 | 장점 | 단점 |
|---|---|---|---|
| (a) 시장 대비 상승 확률 | `P(raw_label_{h}d > 0)` = `P(y_rank_{h}d > 중앙값 근처)` | 기존 검증 35+6 family가 전부 이 라벨 위에서 재졌다. 시장 방향 착시가 없다 | "절대적으로 오른다"가 아니다. 하락장에서 확률 0.7이어도 손실일 수 있다 |
| (b) 상위 분위 확률 | `P(y_rank_{h}d ≥ 0.8)` (= `y_cls = 1`) | 실매수 리스트(top-k)와 직결. 기존 3-class 라벨 그대로 | 양성 비율 20% 고정. 분위 경계 근처는 노이즈 |
| (c) 절대 상승 확률 | `P(fwd_ret_{h}d > 0)` (`kind="abs"`) | 사용자가 말한 "주가가 상승할지"에 가장 가깝다 | 시장 방향이 라벨에 들어온다. 매크로 `cf_*`가 직접 예측 변수가 되지만 사이클 2~3회 표본에서 외삽 위험(`06` §3.3). 기존 피쳐 검증 결과를 그대로 옮길 수 없다 |

**권고.** (a) 또는 (b)로 시작한다. 기존 증거가 그 위에 있고, 여유 자금 장기 운용이든 단기 수익이든 "같은 시장 안에서 상대적으로 강한 종목"을 고르는 문제로 환원된다. (c)는 별도 실험으로 두고, 시장 방향 예측을 따로 떼어 `P(abs up) = P(mkt up) × P(rel up | …)`처럼 합성하는 편이 해석이 낫다. 확률 보정(calibration)은 (a)·(b) 모두 fold별 valid 구간에서 확인해야 한다 — 기존 지표(Rank IC·top-decile spread)는 순위만 본다.

### 1.3 horizon별 라벨의 성질

| h | 라벨 중첩 | 리밸런싱 빈도(연) | T1 실측 turnover | 기존 검증에서의 자리 |
|---|---|---|---|---|
| 5 | 5일 겹침 | 약 50회 (T1 gate n_rebalances 280) | 0.682~0.696 | 반전·수급 5·20d·회전율 충격. 비용에 가장 민감 |
| 20 | 20일 | 약 12회 (70) | 0.571~0.597 | 기존 모델의 배포 대상. 변동성·MAX·개인 수급·filing burst·매크로 베타 |
| 60 | 60일 | 약 4회 (24) | 0.638~0.656 | 재무·밸류·규모·주주환원·지분. holdout 라벨이 2026-10~11에 성숙 |
| 120 | 120일 | 약 2회 | — | 규모·비유동성·발행·밸류의 사전등록 horizon. `common_survivor` 표본이 이 라벨 때문에 2025-02-05에서 끝난다 |

라벨이 겹치므로 (i) 통계는 Newey-West(lag h−1)로, (ii) walk-forward는 `purge = embargo = h`로 처리한다. h가 길수록 독립 표본이 적어지고 시간 placebo가 엄격해진다(Horizon Scan에서 긴 cell이 대부분 placebo에서 떨어진 이유).

---

## 2. 표본·유니버스 규칙 (`horizon_scan_config.yaml`)

| 규칙 | 값 | 뜻 |
|---|---|---|
| 표본 시작 | `2014-06-01` | KRX flow·시장 공통 계열이 2014-06부터 |
| **holdout 시작** | **`2025-08-01`** (`label_end_date` 기준) | **아직 열지 않았다. 모델 실험에서도 열지 않는다** |
| `common_survivor` | formation부터 120일 라벨이 끝까지 살아 있는 공통 표본 | discovery 좌표. formation 끝 `2025-02-05` |
| `available` | 그 시점에 존재한 모든 종목(상폐 포함) | 생존편향 대조. 부호 뒤집히면 등급 상한 C |
| 패널 편입 warm-up | 직전 60행 중 유효 세션 40일 이상 | 신규 상장·장기 정지 배제 |
| `broad` | 전 종목 | discovery 좌표 |
| `tradable` | 20일 평균 거래대금 ≥ 1억원 + 종가 ≥ 1,000원 | 실행 가능성 게이트 (`tradable_retention ≥ 0.50`) |
| 횡단 정규화 그룹 | `(trade_date, market)` — **KOSPI·KOSDAQ 둘뿐** | 업종 중립화 없음 |
| 가격제한폭 | 2014-06-01~2015-06-14 15%, 이후 30% (×1.1 허용) | 품질 플래그 |
| 기업행동 마스킹 | 주식수 변동 ≥ 25% 또는 가격×주식수 비율이 [0.8, 1.25] 밖 | 창 안에 CA가 있으면 가격 피쳐 NULL |
| 공매도 잔고 지연 | 2세션 | `short_balance_lag_sessions` |
| 기간 구간(`common`) | `2014_2016` / `2017_2019` / `2020_2021` / `2022_2023_10` / `2023_11_common_end` | 기간 일관성 검사 |

일별 유니버스의 정본은 KRX `daily_market_cap` 행이다(N3 §3.5 판정 — `daily_ohlcv`에만 있는 종목은 전부 KONEX 구간). `daily_ohlcv`는 naver 수정주가, `daily_market_cap`은 KRX 원주가·시총·상장주식수(T+1).

**생존편향 크기.** `daily_market_cap`에는 있고 `daily_ohlcv`에 없는 종목이 초기 324개다 — `daily_ohlcv`가 상폐 종목을 그만큼 놓치고 있다(S-4). `available` 표본 대조가 이 편향을 재는 장치다.

---

## 3. 검증 틀

### 3.1 Horizon Scan (단변량)

- cell = family × feature × scan_type(cum|bucket) × (h_start, h_end). 판정 단위는 피쳐가 아니라 cell.
- 사전등록 `primary_horizon_set`만 global BH(q 0.10) 모집단. exploratory는 진단.
- 게이트: 방향 · 기간 일관성 · tradable ≥ 0.50 · lag1 ≥ 0.50 · available 부호 · 시간 placebo(NW lag ≥ 59 cell, 100회, p ≤ 0.10) · 비중첩 offset · (Phase B) source 경고.
- permutation: 횡단면 100회(날짜×시장 블록), 결합 AB `p=0.0099`.
- 상세 지표는 `00_reading_guide.md` §3.2.

### 3.2 acceptance gate (모델 묶음)

`research/models/_01_20_access_return_rank`. 스냅샷 `2026-08-23` 기준.

| 항목 | 값 |
|---|---|
| 모델 | HistGradientBoosting (`hgb`). 2026-06 baseline은 Ridge(alpha 10) |
| 분할 | purged, embargoed **expanding-window walk-forward 5-fold**. `embargo = purge = horizon`. `holdout_len = 0`(holdout 미사용) |
| baseline 피쳐 | raw 40 / design 80 (`*_isna` 플래그 포함). `feat_price` 15 + `feat_flow` 15 + `feat_fin_pit` 10 |
| 전처리 | fold별 train slice에서 winsor/log/z-score 적합 → `feat_panel_std.parquet`. 결측은 `*_isna` 플래그 |
| 지표 | `rank_ic_mean`, `icir`, `rank_ic_tstat`, `top_decile_spread`, `top_minus_bottom`, `hit_ratio_top`, turnover, **비용 반영 spread(왕복 60bp)** |
| 경제성 확인 | k=100 실매수 리스트 비용 반영 수익 Δ(h20) > 0 |

**T1 — Grade A 4 family(+5 컬럼) 추가, 2026-08-24**

| h | baseline Rank IC | +5 Rank IC | baseline 비용반영 spread | +5 비용반영 spread |
|---|---:|---:|---:|---:|
| 5 | 0.1155 | 0.1202 | −0.0002 | +0.0007 |
| 20 | 0.1436 | 0.1521 | +0.0126 | **+0.0101** |
| 60 | 0.1753 | 0.1840 | +0.0204 | +0.0207 |

Rank IC는 세 h 모두 개선. 그러나 배포 대상 h20의 비용 반영 spread가 −0.0025, k=100 비용 반영 수익 Δ −0.0045로 **비채택**. 등급 A ≠ 모델 채택의 사례.

**T2 — Phase B `screen_pass` 14 feature 추가, 2026-08-28**

| h | baseline (40/80) | +14 (54/108) | Δ Rank IC | Δ 비용반영 spread |
|---|---:|---:|---:|---:|
| 5 | 0.1155 / −0.0002 | 0.1186 / 0.0015 | +0.0031 | +0.0017 |
| 20 | 0.1436 / 0.0126 | 0.1447 / 0.0155 | +0.0011 | +0.0030 |
| 60 | 0.1753 / 0.0204 | 0.1755 / **0.0283** | +0.0003 | **+0.0080** |

validation 상태 `improved_all_horizons`. **최종 채택은 아니다** — 새 h60 holdout 라벨이 성숙하는 2026년 10~11월 이후 한 번만 평가한다. 14개 안에 확인된 중복 쌍이 셋 있다(`01` §9).

참고: 2026-06 Ridge baseline(px+flow 30/60, 2015-01~2026-06, embargo 20)은 평균 Rank IC 0.128(fold 0.136/0.165/0.127/0.134/0.079)이었다. 마지막 fold의 하락은 hgb에서도 보인다(h20 fold5 0.105).

---

## 4. 다중 horizon 실험에 주는 함의

1. **h마다 피쳐 집합이 다르다.** 사전등록 밴드와 관측 밴드가 그 근거다(`08` §4 배치표). 한 모델에 모든 피쳐를 넣고 h만 바꾸면 h5에서는 재무 피쳐가, h120에서는 반전 피쳐가 노이즈다. 트리 모델은 견디지만 가설 수가 늘어 과적합 여지가 커진다.
2. **h가 짧을수록 비용이 판정을 뒤집는다.** T1이 h5·h20에서 IC 개선 → 비용 반영 음수였다. h5 실험은 반드시 turnover·비용 반영 지표를 주 지표로 둔다. `lag1` 유지율이 낮은 피쳐(`px_reversal_5d` 0.686, `flow_foreign_*` 게이트 실패)는 실행 지연 예산이 하루도 없다.
3. **h가 길수록 표본이 적고 국면 외삽이 커진다.** h120은 연 2회 리밸런싱, 11년이면 22개 독립 창이다. fold별 안정성보다 기간 일관성(5구간)을 본다. `common_survivor` 표본은 2025-02-05에서 끝나므로 최근 반년은 학습에 못 쓴다.
4. **purge·embargo를 h에 맞춘다.** `walk_forward_splits(horizon=h)`가 기본으로 그렇게 한다. h를 바꿀 때 fold 경계가 달라지므로 h 간 비교는 같은 날짜 격자의 valid 구간으로 맞춰야 한다.
5. **holdout은 한 번이다.** 피쳐·h·모델·interaction 선택이 전부 끝난 뒤 2025-08-01 이후 구간을 연다. h60·h120 라벨은 2026-10~11에야 성숙한다. 지금 여는 것은 T1·T2의 예정된 판정을 망친다.
6. **확률 보정은 새 지표다.** 기존 틀은 순위 지표만 본다. 상승 확률 모델은 fold별 valid에서 reliability(예측 확률 구간별 실현 비율)와 Brier/log-loss를 같이 기록해야 한다. 날짜×시장 순위 라벨은 양성 비율이 고정(0.5 또는 0.2)이라 보정 기준선이 명확하다.
7. **국면 변수와 interaction은 KRX 세션 격자**에서 만든다(`06` §3.2·§4.1). fact 격자에서 만든 롤링 창은 2024년 전후로 길이가 다르다.
