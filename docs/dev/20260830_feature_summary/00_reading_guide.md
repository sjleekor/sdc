# 00. 읽는 법 — 모델 개발 관점에서 피쳐를 정리하는 기준

- 작성일: 2026-09-03
- 목적: 지금까지 만든 피쳐를 **예측 모델 개발자가 바로 쓸 수 있는 카탈로그**로 정리한다.
  검증 판정(채택·비채택)을 다시 내리는 문서가 아니다. 판정의 정본은
  `docs/dev/20260731_raw_features/01_feature_candidate/00_status.md`와
  `reports/feature_performance/`다.
- 입력: `docs/dev/20260829_raw_features_explain/`(35 family 해설),
  `docs/dev/20260829_macro_features/`(매크로 6 family·Phase C), `docs/target/01_20_access_return_rank/`
  (baseline 모델·acceptance gate), `research/etl/features/*.py`(산식 정본).

---

## 1. 만들려는 모델과 이 문서 묶음의 관계

목표 모델은 **"특정 시점(h거래일) 뒤 주가가 오를 확률"**을 내는 분류 모델이다. 조건이 셋 있다.

1. **h를 여러 개 실험한다.** 단기(5일 안팎)부터 장기(60~120일)까지. 여유 자금은 길게, 단기 수익은 짧게 볼 수 있어야 한다.
2. **피쳐 조합, 종목 간 관계, 거시 국면을 한 모델에서 같이 본다.**
3. 확률값이 나와야 한다. 순위만 내는 모델과 다르다.

지금까지의 검증은 이 목표와 겹치는 부분과 다른 부분이 있다.

| | 기존 검증(Horizon Scan·acceptance gate) | 목표 모델 |
|---|---|---|
| 라벨 | `(거래일, 시장)` 안 초과수익률 **순위**(`y_rank_{h}d`) | 상승 확률. 시장 대비인지 절대 상승인지는 정해야 한다 (`07` §1) |
| horizon | 1·2·3·5·10·20·40·60·120 누적 + 6개 구간(bucket) | 여러 h 실험. 격자는 그대로 쓸 수 있다 |
| 단위 | 피쳐 하나 × cell 하나(단변량) | 피쳐 조합 |
| 종목 간 관계 | 시장 평균 중립화만 | 업종·peer·리드래그 등 아직 없음 (`08` §3) |
| 거시 | exposure 베타 6개 + 국면별 조건부 IC(Phase C) | 국면 변수·interaction을 입력으로 |
| 모델 | Ridge → HistGradientBoosting, purged walk-forward 5-fold | 자유. 단 검증 틀은 그대로 |

그래서 이 문서 묶음은 **단변량 검증 결과를 모델 입력 설계에 필요한 형태로 재배열**한 것이다. 피쳐마다 "얼마나 예측하나"뿐 아니라 "언제 알 수 있나, 얼마나 자주 바뀌나, 어디가 비어 있나, 무엇과 겹치나, 어느 국면에서 다르나"를 같은 자리에 적었다.

---

## 2. 모델 개발 관점에서 확인해야 할 피쳐 특성 — 검토 결과

문서를 쓰기 전에 "이 모델에는 피쳐의 어떤 특성을 알아야 하는가"를 먼저 정리했다. 12가지다. 각 항목에 **기존 프로세스가 이미 잰 것**과 **아직 없는 것**을 같이 적었다. 카드의 절 이름은 `02`~`05`의 family 카드 구조와 같다.

| # | 특성 | 왜 이 모델에서 중요한가 | 기존 프로세스가 잰 것 | 아직 없는 것 | 카드의 절 |
|---|---|---|---|---|---|
| 1 | **정의·계보** (산식, 버전, config hash) | 같은 이름의 피쳐가 시점마다 다른 값을 낼 수 있다(`fin_v3→v4`, `filing_v1→v3`). 결과를 재현하려면 어느 정의인지 알아야 한다 | 산식 SQL, `*_FORMULA_VERSION`, 사전등록 config hash, run_id | — | 머리표, 산식과 규칙 |
| 2 | **척도와 분포** (수준/변화/서프라이즈, 비율·로그·건수·플래그, 꼬리, 0 과다, 동점) | 변환(rank·winsor·log)과 결측 처리를 정한다. 건수·플래그형은 동점이 많고 이력 초기에 횡단면이 상수가 된다(I13) | 척도 유형, 동점으로 q5 spread가 NaN이 된 사례, 상수 횡단면 사고 | 분위별 분포·왜도 수치, 중앙값 spread | 산식과 규칙, 모델 투입 메모 |
| 3 | **가용 시점(PIT)과 지연** | 학습 시점에 실제로 알 수 있었던 값만 써야 한다. 원천마다 규칙이 다르다: 당일 종가 / 다음 세션 / 접수일 다음 거래일 / period_end+90·45일 / strict vintage / KRX T+1 | 원천별 PIT 규칙, `lag1` 유지율(하루 늦게 써도 남는 비율), look-ahead 위반 0 검증 | fin_pit(모델 baseline)과 fin_scan(검증)의 PIT 규칙이 **다르다**. 모델에 넣을 때 하나로 맞춰야 한다 (`08` §2.10) | 가용 시점(PIT)과 지연 |
| 4 | **갱신 빈도와 지속성** | 일별 롤링 피쳐는 매일 바뀌고 분기·연간 피쳐는 계단식이다. 회전율·거래비용과 어느 h에 맞는지를 정한다 | 신호 모양(onset·peak·half-life), lag1 유지율, 원천 갱신 주기 | 피쳐 자체의 자기상관·회전율 직접 측정 | 가용 시점, 모델 투입 메모 |
| 5 | **커버리지와 표본** (시작일, 종목 비율, NULL 원인, 시장 비중, 생존편향) | 결측 패턴이 시대를 알려 주면 모델이 "연도"를 학습한다. 커버리지가 낮은 피쳐를 넣으면 표본이 그 피쳐가 있는 종목으로 좁아진다 | 유효 시작일, `coverage_ratio`, 날짜당 종목 수, KOSPI/KOSDAQ 비중, `common_survivor` vs `available` 대조 | — | 표본과 커버리지 |
| 6 | **예측력의 크기와 모양** (IC, ICIR, t, q, 5분위 spread, 부호, horizon 밴드) | 어느 h 실험에 넣을지, 부호를 뒤집을지, 순위용인지 분위 전략용인지 정한다 | cell별 Rank IC·ICIR·t_nw·BH q·`q5_spread_aligned`, 사전등록 밴드 대 관측 밴드 | IC와 spread가 어긋나는 원인을 풀 분위별 평균수익률 | 예측력 |
| 7 | **강건성** | 유의하지만 쓸 수 없는 피쳐를 걸러 낸다: 거래 불가 종목에 몰린 신호, 하루 지연에 사라지는 신호, 상폐 포함 시 부호 반전, 특정 국면에만 있는 신호, 시계열 placebo | `tradable_retention`, `lag1` 유지율, `available` 부호, 기간 일관성(5구간), 시간 placebo, 비중첩 offset, Phase B source 경고 | 시간 placebo는 NW lag ≥ 59 cell만 받았다. 짧은 cell의 A등급은 이 검사를 "통과"한 게 아니라 "받지 않은" 것이다 | 강건성 |
| 8 | **중복성·독립성** | 선형 모델은 다중공선성, 트리 모델은 낭비. "14개 후보 = 14개 독립 정보"가 아니다 | A×B 교차 순위상관 204쌍, 매크로 × Phase A 상관 | **A×A·B×B 상관이 없다.** 같은 마트 형제(전체/부분집합, 분자·분모 공유)는 구조적으로만 알고 있다 | 중복성 |
| 9 | **국면 의존** | 거시를 "한 번에 고려"하는 가장 검증된 길이다. 어느 피쳐의 IC가 어느 국면에서 달라지는지 알면 interaction을 명시적으로 만들 수 있다 | Phase C 15쌍(4쌍 통과), 국면 변수 7개 정의·점유율·지속성 | 재무·이벤트 18 family는 Phase C에 넣지 않았다 | 국면 의존 (`06` §4) |
| 10 | **종목 간 관계** | 사용자가 원하는 "다른 종목과의 조합". 업종·peer 상대 위치·리드래그 | 시장 평균 중립화, `px_market_beta`, 업종 중립 진단 variant(PIT 아님) | **PIT 업종 코드가 없다.** 관계 피쳐 0개 | `08` §3 |
| 11 | **검증 상태와 채택 상태** | discovery → screen_pass → 등급 → validation 개선 → 채택은 각각 다른 판정이다. 등급 A라고 모델에 넣으면 이득이라는 뜻이 아니다(T1이 그 사례) | 네 단계 전부 기록 | 최종 holdout(2026-10~11) | 머리표(모델 검증 이력) |
| 12 | **실행 비용** | 확률이 높다고 사도 비용을 빼면 남지 않을 수 있다. 회전이 빠른 피쳐일수록 그렇다 | acceptance gate의 turnover·비용 반영 spread(왕복 60bp), k=100 실매수 비용 확인 | 피쳐 단위 비용은 없다. 묶음 단위만 있다 | 모델 투입 메모 |

이 표가 곧 이 문서 묶음의 설계다. 12개 중 기존 프로세스가 직접 잰 것은 카드에 숫자로 적었고, 없는 것은 `미측정`으로 두고 `08` §3에 모았다.

### 2.1 검토에서 얻은 결론 넷

1. **PIT 규칙을 통일하는 것이 첫 작업이다.** baseline 모델의 `fin_*` 10개는 `stock_metric_fact`에 보수적 지연(+90/+45일)을 붙인 값이고, Horizon Scan의 `fin_*` 5개는 strict PIT vintage 값이다. 같은 `fin_` 접두어인데 시점 규칙이 다르다. 모델에 둘을 섞어 넣기 전에 하나로 맞춰야 한다.
   수급도 같은 문제가 있다. **`flow_*`의 검증 정본은 하루 지연값(`lag1`)**이다 — 투자자별 집계를 장 마감 시점에 쓸 수 있는지 검증되지 않았다(`flow_unverified_same_day_variant: lag1`). baseline 모델은 당일 값을 쓴다. 모델에서 당일 값을 쓰면 검증 결과보다 좋게 나올 수 있고, 그 차이는 미측정이다.
2. **국면 의존이 확인된 피쳐가 넷 있다.** `px_reversal_5d`(VIX 수준), `px_turnover_shock`·`flow_foreign_netbuy_to_volume`(유동성), `px_market_beta`(상승장). "거시까지 한 번에"는 이 넷의 interaction부터 시작하는 것이 검증된 길이다. 매크로 level을 피쳐로 넣는 길은 순위 라벨 틀에서는 막혀 있고, pooled 트리 모델에서 분기 조건으로는 쓸 수 있으나 해석이 어렵다(`06` §1).
3. **종목 간 관계 피쳐는 지금 없다.** 업종 코드가 PIT로 수집돼 있지 않아 업종 중립화도, peer 상대 피쳐도 만들 수 없다. 이건 이 문서가 채울 수 없는 공백이고, 수집 작업이다(`08` §3).

---

## 3. 기존 검증 프로세스와 품질 지표 사전

### 3.1 프로세스

```
raw(Postgres) → 마트(DuckDB parquet, research/etl/features/) → A0 공통 마트
  → Phase A  (px·flow 17 family, 75 primary 가설)      단변량 Rank IC 스캔 + BH
  → Phase B  (fin·ev·own·hc·macro 24 family, 102 cell)  같은 스캔 + source 품질 게이트
  → Phase AB (결합 BH, 177 가설)                        screen_pass · 등급 확정
  → Phase C  (국면별 조건부 IC, 15쌍)                    δ = E[IC|국면=1] − E[IC|국면=0]
  → acceptance gate (T1·T2)                             baseline 40 피쳐 + 후보를 purged walk-forward로 비교
  → holdout (2025-08-01~)                               아직 열지 않았다. 2026-10~11 한 번만
```

모든 판정 기준은 **결과를 보기 전에** config YAML에 사전등록됐다(`horizon_scan_config.yaml`, `_expansion_20260827.yaml`, `_macro_20260829.yaml`). 이 문서의 숫자는 그 사전등록 run의 산출물에서만 왔다.

### 3.2 지표 사전

| 지표 | 정의 | 읽는 기준 |
|---|---|---|
| **Rank IC** (`ic_mean`) | `(거래일, 시장)`마다 피쳐와 `y_rank`의 Spearman 상관, 종목 수 가중으로 하루치 → 날짜 평균. 20종목 미만 횡단면은 버림 | 0.01~0.02 약함, 0.03~0.05 단일 신호로 쓸 만함, 0.05~0.10 강함(중복·편향 의심), 0.10 이상은 대개 규모·유동성 축 |
| **ICIR** | `ic_mean / ic_std` | 클수록 안정 |
| **t_nw** | Newey-West 보정 t. lag는 누적 h−1, bucket은 width−1, KRX 세션 거리 기준 | \|t\| 3 이상이면 exploratory에서도 주목 |
| **BH q** | 전역 BH FDR(q=0.10). Phase A 75 → AB 153/177 모집단 | q < 0.10이면 `discovery` |
| **q5_spread_aligned** | 상위 20% − 하위 20% 평균 초과수익률(해당 h 누적, 비용 차감 전, 50종목 미만 버림), 기대 부호로 정렬 | **IC와 다른 이야기를 할 수 있다.** 순위 모델이면 IC, 분위 롱숏이면 spread |
| **tradable_retention** | \|tradable IC\| / \|broad IC\|. tradable = 20일 평균 거래대금 1억 이상 + 종가 1,000원 이상 | 0.50 미만 게이트 실패. 분모 \|IC\|가 0에 가까우면 폭주하므로 절대 크기와 같이 본다 |
| **lag1 유지율** | \|lag1 IC\| / \|native IC\| | 0.50 미만 실패. "하루 늦으면 얼마가 사라지나" |
| **available 부호** | 상폐 포함 표본에서 부호가 뒤집히면 등급 상한 C | |
| **기간 일관성** | 5구간(`2014_2016`·`2017_2019`·`2020_2021`·`2022_2023_10`·`2023_11_~`) 중 부호 같은 수 | 5/5가 전 구간 같은 방향 |
| **시간 placebo** | 피쳐 시계열을 120세션 이상 원형 이동시켜 100회 재계산. **NW lag ≥ 59 cell만** | p ≤ 0.10 통과. 대상이 아니면 `robustness_required=False` |
| **source 경고** (Phase B) | `revision_ratio` > 0.10, `mapping_fallback_ratio` > 0.50, `value_mismatch_ratio` > 0.01 또는 측정 불가 | 넘으면 **A만 막는다**(B 아래로는 안 내린다) |
| **discovery** | BH 통과 cell | 통계 |
| **screen_pass** | discovery + 방향·기간·유동성·지연·강건성 게이트 전부 | screening 통과 |
| **등급** A/B/C/D/R | A: screen_pass+핵심 경고 없음+모든 offset 평가 가능 / B: 비치명 경고 / C: 탐색·보조·available 반전(Phase A) 또는 강건성 실패(Phase B) / D: 신호 없음·부호 반대 / R: 기준용 | **screening 근거 등급이다. 모델 채택이 아니다** |
| **validation improved** | acceptance gate valid 구간에서 baseline보다 Rank IC·비용 반영 spread 개선 | 최종 채택은 holdout 뒤 |

### 3.3 등급을 읽을 때 반드시 같이 볼 것

- **`robustness_required`.** h40–60 bucket(width 20)은 NW lag 19라 시간 placebo 대상이 아니다. 그 cell의 A는 세 게이트를 다 거친 A가 아니다. `fin_log_mcap` 긴 cell 3개처럼 실제로 통과한 것과 구분한다.
- **Phase A의 C와 Phase B의 C는 다르다.** A의 C는 "애초에 후보가 아닌 역할"(공매도 4개는 2020-03 금지로 표본이 끊겨 판정 보류), B의 C는 "판정했는데 강건성에서 떨어짐"이다.
- **세 가지 `—`.** `reference_only`(안 쟀다, `px_zero_ret_ratio_20d`) / `insufficient`(재려 했는데 표본 없음, `fin_sue`) / `exploratory_short_regime`(진단용으로만 쟀다, 공매도 4개). "재보니 0"과도 다르다.
- **부호가 반대인 것과 신호가 없는 것은 다르다.** `px_turnover_shock`·`flow_foreign_*`·`flow_inst_*`는 기대와 반대 방향이 5/5 구간 일관됐다. 단변량 검증에서는 D지만 **모델에는 부호를 뒤집어 넣을 수 있는 정보**다. `fin_asset_growth_yoy`·`own_insider_filing_activity`는 신호 자체가 없다.

---

## 4. 기준 산출물

이 묶음의 숫자는 아래 run에서만 왔다. 다른 run 값을 섞지 않았다.

| 구분 | run_id | config | 비고 |
|---|---|---|---|
| Phase A | `20260827T221729-4e0ae8b0` | `889c3e83…` | px·flow 17 family, 412 cell |
| Phase B | `20260828T123313-4e0ae8b0` | `889c3e83…` | 18 family 78 cell |
| 결합 AB | `20260828T165038-4e0ae8b0` | `889c3e83…` | 153 가설, discovery 87, screen_pass 40 |
| Phase A/B/AB (매크로 overlay) | `20260830T085718` / `T100518` / `T122850-efd35e70` | `236d0d35…` | +6 family 24 cell, 177 가설. 기존 판정 변화 0 |
| Phase C | `20260830T122850-phasec` | `236d0d35…` | 15쌍 + reference 2 |
| T1 acceptance gate | `grade_a_acceptance_gate_results.json` (2026-08-24) | baseline 40 vs +5 | hgb, h5/20/60 |
| T2 acceptance gate | `phase_b_acceptance_gate_results.json` (2026-08-28) | baseline 40 vs +14 | hgb, h5/20/60 |

- snapshot `2026-08-23`, source `sj2_remote`. holdout 경계 `2025-08-01`. `common_survivor` 표본은 120일 라벨 때문에 formation이 `2025-02-05`에서 끝난다.
- 산식 정본은 `research/etl/features/*.py`, 사전등록 정본은 `research/analysis/horizon_scan*.yaml`.

---

## 5. 문서 지도

| 파일 | 내용 | 누가 쓴 자료인가 |
|---|---|---|
| **`00_reading_guide.md`** | 이 문서. 모델 관점 특성 검토, 지표 사전, 기준 run | — |
| [`01_feature_inventory.md`](01_feature_inventory.md) | **전체 피쳐 컬럼 목록** — 마트별, 등급·screen_pass·모델 사용 여부 한 표 | 마트 컬럼 + AB 결과 |
| [`02_price_liquidity.md`](02_price_liquidity.md) | 가격·유동성 `px_*` 9 family 카드 + 미검정 15 | 해설 01~09 |
| [`03_investor_flow_short.md`](03_investor_flow_short.md) | 수급·공매도 `flow_*` 8 family 카드 + 미검정 15 | 해설 10~17 |
| [`04_financial_size_value.md`](04_financial_size_value.md) | 재무·규모·밸류·인적자본 9 family 카드 + `fin_pit` 10 | 해설 22~30 |
| [`05_filing_event_ownership.md`](05_filing_event_ownership.md) | 공시·자본정책·지분 9 family 카드 + `feat_event` 3 | 해설 18~21, 31~35 |
| [`06_macro_regime.md`](06_macro_regime.md) | 매크로 exposure 베타 6 family, 공통 피쳐 카탈로그 55, 국면 변수 7, Phase C 결과 | 매크로 라운드 문서 |
| [`07_labels_sample_validation.md`](07_labels_sample_validation.md) | 라벨 정의, 표본·유니버스 규칙, 검증 틀(walk-forward·비용), 다중 horizon 실험에 주는 함의 | `labels.py`·config·acceptance gate |
| [`08_quality_summary_gaps.md`](08_quality_summary_gaps.md) | 판정 종합, 횡단 품질 이슈, 모델 개발 공백과 우선순위, horizon별 피쳐 배치 제안 | 종합 |

family 카드(`02`~`05`)는 같은 절 구조를 쓴다: 머리표 → 산식과 규칙 → 가용 시점(PIT)과 지연 → 예측력 → 강건성 → 표본과 커버리지 → 중복성 → 국면 의존(해당 시) → 모델 투입 메모 → 한계.

---

## 6. 이 문서를 쓰는 사람이 지켜야 할 것

1. **holdout(2025-08-01~)을 열지 않는다.** 모델 실험은 그 이전 구간에서 purged walk-forward로만 한다. holdout은 피쳐·horizon·모델 선택이 전부 끝난 뒤 한 번만 쓴다(2026-10~11 예정).
2. **등급은 모델 채택이 아니다.** T1의 A등급 4개는 walk-forward에서 Rank IC가 개선됐지만 k=100 비용 반영 수익에서 h20이 음수여서 비채택됐다. 카드의 "모델 검증 이력"을 등급과 같이 본다.
3. **사전등록 밴드 밖의 horizon을 대표 성능으로 쓰지 않는다.** exploratory cell은 진단용이다. 다만 모델 실험은 새 사전등록이므로, 어느 h에 어느 피쳐를 넣을지는 `08` §4의 제안을 출발점으로 삼되 결과를 보고 옮기지 않는다.
4. **새 피쳐·새 정의는 새 config다.** 기존 `config_hash`의 YAML을 고치지 않는다.
5. **숫자를 다른 run과 섞지 않는다.** 이 묶음은 §4의 run만 인용한다. `09_all_feature_results.md`(config `ab0de634…`, 25 family)와 대표 IC가 다를 수 있는데, 그건 모집단이 달라서다.
