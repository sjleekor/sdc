# 07. Phase 1 acceptance gate — 실행 결과와 판정

- 작성일: 2026-08-10
- 선행 문서: [02_feature_candidate.md](02_feature_candidate.md) §6.1(acceptance gate 기준),
  [04_specific_plan_A.md](04_specific_plan_A.md) §11(Phase A 인계물),
  [06_grade_a_deep_dive/](06_grade_a_deep_dive/)(Grade A 6개 단변량 스크리닝 상세)
- 실행 코드: `research/models/_01_20_access_return_rank/experiments/run_grade_a_acceptance_gate.py`
- 원본 결과: `docs/target/01_20_access_return_rank/grade_a_acceptance_gate_results.{md,json}`
  (walk-forward), `grade_a_acceptance_gate_holdout.{md,json}`(holdout)

## 0. 이 문서가 답하는 질문

Phase A는 6개 family를 단변량으로만 검증했다(날짜×시장 조건부 Rank-IC). 이 문서는
그중 **실제로 baseline 모델에 추가할 가치가 있는지**를 다변량·모델 수준에서
검증한다 — 02 §6.1의 남은 기준 ⑤(경제성: decile spread·turnover·거래비용),
⑥(증분성: baseline 대비 OOS 개선), ⑧(단일 holdout 평가)이 대상이다.

## 1. baseline / candidate 정의 — 6개 중 실제로는 4개만 신규

기존 `research/models/_01_20_access_return_rank`(20일 초과수익 랭킹 예측, HGB)는
이미 px 15개 + flow 15개(레거시) + fin 10개, 총 40개 raw 피쳐를 쓰고 있었다.
그중 **`px_amihud_20d`는 그대로, `px_dist_52w_high`는 `px_near_52w_high`와 동일
산식**으로 이미 포함돼 있었다(`02_feature_candidate.md` §4). 즉 Grade A 6개
중 순수 신규 증분성 검증 대상은 4개뿐이다.

- **baseline**: 기존 40개 raw 피쳐(px 15 + flow 15 + fin 10) 그대로.
- **candidate**: baseline + `px_reversal_5d`, `px_maxret_20d`, `px_idio_vol_60d`,
  `flow_individual_netbuy_to_volume_5d`, `flow_individual_netbuy_to_volume_20d`
  (family 카드에 등록된 primary+secondary만; `_60d` 변형은 미등록이라 제외) = 45개.

`build_dataset.py`에 `feature_cols_override` 파라미터를 추가해, `feat_price`/
`feat_flow` 마트가 Phase A0 이후 갖게 된 20개 이상의 신규 컬럼 중 정확히 이 4개
+ 기존 40개만 골라 쓰도록 고정했다(그 외 신규 컬럼 — 레짐 플래그, `_lag1` 변형,
탈락한 D등급 피쳐 등 — 는 자동 제외).

## 2. holdout 전략 — 기존 holdout 재사용 불가

기존 모델의 trailing holdout(2025-12~2026-06-10, ~120거래일)은 이미 두 번 열려
있었다 — fin/ev 그룹 채택 결정, Ridge→HGB 모델 교체 결정에 각각 사용됨
(`docs/target/01_20_access_return_rank/feature_ablation_results.md`,
`improvement_results.md`). "holdout은 한 번만 연다" 원칙에 따라 세 번째로
재사용하지 않고, **2026-06-11~2026-07-31(36거래일)** — 이 모델의 어떤 결정에도
쓰인 적 없는 신규 구간 — 을 이번 게이트 전용으로 한 번만 열었다.

walk-forward(1단계, holdout 미사용)는 `period_end=2026-06-10`으로 자유롭게
반복 실행했고, holdout(2단계, `--confirm-holdout`)은 `period_end=2026-07-31`
+ `holdout_len=36`으로 **정확히 한 번** 실행했다.

## 3. 실행 중 발견한 재현성 버그 (수정함)

첫 walk-forward 실행과 holdout 실행을 반복하는 과정에서, **같은 spec으로 다시
실행하면 Rank IC가 달라지는** 문제를 발견했다. 원인은 `build_dataset.py`의
`_panel_sql`에 `ORDER BY`가 없어 DuckDB 병렬 스캔이 실행마다 다른 행 순서를
반환했고, HGB의 히스토그램 분할이 부동소수점 합산 순서에 완전히 무관하지 않아
`random_state=42`로 고정해도 walk-forward/holdout 지표가 실행마다 흔들렸던
것이었다. `ORDER BY u.trade_date, u.ticker, u.market`를 추가해 패널을 완전히
결정적으로 만들었고(반복 실행 결과가 소수점까지 정확히 일치함을 확인), 이 문서의
모든 수치는 그 수정 이후 재실행한 값이다. 수정 전후로 결론이 달라지지는
않았다(모든 수치가 0.0001~0.0009 범위에서만 이동).

## 4. Phase 1 — walk-forward 결과 (2015-01-02~2026-06-10, 5-fold)

| target | baseline Rank IC | candidate Rank IC | Δ | fold별 개선 | Δ cost-adj spread |
|---|---:|---:|---:|---|---:|
| 5일 | 0.1138 | **0.1180** | +0.0042 | 5/5 | -0.0003 |
| 20일(모델 실제 타깃) | 0.1394 | **0.1474** | +0.0081 | 4/5 | -0.0018 |
| 60일 | 0.1642 | **0.1762** | +0.0121 | 5/5 | -0.0016 |

- **증분성(⑥)**: 3개 horizon 모두, 거의 모든 fold에서 candidate가 baseline보다
  높다. 20일 기준 Δ+0.0081은 기존 fin 그룹 채택 당시의 개선폭(+0.0010,
  `feature_ablation_results.md`)의 8배다 — 이 정도 크기·일관성은 우연으로 보기
  어렵다.
- **경제성(⑤)**: turnover 차감 후 top-decile spread는 3개 horizon 전부에서
  candidate가 baseline보다 **낮다**(-0.0003~-0.0018). Rank IC(전체 횡단면
  순위상관, 수천 개 날짜 표본)는 개선됐지만, 겹치지 않는 리밸런싱 시점만 쓰는
  상위 10% 포트폴리오 수익(표본 23~280개 리밸런싱)은 오히려 소폭 나쁘다 — "전체
  순위는 더 잘 맞히지만 극단 상위 10%만 놓고 보면 딱히 좋아지지 않는다"는 뜻이다.

## 5. Phase 2 — holdout 결과 (2026-06-11~2026-07-31, 36거래일, 1회 확정)

| target | baseline Rank IC | candidate Rank IC | Δ | n_dates | n_rebalances |
|---|---:|---:|---:|---:|---:|
| 5일 | 0.2097 | 0.1912 | **-0.0185** | 16 | 4 |
| 20일 | 0.2431 | **0.3347** | **+0.0916** | 16 | 1 |
| 60일 | NaN | NaN | — | 0 | 0 |

- **60일은 평가 불가**다 — holdout이 데이터의 마지막 36거래일이라, 60일 뒤
  수익률을 계산할 미래 데이터가 아예 없다(버그가 아니라 구조적 한계. 약 60거래일,
  대략 2026년 10~11월경 데이터가 쌓이면 재평가 가능).
  `dim_universe_daily`가 label_horizon=20을 기준으로 tail을 미리 잘라내는
  기존 설계 때문에 5일·20일 타깃도 36일이 아니라 16일치만 평가됐다(5일 쪽이
  손해를 보는 보수적 설계지만, 이번 acceptance gate가 새로 만든 문제는 아니다).
- **5일은 오히려 악화**(-0.0185) — walk-forward의 +0.0042와 방향이 반대다.
- **20일은 극적으로 개선**(+0.0916) — walk-forward의 +0.0081보다 11배 크다.
- 두 결과 모두 **표본이 극히 작다**(평가일 16일, 20일 타깃은 겹치지 않는
  리밸런싱 시점이 딱 1개뿐이라 turnover 자체를 계산할 수 없다). walk-forward
  대비 11배 큰 개선이나 방향이 뒤집히는 악화 모두, 실제 효과 크기의 반영이라기보다
  **소표본 노이즈**로 보는 것이 합리적이다 — 이 홀드아웃 하나만으로 결론을
  뒤집거나 확정하지 않는다.

## 6. 종합 판정

| 기준(02 §6.1) | 결과 |
|---|---|
| ⑤ 경제성 | **엇갈림** — Rank IC는 개선되나 turnover 차감 top-decile spread는 walk-forward 3개 horizon 전부·holdout 20일에서 candidate가 baseline보다 낮거나 우열이 불명확 |
| ⑥ 증분성 | **충족** — walk-forward(대표본, 11년, 5-fold)에서 3개 horizon·거의 모든 fold가 일관되게 개선 |
| ⑧ 단일 holdout | **결론 불확정** — 표본이 너무 작아(16일, 리밸런싱 1회) 확증도 반증도 아님 |

**권고**: `px_reversal_5d`, `px_maxret_20d`, `px_idio_vol_60d`,
`flow_individual_netbuy_to_volume_{5,20}d`를 baseline에 **조건부 채택**한다.

- 채택 근거: 증분성이 대표본에서 크고 일관됨(⑥ 확실히 충족).
- 조건: 경제성(⑤)이 깨끗하지 않으므로, **상위 10% 이하로 좁힌 실제 top-k 매수
  리스트(`predict.py`의 k=100) 기준으로도 개선이 유지되는지** 별도 확인이
  필요하다 — 이번 게이트는 decile 단위 economic_report로만 봤고, k=100
  고정 리스트의 turnover/비용은 아직 확인하지 않았다.
- h=60일 holdout, 그리고 지금의 36일보다 긴 holdout 재확인은 **2026년 10~11월
  이후 데이터가 쌓인 뒤 새 구간으로 한 번 더**(이번 구간을 재사용하지 않고) 여는
  것을 다음 체크포인트로 남긴다.

### 6.1 k=100 조건 확인 결과 (2026-08-12) — 조건 미충족

위 조건 중 첫 번째를 실행했다. 산출물은
[`docs/target/01_20_access_return_rank/topk_cost_check.md`](../../../target/01_20_access_return_rank/topk_cost_check.md),
코드는 `research/models/_01_20_access_return_rank/experiments/run_topk_cost_check.py`.

Phase 1의 walk-forward 예측을 다시 만들어 decile 리포트와 k=100 리포트를 **같은 fold에서
동시에** 계산했다. 데이터셋 재빌드도 하이퍼파라미터 재탐색도 하지 않았고, holdout(fold 6)은
읽지 않았다. decile 값 6개가 게이트 기록과 소수 넷째 자리까지 일치해, 옆에 놓인 k=100 값이
같은 기준 위에 있다는 것이 확인된다.

| h | Δ decile net | Δ k=100 net | k=100에서 개선 유지 |
|---|---|---|---|
| 5일 | -0.0003 | -0.0002 | 아니오 |
| **20일 (배포 대상)** | -0.0018 | **-0.0045** | **아니오** |
| 60일 | -0.0016 | +0.0092 | 예 |

**조건은 충족되지 않았다.** 배포 대상인 20일에서 candidate는 k=100 비용차감 후 0.0155로
baseline 0.0200보다 낮다. decile에서 이미 -0.0018이던 격차가 실제 매수 리스트로 좁히면
-0.0045로 2.5배 벌어진다. 리스트를 좁힐수록 불리해진다는 뜻이라, §6이 남긴 질문에 대한
답은 "유지되지 않는다"이다.

60일만 방향이 반대다. baseline은 k=100에서 -0.0014로 손실인데 candidate가 +0.0078로
뒤집는다. 다만 3개 중 1개이고, 60일은 holdout 재평가가 아직 열리지 않은 horizon이라
우연을 배제할 수 없다. 단독 근거로 쓰지 않는다.

**갱신된 권고**: 4개 net-new Grade A candidate를 **20일 배포 모델에 채택하지 않는다.**
증분성(⑥)은 그대로 유효하므로 순위 신호로서의 가치는 부정되지 않지만, 실제 거래하는
리스트 기준 경제성이 배포 horizon에서 개선되지 않는다. 60일 계열은 별도 검토 대상으로
남기고, 10~11월 holdout 재평가 때 같이 본다.

부수적으로 기록해둘 관측 하나. baseline 자체도 60일 k=100에서 -0.0014로 손실이다.
같은 예측인데 decile은 +0.0084다. 장기 horizon에서 상위 100종목으로 좁히는 것이
decile보다 불리하다는 신호이며, candidate 판정과 무관하게 배포 구성에서 확인할 값이다.

## 7. 재현

```bash
uv run python -m research.models._01_20_access_return_rank.experiments.run_grade_a_acceptance_gate            # Phase 1, 반복 가능
uv run python -m research.models._01_20_access_return_rank.experiments.run_grade_a_acceptance_gate --confirm-holdout  # Phase 2, 1회 한정 — 이미 실행됨, 재실행 시 --force 없이는 거부됨
```
