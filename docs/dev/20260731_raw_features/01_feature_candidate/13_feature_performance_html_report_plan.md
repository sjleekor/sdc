# 13. 피쳐 성능 HTML 보고서 작성 계획

- 작성일: 2026-08-28
- 상태: 구현 및 자동·브라우저 검증 완료
- 기준 snapshot: `2026-08-23`
- 기준 config: `889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea`
- 보고서 판정 범위: **validation 완료 / h60 최종 holdout 대기**

## 1. 결론

현재 산출물로 35개 family의 단변량 예측력, 다중검정, 강건성, coverage와 T1·T2 모델
validation을 한 화면에 정리한 HTML 보고서를 만든다. 보고서는 외부 서버나 CDN이 없어도 열리는
**self-contained 정적 HTML**로 만든다.

이번 보고서는 최종 채택 보고서가 아니다. T1의 20일 모델 비채택과 T2의 valid 구간 개선은
확정해서 적되, T1·T2 h60 최종 holdout은 `pending`으로 표시한다. 2026년 10~11월 이후 새 h60
구간을 한 번 평가하면 별도 버전으로 갱신한다.

### 1.1 2026-08-28 리뷰 반영 판단

`sdc_13_html_report_plan_review_20260828.md`의 11개 항목을 실제 A/B/AB·T1·T2 산출물과
다시 대조했다.

| 리뷰 항목 | 판단 | 반영 방식 |
|---|---|---|
| 1. 35 family 정본 | **수정 반영** | A card 17 ∪ B summary 18로 고정. AB에 없는 공매도 4개는 exploratory IC를 별도 표시하고 reference 1개는 `—`로 표시 |
| 2. run 선택 인자 | **반영** | `--ab-run-id`를 필수로 받고 A/B run은 AB manifest에서만 도출 |
| 3. manifest 한계 | **반영** | run id·content hash는 `_SUCCESS`, Git 정보는 run spec에서 읽고 원본 JSON 전체 embed 금지 |
| 4. T1 JSON lineage 차이 | **수정 반영** | 8월 24일 decile과 8월 12일 k=100을 별도 결과로 표시. 숫자는 섞지 않고 k=100 gate 규칙으로 h20 판정 |
| 5. B family summary placeholder | **반영** | coverage·readiness 설명만 사용하고 등급·AB 판정은 AB cell에서 다시 집계 |
| 6. coverage 정의 | **반영** | A/B 공통 x축은 cell `n_obs_mean`. B coverage ratio는 보조 정보, A는 `—` |
| 7. 상관 heatmap 범위 | **반영** | A primary 12 × B primary 17 교차 행렬로 한정 |
| 8. insufficient 6 cell | **반영** | `fin_sue` 6개를 별도 marker와 `—`로 표시하고 ready와 evaluable을 구분 |
| 9. 테스트 위치 | **반영** | fixture 단위 테스트와 로컬 산출물 기반 integration 회귀 테스트로 분리 |
| 10. Plotly 크기 | **반영** | Plotly JS 한 번만 inline, WebGL 미사용, HTML 10MB 상한 |
| 11. 소소한 보완 | **부분 반영** | lane 단위·기준값·B plot 부재는 반영. generated manifest 자동 커밋은 하지 않고 명시적 검토·승인 때만 sanitized 사본을 남김 |

## 2. 산출물 위치

| 구분 | 경로 | Git 관리 |
|---|---|---|
| 이 계획 | `docs/dev/20260731_raw_features/01_feature_candidate/13_feature_performance_html_report_plan.md` | 추적 |
| 생성기 | `research/analysis/feature_performance_report.py` | 추적 |
| 단위 테스트 | `tests/unit/test_feature_performance_report.py` | 추적 |
| 회귀 테스트 | `tests/integration/test_feature_performance_report_regression.py` | 추적 |
| HTML | `reports/feature_performance/20260828_validation/index.html` | `.gitignore`, 로컬 산출물 |
| 생성 manifest | `reports/feature_performance/20260828_validation/report_manifest.json` | `.gitignore`, 로컬 산출물 |

`reports/`는 기존 profiling HTML과 같은 로컬 산출물 위치다. HTML에는 원본 row나 종목별 KIS
데이터를 넣지 않는다. 집계된 성능 지표와 원천 이름만 담는다.

기본 실행 명령은 다음 형태로 고정한다.

```bash
uv run --extra analysis python -m research.analysis.feature_performance_report \
  --snapshot-date 2026-08-23 \
  --source sj2_remote \
  --config-hash 889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea \
  --ab-run-id 20260828T165038-4e0ae8b0 \
  --output-dir reports/feature_performance/20260828_validation
```

`--ab-run-id`는 필수다. `--source` 기본값은 `sj2_remote`다. 생성기는 `latest` 디렉터리를
자동 선택하지 않는다. A/B run id를 별도 인자로 받지 않고 AB manifest의
`phase_a_run_id`·`phase_b_run_id`에서만 도출한다. 이 규칙으로 서로 맞지 않는 run을 조합하지
못하게 한다.

`reports/` 아래 v1 보존은 로컬에만 적용된다. 장기 보존이 필요하면 생성 뒤 사람이 내용을
검토하고 홈 경로·비밀정보를 뺀 sanitized manifest만 별도 승인을 받아 docs에 추가한다. 생성기가
추적 경로에 자동으로 쓰지는 않는다.

## 3. 보고서가 답할 질문

1. 어떤 feature family를 어떤 원천과 산식으로 평가했는가?
2. 어느 horizon에서 IC가 나타나고 얼마나 유지되는가?
3. 기대 부호, BH FDR, tradable·기간·delay·temporal placebo 게이트를 통과했는가?
4. coverage와 표본 수가 성능 해석에 충분한가?
5. A primary와 B primary 사이에서 사실상 같은 정보를 담는 feature는 무엇인가?
6. 단변량 screening 결과가 모델에 넣었을 때도 개선으로 이어졌는가?
7. 현재 채택·비채택·보류·holdout 대기 상태는 무엇인가?

## 4. 포함 범위와 제외 범위

### 4.1 포함

- Phase A 가격·수급 17 family. 이 중 primary 12 family·75개 가설은 AB에 들어가고,
  공매도 `exploratory_short_regime` 4 family·28개 cell은 진단으로만 따로 보여 준다.
  `reference_only` 1 family는 IC row가 없음을 표시한다.
- Phase B 기존 8 family와 확장 10 family, 78개 가설
- 결합 AB 153개 가설의 global BH 결과
- Phase A family card의 IC·강건성·등급과 horizon cell의 표본 규모
- Phase B cell의 `screen_pass`, `failed_gates`, source quality와 등급
- A/B primary feature rank correlation
- T1 acceptance gate와 k=100 비용 확인
- T2 14-feature 묶음 validation
- Phase C를 열지 않은 근거
- h60 최종 holdout 대기 상태

공매도 exploratory 28개 cell은 IC heatmap과 family 상세 curve에 별도 band·점선 marker로
포함한다. primary discovery·global BH·AB·모델 validation과 섞지 않으며 기본 필터에서도
`exploratory`라고 표시한다. `px_zero_ret_ratio_20d`는 reference 역할만 보여 주고 IC·q·discovery는
`—`로 둔다. N8 Phase C regime 후보와 Phase A의 `exploratory_short_regime`은 서로 다른
항목이다.

### 4.2 제외

- N7 KIS 횡단면 대조 값과 종목별 KIS 데이터
- look-ahead 때문에 diagnostic-only인 N2 업종 중립 variant
- dormant 상태인 N8 regime을 평가한 것처럼 보이는 차트
- 원본 종목·날짜 row와 실제 매수 종목 목록
- 아직 만들지 않은 `daily_ic`·`cohort_ic` 원시 시계열
- h60 holdout을 미리 열거나 기존 holdout을 재사용하는 작업
- HTML에서 DB·sj2-server·외부 API를 직접 조회하는 기능

`daily_ic`·`cohort_ic`가 없어도 horizon·subperiod·coverage·모델 validation 보고서는 만들 수
있다. 달력 날짜별 IC 시계열 차트는 B-10 Stage 3을 별도 계획으로 열기 전에는 넣지 않는다.

## 5. 정본 입력

### 5.0 family 목록 정본

보고서의 35 family 목록은 다음 합집합으로 고정한다.

```text
Phase A cards/family_cards.json 17개
∪ Phase B core/family_summary.parquet 18개
= 35개
```

AB `combined_ab_primary_hypotheses.parquet`는 최종 판정 overlay지만 family는 30개만 담는다.
Phase A의 exploratory 4 family와 reference 1 family는 AB에 없다. 따라서 AB parquet만 읽어
전체 family 목록을 만들지 않는다.

### 5.1 Phase A

- run: `20260827T221729-4e0ae8b0`
- `manifest.json`, `_SUCCESS.json`, `run_spec.json`
- `core/horizon_ic.parquet`
- `cards/family_cards.json`
- 기존 `plots/*.png` 119개는 대조 자료로만 사용한다. 새 보고서의 공통 차트는 A/B를 같은
  규칙으로 다시 그린다.

Phase A primary 75개를 셀 때는 `hypothesis_role=primary`, `universe=broad`,
`sample_kind=common_survivor`를 모두 적용한다. `horizon_ic.parquet` 전체 412행이나 고유
`hypothesis_id` 103개를 primary 가설 수로 쓰지 않는다. exploratory 공매도는 같은 broad·
common-survivor 조합에서 `hypothesis_role=exploratory_short_regime` 28개를 따로 읽는다.

### 5.2 Phase B

- run: `20260828T123313-4e0ae8b0`
- `manifest.json`, `_SUCCESS.json`, `phase_b_run_spec.json`
- `core/phase_b_primary_hypotheses.parquet`
- `core/horizon_ic.parquet`
- `core/feature_coverage.parquet`
- `core/family_summary.parquet`
- `core/temporal_placebo_summary.parquet`
- `core/nonoverlap_summary.parquet`
- `core/family_cards.md`는 설명 대조용으로만 읽는다. 최종 등급은 AB 결과를 쓴다.

`family_summary.parquet`는 AB 전 산출물이라 18 family의 `evidence_grade`가 모두 `NE`이고
AB q·discovery·screen-pass가 placeholder다. 이 파일에서는 `coverage_ratio`,
`effective_start`, `observations`, `formula_version`, readiness·blocker와
`top_rank_correlation`만 읽는다. grade와 AB 판정은 쓰지 않는다. Phase B run에는 `plots/`
디렉터리가 없으므로 A의 기존 PNG를 재사용해 A/B를 비교하지 않는다.

### 5.3 결합 AB

- run: `20260828T165038-4e0ae8b0`
- `manifest.json`, `_SUCCESS.json`
- `combined_ab_primary_hypotheses.parquet`
- `primary_feature_rank_correlation.parquet`
- `phase_a_card_overlay.parquet`

AB parquet에는 A primary 12 family와 B 18 family, 모두 30 family만 있다. 최종 Phase B
family 요약은 이 파일에서 cell 수·discovery 수·screen-pass 수·grade 분포·IC 범위·min q를
다시 계산한다.

### 5.4 모델 validation

- T1 walk-forward: `docs/target/01_20_access_return_rank/grade_a_acceptance_gate_results.json`.
  2026-08-24 재실행한 Rank IC·decile 결과다.
- T1 k=100: `docs/target/01_20_access_return_rank/topk_cost_check.json`.
  2026-08-12 조건 확인 결과다.
- T1 과거 holdout: `grade_a_acceptance_gate_holdout.json`; **소표본·h60 불가 기록으로만 표시**
- T2: `docs/target/01_20_access_return_rank/phase_b_acceptance_gate_results.json`

T1 앞의 두 JSON은 run id·config·snapshot lineage가 없고 서로 같은 모델 실행 결과도 아니다.
SHA-256과 `07_phase1_acceptance_gate.md`·`00_status.md` 참조로 고정한다. 두 결과를 한 series로
합치거나 차이를 직접 비교하지 않는다. T1 h20 비채택은 사전 조건인
`k=100 cost-adjusted return Δ(h20) < 0`을 `topk_cost_check.json`에서 계산해 표시한다.
2026-08-24 decile Δ도 별도 보조 근거로 보여 준다.

### 5.5 입력 고정 검사

생성을 시작하기 전에 다음을 모두 검사한다.

1. 지정한 AB run 경로에 `manifest.json`과 `_SUCCESS.json`이 있다.
2. A/B manifest의 snapshot·source·config hash가 요청값과 같다. AB는 경로의
   snapshot·source를 고정하고 manifest에 같은 key가 있을 때도 대조한다. 현재 AB manifest에는
   snapshot·source key가 없다.
3. AB `_SUCCESS.json`의 run id가 `--ab-run-id`와 같고 content hash가 있다.
4. A/B run은 AB manifest의 `phase_a_run_id`·`phase_b_run_id`로만 찾는다.
5. A/B `_SUCCESS.json`의 run id·content hash가 AB manifest의 A/B id·hash와 같다.
6. family 정본이 A card 17개 ∪ B summary 18개 = 35개다.
7. A는 §5.1 필터로 primary 75개·exploratory 28개, B는 78개, AB는 153개다.
8. AB discovery는 87개, Phase B `screen_pass`는 40개, status는 valid 147·insufficient 6이다.
9. T2 JSON의 `ab_run_dir`, `ab_run_id`, `ab_content_hash`, config hash가 지정한 AB run·
   `_SUCCESS.json`과 같다.
10. T2 후보 family·feature가 각각 14개다.

하나라도 다르면 보고서를 만들지 않고 어떤 계약이 어긋났는지 보여 준다.

## 6. 숫자를 고르는 규칙

### 6.1 임의의 최고값을 대표 성능으로 쓰지 않는다

성과가 가장 좋아 보이는 horizon을 사후에 골라 family 순위를 만들지 않는다.

- Phase A headline은 발행된 `family_cards.json`의 `broad_ic`, `available_ic`,
  `candidate_horizon_band`, `evidence_grade`, `screen_pass`를 쓴다.
- Phase B는 family마다 하나의 최고 IC를 뽑지 않는다. 사전등록된 cell을 모두 보여 주고,
  family 요약은 AB parquet에서 다시 계산한 cell 수, discovery 수, `screen_pass` 수, grade 분포,
  IC 범위와 min q를 적는다. pre-AB `family_summary.parquet`의 판정 placeholder는 쓰지 않는다.
- T2 모델 차트의 후보 목록은 `phase_b_acceptance_gate_results.json`에 고정된 14개만 쓴다.
- 원래 부호와 방향 정렬 IC를 함께 보존한다. 차트 축이나 tooltip에 어느 값을 썼는지 적는다.

AB에 없는 공매도 4 family와 reference 1 family의 primary 성능 칸은
`primary cell 없음 (exploratory_short_regime)` 또는 `primary cell 없음 (reference_only)`로
표시한다. 공매도 28개 exploratory cell의 IC는 펼친 상세 영역에서 별도 진단값으로 보여 주되
q·discovery·screen-pass는 `—`로 둔다.

### 6.2 단계별 용어를 섞지 않는다

다음 상태는 다른 뜻이다.

| 상태 | 뜻 |
|---|---|
| `discovery` | BH 기준을 통과한 cell |
| `screen_pass` | 방향·기간·tradable·delay·강건성 게이트까지 통과한 cell |
| Grade A/B | screening 근거 등급. 모델 채택을 뜻하지 않음 |
| `validation improved` | 후보 묶음을 모델에 넣었을 때 valid 구간 개선 |
| `not adopted h20` | T1 k=100 비용 반영 수익 Δ(h20)가 0보다 작아 조건을 통과하지 못한 판정 |
| `holdout pending` | 최종 h60 평가 전 |
| `ready` | feature·label dependency가 준비된 상태. IC가 실제 계산됐다는 뜻은 아님 |
| `insufficient` | formation row가 없어 IC를 계산하지 못한 상태 |

Phase A와 Phase B의 C 등급 정의도 다르므로 범례를 따로 둔다. Phase A C는 탐색·보조·보류일
수 있고, Phase B C는 강건성 또는 availability 방향 게이트 실패다. Phase B 78개 cell은
readiness를 모두 통과했지만 `fin_sue` 6개는 `no_formation_rows`라 평가가 안 됐다. 따라서
evaluable cell은 72개다.

### 6.3 결합 결과를 하나의 funnel로 오해하지 않는다

AB discovery 87개는 A+B 전체다. `screen_pass` 40개와 A23·B17·C35·D3은 Phase B cell
판정이다. `153 → 87 → 40`을 하나의 직선 funnel로 그리면 서로 다른 모집단을 섞게 된다.
보고서에서는 A와 B를 두 lane으로 나누고 AB는 결합 BH 단계로 따로 표시한다.

## 7. HTML 구성

### 7.1 상단 고정 요약

- 제목: `피쳐 성능 평가 — 2026-08-28 validation 기준`
- 상태 badge: `FINAL HOLDOUT PENDING`
- snapshot, config hash 앞 12자리, A/B/AB run id
- 전체 35 family. AB 대상은 A primary 12 + B 18 family, 153 hypotheses, discovery 87,
  Phase B screen-pass 40이며 나머지 A 5 family는 exploratory/reference다.
- T1: `h20 비채택`
- T2: `valid h5/h20/h60 개선 · 14-feature bundle · 최종 채택 전`

### 7.2 핵심 차트

| # | 차트 | 표현 | 확인할 내용 |
|---|---|---|---|
| 1 | **A/B 두 lane 평가 흐름** | 단계형 flow | A cell 75 → BH 57 → discovery 32 뒤 family screen-pass 6·A6/C4/D6/R1. B cell 78 → discovery 55 → screen-pass 40 → A23/B17. 노드마다 cell/family 단위를 표시 |
| 2 | **family × horizon IC heatmap** | diverging heatmap | primary의 부호·onset·decay·sign flip. 공매도 exploratory 28 cell은 별도 band, `fin_sue` insufficient 6 cell은 `—` marker |
| 3 | **표본 규모–IC 산점도** | scatter | A/B 공통 x축은 cell `n_obs_mean`. B `coverage_ratio`는 tooltip·표에만 표시하고 A coverage는 `—` |
| 4 | **등급·게이트 분포** | stacked bar | Phase A family A6/C4/D6/R1과 Phase B cell A/B/C/D 및 주요 실패 사유. pre-AB `NE`는 본 범례에서 제외 |
| 5 | **A×B primary feature 상관 heatmap** | 12×17 correlation heatmap | A/B 교차 중복만 확인. `|rho| >= 0.7` 쌍을 표시하며 같은 phase 내부 중복은 판단하지 않음 |
| 6 | **T1/T2 baseline 대 candidate** | horizon별 paired bar + 별도 k=100 표 | paired bar는 8월 24일 T1과 8월 28일 T2의 Rank IC·decile 비용 지표로 축을 통일. 8월 12일 T1 k=100은 별도 표 |
| 7 | **family 상세 horizon curve** | 선택형 line/dot | cumulative·bucket IC, q, 표본 수, grade. exploratory는 별도 style로 표시 |

색만으로 상태를 구분하지 않는다. marker 모양, 텍스트 label과 표를 함께 둔다. IC heatmap은 0을
중심으로 같은 양·음 범위를 쓴다. `status=insufficient`는 색을 0으로 채우지 않고 빈 칸과 별도
marker로 표시한다.

### 7.3 전체 결과표

35개 family를 빠짐없이 싣는다. 사용자가 다음 기준으로 정렬·필터할 수 있게 한다.

- phase, domain, source
- family, primary feature
- expected sign과 observed sign
- candidate horizon band
- primary IC 범위, q, discovery 수, screen-pass 수
- `n_obs_mean`, n_dates, n_obs. B는 coverage ratio도 표시하고 A coverage ratio는 `—`
- evidence grade, grade cap, failed gates
- T1/T2 모델 입력 여부
- 현재 판정: 채택·비채택·보류·validation 통과·holdout 대기

행을 펼치면 사전등록 cell 전체를 보여 준다. AB에 없는 5 family는 현재 판정을
`primary cell 없음 (exploratory_short_regime)` 또는 `primary cell 없음 (reference_only)`로
적는다. 공매도 exploratory IC는 별도 열에 표시한다. `fin_sue` insufficient 6 cell과 계산하지
않은 값은 `—`로 표시하며 0으로 바꾸지 않는다.

### 7.4 설명과 한계

보고서 끝에 다음을 짧게 적는다.

- Rank IC, ICIR, BH q, `screen_pass`, 거래비용 spread의 뜻
- PIT와 holdout 경계
- 생존편향·final-vintage·source quality 경고
- T2는 14개를 함께 넣은 결과라 개별 모델 기여도로 해석할 수 없다는 점
- 상관 heatmap은 A×B 교차만 다루며 A×A·B×B 중복은 판단하지 않는다는 점
- T1 8월 24일 decile과 8월 12일 k=100은 lineage가 다른 별도 실행이라는 점
- T1 과거 holdout은 평가일 16일·h20 리밸런싱 1회라 결론에 쓰지 않았다는 점
- 최종 h60 holdout이 남았다는 점

## 8. 구현 방식

### 8.1 생성기

`research/analysis/feature_performance_report.py`가 다음 순서로 동작한다.

1. `--ab-run-id`로 AB run 경로를 찾고 A/B run은 AB manifest의 id에서 도출한다.
2. A/B `_SUCCESS.json`의 content hash가 AB manifest가 기록한 A/B hash와 같은지 검증한다.
3. Parquet·JSON을 공통 record로 정규화한다.
4. KPI와 차트용 집계를 계산한다.
5. Plotly figure를 만든다.
6. CSS, 표 필터용 짧은 JavaScript와 Plotly JS를 HTML 안에 넣는다. Plotly JS는
   `plotly.offline.get_plotlyjs()`로 한 번만 넣고 각 figure는
   `to_html(include_plotlyjs=False, full_html=False)`로 만든다.
7. `index.html`과 입력 hash를 담은 `report_manifest.json`을 원자적으로 발행한다.

Plotly는 `analysis` extra에 이미 있다. CDN을 쓰지 않고 JS를 inline한다. 표 필터와 정렬은
외부 DataTables 대신 작은 vanilla JavaScript로 만든다. 생성된 HTML은 네트워크 요청 없이
`file://`에서 동작해야 한다. print 결과가 canvas/WebGL 상태에 매달리지 않도록 `scattergl`·
`heatmapgl` 같은 WebGL trace는 쓰지 않는다.

### 8.2 데이터 노출 제한

- HTML에 ticker·종목명·계좌·API key·DB URI를 넣지 않는다.
- 파일 경로는 repo 상대경로로 적고 사용자 홈 경로는 넣지 않는다.
- KIS·KRX raw payload를 embed하지 않는다.
- 외부 URL 요청, analytics, font CDN을 넣지 않는다.
- 보고서 상단에 `비공개 개인 연구용`을 표시한다.
- 원본 manifest·run spec을 통째로 embed하지 않는다. 필요한 key만 골라 넣고 절대경로는 repo
  상대경로로 바꾼다.

### 8.3 재현성

`report_manifest.json`에 다음을 기록한다.

- report schema version
- snapshot, source, config hash
- A/B/AB run id와 content hash. run id·hash는 각 `_SUCCESS.json`에서 읽는다.
- A/B run의 `git_commit`·`git_dirty`·command는 `run_spec.json`·`phase_b_run_spec.json`에서
  필요한 값만 읽는다.
- T1/T2 JSON SHA-256
- 보고서 생성 코드의 Git SHA와 dirty 여부
- 입력별 row count
- 보고서 KPI
- holdout 상태

HTML footer에도 같은 lineage를 사람이 읽을 수 있는 형태로 표시한다.

## 9. 검증 계획

### 9.1 단위 테스트

`tests/unit/test_feature_performance_report.py`는 작은 in-memory record와 `tmp_path` JSON
fixture만 쓴다. canonical Parquet은 integration test에서만 읽는다. unit test는
`research/output/`이나 `docs/target/`이 없어도 항상 돌아야 한다.

- config·run 불일치 시 fail-fast
- A/B/AB row 수와 중복 `hypothesis_id` 검사
- null·NaN은 `—`로 표시하고 0과 구분
- 기대 부호 정렬 IC 계산
- Phase A/B 등급 의미를 따로 매핑
- A card ∪ B summary를 family 목록 정본으로 쓰고 표에 정확히 한 번씩 넣는지 확인
- 14개 T2 후보 집합이 JSON과 같은지 확인
- `index.html`·`report_manifest.json`의 HTML escape, 홈 경로와 비밀정보 패턴 차단
- 외부 URL을 가리키는 `script src`, `link href`, image URL이 없는지 확인. inline script는 허용

### 9.2 기준값 회귀 검사

`tests/integration/test_feature_performance_report_regression.py`가 로컬 정본 run과
`docs/target` JSON을 읽는다. 필요한 산출물이 없으면 self-skip한다. unit test에 로컬 run을
요구하지 않는다.

생성 결과가 다음 값을 보여야 한다.

| 항목 | 기준값 |
|---|---:|
| Phase A valid | 75 / 75 |
| Phase A family grade | A6 / C4 / D6 / R1 |
| Phase A exploratory | 4 family / 28 cell |
| Phase B ready | 78 / 78 |
| AB hypotheses | 153 |
| AB status | valid 147 / insufficient 6 |
| AB discoveries | 87 |
| Phase A discovery 변화 | 0 |
| Phase B screen-pass cell | 40 |
| Phase B cell grade | A23 / B17 / C35 / D3 |
| 결합 permutation | 0.0099 |
| A×B absolute rho >= 0.7 | 2쌍 |
| T1 decile 비용 반영 spread Δ(h20) | -0.0025 |
| T1 k=100 비용 반영 수익 Δ(h20) | -0.0045 |
| T2 Rank IC Δ | h5 +0.0031 / h20 +0.0011 / h60 +0.0003 |
| T2 비용 반영 spread Δ | h5 +0.0017 / h20 +0.0030 / h60 +0.0080 |

기준값은 HTML에 하드코딩하지 않는다. 입력에서 계산한 값이 위 회귀값과 같은지만 테스트한다.

### 9.3 시각 확인

로컬 브라우저에서 다음을 확인한다.

- 1440px desktop과 390px mobile에서 표·차트가 잘리지 않는다.
- hover, legend, family filter와 표 정렬이 동작한다.
- 색약 모드에서도 부호와 등급을 구분할 수 있다.
- 긴 family 이름과 failed-gate 목록이 겹치지 않는다.
- print CSS에서 핵심 요약과 validation 표가 읽힌다.
- 브라우저 개발자 도구의 network 요청이 0건이다.
- `index.html`이 10MB 이하다.

## 10. 작업 순서

1. **입력 계약 고정** — 정본 경로, schema, hash와 기준 KPI 검증기를 만든다.
2. **공통 record 작성** — A family card, B/AB cell, coverage, correlation, T1/T2 JSON을 합친다.
3. **표부터 완성** — 35 family와 모든 cell을 필터·정렬 가능한 표로 검증한다.
4. **차트 1~5 추가** — screening·coverage·중복 정보를 같은 색·범례 체계로 묶는다.
5. **T1/T2 validation 추가** — screening과 모델 성능을 화면에서 분리한다.
6. **한계·lineage·보안 문구 추가** — 최종 채택으로 오해할 표현을 막는다.
7. **자동 테스트와 기준값 대조** — fixture unit test와 로컬 산출물 integration 회귀 테스트를
   나눠 숫자·입력·HTML self-contained 조건을 검사한다.
8. **브라우저 시각 확인** — desktop·mobile·print를 확인하고 고친다.
9. **v1 발행** — `20260828_validation/index.html`을 보존한다.

## 11. 완료 조건

- 명령 한 번으로 같은 입력에서 HTML과 manifest를 다시 만들 수 있다.
- 보고서가 35 family와 153개 가설을 빠짐없이 다룬다.
- A/B/AB 모집단과 `discovery`·`screen_pass` 의미를 섞지 않는다.
- 모든 차트 숫자를 원본 Parquet·JSON까지 추적할 수 있다.
- T1 h20 비채택과 T2 bundle validation 개선을 구분한다.
- T2 14개 각각의 모델 기여도가 검증된 것처럼 쓰지 않는다.
- `FINAL HOLDOUT PENDING`과 2026년 10~11월 h60 평가 일정을 눈에 띄게 표시한다.
- HTML이 외부 네트워크 없이 열리고 종목별·비밀정보를 담지 않는다.
- Plotly JS를 한 번만 포함하고 `index.html` 크기가 10MB 이하다.
- 단위 테스트, 기준값 회귀 검사와 브라우저 시각 확인을 모두 통과한다.

## 12. h60 holdout 이후 갱신

2026년 10~11월에는 v1을 덮어쓰지 않는다. 새 holdout 결과를 입력으로 받아
`reports/feature_performance/<평가일>_h60_holdout/index.html`을 만든다. 그 버전에서만 최종
채택 여부를 표시한다. v1은 selection과 validation이 끝났지만 holdout을 열기 전의 기록으로
로컬에 보존한다. Git에 남길 필요가 생기면 generated manifest를 자동 커밋하지 않고, 사람이
검토해 홈 경로·비밀정보를 제거한 sanitized 사본만 별도 승인 뒤 추가한다.

## 13. 구현 결과

2026-08-28에 v1 구현과 로컬 발행을 마쳤다.

- 생성기와 test를 계획한 경로에 추가했다.
- canonical A/B/AB와 T1/T2 입력으로 `20260828_validation/index.html`과
  `report_manifest.json`을 만들었다.
- 보고서 전용 test 14개가 통과했다. integration test는 canonical 산출물이 없으면 skip한다.
- canonical 회귀값은 family 35개, AB 153개, discovery 87개, B screen-pass 40개,
  correlation 204행을 포함해 계획값과 맞았다.
- HTML 크기는 5,061,774바이트다. Plotly JS는 한 번만 들어가며 WebGL trace는 없다.
- 외부 asset tag, 사용자 홈 절대경로, 비밀정보 패턴은 최종 HTML·manifest에서 나오지 않았다.
- Chrome에서 `file://`로 열어 요약·차트·전체 표를 확인했다. 검색 필터는 `fin_sue` 한 행만
  남겼고, insufficient 6개 cell 상세도 정상적으로 펼쳐졌다.
- desktop 화면과 600px 이하 mobile CSS가 적용된 좁은 창에서 레이아웃을 확인했다. 현재 QA
  환경에서는 정확한 1440px·390px device emulation 대신 일반 창과 554px 좁은 창을 썼다.
- print preview는 11쪽으로 만들어졌고 첫 페이지 요약과 validation 구역을 읽을 수 있었다.

브라우저 Network 패널을 직접 계측하지는 못했다. 대신 생성기와 test가 외부 URL을 가리키는
`script src`, `link href`, `img src`를 막고, 최종 산출물에서도 해당 tag가 0개임을 확인했다.
