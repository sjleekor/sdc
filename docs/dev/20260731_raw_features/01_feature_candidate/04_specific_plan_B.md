# 04-B. Horizon Scan Phase B — Financial·Event 스캔 상세 실행 계획

- 작성일: 2026-08-02 (rev.2)
- 전체 계획: [03_horizon_predictive_power_plan.md](03_horizon_predictive_power_plan.md)
- 피쳐 정의 기준: [02_feature_candidate.md](02_feature_candidate.md)
- 선행 준비 계획: [04_specific_plan_A0.md](04_specific_plan_A0.md)
- 선행 스캔 계획: [04_specific_plan_A.md](04_specific_plan_A.md)

## 0. 요약

Phase B의 목적은 DART 재무·주식수·주주환원 원천을 실제 접수일 기준 point-in-time(PIT)
정보로 재구성하고, financial/event 8개 family의 유효 horizon을 검증하는 것이다.

Phase A의 price·flow는 매 거래일 변하는 dense feature였다. Phase B는 다음 두 경로를
명시적으로 분리한다.

```text
경로 B-daily
  DART 원시 공시와 정정공시
  → 실제 접수일 기반 metric vintage
  → standalone quarter / TTM
  → 일별 PIT broadcast + fin_age
  → 7개 continuous family의 cumulative·bucket IC

경로 B-event
  최초 실적 공시의 SUE event
  → 접수일 다음 KRX session을 event formation으로 고정
  → 정정 전 60-session endpoint가 모두 있는 고정표본
  → 다음 정기공시로 자르지 않은 6개 event-time bucket의 cohort IC

두 경로
  → Phase B 최대 38개 candidate 중 outcome-blind readiness를 통과한 셀 동결
  → Phase A 75개와 결합한 동결 모집단에 global BH
  → 강건성·결론 카드·Phase C/acceptance gate 인계
```

Phase B의 가장 중요한 안전 원칙은 다음과 같다.

1. `rcept_no` 앞 8자리의 실제 접수일을 사용한다. 기존 `period_end+45/+90일`은
   `rcept_no` 결측·파싱 실패 시에만 fallback이며 sj2 현재 원천의 예상 사용 건수는 0이다.
2. 현재 raw에는 filing key당 API 호출 당시의 단일 captured receipt만 있다. 이 값은 그
   `rcept_no`와 같은 filing에서 왔음을 XBRL context/value로 검증한 뒤 그 접수일 다음
   session부터만 사용한다. B-1에서 접수목록과 receipt-targeted XBRL을 백필해 확보한
   원공시·정정본만 새 vintage로 추가하며, raw에 없는 vintage를 추정하지 않는다.
3. 기존 `stock_metric_fact`, `feat_fin_pit`, `feat_event`의 합성 lag 의미를 조용히 바꾸지
   않는다. Phase B용 additive mart를 만들고 official scan은 새 mart 이름을 명시한다.
4. 재무비율을 finalized period 기준으로 미리 계산하지 않는다. standalone quarter와 TTM도
   해당 계산에 필요한 모든 vintage가 당시 공개된 뒤에만 생성한다.
5. SUE는 일별 broadcast 행을 반복 계상하지 않고 `(ticker, original filing event)` 1행을
   관측 단위로 사용한다.
6. 최대 38개 B candidate의 source/formula readiness는 수익률·IC·p-value를 보기 전에
   동결한다. 그때 blocked인 셀은 `blocked_exploratory`로 분리하고, 준비된 B 셀과 Phase A
   75개를 합친 모집단에 최종 global BH를 한 번 적용한다. 동결 뒤 실행 중 실패한 셀만
   `p_for_bh=1.0`으로 모집단에 남긴다.

Phase B에서 하지 않는 일은 다음과 같다.

- holdout(2025-08-01 이후 label 수익률) 개봉
- 다변량 모델, 증분성, 거래비용, turnover 평가
- `fin_announcement_abret_3d`처럼 접수시각이 필요한 장중/장후 이벤트 피쳐
- 개인 수급과 SUE의 interaction, macro/regime interaction
- full Piotroski F-score, 미매핑 운전자본 계정 확장
- `ev_net_payout_yield`처럼 issuance까지 합친 후속 composite
- Phase A/B 결과를 본 뒤 primary feature·horizon·source fallback을 교체하는 행위

## 1. 진입 조건과 종료 조건

### 1.1 진입 조건

Phase B official run은 다음 조건을 모두 만족해야 시작한다.

1. Phase A0의 official manifest가 존재하고 `smoke_only=false`다.
2. A0에서 선택한 `snapshot_date`, `source=sj2_remote`, universe/label/quality mart를 그대로
   사용한다. Phase B가 더 최신인 별도 snapshot을 임의로 결합하지 않는다.
3. raw manifest에 최소 다음 기존 테이블이 모두 있다.
   - `daily_ohlcv`
   - `dart_financial_statement_raw`
   - `dart_share_count_raw`
   - `dart_shareholder_return_raw`
   - `dart_xbrl_fact_raw`
   - `dart_xbrl_document`
   - `dart_corp_master`
   - `stock_master`, `stock_master_snapshot`, `stock_master_snapshot_items`
   현재 lake의 `RAW_TABLES`에는 위 재무/XBRL source가 이미 포함돼 있다. Phase B가 새로
   수집하는 원천은 원공시·정정 관계용 `dart_filing_receipt_raw`와 발행·감소 사유용
   `dart_capital_change_raw` 두 개다. SUE는 전자와 receipt-targeted original XBRL,
   순발행은 후자가 준비되지 않으면 각각 `blocked_exploratory`로 동결한다.
4. A0의 `dim_stock_pit_daily`, `dim_price_quality_daily`, broad/tradable universe,
   `label_scan`과 common formation/survivor flag가 같은 manifest에 존재한다.
5. config가 Phase B 8개 family, 최대 38개 candidate, 재무 PIT 정책, event-time 정책과
   readiness 판정 규칙을 결과 확인 전에 고정한다. source/feature coverage만 본 두 번째
   freeze가 결합 BH의 실제 모집단 크기를 확정한다.
6. official run에서 `holdout_start=2025-08-01`을 변경할 수 없다.
7. Phase A raw p-value를 결합 BH에 사용할 경우 Phase A run이 같은 A0 manifest와 config
   lineage를 사용하고 통계 content hash 검증을 통과한다.

Phase B core scan 자체는 Phase A 실행 완료 전에도 계산할 수 있다. 다만 Phase A artifact가
없으면 `q_fdr_global_ab`, `screen_pass`, acceptance 인계 목록은 만들지 않고
`status=awaiting_phase_a_for_combined_bh`로 남긴다.

이 specific plan은 A0 문서의 잠정적 `Phase B require_derived=true`를 exact dependency로
구체화한다. Phase B official feature는 기존 `stock_metric_fact`나
`common_feature_daily_fact`를 읽지 않고 같은 A0 raw snapshot에서 새 vintage mart를 만든다.
따라서 pre-existing derived snapshot과의 날짜 교집합이나 `common_feature_daily_fact`를
요구하지 않는다. 필요한 것은 A0 artifact lineage와 위 raw table의 완전한 manifest다.
구현 시 A0의 resolver 설명도 이 결정과 동기화한다.

### 1.2 2026-08-02 source-of-truth preflight 관측

sj2 production PostgreSQL을 읽기 전용으로 확인한 초기 범위는 다음과 같다. 이는 고정
row-count gate가 아니라 구현량과 최초 readiness 기대값을 잡기 위한 관측이다. official
run은 선택된 parquet snapshot manifest의 수치를 다시 기록한다.

| Raw table | 행 | ticker | 사업연도 | 잘못된 14자리 `rcept_no` |
|---|---:|---:|---|---:|
| `dart_financial_statement_raw` | 16,887,392 | 2,608 | 2015–2026 | 0 |
| `dart_share_count_raw` | 347,223 | 2,653 | 2015–2026 | 0 |
| `dart_shareholder_return_raw` | 8,647,722 | 2,653 | 2015–2026 | 0 |

현재 `dart_financial_statement_raw`의 83,743개
`(corp_code,bsns_year,reprt_code)` key와 `dart_xbrl_fact_raw`의 83,649개 key는 모두 distinct
`rcept_no`가 정확히 1개다. 수집기가 filing key 단위로 skip-if-present하므로 이는 우연한
표본 특성이 아니라 **API 호출 시점의 단일 captured/latest filing** 구조다. 따라서 기존 raw
안에서 서로 다른 vintage를 보존한다는 주장을 하지 않는다. B-1의 접수목록으로 실제
원공시·정정 비율과 targeted backfill 건수를 확정하고, 원공시 XBRL을 추가로 받은 filing에서만
여러 vintage가 생긴다. XBRL은 financial filing key의 83,649/83,743(99.9%)를 이미 덮으므로
기존 재무 source는 새 table이 아니라 pairing/backfill 대상으로 취급한다.

접수 lag의 긴 꼬리는 일부 저장 행이 원공시가 아니라 후속 정정본임을 시사한다. 그렇더라도
저장된 값과 `rcept_no`가 같은 filing에서 온 짝이면 그 접수 다음 session부터 사용하는
continuous 경로에는 look-ahead가 없다. 이 명제를 가정하지 않고 B-0의 최우선 hard gate로
검증한다. official metric으로 사용되는 모든 행은 동일 `rcept_no`의 XBRL period/instant,
fs-basis, unit과 허용 오차 내 value가 맞아 `verified_same_receipt`여야 한다. mismatch가 하나라도
있으면 mapping 문제가 아닌 source-pairing 오류로 보고 Phase B official run 전체를 막는다.

원천 구조상 2015년은 주로 사업보고서이고 2016년부터 분기·반기 자료가 존재한다.
따라서 예상 가능한 가장 이른 유효 시점은 family마다 다르다.

- 단일 balance/size: 첫 실제 접수 이후, 사실상 2015년 중
- 4개 standalone quarter TTM: 빨라도 2017년 전후
- seasonal SUE의 과거 surprise 8개와 weighted-share coverage: 빨라도 2018~2019년 전후
- issuance/payout: source normalization과 한 해 lag/TTM 이후

readiness report는 이 예상과 실제 최초 유효일이 다르면 원인을 source/mapping/basis별로
분해한다. 위 숫자를 integration test의 고정 상수로 사용하지 않는다.

### 1.3 입력 불변 계약

Phase B는 A0 manifest의 다음 fingerprint를 상속한다.

```text
snapshot_date
source
raw_manifest_hash
a0_manifest_hash
config_schema_version
config_hash
dim/feature/label mart별 schema_hash와 content hash
quality/universe/label policy version
holdout_start
```

Phase B가 추가하는 fingerprint는 다음과 같다.

```text
metric_mapping_rules_hash
stock_metric_vintage_schema_hash
receipt_value_pairing_report_hash
phase_b_readiness_freeze_hash
quarterly_metric_formula_version
financial_feature_formula_version
event_feature_formula_version
filing_availability_policy_version
event_censoring_policy_version
phase_b_code_hash
python/duckdb/polars/numpy version
git_commit / git_dirty
command line
started_at / finished_at (Asia/Seoul)
```

`phase_b_code_hash`는 vintage builder, standalone/TTM builder, financial/event feature
builder, scan driver, 통계 함수, report renderer와 config의 안정적 hash다. mapping rule 또는
정정공시 처리 규칙이 다르면 동일 snapshot의 기존 Phase B artifact를 재사용하지 않는다.

### 1.4 Phase B 종료 조건

다음을 모두 충족해야 Phase B를 기술적으로 완료한 것으로 본다.

1. 8개 family와 최대 38개 candidate ID가 registry에 정확히 존재하고, outcome-blind
   readiness freeze에 `ready_primary`와 `blocked_exploratory`가 빠짐없이 기록된다.
2. `stock_metric_vintage_fact`가 현재 단일 captured receipt와 B-1에서 추가 백필한 vintage를
   실제 접수일별로 보존하고, receipt-value pairing 및 history completeness를 기록한다.
3. standalone quarter·TTM이 finalized future value가 아니라 PIT vintage로 계산된다.
4. 7개 continuous family의 cumulative/bucket 결과와 SUE 6개 event-time 결과가
   `ready_primary`에는 생성되고, 나머지 candidate에는 사전 정의된 blocker가 1행씩 존재한다.
5. Phase A의 검증된 75개 raw p-value와 B의 동결된 `M_B_ready`개를 합친
   `M_AB=75+M_B_ready`개 셀에 global BH가 한 번 적용된다. Phase A가 아직 없으면 이 항목만
   대기 상태로 남고 최종 publish는 하지 않는다.
6. broad/tradable, common-survivor/available과 financial 전용 segment가 독립 축으로
   산출된다.
7. continuous 셀의 gap-aware NW, 전 offset non-overlap, 단면 permutation과 장기 temporal
   placebo가 완료된다.
8. SUE event 셀의 고정표본 cohort inference, 정정 censoring, issuer와 filing-cycle 군집
   진단 및 다음 공시 overlap 진단이 완료된다.
9. 모든 family에 coverage/readiness, decay/event-time 요약, 경고, 다음 단계가 있는 결론
   카드가 생성된다.
10. 모든 수익률 관측의 종료일이 `holdout_start`보다 이르다.
11. 동일 input/config/code/seed 재실행의 논리 통계 content hash가 동일하다.
12. manifest와 `_SUCCESS.json`은 모든 artifact hash 검증 뒤 원자적으로 기록된다.

family가 무신호인 것은 파이프라인 실패가 아니다. 수익률을 보기 전 readiness freeze에서
source/formula가 막힌 candidate는 Phase A의 short family와 같은
`blocked_exploratory`이며 BH에서 제외한다. 반대로 freeze 뒤 계산 실패·insufficient가 된
`ready_primary`는 `p_for_bh=1.0`으로 유지한다. readiness를 label·IC·p-value에 따라 바꾸거나
최대 38개 candidate 중 누락된 ID를 조용히 빼는 것은 실패다.

## 2. 사전등록 검정 계약

### 2.1 Phase B family와 primary 셀

Phase B의 기계 판독 정본은 `horizon_scan_config.yaml`이다. continuous family의 bucket은
Phase A와 동일하게 `h_end`가 primary horizon에 포함될 때 primary가 된다. SUE는 일별
cumulative scan을 하지 않고 사전등록 event bucket만 사용한다.

| Family | Primary feature | 부호 | Primary cumulative h | Primary bucket/end | B 셀 |
|---|---|---:|---|---|---:|
| 규모 | `fin_log_mcap` | `-` | 60,120 | 60,120 | 4 |
| 가치 composite | `fin_value_z` | `+` | 60,120 | 60,120 | 4 |
| 수익성 | `fin_gross_profitability` | `+` | 20,40,60,120 | 20,40,60,120 | 8 |
| 자산성장 | `fin_asset_growth_yoy` | `-` | 60,120 | 60,120 | 4 |
| 발생액 | `fin_accruals_to_assets` | `-` | 60,120 | 60,120 | 4 |
| SUE/PEAD | `fin_sue` | `+` | — | event (0,3], (3,5], (5,10], (10,20], (20,40], (40,60] | 6 |
| 경제적 순발행 | `ev_net_share_issuance_yoy` | `-` | 60,120 | 60,120 | 4 |
| 주주환원 | `ev_payout_yield` | `+` | 60,120 | 60,120 | 4 |
| **최대 candidate** | 8 family |  |  |  | **38** |

continuous bucket은 A0 `label_scan`의 `(40,60]`, `(60,120]` 같은 고정 구간을 사용한다.
SUE event bucket의 숫자는 event formation close 이후의 KRX session endpoint다.

### 2.2 primary·secondary·diagnostic 역할

- `primary_candidate`: §2.1의 최대 38개 셀. 결과를 보기 전 readiness 판정 대상이다.
- `ready_primary`: outcome-blind source/formula/표본 가능성 점검을 통과해 동결된 결합 BH 대상.
- `blocked_exploratory`: readiness freeze 때 source/formula가 막힌 candidate. BH와 screen pass에서
  제외하되 blocker·coverage는 결과에 남긴다.
- `runtime_blocked_primary`: `ready_primary` 동결 뒤 실행 실패·insufficient가 된 셀.
  `p_for_bh=1.0`으로 동결 모집단에 남긴다.
- `secondary_feature`: 아래 component·대체 산식. 강건성 참고이며 BH와 screen pass 제외.
  - value: `fin_book_to_market`, `fin_earnings_yield`, `fin_cfo_yield`,
    `fin_sales_to_price`
  - profitability: `fin_operating_profitability`
  - SUE: 공식 weighted-share 정의가 불가능할 때의 `fin_sue_issued_share_proxy`
  - payout: `ev_dividend_yield`, `ev_buyback_yield`
- `exploratory_horizon`: primary grid 밖 continuous horizon/bucket.
- `segment_diagnostic`: market/size/liquidity/period/fin-age/fs-basis 결과.

component 중 결과가 가장 좋은 것을 사후에 `fin_value_z` 또는 primary profitability로
교체하지 않는다. SUE proxy가 공식 정의보다 좋아도 같은 run에서 승격하지 않는다.

### 2.3 Phase A+B global BH

`03`은 전체 family의 주 검정 집합에 global BH를 한 번 적용하도록 정했다. Phase A가 먼저
실행되므로 Phase A 문서의 75셀 q-value는 Phase A 내부 screening 진단으로 보존한다.
Phase B 완료 시 최종 acceptance 후보를 정할 q-value는 다음 모집단에서 다시 계산한다.

```text
Phase A official raw p-values:     M_A = 75
Phase B readiness-frozen primary:  M_B_ready <= 38
combined global BH:                M_AB = 75 + M_B_ready <= 113
```

`M_B_ready`는 B-0이 정의한 규칙으로 B-1~B-6의 source·mapping·coverage만 검사한 뒤,
feature-label join과 어떤 IC·return·p-value도 계산하기 전에
`phase_b_readiness_freeze.json`에 고정한다. 예를 들어 SUE original source나 순발행 사유
source가 없으면 해당 6개/4개 셀은 이 시점에 `blocked_exploratory`가 된다. freeze 이후
source를 보완하려면 config/run version을 올리고 readiness를 다시 동결해야 한다.

정확한 좌표는 다음과 같다.

```text
phase_set          = AB
universe           = broad
sample_kind        = common_survivor   # continuous
event_sample_kind  = constant_60_session_original_event  # SUE
segment_axis       = overall
segment            = all
feature_role       = primary
hypothesis_role    = primary
feature_variant    = family별 frozen official variant
```

규칙:

1. 모든 셀은 양측 p-value를 사용하고, BH 통과 뒤 expected sign을 별도로 확인한다.
2. freeze 뒤 `insufficient`, 실행 실패, 상수 feature가 된 `ready_primary`는 raw p-value를
   NULL로 두고 `p_for_bh=1.0`을 사용한다. 사전 동결된 `blocked_exploratory`는 애초의
   `M_B_ready`에 포함하지 않는다.
3. 정렬 tie는 `hypothesis_id`, q-value는 역방향 누적 최소값으로 단조화한다.
4. `q_fdr_global_ab < 0.10`만 통과다.
5. Phase A artifact의 raw p-value를 재계산 없이 읽을 때는 75개 ID, code/config/input hash와
   raw p-value content hash를 검증한다.
6. 결합 BH 뒤 Phase A의 기존 q-value를 덮어쓰지 않는다. `q_fdr_phase_a`와
   `q_fdr_global_ab`를 별도 컬럼으로 보존한다.
7. acceptance 인계에는 `q_fdr_global_ab`만 사용한다.

`q_fdr_phase_b`를 `M_B_ready`개 셀의 domain diagnostic으로 계산할 수는 있지만 발견·screen pass·
evidence grade에는 사용하지 않는다. Phase B-only q-value가 결합 q-value보다 유리하다는
이유로 후보를 살리지 않는다.

Phase A 결과를 본 뒤 B family/horizon을 바꾼 config는 confirmatory 결합 검정으로 인정하지
않는다. 변경이 필요하면 config version과 A0 manifest를 올리고 A/B를 모두 새 run으로
재등록한다.

### 2.4 Phase B config 확장

아래 값은 A0의 공통 config와 함께 hash에 포함한다. 현재 schema v2에 필드가 없으면 official
실행 전 schema version을 올리고 A0 manifest부터 다시 만든다.

```yaml
phase_b:
  primary_candidate_count_max: 38
  phase_a_primary_count: 75
  primary_hypothesis_count: derived_from_readiness_freeze
  combined_ab_hypothesis_count: 75_plus_phase_b_ready
  readiness_freeze_before_label_join: true
  preflight_blocked_role: blocked_exploratory
  post_freeze_blocked_p_for_bh: 1.0
  official_availability: next_krx_session_after_rcept_date
  extra_delay_variant_sessions: 1
  rcept_date_parse: first_8_digits_yyyymmdd
  same_day_revision_policy: latest_rcept_no_effective_next_session
  later_revision_policy: new_vintage_effective_next_session
  captured_vintage_policy: never_impute_missing_historical_receipt
  original_filing_source: dart_filing_receipt_raw_plus_original_xbrl
  receipt_value_pairing_required: verified_same_receipt
  receipt_value_pairing_error_tolerance: 0
  receipt_enumeration_coverage_required: 1.0
  targeted_xbrl_terminal_status_required: 1.0
  sue_without_original_filing_source: blocked_exploratory
  issuance_action_source: dart_capital_change_raw_irdsSttus
  issuance_without_action_source: blocked_exploratory
  issuance_known_reason_row_ratio_min: 0.95
  issuance_reconciled_quantity_ratio_min: 0.95
  missing_rcept_fallback_days: {annual: 90, quarterly: 45}
  period_end_source_priority:
    [xbrl_period_or_instant, matched_stlm_dt, reprt_code_calendar_fallback]
  fs_basis_priority: [CFS, OFS]
  require_same_fs_basis_for_ttm: true
  ttm_required_quarters: 4
  sue_scale_surprise_count: 8
  sue_min_prior_surprise_events: 8
  sue_comparative_policy:
    interim: current_filing_frmtrm_q_amount
    annual_q4: reconstructed_prior_annual_minus_q3
  sue_as_was_comparative_variant: secondary
  value_component_min_count: 2
  cross_section_winsor_quantiles: [0.01, 0.99]
  value_z_scope: date_market
  negative_equity_book_to_market: null
  signed_earnings_and_cfo_yield: true
  event_primary_cohort_scope: pooled_market_neutral_ranks
  min_events_per_cohort_total: 30
  min_events_per_market_contribution: 10
  min_events_per_cohort_market_diagnostic: 30
  min_event_cohorts: 8
  min_independent_filing_windows: 12
  grade_a_min_independent_filing_windows: 20
  event_nw_gap_policy: calendar_session_distance
  event_revision_censoring: true
  event_primary_sample: constant_60_session_endpoint
  event_primary_next_filing_censoring: false
  event_secondary_next_filing_censoring: true
  event_report_mix_required: true
  event_issuer_bootstrap_repeats: 999
  event_filing_cycle_bootstrap_repeats: 999
  event_issuer_bootstrap_seed: 20260802
  event_filing_cycle_bootstrap_seed: 20260803
  event_cluster_confirm_p_max: 0.10
  industry_pit_policy: unavailable_no_current_backcast
  industry_sensitive_grade_cap: B
  period_sets:
    common:
      - {id: 2014_2016, start: 2014-06-01, end: 2016-12-31}
      - {id: 2017_2019, start: 2017-01-01, end: 2019-12-31}
      - {id: 2020_2021, start: 2020-01-01, end: 2021-12-31}
      - {id: 2022_2023_10, start: 2022-01-01, end: 2023-10-31}
      - {id: 2023_11_common_end, start: 2023-11-01, end: common_formation_end}
  period_sign_gate:
    valid_segments_gte_3: strict_majority_expected_sign
    valid_segments_eq_2: both_expected_sign_and_grade_cap_B
    valid_segments_lt_2: screen_fail
  event_buckets: [[0,3], [3,5], [5,10], [10,20], [20,40], [40,60]]
  nonoverlap_min_dates:
    default: 20
    cumulative_120: 12
    bucket_60_120: 12
  nonoverlap_valid_offset_ratio_min: 0.80
  nonoverlap_expected_sign_offset_ratio_min: 0.60
  evidence_grade_caps:
    current_industry_backcast: C
    missing_pit_industry_sensitive_family: B
    sue_proxy_only: C
    two_valid_period_segments_only: B
    long_nonoverlap_unassessable: B
    available_sign_flip: C
```

Phase A/A0의 다음 공통 값은 그대로 상속한다.

```yaml
sample.start: 2014-06-01
sample.holdout_start: 2025-08-01
sample.holdout_boundary: label_end_date
sample.common_formation_horizon: 120
sample.common_survivor_horizon: 120
stats.global_bh_q: 0.10
stats.min_names_per_date_market: 20
stats.min_names_for_spread: 50
stats.min_dates_per_cell: 60
stats.nonoverlap_min_dates: 20
# 위 값은 Phase A/default이며 Phase B h=120 계열은 phase_b.nonoverlap_min_dates가 override한다.
stats.nw_gap_policy: calendar_session_distance
placebo.cross_sectional_repeats: 100
placebo.temporal_long_cell_repeats: 100
```

## 3. 재무 PIT 데이터 모델

### 3.1 additive mart와 grain

기존 `stock_metric_fact`는 `(ticker, metric_code, bsns_year, reprt_code)`에서 최종 후보
하나만 남기고 `rcept_no`를 독립 컬럼으로 보존하지 않는다. `feat_fin_pit`와 `feat_event`는
합성 lag를 사용한다. Phase B official 경로에서 이 둘을 직접 읽지 않는다.

다음 additive mart를 만든다.

| Mart | Grain | 역할 |
|---|---|---|
| `stock_metric_vintage_fact` | ticker × metric × statement period × fs basis × `rcept_no` | captured receipt를 availability-normalized row로 만들고, 백필 vintage와 lineage 보존 |
| `fin_quarterly_metric_vintage` | ticker × standalone quarter × metric × effective vintage | instant/cumulative를 standalone quarter와 TTM으로 변환 |
| `feat_fin_scan_daily` | trade_date × ticker × market | 5개 financial family + size/value, `fin_age` broadcast |
| `feat_event_scan_daily` | trade_date × ticker × market | issuance/payout의 PIT continuous feature |
| `fin_sue_event` | ticker × original filing event | SUE와 event censor/endpoints |

모든 mart는 parquet/DuckDB derived layer에만 쓴다. PostgreSQL raw나 기존 canonical-compatible
fact에 write-back하지 않는다.

예외적으로 SUE 원공시 source를 확보하기 위한 `dart_filing_receipt_raw`와 발행·감소 사유를
확보하기 위한 `dart_capital_change_raw`(`irdsSttus`)는 새로운 **raw ingestion table**이다.
둘 다 collector의 ports/adapters/service 경로로 수집하고 PostgreSQL raw에 idempotent
upsert하며 `ingestion_runs` audit와 partial-run 정책을 적용한다. receipt/action relation과
SUE·순발행 값은 다시 DuckDB/parquet derived layer에만 쓴다.

기존 `stock_metric_fact`, `feat_fin_pit`, `feat_event`는 모델 회귀 검증을 위해 그대로 둔다.
새 경로를 검증한 뒤 기존 consumer를 옮기는 작업은 별도 migration으로 취급한다.

### 3.2 실제 접수일과 availability

각 raw row에 다음을 부여한다.

```text
disclosed_date = strptime(left(rcept_no, 8), '%Y%m%d')::date
available_from = first KRX session strictly after disclosed_date
```

접수시각이 없으므로 접수 당일 사용은 금지한다. 금요일·휴일 접수도 다음 실제 KRX session을
사용한다. `rcept_no`가 비어 있거나 파싱 불가일 때만 `period_end+45/+90일` fallback을 쓰고
`availability_source=synthetic_fallback`을 남긴다. fallback row는 official primary 결과에서
별도 coverage를 보고하며 비율이 0이 아니면 evidence grade A를 금지한다.

동일 공시일에 여러 `rcept_no`가 있으면 시각 순서를 알 수 없으므로 다음 session에 알 수 있는
최종 `rcept_no`를 그 session의 값으로 사용한다. 더 늦은 날짜의 정정공시는 새 event를 만들지
않고 그 다음 session부터 continuous feature의 새 vintage가 된다.

### 3.3 period end와 비12월 결산

`reprt_code`를 3/6/9/12월 말로 고정하면 비12월 결산법인의 quarter와 YoY가 틀어진다.
statement period end의 source 우선순위를 다음처럼 고정한다.

1. 같은 `rcept_no`의 XBRL `instant_date` 또는 duration `period_end`
2. 같은 filing과 결합 가능한 `dart_share_count_raw`/`dart_shareholder_return_raw.stlm_dt`
3. 위 source가 없을 때만 `reprt_code` 달력 fallback

서로 다른 source의 period end가 충돌하면 `period_end_conflict=true`를 기록하고 source별
값을 readiness report에 남긴다. fallback period end로 만든 TTM/SUE는 별도 coverage이며,
충돌이 해소되지 않은 primary family는 grade A를 받을 수 없다.

### 3.4 metric candidate와 정정공시 보존

mapping rule의 우선순위는 기존 `metric_rules.py`를 출발점으로 쓰되 winner를 다음 단위에서
선택한다.

```text
(ticker, metric_code, statement_period_end, fs_basis, rcept_no)
```

현재 raw에는 filing key당 `rcept_no`가 하나뿐이므로 최초 build에서 이 grain 확장은 대부분
lineage/availability 정규화다. 서로 다른 `rcept_no`는 B-1의 targeted backfill 뒤에만 생긴다.
그 경우 receipt끼리 경쟁시켜 하나를 버리지 않고, 같은 filing receipt 안에서만 `priority`,
XBRL dimension rank, 안정적 source key로 winner를 정한다.

vintage fact에는 최소 다음 lineage를 보존한다.

```text
ticker, metric_code, period_type, statement_period_start/end
bsns_year, reprt_code, fs_basis
rcept_no, disclosed_date, available_from, availability_source
value_numeric, unit, currency
source_table, source_key, mapping_rule_code, mapping_priority
period_end_source, is_revision, original_rcept_no
receipt_value_pairing_status, pairing_xbrl_source_key, pairing_tolerance
```

`dart_corp_master.is_active=true` 필터는 사용하지 않는다. raw ticker와 PIT universe를 결합해
현재 active 상태를 과거에 소급하는 생존편향을 추가하지 않는다.

### 3.5 captured vintage completeness와 원공시 source

현재 financial/share/XBRL raw는 filing당 API 호출 당시 반환된 단일 `rcept_no`만 저장한다.
따라서 B-1 전에는 원공시·중간 정정본 history가 없다고 보는 것이 맞다. Phase B는 다음
상태를 구분한다.

```text
complete_original_and_revisions
original_confirmed_revisions_partial
captured_vintages_only
unlinked_receipt
```

continuous feature는 §1.2의 receipt-value pairing을 통과한 `captured_vintages_only` 값을
해당 receipt의 실제 접수 다음 session부터 사용하면 look-ahead가 없다. 시장이 원공시 때
알았던 값을 복원하지 못하는 구간은 `history_left_truncated`로 표시하고 coverage 경고를
남기지만, 이 사실만으로 grade A를 자동 금지하지 않는다. pairing mismatch는 grade cap이
아니라 official run hard failure다.

SUE/PEAD는 original announcement reaction이 목적이므로 `captured_vintages_only`를 허용하지
않는다. 다음 source가 필요하다.

1. OpenDART 공시 접수목록을 보존한 `dart_filing_receipt_raw`
2. report name/type, receipt date, correction marker와 원공시–정정 관계
3. original `rcept_no`에 해당하는 XBRL document/fact 또는 동등한 당시 재무값

원천 계약은 OpenDART의 [공시검색 API](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019001)와
[재무제표 원본파일 API](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019019)를
기준으로 한다. 전자는 기간·법인별 receipt list, 후자는 지정 `rcept_no`의 XBRL 원본을 제공한다.

receipt title 문자열만으로 추정한 연결은 diagnostic으로만 사용한다. official original
relation은 source metadata로 확인되거나 사람이 검토한 mapping rule version에 포함돼야 한다.
B-1은 접수목록에서 실제 정정 filing 수와 original XBRL backfill target 수를 먼저 확정한다.
모든 target은 receipt-targeted 요청의 success/no-data terminal status를 가져야 하며, original
값이 없는 event는 최신 정정값으로 대체하지 않는다. source enumeration·weighted-share·event
cohort readiness가 사전등록 기준을 못 채우면 SUE 6셀 전체를 결과 확인 전에
`blocked_exploratory`로 동결한다.

### 3.6 CFS/OFS와 단위

동일 filing에서 CFS가 있으면 CFS를 우선하고 없을 때만 OFS를 사용한다. 결과에 `fs_basis`와
fallback 여부를 남긴다.

- TTM, YoY, SUE는 필요한 모든 quarter가 같은 `fs_basis`일 때만 official 값을 만든다.
- CFS/OFS 전환 구간은 NULL과 `mixed_fs_basis` flag로 처리한다.
- 2025년 이후 OFS 부재 때문에 생기는 coverage 감소를 0으로 채우지 않는다.
- KRW amount와 share 단위를 명시적으로 검사한다. 알 수 없는 currency/unit를 KRW로
  가정하지 않는다.
- 원시 cash outflow는 mapping rule에 방향이 정의된 항목만 양의 지출 magnitude로
  정규화한다.

### 3.7 standalone quarter와 PIT TTM

metric을 instant와 duration으로 분리한다.

- instant: assets, equity, shares처럼 period end level인 값
- duration: revenue, gross profit, income, CFO, dividend, buyback처럼 기간 누적인 값

duration metric은 statement kind와 source field semantics를 먼저 고정한다. OpenDART
[단일회사 전체 재무제표 개발가이드](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DE003&apiId=AE00036)를
원천 계약으로 삼는다. 손익계산서/포괄
손익계산서의 분기·반기·3분기 filing은 OpenDART가 `thstrm_amount`를 당기 3개월,
`thstrm_add_amount`를 누적으로 제공하므로 direct 3개월 값을 official standalone으로 쓴다.

```text
Q1/Q2/Q3 income-statement official = filing의 thstrm_amount
Q1/Q2/Q3 cumulative cross-check    = 각 thstrm_add_amount의 순차 차분
Q4 income-statement official       = annual cumulative - Q3 cumulative
```

direct와 cumulative-derived 값의 차이가 config의 절대·상대 tolerance를 넘으면
`standalone_source_conflict=true`로 두고 official row에서 제외한다. direct가 없는 경우에만
동일 receipt/basis의 cumulative-derived 값을 명시적 fallback으로 허용한다. 이 규칙을 현금흐름
계산서에 확대하지 않는다. CFO처럼 누적 공시되는 duration은 다음 차분 경로를 official로 쓴다.

```text
Q1 = q1 cumulative
Q2 = half cumulative - q1 cumulative
Q3 = q3 cumulative - half cumulative
Q4 = annual cumulative - q3 cumulative
TTM = Q(t) + Q(t-1) + Q(t-2) + Q(t-3)
```

같은 interim income filing의 `frmtrm_q_amount`는 전기 동기 3개월 비교치다. SUE의 official
전년동기 numerator는 이 값을 사용해 현재 공시가 함께 제시한 재작성 비교치와 current value를
비교한다. 이는 event 시점에 공개된 값이므로 PIT에는 맞지만 과거에 실제 알려졌던 q-4 값과는
다를 수 있다. 당시 원공시 q-4 값을 쓴 `as_was_comparative`를 secondary로 함께 계산한다.
사업보고서의 Q4는 current/prior annual cumulative에서 각 Q3 cumulative를 빼서 비교한다.

각 계산은 `available_from` 시점에 공개돼 있던 quarter vintage만 사용한다. 예를 들어 과거
Q1이 다음 해 정정되면 그 정정 접수 다음 session 전의 TTM을 소급 변경하지 않는다.

완전한 4 quarter가 없거나 period 연속성이 깨지면 TTM은 NULL이다. partial TTM을 연율화하지
않는다. negative/비정상 denominator는 feature별 규칙에 따라 NULL과 quality flag로 남긴다.

### 3.8 일별 broadcast와 정보 나이

sparse financial vintage를 다음 vintage 전까지 interval join한다.

```text
[available_from, next_available_from)
```

daily row에는 feature별 정보 나이를 함께 둔다.

```text
fin_age_days     = trade_date - disclosed_date       # calendar day
fin_age_sessions = formation_session_idx - available_session_idx
```

여러 component를 쓰는 feature는 계산 결과가 유효해진 가장 늦은 component availability를
feature vintage의 시작으로 사용한다. 정정공시로 feature가 바뀌면 age가 다시 0부터 시작한다.

continuous 경로는 formation 당시 PIT characteristic의 예측력을 묻기 때문에 다음 정기공시가
나와 feature vintage가 바뀌어도 이미 시작한 forward label을 자르지 않는다. 이는 event
window와 다른 의도된 정책이다. 각 horizon/bucket에 대해 수익 구간 중 다음 filing
`available_from` 이후가 차지하는 `post_next_filing_return_share`를 계산해 fin-age와 함께
stale-information 해석 자료로 남긴다.

## 4. family별 산식과 readiness

### 4.1 규모와 가치 composite

시가총액은 A0 PIT issued shares와 quality-valid close를 사용한다.

```text
market_cap              = close * issued_shares_pit
fin_log_mcap            = ln(market_cap)
fin_book_to_market      = total_equity / market_cap
fin_earnings_yield      = controlling_net_income_ttm / market_cap
fin_cfo_yield           = operating_cash_flow_ttm / market_cap
fin_sales_to_price      = revenue_ttm / market_cap
```

규칙:

- `market_cap<=0`, shares invalid, price-quality invalid는 NULL이다.
- `total_equity<=0`이면 B/M만 NULL이고 `negative_equity=true`를 남긴다.
- E/P와 CFO/P의 음수는 적자·음의 현금흐름 정보이므로 보존한다.
- component를 `(trade_date, market)` 안에서 1/99% winsorize한 뒤 z-score한다.
- 유효 component가 2개 이상일 때만 그 평균을 `fin_value_z`로 만든다.
- component count와 각 component coverage를 daily row와 report에 남긴다.

`fin_log_mcap`은 size segment 자체이므로 size-tertile 강건성은 self-segment diagnostic으로만
표시하고 screen gate에 중복 적용하지 않는다.

### 4.2 수익성

```text
avg_assets = (assets_t + assets_t-4q) / 2

fin_gross_profitability = gross_profit_ttm / avg_assets
fin_operating_profitability = operating_income_ttm / avg_assets
```

`fin_gross_profitability`가 primary다. direct gross profit이 없지만 같은 filing/basis의 revenue와
COGS가 모두 있으면 `revenue-cogs` fallback을 허용하고 source flag를 남긴다. 평균자산은 두
instant가 모두 양수이고 4-quarter 간격이 확인될 때만 계산한다.

`fin_operating_profitability`는 secondary다. primary coverage가 낮다는 이유로 결과를 본 뒤
공식 대표를 교체하지 않는다. primary 승격은 새 config version의 다음 연구에서만 한다.

### 4.3 자산성장과 발생액

```text
fin_asset_growth_yoy = total_assets_t / total_assets_t-4q - 1

fin_accruals_to_assets =
    (net_income_ttm - operating_cash_flow_ttm) / avg_assets
```

- 자산성장 분모가 0 이하이거나 period/basis 연속성이 깨지면 NULL이다.
- 발생액의 net income과 CFO는 같은 four-quarter set과 fs basis를 요구한다.
- `avg_assets<=0`은 NULL이다.
- `fin_cash_earnings_quality` 등 추가 ratio는 secondary/readiness 진단으로만 둔다.

### 4.4 경제적 순발행

단순 `issued_shares_t / issued_shares_t-1y - 1`은 액면분할·병합·무상증자를 발행 신호로
오인한다. official `ev_net_share_issuance_yoy`는 다음 절차를 통과한 경우에만 활성화한다.

1. OpenDART [증자(감자) 현황 API](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019004)의
   `irdsSttus`를 `dart_capital_change_raw`로 수집하고
   `isu_dcrs_de`, `isu_dcrs_stle`, 주식종류, 수량, 액면가·발행가를 보존한다.
2. `isu_dcrs_stle` mapping table로 유상증자·전환권행사·주식매수선택권행사 같은 경제적
   발행과 액면분할·병합·무상증자·주식배당·소각 같은 mechanical action을 구분한다.
3. `dart_share_count_raw`의 `now_to_isu_stock_totqy`, `now_to_dcrs_stock_totqy`와
   `redc`, `profit_incnr`, `rdmstk_repy`, `etc`는 총 증가·감소 및 감소분 교차검증에만 쓴다.
   이 source에 없는 증가 사유를 추정하지 않는다.
4. 기초 + 분류된 증가 - 분류된 감소 = 기말 identity를 source tolerance 안에서 검증하고,
   A0 share-change/price-jump CA event를 보조 교차검증한다. A0 CA 검출만으로 발행 사유를
   대체하지 않는다.
5. economic class로 명시적으로 분류되고 identity가 맞는 순증가만 1년 합산한다.

```text
ev_net_share_issuance_yoy = economic_net_new_shares_1y / lagged_issued_shares
```

identity 불일치, 미분류 `isu_dcrs_stle`, CA 충돌이면 해당 row를 NULL과 quality flag로 둔다.
raw share-count YoY를 공식 feature로 fallback하지 않는다. B-0에 고정한 분류·reconciliation
coverage 기준을 B-1~B-5 source-only 점검에서 만족하지 못하면 4개 candidate는
`blocked_exploratory`로 readiness freeze하며 결합 BH의 `M_B_ready`에서 제외한다. freeze 뒤
예상 밖 runtime failure가 난 경우에만 네 셀을 `p_for_bh=1.0`으로 유지한다.

### 4.4.1 capital action list의 vintage 중복과 dedup 규칙 (2026-08-12 계약 보강)

§4.4 1단계는 `irdsSttus` 응답을 `dart_capital_change_raw`에 보존하라고만 정했고, compute에서
**어느 vintage를 쓸지도 dedup 규칙도 정하지 않았다**. 실데이터에서 이 공백이 드러났다.

`irdsSttus`는 해당 사업연도 사건이 아니라 **상장 이후 누적 이력**을 보고서마다 다시 반환한다.
raw unique key에 `bsns_year`/`reprt_code`/`rcept_no`가 들어 있어 같은 실사건이 보고서 vintage
마다 한 벌씩 쌓인다. `event_scan.build_issuance_sql`의 `capital_change_classified`는 ticker
전체 행을 필터 없이 SUM하므로 §4.4 4단계 identity가 구조적으로 깨지고, 결과적으로
`ev_net_share_issuance_yoy`가 거의 전부 NULL이 된다. 안전망은 설계대로 작동하지만 feature가
죽는다. 이는 수집 범위 문제가 아니라 compute 규칙의 공백이다.

관측(prod, 2026-08-12):

- 실제 사건이 든 판은 `bsns_year=2025, reprt_code=11011` 한 벌뿐(765 ticker). 분기·반기보고서
  행은 전부 `isu_dcrs_stle='-'`, 수량 NULL인 placeholder였다. 표본이 작아 단정하지 않고
  아래 프로브에서 재확인한다.
- 두 vintage가 다 있는 `000040`을 대조하면 36건 중 34건이 동일하고, 1건은 **날짜가 정정**됐으며
  (2021-01-31 → 2021-01-13) 1건은 신규였다. 과거 판이 조용히 고쳐진다.

**dedup 규칙(확정).** raw는 immutable로 두고 compute에서만 축약한다.

1. `reprt_code='11011'` 행만 쓴다. placeholder가 최신 vintage로 뽑혀 이벤트 집합이 비는 것을
   막는다.
2. 티커별로 **판(vintage) 하나를 통째로** 고르고 그 판의 행만 쓴다. 여러 판을 event 식별자로
   union하지 않는다.
3. 판의 접수일 다음 session을 `vintage_available_from`으로 두고, 판 선택 규칙을 여기에 건다.

2항을 event 단위 union으로 하지 않는 이유는 실측에서 나왔다. `000040`의 같은 전환권행사
4,476,350주가 2024판에는 2021-01-31, 2025판에는 2021-01-13으로 실려 있다. 날짜나 수량이 정정되면
같은 사건이 서로 다른 식별자를 갖게 돼 union 뒤에도 두 건으로 남고, §4.4 4단계 identity가 오히려
더 자주 깨진다. 한 판은 발행사가 자기 share count에 맞춰 정합적으로 작성한 목록이므로 통째로
쓰는 쪽이 identity 통과 가능성이 높다.

두 선택지는 "어느 판을 고르는가"에서만 갈린다.

- **(a) latest-vintage** — 티커별 최신 판 하나를 전 구간에 쓴다. 전 구간이 계산되지만 filing
  position보다 나중에 발행된 판의 정정을 소급 적용한다.
- **(b) strict PIT** — position마다 `vintage_available_from <= position.available_from`인 판 중
  최신을 고른다. look-ahead가 없는 대신 그 시점 이전 판이 없는 초기 연도가 NULL이 된다.

어느 쪽을 채택할지는 아래 측정으로 정하며, 규칙과 임계값은 결과 확인 전에 고정한다.

### 4.4.2 isu_dcrs_stle 매핑 판단 근거 (v2, 2026-08-12)

§4.4 2단계가 요구한 mapping table의 v1이 실제 source가 반환하는 사유를 다 담지 못했다.
수집된 annual vintage 이벤트 259건 중 **58건(22.4%)이 미분류**로 떨어졌고, 미분류가 하나라도
있으면 §4.4 4단계가 그 창 전체를 NULL로 만들기 때문에 issuance family의 실질적 병목이었다.
버그가 아니라 카탈로그 누락이다.

관측된 미분류 사유와 판정:

| `isu_dcrs_stle` | 건수 | 판정 | 근거 |
|---|---|---|---|
| 신주인수권행사 | 50 | economic_increase | 이미 매핑된 `전환권행사`와 동일한 구조. 워런트 행사로 신주가 발행되고 기존 주주가 희석된다 |
| 무상감자 | 13 | mechanical_decrease | 이미 매핑된 `감자(무상)`과 같은 행위의 다른 표기 |
| 유상증자(주주우선공모) | 5 | economic_increase | 이미 매핑된 유상증자 3종과 같은 계열, 배정 방식만 다르다 |
| 출자전환 | 2 | economic_increase | 아래 참고 |
| `-` | 11 | **미분류 유지** | 발행사가 사유를 비워둔 행. source에 없는 사유를 추정하지 않는다(§4.4 3단계) |

**출자전환**은 판단이 갈릴 수 있어 근거를 남긴다. 현금 납입이 아니라 채무가 자본으로 바뀌는
것이지만, 신주가 실제로 발행돼 기존 주주 지분이 희석된다. net share issuance anomaly가 포착하는
것이 바로 그 희석이고, 통상 부실 구조조정 국면에서 발생해 후속 수익률이 낮다는 방향도 가설과
일치한다. 미분류로 남기는 선택지의 비용이 비대칭이라는 점이 결정적이다 — 미분류는 2건을 빼는
게 아니라 그 2건이 속한 창 전체를 NULL로 만든다. 관측을 살리는 쪽이 예측 목적에 맞다.

**정확 일치를 유지한다.** `무상감자`를 `감자(무상)`에 흡수시키려고 어순 정규화나 부분 일치
규칙을 넣지 않는다. 그 정도로 느슨한 규칙은 다음에 나올 미지의 사유도 조용히 흡수하고, 그러면
"추정하지 않는다"는 3단계 원칙이 무력화된다. 각 문자열은 읽고 판단해서 추가했다.

매핑 버전은 `EVENT_FEATURE_FORMULA_VERSION = "issuance_v2"`이며 §1.3 fingerprint의
`event_feature_formula_version`으로 `phase_b_run_spec.json`에 기록된다. 매핑이 바뀌면 같은
snapshot의 기존 artifact를 재사용하지 않는다.

보강 후 미분류 비율은 22.4% → **4.25%**(남은 11건은 전부 `-`). 이 변경은 Phase B scan을 한 번도
돌리기 전에, 각 사유의 경제적 의미만으로 판정했다. 결과를 보고 고친 것이 아니다.

#### vintage distance probe — 측정 설계

`dart sync-share-info --bsns-years {2024,2020,2016} --reprt-codes 11011`로 annual vintage 세 벌을
추가 수집한다. 2025판 기준 거리 1년·5년·9년 세 점이다. 1년 거리만 재면 "2025년 문서로 2016년
feature를 계산해도 되는가"에 답할 수 없다. `dart_share_count_raw`·shareholder_return은 2015년
부터 이미 있어 skip되므로 종목당 1요청, 연도당 약 22분이다.

11개 연도를 전부 받지 않는 이유는 그것이 곧 (b)가 요구하는 수집이기 때문이다. 세 벌은 나머지
8벌의 필요 여부를 판정하는 게이트다.

지표는 두 개다.

1. **feature-changing 불일치율** — 옛 판과 2025판을 종목별로 대조한다. 비교 범위는 옛 판의
   회계연도 말 이전 사건이다. 추가·삭제·수량 변경·날짜 변경을 모두 세되, 실제로 feature를
   바꾸는 것은 **`(stlm_dt_prior, stlm_dt]` 창 경계를 넘는 날짜 변경**과 수량·분류 변경뿐이다.
   이 비율을 거리 1·5·9년별로 보고한다.
2. **identity 통과율** — 같은 filing position 집합에 (a)/(b)를 각각 적용해 §4.4 4단계 identity를
   통과하고 `ev_net_share_issuance_yoy`가 non-NULL이 되는 비율을 센다. (b)는 2016판으로
   2017~2020, 2020판으로 2021~2024, 2024판으로 2025 position을 부분 평가한다.

**판정 기준(결과 확인 전 고정).**

| 9년 거리 feature-changing 불일치율 | 결정 |
|---|---|
| 1% 미만 | (a) 채택 |
| 1~5% | (a) 채택 + `vintage_lookahead_ratio` quality flag, 민감도 병기 |
| 5% 초과 | (b) 채택, 잔여 8개 연도 vintage 수집 |

우선 규칙 하나를 둔다. **(b)의 identity 통과율이 (a)의 절반 이하이면 불일치율과 무관하게 (a)를
채택**하고 (b)는 민감도 분석으로만 남긴다. 전 구간이 NULL인 feature는 후보로서 판정 대상이
되지 못한다.

채택 근거와 측정값은 `08_phase_b_implementation_log.md`에 기록한다. (a)로 정해지면 look-ahead는
선언된 제약으로 family 카드에 명시한다. `blocked_exploratory` freeze와 마찬가지로 이 결정도
scan 결과를 보기 전에 끝낸다.

### 4.5 주주환원

```text
cash_dividends_ttm = normalized dividend total over four standalone quarters
buyback_cash_ttm   = normalized treasury-share acquisition cash outflow TTM

ev_payout_yield = (cash_dividends_ttm + buyback_cash_ttm) / market_cap
```

source 우선순위:

1. cash dividend: `dart_shareholder_return_raw`의 배당금총액
2. 위가 없을 때만 `DPS × eligible common shares` proxy를 secondary로 계산
3. buyback: mapped `treasury_share_acquisition_amount` cash-flow metric

행이 없다는 사실을 즉시 0으로 해석하지 않는다. 해당 filing의 dividend/CF statement가
존재하고 target row가 0 또는 dash로 명시된 경우만 0으로 정규화한다. statement 자체가
없거나 mapping이 실패한 경우는 NULL이다.

`ev_dividend_yield`, `ev_buyback_yield`는 component secondary다. issuance를 빼는
`ev_net_payout_yield`는 Phase B primary가 아니다.

### 4.6 SUE와 event-time

official SUE는 standalone controlling net income과 standalone weighted-average shares를
사용한다.

```text
quarterly_eps_q = standalone_controlling_net_income_q
                  / standalone_weighted_avg_shares_q

interim comparative_eps_q-4 = current filing의 frmtrm_q_amount
                              / same filing comparative weighted shares
annual Q4 comparative_eps_q-4 = reconstructed prior-year Q4 EPS

seasonal_change_q = quarterly_eps_q - comparative_eps_q-4

fin_sue_q = seasonal_change_q
            / sample_std(previous 8 valid original-event seasonal changes)
```

interim current/comparative numerator는 §3.7의 같은 original filing에 실린
`thstrm_amount`/`frmtrm_q_amount`를 쓴다. comparative weighted shares도 같은 receipt의 prior
3-month XBRL context를 요구한다. 사업보고서 Q4는 current/prior annual에서 각 Q3를 뺀
standalone 값을 쓴다. 따라서 official 최소 이력은 현재 event와 **과거 유효 surprise 8개**며,
13개 연속 EPS quarter를 별도 hard gate로 두지 않는다. 다만 과거 original filing 당시의 q-4
EPS를 쓴 `as_was_comparative`와 13-quarter chain은 restatement 민감도 secondary로 남긴다.

current/comparative 3-month XBRL context의 weighted-average shares가 있으면 direct 값을
우선한다. 누적 context만 있을 때는 기간 가중으로 standalone quarter를 복원한다. 아래는
분기 길이가 같을 때의 축약식이며 구현은 XBRL duration의 실제 일수를 가중치로 사용한다.

```text
Q1 shares = q1 cumulative average
Q2 shares = 2 * half cumulative average - Q1
Q3 shares = 3 * q3 cumulative average - 2 * half cumulative average
Q4 shares = 4 * annual cumulative average - 3 * q3 cumulative average
```

weighted-share coverage가 SUE의 선행 병목이므로 B-6은 먼저 current/comparative share context
coverage를 측정하고, 통과한 뒤 과거 surprise 8개를 만든다. 양수·단위·period 연속성 sanity를
통과하지 못하면 official SUE는 NULL이다. period-end issued shares로 나눈
`fin_sue_issued_share_proxy`는 secondary이며 official SUE가 되지 않는다.

event 규칙:

1. §3.5 receipt relation으로 확인된 `(ticker, bsns_year, reprt_code, fs_basis)`의 최초 공개를
   original event로 정의한다. raw에 우연히 남은 최소 `rcept_no`를 original로 간주하지 않는다.
2. 같은 날짜 정정은 다음 session에 알 수 있는 최종 same-day vintage로 original event 값을
   구성한다.
3. `event_formation_date`는 original disclosed date 다음 KRX session이다.
4. 수익률은 formation close에서 미래 close까지 계산한다. 접수 당일·formation 당일의
   overnight/장중 수익은 쓰지 않는 보수적 경로다.
5. primary는 original event 후 60 session 안에 늦은 정정이 없고, 품질 mask 전 endpoint와
   모든 CA-quality endpoint가 `(0,3]`부터 `(40,60]`까지 완전히 존재하는 event만 쓰는
   `constant_60_session_endpoint` 표본이다. 같은 event set을 여섯 bucket에 고정한다.
6. primary는 다음 정기 filing에서 자르지 않는다. 표준 PEAD처럼 후속 공시도 drift 구간의
   일부로 두되, bucket별 `post_next_filing_return_share`를 반드시 보고한다.
7. 보수적 sensitivity로 다음 filing formation에서 자른 `next_filing_censored`와 bucket별
   endpoint만 요구하는 `available_event` 표본을 secondary로 함께 계산한다. partial return을
   온전한 bucket으로 축약하지 않는다.
8. primary/secondary 각각 bucket별 `reprt_code` 구성비, event/issuer 수, 60-session retention,
   revision·next-filing·CA·endpoint 탈락 사유를 저장한다.

event return은 A0 label과 같은 corporate-action mask와 시장 동일가중 excess 정책을 사용한다.
primary의 고정표본 덕분에 peak/onset/half-life는 표본 구성 변화가 아니라 동일 event의
event-time 변화로 해석한다. secondary 곡선의 peak는 탐색 진단일 뿐 결론 카드의 primary
half-life를 대체하지 않는다.

### 4.7 초기 readiness 예상

| Family | 현재 코드 상태 | Phase B 예상 작업/차단 조건 |
|---|---|---|
| 규모 | A0 mcap 의존 | A0 PIT shares/mcap lineage 검증 후 확장 |
| 가치 | 미구현 | TTM, component mapping, value-z 필요 |
| 수익성 | 기존 level ratio만 존재 | standalone/TTM/avg-assets로 재구현 |
| 자산성장 | 미구현 | 4-quarter instant continuity 필요 |
| 발생액 | 미구현 | net income/CFO TTM 동시 coverage 필요 |
| SUE | 미구현 | original filing source·current/comparative weighted shares·8개 surprise history·고정표본 event path 필요 |
| 경제적 순발행 | raw YoY proxy만 존재 | `irdsSttus` 사유 정규화·identity reconciliation 선결 |
| 주주환원 | canonical DPS만 존재 | 배당총액·buyback cash mapping/zero policy 선결 |

readiness status는 최소 다음 값을 사용한다.

```text
ready
ready_with_grade_cap
blocked_metric_mapping
blocked_period_end
blocked_fs_basis
blocked_weighted_shares
blocked_original_filing_source
blocked_original_filing_value
blocked_ca_classification
blocked_payout_source
insufficient_history
insufficient_cross_section
```

## 5. 분석 표본과 통계

### 5.1 continuous 분석 패널

7개 continuous family의 기본 grain은 Phase A와 같다.

```text
(trade_date, ticker, market)
```

패널은 다음 컬럼을 가진다.

```text
broad/tradable membership
common_formation_120d / common_survivor_120d
formation_session_idx
price/label quality와 CA flags
PIT mktcap·size/liquidity segment
feature native/extra-delay variant
feature_available_from / fin_age_days / fin_age_sessions
fs_basis / component_count / mapping quality flags
label_scan cumulative·bucket raw/excess/rank/end date
```

official sample은 Phase A와 동일하게 broad + common formation + quality mask 전
common survivor + official feature variant다. CA mask는 survivor 정의 뒤 label/feature
eligibility에 적용한다. available sample은 attrition 진단으로만 사용한다.

### 5.2 event 분석 패널

SUE의 grain은 다음이다.

```text
(ticker, original_rcept_no, event_formation_date, market)
```

각 event에 다음을 저장한다.

```text
sue_value / sue_history_count / fs_basis
reprt_code / bsns_year / comparative_policy
original_disclosed_date / event_formation_date
revision_cutoff_date / next_filing_cutoff_date / effective_cutoff
bucket start/end date / raw return / market excess return
primary_constant_sample / secondary_sample_kind
CA contamination / endpoint existence / censor reason / post_next_filing_return_share
```

primary cohort key는 `event_formation_date`다. market별 percentile rank로 변환한 뒤 두 시장의
rank pair를 pooling하므로 market level 차이를 섞지 않는다. `(event_formation_date,market)`
cohort는 segment diagnostic으로 별도 보존한다. 같은 ticker-event의 일별 broadcast 행을
join하지 않는다.

### 5.3 continuous IC·NW·spread

continuous family는 Phase A의 통계 구현을 그대로 호출한다.

1. `(trade_date, market)` Spearman IC
2. 종목 수 가중 daily IC
3. actual KRX session distance 기반 gap-aware Newey–West
4. 5분위 Q5-Q1 raw excess spread
5. `min_names_per_date_market=20`, spread는 50

lag와 최소 날짜 수:

```text
cumulative h: L = h - 1
bucket (h1,h2]: L = (h2-h1) - 1
min_dates_required = max(60, L+2)
```

`kospi_weight_mean`, `kosdaq_weight_mean`, single-market date ratio를 결과와 카드에 남긴다.

### 5.4 SUE cohort IC와 추론

event-time 통계는 다음 순서로 계산한다.

1. `(event_formation_date,market)` 안에서 SUE와 bucket excess return을 percentile rank로
   변환한다. 한 market이 pooled cohort에 기여하려면 event가 10개 이상이어야 한다.
2. formation date별로 market-neutral rank pair를 합치고 총 event가 30개 이상일 때 Spearman
   IC 하나를 계산한다. market별 30-event IC는 segment diagnostic으로 별도 계산한다.
3. event bucket width `w`에 대해 `L=w-1`인 gap-aware NW를 적용한다.
4. 최소 cohort 수는 `max(min_event_cohorts, L+2)`다.

event cohort가 없는 KRX session을 압축해 이웃으로 만들지 않는다. covariance pair는 실제
formation session index 거리로 정한다.

`n_cohorts`와 별도로 `(bsns_year,reprt_code)`의
`n_independent_filing_windows`를 보고한다. formation date가 많아도 분기 마감 주변 소수 window에
몰리는 의존성을 숨기지 않는다. primary discovery는 999회 issuer cluster bootstrap과 999회
`(bsns_year,reprt_code)` filing-cycle block bootstrap을 모두 수행한다. 전자는 issuer의 반복
history 전체를, 후자는 같은 보고 주기의 event 군집 전체를 resample해 cohort IC를 재계산한다.
둘 중 하나라도 expected sign과 다르거나 양측 empirical p가 0.10 이상이면 SUE의
`screen_pass=false`다. bootstrap은 BH p-value를 대체하지 않는 후속 강건성 gate다.

### 5.5 segment와 freshness 진단

각 축은 독립적으로 실행하고 Cartesian product를 만들지 않는다.

| `segment_axis` | `segment` | 적용 |
|---|---|---|
| market | KOSPI / KOSDAQ | 전 family |
| size | small / mid / large | size family는 self-segment diagnostic |
| liquidity | low / mid / high | 전 family |
| period | family별 유효 common period | continuous 및 event |
| fin_age | fresh / mid / stale tertile | continuous financial family |
| fs_basis | CFS / OFS / fallback | financial·SUE |
| value_component_count | 2 / 3 / 4 | value diagnostic |
| filing_quality | clean / fallback / revision-heavy | 전 family |

fin-age와 size/liquidity tertile은 broad의 date×market cutpoint로 만들고 tradable에서
재계산하지 않는다. event의 period segment는 event formation date 기준이다.

현재 PIT industry code가 없으므로 current industry를 과거 전체에 backcast하거나 금융업을
현재 분류로 제외하지 않는다. value/profitability/accrual처럼 industry-sensitive family는
이 제한을 카드에 남기고 evidence grade 상한을 B로 둔다. 향후 PIT industry를 확보하면 새
config version으로 재검증한다.

B-0은 각 family/cell의 expected effective start, holdout 이전 formation date 수,
non-overlap offset당 예상 관측 수와 유효 common period 수를 outcome 없이 계산해 readiness
freeze에 넣는다. period sign gate는 유효 구간이 3개 이상이면 strict majority, 2개면 둘 다
expected sign이어야 통과하되 grade 상한 B, 1개 이하면 screen fail이다. 모든 family에 동일한
5구간 분모를 강제해 비어 있는 초기 구간을 암묵적으로 만장일치 조건으로 만들지 않는다.

A/B primary feature는 독립 가설이라고 해석하지 않는다. 공통 유효 `(trade_date,market)`에서
feature rank 상관을 계산하고 날짜×시장별 상관을 pairwise 유효 종목 수로 가중한 행렬과
분포를 산출한다. 최소 `fin_log_mcap↔px_amihud_20d`,
`fin_value_z↔px_mom_12_1`을 카드에 직접 표시한다. 상관 구조는 BH 모집단을 줄이는 근거가
아니라 발견 cluster와 중복 경제축을 해석하는 진단이다.

### 5.6 holdout 봉인

continuous:

```text
label_end_date_120d < 2025-08-01     # common sample
각 horizon/bucket end date < 2025-08-01
```

event:

```text
event_bucket_end_date < 2025-08-01
```

holdout 안에 발생한 공시 자체도 feature 선택·mapping 선택·coverage 기준 조정에 사용하지
않는다. holdout 가격과 DART row를 임의 변경한 fixture에서 official Phase B 통계 content
hash가 변하지 않는 tripwire를 둔다.

## 6. 작업 패키지와 구현 순서

### B-0. config 확장과 preflight

**목적**: 결과를 계산하기 전에 최대 38개 candidate, outcome-blind readiness와 source·formula
계약을 동결한다.

구현:

1. A0 config에 §2.4의 Phase B section을 추가하고 schema version을 올린다.
2. 8개 family의 feature, expected sign, primary/secondary 역할과 최대 38개 candidate를 검증한다.
3. 현재 저장된 financial row가 동일 `rcept_no` XBRL의 period/instant, basis, unit, value와
   일치하는지 전수 대조한다. official 사용 row의 pairing mismatch 허용치는 0이다.
4. raw table/column, A0 mart, mapping rule, period source readiness를 점검한다.
5. family/cell별 예상 effective start, holdout 이전 formation 수, h별 offset당 예상 관측 수,
   유효 period 수를 계산한다. SUE는 weighted-share coverage, pooled/market cohort 수와
   independent filing window 수를 먼저 잰다.
6. immutable static `phase_b_run_spec.json`을 기록한다. B-1~B-6이 source/feature mart를 만든
   뒤 같은 규칙을 재실행하되 label/return column은 읽지 않고
   `phase_b_readiness_freeze.json`에 `M_B_ready`, blocker, 예상 표본을 최종 동결한다.
7. `M_AB=75+M_B_ready`인 combined registry를 deterministic하게 만든다.

테스트:

- candidate registry가 37/39개면 실패
- `ready_primary + blocked_exploratory != 38`이면 실패
- AB registry가 `75+M_B_ready`와 다르면 실패
- A hypothesis ID 또는 raw p-value hash 불일치 거부
- config에 없는 fallback/source winner 사용 거부
- holdout override 거부
- 현재 industry code를 PIT로 소급하는 option 거부
- readiness 단계가 label/return/IC/p-value column을 읽으면 실패
- 같은 receipt의 XBRL과 값이 다른 official row가 하나라도 있으면 실패

완료 기준: 결과를 보지 않은 고정 config/input lineage와 outcome-blind readiness freeze가
생성된다.

### B-1. filing receipt·capital action raw와 original source

**목적**: 수집된 최소 `rcept_no`를 원공시로 오인하지 않고 SUE의 original-event source와
순발행의 증가·감소 사유 source를 확보한다.

구현:

1. 현재 financial/share/XBRL raw의 filing identity별 distinct receipt·접수 lag·수집
   vintage 수를 진단하고, 현재 baseline이 filing당 단일 captured receipt임을 manifest에 남긴다.
2. `dart_filing_receipt_raw` DDL과 domain/port/adapter/service/CLI 경로를 추가한다.
3. receipt history를 idempotent·skip-if-present 방식으로 수집하고 `ingestion_runs`에 별도
   RunType과 partial/failure count를 기록한다.
4. correction marker와 source metadata로 original–revision relation을 derived mart로 만든다.
5. original receipt의 XBRL document/fact가 없으면 기존 XBRL ingestion을 receipt-targeted
   backfill이 가능하도록 확장한다.
6. 접수목록으로 실제 정정 filing 비율과 receipt-targeted original XBRL backfill target 수를
   확정한 뒤, 모든 target을 terminal status까지 처리한다. 추정 약 5천 건은 capacity 참고값일
   뿐 gate 상수가 아니며 실제 manifest count가 정본이다.
7. `irdsSttus`의 `dart_capital_change_raw` DDL과 동일한 domain/port/adapter/service/CLI,
   idempotent/audit/partial-run 경로를 추가한다.
8. original filing value, revision chain, unlinked receipt, capital-action reason coverage를
   report한다.

테스트:

- 같은 receipt 재수집 idempotency
- API 일부 실패의 partial-run finalizer
- original/정정/연결 불가 receipt
- receipt는 있으나 original XBRL이 없는 blocked 상태
- `irdsSttus`의 유상/무상/주식배당/분할/감자/전환권행사 parser와 unknown reason
- secret/rate-limit 정책은 기존 OpenDART shared executor 재사용

완료 기준: SUE와 순발행 source readiness를 outcome 없이 판정할 수 있고, 문자열 추정
relation이나 A0 CA 추정만 official action source로 승격되지 않는다.

### B-2. `stock_metric_vintage_fact`

**목적**: raw에 실제 수집된 공시·정정 이력과 metric mapping lineage를 잃지 않는 sparse
fact를 만든다.

구현:

1. 현재 DART raw와 B-1 targeted XBRL을 mapping rule candidate relation으로 변환한다.
2. `rcept_no`, actual disclosed date, next-session availability를 부여한다.
3. XBRL/stlm/fallback period-end source를 결합한다.
4. candidate winner를 filing vintage 안에서만 선택한다.
5. receipt relation이 있으면 original/revision chain을, 없으면 captured sequence와 same-day
   winner를 계산한다.
6. schema/key/coverage/source hash와 fallback count를 manifest에 기록한다.

완료 기준: baseline 단일 captured row와 B-1에서 실제 추가된 서로 다른 receipt가 각각 남고,
다음 captured vintage 전에는 기존 값만 조회된다. 백필이 0건인 fixture에서는 명백한
availability-normalized 항등 변환이며, 존재하지 않는 정정 history를 만들지 않는다.

### B-3. standalone quarter·TTM mart

**목적**: 누적 duration과 instant metric을 PIT-consistent quarter history로 바꾼다.

구현:

1. instant/duration metric catalog를 config로 고정한다.
2. interim IS/CIS의 direct `thstrm_amount` selector와 cumulative 차분 cross-check를 구현하고,
   CFO와 annual Q4에는 cumulative 차분을 적용한다.
3. CFS/OFS basis continuity를 적용한다.
4. 4-quarter TTM, avg-assets, current/comparative weighted-average shares quarter를 계산한다.
5. 정정 vintage마다 영향받는 quarter·TTM의 새 effective row만 생성한다.
6. negative/period-gap/unit/source quality flag를 생성한다.

테스트:

- Q1=100, half=250, q3=390, annual=560이면 standalone 100/150/140/170
- interim `thstrm_amount` direct와 cumulative-derived 일치/불일치 flag
- `frmtrm_q_amount`가 current filing의 prior-year 3개월 비교치로 선택됨
- 네 quarter TTM 항등식
- 중간 quarter 결측 시 TTM NULL
- CFS/OFS 혼합 시 NULL
- 미래 정정이 과거 TTM을 바꾸지 않음
- 비12월 결산 period ordering
- weighted shares 기간가중 역산

완료 기준: family별 최초 TTM/SUE 가능일과 source/basis coverage가 readiness report에 나온다.

### B-4. `feat_fin_scan_daily`

**목적**: size/value/profitability/asset-growth/accrual 5개 financial family를 일별 PIT로
materialize한다.

구현:

1. A0 PIT shares/mcap과 quarterly metric vintage를 interval join한다.
2. §4.1–4.3 산식과 value component z-score를 구현한다.
3. native next-session과 extra-delay 1-session variant를 함께 만든다.
4. `fin_age_days/sessions`, fs basis, component/mapping quality를 붙인다.
5. broad panel 기준 coverage·분포·최초 유효일을 기록한다.

완료 기준: key가 `(trade_date,ticker,market)`에서 유일하고 모든 feature row의
`available_from<=trade_date`가 성립한다.

### B-5. issuance·payout normalization

**목적**: mechanical share action을 issuance로 오인하지 않고, payout의 cash source를
명시적으로 합친다.

구현:

1. `dart_capital_change_raw.isu_dcrs_stle`의 versioned action mapping과 unknown-reason report를
   만든다.
2. share-count total/reduction decomposition과 action 수량을 reconciliation하고 A0 CA event는
   보조 교차검증으로만 연결한다.
3. mechanical action을 제외한 경제적 순발행 1년 합산과 row-level quality flag를 만든다.
4. dividend total, DPS proxy, buyback cash를 별도 component로 정규화한다.
5. missing vs explicit zero를 구분한다.
6. `feat_event_scan_daily`에 native/extra-delay variant를 materialize한다.

완료 기준: issuance identity/action-reason과 payout source coverage가 사전등록 기준 미달이면
raw proxy로 대체하지 않고 readiness freeze에서 명시적으로 `blocked_exploratory` 처리된다.

### B-6. SUE event mart

**목적**: original filing 단위 SUE와 중복 없는 event return을 만든다.

구현:

1. original filing XBRL의 current/comparative weighted-share coverage를 먼저 측정한다.
2. coverage가 준비된 event에서 direct interim comparative 또는 reconstructed annual Q4로
   standalone EPS와 과거 surprise 8개 history를 계산한다. 13-quarter chain은 secondary다.
3. B-1 receipt relation과 original XBRL coverage를 검증하고, 미충족이면 6셀을
   `blocked_exploratory` candidate로 materialize한 뒤 event 계산을 중단한다.
4. original/same-day/later revision chain을 만든다.
5. next-session formation과 extra-delay formation을 만든다.
6. revision/next-filing date를 계산하되 primary는 revision 전 60-session endpoint 고정표본이고
   next filing으로 자르지 않는다. next-filing-censored는 secondary다.
7. 6개 event bucket의 raw/excess return, CA/end-point eligibility, reprt_code mix와
   `post_next_filing_return_share`를 만든다.
8. pooled market-neutral cohort, market diagnostic cohort, issuer 수와 independent filing window
   수를 기록한다.

테스트:

- 접수 당일 event 사용 금지
- 금요일·휴일 접수의 다음 session
- later revision이 60일 안에 있으면 primary 고정표본에서 event 전체 제거
- 다음 분기 공시 이후 return도 primary에는 남고 secondary censor에서만 제거
- 고정표본 여섯 bucket의 event ID·reprt_code 구성 동일
- 동일일 정정은 다음 session 최종 값 하나
- 과거 surprise 7/8개 경계와 13-quarter secondary
- event bucket 복리 항등식

완료 기준: event grain이 유일하고 daily broadcast 중복이 없다.

### B-7. continuous·event core scan

**목적**: 서로 다른 grain을 공통 결과 schema로 투영한다.

구현:

1. 7개 continuous family는 Phase A core runner를 재사용한다.
2. SUE는 cohort runner로 6개 primary event cell을 계산한다.
3. 모든 `ready_primary`/`blocked_exploratory`/secondary role을 결과에 부여한다.
4. continuous 32개 + event 6개 = 최대 38개 candidate의 exact coverage와
   `ready_primary=M_B_ready`를 검증한다.
5. daily/cohort IC intermediate를 저장한다.

완료 기준: B candidate table 38행과 각 셀의 frozen role·valid/blocked/insufficient 사유가
생성되며, primary 통계 table은 정확히 `M_B_ready`행이다.

### B-8. 강건성·null 실험

**목적**: overlap, repeated issuer, 다중검정 규모와 join 오류를 분리해 진단한다.

continuous:

1. broad/tradable, common/available, segment, extra-delay 진단
2. 모든 offset의 non-overlap subsampling. 기본 최소 관측은 20이지만 cumulative 120과
   `(60,120]`은 12로 사전등록하고 각 offset 안 daily IC 부호의 exact sign test를 쓴다.
   `valid_offset_ratio>=0.80` 및 유효 offset 중 expected-sign 비율 `>=0.60`을 강건성 기준으로
   삼으며 모든 offset의 충분성을 요구하지 않는다.
3. B continuous primary를 포함한 date×market 공동 rank permutation 100회
4. `nw_lag>=59`인 B continuous `ready_primary` 장기 셀에 circular date-shift 100회

결합 단면 permutation은 A/B continuous feature에 같은 row permutation을 적용하고 SUE는
같은 replicate seed의 event cohort rank permutation을 사용한다. 각 replicate의 `M_AB`개
p-value에 결합 BH를 적용해 null discovery count를 만든다. feature 값을 다시 rank하지 않고
date×market 또는 cohort 안의 frozen rank vector를 직접 치환한다. 이 permutation은 NW의 시계열
보정을 검증하지 않는다는 제한을 report에 명시한다.

SUE:

1. pooled market-neutral cohort 내 SUE rank permutation 100회
2. primary discovery에 issuer cluster bootstrap 999회
3. `(bsns_year,reprt_code)` filing-cycle block bootstrap 999회
4. event cohort gap-aware NW pair count와 `n_independent_filing_windows`
5. event formation ordinal 기반 non-overlap offset 진단; cohort가 부족하면 grade A 금지

완료 기준: 각 null/robustness artifact가 seed와 replicate ID로 재시작 가능하고 중복 없이
완료된다.

### B-9. 결합 BH와 screening 판정

**목적**: A/B의 readiness-freeze 고정 모집단으로 최종 screening 후보를 결정한다.

구현:

1. Phase A 75개와 B `M_B_ready`개의 raw p-value·status를 freeze hash 검증 후 결합한다.
2. `p_for_bh`와 `q_fdr_global_ab`를 계산한다.
3. Phase A/B family에 동일한 expected-sign·isolated-spike·robustness gate를 적용한다.
4. Phase A 결론 카드에도 combined q-value overlay를 별도 artifact로 만든다.
5. A-only pass였다가 AB에서 탈락한 셀을 숨기지 않는다.

Phase B candidate cell의 `screen_pass=true` 조건은 다음과 같다.

1. `q_fdr_global_ab < 0.10`이고 expected sign이 맞다.
2. isolated spike가 아니다.
3. tradable IC가 broad의 50% 이상이고 같은 방향이다.
4. family별 유효 common period가 3개 이상이면 strict majority, 2개면 두 구간 모두 같은
   방향이다. 1개 이하는 실패다.
5. available sample과 common-survivor의 방향이 같다.
6. 모든 PIT/source/formula readiness gate를 통과했다.
7. long continuous cell은 temporal placebo p<0.10이고, 사전등록된 최소 관측을 만족하는
   non-overlap offset 비율과 expected-sign 비율이 기준 이상이다.
8. SUE는 issuer와 filing-cycle cluster confirmation p가 모두 <0.10이다.
9. CA/availability/holdout 정책이 official contract와 일치한다.

증거 등급:

- `A`: screen pass + 핵심 경고 없음 + 유효 offset 비율 기준 충족 + grade cap 없음
- `B`: screen pass이나 industry/source/segment/offset 중 비치명 경고 존재
- `C`: secondary/proxy, available sign flip 또는 primary robustness 실패
- `D`: 무신호·부호 반대·combined BH 실패

`blocked_exploratory`는 A~D 발견 등급을 부여하지 않고 `NE—not evaluated`로 표시한다.
available sample sign flip은 screen fail이며 자동 grade C다. 유효 period가 정확히 2개라 둘 다
통과한 경우도 evidence grade 상한 B다. 반면 현재 raw가 단일 captured receipt라는 사실만으로
모든 continuous family를 B로 내리지는 않는다. §1.2 pairing hard gate와 실제 availability를
통과했기 때문이다.

PIT industry가 없는 이번 버전에서 value/profitability/accrual은 최대 B다. 현재 industry를
과거에 소급해 이 상한을 해제하지 않는다.

등급 체계가 B/C로 붕괴하지 않도록 구조적 표본 부족 자체를 `any_offset_insufficient`로
일괄 cap하지 않는다. 규모·자산성장·순발행·주주환원과 SUE는 각 source 및 장기 표본 gate를
통과하면 A가 가능하다. value·profitability·accrual의 최대 B는 PIT industry 부재라는 명확한
잔여 한계 때문이며, 해당 한계가 해결된 새 config에서만 A가 가능하다.

### B-10. 결론 카드·보고서·atomic publish

각 family 카드에는 최소 다음을 포함한다.

```text
family / primary_feature / formula_version / expected_sign / frozen_hypothesis_role
readiness / blocker / effective_start / coverage / expected_nonoverlap_dates
primary cells / q_fdr_phase_b / q_fdr_global_ab
peak cumulative or event bucket / onset / half-life / sign flip
broad/tradable IC와 retention / KOSPI weight
period/fin-age/fs-basis/source robustness / post_next_filing_return_share
revision/fallback/CA/industry/attrition warnings
reprt_code mix / n_independent_filing_windows
temporal placebo 또는 issuer·filing-cycle cluster confirmation
주요 A/B feature rank-correlation pair
evidence_grade / screen_pass / target label 후보 / next_step
```

publish 순서:

1. temporary run directory에 artifact 작성
2. schema/key/row/content hash 검증
3. manifest 작성
4. 최종 경로로 atomic rename
5. 마지막에 `_SUCCESS.json` 작성

중간 실패 directory는 `_SUCCESS.json`이 없으며 official consumer가 읽지 않는다.

## 7. 산출물 계약

### 7.1 디렉터리

```text
research/output/horizon_scan/
  snapshot_date=<selected>/
    source=sj2_remote/
      config=<config_hash>/
        phase=B/
          run=<run_id>/
            phase_b_run_spec.json
            phase_b_readiness_freeze.json
            readiness_matrix.parquet
            readiness_matrix.md
            filing_receipt_quality.parquet
            receipt_value_pairing_quality.parquet
            capital_change_quality.parquet
            stock_metric_vintage_quality.parquet
            quarterly_metric_quality.parquet
            feature_coverage.parquet
            event_coverage.parquet
            horizon_ic.parquet
            event_ic.parquet
            daily_ic.parquet
            cohort_ic.parquet
            nonoverlap_summary.parquet
            permutation_summary.parquet
            temporal_placebo_summary.parquet
            issuer_bootstrap_summary.parquet
            filing_cycle_bootstrap_summary.parquet
            primary_feature_rank_correlation.parquet
            phase_b_primary_hypotheses.parquet
            family_summary.parquet
            family_cards.md
            03b_horizon_scan_results.md
            manifest.json
            _SUCCESS.json
        phase=AB/
          run=<ab_run_id>/
            combined_ab_primary_hypotheses.parquet
            phase_a_card_overlay.parquet
            primary_feature_rank_correlation.parquet
            manifest.json
            _SUCCESS.json
```

### 7.2 공통 IC schema

continuous `horizon_ic.parquet`은 Phase A schema를 재사용하고 다음 Phase B 필드를 추가한다.

```text
feature_available_from
fin_age_days_mean / fin_age_sessions_mean
fs_basis / fs_fallback_ratio
mapping_fallback_ratio / revision_ratio
filing_history_status / original_value_coverage
receipt_value_pairing_status / history_left_truncated_ratio
period_end_fallback_ratio
value_component_count_mean
industry_pit_available
q_fdr_phase_b
q_fdr_global_ab
```

event 결과는 다음 필드를 가진다.

```text
family, feature, hypothesis_id
event_h_start, event_h_end
n_events, n_issuers, n_cohorts, n_cohorts_market, n_independent_filing_windows
n_events_per_cohort_mean
revision_censored_count, next_filing_censored_count
ca_masked_count, endpoint_missing_count
event_sample_kind, reprt_code_mix, constant_sample_retention
post_next_filing_return_share
ic_mean, ic_std, icir, t_naive, t_nw, p_nw
nw_lag, nw_pair_count_by_lag
q_fdr_phase_b, q_fdr_global_ab
issuer_bootstrap_p, issuer_direction_pass
filing_cycle_bootstrap_p, filing_cycle_direction_pass
status, status_reason
```

## 8. 파일별 변경 계획

| 파일 | 변경 |
|---|---|
| `research/analysis/horizon_scan_config.yaml` | Phase B 최대 38 candidate, readiness-derived m, PIT/event/formula/combined BH 계약 추가 |
| `sql/postgres_ddl.sql` | `dart_filing_receipt_raw`, `dart_capital_change_raw` raw table·index·natural key 추가 |
| `src/krx_collector/domain/models.py` | filing receipt·capital change raw domain model 추가 |
| `src/krx_collector/domain/enums.py` | filing/capital-action sync용 `RunType` 추가 |
| `src/krx_collector/ports/` | filing receipt protocol 신규, 기존 share-info/storage protocol에 capital change 추가 |
| `src/krx_collector/adapters/opendart_filings/` (신규) | OpenDART receipt list adapter, shared key/rate-limit executor 재사용 |
| `src/krx_collector/adapters/opendart_share_info/provider.py` | 기존 share-info adapter에 `irdsSttus` 추가 |
| `src/krx_collector/service/sync_dart_filings.py` (신규), `service/sync_dart_share_info.py` | 두 raw의 idempotent sync, audit, partial-run finalizer |
| `src/krx_collector/cli/app.py` | `dart sync-filings` 신규 wiring, 기존 `dart sync-share-info`에 capital change 포함 |
| `src/krx_collector/infra/db_postgres/` | 두 raw의 upsert/read adapter와 remote sync 등록 |
| `src/krx_collector/adapters/opendart_xbrl/`, `service/sync_dart_xbrl.py` | original receipt-targeted XBRL backfill 경로 추가 |
| `bin/raw-parquet-export-all.sh` | 신규 raw table export·manifest 포함 |
| `research/etl/config.py` | 신규 raw table을 lake source 목록에 등록 |
| `deploy/prod/bin/`, `docs/operations.md` | filing/capital-action incremental sync wrapper·감사/복구 절차 추가 |
| `docs/database.md` | 두 신규 raw schema·natural key 문서화 |
| `src/krx_collector/definitions/metric_rules.py` | 필요한 EPS·배당총액 등 mapping rule을 pure definition으로 추가 |
| `research/etl/marts/metric_vintages.py` (신규) | 실제 접수일·정정공시 보존 sparse vintage fact |
| `research/etl/marts/financial_quarters.py` (신규) | standalone quarter, TTM, weighted shares, basis continuity |
| `research/etl/features/fin_scan.py` (신규) | Phase B daily financial feature |
| `research/etl/features/event_scan.py` (신규) | issuance/payout normalization과 daily PIT |
| `research/etl/features/sue_event.py` (신규) | SUE original-event, cutoff, bucket return |
| `research/etl/snapshot.py` | Phase B exact raw/A0 dependency와 lineage 검증; 불필요한 common-derived gate 금지 |
| `research/etl/compute_all.py` | additive Phase B mart orchestration; PostgreSQL write 없음 |
| `research/analysis/horizon_scan.py` | continuous runner 재사용과 Phase B registry dispatch |
| `research/analysis/horizon_scan_event.py` (신규) | cohort IC, issuer bootstrap, event permutation |
| `research/etl/metrics.py` | event cohort aggregation·gap-aware NW 공통화 |
| `research/analysis/horizon_scan_report.py` | Phase B 카드와 AB q-value overlay |
| `tests/unit/test_metric_vintages.py` (신규) | actual rcept, same/later revision, period source |
| `tests/unit/test_sync_dart_filings.py` (신규), `test_opendart_share_info.py` | receipt/`irdsSttus` parser, idempotency, partial/rate-limit audit |
| `tests/unit/test_financial_quarters.py` (신규) | standalone/TTM/basis/weighted shares |
| `tests/unit/test_research_fin_scan.py` (신규) | family 산식, value-z, PIT age |
| `tests/unit/test_research_event_scan.py` (신규) | issuance reconciliation, payout missing/zero |
| `tests/unit/test_horizon_scan_event.py` (신규) | event censor/cohort/NW/bootstrap |
| `tests/integration/test_horizon_scan_phase_b_smoke.py` (신규) | 실제 lake smoke, 고정 row count 금지 |
| `03_horizon_predictive_power_plan.md` | 실제 접수일, captured/backfill vintage, readiness-derived 결합 BH 결정 동기화 |
| `04_specific_plan_A0.md`, `04_specific_plan_A.md` | schema version·AB 최종 BH 인계 의미 동기화 |

기존 `fin_pit.py`, `event.py` 테스트를 새 의미에 맞춰 조용히 바꾸지 않는다. 합성 lag legacy
경로의 회귀 테스트는 유지하고 Phase B official 경로가 이를 읽지 않는 것을 별도 테스트한다.

## 9. 검증 계획

### 9.1 unit test 필수 fixture

1. 실제 `rcept_no` 접수일과 next KRX session
2. 동일 receipt XBRL의 period/basis/unit/value pairing 일치와 단일 mismatch hard failure
3. receipt source로 확인된 원공시/정정, 연결 불가 receipt, raw 최소값 오인 방지
4. 같은 날 원공시/정정, 다른 날 정정과 targeted backfill에서만 multiple vintage 생성
5. `rcept_no` 결측에만 합성 fallback
6. 비12월 결산과 period source 충돌
7. 정정 전/후 interval as-of
8. CFS 우선, OFS fallback, basis 혼합 차단
9. interim direct `thstrm_amount`, `frmtrm_q_amount`, cumulative cross-check와 annual Q4 차분
10. 미래 정정이 과거 TTM을 바꾸지 않음
11. value component 1/2개 경계, negative equity, signed yield
12. gross-profit direct/fallback source
13. `irdsSttus` action reason, issuance identity, CA 교차검증과 unknown reason
14. payout explicit zero와 unknown missing 구분
15. current/comparative weighted shares와 SUE 과거 surprise 7/8개 경계
16. primary 60-session constant event set, 다음 filing 미절단, secondary censor
17. bucket별 reprt_code 구성·retention과 next-filing overlap share
18. pooled cohort 29/30, market contribution 9/10, cohort count L+1/L+2 경계
19. 실제 session gap이 있는 event NW
20. issuer 및 `(bsns_year,reprt_code)` block bootstrap
21. h=120 non-overlap 11/12개 경계, valid/directional offset ratio
22. family별 유효 period 1/2/3개 gate
23. candidate 38개와 `ready_primary + blocked_exploratory=38`
24. AB m이 `75+M_B_ready`이며 preflight-blocked는 제외, post-freeze blocked는 p=1
25. readiness code가 label/return을 읽지 못하는 dependency test

### 9.2 synthetic end-to-end

두 시장, 여러 ticker, 16개 quarter, 원공시·정정 receipt, 비12월 결산, mechanical share action과
dividend/buyback을 포함한 synthetic lake를 만든다.

검증:

- expected feature 값과 effective date
- 7개 daily + SUE event 경로의 grain
- holdout 미사용
- combined BH deterministic 결과
- permutation/temporal/bootstrap seed 재현성
- atomic success marker

synthetic signal에는 known delayed accounting signal과 known SUE event drift를 심고 기대
peak bucket이 복원되는지 확인한다. 실데이터에서 특정 anomaly가 나와야 한다는 assertion은
두지 않는다.

### 9.3 실제 lake smoke

선택된 `sj2_remote` snapshot에서 제한된 date/ticker 범위로 다음을 확인한다.

- raw/vintage row와 revision chain sample
- receipt-value pairing mismatch가 0인지
- family별 non-null·최초 유효일·market coverage
- TTM quarter completeness와 CFS/OFS 비율
- fallback lag/period end 비율
- capital-action reason/issuance reconciliation과 payout source coverage
- SUE cohort 크기·event count·filing-window 수·reprt_code mix·censor 사유
- key uniqueness와 finite value

smoke는 `official=false`이고 global discovery를 해석하지 않는다. row count를 특정 snapshot
상수로 고정하지 않고 manifest와 내부 identity를 검증한다.

### 9.4 회귀 검증

- 기존 `stock_metric_fact` golden parity 유지
- 기존 `feat_fin_pit`, `feat_event` consumer 결과가 additive mart 추가만으로 변하지 않음
- Phase A artifact와 통계 content hash 불변
- A0 universe/label/quality mart 불변
- raw/derived PostgreSQL write 없음
- source namespace 격리 유지

문서 작업 완료 시에는 Markdown 구조·링크·table column 수를 검증한다. 실제 구현 전이므로
코드 테스트는 실행 대상이 아니다.

## 10. 구현·리뷰 단위 권장 순서

1. **B-PR1 — contract**
   - config schema, 최대 38 candidate, readiness-derived m, run spec, pairing preflight
2. **B-PR2 — filing receipt·capital action raw**
   - 두 raw의 DDL/port/adapter/service/CLI/audit, original XBRL target backfill
3. **B-PR3 — vintage foundation**
   - actual rcept availability, period source, captured-vintage fact, receipt relation
4. **B-PR4 — quarterly financials**
   - direct interim/cumulative cross-check, TTM/basis/current·comparative weighted shares
5. **B-PR5 — daily financial feature**
   - size/value/profitability/asset growth/accrual
6. **B-PR6 — issuance/payout**
   - `irdsSttus` action classification/reconciliation, payout source normalization
7. **B-PR7 — SUE event**
   - direct comparative SUE, 60-session 고정표본, cohort labels
8. **B-PR8 — core statistics**
   - readiness freeze, continuous/event scan, dynamic combined BH
9. **B-PR9 — robustness/report**
   - segments, null experiments, issuer/filing-cycle bootstrap, rank-correlation, cards, publish

PR2의 original receipt source와 PR3의 captured-vintage fact가 downstream PIT 정본이다.
PR5와 PR6은 PR4 이후 독립 리뷰가 가능하지만 official combined run은 PR7–PR9까지 모두
완료된 뒤 한 번만 수행한다.

## 11. 주요 위험과 대응

| 위험 | 영향 | 대응 |
|---|---|---|
| 합성 +45/+90일 재사용 | 늦은·정정공시 look-ahead | actual `rcept_no` + next-session vintage, fallback count 공개 |
| 저장 값과 `rcept_no`가 다른 filing에서 옴 | Phase B 전반 look-ahead | same-receipt XBRL value/context 전수 pairing, mismatch 0 hard gate |
| canonical에서 revision collapse | 미래 정정값 소급 | captured row 정규화 + 백필 receipt만 additive vintage로 보존 |
| raw가 filing당 단일 captured receipt | PEAD event date·값 오인 | 접수목록으로 실제 정정 수 측정 + targeted original XBRL backfill |
| 비12월 결산 period 오류 | quarter/YoY/TTM 왜곡 | XBRL/stlm actual period source 우선 |
| 3개월액을 누적으로 오인 | 수익성·SUE 산식 오류 | interim `thstrm_amount` direct + cumulative 차분 cross-check |
| CFS/OFS 혼합 | 성장률·TTM 불연속 | same-basis 4Q requirement와 flag |
| 2025 OFS 부재 | 최근 coverage 편향 | basis coverage 보고, missing 보존 |
| EPS shares 부족 | SUE 표본 축소 | weighted-share coverage 선측정, issued proxy는 secondary |
| 정정공시를 새 SUE event로 계상 | event 중복·look-ahead | original event 하나, revision cutoff |
| next-filing censor로 report mix 변화 | 가짜 event decay | uncensored 60-session 고정표본 primary + reprt_code mix, censored secondary |
| 공시 군집과 issuer 반복 | naive 유의성 과대 | cohort IC + gap-aware NW + issuer·filing-cycle bootstrap |
| share split/무상증자를 issuance로 오인 | Q5 신호 오염 | `irdsSttus` 사유 + identity reconciliation, raw YoY fallback 금지 |
| 누적 이력을 vintage마다 중복 합산 | identity 붕괴로 issuance family 전멸 | §4.4.1 event 단위 dedup + vintage distance probe로 (a)/(b) 사전 확정 |
| missing buyback을 0 처리 | payout 과대/과소 | explicit zero와 unknown 분리 |
| 현재 active corp/industry 필터 | 생존·분류 look-ahead | active 필터 금지, current industry backcast 금지 |
| PIT industry 부재 | value/profitability 해석 혼합 | limitation + 해당 family grade B 상한 |
| TTM/SUE effective start 차이 | family curve 비교 오도 | family별 최초 유효일·coverage 병기 |
| 후기 시작 family의 h=120 offset 부족 | grade A 구조적 봉쇄 | 장기 최소 12 + exact sign/valid-offset 비율, 예상 n 사전 동결 |
| family별 유효 period 수 차이 | sign gate 의미 변형 | 3+/2/1 구간별 고정 판정과 2구간 grade cap |
| Phase별 별도 BH | 전체 FDR 붕괴 | A 75 + readiness-frozen B의 dynamic combined BH |
| blocked 판정을 결과 뒤 변경 | m 축소로 q-value 유리 | outcome-blind freeze 전만 제외, freeze 뒤 실패는 `p_for_bh=1` |
| Phase A 결과 후 B 규칙 변경 | 선택 편향 | config/A0 version 재등록 후 A/B 재실행 |
| holdout 공시·가격 참조 | 최종 평가 오염 | event/label endpoint boundary와 hash tripwire |
| 두 grain을 한 runner로 강제 | event 중복 또는 schema 혼란 | daily/event runner 분리, 결과 단계에서 통합 |

생존편향은 Phase B에서도 해소되지 않는다. 현재 상장 종목 중심 raw 수집과 historical
industry 부재를 모든 장기 카드에 명시하며, PIT membership coverage 밖의 장기 성과를
절대 수준의 증거로 과장하지 않는다.

## 12. Phase C·acceptance gate 인계물

Phase B가 완료되면 다음을 전달한다.

1. Phase B run spec, manifest, `_SUCCESS.json`
2. 최대 38개 B candidate의 frozen role과 `M_B_ready`개 primary raw p-value
3. `M_AB=75+M_B_ready`인 combined BH 결과와 freeze hash
4. 7개 continuous curve와 SUE 6-bucket 고정표본/secondary event curve
5. family별 source/basis/age/revision/action/CA/industry coverage
6. broad/tradable, common-survivor/available, period·market 진단
7. non-overlap, permutation, temporal placebo, issuer·filing-cycle bootstrap 결과
8. 8개 family conclusion card와 combined q-value가 반영된 A card overlay
9. `screen_pass=true`인 family/horizon band
10. sign reversal·segment-limited pattern의 Phase C 후보 목록
11. A/B primary feature rank-correlation matrix와 중복 경제축 진단

Phase C는 A/B에서 실제로 부호 반전 또는 경제적으로 해석 가능한 조건부 패턴이 나온
family만 새 config로 사전등록한다. interaction을 만들기 위해 Phase B의 primary 산식이나
event window를 같은 run 안에서 수정하지 않는다.

acceptance gate는 combined BH와 Phase별 robustness gate를 통과한 후보만 대상으로
증분성, purged walk-forward OOS, turnover·거래비용을 평가한다. holdout은 feature,
horizon, variant, interaction 선택이 모두 끝난 뒤 한 번만 연다.
