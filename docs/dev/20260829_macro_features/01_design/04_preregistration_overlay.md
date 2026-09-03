# 04. 사전등록 overlay, 실행 순서, PR 분할, 규율

- 작성일: 2026-08-29 (리뷰 `06_review_20260829.md` M1~M7·§7 반영: `notes` 수정, `vix_high` 국면, P3 국면 변경,
  `source_daily_ic` 제거, `registered_at` 절차, validate 추가 항목, 실행 순서에 G1·G2 사전 계산)
- 이 문서의 §1 YAML이 **계약 초안**이다. `research/analysis/horizon_scan_macro_20260829.yaml`로 커밋되는 순간
  hash가 계산되고, 그 뒤로는 §5의 규율을 따른다.
- 기존 config(`horizon_scan_config.yaml` `ab0de634…`, `horizon_scan_expansion_20260827.yaml` `889c3e83…`)와 그
  산출물은 **그대로 둔다.** `extends`를 두 단계로 이어 쓴다(`_load_raw`가 체인과 `families_append` 누적을 지원한다 —
  `horizon_scan_config.py:255~`).

---

## 1. overlay 초안 — `horizon_scan_macro_20260829.yaml`

`registered_at`은 커밋 직전에 실제 날짜로 바꾼다. **placeholder가 남은 상태로 hash를 기록하지 않는다.**

> **PR-1a-3 완료 (2026-08-29).** 아래 초안이 `research/analysis/horizon_scan_macro_20260829.yaml`로
> 그대로 커밋됐다(`registered_at: 2026-08-29`). **config hash `236d0d3515043e44e280f0c2c2707ca2cc486aa44b638eb893a7095ddac1110f`.**
> 기록은 `05_preregistration_record.md`. 초안과 실제 파일의 차이는 없다.

```yaml
# Third preregistration layer: macro exposure betas (Phase B append) and the
# first Phase C conditional-IC contract. Base + expansion hashes stay immutable.
extends: horizon_scan_expansion_20260827.yaml
schema_version: 5

preregistration:
  id: macro_20260829
  registered_at: 2026-08-29          # 커밋 직전 실제 날짜로 확정
  base_config_hash_prefix: 889c3e83
  outcome_blind: true
  notes:
    - Stage 0 (daily_ic persistence) changes no statistic; Phase A summaries must equal 889c3e83 canonical.
    - Interaction (regime x characteristic) is NOT registered as a scan family; under rank IC it reduces to
      the Phase C conditional-IC difference (01_design/00_overview §1.1).
    - Macro levels/changes are never cross-sectional families (00_survey/00 §3).
    - N8 employment regime_candidates from expansion_20260827 stay dormant and are not in phase_c.pairs.
    - Domestic factors (usdkrw, kr10y) are one session stale in the fact (next_krx_session); their betas pair
      resid_ret_tau with the change published at tau+1 and use a window ending at the prior session.
    - Exposure-beta families start ~2014-12 (factor history begins 2014-06-16); px_market_beta from panel start.
    - Phase C regimes are computed on the KRX session grid, not the fact's weekday grid (2014-2023 holiday rows).
    - Phase C daily_ic source run ids are CLI arguments recorded in phase_c_run_spec.json, not config.

phase_b:
  # 78 (expansion) + 6 macro-exposure families x 4 cells.
  primary_candidate_count_max: 102

phase_c:
  contract: conditional_ic_v1
  open_policy: preregistered_pairs
  holdout_policy: open_once_after_all_selection
  grid: krx_sessions                 # regimes built on label_scan trade_date grid, fact joined by date
  sample_start: 2015-06-16           # R1b/R2/R3 need 252 sessions of daily common features (start 2014-06-16)
  discovery_coordinate: {universe: broad, sample_kind: common_survivor}
  retention_coordinate: {universe: tradable, sample_kind: common_survivor}
  regimes:
    - {id: vix_up,         role: primary,     source: [global_vix_level],                              transform: diff_20_sessions,               cut: gt_zero}
    - {id: vix_high,       role: primary,     source: [global_vix_level],                              transform: minus_median_252_sessions,      cut: gt_zero}
    - {id: market_up,      role: primary,     source: [market_kospi_close],                            transform: log_ratio_252_sessions,         cut: gt_zero}
    - {id: liq_high,       role: primary,     source: [market_kospi_turnover_value, market_kosdaq_turnover_value], transform: log_mean20_over_median252_sum, cut: gt_zero}
    - {id: term_steep,     role: exploratory, source: [rate_kr_term_spread_10y_3y],                    transform: minus_median_252_sessions,      cut: gt_zero}
    - {id: kosdaq_rel_up,  role: exploratory, source: [market_kosdaq_ret_1d, market_kospi_ret_1d],     transform: sum20_diff,                     cut: gt_zero}
    - {id: krw_weak_20d,   role: exploratory, source: [fx_usdkrw_level],                               transform: log_ratio_20_sessions,          cut: gt_zero}
  pairs:
    # role primary → enters BH. direction: "+", "-", or null (양방향, sign fixed after first observation).
    - {id: P1,  role: primary,   family: px_idio_vol_60d,                  cell: {scan_type: cum, h_start: 0, h_end: 60}, regime: vix_up,    direction: "+"}
    - {id: P2,  role: primary,   family: px_maxret_20d,                    cell: {scan_type: cum, h_start: 0, h_end: 60}, regime: vix_up,    direction: "+"}
    - {id: P3,  role: primary,   family: px_reversal_5d,                   cell: {scan_type: cum, h_start: 0, h_end: 5},  regime: vix_high,  direction: "+"}
    - {id: P4,  role: primary,   family: px_mom_12_1,                      cell: {scan_type: cum, h_start: 0, h_end: 60}, regime: market_up, direction: "+"}
    - {id: P5,  role: primary,   family: px_mom_12_1,                      cell: {scan_type: cum, h_start: 0, h_end: 60}, regime: liq_high,  direction: "+"}
    - {id: P6,  role: primary,   family: px_amihud_20d,                    cell: {scan_type: cum, h_start: 0, h_end: 60}, regime: liq_high,  direction: "-"}
    - {id: P7,  role: primary,   family: flow_foreign_netbuy_to_volume,    cell: {scan_type: cum, h_start: 0, h_end: 20}, regime: market_up, direction: "+"}
    - {id: P8,  role: primary,   family: flow_individual_netbuy_to_volume, cell: {scan_type: cum, h_start: 0, h_end: 20}, regime: market_up, direction: "-"}
    - {id: P9,  role: primary,   family: px_turnover_shock,                cell: {scan_type: cum, h_start: 0, h_end: 20}, regime: liq_high,  direction: null}
    - {id: P10, role: primary,   family: flow_inst_netbuy_to_volume,       cell: {scan_type: cum, h_start: 0, h_end: 20}, regime: market_up, direction: null}
    - {id: P11, role: primary,   family: flow_foreign_netbuy_to_volume,    cell: {scan_type: cum, h_start: 0, h_end: 20}, regime: vix_up,    direction: null}
    - {id: P12, role: primary,   family: flow_foreign_netbuy_to_volume,    cell: {scan_type: cum, h_start: 0, h_end: 20}, regime: liq_high,  direction: null}
    - {id: P13, role: primary,   family: flow_inst_netbuy_to_volume,       cell: {scan_type: cum, h_start: 0, h_end: 20}, regime: liq_high,  direction: null}
    - {id: P14, role: primary,   family: flow_individual_netbuy_to_volume, cell: {scan_type: cum, h_start: 0, h_end: 20}, regime: liq_high,  direction: null}
    - {id: P15, role: primary,   family: px_market_beta,                   cell: {scan_type: cum, h_start: 0, h_end: 20}, regime: market_up, direction: null}
    - {id: X1,  role: reference, family: fin_log_mcap,                     cell: {scan_type: cum, h_start: 0, h_end: 60}, regime: market_up, direction: null}
    - {id: X2,  role: reference, family: fin_log_mcap,                     cell: {scan_type: cum, h_start: 0, h_end: 60}, regime: kosdaq_rel_up, direction: null}
  exploratory_grid:
    regimes: [term_steep]
    families: all_primary_pair_families
    extra:
      - {family: px_reversal_5d,    cell: {scan_type: cum, h_start: 0, h_end: 5},  regime: vix_up}
      - {family: px_idio_vol_60d,   cell: {scan_type: cum, h_start: 0, h_end: 60}, regime: vix_high}
      - {family: px_maxret_20d,     cell: {scan_type: cum, h_start: 0, h_end: 60}, regime: vix_high}
      - {family: macro_beta_usdkrw, cell: {scan_type: cum, h_start: 0, h_end: 20}, regime: krw_weak_20d}
      - {family: macro_beta_vix,    cell: {scan_type: cum, h_start: 0, h_end: 20}, regime: vix_up}
      - {family: px_amihud_20d,     cell: {scan_type: cum, h_start: 0, h_end: 60}, regime: vix_up}
    diagnostics: [market_up_24m_variant_for_P1_P2_P4, continuous_z_regression_all_pairs]
  stats:
    bh_q: 0.10
    hac_lag_policy: h_end_minus_1
    hac_gap_policy: calendar_session_distance
    p_value_distribution: asymptotic_normal
    min_dates_per_regime: 250
    min_share_per_regime: 0.20
    subperiod_min_dates_per_regime: 40
    subperiod_sign_rule: ceil_half_of_valid
    tradable_min_abs_delta_retention: 0.50
  placebo:
    regime_circular_shift_repeats: 100
    min_shift_sessions: 120
    p_max: 0.10
    seed: 20260829
  evidence_grade:
    A: screen_pass_and_no_warning
    B: screen_pass_with_nonfatal_warning
    C: exploratory_or_insufficient_occupancy
    D: no_discovery_or_gate_fail
    R: reference_only

families_append:
  - family: macro_beta_usdkrw
    phase: B
    fdr_family: macro_exposure
    role: phase_b_blocked
    expected_sign: null
    features:
      - {column: macro_beta_usdkrw, role: primary}
      - {column: macro_rawbeta_usdkrw, role: secondary}
      - {column: macro_semibeta_usdkrw_up, role: secondary}
    primary_horizon_set: [20, 60]
    exploratory_horizon_set: [1, 2, 3, 5, 10, 40, 120]
    include_bucket_primary: true
    fdr_include: false
    readiness_dependencies: [feat_macro_exposure, common_feature_daily_fact, label_scan]
    official_feature_variant: native_t
    variant_columns: {native_t: macro_beta_usdkrw, lag1: macro_beta_usdkrw_lag1}

  - family: macro_beta_wti
    phase: B
    fdr_family: macro_exposure
    role: phase_b_blocked
    expected_sign: null
    features:
      - {column: macro_beta_wti, role: primary}
      - {column: macro_rawbeta_wti, role: secondary}
    primary_horizon_set: [20, 60]
    exploratory_horizon_set: [1, 2, 3, 5, 10, 40, 120]
    include_bucket_primary: true
    fdr_include: false
    readiness_dependencies: [feat_macro_exposure, common_feature_daily_fact, label_scan]
    official_feature_variant: native_t
    variant_columns: {native_t: macro_beta_wti, lag1: macro_beta_wti_lag1}

  - family: macro_beta_kr10y
    phase: B
    fdr_family: macro_exposure
    role: phase_b_blocked
    expected_sign: null
    features:
      - {column: macro_beta_kr10y, role: primary}
      - {column: macro_rawbeta_kr10y, role: secondary}
    primary_horizon_set: [20, 60]
    exploratory_horizon_set: [1, 2, 3, 5, 10, 40, 120]
    include_bucket_primary: true
    fdr_include: false
    readiness_dependencies: [feat_macro_exposure, common_feature_daily_fact, label_scan]
    official_feature_variant: native_t
    variant_columns: {native_t: macro_beta_kr10y, lag1: macro_beta_kr10y_lag1}

  - family: macro_beta_sp500_lag
    phase: B
    fdr_family: macro_exposure
    role: phase_b_blocked
    expected_sign: null
    features:
      - {column: macro_beta_sp500_lag, role: primary}
      - {column: macro_rawbeta_sp500_lag, role: secondary}
    primary_horizon_set: [20, 60]
    exploratory_horizon_set: [1, 2, 3, 5, 10, 40, 120]
    include_bucket_primary: true
    fdr_include: false
    readiness_dependencies: [feat_macro_exposure, common_feature_daily_fact, label_scan]
    official_feature_variant: native_t
    variant_columns: {native_t: macro_beta_sp500_lag, lag1: macro_beta_sp500_lag_lag1}

  - family: macro_beta_vix
    phase: B
    fdr_family: macro_exposure
    role: phase_b_blocked
    expected_sign: "-"
    features:
      - {column: macro_beta_vix, role: primary}
      - {column: macro_rawbeta_vix, role: secondary}
    primary_horizon_set: [20, 60]
    exploratory_horizon_set: [1, 2, 3, 5, 10, 40, 120]
    include_bucket_primary: true
    fdr_include: false
    readiness_dependencies: [feat_macro_exposure, common_feature_daily_fact, label_scan]
    official_feature_variant: native_t
    variant_columns: {native_t: macro_beta_vix, lag1: macro_beta_vix_lag1}

  - family: px_market_beta
    phase: B
    fdr_family: macro_exposure
    role: phase_b_blocked
    expected_sign: null
    features: [{column: px_market_beta, role: primary}]
    primary_horizon_set: [20, 60]
    exploratory_horizon_set: [1, 2, 3, 5, 10, 40, 120]
    include_bucket_primary: true
    fdr_include: false
    readiness_dependencies: [feat_macro_exposure, common_feature_daily_fact, label_scan]
    official_feature_variant: native_t
    variant_columns: {native_t: px_market_beta, lag1: px_market_beta_lag1}
```

**validate_config에 더한 검사** (`horizon_scan_config.py`, PR-1a-3 — **구현 완료**): `_validate_phase_c`가
`phase_c.pairs[*].family`가 registry에 있고 `cell.h_end`가 그 family의 `primary_horizon_set`에 들어 있음;
`cell.scan_type == cum`; `pairs[*].regime ∈ regimes[*].id`; `exploratory_grid.extra[*]`도 같은 검사;
`role: primary` 쌍이 정확히 15, `reference` 2, 그 밖의 role 금지; pair id·regime id 유일; `direction ∈ {+, −, null}`;
`sample_start`가 date; `placebo.seed` 정수; `regime_candidates`(N8)와 `regimes`가 겹치지 않음. `_validate_registered_at`이
`preregistration.registered_at`을 date로 강제한다.

**`pairs`가 없는 층은 이 검사를 건너뛴다.** base config와 expansion overlay도 `phase_c` 블록을 갖고 있지만
open policy와 dormant N8 후보뿐이라 조건부 IC 계약이 아니다. 두 층의 hash가 움직이지 않는 것을 테스트로 고정했다.

기존 검사(Phase A 17, primary 75, Phase B cell == `primary_candidate_count_max` 102)도 그대로 통과한다 — 실측 확인.

**hash 규칙은 바꾸지 않는다.** `_canonical_hash`는 merge된 전체 계약을 그대로 해시한다(`12` 보존 규칙). Phase C가 읽을
A·B run_id는 CLI 인자다(`03` §7.1) — config에 없으므로 제외 규칙이 필요 없다.

**초안 검증(2026-08-29, 리뷰 반영 후 재확인).** 위 YAML을 `extends` 절대경로로 바꿔 현재 `load_config`로 로드했다.
family 41(A 17, B 24), Phase B cell 102, Phase A primary 75, `phase_c.pairs` primary 15·reference 2, 모든 쌍의
`h_end`가 해당 family `primary_horizon_set` 안, `pairs[*].regime`이 전부 `regimes[*].id` 안, N8 `regime_candidates` 상속.
`phase_c` 전용 검사는 아직 없으므로 PR-1a-3에서 추가한다.

---

## 2. hash 기록

overlay를 커밋한 뒤 `uv run python -c "from research.analysis.horizon_scan_config import load_config;
print(load_config('research/analysis/horizon_scan_macro_20260829.yaml').config_hash)"` 결과를
`05_preregistration_record.md`에 적는다 — `12_expansion_preregistration_20260827.md`와 같은 형식(config 경로, hash,
base hash, 등록 시점 "label·IC·p-value 계산 전", 결론 요약, 보존 규칙). 같은 파일에 **§3 3a의 국면 점유율·G2 유효 구간
사전 계산 결과**도 함께 적는다. 그 뒤 실행 결과는 같은 파일 하단에 "실행 결과" 절로만 덧붙인다.

---

## 3. 실행 순서

한 lineage로 간다. Phase A도 overlay hash로 다시 돌린다(`12`가 그렇게 했다). 단계 0 검증은 그 Phase A run을
canonical A와 비교하는 것으로 한다(`01` §4.2) — Phase A family가 overlay에서 바뀌지 않으므로 별도 재실행 없이
행동 불변을 증명할 수 있다.

| # | 일 | 산출 | 확인 |
|---|---|---|---|
| 1 | ~~snapshot `2026-08-23`에 `common_feature_daily_fact` 빌드~~ **완료 2026-08-29** | `derived_mart/snapshot_date=2026-08-23/source=sj2_remote/common_feature_daily_fact` (708K, 129,322행, active feature 38) | ✅ 일별 계열 2014-06-16·월별 매크로 2013-06-20으로 `00_survey/00` §2.2와 일치. **`commodity_wti_spot_level` 3,139행 NULL 0개**(WTI 계열 중 유일하게 warm-up NULL 없음), readiness coverage 1.0 / PIT 위반 0. 격자는 2014~2023 평일 그대로(M3) |
| 2 | ~~`feat_macro_exposure` 빌드~~ **완료 2026-08-29** | `feature_mart/snapshot_date=2026-08-23/source=sj2_remote/feat_macro_exposure` (1.0G, 7,024,118행, 14.4초) | ✅ 커버리지 표 `02` §2.5b. primary 시작일 2014-12-17(국내)·2014-12-22(해외), `px_market_beta` 2007-12-07. grain 유일, panel join 안전. `analysis_config_hash`는 디스크의 A0 계보(`889c3e83`)에 맞춤 |
| 3a | ~~**국면 시계열 사전 계산**~~ **완료 2026-08-29** — `research/analysis/horizon_scan_phase_c_regimes.py` | `research/output/horizon_scan/phase_c_regimes/` (`regime_series.parquet`, occupancy/persistence/subperiods, `regime_summary.md`) | ✅ **G1 7개 전부 통과, G2 7×5 = 35구간 전부 유효.** `market_up`의 `2023_11_common_end`가 254/52로 가장 빠듯하지만 40을 넘겼다 — §3.2 |
| 3b | ~~`registered_at` 확정 → overlay 커밋 → hash → `05_preregistration_record.md`~~ **완료 2026-08-29** | `horizon_scan_macro_20260829.yaml`, `05_preregistration_record.md` | ✅ hash `236d0d35…`. family 41(A 17·B 24), Phase B cell 102, Phase A primary 75, 쌍 primary 15·reference 2. base 두 hash 불변. **이 시점 이후 §5 규율** |
| 4 | ~~A0 재빌드 → Phase A~~ **완료 2026-08-30** | A `20260830T085718-efd35e70` | ✅ **canonical A와 exact match** — `horizon_ic` 412행×40컬럼 max\|Δ\|=1.388e-17, `bh_pass` 57·discovery 32·등급 A6·C4·D6·R1 전부 동일. `daily_ic_reconciled: true`, 차이 0.0. 66분 |
| 5 | ~~Phase B (overlay) + secondary 진단~~ **완료 2026-08-30** | B `20260830T100518-efd35e70` | ✅ 102 cell 전부 `ready_primary`·valid, 매크로 6 family 24 cell 포함. `daily_ic_reconciled: true`, 차이 0.0. 2시간 20분. **선행: Phase B 마트 9개도 overlay hash로 재빌드 필요** (§3.1) |
| 6 | ~~AB (overlay)~~ **완료 2026-08-30** | AB `20260830T122850-efd35e70` | ✅ 177 가설, discovery 103, `screen_pass` 53, B-cell A23·B30·C40·D9. **기존 Phase A discovery 변화 0개**, 공통 153 가설 등급 변화 0개. permutation p=0.0099 |
| 7 | ~~Phase C~~ **완료 2026-08-30** | C `20260830T122850-phasec` | ✅ primary 15쌍 전부 유효, **discovery 4·`screen_pass` 4**(P3·P9·P12·P15), 등급 A4·D11·R2. G4가 BH와 15쌍 전부 일치. 9초 |
| 8 | ~~legacy Phase B → `daily_ic` parity~~ **완료 2026-08-30** | legacy B `20260830T041554-db50d0ff` | ⚠️ `daily_spread` 완전 일치, `daily_ic` 공통 868,400행 max\|Δ\|=2.776e-16 통과. 단 **행 집합이 474행 다르다** — native 엔진의 상수 횡단면 결함(I13), 단계 0과 무관. 4시간 20분 |
| 9 | ~~결과 문서~~ **완료 2026-08-30** | `05_results_stage1a_20260830.md`, `05_results_stage1b_20260830.md`, `05_preregistration_record.md` §실행 결과, `00_status.md`, `10_known_issues.md` I13 | |

4~6은 `12`의 확장 run과 같은 비용이다(A는 permutation 100회 포함). 어느 parity 범위를 택했는지 결과 문서에 적는다.

### 3.1 실행 1·2 기록 (2026-08-29)

**경로.** `bin/parquet-compute-all.sh`가 아니라 `uv run python -m research.etl.compute_all
--snapshot-date 2026-08-23 --source sj2_remote --from-step marts`를 직접 돌렸다. 래퍼는 `db sync-remote`와
raw export부터 시작해 **오늘 날짜의 새 snapshot**을 만드는데, 이 계약은 2026-08-23에 고정돼 있다.
raw lake와 A0 마트는 그 snapshot에 이미 있었다.

**readiness 게이트는 exit 1로 끝난다 — 기존 상태다.** `required_coverage_ratio=1.0`에서 38개 중 18개가
미달인데, 전부 자기 이력 앞머리의 warm-up NULL(`ret_20d`·`yoy`·`mom`)이거나 한쪽 다리가 없는 스프레드
(`rate_*_term_spread_*`, 6,874일 중 3,139일만)다. **새 feature를 넣은 경우와 뺀 경우의 미달 목록이
정확히 같음을 확인했다** — 18개로 동일, 차집합 없음. `commodity_wti_spot_level` 자신은 ready다.
따라서 이 exit 1은 이번 작업이 만든 것이 아니고, 이후 단계를 막지 않는다.

**`analysis_config_hash` 주의.** 2026-08-23의 A0 마트는 전부 확장 config 해시(`889c3e83…`)로 캐시돼 있고,
`mart.materialize`는 해시가 다르면 **에러를 낸다**(조용히 다시 만들지 않는다). 그래서
`compute_all --features`는 이 snapshot에서 그대로 쓸 수 없다 — `analysis_config_hash=None`으로
A0 마트 캐시와 충돌한다. 실행 2는 `feat_macro_exposure`만 A0와 같은 해시로 materialize 했다.
**새 사전등록 층으로 run하려면 그 층의 해시로 마트를 다시 만들어야 한다.**

**여기에 두 단계가 있다 — 처음엔 절반만 적었다가 실행 5에서 걸렸다.**

1. **A0 7개**: `uv run python -m research.etl.horizon_scan_inputs --source sj2_remote --force --config <overlay>`.
   **`--snapshot-date`를 주지 말 것** — 주면 `resolve_config`가 `auto_selected=False`로 표시하고
   manifest가 `official=false`·`smoke_only=true`가 되어 Phase A preflight가 거부한다.
   auto-select도 같은 2026-08-23을 고른다. 실측 3분 18초.
2. **Phase B 마트 9개**(`feat_market_cap`·`feat_filing_activity`·`feat_periodic_extras`·
   `stock_metric_vintage_fact`·`fin_quarterly_metric_vintage`·`feat_fin_scan_daily`·
   `feat_event_scan_daily`·`fin_sue_event`·`feat_macro_exposure`): `horizon_scan_inputs`는 이것들을
   건드리지 않는다. `register_phase_b_marts(con, lake, force=True)`로 따로 돌린다. 실측 31분.
   빼먹으면 Phase B가 `RuntimeError: mart cache contract mismatch for 'feat_market_cap'`으로
   1.5초 만에 죽는다 — `_try`가 `duckdb.Error`/`FileNotFoundError`만 잡기 때문이고, 그게 맞는 동작이다.

**A0 재빌드는 실질적으로 장부 정리다.** overlay hash로 다시 만든 7개 마트의 행 수·schema hash가
`889c3e83` 것과 전부 같았다(실측). A0에 들어가는 config 섹션(`quality`·`universe`·`sample`·
`horizons`·`buckets`)이 두 층에서 동일하기 때문이다.

**부수 확인.** `feat_price`의 저장된 `sql_hash`가 `110617482d64…`로, PR-1a-2가 시장모형 CTE를
`trading_panel.build_market_model_sql`로 뽑아낸 뒤에도 그대로다. A0 마트 캐시가 무효화되지 않았다는
디스크 상의 증거다.

### 3.2 실행 3a 기록 — 국면 사전 계산 (2026-08-29)

```
uv run python -m research.analysis.horizon_scan_phase_c_regimes \
    --snapshot-date 2026-08-23 --source sj2_remote
```

**판정 창은 `2015-06-16 ~ 2025-02-05`(2,368 세션)이다.** 15쌍이 전부 `common_survivor` cell이라 daily IC가
`common_formation_end`(2025-02-05)에서 끝난다 — 그 뒤 세션은 Phase C가 조건화하지 않으므로 점유율에서 뺐다.
`regime_series.parquet`에는 2026-08-21까지(2,745 세션) 그대로 저장한다. 자르는 것은 읽는 쪽의 결정이지
계열의 성질이 아니다.

### G1 국면 점유율

| regime | role | 세션 | s=1 | s=0 | 점유율 | 시작 | 끝 | G1 |
|---|---|---|---|---|---|---|---|---|
| `vix_up` | primary | 2,368 | 1,107 | 1,261 | 0.467 | 2015-06-16 | 2025-02-05 | 통과 |
| `vix_high` | primary | 2,368 | 1,097 | 1,271 | 0.463 | 2015-06-16 | 2025-02-05 | 통과 |
| `market_up` | primary | 2,362 | 1,317 | 1,045 | 0.558 | 2015-06-24 | 2025-02-05 | 통과 |
| `liq_high` | primary | 2,368 | 1,040 | 1,328 | 0.439 | 2015-06-16 | 2025-02-05 | 통과 |
| `term_steep` | exploratory | 2,368 | 1,109 | 1,259 | 0.468 | 2015-06-16 | 2025-02-05 | 통과 |
| `kosdaq_rel_up` | exploratory | 2,368 | 1,143 | 1,225 | 0.483 | 2015-06-16 | 2025-02-05 | 통과 |
| `krw_weak_20d` | exploratory | 2,368 | 1,324 | 1,044 | 0.559 | 2015-06-16 | 2025-02-05 | 통과 |

### 국면 지속 (§6.5)

| regime | 전환 횟수 | 평균 지속 s=1 | 평균 지속 s=0 |
|---|---|---|---|
| `vix_up` | 329 | 6.7 | 7.6 |
| `vix_high` | 170 | 12.8 | 15.0 |
| `market_up` | 64 | 39.9 | 32.7 |
| `liq_high` | 39 | 52.0 | 66.4 |
| `term_steep` | 100 | 21.7 | 25.2 |
| `kosdaq_rel_up` | 246 | 9.2 | 10.0 |
| `krw_weak_20d` | 225 | 11.7 | 9.2 |

### G2 구간별 유효 여부 (양쪽 ≥ 40일)

| regime | 2014_2016 | 2017_2019 | 2020_2021 | 2022_2023_10 | 2023_11_common_end | 유효 구간 |
|---|---|---|---|---|---|---|
| `vix_up` | 184/199 ✅ | 320/413 ✅ | 237/259 ✅ | 213/237 ✅ | 153/153 ✅ | **5/5** |
| `vix_high` | 182/201 ✅ | 342/391 ✅ | 225/271 ✅ | 205/245 ✅ | 143/163 ✅ | **5/5** |
| `market_up` | 154/223 ✅ | 393/340 ✅ | 422/74 ✅ | 94/356 ✅ | 254/52 ✅ | **5/5** |
| `liq_high` | 138/245 ✅ | 329/404 ✅ | 314/182 ✅ | 160/290 ✅ | 99/207 ✅ | **5/5** |
| `term_steep` | 106/277 ✅ | 293/440 ✅ | 365/131 ✅ | 123/327 ✅ | 222/84 ✅ | **5/5** |
| `kosdaq_rel_up` | 171/212 ✅ | 347/386 ✅ | 281/215 ✅ | 219/231 ✅ | 125/181 ✅ | **5/5** |
| `krw_weak_20d` | 241/142 ✅ | 361/372 ✅ | 249/247 ✅ | 285/165 ✅ | 188/118 ✅ | **5/5** |

**읽는 법 셋.**

- **G1은 7개 전부 통과한다.** 점유율 0.44~0.56으로 어느 국면도 한쪽으로 쏠려 있지 않다. G1 `insufficient`로
  빠지는 쌍은 없을 전망이고, P15(`px_market_beta`)만 1a 산출 여부로 갈린다.
- **G2도 35구간 전부 유효하다.** 설계가 걱정한 `market_up`의 `2023_11_common_end` 구간은 **254/52**로,
  40 문턱을 12일 차이로 넘겼다. 표 전체에서 가장 빠듯한 칸이므로 결과 해석 때 다시 볼 값이다.
  `2020_2021`의 `market_up` 422/74와 `2022_2023_10`의 94/356도 한쪽으로 크게 기울어 있다 — 통과하지만
  그 구간의 `δ` 추정은 적은 쪽 표본이 좌우한다.
- **지속이 G4를 필수로 만든다.** `liq_high`는 평균 52~66세션 지속에 전환 39회뿐이다. HAC lag는 `h_end−1`
  (P5·P6은 59)이라 60세션짜리 국면 블록을 잡지 못한다(§5.1). `market_up`도 33~40세션이다. 반대로
  `vix_up`은 6~8세션이라 HAC 안에 들어온다.

리뷰가 fact 격자로 잰 참고값과 대조하면 같은 자릿수다 — `vix_up` 전환 328→329, `market_up` 86→64,
`liq_high` 40→39, 평균 지속 8/29/63 → 6.7/39.9/52.0. 차이는 격자(평일 → 세션)와 표본 창 때문이다.

---

## 4. PR 분할

**전부 완료 (2026-08-29). 남은 것은 §3의 실행 4~9다.**

| PR | 내용 | 파일 | 행동 변화 |
|---|---|---|---|
| **PR-0** ✅ | `DailyIcSink`, `scan_cell`/`run_registry_scan` kwarg, `cell_identity` 정규화, `daily_spread` 분리, reconcile, manifest, 테스트 | `horizon_scan_daily_ic.py`(신규), `horizon_scan_runner.py`, `horizon_scan.py`, `horizon_scan_phase_b_run.py`, `horizon_scan_phase_b_scan.py`, `horizon_scan_run_spec.py`, tests | **없음** (sink 미지정 시 동일) |
| **PR-1a-1** ✅ | `commodity_wti_spot_level` 카탈로그 + golden 갱신 | `definitions/common_features.py`, `tests/unit/golden/common_feature_daily_fact.json`, `test_default_common_feature_catalog.py`, `test_common_build_mart.py` | fact에 행 추가만. 기존 행 diff 0 |
| **PR-1a-2** ✅ | `feat_macro_exposure` 마트(국내/해외 요인 짝짓기·창 A/B) + `compute_all` 등록 + Phase B run의 `common_feature_daily_fact` view 등록(재계산 vs parquet 읽기 결정) + readiness + secondary 진단 스캔 경로 | `research/etl/features/macro_exposure.py`(신규), `compute_all.py`, `horizon_scan_phase_b_run.py`, `horizon_scan_readiness.py`, `horizon_scan_phase_b_diagnostics.py`, tests | 새 마트·진단만 |
| **PR-1a-3** ✅ | overlay YAML(`registered_at` 확정) + `validate_config` phase_c 검사 + 국면 사전 계산 스크립트 + `05_preregistration_record.md` | `horizon_scan_macro_20260829.yaml`(신규), `horizon_scan_config.py`, `research/analysis/horizon_scan_phase_c_regimes.py`(국면 빌더, PR-1b와 공유), tests, docs | config만. **이 PR의 머지 커밋이 사전등록 시점** |
| **PR-1b** ✅ | `newey_west_ols`, `horizon_scan_phase_c.py`(CLI: `--phase-a-run-id`/`--phase-b-run-id`), 보고서, 테스트 | `metrics.py`, `horizon_scan_phase_c.py`(신규), `horizon_scan_phase_c_report.py`(신규), tests | 새 phase만 |
| PR-docs ✅ | `00_읽는_법` §4.2·§7(a)·§10, `08` §4.3 Stage 3, `00_status` §0·§8; `features/common.py` docstring 시작일(2025-12-15 → 2014-06-16/2013-06-20) | docs, 한 docstring | 없음 |

PR-0과 PR-1a-1/2는 서로 독립이라 병렬로 갈 수 있다. PR-1a-3은 PR-1a-2(마트 컬럼명 확정)와 국면 빌더 뒤, PR-1b는 PR-0 뒤다.

PR-0의 §4.3 parity는 `research/analysis/engine_parity_report.py`에 `OPTIONAL_ARTIFACTS`로 붙였다 —
Phase A/B의 `daily_ic`·`daily_spread` 넷. hive 트리를 한 프레임으로 읽어 기존 tolerance 기계에 그대로
태우고, 정렬 키에 `trade_date`를 더했다. **두 run 모두 없으면 건너뛴다**(Stage 0 이전 run도 비교할 수
있어야 한다). **한쪽에만 있으면 실패다** — 그것은 입력이 없는 것이 아니라 두 run이 실제로 다른 것이다.

---

## 5. 규율 — 사전등록 뒤에 바꿀 수 없는 것과 바꿀 수 있는 것

**바꿀 수 없다** (PR-1a-3 머지 이후):
- family 6개의 산식·짝짓기·창·최소 관측·기대 부호·horizon set. 양방향 family의 부호는 첫 결과의 관측 부호로 고정되고 이후 불변.
- Phase C의 국면 정의(격자 포함)·cut·15 primary 쌍·방향·cell·통계 규약·게이트 임계·placebo 파라미터·seed.
- BH 모집단(Phase B 102, 결합 177, Phase C 15).
- holdout 경계 2025-08-01. holdout은 feature·horizon·variant·Phase C 선택이 전부 끝난 뒤 한 번만 연다(`12` 보존 규칙).

**바꿀 수 있다** (기록을 남기고):
- 구현 버그 수정. 단, 수정 전후 canonical A 요약 exact match(§3-4)가 유지돼야 하고, 새 family/Phase C 수치가 바뀌면
  "버그 수정 전 값은 판정에 쓰지 않는다"고 결과 문서에 적는다.
- exploratory 목록·진단 추가 — 보고만 하고 판정하지 않으므로 허용. 단 primary로 승격은 다음 overlay에서만.
- Phase C 입력 run_id(CLI 인자). 다른 run으로 다시 돌리면 새 Phase C run_id가 생기고 둘 다 남는다.

**결과를 보고 하지 않는 것:**
- Phase C 쌍의 horizon을 바꾸거나 국면 cut을 옮기는 것. 다른 cut이 더 좋아 보이면 §6.2 진단에 적고 **다음** 사전등록 후보로 넘긴다.
- 방향 미고정 쌍에 사후적으로 문헌을 붙여 "예측했던 방향"으로 서술하는 것. 카드에는 `direction_preregistered: null`이 그대로 남는다.
- 실패한 family를 secondary 컬럼(`macro_rawbeta_*`, 세미베타)의 진단 결과로 대신 판정하는 것. secondary는 진단이다.
- 국내 요인의 짝을 매매기준율 산출 방식 추정에 따라 한 세션 더 옮기는 것(`02` §2.2). 실측이 생기면 다음 사전등록에서.

**재현:** 모든 run은 `run_spec`/`manifest`/`_SUCCESS`를 남기고, 결과 문서의 숫자는 그 run_id에서만 읽는다
(`00_읽는_법` §2와 같은 원칙).

---

## 6. 위험과 대응

| 위험 | 대응 |
|---|---|
| `daily_ic` 저장으로 run 시간·디스크 증가 | 412/384 cell × 약 2,500일 = 약 100만 행, 수십 MB. feature 단위 flush, zstd. 복제 루프 제외라 시간 증가는 본 스캔 I/O뿐 |
| `common_feature_daily_fact` 재빌드로 golden 파리티 실패 | PR-1a-1에서 기존 feature_code 행 diff 0을 먼저 확인. 행 추가만 허용 |
| fact 격자가 2014~2023 평일 | 1a 마트는 panel join으로 비세션 행 제거, Phase C는 세션 격자에서 국면 계산. fact는 재빌드하지 않음 |
| 국내 요인 한 세션 지연 | 짝 `(resid_ret_τ, g_τ)` + 창 B로 동시 exposure를 만들고, 매매기준율 산출 방식은 미검증 한계로 적어 둠 |
| 해외 계열 NULL(휴장)로 베타 창의 유효 짝이 126 미달 | `*_n` 컬럼으로 커버리지 진단. 미달이면 NULL — 조용히 0 채우지 않음. `f_sp500_lag`도 같은 규칙 |
| 베타가 규모·변동성의 대리변수 | `02` §4 상관 진단을 카드에 필수 표기. `\|ρ\| ≥ 0.7`이면 결합 BH에는 남되 카드에 "규모 축 중복" 경고 |
| 국면 지속성으로 Phase C `δ̂` 과장 | G4 국면 placebo 필수(HAC는 `liq_high` 평균 63세션을 lag 19로 못 잡는다), G1 점유율, 지속 진단 컬럼 |
| 국면·cell을 공유하는 쌍의 종속성 | `03` §8 해석 규칙 — 같은 국면의 통과 쌍을 독립 발견으로 세지 않음 |
| `2023_11_common_end` 구간에 `market_up = 0` 일수 부족 | 3a에서 미리 계산해 무효 구간이면 hash 전에 확정. G2 규칙은 유효 구간 기준 |
| 15쌍 중 1a family(P15)만 늦게 준비 | `insufficient`·p=1.0으로 모집단 유지. 1a 완료 후 같은 config로 Phase C를 다시 돌리면 P15만 채워진다 — run_id 둘 다 기록 |
| Phase A 요약이 canonical과 어긋남 | 그 자리에서 멈추고 원인 규명 전에는 5~8을 진행하지 않는다 |
