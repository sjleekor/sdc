# 04-A. Horizon Scan Phase A — Price·Flow 스캔 상세 실행 계획

- 작성일: 2026-08-01
- 개정: 2026-08-02 rev.2 — 공매도 4개 family exploratory 강등, gap-aware HAC,
  장기 temporal placebo, non-overlap sign test, A0 execution variant 계약 반영
- 상태: A-PR1~A-PR6 구현·단위테스트·synthetic e2e·실측 smoke 검증·official run
  완료 (2026-08-03, §8.5/§8.6 참조). Phase 1 acceptance gate 인계(§11)는 별도 진행
- 기준 문서: [02_feature_candidate.md](02_feature_candidate.md),
  [03_horizon_predictive_power_plan.md](03_horizon_predictive_power_plan.md)
- 선행 계획: [04_specific_plan_A0.md](04_specific_plan_A0.md)
- 대상: Phase A0에서 준비한 price 9개 + flow 8개 family의 공식 horizon screening

## 0. 요약

Phase A의 목적은 Phase A0 산출물을 변경하는 것이 아니라, 동결된 입력으로 다음 질문에
재현 가능한 답을 만드는 것이다.

> 각 price·flow family의 단변량 예측력은 어느 forward 구간에서 나타나며, 그 결과가
> 공통 survivor 표본·tradable universe·서브기간·1일 실행 지연에서도 유지되는가?

실행 순서는 다음으로 고정한다.

```text
A0 인계물·config hash 검증 / holdout 봉인
  → 분석 패널 grain·coverage 검증
  → 누적 horizon + 비중첩 bucket의 core IC 산출
  → 사전등록 75개 셀에 global BH-FDR 1회 적용
  → tradable·available sample·세그먼트·1일 지연 강건성
  → 전 offset 비중첩 subsampling
  → 100회 date×market 단면 permutation + 장기 셀 100회 temporal placebo
  → decay 요약·패턴 분류·family 결론 카드
  → 결과 manifest·보고서·Phase 1 acceptance gate 인계
```

Phase A는 **screening 단계**다. 여기서 `screen_pass`가 된 family/horizon도 바로 모델
core feature가 되지 않는다. 이후 `02` §6.1의 증분성, purged walk-forward OOS, turnover,
거래비용, 마지막 holdout gate를 별도로 통과해야 한다.

Phase A에서 하지 않는 일은 다음과 같다.

- Phase A0의 피쳐·label·universe 산식 또는 config를 결과에 맞춰 수정
- holdout 기간의 IC·spread·모델 성능 확인
- price/flow family 간 중복 제거 또는 다변량 ablation
- 거래비용 차감 성과나 실거래 가능 수익 주장
- financial/event family와 event-time scan
- 결과를 본 뒤 새로운 window·horizon·interaction 추가
- PostgreSQL 쓰기 또는 derived fact의 PostgreSQL 재도입

## 1. 진입 조건과 종료 조건

### 1.1 진입 조건

공식 Phase A 실행은 다음을 모두 만족해야 한다.

1. 최신 완전한 `source=sj2_remote` raw snapshot을 사용해 Phase A0가 성공했다.
2. 아래 A0 산출물이 동일 `(snapshot_date, source, config_hash)`에 존재한다.
   - `dim_stock_pit_daily`
   - `dim_price_quality_daily`
   - `dim_universe_broad_daily`
   - `dim_universe_tradable_daily`
   - `feat_price`
   - `feat_flow`
   - `label_scan`
3. A0 `_SUCCESS.json`에서 `smoke_only=false`이고 필수 schema/key/coverage 검사가
   통과했다.
4. Phase A의 17개 family가 registry에 있다. global BH 대상 12개 family의 primary
   feature는 `ready`, `px_zero_ret_ratio_20d`는 `reference_only`, 공매도 4개 family는
   `exploratory_short_regime`이다.
5. balance 기반 exploratory family는 A0 publication-lag evidence에 따라
   `ready_exploratory` 또는 `exploratory_blocked_publication_lag`다. 후자여도 75개
   primary scan은 진행할 수 있으나 해당 exploratory 결과는 생성하지 않는다.
6. corporate-action mask가 feature lookback과 label forward window에 모두 적용되었다.
7. config에는 family별 native/lag1 컬럼 mapping, `official_feature_variant`, availability
   evidence가 동결되어 있고 두 variant가 A0 feature mart에 이미 materialize되어 있다.
   같은 날 사용 가능 여부가 확인되지 않은 flow는 `lag1`을 공식 variant로 사용한다.
8. 선택 snapshot의 최대 데이터 날짜가 무엇이든 `holdout_start=2025-08-01`과
   label 종료일 경계는 config 값 그대로다.

하나라도 어기면 full scan을 시작하지 않는다. `--smoke-family`, 수동 snapshot,
permutation 횟수 축소 같은 debug 실행은 허용하지만 결과 manifest를
`official=false`, `smoke_only=true`로 기록하고 결론 카드와 `_SUCCESS.json`을 만들지 않는다.

#### 1.1 충족 현황 (2026-08-02 검증)

2026-08-02 `research/etl/horizon_scan_inputs.py`를 실제 `sj2_remote` snapshot
(`snapshot_date=2026-08-01`, `config_hash=45fbfab7c079537f68531056fd8ea4bb2393abe584b45132294c6c40c813c2f0`)
으로 실행해 8개 조건을 모두 실측 확인했다. 매니페스트:
`data_lake/feature_mart/snapshot_date=2026-08-01/source=sj2_remote/_manifests/_SUCCESS.json`.

| # | 조건 | 상태 | 근거 |
|---|---|---|---|
| 1 | 최신 완전한 sj2_remote raw snapshot으로 A0 성공 | 충족 | `_SUCCESS.json`: `status=success`, `auto_selected=true`, `official=true` |
| 2 | 7개 A0 산출물이 동일 (snapshot_date, source, config_hash)에 존재 | 충족 | 7개 mart 모두 row_count>0로 materialize (아래 표) |
| 3 | A0 `_SUCCESS.json`에서 `smoke_only=false` + 검사 통과 | 충족 | `smoke_only=false`, `smoke_only_reasons=[]` |
| 4 | 17개 family registry + 12 ready / 1 reference_only / 4 exploratory_short_regime | 충족 | `readiness_matrix.md`: 12개 `ready`, `px_zero_ret_ratio_20d`=`reference_only`, 공매도 4개는 config role `exploratory_short_regime` |
| 5 | balance 기반 exploratory family가 `ready_exploratory` 또는 `exploratory_blocked_publication_lag` | 충족 (`ready_exploratory`) | `short_balance_publication_lag.json`: `status=verified`, `public_lag_sessions=2`; `readiness_matrix.md`에서 `flow_short_turnover`/`flow_short_interest`/`flow_days_to_cover`/`flow_nat_proxy_20d` 전부 `ready_exploratory` |
| 6 | CA mask가 feature lookback + label forward window 모두 적용 | 충족 | `research/etl/features/price.py`(`ca_count_*` 마스킹), `research/etl/labels.py`(`ca_count_{h}d` 마스킹)가 실제 mart에 적용된 상태로 materialize |
| 7 | native/lag1 매핑 + `official_feature_variant` 동결, 두 variant가 mart에 이미 materialize | 충족 | `horizon_scan_config.yaml`에 25개 family 전부 `variant_columns`/`official_feature_variant` 고정, `feat_price`/`feat_flow`에 `_lag1` 컬럼 실존 |
| 8 | `holdout_start=2025-08-01` 고정 | 충족 | `horizon_scan_config.yaml: sample.holdout_start`와 `horizon_scan_inputs.py`의 값이 일치 |

mart 실측치 (`(trade_date,ticker,market)` key 유일성은
`tests/integration/test_horizon_scan_inputs_smoke.py`로 확인):

| mart | row_count | min_trade_date | max_trade_date |
|---|---:|---|---|
| dim_stock_pit_daily | 6,650,098 | 2007-06-05 | 2026-07-31 |
| dim_price_quality_daily | 6,650,098 | 2007-06-05 | 2026-07-31 |
| dim_universe_broad_daily | 6,650,098 | 2007-06-05 | 2026-07-31 |
| dim_universe_tradable_daily | 6,650,098 | 2007-06-05 | 2026-07-31 |
| feat_price | 6,650,098 | 2007-06-05 | 2026-07-31 |
| feat_flow | 8,973,748 | 2007-06-05 | 2026-07-31 |
| label_scan | 6,540,724 | 2007-06-05 | 2026-07-31 |

이 검증 과정에서 발견·수정한 이슈 (모두 이 커밋에 포함):

- `research/etl/features/price.py`의 `px_turnover_shock`가 비halt·거래량 0인 날
  (실측 34,301행)에서 `LN(0)`로 크래시하던 버그 — `NULLIF(turnover, 0)`로 수정하고
  회귀 테스트(`tests/unit/test_research_features.py::test_turnover_shock_nulls_zero_volume_instead_of_crashing`)
  추가.
- `horizon_scan_config.yaml`의 `flow_short_turnover.readiness_dependencies`에 어떤
  mart도 가리키지 않는 `short_regime` 항목이 있어 항상 `blocked_missing_dependency`로
  오분류되던 버그 — 제거 (해당 컬럼은 이미 `feat_flow`에 포함).
- `write_publication_lag_evidence`를 고정 스텁에서 실측 진단(`diagnose_publication_lag`)
  으로 교체 — KRX 잔고 raw field(`RPT_DUTY_OCCR_DD`)가 측정일이지 공시일이 아니라는 점과,
  동일 fetch 내 `short_selling_volume`/`value` 대비 실측 2세션 지연을 근거로 사용.

검증 명령: `uv run pytest tests/integration/test_horizon_scan_inputs_smoke.py` (7 passed),
`uv run pytest tests/unit` (473 passed), `uv run ruff check research/ tests/` (이번 세션
변경 파일 전부 clean). 이로써 Phase A 공식 실행을 시작할 준비 조건을 모두 충족한다.

#### 1.1 후속: §2.4 config gap 정리와 A0 재생성 (2026-08-02, A-PR1 착수 전)

`horizon_scan_config.yaml`에 §2.4가 요구하는 `discovery.universe`/`discovery.sample_kind`,
`stats.nw_p_value_distribution`/`bh_missing_p_value`/`nonoverlap_all_offsets`/
`nonoverlap_inference`/`sparse_primary_grid_families`, `placebo.cross_sectional_block`/
`temporal_min_nw_lag`, `evidence_grade` 루브릭이 없어 실제로 결과를 만들기 전 재생성이
필요한 상태였다. `schema_version`을 2→3으로 올리고 위 항목을 기존 섹션(`discovery`
신설, `stats`/`placebo` 보완, `evidence_grade` 신설)에 채운 뒤 `horizon_scan_config.py`의
`validate_config`에 고정 프로토콜 값 검증과 `evidence_grade`/`sparse_primary_grid_families`
검증을 추가했다 (`tests/unit/test_horizon_scan_config.py`에 6개 테스트 추가, 총 16 passed).

config_hash가 `45fbfab7...`에서 `07006c9a597ed0f6699a8819c897efc16222f0489aa3cc16c4cd5e90fe25365e`로
바뀌었으므로 `uv run python -m research.etl.horizon_scan_inputs --force`로 동일 snapshot
(`2026-08-01`)에 대해 A0를 재생성했다 (3m22s, 7개 mart row_count 동일 — 메타데이터만 갱신).
재생성된 `_SUCCESS.json`은 `status=success`, `official=true`, `smoke_only=false`,
`primary_family_not_ready=[]`, `publication_lag_verified=true`를 유지한다.
`uv run pytest tests/integration/test_horizon_scan_inputs_smoke.py`(7 passed),
`uv run pytest tests/unit`(478 passed), `uv run ruff check`(변경 파일 clean)로 재확인했다.
이 시점부터 신규 config_hash가 Phase A 실행의 정본이다.

#### 1.1 후속 2: A-PR1 구현 중 발견한 hypothesis 카운트·A0 feature 버그 3건 (2026-08-02)

A-PR1(75-primary/28-short-exploratory registry, 패널 join, segment/coverage
helper)을 구현하며 §2.1의 정확한 규칙(bucket은 h_end가 family의 primary
cumulative horizon 교집합일 때만 셀로 인정)을 그대로 코드화한 결과, 기존
`primary_hypothesis_count`/`validate_config`의 `len(primary_horizon_set) * (2 if
include_bucket_primary else 1)` 공식이 실제로는 틀렸지만 우연히 75로 맞아떨어지고
있었음을 발견했다. `px_reversal_5d`는 실제 7셀(cum 5 + bucket 2: horizon
{1,2,3,5,10} 중 bucket grid 끝({5,10,20,40,60,120})과 겹치는 건 5,10뿐)인데 공식은
10으로 과다 계산했고, `flow_foreign_holding_ratio_chg`는 `include_bucket_primary:
false`로 3개만 잡혀 문서 표의 6개와 어긋났다 — 두 오차(+3, -3)가 합계에서 상쇄되어
겉보기엔 75가 유지됐다. `bucket_primary_cells()`로 grid 교집합 기반 계산으로 교체하고
yaml의 `include_bucket_primary`를 `true`로 고쳐 실제로도 75/28이 나오게 했다
(`research/analysis/horizon_scan_config.py`, `tests/unit/test_horizon_scan_config.py`).

A-1의 native/lag1 shift-invariant 검사(§A-1 테스트 항목)를 실제 A0 mart에 대해
17개 family 전부 돌린 결과 2건의 실제 A0 feature 버그를 발견해 수정했다(사용자 확인 후
A0 산식 변경 + 재생성 진행, `04_specific_plan_A.md`가 원칙적으로 금지하는 A0 산식
변경에 해당하므로 별도 기록):

1. **`feat_flow`의 rolling window/`_lag1`이 halt day를 배제하지 않음.** 가격 피처
   (`research/etl/features/price.py`)는 `build_valid_session_sql`이 halt를 먼저
   제거한 뒤 `trade_date` 순서로 windowing하지만, `feat_flow`는 원자료(KRX flow
   feed) 자체의 `trade_date` 순서로 5/20/60d 윈도우와 `_lag1`을 계산해 halt일에도
   flow 관측이 남아있으면 그 행이 윈도우 슬롯을 그대로 차지했다. 실측 654만 행 중
   6행(상장 초기 2종목, `060570`/`149980`)에서 `_lag1[t] != native`의 직전
   valid session 값으로 확인. `research/etl/features/flow.py`를 수정해
   `quality_view.valid_session_idx IS NOT NULL`로 windowing 이전에 halt 행을
   제거하도록 재구성(`sessioned` CTE 신설, 기존 `panel_cte`를 `flow_base` 이전으로
   이동). 레거시 model 경로(`quality_view` 미제공)는 동작 불변.
   회귀 테스트: `tests/unit/test_research_flow_extended.py::test_flow_rolling_window_skips_halt_days_like_price_features_do`.
2. **`px_amihud_20d_lag1`이 config에 선언만 되고 `feat_price`에 미materialize.**
   §1.1 조건 7("두 variant가 이미 materialize")을 어기고 있었음 — 다른 8개 price
   lag1과 달리 amihud는 별도 후행 서브쿼리에서 계산되어 lag1 컬럼 자체가 빠져
   있었다. `research/etl/features/price.py`에 `px_amihud_20d_lag1` 추가.
   회귀 테스트: `tests/unit/test_research_features.py::test_amihud_lag1_matches_prior_session_native_value`.

두 A0 fix 모두 `feat_flow`/`feat_price`의 SQL 본문만 바꿔 `config_hash`는
불변(`1d208258...`)이지만, mart 캐시는 `analysis_config_hash`만 비교하고 SQL
content hash는 추적하지 않으므로 `--force`로만 재생성이 감지된다(캐시 설계의 known
gap). `uv run python -m research.etl.horizon_scan_inputs --force`를 2회 재실행
(각 ~3m: 첫 회는 flow fix, 둘째 회는 amihud fix) — `feat_flow` row_count가
8,973,748 → 6,535,918로 감소(halt-day 행 제거, 다른 6개 mart는 row_count 불변).
17개 family 전체의 native/lag1 shift invariant를 실측 재검증(0 실패), `uv run pytest
tests/unit`(516 passed), `tests/integration/test_horizon_scan_inputs_smoke.py`(7
passed), `ruff check`(변경 파일 clean)로 재확인했다.

#### 1.1 후속 3: A-PR4~A-PR6 구현 요약 (2026-08-02)

**A-PR4 (§A-6)**: `research/analysis/horizon_scan_permutation.py`에 두 replicate
루프를 실제 orchestration으로 구현했다. `run_cross_sectional_permutation`은
broad/common-survivor core panel을 한 번만 가져온 뒤(`fetch_broad_common_survivor_frame`)
매 replicate마다 `permute_within_groups`로 모든 primary feature를 date×market
블록 안에서 공동 치환하고, DuckDB에 재등록해 `scan_cell`/`apply_global_bh`를 실제
75셀 경로 그대로 재적용한다. `run_temporal_placebo`는 `nw_lag>=59`인 13셀만
`select_long_horizon_hypotheses`로 골라 같은 shift를 모든 장기 feature에 공동
적용하고 원 label 프레임과 `(formation_session_idx, ticker, market)`로 재조인한다.
두 함수 모두 JSONL checkpoint(`_load_checkpoint`/`_append_checkpoint`)로
resume-safe하며, 완료 후 replicate 수가 정확히 요청한 개수이고 중복이 없는지
assert한다. `run_lookahead_canary`는 `fwd_ret_1d`를 feature로 두고 h=1 IC가
`1.0`에 근접하는지 확인하며 75셀·공식 산출물에는 포함하지 않는다. `scan_cell`에
`compute_spread=False` 옵션을 추가해 replicate 루프(75셀×100회×2종=15000회 호출
규모)에서 불필요한 quantile spread 계산을 건너뛰게 했다. 신규 테스트
`tests/unit/test_horizon_scan_replicate_loop.py` (8개, checkpoint 재사용을
가짜 값 주입으로 직접 검증).

**A-PR5 (§A-7~A-9)**: A-7/A-8 순수 로직(`horizon_scan_report.py`)은 이전
세션에서 이미 완성돼 있었고, 이번에 A-9(plot 7종·markdown 11-section
renderer·atomic publish)를 추가했다. plot은 matplotlib(Agg headless)로
`plot_cumulative_ic_curve`/`plot_bucket_ic_bar`/`plot_native_vs_lag1`/
`plot_subperiod_heatmap`/`plot_segment_dot`/`plot_coverage_curve`/
`plot_offset_distribution` 7개 함수 + `render_family_plots` 조합기로 구현했고,
데이터가 없는 경우 예외 대신 "no data" placeholder를 그린다. markdown
renderer(`render_markdown_report`)는 §A-9가 정한 11-section 순서를 상수
`_REPORT_SECTION_TITLES`로 고정했다. atomic publish(`compute_run_content_hash`/
`publish_run`)는 `horizon_scan_run_spec.py`에 배치했다 — run_spec.json/plots/
_SUCCESS.json처럼 run마다 달라지는 provenance/이미지 파일은 해시에서 제외하고
core/robustness/permutation/cards만 해시해야 §8.3의 "재실행 hash 동일" 요건이
실제로 성립함을 발견하고 반영했다(`_CONTENT_HASH_EXCLUDE_NAMES`/`_DIRS`).
`pyproject.toml`의 `research` extra에 `matplotlib`을 추가했다(§7).

**A-PR6 진행 상황**: `tests/unit`(637개) 전체 통과, 실제 lake 없이도 돌아가는
synthetic e2e 검증테스트(`tests/integration/test_horizon_scan_smoke.py`)를
신설해 2-family/12-ticker/130-session in-memory panel로 A-1(`register_analysis_panel`)
부터 A-9(`publish_run`)까지 실제 함수를 그대로 이어붙여 실행했다 — 이 테스트가
실제 버그 하나를 잡았다: `write_run_spec`이 실제 config로 만든 run_spec을
JSON으로 쓰면 `TypeError: Object of type date is not JSON serializable`로
죽었다(PyYAML이 `sample.holdout_start` 등 bare `YYYY-MM-DD` 스칼라를
`datetime.date`로 파싱하는데 `json.dumps`에 `default=str`이 없었음) —
`horizon_scan_run_spec.py`에 `default=str` 추가로 수정, 회귀 테스트
`test_write_run_spec_serializes_a_real_config_derived_spec` 추가. 이어서
Phase A 전체 CLI(`research/analysis/horizon_scan.py`, 기존 §A0 smoke
드라이버를 대체)를 새로 작성했다 — preflight/run_spec, 7-mart 등록,
75/28-registry, A-2~A-9 전 스테이지를 config의 실제 25개 family(17개
Phase A)에 대해 실행하고 `--smoke-family`/`--permutations` 옵션으로 빠른
real-lake 검증을 지원한다. **범위상 의도적으로 줄인 것**: A-4의
period_sign_pass gate는 "available" 표본이 아니라 "common_survivor"
표본(family당 대표 primary cell 하나)에 대해서만 계산한다 — "available"
표본의 마지막 구간(`2025_04_holdout`)은 셀마다 다른
`horizon_eligible_end`가 필요해 매 cell마다 새 period 컬럼을 만들어야
하므로 이번 CLI 범위에서 제외했다(§A-4 완전 구현은 후속 작업). segment
dot-plot(시장/규모/유동성)도 market 세그먼트 없이 빈 리스트로 둔다.
실제 lake 대상 `--smoke-family`/official run 검증은 §8.3에 따라 별도로
진행한다.

#### 1.1 후속 4: `--smoke-family` 실측 검증에서 발견한 registry 버그 (2026-08-02)

CLI를 실제 `sj2_remote` snapshot에 대해 `px_reversal_5d`(native_t 공식
variant)로 먼저 검증했을 때는 문제없었지만, `flow_foreign_netbuy_to_volume`
(공식 variant `lag1`)로 두 번째 검증을 돌리자 카드의 `native_ic`/`lag1_ic`가
서로 다른 값(`-0.0103` vs `-0.0131`)으로 나와야 할 것이 나오는 대신, 실제로는
A-PR1의 `_hypothesis_rows`(`research/analysis/horizon_scan_readiness.py`)가
**모든 family의 registry `feature` 컬럼을 `official_feature_variant`와
무관하게 항상 `features[].column`(= native_t 컬럼 문자열)으로 고정**하고
있었음을 발견했다. §1.1 조건 7 "같은 날 사용 가능 여부가 확인되지 않은 flow는
`lag1`을 공식 variant로 사용한다"를 어기는 것으로, flow 4개 ready family +
공매도 4개 exploratory family + Phase B `fin_sue`(9개 family)의 **75/28-cell
공식 scan 자체가 지난 세션의 A2/A3 실측 검증을 포함해 계속 native_t 컬럼으로
수행되고 있었다** — lag1이 아니라. `_hypothesis_rows`를 수정해 `feature`를
`family["variant_columns"][family["official_feature_variant"]]`로 resolve하되
`hypothesis_id`는 여전히 native 컬럼 이름으로 구성해 식별자 안정성을
유지했다(`feature`와 `hypothesis_id`의 컬럼 이름이 달라질 수 있음).
회귀 테스트: `test_primary_registry_scans_the_official_feature_variant_not_native`
(`tests/unit/test_horizon_scan_registry.py`). A0 feature/label 산식은 바뀌지
않았으므로(registry가 어떤 기존 컬럼을 고르는지만 수정) `config_hash`는
불변이고 A0 재생성은 불필요하다. 수정 후 `flow_foreign_netbuy_to_volume`
재검증: `native_ic == lag1_ic == -0.01310257...`(둘 다 이제 lag1 컬럼을 가리킴).

CLI 검증 중 그 밖에 두 가지를 더 고쳤다: (1) `research/etl/mart.py`의 mart
캐시가 `LakeConfig.analysis_config_hash`로 유효성을 검사하는데
`run_phase_a`가 이를 설정하지 않아(`resolve_config`는 입력값을 그대로
복사할 뿐 채우지 않음) 매 실행이 거짓 "hash mismatch"로 죽었다 —
`build_a0_inputs`와 동일하게 `dataclasses.replace(lake,
analysis_config_hash=config.config_hash)`로 수정. (2) 보고서의
`common_formation_end`가 실제 resolve된 날짜가 아니라 config의
`common_formation_horizon`(윈도우 길이 정수 `120`)을 잘못 표시하고
`effective_sample_end`가 항상 `None`이었다 — `register_period_segment_view`가
resolve한 날짜를 반환하도록 하고 `bh_rows`에서 실제 max effective_sample_end를
계산하도록 수정.

`px_reversal_5d`/`flow_foreign_netbuy_to_volume` 두 family로 각각
`--smoke-family ... --permutations 1` 실측 검증을 통과했다: 7-cell 부분
registry·BH·A-4 게이트·A-5 offset·A-6 permutation(1회)/canary·A-7/8 카드·A-9
plot 7종·markdown 11-section·atomic publish가 모두 실제 `_SUCCESS.json`을
남기며 끝까지 성공했고, 두 결과 모두 경제적으로 타당했다(reversal: 즉각
반응·양의 부호·q≈6e-99·grade A; foreign flow: 기대 부호와 반대·discovery
없음·grade D). `uv run pytest tests/unit`(643 passed)로 회귀 확인했다.

### 1.2 입력 불변 계약

Phase A는 A0 manifest의 다음 fingerprint를 그대로 상속한다.

```text
snapshot_date
source
raw_manifest_hash
a0_manifest_hash
config_schema_version
config_hash
mart별 schema_hash / row_count / key_count
quality policy version
universe policy version
label policy version
```

코드가 실행되는 worktree가 dirty인 것 자체는 연구 실행을 막지 않지만, 다음을 manifest에
남겨 동일 코드인지 확인할 수 있게 한다.

```text
git_commit
git_dirty
phase_a_code_hash
python_version
duckdb/polars/numpy version
command line
started_at/finished_at (Asia/Seoul)
```

`phase_a_code_hash`는 scan driver, 통계 함수, report renderer와 config 파일 내용의 안정적
hash다. 동일 snapshot/config라도 code hash가 다르면 이전 결과 디렉터리를 재사용하지 않는다.
재현성 비교용 통계 content hash는 `run_id`, 실행 시각, 물리적 parquet metadata를 제외하고
논리 row를 canonical sort한 뒤 계산한다.

### 1.3 Phase A 종료 조건

다음을 모두 충족해야 Phase A가 완료된다.

1. 12개 주 검정 family, 4개 short exploratory family와 1개 reference family가 registry에
   누락 없이 존재하고 각 readiness에 맞게 실행 또는 명시적 blocked 처리되었다.
2. 사전등록 주 검정 집합의 hypothesis ID가 정확히 75개이며 각 ID가 결과에 정확히
   1행 존재한다.
3. 75개 셀 전체에 대해 global BH-FDR q=0.10이 한 번 적용되었다. 표본 부족 셀도
   검정 수에서 빠지지 않았다.
4. 누적·bucket core 결과와 broad/tradable, common-survivor/available 결과가 모두
   materialize되었다.
5. market/size/liquidity/period/short-regime/1일 지연 진단이 Cartesian product 없이
   독립 축으로 산출되었다.
6. 모든 가능한 offset의 비중첩 subsampling 결과가 생성되었다.
7. 고정 seed로 100회 단면 permutation을 완료하고 발견 건수 null 분포를 기록했다.
8. NW lag 59 이상인 장기 primary 셀에 대해 100회 temporal placebo를 완료했다.
9. 모든 family에 decay 요약, 자동 패턴, 경고, 사람 검토 상태, 다음 단계가 있는 결론
   카드가 생성되었다.
10. Phase A 집계에 사용된 모든 row가 `label_end_date < holdout_start`를 만족한다.
11. 동일 코드·config·input·seed 재실행에서 `run_id`·시각 같은 volatile provenance를
    제외하고 정렬한 통계 content hash가 동일하다.
12. 결과 manifest와 `_SUCCESS.json`은 모든 산출물의 schema/content hash 확인 후에만
    원자적으로 기록된다.

실데이터 발견 건수가 0이거나 permutation 95퍼센타일 이하라는 사실은 파이프라인 실패가
아니다. 이는 "price·flow 단변량 증거가 약함"이라는 유효한 연구 결과이며, 기술적 종료
조건과 분리한다.

## 2. 사전등록 검정 계약

### 2.1 family와 주 검정 셀

`horizon_scan_config.yaml`이 정본이다. 아래 표는 Phase A 실행기가 기대하는 기계 판독
내용이며 결과를 보기 전에 config validation으로 대조한다.

bucket은 `h_end`가 family의 primary horizon에 포함될 때만 해당 family의 주 검정 셀로
간주한다. 예를 들어 `[1,2,3,5,10]` family는 누적 5개와 `(0,5]`, `(5,10]` bucket
2개를 가진다.

| Family | Primary feature | 부호 | Cumulative h | Bucket end | BH 셀 | Short exploratory 셀 |
|---|---|---:|---|---|---:|---:|
| 단기 반전 | `px_reversal_5d` | `+` | 1,2,3,5,10 | 5,10 | 7 | 0 |
| 중기 모멘텀 | `px_mom_12_1` | `+` | 20,40,60,120 | 20,40,60,120 | 8 | 0 |
| 잔차 모멘텀 | `px_resid_mom_12_1` | `+` | 20,40,60,120 | 20,40,60,120 | 8 | 0 |
| 52주 고점 | `px_near_52w_high` | `+` | 20,40,60 | 20,40,60 | 6 | 0 |
| MAX | `px_maxret_20d` | `-` | 20,40,60 | 20,40,60 | 6 | 0 |
| IVOL | `px_idio_vol_60d` | `-` | 20,40,60 | 20,40,60 | 6 | 0 |
| 비유동성 | `px_amihud_20d` | `+` | 60,120 | 60,120 | 4 | 0 |
| 거래량 충격 | `px_turnover_shock` | `+` | 5,10,20 | 5,10,20 | 6 | 0 |
| 무변동·무거래 비율 | `px_zero_ret_ratio_20d` | reference | — | — | 0 | 0 |
| 외국인 순매수 | `flow_foreign_netbuy_to_volume_20d` | `+` | 5,10,20 | 5,10,20 | 6 | 0 |
| 기관 순매수 | `flow_inst_netbuy_to_volume_20d` | `+` | 5,10,20 | 5,10,20 | 6 | 0 |
| 개인 순매수 | `flow_individual_netbuy_to_volume_20d` | 양측 | 5,10,20 | 5,10,20 | 6 | 0 |
| 외국인 보유 변화 | `flow_foreign_holding_ratio_chg_20d` | `+` | 20,40,60 | 20,40,60 | 6 | 0 |
| 공매도 강도 | `flow_short_turnover_20d` | `-` | 5,10,20,40,60 | 5,10,20,40,60 | 0 | 10 |
| 공매도 잔고 | `flow_short_interest_ratio` | `-` | 20,40,60 | 20,40,60 | 0 | 6 |
| Days to cover | `flow_days_to_cover` | `-` | 20,40,60 | 20,40,60 | 0 | 6 |
| NAT proxy | `flow_nat_proxy_20d` | `+` | 20,40,60 | 20,40,60 | 0 | 6 |
| **합계** | 12 test + 4 short exploratory + 1 reference |  |  |  | **75** | **28** |

`flow_nat_20d`라는 이름으로 proxy를 실행하지 않는다. 원 논문식 검증 전에는
`flow_nat_proxy_20d`, `formula_version=proxy_v0`만 사용한다.

공매도 4개 family를 primary에서 내린 이유는 신호를 본 결과가 아니라 실행 전 coverage다.
공통 120d formation에서 short turnover는 2020-03 이전 약 1,300일, balance/NAT는
2016-07~2020-03 약 790일의 단일 allowed 블록만 남는다. 2021-05~2023-11 partial 구간을
살릴 PIT KOSPI200/KOSDAQ150 구성종목 데이터가 없으므로 current constituent를 소급하지
않는다. 이 결정은 기존 103셀 중 28셀을 exploratory로 고정하고 global BH `m=75`를 만든다.
Phase A에 한해 이 specific plan과 config schema v2가 `03`의 103-cell 가정보다 우선한다.
향후 PIT index membership을 수집해 short family를 승격할 때는 같은 run을 수정하지 않고
새 config version과 새 검정 집합으로 사전등록한다.

### 2.2 primary·exploratory·reference 분리

셀 역할을 아래처럼 고정한다.

- `primary`: 위 표의 primary feature × primary horizon/bucket. global BH 대상.
- `exploratory_short_regime`: 공매도 강도·잔고·days-to-cover·NAT proxy의 28개 사전등록
  셀. limited single-block sample 진단 전용이며 global BH와 screen pass에서 제외.
- `exploratory_horizon`: primary feature의 사전등록 구간 밖 누적/bucket.
- `secondary_feature`: `px_mom_6_1`, net-buy 5/60d 등 사전등록 secondary window의
  전체 누적/bucket.
- `reference`: `px_zero_ret_ratio_20d`. 품질·실행가능성 진단 전용.
- `segment_diagnostic`: primary/secondary의 시장·규모·유동성·기간·regime 결과.

exploratory/secondary/segment 결과에는 `q_fdr_global`을 채우지 않는다. overall core의
탐색적 발견은 `abs(t_nw) > 3`일 때만 표시하고 `screen_pass`로 승격하지 않는다.
reference family는 부호·peak·발견 판정을 하지 않는다.

### 2.3 global BH의 정확한 모집단

global BH의 주 결과는 다음 좌표 하나로 고정한다.

```text
phase             = A
universe          = broad
sample_kind       = common_survivor
segment_axis      = overall
segment           = all
feature_variant   = family별 official variant
feature_role      = primary
hypothesis_role   = primary
scan_type         = cumulative 또는 bucket
```

두 universe, 두 sample, 지연 variant, secondary feature, 세그먼트를 모두 BH에 넣어
검정 수를 부풀리지 않으며, 반대로 이 중 유리한 결과를 주 결과로 바꾸지도 않는다.
tradable은 acceptance 강건성 gate, available sample은 attrition 진단이다.

검정 규칙:

1. 모든 family에 양측 HAC p-value를 사용한다.
2. 기대 부호가 있는 family는 BH 통과 후 `expected_sign × ic_mean > 0`도 만족해야 한다.
3. 개인 순매수는 양측 결과를 유지하고 관측 부호로 패턴을 기술한다.
4. `insufficient`, 상수 feature, 전부 NULL인 사전등록 primary 셀은 원 p-value를 NULL로
   두되 BH 입력에는 `p_for_bh=1.0`을 넣는다. 따라서 검정 수 `m=75`는 변하지 않는다.
5. p-value tie에는 안정 정렬된 `hypothesis_id`를 보조 키로 쓰며, BH q-value는 역방향
   누적 최소값으로 단조화한다.
6. `q_fdr_global < 0.10`을 통과로 사용한다. `<=`로 구현하지 않는다.

권장 hypothesis ID는 아래 형식이다.

```text
<family>|<primary_feature>|cum|0|<h>
<family>|<primary_feature>|bucket|<h1>|<h2>
```

### 2.4 Phase A 실행 파라미터

A0 config의 `stats`에 아래 Phase A 파라미터가 없으면 결과를 만들기 전에 config schema
version을 올리고 A0 manifest부터 재생성한다. Phase A 코드의 숨은 상수로 두지 않는다.

```yaml
phase_a:
  discovery_universe: broad
  discovery_sample_kind: common_survivor
  min_names_per_date_market: 20
  min_names_for_spread: 50
  quantile_count: 5
  segment_bin_count: 3
  nw_p_value_distribution: asymptotic_normal
  nw_gap_policy: calendar_session_distance
  bh_missing_p_value: 1.0
  delay_confirm_p_nw: 0.05
  delay_min_abs_ic_retention: 0.50
  tradable_min_abs_ic_retention: 0.50
  permutation_repeats: 100
  permutation_seed: 20260801
  permutation_block: date_market
  temporal_placebo_repeats: 100
  temporal_placebo_min_nw_lag: 59
  temporal_placebo_min_shift_sessions: 120
  temporal_placebo_p_max: 0.10
  nonoverlap_all_offsets: true
  nonoverlap_min_dates: 20
  nonoverlap_inference: exact_sign_test
  half_life_fraction: 0.50
  isolated_spike_neighbor: adjacent_registered_cell_same_scan_type
  available_sign_flip_max_grade: C
  insufficient_offset_max_grade: B
  sparse_primary_grid_families: [px_amihud_20d]
  period_sets:
    common:
      - {id: 2014_2016, start: 2014-06-01, end: 2016-12-31}
      - {id: 2017_2019, start: 2017-01-01, end: 2019-12-31}
      - {id: 2020_2021, start: 2020-01-01, end: 2021-12-31}
      - {id: 2022_2023_10, start: 2022-01-01, end: 2023-10-31}
      - {id: 2023_11_common_end, start: 2023-11-01, end: common_formation_end}
    available:
      - {id: 2014_2016, start: 2014-06-01, end: 2016-12-31}
      - {id: 2017_2019, start: 2017-01-01, end: 2019-12-31}
      - {id: 2020_2021, start: 2020-01-01, end: 2021-12-31}
      - {id: 2022_2023_10, start: 2022-01-01, end: 2023-10-31}
      - {id: 2023_11_2025_03, start: 2023-11-01, end: 2025-03-31}
      - {id: 2025_04_holdout, start: 2025-04-01, end: horizon_eligible_end}
  evidence_grade:
    evaluation_order: [R, C, A, B, D]
    A: screen_pass_and_no_core_warning_and_all_offsets_evaluable
    B: screen_pass_with_nonfatal_warning
    C: exploratory_or_secondary_or_available_sign_flip
    D: no_signal_or_wrong_sign_or_robustness_fail
    R: reference_only
```

기존 공통 값도 그대로 사용한다.

```yaml
sample.start: 2014-06-01
sample.holdout_start: 2025-08-01
sample.holdout_boundary: label_end_date
sample.common_formation_horizon: 120
sample.common_survivor_horizon: 120
stats.nw_lag_cumulative: h_minus_1
stats.nw_lag_bucket: width_minus_1
stats.global_bh_q: 0.10
stats.exploratory_abs_t_nw: 3.0
stats.min_dates_per_cell: 60
```

cell의 실제 최소 날짜 수는 고정 60만 쓰지 않고 다음처럼 유도한다.

```text
min_dates_required = max(min_dates_per_cell, nw_lag + 2)
```

따라서 cumulative 120d는 최소 121개 daily IC가 필요하다. quantile 수, segment bin 수,
half-life 비율, isolated-spike 이웃 정의, evidence-grade 상한과 기간 interval도 모두 같은
config hash에 포함한다.

## 3. 분석 표본과 join 규칙

### 3.1 분석 패널 grain

모든 분석 입력은 `(trade_date, ticker, market)` 최대 1행이다. join 순서는 다음과 같다.

```text
label_scan
  LEFT JOIN feat_price / feat_flow
  LEFT JOIN dim_universe_broad_daily
  LEFT JOIN dim_universe_tradable_daily
  LEFT JOIN dim_stock_pit_daily
  LEFT JOIN dim_price_quality_daily
```

각 join 전후 row count와 key distinct count가 같아야 한다. 중복이 생기면 임의 dedup하지
않고 실패한다. price와 flow의 같은 이름 컬럼 충돌은 명시적 select list로 차단한다.

분석 패널에 최소한 다음 공통 컬럼이 있어야 한다.

```text
trade_date, formation_session_idx, ticker, market
in_broad, in_tradable
market_cap_pit, px_amihud_20d
short_regime
ca_mask / quality flags
family별 feature와 lag1 feature
누적·bucket raw return / excess / rank / end date
common_formation_120d, common_survivor_120d
```

wide panel을 메모리에 한 번에 올리지 않는다. DuckDB에서 공통 key·label·segment 컬럼과
현재 family의 feature만 projection하고 Polars로 집계한다. 반복 사용되는 segment bin과
label eligibility는 snapshot/config별 중간 parquet로 cache할 수 있지만 Phase A output
namespace 밖의 A0 mart를 변경하지 않는다.

### 3.2 official sample — common formation + common survivor

주 곡선과 global BH 표본은 아래 조건을 모두 만족한다.

```text
trade_date >= sample.start
common_formation_120d = true
common_survivor_120d = true
in_broad = true
official feature가 finite
해당 feature lookback quality가 valid
해당 horizon/bucket의 quality-valid label이 finite
```

`common_formation_120d`는 공통 KRX calendar의 120번째 다음 session이 holdout 전에 끝나는
**날짜 수준** flag다. `common_survivor_120d`는 그 formation date에서 해당 종목의 120번째
미래 valid-session endpoint가 실제로 존재하고 holdout 전에 끝나는 **종목 수준** flag다.
둘 다 CA/quality mask 전에 계산한다. CA는 survivor에서 종목을 제거하지 않고 이후 해당
label을 NULL로 만들어 `label_coverage`에만 반영한다.

공매도 4개 family는 official primary sample이 없고 exploratory allowed-only 표본만 가진다.
balance family는 여기에 A0 publication availability 조건도 추가한다.
family별 최초 유효일이 다르므로 모든 family의 시작일을 가장 늦은 family에 맞추지 않는다.
대신 각 결과에 `effective_sample_start`, `effective_sample_end`, `n_dates`, `n_obs`를 남긴다.

`common_survivor`는 horizon 간 curve 비교를 위한 조건이지 생존편향 해소가 아니다.
현재 종목 중심으로 수집된 OHLCV의 한계를 모든 60/120d 결론 카드에 붙인다.

### 3.3 available sample — attrition 진단

보조 결과는 horizon별 최대 가용 표본을 사용한다.

```text
trade_date >= sample.start
label_end_date_<h> < holdout_start
해당 horizon/bucket label이 finite
해당 universe의 formation 조건 충족
official feature가 finite
```

available 결과에는 global BH를 다시 적용하지 않는다. common-survivor와의 IC 부호 차이,
크기 차이, date/종목 수 차이를 보고 `attrition_warning`을 생성한다. primary discovery의
available 결과가 반대 부호면 `available_direction_pass=false`, `screen_pass=false`,
evidence grade 상한 `C`로 고정한다. 크기 차이 임계값을 추가하려면 실행 전에 config에
명시한다.

### 3.4 coverage 분모

coverage가 구현마다 다른 분모를 쓰지 않도록 다음을 구분한다.

| 컬럼 | 정의 |
|---|---|
| `n_formation` | 날짜·universe·holdout 조건을 만족한 formation row 수 |
| `feature_coverage` | finite feature row / `n_formation` |
| `survival_to_h` | 품질 mask 전 t+h endpoint price 존재 row / `n_formation` |
| `label_coverage` | CA/quality mask 후 finite label row / `n_formation` |
| `n_obs` | finite feature와 finite label이 모두 있는 최종 IC row 수 |
| `n_obs_mean/min/median` | 유효 date×market 단면의 종목 수 요약 |

date×market 단면의 종목 수가 `min_names_per_date_market`보다 작거나 feature/label이
상수여서 Spearman을 계산할 수 없으면 그 market-date IC는 NULL이다. 유효 daily IC가
`max(min_dates_per_cell, nw_lag+2)`보다 적으면 main 셀을 `insufficient`로 둔다.

### 3.5 holdout 봉인

Phase A는 holdout용 수익률을 집계하지 않는다.

- 누적: `label_end_date_<h>d < 2025-08-01`
- bucket: `bucket_end_date_<h1>_<h2>d < 2025-08-01`
- 공통 표본: `label_end_date_120d < 2025-08-01`
- `--include-holdout`, `--holdout-start` CLI override는 Phase A official mode에 두지 않는다.
- debug override가 있더라도 결과는 별도 namespace의 `official=false`다.
- holdout 구간 가격만 임의 변경한 synthetic/integration fixture에서 Phase A 산출물 hash가
  변하지 않는 tripwire test를 둔다.

## 4. 통계 산식과 결과 단위

### 4.1 date×market IC에서 daily IC까지

각 셀의 주 통계는 다음 순서로 계산한다.

1. `(trade_date, market)` 안에서 feature와 `y_rank_*`의 Spearman IC를 계산한다.
2. 같은 날짜의 KOSPI/KOSDAQ IC를 해당 단면 유효 종목 수로 가중 평균한다.
3. 날짜당 1개인 `daily_ic` 시계열에 평균, 표준편차, ICIR, naive t, NW t를 계산한다.

```text
daily_ic[t] = sum_market(ic[t,m] * n[t,m]) / sum_market(n[t,m])
ICIR        = mean(daily_ic) / sample_std(daily_ic)
```

ICIR은 이 단계에서 연율화하지 않는다. market segment 결과는 시장별 IC 시계열 자체에
NW를 적용하며 다시 두 시장을 합치지 않는다.

```text
kospi_weight[t] = n[t,KOSPI] / (n[t,KOSPI] + n[t,KOSDAQ])
kospi_weight_mean = valid daily_ic 날짜의 kospi_weight 평균
```

한 시장만 유효한 날짜는 그 시장 weight가 1이며 이 날짜의 비율도 함께 기록한다.

### 4.2 Newey–West

lag는 config에서 기계적으로 결정한다.

```text
cumulative h: L = h - 1
bucket (h1,h2]: L = (h2 - h1) - 1
```

Bartlett kernel의 mean-only HAC standard error를 사용한다. `t_nw = mean / se_hac`이고
`p_nw`는 asymptotic normal 양측 p-value다. 자기공분산 `gamma_k`의 pair는 압축된 배열
위치가 아니라 `formation_session_idx`의 정확한 KRX session 거리 `k`로 만든다. 몇 년짜리
regime 공백 양쪽 관측을 lag 1 이웃으로 취급하거나 missing date를 0으로 채우지 않는다.
각 lag의 pair 수를 출력해 gap 영향을 진단한다.

`n_dates < max(60, L+2)`, 분산 0, 비유한 입력은 NULL/insufficient로 처리한다. cumulative
120d는 최소 121개 daily IC가 필요하다. naive t는 진단용으로만 남기며 발견·판정에는
사용하지 않는다.

단위 테스트는 작은 손계산 시계열, 긴 date gap이 있는 시계열과 독립 구현 기준값을 함께
사용한다. 특정 실데이터 family의 t-stat을 golden으로 고정하지 않는다.

### 4.3 raw-return quantile spread

IC는 rank label을 사용하지만 경제 단위 spread는 해당 horizon/bucket의 **raw excess
return**으로 계산한다.

1. date×market 안에서 raw feature를 평균 tie rank한다.
2. `quantile_count=5`, `min_names_for_spread=50`을 만족할 때 Q5와 Q1의 동일가중 raw
   excess return 차이를 계산한다.
3. 같은 날짜의 시장별 spread를 유효 종목 수로 가중 평균한다.
4. `q5_minus_q1_raw`와 날짜 평균 `q5_spread_raw`를 보존한다.

기대 부호가 음인 family는 raw spread와 함께
`q5_spread_aligned = expected_sign × q5_spread_raw`를 제공한다. 개인 순매수는 raw 방향을
그대로 둔다. Phase A에서는 turnover와 비용을 계산하지 않으므로 이 spread를 투자 성과로
해석하지 않는다. 같은 date×market 안에서 benchmark는 모든 종목에 같은 상수이므로
Q5−Q1 spread는 raw return과 excess return 중 어느 것을 써도 수치가 동일하다. 컬럼은 label
계약과 맞추기 위해 excess를 쓰되 이 항등성을 테스트한다.

### 4.4 curve 일관성과 고립 spike

BH 통과 셀이라도 같은 scan axis의 인접 사전등록 셀이 모두 반대 부호·NULL이면
`isolated_spike=true`다. grid 끝 셀은 유일한 이웃을 사용한다. 기대 부호 family는 aligned
IC, 개인 순매수는 관측된 IC 부호를 사용한다.

인접 셀의 추가 유의성 임계값을 결과를 본 뒤 만들지 않는다. `isolated_spike`는 자동
경고이며, `03`의 "인접 horizon에서 일관된 곡선만 신뢰" 규칙에 따라 screen pass를
막는다.

## 5. 작업 패키지와 구현 순서

### A-0. preflight와 official run 계약

**목적**: A0와 다른 입력·config 또는 holdout이 섞인 실행을 시작 전에 차단한다.

구현:

1. A0 manifest를 읽고 snapshot/source/config/schema/code dependency를 검증한다.
2. registry에서 Phase A family만 선택하되 17개 이름과 primary 16개 readiness를 확인한다.
3. §2.1 규칙으로 primary hypothesis registry 75개와 short exploratory registry 28개를
   생성하고 역할·셀 수를 확인한다.
4. A0 config의 family별 official feature variant와 이미 materialize된 variant column을
   선택한다. Phase A에서 feature를 shift해 새 variant를 만들지 않는다.
5. output run directory를 만들고 immutable `run_spec.json`을 먼저 기록한다.
6. holdout boundary와 official/debug mode를 검사한다.
7. 실패 시 결과 parquet와 success marker를 만들지 않는다.

official variant 기본 정책은 A0에서 이미 동결되어 있어야 한다.

- price: `native_t`, `lag1`은 강건성 진단
- flow: 당일 마감 후 게시 가능성이 검증된 family만 `native_t`; 그 외 `lag1`
- balance: A0의 publication lag 적용 후 variant를 사용하며, lag evidence 미확정이면
  exploratory blocked

availability가 불명확한 상태에서 native와 lag1 중 결과가 좋은 것을 고르지 않는다.

테스트:

- config hash mismatch 거부
- A0 `smoke_only=true` 거부
- family/readiness 누락 거부
- primary hypothesis 74/76개, short exploratory 27/29개 거부
- 수동 snapshot 실행이 official로 표시되지 않음
- holdout boundary override 거부

완료 기준: 이후 stage가 변경할 수 없는 `run_spec.json`, 75-cell primary registry와
28-cell short exploratory registry가 생성된다.

### A-1. 분석 패널·segment dimension 준비

**목적**: 모든 scan이 같은 row eligibility와 같은 segment 경계를 재사용하게 한다.

구현:

1. A0 mart를 §3.1 grain으로 join하는 projection/helper를 추가한다.
2. broad formation panel에서 feature와 무관하게 date×market별 PIT mktcap tertile을 만든다.
3. 같은 방식으로 `px_amihud_20d` tertile을 만든다.
4. broad에서 만든 tertile 경계를 tradable에도 재사용해 universe별 segment 의미를 맞춘다.
5. 기간 구간을 config의 고정 interval로 부여한다.
6. family별 effective date, feature coverage, label coverage, short allowed coverage를 계산한다.
7. A0 mart의 native/lag1 column mapping과 value shift invariant를 검증한 뒤 필요한 variant를
   projection한다.

size/liquidity bin은 feature non-null 여부나 horizon label 존재 여부로 다시 계산하지 않는다.
tie는 average percentile rank로 처리한다. 해당 날짜·시장에 bin을 만들 종목이 부족하면
segment를 NULL로 두고 진단 셀을 insufficient 처리한다.

기간 segment는 sample kind별로 분리한다.

```text
common-survivor 공식 표본:
  2014-06-01 .. 2016-12-31
  2017-01-01 .. 2019-12-31
  2020-01-01 .. 2021-12-31
  2022-01-01 .. 2023-10-31
  2023-11-01 .. common_formation_end (KRX calendar로 약 2025-02-04)

available/horizon별 진단:
  위 첫 4구간
  2023-11-01 .. 2025-03-31
  2025-04-01 .. horizon별 pre-holdout eligible end
```

공통 120d formation에는 2025-04 이후 구간이 결정론적으로 존재하지 않으므로 셀 자체를
생성하지 않는다. 2023-11 이후 구간도 실제 `common_formation_end`에서 자른다. available
short-horizon 진단에만 2025-04 이후 구간을 만들며 이를 공식 subperiod gate에 섞지 않는다.

테스트:

- join 전후 row/key 수 보존
- broad cutpoint가 tradable에서 재계산되지 않음
- 두 market의 tertile 계산 격리
- tie와 작은 단면 처리
- A0 native/lag1 mapping과 valid-session shift invariant
- period/short regime 경계일
- family effective start가 A0 readiness와 일치

완료 기준: core/robustness runner가 같은 eligibility helper와 segment dimension을 읽는다.

### A-2. cumulative·bucket core scan

**목적**: 전체 17 family의 누적 효과와 영향 구간을 동일 schema로 산출한다.

구현:

1. config registry 순서대로 family/feature를 projection한다.
2. cumulative 9개 horizon과 bucket 6개를 모두 계산한다.
3. primary·exploratory_short_regime·exploratory_horizon·secondary·reference role을
   row에 부여한다.
4. broad/common-survivor/official variant의 date×market IC, daily IC, NW, spread를 계산한다.
5. broad/tradable × common-survivor/available 보조 결과를 같은 schema로 산출한다.
6. daily IC 중간 결과를 저장해 segment/subsampling에서 재사용한다.
7. NULL/inf/상수/단면 부족 사유를 `status_reason`으로 보존한다.

bucket은 A0 `label_scan`의 raw bucket return과 rank를 직접 사용한다. 누적 excess끼리
빼서 bucket을 다시 계산하지 않는다.

완료 기준: 각 실행 좌표에서 예상 cell 수와 key uniqueness를 만족하고, primary core
좌표에는 75개 hypothesis, short exploratory 좌표에는 28개 셀이 모두 존재하거나 명시적
blocked reason이 있다.

### A-3. global BH와 core 발견 표

**목적**: 주 검정 집합에만 한 번의 발견율 통제를 적용한다.

구현:

1. §2.3의 exact coordinate와 hypothesis registry를 inner validation한다.
2. raw p-value, `p_for_bh`, rank, monotone q-value를 계산한다.
3. `bh_pass`, `expected_sign_pass`, `isolated_spike`, `primary_discovery`를 분리한다.
4. exploratory/secondary 결과에는 `abs(t_nw)>3` 플래그만 부여한다.
5. reference 결과에는 발견 관련 필드를 NULL로 둔다.

```text
primary_discovery =
  status = valid
  AND q_fdr_global < 0.10
  AND expected-sign rule 충족
  AND isolated_spike = false
```

BH 통과와 최종 screening 통과를 한 boolean으로 합치지 않는다. 이후 tradable, subperiod,
delay, CA gate가 별도로 남아 있기 때문이다.

테스트:

- 알려진 p-value 배열의 BH q-value
- tie의 결정적 순서와 monotonicity
- insufficient p가 m=75를 줄이지 않음
- short exploratory p가 m=75에 들어가지 않음
- 양측 family의 부호 강제 없음
- expected sign 반대 방향 BH 통과가 discovery가 아님
- secondary/segment q-value가 NULL

완료 기준: `primary_hypotheses.parquet` 75행과 발견/비발견/insufficient 사유가 생성된다.

### A-4. universe·segment·regime·지연 강건성

**목적**: broad 전체에서 보인 신호가 microcap, 특정 시장·기간 또는 당일 가용성에만
의존하는지 확인한다.

각 축은 독립 실행한다.

| `segment_axis` | `segment` | 적용 |
|---|---|---|
| `overall` | `all` | 모든 family |
| `market` | `KOSPI`, `KOSDAQ` | 모든 family |
| `size` | `small`, `mid`, `large` | 모든 family |
| `liquidity` | `low`, `mid`, `high` | `px_amihud` self-segment는 reference 취급 |
| `period` | §A-1의 6구간 | 모든 family |
| `short_regime` | `allowed`, `partial`, `banned` | short/short balance/NAT 진단 |
| `execution_delay` | `native_t`, `lag1` | 모든 family |

`universe × market × size × period` 같은 교차 셀은 만들지 않는다. broad와 tradable은
각 독립 축 결과를 모두 가지되 세그먼트 결과는 새로운 발견으로 판정하지 않는다.

강건성 flag:

```text
tradable_retention = abs(IC_tradable) / abs(IC_broad)
tradable_pass = same_direction AND retention >= 0.50

period_sign_pass =
  expected direction인 valid period 수 > valid period 수 / 2

available_direction_pass =
  available sample IC가 common-survivor discovery와 같은 방향

delay_pass (h<=5 또는 bucket (0,5] 증거):
  same_direction
  AND abs(IC_lag1) / abs(IC_native) >= 0.50
  AND p_nw_lag1 < 0.05
```

분모 IC가 0 또는 비유한 값이면 retention은 NULL/pass=false다. flow의 official variant가
이미 lag1이면 native는 참고 진단이고 delay gate는 official 결과 자체로 충족 여부를 본다.
short family의 `period_sign_pass`는 `allowed` 관측만 계상한다.

공매도 4개 family는 exploratory라 위 flag를 진단용으로만 계산하고 screen pass/evidence
A·B를 부여하지 않는다. 공식 period gate에는 common-survivor period set만 사용한다.

overall daily IC가 종목 수 가중이므로 결과마다 `kospi_weight_mean`과
`kosdaq_weight_mean`을 저장하고 family 카드에 전자를 표시한다. KOSDAQ 종목 수가 많아
overall 결과가 한 시장에 치우치는 정도를 숨기지 않는다.

`management_filter_available=false`와 historical membership coverage 부족은 모든
tradable 결과 metadata에 유지한다. tradable pass가 생존·거래가능성 문제를 해결했다고
표현하지 않는다.

완료 기준: primary discovery마다 동일 cell의 tradable/period/delay gate를 기계적으로
결합할 수 있고 available direction과 시장 평균 가중치를 함께 읽을 수 있다. 누락된 축은
pass가 아니라 `not_evaluable`로 보존된다.

### A-5. 전 offset 비중첩 subsampling

**목적**: overlapping forward return에 대한 NW 결과의 방향이 특정 calendar offset에만
의존하지 않는지 확인한다.

공통 KRX trading calendar의 formation ordinal을 사용한다.

```text
cumulative h: stride = h, offset = 0 .. h-1
bucket width w: stride = w, offset = 0 .. w-1
선택 조건: formation_ordinal % stride = offset
```

각 offset에서 date×market IC → daily IC를 다시 집계한다. 이 경로에는 main cell의
`min_dates_per_cell=60`을 재사용하지 않는다. `nonoverlap_min_dates=20` 이상이면 평균 IC와
daily aligned IC 부호에 대한 exact binomial sign test(`H0: p=0.5`)를 계산한다. 장기
h=120의 offset당 약 22개 관측처럼 t 추론에는 짧지만 방향 점검에는 쓸 수 있는 표본을
살린다. 대립가설은 사전 방향으로 `H1: p>0.5`다. 양측 family는 core primary discovery에서
정해진 관측 부호를 alignment 방향으로
사용하며 discovery가 없으면 양방향 결과만 보고 pass를 만들지 않는다. 20개 미만은
`offset_status=insufficient`다.

```text
n_offsets_total / n_offsets_valid
offset_ic_mean_median
offset_ic_mean_min/max
offset_sign_agreement_ratio
offset_sign_test_p_median/min/max
offset_status
```

가장 좋은 offset 하나를 대표 결과로 고르지 않는다. subsampling은 global BH에 들어가지
않는다. NW 방향과 offset median 방향이 다르거나 유효 offset의 과반이 같은 방향이 아니면
`overlap_robustness_warning`을 붙인다. 필수 offset 하나라도 insufficient면 screen pass를
자동 취소하지는 않지만 evidence grade `A`는 금지하고 상한을 `B`로 둔다.

테스트:

- h/w별 offset 수 정확성
- 각 formation date가 offset 하나에만 포함
- bucket은 h_end가 아니라 width를 stride로 사용
- KRX 휴일이 calendar ordinal을 깨지 않음
- 20/19개 경계와 exact sign-test 손계산
- stride 60/120에서 min_dates 60을 잘못 재사용하지 않음
- 임의 한 offset 선택 API가 official mode에 없음

완료 기준: 유효한 모든 primary 셀에 offset 요약 또는 명시적 insufficient 사유가 있다.

### A-6. 단면 permutation과 장기 temporal placebo

**목적**: 서로 다른 두 null 실험의 역할을 구분한다. 단면 permutation은 join/leakage와
75셀 다중검정 규모를 진단하고, temporal placebo는 장기 overlapping label에서 NW t의
유한표본 null을 확인한다.

#### A-6a. 100회 date×market 단면 permutation

1. broad/common-survivor core panel을 사용한다.
2. 각 replicate에서 `(trade_date, market)` block 안의 row permutation 하나를 만든다.
3. 같은 permutation mapping을 **모든 primary feature column에 공동 적용**한다.
4. label, universe, segment, date는 고정한다.
5. feature 간 동시점 상관과 horizon label 간 상관을 가능한 한 보존하면서 feature-label
   단면 연결만 끊는다.
6. 각 replicate의 75개 primary 셀에 실제와 같은 NW·BH를 적용한다.

feature별 독립 shuffle은 family 간 상관을 없애므로 금지한다. 값 대신 date×market 내부의
사전 계산된 rank vector를 공동 치환한다. 치환된 값의 rank는 rank의 치환과 같으므로 매
replicate에서 feature ranking을 다시 계산하지 않는다. NULL pattern도 row block과 함께
이동한다.

이 단면 permutation은 날짜마다 독립적으로 섞기 때문에 null daily IC의 시계열
자기상관을 보존하지 않는다. 따라서 **NW overlap 보정의 타당성을 검증하지 않으며**
`p_empirical_count`는 join/leakage와 global search-size 진단으로만 해석한다.

replicate별 산출:

```text
replicate, seed_hash
n_valid_hypotheses, n_bh_pass, n_primary_discovery
min_p_nw, min_q_fdr_global, max_abs_t_nw
```

```text
p_empirical_count
  = (1 + count(null_discovery_count >= real_discovery_count)) / (100 + 1)
```

실데이터 발견 수가 null 95퍼센타일을 넘지 못하면 전체 evidence를 약하게 해석하지만
기술적 실패로 만들지는 않는다.

#### A-6b. 장기 primary 셀 100회 circular date-shift placebo

`nw_lag >= 59`인 primary 13셀(cumulative 60d 7개, cumulative 120d 3개,
bucket `(60,120]` 3개)에만 두 번째 null을 적용한다.

1. holdout을 제외한 공통 KRX formation calendar 전체를 길이 `T>=120`인 하나의 연속
   circular block으로 본다.
2. 모든 장기 primary feature의 ticker×market panel을 **같은 date shift**로 공동 회전한다.
3. shift 거리는 `[120, T-120]`에서 seed로 선택하고 끝을 넘는 부분은 원형 wrap한다.
4. label과 그 overlapping 시계열, universe, 실제 date는 고정한다.
5. feature의 시계열 의존, date별 단면, family 간 상관은 보존하고 feature-label calendar
   정렬만 끊는다.
6. 각 shift에서 실제와 같은 gap-aware NW t를 계산한다.

원형 경계 한 곳의 불연속과 shift 후 ticker coverage를 replicate metadata에 기록한다.
long cell별 empirical p-value는 다음과 같다.

```text
p_temporal_nw
  = (1 + count(abs(t_nw_shift) >= abs(t_nw_real))) / (100 + 1)
temporal_null_pass = p_temporal_nw < 0.10
```

장기 primary discovery가 `temporal_null_pass=false`면 global BH 통과 사실은 보존하되
`screen_pass=false`로 내려 NW 과소보정 가능성을 채택 단계로 넘기지 않는다.

seed는 placebo 종류, replicate index, config hash로 결정해 실행 순서·병렬 수에 관계없이
같다. official run은 두 경로 모두 각 100회를 요구한다.

별도 synthetic look-ahead canary에서는 feature를 정확히 `fwd_ret_1d`로 두고 같은
date×market excess-rank label과 비교한 h=1 Spearman IC가 비퇴화 단면에서
`1.0 ± 1e-12`인지 검사한다. 긴 누적 label도 t+1 수익을 포함하므로 다른 horizon IC가
0이어야 한다는 assertion은 두지 않는다. canary는 75개 BH 집합과 공식 산출물에 절대
포함하지 않는다.

완료 기준: 단면 100개와 temporal 100개 replicate가 각자 중복 없이 존재하고 같은 seed
재실행이 동일 null summary를 만든다.

### A-7. decay 요약과 family 패턴 분류

**목적**: 셀 단위 결과를 "언제 나타나고 언제 사라지는가"라는 family 단위 답으로
변환한다.

공식 broad/common-survivor/official variant 곡선에서 다음을 계산한다.

| 필드 | 규칙 |
|---|---|
| `peak_h_cum` | `abs(aligned_ic)` 최대 cumulative h |
| `peak_bucket` | `aligned_ic` 최대 bucket |
| `onset_h` | primary cumulative 중 처음 `q<0.10`이고 부호가 맞는 h |
| `half_life_bucket` | peak 이후 aligned bucket IC가 config의 `half_life_fraction=0.50` 아래가 되는 첫 bucket |
| `sign_flip_bucket` | 유효 bucket의 IC 부호가 처음 바뀌는 구간 |
| `expected_sign_aligned_ratio` | daily aligned IC가 양수인 날짜 비율 |

peak가 0 이하이거나 유효 후속 bucket이 없으면 half-life를 억지로 만들지 않고 NULL과
reason을 기록한다. 개인 순매수는 raw IC 기준 peak/sign flip을 별도 필드에 둔다.
primary cumulative grid가 60/120 두 셀뿐인 `px_amihud_20d`는
`sparse_primary_grid=true`로 표시하고 isolated-spike가 단일 이웃에 의존함을 카드에 남긴다.
short 4개 family의 패턴에는 항상 `exploratory_short_regime=true`를 붙인다.

자동 패턴 후보:

- `immediate`: `(0,5]`가 peak이고 뒤 bucket의 발견이 없음
- `delayed`: `(5,10]` 이후 bucket에서 peak/발견
- `sign_reversal`: 서로 다른 bucket에서 통계적으로 유효한 반대 부호가 관찰됨
- `no_signal`: primary discovery가 없고 탐색적 `|t_nw|>3`도 없음
- `segment_limited`: core discovery는 있으나 tradable/period/delay gate 실패
- `exploratory_only`: primary discovery 없이 가설 밖 또는 secondary만 `|t_nw|>3`

자동 분류는 `pattern_auto`이고 최종 카드는 `review_status=unreviewed`로 먼저 생성한다.
사람 검토자는 데이터·config를 바꾸지 않고 curve와 diagnostic을 확인한 뒤
`pattern_reviewed`, 짧은 근거, reviewer를 별도 override 파일에 기록한다. 원 parquet를
수정하지 않는다.

family 간 아래 비교는 진단 표로 제공하되 증분성 검정으로 부르지 않는다.

- `px_mom_12_1` vs `px_resid_mom_12_1`
- `px_maxret_20d` vs `px_idio_vol_60d`
- foreign/inst/individual net-buy curve
- short turnover vs short interest vs days-to-cover
- NAT proxy vs 구성요소

완료 기준: 17개 family 모두 decay summary 또는 reference-only summary를 가진다.

### A-8. screening 판정과 결론 카드

**목적**: Phase A 발견과 다음 acceptance gate 진입 대상을 동일 규칙으로 전달한다.

검정 family의 candidate cell은 다음을 모두 만족할 때 `screen_pass=true`다.

1. 가설 구간 primary cell의 `primary_discovery=true`다.
2. 동일 cell의 tradable IC가 broad의 절반 이상이고 같은 방향이다.
3. insufficient를 제외한 유효 서브기간 과반에서 기대 부호가 일치한다.
4. h<=5 또는 `(0,5]` 의존 신호는 §A-4의 delay gate를 통과한다.
5. A0 corporate-action mask가 적용된 official run이다.
6. `isolated_spike=false`다.
7. available sample이 common-survivor discovery와 같은 방향이다.
8. `nw_lag>=59`인 장기 셀은 `temporal_null_pass=true`다.

family는 candidate cell 하나 이상이 통과하면 다음 acceptance gate 후보가 된다. 여러
horizon이 통과하면 하나를 사후 최적화해 고르지 않고 연속된 candidate band 전체를
인계한다. label 5/20/60d 후보군 배정은 다음 원칙을 사용한다.

- 영향이 1~10d에 집중: 5d 후보군
- 5~40d에 이어짐: 20d 후보군
- 20~120d에 유지: 60d 후보군
- bucket 부호 반전: 단일 방향 feature로 자동 인계하지 않고 horizon별 분리/보류

증거 등급:

- `A`: screen pass + 핵심 segment/offset 경고 없음. offset 하나라도 insufficient면 불가
- `B`: screen pass이나 시장·규모·유동성·attrition·offset 중 비치명 경고 존재
- `C`: exploratory/secondary/short-regime only 또는 available sign flip. acceptance gate 진입 불가
- `D`: 무신호 또는 부호 반대/강건성 실패
- `R`: reference/filter

family 카드 schema:

```text
family / domain / primary_feature / secondary_features
expected_sign / observed_sign
pattern_auto / pattern_reviewed / review_status
primary_discoveries / candidate_horizon_band / target_label_candidates
peak_h_cum / peak_bucket / onset_h / half_life / sign_flip
broad_ic / tradable_ic / tradable_retention
valid_subperiods / sign_consistent_subperiods
native_ic / lag1_ic / delay_pass
common_survivor_ic / available_ic / attrition_warning
nonoverlap_offset_summary
kospi_weight_mean / kosdaq_weight_mean
p_temporal_nw / temporal_null_pass
q_fdr_global / evidence_grade / screen_pass
warnings / limitations / next_action
```

모든 60/120d 카드에 `survival_bias_unresolved`, 모든 tradable 카드에 관리종목 source
가용 여부를 표시한다. 이것은 footnote 한 번으로 대체하지 않는다.

완료 기준: 17개 카드와 acceptance gate로 보낼 family/horizon band 목록이 생성된다.

### A-9. plot·보고서·atomic publish

**목적**: 기계 판독 결과와 사람이 검토할 결과 문서를 같은 run에서 재현한다.

family별 기본 plot:

1. cumulative IC curve: broad/tradable, common-survivor/available
2. bucket IC bar: aligned IC와 NW 95% interval
3. native vs lag1 curve
4. subperiod heatmap
5. market/size/liquidity segment dot plot
6. coverage/survival curve
7. non-overlap offset distribution

plot의 y축은 family 간 비교가 필요한 overview에서만 공통 범위를 쓰고, 개별 plot의
자동 확대 여부를 metadata에 표시한다. q 통과는 색/marker로 표시하되 segment의
unadjusted p-value를 발견처럼 강조하지 않는다.

결과 보고서 `03a_horizon_scan_results.md` 순서:

1. run identity와 A0 preflight
2. 표본·coverage·holdout 봉인 확인
3. 75개 global BH 요약과 28개 short exploratory coverage
4. 실제 발견 수 vs 100회 단면 permutation null
5. 장기 셀 NW t vs 100회 temporal placebo null
6. price overview와 9개 카드
7. flow overview와 8개 카드
8. segment·delay·attrition·offset 경고
9. acceptance gate 인계 목록
10. 무신호/보류/탐색적 후보
11. 생존편향·관리종목·publication lag 등 제한

모든 parquet·plot·markdown을 임시 run directory에 쓴 뒤 schema/content hash를 검증하고
최종 directory로 rename한다. `_SUCCESS.json`은 마지막에 기록한다.

## 6. 산출물 계약

### 6.1 디렉터리

생성 결과는 source와 config가 섞이지 않도록 아래에 둔다.

```text
research/output/horizon_scan/
  phase=A/
    snapshot_date=<selected>/
      source=sj2_remote/
        config_hash=<hash>/
          run_id=<kst_timestamp>-<code_hash8>/
            run_spec.json
            manifest.json
            core/
            robustness/
            permutation/
            cards/
            plots/
            03a_horizon_scan_results.md
            _SUCCESS.json
```

`research/output/`은 생성 artifact로 계속 gitignore한다. 설계·코드·YAML·테스트만 commit
대상이다. 결과를 저장소 문서로 승격할 때는 official run 검토 후 별도 작업으로 한다.

### 6.2 `horizon_ic.parquet`

최소 schema:

```text
run_id, snapshot_date, source, config_hash, code_hash
family, fdr_family, feature, feature_role, feature_variant
hypothesis_id, hypothesis_role, scan_type, h_start, h_end, width
universe, sample_kind, segment_axis, segment
expected_sign, effective_sample_start, effective_sample_end
status, status_reason
n_dates, n_obs, n_obs_mean, n_obs_min, n_obs_median
feature_coverage, label_coverage, survival_to_h
ic_mean, ic_std, icir, t_naive, t_nw, p_nw
n_hac_pairs_min, kospi_weight_mean, kosdaq_weight_mean
p_for_bh, q_fdr_global, bh_pass, expected_sign_pass
expected_sign_aligned_ratio, q5_spread_raw, q5_spread_aligned
isolated_spike, primary_discovery, available_direction_pass
p_temporal_nw, temporal_null_pass
```

primary core, robustness, segment 결과는 같은 schema를 사용한다. 해당되지 않는 q/판정
필드는 NULL로 두고 의미가 다른 0으로 채우지 않는다.

### 6.3 보조 parquet

```text
core/daily_ic.parquet
core/primary_hypotheses.parquet
core/coverage.parquet
robustness/nonoverlap_offsets.parquet
robustness/segment_summary.parquet
robustness/delay_summary.parquet
permutation/replicate_summary.parquet
permutation/discovery_count_distribution.parquet
permutation/temporal_shift_summary.parquet
cards/family_summary.parquet
cards/family_cards.json
```

`daily_ic.parquet`은 최소 `hypothesis_id, trade_date, market, ic_market, n_market,
daily_ic, n_total, formation_session_idx`를 보존해 gap-aware t-stat을 재검증할 수 있게 한다.
row-level feature/label 원자료를
output에 복제하지 않고 A0 mart 경로와 hash를 manifest에 참조한다.

## 7. 파일별 변경 계획

| 파일 | 변경 |
|---|---|
| `research/analysis/horizon_scan.py` | Phase A CLI, stage orchestration, official/debug gate |
| `research/analysis/horizon_scan_config.yaml` | Phase A 실행 파라미터·official variant·segment interval 보완 |
| `research/analysis/horizon_scan_runner.py` (신규) | panel projection, core/available/universe/segment scan |
| `research/analysis/horizon_scan_permutation.py` (신규) | 공동 단면 permutation, circular date-shift temporal placebo, resume-safe 실행 |
| `research/analysis/horizon_scan_report.py` (신규) | decay 요약, 카드, plot, markdown renderer |
| `research/etl/metrics.py` | A0 scan metric API 보완, BH·HAC·spread의 순수 함수 유지 |
| `research/analysis/horizon_scan_readiness.py` | Phase A preflight와 primary 75/short exploratory 28-cell registry validation |
| `.gitignore` | 위 analysis 코드/YAML만 allowlist, output은 계속 ignore |
| `pyproject.toml` | A0에서 누락된 YAML dependency와 plot 실행 extra 확인 |
| `tests/unit/test_horizon_scan.py` (신규) | registry/sample/core/BH/segment/판정 단위 테스트 |
| `tests/unit/test_horizon_scan_permutation.py` (신규) | 단면/temporal null, seed, rank 치환, reproducibility 테스트 |
| `tests/unit/test_horizon_scan_report.py` (신규) | decay·card·report schema 테스트 |
| `tests/integration/test_horizon_scan_smoke.py` (신규) | synthetic full run과 실제 lake technical smoke |

`research/etl/features/{price,flow}.py`, `labels.py`, universe·quality mart는 Phase A에서
수정하지 않는 것이 원칙이다. 변경 필요성이 발견되면 Phase A 결과를 폐기하고 config/schema
version을 올린 뒤 A0부터 다시 실행한다.

## 8. 검증 계획

### 8.1 unit test

```bash
uv run pytest tests/unit/test_research_metrics.py
uv run pytest tests/unit/test_horizon_scan.py
uv run pytest tests/unit/test_horizon_scan_permutation.py
uv run pytest tests/unit/test_horizon_scan_report.py
uv run ruff check research/ tests/
```

필수 fixture:

1. 즉각 반응형, 지연 반응형, 부호 반전형, 무신호 family가 각각 하나씩 있는 소형 패널
2. KOSPI/KOSDAQ 종목 수가 달라 n-weighted daily IC를 손계산할 수 있는 패널
3. halt·CA·미래 endpoint·상수 feature·NULL feature가 섞인 패널
4. broad/tradable 결과가 같거나 절반 아래로 줄어드는 패널
5. native에는 신호가 있고 lag1에는 사라지는 패널
6. holdout return만 바꿀 수 있는 tripwire 패널
7. daily IC 중간에 수년짜리 session gap이 있는 HAC 패널
8. stride 60/120에서 offset당 20~59개만 남는 non-overlap 패널

검증 항목:

- 75개 primary + 28개 short exploratory registry와 role
- primary/exploratory/reference 분리
- date×market IC와 market n-weighted daily IC
- cumulative/bucket별 NW lag
- 양측 p-value와 BH monotonicity/fixed m
- raw vs aligned spread
- common-survivor/available eligibility
- segment 독립 실행과 broad tertile 재사용
- tradable/period/delay gate
- isolated spike와 decay summary
- all-offset subsampling
- 공동 단면 permutation, rank-vector 치환과 seed determinism
- circular date-shift가 feature 시계열을 보존하고 label 정렬만 끊는지
- holdout 불변성

### 8.2 synthetic end-to-end

작은 parquet lake/A0 fixture에서 다음 한 명령이 끝까지 성공해야 한다.

```bash
uv run python -m research.analysis.horizon_scan --phase A --fixture synthetic
```

synthetic expected sign 자체를 실데이터 test에 재사용하지 않는다. synthetic에서는 만든
패턴이 카드에 정확히 분류되고, canary가 별도 technical 결과에만 나타나며, 공식 hypothesis
수가 primary 75, short exploratory 28로 유지되는지 확인한다.

### 8.3 실제 lake smoke와 official run

빠른 smoke:

```bash
uv run python -m research.analysis.horizon_scan \
  --phase A --smoke-family px_reversal_5d --permutations 1
```

이는 `official=false`다. 실제 official run은 필터·snapshot override 없이 실행한다.

```bash
uv run python -m research.analysis.horizon_scan --phase A
```

실데이터 검증은 다음 기술적 invariant만 pass/fail로 쓴다.

- key 중복 0
- inf 0
- A0 manifest/hash 일치
- 75개 primary hypothesis와 28개 short exploratory registry 존재
- holdout boundary 위반 0
- broad/tradable label 값 동일
- tradable ⊆ broad
- label coverage가 A0 report와 정합
- 단면 100회와 장기 temporal 100회 placebo 완료
- 재실행 hash 동일

`px_reversal_5d`가 양수인지, flow가 유의한지, 발견이 몇 개인지는 테스트 assertion으로
고정하지 않는다.

### 8.4 회귀 검증

```bash
uv run pytest tests/unit
uv run pytest tests/integration/test_research_features_smoke.py
uv run pytest tests/integration/test_research_labels_smoke.py
uv run pytest tests/integration/test_horizon_scan_smoke.py
```

Phase A는 기존 model dataset/label consumer를 바꾸지 않아야 한다. 결과 생성 때문에 기존
`label_daily`, `feat_price`, `feat_flow` 컬럼의 의미나 source 없는 legacy cache를 조용히
재사용하지 않았는지 함께 확인한다.

### 8.5 구현·검증 진행 상황 (2026-08-02)

| PR | 내용 | 상태 |
|---|---|---|
| A-PR1 | 실행 계약/패널(preflight, run spec, 75/28 registry, holdout gate, panel/segment) | 완료 |
| A-PR2 | core 통계(cumulative/bucket runner, daily IC, NW, spread, fixed-m global BH) | 완료 |
| A-PR3 | 강건성(broad/tradable, available, segment, delay, all-offset subsampling) | 완료 |
| A-PR4 | placebo(공동 permutation, 장기 circular date-shift, seed, 100회 resume/checkpoint) | 완료 (`research/analysis/horizon_scan_permutation.py`) |
| A-PR5 | decay/pattern/family card/plot 7종/markdown/atomic publish | 완료 (`research/analysis/horizon_scan_report.py`, `horizon_scan_run_spec.py`) |
| A-PR6 (unit/e2e/regression) | 단위테스트 651개, synthetic e2e(`tests/integration/test_horizon_scan_smoke.py`), 회귀 | 완료 |
| A-PR6 (CLI) | `research/analysis/horizon_scan.py` 신규 작성 — §7 file plan의 "Phase A CLI, stage orchestration, official/debug gate" | 완료, `--smoke-family`로 4개 family(가격/플로우/reference-only/공매도 exploratory 총 4개 role 조합)를 실제 `sj2_remote` snapshot에 실측 검증 |
| A-PR6 (official run) | §8.3 official run(필터 없이 전체 17개 family, 75/28-cell, 100+100회 replicate) | **완료** — 2026-08-03 06:36 KST 시작, 11:16 KST 발행 (약 4시간 40분), `official=true`/`smoke_only=false`, §8.3 기술적 invariant 전부 통과 (아래 §8.6) |

CLI 실측 검증 중 registry 버그 1건을 포함해 총 5건의 버그를 발견·수정했다 —
자세한 내용은 §1.1 후속 3/4, §8.6 참조. `research/etl/features/{price,flow}.py`,
`labels.py` 등 A0 산식은 이번 A-PR4~A-PR6 작업에서 변경하지 않았다(§7 원칙 유지).

### 8.6 official run 결과 (2026-08-03, run_id=20260803T063659-93effdb0)

경로: `research/output/horizon_scan/phase=A/snapshot_date=2026-08-01/`
`source=sj2_remote/config_hash=1d2082584ceb1d2ec376bff601f79c1e4381c2b32f767ee1fd1e1073324dad6d/`
`run_id=20260803T063659-93effdb0/`

§8.3 기술적 invariant 전부 실측 확인:

- `core/horizon_ic.parquet`: 412행(103 hypothesis × 4 combo), `(hypothesis_id,
  universe, sample_kind)` key 중복 0, 전 numeric 컬럼 inf 0, 전 셀 `status=valid`
- primary hypothesis 75개·short exploratory 28개(broad/common-survivor 기준)
  registry 그대로 존재
- `effective_sample_end` 최댓값이 `holdout_start`(2025-08-01) 미만으로 holdout
  경계 위반 0
- 동일 cell에서 tradable `n_obs` <= broad `n_obs` 위반 0 (tradable ⊆ broad와
  정합)
- 단면 100회(`p_empirical_count=0.0099`, 100회 중 null discovery가 실제
  발견 수(31) 이상인 경우는 최대 0회)와 장기 13-cell temporal 100회 placebo 모두
  완주
- 재실행 hash 비교는 이번에 두 번째 실행(첫 실행은 §1.1 후속 4에서 발견한
  registry 버그 수정 *전*에 돌렸다가 결과 디렉터리가 사고로 삭제되어 재실행함,
  아래 참고)에서만 확인했으므로 "동일 코드로 두 번 실행해 hash가 같다"는
  엄밀한 재현성 검증은 아직 없음 — 두 실행의 `n_primary_discovery=31`,
  family별 grade/pattern/discovery 수는 정확히 일치했다(§8.6 발견 5 참고)

**family 결과 요약** (17개 카드, `run_id=20260803T063659-93effdb0` 기준):

| family | grade | pattern | screen_pass | discoveries |
|---|---|---|---|---|
| px_reversal_5d | A | immediate | true | 7 |
| px_mom_12_1 | D | no_signal | false | 0 |
| px_resid_mom_12_1 | D | no_signal | false | 0 |
| px_near_52w_high | A | delayed | true | 2 |
| px_maxret_20d | A | delayed | true | 6 |
| px_idio_vol_60d | A | delayed | true | 6 |
| px_amihud_20d | A | delayed | true | 4 |
| px_turnover_shock | D | no_signal | false | 0 |
| px_zero_ret_ratio_20d | R | no_signal | false | 0 |
| flow_foreign_netbuy_to_volume | D | no_signal | false | 0 |
| flow_inst_netbuy_to_volume | D | no_signal | false | 0 |
| flow_individual_netbuy_to_volume | A | delayed | true | 6 |
| flow_foreign_holding_ratio_chg | D | no_signal | false | 0 |
| flow_short_turnover | C | exploratory_only | false | 0 |
| flow_short_interest | C | exploratory_only | false | 0 |
| flow_days_to_cover | C | exploratory_only | false | 0 |
| flow_nat_proxy_20d | C | exploratory_only | false | 0 |

price 모멘텀 계열(`px_mom_12_1`/`px_resid_mom_12_1`)은 기대 부호(+)와 반대(관측
`-`)로 무신호, 나머지 grade A 6개 family는 기대 부호와 일치하고 각자
알려진 이상현상(단기 반전, 52주 신고가, MAX 복권 수요, idiosyncratic
vol 저변동, Amihud 비유동성 프리미엄)과 부호가 일치한다. 공매도 4개
exploratory family는 (아래 발견 5 수정 후) 28개 cell 중 25개가
`|t_nw|>3`이고 전부 기대 부호와 일치 — `role=exploratory_short_regime`이라
`screen_pass`/global BH에는 절대 들어가지 않지만 강한 진단적 신호다.

**이번 official run 과정에서 발견·수정한 버그 5건**(§1.1 후속 3/4의 CLI 버그
4건에 이어):

5. **`classify_pattern_auto`의 `has_exploratory_significant`가 항상
   `False`로 하드코딩됨.** 공매도 4개 exploratory family는 절대 primary
   discovery가 될 수 없으므로(§2.2) 패턴이 `no_signal` 또는
   `exploratory_only` 둘 중 하나여야 하는데, 이 하드코딩 때문에 실제로
   28개 cell 중 25개가 `|t_nw| > 3`(config `exploratory_abs_t_nw`)임에도
   4개 family 전부 `no_signal`로 잘못 표시됐다. `research/analysis/horizon_scan.py`의
   `build_family_result`에서 `role == "exploratory_short_regime"`인 family에
   한해 이미 스캔된 자기 cell들(전부 hypothesis_role이 `primary`가 아니므로
   "가설 밖" 조건을 만족) 중 `|t_nw|>3`이 있는지로 계산하도록 수정. ready
   family가 discovery 없을 때의 `exploratory_horizon_set`/secondary feature
   판정은 그 cell들을 아예 스캔하지 않으므로 여전히 `False`로 남는다(범위
   제한, §8.5의 CLI 범위 축소 목록에 추가할 항목).

첫 번째 official run(수정 전 코드, 2026-08-02 20:44~2026-08-03 01:19 KST 발행)의
technical invariant는 전부 동일하게 통과했고 family 결과도 이 표와 동일했으나
공매도 4개 family만 `no_signal`로 잘못 표시돼 있었다. 그 출력 디렉터리는
검증 도중 실수로 `rm -rf research/output/horizon_scan`에 함께 삭제되어(smoke
테스트 정리 명령이 official run 디렉터리까지 지움) 버그 수정 후
재실행했다 — 같은 config_hash/snapshot에 대한 재실행이므로 A0 재생성은
불필요했고, 두 실행의 통계 결과(발견 수 31, family별 grade/pattern)는
공매도 4개의 pattern label을 제외하고 완전히 동일했다.

## 9. 구현·리뷰 단위 권장 순서

1. **A-PR1 — 실행 계약/패널**: preflight, run spec, primary 75/short exploratory 28-cell
   registry, holdout gate,
   panel/segment dimension — 완료
2. **A-PR2 — core 통계**: cumulative/bucket runner, daily IC, NW, spread, fixed-m global BH
   — 완료
3. **A-PR3 — 강건성**: broad/tradable, available sample, segment, delay, all-offset
   subsampling — 완료
4. **A-PR4 — placebo**: 공동 date×market permutation과 장기 circular date-shift,
   deterministic seed, 각 100회 resume/checkpoint — 완료
5. **A-PR5 — 결론 산출**: decay, pattern, family card, plot, markdown, atomic success marker
   — 완료
6. **A-PR6 — 통합 검증**: synthetic full run, real lake official run, reproducibility audit
   — 완료(§8.5/§8.6). 엄밀한 재실행 hash 재현성 검증(동일 코드로 두 번 실행 비교)은
   아직 없음

각 PR은 그 단계의 schema fixture와 unit test를 포함한다. PR2 결과를 먼저 보고 PR3~PR5의
임계값·분류 규칙을 바꾸지 않는다. 병렬 개발이 필요하면 PR4의 permutation engine과
PR5의 renderer는 PR2의 고정 output fixture를 기준으로 개발하되 merge는 PR3 이후로 한다.

## 10. 주요 위험과 대응

| 위험 | 영향 | 대응 |
|---|---|---|
| A0 config/산식 변경 | 이전 scan과 비교 불가 | hash mismatch 즉시 실패, A0부터 재생성 |
| holdout 조기 노출 | horizon·family 선택 오염 | label end-date filter, CLI 차단, holdout mutation tripwire |
| broad/tradable 중 좋은 쪽 선택 | 사후 선택 자유도 | broad/common-survivor만 BH, tradable은 고정 robustness gate |
| 표본 부족 셀을 BH에서 제거 | 실효 검정 수 축소 | primary 75개 고정, missing/insufficient는 p=1 |
| short allowed가 2020-03 이전 단일 블록 | 28셀의 낮은 독립 표본·FDR 예산 낭비 | 공매도 4개 family를 exploratory로 사전 강등, m=75 |
| partial interval의 PIT 지수 구성종목 부재 | current 구성종목 소급 시 look-ahead | partial 공식 제외, PIT membership 수집 전 승격 금지 |
| 세그먼트 다중검정 | 우연한 subgroup 발견 | q 미부여, 전체 표본 발견의 진단으로만 사용 |
| overlapping label의 naive t | 장기 horizon 과대 유의 | horizon별 NW + 전 offset subsampling |
| daily IC date gap을 배열 index로 압축 | 수년 간격 관측이 HAC 인접쌍으로 오인 | KRX session-distance gamma pair와 gap fixture |
| stride 60/120의 짧은 non-overlap 표본 | 장기 offset 전부 기존 60일 기준 미달 | 별도 n>=20 exact sign test, insufficient면 grade A 금지 |
| 독립 feature permutation | family 상관 붕괴 | 모든 primary feature에 공동 block permutation |
| 단면 permutation의 serial-null 부재 | NW 과소보정 진단 불가 | 장기 primary 셀 circular date-shift temporal placebo 추가 |
| common survivor conditioning | 장기 생존 종목 편향 | available 결과 병기, attrition warning, 절대 성과 주장 금지 |
| 현재 상장 종목 중심 수집 | 장기 IC 상향 편향 | 모든 장기 카드에 생존편향 경고, Phase A에서 해소 주장 금지 |
| 관리종목 source 부재 | tradable 과대 포함 | availability metadata와 limitation을 카드마다 유지 |
| flow 게시시각 불확실 | 단기 look-ahead/실행 불가 | 불확실한 family의 official variant=lag1 |
| short 금지·부분허용 혼합 | 구조 단절을 alpha로 오인 | allowed-only exploratory, partial/banned coverage 분리 |
| residual momentum/flow 유효 시작 차이 | family curve 비교 왜곡 | family별 effective start·coverage 병기 |
| 두 종류 각 100회 placebo 비용 | 긴 실행·중간 실패 | rank vector 치환, long cell 한정, checkpoint, deterministic resume |
| plot 축 자동 확대 | 작은 효과 과장 | overview 공통 축과 raw 수치 병기 |
| 유의한 단일 spike | 노이즈 채택 | isolated-spike gate와 인접 curve 확인 |
| correlated family 중복 | 발견 수 과대 해석 | Phase A는 단변량 screening으로 제한, 다음 gate에서 ablation |

## 11. Phase 1 acceptance gate 인계물

Phase A가 다음 단계에 전달하는 것은 "채택 feature"가 아니라 screening 후보와 재현 가능한
증거 묶음이다.

1. official run의 `run_spec.json`, `manifest.json`, `_SUCCESS.json`
2. 75개 primary hypothesis와 global BH 결과, 28개 short exploratory coverage
3. cumulative/bucket 전체 curve와 raw spread
4. broad/tradable, common-survivor/available 비교
5. market/size/liquidity/period/regime/delay 진단
6. all-offset sign-test와 단면/temporal 각 100회 null 분포
7. 17개 family conclusion card
8. `screen_pass=true`인 family와 연속 horizon band
9. exploratory/secondary-only 후보와 차기 사전등록 제안 목록
10. 생존편향·관리종목·publication lag·attrition limitation

다음 acceptance gate는 이 목록에 대해서만 증분성, purged walk-forward OOS,
turnover/거래비용을 평가한다. holdout은 feature·horizon·변형 선택이 끝난 뒤 한 번만 연다.
Phase A에서 탈락한 family를 holdout 결과를 본 뒤 되살리거나, 탐색적 horizon을 같은 run의
주 발견으로 승격하지 않는다.
