# 08. Phase B 구현 진행 기록 (B-PR1 ~ B-PR15)

## 0. 목적과 범위

이 문서는 [04_specific_plan_B.md](./04_specific_plan_B.md) §6(작업 패키지 B-0~B-10)을
§10이 정한 리뷰 단위(B-PR1~B-PR9, +B-PR10, +B-PR11, +B-PR12, +B-PR13, +B-PR14, +B-PR15)로
나눠 실제로 구현한 내용을 기록한다. plan 문서 자체는 사전등록 계약이라 그대로 두고, 이 문서에는
각 PR이 만든 파일, 핵심 설계 결정·편차, 테스트 개수를 남긴다. 다음에 이어서 작업할 때(같은
사람이든 다른 사람이든) 바로 파악할 수 있게 하는 게 목적이다.

기준 시점: 2026-08-10, 브랜치 `refactor/parquet-compute-reproducible`.
`uv run pytest tests/unit` 819개 통과, 회귀 없음. (최신 기준선은 아래 2026-08-12 항목.)

2026-08-11 재확인: 같은 브랜치에서 `uv run pytest tests/unit -q` 819개 그대로 통과.
이 문서가 처음 저장된 뒤(2026-08-10 13:59) 실제로 Phase A 재실행·Phase AB 결합 run이
발행됐다 — 발행물 목록과 지금 무엇이 막혀 있는지는 §3, 이어서 할 일은 §4를 보면 된다.
Phase B 코드는 **아직 커밋되지 않았다**(전부 working tree 상태).

2026-08-12 갱신: §4.1의 1번(커밋+릴리즈)은 끝났다 — `b006a19`·`0581912` 커밋 뒤 v0.9.0~v0.9.2를
릴리즈했고 prod compose도 갱신됐다. 2번(prod 백필)이 진행 중이며, 그 과정에서 capital action
list의 vintage 중복 문제가 드러나 plan §4.4.1이 계약 보강으로 추가됐다. 현재 상태와 대기 중인
측정은 §4.1.1을 보면 된다.

**2026-08-12 세션 종료 시점** — `uv run pytest tests/unit -q` **939개 통과**, lint 통과
(`research/analysis/fin_vs_price_corr/`의 기존 미해결 건 제외). 이날 커밋 10개는 `00` §4b에
표로 있다. 코드 관점의 큰 변화는 셋이다.

1. **B-10이 Stage 3만 남았다.** Stage 2(진단 7종)·4(family 카드)·5(run 리포트 2종)를 넣고
   전부 run 디렉터리에 연결했다. §4.3 참고. → **Stage 3의 `daily_ic.parquet`은 2026-08-29에
   단계 0으로 완료됐다. `cohort_ic.parquet`은 SUE 표본이 없어 미작성.**
2. **B-2 결함 3건을 고쳤다.** Stage 2 진단을 실제 lake에 돌리자마자 나왔다. §4.3.2.
3. **B-9의 마지막 미결 항목이 닫혔다.** source 비치명 경고가 grade A 상한으로 연결됐다.
   §4.2.

**다음 세션은 `00_status.md` §0부터 읽는다** — 백그라운드 수집이 끝났는지 확인하는 명령과,
끝났을 때 어느 순서로 진행하는지가 거기 있다.

**2026-08-24 최신 기준.** 8월 23일 snapshot으로 native scan·Phase A artifact 재사용 경로를
적용한 A → B → AB official run이 다시 완주했다. 최신 결과는 §3.0이며, §3.0a 이하의 8월
12~13일 결과는 이전 계약의 실행 기록으로 읽는다. 8월 27일 `tests/unit`은 1,343개 통과했다.

## 1. 전체 상태 요약

| PR | 대응 작업 패키지 | 상태 |
|---|---|---|
| B-PR1 | B-0 config 확장·preflight | 완료 |
| B-PR2 | B-1 filing receipt·capital action raw | 완료 |
| B-PR3 | B-2 `stock_metric_vintage_fact` | 완료 |
| B-PR4 | B-3 standalone quarter·TTM (`fin_quarterly_metric_vintage`) | 완료 |
| B-PR5 | B-4 `feat_fin_scan_daily` | 완료 |
| B-PR6 | B-5 issuance·payout (`feat_event_scan_daily`) | 완료 |
| B-PR7 | B-6 SUE event mart (`fin_sue_event`) | 완료 |
| B-PR8 | B-7 continuous·event core scan + B-9 결합 BH 핵심부 | 완료 |
| B-PR9 | B-8 강건성·null 실험 + B-9 나머지 + B-10 | **부분 완료** — B-8의 screen_pass 하드게이트 부분만 완료, 나머지는 §4 "남은 작업" 참고 |
| B-PR10 | B-9 step 1-2 (실제 lake 연결 + 결합 BH) | 완료 |
| B-PR11 | B-9 screen_pass 9개 조건 통합 판정 | 완료 |
| B-PR12 | B-9 evidence grade(A/B/C/D/NE) 결정 로직 | 완료 |
| B-PR13 | rank-correlation 진단(B-9) + SUE event-ordinal non-overlap(B-8) | 완료 |
| B-PR14 | 결합 단면 permutation(B-8) | 완료 |
| B-PR15 | B-10 Stage 1 — readiness_matrix + robustness summary 아카이브 | 완료 |

B-PR1~15 전부 실데이터로 돌았다. 최신 Phase B 38셀은 `screen_pass=12`, grade
**A=5, B=7, C=25, D=1**이다. Phase A 75개와 합친 discovery는 56개다(§3.0).

## 2. B-PR별 상세

### B-PR1 — contract (B-0)

**만든/고친 파일**
- `research/analysis/horizon_scan_config.yaml` — `schema_version: 4`, `phase_b:` 섹션 추가(최대 38 candidate, event bucket 등)
- `research/analysis/horizon_scan_config.py` — `_PHASE_B_FIXED_PROTOCOL_VALUES`, `_PHASE_B_EVENT_BUCKETS` 검증 로직
- `research/analysis/horizon_scan_phase_b.py` (신규) — candidate registry(`phase_b_candidate_cells`/`build_phase_b_candidate_registry`), readiness freeze(`build_phase_b_readiness_rows`/`write_phase_b_readiness_freeze`), run spec(`build_phase_b_run_spec`), receipt-value pairing preflight(`check_receipt_value_pairing`)
- `research/analysis/horizon_scan_run_spec.py` — `content_hash_exclude_names` 옵션 추가(하위호환)
- `tests/unit/test_horizon_scan_phase_b.py` (신규, 23개)

**핵심 결정**
- readiness 판정은 "outcome-blind" — label/return/IC/p-value를 읽는 파라미터 자체가 존재하지 않도록 구조적으로 강제(테스트로 고정).
- 연속 32 + SUE 6 = 38 candidate 정확히 일치를 config validate 단계에서 검증.

### B-PR2 — filing receipt·capital action raw (B-1)

**만든/고친 파일**
- `sql/postgres_ddl.sql` — `dart_filing_receipt_raw`, `dart_capital_change_raw` 신규 테이블
- `src/krx_collector/domain/enums.py`, `domain/models.py` — 신규 RunType 3종, 도메인 모델 추가
- `src/krx_collector/ports/filing_receipt.py` (신규), `ports/share_info.py`(`CapitalChangeProvider`)
- `src/krx_collector/adapters/opendart_filings/` (신규) — `OpenDartFilingReceiptProvider`
- `src/krx_collector/adapters/opendart_share_info/provider.py` — `fetch_capital_change()`
- `src/krx_collector/adapters/opendart_common/policy.py`/`__init__.py` — `CAPITAL_CHANGE_POLICY`, `FILING_RECEIPT_POLICY`
- `src/krx_collector/service/sync_dart_filings.py` (신규), `service/sync_dart_share_info.py`, `service/sync_dart_xbrl.py`(`sync_dart_xbrl_receipt_targeted`)
- `src/krx_collector/infra/db_postgres/repositories.py`, `remote_sync.py` — upsert/조회 메서드, `TableSyncSpec` 2건 추가
- `src/krx_collector/cli/app.py` — `dart sync-filings`, `dart backfill-xbrl-receipts` 서브커맨드
- `src/krx_collector/service/profiling/catalog.py`, `research/etl/config.py`, `bin/raw-parquet-export-all.sh` — 신규 raw 테이블 등록
- `docs/database.md`(13/14절 신규, 15절 재번호), `docs/operations.md`, `CLAUDE.md`
- 테스트: `test_opendart_share_info.py`, `test_opendart_xbrl.py`(확장), `test_opendart_filings.py`(신규)

**핵심 결정**
- 원공시(original) 판정은 `dart_filing_receipt_raw`의 `report_nm` "정정" 포함 여부로만 하고, raw에 남은 최소 rcept_no를 원공시로 간주하지 않는다(§3.5 원칙의 근거가 되는 raw).

**부수 수정(요청에 따라)**
- `tests/integration/test_profiling_pipeline.py` — 이번 세션과 무관한 기존 버그(폐기된 `metric_catalog`/`operating_metric_fact` 참조)를 발견해 고침. `git stash`로 세션 이전부터 있던 버그임을 확인.
- `test_remote_db_sync.py`, `test_research_config.py` — 신규 raw 테이블 2개 추가로 하드코딩된 개수(13→15, 12→14) 갱신.

### B-PR3 — vintage foundation (B-2)

**만든 파일**
- `research/etl/marts/metric_vintages.py` (신규) — `stock_metric_vintage_fact`. grain `(ticker, metric_code, statement_period_end, fs_basis, rcept_no)`, `rcept_no`를 독립 컬럼으로 보존(legacy `metrics_normalize.py`는 승자 하나로 collapse).
- `tests/unit/test_metric_vintages.py` (신규, 9개)

**핵심 결정**
- `metrics_normalize.py`(golden-parity 고정 코드)와 코드 공유하지 않음 — 매핑 규칙(`metric_rules.py`)만 재사용, SQL은 독립 작성. Phase B의 모든 mart가 이 원칙을 따른다.
- `complete_original_and_revisions` 상태는 절대 내지 않음(전체 receipt 스캔 없이는 "더 이상 정정 없음"을 증명 불가).

### B-PR4 — standalone quarter·TTM (B-3)

**만든 파일**
- `research/etl/marts/financial_quarters.py` (신규) — `fin_quarterly_metric_vintage`. `direct_interim`/`cumulative_reported`/`instant`/`weighted_share` 4종 metric kind별 분기 환산 + TTM.
- `tests/unit/test_financial_quarters.py` (신규, 10개)

**핵심 결정**
- "N분기 전" 값은 `LAG(n)`이 아니라 정확한 `seq_key` self-join으로 구함(결측 분기가 있을 때 `LAG`이 잘못된 분기를 끌어오는 문제를 원천 차단).

### B-PR5 — daily financial feature (B-4)

**만든 파일**
- `research/etl/features/fin_scan.py` (신규) — `feat_fin_scan_daily`. 규모/가치/수익성/자산성장/발생액 5개 family.
- `tests/unit/test_research_fin_scan.py` (신규, 8개)

**핵심 결정**
- "날짜당 하나의 fs_basis" 원칙 — `net_income`이 CFS 값을 가지면 그 날짜의 모든 metric을 CFS로, 아니면 OFS로 통일 선택(accruals처럼 여러 metric을 한 식에 섞을 때 basis가 섞이지 않게).

### B-PR6 — issuance·payout (B-5)

**만든 파일**
- `research/etl/features/event_scan.py` (신규) — `feat_event_scan_daily`. `irdsSttus` 발행/소각 사유 분류, 배당 소스 정규화.
- `tests/unit/test_research_event_scan.py` (신규, 9개)

### B-PR7 — SUE event mart (B-6)

**만든 파일**
- `research/etl/features/sue_event.py` (신규) — `fin_sue_event`. grain `(ticker, original_rcept_no, event_formation_date, market)` — Phase B에서 유일하게 event-time인 mart.
- `tests/unit/test_research_sue_event.py` (신규, 5개)

**핵심 결정 / 발견한 버그**
- comparative EPS는 §4.6의 1차 방법(같은 filing 안 "전년 동기" XBRL context 재구성)이 아니라 2차 방법(`as_was_lag4q`, B-3의 `value_lag_4q`)만 사용 — 이 리포의 XBRL 파싱이 concept당 값 1개만 캡처하는 구조상 1차 방법은 불가능.
- **실제 버그**: `weighted_avg_shares`는 XBRL 기반이라 B-2에서 항상 `fs_basis=''`인데, `controlling_net_income`은 실제 `CFS`/`OFS` 값을 가진다. 처음 짠 SQL이 두 metric을 `fs_basis` 일치로 조인해 영원히 매칭이 안 되는 버그였음 — shares 쪽 조인에서 `fs_basis` 조건을 제거해 해결.
- 60세션 정정 오염 체크는 캘린더 일수가 아니라 `d_idx`(세션 인덱스) 거리로 판정.

### B-PR8 — continuous·event core scan + 결합 BH 핵심부 (B-7 + B-9 일부)

**만든 파일**
- `research/analysis/horizon_scan_phase_b_scan.py` (신규)
- `tests/unit/test_horizon_scan_phase_b_scan.py` (신규, 15개)
- `horizon_scan_config.yaml`에 §5.4 SUE 코호트 파라미터 추가(`min_events_per_market_contribution: 10`, `min_events_per_cohort_total: 30`, `min_event_cohorts: 8` 등)

**핵심 결정**
- 연속 7개 family(32셀)는 Phase A의 `scan_cell`/`run_registry_scan`을 **그대로 재사용** — `feat_fin_scan_daily`/`feat_event_scan_daily`를 기존 `analysis_panel`에 LEFT JOIN한 `analysis_panel_phase_b` 뷰만 새로 만들면 됨.
- SUE(6셀)는 grain이 달라 새 코호트 알고리즘 구현: `(event_formation_date, market)` 안에서 tie-aware 퍼센타일 순위 변환 → market당 10개 이상일 때만 풀링 → 날짜별 풀링 30개 이상일 때 그 날짜의 Spearman IC 계산 → 날짜별 IC 시퀀스에 gap-aware NW(글로벌 세션 인덱스 기준). 결과 필드명은 연속 셀과 통일(`n_dates`=코호트 수, `n_obs*`=코호트-market 그룹 크기).
- `apply_phase_b_only_bh`/`apply_combined_ab_bh` — 기존 `apply_global_bh`를 그대로 재사용(hypothesis_id 기반이라 A/B 구분 없이 동작).

### B-PR9 — 강건성·null 실험(부분) + B-9 나머지 + B-10 (진행 중)

**만든 파일**
- `research/analysis/horizon_scan_phase_b_robustness.py` (신규)
- `tests/unit/test_horizon_scan_phase_b_robustness.py` (신규, 13개)
- `horizon_scan_phase_b_scan.py` 리팩터 — `_pool_cohort_ranks`/`_aggregate_cohort_rows`로 분리(동작 동일, 기존 15개 테스트 그대로 통과). bootstrap이 "코호트 수 미달" 게이트 없이 같은 순위·풀링 로직을 재사용하기 위함.
- `horizon_scan_config.yaml`에 `nonoverlap_min_dates` override, bootstrap 반복수/시드(999회), `event_cluster_confirm_p_max: 0.10` 추가

**완료된 부분 (screen_pass 하드게이트)**
- 연속 non-overlap offset 진단 — Phase A `run_nonoverlap_offsets` 재사용, h=120 계열만 `nonoverlap_min_dates` 20→12 override. 판정: `valid_offset_ratio>=0.80` AND `offset_sign_agreement_ratio>=0.60`.
- 연속 temporal placebo — Phase A `run_temporal_placebo`가 panel/registry에 완전히 독립적이라 **수정 없이 그대로 호출**.
- SUE issuer cluster bootstrap / filing-cycle block bootstrap (신규 구현, Phase A에는 대응 코드 없음) — 티커 또는 `(bsns_year,reprt_code)` 단위 복원추출 → `_pool_cohort_ranks` 재사용해 리샘플 코호트 IC 재계산 → percentile-method 양측 bootstrap p-value로 확인. 시드는 `derive_replicate_seed` 재사용(재시작 가능).

### B-PR10 — Phase B 실행 파이프라인 + A+B 결합 BH 연결 (B-9 step 1-2)

**만든/고친 파일**
- `research/analysis/horizon_scan_phase_b_run.py` (신규) — `register_phase_b_marts`(5개 mart를 의존 순서대로 best-effort materialize, `duckdb.Error`는 "이 mart는 지금 못 만든다"로 흡수), `run_phase_b_core`(readiness freeze → continuous/event scan → phase-B-only BH → `phase=B/.../run_id=.../` atomic publish), `load_phase_a_primary_rows`(§2.3 rule 5: config_hash·content_hash·75-id population 검증), `run_combined_ab`(Phase A+B 결합 BH → `phase=AB/.../run_id=.../` atomic publish)
- `research/analysis/horizon_scan_phase_b_scan.py` — `build_phase_b_panel_sql`/`register_phase_b_panel`이 `fin_scan_view`/`event_scan_view`를 `None`으로 받아 해당 LEFT JOIN을 생략할 수 있도록 확장(한쪽 mart만 materialize된 부분가용 상태에서 다른 쪽 family까지 덩달아 막히지 않게)
- `research/analysis/horizon_scan.py` — `--phase {A,B,AB}` CLI 확장(`B`/`AB`는 신규 모듈로 위임)
- `tests/unit/test_horizon_scan_phase_b_run.py` (신규, 10개), `test_horizon_scan_phase_b_scan.py`에 panel None-처리 테스트 2개 추가
- `tests/integration/test_horizon_scan_phase_b_smoke.py` (신규) — 실제 로컬 lake 대상 self-skip 통합 테스트

**핵심 결정**
- 디렉터리 nesting은 plan §7.1 ASCII 다이어그램(`config=<hash>/phase=B/`)이 아니라 `run_phase_a`가 실제로 쓰는 `phase=<X>/snapshot_date=/source=/config_hash=/run_id=/` 순서를 그대로 따름 — plan 문서가 `run_phase_a` 구현보다 먼저 쓰여 순서가 어긋나 있었음.
- **중요한 실제 데이터 상태**: 로컬 lake에 이번 세션에 추가한 `dart_filing_receipt_raw`/`dart_capital_change_raw` parquet가 아직 없음(prod 수집·export 전) — `stock_metric_vintage_fact`가 `dart_filing_receipt_raw`를 참조하므로 오늘은 Phase B 38개 후보 전부 `blocked_exploratory`(`M_B_ready=0`)로 정상 동결됨. 코드 버그가 아니라 outcome-blind 설계가 의도한 그대로이며, prod가 두 raw 테이블을 수집하면 코드 변경 없이 일부 셀이 ready로 전환된다.
- **또 다른 실제 상태** (→ 2026-08-10 재실행으로 **해소됨**, §3.1 참고): 당시 이미 발행된 유일한 Phase A 공식 run(`run_id=20260803T063659-93effdb0`)은 이 세션에서 `horizon_scan_config.yaml`에 Phase B 섹션을 추가하기 **이전**에 실행된 것이라, 그 run의 `run_spec.json["config_hash"]`가 지금 로드한 config의 `config_hash`와 다르다. `load_phase_a_primary_rows`는 이걸 정확히 거부한다(§2.3 "Phase A 결과를 본 뒤 B family/horizon을 바꾼 config는 confirmatory 결합 검정으로 인정하지 않는다") — 실제 새 confirmatory 결합 결과를 얻으려면 현재 config로 Phase A를 다시 공식 실행해야 하고, 이는 이번 PR의 범위 밖(수 시간 걸리는 별도의 의도적 실행)이다.
- `register_phase_b_marts`의 try/except 단위는 mart 하나씩 — `dart_capital_change_raw`만 없고 `dart_filing_receipt_raw`는 있는 경우 `feat_event_scan_daily`만 못 만들고 나머지(fin_scan/sue_event)는 정상 진행되는 걸 unit test로 고정.
- **실제 lake로 처음 돌려서 발견한 버그**: `register_phase_b_panel`은 기존 `analysis_panel` 뷰 위에 LEFT JOIN하는데, 처음 구현에서 `run_phase_b_core`가 `analysis_panel`을 등록하지 않았고 `available_assets`에도 `dim_stock_pit_daily`/`dim_price_quality_daily` 2개만 넣어서 `label_scan`(및 나머지 A0 mart)이 항상 "없음"으로 잡혀 모든 continuous family가 실제 이유와 무관하게 blocked 처리됐다. `REQUIRED_A0_MARTS`(7개) 전체 등록 + `register_analysis_panel(con)` 호출 + `assert_a0_manifest_matches` preflight로 고쳤고, 실제 lake에서 재확인해 `missing_dependencies`가 이제 `dart_filing_receipt_raw`/`dart_capital_change_raw`와 그 하위 mart로만 정확히 좁혀지는 걸 확인했다. 이 버그는 unit test(synthetic mart stub)로는 안 잡히고 실제 lake 통합 테스트로만 잡혔다 — synthetic 테스트가 `analysis_panel`을 미리 만들어주는 방식이었기 때문.

### B-PR11 — screen_pass 9개 조건 통합 판정 (B-9 §9)

**만든/고친 파일**
- `research/analysis/horizon_scan_phase_b_scan.py` — `compute_phase_b_period_sign_pass`(규칙 4: n<=1 하드 실패, Phase A `compute_period_sign_pass`와 다른 부분), `compute_phase_b_screen_pass`(9개 규칙 통합, Phase A `horizon_scan_report.compute_screen_pass`와 같은 모양)
- `research/analysis/horizon_scan_phase_b_run.py` — `compute_phase_b_gate_updates`(신규, `run_phase_b_core`에서 분리) — 규칙 3(tradable)/5(available)는 4-combo persist 후 `compute_tradable_pass`/`compute_available_direction_pass`(Phase A `horizon_scan_runner.py`) 재사용, 규칙 4는 Phase B 전용 period-segment view(`_register_phase_b_period_segment_view`/`_compute_phase_b_period_ics`, Phase A `horizon_scan.py`의 동명 함수를 순환참조 회피 위해 복제) 신규 등록, 규칙 7/8은 B-PR9가 이미 만든 `horizon_scan_phase_b_robustness.py`의 오케스트레이션되지 않은 함수(`run_phase_b_continuous_nonoverlap`/`run_phase_b_temporal_placebo`/`run_issuer_cluster_bootstrap`/`run_filing_cycle_block_bootstrap`/`evaluate_sue_cluster_confirmation`)를 처음으로 실제 ready 셀에 오케스트레이션. `run_phase_b_core`는 continuous 4-combo 전체를 `horizon_ic.parquet`에 쓰도록 변경(Phase A `run_phase_a`의 `output_rows` 패턴과 동일 — discovery combo만 BH/게이트 필드를 가짐).
- `run_combined_ab`에 규칙 1(`q_fdr_global_ab<0.10`+sign, `primary_discovery_ab`에 이미 folded됨) 계산과 최종 `compute_phase_b_screen_pass` 호출 추가 — `combined_ab_primary_hypotheses.parquet`에 `screen_pass`/`failed_gates` 컬럼, `manifest.json`에 `phase_b_screen_pass_count` 추가. Phase A의 75개는 이 함수 대상이 아님(family 카드에 자체 screen_pass가 있음).
- `tests/unit/test_horizon_scan_phase_b_scan.py`(+13, `compute_phase_b_period_sign_pass`/`compute_phase_b_screen_pass`), `tests/unit/test_horizon_scan_phase_b_run.py`(+3, `compute_phase_b_gate_updates`를 mock으로 wiring만 검증 + 기존 `run_combined_ab` happy-path 테스트에 screen_pass 검증 추가), `tests/integration/test_horizon_scan_phase_b_smoke.py`(+1, `run_combined_ab`를 실제 lake 대상으로도 실행)

**핵심 결정**
- `compute_phase_b_gate_updates`를 `run_phase_b_core`에서 분리한 이유는 순전히 테스트성 — 실제 DuckDB 연결·disk 기반 `LakeConfig` 없이 이미 등록된 `con` + synthetic scanned_rows만으로 유닛테스트하기 위함(B-PR10과 같은 이유로 `register_phase_b_marts`의 전체 materialize 체인을 unit test에서 재현하지 않기로 한 결정과 일관됨). 실제 게이트 계산 로직(`run_phase_b_continuous_nonoverlap` 등 호출)은 monkeypatch로 검증하고, combo 추출·병합 로직만 실제로 실행해 검증한다.
- 규칙 1(`q_fdr_global_ab`)은 결합 q-value가 필요해 `run_combined_ab`에서만 계산 가능 — 나머지 8개는 `run_phase_b_core`가 `phase_b_primary_hypotheses.parquet`에 미리 계산해 저장해두고, `run_combined_ab`는 그 컬럼을 그대로 읽어 `apply_global_bh`가 모든 입력 필드를 그대로 복사해준다는 점을 이용한다(추가 merge 불필요).
- 규칙 9(CA/availability/holdout 정책 일치)는 오늘 시점엔 항상 참으로 둔다 — `run_phase_b_core`/`run_combined_ab` 둘 다 디버그/holdout 오버라이드를 아예 노출하지 않으므로(B-PR10에서 의도적으로 안 만듦) 정책이 어긋날 방법 자체가 없다. 나중에 디버그 오버라이드가 생기면 그때 진짜 게이트가 된다.
- 오늘 실제 lake는 `M_B_ready=0`이라 이번에 추가한 오케스트레이션 코드(period segment, nonoverlap+temporal placebo, SUE cluster bootstrap)가 전부 `if continuous_scanned_rows:`/`if event_scanned_rows:` 가드에 걸려 실행되지 않는다 — 실제 lake 통합 테스트로는 "크래시 안 함"만 확인 가능하고, 실제 로직 검증은 mock 기반 unit test(`compute_phase_b_gate_updates`)로 했다.

### B-PR12 — evidence grade A/B/C/D/NE 결정 로직 (B-9 §9)

**만든/고친 파일**
- `research/analysis/horizon_scan_phase_b_scan.py` — `PIT_INDUSTRY_CAPPED_FAMILIES`(`{fin_value_z, fin_gross_profitability, fin_accruals_to_assets}` 상수), `compute_phase_b_evidence_grade`(Phase A `assign_evidence_grade`와 같은 순서: role 게이트 → available sign flip/robustness 실패는 C로 우선 라우팅 → screen_pass 분기(offset 전부 evaluable + PIT-capped family 아님 + 유효 기간 2개 아님 → A, 그 외 screen_pass면 B) → 그 외 D)
- `research/analysis/horizon_scan_phase_b_run.py` — `compute_phase_b_gate_updates`가 non-overlap 결과에서 `offset_status`("complete"/"some_insufficient")도 같이 persist하도록 한 줄 추가. `run_combined_ab`에 `compute_phase_b_screen_pass` 호출 직후 `compute_phase_b_evidence_grade` 호출 추가 — `combined_ab_primary_hypotheses.parquet`에 `evidence_grade` 컬럼, `manifest.json`에 `phase_b_evidence_grade_counts`(`{"A":n,"B":n,"C":n,"D":n}`) 추가.
- `tests/unit/test_horizon_scan_phase_b_scan.py`(+8, `compute_phase_b_evidence_grade` 각 등급/캡 케이스), `tests/unit/test_horizon_scan_phase_b_run.py`(offset_status 필드 검증 1줄 추가 + `run_combined_ab` happy-path에 evidence_grade/등급 카운트 검증 추가), `tests/integration/test_horizon_scan_phase_b_smoke.py`(manifest에 `phase_b_evidence_grade_counts` 키 존재 확인 1줄 추가)

**핵심 결정**
- `failed_gates`(B-PR11의 `compute_phase_b_screen_pass` 출력)를 그대로 재사용해 "왜 실패했는지"를 판정 — `"available_direction_pass"`/`"robustness_pass"`가 실패 목록에 있으면 D가 아니라 C로 분류(Phase A가 `available_sign_flip`을 screen_pass 분기보다 먼저 체크하는 것과 같은 우선순위).
- **정직하게 남긴 제약**: plan이 "B" 사유로 나열한 industry/source/segment/offset 4가지 중 **offset**(non-overlap 진단)만 실제 데이터가 있다. source(mapping_fallback_ratio 등)/segment 진단은 아직 아무 데도 구현이 안 돼 있어(§7.1에 필드만 정의됨, B-8 나머지·B-10 항목) 이번 "비치명 경고" 판정에는 반영하지 않았다 — 없는 신호를 있는 척 만들지 않고, 그 진단들이 생기면 그때 추가하도록 코드 docstring에 명시.
- PIT-industry 상한 대상 family는 config에 마커 필드가 없어 코드 상수로 하드코딩(§9 근거를 주석에 남김) — 나중에 PIT industry가 실제로 생기면 이 상수/캡 자체를 제거해야 함.

### B-PR13 — rank-correlation 진단(B-9) + SUE event-ordinal non-overlap(B-8)

**만든/고친 파일**
- `research/analysis/horizon_scan_phase_b_diagnostics.py`(신규) — `compute_phase_b_rank_correlation`(Phase A ready continuous family × Phase B ready continuous family의 primary feature 쌍마다 날짜×시장 Spearman 상관을 `research/etl/metrics.py`의 기존 `per_date_market_rank_ic`/`daily_market_weighted_ic`로 계산 — 신규 통계 로직 없음, SQL eligibility만 신규), `run_sue_event_ordinal_nonoverlap`(SUE ready 셀의 pooled cohort를 formation-date ordinal로 `event_ordinal_nonoverlap_stride`(신규 config, 3)개 subsample로 쪼개 `_pool_cohort_ranks`/`_aggregate_cohort_rows`(B-PR8/9가 이미 만든 함수)로 재집계, `compute_nonoverlap_robustness_pass`(B-PR9)로 게이트 판정 — screen_pass 하드게이트 아님, 카드/진단 정보).
- `research/analysis/horizon_scan_config.yaml` — `phase_b.event_ordinal_nonoverlap_stride: 3` 추가(사전등록값, 근거는 B-PR9의 다른 non-overlap 값들과 나란히 주석에 남김).
- `research/analysis/horizon_scan_phase_b_scan.py` — `compute_phase_b_evidence_grade`에 `n_independent_filing_windows`/`grade_a_min_independent_filing_windows` 파라미터 추가(§6 B-8 SUE 5번 "코호트가 부족하면 grade A 금지" — `n_independent_filing_windows`는 B-7 core scan(`scan_event_cohort_cell`)이 이미 계산해두던 값이라 새 통계 불필요, 기존 config `grade_a_min_independent_filing_windows`(20)와 비교만 추가).
- `research/analysis/horizon_scan_phase_b_run.py` — `compute_phase_b_gate_updates`의 SUE 루프에 `run_sue_event_ordinal_nonoverlap` 호출 추가(`event_ordinal_nonoverlap_pass`/`event_ordinal_offset_status` 저장, `robustness_required`/`robustness_pass`는 안 건드림 — 그건 여전히 규칙 7/8 cluster confirmation 전용). `run_phase_b_core`에 rank-correlation 계산 + `core/primary_feature_rank_correlation.parquet` 저장 추가(ready continuous family가 없으면 파일 자체를 안 씀). `run_combined_ab`의 `compute_phase_b_evidence_grade` 호출에 filing-windows 인자 전달 + Phase B 산출물의 rank-correlation parquet를 phase=AB로 그대로 복사(재계산 없음).
- 테스트: `tests/unit/test_horizon_scan_phase_b_diagnostics.py`(신규, 7개), `test_horizon_scan_phase_b_scan.py`(+3, filing-windows 캡), `test_horizon_scan_phase_b_run.py`(+4, SUE ordinal mock 배선 + rank-correlation 파일 복사/스킵 2케이스), `test_horizon_scan_phase_b_smoke.py`(+2, rank-correlation 파일 존재가 ready_continuous 상태와 일치하는지, phase=AB 복사 일관성).

**핵심 결정**
- rank-correlation의 eligibility 조건(`in_broad`/`NOT ca_mask`/`common_formation_120d`/`common_survivor_120d`)은 `horizon_scan_runner.build_formation_sql`을 그대로 재사용하지 못했다 — 그 함수는 항상 horizon-shift된 label 컬럼과 조인하는 구조라 "동시점 feature-vs-feature" 상관에는 안 맞음. 대신 같은 조건을 새 SQL에 그대로 복사해 재사용(로직은 동일, 함수는 새로 씀).
- 대상 pair는 Phase A ready continuous family(12개) × Phase B **ready**(blocked 아닌) continuous family의 primary feature만 — SUE는 grain이 daily가 아니라 제외.
- SUE ordinal non-overlap의 stride 값(3)은 사전등록 필요한 새 파라미터라 config에 명시적으로 추가(다른 diagnostic 임계값처럼 코드에 숨기지 않음).
- 이번 PR도 오늘 실제 lake `M_B_ready=0`이라 두 진단 모두 실행 경로만 검증되고(크래시 없음, 파일 유무 일관성), 실제 값 계산은 mock/synthetic 테스트로 검증했다.

### B-PR14 — 결합 단면 permutation (B-8)

**만든/고친 파일**
- `research/analysis/horizon_scan_phase_b_scan.py` — `_pool_cohort_ranks`의 날짜별 pooling 뒷부분을 `_pool_qualifying_by_date(qualifying, *, min_events_per_cohort_total)`로 추출(순수 리팩터, 동작 동일 — 기존 SUE/bootstrap/ordinal 테스트 그대로 통과). 실제 `qualifying` 프레임을 이미 갖고 있는 permutation 루프가 SQL을 다시 안 날리고 rank만 다시 섞어 재풀링할 수 있게 하기 위함.
- `research/analysis/horizon_scan_phase_b_joint_permutation.py`(신규) — `_permute_qualifying_sue_ranks`(`(event_formation_date, market)` 그룹 안에서만 `sue_pctrank`를 섞고 `excess_pctrank`는 고정 — "frozen rank vector 치환"), `_scan_sue_null_row`(SUE 셀 하나의 replicate 1개 null row, 최소 코호트 수 게이트 없음 — bootstrap/ordinal과 같은 원칙), `run_combined_cross_sectional_permutation`(continuous는 Phase A `run_cross_sectional_permutation`의 fetch-once/permute-and-rescan 부품(`fetch_broad_common_survivor_frame`/`permute_within_groups`/`_scan_registry_once`)을 그대로 재사용, SUE는 `_pool_cohort_ranks`로 `qualifying`을 한 번만 얻고 매 replicate `_permute_qualifying_sue_ranks`+`_pool_qualifying_by_date`+`_aggregate_cohort_rows`로 재집계, 둘을 합쳐 `apply_global_bh` 한 번으로 replicate당 discovery count 산출. `p_empirical_count`는 여기서 계산하지 않음 — 아직 실제 발견 수를 모름).
- `research/analysis/horizon_scan_phase_b_run.py` — `run_phase_b_core`에 `continuous_scanned_rows or event_scanned_rows`일 때만 Phase A 75개(`build_primary_hypothesis_registry(config)`) + Phase B ready continuous를 합친 registry와 ready SUE 셀로 `run_combined_cross_sectional_permutation` 호출 → `core/permutation_summary.parquet` 저장. `run_combined_ab`에 그 parquet가 있으면 읽어 `real_discovery_count`(이미 계산된 `combined`의 `primary_discovery_ab` 합) 대비 `empirical_discovery_count_p`(재사용)로 `manifest.json`에 `combined_cross_sectional_permutation: {real_discovery_count, n_replicates, p_empirical_count}` 추가 — 파일이 없으면 이 필드 자체를 생략.
- 테스트: `tests/unit/test_horizon_scan_phase_b_joint_permutation.py`(신규, 9개), `test_horizon_scan_phase_b_run.py`(+2, permutation summary present/absent wiring), `test_horizon_scan_phase_b_smoke.py`(+1 신규 + `test_run_combined_ab_against_real_published_phase_a_run` 확장, permutation 파일 존재가 ready 상태와 일치하는지 + manifest 필드 유무).

**핵심 결정**
- **어디서 실행하는가가 핵심 설계 결정**이었다 — `run_combined_ab`는 B-PR10부터 디스크의 두 발행물만 읽는 순수 함수로 의도적으로 설계돼 있어 lake 재접속 없이 어떤 두 published run이든 결합 가능해야 한다. 하지만 null discovery count 자체는 진짜 발견 수 없이도 계산 가능(치환된 데이터를 재스캔·재BH해서 세기만 하면 됨)하므로, null 분포 계산은 이미 connection·panel을 쥐고 있는 `run_phase_b_core`에서 하고, 진짜 발견 수와의 비교(`empirical_discovery_count_p`)만 `run_combined_ab`가 그 parquet를 읽는 순수 계산으로 나중에 한다 — 기존 아키텍처 경계를 유지.
- continuous 쪽 "frozen rank vector 치환"은 새 로직이 필요 없었다 — `permute_within_groups`로 원값을 date×market 그룹 안에서 섞은 뒤 다시 rank-IC를 계산하는 것은, 이미 계산된 rank를 그룹 안에서 직접 치환하는 것과 통계적으로 동일하다(rank 변환은 같은 치환을 feature·label 양쪽에 동일 적용하면 불변). Phase A의 기존 permutation 부품을 그대로 재사용하고 registry만 넓혔다.
- SUE 쪽은 진짜 새로 만들었다 — 코호트 내부 순수 치환(bootstrap의 복원추출과 다름)이 이 리포에 없던 개념. 매 replicate SQL을 다시 안 날리고 `_pool_cohort_ranks`로 한 번만 얻은 `qualifying`을 재사용해 `sue_pctrank`만 섞는 방식으로 비용을 줄였다.
- SUE 셀은 continuous와 달리 코호트 구조가 셀마다 독립이라 **셀마다 별도 seed**(`derive_replicate_seed(placebo_kind=f"combined_sue_rank_permutation:{hypothesis_id}", ...)`)를 쓴다 — continuous는 하나의 공유 panel이라 replicate당 seed 하나로 충분하지만, SUE는 issuer/filing-cycle bootstrap이 이미 세운 전례(`{cluster_kind}:{hypothesis_id}`)를 따랐다.
- 오늘 실제 lake는 `M_B_ready=0`이라 이번 permutation도 실행되지 않는다(`continuous_scanned_rows or event_scanned_rows` 가드) — 실제 CLI `--phase B` 재실행으로 크래시 없음과 `permutation_summary.parquet` 부재(정상)를 확인했고, `--phase AB`는 유일한 발행 Phase A run이 config-stale이라 §2.3 rule 5가 정확히 거부하는 것(B-PR10부터 있던 기존 상태)까지 확인했다. 실제 로직 검증은 synthetic DuckDB 코호트 + mock 기반 unit test로 했다.

### B-PR15 — B-10 Stage 1: readiness_matrix + robustness summary 아카이브

B-10 전체("family별 결론 카드, `03b_horizon_scan_results.md` 보고서, `readiness_matrix.*`/
`*_quality.parquet`/`*_coverage.parquet`/robustness `*_summary.parquet` 등 §7.1 나머지
artifact")는 한 PR에 넣기엔 너무 크고 성격도 다양해(값 재사용 vs 신규 SQL 진단 vs family 카드
서술 vs 아직 없는 §5.5 segment 진단) B-8/B-9처럼 여러 단계로 쪼갰다. 이번 Stage 1은 그중
**새 통계 로직이 전혀 없는, 이미 계산된 값을 디스크에 남기는 배선**만 다룬다.

**만든/고친 파일**
- `research/analysis/horizon_scan_phase_b_run.py` — `compute_phase_b_gate_updates`의 반환
  타입을 `dict[str, dict]` 단독에서 `tuple[dict[str, dict], dict[str, list[dict]]]`로 확장
  (`{"nonoverlap_rows": [...], "temporal_placebo_rows": [...], "issuer_bootstrap_rows": [...],
  "filing_cycle_bootstrap_rows": [...]}`) — `run_phase_b_continuous_nonoverlap`/
  `run_phase_b_temporal_placebo`/`run_issuer_cluster_bootstrap`/
  `run_filing_cycle_block_bootstrap`가 cell마다 이미 반환하던 전체 row를 버리지 않고 그대로
  쌓기만 함(호출 순서·게이트 판정 로직 자체는 전혀 안 바뀜). 신규 `_render_readiness_matrix_md`
  (readiness_rows를 family/feature/h_start/h_end 정렬로 렌더링하는 순수 함수). `run_phase_b_core`에
  `readiness_matrix.parquet`/`.md`를 **항상**(ready 셀 유무와 무관) 쓰는 블록, `phase_b_diagnostics`의
  4개 리스트를 각각 비어있지 않을 때만 `{name}_summary.parquet`로 쓰는 블록 추가.
- 테스트: `tests/unit/test_horizon_scan_phase_b_run.py` — 기존 3개 `compute_phase_b_gate_updates`
  테스트를 새 튜플 반환에 맞게 고치고 diagnostics 내용 검증 추가, `_render_readiness_matrix_md`
  신규 테스트(+1, 정렬·전체 row 포함 확인). `tests/integration/test_horizon_scan_phase_b_smoke.py`(+2) —
  `readiness_matrix.parquet`/`.md`가 항상(38 row) 존재하는지, 4개 summary parquet의 존재가
  "continuous는 `select_phase_b_long_horizon_cells`가 뽑는 nw_lag>=59 셀 존재, SUE는 ready
  SUE 셀 존재"와 정확히 일치하는지.

**핵심 결정**
- `nonoverlap_summary.parquet`/`temporal_placebo_summary.parquet`/`issuer_bootstrap_summary.parquet`/
  `filing_cycle_bootstrap_summary.parquet`는 **신규 계산이 전혀 없다** — 이 4개 로버스트니스 함수는
  이미 cell마다 전체 상세 row를 반환하는데, 지금까지 `compute_phase_b_gate_updates`가 그중
  `nonoverlap_robustness_pass`/`offset_status`/`temporal_null_pass`/`p_temporal_nw`/
  `issuer_bootstrap_p`/`filing_cycle_bootstrap_p` 몇 개 필드만 `gate_updates`에 뽑아 쓰고
  나머지(예: `n_offsets_total`/`n_offsets_valid`/`offset_sign_agreement_ratio`,
  `n_clusters`/`n_valid_replicates`/`bootstrap_mean`)는 버리고 있었다. 이번 PR은 그 버려지던
  값을 그대로 살려 별도 parquet에 담기만 한다.
- `issuer_bootstrap_rows`/`filing_cycle_bootstrap_rows`에서 `replicate_ic_means`(최대 999개
  원소 리스트)는 의도적으로 제외 — "summary" 파일 취지에 안 맞고, 필요하면 나중 단계에서
  별도 replicate-level artifact로 다룰 수 있다.
- `readiness_matrix.parquet`/`.md`는 이 세션이 만든 나머지 모든 Phase B 진단 artifact와 달리
  **항상** 써진다 — ready 셀 존재 여부에 대한 진단이 아니라 38개 candidate 전체(ready+blocked)의
  readiness freeze 자체를 사람이 보기 편한 형태로 다시 쓴 것이기 때문. 다른 파일들의 "ready
  상태에 따라 존재 여부가 갈리는" 패턴과 다르다는 점을 통합 테스트 docstring에 명시했다.
- 오늘 실제 lake는 `M_B_ready=0`이라 4개 summary parquet는 여전히 전부 안 써진다(`long_cells`/
  `ready_events`가 항상 비어 있음) — `readiness_matrix.parquet`/`.md`만 새로 발행되는 것을 실제
  CLI `--phase B` 재실행으로 확인했다.

## 3. 현재 실행 상태 — 발행된 run과 데이터 블로커

(2026-08-11 확인, 2026-08-24 갱신. 이 절은 코드가 아니라 **디스크에 실제로 있는 것**을 적는다.)

### 3.0 snapshot 2026-08-23 — native A·B·AB 최신 official run

세 run은 `source=sj2_remote`, `config_hash=ab0de634…`, `official=true`이며 같은 A0 manifest와
`joint_cs_v2` mapping 계약을 쓴다. 새 원천 후보를 추가한 run은 아니다. I7 `fin_v4`와 최신
raw, native kernel·A 통계 재사용·worker 2개를 적용해 기존 25 family·113가설을 다시 검정했다.

| phase | run_id | content hash | 핵심 결과 | 시간 |
|---|---|---|---|---:|
| A | `20260823T210913-b649a460` | `46ccf585…` | 75/75 valid, `bh_pass` 57, discovery 32 | 60분 11초 |
| B | `20260823T221441-b649a460` | `f556dd3d…` | 38/38 ready, `bh_pass` 28, discovery 24 | 44분 24초 |
| AB | `20260823T225913-b649a460` | `e380d931…` | `m_ab=113`, discovery 56, `screen_pass` 12 | 1초 미만 |

Phase B 단독 결과는 다음과 같다.

| family | 셀 | bh_pass | discovery | robustness 통과/요구 | q_min |
|---|---:|---:|---:|---:|---:|
| `fin_log_mcap` | 4 | 4 | 4 | 3/3 | 1.74e-11 |
| `fin_value_z` | 4 | 4 | 4 | 0/3 | 2.41e-14 |
| `fin_gross_profitability` | 8 | 8 | 8 | 0/3 | 9.10e-7 |
| `ev_payout_yield` | 4 | 4 | 4 | 0/3 | 2.52e-17 |
| `ev_net_share_issuance_yoy` | 4 | 4 | 4 | 0/3 | 2.25e-10 |
| `fin_accruals_to_assets` | 4 | 4 | 0 | 0/3 | 4.08e-4 |
| `fin_asset_growth_yoy` | 4 | 0 | 0 | 0/3 | 0.9175 |
| `fin_sue` | 6 | 0 | 0 | 0/6 | 1.0 |

AB에서 확정된 Phase B 38셀의 grade는 **A5·B7·C25·D1**이다. `screen_pass` 12셀은 다음과
같다.

| family | 통과 horizon | 셀 수 | grade | 대표 IC |
|---|---|---:|---|---:|
| `fin_log_mcap` | 40–60·60–120 bucket, 0–60·0–120 cumulative | 4 | A | −0.1149(0–120) |
| `ev_net_share_issuance_yoy` | 40–60 bucket | 1 | A | −0.0221 |
| `fin_gross_profitability` | 10–20·20–40·40–60 bucket, 0–20·0–40 cumulative | 5 | B | +0.0275(0–40) |
| `ev_payout_yield` | 40–60 bucket | 1 | B | +0.0550 |
| `fin_value_z` | 40–60 bucket | 1 | B | +0.0599 |

12셀 중 temporal placebo가 요구된 것은 `fin_log_mcap` 3셀뿐이며 모두 p=0.0099로 통과했다.
나머지 9셀은 horizon 폭이 짧아 `robustness_required=false`다. B등급 7셀은 통계 때문이 아니라
source 품질 경고로 A 상한이 막혔다. `revision_ratio`는 `fin_gross_profitability` 0.1014,
`fin_value_z` 0.1014, `ev_payout_yield` 0.1116으로 임계 0.10을 넘었다.
`fin_gross_profitability`와 `fin_value_z`는 `mapping_fallback_ratio`도 각각 0.9440·0.9417로
임계 0.50을 넘었다.

C 25셀 중 24셀은 `robustness_pass`가 실패했고, 나머지 1셀은
`available_direction_pass`가 실패한 `fin_asset_growth_yoy`다. D 1셀은
`fin_accruals_to_assets`의 primary discovery·기간 부호 실패다. I7 뒤
`fin_gross_profitability` coverage가 0.5835로 늘면서 짧은 horizon 5셀이 새 B등급으로
살아난 것이 이전 run과 가장 큰 차이다. `fin_sue`는 여전히 표본이 없다.

결합 BH는 A 32개와 B 24개를 모두 유지해 discovery 56개가 됐다. Phase A 강등은 0건이다.
결합 단면 permutation은 100회에서 `p_empirical_count=0.0099`다. 실행시간은 A+B+AB 약
1시간 44분이며, peak RSS는 A 9.94GB, B 20.36GB다.

주의할 점이 둘 있다.

- run spec은 `git_dirty=true`다. 8월 27일 두 번째 official run으로 통계·판정 결정성은
  확인했지만, non-overlap과 rank correlation의 부동소수점 마지막 비트까지 같은 byte-level
  결정성은 아직 아니다(아래 §3.0.1).
- 이 config에는 `mcap_krx_log`, `feat_filing_activity`, N2 업종 중립 variant, N6·N8 후보가
  없다. `fin_log_mcap`은 여전히 DART share 기반 `market_cap_pit`다.

### 3.0.1 2026-08-27 같은 입력 결정성 재실행

8월 23일 A0 manifest와 `config_hash=ab0de634…`, `workers=2`를 그대로 두고 A → B → AB를
새 checkpoint root에서 다시 돌렸다. 기존 run은 덮어쓰지 않았다.

| phase | 재실행 run_id | content hash | 시간 | peak RSS |
|---|---|---|---:|---:|
| A | `20260827T082015-b649a460` | `020b818f…` | 3,758.931초 | 9,352,904,704 bytes |
| B | `20260827T092909-b649a460` | `0aebbce8…` | 2,686.803초 | 17,587,863,552 bytes |
| AB | `20260827T101418-b649a460` | `6ab78f52…` | 1초 미만 | artifact 결합만 실행 |

판정에 쓰는 결과는 재현됐다.

- A `horizon_ic.parquet`와 `permutation_cell_stats.parquet`는 파일 SHA-256까지 같다.
- B `phase_b_primary_hypotheses.parquet`, `horizon_ic.parquet`, `permutation_summary.parquet`,
  `temporal_placebo_summary.parquet`는 파일 내용이 정확히 같다.
- AB `combined_ab_primary_hypotheses.parquet`와 `phase_a_card_overlay.parquet`도 정확히 같다.
- 따라서 discovery 56, `screen_pass` 12, B-cell grade A5·B7·C25·D1,
  `p_empirical_count=0.0099`가 그대로 재현됐다.

content hash가 달라진 이유는 판정 변경이 아니다. A `family_cards.json`의 non-overlap 평균과
B `nonoverlap_summary.parquet`·`primary_feature_rank_correlation.parquet`의 집계값이 실행마다
부동소수점 마지막 비트에서 달랐다. 최대 절대 차이는 **1.11e-16**이고, 문자열·상태·grade 등
비수치 필드는 같았다. 현 기준선은 **통계·판정 결정성 통과, byte-level 결정성 미완료**로 둔다.
canonical 결과표는 판정이 같은 8월 23일 run을 계속 가리키고, 8월 27일 run은 결정성 검증
lineage로 보존한다.

### 3.0a snapshot 2026-08-12 Phase B — 처음으로 실데이터 완주 (2026-08-13, 이전 기록)

`snapshot_date=2026-08-09`를 다룬 §3.1~3.2는 아래 run으로 대체됐다. 그 절들은 블로커가
어떻게 생겼었는지의 기록으로 남긴다.

| 항목 | 값 |
|---|---|
| run_id | `20260812T231507-f9117ce1` |
| snapshot / source | 2026-08-12 / sj2_remote (capital vintage 재export 반영) |
| config_hash | `e55c3046…` (기존 run들과 동일) |
| event feature formula | `issuance_v2` |
| 소요 | 23:15 시작, 약 5시간 30분 |
| readiness freeze | **38 ready / 0 blocked** (candidate 38개 전부) |

**B-PR11~15 오케스트레이션이 실제 데이터로 처음 돌았고 통합 크래시는 없었다.** §2가 우려한
"통합 단계에서만 드러나는 버그"는 이번 실행에서는 나오지 않았다. 산출물이 전부 생겼다 —
`nonoverlap_summary`(21행) · `temporal_placebo_summary`(21행) · `issuer_bootstrap_summary`(6행) ·
`filing_cycle_bootstrap_summary`(6행) · `permutation_summary`, 그리고 Stage 2 진단 7종.

**Phase B 단독 BH.** 평가 38셀 중 `bh_pass` 18, `primary_discovery_phase_b` 14.

| family | 셀 | bh_pass | discovery | robustness_pass / required | q_min |
|---|---|---|---|---|---|
| fin_log_mcap | 4 | 4 | 4 | **3 / 3** | 6.3e-10 |
| fin_value_z | 4 | 4 | 4 | 0 / 3 | 3.1e-9 |
| ev_payout_yield | 4 | 4 | 4 | 0 / 3 | 1.7e-14 |
| fin_accruals_to_assets | 4 | 4 | 0 | 0 / 3 | 1.4e-5 |
| ev_net_share_issuance_yoy | 4 | 2 | 2 | 0 / 3 | 0.0297 |
| fin_gross_profitability | 8 | 0 | 0 | 0 / 3 | 0.4165 |
| fin_asset_growth_yoy | 4 | 0 | 0 | 0 / 3 | 1.0 |
| fin_sue | 6 | 0 | 0 | 0 / 6 | 1.0 |

**등급은 전부 NE인데 이건 버그가 아니다.** family card의 `primary_discovery_cells`와
`evidence_grade`는 `q_fdr_global_ab` 기준이고, 그 값은 phase=AB run이 만든다.
`q_fdr_global_ab_min`이 8개 family 전부 null이다. 즉 **AB를 돌리기 전까지 등급은 미정이 정상**
이고, §4의 14개는 "Phase B 단독 진단" 수치다. 리포트 §4도 그렇게 명시한다.

**떨어진 곳은 temporal placebo 한 군데다.** discovery 14개의 게이트별 통과 현황이다.

| 게이트 | true | false | null(미요구) |
|---|---|---|---|
| `tradable_pass` | 14 | 0 | 0 |
| `period_sign_pass` | 14 | 0 | 0 |
| `expected_sign_pass` | 14 | 0 | 0 |
| `available_direction_pass` | 14 | 0 | 0 |
| `nonoverlap_robustness_pass` | 9 | 1 | 4 |
| **`temporal_null_pass`** | **3** | **7** | 4 |

`robustness_pass=false` 7건이 전부 `temporal_null_pass=false`와 같은 행이다. family로는
`ev_payout_yield` 3 · `fin_value_z` 3 · `ev_net_share_issuance_yoy` 1이다. IC 자체는 크다
(`ev_payout_yield` cum 0~120에서 `ic_mean` 0.097, `icir` 1.12) 그런데 시계열 placebo와
구분되지 않는다. `fin_log_mcap`만 요구 게이트 3개를 다 통과했다.

**source 품질은 8개 중 7개가 `warn`이다.** §2.5 임계값 대비 실측은 이렇다.

| 지표 | 임계 | 실측 | 판정 |
|---|---|---|---|
| `mapping_fallback_ratio` | 0.50 | 0.3195(`net_income`) / 0.3557(`controlling_net_income`) | 통과 |
| `value_mismatch_ratio` | 0.01 | 0.0001 | 통과 |
| **`revision_ratio`** | **0.10** | **0.1056 ~ 0.1259** | **초과 → grade A 상한** |

즉 `warn`을 만든 건 정정 비율 하나다. 0.10을 근소하게 넘는다. `fin_log_mcap`과
`ev_net_share_issuance_yoy`는 `not_applicable`이다.

**커버리지가 얇은 family 둘.** `fin_sue`는 effective start 2025-05-02에 coverage 0.0000이고
(B-1 6항 receipt-targeted XBRL 백필이 필요한 그 지점), `fin_gross_profitability`는 0.0315다.
둘 다 q가 1.0 / 0.42로 나온 family와 일치한다 — 신호가 없다기보다 표본이 없다.

**다음.** 같은 snapshot으로 `--phase A` 재실행 → `--phase AB`. 그래야 등급이 정해진다.
→ 둘 다 끝났다. §3.0b.

### 3.0b Phase A 재실행 → phase=AB — 등급이 정해졌다 (2026-08-13, 이전 기록)

크리티컬 패스의 마지막 두 계산이다. §3.0이 남겨둔 "등급 미정" 상태가 여기서 풀린다.

**Phase A 재실행.**

| 항목 | 값 |
|---|---|
| run_id | `20260813T081646-00fa0e76` |
| snapshot / source | 2026-08-12 / sj2_remote |
| config_hash | `e55c3046…` (Phase B run과 동일) |
| official / git | true / `b314624`, dirty=false |
| 소요 | 08:16:46 → 12:57:29, **4시간 41분** |
| 결과 | 412행 16 family, FDR 채점 75, `bh_pass` 58, `primary_discovery` 31 |

**08-09 run과 값이 완전히 같다.** 메타 5개 컬럼(`run_id`·`snapshot_date`·`source`·
`config_hash`·`phase`)을 뺀 **40개 값 컬럼 × 412행이 전부 일치**한다. 부동소수점 오차 수준의
차이도 없다. 이유는 둘이다 — Phase A 유효 표본이 holdout 경계 2025-08-01에서 끊기고
(`effective_sample_end`가 available 2025-07-30 / common_survivor 2025-02-05), 8년치 capital
vintage 백필은 `dart_capital_change_raw`만 건드려 `fin_*`/`ev_*` 즉 Phase B 소관이다. 즉 이
재실행의 목적은 새 발견이 아니라 **snapshot과 config를 Phase B에 맞추는 것**이었고, 그건
달성됐다.

**phase=AB.**

| 항목 | 값 |
|---|---|
| run_id | `20260813T130307-f9117ce1` |
| 소요 | 1초 미만 (순수 재조합) |
| `m_ab` | **113** = Phase A 75 + Phase B ready 38 |
| q 임계 | 0.10 |
| primary discovery | **45** |
| `screen_pass` | **7** |
| evidence grade | **A=5, B=2, C=24, D=7** |

`--phase AB`는 `--phase-a-run-dir`/`--phase-b-run-dir`를 **필수 인자로** 받는다(자동 탐색이
아니다). 두 run은 결합 전에 content hash로 무결성 검증을 통과했다(§2.3 rule 5).

**BH family를 넓혔는데 강등이 0건이다.**

| 출신 | 채점 | 단독 discovery | 결합 discovery |
|---|---:|---:|---:|
| Phase A | 75 | 31 | **31** |
| Phase B | 38 | 14 | **14** |

75 → 113으로 넓히면 문턱은 엄격해지기만 하는데 떨어진 가설이 하나도 없다. 리포트 §3이
"(none)"인 이유다.

**`screen_pass` 7셀.**

| family | horizon | 셀 | grade | `ic_mean` | `q_fdr_global_ab` | robustness | source quality |
|---|---|---|---|---|---|---|---|
| fin_log_mcap | 0–120 | cum | **A** | −0.1115 | 0.00000 | 필요·통과 | n/a |
| fin_log_mcap | 0–60 | cum | **A** | −0.0838 | 0.00000 | 필요·통과 | n/a |
| fin_log_mcap | 60–120 | bucket | **A** | −0.0653 | 0.00000 | 필요·통과 | n/a |
| fin_log_mcap | 40–60 | bucket | **A** | −0.0387 | 0.00000 | **불필요** | n/a |
| ev_net_share_issuance_yoy | 40–60 | bucket | **A** | −0.0083 | 0.02313 | **불필요** | n/a |
| ev_payout_yield | 40–60 | bucket | **B** | +0.0523 | 0.00000 | **불필요** | warn |
| fin_value_z | 40–60 | bucket | **B** | +0.0348 | 0.00000 | **불필요** | warn |

**7셀 중 3셀만 robustness 게이트를 실제로 거쳤다.** 나머지 4셀은 `robustness_required=False`라
temporal placebo를 아예 안 봤다. 기준은 `nw_lag >= 59`
(`placebo.temporal_min_nw_lag`, `select_phase_b_long_horizon_cells`)다 — h40–60 bucket은
width 20이라 long-horizon 셀이 아니고 rule 7의 non-overlap·temporal placebo 대상에서 빠진다.
width 60인 60–120 bucket과 cumulative 0–60/0–120만 걸린다. **사전등록된 설계이지 빠져나간 게
아니지만, grade A 5개를 같은 무게로 읽으면 안 된다.** 요구 게이트 3개를 전부 통과한 건
`fin_log_mcap`의 3셀이고, 이건 §3.0의 "`fin_log_mcap`만 요구 게이트 3개를 다 통과했다"와 그대로
맞는다.

B등급 2개가 A로 못 간 이유는 통계가 아니라 원천 품질이다. `source_quality_status=warn`이 상한을
B로 묶는데, 원인은 §3.0이 적은 `revision_ratio` 0.1056~0.1259 하나다(임계 0.10).

**막힌 셀의 이유는 한 곳으로 모인다.** grade C는 규칙상 "`available_direction_pass` 실패
**또는** `robustness_pass` 실패"로 라우팅되는데(`compute_phase_b_evidence_grade`), 실측에서는
**24개 전부가 robustness 문으로 왔고 available 방향 문으로 떨어진 셀은 0개**다. 그중 7개는
`primary_discovery`까지 통과하고 오직 강건성에서만 떨어졌다(나머지 17개는 둘 다 실패).
반대로 grade D 7개는 `robustness_pass`가 하나도 없고 `primary_discovery` 미달이 원인이다.
즉 통계적으로 살아남은 셀을 떨어뜨리는 건 사실상 temporal placebo 하나다 — §3.0의 진단이
등급 단계에서도 그대로 재현된다. placebo p값 비교는 `09_all_feature_results.md` §9.2.

| grade | 셀 | `failed_gates` 주요 조합 |
|---|---:|---|
| C | 24 | `[primary_discovery, period_sign_pass, robustness_pass]` 7 · `[robustness_pass]` 7 · `[primary_discovery, tradable_pass, period_sign_pass, robustness_pass]` 6 · 나머지 4 |
| D | 7 | `[primary_discovery, tradable_pass]` 4 · `[primary_discovery, period_sign_pass]` 2 · `[primary_discovery]` 1 |

**결합 단면 permutation (B-PR14).** 실제 발견 45개에 대해 100회 복제로
`p_empirical_count = 0.0099`. 45개 이상을 만들어낸 복제가 0회라는 뜻이다. 다만 **0.0099는 복제
100회에서 나올 수 있는 최솟값**이라, 더 낮은 p를 보려면 `placebo.cross_sectional_repeats`를
올려야 한다.

**`primary_feature_rank_correlation.parquet` 84쌍이 이번에 처음 나왔다** (B-PR13). 08-09 AB
run에는 parquet 2개뿐이었다. Phase A × Phase B 피처 간 일별 rank correlation인데 값이 대체로
낮다 — `px_reversal_5d`↔`fin_log_mcap` −0.021, `px_mom_12_1`↔`fin_asset_growth_yoy` +0.152
수준이다. A쪽 발견과 B쪽 발견이 서로 다른 정보를 보고 있다는 뜻이라, 결합 BH에서 강등이 0건인
것과 일관된다.

산출물 경로는
`research/output/horizon_scan/phase=AB/snapshot_date=2026-08-12/source=sj2_remote/config_hash=e55c3046…/run_id=20260813T130307-f9117ce1/`
— `combined_ab_primary_hypotheses.parquet`, `phase_a_card_overlay.parquet`,
`primary_feature_rank_correlation.parquet`, `03ab_combined_results.md`.

### 3.1 발행된 official run (snapshot 2026-08-09, 기록용)

| phase | run_id | config_hash | 결과 |
|---|---|---|---|
| A (구) | `20260803T063659-93effdb0` | `1d208258…` | Phase B 섹션 추가 **이전** config. 05·06 문서가 인용하는 run |
| A (신) | `20260810T141014-7212fe82` | `e55c3046…` | 현재 config로 재실행. **family 17개 등급이 구 run과 완전히 동일**(A 6, C 4, D 6, R 1, A 목록도 같음) |
| B | `20260810T134333-66c929e0` | `e55c3046…` | B-PR15 이전 |
| B | `20260810T135845-e04c00c7` | `e55c3046…` | 당시 최신. `core/`에 `phase_b_primary_hypotheses.parquet` + `readiness_matrix.{parquet,md}` |
| AB | `20260810T194651-e04c00c7` | `e55c3046…` | `m_ab=75`, `phase_b_screen_pass_count=0`, `phase_b_evidence_grade_counts={A:0,B:0,C:0,D:0}` |

경로는 모두
`research/output/horizon_scan/phase=<X>/snapshot_date=2026-08-09/source=sj2_remote/config_hash=<hash>/run_id=<id>/`.

즉 B-PR10이 "범위 밖"으로 남겼던 **현재 config Phase A 재실행은 이미 끝났고**(§2 B-PR10의
"또 다른 실제 상태" 항목은 이제 해소됨), `load_phase_a_primary_rows`의 config/content hash
검증도 통과해 phase=AB run이 정상 발행됐다. 다만 `M_B_ready=0`이라 결합 결과에 Phase B 셀이
하나도 안 들어갔고, AB run의 실질 내용은 Phase A 75개 + `phase_a_card_overlay.parquet`뿐이다.

### 3.2 왜 아직 `M_B_ready=0`인가

`run_id=20260810T135845-e04c00c7/core/readiness_matrix.md`(Phase B 기준 정본) 기준, 38개
candidate 전부 `blocked_exploratory` / `blocked_missing_dependency`다. family별 실제 결손은
이렇게 좁혀져 있다.

| family | 셀 수 | missing_dependencies |
|---|---:|---|
| fin_log_mcap / fin_value_z / fin_gross_profitability / fin_asset_growth_yoy / fin_accruals_to_assets | 각 4 | `feat_fin_scan_daily` |
| fin_sue | 6 | `dart_filing_receipt_raw`, `fin_sue_event` |
| ev_net_share_issuance_yoy | 4 | `dart_capital_change_raw`, `feat_event_scan_daily` |
| ev_payout_yield | 4 | `feat_event_scan_daily` |

뿌리는 하나다 — 로컬 lake
`data_lake/raw_postgres/snapshot_date=2026-08-09/source=sj2_remote/`에
**`dart_filing_receipt_raw`와 `dart_capital_change_raw` 디렉터리가 아예 없다**. 이 둘이
없으면 `stock_metric_vintage_fact`(→ `feat_fin_scan_daily`, `fin_sue_event`)와
`feat_event_scan_daily`가 materialize되지 않아 나머지가 전부 연쇄로 막힌다.
`dart_xbrl_fact_raw`는 2.5GB로 정상 존재한다(루트의
`research/output/horizon_scan/readiness_matrix.md`는 Phase A run이 쓰는 **다른** 파일이고
blocked family의 의존 목록 전체를 나열하는 형식이라 `dart_xbrl_fact_raw`도 같이 찍힌다 —
Phase B 판정은 run 안의 `core/readiness_matrix.md`를 봐야 한다).

parquet가 없는 이유는 코드가 아니라 배포 체인이다.

```text
Phase B 코드가 전부 uncommitted (56개 변경/신규 파일, 마지막 커밋은 a03872e)
  → 릴리즈·이미지 빌드 없음 (prod는 여전히 ghcr.io/sjleekor/sdc:v0.8.16)
  → prod에 dart sync-filings / capital-change 수집 자체가 존재하지 않음
  → 두 raw 테이블이 prod DB에 없음
  → raw-parquet-export-all.sh가 내보낼 것이 없음 (export 목록 등록은 B-PR2에서 이미 끝남)
  → M_B_ready = 0
```

즉 **지금 막힌 것은 통계도 로직도 아니고 수집이다.**

## 4. 남은 작업

작업 패키지 관점 진척도: B-0 ~ B-9 완료(단 §5.5 segment 진단 제외), B-10은 Stage 1·2·4 완료,
Stage 3·5 미착수.

### 4.1 선행 조건 — 데이터 수집 (코드 작업 아님, 이게 지금 크리티컬 패스)

순서대로 해야 한다. 1~3은 아직 아무것도 안 됐다.

1. **커밋 + 릴리즈** — Phase B 변경분을 커밋하고 `sdc-release` 스킬로 버전 범프·태그·prod
   compose 갱신. 이걸 해야 prod가 `dart sync-filings`를 알게 된다.
2. **prod 백필** — sj2-server에서 연도별로 수집. 2026-08-12에 `dart sync-filings`를
   `bin/dart-backfill-all-years.sh`와 prod wrapper의 **마지막 단계로 엮었다**(접수 달력연도
   기준 자체 범위, 현재 연도가 맨 끝). 지금 돌고 있는 A2는 그 전에 띄운 일회성
   `phase_b_backfill.sh`다. 수동으로 개별 실행할 때의 커맨드는 아래와 같다.
   ```bash
   krx-collector dart sync-filings --years 2015,2016,...,2026   # dart_filing_receipt_raw
   krx-collector dart sync-share-info --year <YYYY>             # dart_capital_change_raw 동반 수집
   ```
   OpenDART 일일 한도에 걸리면 exit 75로 끊기고 다음 실행에서 이어진다(기존 정책 그대로).
3. **raw export 재실행** — `bin/raw-parquet-export-all.sh`(두 테이블은 이미 export 목록과
   `research/etl/config.py`에 등록돼 있다) → 새 `snapshot_date` 발행.
4. **Phase B 재실행** — `--phase B`. 이때 처음으로 `M_B_ready>0`이 되고, B-PR11~B-PR15가
   만든 오케스트레이션(period segment, non-overlap, temporal placebo, issuer/filing-cycle
   bootstrap, SUE ordinal, rank-correlation, 결합 permutation)이 **처음으로 실제 실행된다**.
   지금까지 이 경로들은 mock/synthetic 테스트로만 검증됐다 — 실제 데이터로 처음 도는 순간
   B-PR10 때처럼 통합 단계에서만 드러나는 버그가 나올 수 있다고 보는 게 맞다.
5. **Phase A 재실행 여부 판단** — 새 snapshot으로 B를 돌리면 A도 같은 snapshot으로 다시
   돌려야 결합 BH가 성립한다(§2.3 rule 5). `20260810T141014-7212fe82`는
   `snapshot_date=2026-08-09` 기준이다.
6. **Phase AB 재실행** — 그래야 `q_fdr_global_ab`·`screen_pass`·`evidence_grade`가 처음으로
   진짜 값을 갖는다. 지금 발행된 AB run은 B가 0개인 상태의 껍데기다.

### 4.1.1 진행 상황과 vintage distance probe (2026-08-12)

**끝난 것.** 1번 커밋+릴리즈 완료(v0.9.2). `dart_share_count_raw`는 2015년부터 연도별
1,726~2,238 ticker로 이미 차 있다 — 즉 filing position 자체는 전 구간에 존재한다.

**돌고 있는 것** (2026-08-12 12:00 기준 스냅샷 — 지금 상태는 `00` §0의 명령으로 직접 확인한다).

| 작업 | 내용 | 12:00 시점 상태 |
|---|---|---|
| A2 | `phase_b_backfill.sh filings "2024 … 2015 2026"` (11개 연도, `dart_filing_receipt_raw`) | 06:33 시작, 연도당 약 36분. 2015 진행 중(10/11), 남은 건 2026 하나. 완료 예상 13시 전후 |
| capital probe | `phase_b_backfill.sh capital "2024 2020 2016"`, `SDC_PHASE_B_REPRT_CODES=11011` | `opendart` lock 대기 중(`SDC_LOCK_WAIT_SECONDS=32400`). A2 종료 직후 자동 시작, 약 1시간 10분 → 14시 전후 |

연도 순서는 과거부터고 2026이 마지막이다. 저장된 과거 연도는 다음 실행에서 영원히 skip되지만
현재 연도는 설계상 매번 다시 받으므로 마지막에 둬야 가장 신선하게 끝난다. exit 75(quota)로
끊기면 같은 스크립트를 다시 돌리면 이어받는다.

`phase_b_backfill.sh`는 저장소가 아니라 sj2-server의 `/home/whi/phase_b_backfill.sh`에만 있는
일회성 스크립트다(`deploy/prod/bin/lib/sdc-wrapper.sh`를 source해서 lock과 로깅만 재사용한다).
상시 경로는 이제 `bin/dart-backfill-all-years.sh`의 마지막 단계이므로 이 일회성 스크립트를
저장소로 옮기지 않는다 — 두 벌이 되면 어느 쪽이 정본인지 흐려진다.

두 작업 모두 `sdc_with_source_lock opendart`를 잡으므로 서로 겹치지 않는다. 데일리 OpenDART
체인은 04:00 Corp Sync에서 시작해 chain으로 이어지며 lock을 잡지 않으니, 백필이 다음 날
04:00을 넘기면 데일리 이벤트를 잠시 꺼야 한다. 키 9개(일 18만 요청)라 A2(약 32k) +
probe(약 7.8k)는 한도에 여유가 있다.

**A3는 축소됐다.** 원래 계획한 "11개 연도 × 4개 보고서" capital 수집은 커버리지를 늘리지
않는다. 최신 연간보고서 하나가 상장 이후 전체 이력을 주기 때문이다(표본 19종목 확인:
`000100` 139건 1962~2024, `000050` 5건 1975~2014). 남은 것은 커버리지가 아니라 **어느 vintage를
쓸 것인가**이고, 그건 plan §4.4.1이 정의한 probe로 판정한다.

**probe가 답할 질문과 판정 기준**은 plan §4.4.1 "vintage distance probe"에 사전 고정돼 있다.
여기(구현 로그)에는 실행 결과만 기록한다 — 거리 1·5·9년별 feature-changing 불일치율,
(a)/(b) identity 통과율, 그리고 그 값이 기준표의 어느 칸에 떨어졌는지.

**실행.** probe는 PostgreSQL이 아니라 **lake의 parquet**을 읽는다. 따라서 순서는 수집 완료 →
`bin/raw-parquet-export-all.sh`로 새 snapshot 발행 → probe다. export를 건너뛰면 옛 snapshot을
재는 셈이 된다. 기준표 판정까지 스크립트가 적용해 `research/output/vintage_probe/`에 md·json으로
남긴다.

```bash
uv run python -m research.analysis.capital_change_vintage_probe --snapshot-date YYYY-MM-DD
```

구현은 `research/etl/vintage_probe.py`(지표 SQL)와
`research/analysis/capital_change_vintage_probe.py`(러너·판정)에 있다. 지표 ①은 이벤트를 1:1로
매칭하지 않고 **창별 분류합계**를 비교한다 — 창 안에서 움직인 날짜는 feature를 안 바꾸므로
일치로, 경계를 넘은 날짜는 두 창이 달라져 불일치로 잡힌다. 정정된 사건을 어떻게 "같은 사건"으로
볼지 판단할 필요가 없어진다.

두 vintage가 다 있는 `000040` 하나로 도구를 검증했다(거리 1년): 창 9개 중 불일치 0개,
행 수준 35개 중 1개 불일치(2021-01-31 → 2021-01-13 재기재). 날짜 정정이 창 합계를 안 바꾼다는
것이 실제로 확인된다.

**probe를 돌리기 전에 매핑부터 고쳤다.** 첫 검증에서 identity 통과율이 latest 5/10, strict
3/10으로 낮게 나왔는데, 원인이 vintage 정책이 아니라 `isu_dcrs_stle` 카탈로그 누락이었다.
그대로 probe를 돌렸다면 지표 ②가 매핑 구멍을 재고 있는데 vintage 정책 탓으로 읽혔을 것이고,
"strict의 통과율이 절반 이하" 우선 규칙이 잘못된 근거로 발동할 수 있었다. 판정 근거는
plan §4.4.2, 보강 결과는 아래 §4.3.1.

측정 결과가 나오기 전에는 `event_scan.build_issuance_sql`의 `capital_change_classified`를
고치지 않는다. dedup 규칙 자체(§4.4.1 1~3항)는 확정됐으므로 그 부분 구현은 선행해도 되고,
(a)/(b) 분기만 측정 뒤에 붙인다.

### 4.1.2 수집 완료와 probe 판정 결과 (2026-08-12) — **(b) strict PIT 채택**

**수집이 끝났다.** A2(filings)는 12:53, capital probe는 14:27에 각각 `ALL DONE`으로 종료했다.
quota 중단은 없었고 데일리 OpenDART 체인(04:00~05:22)과도 겹치지 않았다.

| 테이블 | 수집 결과 |
|---|---|
| `dart_filing_receipt_raw` | 접수 달력연도 2015~2026, 1,201,866행. 연도별 법인 1,992(2015)~2,657(2026) |
| `dart_capital_change_raw` | 71,535행. 11011 vintage 2016·2020·2024 + 데일리로 들어온 2025·2026 |

**raw export.** `snapshot_date=2026-08-12` / `source=sj2_remote`를 `--route remote`로 발행했다
(15개 테이블, `_SUCCESS.json` 기록). 첫 실행에서 두 테이블이 `unknown table(s)`로 실패했는데,
§4.1 3항이 "이미 등록돼 있다"고 적은 것과 달리 Rust exporter의
`tools/raw-parquet-exporter/config/export_tables.toml`에만 빠져 있었다.
`research/etl/config.py`와 `bin/raw-parquet-export-all.sh`에는 있었다. 등록 내용은 아래와 같다.

| 테이블 | 전략 | 근거 |
|---|---|---|
| `dart_capital_change_raw` | `raw_id_range` + `bsns_year`/`reprt_code` | `dart_share_count_raw`와 동일한 모양 |
| `dart_filing_receipt_raw` | `full_table`, 무파티션 | `bsns_year`/`reprt_code` 컬럼이 없다. `raw_id_range`는 그 파티션 쌍을 하드코딩으로 요구하고(`export.rs:535`) `date_month`는 `trade_date`에 고정돼 있다(`export.rs:837`) |

full_table은 resume 대상이 아니므로 `raw-parquet-export-all.sh`의 분류도
`raw_id_tables` → `non_resumable_tables`로 옮겼다. 두 테이블 행 수는 Postgres와 정확히 일치한다.

**probe 측정값** (`research/output/vintage_probe/`, snapshot 2026-08-12).

지표 ① feature-changing 불일치율. 티커별 최신 판이 2025판인지 2024판인지에 따라 거리가 갈려
설계가 예상한 1·5·9년 외에 4·8년 점도 같이 나왔다.

| 거리(년) | 티커 | 비교 창 | 불일치 창 | 비율 |
|---|---|---|---|---|
| 1 | 757 | 6,617 | 114 | 0.0172 |
| 4 | 1,328 | 5,970 | 1,345 | 0.2253 |
| 5 | 725 | 3,549 | 498 | 0.1403 |
| 8 | 1,052 | 1,062 | 304 | 0.2863 |
| **9** | 672 | 680 | 124 | **0.1824** |

지표 ② identity 통과율.

| 정책 | position | 전년 있음 | identity 통과 | feature 생성 | 비율 |
|---|---|---|---|---|---|
| (a) latest_vintage | 94,219 | 80,196 | 60,210 | 60,178 | 0.7504 |
| (b) strict_pit | 94,219 | 80,196 | 52,407 | 52,382 | 0.6532 |

**판정.** 9년 거리 불일치율 0.1824는 기준표의 "5% 초과" 칸이다 → **(b) strict PIT 채택 + 잔여
연도 vintage 수집**. 우선 규칙("(b)의 통과율이 (a)의 절반 이하이면 (a)")은 발동하지 않는다 —
0.6532는 (a)의 87% 수준이고 발동선인 0.375보다 한참 위다.

거리 1년이 1.72%인데 9년이 18.2%다. 옛 판을 그대로 쓰면 안 되는 이유가 시간에 비례해 커진다는
뜻이고, 5% 임계값이 거리 1년과 4~9년 사이 어딘가에 놓여 있다. 행 수준으로 보면 더 분명하다 —
거리 9년에서 옛 판 사건 3,129건 중 3,109건이 최신 판에 같은 모양으로 남아 있지 않다.

`DEFAULT_VINTAGE_POLICY`를 `VINTAGE_POLICY_STRICT_PIT`으로 고정했다(유닛 테스트 939개 통과).

**따라오는 수집.** (b) 채택은 plan §4.4.1이 정한 대로 잔여 8개 연도 vintage 수집을 요구한다.
strict PIT은 position 시점마다 그 이전 판을 필요로 하므로 판이 없는 구간은 feature가 NULL이 된다.
11011 기준 현재 커버리지는 이렇다.

| bsns_year | ticker | 상태 |
|---|---|---|
| 2016 / 2020 / 2024 | 1,795 / 2,152 / 2,561 | probe용으로 백필 완료 |
| 2025 | 765 | 데일리 경로로만 들어와 부분 수집 |
| 나머지 | 0 | 미수집 |

남은 건 **2015·2017·2018·2019·2021·2022·2023 + 2025(보완)** 여덟 연도다. 실측 기준 연도당 약
23분이라 합계 3시간 전후다. **이 수집이 끝나기 전에 Phase B를 돌리면 issuance family만 얇은
데이터로 판정된다** — §4.1 4번의 선행 조건에 이 수집이 추가된다.

### 4.1.3 `--phase B`에는 싼 smoke 경로가 없다 (2026-08-12에 확인)

정식 실행 전에 통합 경로 버그를 미리 잡으려고 `--phase B --permutations 20 --output-root <스크래치>`
로 돌렸는데, **`--permutations`와 `--smoke-family`는 `--phase B`에서 조용히 무시된다.**
`run_phase_b_core`의 시그니처에 그 인자가 아예 없고(`horizon_scan.py`가 phase A 분기에서만
넘긴다), §6 B-8 결합 permutation은 `config.raw["placebo"]["cross_sectional_repeats"]`(=100)을
직접 읽는다. `--phase A`는 같은 플래그를 override로 받는다(`horizon_scan.py` 554~557행).

결과적으로 그 실행은 **정식 비용 그대로**였고 manifest에도 `official: true`가 찍혔다
(`--permutations`가 official 등급을 낮추지도 않는다 — Phase B의 official은
`resolution.auto_selected`와 A0 manifest로만 결정된다). 4시간 동안 결합 permutation 단계에서
안 끝났고, 그 사이 vintage 백필이 완료돼 capital 데이터가 낡은 실행이 되어 폐기했다.

건진 것은 있다. 그 실행이 **readiness freeze를 처음으로 실제 데이터로 통과**했다.

| 항목 | 값 |
|---|---|
| `m_b_ready` / `max_candidates` | **38 / 38** (전부 `ready`, `blocked_missing_dependency` 0) |
| `combined_ab_hypothesis_count` | 113 (Phase A primary 75 + Phase B 38) |
| `blocked_exploratory_count` | 0 |

B-10 Stage 2 진단 7종도 전부 `core/`에 산출됐다(`capital_change_quality`,
`filing_receipt_quality`, `receipt_value_pairing_quality`, `stock_metric_vintage_quality`,
`quarterly_metric_quality`, `event_coverage`, `feature_coverage`) — §3.2가 적어둔 블로커 체인이
실제로 풀렸다는 뜻이다.

**남은 판단.** Phase B에 싼 리허설 경로가 없으면 통합 버그를 정식 실행 몇 시간 뒤에야 만난다.
`--permutations`를 `run_phase_b_core`까지 배선하는 건 작지만 사전등록된 scan 경로를 건드리므로
별도로 판단한다. 지금은 "무시된다"는 사실만 기록한다.

### 4.2 B-9 나머지
- ~~evidence grade의 "source 비치명 경고"~~ → 2026-08-12 완료. Stage 2가 값을 내면서 풀렸다.
  `research/analysis/horizon_scan_phase_b_source_quality.py`가 family별로
  `mapping_fallback_ratio`·`revision_ratio`·`value_mismatch_ratio`를 판정하고,
  `compute_phase_b_evidence_grade`가 그 결과로 grade A만 막는다(B 아래로는 내리지 않는다).
  임계값과 근거는 plan **§2.5**에 사전등록했다 — Phase B를 한 번도 돌리기 전에 고정했다.

  설계에서 두 가지가 중요하다.

  1. **family가 읽는 metric 중 가장 나쁜 것**으로 판정한다. 평균이 아니다. 피쳐는 입력들의
     비율이라 하나가 통째로 fallback이면 나머지가 깨끗해도 못 믿는다. metric 안에서는 연도별
     행 수로 가중한다. family→metric 의존 관계는 추측이 아니라 피쳐 SQL에서 읽어 적었다
     (`FAMILY_METRIC_DEPENDENCIES`).
  2. **측정 불가는 깨끗함이 아니다.** 비율이 NULL이면 임계값을 넘긴 것과 똑같이 grade A를
     막는다. 인자를 아예 안 주는 호출도 마찬가지다(fail-closed). 진단이 없다는 사실이 통과의
     근거가 될 수는 없다.

  `fin_log_mcap`(시가총액)과 `ev_net_share_issuance_yoy`(raw 직접)는 metric layer를 안 거치니
  `not_applicable`이다.

  **임계값 세운 직후 정의 결함을 하나 잡았다.** 실측에서 `net_income` fallback이 0.655로
  나왔는데 원인이 데이터가 아니라 `catalog_best_priority`의 정의였다 — 카탈로그 최선
  priority가 CFS 전용 룰(10)이라 OFS 행은 구조적으로 못 맞춘다. `(metric_code, fs_basis)`별로
  바꿔 0.32가 됐고, `stock_metric_vintage_quality`의 grain에도 `fs_basis`를 넣었다.
  임계값을 동결하기 전에 재 본 덕에 잡았다.

  현재 lake(`2026-08-09`) 기준 판정:

  | family | status | fallback(worst metric) | pairing |
  |---|---|---|---|
  | `fin_log_mcap` / `ev_net_share_issuance_yoy` | not_applicable | — | — |
  | `fin_value_z` / `fin_sue` | unmeasured | 0.356 (`controlling_net_income`) | 0.000117 |
  | `fin_gross_profitability` / `fin_asset_growth_yoy` / `fin_accruals_to_assets` | unmeasured | 0.320 (`net_income`) | 0.000117 |
  | `ev_payout_yield` | unmeasured | 0.000 (`issued_shares`) | 0.000117 |

  여섯 family가 전부 `unmeasured`인 이유는 하나다 — 접수 이력이 아직 없어 `revision_ratio`가
  NULL이다. A2 백필이 export되면 저절로 풀린다. fallback 0.32~0.36은 CIS 대신 IS로 보고하는
  발행사에서 나오는 실제 신호이고 임계값 0.50 아래다.
- segment 진단(§5.5)은 여전히 값이 없다 — 생기면 같은 자리에 붙인다.

### 4.3.1 isu_dcrs_stle 카탈로그 v2 (2026-08-12)

`capital_change_quality`가 처음 낸 값에서 2025년 annual vintage의 미분류 비율이 **0.224**
(259건 중 58건)로 나왔다. §4.4 4단계가 미분류 하나로 창 전체를 NULL로 만들기 때문에, vintage
정책과 무관하게 issuance family를 죽이는 값이다.

사유별로 뜯어보니 58건 중 47건이 단순 카탈로그 누락이었다 — `신주인수권행사`(50건, 이미 매핑된
`전환권행사`와 동형), `무상감자`(13건, `감자(무상)`의 다른 표기), `유상증자(주주우선공모)`(5건),
`출자전환`(2건). 판정 근거와 정확 일치 유지 원칙은 plan §4.4.2에 기록했다.

보강 전후:

| 지표 | v1 | v2 |
|---|---|---|
| 2025 annual 미분류 | 58건 (0.224) | 11건 (0.043) |
| 2024 annual 미분류 | 23건 (0.657) | 0건 (0.000) |
| `000040` identity 통과 (latest) | 5/10 | **9/10** |
| `000040` identity 통과 (strict_pit) | 3/10 | 4/10 |

남은 11건은 전부 발행사가 사유를 비워둔 `-` 행이라 미분류로 남긴다. 매핑 버전은
`issuance_v2`이며 `phase_b_run_spec.json`의 `event_feature_formula_version`으로 기록된다
(§1.3 fingerprint 계약의 해당 필드를 이번에 구현했다).

### 4.3 B-10 나머지 (Stage 2~5)
- **Stage 2 — 7종 전부 완료(2026-08-12).** raw 위에 앉는 2종은
  `research/etl/phase_b_quality.py`, 마트(B-2~B-6) 위에 앉는 5종은
  `research/etl/phase_b_coverage.py`에 있다. 모듈을 나눈 이유는 질문이 다르기 때문이다 —
  앞의 둘은 "원천이 쓸 만한가", 뒤의 다섯은 "마트가 그 원천을 얼마나 지켰고 피쳐가 스캔할
  만큼 존재하는가"를 묻는다.

  7종 모두 `write_phase_b_quality_diagnostics`(`horizon_scan_phase_b_run.py`)가 run
  디렉터리에 쓴다. `readiness_matrix`와 같은 취급으로 **readiness와 무관하게 무조건** 쓴다 —
  이 진단들은 스캔 결과가 아니라 입력을 서술하고, 가장 필요한 순간이 바로 모든 셀이 blocked인
  지금이기 때문이다. 입력 뷰가 없는 진단만 파일을 안 만든다(빈 결과는 스키마와 함께 남긴다 —
  "없음"과 "재보니 0"은 다른 사실이다).

  - `receipt_value_pairing_quality` — (bsns_year, reprt_code). 분모는 판정 가능한 행만
    쓴다(=`dart_financial_statement_raw` 출처). XBRL 출처 행까지 분모에 넣으면 페어링이 실제보다
    좋아 보인다.
  - `stock_metric_vintage_quality` — (metric_code, bsns_year, reprt_code). §4.2가 요구한
    `mapping_fallback_ratio`/`revision_ratio`가 여기서 나온다. `revision_ratio`는
    `is_revision`이 **알려진** 행만 분모로 쓴다 — 접수 매칭이 안 된 행을 "정정 아님"으로 세면
    receipt 커버리지가 최악인 구간에서 비율이 가장 좋아 보이게 된다. `mapping_fallback`은
    데이터의 최소 priority가 아니라 **카탈로그의 최선 priority**를 기준으로 잰다.
  - `quarterly_metric_quality` — (metric_code, bsns_year, quarter). 직접 interim 값과 누적
    차분의 불일치율은 두 소스가 있는 행만 분모로 쓴다.
  - `feature_coverage` — (feature, variant, market, year). `coverage_ratio`(패널 대비 값 존재)와
    `min_names_per_date`(그 해 가장 얇은 날이 `min_names`를 넘는가)를 **둘 다** 낸다. 잘 채워진
    것처럼 보이면서 스캔이 안 되는 해가 있다. `lag1` 변형의 age는 NULL이다 — 같은 행의
    native_t age는 정확히 한 세션만큼 과소평가라, 조용히 틀린 숫자 대신 비운다.
  - `event_coverage` — (event_year, market, reprt_code). 버킷별 수익률 존재 수를 따로 낸다.
    상장폐지·짧은 가격 이력은 늦은 버킷부터 죽이므로 이벤트 단위 한 개 숫자로는 안 보인다.

  `filing_receipt_quality` — 접수연도 단위. 수집 커버리지(연도별 receipt·법인 수), 정정
  구분 두 가지(이 접수가 정정본인지 = report_nm의 `[기재정정]` 계열 마커, 나중에 정정된
  접수인지 = list.json `rm`의 `정` 플래그), 그리고 **정기보고서이면서 정정본인 건수**를
  낸다. 마지막 값이 B-1 6항의 receipt-targeted XBRL 백필 규모다. 부분 수집 연도는 행이
  0으로 채워지지 않고 법인 수가 적게 나오는 것으로 드러난다 — 없는 연도는 아예 행이 없다.

  `capital_change_quality` — (vintage 연도, 보고서 코드) 단위. placeholder와 실이벤트 분리,
  미분류 `isu_dcrs_stle` 비율, economic/mechanical 건수. annual vintage 행에는 §4.4.1 probe의
  창 단위 불일치(`compared_windows`/`feature_changing_windows`/`feature_changing_rate`)를
  붙여, vintage 중복이 일회성 측정이 아니라 상시 산출물이 되게 했다. 비교 대상이 없는
  최신 vintage와 분기보고서 행은 0이 아니라 NULL이다.

  실데이터 확인(2026-08-12, A2 진행 중 상태):

  | 접수연도 | receipts | 법인 | 정기보고서 | 정기보고서 정정 | 정정본 비율 |
  |---|---|---|---|---|---|
  | 2022 | 103,629 | 2,507 | 10,425 | 1,406 | 0.137 |
  | 2023 | 108,048 | 2,564 | 10,525 | 1,175 | 0.129 |
  | 2024 | 114,812 | 2,620 | 11,002 | 1,162 | 0.140 |
  | 2025 | 121,700 | 2,653 | 11,588 | 1,298 | 0.140 |

  **정기보고서 정정이 연 1,150~1,400건**이다. 2022~2025 4개 연도 합이 5,041건으로, B-1이
  capacity 참고값으로 적어둔 "약 5천 건"과 사실상 일치한다. 이제 추정이 아니라 측정값이다.
  2021년은 수집 중이라(법인 1,203/약 2,650) 값이 더 올라간다.
- **Stage 3 — `daily_ic.parquet` 완료(2026-08-29), `cohort_ic.parquet` 미작성.**
  별도 계획(단계 0)으로 처리했다 — `docs/dev/20260829_macro_features/01_design/01_stage0_daily_ic_persistence.md`.
  `per_date_market_rank_ic` 내부는 건드리지 않았다. `scan_cell`이 이미 만들어 둔
  `market_ic`·`daily`·`daily_spread`를 side-channel(`DailyIcSink`)로 넘기는 방식이라,
  sink를 주지 않으면 반환값과 계산 경로가 이전과 완전히 같다. 복제 루프·기간 분할·lag1 직접
  호출은 sink를 받지 않으므로 자동으로 빠진다.

  Phase B 산출물은 run 루트의 `daily_ic.parquet`·`daily_spread.parquet`(§7.1 이름 그대로)이고,
  저장값에서 요약 통계를 다시 만들어 `horizon_ic.parquet`과 대조하는 검사가 run마다 돈다
  (`_SUCCESS.json`의 `daily_ic_reconciled`).

  **`cohort_ic.parquet`은 만들지 않았다.** `fin_sue`에 표본이 없어(6 cell 전부 `insufficient`)
  코호트 IC 시계열을 저장해도 볼 것이 없다. SUE에 표본이 생기면 그때 같은 방식으로 붙인다.
- **Stage 4 — 완료(2026-08-12).** `research/analysis/horizon_scan_phase_b_cards.py`가
  family 8개마다 한 행(`family_summary.parquet`)과 한 카드(`family_cards.md`)를 낸다.
  §6 B-10이 정한 카드 항목을 identity / readiness / coverage / result / next_step 다섯 묶음으로
  나눠 렌더한다.

  두 가지 원칙으로 만들었다.

  1. **blocked가 정상 경로다.** 지금은 38개 셀이 전부 blocked이므로 카드의 일은 "무엇이
     없어서 막혔는지"를 말하는 것이다. 통계 필드는 0/False가 아니라 NULL로 두고, 마크다운은
     `—`로 렌더한다. 평가 셀이 0이면 discovery·screen_pass 건수도 `0`이 아니라 `—`다 —
     "재보니 0"과 "안 쟀다"는 다른 사실이다. 카드 머리말에 이 규칙을 적어 둔다.
  2. **여기서 새로 계산하지 않는다.** 모든 숫자는 앞 단계가 이미 만든 행에서 읽는다.
     coverage는 방금 쓴 `feature_coverage.parquet`/`event_coverage.parquet`을 **다시 읽어서**
     채운다 — 카드가 자기가 요약하는 산출물과 어긋날 수 없게 된다. family 단위 집계(최고 등급,
     최소 q, failed gate 합집합)와 `next_step` 문장만 파생값이다.

  `fin_sue`는 grain이 달라 `event_coverage`에서 커버리지를 가져오고, 연속 패널 개념인
  `min_names_per_date`는 비슷한 숫자를 빌려 오지 않고 비운다. `formula_version`은 호출자가
  주입한다 — 오늘 지문이 있는 건 `ev_net_share_issuance_yoy`(`issuance_v2`)뿐이고, 나머지에
  가짜 버전을 붙이지 않는다.
- **Stage 5 — 완료(2026-08-12).** `research/analysis/horizon_scan_phase_b_report.py`가
  `03b_horizon_scan_results.md`(phase=B)와 `03ab_combined_results.md`(phase=AB)를 낸다.
  Phase A `03a`와 같은 세 규칙을 따른다 — context key 전부 필수(빈 리스트는 되지만 누락은
  에러), **새로 계산하지 않음**(전부 앞 단계 행에서 읽는다), 없는 값은 0이 아니라 `—`.

  두 파일이 답하는 질문이 다르다. `03b`는 Phase B 자기 run — 무엇이 ready였고 source layer가
  어땠고 스캔이 뭘 찾았나. `03ab`는 Phase A 75개와 Phase B ready 셀을 한 BH에 넣었을 때
  **무엇이 바뀌었나**만 본다. 안 바뀐 가설은 안 적는다.

  `limitations`는 고정 문구가 아니라 그 run의 상태에서 만든다 — blocked 셀 수와 missing
  dependency, source quality가 `unmeasured`인 family 목록, 그리고 PIT industry 부재라는
  구조적 제약.

  **publish 범위.** `03b`는 필수 산출물이다 — 없이 발행되면 `_SUCCESS.json`은 완전해 보이는데
  사람이 읽을 게 없다. 동시에 `PHASE_B_CONTENT_HASH_EXCLUDE_NAMES`에 넣었다. 리포트에 run
  타임스탬프가 들어가서, 안 빼면 바이트 단위로 같은 스캔 두 번이 다른 해시를 낸다. 기존에
  발행된 run은 그 파일이 없으므로 제외 대상 추가가 no-op이고 검증이 그대로 통과한다.

  `03ab`는 **§7.1 목록에 없다.** phase=AB에는 parquet과 manifest만 적혀 있다. 계약이 이미
  요구하는 산출물을 렌더링할 뿐 새 통계를 만들지 않으므로 의도적으로 추가했고, 대신
  `required_artifacts`에서는 뺐다 — 아무것도 이 파일에 의존하지 않게.
- §5.5 segment/freshness 진단(8개 축) — B-PR12가 이미 명시적으로 미룬 부분, 아직 스코프 밖.

### 4.3.2 Stage 2가 바로 찾아낸 B-2 결함 3건 (2026-08-12) — **해결**

새 진단 5종을 실제 lake(`snapshot_date=2026-08-09`, vintage fact 1,524,088행)에 돌리자마자
`stock_metric_vintage_fact`의 결함이 드러났다. 처음 둘을 적었고, 고치는 과정에서 같은 뿌리의
세 번째를 찾았다. 원인은 하나다 — 한 filing의 XBRL fact가 **여러 회계기간(당기·전기·전전기)과
연결/별도 두 축**에 걸쳐 있는데 B-2가 filing당 하나뿐인 것처럼 다룬다.

**결함 1 — `statement_period_end`가 최대 2년 어긋난다.** `xbrl_period_by_filing`이
`MIN(COALESCE(instant_date, period_end))`을 쓴다. 그런데 FY2024 사업보고서의 XBRL에는
2022·2023·2024 기간이 다 들어 있어 MIN이 **비교표시용 2022-12-31**을 고른다.
`filing_period_end`는 이 XBRL 값을 `stlm_dt`보다 우선하므로, XBRL이 있는 filing은 전부
기간이 틀린다.

| FY2024 연간, `period_end_source` | `statement_period_end` | 행 수 |
|---|---|---|
| xbrl | **2022-12-31** | 64,688 |
| xbrl | 2023-12-31 | 585 |
| xbrl | 2024-12-31 | 379 |
| stlm | 2024-12-31 | 250 |

`stlm_dt`는 2,545건 전부 2024-12-31로 정확하다 — 즉 올바른 값이 있는데도 틀린 쪽을 우선한다.

**영향 범위는 처음 적은 것보다 좁다(정정).** 처음에 "B-3의 quarter/seq_key, B-4의 구간 조인,
B-6의 이벤트 키가 전부 여기서 파생된다"고 적었는데 확인해 보니 아니다. B-3의 `quarter_ordinal`과
`seq_key`는 `reprt_code`에서 나오고(`seq_key = bsns_year * 4 + quarter_ordinal`), B-4는
`statement_period_end`를 아예 안 쓰며, B-6은 조인 키가 아니라 서술 컬럼으로만 나른다. 실제 피해는
세 가지다 — 마트 grain 컬럼의 값이 틀리고, 그게 B-3·B-6까지 그대로 실려 가고,
`period_end_conflict`가 98.3%에서 울려 진단으로 못 쓴다. 수정 후 B-3 행 수가 1,443,122 →
1,443,105로 거의 안 움직인 것이 이 정정을 뒷받침한다.

**결함 2 — 페어링이 기간과 fs_div를 안 맞춘다.** XBRL 페어링 조인이
`(corp_code, bsns_year, reprt_code, rcept_no, concept_id)`만 쓰고 기간도 `fs_div`도 안 건다.
`_XBRL_RANK_SQL`은 dimension 개수와 Consolidated/Separate만 보고 기간은 아예 안 본다. 결과:

- 같은 concept의 당기·전기·전전기 행이 **동점**이라 임의로 하나가 뽑힌다.
- `SeparateMember`는 점수가 항상 높아(+5) 절대 안 뽑히므로, **OFS 재무제표 행은 언제나 CFS
  XBRL 값과 비교되어 반드시 불일치**한다.

실제 사례(FY2024 `ifrs-full_Revenue`): 재무제표는 CFS 224,422,425,036 / OFS 111,577,891,355인데
XBRL 후보는 연결 3개(FY2022 71,379,940,397 · FY2023 139,211,241,785 · FY2024 224,422,425,036)와
별도 3개다. CFS 행은 1/3 확률로만 맞고 OFS 행은 0%다.

그래서 `value_mismatch_ratio`가 전 연도 **0.51~0.97**로 나온다(2018 연간 0.93, 2023 반기 0.97,
2025 연간 0.58). 계약상 `receipt_value_pairing_error_tolerance: 0`,
`receipt_value_pairing_required: verified_same_receipt`이므로 이대로면 전 구간이 게이트에
걸린다.

**결함 3 — XBRL 출처 metric 값이 비교연도 것일 수 있다.** 결함 2를 고치다 같은 뿌리의 세 번째를
찾았다. `candidates`의 `dart_xbrl_fact_raw` 분기가 기간 필터 없이 rule에 조인하고
`statement_period_end`로는 `pe.period_end`를 붙인다. winner tie-break(`_XBRL_RANK_SQL`)도 기간을
안 보므로, **2년 전 숫자가 당기 날짜를 달고 metric 값이 될 수 있다.** 셋 중 이게 제일 나쁘다 —
비어 있는 게 아니라 들어 있는 채로 틀린다. 해당 metric은 `weighted_avg_shares`,
`diluted_shares`, `depreciation_expense`, `amortization_intangible_assets`다.

**고친 방법.** 세 곳 다 같은 원칙이다 — XBRL fact는 기간과 연결기준을 가지므로 그걸 맞춘다.
공통 CTE `xbrl_scoped`가 `period_end_effective`(`COALESCE(instant_date, period_end)`),
`duration_days`, `xbrl_fs_basis`(축에서 읽은 CFS/OFS)를 계산하고 세 곳이 함께 쓴다.

1. `xbrl_period_by_filing` — MIN → **MAX**. 비교표시는 항상 과거이므로 그 filing 자신의 기간이
   가장 늦다. 더해서 **접수일보다 뒤인 컨텍스트는 버린다** — 제출 시점에 끝나지도 않은 기간을
   그 filing의 보고 기간으로 볼 수 없다.
2. 페어링 조인 — `period_end_effective = pe.period_end`와 `xbrl_fs_basis = f.fs_div`를 추가.
   축이 안 붙은 컨텍스트는 특정 fs_div에 귀속시킬 수 없으므로 페어링 대상에서 뺀다(실측:
   FY2024 연간 2,545건 전부 축 표시가 있어 손실이 거의 없다). 맞는 컨텍스트가 없으면
   `unlinked_receipt`이지 `value_mismatch`가 아니다.
   추가로 `duration_pref`를 뒀다. 분기·반기 filing은 **같은 날 끝나는 3개월 컨텍스트와 누적
   컨텍스트를 둘 다** 싣는데, `thstrm_amount`가 어느 쪽인지는 재무제표 종류에 달렸다 —
   IS/CIS는 3개월, CF는 누적(B-3이 `direct_interim` / `cumulative_reported`로 구분하는 바로 그
   규칙). 선호별로 winner를 하나씩 내고 조인이 `sj_div`로 고른다. 반기 XBRL의 31%가 이
   중복 duration을 갖고 있어 실제로 필요하다.
3. XBRL 후보 분기 — 기간 필터를 추가하고, 남은 후보 중 **누적(가장 긴) duration**을 선호한다.
   이 경로로 오는 metric은 전부 현금흐름표 항목이라 OpenDART가 누적으로 낸다.

**전후 실측** (`snapshot_date=2026-08-09` 전체):

| 지표 | before | after |
|---|---|---|
| vintage fact 행 수 | 1,524,088 | 1,524,061 (−27) |
| `period_end_conflict` | 1,498,788 (**98.3%**) | **0** |
| FY2024 연간 `statement_period_end` 최빈값 | 2022-12-31 (64,688행) | **2024-12-31 (65,926행)** |
| 페어링 `verified` | 212,011 | **1,344,746** |
| 페어링 `value_mismatch` | 1,132,907 | **157** |
| 페어링 `unlinked` | 1,670 | 1,685 |
| XBRL 출처 행 | 11,001 | 10,974 (−27) |
| B-3 행 수 | 1,443,122 | 1,443,105 |

연간 `verified_ratio`는 연도별 0.02~0.42에서 **0.996~1.000**으로, `value_mismatch_ratio`는
0.58~0.98에서 **0.0**으로 바뀌었다(2024년만 4건, 0.000066). 12월 결산이 아닌 회사도 이제 제
날짜(2024-09-30, 2024-03-31, 2024-06-30, 2024-11-30)로 붙는다.

이제서야 `receipt_value_pairing_required: verified_same_receipt`를 게이트로 쓸 수 있다. 그전
값은 데이터가 아니라 조인 버그를 재고 있었다.

**남은 157건은 조인 결함이 아니다.** 1,346,588건 중 0.0117%이고 **Q1(11013)
`net_income`·`controlling_net_income`에 72건이 몰려 있다.** Q1은 3개월과 누적이 같은 기간이라
`duration_pref`가 갈라 주지 못하는 유일한 분기다. 값 자체가 다른 실제 불일치일 수 있으니
`receipt_value_pairing_quality`로 계속 지켜보고, 게이트를 켤 때 별도로 판단한다.

**테스트.** 기존 픽스처는 filing당 XBRL 컨텍스트가 하나뿐이라 세 결함을 다 놓쳤다. 실제 filing의
모양(3개 비교연도 × CFS/OFS)으로 픽스처를 바꾸고 회귀 테스트 8개를 추가했다. 수정 전 코드에
돌리면 그중 7개가 실패한다.

`metrics_normalize.py`(구 canonical 경로)는 golden 픽스처로 동결돼 있으므로 건드리지 않았다.
B-2는 Phase B 전용이라 골든 대상이 아니다.

### 4.4 이 문서 밖 — Phase A 트랙에 남은 것

[07_phase1_acceptance_gate.md](07_phase1_acceptance_gate.md) §6이 조건부 채택으로 판정하며
남긴 두 가지. Phase B와 독립이라 순서에 상관없이 할 수 있다.

- **k=100 top-k 비용 확인** — 이번 게이트는 decile 단위 economic_report만 봤다.
  `predict.py`의 실제 매수 리스트(k=100) 기준 turnover·거래비용 차감 후에도 개선이
  남는지 확인해야 조건이 풀린다. 아직 안 함.
- **h=60 holdout 재평가** — 지금 holdout(2026-06-11~07-31)은 데이터 끝이라 60일 라벨이
  아예 없다. 2026년 10~11월쯤 데이터가 쌓인 뒤 **새 구간으로 한 번만** 연다(이번 구간
  재사용 금지).

### 4.5 완료된 후속 조치 (기록용)

- ~~현재 config로 Phase A 공식 재실행~~ → 2026-08-10 완료(`20260810T141014-7212fe82`),
  등급 결과는 구 run과 동일. §3.1 참고.
- ~~`run_combined_ab`를 실제 두 발행물로 실행~~ → 2026-08-10 완료
  (`20260810T194651-e04c00c7`). 단 B가 0개라 내용은 비어 있다.
- ~~snapshot 2026-08-12로 Phase A 재실행 + 내용 있는 AB~~ → 2026-08-13 완료
  (`20260813T081646-00fa0e76` / `20260813T130307-f9117ce1`). `m_ab=113`,
  `screen_pass=7`, grade A=5·B=2·C=24·D=7. §3.0b 참고.
- ~~snapshot 2026-08-23로 native Phase A/B/AB 재실행~~ → 2026-08-23 완료.
  A `20260823T210913-b649a460`, B `20260823T221441-b649a460`, AB
  `20260823T225913-b649a460`. `m_ab=113`, discovery 56, `screen_pass=12`,
  grade A=5·B=7·C=25·D=1. §3.0 참고.

## 5. 재현

```bash
uv run pytest tests/unit -q                                    # 전체 유닛 테스트 (2026-08-10 기준, B-PR15 포함 819개)
uv run pytest tests/unit/test_horizon_scan_phase_b*.py -v       # Phase B 전용 테스트만
uv run pytest tests/integration/test_horizon_scan_phase_b_smoke.py -v  # 실제 로컬 lake 대상(no-DB 환경은 self-skip)
uv run python -m research.analysis.horizon_scan --phase B       # 실제 Phase B run 발행
uv run python -m research.analysis.horizon_scan --phase AB \
  --phase-a-run-dir <phase=A run 경로> --phase-b-run-dir <phase=B run 경로>
```

**`--phase B`에는 A0 feature mart라는 선행 조건이 있다.** 없으면
`A0 manifest is required before a Phase B scan`으로 죽는다. raw export 뒤에 이걸 먼저 돌린다.

```bash
# snapshot_date를 주지 않는다 — auto_selected=False가 되면 official 자격을 잃는다
uv run python -m research.etl.horizon_scan_inputs --source sj2_remote
```

`compute_all --features`로 대신할 수 없다. 계약 해시가 달라 A0가
`mart cache contract mismatch`로 죽는다(그 경우 `--force`). 자세한 이유는 `00_status.md` §5.

마지막으로 최신 `--phase AB` 인자(§3.0의 두 run, snapshot 2026-08-23):

```bash
BASE=research/output/horizon_scan
HASH=ab0de63411c40ca3b59c1c7e6f8653a8e16d980108bee42f5f8cea8e7fcb6588
uv run python -m research.analysis.horizon_scan --phase AB \
  --phase-a-run-dir "$BASE/phase=A/snapshot_date=2026-08-23/source=sj2_remote/config_hash=$HASH/run_id=20260823T210913-b649a460" \
  --phase-b-run-dir "$BASE/phase=B/snapshot_date=2026-08-23/source=sj2_remote/config_hash=$HASH/run_id=20260823T221441-b649a460"
```

직전 세대(snapshot 2026-08-09, §3.1의 두 run)는 `run_id=20260810T141014-7212fe82` /
`20260810T135845-e04c00c7`였다. Phase B가 0셀이던 시절의 기록이다.

이어서 작업할 때 **가장 먼저 볼 것**은 §3.0 최신 결과다. §3.0a~§3.2는 이전 실행과
`M_B_ready=0` 블로커가 어떻게 풀렸는지의 기록으로만 남긴다.
