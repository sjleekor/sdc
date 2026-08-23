# 03. 구현 순서와 검증 계획

- 작성일: 2026-08-23
- 상태: 코드 구현·unit 검증 완료, 새 A0·실데이터 검증 대기
- 원칙: 성능 개선과 통계 계약 변경을 한 commit에서 섞지 않는다.

---

## 1. 전체 작업 순서

```text
I0 stage timing 기준선
  ↓
I0b analysis kernel code fingerprint 확장
  ↓
I1 Polars native IC kernel
  ↓
I1b legacy 재현성·canonical order 기준선
  ↓
I2 vectorized Newey–West — 정렬된 연속형 경로
  ↓
I2b SUE NW·permutation 입력 정렬 — 별도 통계 contract
  ↓
I2c feature별 formation fetch 재사용
  ↓
I3 기존 계약 parity + SUE contract 변경 검증
  ↓
I4 checkpoint/resume 연결
  ↓
I5 공통 permutation mapping v2 + Phase A cell artifact
  ↓
I6 Phase B의 Phase A null 통계 재사용
  ↓
I7 2-worker 병렬 실행
  ↓
I8 timing에서 필요성이 확인될 때만 SUE bootstrap PoC
  ↓
새 snapshot Phase A → B → AB official run
```

I0~I2와 I2c/I4는 기존 통계 계약을 유지한다. I2b는 기존 SUE 순서 의존 버그를 고치는 별도 통계
수정이다. I5부터는 seed/mapping artifact 계약도 바뀐다. I2b와 I5는 각각 새 contract version과
run lineage를 기록한다.

## 2. I0 — stage timing 추가

### 작업

- Phase A/B 주요 stage 앞뒤에 monotonic timer를 둔다.
- `timings.json`과 run report에 같은 값을 쓴다.
- clean run과 resume run을 구분한다.
- wall time, CPU time, peak RSS를 가능한 범위에서 기록한다.

권장 stage 이름은 다음과 같다.

```text
setup_and_register
real_scan
temporal_placebo
cross_sectional_permutation
phase_b_marts
phase_b_robustness
sue_bootstrap
rank_correlation
joint_permutation
artifact_render
```

### 종료 조건

- smoke run의 stage 합계와 전체 시간이 1초 이내 오차로 맞는다.
- 실패 run에서도 완료된 stage timing이 checkpoint 옆에 남는다.

## 2b. I0b — analysis kernel code fingerprint 확장

현재 run spec의 code hash는 계산 kernel을 다 덮지 못한다(02 §6.1). Phase A는
`research/analysis/horizon_scan*.py`만, Phase B는 `horizon_scan_phase_b*.py`만 hash해서
`research/etl/metrics.py`가 둘 다 빠지고, Phase B에는 공통 runner/permutation module도 빠진다.
Phase A glob은 현재도 `horizon_scan_phase_b*.py`를 포함한다. 공통 상수는 이 범위를 명시적으로
고정하고, Phase B의 더 좁은 범위를 맞추며, 양쪽에 빠진 `metrics.py`를 추가하기 위한 것이다.
I1/I2가 metrics.py를 바꾸기 전에 이 단계를 먼저 끝낸다. 그렇지 않으면 kernel이 다른 두 run이
같은 code hash lineage를 공유한다.

### 작업

- `analysis_kernel_hash`용 파일 목록 상수를 만든다: `research/etl/metrics.py` +
  `research/analysis/horizon_scan*.py` 전체(새 `horizon_scan_native.py` 포함).
- Phase A/B run spec의 `code_paths`가 같은 상수를 쓰게 한다.
- I4 checkpoint fingerprint와 I6 artifact 재사용 검사가 이 hash를 쓴다.

### 종료 조건

- `research/etl/metrics.py` 한 줄을 바꾸면 Phase A/B 양쪽 run_id의 code hash 부분이 달라진다.
- Phase B code hash에 `horizon_scan_runner.py`와 `horizon_scan_permutation.py`가 들어간다.

## 3. I1 — Polars native IC kernel

### 대상 파일

- `research/etl/metrics.py`
- `research/analysis/horizon_scan_runner.py`
- `research/analysis/horizon_scan_phase_b_diagnostics.py`
- 필요하면 새 파일 `research/analysis/horizon_scan_native.py`
- 관련 unit/integration tests

### 작업

- `per_date_market_rank_ic`의 Python group loop를 native expression으로 바꾼다.
- legacy 함수를 즉시 지우지 않고 parity 기간 동안 선택 가능하게 둔다.
- 결과 row를 `trade_date, market` 순으로 고정한다.
- 실제 scan의 quantile spread는 별도 commit으로 native화한다.
- run spec에 `scan_engine=legacy|polars_native_v1`을 기록한다.
- `per_date_market_rank_ic` 호출부 세 곳이 모두 같은 engine 선택을 쓰는지 검사한다.

### unit test

다음 fixture를 각각 legacy/native에 넣고 비교한다.

- 동률이 없는 기본 예제
- feature 동률과 label 동률
- 한 시장만 있는 날짜
- KOSPI/KOSDAQ 크기가 다른 날짜
- `NULL`, `NaN`, `+inf`, `-inf`
- `min_names-1`, `min_names`, `min_names+1`
- feature 또는 label이 상수인 group
- 입력 row 순서를 무작위로 바꾼 경우

### 종료 조건

- `n`, 유효 group 집합, status는 exact match다.
- `rank_ic` 최대 절대 차이는 `1e-12` 이하다.
- 실데이터 대표 cell 10개에서 **IC 집계 함수 구간**의 평균 wall time이 legacy보다 3배 이상
  빨라야 한다. DuckDB fetch와 Newey–West는 이 구간에서 제외한다.
- 같은 10개 cell의 formation fetch 시작부터 최종 통계 반환까지 end-to-end 시간도 따로 기록한다.
  I1 단독의 참고 목표는 평균 1.5배이며, I2까지 합친 속도와 섞어 I1 종료 조건으로 쓰지 않는다.

## 4. I1b — legacy 재현성과 canonical order 기준선

native 결과를 비교하기 전에 legacy가 같은 입력에서 얼마나 흔들리는지 먼저 잰다.

### 작업

- 대표 cell 10개와 permutation smoke를 legacy로 두 번 실행한다.
- IC/t/p/q, group row 순서, 최종 판정의 legacy→legacy 차이를 기록한다.
- formation SQL에 `ORDER BY`가 없는 현재 상태와 canonical order 후보를 비교한다.
- 매 cell SQL 정렬과 feature frame 1회 정렬의 실행시간을 비교한다.
- 채택한 순서 계약을 run spec의 `row_order_contract`에 기록한다.

권장안은 feature별 frame을 읽은 뒤 `(trade_date, market, ticker)` 순서를 한 번만 고정하는 것이다.
4.8M행을 cell마다 SQL에서 다시 정렬하는 방식은 성능을 확인하기 전에는 채택하지 않는다.

### 종료 조건

- native tolerance가 legacy 자체의 흔들림보다 엄격하지 않도록 기준선이 문서에 남아 있다.
- 같은 order contract의 legacy 두 run에서 BH/discovery 판정이 같다.
- canonical order 비용이 I0 timing에 포함돼 있다.

## 5. I2 — vectorized Newey–West

### 대상 파일

- `research/etl/metrics.py`
- `tests/unit/test_research_metrics.py`
- Horizon Scan scan/replicate tests

### 작업

- 거리별 pair를 찾는 NumPy 구현을 추가한다.
- `newey_west_tstat`과 `n_hac_pairs`가 같은 pair index를 재사용하게 한다.
- native 함수는 유효 session index가 strictly increasing인지 검사한다.
- dense session, gap, 큰 gap을 테스트하고, 중복 index는 명시적으로 거부한다.
- `fastmath`를 사용하지 않는다.
- 이 단계에서는 이미 정렬이 보장되는 연속형 경로에만 native 함수를 연결한다.

### randomized parity

고정 seed로 다음 조합을 만든다.

- `n`: 2, 10, 100, 500, 2,500
- `lag`: 0, 1, 2, 5, 20, 59, 119
- session gap 비율: 0%, 5%, 20%
- IC 평균: 0 근처와 양수/음수

legacy와 native의 `t_nw`, pair count를 비교한다.

### 종료 조건

- pair count는 exact match다.
- 유효/무효 판정이 exact match다.
- `t_nw` 상대오차 `|Δt| / max(1, |legacy t|)`가 `1e-12` 이하다.
- IC/p/q와 downstream 판정도 같은 상대오차 기준과 exact decision 기준을 통과한다.
- 2,500일·lag 119 기준 5배 이상 빨라야 한다.

거리별 합산은 legacy와 합산 순서가 다르다. 새 구현 내부의 결정성을 요구하지만 legacy와
bit-identical한 부동소수점 값을 요구하지 않는다.

## 6. I2b — SUE NW·permutation 입력 정렬 contract 수정

이 작업은 성능 개선이 아니라 통계 버그 수정이다. 별도 commit과 contract version으로 다룬다.

범위를 정확히 잡는다(01 §4.4). real SUE scan과 event-ordinal 진단은 이미 cohort를 날짜순으로
정렬한 뒤 `_aggregate_cohort_rows`를 호출하고, issuer/filing bootstrap은 replicate IC 평균만
써서 NW 순서와 무관하다. joint SUE permutation(`_scan_sue_null_row`)은 두 순서 계약이 빠져 있다.
cohort aggregate가 정렬되지 않았고, `_permute_qualifying_sue_ranks`도 SQL 반환 순서대로 group을
돌며 RNG를 소비한다.

### 작업

- `build_event_cohort_frame_sql`이 `original_rcept_no`를 가져오게 하고, SUE permutation 전에
  qualifying frame을 실제 mart grain인
  `(event_formation_date, market, ticker, original_rcept_no)` 순으로 정렬한다. 이 key가 중복이면
  dedupe하지 않고 실패한다.
- canonical frame에서 group 순서와 group 안 row 순서를 고정한 뒤, 기존 per-cell seed와 PCG64
  draw를 그대로 적용한다.
- `_aggregate_cohort_rows` 경계에서 cohort를 `(formation_session_idx, event_formation_date)`
  순으로 정렬하고 session index가 strictly increasing인지 assert한다. 호출부별 정렬에 기대지
  않는다.
- SUE real scan, event ordinal, joint SUE permutation이 모두 이 aggregate 경계를 거치는지
  검사한다. issuer/filing bootstrap은 NW를 계산하지 않고 replicate IC 평균만 쓰므로 경계 적용
  대상이 아니다. real scan과 진단 경로는 이미 정렬돼 있으므로 동작이 바뀌지 않아야 한다.
- `sue_nw_order_contract=sue_nw_sorted_v2`와
  `sue_permutation_order_contract=sue_rank_canonical_v2`를 config, run spec, report에 기록한다. 두
  이름은 각각 정렬된 NW 입력과 canonical permutation 입력을 나타낸다. 변화 범위가 joint null에
  한정된다는 사실은 delta artifact와 report가 명시한다.
- 과거→v2의 joint `permutation_summary`, combined empirical p-value, AB 파생값 차이를 별도
  artifact로 남긴다. real SUE 6개 cell의 t/p·BH·screen·grade는 parity 대상이다.

### 종료 조건

- 같은 qualifying frame의 row와 group 등장 순서를 섞어 넣어도 같은 seed의 permuted row mapping과
  SUE 결과가 같다.
- 같은 sorted-v2 joint SUE permutation을 두 번 실행하면 replicate별 결과가 같다.
- `newey_west_tstat`과 `n_hac_pairs`가 같은 정렬·pair 집합을 쓴다.
- 중복 event grain key나 중복 session index를 넣으면 명시적으로 실패한다.
- real SUE 6개 cell의 t/p가 parity tolerance 안에서 과거와 같다. 넘게 바뀌면 예상된 변화가
  아니라 regression으로 조사한다.
- joint null 파생값 변경을 performance parity 실패로 처리하지 않는다.
- 새 Phase B와 AB run이 과거 contract와 섞이지 않는다.

## 7. I2c — feature별 formation fetch 재사용

### 대상 파일

- `research/analysis/horizon_scan_runner.py`
- `research/analysis/horizon_scan_permutation.py`
- `research/analysis/horizon_scan_phase_b_joint_permutation.py`
- 필요하면 `research/analysis/horizon_scan_native.py`

### 작업

- registry를 feature별로 묶는다.
- feature 하나와 그 feature가 쓰는 target/eligibility 컬럼을 한 번에 fetch한다.
- horizon·universe·sample filtering은 Polars에서 기존 formation 조건과 같은 순서로 실행한다.
- permutation은 이미 메모리에 있는 base frame을 직접 scan하고 DuckDB 재등록·cell별 fetch를
  없앤다.
- feature frame을 canonical order로 한 번만 정렬한다.
- 기존 `scan_cell` schema와 row 수를 유지한다.

### 종료 조건

- Phase A primary scan의 formation fetch가 cell 75회가 아니라 unique feature 최대 12회 수준이다.
- permutation replicate 안에서 cell별 DuckDB fetch가 없다.
- legacy와 eligibility row 수, IC/t/p/q, 판정이 검증 기준 안에서 같다.
- representative replicate에서 I1/I2만 적용한 경로보다 1.5배 이상 빨라야 한다.

## 8. I3 — 기존 계약 parity와 SUE 변경 검증

### 순서

1. 2026-08-12 snapshot을 고정한다.
2. legacy를 두 번 실행해 legacy→legacy 흔들림을 기록한다.
3. legacy와 native를 같은 seed/mapping으로 실행한다.
4. 먼저 family 하나, permutation 3회로 smoke 비교한다.
5. 전체 registry, permutation 10회로 확대한다.
6. 마지막에 현재 config로 **legacy 100회와 native 100회 한 쌍**을 새로 실행해 full parity를
   비교한다.

과거 official run은 현재 config와 config hash가 다르고, replicate seed도 config hash에서 나오므로
legacy 비교 대상으로 재사용할 수 없다. I3 일정에는 legacy Phase A+B+AB 약 10시간과 native
Phase A+B+AB 목표 5시간 30분 이하를 모두 잡는다. 순차 실행 기준 약 16시간짜리 검증이며, legacy와
native를 동시에 돌려 resource contention을 만들지 않는다.

### 비교 대상

- formation row 수
- 날짜×시장 group 수
- `n_dates`, `n_obs`, IC, ICIR, t/p/q
- BH pass와 primary discovery
- temporal placebo per-cell p-value와 pass
- permutation별 discovery count
- Phase B readiness와 screen pass
- evidence grade
- Phase AB combined BH와 empirical p-value

### 숫자 기준

| 항목 | 기준 |
|---|---|
| 정수·status·id 집합 | exact match |
| 연속형 IC, t, p, q | `|Δ| / max(1, |legacy|) <= 1e-12` |
| 연속형 BH/discovery/screen/grade | exact match |
| 연속형 replicate discovery count | exact match |
| report 표의 반올림 값 | exact match |

SUE는 `sue_nw_sorted_v2`와 `sue_rank_canonical_v2`에서 joint null 파생값만 바뀌는 것을 예상한다.
real SUE 6개 cell은 연속형과 같은 parity 대상이다. 다음을 별도 gate로 쓴다.

- real SUE 6개 cell의 t/p·BH·screen·grade가 과거와 parity tolerance 안에서 같다. 넘게 바뀌면
  regression으로 조사한다.
- 같은 sorted-v2 run을 반복했을 때 SUE 6개 cell 값과 판정이 같다.
- SUE 입력 row 순서를 섞어도 결과가 같다.
- 과거→v2의 joint `permutation_summary`, combined empirical p-value, AB 파생값 변화가 delta
  artifact와 report에 모두 적혀 있다.
- SUE 외 연속형 cell의 IC/t/p 원시 통계는 바뀌지 않는다. combined 판정이 바뀌면 null 분포
  변경이 전체 모집단에 미친 영향으로 따로 설명한다.

경계에 가까운 연속형 p/q가 tolerance 안의 차이로 판정을 바꾸면 tolerance로 덮지 않는다. 합산
순서나 engine thread 설정을 고쳐 exact decision을 회복한다.

## 9. I4 — checkpoint/resume 연결

### 대상 파일

- `research/analysis/horizon_scan.py`
- `research/analysis/horizon_scan_phase_b_run.py`
- 세 permutation/robustness module
- CLI argparse와 run spec

### 작업

- top-level runner가 checkpoint root를 만든다.
- checkpoint root는 published `phase=*/` 밖인
  `research/output/horizon_scan_checkpoints/`로 둔다.
- worker는 replicate별 임시 파일을 atomic rename한다.
- `--resume`은 compatible checkpoint만 읽는다.
- schema/config/code/contract가 다른 checkpoint는 거부한다.
- 모든 checkpoint 파일(v1 포함)에 02 §6.1의 fingerprint block을 넣는다: `registry_hash`,
  `a0_manifest_hash`, Phase B는 readiness-freeze population hash, `smoke_family`·holdout
  override, scan/row-order/SUE NW/SUE permutation/mapping contract, `analysis_kernel_hash`,
  experiment별 요청 replicate 수, `duckdb_version`, `polars_version`, `numpy_version`.
- resume은 fingerprint 전체를 exact match로 검사하고, fingerprint가 없는 파일은 거부한다.
  `--smoke-family`와 override는 config hash를 바꾸지 않으므로 경로만으로는 smoke와 official을
  구분할 수 없다.
- coordinator는 같은 phase/snapshot/source/config/experiment/contract checkpoint namespace에
  non-blocking exclusive advisory lock을 잡고 실행이 끝날 때까지 유지한다. 두 번째 coordinator는
  checkpoint를 읽거나 쓰기 전에 실패한다.
- 완료된 replicate를 다시 계산하지 않는다.
- run spec에 reused/computed replicate 수를 기록한다.

I4의 `contract=v1` replicate 파일은 현재 helper가 이미 반환하는 summary만 담는다.

- A cross-sectional: 발견 수와 min p/q, `max_abs_t`
- temporal: shift와 hypothesis별 `abs_t_nw`
- issuer/filing bootstrap: `ic_mean`
- A+B joint v1: combined discovery count

cell-level parquet은 I5에서 처음 추가한다. I4 resume 설계가 아직 없는 cell row에 기대지 않게 한다.

### 장애 test

- 10회 중 4회 뒤 강제 중단하고 이어서 10회 완주
- replicate 파일 하나를 잘라낸 뒤 해당 파일만 재계산
- config hash를 바꾸면 전체 checkpoint 거부
- 같은 config에서 `--smoke-family`로 만든 checkpoint를 official `--resume`이 거부
- 같은 config에서 replicate 수 override가 다른 checkpoint 거부
- holdout override가 다른 checkpoint 거부
- DuckDB/Polars/NumPy 중 하나의 버전만 달라도 checkpoint 거부
- worker 수를 1→2로 바꿔 resume
- 중복 replicate와 빠진 hypothesis id 감지
- 같은 checkpoint namespace에서 두 coordinator를 띄우면 두 번째 실행을 즉시 거부

### 종료 조건

- clean run과 resume run artifact가 같다.
- smoke run checkpoint가 official run에 재사용되지 않는다.
- library 버전이 다른 replicate가 한 artifact에 섞이지 않는다.
- 같은 checkpoint namespace의 동시 run이 lock으로 막힌다.
- 동시에 실행한 worker가 같은 checkpoint 파일을 덮어쓰지 않는다.
- 실패 시 발행 디렉터리에 `_SUCCESS.json`이 생기지 않는다.

## 10. I5 — 공통 mapping v2와 Phase A artifact

### 작업

- `joint_cs_v2` mapping contract를 코드와 config에 고정한다.
- mapping은 `(mapping_contract_version, replicate_index, config_hash, trade_date, market)`를
  날짜×시장별 seed key로 삼고, group 안 ticker canonical order에서 결정한다.
- seed 방식은 02 §4.2에 고정한 하나의 계약만 쓴다. seed key를 `0x1F` 구분 UTF-8로 직렬화하고
  SHA-256 digest 32 byte를 big-endian 정수로 바꿔 `SeedSequence` entropy로 넣은 뒤
  `default_rng`(PCG64)의 `permutation(n)`을 호출한다. Philox 대안과 날짜×시장별 32-bit seed는
  쓰지 않는다.
- mapping 전에 Phase A/B panel 모두에서 `(trade_date, market, ticker)`가 unique인지 assert한다.
  중복이면 dedupe하지 않고 실패한다.
- `mapping_hash`는 02 §4.2의 canonical byte 정의로 계산한다. `(trade_date, market)` group key,
  canonical ticker sequence, int32 little-endian permutation index를 모두 포함해 같은 크기의 다른
  ticker 집합도 구분한다.
- Phase A가 이미 계산하는 100 replicate × 75 cell 결과를
  `permutation_cell_stats.parquet`으로 저장한다. 이 단계 때문에 새 scan을 추가하지 않는다.
- summary를 cell artifact에서 다시 계산한다.
- run spec과 manifest에 mapping/artifact hash를 넣는다.
- Phase A와 B의 `family` 집합이 겹치지 않는지 scan 전에 assert한다.

`joint_cs_v2`는 continuous 날짜×시장 mapping만 바꾼다. SUE joint permutation은 I2b에서 고정한
기존 per-cell seed·PCG64 draw와 `sue_rank_canonical_v2` row order를 쓴다. temporal placebo도 기존
circular-shift seed/draw 계약을 유지한다.

cell artifact에는 최소한 다음 필드를 넣는다.

```text
mapping_contract_version, replicate, mapping_hash,
hypothesis_id, family, feature, scan_type, h_start, h_end, expected_sign,
status, ic_mean, t_nw, p_nw, n_dates, n_obs
```

`isolated_spike`, BH q-value, discovery는 저장값을 그대로 믿지 않는다. Phase B에서 A+B row를
합친 뒤 `apply_global_bh`로 다시 계산한다. 현재 config에서 A/B family는 겹치지 않지만, 앞으로
family 이름이 추가돼도 이 전제가 깨지지 않게 계약으로 고정한다.

### 중요한 구분

이 단계부터 과거 `v1` replicate와 숫자가 같을 필요는 없다. seed draw가 달라질 수 있기 때문이다.
대신 다음을 확인한다.

- 같은 v2 run을 두 번 실행하면 결과가 같다.
- worker 수 1/2에서 결과가 같다.
- 모든 feature가 날짜×시장 안에서 같은 mapping을 쓴다.
- feature 간 상관과 `NULL` 패턴이 row vector 단위로 이동한다.
- real scan 결과는 바뀌지 않는다.

v2에서는 A-only null과 joint null이 같은 draw를 쓴다. 각 permutation test는 유효하지만 두
empirical p-value가 서로 독립된 draw에서 나온 값은 아니다. report에 이 해석상의 제한을 적는다.

### 종료 조건

- Phase A 100 replicate × 75 cell = 7,500행이 빠짐없이 있다.
- `(trade_date, market, ticker)` 중복을 넣은 test panel이 mapping 전에 실패한다.
- replicate별 A-only BH summary가 artifact 재계산과 같다.
- artifact의 registry 필드로 combined `isolated_spike`와 BH 판정을 다시 계산할 수 있다.
- A/B family overlap을 넣은 test config가 scan 전에 실패한다.
- artifact content hash 검증이 깨진 파일을 잡는다.

## 11. I6 — Phase B에서 Phase A 통계 재사용

### 작업

- Phase B CLI에 compatible Phase A run dir를 명시한다.
- A artifact의 snapshot/config/A0/mapping/content hash와 registry 필드를 확인한다.
- Phase B가 자기 panel에서 replicate별 continuous mapping을 재생성하고, 각 `mapping_hash`가 A
  artifact의 같은 replicate 저장값과 exact match인지 B cell scan 전에 확인한다.
- Phase B run spec에 참조한 Phase A run id와 permutation cell artifact content hash를 기록한다.
- Phase B는 B continuous와 SUE만 계산한다.
- A/B family disjoint와 별개로 A∪B `hypothesis_id`가 유일한지 assert한다. 그다음 A+B cell row를
  replicate별로 합치고 `apply_global_bh`를 다시 호출해 `isolated_spike`, q-value, discovery를
  계산한다.
- 필요할 때만 전체 A+B 재계산 fallback을 명시적으로 허용한다.
- AB preflight에 A/B 상호 동일성 검사를 넣는다(02 §9): A/B의 snapshot·source·A0 manifest
  hash가 서로 같고, mapping/scan engine/row order/SUE NW/SUE permutation contract와
  `analysis_kernel_hash`가 같고, B run spec이 참조한 Phase A run id·artifact hash가 AB에 넘긴
  Phase A run과 같다. 하나라도 다르면 combined BH와 empirical p-value 계산 전에 실패한다.

### parity test

같은 `joint_cs_v2` mapping으로 다음 두 경로를 비교한다.

```text
경로 1: Phase B가 A+B 전체를 다시 scan
경로 2: 저장된 A 75개 + 새 B 통계를 merge
```

비교 항목은 replicate별 p-value 집합, `isolated_spike`, BH q-value, discovery id,
discovery count다.

부정 test도 넣는다.

- A/B panel의 날짜×시장별 ticker 집합 하나를 바꾸면 `mapping_hash` 불일치로 B scan 전에 실패한다.
- A/B에 같은 `hypothesis_id`를 넣으면 combined BH 전에 실패한다.
- 같은 config hash로 만든 **다른** Phase A run을 AB에 넘기면 artifact hash 불일치로 combined
  계산 전에 실패한다.

### 종료 조건

- 두 경로의 판정과 summary가 exact match다.
- Phase B joint continuous scan 수가 replicate당 107개에서 32개로 줄어든다.
- B가 재생성한 모든 replicate `mapping_hash`가 A artifact와 exact match다.
- A∪B `hypothesis_id` 유일성 검사가 combined BH 전에 실행된다.
- Phase A artifact가 맞지 않으면 scan 전에 실패한다.
- AB가 B run이 참조한 Phase A와 다른 Phase A 입력을 받으면 계산 전에 실패한다.

## 12. I7 — 2-worker 병렬 실행

I2c 뒤에는 fetch와 single-worker compute 비용이 먼저 줄어든다. 따라서 병렬 실행은 3시간 목표의
필수 전제로 잡지 않고, 남은 replicate 구간을 더 줄이는 추가 개선으로 평가한다.

### benchmark matrix

| workers | worker threads | 목적 |
|---:|---:|---|
| 1 | 기본 | 결정성과 단일 worker 기준선 |
| 2 | 4 | 보수적 병렬 기준 |
| 2 | 6 | CPU 활용 증가 효과 |
| 3 | 4 | 메모리 여유가 있을 때만 |

각 조합에서 다음을 기록한다.

- wall/CPU time
- peak RSS
- replicate당 평균·p50·p95 시간
- checkpoint write 크기와 시간
- page fault와 spill 여부
- 1-worker 대비 속도

### 채택 기준

- 2 workers가 1 worker보다 1.5배 이상 빠르다.
- peak RSS가 24GB 목표 안에 든다.
- 결과 판정이 exact match다.
- 3 workers는 2 workers보다 15% 이상 빠를 때만 채택한다.

기본 worker 수는 benchmark 결과로 고정한다. CPU 개수에 따라 매번 자동으로 최대치를 고르는
방식은 official 재현성과 운영 예측을 어렵게 하므로 쓰지 않는다.

## 13. I8 — SUE bootstrap 최적화 조건

I7 이후 새 timing에서 `sue_bootstrap` 절대 시간이 10분을 넘을 때만 backlog에서 꺼낸다. 현재
Phase B 앞단 39분에는 real scan, robustness 축, issuer/filing-cycle bootstrap이 함께 들어가므로
기존 mtime만으로 `sue_bootstrap` 비중을 따로 추정하지 않는다.

### PoC

- issuer/event row를 정수 cluster id로 encode한다.
- draw를 cluster별 weight로 바꾼다.
- 물리적으로 행을 복제한 legacy 결과와 weighted rank 결과를 비교한다.
- 6개 event bucket × issuer/filing-cycle × 999회에서 parity와 시간을 잰다.

### 채택 기준

- bootstrap p-value와 pass가 exact match다.
- replicate IC 최대 절대 차이가 `1e-12` 이하다.
- 전체 bootstrap 구간이 3배 이상 빨라야 한다.

Numba는 이 조건을 Polars/NumPy만으로 달성하지 못할 때만 넣는다.

## 14. 성능 acceptance gate

단계별 최소 목표는 다음과 같다.

| 완료 단계 | Phase A | Phase B | 설명 |
|---|---:|---:|---|
| I3 native kernel + fetch 재사용 | 2시간 30분 이하 | 3시간 이하 | 기존 seed/mapping 유지 |
| I6 A 통계 재사용 | 유지 | 1시간 45분 이하 | A 75개 재계산 제거, 보수적 gate |
| I7 병렬 실행 | 1시간 30분 안팎 | 1시간 30분 이하 | 장비 여건에 따라 재조정 |
| 최종 A+B+AB | — | — | 3시간 이하 목표 |

I6의 1시간 45분은 앞단 39분도 native kernel과 fetch 재사용으로 줄어든다는 전제를 억지로 시간
산수에 넣지 않은 보수적 기준이다. I2c 결과가 좋으면 실제 목표는 더 낮출 수 있다.

시간 목표를 못 맞춰도 parity를 통과한 개선은 버리지 않는다. timing artifact를 보고 다음 병목을
다시 정한다. 반대로 빨라도 판정 parity를 통과하지 못하면 채택하지 않는다.

## 15. rollout과 롤백

### rollout

1. legacy/native 선택 경로를 둔 채 legacy 재현성 기준선과 full parity를 끝낸다.
2. `scan_engine`과 `row_order_contract`를 run spec에 기록한다.
3. SUE NW/draw sorted-v2를 별도 contract/run으로 발행하고 과거 값과 delta를 검토한다.
4. `joint_cs_v2` mapping과 Phase A cell artifact를 별도 config lineage로 발행한다.
5. 새 config hash로 Phase A → B → AB를 한 번 완주한다.
6. 결과 문서와 acceptance gate가 새 run을 가리키게 한다.
7. 한 번 더 같은 입력으로 결정성 run을 실행한다.
8. 그 뒤 native를 기본값으로 바꾼다.

### 롤백

- legacy code path는 새 official run 2회가 안정적으로 끝날 때까지 남긴다.
- 과거 run 디렉터리와 A0 mart를 덮어쓰지 않는다.
- `scan_engine`, `row_order_contract`, `sue_nw_order_contract`, `sue_permutation_order_contract`,
  `mapping_contract_version`이 다른 artifact를 섞지 않는다.
- 문제가 생기면 config/run dir를 되돌리는 대신 명시적으로 legacy engine으로 새 run을 발행한다.

## 16. 완료 정의

다음을 모두 만족하면 작업을 끝낸다.

- [ ] stage timing artifact가 A/B에 있다.
- [ ] `analysis_kernel_hash`가 `research/etl/metrics.py`와 공통 module을 포함한다.
- [ ] legacy→legacy 재현성 기준선과 canonical order 비용이 기록돼 있다.
- [ ] Polars native IC와 vectorized Newey–West가 parity test를 통과했다.
- [ ] SUE sorted-v2가 입력 순서 불변 test를 통과했고, real SUE 6개 cell parity와 joint null
      파생값 delta가 발행됐다.
- [ ] formation fetch가 unique feature 단위로 줄고 permutation의 cell별 DuckDB fetch가 없어졌다.
- [ ] full 100-replicate 기존 계약 parity가 통과했다.
- [ ] replicate checkpoint로 중단된 실행을 이어갈 수 있다.
- [ ] checkpoint fingerprint가 smoke/official 혼입을 거부한다.
- [ ] checkpoint fingerprint가 DuckDB/Polars/NumPy 버전이 다른 resume을 거부한다.
- [ ] 같은 checkpoint namespace의 동시 run이 coordinator lock으로 막힌다.
- [ ] Phase A cell-level permutation artifact가 발행된다.
- [ ] cell artifact에 combined BH 재계산에 필요한 registry 필드가 있다.
- [ ] A/B family disjoint assert가 적용됐다.
- [ ] B가 재생성한 replicate별 `mapping_hash`가 Phase A artifact와 exact match다.
- [ ] A∪B `hypothesis_id` 유일성 assert가 combined BH 전에 적용됐다.
- [ ] Phase B가 Phase A 75개 null 통계를 다시 계산하지 않는다.
- [ ] AB preflight가 A/B 상호 동일성과 B가 참조한 Phase A artifact 일치를 검사한다.
- [ ] 1/2 workers 결과가 같다.
- [ ] peak RSS와 최종 실행시간 목표를 확인했다.
- [ ] Phase A → B → AB 새 official run이 완주했다.
- [ ] 결과 문서가 새 run id와 artifact hash를 가리킨다.

## 17. 2026-08-23 구현 기록

다음 항목은 코드와 synthetic/unit test까지 끝냈다.

- `per_date_market_rank_ic(..., engine="polars_native_v1")`와 legacy parity 경로
- gap-aware vectorized Newey–West와 strictly increasing session contract
- feature별 formation frame 재사용과 canonical row order
- SUE `sue_nw_sorted_v2`·`sue_rank_canonical_v2` 정렬 contract
- `analysis_kernel_hash`와 A0 manifest content hash를 포함한 checkpoint fingerprint
- replicate별 atomic checkpoint, coordinator lock, `--resume`, `--checkpoint-root`
- `joint_cs_v2` mapping hash와 Phase A `permutation_cell_stats.parquet`
- Phase B의 Phase A null cell 통계 재사용과 mapping hash preflight
- A/B family·hypothesis 중복 및 AB contract preflight
- `--workers`; LakeConfig가 있는 Phase A는 process worker, synthetic/B frame 경로는 큰 frame 복사를 피하는 bounded worker pool
- A/B `timings.json`과 report timing context

검증 결과는 `tests/unit` **1,333개 통과**이며, 추가한 directory checkpoint와 1/2 worker
결정성 테스트도 통과했다. 현재 local lake의 최신 A0 manifest는 `2026-08-12`의 이전 config hash이고
현재 config hash와 다르다. 따라서 새 A0를 만든 뒤 Phase A → B → AB official run, timing 목표,
실데이터 parity와 peak RSS를 별도로 확인해야 한다. I8 SUE bootstrap PoC는 timing에서 10분 이상
병목으로 확인될 때만 시작한다.

## 18. 2026-08-23 리뷰 반영

리뷰에서 실제 실행을 막거나 목표를 거꾸로 만드는 항목은 반영했다.

- Phase B report의 `timings` 전달 오류를 고치고 report에 stage timing을 렌더링했다.
- Phase A process worker의 잘못된 `initkwargs`를 positional `initargs`로 바꾸고, child process로 보내기 전에 `base_frame`을 제거했다.
- registry scan은 feature 하나의 frame만 유지하고, 그 feature가 쓰는 target column만 fetch한다.
- `joint_cs_v2`는 canonical sort와 mapping을 한 번만 수행하며, Phase B 재사용 경로는 검증한 mapping을 그대로 적용한다.
- `scan_engine`은 config hash에서 제외해 legacy/native가 같은 seed와 mapping을 공유하게 했다. engine 선택은 run spec과 checkpoint fingerprint에는 남긴다.
- temporal placebo의 `workers`를 실제 bounded worker pool에 연결하고, robustness namespace에도 coordinator lock을 걸었다.
- SUE permutation은 `ticker`와 `original_rcept_no`를 필수 grain key로 요구한다. temporal checkpoint는 SUE contract를 사용하지 않는다.
- native/legacy IC·Newey–West parity, process worker path, mapping 중복 key, corrupt checkpoint 복구, report timing을 unit test로 추가했다.

아직 새 A0와 실데이터 official run은 하지 않았다. 따라서 실제 RSS, Phase A/B/AB 완주 시간,
100회 mapping hash exact match와 full-data parity는 새 A0를 만든 뒤 별도로 확인해야 한다.
