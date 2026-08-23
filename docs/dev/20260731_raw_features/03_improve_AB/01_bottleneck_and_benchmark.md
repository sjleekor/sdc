# 01. Phase A/B 병목과 benchmark 근거

- 작성일: 2026-08-23
- 목적: 최적화 전에 현재 비용이 어디서 생기는지 고정한다.
- 결론: Phase A/B의 주 병목은 mart 생성이 아니라 replicate 안에서 되풀이하는 cell scan이다.

---

## 1. 실행시간 기준선

기존 official run의 기준선은 다음과 같다.

| Phase | run | 구간 | 소요 |
|---|---|---|---:|
| A | `20260813T081646-00fa0e76` | 08:16~12:57 | 4시간 41분 |
| B | `20260812T231507-f9117ce1` | 23:15~04:49 | 약 5시간 34분 |
| B | `20260815T133014-8f47b5fc` | 13:30~19:06 | 약 5시간 37분 |
| AB | 여러 run | artifact merge | 1초 미만 |

운영 문서에서는 Phase B를 약 5시간 30분으로 적는다. 이 문서에서는 파일 시각으로 나뉘는
세부 구간을 설명하기 위해 초 단위 시각을 함께 쓴다.

## 2. Phase B 구간별 비용

`run_phase_b_core`의 코드 순서와 산출물 mtime을 대조했다. 두 run 모두 같은 모양이다.

| run | readiness freeze | rank correlation 완료 | 최종 완료 | 마지막 구간 |
|---|---:|---:|---:|---:|
| `20260812T231507-f9117ce1` | 23:15:10 | 23:54:38 | 04:49:29 | 4시간 54분 51초 |
| `20260815T133014-8f47b5fc` | 13:30:16 | 14:09:33 | 19:06:58 | 4시간 57분 25초 |

`primary_feature_rank_correlation.parquet`을 쓴 다음 실행하는 큰 단계는 A+B joint permutation이다.
`permutation_summary.parquet`은 그 계산이 끝난 뒤 쓰인다. 따라서 마지막 약 4시간 55~57분은
joint permutation 구간이다. `manifest.json`을 쓰기 전에 실제 scan과 robustness·bootstrap이
이미 끝난다는 코드 순서도 확인했다. 마지막 구간의 귀속은 단순 추정이 아니다.

첫 run을 기준으로 나누면 다음과 같다.

```text
전체 약 5시간 34분
  ├─ Phase B mart 등록·실제 scan·robustness·bootstrap·진단: 약 39분
  └─ A+B joint permutation: 약 4시간 55분, 전체의 약 88%
```

joint permutation은 replicate 하나마다 다음을 다시 계산한다.

- Phase A continuous primary 75개
- Phase B continuous primary 32개
- Phase B SUE event primary 6개
- 위 모집단에 combined BH

continuous cell만 100회 × 107개다. Phase A에서 이미 계산했던 75개도 Phase B가 다시 scan한다.

## 3. 현재 cell scan 구조

`scan_cell`은 한 cell마다 다음 순서로 실행된다.

1. DuckDB `analysis_panel`에서 필요한 7개 컬럼을 다시 읽는다.
2. 결과를 Polars `DataFrame`으로 가져온다.
3. `per_date_market_rank_ic`이 날짜×시장 group을 Python `for` loop로 돈다.
4. group마다 NumPy 배열을 만들고 두 입력을 average-rank로 다시 rank한다.
5. 일별 IC로 합친 뒤 Newey–West를 Python loop로 계산한다.
6. 실제 scan이면 quantile spread도 날짜×시장 group별 Python loop로 계산한다.

replicate scan은 spread를 생략하지만 1~5를 모든 cell에 반복한다.

### 3.1 주요 코드 위치

| 구간 | 파일·함수 | 현재 특징 |
|---|---|---|
| 날짜×시장 IC | `research/etl/metrics.py::per_date_market_rank_ic` | group별 Python loop |
| average rank | `research/etl/metrics.py::_rankdata` | group마다 sort/unique |
| Newey–West | `research/etl/metrics.py::newey_west_tstat` | 모든 시작점에서 뒤쪽 배열 재검사 |
| Phase A permutation | `research/analysis/horizon_scan_permutation.py::run_cross_sectional_permutation` | replicate와 cell 모두 순차 실행 |
| Phase A temporal | 같은 파일의 `run_temporal_placebo` | replicate와 long cell 순차 실행 |
| Phase B bootstrap | `horizon_scan_phase_b_robustness.py::run_cluster_bootstrap` | replicate마다 DataFrame 재조립 |
| Phase B joint | `horizon_scan_phase_b_joint_permutation.py::run_combined_cross_sectional_permutation` | A 75개까지 다시 scan |

## 4. 실데이터 cell benchmark

### 4.1 조건

- snapshot: `2026-08-12`, `source=sj2_remote`
- 예제: `px_mom_12_1|px_mom_12_1|cum|0|120`
- formation rows: 4,777,737
- 날짜×시장 group: 4,920
- 일별 IC: 2,460일
- Python: 3.12 계열
- DuckDB: 1.5.4
- Polars: 1.41.2
- NumPy: 2.4.4
- 실행 환경: macOS arm64

현재 config hash와 과거 A0 cache hash가 달라 official runner는 해당 cache를 거부했다. 이
benchmark는 과거 parquet을 read-only view로 직접 등록해 동일한 `analysis_panel`, formation SQL,
metric 함수를 호출했다. 파일과 cache metadata는 바꾸지 않았다.

### 4.2 날짜×시장 Spearman

| 항목 | 시간 |
|---|---:|
| DuckDB formation fetch | 0.657초 |
| 현재 Python group loop | 1.408초 |
| Polars native `group_by + pl.corr(method="spearman")` | 0.442초 |
| group 계산 속도 차이 | 3.2배 |

현재 결과와 Polars native 결과의 최대 절대 차이는 `1.1102230246251565e-16`이었다. 같은
average-rank tie 처리와 같은 유효 행을 사용한 결과다.

DuckDB 안에서 average-rank window와 `corr()`까지 처리하는 후보도 측정했다.

| 구현 | formation부터 group IC까지 |
|---|---:|
| 현재 DuckDB fetch + Python group | 1.977초 |
| DuckDB native SQL | 0.978초 |
| 차이 | 2.02배 |

위의 fetch `0.657초`와 Python group `1.408초`는 component를 따로 잰 값이고, `1.977초`는 formation
fetch부터 group IC까지 한 번에 다시 잰 end-to-end 값이다. 서로 다른 benchmark iteration이라
앞의 두 수를 더한 `2.065초`와 일치하지 않는다.

DuckDB SQL도 쓸 수 있지만 Polars native가 더 빠르고, 현재 replicate frame이 이미 Polars에
있다는 장점이 있다.

### 4.3 Newey–West

실제 2,460일, `lag=119` cell에서 현재 구현은 약 0.224초가 걸렸다. 현재 구현은 각 `i`에서
뒤쪽 전체 session 거리를 만든 뒤 `lag` 안의 pair를 고른다. 날짜 수가 늘면 사실상 제곱으로
커진다.

비슷한 길이와 session gap을 가진 합성 배열에서 `lag=1..119`별 유효 pair만 계산하는
prototype을 비교했다.

| 구현 | 시간 | t-stat 차이 |
|---|---:|---:|
| 현재 중첩 loop | 0.205초 | — |
| 배열 기반 prototype | 0.0138초 | `2.38e-14` |
| 차이 | 14.9배 | bit-identical하지 않음 |

별도 재현에서는 12.8배, t-stat 차이 `1.58e-13`이 나왔다. 입력과 gap에 따라 부동소수점 차이가
달라지므로 고정 절대값 하나만 acceptance 기준으로 삼지 않는다. 거리별로 묶는 새 구현은 legacy와
합산 순서가 다르다. 상대오차 `|Δ| / max(1, |legacy|) <= 1e-12`와 판정 일치를 함께 검사한다.

이 prototype은 공식 코드에 넣지 않았다. 실제 전체 cell 검증을 통과한 뒤 교체한다.

### 4.4 SUE event의 입력 순서 문제

성능 작업 전에 분리해야 할 기존 통계 버그가 확인됐다. 다만 영향 범위는 SUE 전체가 아니라
joint permutation 경로에 한정된다.

- `newey_west_tstat`은 입력 배열을 정렬하지 않고 `i < j`인 pair만 사용한다. 입력이 session
  오름차순일 때만 올바른 pair 집합이 나온다.
- `n_hac_pairs`는 같은 session index를 내부에서 정렬한다.
- 연속형 `scan_cell`은 일별 IC를 `trade_date` 순으로 정렬하므로 안전하다.
- SUE formation SQL과 상위 mart에는 결과 순서를 고정하는 `ORDER BY`가 없다. `_pool_qualifying_by_date`는
  `group_by(maintain_order=True)`로 frame row 순서를 그대로 이어받는다.
- joint SUE의 `_permute_qualifying_sue_ranks`도 `maintain_order=True`로 group을 돌면서 RNG 하나를
  순서대로 소비한다. 따라서 group 등장 순서나 group 안 row 순서가 달라지면 같은 seed에서도
  다른 row에 draw가 배정된다.

호출부별 상태는 다음과 같다.

| 경로 | 정렬 여부 | 영향 |
|---|---|---|
| real SUE scan `scan_event_cohort_cell` | `cohort_rows.sort(key=r[0])` 후 aggregate (`horizon_scan_phase_b_scan.py:426`) | 안전 |
| event-ordinal 진단 | `sorted(...)` 후 aggregate (`horizon_scan_phase_b_diagnostics.py:182`) | 안전 |
| issuer/filing bootstrap | replicate IC 평균만 사용, Newey–West 미사용 | 순서 무관 |
| joint SUE permutation `_scan_sue_null_row` | 정렬 없이 aggregate (`horizon_scan_phase_b_joint_permutation.py:102`) | **영향** |

정렬 없는 입력에서 `t_nw`가 어떻게 달라지는지는 검토 과정의 예로 확인했다. pair 수가 91로
같은데 정렬 입력은 `3.1472788901`, 섞은 입력은 `2.8567137573`이었다. 낮은 자리수 차이가 아니라
판정에 영향을 줄 수 있는 차이다.

따라서 이 버그로 바뀔 것으로 예상하는 값은 joint SUE null 분포와 그것으로 만든
`permutation_summary`, combined empirical p-value, AB 파생값이다. 과거 real SUE 6개 cell의
t/p·BH·grade는 이미 정렬된 입력으로 계산됐으므로 parity 대상이다. 수정은
`_aggregate_cohort_rows` 경계에서 정렬과 strict monotonic assert를 강제하고, SUE permutation
전에 실제 event grain `(event_formation_date, market, ticker, original_rcept_no)`으로 frame을
정렬하는 방식으로 한다. 이 key가 중복이면 dedupe하지 않고 실패한다. 두 순서 규칙은 별도 통계
contract version으로 발행한다. real SUE 값이 tolerance를 넘게 바뀌면 예상된 변화가 아니라
regression으로 조사한다.

### 4.5 replicate base frame

Phase A cross-sectional permutation이 한 번 읽는 base frame도 측정했다.

| 항목 | 값 |
|---|---:|
| 행 | 5,373,848 |
| 컬럼 | 65 |
| primary feature 컬럼 | 12 |
| target·eligibility 컬럼 | 45 |
| Polars 추정 크기 | 약 1.88GB |
| 최초 fetch | 1.55초 |
| 현재 group별 feature permutation | 4.38초/replicate |

feature permutation 자체는 100회여도 약 7분대다. 수 시간의 대부분은 그 뒤에 75개 또는
107개 cell을 하나씩 다시 scan하는 데 쓰인다.

1.88GB frame은 병렬 worker 수를 정할 때 중요하다. worker마다 base/permuted frame을 복제하면
CPU보다 메모리와 memory bandwidth가 먼저 한계에 닿을 수 있다.

### 4.6 legacy 재현성 기준선

일반 formation SQL도 `ORDER BY`가 없다. `per_date_market_rank_ic`은
`group_by(maintain_order=True)`를 쓰므로 DuckDB가 돌려준 group·row 순서가 부동소수점 합산 순서에
영향을 줄 수 있다.

native와 비교하기 전에 legacy를 같은 입력으로 두 번 실행해 legacy→legacy 차이를 먼저 기록한다.
매 cell query에 대규모 `ORDER BY`를 바로 추가하면 정렬 비용이 커질 수 있다. feature별 frame을
한 번 읽는 경로에서 `(trade_date, market, ticker)` canonical order를 한 번만 만드는 방식과 SQL
정렬 방식을 함께 benchmark한 뒤 결정한다.

## 5. 비용 모델

Phase A의 큰 반복 수는 다음과 같다.

```text
cross-sectional permutation: 100 × 75 = 7,500 cell scans
temporal placebo:             100 × 13 = 1,300 cell scans
합계:                                      8,800 cell scans
```

Phase B joint permutation은 continuous만 다음과 같다.

```text
100 × (Phase A 75 + Phase B 32) = 10,700 cell scans
```

cell 하나를 1초 줄이면 Phase A 반복 구간은 이론상 약 2시간 27분, Phase B joint 구간은 약
2시간 58분 줄어든다. 실제로는 horizon별 행 수, temporal join, cache, 메모리 경쟁이 달라 그대로
비례하지는 않는다. 하지만 최적화 우선순위를 정하기에는 충분한 크기다.

Polars IC와 배열 기반 Newey–West만 적용하면 예제 cell의 비용은 대략 다음 모양이 된다.

| 구간 | 현재 | native kernel 뒤 | 비중 |
|---|---:|---:|---:|
| DuckDB formation fetch | 0.657초 | 0.657초 | 약 59% |
| 날짜×시장 group | 1.408초 | 0.442초 | 약 40% |
| Newey–West | 0.224초 | 약 0.014초 | 약 1% |
| 합계 | 약 2.29초 | 약 1.11초 | — |

즉 native kernel 다음 병목은 formation fetch다. Phase A 75개 cell은 primary feature 컬럼 12개를
여러 horizon에 반복해서 쓴다. feature마다 필요한 target·eligibility 컬럼을 한 번에 가져오고
horizon별 filtering을 Polars에서 하면 fetch를 75회에서 최대 12회 수준으로 줄일 수 있다.
permutation에서는 이미 읽은 base frame을 직접 써서 cell별 DuckDB 재조회 자체를 없앤다.

이 작업은 병렬화보다 먼저 한다. 큰 frame을 worker마다 복제하는 병렬화 효과에 최종 목표를
의존하지 않게 하기 위해서다.

## 6. 외부 구현체 평가

### Polars native expression

가장 적합하다.

- 이미 research dependency에 있다.
- `pl.corr(method="spearman")`을 지원한다.
- group/window 연산을 Rust native에서 실행한다.
- 현재 replicate base frame이 이미 Polars다.
- 실데이터에서 값 parity와 3.2배 개선을 확인했다.

참고: <https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.corr.html>

### DuckDB SQL

실데이터에서 약 2배 빨랐다. formation filtering부터 집계까지 한 query로 끝낼 수 있다. 다만
replicate frame을 매번 DuckDB에 등록하는 현재 구조에서는 Polars보다 이점이 작다. DuckDB의
multi-thread `corr` 같은 부동소수점 aggregate는 낮은 자리수가 달라질 수 있으므로 official
결정성 검사가 필요하다.

참고: <https://duckdb.org/docs/lts/operations_manual/non-deterministic_behavior>

### SciPy/statsmodels

일반 Spearman, bootstrap, HAC 구현은 있지만 그대로 바꾸기 어렵다.

- 현재 HAC는 실제 KRX session 간격을 사용한다.
- SUE bootstrap은 issuer와 filing-cycle cluster 전체를 함께 뽑는다.
- 각 replicate 안에서 날짜×시장 rank와 cohort pooling을 다시 한다.

기성 함수에 맞추려고 통계 계약을 바꾸기보다 작은 gap-aware kernel을 직접 유지하는 편이 낫다.

### Numba

Newey–West, cluster bootstrap, replicate loop처럼 모양이 고정된 배열 계산에는 맞는다. 하지만 새
dependency와 JIT warm-up이 생긴다. Polars/NumPy 교체 후에도 bootstrap이 병목으로 남을 때만
도입한다.

### Rust, Ray/Dask, GPU

첫 단계에서는 쓰지 않는다.

- Rust extension: 가장 빠를 수 있지만 FFI, wheel, 플랫폼별 build, parity 검증 비용이 크다.
- Ray/Dask: 단일 36GB 장비에서 1.9GB frame을 여러 worker에 전달하는 비용이 크다.
- GPU: 핵심 연산이 작은 group의 반복 rank/sort라 데이터 이동과 결정성 문제가 먼저 생긴다.

## 7. 결론

다음 세 가지가 실행시간의 대부분을 설명한다.

1. 날짜×시장 group을 Python에서 반복한다.
2. 같은 큰 panel을 cell마다 다시 추출하고 rank한다.
3. Phase B가 Phase A 75개 permutation 통계를 다시 계산한다.

따라서 최적화도 같은 순서로 진행한다. mart SQL이나 Phase AB를 먼저 손대지 않는다.
