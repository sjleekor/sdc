# 00. Phase A/B 실행시간 개선 — 개요와 결정

- 작성일: 2026-08-23
- 상태: 8월 23일 실데이터 official A/B/AB 완주·실행시간 목표 달성. 8월 27일 통계·판정 결정성 재검증 완료
- 대상: Horizon Scan `Phase A`, `Phase B`, `Phase AB`
- 선행 문서:
  - [`../01_feature_candidate/03_horizon_predictive_power_plan.md`](../01_feature_candidate/03_horizon_predictive_power_plan.md)
  - [`../01_feature_candidate/04_specific_plan_A.md`](../01_feature_candidate/04_specific_plan_A.md)
  - [`../01_feature_candidate/04_specific_plan_B.md`](../01_feature_candidate/04_specific_plan_B.md)
  - [`../01_feature_candidate/00_status.md`](../01_feature_candidate/00_status.md)

---

## 1. 결론

Phase A/B를 빠르게 만들기 위해 새 분석 엔진이나 분산 처리 시스템을 먼저 도입하지 않는다.
현재 병목은 DuckDB 자체보다 **Python에서 cell과 날짜×시장을 하나씩 도는 계산 경로**다.

개선 순서는 다음과 같이 확정한다.

1. stage timing과 legacy→legacy 재현성 기준선을 먼저 만든다.
2. 날짜×시장 Spearman IC를 Polars native expression으로 바꾼다.
3. KRX session gap을 반영하는 Newey–West 계산을 배열 기반으로 다시 구현한다.
4. SUE joint permutation의 입력 순서 버그는 성능 개선과 분리해 새 통계 계약으로 고친다.
5. feature별 formation frame을 한 번만 읽고 여러 horizon cell이 재사용한다.
6. replicate별 checkpoint를 공식 CLI에 연결한다.
7. Phase A의 기존 permutation cell 통계를 저장하고 Phase B joint permutation에서 재사용한다.
8. 위 작업이 끝난 뒤 replicate를 2개 worker부터 병렬 실행한다.

Rust extension, Ray/Dask, GPU는 첫 구현 범위에서 제외한다. 현재 쓰고 있는 Polars가 이미 Rust
native 엔진이고, 실데이터 micro-benchmark에서 충분한 개선 폭이 확인됐다.

SUE 정렬은 단순한 성능 개선이 아니다. 현재 joint SUE permutation에는 입력 순서 의존성이 두 군데
있다. `newey_west_tstat`은 정렬되지 않은 입력에서 pair 집합이 달라지고,
`_permute_qualifying_sue_ranks`는 DuckDB가 돌려준 group·row 순서대로 하나의 RNG를 소비한다. real
SUE scan은 `_aggregate_cohort_rows` 호출 전에 cohort를 날짜순으로 정렬하므로 NW 계산은 안전하다.
두 문제가 모두 드러나는 경로는 joint SUE permutation뿐이다. I2b에서 NW 경계 정렬과 함께
permutation 전 frame을 실제 event grain의 canonical key로 정렬한다.

따라서 바뀔 것으로 예상하는 값은 joint `permutation_summary`, combined empirical p-value, AB
파생값이고, real SUE 6개 cell의 t/p는 parity 대상이다. real 값이 의미 있게 바뀌면 예상된 변화가
아니라 regression으로 처리한다. 이 수정은 별도 contract version과 새 Phase B/AB run으로 발행한다.

### 2026-08-23 실데이터 결과

`snapshot_date=2026-08-23`, `config_hash=ab0de634…`로 A → B → AB를 official로 완주했다.

| 단계 | run_id | 시간 | peak RSS |
|---|---|---:|---:|
| Phase A | `20260823T210913-b649a460` | 3,611.211초(60분 11초) | 9.94GB |
| Phase B | `20260823T221441-b649a460` | 2,664.390초(44분 24초) | 20.36GB |
| Phase AB | `20260823T225913-b649a460` | 1초 미만 | artifact 결합만 실행 |

A+B+AB는 약 1시간 44분으로 최종 목표 3시간을 넘지 않았다. Phase B가 Phase A의
`permutation_cell_stats.parquet`를 재사용했고, `joint_cs_v2` mapping hash 검증과 AB preflight도
통과했다. 8월 27일 같은 코드·입력의 두 번째 official A → B → AB도 완주했다. primary 표와
permutation·grade는 정확히 같았고, non-overlap·rank correlation에 최대 `1.11e-16`의 마지막
비트 차이만 남았다. 따라서 통계·판정 결정성은 통과했지만 byte-level content hash 결정성은
아직 완전하지 않다. 기존 계약 full parity 결과의 별도 발행은 남아 있다.

## 2. 지금 구조와 바꿀 구조

현재 구조는 다음과 같다.

```text
A0 공통 mart
  ├─ Phase A
  │    ├─ 실제 75개 primary cell
  │    ├─ A-only cross-sectional permutation 100회
  │    └─ temporal placebo 100회
  │
  ├─ Phase B
  │    ├─ 실제 38개 primary cell
  │    ├─ temporal placebo / SUE bootstrap
  │    └─ A 75개 + B continuous 32개 joint permutation 100회
  │         └─ Phase A 75개를 다시 계산
  │
  └─ Phase AB
       └─ A/B artifact를 읽어 combined BH와 grade 계산
```

목표 구조는 다음과 같다.

```text
A0 공통 mart
  ├─ Phase A native scan
  │    ├─ feature별 1회 fetch
  │    ├─ Polars native Spearman + vectorized Newey–West
  │    ├─ 공통 permutation mapping contract
  │    └─ 이미 계산한 replicate별 A cell 통계를 artifact로 보존
  │
  ├─ Phase B native scan
  │    ├─ 같은 native scan kernel 사용
  │    ├─ SUE permutation 입력과 cohort session을 명시적 정렬
  │    ├─ B cell만 새로 permutation
  │    └─ 저장된 A cell 통계와 합쳐 combined BH
  │
  └─ Phase AB
       └─ 현재처럼 artifact만 읽음
```

Phase AB는 DB나 lake를 읽지 않고 1초 안에 끝난다. AB 계산 자체는 손대지 않는다. 다만 A/B가
새 artifact를 쓰므로 AB가 config hash, content hash, seed contract를 확인하도록 계약을 보강한다.
확인 대상에는 A/B 상호 동일성도 들어간다. snapshot·source·A0 manifest hash가 서로 같아야 하고,
B가 재사용한 Phase A permutation artifact가 AB에 넘긴 Phase A run과 같아야 한다. Phase B는 자기
panel에서 replicate별 continuous mapping을 다시 만들고, 그 `mapping_hash`가 Phase A artifact의
값과 exact match인지도 직접 확인한다. combined BH 직전에는 A∪B `hypothesis_id`가 유일해야 한다.

## 3. 왜 이 순서인가

Phase B 기존 run 두 건에서 초반 스캔·bootstrap·진단은 약 39분이었다. 그 뒤 joint permutation
하나에 4시간 55~57분이 걸렸다. 전체의 약 88%라는 점은 코드 실행 순서와 산출물 시각으로
확인됐다. mart를 더 빠르게 만드는 것보다 반복 scan을 줄이는 편이 효과가 크다.

실데이터 477만 행짜리 cell에서는 다음 결과가 나왔다.

| 계산 | 현재 | 후보 구현 | 차이 |
|---|---:|---:|---:|
| 날짜×시장 Spearman | 1.408초 | Polars native 0.442초 | 3.2배 |
| IC 값 차이 | — | 최대 `1.1e-16` | 사실상 같음 |
| DuckDB fetch + Python group | 1.977초 | DuckDB native SQL 0.978초 | 2.0배 |
| Newey–West micro-benchmark | 0.205초 | 배열 기반 0.0138초 | 14.9배 |

자세한 측정 조건과 해석은 [`01_bottleneck_and_benchmark.md`](01_bottleneck_and_benchmark.md)에
적었다.

## 4. 목표

아래는 개선 전 실측, 8월 23일 최신 실측과 목표를 비교한 값이다.

| 단계 | 개선 전 | 2026-08-23 | 최종 목표 | 판정 |
|---|---:|---:|---:|---|
| Phase A | 4시간 41분 | 60분 11초 | 1시간 30분 안팎 | 달성 |
| Phase B | 5시간 30분 | 44분 24초 | 1시간 30분 이하 | 달성 |
| Phase AB | 1초 미만 | 1초 미만 | 유지 | 달성 |
| A+B+AB | 약 10시간 11분 | 약 1시간 44분 | 3시간 이하 | 달성 |

최신 실행은 native kernel, Phase A 통계 재사용과 worker 2개를 적용했다. B의 peak RSS가
20.36GB였으므로 같은 장비에서 A와 B를 동시에 돌리지 않는다.

## 5. 지켜야 할 것

속도를 위해 통계 계약을 조용히 바꾸면 안 된다.

- 같은 universe, common-survivor, label eligibility를 사용한다.
- Spearman tie는 average rank를 유지한다.
- 빠진 값과 `NaN`/`inf` 제외 순서를 유지한다.
- `min_names`, `min_dates`, `nw_lag`, BH 모집단을 유지한다.
- Newey–West는 배열 위치가 아니라 실제 KRX session 거리를 사용한다.
- Newey–West 입력은 중복 없는 session 오름차순이어야 하며, native 함수가 이를 검사한다.
- SUE 정렬 수정에서 real 6개 cell은 parity 대상으로 두고, joint null 파생값 변화만 예상된
  변화로 다룬다.
- SUE joint permutation은 event grain canonical order에서 draw한다. SQL 반환 순서에 기대지 않는다.
- permutation에서 날짜×시장 안의 모든 primary feature에 같은 row mapping을 적용한다.
- replicate seed는 실행 순서와 worker 수에 영향을 받지 않는다.
- worker 수가 달라도 최종 row 순서와 판정이 같아야 한다.
- 기존 official run을 덮어쓰지 않는다. 새 계산 계약은 새 config/code hash로 발행한다.

seed 계약을 바꾸는 Phase A/B artifact 재사용은 숫자 parity 작업과 분리한다. native kernel은 우선
기존 seed와 mapping으로 검증하고, 그다음 공통 seed contract를 별도 버전으로 도입한다.
`joint_cs_v2` mapping은 continuous 날짜×시장 permutation에만 적용한다. SUE는 I2b에서 고정한
per-cell seed·canonical row order 계약을, temporal placebo는 기존 circular-shift draw 계약을
그대로 쓴다.

## 6. 문서 지도

| 파일 | 내용 |
|---|---|
| **`00_overview.md`** | 이 문서. 결론, 범위, 목표 |
| [`01_bottleneck_and_benchmark.md`](01_bottleneck_and_benchmark.md) | 코드 병목과 실측 근거 |
| [`02_optimization_design.md`](02_optimization_design.md) | native kernel, artifact 재사용, 병렬화 설계 |
| [`03_implementation_and_validation.md`](03_implementation_and_validation.md) | 작업 순서, 테스트, acceptance gate, 롤백 |

## 7. 범위 밖

이번 작업에서 다음은 바꾸지 않는다.

- feature 정의와 primary hypothesis 수
- holdout 경계
- A0 mart 정의와 raw/derived 계층
- BH threshold와 evidence grade 규칙
- bootstrap/permutation 반복 횟수
- Phase C와 모델 acceptance gate
- Phase AB의 통계 의미

반복 횟수를 100/999에서 낮추는 것은 smoke run에서만 허용한다. official run 시간을 줄이기 위한
방법으로 사용하지 않는다.
