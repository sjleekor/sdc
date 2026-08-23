# 02. Phase A/B 최적화 설계

- 작성일: 2026-08-23
- 상태: 코드 구현 완료, 새 A0·실데이터 검증 대기
- 목표: 통계 의미를 유지하면서 반복 scan을 native·재사용·제한 병렬 구조로 바꾼다.

---

## 1. 설계 원칙

1. **통계 계약과 실행 엔진을 분리한다.** universe, label, tie, gap, BH 규칙은 그대로 두고
   계산 방법만 바꾼다.
2. **한 번 바꾸고 한 번 검증한다.** native kernel parity를 먼저 끝낸 뒤 seed/artifact 계약을
   바꾼다.
3. **worker 수와 결과를 분리한다.** seed와 row mapping은 worker 배치가 아니라 replicate id로
   결정한다.
4. **중간 결과를 값 있는 artifact로 남긴다.** Phase B가 Phase A 결과를 다시 계산하지 않게
   한다.
5. **깨진 실행을 이어간다.** checkpoint는 clean run 속도보다 운영 안전을 위한 필수 기능이다.
6. **발견된 통계 버그를 최적화로 숨기지 않는다.** SUE session 정렬 수정은 별도 contract로
   발행하고 전후 차이를 기록한다.

## 2. 새 scan kernel

### 2.1 입력과 출력

기존 `scan_cell`의 formation SQL과 result schema를 유지한다. 바뀌는 것은 날짜×시장 IC를
만드는 내부 경로다.

```text
입력
  trade_date, market, formation_session_idx
  feature_value, target_rank, target_raw
  eligibility가 적용된 행

출력
  날짜×시장별 rank_ic, n
  일별 n-weighted rank_ic
  IC mean/std/ICIR/t_naive/t_nw/p_nw
```

### 2.2 Polars native Spearman

후보 표현은 다음 모양이다.

```python
clean = frame.select(
    "trade_date", "market", "feature_value", "target_rank"
).drop_nulls().filter(
    pl.col("feature_value").is_finite()
    & pl.col("target_rank").is_finite()
)

market_ic = (
    clean.group_by(["trade_date", "market"], maintain_order=True)
    .agg(
        pl.corr("feature_value", "target_rank", method="spearman").alias("rank_ic"),
        pl.len().alias("n"),
    )
    .filter(pl.col("n") >= min_names)
)
```

구현할 때 아래를 확인한다.

- `drop_nulls` 뒤 `is_finite` 순서가 기존과 같다.
- Spearman tie가 average rank다.
- `min_names`보다 작은 group은 correlation을 계산하더라도 결과에서 제외한다.
- degenerate group의 `NaN` 처리와 `rank_ic.is_finite()` 필터가 같다.
- 결과를 `trade_date, market` 순으로 정렬해 후속 계산 순서를 고정한다.

실제 scan의 quantile spread도 같은 원칙으로 native expression으로 바꿀 수 있다. 다만
permutation은 이미 `compute_spread=False`이므로 IC kernel parity를 먼저 끝낸다.

### 2.3 feature별 formation fetch 재사용

native group과 Newey–West를 적용한 뒤에는 cell별 DuckDB fetch가 가장 큰 비용으로 남는다.
`build_formation_sql`에서 hypothesis마다 달라지는 것은 feature, target, eligibility 컬럼이다.

새 API는 cell 하나가 아니라 feature 하나를 입력 단위로 삼는다.

```text
feature batch
  feature 1개
  + 그 feature가 쓰는 horizon별 target_rank / target_raw / label_ok
  + 공통 universe / survivor / CA flag
  → DuckDB fetch 1회
  → horizon·universe·sample별 Polars filter와 native IC
```

Phase A는 75개 primary cell에 12개 primary feature 컬럼을 쓴다. fetch를 cell당 1회에서
feature당 1회로 줄인다. permutation과 temporal placebo는 이미 Polars로 읽은 base/combined
frame을 직접 넘겨 DuckDB 등록·재조회를 생략한다.

구현 조건은 다음과 같다.

- horizon별 `label_ok`와 target의 빠진 값 처리를 기존 formation SQL과 같게 유지한다.
- 실제 scan의 4개 universe/sample 조합도 같은 feature frame을 재사용한다.
- `(trade_date, market, ticker)` canonical order는 feature frame에서 한 번만 만든다.
- batch 결과를 기존 `scan_cell` result schema로 되돌려 downstream 코드를 유지한다.
- feature별 frame의 peak memory와 fetch column 수를 timing에 남긴다.

### 2.4 DuckDB 대안

Polars 경로가 parity나 메모리 문제를 통과하지 못할 때만 DuckDB native SQL을 쓴다.

```text
formation
  → date×market 안에서 feature/target average-rank window
  → corr(feature_rank, target_rank)
  → n-weighted daily aggregation
```

DuckDB에서는 `rank()`와 동률 개수로 average rank를 명시한다. multi-thread aggregate의 낮은
자리수 차이를 피해야 하면 이 query만 `threads=1`로 돌리는 benchmark도 같이 한다.

## 3. vectorized gap-aware Newey–West

현재 수식을 유지한다. 유효한 IC 배열을 `x`, 실제 session index를 `s`, 평균을 뺀 값을 `c`라
하면 다음과 같다.

```text
gamma_0 = sum(c_i²) / n

각 실제 session 거리 d = 1..lag:
  P_d = {(i, j) | s_j - s_i = d}
  gamma_d = sum(c_i × c_j for (i, j) in P_d) / n

long_run = gamma_0
         + 2 × sum((1 - d/(lag+1)) × gamma_d, d=1..lag)

variance_mean = long_run / n
t_nw = mean(x) / sqrt(variance_mean)
```

현재 구현은 각 `i`에서 뒤쪽 session 전체를 만든다. 새 구현은 **session index가 중복 없이
오름차순이라는 명시적 계약** 아래 거리별 pair 위치를 `searchsorted` 또는 dense session lookup으로
찾는다.

구현 조건은 다음과 같다.

- `fastmath`를 쓰지 않는다.
- 새 구현 안의 합산 순서를 고정한다. 거리별 합산은 legacy와 순서가 다르므로 bit 일치를 뜻하지
  않는다.
- 유효 session index가 strictly increasing이 아니면 조용히 계산하지 않고 실패한다.
- 중복 session index는 date당 1행이라는 daily IC/SUE cohort grain이 깨졌다는 신호이므로 거부한다.
- `variance_mean <= 0`, 유효 관측 2개 미만 처리를 유지한다.
- `n_hac_pairs`도 같은 pair index를 재사용한다.
- 숫자 비교는 `|Δ| / max(1, |legacy|) <= 1e-12`와 downstream 판정 일치를 함께 쓴다.

NumPy 구현으로 먼저 넣는다. 이 함수가 다시 병목으로 확인될 때만 Numba JIT를 붙인다.

### 3.1 연속형과 SUE 적용을 분리

연속형 `scan_cell`은 일별 IC를 `trade_date` 순으로 정렬하므로 native Newey–West를 기존 계약
parity 대상으로 적용할 수 있다.

SUE는 경로별로 다르다. cohort pooling은 DuckDB 출력과 Polars `maintain_order` 순서를 이어받고
상위 SQL에 `ORDER BY`가 없지만, real SUE scan과 event-ordinal 진단은 `_aggregate_cohort_rows`
호출 전에 cohort를 날짜순으로 정렬하고, issuer/filing bootstrap은 평균만 써서 순서와 무관하다.
joint SUE permutation에는 NW 입력 정렬뿐 아니라 draw 입력 순서도 빠져 있다.
`_permute_qualifying_sue_ranks`가 group 등장 순서대로 RNG 하나를 소비하므로 같은 seed도 SQL 반환
순서가 달라지면 다른 mapping이 된다(01 §4.4). 따라서 다음 순서로 고친다.

1. native Newey–West는 먼저 연속형 경로에만 연결한다.
2. `build_event_cohort_frame_sql`이 mart grain의 안정적인 row key인 `original_rcept_no`를 함께
   가져오게 한다. permutation 전에 qualifying frame을
   `(event_formation_date, market, ticker, original_rcept_no)`로 정렬하고 이 key의 유일성을
   assert한다. group 순서와 group 안 row 순서를 모두 이 canonical order로 고정한 뒤 기존
   per-cell seed와 PCG64 draw를 그대로 쓴다.
3. `_aggregate_cohort_rows` 경계에서 cohort를 `(formation_session_idx, event_formation_date)`로
   정렬하고 session index가 strictly increasing인지 assert한다. 호출부별 정렬에 기대지 않는다.
4. real scan과 진단 경로는 이미 정렬돼 있으므로 동작이 바뀌지 않아야 한다(assert만 추가).
5. `sue_nw_order_contract=sue_nw_sorted_v2`와
   `sue_permutation_order_contract=sue_rank_canonical_v2`를 config/run spec/report에 남긴다. 두 이름은
   각각 정렬된 NW 입력과 canonical permutation 입력을 나타낸다. 실제 변화 범위가 joint null에
   한정된다는 사실은 delta artifact와 report가 명시한다.
6. real SUE 6개 cell의 t/p는 parity 대상이다. 전후 차이를 기록할 대상은 joint
   `permutation_summary`, combined empirical p-value, AB 파생값이다. real 값이 tolerance를 넘게
   바뀌면 regression으로 조사한다.

2~6은 기존 통계 수정이다. native kernel 속도 개선과 같은 parity commit에 넣지 않는다.

## 4. permutation frame과 mapping

### 4.1 현재 문제

현재 `permute_within_groups`는 base frame을 fixed column 전체로 정렬한 뒤 날짜×시장 group
4,920개를 Python에서 돈다. group별 DataFrame을 만들고 마지막에 모두 합친다.

더 큰 문제는 같은 replicate frame을 DuckDB에 등록한 뒤 cell마다 한 feature와 한 label을 다시
꺼내는 것이다.

### 4.2 새 mapping contract

row mapping을 feature나 frame column 목록과 분리한다.

```text
group key: (trade_date, market)
row key:   ticker
seed key:  (mapping_contract_version, replicate_index, config_hash, trade_date, market)
```

각 날짜×시장을 canonical order로 정렬하고, group 안은 ticker 순으로 고정한다. seed와 mapping은
아래 하나의 방식으로 고정한다. 계약에 선택지를 남기지 않는다.

- **seed key 직렬화**: `mapping_contract_version`, `replicate_index`의 10진수 문자열,
  `config_hash` hex, `trade_date`의 ISO `YYYY-MM-DD`, `market` 코드 문자열을 구분자 `0x1F`로
  이어 붙여 UTF-8로 인코딩한다.
- **digest → seed**: SHA-256 digest 32 byte 전체를 big-endian 정수로 바꿔
  `numpy.random.SeedSequence(entropy)`에 넣는다. digest를 32-bit 정수로 자르지 않는다. 약
  4,920개 group마다 seed를 만들더라도 32-bit 충돌을 받아들이지 않는다.
- **bit generator와 호출**: `numpy.random.default_rng(seed_sequence)`(PCG64)의
  `permutation(n)`이 돌려준 index 배열을 mapping으로 쓴다. Philox key/counter 방식은 쓰지
  않는다.
- **중복 처리**: mapping 전에 Phase A/B panel 모두에서 `(trade_date, market, ticker)`가
  unique인지 assert한다. 중복이면 dedupe하지 않고 실패한다.
- **`mapping_hash` canonical byte**: `(trade_date, market)` 오름차순으로 group을 돈다. 각 group은
  `trade_date` ISO UTF-8, `market` UTF-8, row count의 uint32 little-endian, ticker canonical
  sequence(각 ticker의 uint16 little-endian byte 길이 + UTF-8 bytes), permutation index의 int32
  little-endian bytes 순으로 직렬화한다. 모든 group bytes를 이어 붙여 SHA-256을 계산한다. ticker
  sequence까지 넣어 같은 크기의 다른 ticker 집합도 hash mismatch로 잡는다.

NumPy 버전이 `permutation` 알고리즘을 바꾸면 `mapping_hash`가 달라져 checkpoint/artifact
재사용이 거부된다. run spec의 `numpy_version` 기록과 `mapping_hash` 검증이 함께 이 계약의
재현성을 지킨다.

replicate마다 만들어진 하나의 mapping을 모든 primary feature column에 같이 적용한다. label과
fixed column은 움직이지 않는다.

이 방식의 장점은 다음과 같다.

- Phase A와 Phase B panel의 feature 컬럼 수가 달라도 mapping이 같다.
- worker 실행 순서가 달라도 mapping이 같다.
- 전체 65컬럼 frame을 replicate마다 복사할 필요가 없다.
- mapping 자체는 `int32` index 배열로 저장하거나 즉시 재생성할 수 있다.
- seed entropy를 32-bit로 줄이지 않는다.

현재 seed와 mapping 결과를 그대로 재현하는 `v1` native path를 먼저 만든다. Phase A 통계 재사용
단계에서 위 계약을 `joint_cs_v2`로 도입하고 config/artifact version을 올린다.

`joint_cs_v2`는 Phase A/B continuous 날짜×시장 mapping만 정의한다. SUE joint permutation은
I2b의 per-cell seed·`sue_rank_canonical_v2` 순서 계약을 유지하고, temporal placebo는 기존
circular-shift seed/draw 계약을 유지한다. 세 null draw를 하나의 mapping 계약으로 뭉뚱그리지
않는다.

## 5. Phase A permutation 통계 artifact

### 5.1 새 artifact

Phase A는 이미 replicate마다 75개 cell을 계산하지만 발견 개수 같은 요약만 저장한다. 새 계산을
추가하는 것이 아니라 **이미 계산한 cell 결과를 버리지 않고 저장한다.** Phase B가 combined BH를
하려면 A cell별 p-value와 isolated-spike 재계산에 필요한 registry 필드가 필요하다.

```text
phase=A/.../core/permutation_cell_stats.parquet
```

필수 컬럼은 다음과 같다.

| 컬럼 | 뜻 |
|---|---|
| `mapping_contract_version` | row mapping 계약 |
| `replicate` | 0부터 시작하는 replicate id |
| `mapping_hash` | 해당 replicate의 날짜×시장별 row mapping 전체 content hash |
| `hypothesis_id` | Phase A primary cell id |
| `family`, `feature` | registry identity와 family 축 |
| `scan_type`, `h_start`, `h_end` | isolated-spike 이웃 계산 축 |
| `expected_sign` | 방향 판정 입력 |
| `status` | `valid` / `insufficient` |
| `ic_mean` | null sample IC mean |
| `t_nw` | null sample NW t-stat |
| `p_nw` | combined BH 입력 |
| `n_dates`, `n_obs` | 진단과 parity 확인 |

요약 artifact는 이 parquet에서 다시 만든다. summary와 cell 통계가 어긋나는 별도 계산 경로를 두지
않는다. `expected_sign_pass`, `isolated_spike`, `q_fdr_global`, `primary_discovery`는 저장값을
신뢰하지 않고 `apply_global_bh`가 전체 모집단에서 다시 계산한다.

### 5.2 Phase B 재사용

Phase B는 다음을 모두 확인한 뒤 A 통계를 읽는다.

- snapshot date와 source
- A0 manifest content hash
- analysis config hash
- mapping contract version
- replicate 수와 id 집합
- Phase A primary hypothesis id 집합 75개
- artifact registry 필드와 현재 config registry의 exact match
- artifact content hash
- `analysis_kernel_hash` (§6.1)

Phase B는 자기 panel의 날짜×시장별 ticker 집합으로 continuous mapping을 replicate별로 다시
만든다. 재생성한 `mapping_hash`가 같은 replicate의 Phase A artifact 값과 exact match여야 한다.
이 직접 검사는 A/B group 크기와 ticker 집합이 같아 `permutation(n)`의 의미가 같다는 전제를
확인한다. 하나라도 다르면 B cell scan 전에 실패한다.

그다음 B continuous/SUE null 통계만 계산하고 replicate별로 A+B p-value를 합친다. family 집합이
겹치지 않는다는 검사와 별개로, A∪B `hypothesis_id` 전체가 유일한지 assert한 뒤 combined BH를
적용한다. `apply_global_bh`에 중복 id를 넘겨 BH 모집단 `m`이 조용히 늘어나는 경로를 허용하지
않는다.

```text
replicate r
  A cell stats 75개 읽기
  + B continuous 32개 계산
  + B SUE 6개 계산
  → combined BH
  → n_discoveries 저장
```

호환되는 A artifact가 없으면 official Phase B는 조용히 다른 seed나 과거 artifact를 쓰지 않는다.
명확히 실패하거나, 별도 `--recompute-phase-a-null` 경로를 사용해 전체를 다시 계산한다.

combined isolated-spike는 `(family, scan_type)` 안의 이웃을 본다. Phase B preflight는 Phase A와
Phase B의 family 이름 집합이 서로 겹치지 않는지 assert한다. 현재 config의 A 17개와 B 8개
family는 겹치지 않지만 이를 관례로만 두지 않는다.

### 5.3 통계 계약 변경

현재 A-only permutation과 A+B joint permutation은 서로 다른 `placebo_kind`로 seed를 만든다.
재사용하려면 같은 mapping contract를 써야 한다. 이는 계산 최적화가 아니라 null draw 계약 변경이다.

따라서 두 단계를 분리한다.

1. 기존 seed/mapping으로 native kernel 숫자 parity를 확인한다.
2. `joint_cs_v2`를 새 사전등록 config로 도입한다.
3. 같은 config에서 Phase A와 B를 새로 실행한다.
4. 과거 run과 replicate별 숫자 일치를 요구하지 않고, 새 run의 반복 결정성과 판정 안정성을
   확인한다.

v2에서는 Phase A의 A-only null과 Phase B combined null이 **같은 permutation draw**를 사용한다.
각 empirical p-value는 여전히 해당 모집단의 permutation null이지만 둘을 독립된 Monte Carlo
draw처럼 비교하면 안 된다. Phase A/B report와 AB report에 이 해석 제한을 한 줄로 명시한다.
대신 한 번 계산한 A 75개 cell이 A-only BH와 combined BH 양쪽에 그대로 쓰인다는 이점이 생긴다.

## 6. checkpoint와 resume

하위 replicate 함수는 JSONL checkpoint 인자를 이미 받지만 Phase A/B 공식 진입점은 경로를 넘기지
않는다. 병렬 worker가 한 JSONL에 같이 append하는 구조도 안전하지 않다.

### 6.1 analysis kernel hash와 checkpoint fingerprint

현재 run spec의 code hash는 계산 kernel을 다 덮지 못한다. Phase A는
`research/analysis/horizon_scan*.py`만(`horizon_scan.py:519`), Phase B는
`horizon_scan_phase_b*.py`만(`horizon_scan_phase_b_run.py:787`) hash한다. 핵심 변경 대상인
`research/etl/metrics.py`는 둘 다 빠지고, Phase B에는 공통 `horizon_scan_runner.py`와
`horizon_scan_permutation.py`도 빠진다. Phase A의 현재 glob은 이름상 Phase A 전용처럼 보이지만
실제로 `horizon_scan_phase_b*.py`까지 이미 포함한다. 공통 파일 목록 상수의 핵심 효과는 이 우연한
glob 범위를 명시적 계약으로 바꾸고 `metrics.py`를 추가하며, 더 좁은 Phase B hash 범위를 같은
목록으로 맞추는 데 있다. 이대로면 kernel을 바꿔도 run lineage가 달라지지 않는다.

`analysis_kernel_hash`를 새로 정의한다.

- 대상: `research/etl/metrics.py` + `research/analysis/horizon_scan*.py` 전체
  (새 `horizon_scan_native.py` 포함). Phase A/B가 같은 파일 목록 상수를 공유한다.
- scan config는 기존 `config_hash`가 계속 맡는다.
- checkpoint fingerprint, Phase A artifact 재사용(§5.2), AB preflight(§9)가 이 hash를 쓴다.

checkpoint 경로의 snapshot/source/config/contract만으로는 재사용 안전성이 부족하다.
`--smoke-family`와 `--permutations`, holdout override는 config hash를 바꾸지 않으므로, 같은
경로에 smoke run의 checkpoint가 남을 수 있다. v1 summary checkpoint는 hypothesis id 집합을 담지
않아 내용만으로는 smoke와 official을 구분할 수 없다. 따라서 모든 checkpoint 파일(v1 포함)에
fingerprint block을 넣고 resume에서 전부 exact match로 검사한다.

| fingerprint 필드 | 내용 |
|---|---|
| `registry_hash` | 정렬된 hypothesis id와 registry 필드의 hash |
| `a0_manifest_hash` | A0 `_SUCCESS.json` content hash |
| `readiness_population_hash` | Phase B experiment만: readiness-freeze population hash |
| `smoke_family` | smoke filter 값 (official은 `null`) |
| `requested_replicates` | 해당 experiment가 요청한 전체 replicate 수 |
| `include_holdout`, `holdout_start` | sample/holdout override |
| `scan_engine`, `row_order_contract`, `sue_nw_order_contract`, `sue_permutation_order_contract`, `mapping_contract_version` | 계산 계약 |
| `analysis_kernel_hash` | 위에서 정의한 kernel code hash |
| `duckdb_version`, `polars_version`, `numpy_version` | replicate를 계산한 runtime library 버전 |

fingerprint가 없거나 하나라도 다른 checkpoint는 재사용하지 않고 거부한다.

### 6.2 checkpoint 배치와 resume

새 checkpoint는 replicate별 파일로 둔다.

```text
research/output/horizon_scan_checkpoints/
  phase=A/
    snapshot_date=YYYY-MM-DD/
      source=sj2_remote/
        config_hash=.../
          experiment=cross_sectional/
            contract=v1/
              replicate=000.json
              replicate=001.json
              ...
```

checkpoint를 발행 run의 `phase=*/` 트리 밖에 둬 published artifact와 실행 중 상태를 구분한다.

I4의 `v1` checkpoint는 현재 함수가 이미 만드는 replicate 결과를 그대로 담는다.

| experiment | replicate 파일 내용 |
|---|---|
| A cross-sectional | 발견 수, min p/q, `max_abs_t` 등 현재 summary 1행 |
| temporal placebo | shift와 hypothesis별 `abs_t_nw` |
| issuer/filing bootstrap | replicate `ic_mean` |
| A+B joint v1 | combined discovery count |

I5에서 `joint_cs_v2`를 도입하면 cross-sectional checkpoint가 cell-level parquet으로 확장된다.
I4가 아직 만들지 않는 cell 통계를 v1 checkpoint에 있다고 가정하지 않는다.

각 worker는 임시 파일을 쓴 뒤 atomic rename한다. coordinator만 최종 artifact를 합친다.

checkpoint 경로에 run id가 없으므로 같은 namespace를 두 coordinator가 동시에 쓰면 안 된다.
coordinator는 `experiment=*/contract=*/.checkpoint.lock`을 열고 non-blocking exclusive advisory lock을
실행이 끝날 때까지 유지한다. 같은 phase/snapshot/source/config/experiment/contract의 두 번째 run은
checkpoint를 읽거나 쓰기 전에 실패한다. process가 비정상 종료돼도 OS가 lock을 풀기 때문에 stale
lock을 수동 삭제하는 절차는 두지 않는다. 서로 다른 experiment namespace는 동시에 실행할 수 있다.

resume 조건은 다음과 같다.

- snapshot/source/config/contract가 모두 같다.
- checkpoint의 fingerprint block(§6.1)이 현재 run과 exact match다. fingerprint가 없는 파일은
  거부한다.
- 해당 replicate 파일의 schema와 content hash가 맞다.
- 해당 experiment가 cell 결과를 담는 경우 기대 hypothesis id가 빠짐없이 있다.
- 깨진 파일은 재계산하고 정상 파일은 건너뛴다.

run이 성공하면 사용한 checkpoint 목록과 hash를 run spec에 남긴다. checkpoint를 자동으로 지우지
않는다. 정리 정책은 별도 운영 명령으로 둔다.

## 7. 제한된 병렬 실행

### 7.1 구조

replicate는 서로 독립적이므로 process 단위로 나눌 수 있다.

```text
coordinator
  ├─ worker 0: replicate 0, 2, 4, ...
  └─ worker 1: replicate 1, 3, 5, ...

worker
  → 자기 connection/frame 준비
  → replicate 계산
  → replicate checkpoint atomic write

coordinator
  → checkpoint 전체 검증
  → replicate 순으로 merge
  → BH summary와 report 생성
```

DuckDB connection을 worker가 공유하지 않는다. Polars thread pool과 process worker가 CPU를 서로
과하게 잡지 않도록 worker별 thread 수를 제한한다.

### 7.2 기본값

첫 benchmark 기본값은 다음과 같다.

- process workers: 2
- worker당 DuckDB/Polars threads: 4~6 범위 비교
- 전체 memory hard limit: 24GB 목표
- 1 worker 결과를 결정성 기준선으로 사용

현재 base frame이 약 1.88GB다. 4개 이상 worker를 바로 기본값으로 잡지 않는다. worker 수는 CPU
개수만 보지 않고 peak RSS와 memory bandwidth를 함께 보고 정한다.

### 7.3 결과 순서

worker가 끝난 순서대로 결과를 합치지 않는다. 항상 다음 순서로 정렬한다.

```text
replicate, hypothesis_id
```

seed는 worker id를 포함하지 않는다. 1/2/3 workers에서 artifact row와 판정이 같아야 한다.

## 8. SUE bootstrap 후보

현재 mtime만으로는 초반 약 39분 안에서 SUE bootstrap만의 시간을 나눌 수 없다. I0 timing 뒤에도
절대 10분 이상인 병목으로 확인될 때만 backlog에서 꺼낸다.

후보는 행을 실제로 복제하는 `pl.concat` 대신 cluster별 선택 횟수 `weight`를 쓰는 배열 kernel이다.
weighted average-rank가 legacy의 중복 행 전개와 같아야 하므로 별도 PoC와 randomized parity를
통과해야 한다. 필요할 때만 이 작은 kernel에 Numba를 검토한다.

## 9. Phase AB 영향

Phase AB 계산은 그대로 둔다. 새로 확인할 것은 artifact 계약이다. 현재 AB는 A/B 각각을 현재
config hash·content hash와만 대조한다(`run_combined_ab`). I6부터 Phase B의 permutation 결과가
특정 Phase A run의 cell artifact를 직접 재사용하므로, hash를 manifest에 전달하는 것만으로는
부족하다. AB preflight가 다음을 직접 대조하고 하나라도 다르면 combined BH와 empirical p-value를
계산하기 전에 실패한다.

- A/B run spec의 snapshot date, source, A0 manifest content hash가 서로 같다.
- A/B의 mapping contract, scan engine, row order contract, SUE NW order contract, SUE permutation
  order contract, `analysis_kernel_hash`가 같다.
- B run spec이 참조한 Phase A run id와 permutation cell artifact content hash가 AB에 넘긴
  Phase A run의 값과 같다. 같은 config hash로 만든 **다른** Phase A run도 artifact hash가
  다르면 거부한다.
- A permutation cell artifact content hash를 B run spec과 AB manifest에 전달한다.
- combined BH real result와 null empirical p-value 계산은 현재 위치를 유지한다.
- 과거 `v1` A/B run과 새 `joint_cs_v2` run을 섞지 않는다.

## 10. 채택하지 않는 설계

| 후보 | 제외 이유 |
|---|---|
| A/B를 현재 상태로 동시에 실행 | 각각 CPU와 메모리를 크게 써 resource contention이 생김 |
| official 반복 횟수 축소 | 검정력을 바꾸므로 성능 최적화가 아님 |
| 모든 scan을 Rust extension으로 재작성 | Polars native로 먼저 얻을 수 있는 개선에 비해 비용이 큼 |
| Ray/Dask cluster | 단일 장비와 큰 공유 frame에 비해 직렬화·운영 비용이 큼 |
| GPU/MPS | 작은 group별 rank/sort, 데이터 이동, 결정성 검증에 불리함 |
| DuckDB connection 하나를 thread가 공유 | connection 안전성과 query 경쟁 문제가 있음 |
| 병렬 worker가 JSONL 하나에 append | 중간 파일이 깨지거나 row가 섞일 수 있음 |
