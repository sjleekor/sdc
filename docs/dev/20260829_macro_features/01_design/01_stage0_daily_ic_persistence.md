# 01. 단계 0 — 일별 IC 시계열 저장 (`daily_ic.parquet`)

- 작성일: 2026-08-29 (리뷰 `06_review_20260829.md` §4 반영: 호출자 표, cell 수, checkpoint, `q5_spread` 배치, 정합성 항목)
- 성격: **산출물 계약**. 새 가설이 없고 기존 판정을 하나도 바꾸지 않는다.
- 선행 문서: `08_phase_b_implementation_log.md` §4.3 Stage 3("`scan_cell`이 요약 통계만 반환하고 날짜별 원시
  IC 시퀀스는 버림 — 별도 계획 필요"), `04_specific_plan_B.md` §7.1(`daily_ic.parquet`는 원래 산출물 목록에
  있었다), `00_읽는_법.md` §7(a).

---

## 1. 지금 코드가 무엇을 버리나

`research/analysis/horizon_scan_runner.py:497` `scan_cell`은 cell 하나(feature × scan_type × h × universe ×
sample_kind)에 대해 다음을 순서대로 만든다.

```
frame            (trade_date, market, ticker, feature_value, target_rank, target_raw, formation_session_idx)
market_ic        per_date_market_rank_ic(frame)            → (trade_date, market, rank_ic, n)
daily            daily_market_weighted_ic(market_ic)       → (trade_date, rank_ic)  + formation_session_idx join
values/sessions  daily["rank_ic"], daily["formation_session_idx"]
→ ic_mean, ic_std, icir, t_naive, t_nw(values, sessions, lag), p_nw
market_spread    per_date_market_quantile_spread(frame)    → (trade_date, market, spread)
daily_spread     daily_market_weighted_spread(market_spread)
→ q5_spread_raw = mean(daily_spread["spread"])
```

반환값은 스칼라 dict 하나다. `daily`·`market_ic`·`daily_spread`는 함수가 끝나면 사라진다. 그래서

- "2021년엔 먹혔고 2023년엔 안 먹혔다"를 보여줄 수 없고(`00_읽는_법` §7),
- Phase C(국면별 조건부 IC)를 열 수 없고,
- family 간 중복을 **신호 수준**(일별 IC 상관)에서 볼 수 없다(`00_읽는_법` §9.2·9.3은 feature 값 상관만 있다).

`scan_cell` 호출자 전부다(리뷰 §4.1에서 확인).

| 호출 위치 | 경로 | 일별 IC 저장 |
|---|---|---|
| `horizon_scan_runner.py:854`, `:877` | `run_registry_scan` (feature frame 재사용 / 미재사용 두 분기) | **여기만** |
| `horizon_scan_phase_b_scan.py:180` | `run_phase_b_continuous_scan` → `run_registry_scan` | 위와 같음 |
| `horizon_scan_permutation.py:320` | `_scan_registry_once` (cross-sectional 복제, 프로세스 워커 포함) | 안 함 |
| `horizon_scan_permutation.py:765` | `_compute_temporal_replicate` | 안 함 |
| `horizon_scan_permutation.py:819` | `run_lookahead_canary` | 안 함 |
| `horizon_scan.py:168`, `horizon_scan_phase_b_run.py:553` | `compute_period_ics` / `_compute_phase_b_period_ics` (기간 분할, `extra_where`) | 안 함 — 저장된 일별 IC를 날짜로 잘라 재현 가능 |
| `horizon_scan.py:216`, `:423` | lag1 delay gate·family 대표 cell의 lag1 (직접 호출, family당 cell 하나) | 안 함 (§3.1) |

**`run_registry_scan`이 스캔하는 것은 primary 75 + short exploratory 28 = 103 가설 × 4 combo = 412 cell**이다
(canonical A `horizon_ic.parquet` 412행, `hypothesis_role` primary 300 · exploratory_short_regime 112 — 실측).
exploratory horizon·secondary feature cell은 이 CLI에서 스캔되지 않는다(`horizon_scan.py:389~396` 주석).
Phase B 연속 cell은 72 × 4 = 288행, 단계 1a 뒤 96 × 4 = 384행이다.

---

## 2. 설계

### 2.1 원칙

1. **기본 동작은 바이트 단위로 같다.** sink를 넘기지 않으면 `scan_cell`·`run_registry_scan`의 반환값과 계산
   경로가 지금과 완전히 같다. 복제 루프·기간 분할·lag1 직접 호출은 시그니처를 건드리지 않으므로 자동으로 제외된다.
2. **저장은 side-channel이다.** 반환 타입을 바꾸지 않는다. 호출자가 `daily_sink`를 주면 `scan_cell`이 이미
   만들어 둔 `market_ic`·`daily`·`daily_spread`를 그대로 넘긴다. 추가 계산은 없다.
3. **저장된 일별 IC에서 요약 통계를 다시 만들 수 있어야 한다.** 이것이 정합성 검사의 정의다(§4).
4. **cell 식별자를 그대로 붙인다.** downstream이 config를 다시 join하지 않도록, `run_registry_scan`이 rows에
   registry 필드를 붙이는 것과 같은 원칙이다.

### 2.2 변경 지점

```python
# research/analysis/horizon_scan_daily_ic.py  (신규)
class DailyIcSink(Protocol):
    def emit(self, cell: Mapping[str, Any], *, daily: pl.DataFrame,
             market_ic: pl.DataFrame, daily_spread: pl.DataFrame | None) -> None: ...

class ParquetDailyIcSink:
    """feature 단위로 모아 hive partition(family=)로 parquet를 쓴다."""
    def __init__(self, out_dir: Path): ...
    def emit(...): ...
    def flush_feature(self, feature: str) -> None: ...     # feature의 모든 cell이 끝나면 파일 하나
    def finalize(self) -> DailyIcSummary: ...               # 파일 수·행 수·sha256, manifest용

# research/analysis/horizon_scan_runner.py
def scan_cell(..., cell_identity: Mapping[str, Any] | None = None,
              daily_sink: DailyIcSink | None = None) -> dict[str, Any]:
    ...
    if daily_sink is not None and result["status"] == "valid":
        daily_sink.emit(cell_identity, daily=daily, market_ic=market_ic,
                        daily_spread=daily_spread if compute_spread else None)

def run_registry_scan(..., daily_sink: DailyIcSink | None = None) -> list[dict[str, Any]]:
    # feature loop 안에서 scan_cell에 cell_identity·sink 전달, feature 끝날 때 sink.flush_feature(feature)
```

- `cell_identity`는 Phase A와 B의 registry 스키마가 다르므로 **sink 앞에서 정규화**한다(리뷰 §4.4). Phase A
  행은 `hypothesis_role`·`scan_type`, Phase B 행은 `role`·`cell_type`(`cumulative`/`bucket`)에
  `run_phase_b_continuous_scan`이 `scan_type`을 덧붙인 형태다. 정규화 결과는 §2.3의 컬럼 집합이다.
- `status == "valid"`인 cell만 저장한다. `insufficient` cell은 `daily`가 `min_dates_required` 미만이라 저장해도
  요약과 대조할 것이 없다. 저장 안 함을 `status_reason`으로 이미 설명하고 있다.
- Phase B: `run_phase_b_continuous_scan`이 `run_registry_scan`에 `daily_sink`를 그대로 전달한다.
  SUE event(`scan_event_cohort_cell`)는 이번 범위 밖(`cohort_ic.parquet` 미작성, `00_overview` §1.4).
- 본 스캔에는 재시작 경로가 없다. `horizon_scan_checkpoint.py`는 복제 루프 전용이다(`write_replicate_checkpoint`/
  `load_replicate_checkpoints`). run은 tmp 디렉터리에 쓰고 rename하므로 부분 산출물 문제도 없다. sink에 재시작
  로직을 넣지 않는다.

### 2.3 스키마

두 파일이다. `daily`와 `daily_spread`는 날짜 집합이 다르다 — IC는 `min_names=20`, spread는 `min_names_for_spread=50`
기준이고, 한 시장이 50종목 이상인데 피쳐가 전부 동점이라 IC가 NaN인 날은 spread만 남을 수 있다(리뷰 §4.3).
한 파일에 left join으로 붙이면 그런 날의 spread가 사라져 `mean(q5_spread) ≠ q5_spread_raw`가 되고 §4.1의 검사가
실패한다. 그래서 **따로 쓴다.**

**`daily_ic.parquet/family=<family>/<feature>.parquet`** (Phase A는 `core/` 아래, Phase B는 run 루트 —
`04_specific_plan_B.md` §7.1 이름 그대로)

| 컬럼 | 타입 | 뜻 |
|---|---|---|
| `hypothesis_id` | str | registry의 가설 id (`horizon_ic.parquet`와 join 키) |
| `family`, `feature` | str | feature는 variant를 포함한 실제 컬럼명(`…_lag1`) |
| `scan_type` | str | `cum` / `bucket` |
| `h_start`, `h_end` | int | |
| `universe` | str | `broad` / `tradable` |
| `sample_kind` | str | `common_survivor` / `available` |
| `hypothesis_role` | str | Phase A: `primary` / `exploratory_short_regime`. Phase B: `ready_primary` |
| `trade_date` | date | formation date |
| `formation_session_idx` | int | NW gap 계산용 (cell과 같은 값) |
| `rank_ic` | f64 | `daily_market_weighted_ic` 결과 — **`ic_mean`의 재료** |
| `n_obs` | i64 | 그날 두 시장 종목 수 합 |
| `rank_ic_kospi`, `n_kospi` | f64, i64 | `market_ic`의 KOSPI 행 (없으면 NULL / 0) |
| `rank_ic_kosdaq`, `n_kosdaq` | f64, i64 | 같음 |

**`daily_spread.parquet/family=<family>/<feature>.parquet`** — 같은 식별 컬럼 + `trade_date`, `spread`
(`daily_spread["spread"]`), `n_spread_kospi`, `n_spread_kosdaq`. `compute_spread=False`인 호출은 없다
(`run_registry_scan`은 항상 spread를 계산한다).

`run_id`·`config_hash`·`snapshot_date`·`source`는 디렉터리 경로에 있으므로 컬럼으로 반복하지 않는다. `market_ic`의
시장별 값을 두 컬럼으로 펴서 넣는 이유는 (a) 가중 IC를 재계산할 수 있게 하고(`n_obs_weighted`), (b) Phase C에서
시장별 조건부 IC를 진단으로 볼 수 있게 하기 위해서다.

### 2.4 크기

Phase A 412 cell × 약 2,600 거래일 ≈ **107만 행**, Phase B 384 cell × 약 2,400일 ≈ **92만 행**. 컬럼 15개,
zstd parquet로 phase당 **수십 MB**다. 예산 규칙은 두지 않는다.

### 2.5 manifest·_SUCCESS

- `manifest.json`의 artifacts에 `daily_ic`·`daily_spread`를 추가한다: 파일 수, 행 수, sha256(파일 정렬 후 연결 해시).
- `REQUIRED_RUN_ARTIFACTS`(`horizon_scan_run_spec.py:295`)는 **바꾸지 않는다.** 기존 run은 그대로 유효하다.
- `_SUCCESS.json`(현재 `status, run_id, config_hash, content_hash, published_at`)에 `daily_ic_reconciled`와
  `daily_ic_reconcile_max_abs_diff`를 더한다(§4). reconcile 실패는 run 실패다 — 저장한 것이 요약과 다르면 둘 중
  하나가 틀린 것이고 그 run은 official이 될 수 없다.

---

## 3. 이것이 여는 것 (범위 안 / 밖)

| 소비자 | 범위 | 비고 |
|---|---|---|
| Phase C 조건부 IC (`03_stage1b_…`) | **이번 설계** | 단계 0의 존재 이유 |
| 연도별·기간별 IC 표를 family card에 | 이번에 **보고서 항목만** 추가 | `subperiod_heatmap.png`가 하던 일을 숫자로. 판정에는 안 쓴다 |
| A×A·B×B **일별 IC 상관** (`00_읽는_법` §9.2·9.3) | 이번에 진단 산출물 하나 추가 | `daily_ic_family_correlation.parquet`: primary cell(discovery 좌표, 같은 h_end) 간 일별 IC의 Spearman. 판정에는 안 쓴다 |
| 종목 수준 귀속 (`00_읽는_법` §7(b)) | 밖 | 정책 미정 |
| SUE cohort IC 시계열 | 밖 | 표본 없음 |

### 3.1 저장되지 **않는** 것 — 소비자가 알아야 할 한계

- **lag1 variant cell.** `horizon_scan.py:216`(delay gate)·`:423`(family 대표 cell)이 `scan_cell`을 직접 부르고,
  family당 대표 cell 하나뿐이다. `run_registry_scan` 밖이라 daily_ic에 없다. 그래서 Phase C의 lag1 유지율 게이트는
  성립하지 않는다(`03` §5, 리뷰 M5).
- **secondary feature·exploratory horizon cell.** 스캔 자체가 없다. `02` §4의 "`macro_beta_*` vs `macro_rawbeta_*`
  IC 차이" 진단은 별도 진단 스캔이 있어야 한다(`02` §4).

---

## 4. 정합성 검사 (단계 0의 합격 기준)

### 4.1 run 안에서 — `reconcile_daily_ic(horizon_ic_rows, daily_ic_dir, daily_spread_dir)`

valid cell 전부에 대해 저장된 일별 값에서 요약을 다시 만들어 `horizon_ic.parquet` 값과 비교한다.

| 항목 | 재계산 | 허용 오차 |
|---|---|---|
| `n_dates` | 행 수 | 0 |
| `ic_mean` | `mean(rank_ic)` | 1e-12 |
| `ic_std` | `std(rank_ic, ddof=1)` | 1e-12 |
| `icir`, `t_naive` | 위 둘로 | 1e-9 |
| `t_nw` | `newey_west_tstat(rank_ic, formation_session_idx, lag)` — lag는 cell 규칙 그대로 | 1e-9 |
| `n_hac_pairs_min` | `n_hac_pairs(formation_session_idx, lag)` | 0 |
| `n_obs`, `n_obs_min`, `n_obs_median` | `n_obs`로 | 0 / 0 / 1e-12 |
| `kospi_weight_mean` | `mean(n_kospi / n_obs)` (한쪽 시장만 있는 날은 1 또는 0 — `market_weight_means`의 `fill_null(0)`과 같음) | 1e-12 |
| `q5_spread_raw` | `mean(spread)` from `daily_spread` | 1e-12 |
| 가중 재계산 | `(rank_ic_kospi·n_kospi + rank_ic_kosdaq·n_kosdaq) / n_obs == rank_ic` | 1e-12 |

하나라도 넘으면 `_SUCCESS.json`을 쓰지 않고 실패로 끝낸다.

### 4.2 canonical과의 대조 — 행동 불변 증명

sink를 붙인 코드로 만든 첫 official run의 `horizon_ic.parquet`(A)·`phase_b_primary_hypotheses.parquet`(B)·
`combined_ab_primary_hypotheses.parquet`(AB)를 canonical run(`20260827T221729` / `20260828T123313` /
`20260828T165038`)과 **컬럼 전체**로 비교한다. `run_id`·타임스탬프·`config_hash`를 제외한 모든 값이 같아야
한다(`03_improve_AB/04_engine_parity_20260829.md`의 비교 절차 재사용, 허용 1e-12). 이 비교는
`04_preregistration_overlay.md` §3의 실행 순서에서 overlay Phase A run으로 한다 — Phase A family는 overlay에서
바뀌지 않으므로 Phase A 요약은 canonical과 정확히 같아야 한다. 어긋나면 그 자리에서 멈춘다.

### 4.3 engine parity

`scan_engine=legacy`와 `polars_native_v1` 두 엔진의 `daily_ic`를 같은 config·snapshot·코드에서 비교한다. 요약의
parity는 `00_status` §0에 있다 — Phase A artifact 2개는 1.6e-15, AB 판정 8개는 exact match. Phase B·AB artifact
3개가 1e-12를 넘긴 것은 engine 차이가 아니라 `own_major_filing_activity` 정의가 두 기준 시점 사이에 바뀐 것이다.
같은 코드로 두 엔진을 돌리면 일별 값도 1e-12 안에 있어야 한다. `03_improve_AB`의 parity 하네스에 `daily_ic`
비교를 한 줄 추가한다. 복제 루프는 sink를 받지 않으므로 parity 대상은 본 스캔 결과만이다.

---

## 5. 테스트

| 파일 | 추가할 것 |
|---|---|
| `tests/unit/test_horizon_scan_scan_cell.py` | (a) `daily_sink=None`이면 반환 dict가 기존과 동일(기존 fixture 그대로); (b) sink를 주면 `emit`이 valid cell에서 정확히 한 번 호출되고 `insufficient`에서는 호출되지 않음; (c) emit된 `daily["rank_ic"].mean() == result["ic_mean"]`, 행 수 == `n_dates`; (d) 동점으로 IC가 NaN인 날에 spread만 있는 fixture에서 `daily_spread` 평균이 `q5_spread_raw`와 같음 |
| `tests/unit/test_horizon_scan_runner.py` | `run_registry_scan(daily_sink=...)`가 feature마다 `flush_feature`를 호출하고, sink 없이 부른 결과와 rows가 같음; Phase A/B 두 registry 스키마의 `cell_identity` 정규화 |
| `tests/unit/test_horizon_scan_permutation.py`, `test_horizon_scan_phase_b_robustness.py`, `test_horizon_scan_offsets.py` | 복제·offset·기간 분할 경로가 `scan_cell`에 `daily_sink`를 넘기지 않음(mock으로 kwarg 부재 확인) |
| `tests/unit/test_horizon_scan_daily_ic.py` (신규) | `ParquetDailyIcSink` 스키마·partition·`finalize` 요약·`reconcile_daily_ic` 정상/실패 경로(§4.1 표의 항목별) |
| `tests/unit/test_horizon_scan_run_spec.py` | manifest에 `daily_ic` 항목이 있을 때/없을 때 모두 유효 |
| synthetic end-to-end (`test_horizon_scan_phase_b_run.py`) | run 디렉터리에 `daily_ic/`·`daily_spread/`가 생기고 `_SUCCESS.json.daily_ic_reconciled == true` |

---

## 6. 완료 기준

- [x] 위 테스트 통과, `ruff`·`black` 통과 (PR-0)
- [x] parity 하네스에 `daily_ic`·`daily_spread` 비교 추가 (`engine_parity_report.OPTIONAL_ARTIFACTS`)
- [x] `00_읽는_법.md` §4.2·§7(a)·§10 갱신, 산출물 경로와 "저장되지 않는 것" 명시 (PR-docs)
- [x] `08_phase_b_implementation_log.md` §4.3 Stage 3 완료 표기, `cohort_ic`는 SUE 표본 없음으로 미작성 (PR-docs)
- [x] overlay Phase A run의 요약이 canonical A와 exact match (§4.2) — **실행 4 통과.**
  `horizon_ic.parquet` 412행 × 40컬럼, **max |Δ| = 1.388e-17**. `bh_pass` 57·`primary_discovery` 32·
  family 등급 A6·C4·D6·R1 전부 canonical과 동일
- [x] `_SUCCESS.json.daily_ic_reconciled == true`, 최대 차이 기록 — Phase A·B 양쪽 **true, 차이 0.0**
- [x] legacy/native `daily_ic` 차이 ≤ 1e-12 (§4.3) — **부분 통과.** `daily_spread`는 완전 일치
  (0.000e+00), `daily_ic`는 공통 868,400행에서 **max |Δ| = 2.776e-16**로 통과. 다만 **행 집합이 474행
  다르다**(native에만 존재). 전부 `own_major_filing_activity` 한 family이고, 원인은 단계 0이 아니라
  **native 엔진이 상수 횡단면에서 NaN 대신 가짜 상관을 내는 결함**이다(`10_known_issues.md` I13).
  단계 0은 side channel이라 IC를 만들지 않으며, Phase A 412 cell이 canonical과 1.4e-17로 일치한 것이
  그 증거다 — Phase B의 연속 스캔은 `run_registry_scan`/`scan_cell`이라는 같은 코드 경로를 쓴다
