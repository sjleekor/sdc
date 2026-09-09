# 06. 구현 계획

- 작성일: 2026-09-07
- 원칙: 새 패키지 `research/models/_02_updown_prob/`에 만든다. `_01_20_access_return_rank`는 **읽기 전용**(import만). 공통 라이브러리(`research/etl/labels.py`, `preprocess.py`, `metrics.py`, `splits.py`)는 **추가만** 하고 기존 기본값·시그니처를 바꾸지 않는다. 기존 유닛 테스트 1,535개와 golden parity가 그대로 통과해야 한다.
- 플랫폼 규약: `docs/target/00_shared_etl_platform.md` §8 "새 모델 추가 체크리스트"를 따른다.

---

## 1. 패키지 구조

```
research/models/_02_updown_prob/
  __init__.py
  spec.py            # ModelSpec: horizons, label outputs, feature_set_id, flow_variant, period_end=2025-07-31 강제
  features.py        # FS0/FS1/FS1h/FS2(/FS3) 컬럼 목록, 마트 뷰 매핑, interaction 정의, 기대 부호 표
  build_dataset.py   # _01.build_dataset 재사용 + 그룹 확장 + flow lag1 빌더 + rank 전처리
  train.py           # M-A/M-B/M-C, 고정 grid, fold 루프, 보정 fit
  calibrate.py       # isotonic / platt, cal slice 분리
  evaluate.py        # classification_report + evaluate + economics + reliability
  experiments/
    __init__.py
    run_matrix.py    # E0~E5 매트릭스 실행, run_spec/manifest/결과 기록, --stage E1 --h 20 --dry-run
    registry.py      # 단계별 run 정의(코드로 고정된 매트릭스)
research/etl/features/regime.py   # dim_regime_daily 마트 (Phase C 국면 모듈 재사용)
docs/dev/20260907_model_experiment/results/<E>/<run_id>/   # 산출물 (gitignore 여부는 §6)
```

---

## 2. 변경 파일과 내용

### 2.1 공통 라이브러리 (추가만)

| 파일 | 변경 | 회귀 위험 |
|---|---|---|
| `research/etl/labels.py` | `_VALID_OUTPUTS`에 `"up"`, `"top"`; `_build_select_cols`에 두 CASE 추가 | 기본 `outputs`는 그대로. `_01` 라벨 SQL 문자열 불변 → golden/스냅샷 테스트로 고정 |
| `research/etl/preprocess.py` | `PreprocessConfig.profile`에 `"rank"`; `per_date_rank_transform(panel, cols)`; `FittedPreprocess`에 rank 분기(무상태); `_cast_bool_features`에 `rg_` 접두 | `linear`·`tree` 경로 코드 불변 |
| `research/etl/metrics.py` | `classification_report(df, pred_col, y_col, k, n_bins)` → `ClassificationReport`(log_loss, brier, auc_pooled, auc_daily_mean, ece, precision_at_k, lift_at_k); `reliability_table`; `threshold_economic_report(df, pred_col, realized_col, horizon, tau, cost_bps)` | 기존 함수 불변. 새 함수는 sklearn 대비 단위 테스트 |
| `research/etl/features/regime.py` | 신규 마트 `dim_regime_daily` | Phase C 산출물과 값 일치 테스트 |

### 2.2 새 패키지

| 파일 | 핵심 |
|---|---|
| `spec.py` | `ModelSpec(model_id="02_updown_prob", horizons=(20,5,60,120), label=LabelSpec(horizons=..., outputs=("rank","up","top")), feature_set="FS0", flow_variant="lag1", preprocess_profile="rank", period_start="2015-01-02", period_end="2025-07-31", n_folds=5)`. `__post_init__`에서 `period_end > "2025-07-31"`이면 `ValueError` — holdout 침범 방지. `embargo`·`purge`는 h마다 `walk_forward_splits(horizon=h)`에서 결정 |
| `features.py` | `FEATURE_SETS: dict[str, tuple[str, ...]]`, `HORIZON_SUBSETS: dict[int, tuple[str, ...]]`, `GROUP_VIEW`(`_01._GROUP_VIEW` 확장), `INTERACTIONS`, `EXPECTED_SIGN: dict[str, int]`, `flow_lag1_columns()` |
| `build_dataset.py` | `_01.build_dataset._materialize_source_marts`를 호출해 기존 마트를 만들고(캐시), 추가 마트(`feat_fin_scan_daily` 등)는 **A0가 만든 parquet을 `register_mart_view`로 읽기만** 한다(재계산 금지). 패널 SQL에 flow `LAG`와 interaction을 넣는다. fold·전처리·manifest는 `_01` 함수 재사용 |
| `train.py` | `TrainConfig(model: "hgb_reg"|"hgb_clf"|"logit", target: "y_rank"|"y_up"|"y_top", grid: dict, seed, calibrate: "none"|"isotonic"|"platt", cal_frac=0.2)`; `walk_forward(panel, folds, cfg)` → fold별 `p_raw`, `p_cal`, 지표 |
| `calibrate.py` | `split_cal_slice(train_dates, horizon, frac)`(뒤쪽 20% 세션, embargo h), `fit_isotonic`, `fit_platt`, `apply` |
| `evaluate.py` | fold 예측 → `classification_report` + `evaluate`(Rank IC) + `topk_economic_report(k=100, cost_bps_roundtrip=60)` + `threshold_economic_report(tau=0.6)` + `economic_report`(decile) + 연도별·시장별 분해 |
| `experiments/registry.py` | `05_experiment_matrix.md`의 run 정의를 `Run(stage, h, label, model, fs, variant, seed, grid_size)` 튜플 목록으로 고정. 문서와 코드가 다르면 테스트 실패 |
| `experiments/run_matrix.py` | `--stage E1 [--h 20] [--dry-run] [--smoke]`. run마다 `run_spec.json`(git sha, 코드 hash, spec, 마트 해시) → build(캐시) → train → evaluate → `results/<E>/<run_id>/`. 선택 규칙(`05` §2)은 **코드로 구현**해 `selection.json`을 낸다 — 사람이 표를 보고 고르지 않는다 |

---

## 3. PR 분할

| PR | 내용 | 테스트 | 의존 |
|---|---|---|---|
| **M-PR0** | 이 디렉터리 문서 + `results/.gitkeep` + `spec.py`·`features.py` 골격(FS0 == BASELINE_COLS 테스트) | `test_updown_features.py` | — |
| **M-PR1** | `labels.py` `up`/`top` | null guard, `y_top == (y_cls==1)`, 기존 SQL 불변 스냅샷 | — |
| **M-PR2** | `preprocess.py` `rank` 프로파일 | rank 경계·동점·NULL, bool 컬럼 제외, `linear`/`tree` 불변 | — |
| **M-PR3** | `metrics.py` 분류·보정·임계 포트폴리오 | sklearn `log_loss`/`brier_score_loss`/`roc_auc_score` 대비 1e-12, ECE 수작업 예제, τ 포트폴리오 격자 = `rebalance_grid` | — |
| **M-PR4** | `regime.py` 마트 | Phase C `phase_c_regimes/` 산출물과 이진 값 일치, 세션 격자, warm-up NULL | — |
| **M-PR5** | `build_dataset.py` + flow lag1 + interaction | smoke build(1년, 2 fold) 컬럼 존재·행 수·NULL 비율, interaction 규칙, A0 마트 재계산 안 함(파일 mtime 불변) | PR0·2·4 |
| **M-PR6** | `train.py`·`calibrate.py`·`evaluate.py` | 합성 데이터에서 완벽 예측 → log-loss≈0, 무정보 → 0.693; cal slice가 valid와 겹치지 않음; grid 고정 | PR1·3·5 |
| **M-PR7** | `experiments/registry.py`·`run_matrix.py` | registry ↔ `05` 문서 run 수 일치, `--dry-run` 계획 출력, `--smoke` 1 run 완주 | PR6 |
| M-PR8 | E0 실행 결과 + `results/E0/` + 문서 갱신 | — | PR7 |

PR1~PR4는 서로 독립이라 병행한다. 각 PR은 `uv run pytest tests/unit -q`와 `ruff` 통과가 조건이다.

---

## 4. 실행 명령 (예정)

```bash
# 0. 마트 확인 — A0가 만든 snapshot 2026-08-23 마트가 있어야 한다. 없으면 만들지 말고 멈춘다.
ls data_lake/feature_mart/snapshot_date=2026-08-23/source=sj2_remote/ | grep -E "feat_(price|flow|fin_pit|fin_scan_daily|market_cap|event_scan_daily|filing_activity|periodic_extras|macro_exposure)"

# 1. 국면 마트
uv run --extra analysis python -m research.etl.features.regime --snapshot-date 2026-08-23 --source sj2_remote

# 2. smoke (1 run, 1년, 2 fold) — 배선 확인
uv run --extra analysis python -m research.models._02_updown_prob.experiments.run_matrix --stage E0 --h 20 --smoke

# 3. 단계 실행
uv run --extra analysis python -m research.models._02_updown_prob.experiments.run_matrix --stage E0
uv run --extra analysis python -m research.models._02_updown_prob.experiments.run_matrix --stage E1
uv run --extra analysis python -m research.models._02_updown_prob.experiments.run_matrix --stage E2
uv run --extra analysis python -m research.models._02_updown_prob.experiments.run_matrix --stage E3
uv run --extra analysis python -m research.models._02_updown_prob.experiments.run_matrix --stage E4

# 4. 단계 결과 요약 (selection.json → markdown)
uv run --extra analysis python -m research.models._02_updown_prob.experiments.run_matrix --stage E2 --summarize
```

- `--stage`는 앞 단계의 `selection.json`이 없으면 거부한다(E2는 E1 선택이 필요).
- `--smoke`는 `results/smoke/`에 쓰고 official 자격이 없다.
- 실행 전 `pgrep -f horizon_scan`으로 Horizon Scan이 돌고 있지 않은지 확인한다.

---

## 5. 테스트 목록 (신규)

| 파일 | 내용 |
|---|---|
| `tests/unit/test_labels_up_top.py` | `y_up`·`y_top` null guard, `y_top == (y_cls == 1)`, `kind="abs"`에서 `y_up`이 `fwd_ret > 0`, 기본 outputs SQL 불변 |
| `tests/unit/test_preprocess_rank.py` | 날짜×시장 내 [0,1], 동점 average, NULL 유지, bool 제외, `linear`/`tree` 결과 불변 |
| `tests/unit/test_metrics_classification.py` | sklearn 대비, ECE 예제, precision@k, τ 포트폴리오 격자·비용 |
| `tests/unit/test_regime_mart.py` | Phase C 산출물 일치, 세션 격자, warm-up |
| `tests/unit/test_updown_features.py` | FS0 == `BASELINE_COLS`, 포함 관계, FS1h ⊂ FS1, 마트 매핑 존재, interaction 정의 4개, 기대 부호 표 완전성 |
| `tests/unit/test_updown_build.py` | 합성 lake에서 flow lag1(거래정지 건너뜀), interaction 규칙, `period_end` 침범 시 `ValueError` |
| `tests/unit/test_updown_train.py` | 합성 데이터 극단 케이스, cal slice 분리, grid 고정, 시드 결정성 |
| `tests/unit/test_updown_registry.py` | run 수·단계 정의가 `05` 문서 표와 일치(문서 파싱 또는 상수 비교) |
| `tests/integration/test_updown_smoke.py` | DB 없이 로컬 lake만으로 `--smoke` 1 run 완주(마트 없으면 skip) |

---

## 6. 산출물과 저장

| 경로 | 내용 | git |
|---|---|---|
| `data/datasets/02_updown_prob/<config>/` | 패널·fold·manifest (수 GB) | ignore(기존 `data/` 규칙) |
| `docs/dev/20260907_model_experiment/results/<E>/<run_id>/` | `run_spec.json`, `fold_metrics.parquet`, `reliability.parquet`, `economics.parquet`, `summary.md` | **commit**(작다). `predictions_valid.parquet`(수백 MB)은 `data/`로 보내고 경로만 기록 |
| `docs/dev/20260907_model_experiment/results/<E>/selection.json` | 단계 선택 결과 | commit |
| `docs/dev/20260907_model_experiment/results/README.md` | M-8 결과 문서 | commit |

---

## 7. 일정 (작업 순서)

| 순서 | 작업 | 병행 |
|---|---|---|
| 1 | M-0 결정 확정(`00` §4) | — |
| 2 | M-PR0~PR4 (라이브러리 추가·국면 마트) | 4개 병행 |
| 3 | M-PR5~PR7 (패키지·러너) | 순서대로 |
| 4 | smoke → E0 (3h) → E0 기록 | — |
| 5 | E1 (5h) → 선택 → E2 (6h) → 선택 | — |
| 6 | E3·E4 (약 9h) | 둘 병행 가능(장비 여유 시) |
| 7 | M-8 결과 문서 | — |
| 8 | E5 (FS3 11 컬럼 도착 2026-09-09, run 12) — 08-23 마트 4개 재빌드가 선행(`02` §1.5) | — |
| 9 | holdout (2026-10~11) | T1·T2와 같은 날 |

코드 작업(2~3)은 며칠, 실행(4~6)은 약 23시간 장비 시간이다. Horizon Scan 재실행과 겹치지 않게 일정을 잡는다.

---

## 8. 리스크

| 리스크 | 대응 |
|---|---|
| A0 마트가 새 snapshot으로 다시 만들어져 2026-08-23 마트가 사라짐 | 실험 전 마트 경로를 `run_spec`에 해시로 기록. 사라지면 같은 해시로 재생성 후 진행 |
| `_01` 내부 함수(`_materialize_source_marts`, `_panel_sql`) 시그니처 변경 | `_01`은 동결. 바꿔야 하면 `_02` 쪽에 복사해 쓰고 출처를 적는다 |
| 메모리(패널 5M × 136 + fold 복사) | Polars lazy, fold별 슬라이스만 materialize. 필요하면 h별로 별도 프로세스 |
| E1에서 확률 기준선 대비 무개선 | `05` §3 중단 규칙. 코드 버그(라벨 정렬·리크) 점검 → 피쳐 정보량 → 라벨 정의 순으로 원인 기록 |
| 병행 스트림의 마트 이름·컬럼 변경 | FS3는 컬럼 목록을 `features.py`에 명시하고 존재 테스트로 잡는다 |
| holdout 침범(실수로 `period_end` 변경) | spec `__post_init__` 검사 + 러너에서 이중 확인 + `predictions_valid`에 2025-08-01 이후 행이 있으면 실패 |
