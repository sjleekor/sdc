# 06. 구현 계획

- 작성일: 2026-09-07
- 원칙: 새 패키지 `research/models/_02_updown_prob/`에 만든다. `_01_20_access_return_rank`는 **읽기 전용**(import만). 공통 라이브러리(`research/etl/labels.py`, `preprocess.py`, `metrics.py`, `splits.py`)는 **추가만** 하고 기존 기본값·시그니처를 바꾸지 않는다. 기존 유닛 테스트 **1,622개**(2026-09-09 실측. 작성일의 1,535는 옛 숫자다)와 golden parity가 그대로 통과해야 한다.
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

### 3.1 진행 상태 (2026-09-09)

**M-PR0~M-PR7 완료.** 코드 작업(§7의 2~3)이 끝났고, 남은 것은 실행(4~6)이다.

| PR | 산출물 | 확인한 것 |
|---|---|---|
| M-PR0 | `_02_updown_prob/{__init__,spec,features}.py`, `results/.gitkeep` | FS0 == `BASELINE_COLS` 글자 일치. 집합 크기 40/56/68/79, FS1h 46·49·54·**50**(§1.3 h120 합계 정정). `period_end`가 2025-07-31을 넘으면 `ValueError` |
| M-PR1 | `labels.py` `up`/`top` | 기본 `outputs`의 SQL sha256을 테스트에 박았다(`bd927b31…`) — 모델 01 라벨은 한 글자도 안 바뀐다. `y_top == (y_cls == 1)`, forward 없는 h는 NULL 유지 |
| M-PR2 | `preprocess.py` `rank` 프로파일 | `(trade_date, market)` 백분위, 동점 average, 플래그·`rg_*`·`_isna` 제외. fit할 통계량이 없다는 것을 "train 슬라이스를 바꿔도 결과가 같다"로 고정. `rank_null_fill`(기본 0.5, 트리는 `None`) |
| M-PR3 | `metrics.py` `classification_report`·`reliability_table`·`threshold_economic_report` | log-loss·Brier·AUC를 sklearn 대비 1e-12로 맞췄다. ECE·precision@k는 수작업 예제. τ 포트폴리오는 격자가 `rebalance_grid`와 같고, 문턱을 넘는 종목이 없는 격자일은 **현금(0.0)으로 센다** |
| M-PR4 | `research/etl/features/regime.py`, `dim_regime_daily` | snapshot 2026-08-23에 **빌드 완료**: 4,736 세션(2007-06-05~2026-08-21), 유효 2015-06-16~(`rg_market_up`만 06-24). Phase C 산출물(`phase_c_regimes/regime_series.parquet`, 2,745 세션)과 **이진 전부 일치·max\|Δz\| = 0** |
| M-PR5 | `build_dataset.py` | snapshot 2026-08-23에서 FS0 h20 2015~2016 **844,832행 3.0초**, FS2 68컬럼 3.8초. 마트는 읽기만 하고 파일 mtime 불변을 테스트로 고정. flow `lag1`은 마트 행 위의 `LAG`이고, A0 `feat_flow`에 거래정지 행이 0개(6,572,831행 중)라서 그것이 곧 valid session lag다 |
| M-PR6 | `train.py`·`calibrate.py`·`evaluate.py` | grid 16/4/3/4 고정, `min_samples_leaf=200`·`class_weight=None`·`early_stopping=False`. 보정 slice는 train 뒤쪽 20% + embargo h, valid는 건드리지 않는다. 완벽 예측 → log-loss 0, 무정보 → 0.693 확인 |
| M-PR7 | `experiments/{registry,selection,run_matrix}.py` | run 수 8·16·12·4·12 = **52**(문서 §1 표를 테스트가 직접 파싱한다), optional 포함 60. `05` §2 선택 규칙 6개를 코드로 구현하고 규칙별 테스트를 붙였다. E0 `--smoke` 8 run 완주 |

세 가지가 계획과 달라졌다.

1. **국면 마트에 warm-up 컷이 필요했다.** `label_scan`의 세션 격자는 2007년부터 시작하는데
   `common_feature_daily_fact`의 일별 계열은 2014-06-16부터다. Phase C는 세션 수(`session_idx`)로만
   막으므로, 2014-07의 `rg_vix_high`가 관측 서너 개짜리 252-세션 중위값에서 나온다. Phase C CLI는
   `--sample-start 2015-06-16`으로 잘라서 이 문제를 피한다. 마트는 자를 수 없으니 `REGIME_VALID_FROM
   = 2015-06-16` 이전을 NULL로 둔다(행은 남긴다). 이 컷이 Phase C 산출물의 시작일과 정확히 같다.
2. **`FS2h`·`FS3h` id가 생겼다.** E2-c가 "FS1과 FS1h 중 나은 쪽 위에 FS2를 얹는다"(`05` §2)고 했으므로
   쌓기가 기계적으로 필요하다. 새 가설이 아니라 조합 규칙의 이름이다.
3. **`_cast_bool_features`가 이름 기준도 본다.** `rg_*`를 넣으라는 `02` §4.1 요구가 dtype만으로는
   안 된다 — 마트가 Int8로 줄 수 있다. `px_is_halted`·`fin_is_negative_equity`·`fin_has_fs`·`rg_*`·
   `*_isna`가 목록이고, 값은 그대로 두고 Float64로만 넓힌다.

PR5~PR7에서 계획에 없던 것 다섯 개를 처리했다.

4. **마트 계약 해시가 snapshot 안에서 갈린다.** 08-23의 `feat_price`·`feat_flow`·`label_scan`은 A0
   해시(`236d0d35…`), `dim_universe_daily`는 `889c3e83…`, `feat_fin_pit`은 `ab0de634…`다.
   `register_mart_view`는 이 값이 정확히 맞아야 통과하므로 LakeConfig 하나로 셋을 다 등록할 수 없다.
   `02` §5가 "있는 것을 읽기만 한다"고 했으므로 **읽기 전용 등록**(`register_read_only`)을 쓰고, 각
   마트의 저장된 계약을 manifest에 적는다(§8 리스크 대응). 대신 `dim_universe_daily`는 더 강한 검사를
   한다 — 저장된 `sql_hash`가 이 spec의 `UniverseFilter`가 만들 SQL의 해시와 같아야 한다. 실측 결과
   08-23의 것은 **기본 필터로 만든 것이 맞다**(D-5 충족). broad 유니버스를 넣으면 거부한다.
5. **전 구간이 결측인 design 컬럼이 sklearn 1.9의 binner를 깬다.** 공매도 잔고 계열이 2016-06-30부터
   시작하므로 이른 fold의 train 슬라이스에는 값이 아예 없다. sklearn은 `_BinMapper.fit` 안에서
   numpy의 `window shape cannot be larger than input array shape`를 던지는데, 어느 컬럼 때문인지
   말하지 않는다. 그런 컬럼은 상수 0.0으로 채우고 run 요약에 적는다(정보량은 어느 쪽이든 0이고,
   빼면 fold마다 design 행렬이 달라진다).
6. **`--smoke` 구간이 5년이어야 한다.** 2년(2016-12-31)으로 잡으니 h120 fold의 train 세션이 8개만
   남아 보정 slice + embargo 120이 들어가지 않았다. `SMOKE_PERIOD_END = 2019-12-31`.
7. **run 하나가 실패해도 단계는 계속 돌고, 선택은 거부한다.** 실패한 run 디렉터리에 `failed.json`을
   남기고 나머지를 끝낸 뒤 exit 1이다. 단계 판정은 run 집합이 완전할 때만 한다.
8. **`ix_*`는 rank 전처리에서 제외한다.** interaction은 이미 `pct_rank × regime`이라 [0,1]인데, 다시
   백분위를 매기면 국면이 0인 날의 0들이 0.5로 옮겨가 "국면이 꺼져 있었다"는 표지가 사라진다.

### 3.2 E1 실행 완료 (2026-09-10)

**16 run 전부 성공.** 기록은 [`results/E1/`](results/E1/). fit 시간 합계 **3시간 52분**
(HGB 분류기 run 약 25분, 로지스틱 run 약 2.5분).

**네 h 모두 라벨은 L-A(`y_up`)다.** 모델은 갈렸다 — 짧은 h는 트리, 긴 h는 선형이다.

| h | 채택 | log-loss | E0 기준선 | Δ | 경제성 주 지표 | 기준선 | Δ | ECE |
|---|---|---|---|---|---|---|---|---|
| 5 | `y_up` + HGB | 0.67829 | 0.67937 | **−0.00109** | +0.00016 | −0.00076 | **+0.00093** | 0.0136 |
| 20 | `y_up` + HGB | 0.66912 | 0.67045 | **−0.00133** | +0.00939 | +0.00665 | **+0.00274** | 0.0128 |
| 60 | `y_up` + 로지스틱 | 0.66195 | 0.66273 | **−0.00078** | +0.02157 | +0.01523 | **+0.00633** | 0.0188 |
| 120 | `y_up` + 로지스틱 | 0.65703 | 0.65595 | +0.00109 | +0.16348 | +0.18703 | −0.02355 | 0.0321 |

`05` §3의 중단 규칙("어떤 h도 기준선보다 log-loss가 나아지지 않으면 멈춘다")은 걸리지 않았다 —
h5·h20·h60이 두 주 지표 모두에서 기준선을 넘었다.

**h120은 기준선을 못 넘었다.** log-loss도 Rank IC도 M-A(순위 회귀 + isotonic)가 낫다. 순위
목적함수를 직접 맞추는 모델이 순위 지표에서 이기는 것이고, h120의 경제성 주 지표가 Rank IC라
차이가 그대로 드러난다(0.163 대 0.187). E2는 규칙대로 E1 승자 위에 피쳐를 얹지만, h120에서
FS1·FS2가 이 격차를 못 메우면 **h120의 결론은 "확률 모델보다 순위 모델이 낫다"가 된다.** 그것도
결과이고 M-8에 그렇게 적는다.

**긴 h에서 선형이 트리를 이긴다.** h60·h120에서 로지스틱이 HGB보다 log-loss가 낮다(0.66195 대
0.66312, 0.65703 대 0.66148). `05` §2 E1이 미리 적어둔 경우다 — "그 h는 피쳐 정보가 선형으로
충분하다는 뜻이고 M-C를 채택한다". 2.5분짜리 모델이 25분짜리를 이겼다.

**L-B의 Rank IC가 음수다 — 기록해야 할 발견.** `y_top` 모델의 확률이 실현 초과수익과 반대로
정렬된다(h20에서 −0.067·−0.103, h60에서 −0.038·+0.003, h120에서 +0.035·+0.036). 같은 모델의
상위 100개 정밀도는 base rate보다 높다(h5 lift 1.51, h20 1.40). 모순이 아니다: "상위 20%에 들
확률"은 **변동성이 큰 종목을 위로 올리고**, 그 종목은 하위 20%에도 잘 든다. 그래서 극단 tail은
맞히면서 횡단면 전체 순위는 뒤집힌다(`02` §1.2가 `px_maxret_20d`에 −1을 적어둔 로또 효과).
운용 의미는 분명하다 — **L-B는 top-k 매수에는 쓸 수 있어도 순위 신호로는 못 쓴다.** `01` §1.3에
이 관측을 붙인다.

**규칙이 일한 자리.** h60에서 표의 최고 경제성은 `y_top` + HGB(+0.03184)였다. 사람이 표를 보고
골랐으면 그것을 집었을 것이다. 규칙은 라벨 안에서 log-loss로만 고르라고 했으므로 `y_top`의 대표는
로지스틱(+0.01268)이 되고, 그 대표가 L-A에 졌다. +0.03184의 fold 표준편차가 0.0223 — 값의 70%다.

**보정.** 열여섯 run 모두 ECE ≤ 0.0372이고 채택된 넷은 ≤ 0.0321이다. 보정 단계 없이 log-loss로
학습한 것만으로 확률이 맞았다(`04` §3.1의 예상대로). h120의 0.0321만 문턱 0.03을 살짝 넘는다.

**로지스틱 수렴 문제는 없었다.** 로그에 `ConvergenceWarning`이 하나도 없다. smoke에서 `max_iter=500`에
걸린 것은 `tree` 프로파일의 스케일 안 맞은 행렬 때문이었고, E0가 고른 `rank` 프로파일(0~1)에서는
정상 수렴한다 — §3.4의 열린 질문은 이것으로 닫혔다.

---

### 3.3 메모리 구조 수정 (2026-09-10, E0 뒤 · E1 전)

E0가 끝난 뒤 실측한 값이 `05` §3의 추정(6~8GB)과 달라서 두 곳을 고쳤다. **E0 결과는 그대로
유효하다** — 저장 방식과 읽는 방식만 바뀌었고 각 fold가 보는 값은 같다.

**1. design 행렬을 fold별로 복사하지 않는다.** `rank` 프로파일은 `(trade_date, market)` 안의
백분위이고 fold 슬라이스는 날짜 단위로 잘리므로, 어느 fold에서 변환해도 한 행은 같은 값이 된다
(`02` §4.1). 그런데 예전 코드는 fold마다 train+valid 슬라이스를 만들어 하나로 이어 붙였다 —
expanding fold 5개면 앞쪽 연도가 다섯 번 들어간다. 상태 없는 프로파일(`rank`)은 변환한 패널
하나만 쓰고 fold 표가 날짜 경계를 말하게 했다. `tree`·`linear`는 fold마다 winsor 경계를 fit하므로
여전히 fold별 파트지만, 한 장씩 디스크로 흘려보내 메모리에 쌓지 않는다.

| | 예전 | 지금 |
|---|---|---|
| FS0 h20 design 행렬 | 3.52GB (한 파일) | **0.94GB** (한 파트) |
| 저장 형태 | `feat_panel_std.parquet` | `feat_panel_std/` 디렉터리 |

두 방식이 같은 값을 낸다는 것도 테스트로 고정했다(`test_updown_build.py`).

**2. 학습이 design 행렬 전체를 읽지 않는다.** `train.DatasetFolds`가 fold 하나의 train·valid만
읽는다. 루프 순서도 grid 바깥 → **fold 바깥, grid 안쪽**으로 바꿔서 슬라이스를 grid 점마다 다시
읽지 않고 run당 두 번만 읽는다(sweep + 승자 재적합). `walk_forward`는 예전처럼 프레임을 직접
받을 수도 있고(테스트), 두 경로가 같은 숫자를 낸다는 테스트가 있다.

**실측 (2026-09-10, FS0, design 80컬럼, 전 구간 5 fold).**

| 단계 | peak RSS |
|---|---|
| 빌드 | 16.4GB (DuckDB `memory_limit=6GB`을 걸어도 17.5 → 16.4로만 내려간다 — DuckDB 조인이 아니라 Polars 변환이 주다. 그래서 상한은 박지 않았다) |
| 학습: 최대 fold(train 439만 행) 읽기 | **3.5GB** |
| 학습: 거기서 1 fit | **12.7GB** (sklearn이 행렬을 복사하는 몫) |

**FS2 실측 (2026-09-10, E2 직전). 추가 수정은 필요 없다.**

E2 전에 재기로 한 값이다. 추정(빌드 25GB·학습 21GB)이 둘 다 틀렸다.

| | FS0 (design 80) | FS2 (design 136) |
|---|---|---|
| 빌드 peak RSS | 16.4GB | **15.7GB** |
| 최대 fold 읽기 | 3.5GB | 5.1GB |
| 거기서 1 fit | 12.7GB | **18.3GB** |
| 패널 + design 디스크 | 1.08 + 0.94GB | 1.50 + 1.24GB |

**빌드 peak는 피쳐 수를 따라가지 않는다.** peak를 지배하는 것은 DuckDB의 패널 조인(flow 15개
LAG 윈도우를 650만 행 위에서)과 Arrow→Polars 변환이고, 피쳐 28개가 더 붙어도 거의 움직이지
않는다. 학습 peak는 1.44배 늘었지만(12.7 → 18.3GB) 36GB 장비에서 17GB가 남는다.

그래서 **E2는 지금 코드로 그대로 돌린다.** 변환을 연도별로 쪼개는 안은 접는다 — 재보니 필요
없었고, 필요 없는 최적화는 검증할 것만 늘린다.

**같이 나온 시간 실측.** 16-grid의 가장 무거운 점(`max_iter=400, max_leaf_nodes=31`)이 FS2
최대 fold(457만 행)에서 **fit당 157초**다. 100만 행당 34초이고, 이 값으로 `--dry-run`의
추정을 갱신했다. 같은 김에 fit 수 계산도 고쳤다 — `grid x fold x 2`로 세고 있었는데 실제는
`grid x fold + fold`(sweep 뒤 승자만 fold마다 재적합)라 E2가 1,140이 아니라 **630 fit**이다.

---

### 3.4 E0 실행 완료 (2026-09-09)

**8 run 전부 성공.** 기록은 [`results/E0/`](results/E0/)(`runs.md`, `selection.json`, run별 디렉터리).
패널 **5,223,425행**, 5 fold, grid 4점, fit 시간 합계 **100분**(run당 617~944초).

**네 h 모두 `rank` 프로파일이 채택됐다.** 이후 단계는 전부 `rank`로 간다.

| h | Rank IC (rank) | Rank IC (tree) | 차 | 판정 | 기존 게이트 |
|---|---|---|---|---|---|
| 5 | 0.1115 | 0.1142 | −0.0027 | rank(허용치 0.005 안) | 0.1155 |
| 20 | 0.1496 | 0.1515 | −0.0019 | rank(허용치 안) | 0.1436 |
| 60 | 0.1802 | 0.1766 | +0.0035 | rank(우세) | 0.1753 |
| 120 | 0.1870 | 0.1792 | +0.0078 | rank(우세) | **없었다** |

**확률 기준선**(M-A + isotonic → `y_up`). E1의 분류기는 이 log-loss를 이겨야 한다.

| h | log-loss | ECE | 경제성 주 지표 |
|---|---|---|---|
| 5 | 0.67937 | 0.0153 | top-k 비용반영 **−0.00076** |
| 20 | 0.67045 | 0.0194 | top-k 비용반영 +0.00665 |
| 60 | 0.66273 | 0.0270 | top-k 비용반영 +0.01523 |
| 120 | 0.65595 | 0.0303 | Rank IC +0.18703 |

E0가 답하기로 했던 세 가지(`05` §2 E0)의 답.

1. **경계 2025-07-31의 baseline은 기존 게이트와 이어진다.** h20이 0.1436 → 0.1515(tree),
   h60이 0.1753 → 0.1766이다. 구간을 당긴 쪽이 오히려 조금 높다. h5는 0.1155 → 0.1142로 거의 같다.
2. **h120 baseline이 처음 생겼다.** Rank IC 0.1870으로 네 h 중 가장 높고, log-loss도 가장 낮다.
   격자가 22개뿐이라 경제성은 진단으로만 읽는다(그래서 h120의 주 지표가 Rank IC다).
3. **`rank`가 `tree`와 같은 수준이다.** h60·h120에서는 오히려 낫다. 전처리를 통일한다.

기록해 둘 두 가지. **h5의 비용 반영 수익이 음수(−0.00076)다** — 연 50회 회전에 turnover 0.7이라
비용이 수익을 넘는다. `03` §3.2가 h5에 경고한 그대로이고, h5에서 무엇을 채택하려면 이 값을 넘겨야
한다. 그리고 **h120 기준선의 ECE가 0.0303으로 문턱 0.03을 살짝 넘는다.** M-A는 후보가 아니라
기준선이므로 `03` §3.3의 후보 조건에 걸리는 것은 아니지만, h120에서 보정이 가장 어렵다는 신호다.

**앞서 열려 있던 M-C 프로파일 문제는 해소됐다.** 네 h 모두 `rank`를 골랐으므로 로지스틱이 스케일
안 맞은 행렬을 받는 경우가 없다. smoke에서 h20이 `tree`로 간 것은 5년 구간·grid 1점의 결과였다.

---

**`--smoke` 로 확인한 것과, 거기서 나온 열린 질문 하나.**

E0 8 run이 완주하고 선택까지 나왔다(5년 구간·grid 1점·2 fold이므로 **공식 결과가 아니다**).
E0 규칙이 문서대로 동작했다: h5 rank 채택(0.0951 vs 0.0996, 차 0.0045 < 0.005), h20 tree 유지(차
0.0071), h60 rank(차 0.0002), h120 rank(0.1407 vs 0.1336). E1 h20 4 run도 E0 선택을 물려받아
완주했고, M-B `y_up`의 log-loss 0.6778이 M-A 기준선 0.6805보다 낮았다.

그런데 **E0가 어떤 h에서 `tree`를 고르면 M-C(로지스틱)의 입력이 계획과 달라진다.** `04` §1은 "세
모델 모두 입력은 `rank` 프로파일의 design 행렬"이라고 적었는데, `05` §2 E0의 규칙은 프로파일을 뒤
단계 전체에 물려준다. smoke에서 h20이 `tree`로 갔고, 그 결과 로지스틱이 `max_iter=500`에서 수렴하지
못했다(`ConvergenceWarning`). 스케일이 안 맞은 행렬에서 나온 확률이라 보정도 깨졌다 —
`y_top-logit`의 ECE가 **0.2306**이다. h20 `y_up-logit`도 ECE 0.0695로 문턱 0.03을 크게 넘었다.

공식 E0가 네 h 모두 `rank`를 고르면 이 문제는 생기지 않는다. 하나라도 `tree`가 나오면 그때
**M-C만 `rank` 프로파일로 돌릴지**를 정해야 한다(`04` §1의 문장을 따르는 쪽). 코드에서는
`run_matrix.spec_for`의 한 줄이다. 결과를 보고 고르는 것이 아니라 E1 실행 전에 정할 일이다.

**측정한 실행 비용.** fit 하나가 train 100만 행당 약 14~25초다(2026-09-09, 이 호스트). 전 구간
fold의 train이 약 300만 행이므로 fit 하나에 1~2분이고, grid 16 × 5 fold × (sweep + 재적합) = 160 fit
이면 run 하나가 **약 3시간**이다. `05` §2.1의 "grid 16이면 약 1시간"보다 크다 — E0 공식 실행에서
실측해 `05`의 예산을 갱신해야 한다.

---

### 3.5 E2 실행 완료 (2026-09-11)

**12 run 전부 성공.** 기록은 [`results/E2/`](results/E2/). fit 시간 합계 **10시간 36분**
(38,139초) — HGB run(h5·h20)이 5,159~7,217초, 로지스틱 run(h60·h120)이 248~441초다.
`05` §1의 예산 6시간을 크게 넘었다. h5·h20이 E1에서 M-B를 골라 grid 16을 물려받았기 때문이다.

**채택은 h20 하나뿐이다.**

| h | 채택 | 판정 |
|---|---|---|
| 5 | FS0 유지 | 후보 3개 모두 fold 조건에서 기각 |
| 20 | **FS1h** | 약한 개선(개선폭 < fold 표준편차) |
| 60 | FS0 유지 | 후보 3개 모두 log-loss가 올랐다 |
| 120 | FS0 유지 | 피쳐를 늘릴수록 단조롭게 나빠진다 |

후보별 델타는 이렇다(경제성 주 지표는 h120만 Rank IC, 나머지는 top-k 비용 반영 수익).

| h | 후보 | 기준 | Δ log-loss | Δ 경제성 | fold | 판정 |
|---|---|---|---|---|---|---|
| 5 | FS1 | FS0 | −0.000082 | +0.000518 | 3/5 | 기각(fold) |
| 5 | FS1h | FS0 | −0.000103 | −0.000013 | 4/5 | 기각(경제성) |
| 5 | FS2 | FS0 | −0.000179 | +0.000516 | 3/5 | 기각(fold) |
| 20 | FS1 | FS0 | −0.000374 | +0.001886 | 3/5 | 기각(fold) |
| 20 | **FS1h** | FS0 | **−0.000396** | **+0.002788** | **4/5** | **약한 개선** |
| 20 | FS2 | FS1h | −0.000018 | −0.003880 | 3/5 | 기각(경제성·fold) |
| 60 | FS1 | FS0 | +0.000063 | +0.009171 | 2/5 | 기각(log-loss) |
| 60 | FS1h | FS0 | +0.000076 | +0.013927 | 2/5 | 기각(log-loss) |
| 60 | FS2 | FS0 | +0.000347 | +0.012539 | 3/5 | 기각(log-loss) |
| 120 | FS1 | FS0 | +0.000905 | +0.006359 | 1/5 | 기각(log-loss) |
| 120 | FS1h | FS0 | +0.002290 | +0.000214 | 1/5 | 기각(log-loss) |
| 120 | FS2 | FS0 | +0.003983 | −0.005373 | 1/5 | 기각(log-loss) |

E2가 답하기로 했던 것(`05` §2 E2)의 답은 **"피쳐를 더 넣어서 얻는 것이 거의 없다"**다.
h를 늘릴수록 더 그렇다. 기록해 둘 네 가지.

1. **h20의 개선은 FS1h에서 멈춘다.** FS1h를 채택한 뒤 그 위에 FS2를 얹으면 log-loss는 거의
   그대로(−0.000018)인데 경제성이 −0.00388 떨어진다. h 부분집합(FS1h)이 전체집합(FS1)보다
   나은 것도 방향이 맞다 — h20에 안 맞는 윈도우를 뺀 쪽이 이겼다.
2. **h60·h120에서 두 주 지표가 서로 반대로 움직인다.** h60은 FS1h가 경제성을 +1.39pp
   올리는데 log-loss가 +0.000076 올라 기각됐다. `03` §3.1이 "여러 지표 중 좋은 것을 고르지
   않는다"고 미리 못 박아 둔 상황이 실제로 나왔고, 규칙대로 확률 지표를 따랐다. 다만
   h60에서 경제성만 보면 피쳐 확장이 이겼다는 사실은 남겨둔다.
3. **h5는 평균으로는 개선인데 fold에서 걸렸다.** FS1·FS2 둘 다 log-loss와 경제성이 같이
   좋아졌지만 5 fold 중 3개만 개선해 `03` §3.3의 "4 이상" 조건을 못 넘었다. h5에서 비용
   반영 수익이 처음 확실히 양수로 나온 것(FS1·FS2 모두 +0.00068, E0 기준선 −0.00076,
   E1 승계 +0.00016)은 기록해 둘 값이다.
4. **h120이 보정 문턱을 못 넘는다.** 채택된 h120 config(E1 `y_up`-logit)의 ECE가 0.0321로
   문턱 0.03을 넘고, L-A h120 run은 **전부** 넘는다(E0 M-A 0.0303, E1 logit 0.0321, E1 HGB
   0.0372, E2 후보 0.0357~0.0393). 한 config의 문제가 아니라 h120·L-A 전체의 성질이다.
   `03` §3.3이 요구하는 재보정을 §3.6에서 돌렸고, **보정 뒤에도 넘었다**.

---

### 3.6 h120 ECE 재보정 (2026-09-11)

**보정 뒤에도 문턱을 넘었고, 규칙대로 h120을 후보에서 뺐다.** `p_cal`의 fold 평균 ECE가
**0.0323**으로 0.03을 넘는다. `03` §3.3에 따라 h120에는 채택 config가 없고, 뒤 단계는
h5·h20·h60만 돈다.

기록은 [`results/recheck/E2/E2_h120_recal-isotonic_seed0/`](results/recheck/E2/E2_h120_recal-isotonic_seed0/)
(`recheck.json`에 판정, 나머지는 다른 run과 같은 산출물). 146초.

**재보정은 컬럼을 하나 더 붙이는 일이 아니라 새 fit이다.** 보정을 켜면 `04` §4대로 fold train의
뒤쪽 20%를 모델 학습에서 떼어내므로 `p_raw`까지 달라진다. 그래서 이 run의 확률·경제성 숫자는
채택 config와 비교할 수 없고, 답하는 질문은 하나뿐이다 — 보정된 확률이 문턱 안에 드는가.

| | 채택 config (E1 h120 `y_up`-logit) | 재보정 run `p_raw` | 재보정 run `p_cal` |
|---|---|---|---|
| ECE | 0.0321 | 0.0587 | **0.0323** |
| log-loss | 0.65703 | 0.66564 | 0.66003 |
| Rank IC | +0.16348 | +0.13021 | +0.13021 |

읽을 것 셋.

1. **isotonic은 제 일을 했다.** cal 슬라이스를 빼고 적합한 모델의 ECE가 0.0587까지 나빠졌고,
   보정이 그것을 0.0323까지 되돌렸다. 문턱을 못 넘은 것은 보정이 약해서가 아니라 학습 데이터
   20%(그것도 valid에 가장 가까운 20%)를 뺀 손실을 다 메우지 못했기 때문이다.
2. **`p_cal`과 `p_raw`의 Rank IC가 같다.** isotonic이 단조 변환이라 순위를 바꾸지 않는다.
   보정 절차가 의도대로 동작했다는 확인이다.
3. **문턱을 넘기는 것은 앞쪽 fold다.** fold별 `p_cal` ECE가 0.0509 / 0.0454 / 0.0171 / 0.0197 /
   0.0285다. train이 190만 행뿐인 fold 1·2가 평균을 끌어올리고, 뒤쪽 세 fold는 문턱 안이다.
   h120에서 보정이 어려운 것은 학습 표본이 적을 때의 성질로 보인다.

**결정: 규칙대로 h120을 후보에서 뺀다 (2026-09-11).** 검토한 선택지는 셋이었다.

| 안 | 내용 | 비용 | 판정 |
|---|---|---|---|
| **(a) 규칙대로** | h120을 후보에서 빼고 채택 config 없이 남긴다 | E3·E4가 h120을 못 돌린다 | **채택** |
| (b) L-B로 내린다 | `y_top`은 ECE 0.0218로 통과 | Rank IC가 +0.1635 → +0.0361. 주 경제 지표를 4.5배 버린다 | 기각 |
| (c) 순위 모델로만 채택 | 확률 주장을 빼고 Rank IC로만 h120을 읽는다 | `03` §3.3의 보정 조건을 h120에서 완화하는 것 | 기각 |

(c)를 골랐다면 "왜 h120에서만 문턱을 완화하는가"를 결과와 무관하게 설명할 수 있어야 했다.
결과를 보고 게이트를 고치는 것이라 하지 않았다. **`03`은 고치지 않았다** — 이 결정은 이미
적혀 있던 규칙을 적용한 것이고, 사전등록 수정이 아니다.

`p_cal` ECE 0.0323은 문턱을 0.0023 넘은 값이고 fold 분산(0.017~0.051)이 그보다 훨씬 크다.
근거가 강하지는 않다. 규칙을 미리 적어 둔 이유가 이런 자리에서 다투지 않기 위해서였으므로,
증거의 세기를 다시 재지 않고 규칙을 따랐다. h120을 살리려면 **다음 스냅샷에서 fold 1·2의
학습 표본이 늘어난 뒤 다시 재는 것**이 맞는 순서다.

**코드에 반영한 것.**

- `select_e2`가 재보정 판정을 받는다(`rechecks` 인자). 문턱을 넘은 승자는 재보정 기록이
  없으면 지금처럼 pending이고, "여전히 넘는다"는 기록이 있으면 **`by_horizon`에서 빠지고**
  `dropped_horizons`에 이유와 함께 들어간다.
- `run_matrix.load_rechecks`가 `results/recheck/<stage>/`를 읽어 넘긴다. 그래서 `--stage E2`를
  다시 돌려도 같은 drop이 재현된다 — 판정을 파일에 적어 두는 것이 요점이다.
- `resolve_stage`가 앞 단계에서 빠진 h의 run을 건너뛴다. E3는 이제 4 run이 아니라 **3 run**
  (h5·h20·h60)이고, 건너뛴 이유를 한 줄 찍는다. 단계 완주 판정도 이 3개를 기준으로 하므로
  E3·E4의 selection은 정상적으로 쓰인다.
- 테스트: `select_e2`의 세 갈래(pending / drop / 통과)와 `load_rechecks`·`resolve_stage`
  건너뛰기. 단위 테스트 **1,900개 통과**.

**같이 확인된 규칙 구멍 하나.** ECE 문턱 검사가 `select_e2`에만 있다. E1의 승자도 `03` §3.3이
말하는 "채택 후보"인데 `select_e1`은 문턱을 보지 않아서, h120 L-A가 E1에서 통과한 뒤 E2에서야
걸렸다. E1 시점에 걸렸다면 h120의 라벨 선택을 그때 다시 봤을 것이다. 지금 결과가 달라지지는
않지만(E2가 같은 config를 그대로 물려받았다), 뒤 단계 선택 규칙에 같은 검사를 넣어야 한다.

---

### 3.7 E3 실행 완료 (2026-09-11)

**3 run 전부 성공.** 기록은 [`results/E3/`](results/E3/). h120은 §3.6에서 빠져 4 run이 아니라
3 run이다. fit 시간 합계 **2시간 49분**(h5 4,943초, h20 5,004초, h60 179초).

E3는 채택을 바꾸지 않는다. 숫자는 "당일 수급 집계를 실제로 받을 수 있는지 검증할 값어치가
있는가"를 재는 데만 쓴다.

| h | 채택(`lag1`) | `native_t` | Δ log-loss | Δ 경제성 | fold sd (log-loss) |
|---|---|---|---|---|---|
| 5 | 0.67829 / +0.00016 | 0.67798 / +0.00148 | −0.000309 | **+0.001320** | 0.00253 |
| 20 | 0.66872 / +0.01218 | 0.66855 / +0.01013 | −0.000168 | −0.002049 | 0.00456 |
| 60 | 0.66195 / +0.02157 | 0.66186 / +0.02500 | −0.000095 | +0.003430 | 0.00685 |

**h가 짧을수록 당일 수급이 값어치를 한다.** log-loss 개선폭이 h5 −0.000309, h20 −0.000168,
h60 −0.000095로 단조롭게 줄어든다. 수급은 하루짜리 신호라 예측 구간이 길어질수록 하루 늦은
값과 당일 값의 차이가 묻힌다 — 방향이 그럴듯하고, 세 h에서 어긋나지 않는다.

**h5에서만 경제성이 뚜렷하게 움직인다.** 비용 반영 수익이 +0.00016 → +0.00148로 9배가 된다.
E0 기준선이 −0.00076이었고 E1 승계가 +0.00016이었으니, h5가 비용을 겨우 넘던 상태에서 확실히
넘는 상태로 바뀐다. ECE(0.0136 → 0.0133)·precision@k(+1.17pp)·lift(+0.028)가 전부 같은 방향이고
log-loss도 5 fold 중 4개에서 좋아졌다. turnover는 0.768 → 0.835로 늘었는데, 더 거래하고도
60bp를 내고 더 남겼다.

**h20은 반대로 경제성이 내려간다**(−0.00205). log-loss는 좋아지는데 top-k 수익이 나빠지는,
E2 h60에서 봤던 어긋남이 여기서도 나온다.

**그래서 다음 과제는 h5에 한정된다.** "장 마감 직후 수급 집계를 받을 수 있나"를 수집
스트림에서 검증할 이유는 h5에만 있다. h60은 log-loss 차이가 fold 표준편차의 1.5%라 투자할
근거가 없고, h20은 확률은 좋아지지만 경제성이 나빠져 방향이 갈린다. 세 h 모두 개선폭이 fold
표준편차 안이라는 점은 같이 읽어야 한다 — **미검증 경로가 사 오는 것이 잡음보다 크지 않다.**

**같이 고친 버그 하나.** `select_e3`·`select_e4`가 채택 config의 기록을 **E2 디렉터리에서만**
찾고 있었다. E2가 아무것도 채택하지 않은 h(여기서는 h5·h60)의 승자는 E1의 run이라 E2에는
없고, 그래서 E3의 Δ가 `NaN`으로 적혔다. E4는 더 나빠서 `ValueError: E4 h=5 has no E2 winner`로
죽었을 것이다 — E4를 돌리기 전에 걸린 것이 다행이다. `adopted_records`가 run_id로 여러 단계의
기록을 훑도록 고쳤고, 빠진 h는 건너뛴다. 테스트 4개를 붙였다(단위 테스트 **1,904개 통과**).

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

# 5. ECE 재보정 (조건부 — selection.json의 pending_calibration_recheck에 올라온 h만)
uv run --extra analysis python -m research.models._02_updown_prob.experiments.recheck_calibration --stage E2 --h 120
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
| `tests/unit/test_updown_recheck.py` | 재보정 run이 단계 선택에서 config를 읽고 isotonic만 켠다, 판정이 `p_cal`을 읽는다, 기록이 단계 디렉터리 밖으로 간다 |
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
| 6 | E3·E4 (h120 제외 — §3.6) | 둘 병행 가능(장비 여유 시) |
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
