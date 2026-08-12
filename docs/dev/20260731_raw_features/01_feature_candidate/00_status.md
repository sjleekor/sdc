# 00. 진행 상태 — 이 디렉터리를 이어받는 사람이 먼저 읽는 문서

- 작성일: 2026-08-11 (갱신: 2026-08-13 05:10 KST)
- 브랜치: `refactor/parquet-compute-reproducible`
- 목적: 문서가 8개(합계 30만 자 이상)라 어디까지 왔는지 한눈에 안 보인다. 이 파일은
  **지금 상태와 다음에 할 일만** 적는다. 근거·설계는 각 문서로 넘긴다.

## 0. 새 세션이면 여기부터

**수집도 export도 Phase B도 끝났다. 남은 건 로컬 계산 두 번뿐이다** — `--phase A` 재실행,
그다음 `--phase AB`. prod에 더 받을 것은 없다.

```bash
# 지금 위치 확인 — B는 있고 A(2026-08-12)는 없어야 정상이다
find research/output/horizon_scan -name manifest.json | sort
```

이어서 할 일은 이 한 줄이다. **`--snapshot-date`를 주지 않는다** — `auto_selected`가 꺼지면
official 자격을 잃는다. 자동 선택이 최신 유효 snapshot(2026-08-12)을 고른다.

```bash
uv run python -m research.analysis.horizon_scan --phase A --source sj2_remote
```

Phase B가 5시간 30분 걸렸으니 A도 비슷하게 잡는다. 끝나면 §5의 10단계(AB)로 간다.

**등급이 전부 NE인 걸 보고 놀라지 않는다.** family card의 등급·discovery 집계는
`q_fdr_global_ab` 기준이고 그 값은 phase=AB run이 만든다. AB를 돌리기 전까지는 NE가 정상이다.

주의: Phase A와 B를 동시에 돌리지 않는다. 정합성 문제는 없지만(출력 경로 분리, A는 A0 마트를
읽기만 함, DuckDB 1.5.4는 같은 `.tmp`를 공유해도 충돌하지 않는 것을 실측으로 확인) DuckDB가
인스턴스마다 `threads=14`·`memory_limit=28.7GiB`를 기본으로 잡아 14코어/36GB 장비에서 서로를
느리게 만든다.

## 1. 트랙이 두 개다

| 트랙 | 대상 | 상태 |
|---|---|---|
| **T1. px/flow 피쳐 검증** | 가격·수급 피쳐 17 family (Phase A0/A → acceptance gate) | **판정까지 완료**. 20일 모델 채택은 보류(§3) |
| **T2. 재무/이벤트 피쳐 검증** | fin_*/ev_* 8 family, 38 candidate cell (Phase B) | **Phase B 완주**(38/38 ready). Phase A 재실행 → AB가 남았고 등급은 그때 정해진다 |

## 2. 문서 지도

| 파일 | 내용 | 트랙 |
|---|---|---|
| `02_feature_candidate.md` | 후보 정의, §6.1 acceptance gate 8개 기준 | 공통 |
| `03_horizon_predictive_power_plan.md` | 호라이즌 예측력 검증 설계 | 공통 |
| `04_specific_plan_A0.md` / `_A.md` / `_B.md` | 실행 계획 = **사전등록 계약**. 결과를 본 뒤 고치지 않는다 | T1 / T2 |
| `05_phase_a_results_explained.md` | Phase A 결과 해설 | T1 |
| `06_grade_a_deep_dive/` | Grade A 6개 family 종목 단위 상세(+`07_glossary.md`) | T1 |
| `07_phase1_acceptance_gate.md` | 증분성·경제성·holdout 판정과 채택 결론 | T1 |
| `08_phase_b_implementation_log.md` | B-PR1~15 구현 기록, **현재 실행 상태(§3)·남은 작업(§4)** | T2 |

계약(`04_*`)과 기록(`08`)의 역할이 다르다. 임계값·판정 기준은 **결과를 보기 전에** `04`에
적고, 실행 결과와 실측치는 `08`에 적는다. 이 구분을 깨지 않는 게 이 트랙의 핵심 규율이다.

## 3. T1 — 끝났다 (20일 모델 채택 보류)

Grade A 6개 중 `px_amihud_20d`/`px_near_52w_high`는 baseline 모델에 이미 있던 피쳐라 제외,
나머지 4개 `px_reversal_5d` · `px_maxret_20d` · `px_idio_vol_60d` ·
`flow_individual_netbuy_to_volume_{5,20}d`가 판정 대상이었다.

- 증분성(기준 ⑥): 충족 — walk-forward 11년 5-fold에서 3개 horizon 모두 개선(20일 Δ+0.0081).
- 경제성(기준 ⑤): **미충족** — k=100 실매수 리스트 기준으로 배포 대상인 20일에서 candidate가
  baseline보다 −0.0045 낮다. 60일만 반대 방향(+0.0092). → `07` §6.1
- holdout(기준 ⑧): 불확정 — 평가일 16일, 리밸런싱 1회로 표본이 너무 작다.

**결론: 4개 candidate를 20일 모델에 채택하지 않는다.** 남은 건 하나다.

- **h=60 holdout 재평가** — 2026년 10~11월 이후 **새 구간으로 한 번만**(이번 구간 재사용
  금지). 위 60일 결과도 이때 같이 본다. → `07` §6

## 4. T2 — 데이터가 붙었고 Phase B는 완주했다

작업 패키지 B-0~B-9 완료(§5.5 segment 진단 제외), B-10은 Stage 1·2·4·5 완료로 **Stage 3만**
남았다. 유닛 테스트 939개 통과(2026-08-12).

`M_B_ready=0` 동결이 풀렸다. `run_id=20260812T231507-f9117ce1`에서 **38 candidate 전부
ready**가 됐고 B-PR11~B-PR15 오케스트레이션이 실제 데이터로 처음 돌았다 — 통합 크래시는 없었다.

Phase B 단독 기준으로 `bh_pass` 18, `primary_discovery` 14다. 다만 **family 등급은 전부 NE**이고
이건 정상이다 — 등급과 discovery 집계는 `q_fdr_global_ab` 기준인데 그 값은 phase=AB run이
만들기 때문이다. 아직 안 돌렸다.

지금까지 드러난 것 둘. **temporal placebo가 유일한 병목이다** — robustness 탈락 7건이 전부
`temporal_null_pass=false`다(`ev_payout_yield` 3 · `fin_value_z` 3 ·
`ev_net_share_issuance_yoy` 1). 그리고 **`fin_sue`·`fin_gross_profitability`는 표본이 없다**
(coverage 0.0000 / 0.0315).

상세: `08` §3.0(이번 run 측정값), §4(남은 작업).

## 4b. 2026-08-12에 한 일

수집을 기다리는 동안 코드 쪽을 정리했다. 커밋 순서대로다.

| 커밋 | 내용 |
|---|---|
| `21f0502` | capital-change vintage dedup + §4.4.1 distance probe |
| `be58bc7` | k=100 실매수 리스트 비용 측정 → T1 경제성 미충족 판정 |
| `e3c9e59` | B-10 Stage 2 raw측 진단 2종 |
| `ac68f33` | `isu_dcrs_stle` 카탈로그 v2 (미분류 22.4% → 4.3%) |
| `6e00193` | Stage 2 마트측 진단 5종 + 7종 전부 run 디렉터리에 연결 |
| `486642b` | `sync-filings`를 `dart-backfill-all-years.sh` 마지막 단계로 연결 |
| `17a0a66` | B-10 Stage 4 family 카드 |
| `8550fbc` | **B-2 결함 3건 수정**(§4c) |
| `5705519` | B-9 source 비치명 경고 → grade A 상한 |
| `ce8ade3` | B-10 Stage 5 run 리포트 2종 |

이 중 두 가지는 **진단을 먼저 만든 덕에** 찾은 것이다.

1. `isu_dcrs_stle` 카탈로그 구멍 — 안 고치고 probe를 돌렸다면 지표 ②가 매핑 구멍을 재고
   있는데 vintage 정책 탓으로 읽혔을 것이다. `000040` identity 통과율이 5/10 → 9/10.
2. B-2 결함 3건 — Stage 2 진단을 실제 lake에 돌리자마자 나왔다.

## 4b-2. 2026-08-12 오후 ~ 08-13 새벽에 한 일

수집 대기가 풀리면서 파이프라인을 끝까지 밀었다. 순서대로다.

| 시각 | 내용 |
|---|---|
| 12:53 / 14:27 | 1차 백필 종료 — filings 2015~2026, capital vintage 2016·2020·2024 |
| 16:51~17:23 | raw export `snapshot_date=2026-08-12` 발행. 두 테이블이 exporter 설정에 없어 먼저 등록해야 했다 → `5631eeb` |
| 17:24 | vintage probe 판정 → **strict PIT 채택**, `DEFAULT_VINTAGE_POLICY` 고정 → `bfcb252` |
| 17:36~21:45 | 판정이 요구한 잔여 8개 연도 vintage 백필. 전 연도 `rc=0` |
| 18:00 전후 | A0 마트 경로 정리(§5 주의 참고). `compute_all --features`로는 A0를 못 만든다 |
| 23:14 | capital_change만 `--force-table` 재export. 71,535 → 245,120행 |
| 23:15~04:49 | **Phase B 정식 실행 완주** — 38/38 ready → `08` §3.0 |

문서 커밋은 `93f833d`(상태 문서 재작성)이고, `08` §3.0·§4.1.2·§4.1.3은 이 갱신에 포함된다.

## 4c. B-2 결함 3건 — 찾고 고쳤다

뿌리는 하나다. 한 filing의 XBRL fact가 **여러 회계기간(당기·전기·전전기)과 연결/별도 두 축**에
걸쳐 있는데 B-2가 filing당 하나뿐인 것처럼 다뤘다. 세 곳에서 같은 실수를 하고 있었다.

| 지표 | before | after |
|---|---|---|
| `period_end_conflict` | 1,498,788 (98.3%) | **0** |
| FY2024 연간 `statement_period_end` 최빈값 | 2022-12-31 (64,688행) | **2024-12-31 (65,926행)** |
| 페어링 `verified` | 212,011 | **1,344,746** |
| 페어링 `value_mismatch` | 1,132,907 | **157** |

이제서야 `receipt_value_pairing_required`를 게이트로 쓸 수 있다. 그전 값은 데이터가 아니라
조인 버그를 재고 있었다. 남은 157건(0.0117%)은 Q1 `net_income` 계열에 몰려 있다 — Q1은
3개월과 누적이 같은 기간이라 갈라낼 수 없는 유일한 분기다. 게이트를 켤 때 별도로 판단한다.

기존 픽스처가 filing당 컨텍스트 하나뿐이라 세 결함을 다 놓쳤다. 실제 모양(3개 비교연도 ×
CFS/OFS)으로 바꾸고 회귀 테스트 8개를 추가했다 — 수정 전 코드에 돌리면 7개가 실패한다.

상세와 수정 방향: `08` §4.3.2.

## 4d. 새로 사전등록한 계약 (결과 보기 전에 고정)

재실행 전에 확정해야 했던 판정 기준 두 묶음을 `04_specific_plan_B.md`에 넣었다.

- **§4.4.1 vintage distance probe** — capital-change vintage를 latest로 쓸지 strict PIT로
  쓸지 정하는 지표 2종과 기준표. probe 스크립트가 판정까지 자동으로 한다.
- **§2.5 source 비치명 경고** — `mapping_fallback_ratio` 0.50 / `revision_ratio` 0.10 /
  `value_mismatch_ratio` 0.01. 넘거나 **측정 불가면 grade A만 막는다**(B 아래로는 안 내린다).
  임계값을 세운 직후 실측에서 `net_income` fallback이 0.655로 나왔는데 데이터가 아니라 정의
  문제였다(카탈로그 최선 priority가 CFS 전용이라 OFS 행이 구조적으로 못 맞춤). `(metric_code,
  fs_basis)`별로 고쳐 0.32가 됐다. **동결 전에 재 본 덕에 잡았다.**

## 5. 다음에 할 일 — 이 순서로

### 1. [크리티컬 패스] 수집 → probe 판정 → 재실행

앞 단계가 끝나야 다음이 된다. 명령과 주의점은 `08` §4.1·§4.1.1·§4.1.2.

1. ~~Phase B 코드 커밋 → 릴리즈(v0.9.2)~~ 완료
2. ~~B-2 결함 수정(§4c)~~ 완료
3. ~~prod 1차 백필~~ 완료 (filings 2015~2026, capital vintage 2016·2020·2024)
4. ~~raw export~~ 완료 (`snapshot_date=2026-08-12` / `source=sj2_remote`, `--route remote`).
   exporter 설정에 두 테이블이 빠져 있어 먼저 등록해야 했다 → `08` §4.1.2
5. ~~vintage probe 판정~~ 완료 — **strict PIT 채택**. `DEFAULT_VINTAGE_POLICY` 고정.
   측정값은 `08` §4.1.2, 산출물은 `research/output/vintage_probe/`
6. ~~잔여 vintage 백필~~ 완료 (2026-08-12 21:45 `ALL DONE`). 11011 vintage가 2015~2025 전부
   찼다 — ticker 1,726(2015) → 2,647(2025)
7. ~~capital_change만 재export~~ 완료. 71,535 → 245,120행
   ```bash
   uv run krx-collector db with-remote-dsn -- bin/raw-parquet-export-all.sh \
     --snapshot-date 2026-08-12 --route remote --force-table dart_capital_change_raw
   ```
8. ~~`--phase B` 실행~~ 완료 — `run_id=20260812T231507-f9117ce1`, **38 ready / 0 blocked**.
   측정값과 해석은 `08` §3.0
9. **`--phase A` 재실행** — 같은 snapshot이어야 결합 BH가 성립한다(§2.3 rule 5). 아직 안 돌렸다
   ```bash
   uv run python -m research.analysis.horizon_scan --phase A --source sj2_remote
   ```
10. **`--phase AB` 실행** — `q_fdr_global_ab`·`screen_pass`·`evidence_grade`가 처음으로
    진짜 값을 갖는다. **지금 family 등급이 전부 NE인 건 이 단계를 안 돌렸기 때문이다**

Phase B는 약 5시간 30분 걸렸다. Phase A도 비슷한 규모로 보고 일정을 잡는다. 두 개를 동시에
돌리는 건 권하지 않는다 — 출력 경로와 마트 쓰기는 겹치지 않고 DuckDB spill도 실측상 충돌하지
않지만(1.5.4는 인스턴스별로 분리한다), 인스턴스마다 `threads=14`·`memory_limit=28.7GiB`를
기본으로 잡아 14코어/36GB 장비에서 서로를 느리게 만든다.

**순서 주의 두 가지.**

- probe와 export: probe는 lake의 parquet을 읽으므로 **export가 먼저**다.
- **A0 feature mart가 Phase A/B의 선행 조건이다.** `08` §5의 재현 명령에는 이 단계가 빠져
  있었다. raw export 뒤에 아래를 돌려야 `--phase B`가 `A0 manifest is required` 에러로
  죽지 않는다.
  ```bash
  # snapshot_date를 주지 않는다 — auto_selected=False가 되면 official 자격을 잃는다
  uv run python -m research.etl.horizon_scan_inputs --source sj2_remote
  ```
  **`compute_all --features`로 대신하면 안 된다.** 두 경로가 같은 `feature_mart/` 아래
  같은 이름의 마트를 만들지만 계약 해시가 달라서, `compute_all`이 먼저 만들어두면 A0가
  `mart cache contract mismatch for 'dim_stock_pit_daily'`로 죽는다(그 경우 `--force`로
  다시 만든다). `_manifests/_SUCCESS.json`과 official/smoke_only 판정을 쓰는 쪽은 A0
  진입점뿐이다. `compute_all`은 `stock_metric_fact`/`common_feature_daily_fact`
  (derived_mart) 담당이고, 이건 A0의 선행이므로 **먼저** 돌려야 한다.

  그 `compute_all`을 `--features` 없이 freshness부터 돌리면 **common feature readiness
  게이트에서 멈춘다**(37개 중 4개만 ready). 이건 오늘 생긴 게 아니다 —
  `snapshot_date=2026-08-09`에서 같은 값이 나온다. 원인은 데이터 사고가 아니라 창 정렬이다.
  macro 계열은 2013-06-20부터, 일별 시장·해외·환율 계열은 2014-06-16부터 시작하는데
  coverage 창이 가장 이른 날짜에 맞춰져 있어 뒤에 시작한 계열이 앞쪽 약 257 거래일을 비운
  것으로 잡힌다(`missing_count=257`과 일치). horizon scan은
  `feat_price`/`feat_flow`/`label_scan`(daily_ohlcv·flow 기반)만 읽으므로 이 게이트와
  무관하다. derived mart 자체는 게이트 전에 이미 만들어진다.

### 2. 데이터 없이도 가능한 코드 작업

~~B-10 Stage 2·4·5~~ 완료. 남은 건 **Stage 3**(`daily_ic`/`cohort_ic`)뿐이다.
`scan_cell`/`scan_event_cohort_cell`이 요약 통계만 반환하고 날짜별 원시 IC 시퀀스를 버리므로,
Phase A와 공유하는 `per_date_market_rank_ic` 내부를 고쳐야 한다. **발행된 Phase A run의 재현
경로에 걸리므로 별도 계획이 필요하다** — 급하게 손대지 않는다. → `08` §4.3.

§5.5 segment/freshness 진단(8개 축)은 여전히 스코프 밖이다. 값이 생기면
`horizon_scan_phase_b_source_quality.py`의 같은 자리에 붙이면 된다.

### 3. 수집이 끝난 뒤에 확정되는 것

- **정기보고서 정정 최종 집계** — 2022~2025는 5,041건으로 측정됐고 2021 이전이 더해진다.
  이 값이 B-1 6항 receipt-targeted XBRL 백필의 규모다 → `08` §4.3 Stage 2
- **`vintage_lookahead_ratio`** — probe가 latest_vintage를 채택한 경우에만 evidence grade에
  추가한다

### 4. T1 잔여

**h=60 holdout 재평가**만 남았다. 2026년 10~11월 이후, 새 구간으로 한 번만. → `07` §6

## 6. 상태 확인 명령

```bash
uv run pytest tests/unit -q                                   # 939개 통과가 기준선(2026-08-12)
uv run ruff check src/ tests/                                 # research/의 fin_vs_price_corr는 기존 미해결

# T2가 아직 막혀 있는지 — 아무것도 안 나오면 여전히 막힌 상태다
ls data_lake/raw_postgres/snapshot_date=*/source=sj2_remote/ | grep -E "filing_receipt|capital_change"

find research/output/horizon_scan -name manifest.json         # 발행된 run 목록
find research/output/horizon_scan -name "03b_*"               # Phase B 리포트(있으면 재실행이 된 것)
```
