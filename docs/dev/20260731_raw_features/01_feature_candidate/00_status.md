# 00. 진행 상태 — 이 디렉터리를 이어받는 사람이 먼저 읽는 문서

- 작성일: 2026-08-11 (갱신: 2026-08-12 12:00 KST)
- 브랜치: `refactor/parquet-compute-reproducible`
- 목적: 문서가 8개(합계 30만 자 이상)라 어디까지 왔는지 한눈에 안 보인다. 이 파일은
  **지금 상태와 다음에 할 일만** 적는다. 근거·설계는 각 문서로 넘긴다.

## 0. 새 세션이면 여기부터

지금 크리티컬 패스는 **코드가 아니라 prod 수집 대기**다. 2026-08-12 12:00 기준으로 백그라운드
작업 두 개가 sj2-server에서 돌고 있었다. 먼저 그게 끝났는지 확인한다.

```bash
# 1) 아직 돌고 있나. PID(2805134 filings / 2836192 capital)는 재사용될 수 있으니
#    스크립트 이름으로 본다. 대괄호는 pgrep이 자기 명령줄을 잡는 걸 막는다.
ssh whi@sj2-server "pgrep -af '[p]hase_b_backfill' || echo 'no backfill running'"
ssh whi@sj2-server 'tail -n 2 /home/whi/phase_b_backfill.log /home/whi/phase_b_capital_probe.log'

# 2) 로그보다 DB가 정본이다. filings는 접수 달력연도 2015~2026이 다 있어야 한다.
.agents/skills/sdc-db/scripts/dbq.sh sj2 -c \
  "select extract(year from rcept_dt) y, count(*), count(distinct corp_code)
   from dart_filing_receipt_raw group by 1 order by 1;"

# 3) capital probe는 연간보고서(11011) 2016/2020/2024 세 vintage만 받는다.
.agents/skills/sdc-db/scripts/dbq.sh sj2 -c \
  "select bsns_year, reprt_code, count(*), count(distinct ticker)
   from dart_capital_change_raw group by 1,2 order by 1,2;"
```

**연도가 다 찼는지는 receipt 수가 아니라 법인 수로 본다.** 2016~2025는 법인 2,056~2,653개가
정상 범위다. 그보다 훨씬 적으면(예: 2015가 837) 아직 수집 중이거나 중간에 끊긴 것이다.
2015는 상장사 수 자체가 적어 다른 해보다 낮게 끝날 수 있으니, 최종 판단은 로그의 완료 문구로
한다.

로그 마지막 줄이 `ALL DONE`이면 정상 종료, `QUOTA EXHAUSTED`면 OpenDART 일일 한도로 끊긴
것이다. 후자면 같은 스크립트를 다시 실행하면 이어받는다 — 저장된 raw는 skip된다.

두 작업이 끝났으면 **§5의 1번 순서를 그대로 따라간다.** 아직 돌고 있으면 §5의 2번(코드
작업)에서 고른다 — 남은 건 B-10 Stage 3 하나뿐이고 별도 계획이 필요하다.

주의: 두 작업 모두 `sdc_with_source_lock opendart`를 잡는다. 데일리 OpenDART 체인은 04:00에
lock 없이 시작하므로, 백필이 다음 날 04:00을 넘겼다면 데일리 이벤트가 겹쳤을 수 있다.
`ingestion_runs`에서 그날 OpenDART run의 status를 먼저 본다.

## 1. 트랙이 두 개다

| 트랙 | 대상 | 상태 |
|---|---|---|
| **T1. px/flow 피쳐 검증** | 가격·수급 피쳐 17 family (Phase A0/A → acceptance gate) | **판정까지 완료**. 20일 모델 채택은 보류(§3) |
| **T2. 재무/이벤트 피쳐 검증** | fin_*/ev_* 8 family, 38 candidate cell (Phase B) | **코드 완료·커밋됨, 실행 0%** — prod 수집만 기다린다 |

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

## 4. T2 — 코드는 다 됐고, 데이터만 없다

작업 패키지 B-0~B-9 완료(§5.5 segment 진단 제외), B-10은 Stage 1·2·4·5 완료로 **Stage 3만**
남았다. 유닛 테스트 939개 통과(2026-08-12).

Phase B candidate **38개가 전부 `blocked_missing_dependency`**(`M_B_ready=0`)다. 로컬 lake에
`dart_filing_receipt_raw`/`dart_capital_change_raw` parquet가 없기 때문이고, 버그가 아니라
outcome-blind readiness 설계가 의도한 동결 상태다.

그래서 B-PR11~B-PR15의 게이트·진단 오케스트레이션은 **아직 한 번도 실제 데이터로 실행된 적이
없다**(mock/synthetic 테스트로만 검증). 발행된 phase=AB run(`20260810T194651-e04c00c7`)도
Phase B 셀이 0개라 껍데기다. 첫 실제 실행에서 통합 단계 버그가 나올 수 있다고 보는 게 맞다.

상세: `08` §3(발행된 run·블로커 체인), §4(남은 작업).

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

앞 단계가 끝나야 다음이 된다. 명령과 주의점은 `08` §4.1·§4.1.1.

1. ~~Phase B 코드 커밋 → 릴리즈(v0.9.2)~~ 완료
2. ~~B-2 결함 수정(§4c)~~ 완료
3. **prod 백필** — §0에서 상태 확인. 끝났으면 4번으로
4. **vintage probe 판정** — 수집 완료 + raw export 후 아래 한 줄. 기준표 판정까지 스크립트가
   해서 `research/output/vintage_probe/`에 md·json으로 남긴다
   ```bash
   uv run python -m research.analysis.capital_change_vintage_probe --snapshot-date YYYY-MM-DD
   ```
   판정 결과를 `08` §4.1.1에 기록하고, `event_scan.build_issuance_sql`의 `vintage_policy`
   기본값을 그 결과로 고정한다. dedup 규칙 자체(§4.4.1 1~3항)는 이미 구현돼 있다
5. **raw export** — `bin/raw-parquet-export-all.sh` → 새 `snapshot_date`
6. **`--phase B` 재실행** — 여기서 처음으로 `M_B_ready > 0`이 된다
7. **`--phase A` 재실행** — 같은 snapshot이어야 결합 BH가 성립한다(§2.3 rule 5)
8. **`--phase AB` 재실행** — `q_fdr_global_ab`·`screen_pass`·`evidence_grade`가 처음으로
   진짜 값을 갖는다

4번과 5번의 순서에 주의: probe는 lake의 parquet을 읽으므로 **export가 먼저**다.

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
