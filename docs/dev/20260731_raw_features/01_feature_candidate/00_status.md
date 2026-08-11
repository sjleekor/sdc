# 00. 진행 상태 — 이 디렉터리를 이어받는 사람이 먼저 읽는 문서

- 작성일: 2026-08-11
- 브랜치: `refactor/parquet-compute-reproducible`
- 목적: 문서가 8개(합계 30만 자 이상)라 어디까지 왔는지 한눈에 안 보인다. 이 파일은
  **지금 상태와 다음에 할 일만** 적는다. 근거·설계는 각 문서로 넘긴다.

## 1. 트랙이 두 개다

| 트랙 | 대상 | 상태 |
|---|---|---|
| **T1. px/flow 피쳐 검증** | 가격·수급 피쳐 17 family (Phase A0/A → acceptance gate) | **판정까지 완료**, 커밋됨(`a03872e`) |
| **T2. 재무/이벤트 피쳐 검증** | fin_*/ev_* 8 family, 38 candidate cell (Phase B) | **코드 거의 완료, 실행 0%** — 수집이 안 돼 막힘. 전부 uncommitted |

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

## 3. T1 — 끝났다 (조건부 채택)

Grade A 6개 중 `px_amihud_20d`/`px_near_52w_high`는 baseline 모델에 이미 있던 피쳐라 제외,
나머지 4개 `px_reversal_5d` · `px_maxret_20d` · `px_idio_vol_60d` ·
`flow_individual_netbuy_to_volume_{5,20}d`를 **조건부 채택**으로 판정했다.

- 증분성(기준 ⑥): 충족 — walk-forward 11년 5-fold에서 3개 horizon 모두 개선(20일 Δ+0.0081).
- 경제성(기준 ⑤): 엇갈림 — turnover 차감 top-decile spread는 오히려 낮다.
- holdout(기준 ⑧): 불확정 — 평가일 16일, 리밸런싱 1회로 표본이 너무 작다.

남은 두 가지(→ `07_phase1_acceptance_gate.md` §6, `08` §4.4):

1. **k=100 실매수 리스트 기준 비용 확인** — 지금 바로 할 수 있다. 아직 안 했다.
2. **h=60 holdout 재평가** — 2026년 10~11월 이후 새 구간으로 한 번만. 데이터 대기.

## 4. T2 — 코드는 됐는데 데이터가 없다

작업 패키지 B-0~B-9 완료(§5.5 segment 진단 제외), B-10은 Stage 1만 완료. 유닛 테스트
819개 통과(2026-08-11 재확인).

그런데 Phase B candidate **38개가 전부 `blocked_missing_dependency`**(`M_B_ready=0`)다.
원인은 하나다 — 로컬 lake에 `dart_filing_receipt_raw`/`dart_capital_change_raw` parquet가
없고, 그 뿌리는 **Phase B 코드가 커밋·릴리즈되지 않아 prod가 아직 수집을 못 한다**는 것이다.
버그가 아니라 outcome-blind readiness 설계가 의도한 동결 상태다.

그 결과 B-PR11~B-PR15가 만든 게이트·진단 오케스트레이션은 **아직 한 번도 실제 데이터로
실행된 적이 없다**(mock/synthetic 테스트로만 검증). 발행된 phase=AB run
(`20260810T194651-e04c00c7`)도 Phase B 셀이 0개라 사실상 껍데기다.

상세: `08_phase_b_implementation_log.md` §3(발행된 run 목록·블로커 체인), §4(남은 작업).

## 4b. T2 진행 (2026-08-12 갱신)

커밋·릴리즈는 끝났다(v0.9.2). prod 백필이 돌고 있다 — filings 11개 연도가 13시 전후 완료,
이어서 capital vintage probe가 자동으로 시작한다. 상세는 `08` §4.1.1.

백필 중에 새 이슈가 하나 확정됐다. `irdsSttus`가 누적 이력을 보고서마다 다시 주는데 마트가
그걸 중복 합산해 `ev_net_share_issuance_yoy`가 전부 NULL이 된다. dedup 규칙과, latest-vintage
대 strict PIT 중 무엇을 쓸지 정하는 측정 설계·판정 기준을 plan §4.4.1에 사전 고정했다.

## 5. 다음에 할 일 — 이 순서로

1. **[크리티컬 패스]** ~~Phase B 코드 커밋 → 릴리즈~~ 완료 → prod 백필(진행 중) →
   vintage probe 판정(`04_specific_plan_B.md` §4.4.1) → `raw-parquet-export-all.sh` →
   `--phase B` → `--phase A` → `--phase AB` 재실행. 명령과 주의점은 `08` §4.1·§4.1.1.
2. **데이터 없이도 가능한 코드 작업**: B-10 Stage 2(품질·커버리지 진단 parquet 7종),
   Stage 4(family 카드). Stage 3은 Phase A 공유 코드 내부를 건드려야 해 별도 계획이 필요하다.
   → `08` §4.3.
3. **T1 잔여**: k=100 비용 확인(지금 가능) → `07` §6.

## 6. 상태 확인 명령

```bash
uv run pytest tests/unit -q                                   # 819개 통과가 기준선
git status --porcelain | wc -l                                # T2 코드가 아직 uncommitted인지
ls data_lake/raw_postgres/snapshot_date=*/source=sj2_remote/ | grep -E "filing_receipt|capital_change"
                                                              # 아무것도 안 나오면 T2는 여전히 막힌 상태
find research/output/horizon_scan -name manifest.json         # 발행된 run 목록
```
