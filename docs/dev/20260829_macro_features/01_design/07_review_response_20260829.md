# 07. 리뷰 반영 기록 — `06_review_20260829.md`에 대한 답

- 작성일: 2026-08-29
- 원칙: 리뷰의 사실 주장은 코드·데이터로 다시 확인한 뒤 반영했다. 재확인 결과는 각 항목에 적었다.
- "반영"은 `00`~`04` 문서를 고쳤다는 뜻이다. 코드는 아직 바꾸지 않았다(사전등록 전).

---

## 1. 계약에 들어가는 항목 (M1~M7)

| # | 리뷰 지적 | 재확인 | 결정·반영 |
|---|---|---|---|
| **M1** | 환율·국고채 요인은 fact에서 한 세션 지연된 값이다 | **사실.** `fx_usdkrw_ecos`·`rate_kr_gov10y`·`market_kospi_krx` 모두 `availability_policy="next_krx_session"`. fact `fx_usdkrw_level` 2024-07-02 = 1382.4 = raw 07-01 관측 | **(b) 채택.** 국내 요인은 `(resid_ret_τ, g_τ = ln(fx_{τ+1}/fx_τ))`로 짝짓고 창을 `252 PRECEDING AND 1 PRECEDING`에서 끝낸다. 해외 요인(VIX·WTI·S&P)은 NY t−1 → KRX t의 spillover 짝 그대로. 매매기준율 산출 방식은 미검증 한계로 명시하고 짝을 더 옮기지 않는다 → `02` §2.2~2.3, `04` notes·§5 |
| **M2** | 베타 시작일 2015-12/2015-06은 틀렸다 | **사실.** `feat_price` 2007-06-05부터, `px_idio_vol_60d` 2008-12-16부터, `px_resid_mom_12_1` 2009-06-22부터 | 반영. primary·rawbeta ≈ 2014-12, `px_market_beta`는 panel 시작. `2014_2016` 구간은 약 2년치 → `02` §2.6, `04` notes |
| **M3** | fact의 `feature_date`가 2014~2023년에 KRX 세션이 아니다 | **사실.** 비세션 평일: 2015년 13, 2019년 15, 2023년 15, 2024년 1, 2025년 3 | **세션 격자 채택.** 국면은 `label_scan`의 `trade_date`에 fact를 join한 뒤 세션 index 위에서 LAG·창을 잡는다. fact는 다시 만들지 않는다(`feat_common`·상관표 재현값 보존). 1a 마트는 panel join이라 영향 없음을 명시 → `03` §2.1, `02` §2.1, `04` `grid: krx_sessions`, `00_survey/00` §4.2 각주 |
| **M4** | `f_sp500_lag`에 asof 미갱신 NULL 규칙이 빠졌다 | 사실(리뷰 실측 115세션) | 반영. NULL 규칙을 여섯 요인 전부에 적용 → `02` §2.2 |
| **M5** | G5(lag1 유지)는 daily_ic로 계산할 수 없다 | **사실.** lag1은 `horizon_scan.py:216`·`:423`에서 `scan_cell` 직접 호출, family 대표 cell 하나 | **G5 제거.** 등급 A 조건을 "유효 구간 ≥ 4 ∧ 대안 cut 부호 일치"로 바꿈. 단계 0 범위는 넓히지 않음(sink는 `run_registry_scan`에만) → `03` §5, `01` §3.1 |
| **M6** | P3의 근거(Nagel 2012)는 VIX 수준인데 국면은 20세션 변화 | 문헌 요약(`00_survey/01`)과 대조해 사실 | **국면 추가.** `vix_high = VIX_t − median_252(VIX)` (R1b, primary)를 두고 P3를 거기에 건다. P1·P2는 `vix_up`(Kim-Park-Ok의 ΔVIX) 유지. 교차 확인은 exploratory → `03` §2.3·§3.2, `04` regimes·pairs·extra |
| **M7** | `registered_at` placeholder와 `source_daily_ic`가 hash에 들어간다 | 사실 | `registered_at`은 커밋 직전 실제 날짜로 확정(placeholder 상태로 hash 금지). **`source_daily_ic`는 config에서 제거**하고 Phase C CLI 인자 `--phase-a-run-id`/`--phase-b-run-id`로 받아 run_spec에 sha와 함께 기록. hash 제외 규칙은 만들지 않는다 → `03` §7.1, `04` §1·§5 |

재검증: 수정한 YAML을 `load_config`로 다시 로드했다. family 41(A 17, B 24), primary A 75, Phase B cell 102, 국면 7(primary 4),
쌍 primary 15·reference 2, 모든 쌍·exploratory extra의 family·cell·regime 참조 정상, `registered_at`이 date로 파싱, `source_daily_ic` 없음.

---

## 2. 계약 밖 정확성 항목 (리뷰 §8)

| 문서 | 지적 | 반영 |
|---|---|---|
| `00` §1.1 | 평균 IC 공식에 코딩 조건 누락 | 반영. `{0,1}` 코딩은 한쪽 조건부 평균, `{−1,+1}` 코딩은 `(P₁−P₀)E[IC] + 2P₁P₀δ` — 어느 쪽도 δ가 아님을 명시. 결정 1 유지 |
| `00` §1.1 | 연속 국면 가중은 rank IC가 크기를 버린다 | 반영. 한 문단 추가. 크기 가중은 Phase C 연속 회귀 진단(`03` §6.4)으로만 |
| `00` §1.1 | interaction family는 temporal placebo에서 불리 — 결정 1의 근거 | 반영 |
| `01` §1 표 | lag1 직접 호출자 누락 | 반영. 호출자 표를 리뷰 §4.1 그대로 |
| `01` §2.2 | checkpoint 재시작 문단은 존재하지 않는 경로 전제 | **사실**(`horizon_scan_checkpoint.py`는 복제 전용 함수만). 문단 삭제 |
| `01` §2.2 | `cell_identity`를 Phase A/B 스키마로 정규화 | 반영 |
| `01` §2.3 | `q5_spread`를 IC 행에 left join하면 동점일에 reconcile 실패 가능 | 반영. `daily_spread.parquet`로 **분리 저장** |
| `01` §2.3 | `hypothesis_role` 값 목록 | 반영 (`primary` / `exploratory_short_regime` / `ready_primary`) |
| `01` §2.4 | cell 수 2,000~2,700은 틀림 — 412 | **사실**(canonical A `horizon_ic.parquet` 412행, 103 가설). 크기 약 100만 행·수십 MB로 수정, 예산·`available` 제외 규칙 삭제 |
| `01` §3 | secondary·exploratory cell은 스캔되지 않는다 | 반영. §3.1 "저장되지 않는 것" 추가 |
| `01` §4.1 | `n_hac_pairs_min` 추가 | 반영 (`n_obs_min`·`n_obs_median`도 추가) |
| `01` §4.3 | parity 단서(Phase B·AB 3개 artifact 1e-12 초과 원인) | 반영 |
| `02` §1 | "시장 요인은 라벨에서 빠져 있다"는 부정확 | 반영. "시장 평균 중립이지 베타 중립은 아니다", `(β_i − 1)·r_m`이 남는다 |
| `02` §2.3 | 126의 근거를 `price.py` 규약이라 한 것은 오해 | 반영. "이 설계의 선택"으로. `00_survey/02` §4 규칙 4도 같이 수정 |
| `02` §2.5 | `_feature`는 위치 인자 | 반영 |
| `02` §4 | `macro_beta` vs `macro_rawbeta` IC 차이 진단은 secondary 스캔 경로가 필요 | 반영. `horizon_scan_phase_b_diagnostics.py`에 진단 전용 secondary 스캔 경로를 두기로(PR-1a-2) |
| `02` §3 | `register_derived_marts`는 fact를 재계산 — parquet 읽기와 선택 필요 | 반영. "마트가 읽은 fact와 readiness가 본 fact가 같아야 한다"를 조건으로, 선택은 PR-1a-2 |
| `03` §6.1 | `mapping_seed_sequence` "재사용"은 부정확 | 반영. "같은 방식의 새 키 `(contract, replicate, config_hash, pair_id, universe)`" |
| `03` §6 | 국면 전환 횟수·평균 지속 진단 컬럼 | 반영 (`n_regime_transitions`, `mean_run_length_*`) |
| `03` §6.2 | G2 유효 구간을 hash 전에 미리 계산 | 반영. 실행 순서 3a로 추가, `05_preregistration_record.md`에 기록 |
| `03` §8 | 쌍 간 종속성(cell·국면 공유) 해석 규칙 | 반영 |
| `04` §1 | `validate_config` 추가 검사에 regime 참조·extra family 검사 | 반영 (+ `registered_at`이 date인지) |
| `04` §3 | 실행 순서 `--from-step marts` 존재 확인, 격자 문제 상속 | 반영(격자는 세션 join으로 처리한다고 명시) |

---

## 3. 반영하지 않은 것과 이유

| 항목 | 이유 |
|---|---|
| M1 (a) "지연 베타를 그대로 두고 문서에 반영" | (b)가 가설("수출/내수 exposure")과 맞고 look-ahead 없이 구현 가능해 (b)를 택했다. (a)를 병행 secondary로 두는 것도 고려했지만 secondary가 이미 둘(rawbeta·세미베타)이라 늘리지 않았다 |
| M3 "CSV를 2014년까지 채우고 fact 재빌드" | fact를 다시 만들면 `feat_common` 소비자와 `00_survey/00` §4 재현값이 같이 움직인다. 세션 격자 계산으로 같은 효과를 얻는다. **휴장 CSV 보강 자체는 별도 과제**로 남긴다(다른 소비자에게도 이로우나 이 사전등록의 범위가 아니다) |
| M5 (b) "lag1 스캔에도 sink를 붙이고 쌍 cell로 확장" | 단계 0의 "행동 불변·`run_registry_scan`에만 sink" 원칙을 깨고 Phase A 스캔 범위를 넓힌다. lag1 유지율은 Phase B 게이트가 family 수준에서 이미 보고 있으므로 Phase C에서 뺀다 |
| `01` §4.3 "same snapshot·same code parity는 1e-12가 현실적" | 이미 그렇게 적혀 있었고 단서만 보강했다 |
| 리뷰 §10 "매매기준율 산출 방식 미검증" | 검증하지 않고 넘어간다. 짝을 더 옮기지 않는 것을 `04` §5 "결과를 보고 하지 않는 것"에 넣었다. 실측이 생기면 다음 사전등록에서 다룬다 |

---

## 4. 남은 확인 항목 (설계 밖, 구현 시)

- `register_derived_marts` 재계산 경로 vs snapshot parquet 읽기 — PR-1a-2에서 결정.
- G2 유효 구간 실제 값 — 실행 순서 3a에서 계산(리뷰 §10 미검증 항목).
- `newey_west_ols`의 HAC 구현 — PR-1b에서 상수 회귀 일치 테스트로 고정.
- `docs/holidays_krx.csv` 2014~2023 보강 — 별도 과제. 보강하면 fact 격자 문제가 원천에서 사라지지만 golden·재현값이 바뀐다.
