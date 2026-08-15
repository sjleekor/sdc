# 06. Grade A 후보 6개 — 종목 단위 상세 조사

- 작성일: 2026-08-08
- 대상: Phase A official run(`run_id=20260803T063659-93effdb0`)에서 `evidence_grade=A`로
  통과한 6개 family
- 선행 문서: [02_feature_candidate.md](../02_feature_candidate.md) (정의),
  [04_specific_plan_A.md](../04_specific_plan_A.md) (실행 계획),
  [05_phase_a_results_explained.md](../05_phase_a_results_explained.md) (결과 해설)

## 0. 이 디렉터리의 목적

05번 문서는 Phase A 결과를 "숫자가 무슨 뜻인지" 수준으로 풀어 썼다. 이 디렉터리는 한
단계 더 들어가 **family별로 실제로 어떤 종목이 뽑히는지, 그 종목들이 실제로 어떻게
움직였는지**를 원본 parquet에서 직접 조회해 정리한 것이다. family당 파일 하나.

01~06 각 파일은 맨 앞에 **`## 0. 쉬운 설명 (먼저 읽기)`** 절을 두고 있다. 그
family가 무엇을 재는지, 그 뒤 표를 어떤 순서로 읽어야 하는지, 어디가 함정인지를
통계 배경 없이 읽을 수 있게 정리한 부분이다. 처음 보는 family라면 여기부터 읽고
본문으로 넘어가면 된다.

| 파일 | family | 등급 근거 요약 |
|---|---|---|
| [01_px_reversal_5d.md](01_px_reversal_5d.md) | 단기 반전 | IC 0.048, q≈6.5e-98, 5/5 구간 일치 — 6개 중 가장 견고 |
| [02_flow_individual_netbuy_to_volume.md](02_flow_individual_netbuy_to_volume.md) | 개인 순매수 | IC 0.012, q≈1.1e-12, 부호 사전 미고정 → 사후 + 확정 |
| [03_px_near_52w_high.md](03_px_near_52w_high.md) | 52주 신고가 근접도 | IC 0.011, **누적 60일 셀은 BH 탈락(q=0.46)** — bucket 20-40/40-60만 생존, 장기 placebo 실패 |
| [04_px_maxret_20d.md](04_px_maxret_20d.md) | MAX (복권형 과열) | IC -0.088, q≈3.9e-39, temporal placebo 100/100 통과 |
| [05_px_idio_vol_60d.md](05_px_idio_vol_60d.md) | 특이변동성(idio-vol) | IC -0.108, q≈2.8e-42, temporal placebo 100/100 통과 |
| [06_px_amihud_20d.md](06_px_amihud_20d.md) | Amihud 비유동성 | IC 0.108(60d)/0.133(120d), q≈8.3e-14, 유일하게 60~120일 구간까지 생존 |
| [07_glossary.md](07_glossary.md) | (전체 공통) | 위 파일들에 나온 통계 용어·컬럼명(IC, ICIR, q_fdr_global, decile 등) 해설 |

### 0.1 나중에 추가된 T2(재무·이벤트) 4개 — 다른 run이다

01~06은 Phase A official run(`20260803T063659`, snapshot `2026-08-01`)만 다룬다.
그 뒤 Phase B가 돌면서 A/B 등급을 받은 재무·이벤트 family 4개가 더 나왔고,
`../09_all_feature_results.md`가 25개 family를 한 표에 모았다. 그 4개의 상세
조사가 아래다.

| 파일 | family | 등급 | 등급 근거 요약 |
|---|---|:---:|---|
| [08_fin_log_mcap.md](08_fin_log_mcap.md) | 규모(로그 시총) | **A** | IC -0.1115, 4개 셀 전부 A — **25개 중 요구 게이트 3개를 다 통과한 유일한 family** |
| [09_ev_net_share_issuance_yoy.md](09_ev_net_share_issuance_yoy.md) | 순주식발행 | **A** | IC -0.0221, 4개 셀 중 1개만 A. 값의 88.4%가 0 |
| [10_ev_payout_yield.md](10_ev_payout_yield.md) | 주주환원 수익률 | **B** | IC +0.0975(cum 120일)로 크지만 정정 비율 0.1146이 A를 막음. 장기 셀은 placebo 실패 |
| [11_fin_value_z.md](11_fin_value_z.md) | 밸류 종합 z | **B** | IC +0.1384(cum 120일)로 T2 2위. 표본은 2019-10부터 5년뿐 |

**이 4개는 다른 실행의 숫자다.** Phase B `20260815T133014-8f47b5fc` + 결합 AB
`20260815T190659-8f47b5fc`(snapshot `2026-08-12`, config `e55c3046…`)를 인용한다.
**2026-08-15 재실행분**이며, 그 전 라운드에서 발견된 결함 수정이 반영돼 있다
(`../10_known_issues.md`).
config·표본 구간·BH 모집단(75개 → 113개)이 모두 달라 **01~06의 숫자와 직접
비교하면 안 된다.** holdout 경계(2025-08-01)만 동일하다.

## 1. 데이터 소스 — 두 종류를 구분해서 인용

### 1.1 공식 통계 (재계산하지 않고 원본 인용)

Phase A official run의 산출물을 그대로 읽었다. 이 디렉터리의 IC·ICIR·q값·subperiod·
temporal placebo 수치는 **모두 아래 두 파일에서 그대로 가져온 값**이며, 새로 계산한
것이 아니다.

```text
research/output/horizon_scan/phase=A/snapshot_date=2026-08-01/source=sj2_remote/
  config_hash=1d2082584ceb1d2ec376bff601f79c1e4381c2b32f767ee1fd1e1073324dad6d/
  run_id=20260803T063659-93effdb0/
    cards/family_cards.json        # family별 요약 카드 (17개)
    core/horizon_ic.parquet        # 셀 단위(family×scan_type×horizon×universe×sample_kind) 원본 통계
    plots/{family}_*.png           # family별 7종 그래프(cumulative_ic, bucket_ic, coverage,
                                    #   native_vs_lag1, offset_distribution, segment_dot,
                                    #   subperiod_heatmap)
```

`q_fdr_global`/`bh_pass`는 `core/horizon_ic.parquet`에서 `sample_kind='common_survivor'
AND universe='broad'`인 행(75개 사전등록 primary 셀)에만 채워져 있다 — 이것이 실제
global BH-FDR 검정에 들어간 모집단이다. family_cards.json의 최상위 `q_fdr_global`
필드는 그 family의 `peak_h_cum` 셀 값 하나를 대표로 뽑아놓은 것이라, **family
카드의 대표값만 보면 그 family의 다른 셀이 BH를 통과했는지 못 했는지가 가려진다**
(03_px_near_52w_high.md가 정확히 이 문제를 겪는 사례다 — 개별 파일에서 상세히 다룬다).

### 1.2 종목 단위 사례 (이번에 새로 조회 — 공식 run의 일부가 아님)

공식 run은 family가 "예측력이 있는가"만 통계로 답한다. "어떤 종목이 뽑히는가, 그
종목이 실제로 올랐는가"는 이번에 직접 아래 마트를 DuckDB로 조회해 만들었다.

```text
data_lake/feature_mart/snapshot_date=2026-08-01/source=sj2_remote/
  feat_price/               # px_* 피쳐 (Phase A0가 만든 것)
  feat_flow/                # flow_* 피쳐
  label_scan/               # fwd_ret_*d, raw_label_*d(초과수익), label_ok_*d
  dim_universe_tradable_daily/  # 거래가능 universe 필터
data_lake/raw_postgres/snapshot_date=2026-08-01/source=sj2_remote/stock_master/
  # ticker -> 종목명
```

방법:

1. `dim_universe_tradable_daily.in_universe=true`인 날짜만 사용(공식 run의 tradable
   universe와 동일 정의 — 유동성·가격·정지·관리종목 필터 적용).
2. **decile 검증표**: family의 peak horizon(예: reversal=3일, amihud=120일)에서
   `label_ok_{h}d=true`인 전체 표본을 그날짜 내에서 feature 값으로 10분위(ntile)로
   나누고, 분위별 평균 raw 수익률·평균 초과수익률·상승 비율을 집계했다. 이건 공식
   Spearman IC와 다른 계산이지만(단순 10분위 평균 vs rank correlation, 날짜×시장 결합
   vs 날짜 단독), **방향성이 같은지 눈으로 확인하는 용도**로는 충분하다.
3. **종목 리스트**: family마다 그 family의 `label_ok_{h}d`가 참인 **가장 최근 날짜**
   하나를 골라, 그날 feature 값 기준 상위/하위 15개 종목을 종목명과 함께 나열하고
   실제 forward 수익률(raw·초과)을 붙였다.

두 계산 모두 날짜×시장 조건부 rank가 아니라 **날짜 전체(KOSPI+KOSDAQ 합산) 단순
cross-section**이라, 공식 IC보다 노이즈가 크다. 종목 리스트 하나만 보고 판단하지
말 것 — 개별 종목의 등락은 6개월~1년 뒤까지 온갖 종목 고유 이벤트(실적, 유상증자,
테마 등)가 섞여 들어간다. decile 표(수천~수십만 관측치 평균)가 더 신뢰할 수 있는
근거다.

## 2. Holdout 경계 — 왜 종목 사례가 전부 "1년 전" 날짜인가

`label_scan`을 조회하면 `label_ok_1d`조차 `trade_date <= 2025-07-30`에서만 참이고,
그 뒤로는 전부 거짓이다. 실제 데이터는 `feat_price`/`feat_flow` 모두
`2026-07-31`까지 있는데도 그렇다. 확인해보면 경계가 정확히 다음과 같다.

```text
label_end_date_{h}d <= 2025-07-31  →  label_ok_{h}d = true
label_end_date_{h}d >  2025-07-31  →  label_ok_{h}d = false
```

즉 **2025-08-01 ~ 2026-07-31 구간은 forward label이 의도적으로 봉인된 holdout**이다.
`04_specific_plan_A.md` §11이 명시한 원칙 — "holdout은 feature·horizon·변형 선택이
끝난 뒤 한 번만 연다" — 이 실제로 데이터 레벨에서 지켜지고 있다는 뜻이다. 이 조사는
그 원칙을 그대로 따라 **holdout 이전(≤2025-07-31 종료 기준) 데이터만 사용**했다.
holdout 구간을 미리 들여다보면 이후 실제 acceptance gate 평가가 오염되므로, 최근
1년 데이터로 "진짜 지금도 통하는지" 확인하고 싶더라도 지금 단계에서는 열어보지
않는 것이 맞다.

family별로 사용한 날짜(= 해당 family의 peak horizon 기준 마지막 유효 formation
date)는 다음과 같다.

| family | peak horizon | 사용한 formation date | label 종료일 |
|---|---|---|---|
| px_reversal_5d | 3일(cum) | 2025-07-28 | 2025-07-31 |
| flow_individual_netbuy_to_volume_20d | 20일(cum) | 2025-07-03 | 2025-07-31 |
| px_near_52w_high | 60일(cum) | 2025-05-02 | 2025-07-31 |
| px_maxret_20d | 60일(cum) | 2025-05-02 | 2025-07-31 |
| px_idio_vol_60d | 60일(cum) | 2025-05-02 | 2025-07-31 |
| px_amihud_20d | 120일(cum) | 2025-02-05 | 2025-07-31 |

## 3. 종합 결론 (미리보기)

- **가장 견고한 신호**: `px_reversal_5d` — 결정력(IC)은 크지 않지만 5/5 구간 일치,
  하루 지연 후에도 유지, q값이 압도적으로 작다. 종목 사례에서도 decile 1→10 초과수익이
  단조 증가한다.
- **가장 극적인 스프레드**: `px_amihud_20d` — 120일 decile 스프레드가 raw
  약 9.4%p, 초과수익 기준 약 8.9%p(decile10 +4.9% vs decile1 -4.1%)에 달한다.
  다만 최근 표본(2025-02~07)에 코스닥/중소형이 강하게 반등한 국면이 섞여 있어
  "초과수익"의 상당 부분이 삼성전자·SK하이닉스 같은 초대형주가 동일가중 지수보다
  덜 오른 데서 나온다는 점을 감안해야 한다.
- **가장 약한 신호**: `px_near_52w_high` — family 카드는 A등급이지만, 실제로는
  6개 사전등록 셀 중 4개(누적 20/40/60일, bucket 10-20일)가 개별로는 BH를 통과하지
  못했고, 유일하게 살아남은 두 bucket 셀(20-40일, 40-60일) 중 장기 셀은 temporal
  placebo(날짜 뒤섞기 100회 재검증)를 통과하지 못했다(p=0.91). "A등급"이라는 표시만
  보고 신뢰도를 동일하게 취급하면 안 된다.
- 6개 모두 **아직 acceptance gate(증분성·거래비용·holdout 최종평가)를 통과한 것은
  아니다** — 이 조사는 Phase A 스크리닝 결과를 더 구체적으로 검증한 것일 뿐,
  실거래 채택 여부에 대한 결론이 아니다.
- **후속**: 이 acceptance gate를 실제로 실행한 결과는
  [../07_phase1_acceptance_gate.md](../07_phase1_acceptance_gate.md) 참고 —
  `px_amihud_20d`/`px_near_52w_high`는 이미 baseline 모델에 있던 피쳐라 제외하고,
  나머지 4개(`px_reversal_5d`/`px_maxret_20d`/`px_idio_vol_60d`/
  `flow_individual_netbuy_to_volume`)만 조건부 채택으로 판정했다.
