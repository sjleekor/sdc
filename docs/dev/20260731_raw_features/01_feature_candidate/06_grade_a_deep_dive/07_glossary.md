# 07. 용어·통계 지표 해설

- 작성일: 2026-08-09
- 대상: [00_overview.md](00_overview.md)~[06_px_amihud_20d.md](06_px_amihud_20d.md)에
  나온 통계 용어·컬럼명을 전부 모아 풀어썼다. 각 파일을 읽다가 낯선 이름이
  나오면 이 문서로 돌아와 찾아보는 용도.
- 05_phase_a_results_explained.md에도 짧은 용어표가 있지만, 이 문서는 06
  디렉터리에서 실제로 쓰인 컬럼명 전부(예: `t_nw`, `bh_pass`, `sample_kind`,
  `peak_bucket` 등)를 빠짐없이 다룬다는 점이 다르다.

## 0. 큰 그림 먼저

01~06 파일의 각 표는 사실 딱 두 종류의 계산에서 나온다.

1. **공식 통계** — Phase A가 2026-08-03에 실제로 돌린 결과. `family_cards.json`,
   `core/horizon_ic.parquet`에서 그대로 인용한 값. 여기 나오는 `ic_mean`,
   `t_nw`, `q_fdr_global`, `bh_pass` 같은 용어가 §1~§5 대상이다.
2. **종목 사례** — 이번에 새로 만든 것. "그 통계가 실제로 어떤 종목에서
   나오는지" 확인하려고 feature/label 마트를 직접 조회해 만든 decile 표와
   상위/하위 종목 리스트. `decile`, `avg_feat`, `avg_excess_ret` 같은 용어가
   §6 대상이다.

두 종류를 헷갈리지 않는 게 중요하다 — 공식 통계는 날짜×시장 조건부 rank
correlation(Spearman)이고, 종목 사례의 decile 표는 그보다 단순한 전체
cross-section 평균이라 **같은 현상을 다른 정밀도로 보는 것**이다
([00_overview.md](00_overview.md) §1.2 참고).

## 1. "예측력이 있는가"를 재는 핵심 통계

| 용어 | 뜻 | 읽는 법 |
|---|---|---|
| **IC** (정보계수, Information Coefficient) | 어떤 날 피쳐값이 높은 종목이 실제로도 이후 수익률이 높았는지를 종목 순위로 재는 상관계수(Spearman rank correlation). -1~+1 범위. | 0에 가까우면 무관, ±0.02~0.05면 실전에서 쓸만한 수준, ±0.1 이상이면 강한 편. `px_idio_vol_60d`의 -0.11이 6개 A등급 중 절댓값 최대([05](05_px_idio_vol_60d.md) §1). |
| **broad_ic / tradable_ic** | 같은 IC를 계산 범위만 다르게 잰 것. `broad`=거래정지·관리종목 등을 다 포함한 전체 종목, `tradable`=실제로 사고팔 수 있는 종목만. | 둘이 비슷하면 "이론상 신호가 실전에서도 살아있다"는 뜻. `tradable_ic`가 `broad_ic`보다 많이 작아지면 유동성 낮은 종목에서만 신호가 나온다는 경고. |
| **ICIR** (IC Information Ratio) | IC의 평균을 IC의 표준편차로 나눈 값 — "얼마나 크냐"가 아니라 "얼마나 안정적으로 그 방향이냐"를 잰다. 통계학의 t-통계량과 비슷한 개념. | 절댓값이 클수록 날짜마다 들쭐날쭐하지 않고 꾸준히 같은 방향. `px_idio_vol_60d`의 ICIR -1.54가 6개 중 최대. |
| **t_nw** | Newey-West 보정을 적용한 t-통계량. IC가 하루하루 독립이 아니라 며칠씩 겹쳐서(자기상관) 계산되는 걸 감안해 표준오차를 부풀려 보정한 것. | 그냥 t-통계량보다 항상 더 엄격하다(같은 데이터면 절댓값이 더 작게 나온다). ±2 이상이면 통상적 유의 수준. |
| **p_nw** | `t_nw`에 대응하는 p-value. "이 정도 IC가 우연히 나올 확률." | 작을수록 좋다. 단, 아래 `q_fdr_global`이 더 중요한 최종 판정 기준이다. |
| **q_fdr_global** | 여러 개(75개) 셀을 동시에 검사한다는 걸 감안해 p-value를 보정한 값(Benjamini-Hochberg FDR). | **0.1보다 작아야 "발견"으로 인정**한다([02_feature_candidate.md](../02_feature_candidate.md) §6.1). `px_near_52w_high`의 누적 60일 셀이 0.458로 이 기준을 통과하지 못한 것이 [03](03_px_near_52w_high.md)의 핵심 논지다. |
| **bh_pass** | `q_fdr_global < 임계값`인지를 참/거짓으로 미리 계산해둔 플래그(BH=Benjamini-Hochberg). | `false`면 그 셀은 "통계적으로 유의하다고 볼 수 없다"는 뜻 — family 전체가 A등급이라도 개별 셀은 `bh_pass=false`일 수 있다. |

## 2. 기간을 어떻게 나누는가

| 용어 | 뜻 | 읽는 법 |
|---|---|---|
| **horizon** | "며칠 뒤 수익률과 비교했는가." 1·2·3·5·10·20·40·60·120일 중 하나. | 짧을수록 "즉각 반응", 길수록 "천천히 누적되는 효과"를 본다. |
| **scan_type: cum** | 누적(cumulative) — 0일부터 h일까지 다 합친 수익률. `cum 0-60`은 "지금부터 60거래일 뒤까지의 수익률." | 구간이 길수록 앞쪽(예: 1~10일)의 효과가 뒤(60일)까지 섞여 보이는 착시가 있을 수 있다. |
| **scan_type: bucket** | 비중첩(non-overlapping) 구간 — 예를 들어 `bucket 20-40`은 "20일 뒤부터 40일 뒤까지만"의 수익률. `cum`의 착시를 없애려고 따로 잰다. | "언제 정확히 효과가 나타나는지"를 보려면 `cum`보다 `bucket`을 봐야 한다. `px_near_52w_high`는 `cum` 셀은 실패하고 `bucket` 셀만 통과한 대표 사례([03](03_px_near_52w_high.md) §1). |
| **peak_h_cum** | 누적(cum) 기준으로 효과(IC 절댓값)가 가장 큰 horizon. family 카드의 "대표 horizon"으로 흔히 인용됨. | 이 값 하나만 보고 "이 horizon에서만 써야 한다"고 단정하면 안 된다 — `px_amihud_20d`는 IC가 가장 큰 건 120일이지만 t-통계량이 가장 큰 건 60일이다([06](06_px_amihud_20d.md) §1). |
| **peak_bucket** | 비중첩(bucket) 기준으로 효과가 가장 큰 구간. `[20, 40]`이면 "20~40일 사이"를 뜻한다. | `cum`의 peak와 다를 수 있다(예: `px_maxret_20d`는 peak_h_cum=60인데 peak_bucket=[20,40]). |
| **onset_h** | 효과가 통계적으로 처음 나타나기 시작하는 horizon. | 이보다 짧은 구간에서는 신호가 아직 없거나 불안정하다는 뜻. `px_maxret_20d`/`px_idio_vol_60d`는 onset=20일 — 20일 전에는 신호가 없다. |
| **half_life_bucket** | 효과 크기가 정점 대비 절반 이하로 줄어드는 구간. | `px_reversal_5d`의 half-life가 `[5,10]`이라는 건 "3일째 정점을 찍은 효과가 5~10일 구간에서는 절반 이하로 줄어든다"는 뜻([01](01_px_reversal_5d.md) §1). |

## 3. "우연이 아님"을 확인하는 재검증 4가지

같은 IC라도 "우연히 한 번 잘 맞은 것"인지 "구조적으로 반복되는 패턴"인지를
구분하려고 4가지 다른 방식으로 재확인한다.

| 재검증 | 방법 | 통과 기준 / 읽는 법 |
|---|---|---|
| **서브기간(subperiod) 일치** | 2014~2025년을 5개 구간으로 나눠 각 구간에서 부호가 같은지 확인. `valid_subperiods`(유효 구간 수) 중 `sign_consistent_subperiods`(부호 일치 구간 수). | `5/5`면 특정 시기의 우연이 아니라는 뜻. `px_near_52w_high`만 `4/5`로 6개 중 유일하게 한 구간에서 부호가 반대로 나왔다([03](03_px_near_52w_high.md) §1). |
| **non-overlap offset + sign test** | 예를 들어 20일 horizon이면, 시작점을 1일씩 밀린 20개의 "겹치지 않는 표본 집합"(offset 0~19)을 따로 만들어 각각 IC를 재계산. `offset_sign_agreement_ratio`=이 offset들 사이 부호가 같은 비율, `p_sign_test`=그 정도로 부호가 일치할 확률. | 100%(=1.0)면 시작점을 어디로 잡아도 항상 같은 방향이 나온다는 뜻 — 특정 날짜 집합을 우연히 골라서 나온 결과가 아님을 보장. |
| **delay_pass / native_ic vs lag1_ic** | 오늘 계산한 값 그대로 쓴 것(`native_ic`)과 하루 늦춰 쓴 것(`lag1_ic`)을 비교. | `lag1_ic`가 `native_ic`의 절반 이상이면 `delay_pass=true` — "어제 값으로 오늘 매매"해도 효과가 크게 죽지 않는다는 뜻으로, 실전 적용 가능성을 본다. `flow_individual_netbuy_to_volume`은 두 값이 원래부터 같다 — 공식 registry가 이미 지연 버전을 "기본값"으로 등록했기 때문([02](02_flow_individual_netbuy_to_volume.md) §1). |
| **temporal placebo** (`p_temporal_nw`, `temporal_null_pass`) | 60일 이상 긴 구간에만 추가로 적용. 날짜 순서를 통째로 무작위 순환시켜(circular shift) 100번 다시 계산했을 때, 실제 관측값만큼 극단적인 값이 몇 번이나 나오는지 측정. | `p_temporal_nw=0.0099`는 "100번 중 0번만 실제 값과 비슷하거나 더 컸다"는 뜻(통과). `px_near_52w_high`는 `p=0.911`로 실패했다 — 100번 중 91번이 실제 관측값과 비슷하거나 더 컸다는 뜻이라, 이 장기 신호가 통계 기법의 함정(우연한 시계열 패턴)일 가능성을 배제할 수 없다([03](03_px_near_52w_high.md) §1). |

## 4. 어떤 종목 집합으로 계산했는가

| 용어 | 뜻 |
|---|---|
| **universe: broad** | 거래정지·관리종목까지 포함한 전체 상장 종목. |
| **universe: tradable** | 유동성·가격·정지·관리종목 필터를 거친, 실제로 매매 가능한 종목만. `dim_universe_tradable_daily.in_universe=true`인 종목([00_overview.md](00_overview.md) §1.2). |
| **tradable_retention** | `tradable_ic / broad_ic` — tradable로 좁혔을 때 효과가 얼마나 남는지 비율. | 100% 근처면 유동성 낮은 종목이 신호에 크게 기여하지 않는다는 뜻. `px_amihud_20d`는 85.1%로 6개 중 가장 낮다 — 비유동성 프리미엄의 성격상 유동성 필터로 걸러지는 부분이 있다는 뜻([06](06_px_amihud_20d.md) §1). |
| **sample_kind: common_survivor** | 분석 대상 기간 내내 데이터가 끊기지 않고 존재하는 종목만 골라 만든 표본. 공식 BH-FDR 검정(75개 사전등록 셀)에 쓰인 표본이 바로 이것. |
| **sample_kind: available** | 그 시점에 실제로 값이 존재하는 모든 종목(상장폐지·데이터 시작 시점이 종목마다 달라도 포함). `common_survivor`와 비교해 생존편향 영향을 점검하는 용도. |
| **kospi_weight_mean / kosdaq_weight_mean** | 그 family의 표본에서 KOSPI·KOSDAQ 종목이 차지하는 평균 비중. | 6개 A등급 모두 KOSDAQ 비중이 58~59%로 비슷 — 한국 상장종목 수 자체가 KOSDAQ에 더 많기 때문이며, 특정 family가 한쪽 시장에 유독 편중된 것은 아니라는 뜻. |

## 5. 최종 등급과 한계

| 용어 | 뜻 |
|---|---|
| **evidence_grade** | A(다음 단계 후보)/B/C(진단용, 후보 아님)/D(무신호·탈락)/R(비교 기준용) 5단계. 이 디렉터리는 전부 A등급만 다룬다. |
| **screen_pass** | Phase A 스크리닝을 통과했는지(참/거짓). `evidence_grade=A`와 사실상 같은 의미로 쓰인다. |
| **primary_discovery / registry(75개 셀)** | Phase A가 사전에 "이 family는 이 horizon들만 검정하겠다"고 등록해둔 75개 (family×horizon×scan_type) 조합. `q_fdr_global`·`bh_pass`는 이 75개 안에서만 계산된다 — 사후에 아무 horizon이나 골라 유의한 걸 찾는(data mining) 것을 막기 위한 장치. |
| **sparse_primary_grid** | 그 family가 등록한 primary 셀 수가 다른 family보다 적다는 표시. `px_amihud_20d`는 onset이 60일로 늦어 40일 이전 셀이 원천적으로 등록 대상이 아니었다([06](06_px_amihud_20d.md) §1). |
| **survival_bias_unresolved** | "현재 상장된 종목 위주로 수집된 데이터라 상장폐지 종목의 나쁜 수익률이 빠져 있을 수 있다"는 한계 표시. 60일 이상 장기 horizon을 쓰는 family 전부에 공통으로 붙는다. |
| **acceptance gate** | Phase A 스크리닝 다음 단계. 증분성·거래비용 차감 후 수익성·holdout 최종평가를 거쳐야 "실거래 채택"이 확정된다. 이 디렉터리의 6개는 전부 이 단계를 아직 통과하지 않았다. |

## 6. 종목 사례 표 읽는 법 (이번에 새로 만든 부분)

| 용어 | 뜻 |
|---|---|
| **decile / 10분위** | 어떤 날짜의 전체 종목을 피쳐값 기준으로 10등분한 그룹. decile 1이 가장 작은 값, decile 10이 가장 큰 값(단, `px_maxret_20d`·`px_idio_vol_60d`처럼 기대 방향이 `-`인 family는 decile 10이 "안 좋은 쪽"이다). |
| **avg_feat** | 그 decile에 속한 종목들의 피쳐값 평균 — "이 그룹이 실제로 어느 정도 수준의 값인지" 감을 잡는 용도. |
| **raw 수익률 / avg_fwd_ret** | 종목의 실제 가격 변화 그대로(시장 대비 조정 없음). "얼마나 올랐나"의 가장 직관적인 답. |
| **초과수익 / excess_ret / raw_label** | 같은 날짜·기간의 동일가중 시장 평균 수익률을 뺀 값. "시장 전체가 오른 만큼을 빼고, 이 종목이 상대적으로 더 잘했는가"를 본다. Phase A가 실제로 검정하는 것도 이 초과수익 기준 순위다. | raw와 방향이 다를 수 있다 — `px_amihud_20d`의 decile 1(초대형주)은 raw로는 거의 0%인데 초과수익은 -4%까지 밀리는데, 이는 종목 자체가 나쁜 게 아니라 소형주 중심의 벤치마크가 더 많이 올랐기 때문이다([06](06_px_amihud_20d.md) §2). |
| **pct_positive / 상승 비율** | raw 수익률이 양수인 관측치의 비율. | 이게 50%를 넘지 않아도 평균 수익률은 개선될 수 있다 — "이길 확률"이 아니라 "평균적으로 얼마나 버는가/잃는가"가 핵심이다([01](01_px_reversal_5d.md) §2). |
| **formation date / anchor_date** | 피쳐값을 관측한 시점(매매 신호가 나온 날). 이 날짜부터 며칠 뒤(horizon)까지의 수익률로 검증한다. |
| **label_ok_{h}d** | 그 formation date에서 h일 뒤 수익률을 안전하게 계산할 수 있는지(참/거짓). 데이터가 아직 없거나(미래), holdout 구간에 걸리면 거짓이 된다. |
| **holdout** | 2025-08-01~2026-07-31 구간처럼, 최종 acceptance gate 평가를 위해 미리 들여다보지 않기로 봉인해둔 최근 기간. `label_ok_*d`가 이 구간에서는 전부 거짓으로 처리돼 있다([00_overview.md](00_overview.md) §2). |

## 7. 직접 숫자 하나를 읽어보기 (연습)

[01_px_reversal_5d.md](01_px_reversal_5d.md) §1의 이 줄을 예로 들어본다.

```text
cum | 0–3일 (peak) | ic_mean=0.0540 | icir=0.503 | t_nw=18.9 | q_fdr_global=4.6e-78
```

풀어 쓰면: "형성일부터 3거래일 뒤까지의 초과수익 순위를, 형성일의
`px_reversal_5d` 순위와 비교했더니 상관계수(IC)가 0.054로 나왔다. 이 상관은
날마다 방향이 꽤 일관됐고(ICIR 0.50), Newey-West로 엄격하게 보정한
t-통계량도 18.9로 매우 크며, 75개 사전등록 셀을 동시에 검사했다는 걸 감안해
보정한 확률(q값)은 10의 -78승 수준으로 사실상 0이다." — 즉 "우연일 가능성은
없다고 봐도 되지만, 상관계수 0.054 자체는 크지 않다"는 두 가지를 동시에
읽어야 한다. **통계적으로 확실한 것(q값)과 실제 효과의 크기(IC)는 서로 다른
질문**이라는 점이 이 표 전체를 읽는 핵심이다.
