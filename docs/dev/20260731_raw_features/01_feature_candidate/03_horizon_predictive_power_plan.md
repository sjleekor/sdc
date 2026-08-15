# 03. 가변 horizon 피쳐 예측력 검증 계획 (Horizon Scan)

- 작성일: 2026-08-01 (rev.2 — 최신 sj2_remote 공통 snapshot 자동 선택 규칙 반영)
- 선행 문서: [02_feature_candidate.md](02_feature_candidate.md) (피쳐 정의·검증 설계의 기준 문서)
- 데이터 기준: `raw_postgres`와 `derived_mart` 양쪽에서 `source=sj2_remote`인
  snapshot 중 가장 최신의 공통 `snapshot_date`를 실행 시 자동 선택한다(특정 날짜 고정 금지).
- 재사용 코드: `research/etl/labels.py`(파라미터화 레이블), `research/etl/metrics.py`
  (per-date rank IC), `research/etl/splits.py`, `research/etl/features/{price,flow}.py`

> **rev.1 변경 요지**: ① bucket 수익률 산식을 복리 기준으로 정정 ② holdout 경계를
> label **종료일** 기준으로 정정 + 공통 formation sample 주 분석 ③ corporate-action
> flag를 경고에서 **판정 선결 조건**으로 격상 ④ 기존 마트 현황을 실제 코드와 맞추고
> Phase A0(피쳐·universe 준비) 신설 ⑤ horizon별 표본 구성 변화(censoring) 통제 추가
> ⑥ 검정 집합·FDR 단위를 설정 파일로 고정 ⑦ 추론 단위(날짜×시장)와 HAC lag 명시
> ⑧ event-time 규칙 구체화 ⑨ sanity·결과 컬럼 정밀화.

### 데이터 snapshot 선택 규칙

분석 실행 시 snapshot 날짜를 문서나 실행 인자에 고정하지 않고, 두 레이크에서
동시에 사용할 수 있는 최신 `sj2_remote` snapshot을 선택한다.

```text
raw_dates     = snapshot_date 디렉터리 중 source=sj2_remote인 날짜 집합
derived_dates = snapshot_date 디렉터리 중 source=sj2_remote인 날짜 집합
snapshot_date = max(raw_dates ∩ derived_dates)
```

선택된 날짜의 입력 경로는 다음과 같다.

```text
data_lake/raw_postgres/snapshot_date=<selected>/source=sj2_remote/
data_lake/derived_mart/snapshot_date=<selected>/source=sj2_remote/
```

`raw_dates ∩ derived_dates`가 비어 있으면 실행을 중단한다. raw와 derived에 서로
다른 최신 날짜가 있더라도 각기 다른 날짜를 조합하지 않는다. raw snapshot은
`_manifests/_SUCCESS.json`의 완료 표식과 계획상 필요한 테이블 집합을 확인하고,
derived snapshot은 `stock_metric_fact`와 `common_feature_daily_fact`가 모두 존재하는지
확인한다. 실행 로그와 산출물 manifest에는 실제로 선택된 `snapshot_date`와
`source=sj2_remote`를 기록한다.

## 0. 목적과 답하려는 질문

02 문서의 acceptance gate(§6.1)는 "고정된 label(기본 20d)에 대해 피쳐를 채택할지"를
판정한다. 그 전에 먼저 답해야 할 질문이 있다:

> **각 피쳐의 변화는 주가에 "언제" 영향을 미치는가?**
> 예측력이 있다면 어느 horizon에서 나타나고, 언제 정점이고, 언제 소멸하는가?

이 문서는 02 §3의 피쳐 후보들에 대해 **label horizon을 변경해 가며** 단변량
예측력을 스캔하는 계획이다. 산출물은 피쳐 family별 "유효 horizon 지도"이며,
이것이 이후 두 가지 결정의 입력이 된다:

1. 어떤 피쳐를 어떤 label(5d / 20d / 60d)의 후보군에 배정할지.
2. 예상 horizon과 실측 horizon이 다른 피쳐(예: 한국에서 약화가 보고된 모멘텀)를
   acceptance gate에 올릴지, 폐기할지, 변형(잔차·interaction)으로 우회할지.

**이 스캔은 screening이지 최종 채택이 아니다.** 여기서 살아남은 (family, horizon)
조합만 02 §6.1 gate(증분성·거래비용·holdout)로 진입한다.

## 1. 방법 개요 — 두 개의 스캔 축

### 1.1 누적 IC 곡선 — IC(h)

- label: `t+1 ~ t+h`의 시장 내 초과수익 rank (`kind="excess"`, `bench="eqw_market"`,
  `y_rank_{h}d`) — 기존 `make_label` 정의 그대로, horizon만 확장.
- **사전 등록 horizon grid (고정, 추가 금지)**:

  ```text
  h ∈ {1, 2, 3, 5, 10, 20, 40, 60, 120}
  ```

- 지표: 날짜별 Rank-IC(§3.1의 추론 단위 정의를 따름)의 평균, ICIR,
  Newey–West t-stat, 예상부호 일치율(`expected_sign_aligned_ratio`, §1.4),
  분위(quintile) top–bottom spread.

### 1.2 구간(비중첩 bucket) IC — "언제"에 대한 직접 답

누적 IC(h)만 보면 **초기 5일의 효과가 60d 곡선까지 끌려가 보이는 착시**가 있다
(누적 수익에 초기 구간이 포함되므로). 영향 시점을 분리하려면 서로 겹치지 않는
forward 구간별 수익으로 IC를 계산한다:

```text
bucket 구간:  (t+0..t+5], (t+5..t+10], (t+10..t+20],
              (t+20..t+40], (t+40..t+60], (t+60..t+120]
```

**산식 (복리 기준 — 단순 차분 금지)**: `labels.py`의 `fwd_ret_{h}d`는
`close[t+h]/close[t] − 1` **단순수익률**이므로, bucket 수익은 비율로 계산한다.

```text
bucket_ret(h1, h2] = (1 + fwd_ret_{h2}) / (1 + fwd_ret_{h1}) − 1
                   = close[t+h2] / close[t+h1] − 1
(log 표현: log1p(fwd_ret_{h2}) − log1p(fwd_ret_{h1}))
```

`fwd_ret_{h2} − fwd_ret_{h1}` 같은 단순 차분은 복리를 무시해 특히 고변동 종목에서
bucket 수익의 크기·순위를 모두 왜곡하므로 금지. 단위 테스트로 항등성
`close[t+h2]/close[t+h1] − 1`을 검증한다(§6.3).

- label: 각 bucket 수익의 (date, market) 내 excess rank (excess도 bucket 수익
  기준의 동일가중 시장 평균 차감으로 재계산 — 누적 label의 excess 재사용 금지).

해석 규칙:

- **즉각 반응형** (bucket1만 유의): 단기 label 전용. 실행 지연에 취약 → §4의
  1일 지연 강건성 체크 필수.
- **지연 반응형** (bucket2~4에서 정점): 20d/60d label 후보. 가장 가치 있는 패턴
  (거래비용·실행 지연에 강함).
- **부호 반전형** (단기 +, 장기 − 또는 그 반대): 예컨대 flow price-pressure 후
  반전. 단일 부호 alpha로 쓰면 안 되고 horizon별로 다른 취급 필요 — 이 패턴의
  검출이 bucket 분석의 존재 이유다.

### 1.3 이벤트 기준 피쳐 — event-time 곡선

`fin_sue` 등 공시 이벤트 피쳐는 달력 horizon 스캔이 부적절하다(피쳐 값이 접수일
이후 정체 구간에서 반복 관측됨). 전용 경로의 규칙:

1. **관측 단위**: (종목, 공시 이벤트) 1행 — 일별 broadcast 행 재사용 금지
   (같은 이벤트의 중복 계상).
2. **최초 거래 가능일**: 접수시각(`rcept_time`) dimension은 02 Phase 3 이후에야
   확보되므로, 그 전에는 **접수일 다음 거래일부터 사용** (장중/장후 구분 불가 →
   보수적 1일 지연). event day 0 = 접수일 다음 거래일.
3. **event-time bucket**: day 0 이후 `{1–3, 4–5, 6–10, 11–20, 21–40, 41–60}`
   거래일 bucket별 초과수익(§1.2와 동일한 복리 산식)에 대한 SUE의 IC.
4. **수정공시**: SUE 값은 **원공시(최초 접수) 기준**으로 event를 정의한다.
   수정공시는 새 event를 만들지 않으며, 수정 접수일 이후 구간은 해당 event의
   window에서 절단(원공시 SUE가 더 이상 유효한 정보가 아님).
5. **동일 종목 중첩 공시**: 다음 공시의 day 0에서 이전 event의 잔여 window를
   절단한다 (한 시점의 수익률이 두 event에 이중 귀속되지 않게).
6. **추론**: 서로 다른 날짜의 이벤트를 하나의 pooled Spearman으로 섞지 않는다.
   한국 분기공시는 마감일 부근에 강하게 몰리므로 **cohort(공시일) 단위
   cross-sectional IC**를 계산하고(최소 이벤트 수 `n ≥ 30` cohort만),
   cohort-date 시계열에 대해 NW t-stat을 적용한다. 동일 종목 반복 출현에 대한
   issuer-clustering 보정 또는 calendar-time portfolio 방식은 결과가 경계선일
   때의 확인 절차로 둔다.

재무 비율류(value/profitability 등)는 연속 피쳐로 취급해 §1.1~1.2 스캔을 쓰되,
`fin_age_days`(공시 후 경과일) 3분위로 나눠 IC를 보고 — "신선한 재무 정보일수록
예측력이 큰가"를 함께 답한다.

### 1.4 decay 요약 통계

family별로 곡선을 요약해 비교 가능하게 만든다. 모든 요약치는 **예상 부호로 정렬한
IC** (`aligned_ic = expected_sign × ic`) 기준으로 계산한다 — 음(−) 가설 family에서
"양수 비율" 류 통계가 무의미해지는 문제를 피하기 위함이다.

| 통계 | 정의 | 적용 축 |
|---|---|---|
| `peak_h_cum` | \|aligned IC\| 최대 누적 horizon | 누적 |
| `peak_bucket` | aligned IC 최대 bucket | bucket |
| `onset_h` | NW t-stat 기준 처음 유의해지는 누적 horizon | 누적 |
| `half_life_bucket` | bucket aligned IC가 peak의 50% 아래로 처음 내려가는 bucket | bucket |
| `sign_flip_bucket` | bucket aligned IC 부호가 바뀌는 bucket (없으면 NULL) | bucket |
| `expected_sign_aligned_ratio` | aligned IC > 0인 날짜 비율 | 공통 |

누적 곡선은 단조 누적 성격상 half-life 개념이 성립하지 않으므로 decay 판정은
bucket 축에서만 한다.

## 2. 레이블·데이터 정의

### 2.1 레이블

- 기존 `LabelSpec`을 그대로 사용하되 `horizons=(1,2,3,5,10,20,40,60,120)` 인스턴스를
  스캔 전용으로 materialize (`label_scan` 테이블, 기존 `label_daily`와 분리).
- bucket 레이블은 `labels.py` 확장: §1.2의 복리 산식(`close[t+h2]/close[t+h1] − 1`)
  으로 bucket 수익 컬럼 생성 → bucket별 eqw excess → per-(date, market) rank.
- halt(`open=high=low=0`) 제외 거래일 인덱스, 상하한가·정지 종목의 진입 불가 처리
  등은 기존 label 규칙(02 §5 Phase 0-6)을 그대로 상속.

### 2.2 기간·holdout 경계

- 표본 시작: **2014-06** (KOSPI 2014-01-20 이전 KOSDAQ 편향 회피, 02 §2.2).
  flow 피쳐는 실데이터 시작일부터, 공매도 잔고는 2016-06-30부터(`is_available`).
- **holdout 오염 방지 — label 종료일 기준**: holdout 12개월(2025-08 ~ 2026-07)의
  수익률이 스캔에 한 조각도 들어가지 않아야 한다. formation date만 제외하면
  2025-07-31의 20d/120d label이 holdout 수익률을 사용하게 되므로, 제외 조건은
  formation이 아니라 label 종료일에 적용한다:

  ```text
  포함 조건: label_end_date(t, h) < holdout_start (2025-08-01)
  ```

- **공통 formation sample을 주 분석으로**: horizon 간 곡선을 직접 비교하려면
  horizon마다 표본 끝이 달라지면 안 된다. 주 분석은
  `label_end_date(t, 120) < holdout_start`를 만족하는 **공통 formation 구간**
  (사실상 formation ≤ 2025-02 무렵)으로 전 horizon을 고정한다. horizon별 최대
  가용 표본 결과는 보조 분석으로만 병기한다.
- **서브기간 최소 표본 규칙**: 위 규칙으로 2025.04– 서브기간은 120d 분석에서 거의
  남지 않는다. 세그먼트·서브기간 셀은 `n_dates ≥ 60`(cohort 분석은 cohort ≥ 8)
  미만이면 판정에서 제외하고 표에 `insufficient`로 표기한다.

### 2.3 universe — 이원 보고

- universe 이원 보고(02 §6.3): broad / tradable(최근 20일 거래대금 하한, 정지·관리
  제외) 두 벌로 전 지표 산출. **tradable에서 소멸하는 신호는 microcap 착시로 분류.**
- 현재 코드의 `dim_universe_daily`(`research/etl/universe.py`)는 warm-up +
  유동성 하한(1억원)의 **단일 universe**다. broad/tradable은 `UniverseFilter`
  파라미터 2벌(broad: 유동성 하한 완화, tradable: 02 §6.3 기준)로 materialize한다
  — Phase A0 작업 항목(§6.1).

### 2.4 조정주가 — 스캔 판정의 선결 조건 (경고 아님)

액면분할·병합·무상증자는 momentum류 **피쳐**뿐 아니라 corporate action을
가로지르는 **모든 forward label**(특히 60d/120d)을 오염시킨다 — flow 피쳐의 장기
IC도 함께 왜곡된다. 따라서 02 §2.3(Phase 1 진입 조건)과 일관되게, **공식 horizon
판정은 아래 중 하나가 갖춰진 뒤의 실행 결과로만 한다**:

1. adjusted close (또는 corporate-action factor) 구축, 또는
2. corporate action이 걸린 (종목, 기간)의 feature/label window **전체 제외**, 또는
3. 최소 요건: anomaly flag(주식수 급변 + 비정상 OHLC jump) 기반 마스킹을 label과
   피쳐 양쪽에 적용한 결과만 판정에 사용.

flag 적용 전 실행은 **파이프라인 smoke test로만** 취급하며 결론 카드에 올리지
않는다. flag 적용 전/후 IC 차이는 flag 자체의 품질 진단으로만 기록한다.

### 2.5 horizon별 표본 구성 변화 (censoring) 통제

h가 길수록 미래 가격이 없는 관측(상장폐지·장기 정지·표본 말단)이 label NULL로
빠진다. 이를 방치하면 **IC 곡선의 변화가 decay가 아니라 생존 종목 구성 변화**일
수 있다. 통제 장치:

- 산출물에 horizon별 `label_coverage`(= label 비NULL 비율)와
  `survival_to_h`(formation 시점 universe 중 t+h 가격 존재 비율)를 함께 기록.
- **공통 survivor sample 주 분석**: horizon 간 곡선 비교는 전 horizon에서 label이
  존재하는 관측(= 120d survivor)으로 고정한 표본에서 수행. horizon별 최대 가용
  표본 결과는 보조 병기 (§2.2의 공통 formation 규칙과 결합).
- 상장폐지 terminal return 처리(폐지 전 마지막 가격으로 절단 vs 폐지 손실 반영)는
  현 master가 DELISTED 28개뿐이라(02 §5 Phase 0-7) 어차피 생존편향이 커서 이번
  스캔에서는 정책을 새로 만들지 않는다. 대신 survivor sample과 available sample의
  IC가 크게 갈리는 family에는 **attrition 경고**를 결론 카드에 부착한다.

## 3. 통계 처리

### 3.1 추론 단위와 자기상관(HAC) 처리

**추론 단위(주 통계)**: `metrics.per_date_rank_ic`는 date 단일 키 그룹핑이므로
그대로 쓰면 KOSPI/KOSDAQ 두 IC가 같은 날짜에 독립 관측으로 이중 계상된다.
주 통계는 다음으로 고정한다:

1. (date, market)별 Spearman IC 계산 →
2. 같은 날짜의 시장별 IC를 (종목 수 가중) 평균해 **날짜당 1개의 daily IC** 생성 →
3. 이 단일 시계열에 HAC t-stat 적용.

시장별 IC 시계열과 각각의 HAC t-stat은 세그먼트 결과(§4)로 별도 보고한다.

**HAC(Newey–West) lag — scan 축별로 구분**: h>1이면 인접 날짜 label이 기간을
공유해 daily IC에 강한 양의 자기상관이 생기고, naive `mean/std/√T` t-stat은 h가
클수록 유의성을 심하게 과대평가한다.

| scan 축 | 중첩 구조 | NW lag |
|---|---|---|
| 누적 IC(h) | 인접 formation이 h−1일 공유 | `h − 1` |
| bucket (h1, h2] | formation 간격 < 폭이면 공유 | `(h2 − h1) − 1` |
| event-time | cohort-date 시계열 | cohort 간격 기준 소폭(주 단위) + 경계선 결과는 issuer/cohort clustering 재확인(§1.3-6) |

모든 표에서 naive t와 NW t를 병기하되 판정은 NW t로 한다.

**비중첩 subsampling 교차검증**: 매 h일 간격으로 formation date를 골라낸
비중첩 표본의 IC로 NW 결과의 방향을 확인한다. 이때 **임의의 한 offset이 아니라
가능한 h개 offset 전부**를 계산해 offset 간 분산까지 함께 본다 (한 offset만 쓰면
그 자체가 또 하나의 선택 자유도가 된다).

### 3.2 multiple testing — 검정 집합을 코드로 고정

스캔 대상은 §5의 **25 family** (price 9 + flow 8 + financial/event 8)다.
(02 §8의 "price 9 + flow 11 + fin 9"는 **컬럼** 수 기준 — family 수와 혼동 금지.)

**검정 집합은 설정 파일로 고정한다** — `research/analysis/horizon_scan_config.yaml`:

```yaml
- family: flow_foreign_netbuy_to_volume
  fdr_family: flow
  expected_sign: +
  features:
    - {column: flow_foreign_netbuy_to_volume_20d, role: primary}
    - {column: flow_foreign_netbuy_to_volume_5d,  role: secondary}
    - {column: flow_foreign_netbuy_to_volume_60d, role: secondary}
  primary_horizon_set:     [5, 10, 20]        # 가설 구간 (§5 표)
  exploratory_horizon_set: [1, 2, 3, 40, 60, 120]
```

통제 규칙:

1. **피쳐 window 취급 명시**: "window × horizon 탐색 금지"의 정확한 의미는
   *window를 결과를 보고 고르지 않는다*는 것이다. net-buy 5/20/60d처럼 02에서
   family로 사전 등록된 복수 window는 모두 스캔하되, family당 **primary 컬럼
   1개**(위 예시는 20d)만 판정에 사용하고 secondary는 강건성 참고로만 본다.
   primary 지정은 스캔 실행 전에 config에 고정한다.
2. **주 검정 집합** = 전 family의 (primary feature × primary_horizon_set 셀,
   누적+해당 bucket). 이 집합 전체에 대해 **global BH-FDR q=0.10**을 1회 적용한다
   (family 내부 BH만 하면 family 간 선택에서 전체 FDR이 무너진다).
   `fdr_family` 필드는 도메인별 발견율 보고용이지 통제 단위가 아니다.
3. **탐색 집합** = exploratory_horizon_set + secondary feature 셀. 여기서의
   발견은 global BH에 넣지 않는 대신 **\|t_NW\| > 3** (Harvey, Liu & Zhu 2016)을
   요구하고, 통과해도 "탐색적 발견"으로 강등 — 본 채택 라인에 올리지 않고
   차기 사전 등록 목록의 후보로만 남긴다.
4. **판정 임계 요약**: 가설 구간 내 = global BH q<0.10 (t 임계는 BH가 결정 —
   1.96 아님). 가설 구간 밖 = \|t_NW\| > 3 + 탐색적 강등. 어느 쪽이든 단일
   horizon에서만 튀는 결과는 노이즈로 취급하고 **인접 horizon에서 일관된 곡선
   형태**만 신뢰한다.

## 4. 세그먼트·레짐·강건성 축

모든 (family, horizon) 셀에 대해 아래 분할을 보고한다. **전체 표본 유의 + 특정
세그먼트 전멸**이면 그 세그먼트를 결론 카드에 명시한다. §2.2의 최소 표본 규칙
(`n_dates ≥ 60`)에 못 미치는 셀은 `insufficient` 처리.

| 축 | 분할 | 목적 |
|---|---|---|
| 시장 | KOSPI / KOSDAQ | 구조 차이 (02 §2.1). §3.1의 시장별 IC를 그대로 사용 |
| 규모 | PIT mktcap 3분위 (날짜별) | microcap-only anomaly 검출 (Hou et al. 2020). PIT 시총 daily 테이블(02 Phase 0-3)이 선결 — Phase A0 |
| 유동성 | `px_amihud_20d` 3분위 | 실행 가능성 |
| 기간 | 2014-06–2016 / 2017–2019 / 2020–2021 / 2022–2023.10 / 2023.11–2025.03 / 2025.04– | 서브기간 부호 안정성. 경계는 공매도 레짐(02 §2.4)과 정렬 — 사전 고정. 2025.04–는 장기 horizon에서 `insufficient` 예상(§2.2) |
| 공매도 레짐 | 허용 / 금지 구간 | F6~F9·NAT는 금지 구간 제외 또는 flag 조건부로만 판정 |
| 실행 지연 | 피쳐를 1거래일 늦춰(`t-1` 값) 재계산한 IC | 단기 horizon 신호의 데이터 가용시점 leakage·실행 지연 강건성 (특히 flow — KRX 게시 시각 불확실, 02 §2.2) |

실행 지연 체크 판정: bucket1(0–5d) IC가 1일 지연 시 크게 소멸하면, 그 피쳐의
단기 예측력은 "당일 마감 직후 실행" 가정에 의존하는 것 — live 경로에서 게시
시각이 확정되기 전까지는 지연 버전 IC를 공식 수치로 쓴다.

## 5. family별 사전 horizon 가설 (스캔 전 등록)

아래 25 family를 스캔 대상으로 한다(§3.2의 config가 본 표의 기계 판독 버전).
가설 구간은 문헌·02 문서의 비고에서 도출했으며, **스캔 결과를 본 뒤 수정하지
않는다**. "예상 peak horizon" 열이 config의 `primary_horizon_set`이 된다.

### 5.1 price — 9 family (R0, Phase A)

| Family | 대표(primary) 컬럼 | 예상 부호 | 예상 peak horizon | 근거 요지 |
|---|---|---|---|---|
| 단기 반전 | `px_reversal_5d` | `+` | 1–10d | Jegadeesh 1990; 한국 개인 비중 → 강할 것. bid-ask bounce 확인 위해 1일 지연 체크 중요 |
| 중기 모멘텀 | `px_mom_12_1` (secondary: `px_mom_6_1`) | `+` | 20–120d | 한국 약화/부재 보고 다수 — **부재 확인 자체가 이 스캔의 성과** |
| 잔차 모멘텀 | `px_resid_mom_12_1` | `+` | 20–120d | 클래식보다 label 정합적 — 클래식과 곡선 비교가 핵심 산출물 |
| 52주 고점 | `px_near_52w_high` | `+` | 20–60d | George & Hwang 2004; 아시아 견조 |
| MAX | `px_maxret_20d` | `-` | 20–60d | 복권 수요 — 상한가 뭉침 왜곡 병행 확인 |
| IVOL/변동성 | `px_idio_vol_60d` | `-` (민감) | 20–60d | 부호 민감 B등급 — 세그먼트 불안정하면 risk feature로 강등 |
| 비유동성 | `px_amihud_20d` | `+` | 60–120d | 프리미엄은 장기 — 단기 유의는 microcap 착시 의심 |
| 거래량 충격 | `px_turnover_shock` | `+` | 5–20d | Gervais et al. 2001 — 단기 한정, 장기 소멸 예상 |
| 거래정지 비율 | `px_zero_ret_ratio_20d` | 필터 | — | alpha 아닌 필터 — 참고 스캔만 (검정 집합 제외) |

### 5.2 flow — 8 family (R0/R1, Phase A, 최우선)

| Family | 대표(primary) 컬럼 | 예상 부호 | 예상 peak horizon | 근거 요지 |
|---|---|---|---|---|
| 외국인 순매수 | `flow_foreign_netbuy_to_volume_20d` (secondary: 5/60d) | `+` | 5–20d (단기 압력) | **bucket 분석 최우선 대상** — price pressure 후 반전 vs 정보 지속의 분리가 목적 |
| 기관 순매수 | `flow_inst_netbuy_to_volume_20d` (secondary: 5/60d) | `+` | 5–20d | 동일 |
| 개인 순매수 | `flow_individual_netbuy_to_volume_20d` (secondary: 5d) | **미고정** | — | 부호 자체가 검증 대상 (02 §4). 단기 −(유동성 공급 반전) / 장기 0 가설. 부호 미고정이므로 양측 검정 |
| 외국인 보유 변화 | `flow_foreign_holding_ratio_chg_20d` | `+` | 20–60d | ownership 이동은 지연 반응형 예상 |
| 공매도 강도 | `flow_short_turnover_20d` | `-` | 5–60d | Diether et al. 2009 — 금지 레짐 제외 |
| 공매도 잔고 | `flow_short_interest_ratio` (secondary: `_chg_20d`) | `-` | 20–60d | position stock이므로 flow보다 장기. 공개지연 as-of 반영 |
| Days to cover | `flow_days_to_cover` | `-` | 20–60d | 잔고 family와 곡선 형태 비교 → 중복이면 하나만 |
| NAT proxy | `flow_nat_proxy_20d` | `+` | 20–60d | Jeong, Eo & Kang 2026 replication 전의 고정 proxy — 원 논문 horizon(월 단위)과 대조 |

### 5.3 financial/event — 8 family (R1, Phase B)

| Family | 대표(primary) 컬럼 | 예상 부호 | 예상 peak horizon | 근거 요지 |
|---|---|---|---|---|
| 규모 | `fin_log_mcap` | `-` (민감) | 60–120d | regime 축 겸용 — 단독 판정보다 참조 |
| 가치 composite | `fin_value_z` | `+` | 60–120d | 장기 성격 — 20d 이하 무유의는 정상. `fin_age_days` 3분위 병행(§1.3) |
| 수익성 | `fin_gross_profitability` (secondary: `fin_operating_profitability`) | `+` | 20–120d | 완만하고 평평한 곡선 예상 |
| 자산성장 | `fin_asset_growth_yoy` | `-` | 60–120d | 연 단위 정보 |
| 발생액 | `fin_accruals_to_assets` | `-` | 60–120d | 다음 실적 공시 전후 정점 가능 — event-time 보조 확인 |
| SUE/PEAD | `fin_sue` | 서프라이즈 방향 | 접수 후 1–60d (event-time) | §1.3 전용 경로 (접수일 +1 거래일부터, cohort 추론) |
| 발행/희석 | `ev_net_share_issuance_yoy` | `-` | 60–120d | corporate action 정제 전 quality flag 필수 |
| 주주환원 | `ev_payout_yield` | `+` | 60–120d | 밸류업 레짐(2025~) 서브기간 별도 확인 |

## 6. 실행 설계

### 6.1 단계 — Phase A0(준비) 신설

**현황 정정**: 기존 `feat_price`/`feat_flow` 마트에는 §5 피쳐의 상당수가 아직
없다. `price.py`에는 `px_ret_*`(원시 수익률)·`px_dist_52w_high`·`px_amihud_20d`·
`px_vol_{20,60}d`만 있고 reversal/12-1 momentum/잔차 momentum/MAX/turnover shock/
zero-ret ratio가 없다. `flow.py`는 5/20d raw sum·z-score 위주로
`netbuy_to_volume_*`·60d window·`short_interest_ratio`·`days_to_cover`·NAT가 없다.
universe도 단일 구성이다. 따라서:

- **Phase A0 — 피쳐·universe·인프라 준비 (스캔 실행 전 선결)**
  1. **후보별 readiness matrix 산출**: §5의 각 primary/secondary 컬럼에 대해
     `ready / mart 확장 필요 / PIT 의존(blocked)` 3분류 표를 만들어 config에
     병기. (첫 작업 — 이후 항목의 작업량이 여기서 확정된다.)
  2. `feat_price` 확장: `px_reversal_5d`, `px_mom_6_1`, `px_mom_12_1`,
     `px_resid_mom_12_1`, `px_near_52w_high`(기존 `px_dist_52w_high` 정합 확인),
     `px_maxret_20d`, `px_idio_vol_60d`, `px_turnover_shock`,
     `px_zero_ret_ratio_20d`.
  3. `feat_flow` 확장: `netbuy_to_volume_{5,20,60}d`(3주체),
     `flow_short_turnover_20d`, `flow_short_interest_ratio`(+chg),
     `flow_days_to_cover`, `flow_nat_proxy_20d`. (float 분모 버전은 PIT 주식수 의존 →
     Phase B로 순연 가능.)
  4. **PIT 시총/주식수 daily 테이블** (02 Phase 0-3): §4 규모 세그먼트와 float
     분모의 공통 의존. 이것이 준비 안 되면 Phase A는 규모 세그먼트 없이 실행하고
     결과에 명시.
  5. broad/tradable **universe 2벌** materialize (§2.3).
  6. **corporate-action anomaly flag** (§2.4) — 공식 판정의 선결 조건.
  7. `label_scan` + bucket label (§2.1).
- **Phase A**: price 9 + flow 8 family 스캔. flow 최신일 정지(KRX 비밀번호 만료,
  2026-07-24)는 스캔 표본이 §2.2 규칙상 2025-02 무렵에서 끝나므로 영향 없음 —
  백필과 독립 진행.
- **Phase B (fin PIT 마트 완성 후)**: financial/event 8 family + `fin_age_days`
  분위·event-time 분석.
- **Phase C (선택)**: Phase A/B에서 부호 반전형·조건부 패턴이 나온 family에 한해
  02 §3.6 interaction 후보의 conditional IC 스캔.

### 6.2 구현 (기존 모듈 확장)

| 작업 | 위치 | 내용 |
|---|---|---|
| readiness matrix | `research/analysis/horizon_scan_config.yaml` + 생성 스크립트 | §6.1-A0-1. family/feature/window/expected_sign/primary·exploratory horizon set/fdr_family/readiness |
| 피쳐 마트 확장 | `research/etl/features/{price,flow}.py` | §6.1-A0-2·3 컬럼 추가 (기존 컬럼·명명 관례 유지) |
| 스캔용 label | `research/etl/labels.py` | `LabelSpec(horizons=(1,2,3,5,10,20,40,60,120))` + **복리 bucket 수익**(§1.2) → `label_scan` materialize |
| 추론 통계 | `research/etl/metrics.py` | (date, market)→daily 집계(§3.1), `newey_west_tstat(ic, lag)`, quantile spread를 **raw excess return** 기준으로 산출하는 옵션 (rank label 기반 spread는 경제적 단위가 없음) |
| 스캔 드라이버 | `research/analysis/horizon_scan.py` (신규) | config 로드 → feature × `label_scan` join → (family, horizon/bucket, universe, segment)별 IC·요약(§1.4) → global BH(§3.2) |
| event-time 경로 | 동일 파일 | SUE 전용: 이벤트 단위 재구성(§1.3 규칙 1~5) → cohort IC → NW |
| 산출물 | `research/output/horizon_scan/` | ① `horizon_ic.parquet` — 컬럼: `family, feature, role, scan_type(cum/bucket/event), h_start, h_end, universe, segment_axis, segment, n_dates, n_obs_mean, label_coverage, survival_to_h, ic_mean, ic_std, icir, t_naive, t_nw, p_nw, q_fdr_global, in_hypothesis, expected_sign_aligned_ratio, q5_spread_raw` ② family별 IC decay 곡선 plot (공통 survivor sample 기준) ③ `03a_horizon_scan_results.md` 결과 보고서 |

계산량 참고: 25 family × 15 label × ~2,600 거래일 × 세그먼트 — per-date Spearman은
polars group_by로 충분히 빠름. 세그먼트 분할은 broad/tradable 2 × (시장/규모/
유동성/기간/지연) 축을 독립적으로 도는 구조로 (조합 폭발 금지).

### 6.3 검증(스캔 자체의 sanity)

1. **synthetic fixture 단위 테스트** (산식·join 검증의 정본): 손으로 만든 소형
   가격 패널로 ① bucket 항등성 `bucket_ret(h1,h2] == close[t+h2]/close[t+h1] − 1`
   ② halt 제외 거래일 인덱스 ③ as-of join 방향 ④ NW t-stat 기준값을 검증.
2. **placebo (permutation)**: 피쳐를 날짜 내 무작위 셔플해 전체 파이프라인 실행.
   q=0.10 global BH에서 발견 0건을 요구하는 것은 통계적으로 틀렸다(기대 발견율이
   0이 아님) — **B회(예: 100회) 반복 permutation의 발견 건수 분포**를 만들고,
   실데이터 발견 건수가 그 분포의 우측 꼬리(예: 95퍼센타일 초과)에 있는지로
   판정한다. 1회 permutation은 leakage smoke test로만 사용.
3. **look-ahead canary**: `t+1` 수익률 자체를 피쳐로 넣으면 h=1 IC ≈ 1 —
   as-of join 붕괴 검출용 (정상이면 이 canary만 비정상적으로 높아야 함).
4. **empirical diagnostic** (테스트 아님 — 참고 진단): 실데이터
   `px_reversal_5d`의 단기 IC 부호가 +인지. 어긋나면 버그를 *의심*하고 1의
   fixture로 원인을 찾되, 이 자체를 pass/fail 기준으로 삼지 않는다 (실증 결과를
   테스트로 굳히면 그 자체가 look-ahead).
5. label 항등성: `y_rank_20d`가 기존 `label_daily`와 동일 (재사용 확인).

## 7. 판정 기준과 결론 카드

family별로 아래 형식의 결론 카드를 만들어 `03a` 결과 문서에 수록한다:

```text
family: flow_foreign_netbuy_to_volume
  판정:        지연 반응형 / 즉각 반응형 / 부호 반전형 / 무신호 / 세그먼트 한정
  채택 horizon: (예) 5–20d → 20d label 후보군 배정
  부호:        가설 일치 여부, 서브기간 안정성 (유효 서브기간 중 몇 개 일치)
  증거 강도:   A(가설 구간 내 global BH 통과 + 전 세그먼트 일관)
              / B(통과했으나 세그먼트·지연 민감)
              / C(탐색적 — 가설 밖 |t_NW|>3 또는 secondary window만)
  탈락/경고:   (해당 시) microcap-only / tradable 소멸 / 지연 시 소멸
              / 부호 불안정 / attrition 경고(§2.5)
  다음 단계:   acceptance gate 진입 / 변형 재시도(잔차·interaction) / 폐기
```

acceptance gate 진입 조건 (전부 충족):

1. **가설 horizon 구간 내**에서 주 검정 집합 global BH-FDR q<0.10 통과 (§3.2 —
   가설 밖 발견은 \|t_NW\|>3이어도 탐색적 강등, 진입 불가).
2. tradable universe에서 broad 대비 IC가 절반 이상 유지.
3. 유효(`insufficient` 제외) 서브기간 중 과반에서 예상 부호 일치
   (공매도류는 허용 레짐만 계상).
4. 단기(bucket1) 의존 신호는 1일 지연 후에도 유의 유지 — 아니면 "게시시각 확정
   후 재평가"로 보류.
5. corporate-action flag 적용 실행의 결과일 것 (§2.4 — flag 전 실행은 무효).

## 8. 한계와 명시적 비목표

- **단변량 스캔이다** — 피쳐 간 중복(MAX↔IVOL, short flow↔balance 등)은 여기서
  분리하지 않는다. 중복 제거·증분성은 acceptance gate(02 §6.1-6)와 ablation의 몫.
- **거래비용 미반영** — bucket1 신호의 실제 수익성은 비용 차감 후 별도 판정.
- horizon 채택 자체가 in-sample 선택이므로, 최종 성능 주장은 반드시 스캔에서
  제외한 holdout(§2.2 — label 종료일 기준)에서만 한다.
- 생존편향은 완화되지 않은 상태다(master DELISTED 28개) — §2.5는 horizon 간
  비교의 왜곡을 통제할 뿐, 장기 성과의 절대 수준에는 02 §5 Phase 0-7의 경고가
  그대로 적용된다.
- event-time 추론의 issuer-clustering / calendar-time portfolio는 경계선 결과의
  확인 절차로만 두었다 — 전면 도입은 Phase 3(접수시각 dimension)과 함께 재검토.
