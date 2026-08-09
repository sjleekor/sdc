# 04. Horizon Scan Phase A0 상세 구현 계획

- 작성일: 2026-08-01
- 개정: 2026-08-02 rev.3 — 공매도 family의 제한 표본과 exploratory 강등,
  publication-lag 진단 주체, execution variant, common formation/survivor 정의 반영
- 상태: 구현 전 상세 계획
- 기준 문서: [02_feature_candidate.md](02_feature_candidate.md),
  [03_horizon_predictive_power_plan.md](03_horizon_predictive_power_plan.md)
- 대상: Phase A의 price 9개 + flow 8개 family 중 12개 주 검정, 4개 short exploratory,
  1개 reference를 재현 가능하게 실행하기 위한 피쳐·universe·label·통계 기반

## 0. 요약

Phase A0의 목적은 피쳐의 예측력을 판단하는 것이 아니라, **Phase A가 동일한 입력과
판정 규칙으로 재현 가능하게 실행될 수 있는 상태**를 만드는 것이다. 구현 순서는 다음과
같이 고정한다.

```text
계약/config 고정
  → 최신 완전한 sj2_remote raw snapshot 선택 + source별 mart/dataset 격리
  → 공통 거래일 패널 + PIT 주식수/시총
  → corporate-action/공매도 품질 flag
  → broad/tradable universe 2벌
  → feat_price 확장
  → feat_flow 확장
  → label_scan + bucket label
  → market-aware IC/NW 통계 + scan driver 골격
  → 전체 materialize·sanity·dry-run
```

`03` §6.1의 항목 순서와 달리 PIT 주식수와 corporate-action flag를 price/flow보다
먼저 만든다. price의 장기 lookback, flow의 float 분모, 모든 forward label이 이 두
계층에 의존하기 때문이다.

Phase A0 완료 후에도 다음 작업은 수행하지 않는다.

- 실제 17개 family의 예측력 결론 작성
- global BH-FDR에 근거한 채택·폐기 판정
- holdout 공개 또는 평가
- financial/event 8개 family 구현
- interaction 후보 탐색
- PostgreSQL raw 또는 derived fact 쓰기

## 1. 완료 상태와 진입/종료 조건

### 1.1 시작 조건

1. `raw_postgres`에 `source=sj2_remote` snapshot이 하나 이상 존재한다.
2. 선택 가능한 raw snapshot에 `_manifests/_SUCCESS.json`이 존재한다.
3. raw manifest에 최소 A0 입력인 `daily_ohlcv`, `krx_security_flow_raw`,
   `dart_share_count_raw`, `stock_master`, `stock_master_snapshot`,
   `stock_master_snapshot_items`가 있다.

Phase A0/Phase A의 price·flow 경로는 derived mart를 읽지 않으므로 raw와 derived 날짜의
교집합을 시작 조건으로 삼지 않는다. `require_derived=true`는 Phase B 또는 전체 25-family
실행에서만 사용한다. 조건을 만족하는 raw 날짜가 없으면 즉시 실패한다. 이는 `03`의
전역 공통 snapshot 규칙을 A/B 실제 의존성에 맞춰 세분화한 것이므로 구현 시 `03`도 함께
정정한다.

### 1.2 Phase A0 종료 조건

다음 조건을 모두 만족해야 Phase A 실행으로 넘어간다.

1. 실행 시점의 최신 완전한 `sj2_remote` raw snapshot이 자동 선택되고 manifest에 기록된다.
2. feature mart와 model dataset cache가 `(snapshot_date, source)`별로 격리된다.
3. Phase A의 17개 family가 모두 config에 등록된다. global BH 대상 12개 family의
   primary 컬럼은 `ready`, `px_zero_ret_ratio_20d`는 `reference_only`, 공매도 관련
   4개 family는 `exploratory_short_regime`으로 구분한다.
4. `dim_stock_pit_daily`, `dim_price_quality_daily`, broad/tradable universe 2벌,
   `feat_price`, `feat_flow`, `label_scan`이 같은 snapshot에서 materialize된다.
5. corporate-action mask가 feature lookback과 label forward window 양쪽에 적용된다.
6. `label_scan`의 20d 누적 label이 기존 `label_daily` 20d 정의와 mask 적용 전 동일하다.
7. bucket 수익률 항등식, holdout 종료일 경계, (date, market) IC, NW t-stat 단위
   테스트가 통과한다.
8. synthetic end-to-end fixture와 실제 lake smoke run이 모두 통과한다.
9. native/lag1 execution variant와 family별 official variant가 config·feature mart·manifest에
   같은 의미로 기록된다.
10. 실행 manifest에 선택 snapshot, source, config hash, 각 mart row count, 품질 flag
    count, coverage, family별 최초 유효일, 연도별 universe 종목 수가 기록된다.

`corporate-action mask 미적용`, `source가 sj2_remote 아님`, `주 검정 family readiness
미완료` 중 하나라도 해당하면 실행은 `smoke_only=true`로 남기고 Phase A 공식 결과 생성을
막는다. balance 기반 exploratory family의 publication lag가 끝내 확인되지 않은 경우에는
그 family만 `exploratory_blocked_publication_lag`로 남기며 75개 주 검정 실행은 막지 않는다.

## 2. 현재 구현과 gap

| 영역 | 현재 상태 | Phase A0에서 해소할 gap |
|---|---|---|
| snapshot | `LakeConfig` 기본 날짜가 고정값이고 `compute_all`은 raw snapshot 하나만 검사 | A0/A는 최신 완전 raw, B는 raw/derived 공통 날짜를 선택하는 dependency-aware resolver |
| cache | feature mart와 `dataset_dir()` 모두 source 구분이 없음 | 둘 다 `source=<source>` 계층 추가, remote `--features` 가드 해제 |
| 실행 조립 | `compute_all._build_features()`가 calendar, 단일 universe, fin/common view만 생성 | A0 산출물을 의존 순서대로 materialize하는 공통 builder 추가 |
| price | 단순 1/5/20/60d 수익률, 변동성, Amihud, 52주 고점 거리 등만 존재 | reversal, 6-1/12-1 momentum, residual momentum, MAX, IVOL, turnover shock, zero-return ratio 추가 |
| flow | 5/20d raw share 합과 일부 level/change만 존재하며 OHLCV를 결합하지 않음 | volume/float 정규화, 60d, short ratios, days-to-cover, NAT proxy 추가 |
| PIT shares/mcap | 일별 PIT 마트 없음 | 실제 `rcept_no` 접수일을 적용한 issued/float/treasury shares와 근사 시총 일별 마트 추가 |
| universe | 60일 warm-up·1억원 유동성 하한의 단일 universe | broad/tradable 2벌, 20일 거래대금 기준, 현재 상태의 과거 소급 필터 금지 |
| corporate action | 가격/label 오염 방지 flag 없음 | 비정상 가격 jump와 주식수 변화 진단, 모든 관련 window 마스킹 |
| short regime | 공매도 금지·잔고 가용/공개 지연 flag 없음 | config 기반 regime 및 availability 처리 |
| label | 누적 20/5/60d만 지원, label 종료일·bucket 없음 | 9개 horizon, 6개 bucket, 실제 종료일, CA mask, 별도 `label_scan` |
| metrics | date 단일 그룹 IC와 naive t-stat만 존재 | (date, market) IC → 종목 수 가중 daily IC, NW t-stat, raw spread 옵션 |
| scan config | 없음 | 25 family 전체 preregistration + readiness schema와 검증기 추가 |

### 2.1 Phase A family 초기 readiness 예상

아래 분류는 첫 readiness report의 기대값이다. 실제 report는 코드의 컬럼 존재 여부와
의존 mart를 검사해 생성하며 수동으로 `ready`를 강제하지 않는다.

| Family | Primary | 현재 판정 | A0 작업 |
|---|---|---|---|
| 단기 반전 | `px_reversal_5d` | `mart_extension` | 5-session 유효 수익률의 부호 반전 |
| 중기 모멘텀 | `px_mom_12_1` | `mart_extension` | 252→21 session skip-return + CA mask |
| 잔차 모멘텀 | `px_resid_mom_12_1` | `mart_extension` | PIT market-model residual 경로 추가 |
| 52주 고점 | `px_near_52w_high` | `definition_hardening` | valid-session/CA 정책으로 재계산, legacy 컬럼과 차이 진단 |
| MAX | `px_maxret_20d` | `mart_extension` | 20-session 최대 일간 수익률 + CA mask |
| IVOL | `px_idio_vol_60d` | `mart_extension` | 기존 total vol과 분리한 market-model residual std |
| 비유동성 | `px_amihud_20d` | `definition_hardening` | halt/volume=0 제외 및 완전 window 규칙 |
| 거래량 충격 | `px_turnover_shock` | `mart_extension` | 현재 turnover / 과거 60-session median의 log ratio |
| 무변동·무거래 비율 | `px_zero_ret_ratio_20d` | `mart_extension` | filter/reference 컬럼, 주 검정 집합 제외 |
| 외국인 순매수 | `flow_foreign_netbuy_to_volume_20d` | `cross_mart_dependency` | flow × OHLCV 결합, 5/20/60d 비율 |
| 기관 순매수 | `flow_inst_netbuy_to_volume_20d` | `cross_mart_dependency` | 동일 |
| 개인 순매수 | `flow_individual_netbuy_to_volume_20d` | `cross_mart_dependency` | 동일, 양측 검정 유지 |
| 외국인 보유 변화 | `flow_foreign_holding_ratio_chg_20d` | `pit_dependency` | float PIT 정규화 후 변화량 |
| 공매도 강도 | `flow_short_turnover_20d` | `cross_mart_dependency` | short volume / total volume + regime |
| 공매도 잔고 | `flow_short_interest_ratio` | `pit_dependency` | balance / float PIT + 공개 지연 |
| Days to cover | `flow_days_to_cover` | `cross_mart_dependency` | balance / 20d 평균 volume |
| NAT proxy | `flow_nat_proxy_20d` | `pit_dependency` | holding/short-interest abnormal rank 결합, 원 논문식 검증 전 proxy |

마지막 4개 공매도 family(`short_turnover`, `short_interest`, `days_to_cover`, `NAT proxy`)는
컬럼이 구현되어도 Phase A global BH의 primary가 아니다. PIT KOSPI200/KOSDAQ150 과거
구성종목이 없어 2021-05~2023-11 부분 허용 구간을 종목 단위로 안전하게 복원할 수 없고,
120d 공통 formation에서 공식 `allowed` 관측이 2020-03-13 이전 한 블록에 몰리기 때문이다.
readiness는 컬럼 가용성과 검정 역할을 분리해 `ready_exploratory` 또는
`exploratory_blocked_publication_lag`로 표시한다.

기존 `flow_foreign_holding_chg_{5,20}d`는 **보유주식수 level 차분**이다. 이 값을
정규화된 비율 변화로 조용히 재정의하지 않는다. 기존 컬럼은 호환 목적으로 유지하고,
horizon scan config는 의미가 명확한 `flow_foreign_holding_ratio_chg_20d`를 primary로
사용한다. 이에 맞춰 `03`의 축약 컬럼명도 구현 시 함께 정정한다.

## 3. 공통 설계 결정

### 3.1 저장 계층

- PostgreSQL raw table과 `stock_metric_fact`/`common_feature_daily_fact`에는 쓰지 않는다.
- 모든 A0 공유 산출물은 parquet feature mart에 저장한다.
- 목표 경로는 다음과 같다.

  ```text
  data_lake/feature_mart/
    snapshot_date=<selected>/
      source=sj2_remote/
        <mart_name>/part-*.parquet
        _manifests/horizon_scan_A0.json
  ```

- 기존 source 없는 cache는 자동 삭제하거나 이동하지 않는다. 새 경로에서 재생성하고,
  이전 경로는 명시적 정리 작업 전까지 읽지 않는다.
- model dataset도
  `data/datasets/<model_id>/snapshot_date=<date>/source=<source>/`로 분리한다.
- 각 mart는 기존처럼 skip-if-present를 유지하되, manifest의 `config_hash` 또는
  `schema_version`이 달라지면 `--force` 없이 재사용하지 않고 명확한 오류를 낸다.

### 3.2 공통 거래 session 규칙

가격·피쳐·label이 서로 다른 row index를 쓰지 않도록 다음 정의를 한 곳에서 재사용한다.

```text
is_halted = open = 0 AND high = 0 AND low = 0
is_valid_session = NOT is_halted
turnover = CAST(close AS DOUBLE) * CAST(volume AS DOUBLE)
simple_ret = close[t] / close[prev_valid] - 1
log_ret = ln(close[t] / close[prev_valid])
```

- rolling/lag/lead는 종목·시장별 `is_valid_session` row index를 기준으로 한다.
- 원천 grain 보존이 필요한 mart는 계산 후 전체 OHLCV key에 다시 left join한다.
- formation universe는 halt 당일을 제외한다.
- `volume=0`은 ratio 분모와 Amihud 계산에서 제외하며 0으로 대체하지 않는다.
- window가 완전히 채워지지 않은 값은 NULL로 둔다. residual model만 문서에 고정한
  `min_periods`를 별도로 사용한다.

공통 SQL 조각은 price/label/universe 파일에 복제하지 않고 예를 들어
`research/etl/trading_panel.py`에 모은다.

### 3.3 PIT 주식수 정책

`dart_share_count_raw`의 `se='합계'` 행을 기본으로 하고 다음 우선순위를 사용한다.

```text
issued_shares_pit = istc_totqy
float_shares_pit  = distb_stock_co
                    fallback: istc_totqy - tesstk_co
market_cap_pit    = close * issued_shares_pit
```

- `disclosed_date = strptime(left(rcept_no, 8), '%Y%m%d')::date`를 실제 접수일로 사용한다.
  sj2 원본 기준 `rcept_no` 347,223행은 빈 값·형식 오류가 모두 0건이다.
- sj2 `se='합계'` 기준 `stlm_dt→접수일` lag는 사업보고서 median 85일, p95 약
  270일이며 최대 2,875일이다. 고정 +90일은 오른쪽 꼬리와 정정공시에서 look-ahead를
  만들 수 있으므로 정상 경로에 사용하지 않는다.
- 접수시각은 없으므로 `available_from`은 `disclosed_date` **다음 KRX session**으로 둔다.
  합성 lag는 `rcept_no`가 없거나 파싱 불가할 때만 fallback으로 사용하며 현재 예상
  적용 건수는 0이다.
- `period_end`는 100% 채워진 `stlm_dt`를 사용한다. `reprt_code`→달력 분기 매핑은
  비12월 결산법인 오류를 피하기 위해 교차검증/quality flag 용도로만 사용한다.
- `available_from <= trade_date`인 값만 interval/as-of join한다.
- 첫 공시 이전 기간으로 backward-fill하지 않는다.
- 원공시와 정정공시는 각각의 실제 접수일에 새 interval을 시작한다. 각 거래일에서
  이미 접수된 filing 중 `period_end`가 가장 최신인 보고서를 먼저 고르고, 같은
  `(ticker, period_end, reprt_code)` 안에서는 그 시점까지 접수된 최신 `rcept_no`를
  선택한다. 따라서 미래 정정공시가 과거로 소급되지 않고, 오래된 period의 늦은 정정이
  더 최신 period의 주식수를 덮어쓰지도 않는다.
- `tesstk_co IS NULL`을 자기주식 0으로 간주하지 않는다
  (`treasury_null_policy: preserve_null`). 따라서 `distb_stock_co`가 없을 때의
  `issued - treasury` fallback은 두 값이 모두 유효할 때만 적용한다.
- `distb_stock_co <= 0`, `issued_shares_pit <= 0`, `float > issued`는 NULL 처리하고
  별도 quality flag를 남긴다. sj2 `se='합계'` 94,455행의 기준치는 treasury NULL
  32,964, issued NULL 5,454, float NULL 5,385, `float>issued` 12,
  `float<=0` 0건이며 smoke report가 이 규모와 크게 달라지면 조사한다.
- DART 이전 기간에는 size segment와 float 기반 피쳐가 NULL일 수 있다. 이 기간을
  미래 공시값으로 채우지 않으며 결과에서 `insufficient`로 처리한다.
- readiness에는 family별 `effective_sample_start`를 기록한다. DART float family는
  2015년 접수 이후, short-balance 결합 family는 2016-06-30 이후와 publication lag를
  모두 만족한 첫 날짜부터 유효하다.

목표 mart `dim_stock_pit_daily`의 최소 컬럼은 다음과 같다.

```text
trade_date, ticker, market,
issued_shares_pit, treasury_shares_pit, float_shares_pit, market_cap_pit,
shares_available_from, shares_age_days, shares_source,
shares_is_available, shares_invalid_flag,
treasury_missing_flag, float_fallback_used
```

### 3.4 corporate-action 최소 안전 정책

A0에서는 adjusted close를 새로 수집하지 않고 보수적인 **window 전체 제외 방식**을
채택한다. flag는 alpha 입력으로 사용하지 않고 데이터 품질 마스크로만 사용한다.

1. 유효 session 간 `close_ratio`와 log/simple return을 계산한다.
2. 거래일별 가격제한폭 regime으로 설명하기 어려운 jump를
   `ca_price_jump_suspect`로 표시한다.
3. DART issued/float shares의 큰 변화와 가격 ratio의 역방향 대응 여부를
   `ca_share_change_confirmed` 진단으로 기록한다.
4. 공식 mask는 누락을 줄이기 위해 suspect도 포함한다.
5. feature는 lookback window 안에 action이 하나라도 있으면 NULL이다.
6. label은 `(t, t+h]` 또는 bucket `(t+h1, t+h2]` 안에 action이 하나라도 있으면
   NULL이다.

임계값은 코드에 흩어 쓰지 않고 config에 사전 고정한다. 최초 smoke run에서 flag
건수와 대표 종목을 사람이 확인할 수는 있지만, IC 결과를 보고 임계값을 바꾸지 않는다.
가격 jump 단독 mask와 share-confirmed subset의 건수·IC 차이는 품질 진단으로만 남긴다.

A0 v0는 2015-06-15 이전 ±15%, 이후 ±30%인 가격제한폭 regime과
`price_limit_multiplier=1.10`을 사용한다. 즉 suspect 기준은 단일 35%가 아니라
`abs(simple_ret) > applicable_limit × 1.10`이다. 신규상장 첫 session처럼 이전 종가가
없거나 가격제한폭 적용 예외를 식별할 수 있는 행은 price-only 판정을 하지 않는다.
정리매매 등 예외 여부를 raw에서 식별하지 못한 행은 `ca_rule_applicability_unknown`으로
표시해 diagnostic에 포함하고 보수적으로 mask한다.

주식수 변화율 절대값 25% 이상과
`close_ratio × share_ratio ∈ [0.8, 1.25]`를 share confirmation 기준으로 둔다.
두 공시가 포괄하는 기간 안의 price-jump 후보 중 역비율 정합성이 가장 높은 날짜에만
confirmation을 부여한다. 이는 retrospective **제외 규칙**일 뿐 PIT 예측 피쳐가
아니다. 실제 표본 진단으로 임계값을 바꿔야 하면 config version을 올리고 A0 전체를
다시 생성한다.

목표 mart `dim_price_quality_daily`의 최소 컬럼은 다음과 같다.

```text
trade_date, ticker, market, valid_session_idx,
is_halted, volume_zero, simple_ret, log_ret,
ca_price_jump_suspect, ca_share_change_confirmed, ca_mask,
ca_rule_applicability_unknown, ca_event_cumulative
```

`ca_event_cumulative`은 임의 구간에 action이 포함되는지 O(1) 차분으로 판단하기 위한
누적 count다. 이 값 자체는 모델이나 IC 입력 컬럼으로 노출하지 않는다.

### 3.5 공매도 availability와 regime

- `short_selling_balance_quantity`는 2016-06-30 이전에 NULL이며 0으로 채우지 않는다.
- 측정일과 공개일이 다르므로 `short_balance_lag_sessions`를 config에 고정한다.
  A0 v0는 보수적으로 2 KRX session을 사용한다. raw payload/source 문서 진단이 이와
  다르면 IC 실행 전에 config version을 올려 수정한다.
- A0-3이 publication-lag 진단의 명시적 owner다. KRX source BLD의 기준일 의미, 공식
  공개 주기, raw payload의 날짜 필드를 대조한 evidence artifact를 만들고, 확인된 lag만
  official metadata로 승격한다.
- source 진단이 완료되기 전 balance 기반 family(`short_interest`, `days_to_cover`,
  NAT)는 readiness를 `exploratory_blocked_publication_lag`로 유지한다. 이 상태는 해당
  exploratory 결과만 막고 Phase A의 75개 primary scan은 막지 않는다.
- 금지/부분 허용 구간은 날짜 interval config로 관리하고 다음 컬럼을 출력한다.

  ```text
  short_regime: allowed | partial | banned
  short_balance_is_available
  short_balance_available_from
  ```

- banned 구간의 0/NULL을 관측 가능한 정상 signal로 간주하지 않는다. 공매도 4개
  exploratory family는 `allowed`만 분석하고 partial은 별도 coverage segment로 남긴다.

sj2 raw calendar에서 확인한 regime별 세션 수는 다음과 같다.

| 구간 | 세션 | Phase A 공통 120d formation 기여 |
|---|---:|---|
| 2014-06-01~2020-03-13 allowed | 1,420 | 기여 |
| 2020-03-16~2021-05-02 banned | 280 | 제외 |
| 2021-05-03~2023-11-03 partial | 619 | PIT 구성종목 부재로 공식 제외 |
| 2023-11-06~2025-03-28 banned | 339 | 제외 |
| 2025-03-31~2025-07-31 resumed | 84 | 공통 formation 상한 뒤라 0일 |

balance 가용 이후 allowed 구간은 2016-06-30~2020-03-13의 909세션뿐이며 120d
공통 survivor를 적용하면 약 790 formation date만 남는다. 현재 DB·lake에는 과거
KOSPI200/KOSDAQ150 구성종목 테이블이 없으므로 current constituent를 과거 partial 구간에
소급 적용하지 않는다. 이 제한 때문에 `flow_short_turnover_20d` 10셀과 balance/NAT
3개 family 18셀, 합계 28셀은 Phase A primary에서 exploratory로 사전 강등한다. Phase A의
global BH 주 검정 수는 103이 아니라 75다.

### 3.6 universe 정책

두 universe는 서로 다른 테이블로 materialize하되 동일한 flag schema를 가진다.

| 규칙 | broad | tradable |
|---|---:|---:|
| 유효 OHLCV row | 필수 | 필수 |
| halt 당일 제외 | 필수 | 필수 |
| warm-up | 60 중 40 valid | 60 중 40 valid |
| 최근 20일 평균 거래대금 | 기록만 | 1억원 이상 |
| 종가 하한 | 기록만 | 1,000원 이상 |
| PIT membership/status | 가용 구간만 적용 | 가용 구간만 적용 |
| 관리종목 | 기록/제외 | 기록/제외 |
| label 존재 여부 | 별도 flag | 별도 flag |

현재 `stock_master.status`는 PIT가 아니므로 `DELISTED` 29개 종목의 과거 이력을 통째로
제외하지 않는다. `first_seen_date`도 2026-04 이후 관측에 치우쳐 과거 listing date로
사용할 수 없다. `stock_master_snapshot` 56회의 범위도 2026-04-10~2026-07-31뿐이므로
주 분석기간의 PIT universe를 복원하지 못한다.

따라서 A0 v0의 과거 universe base는 **그 날짜에 실제 OHLCV row가 존재하는 종목**으로
두고 current status를 소급 적용하지 않는다. snapshot membership은 실제 coverage 구간의
교차검증에만 사용한다. `last_seen_date`를 사용할 때도 그 날짜 이후만 제외하고 이전
이력을 삭제하지 않는다. manifest에는 연도별 종목 수, 최초/최종 관측일, snapshot
coverage 구간, `membership_reconstruction_available`을 기록한다.

이 정책은 기존 표본에 있는 29개 종목을 보존할 뿐, 수집 대상에서 처음부터 빠진 과거
상장폐지 종목을 복구하지 못한다. sj2 기준 19년 OHLCV의 distinct ticker 2,792개가 현재
`stock_master` 2,792개와 모두 일치하므로 생존편향은 **미해결 핵심 위험**이다. 특히
60/120d IC 절대 수준과 survivor sample은 낙관적일 수 있으며 Phase A 결론 카드와 인계
manifest에 항상 경고를 붙인다.

또한 raw schema에는 신뢰 가능한 관리종목 전용 flag가 없다. `stock_master.status=ACTIVE`를
관리종목 아님으로 해석하지 않는다. A0 v0는 `management_filter_available=false`를
manifest에 남긴다. 관리종목 source가 추가되기 전 tradable 결과에는 이 제한을 명시하며,
horizon screening을 넘어 실제 거래가능성을 주장하는 근거로 사용하지 않는다.

기존 `dim_universe_daily` 소비자를 깨지 않기 위해 다음과 같이 구성한다.

- 신규: `dim_universe_broad_daily`, `dim_universe_tradable_daily`
- 호환 alias: 기존 `dim_universe_daily`는 현재 기본 `UniverseFilter` 의미를 유지
- horizon scan은 alias를 사용하지 않고 신규 두 mart를 명시적으로 join
- `UniverseFilter`에 `liquidity_window`, `min_close_krw`, membership 정책을 모두 노출

## 4. 작업 패키지와 구현 순서

### A0-0. config·readiness 계약 고정

**목적**: 결과를 보기 전에 검정 집합, 산식 버전, 임계값과 의존성을 기계 판독 형태로
고정한다.

구현:

1. `research/analysis/horizon_scan_config.yaml`을 추가한다.
2. YAML parser는 `PyYAML`로 고정하고 research dependency에 명시한다.
3. 현재 `.gitignore`가 신규 `research/analysis/*`를 무시하므로 horizon scan의 Python
   진입점·readiness script·YAML config만 명시적으로 allowlist한다. 생성 결과인
   `research/output/`은 계속 local artifact로 무시한다.
4. 25개 family 전체를 등록하고 Phase A 17개와 Phase B 8개를 `phase`로 구분한다.
5. family마다 `features`, primary/secondary role, expected sign, hypothesis horizons,
   FDR 포함 여부, readiness dependency, native/lag1 column mapping,
   `official_feature_variant`, availability evidence를 선언한다.
6. `price_quality`, universe, publication lag, regime interval, window minimum 관측 수를
   같은 config의 versioned section에 둔다.
7. config validation과 readiness report generator를 추가한다.

최초 config의 공통 파라미터는 아래 값으로 고정한다.

```yaml
schema_version: 2
horizons: [1, 2, 3, 5, 10, 20, 40, 60, 120]
buckets: [[0, 5], [5, 10], [10, 20], [20, 40], [40, 60], [60, 120]]
quality:
  price_limit_regimes:
    - {start: 2014-06-01, end: 2015-06-14, limit: 0.15}
    - {start: 2015-06-15, end: null, limit: 0.30}
  price_limit_multiplier: 1.10
  ca_abs_share_change: 0.25
  ca_ratio_product_range: [0.8, 1.25]
  short_balance_lag_sessions: 2
shares:
  disclosed_date_source: rcept_no_yyyymmdd
  availability: next_krx_session
  period_end_source: stlm_dt
  fallback_lag_days: {annual: 90, quarterly: 45}
  treasury_null_policy: preserve_null
universe:
  warmup_window: 60
  warmup_min_valid: 40
  tradable_liquidity_window: 20
  tradable_min_turnover_krw: 100000000
  tradable_min_close_krw: 1000
price:
  market_model_window: 252
  resid_mom_model_min_valid: 252
  resid_mom_require_complete_history: true
  idio_model_min_valid: 126
  idio_vol_window: 60
  idio_vol_min_valid: 40
  turnover_shock_window: 60
  turnover_shock_include_current: false
flow:
  require_complete_window: true
  nat_feature: flow_nat_proxy_20d
  short_family_role: exploratory_short_regime
  partial_membership_policy: unavailable_no_pit_constituents
execution:
  variants: [native_t, lag1]
  lag1_basis: prior_valid_session
  price_default_official_variant: native_t
  flow_unverified_same_day_variant: lag1
sample:
  start: 2014-06-01
  holdout_start: 2025-08-01
  holdout_boundary: label_end_date
  common_formation_horizon: 120
  common_survivor_horizon: 120
  common_survivor_quality_independent: true
  label_population: all_quality_valid_sessions
  period_sets:
    common:
      - {id: 2014_2016, start: 2014-06-01, end: 2016-12-31}
      - {id: 2017_2019, start: 2017-01-01, end: 2019-12-31}
      - {id: 2020_2021, start: 2020-01-01, end: 2021-12-31}
      - {id: 2022_2023_10, start: 2022-01-01, end: 2023-10-31}
      - {id: 2023_11_common_end, start: 2023-11-01, end: common_formation_end}
    available:
      - {id: 2014_2016, start: 2014-06-01, end: 2016-12-31}
      - {id: 2017_2019, start: 2017-01-01, end: 2019-12-31}
      - {id: 2020_2021, start: 2020-01-01, end: 2021-12-31}
      - {id: 2022_2023_10, start: 2022-01-01, end: 2023-10-31}
      - {id: 2023_11_2025_03, start: 2023-11-01, end: 2025-03-31}
      - {id: 2025_04_holdout, start: 2025-04-01, end: horizon_eligible_end}
stats:
  ic_unit: date_market
  daily_market_aggregation: n_obs_weighted
  nw_lag_cumulative: h_minus_1
  nw_lag_bucket: width_minus_1
  nw_gap_policy: calendar_session_distance
  global_bh_q: 0.10
  exploratory_abs_t_nw: 3.0
  min_dates_per_cell: 60
  min_names_per_date_market: 20
  min_names_for_spread: 50
  quantile_count: 5
  segment_bin_count: 3
  nonoverlap_min_dates: 20
  nonoverlap_inference: exact_sign_test
  min_events_per_cohort: 30
  min_event_cohorts: 8
decision:
  half_life_fraction: 0.50
  isolated_spike_neighbor: adjacent_registered_cell_same_scan_type
  tradable_min_abs_ic_retention: 0.50
  delay_min_abs_ic_retention: 0.50
  delay_confirm_p_nw: 0.05
  available_sign_flip_max_grade: C
  insufficient_offset_max_grade: B
  sparse_primary_grid_families: [px_amihud_20d]
  evidence_grade:
    evaluation_order: [R, C, A, B, D]
    A: screen_pass_and_no_core_warning_and_all_offsets_evaluable
    B: screen_pass_with_nonfatal_warning
    C: exploratory_or_secondary_or_available_sign_flip
    D: no_signal_or_wrong_sign_or_robustness_fail
    R: reference_only
placebo:
  cross_sectional_repeats: 100
  cross_sectional_block: date_market
  temporal_long_cell_repeats: 100
  temporal_min_shift_sessions: 120
  temporal_p_max: 0.10
  seed: 20260801
```

main cell의 최소 날짜 수는 `max(min_dates_per_cell, nw_lag + 2)`로 유도한다. 따라서
cumulative 120d는 121일이 필요하다. non-overlap 경로만 별도
`nonoverlap_min_dates=20`과 exact sign test를 사용한다.

공매도 regime의 정확한 일자와 종목 범위(KOSPI200/KOSDAQ150 부분 허용 포함)는 기존
문서의 월 단위 표현을 그대로 코드화하지 않는다. KRX/금융위원회 이력과 대조한 interval
목록을 config에 넣고, 각 경계일 synthetic test가 추가된 뒤 config를 freeze한다.

필수 validation:

- family 이름 중복 없음
- family당 primary feature 정확히 1개
- primary/exploratory horizon 중복 없음
- family별 `primary_horizon_set`/`exploratory_horizon_set`이 모두 명시됨
- horizon과 bucket 경계가 `03`의 고정 grid와 일치
- 양측 검정 family 외 expected sign 누락 없음
- FDR 제외 reference/filter family가 주 검정 집합에 들어가지 않음
- 공매도 4개 family가 `exploratory_short_regime`, Phase A primary hypothesis가 75개
- 필요한 mart/column이 없으면 `ready`가 될 수 없음
- family마다 native/lag1 column mapping과 official variant 근거가 존재
- common survivor가 CA/quality mask와 독립적으로 정의됨
- 기간 set, bin/quantile 수, half-life, isolated-spike, evidence-grade 규칙이 hash에 포함됨
- sample/stats/quality 파라미터가 모두 config hash에 포함됨
- config hash를 안정적으로 계산할 수 있음

산출물:

```text
research/analysis/horizon_scan_config.yaml
research/analysis/horizon_scan_readiness.py
research/output/horizon_scan/readiness_matrix.parquet
research/output/horizon_scan/readiness_matrix.md
```

완료 기준: A0 시작 시 예상대로 price/flow는 extension/dependency, financial/event는
`phase_b_blocked`로 보고된다. A0 종료 시 Phase A global BH 대상 12개 family는 `ready`,
zero-return은 `reference_only`, 공매도 4개는 `ready_exploratory` 또는 balance 진단 미완료
시 `exploratory_blocked_publication_lag`가 된다. registry에서 도출한 primary hypothesis
수는 정확히 75다.

### A0-1. snapshot resolver와 mart namespace 수정

**목적**: 특정 날짜 고정과 local/sj2 cache 혼용을 제거하되, phase가 읽지 않는 derived
mart 때문에 최신 raw 사용이 막히지 않게 한다.

구현:

1. `research/etl/snapshot.py`에 snapshot 후보 탐색·검증 함수를 추가한다.
2. resolver는 `required_inputs`를 받아 A0/A에서는 검증된 raw 날짜의 최댓값을 선택한다.
   Phase B에서는 `require_derived=true`로 raw/derived 교집합을 적용한다.
3. raw `_SUCCESS.json`의 완료 여부와 A0 필수 table 집합을 검사한다.
4. `require_derived=true`일 때만 `stock_metric_fact`, `common_feature_daily_fact` parquet
   존재 여부를 검사한다.
5. `mart_root()`와 `LakeConfig.dataset_dir()`에 `source=<config.source>`를 포함한다.
6. `bin/parquet-compute-all.sh`의 remote `--features` 차단 가드를 제거하고 source 분리
   회귀 테스트를 추가한다.
7. dataset/manifest에도 source와 resolver 선택 근거를 기록한다.
8. CLI의 수동 snapshot override는 테스트·재현 전용으로만 허용하고 공식 scan에서는
   `auto_select=true`가 아니면 `smoke_only`로 표시한다.

테스트:

- A0에서 raw `{d1,d3}`, derived `{d2}`이면 d3 선택
- Phase B에서 raw `{d1,d3}`, derived `{d2,d3}`이면 공통 d3 선택
- required input의 marker/table이 부족하면 실패
- 같은 날짜 local/sj2 mart가 서로 다른 경로를 사용
- 같은 날짜 local/sj2 model dataset이 서로 다른 경로를 사용
- 기존 source 없는 cache를 읽지 않음

완료 기준: 선택된 config만 아래 작업에 전달되며 하위 builder가 자체 default 날짜를
다시 만들지 않는다.

### A0-2. 공통 거래 패널과 PIT 주식수/시총

**목적**: 모든 downstream mart가 같은 valid-session index와 주식수 분모를 사용하게
한다.

구현:

1. 공통 valid-session SQL/helper를 추가한다.
2. DART share total row dedup, `rcept_no` 접수일, 다음 KRX session available-from을
   구현한다.
3. disclosure interval을 만든 뒤 OHLCV key에 PIT join한다.
4. issued/treasury/float shares와 `market_cap_pit`을 계산한다.
5. pre-DART·invalid·stale coverage flag를 보존한다.
6. `dim_stock_pit_daily`를 materialize한다.

테스트:

- 접수일 당일에는 보이지 않고 다음 KRX session부터 보임
- `rcept_no` 결측 fixture에서만 quarterly +45일/annual +90일 fallback
- `stlm_dt`가 reprt-code 달력 분기와 다른 비12월 결산 fixture 보존
- 후속 정정공시가 원공시 이전 거래일에 소급되지 않음
- 오래된 period의 늦은 정정이 최신 period 값을 덮어쓰지 않음
- 다음 report available date부터 이전 값이 교체됨
- 첫 공시 이전 backward-fill 없음
- `distb_stock_co` 우선, issued−treasury fallback
- treasury NULL은 0으로 대체되지 않으며 fallback 미사용
- 잘못된 float/issued 관계는 NULL + flag
- `close * issued_shares`가 DOUBLE overflow 없이 계산됨

완료 기준: 각 key가 최대 1행이고 `available_from <= trade_date` 위반이 0건이다.

### A0-3. price quality·corporate action·short regime

**목적**: 공식 판정 전에 feature/label 오염 window를 차단하고 공매도 구조 단절을
표시한다.

구현:

1. `dim_price_quality_daily` builder를 추가한다.
2. 가격제한폭 regime·적용 예외를 반영해 비정상 jump suspect와 share-change
   confirmation을 분리해 계산한다.
3. `ca_mask`와 누적 event count를 생성한다.
4. short regime interval과 balance availability helper를 추가한다.
5. KRX balance source BLD·공식 제공 규칙·raw payload의 기준일을 대조해 measurement date와
   공개 가능일을 확정하고 `short_balance_publication_lag.json` evidence를 생성한다.
6. 진단이 확정되면 config lag와 evidence hash를 연결한다. 확정하지 못하면 balance family를
   `exploratory_blocked_publication_lag`로 유지하고 임의 lag로 ready 처리하지 않는다.
7. PIT index constituent가 없음을 검사하고 partial interval을 official allowed로 복원하지
   않는다.
8. flag count, ticker count, 연도별 count와 regime별 session count를 A0 manifest에 기록한다.
9. 실제 lake에서 대표 flag 표본을 별도 diagnostic parquet로 출력한다.

테스트:

- synthetic 1:5 split에서 action 확인 및 양쪽 관련 window mask
- 2015-06-15 전후의 ±15%/±30% regime 경계
- 신규상장 첫 session은 price-only suspect에서 제외
- 정상적인 작은 가격 변화는 mask되지 않음
- halt stale close가 action으로 오인되지 않음
- CA가 feature lookback 밖이면 feature 유지
- CA가 label `(t,t+h]` 안에 있을 때만 해당 horizon NULL
- short availability 이전은 NULL, lag 이후 노출
- publication-lag evidence와 config hash 연결
- regime interval 양 끝 경계
- PIT constituent가 없을 때 partial row가 official eligibility에 들어가지 않음

완료 기준: flag 전용 smoke label과 mask 적용 label의 row/coverage 차이가 manifest에
설명되고, mask 없는 결과는 공식 모드에서 생성할 수 없다. publication-lag 진단의
`verified/unresolved` 상태와 공매도 family별 readiness가 별도 산출물로 남는다.

### A0-4. broad/tradable universe 2벌

**목적**: horizon scan의 microcap/비유동성 착시를 동일한 규칙으로 비교한다.

구현:

1. `UniverseFilter`를 broad/tradable 두 named spec으로 확장한다.
2. 최근 20 valid-session 평균 turnover, 종가, halt, warm-up, membership/상태를 각각
   boolean flag로 출력한다.
3. `label_ok_1d`부터 `label_ok_120d`까지 미래 가격 존재 여부는 별도 컬럼으로 두고
   formation eligibility에 섞지 않는다.
4. 두 mart를 별도 materialize하고 row-level exclusion reason을 유지한다.
5. broad→tradable 감소율을 날짜·시장별로 산출해 비정상 급변을 검사한다.
6. current `stock_master.status`를 과거 전 기간에 적용하지 않고, 연도별 종목 수와
   membership source/coverage를 manifest에 기록한다.

테스트:

- warm-up 경계와 halt 제외
- 20일 liquidity 경계값 바로 위/아래
- broad에는 남고 tradable에서만 제외되는 저유동 종목
- future price 여부가 `in_universe`를 바꾸지 않음
- DELISTED 종목도 `last_seen_date` 이전 과거 이력은 보존
- snapshot coverage 밖에서 `first_seen_date`를 listing date로 오용하지 않음
- 연도별 distinct ticker 추이가 manifest와 일치

완료 기준: tradable이 broad의 부분집합이고 역포함 위반이 0건이다.

### A0-5. `feat_price` 확장

**목적**: Phase A price 9개 family의 primary/secondary 컬럼을 사전 등록 산식대로
제공한다.

구현 컬럼:

```text
px_reversal_5d
px_mom_6_1
px_mom_12_1
px_resid_mom_12_1
px_near_52w_high
px_maxret_20d
px_idio_vol_60d
px_turnover_shock
px_zero_ret_ratio_20d
```

세부 규칙:

- `px_reversal_5d = -sum(log_ret, trailing 5 valid sessions)`
- `px_mom_6_1 = ln(close[t-21] / close[t-126])`
- `px_mom_12_1 = ln(close[t-21] / close[t-252])`
  (`t-k`는 모두 valid-session index 기준)
- market return은 `(trade_date, market)` 내 유효 종목의 동일가중 return으로 계산한다.
- residual return `e[t]`은 `[t-252,t-1]`의 252 valid session으로 추정한
  `r_i = alpha + beta × r_market + e`의 alpha/beta를 당일 수익률에 적용해 계산한다.
  `px_resid_mom_12_1`은 `e[t-252]..e[t-21]`의 합이다. 당일 또는 미래 수익률로
  coefficient를 다시 추정하지 않는다. 첫 유효값에는 회귀 history와 residual 집계
  history를 합쳐 약 504 valid session이 필요하므로 readiness에 별도 최초 유효일을 남긴다.
- `px_idio_vol_60d`는 residual return의 60-session 표준편차이며 기존
  `px_vol_60d`와 별도 컬럼이다.
- `px_near_52w_high = close / max(close, 252 valid sessions) - 1`로 새로 계산한다.
  기존 `px_dist_52w_high`는 halt row 포함 252-row 정의이므로 alias로 취급하지 않고
  legacy 컬럼으로 유지한다.
- `px_maxret_20d = max(simple_ret, trailing 20 valid sessions)`로 원 논문의 simple
  return 정의를 따른다. log return은 rank가 같더라도 별도 경제 단위이므로 사용하지 않는다.
- `px_turnover_shock = ln(turnover[t] / median(turnover[t-60:t-1]))`로 당일을 제외한
  직전 60 valid-session median을 사용한다.
- `px_zero_ret_ratio_20d = mean(log_ret=0 OR volume=0, 20 sessions)`이며 alpha가 아닌
  reference/filter다. halt row는 valid-session 계산에서 이미 빠지므로 명칭과 설명은
  "거래정지 비율"이 아니라 "무변동·무거래 비율"로 통일한다.
- 각 피쳐의 lookback에 `ca_mask`가 있으면 해당 값은 NULL이다.
- config에 등록된 Phase A primary/secondary native 컬럼마다 이전 **valid session** 값을
  `<column>_lag1`로 함께 materialize한다. halt calendar row를 1일로 세지 않으며 variant
  column mapping과 official 선택 근거를 manifest에 넣는다.
- 기존 컬럼을 제거하지 않고 의미가 잘못된 컬럼만 deprecated metadata로 표시한다.

테스트:

- 손계산 ramp/reversal/MAX/52주 고점/turnover shock
- 21-session skip이 현재 1개월을 포함하지 않음
- 126/252-session warm-up 경계
- residual momentum의 약 504-session effective warm-up과 최초 유효일
- 한 market만 바꿔도 다른 market의 residual 계산이 변하지 않음
- future return을 변경해도 과거 feature가 변하지 않는 look-ahead canary
- native/lag1이 직전 valid session 기준으로 정확히 한 칸 이동
- CA가 포함된 lookback만 NULL
- 새 `px_near_52w_high`와 legacy `px_dist_52w_high`의 차이가 halt/CA/window 정책으로
  설명됨
- 기존 대표 컬럼 회귀 테스트 유지

완료 기준: Phase A price primary/secondary의 native/lag1 컬럼이 `DESCRIBE feat_price`에
모두 있고, finite/NULL/coverage report가 생성된다.

### A0-6. `feat_flow` 확장

**목적**: raw share level을 직접 비교하지 않고 거래량·float로 정규화한 flow 피쳐를
제공한다.

builder 입력을 다음과 같이 확장한다.

```text
krx_security_flow_raw
+ daily_ohlcv 또는 공통 거래 패널
+ dim_stock_pit_daily
+ short availability/regime
```

구현 컬럼:

```text
flow_{foreign,inst,individual}_netbuy_to_volume_{5,20,60}d
flow_foreign_holding_ratio
flow_foreign_holding_ratio_chg_{5,20,60}d
flow_short_turnover_20d
flow_short_interest_ratio
flow_short_interest_ratio_chg_20d
flow_days_to_cover
flow_nat_proxy_20d
flow_short_balance_is_available
flow_short_regime
```

세부 규칙:

- net-buy strength는 `sum(net_buy_volume,h) / sum(total volume,h)`이다.
- short turnover는 `sum(short_selling_volume,20) / sum(total volume,20)`이다.
- window 내 필요한 metric이 빠진 날을 0으로 보지 않는다. `count_valid`가 config의
  최소값보다 작으면 NULL이며 coverage count를 함께 계산한다.
- holding/short-interest 분모는 `float_shares_pit`, fallback 여부는 별도 flag로 남긴다.
- balance level은 publication lag 이후에만 as-of 사용한다.
- days-to-cover는 `short_balance / mean(volume,20)`이다.
- NAT proxy v0는 `rank(AHF_20) - rank(ASI_20)`이며 rank 단위는 `(date, market)`이다.
  처음부터 `flow_nat_proxy_20d`, `formula_version=proxy_v0`로 고정한다. 원 논문식 검증이
  끝난 뒤에만 별도 `flow_nat_20d`를 추가하며 기존 proxy 컬럼의 이름이나 의미를 바꾸지
  않는다. `03`의 NAT primary도 그때까지 proxy 컬럼으로 정정한다.
- 공매도 banned 구간의 값은 공식 scan input에서 NULL/제외하고 regime을 남긴다.
- config에 등록된 Phase A flow primary/secondary native 컬럼의 `<column>_lag1`을 이전
  valid session 기준으로 materialize한다. Phase A가 실행 중 lag1을 새로 만들지 않게 한다.
- 기존 raw sum·z-score 컬럼은 호환 목적으로 유지한다.
- 각 flow family의 실제 최초 non-null date와 유효 date 수를 readiness에 기록한다.
  float 기반 family는 DART 접수 시작 이후, balance 기반 family는 2016-06-30과 공개
  lag 이후에만 시작한다.

테스트:

- KRX-first dedup 회귀 테스트
- 5/20/60d 분자·분모 손계산
- 일부 flow date 누락 시 0 대체되지 않음
- float fallback과 분모 0 처리
- balance publication lag 경계
- banned/partial/allowed 처리
- native/lag1 valid-session 이동과 halt 경계
- NAT proxy의 date×market rank와 두 market 격리
- 결과 grain `(trade_date,ticker,market)` 유일성

완료 기준: 8개 flow family의 native/lag1 ratio가 비정상 무한대 없이 coverage report에
포함된다. non-short 4개는 primary `ready`, short 4개는 `ready_exploratory` 또는
`exploratory_blocked_publication_lag`로 역할과 가용 상태를 분리해 표시한다.

### A0-7. `label_scan`과 bucket label

**목적**: 누적 효과와 영향 시점을 동일한 forward-price 기반으로 생성한다.

구현:

1. 기존 `LabelSpec`과 forward CTE를 재사용하되 scan 전용 spec/table을 분리한다.
2. 누적 horizon을 `(1,2,3,5,10,20,40,60,120)`으로 고정한다.
3. 각 horizon에 실제 `label_end_date_{h}d`를 출력한다.
4. bucket `(0,5], (5,10], (10,20], (20,40], (40,60], (60,120]`을 만든다.
5. bucket별 raw return, eqw market excess, `(date,market)` rank를 별도로 계산한다.
6. CA가 포함된 forward 구간은 누적/bucket label을 NULL 처리한다.
7. label 종료일 기반 holdout eligibility와 120d 공통 formation/survivor flag를 출력한다.

두 공통 flag는 다음처럼 분리한다.

```text
common_formation_120d (날짜 수준)
  = 공통 KRX calendar에서 trade_date의 120번째 다음 session이 holdout_start보다 앞섬

common_survivor_120d (종목 수준)
  = common formation date에서 해당 종목의 120번째 미래 valid-session endpoint 가격이
    존재하고 실제 label_end_date_120d가 holdout_start보다 앞섬
```

두 flag 모두 **CA/quality mask 적용 전** endpoint 존재 여부로 계산한다. 향후 120일 안에
기업행위가 있다는 이유로 survivor=false가 되어서는 안 된다. CA/quality는 이후 raw return을
NULL 처리하고 `label_coverage`를 낮추는 별도 축이다. Phase A official common sample은 두
flag를 모두 요구한 뒤 해당 horizon의 quality-valid label을 적용한다.

label 모집단과 mask 순서는 다음으로 고정한다.

1. `label_scan`은 broad/tradable과 무관한 **전체 valid-session 가격 패널**에서 만든다.
2. horizon/bucket별 raw forward return을 계산한다.
3. 해당 forward window의 CA/quality mask를 먼저 적용해 invalid raw return을 NULL로 만든다.
4. 같은 `(date, market)`의 남은 non-null raw return 모집단으로 eqw benchmark와 rank를
   계산한다. 따라서 benchmark·excess·rank의 모집단이 항상 같다.
5. broad/tradable은 label을 다시 만들지 않고 IC 산출 단계에서 formation row를 필터한다.

이 순서로 두 universe가 동일 label을 공유하고, 각 horizon/bucket의 unmasked 모집단에서
excess 평균 0 항등식이 성립한다. 기존 `label_daily`와의 20d 동치 테스트는 quality mask를
끄고 전체 valid-session 모집단을 사용한 compatibility mode에서 수행한다.

권장 컬럼 규칙:

```text
fwd_ret_<h>d
label_end_date_<h>d
bench_ret_<h>d
raw_label_<h>d
y_rank_<h>d

bucket_ret_<h1>_<h2>d
bucket_bench_ret_<h1>_<h2>d
raw_bucket_label_<h1>_<h2>d
y_rank_bucket_<h1>_<h2>d
bucket_end_date_<h1>_<h2>d
```

bucket 산식은 반드시 다음 항등식을 따른다.

```text
bucket_ret(h1,h2]
  = (1 + fwd_ret_h2) / (1 + fwd_ret_h1) - 1
  = close[t+h2] / close[t+h1] - 1
```

`h1=0`이면 `fwd_ret_0=0`, 기준 가격은 formation close다. bucket excess는 누적
excess끼리 차분하지 않고 bucket raw return의 시장 평균을 새로 계산한다.

테스트:

- 위 bucket 항등식
- halt session을 건너뛴 h-step end date
- bucket excess의 date×market 평균 0
- NULL forward return에 rank가 부여되지 않음
- config의 `holdout_start=2025-08-01`에 대한 `label_end_date < holdout_start` 경계
- 날짜 수준 common formation과 종목 수준 common survivor의 차이
- CA가 있는 120d survivor도 endpoint가 있으면 survivor=true이고 label만 NULL
- CA가 누적 및 해당 bucket에만 마스킹됨
- mask 전 `y_rank_20d`와 기존 `label_daily`의 key/value 동일
- 동일 key의 broad/tradable label 값 동일

완료 기준: `label_scan`은 기존 `label_daily`와 별도 materialize되고 horizon 간 표본
비교용 end-date/survival 정보를 제공한다.

### A0-8. 통계 함수와 scan driver 골격

**목적**: Phase A 실행 코드가 잘못된 추론 단위를 사용할 여지를 없앤다.

`research/etl/metrics.py` 확장:

1. `(date, market)`별 Spearman IC를 계산한다.
2. 같은 날짜의 market IC를 유효 종목 수로 가중 평균해 날짜당 IC 1개를 만든다.
3. `newey_west_tstat(values, session_index, lag)`를 추가한다. HAC lag pair는 압축된 배열
   위치가 아니라 공통 KRX `formation_session_idx`의 정확한 거리로 만든다.
4. naive t와 NW t를 모두 반환하되 판정 필드는 NW만 사용한다.
5. quantile spread가 rank label이 아니라 raw excess return을 받도록 명시적 인자를 둔다.
6. 기존 `per_date_rank_ic()` API는 회귀 호환을 유지하고 scan 전용 함수를 추가한다.

`research/analysis/horizon_scan.py` 골격:

1. config 검증 → snapshot 선택 → A0 manifest 확인 순으로 gate한다.
2. feature × label join을 공통 survivor/available sample로 분리한다.
3. scan type과 horizon 폭에 맞춰 NW lag를 자동 결정한다.
4. broad/tradable과 독립 segment 축을 순회하되 segment Cartesian product는 만들지 않는다.
5. `--smoke-family`로 한 family만 끝까지 실행할 수 있게 한다.
6. Phase A0에서는 global BH 구현을 단위 테스트할 수 있으나 실제 17-family 판정 결과와
   `03a` 결론 카드는 생성하지 않는다.
7. sample start, holdout start, 최소 cell 크기, market IC 가중, BH q, 탐색 t 임계값을
   코드 상수로 두지 않고 config에서만 읽는다.

테스트:

- 두 market IC가 날짜 관측 2개로 이중 계상되지 않음
- 종목 수 가중 daily IC 손계산
- white-noise, 상수, AR(1) synthetic series의 NW 기준값
- 긴 결측 구간 양쪽 관측이 인접 lag pair로 오인되지 않는 gap fixture
- cum/bucket별 lag 선택
- raw return quantile spread의 경제 단위 유지
- config 밖 horizon/feature 실행 거부

완료 기준: 한 synthetic family와 실제 lake의 한 smoke family가 동일 schema의
`horizon_ic.parquet`을 생성한다.

### A0-9. materialization orchestration과 전체 검증

**목적**: 개별 SQL 성공이 아니라 빈 환경에서 한 명령으로 A0 input을 재현한다.

구현:

1. `research/etl/horizon_scan_inputs.py`에 topological builder를 추가한다.
2. `compute_all --features`와 model dataset builder의 중복 조립을 공통 함수로 정리한다.
3. A0 builder 순서를 `stock PIT → quality → universes → price → flow → label_scan`으로
   고정한다.
4. 성공한 뒤에만 A0 `_SUCCESS.json`을 원자적으로 기록한다.
5. 실패 시 일부 parquet가 있어도 성공 marker를 남기지 않는다.
6. 각 단계 duration, row count, schema hash, min/max date, NULL coverage를 manifest에
   기록한다.

권장 실행 인터페이스:

```bash
uv run python -m research.etl.horizon_scan_inputs
uv run python -m research.analysis.horizon_scan --smoke-family px_reversal_5d
```

공식 A0/A 실행은 snapshot date를 받지 않고 최신 완전한 `sj2_remote` raw snapshot을
자동 선택한다. Phase B/full 실행만 필요 derived mart와의 공통 날짜를 선택한다.
재현/debug 전용 override는 manifest에 `official=false`를 기록한다.

완료 기준: 깨끗한 임시 lake fixture와 실제 선택 snapshot에서 한 번에 A0 marker까지
생성되고, 재실행 시 동일 config/schema에서는 안전하게 cache를 재사용한다.

## 5. 파일별 변경 계획

| 파일 | 변경 내용 |
|---|---|
| `.gitignore` | horizon scan Python/config만 allowlist하고 `research/output`은 계속 제외 |
| `pyproject.toml` | PyYAML research dependency 추가 |
| `research/etl/config.py` | auto-selected config 전달, source별 `dataset_dir()` 지원 |
| `research/etl/snapshot.py` (신규) | phase별 required input 기반 sj2 snapshot 탐색·검증 |
| `research/etl/mart.py` | source별 mart root, schema/config hash cache gate |
| `research/etl/trading_panel.py` (신규) | valid-session index와 공통 return/turnover SQL |
| `research/etl/stock_pit.py` (신규) | PIT shares/float/mcap daily mart |
| `research/etl/quality.py` (신규) | corporate-action, price quality, short regime helper |
| `research/output/horizon_scan/short_balance_publication_lag.json` | KRX balance 기준일·공개 lag evidence와 검증 상태(생성 artifact) |
| `research/etl/universe.py` | named broad/tradable spec와 별도 materialization |
| `research/etl/features/price.py` | price 후보 9개와 CA window mask |
| `research/etl/features/flow.py` | OHLCV/PIT 결합, ratio·balance·NAT 후보 |
| `research/etl/labels.py` | shared forward primitive, end date, bucket, mask |
| `research/etl/metrics.py` | market-aware daily IC, NW, raw spread |
| `research/etl/horizon_scan_inputs.py` (신규) | A0 mart topological builder와 manifest |
| `research/analysis/horizon_scan_config.yaml` (신규) | preregistered family/horizon/품질 정책 |
| `research/analysis/horizon_scan_readiness.py` (신규) | dependency/column readiness report |
| `research/analysis/horizon_scan.py` (신규) | scan driver와 smoke path |
| `research/etl/compute_all.py` | 공통 feature builder 호출, 불완전한 기존 `_build_features` 정리 |
| `research/models/.../build_dataset.py` | 공통 mart builder 재사용, 기존 모델 회귀 호환 |
| `bin/parquet-compute-all.sh` | source별 dataset 분리 후 remote `--features` 차단 가드 해제 |

테스트는 기존 파일을 확장하되 책임이 커지면 다음처럼 분리한다.

```text
tests/unit/test_research_snapshot.py
tests/unit/test_research_stock_pit.py
tests/unit/test_research_quality.py
tests/unit/test_research_features.py
tests/unit/test_research_labels.py
tests/unit/test_research_metrics.py
tests/unit/test_horizon_scan_config.py
tests/unit/test_horizon_scan_inputs.py
tests/integration/test_horizon_scan_inputs_smoke.py
```

## 6. 검증 순서

### 6.1 빠른 검증

각 작업 패키지 안에서 관련 synthetic unit test와 lint를 먼저 실행한다.

```bash
uv run pytest tests/unit/test_research_snapshot.py
uv run pytest tests/unit/test_research_stock_pit.py tests/unit/test_research_quality.py
uv run pytest tests/unit/test_research_calendar_universe.py
uv run pytest tests/unit/test_research_features.py
uv run pytest tests/unit/test_research_labels.py tests/unit/test_research_metrics.py
uv run ruff check research/ tests/
```

### 6.2 회귀 검증

```bash
uv run pytest tests/unit
uv run pytest tests/integration/test_research_features_smoke.py
uv run pytest tests/integration/test_research_labels_smoke.py
uv run pytest tests/integration/test_research_build_dataset_smoke.py
```

고정된 2026-06-19 `local_mydb` row count를 전제로 한 기존 integration guard는 해당
snapshot/source가 없어 현재 대부분 skip되는 죽은 guard다. 이를 정리하고, 산식은 고정
fixture로 검증하며 실제 lake smoke test는 자동 선택 snapshot의 manifest
row count·grain·key uniqueness를 기준으로 새로 수립한다. 과거 snapshot 재현 테스트는
명시적 fixture/config로 별도 남긴다.

### 6.3 A0 end-to-end sanity

1. readiness before/after diff에서 Phase A BH 대상 12개가 `ready`, zero-return이
   `reference_only`, short 4개가 정해진 exploratory 상태인지 확인한다.
2. mart마다 `(trade_date,ticker,market)` key 중복이 0인지 확인한다.
3. broad/tradable 부분집합 관계를 확인한다.
4. feature/label에서 inf가 0건인지 확인한다.
5. horizon별 label coverage와 `survival_to_h`가 합리적으로 비증가하는지 확인한다.
6. mask 전/후 coverage와 CA flag 표본을 확인한다.
7. 연도별 universe 종목 수, family별 최초 유효일, residual momentum warm-up을 확인한다.
8. look-ahead canary가 의도대로 h=1에서만 비정상 고 IC를 보이는지 확인한다.
9. 1회 permutation을 leakage smoke test로 실행한다. 100회 발견 건수 분포는 Phase A
   전체 검증에서 실행한다.
10. `px_reversal_5d` 부호는 참고 진단으로만 보고 테스트 pass/fail로 사용하지 않는다.

## 7. 구현 단위 권장 순서

서로 독립적으로 검토·회귀하기 쉽도록 다음 단위로 나눈다. 번호는 merge 순서다.

1. **A0-PR1 — 계약/경로**: config schema, dependency-aware snapshot resolver,
   source별 mart/dataset path
2. **A0-PR2 — 공통 기반/PIT**: common session, actual-filing-date stock PIT
3. **A0-PR3a — 품질/가용성**: corporate-action, price-limit regime, short regime,
   balance publication-lag evidence와 readiness
4. **A0-PR3b — Universe**: broad/tradable 2벌과 coverage report. PR2의 common session
   이후 PR3a와 병렬 개발 가능
5. **A0-PR4 — Price**: PR3a 이후 price family native/lag1 확장과 회귀 테스트
6. **A0-PR5 — Flow**: PR2 stock PIT와 PR3a short availability 이후 flow
   ratio·short·NAT proxy와 native/lag1 확장
7. **A0-PR6 — Label/통계**: label_scan, bucket, end-date, NW/market-aware IC
8. **A0-PR7 — 조립/검증**: builder, manifest, readiness after, end-to-end smoke

각 단위는 관련 unit test가 통과하고 기존 모델 입력 컬럼의 의미가 조용히 바뀌지 않아야
다음 단위로 넘어간다.

## 8. 주요 위험과 대응

| 위험 | 영향 | 대응 |
|---|---|---|
| adjusted close 부재 | 장기 momentum/label 오염 | A0는 보수적 action-window 전체 제외; adjusted close는 별도 후속 |
| DART share count가 저빈도 | action 날짜·size segment coverage 제한 | price jump suspect를 함께 사용하고 pre-DART backward-fill 금지 |
| 현재 종목 중심 OHLCV 수집 | 과거 상장폐지 종목 누락으로 60/120d IC 상향 편향 | current status 소급 필터 금지, 연도별 종목 수 기록, 모든 장기 결론에 survival-bias 경고 |
| snapshot membership이 2026-04 이후뿐 | 주 분석기간 PIT universe 복원 불가 | coverage 밖은 observed OHLCV base로 두고 복원 가능하다고 주장하지 않음 |
| 관리종목 flag 부재 | tradable universe 과대 포함 | availability를 manifest에 명시하고 실제 거래가능성 주장을 제한 |
| short balance 공개 지연 불확실 | PIT leakage | A0-PR3a가 evidence 생성; 미확정 시 해당 exploratory family만 차단 |
| partial short interval PIT 구성종목 부재 | current constituent 소급 시 look-ahead | partial 공식 제외, short 4개 family를 primary 28셀에서 exploratory로 강등 |
| 합성 filing lag의 긴 오른쪽 꼬리 | 늦은 공시·정정공시 look-ahead | 실제 `rcept_no` 접수일+다음 session interval as-of, 합성 lag는 결측 fallback만 |
| 기존 flow 컬럼 의미 충돌 | 기존 모델 회귀 | old share-level 컬럼 유지, 새 ratio 컬럼을 별도 이름으로 추가 |
| source 없는 cache | local/sj2 혼용 | mart/dataset 모두 source partition 사용; 현재 feature mart가 비어 있어 migration 위험은 낮음 |
| residual momentum SQL 비용 | A0 build 시간·메모리 증가 | 공통 market return/residual 중간 mart 재사용, DuckDB spill 설정 기록 |
| 60/120d censoring | 가짜 decay | label end date, common survivor/formation flag를 label 단계부터 제공 |
| config 변경 후 stale cache | 산식과 parquet 불일치 | config/schema hash mismatch 시 명시적 rebuild 요구 |

## 9. Phase A 인계물

Phase A에는 다음만 전달한다.

1. 검증된 `horizon_scan_config.yaml`과 hash
2. 선택 snapshot/source가 기록된 A0 `_SUCCESS.json`
3. 75개 primary hypothesis의 12개 `ready` family와 short exploratory readiness 목록
4. broad/tradable universe 및 limitation metadata
5. CA mask 적용 `feat_price`, `feat_flow`, `label_scan`
6. smoke run으로 schema가 검증된 horizon scan driver
7. coverage/quality diagnostic과 family별 최초 유효일
8. 현재 종목 중심 수집, PIT membership coverage, 관리종목 source 부재를 포함한
   survival/tradability limitation
9. `short_balance_publication_lag.json`과 verified/unresolved 상태
10. native/lag1 variant column mapping, official variant와 availability evidence hash

Phase A는 이 인계물을 변경하지 않고 전체 17-family scan, 75셀 global BH-FDR,
short exploratory 진단, permutation 분포, family conclusion card를 생성한다. A0 산식이나
config를 바꿔야 한다면 기존
Phase A 결과를 이어 쓰지 않고 config version을 올린 뒤 A0부터 다시 실행한다.
