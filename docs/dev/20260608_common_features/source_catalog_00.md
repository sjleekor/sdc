# 공통 피쳐 원천 카탈로그 초안

- 작성일: 2026-06-09
- 최종 업데이트: 2026-06-10
- 관련 계획: `docs/dev/20260608_common_features/plan_00.md`
- 목적: 공통 시장/거시 feature의 upstream source code, availability policy, 활성화 상태를 코드 반영 전에 명시한다.
- 상태: PR 4-H active readiness 기준/리포트까지 완료. ECOS 후보는 검증 후에도 `active=false`로 유지한다.

## 1. 원칙

1. source code는 구현 편의보다 point-in-time 안전성을 우선한다.
2. 실제 API smoke 전에는 신규 macro series를 active feature로 모델에 노출하지 않는다.
3. 발표일을 API에서 직접 얻지 못하는 월간 지표는 conservative lag를 먼저 적용하고, release calendar가 준비되면 교체한다.
4. 공식 원천으로 대체 가능한 항목은 FDR/pykrx fallback보다 ECOS/FRED/KRX direct를 우선한다.
5. feature catalog는 `active=false`여도 source/catalog FK와 transform 계약을 먼저 검증할 수 있다.

## 2. 현재 검증 상태 요약

| source | 상태 | 비고 |
|---|---|---|
| FDR | 제한 smoke 완료 | `US500`, `VIX`, `USD/KRW`, `CL=F` 적재 확인. 연구/MVP fallback |
| PYKRX | mock test 완료, live smoke 실패 | 현재 네트워크에서 KRX auth/JSON parse 실패. PR 6 KRX direct로 대체 예정 |
| ECOS | client/provider mock test, CLI wiring, live smoke, inactive daily fact/coverage/readiness 검증 완료 | PR 4-C seed는 inactive |
| FRED | 미구현 | PR 5 예정 |
| KRX direct | 미구현 | PR 6 예정 |

## 3. ECOS API 호출 형태

ECOS `StatisticSearch`는 아래 path 구조를 사용한다.

```text
https://ecos.bok.or.kr/api/StatisticSearch/{API_KEY}/json/kr/{start_row}/{end_row}/{stat_code}/{cycle}/{start_period}/{end_period}/{item_code1}/{item_code2}/...
```

현재 구현된 `EcosStatisticSearchClient`는 아래 입력을 받는다.

| 필드 | 의미 | 예 |
|---|---|---|
| `stat_code` | ECOS 통계표 코드 | `817Y002` |
| `cycle` | 주기 | `D`, `M`, `Q`, `A` |
| `start_period` | 주기별 시작 period | `20240102`, `202401` |
| `end_period` | 주기별 종료 period | `20240112`, `202401` |
| `item_codes` | 통계항목 코드 배열 | `["010200000"]` |

참고 공식 진입점:

- 한국은행 ECOS Open API: https://ecos.bok.or.kr/api/

## 4. ECOS 후보 series

### 4.1 `rate_kr_gov3y`

| 항목 | 값 |
|---|---|
| `series_id` | `rate_kr_gov3y` |
| source | `ECOS` |
| category | `rate` |
| frequency | `D` |
| name_kr | 국고채 3년 수익률 |
| unit | `pct` |
| `stat_code` 후보 | `817Y002` |
| `cycle` | `D` |
| `item_code1` 후보 | `010200000` |
| availability policy | `next_krx_session` |
| `manual_lag_days` | `0` |
| `max_stale_business_days` | `5` |
| default transform | `level` |
| seed active | `false` |
| feature 후보 | `rate_kr_gov3y_level` |
| smoke 상태 | 1개월 coverage 완료, 2026-06-10 |

판단:

- 일간 금리이며 장중 모델 feature에는 당일 확정값을 바로 쓰지 않는다.
- 현재는 `next_krx_session`으로 보수 처리한다.
- live smoke에서 `TIME`, `DATA_VALUE`, `UNIT_NAME`, `available_from_date` 매핑을 확인했다.
- 2024년 1월 Mon-Fri 22일 기준 22 rows가 들어와 단기 결측은 없었다.
- active 전환은 더 긴 기간 coverage와 source code review 후 별도 PR에서 결정한다.

### 4.2 `macro_cpi`

| 항목 | 값 |
|---|---|
| `series_id` | `macro_cpi` |
| source | `ECOS` |
| category | `macro_price` |
| frequency | `M` |
| name_kr | 소비자물가지수 |
| unit | `2020=100` |
| `stat_code` 후보 | `901Y009` |
| `cycle` | `M` |
| `item_code1` 후보 | `0` |
| availability policy | `manual_lag_days` |
| `manual_lag_days` | `20` |
| `max_stale_business_days` | `45` |
| default transform | `level` |
| seed active | `false` |
| feature 후보 | `macro_cpi_level` |
| smoke 상태 | 월간 smoke 완료, 2026-06-10 |

판단:

- 월간 macro series는 `period_end_date`만으로 모델 feature에 노출하면 look-ahead leakage가 생긴다.
- API에서 release date를 직접 확보하기 전까지는 `period_end_date + 20 calendar days`를 보수 lag로 둔다.
- live smoke에서 ECOS 단위가 `2020=100`으로 확인되어 seed/catalog unit을 반영했다.
- `manual_lag_days=20` 적용과 주말 다음 KRX session 보정은 동작 확인했지만, 실제 공식 발표일 달력과의 일치성은 아직 별도 확인이 필요하다.
- 장기적으로는 `macro_cpi_yoy_latest`가 모델에 더 적합하지만, 현재 builder의 PR 3 범위가 `level`/`ret_*` 중심이므로 PR 4-C seed는 `macro_cpi_level`만 inactive로 둔다.

## 5. 코드 반영 상태

PR 4-C에서 아래 seed를 추가한다.

| 대상 | 코드 | active | 비고 |
|---|---|---:|---|
| source series | `rate_kr_gov3y` | false | ECOS smoke 전 |
| source series | `macro_cpi` | false | release calendar 검증 전 |
| feature catalog | `rate_kr_gov3y_level` | false | single input, `level` |
| feature catalog | `macro_cpi_level` | false | monthly, `level` |

`active=false`이므로 기본 `common sync`와 `common build-daily` active-only 경로에는 노출되지 않는다. smoke가 필요할 때는 `common sync --sources ecos --series ... --include-inactive`처럼 explicit allowlist와 inactive 허용 옵션을 함께 지정한다.

## 6. PR 4-E/4-F live smoke 결과

PR 4-D에서 `common sync --sources ecos` CLI dispatch를 열었다. 다만 seed는 계속 inactive이므로 smoke에는 `--include-inactive`가 필요하다.

PR 4-E에서 API key가 있는 로컬 환경으로 아래 제한 smoke를 수행했다.

```bash
uv run krx-collector common seed-catalog

uv run krx-collector common sync \
  --sources ecos \
  --series rate_kr_gov3y \
  --start 2024-01-02 \
  --end 2024-01-05 \
  --include-inactive \
  --rate-limit-seconds 3
```

실행 결과:

| 항목 | 결과 |
|---|---|
| key loading | `ECOS_API_KEY` present |
| seed upsert | series 10, catalog 14 |
| sync status | success |
| provider requests | 1 |
| rows upserted | 4 |
| no-data requests | 0 |
| error count | 0 |

저장 row 확인:

| `TIME` | observation/period end | available from | value | unit |
|---|---|---|---:|---|
| `20240102` | `2024-01-02` | `2024-01-03` | 3.24 | 연% |
| `20240103` | `2024-01-03` | `2024-01-04` | 3.278 | 연% |
| `20240104` | `2024-01-04` | `2024-01-05` | 3.227 | 연% |
| `20240105` | `2024-01-05` | `2024-01-08` | 3.283 | 연% |

확인 사항:

- ECOS `TIME`은 daily series에서 `observation_date`/`period_end_date`로 직접 매핑된다.
- `availability_policy=next_krx_session`이 적용되어 금요일 `2024-01-05` 관측값은 다음 KRX session인 `2024-01-08`부터 사용 가능하다.
- `UNIT_NAME`은 `연%`로 들어오며 raw unit에 저장된다.
- 이번 smoke는 4영업일 단기 확인이므로, active 전환 전 더 긴 기간 coverage 확인이 필요하다.
- 월간 CPI의 release lag 정책 적정성은 아직 미확인이다.

### 6.2 PR 4-F 확장 smoke

PR 4-F에서는 호출 수를 2회로 제한해 `rate_kr_gov3y` 1개월 범위와 `macro_cpi` 3개월 범위를 확인했다.

```bash
uv run krx-collector common sync \
  --sources ecos \
  --series rate_kr_gov3y \
  --start 2024-01-02 \
  --end 2024-01-31 \
  --include-inactive \
  --rate-limit-seconds 3

uv run krx-collector common sync \
  --sources ecos \
  --series macro_cpi \
  --start 2024-01-01 \
  --end 2024-03-31 \
  --include-inactive \
  --rate-limit-seconds 3
```

실행 결과:

| series | range | requests | rows upserted | status | no-data | errors |
|---|---|---:|---:|---|---:|---:|
| `rate_kr_gov3y` | `2024-01-02..2024-01-31` | 1 | 22 | success | 0 | 0 |
| `macro_cpi` | `2024-01-01..2024-03-31` | 1 | 3 | success | 0 | 0 |

`rate_kr_gov3y` 확인:

| 항목 | 결과 |
|---|---|
| Mon-Fri target days | 22 |
| observed rows | 22 |
| missing Mon-Fri days | 0 |
| observed range | `2024-01-02..2024-01-31` |
| value range | `3.191..3.313` |
| unit | `연%` |

`macro_cpi` 저장 row 확인:

| `TIME` | observation/period end | available from | value | unit |
|---|---|---|---:|---|
| `202401` | `2024-01-31` | `2024-02-20` | 113.17 | 2020=100 |
| `202402` | `2024-02-29` | `2024-03-20` | 113.78 | 2020=100 |
| `202403` | `2024-03-31` | `2024-04-22` | 113.95 | 2020=100 |

확인 사항:

- `rate_kr_gov3y`의 stat/item code는 단기 live smoke 기준 정상 동작한다.
- `macro_cpi`의 stat/item code는 단기 live smoke 기준 정상 동작한다.
- `macro_cpi`는 `period_end_date + 20 calendar days`를 적용하며, 결과일이 비거래일이면 다음 KRX session으로 보정된다.
- 두 후보는 계속 `active=false`로 유지한다.

### 6.3 PR 4-G inactive fact/coverage 검증

PR 4-G에서는 inactive 후보를 모델에 기본 노출하지 않으면서도 명시적으로 daily fact와 coverage를 검증할 수 있도록 `common build-daily`와 `common coverage-report`에 `--include-inactive`를 추가했다. 이 옵션은 broad build/report를 막기 위해 `--feature-codes`와 함께만 사용할 수 있다.

검증 명령:

```bash
uv run krx-collector common build-daily \
  --feature-codes rate_kr_gov3y_level \
  --start 2024-01-03 \
  --end 2024-01-31 \
  --include-inactive

uv run krx-collector common coverage-report \
  --feature-codes rate_kr_gov3y_level \
  --start 2024-01-03 \
  --end 2024-01-31 \
  --include-inactive

uv run krx-collector common build-daily \
  --feature-codes macro_cpi_level \
  --start 2024-02-20 \
  --end 2024-04-30 \
  --include-inactive

uv run krx-collector common coverage-report \
  --feature-codes macro_cpi_level \
  --start 2024-02-20 \
  --end 2024-04-30 \
  --include-inactive
```

결과:

| feature | range | target days | facts | nulls | coverage | PIT violations |
|---|---|---:|---:|---:|---:|---:|
| `rate_kr_gov3y_level` | `2024-01-03..2024-01-31` | 21 | 21 | 0 | 1.0000 | 0 |
| `macro_cpi_level` | `2024-02-20..2024-04-30` | 49 | 49 | 0 | 1.0000 | 0 |

`macro_cpi_level` as-of 전환 확인:

| feature date | value | as-of available date |
|---|---:|---|
| `2024-03-19` | 113.17 | `2024-02-20` |
| `2024-03-20` | 113.78 | `2024-03-20` |
| `2024-04-19` | 113.78 | `2024-03-20` |
| `2024-04-22` | 113.95 | `2024-04-22` |

### 6.4 PR 4-H active readiness 기준

PR 4-H에서는 active 전환 전에 확인할 기계적 기준을 `common readiness-report`로 고정했다.

기본 기준:

- required coverage `1.0000`
- null fact 0
- missing target date 0
- PIT violation 0

검증 명령:

```bash
uv run krx-collector common readiness-report \
  --feature-codes rate_kr_gov3y_level \
  --start 2024-01-03 \
  --end 2024-01-31 \
  --include-inactive \
  --required-coverage-ratio 1.0

uv run krx-collector common readiness-report \
  --feature-codes macro_cpi_level \
  --start 2024-02-20 \
  --end 2024-04-30 \
  --include-inactive \
  --required-coverage-ratio 1.0
```

결과:

| feature | range | target days | ready | coverage | nulls | missing | PIT violations | blockers |
|---|---|---:|---|---:|---:|---:|---:|---|
| `rate_kr_gov3y_level` | `2024-01-03..2024-01-31` | 21 | true | 1.0000 | 0 | 0 | 0 | 없음 |
| `macro_cpi_level` | `2024-02-20..2024-04-30` | 49 | true | 1.0000 | 0 | 0 | 0 | 없음 |

해석:

- `rate_kr_gov3y_level`은 단기 readiness를 통과했다. active 전환 전 최근 3~12개월 운영 범위로 같은 기준을 재확인한다.
- `macro_cpi_level`은 daily fact 기준은 통과했지만, 월간 지표 발표일을 `manual_lag_days=20`으로 근사하고 있다. 공식 release calendar 또는 발표일 source를 확보하기 전까지 active 전환하지 않는다.

## 7. 보류 후보

아래는 source code 검증 전까지 seed에 넣지 않는다.

| 후보 | 이유 |
|---|---|
| `rate_kr_gov10y_level` | 10Y item code 확인 필요 |
| `rate_kr_term_spread_10y_3y` | multi-input spread transform 미구현 |
| `fx_usdkrw_ecos_level` | FDR fallback이 있으나 ECOS 환율 stat/item code 확인 필요 |
| `macro_cpi_yoy_latest` | `yoy` transform 및 release calendar 보강 후 추가 |
| `macro_consumer_sentiment_latest` | 통계표/항목 코드와 발표 lag 확인 필요 |
