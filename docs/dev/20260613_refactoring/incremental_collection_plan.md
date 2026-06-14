# 저장 최신 지점 기반 증분 수집 구현 계획

- 작성일: 2026-06-13 KST
- 범위: SDC 수집/정규화 CLI와 운영 wrapper가 호출하는 collector 명령
- 목표: 각 수집 작업이 저장된 마지막 데이터 지점을 먼저 확인하고, 그 이후 또는 보수적 lookback 범위만 수집하도록 정렬한다.
- 배포 제약: 이 목표만 단독 배포하지 않는다. 목표 2(수집 소스별 wrapper 분리/재배치와 throttling 정렬)까지 구현한 뒤 한 번에 배포한다.

---

## 1. 핵심 원칙

1. **명시 backfill과 일일 증분을 분리한다**
   - `--start/--end`는 사람이 지정한 backfill/repair 용도다.
   - `--incremental`은 운영 일일 수집 용도이며 DB 상태로 범위를 계산한다.

2. **baseline 부재 시 자동 장기 백필을 시작하지 않는다**
   - 일일 증분 모드에서 대상 테이블이 비어 있거나 기준 key가 없으면 실패한다.
   - 장기 이력 수집은 별도 backfill wrapper와 명시 범위로 실행한다.

3. **최신 상태면 외부 API 호출 없이 성공 종료한다**
   - no-work를 정상 상태로 처리한다.
   - 증분 모드의 no-work는 항상 `ingestion_runs`에 감사 정보를 남긴다.

4. **자동 계산 범위에 guard를 둔다**
   - 계산된 범위가 운영 기본 한도를 넘으면 실패한다.
   - `--allow-large-range` 같은 명시 override가 있을 때만 장기 범위를 허용한다.

5. **해석된 범위를 감사 가능하게 남긴다**
   - `ingestion_runs.params`에 `incremental`, resolved `start/end`, latest 기준값, lookback, auto range days, no-work 여부를 기록한다.
   - DART처럼 날짜가 아니라 business year/report key 기반인 작업은 resolved target key plan을 기록한다.

6. **resolver와 실행을 분리한다**
   - 각 명령은 먼저 incremental resolver로 실행 계획을 만든다.
   - resolver 결과가 `no_work`, `baseline_missing`, `range_too_large`여도 동일한 audit helper를 통해 run row를 남긴다.
   - 외부 API 호출은 resolver가 `attempt_targets`를 반환한 뒤에만 시작한다.

---

## 2. 현재 상태 요약

| 영역 | 현재 상태 | 목표 1 기준 판정 |
|---|---|---|
| prices | `--incremental`이 ticker별 `MAX(trade_date)+1` 사용 | 부분 충족. 신규 ticker/baseline guard와 range audit 보강 필요 |
| flows | `daily_ohlcv.max`와 flow metric 최신일 기반 `--incremental` 존재 | 기준 구현에 가까움. 남은 버그/감사 정보 마감 |
| common sync/build | wrapper가 고정 lookback window 계산 | 개선 필요. DB 최신 관측일/fact date 기반 증분 필요 |
| DART corp | 직전 성공 run skip 모델 | 수용 가능. `--skip-if-success-today` 정책 명시 필요 |
| DART financial/share/xbrl | year/report key 기반 skip은 있으나 기본 target 정책이 제각각 | 개선 필요. target planner 필요 |
| metrics normalize | 기본 범위 정규화. 입력 변경분 기준 아님 | 개선 필요. 최근 target key 기반 증분화 |

---

## 3. 공통 기반 작업

### 3.1 freshness/range 조회 API 추가

수정 대상:

- `src/krx_collector/ports/storage.py`
- `src/krx_collector/infra/db_postgres/repositories.py`
- 신규 후보: `src/krx_collector/service/freshness.py`
- 신규 후보: `src/krx_collector/service/incremental_ranges.py`

필요 조회:

| 대상 | 조회 내용 |
|---|---|
| `daily_ohlcv` | 전체/market/ticker별 `max(trade_date)` |
| `krx_security_flow_raw` | metric/group별 `max(trade_date)` |
| `common_feature_observation_raw` | source/series별 `max(observation_date)` |
| `common_feature_daily_fact` | feature별 또는 전체 `max(feature_date)` |
| DART raw tables | `(bsns_year, reprt_code, fs_div/statement_type)` coverage |
| `stock_metric_fact` | `(bsns_year, reprt_code)` coverage |

### 3.2 incremental resolver 출력 계약

신규 공통 모델 후보:

- `IncrementalPlan`
- `IncrementalDateRangePlan`
- `IncrementalTargetKeyPlan`

모든 증분 resolver는 아래 필드를 제공한다.

| field | 의미 |
|---|---|
| `run_type` | 기록할 `ingestion_runs.run_type` |
| `status_hint` | `ready`, `no_work`, `baseline_missing`, `range_too_large` 중 하나 |
| `resolved_start` / `resolved_end` | 날짜 범위형 작업의 해석된 범위 |
| `latest_by_target` | ticker, metric group, series, feature 등 대상별 최신 저장 지점 |
| `lookback_days` / `lookback_years` | 적용된 보수적 재스캔 범위 |
| `attempt_targets` | 실제 외부 호출 또는 정규화를 수행할 target |
| `skipped_targets` | skip된 target과 reason |
| `auto_range_days` | 자동 계산된 날짜 범위 길이 |
| `audit_params` | `ingestion_runs.params`에 병합할 직렬화 가능 dict |

정책:

- `status_hint=no_work`는 성공 run으로 기록하고 외부 API를 호출하지 않는다.
- `status_hint=baseline_missing`은 실패 run으로 기록하고 명시 backfill 안내 메시지를 남긴다.
- `status_hint=range_too_large`는 실패 run으로 기록하고 override 안내를 남긴다.
- flows처럼 현재 handler에서 service 호출 전에 return하는 명령도 공통 audit helper를 호출한 뒤 종료하도록 바꾼다.

### 3.3 read-only freshness report 추가

신규 CLI 후보:

```bash
krx-collector ops freshness-report
```

요구 동작:

- 읽기 전용.
- 운영 실행 전후에 한 화면으로 최신 적재 지점을 확인할 수 있어야 한다.
- 기본은 report-only success.
- 선택 플래그로 staleness threshold 초과 시 non-zero exit을 지원한다.

출력 항목:

- price latest date
- flow metric group latest date
- common raw observation latest by source
- common daily fact latest
- DART raw/fact min/max business year와 주요 report code coverage
- running ingestion run summary

---

## 4. Prices 증분 하드닝

수정 대상:

- `src/krx_collector/service/backfill_daily.py`
- `src/krx_collector/cli/app.py`
- `tests/unit/` 내 prices 관련 테스트

현재 동작:

- `prices backfill --incremental`은 ticker별 `MAX(trade_date)+1`부터 `end`까지 fetch한다.
- baseline 없는 ticker는 `start` 또는 기본 `2000-01-01`로 떨어질 수 있다.

변경 계획:

1. `--incremental`에서 ticker별 resolved range를 명확히 기록한다.
2. baseline 없는 신규 ticker가 자동으로 장기 백필을 시작하지 않도록 guard를 둔다.
3. 신규 ticker를 허용할 경우에는 `--new-ticker-start` 또는 명시 `--allow-new-ticker-backfill` 같은 override를 둔다.
4. 자동 범위가 `--max-auto-range-days`를 넘으면 실패한다.
5. no-work ticker count와 skipped reason을 `counts`에 기록한다.

추가 옵션 후보:

```bash
prices backfill --incremental \
  --lookback-days 0 \
  --max-auto-range-days 10 \
  --new-ticker-start YYYY-MM-DD
```

완료 기준:

- 최신 ticker는 provider 호출 없이 skip된다.
- baseline 없는 ticker는 기본 일일 증분에서 장기 요청을 만들지 않는다.
- `ingestion_runs.params`로 어떤 ticker가 어떤 기준으로 처리됐는지 감사 가능하다.

---

## 5. Flows 마감

수정 대상:

- `src/krx_collector/cli/app.py`
- `src/krx_collector/service/sync_krx_flows.py`
- `deploy/prod/bin/flows-backfill-range.sh`
- `tests/unit/test_sync_krx_flows.py`

현재 동작:

- `flows sync --incremental`은 `daily_ohlcv.max(trade_date)`를 end로 사용한다.
- flow metric group별 latest date와 lookback으로 start를 계산한다.
- `--max-auto-range-days` guard가 있다.

변경 계획:

1. 명시 범위 실행(`--start/--end`)에서도 `--exclude-groups`가 반영되도록 한다.
2. `enabled_flow_groups`, excluded groups, group latest dates, lag days를 감사 정보에 남긴다.
3. no-work는 service 호출 전 return하더라도 공통 audit helper로 success run을 기록한다.
4. catch-up 종료 전에는 운영 flows 실행 또는 Cronicle trigger를 하지 않는다.

완료 기준:

- explicit range와 incremental range 모두 group filtering이 동일하게 동작한다.
- 자동 증분 범위가 장기 백필로 변질되지 않는다.
- no-work도 `ingestion_runs`에 `status=success`, `params.no_work=true`로 남는다.
- 현재 실행 중인 investor/shorting catch-up과 충돌하지 않는다.

---

## 6. Common Features 증분화

수정 대상:

- `src/krx_collector/service/sync_common_features.py`
- `src/krx_collector/service/build_common_feature_daily_facts.py`
- `src/krx_collector/cli/app.py`
- `deploy/prod/bin/common-features-refresh.sh`는 목표 2에서 최종 재배치
- common feature 관련 단위 테스트

### 6.1 `common sync --incremental`

범위 계산:

- source/series별 `max(observation_date)`를 조회한다.
- `start = min(series_latest + 1 day, end - lookback_days)`로 계산한다.
- `series.history_start_date`보다 앞서지 않게 clamp한다.
- baseline이 없으면 기본적으로 실패한다.
- lookback이 0보다 크면 최신 상태여도 resolved lookback window를 재수집 대상으로 본다.
- 따라서 no-work는 `series_latest >= end`이고 적용 lookback이 0인 경우에만 성립한다.

옵션 후보:

```bash
common sync --incremental \
  --sources krx,fdr,fred,ecos \
  --lookback-days 45 \
  --max-auto-range-days 90
```

정책:

- `--force`는 일일 증분 wrapper에서 사용하지 않는다.
- incremental lookback 구간은 기존 coverage가 있어도 coverage 존재만으로 provider 호출을 skip하지 않는다.
- 현재 `_has_existing_coverage()` 기반 skip은 명시 range 실행 또는 `lookback_days=0`인 증분 no-work 판정에만 사용한다.
- late revision 흡수가 필요한 source(FRED/ECOS/FDR)는 lookback window를 항상 재요청한다.
- KRX/PYKRX처럼 revision 가능성이 낮은 source도 목표 2에서 source별 wrapper를 분리하기 전까지는 동일 정책을 적용하고, source별 최적화는 목표 2에서 다룬다.
- baseline 없는 series를 허용하려면 명시 backfill 또는 별도 override가 필요하다.
- source별 revision 특성이 다르므로 lookback 기본값은 source/series 단위 override 가능하게 둔다.

CLI 정책:

- `--incremental`이면 `--start/--end`는 기본적으로 금지한다.
- 증분 범위를 사람이 상한/하한으로 clamp해야 하는 경우에는 별도 `--range-floor` / `--range-ceiling` 옵션을 검토한다.
- 기존 required `--start/--end`는 `--incremental` 도입 시 required가 아니도록 parser 구조를 바꾸고, 명시 range 실행에서는 계속 필수로 검증한다.

### 6.2 `common build-daily --incremental`

범위 계산:

- `common_feature_daily_fact.max(feature_date)`를 조회한다.
- raw observation latest와 비교해 build end를 결정한다.
- late revision을 흡수하기 위해 build lookback은 유지한다.
- no-work 조건을 만족하면 성공 종료하고 audit을 남긴다.
- lookback이 0보다 크면 최신 상태여도 lookback window의 fact를 재생성한다.
- no-work는 `fact_latest >= resolved_end`이고 적용 lookback이 0인 경우에만 성립한다.

옵션 후보:

```bash
common build-daily --incremental \
  --lookback-days 120 \
  --max-auto-range-days 180
```

완료 기준:

- common raw sync와 daily fact build가 고정 날짜 window가 아니라 DB 최신 상태로 범위를 계산한다.
- `lookback_days=0`이고 최신 상태이면 provider 호출 또는 fact 재생성을 하지 않는다.
- `lookback_days>0`이면 late revision 흡수를 위해 coverage가 있어도 resolved lookback window를 재처리한다.
- 계산된 source/series/feature별 범위가 run params에 남는다.

---

## 7. DART target planner

DART는 trade date가 아니라 business year/report key 도메인이다. 따라서 목표 1은 "마지막 날짜 이후"가 아니라 "저장된 target key 이후 또는 미완료 key만 수집"으로 해석한다.

수정 대상:

- 신규 후보: `src/krx_collector/service/dart_target_plan.py`
- `src/krx_collector/service/sync_dart_financials.py`
- `src/krx_collector/service/sync_dart_share_info.py`
- `src/krx_collector/service/sync_dart_xbrl.py`
- `src/krx_collector/cli/app.py`
- OpenDART 관련 단위 테스트

기본 target policy:

- 일일 증분 후보는 당해와 전년이다.
- report code 후보는 `11011,11012,11013,11014`를 기본으로 통일한다.
- 실제 attempt target은 filing availability planner를 통과한 항목으로 제한한다.
- 기존 raw key가 있으면 현재처럼 skip한다.
- 최근 no-data로 확인된 target은 negative cache TTL 안에서는 skip한다.
- 한 번의 운영 실행에서 시도할 target 수는 `--max-attempt-targets`로 제한한다.
- 장기 catch-up은 일일 증분에서 자동 시작하지 않는다.

추가 CLI 후보:

```bash
dart sync-financials --incremental --lookback-years 1
dart sync-share-info --incremental --lookback-years 1
dart sync-xbrl --incremental --lookback-years 1
```

planner 출력:

- resolved `bsns_years`
- resolved `reprt_codes`
- available target count
- existing key count
- negative-cache skip count
- missing/attempt target count
- skipped target count
- estimated request count

### 7.1 filing availability와 no-data 방지

당해/전년 + 4개 report code 정책은 현재 기본값보다 요청 후보를 크게 늘린다. 단순히 raw key 부재만 보고 요청하면 아직 공시되지 않은 보고서에 대해 매일 no-data API 호출을 반복할 수 있다.

planner 요구사항:

1. report code별 예상 공시 가능 시점을 둔다.
   - `11013`: 1분기 보고서, 기본 5월 중순 이후
   - `11012`: 반기 보고서, 기본 8월 중순 이후
   - `11014`: 3분기 보고서, 기본 11월 중순 이후
   - `11011`: 사업보고서, 다음 해 3월 말 이후
2. availability window 이전 target은 `skipped_targets`에 `not_yet_available`로 기록하고 provider 호출을 하지 않는다.
3. provider가 no-data를 반환한 request key는 negative cache로 남긴다.
   - 구현 후보 1: `ingestion_runs.params.no_data_request_keys`와 최근 run 조회.
   - 구현 후보 2: 별도 `dart_request_attempt`류 테이블. 1차 구현에서는 스키마 변경을 피하려면 후보 1을 우선 검토한다.
4. negative cache TTL 기본값을 둔다.
   - 분기/반기/3분기 보고서: 3일
   - 사업보고서: 7일
5. `--max-attempt-targets` 기본값을 두고 초과 시 실패 또는 partial plan으로 제한한다.

### 7.2 요청량 계산표

구현 전 현재 active DART corp 수를 기준으로 요청량을 산정한다. 아래 표는 planner가 출력해야 하는 항목의 예시다.

| 명령 | 후보 target 산식 | availability/cache 적용 후 attempt 산식 |
|---|---|---|
| `dart sync-financials` | active corp 수 × year 수 × report code 수 × fs_div 수 | 후보 - existing key - not-yet-available - negative-cache |
| `dart sync-share-info` | active corp 수 × year 수 × report code 수 × statement group 수 | 후보 - existing key - not-yet-available - negative-cache |
| `dart sync-xbrl` | financial raw의 filing target 수 | 후보 - existing xbrl key - negative-cache |

배포 전 freshness report 또는 dry-run planner 출력으로 예상 request count를 확인한다. 이 값이 운영 한도를 넘으면 wrapper 실행이 아니라 명시 backfill 작업으로 분리한다.

완료 기준:

- DART 세 명령의 기본 target year/report 정책이 일관된다.
- 이미 저장된 key는 provider 호출 없이 skip된다.
- 아직 공시 가능 시점이 아닌 target과 최근 no-data target은 provider 호출 없이 skip된다.
- 실행 전 예상 request count가 출력되고 guard로 제한된다.
- baseline이 비어 있다고 해서 2015년부터 전체 backfill이 자동 실행되지 않는다.

---

## 8. Metrics Normalize 증분화

수정 대상:

- `src/krx_collector/service/normalize_metrics.py`
- `src/krx_collector/cli/app.py`
- metrics 관련 단위 테스트

1차 정책:

- raw 변경 row 기반 추적은 후속 과제로 둔다.
- 우선 DART target planner와 같은 최근 business year/report code를 normalize 대상으로 삼는다.
- `stock_metric_fact` coverage만으로 전체 명령을 skip하지 않는다.
- catalog/rule 갱신은 항상 실행한다.
- target별 skip은 catalog/rule 갱신 이후 fact normalization 단계에만 적용한다.
- skip 기준은 year/report 단위 coverage가 아니라 `(ticker, bsns_year, reprt_code, metric_code)` 수준의 expected coverage로 잡는다.

추가 CLI 후보:

```bash
metrics normalize --incremental --lookback-years 2
```

완료 기준:

- 기본 운영 normalize가 전체 또는 불명확한 범위를 재처리하지 않는다.
- mapping rule 변경은 incremental 실행에서도 반영된다.
- fact normalization skip은 ticker/metric coverage 기준으로만 적용된다.
- 최근 target key만 처리하고, 처리 대상과 skip 대상이 run params/counts에 남는다.

---

## 9. 테스트 계획

필수 단위 테스트:

- prices
  - ticker latest 이후만 fetch한다.
  - no-work면 provider를 호출하지 않는다.
  - no-work도 `ingestion_runs`에 success로 기록한다.
  - baseline 없는 ticker가 기본 증분에서 장기 요청을 만들지 않는다.

- flows
  - explicit range와 incremental range 모두 `--exclude-groups`를 반영한다.
  - range guard가 동작한다.
  - no-work는 service 호출 전 return하더라도 audit helper로 기록된다.

- common
  - source/series별 latest + lookback 범위를 계산한다.
  - `lookback_days>0`이면 existing coverage만으로 provider 호출을 skip하지 않는다.
  - `--incremental`과 명시 `--start/--end` parser 정책이 고정되어 있다.
  - baseline 없음은 실패한다.
  - `lookback_days=0`의 no-work면 provider를 호출하지 않고 audit을 남긴다.

- DART
  - target planner가 current/previous year와 report codes를 생성한다.
  - existing key skip이 유지된다.
  - filing availability 이전 target은 provider를 호출하지 않는다.
  - negative cache TTL 안의 no-data target은 provider를 호출하지 않는다.
  - `--max-attempt-targets` guard가 동작한다.
  - baseline 부재 시 장기 자동 백필을 하지 않는다.

- metrics
  - incremental target years만 normalize한다.
  - catalog/rule 갱신은 incremental에서도 실행한다.
  - coverage 완료 target은 ticker/metric 수준에서 fact normalization만 skip한다.

검증 명령:

```bash
uv run pytest tests/unit/test_sync_krx_flows.py
uv run pytest tests/unit/test_common_features*.py
uv run pytest tests/unit/test_opendart_*.py
uv run pytest tests/unit/test_metric_coverage_report.py
uv run ruff check src tests
```

가능하면 최종 배포 전 전체 테스트도 실행한다.

```bash
uv run pytest
```

---

## 10. 실행 순서

1. 공통 incremental resolver 출력 계약과 audit helper를 추가한다.
2. 공통 freshness/range 조회 API와 `ops freshness-report`를 추가한다.
3. prices 증분 guard와 감사 기록을 보강한다.
4. flows의 남은 버그와 no-work audit을 마감한다.
5. `common sync/build-daily --incremental`을 구현하고 coverage skip 우회 정책을 테스트한다.
6. DART target planner, filing availability, negative cache, request guard를 구현한다.
7. DART 세 명령의 `--incremental`을 planner 기반으로 연결한다.
8. `metrics normalize --incremental`을 구현하되 catalog/rule 갱신은 유지한다.
9. 목표 2에서 wrapper를 수집 소스별로 분리하고 throttling/lock domain을 정렬한다.
10. 두 목표 구현과 검증이 끝난 뒤에만 운영 배포한다.

---

## 11. 운영 주의사항

- 현재 실행 중인 investor/shorting flows catch-up이 있으면 동일 범위 flows 실행, Cronicle trigger, Cronicle timing 활성화를 하지 않는다.
- 목표 1 구현 중에는 운영 wrapper 실행을 smoke test로 사용하지 않는다.
- 배포는 목표 2까지 끝난 뒤 진행한다.
- 배포 후 smoke test도 source/group 충돌을 피해서 제한적으로 실행한다.

---

## 12. 구현 상태

최종 갱신: 2026-06-13 KST

완료:

- 공통 no-work/guard 실패 audit helper 추가.
- `ops freshness-report` read-only CLI 추가.
- prices `--incremental` guard 보강.
  - `--lookback-days`
  - `--max-auto-range-days`
  - `--new-ticker-start`
  - `--allow-new-ticker-backfill`
  - `--allow-large-range`
- flows no-work audit 기록 추가.
- common `sync --incremental` 추가.
  - series별 latest observation 기반 range 계산.
  - lookback 구간은 existing coverage만으로 skip하지 않음.
- common `build-daily --incremental` 추가.
  - feature별 latest fact 기반 range 계산.
  - lookback 구간 재생성.
- DART target planner 추가.
  - filing availability window.
  - 최근 no-data request key negative cache.
  - existing raw key와 negative cache를 제외한 실제 attempt 후보 기반 request guard.
  - `--force`는 existing raw key와 negative cache를 모두 우회.
- prices/common incremental 오류는 CLI non-zero exit으로 전달.
- DART `sync-financials`, `sync-share-info`, `sync-xbrl`에 `--incremental` 추가.
- metrics `normalize --incremental` 추가.
  - catalog/rule 갱신은 유지.
  - 최근 business year 범위로 정규화 대상 제한.
- 운영 wrapper가 새 incremental 옵션을 호출하도록 최소 갱신.

검증:

```bash
uv run python -m compileall src/krx_collector
uv run python -c "from krx_collector.cli.app import build_parser; p=build_parser(); p.parse_args(['ops','freshness-report']); p.parse_args(['common','sync','--incremental']); p.parse_args(['common','build-daily','--incremental']); p.parse_args(['dart','sync-financials','--incremental']); p.parse_args(['metrics','normalize','--incremental']); print('parser ok')"
bash -n deploy/prod/bin/prices-backfill-incremental.sh deploy/prod/bin/dart-sync-financials.sh deploy/prod/bin/dart-sync-share-info.sh deploy/prod/bin/dart-sync-xbrl.sh deploy/prod/bin/common-features-refresh.sh deploy/prod/bin/flows-sync.sh deploy/prod/bin/flows-backfill-range.sh
uv run pytest
uv run ruff check src tests
```

결과:

- `uv run pytest`: 292 passed, 10 skipped.
- `uv run ruff check src tests`: passed.

남은 제약:

- 아직 운영 배포하지 않는다.
- 목표 2(wrapper 수집 소스별 분리/재배치와 throttling 정렬) 완료 후 함께 배포한다.
