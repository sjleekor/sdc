# flows-sync 증분 실행 변경 계획

- 작성일: 2026-06-12 (같은 날 검토 의견 반영 1차 갱신)
- 범위: Phase 0 `flows-sync 장기 실행/실패 원인 조사` 후속 변경 계획
- 목표: 일일 `flows-sync`가 DB에 저장된 최신 수급 데이터 이후만 업데이트하도록 변경한다.

## 1. 배경

현재 프로덕션 래퍼 `deploy/prod/bin/flows-sync.sh`는 다음 명령을 실행한다.

```bash
docker compose run --rm collector flows sync --use-price-range
```

`--use-price-range`는 `daily_ohlcv`의 전체 저장 범위로 수급 수집 범위를 계산한다. sj2-server 기준 가격 범위가 `2007-06-05`부터라서, 일일 파이프라인도 매번 2007년부터 최신 가격일자까지의 히스토리 범위를 스캔한다.

이 구조의 문제:

- 일일 작업이 사실상 히스토리 백필처럼 동작한다.
- 과거 foreign holding completeness가 현재 active ticker 수와 비교되어, 이미 수집된 과거 날짜도 계속 미완료로 판정된다.
- 2026-06-10 실행은 `foreign_holding` phase만 24시간 이상 진행하다 abort 되었고, investor/shorting 최신일은 따라오지 못했다.

## 2. 목표 동작

일일 `flows-sync`는 다음 방식으로 날짜 범위를 자동 계산한다.

| 값 | 기준 |
|---|---|
| `FLOW_END` | `daily_ohlcv`의 최신 `trade_date` |
| `FLOW_START` | 저장된 KRX 수급 metric group 최신일 중 가장 오래된 날짜를 기준으로 계산 |

> **전제 조건 — `FLOW_END`와 가격 파이프라인의 결합.** `FLOW_END = 가격 최신일` 설계는 flows가 prices 뒤에 체인으로 실행된다는 전제(`sdc_daily_pipeline`: universe → prices → flows, `set -euo pipefail`)에서만 안전하다. flows를 별도 Cronicle 이벤트로 분리하면 "prices 실패 → flows가 조용히 아무 일도 안 함"이 되므로, 분리 시에는 이 결합을 재설계해야 한다.

기본 정책:

```text
latest_price_date = max(daily_ohlcv.trade_date)
latest_flow_date  = min(metric_group별 max(krx_security_flow_raw.trade_date))
FLOW_END          = latest_price_date
FLOW_START        = min(latest_flow_date + 1 trading day, latest_price_date - lookback_days)
```

단, 위 식은 구현 시 다음처럼 해석한다.

- metric group별 최신일 차이를 놓치지 않기 위해 단일 `MAX(trade_date)`가 아니라 group별 최신일의 최소값을 사용한다.
- `lookback_days`는 최근 부분 실패와 KRX 지연을 흡수하기 위한 **상시 재스캔 window**다. 이미 최신일까지 수집되어 있어도 최근 N일은 다시 확인한다.
- 기본 `lookback_days`는 7일 또는 14일 중 하나로 시작한다. 운영 안정성을 우선하면 14일이 더 보수적이다.
- `FLOW_START > FLOW_END`이면 외부 API 호출 없이 정상 종료한다.
- `krx_security_flow_raw`에 baseline이 없거나, min 계산 대상 group 중 하나라도 최신일을 계산할 수 없으면 자동 증분은 실패한다. 이 경우 `flows-backfill-range.sh`로 명시적 baseline 백필을 먼저 수행한다. 자동 증분 모드가 암묵적으로 장기 백필을 시작하지 않도록 하기 위함이다.

## 3. Metric Group 기준

초기 구현은 세 group으로 나눈다.

| group | metric_code |
|---|---|
| `foreign_holding` | `foreign_holding_shares` |
| `investor` | `institution_net_buy_volume`, `individual_net_buy_volume`, `foreign_net_buy_volume` |
| `shorting` | `short_selling_volume`, `short_selling_value`, `short_selling_balance_quantity` |

현재 sj2 실측상 `foreign_holding_shares`는 `2026-06-10`까지 있으나 investor/shorting 계열은 `2026-05-21`까지라서, 단일 전체 최신일 기준 증분은 누락을 만들 수 있다. 따라서 최소 group 최신일 기준으로 시작한다.

### 3.1 group 고착(stall) 시나리오 방어

min-of-group-maxes 방식에는 고착 위험이 있다: 한 group이 구조적으로 멈추면(예: KRX가 특정 시리즈 제공을 중단 — 실제로 `short_selling_balance_quantity`는 distinct 일수가 타 metric의 절반으로 시계열 시작점이 다르다) min이 동결되어 auto range가 매일 자라고, 결국 guard에 걸려 **전체 일일 잡이 영구 실패**한다.

- v1 방어: 특정 group을 min 계산에서 제외하는 수단(env 또는 `--exclude-groups`)을 마련하고, group별 최신일/지연일수를 매 실행 로그로 남긴다(§5 로그 포맷에 이미 포함).
- 근본 해법은 **group별 독립 범위 동기화**(각 group이 자기 최신일부터 자기 범위만 수집)다. 전역 min 방식이 가진 "앞서 있는 group이 뒤처진 group의 지연 구간을 매일 재스캔하는 낭비"도 함께 사라진다. §8 후속 과제로 격상한다.

## 4. CLI 변경

`flows sync`에 증분 모드를 추가한다.

**플래그 이름은 `--incremental`로 통일한다.** `prices backfill --incremental`이 이미 같은 의미론("저장 최신일 이후만 수집")으로 존재하므로, `--incremental-from-db` 같은 새 어휘를 만들면 모듈 정렬 리팩토링이 없애려는 "모듈마다 다른 어휘" 문제를 하나 추가하는 셈이다.

예상 옵션:

```bash
krx-collector flows sync --incremental --lookback-days 14
```

옵션 의미:

- `--incremental`: DB의 `daily_ohlcv`, `krx_security_flow_raw` 상태를 읽어 `start/end`를 자동 계산한다.
- `--lookback-days N`: 계산된 시작일을 최근 N일 window까지 뒤로 당겨 재시도한다.
- `--max-auto-range-days N`: 자동 계산 범위가 너무 크면 실패한다. 기본 30일 권장.
- `--allow-large-range`: 명시적 운영자 의도 없이 큰 범위를 막기 위한 override.
- `--exclude-groups`: group 고착 시 min 계산에서 제외할 metric group 지정(§3.1).

**감사 기록(`ingestion_runs`) 의무화:** `prices backfill`이 `IngestionRun.params.incremental`을 기록하는 것과 동일하게, `incremental=true`와 함께 **해석된 start/end, lookback_days, group별 최신일/지연일수**를 `params`에 기록한다. §5의 로그 출력만으로는 DB에서 사후 감사가 불가능하다.

`--start` / `--end`와의 관계:

- 초기 구현(v1)에서는 혼용을 금지한다. 단순성과 안전성을 우선한다.
- 장기적으로는 `prices backfill`의 의미론("`--incremental`은 `--end`만 존중, `--start`는 무시")과 맞추는 것이 일관적이므로, v2에서 동일 규약으로 수렴시킨다.

## 5. 프로덕션 래퍼 변경

현재:

```bash
docker compose run --rm collector flows sync --use-price-range
```

변경:

```bash
docker compose run --rm collector flows sync --incremental --lookback-days "${FLOW_LOOKBACK_DAYS:-14}"
```

래퍼는 실행 전후에 계산된 범위를 로그에서 확인할 수 있어야 한다.

예상 로그:

```text
flows incremental range resolved:
  latest_price_date=2026-06-10
  latest_foreign_holding_date=2026-06-10
  latest_investor_date=2026-05-21
  latest_shorting_date=2026-05-21
  start=2026-05-22
  end=2026-06-10
  lookback_days=14
```

## 6. 히스토리 백필 분리

`--use-price-range`는 일일 래퍼에서 제거하고, 히스토리 보수용으로만 남긴다.

별도 래퍼를 추가한다.

```text
deploy/prod/bin/flows-backfill-range.sh
```

예상 사용법:

```bash
FLOW_START=2026-05-01 FLOW_END=2026-05-31 /home/whi/apps/sdc/bin/flows-backfill-range.sh
```

이 래퍼는 `FLOW_START` / `FLOW_END`를 필수로 요구하고, 큰 범위는 명시적 승인 없이 실행하지 않도록 한다.

## 7. 안전장치

1. `--use-price-range`에 max range guard를 추가한다.
   - 예: 90일 초과 시 `--allow-large-range` 없이는 실패.
2. `--incremental`에도 max auto range guard를 둔다.
   - 예: 30일 초과 시 실패.
   - 현재처럼 investor/shorting이 20일 정도 뒤처진 상황은 통과 가능해야 한다.
3. 계산된 범위와 metric group별 최신일을 반드시 로그로 남기고, 동일 내용을 `ingestion_runs.params`에도 기록한다(§4).
4. `FLOW_START > FLOW_END`인 경우 성공 종료하되 "no work" 로그를 남긴다.
5. **lookback 상시 재스캔의 전제와 비용 한계를 명시한다.** lookback window는 매 실행 최근 N일을 재스캔하는 설계이며, foreign_holding은 N일 × market 수 요청이라 비용이 무시할 수준이다. 단, 현재 completeness 판정 결함(기대값을 현재 active 종목 수 / 전체 범위 일수로 계산) 때문에 **window 내 신규 상장·거래정지 종목은 영원히 미완료로 판정되어 매일 재요청**된다. 비용은 해당 소수 종목으로 bounded라 수용 가능하지만, §8의 completeness 기준 전환(요청 단위 completion)이 lookback 설계가 장기적으로 성립하기 위한 전제다.
6. 날짜 계산("+1 trading day")은 거래일 캘린더를 사용한다. 증분 window는 최근 구간이라 2024년 이후만 커버하는 `docs/holidays_krx.csv`로 충분하지만, **같은 계산 함수를 히스토리 백필 범위(2015~2023)에 재사용하면 휴일 과대계상 문제가 따라온다**(audit_and_refactor_plan.md Phase 1의 캘린더 보강과 연동).
7. baseline 부재 시 자동 증분은 실패한다. `krx_security_flow_raw`가 비었거나 대상 group 최신일이 `NULL`이면, 자동으로 `daily_ohlcv` 전체 범위를 타지 않고 명시적 backfill 실행을 요구한다.

## 8. 후속 과제

증분 실행만으로 일일 장기 실행은 해소할 수 있지만, 과거 구간의 근본적인 completeness 문제는 별도 조치가 필요하다.

- **completeness 기준 전환 (세 group 공통).** 결함은 foreign holding에 국한되지 않는다. `sync_krx_flows.py`의 판정 로직상 foreign holding은 과거 날짜를 **현재** active 종목 수와 비교하고(258~264행), investor/shorting도 ticker별 기대값을 **요청 범위 전체** `trading_days × metrics`로 계산하므로(245~256행) 범위 중간 상장 종목은 절대 완료될 수 없다. 둘 다 row count 비교가 아니라 `(trade_date, market, metric_code, source)` 등 **요청 단위 completion 기록** 기준으로 전환한다. 이 전환은 §7-5 lookback 설계의 전제이기도 하다.
- **group별 독립 범위 동기화(v2).** §3.1의 고착 방어를 근본 해결하는 형태로, 전역 min start 대신 각 group이 자기 최신일부터 자기 범위만 수집하도록 전환한다. 앞서 있는 group의 불필요한 재스캔도 함께 제거된다.
- `--start`/`--end` 혼용 규약을 `prices backfill`과 동일하게 수렴(§4).
- abort/SIGTERM 시 `ingestion_runs`가 `running`으로 남지 않도록 stale-run reaper 또는 signal handling을 추가한다.

## 9. 실행 순서

1. `flows sync --incremental --lookback-days` 구현 (`ingestion_runs.params` 기록 포함).
2. 자동 계산 범위 guard와 로그 추가 (`--exclude-groups` 포함).
3. 단위 테스트 추가:
   - metric group별 최신일이 다를 때 최소 최신일 기준으로 시작하는지 확인.
   - 자동 범위가 max를 초과하면 실패하는지 확인.
   - 저장 최신일이 이미 가격 최신일 이상이어도 최근 lookback window로 계산되는지 확인.
   - baseline이 없거나 대상 group 최신일이 없으면 자동 증분이 실패하는지 확인.
   - `--exclude-groups` 지정 시 해당 group이 min 계산에서 빠지는지 확인.
   - `params`에 해석된 범위/그룹별 최신일이 기록되는지 확인.
4. `deploy/prod/bin/flows-sync.sh`를 증분 모드로 변경.
5. `flows-backfill-range.sh` 추가.
6. 문서와 Cronicle 운영 설명 갱신.
7. 배포 후 다음 `sdc_daily_pipeline` 로그에서 full price range가 더 이상 출력되지 않는지 확인.

## 10. 구현 실행 단위

구현은 아래 단위로 나누어 진행한다. 각 단위는 가능한 한 독립적으로 테스트 가능해야 하며, 운영 배포가 필요한 변경은 마지막 단위로 모은다.

### Unit 1. 증분 범위 계산 모델과 storage 조회

목표:

- `daily_ohlcv` 최신일과 `krx_security_flow_raw`의 metric group별 최신일을 조회하는 기능을 추가한다.
- `latest_price_date`, group별 `latest_flow_date`, `lookback_days`, `exclude_groups`를 입력으로 받아 최종 `start/end`를 계산하는 순수 로직을 만든다.

주요 작업:

- metric group 정의를 코드 상수로 추가한다.
- storage port / Postgres storage에 다음 조회를 추가한다.
  - `daily_ohlcv` 최신 `trade_date`
  - group별 `krx_security_flow_raw` 최신 `trade_date`
- baseline 부재, group 최신일 `NULL`, `exclude_groups` 적용, `FLOW_START > FLOW_END` 케이스를 계산 로직에서 명확히 표현한다.

검증:

- 순수 범위 계산 단위 테스트.
- group별 최신일이 다를 때 최소 group 기준으로 시작하는지 확인.
- baseline 부재 시 자동 증분 실패가 반환되는지 확인.
- `exclude_groups`가 min 계산에서 제외되는지 확인.

### Unit 2. CLI 옵션과 guard 연결

목표:

- `flows sync`에 `--incremental`, `--lookback-days`, `--max-auto-range-days`, `--allow-large-range`, `--exclude-groups`를 추가한다.
- `--incremental` 실행 시 Unit 1의 계산 결과로 기존 `start/end`를 결정한다.

주요 작업:

- parser 옵션 추가.
- v1에서는 `--incremental`과 `--start` / `--end` 혼용을 금지한다.
- 계산된 자동 범위가 `--max-auto-range-days`를 초과하면 `--allow-large-range` 없이는 실패한다.
- 계산된 범위, group별 최신일, 지연일수, 제외 group을 stdout/log에 출력한다.
- `FLOW_START > FLOW_END`이면 no-work로 정상 종료한다.

검증:

- parser 단위 테스트.
- `--incremental`과 `--start` / `--end` 혼용 거부 테스트.
- max auto range guard 테스트.
- 최신 상태에서도 lookback window가 계산되는지 테스트.
- no-work 경로가 외부 provider 호출 없이 종료되는지 테스트.

### Unit 3. ingestion_runs 감사 정보 기록

목표:

- 자동 증분 실행의 해석 결과를 `ingestion_runs.params`에 남긴다.

주요 작업:

- `sync_krx_security_flows()`가 optional incremental metadata를 받을 수 있도록 한다.
- 기존 params에 다음 값을 추가한다.
  - `incremental`
  - `resolved_start`
  - `resolved_end`
  - `lookback_days`
  - `max_auto_range_days`
  - `excluded_groups`
  - group별 최신일
  - group별 지연일수
  - `latest_price_date`
- 일반 수동 `--start/--end` 실행은 기존 params 구조와 호환되게 유지한다.

검증:

- fake storage 기반 service 테스트에서 `record_run()`에 전달된 params를 확인한다.
- 기존 non-incremental flows 테스트가 깨지지 않는지 확인한다.

### Unit 4. 프로덕션 일일 래퍼 전환

목표:

- 일일 `flows-sync.sh`가 더 이상 full price range를 사용하지 않도록 변경한다.

주요 작업:

- `deploy/prod/bin/flows-sync.sh`를 다음 형태로 변경한다.

```bash
docker compose run --rm collector flows sync --incremental --lookback-days "${FLOW_LOOKBACK_DAYS:-14}"
```

- 필요하면 `FLOW_MAX_AUTO_RANGE_DAYS` 환경변수도 래퍼에서 받을 수 있게 한다.

검증:

- shell syntax 확인.
- 래퍼 내용에서 `--use-price-range`가 제거되었는지 확인.
- 로컬에서 명령 문자열이 의도대로 구성되는지 확인한다.

### Unit 5. 히스토리 백필 래퍼 추가

목표:

- 일일 freshness와 히스토리 repair 경로를 분리한다.

주요 작업:

- `deploy/prod/bin/flows-backfill-range.sh` 추가.
- `FLOW_START` / `FLOW_END`를 필수로 검증한다.
- 선택적으로 `FLOW_TICKERS`, `FLOW_ALLOW_LARGE_RANGE`, `FLOW_MAX_RANGE_DAYS`를 지원한다.
- 내부 명령은 명시 범위 기반 `flows sync --start "$FLOW_START" --end "$FLOW_END"`를 사용한다.

검증:

- 필수 env 누락 시 실패하는지 확인.
- 정상 env 입력 시 예상 명령을 실행하는지 확인.
- 기본적으로 `--use-price-range`를 사용하지 않는지 확인.

### Unit 6. 문서 갱신

목표:

- 운영자가 일일 증분과 히스토리 백필을 구분해 사용할 수 있게 문서를 맞춘다.

주요 작업:

- `docs/deploy.md`의 `flows-sync.sh` 설명 갱신.
- 필요하면 `README.md`의 flows 예시를 보강한다.
- `audit_and_refactor_plan.md` Phase 0 항목에 이 계획 문서를 링크한다.

검증:

- 문서 내 `flows-sync.sh` 설명이 `--use-price-range` 일일 실행을 더 이상 권장하지 않는지 확인.
- 히스토리 백필은 별도 래퍼로 안내되는지 확인.

### Unit 7. 통합 검증과 배포 준비

목표:

- 코드, 테스트, 운영 래퍼가 함께 일관되게 동작하는지 확인한다.

주요 작업:

- 관련 unit test 실행.
- 가능하면 local DB 또는 fake storage 기반으로 `flows sync --incremental` dry-run 성격의 경로를 검증한다.
- sj2 배포 전, 계산될 범위를 DB 쿼리로 수동 확인한다.

검증:

- `flows sync --incremental --lookback-days 14`가 `2007-06-05` 같은 full price start를 계산하지 않는지 확인.
- 현재 sj2 상태 기준으로 investor/shorting 지연 구간을 포함하는 범위가 계산되는지 확인.
- 배포 후 Cronicle 로그에서 `Price range resolved: start=2007-06-05`가 더 이상 출력되지 않는지 확인한다.

## 11. 진행상황 및 세션 인계 메모

최종 갱신: 2026-06-13 08:03 KST

### 11.1 완료된 구현/배포

- Unit 1~6의 핵심 구현은 완료했다.
- `flows sync --incremental`을 추가했고, `FLOW_LOOKBACK_DAYS`, `FLOW_MAX_AUTO_RANGE_DAYS`, `FLOW_EXCLUDE_GROUPS`를 받는 일일 래퍼로 전환했다.
- `deploy/prod/bin/flows-backfill-range.sh`를 추가해 명시 범위 백필 경로를 분리했다.
- `ingestion_runs.params`에 증분 실행 감사 정보가 기록된다.
- `v0.8.10` 이미지가 sj2-server 운영 compose에 배포되어 있다.
- 배포 후 smoke test:
  - `FLOW_EXCLUDE_GROUPS=investor,shorting FLOW_LOOKBACK_DAYS=0 ./bin/flows-sync.sh`
  - 결과 run: `b893d9d6-9090-42f1-9145-1f9220188da1`
  - `enabled_flow_groups=["foreign_holding"]`
  - `resolved_start=2026-06-10`, `resolved_end=2026-06-10`
  - `status=success`, `requests_attempted=0`, `requests_skipped=2`

### 11.2 운영 DB 정리

- 2026-06-13 00:01 KST 기준 오래된 `ingestion_runs.status='running'` 고아 run 18건을 `failed`로 정리했다.
- 이후 잘못 시작된 명시 범위 run 1건도 `failed`로 정리했다.
  - run_id: `e7f5a05c-63c1-4262-80f8-bb1c4cd7c67e`
  - 사유: v0.8.10 CLI에서 명시 `--start/--end` 실행 시 `--exclude-groups`가 실제 수집 group에 반영되지 않음.

### 11.3 발견된 추가 버그와 로컬 수정 상태

v0.8.10에는 다음 버그가 남아 있다.

- `flows sync --start ... --end ... --exclude-groups ...`에서 `--exclude-groups`가 로그에는 찍히지만 실제 `enabled_flow_groups`에는 반영되지 않는다.
- 기존 구현이 `enabled_flow_groups` 계산을 `--incremental` 분기 안에서만 수행했기 때문이다.

로컬 workspace에는 이 문제를 수정한 변경이 아직 미커밋 상태로 남아 있다.

- `src/krx_collector/cli/app.py`
  - `exclude_groups`와 `enabled_flow_groups` 계산을 `--incremental` 여부와 분리해 항상 적용하도록 수정.
  - `--exclude-groups` help text를 "range resolution and collection"으로 수정.
- `deploy/prod/bin/flows-backfill-range.sh`
  - `FLOW_EXCLUDE_GROUPS`를 받아 `--exclude-groups`로 전달하도록 수정.
- `tests/unit/test_sync_krx_flows.py`
  - 명시 범위 실행에서 `--exclude-groups foreign_holding`가 `enabled_flow_groups=["investor","shorting"]`로 전달되는 회귀 테스트 추가.

검증 결과:

```bash
uv run pytest
# 289 passed, 10 skipped

uv run ruff check src tests
# All checks passed
```

다음 세션에서는 이 로컬 수정분을 commit/release/deploy해야 한다.

### 11.4 현재 진행 중인 investor/shorting catch-up

목표:

- `investor`, `shorting` 계열 metric을 `2026-05-22`부터 `2026-06-10`까지 보강한다.
- `foreign_holding`은 이미 `2026-06-10`까지 있으므로 제외한다.

v0.8.10 CLI 버그 때문에 wrapper/CLI를 쓰지 않고, 운영 컨테이너 안에서 service API를 직접 호출해 `enabled_flow_groups=["investor","shorting"]`를 명시했다.

현재 실행 정보:

- run_id: `b1adfe71-7ee1-4210-8770-a3985c3750dd`
- 상태: `running`
- 시작: `2026-06-13 00:07:58 KST`
- 대상 범위: `2026-05-22..2026-06-10`
- 대상 group: `["investor", "shorting"]`
- 로그 파일: `/home/whi/apps/sdc/logs/flows-catchup-investor-shorting-20260613.log`
- 실행 컨테이너: `sdc-collector-run-e1b292d14c79` (`8bf86651cc82`)

2026-06-13 08:03 KST 기준 진행률:

```text
processed=1689/5538
attempted=1689
skipped=0
rows_upserted=59379
no_data=32
errors=1
```

주의:

- 최신 metric 일자는 이미 모두 `2026-06-10`까지 올라왔다.
- 단, catch-up run 자체는 아직 전체 ticker-group을 완료하지 않았다. "최신일자 도달"과 "전체 run 완료"를 분리해서 판단해야 한다.
- 중복 실행을 피하기 위해 이 run이 종료되기 전에는 같은 범위 catch-up을 새로 시작하지 않는다.

2026-06-13 08:03 KST 기준 sj2 운영 DB의 최신일:

```text
foreign_net_buy_volume          2026-06-10
individual_net_buy_volume       2026-06-10
institution_net_buy_volume      2026-06-10
short_selling_balance_quantity  2026-06-10
short_selling_value             2026-06-10
short_selling_volume            2026-06-10
```

### 11.5 다음 세션에서 먼저 확인할 것

1. catch-up 컨테이너가 아직 실행 중인지 확인한다.

```bash
ssh whi@sj2-server 'docker ps --format "{{.ID}} {{.Names}} {{.Status}}" | grep sdc-collector-run || true'
```

2. catch-up 로그 tail을 확인한다.

```bash
ssh whi@sj2-server 'tail -n 80 /home/whi/apps/sdc/logs/flows-catchup-investor-shorting-20260613.log'
```

3. run 상태를 확인한다.

```bash
.agents/skills/sdc-db/scripts/dbq.sh sj2 "
select run_id, status, started_at, ended_at, now() - started_at as age,
       params->>'enabled_flow_groups' as enabled_flow_groups,
       counts,
       left(coalesce(error_summary,''), 300) as error_summary
  from ingestion_runs
 where run_id='b1adfe71-7ee1-4210-8770-a3985c3750dd'::uuid;
"
```

4. catch-up 종료 후 metric 최신일과 범위 coverage를 다시 확인한다.

```bash
.agents/skills/sdc-db/scripts/dbq.sh sj2 "
select metric_code, max(trade_date) as max_trade_date
  from krx_security_flow_raw
 where metric_code in (
       'institution_net_buy_volume',
       'individual_net_buy_volume',
       'foreign_net_buy_volume',
       'short_selling_volume',
       'short_selling_value',
       'short_selling_balance_quantity'
 )
 group by metric_code
 order by metric_code;
"
```

```bash
.agents/skills/sdc-db/scripts/dbq.sh sj2 "
select metric_code,
       min(trade_date) as min_trade_date,
       max(trade_date) as max_trade_date,
       count(distinct trade_date) as trade_dates,
       count(distinct ticker) as tickers,
       count(*) as rows
  from krx_security_flow_raw
 where trade_date between date '2026-05-22' and date '2026-06-10'
   and metric_code in (
       'institution_net_buy_volume',
       'individual_net_buy_volume',
       'foreign_net_buy_volume',
       'short_selling_volume',
       'short_selling_value',
       'short_selling_balance_quantity'
   )
 group by metric_code
 order by metric_code;
"
```

5. run이 `partial` 또는 `failed`로 끝났다면 `error_summary`와 로그의 error sample을 보고 재시도 범위를 결정한다.

6. 로컬의 CLI/wrapper 수정분을 commit하고 patch release를 만든 뒤 sj2에 배포한다.

권장 순서:

```bash
git status --short
uv run pytest
uv run ruff check src tests
uv run python .agents/skills/sdc-release/scripts/release.py --bump patch --stage-all --remote-update
uv run python .agents/skills/sdc-release/scripts/release.py --bump patch --stage-all --remote-update --apply
./deploy/deploy_to_sj2.sh
```

release 후에는 `docker compose pull collector`와 운영 smoke test로 새 이미지/wrapper가 `FLOW_EXCLUDE_GROUPS`를 명시 범위에도 반영하는지 확인한다.
