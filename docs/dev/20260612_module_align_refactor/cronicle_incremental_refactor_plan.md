# Cronicle 작업 증분 수집 정렬 리팩토링 계획

- 작성일: 2026-06-13 KST
- 범위: sj2-server Cronicle에 실제 등록된 SDC 작업 3개와 해당 운영 래퍼
- 목표: 운영 수집 파이프라인이 DB의 최신 적재 지점을 확인하고, 그 이후 또는 보수적 lookback 구간만 수집하도록 정렬한다.
- 기준 구현: 최근 추가한 `flows sync --incremental`

---

## 1. 2026-06-13 KST 실측 상태

### 1.1 Cronicle 이벤트

조회 원천: Cronicle API `GET /api/app/get_schedule/v1`

| event id | enabled | timing | chain | script |
|---|---:|---|---|---|
| `sdc_daily_pipeline` | 1 | `false` | `""` | `universe-sync.sh` -> `prices-backfill-incremental.sh` -> `flows-sync.sh` |
| `sdc_daily_accounts_flows` | 1 | `false` | `""` | `dart-sync-corp.sh` -> `dart-sync-financials.sh` -> `dart-sync-share-info.sh` -> `dart-sync-xbrl.sh` -> `metrics-normalize.sh` |
| `sdc_daily_common_features` | 1 | `false` | `""` | `common-features-refresh.sh` |

세 이벤트 모두 수동 실행 상태다. 자동 스케줄을 켜는 작업은 리팩토링/배포/백필 상태가 안정화된 뒤 별도 승인으로 진행한다.

### 1.2 운영 DB 최신 적재 상태

조회 원천: sj2 PostgreSQL `krx_data`

| table | min | max | rows |
|---|---|---|---:|
| `daily_ohlcv` | 2007-06-05 | 2026-06-10 | 6,550,517 |
| `krx_security_flow_raw` | 2007-06-05 | 2026-06-10 | 76,312,333 |
| `common_feature_observation_raw` | 2024-09-30 | 2026-06-12 | 2,738 |
| `common_feature_daily_fact` | 2025-11-03 | 2026-06-12 | 5,550 |

DART 계열은 2015년부터 적재되어 있다.

| table | min_year | max_year | rows |
|---|---:|---:|---:|
| `dart_financial_statement_raw` | 2015 | 2026 | 16,887,271 |
| `dart_share_count_raw` | 2015 | 2025 | 312,329 |
| `dart_shareholder_return_raw` | 2015 | 2025 | 7,831,054 |
| `dart_xbrl_document` | 2015 | 2025 | 81,532 |
| `stock_metric_fact` | 2015 | 2025 | 765,966 |

### 1.3 진행 중인 작업 제약

`2026-06-13 08:12 KST` 기준 `investor` / `shorting` 수급 catch-up 컨테이너가 아직 실행 중이었다.

- run_id: `b1adfe71-7ee1-4210-8770-a3985c3750dd`
- status: `running`
- range: `2026-05-22..2026-06-10`
- enabled groups: `["investor", "shorting"]`
- progress: `1729/5538`, `rows_upserted=60741`, `errors=1`

이 값은 작성 시점의 snapshot일 뿐이다. 이후 계획을 실행할 때는 반드시 `flows_sync_incremental_plan.md` §11.5의 절차로 현재 상태를 재확인한다. 이 run이 실제로 아직 실행 중이면 같은 범위의 flows catch-up을 새로 시작하지 않는다. Cronicle 자동 스케줄 활성화도 보류한다.

---

## 2. 증분 수집 표준 계약

`flows sync --incremental`을 기준 구현으로 삼는다. Cronicle에서 도는 수집 작업은 가능한 한 아래 계약을 따른다.

1. **Upstream 기준 end 계산**
   - 수집 대상의 끝을 외부 기본값이 아니라 DB 상태에서 계산한다.
   - 예: flows는 `daily_ohlcv.max(trade_date)`를 `end`로 사용한다.

2. **Downstream 최신 지점 기준 start 계산**
   - 대상 테이블의 최신 적재 지점을 읽고 다음 날짜부터 시작한다.
   - 부분 실패와 지연 반영을 위해 최근 N일 lookback은 항상 재스캔할 수 있다.

3. **Baseline 부재 시 장기 백필을 자동 시작하지 않음**
   - 일일 증분 모드에서 baseline이 없으면 실패한다.
   - 히스토리 백필은 별도 래퍼와 명시 범위로 실행한다.

4. **자동 범위 guard**
   - 계산된 범위가 운영 기본 한도를 넘으면 실패한다.
   - `--allow-large-range` 같은 명시적 override 없이는 장기 작업으로 변질되지 않게 한다.

5. **감사 가능성**
   - `ingestion_runs.params`에 `incremental`, resolved `start/end`, latest 기준값, lookback, 제외 group/source, auto range days를 기록한다.

6. **No-work 성공 종료**
   - 최신 상태이면 외부 API 호출 없이 성공 종료한다.

7. **명시 백필 분리**
   - 일일 래퍼는 `--incremental`.
   - 히스토리 보수는 `*-backfill-range.sh` 형태로 `START/END` 필수.

---

## 3. 현재 래퍼별 판정

| Cronicle event | step | 현재 래퍼 | DB 최신일 기반 증분 여부 | 판정 |
|---|---|---|---|---|
| `sdc_daily_pipeline` | universe | `universe sync --source fdr --markets kospi,kosdaq` | 아님. 현재 snapshot diff 방식 | 개선 필요 |
| `sdc_daily_pipeline` | prices | `prices backfill --market all --incremental` | ticker별 `MAX(trade_date)+1` | 대체로 충족 |
| `sdc_daily_pipeline` | flows | `flows sync --incremental --lookback-days ...` | `daily_ohlcv.max` + flow group 최신일 | 기준 구현. 패치 배포 필요 |
| `sdc_daily_accounts_flows` | corp | `dart sync-corp` | 날짜 증분은 아니고 직전 성공 run skip | 수용 가능 |
| `sdc_daily_accounts_flows` | financials | current/current-1 year + 기존 key skip | year/report key 기반 멱등 | 개선 필요 |
| `sdc_daily_accounts_flows` | share-info | 기본 previous year + 기존 key skip | year/report key 기반 멱등 | 개선 필요 |
| `sdc_daily_accounts_flows` | xbrl | 기본 previous year + 기존 key skip | year/report key 기반 멱등 | 개선 필요 |
| `sdc_daily_accounts_flows` | metrics | `metrics normalize` 전체 실행 | 입력 변경분 기준 아님 | 개선 필요 |
| `sdc_daily_common_features` | common sync/build | 고정 lookback window | DB 최신일 기준 아님 | 개선 필요 |

---

## 4. 실행 계획

`investor` / `shorting` catch-up 상태는 stale일 수 있으므로, **모든 실행의 첫 단계는 현재 상태 재확인이다.** 단, A1의 bugfix release/deploy는 실행 중인 service-API catch-up과 독립적이므로 catch-up 종료를 기다리지 않고 진행할 수 있다. 반대로 같은 범위의 flows 실행, Cronicle 수동 트리거, Cronicle timing 변경은 catch-up 종료 확인 뒤로 미룬다.

### Phase 0. 현재 상태 재확인

목표: 문서의 snapshot을 믿고 진행하지 않고, 실제 운영 상태를 기준으로 다음 행동을 결정한다.

1. catch-up 컨테이너 확인

```bash
ssh whi@sj2-server 'docker ps --format "{{.ID}} {{.Names}} {{.Status}}" | grep sdc-collector-run || true'
```

2. catch-up 로그 확인

```bash
ssh whi@sj2-server 'tail -n 80 /home/whi/apps/sdc/logs/flows-catchup-investor-shorting-20260613.log'
```

3. run 상태 확인

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

판단:

- 아직 running이면 같은 범위 flows 실행과 Cronicle trigger/timing 변경은 금지한다.
- 종료됐다면 Phase B의 coverage 확인으로 넘어간다.
- A1 release/deploy는 이 상태와 독립적으로 진행 가능하다. 단, 배포 직후 flows wrapper를 실행하는 smoke test는 catch-up 상태를 보고 범위를 제한한다.

### Phase A. 즉시 처리할 bugfix release/deploy

목표: v0.8.10 운영본에 남아 있는 `--exclude-groups` 버그를 먼저 제거한다. 이 배포는 현재 catch-up 컨테이너를 재시작하지 않으며, 실행 중인 service-API catch-up과 충돌하지 않는다.

#### A1. flows 명시 범위 `--exclude-groups` 버그 수정 배포

상태: 로컬 수정 진행 중.

수정 대상:

- `src/krx_collector/cli/app.py`
  - `exclude_groups`와 `enabled_flow_groups` 계산을 `--incremental` 분기 밖에서 항상 수행한다.
  - 명시 범위 실행(`--start/--end`)에서도 제외 group이 실제 수집 group에 반영되게 한다.
- `deploy/prod/bin/flows-backfill-range.sh`
  - `FLOW_EXCLUDE_GROUPS`를 `--exclude-groups`로 전달한다.
- `tests/unit/test_sync_krx_flows.py`
  - `flows sync --start ... --end ... --exclude-groups foreign_holding`이 `enabled_flow_groups=["investor","shorting"]`를 전달하는 회귀 테스트를 둔다.

검증:

- `uv run pytest tests/unit/test_sync_krx_flows.py`
- 가능하면 전체 `uv run pytest`
- `uv run ruff check src tests`

배포:

- patch release 생성.
- sj2-server에 image/wrapper 배포.
- 실행 중인 catch-up 컨테이너가 있더라도 중단하지 않는다.

배포 후 smoke test:

- catch-up이 아직 running이면 같은 `investor` / `shorting` 범위를 건드리는 테스트는 하지 않는다.
- 가능한 smoke:
  - `FLOW_EXCLUDE_GROUPS=investor,shorting FLOW_LOOKBACK_DAYS=0 flows-sync.sh`
  - 기대: 외부 요청 없이 foreign holding no-work 또는 skip으로 종료.

### Phase B. catch-up 중 선행 가능한 코드 수정

목표: 운영 중인 수급 catch-up을 건드리지 않고, 다음 배포에 포함할 코드 변경을 준비한다.

#### B1. freshness report 명령 추가

수정 대상:

- 신규 서비스 또는 CLI 핸들러
  - 예: `krx-collector ops freshness-report`
- storage 조회 메서드
  - `daily_ohlcv.max(trade_date)`
  - `krx_security_flow_raw` metric group별 max date
  - `common_feature_observation_raw.max(observation_date)`
  - `common_feature_daily_fact.max(feature_date)`
  - DART raw/fact table별 min/max `bsns_year`

요구 동작:

- 읽기 전용.
- 사람이 Cronicle 실행 전후에 바로 볼 수 있는 한 화면 요약을 출력한다.
- 향후 Cronicle smoke step에서 재사용할 수 있게 exit code 정책을 둔다.
  - 기본은 report-only success.
  - 선택 플래그로 staleness threshold 초과 시 non-zero exit.

검증:

- fake storage 기반 단위 테스트.
- local DB 또는 sj2 read-only 쿼리로 출력 형식 smoke test.

#### B2. KRX host-level `flock` 래퍼 유틸 추가

수정 대상:

- `deploy/prod/bin/` 공용 helper 또는 각 래퍼
  - 대상: `prices-backfill-incremental.sh`, `flows-sync.sh`, `flows-backfill-range.sh`
  - 조건부 대상: `common-features-refresh.sh`는 `SDC_COMMON_SYNC_SOURCES`에 `krx` 또는 `pykrx`가 포함될 때
  - 조건부 대상: `universe-sync.sh`는 source가 `pykrx`일 때

요구 동작:

- KRX/pykrx 호출이 포함된 작업만 같은 host lock을 잡고 실행한다.
- DART 계열과 `metrics-normalize.sh`는 이 lock에 묶지 않는다.
- lock timeout을 환경변수로 조정 가능하게 한다.
- lock 획득 실패 시 명확한 로그와 non-zero exit.

주의:

- lock 범위는 "KRX/pykrx 외부 호출"로 한정한다. DART/metrics 작업을 같은 lock에 묶으면 KRX와 무관한 파이프라인까지 불필요하게 직렬 대기한다.
- 운영 배포는 A1과 분리해도 된다. 자동 스케줄 활성화 전에는 반드시 반영한다.

검증:

- shell syntax check.
- 래퍼 dry-run이 없으므로 명령 조립 로직은 최소화한다.

#### B3. `sdc_daily_pipeline`의 universe/prices 증분 하드닝

수정 대상:

- `universe sync`
  - `--skip-if-success-today` 또는 기본 run guard 추가.
  - 같은 `as_of` + source + markets 성공 run이 있으면 provider 호출 없이 success.
  - `--force`로 override.
- `prices backfill --incremental`
  - 기본 `end`를 `today`가 아니라 최근 KRX 거래일로 계산하는 옵션 추가.
  - 신규 ticker fallback start를 `COLLECTION_START_DATE` 또는 listing-aware start로 제한하는 옵션 추가.

우선순위:

- B3는 B1/B2보다 뒤다. 자동 스케줄 전 안정성에는 freshness report와 KRX lock이 더 직접적이다.

### Phase C. 재발 방지 자동화

목표: 이미 수동 정리한 고아 run 문제가 다시 생겼을 때 안전하게 정리할 수 있게 한다. 즉각적인 운영 장애 해소가 아니라 재발 방지 항목이다.

#### C1. stale running run reaper 추가

수정 대상:

- 신규 CLI 명령
  - 예: `krx-collector ops reap-stale-runs`
- storage 메서드
  - 오래된 `ingestion_runs.status='running'` 조회
  - 대상 run을 `failed`로 finalize

요구 동작:

- 기본은 dry-run.
- `--apply`가 있을 때만 DB를 변경한다.
- `--older-than-hours`와 run type allowlist를 지원한다.
- `error_summary`에는 `orphaned/stale running run reaped`와 기준 시간을 기록한다.

주의:

- 2026-06-13 00:01 KST 기준 오래된 고아 run은 이미 수동으로 failed 정리됐다.
- 따라서 이 항목은 P0가 아니라 재발 방지 자동화로 다룬다.
- 현재 실행 중인 catch-up run을 건드리지 않도록 기본 threshold는 보수적으로 둔다.
- 코드와 테스트까지만 먼저 완료하고, 운영 `--apply` 실행은 catch-up 종료 뒤 판단한다.

검증:

- fake storage 단위 테스트.
- SQL update는 별도 사용자 승인 전에는 sj2에서 실행하지 않는다.

### Phase D. catch-up 종료 확인

목표: 진행 중인 수급 catch-up을 안전하게 마무리하고, 다음 배포가 같은 범위를 중복 실행하지 않게 한다.

1. `b1adfe71-7ee1-4210-8770-a3985c3750dd` run 종료 확인
   - 성공/partial/failed 상태와 error sample 확인
   - metric별 `2026-05-22..2026-06-10` coverage 확인

2. 실패 또는 partial이면 재시도 범위 결정
   - 전체 범위 재시작 금지.
   - 로그와 DB coverage를 보고 필요한 ticker/group/date만 재시도한다.

### Phase E. 코드 배포와 smoke test

목표: Phase B/C 코드 수정분을 운영에 배포하고, 일일 래퍼가 장기 백필로 변질되지 않는지 확인한다.

1. 로컬 수정분 release/deploy
   - A1 bugfix는 이미 배포했어야 한다.
   - B1/B2/C1은 구현 완료 상태에 따라 같은 릴리스 또는 후속 릴리스로 분리한다.

2. 배포 후 smoke test
   - `FLOW_EXCLUDE_GROUPS=investor,shorting FLOW_LOOKBACK_DAYS=0 flows-sync.sh`
   - 외부 요청 없이 foreign holding no-work 또는 skip으로 종료되는지 확인
   - freshness report가 운영 DB 상태를 정확히 요약하는지 확인

3. reaper는 dry-run 먼저 실행
   - `--apply`는 현재 running 작업이 없고 대상 run이 명확할 때만 별도 승인 후 실행한다.

### Phase F. `sdc_daily_pipeline` 정렬

목표: universe -> prices -> flows 전체를 DB 기준 증분 계약으로 맞춘다.

1. universe
   - `--skip-if-success-today` 또는 기본 run guard를 운영 래퍼에 적용.
   - `ingestion_runs.params`에 resolved `as_of`, source, markets, skipped reason 기록.

2. prices
   - 현행 `--incremental`은 유지.
   - 최근 KRX 거래일 end 계산과 신규 ticker start 제한을 운영 기본으로 적용.
   - run params/counts에 ticker별 latest 요약과 resolved end 기록.

3. flows
   - 현행 `--incremental`을 일일 표준으로 유지.
   - `FLOW_END = daily_ohlcv.max(trade_date)` 결합은 flows가 prices 뒤에서 같은 파이프라인으로 실행된다는 전제에서만 안전하다.
   - flows를 prices와 별도 Cronicle 이벤트나 독립 시간대로 분리하려면, "prices 실패 -> flows가 조용히 no-work"가 되지 않도록 `FLOW_END` 계산과 실패 정책을 재설계한다.
   - 후속 개선:
     - group별 독립 범위 동기화로 전역 min 최신일 병목 제거.
     - row-count completeness 대신 요청 단위 completion 기록 도입.
     - `--use-price-range`는 히스토리 보수 전용으로 문서화하고 guard 유지.

### Phase G. `sdc_daily_common_features` 정렬

목표: 고정 lookback 래퍼를 DB 최신 상태 기반 증분으로 바꾼다.

1. `common sync --incremental` 추가
   - series별 `max(observation_date)` 또는 source-specific 최신 관측일을 조회.
   - `start = min(series_latest + 1 day, end - lookback_days)`로 계산.
   - source/series별 baseline이 없으면 일일 모드에서는 실패하거나 명시 백필 요구.
   - `--force`는 히스토리 보수 래퍼에서만 사용.

2. `common build-daily --incremental` 추가
   - `common_feature_daily_fact.max(feature_date)`와 raw observation 최신 상태를 기준으로 build range 계산.
   - late revision을 흡수하기 위해 build lookback은 유지.
   - resolved range와 feature별 latest fact date를 `ingestion_runs.params`에 기록.

3. 히스토리 백필 분리
   - `common-features-refresh.sh`는 일일 증분 전용.
   - `common-features-backfill-range.sh` 추가: `COMMON_START`, `COMMON_END`, source/series override 필수.

### Phase H. `sdc_daily_accounts_flows` 정렬

목표: DART는 trade date가 아니라 business year/report key 도메인이므로, "DB 최신일" 대신 "대상 key plan"을 표준화한다.

1. DART target planner 추가
   - 현재일 기준으로 필요한 `bsns_years`와 `reprt_codes`를 계산.
   - 기본 일일 대상: 당해/전년 + 주요 보고서 코드.
   - 장기 catch-up 대상: 2015부터 전년까지 분기 또는 월 1회.

2. financial/share/xbrl 공통 계약 정리
   - 이미 있는 `get_existing_*_keys()` skip 패턴을 표준으로 유지.
   - 세 명령의 default year/report 정책을 동일하게 맞춤.
   - resolved target years/report codes와 skipped/attempted count를 params/counts에 기록.

3. metrics normalize 증분화
   - 입력 raw table에서 변경된 `(corp_code, bsns_year)` 또는 최근 N년만 normalize.
   - `metrics normalize --incremental --lookback-years 2` 추가.
   - 히스토리 재계산은 `metrics-normalize-range.sh` 또는 명시 `--bsns-years`로 분리.

### Phase I. 스케줄 활성화와 검증

목표: 자동 실행을 켠 뒤 데이터 freshness 회귀를 잡는다.

1. Cronicle 이벤트 timing 설정
   - mutating API이므로 실행 전 별도 승인 필요.
   - 세 이벤트가 같은 KRX lock을 두고 충돌하지 않도록 시간 또는 chain 조정.
   - `sdc_daily_pipeline`의 prices와 flows는 같은 script 안에서 순서 실행하거나 명시 chain으로 묶는다.
   - flows를 prices와 분리 배치하려면 `FLOW_END = daily_ohlcv.max(trade_date)` 결합을 먼저 재설계한다.

2. 첫 자동 실행 검증
   - Cronicle job log에서 resolved range가 full history로 잡히지 않는지 확인.
   - `daily_ohlcv.max(trade_date)`와 flow metric max date가 최신 거래일까지 올라오는지 확인.
   - common fact/observation latest가 해당 일자까지 올라오는지 확인.

3. 회귀 알림 기준
   - 가격/수급: 최신 KRX 거래일 대비 1거래일 초과 지연이면 경고.
   - common daily fact: 1일 초과 지연이면 경고.
   - DART: 대상 business year/report key 미수집 count가 증가하면 경고.

---

## 5. 우선순위

1. **P0: 현재 상태 재확인(Phase 0)**
2. **P1: A1 bugfix release/deploy(Phase A)** — catch-up 종료를 기다리지 않는다.
3. **P2: freshness report + KRX 한정 `flock` 준비(Phase B)**
4. **P3: catch-up 종료 확인과 필요한 범위 재시도 판단(Phase D)**
5. **P4: 재발 방지 reaper(Phase C)와 후속 코드 배포/smoke(Phase E)**
6. **P5: universe/prices/flows daily pipeline 정렬(Phase F)**
7. **P6: common features 증분 전환(Phase G)**
8. **P7: DART target planner와 metrics normalize 증분화(Phase H)**
9. **P8: Cronicle timing 활성화(Phase I)**

자동 스케줄 활성화는 마지막이다. 지금 핵심 리스크는 스케줄 부재 자체보다, 스케줄을 켰을 때 장기 백필성 작업과 KRX 동시 호출이 자동으로 반복될 수 있다는 점이다.

---

## 6. 관련 문서

- `docs/dev/20260612_module_align_refactor/audit_and_refactor_plan.md`
- `docs/dev/20260612_module_align_refactor/flows_sync_incremental_plan.md`

---

## 7. 세션 인계 메모

최종 갱신: 2026-06-13 13:59 KST

### 7.1 현재 운영 상태

`investor` / `shorting` catch-up은 아직 실행 중이다.

- run_id: `b1adfe71-7ee1-4210-8770-a3985c3750dd`
- 컨테이너: `8bf86651cc82 sdc-collector-run-e1b292d14c79`
- DB status: `running`
- started_at: `2026-06-13 00:07:58 KST`
- age: 약 `13시간 51분`
- enabled groups: `["investor", "shorting"]`
- 대상 범위: `2026-05-22..2026-06-10`
- 최신 로그 진행률: `2931/5538`
- counts snapshot: `rows_upserted=103146`, `no_data=51`, `errors=2`
- 최신 로그 시각: `2026-06-13 13:58:49 KST`

현재 평균 속도 기준 예상 남은 시간은 약 `12시간 20분`이다. 예상 종료 시각은 **2026-06-14 02:10~02:30 KST** 정도로 본다. KRX 응답 지연이 섞여 있으므로 보수적으로는 **새벽 2~3시 KST** 범위로 판단한다.

### 7.2 지금 하면 안 되는 작업

catch-up이 끝나기 전에는 아래 작업을 하지 않는다.

- 같은 `2026-05-22..2026-06-10` 범위의 flows catch-up 재실행
- `investor` / `shorting` group을 포함하는 flows wrapper smoke test
- Cronicle `sdc_daily_pipeline` 수동 trigger
- Cronicle timing 활성화
- catch-up 컨테이너 중단/재시작

### 7.3 지금 진행 가능한 작업

다음 작업은 실행 중인 service-API catch-up과 독립적이라 진행 가능하다.

1. **A1 bugfix release/deploy**
   - v0.8.10 운영본에 남은 `flows sync --start/--end --exclude-groups` 버그 제거
   - 로컬 수정 대상:
     - `src/krx_collector/cli/app.py`
     - `deploy/prod/bin/flows-backfill-range.sh`
     - `tests/unit/test_sync_krx_flows.py`
   - 배포는 catch-up 컨테이너를 중단하지 않는다.

2. **B1 freshness report 구현**
   - read-only 기능이므로 catch-up과 충돌하지 않는다.

3. **B2 KRX 한정 `flock` 래퍼 준비**
   - 배포는 자동 스케줄 활성화 전이면 충분하다.

4. **C1 stale run reaper 구현**
   - 긴급도는 낮다. 코드와 테스트만 먼저 작성하고, 운영 `--apply`는 별도 승인 전에는 실행하지 않는다.

### 7.4 다음 세션 첫 확인 명령

다음 세션은 반드시 현재 상태를 다시 확인하고 시작한다.

```bash
ssh whi@sj2-server 'docker ps --format "{{.ID}} {{.Names}} {{.Status}}" | grep sdc-collector-run || true'
```

```bash
ssh whi@sj2-server 'tail -n 80 /home/whi/apps/sdc/logs/flows-catchup-investor-shorting-20260613.log'
```

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

catch-up이 종료되어 있으면 metric coverage를 확인한다.

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

### 7.5 권장 다음 작업 순서

1. 상태 재확인.
2. catch-up이 아직 running이면 A1 release/deploy를 먼저 진행.
3. A1 배포 후 smoke test는 `investor` / `shorting`을 건드리지 않는 범위로 제한.
4. catch-up 종료 후 coverage 확인.
5. partial/failed이면 전체 재실행하지 말고 로그와 coverage 기준으로 필요한 ticker/group/date만 재시도.
6. 이후 freshness report와 KRX `flock` 구현으로 진행.
