# 수집 소스별 wrapper 분리와 throttling 재배치 계획

- 작성일: 2026-06-13
- 범위: `deploy/prod/bin/` 운영 wrapper, Cronicle 실행 순서, 관련 운영 문서
- 목표: 외부 수집 소스별로 Cronicle event, wrapper, lock/throttling domain을 분리해서 같은 source를 치는 작업만 직렬화한다.
- 배포 제약: 목표 1(DB 최신 지점 기반 증분 수집)과 목표 2를 모두 구현한 뒤 함께 배포한다.

## 1. 배경

현재 운영 wrapper는 업무 단계 중심으로 묶여 있다.

| wrapper | 현재 역할 | 문제 |
| --- | --- | --- |
| `common-features-refresh.sh` | FDR/FRED/ECOS/KRX/PYKRX common feature sync, build, readiness를 한 번에 실행 | 여러 외부 source가 한 wrapper에 묶여 source별 throttling/lock을 걸기 어렵다. |
| `prices-backfill-incremental.sh` | 가격 증분 수집 | 현재 CLI가 `PykrxDailyPriceProvider`를 사용하므로 FDR이 아니라 KRX/PYKRX market-data domain으로 보호해야 한다. |
| `flows-sync.sh` | KRX 수급 증분 수집 | KRX를 치는 다른 작업과 프로세스 간 직렬화가 없다. |
| `dart-sync-*.sh` | OpenDART corp/financial/share-info/xbrl 수집 | 같은 OpenDART quota를 공유하지만 wrapper 레벨 lock이 없다. |
| `metrics-normalize.sh` | DART raw 정규화 | 외부 API를 치지 않지만 DART raw sync 뒤에 실행되어야 한다. |

목표 1에서 각 collector 명령은 DB 최신 상태를 기준으로 증분 범위를 계산하도록 정렬했다. 목표 2에서는 Cronicle event와 운영 wrapper를 source 기준으로 재배치해서 불필요한 동시 호출과 API quota 충돌을 줄인다.

## 2. 설계 원칙

1. 외부 수집 source가 lock/throttle의 기준이다.
2. 같은 source를 호출하는 wrapper는 같은 lock을 공유한다.
3. 서로 다른 source는 독립 Cronicle event로 분리해서 개별 schedule, retry, alert, throttle 정책을 가질 수 있게 한다.
4. 외부 API를 치지 않는 build/normalize/readiness는 source lock에 묶지 않는다.
5. 기존 통합 wrapper는 수동 호환/운영 편의용으로만 남기고, Cronicle의 기본 실행 단위에서는 제외한다.
6. 명시 backfill wrapper도 같은 source lock을 사용해서 일일 증분 작업과 충돌하지 않게 한다.
7. source sync 이후 필요한 build/normalize/readiness는 별도 downstream event로 분리하고, Cronicle chain 또는 명시 실행 순서로 연결한다.

## 3. Source Lock Domain

| domain | 대상 source | 대표 wrapper |
| --- | --- | --- |
| `krx_marketdata` | KRX 직접 호출과 pykrx 기반 KRX market data | `prices-backfill-incremental.sh`, `flows-sync.sh`, `flows-backfill-range.sh`, `common-sync-krx.sh`, `common-sync-pykrx.sh` |
| `opendart` | OpenDART API | `dart-sync-corp.sh`, `dart-sync-financials.sh`, `dart-sync-share-info.sh`, `dart-sync-xbrl.sh`, `dart-backfill-all-years.sh` |
| `fdr` | FinanceDataReader 기반 universe/common feature | `universe-sync.sh`, `common-sync-fdr.sh` |
| `fred` | FRED API | `common-sync-fred.sh` |
| `ecos` | ECOS API | `common-sync-ecos-daily.sh`, `common-sync-ecos-macro.sh` |
| `local` | DB 내부 build/coverage/normalize/readiness | `common-build-daily.sh`, `common-coverage-report.sh`, `common-readiness-check.sh`, `metrics-normalize.sh` |

`local` domain은 lock이 필요한 source가 아니라 운영 분류다. 기본적으로 외부 source lock을 걸지 않는다.

`krx_marketdata`는 collector CLI source 값이 아니라 운영 lock domain이다. `common sync --sources krx`와 `common sync --sources pykrx`는 source를 분리해서 실행하지만, 둘 다 KRX 계열 endpoint/auth/세션 영향을 받으므로 같은 `krx_marketdata` lock을 공유한다.

## 4. 공통 Wrapper Helper

신규 파일:

```text
deploy/prod/bin/lib/sdc-wrapper.sh
```

책임:

- `SDC_APP_DIR` 기본값 처리
- `docker compose run --rm collector ...` 공통 함수 제공
- source별 lock 실행 함수 제공
- source별 최소 실행 간격 throttle 제공
- 로그 prefix와 실행 command 출력 형식 통일

예상 환경 변수:

| env | 기본값 | 의미 |
| --- | --- | --- |
| `SDC_APP_DIR` | `$HOME/apps/sdc` | 운영 app directory |
| `SDC_LOCK_DIR` | `/tmp/sdc-locks` | host-side lock file directory |
| `SDC_THROTTLE_DIR` | `/tmp/sdc-throttle` | source별 last-run marker directory |
| `SDC_DOCKER_COMPOSE_CMD` | `docker compose` | compose command override |
| `SDC_LOCK_WAIT_SECONDS` | helper 기본 `0`, daily Cronicle wrapper 기본 `900` | lock 대기 시간. 0이면 즉시 실패. 일일 Cronicle wrapper는 짧은 overlap을 흡수하기 위해 15분 대기를 기본으로 둔다. |
| `SDC_LOCK_CONFLICT_MODE` | `fail` | `fail`이면 lock 충돌 시 exit `75`, `skip`이면 exit `0`. Cronicle wrapper 기본은 `fail` |
| `SDC_KRX_MARKETDATA_MIN_INTERVAL_SECONDS` | source별 기본값 | KRX/PYKRX market-data wrapper 간 최소 간격 |
| `SDC_OPENDART_MIN_INTERVAL_SECONDS` | source별 기본값 | OpenDART wrapper 간 최소 간격 |
| `SDC_FDR_MIN_INTERVAL_SECONDS` | source별 기본값 | FDR wrapper 간 최소 간격 |
| `SDC_FRED_MIN_INTERVAL_SECONDS` | source별 기본값 | FRED wrapper 간 최소 간격 |
| `SDC_ECOS_MIN_INTERVAL_SECONDS` | source별 기본값 | ECOS wrapper 간 최소 간격 |

Lock 정책은 wrapper 프로세스 간 동시 실행 방지에 집중한다. Collector 내부 provider의 request-level rate limit은 기존 CLI/service 옵션으로 유지한다.

### 4.1 Lock/throttle 동작 계약

- `flock`은 host-side lock file에 건다.
- helper 함수의 기본 `SDC_LOCK_WAIT_SECONDS=0`은 non-blocking acquire다.
- daily Cronicle wrapper는 명시 override가 없으면 `SDC_LOCK_WAIT_SECONDS=900`을 적용한다. 같은 source event는 chain/stagger로 겹치지 않게 배치하되, 운영상 몇 분 overlap이 생기면 대기 후 실행한다.
- lock 획득 실패는 대기 시간이 모두 지난 뒤 exit code `75`로 종료한다. Cronicle에서는 실패로 기록되어야 한다.
- `SDC_LOCK_CONFLICT_MODE=skip`은 수동 운영 또는 특수 backfill에서만 사용한다. 이때 lock 충돌은 로그를 남기고 exit code `0`으로 종료한다.
- throttle marker는 lock을 획득하고 source 호출을 시작하기 직전에 갱신한다. 실패한 호출도 외부 source에 부하를 줬을 수 있으므로 다음 실행을 지연한다.
- throttle marker는 source별로 따로 둔다. 예: `krx_marketdata`, `opendart`, `fdr`, `fred`, `ecos`.
- lock file은 `flock` kernel lock을 사용하므로 프로세스 종료 시 자동 해제된다. 별도 stale lock cleanup은 두지 않는다.
- `SDC_LOCK_DIR`와 `SDC_THROTTLE_DIR`의 기본값은 `/tmp` 하위다. 재부팅 시 marker 유실은 허용한다.
- wrapper는 lock 획득/대기/충돌/skip/throttle sleep 시간을 Cronicle 로그에 출력한다.

### 4.2 Lock 대기 정책 결정

source별 Cronicle event는 lock을 queue로 쓰는 방식에 의존하지 않는다. 운영 기본은 다음과 같다.

- 같은 domain 안의 일일 event는 Cronicle chain 또는 충분한 시간차로 배치한다.
- daily wrapper는 예상치 못한 overlap만 흡수하도록 최대 900초 대기한다.
- 900초 안에 lock을 얻지 못하면 source 충돌 또는 장기 backfill로 보고 exit `75`로 실패시킨다.
- 명시 backfill wrapper는 기본적으로 non-blocking fail을 유지한다. maintenance window에서 daily job을 기다리게 하려면 운영자가 `SDC_LOCK_WAIT_SECONDS`를 명시한다.
- `SDC_LOCK_CONFLICT_MODE=skip`은 사람이 수동으로 실행하는 진단/보수 작업에서만 허용하고 Cronicle 기본값으로 쓰지 않는다.

## 5. Wrapper 재배치

### 5.1 KRX/PYKRX market-data 계열

변경 대상:

- `prices-backfill-incremental.sh`
- `flows-sync.sh`
- `flows-backfill-range.sh`
- `common-sync-krx.sh` 신규
- `common-sync-pykrx.sh` 신규

정책:

- 모두 `krx_marketdata` lock을 공유한다.
- `prices-backfill-incremental.sh`는 현재 CLI가 `PykrxDailyPriceProvider`를 사용하므로 `fdr`가 아니라 `krx_marketdata` domain이다.
- `flows-sync.sh`는 목표 1의 `flows sync --incremental` 호출을 유지한다.
- `flows-backfill-range.sh`도 명시 range backfill이므로 같은 `krx_marketdata` lock을 사용한다.
- `common-sync-krx.sh`는 `common sync --sources krx`만 실행한다.
- `common-sync-pykrx.sh`는 `common sync --sources pykrx`만 실행한다.
- 두 common wrapper는 source는 다르지만 같은 `krx_marketdata` lock을 사용한다.
- `common-sync-pykrx.sh`는 wrapper는 추가하되 Cronicle 기본 활성화와 common build 필수 source에는 넣지 않는다. 운영에서 pykrx common source를 필수로 쓰기로 확정하고 smoke/backfill이 끝난 뒤 활성화한다.

주의:

- `flows sync --incremental`은 가격 최신일을 end 기준으로 사용한다.
- 따라서 가격 수집이 실패했는데 flows만 별도 실행되어 조용히 no-work가 되는 상황을 Cronicle 순서와 로그로 확인 가능하게 둔다.

### 5.2 FDR 계열

변경 대상:

- `universe-sync.sh`
- `common-sync-fdr.sh` 신규

정책:

- 모두 `fdr` lock을 공유한다.
- `universe-sync.sh`는 현재 `universe sync --source fdr`이므로 `fdr` domain이다.
- 현재 가격 수집은 PyKRX provider이므로 `prices-backfill-incremental.sh`를 FDR domain에 넣지 않는다.
- 향후 가격 provider가 FDR로 바뀌면 그때 `prices-backfill-incremental.sh`의 lock domain을 `fdr`로 이동한다.

### 5.3 OpenDART 계열

변경 대상:

- `dart-sync-corp.sh`
- `dart-sync-financials.sh`
- `dart-sync-share-info.sh`
- `dart-sync-xbrl.sh`
- `dart-backfill-all-years.sh`

정책:

- 일일 wrapper는 모두 `opendart` lock을 공유한다.
- 목표 1에서 추가한 incremental planner, negative cache, max-attempt guard는 그대로 유지한다.
- `--force` repair 실행도 같은 lock을 사용한다.
- `metrics-normalize.sh`는 OpenDART API를 치지 않으므로 `opendart` lock에 묶지 않는다.
- `dart-backfill-all-years.sh`는 기본적으로 `SDC_DART_BACKFILL_EXCLUSIVE=1`로 전체 backfill 동안 `opendart` lock을 보유한다. 이 경우 연도별 `metrics normalize` 시간까지 일일 OpenDART 실행이 대기/실패할 수 있으며, 이는 의도된 maintenance-window 동작이다.
- backfill 중 local normalize 시간에는 OpenDART lock을 풀고 싶다면 `SDC_DART_BACKFILL_EXCLUSIVE=0`으로 두고 연도별 DART sync 단계만 sub-wrapper lock을 사용한다. 이 모드는 daily OpenDART와 interleaving될 수 있으므로 기본값으로 쓰지 않는다.

### 5.4 Common Feature 계열

현재 `common-features-refresh.sh`를 source별 wrapper orchestration으로 바꾼다.

신규 wrapper 후보:

- `common-seed-catalog.sh`
- `common-sync-fdr.sh`
- `common-sync-fred.sh`
- `common-sync-ecos-daily.sh`
- `common-sync-ecos-macro.sh`
- `common-sync-krx.sh`
- `common-sync-pykrx.sh`
- `common-build-daily.sh`
- `common-coverage-report.sh`
- `common-readiness-check.sh`

`common-features-refresh.sh`는 수동 호환 wrapper로 남기고 내부에서 다음 순서로 호출한다.

```text
common-seed-catalog.sh
common-sync-fdr.sh
common-sync-fred.sh
common-sync-ecos-daily.sh
common-sync-ecos-macro.sh
common-sync-krx.sh
# optional. SDC_COMMON_ENABLE_PYKRX=1일 때만 실행
common-sync-pykrx.sh
common-build-daily.sh
common-coverage-report.sh
common-readiness-check.sh
```

Cronicle은 이 통합 wrapper를 호출하지 않는다. 운영자가 전체 common feature refresh를 수동으로 한 번에 실행해야 할 때만 사용한다.

ECOS는 기존 wrapper의 두 패스를 보존한다.

| wrapper | command 정책 | lookback |
| --- | --- | --- |
| `common-sync-ecos-daily.sh` | `common sync --sources ecos --incremental` | `SDC_COMMON_DAILY_LOOKBACK_DAYS` |
| `common-sync-ecos-macro.sh` | `common sync --sources ecos --series "$SDC_COMMON_MACRO_SERIES" --incremental` | `SDC_COMMON_MACRO_LOOKBACK_DAYS` |

기본 macro series는 기존과 동일하게 `macro_cpi,macro_ppi,macro_m2,macro_consumer_sentiment`를 유지한다.

### 5.5 Local Build/Normalize

변경 대상:

- `metrics-normalize.sh`
- `common-build-daily.sh` 신규
- `common-coverage-report.sh` 신규
- `common-readiness-check.sh` 신규

정책:

- 외부 API lock을 걸지 않는다.
- `metrics-normalize.sh`는 목표 1의 `metrics normalize --incremental --lookback-years ...` 호출로 정렬한다.
- build/coverage/readiness는 raw sync 이후 실행되도록 Cronicle 또는 orchestration wrapper 순서로 제어한다.
- `common-build-daily.sh`는 build 전에 구조화된 freshness guard CLI를 호출한다. 사람이 읽는 `ops freshness-report` 출력을 파싱하지 않는다.
- 신규 CLI 후보는 `ops assert-common-freshness`다. 이 명령은 필수 source/series의 freshness를 검사하고 통과하면 exit `0`, 미달이면 원인 로그를 남기고 non-zero로 종료한다.
- `common-build-daily.sh`는 guard 미달이면 stale raw로 build하지 않고 즉시 non-zero로 종료한다.
- 필수 source 기본값은 `SDC_COMMON_REQUIRED_SOURCES=fdr,fred,ecos,krx`로 둔다. `pykrx`는 현재 운영 wrapper 기본 source가 아니므로 기본 필수 source에서 제외한다.
- pykrx common source를 운영 필수로 승격할 때는 `common-sync-pykrx.sh` smoke/backfill을 먼저 완료한 뒤 `SDC_COMMON_ENABLE_PYKRX=1`과 `SDC_COMMON_REQUIRED_SOURCES=fdr,fred,ecos,krx,pykrx`를 함께 적용한다.
- guard 기준은 source별 마지막 성공 run age와 active source series의 최신 관측일을 함께 사용한다. 단, FRED/ECOS macro series는 일간 source처럼 최신 KRX 거래일을 요구하지 않고 catalog의 frequency/release lag 또는 source별 허용 lag를 따른다.

`ops assert-common-freshness` 예상 옵션:

| option | 기본값 | 의미 |
| --- | --- | --- |
| `--sources` | `SDC_COMMON_REQUIRED_SOURCES` | 필수 common source allowlist |
| `--end` | KST today | freshness 판정 기준일 |
| `--max-run-age-hours` | `SDC_COMMON_SOURCE_MAX_AGE_HOURS` 또는 `30` | source별 마지막 successful `COMMON_FEATURE_SYNC` run 허용 age |
| `--daily-max-lag-days` | `SDC_COMMON_DAILY_MAX_LAG_DAYS` 또는 `2` | FDR/KRX/PYKRX 등 daily series 최신 관측일 허용 lag |
| `--macro-max-lag-days` | `SDC_COMMON_MACRO_MAX_LAG_DAYS` 또는 `45` | FRED/ECOS macro series 최신 관측일 허용 lag |
| `--series` | 없음 | smoke/특수 운영용 series allowlist |

`common-coverage-report.sh`는 기존 `common-features-refresh.sh`의 coverage step을 보존한다. coverage는 최종 gate는 아니지만 운영 로그에서 coverage/null/missing/PIT 상태를 빠르게 확인하는 진단 단계이므로 build와 readiness 사이에 둔다.

## 6. Cronicle Source별 재구성 방안

### 6.1 목표 구조

Cronicle event를 source별로 분리한다. 기존 큰 event는 비활성화하거나 삭제하고, 새 event들이 source별 wrapper를 직접 호출한다.

| 신규 event | source/domain | command | 의존성 |
| --- | --- | --- | --- |
| `sdc_daily_fdr_universe` | `fdr` | `/home/whi/apps/sdc/bin/universe-sync.sh` | 없음 |
| `sdc_daily_fdr_common` | `fdr` | `/home/whi/apps/sdc/bin/common-sync-fdr.sh` | `sdc_daily_fdr_universe`와 같은 시간대에 겹치지 않게 chain 또는 stagger |
| `sdc_daily_pykrx_prices` | `krx_marketdata` | `/home/whi/apps/sdc/bin/prices-backfill-incremental.sh` | `sdc_daily_fdr_universe` 성공 후 |
| `sdc_daily_krx_flows` | `krx_marketdata` | `/home/whi/apps/sdc/bin/flows-sync.sh` | `sdc_daily_pykrx_prices` 성공 후 |
| `sdc_daily_krx_common` | `krx_marketdata` | `/home/whi/apps/sdc/bin/common-sync-krx.sh` | `sdc_daily_krx_flows` 이후 또는 KRX price/flow 시간대와 충분히 분리된 schedule |
| `sdc_daily_pykrx_common` | `krx_marketdata` | `/home/whi/apps/sdc/bin/common-sync-pykrx.sh` | optional. 기본 비활성. 활성화 시 `sdc_daily_krx_common` 이후 |
| `sdc_daily_fred_common` | `fred` | `/home/whi/apps/sdc/bin/common-sync-fred.sh` | 없음 |
| `sdc_daily_ecos_common_daily` | `ecos` | `/home/whi/apps/sdc/bin/common-sync-ecos-daily.sh` | 없음 |
| `sdc_daily_ecos_common_macro` | `ecos` | `/home/whi/apps/sdc/bin/common-sync-ecos-macro.sh` | `sdc_daily_ecos_common_daily` 성공 후 또는 별도 schedule |
| `sdc_daily_common_build` | `local` | `/home/whi/apps/sdc/bin/common-build-daily.sh` | 필수 common source sync들 성공 후. all-success join이 어려우면 예상 종료 이후 schedule + freshness guard |
| `sdc_daily_common_coverage` | `local` | `/home/whi/apps/sdc/bin/common-coverage-report.sh` | `sdc_daily_common_build` 성공 후 |
| `sdc_daily_common_readiness` | `local` | `/home/whi/apps/sdc/bin/common-readiness-check.sh` | `sdc_daily_common_coverage` 성공 후 |
| `sdc_daily_opendart_corp` | `opendart` | `/home/whi/apps/sdc/bin/dart-sync-corp.sh` | 없음 |
| `sdc_daily_opendart_financials` | `opendart` | `/home/whi/apps/sdc/bin/dart-sync-financials.sh` | `sdc_daily_opendart_corp` 성공 후 |
| `sdc_daily_opendart_share_info` | `opendart` | `/home/whi/apps/sdc/bin/dart-sync-share-info.sh` | `sdc_daily_opendart_financials` 성공 후 |
| `sdc_daily_opendart_xbrl` | `opendart` | `/home/whi/apps/sdc/bin/dart-sync-xbrl.sh` | `sdc_daily_opendart_share_info` 성공 후 |
| `sdc_daily_metrics_normalize` | `local` | `/home/whi/apps/sdc/bin/metrics-normalize.sh` | `sdc_daily_opendart_xbrl` 성공 후 |

source event를 분리하더라도 wrapper lock은 유지한다. Cronicle chain 설정 실수, 수동 trigger, backfill 실행이 겹쳐도 같은 source는 wrapper 레벨에서 다시 보호한다.

### 6.2 Chain 구성

권장 chain은 세 갈래다.

```text
FDR/KRX price-flow chain:
sdc_daily_fdr_universe
  -> sdc_daily_pykrx_prices
  -> sdc_daily_krx_flows

Common feature chain:
sdc_daily_fdr_common
sdc_daily_fred_common
sdc_daily_ecos_common_daily
  -> sdc_daily_ecos_common_macro
sdc_daily_krx_common
(optional) sdc_daily_pykrx_common
[all required source syncs complete, or freshness guard passes at scheduled build time]
  -> sdc_daily_common_build
  -> sdc_daily_common_coverage
  -> sdc_daily_common_readiness

OpenDART chain:
sdc_daily_opendart_corp
  -> sdc_daily_opendart_financials
  -> sdc_daily_opendart_share_info
  -> sdc_daily_opendart_xbrl
  -> sdc_daily_metrics_normalize
```

Cronicle이 여러 upstream event의 all-success join을 직접 표현하기 어렵다면 `sdc_daily_common_build`는 common source sync 예상 종료 이후의 독립 schedule로 둔다. 이 경우에도 `common-build-daily.sh`가 `ops assert-common-freshness`로 필수 source별 최신 성공/최신 관측일을 검사하고, 미달이면 non-zero로 종료한다. coverage는 진단 단계, readiness는 최종 품질 gate다.

같은 source domain event는 lock으로 보호하지만, 기본 운영은 lock 충돌을 정상적인 queue로 쓰지 않는다. 초기 배포에서는 같은 domain event를 chain 또는 시간차 schedule로 배치하고, wrapper의 900초 lock wait은 작은 overlap을 흡수하는 안전망으로만 사용한다.

### 6.3 Timing 정책

초기 timing 예시는 다음과 같다. 실제 시간은 배포 직전 catch-up 상태와 API quota 상황을 확인한 뒤 확정한다.

| event group | 권장 시간대 | 이유 |
| --- | --- | --- |
| FDR universe | 장 마감 후 | 최신 상장 universe를 먼저 확보한다. |
| PYKRX prices | FDR universe 성공 후 | 현재 가격 provider가 PyKRX이고, 가격 최신일을 먼저 확보해야 flows range가 안정된다. |
| KRX flows | PYKRX prices 성공 후 | `flows sync --incremental`이 가격 최신일을 end로 사용한다. |
| common source sync | FDR/KRX/API 부하가 낮은 시간대 | 각 source lock으로 충돌은 막되, 실패 격리를 위해 event를 분리한다. 같은 source domain 안에서는 chain 또는 시간차 schedule을 둔다. |
| common build/coverage/readiness | common source sync 이후 | 외부 API 없이 DB 내부 산출물과 진단/품질 gate를 만든다. build 전 freshness guard로 stale raw 사용을 막는다. |
| OpenDART | KRX/FDR 주요 수집과 분리된 시간대 | OpenDART quota와 latency를 별도 관측한다. |
| metrics normalize | OpenDART raw sync 이후 | raw 수집 결과를 `stock_metric_fact`로 반영한다. |

### 6.4 기존 Event 처리

기존 event는 새 event가 등록되고 smoke가 끝난 뒤 비활성화한다.

| 기존 event | 처리 |
| --- | --- |
| `sdc_daily_pipeline` | `sdc_daily_fdr_universe`, `sdc_daily_pykrx_prices`, `sdc_daily_krx_flows`로 대체 |
| `sdc_daily_accounts_flows` | OpenDART chain과 metrics normalize event로 대체 |
| `sdc_daily_common_features` | common source sync/build/coverage/readiness event로 대체 |

전환 중 중복 실행을 피하기 위해 기존 event timing 활성화와 신규 event timing 활성화가 겹치지 않게 한다. 현재 running flows catch-up이 있으면 KRX 관련 신규 event trigger/timing은 보류한다.

## 7. 구현 단계

1. `deploy/prod/bin/lib/sdc-wrapper.sh`를 추가한다.
2. 기존 wrapper가 helper를 source하도록 바꾼다.
3. KRX/PYKRX market-data wrapper에 `krx_marketdata` lock/throttle을 적용한다.
4. FDR wrapper에 `fdr` lock/throttle을 적용한다.
5. OpenDART wrapper에 `opendart` lock/throttle을 적용한다.
6. common feature source별 wrapper를 추가한다. `common-sync-pykrx.sh`는 optional wrapper로 추가하되 기본 Cronicle 활성화 대상에서는 제외한다.
7. ECOS daily/macro wrapper를 분리하고 기존 두 패스의 lookback/series 정책을 보존한다.
8. `ops assert-common-freshness` CLI를 추가한다. `ops freshness-report`는 사람이 읽는 리포트로 유지하고, wrapper guard는 이 구조화된 assert 명령만 사용한다.
9. `common-build-daily.sh`에 `ops assert-common-freshness` 호출을 추가한다.
10. `common-coverage-report.sh`와 `common-readiness-check.sh`를 추가한다.
11. `common-features-refresh.sh`를 수동 호환 orchestration wrapper로 변경한다. pykrx common은 `SDC_COMMON_ENABLE_PYKRX=1`일 때만 호출한다.
12. `metrics-normalize.sh`를 incremental normalize wrapper로 정렬한다.
13. source별 Cronicle event 정의를 문서화하고, 기존 event 대체표를 `docs/deploy.md`에 반영한다.
14. shell syntax, wrapper lock/throttle smoke, parser smoke, unit test를 실행한다.
15. 배포 후 사용자 승인 하에 기존 Cronicle event를 비활성화하고 source별 신규 event를 등록/활성화한다.

## 8. 검증 계획

Shell syntax:

```bash
bash -n deploy/prod/bin/*.sh deploy/prod/bin/lib/*.sh
```

Parser smoke:

```bash
uv run python -c "from krx_collector.cli.app import build_parser; p=build_parser(); p.parse_args(['flows','sync','--incremental']); p.parse_args(['common','sync','--incremental','--sources','krx']); p.parse_args(['common','sync','--incremental','--sources','pykrx']); p.parse_args(['dart','sync-financials','--incremental']); p.parse_args(['metrics','normalize','--incremental']); p.parse_args(['ops','assert-common-freshness','--sources','fdr,fred,ecos,krx']); print('parser ok')"
```

Wrapper lock/throttle smoke:

```bash
tests/shell/sdc-wrapper-smoke.sh
```

검증 항목:

```text
1. 같은 domain lock을 동시에 잡으면 한쪽이 대기 또는 exit 75로 종료된다.
2. SDC_LOCK_CONFLICT_MODE=skip이면 lock 충돌 시 exit 0과 skip 로그를 남긴다.
3. 서로 다른 domain lock은 동시에 실행된다.
4. throttle marker가 있으면 source 호출 전에 sleep 로그가 남는다.
5. throttle marker는 lock 획득 뒤 source 호출 직전에 갱신된다.
```

Code checks:

```bash
uv run ruff check src tests
uv run pytest
```

운영 전 확인:

```bash
docker compose run --rm collector ops freshness-report
docker compose run --rm collector ops assert-common-freshness --sources fdr,fred,ecos,krx
```

또한 현재 실행 중인 flows catch-up이 있으면 같은 source의 wrapper smoke나 Cronicle trigger를 실행하지 않는다.

Cronicle 전환 검증:

```text
1. 기존 event timing=false 또는 disabled 상태 확인
2. source별 신규 event command가 새 wrapper를 직접 호출하는지 확인
3. 동일 source event를 수동 trigger했을 때 wrapper lock 로그가 남는지 확인
4. PYKRX prices 성공 후 KRX flows가 실행되는지 확인
5. OpenDART chain이 quota exhaustion exit code를 실패로 전달하는지 확인
6. common build가 필수 source guard 미달 시 build를 실행하지 않고 non-zero로 종료하는지 확인
7. common coverage event가 build 이후 진단 로그를 남기는지 확인
8. common readiness event가 최종 품질 gate로 실패를 전달하는지 확인
9. optional `sdc_daily_pykrx_common`은 기본 비활성 상태인지 확인
```

## 9. 완료 기준

- 운영 wrapper가 source별 lock domain을 갖는다.
- Cronicle event가 FDR/KRX/OpenDART/FRED/ECOS/local build 단위로 분리된다. PYKRX common event는 optional로 준비하되 기본 비활성으로 둔다.
- 기존 통합 Cronicle event는 source별 신규 event로 대체된다.
- `common-features-refresh.sh`가 수동 호환 orchestration wrapper가 된다.
- KRX/PYKRX/OpenDART/FDR/FRED/ECOS 호출이 source domain별 lock/throttle 설정을 사용할 수 있다.
- daily Cronicle wrapper는 같은 source overlap 시 최대 900초 대기하고, 이후에도 충돌하면 exit `75`로 실패한다.
- 가격 수집 wrapper는 현재 provider 기준으로 `krx_marketdata` lock을 사용한다.
- ECOS daily/macro 두 패스가 분리되거나 명확히 보존된다.
- common build는 `ops assert-common-freshness` 통과 없이 stale raw로 실행되지 않는다.
- common coverage report 단계가 build와 readiness 사이에 유지된다.
- `pykrx` common source는 wrapper만 준비되고 기본 필수 source/Cronicle 활성화 대상에서는 제외된다.
- OpenDART 세부 wrapper가 같은 quota domain을 공유한다.
- build/normalize/readiness는 외부 source lock과 분리된다.
- 목표 1에서 추가한 incremental 호출이 wrapper에 유지된다.
- `bash -n`, wrapper lock/throttle smoke, parser smoke, `ruff`, `pytest`가 통과한다.
- 배포는 목표 1과 목표 2가 모두 구현된 뒤 한 번에 진행한다.
