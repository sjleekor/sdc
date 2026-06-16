# 의존성 기준 Cronicle Chain 재구성 계획

- 작성일: 2026-06-16
- 개정: 2026-06-16 (검토 반영 — preflight/도메인 분리 제거, helper shift 버그 수정, rollback·실패표 정리, wrapper throttle 범위 명확화)
- 개정: 2026-06-16 (2차 검토 반영 — fallback의 exit-75 재도입 trade-off 명시, lock domain 표를 명명/실제-lock 2열로 분리, throttle gap 서술 정정, 실패표에 OpenDART·ecos_macro·normalize·coverage·readiness 행 추가)
- 범위: SDC daily Cronicle 이벤트 재구성, 운영 wrapper lock 정책, 전환 검증 계획
- 전제: 현재 네트워크에서는 `sj2-server`에 접근하지 않고, 프로젝트 내 코드와 문서만 기준으로 작성한다.
- 관련 문서:
  - `docs/dev/20260615_cronicles_events_summary/cronicle_events_overview.md`
  - `docs/dev/20260613_refactoring/source_wrapper_throttling_plan.md`

## 1. 목표

기존 구조는 source별 lock으로 서로 다른 Cronicle chain/timing에서 같은 외부 source를 동시에 치지 못하게 막는다. 2026-06-15 장애의 근원은 `krx_common`이 별도 timing(21:30) 이벤트로 떠서 18:30 KRX 체인(`prices`/`flows`)과 같은 `krx_marketdata` lock을 공유하다 충돌(exit 75)한 데 있다.

핵심 통찰: **같은 source를 치는 daily 이벤트를 하나의 chain 안에서 순차 실행하면, 동시 실행이 구조적으로 발생하지 않으므로 daily path에 source lock이 필요 없다.**

목표는 다음과 같다.

1. Cronicle daily path를 **실제 데이터 의존성**을 따르는 소수의 chain으로 정리한다. 같은 source를 치는 daily 이벤트는 같은 chain 안에서 순차 실행되어 동시 실행이 구조적으로 발생하지 않는다.
2. KRX daily 경로(`prices` → `flows` → `krx_common`)를 하나의 chain으로 통합해, 별도 timing 이벤트가 source lock을 공유하다 충돌하던 문제를 제거한다.
3. source별 API 과부하 방지는 daily wrapper의 process-level lock 대신 chain 순서 + CLI별 기존 rate-limit/retry/throttle 정책으로 처리한다.
4. manual/backfill 계열은 daily chain 밖에서 실행될 수 있으므로 별도 source lock 보호를 유지한다.
5. Common build는 all-source join을 Cronicle로 직접 표현하는 대신 기존 freshness guard로 대체한다.

핵심 재구성은 **신규 Cronicle 이벤트나 신규 collector CLI를 추가하지 않는다.** 기존 16개 이벤트의 chain/timing 변경과 daily wrapper의 lock 호출 교체만으로 daily path의 source lock 제거를 완성한다. 단, backfill 안전성을 높이기 위한 wrapper-level Cronicle schedule 자동 가드는 Phase 3에서 별도 보강으로 검토한다(5.3).

## 2. 결정 사항

### 2.1 Daily source lock 제거 범위

결정: **Cronicle daily path wrapper에서는 source lock을 제거한다.**

대상:

- `universe-sync.sh`
- `prices-backfill-incremental.sh`
- `flows-sync.sh`
- `common-sync-fdr.sh`
- `common-sync-fred.sh`
- `common-sync-ecos-daily.sh`
- `common-sync-ecos-macro.sh`
- `common-sync-krx.sh`
- `common-sync-pykrx.sh` (optional, Cronicle 이벤트 미등록 — 기본 비활성)
- `dart-sync-corp.sh`
- `dart-sync-financials.sh`
- `dart-sync-share-info.sh`
- `dart-sync-xbrl.sh`

이 wrapper들은 target state에서 `sdc_run_collector_with_lock <domain> ...` 대신 `sdc_run_daily_collector <domain> ...`를 호출한다(5장). `metrics-normalize.sh`, `common-build-daily.sh`, `common-coverage-report.sh`, `common-readiness-check.sh`는 이미 lock 없이 `sdc_run_collector`만 호출하므로 변경 대상이 아니다.

> 참고: `common-features-refresh.sh`는 저장소에 존재하지만 현재 등록된 16개 Cronicle 이벤트 어디에서도 호출하지 않는다. 이 계획의 범위 밖이며 변경하지 않는다.

### 2.2 Manual/backfill lock 유지

결정: **manual/backfill wrapper는 source lock을 유지한다.**

대상:

- `flows-backfill-range.sh`
- `dart-backfill-all-years.sh`
- 향후 추가되는 explicit range backfill wrapper

이유:

- Cronicle daily chain 밖에서 사람이 실행할 수 있다.
- 장기 backfill은 해당 source quota를 오래 점유할 수 있으므로 자기 자신의 중복 실행을 막아야 한다.

다만 daily wrapper가 더 이상 lock을 잡지 않으므로(2.1), backfill의 source lock은 **daily 실행을 막지 못한다.** backfill lock의 실효 범위는 다음 둘로 한정된다.

1. backfill 끼리의 중복 실행 방지(같은 backfill 두 번 동시 실행 차단).
2. daily wrapper가 fallback으로 `SDC_DAILY_USE_SOURCE_LOCK=1`을 켠 경우에 한해 daily와 **상호배제(graceful queue 아님, hard-fail)**.

> ⚠️ fallback 상호배제의 정확한 동작: 이 "상호배제"는 둘 중 늦게 온 쪽이 대기하다 이어 실행되는 graceful queueing이 아니라, **둘 중 하나가 exit-75로 실패**하는 hard-fail이다. 이유는 lock-wait 비대칭이다. daily wrapper는 fallback에서 `sdc_use_daily_lock_defaults`를 통해 `SDC_LOCK_WAIT_SECONDS=900`을 받지만, backfill wrapper(`flows-backfill-range.sh`)는 `sdc_use_daily_lock_defaults`를 호출하지 않으므로 `SDC_LOCK_WAIT_SECONDS`가 기본 `0`이다(`sdc-wrapper.sh`의 `sdc_with_source_lock` default). 따라서 daily가 lock을 쥐고 있는 동안 backfill이 들어오면 backfill은 0초 대기 후 즉시 exit-75로 실패한다(그 반대 방향이면 daily가 최대 900초 대기 후 실패). **즉 긴급 fallback은 이 재구성이 1장에서 없애려던 바로 그 exit-75 lock 충돌 실패모드를 daily–backfill 경계에서 다시 도입한다.** 이는 의도된 trade-off다 — fallback은 "lock 보호를 통째로 되살려 안전 우선으로 즉시 되돌리는" 비상 스위치이지, 정상 운영 경로가 아니다. backfill을 안전하게 돌리는 1차 방어선은 어디까지나 daily event disable(아래)이며, fallback과 backfill을 동시에 켜는 상황 자체를 피해야 한다.

평상시 daily–backfill overlap 방지는 lock이 아니라 **운영 절차**(7장 Phase 3: backfill 전 해당 domain daily event disable)가 유일한 1차 방어선이다. 이는 사람이 disable을 빠뜨리면 즉시 깨지는 휴먼 에러 취약점이다. 따라서 운영 문서화만으로 끝내지 않고, 가능하면 backfill wrapper가 실행 전 Cronicle schedule 상태를 확인하는 자동 가드를 우선 검토한다(5.3).

### 2.3 KRX chain 통합 (재구성의 핵심)

결정: **`krx_common`의 독립 timing(21:30)을 제거하고 `krx_flows` 뒤에 chain으로 붙인다.**

2026-06-15 장애의 핵심은 `prices -> flows` chain과 `krx_common`이 별도 timing으로 실행되어 `krx_marketdata` lock에 의존한다는 점이었다. target state에서는 다음 하나의 KRX data chain으로 통합한다.

```text
sdc_daily_fdr_universe
  -> sdc_daily_pykrx_prices
  -> sdc_daily_krx_flows
  -> sdc_daily_krx_common
```

`krx_common`이 `krx_flows` 성공 이후에만 실행되므로, daily path에서 `krx_marketdata`를 동시에 치는 일이 구조적으로 사라진다. flows catch-up이 길어져도 `krx_common`은 lock 대기/실패 없이 chain 순서대로 그 뒤에 실행될 뿐이다.

### 2.4 FDR universe와 KRX prices 의존성

결정: **`fdr_universe`를 KRX data chain의 head로 유지한다(기존 구조 유지). 별도 preflight 이벤트나 universe freshness CLI는 도입하지 않는다.**

근거:

- 가격 수집은 최신 `stock_master` universe에 의존한다. 이 의존성은 `fdr_universe -> pykrx_prices` chain link로 **이미 정확하게 표현**되어 있다. universe가 실패하면 chain이 정지해 `prices`/`flows`/`krx_common`이 실행되지 않으므로 stale universe로 수집되는 일이 없다. 이는 hard guarantee다.
- 검토 단계에서 고려한 대안(별도 `fdr` chain으로 universe를 떼어내고 KRX chain 앞에 `krx_preflight` + `ops assert-universe-freshness` 신규 CLI 추가)은 다음 이유로 채택하지 않는다.
  - 신규 Cronicle 이벤트 + 신규 CLI 구현/테스트/운영 부담이 추가된다.
  - preflight의 "최근 successful universe run age ≤ N시간" 검사는 soft 휴리스틱이라, age를 넉넉히 두면 **어제 run으로도 통과**해 "오늘 universe 실패"를 못 잡고, 당일로 좁히면 18:30 universe와 preflight 사이 race가 예민해진다.
  - chain link가 제공하는 hard dependency보다 완결성이 떨어진다.
- `fdr_common`은 universe가 아니라 시장/매크로 공통 피처를 FDR에서 sync하므로 universe에 의존하지 않는다. 따라서 KRX chain에 묶지 않고 **독립 root(평일 20:30)** 로 유지한다.

결과적으로 `fdr` source는 `fdr_universe`(18:30, chain head)와 `fdr_common`(20:30, 독립 root)이 서로 다른 timing에 실행되어 동시 실행이 발생하지 않는다.

### 2.5 Common build timing

결정: **Common build는 source chain들과 분리된 scheduled root로 유지하되 22:30에서 23:30 KST로 늦춘다.**

이유:

- Cronicle에서 `fdr`, `fred`, `ecos`, `krx` 네 source의 all-success join을 단순하게 표현하기 어렵다. 기존 freshness guard가 이 join 역할을 안전하게 대체한다.
- `krx_common`이 이제 KRX chain의 맨 뒤(`flows` 이후)에서 실행되므로, 기존 21:30 독립 실행보다 완료 시각이 늦어질 수 있다. flows catch-up까지 감안하면 기존 22:30보다 늦은 23:30이 안정적이다.
- catch-up이 매우 길어 `krx_common`이 23:30까지 못 끝나는 날에는 build의 freshness guard가 실패해 stale fact 생성을 막는다(의도된 동작, regression 아님).

Target:

```text
sdc_daily_common_build
  -> sdc_daily_common_coverage
  -> sdc_daily_common_readiness
```

`common-build-daily.sh`의 `ops assert-common-freshness --sources fdr,fred,ecos,krx`는 유지한다.

## 3. Target Event Graph

### 3.1 Chain 구성

| chain | root event | 흐름 | timing |
|---|---|---|---|
| KRX data | `sdc_daily_fdr_universe` | `universe -> pykrx_prices -> krx_flows -> krx_common` | 평일 18:30 |
| FDR common | `sdc_daily_fdr_common` | 단일 이벤트 | 평일 20:30 |
| FRED common | `sdc_daily_fred_common` | 단일 이벤트 | 평일 20:30 |
| ECOS | `sdc_daily_ecos_common_daily` | `ecos_daily -> ecos_macro` | 평일 20:30 |
| OpenDART | `sdc_daily_opendart_corp` | `corp -> financials -> share_info -> xbrl -> metrics_normalize` | 매일 04:00 |
| Common build | `sdc_daily_common_build` | `build -> coverage -> readiness` | 평일 23:30 |

source lock domain 매핑은 다음과 같다. "명명 domain"은 wrapper가 lock helper에 인자로 넘기는 domain 이름이고, "target state 실제 lock"은 기본값(`SDC_DAILY_USE_SOURCE_LOCK=0`)에서 그 wrapper가 실제로 lock을 잡는지를 가리킨다. daily wrapper는 target state에서 `sdc_run_daily_collector`를 통해 lock 없이 실행되므로 domain을 "명명"만 할 뿐 lock을 잡지 않는다. **target state에서 `krx_marketdata` lock을 실제로 잡는 wrapper는 backfill인 `flows-backfill-range.sh`뿐이고, `opendart`는 `dart-backfill-all-years.sh`뿐이다. `fdr`/`fred`/`ecos`는 backfill wrapper가 없어 target state에서 실제로 lock을 잡는 경로가 없다.**

| 명명 domain | wrapper | 종류 | target state 실제 lock |
|---|---|---|---|
| `fdr` | `universe-sync.sh` | daily | ✗ (lock 미사용) |
| `fdr` | `common-sync-fdr.sh` | daily | ✗ (lock 미사용) |
| `krx_marketdata` | `prices-backfill-incremental.sh` | daily | ✗ (lock 미사용) |
| `krx_marketdata` | `flows-sync.sh` | daily | ✗ (lock 미사용) |
| `krx_marketdata` | `common-sync-krx.sh` | daily | ✗ (lock 미사용) |
| `krx_marketdata` | `common-sync-pykrx.sh`(미등록) | daily | ✗ (lock 미사용) |
| `krx_marketdata` | `flows-backfill-range.sh` | backfill | ✓ (lock 유지) |
| `fred` | `common-sync-fred.sh` | daily | ✗ (lock 미사용) |
| `ecos` | `common-sync-ecos-daily.sh` | daily | ✗ (lock 미사용) |
| `ecos` | `common-sync-ecos-macro.sh` | daily | ✗ (lock 미사용) |
| `opendart` | `dart-sync-{corp,financials,share-info,xbrl}.sh` | daily | ✗ (lock 미사용) |
| `opendart` | `dart-backfill-all-years.sh` | backfill | ✓ (lock 유지) |

daily path에서는 같은 lock domain의 이벤트가 하나의 chain 안에서 순차 실행되므로(예: `prices`/`flows`/`krx_common`은 모두 KRX data chain 내부) lock 없이도 동시 실행이 발생하지 않는다. 따라서 위 표에서 보듯 lock은 ✓ 표시된 manual/backfill wrapper에서만 실제 의미를 가진다(2.2). daily wrapper의 "명명 domain"은 fallback(`SDC_DAILY_USE_SOURCE_LOCK=1`)을 켰을 때 어느 lock을 잡을지를 정의하는 용도로만 남는다.

### 3.2 Event naming convention

결정: **Cronicle event id prefix로 scheduled/daily job과 manual job을 명확히 구분한다.**

기존 daily event는 이미 `sdc_daily_` prefix를 사용하므로 유지한다. 향후 만드는 수동 실행용 event는 `sdc_manual_` prefix를 사용한다(이 계획에서 신규 생성하는 manual event는 없다 — 4.2 참조).

| prefix | 용도 | timing | 예 |
|---|---|---|---|
| `sdc_daily_` | 주기적으로 자동 실행되는 production daily/scheduled chain | 있음(root) 또는 chain-only | `sdc_daily_pykrx_prices`, `sdc_daily_common_build` |
| `sdc_manual_` | 사람이 의도적으로 실행하는 보수/진단/일회성 작업 | 없음, manual trigger only | `sdc_manual_backfill_krx_flows_range`, `sdc_manual_backfill_opendart_all_years` |

Manual event 세부 이름은 목적을 prefix 뒤에 붙인다.

- `sdc_manual_backfill_*`: 장기 또는 명시 range backfill
- `sdc_manual_repair_*`: 특정 장애 복구/재처리
- `sdc_manual_diag_*`: read-only 진단/점검

운영 규칙:

1. `sdc_daily_` event는 사용자가 직접 trigger할 수는 있지만, 설계상 scheduled/chain graph의 일부로 본다.
2. `sdc_manual_` event는 Cronicle timing을 두지 않는다.
3. `sdc_manual_` event는 기본 daily chain의 chain target으로 연결하지 않는다.
4. `sdc_manual_backfill_*` event는 source lock을 유지하는 wrapper만 호출한다.
5. Manual event를 추가할 때는 동일한 wrapper를 호출하더라도 `sdc_daily_` event와 event id를 공유하지 않는다.

### 3.3 평일 타임라인

```text
04:00  opendart:
       corp -> financials -> share-info -> xbrl -> metrics-normalize

18:30  krx_data:
       universe -> pykrx_prices -> krx_flows -> krx_common

20:30  fdr_common      (단일)
20:30  fred_common     (단일)
20:30  ecos:
       ecos_daily -> ecos_macro

23:30  common:
       common_build -> common_coverage -> common_readiness
```

`fdr_universe`(fdr source)가 KRX data chain의 head에 있는 이유는 `pykrx_prices`가 최신 universe에 선행 의존하기 때문이다(2.4). universe가 실패하면 chain이 정지해 stale universe로 가격을 수집하지 않는다. `fdr_common`은 universe에 의존하지 않으므로 별도 root다.

## 4. Event 변경안

### 4.1 변경 요약

기존 event id는 운영 이력/알림 continuity를 위해 모두 유지한다. 실제 변경은 **3개 event의 chain/timing 속성**뿐이다.

| event id | 변경 | 비고 |
|---|---|---|
| `sdc_daily_opendart_corp` | 변경 없음 | timing 04:00, chain 유지 |
| `sdc_daily_opendart_financials` | 변경 없음 | chain-only |
| `sdc_daily_opendart_share_info` | 변경 없음 | chain-only |
| `sdc_daily_opendart_xbrl` | 변경 없음 | chain-only |
| `sdc_daily_metrics_normalize` | 변경 없음 | chain-only |
| `sdc_daily_fdr_universe` | 변경 없음 | timing 18:30, chain → `pykrx_prices` |
| `sdc_daily_fdr_common` | 변경 없음 | timing 20:30, 단일 root |
| `sdc_daily_pykrx_prices` | 변경 없음 | chain-only, chain → `krx_flows` |
| `sdc_daily_krx_flows` | **chain을 `sdc_daily_krx_common`으로 설정** | 기존 chain target 없음 → 추가 |
| `sdc_daily_krx_common` | **timing 제거(21:30 → false), `krx_flows`의 chain target으로 전환** | root → chain-only |
| `sdc_daily_fred_common` | 변경 없음 | timing 20:30 |
| `sdc_daily_ecos_common_daily` | 변경 없음 | timing 20:30, chain → `ecos_common_macro` |
| `sdc_daily_ecos_common_macro` | 변경 없음 | chain-only |
| `sdc_daily_common_build` | **timing 22:30 → 23:30** | chain → `common_coverage` |
| `sdc_daily_common_coverage` | 변경 없음 | chain-only |
| `sdc_daily_common_readiness` | 변경 없음 | chain-only |

### 4.2 신규 event

**없음.** 이 재구성은 신규 Cronicle 이벤트를 추가하지 않는다. (이전 초안의 `sdc_daily_krx_preflight` + `ops assert-universe-freshness` CLI 안은 2.4 근거에 따라 폐기했다.)

## 5. Wrapper 변경안

### 5.1 Helper 추가

`deploy/prod/bin/lib/sdc-wrapper.sh`에 daily 전용 helper를 추가한다. daily wrapper는 이 helper만 호출하고, lock 사용 여부는 `SDC_DAILY_USE_SOURCE_LOCK` 플래그로 제어한다.

```bash
sdc_run_daily_collector() {
  local domain="$1"
  shift
  if [[ "${SDC_DAILY_USE_SOURCE_LOCK:-0}" == "1" ]]; then
    sdc_use_daily_lock_defaults
    sdc_run_collector_with_lock "$domain" "$@"
  else
    sdc_run_collector "$@"
  fi
}
```

설계 포인트:

- `local domain="$1"; shift`를 **분기 밖에서 먼저** 수행한다. lock을 쓰지 않는 기본 경로(`SDC_DAILY_USE_SOURCE_LOCK=0`)에서도 domain 인자가 CLI로 새어 들어가지 않게 한다. (이 shift를 분기 안에만 두면 lock=0일 때 `sdc_run_collector krx_marketdata prices backfill ...`처럼 domain이 CLI 첫 인자로 전달되어 argparse가 죽는다.)
- `sdc_use_daily_lock_defaults` 호출을 lock=1 분기 안으로 흡수한다. 그러면 daily wrapper는 이 함수를 따로 부를 필요가 없다(5.2).
- Target state 기본값은 `SDC_DAILY_USE_SOURCE_LOCK=0`(lock 미사용)이다. 전환 중 문제가 생기면 Cronicle event script에서 `1`로 임시 override해 기존 lock 보호로 즉시 되돌릴 수 있다.

### 5.2 Daily wrapper 수정

Daily wrapper는 기존의 두 줄(`sdc_use_daily_lock_defaults` + `sdc_run_collector_with_lock`)을 helper 한 줄로 교체한다.

```bash
# before
sdc_use_daily_lock_defaults
sdc_run_collector_with_lock krx_marketdata "${args[@]}"

# after
sdc_run_daily_collector krx_marketdata "${args[@]}"
```

`sdc_use_daily_lock_defaults` 직접 호출은 모든 daily wrapper에서 제거한다(helper 내부에서 lock=1일 때만 호출). domain 인자는 기존 lock domain 매핑(3.1 표)을 그대로 사용한다.

### 5.3 Backfill wrapper 유지

Backfill wrapper는 기존처럼 명시 lock을 사용한다.

```bash
sdc_run_collector_with_lock krx_marketdata ...
sdc_with_source_lock opendart ...
```

단, daily wrapper가 더 이상 lock을 잡지 않으므로 backfill lock의 실효 범위는 2.2에 정리한 둘로 한정된다. 즉 backfill lock은 daily 실행을 전혀 막지 못한다. 따라서 backfill 실행 시에는 다음 순서로 방어선을 둔다.

1. 해당 domain의 daily event를 Cronicle에서 일시 disable한다. **(필수 운영 절차)**
2. backfill wrapper가 실행 전 Cronicle schedule 상태를 확인하고, 관련 daily root event가 `enabled=1`이면서 `timing`이 활성 상태면 실패하도록 자동 가드를 둔다. **(권장)**
3. daily wrapper도 임시로 `SDC_DAILY_USE_SOURCE_LOCK=1`을 켠다. **(긴급 fallback)**

자동 가드 후보:

- `krx_marketdata` backfill(`flows-backfill-range.sh`)은 KRX data chain root인 `sdc_daily_fdr_universe`가 비활성화되었는지 확인한다.
- `opendart` backfill(`dart-backfill-all-years.sh`)은 OpenDART chain root인 `sdc_daily_opendart_corp`가 비활성화되었는지 확인한다.
- 관련 daily job이 이미 running이면 backfill을 시작하지 않는다.
- Cronicle API 확인이 실패하면 long backfill은 fail-closed를 기본으로 하고, 예외적으로만 `SDC_BACKFILL_ALLOW_DAILY_OVERLAP=1` 같은 명시 override를 허용한다.

Trade-off: 이 자동 가드는 신규 collector CLI나 신규 Cronicle event를 만들지는 않지만, host wrapper에 Cronicle API credential/network 의존성을 추가한다. "기존 16개 event의 chain/timing 변경 + daily wrapper lock 호출 교체"라는 최소 변경 원칙보다 범위가 넓다. 따라서 Phase 3에서 운영 문서화와 함께 우선 검토하되, credential 배포/장애 시 fail-closed 정책까지 확인한 뒤 적용한다.

### 5.4 Wrapper-level throttle 처리

기존 wrapper-level throttle(`sdc_throttle` / `/tmp/sdc-throttle/<domain>.last` 마커)은 `sdc_with_source_lock` 내부에서만 호출된다. daily wrapper가 lock을 잡지 않으면 이 inter-run throttle은 daily path에서 동작하지 않는다.

이는 기본적으로 의도된 변경이다. 같은 source를 치는 daily 이벤트가 하나의 chain 안에서 순차 실행되므로(3.1), wrapper-level throttle이 맡던 **동시 실행 간격 조절**의 보호 가치는 줄어든다. 다만 "source별 request 간 간격은 `HumanThrottlePolicy`가 담당한다"로 일반화하면 부정확하다.

현재 request/request-group 간격 정책은 CLI별로 다르다.

| 경로 | daily lock 제거 후 남는 간격/보호 정책 |
|---|---|
| KRX flows | `HumanThrottlePolicy`로 KRX MDC HTTP request spacing/long rest/auth cooldown/error backoff 적용 + `krx_logical_rate_limit_seconds` request-group sleep |
| KRX common | `common_features_krx` provider가 `HumanThrottlePolicy` 적용. `common sync` loop의 `--rate-limit-seconds`도 적용 |
| prices(PyKRX) | `backfill_daily_prices`의 `rate_limit_seconds`/long-rest 정책 적용. `HumanThrottlePolicy` 경로 아님 |
| FDR/FRED/ECOS common | `common sync` loop의 `--rate-limit-seconds`와 provider 결과의 retry/backoff(`retry_after_seconds`)에 의존. `HumanThrottlePolicy` 경로 아님 |
| OpenDART corp | 단일 corp-code master fetch + retry/key-rotation 정책에 의존 |
| OpenDART financials/share-info/xbrl | 각 CLI 내부의 `--rate-limit-seconds`(기본 0.2s) request/request-group sleep + `OpenDartRequestExecutor` key rotation/cooldown + quota guard에 의존 |

따라서 Phase 2에서 daily lock을 끄면 wrapper-level inter-run throttle 기본값(`krx_marketdata=60s`, `opendart=5s`, `fdr/fred/ecos=10s`)은 daily path에서 사라진다. KRX는 CLI 내부 throttle이 비교적 강하게 남는다.

> 정정: `sdc_throttle`은 "단계 간 고정 gap"이 아니다. `sdc-wrapper.sh`의 로직은 **직전 마커(`<domain>.last`) 이후 경과시간이 `min_interval`(opendart 5s, fdr/fred/ecos 10s) 미만일 때만** 그 차이만큼 sleep하고, 마커는 각 단계가 lock을 잡는 **시작 시점**에 갱신된다. financials/share-info처럼 한 단계가 수 분~15분+ 걸리는 OpenDART chain에서는 다음 단계 진입 시 elapsed ≫ `min_interval`이라 거의 항상 `throttle pass`(sleep 없음)로 통과한다. 즉 daily lock 제거로 실제로 사라지는 건 "모든 단계 간 5~10초 gap"이 아니라, **앞 단계가 매우 짧아 5/10초 안에 다음 단계가 시작되는 빠른 전환(예: corp→financials 초기, 또는 캐시 히트로 즉시 끝난 단계)에서만 남아 있던 잔여 sleep**에 한정된다. 같은 이유로 ECOS daily→macro 전환에서도 daily 단계가 5초 이상 걸렸다면 throttle은 이미 pass였다.

이 잔여 gap 제거가 OpenDART quota나 운영 안정성에 실제로 필요한지는 별도 검증한다. 현재 OpenDART daily chain은 순차 실행이고 각 단계 내부에 request-level sleep, retry, key cooldown, attempt guard가 있으므로 동시 실행 위험은 없다. 하지만 전환 직후 OpenDART `020` rate-limit 증가, `all_rate_limited` exit 75 증가, 단계 시작 직후 실패 증가가 관측되면 다음 중 하나를 적용한다.

1. OpenDART daily event만 임시로 `SDC_DAILY_USE_SOURCE_LOCK=1`을 켜서 기존 `opendart` 5초 wrapper throttle을 복구한다.
2. lock 없이 단계 시작 전 sleep만 수행하는 별도 helper를 추가할지 검토한다.
3. OpenDART wrapper에 명시적 `SDC_OPENDART_CHAIN_STEP_SLEEP_SECONDS`를 추가할지 검토한다.

backfill path는 여전히 lock을 잡으므로 wrapper-level throttle도 그대로 적용된다.

### 5.5 Cross-day KRX overlap 잔여 리스크

Daily chain은 **한 번 시작된 같은 날짜의 chain 내부**에서 `universe -> prices -> flows -> krx_common` 순서를 보장한다. 하지만 day N의 `flows` catch-up이 24시간 이상 길어져 day N+1 18:30 root가 다시 시작되는 경우, day N+1의 `pykrx_prices`는 기본값(`SDC_DAILY_USE_SOURCE_LOCK=0`)에서 source lock을 잡지 않으므로 day N의 `flows`와 동시에 KRX 계열 endpoint를 호출할 수 있다.

이는 의도된 trade-off다. 기존 lock 구조는 이 상황을 graceful queue가 아니라 `exit 75` hard-fail로 만들었고, 새 구조는 같은 날짜 chain의 정상 동시성 문제를 제거하는 대신 cross-day 장기 catch-up overlap 가능성은 남긴다. 두 프로세스가 동시에 돌면 CLI 내부 request spacing은 각 프로세스 단위로만 적용되므로 aggregate KRX request rate는 증가할 수 있다.

운영 대응:

1. `flows`가 다음 영업일 18:30까지 끝나지 않는 장기 catch-up 상태면 다음 daily root(`sdc_daily_fdr_universe`)를 일시 disable한다.
2. 긴급히 hard-fail 보호를 되살려야 하면 관련 daily event script에 `SDC_DAILY_USE_SOURCE_LOCK=1`을 주입한다.
3. 같은 현상이 반복되면 KRX chain root 실행 전 active KRX job을 확인하는 자동 guard 또는 queue 정책을 별도 보강으로 검토한다.

## 6. Common Build Freshness Guard

Common build는 source chain의 직접 chain target으로 만들지 않는다.

유지할 guard:

```text
ops assert-common-freshness --sources fdr,fred,ecos,krx
```

판정 기준:

- source별 마지막 successful `common_feature_sync` run age(기본 ≤ 30h)
- daily source 최신 관측일 lag(기본 ≤ 2일)
- macro source 최신 관측일 lag(기본 ≤ 60일)

이 guard가 실패하면 `common build-daily`를 실행하지 않고 build event를 실패시킨다. 따라서 source chain 중 하나가 실패해 raw가 stale하면 fact를 만들지 않는다. `krx_common`이 KRX chain 맨 뒤로 이동(2.3)했으므로, flows catch-up이 길어 `krx_common`이 늦거나 누락되면 이 guard가 build를 차단한다.

## 7. 전환 순서

### Phase 1. Cronicle chain/timing만 먼저 변경

목표: wrapper lock은 그대로 둔 상태에서 새 chain graph가 의도대로 동작하는지 확인한다.

작업:

1. `sdc_daily_krx_flows.chain = sdc_daily_krx_common`
2. `sdc_daily_krx_common.timing = false`
3. `sdc_daily_common_build` timing을 평일 23:30으로 변경

검증:

- KRX data chain이 `universe -> prices -> flows -> krx_common` 순서로 실행되는지 확인한다.
- `krx_common`이 더 이상 21:30 독립 timing으로 뜨지 않고, lock wait/충돌(exit 75) 없이 chain 순서로 실행되는지 확인한다.
- `common_build`가 freshness guard 통과 후 실행되는지 확인한다.

### Phase 2. Daily wrapper lock optional화

목표: daily wrapper에서 lock 제거를 feature flag로 제어할 수 있게 한다.

작업:

1. `sdc_run_daily_collector` helper 추가(5.1)
2. daily wrapper의 `sdc_use_daily_lock_defaults` + `sdc_run_collector_with_lock` 두 줄을 `sdc_run_daily_collector` 한 줄로 교체(5.2)
3. Cronicle daily event script에는 `SDC_DAILY_USE_SOURCE_LOCK`을 설정하지 않는다(기본 `0`).
4. 기존 임시 `SDC_LOCK_WAIT_SECONDS=1800` override(`krx_common`/`pykrx_prices`/`krx_flows`)는 제거한다 — lock을 잡지 않는 기본 경로에서는 무의미하고, fallback이 필요하면 `SDC_DAILY_USE_SOURCE_LOCK=1`과 함께 명시적으로 다시 설정한다.

검증:

- Cronicle daily 실행 로그에 source lock wait/acquired/released 로그가 더 이상 나오지 않는지 확인한다.
- KRX flows/common의 `HumanThrottlePolicy` 로그와 CLI별 `rate_limit_seconds` 기반 sleep/retry 동작이 기존대로 유지되는지 확인한다.
- OpenDART chain은 빠른 단계 전환에서의 잔여 wrapper sleep(5.4)이 사라진 뒤 `020` rate-limit, `all_rate_limited` exit 75, 단계 시작 직후 실패가 증가하지 않는지 확인한다.
- source별 chain 순서가 깨지지 않는지 확인한다.

### Phase 3. Manual/backfill 운영 절차 정리

목표: daily lock 제거 후에도 장기 backfill을 안전하게 실행할 수 있게 한다.

작업:

1. `flows-backfill-range.sh`와 `dart-backfill-all-years.sh`는 lock 유지 확인.
2. backfill wrapper의 Cronicle schedule 자동 가드 적용 여부를 결정한다. 적용 시 관련 daily root event가 `enabled=1`이고 `timing`이 활성 상태이거나 이미 running이면 backfill을 fail-closed로 중단하고, 명시 override(`SDC_BACKFILL_ALLOW_DAILY_OVERLAP=1`)만 예외로 허용한다(5.3).
3. 운영 문서(`docs/operations.md`)에 "backfill 전 해당 domain daily event disable" 절차를 1차 방어선으로 추가(2.2, 5.3).
4. 긴급 fallback으로 `SDC_DAILY_USE_SOURCE_LOCK=1` 적용 방법을 문서화.

검증:

- daily event disabled 상태에서 backfill이 source lock을 잡고 실행되는지 확인한다.
- daily root event가 `enabled=1`이고 `timing`이 활성 상태이거나 running 상태일 때 자동 가드가 backfill을 차단하는지 확인한다(자동 가드 적용 시).
- daily event 재활성화 후 다음 scheduled run이 정상 실행되는지 확인한다.

## 8. 실패 동작

Cronicle chain은 **앞 단계가 exit 0(성공)일 때만** 다음 단계를 잇고, 실패하면 그 지점에서 체인이 멈춘다(실패 분기 `chain_error` 없음 — `cronicle_events_overview.md`). 따라서 한 단계 실패는 같은 chain의 뒤 단계 전부를 미실행으로 만든다. 아래 표는 각 단계 실패 시 같은 chain의 후속 단계와 cross-chain 영향(common build freshness guard)을 모두 나열한다.

| chain | 실패 지점 | 기대 동작 |
|---|---|---|
| KRX data | `fdr_universe` 실패 | `pykrx_prices`/`krx_flows`/`krx_common` 미실행(chain 정지). `fdr_common`은 독립 root라 영향 없음. krx freshness 미충족 → common build guard 실패 가능 |
| KRX data | `pykrx_prices` 실패 | `krx_flows`/`krx_common` 미실행. krx freshness 미충족 → common build guard 실패 가능 |
| KRX data | `krx_flows` 실패 | `krx_common` 미실행. krx freshness 미충족 → common build guard 실패 가능 |
| KRX data | `krx_common` 미실행/실패 | KRX chain의 마지막 단계라 후속 미실행 없음. krx source freshness 미충족 → common build guard 실패 가능 |
| FDR common | `fdr_common` 실패 | 단일 이벤트(후속 없음). fdr freshness 미충족 → common build guard 실패 가능 |
| FRED common | `fred_common` 실패 | 단일 이벤트(후속 없음). fred freshness 미충족 → common build guard 실패 가능 |
| ECOS | `ecos_daily` 실패 | `ecos_macro` 미실행. ecos freshness 미충족 → common build guard 실패 가능 |
| ECOS | `ecos_macro` 실패 | ECOS chain의 마지막 단계라 후속 미실행 없음. macro 계열 lag 미충족 → common build guard 실패 가능 |
| OpenDART | `opendart_corp` 실패 | `financials`/`share_info`/`xbrl`/`metrics_normalize` 전부 미실행(전체 DART chain 정지). corp master fetch 단일 단계이므로 부분 처리 없음 |
| OpenDART | `opendart_financials` 실패 | `share_info`/`xbrl`/`metrics_normalize` 미실행 |
| OpenDART | `opendart_share_info` 가드/실패 | `xbrl`/`metrics_normalize` 미실행. `--max-attempt-targets` 가드 초과 시 부분 처리 없이 exit 1로 즉시 실패(2026-06-15 사례) |
| OpenDART | `opendart_xbrl` 실패 | `metrics_normalize` 미실행 |
| OpenDART | `metrics_normalize` 실패 | OpenDART chain의 마지막 단계라 후속 미실행 없음. canonical `stock_metric_fact` 갱신 누락(external API 호출 없는 DB 내부 정규화라 quota 영향 없음) |
| Common build | `common_build` freshness guard 실패 | `coverage`/`readiness` 미실행. stale raw로 fact 생성하지 않음(의도된 동작) |
| Common build | `common_coverage` 실패 | `readiness` 미실행. coverage는 진단 로깅 단계라 fact 자체에는 영향 없음 |
| Common build | `common_readiness` 실패 | Common build chain의 마지막 단계(품질 gate). `--fail-on-not-ready` 미달 시 실패로 종료, 후속 미실행 없음 |

## 9. Rollback

전환 후 문제가 생기면 아래 순서로 되돌린다. universe/prices/flows의 chain head 구조와 `fdr_common` root는 변경하지 않았으므로 rollback 대상이 아니다.

1. (Phase 2 이후 문제 시) Cronicle daily event script에 `export SDC_DAILY_USE_SOURCE_LOCK=1`을 임시 주입해 기존 lock 보호를 복구한다.
2. `sdc_daily_krx_common.timing`을 기존 평일 21:30으로 복구한다.
3. `sdc_daily_krx_flows.chain`을 빈 값으로 되돌린다(기존 상태: chain target 없음).
4. `sdc_daily_common_build.timing`을 기존 평일 22:30으로 복구한다.

Rollback 중에도 freshness guard는 유지한다.

## 10. 최종 상태 요약

최종 구조는 다음 원칙을 따른다.

- Daily Cronicle은 실제 데이터 의존성을 따르는 chain으로 같은 날짜 chain 내부의 같은 source 동시 실행을 구조적으로 제거한다.
- Daily wrapper는 source lock을 기본 사용하지 않는다(`SDC_DAILY_USE_SOURCE_LOCK=0`).
- Manual/backfill wrapper는 source lock을 유지하되, 평상시 daily–backfill overlap 방지는 daily event disable 운영 절차로 처리한다.
- day N의 장기 KRX catch-up이 day N+1 root와 겹치는 cross-day overlap은 남은 운영 리스크이며, 장기 실행이 확인되면 다음 daily root disable 또는 `SDC_DAILY_USE_SOURCE_LOCK=1` fallback으로 대응한다.
- KRX daily path는 `universe -> prices -> flows -> krx_common` 단일 chain으로 통합한다.
- `fdr_universe`는 KRX chain head로 두어 stale universe 진행을 hard chain dependency로 막는다. 별도 preflight 이벤트나 신규 CLI는 도입하지 않는다.
- Common build는 23:30 scheduled root와 freshness guard로 all-source join을 대체한다.
- 핵심 재구성은 신규 Cronicle 이벤트/collector CLI 없이 기존 16개 이벤트의 chain/timing 변경 + daily wrapper lock 호출 교체로 완성한다. Backfill 자동 가드는 Phase 3의 별도 안전 보강으로 판단한다.
