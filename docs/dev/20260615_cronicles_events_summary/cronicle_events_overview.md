# Cronicle 이벤트 구성과 실행 순서 정리

- 작성일: 2026-06-15
- 대상 호스트: sj2-server (`192.168.0.11`), Cronicle UI `http://192.168.0.11:3012/#Schedule`
- 범위: SDC(`stock_data_collector`) 운영을 구동하는 Cronicle 16개 이벤트와 각 이벤트가 호출하는 wrapper, 실행 순서, 타이밍, source lock
- 출처(source of truth):
  - 스케줄: Cronicle API `GET /api/app/get_schedule/v1` (2026-06-15 조회)
  - wrapper: `whi@sj2-server:/home/whi/apps/sdc/bin/*.sh`
  - lock/throttle 설계: `docs/dev/20260613_refactoring/source_wrapper_throttling_plan.md`

> 이 문서는 "각 이벤트가 무엇을 하고, 언제, 어떤 순서로 실행되는가"를 운영자가 빠르게 이해하기 위한 지도다. 실제 값은 Cronicle API와 wrapper가 항상 우선한다.

---

## 1. 한눈에 보기

### 1.1 실행 트리거 방식

Cronicle 이벤트는 두 가지 방식으로만 실행된다.

- **timing (스케줄 트리거)**: 지정 시각에 자동 실행되는 "체인의 시작점(root)".
- **chain (선행 성공 시 연쇄)**: 자체 timing 없이(`timing=false`), 앞 이벤트가 **성공(exit 0)** 했을 때만 Cronicle이 다음으로 이어 실행. 앞 이벤트가 실패하면 체인은 거기서 멈춘다(`chain_error`가 비어 있으므로 실패 분기 없음).

즉 **timing이 있는 이벤트 = 체인의 시작**, **timing=false 이벤트 = 누군가의 chain 대상**이다.

### 1.2 4개의 독립 체인 / 그룹

| 그룹 | 시작 트리거 | 흐름 | source lock domain |
|---|---|---|---|
| **A. KRX 가격·수급 체인** | `fdr_universe` 18:30 (Mon–Fri) | universe → prices → flows | `fdr` → `krx_marketdata` → `krx_marketdata` |
| **B. Common Feature 그룹** | 여러 이벤트가 20:30/21:30/22:30 개별 timing | (소스별 sync 병렬) → build → coverage → readiness | `fdr`/`fred`/`ecos`/`krx_marketdata` → (build 이후는 lock 없음) |
| **C. OpenDART 체인** | `opendart_corp` 04:00 (매일) | corp → financials → share-info → xbrl → metrics-normalize | `opendart` 공유 (normalize는 lock 없음) |
| **D. KRX Common (그룹 B의 일부지만 lock은 A와 공유)** | `krx_common` 21:30 | krx common sync 단독 | `krx_marketdata` |

핵심: **그룹 A와 그룹 D(krx_common)는 같은 `krx_marketdata` lock을 공유**한다. 둘은 별도 체인/타이밍이지만 동시에 KRX를 치치 못하도록 host-side flock으로 직렬화된다. (2026-06-15 장애의 근원 — 8장 참조)

### 1.3 전체 이벤트 표 (2026-06-15 기준)

| 이벤트 id | 제목 | timing | chain → 다음 | lock domain | wrapper |
|---|---|---|---|---|---|
| `sdc_daily_fdr_universe` | FDR Universe | **Mon–Fri 18:30** | → `pykrx_prices` | `fdr` | `universe-sync.sh` |
| `sdc_daily_pykrx_prices` | PYKRX Prices | chain-only | → `krx_flows` | `krx_marketdata` | `prices-backfill-incremental.sh` |
| `sdc_daily_krx_flows` | KRX Flows | chain-only | (끝) | `krx_marketdata` | `flows-sync.sh` |
| `sdc_daily_fdr_common` | FDR Common | **Mon–Fri 20:30** | (끝) | `fdr` | `common-sync-fdr.sh` |
| `sdc_daily_fred_common` | FRED Common | **Mon–Fri 20:30** | (끝) | `fred` | `common-sync-fred.sh` |
| `sdc_daily_ecos_common_daily` | ECOS Daily Common | **Mon–Fri 20:30** | → `ecos_common_macro` | `ecos` | `common-sync-ecos-daily.sh` |
| `sdc_daily_ecos_common_macro` | ECOS Macro Common | chain-only | (끝) | `ecos` | `common-sync-ecos-macro.sh` |
| `sdc_daily_krx_common` | KRX Common | **Mon–Fri 21:30** | (끝) | `krx_marketdata` | `common-sync-krx.sh` |
| `sdc_daily_common_build` | Common Build Daily | **Mon–Fri 22:30** | → `common_coverage` | (없음) | `common-build-daily.sh` |
| `sdc_daily_common_coverage` | Common Coverage Report | chain-only | → `common_readiness` | (없음) | `common-coverage-report.sh` |
| `sdc_daily_common_readiness` | Common Readiness Check | chain-only | (끝) | (없음) | `common-readiness-check.sh` |
| `sdc_daily_opendart_corp` | OpenDART Corp Sync | **매일 04:00** | → `opendart_financials` | `opendart` | `dart-sync-corp.sh` |
| `sdc_daily_opendart_financials` | OpenDART Financials | chain-only | → `opendart_share_info` | `opendart` | `dart-sync-financials.sh` |
| `sdc_daily_opendart_share_info` | OpenDART Share Info | chain-only | → `opendart_xbrl` | `opendart` | `dart-sync-share-info.sh` |
| `sdc_daily_opendart_xbrl` | OpenDART XBRL | chain-only | → `metrics_normalize` | `opendart` | `dart-sync-xbrl.sh` |
| `sdc_daily_metrics_normalize` | Metrics Normalize | chain-only | (끝) | (없음) | `metrics-normalize.sh` |

- 16개 이벤트 전부 `enabled=1`, `max_children=1`, `multiplex=0`, `timezone=Asia/Seoul`, `plugin=shellplug`.
- `pykrx_common`(optional)은 wrapper(`common-sync-pykrx.sh`)는 있으나 Cronicle 이벤트로 **등록되지 않음**(설계상 기본 비활성).

---

## 2. 시간대별 타임라인 (평일 기준, KST)

```
04:00  ┌─ [C] OpenDART corp ─────────────────────────────────────────────┐
       │   → financials → share-info → xbrl → metrics-normalize           │
       │   (opendart lock 공유, 순차. financials는 ~15분+ 소요)            │
       └──────────────────────────────────────────────────────────────────┘

18:30  ┌─ [A] FDR universe ──────────────────────────────────────────────┐
       │   → pykrx prices → krx flows                                      │
       │   (universe=fdr lock, prices/flows=krx_marketdata lock 공유, 순차)│
       └──────────────────────────────────────────────────────────────────┘

20:30  ┌─ [B] FDR common ┐ ┌─ FRED common ┐ ┌─ ECOS daily → ECOS macro ┐
       │   (fdr lock)    │ │ (fred lock)  │ │ (ecos lock)               │
       │   3개 그룹은 서로 다른 lock이라 동시 실행 가능                  │
       └─────────────────┘ └──────────────┘ └───────────────────────────┘

21:30  ┌─ [D] KRX common (krx_marketdata lock — 그룹 A와 lock 공유!) ─────┐
       │   18:30 flows가 아직 돌고 있으면 lock 대기(최대 1800s) 후 실행   │
       └──────────────────────────────────────────────────────────────────┘

22:30  ┌─ [B] Common build ──────────────────────────────────────────────┐
       │   freshness guard 통과 시 build → coverage → readiness           │
       │   (외부 API lock 없음. 앞선 source sync들이 신선해야 통과)       │
       └──────────────────────────────────────────────────────────────────┘
```

설계 의도(`source_wrapper_throttling_plan.md` 6.3):

- **18:30 KRX 체인 먼저** — 최신 universe 확보 후 가격, 가격 최신일을 기준으로 flows range 산정.
- **20:30 common source sync** — FDR/FRED/ECOS는 서로 다른 외부 source라 lock이 달라 병렬 가능.
- **21:30 krx_common** — KRX 계열이므로 18:30 가격·수급과 시간차를 두고, 그래도 겹치면 lock으로 직렬화.
- **22:30 build** — 모든 source sync가 끝났을 것으로 보고 DB 내부 산출물 생성. 단 stale raw를 막기 위해 build 전에 freshness guard(`ops assert-common-freshness`)를 실행.

---

## 3. 그룹 A — KRX 가격·수급 체인

**트리거**: `fdr_universe` 가 평일 18:30에 시작 → 성공 시 `pykrx_prices` → 성공 시 `krx_flows`.

| 단계 | 이벤트 | wrapper | 실제 CLI 호출 | lock |
|---|---|---|---|---|
| 1 | `fdr_universe` | `universe-sync.sh` | `universe sync --source fdr --markets kospi,kosdaq` | `fdr` |
| 2 | `pykrx_prices` | `prices-backfill-incremental.sh` | `prices backfill --market all --incremental --lookback-days 0 --max-auto-range-days 10` | `krx_marketdata` |
| 3 | `krx_flows` | `flows-sync.sh` | `flows sync --incremental --lookback-days 14 --max-auto-range-days 30` | `krx_marketdata` |

설명:

- **universe**: KOSPI/KOSDAQ 상장 종목 마스터를 FinanceDataReader로 동기화. lock domain은 `fdr`이라 가격/수급과 다름(같은 체인이지만 lock은 별개).
- **prices**: 일봉 OHLCV 증분 수집. provider가 PyKRX라 KRX를 치므로 lock domain은 `krx_marketdata`. `--lookback-days 0`은 "각 종목 `MAX(trade_date)` 이후만" 수집(목표 1의 DB 최신 기준 증분).
- **flows**: 투자자별 순매수·공매도 등 수급 raw 수집. `--incremental`은 가격 최신일을 end로 사용 → **prices가 먼저 성공해야 flows range가 안정**된다. 그래서 chain 순서가 prices → flows다.
- **느려질 수 있는 단계**: flows는 종목 × (investor/shorting)로 수천 요청을 돌고 KRX 응답이 건당 수십 초까지 느려질 수 있어, 밀린 날을 catch-up할 때 수 시간이 걸린다. 이때 `krx_marketdata` lock을 오래 쥐어 21:30 `krx_common`과 충돌한다(8장).

**환경변수 오버라이드** (이벤트 script 또는 `.env`):
`PRICE_LOOKBACK_DAYS`, `PRICE_MAX_AUTO_RANGE_DAYS`, `FLOW_LOOKBACK_DAYS`(기본 14), `FLOW_MAX_AUTO_RANGE_DAYS`(기본 30), `FLOW_EXCLUDE_GROUPS`, `FLOW_ALLOW_LARGE_RANGE`.

---

## 4. 그룹 B — Common Feature

시장/매크로 공통 피처. **소스별 sync 이벤트는 각자 다른 timing**으로 시작하고(체인 root가 여럿), build/coverage/readiness는 별도 체인이다.

### 4.1 소스별 sync (20:30 ~ 21:30)

| 이벤트 | timing | wrapper | CLI | lock |
|---|---|---|---|---|
| `fdr_common` | Mon–Fri 20:30 | `common-sync-fdr.sh` | `common sync --sources fdr --incremental --lookback-days 45 --max-auto-range-days 90` | `fdr` |
| `fred_common` | Mon–Fri 20:30 | `common-sync-fred.sh` | `common sync --sources fred ...` | `fred` |
| `ecos_common_daily` | Mon–Fri 20:30 | `common-sync-ecos-daily.sh` | `common sync --sources ecos --incremental --lookback-days 45 ...` | `ecos` |
| `ecos_common_macro` | chain-only (daily 성공 후) | `common-sync-ecos-macro.sh` | `common sync --sources ecos --series macro_cpi,macro_ppi,macro_m2,macro_consumer_sentiment --lookback-days 540 --max-auto-range-days 730` | `ecos` |
| `krx_common` | Mon–Fri 21:30 | `common-sync-krx.sh` | `common sync --sources krx --incremental --lookback-days 45 --max-auto-range-days 90` | **`krx_marketdata`** |

- FDR/FRED/ECOS는 source lock이 모두 달라 20:30에 동시에 떠도 충돌하지 않는다.
- ECOS는 **daily → macro 2패스**로 분리. daily는 일간 시계열(짧은 lookback 45일), macro는 월간 매크로 시계열(긴 lookback 540일). daily 성공 후 macro가 chain으로 이어진다.
- **krx_common만 그룹 A와 lock(`krx_marketdata`)을 공유**한다. 21:30으로 가격·수급(18:30)과 시간차를 뒀고, 그래도 겹치면 lock 대기.

### 4.2 build → coverage → readiness (22:30 체인)

| 이벤트 | timing | wrapper | 하는 일 | lock |
|---|---|---|---|---|
| `common_build` | Mon–Fri 22:30 | `common-build-daily.sh` | ① `ops assert-common-freshness` freshness guard 통과 시 ② `common build-daily --incremental` 로 daily fact 생성 | 없음 |
| `common_coverage` | chain-only | `common-coverage-report.sh` | `common coverage-report` (진단 — null/missing/PIT 상태 로깅) | 없음 |
| `common_readiness` | chain-only | `common-readiness-check.sh` | `common readiness-report --fail-on-not-ready` (최종 품질 gate, 미달 시 실패) | 없음 |

- build/coverage/readiness는 외부 API를 치지 않으므로 source lock 없음.
- **freshness guard**: `common-build-daily.sh`는 build 전에 `ops assert-common-freshness --sources fdr,fred,ecos,krx`를 호출. 필수 source의 마지막 성공 run age(기본 ≤30h)와 최신 관측일 lag(daily ≤2일, macro ≤45일)를 검사해 **미달이면 stale raw로 build하지 않고 즉시 실패**한다.
- 따라서 22:30은 "all-success join"을 Cronicle로 직접 표현하는 대신, **예상 종료 이후 시각에 독립 timing + freshness guard**로 안전하게 처리하는 설계다.
- 필수 source 기본값 `SDC_COMMON_REQUIRED_SOURCES=fdr,fred,ecos,krx` — `pykrx`는 제외(아직 운영 필수 아님).

---

## 5. 그룹 C — OpenDART 체인

**트리거**: `opendart_corp` 가 **매일(주말 포함) 04:00**에 시작 → 성공 시 순차 chain.

| 단계 | 이벤트 | wrapper | CLI | lock |
|---|---|---|---|---|
| 1 | `opendart_corp` | `dart-sync-corp.sh` | `dart sync-corp` | `opendart` |
| 2 | `opendart_financials` | `dart-sync-financials.sh` | `dart sync-financials --incremental --lookback-years 1 --max-attempt-targets 10000 --negative-cache-ttl-days 3` | `opendart` |
| 3 | `opendart_share_info` | `dart-sync-share-info.sh` | `dart sync-share-info --incremental --lookback-years 1 --max-attempt-targets 10000 --negative-cache-ttl-days 3` | `opendart` |
| 4 | `opendart_xbrl` | `dart-sync-xbrl.sh` | `dart sync-xbrl --incremental --lookback-years 1 --max-attempt-targets 10000 --negative-cache-ttl-days 3` | `opendart` |
| 5 | `metrics_normalize` | `metrics-normalize.sh` | `metrics normalize --incremental --lookback-years 2` | **없음** |

설명:

- 1~4단계는 OpenDART API quota를 공유하므로 **모두 같은 `opendart` lock**으로 직렬화. corp(법인코드) → financials(재무제표) → share-info(주식수/배당/자기주식) → xbrl(원문 문서) 순.
- **`--max-attempt-targets`**: 증분 추정 요청 수가 이 가드를 넘으면 부분 처리 없이 실패(exit 1)해 quota 폭주를 막는다. (2026-06-15 share-info가 이 가드에 걸림 — 8장)
- 5단계 `metrics_normalize`는 OpenDART를 치지 않고 raw → `stock_metric_fact` 정규화만 하므로 lock 없음. raw sync 결과를 캐노니컬 메트릭에 반영.
- corp만 timing(04:00)이 있고 financials~normalize는 chain-only. **앞 단계가 실패하면 거기서 체인이 끊겨 뒤 단계는 미실행**된다.

**환경변수**: `DART_LOOKBACK_YEARS`(기본 1), `DART_{FINANCIAL,SHARE_INFO,XBRL}_MAX_ATTEMPT_TARGETS`, `DART_NEGATIVE_CACHE_TTL_DAYS`(기본 3), `SDC_METRICS_NORMALIZE_LOOKBACK_YEARS`(기본 2).

---

## 6. Source Lock / Throttle 동작

모든 daily wrapper는 `lib/sdc-wrapper.sh`를 source한다. 핵심 함수는 `sdc_run_collector_with_lock <domain> <cli...>`.

- **lock**: host-side `flock`을 `/tmp/sdc-locks/<domain>.lock`에 건다. 같은 domain은 동시에 한 프로세스만 CLI를 실행.
- **lock 대기**: `SDC_LOCK_WAIT_SECONDS`(daily 기본 900s) 동안 non-fatal 대기. 시간 내 못 얻으면:
  - `SDC_LOCK_CONFLICT_MODE=fail`(기본) → **exit 75**로 실패.
  - `SDC_LOCK_CONFLICT_MODE=skip` → 로그 남기고 exit 0(수동/진단용).
- **throttle**: source별 최소 실행 간격(`/tmp/sdc-throttle/<domain>.last` 마커). lock 획득 직후·CLI 호출 직전에 sleep + 마커 갱신. 기본 간격: `krx_marketdata` 60s, `opendart` 5s, `fdr`/`fred`/`ecos` 10s.

lock domain 매핑:

| domain | 이 lock을 쓰는 wrapper |
|---|---|
| `fdr` | `universe-sync.sh`, `common-sync-fdr.sh` |
| `krx_marketdata` | `prices-backfill-incremental.sh`, `flows-sync.sh`, `common-sync-krx.sh`, `common-sync-pykrx.sh`(미등록) |
| `fred` | `common-sync-fred.sh` |
| `ecos` | `common-sync-ecos-daily.sh`, `common-sync-ecos-macro.sh` |
| `opendart` | `dart-sync-corp/financials/share-info/xbrl.sh` |
| (없음) | `common-build/coverage/readiness.sh`, `metrics-normalize.sh`, `common-seed-catalog.sh` |

**왜 lock과 chain을 둘 다 쓰나**: chain은 같은 그룹의 *정상 순서*를 보장하고, lock은 *다른 그룹·수동 trigger·backfill이 겹쳐도* 같은 source를 동시에 못 치게 막는 2차 방어선이다. 예: 18:30 flows(그룹 A)와 21:30 krx_common(그룹 D)은 체인이 다르지만 같은 `krx_marketdata` lock으로 직렬화된다.

---

## 7. 2026-06-15 적용된 운영 오버라이드

`source_wrapper_throttling_plan.md` 배포 직후 발생한 장애 대응으로, **wrapper 파일은 두고 Cronicle 이벤트 script에 env export만 주입**했다.

| 이벤트 | 주입한 export | 목적 |
|---|---|---|
| `sdc_daily_krx_common` | `SDC_LOCK_WAIT_SECONDS=1800` | KRX overlap 시 exit-75 대신 최대 30분 큐 대기 |
| `sdc_daily_pykrx_prices` | `SDC_LOCK_WAIT_SECONDS=1800` | 〃 |
| `sdc_daily_krx_flows` | `SDC_LOCK_WAIT_SECONDS=1800` | 〃 |
| `sdc_daily_opendart_share_info` | `DART_SHARE_INFO_MAX_ATTEMPT_TARGETS=35000` | 분기/반기 미수집 백로그(추정 31,914건) 1회 통과용. **소진 후 원복 권장** |

> 이 오버라이드는 Cronicle 이벤트 script에만 있고 git 저장소의 wrapper에는 없다. wrapper를 재배포해도 사라지지 않지만, 이벤트를 재생성하면 사라진다.

---

## 8. 2026-06-15 장애 사례 (이 구조에서 무엇이 깨졌나)

이해를 돕기 위한 실제 사례.

1. **`krx_common` exit 75** — 18:30 체인의 `flows` catch-up(6/11부터 밀린 수급)이 KRX 응답 지연으로 `krx_marketdata` lock을 수 시간 점유. 21:30 `krx_common`이 기본 900s 대기를 넘겨 lock 충돌 exit 75. → lock 직렬화는 의도대로 동작했고, flows가 너무 오래 쥔 게 원인. 대응: KRX daily wrapper lock-wait를 1800s로 상향(7장).
2. **`opendart_share_info` exit 1** — `estimated requests exceed guard (31914 > 10000)`. 버그가 아니라 실제 백로그. share-info에서 체인이 끊겨 xbrl·normalize까지 미실행. 대응: 가드 35,000으로 한시 상향(7장).

교훈:

- **chain은 실패 전파가 단방향**이다. 한 단계가 실패(또는 가드 차단)하면 뒤 단계 전부 멈춘다. OpenDART·common 체인을 볼 때 "왜 normalize가 안 돌았나?" → 앞 단계 실패부터 본다.
- **같은 lock을 쓰는 다른 그룹은 시간상 겹치면 직렬화로 대기/실패**한다. KRX 계열(가격·수급·krx_common)이 대표적. catch-up 등 장기 작업이 있으면 같은 domain의 daily 이벤트가 밀린다.

---

## 9. 빠른 점검 명령 (read-only)

```bash
# 스케줄 전체 (sj2-server skill의 APIKEY 사용)
APIKEY=$(awk -F': *' '/^APIKEY:/ {print $2}' /Users/whishaw/wss_p/stock_data_collector_secrets/cronicle_info)
curl -fsS -H "X-API-Key: $APIKEY" 'http://192.168.0.11:3012/api/app/get_schedule/v1' | python3 -m json.tool

# 특정 이벤트 최근 실행 이력 (code != 0 = 실패)
curl -fsS -H "X-API-Key: $APIKEY" 'http://192.168.0.11:3012/api/app/get_event_history/v1?id=sdc_daily_krx_common&limit=5'

# 실행 중인 collector 컨테이너 / lock 현황
ssh whi@sj2-server 'docker ps --format "{{.Names}} {{.Status}} {{.Command}}" | grep collector; ls -la /tmp/sdc-locks/'
```

---

## 10. 참고

- 설계 근거 / lock 정책 상세: `docs/dev/20260613_refactoring/source_wrapper_throttling_plan.md`
- 운영 런북 / 부분 실패 복구: `docs/operations.md`
- sj2-server 접근·안전 규칙: `.claude/skills/sj2-server/`
- CLI 명령 트리: `CLAUDE.md` "CLI command tree"
