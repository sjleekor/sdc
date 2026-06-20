# KRX Collection Throttle Timing

작성일: 2026-06-20

## 조사 범위와 한계

기준 소스:

- 운영 wrapper/IaC: `deploy/prod/README.md`, `deploy/prod/bin/*.sh`
- 코드: `sync_krx_flows.py`, `sync_common_features.py`, `backfill_daily.py`, `KrxMdcClient`
- 운영 DB: `sj2`의 `ingestion_runs`
- 운영 로그: `sj2-server:/home/whi/apps/cronicle/logs/jobs/jmqkx0rf608.log`

중요 한계:

- direct KRX의 `HumanThrottle` sleep은 DEBUG 로그로만 남는다. 현재 운영 INFO 로그만으로는
  실제 난수 sleep 합계를 복원할 수 없다.
- `Slow flow request elapsed`는 순수 HTTP 응답 시간이 아니다. provider 호출 전체 시간이며,
  내부 KRX HTTP throttle, long rest, error backoff, retry delay가 섞여 있다.
- `prices backfill`과 `common sync`는 정상 요청별 elapsed를 기록하지 않는다. 따라서 표의
  요청 처리 시간은 run duration에서 코드상 throttle 산정치를 뺀 값이다.

## KRX 관련 수집 경로

| 구분 | 운영 event/wrapper | 실제 provider/source | 요청 단위 | 운영 상태 | 최근 1회 총 소요시간 | KRX 차단 방지 장치 |
|---|---|---|---|---|---:|---|
| Universe | `sdc_daily_fdr_universe` / `universe-sync.sh` | 기본 `FDR`; 실패 시 `PykrxUniverseProvider` fallback | market별 listing + fallback 시 ticker name loop | daily chain 첫 단계 | `0.0002h` (`0.6s`) | wrapper domain은 `fdr`; fallback pykrx에는 별도 per-request throttle 없음 |
| Prices | `sdc_daily_pykrx_prices` / `prices-backfill-incremental.sh` | `PykrxDailyPriceProvider` (`pykrx`) | ticker/date-range 1요청 | daily chain 활성 | 정상 수집 `0.106h` (`6.4m`); no-work `0.0003h` | service sleep `rate_limit=0.1s`, `1000`요청마다 `5s` long rest, wrapper domain `krx_marketdata` |
| Security flows | `sdc_daily_krx_flows` / `flows-sync.sh` | `KrxDirectFlowProvider` (`KRX` MDC) | foreign: trading day x market, investor: ticker, shorting: ticker | daily chain 활성, 현재 slow issue 대상 | 최근 성공 `26.7h`; 현재 slow run `23.1h+` | service logical sleep `8.0s`; KRX HTTP `1.5..4.0s`; 15 HTTP마다 `30..90s`; error backoff `45..180s`; wrapper domain `krx_marketdata` |
| Common features, KRX direct | `sdc_daily_krx_common` / `common-sync-krx.sh` | `KrxCommonFeatureProvider` (`KRX` MDC) | series 단위. market breadth는 내부에서 trading day x market HTTP로 확장 | `flows` 뒤 chain 활성 | `0.167h` (`10.0m`) | service sleep `0.2s`; KRX HTTP `1.5..4.0s`; 15 HTTP마다 `30..90s`; wrapper domain `krx_marketdata` |
| Common features, PYKRX | `common-sync-pykrx.sh` | `PykrxCommonFeatureProvider` (`pykrx`) | series 단위 | wrapper만 있고 최근 운영 run 없음 | 관측 없음 | service sleep `0.2s`; pykrx 호출 자체에는 `HumanThrottle` 없음; wrapper domain `krx_marketdata` |
| Flow backfill | `flows-backfill-range.sh` | `KrxDirectFlowProvider` (`KRX` MDC) | explicit range의 flow group | 수동 보정용 | 최근 investor/shorting catch-up `26.2h` | 항상 `krx_marketdata` source lock 사용 |

host-side source lock/throttle:

| Domain | 대상 wrapper | source lock 조건 | job 시작 간격 |
|---|---|---|---:|
| `krx_marketdata` | `prices-backfill-incremental.sh`, `flows-sync.sh`, `common-sync-krx.sh`, `common-sync-pykrx.sh`, `flows-backfill-range.sh` | daily wrapper는 `SDC_DAILY_USE_SOURCE_LOCK=1`일 때만; `flows-backfill-range.sh`는 항상 | `60s` |

## Run 기준 처리 시간 / throttle 비율

| 수집 | 관측 기준 | 관측 elapsed | 총 소요시간 | 요청 수 기준 | throttle 산정 | throttle 비율 | 요청 처리/응답 비율 | 해석 |
|---|---|---:|---:|---|---:|---:|---:|---|
| PYKRX prices | `sj2` `daily_backfill`, 2026-06-18 18:30, success | `382.0s` | `0.106h` | 약 `2767` pykrx ticker 요청 | `2767*0.1 + floor(2767/1000)*5 = 286.7s` | `75.1%` | `95.3s` / `24.9%` | 가격 수집은 대부분 의도적 sleep이다. 순수 처리/pykrx 응답은 약 25% 수준으로 추정된다. |
| KRX flows 전체 | `sj2` `krx_flow_sync`, 2026-06-15 18:37, success | `95788.2s` | `26.608h` | service requests `5548`; 추정 KRX HTTP 약 `8321` | logical `5548*8 = 44384.0s` + **최소** HTTP throttle `29101.5s` = `73485.5s` | `>=76.7%` | `<=22302.7s` / `<=23.3%` | 최소 sleep만 잡아도 3/4 이상이 차단 방지 대기다. 실제 sleep은 난수라 더 클 수 있다. |
| KRX flows slow run | Cronicle `jmqkx0rf608`, 2026-06-20 20:29 로그 tail | ticker phase `81893.9s` | `22.748h` | attempted `3186`, processed `3173/5536` | logical sleep만 `3186*8 = 25488.0s` | `31.1%` 이상 | slow request elapsed 합 `48891.3s` / `59.7%` | slow request elapsed 안에 응답 지연, non-JSON retry, HTTP throttle, error backoff가 섞여 있다. 순수 응답시간으로 해석하면 안 된다. |
| KRX common direct | `sj2` `common_feature_sync`, 2026-06-18 21:18, success | `601.5s` | `0.167h` | service requests `11`; 추정 KRX HTTP `67` (`3` index + `31*2` breadth + `2` warmup) | 최소 `222.7s`; 평균 설정값 기준 `426.4s` | 최소 `37.0%`; 평균 기준 `70.9%` | 평균 기준 `175.1s` / `29.1%` | service request는 11개지만 실제 KRX HTTP는 breadth 때문에 수십 건이다. 평균 설정값 기준으로도 대부분이 throttle이다. |
| PYKRX common | 최근 `sj2` `common_feature_sync` run 없음 | - | - | - | service sleep `0.2s/request`만 예정 | - | - | wrapper는 있으나 운영 필수 path에는 보이지 않는다. 실행 시 pykrx 내부 HTTP에는 별도 human throttle이 없다. |

### 산정식 메모

| 항목 | 산정식 |
|---|---|
| PYKRX prices throttle | `api_requests * rate_limit + floor(api_requests / long_rest_interval) * long_rest_seconds` |
| KRX flows 최소 throttle | `service_attempts * logical_rate_limit + krx_http_calls * http_min_delay + floor((krx_http_calls - 1) / long_rest_every) * long_rest_min` |
| KRX common 평균 throttle | `service_requests * service_rate + krx_http_calls * avg(http_min,http_max) + floor((krx_http_calls - 1) / long_rest_every) * avg(long_rest_min,long_rest_max)` |
| direct KRX error path | non-JSON 등 KRX error 시 `KrxMdcClient`가 `45..180s` backoff를 수행하고, service `call_with_retry`가 최대 3회 재시도한다. |

## Slow flow job 요청 지연 통계

대상 로그: `sj2-server:/home/whi/apps/cronicle/logs/jobs/jmqkx0rf608.log`

로그 기준:

- command: `flows sync --incremental --lookback-days 14 --max-auto-range-days 30`
- resolved range: `2026-06-05..2026-06-19`
- enabled groups: `foreign_holding`, `investor`, `shorting`
- observed at log tail: ticker phase `3173/5536`, attempted `3186`, errors `77`
- `Slow flow request` threshold: `30s`

| group | slow_count | elapsed_sum_s | avg_s | median_s | p95_s | max_s |
|---|---:|---:|---:|---:|---:|---:|
| `foreign` | 2 | 159.7 | 79.8 | 79.8 | 79.3 | 80.4 |
| `investor` | 121 | 19659.8 | 162.5 | 78.5 | 448.9 | 562.2 |
| `shorting` | 266 | 29071.8 | 109.3 | 67.2 | 375.8 | 586.5 |
| total | 389 | 48891.3 | 125.7 | 70.7 | 424.7 | 586.5 |

non-JSON 응답 집계:

| bld | count | 관련 group |
|---|---:|---|
| `dbms/MDC/STAT/srt/MDCSTAT30001` | 273 | shorting status |
| `dbms/MDC/STAT/standard/MDCSTAT02302` | 266 | investor |
| `dbms/MDC/STAT/srt/MDCSTAT30502` | 2 | shorting balance |

## 결론

| 결론 | 근거 |
|---|---|
| KRX 관련 수집 중 runtime을 지배하는 것은 `KRX flows`다. | 성공 run도 약 `26.6h`; 최소 throttle만 `76.7%` 이상이다. |
| 가격 수집은 많은 요청을 빠르게 돌리지만 sleep 비율이 높고 안정적이다. | 2026-06-18 가격 run은 `382.0s` 중 약 `286.7s`가 의도적 sleep이다. |
| KRX common direct는 service request 수가 작아 보여도 실제 KRX HTTP 수가 훨씬 크다. | `11` series request가 `67` HTTP 수준으로 확장된다. |
| 현재 slow flow job의 slow elapsed는 순수 응답 지연이 아니다. | non-JSON retry, KRX error backoff, HTTP throttle, logical sleep 일부가 함께 섞인다. |
| 정확한 응답시간/throttle 비율을 얻으려면 계측 추가가 필요하다. | 현재 INFO 로그에는 `HumanThrottle` sleep과 정상 request elapsed가 남지 않는다. |

## 후속 계측 권장

| 개선 | 목적 |
|---|---|
| `KrxMdcClient`에서 HTTP call별 `request_elapsed_s`, `sleep_before_s`, `long_rest_s`, `error_backoff_s`를 INFO summary counter로 누적 | direct KRX의 실제 응답시간과 throttle 시간을 분리 |
| `sync_krx_flows`에서 group별 `service_sleep_s`, `provider_elapsed_s`, `retry_delay_s`를 run counts에 기록 | investor/shorting/foreign별 병목 비교 |
| `backfill_daily_prices`에서 `api_requests_count`와 sleep 합계를 `ingestion_runs.counts`에 저장 | PYKRX prices의 요청 수를 bars 수로 추정하지 않도록 개선 |
| `common sync`에서 provider별 실제 HTTP call 수를 기록 | service request와 실제 KRX HTTP 확장 비율을 가시화 |
