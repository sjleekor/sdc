# KRX Flows Slow Job Issue Analysis

작성일: 2026-06-20

## 배경

- 대상 Cronicle job: `jmqkx0rf608`
- 대상 event: `sdc_daily_krx_flows` (`SDC KRX Flows`)
- 서버: `sj2-server` (`192.168.0.11`)
- 배포 collector image: `ghcr.io/sjleekor/sdc:v0.8.12`
- 실행 command: `flows sync --incremental --lookback-days 14 --max-auto-range-days 30`
- job 시작 시각: `2026-06-19 21:38:57 KST`

조사는 Cronicle job 파일/로그, 배포 wrapper, Docker process, 프로덕션 DB(`sj2` / `krx_data`)를 기준으로 수행했다. Cronicle API/UI 포트는 로컬 실행 환경에서 직접 접근이 되지 않아, 서버 내부 파일과 SSH read-only 명령을 주로 사용했다.

## 현재 확인된 현상

`jmqkx0rf608`는 멈춘 상태가 아니라 매우 느리게 진행 중이었다.

2026-06-20 19:20 KST 확인 시점의 job 로그 기준:

- phase: `ticker_metrics`
- progress: `2932/5536`
- attempted: `2945`
- skipped: `9`
- rows_upserted: `100214`
- no_data: `51`
- errors: `77`
- elapsed: 약 `77771.7s` (`21h 36m` 수준)

로그 파일에는 다음 패턴이 반복된다.

- `Slow flow request`가 다수 발생
- 특정 요청이 40~90초 이상 걸리는 경우가 반복
- 일부 요청은 최대 수백 초까지 지연
- KRX가 JSON 대신 HTML 에러 페이지를 반환
- 실패 요청은 재시도와 backoff를 거치며 전체 진행을 더 늦춤

집계상 `Slow flow request`는 364회 확인되었고, 평균 지연은 약 129초, 최대 지연은 586초였다.

## 배포된 수집 스케줄

확인된 Cronicle chain은 다음과 같다.

```text
18:30 SDC FDR Universe
  -> SDC PYKRX Prices
  -> SDC KRX Flows
  -> SDC KRX Common Features
```

관련 event들은 `max_children=1`, `queue=0`으로 설정되어 있다. 따라서 `sdc_daily_krx_flows`가 하루 이상 실행되면 다음 일일 chain 실행은 대기하지 못하고 실패하거나 launch가 거부될 수 있다.

실제로 Cronicle 기록에서 `2026-06-18 18:36:30 KST`에 다음 경고가 확인되었다.

```text
Failed to launch chain reaction: SDC KRX Flows:
Maximum of 1 job already running for event: SDC KRX Flows
```

즉, 단일 job 지연이 다음 일일 수집 스케줄에도 영향을 주는 구조다.

## DB 커버리지 상태

프로덕션 DB 기준 `daily_ohlcv`는 최신 가격일이 `2026-06-19`까지 들어와 있었다.

최근 flow 데이터는 부분 커버리지 상태였다.

`2026-06-05` ~ `2026-06-19` 범위 기준:

- `investor` 그룹 complete ticker: `1351`
- `shorting` 그룹 complete ticker: `0`
- `short_selling_balance_quantity`는 최신 `2026-06-17`까지만 존재
- `2026-06-18`, `2026-06-19`의 shorting balance가 없어 shorting 그룹 전체가 complete 조건을 만족하지 못함

최근 날짜별 대략적인 row/ticker 수:

- `2026-06-19`
  - `foreign_holding_shares`: 2768 tickers
  - investor 계열: 약 1356 tickers
  - shorting value/volume: 약 1430 tickers
  - `short_selling_balance_quantity`: 없음
- `2026-06-18`
  - `foreign_holding_shares`: 2767 tickers
  - investor 계열: 약 1357 tickers
  - shorting value/volume: 약 1429 tickers
  - `short_selling_balance_quantity`: 없음
- `2026-06-17`
  - `foreign_holding_shares`: 2768 tickers
  - investor 계열: 약 2651 tickers
  - shorting value/volume: 2768 tickers
  - `short_selling_balance_quantity`: 1429 tickers

## 코드상 지연을 키우는 구조

`src/krx_collector/service/sync_krx_flows.py`의 기존 coverage skip 기준은 다음과 같다.

- `foreign_holding`
  - trading day × market 단위로 market 내 전체 ticker 수가 있으면 skip
- `investor`
  - ticker별로 `trading_days * INVESTOR_METRICS` 개수를 모두 만족해야 skip
- `shorting`
  - ticker별로 `trading_days * SHORTING_METRICS` 개수를 모두 만족해야 skip

현재 실행 범위는 `2026-06-05..2026-06-19`이고 거래일은 11일이다. 따라서 `shorting`은 ticker별로 11일 × 3개 metric을 모두 가져야 skip된다.

그런데 `short_selling_balance_quantity` 최신일이 `2026-06-17`이고 18/19일이 비어 있어, `shorting` complete ticker가 0개가 된다. 결과적으로 거의 전 종목에 대해 `shorting` 요청을 다시 수행한다.

또한 `investor`도 최신 날짜의 일부 결손 때문에 완전한 ticker가 제한적이다. 따라서 incremental 실행이 사실상 대규모 재수집처럼 동작한다.

## 비용 모델 (런타임을 지배하는 것은 요청 건수다)

> **이 절은 우선순위 판단의 전제다. 처음 초안의 "lookback 축소 / missing-only가 일일 런타임을
> 줄인다"는 가정이 이 작업의 구조와 맞지 않아 정정한다.**

`ticker_metrics` 단계의 fetch는 **종목당 1요청으로 전체 날짜 범위를 한 번에** 가져온다.

- investor: `fetch_investor_net_volume(ticker, market, start, end)` — `strtDd..endDd` 범위 1요청
- shorting: `fetch_shorting_metrics(ticker, market, start, end)` — 같은 범위에 대해
  status(volume+value) 1 POST + balance 1 POST = 종목당 2 POST
- 즉 **요청 건수 = 활성 종목 수 × 그룹 수**이고, 날짜 폭(lookback)과는 무관하다.

진행률 `2932/5536`이 이를 뒷받침한다: 약 2768 종목 × (investor + shorting) 2그룹 ≈ 5536건.

결정적인 점: **매일 새로 생긴 거래일은 어떤 종목도 아직 가지고 있지 않으므로, 정상 상태에서도
거의 전 종목이 매일 그룹 incomplete → 매일 재수집된다.** 따라서

- `lookback-days`를 14 → 2/3으로 줄여도, missing-only로 날짜만 좁혀도
  **일일 요청 건수는 그대로 ~5536건**이다. 이 둘은 요청당 payload(날짜 폭)와
  catch-up 시 재호출 폭만 줄일 뿐, **정상 일일 델타의 요청 건수는 줄이지 못한다.**

요청 건수에 throttle를 곱하면 런타임이 나온다 (logical rate limit `8.0s` 기준 하한):

- investor 단독 ≈ 2768건 × 8s ≈ **6~7시간**
- shorting 단독 ≈ 2768건 (× 2 POST + balance) ≈ **그 이상**
- foreign_holding은 `trading_days × markets` 단위라 일일 수십 건 — **사실상 무시 가능**

문서가 인용한 "정상 실행도 약 26시간"은 이 구조의 직접적 결과다. **KRX flows 전체 universe
일일 배치는 본질적으로 multi-hour 작업이며, skip 최적화만으로는 일일 런타임을 줄일 수 없다.**
런타임을 실제로 줄이는 레버는 셋뿐이다.

1. **요청 건수 자체를 빼기** — investor/shorting을 daily critical chain에서 분리
2. **lag-aware 완전성** — balance 지연 tail을 complete로 인정해 종목을 skip → catch-up/재실행
   요청을 줄임
3. **throttle 크기 / 동시성** — `8.0s` logical rate limit 자체. 단, 아래 차단 trade-off 주의

### throttle ↔ 차단 trade-off

KRX의 HTML(non-JSON) 응답은 사실상 소프트 차단 신호로 보인다. 따라서 `8.0s` rate limit을
낮추거나 동시화하면 차단/오류 위험이 올라간다. 결국 현실적 결론은 **"문제 그룹을 분리하고 충분한
실행 시간을 확보"**하는 쪽이며, throttle 인하는 차단 모니터링과 함께 신중히 다뤄야 한다.

## KRX 응답 오류 패턴

오류 로그의 핵심 형태:

```text
KRX returned non-JSON for bld=dbms/MDC/STAT/srt/MDCSTAT30001: <!DOCTYPE HTML>
```

또는:

```text
KRX returned non-JSON for bld=dbms/MDC/STAT/srt/MDCSTAT30502: <!DOCTYPE HTML>
```

이는 KRX endpoint가 기대한 JSON 대신 HTML 페이지를 반환하는 상황이다. HTTP status 자체보다 response body가 JSON이 아닌 것이 문제로 드러난다.

현재 설정상 flow sync는 다음 throttle/backoff 정책을 사용한다 (`src/krx_collector/infra/config/settings.py` 기본값 기준).

- logical rate limit: `8.0s` (`krx_logical_rate_limit_seconds`)
- HTTP delay: `1.5..4.0s` (`krx_min_delay_seconds` / `krx_max_delay_seconds`)
- long rest: 15회마다 `30..90s`
- error backoff: `45..180s` (`krx_error_backoff_min_seconds` / `..max..`)
- request retry: 최대 `3회` (`call_with_retry`, `max_attempts=3`)

> **정정 (timeout)**: 코드 기본 `krx_mdc_timeout_seconds`는 `20.0s`이고
> (`settings.py`의 `DEFAULT_KRX_MDC_TIMEOUT_SECONDS = 20.0`), `flows-sync.sh`는
> `--timeout-seconds`를 넘기지 않는다. 따라서 본문에서 처음 적었던 `timeout=150s`는
> prod compose가 `KRX_MDC_TIMEOUT_SECONDS=150` 같은 env override를 줄 때만 성립한다.
> 다만 관측된 "최대 586초 slow request"는 `call_with_retry`의 전체(재시도+backoff 포함)
> elapsed이므로 timeout 단일 값과는 별개다. **prod env override 여부를 확인해 이 값을
> 확정**해야 한다 — circuit breaker / 재시도 축소 권장의 근거가 되는 수치다.

따라서 KRX non-JSON 응답이 특정 종목에서 반복되면, 같은 요청에 대해 재시도(최대 3회)와
error backoff(`45..180s`)가 누적되어 종목당 수백 초까지 소비될 수 있다. 관측된 errors 77건이
대부분 이 경로다.

## 원인 요약

1. `flows sync` 작업량이 일일 스케줄 주기보다 크다 — **구조적**이다.
   - ticker_metrics는 종목당 1요청(≈ 종목 수 × 그룹 수)이고, `8.0s` logical rate limit과
     곱하면 investor/shorting만으로도 multi-hour가 된다(위 "비용 모델" 참고).
   - 최근 정상 실행도 약 26시간 이상 걸린 기록이 있다.
   - `lookback-days=14`는 payload만 키울 뿐 요청 건수를 키우지 않는다 — 런타임의 주원인이
     아니다. 분리 없이 lookback만 줄여도 일일 런타임은 거의 그대로다.

2. `shorting` 그룹의 최신 balance 데이터 결손이 skip을 거의 무력화한다.
   - `short_selling_balance_quantity`가 2026-06-17까지만 있어 18/19일 complete 조건을 충족하지 못한다.
   - 그 결과 shorting 전체가 전 종목 재호출된다.

3. KRX가 일부 요청에 HTML 에러 페이지를 반환한다.
   - 코드에서는 이를 `KrxMdcResponseError`로 처리한다.
   - 3회 재시도 + error backoff가 누적된다.

4. Cronicle event 설정이 긴 작업에 취약하다.
   - `max_children=1`, `queue=0`
   - 이전 job이 살아 있으면 다음 chain reaction이 대기하지 않고 거부될 수 있다.

5. `foreign_holding`, `investor`, `shorting`이 하나의 일일 flow job에 묶여 있다.
   - 안정적인 그룹과 불안정한 그룹이 서로 영향을 준다.
   - 특히 `shorting` 지연이 전체 KRX common chain을 막는다.

## 해결 방안 후보

### 1. 즉시 운영 조치: 현재 job 중단 후 shorting 제외 재실행

일일 파이프라인 복구가 우선이면 현재 `sdc_daily_krx_flows` job을 abort하고, `shorting`을 제외해 재실행하는 방안이 있다.

예상 실행 형태 (이 env/플래그는 **이미 구현되어 있다** — `flows-sync.sh`가
`FLOW_EXCLUDE_GROUPS`를 `--exclude-groups`로 전달, `app.py`/`sync_krx_flows.py`가 지원):

```bash
FLOW_EXCLUDE_GROUPS=shorting /home/whi/apps/sdc/bin/flows-sync.sh
```

장점:

- 가장 빠르게 daily chain을 회복할 수 있다.
- 현재 지연의 핵심인 shorting API와 balance 결손을 우회한다.
- foreign/investor 데이터는 계속 최신화할 수 있다.

주의:

- shorting만 제외해도 **investor가 남아 있으면 ~6~7시간**이 든다(위 비용 모델 참고).
  daily chain을 빠르게 회복하는 게 목적이라면 `FLOW_EXCLUDE_GROUPS=shorting,investor`로
  foreign만 남기는 편이 즉시 효과가 크다. investor는 별도 분리 job으로 돌린다.

주의점:

- job abort는 운영 mutation이므로 명시적 승인 후 수행해야 한다.
- 이미 upsert된 부분 데이터는 DB에 남는다.
- `ingestion_runs`의 running 상태가 남을 수 있어 cleanup이 필요할 수 있다.
- shorting 데이터는 별도 보정 job으로 회수해야 한다.

### 2. 단기 스케줄 개선: flow 그룹 분리

현재 `flows sync`를 하나로 실행하지 말고 그룹별 event로 분리한다.

> **정정**: 처음 초안은 `investor`를 daily critical path에 남겨 두었으나, investor 단독이
> ~6~7시간이라 그렇게 두면 `KRX Common Features`가 매일 그만큼 지연된다. **foreign_holding만
> daily critical path에 남기고 investor·shorting 둘 다 분리**하는 편이 비용 모델과 일관된다.

제안 (수정안):

```text
SDC FDR Universe
  -> SDC PYKRX Prices
  -> SDC KRX Foreign Holding      (일일 수십 건, 빠름)
  -> SDC KRX Common Features      (foreign 완료 직후 빠르게 실행)

SDC KRX Investor Flows            (별도 multi-hour job)
SDC KRX Shorting Flows            (별도 multi-hour job, balance 지연 보정 포함)
  - 둘 다 daily critical chain 밖
  - 실패해도 daily common chain을 막지 않게 구성
```

단, common features가 investor/shorting 입력을 실제로 요구하는지(아래 "추가 확인 필요"의
flow group 분류)에 따라 위 체인이 달라질 수 있다. common이 foreign만으로 충분하지 않다면,
"오늘자 데이터 없이도 readiness가 통과되는가"를 먼저 확정해야 한다.

장점:

- shorting/investor 지연이 가격/foreign/common 갱신을 막지 않는다.
- 문제 그룹만 별도로 재시도하거나 보정할 수 있다.
- Cronicle에서 실패 영향 범위를 줄일 수 있다.

필요 작업:

- **별도 wrapper 추가는 불필요**할 수 있다 — 기존 `flows-sync.sh`에 `FLOW_EXCLUDE_GROUPS`로
  서브셋을 지정하면 그룹별 실행이 가능하다. 예: investor-only는
  `FLOW_EXCLUDE_GROUPS=foreign_holding,shorting`.
- 따라서 주 작업은 **Cronicle event/chain 재구성**이며, wrapper는 env만 다르게 둔
  이벤트 3개(또는 래퍼 1개 + 이벤트별 env)로 처리한다.

### 3. 단기 설정 개선: 일일 lookback 축소 (효과 한정)

현재 daily flow는 `FLOW_LOOKBACK_DAYS=14`로 실행된다.

> **효과 정정**: lookback은 요청당 날짜 폭(payload)과 catch-up 시 재호출 범위만 줄인다.
> ticker_metrics는 종목당 1요청이므로 **lookback을 줄여도 정상 일일 요청 건수(≈ 종목 수 ×
> 그룹 수)는 변하지 않는다.** 따라서 이 항목은 "일일 런타임 해결책"이 아니라 **payload 절감 +
> 부분 실패 후 catch-up 비용 절감**으로만 봐야 한다.

제안:

- 평일 일일 실행: `FLOW_LOOKBACK_DAYS=2` 또는 `3`
- 주말/보정 실행: `FLOW_LOOKBACK_DAYS=14`
- 장기 결손 보정: 명시 range backfill job으로 별도 운영

장점:

- 요청당 payload와 catch-up 재호출 폭을 줄인다.
- 매일 14일치를 반복 확인하는 부수 비용을 줄인다.

주의점:

- 일일 런타임의 지배 요인(요청 건수)은 줄지 않는다 — 분리(#1/#2)와 병행해야 한다.
- provider별 데이터 지연 특성을 반영해야 한다.
- shorting balance처럼 원천 지연이 있는 metric은 별도 lag 정책이 더 적합할 수 있다.

### 4. 코드 개선: missing-only 수집 (효과 한정)

현재 ticker가 범위 전체를 완전히 갖고 있지 않으면 해당 ticker의 전체 기간을 다시 요청한다.

> **효과 정정**: KRX API는 ticker당 `strtDd..endDd` 범위 1요청이므로(`provider.py`),
> 날짜 폭을 좁혀도 **요청 건수는 줄지 않고 payload만 줄어든다.** 오히려 날짜별로 쪼개면
> 요청 수가 *늘어난다*. 따라서 이 항목의 실익은 "일일 호출량 감소"가 아니라
> **부분 실패 후 catch-up payload 절감**에 한정된다. 일일 런타임을 줄이려면 #1/#2 분리가
> 우선이고, 재실행 비용 측면에서는 #5(lag-aware)가 missing-only보다 직접적이다.

개선 방향:

- ticker/group별로 누락된 날짜만 계산
- group별 metric 완전성 판단과 upsert 결과를 더 세밀하게 추적
- 이미 완성된 날짜/metric은 재호출하지 않음

검토 포인트:

- ticker당 1요청 구조에서는 "누락 날짜만 요청"이 요청 수를 늘릴 수 있으므로,
  날짜 폭을 좁히되 **여전히 종목당 1요청을 유지**하는 형태로 설계해야 실익이 있다.
- 따라서 우선순위는 #5(lag-aware 완전성)보다 낮다 — lag-aware가 재호출 자체를
  발생시키지 않게 막는 반면, missing-only는 발생한 재호출의 payload만 줄인다.

### 5. 코드 개선: metric group별 lag 정책

`short_selling_balance_quantity`는 다른 shorting metric보다 늦게 제공되는 것으로 보인다.

제안:

- `shorting_balance`에 별도 lag allowance 적용
- 최신 1~2거래일은 complete 조건에서 제외하거나 별도 보정 대상으로 분리
- `short_selling_volume`, `short_selling_value`와 `short_selling_balance_quantity`를 별도 group으로 분리

예시:

```text
shorting_trading:
  - short_selling_volume
  - short_selling_value

shorting_balance:
  - short_selling_balance_quantity
  - lag allowance: 2 trading days
```

> **구현 주의**: `fetch_shorting_metrics`는 status(volume+value)와 balance를 **한 메서드 안의
> 2 POST**로 묶어 가져온다(`provider.py`의 `SHORTING_STATUS_BLD` + `SHORTING_BALANCE_BLD`).
> 따라서 "shorting_trading / shorting_balance 별도 group"은 단순 catalog 설정 변경이 아니라
> 둘 중 하나가 필요하다.
>
> - (간단) **완전성 판정을 metric 단위로 세분화** — balance에만 lag allowance를 적용해
>   complete 조건에서 최신 1~2거래일을 빼고, fetch는 현재처럼 묶어 둔다.
> - (큼) fetch를 status/balance 2개 메서드로 분리 — 요청 수·완전성·upsert 추적을 모두 손봐야 함.
>
> 1차 권장은 fetch는 그대로 두고 **완전성 판정에만 lag allowance를 넣는** 쪽이다.

장점:

- balance 지연 때문에 shorting 전체가 매일 full retry되는 문제를 줄인다.
- 원천 데이터의 제공 지연을 수집 정책에 반영할 수 있다.

### 6. 코드 개선: KRX non-JSON circuit breaker

현재 KRX HTML 응답이 반복되어도 요청 단위 재시도를 계속 수행한다.

제안:

- 동일 bld 또는 동일 group에서 non-JSON 오류가 일정 횟수 이상 발생하면 group을 빠르게 partial fail 처리
- job 전체를 계속 끌고 가지 않고 다음 group 또는 다음 chain으로 넘김
- 오류 summary에 중단 사유를 명확히 기록

장점:

- KRX가 차단성 응답을 주는 시간대에 무의미한 재시도를 줄인다.
- Cronicle job이 수십 시간 지속되는 상황을 막는다.

주의점:

- 너무 공격적으로 끊으면 일시 오류를 회복할 기회를 잃을 수 있다.
- threshold와 cooldown은 운영 경험을 기반으로 조정해야 한다.

### 7. Cronicle 설정 개선: queue 또는 스케줄 분리

현재 `queue=0`이라 이전 job이 실행 중이면 다음 chain reaction이 실패할 수 있다.

선택지:

- 중요한 event에 `queue=1` 적용
- long-running job은 daily chain에서 분리
- daily root schedule과 보정 schedule 시간대를 분리

권장 방향:

- `shorting`처럼 오래 걸리고 불안정한 job은 queue보다 분리가 우선이다.
- daily chain의 핵심 데이터는 빠르게 끝나는 작업만 포함한다.
- 보정 job은 별도 schedule로 두고, 실패해도 daily readiness에 직접 영향을 주지 않게 한다.

## 권장 실행 순서

### Phase 0: 운영 복구

1. 현재 `jmqkx0rf608`를 계속 둘지 abort할지 결정한다.
2. 일일 chain 회복이 우선이면 abort 후 **`FLOW_EXCLUDE_GROUPS=shorting,investor`로
   foreign만** 재실행한다(investor만 남겨도 ~6~7시간이므로). investor/shorting은 별도 회수.
3. 필요하면 stale `ingestion_runs` running 상태를 정리한다.

### Phase 1: 스케줄 분리 (런타임에 가장 효과 큼)

1. daily critical chain에는 **foreign_holding만** 남긴다.
2. Cronicle chain에서 `investor`, `shorting` 둘 다 daily critical path 밖으로 뺀다.
3. `KRX Common Features`는 foreign 완료 직후 실행되도록 둔다(common이 investor/shorting을
   요구하는지 "추가 확인 필요"에서 먼저 확정).
4. investor/shorting은 각각 별도 multi-hour job으로 운영한다. 별도 wrapper는 보통 불필요하고
   `FLOW_EXCLUDE_GROUPS`로 서브셋을 지정한 Cronicle event로 구성한다.

### Phase 2: lag-aware 완전성 (재호출 폭주 차단)

1. `short_selling_balance_quantity`에 lag allowance(최신 1~2거래일 제외)를 적용해
   shorting 전체가 매일 full retry되는 문제를 끊는다 — 1차는 fetch는 그대로 두고
   **완전성 판정에만** 반영(#5 참고).
2. balance의 실제 제공 지연을 확인해 allowance 일수를 확정한다.

### Phase 3: 안정화 / 부수 비용 절감

1. KRX non-JSON 반복 시 그룹을 빠르게 partial-fail로 끊는 fast-fail / circuit breaker 추가
   (errors 77건 대부분이 재시도+backoff 누적 경로).
2. daily lookback을 `2~3일`로 축소(payload·catch-up 절감, 런타임 해결책 아님).
3. missing-only 수집(catch-up payload 절감) — 종목당 1요청을 유지하는 형태로만.
4. unit test와 작은 integration smoke test를 추가한다.

## 우선순위 제안

비용 모델(런타임 = 요청 건수 × throttle)을 반영한 현실적 우선순위는 다음과 같다.

1. **investor·shorting을 daily critical path에서 분리** (foreign만 daily) — 유일하게 일일
   critical path 런타임을 즉시 줄인다.
2. **`short_selling_balance_quantity` lag-aware 완전성** — 재호출 폭주의 근본 원인 제거.
3. **KRX non-JSON fast-fail / 재시도 축소** — 종목마다 쌓이는 backoff가 가장 비싸다.
   (그룹 단위 circuit breaker는 그다음.)
4. KRX non-JSON circuit breaker(그룹 단위)
5. daily `FLOW_LOOKBACK_DAYS` 축소 / `missing-only` — **payload·catch-up 절감으로 한정**
   (일일 런타임 해결책 아님 — 처음 초안에서 순위 하향).
6. Cronicle queue/schedule 정책 정리.

> **처음 초안 대비 변경점**: ① shorting뿐 아니라 investor도 분리 대상으로 격상,
> ② lag-aware를 missing-only보다 위로, ③ lookback 축소 / missing-only를 "런타임 해결책"에서
> "payload·catch-up 절감"으로 재포지셔닝하며 하향. 근거는 위 "비용 모델" 절.

## 추가 확인 필요 사항

- `KRX_MDC_TIMEOUT_SECONDS` 등 prod env override가 실제로 걸려 있는지 확인 — timeout 기본값은
  코드상 `20s`이며, 본문의 `150s`는 override가 있을 때만 성립한다.
- KRX shorting balance의 정상 제공 지연이 몇 거래일인지 확인 → lag allowance 일수 확정.
- `short_selling_balance_quantity`가 2026-06-18/19에 원천적으로 미제공인지, KRX 응답 오류로 수집 실패한 것인지 구분.
- **`common build/readiness`가 investor/shorting을 실제로 입력으로 요구하는지** 확인 — Phase 1
  체인(common을 foreign 직후 실행)의 전제다.
- daily chain에서 반드시 필요한 flow group과 보정 가능 group을 명확히 분류.
- 분리 후 investor / shorting 각각의 **예상 정상 런타임(≈6~7h 이상)** 을 수치로 확정하고,
  이를 일일 배치로 둘지 롤링/연속 background로 둘지 결정.
- Cronicle event 변경을 IaC(`deploy/prod/`)로 관리할지, API 기반으로 관리할지 결정.
