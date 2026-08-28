# 운영 가이드

## 일일 스케줄

KRX 정규장 시간: 09:00–15:30 KST. 당일의 온전한 데이터를 확보하기 위해 파이프라인은 반드시 **장이 마감된 후**에 실행해야 합니다.

### 권장 크론탭(cron) 스케줄 (KST 기준)

> **prod은 이 표대로 돌지 않는다.** sj2-server는 crontab이 아니라 **Cronicle**을 쓰고,
> 체인 셋과 독립 event로 나뉜다 — **18:30** FDR Universe → Prices → KRX Flows → KRX Common,
> **19:00** KIS foreign holding, **20:00** market cap, **20:30** ECOS/FRED/FDR common,
> **23:00** freshness gate, **04:00** OpenDART Corp → Financials → Share Info → XBRL.
> 아래는 이 저장소를 새로 배포하는 사람을 위한 최소 예시다. 실제 prod 스케줄은
> Cronicle API로 확인한다(`sj2-server` 스킬).

```cron
# ┌───── 분 (min)
# │ ┌───── 시 (hour)
# │ │ ┌───── 일 (day)
# │ │ │ ┌───── 월 (month)
# │ │ │ │ ┌───── 요일 (dow)
# │ │ │ │ │
# 종목 유니버스 동기화 — 매일 16:00 KST (평일)
  0  16  *  *  1-5  cd /opt/krx-data-pipeline && uv run krx-collector universe sync --source fdr

# 일봉 OHLCV 수집 (증분) — 매일 16:30 KST (평일)
# --incremental: 각 티커의 MAX(trade_date) 이후만 가져오므로 일일 catch-up이 빠릅니다.
  30 16  *  *  1-5  cd /opt/krx-data-pipeline && uv run krx-collector prices backfill --market all --incremental

# 일별 시가총액·거래대금·상장주식수 — 매일 16:45 KST (평일)
# prices backfill과 같은 KRX 원천이므로 시간대를 겹치지 않게 둡니다 (exit 75 lock).
# 원천(KRX Open API)이 T+1이라 이 실행이 가져오는 것은 **전 거래일**입니다.
# 당일치를 기대하지 마십시오 — 날짜를 안 넘기면 갭 탐지가 밀린 세션을 알아서 채웁니다.
  45 16  *  *  1-5  cd /opt/krx-data-pipeline && uv run krx-collector prices market-cap-backfill --market all

# 데이터 정합성 검증 — 매일 17:00 KST (평일)
  0  17  *  *  1-5  cd /opt/krx-data-pipeline && uv run krx-collector validate --market all

# 월말 유니버스 스냅샷 — 매월 1일 05:00 KST
# 전월 말일까지의 스냅샷을 채웁니다. 이미 있는 날짜는 건너뜁니다.
# prod에서는 돌리지 않습니다 — daily_market_cap이 일별 PIT 유니버스를 대신합니다
# (04_w1_pit_universe.md §3.6). 감사·교차검증이 필요할 때만 씁니다.
  0   5  1  *  *    cd /opt/krx-data-pipeline && uv run krx-collector universe backfill-snapshots

# 기업개황 refresh — 매월 1일 05:30 KST
# 신규 상장분만 받습니다 (profile_fetched_at이 NULL인 것).
  30  5  1  *  *    cd /opt/krx-data-pipeline && uv run krx-collector dart sync-corp-profile --universe-scope historical

# freshness 게이트 — 매일 23:00 KST
# 저녁 수집 창이 끝난 뒤에 돌아야 합니다. --fail-if-stale이 없으면 출력만 하고
# 항상 exit 0이라, 스케줄에 넣어도 알람이 되지 않습니다.
# daily_market_cap은 T+1 원천이라 별도 기본 예산 2거래일을 씁니다. 다른 일별 도메인은
# 기본 1거래일 예산을 그대로 씁니다 — 아래 "KRX Open API 일별매매정보는 T+1이다".
  0  23  *  *  *    cd /opt/krx-data-pipeline && uv run krx-collector ops freshness-report --fail-if-stale
```

> **Tip:** crontab 맨 위에 `TZ=Asia/Seoul`을 설정하거나, systemd timer의 `OnCalendar=`를 사용하여 UTC 혼동을 방지하는 것이 좋습니다.

### 수집 안 됨을 탐지하는 방법 (freshness 게이트)

**돌지 않은 run은 어디에도 흔적을 남기지 않는다.** `ingestion_runs`에 행이 생기지 않고,
실패한 잡도 없다. 2026-08-14에 sj2-server가 저녁 내내 다운돼 KRX 체인과 common sync가
통째로 빠졌을 때 모든 raw 테이블이 하루 뒤처졌지만 아무 신호도 없었다.

증거는 데이터 자체에만 있다. 그래서 게이트가 보는 것은 "실패한 run이 있는가"가 아니라
**"가장 최신 행이 달력이 요구하는 만큼 최신인가"**다.

```bash
# 사람이 읽는 리포트 (항상 exit 0)
krx-collector ops freshness-report

# 스케줄러가 읽는 게이트 (미달이면 exit 1)
krx-collector ops freshness-report --fail-if-stale
```

| 옵션 | 기본값 | 대상 |
|---|---|---|
| `--max-lag-trading-days` | `1` | `daily_ohlcv`, flow 그룹, KRX/KIS/PYKRX 계열 common series. **1 = 최근 세션이 저장돼 있어야 한다** |
| `--max-lag-calendar-days` | `14` | 카탈로그 metadata가 없는 common series의 fallback |

FDR·ECOS·FRED series는 `common_feature_series.max_stale_business_days`를 쓴다. 빈도와
원천별 공개 지연을 반영해 일별 series는 보통 7~10영업일, 월별 ECOS series는
45~90영업일을 허용한다. 해외시장 휴장과 월간 지표 공개 주기를 정상 지연으로 처리하되,
각 series의 카탈로그 예산을 넘으면 gate가 실패한다.

엄격한 KRX 거래일 예산과 series별 공개 지연 예산을 나눈 이유는, 해외시장과 발표 지연이
있는 시리즈를 KRX 거래일 기준으로 재면 매일 걸려서 **아무도 안 보는 게이트**가 되기
때문이다. 빈 테이블은 "검사 대상 없음"이 아니라 stale로 센다.

## 런북 (Runbook)

### 백필(Backfill) 재실행하기

백필 작업은 `ON CONFLICT … DO UPDATE` 덕분에 **멱등성(Idempotent)**을 가집니다.
동일한 파라미터로 다시 실행해도 데이터가 중복 생성되지 않으며, 최신 데이터로 기존 행을 덮어씁니다.

```bash
# 특정 종목의 특정 기간 다시 백필하기
uv run krx-collector prices backfill --tickers 005930 --start 2024-01-01 --end 2024-12-31

# 특정 시장의 모든 종목 처음부터 다시 백필하기
uv run krx-collector prices backfill --market kospi
```

### 백필 모드: 기본(gap detection) vs `--incremental`

| 모드 | 시작일 결정 | 조회 범위 | 주 용도 |
|---|---|---|---|
| **기본** | `--start` (또는 2000-01-01). `--start`가 없을 때만 각 티커의 `MIN(trade_date)`로 자동 클램핑 | 거래일 캘린더 기준 누락된 모든 영업일을 구간으로 묶어 fetch | 최초 백필, 히스토리 보강, 중간 구멍(holes) 메우기 |
| **`--incremental`** | 각 티커의 `MAX(trade_date) + 1` (또는 `--start` 중 더 늦은 날). baseline이 없으면 `--new-ticker-start` / `listing_date` / `first_seen_date`로 결정(아래 참고) | 시작일 ~ `--end`까지 단일 연속 구간 | 매일 돌리는 catch-up cron |

**언제 어떤 모드를 써야 하나?**

- **매일 돌리는 자동화 작업** → `--incremental` 사용. gap 검출 쿼리를 건너뛰고 티커당 한 번의 가벼운 `MAX()` 조회 후 신규 영업일만 가져오므로 가장 빠릅니다.
- **최초 백필** 또는 **장기 히스토리 보강** → 기본 모드. 누락된 모든 거래일을 거래일 캘린더 기준으로 찾아 채웁니다.
- **데이터 중간에 구멍이 생긴 티커 복구** → 기본 모드. `--incremental`은 tail만 보므로 중간 구멍을 못 채웁니다.

```bash
# 일일 증분 수집 (cron 권장)
uv run krx-collector prices backfill --market all --incremental

# 특정 종목만 증분
uv run krx-collector prices backfill --tickers 005930,000660 --incremental

# 기본 모드 (최초 백필 또는 hole 보강)
uv run krx-collector prices backfill --market all
```

> **메모**: 두 모드 모두 주말·공휴일은 `query_missing_days` / 단일 구간 fetch 단계에서 자연스럽게 배제됩니다. 또한 기본 모드에서는 `MIN(trade_date)` 클램프 덕분에 005930처럼 pykrx가 제공하지 못하는 과거 구간(예: 2014-01-20 이전)을 매 실행마다 헛스캔하지 않습니다. 단, `MIN(trade_date)` 클램프는 **`--start`가 없을 때만** 적용됩니다. 명시적 `--start`는 운영자 의도로 존중되므로, 최근 baseline이 이미 있어도 그보다 이른 히스토리를 다시 채울 수 있습니다(아래 신규 종목 full-history 복구 참고).

#### 신규 상장 종목 (baseline 없는 티커) 시작일 결정

`--incremental` 모드에서 `daily_ohlcv` baseline이 아직 없는 `ACTIVE` 티커는 다음 우선순위로 시작일을 정합니다(운영자가 `PRICE_NEW_TICKER_START`를 설정할 필요 없음):

1. `--new-ticker-start`가 있으면 그대로 사용(클램프하지 않음).
2. 없으면 `stock_master.listing_date`(FDR 제공) → 자동 시작일; **클램프**.
3. 없으면 `stock_master.first_seen_date`(수집기가 처음 ACTIVE로 관측한 날) → 자동 시작일; **클램프**.
4. 셋 다 없으면 baseline-missing 에러로 기록하고 해당 티커만 건너뜁니다.

클램프는 자동 시작일(2·3번)을 `--max-auto-range-days` 가드 윈도(`end - (N-1)`)까지 끌어올려, 긴 과거 백필이 daily critical path에 들어오지 않게 합니다. 클램프된 티커 수는 `ingestion_runs.counts.baseline_clamped_tickers`와 stdout(`- Clamped new-ticker starts: N`)에 집계됩니다. 명시적 `--new-ticker-start`(오래된 날짜)와 오래된 기존 `MAX(trade_date)`는 여전히 `--allow-large-range` 없이는 range 가드에서 실패합니다.

**클램프된 티커의 full-history 복구** — daily chain 밖에서, **비증분(`--incremental` 생략)** 모드로 명시적 `--start`를 주어 실행합니다. §5.7 수정 덕분에 명시적 `--start`는 최근 baseline의 `MIN(trade_date)`로 앞당겨지지 않고 그대로 존중됩니다:

```bash
# baseline_clamped_tickers로 잡힌 종목을 상장일부터 다시 채우기
uv run krx-collector prices backfill \
  --tickers 475040,153890,0164H0 \
  --start 2026-06-01 --end 2026-07-03
```

상장일보다 늦게 첫 가격이 찍힌 종목을 찾는 운영자 쿼리:

```sql
SELECT sm.ticker, sm.name, sm.market, sm.listing_date, sm.first_seen_date,
       MIN(d.trade_date) AS first_price_date
FROM stock_master sm
JOIN daily_ohlcv d ON d.ticker = sm.ticker AND d.market = sm.market
WHERE sm.status = 'ACTIVE' AND sm.listing_date IS NOT NULL
GROUP BY sm.ticker, sm.name, sm.market, sm.listing_date, sm.first_seen_date
HAVING MIN(d.trade_date) > sm.listing_date
ORDER BY sm.listing_date DESC, sm.ticker;
```

### 종목 유니버스 전체 갱신 (Full Refresh)

`stock_master` 데이터가 꼬였거나 완전히 새로 덮어쓰고 싶을 때 사용합니다:

```bash
uv run krx-collector universe sync --source fdr --full-refresh
```

증분 비교(Diff)를 계산하지 않고 기존 데이터를 모두 새 데이터로 교체합니다.

### 데이터 품질 검증 (Validation)

```bash
# 특정 날짜 검증하기
uv run krx-collector validate --date 2024-06-15 --market all

# 오늘 날짜 검증하기 (기본값)
uv run krx-collector validate
```

```bash
# 유니버스 변동 임계값 조정 (0이면 해당 검사 끔)
uv run krx-collector validate --date 2016-06-30 --universe-drift-pct 5
```

수행되는 검증 항목:
1. **OHLC 정합성**: 저가 ≤ 시가 ≤ 고가, 저가 ≤ 종가 ≤ 고가, 가격 > 0 체크.
2. **누락 종목**: 그 날짜에 **실제로 상장돼 있던 종목** 기준으로 누락을 센다.
3. **유니버스 카운트 변동**: source별로 연속 스냅샷 크기를 비교해 임계값(기본 ±5%) 초과 시 경고.

**2번의 기준이 2026-08-15에 바뀌었다.** 이전에는 `get_active_stocks()`, 즉 **오늘 상장된
종목**과 비교했다. 과거 날짜에서는 그게 구조적으로 구멍을 못 본다 — 누락된 종목이 곧 그 뒤
상폐된 종목이라 기대 집합에서도 빠져 상쇄되기 때문이다. 실제로 2016-06-30에서 이전 코드는
누락 0을 보고했고, 지금은 **286 / 2,056 (13.9%)** 을 보고한다.

과거 날짜는 그 시점 이전의 가장 가까운 `stock_master_snapshot`을 기준으로 삼는다.
스냅샷이 없으면 현재 유니버스로 폴백하며, 어느 쪽을 썼는지 `ingestion_runs.params`의
`universe_source`에 남는다. **`counts.universe_size`가 같이 기록되므로 "누락 0"이
"완전"인지 "빈 유니버스"인지 구분된다.**

> 과거 구간을 검증하려면 `universe backfill-snapshots`가 먼저 돌아 있어야 한다.

### 신규 수집 명령 (2026-08 추가)

> **N1·N3는 2026-08-16부터 실행하지 않는다.** 두 명령 모두 KRX를 직접 치는데
> 그 경로가 약관 위반으로 차단됐다. 아래 "KRX 접근 제한" 절 참고.
> Open API 어댑터(K-4)로 교체한 뒤 재개한다. N2는 OpenDART라 해당 없다.

```bash
# N1 — 일별 KRX 시가총액 · 거래대금 · 상장주식수  ※ 중단됨
uv run krx-collector prices market-cap-backfill --start 2014-06-02 --end 2026-08-15

# N3 — 월말 역사적 유니버스 스냅샷 (생존편향 감사)  ※ 중단됨, 60/152
uv run krx-collector universe backfill-snapshots --start 2014-06-01 --end 2026-08-15

# N2 — OpenDART 기업개황 (업종코드 · 설립일 · 결산월)
uv run krx-collector dart sync-corp-profile --universe-scope historical
```

**`--universe-scope`가 모든 수집 명령에 붙는다.**

| 값 | 대상 | 쓸 때 |
|---|---|---|
| `current` (기본) | 현재 상장 (DART 2,657 / 주가 active) | 일 단위 증분 sync |
| `historical` | ticker를 가진 적 있는 전체 (DART 3,959) | **백테스트에 들어가는 모든 백필** |

기본값이 `current`인 것은 일 sync 기준이다. **백필은 거의 항상 `historical`이어야 한다** —
상폐 법인이 빠지면 부실·전이 피쳐가 구조적으로 부풀려진다
(`docs/dev/20260731_raw_features/02_data_expansion_plan/poc/survivorship_gap.md`).

```bash
# 조정주가가 stale해진 종목 이력 재수집 (분할 이후 소급 재조정 미반영)
uv run krx-collector prices backfill --refetch --tickers 005930,000660 --start 2014-06-02

# 상폐 종목 가격 백필
uv run krx-collector prices backfill --universe-scope historical --start 2014-06-02
```

`--refetch`는 gap detection을 건너뛰고 구간 전체를 다시 받아 덮어쓴다.
`--incremental`과 함께 쓸 수 없다(아무것도 재수집하지 않으면서 성공처럼 보인다).

### 데이터베이스 초기화

```bash
# 테이블 생성 (멱등성 보장 — CREATE TABLE IF NOT EXISTS 사용)
uv run krx-collector db init
```

### 계정/수급 raw 파이프라인 실행

```bash
# 1) OpenDART corp_code 마스터 동기화
uv run krx-collector dart sync-corp

# 2) 재무 raw 적재
uv run krx-collector dart sync-financials --tickers 005930 --bsns-years 2025 --reprt-codes 11011 --fs-divs CFS

# 3) 주식수 / 배당 / 자사주 raw 적재
uv run krx-collector dart sync-share-info --tickers 005930 --bsns-years 2025 --reprt-codes 11011

# 4) XBRL 원문 파싱
uv run krx-collector dart sync-xbrl --tickers 005930 --bsns-years 2025 --reprt-codes 11011

# 5) 수급 raw 적재 (KRX MDC 직접 호출)
uv run krx-collector flows sync --tickers 005930 --start 2026-04-17 --end 2026-04-17

# 6) 공시 접수 이력 적재 (Phase B: 원공시/정정 관계, SUE original-event source)
uv run krx-collector dart sync-filings --tickers 005930 --years 2025

# 7) 증자(감자) 현황은 sync-share-info에 포함되어 함께 수집됨 (irdsSttus)
```

재무 metric 정규화와 common daily fact 생성은 PostgreSQL CLI가 아니라 아래 "Parquet compute 파이프라인"에서 DuckDB 마트로 실행합니다.

`dart sync-filings`는 `dart-backfill-all-years.sh`에 포함돼 있습니다(아래 "OpenDART 전체
사업연도 백필"). 일일 wrapper에는 아직 없습니다 — 접수 이력은 Phase B 전용이라 매일 돌릴
이유가 없고, 백필 스크립트가 현재 연도를 매번 다시 받으므로 최신분도 그때 따라옵니다.
`dart sync-share-info`는 기존 주식수/배당/자사주 raw와 함께
`dart_capital_change_raw`(증자·감자 현황)도 같은 실행에서 수집합니다.

`dart sync-filings`는 다른 OpenDART 커맨드와 달리 (corp, 연도) 윈도우 하나가 여러 페이지로
나뉘어 옵니다. `--rate-limit-seconds`는 윈도우 사이와 **페이지 사이 모두**에 적용되므로, 공시가
많은 발행사에서도 요청 간격이 다른 수집기와 같게 유지됩니다. 대량 백필에서는 이 값을 기본값
0.2보다 높여(예: 0.5) 평균 호출률을 낮추는 것을 권장합니다.

원공시 receipt를 지정해 XBRL을 다시 받아야 할 때(예: 기존 raw가 정정본만 captured한 경우)는
`dart backfill-xbrl-receipts --targets-file <jsonl>`을 사용합니다. 어떤 receipt가 백필 대상인지
찾는 것은 이 커맨드의 범위가 아니며 `dart_filing_receipt_raw` 위에서 별도로 분석해야 합니다.

### OpenDART 전체 사업연도 백필

전체 사업연도 백필은 매일 최신분을 처리하는 계정/수급 이벤트와 분리해서 실행합니다. 백필은 시간이 길고 OpenDART quota 소진으로 실패 종료될 수 있으므로, Cronicle에서는 별도 manual 이벤트(예: `sdc_manual_backfill_opendart_all_years`)로 등록합니다.

백필 실행 전 안전 절차:

1. Cronicle에서 OpenDART daily root인 `sdc_daily_opendart_corp`를 일시 disable합니다.
2. `get_active_jobs` 또는 UI에서 OpenDART daily chain이 이미 실행 중이 아닌지 확인합니다.
3. 백필을 실행합니다.
4. 백필 종료 후 `sdc_daily_opendart_corp`를 다시 enable합니다.

`dart-backfill-all-years.sh`는 `opendart` source lock을 유지하지만, daily wrapper는 기본값에서 source lock을 잡지 않습니다. 따라서 daily event disable이 daily-backfill overlap을 막는 1차 방어선입니다. 긴급하게 daily lock 보호를 되살려야 할 때만 daily event script에 `SDC_DAILY_USE_SOURCE_LOCK=1`을 주입합니다.

권장 Cronicle command:

```bash
/home/whi/apps/sdc/bin/dart-backfill-all-years.sh
```

스크립트 기본값:

- 시작연도: `2015`
- 종료연도: 현재연도 - 1
- 보고서 코드: `11011,11012,11013,11014`
- 재무제표 구분: `CFS,OFS`
- 처리 순서: 최신 연도부터 `dart sync-financials`, `dart sync-share-info`, `dart sync-xbrl` raw 적재
- 마지막 단계: `dart sync-filings`로 공시 접수 이력(`dart_filing_receipt_raw`) 적재

접수 이력 단계는 앞의 세 단계와 **연도 축이 다릅니다.** 나머지는 사업연도(bsns_year) 기준인데
접수 이력은 접수 **달력연도** 기준입니다 — FY2025 사업보고서는 2026년에 접수되므로, 이 단계만
`end_year`가 아니라 **현재 달력연도까지** 돌립니다. 순서는 과거 연도를 내림차순으로 먼저 하고
현재 연도를 맨 마지막에 둡니다. 저장된 과거 연도는 이후 실행에서 영원히 skip되지만 현재 연도는
설계상 매번 다시 받으므로, 마지막에 둬야 가장 신선한 상태로 끝납니다.

접수 이력을 마지막에 두는 이유는 또 있습니다. quota로 exit 75가 나도 다른 모든 consumer가
쓰는 metric raw는 이미 들어와 있게 됩니다. 접수 이력은 Phase B SUE 원공시 source 전용입니다.

필요하면 Cronicle 이벤트 환경 변수로 범위를 좁힙니다.

```bash
SDC_DART_BACKFILL_START_YEAR=2018
SDC_DART_BACKFILL_END_YEAR=2025
SDC_DART_BACKFILL_INCLUDE_CURRENT_YEAR=0
SDC_DART_BACKFILL_REPRT_CODES=11011,11012,11013,11014
SDC_DART_BACKFILL_FS_DIVS=CFS,OFS
SDC_DART_BACKFILL_FILINGS=1              # 0이면 접수 이력 단계 skip
SDC_DART_BACKFILL_FILINGS_END_YEAR=2026  # 기본값 = 현재 달력연도
SDC_DART_BACKFILL_FILINGS_RATE_LIMIT=0.5 # sync-filings에만 적용
```

모든 OpenDART API key가 일일 한도에 도달하면 각 OpenDART CLI는 exit code `75`로 종료됩니다. 스크립트는 `set -euo pipefail`이므로 그 지점에서 멈추고, 다음 실행 때 이미 저장된 raw/XBRL은 skip되어 같은 범위를 이어받습니다.

#### 소요 시간 실측 (2026-08-12)

접수 이력 11개 연도를 prod에서 실제로 돌린 결과 **연도당 약 36분**, 전체 약 6시간 30분이었습니다
(`--rate-limit-seconds 0.5`, API key 9개). 요청 수는 약 32,000건으로 일일 한도(9키 × 20,000)에는
여유가 있었습니다. `sync-share-info`를 연간보고서(`11011`)만으로 한정한 증자·감자 수집은 3개
연도에 약 1시간 10분이었습니다.

전체 백필(재무 + 주식수 + XBRL + 접수 이력)은 하루를 넘길 수 있으므로, 다음 날 04:00 OpenDART
daily chain과 겹치지 않는지 확인하고 필요하면 daily event를 그때까지 disable 상태로 둡니다.

### KRX 수급 범위 백필

KRX 수급 히스토리 보수는 daily KRX chain과 분리해서 명시 범위 wrapper로 실행합니다.

```bash
FLOW_START=2026-05-01 FLOW_END=2026-05-31 /home/whi/apps/sdc/bin/flows-backfill-range.sh
```

백필 실행 전 안전 절차:

1. Cronicle에서 KRX daily root인 `sdc_daily_fdr_universe`를 일시 disable합니다.
2. `get_active_jobs` 또는 UI에서 KRX daily chain이 이미 실행 중이 아닌지 확인합니다.
3. 백필을 실행합니다.
4. 백필 종료 후 `sdc_daily_fdr_universe`를 다시 enable합니다.

`flows-backfill-range.sh`는 `krx_marketdata` source lock을 유지하지만, daily KRX wrapper는 기본값에서 source lock을 잡지 않습니다. 따라서 daily event disable이 daily-backfill overlap을 막는 1차 방어선입니다. 자동 schedule guard는 아직 wrapper에 구현하지 않았습니다.

### 공통 시장/거시 feature raw 갱신

공통 feature source sync는 raw 수집 이벤트로 운영합니다. 기존 가격/수급/계정 파이프라인과 독립적으로 실행할 수 있으며, coverage/readiness 판단은 sj2가 아니라 아래 "Parquet compute 파이프라인"에서 수행합니다.

권장 Cronicle command:

```bash
/home/whi/apps/sdc/bin/common-features-refresh.sh
```

스크립트 기본 흐름:

1. `common seed-catalog --init-schema`
2. 일간 source sync: `fdr,fred,ecos,krx`, 최근 45일
3. monthly macro sync: CPI/PPI/M2/CSI, 최근 540일, `--force`
4. 파생 daily fact, coverage, readiness는 로컬/compute 노드에서 `bin/parquet-compute-all.sh`로 실행

필요하면 Cronicle 이벤트 환경 변수로 범위를 조정합니다.

```bash
SDC_COMMON_DAILY_LOOKBACK_DAYS=45
SDC_COMMON_MACRO_LOOKBACK_DAYS=540
SDC_COMMON_BUILD_LOOKBACK_DAYS=120
SDC_COMMON_READINESS_LOOKBACK_DAYS=60
SDC_COMMON_MACRO_MAX_LAG_DAYS=60
SDC_COMMON_RATE_LIMIT_SECONDS=0.2
SDC_COMMON_REQUIRED_COVERAGE_RATIO=1.0
```

운영 전제:

- `.env`에 `ECOS_API_KEY`, `FRED_API_KEY`가 설정되어 있어야 합니다.
- KRX direct source는 필요 시 `.env`의 `KRX_ID`/`KRX_PW`로 로그인 retry를 수행합니다.
- monthly macro는 revision 가능성이 있어 최근 540일을 `--force`로 재조회합니다. 이 범위는 YoY 계산에 필요한 전년동월 raw도 함께 보강합니다.

### 데이터 수집 이력 조회

```sql
-- 최근 10번의 실행 이력 확인
SELECT run_id, run_type, started_at, ended_at, status, counts
FROM ingestion_runs
ORDER BY started_at DESC
LIMIT 10;

-- 실패 또는 부분 실패 이력 확인
SELECT *
FROM ingestion_runs
WHERE status IN ('failed', 'partial')
ORDER BY started_at DESC;
```

`ingestion_runs.status` 해석:

- `running`: 아직 실행 중
- `success`: 모든 요청 성공 또는 no-data
- `partial`: 파이프라인 자체는 완료됐지만 일부 요청이 실패
- `failed`: 파이프라인이 중간에 중단됨

`counts` 공통 필드:

- `error_count`: 실패한 요청 수
- `partial_failure_count`: 부분 실패 수
- `completed_request_count`: 오류 없이 끝난 요청 수

## 모니터링

### 추적해야 할 주요 지표

- 일별 `ingestion_runs.status IN ('failed', 'partial')` 발생 건수.
- `stock_master` 전체 행 개수 (평소 대비 ± 5% 내로 안정적인지 확인).
- `daily_ohlcv` 일별 데이터 증가량 (거래일 기준 매일 약 2,500건 내외의 새로운 행이 추가되어야 함).
- 백필에 소요된 시간.

### 알림(Alerting) 권장 사항

- `ingestion_runs` 테이블에 `status = 'failed'`가 기록되면 즉시 알림.
- `ingestion_runs` 테이블에 `status = 'partial'`가 반복 기록되면 경고 알림.
- 유니버스 동기화 시 수집된 종목 수(`record_count`)가 평소 대비 10% 이상 감소하면 알림.
- 영업일(주말, 공휴일 아님)인데 `daily_ohlcv`에 새로운 행이 전혀 없다면 알림.

## Parquet compute 파이프라인 (수동 실행)

> 리팩터(2026-07): `metrics normalize`·`common build-daily`·`coverage-report`·`readiness-report`
> 같은 *compute* 단계는 더 이상 sj2(Postgres)에서 돌지 않습니다. sj2는 **raw 수집 전용**이고,
> 파생 데이터는 사용자가 필요할 때 로컬에서 **parquet → DuckDB 마트**로 재계산합니다. 자동
> 스케줄러는 없습니다(raw 수집만 sj2가 자동).

### 한 번에 실행

```bash
# raw 미러 → parquet export → freshness 게이트 → normalize/build-daily 마트 → coverage/readiness
bin/parquet-compute-all.sh

# feat_*/labels 마트까지 빌드
bin/parquet-compute-all.sh --features
```

### 경로 선택 (`--route local` / `--route remote`)

raw 확보는 **두 경로 중 선택**할 수 있습니다(`docs/dev/20260730_refactor_dump/00_dual_route_raw_export_plan.md`).
`--route`를 생략하면 지금까지와 완전히 동일하게 동작합니다(`local`이 기본값).

| 경로 | 흐름 | 로컬 SSD | 용도 |
|---|---|---|---|
| **`local`(기본)** | sj2 → `db sync-remote --full-refresh` → mydb → parquet | ~189 GB 상주 | 반복 재계산, 오프라인, 재현 가능한 재읽기 |
| **`remote`** | sj2 → parquet 직접 캡처 | 0 GB(출력만) | 1회성 최신 캡처, 디스크 절약, 전체적으로 더 빠름(90분 미러 갱신을 생략) |

```bash
# 직접 캡처 경로 (sj2 SSH 터널 필요 시 --ssh-host 전달)
bin/parquet-compute-all.sh --route remote --ssh-host sj2-server
```

- `remote` 경로는 raw-parquet-exporter 바이너리를 손대지 않고 `db with-remote-dsn`으로 감싸
  `SDC_REMOTE_DSN`을 export 하위 프로세스에만 주입합니다. 자격증명은 `stock_data_collector_secrets/db_info`에서
  읽습니다(`db sync-remote`와 동일).
- **캡처 격리 수준.** 정책은 여전히 테이블 export 시점별 read-committed입니다(공유 스냅샷 아님).
  raw 테이블이 `ON CONFLICT ... DO UPDATE`로 기존 행도 갱신하므로, export가 수집(Cronicle) 창과
  겹치면 한 테이블 안에 서로 다른 시점의 값이 섞일 수 있습니다. 그래서 결과물을 "스냅샷"이 아니라
  **"캡처(capture)"**라고 부릅니다. 가능하면 Cronicle 체인(18:30/20:30/23:30/04:00 KST)과 겹치지 않는
  시각에 실행하세요. 캡처 창과 수집 창이 겹쳤는지는 `_SUCCESS.json`의 `collector_overlap`에 기록됩니다.
- `--route remote --features`는 아직 차단되어 있습니다 — 모델별 dataset 경로(`dataset_dir()`)가 아직
  `source=`로 분리되지 않아, remote 캡처로 만든 feature/label 마트가 local 캡처 결과를 같은
  `--snapshot-date`에서 덮어쓸 수 있기 때문입니다.

### `_SUCCESS.json` 완료 표식

`bin/raw-parquet-export-all.sh`는 설정된 13개 테이블이 **전부** skip/export/resume으로 완료된 뒤에만
`data_lake/raw_postgres/snapshot_date=<D>/source=<S>/_manifests/_SUCCESS.json`을 원자적으로 씁니다.
`research.etl.compute_all`은 이 표식이 없거나 테이블 목록이 기대치(13개)와 다르면 **거부**합니다 —
export가 절반만 끝난 레이크로 조용히 계산이 도는 것을 막기 위함입니다.

긴급하게 우회해야 하면(예: 부분 이력만으로 먼저 확인하고 싶을 때) `--allow-incomplete-lake`를 씁니다.
기본은 off이고, 쓰면 stderr에 경고가 남습니다.

```bash
uv run python -m research.etl.compute_all --snapshot-date 2026-07-30 --allow-incomplete-lake
```

### export 실패 후 재실행

한 테이블이라도 실패하면 `_SUCCESS.json`은 쓰이지 않고 스크립트는 실패한 테이블 목록과 함께
non-zero로 종료합니다. **같은 `--snapshot-date`/`--route`로 다시 실행**하면 됩니다 — 이미 완료된
테이블(유효한 manifest 존재)은 skip, 도중에 끊긴 테이블(체크포인트 1개, `raw_id_range`/`date_month`
전략만)은 `resume`으로 이어받고, resume을 지원하지 않는 나머지 7개 테이블(`full_table`/
`snapshot_items` 전략)은 저렴하므로 그냥 다시 `--force` export합니다. 한 테이블에 미완료 체크포인트가
2개 이상 쌓여 있으면(반복 중단) 자동 판단을 포기하고 에러를 내며 `--force-table <테이블명>`을
요구합니다.

### 단계 (각 단계는 게이트)

1. `db sync-remote --full-refresh`(`--route local`) 또는 sj2 직접 캡처(`--route remote`) — raw +
   `common_feature_series` 확보.
2. `bin/raw-parquet-export-all.sh` — 확보한 DB → `data_lake/raw_postgres/<snapshot>/...` parquet.
3. **freshness 게이트** — raw 입력이 충분히 신선한지(`common_feature_observation_raw` 최신 관측이
   series별 허용 lag 이내) 확인. 미달 시 non-zero exit + stderr 요약 → compute가 stale raw 위에서
   도는 것을 차단.
4. **normalize/build-daily 마트** — `stock_metric_fact` / `common_feature_daily_fact`를 raw에서 재계산
   (`research/etl/marts/`). 룰·카탈로그는 `krx_collector.definitions` 코드 정의에서 직접 읽습니다.
5. **coverage / readiness 게이트** — `common_feature_daily_fact` 마트 위에서 커버리지/준비도 체크.
   미달 시 non-zero exit + stderr 요약.

### 부분 실행

```bash
# 이미 미러/export된 스냅샷을 재계산만 (sync/export 건너뜀)
bin/parquet-compute-all.sh --skip-sync --snapshot-date 2026-06-19

# 특정 단계부터: sync|export|freshness|marts|reports|features
bin/parquet-compute-all.sh --from-step marts --snapshot-date 2026-06-19

# readiness 임계값 조정(부분 이력 스냅샷에서 게이트 완화)
bin/parquet-compute-all.sh --from-step reports --required-coverage-ratio 0.0
```

### 게이트 실패 시

대화형 실행이므로 별도 notifier가 없습니다. 스크립트가 non-zero로 종료하며 stderr에 사람이 읽는
요약(어떤 series/feature가 왜 미달인지)을 출력합니다. freshness 실패면 raw 수집(sj2)을 먼저
확인하고, readiness 실패면 해당 feature의 커버리지/누락/PIT 위반 내역을 보고 재수집 또는
스냅샷/임계값을 조정해 재실행하세요.

## KRX 접근 제한 (2026-08-16~)

**결론부터. 스크래핑 경로로 KRX를 치는 작업은 하지 않는다.**

2026-08-16, sj2-server IP가 KRX Data Marketplace에서 차단됐다. 안내문이 사유를 명시했다 —
**약관 제10조 제2호(자동화 수단을 이용한 정보 무단 수집 금지) 위반**이고,
제6조 제2항에 따라 이용이 제한된다. 탐지일로부터 **1일**.

**속도로 푸는 문제가 아니다.** 차단 후 스로틀을 MDC와 같은 1.5~4.0초로 통일하고
**0.32 req/s**(직전의 1/3)로 다시 돌렸는데 **95요청 만에 재차단**됐다.
1차 차단은 9시간 만에 풀렸으나 재차단은 5분 만에 왔다. 약관에 속도 조건이 없다.

### 지금 상태 (2026-08-22 KIS 전환 반영) — 매일 KRX를 치는 것은 **둘**이다

| 경로 | 문 | 상태 |
|---|---|---|
| `universe sync --source fdr` | **익명** (MDC 메타데이터 2요청 × 시장) | 매일 18:30 — **prod에 `AUTH_KEYS`가 들어오면 `--source krx-openapi`로 바꾼다** |
| `flows sync` | MDC 로그인 | **KIS 6개 지표로 전환.** KIS 정기 이벤트를 등록했고, `short_selling_balance_quantity` 보완 때문에 기존 KRX 체인은 유지한다 |
| `common sync --sources krx` | MDC 로그인 | 매일 (체인) — 대체재 없음 |
| ~~`prices backfill`~~ | ~~pykrx 로그인~~ | **닫혔다.** naver 직접 어댑터가 기본값 |
| ~~`prices market-cap-backfill`~~ (N1) | ~~pykrx 로그인~~ | **닫혔다.** Open API가 기본값 (K-4) |
| ~~`universe backfill-snapshots`~~ (N3) | ~~pykrx 로그인~~ | **닫혔다.** Open API가 기본값 (K-4) |
| `universe sync --source pykrx` · `common-sync-pykrx.sh` · live test | pykrx 로그인 | **`ALLOW_KRX_SCRAPING=1` 없이는 안 돈다** |

**`prices backfill`이 가장 컸다.** 데이터는 원래 naver였다 — `adjusted=True`가 기본이라
`pykrx.website.naver`를 타는데, `get_pykrx_stock_module()`이 pykrx를 import하는 순간
`webio.py`의 모듈 레벨 `build_krx_session()`이 돌아 **실행당 로그인 3~4요청이
`data.krx.co.kr`로 나갔다.** naver 어댑터를 따로 만들어 끊었다.
좁은 모듈만 import해서 피할 수는 없다 — `pykrx.website.naver.core`가 같은 `webio`를 쓴다.

**FDR에 대한 기록 두 개가 서로 달랐는데, 둘 다 반만 맞다.** 실제로는
`fdr.StockListing`이 **`data.krx.co.kr`을 두 번 친다**(같은 URL을 중복 호출한다 —
`executeForResourceBundle.cmd`, `max_work_dt` 하나 읽으려고). **목록 데이터 자체는
GitHub CSV 캐시**에서 온다. 즉 "KRX를 친다"도 "GitHub을 읽는다"도 각각 맞다.
로그인은 하지 않지만 **우리 `HumanThrottle` 밖이고 계수에도 안 잡힌다.**

**FDR의 pykrx 자동 폴백은 제거했다.** 차단 상황에서 가장 나쁜 실패 방식이었다 —
FDR이 흔들리는 순간(= KRX가 흔들리는 순간) 익명 조회가 **로그인 조회로 자동 전환**됐다.
이제 FDR 실패는 그냥 실패다.

**`shorting` freshness를 먼저 손봤다.** 그룹 최신일이 metric 3개의 **최솟값**인데
`short_selling_balance_quantity`는 KIS가 못 채운다. KRX를 끄면 이 그룹이 매일 stale로
뜨고, "매일 걸리는 게이트는 아무도 안 본다"에 정면으로 걸린다.
`DISCONTINUED_FLOW_METRICS`로 **수집 중단을 선언**해 예산에서 빼되,
`ops freshness-report`에는 마지막 수집일과 사유가 계속 보인다.

> **운영 지속은 여전히 미결이다.** "하루 53요청이라 규모가 아니다"는 근거가 아니다 —
> **약관 제10조 제2호에 속도 조건이 없다.** 남은 둘(`flows sync`·`common sync --sources krx`)을
> 계속 돌린다면 근거는 **"데이터 공백을 감수하지 않기로 한 선택"**이어야 하고,
> 둘 다 대체재가 준비되는 즉시 끈다. → K-0b

전체 인벤토리:
[`poc/krx_access_inventory.md`](dev/20260731_raw_features/02_data_expansion_plan/poc/krx_access_inventory.md)

### 해결 방향

KRX 안내문이 제시한 공식 경로는 셋이다 — **Open API**, 화면 다운로드, 데이터 상품 구입.

**Open API(`openapi.krx.co.kr`)가 주 경로다.** 일별매매정보·종목기본정보·지수 시세를 준다.
하루 10,000회, 무료. **투자자별 거래실적·공매도·PER/PBR·지수 구성종목은 없다** —
`flows`와 N4·N7이 여기 걸린다.

**`flows`는 KIS Developers로 6/7이 대체된다** — 어댑터는 구현됐다(K-6f).
개인·외국인·기관 순매수, 공매도 거래량·거래대금, 외국인 보유주식수까지 옮겼고
**`공매도 잔고 수량` 하나만 갈 곳이 없다.** KRX만 만드는 데이터다.
→ 아래 "KIS flows 수집" · [`poc/flows_alternatives.md`](dev/20260731_raw_features/02_data_expansion_plan/poc/flows_alternatives.md)

### 백필 이벤트 (2026-08-22 기준)

Cronicle 일회성 이벤트는 **끝나면 삭제한다** — 남겨두면 다음 사람이 스케줄을 읽을 때
무엇이 정기 작업인지 흐려진다.

| 이벤트 | 상태 |
|---|---|
| `sdc_backfill_n1_marketcap` | **삭제 완료.** 7,060,600행 / 2014-06-02~2026-08-18 백필을 마쳤다 |
| `sdc_backfill_s1_remainder` | **등록되지 않음.** S-1 일회성 이벤트는 검증 뒤 삭제했다 |
| `sdc_kis_flows_trial` | **정기화.** 평일 19:00 `foreign_holding`만 수집 |
| `sdc_kis_flows_weekly` | **신규 정기.** 월요일 20:30 `investor`·`shorting` 수집 |
| `SDC Backfill DART Corp Profile` | 완료 (08-15). 삭제 대상 |
| `SDC Backfill S-1 DART Filings` | **삭제 완료.** 3,490/3,959 법인에 receipt가 있고, financials/XBRL은 3,093, share-info는 3,430이다. 나머지는 `no_data` 집합으로 분류했다 |
| `SDC Backfill N3 Universe Snapshots` | **중단된 채 방치.** 2019~2025 7년치가 비어 있다. **재개하지 않는다** — 아래 |
| `SDC Common Backfill 2015` | 완료 (07-04). 삭제 대상 |

**lock 도메인이 겹치지 않게 짰다.** N1-8은 `krx_marketdata`, S-1은 `opendart`, KIS flows는
`kis`다. 일일 잡은 lock을 **900초 기다린 뒤 실패**하므로, 백필이 프로덕션 슬롯을 물면
그날 수집이 통째로 빠진다.

#### S-1 백필 이벤트 정리 (2026-08-22)

S-1 일회성 이벤트 `emsuia7tg0e`는 historical filing 수집 검증을 마친 뒤 삭제했다.
prod에서 stale로 남은 financial/share-info run도 닫았고, 남은 원천 누락은
`collection_slice_state=no_data` 집합으로 분류했다. 따라서 S-1을 다시 돌리는 정기 이벤트는
두지 않는다. filing receipt의 연도별 no-data 사유를 남기는 ledger는 추적성 개선 항목으로
별도 관리한다.

#### N3 월말 스냅샷은 재개하지 않는다 (2026-08-19 판정)

백필이 세 번 다 중단돼 **2019~2025년 84개 월말이 비어 있다.** 그런데
[`04_w1_pit_universe.md`](dev/20260731_raw_features/02_data_expansion_plan/04_w1_pit_universe.md) §3.5가
이미 N3의 역할을 축소해뒀다 — 일별 PIT 유니버스는 N1 행이 맡는다. 조건이었던
**N3-PR1 전제 검증을 2026-08-19에 마쳤고 통과했다.** 상세는 그 문서 §3.6.

KRX 접근이 제한된 상황에서 7년치를 다시 긁을 이유가 없다. 감사·교차검증용으로
2014~2018 구간은 남아 있다.

### 일일 잡이 없는 raw 테이블 둘 (2026-08-19 확인)

`daily_market_cap`과 `dart_filing_receipt_raw`는 **일회성 백필로만 채워졌다.** Cronicle 이벤트
19개의 wrapper를 전부 대조해서 확인했다 — `prices-market-cap-backfill.sh`와
`dart-sync-filings.sh`는 어떤 정기 이벤트에도 걸려 있지 않다. 백필이 끝나는 순간부터
두 표는 그냥 멈춘다.

둘 다 피처의 입력이다. 시가총액은 규모·밸류 분모·유동성으로, 공시 접수는
`feat_filing_activity`(N5-7)로 들어간다.

**막는 것은 없다.** compute 게이트(`reports.freshness_violations`)는 `common_feature_series`만
본다. `daily_market_cap`을 보는 것은 `evaluate_staleness`인데, 이건
`ops freshness-report --fail-if-stale`에서만 돌고 어떤 Cronicle 이벤트에도 안 걸려 있다.
그리고 갭은 백필로 복구된다 — 12년치를 84분에 채웠다. **급한 것이 아니라 빠진 것이다.**

**공시 접수는 그냥 붙일 수 없다.** skip-if-present가 `(corp_code, year)` 단위라 현재 연도가
이미 "완료"로 잡혀 있어서, 그대로 돌리면 한 건도 안 가져온다. `--force`가 필요하고 그러면
하루 2,763콜이 S-1과 같은 할당량을 쓴다. **S-1이 끝난 뒤에 붙인다.**

#### 2026-08-28 재부팅 뒤 운영 점검

sj2-server는 17:42 무렵 다시 올라왔고 Postgres 18.3은 healthy다. collector는
KIS source별 cursor 수정과 series별 freshness 예산을 담은 `v0.11.4`까지 배포했다.
Cronicle에 `sdc_daily_market_cap`(평일 20:00)과 `sdc_daily_freshness`(매일 23:00)를
등록했고 둘 다 enabled다.

market cap은 08-20~27의 빠진 6거래일을 복구했다. 각 날짜의 ticker 수는 같은 날
`daily_ohlcv`와 모두 일치하며 최신일은 T+1 원천에 맞는 08-27이다.
N6는 Cronicle 일회성 event가 남아 있지 않지만 DB audit에는 08-22~23 run이 success로 닫혀 있다.
122,729슬라이스를 처리해 employee 92,163행과 governance 326,997행을 저장했고 오류는 0이다.

KIS daily `foreign_holding` event가 08-21~27에 전 종목을 `skipped_current`로 넘긴 원인은
KRX·KIS coverage를 합친 cursor였다. source별 cursor 수정은 `v0.11.3`으로 배포했다.
08-28 전 종목 2,767개를 다시 수집해 2,767행을 저장했고 audit run은 `success`다.
token 발급 1회와 종목 조회 2,767회 외에 재시도나 rate-limit은 없었다.

첫 market cap 복구 run을 터미널에서 중단하면서 audit run
`2163e76f-6dd0-4555-8765-19eae29fbe54` 하나가 `running`으로 남았다. 뒤이은 복구와
정기 event는 정상 완료됐지만, 이 감사 행을 `failed`로 닫는 DB 수정은 별도 승인을 받아야 한다.

과거 Cronicle job 저장 파일도 비밀정보 패턴으로 검사했다. `/home/whi/apps/cronicle/data/jobs`
아래 178개 파일에 userinfo가 포함된 PostgreSQL URI가 평문으로 남아 있다. 값은 운영 점검
출력이나 이 문서에 복사하지 않는다. 해당 DB credential은 노출된 것으로 보고 회전한 뒤,
과거 job 파일은 백업 정책을 확인해 삭제하거나 URI의 userinfo를 마스킹해야 한다. 둘 다 운영
변경이고 job history 삭제는 되돌리기 어려우므로 별도 승인 전에는 하지 않는다. 178개 모두
**현재 DB 비밀번호와 일치**한다. 기록 시각은 2026-04-12~06-27이다. 원인은 당시 connection
pool INFO 로그가 DSN 전체를 찍은 것이며, 현재 코드는 `_mask_dsn()`으로 userinfo를 가린다.
06-27 뒤 신규 평문 기록은 찾지 못했다. 따라서 재발 경로는 닫혔지만 기존 비밀번호 회전과
history 정리는 여전히 해야 한다.

#### KRX Open API 일별매매정보는 T+1이다

`sto/stk_bydd_trd`는 **당일치를 그날 안에 내놓지 않는다.** 문서를 읽은 게 아니라 실측이다.

| 시각 | 요청 날짜 | 결과 |
|---|---|---|
| 2026-08-19 00:43 | 2026-08-18 | 빈 응답 |
| 2026-08-19 19:31 | 2026-08-18 | 2,763행 |
| 2026-08-19 19:31 | 2026-08-19 | 빈 응답 |

둘이 따라 나온다.

- **일일 시가총액 잡은 T-1이 대상이다.** 갭 탐지가 기본이라 날짜를 안 넘기면 밀린 세션을
  알아서 채운다. 18:30 체인 꼬리에 붙여도 그날치가 아니라 전날치를 가져온다
- **`daily_market_cap`은 별도 2거래일 예산을 쓴다.** 일반 일별 도메인의
  `max_lag_trading_days=1`은 가장 최근 세션을 요구하므로 T+1 원천이 매일 실패한다. 그렇다고
  gate에서 빼면 장기 중단도 못 잡는다. `max_market_cap_lag_trading_days=2`로 전 거래일까지
  허용하고, 그보다 더 늦으면 실패시킨다.

`daily_ohlcv`(naver)는 당일 저녁에 들어온다. **둘의 최신 날짜는 정상 상태에서도 하루
어긋난다** — 어긋났다고 사고가 아니다.

### KIS 자격증명 (2026-08-16 발급)

`.env`에 있다. 시크릿 디렉터리가 아니다 — 거기는 접속 메타데이터(`db_info`·`cronicle_info`)용이고
**API 키는 항상 `.env`**다.

| 변수 | 용도 |
|---|---|
| `KIS_APP_KEY` / `KIS_APP_SECRET` | 앱키·시크릿 |
| `KIS_BASE_URL` | 실전 `openapi.koreainvestment.com:9443` · 모의 `openapivts…:29443` |
| `KIS_TIMEOUT_SECONDS` | 기본 20.0 |
| `KIS_TOKEN_CACHE_PATH` | 기본 `state/kis_token.json` · prod은 `/state/kis_token.json` |
| `KIS_REQUESTS_PER_SECOND` | 기본 **1.0** (아래 참조) |
| `KIS_TOKEN_REFRESH_MARGIN_SECONDS` | 기본 3600 |

**prod에 들어갔다 (2026-08-18).** `/home/whi/apps/sdc/.env`에 로컬과 같은 값을 넣었고,
`collector`가 `env_file: - .env`라 컨테이너로 그대로 들어간다. 08-19 18:30 전량 실행으로
동작을 확인했다.

> `collector` 서비스에는 `profiles: ["manual"]`이 걸려 있어 `docker compose pull`이
> **건너뛴다.** 이미지 갱신은 `pull-image.sh`가 따로 한다.

시세조회에는 **계좌번호가 필요 없다.** 앱키·시크릿·토큰만 쓴다(계좌번호는 주문·잔고용).

> **토큰 캐시는 호스트에 둔다 — 이미 반영했다.**
> access token은 유효기간 1일이고, 6시간 이내 재발급은 같은 값을 주지만
> **발급할 때마다 알림톡이 발송된다.** 수집기는 매번 `docker compose run --rm`으로
> 새 컨테이너를 띄우므로 컨테이너 안에 캐시하면 **매 실행이 재발급 + 알림톡**이 된다.
> `deploy/prod/compose.yaml`에 `./state:/state` 마운트와 `KIS_TOKEN_CACHE_PATH`를 넣었다.
> **`collector`에 볼륨이 붙은 유일한 이유가 이것이다.**

### 유량 제한 — 문서는 초당 20건인데 실측은 **초당 1건**이다

KIS 문서와 안내는 **실전 계좌 초당 20건**이라고 한다. 이 계정은 그렇지 않다.
2026-08-16 실측:

| 설정 rate | 성공 | 거부(EGW00201) | 실효 처리량 |
|---:|---:|---:|---:|
| 20 / 10 / 5 /s | 6~7 / 15 | 8~9 | 2.1~3.4/s |
| 1.5/s | 14 / 20 | 6 | 1.09/s |
| 1.2/s | 17 / 20 | 3 | 1.11/s |
| **1.0/s** | **20 / 20** | **0** | 1.05/s |

**어떤 rate로 올려도 실효 처리량은 1.1/s를 못 넘었다.** 높게 잡으면 성공이 재시도로
바뀔 뿐이라 기본값을 **1.0**으로 뒀다. 이게 운영 시간을 결정한다.

> **함정 하나 — 유량 초과가 HTTP 500으로 온다.** KIS는 `초당 거래건수를 초과하였습니다`
> (`EGW00201`)를 **429가 아니라 HTTP 500 + 본문**으로 준다. 상태 코드만 보면 전부
> 일반 서버 오류로 분류돼 **rate-limit 카운터가 0으로 남고 서킷 브레이커가 영영 안 걸린다.**
> 어댑터는 상태 코드보다 **본문 `msg_cd`를 먼저** 본다.

### KIS flows 수집 — `flows sync-kis`

```bash
krx-collector flows sync-kis --plan-only            # 요청 0건·토큰 발급 0건, 계획만 출력
krx-collector flows sync-kis                        # 최근 거래일까지 증분
krx-collector flows sync-kis --exclude-groups foreign_holding
```

**구조가 KRX와 정반대다.** KRX는 `(날짜×시장)→전종목`, KIS는 `(종목)→기간`이다.
그래서 실패 단위가 시장-일 공백이 아니라 **종목별 구멍**이고,
`ingestion_runs.params.failed_request_keys`에 **어느 종목이 빠졌는지** 그대로 남는다.

| 그룹 | 엔드포인트 | 한 호출이 주는 것 | 주기 |
|---|---|---|---|
| `investor` | `FHPTJ04160001` | **30 거래일** | 주 1회로 충분 |
| `shorting` | `FHPST04830000` | **100 거래일** | 주 1회로 충분 |
| `foreign_holding` | `FHKST01010100` | **현재값 1건** | **매일 돌려야 한다** |

**`foreign_holding`만 성격이 다르다.** `inquire-price`는 기준일이 없는 현재값이라
과거를 못 받는다. 그래서 어댑터는 **최신 거래일이 아닌 날짜로는 아예 수집하지 않는다** —
오늘 값을 과거 날짜로 적는 것이 안 받는 것보다 나쁘다. 놓친 날은 영구 손실이다.

**소요 시간은 초당 1건에서 나온다.** 2,763종목 기준:

| 작업 | 요청 수 | 소요 |
|---|---:|---:|
| `foreign_holding` 1일치 | 2,763 | **약 46분** |
| `investor` + `shorting` 1회 | 5,526 | **약 1.5시간** |
| 전체 1회 | 8,289 | **약 2.3시간** |

초당 20건 가정으로 잡았던 "4.6분"은 성립하지 않는다.
스케줄은 **`foreign_holding` 매일 · 나머지 주 1회**로 나누는 것이 맞다.

> **주기를 늘리면 `--lookback-days`도 같이 늘려야 한다.** 기본 10일은 약 7 세션이라
> 주 1회는 덮지만, **한 번 실패해서 2주가 벌어지면 창 밖의 날은 그냥 빠진다.**
> 실패를 견디려면 `주기 × 2` 이상으로 둔다. 호출당 30~100 세션이라 창을 넓혀도
> 요청 수는 거의 그대로다 — **겹치는 건 사실상 공짜고, 빠지는 건 아니다.**

**중복 실행 방지는 종목별 커서다.** 이미 최신 거래일까지 있고 창 안에 구멍이 없으면
그 종목·그룹은 요청하지 않는다. 재실행하면 요청 0건이다.
**KIS 커서는 `source=KIS` 행만 읽는다.** KRX와 KIS가 함께 도는 동안 두 source를 합치면
KRX가 먼저 채운 당일 행 때문에 KIS 작업이 완료로 잘못 잡힌다. 실제로 2026-08-21~27
평일 `foreign_holding` 잡이 전 종목을 `skipped_current`로 넘겨 KIS 최신일이 08-20에
멈췄다. 첫 전환 실행에서 KIS 이력이 비어 있으면 bounded lookback을 한 번 다시 받는다.

**자료 없음은 `no_data_request_keys`에 남고 7일간 재요청하지 않는다.** TTL이 있는 이유는
거래정지 종목이 다시 살아나기 때문이다. 영구 tombstone은 그걸 못 받는다.

**인증·유량 실패는 항목 오류가 아니라 run 실패다.** 계속 돌면 남은 종목 전부를
거부하는 서버에 던지게 되고, KIS는 인증 재시도마다 알림톡이 나간다.
유량 소진은 종료 코드 **75**(OpenDART 전례), 인증 실패는 **1**이다.

### KRX ↔ KIS 병행 대조 (1일차 2026-08-19)

전량 실행 8,289요청 / 2시간 18분 / **오류 0 · no_data 0** · 137,118행.

**커버리지는 통과다.** 6개 지표 전부 `(ticker)` 집합이 KRX와 **정확히 일치**했다 —
KRX에만 있는 종목 0, KIS에만 있는 종목 0.

**값은 지표마다 다르다.**

| 지표 | 불일치 | 성격 |
|---|---|---|
| 개인·외국인·기관 순매수 (3종) | **0건** | 완전 일치 |
| `short_selling_volume` · `_value` | 210 / 2,763 (7.6%) | **KIS가 항상 낮다.** 총량 기준 **-1.19%** |
| `foreign_holding_shares` | 477 / 2,763 (17.3%) | **양방향.** 대형주는 1% 안, 소형주에서 최대 5배 |

**공매도는 정의 차이로 확정했다.** `--lookback-days 14`가 11거래일을 같이 받아온 덕에
세션별 불일치 건수를 볼 수 있었는데 197~227로 **일정하다.** 시간이 지나도 안 줄어드니
정산 지연이 아니다. 210건이 전부 한 방향(KIS가 낮음)이므로 KIS가 어떤 부분집합을 빼고 준다.
삼성전자 1,688,352 → 1,586,828(-6%), KT -18%.
**교체하면 교체일에 계단이 생긴다** — 총량 -1.19% 규모다.

**외국인 보유는 아직 모른다.** 이 지표는 **스냅샷이라 최신 세션 하나만 쌓인다.**
공매도처럼 여러 세션으로 "일정한가"를 볼 수가 없어서, 지속적인 정의 차이인지
08-19만의 것인지 가릴 수 없었다. 증자·분할도 원인이 아니다 — 이상치 6종목 중 5종목이
7월 이후 상장주식수가 그대로였다.

| 종목 | KRX | KIS | 차이 |
|---|---|---|---|
| 센서뷰 (321370) | 163,299 | 838,365 | +413% |
| 세아메카닉스 (396300) | 586,015 | 141,461 | -76% |
| 대한항공 (003490) | 84,521,094 | 83,874,120 | -0.77% |

→ **2일차(08-20)를 이 질문에만 쓴다.** 세션이 둘이면 공매도처럼 갈라낼 수 있다.
그때까지 `SDC KRX Flows`는 켜 둔다.

### 병행 대조 결론 (2일차 2026-08-20)

2일차도 커버리지는 맞았다. KIS와 KRX의 종목 집합은 모든 공통 지표에서 같았고,
순매수 3종은 2일 연속 값이 전부 일치했다. 반면 정의가 다른 지표는 반복해서 갈렸다.

| 지표 | 08-19 불일치 | 08-20 불일치 | 판정 |
|---|---:|---:|---|
| 개인·외국인·기관 순매수 | 0 / 2,635 | 0 / 2,631 | KIS 값 채택 가능 |
| `short_selling_volume`·`_value` | 210 / 2,763 | 205 / 2,763 | KIS가 낮은 정의 차이 |
| `foreign_holding_shares` | 477 / 2,763 | 487 / 2,763 | 스냅샷 정의 차이, 교체일 계단 기록 |

따라서 KIS는 6개 지표의 정기 수집 경로로 확정한다. `short_selling_balance_quantity`는
KIS endpoint가 없으므로 KRX만 남긴다. KRX 전체 이벤트를 바로 끄면 이 지표가 멈추므로,
정기 이벤트를 바꿀 때는 KRX를 **잔고 지표 전용**으로 만들거나 해당 지표를 명시적으로
중단하는 후속 작업이 필요하다. 이번 전환에서는 KIS 정기 이벤트를 먼저 등록하고,
기존 KRX 체인은 잔고 경로를 정리할 때까지 유지한다.

### K-5(KRX 경로 폐기) 전에 처리해야 할 것 하나

**freshness의 `shorting` 그룹은 metric 3개의 최솟값이다.** 그 안에 KIS가 못 채우는
`short_selling_balance_quantity`가 들어 있다. KRX 수집을 끄면 이 metric만 날짜가
멈추고, **`krx_security_flow_raw:shorting`이 매일 stale로 뜬다.**

매일 걸리는 게이트는 아무도 안 본다 — O-8에서 이미 정한 원칙이다.
그래서 **K-5를 할 때 그룹 정의를 쪼개거나 이 metric을 "수집 중단"으로 명시해야 한다.**
지금은 KRX가 계속 돌고 있어 드러나지 않는다. **고쳐 둔 게 아니라 미뤄 둔 것이다.**

조사 결과와 진행 상태:
[`docs/dev/20260731_raw_features/02_data_expansion_plan/poc/krx_open_api.md`](dev/20260731_raw_features/02_data_expansion_plan/poc/krx_open_api.md) ·
[`10_work_breakdown.md` K 묶음](dev/20260731_raw_features/02_data_expansion_plan/10_work_breakdown.md)

### 차단됐을 때 운영자가 할 것

1. **재시도하지 않는다.** 차단 중 로그인을 두드리는 것이 탐지가 가장 민감하게 보는 행동이다.
   v0.9.10의 로그인 실패 쿨다운(60s→300s→900s→3600s)이 이걸 억제하지만, 수동 재실행은 그 밖이다
2. 해당 Cronicle 이벤트를 disable한다 — 청크 루프는 첫 실패에서 멈추지만(O-15) 스케줄은 다시 돈다
3. 1일 경과 후 자동 해제. **해제됐다고 같은 작업을 다시 돌리지 않는다** — 같은 결과가 나온다

---

## 트러블슈팅

| 증상 (Symptom) | 예상 원인 | 해결 방법 |
|---------|-------------|-----|
| 어떤 명령어를 쳤는데 `NotImplementedError`가 남 | 어댑터(Adapter) 코드가 아직 껍데기(Stub) 상태임 | TODO 주석을 참고하여 어댑터 구현을 완료하세요. |
| DB `Connection refused` 발생 | PostgreSQL이 꺼져있거나 DSN 정보가 틀림 | `.env` 파일의 DB 설정 확인 및 `pg_isready`로 DB 상태 점검 |
| KRX 접근 차단 (`blockError_01.jsp` / KDM 이용 제한 안내) | **약관 위반이다. 속도 문제가 아니다.** KRX Data Marketplace 약관 제10조 제2호가 자동화 수단에 의한 수집 자체를 금지한다 | **스로틀을 더 늦춰도 풀리지 않는다.** 아래 "KRX 접근 제한" 절 참고 |
| 검증 시 휴장일이 정상 거래일로 인식됨 | `docs/holidays_krx.csv` 파일이 비어있음 | CSV 파일에 KRX 휴장일 날짜를 추가 |
| 수집 중 `JSONDecodeError` 발생 | KRX가 JSON 대신 HTML을 준다 — 차단 페이지이거나 사이트 개편 | 응답 본문을 먼저 확인. 차단 페이지면 위 항목으로, 아니면 MDC client/parser를 최신 응답 형식에 맞게 수정 |
| `ingestion_runs.status = 'partial'` 발생 | 외부 API 일부 요청 실패, 타임아웃, 개별 종목 no-response | 같은 파라미터로 재실행하고 `error_summary`, `counts.error_count` 및 샘플 request key를 확인 |
| `flows sync`에서 KRX MDC timeout 반복 | KRX MDC 응답 정체 또는 차단 | `.env`의 `KRX_MDC_TIMEOUT_SECONDS` 또는 `flows sync --timeout-seconds`를 조정하고, 종목 수/기간을 줄여 재실행. 계속 실패하면 KRX 응답 상태를 점검 |
| `flows sync`가 `KrxMdcAuthenticationError` 또는 `LOGOUT` 메시지로 실패 | KRX MDC 세션 만료 또는 자격증명 누락 | `.env`에 `KRX_ID` / `KRX_PW`를 설정하면 client가 자동 로그인 후 재시도합니다. 자격증명이 이미 설정되어 있는데도 반복 실패한다면 KRX 계정 상태(중복 로그인/잠금)를 확인 |
| OpenDART raw/XBRL 단계가 부분 실패 | 일시적 OpenDART 응답 오류 | 동일 파라미터로 재실행. 공통 재시도 로직이 3회까지 복구를 시도하므로 반복 실패 종목만 선별 재처리 |
