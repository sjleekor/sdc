# 운영 가이드

## 일일 스케줄

KRX 정규장 시간: 09:00–15:30 KST. 당일의 온전한 데이터를 확보하기 위해 파이프라인은 반드시 **장이 마감된 후**에 실행해야 합니다.

### 권장 크론탭(cron) 스케줄 (KST 기준)

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

# 데이터 정합성 검증 — 매일 17:00 KST (평일)
  0  17  *  *  1-5  cd /opt/krx-data-pipeline && uv run krx-collector validate --market all
```

> **Tip:** crontab 맨 위에 `TZ=Asia/Seoul`을 설정하거나, systemd timer의 `OnCalendar=`를 사용하여 UTC 혼동을 방지하는 것이 좋습니다.

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
| **기본** | `--start` (또는 2000-01-01), 각 티커의 `MIN(trade_date)`로 자동 클램핑 | 거래일 캘린더 기준 누락된 모든 영업일을 구간으로 묶어 fetch | 최초 백필, 히스토리 보강, 중간 구멍(holes) 메우기 |
| **`--incremental`** | 각 티커의 `MAX(trade_date) + 1` (또는 `--start` 중 더 늦은 날) | 시작일 ~ `--end`까지 단일 연속 구간 | 매일 돌리는 catch-up cron |

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

> **메모**: 두 모드 모두 주말·공휴일은 `query_missing_days` / 단일 구간 fetch 단계에서 자연스럽게 배제됩니다. 또한 기본 모드에서는 `MIN(trade_date)` 클램프 덕분에 005930처럼 pykrx가 제공하지 못하는 과거 구간(예: 2014-01-20 이전)을 매 실행마다 헛스캔하지 않습니다.

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

수행되는 검증 항목:
1. **OHLC 정합성**: 저가 ≤ 시가 ≤ 고가, 저가 ≤ 종가 ≤ 고가, 가격 > 0 체크.
2. **누락된 거래일**: 거래소 휴장일(공휴일+주말)을 제외한 정상 거래일에 누락된 데이터가 있는지 확인.
3. **유니버스 카운트 변동**: 이전 스냅샷 대비 종목 수가 5% 이상 변동했는지 확인. (구현 예정)

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
```

재무 metric 정규화와 common daily fact 생성은 PostgreSQL CLI가 아니라 아래 "Parquet compute 파이프라인"에서 DuckDB 마트로 실행합니다.

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

필요하면 Cronicle 이벤트 환경 변수로 범위를 좁힙니다.

```bash
SDC_DART_BACKFILL_START_YEAR=2018
SDC_DART_BACKFILL_END_YEAR=2025
SDC_DART_BACKFILL_INCLUDE_CURRENT_YEAR=0
SDC_DART_BACKFILL_REPRT_CODES=11011,11012,11013,11014
SDC_DART_BACKFILL_FS_DIVS=CFS,OFS
```

모든 OpenDART API key가 일일 한도에 도달하면 각 OpenDART CLI는 exit code `75`로 종료됩니다. 스크립트는 `set -euo pipefail`이므로 그 지점에서 멈추고, 다음 실행 때 이미 저장된 raw/XBRL은 skip되어 같은 범위를 이어받습니다.

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

### 단계 (각 단계는 게이트)

1. `db sync-remote` — sj2 raw + `common_feature_series`를 로컬 mydb로 미러.
2. `bin/raw-parquet-export-all.sh` — mydb → `data_lake/raw_postgres/<snapshot>/...` parquet.
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

## 트러블슈팅

| 증상 (Symptom) | 예상 원인 | 해결 방법 |
|---------|-------------|-----|
| 어떤 명령어를 쳤는데 `NotImplementedError`가 남 | 어댑터(Adapter) 코드가 아직 껍데기(Stub) 상태임 | TODO 주석을 참고하여 어댑터 구현을 완료하세요. |
| DB `Connection refused` 발생 | PostgreSQL이 꺼져있거나 DSN 정보가 틀림 | `.env` 파일의 DB 설정 확인 및 `pg_isready`로 DB 상태 점검 |
| KRX 접근 차단 (Rate-limited) | 너무 빠른 속도로 많은 요청을 보냄 | `.env`에서 `RATE_LIMIT_SECONDS` 값을 더 높게 설정 |
| 검증 시 휴장일이 정상 거래일로 인식됨 | `docs/holidays_krx.csv` 파일이 비어있음 | CSV 파일에 KRX 휴장일 날짜를 추가 |
| 수집 중 `JSONDecodeError` 발생 | KRX 웹사이트가 개편되었거나 IP가 차단됨 | 프록시를 사용하거나 KRX MDC client/parser를 최신 응답 형식에 맞게 수정 |
| `ingestion_runs.status = 'partial'` 발생 | 외부 API 일부 요청 실패, 타임아웃, 개별 종목 no-response | 같은 파라미터로 재실행하고 `error_summary`, `counts.error_count` 및 샘플 request key를 확인 |
| `flows sync`에서 KRX MDC timeout 반복 | KRX MDC 응답 정체 또는 차단 | `.env`의 `KRX_MDC_TIMEOUT_SECONDS` 또는 `flows sync --timeout-seconds`를 조정하고, 종목 수/기간을 줄여 재실행. 계속 실패하면 KRX 응답 상태를 점검 |
| `flows sync`가 `KrxMdcAuthenticationError` 또는 `LOGOUT` 메시지로 실패 | KRX MDC 세션 만료 또는 자격증명 누락 | `.env`에 `KRX_ID` / `KRX_PW`를 설정하면 client가 자동 로그인 후 재시도합니다. 자격증명이 이미 설정되어 있는데도 반복 실패한다면 KRX 계정 상태(중복 로그인/잠금)를 확인 |
| OpenDART raw/XBRL 단계가 부분 실패 | 일시적 OpenDART 응답 오류 | 동일 파라미터로 재실행. 공통 재시도 로직이 3회까지 복구를 시도하므로 반복 실패 종목만 선별 재처리 |
