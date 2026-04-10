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
  0  16  *  *  1-5  cd /opt/krx-data-pipeline && krx-collector universe sync --source fdr

# 일봉 OHLCV 수집 — 매일 16:30 KST (평일)
  30 16  *  *  1-5  cd /opt/krx-data-pipeline && krx-collector prices backfill --market all --since-listing

# 데이터 정합성 검증 — 매일 17:00 KST (평일)
  0  17  *  *  1-5  cd /opt/krx-data-pipeline && krx-collector validate --market all
```

> **Tip:** crontab 맨 위에 `TZ=Asia/Seoul`을 설정하거나, systemd timer의 `OnCalendar=`를 사용하여 UTC 혼동을 방지하는 것이 좋습니다.

## 런북 (Runbook)

### 백필(Backfill) 재실행하기

백필 작업은 `ON CONFLICT … DO UPDATE` 덕분에 **멱등성(Idempotent)**을 가집니다.
동일한 파라미터로 다시 실행해도 데이터가 중복 생성되지 않으며, 최신 데이터로 기존 행을 덮어씁니다.

```bash
# 특정 종목의 특정 기간 다시 백필하기
krx-collector prices backfill --tickers 005930 --start 2024-01-01 --end 2024-12-31

# 특정 시장의 모든 종목 처음부터 다시 백필하기
krx-collector prices backfill --market kospi --since-listing
```

### 종목 유니버스 전체 갱신 (Full Refresh)

`stock_master` 데이터가 꼬였거나 완전히 새로 덮어쓰고 싶을 때 사용합니다:

```bash
krx-collector universe sync --source fdr --full-refresh
```

증분 비교(Diff)를 계산하지 않고 기존 데이터를 모두 새 데이터로 교체합니다.

### 데이터 품질 검증 (Validation)

```bash
# 특정 날짜 검증하기
krx-collector validate --date 2024-06-15 --market all

# 오늘 날짜 검증하기 (기본값)
krx-collector validate
```

수행되는 검증 항목:
1. **OHLC 정합성**: 저가 ≤ 시가 ≤ 고가, 저가 ≤ 종가 ≤ 고가, 가격 > 0 체크.
2. **누락된 거래일**: 거래소 휴장일(공휴일+주말)을 제외한 정상 거래일에 누락된 데이터가 있는지 확인.
3. **유니버스 카운트 변동**: 이전 스냅샷 대비 종목 수가 5% 이상 변동했는지 확인. (구현 예정)

### 데이터베이스 초기화

```bash
# 테이블 생성 (멱등성 보장 — CREATE TABLE IF NOT EXISTS 사용)
krx-collector db init
```

### 데이터 수집 이력 조회

```sql
-- 최근 10번의 실행 이력 확인
SELECT run_id, run_type, started_at, ended_at, status, counts
FROM ingestion_runs
ORDER BY started_at DESC
LIMIT 10;

-- 실패한 실행 이력 확인
SELECT * FROM ingestion_runs WHERE status = 'failed' ORDER BY started_at DESC;
```

## 모니터링

### 추적해야 할 주요 지표

- 일별 `ingestion_runs.status = 'failed'` 발생 건수.
- `stock_master` 전체 행 개수 (평소 대비 ± 5% 내로 안정적인지 확인).
- `daily_ohlcv` 일별 데이터 증가량 (거래일 기준 매일 약 2,500건 내외의 새로운 행이 추가되어야 함).
- 백필에 소요된 시간.

### 알림(Alerting) 권장 사항

- `ingestion_runs` 테이블에 `status = 'failed'`가 기록되면 즉시 알림.
- 유니버스 동기화 시 수집된 종목 수(`record_count`)가 평소 대비 10% 이상 감소하면 알림.
- 영업일(주말, 공휴일 아님)인데 `daily_ohlcv`에 새로운 행이 전혀 없다면 알림.

## 트러블슈팅

| 증상 (Symptom) | 예상 원인 | 해결 방법 |
|---------|-------------|-----|
| 어떤 명령어를 쳤는데 `NotImplementedError`가 남 | 어댑터(Adapter) 코드가 아직 껍데기(Stub) 상태임 | TODO 주석을 참고하여 어댑터 구현을 완료하세요. |
| DB `Connection refused` 발생 | PostgreSQL이 꺼져있거나 DSN 정보가 틀림 | `.env` 파일의 DB 설정 확인 및 `pg_isready`로 DB 상태 점검 |
| KRX 접근 차단 (Rate-limited) | 너무 빠른 속도로 많은 요청을 보냄 | `.env`에서 `RATE_LIMIT_SECONDS` 값을 더 높게 설정 |
| 검증 시 휴장일이 정상 거래일로 인식됨 | `docs/holidays_krx.csv` 파일이 비어있음 | CSV 파일에 KRX 휴장일 날짜를 추가 |
| 수집 중 `JSONDecodeError` 발생 | KRX 웹사이트가 개편되었거나 IP가 차단됨 | 프록시를 사용하거나 `pykrx`, `FinanceDataReader` 라이브러리의 최신 패치가 올라올 때까지 대기 |