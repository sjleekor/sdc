# krx-data-pipeline

유지보수 가능하며 운영 환경을 고려하여 설계된 한국 주식 데이터 파이프라인입니다.

1. [FinanceDataReader](https://github.com/financedata-org/FinanceDataReader) 및 [pykrx](https://github.com/sharebook-kr/pykrx)를 사용하여 **KOSPI / KOSDAQ 종목 유니버스를 동기화**합니다 (종목 마스터 관리).
2. pykrx를 사용하여 상장일로부터 **종목별 일봉(OHLCV) 이력 데이터를 수집**합니다.
3. [OpenDART](https://opendart.fss.or.kr)를 사용하여 **재무제표 / 주식수 / 배당 / 자사주 raw 값과 XBRL fact**를 수집합니다.
4. KRX MDC 소스를 직접 호출하여 **일자별 수급 raw**(투자자별 순매수, 공매도 등)를 수집합니다.
5. 공시 원문 기반 **섹터별 사업 KPI extractor 프레임워크**를 제공합니다 (파일럿: 조선/방산 수주).
6. raw 테이블과 별도로 `metric_catalog` / `metric_mapping_rule` / `stock_metric_fact` 기반 **canonical metric 정규화 계층**을 운영합니다.
7. 깔끔한 포트/어댑터(Ports & Adapters) 아키텍처를 적용하여 **PostgreSQL에 모든 데이터를 저장**합니다. 핵심 로직의 리팩토링 없이 향후 파일 기반 저장소(CSV / Parquet)로 확장할 수 있도록 설계되었습니다.

## 목표 제외 범위 (현재 스코프)

- **분봉/시간봉 (Intraday)** 수집은 현재 범위에서 제외됩니다. 확장 포인트는 설계되어 있으나 아직 구현되지 않았습니다.
- **Selenium**은 명시적으로 사용하지 않습니다.

## 빠른 시작 (Quickstart)

### 필수 조건 (Prerequisites)

- Python ≥ 3.12
- [uv](https://docs.astral.sh/uv/) 패키지 매니저
- PostgreSQL (실제 운영 환경용)

### 설정 (Setup)

```bash
# 1. 의존성 패키지 설치
uv sync

# 2. 환경 변수 설정
cp .env.example .env
# .env 파일을 열어 데이터베이스 계정 정보 및 설정을 수정하세요
# `dart` 계열 명령은 OPENDART_API_KEY 또는 OPENDART_API_KEYS가 반드시 설정되어야 동작합니다
# `metrics` 계열 명령은 별도 API 호출 없이 이미 적재된 raw 데이터를 정규화합니다

# 3. 데이터베이스 스키마 초기화
uv run krx-collector db init

# 4. 종목 유니버스 동기화
uv run krx-collector universe sync --source fdr --markets kospi,kosdaq

# 5. 일봉(OHLCV) 데이터 백필(수집) — 최초 1회: 전체 히스토리 수집
uv run krx-collector prices backfill --market all

# 5-1. 일봉 데이터 일일 증분 수집 — 두 번째 이후 실행: 마지막 저장일 이후만
uv run krx-collector prices backfill --market all --incremental

# 6. 데이터 정합성 검증 실행
uv run krx-collector validate --date 2025-01-15 --market all
```

### 계정 / 재무 / XBRL 파이프라인 (OpenDART)

OpenDART 명령은 `.env`의 `OPENDART_API_KEY` 단일 키 또는 `OPENDART_API_KEYS` 쉼표 구분 멀티 키를 사용합니다.
두 값을 모두 설정하면 중복을 제거한 뒤 `OPENDART_API_KEYS` 순서 다음에 `OPENDART_API_KEY`를 병합합니다.
멀티 키 실행 시 공통 executor가 키별 rate-limit, 일시 오류, 비활성 키 상태를 분류하고 가능한 경우 다른 키로 자동 전환합니다.
각 sync 명령은 동일 요청 결과가 이미 DB에 있으면 OpenDART를 다시 호출하지 않고 건너뜁니다.

```bash
# 7. OpenDART corp_code 마스터 동기화 및 ticker 매핑 검증
uv run krx-collector dart sync-corp

# 8. OpenDART 재무 raw 적재 (예: 삼성전자 2025 사업보고서 연결재무)
uv run krx-collector dart sync-financials --tickers 005930 --bsns-years 2025 --reprt-codes 11011 --fs-divs CFS

# 9. OpenDART 주식수 / 배당 / 자사주 raw 적재
uv run krx-collector dart sync-share-info --tickers 005930 --bsns-years 2025 --reprt-codes 11011

# 10. OpenDART XBRL ZIP 파싱 및 fact raw 적재
uv run krx-collector dart sync-xbrl --tickers 005930 --bsns-years 2025 --reprt-codes 11011
```

전체 사업연도 백필은 서버 checkout에서 host-side 스크립트로 실행합니다. 기본 범위는 2015년부터 전년도까지이며, 최신 연도부터 `sync-financials → sync-share-info → sync-xbrl → metrics normalize` 순서로 처리합니다. 모든 OpenDART key가 일일 한도에 도달하면 해당 CLI가 exit code `75`로 종료되고, 다음 실행 때 이미 저장된 raw는 건너뛰며 이어받습니다.

```bash
bin/dart-backfill-all-years.sh

SDC_DART_BACKFILL_START_YEAR=2018 \
SDC_DART_BACKFILL_END_YEAR=2025 \
bin/dart-backfill-all-years.sh
```

### Canonical metric 정규화

```bash
# 11. raw 테이블 → stock_metric_fact 정규화
uv run krx-collector metrics normalize --tickers 005930 --bsns-years 2025 --reprt-codes 11011

# 12. raw 대비 정규화 커버리지 리포트
uv run krx-collector metrics coverage-report --tickers 005930 --bsns-years 2025 --reprt-codes 11011
```

### 수급 raw (KRX)

```bash
# 13. 종목/일자 기준 수급 raw 적재
uv run krx-collector flows sync --tickers 005930 --start 2026-04-17 --end 2026-04-17
```

`flows sync`는 KRX MDC JSON endpoint를 직접 호출합니다. 적재 row의 `source` 컬럼은 `KRX`로 기록됩니다. KRX MDC가 비로그인 응답을 거부하면 `.env`의 `KRX_ID` / `KRX_PW` 자격증명으로 자동 로그인 후 재시도합니다.

### 사업 KPI 파일럿 (섹터별 extractor)

```bash
# 14. 문서 등록 + 섹터별 extractor로 operating_metric_fact 적재
uv run krx-collector operating process-document \
  --ticker 009540 \
  --market KOSPI \
  --sector-key shipbuilding_defense \
  --document-type manual_text \
  --title "조선 방산 수주 샘플" \
  --document-date 2026-04-19 \
  --period-end 2025-12-31 \
  --source-system LOCAL \
  --text-file tests/fixtures/operating/shipbuilding_defense_sample.txt
```

현재 `flows sync` 1차 구현은 다음 metric을 대상으로 합니다.

- `foreign_holding_shares`
- `foreign_net_buy_volume`
- `institution_net_buy_volume`
- `individual_net_buy_volume`
- `short_selling_volume`
- `short_selling_value`
- `short_selling_balance_quantity`

`borrow_balance_quantity`는 KRX MDC provider에 아직 안정 경로를 붙이지 않아 pending 상태입니다.

현재 `operating process-document` 파일럿은 `shipbuilding_defense` 섹터에 대해 다음 metric을 추출합니다.

- `order_intake_amount`
- `order_backlog_amount`

신규 파이프라인들은 외부 API 장애 시 자동 재시도/rate-limit/jitter를 수행하며, 최종적으로 일부 요청이 실패해도 파이프라인은 정상 종료됩니다. 이 경우 `ingestion_runs.status`가 `partial`로 기록되고 `counts.error_count` / `partial_failure_count` / `completed_request_count` 값이 함께 저장됩니다. OpenDART 실행은 추가로 `opendart_key_count`, `key_rotation_count`, `rate_limit_count`, `key_disable_count`, `all_rate_limited_count`, `all_disabled_count`, `request_invalid_count`, `retryable_error_count`, `terminal_error_count`, `status_<code>_count` 같은 키/상태코드 메트릭을 기록합니다. 해석/복구 절차는 [docs/operations.md](docs/operations.md)를 참고하세요.

> **중복 실행 방지**
> - `dart sync-corp`: `dart_corp_master`에 데이터가 있으면 corp-code ZIP 다운로드를 건너뜁니다.
> - `dart sync-financials`: `(corp_code, bsns_year, reprt_code, fs_div)` raw 행이 있으면 해당 재무제표 요청을 건너뜁니다.
> - `dart sync-share-info`: 주식수, 배당, 자사주 각각에 대해 해당 raw 행이 있으면 해당 요청만 건너뜁니다.
> - `dart sync-xbrl`: `(corp_code, bsns_year, reprt_code, rcept_no)` XBRL 문서가 있으면 ZIP 다운로드/파싱을 건너뜁니다.
> - `metrics normalize`: 이미 같은 `(ticker, metric_code, bsns_year, reprt_code)` canonical fact가 있으면 다시 쓰지 않습니다.
> - `metrics coverage-report`: read-only 리포트라 외부 다운로드와 DB 쓰기를 하지 않습니다.

> **백필 모드 요약**
> - **기본 모드** (gap detection): 거래일 캘린더 기준으로 누락된 모든 영업일을 찾아 채웁니다. 최초 백필이나 히스토리 보강에 적합합니다. 각 티커마다 `MIN(trade_date)`로 자동 클램핑되어 상장 이전(또는 pykrx가 제공하지 못하는 과거) 구간을 매번 재요청하지 않습니다.
> - **`--incremental` 모드**: 각 티커의 `MAX(trade_date)` 이후만 단일 연속 구간으로 수집합니다. gap 검출을 건너뛰므로 매일 돌리는 catch-up 작업에 가장 빠릅니다.

### `python -m`으로 실행하기

```bash
uv run python -m krx_collector universe sync --source pykrx
```

## 원격 DB를 로컬 PostgreSQL로 동기화하기

`sj2-server`에서 매일 수집한 데이터를 로컬 PostgreSQL로 가져와 로컬에서도 바로 분석할 수 있도록 `db sync-remote` 명령을 제공합니다.
로컬 PostgreSQL 접속 정보는 `.env`의 `DB_DSN` 또는 `DB_HOST`/`DB_PORT`/`DB_NAME`/`DB_USER`/`DB_PASSWORD`를 사용합니다.
원격 PostgreSQL 접속 정보는 기본적으로 `/Users/whishaw/wss_p/stock_data_collector_secrets/db_info`에서 읽습니다.

```bash
# 기본 증분 동기화
uv run krx-collector db sync-remote
```

원격 DB 호스트가 로컬에서 직접 열리지 않고 `sj2-server` SSH 접속을 통해서만 접근 가능하다면 SSH 터널 옵션을 함께 사용하세요.

```bash
# SSH 터널을 통한 증분 동기화
uv run krx-collector db sync-remote --ssh-host whi@sj2-server
```

### 동기화 모드

`db sync-remote`는 세 가지 모드를 지원합니다. 모든 모드는 SSH 터널 옵션(`--ssh-host`, `--ssh-local-port`)과 자유롭게 조합할 수 있습니다.

#### 1. 증분 동기화 (기본)

핵심 4개 테이블만 대상으로 새 행만 upsert 합니다.

- `stock_master`
- `stock_master_snapshot`
- `stock_master_snapshot_items`
- `daily_ohlcv`

각 테이블의 `updated_at` 또는 `fetched_at` 워터마크를 기준으로 동작하며, 동일 시각 행이 배치 경계에서 누락되지 않도록 `(timestamp, primary_key...)` 복합 커서를 사용합니다. `daily_ohlcv`는 `sync_checkpoints`에 재개 커서를 저장해 중단 지점에서 이어받습니다.

#### 2. `--full-refresh`

위 4개 테이블을 `TRUNCATE` 후 원격 데이터를 처음부터 다시 적재합니다. 로컬 복제본이 손상됐거나 첫 동기화일 때 사용합니다.

```bash
uv run krx-collector db sync-remote --ssh-host whi@sj2-server --full-refresh
```

#### 3. `--all-tables` (반드시 `--full-refresh`와 함께)

유니버스 동기화, 일봉 백필, OpenDART 계정/재무/XBRL 및 canonical metric 정규화가 쓰는 관리 대상 테이블만 원격에서 로컬로 통째 복제합니다. 대상 로컬 테이블은 비워지고 다시 적재되므로 파괴적 작업입니다. `--full-refresh`를 함께 지정하지 않으면 즉시 에러로 중단됩니다.

```bash
uv run krx-collector db sync-remote --ssh-host whi@sj2-server --full-refresh --all-tables
```

동기화 대상은 다음 테이블입니다.

- `stock_master`
- `stock_master_snapshot`
- `stock_master_snapshot_items`
- `daily_ohlcv`
- `dart_corp_master`
- `dart_financial_statement_raw`
- `dart_share_count_raw`
- `dart_shareholder_return_raw`
- `dart_xbrl_document`
- `dart_xbrl_fact_raw`
- `metric_catalog`
- `metric_mapping_rule`
- `stock_metric_fact`
- `ingestion_runs`

`--all-tables` 모드는 다음 순서로 동작합니다.

1. **대상 테이블 schema reset** — 위 대상 로컬 테이블만 drop 후 `sql/postgres_ddl.sql`을 다시 적용합니다. 대상 밖의 public 테이블은 삭제하지 않습니다.
2. **사전 검증** — 대상 테이블이 원격/로컬 양쪽에 모두 있는지와 컬럼 구성(이름·순서)이 일치하는지 확인합니다. 불일치가 있으면 truncate 전에 즉시 오류로 중단됩니다.
3. **위상 정렬 후 truncate** — 외래키 의존성을 따라 부모 → 자식 순으로 정렬한 뒤 `TRUNCATE ... RESTART IDENTITY`로 대상 테이블만 비웁니다. 순환 FK가 발견되면 오류로 중단됩니다.
4. **바이너리 COPY 스트리밍** — 각 테이블을 PostgreSQL `COPY ... (FORMAT BINARY)`로 OS 파이프를 통해 원격→로컬 스트리밍합니다. 큰 테이블도 메모리 스파이크 없이 처리됩니다.
5. **시퀀스 복제** — 각 테이블이 소유한 시퀀스의 `last_value`/`is_called`를 `setval`로 그대로 복제합니다.
6. **체크포인트 정렬** — 마지막으로 `daily_ohlcv` 증분 체크포인트를 로컬에 적재된 데이터 기준으로 다시 써서, 이후 증분 sync가 올바르게 재개되도록 보정합니다.

### 옵션

- `--full-refresh`: 기본 4개 테이블을 truncate 후 처음부터 적재합니다.
- `--all-tables`: 관리 대상 파이프라인 테이블만 full-refresh 방식으로 복제합니다 (`--full-refresh` 필수).
- `--db-info-path`: 원격 DB 정보 파일 경로를 변경합니다.
- `--batch-size`: 증분 동기화에서 한 번에 읽어올 행 수를 조절합니다 (`--all-tables` 모드에서는 사용되지 않습니다).
- `--remote-host`: `db_info`의 host 값을 다른 호스트명으로 덮어씁니다.
- `--ssh-host`: 지정 시 SSH 로컬 포트 포워딩을 통해 원격 DB에 접속합니다.
- `--ssh-local-port`: SSH 터널에 고정 로컬 포트를 사용합니다 (미지정 시 임의의 빈 포트).

## Docker로 실행하기

### 필수 조건

- Docker
- Docker Compose Plugin (`docker compose`)

### Docker 이미지 빌드

```bash
docker build -t ghcr.io/sjleekor/sdc:latest .
```

`main` 브랜치에 push 하면 GitHub Actions가 동일 이미지를 `ghcr.io/sjleekor/sdc`로 자동 build/push 하도록 설정되어 있습니다. workflow 파일은 [`.github/workflows/docker.yml`](.github/workflows/docker.yml)입니다.

### 단일 컨테이너로 실행

기존 PostgreSQL이 이미 떠 있다면 `.env`의 `DB_DSN` 또는 `DB_HOST`/`DB_PORT` 값을 맞춘 뒤 다음처럼 실행할 수 있습니다.

```bash
docker run --rm --env-file .env ghcr.io/sjleekor/sdc:latest db init
docker run --rm --env-file .env ghcr.io/sjleekor/sdc:latest universe sync --source fdr --markets kospi,kosdaq
docker run --rm --env-file .env ghcr.io/sjleekor/sdc:latest prices backfill --market all --incremental
```

### Docker Compose로 실행

이 저장소에는 PostgreSQL과 collector 실행을 위한 [`docker-compose.yml`](docker-compose.yml)이 포함되어 있습니다.

```bash
# 1. 환경 변수 파일 준비
cp .env.example .env
```

`docker-compose.yml`은 DB 컨테이너 내부 호스트명을 사용하므로, `.env`에서 `DB_DSN`을 비우거나 아래처럼 맞추는 것을 권장합니다.

```env
DB_DSN=
DB_HOST=db
DB_PORT=5432
DB_NAME=krx_data
DB_USER=krx_user
DB_PASSWORD=changeme
```

```bash
# 2. PostgreSQL 시작
docker compose up -d

# 3. 스키마 초기화
docker compose run --rm collector db init

# 4. 종목 유니버스 동기화
docker compose run --rm collector universe sync --source fdr --markets kospi,kosdaq

# 5. 일봉 증분 수집
docker compose run --rm collector prices backfill --market all --incremental

# 6. 검증
docker compose run --rm collector validate --market all
```

`collector` 서비스는 배치 실행용이므로 `docker compose up -d` 시에는 기본적으로 `db`만 상시 실행됩니다.

### 개발 환경 (Development)

```bash
# 개발용 의존성 패키지 포함하여 설치
uv sync --extra dev

# 테스트 실행
uv run pytest

# 코드 린트(Lint) 검사
uv run ruff check src/ tests/

# 코드 포맷팅
uv run black src/ tests/
```

## 프로젝트 구조

```text
krx-data-pipeline/
├── .env.example                      # 환경 변수 템플릿
├── Dockerfile                        # 컨테이너 이미지 정의
├── docker-compose.yml                # PostgreSQL + collector 구성
├── pyproject.toml / uv.lock          # 프로젝트 메타데이터 및 의존성 (uv)
├── sql/
│   └── postgres_ddl.sql              # 전체 스키마 DDL (OHLCV / DART / XBRL / 수급 / KPI)
├── docs/
│   ├── architecture.md               # 아키텍처 및 데이터 흐름 설명
│   ├── database.md                   # 데이터베이스 스키마 문서
│   ├── operations.md                 # 운영 가이드(Runbook), cron, partial run 해석
│   ├── holidays_krx.csv              # KRX 휴장일 데이터 (trading calendar에서 사용)
│   └── dev/                          # 설계/구현 계획 및 세부 구현 추적표
├── src/krx_collector/
│   ├── __main__.py                   # `python -m krx_collector` 진입점
│   ├── main.py                       # main() 어댑터 shim
│   ├── cli/app.py                    # argparse 기반 CLI (db/universe/prices/dart/metrics/flows/operating/validate)
│   ├── domain/                       # 순수 도메인 모델 및 Enum (Source, RunType, RunStatus 등)
│   ├── ports/                        # 프로토콜 인터페이스
│   │                                 #   universe, prices, storage, corp_codes,
│   │                                 #   financials, share_info, xbrl, flows,
│   │                                 #   operating_extractors
│   ├── adapters/                     # Provider 구현체
│   │                                 #   universe_fdr / universe_pykrx / prices_pykrx
│   │                                 #   opendart_common / opendart_corp / opendart_financials /
│   │                                 #   opendart_share_info / opendart_xbrl
│   │                                 #   flows_krx / operating_extractors
│   ├── service/                      # 유스케이스 오케스트레이션
│   │                                 #   sync_universe, backfill_daily, validate,
│   │                                 #   sync_dart_corp / sync_dart_financials /
│   │                                 #   sync_dart_share_info / sync_dart_xbrl,
│   │                                 #   normalize_metrics, report_metric_coverage,
│   │                                 #   sync_krx_flows, process_operating_document,
│   │                                 #   operating_registry, sync_local_db
│   ├── infra/
│   │   ├── calendar/                 # KRX 거래일 계산 유틸리티
│   │   ├── config/                   # pydantic-settings 기반 환경 설정
│   │   ├── db_postgres/              # PostgreSQL 연결, 저장소, 원격 동기화 구현
│   │   └── logging/                  # 구조화 로깅 설정
│   └── util/                         # pipeline.py(재시도/jitter/partial-run finalizer),
│                                     #   시간대(Asia/Seoul) 유틸리티
└── tests/
    ├── unit/                         # 파서 / 매핑 / 재시도 / pipeline util 단위 테스트
    ├── integration/                  # DB 연결, OHLCV end-to-end, 운영 KPI round-trip
    ├── helpers/                      # 테스트용 fake provider/executor 헬퍼
    └── fixtures/                     # 섹터별 KPI 샘플 문서 등 테스트 픽스처
```

## 아키텍처 및 추가 문서

- [docs/architecture.md](docs/architecture.md) — 포트/어댑터 설계, 전체 데이터 흐름.
- [docs/database.md](docs/database.md) — raw 테이블, canonical 테이블, 인덱스/제약 설명.
- [docs/operations.md](docs/operations.md) — cron 스케줄, `ingestion_runs.status` 해석(`running` / `success` / `partial` / `failed`), 실패 복구 절차.

## 라이선스

MIT
