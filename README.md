# krx-data-pipeline

유지보수 가능하며 운영 환경을 고려하여 설계된 한국 주식 데이터 파이프라인입니다.

1. [FinanceDataReader](https://github.com/financedata-org/FinanceDataReader) 및 [pykrx](https://github.com/sharebook-kr/pykrx)를 사용하여 **KOSPI / KOSDAQ 종목 유니버스를 동기화**합니다 (종목 마스터 관리).
2. pykrx를 사용하여 상장일로부터 **종목별 일봉(OHLCV) 이력 데이터를 수집**합니다.
3. 깔끔한 포트/어댑터(Ports & Adapters) 아키텍처를 적용하여 **PostgreSQL에 모든 데이터를 저장**합니다. 핵심 로직의 리팩토링 없이 향후 파일 기반 저장소(CSV / Parquet)로 확장할 수 있도록 설계되었습니다.

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

> **백필 모드 요약**
> - **기본 모드** (gap detection): 거래일 캘린더 기준으로 누락된 모든 영업일을 찾아 채웁니다. 최초 백필이나 히스토리 보강에 적합합니다. 각 티커마다 `MIN(trade_date)`로 자동 클램핑되어 상장 이전(또는 pykrx가 제공하지 못하는 과거) 구간을 매번 재요청하지 않습니다.
> - **`--incremental` 모드**: 각 티커의 `MAX(trade_date)` 이후만 단일 연속 구간으로 수집합니다. gap 검출을 건너뛰므로 매일 돌리는 catch-up 작업에 가장 빠릅니다.

### `python -m`으로 실행하기

```bash
uv run python -m krx_collector universe sync --source pykrx
```

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
├── .env.example                  # 환경 변수 템플릿
├── pyproject.toml                # 프로젝트 메타데이터 및 의존성 (uv)
├── sql/
│   └── postgres_ddl.sql          # 데이터베이스 스키마 DDL
├── docs/
│   ├── architecture.md           # 아키텍처 및 데이터 흐름 설명
│   ├── database.md               # 데이터베이스 스키마 문서
│   └── operations.md             # 운영 가이드(Runbook) 및 cron 스케줄 예시
├── src/krx_collector/
│   ├── cli/app.py                # 하위 명령어를 포함하는 argparse CLI 진입점
│   ├── domain/                   # 외부 의존성이 없는 순수 도메인 모델 및 Enum
│   ├── ports/                    # 프로토콜 인터페이스 (universe, prices, storage)
│   ├── adapters/                 # 실제 데이터를 가져오거나 저장하는 구현체 (Providers)
│   ├── service/                  # 유스케이스(Use-case) 오케스트레이션
│   ├── infra/                    # 설정(Config), 로깅, 캘린더, 데이터베이스 인프라
│   └── util/                     # 재시도(Retry), 시간대(Timezone) 유틸리티
└── tests/
    ├── unit/
    └── integration/
```

## 아키텍처

전체 데이터 흐름도와 포트/어댑터 설계 이유에 대한 상세 내용은 [docs/architecture.md](docs/architecture.md) 문서를 참고하세요.

## 라이선스

MIT
