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

# 5. 일봉(OHLCV) 데이터 백필(수집)
uv run krx-collector prices backfill --market all

# 6. 데이터 정합성 검증 실행
uv run krx-collector validate --date 2025-01-15 --market all
```

### `python -m`으로 실행하기

```bash
uv run python -m krx_collector universe sync --source pykrx
```

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
