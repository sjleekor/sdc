# Implementation Plan: Phase 1 - Foundation & Infrastructure (v01.phase1)

본 문서는 `krx-data-pipeline` 프로젝트의 `Phase 1`을 실행하기 위한 매우 구체적이고 단계적인 지침을 담고 있습니다. 각 작업은 독립적으로 구현 및 테스트가 가능해야 하며, 결과물은 이후 Phase의 기반이 됩니다.

---

## 1. DB 연결 및 세션 관리 (infra/db_postgres)

**목표:** `psycopg2` 연결 풀을 구현하여 안정적인 데이터베이스 통신 환경을 구축합니다.

### 1.1. `connection.py` 구현 상세
- **파일 위치:** `src/krx_collector/infra/db_postgres/connection.py`
- **구현 내용:**
    1.  `psycopg2.pool.ThreadedConnectionPool`을 사용하여 전역 연결 풀(`_POOL`)을 관리합니다.
    2.  `get_connection(dsn: str)` 함수는 다음 로직을 따라야 합니다:
        *   최초 호출 시 `_POOL`이 `None`이면 `ThreadedConnectionPool`을 생성하여 초기화합니다 (minconn=1, maxconn=10 권장).
        *   풀에서 연결(`conn`)을 가져옵니다 (`getconn()`).
        *   `yield conn`을 통해 컨텍스트를 제공합니다.
        *   성공 시 `conn.commit()`을 호출합니다.
        *   예외 발생 시 `conn.rollback()`을 호출합니다.
        *   `finally` 블록에서 `_POOL.putconn(conn)`을 호출하여 연결을 풀에 반환합니다.
- **주의 사항:** `psycopg2-binary`를 사용하여 추가 라이브러리 설치 없이 실행 가능해야 합니다.

### 1.2. 통합 테스트 수행
- **파일 생성:** `tests/integration/test_db_connection.py`
- **테스트 코드:**
    ```python
    from krx_collector.infra.db_postgres.connection import get_connection
    from krx_collector.infra.config.settings import get_settings

    def test_database_connection():
        settings = get_settings()
        with get_connection(settings.db_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                result = cur.fetchone()
                assert result[0] == 1
    ```
- **실행:** `pytest tests/integration/test_db_connection.py`

---

## 2. 시간 및 거래일 로직 (util/time, infra/calendar)

**목표:** 한국 거래소(KRX)의 거래일 계산 기준을 확립합니다.

### 2.1. 공휴일 마스터 데이터 생성
- **파일 생성:** `docs/holidays_krx.csv`
- **내용:** 최소 2024년과 2025년의 주요 공휴일을 포함합니다.
    ```csv
    date,name
    2024-01-01,New Year's Day
    2024-02-09,Lunar New Year
    2024-02-12,Lunar New Year (Substitute)
    2024-03-01,Samiljeol
    2024-04-10,General Election
    2024-05-01,Labor Day
    2024-05-06,Children's Day (Substitute)
    2024-05-15,Buddha's Birthday
    2024-06-06,Memorial Day
    2024-08-15,Liberation Day
    2024-09-16,Chuseok
    2024-09-17,Chuseok
    2024-09-18,Chuseok
    2024-10-03,National Foundation Day
    2024-10-09,Hangeul Day
    2024-12-25,Christmas Day
    2024-12-31,Market Closing Day
    ```

### 2.2. 거래일 계산 로직 검증
- **파일 생성:** `tests/unit/test_calendar.py`
- **테스트 케이스:**
    *   `get_trading_days(date(2024,1,1), date(2024,1,5))` 호출 시 1월 1일(신정)이 제외된 1월 2일~5일이 반환되는지 확인.
    *   주말(토/일)이 결과 리스트에서 제외되는지 확인.
    *   공휴일 파일이 없을 때 경고 로그가 남고 주말만 제외하는지 확인.

---

## 3. 로그 및 재시도 유틸리티 (infra/logging, util/retry)

**목표:** 시스템 전반에 구조화된 로깅을 적용하고, 네트워크 호출 실패에 대비한 재시도 메커니즘을 검증합니다.

### 3.1. 로깅 초기화 적용
- **파일 수정:** `src/krx_collector/__main__.py` (또는 실제 진입점)
- **변경 사항:**
    ```python
    from krx_collector.infra.config.settings import get_settings
    from krx_collector.infra.logging.setup import setup_logging

    def main():
        settings = get_settings()
        setup_logging(
            level=settings.log_level,
            fmt=settings.log_format.value,
            log_dir=settings.log_dir
        )
        # ... 이후 로직
    ```
- **검증:** `.env`에서 `LOG_FORMAT=json` 설정 후 실행 시 로그가 JSON 형태로 출력되는지 확인.

### 3.2. 재시도 로직 단위 테스트
- **파일 생성:** `tests/unit/test_retry.py`
- **테스트 코드:**
    ```python
    import pytest
    from krx_collector.util.retry import retry

    def test_retry_success_after_failure():
        calls = 0
        @retry(max_attempts=3, base_delay=0.1)
        def flaky_func():
            nonlocal calls
            calls += 1
            if calls < 2:
                raise ValueError("Fail")
            return "Success"

        assert flaky_func() == "Success"
        assert calls == 2

    def test_retry_exhaustion():
        @retry(max_attempts=2, base_delay=0.1)
        def failing_func():
            raise ValueError("Permanent Fail")

        with pytest.raises(ValueError):
            failing_func()
    ```

---

## Phase 1 완료 기준 (Definition of Done)
1.  `pytest tests/integration/test_db_connection.py` 통과 (DB 연결 가능 시).
2.  `pytest tests/unit/test_calendar.py` 통과.
3.  `pytest tests/unit/test_retry.py` 통과.
4.  애플리케이션 실행 시 지정된 `LOG_FORMAT`에 맞춰 로그가 출력됨.
