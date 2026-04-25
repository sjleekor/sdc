# Implementation Plan: krx-data-pipeline (v01)

본 문서는 `krx-data-pipeline` 프로젝트의 초기 골격 생성을 마친 후, 실제 기능을 구현하기 위한 단계별 상세 계획을 담고 있습니다. 각 단계는 독립적으로 검증 가능하며 점진적으로 기능을 확장하는 순서로 구성되었습니다.

---

## Phase 1: Foundation & Infrastructure (기반 구축)
가장 먼저 다른 모듈들이 의존하는 공통 인프라와 유틸리티를 구현합니다.

1.  **DB 연결 및 세션 관리 (infra/db_postgres)**
    *   `psycopg2`를 이용한 연결 풀링 및 컨텍스트 매니저 구현.
    *   `Settings`에서 DSN 정보를 읽어와 안정적인 연결 보장.
2.  **시간 및 거래일 로직 (util/time, infra/calendar)**
    *   `Asia/Seoul` 타임존 처리 로직 완성.
    *   공휴일 제외 거래일 계산 로직 (Standard Library 기반 baseline) 구현.
3.  **로그 및 재시도 유틸리티 (infra/logging, util/retry)**
    *   구조화된 로깅(Plain/JSON) 활성화.
    *   네트워크 호출을 위한 지수 백오프(Exponential Backoff) 재시도 데코레이터/헬퍼 구현.

## Phase 2: Stock Universe Sync (마스터 데이터 동기화)
상장 종목 리스트를 관리하는 기능을 구현합니다.

1.  **FDR Universe Provider (adapters/universe_fdr)**
    *   `FinanceDataReader`를 사용하여 KOSPI/KOSDAQ 종목 리스트 호출 및 도메인 모델 변환.
2.  **pykrx Universe Provider (adapters/universe_pykrx)**
    *   `pykrx`를 사용하여 종목 리스트 및 종목명 호출 로직 구현.
3.  **Stock Master Storage (infra/db_postgres/repositories)**
    *   `stock_master` 및 `stock_master_snapshot` 테이블에 대한 Upsert 로직 구현.
4.  **Sync Universe Service (service/sync_universe)**
    *   두 Provider의 데이터를 비교하거나 선택하여 최종 스냅샷을 생성하고 DB에 저장하는 오케스트레이션 로직.
5.  **CLI 연동 (`universe sync`)**
    *   CLI 명령어를 서비스와 연결하고 실제 실행 결과 확인.

## Phase 3: Daily Price Collection (일봉 데이터 수집)
개별 종목의 과거 OHLCV 데이터를 수집하는 핵심 기능을 구현합니다.

1.  **pykrx Price Provider (adapters/prices_pykrx)**
    *   `pykrx`를 이용해 특정 기간의 종목별 일봉(OHLCV) 데이터 호출 로직 구현.
2.  **Price Storage (infra/db_postgres/repositories)**
    *   `daily_ohlcv` 테이블에 대한 대량(Bulk) Upsert 로직 구현.
3.  **Backfill Service (service/backfill_daily)**
    *   미수집 기간 계산, 수집 대상 종목 필터링, 속도 제한(Rate Limiting)을 고려한 루프 구현.
    *   `ingestion_runs` 테이블을 활용한 작업 상태 기록 및 체크포인트 설계.
4.  **CLI 연동 (`prices backfill`)**
    *   다양한 옵션(`--market`, `--tickers`, `--since-listing`)이 정상 작동하도록 연동.

## Phase 4: Validation & Stability (검증 및 안정화)
데이터 정합성을 체크하고 시스템의 안정성을 높입니다.

1.  **Data Validation Service (service/validate)**
    *   누락된 거래일 체크, 시가/고가/저가/종가 정합성 확인 로직 구현.
2.  **오류 처리 고도화**
    *   네트워크 오류, 데이터 유효성 오류 발생 시나리오별 예외 처리 및 로깅 강화.
3.  **CLI 연동 (`validate`)**
    *   수집된 데이터의 상태를 리포팅하는 기능 완성.

## Phase 5: Finalization (마무리)
전체 파이프라인을 점검하고 문서를 최신화합니다.

1.  **통합 테스트 작성**
    *   DB와 실제 라이브러리를 사용한 (Mocking 포함) End-to-End 테스트 수행.
2.  **성능 최적화**
    *   DB 인덱스 검토 및 대량 데이터 입력 시 성능 병목 지점 개선.
3.  **운영 가이드 완성**
    *   `docs/operations.md`에 구체적인 크론탭 설정 및 트러블슈팅 가이드 추가.

---

## 구현 순서 요약
1.  **Foundation** (DB, Time, Logging)
2.  **Universe Sync** (FDR/pykrx Adapter -> DB Repo -> Service -> CLI)
3.  **Daily Prices** (pykrx Adapter -> DB Repo -> Service -> CLI)
4.  **Validation** (Logic -> CLI)
5.  **Integration Testing & Ops Docs**
