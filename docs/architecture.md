# 아키텍처

## 개요

KRX 데이터 파이프라인은 **포트 & 어댑터**(헥사고날) 아키텍처를 따릅니다.
도메인 로직은 인프라와 분리되어 있어, 핵심 비즈니스 규칙을 수정하지 않고도 데이터 소스나 저장소를 쉽게 교체할 수 있습니다.

## 데이터 흐름

```
┌─────────────────────────────────────────────────────────────────┐
│                          CLI (argparse)                         │
│  krx-collector universe sync / prices backfill / validate       │
│  (phase 0 adds config + schema scaffold for account/flow data)  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      서비스 계층 (Use-cases)                      │
│  sync_universe() │ backfill_daily_prices() │ validate()         │
│  future: dart sync / flow sync / metric normalize              │
└────────┬─────────────────────┬─────────────────────┬────────────┘
         │                     │                     │
         ▼                     ▼                     ▼
┌─────────────────┐  ┌─────────────────┐   ┌─────────────────────┐
│  포트 (Ports)    │  │  포트 (Ports)    │   │  포트 (Ports)         │
│  UniverseProvider│  │  PriceProvider   │   │  Storage            │
│  (Protocol)      │  │  (Protocol)      │   │  (Protocol)         │
└────────┬─────────┘  └────────┬─────────┘   └──────────┬──────────┘
         │                     │                        │
         ▼                     ▼                        ▼
┌─────────────────────────────────────────┐   ┌─────────────────────┐
│           어댑터 (Adapters)               │   │  인프라 / DB          │
│  FdrUniverseProvider                    │   │  PostgresStorage    │
│  PykrxUniverseProvider                  │   │  (future: FileStore)│
│  PykrxDailyPriceProvider                │   │                     │
│  future: OpenDART / KRX adapters        │   │  + raw account tables│
└─────────────────────────────────────────┘   └─────────────────────┘
         │                     │                        │
         ▼                     ▼                        ▼
   FinanceDataReader       pykrx API               PostgreSQL
   OpenDART / KRX (future)
```

## 포트 & 어댑터 설계 이유

### ABC 대신 Protocol을 사용하는 이유?

- **구조적 타이핑 (Structural typing)**: 어댑터는 기본 클래스를 상속받을 필요가 없습니다. 알맞은 메서드 시그니처를 가진 클래스라면 자동으로 프로토콜을 만족하므로 Mock/Fake 객체를 활용한 테스트가 더 쉬워집니다.
- **런타임 임포트 결합 제거**: 도메인과 서비스 계층은 절대 어댑터 코드를 임포트하지 않습니다. 의존성 주입은 CLI / Composition Root에서 이루어집니다.

### Universe와 Price 포트를 분리한 이유?

- **단일 책임 원칙 (Single Responsibility)**: 종목 목록(Universe) 수집과 개별 종목의 일봉 데이터(Price) 수집은 서로 다른 Rate-limiting, 에러 처리, 캐싱 전략이 필요한 근본적으로 다른 작업입니다.
- **소스 유연성**: Universe는 FDR이나 pykrx에서 가져올 수 있지만, Price는 현재 pykrx에서만 가져옵니다. 두 포트를 분리하면 결합도를 낮출 수 있습니다.

### Storage 추상화

`Storage` 프로토콜은 다음과 같이 설계되었습니다:

1. **PostgreSQL**이 주력 백엔드입니다 (`PostgresStorage` 사용).
2. 추후 **파일 기반 백엔드** (CSV / Parquet 저장)도 동일한 프로토콜을 구현하여 서비스나 도메인 변경 없이 CLI 계층에서 의존성 주입을 통해 교체할 수 있습니다.

Phase 0에서는 아직 신규 저장소 메서드를 도입하지 않고, 향후 OpenDART/수급 적재에 필요한 raw 테이블만 DDL에 추가했습니다. 즉, 현재 구현은 그대로 유지하면서 이후 phase에서 포트와 저장소 책임을 점진적으로 늘리는 전략입니다.

## 도메인 계층

프레임워크 의존성이 없는 순수 Python 데이터 클래스(Dataclass)입니다:

- `Stock`, `DailyBar`, `StockUniverseSnapshot` — 불변 값 객체.
- `IngestionRun` — 가변 감사(Audit) 기록.
- `UpsertResult`, `SyncResult`, `BackfillResult` — 작업 결과.
- Enums: `Market`, `Source`, `ListingStatus`, `RunType`, `RunStatus`.

## 설정 (Configuration)

- `pydantic-settings`가 `.env` 및 환경 변수에서 설정을 불러옵니다.
- `OPENDART_API_KEY`가 Phase 0에서 추가되었으며, 향후 재무/주주환원 수집 시 사용됩니다.
- 시간대는 `Asia/Seoul`로 고정되어 있습니다 (설정 변경 불가).
- 설정은 `get_settings()`를 통해 싱글톤으로 캐싱됩니다.

## Phase 0 스캐폴드

계정/재무/수급 확장을 위한 Phase 0에서는 아래 기반만 먼저 반영합니다.

1. 설정 추가: `OPENDART_API_KEY`
2. 공용 enum 확장: `Source.OPENDART`, `Source.KRX`, 신규 `RunType`
3. DB 스키마 초안 추가
   - `dart_corp_master`
   - `dart_financial_statement_raw`
   - `dart_share_count_raw`
   - `dart_shareholder_return_raw`
   - `krx_security_flow_raw`
4. 문서 및 테스트 보강

중요한 점은, Phase 0은 "실제 데이터 수집 로직"이 아니라 이후 phase가 안전하게 올라갈 수 있는 공용 기반을 만드는 단계라는 점입니다.

## 백필 스킵 전략

`backfill_daily_prices`는 두 가지 스킵 모드를 지원합니다.

1. **기본 (gap detection)** — `Storage.query_missing_days(ticker, start, end)`가 거래일 캘린더(`infra/calendar/trading_days.py`)에서 예상 영업일 집합을 만든 뒤, `daily_ohlcv`에 이미 저장된 `trade_date` 집합을 빼서 누락 일자를 반환합니다. 이렇게 얻은 missing 일자들은 연속 구간으로 묶여 1년 단위 청크로 fetch됩니다. 또한 시작일은 `Storage.get_min_trade_date(ticker)`로 자동 클램핑되어, 상장 이전 또는 pykrx가 제공하지 못하는 과거 구간을 매 실행마다 헛스캔하는 일을 방지합니다.

2. **`incremental=True`** — `query_missing_days`를 호출하지 않습니다. 대신 `Storage.get_max_trade_date(ticker)`로 마지막 저장일을 조회하고, 그 다음 날부터 `--end`까지를 **단일 연속 구간**으로 fetch합니다. 매일 돌리는 catch-up 작업에 최적화된 경로입니다. 데이터 중간에 구멍이 있어도 메우지 않으므로, hole 복구 목적이라면 기본 모드를 사용해야 합니다.

두 경로는 동일한 fetch/upsert 파이프라인(rate limit, retry, long rest, 청크화)을 공유하며, 차이는 "어떤 날짜 구간을 만들어 넣는가"에만 있습니다. `IngestionRun.params.incremental`로 어떤 모드로 실행되었는지가 감사 로그에 기록됩니다.

## 향후 확장: 분봉(Intraday) 수집

`IntradayPriceProvider` 프로토콜 초안이 `ports/prices.py`에 주석 처리되어 있습니다. 향후 구현 시:

1. `fetch_intraday_bars(ticker, date, interval)` 프로토콜 메서드 추가.
2. `intraday_ohlcv` 테이블 추가 (DDL 주석 참고).
3. 새로운 서비스 Use-case `backfill_intraday` 추가.
4. CLI 하위 명령어 `prices backfill-intraday` 추가.
