# 아키텍처

## 개요

KRX 데이터 파이프라인은 **포트 & 어댑터**(헥사고날) 아키텍처를 따릅니다.
도메인 로직은 인프라와 분리되어 있어, 핵심 비즈니스 규칙을 수정하지 않고도 데이터 소스나 저장소를 쉽게 교체할 수 있습니다.

## 데이터 흐름

```
┌─────────────────────────────────────────────────────────────────┐
│                          CLI (argparse)                         │
│  krx-collector universe sync / prices backfill / validate       │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      서비스 계층 (Use-cases)                      │
│  sync_universe() │ backfill_daily_prices() │ validate()         │
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
└─────────────────────────────────────────┘   └─────────────────────┘
         │                     │                        │
         ▼                     ▼                        ▼
   FinanceDataReader       pykrx API               PostgreSQL
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

## 도메인 계층

프레임워크 의존성이 없는 순수 Python 데이터 클래스(Dataclass)입니다:

- `Stock`, `DailyBar`, `StockUniverseSnapshot` — 불변 값 객체.
- `IngestionRun` — 가변 감사(Audit) 기록.
- `UpsertResult`, `SyncResult`, `BackfillResult` — 작업 결과.
- Enums: `Market`, `Source`, `ListingStatus`, `RunType`, `RunStatus`.

## 설정 (Configuration)

- `pydantic-settings`가 `.env` 및 환경 변수에서 설정을 불러옵니다.
- 시간대는 `Asia/Seoul`로 고정되어 있습니다 (설정 변경 불가).
- 설정은 `get_settings()`를 통해 싱글톤으로 캐싱됩니다.

## 향후 확장: 분봉(Intraday) 수집

`IntradayPriceProvider` 프로토콜 초안이 `ports/prices.py`에 주석 처리되어 있습니다. 향후 구현 시:

1. `fetch_intraday_bars(ticker, date, interval)` 프로토콜 메서드 추가.
2. `intraday_ohlcv` 테이블 추가 (DDL 주석 참고).
3. 새로운 서비스 Use-case `backfill_intraday` 추가.
4. CLI 하위 명령어 `prices backfill-intraday` 추가.