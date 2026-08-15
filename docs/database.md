# 데이터베이스 스키마

전체 DDL은 [`sql/postgres_ddl.sql`](../sql/postgres_ddl.sql)에서 확인할 수 있습니다.

## 테이블

현재 운영 중인 테이블과 함께, `account_info` 확장을 위한 Phase 0 스캐폴드 테이블도 같은 DDL에 포함되어 있습니다. 아래 1~6번은 현재 구현 범위, 7~11번은 향후 OpenDART/수급 수집을 위한 기반 스키마입니다.

### 1. `stock_master`

각 상장 종목의 최신 상태를 저장합니다.

| 컬럼명         | 타입          | 비고                           |
|----------------|---------------|--------------------------------|
| `ticker`       | TEXT NOT NULL  | 6자리 KRX 종목 코드 (PK part 1) |
| `market`       | TEXT NOT NULL  | KOSPI \| KOSDAQ (PK part 2)   |
| `name`         | TEXT NOT NULL  | 종목명 (한글)                  |
| `status`       | TEXT NOT NULL  | ACTIVE \| DELISTED \| UNKNOWN  |
| `last_seen_date` | DATE NOT NULL | 이 종목이 마지막으로 확인된 스냅샷 날짜 |
| `source`       | TEXT NOT NULL  | FDR \| PYKRX                   |
| `updated_at`   | TIMESTAMPTZ   | Insert/Update 시 자동 갱신     |

**기본키(Primary key):** `(ticker, market)`

### 2. `stock_master_snapshot`

특정 시점의 종목 유니버스 수집 메타데이터를 저장합니다.

| 컬럼명         | 타입          | 비고                         |
|----------------|---------------|------------------------------|
| `snapshot_id`  | UUID PK       | 스냅샷 고유 식별자           |
| `as_of_date`   | DATE NOT NULL  | 기준 날짜                    |
| `source`       | TEXT NOT NULL  | FDR \| PYKRX                 |
| `fetched_at`   | TIMESTAMPTZ   | 데이터를 수집한 시간         |
| `record_count` | INT NOT NULL   | 스냅샷에 포함된 종목 수      |

### 3. `stock_master_snapshot_items`

스냅샷에 캡처된 개별 종목들입니다. 임의의 두 스냅샷을 비교하여 신규 상장, 상장 폐지, 종목명 변경 등을 찾아낼 수 있습니다.

| 컬럼명         | 타입          | 비고                         |
|----------------|---------------|------------------------------|
| `snapshot_id`  | UUID FK       | `stock_master_snapshot` 참조 |
| `ticker`       | TEXT NOT NULL  | 6자리 KRX 종목 코드          |
| `market`       | TEXT NOT NULL  | KOSPI \| KOSDAQ              |
| `name`         | TEXT NOT NULL  | 스냅샷 당시의 종목명         |
| `status`       | TEXT NOT NULL  | ACTIVE \| DELISTED \| UNKNOWN |

**고유키(Unique):** `(snapshot_id, ticker, market)`

### 4. `daily_ohlcv`

일봉 OHLCV 가격 데이터입니다.

| 컬럼명       | 타입           | 비고                          |
|--------------|----------------|-------------------------------|
| `trade_date` | DATE NOT NULL   | 거래일 (PK part 1)            |
| `ticker`     | TEXT NOT NULL   | 6자리 KRX 종목 코드 (PK part 2)|
| `market`     | TEXT NOT NULL   | KOSPI \| KOSDAQ (PK part 3)   |
| `open`       | BIGINT NOT NULL | 시가 (KRW)                    |
| `high`       | BIGINT NOT NULL | 고가                          |
| `low`        | BIGINT NOT NULL | 저가                          |
| `close`      | BIGINT NOT NULL | 종가                          |
| `volume`     | BIGINT NOT NULL | 거래량                        |
| `source`     | TEXT NOT NULL   | PYKRX (현재 유일한 소스)      |
| `fetched_at` | TIMESTAMPTZ    | 데이터를 수집한 시간          |

**기본키(Primary key):** `(trade_date, ticker, market)`
**인덱스(Index):** `(ticker, market, trade_date DESC)` - 특정 종목별 조회를 위한 인덱스.

### 4b. `daily_market_cap`

KRX가 일별로 공표하는 시가총액·거래대금·상장주식수입니다. `daily_ohlcv`와 **격자는 같지만
원천이 다릅니다.**

| | `daily_ohlcv` | `daily_market_cap` |
|---|---|---|
| pykrx 엔드포인트 | `get_market_ohlcv_by_date` | `get_market_cap_by_ticker` |
| 실제 원천 | **naver** (`adjusted=True` 기본) | **KRX** |
| 가격 | **수정주가** | **당일 실제 종가(미수정)** |

그래서 종가 컬럼 이름이 `close`가 아니라 `source_close`입니다 — 호출부에서 의미 차이가
드러나게 하기 위해서입니다.

| 컬럼명          | 타입            | 비고                                   |
|-----------------|-----------------|----------------------------------------|
| `trade_date`    | DATE NOT NULL   | 거래일 (PK part 1)                     |
| `ticker`        | TEXT NOT NULL   | 6자리 KRX 종목 코드 (PK part 2)        |
| `market`        | TEXT NOT NULL   | KOSPI \| KOSDAQ (PK part 3)            |
| `source_close`  | BIGINT          | KRX 당일 실제 종가 (미수정)            |
| `market_cap`    | BIGINT          | 시가총액 (KRW)                         |
| `trading_value` | BIGINT          | 거래대금 (KRW)                         |
| `listed_shares` | BIGINT          | 상장주식수                             |
| `volume`        | BIGINT          | KRX 기준 거래량 (naver 기준과 대조용)  |
| `source`        | TEXT NOT NULL   | PYKRX                                  |
| `fetched_at`    | TIMESTAMPTZ NOT NULL | 데이터를 수집한 시간              |

**기본키(Primary key):** `(trade_date, ticker, market)`
**인덱스(Index):** `(ticker, market, trade_date DESC)`, `(fetched_at, trade_date, ticker, market)`

**`market`은 호출 인자로만 채웁니다.** 응답에 시장 구분 컬럼이 없어서
`market='ALL'` 한 번이 아니라 **날짜마다 시장별로 따로 호출**합니다. `stock_master`를 조인해
채우면 KOSDAQ→KOSPI 이전상장 종목의 과거 행에 확정 이후의 시장이 붙어 룩어헤드가 됩니다.
`ALL`을 쓰지 않는 두 번째 이유는 KONEX가 섞이기 때문입니다(2024-01-02 기준 129종목).

**결측 규칙.** pykrx가 빈 값을 `0`으로 캐스팅해 진짜 0과 결측이 구분되지 않습니다.

- `source_close == 0` → **행 전체가 결측**입니다. 휴장일에 pykrx가 빈 응답 대신
  가격만 0인 전종목 행을 돌려주기 때문입니다
- `source_close > 0`인데 `volume`·`trading_value`가 0이면 **진짜 0**(거래정지)입니다.
  NULL로 바꾸지 않습니다

**항등식.** `market_cap = source_close × listed_shares`가 정확히 성립합니다
(PoC 6,904행 전수 확인). 즉 `market_cap`은 파생값이고, 이 테이블이 새로 가져오는 정보는
`listed_shares`와 `trading_value`입니다.

### 5. `ingestion_runs`

파이프라인의 모든 실행 이력을 기록하는 감사(Audit) 로그입니다.

| 컬럼명          | 타입           | 비고                           |
|-----------------|----------------|--------------------------------|
| `run_id`        | UUID PK        | 고유 실행 식별자               |
| `run_type`      | TEXT NOT NULL   | universe_sync \| daily_backfill \| validate |
| `started_at`    | TIMESTAMPTZ    | 실행 시작 시간                 |
| `ended_at`      | TIMESTAMPTZ    | 실행 종료 시간 (실행 중엔 NULL)|
| `status`        | TEXT NOT NULL   | running \| success \| failed   |
| `params`        | JSONB          | 실행 파라미터                  |
| `counts`        | JSONB          | 집계 카운터 (성공 건수 등)     |
| `error_summary` | TEXT           | 사람이 읽기 쉬운 에러 요약     |

### 6. `sync_checkpoints`

긴 증분 동기화 작업의 커서를 저장합니다.

| 컬럼명          | 타입            | 비고                          |
|-----------------|-----------------|-------------------------------|
| `sync_name`     | TEXT PK         | 동기화 작업 이름              |
| `cursor_payload`| JSONB NOT NULL  | 다음 배치를 위한 커서 payload |
| `updated_at`    | TIMESTAMPTZ     | 커서 갱신 시각                |

### 7. `dart_corp_master`

OpenDART `corp_code`와 KRX ticker를 연결하는 기준 테이블입니다.

| 컬럼명         | 타입            | 비고                               |
|----------------|-----------------|------------------------------------|
| `corp_code`    | TEXT PK         | OpenDART 기업 고유번호             |
| `ticker`       | TEXT            | 6자리 KRX 종목 코드                |
| `corp_name`    | TEXT NOT NULL   | DART 기준 회사명                   |
| `market`       | TEXT            | KOSPI \| KOSDAQ 등                 |
| `stock_name`   | TEXT            | 종목명                             |
| `modify_date`  | DATE            | DART 파일 기준 수정일              |
| `is_active`    | BOOLEAN         | 현재 사용 대상 여부                |
| `source`       | TEXT NOT NULL   | 기본값 `OPENDART`                  |
| `fetched_at`   | TIMESTAMPTZ     | 데이터를 수집한 시간               |
| `updated_at`   | TIMESTAMPTZ     | Insert/Update 시 자동 갱신         |

**기본키(Primary key):** `corp_code`
**인덱스(Index):** `ticker`

### 8. `dart_financial_statement_raw`

OpenDART `fnlttSinglAcntAll` 및 후속 XBRL 파서가 적재할 재무 raw 테이블입니다.

| 컬럼명             | 타입              | 비고                                 |
|--------------------|-------------------|--------------------------------------|
| `raw_id`           | BIGSERIAL PK      | 내부 surrogate key                   |
| `corp_code`        | TEXT NOT NULL     | OpenDART 기업 고유번호               |
| `ticker`           | TEXT              | 6자리 KRX 종목 코드                  |
| `bsns_year`        | INT NOT NULL      | 사업연도                             |
| `reprt_code`       | TEXT NOT NULL     | 보고서 코드                          |
| `fs_div`           | TEXT NOT NULL     | CFS \| OFS                           |
| `sj_div`           | TEXT NOT NULL     | BS \| IS \| CIS \| CF \| SCE         |
| `sj_nm`            | TEXT NOT NULL     | 재무제표명                           |
| `account_id`       | TEXT NOT NULL     | 표준 계정 ID                         |
| `account_nm`       | TEXT NOT NULL     | 계정명                               |
| `account_detail`   | TEXT NOT NULL     | 계정 상세                            |
| `thstrm_nm`        | TEXT NOT NULL     | 당기명                               |
| `ord`              | BIGINT            | 원본 순서                            |
| `thstrm_amount`    | NUMERIC(30,4)     | 당기 금액                            |
| `thstrm_add_amount`| NUMERIC(30,4)     | 당기 누적 금액                       |
| `frmtrm_nm`        | TEXT NOT NULL     | 전기명                               |
| `frmtrm_amount`    | NUMERIC(30,4)     | 전기 금액                            |
| `frmtrm_q_nm`      | TEXT NOT NULL     | 전기명(분/반기)                      |
| `frmtrm_q_amount`  | NUMERIC(30,4)     | 전기 금액(분/반기)                   |
| `frmtrm_add_amount`| NUMERIC(30,4)     | 전기 누적 금액                       |
| `bfefrmtrm_nm`     | TEXT NOT NULL     | 전전기명                             |
| `bfefrmtrm_amount` | NUMERIC(30,4)     | 전전기 금액                          |
| `currency`         | TEXT              | 통화 코드                            |
| `rcept_no`         | TEXT NOT NULL     | 접수번호                             |
| `source`           | TEXT NOT NULL     | `OPENDART` 등                        |
| `fetched_at`       | TIMESTAMPTZ       | 데이터를 수집한 시간                 |
| `raw_payload`      | JSONB NOT NULL    | 원본 응답 payload                    |

**고유키(Unique):** `(corp_code, bsns_year, reprt_code, fs_div, sj_div, account_id, ord, rcept_no)`
**인덱스(Index):** `(ticker, bsns_year, reprt_code, fs_div, sj_div)`

### 9. `dart_share_count_raw`

OpenDART `stockTotqySttus` raw 저장용 테이블입니다.

| 컬럼명                 | 타입              | 비고                           |
|------------------------|-------------------|--------------------------------|
| `raw_id`               | BIGSERIAL PK      | 내부 surrogate key             |
| `corp_code`            | TEXT NOT NULL     | OpenDART 기업 고유번호         |
| `ticker`               | TEXT              | 6자리 KRX 종목 코드            |
| `bsns_year`            | INT NOT NULL      | 사업연도                       |
| `reprt_code`           | TEXT NOT NULL     | 보고서 코드                    |
| `rcept_no`             | TEXT NOT NULL     | 접수번호                       |
| `corp_cls`             | TEXT NOT NULL     | 법인 구분                      |
| `se`                   | TEXT NOT NULL     | 주식 구분/합계/비고            |
| `isu_stock_totqy`      | BIGINT            | 수권주식 총수                  |
| `now_to_isu_stock_totqy` | BIGINT          | 현재까지 발행한 주식 총수      |
| `now_to_dcrs_stock_totqy` | BIGINT         | 현재까지 감소한 주식 총수      |
| `redc`                 | TEXT NOT NULL     | 감자 관련 raw                  |
| `profit_incnr`         | TEXT NOT NULL     | 이익소각/증자 관련 raw         |
| `rdmstk_repy`          | TEXT NOT NULL     | 상환주식 상환 관련 raw         |
| `etc`                  | TEXT NOT NULL     | 기타 raw                       |
| `istc_totqy`           | BIGINT            | 발행주식 총수                  |
| `tesstk_co`            | BIGINT            | 자기주식 수                    |
| `distb_stock_co`       | BIGINT            | 유통주식 수                    |
| `stlm_dt`              | DATE              | 결산일                         |
| `source`               | TEXT NOT NULL     | `OPENDART` 등                  |
| `fetched_at`           | TIMESTAMPTZ       | 데이터를 수집한 시간           |
| `raw_payload`          | JSONB NOT NULL    | 원본 응답 payload              |

**고유키(Unique):** `(corp_code, bsns_year, reprt_code, se, rcept_no)`
**인덱스(Index):** `(ticker, bsns_year, reprt_code)`

### 10. `dart_shareholder_return_raw`

배당 및 자기주식 취득/처분/소각 관련 공시 raw 저장용 테이블입니다.

| 컬럼명           | 타입              | 비고                           |
|------------------|-------------------|--------------------------------|
| `raw_id`         | BIGSERIAL PK      | 내부 surrogate key             |
| `corp_code`      | TEXT NOT NULL     | OpenDART 기업 고유번호         |
| `ticker`         | TEXT              | 6자리 KRX 종목 코드            |
| `bsns_year`      | INT NOT NULL      | 사업연도                       |
| `reprt_code`     | TEXT NOT NULL     | 보고서 코드                    |
| `statement_type` | TEXT NOT NULL     | `dividend` \| `treasury_stock` |
| `row_name`       | TEXT NOT NULL     | 행 이름                        |
| `stock_knd`      | TEXT NOT NULL     | 보통주/우선주 등               |
| `dim1`           | TEXT NOT NULL     | 1차 분류축                     |
| `dim2`           | TEXT NOT NULL     | 2차 분류축                     |
| `dim3`           | TEXT NOT NULL     | 3차 분류축                     |
| `metric_code`    | TEXT NOT NULL     | `thstrm`, `bsis_qy` 등         |
| `metric_name`    | TEXT NOT NULL     | 표시용 metric 명               |
| `value_numeric`  | NUMERIC(30,4)     | 수치형 값                      |
| `value_text`     | TEXT NOT NULL     | 원문 텍스트 값                 |
| `unit`           | TEXT              | 주, 원 등 단위                 |
| `rcept_no`       | TEXT NOT NULL     | 접수번호                       |
| `stlm_dt`        | DATE              | 결산일                         |
| `source`         | TEXT NOT NULL     | `OPENDART` 등                  |
| `fetched_at`     | TIMESTAMPTZ       | 데이터를 수집한 시간           |
| `raw_payload`    | JSONB NOT NULL    | 원본 응답 payload              |

**고유키(Unique):** `(corp_code, bsns_year, reprt_code, statement_type, row_name, stock_knd, dim1, dim2, dim3, metric_code, rcept_no)`
**인덱스(Index):** `(ticker, bsns_year, reprt_code, statement_type)`

### 11. `dart_xbrl_document`

OpenDART `fnlttXbrl` ZIP 문서 메타 저장용 테이블입니다.

| 컬럼명                   | 타입            | 비고                           |
|--------------------------|-----------------|--------------------------------|
| `document_id`            | BIGSERIAL PK    | 내부 surrogate key             |
| `corp_code`              | TEXT NOT NULL   | OpenDART 기업 고유번호         |
| `ticker`                 | TEXT            | 6자리 KRX 종목 코드            |
| `bsns_year`              | INT NOT NULL    | 사업연도                       |
| `reprt_code`             | TEXT NOT NULL   | 보고서 코드                    |
| `rcept_no`               | TEXT NOT NULL   | 접수번호                       |
| `zip_entry_count`        | INT NOT NULL    | ZIP 내부 파일 수               |
| `instance_document_name` | TEXT NOT NULL   | `.xbrl` instance 파일명        |
| `label_ko_document_name` | TEXT NOT NULL   | 한국어 label linkbase 파일명   |
| `source`                 | TEXT NOT NULL   | `OPENDART`                     |
| `fetched_at`             | TIMESTAMPTZ     | 데이터를 수집한 시간           |
| `raw_payload`            | JSONB NOT NULL  | ZIP 엔트리 목록 등 메타        |

**고유키(Unique):** `(corp_code, bsns_year, reprt_code, rcept_no)`
**인덱스(Index):** `(ticker, bsns_year, reprt_code)`

### 12. `dart_xbrl_fact_raw`

OpenDART XBRL instance에서 추출한 fact raw 저장용 테이블입니다.

| 컬럼명         | 타입            | 비고                                   |
|----------------|-----------------|----------------------------------------|
| `raw_id`       | BIGSERIAL PK    | 내부 surrogate key                     |
| `corp_code`    | TEXT NOT NULL   | OpenDART 기업 고유번호                 |
| `ticker`       | TEXT            | 6자리 KRX 종목 코드                    |
| `bsns_year`    | INT NOT NULL    | 사업연도                               |
| `reprt_code`   | TEXT NOT NULL   | 보고서 코드                            |
| `rcept_no`     | TEXT NOT NULL   | 접수번호                               |
| `concept_id`   | TEXT NOT NULL   | `ifrs-full_Revenue` 형태 concept ID    |
| `concept_name` | TEXT NOT NULL   | local-name                             |
| `namespace_uri`| TEXT NOT NULL   | namespace URI                          |
| `context_id`   | TEXT NOT NULL   | XBRL context 식별자                    |
| `context_type` | TEXT NOT NULL   | `duration` \| `instant`                |
| `period_start` | DATE            | duration 시작일                        |
| `period_end`   | DATE            | duration 종료일                        |
| `instant_date` | DATE            | instant 기준일                         |
| `dimensions`   | JSONB NOT NULL  | axis/member 목록                       |
| `unit_id`      | TEXT NOT NULL   | XBRL unit 식별자                       |
| `unit_measure` | TEXT NOT NULL   | `iso4217:KRW`, `xbrli:shares` 등       |
| `decimals`     | TEXT NOT NULL   | decimals 속성                          |
| `value_numeric`| NUMERIC(30,4)   | 수치형 fact 값                         |
| `value_text`   | TEXT NOT NULL   | 원문 텍스트 fact 값                    |
| `is_nil`       | BOOLEAN         | nil fact 여부                          |
| `label_ko`     | TEXT NOT NULL   | 한국어 label                           |
| `source`       | TEXT NOT NULL   | `OPENDART`                             |
| `fetched_at`   | TIMESTAMPTZ     | 데이터를 수집한 시간                   |
| `raw_payload`  | JSONB NOT NULL  | tag/attribute 메타                     |

**고유키(Unique):** `(corp_code, bsns_year, reprt_code, rcept_no, context_id, concept_id)`
**인덱스(Index):** `(ticker, bsns_year, reprt_code, concept_id)`

### 13. `dart_filing_receipt_raw`

OpenDART `list.json`(공시검색) 접수 이력 raw 저장용 테이블입니다. Phase B의 원공시·정정공시
관계 판별과 SUE original-event source 확보에 사용합니다(`docs/dev/20260731_raw_features/`).

| 컬럼명        | 타입              | 비고                             |
|---------------|-------------------|-----------------------------------|
| `raw_id`      | BIGSERIAL PK      | 내부 surrogate key                |
| `corp_code`   | TEXT NOT NULL     | OpenDART 기업 고유번호            |
| `ticker`      | TEXT              | 6자리 KRX 종목 코드               |
| `corp_name`   | TEXT NOT NULL     | 법인명                            |
| `stock_code`  | TEXT NOT NULL     | 종목코드(공시 응답 기준)          |
| `corp_cls`    | TEXT NOT NULL     | 법인 구분                         |
| `report_nm`   | TEXT NOT NULL     | 보고서명                          |
| `rcept_no`    | TEXT NOT NULL     | 접수번호                          |
| `flr_nm`      | TEXT NOT NULL     | 공시 제출인명                     |
| `rcept_dt`    | DATE              | 접수일자                          |
| `rm`          | TEXT NOT NULL     | 비고(코스피/코스닥/정정 등 flag)  |
| `source`      | TEXT NOT NULL     | `OPENDART`                        |
| `fetched_at`  | TIMESTAMPTZ       | 데이터를 수집한 시간              |
| `raw_payload` | JSONB NOT NULL    | 원본 응답 payload                 |

**고유키(Unique):** `(corp_code, rcept_no)`
**인덱스(Index):** `(ticker, rcept_dt)`

### 14. `dart_capital_change_raw`

OpenDART `irdsSttus`(증자(감자)현황) raw 저장용 테이블입니다. Phase B의 경제적 순발행
(`ev_net_share_issuance_yoy`) 산식에서 유상증자/감자 등 발행·감소 사유를 액면분할·무상증자 같은
mechanical action과 구분하는 데 사용합니다.

| 컬럼명                          | 타입              | 비고                        |
|---------------------------------|-------------------|-----------------------------|
| `raw_id`                        | BIGSERIAL PK      | 내부 surrogate key          |
| `corp_code`                     | TEXT NOT NULL     | OpenDART 기업 고유번호      |
| `ticker`                        | TEXT              | 6자리 KRX 종목 코드         |
| `bsns_year`                     | INT NOT NULL      | 사업연도                    |
| `reprt_code`                    | TEXT NOT NULL     | 보고서 코드                 |
| `rcept_no`                      | TEXT NOT NULL     | 접수번호                    |
| `corp_cls`                      | TEXT NOT NULL     | 법인 구분                   |
| `isu_dcrs_de`                   | DATE              | 발행/감소 일자              |
| `isu_dcrs_stle`                 | TEXT NOT NULL     | 발행/감소 형태(유상증자 등) |
| `isu_dcrs_stock_knd`            | TEXT NOT NULL     | 주식 종류                   |
| `isu_dcrs_qy`                   | BIGINT            | 발행/감소 수량              |
| `isu_dcrs_mstvdv_fval_amount`   | NUMERIC(30,4)     | 주당 액면가액               |
| `isu_dcrs_mstvdv_fval_amount2`  | NUMERIC(30,4)     | 주당 발행/감소 가액         |
| `stlm_dt`                       | DATE              | 결산일                      |
| `source`                        | TEXT NOT NULL     | `OPENDART`                  |
| `fetched_at`                    | TIMESTAMPTZ       | 데이터를 수집한 시간        |
| `raw_payload`                   | JSONB NOT NULL    | 원본 응답 payload           |

**고유키(Unique):** `(corp_code, bsns_year, reprt_code, rcept_no, isu_dcrs_de, isu_dcrs_stle, isu_dcrs_stock_knd)`
**인덱스(Index):** `(ticker, bsns_year, reprt_code)`

### Removed derived metric tables

리팩터 이후 PostgreSQL에는 raw 수집 테이블만 남기고, `metric_catalog`,
`metric_mapping_rule`, `stock_metric_fact`는 DDL에서 제거했습니다. metric 정의와 mapping rule은
`src/krx_collector/definitions/metric_rules.py`의 코드 정의가 source of truth이며,
canonical-compatible `stock_metric_fact`는 `bin/parquet-compute-all.sh`가 raw parquet 위에서
DuckDB derived mart로 재계산합니다.

### 15. `krx_security_flow_raw`

KRX MDC 기반 수급 raw 저장용 테이블입니다.

| 컬럼명         | 타입              | 비고                              |
|----------------|-------------------|-----------------------------------|
| `raw_id`       | BIGSERIAL PK      | 내부 surrogate key                |
| `trade_date`   | DATE NOT NULL     | 거래일                            |
| `ticker`       | TEXT NOT NULL     | 6자리 KRX 종목 코드               |
| `market`       | TEXT NOT NULL     | KOSPI \| KOSDAQ                   |
| `metric_code`  | TEXT NOT NULL     | 내부 metric code                  |
| `metric_name`  | TEXT NOT NULL     | 표시용 metric 명                  |
| `value`        | NUMERIC(30,4)     | 정규화된 수치 값                  |
| `unit`         | TEXT              | 주, 원, 비율 등                   |
| `source`       | TEXT NOT NULL     | `KRX`                             |
| `fetched_at`   | TIMESTAMPTZ       | 데이터를 수집한 시간              |
| `raw_payload`  | JSONB NOT NULL    | 원본 응답 payload                 |

**고유키(Unique):** `(trade_date, ticker, market, metric_code, source)`
**인덱스(Index):** `(ticker, market, trade_date DESC)`

현재 1차 구현 metric code:

- `foreign_holding_shares`
- `foreign_net_buy_volume`
- `institution_net_buy_volume`
- `individual_net_buy_volume`
- `short_selling_volume`
- `short_selling_value`
- `short_selling_balance_quantity`

보류 metric:

- `borrow_balance_quantity`

### Removed operating pilot tables

파일럿 operating KPI 경로(`operating_source_document`, `operating_metric_fact`)는 스케줄과
프로덕션 데이터가 없어 리팩터 P5에서 DDL과 CLI 경로를 제거했습니다.

## Upsert 전략

### `daily_ohlcv`

```sql
INSERT INTO daily_ohlcv (trade_date, ticker, market, open, high, low, close, volume, source, fetched_at)
VALUES (...)
ON CONFLICT (trade_date, ticker, market) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    source = EXCLUDED.source,
    fetched_at = EXCLUDED.fetched_at;
```

**설계 이유:** KRX에서 가끔 가격 데이터를 수정하는 경우가 있기 때문에, `DO NOTHING` 대신 `DO UPDATE`를 사용하여 재수집 시 기존(수정 전) 데이터를 덮어쓰도록 했습니다.

### `daily_market_cap`

```sql
INSERT INTO daily_market_cap (trade_date, ticker, market, source_close, market_cap,
                              trading_value, listed_shares, volume, source, fetched_at)
VALUES (...)
ON CONFLICT (trade_date, ticker, market) DO UPDATE SET
    source_close = EXCLUDED.source_close,
    market_cap = EXCLUDED.market_cap,
    trading_value = EXCLUDED.trading_value,
    listed_shares = EXCLUDED.listed_shares,
    volume = EXCLUDED.volume,
    source = EXCLUDED.source,
    fetched_at = EXCLUDED.fetched_at;
```

**설계 이유:** `daily_ohlcv`와 같습니다. 추가로 **한 응답(= 한 날짜 · 한 시장)의 전체 행을
한 트랜잭션으로** upsert합니다. 배치를 쪼개면 중간에 중단됐을 때 그 슬라이스가 영구히
불완전하게 남고, "행이 있으면 완료"로 판정하는 skip 규칙이 다시는 그 구멍을 채우지 않습니다.

### `stock_master`

```sql
INSERT INTO stock_master (ticker, market, name, status, last_seen_date, source)
VALUES (...)
ON CONFLICT (ticker, market) DO UPDATE SET
    name = EXCLUDED.name,
    status = EXCLUDED.status,
    last_seen_date = EXCLUDED.last_seen_date,
    source = EXCLUDED.source;
```

## 향후 확장: `intraday_ohlcv`

`sql/postgres_ddl.sql`에 주석 처리된 DDL을 참고하세요. 기본키(Primary key)는 `(trade_ts, ticker, market, interval)`이 될 예정입니다.
