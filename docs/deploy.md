# 배포 및 Cronicle 데이터 수집 매핑

이 문서는 `sj2-server`에서 Cronicle이 실행하는 SDC 운영 래퍼 스크립트가 어떤 collector 명령을 실행하고, 각 명령이 어떤 DB 테이블에 어떤 데이터를 저장하는지 정리한다.

## 확인 기준

- 기준일: 2026-05-31 KST
- Cronicle source: `http://192.168.0.11:3012/api/app/get_event/v1`
- 운영 래퍼 source: `whi@sj2-server:/home/whi/apps/sdc/bin/*.sh`
- IaC source: [`deploy/prod/bin`](../deploy/prod/bin), [`deploy/prod/compose.yaml`](../deploy/prod/compose.yaml)
- collector image: `ghcr.io/sjleekor/sdc:v0.8.7`
- DB schema: [`sql/postgres_ddl.sql`](../sql/postgres_ddl.sql), 요약 문서: [`docs/database.md`](database.md)

Cronicle 이벤트는 shell plugin으로 `/home/whi/apps/sdc/bin/*.sh` 래퍼를 순서대로 호출한다. 각 래퍼는 `/home/whi/apps/sdc`에서 `docker compose run --rm collector ...`를 실행한다. 이 문서의 수집/정규화 collector 명령은 실행 감사 로그를 `ingestion_runs`에 기록한다.

보고서 코드 해석:

| `reprt_code` | 의미 | 정규화 `period_type` |
|---|---|---|
| `11013` | 1분기보고서 | `q1` |
| `11012` | 반기보고서 | `half` |
| `11014` | 3분기보고서 | `q3` |
| `11011` | 사업보고서 | `annual` |

## Cronicle 이벤트 요약

두 이벤트 모두 Cronicle `shellplug`로 실행되며, `target=maingrp`, `max_children=1`, `timezone=Asia/Seoul` 설정이다.

| Cronicle event id | Title | 상태 | 실행 순서 |
|---|---|---|---|
| `sdc_daily_pipeline` | `SDC Daily Pipeline` | enabled | `universe-sync.sh` -> `prices-backfill-incremental.sh` -> `flows-sync.sh` |
| `sdc_daily_accounts_flows` | `SDC Daily Accounts/Flows Pipeline` | enabled | `dart-sync-corp.sh` -> `dart-sync-financials.sh` -> `dart-sync-share-info.sh` -> `dart-sync-xbrl.sh` -> `metrics-normalize.sh` |
| `sdc_daily_common_features` | `SDC Daily Common Features` | 권장 추가 | `common-features-refresh.sh` |

Cronicle script는 `set -euo pipefail`을 사용하므로 앞 단계 래퍼가 실패하면 이후 래퍼는 실행되지 않는다.

## `sdc_daily_pipeline`

### 1. `universe-sync.sh`

```bash
docker compose run --rm collector universe sync --source fdr --markets kospi,kosdaq
```

| 항목 | 내용 |
|---|---|
| 외부 source | FinanceDataReader `StockListing("KOSPI")`, `StockListing("KOSDAQ")`; 실패 시 pykrx fallback |
| 주요 read table | `stock_master` |
| write table | `stock_master_snapshot`, `stock_master_snapshot_items`, `stock_master`, `ingestion_runs` |
| `ingestion_runs.run_type` | `universe_sync` |

저장 데이터:

- `stock_master_snapshot`: 수집 시점의 KOSPI/KOSDAQ 상장 종목 universe snapshot 메타데이터.
- `stock_master_snapshot_items`: snapshot에 포함된 개별 종목 목록.
- `stock_master`: 최신 종목 마스터. 신규 종목, 종목명 변경, 상장폐지 추정 상태를 반영한다.
- `ingestion_runs`: universe sync 실행 시작/종료, 성공/실패, 수집 건수와 diff 카운터.

### 2. `prices-backfill-incremental.sh`

```bash
docker compose run --rm collector prices backfill --market all --incremental
```

| 항목 | 내용 |
|---|---|
| 외부 source | pykrx `get_market_ohlcv_by_date` |
| 주요 read table | `stock_master`, `daily_ohlcv` |
| write table | `daily_ohlcv`, `ingestion_runs` |
| `ingestion_runs.run_type` | `daily_backfill` |

저장 데이터:

- `daily_ohlcv`: 종목별 일봉 OHLCV. `trade_date`, `ticker`, `market` 단위로 시가, 고가, 저가, 종가, 거래량, source, fetched timestamp를 저장한다.
- `ingestion_runs`: 가격 backfill 실행 상태, 처리 종목 수, upsert row 수, 에러 수.

`--incremental` 모드는 종목별 `MAX(trade_date) + 1`부터 오늘(KST)까지 한 번의 연속 구간으로 가져온다. 중간 누락 구간을 찾는 gap detection은 하지 않는다.

### 3. `flows-sync.sh`

```bash
docker compose run --rm collector flows sync --incremental --lookback-days "${FLOW_LOOKBACK_DAYS:-14}"
```

| 항목 | 내용 |
|---|---|
| 외부 source | KRX MDC JSON endpoint 직접 호출 |
| 주요 read table | `daily_ohlcv`, `krx_security_flow_raw`, `stock_master`; `stock_master` 대상이 없으면 `dart_corp_master` fallback |
| write table | `krx_security_flow_raw`, `ingestion_runs` |
| `ingestion_runs.run_type` | `krx_flow_sync` |

저장 데이터:

- `krx_security_flow_raw`: 거래일/종목/시장/metric 단위 수급 raw metric.
- `ingestion_runs`: flow sync 실행 상태, 시도/skip request 수, upsert row 수, no-data/에러 수.

일일 래퍼는 `--incremental` 모드를 사용한다. `FLOW_END`는 `daily_ohlcv`의 최신 거래일이고, `FLOW_START`는 KRX 수급 metric group별 최신일 중 가장 오래된 날짜와 최근 lookback window를 함께 고려해 계산한다. 기본 lookback은 14일이다. 계산된 범위와 group별 최신일은 로그와 `ingestion_runs.params`에 기록된다.

`--use-price-range`는 일일 래퍼에서 사용하지 않는다. 히스토리 보수는 `flows-backfill-range.sh`로 명시 범위를 지정해 실행한다.

```bash
FLOW_START=2026-05-01 FLOW_END=2026-05-31 /home/whi/apps/sdc/bin/flows-backfill-range.sh
```

현재 저장 metric:

| metric_code | 의미 | 단위 |
|---|---|---|
| `foreign_holding_shares` | 외국인 보유주식수 | `shares` |
| `institution_net_buy_volume` | 기관 순매수 수량 | `shares` |
| `individual_net_buy_volume` | 개인 순매수 수량 | `shares` |
| `foreign_net_buy_volume` | 외국인 순매수 수량 | `shares` |
| `short_selling_volume` | 공매도 거래량 | `shares` |
| `short_selling_value` | 공매도 거래대금 | `KRW` |
| `short_selling_balance_quantity` | 공매도 잔고 수량 | `shares` |

`--incremental` 모드의 종료일은 가격 최신일이므로 가격 데이터가 먼저 적재되어 있어야 수급 수집이 진행된다. Cronicle의 `sdc_daily_pipeline`은 `prices-backfill-incremental.sh` 뒤에 `flows-sync.sh`를 실행하므로 이 전제를 만족한다.

## `sdc_daily_accounts_flows`

### 1. `dart-sync-corp.sh`

```bash
docker compose run --rm collector dart sync-corp
```

| 항목 | 내용 |
|---|---|
| 외부 source | OpenDART `corpCode.xml` |
| 주요 read table | `stock_master`, `ingestion_runs` |
| write table | `dart_corp_master`, `ingestion_runs` |
| `ingestion_runs.run_type` | `dart_corp_sync` |

저장 데이터:

- `dart_corp_master`: OpenDART `corp_code`와 KRX ticker/market/name 매핑. `stock_master`의 active ticker와 매칭되면 `is_active=true`로 저장한다.
- `ingestion_runs`: corp master sync 실행 상태, OpenDART key 수, fetch/upsert/매칭 카운터.

주의: 래퍼는 `--force`를 주지 않는다. 이전 성공 이력이 있으면 OpenDART를 다시 받지 않고 skip 결과를 `ingestion_runs`에 기록한다.

### 2. `dart-sync-financials.sh`

```bash
docker compose run --rm collector dart sync-financials \
  --reprt-codes 11011,11012,11013,11014 \
  --bsns-years "$(date +%Y),$(($(date +%Y)-1))"
```

| 항목 | 내용 |
|---|---|
| 외부 source | OpenDART `fnlttSinglAcntAll.json` |
| 주요 read table | `dart_corp_master`, `dart_financial_statement_raw` |
| write table | `dart_financial_statement_raw`, `ingestion_runs` |
| `ingestion_runs.run_type` | `dart_financial_sync` |

저장 데이터:

- `dart_financial_statement_raw`: 기업/사업연도/보고서/재무제표 구분(`fs_div`) account line 단위의 재무제표 raw row. 현재 래퍼는 CLI 기본값인 `CFS`만 수집한다. 재무상태표, 손익계산서, 현금흐름표 등 OpenDART 응답의 계정 ID, 계정명, 당기/전기 금액, 접수번호, 원본 payload를 저장한다.
- `ingestion_runs`: 대상 기업 수, request 시도/skip 수, upsert row 수, no-data/에러 수, OpenDART key 사용 현황.

기본 `fs_divs`는 CLI 기본값 `CFS`이다. 2026-05-31에 실행하면 `bsns_years`는 `2026,2025`로 해석된다. 기존 raw key가 있으면 `--force` 없이는 해당 request를 skip한다.

### 3. `dart-sync-share-info.sh`

```bash
docker compose run --rm collector dart sync-share-info
```

| 항목 | 내용 |
|---|---|
| 외부 source | OpenDART `stockTotqySttus.json`, `alotMatter.json`, `tesstkAcqsDspsSttus.json` |
| 주요 read table | `dart_corp_master`, `dart_share_count_raw`, `dart_shareholder_return_raw` |
| write table | `dart_share_count_raw`, `dart_shareholder_return_raw`, `ingestion_runs` |
| `ingestion_runs.run_type` | `dart_share_info_sync` |

저장 데이터:

- `dart_share_count_raw`: 발행가능주식수, 현재까지 발행/감소 주식수, 발행주식 총수, 자기주식수, 유통주식수, 결산일 등 주식 총수 현황 raw.
- `dart_shareholder_return_raw`: 배당과 자기주식 취득/처분/소각 공시를 metric row로 평탄화한 raw. `statement_type`은 `dividend` 또는 `treasury_stock`이다.
- `ingestion_runs`: share-count/shareholder-return upsert row 수, request 시도/skip 수, no-data/에러 수, OpenDART key 사용 현황.

래퍼가 연도/보고서 코드를 지정하지 않으므로 CLI 기본값을 사용한다. 기본값은 전년도 `bsns_year`, 사업보고서 `11011`이다.

### 4. `dart-sync-xbrl.sh`

```bash
docker compose run --rm collector dart sync-xbrl \
  --reprt-codes 11011,11012,11013,11014
```

| 항목 | 내용 |
|---|---|
| 외부 source | OpenDART `fnlttXbrl.xml` |
| 주요 read table | `dart_corp_master`, `dart_financial_statement_raw`, `dart_xbrl_document` |
| write table | `dart_xbrl_document`, `dart_xbrl_fact_raw`, `ingestion_runs` |
| `ingestion_runs.run_type` | `xbrl_parse` |

저장 데이터:

- `dart_xbrl_document`: XBRL ZIP 문서 메타데이터. 접수번호, ZIP entry 수, instance `.xbrl` 파일명, 한국어 label linkbase 파일명, 원본 entry 목록 등을 저장한다.
- `dart_xbrl_fact_raw`: XBRL instance에서 추출한 fact row. concept ID/name, namespace, context, period, dimensions, unit, decimals, numeric/text value, 한국어 label, 원본 tag/attribute를 저장한다.
- `ingestion_runs`: XBRL request 시도/skip 수, document/fact upsert row 수, no-data/에러 수, OpenDART key 사용 현황.

XBRL 수집 대상은 먼저 `dart_financial_statement_raw`에 저장된 `rcept_no`에서 만든다. 재무 raw row가 없으면 해당 filing의 XBRL 요청도 만들 수 없다. 래퍼가 연도를 지정하지 않으므로 CLI 기본값인 전년도만 대상으로 한다.

### 5. `metrics-normalize.sh`

```bash
docker compose run --rm collector metrics normalize
```

| 항목 | 내용 |
|---|---|
| 외부 source | 없음. DB raw table을 정규화 |
| 주요 read table | `dart_corp_master`, `dart_financial_statement_raw`, `dart_share_count_raw`, `dart_shareholder_return_raw`, `dart_xbrl_fact_raw`, `metric_mapping_rule` |
| write table | `metric_catalog`, `metric_mapping_rule`, `stock_metric_fact`, `ingestion_runs` |
| `ingestion_runs.run_type` | `metric_normalize` |

저장 데이터:

- `metric_catalog`: canonical metric 사전. 매출액, 영업이익, 총자산, 발행주식수, DPS, 가중평균주식수 등 표준 metric 정의.
- `metric_mapping_rule`: raw row를 canonical metric으로 변환하는 규칙. 실행 시 기존 rule을 inactive로 만들고 현재 기본 rule set을 upsert한다.
- `stock_metric_fact`: 종목/사업연도/보고서/metric 단위 정규화 fact. 원천 raw table, 원천 row key, 적용 mapping rule, period type/end, numeric value를 저장한다.
- `ingestion_runs`: 정규화 대상 수, catalog/rule upsert 수, fact write 수, 에러 수.

래퍼가 연도/보고서 코드를 지정하지 않으므로 CLI 기본값을 사용한다. 기본값은 전년도 `bsns_year`, 사업보고서 `11011`이다. 따라서 `dart-sync-financials.sh`가 1분기/반기/3분기 raw까지 수집하더라도, 이 래퍼만으로는 기본적으로 전년도 사업보고서 raw만 `stock_metric_fact`로 정규화된다.

## `sdc_daily_common_features`

### 1. `common-features-refresh.sh`

```bash
docker compose run --rm collector common seed-catalog --init-schema
docker compose run --rm collector common sync --sources fdr,fred,ecos,krx --start <daily_start> --end <end>
docker compose run --rm collector common sync --sources ecos --series macro_cpi,macro_ppi,macro_m2,macro_consumer_sentiment --start <macro_start> --end <end> --force
docker compose run --rm collector common build-daily --start <build_start> --end <end>
docker compose run --rm collector common coverage-report --start <readiness_start> --end <end>
docker compose run --rm collector common readiness-report --start <readiness_start> --end <end> --required-coverage-ratio 1.0 --fail-on-not-ready
```

| 항목 | 내용 |
|---|---|
| 외부 source | KRX direct, ECOS, FRED, FinanceDataReader |
| 주요 read table | `common_feature_series`, `common_feature_catalog`, `common_feature_observation_raw`, KRX 휴장일 CSV |
| write table | `common_feature_series`, `common_feature_catalog`, `common_feature_catalog_input`, `common_feature_observation_raw`, `common_feature_daily_fact`, `ingestion_runs` |
| `ingestion_runs.run_type` | `common_feature_sync`, `common_feature_build` |

기본 lookback:

| 범위 | 기본값 | 목적 |
|---|---:|---|
| daily sync | 45 calendar days | KRX/FDR/FRED/ECOS 일간 source 최근분 보강 |
| monthly macro sync | 540 calendar days | CPI/PPI/M2/CSI revision 및 YoY 입력 보강 |
| build daily | 120 calendar days | 최근 모델 feature row 재생성 |
| readiness | 60 calendar days | 운영 품질 판정 |

`readiness-report --fail-on-not-ready`가 not-ready feature 또는 report error를 발견하면 exit code `2`로 종료한다. Cronicle은 이 exit code를 이벤트 실패로 기록한다.

## 테이블별 write 경로

| 테이블 | 쓰는 래퍼/명령 | 저장 데이터 요약 |
|---|---|---|
| `stock_master_snapshot` | `universe-sync.sh` | universe 수집 한 번의 snapshot 메타데이터 |
| `stock_master_snapshot_items` | `universe-sync.sh` | snapshot에 포함된 종목 목록 |
| `stock_master` | `universe-sync.sh` | 최신 KOSPI/KOSDAQ 종목 마스터와 상장 상태 |
| `daily_ohlcv` | `prices-backfill-incremental.sh` | 종목별 일봉 OHLCV와 거래량 |
| `krx_security_flow_raw` | `flows-sync.sh` | KRX 수급, 외국인 보유, 공매도 raw metric |
| `dart_corp_master` | `dart-sync-corp.sh` | OpenDART corp_code와 KRX ticker 매핑 |
| `dart_financial_statement_raw` | `dart-sync-financials.sh` | OpenDART 단일회사 전체 재무제표 raw account line |
| `dart_share_count_raw` | `dart-sync-share-info.sh` | OpenDART 주식 총수 현황 raw |
| `dart_shareholder_return_raw` | `dart-sync-share-info.sh` | OpenDART 배당/자기주식 공시 raw metric row |
| `dart_xbrl_document` | `dart-sync-xbrl.sh` | OpenDART XBRL ZIP 문서 메타데이터 |
| `dart_xbrl_fact_raw` | `dart-sync-xbrl.sh` | OpenDART XBRL instance fact raw row |
| `metric_catalog` | `metrics-normalize.sh` | canonical metric 정의 |
| `metric_mapping_rule` | `metrics-normalize.sh` | raw-to-canonical metric mapping rule |
| `stock_metric_fact` | `metrics-normalize.sh` | 종목별 canonical metric fact |
| `common_feature_series` | `common-features-refresh.sh` | 공통 feature source catalog |
| `common_feature_catalog` | `common-features-refresh.sh` | 모델 노출 공통 feature catalog |
| `common_feature_catalog_input` | `common-features-refresh.sh` | feature와 source series input mapping |
| `common_feature_observation_raw` | `common-features-refresh.sh` | 시장/거시 source raw observation |
| `common_feature_daily_fact` | `common-features-refresh.sh` | KRX 거래일 기준 PIT-safe 공통 feature fact |
| `ingestion_runs` | 모든 collector 명령 | collector 실행 감사 로그, 상태, counts, 에러 요약 |

## 주요 의존성

| downstream 단계 | 의존 데이터 | 이유 |
|---|---|---|
| `prices backfill` | `stock_master` | active ticker 목록을 읽어 가격 수집 대상을 만든다. |
| `flows sync --incremental` | `daily_ohlcv`, `krx_security_flow_raw` | 수급 수집 종료일은 가격 최신일, 시작일은 저장된 수급 metric group 최신일과 lookback window로 계산한다. |
| `flows sync` | `stock_master`, fallback `dart_corp_master` | 수급 수집 대상 종목과 시장을 만든다. |
| `dart sync-corp` | `stock_master` | OpenDART corp_code를 현재 active KRX ticker/market/name과 매핑한다. |
| `dart sync-financials` | `dart_corp_master` | active OpenDART corp mapping이 있어야 기업별 재무 요청을 만든다. |
| `dart sync-share-info` | `dart_corp_master` | active OpenDART corp mapping이 있어야 주식수/배당/자사주 요청을 만든다. |
| `dart sync-xbrl` | `dart_financial_statement_raw` | 저장된 재무 raw의 `rcept_no`를 XBRL ZIP 요청 키로 사용한다. |
| `metrics normalize` | DART raw tables, `dart_corp_master` | raw row를 canonical metric으로 변환하고 market/corp_code를 채운다. |
| `common build-daily` | `common_feature_observation_raw`, `common_feature_catalog_input` | raw observation을 KRX 거래일 기준 point-in-time daily fact로 정렬한다. |

## 운영상 주의사항

- 대부분의 raw 수집 명령은 기존 key가 있으면 `--force` 없이는 skip한다. 재실행은 대체로 멱등적이며, 필요한 경우 동일 파라미터로 다시 실행해 누락분을 이어받는다.
- OpenDART 명령은 모든 API key가 일일 한도에 도달하면 exit code `75`로 종료한다. Cronicle script는 `set -e`라서 그 시점에서 이벤트가 중단된다.
- `sdc_daily_accounts_flows`라는 이벤트 이름에 `flows`가 들어가지만, 실제 KRX 수급(`krx_security_flow_raw`) 수집은 `sdc_daily_pipeline`의 `flows-sync.sh`에서 수행한다. accounts event의 마지막 단계는 DART raw를 `stock_metric_fact`로 정규화하는 단계다.
- 현재 나열된 두 Cronicle 이벤트는 `sync_checkpoints`, `operating_source_document`, `operating_metric_fact`에는 쓰지 않는다.
