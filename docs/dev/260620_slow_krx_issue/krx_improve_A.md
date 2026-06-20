# KRX Security Flows 개선안 A

작성일: 2026-06-20

## 목적

`sdc_daily_krx_flows`가 26시간 이상 걸리는 문제에 대해, security flows를 KRX 말고 다른
소스에서 수집할 수 있는지 검토하고, 가능성이 낮다면 KRX 요청 구조를 어떻게 바꿔야 하는지
정리한다.

대상 metric:

- `foreign_holding_shares`
- `institution_net_buy_volume`
- `individual_net_buy_volume`
- `foreign_net_buy_volume`
- `short_selling_volume`
- `short_selling_value`
- `short_selling_balance_quantity`

## 결론

| 판단 | 내용 |
|---|---|
| KRX 완전 대체 | 현재 필요한 metric 전체를 무료/공식/전종목/일별로 안정 제공하는 대체 소스는 찾기 어렵다. |
| pykrx 대체 | pykrx는 별도 원천이 아니라 KRX/Naver 스크래핑 라이브러리다. 대체 소스라기보다 KRX endpoint 구조를 파악하는 데 유용하다. |
| 실질 개선안 | KRX를 버리는 것이 아니라, 현재의 `종목별 range serial 요청`을 `일자 x 시장` 또는 `일자 x 시장 x 투자자` bulk 요청으로 바꾸는 것이 가장 효과적이다. |
| 기대 효과 | 정상 11거래일 lookback 기준 logical request를 약 `5536`건에서 약 `132`건 수준으로 줄일 수 있다. |
| 선행 조건 | `MDCSTAT02401`이 실제로 전종목 투자자 순매수 row를 충분히 반환하는지 운영 인증 환경에서 row count/value 비교가 필요하다. |

## KRX 외 수집원 검토

| 후보 | 커버 가능성 | 한계 | 판단 |
|---|---|---|---|
| KRX Data Marketplace / KRX 정보데이터시스템 | 모든 대상 metric의 원천 메뉴가 존재한다. 투자자별 거래실적, 순매수, 외국인보유량, 공매도 거래/잔고/대차 정보가 KRX 메뉴에 있다. | 현재 병목 원천이기도 하다. 직접 호출 방식과 throttle이 문제다. | canonical source |
| pykrx | KRX/Naver 데이터를 스크래핑하며, 전종목 공매도 거래/잔고와 투자자별 순매수 wrapper가 있다. | 별도 데이터 소스가 아니다. README도 KRX/Naver 스크래핑 및 무분별한 호출 자제를 명시한다. | 원천 대체 X, endpoint 전환 참고 O |
| Naver / FinanceDataReader | 가격/종목 universe 일부에는 유용할 수 있다. | 대상 security flows 전체, 특히 공매도 잔고/외국인 보유/투자자별 전종목 일별 수급을 공식·일관 스키마로 제공한다고 보기 어렵다. | 부적합 |
| OpenDART | 공시/재무/기업 이벤트에는 적합하다. | 일별 시장 수급, 공매도 거래/잔고, 외국인 보유수량 데이터가 아니다. | 부적합 |
| 증권사 OpenAPI | 일부 화면성 데이터나 종목별 투자자/공매도 조회가 가능할 수 있다. | 계좌/API 인증, 호출 제한, 과거 범위, 전종목 bulk, 라이선스 조건이 불확실하다. 현 KRX 문제와 같은 rate-limit 문제가 남을 가능성이 크다. | 1차 대안 아님 |
| 유료 벤더 | Koscom, FnGuide, NICE 등 계약형 데이터로 해결 가능성이 있다. | 비용/계약/라이선스/납기 이슈가 있고 현재 코드의 즉시 개선책은 아니다. | 사업적 대안 |

참고:

- KRX Data Marketplace: https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd
- pykrx GitHub: https://github.com/sharebook-kr/pykrx

## 현재 병목 구조

현재 `KrxDirectFlowProvider`는 investor/shorting을 종목 단위로 호출한다.

| 그룹 | 현재 코드 | 현재 요청 단위 | 내부 KRX endpoint |
|---|---|---|---|
| foreign_holding | `fetch_foreign_holding_shares(date, market, tickers)` | `trading day x market` | `MDCSTAT03701` |
| investor | `fetch_investor_net_volume(ticker, market, start, end)` | `ticker x date range` | `MDCSTAT02302` |
| shorting | `fetch_shorting_metrics(ticker, market, start, end)` | `ticker x date range` | `MDCSTAT30001` + `MDCSTAT30502` |

핵심 문제는 investor/shorting이 매일 전종목을 한 번씩 다시 지나간다는 점이다.

- investor: 약 `2768` ticker 요청
- shorting: 약 `2768` logical request, 내부적으로 status/balance 2 POST
- `sync_krx_flows`는 각 logical request 뒤 `krx_logical_rate_limit_seconds=8.0s` sleep
- direct KRX client는 별도로 HTTP human throttle, long rest, error backoff를 수행

따라서 lookback을 줄이거나 missing-only를 개선해도 정상 일일 신규 거래일이 생기는 한
요청 건수 자체가 충분히 줄지 않는다.

## Bulk endpoint 전환 후보

로컬 pykrx 소스 기준으로, 현재 metric과 매핑 가능한 KRX bulk endpoint가 있다.

| 대상 metric | 현재 endpoint | Bulk 후보 | Bulk 요청 단위 | 비고 |
|---|---|---|---|---|
| `foreign_holding_shares` | `MDCSTAT03701` | 현행 유지 | `date x market` | 이미 전종목 bulk다. |
| `institution_net_buy_volume` | `MDCSTAT02302` | `MDCSTAT02401` | `date/range x market x investor=기관합계` | `NETBID_TRDVOL` 매핑 후보. |
| `individual_net_buy_volume` | `MDCSTAT02302` | `MDCSTAT02401` | `date/range x market x investor=개인` | `NETBID_TRDVOL` 매핑 후보. |
| `foreign_net_buy_volume` | `MDCSTAT02302` | `MDCSTAT02401` | `date/range x market x investor=외국인` | `NETBID_TRDVOL` 매핑 후보. |
| `short_selling_volume` | `MDCSTAT30001`/`MDCSTAT30102` | `MDCSTAT30101` | `date x market` | 전종목 공매도 거래량. |
| `short_selling_value` | `MDCSTAT30001`/`MDCSTAT30102` | `MDCSTAT30101` | `date x market` | 전종목 공매도 거래대금. |
| `short_selling_balance_quantity` | `MDCSTAT30502` | `MDCSTAT30501` | `date x market` | 전종목 공매도 잔고. |

pykrx에서 확인한 wrapper:

| pykrx 함수/클래스 | KRX endpoint | 의미 |
|---|---|---|
| `get_market_net_purchases_of_equities_by_ticker` / `투자자별_순매수상위종목` | `MDCSTAT02401` | 투자자별 종목 순매수 거래량/대금 |
| `get_shorting_volume_by_ticker` / `get_shorting_value_by_ticker` / `개별종목_공매도_거래_전종목` | `MDCSTAT30101` | 일자·시장별 전종목 공매도 거래 |
| `get_shorting_balance_by_ticker` / `전종목_공매도_잔고` | `MDCSTAT30501` | 일자·시장별 전종목 공매도 잔고 |
| `get_exhaustion_rates_of_foreign_investment_by_ticker` / `외국인보유량_전종목` | `MDCSTAT03701` | 일자·시장별 전종목 외국인 보유 |

## 예상 요청 수 비교

기준:

- target universe: 약 `2768` tickers
- lookback resolved trading days: `11`
- markets: KOSPI/KOSDAQ `2`
- investor categories: 기관합계/개인/외국인 `3`

| 구분 | 현재 요청 모델 | 현재 logical request | Bulk 전환 모델 | Bulk request |
|---|---|---:|---|---:|
| foreign_holding | `trading_days x markets` | `22` | 동일 | `22` |
| investor | `tickers` | `2768` | `trading_days x markets x investors` | `66` |
| shorting trading | `tickers` 내부 status POST | `2768` logical | `trading_days x markets` | `22` |
| shorting balance | `tickers` 내부 balance POST | shorting logical 내부 포함 | `trading_days x markets` | `22` |
| 합계 | `foreign + investor + shorting` | 약 `5536` logical | `foreign + investor bulk + shorting bulk` | 약 `132` |

주의:

- 현재 shorting logical request는 내부 KRX POST가 2개라 실제 HTTP 수는 logical request보다 크다.
- Bulk 전환 후에도 KRX HTTP throttle은 유지해야 한다.
- `MDCSTAT02401`은 이름이 "순매수상위종목"이므로 실제 반환 row가 전종목인지 실측해야 한다.

## 검증 계획

운영 인증 환경에서 하루 또는 2~3거래일 샘플로 비교한다.

| 검증 | 방법 | 통과 기준 |
|---|---|---|
| 투자자 bulk row count | `MDCSTAT02401`을 KOSPI/KOSDAQ, 기관합계/개인/외국인으로 호출 | 각 시장 row count가 현재 universe와 큰 차이 없이 반환되어야 한다. |
| 투자자 value parity | 기존 `MDCSTAT02302` 종목별 결과와 bulk `NETBID_TRDVOL` 비교 | 같은 ticker/date/investor의 순매수 수량이 일치해야 한다. |
| 공매도 거래 parity | 기존 shorting status 결과와 `MDCSTAT30101` 비교 | `CVSRTSELL_TRDVOL`, `CVSRTSELL_TRDVAL`이 일치해야 한다. |
| 공매도 잔고 parity | 기존 balance 결과와 `MDCSTAT30501` 비교 | `BAL_QTY`가 일치해야 한다. |
| 지연일 정책 | `MDCSTAT30501` 최신 제공일 확인 | balance가 1~2거래일 늦으면 lag-aware completeness로 처리한다. |
| 차단 내성 | 작은 canary job으로 2~3회 반복 | non-JSON/HTML 응답, backoff, retry 증가가 없어야 한다. |

샘플 SQL/코드 비교 대상은 다음 키로 맞춘다.

- `trade_date`
- `ticker`
- `market`
- `metric_code`

## 구현 방향

1. `KrxDirectFlowProvider`에 bulk method를 추가한다.

   - `fetch_investor_net_volume_bulk(trade_date, market, tickers)`
   - `fetch_shorting_trading_bulk(trade_date, market, tickers)`
   - `fetch_shorting_balance_bulk(trade_date, market, tickers)`

2. parser를 분리 추가한다.

   - `parse_investor_net_volume_bulk_rows`
   - `parse_shorting_trading_bulk_rows`
   - `parse_shorting_balance_bulk_rows`

3. `sync_krx_flows`의 실행 단위를 group별로 바꾼다.

   - foreign: 기존 유지
   - investor: `date x market x investor_category`
   - shorting_trading: `date x market`
   - shorting_balance: `date x market`, 별도 lag allowance 적용

4. 기존 종목별 provider는 fallback 또는 parity 검증용으로 남긴다.

5. ingestion run counts에 다음을 기록한다.

   - `bulk_requests_attempted`
   - `bulk_rows_upserted`
   - `bulk_provider_elapsed_s`
   - `krx_http_sleep_s`
   - `krx_error_backoff_s`

## 예상 효과

| 항목 | 현재 | 개선안 A |
|---|---:|---:|
| 정상 일일 logical request | 약 `5536` | 약 `132` |
| 요청 수 감소율 | - | 약 `97.6%` |
| 8초 logical sleep만 적용한 하한 | `5536 * 8s = 12.3h` | `132 * 8s = 17.6m` |
| shorting 내부 POST | 종목당 status+balance | 일자·시장별 trading+balance |
| 26시간 run 발생 가능성 | 높음 | 크게 낮아짐 |

이 수치는 HTTP human throttle, long rest, error backoff를 제외한 단순 logical request 기준이다.
실제 runtime은 KRX 응답 상태와 long rest에 따라 달라진다. 그래도 병목의 지배항인 요청 건수를
줄이기 때문에, daily batch가 24시간을 넘는 구조적 문제를 해소할 가능성이 높다.

## 리스크와 보완

| 리스크 | 영향 | 보완 |
|---|---|---|
| `MDCSTAT02401`이 상위 종목만 반환 | investor 전종목 데이터 결손 | 운영 인증 환경에서 row count 검증 후 진행. 미충족이면 investor는 별도 대안 필요. |
| bulk endpoint의 날짜/range 의미가 기존과 다름 | 값 불일치 | 단일일자 호출부터 parity 검증. range 호출은 통과 후 적용. |
| KRX가 bulk endpoint에도 non-JSON을 반환 | job 실패/지연 | 기존 HumanThrottle 유지, canary, circuit breaker 적용. |
| 공매도 잔고 제공 지연 | 최신 1~2일 incomplete 반복 | shorting balance를 별도 group으로 분리하고 lag-aware completeness 적용. |
| 기존 raw payload/source_bld 변경 | 데이터 lineage 변화 | `source_bld`를 bulk endpoint로 저장하고 migration/비교 로그를 남긴다. |

## 권장 순서

1. 운영 인증 환경에서 `MDCSTAT02401`, `MDCSTAT30101`, `MDCSTAT30501` 샘플 row count/parity를 검증한다.
2. 공매도 trading/balance bulk부터 구현한다. 공매도는 현재 병목과 결손 tail의 영향이 가장 크다.
3. investor bulk를 구현하되, `MDCSTAT02401` row count가 부족하면 investor는 기존 방식 분리 job으로 유지한다.
4. `sync_krx_flows`에서 shorting balance lag allowance를 추가한다.
5. daily chain에는 foreign 및 검증된 bulk group만 남기고, 종목별 fallback은 비상/보정 job으로 분리한다.

## 최종 판단

`security flows`의 현실적인 개선안 A는 **KRX 외부 소스로 바꾸는 것**이 아니라,
**KRX 내부의 전종목 bulk endpoint로 수집 단위를 바꾸는 것**이다.

현재 문서의 "investor/shorting을 daily critical path에서 분리"는 즉시 운영 안정화에는 유효하지만,
근본적으로 26시간짜리 작업을 없애지는 못한다. Bulk endpoint 전환이 검증되면 daily batch 안에서
security flows를 다시 감당 가능한 시간으로 수집할 수 있다.
