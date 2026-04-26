# flows sync pykrx 의존 제거 계획

작성일: 2026-04-26

## 목적

`krx-collector flows sync`는 현재 `pykrx.stock`의 수급/공매도 API를 직접 호출한다. pykrx의 KRX 대응이 늦거나 내부 예외를 stdout으로만 출력하는 문제가 있어, `flows sync`에 필요한 KRX HTTP 호출과 parser를 프로젝트 내부에 직접 구현한다.

## 현재 flows sync 구조

- CLI 진입점은 `src/krx_collector/cli/app.py`의 `_handle_flows_sync()`이다.
- 현재 CLI는 `PykrxFlowProvider`를 고정 import하여 `sync_krx_security_flows()`에 주입한다.
- 서비스 포트는 이미 `FlowProvider`로 분리되어 있어 provider 교체 자체는 작다.
- 저장 테이블은 `krx_security_flow_raw`이며 unique key는 `(trade_date, ticker, market, metric_code, source)`이다.
- 현재 서비스의 기존 데이터 skip 조회는 `Source.PYKRX`로 고정되어 있다. 직접 KRX provider를 `Source.KRX`로 기록하려면 이 부분도 provider source 기준으로 바꿔야 한다.

현재 수집 metric:

| metric_code | 의미 | 현재 source |
| --- | --- | --- |
| `foreign_holding_shares` | 외국인 보유주식수 | pykrx |
| `institution_net_buy_volume` | 기관 순매수 수량 | pykrx |
| `individual_net_buy_volume` | 개인 순매수 수량 | pykrx |
| `foreign_net_buy_volume` | 외국인 순매수 수량 | pykrx |
| `short_selling_volume` | 공매도 거래량 | pykrx |
| `short_selling_value` | 공매도 거래대금 | pykrx |
| `short_selling_balance_quantity` | 공매도 잔고 수량 | pykrx |

현재 pending metric:

- `borrow_balance_quantity`: pykrx clone에서도 안정적인 대차잔고 API 경로를 찾지 못했다. 이번 변경 범위에서는 기존과 동일하게 pending 유지가 맞다.

## 현재 pykrx 사용 지점

`src/krx_collector/adapters/flows_pykrx/provider.py` 기준 현재 호출은 4개다.

| provider method | pykrx function | 호출 단위 | 내부 parser |
| --- | --- | --- | --- |
| `fetch_investor_net_volume()` | `stock.get_market_trading_volume_by_date(from, to, ticker)` | 종목별 기간 | `parse_investor_net_volume_frame()` |
| `fetch_foreign_holding_shares()` | `stock.get_exhaustion_rates_of_foreign_investment_by_ticker(date, market)` | 시장별 일자 | `parse_foreign_holding_frame()` |
| `fetch_shorting_metrics()` | `stock.get_shorting_status_by_date(from, to, ticker)` | 종목별 기간 | `parse_shorting_frames()` |
| `fetch_shorting_metrics()` | `stock.get_shorting_balance_by_date(from, to, ticker)` | 종목별 기간 | `parse_shorting_frames()` |

현재 provider는 pykrx 호출을 `SIGALRM` timeout으로 감싸고, pykrx의 `dataframe_empty_handler`가 출력하는 `Error occurred ...` 문자열을 예외로 바꾼다. 직접 구현에서는 HTTP timeout과 응답 검증을 명시적으로 처리하면 이 우회 로직이 필요 없다.

## pykrx가 감싸는 KRX 화면/API

pykrx clone 경로: `/Users/whishaw/wss_p/pykrx/pykrx`

공통 호출 방식:

- URL: `https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd`
- Method: `POST`
- 공통 header:
  - `User-Agent`
  - `Referer: https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd`
  - `X-Requested-With: XMLHttpRequest`
- payload에는 화면별 `bld`와 파라미터를 넣는다.
- pykrx의 `KrxWebIo.read()`는 `strtDd/endDd` 요청을 730일 단위로 쪼갠다.

### 1. 투자자별 순매수 수량

pykrx public API:

- `stock.get_market_trading_volume_by_date(fromdate, todate, ticker)`
- 기본값: `on="순매수"`, `detail=False`

KRX 직접 호출:

| 항목 | 값 |
| --- | --- |
| bld | `dbms/MDC/STAT/standard/MDCSTAT02302` |
| pykrx class | `투자자별_거래실적_개별종목_일별추이_일반` |
| payload | `strtDd`, `endDd`, `isuCd`, `inqTpCd=2`, `trdVolVal=1`, `askBid=3` |
| output key | `output` |
| 주요 raw columns | `TRD_DD`, `TRDVAL1`, `TRDVAL2`, `TRDVAL3`, `TRDVAL4`, `TRDVAL_TOT` |
| pykrx column mapping | 날짜, 기관합계, 기타법인, 개인, 외국인합계, 전체 |

현재 저장 mapping:

- `TRDVAL1` → `institution_net_buy_volume`
- `TRDVAL3` → `individual_net_buy_volume`
- `TRDVAL4` → `foreign_net_buy_volume`

### 2. 외국인 보유주식수

pykrx public API:

- `stock.get_exhaustion_rates_of_foreign_investment_by_ticker(date, market)`

KRX 직접 호출:

| 항목 | 값 |
| --- | --- |
| bld | `dbms/MDC/STAT/standard/MDCSTAT03701` |
| pykrx class | `외국인보유량_전종목` |
| payload | `searchType=1`, `mktId`, `trdDd`, `isuLmtRto=0` |
| market mapping | `KOSPI -> STK`, `KOSDAQ -> KSQ` |
| output key | `output` |
| 주요 raw columns | `ISU_SRT_CD`, `LIST_SHRS`, `FORN_HD_QTY`, `FORN_SHR_RT`, `FORN_ORD_LMT_QTY`, `FORN_LMT_EXHST_RT` |

현재 저장 mapping:

- `ISU_SRT_CD` → ticker
- `FORN_HD_QTY` → `foreign_holding_shares`

### 3. 공매도 거래량/거래대금/종합 잔고

pykrx public API:

- `stock.get_shorting_status_by_date(fromdate, todate, ticker)`

KRX 직접 호출:

| 항목 | 값 |
| --- | --- |
| bld | `dbms/MDC/STAT/srt/MDCSTAT30001` |
| pykrx class | `개별종목_공매도_종합정보` |
| payload | `isuCd`, `strtDd`, `endDd` |
| output key | `OutBlock_1` |
| 주요 raw columns | `TRD_DD`, `CVSRTSELL_TRDVOL`, `STR_CONST_VAL1`, `CVSRTSELL_TRDVAL`, `STR_CONST_VAL2` |
| pykrx column mapping | 날짜, 거래량, 잔고수량, 거래대금, 잔고금액 |

현재 저장 mapping:

- `CVSRTSELL_TRDVOL` → `short_selling_volume`
- `CVSRTSELL_TRDVAL` → `short_selling_value`
- `STR_CONST_VAL1`은 balance endpoint 값이 없을 때 `short_selling_balance_quantity` fallback으로만 사용

### 4. 공매도 잔고 수량

pykrx public API:

- `stock.get_shorting_balance_by_date(fromdate, todate, ticker)`

KRX 직접 호출:

| 항목 | 값 |
| --- | --- |
| bld | `dbms/MDC/STAT/srt/MDCSTAT30502` |
| pykrx class | `개별종목_공매도_잔고` |
| payload | `strtDd`, `endDd`, `isuCd` |
| output key | `OutBlock_1` |
| 주요 raw columns | `RPT_DUTY_OCCR_DD`, `BAL_QTY`, `LIST_SHRS`, `BAL_AMT`, `MKTCAP`, `BAL_RTO` |

현재 저장 mapping:

- `BAL_QTY` → `short_selling_balance_quantity`

### 5. ticker → ISIN 변환

투자자별/공매도 개별종목 endpoint는 6자리 ticker가 아니라 ISIN(`KR7005930003` 형태)을 요구한다.

pykrx는 import 시 `StockTicker()`를 만들고 다음 finder endpoint를 호출한다.

| 항목 | 값 |
| --- | --- |
| bld | `dbms/comm/finder/finder_stkisu` |
| payload | `locale=ko_KR`, `mktsel=ALL`, `searchText=""`, `typeNo=0` |
| output key | `block1` |
| 주요 raw columns | `short_code`, `codeName`, `full_code`, `marketName` |

직접 구현에서도 이 finder를 호출해 ticker→ISIN cache를 만든다. DB의 `stock_master`에는 현재 ISIN이 없으므로 KRX finder cache가 가장 작은 변경이다.

## 직접 구현 설계

새 adapter를 추가한다.

```text
src/krx_collector/adapters/flows_krx/
  __init__.py
  client.py        # KRX MDC HTTP client
  codes.py         # ticker -> ISIN resolver/cache
  provider.py      # FlowProvider 구현
  parsers.py       # KRX JSON row -> SecurityFlowLine 변환
```

### `KrxMdcClient`

역할:

- `requests.Session` 기반 POST client
- 공통 header 주입
- `timeout_seconds` 기본값 20초
- `strtDd/endDd` 기간 요청은 730일 단위 chunk로 분할
  - 적용 대상: `MDCSTAT02302`(투자자별), `MDCSTAT30001`(공매도 종합), `MDCSTAT30502`(공매도 잔고)
  - 미적용 대상: `MDCSTAT03701`(외국인 보유, 단일 `trdDd`), `finder_stkisu`(단발성)
- HTTP status, JSON decode, KRX error payload, 필수 output key 부재를 명시적 예외로 변환
- raw response는 parser에 넘기고, `raw_payload`에는 최소 `bld`, 요청 parameter, raw row를 남김

인증:

- 1차 구현은 비로그인 session + warm-up 요청으로 시작한다.
- KRX가 인증을 요구하는 응답을 반환하면 `settings.krx_id`, `settings.krx_pw`를 사용한 로그인 흐름을 client 내부에 추가한다.
- pykrx의 auth 모듈에 의존하지 않고, 필요한 URL과 payload만 자체 구현한다.

### `KrxStockCodeResolver`

역할:

- `finder_stkisu` 호출 결과를 process-local cache로 보관한다.
- ticker로 `full_code` ISIN을 찾는다.
- `KOSPI/KOSDAQ` market filter가 필요한 경우 `marketName` 또는 `marketCode`를 함께 검증한다.
- cache miss는 명시적 `SecurityFlowFetchResult(error=...)`로 반환한다.

향후 task (이번 범위 밖):

- 운영 환경에서 짧은 CLI 호출이 반복되면 매 invocation마다 finder를 다시 부르게 된다. 충분히 안정화되면 `stock_master`에 ISIN 컬럼을 추가하고 `dart sync-corp` / `universe sync` 단계에서 영속화하는 별도 task로 분리한다.

### `KrxDirectFlowProvider`

`FlowProvider`를 구현한다.

- `fetch_investor_net_volume(ticker, market, start, end)`
  - resolver로 ISIN 조회
  - `MDCSTAT02302` 호출
  - 3개 순매수 metric 생성
- `fetch_foreign_holding_shares(trade_date, market, tickers)`
  - `MDCSTAT03701` 호출
  - ticker allowlist 적용
  - `foreign_holding_shares` 생성
- `fetch_shorting_metrics(ticker, market, start, end)`
  - resolver로 ISIN 조회
  - `MDCSTAT30001`, `MDCSTAT30502` 호출
  - 날짜 기준 merge 후 3개 shorting metric 생성
- `unsupported_metric_codes()`
  - 기존처럼 `["borrow_balance_quantity"]`
- `source()`
  - `Source.KRX`

### parser 전략

pykrx처럼 DataFrame으로 변환한 뒤 한글 column을 붙이는 대신, KRX JSON dict를 바로 parse한다.

공통 util:

- 쉼표 제거: `"1,234" -> Decimal("1234")`
- 공백/빈 문자열/`"-"` 처리: 값 없음은 `None`, 수치 fallback이 필요한 metric은 skip
- 날짜 parse: `YYYY/MM/DD`, `YYYYMMDD` 모두 허용
- ticker normalize: `zfill(6)`

기존 `flows_pykrx.provider` parser와 직접 parser의 의미가 달라지면 안 된다. 가능하면 metric 생성 로직은 `flows_common` 모듈로 옮기고, pykrx adapter와 KRX adapter가 같은 mapping 상수를 쓰게 한다.

단, `raw_payload` 스키마는 의도적으로 다르다.

- pykrx parser: `{"kind": "...", "row": <Korean column dict>}`
- KRX direct parser: `{"source_bld": "...", "request": {...}, "row": <raw English column dict>}`

테스트 비교는 `metric_code`, `metric_name`, `value`, `unit`, `trade_date`, `ticker`, `market`, `source` 까지로 한정한다. `raw_payload`는 fixture별 expected snapshot으로 따로 검증한다.

### 공매도 fallback 정책

- 1차 정책: `MDCSTAT30001`(종합)과 `MDCSTAT30502`(잔고)를 `trade_date` 기준으로 outer merge.
- `short_selling_balance_quantity`는 다음 우선순위로 결정한다.
  1. `MDCSTAT30502.BAL_QTY`가 있으면 그 값을 사용.
  2. 없으면 같은 날짜의 `MDCSTAT30001.STR_CONST_VAL1`로 fallback.
  3. 둘 다 없으면 해당 날짜의 balance metric은 skip(`None`을 저장하지 않음).
- 이 fallback은 기존 pykrx parser가 `balance:공매도잔고` → `status:잔고수량` 순으로 사용하던 동작과 정확히 동일하다.
- 기록할 때 `raw_payload`에 어느 source endpoint에서 값을 가져왔는지(`source_bld`)를 함께 남겨 추후 검증을 용이하게 한다.

## 서비스/CLI 변경 계획

### 1. `FlowProvider` source 확장

`src/krx_collector/ports/flows.py`에 provider source를 노출한다.

```python
def source(self) -> Source:
    ...
```

기존 `PykrxFlowProvider.source()`는 `Source.PYKRX`, 새 `KrxDirectFlowProvider.source()`는 `Source.KRX`를 반환한다.

### 2. `sync_krx_security_flows()` source 고정 제거

현재 skip count가 `Source.PYKRX`로 고정되어 있다. 이를 `provider.source()`로 바꾼다.

변경 대상:

- foreign holding existing count
- investor metric existing count
- shorting metric existing count

주의:

- unique key에 source가 포함되어 있으므로 `Source.KRX`로 처음 실행하면 기존 `Source.PYKRX` rows를 재사용하지 않고 새 rows를 적재한다.
- 이 provenance 분리는 의도적으로 유지한다. 기존 PYKRX rows와 직접 KRX rows를 비교 검증할 수 있기 때문이다.

### 3. CLI provider 선택

`flows sync`에 provider 선택 옵션을 추가한다.

```bash
uv run krx-collector flows sync --provider krx --tickers 005930 --start 2026-04-17 --end 2026-04-17
uv run krx-collector flows sync --provider pykrx --tickers 005930 --start 2026-04-17 --end 2026-04-17
```

권장 기본값:

- 1차 merge: 기본값은 **기존과 동일하게 `pykrx`** 로 유지한다. CLI는 `--provider krx`를 새 옵션으로만 추가한다.
- 별도 후속 PR에서 live smoke가 통과되고 `Source.KRX` rows의 metric/value가 기존 `Source.PYKRX` rows와 동등한 것을 확인한 뒤 default를 `krx`로 전환한다.
- `pykrx` provider는 fallback 검증/대조용으로 당분간 유지한다.

출력에는 provider/source를 표시한다.

## 구현 단계

0. 사전 curl 스파이크 (구현 시작 전 30분 이내)
   - 비로그인 session으로 `MDCSTAT02302`, `MDCSTAT03701`, `MDCSTAT30001`, `MDCSTAT30502`, `finder_stkisu`를 각 1회씩 호출한다.
   - 응답이 정상 JSON인지 / 401·403·로그인 redirect·"권한 없음" 류 메시지인지 확인하고 결과를 본 plan 문서 하단에 한 줄씩 기록한다.
   - 비로그인이 막히는 endpoint가 있으면 1단계 진입 전에 KRX login 흐름을 client 설계에 포함시킨다. (이 단계가 끝나기 전에는 `KrxMdcClient` 구현을 시작하지 않는다.)

1. 공통 parser/mapping 정리
   - metric code/name/unit mapping을 `flows_common` 또는 새 `flows_krx/parsers.py`에 명시한다.
   - 직접 parser가 `Source.KRX`를 넣도록 한다.

2. KRX HTTP client 구현
   - `KrxMdcClient.post_json(bld, params, output_key)` 구현
   - timeout, status, JSON decode, output key 검증
   - 730일 chunk helper 구현 (적용 대상은 위 client 절 참조)

3. ticker→ISIN resolver 구현
   - `finder_stkisu` 직접 호출
   - ticker, name, isin, market code cache
   - resolver 단위 테스트 작성

4. `KrxDirectFlowProvider` 구현
   - investor, foreign holding, shorting 세 경로 구현
   - raw payload에 `source_bld`, `request`, `row` 저장

5. 서비스/CLI 연결
   - `FlowProvider.source()` 추가
   - `sync_krx_security_flows()`의 `Source.PYKRX` 고정 제거
   - CLI `--provider krx|pykrx` 추가 (default는 `pykrx` 유지)

6. 테스트/검증
   - unit test: 각 KRX endpoint raw JSON fixture → expected `SecurityFlowLine`
   - unit test: service skip count가 provider source를 사용하는지 확인
   - unit test: CLI parser가 `--provider`를 처리하는지 확인
   - optional live smoke:
     ```bash
     RUN_KRX_FLOW_LIVE=1 uv run pytest -q tests/integration/test_flows_krx_live.py -s
     uv run krx-collector flows sync --provider krx --tickers 005930 --start 2026-04-17 --end 2026-04-17
     ```

7. 문서 갱신
   - README와 `docs/operations.md`의 수급 raw 설명을 `pykrx / KRX`에서 직접 KRX 기본으로 갱신
   - `borrow_balance_quantity`는 계속 pending으로 명시

## 테스트 fixture 기준

테스트가 비교하는 필드는 `metric_code`, `metric_name`, `value`, `unit`, `trade_date`, `ticker`, `market`, `source` 까지로 한정한다. `raw_payload`는 provider별 스키마가 다르므로 fixture별 expected snapshot으로 따로 검증한다.

최소 fixture:

- `foreign_holding_all_kospi_20260417.json`
  - `output`에 `ISU_SRT_CD=005930`, `FORN_HD_QTY` 포함
- `investor_ticker_005930_20260417.json`
  - `output`에 `TRD_DD`, `TRDVAL1`, `TRDVAL3`, `TRDVAL4` 포함
- `shorting_status_005930_20260417.json`
  - `OutBlock_1`에 `CVSRTSELL_TRDVOL`, `CVSRTSELL_TRDVAL`, `STR_CONST_VAL1` 포함
- `shorting_balance_005930_20260417.json`
  - `OutBlock_1`에 `RPT_DUTY_OCCR_DD`, `BAL_QTY` 포함
- `finder_stkisu.json`
  - `block1`에 `short_code=005930`, `full_code=KR7005930003`

각 fixture test는 위에 정의된 비교 필드 범위(`metric_code`/`metric_name`/`value`/`unit`/`trade_date`/`ticker`/`market`/`source`)에서 기존 pykrx parser test와 동등한 결과를 기대해야 한다.

## 리스크와 대응

- KRX output key가 `output`/`OutBlock_1`로 화면마다 다르다.
  - client에서 output key를 명시하고, 없을 때 bld와 응답 일부를 포함한 예외를 낸다.
- KRX 숫자 문자열이 `"-"`, 빈 문자열, comma 포함 문자열로 온다.
  - parser util에서 일괄 정규화한다.
- 기간 endpoint가 긴 기간에서 실패할 수 있다.
  - pykrx와 동일하게 730일 chunk를 적용한다.
- 직접 KRX source로 기록하면 기존 PYKRX rows와 중복 trade_date/metric이 생긴다.
  - unique key가 source를 포함하므로 데이터 무결성 문제는 없다.
  - 운영 전환 후 같은 source로 skip이 동작하도록 provider source 기반 count만 보장한다.
  - **첫 KRX 실행 비용**: skip 임계값이 source별로 카운트되므로, `--provider krx` 첫 실행은 PYKRX rows가 있어도 모두 신규 fetch한다. 백필 범위 × 종목 수 × metric 7종 만큼의 신규 upsert가 발생한다 (예: 2,500종목 × 200거래일 × 7 ≈ 350만 행, KRX 요청량도 그만큼 늘어난다). 1차 운영 전환은 좁은 ticker/날짜 범위로 단계적으로 진행한다.
- KRX가 로그인/세션을 요구할 수 있다.
  - 1차는 비로그인 session + warm-up, 필요 시 자체 login flow를 추가한다.
  - 가정이 깨지면 1차 PR이 통째로 막히므로, "구현 단계 0. 사전 curl 스파이크"에서 endpoint 5개의 비로그인 응답을 먼저 확인한 뒤 본 구현에 들어간다.
- 대차잔고(`borrow_balance_quantity`)는 이번 조사 범위에서 경로가 없다.
  - 직접 KRX 구현 1차 범위에 넣지 않고 별도 endpoint 조사 task로 분리한다.

## 완료 기준

- `flows sync --provider krx`가 pykrx import 없이 실행된다.
- `005930`, 단일 거래일 smoke에서 기존 metric 7종이 `Source.KRX`로 upsert된다.
- unit test에서 KRX raw JSON fixture가 기존 pykrx parser와 같은 metric/value를 생성한다.
- `sync_krx_security_flows()`가 `Source.PYKRX`에 고정되지 않고 provider source로 skip count를 수행한다.
- 기존 `--provider pykrx` fallback 경로도 테스트가 유지된다.

## 사전 endpoint 스파이크 결과

확인일: 2026-04-26

- `MDCSTAT02302` 투자자별 순매수: 비로그인 단발 호출 `LOGOUT`, warm-up cookie 후에도 `LOGOUT`.
- `MDCSTAT03701` 외국인 보유주식수: 비로그인 단발 호출 `LOGOUT`.
- `MDCSTAT30001` 공매도 종합: 비로그인 단발 호출 `LOGOUT`.
- `MDCSTAT30502` 공매도 잔고: 비로그인 단발 호출 `LOGOUT`.
- `finder_stkisu` 종목 finder: 비로그인 호출 정상 JSON 응답.

구현 반영:

- 직접 KRX client는 `LOGOUT`/login HTML을 `KrxMdcAuthenticationError`로 명시 변환한다.
- `settings.krx_id` / `settings.krx_pw`가 있으면 직접 KRX client가 자체 login flow를 수행하고 1회 재시도한다.
- KRX duplicate-login 응답(`CD011`)은 `skipDup=Y`로 재시도한다.
- `005930`, `2026-04-17` live provider smoke에서 7개 metric 코드가 모두 반환됨을 확인했다.
