# Raw Feature Inventory — 수집된 원천 피쳐 조사

- 작성일: 2026-07-31
- 데이터 기준: `data_lake/raw_postgres/snapshot_date=2026-07-30/source=sj2_remote` (route=remote, sj2 prod DB 직접 캡처)
- 목적: 유용 피쳐 발굴(feature discovery)에 앞서, 현재 raw 레이어에 **무엇이, 어느 기간/커버리지로** 수집돼 있는지 전수 조사.
- 코드 기준: `src/krx_collector/definitions/` (metric_rules.py, common_features.py), `research/etl/marts/`

## 1. 전체 요약

스냅샷에는 13개 테이블이 있고, 실질적 피쳐 원천은 5개 도메인이다.

| 도메인 | 테이블 | 행 수 | 기간 | 종목/시리즈 수 | 크기 |
|---|---|---|---|---|---|
| 주가 | `daily_ohlcv` | 6,647,335 | 2007-06-05 ~ 2026-07-30 | 2,792 tickers | 115MB |
| 수급/공매도 | `krx_security_flow_raw` | 77,030,743 | 2007-06-05 ~ 2026-07-24 | 2,787 tickers × 7 metric | 2.3GB |
| 시장/매크로 | `common_feature_observation_raw` | ~76k | 2013-05-31 ~ 2026-07-30 | 26개 active 시리즈 | 2.2MB |
| 재무 (DART) | `dart_financial_statement_raw` | 16,887,271 | 사업연도 2015 ~ 2026 | ~2,600 tickers | 883MB |
| 재무 (XBRL) | `dart_xbrl_fact_raw` | 84,051,816 | 사업연도 2015 ~ 2026 | 2,608 tickers, 954,138 concepts | 2.5GB |
| 주식수 (DART) | `dart_share_count_raw` | 347,221 | 사업연도 2015 ~ 2026 | 2,653 tickers | 19MB |
| 주주환원 (DART) | `dart_shareholder_return_raw` | 8,647,696 | 사업연도 2015 ~ 2026 | 2,653 tickers | 95MB |
| 마스터/메타 | `stock_master`(2,792), `stock_master_snapshot`(55)/`_items`(150,404), `dart_corp_master`(116,503), `dart_xbrl_document`(83,648), `common_feature_series`(33) | | | | |

파티셔닝: 시계열 테이블은 `schema_version=1/year=YYYY/month=MM/`, DART는 `schema_version=1/bsns_year=YYYY/`. `_manifests/_SUCCESS.json`에 테이블별 rows_exported/schema_hash가 기록되어 있고 위 행 수와 일치함을 확인했다.

## 2. 도메인별 상세

### 2.1 daily_ohlcv — 일봉 (pykrx)

컬럼: `trade_date, ticker, market, open, high, low, close, volume, source, fetched_at` (가격은 KRW 정수).

| market | 행 수 | tickers | 기간 |
|---|---|---|---|
| KOSDAQ | 4,007,428 | 1,842 | 2007-06-05 ~ 2026-07-30 |
| KOSPI | 2,639,907 | 950 | **2014-01-20** ~ 2026-07-30 |

- **KOSPI는 2014-01-20부터만 존재** (KOSDAQ은 2007부터). 2014 이전 구간을 쓰는 피쳐는 KOSDAQ 편향이 생기므로 주의. 반면 flows는 양 시장 모두 2007부터 있어 비대칭.
- 거래정지 흔적: volume=0 행 143,550건(2.2%), open=0 행 109,256건. 수익률 계산 시 필터/포워드필 정책 필요.
- 시가총액·상장주식수는 이 테이블에 없음 (주식수는 DART `share_count`/XBRL에서, 근사 시총은 close×발행주식수로 유도해야 함).

### 2.2 krx_security_flow_raw — 투자자별 수급 + 공매도 (KRX MDC)

종목·일 단위 long 포맷(`metric_code`, `value`, `unit`, `raw_payload`). 7개 metric:

| metric_code | 이름 | 단위 | 행 수 | tickers | 기간 |
|---|---|---|---|---|---|
| foreign_net_buy_volume | 외국인 순매수 수량 | shares | 12,802,863 | 2,782 | 2007-06-05 ~ 2026-07-24 |
| institution_net_buy_volume | 기관 순매수 수량 | shares | 12,802,863 | 2,782 | 〃 |
| individual_net_buy_volume | 개인 순매수 수량 | shares | 12,802,863 | 2,782 | 〃 |
| foreign_holding_shares | 외국인 보유주식수 | shares | 17,503,108 | 2,787 | 〃 |
| short_selling_volume | 공매도 거래량 | shares | 8,034,455 | 2,780 | 〃 |
| short_selling_value | 공매도 거래대금 | KRW | 8,034,455 | 2,780 | 〃 |
| short_selling_balance_quantity | 공매도 잔고 수량 | shares | 5,050,136 | 2,779 | **2016-06-30** ~ 2026-07-22 |

- 공매도 잔고는 공시제도 시작(2016-06) 이후만 존재.
- **최신일이 2026-07-24에서 멈춰 있음** — KRX MDC 계정 비밀번호 만료(CD010)로 `sdc_daily_krx_common`이 실패 중인 것과 일치 (수동 조치 필요, 코드 문제 아님).
- 이 테이블은 **현재 canonical metric 정규화(metric_rules)에 전혀 매핑돼 있지 않음** → 종목 단위 수급/공매도 피쳐는 사실상 미개발 상태의 가장 큰 원천.
- 파생 가능 예: 순매수 수량은 있으나 **금액 기준 순매수는 미수집** (수량 × 당일 가격으로 근사 필요), 외국인 보유비율(보유주식수 ÷ 발행주식수), 공매도 비중(공매도 거래량 ÷ 총 거래량), 잔고회전일수 등.

### 2.3 common_feature_observation_raw — 시장/매크로 공통 시계열

PIT 정합성을 위한 `observation_date / period_end_date / release_date / available_from_date / vintage` 컬럼 보유. `common_feature_series` 카탈로그 33개 중 **active 26개**가 수집됨 (inactive 7개: pykrx 지수 3종 폴백, KRX 산업지수 4종 후보 — 데이터 없음).

| 그룹 | series (n=26) | 빈도 | 기간 |
|---|---|---|---|
| 시장지수 (KRX direct) | market_kospi_krx, market_kosdaq_krx, market_kospi200_krx | D | 2014-06-13 ~ 2026-07-24 |
| 시장 폭(breadth) | kospi/kosdaq × advancers/decliners/unchanged (6종) | D | 〃 |
| 시장 유동성 | kospi/kosdaq turnover_value (2종) | D | 〃 |
| 글로벌 | global_sp500, global_nasdaq, global_vix (FDR) | D | 2014-06-13 ~ 2026-07-29/30 |
| 환율 | fx_usdkrw (FDR), fx_usdkrw_ecos (ECOS 매매기준율) | D | 2014-06 ~ 2026-07-30 |
| 원자재 | commodity_wti (FDR CL=F 선물), commodity_wti_fred (FRED 현물) | D | 2014-06 ~ 2026-07-27/30 |
| 금리 | rate_kr_gov3y/10y (ECOS), rate_us2y/10y (FRED) | D | 2014-06-13 ~ 2026-07-28/30 |
| 매크로(월) | macro_cpi, macro_ppi, macro_m2, macro_consumer_sentiment (ECOS) | M | 2013-05-31 ~ 2026-05/07 |

- 일간 시리즈는 대부분 **2014-06-13 시작** (약 12년). KRX direct 시리즈(지수/breadth/turnover)는 flows와 동일하게 2026-07-24에서 멈춤(위 KRX 계정 이슈).
- 모델 노출 피쳐 카탈로그(`definitions/common_features.py`)는 54개 정의 중 active 37개: 지수 수익률(1/5/20d), breadth/turnover level, VIX level, USD/KRW level+5d, WTI 20d(선물·현물 별도), 한/미 금리 level + 기간 스프레드(10y-3y, 10y-2y), CPI/PPI/M2 level+YoY+MoM, 소비자심리 level.

### 2.4 DART 재무 — dart_financial_statement_raw

단일회사 전체 재무제표 API raw. 키: `corp_code/ticker × bsns_year(2015~2026) × reprt_code(11011 사업, 11012 반기, 11013 1Q, 11014 3Q) × fs_div(CFS 연결/OFS 별도) × account`. 당기/전기/전전기 금액(`thstrm/frmtrm/bfefrmtrm_amount`)과 분기 누적(`_add_amount`) 컬럼 보유.

- 2015년은 사업보고서(11011)만; **2016년부터 분기·반기 전체** 수집.
- **2025년부터 OFS(별도)가 없음** — CFS만 수집됨. 별도재무제표 기반 피쳐는 2024까지만 일관됨.
- 2026년은 1Q(11013, 2,080 tickers)가 주 수집분이고 11011/11012/11014은 극소수(이월 공시).
- 계정 표준화 수준: 표준 `account_id`(ifrs-full/ifrs/dart 접두) 14.4M행(85%), 비표준(`-표준계정코드 미사용-` 등) 2.5M행(15%).
- sj_div 분포: CF 6.2M, BS 6.0M, CIS 3.2M, SCE 1.2M, IS 0.25M — **현금흐름표·재무상태표가 가장 두터움**.
- 커버리지가 넓은 핵심 계정(2,500+ 종목): ProfitLoss, Equity, Assets, Liabilities, 영업이익, 매출액, 3대 현금흐름, 유형자산, 재고자산, EPS 등 — §3의 canonical metric 29종이 이 위에 매핑됨.

### 2.5 dart_xbrl_fact_raw / dart_xbrl_document

XBRL 인스턴스 문서(83,648건)에서 추출한 fact 84M행. `concept_id, namespace, context(duration/instant), period_start/end, instant_date, dimensions(JSON), unit, value_numeric/text, label_ko`.

- distinct concept 954,138개 — 대부분 기업별 확장 태그(entity-specific). 표준 ifrs-full 서브셋으로 좁혀 써야 함.
- duration context 57.8M(수치 50.2M), instant 26.3M(수치 26.3M).
- 연도별로 2024(12.2M)·2025(18.8M)에 급증 — 최근 연도가 훨씬 조밀함. 2015년은 1.3M/1,586 tickers로 얇음.
- 현재 metric_rules에서 11개 rule만 사용(가중평균/희석주식수, 감가상각비, 무형자산상각비 등) — **세부 계정(부문별 매출, 판관비 내역, 리스, 충당금 등) 발굴 여지가 가장 큰 테이블**. 단, 스캔 비용(2.5GB)과 개념 표준화 작업이 필요.

### 2.6 dart_share_count_raw / dart_shareholder_return_raw

- `share_count` (347k행, 2,653 tickers): 주식 종류(`se`: 보통주/우선주/합계 등)별 발행총수(`istc_totqy`), 자기주식(`tesstk_co`), 유통주식(`distb_stock_co`), 감소 내역. canonical `issued_shares`/`treasury_shares`의 원천.
- `shareholder_return` (8.6M행): 두 statement_type.
  - `dividend` (3.5M): row_name에 **주당 현금배당금, 현금배당수익률, 현금배당성향(연결), 배당금총액, 주당순이익, 액면가** 등 — 당기/전기/전전기(thstrm/frmtrm/lwfr) × 주식종류(stock_knd).
  - `treasury_stock` (5.2M): 자기주식 기초/기말수량, 취득/처분/소각 변동수량 — 취득방법·목적 차원(dim1~3) 포함.
  - 현재 canonical에는 `dps` 1개만 매핑 — 배당성향·배당수익률·자사주 매입/소각 강도 등이 미사용 상태.

### 2.7 마스터/메타 테이블

- `stock_master`: 2,792 종목 (ACTIVE 2,764 = KOSPI 943 + KOSDAQ 1,821; DELISTED 28). `listing_date`, `first/last_seen_date` 보유. **DELISTED가 28개뿐** → 유니버스가 현존 상장사 중심이라 장기 백테스트에는 생존 편향(survivorship bias) 존재.
- `stock_master_snapshot`(55회) + `_items`(150k): 유니버스 동기화 시점별 소속 스냅샷 — PIT 유니버스 구성에 활용 가능하나 스냅샷 시작 시점 이후만 가능.
- `dart_corp_master`: 116,503 법인 중 ticker 매핑 3,959건.

## 3. 코드 정의 레이어 (raw → canonical 매핑 현황)

`definitions/metric_rules.py`: **catalog 29개 metric, mapping rule 67개** (fs_raw 52, xbrl 11, share_count 2, shareholder_return 2).

- financial(25): revenue, cogs, gross_profit, sga, operating_income, net_income, controlling_net_income, total_assets/liabilities/equity, cash_and_cash_equivalents, 3대 현금흐름, interest_received/paid, dividends_paid, capex_ppe/intangible, 장기차입 조달/상환, treasury_share_acquisition_amount, depreciation/amortization
- share_count(2): issued_shares, treasury_shares / shareholder_return(1): dps / xbrl(2): weighted_avg_shares, diluted_shares

이들은 `research/etl/marts/metrics_normalize.py`가 DuckDB로 `stock_metric_fact`를 재계산하는 데 쓰이고, 공통 피쳐는 `marts/common_build.py`가 `common_feature_daily_fact`로 빌드한다 (파리티는 `tests/unit/golden/*.json`에 고정).

## 4. 품질/주의사항 정리 (피쳐 연구 시 체크리스트)

1. **KRX 소스 정지**: flows·KRX direct 시리즈 최신일 2026-07-24 (KRX 비밀번호 만료, 수동 조치 대기). OHLCV/FDR/ECOS/FRED는 정상(~2026-07-30).
2. **KOSPI OHLCV는 2014-01-20 시작**, KOSDAQ은 2007 시작 — 2014 이전 학습 구간은 시장 편향.
3. 공매도 잔고는 2016-06-30부터.
4. DART: 2015년은 연간만, 2025년부터 OFS 없음(CFS만), 2026년은 1Q까지.
5. OHLCV zero-volume 2.2% (거래정지) — 수익률·유동성 피쳐 계산 시 처리 필요.
6. XBRL concept 95만 개 중 표준 태그는 소수 — 표준 서브셋 필터가 선행돼야 함. FS raw도 15%는 비표준 계정명 매칭 필요.
7. 상폐 종목이 28개뿐인 유니버스 → 생존 편향. 장기 성과 검증 시 감안.
8. 매크로 월간 시리즈는 period-end + 20일 보수적 availability 정책 — PIT 컬럼(`available_from_date`)을 반드시 사용.

## 5. 피쳐 발굴 관점 제언 (다음 단계 후보)

raw 대비 canonical 활용도를 기준으로 본 미개발(untapped) 순위:

1. **krx_security_flow_raw (77M행, 완전 미사용)** — 외국인/기관/개인 순매수 모멘텀(5/20/60d 누적, 시총·거래량 정규화), 외국인 보유비율 및 변화, 공매도 비중·잔고 변화. 종목 단위 cross-sectional 피쳐로 즉시 후보.
2. **dart_shareholder_return_raw** — 배당성향/배당수익률(raw에 이미 존재), 자사주 취득/소각 강도(취득변동수량 ÷ 발행주식수), 주주환원 합산율.
3. **dart_financial_statement_raw 미매핑 계정** — 판관비 세부, 금융수익/비용, 재고·매출채권(운전자본 변화), 포괄손익 항목 등. 표준 account_id 기준 상위 계정부터.
4. **dart_xbrl_fact_raw 표준 서브셋** — 분기 duration fact 기반의 정밀 기간 매칭 재무 피쳐, 주석 수준 항목. 비용 대비 효과는 3번 이후 평가 권장.
5. **breadth/매크로 조합** — 이미 카탈로그화된 37개 active 공통 피쳐 위에 레짐(advance-decline ratio, term spread 부호 등) 파생.

## 부록: 재현 쿼리

프로파일링은 DuckDB CLI(1.5.4)로 수행. 예:

```sql
-- 스냅샷 루트에서
SELECT metric_code, count(*), count(DISTINCT ticker), min(trade_date), max(trade_date)
FROM read_parquet('krx_security_flow_raw/**/*.parquet') GROUP BY 1;

SELECT series_id, any_value(source), count(*), min(observation_date), max(observation_date)
FROM read_parquet('common_feature_observation_raw/**/*.parquet') GROUP BY 1 ORDER BY 1;

SELECT bsns_year, reprt_code, fs_div, count(*), count(DISTINCT ticker)
FROM read_parquet('dart_financial_statement_raw/**/*.parquet') GROUP BY 1,2,3 ORDER BY 1,2,3;
```
