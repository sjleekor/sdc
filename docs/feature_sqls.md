# 데이터 피쳐 및 조회 SQL 가이드

본 문서는 `stock_data_collector` 프로젝트에서 수집하는 데이터(피쳐) 카테고리별 설명과 이를 데이터베이스에서 직접 조회해 볼 수 있는 샘플 SQL 문을 제공합니다.

데이터베이스 접속 기본 명령: `psql -d krx_data`

## 1. 기본 주식 및 메타 데이터 (Stock Master)

종목의 기본 식별 정보, 상장 상태, 그리고 재무제표 수집을 위한 OpenDART 고유번호 맵핑 정보를 확인합니다.

```sql
-- 특정 종목(예: 삼성전자 '005930')의 마스터 및 DART 고유코드 조회
SELECT s.ticker,
       s.name      AS stock_name,
       s.market,
       s.status,
       d.corp_code,
       d.corp_name AS dart_corp_name
FROM stock_master s
         LEFT JOIN dart_corp_master d ON s.ticker = d.ticker
WHERE s.ticker = '005930';
```

| ticker | stock_name | market | status | corp_code | dart_corp_name |
|--------|------------|--------|--------|-----------|----------------|
| 005930 | 삼성전자       | KOSPI  | ACTIVE | 00126380  | 삼성전자           |

## 2. 일별 시세 데이터 (Daily Price & Volume)

종목의 일별 시가, 고가, 저가, 종가 및 거래량 데이터를 조회합니다. 수익률, 이동평균, 변동성 피쳐 생성의 기본 데이터입니다.

```sql
-- 최근 30일간의 일별 OHLCV 및 전일 대비 등락률 조회 (삼성전자 기준)
SELECT trade_date,
       ticker,
       open,
       high,
       low,
       close,
       volume,
       -- 전일 대비 종가 등락률 계산 예시
       ROUND(((close::numeric / LAG(close) OVER (ORDER BY trade_date ASC)) - 1) * 100, 2) AS return_pct
FROM daily_ohlcv
WHERE ticker = '005930'
ORDER BY trade_date DESC
LIMIT 30;
```

*(주의: 윈도우 함수 사용 시 시간순 정렬을 위해 서브쿼리를 사용하거나 위와 같이 사용할 경우 조회된 결과 내에서만 계산됩니다. 전체 데이터 기준 계산 후 LIMIT을 걸려면 서브쿼리를 권장합니다.)*

| trade_date | ticker | open   | high   | low    | close  | volume   | return_pct |
|------------|--------|--------|--------|--------|--------|----------|------------|
| 2026-05-21 | 005930 | 291000 | 299500 | 287000 | 299500 | 36133485 | 8.51       |
| 2026-05-20 | 005930 | 278000 | 282500 | 263500 | 276000 | 35662077 | 0.18       |
| 2026-05-19 | 005930 | 274000 | 281500 | 266000 | 275500 | 30767569 | -1.96      |
| 2026-05-18 | 005930 | 269500 | 288500 | 262000 | 281000 | 33555214 | 3.88       |
| 2026-05-15 | 005930 | 291000 | 296500 | 266000 | 270500 | 38075487 | -8.61      |
| 2026-05-14 | 005930 | 282000 | 299500 | 282000 | 296000 | 39314752 | 4.23       |
| 2026-05-13 | 005930 | 264000 | 285500 | 262000 | 284000 | 35540134 | 1.79       |
| 2026-05-12 | 005930 | 290000 | 291500 | 266000 | 279000 | 41211149 | -2.28      |
| 2026-05-11 | 005930 | 284500 | 288500 | 280000 | 285500 | 36031094 | 6.33       |
| 2026-05-08 | 005930 | 260000 | 270000 | 260000 | 268500 | 25875880 | -1.1       |
| 2026-05-07 | 005930 | 272000 | 277000 | 260000 | 271500 | 41404687 | 2.07       |
| 2026-05-06 | 005930 | 254000 | 270000 | 251000 | 266000 | 53097996 | 14.41      |
| 2026-05-04 | 005930 | 228000 | 232500 | 224000 | 232500 | 32920816 | 5.44       |
| 2026-04-30 | 005930 | 229000 | 230000 | 220500 | 220500 | 22161975 | -2.43      |
| 2026-04-29 | 005930 | 219500 | 228000 | 218500 | 226000 | 20363756 | 1.8        |
| 2026-04-28 | 005930 | 224000 | 226000 | 221500 | 222000 | 18444490 | -1.11      |
| 2026-04-27 | 005930 | 220000 | 226000 | 218500 | 224500 | 22870374 | 2.28       |
| 2026-04-24 | 005930 | 224000 | 225000 | 216500 | 219500 | 19165257 | -2.23      |
| 2026-04-23 | 005930 | 223000 | 229500 | 216000 | 224500 | 33874721 | 3.22       |
| 2026-04-22 | 005930 | 218500 | 222500 | 215500 | 217500 | 16732867 | -0.68      |
| 2026-04-21 | 005930 | 218000 | 220000 | 216000 | 219000 | 16705245 | 2.1        |
| 2026-04-20 | 005930 | 214500 | 219000 | 213000 | 214500 | 16445128 | -0.69      |
| 2026-04-17 | 005930 | 217000 | 218000 | 215000 | 216000 | 15537867 | -0.69      |
| 2026-04-16 | 005930 | 212000 | 218000 | 210500 | 217500 | 21499788 | 3.08       |
| 2026-04-15 | 005930 | 215000 | 215500 | 210000 | 211000 | 24092884 | 2.18       |
| 2026-04-14 | 005930 | 208000 | 210000 | 205500 | 206500 | 23672078 | 2.74       |
| 2026-04-13 | 005930 | 198200 | 203000 | 198200 | 201000 | 19603415 | -2.43      |
| 2026-04-10 | 005930 | 208500 | 211000 | 205500 | 206000 | 18229163 | 0.98       |
| 2026-04-09 | 005930 | 207000 | 207500 | 202000 | 204000 | 42320839 | -3.09      |
| 2026-04-08 | 005930 | 214000 | 214500 | 207500 | 210500 | 35890973 | 7.12       |

## 3. 수급 및 공매도 지표 (Security Flows)

투자자별 거래 동향(외국인, 기관 등), 외국인 보유 비중, 공매도 거래 및 잔고 등 가격 모멘텀에 영향을 미치는 수급 피쳐를 조회합니다.

```sql
-- 최신 일자 기준 종목의 수급 및 공매도 관련 지표 전체 조회
SELECT trade_date,
       metric_code,
       metric_name,
       value AS metric_value,
       unit
FROM krx_security_flow_raw
WHERE ticker = '005930'
ORDER BY trade_date DESC, metric_code
LIMIT 50;
```

| trade_date | metric_code                    | metric_name | metric_value      | unit   |
|------------|--------------------------------|-------------|-------------------|--------|
| 2026-05-21 | foreign_holding_shares         | 외국인 보유주식수   | 2825334465.0000   | shares |
| 2026-05-18 | foreign_holding_shares         | 외국인 보유주식수   | 2846384725.0000   | shares |
| 2026-05-14 | foreign_holding_shares         | 외국인 보유주식수   | 2854564675.0000   | shares |
| 2026-05-13 | foreign_holding_shares         | 외국인 보유주식수   | 2859644172.0000   | shares |
| 2026-05-11 | foreign_holding_shares         | 외국인 보유주식수   | 2876884902.0000   | shares |
| 2026-05-08 | foreign_holding_shares         | 외국인 보유주식수   | 2888080689.0000   | shares |
| 2026-05-07 | foreign_holding_shares         | 외국인 보유주식수   | 2900260708.0000   | shares |
| 2026-05-06 | foreign_holding_shares         | 외국인 보유주식수   | 2886478875.0000   | shares |
| 2026-05-04 | foreign_holding_shares         | 외국인 보유주식수   | 2878971983.0000   | shares |
| 2026-04-30 | foreign_holding_shares         | 외국인 보유주식수   | 2880255430.0000   | shares |
| 2026-04-29 | foreign_holding_shares         | 외국인 보유주식수   | 2875735982.0000   | shares |
| 2026-04-28 | foreign_holding_shares         | 외국인 보유주식수   | 2879307377.0000   | shares |
| 2026-04-24 | foreign_holding_shares         | 외국인 보유주식수   | 2878653125.0000   | shares |
| 2026-04-24 | foreign_holding_shares         | 외국인 보유주식수   | 2878653125.0000   | shares |
| 2026-04-24 | foreign_net_buy_volume         | 외국인 순매수 수량  | -4887720.0000     | shares |
| 2026-04-24 | foreign_net_buy_volume         | 외국인 순매수 수량  | -4887720.0000     | shares |
| 2026-04-24 | individual_net_buy_volume      | 개인 순매수 수량   | 4827781.0000      | shares |
| 2026-04-24 | individual_net_buy_volume      | 개인 순매수 수량   | 4827781.0000      | shares |
| 2026-04-24 | institution_net_buy_volume     | 기관 순매수 수량   | -66031.0000       | shares |
| 2026-04-24 | institution_net_buy_volume     | 기관 순매수 수량   | -66031.0000       | shares |
| 2026-04-24 | short_selling_value            | 공매도 거래대금    | 212089951250.0000 | KRW    |
| 2026-04-24 | short_selling_volume           | 공매도 거래량     | 967743.0000       | shares |
| 2026-04-23 | foreign_holding_shares         | 외국인 보유주식수   | 2874108639.0000   | shares |
| 2026-04-23 | foreign_holding_shares         | 외국인 보유주식수   | 2874108639.0000   | shares |
| 2026-04-23 | foreign_net_buy_volume         | 외국인 순매수 수량  | 3298847.0000      | shares |
| 2026-04-23 | foreign_net_buy_volume         | 외국인 순매수 수량  | 3298847.0000      | shares |
| 2026-04-23 | individual_net_buy_volume      | 개인 순매수 수량   | -3920976.0000     | shares |
| 2026-04-23 | individual_net_buy_volume      | 개인 순매수 수량   | -3920976.0000     | shares |
| 2026-04-23 | institution_net_buy_volume     | 기관 순매수 수량   | 677286.0000       | shares |
| 2026-04-23 | institution_net_buy_volume     | 기관 순매수 수량   | 677286.0000       | shares |
| 2026-04-23 | short_selling_value            | 공매도 거래대금    | 52691301500.0000  | KRW    |
| 2026-04-23 | short_selling_volume           | 공매도 거래량     | 235587.0000       | shares |
| 2026-04-22 | foreign_holding_shares         | 외국인 보유주식수   | 2875135385.0000   | shares |
| 2026-04-22 | foreign_holding_shares         | 외국인 보유주식수   | 2875135385.0000   | shares |
| 2026-04-22 | foreign_net_buy_volume         | 외국인 순매수 수량  | -1304307.0000     | shares |
| 2026-04-22 | foreign_net_buy_volume         | 외국인 순매수 수량  | -1304307.0000     | shares |
| 2026-04-22 | individual_net_buy_volume      | 개인 순매수 수량   | 1950044.0000      | shares |
| 2026-04-22 | individual_net_buy_volume      | 개인 순매수 수량   | 1950044.0000      | shares |
| 2026-04-22 | institution_net_buy_volume     | 기관 순매수 수량   | -653920.0000      | shares |
| 2026-04-22 | institution_net_buy_volume     | 기관 순매수 수량   | -653920.0000      | shares |
| 2026-04-22 | short_selling_balance_quantity | 공매도 잔고 수량   | 85173.0000        | shares |
| 2026-04-22 | short_selling_value            | 공매도 거래대금    | 25500235000.0000  | KRW    |
| 2026-04-22 | short_selling_volume           | 공매도 거래량     | 116964.0000       | shares |
| 2026-04-21 | foreign_holding_shares         | 외국인 보유주식수   | 2873068206.0000   | shares |
| 2026-04-21 | foreign_holding_shares         | 외국인 보유주식수   | 2873068206.0000   | shares |
| 2026-04-21 | foreign_net_buy_volume         | 외국인 순매수 수량  | 1295451.0000      | shares |
| 2026-04-21 | foreign_net_buy_volume         | 외국인 순매수 수량  | 1295451.0000      | shares |
| 2026-04-21 | individual_net_buy_volume      | 개인 순매수 수량   | -2499998.0000     | shares |
| 2026-04-21 | individual_net_buy_volume      | 개인 순매수 수량   | -2499998.0000     | shares |
| 2026-04-21 | institution_net_buy_volume     | 기관 순매수 수량   | 1226321.0000      | shares |

```sql
SELECT DISTINCT metric_code, metric_name
FROM metric_catalog
LIMIT 100;
```

| metric_code                       | metric_name |
|-----------------------------------|-------------|
| gross_profit                      | 매출총이익       |
| cogs                              | 매출원가        |
| capex_intangible                  | 무형자산 취득액    |
| diluted_shares                    | 희석주식수       |
| operating_cash_flow               | 영업활동현금흐름    |
| capex_ppe                         | 유형자산 취득액    |
| depreciation_expense              | 감가상각비       |
| investing_cash_flow               | 투자활동현금흐름    |
| borrowing_repayments_long_term    | 장기차입금 상환액   |
| total_assets                      | 총자산         |
| total_equity                      | 총자본         |
| issued_shares                     | 발행주식수       |
| treasury_share_acquisition_amount | 자사주 매입금액    |
| controlling_net_income            | 지배주주순이익     |
| interest_received                 | 이자수익        |
| weighted_avg_shares               | 가중평균주식수     |
| dps                               | 주당 현금배당금    |
| treasury_shares                   | 자기주식수       |
| sga                               | 판매비와관리비     |
| revenue                           | 매출액         |
| financing_cash_flow               | 재무활동현금흐름    |
| cash_and_cash_equivalents         | 현금및현금성자산    |
| total_liabilities                 | 총부채         |
| operating_income                  | 영업이익        |
| net_income                        | 당기순이익       |
| dividends_paid                    | 배당금 지급액     |
| interest_paid                     | 이자비용        |
| borrowing_proceeds_long_term      | 장기차입금 증가액   |
| amortization_intangible_assets    | 무형자산상각비     |

## 4. 기업 재무제표 데이터 (Financial Metrics)

OpenDART에서 수집 후 분석 및 비교가 용이하도록 자체 Rule을 통해 표준화된(Normalized) 주요 재무 지표들을 조회합니다. (매출액, 영업이익, 자산 등) 가치평가(Valuation) 피쳐의
핵심입니다.

```sql
-- 최근 보고서 기준 주요 표준화 재무 지표 트렌드 조회
SELECT 
    bsns_year,
    reprt_code,
    metric_code,
    value_numeric AS value,
    unit,
    period_type,
    period_end
FROM stock_metric_fact
WHERE ticker = '005930'
--  AND metric_code IN ('revenue', 'operating_income', 'net_income') -- 특정 메트릭 필터링 시 사용
ORDER BY bsns_year DESC, reprt_code DESC, metric_code;
```

#### 참고: 원본 재무 데이터 (Raw) 조회
표준화되지 않은 DART 공시 원본 데이터를 직접 확인해야 할 경우 아래 쿼리를 사용합니다.

```sql
-- 1) 재무제표 원본 (fnlttSinglAcntAll 기준)
SELECT 
    bsns_year,
    reprt_code,
    sj_nm,
    account_nm,
    thstrm_amount,
    frmtrm_amount,
    rcept_no
FROM dart_financial_statement_raw
WHERE ticker = '005930'
ORDER BY bsns_year DESC, reprt_code DESC, sj_div, ord;

-- 2) XBRL 원본 팩트 데이터
SELECT 
    bsns_year,
    reprt_code,
    concept_id,
    label_ko,
    value_numeric,
    value_text,
    context_type
FROM dart_xbrl_fact_raw
WHERE ticker = '005930'
ORDER BY bsns_year DESC, reprt_code DESC, concept_id;
```
*(참고: 가공 전 원본 재무/회계 계정을 확인하려면 `dart_financial_statement_raw` 또는 `dart_xbrl_fact_raw` 테이블을 조회해야 합니다.)*

## 5. 주식 수 및 주주환원 데이터 (Share Info & Returns)

시가총액 산출에 필요한 실질 발행 주식 수(총 주식수 - 자사주)와, 배당/자사주 매입 등 주주환원 정책 피쳐 산정을 위한 데이터를 조회합니다.

```sql
-- 1) 가장 최근 사업보고서/분기보고서 기준 보통주 주식 수 현황
SELECT bsns_year,
       reprt_code,
       isu_stock_totqy AS total_issued_shares,
       tesstk_co       AS treasury_shares,
       distb_stock_co  AS distributed_shares
FROM dart_share_count_raw
WHERE ticker = '005930'
  AND se = '보통주'
ORDER BY bsns_year DESC, reprt_code DESC
LIMIT 1;

-- 2) 배당 및 주주환원 정책 관련 지표 조회
SELECT bsns_year,
       reprt_code,
       statement_type,
       metric_name,
       value_numeric,
       unit
FROM dart_shareholder_return_raw
WHERE ticker = '005930'
ORDER BY bsns_year DESC, reprt_code DESC, metric_name
LIMIT 20;
```

## 6. 산업/섹터별 핵심 영업 지표 (Operating KPIs)

방산, 조선 등 특정 산업군 종목의 공시 문서 본문에서 텍스트 마이닝 파이프라인을 통해 추출한 비재무적 핵심 성과 지표(예: 신규 수주액, 수주 잔고 등)를 조회합니다.

```sql
-- 특정 종목의 추출된 영업 지표(KPI) 및 원본 문장(snippet) 확인
SELECT period_end,
       sector_key,
       metric_code,
       metric_name,
       value_numeric,
       unit,
       raw_snippet
FROM operating_metric_fact
WHERE ticker = '005930' -- (실제 추출 공시가 있는 조선/방산 종목 티커로 변경하여 테스트)
ORDER BY period_end DESC, metric_code;
```
