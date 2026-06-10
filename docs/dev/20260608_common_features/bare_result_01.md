아래 기준으로 우선순위를 나누는 것을 권합니다.

1. **전 종목에 동일하게 붙는 시장·거시 공통 피처**
2. **업종/섹터별로 다르게 붙는 공통 피처**
3. **종목별 펀더멘털과 결합해 파생하는 cross-sectional 피처**
4. **실시간 예측에는 금지해야 할 look-ahead 위험 피처**

---

## 1. 가장 먼저 추가할 피처 세트

### A. 시장 대표 지수

현재 개별 종목 OHLCV는 있으나, “시장 전체가 오른 날인가 / 업종이 오른 날인가 / 종목만 오른 날인가”를 분리할 기준축이 부족합니다. 주가 예측에서는 반드시 넣는 편이 좋습니다.

| 피처                    | 주기 | 추천 소스                           | 활용                   |
| --------------------- | -: | ------------------------------- | -------------------- |
| KOSPI, KOSDAQ 종가/수익률  | 일간 | KRX Data Marketplace / 정보데이터시스템 | 시장 베타, 시장 모멘텀        |
| KOSPI200, KOSDAQ150   | 일간 | KRX                             | 대형주/대표지수 민감도         |
| KRX 300, KRX TMI      | 일간 | KRX                             | 전체 시장 proxy          |
| 업종지수                  | 일간 | KRX                             | 업종 relative strength |
| 거래대금, 거래량, 상승/하락 종목 수 | 일간 | KRX                             | market breadth       |
| 투자자별 시장 순매수           | 일간 | KRX                             | 외국인/기관 시장 방향성        |

KRX Data Marketplace는 KOSPI, KOSDAQ, KRX 300, KOSDAQ150 등 주요 지수와 투자자별 매매동향, 시장별 거래대금, 상장종목 현황을 제공하는 공식 시장 데이터 포털입니다. ([한국거래소 데이터][1])

**파생 피처 예시**

```text
kospi_ret_1d
kospi_ret_5d
kospi_ret_20d
kosdaq_ret_1d
market_ret_rolling_20d
market_volatility_20d
stock_ret_minus_market_ret_1d
stock_ret_minus_industry_ret_1d
industry_ret_5d
industry_relative_strength_20d
market_breadth_up_down_ratio
```

---

### B. 금리·채권 피처

한국 주식은 금리 레벨과 금리 변화에 민감합니다. 특히 성장주, 바이오, 2차전지, 플랫폼주는 할인율 변화에 민감하고, 은행/보험은 장단기 금리차에 민감합니다.

| 피처                 |     주기 | 추천 소스       | 비고              |
| ------------------ | -----: | ----------- | --------------- |
| 한국 기준금리            | 이벤트/월간 | 한국은행 ECOS   | 정책금리            |
| CD 91일             |     일간 | ECOS        | 단기금리            |
| CP 91일             |     일간 | ECOS        | 신용 단기금리         |
| 국고채 1Y/3Y/5Y/10Y   |     일간 | ECOS        | 할인율, 장단기 스프레드   |
| 회사채 AA-, BBB-      |     일간 | ECOS        | credit spread   |
| 미국 Fed Funds Rate  |  일간/월간 | FRED        | 글로벌 금리          |
| 미국 2Y/10Y Treasury |     일간 | FRED        | 글로벌 할인율         |
| 한미 금리차             |  일간/월간 | ECOS + FRED | 환율/외국인 수급 proxy |

한국은행 ECOS는 금리, 환율, GDP, 소비자물가, 통화량, 국제수지 등 한국 거시 통계를 제공하며 Open API도 제공합니다. ([한국은행 경제정보시스템][2]) FRED API는 미국 및 글로벌 경제 데이터를 프로그램으로 조회할 수 있는 공식 API입니다. ([FRED][3])

**파생 피처 예시**

```text
kr_base_rate
kr_gov3y_yield
kr_gov10y_yield
kr_term_spread_10y_3y
kr_credit_spread_corp_aa_minus_gov3y
us10y_yield
us2y_yield
us_term_spread_10y_2y
kr_us_policy_rate_diff
yield_change_1d
yield_change_20d
```

---

### C. 환율·외환 피처

한국 상장사는 수출기업 비중이 높고, 외국인 수급도 환율에 민감합니다. USD/KRW는 거의 필수입니다.

| 피처        | 주기 | 추천 소스              | 활용           |
| --------- | -: | ------------------ | ------------ |
| USD/KRW   | 일간 | 한국은행 ECOS 또는 수출입은행 | 외국인 수급, 수출주  |
| JPY/KRW   | 일간 | ECOS               | 자동차/기계/일본 경쟁 |
| CNY/KRW   | 일간 | ECOS               | 중국 노출        |
| EUR/KRW   | 일간 | ECOS               | 글로벌 수출       |
| 달러인덱스 DXY | 일간 | FRED/시장 데이터        | 글로벌 달러 강세    |
| 외환보유액     | 월간 | ECOS               | 외환 안정성       |
| 경상수지      | 월간 | ECOS               | 원화 중기 방향     |

수출입은행 Open API는 현재환율 API를 제공하며 인증키 기반으로 조회할 수 있습니다. ([“Hello, World!" 거긴 어떤월드냐?][4]) 다만 주가 예측용 장기 시계열은 ECOS가 더 정규화된 통계 API로 관리하기 좋습니다. ECOS는 환율/통관수출입, 외환보유액, 국제수지 항목을 제공합니다. ([한국은행 경제정보시스템][2])

**파생 피처 예시**

```text
usdkrw_close
usdkrw_ret_1d
usdkrw_ret_5d
usdkrw_volatility_20d
jpykrw_ret_20d
cnykrw_ret_20d
dxy_ret_5d
current_account_yoy
fx_reserves_mom
```

---

### D. 물가·경기 피처

CPI, PPI, GDP, 산업생산, 소매판매는 주식시장 레짐 구분에 중요합니다. 단, 발표 주기가 월간/분기라서 **발표일 기준으로 lag 처리**해야 합니다.

| 피처           | 주기 | 추천 소스      | 사용 방식       |
| ------------ | -: | ---------- | ----------- |
| CPI          | 월간 | ECOS/KOSIS | 인플레이션       |
| Core CPI     | 월간 | ECOS/KOSIS | 정책금리 기대     |
| PPI          | 월간 | ECOS       | 원가 부담       |
| GDP 성장률      | 분기 | ECOS/KOSIS | 경기 레짐       |
| 산업생산지수       | 월간 | KOSIS/ECOS | 제조업 경기      |
| 선행종합지수       | 월간 | KOSIS      | 경기 전환       |
| 소매판매         | 월간 | KOSIS      | 내수          |
| 설비투자지수       | 월간 | KOSIS      | Capex cycle |
| 소비자심리지수      | 월간 | ECOS       | sentiment   |
| 기업경기실사지수 BSI | 월간 | ECOS       | 기업 심리       |

KOSIS OpenAPI는 국가통계포털 통계자료를 API로 제공하며, 공공데이터포털 설명 기준으로 JSON/XML 형태 활용이 가능합니다. ([KOSIS][5])

**중요한 점**

월간 CPI가 예를 들어 2026년 5월 데이터라고 해도, 실제 발표일이 2026년 6월 초라면 2026년 5월 말 예측에는 쓰면 안 됩니다. 데이터 테이블에는 반드시 아래 컬럼이 필요합니다.

```text
period_end_date
release_date
effective_from_date
value
source_revision_version
```

---

### E. 수출입·무역 피처

한국 시장에서는 수출 사이클이 매우 중요합니다. 특히 반도체, 자동차, 2차전지, 화학, 철강, 조선, 화장품, 음식료, 기계 업종에는 품목별 수출입이 강한 설명력을 가질 수 있습니다.

| 피처               |       주기 | 추천 소스            | 활용     |
| ---------------- | -------: | ---------------- | ------ |
| 총수출, 총수입         | 월간/일간 일부 | 관세청, KOSIS, KITA | 경기/원화  |
| 무역수지             |       월간 | 관세청/KOSIS        | 원화/시장  |
| 반도체 수출           |       월간 | 관세청/KITA         | 반도체 업종 |
| 자동차 수출           |       월간 | 관세청/KITA         | 자동차    |
| 선박 수출            |       월간 | 관세청/KITA         | 조선     |
| 석유제품 수출          |       월간 | 관세청/KITA         | 정유     |
| 이차전지/양극재 수출      |       월간 | 관세청 HS code      | 2차전지   |
| 국가별 수출: 중국/미국/EU |       월간 | 관세청/KITA         | 지역 노출  |

관세청 무역통계 포털은 HS Code, 국가, 품목 기준 수출입 통계를 제공하고, 공공데이터포털에는 품목별 수출입 실적 OpenAPI가 등록되어 있습니다. ([무역통계][6]) K-Stat은 한국무역협회가 제공하는 무역통계 서비스로 한국 및 해외 무역통계를 제공합니다. ([Kita Stats][7])

**파생 피처 예시**

```text
export_total_yoy
export_total_mom
import_total_yoy
trade_balance
semiconductor_export_yoy
auto_export_yoy
ship_export_yoy
battery_export_yoy
china_export_yoy
us_export_yoy
sector_export_momentum_3m
```

---

### F. 원자재·에너지 피처

원자재는 업종별 방향성이 다릅니다. 유가 상승은 정유에는 긍정적일 수 있지만 항공/화학/운송에는 비용 부담입니다. 구리, 철광석, 리튬, 니켈은 소재/2차전지/철강에 유용합니다.

| 피처         |    주기 | 소스 후보                         | 관련 업종      |
| ---------- | ----: | ----------------------------- | ---------- |
| WTI, Brent |    일간 | FRED, Nasdaq Data Link, EIA 등 | 정유, 화학, 항공 |
| 천연가스       |    일간 | FRED/EIA                      | 유틸리티, 화학   |
| 금          |    일간 | FRED/Nasdaq/Yahoo             | 안전자산       |
| 구리         |    일간 | FRED/Nasdaq/Yahoo             | 경기민감, 전선   |
| 알루미늄       |    일간 | FRED/Nasdaq                   | 소재         |
| 니켈         |    일간 | Nasdaq/LME 데이터 벤더             | 2차전지/스테인리스 |
| 리튬/탄산리튬    | 주간/월간 | 유료 데이터 가능성 높음                 | 2차전지       |
| 철광석        | 일간/주간 | 시장 데이터 벤더                     | 철강         |
| 원당/소맥/옥수수  |    일간 | Nasdaq/Yahoo                  | 음식료        |

Nasdaq Data Link는 금융·경제 데이터 API 문서를 제공하며, 일부 무료/유료 데이터셋을 API로 접근할 수 있습니다. ([Nasdaq Data Link Documentation][8]) FRED도 경제 및 일부 시장 지표 시계열 수집에 적합합니다. ([FRED][3])

**파생 피처 예시**

```text
wti_ret_1d
wti_ret_20d
brent_wti_spread
copper_ret_20d
gold_ret_20d
oil_volatility_20d
commodity_index_ret_20d
input_cost_pressure_by_sector
```

---

## 2. 종목별로 붙이면 좋은 “공통 피처 × 종목 속성” 조합

거시 피처는 모든 종목에 똑같이 붙이면 모델이 종목별 민감도를 스스로 학습해야 합니다. 더 좋은 방식은 종목의 업종/재무 특성과 결합해 interaction feature를 만드는 것입니다.

### A. 수출주 민감도

```text
export_exposure_score
usdkrw_ret_20d * export_exposure_score
global_demand_proxy * export_exposure_score
```

수출 비중은 사업보고서에서 직접 추출하거나, 업종별 proxy를 먼저 사용할 수 있습니다.

예:

```text
반도체: semiconductor_export_yoy
자동차: auto_export_yoy
조선: ship_export_yoy
화학: oil_price + petrochemical_export
철강: iron_ore + steel_export
```

---

### B. 금리 민감도

현재 보유한 재무 피처에서 부채/자본 구조를 만들 수 있습니다.

```text
debt_to_equity = total_liabilities / total_equity
net_debt_proxy = total_liabilities - cash_and_cash_equivalents
interest_burden_proxy = interest_paid / operating_income
rate_sensitivity = debt_to_equity * kr_gov3y_yield_change_20d
```

추천 추가 파생 피처:

```text
interest_paid_to_operating_income
cash_to_assets
liabilities_to_assets
rate_change_20d_x_debt_to_equity
credit_spread_x_interest_burden
```

---

### C. 원자재 비용 민감도

COGS가 이미 있으므로 원자재 가격과 결합할 수 있습니다.

```text
cogs_ratio = cogs / revenue
gross_margin = gross_profit / revenue
oil_ret_20d * cogs_ratio
copper_ret_20d * cogs_ratio
```

업종별로 commodity mapping을 둡니다.

| 업종      | 주요 원자재                   |
| ------- | ------------------------ |
| 정유      | WTI, Brent, crack spread |
| 화학      | WTI, naphtha             |
| 철강      | iron ore, coking coal    |
| 전선/전력기기 | copper                   |
| 음식료     | wheat, corn, sugar       |
| 항공/운송   | jet fuel, WTI            |
| 2차전지    | lithium, nickel, cobalt  |

---

### D. 밸류에이션 공통 피처

현재 재무 피처와 일봉 시세가 있으므로 아래는 반드시 만들 수 있습니다.

```text
market_cap = close * issued_shares
free_float_adjusted_market_cap  # 가능하면 유통주식수 필요
per = market_cap / controlling_net_income
pbr = market_cap / total_equity
psr = market_cap / revenue
p_ocf = market_cap / operating_cash_flow
dividend_yield = dps / close
ev = market_cap + total_liabilities - cash_and_cash_equivalents
ev_ebit = ev / operating_income
```

단, 분기/사업보고서 발표일 기준으로 값을 forward-fill해야 합니다.

---

## 3. 데이터 소스별 수집 방법

## 3.1 한국은행 ECOS

**추천 수집 항목**

```text
금리: 기준금리, CD91, CP91, 국고채 1Y/3Y/5Y/10Y, 회사채 AA-/BBB-
환율: USD/KRW, JPY/KRW, CNY/KRW, EUR/KRW
물가: CPI, Core CPI, PPI
국민계정: GDP, GNI
통화: M1, M2
국제수지: 경상수지, 상품수지
심리: 소비자심리지수, BSI
```

**수집 방식**

ECOS Open API는 대략 다음 구조로 호출합니다.

```text
https://ecos.bok.or.kr/api/StatisticSearch/{API_KEY}/json/kr/1/1000/{STAT_CODE}/{PERIOD}/{START}/{END}/{ITEM_CODE}
```

예시 구조:

```python
import requests
import pandas as pd

def fetch_ecos_series(api_key, stat_code, period, start, end, item_code):
    url = (
        f"https://ecos.bok.or.kr/api/StatisticSearch/"
        f"{api_key}/json/kr/1/10000/{stat_code}/{period}/{start}/{end}/{item_code}"
    )
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()

    rows = data.get("StatisticSearch", {}).get("row", [])
    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=["date", "value"])

    df = df.rename(columns={"TIME": "date", "DATA_VALUE": "value"})
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["source"] = "BOK_ECOS"
    df["stat_code"] = stat_code
    df["item_code"] = item_code
    return df
```

**주의점**

ECOS 통계코드와 item code는 수동 매핑 테이블을 따로 만들어야 합니다.

```sql
CREATE TABLE macro_series_master (
    series_id TEXT PRIMARY KEY,
    source TEXT,
    stat_code TEXT,
    item_code TEXT,
    frequency TEXT,
    name_kr TEXT,
    name_en TEXT,
    unit TEXT,
    transform_default TEXT,
    release_lag_days INTEGER,
    active BOOLEAN
);
```

---

## 3.2 KOSIS

**추천 수집 항목**

```text
CPI 세부 품목
산업생산지수
소매판매액지수
설비투자지수
건설기성
고용률/실업률
경기종합지수
업종별 생산/출하지수
```

KOSIS는 통계표 구조가 복잡하므로 “범용 크롤러”보다 **통계표별 adapter**를 만드는 편이 안정적입니다. KOSIS OpenAPI는 통계자료와 통계설명자료를 제공하며, JSON/XML 형태로 활용할 수 있습니다. ([KOSIS][5])

**권장 구조**

```text
kosis_series_master
- series_id
- org_id
- tbl_id
- itm_id
- obj_l1
- obj_l2
- obj_l3
- frequency
- name
- unit
```

**수집 로직**

```python
import requests
import pandas as pd

def fetch_kosis(api_key, org_id, tbl_id, start_prd_de, end_prd_de, itm_id="T00"):
    params = {
        "method": "getList",
        "apiKey": api_key,
        "format": "json",
        "jsonVD": "Y",
        "userStatsId": f"{org_id}/{tbl_id}/DT_1",
        "prdSe": "M",
        "startPrdDe": start_prd_de,
        "endPrdDe": end_prd_de,
        "orgId": org_id,
        "tblId": tbl_id,
        "itmId": itm_id,
    }
    url = "https://kosis.kr/openapi/Param/statisticsParameterData.do"
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    return df
```

실제 파라미터는 통계표마다 다르므로, KOSIS 웹에서 원하는 통계표의 API 요청 URL을 먼저 확인해 master에 저장하는 방식이 좋습니다.

---

## 3.3 KRX

**추천 수집 항목**

```text
지수 OHLCV: KOSPI, KOSDAQ, KOSPI200, KOSDAQ150, KRX300
업종지수 OHLCV
시장별 투자자 순매수
시장별 거래대금/거래량
상승/하락/보합 종목 수
신고가/신저가 종목 수
상한가/하한가 수
VKOSPI
프로그램 매매
선물/옵션 지표
```

KRX는 공식 화면에서 CSV 다운로드가 가능한 항목이 많습니다. 다만 웹 내부 API는 OTP 발급 후 CSV를 다운로드하는 패턴이 자주 사용됩니다. 운영 환경에서는 화면 구조 변경 가능성이 있으므로, 가능하면 KRX Data Marketplace의 공식 데이터 상품/API/파일 구독을 검토하는 것이 안정적입니다. KRX Data Marketplace는 시장정보, 공매도정보, 투자분석정보 등을 통합 제공하는 포털입니다. ([한국거래소 데이터][1])

**웹 다운로드 패턴 예시**

```python
import requests
import pandas as pd
from io import BytesIO

KRX_OTP_URL = "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
KRX_DOWNLOAD_URL = "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"

def download_krx_csv(params):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://data.krx.co.kr/",
    }

    with requests.Session() as s:
        otp = s.post(KRX_OTP_URL, data=params, headers=headers, timeout=20).text
        r = s.post(KRX_DOWNLOAD_URL, data={"code": otp}, headers=headers, timeout=20)
        r.raise_for_status()

    return pd.read_csv(BytesIO(r.content), encoding="euc-kr")
```

**주의점**

KRX 웹 다운로드는 비공식 호출에 가깝기 때문에 다음 처리가 필요합니다.

```text
1. 실패 재시도
2. 응답 HTML 여부 검사
3. 컬럼명 변경 감지
4. 거래일 캘린더 기반 backfill
5. 공식 라이선스/이용약관 확인
```

---

## 3.4 FRED

**추천 수집 항목**

```text
미국 기준금리
미국 2년/10년 국채금리
미국 CPI/PPI
미국 실업률
미국 ISM/PMI 대체 지표
달러인덱스 또는 broad dollar index
WTI
VIX 계열 가능 시
글로벌 경기 proxy
```

FRED API는 경제 데이터 전체 히스토리 및 release 단위 observation 조회에 적합합니다. ([FRED][3]) Python에서는 `fredapi` 같은 wrapper도 사용할 수 있습니다. ([GitHub][9])

```python
from fredapi import Fred
import pandas as pd

def fetch_fred_series(api_key, series_id):
    fred = Fred(api_key=api_key)
    s = fred.get_series(series_id)
    df = s.reset_index()
    df.columns = ["date", "value"]
    df["series_id"] = series_id
    df["source"] = "FRED"
    return df
```

---

## 3.5 관세청 / KITA / KOSIS 무역통계

**추천 수집 항목**

```text
총수출/총수입
무역수지
HS 2/4/6/10단위 품목별 수출입
국가별 수출입
주요 품목: 반도체, 자동차, 선박, 석유제품, 화장품, 의약품, 이차전지
```

관세청 통계는 HS Code 기준으로 세밀하게 가져오는 것이 장점입니다. 품목별 수출입 실적 OpenAPI는 HS Code 단위 통계를 제공합니다. ([데이터.go.kr][10]) KITA K-Stat은 국내외 무역통계를 제공하는 별도 포털입니다. ([Kita Stats][7])

**HS code mapping 테이블 권장**

```sql
CREATE TABLE hs_sector_mapping (
    hs_code TEXT,
    hs_level INTEGER,
    feature_group TEXT,
    sector_code TEXT,
    description TEXT,
    weight NUMERIC,
    active BOOLEAN
);
```

예:

```text
semiconductor_export_yoy -> 반도체 관련 HS code 묶음
battery_export_yoy -> 이차전지/양극재/음극재 관련 HS code 묶음
cosmetics_export_yoy -> 화장품 HS code 묶음
ship_export_yoy -> 선박 HS code 묶음
```

---

## 4. 최종 피처 카탈로그 제안

### 4.1 Daily market common features

```text
market_kospi_close
market_kospi_ret_1d
market_kospi_ret_5d
market_kospi_ret_20d
market_kosdaq_close
market_kosdaq_ret_1d
market_kosdaq_ret_5d
market_kosdaq_ret_20d
market_kospi200_ret_1d
market_kosdaq150_ret_1d
market_krx300_ret_1d
market_total_turnover
market_total_volume
market_advancers
market_decliners
market_advance_decline_ratio
market_new_highs
market_new_lows
market_upper_limit_count
market_lower_limit_count
```

---

### 4.2 Daily industry features

```text
industry_index_close
industry_ret_1d
industry_ret_5d
industry_ret_20d
industry_volatility_20d
industry_turnover
stock_ret_minus_industry_ret_1d
stock_ret_minus_industry_ret_20d
industry_rank_ret_20d
```

---

### 4.3 Rates and FX features

```text
kr_base_rate
kr_cd91
kr_cp91
kr_gov1y
kr_gov3y
kr_gov5y
kr_gov10y
kr_term_spread_10y_3y
kr_corp_aa_3y
kr_corp_bbb_3y
kr_credit_spread_aa
kr_credit_spread_bbb

usdkrw
jpykrw
cnykrw
eurkrw
usdkrw_ret_1d
usdkrw_ret_5d
usdkrw_ret_20d
usdkrw_vol_20d

us_ffr
us2y
us10y
us_term_spread_10y_2y
kr_us_10y_spread
kr_us_policy_rate_spread
```

---

### 4.4 Macro monthly/quarterly features

```text
cpi_yoy
core_cpi_yoy
ppi_yoy
gdp_yoy
gdp_qoq
industrial_production_yoy
retail_sales_yoy
facility_investment_yoy
construction_completed_yoy
unemployment_rate
employment_rate
consumer_sentiment_index
business_survey_index
leading_composite_index
coincident_composite_index
m2_yoy
current_account
goods_account
fx_reserves
```

---

### 4.5 Trade and commodity features

```text
export_total_yoy
import_total_yoy
trade_balance
export_to_china_yoy
export_to_us_yoy
export_to_eu_yoy
semiconductor_export_yoy
auto_export_yoy
ship_export_yoy
petrochemical_export_yoy
steel_export_yoy
battery_export_yoy
cosmetics_export_yoy

wti_close
wti_ret_20d
brent_close
brent_ret_20d
natural_gas_ret_20d
gold_ret_20d
copper_ret_20d
iron_ore_ret_20d
nickel_ret_20d
lithium_price_change
```

---

### 4.6 Derived interaction features

```text
rate_change_20d_x_debt_to_equity
credit_spread_x_interest_burden
usdkrw_ret_20d_x_export_exposure
oil_ret_20d_x_cogs_ratio
copper_ret_20d_x_cogs_ratio
industry_ret_20d_x_stock_beta_to_industry
market_volatility_20d_x_stock_beta
export_yoy_x_sector_dummy
```

---

## 5. 데이터 모델 설계

현재 종목 데이터와 결합하기 위해 아래처럼 “원천 시계열”과 “피처 테이블”을 분리하는 것을 권합니다.

### 5.1 Raw series table

```sql
CREATE TABLE raw_macro_series (
    source TEXT NOT NULL,
    series_id TEXT NOT NULL,
    date DATE NOT NULL,
    value NUMERIC,
    unit TEXT,
    frequency TEXT,
    collected_at TIMESTAMP NOT NULL DEFAULT now(),
    source_updated_at TIMESTAMP,
    PRIMARY KEY (source, series_id, date)
);
```

---

### 5.2 Release-aware table

월간/분기 거시지표에는 발표일이 중요합니다.

```sql
CREATE TABLE macro_observation_release (
    source TEXT NOT NULL,
    series_id TEXT NOT NULL,
    period_end_date DATE NOT NULL,
    release_date DATE NOT NULL,
    value NUMERIC,
    vintage TEXT,
    collected_at TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (source, series_id, period_end_date, release_date, vintage)
);
```

---

### 5.3 Daily aligned feature table

모델 학습용 최종 테이블입니다.

```sql
CREATE TABLE daily_common_features (
    trade_date DATE PRIMARY KEY,

    kospi_ret_1d NUMERIC,
    kosdaq_ret_1d NUMERIC,
    kospi_ret_20d NUMERIC,
    market_vol_20d NUMERIC,

    usdkrw_ret_1d NUMERIC,
    usdkrw_ret_20d NUMERIC,

    kr_gov3y NUMERIC,
    kr_gov10y NUMERIC,
    kr_term_spread_10y_3y NUMERIC,

    cpi_yoy_latest NUMERIC,
    export_total_yoy_latest NUMERIC,
    semiconductor_export_yoy_latest NUMERIC,

    wti_ret_20d NUMERIC,
    copper_ret_20d NUMERIC,

    feature_generated_at TIMESTAMP NOT NULL DEFAULT now()
);
```

---

### 5.4 Stock-date feature join table

```sql
CREATE TABLE stock_daily_features (
    trade_date DATE NOT NULL,
    ticker TEXT NOT NULL,

    stock_ret_1d NUMERIC,
    stock_ret_5d NUMERIC,
    stock_ret_20d NUMERIC,

    market_ret_1d NUMERIC,
    industry_ret_1d NUMERIC,
    stock_minus_market_ret_1d NUMERIC,
    stock_minus_industry_ret_1d NUMERIC,

    usdkrw_ret_20d NUMERIC,
    kr_gov3y_change_20d NUMERIC,
    export_yoy_sector NUMERIC,

    per NUMERIC,
    pbr NUMERIC,
    psr NUMERIC,
    ev_ebit NUMERIC,
    debt_to_equity NUMERIC,
    cogs_ratio NUMERIC,

    PRIMARY KEY (trade_date, ticker)
);
```

---

## 6. 수집 파이프라인 권장 구조

```text
1. series_master 관리
   - source, series_id, frequency, unit, transform, release_lag

2. raw collector
   - ECOS collector
   - KOSIS collector
   - KRX collector
   - FRED collector
   - Customs/KITA collector
   - Commodity collector

3. raw validation
   - 중복 날짜 검사
   - 결측률 검사
   - 단위 변경 검사
   - 전일 대비 비정상 jump 검사

4. calendar alignment
   - KRX 거래일 캘린더 기준으로 daily forward-fill
   - 월간/분기 데이터는 release_date 이후부터만 사용

5. feature transform
   - ret_1d, ret_5d, ret_20d
   - yoy, mom
   - rolling volatility
   - spread
   - z-score
   - percentile rank

6. stock-level join
   - ticker × trade_date 기준 병합
   - 업종지수 매핑
   - 재무제표 발표일 기준 forward-fill

7. model training snapshot
   - train cutoff date 기준 point-in-time dataset 생성
```

---

## 7. 크롤링/수집 운영 팁

### A. 주기별 스케줄

| 주기       | 대상                               |
| -------- | -------------------------------- |
| 매일 장마감 후 | KRX 지수, 업종지수, 시장 수급, 금리, 환율, 원자재 |
| 매월 초     | CPI, 수출입, 산업생산, 심리지수             |
| 매분기      | GDP, 국민계정, 일부 기업경영 통계            |
| 수시/이벤트   | 기준금리, FOMC, 금통위                  |

---

### B. Look-ahead 방지 규칙

가장 중요합니다.

```text
일간 시장 데이터: trade_date 당일 장마감 이후 사용 가능
월간 거시 데이터: 발표일 release_date 이후 사용 가능
분기 GDP: 발표일 이후 사용 가능
재무제표: 공시일 이후 사용 가능
수정 통계: vintage 관리 또는 수집일 기준 snapshot 관리
```

특히 CPI, GDP, 수출입은 “해당 월 데이터”가 아니라 “발표된 날짜” 기준으로 feature effective date를 잡아야 합니다.

---

### C. 피처 변환 기본 규칙

```python
def add_time_series_features(df, value_col="value"):
    df = df.sort_values("date").copy()

    for lag in [1, 5, 20, 60, 120]:
        df[f"ret_{lag}d"] = df[value_col].pct_change(lag)

    for window in [20, 60, 120]:
        df[f"vol_{window}d"] = df[f"ret_1d"].rolling(window).std()

    for window in [60, 120, 252]:
        mean = df[value_col].rolling(window).mean()
        std = df[value_col].rolling(window).std()
        df[f"zscore_{window}d"] = (df[value_col] - mean) / std

    return df
```

월간 데이터:

```python
def add_monthly_features(df, value_col="value"):
    df = df.sort_values("period_end_date").copy()
    df["mom"] = df[value_col].pct_change(1)
    df["yoy"] = df[value_col].pct_change(12)
    df["diff_1m"] = df[value_col].diff(1)
    df["diff_12m"] = df[value_col].diff(12)
    return df
```

---

## 8. 우선순위 로드맵

### Phase 1: 즉시 추가 권장

가성비가 가장 높습니다.

```text
KOSPI/KOSDAQ/KOSPI200/KOSDAQ150
KRX 업종지수
USD/KRW
국고채 3년/10년
CD91
회사채 AA- spread
CPI YoY
수출 YoY
반도체 수출 YoY
WTI
Copper
미국 10Y
S&P500/Nasdaq/VIX
```

S&P500, Nasdaq, VIX 같은 해외 시장 지수는 국내 장 시작 전 글로벌 risk-on/risk-off를 반영하는 데 유용합니다. 단, Yahoo/yfinance 계열은 편리하지만 공식 보증 데이터가 아니므로 연구용으로 두고, 상용 운영은 라이선스 있는 벤더를 검토하는 편이 안전합니다. yfinance 공식 저장소도 Yahoo와 제휴·보증 관계가 아니며 연구/교육 목적 사용과 약관 확인을 명시하고 있습니다. ([GitHub][11])

---

### Phase 2: 업종 특화

```text
HS Code 기반 품목별 수출입
업종별 생산지수
업종별 재고/출하지수
원자재 basket
중국 경기 proxy
미국 제조업 proxy
```

---

### Phase 3: 고급 시장 미시구조

```text
시장 breadth
신고가/신저가
프로그램 매매
선물 basis
KOSPI200 선물 거래량/미결제약정
VKOSPI
ETF 자금흐름
공매도 시장 aggregate
```

---

## 9. 결론

현재 데이터셋은 **종목 단위 가격·수급·재무**는 꽤 잘 갖춰져 있지만, 시장 전체 레짐을 설명하는 축이 약합니다. 가장 먼저 추가할 것은 아래 12개 그룹입니다.

```text
1. KOSPI/KOSDAQ/KOSPI200/KOSDAQ150 수익률
2. KRX 업종지수 수익률
3. 시장 breadth
4. USD/KRW, JPY/KRW, CNY/KRW
5. 한국 국고채 3년/10년, CD91, 회사채 spread
6. 미국 2년/10년 금리, S&P500, Nasdaq, VIX
7. CPI/Core CPI/PPI
8. GDP/산업생산/소매판매/선행지수
9. 총수출/총수입/무역수지
10. 품목별 수출: 반도체, 자동차, 선박, 석유제품, 2차전지
11. WTI/Brent/구리/금/니켈/리튬
12. 거시 × 종목 재무 interaction feature
```

구현상 핵심은 단순히 데이터를 많이 붙이는 것이 아니라, **거래일 기준 point-in-time alignment**입니다. 모든 월간·분기 거시지표와 재무제표는 `period_end_date`가 아니라 `release_date` 또는 `disclosure_date` 이후부터만 학습 데이터에 노출해야 합니다. 이 규칙만 지켜도 backtest 과최적화와 look-ahead leakage를 크게 줄일 수 있습니다.

[1]: https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd?utm_source=chatgpt.com "KRX | KRX Data Marketplace"
[2]: https://ecos.bok.or.kr/?utm_source=chatgpt.com "한국은행경제통계시스템"
[3]: https://fred.stlouisfed.org/docs/api/fred/?utm_source=chatgpt.com "St. Louis Fed Web Services: FRED® API"
[4]: https://issuemaker99.tistory.com/11?utm_source=chatgpt.com "환율 API 사용하기 수출입은행 Open API 제공 - issuemaker99 님의 블로그"
[5]: https://kosis.kr/openapi/?utm_source=chatgpt.com "KOSIS 공유서비스"
[6]: https://www.tradedata.go.kr/cts/index_eng.do?utm_source=chatgpt.com "Korea Customs Service Trade Statistics"
[7]: https://stat.kita.net/?utm_source=chatgpt.com "K-stat 무역통계 - 한국무역협회"
[8]: https://docs.data.nasdaq.com/?utm_source=chatgpt.com "Nasdaq Data Link Documentation"
[9]: https://github.com/mortada/fredapi?utm_source=chatgpt.com "fredapi: Python API for FRED (Federal Reserve Economic Data)"
[10]: https://www.data.go.kr/en/data/15101609/openapi.do?utm_source=chatgpt.com "OPENAPI Detail | PUBLIC DATA PORTAL"
[11]: https://github.com/ranaroussi/yfinance?utm_source=chatgpt.com "Download market data from Yahoo! Finance's API - GitHub"
