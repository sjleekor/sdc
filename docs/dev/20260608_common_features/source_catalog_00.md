# 공통 피쳐 원천 카탈로그 초안

- 작성일: 2026-06-09
- 최종 업데이트: 2026-06-11
- 관련 계획: `docs/dev/20260608_common_features/plan_00.md`
- 목적: 공통 시장/거시 feature의 upstream source code, availability policy, 활성화 상태를 코드 반영 전에 명시한다.
- 상태: ECOS `rate_kr_gov3y`, `rate_kr_gov10y`/10Y-3Y spread, `fx_usdkrw_ecos`, `macro_cpi` level/YoY/MoM, PPI/M2/CSI monthly macro 7개 feature active 전환 완료. FRED `rate_us2y`, `rate_us10y`, `rate_us_term_spread_10y_2y`, `commodity_wti_spot_ret_20d` active 전환 완료. KRX direct 국내 지수(`market_kospi_krx`, `market_kosdaq_krx`, `market_kospi200_krx`)와 KRX breadth/liquidity 8개 feature active 전환 완료. KRX/KOSPI/KOSDAQ 업종지수 4개 source와 level/ret_1d 8개 feature는 inactive smoke 완료.

## 1. 원칙

1. source code는 구현 편의보다 point-in-time 안전성을 우선한다.
2. 실제 API smoke 전에는 신규 macro series를 active feature로 모델에 노출하지 않는다.
3. 발표일을 API에서 직접 얻지 못하는 월간 지표는 conservative lag를 먼저 적용하고, release calendar가 준비되면 교체한다.
4. 공식 원천으로 대체 가능한 항목은 FDR/pykrx fallback보다 ECOS/FRED/KRX direct를 우선한다.
5. feature catalog는 `active=false`여도 source/catalog FK와 transform 계약을 먼저 검증할 수 있다.

## 2. 현재 검증 상태 요약

| source | 상태 | 비고 |
|---|---|---|
| FDR | 제한 smoke 완료 | `US500`, `VIX`, `USD/KRW`, `CL=F` 적재 확인. 연구/MVP fallback |
| PYKRX | mock test, 3M fallback sync 완료 | KRX direct active 전환 후 fallback inactive. credential/session 상태에 의존하므로 운영 기본 경로에서는 제외 |
| ECOS | client/provider mock test, CLI wiring, live smoke, readiness 검증 완료 | `rate_kr_gov3y`, `rate_kr_gov10y`, 10Y-3Y spread, `fx_usdkrw_ecos`, `macro_cpi` level/YoY/MoM, PPI/M2/CSI monthly macro active |
| FRED | client/provider mock test, CLI wiring, live smoke/readiness 완료 | `rate_us2y`, `rate_us10y`, `rate_us_term_spread_10y_2y`, `commodity_wti_spot_ret_20d` active |
| KRX direct | provider/unit test, provider-level live smoke, DB build/readiness, pykrx 비교 완료 | `market_kospi_krx`, `market_kosdaq_krx`, `market_kospi200_krx`와 breadth/liquidity active. 업종지수 4개 후보는 inactive smoke 완료 |

## 3. ECOS API 호출 형태

ECOS `StatisticSearch`는 아래 path 구조를 사용한다.

```text
https://ecos.bok.or.kr/api/StatisticSearch/{API_KEY}/json/kr/{start_row}/{end_row}/{stat_code}/{cycle}/{start_period}/{end_period}/{item_code1}/{item_code2}/...
```

현재 구현된 `EcosStatisticSearchClient`는 아래 입력을 받는다.

| 필드 | 의미 | 예 |
|---|---|---|
| `stat_code` | ECOS 통계표 코드 | `817Y002` |
| `cycle` | 주기 | `D`, `M`, `Q`, `A` |
| `start_period` | 주기별 시작 period | `20240102`, `202401` |
| `end_period` | 주기별 종료 period | `20240112`, `202401` |
| `item_codes` | 통계항목 코드 배열 | `["010200000"]` |

참고 공식 진입점:

- 한국은행 ECOS Open API: https://ecos.bok.or.kr/api/

## 4. ECOS 후보 series

### 4.1 `rate_kr_gov3y`

| 항목 | 값 |
|---|---|
| `series_id` | `rate_kr_gov3y` |
| source | `ECOS` |
| category | `rate` |
| frequency | `D` |
| name_kr | 국고채 3년 수익률 |
| unit | `pct` |
| `stat_code` 후보 | `817Y002` |
| `cycle` | `D` |
| `item_code1` 후보 | `010200000` |
| availability policy | `next_krx_session` |
| `manual_lag_days` | `0` |
| `max_stale_business_days` | `5` |
| default transform | `level` |
| seed active | `true` |
| feature 후보 | `rate_kr_gov3y_level` |
| smoke 상태 | 12개월 coverage/readiness 완료 후 active 전환, 2026-06-10 |

판단:

- 일간 금리이며 장중 모델 feature에는 당일 확정값을 바로 쓰지 않는다.
- 현재는 `next_krx_session`으로 보수 처리한다.
- live smoke에서 `TIME`, `DATA_VALUE`, `UNIT_NAME`, `available_from_date` 매핑을 확인했다.
- 2024년 1월 Mon-Fri 22일 기준 22 rows가 들어와 단기 결측은 없었다.
- 3개월/12개월 운영 범위 coverage와 readiness를 통과해 `active=true`로 전환했다.

### 4.2 `rate_kr_gov10y`

| 항목 | 값 |
|---|---|
| `series_id` | `rate_kr_gov10y` |
| source | `ECOS` |
| category | `rate` |
| frequency | `D` |
| name_kr | 국고채 10년 수익률 |
| unit | `pct` |
| `stat_code` | `817Y002` |
| `cycle` | `D` |
| `item_code1` | `010210000` |
| availability policy | `next_krx_session` |
| `manual_lag_days` | `0` |
| `max_stale_business_days` | `5` |
| default transform | `level` |
| seed active | `true` |
| feature 후보 | `rate_kr_gov10y_level`, `rate_kr_term_spread_10y_3y` |
| metadata 확인 | ECOS `StatisticItemList`에서 `ITEM_CODE=010210000`, `ITEM_NAME=국고채(10년)`, `START_TIME=20001218`, `UNIT_NAME=연%` 확인 |
| smoke 상태 | 2024년 1월 단기 + 최근 3개월/12개월 spread coverage/readiness 통과, 2026-06-11 |

판단:

- 10Y 금리는 3Y 금리와 동일한 ECOS `817Y002` 일간 시장금리 테이블에 있다.
- 일간 금리이며 3Y와 동일하게 `next_krx_session` 정책을 적용한다.
- `rate_kr_gov10y_level`과 `rate_kr_term_spread_10y_3y`는 단기 및 3개월/12개월 운영 범위 검증을 통과해 `active=true`로 전환했다.

### 4.3 `fx_usdkrw_ecos`

| 항목 | 값 |
|---|---|
| `series_id` | `fx_usdkrw_ecos` |
| source | `ECOS` |
| category | `fx` |
| frequency | `D` |
| name_kr | 원/미국달러 매매기준율 |
| unit | `KRW` |
| `stat_code` | `731Y001` |
| `cycle` | `D` |
| `item_code1` | `0000001` |
| availability policy | `next_krx_session` |
| `manual_lag_days` | `0` |
| `max_stale_business_days` | `10` |
| default transform | `level` |
| seed active | `true` |
| feature 후보 | `fx_usdkrw_ecos_level`, `fx_usdkrw_ecos_ret_5d` |
| metadata 확인 | ECOS `StatisticItemList`에서 `STAT_CODE=731Y001`, `ITEM_CODE=0000001`, `ITEM_NAME=원/미국달러(매매기준율)`, `START_TIME=19640504`, `UNIT_NAME=원` 확인 |
| smoke 상태 | 단기/3개월/12개월 DB build/coverage/readiness 통과 후 active 전환 |

판단:

- 기존 active `fx_usdkrw_level`/`fx_usdkrw_ret_5d`의 input은 FDR fallback `fx_usdkrw`에서 ECOS official `fx_usdkrw_ecos`로 전환했다.
- FDR `fx_usdkrw` source series는 fallback으로 남긴다.
- `fx_usdkrw_ecos_level`/`fx_usdkrw_ecos_ret_5d`는 검증 결과 추적용 inactive feature로 유지한다.

### 4.4 `macro_cpi`

| 항목 | 값 |
|---|---|
| `series_id` | `macro_cpi` |
| source | `ECOS` |
| category | `macro_price` |
| frequency | `M` |
| name_kr | 소비자물가지수 |
| unit | `2020=100` |
| `stat_code` 후보 | `901Y009` |
| `cycle` | `M` |
| `item_code1` 후보 | `0` |
| availability policy | `manual_lag_days` |
| `manual_lag_days` | `20` |
| `max_stale_business_days` | `45` |
| default transform | `level` |
| seed active | `true` |
| feature 후보 | `macro_cpi_level`, `macro_cpi_yoy_latest`, `macro_cpi_mom_latest` |
| smoke 상태 | conservative release policy 기반 active 전환 완료, 2026-06-11 |

판단:

- 월간 macro series는 `period_end_date`만으로 모델 feature에 노출하면 look-ahead leakage가 생긴다.
- API에서 release date를 직접 확보하기 전까지는 `period_end_date + 20 calendar days`를 보수 lag로 둔다. 결과일이 KRX 비거래일이면 다음 KRX session부터 사용한다.
- live smoke에서 ECOS 단위가 `2020=100`으로 확인되어 seed/catalog unit을 반영했다.
- `manual_lag_days=20` 적용과 주말 다음 KRX session 보정은 동작 확인했다. 실제 공식 발표일 달력과의 일치성은 아직 별도 확인이 필요하므로, 공식 calendar가 준비되면 현재 보수 lag를 교체한다.
- `macro_cpi_yoy_latest`와 `macro_cpi_mom_latest`는 builder의 calendar month exact-match `yoy`/`mom` transform으로 active 전환했다.

### 4.5 PPI/M2/CSI 월간 macro 후보

아래 후보는 CPI와 같은 conservative monthly availability 정책(`period_end_date + 20 calendar days`, 다음 KRX session 보정)을 적용한다. inactive smoke와 active-only 검증을 통과해 active 전환했다.

| `series_id` | source | ECOS code | item codes | category | unit | history start | active | feature 후보 |
|---|---|---|---|---|---|---|---:|---|
| `macro_ppi` | `ECOS` | `404Y014` 생산자물가지수(기본분류) | `*AA` 총지수 | `macro_price` | `2020=100` | `1965-01` | true | `macro_ppi_level`, `macro_ppi_yoy_latest`, `macro_ppi_mom_latest` |
| `macro_m2` | `ECOS` | `161Y005` M2 상품별 구성내역(평잔, 계절조정계열) | `BBHS00` M2(평잔,계절조정계열) | `macro_money` | `KRW_bn` API unit `십억원` | `2003-10` | true | `macro_m2_level`, `macro_m2_yoy_latest`, `macro_m2_mom_latest` |
| `macro_consumer_sentiment` | `ECOS` | `511Y002` 소비자동향조사(전국, 월) | `FME` 소비자심리지수, `99988` 전체 | `macro_sentiment` | `index` | `2008-07` | true | `macro_consumer_sentiment_level` |

metadata 확인:

- `StatisticTableList`: `404Y014`, `161Y005`, `511Y002` 모두 월간(`M`) 조회 가능(`SRCH_YN=Y`).
- `StatisticItemList`: PPI `*AA` 총지수, M2 `BBHS00`, CSI `FME`/`99988` item code 확인.
- 단기 provider smoke: `2026-01..2026-05` 범위에서 PPI 4 rows, M2 3 rows, CSI 5 rows를 반환했다. PPI/M2는 ECOS 최신 공표 범위가 각각 `2026-04`, `2026-03`까지라 최신월 차이가 존재한다.

### 4.6 FRED 후보 series

FRED `series/observations` API는 `series_id`, `observation_start`, `observation_end`, `file_type=json`으로 최신 observation을 조회한다. 1차 구현은 최신 observation만 raw에 저장하고, `realtime_start`/`realtime_end`는 raw payload에 보존한다. ALFRED/vintage 기반 PIT 재구성은 후속 작업으로 둔다.

참고 공식 진입점:

- FRED series observations API: https://fred.stlouisfed.org/docs/api/fred/series_observations.html
- DGS2: https://fred.stlouisfed.org/series/DGS2
- DGS10: https://fred.stlouisfed.org/series/DGS10
- DCOILWTICO: https://fred.stlouisfed.org/series/DCOILWTICO

| `series_id` | source | FRED code | category | unit | history start | policy | active | feature 후보 |
|---|---|---|---|---|---|---|---:|---|
| `rate_us2y` | `FRED` | `DGS2` | `rate` | `pct` | `1976-06-01` | `same_krx_session_morning` | true | `rate_us2y_level`, `rate_us_term_spread_10y_2y` |
| `rate_us10y` | `FRED` | `DGS10` | `rate` | `pct` | `1962-01-02` | `same_krx_session_morning` | true | `rate_us10y_level`, `rate_us_term_spread_10y_2y` |
| `commodity_wti_fred` | `FRED` | `DCOILWTICO` | `commodity` | `USD/bbl` | `1986-01-02` | `same_krx_session_morning` | true | `commodity_wti_spot_ret_20d`, `commodity_wti_fred_ret_20d` |

판단:

- 미국 일간 금리/원자재는 한국 장 시작 전에 전일 데이터가 확인 가능하다는 보수 가정으로 `same_krx_session_morning` 정책을 둔다.
- `rate_us_term_spread_10y_2y`는 `rate_us10y - rate_us2y` multi-input spread로 등록했고, live smoke/readiness 통과 후 active 전환했다.
- 기존 active `commodity_wti_ret_20d`는 FDR `commodity_wti`(`CL=F` futures)를 계속 사용한다. FRED WTI spot은 비교 결과 의미 차이가 있어 기존 feature를 대체하지 않고 `commodity_wti_spot_ret_20d`로 병행 노출한다.
- `FRED_API_KEY` 미설정 환경에서는 provider가 네트워크 호출 없이 error result를 반환한다.

### 4.7 KRX direct 국내 지수 후보

KRX MDC 직접 호출은 기존 `flows_krx`에서 검증한 `KrxMdcClient`를 공통 모듈(`adapters/krx_common/client.py`)로 추출해 재사용한다. 지수 OHLCV는 pykrx 내부 호출과 동일한 KRX endpoint를 직접 호출한다.

| 항목 | 값 |
|---|---|
| bld | `dbms/MDC/STAT/standard/MDCSTAT00301` |
| output key | `output` |
| date params | `strtDd`, `endDd` (`YYYYMMDD`) |
| index params | `indIdx`, `indIdx2` |
| close column | `CLSPRC_IDX` |
| date column | `TRD_DD` |

pykrx index code는 KRX direct 파라미터로 아래처럼 분해된다.

| `series_id` | pykrx index code | `indIdx` | `indIdx2` | policy | seed active | validation feature |
|---|---|---:|---:|---|---:|---|
| `market_kospi_krx` | `1001` | `1` | `001` | `next_krx_session` | true | `market_kospi_krx_close`, `market_kospi_krx_ret_1d`, `market_kospi_krx_ret_5d`, `market_kospi_krx_ret_20d` |
| `market_kosdaq_krx` | `2001` | `2` | `001` | `next_krx_session` | true | `market_kosdaq_krx_ret_1d` |
| `market_kospi200_krx` | `1028` | `1` | `028` | `next_krx_session` | true | `market_kospi200_krx_ret_1d` |

판단:

- 국내 지수 close는 장마감 후 확정값이므로 기존 pykrx 경로와 동일하게 `next_krx_session` 정책을 유지한다.
- 신규 KRX direct 후보는 provider-level live smoke, DB build/coverage/readiness, pykrx 값 비교를 통과해 `active=true`로 전환했다. 기존 모델용 feature code(`market_kospi_close`, `market_kospi_ret_*`, `market_kosdaq_ret_1d`, `market_kospi200_ret_1d`)는 유지하되 input만 KRX direct series로 전환했고, pykrx series는 fallback inactive로 격하했다.
- KRX provider는 `KRX_ID`/`KRX_PW`가 있으면 session 만료 시 login retry를 수행하고, 없으면 warmup cookie 기반 공개 endpoint 호출만 시도한다.

## 5. 코드 반영 상태

현재 seed 반영 상태:

| 대상 | 코드 | active | 비고 |
|---|---|---:|---|
| source series | `rate_kr_gov3y` | true | 3개월/12개월 coverage/readiness 통과 |
| source series | `rate_kr_gov10y` | true | item code 확인, 3개월/12개월 운영 범위 검증 통과 |
| source series | `fx_usdkrw_ecos` | true | ECOS code 확인, 단기/3개월/12개월 coverage/readiness 통과 |
| source series | `macro_cpi` | true | conservative period-end + 20 calendar day policy 검증 후 active |
| source series | `rate_us2y` | true | FRED `DGS2`, 단기/3개월/12개월 coverage/readiness 통과 |
| source series | `rate_us10y` | true | FRED `DGS10`, 단기/3개월/12개월 coverage/readiness 통과 |
| source series | `commodity_wti_fred` | true | FRED `DCOILWTICO`, 3개월/12개월 readiness와 FDR futures 비교 후 spot feature로 병행 active |
| source series | `market_kospi_krx` | true | KRX direct `MDCSTAT00301`, provider-level smoke + 3M DB readiness + pykrx exact comparison 통과 |
| source series | `market_kosdaq_krx` | true | KRX direct `MDCSTAT00301`, provider-level smoke + 3M DB readiness + pykrx exact comparison 통과 |
| source series | `market_kospi200_krx` | true | KRX direct `MDCSTAT00301`, provider-level smoke + 3M DB readiness + pykrx exact comparison 통과 |
| source series | `market_kospi` | false | pykrx fallback, KRX direct 전환 후 inactive |
| source series | `market_kosdaq` | false | pykrx fallback, KRX direct 전환 후 inactive |
| source series | `market_kospi200` | false | pykrx fallback, KRX direct 전환 후 inactive |
| source series | `macro_ppi` | true | ECOS `404Y014`/`*AA`, active-only smoke 완료 |
| source series | `macro_m2` | true | ECOS `161Y005`/`BBHS00`, active-only smoke 완료 |
| source series | `macro_consumer_sentiment` | true | ECOS `511Y002`/`FME`/`99988`, active-only smoke 완료 |
| feature catalog | `market_kospi_close` | true | KRX direct `market_kospi_krx` input으로 전환 |
| feature catalog | `market_kospi_ret_1d` | true | KRX direct `market_kospi_krx` input으로 전환 |
| feature catalog | `market_kospi_ret_5d` | true | KRX direct `market_kospi_krx` input으로 전환 |
| feature catalog | `market_kospi_ret_20d` | true | KRX direct `market_kospi_krx` input으로 전환 |
| feature catalog | `market_kosdaq_ret_1d` | true | KRX direct `market_kosdaq_krx` input으로 전환 |
| feature catalog | `market_kospi200_ret_1d` | true | KRX direct `market_kospi200_krx` input으로 전환 |
| feature catalog | `rate_kr_gov3y_level` | true | single input, `level` |
| feature catalog | `rate_kr_gov10y_level` | true | single input, `level`; 3개월/12개월 운영 범위 검증 통과 |
| feature catalog | `rate_kr_term_spread_10y_3y` | true | multi input, `spread_long - spread_short`; 3개월/12개월 운영 범위 검증 통과 |
| feature catalog | `fx_usdkrw_level` | true | ECOS `fx_usdkrw_ecos` input으로 전환 |
| feature catalog | `fx_usdkrw_ret_5d` | true | ECOS `fx_usdkrw_ecos` input으로 전환 |
| feature catalog | `fx_usdkrw_ecos_level` | false | ECOS validation alias, `level` |
| feature catalog | `fx_usdkrw_ecos_ret_5d` | false | ECOS validation alias, `ret_5d` |
| feature catalog | `macro_cpi_level` | true | monthly, `level` |
| feature catalog | `macro_cpi_yoy_latest` | true | monthly, `yoy` |
| feature catalog | `macro_cpi_mom_latest` | true | monthly, `mom` |
| feature catalog | `macro_ppi_level` | true | monthly, `level` |
| feature catalog | `macro_ppi_yoy_latest` | true | monthly, `yoy` |
| feature catalog | `macro_ppi_mom_latest` | true | monthly, `mom` |
| feature catalog | `macro_m2_level` | true | monthly, `level` |
| feature catalog | `macro_m2_yoy_latest` | true | monthly, `yoy` |
| feature catalog | `macro_m2_mom_latest` | true | monthly, `mom` |
| feature catalog | `macro_consumer_sentiment_level` | true | monthly, `level` |
| feature catalog | `rate_us2y_level` | true | FRED official, `level` |
| feature catalog | `rate_us10y_level` | true | FRED official, `level` |
| feature catalog | `rate_us_term_spread_10y_2y` | true | FRED official, multi input `spread` |
| feature catalog | `commodity_wti_spot_ret_20d` | true | FRED official WTI spot, `ret_20d`; FDR futures feature와 병행 |
| feature catalog | `commodity_wti_fred_ret_20d` | false | FRED validation alias, `ret_20d`; active spot feature는 `commodity_wti_spot_ret_20d` |
| feature catalog | `market_kospi_krx_close` | false | KRX direct validation alias, `level` |
| feature catalog | `market_kospi_krx_ret_1d` | false | KRX direct validation alias, `ret_1d` |
| feature catalog | `market_kospi_krx_ret_5d` | false | KRX direct validation alias, `ret_5d` |
| feature catalog | `market_kospi_krx_ret_20d` | false | KRX direct validation alias, `ret_20d` |
| feature catalog | `market_kosdaq_krx_ret_1d` | false | KRX direct validation alias, `ret_1d` |
| feature catalog | `market_kospi200_krx_ret_1d` | false | KRX direct validation alias, `ret_1d` |

`active=false` 후보는 기본 `common sync`와 `common build-daily` active-only 경로에는 노출되지 않는다. smoke가 필요할 때는 `common sync --sources ecos --series ... --include-inactive`, `common sync --sources fred --series ... --include-inactive`, `common sync --sources krx --series ... --include-inactive`처럼 explicit allowlist와 inactive 허용 옵션을 함께 지정한다.

## 6. PR 4-E/4-F live smoke 결과

PR 4-D에서 `common sync --sources ecos` CLI dispatch를 열었다. 다만 seed는 계속 inactive이므로 smoke에는 `--include-inactive`가 필요하다.

PR 4-E에서 API key가 있는 로컬 환경으로 아래 제한 smoke를 수행했다.

```bash
uv run krx-collector common seed-catalog

uv run krx-collector common sync \
  --sources ecos \
  --series rate_kr_gov3y \
  --start 2024-01-02 \
  --end 2024-01-05 \
  --include-inactive \
  --rate-limit-seconds 3
```

실행 결과:

| 항목 | 결과 |
|---|---|
| key loading | `ECOS_API_KEY` present |
| seed upsert | series 10, catalog 14 |
| sync status | success |
| provider requests | 1 |
| rows upserted | 4 |
| no-data requests | 0 |
| error count | 0 |

저장 row 확인:

| `TIME` | observation/period end | available from | value | unit |
|---|---|---|---:|---|
| `20240102` | `2024-01-02` | `2024-01-03` | 3.24 | 연% |
| `20240103` | `2024-01-03` | `2024-01-04` | 3.278 | 연% |
| `20240104` | `2024-01-04` | `2024-01-05` | 3.227 | 연% |
| `20240105` | `2024-01-05` | `2024-01-08` | 3.283 | 연% |

확인 사항:

- ECOS `TIME`은 daily series에서 `observation_date`/`period_end_date`로 직접 매핑된다.
- `availability_policy=next_krx_session`이 적용되어 금요일 `2024-01-05` 관측값은 다음 KRX session인 `2024-01-08`부터 사용 가능하다.
- `UNIT_NAME`은 `연%`로 들어오며 raw unit에 저장된다.
- 이번 smoke는 4영업일 단기 확인이므로, active 전환 전 더 긴 기간 coverage 확인이 필요하다.
- 월간 CPI의 release lag 정책 적정성은 아직 미확인이다.

### 6.2 PR 4-F 확장 smoke

PR 4-F에서는 호출 수를 2회로 제한해 `rate_kr_gov3y` 1개월 범위와 `macro_cpi` 3개월 범위를 확인했다.

```bash
uv run krx-collector common sync \
  --sources ecos \
  --series rate_kr_gov3y \
  --start 2024-01-02 \
  --end 2024-01-31 \
  --include-inactive \
  --rate-limit-seconds 3

uv run krx-collector common sync \
  --sources ecos \
  --series macro_cpi \
  --start 2024-01-01 \
  --end 2024-03-31 \
  --include-inactive \
  --rate-limit-seconds 3
```

실행 결과:

| series | range | requests | rows upserted | status | no-data | errors |
|---|---|---:|---:|---|---:|---:|
| `rate_kr_gov3y` | `2024-01-02..2024-01-31` | 1 | 22 | success | 0 | 0 |
| `macro_cpi` | `2024-01-01..2024-03-31` | 1 | 3 | success | 0 | 0 |

`rate_kr_gov3y` 확인:

| 항목 | 결과 |
|---|---|
| Mon-Fri target days | 22 |
| observed rows | 22 |
| missing Mon-Fri days | 0 |
| observed range | `2024-01-02..2024-01-31` |
| value range | `3.191..3.313` |
| unit | `연%` |

`macro_cpi` 저장 row 확인:

| `TIME` | observation/period end | available from | value | unit |
|---|---|---|---:|---|
| `202401` | `2024-01-31` | `2024-02-20` | 113.17 | 2020=100 |
| `202402` | `2024-02-29` | `2024-03-20` | 113.78 | 2020=100 |
| `202403` | `2024-03-31` | `2024-04-22` | 113.95 | 2020=100 |

확인 사항:

- `rate_kr_gov3y`의 stat/item code는 단기 live smoke 기준 정상 동작한다.
- `macro_cpi`의 stat/item code는 단기 live smoke 기준 정상 동작한다.
- `macro_cpi`는 `period_end_date + 20 calendar days`를 적용하며, 결과일이 비거래일이면 다음 KRX session으로 보정된다.
- 두 후보는 계속 `active=false`로 유지한다.

### 6.3 PR 4-G inactive fact/coverage 검증

PR 4-G에서는 inactive 후보를 모델에 기본 노출하지 않으면서도 명시적으로 daily fact와 coverage를 검증할 수 있도록 `common build-daily`와 `common coverage-report`에 `--include-inactive`를 추가했다. 이 옵션은 broad build/report를 막기 위해 `--feature-codes`와 함께만 사용할 수 있다.

검증 명령:

```bash
uv run krx-collector common build-daily \
  --feature-codes rate_kr_gov3y_level \
  --start 2024-01-03 \
  --end 2024-01-31 \
  --include-inactive

uv run krx-collector common coverage-report \
  --feature-codes rate_kr_gov3y_level \
  --start 2024-01-03 \
  --end 2024-01-31 \
  --include-inactive

uv run krx-collector common build-daily \
  --feature-codes macro_cpi_level \
  --start 2024-02-20 \
  --end 2024-04-30 \
  --include-inactive

uv run krx-collector common coverage-report \
  --feature-codes macro_cpi_level \
  --start 2024-02-20 \
  --end 2024-04-30 \
  --include-inactive
```

결과:

| feature | range | target days | facts | nulls | coverage | PIT violations |
|---|---|---:|---:|---:|---:|---:|
| `rate_kr_gov3y_level` | `2024-01-03..2024-01-31` | 21 | 21 | 0 | 1.0000 | 0 |
| `macro_cpi_level` | `2024-02-20..2024-04-30` | 49 | 49 | 0 | 1.0000 | 0 |

`macro_cpi_level` as-of 전환 확인:

| feature date | value | as-of available date |
|---|---:|---|
| `2024-03-19` | 113.17 | `2024-02-20` |
| `2024-03-20` | 113.78 | `2024-03-20` |
| `2024-04-19` | 113.78 | `2024-03-20` |
| `2024-04-22` | 113.95 | `2024-04-22` |

### 6.4 PR 4-H active readiness 기준

PR 4-H에서는 active 전환 전에 확인할 기계적 기준을 `common readiness-report`로 고정했다.

기본 기준:

- required coverage `1.0000`
- null fact 0
- missing target date 0
- PIT violation 0

검증 명령:

```bash
uv run krx-collector common readiness-report \
  --feature-codes rate_kr_gov3y_level \
  --start 2024-01-03 \
  --end 2024-01-31 \
  --include-inactive \
  --required-coverage-ratio 1.0

uv run krx-collector common readiness-report \
  --feature-codes macro_cpi_level \
  --start 2024-02-20 \
  --end 2024-04-30 \
  --include-inactive \
  --required-coverage-ratio 1.0
```

결과:

| feature | range | target days | ready | coverage | nulls | missing | PIT violations | blockers |
|---|---|---:|---|---:|---:|---:|---:|---|
| `rate_kr_gov3y_level` | `2024-01-03..2024-01-31` | 21 | true | 1.0000 | 0 | 0 | 0 | 없음 |
| `macro_cpi_level` | `2024-02-20..2024-04-30` | 49 | true | 1.0000 | 0 | 0 | 0 | 없음 |

해석:

- `rate_kr_gov3y_level`은 단기 readiness를 통과했다. active 전환 전 최근 3~12개월 운영 범위로 같은 기준을 재확인한다.
- `macro_cpi_level`은 daily fact 기준은 통과했지만, 월간 지표 발표일을 `manual_lag_days=20`으로 근사하고 있다. 공식 release calendar 또는 발표일 source를 확보하기 전까지 active 전환하지 않는다.

### 6.5 ECOS 10Y/spread inactive smoke

ECOS `StatisticItemList`에서 `rate_kr_gov10y` item code를 확인한 뒤 inactive seed를 재적용하고, 2024년 1월 단기 범위로 10Y-3Y spread 경로를 검증했다.

검증 명령:

```bash
uv run krx-collector common seed-catalog --init-schema

uv run krx-collector common sync \
  --sources ecos \
  --series rate_kr_gov10y,rate_kr_gov3y \
  --start 2024-01-02 \
  --end 2024-01-31 \
  --include-inactive \
  --rate-limit-seconds 3

uv run krx-collector common build-daily \
  --feature-codes rate_kr_term_spread_10y_3y \
  --start 2024-01-03 \
  --end 2024-01-31 \
  --include-inactive
```

결과:

| 항목 | 결과 |
|---|---:|
| seed upsert | series 11, catalog 16 |
| raw sync requests | 1 attempted, 1 skipped(existing 3Y coverage) |
| `rate_kr_gov10y` rows upserted | 22 |
| spread target dates | 21 |
| spread facts built | 21 |
| spread nulls | 0 |
| spread coverage | 1.0000 |
| spread PIT violations | 0 |
| readiness | true |

해석:

- `rate_kr_gov10y` ECOS item code와 provider wiring은 단기 smoke 기준 정상이다.
- spread builder의 실제 seed 경로도 source trace/as-of/PIT 기준을 만족했다.
### 6.6 ECOS 10Y/spread 운영 범위 확대 검증

단기 smoke 이후, 3Y active 전환 때와 같은 기준으로 최근 3개월/12개월 범위를 검증했다. 외부 ECOS API 요청은 sync 두 번에서 총 4회 발생했고 재시도는 없었다.

검증 명령:

```bash
uv run krx-collector common sync \
  --sources ecos \
  --series rate_kr_gov10y,rate_kr_gov3y \
  --start 2026-03-09 \
  --end 2026-06-10 \
  --include-inactive \
  --rate-limit-seconds 3

uv run krx-collector common build-daily \
  --feature-codes rate_kr_term_spread_10y_3y \
  --start 2026-03-10 \
  --end 2026-06-11 \
  --include-inactive

uv run krx-collector common sync \
  --sources ecos \
  --series rate_kr_gov10y,rate_kr_gov3y \
  --start 2025-06-09 \
  --end 2026-06-10 \
  --include-inactive \
  --rate-limit-seconds 3

uv run krx-collector common build-daily \
  --feature-codes rate_kr_term_spread_10y_3y \
  --start 2025-06-10 \
  --end 2026-06-11 \
  --include-inactive
```

결과:

| 범위 | sync requests | rows upserted | target days | facts | nulls | coverage | PIT violations | readiness |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| 3개월 `2026-03-10..2026-06-11` | 2 | 128 | 68 | 68 | 0 | 1.0000 | 0 | true |
| 12개월 `2025-06-10..2026-06-11` | 2 | 494 | 256 | 256 | 0 | 1.0000 | 0 | true |

해석:

- 10Y raw 수집과 10Y-3Y spread daily fact 경로가 최근 3개월/12개월 범위 모두에서 통과했다.
- active 전환에 필요한 기계적 기준(coverage 1.0000, null 0, missing 0, PIT 위반 0, readiness true)을 만족한다.
- `rate_kr_gov10y`, `rate_kr_gov10y_level`, `rate_kr_term_spread_10y_3y`를 `active=true`로 전환했다.

### 6.7 ECOS 10Y/spread active-only 검증

active 전환 후 seed를 재적용하고 `--include-inactive` 없이 12개월 범위를 재검증했다.

결과:

| 항목 | 결과 |
|---|---:|
| seed upsert | series 11, catalog 16 |
| active-only sync requests | 2 |
| active-only sync rows upserted | 494 |
| build target dates | 256 |
| features built | 2 |
| facts built | 512 |
| nulls | 0 |
| coverage | 1.0000 |
| PIT violations | 0 |
| readiness | true |

해석:

- `rate_kr_gov10y_level`과 `rate_kr_term_spread_10y_3y`는 active-only 경로에서 `--include-inactive` 없이 조회/빌드/리포트가 가능하다.
- active-only sync는 raw count 기반 skip 조건을 만족하지 못해 ECOS 요청 2회를 수행했다. 이는 build coverage 실패가 아니라 raw 일간 observation count와 KRX target day count의 차이 때문이다.

### 6.8 ECOS USD/KRW source code 확인 및 active 전환

Next-B 첫 단계로 ECOS 공식 USD/KRW 후보 코드를 API metadata와 provider 경로에서 확인했다. 이후 로컬 DB에서 validation feature coverage/readiness를 확인하고 active feature input을 전환했다.

공식 확인:

| 항목 | 결과 |
|---|---|
| `StatisticItemList` | `731Y001` |
| `ITEM_CODE` | `0000001` |
| `ITEM_NAME` | `원/미국달러(매매기준율)` |
| `CYCLE` | `D` |
| `START_TIME` | `19640504` |
| `UNIT_NAME` | `원` |

provider smoke:

| 범위 | records | first | last | unit | value range |
|---|---:|---|---|---|---|
| `2024-01-02..2024-01-10` | 7 | `2024-01-02` | `2024-01-10` | `원` | first `1289.4` |
| `2025-06-09..2026-06-10` | 247 | `2025-06-09` | `2026-06-10` | `원` | `1352.6..1546.5` |

DB validation:

| 범위 | raw rows | target dates | facts | nulls | coverage | PIT violations | readiness |
|---|---:|---:|---:|---:|---:|---:|---|
| 단기 `2024-01-10..2024-01-31` | 22 | 16 | 32 | 0 | 1.0000 | 0 | true |
| 3개월 `2026-03-17..2026-06-11` | 64 | 63 | 126 | 0 | 1.0000 | 0 | true |
| 12개월 `2025-06-17..2026-06-11` | 247 | 251 | 502 | 0 | 1.0000 | 0 | true |

active 전환 후 검증:

| 항목 | 결과 |
|---|---:|
| seed upsert | series 12, catalog 18 |
| active-only sync | 0 attempted, 1 skipped |
| active feature build | target 251, facts 502, null 0 |
| active feature coverage/readiness | coverage 1.0000, PIT 0, ready true |
| source trace | `fx_usdkrw_level`/`fx_usdkrw_ret_5d` facts use `source_series_ids=["fx_usdkrw_ecos"]` |

해석:

- ECOS provider parameter와 row parser는 USD/KRW 공식 원천에도 그대로 동작한다.
- `ret_5d`는 초기 history 부족을 피하기 위해 raw 시작일보다 충분히 뒤에서 coverage/readiness를 평가했다.
- active 전환 후 기존 feature code의 source trace가 ECOS series를 가리키는 것을 확인했다.

### 6.9 FRED US rates/WTI live smoke 및 active 전환

Next-C-2에서 `FRED_API_KEY`가 있는 로컬 환경으로 FRED provider를 검증했다. 1차 구현은 latest observation만 저장하고, `realtime_start`/`realtime_end`는 raw payload에 보존한다.

검증 명령 개요:

```bash
uv run krx-collector common seed-catalog

uv run krx-collector common sync \
  --sources fred \
  --series rate_us2y,rate_us10y,commodity_wti_fred \
  --start 2025-06-09 \
  --end 2026-06-10 \
  --include-inactive \
  --rate-limit-seconds 1

uv run krx-collector common build-daily \
  --feature-codes rate_us2y_level,rate_us10y_level,rate_us_term_spread_10y_2y,commodity_wti_fred_ret_20d \
  --start 2025-07-08 \
  --end 2026-06-11 \
  --include-inactive
```

raw sync 결과:

| 범위 | requests | rows upserted | no-data | errors |
|---|---:|---:|---:|---:|
| 단기 `2024-01-02..2024-01-31` | 3 | 63 | 0 | 0 |
| 3개월 `2026-03-09..2026-06-10` | 3 | 196 | 0 | 0 |
| 12개월 `2025-06-09..2026-06-10` | 3 | 751 | 0 | 0 |

12개월 raw 저장 상태:

| series | rows | first obs | last obs | null values |
|---|---:|---|---|---:|
| `rate_us2y` | 251 | `2025-06-09` | `2026-06-09` | 0 |
| `rate_us10y` | 251 | `2025-06-09` | `2026-06-09` | 0 |
| `commodity_wti_fred` | 249 | `2025-06-09` | `2026-06-08` | 0 |

daily fact 검증:

| 범위 | feature set | target days | facts | nulls | coverage | PIT violations | readiness |
|---|---|---:|---:|---:|---:|---:|---|
| 단기 `2024-01-03..2024-01-31` | US rates + 10Y-2Y spread | 21 | 63 | 0 | 1.0000 | 0 | true |
| 3개월 `2026-04-08..2026-06-11` | US rates + spread + WTI ret20 | 47 | 188 | 0 | 1.0000 | 0 | true |
| 12개월 `2025-07-08..2026-06-11` | US rates + spread + WTI ret20 | 236 | 944 | 0 | 1.0000 | 0 | true |

active 전환:

| 대상 | 상태 |
|---|---|
| `rate_us2y`, `rate_us10y` | `active=true` 전환 |
| `rate_us2y_level`, `rate_us10y_level`, `rate_us_term_spread_10y_2y` | `active=true` 전환 |
| `commodity_wti_fred`, `commodity_wti_fred_ret_20d` | WTI spot/futures 비교 전까지 inactive 유지 |

active-only 검증:

| 항목 | 결과 |
|---|---:|
| seed upsert | series 15, catalog 22 |
| active-only sync | 0 attempted, 2 skipped |
| active feature build | target 256, facts 768, null 0 |
| active feature coverage/readiness | coverage 1.0000, PIT 0, ready true |

해석:

- 미국 2Y/10Y 금리와 10Y-2Y spread는 단기/3개월/12개월 검증과 active-only 검증을 통과해 active 전환했다.
- FRED WTI는 provider와 `ret_20d` transform 자체는 통과했지만, 기존 active `commodity_wti_ret_20d`가 FDR `CL=F` 선물 기반이라 spot series인 `DCOILWTICO`와 의미가 다르다. Next-C-3에서 값 차이를 비교해 교체가 아니라 병행 노출로 결정했다.

### 6.10 FRED WTI spot vs FDR WTI futures 비교

Next-C-3에서는 기존 active `commodity_wti_ret_20d`(FDR `CL=F`, futures)와 FRED `commodity_wti_fred_ret_20d`(FRED `DCOILWTICO`, spot)를 같은 feature date 범위에서 비교했다.

비교 준비:

```bash
uv run krx-collector common sync \
  --sources fdr \
  --series commodity_wti \
  --start 2025-06-09 \
  --end 2026-06-10 \
  --rate-limit-seconds 1

uv run krx-collector common build-daily \
  --feature-codes commodity_wti_ret_20d \
  --start 2025-07-08 \
  --end 2026-06-11

uv run krx-collector common build-daily \
  --feature-codes commodity_wti_fred_ret_20d \
  --start 2025-07-08 \
  --end 2026-06-11 \
  --include-inactive
```

raw/fact 준비 결과:

| 항목 | 결과 |
|---|---:|
| FDR `commodity_wti` raw sync | 1 request, 253 rows upserted |
| FDR `commodity_wti_ret_20d` build | target 236, facts 236, null 0 |
| FRED `commodity_wti_fred_ret_20d` build | target 236, facts 236, null 0 |

paired feature 비교(`2025-07-08..2026-06-11`, 236 days):

| 지표 | 값 |
|---|---:|
| correlation | 0.991004 |
| average difference(FDR - FRED) | -0.001684 |
| average absolute difference | 0.012379 |
| median absolute difference | 0.008586 |
| p95 absolute difference | 0.030431 |
| max absolute difference | 0.134789 |
| sign match ratio | 0.9280 |
| `abs(diff) > 1pp` | 93 days |
| `abs(diff) > 2pp` | 35 days |
| `abs(diff) > 5pp` | 9 days |

큰 차이 예시:

| feature date | FDR futures ret20 | FRED spot ret20 | diff |
|---|---:|---:|---:|
| `2025-07-09` | 0.046562 | -0.088228 | 0.134789 |
| `2025-07-08` | -0.034811 | -0.116731 | 0.081920 |
| `2025-07-22` | -0.019121 | -0.096804 | 0.077683 |
| `2025-07-15` | -0.082214 | -0.007857 | -0.074357 |
| `2026-06-10` | -0.100642 | -0.039142 | -0.061500 |

raw level 비교(`2025-06-09..2026-06-08`, 249 paired observations):

| 지표 | 값 |
|---|---:|
| level correlation | 0.998721 |
| average level difference(FDR - FRED) | -0.976948 |
| average absolute level difference | 1.053253 |
| max absolute level difference | 4.320003 |

결정:

- 기존 `commodity_wti_ret_20d`는 FDR `CL=F` futures 기반 feature로 유지한다.
- FRED `DCOILWTICO` spot 기반 feature는 `commodity_wti_spot_ret_20d`라는 별도 active feature로 병행 노출한다.
- 기존 validation alias `commodity_wti_fred_ret_20d`는 inactive로 유지한다.

active-only 검증:

| 항목 | 결과 |
|---|---:|
| seed upsert | series 15, catalog 23 |
| active-only FRED WTI sync | 0 attempted, 1 skipped |
| `commodity_wti_spot_ret_20d` build | target 236, facts 236, null 0 |
| coverage/readiness | coverage 1.0000, PIT 0, ready true |
| source trace | `source_series_ids=["commodity_wti_fred"]` |

### 6.11 Next-D-1 KRX direct provider-level smoke

Next-D-1에서는 DB write 없이 provider 경로만 짧게 확인했다. `common_features_krx` provider는 `.env`의 KRX credential을 사용해 `LOGOUT` 응답 시 login retry를 수행한다. credential 없이 direct client로 `MDCSTAT00301`을 호출하면 `LOGOUT`이 반환될 수 있다.

검증 범위:

| 항목 | 값 |
|---|---|
| range | `2026-06-08..2026-06-10` |
| source | `KRX` |
| endpoint | `dbms/MDC/STAT/standard/MDCSTAT00301` |
| output key | `output` |
| DB write | 없음 |

provider 결과:

| series | records | first observation/value | status |
|---|---:|---|---|
| `market_kospi_krx` | 3 | `2026-06-10`, `7730.82` | success |
| `market_kosdaq_krx` | 3 | `2026-06-10`, `951.63` | success |
| `market_kospi200_krx` | 3 | `2026-06-10`, `1227.12` | success |

추가 확인:

- `finder_equidx`에서 KOSPI/KOSPI200 매핑은 `full_code=1`, `short_code=001/028`, KOSDAQ 매핑은 `full_code=2`, `short_code=001`임을 확인했다.
- 이 smoke는 provider parsing과 KRX auth/session 경로 확인까지만 의미한다. raw DB upsert, inactive daily build, coverage/readiness, pykrx 대비 값 비교는 Next-D-2에서 수행했다.

### 6.12 Next-D-2 KRX direct DB readiness 및 active 전환

Next-D-2에서는 로컬 DB에 KRX direct 후보를 seed/sync/build하고, 기존 pykrx fallback과 값을 비교한 뒤 active 전환했다.

seed:

| 항목 | 결과 |
|---|---:|
| source series | 18 total, 14 active |
| feature catalog | 29 total, 19 active |

raw sync:

| step | range | requests | skipped | rows upserted |
|---|---|---:|---:|---:|
| KRX direct short | `2026-05-11..2026-06-10` | 3 | 0 | 63 |
| KRX direct 3M | `2026-03-09..2026-06-10` | 3 | 0 | 192 |
| pykrx fallback 3M | `2026-03-09..2026-06-10` | 3 | 0 | 192 |
| active-only KRX after holiday fix | `2026-03-09..2026-06-10` | 0 | 3 | 0 |

holiday/calendar correction:

- KRX endpoint와 local calendar 차이로 `2026-01-01`, `2026-02-16`, `2026-02-17`, `2026-02-18`, `2026-03-02`, `2026-05-01`, `2026-05-05`, `2026-05-25`, `2026-06-03`을 `docs/holidays_krx.csv`에 추가했다.
- 이 보정 전에는 `2026-03-09..2026-06-10` local target이 68일이었으나 KRX raw는 64 rows라 active-only sync skip이 되지 않았다.
- holiday 보정 후 target은 64일이 되었고 active-only KRX sync는 `0 attempted / 3 skipped`로 바뀌었다.
- 보정 전 생성된 `common_feature_daily_fact` 휴장일 row 165개는 삭제했다.

active feature build/readiness(`2026-04-08..2026-06-11`, corrected target 43 dates):

| feature | facts | nulls | coverage | PIT violations | ready |
|---|---:|---:|---:|---:|---|
| `market_kospi_close` | 43 | 0 | 1.0000 | 0 | true |
| `market_kospi_ret_1d` | 43 | 0 | 1.0000 | 0 | true |
| `market_kospi_ret_5d` | 43 | 0 | 1.0000 | 0 | true |
| `market_kospi_ret_20d` | 43 | 0 | 1.0000 | 0 | true |
| `market_kosdaq_ret_1d` | 43 | 0 | 1.0000 | 0 | true |
| `market_kospi200_ret_1d` | 43 | 0 | 1.0000 | 0 | true |

pykrx fallback vs KRX direct paired comparison(`2026-04-08..2026-06-11`, 43 paired dates):

| feature pair | max absolute diff | differing days |
|---|---:|---:|
| `market_kospi_close` vs `market_kospi_krx_close` | 0.00000000 | 0 |
| `market_kospi_ret_1d` vs `market_kospi_krx_ret_1d` | 0.00000000 | 0 |
| `market_kospi_ret_5d` vs `market_kospi_krx_ret_5d` | 0.00000000 | 0 |
| `market_kospi_ret_20d` vs `market_kospi_krx_ret_20d` | 0.00000000 | 0 |
| `market_kosdaq_ret_1d` vs `market_kosdaq_krx_ret_1d` | 0.00000000 | 0 |
| `market_kospi200_ret_1d` vs `market_kospi200_krx_ret_1d` | 0.00000000 | 0 |

source trace 확인:

| feature | source series |
|---|---|
| `market_kospi_close`/`market_kospi_ret_*` | `["market_kospi_krx"]` |
| `market_kosdaq_ret_1d` | `["market_kosdaq_krx"]` |
| `market_kospi200_ret_1d` | `["market_kospi200_krx"]` |

결정:

- 기존 모델용 국내 지수 feature code는 유지하고 input만 KRX direct series로 전환했다.
- pykrx source series(`market_kospi`, `market_kosdaq`, `market_kospi200`)는 fallback으로 보존하되 `active=false`로 격하했다.
- KRX direct validation alias(`market_*_krx_*`)는 비교 추적용 inactive feature로 유지한다.

### 6.13 Next-D-3a/3b KRX breadth/liquidity inactive slice

2026-06-11 재검증에서는 직전 세션에서 보였던 `MDCSTAT01501`/`finder_equidx` HTTP 403이 재현되지 않았다. 기존 `KrxMdcClient` warmup/session/header 경로로 두 endpoint 모두 정상 응답했다.

endpoint smoke:

| endpoint | params | output | result |
|---|---|---|---|
| `dbms/MDC/STAT/standard/MDCSTAT01501` | `mktId=STK`, `trdDd=20260610` | `OutBlock_1` | 947 rows |
| `dbms/MDC/STAT/standard/MDCSTAT01501` | `mktId=KSQ`, `trdDd=20260610` | `OutBlock_1` | 1822 rows |
| `dbms/comm/finder/finder_equidx` | `mktsel=1` | `block1` | 163 rows |

`MDCSTAT01501` 집계 규칙:

| metric | rule |
|---|---|
| `advancers` | `FLUC_TP_CD in ('1', '4')` |
| `decliners` | `FLUC_TP_CD in ('2', '5')` |
| `unchanged` | `FLUC_TP_CD = '3'` |
| `total_turnover_value` | `sum(ACC_TRDVAL)` |

2026-06-10 provider-level smoke:

| market | rows | fluc_counts | advancers | decliners | unchanged | turnover |
|---|---:|---|---:|---:|---:|---:|
| KOSPI | 947 | `{'0': 26, '1': 341, '2': 550, '3': 28, '4': 2}` | 343 | 550 | 28 | 39944843351120 |
| KOSDAQ | 1822 | `{'0': 86, '1': 493, '2': 1175, '3': 61, '4': 6, '5': 1}` | 499 | 1176 | 61 | 9148499012943 |

inactive seed:

| source series | feature | active |
|---|---|---|
| `market_kospi_advancers_krx` | `market_kospi_advancers_count` | false |
| `market_kospi_decliners_krx` | `market_kospi_decliners_count` | false |
| `market_kospi_unchanged_krx` | `market_kospi_unchanged_count` | false |
| `market_kospi_turnover_value_krx` | `market_kospi_turnover_value` | false |
| `market_kosdaq_advancers_krx` | `market_kosdaq_advancers_count` | false |
| `market_kosdaq_decliners_krx` | `market_kosdaq_decliners_count` | false |
| `market_kosdaq_unchanged_krx` | `market_kosdaq_unchanged_count` | false |
| `market_kosdaq_turnover_value_krx` | `market_kosdaq_turnover_value` | false |

local DB smoke:

| step | result |
|---|---:|
| seed 재적용 | `common_feature_series=26`, `common_feature_catalog=37` |
| raw sync | 8 series processed, 8 requests, 8 rows upserted |
| build daily | 8 features, 1 target date, 8 facts, null 0 |
| coverage/readiness | coverage `1.0000`, missing 0, PIT violations 0, ready true |

fact date는 `next_krx_session` 정책에 따라 raw `2026-06-10` 관측값을 `2026-06-11` feature row로 노출했다. `source_series_ids`는 각 inactive KRX breadth/liquidity source를 정확히 가리키는 것을 확인했다.

`finder_equidx` code discovery:

- KOSPI/KOSPI200: `full_code=1`, `short_code=001/028`
- KOSDAQ/KOSDAQ150: `full_code=2`, `short_code=001/203`
- KRX300: `full_code=5`, `short_code=300`
- KOSPI 업종 예: 전기전자 `full_code=1`, `short_code=013`, 금융 `full_code=1`, `short_code=021`
- KOSDAQ 업종 예: 제조 `full_code=2`, `short_code=024`, 제약 `full_code=2`, `short_code=066`

VKOSPI는 `finder_equidx`와 pykrx KRX menu catalog에서 확인되지 않았다. pykrx 파생상품 catalog에는 `V-KOSPI Futures` 상품군만 존재한다. 현물 VKOSPI feature는 별도 endpoint 확인 전까지 seed하지 않는다.

### 6.14 Next-D-3c KRX breadth/liquidity active 전환

Next-D-3c에서는 하루치 smoke를 최근 3개월 범위로 확장한 뒤, 8개 breadth/liquidity feature를 active 전환했다.

운영 범위 검증:

| step | range | result |
|---|---|---:|
| raw sync | `2026-03-09..2026-06-10` | 8 series, 8 requests, 512 rows upserted |
| daily build | `2026-03-10..2026-06-11` | 64 target dates, 512 facts, null 0 |
| coverage | `2026-03-10..2026-06-11` | 모든 feature coverage `1.0000`, missing 0, PIT violations 0 |
| readiness | `2026-03-10..2026-06-11` | 모든 feature ready true |

active-only 검증:

| step | result |
|---|---:|
| seed 재적용 | `common_feature_series=26`, `common_feature_catalog=37` |
| active-only sync | 8 series processed, 0 attempted, 8 skipped |
| active-only build | 64 target dates, 512 facts, null 0 |
| active-only coverage/readiness | coverage `1.0000`, PIT 0, ready true |

3개월 값 범위:

| feature | min | max | avg |
|---|---:|---:|---:|
| `market_kospi_advancers_count` | 42 | 840 | 398.6563 |
| `market_kospi_decliners_count` | 71 | 876 | 486.4219 |
| `market_kospi_unchanged_count` | 3 | 59 | 28.5000 |
| `market_kospi_turnover_value` | 20827490324014 | 80332510678397 | 37018525930328.6094 |
| `market_kosdaq_advancers_count` | 75 | 1553 | 721.8125 |
| `market_kosdaq_decliners_count` | 160 | 1634 | 928.4375 |
| `market_kosdaq_unchanged_count` | 27 | 117 | 67.2500 |
| `market_kosdaq_turnover_value` | 8929291241211 | 21316111092807 | 13872455965285.1719 |

구현 보정:

- `common_features_krx` provider에 `MDCSTAT01501` row cache를 추가했다. 같은 provider 인스턴스에서 동일 `(bld, output_key, market, trade_date)` 응답을 재사용해 KOSPI 4개 metric과 KOSDAQ 4개 metric이 같은 원천 row를 공유한다.
- active 전환 후에도 넓은 broad sync를 막기 위한 CLI guard는 유지된다. inactive가 아니라 active가 되었으므로 명시 `--series` 없이 `--sources krx`를 실행하면 KRX active series 전체가 대상이 된다.

### 6.15 Next-D-4 KRX industry index inactive slice

`finder_equidx`로 확인한 소수 업종 후보만 `MDCSTAT00301` 기존 provider 경로에 태워 inactive 검증했다. VKOSPI 현물 지수 endpoint는 여전히 미확정이므로 이번 seed에 포함하지 않았다.

inactive source candidates:

| series_id | index | indIdx | indIdx2 | market |
|---|---:|---:|---:|---|
| `industry_krx_semiconductor_krx` | `5044` | `5` | `044` | KRX |
| `industry_kospi_electronics_krx` | `1013` | `1` | `013` | KOSPI |
| `industry_kospi_financials_krx` | `1021` | `1` | `021` | KOSPI |
| `industry_kosdaq_pharma_krx` | `2066` | `2` | `066` | KOSDAQ |

inactive feature candidates:

| source | features |
|---|---|
| KRX 반도체 | `industry_krx_semiconductor_level`, `industry_krx_semiconductor_ret_1d` |
| KOSPI 전기전자 | `industry_kospi_electronics_level`, `industry_kospi_electronics_ret_1d` |
| KOSPI 금융 | `industry_kospi_financials_level`, `industry_kospi_financials_ret_1d` |
| KOSDAQ 제약 | `industry_kosdaq_pharma_level`, `industry_kosdaq_pharma_ret_1d` |

provider-level live smoke:

| series_id | range | result |
|---|---|---:|
| `industry_krx_semiconductor_krx` | `2026-06-08..2026-06-10` | 3 rows, latest `15751.10` |
| `industry_kospi_electronics_krx` | `2026-06-08..2026-06-10` | 3 rows, latest `128680.80` |
| `industry_kospi_financials_krx` | `2026-06-08..2026-06-10` | 3 rows, latest `1153.44` |
| `industry_kosdaq_pharma_krx` | `2026-06-08..2026-06-10` | 3 rows, latest `10739.11` |

local DB smoke:

| step | range | result |
|---|---|---:|
| seed 재적용 | - | `common_feature_series=30`, `common_feature_catalog=45` |
| raw sync | `2026-03-09..2026-06-10` | 4 series, 4 requests, 256 rows upserted |
| daily build | `2026-03-10..2026-06-11` | 8 features, 64 target dates, 512 facts, null 4 |
| coverage | `2026-03-10..2026-06-11` | level 4개 coverage `1.0000`; ret_1d 4개는 첫 날 warm-up null로 `0.9844`; PIT 0 |
| coverage/readiness | `2026-03-11..2026-06-11` | 8개 모두 coverage `1.0000`, missing/null/PIT 0, ready true |

`ret_1d`의 첫 target date null은 raw 시작일의 직전 관측값이 없어서 생긴 정상 warm-up row다. 모델 노출 후보 검증은 warm-up을 제외한 `2026-03-11..2026-06-11` 구간 기준으로 통과했다.

3개월 값 범위(warm-up 제외):

| feature | min | max | avg |
|---|---:|---:|---:|
| `industry_krx_semiconductor_level` | 9033.98000000 | 17653.33000000 | 12896.778412698413 |
| `industry_krx_semiconductor_ret_1d` | -0.09478953 | 0.11880369 | 0.00863913460317460317 |
| `industry_kospi_electronics_level` | 65406.95000000 | 150234.30000000 | 98190.360000000000 |
| `industry_kospi_electronics_ret_1d` | -0.08880170 | 0.11359404 | 0.01139164634920634921 |
| `industry_kospi_financials_level` | 905.13000000 | 1259.92000000 | 1080.2874603174603175 |
| `industry_kospi_financials_ret_1d` | -0.08546375 | 0.07400938 | 0.00378645777777777778 |
| `industry_kosdaq_pharma_level` | 10320.78000000 | 17420.86000000 | 14164.476825396825 |
| `industry_kosdaq_pharma_ret_1d` | -0.10453168 | 0.06366806 | -0.00578900952380952381 |

### 6.16 Next-E CPI conservative release policy active 전환

Next-E 1차 범위는 공식 발표일 calendar table을 새로 붙이지 않고, 기존 `manual_lag_days` 정책을 모델 노출 가능한 보수 정책으로 고정하는 것이다. `macro_cpi`와 level/YoY/MoM feature는 active로 전환했다. 공식 release calendar가 준비되면 현재 보수 lag를 교체한다.

policy:

| 항목 | 값 |
|---|---|
| source series | `macro_cpi` |
| availability policy | `manual_lag_days` |
| lag anchor | `period_end_date` |
| lag | 20 calendar days |
| KRX 보정 | lag 결과일이 비거래일이면 다음 KRX session |
| active 전환 | 완료. 공식 release calendar가 준비되면 policy 교체 |

active feature candidates:

| feature | transform | unit |
|---|---|---|
| `macro_cpi_level` | `level` | `2020=100` |
| `macro_cpi_yoy_latest` | `yoy` | `pct` |
| `macro_cpi_mom_latest` | `mom` | `pct` |

local DB smoke:

| step | range | result |
|---|---|---:|
| seed 재적용 | - | `common_feature_series=30`, `common_feature_catalog=47` |
| raw sync | `2025-01-01..2026-05-31` | 1 series, 1 request, 17 rows upserted |
| daily build | `2026-02-20..2026-06-11` | 3 features, 75 target dates, 225 facts, null 0 |
| inactive coverage/readiness | `2026-02-20..2026-06-11` | 3개 모두 coverage `1.0000`, missing/null/PIT 0, ready true |
| active-only sync | `2025-01-01..2026-05-31` | 1 series, 1 request, 17 rows upserted |
| active-only build | `2026-02-20..2026-06-11` | 3 features, 75 target dates, 225 facts, null 0 |
| active-only coverage/readiness | `2026-02-20..2026-06-11` | 3개 모두 coverage `1.0000`, missing/null/PIT 0, ready true |

active 전환 후 seed 상태:

| 항목 | 값 |
|---|---:|
| active source series | 23 |
| inactive source series | 7 |
| active features | 30 |
| inactive features | 17 |

availability 확인:

| period_end | available_from | value |
|---|---|---:|
| `2026-01-31` | `2026-02-20` | 118.03 |
| `2026-02-28` | `2026-03-20` | 118.40 |
| `2026-03-31` | `2026-04-20` | 118.80 |
| `2026-04-30` | `2026-05-20` | 119.37 |
| `2026-05-31` | `2026-06-22` | 119.92 |

`2026-06-11` feature row에는 `2026-05-31` CPI가 노출되지 않고, `2026-04-30` CPI(`available_from=2026-05-20`)가 유지된다.

값 범위:

| feature | min | max | avg |
|---|---:|---:|---:|
| `macro_cpi_level` | 118.03000000 | 119.37000000 | 118.6069333333333333 |
| `macro_cpi_yoy_latest` | 0.01998622 | 0.02569170 | 0.02156957320000000000 |
| `macro_cpi_mom_latest` | 0.00313480 | 0.00479798 | 0.00372942320000000000 |

### 6.17 Next-E-3 PPI/M2/CSI monthly macro inactive smoke

Next-E-3에서는 CPI에서 검증한 conservative monthly availability 패턴을 PPI/M2/CSI 후보로 확장했다. 세 source와 7개 feature는 모두 `active=false`로 유지하며, 기본 active-only sync/build에는 노출하지 않는다.

metadata/provider 확인:

| series | ECOS table | item code | provider smoke |
|---|---|---|---:|
| `macro_ppi` | `404Y014` 생산자물가지수(기본분류) | `*AA` 총지수 | `2026-01..2026-05` 4 rows, latest `2026-04` |
| `macro_m2` | `161Y005` M2 상품별 구성내역(평잔, 계절조정계열) | `BBHS00` | `2026-01..2026-05` 3 rows, latest `2026-03` |
| `macro_consumer_sentiment` | `511Y002` 소비자동향조사(전국, 월) | `FME`/`99988` 전체 | `2026-01..2026-05` 5 rows, latest `2026-05` |

local DB smoke:

| step | range | result |
|---|---|---:|
| seed 재적용 | - | active series 23, inactive series 10, active features 30, inactive features 24 |
| raw sync | `2025-01-01..2026-05-31` | 3 series, 3 requests, 48 rows upserted |
| daily build | `2026-02-20..2026-06-11` | 7 features, 75 target dates, 525 facts, null 0 |
| coverage/readiness | `2026-02-20..2026-06-11` | 7개 모두 coverage `1.0000`, missing/null/PIT 0, ready true |

raw 저장 범위:

| series | first period | latest period | rows | latest available_from |
|---|---|---|---:|---|
| `macro_ppi` | `2025-01-31` | `2026-04-30` | 16 | `2026-05-20` |
| `macro_m2` | `2025-01-31` | `2026-03-31` | 15 | `2026-04-20` |
| `macro_consumer_sentiment` | `2025-01-31` | `2026-05-31` | 17 | `2026-06-22` |

daily fact 값 범위:

| feature | min | max | avg |
|---|---:|---:|---:|
| `macro_ppi_level` | 122.56000000 | 128.43000000 | 124.6796000000000000 |
| `macro_ppi_yoy_latest` | 0.01904049 | 0.06900283 | 0.03654433426666666667 |
| `macro_ppi_mom_latest` | 0.00587467 | 0.02457120 | 0.01270123426666666667 |
| `macro_m2_level` | 4113407.10000000 | 4132066.90000000 | 4122161.850666666667 |
| `macro_m2_yoy_latest` | 0.04668880 | 0.05559797 | 0.05125201960000000000 |
| `macro_m2_mom_latest` | 0.00004067 | 0.00787530 | 0.00410435426666666667 |
| `macro_consumer_sentiment_level` | 99.20000000 | 112.10000000 | 107.8306666666666667 |

PIT 확인:

- `2026-05-20` 기준 `macro_ppi_level`은 `2026-04-30` 관측값(`available_from=2026-05-20`)까지 노출된다.
- `2026-06-11` 기준 `macro_consumer_sentiment_level`은 `2026-04-30` 관측값을 유지한다. `2026-05-31` 관측값은 `available_from=2026-06-22`라 아직 노출되지 않는다.

### 6.18 Next-E-4 PPI/M2/CSI active 전환

Next-E-4에서는 공식 monthly release calendar를 새로 도입하지 않고, CPI와 동일한 conservative `period_end_date + 20 calendar days` 정책으로 PPI/M2/CSI를 active 전환했다. 공식 발표 calendar가 준비되면 CPI/PPI/M2/CSI 전체 monthly macro policy를 교체한다.

active-only local DB smoke:

| step | range | result |
|---|---|---:|
| seed 재적용 | - | active series 26, inactive series 7, active features 37, inactive features 17 |
| active-only raw sync | `2025-01-01..2026-05-31` | 3 series, 3 requests, 48 rows upserted |
| active-only daily build | `2026-02-20..2026-06-11` | 7 features, 75 target dates, 525 facts, null 0 |
| active-only coverage/readiness | `2026-02-20..2026-06-11` | 7개 모두 coverage `1.0000`, missing/null/PIT 0, ready true |

catalog 확인:

| 항목 | 값 |
|---|---:|
| active source series | 26 |
| inactive source series | 7 |
| active features | 37 |
| inactive features | 17 |

## 7. 보류 후보

아래는 source code 검증 또는 정책 보강 전까지 active로 전환하지 않는다.

| 후보 | 이유 |
|---|---|
| `commodity_wti_fred_ret_20d` | source-specific validation alias. Active spot feature는 `commodity_wti_spot_ret_20d` |
