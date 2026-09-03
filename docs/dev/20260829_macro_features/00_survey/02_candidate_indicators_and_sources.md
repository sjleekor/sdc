# 02. 후보 매크로 지표 — 현재 카탈로그 대비 공백, 원천, 사용 형태, 우선순위

- 작성일: 2026-08-29
- 입력: `01_macro_predictor_literature_survey.md` §6의 지표 목록, `00_existing_features_vs_macro.md` §5의 제약,
  그리고 2026-08-29 원천 조사(ECOS `StatisticItemList` 실호출 포함).
- 목적: 문헌이 중요하다고 한 지표를 **우리가 이미 갖고 있는 것 / 거의 비용 없이 더할 수 있는 것 / 새 어댑터가
  필요한 것 / 무료로는 못 구하는 것**으로 나눈다. 사전등록 설계는 다음 문서에서 한다.
- 표기: ✅ 카탈로그 active · ◐ 카탈로그에 있으나 inactive/dormant · ➕ 기존 어댑터로 시리즈만 추가 ·
  🆕 새 어댑터 필요 · ✖ 무료 원천 없음. "미확인"은 조사에서 끝까지 확인하지 못한 값이다.

---

## 1. 결론

1. **우선순위 1의 세 축(위험선호·시장 상태·유동성 국면)은 원천이 전부 카탈로그에 있다.** VIX, KOSPI/KOSDAQ
   지수·거래대금·상승하락 종목 수, 한/미 기간 스프레드, 전일 S&P500이 `common_feature_daily_fact`에
   2014-06-16부터 일별로 들어와 있다. **새 수집 없이 interaction(②)과 조건부 IC(③)를 시작할 수 있다.**
   exposure 베타(①)도 USD/KRW·WTI·국고채 10년·S&P500·VIX가 있어 바로 만들 수 있다.
2. **비용이 거의 0인 추가가 셋 있다** — 회사채 AA-/BBB- 3년·CD 91일(ECOS `817Y002`, 국고채와 같은 통계표),
   기준금리(ECOS `722Y001`), 경제심리지수(ECOS `513Y001`). `definitions/common_features.py`에 시리즈를
   추가하고 `common seed → sync`만 돌리면 된다. N8 고용 시리즈가 그렇게 들어간 것과 같은 길이다.
3. **새 어댑터가 필요한 것 중 값이 큰 것은 둘이다** — 관세청 10일 수출 잠정치(반도체 포함, 공공데이터포털
   API, `DATAGO_KEY`가 이미 로컬 `.env`에 있다)와 필라델피아 반도체지수 SOX(무료 일별). 둘 다 학술 검정은
   비어 있고 실무 근거만 있어 우선순위 3이다.
4. **국내 원천은 어디에도 vintage(as-of) 조회가 없다.** ECOS 응답에는 공표일·개정일 필드 자체가 없다. 그래서
   개정되는 지표(경기지수·산업활동·고용·경상수지·수출 잠정치)는 **매 수집 시 전체 시계열을 다시 받아
   수집 시각을 찍어 두는 자체 vintage**가 유일한 방법이다. 개정되지 않는 시세·금리는 문제가 없다.
5. **무료로는 못 구하는 것**은 국가 CDS, DRAM/NAND 계약가, Caixin PMI, ISM PMI, Baker-Wurgler식 한국
   심리지수다. 문헌 근거가 약한 중국 변수와 함께 이번 범위에서 뺀다.

---

## 2. 현재 카탈로그 — 무엇이 이미 있나

`definitions/common_features.py` 기준 시리즈 36개(active 28 · inactive 8), 모델 노출 feature 53개(active 36) — 2026-08-29 코드 집계.
`common_feature_daily_fact`(snapshot 2026-08-12) 실측 커버리지는 `00` §2.2와 같다.

| 축 | 시리즈 (source) | 빈도 | 파생 feature (active) | 실측 시작 |
|---|---|---|---|---|
| 시장 지수 | `market_kospi`·`market_kosdaq`·`market_kospi200` (pykrx) | D | ret 1/5/20d, KOSPI close | 2014-06-16 |
| 시장 폭·유동성 | KOSPI/KOSDAQ advancers·decliners·unchanged·turnover_value (KRX) | D | count·value level | 2014-06-16 |
| 글로벌 | `global_sp500`·`global_nasdaq`·`global_vix` (FDR) | D | S&P·Nasdaq ret_1d, VIX level | 2014-06-16 |
| 환율 | `fx_usdkrw` (FDR)·`fx_usdkrw_ecos` | D | level, ret_5d | 2014-06-16 |
| 금리 | `rate_kr_gov3y`·`rate_kr_gov10y` (ECOS 817Y002), `rate_us2y`·`rate_us10y` (FRED) | D | level, 기간 스프레드 10y−3y / 10y−2y | 2014-06-16 |
| 상품 | `commodity_wti` (FDR 선물)·`commodity_wti_fred` (현물) | D | ret_20d | 2014-06-16 |
| 물가·통화·심리 | `macro_cpi`·`macro_ppi`·`macro_m2`·`macro_consumer_sentiment` (ECOS) | M | level, MoM, YoY (`manual_lag_days=20`) | 2013-06-20 |
| 고용 ◐ | `macro_unemployment_rate`·`macro_employment_rate`·`macro_employed_persons` (ECOS 901Y027) | M | level (Phase C dormant, `12`) | 카탈로그만 |
| 산업지수 ◐ | `industry_*` 4종 (KRX) | — | inactive, 데이터 0 | — |

**있는 것으로 바로 만들 수 있는 국면 변수**(카탈로그에 파생만 추가하면 된다 — `02_feature_candidate.md` §3.6의 M1~M7):

- 시장 상태: KOSPI 12/24/36개월 누적수익 부호(CGH·DM), 20/60일 실현분산, `kosdaq_ret_20d − kospi_ret_20d`
- 유동성 국면: KOSPI/KOSDAQ 거래대금 `log(turnover / median60)`, 회전율 z
- 시장 폭: `(adv−dec)/(adv+dec)`, 20일 AD line, 지수−폭 divergence
- 위험선호: ΔVIX 5/20d, VIX 백분위(과거 창), VIX>20/25 flag, `z(ΔVIX) − z(sp500_ret) + z(usdkrw_ret)` composite
- 금리: 기간 스프레드 Δ·부호 더미, 미-한 10년 금리차, 10년 금리 20일 Δ
- 환율: USD/KRW 20/60일 수익률, z-score
- 물가·통화: CPI/PPI/M2 YoY 가속·감속 더미

---

## 3. 후보 목록 — 축별

우선순위는 `01` §6을 따르고, 원천 열은 이번 조사에서 확인한 값이다.

### 3.1 위험선호·변동성 (우선순위 1)

| 지표 | 원천 / 코드 | 빈도·시작 | 지연·개정 | 상태 | 사용 형태 |
|---|---|---|---|---|---|
| VIX level·ΔVIX·문턱 flag | 카탈로그 `global_vix` | D 2014-06 | 0, 개정 없음 | ✅ (파생 추가) | ②(`px_idio_vol`·`px_maxret`·`flow_foreign_*`), ③ |
| VKOSPI | KRX Open API 파생상품지수 시세정보 (엔드포인트 이용신청 필요) | D 2010-01 | 0 | 🆕 (`market_data_krx_openapi` 확장) | ② — VIX와 중복 확인 후 |
| KOSPI 실현분산 20/60d | 카탈로그 `market_kospi`에서 계산 | D | 0 | ✅ (파생) | ②(모멘텀·변동성 축), ③ |
| 미국 HY OAS | FRED `BAMLH0A0HYM2` | D 1996-12 | 0, ALFRED vintage 有 | ➕ (FRED 어댑터) | ② 글로벌 신용 국면 |
| MOVE | Yahoo `^MOVE` (FRED 없음) | D | 0 | 🆕 | 보류 |
| 국가 CDS 5y | 무료 API 없음 (worldgovernmentbonds·investing.com 화면만; 국제금융센터 다운로드 미제공) | D | — | ✖ | 이번 범위 제외 |

### 3.2 시장 상태·유동성 (우선순위 1, 전부 자체 계산)

| 지표 | 원천 | 상태 | 사용 형태 |
|---|---|---|---|
| KOSPI 12/24/36개월 누적수익 부호 (UP/DOWN) | `market_kospi` close | ✅ (파생) | ②(`px_mom_12_1`, `flow_foreign_*`), ③ |
| `kosdaq_ret_20d − kospi_ret_20d` | 카탈로그 | ✅ (파생) | ②(`fin_log_mcap`, `is_kosdaq`) |
| 시장 거래대금 z / `log(turnover/median60)` | `market_*_turnover_value_krx` | ✅ (파생) | ②(수급 3종·`px_amihud`·`px_turnover_shock`), ③ — `00` §4.3 IQR ρ −0.72의 직접 후속 |
| 시장 폭 `(adv−dec)/(adv+dec)` 20d | breadth 6종 | ✅ (파생) | ②(`px_reversal_5d`, `flow_*`) |
| 시장 전체 투자자별 순매수 (외국인·기관·개인 합계) | KRX Open API 목록에 **없음**. 대안: 기존 MDC `flows sync`의 종목 합산, 또는 KIS 시장별 투자자매매동향 | ✅ (feat_flow 합산) | ② 외국인 순매수 국면 (§5.4 Kang-Kwon-Park) |
| KOSPI200 선물 베이시스·외국인 선물 순매수 | KRX Open API 선물 일별매매정보 + 현물지수로 베이시스 계산; 투자자별은 미제공 | 🆕 | 보류 (근거는 있으나 원천 반쪽) |
| 집계 밸류에이션 (시장 B/M·E/P 중앙값) | `feat_fin_scan_daily`의 횡단면 집계 | ✅ (파생) | ②(개별 밸류 × 집계 밸류; GKX Table 4) |

### 3.3 금리·신용 (우선순위 2~3)

| 지표 | 원천 / 코드 | 빈도·시작 | 지연·개정 | 상태 | 사용 형태 |
|---|---|---|---|---|---|
| 국고채 기간 스프레드 10y−3y level·Δ·부호 | 카탈로그 | D | 0 | ✅ (Δ·부호 파생 추가) | ②(레버리지·규모·밸류), ③ |
| 미-한 10년 금리차 | `rate_us10y − rate_kr_gov10y` | D | 0 | ✅ (파생) | ②(`flow_foreign_*`) — 단 문헌은 주식자금 설명력 부정 |
| 회사채 3년 AA- / BBB- | ECOS `817Y002` 항목 `010300000` / `010320000` | D | 익영업일, 개정 없음 | ➕ | 신용 스프레드 `AA- − 국고3y`, `BBB- − AA-` → ②(레버리지 × 스프레드) |
| CD 91일 · 콜금리 · KORIBOR 3M | ECOS `817Y002` `010502000` / `010101000` / `010150000` | D | 익영업일 | ➕ | CD−국고3y 단기 스프레드 (윤선중-전귀환 2023은 CD−KOFR) |
| 기준금리 | ECOS `722Y001` 항목 `0101000` | D 1999-05 | 당일 | ➕ | 인상/인하 국면 flag |
| 미국 신용 스프레드 BAA−10y | FRED `BAA10Y` | D 1986 | 0, vintage 有 | ➕ | ② 글로벌 신용 국면 |
| 미국 breakeven 10y | FRED `T10YIE` | D 2003 | 0 | ➕ | ② 인플레 기대 국면 |
| 미국 단기금리 tbl | FRED (`DTB3` 등) | D | 0 | ➕ | GWZ 2024 생존 변수 |
| 금리 베타 (`rolling_beta(r_i, Δkr10y)`) | 카탈로그로 계산 | — | — | ✅ (①) | 한국 문헌 공백 — 자체 검정 |

### 3.4 환율·달러 (우선순위 2)

| 지표 | 원천 / 코드 | 상태 | 사용 형태 |
|---|---|---|---|
| USD/KRW 20/60d 수익률·z | 카탈로그 `fx_usdkrw` | ✅ (파생) | ②(`flow_foreign_*`) — 동행 변수, 예측 재료 아님 |
| 환율 베타 · 하방 세미베타 | 카탈로그로 계산 | ✅ (①) | 부호 불안정(고강석 2019) → **양방향 등록**, Chu(2022)식 하방 세미베타 우선 |
| 광의 달러지수 | FRED `DTWEXBGS` (D 2006, vintage 有); DXY는 FRED 없음 → Yahoo `DX-Y.NYB` | ➕ | ② EM 금융조건(BIS) |
| USD/CNY | FRED `DEXCHUS` (D 1981) | ➕ | 보류 (문헌 약함) |
| 원-엔 | ECOS `731Y001` 계열 (항목 미확인) | ➕ | 보류 |

### 3.5 경기·수출·반도체 (우선순위 3, 학술 공백)

| 지표 | 원천 / 코드 | 빈도·시작 | 지연·개정 | 상태 | 사용 형태 |
|---|---|---|---|---|---|
| **수출 10일 잠정치 (10대 품목, 반도체 포함)** | 공공데이터포털 관세청 `15157908` (품목별), `15157941` (국가별) — `DATAGO_KEY` | 10일 2016-01~ | 1~10일치 11일, 1~20일치 21일, 월치 익월 1일 공표; 확정치로 개정 | 🆕 (첫 `DATAGO_KEY` 어댑터) | 수출 YoY z(삼성 경기 축), 반도체 수출 YoY → ①(`beta_semis_export`), ② |
| 월 확정 수출입 (HS 품목) | 공공데이터포털 관세청 `15101609` | M | 매월 15일경 전월 수정 | 🆕 | 위의 확정치 |
| 산업부 20대 품목 (D램/낸드 분리, 2026-06~) | 보도자료만, API 없음 | M | — | ✖ | — |
| SOX | Yahoo `^SOX` / Nasdaq 공식 히스토리 | D | 0 | 🆕 (FDR 어댑터로 가능한지 확인) | ①(`beta_sox`) — KOSPI–SOX 상관 0.45↑ |
| DRAM/NAND 계약가·DXI | DRAMeXchange 유료 (Silver $4K/년), TrendForce 현재값만 | — | — | ✖ | 대체: 반도체 수출 10일치 |
| 경기선행/동행지수 순환변동치 | ECOS `901Y067` `I16E` / `I16D` | M 1970 | **T+30, 매월 과거치 재계산, 기준연도 개편 시 전체 개정** | ➕ (단 §5 vintage 문제) | 우선순위 5 — 선행지수에 KOSPI 포함(순환), 동행·후행 |
| 전산업/광공업 생산 | ECOS `901Y033` `A00` / `AB00` | M 2000 | T+30, 전월치 수정, 연 1회 보정 | ➕ (§5) | 우선순위 5 |
| 소매판매 | ECOS 코드 미확인 (KOSIS 산업활동동향 트리) | M | T+30 | 미확인 | 보류 |
| 미국 ADS · CFNAI · NFCI | Philadelphia Fed(전체 vintage 한 파일) · FRED `CFNAI` · FRED `NFCI` | 주·월 | vintage 有 | ➕/🆕 | ② 글로벌 경기 국면 |
| OECD CLI (MSCI 국면 변수) | OECD API | M | 개정 있음 | 🆕 | 보류 |
| ISM PMI | FRED에서 2016-06 삭제, 유료 | — | — | ✖ | — |
| 중국 NBS PMI · Caixin PMI | NBS `data.stats.gov.cn`(해외 접속 불안정) · Caixin 유료 | M | — | ✖ | 문헌 근거 약함, 제외 |

### 3.6 물가·통화 (우선순위 3)

| 지표 | 원천 | 상태 | 사용 형태 |
|---|---|---|---|
| CPI YoY 가속/감속, CPI z-score(삼성 물가 축) | 카탈로그 `macro_cpi` | ✅ (파생) | ②(밸류·규모 × 인플레 국면) — 전성주 2020 OOS 생존 |
| PPI·M2 YoY | 카탈로그 | ✅ | ② 보조 |
| 미국 CPI·PCE | FRED (`CPIAUCSL`, `PCEPI`) | ➕ | 보류 |

### 3.7 심리·불확실성 (우선순위 4~5)

| 지표 | 원천 | 빈도·시작 | 상태 | 사용 형태 |
|---|---|---|---|---|
| 경제심리지수 ESI 원계열·순환변동치 | ECOS `513Y001` `E1000` / `E2000` | M 2003-01, 당월 말 공표 | ➕ | ② 보조 (동행) |
| 업황 BSI | ECOS `512Y007` (`AA` 업황실적, 업종은 항목2) | M 2009-08 | ➕ | 우선순위 5 (KOSPI가 BSI를 선행) |
| 소비자심리 | 카탈로그 | ✅ | 우선순위 5 |
| 한국 EPU (BBD판 / Cho-Kim판 4개 하위지수) | policyuncertainty.com xls | M 1990-01, 갱신 주기 미명시, 재산정 가능 | 🆕 (xls) | ①(`beta_epu`) — Balcilar 2019 분위 인과만 |
| 미국 EPU | FRED `USEPUINDXD` | D 1985 | ➕ | ① |
| Baker-Wurgler식 한국 심리지수 | **공개 지수 없음.** 원재료: 금융투자협회 FreeSIS/KOFIA OpenAPI(신용공여·예탁금, 서비스 목록 미확인), 공공데이터포털 `15094809`, ECOS `901Y014` 주식시장 월표 | — | ✖ (자체 구성 필요) | SYY 2012의 short leg 가설을 한국에서 보려면 필요. 보류 |

### 3.8 고용 (보류 유지)

| 지표 | 원천 | 상태 | 비고 |
|---|---|---|---|
| 실업률·고용률·취업자 | ECOS `901Y027` (항목2 `I28A` 원계열 / `I28B` 계절조정) | ◐ 카탈로그 dormant | `12`의 Phase C 규율 유지. Yoon-Kim(2025)에서도 비일관. T+10일, vintage 없음 |

### 3.9 상품 (보조)

| 지표 | 원천 | 상태 | 사용 형태 |
|---|---|---|---|
| WTI | 카탈로그 (선물·현물) | ✅ | ①(`beta_wti`) |
| Brent | FRED `DCOILBRENTEU` (D 1987, vintage 有) | ➕ | WTI와 중복 |
| 구리 | FRED `PCOPPUSDM` (M); 일별은 Yahoo `HG=F` | ➕/🆕 | IBK 8변수 중 하나. 보류 |
| 금 | FRED LBMA 삭제 → Yahoo `GC=F` | 🆕 | 보류 |
| BDI | 무료 API 없음 | ✖ | — |

---

## 4. 사용 형태 규칙 — 문헌이 요구하는 변환

`01` §7과 `00` §5를 규칙으로 옮긴다.

1. **level을 그대로 쓰지 않는다.** z-score(과거 창만, 예: 3년 rolling), Δ(5/20/60d), YoY·가속도, 부호 flag,
   분위 중 하나로 변환한다(CPZ 2024, MSCI, IBK). 월간 계열은 YoY 또는 가속·감속 더미.
2. **국면은 시점 t에 관측 가능한 정의만.** 누적수익 부호·VIX 문턱·과거 창 z-score. 사후 확정 변수(침체 날짜)와
   전체 표본 z-score는 금지.
3. **2국면부터 시작한다.** 4국면은 표본이 갈린다(삼성 Slowdown 14개월).
4. **① exposure 베타는 252일 rolling, 최소 유효 짝 126개**로 둔다. 이것은 이 설계의 선택이다 — `price.py`의
   `idio_model_min_valid=126`은 "126세션 창이 완전히 채워졌을 때"라는 다른 뜻이고, market model은 완전 252 창을 요구한다
   (`01_design/06_review_20260829.md` §5.5). **해외 계열은 한국 EOD보다 늦게 끝나는 같은 달력일 값을 쓰지 않는다**
   (`02_feature_candidate.md` §2.2) — S&P500·VIX·WTI는 NY 전일 값이 다음 KRX 세션에 들어온다. 국내 ECOS 계열(환율·국고채)은
   `next_krx_session` 정책이라 fact에서 한 세션 지연된다는 점을 짝짓기에 반영해야 한다(`01_design/02` §2.2).
5. **② interaction은 `characteristic` 단독 대비 증분성**을 함께 본다. 국면 변수 자체는 날짜 상수라 IC가 없고,
   곱한 결과가 특성 단독과 다른지가 검정 대상이다.
6. **PIT.** 월간 계열은 `manual_lag_days=20` 정책을 그대로 쓴다. 공표일이 확실한 계열(수출 10일치: 11·21·1일,
   고용: 둘째 주 수요일)은 `manual_lag_days`를 실제 지연으로 좁힐 수 있지만, **개정 문제(§5)가 해결되기 전에는
   보수적 20일을 유지한다.**

---

## 5. 새 수집의 vintage 문제 — 국내 원천은 as-of가 없다

조사에서 확인한 사실이다.

- ECOS `StatisticSearch` 응답 필드는 `STAT_CODE, STAT_NAME, ITEM_CODE1~4, ITEM_NAME1~4, UNIT_NAME, WGT, TIME,
  DATA_VALUE`뿐이다. **공표일·개정일이 없다.** 개정 이력 조회 기능도 없다.
- KOSIS는 행 단위 `LST_CHN_DE`(최종 갱신일)만 준다. 공표 일정 API는 없다.
- 관세청·KRX도 as-of 조회가 없다.
- vintage가 있는 곳은 FRED/ALFRED, Philadelphia Fed ADS, Chicago Fed CFNAI뿐이다.

우리 쪽 상태는 이렇다. `common_feature_observation_raw`에는 `observation_date / period_end_date / release_date /
available_from_date / vintage` 컬럼이 있지만, ECOS provider는 `release_date=None, available_from_date=None`으로
넣고 `manual_lag_days` 정책이 availability를 채운다. 즉 **지연은 처리하지만 개정은 처리하지 않는다** — 오늘
받은 2018년 값은 최신 개정치다. N8 §B1.5가 지적한 것과 같은 한계다.

이것이 지표별로 중요한 정도는 다르다.

| 개정 없음 (문제 없음) | 개정 있음 (자체 vintage 필요) |
|---|---|
| 모든 시세·지수·환율·금리 (국고채·회사채·CD·기준금리·VIX·SOX) | 경기선행/동행지수 (매월 재계산 + 기준연도 개편) |
| 수출 10일 잠정치 **공표 시점 값** | 산업생산·소매판매 (전월 수정, 연 보정) |
| ESI 원계열 | ESI 순환변동치, 고용 계절조정, 경상수지, 수출 확정치 |

**대응.** 개정 있는 지표를 수집할 때는 매 실행마다 전체 시계열을 다시 받아 `fetched_at`으로 vintage를 쌓고,
마트에서 `available_from_date`뿐 아니라 **`vintage <= feature_date`인 관측만 쓰는** 규칙을 추가해야 한다.
현재 ECOS 어댑터가 재수집 시 값이 바뀐 관측을 새 vintage로 남기는지, 덮어쓰는지는 이번 조사에서 확인하지
않았다 — **설계 문서 단계의 첫 확인 항목**이다. 이 문제를 풀기 전까지는 §3.5의 경기·생산 지표를 넣지 않는다.
우선순위가 5인 지표들이라 잃는 것이 적다.

---

## 6. 원천 접근 제한 (참고)

| 원천 | 제한 | 우리 상태 |
|---|---|---|
| ECOS OpenAPI | 1회 최대 건수·일일 한도 **공식 문서 미기재** (커뮤니티 1,000~20,000건으로 엇갈림). 샘플 키는 10건 | 어댑터·키 보유 |
| KOSIS OpenAPI | 1회 40,000셀, 분당 200회, 일일 제한 없음 | 어댑터 없음 |
| 공공데이터포털 (관세청) | 개발계정 10,000건/일 | `DATAGO_KEY` 로컬만, 어댑터 없음. Settings 필드는 이미 있다 |
| FRED | 무료 키, ALFRED vintage | 어댑터·키 보유 |
| KRX Open API | 엔드포인트별 이용신청, 승인 안 된 엔드포인트는 `401 Unauthorized API Call` | 어댑터·키 보유(16개 승인). VKOSPI·선물 엔드포인트는 추가 신청 필요 |
| Yahoo/Stooq (SOX·DXY·MOVE·구리) | 비공식, 약관 주의 | FDR 어댑터가 S&P500·VIX를 어떤 경로로 받는지 확인 후 같은 길 사용 가능한지 판단 |

---

## 7. 미확인 항목 — 설계 전에 채울 것

1. ECOS 정식 키로 `StatisticItemList/…/901Y027`을 돌려 실업률·고용률 정확한 item_code 확인(현재 카탈로그의
   `I61BC`가 맞는지 대조).
2. ECOS `732Y001` 외환보유액 합계 항목, 통관기준 수출입 월표 코드(옛 `901Y011`은 `INFO-200`), 소매판매 코드.
3. ECOS 일일 호출 한도 실측.
4. `common_features_fdr` 어댑터의 심볼 경로 — SOX·DXY를 같은 경로로 받을 수 있는지.
5. ECOS 어댑터의 재수집 동작 — 값이 바뀐 관측을 새 vintage로 남기는가(§5).
6. KRX Open API 파생상품지수(VKOSPI)·선물 일별매매정보 엔드포인트 이용신청 여부.

---

## 8. 정리 — 단계별 후보 묶음

이 표는 우선순위이지 사전등록이 아니다. 실제 family·horizon·부호는 다음 설계 문서에서 **결과를 보기 전에**
못 박는다(`02_feature_candidate.md` §6.2, `12`).

| 단계 | 내용 | 새 수집 | 선행 조건 |
|---|---|---|---|
| **0** | **일별 IC 시계열 저장** (`per_date_market_rank_ic` 결과를 버리지 않는다) | 없음 | 없음. ③의 전제이고 `00` §4.4·`00_읽는_법` §7이 같은 결론 |
| **1** | ① exposure 베타 family: `beta_usdkrw`(양방향·하방 세미베타), `beta_wti`, `beta_kr10y_chg`, `beta_sp500_lag1`, `beta_vix_chg` | 없음 | 4번 규칙(해외 계열 전일 값) |
| **1** | ② interaction 사전등록: `flow_foreign_* × usdkrw_ret_20d`, `flow_foreign_* × ΔVIX`, `px_idio_vol`/`px_maxret × VIX 국면`, 수급 3종 × 거래대금 국면, `px_amihud × 거래대금 국면`, `px_mom_12_1 × 시장 상태(36개월 부호)`, `fin_log_mcap × (kosdaq−kospi 20d)` | 없음 | 국면 변수 파생(§2) |
| **1** | ③ 조건부 IC: 단계 0의 일별 IC를 VIX 국면·시장 상태·유동성 국면 2분할로 | 없음 | 단계 0 |
| **2** | ➕ ECOS `817Y002` 회사채 AA-/BBB-·CD 91일, `722Y001` 기준금리, `513Y001` ESI; FRED `BAA10Y`·`BAMLH0A0HYM2`·`DTWEXBGS`·`T10YIE`·`USEPUINDXD` | 시리즈 정의만 | readiness 창 정렬 문제(N8 §B3) 먼저 |
| **3** | 🆕 관세청 10일 수출(`15157908`), SOX | 어댑터 2개 | `DATAGO_KEY` prod 반영, FDR 경로 확인 |
| 보류 | 경기지수·산업생산·고용·경상수지, VKOSPI·선물, 한국 EPU, 구리·금 | — | §5 vintage 처리 설계 |
| 제외 | CDS, DRAM 가격, Caixin/ISM PMI, BW 심리지수, 중국 변수 | — | 무료 원천 없음 또는 근거 약함 |

단계 0·1은 새 수집이 없다. **다음 설계 문서는 단계 0과 1의 사전등록 계약**이다.
