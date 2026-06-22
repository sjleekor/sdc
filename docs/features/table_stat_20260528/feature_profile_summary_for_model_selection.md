# 주가예측 모델 선정용 — 피처 프로파일 요약 (단일 프롬프트용)

- 최초 작성: 2026-05-28
- 갱신 일시: 2026-06-18 — **재무 다년도 백필 반영 + 신규 모달리티(수급/공매도, 시장·거시 공통 피처) 추가**
- 출처: `docs/features/table_stat_20260528/statistical_profile_*.md` **21개 프로파일** + [`PLAN.md`](./PLAN.md) §1 인벤토리 압축 요약
- 목적: **하나의 AI 프롬프트**로 「어떤 모델 아키텍처(트리계열 / 시계열 RNN·TCN·Transformer / 멀티모달 / 패널 회귀 등)가 적합한가」를 결정짓기 위한 입력 컨텍스트 제공
- 대상 DB: 로컬 PostgreSQL `mydb` (`.env` 의 `DB_DSN`) — `sj2-server` 프로덕션 DB 의 미러
- 수치 기준: 행수·커버리지는 **PLAN.md §1 의 2026-06-15 로컬 실측값**이 authoritative. 세부 분포(분위수·NULL·카테고리)는 각 프로파일 본문 조사 시점값이며, 시점이 다른 경우 본문에 시점을 병기했다.

> ⚠️ **시점 정합성 주의**: 개별 `statistical_profile_*` 본문의 일부(특히 DART 재무·`stock_metric_fact`)는 **2026-05-28 단일연도(2025) 스냅샷** 기준으로 작성되어 있고, 그 이후 **2015~2025 다년도 백필이 완료**되었다. 따라서 본 요약의 행수/연도 커버리지는 PLAN.md §1(2026-06-15)을 따르며, 분포 세부값은 백필 전 스냅샷일 수 있음을 명시한다. sj2 접근 복구 시 동일 쿼리로 분포를 재집계해야 한다.

---

## 0. 한 줄 요약 (TL;DR)

- **시세(시계열) + 수급/공매도(시계열) + 재무·이벤트(다년도 패널) + 시장·거시 공통 피처(시계열)** 로 구성된 **멀티모달 패널 데이터셋**으로 진화했다.
- **시세 `daily_ohlcv`**: 2,783 종목 × 2007-06-05~2026-06-10, **6.55M 행**. 종목 횡단 학습 컷오프 **2014-01-02**(이상적으로는 수급 join 100% 인 **2015-01-02**) 권장.
- **수급/공매도 `krx_security_flow_raw`**(신규 핵심): **76.4M 행** / 2,779 종목 / 7 metric / 2007~2026. 종목별 **외국인·기관·개인 순매수, 외국인 보유, 공매도 거래량·대금·잔고** — 시세와 동일 `(trade_date,ticker)` 축으로 join 되는 강력한 일별 피처군.
- **재무**: **다년도(2015~2025) 백필 완료**. `dart_financial_statement_raw` 16.9M / `dart_xbrl_fact_raw` 80.1M / `stock_metric_fact` 766K. → **이제 시계열 재무 피처 학습이 가능**(과거 요약의 "재무 시계열 불가" 제약 해소). 단 reprt_code 비대칭·IS 매핑 희소·OFS 미수집 등 정제 이슈는 잔존.
- **시장·거시 공통 피처 `common_feature_*`**(신규): 37 active feature(지수·시장폭·금리·환율·원자재·거시) × KRX 거래일 정렬, **PIT-safe(look-ahead 위반 0)**. 단 **일별 raw 이력이 2025-12-15부터**라 장기 패널엔 백필 필요.
- **무결성**: 전 테이블 자연키 중복 0, 핵심 join(시세↔마스터↔수급) 고아 0~소수. 비표준 표기(`stock_knd`,`se`)·CF 부호·자본잠식 등은 피처 단계 정규화 필요.
- **권장 1차 모델**: ① 시세+수급 단일/이중 모달리티(트리/Boosting + TS-CV) → ② 재무 다년도 PIT 패널 결합(LightGBM/CatBoost/TFT-lite) → ③ 시세+수급+재무 시계열+공통 거시 멀티모달(TFT/Informer/GNN).

---

## 1. 테이블 인벤토리 한눈에 보기 (2026-06-15 로컬 실측, 23개 public 테이블)

### 1.1 수집/원시/팩트 — 피처 1순위

| # | 테이블 | 도메인 | 행수 | 자연키 | 시간 축 / 커버리지 | 종목·corp | 결측·주의 |
|---|---|---|---:|---|---|---:|---|
| 1 | `dart_xbrl_fact_raw` | XBRL 팩트 원시 | **80,143,928** | `(corp,year,reprt,rcept,context,concept,…)` | **2015~2025**, 4종 보고서(11011~14) | 2,606 | value_numeric NULL ~9%, dimensions 99.4% |
| 2 | `krx_security_flow_raw` | KRX 수급/공매도 원시 | **76,446,601** | `(trade_date,ticker,market,metric_code,source)` | 2007-06-05~2026-06-10 | 2,779 | 7 metric, 2 source(KRX/PYKRX) 중복분 dedupe 필요 |
| 3 | `dart_financial_statement_raw` | 재무제표 원시 | **16,887,271** | `(corp,year,reprt,fs_div,sj_div,account_id,ord)` | **2015~2026** | 2,608 | `fs_div=CFS` 만, frmtrm 컬럼 50~78% NULL |
| 4 | `dart_shareholder_return_raw` | 배당/자기주식 | **7,831,054** | `(corp,year,reprt,stmt_type,…,metric_code,rcept)` | **2015~2025** | 2,650 | `stock_knd` 50+ 비표준, value_numeric ~77% NULL |
| 5 | `daily_ohlcv` | 일봉 시세 | **6,550,517** | `(trade_date,ticker,market)` | 2007-06-05~2026-06-10 | 2,783 | OHL=0 정지일 1.6%, volume=0 2.1% |
| 6 | `stock_metric_fact` | 정규화 종목 지표(피처 직후보) | **765,966** | `(ticker,metric_code,bsns_year,reprt_code)` | **2015~2025** | 2,650 | NULL 0(정제완료), IS 5종 매핑 희소 |
| 7 | `dart_share_count_raw` | 주식수 변동 | **312,329** | `(corp,year,reprt,se,rcept)` | **2015~2025** | 2,650 | `se` 139종 비표준, 수치 43~70% NULL |
| 8 | `dart_xbrl_document` | XBRL 문서 메타 | **81,532** | `(corp,year,reprt,rcept_no)` | **2015~2025**, 11011~14 | 2,606 | NULL 0, fact 와 4-key 1:1 정합 |
| 9 | `common_feature_daily_fact` | 거래일 정렬 공통 피처 팩트 | **5,550** | `(feature_date,feature_code)` | 2025-11-03~2026-06-12 | 37 feat | PIT 위반 0, 초기구간 value NULL 16.5% |
| 10 | `common_feature_observation_raw` | 시장/거시 원천 observation | **2,752** | `(source,series,obs_date,period_end,release,vintage)` | 2024-09-30~2026-06-12 | 26 series | 일별 raw 2025-12-15부터, NULL 0 |
| 11 | `operating_source_document` | 섹터 영업 KPI 원천 | **0** | — | 미적재 | — | Wave 4(적재 후 프로파일) |
| 12 | `operating_metric_fact` | 섹터 영업 KPI 팩트 | **0** | — | 미적재 | — | Wave 4 |

### 1.2 마스터/카탈로그/브리지 — 보조

| # | 테이블 | 용도 | 행수 | 핵심 | 비고 |
|---|---|---|---:|---|---|
| 13 | `dart_corp_master` | corp↔ticker 브리지 | 116,503 | active listed pair **2,657** | ticker NULL 97.4%(비상장), modify_date 2017~2026, 스냅샷 ~52일 지연 |
| 14 | `stock_master_snapshot_items` | 마스터 스냅샷 항목 | 56,357 | 21 snapshot / 2,783 ticker | PIT universe 재구성용 |
| 15 | `stock_master` | 종목 마스터 현행 | 2,783 | active **2,769**(KOSDAQ 1,833/KOSPI 950) | DELISTED 14, source FDR |
| 16 | `stock_master_snapshot` | 스냅샷 메타 | 21 | 2026-04-10~2026-06-10 | 2026-05-21 02:17 부분 스냅샷(KOSPI만 948) 주의 |
| 17 | `common_feature_catalog` | 공통 피처 카탈로그 | 54 | active 37 / 12 category | transform 7종(level/ret/yoy/mom/spread) |
| 18 | `common_feature_catalog_input` | 피처↔series 브리지 | 56 | active 39 mapping | 고아 0, fx_usdkrw(FDR) raw 존재하나 미사용 |
| 19 | `common_feature_series` | 원천 series 카탈로그 | 33 | active 26(ECOS7/FDR5/FRED3/KRX11) | 산업지수 4 + PYKRX 3 inactive |
| 20 | `metric_catalog` | 재무 지표 카탈로그 | 29 | active 29, fact 사용 29/29 | financial22/share2/SR1/xbrl4 |
| 21 | `metric_mapping_rule` | 지표 매핑 규칙 | 59 | active 59, CFS우선/OFS폴백 | 미사용 XBRL fallback 6(2023+ 신규) |

### 1.3 운영/동기화 — 경량

| # | 테이블 | 행수 | 핵심 | 비고 |
|---|---|---:|---|---|
| 22 | `ingestion_runs` | 171 | run_type 11, success 144/running 16/failed 7/partial 4 | stale running 16건, `krx_flow_sync` success=0 이나 데이터는 존재(로그 신뢰도 낮음) |
| 23 | `sync_checkpoints` | 1 | `remote_db_sync.daily_ohlcv` cursor=2026-06-10 | 마지막 remote sync 성공 2026-06-14, 로컬 lag 0 |

---

## 2. 모달리티별 핵심 프로파일

### 2.1 시계열 모달리티 ① — `daily_ohlcv` (핵심 라벨/피처)

- 컬럼: `trade_date, ticker, market(KOSPI/KOSDAQ), open, high, low, close, volume, source(PYKRX)`. PK `(trade_date,ticker,market)` 중복 0.
- **규모**: 6.55M 행 / 2,783 종목 / 2007-06-05~2026-06-10. 시장 **KOSDAQ 60.2% / KOSPI 39.8%**(KOSDAQ 1,830 vs KOSPI 950).
- **종목 횡단 컷오프**: 2007~2013 은 1~2 종목만(백테스트성) → **2014-01-02 이후** 권장. 2014: 1,678 종목 → 2026: 2,783.
- **거래정지/기준가 유지일**: `open=high=low=0 & close>0` = **1.597%**(약 104K 행, pykrx 규약 — 데이터 오류 아님). 이로 인해 `close>high` 항등식 위배가 2.34% 카운트됨 → `is_halted` 플래그 또는 `(o,h,l)=close` 임퓨테이션 필요.
- **거래량 0**: 2.12%. 종가는 0 없음(비거래일에도 직전 기준가 유지) → 수익률 계산 안전.
- **가격(close) 분위수**: p01 ≈ 569원, p50 ≈ 7,060원, **p99 ≈ 262,673원**, max ≈ 4.6M원 → log 변환/percentile-clip 필수.
- **거래대금(close×volume)**: p50 ≈ 6.5억, p95 ≈ 267억, p99 ≈ 1,168억, max ≈ 109.6조(2007 단일 이상치) → robust scaler/winsor.
- **정합성**: `stock_master`(2,783) 와 `(ticker,market)` 완전 정합(고아 0). 최신일 활동 종목 ~99.6% 적재.

**라벨 후보**: 미래 N일 수익률 `close.shift(-N)/close−1`, 다음날 방향, 변동성. **피처 후보**: lag수익률(1/5/20/60), σ_20, 거래대금 z-score, RSI/MACD/BB, market dummy, `is_halted`.

### 2.2 시계열 모달리티 ② — `krx_security_flow_raw` (수급/공매도, 신규 핵심)

- **규모**: **76.4M 행** / 2,779 종목 / 2007-06-05~2026-06-10 / **7 metric_code** / 2 source(`KRX`,`PYKRX`). 시세와 동일한 `(trade_date,ticker,market)` 축 → daily_ohlcv 에 직접 LEFT JOIN.
- **metric_code 7종**: `foreign_holding_shares`, `foreign_net_buy_volume`, `individual_net_buy_volume`, `institution_net_buy_volume`, `short_selling_volume`, `short_selling_value`, `short_selling_balance_quantity`.
- **커버리지 비대칭**: 외국인보유는 거의 전종목/전기간, 투자자별 순매수는 ~2,400 종목, 공매도 잔고는 **2016-06-30 시작**·최신일 ~916 종목으로 가장 희소.
- **source 중복**: KRX/PYKRX 동일 자연키(source 제외) 약 19.9M 행 중복이나 값 충돌 0 → **`(trade_date,ticker,market,metric_code)` 기준 KRX 우선 dedupe** 필요(약 49.8M distinct).
- **NULL/0**: value NULL 0%. value=0 비율은 metric별 4.7%(외국인보유)~52%(공매도) — "사건 없음" 의미.
- **극단치**: 보유주식수·순매수·공매도대금 모두 fat-tail(예: foreign_holding max 3.46B주, short_value max 886B원) → log/winsor 또는 `short_value/(close×volume)` 같은 비율 피처 권장.
- **시세 join**: 2015년 이후 **100%**, 2014년 95%, 2007~2013 은 daily_ohlcv 가 단일종목이라 <1% → 학습 컷오프를 **2015-01-02** 로 잡으면 시세+수급 모두 안정.
- **주의**: 개인+기관+외국인 순매수 합 ≠ 0(기타법인 metric 미포함) → 합계 항등식으로 검증 금지.

**피처 후보**: 외국인/기관/개인 순매수 누적·z-score, 외국인 지분율 변화, 공매도 비중(`short_volume/volume`)·대금비율·잔고 변화 — 모멘텀/역추세 시그널의 핵심.

### 2.3 재무 모달리티 (다년도 패널 — 백필 완료)

> 과거(2026-05-28) 요약의 "재무는 2025 단일·시계열 불가" 제약은 **해소**되었다. 아래 행수/연도는 2026-06-15 기준.

#### 2.3.1 `dart_financial_statement_raw` (DART 표준 재무제표)

- **16.9M 행 / 2,608 corp / 2015~2026** / `fs_div=CFS`(연결)만(별도 OFS 미수집).
- `sj_div`: BS 36% / CF 34% / CIS 20% / SCE 8% / IS 1.3%. `account_id` 표준코드 미사용 6.8%(account_nm 폴백 필요).
- 직전기 컬럼 `frmtrm_amount`(50% NULL)·`bfefrmtrm_amount`(78% NULL) → 과거 비교는 연도 self-join 권장. 금액 fat-tail(±수십조) → log/winsor.

#### 2.3.2 `dart_xbrl_fact_raw` + `dart_xbrl_document` (XBRL 멀티 차원)

- **fact 80.1M / 문서 81,532 / corp 2,606 / 2015~2025 / 4종 보고서(11011~14)**. 문서↔fact 4-key **1:1 완전 정합(고아 0)**.
- `value_numeric` 채움률 ~91%, `dimensions` 99.4%(세그먼트·차원분석 가능), context instant/duration ≈ 64/36, 통화 KRW ~85%.
- 단위/부호 극단 → 통화 분리 + log + dimensions 해시 키 필요(concept_id 단독 식별 불가).

#### 2.3.3 `stock_metric_fact` (정규화된 피처 직후보)

- **766K 행 / 2,650 종목 / 29 metric_code / 2015~2025 / NULL 0% / UQ 중복 0**.
- source_table 분포(2026-05-28 단일연도 스냅샷 기준): FS ~76% / SC ~13% / XBRL ~8% / SR ~3%. 단위 KRW ~83% / shares ~17%.
- `stock_master`/`daily_ohlcv` 와 ticker 완전 정합(고아 0). corp 기준 SMF 단독 499개(SC/SR 만 있고 FS 없는 종목) → `has_fs` 플래그 권장.
- 현금흐름 부호 합리적(investing 음수 76%), **자본잠식 12종**.
- **한계**: IS 5종(`revenue/cogs/operating_income/net_income/sga`) 매핑이 ~120~200 종목에 그침(BS/CF/shares 는 2,100+ 종목 커버) → 카탈로그 보강 또는 XBRL 폴백(미사용 rule 6개 존재) 필요. 결측-aware 학습기(LightGBM) 또는 core metric subset 권장.

### 2.4 시장·거시 공통 피처 모달리티 — `common_feature_*` (신규)

- **종목 무관, 전 종목 공통(cross-sectional broadcast)** 시장·거시 시계열. `common_feature_daily_fact`(5,550행, 37 active feature, KRX 거래일 정렬), 원천 `common_feature_observation_raw`(2,752행, 26 series, ECOS/FDR/FRED/KRX).
- **카테고리 12종**: 지수, 시장폭(breadth), 금리, 환율, 원자재, 글로벌지수/리스크, 거시(통화·물가·심리) 등. transform 7종(level/ret_1d/5d/20d/yoy/mom/spread).
- **PIT-safe**: `available_from_date ≤ feature_date` 위반 **0**, `asof_available_date` 위반 0. 일별 가용 지연 p95 1~3영업일, ECOS 월별 거시는 의도적 20일 지연.
- **한계**: 일별 raw 이력이 **2025-12-15 시작** → daily_fact 초기 구간(2025-11~12) value NULL 16.5% + ret_20d 등 warm-up NULL. 월별 거시는 2024-09 부터. **2015+ 장기 패널엔 4개 source 백필 필요**.
- **결합**: feature_date 가 daily_ohlcv 거래일과 147/150 정합(3일은 공통피처 단독·휴일 캘린더 점검 필요). 종목 패널에 날짜 키로 broadcast join, **피처별 valid-start gate + max_stale_business_days**(거시 45~90일) 준수.

### 2.5 이벤트 모달리티 (희소·불규칙)

#### 2.5.1 `dart_shareholder_return_raw` (배당·자기주식)

- **7.83M 행 / 2,650 corp / 2015~2025**. `stmt_type` treasury_stock ~59% / dividend ~41%, metric_code 8종.
- value_numeric ~77% NULL(value_text 폴백 100%), `stock_knd` 50+ 비표준 표기 → 보통주/우선주 정규화 사전 필요.

#### 2.5.2 `dart_share_count_raw` (주식수 변동)

- **312K 행 / 2,650 corp / 2015~2025**. corp_cls K:Y ≈ 68:32.
- 수치 6컬럼 NULL 43~70%, 항등식 `distb = istc − tesstk` 위배 다수(NULL→0 임퓨테이션 규칙 필요). `se`(주식 종류) **139종 비표준** → 자연키 포함이라 raw 단계 정규화 불가, **피처 단계 정규화** 필요. `se='합계'` 행이 수치 채움률 99.7% 로 앵커.

---

## 3. 조인·정합성 한눈에 보기

| 키 | 매핑 | 정합 상태 |
|---|---|---|
| `(trade_date,ticker,market)` | 시세 ↔ 수급 | flow 2,779 ⊂ master 2,783(고아 0). 시세 join 2015+ 100% / 2014 95% / 2007~2013 <1% |
| `corp_code ↔ ticker` | DART corp ↔ KRX 종목 | `dart_corp_master` active pair 2,657 ⊂ master 2,783, 스냅샷 ~52일 지연. 이름 join 금지(불일치 32) |
| `(corp,year,reprt,rcept_no)` | XBRL 문서 ↔ XBRL 팩트 | **1:1 완전 정합(81,532=81,532)**, 양방향 고아 0 |
| FS_raw corp ∩ SR/SC corp | — | SR·SC(2,650)가 상위집합, FS(2,608)에 없는 ~499 corp 존재(비외감 등) |
| `ticker` (SMF) ∩ master/시세 | 2,650 ⊂ 2,783 | 고아 0. master 기준 133 종목이 SMF 미보유(신규상장·매핑갭) |
| `feature_date` ↔ 시세 거래일 | 공통피처 ↔ 시세 | 147/150 정합(3일 공통피처 단독, 휴일 캘린더 점검) |

---

## 4. 데이터 한계 (모델 선정 시 반드시 고려)

1. **시점 정합성**: 개별 프로파일 일부 분포값은 2026-05-28 단일연도 스냅샷 기준 → **다년도 백필 후 재집계 필요**. 행수/커버리지는 2026-06-15 기준으로 갱신됨.
2. **수급 source 중복**: `krx_security_flow_raw` 는 KRX/PYKRX 중복 ~19.9M 행 → KRX 우선 dedupe 필수(값 충돌 0).
3. **시세·수급 종목 횡단성**: 2007~2013 종목 1~2개 → **컷오프 2015 권장**(수급 join 100% 구간).
4. **reprt_code 비대칭**: XBRL/문서/FS 는 4종 보고서(11011~14) 보유, SR·SC 는 사업보고서(11011) 위주 → 분기/중간 이벤트 시계열은 채널별 일관성 확보 필요.
5. **재무 OFS 미수집**: `fs_div=CFS`(연결)만 → 연결 vs 별도 비교 모델 불가.
6. **IS(손익) 매핑 부족**: `stock_metric_fact` 의 revenue/cogs/operating_income/net_income/sga 매핑이 ~120~200 종목 → BS/CF/shares(2,100+) 대비 희소. 결측-aware 학습기 또는 XBRL 폴백 보강.
7. **공통 피처 단기 이력**: 일별 raw 2025-12-15 시작 → 장기 패널엔 4 source 백필 선행. 피처별 warm-up·stale window 준수(거시 최대 45~90영업일).
8. **이상치·정지일·비표준 표기**: OHL=0 정지일 `is_halted` 표준화; `stock_knd`(50+)·`se`(139) 정규화; 자본잠식 12종 비율 피처 무한대/음수 처리; CF 부호 signed-log.
9. **마스터 시점성**: `dart_corp_master` 스냅샷 ~52일 지연, `stock_master_snapshot` 2026-05-21 부분 스냅샷(KOSPI만) 존재 → PIT universe 재구성 시 `(as_of_date, MAX(fetched_at))` + record_count 검증.

---

## 5. 모델 선정을 위한 의사결정 가이드 (제안)

> ※ 본 섹션은 **AI 가 답할 영역**이므로 가이드라인만 제공.

### 5.1 1차(즉시 시작 가능 — 시세 + 수급)

- **모달리티**: `daily_ohlcv` + `krx_security_flow_raw`(KRX dedupe).
- **데이터셋**: 2015-01-02 ~ 2026-06-10, ~2,500 종목.
- **모델 후보**: LightGBM/XGBoost/CatBoost(TA·통계 + 수급 피처 + lag + 시장더미); 1D-CNN/TCN/GRU(window 60~120); N-BEATS/N-HiTS/PatchTST.
- **CV**: Purged Walk-Forward(TS-CV), gap = label horizon.

### 5.2 2차(재무 다년도 패널 결합)

- **모달리티**: 시세 + 수급 + `stock_metric_fact`(2015~2025 다년도) + 공통 거시 피처(가용 구간).
- **모델 후보**: LightGBM/CatBoost(범주형·결측 강함), TabNet, **TFT-lite**.
- **주의**: PIT lookahead 방지 — 재무는 보고서 `rcept_dt` 공시일 이후, 공통 피처는 `available_from_date` 이후로만 가용. `has_fs` / 커버리지 마스크 적용.

### 5.3 3차(완전 멀티모달 시계열 — 공통 피처 백필 후)

- **모달리티**: 시세 + 수급 + 재무 시계열(2015~) + 이벤트(배당·자사주) + 시장·거시 공통 피처.
- **모델 후보**: Temporal Fusion Transformer(TFT), Informer, 멀티-인덱스 LightGBM, Graph Neural Net(섹터·산업 그래프).

### 5.4 라벨 후보

- 회귀: `r_{t+1,t+N} = close_{t+N}/close_t − 1`, log-return, Sharpe-adjusted.
- 분류: 다음 N일 수익률 > 0, Top/Bottom 분위(랭킹), 변동성 레짐.

### 5.5 평가 메트릭

- 회귀: IC, Rank-IC, RMSE. 분류/랭킹: AUC, NDCG@K, 분위 포트폴리오 수익률(롱숏). 경제성: Sharpe, MDD, turnover.

---

## 6. AI 프롬프트에 함께 첨부하면 좋은 1-페이지 요약 (복사용)

```
[프로젝트 컨텍스트]
주가예측 모델을 만들기 위한 한국 시장(KOSPI/KOSDAQ) 데이터를 수집 중. (수치 기준일 2026-06-15)

[가용 데이터]
1) 일봉 시세 daily_ohlcv: 2,783종목 × 2007-06-05~2026-06-10, 6.55M행. KOSDAQ 60%/KOSPI 40%.
   정지일(OHL=0) 1.6%, volume=0 2.1%. close p99=263K원, max=4.6M원(fat-tail). 종목 횡단 컷오프 2014~2015.
2) 수급/공매도 krx_security_flow_raw: 76.4M행 / 2,779종목 / 7 metric(외국인·기관·개인 순매수, 외국인보유,
   공매도 거래량·대금·잔고) / 2007~2026. 시세와 (trade_date,ticker) 동일축 join(2015+ 100%).
   KRX/PYKRX 중복 dedupe 필요(값 충돌 0). 공매도 잔고는 2016-06~, 종목 커버리지 가장 희소.
3) 재무 dart_financial_statement_raw: 16.9M행 / 2,608 corp / 2015~2026 / 연결(CFS)만.
4) XBRL dart_xbrl_fact_raw(80.1M) + dart_xbrl_document(81,532): 2,606 corp / 2015~2025 / 4종 보고서 완비.
   value_numeric 91%, dimensions 99.4%, KRW 85%. 문서-팩트 4-key 1:1 정합.
5) 정규화 종목 지표 stock_metric_fact: 766K행 / 2,650종목 / 29 metric / 2015~2025 / NULL 0.
   원천 FS76%/SC13%/XBRL8%/SR3%. 손익 5종 매핑 종목 120~200개로 희소(BS/CF/shares는 2,100+).
6) 주주환원 dart_shareholder_return_raw: 7.83M행 / 2,650 corp / 2015~2025 / treasury 59%·dividend 41%.
7) 주식수 변동 dart_share_count_raw: 312K행 / 2,650 corp / 2015~2025 / se 139종 비표준 / 수치 NULL 43~70%.
8) 시장·거시 공통 피처 common_feature_daily_fact: 37 active feature(지수·시장폭·금리·환율·원자재·거시) ×
   KRX 거래일 정렬, PIT-safe. 단 일별 raw 이력 2025-12-15~ (장기 패널엔 백필 필요).

[조인 키]
- (trade_date,ticker,market): 시세↔수급. corp_code↔ticker: DART↔KRX(dart_corp_master 브리지).
- (corp,year,reprt,rcept_no): XBRL 문서↔팩트. feature_date: 공통 피처를 종목 패널에 broadcast.

[데이터 한계]
- 재무는 2015~2025 다년도 백필 완료(시계열 학습 가능). 단 OFS 미수집, SR·SC는 사업보고서 위주, 손익 매핑 희소.
- 시세·수급은 2015 이후 종목 풀이 충분. 공통 피처 일별 이력은 짧음(백필 선행).
- 비표준 표기(stock_knd, se), 자본잠식 12종, CF 부호, 수급 source 중복 dedupe 등 정제 필요. PIT 누설 금지.

[질문]
위 데이터 상태에서 1차로 어떤 모델 아키텍처를 선택해야 하나(시세+수급 단독 vs 재무 결합)?
재무 다년도 패널과 공통 거시 피처를 결합할 때 PIT-safe 한 멀티모달 구조는? 라벨 정의, CV 설계, 평가 지표를 함께 제안해 달라.
```

---

## 7. 참고 문서

| 분류 | 테이블 | 상세 프로파일 |
|---|---|---|
| 시세 | `daily_ohlcv` | [statistical_profile_daily_ohlcv.md](./statistical_profile_daily_ohlcv.md) |
| 수급 | `krx_security_flow_raw` | [statistical_profile_krx_security_flow_raw.md](./statistical_profile_krx_security_flow_raw.md) |
| 재무 | `dart_financial_statement_raw` | [statistical_profile_dart_financial_statement_raw.md](./statistical_profile_dart_financial_statement_raw.md) |
| 재무 | `dart_xbrl_fact_raw` | [statistical_profile_dart_xbrl_fact_raw.md](./statistical_profile_dart_xbrl_fact_raw.md) |
| 재무 | `dart_xbrl_document` | [statistical_profile_dart_xbrl_document.md](./statistical_profile_dart_xbrl_document.md) |
| 재무 | `stock_metric_fact` | [statistical_profile_stock_metric_fact.md](./statistical_profile_stock_metric_fact.md) |
| 이벤트 | `dart_shareholder_return_raw` | [statistical_profile_dart_shareholder_return_raw.md](./statistical_profile_dart_shareholder_return_raw.md) |
| 이벤트 | `dart_share_count_raw` | [statistical_profile_dart_share_count_raw.md](./statistical_profile_dart_share_count_raw.md) |
| 공통피처 | `common_feature_daily_fact` | [statistical_profile_common_feature_daily_fact.md](./statistical_profile_common_feature_daily_fact.md) |
| 공통피처 | `common_feature_observation_raw` | [statistical_profile_common_feature_observation_raw.md](./statistical_profile_common_feature_observation_raw.md) |
| 공통피처 | `common_feature_series` | [statistical_profile_common_feature_series.md](./statistical_profile_common_feature_series.md) |
| 공통피처 | `common_feature_catalog` | [statistical_profile_common_feature_catalog.md](./statistical_profile_common_feature_catalog.md) |
| 공통피처 | `common_feature_catalog_input` | [statistical_profile_common_feature_catalog_input.md](./statistical_profile_common_feature_catalog_input.md) |
| 마스터 | `stock_master` | [statistical_profile_stock_master.md](./statistical_profile_stock_master.md) |
| 마스터 | `stock_master_snapshot` | [statistical_profile_stock_master_snapshot.md](./statistical_profile_stock_master_snapshot.md) |
| 마스터 | `stock_master_snapshot_items` | [statistical_profile_stock_master_snapshot_items.md](./statistical_profile_stock_master_snapshot_items.md) |
| 브리지 | `dart_corp_master` | [statistical_profile_dart_corp_master.md](./statistical_profile_dart_corp_master.md) |
| 카탈로그 | `metric_catalog` | [statistical_profile_metric_catalog.md](./statistical_profile_metric_catalog.md) |
| 카탈로그 | `metric_mapping_rule` | [statistical_profile_metric_mapping_rule.md](./statistical_profile_metric_mapping_rule.md) |
| 운영 | `ingestion_runs` | [statistical_profile_ingestion_runs.md](./statistical_profile_ingestion_runs.md) |
| 운영 | `sync_checkpoints` | [statistical_profile_sync_checkpoints.md](./statistical_profile_sync_checkpoints.md) |
| 계획 | — | [PLAN.md](./PLAN.md) |
