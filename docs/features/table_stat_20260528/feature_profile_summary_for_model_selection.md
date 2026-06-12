# 주가예측 모델 선정용 — 피처 프로파일 요약 (단일 프롬프트용)

- 작성 일시: 2026-05-28
- 출처: `docs/features/table_stat_20260528/statistical_profile_*.md` 7개 문서 압축 요약
- 목적: **하나의 AI 프롬프트**로 「어떤 모델 아키텍처(트리계열 / 시계열 RNN·TCN·Transformer / 멀티모달 / 패널 회귀 등)가 적합한가」를 결정짓기 위한 입력 컨텍스트 제공
- 대상 DB: PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 시점: 2026-05-28 — 데이터 적재 초기(대부분 `bsns_year=2025`, 분·반기 일부만 적재)

---

## 0. 한 줄 요약 (TL;DR)

- **시세(시계열) 1테이블 + 재무·이벤트(횡단/스냅샷) 6테이블**로 구성된 **하이브리드 패널 데이터셋**.
- **시세**: 종목 2,780 × 거래일 4,674(2007-06-05 ~ 2026-05-21), 약 651만행. 종목 횡단 학습 가능 구간은 **2014-01-02 이후** 권장.
- **재무**: 현재 `bsns_year=2025` 단일·대부분 `reprt_code=11011(사업보고서)` 단일 → **장기 시계열 재무 피처는 사실상 불가**, 횡단/Point-in-time(PIT) 학습부터 시작해야 함.
- **자연 조인 키**: `corp_code↔ticker` (DART↔KRX), `(corp_code,bsns_year,reprt_code,rcept_no)` (보고서 단위), `(ticker,trade_date)` (시세).
- **무결성**: 7테이블 모두 UQ 중복 0, 자연키 충돌 0. 단 일부 컬럼(NULL, 표기 비표준)은 정규화 필요.
- **권장 1차 모델**: ① 시세 단일 모달리티(트리/Boosting + TS-CV) → ② 재무 PIT 피처를 횡단 결합한 LightGBM/CatBoost 패널 모델 → ③ 재무 백필 완료 후 Temporal Fusion Transformer / N-BEATS / TFT-lite 등 멀티모달 시계열 모델.

---

## 1. 테이블 인벤토리 한눈에 보기

| # | 테이블 | 도메인 | 행수 | 자연키 | 시간 축 | 종목/Corp 수 | 결측 핵심 |
|---|---|---|---:|---|---|---:|---|
| 1 | `daily_ohlcv` | 일봉 시세 | **6,517,317** | `(ticker, trade_date)` | 2007-06-05 ~ 2026-05-21 (4,674거래일) | 2,780 | volume=0 = 2.12%, OHL=0 정지일 = 1.60% |
| 2 | `dart_xbrl_fact_raw` | XBRL 팩트 | **18,696,562** | `(corp,year,reprt,rcept,concept,context,unit,dimensions)` | `bsns_year=2025` 단일 | 2,140 corp / 8,255 rcept | value_numeric NULL 9.03%, ticker NULL 비율 일부 |
| 3 | `dart_xbrl_document` | XBRL 문서 메타 | **8,255** | `(corp,year,reprt,rcept_no)` | `bsns_year=2025` 단일, reprt 11011~11014 4종 | 2,140 corp | NULL 0 |
| 4 | `dart_financial_statement_raw` | 재무제표(원시) | **1,254,634** | `(corp,year,reprt,fs_div,sj_div,account_id)` | `bsns_year ∈ {2025,2026}` | 2,151 corp | `fs_div=CFS` 만 존재 |
| 5 | `dart_shareholder_return_raw` | 배당/자기주식 | **263,030** | `(corp,year,reprt,rcept,stmt_type,metric_code,stock_knd,line_idx)` | `bsns_year=2025` + `reprt=11011` 단일 | 2,647 corp / 2,677 보고서 | `stock_knd` 50+ 종 비표준 |
| 6 | `dart_share_count_raw` | 주식수 변동 | **10,295** | `(corp,year,reprt,rcept,se)` | 〃 | 2,647 corp / 2,677 보고서 | `se` 139종 비표준, 수치 NULL 43~70% |
| 7 | `stock_metric_fact` | 정규화 종목 지표(피처 직후보) | **34,411** | `(ticker,date,metric_code,bsns_year,reprt_code)` | `bsns_year=2025` + `reprt=11011` 단일 | 2,647 ticker | NULL 0 (정제 완료) |

> 운영/설정 테이블(`metric_catalog 29`, `metric_mapping_rule 59`, `ingestion_runs 92`, `sync_checkpoints 1`) 및 미적재(Wave 4) 테이블은 본 요약에서 제외.

---

## 2. 모달리티별 핵심 프로파일

### 2.1 시계열 모달리티 — `daily_ohlcv` (핵심 라벨/피처)

- 컬럼: `ticker, market(KOSPI/KOSDAQ/KONEX), trade_date, open, high, low, close, volume, change`
- PK `(ticker, trade_date)` 중복 0. 시장 분포 **KOSDAQ 60.2% / KOSPI 39.8%** (KONEX 미적재).
- **시계열 길이**: 종목당 평균 ~2,344일, 최댓값 4,674일. 단 2007~2013 구간은 **종목 1~2개만 존재**(백필 한계) → **종목 횡단 학습 컷오프 = 2014-01-02 권장**.
- **거래정지/기준가 유지일**: OHL=0 / close>0 행 = 1.597% (pykrx 규약, 데이터 오류 아님) → `is_halted` 플래그 또는 `(open,high,low)=close` 임퓨테이션 필요.
- **거래량 0 (휴장·정지)**: 2.12%.
- **가격 분포(close)**: median ≈ 5,540원, p95 ≈ 59,400원, **p99 ≈ 262,673원**, max ≈ 4.6M원(LG생활건강·삼성바이오 등) → 로그변환 또는 percentile-clip 필수.
- **거래대금**: p99 ≈ 1,168억원 → fat-tail, robust scaler 권장.
- **최신일(2026-05-21) 커버리지**: 활동 종목 99.6% 적재 → 일배치 신선도 양호.
- `stock_master`(2,780) 와 (ticker, market) 완전 정합(2,780=2,780).

#### 활용 가능 라벨/피처

- **라벨 후보**: 미래 N일 수익률 `close.shift(-N)/close - 1`, 다음날 방향, 변동성 등.
- **피처 후보**: lag수익률(1/5/20/60), 변동성(σ_20), 거래대금 z-score, RSI/MACD/BB 등 TA, market dummy, 거래정지 플래그.

### 2.2 재무 모달리티 (Point-in-time)

#### 2.2.1 `dart_financial_statement_raw` (DART 표준 재무제표)

- 1.25M행 / **2,151 corp** / `bsns_year ∈ {2025, 2026}` / `fs_div=CFS`(연결) 만 존재(별도 `OFS` 미수집).
- `account_id` 약 300종, 표준 항목(자산총계·매출액·당기순이익 등) 100% 커버.
- 시계열 모델 학습 전 **2020~2024 백필 필수**.

#### 2.2.2 `dart_xbrl_fact_raw` + `dart_xbrl_document` (XBRL 멀티 차원)

- **fact 18.7M / 문서 8,255 / corp 2,140 / 자연키 8-tuple 중복 0**.
- `value_numeric` 채움률 **90.97%**, `dimensions` 사용률 99.4%(세그먼트·차원분석 가능).
- context: **instant 63.8% / duration 36.2%**, 통화 **KRW 85%**.
- `dart_xbrl_document` 와 (corp,year,reprt,rcept) 4-key **1:1 완전 정합**(양방향 고아 0).
- 보고서 4종(11011·11012·11013·11014) **메타·팩트 모두 적재 완료**. fact 분포: 11011 7.57M / 11012 3.85M / 11013 3.31M / 11014 3.97M.
- `bsns_year=2025` 단일 → **과거 백필 필요**.

#### 2.2.3 `stock_metric_fact` (정규화된 피처 직후보)

- 34,411행 / **NULL 0%** / UQ 중복 0 / 2,647 종목 / 29 metric_code 100% 사용.
- source_table 분포: **FS 75.7% / SC 12.8% / XBRL 8.2% / SR 3.3%** → 4개 원천을 단일 키로 통합.
- 단위: KRW 83.4% / shares 16.6%.
- `stock_master`/`daily_ohlcv` 와 ticker 완전 정합(고아 0).
- 현금흐름 부호 합리적(investing 음수 76%), **자본잠식 종목 12종** 존재.
- 한계: **IS 5종(`revenue/cogs/operating_income/net_income/sga`) 매핑이 121~165 종목에만 적재** → 매핑 룰 보강 또는 XBRL 폴백 필요.
- 시점: `bsns_year=2025` + `reprt=11011` 단일 → **현 시점에서 PIT 1회분 횡단 피처**로만 활용 가능.

### 2.3 이벤트 모달리티 (희소·불규칙)

#### 2.3.1 `dart_shareholder_return_raw` (배당·자기주식)

- 263,030행 / 2,647 corp / 2,677 보고서 / `stmt_type` = treasury_stock 59.2% / dividend 40.8%.
- `metric_code` 8종(현금배당·주식배당·취득·소각·처분 등).
- `bsns_year=2025` + `reprt=11011` 단일 → **분기/중간배당 시계열 모델은 11012~11014 + 과거연도 백필 후 가능**.
- `stock_knd` 50+ 종 비표준 표기(보통주/우선주 정규화 사전 필요).

#### 2.3.2 `dart_share_count_raw` (주식수 변동)

- 10,295행 / 2,647 corp / 2,677 보고서 / `corp_cls` K:Y = 6,990:3,305.
- 수치 6컬럼(`istc_totqy`, `tesstk_co`, `distb_stock_co` 등) NULL 43~70%.
- 항등식 `distb = istc − tesstk` 위배 다수 존재 → 클렌징 필요.
- `se`(주식 종류) **139종 비표준** — 자연키에 포함되어 raw 단계 정규화 불가, **피처 단계에서 정규화** 필요.

---

## 3. 조인·정합성 한눈에 보기

| 키 | 매핑 | 정합 상태 |
|---|---|---|
| `corp_code ↔ ticker` | DART corp ↔ KRX 종목 | XBRL 문서 2,140 ⊂ stock_master 2,780, ticker NULL 0 (메타 테이블 기준) |
| `(corp,year,reprt,rcept_no)` | XBRL 문서 ↔ XBRL 팩트 | **1:1 완전 정합(8,255 = 8,255)**, 양방향 고아 0 |
| FS_raw corp ∩ XBRL doc corp | 2,140 | FS_raw 단독 11개(XBRL 미공시 추정) |
| FS_raw corp ∩ SR corp | 2,148 | SR 이 상위집합(2,647), 비상장/비외감대상 일부 포함 |
| `ticker` (SMF) ∩ stock_master | 2,647 ⊂ 2,780 | 고아 0 |
| `ticker` (SMF) ∩ daily_ohlcv | 2,647 ⊂ 2,780 | 고아 0 |

---

## 4. 데이터 한계 (모델 선정 시 반드시 고려)

1. **시간 축 단일성**: 재무 6테이블 중 5개가 `bsns_year=2025` 단일. → 현재 데이터로 **시계열 재무 모델 학습 불가능**. 시세 시계열 단독 + 재무 PIT 횡단 피처 결합 모델로 시작 권장.
2. **분기 보고서 비대칭**: 메타(`dart_xbrl_document`) 와 XBRL fact 는 4종(11011~11014) 모두 적재 완료 / FS_raw·SR·SC 는 사업보고서(11011) 단일 → 두 채널 일관성 확보 필요.
3. **시세 종목 횡단성**: 2007~2013 종목 1~2개만 → 컷오프 2014 이후 권장.
4. **이상치·정지일**: OHL=0 정지일은 데이터 오류 아님 → `is_halted` 플래그 표준화.
5. **비표준 표기**: `stock_knd`(SR 50+), `se`(SC 139) → 피처 단계 정규화 사전 필요.
6. **자본잠식·음수 자본**: 12종 — 비율 피처(`debt/equity` 등) 무한대/음수 처리 정책 필요.
7. **IS(손익) 매핑 부족**: stock_metric_fact 의 `revenue/cogs/operating_income/net_income/sga` 매핑 종목이 121~165개에 그침 → 모델 입력 직전 카탈로그 보강 필요.

---

## 5. 모델 선정을 위한 의사결정 가이드 (제안)

> ※ 본 섹션은 **AI 가 답할 영역**이므로 가이드라인만 제공.

### 5.1 1차(즉시 시작 가능)

- **모달리티**: 시세 단일.
- **데이터셋**: `daily_ohlcv` (2014-01-02 ~ 2026-05-21, ~2,500 종목).
- **모델 후보**:
  - LightGBM/XGBoost/CatBoost — TA·통계 피처 + lag + 시장더미.
  - 1D-CNN / TCN / GRU — sequence input(window 60~120일).
  - N-BEATS / N-HiTS / PatchTST — 시계열 전용.
- **CV**: Purged Walk-Forward (TS-CV), gap = label horizon.

### 5.2 2차(재무 PIT 결합)

- **모달리티**: 시세 + `stock_metric_fact` 의 PIT 횡단 피처(`bsns_year=2025` snapshot).
- **모델 후보**: LightGBM/CatBoost(범주형 강함), TabNet, **TFT-lite**.
- **주의**: PIT lookahead 방지 — 보고서 `rcept_dt` 공시일 이후로만 피처 가용.

### 5.3 3차(재무 시계열 + 이벤트 — 백필 완료 후)

- **모달리티**: 시세 + 재무 시계열(2020~) + 이벤트(배당·자사주).
- **모델 후보**: Temporal Fusion Transformer(TFT), Informer, 멀티-인덱스 LightGBM, Graph Neural Net(섹터·산업 그래프 결합).

### 5.4 라벨 후보

- 회귀: `r_{t+1,t+N} = close_{t+N}/close_t − 1`, log-return, Sharpe-adjusted.
- 분류: 다음 N일 수익률 > 0, Top/Bottom 분위(랭킹), 변동성 레짐.

### 5.5 평가 메트릭

- 회귀: IC(Information Coefficient), Rank-IC, RMSE.
- 분류/랭킹: AUC, NDCG@K, 분위 포트폴리오 수익률(롱숏).
- 경제성: Sharpe, MDD, turnover.

---

## 6. AI 프롬프트에 함께 첨부하면 좋은 1-페이지 요약 (복사용)

```
[프로젝트 컨텍스트]
주가예측 모델을 만들기 위한 한국 시장(KOSPI/KOSDAQ) 데이터를 수집 중.

[가용 데이터]
1) 일봉 시세 daily_ohlcv: 2,780종목 × 4,674거래일(2007-06-05~2026-05-21), 6.5M행. KOSDAQ 60% / KOSPI 40%. 거래정지일 1.6%, volume=0 2.1%. close p99=263K원, max=4.6M원(fat-tail). 종목 횡단 학습 권장 컷오프=2014-01-02.
2) 종목 마스터 stock_master(2,780).
3) 정규화 종목 지표 stock_metric_fact: 34K행 / 2,647종목 / 29개 metric_code / NULL 0 — 그러나 `bsns_year=2025` 사업보고서 1회분(PIT 1시점). 원천: FS 76%, SC 13%, XBRL 8%, SR 3%. 손익 5종 매핑 종목 121~165개로 부족.
4) DART 재무제표 dart_financial_statement_raw: 1.25M행 / 2,151 corp / 2025-2026 / 연결(CFS)만.
5) DART XBRL dart_xbrl_fact_raw(18.7M) + dart_xbrl_document(8,255): 2,140 corp / 2025년 4종 보고서(사업·반기·1Q·3Q) 완비. value_numeric 91%, dimensions 99.4%, KRW 85%. 문서-팩트 4-key 1:1 정합.
6) 주주환원 dart_shareholder_return_raw: 263K행 / 2,647 corp / 2025·사업보고서 단일 / treasury 59%, dividend 41%.
7) 주식수 변동 dart_share_count_raw: 10K행 / 2,647 corp / 2025·사업보고서 단일 / `se` 139종 비표준 / 수치 NULL 43~70%.

[조인 키]
- corp_code ↔ ticker(stock_master·daily_ohlcv·SMF), (corp,year,reprt,rcept_no)로 XBRL 문서-팩트, (ticker,trade_date)로 시세.

[데이터 한계]
- 재무 데이터는 2025·사업보고서 단일에 가까움(XBRL만 4종 보고서 완비). 시계열 재무 학습은 2020~2024 백필 후 가능.
- 시세는 2014 이후 종목 풀이 충분.
- 자본잠식 12종, 손익 매핑 부족(IS 5종 121~165 종목), 비표준 표기(`stock_knd`,`se`) 정규화 필요.

[제약]
- 라벨: 미래 N일 수익률(회귀) 또는 분위 랭킹(분류) 둘 다 고려 중.
- PIT 누설 금지(공시일 이후로만 재무 피처 사용).

[질문]
위 데이터 상태(현 시점)에서 1차로 어떤 모델 아키텍처를 선택해야 하나? 향후 재무 백필(2020~2024)이 완료되면 어떻게 발전시켜야 하나? 라벨 정의, CV 설계, 평가 지표를 함께 제안해 달라.
```

---

## 7. 참고 문서

| 테이블 | 상세 프로파일 |
|---|---|
| `daily_ohlcv` | [statistical_profile_daily_ohlcv.md](./statistical_profile_daily_ohlcv.md) |
| `dart_xbrl_fact_raw` | [statistical_profile_dart_xbrl_fact_raw.md](./statistical_profile_dart_xbrl_fact_raw.md) |
| `dart_xbrl_document` | [statistical_profile_dart_xbrl_document.md](./statistical_profile_dart_xbrl_document.md) |
| `dart_financial_statement_raw` | [statistical_profile_dart_financial_statement_raw.md](./statistical_profile_dart_financial_statement_raw.md) |
| `dart_shareholder_return_raw` | [statistical_profile_dart_shareholder_return_raw.md](./statistical_profile_dart_shareholder_return_raw.md) |
| `dart_share_count_raw` | [statistical_profile_dart_share_count_raw.md](./statistical_profile_dart_share_count_raw.md) |
| `stock_metric_fact` | [statistical_profile_stock_metric_fact.md](./statistical_profile_stock_metric_fact.md) |
| 계획 | [PLAN.md](./PLAN.md) |
