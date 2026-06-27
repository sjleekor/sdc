# 재무지표 ↔ 주가 상관분석 계획 (Jupyter + DuckDB + pandas/scikit-learn)

- 작성일: 2026-06-27
- 대상 데이터: `dart_financial_statement_raw`(재무) × `feat_price`(주가 수익률)
- 스냅샷: `data_lake/.../snapshot_date=2026-06-19`
- 선행 작업: `research/analysis/dart_financial_statement_raw/`(DuckDB 쿼리 모음, C9 후속 분석)

## 1. 목적

`dart_financial_statement_raw` 의 재무 지표가 **실제 주가 수익률과 상관(correlation)이 있는지** 검증한다.
DuckDB 로 무거운 JOIN·집계를 처리하고, 결과 DataFrame 을 pandas/scikit-learn 으로 받아
상관행렬·산점도·단순 회귀로 신호 유무를 확인하는 것이 목표다.

이 문서는 "정식 모델링"이 아니라 **탐색(EDA) 단계**다. 신호가 확인되면
`research/models/` 의 정식 파이프라인으로 승격한다.

## 2. 도구 구성 및 역할 분담

| 계층 | 도구 | 역할 |
| --- | --- | --- |
| 데이터 게이트웨이 | **DuckDB** | parquet 직접 읽기, PIT JOIN, 표준 통계함수(`corr`, `regr_*`)로 1차 스캔 |
| 분석/통계 | **pandas + scikit-learn + scipy** | 상관행렬, 분위수 버킷, 단순/다중 회귀, 이상치 처리 |
| 시각화 | **matplotlib / plotly / seaborn** | 산점도, 상관 heatmap, 분위수별 수익률 막대 |
| 실행 환경 | **Jupyter (`ipykernel`)** | 셀 단위 탐색 → 시각화 → 회귀를 한 흐름으로 |

> 의존성 현황: `matplotlib`, `plotly`, `ipykernel`, `scikit-learn`, `duckdb`, `pandas`,
> `polars`, `pyarrow` 는 **이미 `pyproject.toml` 에 존재**. 추가 설치는 `seaborn`(heatmap 편의용) 정도만
> 선택적으로 필요하며, 없으면 matplotlib 으로 대체한다.

## 3. 데이터 소스와 결합 키 (핵심)

상관분석의 성패는 도구가 아니라 **PIT(Point-In-Time) 정렬**에 달려 있다.
재무를 발표 시점 기준으로 주가에 붙이지 않으면 look-ahead bias 로 상관이 과대평가된다.

### 경로 A — 마트 JOIN (1차 권장, PIT 정렬 마트 사용)

`feat_fin_pit` 마트가 **이미 PIT 정렬**되어 `trade_date × ticker` 로 표준 재무비율을 제공한다.
`feat_price` 와 키가 동일해 바로 JOIN 가능 → **가장 빠른 출발점**.

- `feat_fin_pit` 컬럼(재무 = **예측변수**): `fin_roa`, `fin_roe`, `fin_debt_to_equity`,
  `fin_equity_ratio`, `fin_ocf_to_assets`, `fin_cash_ratio`, `fin_asset_turnover`,
  `fin_operating_margin`, `fin_is_negative_equity`, `fin_has_fs`
- JOIN 키: `(trade_date, ticker)` — 마트 공통

#### ⚠ 수익률 타깃은 `feat_price.px_ret_*` 가 아니다

`feat_price` 의 `px_ret_1d/5d/20d/60d` 는 `LAG` 기반 **과거 N일 로그수익률**이다
(`research/etl/features/price.py`). 이를 상관 타깃으로 쓰면 "과거 수익률 ↔ 현재 재무"를 보는
셈이라 분석 의도(재무 → **미래** 주가)와 어긋난다. `feat_price` 의 `px_*` 컬럼들은
**예측변수(모멘텀/변동성/유동성)** 로만 쓰고, **상관 타깃은 forward return** 인
`label_daily` 의 `fwd_ret_{h}d` / `raw_label_{h}d`(시장 대비 초과수익) 를 쓴다
(`research/etl/labels.py`).

- `label_daily` 는 현재 스냅샷 parquet 에 **없다**. `daily_ohlcv` 뷰를 등록한 뒤
  `research.etl.labels.materialize_label` 로 1회 생성해 마트에 추가하거나,
  노트북에서 동일 로직(비-halt 거래일 인덱스 `d_idx + h`)으로 forward return 을 직접 계산한다.
- 키는 동일하게 `(trade_date, ticker)`.

```sql
-- 재무(예측변수) ↔ forward return(타깃)
SELECT f.*,                         -- feat_fin_pit: 재무비율 (예측변수)
       l.fwd_ret_20d, l.fwd_ret_60d,
       l.raw_label_20d              -- 시장 대비 초과수익 (excess)
FROM read_parquet('.../feat_fin_pit/**/*.parquet') f
JOIN label_daily l                  -- materialize_label 로 생성한 forward-return 마트
  USING (trade_date, ticker);
-- 필요 시 feat_price 의 px_mom_20_60 등은 '추가 예측변수'로만 LEFT JOIN
```

### 경로 B — 원본 PIT 직접 정렬 (심화, 임의 계정 분석용)

`feat_fin_pit` 의 표준 비율 밖의 계정(예: C9 의 `-표준계정코드 미사용-` 항목,
특정 `account_nm`)을 분석할 때는 `fs_raw` 원본을 직접 PIT 정렬한다.

- **발표 시점 키**: `rcept_no` 앞 8자리 = 접수일(YYYYMMDD). 예) `20160330...` → 2016-03-30 발표.
  `fetched_at`(수집일시)이 아니라 `rcept_no` 기반 접수일을 PIT 기준으로 써야 한다.
- 정렬 규칙: 재무값은 **접수일 다음 거래일 이후**의 주가에만 매칭.

#### dedup 은 trade_date 기준 interval/as-of 여야 한다 (전역 최신 금지)

전역 "최신 `rcept_no` 1건"으로 dedup 하면 **look-ahead** 가 생긴다 — 나중에 나온 정정공시가
그보다 과거의 거래일에 소급 적용되기 때문이다. 반드시 **각 `trade_date` 시점에 이미 공개된
(`disclosed_date <= trade_date`) filing 중 최신** 만 고르는 interval/as-of 방식을 쓴다
(경로 A 의 `feat_fin_pit` 가 `available_from` interval 로 하는 것과 동일한 원리,
`research/etl/features/fin_pit.py`).

#### dedup 키는 raw unique key 전체를 보존해야 한다

`fs_raw` 의 실제 unique key 는
`(corp_code, bsns_year, reprt_code, fs_div, sj_div, account_id, ord, rcept_no)` 이다
(`sql/postgres_ddl.sql`). dedup 키를 `(corp_code, bsns_year, reprt_code, account_id)` 로 좁히면
**서로 다른 계정이 한 줄로 붕괴**한다. 특히 `account_id = '-표준계정코드 미사용-'` 분석에서는
`account_id` 가 전부 동일하므로 `fs_div`, `sj_div`, `account_nm`, `ord` 를 반드시 보존해
계정을 구분해야 한다. → 정정공시 dedup 은 "키 전체 중 `rcept_no` 만 as-of 최신" 으로 적용한다.

```sql
-- 접수일 추출 예시
SELECT *, strptime(left(rcept_no, 8), '%Y%m%d')::date AS disclosed_date
FROM fs_raw
```

### 경로 A vs 경로 B — PIT 기준이 다르다 (비교 시 명시)

두 경로는 "언제부터 그 재무를 알 수 있었나(PIT)"의 기준이 서로 다르다. 결과를 비교할 때
이 차이를 전제로 해석해야 한다.

- **경로 A (`feat_fin_pit`)**: 실제 접수일이 아니라 **`period_end + 90d`(연간) / `+45d`(분기)**
  의 보수적 지연으로 `available_from` 을 만든다(`research/etl/features/fin_pit.py`).
- **경로 B (`fs_raw`)**: `rcept_no` 기반 **실제 접수일**을 PIT 기준으로 쓴다.

→ 같은 보고서라도 경로 A 가 경로 B 보다 늦게(보수적으로) 가용해진다. 두 경로의 상관 결과 차이
일부는 신호가 아니라 **PIT 지연 기준 차이**에서 올 수 있다.

## 4. 분석 단계 (노트북 셀 흐름)

0. **타깃 준비** — forward return 마트(`label_daily`)가 스냅샷에 없으므로,
   `daily_ohlcv` 뷰 등록 후 `materialize_label` 로 생성(또는 노트북에서 동일 로직으로 계산).
1. **로드 & 결합** — DuckDB 로 경로 A JOIN(재무 = 예측변수, `label_daily.fwd_ret_*` = 타깃) →
   `.df()` 로 pandas DataFrame 화. (행 수가 크면 DuckDB 단에서 연도/섹터 필터·샘플링 후 반입)
2. **전처리** — 결측(`NULL`) 처리, 비율 지표 winsorize(상하위 1% 클리핑), 거래정지(`px_is_halted`) 행 제외.
3. **상관 1차 스캔** — `df.corr()`(Pearson) + Spearman(순위 상관, 비선형/이상치에 강건) 둘 다.
   - 출력: 재무지표 × **forward return** horizon(5/20/60d) 상관 heatmap.
4. **산점도 검증** — 상관 상위 지표는 반드시 산점도로 비선형·이상치·군집 확인
   (상관계수 숫자만 믿지 않는다).
5. **분위수 포트폴리오** — 재무지표를 5분위(quintile)로 나눠 분위수별 평균 forward return 비교.
   단조성(monotonicity)이 보이면 단순 상관보다 강한 신호.
6. **단순 회귀** — scikit-learn `LinearRegression` 으로 지표→forward return 회귀,
   `regr_r2`/계수 부호 확인. 필요 시 섹터·시가총액 통제(다중 회귀).
7. **그룹별 안정성** — 연도별·시장(KOSPI/KOSDAQ)별로 상관을 재계산해 신호의 시간/구간 안정성 점검.

## 5. 파일 구성 (산출물 위치)

분석 코드(쿼리·노트북)는 `research/` 아래, 데이터성 결과는 `reports/` 아래에 둔다.
**`research/` 와 `reports/` 는 둘 다 `.gitignore` 대상**이다 — 이 상관분석은 배포 파이프라인의
일부가 아니라 로컬 전용 탐색 작업이므로 커밋 범위에서 제외한다(이 계획 문서 `docs/dev/...` 만 커밋).

```
research/analysis/fin_vs_price_corr/        ← 로컬 전용 (gitignore)
├── README.md
├── notebooks/
│   └── 01_fin_price_correlation.ipynb      # 본 분석 노트북
└── queries/
    ├── 00_setup_views.sql                  # feat_fin_pit / feat_price / fs_raw VIEW
    ├── 01_pit_join_marts.sql               # 경로 A: 마트 JOIN
    └── 02_pit_join_raw_account.sql         # 경로 B: 원본 PIT 정렬

reports/analysis/fin_vs_price_corr/<날짜>/  ← 로컬 전용 (gitignore)
├── corr_heatmap.png
├── quintile_returns.csv
└── ...
```

> `research/` 가 gitignore 이므로 코드 재현성은 git 이 아니라 **이 계획 문서 + `README.md`** 가
> 담보한다. 따라서 데이터 경로·결합 규칙은 `00_setup_views.sql` 한 곳에 모으고, 핵심 가정은
> 문서에 남긴다. 노트북 `.ipynb` 는 어차피 추적하지 않으므로 자유롭게 작업하되, 안정화되면
> 핵심 쿼리를 `queries/*.sql` 로 분리해 재현성을 확보한다.

## 6. 주의사항 / 함정

- **타깃은 forward return**: `feat_price.px_ret_*` 는 과거(LAG) 수익률이므로 상관 타깃이 아니다.
  타깃은 `label_daily.fwd_ret_*`/`raw_label_*`. `px_*` 는 예측변수로만 쓴다.
- **PIT 정렬 필수**: 사업연도(`bsns_year`)를 그대로 주가에 붙이면 안 된다. 접수일 기준 결합.
- **정정공시 dedup = as-of**: 전역 최신 `rcept_no` 1건이 아니라 `disclosed_date <= trade_date`
  중 최신만. dedup 키는 raw unique key 전체를 보존(`account_nm`/`ord` 포함)해 계정 붕괴 방지.
- **경로 A vs B PIT 차이**: A 는 `period_end+90/45d` 보수 지연, B 는 실제 접수일 — 비교 시 전제.
- **생존 편향**: `feat_price`/마트가 상장폐지 종목을 포함하는지 확인. 누락 시 상관이 낙관 편향.
- **상관 ≠ 인과**: 섹터·규모 같은 공통요인이 상관을 만들 수 있으니 그룹별 재검증 필수.
- **DuckDB 단일 writer**: IntelliJ 와 노트북이 같은 `.duckdb` 를 동시에 잡지 않도록 주의(read-only parquet 직접 읽기면 무관).

## 7. 완료 기준 (Definition of Done)

- 경로 A 기준 재무지표 × forward return 상관 heatmap 1장 + 상관표 CSV 산출.
- 상관 상위 3개 지표에 대한 산점도 + 5분위 forward return 비교.
- 노트북이 스냅샷만 바꿔 재실행 가능(경로 하드코딩은 `00_setup_views.sql` 한 곳).
- 신호 유무에 대한 1단락 결론을 `research/analysis/fin_vs_price_corr/README.md` 에 기록.
