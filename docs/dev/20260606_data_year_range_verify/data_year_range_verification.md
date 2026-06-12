# 수집 데이터 기간(연도) 범위 검증 보고서

- 작성 일시: 2026-06-06
- 작성 목적: sj2-server cronicle 에 배포된 수집 이벤트들이 **어떤 기간에 대해** 데이터를 수집하도록 구성되어 있는지를 (1) 스크립트/코드로 먼저 확인하고, (2) 실제 DB 데이터를 조회하여 검증한 뒤, (3) 주가 예측에 필요한 기간과 비교 정리한다.
- 검증 대상 DB
  - local: `mydb` (`.env` 의 `DB_DSN`, `localhost:5432`, PostgreSQL 17.6)
  - sj2-server: `krx_data` (`192.168.0.11:5432`, PostgreSQL 18.3) — 실제 수집 서버
  - ※ 실수집은 sj2-server 에서 진행하고 local 로 수작업 동기화. **본 검증 시점 두 DB 의 모든 수치는 완전히 동일**(아래 §3 참조).
- 검증 기준일: 두 DB 모두 시세/플로우 최신 거래일 = **2026-05-21** (작성일 기준 약 2주 지연 — 수작업 동기화 주기로 추정).

---

## 1. cronicle 이벤트 구성 (스크립트 레벨)

cronicle API(`get_event`) 로 조회한 두 이벤트의 실제 실행 스크립트.

### 1.1 `sdc_daily_pipeline` — "SDC Daily Pipeline" (enabled)
> notes: *Universe sync + price incremental backfill + KRX flows after prices.*

```bash
/home/whi/apps/sdc/bin/universe-sync.sh
/home/whi/apps/sdc/bin/prices-backfill-incremental.sh
/home/whi/apps/sdc/bin/flows-sync.sh
```

### 1.2 `sdc_daily_accounts_flows` — "SDC Daily Accounts/Flows Pipeline" (enabled)
> notes: *OpenDART corp/financials/share-info/XBRL raw + canonical metric normalize. KRX flows moved to sdc_daily_pipeline after prices.*

```bash
/home/whi/apps/sdc/bin/dart-sync-corp.sh
/home/whi/apps/sdc/bin/dart-sync-financials.sh
/home/whi/apps/sdc/bin/dart-sync-share-info.sh
/home/whi/apps/sdc/bin/dart-sync-xbrl.sh
/home/whi/apps/sdc/bin/metrics-normalize.sh
```

각 스크립트(`deploy/prod/bin/*.sh`)가 호출하는 실제 CLI 명령:

| 스크립트 | 실행 CLI | 기간 관련 인자 |
|---|---|---|
| `universe-sync.sh` | `universe sync --source fdr --markets kospi,kosdaq` | 기간 개념 없음(현행 유니버스 스냅샷) |
| `prices-backfill-incremental.sh` | `prices backfill --market all --incremental` | **증분**: 종목별 `MAX(trade_date)+1` ~ 오늘 |
| `flows-sync.sh` | `flows sync --use-price-range` | **가격범위 추종**: `daily_ohlcv` 전체 거래일 범위 |
| `dart-sync-corp.sh` | `dart sync-corp` | 기업 마스터 전체(기간 없음) |
| `dart-sync-financials.sh` | `dart sync-financials --reprt-codes 11011,11012,11013,11014 --bsns-years <올해>,<작년>` | **올해+작년 2개 연도** (2026, 2025) |
| `dart-sync-share-info.sh` | `dart sync-share-info` (인자 없음) | **기본값 = 작년 1개 연도** (2025), `reprt 11011` |
| `dart-sync-xbrl.sh` | `dart sync-xbrl --reprt-codes 11011,11012,11013,11014` (연도 미지정) | **기본값 = 작년 1개 연도** (2025) |
| `metrics-normalize.sh` | `metrics normalize` (인자 없음) | **기본값 = 작년 1개 연도** (2025), `reprt 11011` |

> ※ `bin/dart-backfill-all-years.sh` (기본 `start_year=2015` ~ 작년) 스크립트가 별도로 존재하지만, **cronicle 두 이벤트 어디에도 포함되어 있지 않다.** 즉 다년치 재무 백필은 수동 1회성 작업으로만 수행되며, 일배치 자동수집 대상이 아니다.

---

## 2. CLI 기간 결정 로직 (코드 레벨)

`src/krx_collector/cli/app.py` 및 서비스 계층 확인 결과.

### 2.1 시세 `prices backfill --incremental`
- `_handle_prices_backfill` → `service/backfill_daily.py::backfill_daily_prices(incremental=True)`.
- 종목별로 `MAX(trade_date)+1` 부터 `오늘(KST)` 까지 단일 연속 구간 수집. 저장 이력이 없는 신규 종목은 `start`(미지정 시 `2000-01-01`) 부터.
- 즉 **과거 전체 이력은 보존**되며 매일 최신분만 따라잡는다(catch-up). 기간 상한 = 오늘.

### 2.2 플로우 `flows sync --use-price-range`
- `_handle_flows_sync`: `--use-price-range` 가 켜지면 `storage.get_daily_price_date_range()` 로 `daily_ohlcv` 의 (min, max) 거래일을 가져와 그 범위 전체를 수집 대상으로 삼는다.
- `--start/--end` 미지정 시 가격범위 그대로 사용 → **시세가 보유한 전 기간을 플로우도 추종**.

### 2.3 DART `dart sync-*`
- 파서 기본값: `--bsns-years = str(date.today().year - 1)` (= 단일 작년), `--reprt-codes = "11011"`.
- 따라서 일배치 cron 기준 실제 수집 연도:
  - `sync-financials`: 스크립트가 `올해,작년` 명시 → **2026, 2025**, reprt 11011~11014.
  - `sync-xbrl`: 연도 미지정 → 기본값 **2025** 단일, reprt 11011~11014.
  - `sync-share-info`: 인자 없음 → 기본값 **2025** 단일, reprt 11011.
  - `metrics normalize`: 인자 없음 → 기본값 **2025** 단일, reprt 11011.
- 결론: **DART 계열은 사실상 최근 1~2개 사업연도만 일배치로 수집**한다. 과거 연도(2015~2024)는 자동 수집되지 않는다.

---

## 3. 실제 DB 데이터 검증 (local = sj2-server, 수치 동일)

### 3.1 시세/플로우 (시계열 모달리티)

| 테이블 | 거래일 범위 | 거래일수 | 행수 | 종목수 |
|---|---|---:|---:|---:|
| `daily_ohlcv` | 2007-06-05 ~ 2026-05-21 | 4,674 | 6,517,317 | 2,780 |
| `krx_security_flow_raw` | 2007-06-05 ~ 2026-05-21 | — | 76,222,905 | 2,776 |

연도별 종목 커버리지(요지):

| 연도 | `daily_ohlcv` 종목수 | `krx_security_flow_raw` 종목수 |
|---|---:|---:|
| 2007 | 1 | 1,263 |
| 2008~2013 | 1~2 | 1,303~1,601 |
| 2014 | 1,678 | 1,677 |
| 2015 | 1,783 | 1,782 |
| 2020 | 2,218 | 2,217 |
| 2024 | 2,641 | 2,637 |
| 2025 | 2,756 | 2,752 |
| 2026(~05-21) | 2,780 | 2,776 |

- **시세(`daily_ohlcv`)**: 2007~2013 은 종목 1~2개만 존재(백필 한계) → **종목 횡단 학습 실질 시작점 = 2014**. 2014 이후는 매년 1,600~2,780 종목으로 조밀.
- **플로우(`krx_security_flow_raw`)**: 2007 부터 이미 1,263 종목으로 조밀 → 시세보다 과거 커버리지가 더 좋다. 단 시세와 조인하면 시세의 2014 컷오프가 병목.

### 3.2 DART 재무/지분 (PIT·횡단 모달리티)

| 테이블 | 보유 `bsns_year` | 행수 | corp/종목수 |
|---|---|---:|---:|
| `dart_financial_statement_raw` | **2025, 2026** (reprt 4종) | 2025: 1,016,063 / 2026: 238,612 | 2,141 / 2,079 corp |
| `dart_xbrl_document` | **2025** | 8,255 | 2,140 corp |
| `dart_xbrl_fact_raw` | **2025** | 18,696,562 | — |
| `dart_share_count_raw` | **2025** | 10,295 | — |
| `dart_shareholder_return_raw` | **2025** | 263,030 | — |
| `stock_metric_fact` (정규화 지표) | **2025** | 34,411 | — |
| `dart_corp_master` | (마스터) | 116,503 | — |
| `stock_master` | (현행) | 2,780 | — |

- **DART 전 테이블이 단일 연도(`2025`) 중심**이며, `dart_financial_statement_raw` 만 2026 분이 추가되어 있다.
- 즉 §2.3 의 코드/스크립트 분석(“최근 1~2개 연도만 자동 수집”)이 실데이터로 그대로 확인된다. **2015~2024 의 DART 데이터는 DB 에 존재하지 않는다.**

---

## 4. 주가 예측에 필요한 기간 vs 현재 수집 기간 비교

주가 예측 요구 기간 기준은 본 프로젝트 모델 선정용 분석 문서
(`docs/features/table_stat_20260528/feature_profile_summary_for_model_selection.md`) 를 근거로 한다.

| 모달리티 | 예측에 필요한 기간(권장) | 현재 수집 설정(cron) | 실제 DB 보유 기간 | 충족 여부 |
|---|---|---|---|---|
| 시세 `daily_ohlcv` | 종목 횡단 학습 **2014-01-02 이후**, 가능하면 전체 | 증분(전이력 보존 + 매일 catch-up) | 2007-06-05~2026-05-21, 2014+ 조밀 | ✅ **충족** |
| 플로우 `krx_security_flow_raw` | 시세와 동일 구간(2014+) | 가격범위 전체 추종 | 2007~2026, 2007+ 조밀 | ✅ **충족**(시세보다 우수) |
| DART 재무 `dart_financial_statement_raw` | 장기 시계열 재무피처용 **최소 2015~현재(10년+)** | 올해+작년(2026,2025)만 | 2025, 2026 | ❌ **미충족**(2015~2024 결손) |
| DART XBRL `dart_xbrl_*` | 〃 | 작년(2025)만 | 2025 | ❌ **미충족** |
| DART 지분/주주환원 `dart_share_count_raw`, `dart_shareholder_return_raw` | 〃 | 작년(2025)만 | 2025 | ❌ **미충족** |
| 정규화 지표 `stock_metric_fact` | 재무 백필에 종속 | 작년(2025)만 | 2025 | ❌ **미충족**(원천 종속) |

### 핵심 결론
1. **시세·플로우(시계열)는 예측 요건을 충분히 충족**한다. 전 이력이 보존되며 일배치 증분으로 최신성도 유지된다(현재는 2026-05-21 까지, 수작업 동기화 지연 ≈2주).
2. **DART 재무 계열은 단일~2개 연도만 보유**하여 장기 시계열/PIT 패널 재무피처 구축이 불가능하다. 이는 일배치 cron(`sdc_daily_accounts_flows`)이 기본값상 **최근 연도만** 수집하도록 구성되어 있기 때문이며, 다년 백필(`dart-backfill-all-years.sh`, 2015~)이 **cronicle 에 등록되어 있지 않은** 것이 직접적 원인이다.
3. 따라서 멀티모달/장기 재무 시계열 모델로 가려면 **`dart-backfill-all-years.sh` 를 2015~2024(또는 그 이전)에 대해 1회 실행**한 뒤 `metrics normalize` 를 해당 연도들에 대해 재실행하여 재무 데이터의 연도 범위를 시세 범위(2014+)에 맞춰야 한다.

### 권장 후속 조치
- (자동화 갭) `sdc_daily_accounts_flows` 의 `dart-sync-xbrl.sh` / `dart-sync-share-info.sh` 가 연도 인자 없이 기본값(작년 1개)만 받는 점을 인지하고, 과거 연도는 별도 백필로 보완.
- (1회성 백필) `SDC_DART_BACKFILL_START_YEAR=2015 bin/dart-backfill-all-years.sh` 실행 → financials/share-info/xbrl/normalize 를 2015~작년 전 연도에 대해 채움.
- (정규화) 백필 후 `metrics normalize --bsns-years 2015,2016,...` 로 `stock_metric_fact` 연도 확장.
- (동기화) sj2-server → local 수작업 동기화 주기를 단축하면 최신 거래일 지연(현재 ≈2주)을 줄일 수 있음.

---

## 부록 A. 검증에 사용한 주요 쿼리

```sql
-- 시세/플로우 범위
select min(trade_date), max(trade_date), count(*), count(distinct trade_date), count(distinct ticker) from daily_ohlcv;
select min(trade_date), max(trade_date), count(*), count(distinct ticker) from krx_security_flow_raw;

-- DART 연도별 분포
select bsns_year, count(*), count(distinct corp_code), count(distinct reprt_code) from dart_financial_statement_raw group by bsns_year order by bsns_year;
select bsns_year, count(*) from dart_xbrl_document group by bsns_year order by bsns_year;
select bsns_year, count(*) from dart_xbrl_fact_raw group by bsns_year order by bsns_year;
select bsns_year, count(*) from dart_share_count_raw group by bsns_year order by bsns_year;
select bsns_year, count(*) from dart_shareholder_return_raw group by bsns_year order by bsns_year;
select bsns_year, count(*) from stock_metric_fact group by bsns_year order by bsns_year;

-- 연도별 종목 커버리지
select extract(year from trade_date)::int yr, count(distinct ticker), count(*) from daily_ohlcv group by 1 order by 1;
select extract(year from trade_date)::int yr, count(distinct ticker), count(*) from krx_security_flow_raw group by 1 order by 1;
```

## 부록 B. 검증 환경
- local DB: PostgreSQL 17.6 (`mydb`)
- sj2-server DB: PostgreSQL 18.3 (`krx_data`, 컨테이너 `sdc-postgres`)
- 두 DB 의 §3 모든 집계값이 1:1 동일함을 확인(수작업 동기화 정합성 OK).
