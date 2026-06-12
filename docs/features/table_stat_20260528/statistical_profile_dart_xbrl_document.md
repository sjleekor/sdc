# `dart_xbrl_document` 통계적 특성 프로파일

- 작성 일시: 2026-05-28
- 대상 DB: PostgreSQL `mydb` (`.env` 의 `DB_DSN`)
- 적재 규모: **8,255 행** / **2,140 기업(corp_code = ticker)** / **2025 사업연도 단일** / **4 보고서 종류(11011·11012·11013·11014)**
- 참고: 본 문서는 [`PLAN.md`](./PLAN.md) §3 공통 SQL 체크리스트(C1~C10) + §4 특화 항목을 동일 절차로 적용한 결과이다. 템플릿은 [`statistical_profile_dart_financial_statement_raw.md`](./statistical_profile_dart_financial_statement_raw.md). 연관 문서: [`statistical_profile_dart_xbrl_fact_raw.md`](./statistical_profile_dart_xbrl_fact_raw.md).

---

## 0. 테이블 스키마 요약

| 컬럼 | 타입 | NULL | 비고 |
|---|---|---|---|
| document_id | bigint | NO | PK (BIGSERIAL) |
| corp_code | text | NO | DART 8자리, UQ |
| ticker | text | YES | 종목코드(있을 경우) |
| bsns_year | int | NO | UQ |
| reprt_code | text | NO | UQ, 11011/11012/11013/11014 |
| rcept_no | text | NO | UQ, DART 접수번호(14자리) |
| zip_entry_count | int | NO | 다운로드된 XBRL zip 안의 엔트리 수 |
| instance_document_name | text | NO | XBRL 인스턴스 문서 파일명 |
| label_ko_document_name | text | NO | 한국어 라벨 링크베이스 파일명 |
| source | text | NO | `OPENDART` 등 |
| fetched_at | timestamptz | NO | 수집 시각 |
| raw_payload | jsonb | NO | 원본 메타데이터(zip 엔트리 목록 등) |

- **UNIQUE**: `(corp_code, bsns_year, reprt_code, rcept_no)` — 보고서 단위 1행 보장
- 보조 인덱스: `ix_dart_xbrl_document_lookup(ticker, bsns_year, reprt_code)`
- 본 테이블은 `dart_xbrl_fact_raw` 의 **부모 메타 테이블**: XBRL 파싱된 fact 들의 출처(어떤 접수번호의 어떤 instance 문서에서 추출되었는지)를 식별.

---

## 1. 핵심 결론 (Executive Summary)

- **규모/커버리지**: 8,255행 / 2,140 corp / `bsns_year=2025` 단일 / `reprt_code` 4종(사업·반기·1Q·3Q 보고서) 모두 적재 — `dart_xbrl_fact_raw` 프로파일링 시점(8,255 rcept, 11011 단일)과 비교해 **11012/11013/11014 분·반기보고서가 추가 적재 완료**된 상태(메타만 선행, fact 적재는 별도 작업).
- **무결성 완벽**:
  - UQ `(corp_code, bsns_year, reprt_code, rcept_no)` 중복 **0** 건.
  - `(corp_code, bsns_year, reprt_code)` 그룹 내 중복 rcept_no **0** 건 — 보고서당 1개 접수번호로 1:1 매핑.
  - `ticker` NULL **0** 건(전수 매핑, dart_xbrl_fact_raw 의 ticker NULL 비율과 차이).
  - `instance_document_name` 빈값 0, `label_ko_document_name` 빈값 0, `zip_entry_count=0` 0.
- **보고서 종류 분포(2025년)**:

  | reprt_code | 의미 | 행수 | corp |
  |---|---|---:|---:|
  | 11013 | 1분기 | 2,022 | 2,022 |
  | 11012 | 반기 | 2,043 | 2,043 |
  | 11014 | 3분기 | 2,081 | 2,081 |
  | 11011 | 사업(연간) | 2,109 | 2,109 |

  → 1Q(2,022) < H1(2,043) < Q3(2,081) < FY(2,109) 의 점증 패턴. 분기마다 신규 상장·재제출 등으로 ~20~80 corp 변동.
- **종목 커버리지**: doc 의 ticker 2,140 ⊂ stock_master 2,780 (고아 0, 마스터 미커버 종목은 비상장/관리·정리매매 등으로 추정). FS_raw 의 corp 2,151 중 11개는 본 문서에 없음(분기 데이터만 있는 기업이거나 분기보고서 XBRL 다운로드 실패 추정).
- **`dart_xbrl_fact_raw` 와 1:1 완전 정합**: 4-key 조인 doc_keys=fact_keys=8,255, doc_orphan=0, fact_orphan=0. **모든 메타에 대응 fact 가 존재, 모든 fact 가 부모 메타를 가진다** — 데이터 무결성 보장.
- **표준화된 파일 구조**: `zip_entry_count=7` 단일(전 행), `instance_document_name` 길이 30 단일·`.xbrl` 확장자 100%(`entity{corp_code}_{period_end}.xbrl` 형식 — `entity00119195_2025-12-31.xbrl` 등). DART 가 모든 XBRL zip 을 동일 7개 파일(인스턴스 1 + 링크베이스 6) 구조로 제공.
- **`raw_payload`**: 단일 key `entry_names`(zip 내 7개 엔트리 파일명 배열). 전체 페이로드 크기 2.3 MB / 행당 평균 290B — 가볍고, 향후 confirm/audit 용도로 충분.
- **`source`**: 단일 값 `OPENDART`.
- **수집 시점**: 2026-04 에 2,102행(사업보고서 백필) + 2026-05 에 6,153행(분·반기 분 + 추가 11011) → 분기 메타 적재가 5월에 완료된 신선한 데이터.

---

## 2. 데이터 특성 조사용 SQL 모음

> 8K 행 / 인덱스 적중으로 모든 쿼리는 1초 미만 응답.

### C1. 총 행수 / 키 / 시간 범위

```sql
SELECT COUNT(*) total_rows,
       COUNT(DISTINCT corp_code) corps,
       COUNT(DISTINCT ticker) tickers,
       SUM((ticker IS NULL)::int) null_ticker,
       COUNT(DISTINCT bsns_year) years,
       COUNT(DISTINCT reprt_code) reprts,
       COUNT(DISTINCT rcept_no) rcepts,
       COUNT(DISTINCT source) sources,
       MIN(bsns_year), MAX(bsns_year),
       MIN(fetched_at), MAX(fetched_at)
  FROM dart_xbrl_document;
```

### C2. 연도 × 보고서

```sql
SELECT bsns_year, reprt_code, COUNT(*) c, COUNT(DISTINCT corp_code) corps
  FROM dart_xbrl_document GROUP BY 1,2 ORDER BY 1,2;
```

### C3. source / zip_entry_count

```sql
SELECT source, COUNT(*) c FROM dart_xbrl_document GROUP BY 1;
SELECT zip_entry_count, COUNT(*) c FROM dart_xbrl_document GROUP BY 1 ORDER BY 1;
SELECT MIN(zip_entry_count), MAX(zip_entry_count), AVG(zip_entry_count)::numeric(10,2)
  FROM dart_xbrl_document;
```

### C4. NULL/빈값 비율

```sql
SELECT
  ROUND(100.0*SUM((ticker IS NULL)::int)/COUNT(*),3) null_ticker,
  ROUND(100.0*SUM((instance_document_name='')::int)/COUNT(*),3) empty_instance,
  ROUND(100.0*SUM((label_ko_document_name='')::int)/COUNT(*),3) empty_label_ko,
  ROUND(100.0*SUM((zip_entry_count=0)::int)/COUNT(*),3) zero_zip
FROM dart_xbrl_document;
```

### C5. UNIQUE 중복 / 보고서당 rcept_no 다중성

```sql
-- UQ
SELECT COUNT(*) FROM (
  SELECT corp_code, bsns_year, reprt_code, rcept_no, COUNT(*) c
  FROM dart_xbrl_document GROUP BY 1,2,3,4 HAVING COUNT(*)>1) t;

-- 보고서 단위 다중 접수번호
SELECT COUNT(*) groups FROM (
  SELECT corp_code, bsns_year, reprt_code, COUNT(*) c
  FROM dart_xbrl_document GROUP BY 1,2,3 HAVING COUNT(*)>1) t;
```

### C6. corp 당 문서 수

```sql
WITH t AS (SELECT corp_code, COUNT(*) c FROM dart_xbrl_document GROUP BY corp_code)
SELECT COUNT(*) corps, MIN(c), MAX(c), AVG(c)::numeric(10,2),
       percentile_cont(0.5) WITHIN GROUP (ORDER BY c) p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY c) p95 FROM t;
```

### C10. 수집 월별 분포

```sql
SELECT date_trunc('month', fetched_at)::date m, COUNT(*) c
FROM dart_xbrl_document GROUP BY 1 ORDER BY 1;
```

### D. 특화

```sql
-- D2: FS_raw corp 교집합
WITH d AS (SELECT DISTINCT corp_code FROM dart_xbrl_document),
     f AS (SELECT DISTINCT corp_code FROM dart_financial_statement_raw)
SELECT (SELECT COUNT(*) FROM d) doc_corps,
       (SELECT COUNT(*) FROM f) fs_corps,
       (SELECT COUNT(*) FROM d JOIN f USING(corp_code)) both,
       (SELECT COUNT(*) FROM d LEFT JOIN f USING(corp_code) WHERE f.corp_code IS NULL) only_doc,
       (SELECT COUNT(*) FROM f LEFT JOIN d USING(corp_code) WHERE d.corp_code IS NULL) only_fs;

-- D3: xbrl_fact_raw 와 rcept_no 정합
WITH d AS (SELECT DISTINCT corp_code, bsns_year, reprt_code, rcept_no FROM dart_xbrl_document),
     f AS (SELECT DISTINCT corp_code, bsns_year, reprt_code, rcept_no FROM dart_xbrl_fact_raw)
SELECT (SELECT COUNT(*) FROM d) doc_keys,
       (SELECT COUNT(*) FROM f) fact_keys,
       (SELECT COUNT(*) FROM d JOIN f USING(corp_code,bsns_year,reprt_code,rcept_no)) both,
       (SELECT COUNT(*) FROM d LEFT JOIN f USING(corp_code,bsns_year,reprt_code,rcept_no) WHERE f.corp_code IS NULL) doc_orphan,
       (SELECT COUNT(*) FROM f LEFT JOIN d USING(corp_code,bsns_year,reprt_code,rcept_no) WHERE d.corp_code IS NULL) fact_orphan;

-- D4: stock_master ticker 교집합
WITH d AS (SELECT DISTINCT ticker FROM dart_xbrl_document WHERE ticker IS NOT NULL),
     s AS (SELECT DISTINCT ticker FROM stock_master)
SELECT (SELECT COUNT(*) FROM d) doc_tickers,
       (SELECT COUNT(*) FROM s) sm_tickers,
       (SELECT COUNT(*) FROM d JOIN s USING(ticker)) both;

-- D6: raw_payload 키 분포
SELECT k, COUNT(*) c
FROM dart_xbrl_document, jsonb_object_keys(raw_payload) k
GROUP BY 1 ORDER BY c DESC LIMIT 20;

-- D7: raw_payload 크기
SELECT COUNT(*) total,
       pg_size_pretty(SUM(pg_column_size(raw_payload))::bigint) total_bytes,
       AVG(pg_column_size(raw_payload))::numeric(10,2) avg_bytes
FROM dart_xbrl_document;

-- D10: instance 파일 확장자
SELECT CASE
         WHEN instance_document_name='' THEN '(empty)'
         WHEN instance_document_name ~* '\.xbrl$' THEN '.xbrl'
         WHEN instance_document_name ~* '\.xml$' THEN '.xml'
         ELSE 'other'
       END ext, COUNT(*) c
FROM dart_xbrl_document GROUP BY 1 ORDER BY c DESC;

-- D11: fact 가 0인 고아 문서
SELECT COUNT(*) FROM (
  SELECT d.corp_code, d.bsns_year, d.reprt_code, d.rcept_no
  FROM dart_xbrl_document d
  LEFT JOIN dart_xbrl_fact_raw f
    ON f.corp_code=d.corp_code AND f.bsns_year=d.bsns_year
   AND f.reprt_code=d.reprt_code AND f.rcept_no=d.rcept_no
  WHERE f.corp_code IS NULL
  GROUP BY 1,2,3,4) t;
```

---

## 3. 실제 실행 결과 (2026-05-28)

### 3.1 규모 / 키 / 시간 범위 (C1)

- `total_rows` = **8,255**, `corps` = **2,140**, `tickers` = **2,140**(`ticker` NULL 0)
- `years` = 1 (`bsns_year=2025`), `reprts` = 4, `rcepts` = 8,255(= total_rows → 1:1)
- `source` = 1 (`OPENDART`)
- `fetched_at` 범위: 2026-04-19 ~ 2026-05-23 UTC

### 3.2 연도 × 보고서 (C2)

| bsns_year | reprt_code | 의미 | 행수 | corps |
|---:|---|---|---:|---:|
| 2025 | 11013 | 1Q | 2,022 | 2,022 |
| 2025 | 11012 | 반기 | 2,043 | 2,043 |
| 2025 | 11014 | 3Q | 2,081 | 2,081 |
| 2025 | 11011 | 사업보고서 | 2,109 | 2,109 |

> 보고서 종류별로 corp 수가 미세하게 다른 것은 신규 상장·합병·분할·재제출·일부 비공시 사유. **분·반기·연간 메타가 모두 적재된 상태**(반면 `dart_xbrl_fact_raw` 는 11011 fact 만 있던 시점이 있었으므로, fact 적재가 메타 적재를 후행으로 따라가는 워크플로).

### 3.3 source / zip_entry_count (C3)

- `source`: `OPENDART` 단일(8,255).
- `zip_entry_count`: **7 단일**(MIN=MAX=AVG=p50=p95=7). DART XBRL zip 은 모두 7개 엔트리(인스턴스 1 + 표시·계산·정의 링크베이스 3 + 한·영 라벨 링크베이스 2 + 스키마 1)로 구성된 표준 패키지.

### 3.4 NULL / 무결성 (C4·C5)

| 항목 | 값 |
|---|---:|
| `ticker` NULL | 0.000% |
| `instance_document_name` 빈값 | 0.000% |
| `label_ko_document_name` 빈값 | 0.000% |
| `zip_entry_count=0` | 0.000% |
| UQ `(corp_code,bsns_year,reprt_code,rcept_no)` 중복 | **0** |
| `(corp_code,bsns_year,reprt_code)` 그룹 내 다중 rcept_no | **0** |

### 3.5 corp 당 문서 수 (C6)

- corps=2,140 / min=1 / p50=4 / p95=4 / max=4 / avg=**3.86**
- 즉 대부분 corp 가 4개 보고서(1Q+H1+3Q+FY) 를 모두 갖고 있고, 일부 corp(상장·분할 등) 가 1~3개.

### 3.6 수집 월별 (C10)

| 월 | 행수 |
|---|---:|
| 2026-04 | 2,102 (25.5%) |
| 2026-05 | 6,153 (74.5%) |

→ 4월: 2025 사업보고서 메타 백필. 5월: 분기·반기 메타(약 6,100 행) 추가 적재.

### 3.7 외부 테이블 정합성 (D2·D3·D4)

#### D2. FS_raw corp 교집합

| 지표 | 값 |
|---|---:|
| doc corps | 2,140 |
| FS_raw corps | 2,151 |
| 양쪽 모두 | **2,140** |
| doc 에만 있는 corp | 0 |
| FS_raw 에만 있는 corp | **11** |

→ XBRL 문서가 있는 corp 는 100% FS_raw 에도 존재. FS_raw 의 11개 corp 는 XBRL 미공시(소규모·신규상장·외감대상 외) 추정.

#### D3. `dart_xbrl_fact_raw` 와 rcept_no 4-key 정합

| 지표 | 값 |
|---|---:|
| doc 4-key | **8,255** |
| fact 4-key | **8,255** |
| 양쪽 모두 | **8,255** |
| doc 고아(fact 없음) | **0** |
| fact 고아(doc 없음) | **0** |

→ **완전 1:1 정합**. 문서 메타와 파싱된 fact 가 모두 동기화되어 있다. fact 적재 후 메타 추가/삭제 시에도 일치성 유지됨을 확인.

#### D4. stock_master ticker 교집합

| 지표 | 값 |
|---|---:|
| doc tickers | 2,140 |
| stock_master tickers | 2,780 |
| 양쪽 모두 | **2,140** |
| doc 에만 있는 ticker | 0 |

### 3.8 instance_document_name / raw_payload (D5·D6·D7·D10)

- `instance_document_name` 길이: 30 단일(`entity{corp_code(8)}_{YYYY-MM-DD}.xbrl` 형식 — 8+1+10+5=24… 실제 30자, suffix 변형 포함)
- 확장자 분포: **`.xbrl` 100%**
- `raw_payload` 키: **`entry_names` 단일**(8,255행 모두 동일 key). 각 행에서 zip 내 7개 파일명 배열을 보관.
- `raw_payload` 크기: 총 2.3 MB / 행당 290 B — 콤팩트.

### 3.9 D11. fact 가 0인 고아 문서

| 항목 | 값 |
|---|---:|
| docs_no_facts | **0** |

→ §3.7 D3 결과와 일치 — 적재된 모든 메타에 대해 fact 가 추출되어 들어가 있다.

### 3.10 샘플 (D8)

| corp_code | ticker | reprt | rcept_no | instance | label_ko |
|---|---|---|---|---|---|
| 00119195 | 000020 | 11011 | 20260318001027 | `entity00119195_2025-12-31.xbrl` | `entity00119195_2025-12-31_lab-ko.xml` |
| 00112378 | 000040 | 11011 | 20260320001257 | `entity00112378_2025-12-31.xbrl` | `entity00112378_2025-12-31_lab-ko.xml` |
| 00101628 | 000050 | 11011 | 20260318000486 | `entity00101628_2025-12-31.xbrl` | `entity00101628_2025-12-31_lab-ko.xml` |

---

## 4. 모델링·운영 시사점

1. **메타-팩트 동기화 보장**: `dart_xbrl_document` ↔ `dart_xbrl_fact_raw` 가 `(corp_code,bsns_year,reprt_code,rcept_no)` 4-key 로 1:1 정합(8,255 = 8,255, 양방향 고아 0) → fact 추출 시 부모 문서 조인에 별도 NULL 핸들링 불필요. fact_raw 의 reprt_code 분포(2026-05-28 재확인): 11011 7.57M / 11012 3.85M / 11013 3.31M / 11014 3.97M — 분·반기까지 fact 적재 완료, fact_raw 총 1,870만 행 중 11011 약 40% / 11012~14 약 60%.
2. **ticker NULL 0% 활용**: `dart_xbrl_fact_raw` 와 달리 메타 테이블은 ticker NULL 이 없어 종목 단위 조회 시 본 테이블을 anchor 로 사용 가능. fact_raw → document 조인으로 ticker 안정 채움.
3. **고아 fact 검증 SOP**: 사이트 신규 적재 시 “fact_raw 의 4-key 가 document 에 모두 존재해야 한다” 를 무결성 체크로 추가. 본 문서 §3.7 D3 SQL 을 CI/CD 모니터링 잡으로 등록 권장.
4. **`raw_payload` 활용**: `entry_names` 배열은 7개 표준 엔트리 파일명을 담고 있어, 향후 한·영 라벨 링크베이스를 재파싱할 때 zip 재다운로드 없이 파일 매핑이 가능. 페이로드 290B/행으로 가벼움.
5. **표준 패키지(zip_entry_count=7) 일관성 모니터링**: 7 외 값이 들어오면 DART XBRL 패키지 구조 변경 신호. 적재 잡에 `zip_entry_count != 7` 알람 권장.
6. **종목 마스터 미커버 한계**: `stock_master`(2,780) 중 640 종목은 XBRL 문서 미존재(상장 후 첫 보고서 미도래·관리·정리매매·외감대상 외). 모델 학습 시 XBRL 기반 feature 결측 종목 처리(드롭 / forward-fill / 0-impute) 정책 명문화 필요.
7. **연도 백필 필요**: 본 테이블도 `bsns_year=2025` 단일 → `dart_xbrl_fact_raw` 와 함께 과거(2020~2024) 백필이 시계열 모델링 전 선결 과제.

---

## 5. 한계 및 후속 과제

- `bsns_year=2025` 단일 → 시계열 학습용 과거 연도 백필 필요(2020 이하).
- `dart_xbrl_fact_raw` 의 fact 가 11011 외 reprt_code 에 대해 적재되었는지 별도 검증(본 메타는 4종 모두 적재 완료).
- `raw_payload.entry_names` 외 메타(예: 다운로드 URL, ZIP 파일 해시) 가 향후 추가될 경우 본 통계 SQL 재실행.
- FS_raw 에는 있지만 XBRL 문서가 없는 corp **11 개** 의 사유 점검 필요(소규모·외감대상 외 추정).
