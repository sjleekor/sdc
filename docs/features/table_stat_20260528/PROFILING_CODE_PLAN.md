# 피처 프로파일링 자동화 프로그램 작성 계획

본 문서는 [`PLAN.md`](./PLAN.md) 에서 **수작업 마크다운**으로 진행하던 테이블/피처 통계 프로파일링을,
피처 수집이 계속 늘어나는 상황에서 **필요할 때마다 반복 실행 가능한 코드**로 전환하기 위한 설계·구현 계획서이다.

- 최초 작성: 2026-06-19
- 대상 DB: 로컬 PostgreSQL `mydb` (`.env` 의 `DB_DSN`) — `local`/`sj2` 파라미터화 (dbq.sh 와 동일 타깃 개념)
- 기준 자료: 기존 21개 `statistical_profile_*.md`, [`feature_profile_summary_for_model_selection.md`](./feature_profile_summary_for_model_selection.md), `sql/postgres_ddl.sql`
- 산출물(확정): **(1) Jupyter Notebook(.ipynb)** + **(2) 기계판독 통계 아티팩트(Parquet/JSON)** + **(3) Notebook→HTML 익스포트**(nbconvert, "최소 3종 포맷" 제약 충족용 파생 포맷)

---

## 0. 문제 정의 (Why)

기존 `PLAN.md` 방식의 한계:

1. **재현 불가** — 통계치가 사람이 psql 로 돌려 마크다운에 붙여넣은 스냅샷이다. 수집이 진행되면 매번 손으로 다시 돌려야 한다. (요약 문서 §0 에 "2026-05-28 단일연도 스냅샷이며 백필 후 재집계 필요" 경고가 박혀 있는 이유.)
2. **시점 정합성 붕괴** — 표마다 집계 시점이 달라 행수·분포가 어긋난다.
3. **드리프트 추적 불가** — 실행 간 통계 변화(신규 결측, 분포 이동, 커버리지 확장)를 기계적으로 비교할 수 없다.
4. **그래프 부재** — 전부 표/텍스트라 분포·커버리지의 시각적 진단이 빠져 있다.

목표: **카탈로그 기반·1회 명령 재실행 가능·그래프 포함·실행 간 diff 가능한** 프로파일링 프로그램.
선언적 카탈로그에 테이블/피처를 추가하면 통계가 자동 재계산되도록 한다.

---

## 1. 핵심 설계 원칙

### 1.1. 헥사고날 아키텍처 준수 (CLAUDE.md 불변식)

`domain/`·`service/` 는 `adapters/`·`infra/` 를 절대 import 하지 않는다. 와이어링은 `cli/app.py` 합성 루트에서만.
프로파일링은 다음 레이어로 분해한다.

```
domain/profiling.py        순수 dataclass/Enum: 무엇을 측정할지의 모델
ports/profiling.py         Protocol: ProfileQueryRunner, ProfileRenderer
service/profile_table.py   오케스트레이터: 카탈로그 spec → 체크 실행 → ProfileResult 조립
infra/db_postgres/profiling_query_runner.py   체크 종류 → SQL 변환 + 실행(+샘플링)
adapters/profiling_render/ 노트북/아티팩트/HTML 렌더러 (그래프 생성)
```

### 1.2. 선언적 "프로파일 카탈로그" — 기존 metric_catalog 패턴 재사용

이 프로젝트는 이미 `metric_catalog`/`metric_mapping_rule` 로 "무엇을(선언) ↔ 어떻게(매핑)"를 분리한다.
동일 철학을 적용한다. SQL 을 service 에 박지 않고, **테이블별 프로파일 설정을 선언적 카탈로그**로 둔다.

테이블당 1개 `TableProfileSpec` (Python 모듈 또는 `profiling_catalog.yaml`):

| 필드 | 의미 | 예 (`krx_security_flow_raw`) |
|---|---|---|
| `table` | 테이블명 | `krx_security_flow_raw` |
| `weight` | `full` / `light` | `full` |
| `entity_key` | 엔티티 컬럼 | `ticker` |
| `time_col` | 시간축 | `trade_date` |
| `natural_key` | 자연키(중복검사) | `(trade_date,ticker,market,metric_code,source)` |
| `numeric_cols` | 분위수 대상 | `[value_numeric]` |
| `category_cols` | 분포 대상 | `[market, metric_code, source]` |
| `null_cols` | NULL 비율 대상 | (전 컬럼 기본) |
| `fk_relations` | FK 정합성 | `daily_ohlcv (trade_date,ticker,market)` |
| `drilldown_dim` | 피처 드릴다운 차원 | `metric_code` (7종) |
| `sampling` | 대형 테이블 샘플 정책 | `TABLESAMPLE SYSTEM (1)` for quantiles |
| `domain_checks` | §4 도메인 특화 체크 id 목록 | `[flow_source_dedupe, flow_pit_join_coverage, ...]` |

service 는 이 spec 만 보고 표준 체크(C1~C13)를 자동 수행한다. 신규 테이블 = 카탈로그 항목 1개 추가.

### 1.3. 표준 체크 = PLAN.md §3 의 C1~C13 코드화

`PLAN.md` §3 의 C1~C13 체크리스트를 그대로 `CheckKind` Enum + SQL 빌더로 코드화한다.
spec 에 해당 컬럼이 없으면 자동 생략(현재 수작업 규칙을 코드가 강제).

| 코드 | 코드화된 메서드 | 그래프 |
|---|---|---|
| C1 행수/키/범위 | `count_keys_range()` | — (헤더 카드) |
| C2 시간 분포 | `time_distribution()` | 라인/area (연·월별 행수·엔티티수) |
| C3 카테고리 분포 | `category_distribution()` | 막대 / treemap |
| C4 NULL 비율 | `null_ratios()` | 가로 막대 |
| C5 자연키 중복 | `duplicate_groups()` | — (수치 카드) |
| C6 엔티티당 행수 분포 | `per_entity_distribution()` | 히스토그램 + box |
| C7 엔티티×시간 커버리지 | `entity_time_coverage()` | 히트맵 |
| C8 수치 분위수 | `numeric_quantiles()` | box/violin, log축 히스토그램 |
| C9 코드 Top-N | `category_top_n()` | 가로 막대 Top-20 |
| C10 적재시각 추세 | `ingest_time_trend()` | 라인 |
| C11 통화/단위 스케일 | `unit_scale()` | 그룹 막대 |
| C12 FK 정합성 | `fk_integrity()` | — (수치 카드) |
| C13 PIT 가용성 | `pit_validity()` | 산점/플래그 카드 |

### 1.4. 대형 테이블 샘플링 강제

`dart_xbrl_fact_raw`(80M), `krx_security_flow_raw`(76M), `dart_financial_statement_raw`(17M) 등은
spec 의 `sampling` 정책에 따라 고비용 분위수/문자열 Top-N 을 `TABLESAMPLE SYSTEM (1)` 또는 corp/연도 subset 으로 수행하고,
**아티팩트와 노트북에 "샘플링됨/샘플비율" 메타를 자동 표기**한다(현재 수작업 표기 규약을 코드가 보장).

---

## 2. 산출물 포맷 & 디렉터리 레이아웃

### 2.1. 3종 포맷의 역할

1. **Jupyter Notebook (.ipynb)** — 1차 산출물. 테이블당 1개(+ 다중 metric 테이블은 드릴다운 노트북). papermill 로 `{table, target, run_date}` 파라미터 주입 후 nbclient 로 실행 → 그래프가 셀 출력으로 인라인 임베드. 사람이 셀을 다시 돌려 인터랙티브 탐색 가능.
2. **기계판독 아티팩트 (Parquet + JSON)** — 모든 체크 결과를 정규화 저장. 실행 간 **diff/드리프트 감지**, 데이터 품질 모니터링, 후속 피처 카탈로그 입력으로 재활용. JSON 은 요약/매니페스트, Parquet 은 행 단위 분포 테이블.
3. **HTML (nbconvert `--to html`)** — 실행된 노트북을 정적 HTML 로 익스포트. 커널/주피터 없이 브라우저로 그래프 포함 공유. (제약상의 3번째 포맷.)

> Markdown·Plotly-standalone HTML 은 후보였으나 미채택. 단, 노트북 셀에 Plotly 를 쓰면 HTML 익스포트가 그대로 인터랙티브가 되므로 정적/인터랙티브 둘 다 커버된다.

### 2.2. 디렉터리 (피처별 파일 분리 — 테이블 단위 + 드릴다운)

분석 단위 결정: **테이블당 1개 상위 프로파일 + 다중 metric/feature 테이블은 하위 디렉터리에 피처별 분리.**
드릴다운 자동 분리 임계값: `drilldown_dim` 의 distinct 수가 임계(기본 ≥ 5) 이상이면 하위 디렉터리 생성.

대상: `krx_security_flow_raw`(7 metric), `stock_metric_fact`(29 metric), `common_feature_daily_fact`(37 feature), `dart_xbrl_fact_raw`(concept Top-N).

```
docs/features/table_stat_20260528/generated/
  <target>/<run_date>/                # 예: local/2026-06-19  — 실행 이력 보존(diff 용)
    index.html                        # 전체 테이블 요약 대시보드(상태·행수·결측 한눈에)
    index.json                        # 동일 요약의 기계판독본
    tables/
      daily_ohlcv.ipynb
      daily_ohlcv.html
      krx_security_flow_raw.ipynb     # 테이블 상위 프로파일
      krx_security_flow_raw.html
      krx_security_flow_raw/          # 드릴다운(피처별 분리)
        metric_foreign_holding_shares.ipynb
        metric_short_selling_value.ipynb
        ...
    artifacts/
      daily_ohlcv.stats.json
      daily_ohlcv.dist.parquet        # 분포/시간/카테고리 행단위 테이블
      _run_manifest.json              # 실행 메타(target, run_date, 행수, 샘플비율, lib 버전)
  latest -> 2026-06-19                # 최신 실행 심볼릭(diff 기준)
```

`run_date` 디렉터리 분리로 실행 이력이 쌓이고, `latest` 대비 **이전 실행과의 diff** 를 산출할 수 있다.

---

## 3. CLI 설계 (`cli/app.py` 합성 루트)

신규 top-level 서브커맨드 `profile` 을 추가한다(기존 `metrics`/`validate` 와 동일 패턴).

```bash
# 단일 테이블
uv run krx-collector profile table daily_ohlcv --target local

# 전체 카탈로그(가중치별)
uv run krx-collector profile all --target local --weight full,light

# 드릴다운 포함
uv run krx-collector profile table krx_security_flow_raw --drilldown --target local

# 포맷 선택(기본 ipynb,artifact,html)
uv run krx-collector profile table daily_ohlcv --formats ipynb,artifact

# 이전 실행 대비 통계 diff (드리프트 리포트)
uv run krx-collector profile diff --target local --against latest

# sj2 복구 시 미러 지연 비교
uv run krx-collector profile table daily_ohlcv --target sj2
```

- 옵션: `--target {local,sj2}`(dbq.sh 동일), `--weight`, `--formats`, `--drilldown`, `--sample-pct`, `--out-dir`, `--run-date`(미지정 시 `now_kst()` 날짜), `--against`(diff).
- 종료코드: 데이터 없음/연결 실패는 명확히 구분(기존 partial-run/exit-code 규약 존중). 카탈로그에 0행 테이블(`operating_*`)은 "skipped: empty" 로 표기하고 정상 종료.

---

## 4. 테이블별 최적화 분석 (PLAN.md §4 → 카탈로그 domain_checks 매핑)

각 테이블의 도메인 특화 항목을 `domain_checks` 와 노트북 섹션/그래프로 코드화한다.
"피처 특성이 다르다"는 요구를 카탈로그의 테이블별 설정과 전용 체크로 흡수한다.

| 테이블 | 핵심 domain_checks (PLAN.md §4 근거) | 특화 그래프 |
|---|---|---|
| `daily_ohlcv` | OHLC 항등식(`low<=min(o,c) & high>=max(o,c)`), `is_halted=(o=h=l=0)` 비율, volume=0 비율, 종목별 상장 span, 가격/거래대금 fat-tail | 캔들 정합성 카드, log(close) 히스토그램, 거래대금 분위수 box, 거래일 커버리지 히트맵 |
| `krx_security_flow_raw` | metric_code 7종 커버리지 비대칭, KRX/PYKRX **source dedupe**(동일 자연키 값충돌), 공매도잔고 2016-06-30 시작, `daily_ohlcv` PIT join 커버리지(연도별), 합계 항등식 **검증 금지** 주석 | metric×연도 커버리지 히트맵, metric별 value 분포(드릴다운 7파일), join 커버리지 라인 |
| `dart_xbrl_fact_raw` | **샘플링 필수**, concept_id/element_id Top-50, unit/decimals 분포, instant/duration 비율, `dart_xbrl_document` 고아 fact | concept Top-50 막대, 통화 분포, 연도×corp fact수 히트맵 |
| `dart_xbrl_document` | 보고서유형/연도/분기 분포, `dart_financial_statement_raw.rcept_no` 교집합/차집합, 재공시 횟수 | 보고서유형 막대, 교집합 벤 카드 |
| `dart_financial_statement_raw` | `fs_div=CFS` 단독, `sj_div` 분포, frmtrm/bfefrmtrm NULL률, account_id 표준코드 미사용률, 금액 fat-tail | sj_div 막대, NULL 비율 막대, 금액 log 분포 |
| `dart_shareholder_return_raw` | 이벤트유형 코드 분포, `stock_knd` 비표준 50+, value_numeric ~77% NULL, 재공시 패턴 | 이벤트유형 Top-N, NULL률 막대 |
| `dart_share_count_raw` | `se` 139종 비표준, 발행주식총수 시계열 jump 탐지, corp당 변동 빈도 | 변동사유 Top-N, jump 산점 |
| `stock_metric_fact` | metric_code 29 × source_table 분포, 종목/연도 커버리지, IS 5종 매핑 희소, 자본잠식, 자연키 중복 | metric별 커버리지 히트맵(드릴다운 29), source_table 막대, core/sparse 분리 표 |
| `dart_corp_master` | 상장/비상장 비율, ticker NULL 97.4%, modify_date 분포, `stock_master` join 일치율, 스냅샷 지연 | 상태 막대, modify_date 라인 |
| `stock_master*` | 시장/상태 분포, 우선주 식별, 스냅샷 간격·추가/제거 수, 부분 스냅샷(2026-05-21 KOSPI만) 경고 | 스냅샷 종목수 라인, 시장 막대 |
| `common_feature_*` | series source/category/freq active 수, observation 범위·vintage, **PIT 위반(available_from_date<=feature_date)**, daily fact forward-fill/stale 구간, 카탈로그↔series 고아, daily_ohlcv 거래일 대비 coverage gap | feature별 커버리지 히트맵(드릴다운 37), PIT 위반 카드, stale 구간 라인 |
| `metric_catalog`/`metric_mapping_rule` | active 수, distinct key, 매핑 규칙이 참조하는 metric code 유효성, 미사용 XBRL fallback | 경량 표 |
| `ingestion_runs` | run_type별 success/failed/partial/running 비율, stale running, 평균 소요, 최근 실행 | 상태 stacked 막대, 소요 box |
| `sync_checkpoints` | 채널별 cursor, stale 여부 | 경량 카드 |
| `operating_*` | 0행 → "skipped: empty". 적재 후 FK 정합성·섹터/문서유형/metric 분포·추출기 성공률 (체크는 미리 작성, 데이터 없으면 자동 skip) | 적재 후 활성화 |

---

## 5. 모듈별 구현 명세

### 5.1. `domain/profiling.py` (순수)

```python
class CheckKind(StrEnum): COUNT_KEYS_RANGE; TIME_DIST; CATEGORY_DIST; NULL_RATIOS;
    DUP_GROUPS; PER_ENTITY_DIST; ENTITY_TIME_COVERAGE; NUMERIC_QUANTILES;
    CATEGORY_TOP_N; INGEST_TREND; UNIT_SCALE; FK_INTEGRITY; PIT_VALIDITY

class ProfileWeight(StrEnum): FULL; LIGHT

@dataclass(frozen=True)
class TableProfileSpec: table; weight; entity_key; time_col; natural_key;
    numeric_cols; category_cols; null_cols; fk_relations; drilldown_dim;
    sampling; domain_checks

@dataclass class CheckResult: kind; rows: list[dict]; sampled: bool; sample_pct; sql; note
@dataclass class ProfileResult: spec; generated_at; row_count; checks: list[CheckResult];
    drilldown: dict[str, list[CheckResult]]
```

### 5.2. `ports/profiling.py` (Protocol)

```python
class ProfileQueryRunner(Protocol):
    def describe_schema(self, table) -> list[ColumnInfo]: ...
    def run_check(self, spec, kind, *, drill_value=None) -> CheckResult: ...

class ProfileRenderer(Protocol):
    def render(self, result: ProfileResult, *, out_dir, formats) -> list[Path]: ...
```

### 5.3. `service/profile_table.py`

- `build_profile(spec, runner) -> ProfileResult`: spec 의 컬럼 존재에 맞춰 적용 가능한 표준 체크 + domain_checks 선택, 드릴다운 차원 distinct 조회 후 임계 초과 시 per-value 체크. **SQL 을 직접 만들지 않는다**(전부 runner 에 위임) → 헥사고날 순수성 유지.
- `profile_catalog(specs, runner, renderer, ...)`: 전체 순회 + index 요약 조립.

### 5.4. `infra/db_postgres/profiling_query_runner.py`

- `PostgresProfileQueryRunner` — 기존 `get_connection(dsn)` 사용. `CheckKind` → SQL 템플릿 매핑(파라미터 바인딩, identifier 화이트리스트로 SQL injection 차단: 테이블/컬럼명은 카탈로그 출처만 허용).
- 대형 테이블: `sampling` 정책에 따라 분위수/Top-N 만 `TABLESAMPLE` 적용, 행수/커버리지는 본실행. 결과에 `sampled/sample_pct` 기록.
- `--target` → DSN 선택: local 은 `.env DB_DSN`, sj2 는 secrets `db_info`(dbq.sh 와 동일 출처) 재사용.

### 5.5. `adapters/profiling_render/`

- `notebook_renderer.py` — nbformat 으로 셀 조립(헤더/스키마/체크별 표+그래프), papermill 파라미터, nbclient 실행. 그래프는 matplotlib(정적) 기본, 분포·커버리지는 선택적 plotly(인터랙티브 HTML).
- `artifact_renderer.py` — JSON(요약/매니페스트) + Parquet(행단위 분포) via pyarrow.
- `html_renderer.py` — nbconvert 로 실행된 노트북 → HTML.
- `index_renderer.py` — 전 테이블 상태 대시보드(index.html/json).
- `diff.py` — 이전 run artifacts(JSON) 대비 행수/NULL/분위수/커버리지 변화 리포트(드리프트 임계 초과 강조).

### 5.6. 의존성 추가 (`pyproject.toml` `[analysis]` extra)

기존 dev 와 분리한 optional extra:
```toml
[project.optional-dependencies]
analysis = ["matplotlib>=3.8", "plotly>=5.20", "nbformat>=5.10",
            "nbclient>=0.10", "nbconvert>=7.16", "papermill>=2.6", "pyarrow>=15"]
```
(matplotlib·plotly 는 환경에 이미 존재. nbformat/nbclient/nbconvert/papermill/pyarrow 신규.)
프로덕션 이미지에는 미포함 — 프로파일링은 분석 도구이지 수집 파이프라인이 아니므로 운영 컨테이너 비대화 방지.

---

## 6. 검증 전략

1. **기존 수작업 프로파일과 패리티 검증** — `daily_ohlcv` 를 먼저 코드로 생성해, 현재 `statistical_profile_daily_ohlcv.md` 의 수치(행수, KOSPI/KOSDAQ 비율, zero_open 1.597%, close 분위수 등)와 일치하는지 대조. 일치 = 코드 신뢰 확보 후 나머지 확장.
2. **단위 테스트(`tests/unit/`, DB 불필요)** — 카탈로그 완전성(23개 테이블 누락 없음), SQL 빌더 식별자 화이트리스트, CheckKind→SQL 스냅샷, spec 컬럼 미존재 시 자동 생략.
3. **통합 테스트(`tests/integration/`)** — 기존 규약대로 DB 미도달 시 self-skip. local 에서 소형 테이블(`sync_checkpoints`, `stock_master`) 전체 파이프라인 smoke.
4. **재실행 멱등성** — 동일 `run_date` 재실행 시 산출물 안정, diff 0.

---

## 7. 구현 마일스톤

> **상태(2026-06-19): M0~M3 전부 구현·검증 완료.** 통합 실행 계획·코드 위치는
> [`automated_feature_profiling_plans/final.md`](../automated_feature_profiling_plans/final.md) §8 참고.

- **M0 — 스캐폴딩 + 레퍼런스(`daily_ohlcv`)** — ✅ 완료
  - domain/ports/service/infra/renderer 골격, 카탈로그 1개, 표준 체크 C1~C8, 노트북+아티팩트 렌더러.
  - §6.1 패리티 검증 통과(KOSDAQ/KOSPI 60.2/39.8%, halted 1.61%, close>high 2.36% 등 수동 문서와 일치).
- **M1 — Wave 0 대형 테이블 + 샘플링** — ✅ 완료
  - `krx_security_flow_raw`, `dart_xbrl_fact_raw`, `dart_financial_statement_raw`, `stock_metric_fact` + DART raw 추가. TABLESAMPLE·드릴다운(metric_code)·`bsns_year` INT 시간축 타입 인지 도입.
- **M2 — common_feature_* + PIT + 드릴다운 완성** — ✅ 완료
  - C13 PIT 체크, feature 드릴다운(37), 커버리지 gap, stale-run, HTML 익스포트, index 대시보드.
- **M3 — 마스터/카탈로그/운영 경량 + diff + CLI 마감** — ✅ 완료
  - light weight 체크(총 23개 테이블), `profile diff` 드리프트 리포트, `profile publish`, long-format per-value 파일 분리, `operating_*` skip-empty, 단위 40 + 통합 3 테스트, 문서화.

각 마일스톤이 `PLAN.md` 의 수작업 체크리스트를 코드 생성본으로 대체한다.

---

## 8. 산출물의 활용 (PLAN.md §8 계승·확장)

- 누적 run 디렉터리 → 모델링 피처 카탈로그(`docs/features/feature_catalog.md`) 1차 입력으로 자동 갱신.
- 아티팩트(JSON/Parquet)는 **데이터 품질 모니터링·드리프트 감지**의 재료(`profile diff`).
- `common_feature_*` PIT/stale/coverage gap 결과는 종목 패널 결합 시 look-ahead 방지·stale 제한·coverage gate 기준으로 재사용.
- sj2 복구 시 `--target sj2` 동일 코드 실행으로 로컬 미러 지연을 정량 비교.

---

## 9. 미해결/후속 결정 사항

- 카탈로그 형식: Python 모듈(타입체크 유리) vs YAML(비개발자 편집 유리) — M0 에서 Python dataclass 로 시작, 필요 시 YAML 로더 추가.
- 그래프 엔진 기본값: 정적(matplotlib) vs 인터랙티브(plotly) — 노트북 기본 matplotlib, 커버리지 히트맵/분포는 plotly 옵션(`--interactive`).
- diff 임계값(드리프트 경보 기준)은 M3 에서 테이블별 튜닝.
