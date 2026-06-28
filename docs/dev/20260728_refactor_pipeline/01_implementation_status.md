# 구현 상태 및 이어가기 가이드 (P1–P6)

갱신: 2026-06-29 / 짝 문서: [`00_refactor_plan.md`](00_refactor_plan.md)(설계 레퍼런스)

이 문서는 리팩터의 **현재 구현 상태**, **검증 결과**, **남은 작업**, **다음 세션이 이어가는 법**을 담는다.

---

## 0. 한눈에 보기

| 단계 | 상태 | 비고 |
|---|---|---|
| **P1** DuckDB 마트 구현 | ✅ 완료 | normalize/common_build/reports 마트 + 정의 분리 |
| **P2** Parity 검증 | ✅ 완료 | 차등→**골든** 전환, 18개 골든 테스트로 동치 동결 |
| **P3** compute 오케스트레이션 | ✅ 완료 | `compute_all.py` + `bin/parquet-compute-all.sh` |
| **P4** sj2 Cronicle compute 제거 | ⛔ **미실행(승인 필요)** | 운영 mutation — 다음 세션에서 `sj2-server` 스킬로 |
| **P5** 비-raw 테이블 디커미션 | ✅ 코드 완료 / ⛔ **실제 DROP 미실행(승인 필요)** | 코드·DDL·테스트 모두 반영. 실DB drop만 남음 |
| **P6** 정리(문서/릴리스) | ✅ repo 완료 / ⏳ README·릴리스 잔여 | exporter deprecate + 문서 갱신 완료 |

**검증(현재 워킹트리):** `uv run pytest tests/unit` → **408 passed**. `ruff check` / `black --check` 클린.
`tests/integration` 36개 collect 정상(실DB는 self-skip).

**브랜치:** 작업은 `main`에서 진행, **아직 커밋 안 함**. 커밋 시 별도 브랜치 권장.

---

## 1. 완료한 작업 상세

### P1 — 마트 + 정의 분리

**신규 (대부분 `research/`는 gitignore → untracked, 단 `src/definitions`는 tracked):**
- `src/krx_collector/definitions/metric_rules.py` — `default_metric_catalog`,
  `default_metric_mapping_rules`, `reprt_code_to_period_type`, `infer_period_end` (Storage 의존 0).
- `src/krx_collector/definitions/common_features.py` — `default_common_feature_series`,
  `default_common_feature_catalog` + `_feature`/`_multi_input_feature` 헬퍼.
- `research/etl/marts/metrics_normalize.py` — raw 4소스 → `stock_metric_fact` SQL 포팅
  (룰 매칭 와일드카드, `QUALIFY ROW_NUMBER() ORDER BY priority, candidate_rank, source_key`,
  XBRL dimensions rank, source별 `value_text` 캐스팅).
- `research/etl/marts/common_build.py` — observation_raw → `common_feature_daily_fact`
  (period-latest-vintage as-of, stale 영업일 게이트, positional lag vs calendar-offset,
  vol ddof=1, spread/ratio multi-input). 나눗셈은 `DECIMAL(38,12)` 고정으로 exact.
  series config는 `common_feature_series` 레이크 뷰 우선, 없으면 코드 정의 fallback(결정 7).
- `research/etl/marts/reports.py` — coverage/readiness/freshness DuckDB 체크(§4).
- `research/etl/marts/__init__.py`.

**수정:** `service/normalize_metrics.py`(→ 후에 §P5에서 삭제됨), `default_common_feature_catalog.py`
(정의를 definitions로 위임).

### P2 — Parity 검증(골든 전환)

> **핵심 결정:** 현재 Postgres canonical parquet는 **stale 오라클**(과거 룰버전 + 부분연도)이라
> 행수 비교에 부적합. 올바른 parity는 "동일 raw + 동일 현재 룰" 비교다. 처음엔 service를 **라이브
> 오라클**로 쓰는 차등 테스트였으나, P5에서 service를 지우므로 **골든값으로 동결**했다.

- 공유 픽스처 분리(service 비의존): `tests/unit/_metric_fixtures.py`, `tests/unit/_common_fixtures.py`.
  (service 테스트들도 여기서 import → service 삭제와 무관하게 픽스처 유지.)
- 골든 파일: `tests/unit/golden/{stock_metric_fact,common_feature_daily_fact,common_feature_reports}.json`.
- 골든 테스트: `tests/unit/test_metrics_normalize_mart.py`(3), `test_common_build_mart.py`(11 시나리오:
  level/ret/change/vol/stale/yoy/yoy_null/mom/spread/ratio/latest_vintage), `test_reports_mart.py`(4).
- **tolerance:** financial=exact, common 파생=DECIMAL exact, **vol만 상대오차 1e-9**(Decimal.sqrt vs DOUBLE).
- **골든 재생성:** `SDC_UPDATE_GOLDEN=1 uv run pytest tests/unit/test_*_mart.py`. service 삭제 후엔
  오라클이 없어 regen이 **명확한 RuntimeError**로 실패(graceful) → 골든이 진실의 원천. 룰 변경 시
  마트를 고치고 골든 diff를 수동 리뷰해 갱신한다.
- 실제 프로덕션 레이크(snapshot 2026-06-19)에서도 마트가 현재 룰 100% 준수 확인(룰 위반 0건).

### P3 — compute 오케스트레이션

- `research/etl/compute_all.py`(untracked) — `run(from_step=...)`: freshness→marts→reports→features.
  게이트 미달 시 non-zero exit + stderr 요약(자동 notifier 없음, OQ1).
- `bin/parquet-compute-all.sh`(tracked, 신규) — `db sync-remote` → `raw-parquet-export-all.sh` →
  `compute_all`. `--snapshot-date`/`--from-step`/`--skip-sync`/`--features`/`--required-coverage-ratio`.
- `research/etl/lake.py`: `register_derived_marts()`(두 fact를 마트로 빌드 후 canonical 뷰명으로 등록) +
  `_common_feature_calendars()`(KRX 세션 캘린더 from raw).
- `research/etl/config.py`: `CONFIG_TABLES=("common_feature_series",)` 추가(raw lake root로 라우팅),
  `DERIVED_MART_LAKE_NAME="derived_mart"` 추가(`canonical_postgres`는 A/B 동안 읽기 유지).
- 소비자 재배선: `research/models/_01_20_access_return_rank/build_dataset.py`,
  `tests/integration/test_research_{fin_pit,common_event}_smoke.py`가 canonical 직접 읽기 →
  raw 입력 등록 + `register_derived_marts` 호출로 전환(`canonical_root.exists()` 게이트 → `raw_root`).
- exporter: `tools/raw-parquet-exporter/config/export_tables.toml`에 `common_feature_series` 추가(결정 7).

### P5 — 비-raw 테이블 디커미션 (코드/DDL 완료, 실DB DROP 미실행)

**삭제된 src 모듈:** `service/{normalize_metrics, build_common_feature_daily_facts,
report_common_feature_coverage, report_common_feature_readiness, report_metric_coverage,
process_operating_document, default_operating_registry, operating_registry}.py`,
`ports/operating_extractors.py`, `adapters/operating_extractors/`.

**축소/수정:**
- `service/freshness.py` — raw-status 전용으로 축소. `assert_common_freshness`(게이트)는 제거되고
  `research/etl/marts/reports.py:freshness_violations`로 이동. `build_freshness_report`는 유지하되
  dropped-table 참조(`stock_metric_fact` 연도범위, `common_feature_daily_fact` max-date) 제거.
- `service/default_common_feature_catalog.py` — `seed_common_feature_catalog`가 **series만** upsert
  (catalog는 코드 전용, 결정 7).
- `cli/app.py` — compute 서브커맨드 전부 제거: `metrics normalize/coverage-report`,
  `common build-daily/coverage-report/readiness-report`, `ops assert-common-freshness`,
  `operating process-document`(핸들러+파서). 남은 명령: `db/universe/prices/dart/common(seed-catalog·sync)/
  flows/ops(freshness-report)/validate/profile`.
  - ⚠️ **주의(이미 처리됨):** ops 핸들러 삭제 중 인접한 dart 헬퍼 3개
    (`_dart_{financial,share_info,xbrl}_actual_attempt_estimate`)를 실수로 지웠다가 git HEAD에서
    복원함. 다음에 이 영역 손대면 동일 실수 주의.
- `infra/db_postgres/remote_sync.py` — 미러 목록 13개로 축소(raw 11 + `common_feature_series` +
  `common_feature_observation_raw`; 파생 fact 2 + catalog/rule 4 제외). `_select_sync_specs(None)`
  기본 경로를 `PIPELINE_FULL_REFRESH_TABLE_NAMES`로 필터(SYNC_TABLE_SPECS의 dropped-table spec은
  inert로 잔류). `SYNC_TABLE_DEPENDENCIES`도 정리.
- `service/profiling/catalog.py` — dropped-table profile spec 8개 + `_CATALOG` 엔트리 제거(15개 잔존).
- `sql/postgres_ddl.sql` — **8개 테이블 DDL 제거**(stock_metric_fact, common_feature_daily_fact,
  metric_catalog, metric_mapping_rule, common_feature_catalog, common_feature_catalog_input,
  operating_metric_fact, operating_source_document). 16개 잔존, dangling FK/index 없음.
- `tools/raw-parquet-exporter/config/export_canonical_tables.toml` + `bin/canonical-parquet-export-all.sh`
  — deprecated 주석(폐기 아님; pre-drop 백업 용도로 보존).

**삭제/수정된 테스트:** `test_{metric_normalization, build_common_feature_daily_facts,
common_feature_coverage_report, common_feature_readiness_report, metric_coverage_report,
operating_metrics, freshness}.py` 삭제. `test_{cli_entrypoints, profiling, remote_db_sync,
research_config, common_features_storage, default_common_feature_catalog}.py` + 2개 통합 스모크 수정.

**마이그레이션 SQL(tracked, 미실행):** `sql/migrations/20260728_drop_derived_tables.sql` —
8테이블 child-first DROP(roll-forward). 헤더에 백업 전제조건 명시(특히 `operating_*` pg_dump 필수).

### P6 — 정리(repo)

- `CLAUDE.md` — CLI 트리/아키텍처/raw·derived 2계층/`definitions/`/compute 파이프라인 섹션 갱신.
- `docs/operations.md` — "Parquet compute 파이프라인(수동 실행)" 런북 추가(OQ1 완료 조건).

---

## 2. 남은 작업 (다음 세션)

### A. 운영 mutation — **사용자 승인 필수**

1. **P5 실제 DROP** — `sdc-db` 스킬로:
   - **선행(필수):** `operating_source_document`/`operating_metric_fact` **pg_dump 백업**
     (content_text/raw_payload는 어떤 parquet에도 없음). 파생 fact 2개는 마트가 재생성하므로 백업 선택.
   - 로컬 `mydb` + sj2 `krx_data` 양쪽에서 `sql/migrations/20260728_drop_derived_tables.sql` 실행.
2. **P4 Cronicle** — `sj2-server` 스킬로:
   - 제거: `sdc_daily_metrics_normalize`, `sdc_daily_common_build/coverage/readiness` 4개 이벤트.
   - 체인 재배선: 04:00 `sync-xbrl`의 `chain=''`(metrics-normalize tail 제거), 23:30 root 삭제.
   - 18:30/20:30 raw 체인 무변경. sj2 신규 알람 이벤트 없음(freshness는 compute 노드로 이동).
   - 적용 후 며칠 raw-only 관찰(`ingestion_runs`).

### B. repo 잔여 (가역, 저위험)

3. **README.md 한글판** — 제거된 compute 명령(`metrics normalize/coverage-report`,
   `common build-daily/coverage-report/readiness-report`, `operating process-document`) 정리 +
   `bin/parquet-compute-all.sh` 절차로 대체. (CLAUDE.md/operations.md는 이미 갱신됨.)
4. **`sdc-release` 버전 범프** — **P4/P5 prod 적용 후** 수행 권장(이미지가 raw-only CLI 반영).
5. **(선택, 저가치) inert dead code 제거** — `infra/db_postgres/repositories.py` +
   `ports/storage.py`의 dropped-table 메서드(`upsert_stock_metric_facts`,
   `upsert_common_feature_daily_facts`, `get_operating_metric_facts`, `upsert_metric_catalog`,
   `replace_metric_mapping_rules` 등)와 미사용 도메인 result 모델(`MetricNormalizationResult`,
   `MetricCoverageReport`, `CommonFeatureCoverageReport`, `CommonFeatureReadinessReport`,
   `CommonFeatureBuildResult`). 호출되지 않고 테스트도 통과하나 dropped 테이블을 참조.
   ⚠️ `MetricCatalogEntry`/`MetricMappingRule`/`CommonFeatureCatalogEntry`/`StockMetricFact` 등은
   **definitions·마트·골든 테스트가 계속 사용**하므로 보존. 제거 시
   `tests/unit/test_common_features_storage.py`의 catalog/daily-fact 메서드 테스트와
   `test_common_feature_models.py`도 함께 정리 필요.

---

## 3. 이어가기 빠른 참조

```bash
# 현재 상태 검증
uv run pytest tests/unit -q                    # 408 passed 기대
uv run ruff check src/ tests/ research/etl/
uv run black --check src/ tests/ research/etl/

# 마트 동작(실제 로컬 레이크 snapshot 2026-06-19 존재 시)
uv run python -c "from research.etl.config import LakeConfig; from research.etl.lake import connect, register_views, register_derived_marts; \
cfg=LakeConfig(); con=connect(cfg); \
register_views(con,cfg,tables=['daily_ohlcv','dart_financial_statement_raw','dart_share_count_raw','dart_shareholder_return_raw','dart_xbrl_fact_raw','dart_corp_master']); \
print('smf rows:', con.execute('SELECT count(*) FROM '+register_derived_marts(con,cfg,which=['stock_metric_fact'])[0]).fetchone())"

# compute 파이프라인(수동)
bin/parquet-compute-all.sh --help

# 골든 재생성(주의: service 삭제됨 → RuntimeError로 실패. 골든이 진실의 원천)
```

**관련 메모리:** `refactor-parquet-mart-parity.md`(parity 오라클 함정 + 진행상황).

**기존 integration 실패(내 작업과 무관, 주의):** `test_research_build_dataset_smoke.py`의
`test_per_date_zscore_and_finite`/`test_manifest_pins_reproducibility`와
`test_research_train_smoke.py` 2개는 **px+flow만 + force_mart로도 재현**되는 이 lake snapshot의
panel/표준화 선행 문제. 리팩터 회귀 아님.

---

## 4. 변경 파일 인벤토리 (커밋 참고)

**tracked 수정:** `CLAUDE.md`, `docs/operations.md`, `sql/postgres_ddl.sql`, `cli/app.py`,
`infra/db_postgres/remote_sync.py`, `service/{freshness,default_common_feature_catalog}.py`,
`service/profiling/catalog.py`, `bin/canonical-parquet-export-all.sh`,
`tools/raw-parquet-exporter/config/export_{tables,canonical_tables}.toml`, 다수 `tests/`.

**tracked 신규:** `bin/parquet-compute-all.sh`, `src/krx_collector/definitions/`,
`sql/migrations/20260728_drop_derived_tables.sql`, `tests/unit/_{metric,common}_fixtures.py`,
`tests/unit/golden/`, `tests/unit/test_{metrics_normalize,common_build,reports}_mart.py`,
`docs/dev/20260728_refactor_pipeline/`.

**tracked 삭제:** §1 P5의 service/ports/adapters/tests 목록 참조.

**untracked(`research/` gitignore — 로컬 전용):** `research/etl/marts/*.py`, `research/etl/compute_all.py`,
그리고 `research/etl/{config,lake}.py`·`models/.../build_dataset.py` 수정분.
⚠️ 마트 코드가 gitignore라 CI/원격엔 안 올라간다(CI는 tag시 docker build만, pytest 미실행). 로컬에서만 동작.

**부수효과(black 리포맷만, 로직 무변):** `adapters/common_features_krx/provider.py`,
`adapters/krx_common/client.py`, `service/sync_common_features.py` — 광범위 `black src/` 실행의 결과.
