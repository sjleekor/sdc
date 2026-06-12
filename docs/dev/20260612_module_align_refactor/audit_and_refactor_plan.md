# 수집 모듈 정합성 감사 및 표준화 리팩토링 계획

- 작성일: 2026-06-12 (같은 날 sj2-server 실측 반영하여 갱신)
- 범위: `src/krx_collector` 전체 수집 모듈 + `deploy/prod/bin/` 운영 래퍼 + **sj2-server 프로덕션 DB(`krx_data`) / Cronicle 실측**
- 감사 항목: ① 중복 다운로드 방지(멱등성) ② 수집 대상 기간(2015~현재) ③ KRX API throttling 및 중앙화

---

## 0. 전제에 대한 정정

감사에 앞서 한 가지 정정한다. 이 코드베이스는 "구조가 없어서 난잡한" 상태가 **아니다**. 이미 엄격한 헥사고날 아키텍처(ports/adapters/service/infra), 공용 throttle/retry 유틸(`util/pipeline.py`), 일관된 감사 기록(`ingestion_runs`)을 갖추고 있다. 실제 문제는 다음 두 가지다.

1. **기존 패턴의 불균일한 채택** — 신규 모듈(DART, flows, common features)은 모범적으로 구현되어 있으나, 오래된 모듈/pykrx 기반 모듈은 같은 패턴을 따르지 않는다.
2. **운영(프로덕션) 설정 공백** — 코드로는 가능한 것이 Cronicle 스케줄/래퍼 스크립트 수준에서 빠져 있다.

따라서 본 리팩토링 계획은 새 아키텍처를 발명하는 것이 아니라, **가장 잘 구현된 모듈의 패턴을 표준으로 승격하고 나머지를 그 위로 이주**시키는 방향이다.

---

## 1. sj2-server 실측 결과 (2026-06-12)

> 조회 경로: `ssh whi@sj2-server` → `docker exec sdc-postgres psql -U krx_user -d krx_data`(직접 5432 접속은 No route to host), Cronicle API `http://sj2-server:3012/api/app/*`.

### 1.1 시계열 테이블 커버리지

| 테이블 | min | max | rows |
|---|---|---|---|
| `daily_ohlcv` | 2007-06-05 | **2026-06-10** | 6,550,517 |
| `krx_security_flow_raw` | 2007-06-05 | 2026-06-10 | 76,249,700 |
| `common_feature_observation_raw` | **2024-09-30** | 2026-06-12 | 2,738 |
| `common_feature_daily_fact` | **2025-11-03** | 2026-06-12 | 5,550 |

### 1.2 DART 계열 연도 분포 — **2015~2024 백필 완료 확인**

`ingestion_runs` 기록상 2026-06-06~06-08에 `dart-backfill-all-years.sh`가 2024→2015 내림차순으로 실행되어 연도별 `dart_financial_sync → dart_share_info_sync → xbrl_parse → metric_normalize`가 **전부 `success`로 완주**했다. 직전 검증 문서(20260606)의 "2025~2026만 보유" 상태는 해소되었다.

| bsns_year | financial_raw | xbrl_doc | share_count | sh_return | metric_fact |
|---|---|---|---|---|---|
| 2015 | **359,184** | **1,586** | 23,791 | 447,399 | **12,827** |
| 2016 | 1,446,642 | 6,428 | 25,259 | 664,791 | 17,726 |
| 2017 | 1,478,740 | 6,794 | 26,880 | 702,563 | 18,950 |
| 2018 | 1,536,833 | 7,135 | 28,225 | 730,538 | 20,161 |
| 2019 | 1,616,439 | 7,474 | 29,686 | 767,217 | 62,946 |
| 2020 | 1,677,490 | 7,850 | 31,199 | 816,371 | 108,655 |
| 2021 | 1,755,891 | 8,279 | 32,971 | 857,890 | 113,410 |
| 2022 | 1,824,026 | 8,657 | 33,684 | 849,655 | 117,733 |
| 2023 | 1,920,840 | 9,204 | 34,465 | 853,673 | 124,445 |
| 2024 | 2,013,400 | 9,870 | 35,874 | 877,927 | 134,702 |
| 2025 | 1,016,063 | 8,255 | 10,295 | 263,030 | 34,411 |
| 2026 | 241,723 | — | — | — | — |

잔여 확인 항목:

- **2015년 financial(36만, 2016의 1/4)과 xbrl_doc(1,586, 2016의 1/4)이 이상 저점** — OpenDART 자체 커버리지 한계인지, 백필 부분 누락인지 검증 필요. `metric_fact` 2015~2018 저점(1.2만~2만 vs 2019+ 6만~13만)도 같은 원인 추정.
- 2025/2026은 연도 진행 중이라 부분 적재가 정상.

### 1.3 Cronicle 실측 — **자동 스케줄이 존재하지 않음 (신규 최중요 발견)**

| 이벤트 | enabled | timing | 내용 |
|---|---|---|---|
| `sdc_daily_pipeline` | 1 | **false** | universe-sync → prices-backfill-incremental → flows-sync |
| `sdc_daily_accounts_flows` | 1 | **false** | dart-sync-corp → financials → share-info → xbrl → metrics-normalize |
| `sdc_daily_common_features` | 1 | **false** | common-features-refresh.sh |

세 이벤트 모두 `timing: false`, `chain` 없음 — **즉 "일일 파이프라인"이 사실상 전부 수동 트리거로 운영 중**이다. 실행 이력도 04-26 / 05-21 / 05-23 / 06-10~12의 산발적 수동 실행뿐이다. 호스트 crontab에도 Cronicle 자체 기동(`@reboot`) 외 항목이 없다.

직접적 결과: 조회 시점(06-12 금) 기준 `daily_ohlcv`/`krx_security_flow_raw` 최신일이 06-10(수)로 **거래일 2일 지연**.

### 1.4 운영 건전성 이슈

- **고아 `running` run 17건** — `ingestion_runs`에 2026-04-10~06-10 사이 `status='running'`으로 영구 잔류한 row 17개(daily_backfill 6, krx_flow_sync 7, metric_normalize 3, dart_financial_sync 1). 프로세스 중단/재부팅 시 run을 finalize하는 장치(reaper)가 없다.
- **최근 `sdc_daily_pipeline` 실패** — 06-10 23:25 실행이 약 24.3시간 경과 후 exit code 1로 종료. flows 단계(`krx_flow_sync`, 06-10 23:32 시작)가 고아 `running`으로 남음. `flows-sync.sh`가 매회 `--use-price-range`(2007~) 전 구간을 스캔하는 구조여서 skip 체크가 있어도 1회 실행이 수 시간~수십 시간 소요.
- 과거 05-21 `sdc_daily_pipeline` 실행은 elapsed **약 12.6일**(초기 전체 백필을 "일일" 이벤트로 수행).
- 호스트가 06-12 약 22:00 KST에 재부팅됨(조회 시점 uptime 23분). 컨테이너는 정상 기동.

---

## 2. 감사 리포트

### 2.1 이슈 ① — 멱등성 (중복 다운로드 방지)

DB 쓰기 계층은 **전 테이블이 균일하게 멱등**하다(`ON CONFLICT … DO UPDATE`). 차이는 "외부 API 호출 **전에** DB에 이미 있는지 확인하는가(skip-if-present)"에서 발생한다.

| 모듈 | 사전 skip 체크 | 판정 |
|---|---|---|
| `dart sync-financials / share-info / xbrl` | 있음 — 기존 키 튜플(예: `(corp_code, bsns_year, reprt_code, fs_div)`)을 미리 로드해 요청 단위로 skip, `--force`로 무시 가능 | **SOLID** |
| `dart sync-corp` | 있음 — 직전 성공 run 존재 시 skip | **SOLID** |
| `flows sync` | 있음 — 3종 커버리지 카운터(외인 보유: market-day 단위, 투자자/공매도: ticker-metric-date 단위) | **SOLID** |
| `common sync` | 있음 — 정확 카운트 + 비KRX 소스용 90% 커버리지 완화 폴백 | **SOLID** |
| `db sync-remote` | 있음 — `sync_checkpoints` 테이블의 복합 커서, 중단 후 재개 가능 | **SOLID** |
| `prices backfill` | 부분적 — gap detection(`query_missing_days()`) + min/max trade_date 클램핑(`service/backfill_daily.py:131–142`)은 있으나 **영속 체크포인트가 없어** 전체 백필 중단 시 gap 스캔을 처음부터 다시 수행 | PARTIAL |
| `metrics normalize` | 없음 — 매 실행마다 대상 ticker/연도 전체를 재변환. upsert가 쓰기는 dedupe하지만 변환 연산은 반복 | PARTIAL |
| `common build-daily` | 없음 — 범위 내 fact를 매번 전체 재빌드(파생 데이터이므로 의미상 정당하나 비용이 큼) | PARTIAL |
| `operating process-document` | 콘텐츠 해시 키로 쓰기는 dedupe되나 extractor는 항상 재실행 | PARTIAL |
| `universe sync` | **없음** — 매 실행 provider 전체 재호출. `dart sync-corp`에 이미 있는 "최근 성공 시 skip" 패턴이 적용되어 있지 않음 | **MISSING** |

**더 큰 유지보수 문제는 구현의 비일관성이다.** 같은 개념("이미 가진 것이 무엇인가")에 대해 모듈마다 어휘가 다르다.

| 패턴 | DART 계열 | flows | prices | remote sync |
|---|---|---|---|---|
| 존재 확인 메서드 | `get_existing_*_keys()` → 키 튜플 set | `count_krx_security_flow_*()` → 카운트 dict | `query_missing_days()` | `sync_checkpoints` 체크포인트 |
| force 재수집 플래그 | `--force` 있음 | 없음 | 없음 | `--full-refresh` |

**실측 보강(§1.4):** run 라이프사이클에도 멱등성 공백이 있다. 중단된 run이 `running`으로 영구 잔류(17건)하며, `dart sync-corp`·`universe sync`류의 "직전 성공 run 기반 skip"이 고아 row 때문에 오판할 수 있는 구조적 위험이 있다. **crash-recovery finalizer(시작 시 stale `running` → `failed` 정리)가 필요하다.**

### 2.2 이슈 ② — 수집 대상 기간 (2015 ~ 현재)

> **(2026-06-12 갱신)** 초판에서 최중요 결함으로 지적했던 "DART 2015~2024 부재"는 **2026-06-06~08 다년 백필 완주로 해소**되었다(§1.2). 단, 백필 스크립트의 Cronicle 미등록 등 *재발 방지 장치 부재*는 그대로 유효하다.

| 단계 | CLI로 2015+ 가능? | 프로덕션 DB 실측 (2026-06-12) |
|---|---|---|
| `prices backfill` | 가능 (기본 start 2000-01-01, `cli/app.py:1747`) | ✅ 2007-06-05~2026-06-10 보유. 단 **수동 트리거 운영으로 거래일 2일 지연 중** |
| `flows sync` | 가능 (`--use-price-range`) | ✅ 2007-06-05~2026-06-10 보유 |
| `dart sync-financials` | 가능 (`--bsns-years 2015,…`) | ✅ 2015~2026 보유 (백필 완료). 일배치 래퍼는 여전히 당해+전년만 — 히스토리는 백필 잡 소유로 역할 분리하면 정상 |
| `dart sync-share-info` / `sync-xbrl` | 가능 | ✅ 2015~2025 보유. **2015년 분량 이상 저점 검증 필요**(§1.2) |
| `metrics normalize` | 가능 | ✅ 2015~2025 보유. 2015~2018 fact 수 저점 — raw 가용성/매핑룰 커버리지 확인 필요 |
| `common sync` / `build-daily` | 가능 (`--start/--end` 필수) | ❌ **observation 2024-09-30~, daily fact 2025-11-03~ 뿐. 2015+ 히스토리 백필 미수행** — 현재 유일하게 남은 기간 공백 |

**근본 원인(잔존):**

- "수집 지평선은 2015부터"라는 단일 기준점(source of truth)이 코드 어디에도 없다. 모듈별 기본값이 제각각이다(2000-01-01 / 어제 / 전년 / 기본값 없음).
- `dart-backfill-all-years.sh`는 이번에 수동 1회 실행으로 완주했지만 **여전히 Cronicle에 등록되어 있지 않아**, 신규 연도/누락분 드리프트를 막는 자동 안전망이 없다.
- **(신규)** 일일 파이프라인 자체가 `timing: false`로 수동 운영이라(§1.3), "2015~현재"의 '현재' 쪽 끝이 사람이 잊는 순간부터 벌어진다. 실제로 2거래일 지연이 관측됐다.

**부차 결함:** `docs/holidays_krx.csv`가 **2024~2026년만** 커버한다. 2015~2023 구간에서는 gap detection이 한국 공휴일을 "누락 거래일"로 오인한다. 가격 수집은 pykrx가 휴일에 빈 응답을 주므로 결과적으로 무해하지만, 2015+ 구간의 gap detection / 커버리지 리포트는 누락일을 체계적으로 과대 계상한다.

### 2.3 이슈 ③ — KRX API throttling 및 중앙화

현실은 2계층으로 갈라져 있다.

**(A) KRX MDC 직접 호출 경로 — 잘 중앙화되고 throttle 적용됨.**
단일 공용 클라이언트 `adapters/krx_common/client.py`(`KrxMdcClient`)가 세션·헤더·쿠키 warmup·로그인·730일 요청 청크화를 소유하고, 모든 POST 전후에 `HumanThrottlePolicy`(`util/pipeline.py`)를 적용한다. 기본값(`infra/config/settings.py:127–135`): 요청 간 1.5~4.0초, 15회마다 30~90초 long rest, 인증 후 10초 cooldown, 오류 시 45~180초 backoff. `flows_krx`, `common_features_krx` 둘 다 이 클라이언트를 경유한다.

**(B) pykrx 기반 경로 — 위 장치를 전부 우회.**
pykrx도 동일한 KRX 서버를 때리지만:

| 호출 경로 | throttle 적용 | 실제 지연값 |
|---|---|---|
| `flows_krx` (MDC 직접) | HumanThrottle | 1.5~4.0초 + long rest + backoff |
| `common_features_krx` (MDC 직접) | HumanThrottle | 동일 |
| `prices_pykrx` | 서비스 레벨 sleep만 (`backfill_daily.py:196–217`) | ticker-청크당 0.2초 ± 20% (pykrx 1회 호출이 내부적으로 다수 HTTP 요청 발생) |
| `universe_pykrx` | **전혀 없음** (ticker별 `get_market_ticker_name()` 루프 포함) | 0 |
| `common_features_pykrx` | 서비스 레벨 sleep만 | 기본 0초 |

**(C) 프로세스 간 조율 부재.**
각 Cronicle 잡은 독립적인 `docker compose run`이다. `flows-sync.sh`와 `common-features-refresh.sh`(sync 소스에 `krx` 포함)가 동시에 돌면 서로 무관한 throttle 인스턴스 2개가 KRX MDC를 병행 타격하고, 각 실행이 별도로 로그인한다.

**실측 보강(§1.3~1.4):** 현재는 모든 이벤트가 수동 트리거라 우연히 동시 실행이 드물지만, 06-10 실행처럼 flows 잡이 24시간 이상 끌리는 동안 다른 이벤트를 수동으로 쏘면 곧바로 동시 타격이 된다. 자동 스케줄을 켜는 순간(아래 Phase 0) 이 조율 부재는 잠재 이슈에서 실제 이슈로 격상되므로, 스케줄 활성화와 직렬화(chain/flock)는 **함께** 적용해야 한다.

**판정:** KRX 접근은 단일 게이트웨이로 중앙화되어 있지 **않다**. MDC 경로는 중앙화, pykrx 경로는 산재, 둘 사이 공유 예산은 없음.

---

## 3. 리팩토링 전략

원칙: **"가장 잘 된 모듈의 패턴을 의무화하고, 갈라진 구현을 제거한다."** 4개 워크스트림으로 나눈다.

### 3.1 표준 수집 모듈 계약 (4-phase contract)

모든 ingestion 서비스가 동일한 4단계 템플릿을 따른다. 공용 부분은 run-finalizer가 이미 있는 `util/pipeline.py`로 추출한다.

```
plan     → storage.get_existing_*_keys(...)   작업 목록 결정, 기존분 skip
fetch    → provider.fetch(...)                반드시 throttled gateway 경유
persist  → storage.upsert_*(...)              ON CONFLICT upsert (이미 전 모듈 적용)
finalize → finalize_run(...)                  ingestion_runs 상태/카운터 (이미 전 모듈 적용)
```

구체 조치:

- **Storage 포트의 존재 확인 어휘 단일화** — DART식 `get_existing_*_keys() -> set[tuple]` 명명/반환 규약으로 통일. flows의 `count_*`와 prices의 ad-hoc 쿼리를 이 위로 이주.
- **모든 sync 명령에 균일한 `--force` 플래그** (DART에는 이미 있음).
- **영속 체크포인트 재사용** — `db sync-remote` 전용인 `sync_checkpoints` 테이블을 `prices backfill` 전체 히스토리 실행에도 사용해 중단 후 재개 지원.
- `universe sync`에 `dart sync-corp` 식 "당일 성공 run 존재 시 skip" 가드 추가.
- **(실측 반영) stale-run reaper** — CLI 기동 시(또는 `validate`에서) 일정 시간 초과 `running` run을 `failed`(error_summary="orphaned")로 finalize. 고아 17건 정리 + 재발 방지.

### 3.2 프로세스 전역 throttle을 가진 단일 KRX 게이트웨이

- **프로세스당 `HumanThrottle` 인스턴스 1개** — CLI composition root에서 생성해 모든 KRX-facing 어댑터에 주입. (현재는 `flows_krx`, `common_features_krx`가 각자 생성.)
- **pykrx도 동일 규율로 편입** — `KrxGateway` 포트를 만들고 모든 pykrx 호출 전후에 `throttle.before_request()/after_request()`를 적용. pykrx 내부 HTTP를 가로챌 수는 없으나, 호출 경계에서 동일 정책 값으로 예산을 강제할 수 있다(현행 0.2초/0초 대비). `pykrx_auth.py`의 자격증명 로딩도 게이트웨이로 흡수.
- **프로세스 간 직렬화** — KRX 타격 Cronicle 잡들을 독립 스케줄 대신 체인(universe → prices → flows → common-features)으로 연결하고, 래퍼 스크립트에 호스트 사이드 `flock`(예: `flock /tmp/sdc-krx.lock`)을 가드로 추가. **스케줄 자동화(Phase 0)와 동시 적용 필수.**

### 3.3 단일 수집 지평선 정책

- settings에 `COLLECTION_START_DATE = 2015-01-01`(env 오버라이드 가능)을 **유일한 기준점**으로 추가.
- CLI 기본값을 이 값에서 파생: `dart` 연도 플래그는 `2015 … 전년` 의미론을 기본으로 하거나, "일배치 래퍼는 rolling window를 전달하고 히스토리는 백필 잡이 소유한다"를 명시적으로 문서화. `common sync/build-daily`는 필수 인자 대신 기본값 부여.
- `dart-backfill-all-years.sh`를 Cronicle에 등록(예: 분기 1회 catch-up) — 멱등성 덕에 재실행 비용이 낮으므로, 이번처럼 사람이 기억해서 돌리는 구조를 없애는 안전망이 된다.
- `holidays_krx.csv`를 2015년까지 소급 보강하고, `validate`(또는 `common readiness`)에 테이블별 연도 커버리지 ≥ 2015 단언을 추가해 회귀를 자동 검출.

### 3.4 신규 소스용 보일러플레이트

신규 수집기는 다음 구성을 의무화한다: 포트 Protocol + 공유 게이트웨이/throttle을 주입받는 어댑터 + 4-phase 계약을 따르는 서비스 + `RunType` enum 항목 + start/end를 env에서 받고 기본값을 지평선에서 파생하는 래퍼 스크립트. CLAUDE.md / `docs/architecture.md`에 1페이지로 명문화하고 리뷰에서 강제한다.

---

## 4. 실행 계획 (Actionable Next Steps)

> (2026-06-12 갱신) 초판 Phase 0의 "DART 다년 백필 실행"은 06-08 완주로 **완료 처리**. 실측에서 드러난 운영 이슈로 대체한다.

### Phase 0 — 운영 정상화 (코드 변경 최소, 최고 가치)

1. ~~`bin/dart-backfill-all-years.sh` 실행~~ → **완료** (2026-06-06~08, 2015~2024 전 연도 success).
2. **Cronicle 이벤트 3개에 timing 설정** — 현재 전부 `timing: false`(수동 운영). 평일 장 마감 후 자동 실행으로 전환하고, KRX 타격 잡은 체인으로 직렬화. *(이벤트 수정은 mutating API이므로 사용자 승인 후 진행.)*
3. **flows-sync 장기 실행/실패 원인 조사** — 06-10 실행이 약 24시간 후 exit 1. `--use-price-range` 전 구간 스캔을 일일 잡에서 분리(일일은 최근 N일, 전 구간 보수는 주기 잡으로)하는 방안 포함. 세부 변경 계획: `docs/dev/20260612_module_align_refactor/flows_sync_incremental_plan.md`.
4. **고아 `running` run 17건 정리** + stale-run reaper 도입(§3.1).
5. **common features 히스토리 백필** — 현재 observation 2024-09-30~, daily fact 2025-11-03~ 뿐. `common sync` / `common build-daily`를 `--start 2015-01-01`로 1회 실행 (소스별 제공 범위 한계는 결과로 확인).
6. **DART 2015년 분량 이상 저점 검증** — financial/xbrl 2015가 2016의 약 1/4. OpenDART 자체 한계인지 백필 누락인지 corp 단위 표본 대조.
7. `dart-backfill-all-years.sh`와 common 히스토리 refresh를 Cronicle에 등록(분기 1회 catch-up).

### Phase 1 — 수집 기간 하드닝

8. settings에 `COLLECTION_START_DATE` 도입, CLI 기본값을 여기서 파생, 모듈별 매직 기본값(전년만 / 2000-01-01 / 기본값 없는 필수 인자) 제거.
9. `holidays_krx.csv` 2015년까지 보강, `validate`에 연도 커버리지 단언 추가.

### Phase 2 — KRX 게이트웨이 통합

10. 기존 `HumanThrottlePolicy` 기본값을 사용하는 pykrx용 `KrxGateway` 래퍼 생성. `universe_pykrx`(현재 완전 무-throttle), `prices_pykrx`, `common_features_pykrx`를 경유시킴.
11. `flows_krx`와 `common_features_krx`가 프로세스당 단일 `HumanThrottle` 인스턴스를 공유하도록 변경.
12. KRX 타격 래퍼 스크립트에 `flock` 추가 및/또는 Cronicle 이벤트 체인화 (Phase 0-2와 연동).

### Phase 3 — 멱등성 표준화

13. Storage 포트의 존재 확인 메서드를 단일 명명/반환 규약으로 통일, flows·prices를 이주.
14. `universe sync`에 run-guard skip, 전 명령에 `--force` 플래그, 전체 히스토리 가격 백필에 `sync_checkpoints` 기반 재개 추가.
15. (선택) `metrics normalize` / `common build-daily`에 증분 스코핑(입력이 변한 키/일자만 처리) — 정확성이 아닌 성능 항목.

각 Phase는 독립적으로 배포 가능하며, **Phase 0(특히 스케줄 자동화)만으로도 현재 진행형인 데이터 신선도 문제(거래일 2일 지연)가 해소된다.**

---

## 부록 A: 주요 근거 파일

| 항목 | 위치 |
|---|---|
| Throttle 정책 정의 | `src/krx_collector/util/pipeline.py:20–64` |
| Throttle 기본값 | `src/krx_collector/infra/config/settings.py:127–135` |
| KRX MDC 공용 클라이언트 | `src/krx_collector/adapters/krx_common/client.py` |
| 가격 백필 서비스 레벨 sleep | `src/krx_collector/service/backfill_daily.py:196–217` |
| 가격 백필 클램핑 | `src/krx_collector/service/backfill_daily.py:131–142` |
| DART skip-if-present 예시 | `src/krx_collector/service/sync_dart_financials.py:74–91` |
| CLI 기본값(전체) | `src/krx_collector/cli/app.py` (prices 1747, dart 1171/1208/1240, common 1355+) |
| 프로덕션 래퍼 | `deploy/prod/bin/*.sh` (호스트: `whi@sj2-server:/home/whi/apps/sdc/bin/`) |
| 다년 백필 스크립트 | `bin/dart-backfill-all-years.sh` (Cronicle 미등록, 06-08 수동 완주) |
| 휴일 캘린더(2024~2026만) | `docs/holidays_krx.csv` |
| 선행 검증 문서 | `docs/dev/20260606_data_year_range_verify/data_year_range_verification.md` |

## 부록 B: 실측 명령 (재현용)

```bash
# 프로덕션 DB (sj2-server, 직접 5432 접속 불가 시 SSH 경유)
ssh whi@sj2-server "docker exec sdc-postgres psql -U krx_user -d krx_data -c '<SQL>'"

# 연도 분포
select bsns_year, count(*) from dart_financial_statement_raw group by 1 order by 1;

# 고아 running run
select run_type, started_at, now()-started_at age
  from ingestion_runs where status='running' order by started_at;

# Cronicle 스케줄 (read-only API)
curl -fsS -H "X-API-Key: $APIKEY" 'http://sj2-server:3012/api/app/get_schedule/v1'
curl -fsS -H "X-API-Key: $APIKEY" 'http://sj2-server:3012/api/app/get_event_history/v1?id=<event-id>&limit=5'
```
