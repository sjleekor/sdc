# DART 다년 재무 데이터 백필 실행 계획

- 작성 일시: 2026-06-06
- 선행 분석: [`data_year_range_verification.md`](./data_year_range_verification.md)
- 목적: 위 검증 보고서에서 **미충족**으로 판정된 DART 재무 계열 데이터(2015~2024 결손)를 수집하여, 재무 모달리티의 연도 범위를 시세 범위(2014+)에 맞춘다.

---

## 1. 배경 (검증 결과 요약)

검증 보고서 §3·§4 결론을 그대로 인용한다.

| 모달리티 | 실제 DB 보유 기간 | 충족 여부 |
|---|---|---|
| 시세 `daily_ohlcv` | 2007~2026 (2014+ 조밀) | ✅ 충족 |
| 플로우 `krx_security_flow_raw` | 2007~2026 | ✅ 충족 |
| DART 재무 `dart_financial_statement_raw` | **2025, 2026** | ❌ 미충족 |
| DART XBRL `dart_xbrl_document` / `dart_xbrl_fact_raw` | **2025** | ❌ 미충족 |
| DART 지분 `dart_share_count_raw` / `dart_shareholder_return_raw` | **2025** | ❌ 미충족 |
| 정규화 지표 `stock_metric_fact` | **2025** | ❌ 미충족 |

**직접 원인**: 일배치 cronicle 이벤트(`sdc_daily_accounts_flows`)의 DART 래퍼들이 기본값상 최근 연도만 수집하도록 구성되어 있고(정확히는 **`dart-sync-financials.sh` 만 올해+작년 2개 연도**, 그 외 `dart-sync-share-info.sh` / `dart-sync-xbrl.sh` / `metrics-normalize.sh` 는 **작년 1개 연도만**), 다년 백필 스크립트 `bin/dart-backfill-all-years.sh`(기본 `start_year=2015`~작년)가 **cronicle 어느 이벤트에도 등록되어 있지 않다.**

따라서 일배치 구성을 바꾸기보다는, **이미 존재하는 백필 스크립트를 1회성으로 실행**해 과거 연도를 채우는 것이 가장 적절하다(과거 사업연도 공시는 확정되면 거의 불변이므로 매일 재수집할 필요가 없다).

---

## 2. 목표 (Definition of Done)

1. `dart_financial_statement_raw`, `dart_xbrl_document`, `dart_xbrl_fact_raw`, `dart_share_count_raw`, `dart_shareholder_return_raw` 의 `bsns_year` 분포가 **2015~2024** 전 연도를 포함한다(기존 2025/2026 유지).
2. `stock_metric_fact` 의 `bsns_year` 가 동일하게 **2015~2024** 로 확장된다.
3. 모든 백필은 **sj2-server(`krx_data`)에서 실행**하고, 완료 후 local(`mydb`)로 수작업 동기화하여 두 DB 수치를 다시 일치시킨다.
4. 백필 과정에서 기존 2025/2026 데이터가 손상/삭제되지 않는다(전 명령이 `INSERT … ON CONFLICT DO UPDATE` upsert).

> reprt 코드 범위는 일배치와 동일하게 `11011`(사업보고서), `11012`(반기), `11013`(1분기), `11014`(3분기) 4종, `fs_divs=CFS,OFS` 로 맞춘다.

---

## 3. 실행 도구

`deploy/prod/bin/dart-backfill-all-years.sh` 가 이미 목표 작업을 정확히 수행한다. 동작 요약:

- 환경변수로 범위/옵션 제어
  - `SDC_DART_BACKFILL_START_YEAR` (기본 `2015`)
  - `SDC_DART_BACKFILL_END_YEAR` (기본 `올해-1`)
  - `SDC_DART_BACKFILL_INCLUDE_CURRENT_YEAR=1` (올해 포함)
  - `SDC_DART_BACKFILL_REPRT_CODES` (기본 `11011,11012,11013,11014`)
  - `SDC_DART_BACKFILL_FS_DIVS` (기본 `CFS,OFS`)
  - `SDC_DART_BACKFILL_PULL_IMAGE` (기본 `1`)
- `dart sync-corp` 1회 실행 후, `end_year` → `start_year` 역순으로 연도별 루프:
  `dart sync-financials` → `dart sync-share-info` → `dart sync-xbrl` → `metrics normalize` (각 연도, 4개 reprt 코드).

즉 **별도 코드/스크립트 신규 작성 불필요.** 기존 스크립트에 연도 범위만 지정하여 실행하면 된다.

---

## 4. 사전 점검 (Pre-flight)

`sj2-server` / `sdc-db` 스킬을 사용해 다음을 확인한다.

1. **OpenDART API 키 한도/개수 확인**
   - OpenDART 는 키당 일일 요청 한도가 있으며, 본 프로젝트 운영 키는 **2026-06-06 확인 시점 기준 일일 40,000건**이다(키 등급/승인 조건에 따라 다르므로 백필 직전 OpenDART 마이페이지에서 재확인 권장). 한도는 **KST 자정(00:00) 기준으로 리셋**된다.
   - 백필 규모는 `(연도 10) × (reprt 4) × (corp ≈ 2,100)` 수준으로 financials 만 수만~수십만 요청에 달해 **단일 키 하루 한도(40,000)를 초과**할 가능성이 높다.
   - 프로덕션 `.env` 의 `OPENDART_API_KEYS`(복수 키, 콤마구분) 설정 여부를 확인한다. 코드 측 동작은 코드 레벨에서 직접 확인됨:
     - `src/krx_collector/cli/app.py` 의 `_handle_dart_sync_financials` / `_handle_dart_sync_share_info` / `_handle_dart_sync_xbrl` 세 핸들러가 **모두 동일한 `OpenDartRequestExecutor`(`adapters/opendart_common/client.py`)를 사용**하고, 종료 직전 **모두 `_exit_if_opendart_key_exhausted` 를 호출**한다.
     - 따라서 다중 키 로테이션(`OPENDART_API_KEYS` 콤마구분 다중 키) + 모든 키 소진 시 비정상 종료 동작은 financials/share-info/xbrl 세 명령에 **동일하게 적용**됨이 확인되었다(배경: `docs/dev/260422_multiple_opendart_api_key`, `260425_stop_after_all_opendart_key_consumed`).
   - 키가 부족하면 **연도를 나눠 여러 날에 걸쳐** 실행하는 분할 전략을 사용한다(§5.2).

2. **현재 보유 연도 재확인** (백필 전 베이스라인 스냅샷)
   ```bash
   .agents/skills/sdc-db/scripts/dbq.sh sj2 \
     "select bsns_year, count(*) from dart_financial_statement_raw group by bsns_year order by bsns_year;"
   ```
   다른 DART 테이블(`dart_xbrl_document`, `dart_share_count_raw`, `dart_shareholder_return_raw`, `stock_metric_fact`)도 동일하게 스냅샷.

3. **디스크 여유 확인**: `dart_xbrl_fact_raw` 가 2025 단일 연도에 약 1,870만 행이므로 10개 연도 추가 시 단순 선형 환산으로 수억 행·수십 GB 증가가 예상된다. **단, 이는 보수적 상한 추정**이며, 2015~2018 등 과거 연도는 (a) 상장 종목 수가 더 적고 (b) XBRL 의무 적용 범위가 좁아 실제 증가량은 이보다 작을 가능성이 높다(절대값으로 받아들이지 말 것). 그럼에도 충분한 여유를 확보하기 위해 sj2-server 의 볼륨 여유 공간을 점검한다.
   ```bash
   ssh whi@sj2-server 'df -h /home/whi/apps/sdc && docker system df'
   ```

---

## 5. 실행 절차

> 모든 명령은 sj2-server 의 프로덕션 디렉터리(`/home/whi/apps/sdc`)에서 실행한다. 장시간(수 시간~수 일) 작업이므로 `tmux`/`nohup` 등 세션 분리 사용을 권장한다.

### 5.1 전체 일괄 실행 (키 한도가 충분한 경우)

```bash
ssh whi@sj2-server '
  cd /home/whi/apps/sdc &&
  SDC_DART_BACKFILL_START_YEAR=2015 \
  SDC_DART_BACKFILL_END_YEAR=2024 \
  nohup bash bin/dart-backfill-all-years.sh > /home/whi/apps/sdc/backfill_2015_2024.log 2>&1 &
'
```

- 2025/2026 은 일배치가 계속 채우므로 백필 범위는 `2015~2024` 로 한정한다.
- 진행 로그는 `backfill_2015_2024.log` 에서 `tail -f` 로 모니터링.

### 5.2 연도 분할 실행 (키 한도가 빠듯한 경우, 권장)

키 소진으로 중단되면 해당 연도부터 재개하면 되므로(upsert 라 재실행 안전), 2~3개 연도씩 끊어 실행한다. 예:

```bash
# 1일차
SDC_DART_BACKFILL_START_YEAR=2022 SDC_DART_BACKFILL_END_YEAR=2024 bash bin/dart-backfill-all-years.sh
# 2일차
SDC_DART_BACKFILL_START_YEAR=2019 SDC_DART_BACKFILL_END_YEAR=2021 bash bin/dart-backfill-all-years.sh
# 3일차
SDC_DART_BACKFILL_START_YEAR=2015 SDC_DART_BACKFILL_END_YEAR=2018 bash bin/dart-backfill-all-years.sh
```

- **OpenDART 키 한도는 KST 자정(00:00) 기준으로 리셋**된다. "1일차/2일차/3일차"는 KST 달력 기준 별개 일자를 의미하며, 각 회차는 가능한 한 **KST 00:00 직후에 시작**해야 그날 한도(키당 40,000건)를 온전히 사용할 수 있다. 자정 직전에 시작하면 곧 리셋되어 분할 의미가 사라진다.
- 스크립트는 `dart sync-corp` 를 매 실행 1회 수행하므로 중복 실행해도 무해(corp 마스터 upsert).
- 키 소진으로 비정상 종료 시, 종료 직전 로그의 "Backfilling … for <year>" 를 확인해 **그 연도부터** 다시 실행한다(다음 KST 일자에).

### 5.3 (선택) cronicle 1회성 이벤트로 실행
SSH 세션 안정성이 우려되면 cronicle 에 비반복(`timing: false`) 이벤트를 한시적으로 만들어 백필 스크립트를 실행할 수도 있다. 단,
- 이벤트 생성/실행은 **사용자 명시 승인이 필요한 mutating 작업**이다(`sj2-server` 스킬 규칙).
- `sdc_daily_accounts_flows` 와 동시 실행 시 DART 테이블에 동일 키 upsert 경합이 가능하나 데이터 정합성은 보장된다(충돌 시 갱신). 다만 OpenDART 키 한도를 양쪽이 공유하므로 **시간대를 겹치지 않게** 한다.

---

## 6. 정규화(metrics normalize) 확인

`dart-backfill-all-years.sh` 는 연도 루프 안에서 `metrics normalize --bsns-years <year> --reprt-codes 11011,11012,11013,11014` 를 이미 호출한다. 따라서 별도 정규화 단계는 원칙적으로 불필요하다.

다만 백필이 중간에 끊겨 일부 연도의 normalize 만 누락된 경우, 누락 연도에 대해 수동 재실행한다:

```bash
ssh whi@sj2-server 'cd /home/whi/apps/sdc &&
  docker compose run --rm collector metrics normalize \
    --bsns-years 2015,2016,2017,2018,2019,2020,2021,2022,2023,2024 \
    --reprt-codes 11011,11012,11013,11014'
```

---

## 7. 검증 (Post-backfill)

백필 완료 후 §4 사전 스냅샷과 비교하여 목표 달성 여부를 확인한다.

```bash
# sj2 에서 연도 분포가 2015~2024 를 포함하는지 확인
.agents/skills/sdc-db/scripts/dbq.sh sj2 "
  select bsns_year, count(*), count(distinct corp_code)
    from dart_financial_statement_raw group by bsns_year order by bsns_year;"
.agents/skills/sdc-db/scripts/dbq.sh sj2 "select bsns_year, count(*) from dart_xbrl_document        group by bsns_year order by bsns_year;"
.agents/skills/sdc-db/scripts/dbq.sh sj2 "select bsns_year, count(*) from dart_xbrl_fact_raw         group by bsns_year order by bsns_year;"
.agents/skills/sdc-db/scripts/dbq.sh sj2 "select bsns_year, count(*) from dart_share_count_raw       group by bsns_year order by bsns_year;"
.agents/skills/sdc-db/scripts/dbq.sh sj2 "select bsns_year, count(*) from dart_shareholder_return_raw group by bsns_year order by bsns_year;"
.agents/skills/sdc-db/scripts/dbq.sh sj2 "select bsns_year, count(*) from stock_metric_fact          group by bsns_year order by bsns_year;"
```

합격 기준:
- 위 6개 테이블 모두 `bsns_year` 에 2015~2024 가 빠짐없이 존재.
- `stock_metric_fact` 연도 범위가 원천 테이블과 일치.
- 기존 2025/2026 행수가 백필 전 대비 감소하지 않음(유지 또는 증가).

---

## 8. local(`mydb`) 동기화

검증이 끝나면 sj2-server → local 동기화를 수행해 두 DB 수치를 다시 일치시킨다. 본 프로젝트의 표준 동기화 명령은 다음과 같다(로컬에서 실행).

```bash
uv run krx-collector db sync-remote --ssh-host whi@sj2-server
```

동기화 후 §7 쿼리를 `local` 타겟으로도 실행해 일치를 확인한다.

```bash
.agents/skills/sdc-db/scripts/dbq.sh local "select bsns_year, count(*) from dart_financial_statement_raw group by bsns_year order by bsns_year;"
```

---

## 9. 리스크 및 유의사항

| 항목 | 내용 | 대응 |
|---|---|---|
| OpenDART 일일 키 한도 | 백필 요청량이 단일 키 한도 초과 가능 | 다중 키(`OPENDART_API_KEYS`) 구성, §5.2 연도 분할 |
| 장시간 실행 | 수 시간~수 일 소요 | `nohup`/`tmux`, 로그 모니터링, 연도 분할 |
| 디스크 증가 | `dart_xbrl_fact_raw` 수억 행 증가 | §4-3 사전 용량 점검 |
| 일배치와 경합 | 백필이 `sdc_daily_accounts_flows` 와 동시 실행 | upsert 라 데이터는 안전, 단 키 한도 공유 → 시간대 분리 |
| 과거 공시 정정 | 2015~2024 도 드물게 정정 공시 존재 | 1회 백필로 충분, 필요 시 특정 연도만 재실행 |

**롤백**: 모든 명령이 upsert/append 라 별도 롤백 절차는 불필요하다. 문제가 생기면 해당 연도 명령을 재실행하면 된다.

---

## 10. 후속(자동화 갭) 검토 — 선택

본 백필은 1회성 보완이다. 향후 **신규 사업연도가 시작될 때마다** 과거 누락이 재발하지 않도록, 일배치(`dart-sync-financials.sh` 등)가 항상 최근 1~2개 연도만 수집하는 현 구조를 유지하되:

- (권장) 본 백필 스크립트를 **분기/연 1회 cron** 으로 등록하는 방안 검토(예: `SDC_DART_BACKFILL_START_YEAR` 를 직전 2~3년으로 좁혀 정정 공시 반영).
- 단, 이 변경은 운영 스케줄 변경이므로 별도 이슈로 분리하고 사용자 승인 후 `deploy/prod/` 수정 → `deploy/deploy_to_sj2.sh` 절차로 반영한다(이 문서 범위 밖).

---

## 11. 실행 로그 (2026-06-06 실제 실행)

### 11.1 사전 점검 결과
- **베이스라인(백필 전, sj2 `krx_data`)**: 6개 DART 테이블 모두 `bsns_year` 가 2025 중심(+ financials 만 2026 추가). 즉 검증 보고서와 동일.
  - `dart_financial_statement_raw`: 2025=1,016,063 / 2026=238,612
  - `dart_xbrl_document`: 2025=8,255 · `dart_xbrl_fact_raw`: 2025=18,696,562
  - `dart_share_count_raw`: 2025=10,295 · `dart_shareholder_return_raw`: 2025=263,030
  - `stock_metric_fact`: 2025=34,411
- **디스크**: sj2-server `/` 915G 중 가용 **521G(41% 사용)** → 백필 증가분 수용에 충분.
- **OpenDART 키**: 프로덕션 `.env` 의 `OPENDART_API_KEYS` 에 **9개** + 레거시 `OPENDART_API_KEY` 1개 구성. 코드(`settings.py`)가 두 변수를 합쳐 사용. **9개 키 × 40,000 = 일 36만 요청** capacity로, 전체 2015~2024 백필을 단일 일자 한도 내에서 처리 가능 → **§5.1 전체 일괄 실행 채택**(연도 분할 불요).

### 11.2 기동
- 실행: §5.1 그대로, `SDC_DART_BACKFILL_START_YEAR=2015 SDC_DART_BACKFILL_END_YEAR=2024` 로 `setsid nohup` 백그라운드 기동(SSH 세션 종료 후에도 지속).
- 시작 시각: **2026-06-06 10:04 KST**, 로그: `/home/whi/apps/sdc/backfill_2015_2024.log`.
- 초기 동작 확인:
  - `dart sync-corp` → 최근(2026-05-23) 동기화 존재로 **skip**(정상, `--force` 미사용).
  - `end_year=2024` 부터 역순 루프 시작, financials 수집 개시. 9개 키(`key#1`~`key#9`) 라운드로빈, 약 **3 req/s**.
  - DB 실적재 확인: 기동 직후 `dart_financial_statement_raw` 에 `bsns_year=2024` 행이 생성되며 증가(예: 28,662행 / corp 29개 시점 스냅샷).

### 11.3 진행 상태 (본 문서 작성 시점)
- 백필은 **진행 중**이며 2024 연도부터 2015 까지 순차 수집한다. financials/share-info/xbrl 각 연도별로 약 1.6만~수만 요청이 필요해 **전체 완료까지 수십 시간(다일) 소요** 예상. 본 작업 세션 범위에서 완료를 보장하지 않으며, 분리된 백그라운드 프로세스가 계속 수행한다.

### 11.4 모니터링 명령
```bash
# 진행 로그 실시간 확인
ssh whi@sj2-server 'tail -f /home/whi/apps/sdc/backfill_2015_2024.log'

# 프로세스 생존 확인
ssh whi@sj2-server 'pgrep -af dart-backfill-all-years || echo "(finished or stopped)"'

# 연도별 적재 진척 확인 (반복 실행)
.agents/skills/sdc-db/scripts/dbq.sh sj2 "select bsns_year, count(*) from dart_financial_statement_raw group by bsns_year order by bsns_year;"
```

- 키 소진(`_exit_if_opendart_key_exhausted`)으로 비정상 종료되면, 로그의 마지막 "Backfilling … for <year>" 연도를 확인해 **다음 KST 일자에 그 연도부터** `SDC_DART_BACKFILL_START_YEAR=2015 SDC_DART_BACKFILL_END_YEAR=<중단연도>` 로 재실행한다(upsert 라 재실행 안전).

### 11.5 완료 후 할 일
- 완료(프로세스 종료) 확인 후 **§7 검증 쿼리**로 6개 테이블의 `bsns_year` 가 2015~2024 를 빠짐없이 포함하는지 확인.
- **§8** `uv run krx-collector db sync-remote --ssh-host whi@sj2-server` 로 local 동기화 후 `local` 타겟으로 동일 검증.

---

## 12. 백그라운드 실행 운영 가이드 (진행/완료 확인 · 중단 후 수동 재실행)

§5.1 처럼 `setsid nohup ... &` 로 기동한 백필은 SSH 세션과 무관하게 sj2-server 에서 독립적으로 계속 실행된다. 따라서 진행 여부·완료 여부는 매번 **(a) 프로세스 생존**, **(b) 로그 내용**, **(c) DB 적재량** 세 가지로 확인한다.

### 12.1 진행 중인지 / 완료되었는지 확인하는 방법

#### (a) 프로세스 생존 여부 — 가장 빠른 판별
```bash
ssh whi@sj2-server 'pgrep -af dart-backfill-all-years | grep -v pgrep || echo "(stopped/finished)"'
```
- **출력에 프로세스 라인이 보이면 → 아직 진행 중.**
- `(stopped/finished)` 만 출력되면 → **종료됨**(정상 완료 또는 중단). 정상 완료인지 키 소진/오류 중단인지는 (b) 로그로 구분한다.
- 실제로 OpenDART 요청을 수행하는 컨테이너까지 확인하려면:
  ```bash
  ssh whi@sj2-server 'docker ps --format "{{.Names}} {{.Status}}" | grep -i collector || echo "(no collector container running)"'
  ```
  진행 중이면 `sdc-collector-run...` 컨테이너가 떠 있고, 완료/중단 시 사라진다.

#### (b) 로그로 진행 단계·완료/중단 사유 확인
```bash
# 최근 로그 30줄 (스냅샷)
ssh whi@sj2-server 'tail -n 30 /home/whi/apps/sdc/backfill_2015_2024.log'

# 실시간 추적 (Ctrl-C 로 빠져나와도 백필 프로세스에는 영향 없음)
ssh whi@sj2-server 'tail -f /home/whi/apps/sdc/backfill_2015_2024.log'
```
로그에서 확인할 것:
- 현재 처리 중인 연도: `Backfilling ... for <year>` 라인. 백필은 `2024 → 2015` **역순**이므로, 이 연도가 작을수록 끝에 가깝다.
- **정상 완료 신호**: 마지막 연도(`2015`)까지 `metrics normalize` 가 끝나고 스크립트가 에러 없이 종료된 흔적(쉘 프롬프트 복귀 / 마지막 연도 루프 종료 메시지). 프로세스가 (a)에서 사라졌고 로그 끝에 오류가 없으면 정상 완료로 간주한다.
- **키 소진/오류 중단 신호**: 로그 끝부분에 `_exit_if_opendart_key_exhausted` 관련 종료 메시지, OpenDART 에러, 또는 마지막 `Backfilling ... for <year>` 가 2015 보다 큰 연도에서 멈춰 있으면 → **중단**으로 판단하고 §12.2 로 재개한다.

#### (c) DB 적재량으로 실제 진척/완료 교차검증
프로세스/로그와 별개로, 실제 데이터가 어디까지 채워졌는지가 최종 진실이다.
```bash
.agents/skills/sdc-db/scripts/dbq.sh sj2 "select bsns_year, count(*), count(distinct corp_code) from dart_financial_statement_raw group by bsns_year order by bsns_year;"
```
- 반복 실행 시 진행 중인 연도의 `count` 가 계속 증가하면 → **정상 진행 중**.
- **완료 판정**: 위 쿼리 결과의 `bsns_year` 에 2015~2024 가 빠짐없이 등장하고, §7 의 나머지 5개 테이블 쿼리도 모두 2015~2024 를 포함하면 → 백필이 사실상 완료된 것. 이 상태가 되면 §11.5 / §7 / §8 절차로 넘어간다.

> 요약 판단표
>
> | (a) 프로세스 | (b) 로그 마지막 연도 | (c) DB 연도 분포 | 상태 |
> |---|---|---|---|
> | 살아있음 | 2024~2016 진행 | 해당 연도 count 증가 중 | **진행 중** |
> | 없음 | 2015 까지 도달·에러 없음 | 2015~2024 모두 존재 | **정상 완료** |
> | 없음 | 2015 이전 연도에서 멈춤·키소진/에러 | 2015~2024 일부 결손 | **중단됨 → §12.2 재실행** |

### 12.2 중단된 백필을 수동으로 다시 실행하는 방법

백필 명령은 전부 `INSERT ... ON CONFLICT DO UPDATE` (upsert) 라 **이미 채워진 연도를 다시 돌려도 안전**(중복/손상 없음)하다. 따라서 재실행 시 “이미 끝난 연도를 다시 포함해도” 문제가 없다.

**1단계 — 중단 지점(재개 시작 연도) 파악**
```bash
# 로그에서 마지막으로 처리하던 연도 확인
ssh whi@sj2-server 'grep -E "Backfilling .* for [0-9]{4}" /home/whi/apps/sdc/backfill_2015_2024.log | tail -n 3'

# DB 에서 아직 비어 있는(또는 부족한) 연도 확인
.agents/skills/sdc-db/scripts/dbq.sh sj2 "select bsns_year, count(*) from dart_financial_statement_raw group by bsns_year order by bsns_year;"
```
백필은 `END_YEAR → START_YEAR` 역순이므로, **로그가 멈춘 연도(가장 작은, 아직 미완 연도)** 가 재개 상한이 된다. 예) 2024~2019 까지 채워지고 2018 에서 중단 → 남은 범위는 `2015~2018`.

**2단계 — 중복 실행 방지(혹시 살아있는지 재확인)**
```bash
ssh whi@sj2-server 'pgrep -af dart-backfill-all-years | grep -v pgrep || echo "(none running — safe to start)"'
```
프로세스가 남아 있으면 새로 띄우지 말 것(키 한도를 동시에 소모하고 로그가 섞인다). 정말 중단해야 한다면 §12.3 으로 먼저 정지한다.

**3단계 — 남은 연도 범위로 수동 재실행**
키 한도가 충분하면 남은 전 범위를 한 번에:
```bash
ssh whi@sj2-server '
  cd /home/whi/apps/sdc &&
  SDC_DART_BACKFILL_START_YEAR=2015 \
  SDC_DART_BACKFILL_END_YEAR=2018 \
  setsid nohup bash bin/dart-backfill-all-years.sh \
    > /home/whi/apps/sdc/backfill_2015_2018_resume.log 2>&1 < /dev/null &
  disown
'
```
- `SDC_DART_BACKFILL_END_YEAR` 를 **중단 연도**로, `START_YEAR` 를 최종 목표 시작연도(`2015`)로 지정한다.
- 키 한도가 빠듯하면 §5.2 처럼 2~3개 연도씩 끊고, **각 회차는 KST 자정 직후**에 시작한다(키 한도는 KST 00:00 리셋).
- 로그 파일명은 회차별로 다르게(`..._resume.log`) 두면 §11 의 원본 로그와 섞이지 않는다.
- 재개 직후 §12.1 (a)·(b)·(c) 로 정상 기동을 확인한다.

**4단계 — 누락 연도 normalize 보강(필요 시)**
재실행에서 financials/xbrl/share-info 만 채워지고 일부 연도 `metrics normalize` 가 빠졌다면 §6 의 수동 normalize 명령을 누락 연도에 대해 실행한다.

### 12.3 (필요 시) 진행 중인 백필을 의도적으로 중단하는 방법

> 중단은 데이터 손상을 일으키지 않는다(upsert·연도 단위 진행). 단, 진행 중이던 연도는 부분 적재로 남을 수 있으므로 재개 시 그 연도부터 다시 포함하면 된다.

```bash
# 프로세스 그룹 종료 (setsid 로 띄웠으므로 그룹 단위로 정리)
ssh whi@sj2-server '
  pid=$(pgrep -f dart-backfill-all-years | head -1);
  if [ -n "$pid" ]; then
    pgid=$(ps -o pgid= -p "$pid" | tr -d " ");
    echo "killing pgid=$pgid";
    kill -TERM -"$pgid" 2>/dev/null || kill -TERM "$pid";
  else
    echo "(no backfill process running)";
  fi
'
# 진행 중이던 collector 컨테이너가 남아 있으면 함께 정리(있을 때만)
ssh whi@sj2-server 'docker ps --filter "name=sdc-collector-run" --format "{{.ID}}" | xargs -r docker stop'
```
중단 후에는 §12.1 (a) 로 종료를 확인하고, 재개가 필요하면 §12.2 절차를 따른다.

---

## 13. 재기동 로그 (2026-06-06)

### 13.1 재기동 전 상태
- 확인 시각: **2026-06-06 19:03 KST**
- sj2-server 재시작 이후 `dart-backfill-all-years` 프로세스와 `sdc-collector-run-*` 컨테이너는 모두 없음.
- `sdc-postgres` 는 `healthy` 상태로 재기동 완료.
- 원본 로그(`/home/whi/apps/sdc/backfill_2015_2024.log`)는 **2024 financials 수집 중 10:28 KST 부근**에서 중단.
- 재개 전 sj2 `dart_financial_statement_raw` 분포:
  - 2024=410,560 rows / 455 corps
  - 2025=1,016,063 rows / 2,141 corps
  - 2026=238,612 rows / 2,079 corps

### 13.2 재기동
- 재기동 범위: `SDC_DART_BACKFILL_START_YEAR=2015`, `SDC_DART_BACKFILL_END_YEAR=2024`
- 실행 로그: `/home/whi/apps/sdc/backfill_2015_2024_resume_20260606.log`
- 시작 시각: **2026-06-06 19:03 KST**
- 실행 명령:
  ```bash
  ssh whi@sj2-server 'cd /home/whi/apps/sdc &&
    SDC_DART_BACKFILL_START_YEAR=2015 \
    SDC_DART_BACKFILL_END_YEAR=2024 \
    setsid nohup bash bin/dart-backfill-all-years.sh \
      > /home/whi/apps/sdc/backfill_2015_2024_resume_20260606.log 2>&1 < /dev/null &
    disown'
  ```

### 13.3 초기 확인
- `dart-backfill-all-years.sh` 프로세스 기동 확인.
- `sdc-collector-run-*` 컨테이너 기동 확인.
- 새 로그에서 `dart sync-corp` 는 기존 성공 이력으로 skip 되었고, **2024 financials** 부터 재처리 시작.
- 재실행 초반은 이미 부분 적재된 2024 구간을 upsert 로 다시 지나가므로, DB 총 row 수가 즉시 증가하지 않을 수 있다.
