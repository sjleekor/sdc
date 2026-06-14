# Source wrapper throttling 배포 실행 계획

- 작성일: 2026-06-14 KST
- 상태: 완료
- 완료일: 2026-06-14 KST
- 최종 배포 버전: `v0.8.12`
- 범위: 목표 1(DB 최신 지점 기반 증분 수집)과 목표 2(source별 wrapper/lock/throttle 분리) 구현분의 운영 배포
- 관련 계획:
  - `docs/dev/20260613_refactoring/incremental_collection_plan.md`
  - `docs/dev/20260613_refactoring/source_wrapper_throttling_plan.md`
- 배포 원칙: 코드/이미지/wrapper 배포와 Cronicle event 전환을 분리한다.

## 1. 현재 전제

- `investor` / `shorting` catch-up run은 종료됐다.
- run `b1adfe71-7ee1-4210-8770-a3985c3750dd`는 `partial`로 끝났지만, 남은 에러는 `204210`의 KRX ISIN finder 실패 2건이다.
- 사용자가 이 에러는 무시하기로 결정했다.
- 따라서 배포 전 KRX 관련 작업 보류 조건은 해제된 상태로 본다.

## 2. 배포 단위

이번 배포는 다음을 한 patch release로 묶는다.

1. 증분 수집 정렬
   - `flows sync --incremental`
   - DART target planner / incremental guard
   - `metrics normalize --incremental`
   - common sync/build incremental
2. 운영 wrapper 재배치
   - `deploy/prod/bin/lib/sdc-wrapper.sh`
   - source별 lock/throttle 적용
   - common feature source별 wrapper 추가
   - `common-features-refresh.sh`는 수동 호환 orchestration wrapper로 유지
3. freshness guard
   - `ops freshness-report`
   - `ops assert-common-freshness`
   - `common-build-daily.sh`의 stale raw build 방지
4. 운영 문서
   - `docs/deploy.md`
   - 관련 dev plan 문서

## 3. 로컬 검증

배포 전 아래 검증을 모두 통과시킨다.

주의: `.agents/skills/sdc-release/scripts/release.py`의 dry-run은 실제로 `ruff`, `pytest`, `uv lock`, `git status`를 실행하지 않고 실행할 명령만 출력한다. 따라서 검증 실패 여부는 이 섹션의 명령으로 먼저 확인한다.

```bash
bash -n deploy/prod/bin/*.sh deploy/prod/bin/lib/*.sh
```

```bash
uv run python -c "from krx_collector.cli.app import build_parser; p=build_parser(); p.parse_args(['flows','sync','--incremental']); p.parse_args(['common','sync','--incremental','--sources','krx']); p.parse_args(['common','sync','--incremental','--sources','pykrx']); p.parse_args(['dart','sync-financials','--incremental']); p.parse_args(['metrics','normalize','--incremental']); p.parse_args(['ops','assert-common-freshness','--sources','fdr,fred,ecos,krx']); print('parser ok')"
```

```bash
tests/shell/sdc-wrapper-smoke.sh
```

```bash
uv run ruff check src tests
uv run pytest
```

## 4. 릴리스 준비

현재 기준 버전은 `0.8.10`이므로 다음 patch release는 `v0.8.11`이다.

중요한 주의점:

- `./deploy/deploy_to_sj2.sh`는 `deploy/prod/compose.yaml`과 `deploy/prod/bin/`을 sj2-server로 동기화한다.
- 따라서 remote `compose.yaml`만 릴리스 helper로 고치고 이후 `deploy_to_sj2.sh`를 실행하면, remote image tag가 로컬 `deploy/prod/compose.yaml` 값으로 되돌아갈 수 있다.
- 이번 배포에서는 `deploy/prod/compose.yaml`의 collector image tag도 새 버전으로 커밋한다.

권장 절차:

```bash
git status --short
```

`deploy/prod/compose.yaml`의 collector image를 새 tag로 맞춘다.

```yaml
image: ghcr.io/sjleekor/sdc:v0.8.11
```

릴리스 dry-run:

```bash
uv run python .agents/skills/sdc-release/scripts/release.py --bump patch --stage-all
```

dry-run은 release helper가 어떤 명령을 실행할지 미리 출력하는 용도다. 예상하지 못한 stage 대상, 기존 tag 충돌, detached HEAD, 잘못된 버전 계산이 보이면 중단한다. 실제 검증 실패는 앞선 로컬 검증 또는 아래 `--apply` 실행에서 확인한다.

릴리스 적용:

```bash
uv run python .agents/skills/sdc-release/scripts/release.py --bump patch --stage-all --apply
```

이 명령은 다음을 수행한다.

- `pyproject.toml` 버전 bump
- `uv lock`
- 검증 실행
- release commit 생성
- `v0.8.11` tag 생성
- branch와 tag push

## 5. 이미지 publish 확인

tag push 후 GitHub Actions가 GHCR 이미지를 publish했는지 확인한다.

확인 대상:

- `ghcr.io/sjleekor/sdc:v0.8.11`
- GitHub Actions docker workflow 성공 여부

예시:

```bash
gh run list --workflow docker.yml --limit 5
```

필요하면 release tag 기준 workflow 상세를 확인한다.

## 6. sj2-server 코드/wrapper 배포

이미지가 publish된 뒤 sj2-server에 compose와 wrapper를 동기화한다.

```bash
./deploy/deploy_to_sj2.sh
```

이 스크립트는 다음만 수행한다.

- `/home/whi/apps/sdc/compose.yaml` 동기화
- `/home/whi/apps/sdc/bin/` 동기화

컨테이너 pull/restart는 별도로 수행한다.

```bash
ssh whi@sj2-server 'cd /home/whi/apps/sdc && docker compose config >/dev/null && docker compose pull collector'
```

필요 시 collector image tag를 확인한다.

```bash
ssh whi@sj2-server 'grep -nE "^[[:space:]]*image:" /home/whi/apps/sdc/compose.yaml'
```

## 7. 운영 smoke

Cronicle event를 전환하기 전, sj2-server에서 새 wrapper와 새 이미지가 기본 동작하는지 확인한다.

Shell syntax:

```bash
ssh whi@sj2-server 'cd /home/whi/apps/sdc && bash -n bin/*.sh bin/lib/*.sh'
```

Freshness report:

```bash
ssh whi@sj2-server 'cd /home/whi/apps/sdc && docker compose run --rm collector ops freshness-report'
```

Common build guard:

```bash
ssh whi@sj2-server 'cd /home/whi/apps/sdc && docker compose run --rm collector ops assert-common-freshness --sources fdr,fred,ecos,krx'
```

주의:

- source wrapper smoke는 외부 API를 칠 수 있다.
- Cronicle 전환 직전에 필요한 최소 범위로만 수행한다.
- `sdc_daily_pykrx_common`은 optional이므로 기본 smoke/필수 source에는 넣지 않는다.

## 8. Cronicle 전환

이 단계는 Cronicle mutating API를 사용하므로 별도 사용자 승인 후 진행한다.

현재 Cronicle에는 기존 세 event만 있고 신규 source별 event는 아직 없다. 세 기존 event는 `enabled=1`, `timing=false`, `chain=""`인 수동 실행 event다. 전환은 신규 event를 먼저 `timing=false`로 등록하고, wrapper smoke 후 기존 event를 비활성화한 다음 root event timing을 켜는 순서로 진행한다.

전환 순서:

1. 기존 event 상태 확인
   - `sdc_daily_pipeline`
   - `sdc_daily_accounts_flows`
   - `sdc_daily_common_features`
2. 신규 source별 event를 `timing=false` 또는 disabled 상태로 먼저 등록
3. 신규 event command가 `/home/whi/apps/sdc/bin/<wrapper>.sh`를 직접 호출하는지 확인
4. 신규 event smoke를 제한적으로 실행
5. 기존 세 event를 비활성화
6. 신규 event chain/timing을 활성화

기존 event 대체:

| 기존 event | 대체 event |
| --- | --- |
| `sdc_daily_pipeline` | `sdc_daily_fdr_universe`, `sdc_daily_pykrx_prices`, `sdc_daily_krx_flows` |
| `sdc_daily_accounts_flows` | OpenDART chain과 `sdc_daily_metrics_normalize` |
| `sdc_daily_common_features` | common source sync/build/coverage/readiness event |

신규 event create payload 기준:

| event id | title | script | chain | timing | timezone | target | max_children | enabled |
| --- | --- | --- | --- | --- | --- | --- | ---: | ---: |
| `sdc_daily_fdr_universe` | `SDC FDR Universe` | `/home/whi/apps/sdc/bin/universe-sync.sh` | `sdc_daily_pykrx_prices` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |
| `sdc_daily_pykrx_prices` | `SDC PYKRX Prices` | `/home/whi/apps/sdc/bin/prices-backfill-incremental.sh` | `sdc_daily_krx_flows` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |
| `sdc_daily_krx_flows` | `SDC KRX Flows` | `/home/whi/apps/sdc/bin/flows-sync.sh` | `""` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |
| `sdc_daily_fdr_common` | `SDC FDR Common Features` | `/home/whi/apps/sdc/bin/common-sync-fdr.sh` | `""` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |
| `sdc_daily_fred_common` | `SDC FRED Common Features` | `/home/whi/apps/sdc/bin/common-sync-fred.sh` | `""` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |
| `sdc_daily_ecos_common_daily` | `SDC ECOS Daily Common Features` | `/home/whi/apps/sdc/bin/common-sync-ecos-daily.sh` | `sdc_daily_ecos_common_macro` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |
| `sdc_daily_ecos_common_macro` | `SDC ECOS Macro Common Features` | `/home/whi/apps/sdc/bin/common-sync-ecos-macro.sh` | `""` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |
| `sdc_daily_krx_common` | `SDC KRX Common Features` | `/home/whi/apps/sdc/bin/common-sync-krx.sh` | `""` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |
| `sdc_daily_pykrx_common` | `SDC PYKRX Common Features` | `/home/whi/apps/sdc/bin/common-sync-pykrx.sh` | `""` | `false` | `Asia/Seoul` | `maingrp` | 1 | 0 |
| `sdc_daily_common_build` | `SDC Common Feature Build` | `/home/whi/apps/sdc/bin/common-build-daily.sh` | `sdc_daily_common_coverage` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |
| `sdc_daily_common_coverage` | `SDC Common Feature Coverage` | `/home/whi/apps/sdc/bin/common-coverage-report.sh` | `sdc_daily_common_readiness` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |
| `sdc_daily_common_readiness` | `SDC Common Feature Readiness` | `/home/whi/apps/sdc/bin/common-readiness-check.sh` | `""` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |
| `sdc_daily_opendart_corp` | `SDC OpenDART Corp` | `/home/whi/apps/sdc/bin/dart-sync-corp.sh` | `sdc_daily_opendart_financials` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |
| `sdc_daily_opendart_financials` | `SDC OpenDART Financials` | `/home/whi/apps/sdc/bin/dart-sync-financials.sh` | `sdc_daily_opendart_share_info` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |
| `sdc_daily_opendart_share_info` | `SDC OpenDART Share Info` | `/home/whi/apps/sdc/bin/dart-sync-share-info.sh` | `sdc_daily_opendart_xbrl` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |
| `sdc_daily_opendart_xbrl` | `SDC OpenDART XBRL` | `/home/whi/apps/sdc/bin/dart-sync-xbrl.sh` | `sdc_daily_metrics_normalize` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |
| `sdc_daily_metrics_normalize` | `SDC Metrics Normalize` | `/home/whi/apps/sdc/bin/metrics-normalize.sh` | `""` | `false` | `Asia/Seoul` | `maingrp` | 1 | 1 |

활성화 timing 기준:

| root event | timing | downstream |
| --- | --- | --- |
| `sdc_daily_fdr_universe` | 평일 18:30 KST | `sdc_daily_pykrx_prices` -> `sdc_daily_krx_flows` |
| `sdc_daily_fdr_common` | 평일 20:30 KST | 없음 |
| `sdc_daily_fred_common` | 평일 20:30 KST | 없음 |
| `sdc_daily_ecos_common_daily` | 평일 20:30 KST | `sdc_daily_ecos_common_macro` |
| `sdc_daily_krx_common` | 평일 21:30 KST | 없음 |
| `sdc_daily_common_build` | 평일 22:30 KST | `sdc_daily_common_coverage` -> `sdc_daily_common_readiness` |
| `sdc_daily_opendart_corp` | 매일 04:00 KST | `sdc_daily_opendart_financials` -> `sdc_daily_opendart_share_info` -> `sdc_daily_opendart_xbrl` -> `sdc_daily_metrics_normalize` |

`sdc_daily_common_build`는 여러 common source sync의 all-success join을 직접 표현하지 않고 scheduled root event로 둔다. 이 event는 `common-build-daily.sh` 내부의 `ops assert-common-freshness`가 필수 source 최신 성공 run과 최신 observation을 검사하므로, upstream source sync가 빠졌거나 stale이면 build를 실행하지 않는다.

## 9. 신규 event chain

FDR/KRX price-flow chain:

```text
sdc_daily_fdr_universe
  -> sdc_daily_pykrx_prices
  -> sdc_daily_krx_flows
```

Common feature chain:

```text
sdc_daily_fdr_common
sdc_daily_fred_common
sdc_daily_ecos_common_daily
  -> sdc_daily_ecos_common_macro
sdc_daily_krx_common
[필수 source sync 완료 또는 scheduled build time의 freshness guard 통과]
  -> sdc_daily_common_build
  -> sdc_daily_common_coverage
  -> sdc_daily_common_readiness
```

OpenDART chain:

```text
sdc_daily_opendart_corp
  -> sdc_daily_opendart_financials
  -> sdc_daily_opendart_share_info
  -> sdc_daily_opendart_xbrl
  -> sdc_daily_metrics_normalize
```

Optional:

- `sdc_daily_pykrx_common`은 wrapper만 준비한다.
- 기본 Cronicle 활성화 대상과 `SDC_COMMON_REQUIRED_SOURCES`에는 넣지 않는다.

## 10. 전환 후 검증

Cronicle 전환 후 아래 항목을 확인한다.

1. 기존 event가 disabled 또는 `timing=false` 상태다.
2. 신규 event command가 새 source별 wrapper를 직접 호출한다.
3. wrapper 로그에 lock/throttle 메시지가 남는다.
4. `sdc_daily_pykrx_prices` 성공 후 `sdc_daily_krx_flows`가 실행된다.
5. `flows sync --incremental`이 full history가 아니라 최근 범위만 계산한다.
6. OpenDART chain이 quota exhaustion exit code를 실패로 전달한다.
7. `sdc_daily_common_build`가 freshness guard 실패 시 build를 실행하지 않고 non-zero로 종료한다.
8. `sdc_daily_common_coverage`가 build 이후 진단 로그를 남긴다.
9. `sdc_daily_common_readiness`가 최종 품질 gate로 실패를 전달한다.
10. `sdc_daily_pykrx_common`은 기본 비활성 상태다.

## 11. 롤백 기준

코드/wrapper 배포 직후 문제가 있으면 다음 순서로 되돌린다.

1. Cronicle 전환 전이면 신규 event를 활성화하지 않는다.
2. 이미 신규 event를 활성화했다면 신규 event timing을 끄고 기존 event를 다시 켠다.
3. 이 repo에서 `deploy/prod/compose.yaml`과 `deploy/prod/bin/`을 이전 release 상태로 맞춘다.
4. `./deploy/deploy_to_sj2.sh`로 sj2-server에 이전 release의 compose/wrapper tree를 재배포한다.
5. sj2-server에서 `docker compose config`와 `docker compose pull collector`로 rollback tag가 유효한지 확인한다.

주의:

- sj2-server에서 `compose.yaml`이나 `bin/`을 직접 수정하지 않는다. 운영 source of truth는 이 repo의 `deploy/prod/`다.
- `deploy_to_sj2.sh`는 `bin/`을 `--delete`로 동기화하므로, rollback할 때도 이전 release의 wrapper tree 전체를 기준으로 배포한다.
- DB write를 되돌리는 절차는 이 문서 범위에 포함하지 않는다. 수집/정규화 명령은 대체로 upsert 기반이므로, 데이터 수정이 필요하면 별도 검증 후 개별 대응한다.

## 12. 완료 기준

- sj2-server compose가 새 collector image tag를 가리킨다.
- sj2-server `/home/whi/apps/sdc/bin/`에 source별 wrapper와 `lib/sdc-wrapper.sh`가 배포되어 있다.
- 운영 smoke가 통과한다.
- 기존 통합 Cronicle event가 신규 source별 event로 대체되어 있다.
- KRX/PYKRX/OpenDART/FDR/FRED/ECOS wrapper가 source domain별 lock/throttle 로그를 남긴다.
- `common-build-daily.sh`는 `ops assert-common-freshness` 통과 없이 build를 수행하지 않는다.
- `pykrx` common source는 wrapper만 준비되고 기본 Cronicle 활성화/필수 source에서는 제외되어 있다.

## 13. 실행 결과

2026-06-14 KST에 배포와 Cronicle 전환을 완료했다.

초기 계획의 목표 release는 `v0.8.11`이었으나, 운영 smoke 중 `ops assert-common-freshness`가 실제 원천 데이터 지연을 감지했다. 원인은 구현 누락이 아니라 per-series stale window가 실제 운영 지연보다 좁았던 것으로 확인했다. 이에 따라 `commodity_wti_fred`와 `macro_m2`의 `max_stale_business_days`를 조정한 hotfix를 포함해 최종 배포 버전은 `v0.8.12`가 되었다.

Release/deploy 기록:

- `v0.8.11`: 최초 source wrapper throttling 배포 release
- `v0.8.12`: freshness guard per-series stale window hotfix release
- `e8c097c`: `release: v0.8.12`
- `fcb61e3`: `deploy: use sdc v0.8.12`
- sj2-server `/home/whi/apps/sdc/compose.yaml` collector image: `ghcr.io/sjleekor/sdc:v0.8.12`
- sj2-server에 `deploy/prod/bin/` wrapper tree와 `lib/sdc-wrapper.sh` 동기화 완료
- GHCR `ghcr.io/sjleekor/sdc:v0.8.12` publish 및 sj2-server pull 완료

운영 smoke 결과:

- `common-seed-catalog.sh`: 완료
- source별 common sync 수동 실행 완료: `FDR`, `FRED`, `ECOS daily`, `ECOS macro`, `KRX`
- `ops assert-common-freshness --sources fdr,fred,ecos,krx`: 통과
- `common-build-daily.sh`: 통과
  - features processed: 37
  - facts built: 2849
  - facts upserted: 849
- `common-coverage-report.sh`: 통과, active common feature coverage `1.0000`
- `common-readiness-check.sh`: 통과, PIT violation 0

Cronicle 전환 결과:

- 신규 source별 event 17개 생성 완료
- 기존 통합 event 3개 비활성화 완료
  - `sdc_daily_pipeline`
  - `sdc_daily_accounts_flows`
  - `sdc_daily_common_features`
- `sdc_daily_pykrx_common`은 계획대로 `enabled=0`, `timing=false` 유지
- 신규 event smoke 완료
  - `sdc_daily_fdr_common`: 수동 실행 성공
  - `sdc_daily_common_build` -> `sdc_daily_common_coverage` -> `sdc_daily_common_readiness`: chain 실행 성공
- root event timing 활성화 완료
  - `sdc_daily_fdr_universe`: 평일 18:30 KST
  - `sdc_daily_fdr_common`: 평일 20:30 KST
  - `sdc_daily_fred_common`: 평일 20:30 KST
  - `sdc_daily_ecos_common_daily`: 평일 20:30 KST
  - `sdc_daily_krx_common`: 평일 21:30 KST
  - `sdc_daily_common_build`: 평일 22:30 KST
  - `sdc_daily_opendart_corp`: 매일 04:00 KST

최종 확인:

- 기존 통합 Cronicle event는 `enabled=0`
- 신규 root event만 timing 활성화
- chain child event는 `enabled=1`, `timing=false`
- `sdc_daily_pykrx_common`은 optional 상태로 비활성
- `git status --short`: clean
