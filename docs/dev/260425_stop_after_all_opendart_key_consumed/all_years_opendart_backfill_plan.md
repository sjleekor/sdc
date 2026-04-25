# 전체 사업연도 OpenDART / Metric 백필 계획

## 목표

현재 `sdc_daily_accounts_flows`는 CLI 기본값 때문에 2026-04-25 기준 2025 사업연도, `11011` 사업보고서, financials `CFS`만 대상으로 실행된다.

목표는 OpenDART에서 수집 가능한 모든 사업연도와 보고서 구간에 대해 아래 순서로 백필하고, API key가 모두 소진되면 중단한 뒤 다음날 같은 지점부터 이어서 실행되게 만드는 것이다.

1. `dart sync-financials`
2. `dart sync-share-info`
3. `dart sync-xbrl`
4. `metrics normalize`

## 현재 동작 요약

- `dart sync-financials`
  - 기본값: `--bsns-years` = 전년도, `--reprt-codes` = `11011`, `--fs-divs` = `CFS`
  - 기존 raw key `(corp_code, bsns_year, reprt_code, fs_div)`가 있으면 OpenDART 호출을 skip한다.
- `dart sync-share-info`
  - 기본값: `--bsns-years` = 전년도, `--reprt-codes` = `11011`
  - share-count, dividend, treasury-stock 각각 기존 raw key가 있으면 개별 요청을 skip한다.
- `dart sync-xbrl`
  - 기본값: `--bsns-years` = 전년도, `--reprt-codes` = `11011`
  - `dart_financial_statement_raw`의 `rcept_no`를 대상 목록으로 사용한다.
  - 기존 `(corp_code, bsns_year, reprt_code, rcept_no)` XBRL 문서가 있으면 skip한다.
- `metrics normalize`
  - 기본값: `--bsns-years` = 전년도, `--reprt-codes` = `11011`
  - raw 테이블과 XBRL fact를 읽어 `stock_metric_fact`에 upsert한다.
- OpenDART key exhaustion
  - `all_rate_limited`는 즉시 실패 종료하도록 변경되어 있다.
  - 스크립트는 `set -euo pipefail`이므로 해당 단계에서 Cronicle job이 중단된다.
  - 다음 실행 시 이미 저장된 raw는 skip되므로 이어받기 가능하다.

## 수집 범위 제안

### 사업연도

“수집 가능한 모든 사업연도”는 코드에 고정하지 말고 설정값으로 둔다.

- 기본 시작연도: `2015`
  - DART / OpenDART 전자공시 데이터와 XBRL 가용성, API 비용을 감안한 운영 기본값이다.
  - 더 오래된 연도까지 필요하면 환경 변수 또는 스크립트 상수로 낮출 수 있게 한다.
- 종료연도: 기본 `현재연도 - 1` (2026-04-25 기준 `2025`). 사업보고서(`11011`) 위주 백필에 자연스러운 default.
  - 현재연도 분기보고서(`11013`/`11012`/`11014`)까지 받고 싶을 때는 `SDC_DART_BACKFILL_INCLUDE_CURRENT_YEAR=1`을 켜서 종료연도를 `현재연도`로 끌어올린다 (스크립트 안에서 처리).
  - daily 이벤트는 default(전년도)만 돌려도 사업보고서 시점에는 자연 누락이 없다 — 분기는 별도로 트리거해야 한다는 뜻이다.

예시:

```text
SDC_DART_BACKFILL_START_YEAR=2015
SDC_DART_BACKFILL_END_YEAR=2025          # default = 현재연도 - 1
SDC_DART_BACKFILL_INCLUDE_CURRENT_YEAR=1 # 현재연도 분기보고서까지 포함
```

### 보고서 코드

OpenDART 주요 정기보고서 코드를 모두 대상으로 한다.

```text
11011 사업보고서
11012 반기보고서
11013 1분기보고서
11014 3분기보고서
```

초기 백필은 전체 코드를 대상으로 하되, API key 소진 가능성이 크므로 연도 단위 또는 보고서 코드 단위로 쪼개 실행한다.

### 재무제표 구분

`sync-financials`는 연결과 별도 모두 수집한다.

```text
CFS 연결재무제표
OFS 별도재무제표
```

`metrics normalize`는 현재 mapping rule이 CFS/OFS 모두 일부 처리할 수 있으므로, raw 수집은 둘 다 해두는 편이 좋다.

## 실행 순서

한 번의 큰 실행에서 아래 순서를 유지한다.

```bash
dart sync-corp
dart sync-financials
dart sync-share-info
dart sync-xbrl
metrics normalize
```

`sync-xbrl`은 financial raw의 `rcept_no`에 의존하므로 반드시 `sync-financials` 뒤에 둔다.

`metrics normalize`는 financial/share-info/xbrl raw를 읽으므로 맨 마지막에 둔다.

### 처리 우선순위 (corp outermost loop)

CLI 내부 iteration 순서는 `for corp in targets: for bsns_year: for reprt_code: for fs_div`이다 (`src/krx_collector/service/sync_dart_financials.py:81-86`, share-info/xbrl도 동일 패턴). 즉 **한 corp의 모든 연도 × 보고서 × fs_div를 끝낸 뒤 다음 corp으로** 넘어간다.

따라서 “연도 단위 우선순위”는 `--bsns-years` 인자 순서로 결정된다.

- 최신 연도 우선이 운영적으로 자연스럽다 → 인자는 **역순**(`2025,2024,...,2015`)으로 전달한다.
- 연도별 loop 변형(아래 §3)에서도 `seq "$end_year" -1 "$start_year"`로 최신 연도가 먼저 끝나게 한다.
- 키 소진 시 corp 목록의 뒤쪽 corp이 다음 날로 밀리지만, 매일 같은 순서로 재시도되므로 결국 모두 채워진다.

## 구현 계획

### 1. 백필 전용 서버 스크립트 추가

신규 스크립트:

```text
/home/whi/apps/sdc/bin/dart-backfill-all-years.sh
```

역할:

- `SDC_DART_BACKFILL_START_YEAR`, `SDC_DART_BACKFILL_END_YEAR`를 읽는다.
- 기본값은 시작 `2015`, 종료 `date +%Y - 1`로 둔다.
- 보고서 코드는 기본 `11013,11012,11014,11011` 또는 `11011,11012,11013,11014` 중 하나로 고정한다.
  - 운영상 최근/연간 우선이면 `11011`부터.
  - 시간순 누적이면 `11013,11012,11014,11011`.
- financials는 `--fs-divs CFS,OFS`로 실행한다.
- 각 명령은 `set -e` 아래에서 실행해 API key 소진 시 즉시 중단한다.

예상 형태 (참고용 all-in-one 변형 — 실제 default는 §3 연도별 loop):

```bash
#!/usr/bin/env bash
set -euo pipefail

cd "$HOME/apps/sdc"

start_year="${SDC_DART_BACKFILL_START_YEAR:-2015}"
default_end_year=$(( $(date +%Y) - 1 ))
if [[ "${SDC_DART_BACKFILL_INCLUDE_CURRENT_YEAR:-0}" == "1" ]]; then
  default_end_year=$(date +%Y)
fi
end_year="${SDC_DART_BACKFILL_END_YEAR:-$default_end_year}"

# 최신 연도 우선 — CLI는 corp outermost loop이므로 인자 순서가 곧 corp 안 우선순위.
years="$(seq -s, "$end_year" -1 "$start_year")"
reprt_codes="${SDC_DART_BACKFILL_REPRT_CODES:-11011,11012,11013,11014}"
fs_divs="${SDC_DART_BACKFILL_FS_DIVS:-CFS,OFS}"

docker compose pull collector
docker compose run --rm collector dart sync-corp
docker compose run --rm collector dart sync-financials \
  --bsns-years "$years" \
  --reprt-codes "$reprt_codes" \
  --fs-divs "$fs_divs"
docker compose run --rm collector dart sync-share-info \
  --bsns-years "$years" \
  --reprt-codes "$reprt_codes"
docker compose run --rm collector dart sync-xbrl \
  --bsns-years "$years" \
  --reprt-codes "$reprt_codes"
docker compose run --rm collector metrics normalize \
  --bsns-years "$years" \
  --reprt-codes "$reprt_codes"
```

### 2. 일일 이벤트와 백필 이벤트 분리

기존 `sdc_daily_accounts_flows`는 매일 최신분을 처리하는 목적이므로, 전체 백필을 그대로 넣으면 실행 시간이 길고 API key 소진이 반복될 수 있다.

권장 (분리 운영):

- `sdc_daily_accounts_flows`
  - 기존처럼 전년도 / `11011` / `CFS` default 실행만 유지. 매일 빠르게 끝나야 다운스트림(flows sync, KRX 수급)이 영향을 받지 않는다.
- 신규 Cronicle 이벤트 `sdc_opendart_all_years_backfill`
  - `dart-backfill-all-years.sh` 실행.
  - 수동 실행 또는 daily와 시간이 겹치지 않는 야간/주말 슬롯에서 저빈도 실행.
  - API key 소진으로 실패해도 다음날 재실행하면 skip 기반으로 이어받는다.

> `todo.md`의 “sdc_daily_accounts_flows 에 반영”을 **daily 이벤트 자체를 부풀리는** 방향으로 해석하지 않은 이유: daily가 매일 success로 끝나야 모니터링/알림이 “quota 소진”과 “진짜 장애”를 구분할 수 있다. 백필을 daily에 합치면 quota 소진 시 daily도 늘 failed로 끝나 신호가 죽는다. 분리하면 daily는 짧고 안정, 백필은 며칠에 걸쳐 채워나가는 별도 이벤트가 된다.

### 3. 청크 단위 실행 옵션 (default 권장 형태)

연도 전체를 한 번에 넘기면 한 job이 매우 길어진다. 운영 관찰성과 우선순위 제어를 위해 **연도별 loop를 default 형태로** 한다.

장점:

- 어느 연도에서 멈췄는지 로그가 명확하다.
- 다음날 재실행 시 이미 끝난 연도는 빠르게 skip된다.
- 한 연도 단위로 `metrics normalize`까지 마무리해 부분 결과를 일찍 확인할 수 있다.

핵심 운영 규칙:

- loop 방향은 **최신 연도 → 과거 연도** (`seq "$end_year" -1 "$start_year"`). quota 소진 시 가장 시급한 최신 연도가 먼저 채워진다.
- `dart sync-corp`은 corp master가 연도와 무관하므로 loop 밖에서 1회만 실행한다.
- 한 단계가 exit code `75`로 종료하면 `set -e`로 스크립트 전체가 중단된다. 다음날 같은 스크립트를 다시 돌리면 skip 기반으로 같은 지점에서 이어받는다.

권장 형태 (`/home/whi/apps/sdc/bin/dart-backfill-all-years.sh`):

```bash
#!/usr/bin/env bash
set -euo pipefail

cd "$HOME/apps/sdc"

start_year="${SDC_DART_BACKFILL_START_YEAR:-2015}"
default_end_year=$(( $(date +%Y) - 1 ))
if [[ "${SDC_DART_BACKFILL_INCLUDE_CURRENT_YEAR:-0}" == "1" ]]; then
  default_end_year=$(date +%Y)
fi
end_year="${SDC_DART_BACKFILL_END_YEAR:-$default_end_year}"
reprt_codes="${SDC_DART_BACKFILL_REPRT_CODES:-11011,11012,11013,11014}"
fs_divs="${SDC_DART_BACKFILL_FS_DIVS:-CFS,OFS}"

docker compose pull collector
docker compose run --rm collector dart sync-corp

# 최신 연도부터 처리 — quota 소진 시 시급한 연도가 먼저 채워지도록.
for year in $(seq "$end_year" -1 "$start_year"); do
  docker compose run --rm collector dart sync-financials \
    --bsns-years "$year" \
    --reprt-codes "$reprt_codes" \
    --fs-divs "$fs_divs"
  docker compose run --rm collector dart sync-share-info \
    --bsns-years "$year" \
    --reprt-codes "$reprt_codes"
  docker compose run --rm collector dart sync-xbrl \
    --bsns-years "$year" \
    --reprt-codes "$reprt_codes"
  docker compose run --rm collector metrics normalize \
    --bsns-years "$year" \
    --reprt-codes "$reprt_codes"
done
```

XBRL이 API key 소진으로 중단되면 해당 연도 normalize는 실행되지 않는다. 다음날 이어서 financials/share-info skip → XBRL 잔여분 fetch → normalize 순으로 자연 복구된다.

### 4. API key 소진 후 이어받기 동작 검증

아래를 확인한다.

- `dart sync-financials`
  - `all_rate_limited` 발생 시 CLI exit code `75` (`src/krx_collector/cli/app.py:37-46`).
  - Cronicle job failed 처리.
  - 다음 실행 시 기존 `(corp_code, bsns_year, reprt_code, fs_div)` 키는 skip.
- `dart sync-share-info`
  - share-count는 `(corp_code, bsns_year, reprt_code)`, dividend/treasury는 `(corp_code, bsns_year, reprt_code, statement_type)` 단위로 각각 skip.
- `dart sync-xbrl`
  - 이미 저장된 `(corp_code, bsns_year, reprt_code, rcept_no)` XBRL document는 skip.
- `metrics normalize`
  - `(ticker, metric_code, bsns_year, reprt_code)` upsert이므로 raw가 일부만 적재된 상태에서 돌려도 안전. 다음 실행에서 raw가 보강되면 자연 갱신된다.

### 4-1. no_data 응답이 raw에 저장되지 않는 점에 주의

`sync-financials`/`sync-share-info`/`sync-xbrl` 모두 OpenDART가 빈 응답을 줄 때 raw에 행을 쓰지 않는다 (`src/krx_collector/service/sync_dart_financials.py:116-117` 등). 따라서 `get_existing_*_keys` 검색에 잡히지 않아 다음 실행에서 같은 (corp, year, reprt_code, …) 요청을 다시 보낸다.

영향:

- 2015년 미상장 corp, quarterly 미제출 corp, OFS 미보고 corp 등이 매일 quota를 영구적으로 갉아먹는다.
- corp × year × reprt × fs_div 조합이 많을수록 비어있는 응답 비율이 커져, 백필이 “끝난 뒤”에도 매일 fixed cost가 남는다.

대응 (이 plan의 1차 범위 밖, 운영 관찰 후 별도 이슈로 처리):

1. raw 테이블에 빈 행이라도 1건 marker insert.
2. 별도 `dart_no_data_log` 테이블에 `(corp_code, bsns_year, reprt_code, [fs_div|statement_type|rcept_no], fetched_at)`을 기록하고 skip set에 합집합.
3. `ingestion_runs.counts.no_data_requests` 추이를 대시보드화해 비효율 corp만 수동 ticker exclude.

당장의 quota 소진 → 다음 날 이어받기 동작에는 영향이 없으므로, 백필 1차 운영은 그대로 진행하고 추세 관측 후 결정한다.

### 5. 운영 전 사전 산정 쿼리

백필 대상 규모를 DB에서 먼저 확인한다.

```sql
SELECT count(*) FROM dart_corp_master WHERE is_active = true AND ticker IS NOT NULL;
```

예상 OpenDART 요청 수는 대략:

```text
financials = active_corp_count * year_count * reprt_code_count * fs_div_count
share_info = active_corp_count * year_count * reprt_code_count * 3
xbrl       = financial raw rcept_no count 기준
```

예를 들어 active corp 2,700개, 2015~2025 11년, 보고서 4개, CFS/OFS 2개면 financials만 약 237,600 요청이다. 일일 API quota 안에서 여러 날에 나눠 진행하는 전제가 필요하다.

## 권장 최종안

1. `/home/whi/apps/sdc/bin/dart-backfill-all-years.sh`를 연도별 loop 방식으로 추가한다.
2. 신규 Cronicle 이벤트 `sdc_opendart_all_years_backfill`을 만든다.
3. 이벤트는 수동 실행 또는 daily accounts 이벤트와 겹치지 않는 시간에 실행한다.
4. `sdc_daily_accounts_flows`는 최신분 유지용으로 남긴다.
5. key 소진으로 실패하면 다음날 같은 이벤트를 다시 실행한다.

## 보류 / 추가 개선

- CLI에 `--bsns-years all` 또는 `--year-start/--year-end` 옵션을 추가하면 스크립트 문자열 조립을 줄일 수 있다.
- `sync-xbrl` 대상은 financial raw의 `rcept_no` 기반이므로, financials의 CFS/OFS 중복 rcept_no 중복 처리 상태를 관찰해야 한다.
- API quota 관측을 위해 Cronicle 로그 외에 `ingestion_runs.counts`의 `all_rate_limited_count`, `rate_limit_count`, `opendart_key_count`를 대시보드화하면 좋다.
