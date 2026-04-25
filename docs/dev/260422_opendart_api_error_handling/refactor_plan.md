# OpenDART API Error Handling 리팩터링 계획

작성일: 2026-04-22
최종 갱신: 2026-04-22 (현행 코드 교차 검증 및 세부 보강)

## 관련 문서

- `docs/dev/260422_opendart_api_error_handling/opendart_api_error_handling_review.md`
  - 현재 OpenDART 상태코드 대응 현황을 정리한 리뷰 문서.
- `docs/dev/260422_multiple_opendart_api_key/multiple_opendart_api_key_design.md`
  - 공통 executor / key rotation 도입 배경과 설계를 정리한 문서.

## 문서 목적

현재 구현은 이전 대비 많이 개선되었고, 핵심 오류코드 분기와 multi-key 회전도 이미 동작한다. 이번 리팩터링의 목적은 "오류를 더 많이 처리한다"가 아니라, 이미 들어온 처리 규칙을 더 일관되게 유지하고, 서비스별 중복과 정책 분산을 줄이고, 운영 관측성과 테스트 신뢰도를 높이는 것이다.

즉 이번 계획의 초점은 다음 4가지다.

1. OpenDART 오류 정책을 provider별 하드코딩에서 공통 정책 레이어로 끌어올린다.
2. service / provider / domain result에 중복된 판단 로직을 줄인다.
3. `corp` 포함 전체 OpenDART sync의 실패/재시도 정책을 더 명시적으로 통일한다.
4. 상태코드별 테스트와 run-level 관측성을 보강해 이후 변경이 안전하게 만든다.

## 현재 코드 기준 핵심 진단

### 1. 상태코드 정책이 여전히 provider마다 분산되어 있다

현재 `OpenDartRequestExecutor` 가 공통 분기를 담당하지만, API별 차이는 아직 provider 내부 인자로 흩어져 있다.

- `src/krx_collector/adapters/opendart_financials/provider.py`
- `src/krx_collector/adapters/opendart_share_info/provider.py`
- `src/krx_collector/adapters/opendart_xbrl/provider.py`
- `src/krx_collector/adapters/opendart_corp/provider.py`

대표적으로 아래 규칙이 각 provider에 직접 박혀 있다.

- 어떤 API가 `013` 을 `no_data` 로 보는지
- 어떤 API가 `014` 를 `no_data` 로 볼지, `request_invalid` 로 볼지
- ZIP 응답 여부와 XML/JSON 에러 payload 분기

지금 당장은 동작하지만, 다음 API를 추가하거나 기존 정책을 수정할 때 provider별 규칙 drift가 다시 생길 가능성이 높다.

### 2. "정책 판단"과 "성공 payload 파싱" 책임이 섞여 있다

현재 provider 흐름은 전반적으로 다음 두 단계가 섞여 있다.

- `call_result` 를 보고 오류/재시도/no_data 판정
- 정상 payload를 도메인 레코드로 변환

그 결과로 아래 같은 중복이 남아 있다.

- provider마다 `if call_result.error or call_result.no_data: ...` 블록 반복
- `status_code`, `retryable`, `exhaustion_reason` 복사 반복
- `parse_*_response()` 함수가 성공 parsing 외에 status/no_data 방어 로직까지 다시 가짐

특히 `parse_fnltt_singl_acnt_all_response`, `parse_stock_count_response`, `parse_dividend_response`, `parse_treasury_stock_response`, `parse_xbrl_zip_response` 는 공통 executor 이후에도 일부 오류 해석 책임을 여전히 갖고 있다.

추가 관찰

- JSON 계열 성공 parser(`parse_fnltt_singl_acnt_all_response` 등)의 `if status != OPENDART_OK_STATUS: return ...(error=...)` 분기는, 현재 executor 가 `status=000` 일 때만 `parsed_payload` 를 채워 provider 에 넘기므로 **사실상 dead branch** 다. Phase 2 에서 "성공 parser 전용화"는 이 분기 제거까지 포함하는 것이 명확하다.
- XBRL 의 `_parse_xbrl_error()` (`opendart_xbrl/provider.py:205`) 와 corp 의 `parse_opendart_error_message()` (`opendart_corp/provider.py:73`) 는 XML status/message 추출이라는 동일한 작업을 `extract_xml_status()` 위에 각자 얇게 덧입힌 형태다. 둘을 공통 helper 로 합칠 여지가 있다.
- executor 쪽에도 정책 중복 진원지가 하나 남아있다: `client.py:383-386` 에서 HTTPError 바디에 payload 가 있으면 parser 를 다시 호출해 provider 정책으로 재해석한다. 이 경로는 "HTTP 레벨 오류 + XML/JSON status 혼합 응답" 이라는 이례 케이스를 위한 것이나, Phase 1 의 endpoint policy 레이어를 도입하면 이 재위임을 제거하거나 policy 경유로 대체할 수 있다.
- `_build_exhausted_result()` 의 `if last_result.exhaustion_reason == "request_invalid": return last_result` 분기(`client.py:304`) 는 현재 호출 경로에서 도달하지 않는 dead code 다. request_invalid 결과는 `switch_key=False` 라 loop 내 `if not last_result.switch_key: return last_result` 에서 먼저 반환되기 때문. Phase 2 에서 정리 대상.

### 3. service 레이어 재시도 규칙이 중복되고 `corp`만 별도 취급된다

현재 `_should_retry_opendart_result()` 는 아래 3개 서비스에 동일 구현으로 반복된다.

- `src/krx_collector/service/sync_dart_financials.py`
- `src/krx_collector/service/sync_dart_share_info.py`
- `src/krx_collector/service/sync_dart_xbrl.py`

반면 `src/krx_collector/service/sync_dart_corp.py` 는 `call_with_retry()` 를 사용하지 않고, fetch error 시 즉시 예외로 승격한다.

이 차이는 도메인 성격상 일부 타당하다. 다만 현재 구조에서는 "왜 corp만 예외인지", "어떤 에러는 즉시 실패시키고 어떤 에러는 재시도할지"가 코드 구조보다 관습에 더 가깝게 남아 있다.

권장 방향은 "corp는 최종적으로 FAILED 유지"와 "일시 오류는 financial/share-info/xbrl과 같은 공통 재시도 규칙 사용"을 분리해서 표현하는 것이다.

### 4. 결과 모델이 반복 확장되면서 공통 속성이 중복되었다 (단, `corp`는 예외적으로 축소됨)

아래 result 모델들은 거의 같은 transport-level 필드를 반복해서 가진다.

- `DartFinancialStatementResult`
- `DartShareCountResult`
- `DartShareholderReturnResult`
- `DartXbrlResult`

공통 필드:

- `no_data`
- `error`
- `status_code`
- `retryable`
- `exhaustion_reason`

반면 `DartCorpCodeResult` 는 `records` 와 `error` 만 가지며 위 transport-level 메타를 전혀 노출하지 않는다 (`src/krx_collector/domain/models.py:432-436`). 이 비대칭은 Phase 3 에서 "corp에도 `call_with_retry` 적용" 을 이야기할 때 바로 걸림돌이 된다. `OpenDartCallResult` 의 메타가 서비스 레이어까지 전달되지 않으므로, 지금 구조에서 corp 재시도는 `retryable` / `exhaustion_reason` 기반이 아니라 예외 기반으로만 가능해진다.

이 구조는 단기적으로 단순하지만, OpenDART 공통 메타가 더 늘어나면 model 수정과 provider mapping이 연쇄적으로 중복된다.

### 5. 운영 관측성은 있으나, "무슨 이유로 실패했는지"까지는 부족하다

executor 메트릭은 이미 유용하다.

- `key_rotation_count`
- `key_disable_count`
- `rate_limit_count`
- `key_effective_use_count`

하지만 실제 운영에서 더 필요한 것은 아래다.

- `status_code` 유형별 건수
- `request_invalid` / `all_rate_limited` / `all_disabled` 건수
- API별 `no_data` / `retryable_error` / `terminal_error` 건수
- corp sync 실패가 `키 전부 비활성` 때문인지 `요청 자체 오류` 때문인지에 대한 명시적 구분

지금은 로그를 뒤져야 하는 구간이 남아 있다.

### 6. 테스트는 핵심 골격은 있으나, 회귀 방지 관점에서는 아직 얕다

현재 테스트는 중요한 축을 이미 잡고 있다.

- executor의 `020`, `010`, `100`, `HTTP 429`
- financial/share-info/xbrl provider의 대표 매핑
- XBRL `014 -> no_data`

하지만 아래 케이스는 보강이 필요하다.

- `HTTP 5xx -> 800`, `URLError -> 900`
- `all_disabled`
- provider별 invalid payload(JSON/XML/ZIP 손상)
- service-level retry가 실제로 `retryable` / `all_rate_limited` 에만 반응하는지
- `corp` 의 일시 오류 재시도 vs 최종 FAILED 정책

## 개선 방향성

### 방향 1. API별 오류정책을 `policy` 객체로 명시화한다

현재는 `classify_status(..., no_data_statuses=..., request_invalid_statuses=...)` 를 provider가 직접 구성한다. 이를 OpenDART 공통 정책 객체로 올리는 것이 좋다.

예시 방향:

- `src/krx_collector/adapters/opendart_common/policy.py` 신규
- `OpenDartEndpointPolicy` dataclass 도입
- endpoint별 정책 상수 정의
  - `CORP_CODE_POLICY`
  - `FINANCIAL_STATEMENT_POLICY`
  - `SHARE_COUNT_POLICY`
  - `DIVIDEND_POLICY`
  - `TREASURY_STOCK_POLICY`
  - `XBRL_POLICY`

정책 객체가 최소한 아래를 담도록 권장한다.

- endpoint name
- payload kind (`json` / `xml_zip`)
- `no_data_statuses`
- `request_invalid_statuses`
- 성공 payload 판정 방식
- request label suffix 혹은 metric label

이렇게 되면 provider는 "정책을 선택하고 성공 payload만 파싱"하는 수준으로 단순화된다.

### 방향 2. OpenDART 공통 outcome 메타를 재사용 가능한 구조로 정리한다

중복 필드를 공통 구조로 묶는 편이 낫다.

선택지는 2가지다.

1. `OpenDartResultMeta` 같은 별도 dataclass를 도메인 result에 포함한다.
2. `OpenDartResultMixin` 또는 공통 base dataclass를 두고 결과 모델이 상속한다.

이 코드베이스에서는 dataclass 위주의 단순 구조를 유지하는 편이 맞으므로, 우선은 composition 방식이 안전하다.

권장 예시:

```python
@dataclass(slots=True)
class OpenDartResultMeta:
    status_code: str | None = None
    error: str | None = None
    no_data: bool = False
    retryable: bool = False
    exhaustion_reason: str | None = None
```

그 다음 각 result 모델은 `meta: OpenDartResultMeta = field(default_factory=OpenDartResultMeta)` 를 가지게 하고, 기존 필드는 1차 리팩터링에서 유지하거나 점진적으로 제거한다.

핵심은 "이번 리팩터링에서 모든 호출자를 한 번에 깨지 않되, 이후 확장 비용을 줄이는 방향"이다.

### 방향 3. service 재시도 규칙을 공통 helper로 올리고, corp 정책도 명시화한다

`_should_retry_opendart_result()` 는 공통 helper로 이동하는 것이 맞다.

대상 후보:

- `src/krx_collector/adapters/opendart_common/client.py`
- 또는 `src/krx_collector/util/pipeline.py`

권장 규칙:

- `retryable=True` 이면 재시도
- `exhaustion_reason == "all_rate_limited"` 이면 재시도
- `request_invalid`, `all_disabled` 는 재시도하지 않음

그리고 `corp` 에 대해서는 아래 정책을 문서와 코드로 같이 고정하는 것이 좋다.

- `request_invalid`, `all_disabled` 는 즉시 terminal failure
- `all_rate_limited`, transient retryable 결과는 `call_with_retry()` 적용
- 최종 귀결은 여전히 `FAILED` 유지

즉 "corp도 재시도는 하되, 성공하지 못하면 run 전체는 실패"라는 구조로 명확히 만드는 편이 운영적으로 가장 자연스럽다.

### 방향 4. provider는 "공통 fetch + 성공 parser" 패턴으로 단순화한다

provider마다 아래 패턴이 반복된다.

- executor 호출
- error/no_data short-circuit
- payload type 검사
- success parser 호출
- call_result 메타 복사

이를 공통 helper 함수로 줄일 수 있다.

예시:

- `build_transport_result(...)`
- `apply_call_result_meta(...)`
- `fetch_json_with_policy(...)`
- `fetch_zip_with_policy(...)`

이 단계에서 목표는 class hierarchy를 크게 만드는 것이 아니라, 반복되는 8~15줄짜리 블록을 공통 함수로 정리하는 것이다.

### 방향 5. run-level counts와 로그를 "운영 판단 가능" 수준으로 확장한다

최소한 아래 카운터는 추가 가치가 크다. 네이밍은 현재 `IngestionRun.counts` 의 기존 키(`key_rotation_count`, `rate_limit_count` 등 — prefix 없는 snake_case)와 일관되게 유지하고, `opendart_` prefix 는 도입하지 않는 것이 현실적이다 (기존 대시보드/질의와의 호환).

- `request_invalid_count`
- `all_rate_limited_count`
- `all_disabled_count`
- `retryable_error_count`
- `terminal_error_count`
- 가능하면 status bucket: `status_020_count`, `status_800_count`, `status_900_count` 등

또한 로그는 아래 필드를 일관되게 포함하도록 맞추는 것이 좋다.

- request label
- endpoint label
- key alias
- status code
- exhaustion reason
- retryability

이렇게 해야 운영 중 "키 부족 문제"와 "요청 파라미터 문제"를 빠르게 분리할 수 있다.

## 세부 실행 계획

## Phase 1. 정책 정의 정리

목표는 endpoint별 상태코드 정책을 provider 밖으로 끌어내는 것이다.

대상 파일

- `src/krx_collector/adapters/opendart_common/client.py`
- `src/krx_collector/adapters/opendart_common/__init__.py`
- `src/krx_collector/adapters/opendart_common/policy.py` 신규
- `src/krx_collector/adapters/opendart_financials/provider.py`
- `src/krx_collector/adapters/opendart_share_info/provider.py`
- `src/krx_collector/adapters/opendart_xbrl/provider.py`
- `src/krx_collector/adapters/opendart_corp/provider.py`

작업 항목

1. `OpenDartEndpointPolicy` 정의
2. endpoint별 status policy 상수 정의
3. `classify_status()` 호출부를 policy 기반으로 변경
4. provider 내 하드코딩된 `{"013"}`, `{"014"}` 규칙 제거

완료 기준

- provider가 상태코드 집합을 직접 조합하지 않는다.
- endpoint별 정책 차이는 `policy.py` 에서 한 번에 읽힌다.

## Phase 2. 결과 메타와 provider 반복 코드 정리

목표는 provider별 공통 패턴을 줄여서 변경 비용을 낮추는 것이다.

대상 파일

- `src/krx_collector/domain/models.py`
- `src/krx_collector/adapters/opendart_financials/provider.py`
- `src/krx_collector/adapters/opendart_share_info/provider.py`
- `src/krx_collector/adapters/opendart_xbrl/provider.py`
- `src/krx_collector/adapters/opendart_corp/provider.py`

작업 항목

1. `OpenDartResultMeta` 도입 여부 결정
   - 도입 시에는 모든 도메인 result 를 **한 번에** 전환하는 편이 낫다. 기존 top-level 필드와 `meta.*` 가 공존하는 기간이 길면 "어느 쪽을 보고 판단할지" 가 또 다른 drift 원천이 된다. 점진 전환이 필요하면 migration 기간을 테스트로 묶어 한 PR 안에서 끝내는 것을 권장.
2. transport-level 메타 복사 helper 추가 (예: `apply_call_result_meta(result, call_result)`)
3. 정상 payload parser를 "성공 응답 전용"으로 정리
   - `parse_fnltt_singl_acnt_all_response`, `parse_stock_count_response`, `parse_dividend_response`, `parse_treasury_stock_response` 의 non-`000` 분기는 executor 경유 호출 경로에서는 도달하지 않으므로 제거한다. (파서를 executor 외부에서 독립 사용하는 테스트가 있다면 테스트부터 조정.)
4. `parse_xbrl_zip_response()` 의 비-ZIP 에러 해석과 XBRL `_parse_xbrl_error()`/corp `parse_opendart_error_message()` 를 공통 helper 로 통합
5. `_build_exhausted_result()` 의 `request_invalid` dead branch 제거
6. executor HTTPError payload 재-parser 호출(`client.py:383-386`) 경로는 policy 레이어 도입 후 제거 가능한지 재검토 (제거 어려우면 주석으로 "이 경로는 HTTP 4xx 본문이 OpenDART XML status 를 포함하는 예외 상황에서만 쓰임" 을 명시)

완료 기준

- provider마다 반복되는 short-circuit/mapping 코드가 줄어든다.
- 성공 parser는 "성공 payload를 도메인으로 변환"하는 역할에 더 집중한다.

## Phase 3. service 재시도 정책 통일

목표는 service별 중복 retry predicate를 제거하고 `corp` 도 명시적 정책 아래 두는 것이다.

대상 파일

- `src/krx_collector/util/pipeline.py`
- `src/krx_collector/service/sync_dart_financials.py`
- `src/krx_collector/service/sync_dart_share_info.py`
- `src/krx_collector/service/sync_dart_xbrl.py`
- `src/krx_collector/service/sync_dart_corp.py`

작업 항목

1. 공통 `should_retry_opendart_result()` helper 도입
2. 3개 service의 중복 `_should_retry_opendart_result()` 제거
3. `sync_dart_corp_master()` 에도 `call_with_retry()` 적용
   - 전제: `DartCorpCodeResult` 에 `status_code`, `retryable`, `exhaustion_reason` 필드를 추가하고(또는 `OpenDartResultMeta` composition), `OpenDartCorpCodeProvider.fetch_corp_codes()` 가 `call_result` 메타를 그대로 복사해 내보내도록 수정. 그렇지 않으면 retry predicate 가 항상 `False` 가 되어 사실상 예외 기반 retry 만 작동한다.
   - 이 변경이 Phase 2 의 result 모델 정비와 묶일 수 있으므로, Phase 2/3 의 실행 경계를 유연하게 본다.
4. corp 최종 실패 정책을 코드/문서로 고정
   - 실패 시 `run.error_summary` 에 `exhaustion_reason` (`all_rate_limited` / `all_disabled` / `request_invalid`) 을 포함하도록 보강. 현재는 단순 `str(exc)` 라 원인 구분이 로그에 의존한다.

완료 기준

- OpenDART 재시도 조건은 한 곳에서 바뀐다.
- corp가 transient 상황에서 불필요하게 즉시 실패하지 않는다.
- corp run의 terminal failure 의미는 유지된다.

## Phase 4. 관측성 확장

목표는 run 결과만 보고도 장애 성격을 구분할 수 있게 만드는 것이다.

대상 파일

- `src/krx_collector/adapters/opendart_common/client.py`
- `src/krx_collector/util/pipeline.py`
- `src/krx_collector/service/sync_dart_corp.py`
- `src/krx_collector/service/sync_dart_financials.py`
- `src/krx_collector/service/sync_dart_share_info.py`
- `src/krx_collector/service/sync_dart_xbrl.py`

작업 항목

1. executor snapshot metric 확장
2. run counts에 주요 OpenDART outcome bucket 반영
3. 로그 필드 표준화
4. 필요하면 `error_summary` 생성 시 exhaustion reason 샘플 포함

완료 기준

- `ingestion_runs.counts` 만으로 rate-limit, invalid-request, all-disabled를 대략 구분할 수 있다.
- 운영 로그에서 key 문제와 요청 문제를 빠르게 분리할 수 있다.

## Phase 5. 테스트 보강

목표는 리팩터링 이후 정책 회귀를 막는 것이다.

대상 파일

- `tests/unit/test_opendart_common_client.py`
- `tests/unit/test_opendart_financials.py`
- `tests/unit/test_opendart_share_info.py`
- `tests/unit/test_opendart_xbrl.py`
- `tests/unit/test_opendart_corp.py`
- 필요 시 `tests/unit/test_pipeline.py` 신규

필수 추가 케이스

1. executor
   - `HTTP 5xx -> 800`
   - `URLError -> 900`
   - 전 key disabled 시 `all_disabled`
   - unknown status는 retry하지 않음
   - `call_with_retry` 가 `max_attempts` 에 도달했을 때 마지막 result 를 그대로 반환하는지(exception 미발생 경로)
2. provider
   - invalid JSON/XML payload 처리
   - bad ZIP 처리
   - 정책 객체가 `013/014` 매핑을 정확히 반영하는지
   - **corp 본문 오류**: XML status `010` / `020` / `100` / `014` 에 대한 어댑터 레벨 end-to-end (현재 리뷰 문서에서도 공백으로 지적됨)
3. service
   - `retryable=True` 인 결과만 재시도
   - `all_rate_limited` 재시도
   - `request_invalid` 미재시도
   - corp transient 재시도 후 최종 FAILED/SUCCESS 분기
   - corp FAILED 시 `run.error_summary` 에 exhaustion_reason 이 포함되는지

### Phase 5 를 앞당기는 보조 단계 (characterization tests)

Phase 1 의 policy 객체 도입 전에, **현행 동작을 고정하는 테스트 1~2개**를 먼저 추가하면 policy 치환 과정에서의 회귀를 저비용으로 잡을 수 있다.

- `fnlttSinglAcntAll` 에서 `013 -> no_data`, `014 -> request_invalid`
- `fnlttXbrl` 에서 `013`/`014 -> no_data`, ZIP 응답 success
- `corpCode` 에서 ZIP success, 비-ZIP 본문 오류 → `error` 매핑

즉 "Phase 1 착수 전에 위 3개 테스트를 먼저 추가 → Phase 1 → 나머지 Phase 5" 순서를 권장.

완료 기준

- endpoint별 핵심 정책 차이(`corp`, JSON 계열, XBRL)가 테스트로 고정된다.
- service retry 정책이 문서가 아니라 테스트로 보장된다.

## 권장 구현 순서

가장 안전한 순서는 아래다.

1. Phase 5 의 characterization tests 선-추가 (현행 `013/014/000/본문오류` 동작 pin)
2. Phase 1 (policy 레이어 분리)
3. Phase 2 (result meta 정비 + 성공 parser 전용화 + dead branch 제거)
4. Phase 3 (service retry 공통화 + corp retry 활성화; 단 Phase 2 에서 `DartCorpCodeResult` 에 meta 를 실어야 함)
5. Phase 4 (관측성 확장)
6. 남은 Phase 5 보강 (5xx/URLError/all_disabled/retry 한계 등)

이 순서를 권장하는 이유는 다음과 같다.

- policy를 먼저 분리해야 이후 리팩터링이 단순 치환 작업이 된다.
- 테스트를 중간에 먼저 보강해야 provider/service 정리 과정에서 회귀를 빨리 잡을 수 있다.
- 관측성 확장은 마지막에 넣어도 기능적 리스크가 낮다.

## 비목표

이번 리팩터링에서 바로 하지 않아도 되는 항목은 다음과 같다.

- OpenDART 외 외부 API까지 아우르는 범용 retry framework 일반화
- 병렬 수집 도입과 thread-safe executor 전환 (`OpenDartRequestExecutor` 는 현재 의도적으로 thread-unsafe 로 명시되어 있음 — `adapters/opendart_common/client.py:145`)
- DB 스키마 변경
- alerting / dashboard 구성 자동화
- `corpCode` run 의 terminal-FAILED 정책 자체를 재고하는 것 (Phase 3 은 "retry 로 회복 가능한 케이스만 회복" 에 한정; 최종 실패 의미는 유지)

## 기대 효과

리팩터링이 끝나면 기대하는 결과는 아래와 같다.

- OpenDART 오류 정책이 endpoint 단위로 문서와 코드에서 같은 위치에 존재한다.
- provider와 service의 중복 분기 코드가 줄어든다.
- `corp` 를 포함한 전체 OpenDART sync가 더 예측 가능한 재시도/실패 규칙을 가진다.
- 운영 중 장애 원인을 로그와 run counts만으로 더 빨리 좁힐 수 있다.
- 이후 OpenDART 신규 API 추가 시 "정책 추가 + 성공 parser 구현 + 테스트 추가" 패턴으로 확장 가능해진다.
