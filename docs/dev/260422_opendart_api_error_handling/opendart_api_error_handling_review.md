# OpenDART API 오류 응답코드 대응 점검

작성일: 2026-04-22
갱신일: 2026-04-22 (공통 클라이언트 `OpenDartRequestExecutor` 도입 이후 현행화)

## 조사 범위

- 현재 코드베이스에서 실제 호출하는 OpenDART API
  - `corpCode.xml`
  - `fnlttSinglAcntAll.json`
  - `stockTotqySttus.json`
  - `alotMatter.json`
  - `tesstkAcqsDspsSttus.json`
  - `fnlttXbrl.xml`
- 공식 개발가이드에 명시된 API별 `status` 코드
- 현재 코드(특히 신규 공통 클라이언트 `opendart_common.client`)가 각 `status` 코드를 어떻게 분기 처리하는지
- 서비스 레이어에서 최종적으로 `SUCCESS` / `PARTIAL` / `FAILED` 중 어떻게 귀결되는지

## 먼저 결론

- 이전 리뷰에서 지적했던 주요 문제들이 `OpenDartRequestExecutor` 도입으로 상당수 해결되었다.
  - `020`(호출제한) / `010·011·012·901`(키 사용 불가) / `021·100·101`(요청 무효) / `800·900`(일시 장애) 가 각각 다르게 분기 처리된다.
  - 여러 API 키를 순환(round-robin)하고, 키별 쿨다운·영구 비활성화 상태를 관리한다.
  - HTTP 429 → `020`, HTTP 5xx → `800`, 네트워크 예외 → `900` 로 매핑되어 동일한 분기 규칙을 탄다.
  - `fnlttXbrl`의 `013`/`014` 가 이제 실제로 `no_data` 로 귀결된다(과거 리뷰에서 가장 큰 불일치로 지적했던 버그가 해소).
- 여전히 남은 차이
  - `corpCode.xml` 은 모든 본문 오류가 run 전체를 `FAILED` 로 끝내는 것은 동일. 다만 공통 분류기 때문에 `020`/`800`/`900` 의 재시도·키 전환은 된다.
  - JSON 4종은 공식상 존재하는 `014`(파일이 존재하지 않음)를 `request_invalid` 로 분류하여 이 API 군에서는 재시도 없이 에러로 확정한다(의도적).
  - 서비스 레이어 `call_with_retry` 는 결과가 `retryable=True` 또는 `exhaustion_reason == "all_rate_limited"` 일 때만 재시도한다. 즉 영구 오류(`010·011·012·021·100·101·901`) 는 서비스 레벨에서 더 이상 불필요 재시도되지 않는다.
  - Provider 내부의 옛 `@retry(max_attempts=3)` 는 제거되었다. 재시도는 공통 클라이언트와 서비스 레이어 `call_with_retry` 로 일원화되었다.

## 코드베이스에서 확인한 API 사용처

| CLI/Service | OpenDART API | 코드 위치 |
| --- | --- | --- |
| `dart sync-corp` | `https://opendart.fss.or.kr/api/corpCode.xml` | `src/krx_collector/adapters/opendart_corp/provider.py:24`, `src/krx_collector/service/sync_dart_corp.py:22` |
| `dart sync-financials` | `https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json` | `src/krx_collector/adapters/opendart_financials/provider.py:26`, `src/krx_collector/service/sync_dart_financials.py:35` |
| `dart sync-share-info` | `https://opendart.fss.or.kr/api/stockTotqySttus.json` | `src/krx_collector/adapters/opendart_share_info/provider.py:29`, `src/krx_collector/service/sync_dart_share_info.py:35` |
| `dart sync-share-info` | `https://opendart.fss.or.kr/api/alotMatter.json` | `src/krx_collector/adapters/opendart_share_info/provider.py:30`, `src/krx_collector/service/sync_dart_share_info.py:35` |
| `dart sync-share-info` | `https://opendart.fss.or.kr/api/tesstkAcqsDspsSttus.json` | `src/krx_collector/adapters/opendart_share_info/provider.py:31`, `src/krx_collector/service/sync_dart_share_info.py:35` |
| `dart sync-xbrl` | `https://opendart.fss.or.kr/api/fnlttXbrl.xml` | `src/krx_collector/adapters/opendart_xbrl/provider.py:25`, `src/krx_collector/service/sync_dart_xbrl.py:35` |

## 공식 문서상 공통 `status` 코드

아래 코드는 이번에 조사한 6개 API 문서에 모두 동일하게 기재되어 있다.

| `status` | 공식 의미 |
| --- | --- |
| `000` | 정상 |
| `010` | 등록되지 않은 키 |
| `011` | 사용할 수 없는 키 |
| `012` | 접근할 수 없는 IP |
| `013` | 조회된 데이터가 없음 |
| `014` | 파일이 존재하지 않음 |
| `020` | 요청 제한 초과 |
| `021` | 조회 가능한 회사 개수 초과 |
| `100` | 필드 값 부적절 |
| `101` | 부적절한 접근 |
| `800` | 시스템 점검으로 서비스 중지 |
| `900` | 정의되지 않은 오류 |
| `901` | 개인정보 보유기간 만료로 사용할 수 없는 키 |

공식 참조 URL

- `corpCode.xml`: <https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019018>
- `fnlttSinglAcntAll.json`: <https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019020>
- `stockTotqySttus.json`: <https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2020002>
- `alotMatter.json`: <https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019005>
- `tesstkAcqsDspsSttus.json`: <https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019006>
- `fnlttXbrl.xml`: <https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019019>

## 공통 동작 구조 (현재 구현)

### 1. 공통 클라이언트 `OpenDartRequestExecutor`

- 위치: `src/krx_collector/adapters/opendart_common/client.py`
- 여러 API 키를 round-robin 으로 순환한다. 키는 `OPENDART_API_KEYS`(콤마 구분) 또는 레거시 `OPENDART_API_KEY` 에서 수집된다.
  - 설정 결합: `src/krx_collector/infra/config/settings.py:119`
  - 실행기 생성: `src/krx_collector/cli/app.py:31`
- 키별 상태 관리
  - `cooldown_until_monotonic`: 일시 쿨다운(기본 rate-limit 60초, transient 5초).
  - `disabled_reason`: 영구 비활성(`010`/`011`/`012`/`901`).
  - `consecutive_failures`: 연속 transient 실패. 임계치(기본 3) 도달 시 transient 쿨다운.
- 메트릭 스냅샷: `key_rotation_count`, `key_disable_count`, `rate_limit_count`, `key_effective_use_count`. 서비스 레이어가 `IngestionRun.counts` 에 기록한다.
- `fetch_bytes(endpoint_url, params, request_label, parser, timeout_seconds)` 가 키 선택→HTTP 호출→파서 호출→재시도/키 전환 루프를 수행한다.

### 2. 상태코드 분류 `classify_status`

`src/krx_collector/adapters/opendart_common/client.py:81` 에서 공식 상태코드를 다음처럼 분기한다.

| 공식 `status` | 분류 결과 (`OpenDartCallResult`) | Executor 후속 동작 |
| --- | --- | --- |
| `000` | 정상 payload/parsed_payload 반환 | 종료 |
| no-data 셋 (JSON은 `{013}`, XBRL은 `{013,014}`) | `no_data=True`, `error=None` | 종료 |
| `020` | `retryable=True`, `switch_key=True` | 현재 키 쿨다운 → 다음 키로 재시도 |
| `010`/`011`/`012`/`901` | `switch_key=True`, `disable_key=True` | 키 영구 비활성 → 다음 키로 재시도 |
| `021`/`100`/`101` (+JSON계열만 `014`) | `exhaustion_reason="request_invalid"`, 재시도 없음 | 즉시 에러 반환 |
| `800`/`900` | `retryable=True`, `switch_key=True` | transient 카운터 증가 → 다음 키로 재시도 |
| 그 외 | 단순 `error` | 재시도 없이 반환 |

`HTTPError` / `URLError` / 기타 예외도 공통 클라이언트 안에서 다음과 같이 상태코드로 매핑한다 (`client.py:367` 이하).

| 예외 | 매핑 결과 |
| --- | --- |
| HTTP 429 | `status_code="020"`, `retryable=True`, `switch_key=True` |
| HTTP 5xx | `status_code="800"`, `retryable=True`, `switch_key=True` |
| 기타 HTTPError | payload 가 있으면 파서에게 재위임, 없으면 단순 에러 |
| `URLError` | `status_code="900"`, `retryable=True`, `switch_key=True` |
| 기타 Exception | `status_code="900"`, `retryable=True`, `switch_key=True` |

### 3. 키 풀 고갈 처리

`_build_exhausted_result()` (`client.py:281`) 가 다음을 구분한다.

- 모든 키가 영구 비활성 → `exhaustion_reason="all_disabled"`, `retryable=False` (치명적)
- 모든 키가 쿨다운 중 → `exhaustion_reason="all_rate_limited"`, `retryable=True`
- 직전 결과가 `request_invalid` → 해당 결과 그대로 반환 (재시도 안 함)

### 4. 서비스 레이어 재시도 `call_with_retry`

- 구현: `src/krx_collector/util/pipeline.py:34`
- `should_retry_result` 콜백을 받는다. OpenDART sync 3종 모두 동일하게 다음으로 설정한다.

  ```python
  def _should_retry_opendart_result(result):
      return bool(getattr(result, "retryable", False)) or (
          getattr(result, "exhaustion_reason", None) == "all_rate_limited"
      )
  ```

  - 사용: `sync_dart_financials.py:29`, `sync_dart_share_info.py:29`, `sync_dart_xbrl.py:29`
- 따라서 서비스 레벨에서 재시도되는 것은 "요청이 전부 rate-limit 에 걸려 키 풀이 소진" 또는 "결과가 재시도 가능 플래그를 들고 돌아온" 경우뿐이다.
  - `010`/`011`/`012`/`021`/`100`/`101`/`901` 처럼 영구 오류는 서비스 레벨에서 재시도되지 않는다(이는 의도). 실행기가 키 전환/비활성을 통해 이미 이 키로는 더 이상 시도하지 않는 방향으로 처리했기 때문.
- 예외 기반 재시도는 최대 3회, 지수 backoff(0.5 × 2ⁿ 초)로 동일하게 동작한다.
- Provider 내부의 과거 `@retry(max_attempts=3)` 데코레이터(`_download()` 계열)는 현재 코드에서 제거되었다. 네트워크/HTTP 예외의 재시도는 실행기 단에서 상태코드 매핑 후 키 전환으로 흡수된다.

### 5. 최종 run 상태

- `financial`, `share-info`, `xbrl`: 요청별 에러가 있어도 전체 파이프라인을 `PARTIAL` 로 끝낸다.
  - `complete_run()` 이 `errors` 유무에 따라 `SUCCESS` / `PARTIAL` 을 결정. 위치: `src/krx_collector/util/pipeline.py:131`
- `corp`: `fetch_result.error` 가 있으면 예외를 던져 run 전체를 `FAILED` 로 끝낸다.
  - 위치: `src/krx_collector/service/sync_dart_corp.py:40`

## API별 점검 결과

### 1. `corpCode.xml`

- 공식 참조 URL: <https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019018>
- 파서: `OpenDartCorpCodeProvider._parse_corp_code_payload`
  - 응답이 ZIP(`PK` prefix) 이면 `status_code="000"` 로 반환.
  - ZIP 이 아니면 XML 본문에서 `status`/`message` 를 뽑아 `classify_status()` 로 위임.
  - `no_data_statuses=()` (코프코드에는 "조회된 데이터 없음" 개념을 쓰지 않음).
  - `request_invalid_statuses=OPENDART_REQUEST_INVALID_STATUSES | {"014"}` (공식 문서상 `014` 도 포함).
  - 위치: `src/krx_collector/adapters/opendart_corp/provider.py:101`
- 현재 대응
  - `000` → ZIP 파싱 후 upsert, run `SUCCESS`.
  - `020`/`800`/`900` → 공통 실행기가 키 전환·쿨다운 후 재시도. 풀 고갈 시 `all_rate_limited` 로 결과 반환.
  - `010`/`011`/`012`/`901` → 해당 키 영구 비활성. 잔여 키로 재시도. 전 키 비활성이면 `all_disabled`.
  - `013`/`014`/`021`/`100`/`101` → 재시도 없이 에러.
  - 어느 경로로든 `fetch_result.error` 가 비어있지 않으면 `sync_dart_corp_master` 가 예외로 격상 → run `FAILED`.
- 평가
  - 이전 리뷰에서 지적했던 "본문 오류에 대한 외부 재시도 없음" 은 이제 부분적으로 해소: `020`/`800`/`900` 에 대해서는 실행기 단 재시도가 작동한다. 서비스 레벨의 `call_with_retry` 는 여전히 이 명령에는 사용되지 않는다.
  - 공식 `013`(no-data)을 `no_data` 로 보지 않고 에러로 취급하는 것은 코프코드 마스터 성격상 의도적 유지.

### 2. `fnlttSinglAcntAll.json`

- 공식 참조 URL: <https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019020>
- 파서: `OpenDartFinancialStatementProvider._parse_financial_payload`
  - JSON 디코딩 후 `classify_status()` 에 `no_data_statuses={"013"}`, `request_invalid_statuses=OPENDART_REQUEST_INVALID_STATUSES | {"014"}` 로 전달.
  - 위치: `src/krx_collector/adapters/opendart_financials/provider.py:146`
- 현재 대응
  - `000` → records 생성 후 upsert.
  - `013` → `no_data=True`. 서비스는 `no_data_requests` 카운터만 증가.
  - `014` → request-invalid 로 분류. 즉시 에러, 서비스 레벨 재시도 없음.
  - `020`, `800`, `900`, HTTP 429/5xx → 실행기에서 키 전환 후 재시도. 전 키 소진 시 `all_rate_limited` → `call_with_retry` 가 추가로 최대 3회 재시도.
  - `010`/`011`/`012`/`901` → 키 영구 비활성 후 다음 키 사용.
  - `021`/`100`/`101` → request-invalid 로 분류, 재시도 없음.
- 평가
  - 상태코드별 차등 대응이 명확해졌다. 특히 `020` 과 `010~012` 의 처리가 분리되었다.
  - `014` 를 request-invalid 로 본 선택은 명시적이며, 이는 "요청 파라미터 조합으로 해당 파일이 없는 경우" 로 간주하여 무의미한 재호출을 차단한다.

### 3. `stockTotqySttus.json` / 4. `alotMatter.json` / 5. `tesstkAcqsDspsSttus.json`

- 공식 참조 URL
  - `stockTotqySttus.json`: <https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2020002>
  - `alotMatter.json`: <https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019005>
  - `tesstkAcqsDspsSttus.json`: <https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019006>
- 파서: `OpenDartShareInfoProvider._parse_json_payload` (3 API 공통)
  - `no_data_statuses={"013"}`, `request_invalid_statuses=OPENDART_REQUEST_INVALID_STATUSES | {"014"}`.
  - 위치: `src/krx_collector/adapters/opendart_share_info/provider.py:318`
- 현재 대응
  - `fnlttSinglAcntAll` 과 완전히 동일한 규칙으로 돌아간다.
  - 서비스 진입점은 엔드포인트별로 3개 (`fetch_share_count`, `fetch_dividend`, `fetch_treasury_stock`), 모두 `call_with_retry + _should_retry_opendart_result` 를 사용한다.
  - 요청별 에러는 `errors["{ticker}:{year}:{reprt}:(share_count|dividend|treasury_stock)"]` 로 누적, run 은 `PARTIAL`.

### 6. `fnlttXbrl.xml`

- 공식 참조 URL: <https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019019>
- 파서: `OpenDartXbrlProvider._parse_xbrl_payload`
  - 응답이 ZIP(`PK` prefix) 이면 `status_code="000"` 반환.
  - ZIP 이 아니면 `extract_xml_status()` → `classify_status()` 에 `no_data_statuses={"013","014"}` 로 위임.
  - 위치: `src/krx_collector/adapters/opendart_xbrl/provider.py:359`
- 현재 대응
  - `000` → `parse_xbrl_zip_response()` 호출 후 문서/팩트 upsert.
  - `013`/`014` → `no_data=True, error=None`. 서비스에서 `no_data_requests` 만 증가.
  - 나머지 상태코드 및 HTTP/네트워크 예외 → 다른 JSON API 와 동일 규칙.
- 평가
  - **과거 리뷰에서 지적한 XBRL `013`/`014` 불일치가 해소되었다.** 파서가 ZIP 이면 곧장 `000` 으로 조기 반환하고, 비-ZIP 경로에서만 `classify_status` 가 `no_data` 결정을 내리기 때문에, 서비스 레이어의 `if fetch_result.error: ... elif fetch_result.no_data: ...` 분기가 의도대로 `no_data` 로 흐른다.
  - 단위 테스트 `test_open_dart_xbrl_provider_maps_file_missing_as_no_data` (`tests/unit/test_opendart_xbrl.py:107`) 가 이 동작을 고정한다.

## 상태코드별 현재 대응 요약

| 공식 `status` | `corpCode` | `fnlttSinglAcntAll` | `stockTotqySttus` | `alotMatter` | `tesstkAcqsDspsSttus` | `fnlttXbrl` |
| --- | --- | --- | --- | --- | --- | --- |
| `000` | 정상 처리 | 정상 처리 | 정상 처리 | 정상 처리 | 정상 처리 | 정상 처리 |
| `010`/`011`/`012`/`901` | 키 영구 비활성 → 다른 키 재시도 / 전 키 소진 시 run `FAILED` | 키 영구 비활성 → 다른 키 재시도 | 좌동 | 좌동 | 좌동 | 좌동 |
| `013` | 에러(run `FAILED`) | `no_data` | `no_data` | `no_data` | `no_data` | `no_data` |
| `014` | request-invalid(에러, run `FAILED`) | request-invalid(요청단위 에러) | 좌동 | 좌동 | 좌동 | `no_data` |
| `020` | 쿨다운+키 전환 재시도 | 쿨다운+키 전환 재시도 / 풀 고갈 시 `call_with_retry` 재시도 | 좌동 | 좌동 | 좌동 | 좌동 |
| `021`/`100`/`101` | request-invalid(run `FAILED`) | request-invalid(요청단위 에러, 재시도 없음) | 좌동 | 좌동 | 좌동 | 좌동 |
| `800`/`900` | transient 쿨다운+키 전환 재시도 | 좌동 | 좌동 | 좌동 | 좌동 | 좌동 |

HTTP 레벨 매핑: 429 → `020` 과 동일, 5xx → `800` 과 동일, 네트워크 예외 → `900` 과 동일.

## 재시도 관점에서 본 추가 관찰

- 재시도는 두 계층에서 일어난다.
  1. 공통 실행기: 키 풀이 남아 있는 동안 키 전환을 무한히 반복한다(루프 종료 조건은 "성공" 또는 "재시도 불가 에러" 또는 "풀 고갈").
  2. 서비스 `call_with_retry`: 결과가 `retryable=True` 거나 `all_rate_limited` 일 때만 최대 3회 지수 backoff 재시도.
- 덕분에 영구 오류(`010`/`011`/`012`/`021`/`100`/`101`/`901`)는 서비스 레벨에서 더 이상 불필요 재호출되지 않는다.
- `020` 은 키 풀이 풍부하면 실행기만으로 흡수되고, 풀이 고갈되면 서비스 레벨 backoff 가 한 번 더 완충을 건다.
- Provider 파일에서는 더 이상 `@retry` 데코레이터가 붙은 `_download()` 가 없다. 이전 리뷰에서 지적한 "요청 1건당 최대 9회 시도" 시나리오는 현재 구조에서는 발생하지 않는다.

## 테스트 커버리지 관찰

- 공통 실행기 단위 테스트: `tests/unit/test_opendart_common_client.py`
  - `test_request_executor_rotates_on_rate_limit`: `020` → 두 번째 키로 전환, 메트릭 확인.
  - `test_request_executor_disables_invalid_key_and_uses_next_key`: `010` → 첫 키 비활성 후 두 번째 키 사용.
  - `test_request_executor_returns_all_rate_limited_when_every_key_cools_down`: 전 키 `020` → `exhaustion_reason="all_rate_limited"`, `retryable=True`.
  - `test_request_executor_returns_request_invalid_without_rotation`: `100` → 키 전환 없이 즉시 종료.
  - `test_request_executor_maps_http_429_to_rate_limit`: HTTP 429 → `020` 매핑.
- 어댑터별 단위 테스트
  - `tests/unit/test_opendart_financials.py:79` : `013` 을 `no_data=True` 로 돌려주는지 확인.
  - `tests/unit/test_opendart_financials.py:90` : `020` 응답을 `retryable=True` 로 노출하는지 확인.
  - `tests/unit/test_opendart_share_info.py`: `013` → `no_data` 경로 검증(`status_code="013"` 확인).
  - `tests/unit/test_opendart_xbrl.py:107` : `014` → `no_data=True, error=None` (과거 버그에 대한 회귀 방지).
  - `tests/unit/test_opendart_corp.py`: executor payload 소비 및 sync 동작 검증. 단, 본문 `status` 오류에 대한 직접 테스트는 여전히 없음.
- 공백
  - `021`/`100`/`101` request-invalid 분류가 어댑터 레벨에서 요청단위 에러로 맺히는지에 대한 end-to-end 회귀 테스트는 없음(공통 클라이언트 수준에서만 검증).
  - `800`/`900` 상태코드에 대한 어댑터/서비스 통합 테스트 없음.
  - `corpCode.xml` 의 본문 오류(`010`/`020` 등)에 대한 단위 테스트 없음.

## 최종 판단

현재 구현은 "OpenDART 상태코드를 의미에 맞게 분기 대응하는가" 기준에서 이전 리뷰 대비 크게 개선되었다.

- 상태코드 분류가 `classify_status()` 한 곳에 모여 일관된 규칙(`retryable` / `switch_key` / `disable_key` / `exhaustion_reason`)으로 표현된다.
- 여러 키 로테이션과 키별 쿨다운/영구 비활성이 실행기 안에서 일어나고, 키 풀 메트릭이 `IngestionRun.counts` 로 노출된다.
- HTTP 429/5xx/네트워크 예외가 공식 상태코드와 동일한 경로로 흡수된다.
- 과거 리뷰에서 "가장 명확한 불일치" 로 꼽았던 XBRL `013`/`014` 문제가 해소되었고, 이를 고정하는 단위 테스트가 있다.
- Provider 단 옛 `@retry` 중첩 재시도가 제거되어, 재시도 책임이 두 계층(실행기 + `call_with_retry`)으로 명확히 분리되었다.

남은 개선 여지는 주로 테스트 커버리지(`021`/`100`/`101`/`800`/`900` end-to-end 와 `corpCode` 본문 오류 경로)와, `corpCode.xml` 실패 시 run 전체를 `FAILED` 로 돌리는 현재 동작을 유지할지 재고할 지 정도다.
