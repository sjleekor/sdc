# OpenDART 다중 API Key 지원 설계

작성일: 2026-04-22
최종 갱신: 2026-04-22 (설계 리뷰 반영)

## 관련 문서

- `docs/dev/260422_opendart_api_error_handling/opendart_api_error_handling_review.md`
  - OpenDART 상태코드별 현재 대응을 점검한 문서.
  - 이 설계의 "상태코드 분류 규칙"은 위 문서에 정리된 공식 코드 표를 기준으로 한다.
  - 해당 문서가 지적한 XBRL `013`/`014` 불일치는 multi-key 작업과 독립적인 단일 버그이므로, 이 설계와 별개로 선행 수정을 권장한다(아래 "권장 구현 순서 / Phase 0" 참고).

## 목적

현재 코드는 `OPENDART_API_KEY` 단일 값만 읽고, 모든 OpenDART 요청에 같은 key를 사용한다. 이 문서는 여러 개의 OpenDART key를 순환 사용하도록 구조를 변경하기 위한 설계를 정리한다.

이번 설계의 목표는 다음과 같다.

- 환경 변수로 여러 개의 OpenDART key를 설정할 수 있어야 한다.
- 기존 단일 key 설정(`OPENDART_API_KEY`)은 깨지지 않아야 한다.
- 각 요청은 사용 가능한 key 중 하나를 선택해 호출해야 한다.
- `020`(요청 제한 초과) 같은 key 단위 문제는 다른 key로 즉시 우회할 수 있어야 한다.
- key가 늘어나더라도 provider별 중복 구현이 생기지 않도록 공통 계층으로 묶어야 한다.

비목표는 다음과 같다.

- DB 스키마 변경
- 병렬 수집 도입
- OpenDART 외 외부 API 공통 key-pool 일반화

## 현재 구조 요약

### 1. 설정

- `src/krx_collector/infra/config/settings.py`
  - `Settings.opendart_api_key: str` 단일 필드만 존재한다.

### 2. CLI 의존성 주입

- `src/krx_collector/cli/app.py`
  - `dart sync-corp`
  - `dart sync-financials`
  - `dart sync-share-info`
  - `dart sync-xbrl`
- 모두 `settings.opendart_api_key`를 provider 생성자에 그대로 주입한다.

### 3. Provider

- `src/krx_collector/adapters/opendart_corp/provider.py`
- `src/krx_collector/adapters/opendart_financials/provider.py`
- `src/krx_collector/adapters/opendart_share_info/provider.py`
- `src/krx_collector/adapters/opendart_xbrl/provider.py`

각 provider는 공통적으로 아래 구조를 가진다.

- 생성자에서 `api_key: str` 하나를 받는다.
- `_download()`에서 `crtfc_key=<that one key>`를 쿼리스트링에 직접 붙인다.
- `_download()`에는 `@retry(max_attempts=3)`가 걸려 있다.
- service 레이어는 다시 `call_with_retry()`로 결과 객체의 `error`까지 재시도한다.

즉 현재는 "단일 key 고정 + provider 내부 재시도 + service 외부 재시도" 구조다.

## 현재 구조의 문제점

### 1. key 선택 책임이 provider 내부에 하드코딩되어 있다

provider가 문자열 key 하나를 들고 있으므로, 다중 key 지원을 넣으려면 모든 provider가 각자 회전 로직을 구현하게 될 가능성이 높다.

### 2. 재시도 레이어가 중첩되어 있다

- provider `_download()`의 `@retry(max_attempts=3)`
- service `call_with_retry(max_attempts=3)`

이 상태에서 key rotation까지 넣으면 최악의 경우 "key 수 x provider retry x service retry"로 시도 수가 과도하게 늘어난다.

### 3. 오류가 key 문제인지 요청 문제인지 구분이 약하다

현재는 대부분 `"OpenDART error {status}: {message}"` 문자열로만 반환된다. 다중 key를 쓰려면 최소한 아래 정도는 구분해야 한다.

- 다른 key로 바꾸면 회복 가능한 오류
- 같은 key든 다른 key든 실패할 요청 오류
- 해당 key를 풀에서 제외해야 하는 인증/권한 오류

### 4. 공통 관측성이 없다

현재 run 결과에는 어떤 key가 얼마나 사용되었는지, 몇 번 rotation 되었는지, 몇 개 key가 비활성화되었는지 남지 않는다.

## 설계 원칙

### 1. 설정은 backward-compatible 해야 한다

- 기존 `OPENDART_API_KEY`는 그대로 지원한다.
- 새 환경 변수 `OPENDART_API_KEYS`를 추가한다.
- 내부적으로는 항상 "정규화된 key 목록"으로만 다룬다.

### 2. key 선택은 provider 바깥 공통 계층으로 올린다

모든 OpenDART provider가 같은 executor / key-pool을 사용해야 한다.

### 3. 재시도는 OpenDART 전용 공통 계층으로 수렴시킨다

다중 key 지원 이후에는 provider 내부 `@retry`와 service 외부 `call_with_retry()`의 책임을 분리해야 한다.

### 4. 로그에는 실제 key를 남기지 않는다

- key 원문 로그 금지
- `key#1`, `key#2` 같은 alias 또는 마스킹된 fingerprint만 사용

### 5. 동시성 가정을 문서로 고정한다

- 1차 구현은 단일 스레드 전제로 한다.
- executor docstring에 "thread-unsafe, 단일 스레드 전제"를 명시한다.
- key state 변경 지점(`cooldown_until`, `disabled_reason`, `consecutive_failures`, 라운드로빈 커서)은 추후 `threading.Lock`을 붙일 수 있는 위치로 몰아둔다.
- 이유: 현재 설계의 비목표는 "병렬 수집"이지만, 나중에 누군가 provider 인스턴스를 `ThreadPoolExecutor`와 공유하면 state race가 생길 수 있다. 가정을 명시해야 조용히 깨지지 않는다.

## 제안 구조

### 1. 설정 계층 변경

대상 파일

- `src/krx_collector/infra/config/settings.py`
- `.env.example`
- `README.md`
- `tests/unit/test_phase0.py`

제안

- 새 환경 변수 추가: `OPENDART_API_KEYS`
  - 형식: 쉼표 구분 문자열
  - 예: `OPENDART_API_KEYS=key_a,key_b,key_c`
- 기존 `OPENDART_API_KEY` 유지

정규화 규칙

1. `OPENDART_API_KEYS`가 비어 있지 않으면 쉼표 기준 분리 후 trim 한다.
2. `OPENDART_API_KEY`가 비어 있지 않으면 목록 뒤에 추가한다.
3. 중복 key는 제거하되, 최초 등장 순서는 유지한다.
4. 최종 목록이 비어 있으면 빈 리스트다.

`Settings` 제안 인터페이스

기존 `_compute_dsn` 패턴(`model_validator(mode="after")`)과 일관되도록, **프로퍼티 대신 validator에서 한 번만 정규화**해 `tuple[str, ...]`로 고정한다. 프로퍼티 접근마다 재계산되는 것을 피하고, 다른 필드 초기화 패턴과도 맞춘다.

```python
from pydantic import Field, model_validator

class Settings(BaseSettings):
    opendart_api_key: str = ""
    # validation_alias를 통해 OPENDART_API_KEYS 환경변수를 이 필드로 매핑
    opendart_api_keys_raw: str = Field(default="", validation_alias="OPENDART_API_KEYS")

    # validator 수행 후 고정되는 정규화된 튜플
    opendart_api_keys: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _normalize_opendart_keys(self) -> "Settings":
        ...
```

메모

- 기존 코드를 한 번에 다 바꾸기 어렵다면 `opendart_api_key`는 우선 남겨둔다.
- 신규 코드는 모두 `settings.opendart_api_keys`만 사용한다.
- 추후 단일 key 필드를 deprecated 처리할 수 있다.

빈 리스트에 대한 fail-fast 정책

- `Settings` validator는 빈 key 목록을 하드 에러로 올리지 않는다. `db init`, `db sync-remote`처럼 OpenDART를 전혀 사용하지 않는 서브커맨드가 있기 때문.
- 대신 **`OpenDartRequestExecutor` 생성 시점에 fail-fast** 한다. 즉 OpenDART CLI 경로 진입 시에만 "key가 하나도 설정되지 않았다"는 명시적 에러가 난다.

### 2. OpenDART 공통 key-pool / executor 도입

신규 모듈 제안

- `src/krx_collector/adapters/opendart_common/client.py`

핵심 책임

- 사용 가능한 key 목록 보관
- 다음 key 선택
- key별 cooldown 관리
- key별 disable 관리
- HTTP 호출 수행
- OpenDART 상태코드 분류
- 필요 시 key rotation 후 재시도

제안 객체

cooldown은 **`time.monotonic()` 기준**으로 저장한다. `datetime` 기반으로 저장하면 NTP 보정, suspend/resume, 수동 시계 조정에서 key가 조기 재사용되거나 영원히 cooldown되는 버그가 가능하다. 로그 표시용으로만 `datetime`을 부가 기록한다.

```python
@dataclass(slots=True)
class OpenDartKeyState:
    alias: str
    api_key: str
    cooldown_until_monotonic: float | None = None  # time.monotonic() 기준
    disabled_reason: str | None = None
    consecutive_failures: int = 0
    last_used_at: datetime | None = None  # 로그/관측 전용


@dataclass(slots=True)
class OpenDartCallResult:
    payload: bytes | None
    key_alias: str | None
    status_code: str | None = None
    error: str | None = None
    no_data: bool = False
    retryable: bool = False
    switch_key: bool = False
    disable_key: bool = False
    # 모든 key 소진 시 service 레이어 판단에 쓰일 메타
    exhaustion_reason: str | None = None  # "all_rate_limited" | "all_disabled" | "request_invalid" | None


class OpenDartRequestExecutor:
    def fetch_bytes(
        self,
        *,
        endpoint_url: str,
        params: Mapping[str, str],
        request_label: str,
        parser: Callable[[bytes], OpenDartCallResult],
    ) -> OpenDartCallResult:
        ...
```

여기서 `parser`는 "응답 body를 읽고 이 응답이 정상인지/no-data인지/다른 key로 돌려야 하는지"를 판정하는 콜백이다.

`exhaustion_reason` 설계 이유

- "모든 key를 시도했지만 실패"라는 상황은 의미가 제각각이다. 문자열 `error`에 뭉뚱그리면 service 레이어가 다시 문자열을 파싱해 판단해야 한다.
- 아래 3가지를 구분한다:
  - `all_rate_limited`: 전부 `020`/HTTP 429. 다음 run 재시도 가치가 있음. 요청 단위 `error`로 기록하되 run은 `PARTIAL`.
  - `all_disabled`: 전부 `010`/`011`/`012`/`901`로 disable. key 전멸 상황. service는 run을 조기 종료하거나 `FAILED`로 올릴 수 있음.
  - `request_invalid`: 전부 `021`/`100`/`101`. 요청 자체가 잘못됨. 다른 key로도 안 되므로 rotation 의미 없음. 요청 단위 실패.

### 3. 상태코드 분류 규칙

다중 key 지원에서는 단순 문자열 에러보다 아래 분류가 중요하다.

| 코드 | 의미 | 제안 동작 |
| --- | --- | --- |
| `000` | 정상 | 성공 반환 |
| `013` | 데이터 없음 | provider별 규칙대로 `no_data=True` |
| `014` | 파일 없음 | XBRL은 `no_data=True`, 나머지는 요청 오류 |
| `020` | 요청 제한 초과 | 현재 key를 cooldown 후 다른 key로 즉시 전환 |
| `010` `011` `012` `901` | key 자체 문제 | 해당 key를 disable 후 다른 key 시도 |
| `021` | 조회 범위 초과 | 요청 오류로 즉시 반환 |
| `100` `101` | 잘못된 요청 | 요청 오류로 즉시 반환 |
| `800` `900` | 일시 장애 가능 | 짧은 backoff 후 다른 key 또는 동일 key 재시도 |

핵심은 다음이다.

- `020`은 "이 요청이 잘못됐다"가 아니라 "이 key가 지금 막혔다"에 가깝다.
- `010`/`011`/`012`/`901`은 같은 key를 계속 써도 회복되지 않으므로 풀에서 제외해야 한다.
- `100`/`101`/`021`은 key를 바꿔도 해결되지 않으므로 rotation 대상이 아니다.

HTTP 레벨 신호 매핑

OpenDART 응답 본문 `status` 외에 HTTP 레벨 실패도 key 단위 문제일 수 있다. executor는 아래처럼 OpenDART 상태코드와 동일 테이블로 합류시킨다.

| HTTP 신호 | 매핑 | 제안 동작 |
| --- | --- | --- |
| `HTTP 429` | `020`과 동등 | 현재 key cooldown 후 다른 key 전환 |
| `HTTP 5xx` | `800`/`900`과 동등 | 짧은 backoff 후 rotation 또는 동일 key 재시도 |
| `URLError` / socket timeout 반복 | `consecutive_failures++` | 임계치(예: 3회) 도달 시 해당 key를 짧게 cooldown |
| 단발성 `URLError` | transient | 같은 key로 1회 재시도 후 실패 시 rotation |

즉 HTTP 레벨 실패는 먼저 HTTP 상태코드로 분류하고, 분류 결과를 OpenDART 상태코드 표의 해당 행과 같은 정책으로 처리한다.

### 4. key 선택 알고리즘

1차 구현은 단순 round-robin으로 충분하다.

동작 규칙

1. 활성 key 중 `cooldown_until_monotonic`이 지나지 않은 key는 건너뛴다 (`time.monotonic()` 기준).
2. 다음 활성 key를 round-robin으로 고른다.
3. 호출 성공 시 해당 key를 마지막 사용 key로 기록한다.
4. `020` 또는 `HTTP 429`이면 해당 key를 `cooldown_until_monotonic = monotonic() + cooldown_seconds`로 설정한다.
5. `010`/`011`/`012`/`901`이면 해당 key를 disable 한다.
6. 현재 요청에서 시도 가능한 다른 key가 남아 있으면 같은 provider 호출 안에서 재시도한다. 이때 짧은 딜레이(예: `rate_limit_seconds` 활용 또는 0.1~0.2초)를 주어 OpenDART 서버 측의 동시 요청 간섭을 방지한다.
7. 모든 key가 소진되면 `exhaustion_reason`을 채운 `OpenDartCallResult`를 반환한다 (앞 섹션의 3가지 사유 중 하나).
8. 모든 key가 cooldown 상태이고 disable이 아닌 경우, 1차 구현은 **즉시 "all_rate_limited"로 반환**한다. "가장 먼저 cooldown이 풀리는 key까지 sleep 후 재시도" 정책은 `rate_limit_seconds` 정책과 상호작용이 크므로 1차 범위 밖으로 둔다.

초기 cooldown 제안

- 기본 60초
- 설정화가 필요하면 추후 `OPENDART_KEY_COOLDOWN_SECONDS` 추가
- 1차 구현에서는 상수로 시작해도 무방

### 5. provider 구조 변경

대상 파일

- `src/krx_collector/adapters/opendart_corp/provider.py`
- `src/krx_collector/adapters/opendart_financials/provider.py`
- `src/krx_collector/adapters/opendart_share_info/provider.py`
- `src/krx_collector/adapters/opendart_xbrl/provider.py`

변경 방향

- provider 생성자에서 `api_key: str` 대신 `request_executor: OpenDartRequestExecutor`를 받는다.
- 각 provider는 아래만 담당한다.
  - endpoint/params 구성
  - payload 파싱
  - provider-specific status 해석
  - domain result 생성

즉 "어떤 key를 쓸지"와 "언제 다른 key로 돌릴지"는 provider 책임이 아니다.

예시

```python
class OpenDartFinancialStatementProvider:
    def __init__(self, request_executor: OpenDartRequestExecutor, timeout_seconds: float = 30.0):
        self._request_executor = request_executor
        self._timeout_seconds = timeout_seconds
```

### 6. service / CLI 변경

대상 파일

- `src/krx_collector/cli/app.py`
- 필요 시 `src/krx_collector/util/pipeline.py`

CLI 변경

- provider 생성 시 단일 key 문자열이 아니라 shared executor를 생성해 주입한다.
- 한 명령 안에서 사용하는 provider들은 가능하면 같은 executor 인스턴스를 공유한다.
  - 예: `sync-share-info`에서 share-count/dividend/treasury가 같은 key-pool 상태를 공유해야 한다.

service 변경

- 현재 `call_with_retry()`는 result의 `error`만 있으면 무조건 재시도한다.
- 다중 key 도입 후에는 이 정책을 그대로 두면 "provider 내부 key rotation + service 외부 전체 재시도"가 겹친다.

권장안

- OpenDART provider 내부 `@retry`는 제거한다.
- OpenDART 관련 service 호출은 `call_with_retry(max_attempts=1)`로 낮추거나,
- `call_with_retry()`에 `should_retry_result` predicate를 추가해 "진짜 transient"만 재시도하게 바꾼다.

이 문서에서는 두 번째를 권장한다.

이유

- 기존 pipeline 유틸을 계속 활용할 수 있다.
- OpenDART 외 provider에는 영향이 작다.
- 향후 KRX/pykrx에도 같은 패턴을 적용하기 쉽다.

`call_with_retry()` 하위호환 제약

- `call_with_retry()`는 OpenDART 외에도 `sync_krx_flows.py`의 KRX 호출 2곳에서 사용된다 (`sync_krx_flows.py:97`, `:125`).
- `should_retry_result` 파라미터의 **기본값(None)은 현재 동작을 그대로 유지**해야 한다. 즉 truthy `error`이면 재시도.
- OpenDART 호출 경로만 명시적으로 predicate를 넘겨서 "`exhaustion_reason`이 `all_rate_limited`일 때만 재시도" 또는 "transient(`800`/`900`/`HTTP 5xx`)일 때만 재시도" 같은 규칙을 적용한다.
- 이렇게 하면 KRX 호출 동작에 영향 없이 OpenDART 호출에서만 중복 재시도를 제거할 수 있다.

예외(Exception) 처리 주의사항

- `OpenDartRequestExecutor` 내부 HTTP 호출 시 발생하는 `urllib.error.URLError`, `http.client.RemoteDisconnected` 등의 예외는 밖으로 던지지 않고 내부에서 잡아(catch) `OpenDartCallResult(error="...", switch_key=True)` 형태로 변환해 반환해야 한다.
- 예외가 밖으로 새어 나가면 `call_with_retry()`의 `except Exception:` 블록에 걸려 의도했던 `should_retry_result` 로직을 타기 전에 **동일한 키로 중복 재시도**(지수 백오프)가 발생할 수 있다.

### 7. 결과/관측성 보강

1차 구현에서는 DB 스키마 변경 없이 아래만 추가한다.

- `run.params["opendart_key_count"]` — 설정된 key 총 개수
- `run.counts["key_rotation_count"]` — 한 요청 안에서 다른 key로 전환된 횟수의 합
- `run.counts["key_disable_count"]` — 이번 run 동안 disable 된 key 개수
- `run.counts["rate_limit_count"]` — `020` / `HTTP 429` 발생 횟수
- `run.counts["key_effective_use_count"]` — 이번 run 에서 **최소 1회 성공**한 서로 다른 key 개수. 설정상으로는 여러 key가 있지만 실제로는 한 key만 일하고 있는 상황을 빠르게 포착하기 위함.

또한 로그에 아래 정보를 남긴다.

- `request_label`
- `key_alias`
- `status_code`
- `rotation reason`

실제 key 값은 로그에 남기지 않는다.

## 권장 구현 순서

### Phase 0. (선행) XBRL `013`/`014` 불일치 수정

- 자매 문서(`opendart_api_error_handling_review.md`)에서 지적한 XBRL `013`/`014`가 실제로는 `error`로 잡히는 버그는 **multi-key 작업과 독립**이다.
- 이 설계 안에서 함께 고치면 Phase 3 PR이 커지고 "multi-key 때문에 바뀐 것"과 "버그 수정" 경계가 흐려진다.
- 별도 PR로 먼저 머지하고, 그 뒤에 아래 Phase 1을 시작하는 것을 권장한다.
- 이 Phase는 이 문서 범위 외이지만, 의존 관계 명시를 위해 순서에 남겨둔다.

### Phase 1. 설정 정규화

- `Settings`에 정규화된 `opendart_api_keys` 추가
- `.env.example`, `README.md`, `tests/unit/test_phase0.py` 갱신

완료 기준

- 단일 key만 있어도 기존과 동일하게 동작
- 다중 key 문자열을 읽어 순서 보존된 목록으로 정규화

### Phase 2. OpenDART 공통 executor 도입

- `opendart_common/client.py` 추가
- key state, round-robin, cooldown, disable 로직 구현
- cooldown은 `time.monotonic()` 기준
- HTTP 레벨 신호(`HTTP 429`, `HTTP 5xx`, `URLError`) → OpenDART status 분류 매핑 구현
- `OpenDartCallResult.exhaustion_reason` 3가지 사유 구현
- **provider 테스트용 in-memory fake executor 헬퍼 제공**
  - Phase 3에서 기존 provider 테스트 픽스처를 최소 수정으로 마이그레이션하기 위함.
  - `tests/helpers/fake_opendart_executor.py`(가칭)에 응답 큐 주입형 fake를 둔다.

완료 기준

- 공통 unit test로 key rotation 로직 검증 가능
- fake executor를 이용해 기존 provider 파싱 테스트가 최소 수정으로 재사용 가능함이 확인됨

### Phase 3. provider 이관

- corp / financials / share-info / xbrl provider를 executor 기반으로 전환
- provider 내부 `@retry` 제거

완료 기준

- 기존 성공 케이스 테스트 유지
- `020`, `010`, `901` 등 key-aware 케이스 테스트 추가

### Phase 4. service 재시도 정리

- `call_with_retry()`에 `should_retry_result` predicate 파라미터 추가. **기본값은 현재 동작과 동일**해야 한다 (truthy `error`면 재시도).
- OpenDART service 호출부만 명시적으로 predicate를 넘겨서 "transient/`all_rate_limited`일 때만 재시도" 규칙을 적용한다.
- KRX 서비스(`sync_krx_flows.py`)는 변경하지 않는다.
- OpenDART provider에서 중복 재시도 제거 (provider 내부 `@retry`는 Phase 3에서 이미 제거됨).

완료 기준

- 요청 1건당 실제 시도 수가 예측 가능
- key 수가 늘어나도 과도한 중첩 재시도 없음
- KRX 호출 경로의 재시도 동작이 변경 전과 동일함이 테스트로 검증됨

### Phase 5. 관측성 보강

- run counts / params 및 로그 보강

완료 기준

- "몇 개 key가 있었는지, 몇 번 rotation 되었는지"를 실행 결과에서 확인 가능

## 테스트 계획

### 1. 설정 테스트

- `OPENDART_API_KEY`만 설정된 경우
- `OPENDART_API_KEYS`만 설정된 경우
- 둘 다 설정된 경우
- 중복 key 제거 (`OPENDART_API_KEY`가 `OPENDART_API_KEYS`에 포함된 key와 동일한 경우 포함)
- 공백 trim
- 경계값: `OPENDART_API_KEYS=" , , "` 같은 전부-공백 입력 → 빈 리스트
- 경계값: 선행/후행 쉼표 `",key_a,,key_b,"` → `(key_a, key_b)`

### 2. key-pool 단위 테스트

- round-robin 순환
- `020` 발생 시 다음 key로 전환
- `HTTP 429` 발생 시 `020`과 동일하게 cooldown + rotation
- `HTTP 5xx` 발생 시 `800`/`900`과 동일하게 backoff + rotation
- `010`/`011`/`012`/`901` 발생 시 key disable
- 모든 key cooldown 상태일 때 `exhaustion_reason="all_rate_limited"` 반환
- 모든 key disable 상태일 때 `exhaustion_reason="all_disabled"` 반환
- 모든 key에서 `100`/`101` 반환 시 `exhaustion_reason="request_invalid"` 반환
- cooldown 시계가 `time.monotonic()` 기반인지 검증 (wall-clock 변경 주입으로 영향 없음 확인)
- `key_rotation_count` / `key_disable_count` / `rate_limit_count` / `key_effective_use_count`가 executor에서 올바르게 노출되고, service가 `run.counts`로 전달하는지

### 3. provider 테스트

- financial/share-info/xbrl/corp 각 provider가 executor 결과를 올바른 domain result로 변환하는지
- 기존 파싱 중심 테스트가 fake executor 헬퍼로 마이그레이션되어 그대로 통과하는지
- (Phase 0에서 선행 수정된) XBRL `013`/`014` 회귀 테스트

### 4. integration 성격 테스트

- `sync-share-info`에서 share-count/dividend/treasury가 같은 executor 상태를 공유하는지
- `sync-corp`가 첫 key 실패 후 다른 key로 성공 가능한지
- `call_with_retry()` 하위호환: KRX 호출 경로의 재시도 횟수/동작이 변경 전과 동일한지

## 리스크와 대응

### 1. 재시도 폭증 리스크

다중 key를 넣고 기존 이중 재시도를 유지하면 시도 수가 급격히 늘어난다.

대응

- provider 내부 retry 제거
- service retry 조건 축소

### 2. 잘못된 상태코드 분류 리스크

`014`처럼 endpoint별 의미가 다를 수 있다.

대응

- 공통 executor는 "상태코드 분류 틀"만 제공하고,
- 최종 no-data 여부는 provider-specific parser가 결정한다.

### 3. 로그에 key 노출 리스크

대응

- alias만 사용
- 에러 메시지 조합 시 원문 key 삽입 금지

### 4. `call_with_retry()` 변경이 KRX에 영향

대응

- `should_retry_result` 파라미터의 기본 동작을 현재와 동일하게 유지
- KRX 호출 경로는 변경하지 않음
- 변경 전/후의 KRX 재시도 동작 동등성을 Phase 4 테스트로 고정

### 5. 시계 기반 cooldown 버그

대응

- `datetime.now()` 대신 `time.monotonic()` 사용
- `datetime` 필드는 로그 표시용으로만 유지

### 6. 동시성 가정이 조용히 깨질 리스크

대응

- executor docstring에 "단일 스레드 전제, thread-unsafe" 명시
- state 변경 지점을 한 곳으로 몰아 두어 추후 `threading.Lock` 추가가 쉬운 구조로 구현
- 비목표로 명시된 "병렬 수집"이 나중에 도입될 때 재점검 포인트로 남김

### 7. `all_rate_limited` 시 외곽 재시도 동작 (Fail-fast)

모든 키가 쿨다운에 걸려 `exhaustion_reason="all_rate_limited"`가 반환되었을 때, service 계층의 `call_with_retry`가 이를 "재시도 대상"으로 판단하면 짧은 지수 백오프(예: 3.5초 내 3회) 후 포기하고 다음 종목으로 넘어간다.

대응

- 1차 구현에서는 이 동작(빠르게 실패하고 다음 종목으로 넘어가서 파이프라인이 헛도는 현상)을 허용(Accept)한다.
- 추후 Rate Limiting 로직을 고도화할 때 "가장 빨리 풀리는 키의 남은 시간만큼 sleep" 하는 기능을 파이프라인 레벨에서 제어하도록 개선 포인트로 남긴다.

## 최종 제안

가장 안전한 방향은 "설정에 key 목록을 추가하고, OpenDART 전용 공통 executor에서 key rotation/cooldown/disable을 처리하며, provider는 파싱 전용으로 단순화"하는 구조다.

이 구조를 선택하면 다음이 가능해진다.

- 기존 단일 key 사용자와 호환 유지
- `020` 및 key 불량 상태를 자동 우회
- provider별 중복 구현 방지
- 이후 OpenDART 오류코드 대응 개선과 관측성 보강을 같은 구조 안에서 확장 가능

구현 우선순위는 `(선행) XBRL 013/014 버그 수정 -> 설정 정규화 -> 공통 executor -> provider 이관 -> service 재시도 정리 -> 관측성` 순서를 권장한다.

## 설계 리뷰 반영 체크리스트

이 설계는 2026-04-22 리뷰 결과를 다음과 같이 반영한다.

| 항목 | 반영 위치 |
| --- | --- |
| 자매 문서(상태코드 점검)와 의존 관계 명시 | 상단 "관련 문서" |
| XBRL `013`/`014` 버그를 선행 수정으로 분리 | 권장 구현 순서 Phase 0 |
| `Settings`를 프로퍼티 대신 `model_validator` 기반 정규화 | 설정 계층 변경 |
| 빈 key 리스트는 executor 생성 시점 fail-fast | 설정 계층 변경 |
| cooldown을 `time.monotonic()` 기반으로 | 제안 객체 / 동작 규칙 / 리스크 5 |
| HTTP 429 / 5xx / 반복 URLError 매핑 | 상태코드 분류 규칙 |
| `exhaustion_reason`으로 소진 사유 3가지 분리 | 제안 객체 / 동작 규칙 |
| 모든 key cooldown 상황의 1차 정책 명시 | 동작 규칙 8 |
| `call_with_retry()` 하위호환(KRX 영향 차단) | service / CLI 변경 / 리스크 4 |
| `key_effective_use_count` 관측 지표 추가 | 결과/관측성 보강 |
| 동시성 가정 명시 | 설계 원칙 5 / 리스크 6 |
| provider 테스트 마이그레이션용 fake executor 헬퍼 | Phase 2 완료 기준 |
| 설정/executor 경계값 테스트 보강 | 테스트 계획 1·2 |
