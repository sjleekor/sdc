# 05. N4 — 업종 지수와 구성종목 (2차)

- 작성일: 2026-08-15
- 공통 규약: [`01_implementation_checklist.md`](01_implementation_checklist.md)
- 원천: pykrx `get_index_ticker_list` / `get_index_portfolio_deposit_file` /
  `get_index_ohlcv_by_date` · 새 테이블 `krx_index_constituent`
- 선행: [N2](03_w1_company_profile.md) (대조 대상이 있어야 의미가 있다)
- 예상 규모: 호출 3–4천

---

> ## ⚠ 2026-08-16 재정의 — 아래 §3~§6은 실행하지 않는다
>
> 조사: [`poc/n7_n4_alternatives.md`](poc/n7_n4_alternatives.md) §2 · 판정: K-6d
>
> 지수 구성종목은 KRX Open API에 없고, pykrx 경로는 약관 위반이라 실행할 수 없다.
> 그런데 조사해보니 **§1의 목적 둘 중 하나는 이미 충족됐고, 남은 하나는 더 나은 원천이 있다.**
>
> **목적 B는 N4 없이 산다.** N2-7b 완료로 3,959 법인 전부에 `induty_code`(고유 630종)가 있고,
> `definitions/industry_groups.py`가 이미 KSIC 중분류로 접어 `fin_scan.py`의
> 횡단면 z-score에 쓰고 있다. **"같은 업종 종목 집합"은 지수 구성종목이 아니어도 된다.**
>
> **목적 A는 `업종분류 현황`(`MDC0201020506`) 전종목 CSV가 더 맞는다.**
> 화면 다운로드는 KRX 차단 안내가 명시적으로 허용한 경로다.
> 그리고 **§4의 N4-PR6 측정 3이 안고 있던 문제가 사라진다** —
> 지수 편입·제외에 섞이는 신규상장·유동성 미달·방법론 개편 잡음이 없다.
> 업종을 직접 읽기 때문이다. 조건 3개로 후보를 좁히는 설계 자체가 불필요해진다.
>
> **대체 실행**: 업종분류 현황 월별 스냅샷.
> **분기점 하나** — 그 화면이 **과거 기준일 조회를 받는가**(K-6e).
> 받으면 목적 A가 소급으로 풀리고, 안 받으면 앞으로만 쌓여 몇 년 뒤에나 측정된다.
>
> 아래 §3~§6(`krx_index_constituent` + 3,600 호출)은 **기록으로 남긴다.**

## 1. 왜 — 두 가지 목적이 있다

**목적 A. N2의 PIT 약점을 측정한다.** N2(DART `induty_code`)는 **현재 시점 업종만** 준다.
지수 구성종목은 **그 시점의 실제 소속**이라 룩어헤드가 없다. 둘을 대조하면
**"실제로 업종이 바뀐 기업이 몇 개인가"**를 잴 수 있다.

- 그 수가 작으면 → N2의 PIT 약점이 실무적으로 무시할 수준이라는 **근거**가 된다
- 크면 → N4를 알파 경로로 승격해야 한다

**목적 B. 관계 피쳐(C5)의 재료다.** `11_feature_taxonomy.md` §7의 산업 모멘텀·
대형주 리드래그·업종 단위 수급 쏠림이 전부 "같은 업종 종목 집합"을 필요로 한다.

여기에 하나 더. `common_feature_series` 카탈로그에 이미 `industry_krx_semiconductor_krx` 등
**산업지수 4종이 inactive(데이터 0건)**으로 등록돼 있다. N4는 그 항목을 정리하는 자리이기도
하다 — 다만 **지수 시계열이 아니라 구성종목이 주 목표**라는 점이 다르다.

---

## 2. 원천 확인 결과와 결정적 제약

```python
get_index_ticker_list(date=None, market='KOSPI') -> list
get_index_portfolio_deposit_file(ticker, date=None, alternative=False) -> list
#   docstring NOTE: 2014년 5월 2일 까지만 조회 가능
get_index_ohlcv_by_date(fromdate, todate, ticker) -> DataFrame
```

**`get_index_portfolio_deposit_file`은 2014-05-02까지만 조회된다.** docstring에 명시돼 있다.

**다행히 현재 표본 시작과 거의 맞는다** — px/flow 표본이 2014-06-02 시작이고 공통 피쳐도
2014-06부터다(`02_feature_candidate.md` §2.2가 "학습 구간 시작은 2014-06 이후 권장"). 즉
**이 제약이 실질적 손실을 거의 만들지 않는다.**

> **미검증.** 실호출 실패로 확인 못 했다. 특히 `date` 인자 포맷이 docstring에 `YYMMDD`로
> 적혀 있는데 다른 함수는 `YYYYMMDD`다. **PoC에서 확정**한다.

---

## 3. 스키마

```sql
-- sql/postgres_ddl.sql
CREATE TABLE IF NOT EXISTS krx_index_constituent (
    as_of_date      DATE        NOT NULL,
    index_code      TEXT        NOT NULL,   -- pykrx 지수 티커 (예: '1005')
    index_name      TEXT,
    market          TEXT        NOT NULL,   -- KOSPI | KOSDAQ
    ticker          TEXT        NOT NULL,
    source          TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (as_of_date, index_code, ticker)
);

CREATE INDEX IF NOT EXISTS ix_krx_index_constituent_ticker
    ON krx_index_constituent (ticker, as_of_date DESC);

CREATE INDEX IF NOT EXISTS ix_krx_index_constituent_sync_cursor
    ON krx_index_constituent (fetched_at, as_of_date, index_code, ticker);
```

**등록 6곳** (`01` §1). 익스포터 전략은 `date_month`(`date_column = "as_of_date"`),
`bin/...sh`의 `date_month_tables` 배열.

**지수 시계열은 별도로 만들지 않는다.** `common_feature_observation_raw`가 이미 시계열
저장소이고 `common_feature_series` 카탈로그에 산업지수 4종이 등록돼 있다. **지수 값이
필요하면 그 경로로 활성화**한다(§6).

---

## 4. 작업 순서

### N4-PR1 — PoC (호출 20건 미만)

```python
get_index_ticker_list('20240102', market='KOSPI')    # 지수 목록과 코드 체계
get_index_ticker_list('20240102', market='KOSDAQ')
get_index_portfolio_deposit_file('1005', '20240102') # 구성종목
get_index_portfolio_deposit_file('1005', '20140602') # 하한 근처
get_index_portfolio_deposit_file('1005', '20140401') # 하한 밖 — 실패 확인
```

**확인할 것.**

- **어느 지수가 업종 지수인가.** KOSPI/KOSDAQ 전체 지수, 규모별 지수(대·중·소형),
  섹터 지수가 섞여 있다. **업종 지수만 고르는 코드 목록을 확정**한다
- `date` 포맷 (`YYMMDD` vs `YYYYMMDD`)
- 2014-05-02 하한이 실제로 걸리는가
- 구성종목 합집합이 전체 상장 종목을 얼마나 덮는가 — **여기가 N4의 약점**(§5)
- 지수 개편 이력이 있는가 (지수 코드가 중간에 바뀌면 시계열이 끊긴다)

산출물: `poc/n4_index_constituent.md` + **확정된 업종 지수 코드 목록**.

### N4-PR2 — 스키마 + 등록 6곳

### N4-PR3 — 어댑터 + 포트 + 서비스 + CLI

```
ports/index_constituent.py            IndexConstituentProvider(Protocol)
adapters/index_krx_pykrx/provider.py
domain/enums.py                       RunType.KRX_INDEX_CONSTITUENT_SYNC
service/sync_index_constituents.py
cli/app.py:  universe sync-index-constituents --start --end [--freq month] [--indices]
```

수집 해상도는 **월말**이다. N3와 같은 이유이고 같은 캘린더 유틸을 쓴다.
호출 = 145 월말 × 업종 지수 약 25개 ≈ **3,600회.**

skip-if-present 키는 `(as_of_date, index_code)`.

### N4-PR4 — 테스트

- 월말 산출, skip, 부분 실패 → `partial`
- **2014-05-02 하한 밖 요청을 아예 만들지 않는지** — 실패를 에러로 쌓지 않고 스킵해야 한다

### N4-PR5 — 실행

### N4-PR6 — N2 대조 (이 작업의 주 산출물)

```
측정 1. 커버리지
  - 각 시점에서 업종 지수 합집합이 덮는 종목 비율
  - 미편입 종목의 특성 (시총 하위? 신규 상장?)

측정 2. N2와의 일치도
  - 현재 시점: DART induty_code 그룹 vs KRX 업종 지수 소속의 교차표
  - 두 분류가 얼마나 같은 것을 보고 있는가

측정 3. ★ 업종 변경 기업 수 — 정의를 좁게 잡는다
```

**측정 3의 정의가 이 작업에서 가장 조심할 부분이다.** 지수 편입·제외에는 업종 변경 말고도
**신규상장, 유동성 기준 미달, 지수 방법론 개편**이 전부 섞인다. 그냥 "소속 지수가 바뀐 종목"을
세면 업종 변경을 크게 과대평가한다.

> **업종 변경 후보 = 아래를 전부 만족하는 종목만 센다.**
>
> 1. 두 시점 사이에 **연속 상장**돼 있었다 (신규상장·상폐 제외)
> 2. 두 시점 모두 **정확히 하나의 업종 지수**에 속했다
> 3. **서로 배타적인** 업종 지수 A → B로 이동했다 (규모별·테마 지수 제외)
> 4. 그 시점에 지수 방법론 개편이 없었다 (KRX 공표 이력과 대조)

**임계값은 통계적 근거가 아니라 운영 규칙이다.** 그 점을 분명히 하고 고정한다.

| 업종 변경 비율 | 운영 결론 |
|---|---|
| < 3% | N2의 PIT 오염이 작다. **진단 범위를 조금 넓혀도 된다** |
| 3~10% | N2는 진단 전용 유지. 알파 경로는 N4로 |
| > 10% | **N2는 진단에서도 조심.** N4 기반으로 전환 |

**어느 경우에도 N2로 scored backtest를 돌리지는 않는다** — 그건
[N2 §6](03_w1_company_profile.md)에서 이미 금지선으로 그었고, 이 측정으로 풀리지 않는다.
이 표가 정하는 것은 "진단을 어디까지 믿을 것인가"이지 "알파로 쓸 것인가"가 아니다.

**이 정의와 임계값을 결과 보기 전에 고정한다**(§7).

---

## 5. N4의 약점 — 커버리지

| | N2 (DART induty_code) | N4 (KRX 업종 지수) |
|---|---|---|
| PIT | ✗ 현재 시점만 | **○ 시점별 실제 소속** |
| 커버리지 | **○ 상장사 전체** | **✗ 지수 미편입 종목 누락** |
| 분류 깊이 | 표준산업분류 (조정 가능) | KRX 업종 (고정) |
| 하한 | 없음 | **2014-05-02** |
| 비용 | 2,700 호출 1회 | 3~4천 호출 |

**둘은 대체 관계가 아니라 보완 관계다.** N2로 전 종목을 덮고, N4로 PIT 약점을 측정한다.
이게 §1의 목적 A다.

**미편입 종목 처리**를 미리 정한다. 지수에 없는 종목은 N4 기준으로 업종이 없다.

- 권고: **N2 업종을 폴백으로 쓰고, `industry_source` 컬럼으로 출처를 구분**한다.
  마트에서 폴백 비율을 항상 같이 리포트한다

---

## 6. 곁다리 — inactive 산업지수 4종 정리

`common_feature_series`의 `industry_krx_semiconductor_krx` 등 4종이 **inactive, 데이터 0건**이다
(`00_raw_feature_inventory.md` §2.3). N4 작업 중에 셋 중 하나로 정리한다.

1. **활성화** — `get_index_ohlcv_by_date`로 지수 시계열을 받아 `common feature sync` 경로에 태운다.
   `11_feature_taxonomy.md` §7.2의 `rel_industry_mom_20d` 재료가 된다
2. **재정의** — PoC에서 확인한 실제 지수 코드로 카탈로그를 고친다 (지금 것이 잘못된 후보일 수 있다)
3. **삭제** — 안 쓸 거면 카탈로그에서 뺀다. inactive로 남겨두면 readiness 게이트만 어지럽힌다

**권고는 1번이다.** 다만 `00_status.md` §5-1의 경고를 기억한다 — common feature readiness
게이트가 창 정렬 문제로 37개 중 4개만 ready인 상태다. **시리즈를 늘리면 그 게이트가 더
어지러워진다.** 산업지수를 켜기 전에 readiness 창 정렬 문제를 먼저 정리하거나, 최소한
새 시리즈가 그 게이트에 미치는 영향을 확인한다.

---

## 7. 결과 보기 전에 고정할 것

1. **업종 지수 코드 목록** — PoC에서 확정한 목록을 코드에 상수로 박고, 나중에 "이 지수를 빼니
   결과가 좋아진다"는 식으로 고치지 않는다. **배타적 업종 지수만** 넣고 규모별·테마 지수는 뺀다
2. **§4-PR6 측정 3의 정의 4개 조건과 임계값** — 3% / 10% 두 문턱. **운영 규칙임을 명시**한다
3. **미편입 종목 폴백 정책** — N2 폴백 여부와 `industry_source` 노출

---

## 8. 완료 기준

공통 DoD(`01` §7)에 더해:

- [ ] `poc/n4_index_constituent.md` + 확정 업종 지수 코드 목록
- [ ] 2014-06 ~ 현재 월말 구성종목 백필 완료
- [ ] **N2 대조 3종 측정 결과 문서화** (커버리지 / 일치도 / **업종 변경 비율**)
- [ ] 측정 3이 **4개 조건을 모두 적용한 좁은 정의**로 계산됐는가
- [ ] §4-PR6 표의 임계값에 따른 **N2 진단 신뢰 범위** 결론 기록
- [ ] inactive 산업지수 4종 처리 결정(§6)과 근거 기록
