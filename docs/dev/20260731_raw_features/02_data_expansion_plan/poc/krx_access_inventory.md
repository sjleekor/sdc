# 인벤토리 — `data.krx.co.kr`을 치는 경로 전부

- 조사일: 2026-08-16
- 계기: KRX 접근 경로 교체(K 묶음)를 하려면 **무엇을 교체해야 하는지**가 먼저 있어야 한다.
- 방법: `grep -rl 'krx\.co\.kr' src/` + pykrx `stock_api.py` 소스에서 KRX/naver 분기 확인
  + FinanceDataReader 패키지 소스 확인 + Cronicle 스케줄 + `ingestion_runs.counts` 실측

---

## 0. 한 줄 결론

**KRX를 치는 경로는 문서에 적힌 것보다 많고, 문은 3개다.**

**`universe sync`가 KRX 수집기였다** — FDR 라이브러리 안에서 우리 MDC client와
같은 호스트를 친다. 이전 회계에 없던 항목이다.

> **2026-08-16 정정 3건.** 외부 리뷰에서 초판의 사실 주장 셋이 틀린 것으로 확인됐다.
>
> 1. **`prices backfill`도 KRX를 친다** — 데이터는 naver지만 pykrx import가
>    로그인을 발생시킨다. **매일 도는 것은 셋이 아니라 넷이다** (§4)
> 2. **FDR 경로 분석이 틀렸다** — `MDCSTAT01501`이 아니라 GitHub CSV 캐시를 읽는다.
>    이를 근거로 한 K-2 간접 검증도 **철회한다** (§2.1, §7)
> 3. **"약 53건"은 HTTP 요청 수가 아니다** — 논리 작업 수다. 실제는 그보다 많다 (§1.1)
>
> 그리고 조건부·수동·테스트 경로와 **FDR의 pykrx 자동 폴백**이 빠져 있었다 (§1.3, §2.1b).

차단을 부른 건 백필 두 개(N1 약 6,000 / N3 약 290)로 보이지만,
**요청량으로 차단을 설명하는 것 자체가 근거가 약하다** — 약관 위반은 양이 아니라
자동화 수집 여부다(§8.1).

---

## 1. 전체 목록

| 파이프라인 | 어댑터 | 문 | 일 요청 | 상태 |
|---|---|---|---:|---|
> **2026-08-16 재작성.** 초판은 현재 스케줄만 세었고 **조건부·수동·테스트·
> 라이브러리 side effect를 빠뜨렸다.** 네 층으로 나눈다.

### 1.1 매일 도는 것 — 셋이 아니라 **넷**이다

| 파이프라인 | 문 | 상태 |
|---|---|---|
| `universe sync --source fdr` | **익명** (+ 실패 시 pykrx 폴백, §2.1b) | 매일 18:30 · 실 HTTP ~4 |
| **`prices backfill`** | **pykrx 로그인** (import side effect, §4) | **매일. 초판에서 누락** · 실 HTTP 3~4 |
| `flows sync` | MDC 로그인 | 매일 (체인) · 논리 38 |
| `common sync --sources krx` | MDC 로그인 | 매일 (체인) · 논리 11 |

> **숫자의 성격에 주의한다.** `flows` 38과 `common krx` 11은
> `ingestion_runs.counts.requests_attempted`인데 이건 **논리 작업 수이지 HTTP 요청 수가 아니다.**
> 투자자 bulk 1건이 내부에서 **4개 엔드포인트를 호출한다**
> (`flows_krx/provider.py:115` — 기관·개인·외국인·기타외국인). warmup·login·retry도 안 세어진다.
> **"하루 53요청"은 하한이고, 차단 원인을 요청량으로 설명한 근거는 부정확했다.**
> 실 HTTP·page·retry·login을 따로 세는 계측이 필요하다.

### 1.2 중단된 백필

| 명령 | 문 | 상태 |
|---|---|---|
| `prices market-cap-backfill` (N1) | pykrx 로그인 | **중단** (~6,000 예정분) |
| `universe backfill-snapshots` (N3) | pykrx 로그인 | **중단** (60/152) |

### 1.3 조건부 · 수동 — 차단이 풀리면 다시 실행되기 쉽다

| 경로 | 근거 |
|---|---|
| `universe sync --source pykrx` | CLI가 직접 지원 (`cli/app.py`) |
| `common-sync-pykrx.sh` | `SDC_COMMON_ENABLE_PYKRX=1`이면 실행 |
| `flows-backfill-range.sh` | 명시 범위 수동 백필 |
| `universe_fdr` → **pykrx 폴백** | FDR 실패 시 **자동** (§2.1b) |

### 1.4 테스트 — opt-in live

`tests/integration/test_universe_fdr_live.py` (`RUN_LIVE_FDR_TEST=1`) ·
`test_universe_pykrx_live.py` · `test_market_cap_live.py` (`RUN_LIVE_PYKRX_TEST=1`)

---

## 2. 매일 도는 것

### 2.1 `universe sync --source fdr` — **KRX 수집기다**

> **2026-08-16 정정.** 초판은 이 경로가 `KrxMarcapListing`을 거쳐 `MDCSTAT01501`을
> 받는다고 적었다. **틀렸다.** 설치본(FDR 0.9.110)에서 `StockListing('KOSPI')`는
> `KrxMarcapListingCache`로 가고, 이 클래스는 KRX resource bundle을 GET한 뒤
> **GitHub CSV 캐시를 읽는다.** `MDCSTAT01501`을 치는 `KrxMarcapListing`은
> 별도 클래스고 이 경로에서 쓰이지 않는다.
> 근거: `.venv/.../FinanceDataReader/data.py:162`, `krx/listing.py:10`

18:30 체인의 루트다. `adapters/universe_fdr/provider.py`가 `fdr.StockListing('KOSPI')`를
부르고, FDR 안에서 `KrxMarcapListingCache.read()`가 실행된다.

```
http://data.krx.co.kr/comm/bldAttendant/executeForResourceBundle.cmd   ← KRX. 최근 거래일(max_work_dt)만 얻는다
https://raw.githubusercontent.com/FinanceData/fdr_krx_data_cache/.../<날짜>.csv   ← 실제 데이터
```

**KRX에는 resource bundle GET만 나간다.** 그마저 코드에 같은 GET이 두 번 있어
(`r = requests.get(url…)` 뒤에 `json.loads(requests.get(url…).text)`)
**시장당 2요청 × 2시장 = 4건**이다. 결과 수치는 KRX가 아니라 GitHub 캐시에서 온다.

주의할 점 셋:

- **우리 `HumanThrottle`이 안 걸린다.** 라이브러리 안에서 나가는 요청이다
- **로그인하지 않는다** — 익명. `KRX_ID`/`KRX_PW`와 무관한 문이다
- UA도 FDR이 자체 지정한다 (`Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36`)

### 2.1b 그리고 문서에 없던 pykrx 폴백이 있다

`universe_fdr/provider.py:80` — **FDR 호출이 실패하면 `PykrxUniverseProvider`로 자동 전환한다.**

```python
except Exception as exc:
    logger.warning("FDR universe fetch failed; falling back to pykrx: %s", exc)
    fallback_result = PykrxUniverseProvider().fetch_universe(markets, as_of)
```

`tests/unit/test_universe_fdr_provider.py:108`이 이 동작을 고정하고 있다.

**차단 상황에서 가장 나쁜 실패 방식이다.** KRX resource bundle이나 GitHub 캐시가
흔들리는 바로 그 순간 **로그인 기반 pykrx 수집으로 넘어간다.**
K-5 범위에 폴백 제거 또는 명시적 비활성화를 넣어야 한다.

### 2.2 `flows sync` — 일 38요청

`(거래일 × 시장)` 단위 bulk 3단계. 종목당이 아니라서 양이 작다.

| 단계 | bld |
|---|---|
| `foreign_holding` | `MDCSTAT03701` |
| `investor_bulk` | `MDCSTAT02302` |
| `shorting_bulk` | `MDCSTAT30101` (거래) + `MDCSTAT30501` (잔고) |

ISIN 해석용 `dbms/comm/finder/finder_stkisu`가 추가로 붙는다(캐시됨).
2026-08-15 실측: 38 attempted / 50 skipped / 82,609행.

### 2.3 `common sync --sources krx` — 일 11요청

active 11 시리즈, 시리즈당 1요청.

| bld | 건수 | 내용 |
|---|---:|---|
| `MDCSTAT00301` | 3 | KOSPI · KOSDAQ · KOSPI200 종가 |
| `MDCSTAT01501` | 8 | 두 시장의 상승/하락/보합 종목수 + 거래대금 |

---

## 3. 중단된 백필 — 2개

pykrx의 KRX 경로다. **2026-08-16 차단이 여기서 났다.**

| 명령 | pykrx 함수 |
|---|---|
| `prices market-cap-backfill` (N1) | `get_market_cap_by_ticker(date, market)` |
| `universe backfill-snapshots` (N3) | `get_market_ticker_list` + `get_market_price_change_by_ticker` |

---

## 4. `prices backfill` — 데이터는 naver, 그런데 KRX-free가 아니다

> **2026-08-16 정정.** 초판은 이 항목을 "KRX처럼 보이지만 아닌 것"으로 분류했다.
> **데이터에 대해서만 맞는 말이었다.**

데이터는 naver에서 온다. pykrx `stock_api.py`:

```python
if adjusted:
    df = naver.get_market_ohlcv_by_date(fromdate, todate, ticker)   # ← 우리 경로
else:
    df = krx.get_market_ohlcv_by_date(fromdate, todate, ticker, False)
```

**그런데 실행 과정이 KRX를 친다.**

```
prices_pykrx/provider.py:52   stock = get_pykrx_stock_module()
pykrx_auth.py                 자격증명을 환경에 넣고 pykrx를 import
webio.py:11                   _session = build_krx_session()   ← import 시점에 실행
auth.py  login_krx()          warmup GET 2회 + login POST 1회 (CD011이면 재전송 1회 더)
```

**프로세스당 data.krx.co.kr로 3~4요청이 나간다.** 종목 수와 무관하게 실행당 한 번이다.
데이터 요청 2,763건에 비하면 작지만 **0이 아니다.**

**그래서 K-5에서 `flows`·`common krx`만 지워도 KRX 자동 접속이 남는다.**
가격 수집을 pykrx import를 거치지 않는 naver 어댑터로 분리하거나,
최소한 KRX 인증 side effect가 없는 방식으로 바꿔야 한다.

**`validate`** — DB만 읽는다. `validate.py`의 pykrx 언급은 docstring이다.

---

## 5. 비활성 — 켜면 KRX 요청이 는다

`definitions/common_features.py`에서 `active=False`인 것들이다.

| 시리즈 | 소스 | 사유 |
|---|---|---|
| `market_kospi` · `market_kosdaq` · `market_kospi200` | PYKRX | MDC 직접 경로로 대체됨 (폴백 보존) |
| `industry_krx_semiconductor_krx` 외 3종 | KRX (`MDCSTAT00301`) | 미착수 |

---

## 6. 문이 셋이다

같은 호스트인데 인증 경로가 따로 있다. **하나를 고쳐도 나머지가 남는다.**

| | MDC 직접 | pykrx | FDR |
|---|---|---|---|
| 파일 | `adapters/krx_common/client.py` | `adapters/pykrx_auth.py` | 라이브러리 내부 |
| 로그인 | `MDCCOMS001D1.cmd` (`KRX_ID`/`KRX_PW`) | import 시점에 자동 | **안 함 (익명)** |
| 우리 스로틀 | 적용 | 적용 (v0.9.10~) | **미적용** |
| 쓰는 곳 | `flows` · `common krx` | N1 · N3 · **`prices backfill`** | `universe sync` |

`prices backfill`이 pykrx 문에 들어간 것이 §4의 정정이다.
**데이터를 안 받아도 문은 연다.**

---

## 7. 철회 — "매일 N1 데이터를 받아서 버리고 있다"

> **2026-08-16 철회.** 초판은 FDR이 매일 `MDCSTAT01501`을 받아
> `MKTCAP`·`LIST_SHRS`·`ACC_TRDVAL`을 버린다고 적고, 응답 키가 `OutBlock_1`이라는 점을
> **K-2의 간접 근거**로 삼았다. **§2.1 정정대로 그 경로는 쓰이지 않는다.**
> 두 주장 다 철회한다.

- ~~"N1 필드를 이미 받아 놓고 버린다"~~ — `MDCSTAT01501`을 치지 않는다
- ~~"`OutBlock_1`이라 Open API 스키마가 같을 것"~~ — **간접 근거가 성립하지 않는다**

**K-2는 다른 근거로 이미 닫혔다.** KRX Open API 주식 서비스 상세 페이지의
embedded schema에 `ACC_TRDVAL`·`MKTCAP`·`LIST_SHRS`가 있다
([`krx_open_api.md`](krx_open_api.md) §4.1). **공식 스키마이지 정황이 아니다.**

**교훈은 남긴다.** 라이브러리 내부를 읽을 때 **클래스가 실제로 호출되는지**를
디스패처에서 확인하지 않았다. 이름이 비슷한 두 클래스(`KrxMarcapListing` /
`KrxMarcapListingCache`) 중 안 쓰이는 쪽을 읽고 결론을 냈다.

---

## 8.1 요청량으로 운영 지속을 정당화한 것은 틀렸다 (2026-08-16)

초판과 [`flows_alternatives.md`](flows_alternatives.md) §5, `docs/operations.md`는
**"하루 38요청이라 차단을 부른 규모가 아니므로 교체 전까지 계속 돌려도 된다"** 고 적었다.

**계획 내부 논리와 충돌한다.** K-0은 차단이 **속도가 아니라 약관 위반**이라고 결론냈다.
약관 제10조 제2호는 **자동화 수집 자체**를 금지하고 속도 조건이 없다.

> 위반 기준이 자동화 여부라면 **38회와 6,000회 사이에 허용·금지 경계가 없다.**
> 호출량은 **탐지 위험을 바꿀 뿐 권한을 만들지 않는다.**

같은 논리로 `universe sync`·`common krx`·`prices backfill`도 정당화할 수 없다.
**계속 돌리는 것은 "안전해서"가 아니라 "데이터 공백을 감수하지 않기로 한 선택"이다.**
그 선택을 하려면 그렇게 적어야지, 요청량을 근거로 대면 안 된다.

**미결 — 운영 결정이 필요하다.**

| 안 | 내용 | 대가 |
|---|---|---|
| A | 공식 경로 준비까지 **Marketplace 자동화 경로 전면 중단** | `flows`·`common krx`·`universe`·`prices` 공백 |
| B | 계속 돌리되 **한시적 위반임을 명시**하고 교체를 최우선으로 | 위반 상태 지속 |

**논리적으로 일관된 것은 A다.** B를 택한다면 근거는 "요청량이 작아서"가 아니라
"대체 경로 완성까지의 기간과 데이터 공백을 견주어 감수한다"여야 한다.

---

## 8. 교체 범위에 주는 함의

Open API로 옮기면 **N1·N3만 덮인다.** 매일 도는 셋 중에서는

| 파이프라인 | Open API 대체 |
|---|---|
| `universe sync` | 종목기본정보(`stk_isu_base_info`)로 **가능성 있음** — 필드 확인 필요 |
| `common sync --sources krx` | 지수 시세는 **가능**. 등락종목수·거래대금(`MDCSTAT01501`)은 **불명** |
| **`flows sync`** | **대체재 없음** — 투자자별 거래실적·공매도가 Open API에 없다 |

**`flows`가 가장 급하다.** 매일 38요청을 보내고 있고 갈 곳이 없다.
→ [`10_work_breakdown.md`](../10_work_breakdown.md) K-6
