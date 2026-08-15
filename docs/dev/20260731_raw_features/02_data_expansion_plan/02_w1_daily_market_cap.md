# 02. N1 — 일별 시가총액 · 거래대금 · 상장주식수 (1차)

- 작성일: 2026-08-15
- 공통 규약: [`01_implementation_checklist.md`](01_implementation_checklist.md)
- 원천: pykrx (KRX 웹) · 새 테이블 `daily_market_cap`
- 예상 규모: 호출 3–5천 · 행 8–13M · 1차 묶음 중 가장 큼

---

## 1. 왜 이걸 먼저 하나

**새 카테고리를 여는 게 아니다. 이미 A등급을 받은 피쳐가 근사값 위에 서 있는 문제다.**

| 피쳐 | 등급 | 지금 쓰는 값 | 문제 |
|---|---|---|---|
| `px_amihud_20d` | **A** (IC +0.1330, 25개 중 2위) | 분모 = `종가 × 거래량` | `daily_ohlcv`에 **거래대금 컬럼이 없다**(DDL 76~104행 확인) |
| `fin_log_mcap` | **A** (이번 검증에서 가장 단단) | `종가 × DART issued_shares` | 보고서 접수 때만 갱신되는 **계단형 근사** |
| `ev_net_share_issuance_yoy` | A(40–60일) | `isu_dcrs_stle` 문자열 매칭 | **I3 버그 — 감소가 한 번도 매칭 안 됨.** 순발행이 아니라 총발행 |

셋 다 KRX가 일별로 공식 제공한다. **N1 하나가 셋을 동시에 건드린다.**

---

## 2. 원천 확인 결과

`pykrx==1.2.8` 설치본 소스에서 확인했다.

```python
get_market_cap_by_ticker(date, market='ALL', acending=False, alternative=False)
# stock_api.py 반환 예시 — 컬럼이 5개다
#           종가         시가총액    거래량       거래대금   상장주식수
# 티커
# 005930   83000  495491951650000  38655276  3185356823460  5969782550
```

**세 가지가 계획을 바꾼다.**

**① 응답에 `종가`가 있다.** 처음 스키마에서 빠뜨렸다. 이 값이 있어야 §5·§6의 검증이 성립한다.

**② 응답에 시장 구분 컬럼이 없다.** index는 티커뿐이다. 즉 **`market='ALL'`로 받으면
`market` 컬럼을 채울 방법이 없다.** 현재 `stock_master`로 시장을 붙이면 **KOSDAQ→KOSPI
이전상장 종목에서 미래 정보가 섞인다** — 2016년 행에 2020년 이후에 확정된 시장을 붙이는 셈이다.

> **날짜마다 KOSPI와 KOSDAQ을 따로 호출한다.** 호출량이 약 3,000회가 아니라
> **약 6,000회**다.

**③ `daily_ohlcv`와 원천이 다르다.** `prices_pykrx/provider.py`가 부르는
`get_market_ohlcv_by_date`는 `adjusted=True`가 기본인데, 그 분기는 **KRX가 아니라 naver로
간다**(`stock_api.py`: `if adjusted: df = naver.get_market_ohlcv_by_date(...)`).

| 테이블 | 원천 | 가격 |
|---|---|---|
| `daily_ohlcv` | **naver** | **수정주가** |
| `daily_market_cap` (신규) | **KRX** | **당일 실제 종가(미수정)** |

이 사실이 §5 검증의 해석을 통째로 바꾼다. 처음에 "volume 불일치 = 조정 정책 불일치"라고
적었는데 **틀렸다.** volume 차이는 KRX와 naver의 원천 차이로 봐야 하고, 조정 여부는
**두 종가의 비율**로 본다(§6).

> **미검증.** 이번 세션에서 실호출이 네트워크 차단으로 실패했다. 위는 설치본 소스 기준이다.
> 결측 처리는 **PoC로 반드시 확인**해야 한다 — `wrap.py`가 빈 값을 `0`으로 바꾸고
> `np.int64`로 캐스팅하므로(`df.replace("", 0); df.astype(np.int64)`) **진짜 0과 결측이
> 구분되지 않는다.**

---

## 3. 스키마

```sql
-- sql/postgres_ddl.sql
CREATE TABLE IF NOT EXISTS daily_market_cap (
    trade_date      DATE        NOT NULL,
    ticker          TEXT        NOT NULL,
    market          TEXT        NOT NULL,   -- 시장별 호출로 채운다 (§2 ②)
    source_close    BIGINT,                 -- ★ KRX 당일 실제 종가 (미수정)
    market_cap      BIGINT,                 -- 시가총액 (KRW)
    trading_value   BIGINT,                 -- 거래대금 (KRW)
    listed_shares   BIGINT,                 -- 상장주식수
    volume          BIGINT,                 -- KRX 기준 거래량 (naver 기준 daily_ohlcv와 대조용)
    source          TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (trade_date, ticker, market)
);

CREATE INDEX IF NOT EXISTS ix_daily_market_cap_ticker_date
    ON daily_market_cap (ticker, market, trade_date DESC);

CREATE INDEX IF NOT EXISTS ix_daily_market_cap_sync_cursor
    ON daily_market_cap (fetched_at, trade_date, ticker, market);
```

**설계 판단 세 가지.**

1. **`source_close`를 반드시 저장한다.** 이게 있어야 ① 같은 응답 안에서
   `market_cap = source_close × listed_shares` 항등성을 검증할 수 있고, ② 수정주가
   `daily_ohlcv.close`와의 비율로 **corporate action 조정을 직접 확인**할 수 있다(§6).
   이름을 `close`가 아니라 `source_close`로 둔 것은 `daily_ohlcv.close`(수정주가)와
   **의미가 다르다는 것을 이름에서 드러내기 위해서**다.
2. **`daily_ohlcv`에 컬럼을 붙이지 않는다.** 원천이 다르고(KRX vs naver) 백필 시점도 다르다.
   컬럼을 붙이면 기존 6.6M행 전체가 NULL인 채로 남고, 백필 진행률을 알 수 없다.
3. **`volume`을 중복 저장한다.** 두 원천의 거래량을 대조하는 유일한 수단이다.

**결측 정책은 PoC 뒤에 확정한다.** pykrx가 빈 값을 `0`으로 바꾸고 `int64`로 캐스팅하므로
**진짜 0(거래정지)과 결측이 구분되지 않는다.** 어댑터에서 일괄 `0 → NULL`로 바꾸면 거래정지일의
진짜 0을 잃고, 그대로 두면 결측을 0으로 학습한다.

> 잠정 방침: **`source_close == 0`이면 그 행 전체를 결측으로 본다.** 종가가 0인 정상
> 거래일은 없기 때문이다. 종가가 정상인데 `trading_value == 0`이면 그건 진짜 0(거래정지)이다.
> **PoC에서 휴장일·거래정지일 응답으로 확인한 뒤 고정한다.**

**익스포터 등록**은 `date_month` 전략이다 — `daily_ohlcv`와 같은 모양.

```toml
# tools/raw-parquet-exporter/config/export_tables.toml
[[tables]]
name = "daily_market_cap"
priority = "P0"
extract_strategy = "date_month"
date_column = "trade_date"
output_partitions = ["year(trade_date)", "month(trade_date)"]
order_by = ["trade_date", "ticker", "market"]
```

`bin/raw-parquet-export-all.sh`에서는 `date_month_tables` 배열에 넣는다.

---

## 4. 작업 순서

### N1-PR1 — PoC (선행, 코드 커밋 없음)

```python
# 확인 항목 — 시장별로 따로 호출한다 (§2 ②)
get_market_cap_by_ticker('20240102', market='KOSPI')
get_market_cap_by_ticker('20240102', market='KOSDAQ')
get_market_cap_by_ticker('20140602', market='KOSPI')     # 과거 구간도 나오는지
get_market_cap_by_ticker('20240101', market='KOSPI')     # 휴장일 응답 모양
get_market_cap_by_date('20240102','20240131','005930')   # by_date 폴백 확인

# 조정주가 확인용 — 같은 종목·날짜를 두 경로로
get_market_cap_by_ticker(...)                # KRX 미수정 종가
get_market_ohlcv_by_date(..., adjusted=True) # naver 수정주가
```

**판정 세 가지.**

1. **5개 컬럼(종가 포함)이 전종목으로 오는가** → 날짜×시장 루프 채택 (호출 ~6,000)
   - 아니면 종목 루프 폴백 (호출 ~2,800 × 기간 청크). **비용이 10배 이상 뛰므로 범위를
     2014-06 이후로 자른다**
2. **결측·휴장일 응답 모양** → §3의 잠정 결측 방침(`source_close == 0` → 행 결측)이 맞는지 확정
3. **KOSPI/KOSDAQ 합이 `ALL`과 같은가** → 다르면 KONEX·ETF 등이 섞이는 것이므로 필터 정책 필요

산출물: `poc/n1_pykrx_market_cap.md` — 응답 샘플, 컬럼명, 결측 패턴, 소요 시간, 스로틀 한계.

### N1-PR2 — 스키마 + 등록 6곳

`01_implementation_checklist.md` §1 그대로. 이 PR만으로 `db init` → `db sync-remote` →
`raw-parquet-export-all.sh`가 빈 테이블을 끝까지 통과해야 한다. **데이터 없이 배관을 먼저
뚫는다.**

### N1-PR3 — 도메인 + 포트 + 어댑터

```
domain/models.py     DailyMarketCapRow, DailyMarketCapResult, MarketCapSyncResult
domain/enums.py      RunType.MARKET_CAP_BACKFILL = "market_cap_backfill"
ports/market_cap.py  MarketCapProvider(Protocol)
                       fetch_by_date(trade_date, market) -> DailyMarketCapResult
adapters/market_cap_pykrx/provider.py
```

어댑터는 `adapters/prices_pykrx/provider.py`와 같은 모양이다 —
`get_pykrx_stock_module()`, try/except로 감싸 `result.error`에 담고 예외를 밖으로 던지지 않는다.

**파싱 주의.** PoC(§4 N1-PR1 판정 2)에서 확정한 결측 정책을 여기서 코드로 고정한다.
`market='ALL'`을 쓰지 않으므로 `market`은 **호출 인자에서 온다** — 응답에서 유도하거나
`stock_master`에서 조인하지 않는다(§2 ②).

### N1-PR4 — 스토리지

```
ports/storage.py    upsert_daily_market_cap(rows) -> UpsertResult
                    get_completed_market_cap_slices(start, end) -> set[tuple[date, Market]]
infra/db_postgres/repositories.py   execute_values + ON CONFLICT DO UPDATE
```

**skip 규칙 — "행이 하나라도 있으면 완료"로 하지 않는다**(`01` §2.4). 응답 일부만 저장된 뒤
중단되면 그 슬라이스가 영구히 불완전하게 남는다.

- 한 `(trade_date, market)` 응답의 **전체 행을 한 트랜잭션으로** upsert한다. 배치를 쪼개지 않는다
- 응답 행 수와 저장 행 수를 대조하고, 다르면 슬라이스를 완료로 치지 않는다
- 완료 판정은 **행 존재 여부가 아니라 슬라이스 상태**로 한다. 단일 run으로 끝나는 작업이라
  `collection_slice_state` 전용 테이블까지는 필요 없지만, **최소한 기대 행 수와 실제 행 수를
  `ingestion_runs.counts`에 남긴다**
- `--force`로 skip을 끌 수 있게 한다

### N1-PR5 — 서비스 + CLI

```
service/backfill_market_cap.py    backfill_market_cap(provider, storage, start, end, markets, ...)
cli/app.py                        prices market-cap-backfill
```

CLI 인자는 `prices backfill`과 이름을 맞춘다 — `--market`, `--start`, `--end`,
`--rate-limit-seconds`, `--long-rest-interval`, `--long-rest-seconds`, `--force`.
거래일 목록은 **`infra/calendar/`의 KRX 거래일 캘린더**를 쓴다 (휴일에 호출하지 않는다).

### N1-PR6 — 테스트

- `tests/unit/test_backfill_market_cap.py` — 커버된 날짜 skip, 부분 실패 시 `partial`,
  0/결측 정책, 거래일만 호출
- `tests/unit/test_market_cap_pykrx_provider.py` — 가짜 DataFrame 파싱
- `tests/integration/test_market_cap_live.py` — `RUN_LIVE_PYKRX_TEST=1` 게이트

### N1-PR7 — 백필 실행

```bash
# 최근 구간 먼저 (검증이 쉽다)
uv run krx-collector prices market-cap-backfill --start 2024-01-01 --end 2026-08-15

# 검증 통과 후 전체
uv run krx-collector prices market-cap-backfill --start 2014-06-02 --end 2023-12-31
```

**2014-06-02를 시작으로 잡은 이유**는 `02_feature_candidate.md` §2.2다 — KOSPI OHLCV가
2014-01-20 시작이라 그 이전은 KOSDAQ 편향이고, 공통 피쳐도 2014-06부터다. 2007까지 내려가는 건
KOSDAQ 단독 구간이라 **N1의 우선 범위가 아니다.**

---

## 5. 수집 후 검증 (필수)

**여기가 이 작업 패키지의 핵심 산출물이다.** 값을 받는 것보다 대조가 중요하다.

| # | 검증 | 기대 | 어긋나면 |
|---|---|---|---|
| **V1** | **`market_cap` vs `source_close × listed_shares`** | 반올림 오차 내 | **같은 응답 안의 항등성이다.** 어긋나면 시총 정의가 다르다(우선주·해외DR 포함 여부) |
| **V2** | **`source_close`(KRX 미수정) vs `daily_ohlcv.close`(naver 수정)의 비율 시계열** | 종목별로 계단형 | **corporate action 조정 검증.** §6 |
| V3 | `daily_market_cap.volume` vs `daily_ohlcv.volume` 불일치율 | 대체로 일치 | **조정 정책이 아니라 KRX vs naver 원천 차이다.** 불일치 종목·구간을 목록화만 한다 |
| V4 | `listed_shares` 급변일 ↔ `dart_capital_change_raw` 이벤트 | 대응돼야 함 | 한쪽이 놓치고 있다 |
| V5 | `종가×거래량` vs 실제 `거래대금`의 일별 rank 상관 | 높을 것으로 예상 | **낮으면 `px_amihud_20d` 재검정이 필요하다** |
| V6 | 일별 커버리지(종목 수) 시계열, 시장별 | 완만한 증가 | 급락 구간은 수집 실패 |
| V7 | `listed_shares` vs DART `issued_shares` 시계열 | 계단 대 연속 | 차이 크기가 `fin_log_mcap` 개선폭의 상한 |
| V8 | 시장 이전상장 종목 수 — 같은 ticker의 `market`이 바뀐 건수 | — | **`stock_master` 조인이 만들었을 룩어헤드의 크기**(§2 ②) |

**V1·V2가 새로 들어온 핵심이다.** 처음 계획은 `source_close`를 저장하지 않아 둘 다 불가능했다.

V5는 판단이 갈리는 지점이다. **상관이 매우 높게 나오면 `px_amihud_20d`를 다시 돌릴 이유가
약해진다.** 반대로 낮으면 A등급 하나가 근사 오차 위에 서 있었다는 뜻이다. **어느 쪽이든 값이
있는 측정이고, 피쳐를 바꾸기 전에 이 숫자를 먼저 본다.**

산출물: `poc/n1_validation.md` — V1~V8 실측치.

---

## 6. 곁다리로 확인할 것 — 조정주가와 두 원천

`prices_pykrx/provider.py`는 `get_market_ohlcv_by_date(start, end, ticker)`를 호출하고,
그 함수는 **`adjusted=True`가 기본이며 그 분기는 KRX가 아니라 naver로 간다**(`stock_api.py`).

```python
if adjusted:
    df = naver.get_market_ohlcv_by_date(fromdate, todate, ticker)
else:
    df = krx.get_market_ohlcv_by_date(fromdate, todate, ticker, False)
```

즉 현재 `daily_ohlcv`는 **naver 수정주가**다. `02_feature_candidate.md` §2.3이 Phase 1 진입
조건으로 걸어둔 조정주가 요건은 **이미 충족돼 있을 가능성이 높다.** 다만 확인된 적이 없다.

**N1이 그 확인을 가능하게 한다.** `source_close`(KRX 미수정) ÷ `daily_ohlcv.close`(naver 수정)의
시계열을 종목별로 그리면 된다.

- 비율이 **계단형으로 바뀌고 그 시점이 `listed_shares` 급변일과 일치**하면 → 조정이
  제대로 되고 있다. `02` §2.3 항목을 닫는다
- 비율이 **1에서 안 움직이면** → 조정이 안 되고 있다. **모멘텀·MAX·변동성 피쳐 전부가
  영향권**이고 별도 작업이 필요하다

**두 원천이 다르다는 사실 자체도 기록해야 한다.** `daily_ohlcv`(naver)와
`daily_market_cap`(KRX)의 거래량이 어긋나는 종목·구간이 있으면, 그건 조정 정책 문제가 아니라
**원천 차이**다. V3에서 목록화하고, 어느 쪽을 정본으로 볼지 정한다.

**이건 수집이 아니라 확인 작업이다.** N1-PR7 백필 직후 V2·V3와 함께 한 번에 본다.

---

## 7. 결과 보기 전에 고정할 것

`01_implementation_checklist.md` §6 규율이다.

1. **기존 피쳐의 분모를 교체할지, 별도 variant로 둘지.**
   - 교체 → 발행된 Phase A/B run의 **재현 경로가 깨진다**
   - variant 추가 → BH 모집단이 늘어 기존 발견의 문턱이 올라간다
   - **권고: variant로 추가하고, 기존 셀과 나란히 놓고 IC 차이를 본다.** 차이가 작으면 교체할
     이유가 없고, 크면 새 config로 사전등록해 정식 검정한다
2. **`ev_net_share_issuance_yoy`를 상장주식수 차분으로 대체할지.**
   - **권고: 대체하지 않고 독립 교차검증 축으로 쓴다.** 상장주식수 차분에는 액면분할·무상증자
     같은 기계적 변경이 섞이므로, 경제적 발행을 갈라내려면 `dart_capital_change_raw`와
     결합해야 한다. 다만 I3 버그(감소 미매칭)를 잡는 **정답지** 역할은 즉시 가능하다
3. **회전율 정의.** `거래대금 / 시가총액`으로 갈지 기존 `거래량 / issued_shares`를 유지할지.
   `02` P7이 "raw turnover level 단독 입력 금지, 변화율만"이라고 이미 못박았으므로 그 선은 유지한다

---

## 8. 리스크

| 리스크 | 대응 |
|---|---|
| pykrx가 KRX 웹을 긁는 비공식 경로 — 차단·스로틀 | `prices backfill`과 같은 `HumanThrottlePolicy`. 야간 배치 |
| **호출량이 추정의 2배(약 6,000회)** | 시장 구분 컬럼이 없어 시장별 호출이 강제된다(§2 ②). 일정에 반영 |
| **pykrx가 결측을 0으로 캐스팅** | §3 잠정 방침 → PoC에서 확정. 일괄 `0 → NULL`은 거래정지일의 진짜 0을 잃는다 |
| `by_ticker` 응답이 소스와 다름 | **PoC(N1-PR1)에서 확정.** 다르면 `by_date` 폴백 + 범위 축소 |
| 과거 구간 응답 없음 | PoC에서 2014-06 시점 확인. 없으면 시작일을 뒤로 민다 |
| KRX 계열 잡과 시간 충돌 (exit 75 lock) | 기존 `sdc_daily_krx_*` 스케줄과 겹치지 않게 배치 |
| 8–13M행 추가로 `db sync-remote` 시간 증가 | `date_month` 증분 커서가 있어 전체 재전송은 아니다. 최초 1회만 길다 |

---

## 9. 완료 기준

공통 DoD(`01` §7)에 더해:

- [ ] PoC 문서 `poc/n1_pykrx_market_cap.md` 존재
- [ ] 2014-06-02 ~ 최신 구간 백필 완료, 결측 구간 목록 문서화
- [ ] 검증 V1~V8 실측치가 `poc/n1_validation.md`에 기록
- [ ] `market`이 **호출 인자에서만** 채워졌음을 코드로 확인 (`stock_master` 조인 없음)
- [ ] **조정주가 확인 결과가 문서에 기록**(§6) — 이건 `02_feature_candidate.md` §2.3의 미해결
      항목을 닫는 작업이다
- [ ] §7의 세 결정이 문서에 고정됨 (결과를 보기 **전에**)
