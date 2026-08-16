# 인벤토리 — `data.krx.co.kr`을 치는 경로 전부

- 조사일: 2026-08-16
- 계기: KRX 접근 경로 교체(K 묶음)를 하려면 **무엇을 교체해야 하는지**가 먼저 있어야 한다.
- 방법: `grep -rl 'krx\.co\.kr' src/` + pykrx `stock_api.py` 소스에서 KRX/naver 분기 확인
  + FinanceDataReader 패키지 소스 확인 + Cronicle 스케줄 + `ingestion_runs.counts` 실측

---

## 0. 한 줄 결론

**KRX를 치는 경로는 5개고, 문은 3개다.**

그리고 **`universe sync`가 KRX 수집기였다** — FDR 라이브러리 안에서 우리 MDC client와
같은 엔드포인트를 친다. 이전 회계에 없던 항목이다.

매일 나가는 KRX 요청은 **약 53건**이다. 2026-08-16 차단을 부른 건 이쪽이 아니라
백필 두 개(N1 약 6,000 / N3 약 290)다.

---

## 1. 전체 목록

| 파이프라인 | 어댑터 | 문 | 일 요청 | 상태 |
|---|---|---|---:|---|
| `universe sync --source fdr` | FDR 라이브러리 내부 | **익명** | ~4 | 매일 18:30 |
| `flows sync` | `flows_krx` | MDC 로그인 | **38** | 매일 (체인) |
| `common sync --sources krx` | `common_features_krx` | MDC 로그인 | **11** | 매일 (체인) |
| `prices market-cap-backfill` (N1) | `market_cap_pykrx` | pykrx 로그인 | ~6,000 | **중단** |
| `universe backfill-snapshots` (N3) | `universe_pykrx` | pykrx 로그인 | ~290 | **중단** (60/152) |

일 요청은 `ingestion_runs.counts.requests_attempted` 실측(2026-08-15)이다.

---

## 2. 매일 도는 것 — 3개

### 2.1 `universe sync --source fdr` — **KRX 수집기다**

18:30 체인의 루트다. `adapters/universe_fdr/provider.py`가 `fdr.StockListing('KOSPI')`를
부르고, FDR 안에서 `KrxMarcapListingCache` → `KrxMarcapListing`으로 간다.

```
http://data.krx.co.kr/comm/bldAttendant/executeForResourceBundle.cmd   (bld/기준일 조회)
https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd               (bld=MDCSTAT01501)
```

**두 번째 URL은 우리 `krx_common/client.py`의 `KRX_MDC_URL`과 같은 주소다.**
시장당 2요청 × 2시장 = 4건.

주의할 점 셋:

- **우리 `HumanThrottle`이 안 걸린다.** 라이브러리 안에서 나가는 요청이다
- **로그인하지 않는다** — 익명. `KRX_ID`/`KRX_PW`와 무관한 세 번째 문이다
- UA도 FDR이 자체 지정한다 (`Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36`)

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

## 4. KRX처럼 보이지만 아닌 것

**`prices backfill`** — 매일 2,763종목을 도는 가장 큰 수집기인데 **KRX가 아니다.**

pykrx `stock_api.py`:

```python
if adjusted:
    df = naver.get_market_ohlcv_by_date(fromdate, todate, ticker)   # ← 우리 경로
else:
    df = krx.get_market_ohlcv_by_date(fromdate, todate, ticker, False)
```

`adjusted` 기본값이 `True`고 우리는 넘기지 않는다. **차단과 무관하고 지금도 정상이다.**

이 사실은 O-14에서 한 번 틀렸던 부분이라 근거를 남긴다.

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
| 쓰는 곳 | `flows` · `common krx` | N1 · N3 | `universe sync` |

---

## 7. 부수 발견 — 매일 N1 데이터를 받아서 버리고 있다

§2.1의 `KrxMarcapListing`이 파싱하는 필드다.

```python
numeric_cols = ['CMPPREVDD_PRC', 'FLUC_RT', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC',
                'ACC_TRDVOL', 'ACC_TRDVAL', 'MKTCAP', 'LIST_SHRS']
```

**`MKTCAP`(시가총액) · `LIST_SHRS`(상장주식수) · `ACC_TRDVAL`(거래대금) — N1이 받으려는 것 그대로다.**
매일 18:30에 받아서 티커·종목명만 쓰고 버린다.

`trdDd`가 `max_work_dt`(최근 거래일)로 고정돼 있어 **과거는 못 받는다** → 백필은 대체하지 못한다.
하지만 **앞으로의 일별 적재는 이미 손에 들어와 있다.**

### 7.1 K-2에 대한 간접 근거

응답 키가 **`OutBlock_1`**이다. KRX Open API 일별매매정보와 같다.

`MDCSTAT01501`은 전종목시세 통계고 Open API 일별매매정보는 그 통계의 API 형태다.
**필드가 같을 가능성이 한층 높아졌다** — 다만 여전히 정황이고,
확정은 [`krx_open_api.md`](krx_open_api.md) §4.1대로 개발명세서를 봐야 한다.

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
