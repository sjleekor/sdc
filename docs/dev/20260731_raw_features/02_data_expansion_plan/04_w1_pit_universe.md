# 04. N3 — PIT 유니버스 (과거 시점 상장 종목) (1차)

- 작성일: 2026-08-15
- 공통 규약: [`01_implementation_checklist.md`](01_implementation_checklist.md)
- 원천: pykrx `get_market_ticker_list(date, market)` · **새 테이블 없음**
- 예상 규모: 호출 **약 290회.** 이 계획에서 가장 싸다

---

## 1. 왜

생존편향이다. `stock_master`의 DELISTED가 **28개뿐**이다
(`00_raw_feature_inventory.md` §2.7).

**원인을 코드에서 확인했다.** `service/sync_universe.py` 95~125행이다.

```python
for ticker in existing_stocks_map:
    if ticker not in snapshot_tickers:
        delisted_tickers.append(ticker)
```

**스냅샷 간 diff로만 상폐를 잡는다.** 즉 스냅샷 수집을 시작한 이후의 상폐만 기록된다.
그 이전 12년치 상폐 종목은 표본에 아예 없다. 구조적 결함이지 버그가 아니다 — 설계상
그 이전을 알 방법이 없었다.

**지금 이게 왜 급한가.** [`11_feature_taxonomy.md`](../01_feature_candidate/11_feature_taxonomy.md)
§5.5에서 지적했듯 **전이·부실·생애주기 Decline 피쳐는 생존편향에 직격**당한다.

> 적자 기업 중 상장폐지된 쪽이 표본에서 빠지면 "적자에서 흑자로 돌아선 기업"만 남는다.
> `fin_turn_to_profit`의 성과가 구조적으로 부풀려진다.

즉 **N3는 새 피쳐를 여는 게 아니라, 2차 우선순위인 전이·재무위험 축의 선행 조건**이다.
N3 없이 그 축의 결론을 내면 안 된다.

---

## 2. 원천 확인 결과

```python
get_market_ticker_list(date: str = None, market: str = 'KOSPI') -> list
# docstring: date (str, optional): 조회 일자 (YYYYMMDD)
#            market: KOSPI/KOSDAQ/KONEX/ALL
```

**`date` 인자를 받는다.** 과거 임의 시점의 상장 종목 목록을 그대로 준다.
`get_market_ticker_name(ticker)`로 종목명도 받을 수 있다.

> **미검증.** 실호출은 네트워크 차단으로 실패했다. **PoC에서 2014·2016·2020 시점 응답을
> 반드시 확인**한다. 과거 구간이 안 나오면 이 작업 패키지 전체가 성립하지 않는다.

---

## 3. 저장 — 새 테이블이 필요 없다

**기존 테이블을 그대로 쓴다.** `stock_master_snapshot` + `stock_master_snapshot_items`가
이미 이 목적으로 설계돼 있다. DDL 주석이 그렇게 적혀 있다.

> Recommended for full auditability: you can diff any two snapshots to find
> new listings, delistings, or name changes.

현재 스냅샷 55회 / items 150,404행이 있다. **`as_of_date`를 과거로 두고 같은 테이블에
백필**하면 된다.

```sql
-- 기존 스키마 그대로. 손댈 것 없음.
stock_master_snapshot(snapshot_id, as_of_date, source, fetched_at, record_count)
stock_master_snapshot_items(snapshot_id, ticker, market, name, status, listing_date)
```

**등록 6곳도 손댈 게 없다.** 두 테이블 다 이미 `RAW_TABLES`·`export_tables.toml`·
`remote_sync`·profiling catalog에 등록돼 있다. **이 계획에서 유일하게 배관 작업이 0인 항목이다.**

### 3.1 그런데 기존 repository는 그대로 못 쓴다

**처음 계획이 여기를 놓쳤다.** `repositories.py`의 `upsert_stock_master()`를 확인해보니
한 메서드 안에서 **세 가지를 다 한다.**

```
1. INSERT INTO stock_master_snapshot        (스냅샷 메타)
2. INSERT INTO stock_master_snapshot_items  (구성 종목)
3. Upsert stock_master                      ← 이게 문제다
```

즉 이 메서드를 부르면 **`stock_master`가 반드시 같이 갱신된다.** 백필 스냅샷으로 부르면
과거 시점 목록이 현재 상태를 덮어써서 **`stock_master`가 오염된다.**

**새로 필요한 것 세 가지.**

| # | 필요한 것 | 위치 |
|---|---|---|
| 1 | `insert_stock_master_snapshot_only(stocks, snapshot)` | `ports/storage.py` + `repositories.py` |
| 2 | `(as_of_date, source)` 중복 방지 | 위 메서드 안. 같은 키 스냅샷이 있으면 재삽입하지 않는다 |
| 3 | **`Source.PYKRX_BACKFILL` enum 값** | `domain/enums.py` — **현재 enum에 없다**(FDR/PYKRX/OPENDART/KRX/ECOS/FRED/KOSIS/CUSTOMS/KITA/NASDAQ_DATA_LINK) |

3번은 사소해 보이지만 빠뜨리면 `snapshot.source.value`에서 죽는다.

**`stock_master`는 절대 건드리지 않는다.** items만 쌓고, 유니버스 판정은 읽는 쪽(마트)이 한다.

---

## 3.5. 역할을 축소한다 — 월말 스냅샷은 "정확한 PIT 유니버스"가 아니다

**처음 계획이 과대평가했다.** 월말 스냅샷에는 구조적 구멍이 둘 있다.

- **한 달 안에 상장했다가 상폐된 종목을 아예 못 잡는다** (스팩 합병·관리종목 급속 퇴출)
- **월중 신규상장은 다음 월말까지 유니버스에서 빠진다** — 최대 한 달 지연

그런데 **[N1](02_w1_daily_market_cap.md)이 날짜×시장으로 전종목을 받는다.** 즉
`daily_market_cap`의 `(trade_date, ticker)` 집합 자체가 **일별 상장 종목 목록**이다.
월말 스냅샷보다 해상도가 30배 높고, 별도 수집도 필요 없다.

> **따라서 역할을 이렇게 나눈다.**
>
> | 용도 | 담당 |
> |---|---|
> | **일별 PIT 유니버스 (마트가 실제로 쓰는 것)** | **N1 행** |
> | 감사·교차검증, 종목명·상태 이력 | N3 월말 스냅샷 |
>
> **단, N1 응답이 상장 종목을 빠짐없이 덮는다는 전제**가 필요하다. 거래정지 종목이
> 응답에서 빠지면 유니버스에 구멍이 생긴다. **N3-PR1 PoC에서 같은 날짜의
> `get_market_ticker_list` 결과와 `get_market_cap_by_ticker` 행 집합을 대조**해
> 이 전제를 검증한다. 어긋나면 N3가 다시 주 경로가 된다.

이 축소로 N3의 비용은 그대로(약 290 호출)인데 **기대 역할이 줄고 리스크도 준다.**

---

## 3.6. 전제 검증 결과 (2026-08-19) — **통과. 안 1로 간다**

§3.5가 남긴 조건은 하나였다. "**N1 응답이 상장 종목을 빠짐없이 덮는다는 전제**가 필요하다.
거래정지 종목이 응답에서 빠지면 유니버스에 구멍이 생긴다."

N1-8 백필(2014-06-02~2026-08-18, 7,060,600행)이 끝나서 **호출 없이 DB만으로 검증했다.**
원래 계획했던 `get_market_ticker_list` 대조는 pykrx라 더 이상 쓸 수 없고, 쓸 필요도 없었다 —
`daily_ohlcv`가 같은 역할을 한다. 연도별 한 세션씩 13개를 골라 `(trade_date, ticker)` 집합을
맞춰봤다.

| 세션 | `daily_ohlcv`에만 | `daily_market_cap`에만 |
|---|---|---|
| 2014-06-30 | 20 | 324 |
| 2016-06-30 | 37 | 286 |
| 2020-06-30 | 32 | 203 |
| 2023-06-30 | 9 | 174 |
| 2026-06-30 | **0** | **0** |

**`daily_ohlcv`에만 있는 종목은 전부 KONEX 시절 이력이다.** 2016-06-30의 37개를 하나씩
확인했더니 전부 KOSDAQ 이전상장 종목이었고, `daily_market_cap`의 첫 등장일이 정확히
이전상장일이었다.

| 종목 | `daily_market_cap` 첫 등장 | `daily_ohlcv` 첫 등장 |
|---|---|---|
| 엠로 (058970) | 2021-08-13 | 2016-04-28 |
| 툴젠 (199800) | 2021-12-10 | 2014-06-25 |
| 그린플러스 (186230) | 2019-08-07 | 2014-01-20 |
| 비나텍 (126340) | 2020-09-23 | 2014-01-20 |

KONEX 종목이 하나씩 코스닥으로 옮겨가면서 차이가 0으로 수렴한다(2026년 0건). N1은
`sto/stk_bydd_trd` + 코스닥 엔드포인트만 부르고 **KONEX는 안 부른다.** 이 프로젝트 범위가
KOSPI/KOSDAQ이므로 이건 구멍이 아니라 범위다.

**따라서 §3.5 표의 전제는 성립하고, §6의 안 1(일별 `daily_market_cap` 유니버스)로 간다.**
N3 월말 백필은 2019~2025가 비어 있지만 **재개하지 않는다.**

> **덤으로 나온 것 — `daily_ohlcv`에 KONEX 시절 행이 섞여 있다.**
> 지금 마트가 `daily_ohlcv`를 유니버스로 쓰면 유동성 특성이 전혀 다른 시장 구간을 조용히
> 포함한다. 안 1로 옮기면 생존편향과 함께 이것도 같이 빠진다 — **안 1의 이득이 §3.5에
> 적어둔 것보다 하나 많다.**
>
> 반대 방향(`daily_market_cap`에만 있는 종목)이 초기 324개인 것이 생존편향의 크기다.
> `daily_ohlcv`가 상폐 종목을 그만큼 놓치고 있었다.

## 4. 수집 해상도 — 월말이면 충분하다

매 거래일까지 갈 필요가 없다. 상폐·신규상장은 월 단위 해상도로 충분하고, 호출이 20분의 1이 된다.

```text
for d in 월말 거래일 (2014-06 ~ 현재 ≈ 145개):
    for m in [KOSPI, KOSDAQ]:
        get_market_ticker_list(d, market=m)
```

**호출 약 290회.** 거래일 캘린더(`infra/calendar/`)로 그 달의 마지막 거래일을 구한다.

**해상도를 높일지는 나중에 정한다.** 라벨 horizon이 최대 120일이라 월말 해상도면 유니버스
필터로 충분하다. 상폐 **시점**을 정밀하게 써야 하는 피쳐(부실 예측 라벨 등)를 만들 때
주간·일간으로 조밀화한다. 지금은 하지 않는다.

---

## 5. 작업 순서

### N3-PR1 — PoC (호출 10건 미만) → **완료 (2026-08-19), 호출 0건**

결과는 §3.6. pykrx가 닫혀서 아래 계획대로는 못 했고, N1-8 백필이 끝난 덕에
`daily_market_cap` 대 `daily_ohlcv` 대조로 **호출 없이** 같은 질문에 답했다.

<details><summary>원래 계획</summary>


```python
get_market_ticker_list('20140630', market='KOSPI')   # 과거 구간 나오는가
get_market_ticker_list('20160630', market='KOSDAQ')
get_market_ticker_list('20200331', market='KOSPI')   # 공매도 금지 직후
get_market_ticker_list('20260731', market='ALL')     # 현재와 대조
```

</details>

**확인할 것.**

- 과거 시점 응답 여부 (**이게 실패하면 작업 중단**)
- **★ `get_market_ticker_list` 결과 vs 같은 날 `get_market_cap_by_ticker` 행 집합의 차집합**
  — §3.5의 전제 검증. 차이가 크면 N1 행을 유니버스로 못 쓴다
- 종목 수가 그 시점 KRX 공표치와 맞는가
- `market='ALL'`이 KOSPI/KOSDAQ 구분을 주는가 — 안 주면 시장별로 따로 호출
- 우선주·리츠·스팩이 섞이는가 (필터 정책 필요)
- 종목명 조회 비용 (종목당 1회면 호출이 폭증한다 → 이름 없이 갈지 결정)

산출물: `poc/n3_pit_universe.md`.

### N3-PR2 — 서비스 + CLI

새 테이블은 필요 없지만 **새 storage 메서드는 필요하다**(§3.1).

```
domain/enums.py     Source.PYKRX_BACKFILL = "PYKRX_BACKFILL"      ← 신규
                    RunType.UNIVERSE_SNAPSHOT_BACKFILL = "universe_snapshot_backfill"
ports/storage.py    insert_stock_master_snapshot_only(stocks, snapshot) -> UpsertResult
                    get_existing_snapshot_dates(source) -> set[date]
infra/db_postgres/repositories.py   위 메서드 구현 — stock_master를 건드리지 않는다
service/backfill_universe_snapshots.py
cli/app.py:  universe backfill-snapshots --start 2014-06 --end 2026-08 [--freq month] [--force]
```

skip-if-present 키는 **`(as_of_date, source)`**다. 같은 날짜 스냅샷이 이미 있으면 건너뛴다.

**종목명 처리.** `stock_master_snapshot_items.name`이 NOT NULL이다. 이름을 종목당 조회하면
호출이 폭증하므로, **PoC 결과에 따라 둘 중 하나**를 고른다.

- `get_market_ticker_name`이 캐시돼 싸면 → 조회
- 비싸면 → **현재 `stock_master`의 이름으로 채우고, 없으면 `ticker`를 그대로 넣는다.**
  이름은 이 작업의 목적이 아니다. 목적은 **그 날짜에 상장돼 있었는가**뿐이다

### N3-PR3 — 테스트

- `tests/unit/test_backfill_universe_snapshots.py` — 월말 거래일 산출, 기존 스냅샷 skip,
  `source` 구분
- **`insert_stock_master_snapshot_only`가 `stock_master`를 갱신하지 않는다**는 것을
  명시적으로 단언하는 테스트. §3.1의 함정이 여기서 잡혀야 한다
- **기존 `sync_universe` 경로가 백필 스냅샷을 diff 대상으로 삼지 않는지 회귀 테스트**
  — 이게 이 PR에서 가장 위험한 부분이다(§7)

### N3-PR4 — 실행

```bash
uv run krx-collector universe backfill-snapshots --start 2014-06-01 --end 2026-08-15
```

### N3-PR5 — 상폐 종목 데이터 실태 조사

**수집이 아니라 조사다. 그리고 이게 이 작업의 진짜 결론이다.**

목록이 복원돼도 **상폐 직전 구간의 가격·수급 데이터가 없으면 편향은 안 없어진다.**

```sql
-- 백필 스냅샷에는 있는데 현재 stock_master에 ACTIVE로 없는 종목 = 상폐 추정
-- 그중 daily_ohlcv에 가격이 있는 비율은?
```

| 측정 | 의미 |
|---|---|
| 상폐 추정 종목 수 (연도별) | KRX 공표 상폐 건수와 대조 |
| 그중 `daily_ohlcv`에 가격이 있는 비율 | **이게 낮으면 편향이 여전히 남는다** |
| `krx_security_flow_raw` 커버 비율 | 수급 피쳐의 편향 범위 |
| 상폐 직전 60거래일 데이터 보유율 | 전이·부실 피쳐에 직접 걸린다 |

**이 조사 결과에 따라 후속이 갈린다.**

- 가격이 대부분 있으면 → 유니버스 필터만 바꾸면 끝. 후속 백필 불필요
- 없으면 → **상폐 종목 가격 백필**이 별도 작업으로 필요하다. pykrx가 상폐 종목 OHLCV를 주는지
  부터 확인해야 한다. **이 문서는 거기까지 계획하지 않는다** — 조사 결과를 보고 판단한다

---

## 6. 마트 반영

수집만으로는 아무것도 안 바뀐다. 읽는 쪽을 바꿔야 한다.

현재 마트의 유니버스 필터는 사실상 `stock_master.status = ACTIVE`다. **§3.5 판정에 따라
둘 중 하나로 바꾼다.**

```sql
-- 안 1 (권장, N1 전제가 검증되면) — 일별
universe(trade_date, ticker) = daily_market_cap에 그 (trade_date, ticker) 행이 있다

-- 안 2 (N1 커버리지가 부족하면) — 월말 근사
universe(trade_date, ticker) =
    그 trade_date 이전 가장 가까운 as_of_date 스냅샷의 items에 포함
```

**안 1이 나은 이유는 해상도만이 아니다.** 안 2는 월중 신규상장을 최대 한 달 누락하고,
그 누락이 **신규상장 종목에 체계적으로 몰린다** — 편향을 고치려다 다른 편향을 넣는 셈이다.

**기존 경로를 덮어쓰지 않고 variant로 만든다.** 발행된 Phase A/B run의 재현 경로가 깨지면
안 된다. 두 유니버스로 같은 피쳐를 돌려 **IC 차이를 측정하는 것 자체가 생존편향의 크기
측정**이다.

---

## 7. 리스크

| 리스크 | 대응 |
|---|---|
| **백필 스냅샷이 기존 `sync_universe` diff를 오염** | `source` 구분 + `stock_master` 미갱신 + 회귀 테스트(N3-PR3) |
| **기존 `upsert_stock_master`를 그대로 쓰면 `stock_master`가 오염된다** | §3.1 — 전용 메서드를 새로 만든다. 이걸 놓치면 현재 유니버스가 깨진다 |
| 월말 해상도의 구조적 구멍 | §3.5 — 일별 유니버스는 N1이 담당. N3는 감사용 |
| 과거 구간 응답 없음 | PoC에서 확인. 실패 시 작업 중단하고 대안(FDR·KRX 상장법인목록) 재검토 |
| 우선주·리츠·스팩 혼입 | PoC에서 확인 후 필터 정책 고정. 현재 유니버스 정의와 일치시킨다 |
| 상폐 종목 가격이 없음 | **높은 확률로 실제 상황이다.** N3-PR5가 이걸 측정하는 게 목적 |
| `record_count` 불일치 | items 행 수와 맞추는 검증 추가 |

**가장 큰 리스크는 마지막 두 번째다.** 목록만 복원하고 "생존편향을 해결했다"고 쓰면 안 된다.
**목록 복원은 편향의 크기를 측정할 수 있게 해줄 뿐이고, 해소는 가격 데이터가 있어야 된다.**
N3-PR5를 완료 기준에 넣은 이유다.

---

## 8. 완료 기준

공통 DoD(`01` §7) 중 등록 6곳은 해당 없음(§3). 나머지에 더해:

- [ ] `poc/n3_pit_universe.md` — 과거 구간 응답 확인 + **N1 행 집합과의 차집합**(§3.5 전제)
- [ ] 2014-06 ~ 현재 월말 스냅샷 백필 완료, 스냅샷별 종목 수 시계열 기록
- [ ] 연도별 상폐 추정 건수 vs KRX 공표치 대조표
- [ ] **N3-PR5 실태 조사 결과 문서화** — 상폐 종목의 가격·수급 데이터 보유율
- [ ] 기존 `universe sync` 회귀 테스트 통과 (백필 스냅샷이 diff를 오염시키지 않음)
- [ ] **§3.5 판정 기록** — 일별 유니버스를 N1 행으로 쓸 수 있는가. 못 쓰면 N3 해상도를 올린다
- [ ] 마트에 PIT 유니버스 variant 추가 (기존 경로 유지)
- [ ] **후속 판단 기록**: 상폐 종목 가격 백필이 필요한지, 필요하면 별도 작업 문서 생성
