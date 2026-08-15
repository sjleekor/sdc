# 00. 신규 수집 계획 — 무엇을 더 모아야 하나

- 작성일: 2026-08-15 (개정: 2026-08-15, 코드·API 재검증 반영 — §10)
- 선행 문서: [`01_feature_candidate/11_feature_taxonomy.md`](../01_feature_candidate/11_feature_taxonomy.md)
  (카테고리 체계와 빈 칸), [`00_raw_feature_inventory.md`](../00_raw_feature_inventory.md) (현재 원천 전수조사)
- **범위: 신규 수집만 다룬다.** 이미 있는 원천으로 계산만 하면 되는 것(레버리지, 생애주기,
  계절성, 재무 Δ·전이)은 §8에 포인터만 두고 여기서 다루지 않는다.
- 이 문서는 **수집 계획이지 피쳐 계약이 아니다.** 수집한 원천으로 피쳐를 만들어 검정하려면
  `01_feature_candidate/02` §6.2 규율대로 **새 config로 따로 사전등록**해야 한다(§9).

---

## 1. 한 장 요약

수집 후보를 9개로 좁혔다. **비용이 낮은 순이 아니라 효과 순**으로 적었다.

| # | 대상 | 원천 | 새 API 호출 | 무엇이 열리나 |
|---|---|---|---:|---|
| **N1** | **일별 시총·거래대금·상장주식수·종가** | pykrx | ~6천 | **기존 A등급 2개의 분모가 근사값이다.** 정확값으로 바꾼다 |
| **N2** | **업종 코드 + 설립일·결산월** | DART `company.json` | ~4천 | 업종 중립화(진단 전용). 관계 피쳐의 선행 조건 |
| **N3** | **PIT 유니버스 (과거 상장 종목)** | pykrx | ~300 | 생존편향. 전이·부실 피쳐의 선행 조건 |
| **N4** | 업종 지수 구성종목 | pykrx | ~1–3천 | N2의 **준-PIT 대안**. 산업 모멘텀 |
| **N5** | 임원·주요주주 소유 / 대량보유 | DART DS004 | **PoC 전 미정** | 내부자 **보유수량 증감** |
| **N6** | 직원·임원·최대주주·감사의견 | DART DS002 | ~16만 | 인적자본, 지배구조, 부실 신호 |
| **N7** | KRX 공식 밸류에이션(PER/PBR/DIV) | pykrx | ~6천 | `fin_value_z` 독립 교차검증 |
| **N8** | 고용·실업률 | ECOS | 시리즈 2–3개 | 거시 레짐 재료 |
| **N9** | 공시 원문 텍스트 | DART 원본파일 | 대용량 | Lazy Prices 계열 |

**가장 먼저 짚을 것은 N1이다.** 지금까지 논의는 "새 카테고리를 열자"였는데, N1은 그게 아니라
**이미 A등급을 받은 피쳐가 근사값 위에 서 있다**는 문제다.

- `px_amihud_20d` (A, |IC| 0.1330 — 25개 중 2위) 의 분모가 **거래대금이 아니라 `종가 × 거래량`**이다.
  `daily_ohlcv` 스키마에 거래대금 컬럼이 없다(확인함).
- `fin_log_mcap` (A, 이번 검증에서 **가장 단단한 결과**) 의 시가총액이 DART `issued_shares`로
  만든 **계단형 근사**다. 보고서 접수 때만 갱신된다.
- `ev_net_share_issuance_yoy` 는 `isu_dcrs_stle` 문자열 매칭으로 주식수 변동을 재는데,
  `10_known_issues.md` I3 기준 **감소가 한 번도 매칭되지 않아 순발행이 아니라 총발행을 재고 있다.**

세 가지 모두 **KRX가 일별로 공식 제공하는 값**이다. N1 하나가 셋을 동시에 건드린다.

권장 순서는 §7에 있다. 요약하면 **N1·N2·N3을 1차 묶음으로 병행**한다 — 서로 다른 API, 서로
다른 테이블이라 충돌하지 않는다.

### 문서 지도

이 문서는 **무엇을 왜 모으는가**만 정한다. **어떻게 구현하는가**는 작업 패키지별 문서에 있다.

| 파일 | 대상 | 차수 |
|---|---|---|
| **`00_new_collection_plan.md`** | **이 문서 — 전체 계획·우선순위** | — |
| [`01_implementation_checklist.md`](01_implementation_checklist.md) | **공통 규약 — 새 테이블은 6곳에 등록해야 한다** | — |
| [`02_w1_daily_market_cap.md`](02_w1_daily_market_cap.md) | N1 일별 시총·거래대금·상장주식수 | 1차 |
| [`03_w1_company_profile.md`](03_w1_company_profile.md) | N2 업종 코드·설립일·결산월 | 1차 |
| [`04_w1_pit_universe.md`](04_w1_pit_universe.md) | N3 PIT 유니버스 | 1차 |
| [`05_w2_industry_index.md`](05_w2_industry_index.md) | N4 업종 지수 구성종목 | 2차 |
| [`06_w2_ownership_disclosure.md`](06_w2_ownership_disclosure.md) | N5 임원·주요주주 / 대량보유 | 2차 |
| [`07_w3_periodic_report_extras.md`](07_w3_periodic_report_extras.md) | N6 직원·임원·최대주주·감사의견 | 3차 |
| [`08_w3_valuation_and_macro.md`](08_w3_valuation_and_macro.md) | N7 KRX 밸류에이션 · N8 고용 | 3차 |
| [`09_w4_filing_text.md`](09_w4_filing_text.md) | N9 공시 원문 텍스트 (조건부) | 후순위 |

**먼저 읽을 것은 `01_implementation_checklist.md`다.** 새 테이블 하나가 6곳에 등록돼야 하는데,
실제로 `08` §4.1.2에서 두 테이블이 exporter 설정에 빠져 raw export가 한 번 막혔다.

---

## 2. 우선순위를 매긴 기준

1. **기존 결과를 바꾸는가** — 새 발견보다 기존 A/B 등급의 해석을 바꾸는 쪽이 먼저다.
2. **다른 것의 선행 조건인가** — 업종(N2)은 관계 피쳐 전체를, PIT 유니버스(N3)는 전이·부실
   피쳐 전체를 막고 있다.
3. **기존 수집 패턴을 재사용하는가** — OpenDART 다중키 실행기, 부분실패 finalizer,
   `ingestion_runs` 감사, skip-if-present는 이미 있다. 같은 모양이면 구현 비용이 크게 준다.
4. **PIT가 깨끗한가** — 룩어헤드가 원천적으로 불가능한 것을 먼저.
5. **한국 시장에서 기대값이 있는가** — `11_feature_taxonomy.md` §4의 복제율 표를 참고했다.

---

## 3. 1차 묶음 — N1 · N2 · N3

### N1. 일별 시가총액 · 거래대금 · 상장주식수 (pykrx)

**왜.** §1에 적은 셋이다. 요약하면 **지금 가장 강한 피쳐 두 개의 분모가 근사값**이고,
세 번째는 알려진 버그가 있다.

**원천.** `pykrx.stock.get_market_cap_by_date(fromdate, todate, ticker)` — docstring으로 확인한
반환 컬럼은 **시가총액 · 거래량 · 거래대금 · 상장주식수** 4개다. 전종목 일괄은
`get_market_cap_by_ticker(date, market='ALL')`.

**수집 방식.** 종목별 루프가 아니라 **날짜별 전종목**이 압도적으로 싸다.

```text
for d in 거래일:                       # 2014-06 ~ 현재 ≈ 3,000일 (2007부터면 ≈ 4,700일)
    get_market_cap_by_ticker(d, market='ALL')   # 1 call → 약 2,800행
```

호출 3–5천 회, 행 8–13M. `daily_ohlcv`(6.6M행)와 같은 급이다. 기존 `prices backfill`의
gap-detection·throttle·부분실패 패턴을 그대로 쓴다.

**저장 스키마 초안.** `daily_ohlcv`에 컬럼을 붙이지 않고 **별도 테이블**로 둔다. 원천이 다르고
(pykrx 다른 엔드포인트) 백필 시점도 다르다.

```sql
CREATE TABLE IF NOT EXISTS daily_market_cap (
    trade_date      DATE   NOT NULL,
    ticker          TEXT   NOT NULL,
    market          TEXT   NOT NULL,
    market_cap      BIGINT,        -- 시가총액 (KRW)
    trading_value   BIGINT,        -- 거래대금 (KRW)
    listed_shares   BIGINT,        -- 상장주식수
    volume          BIGINT,        -- 교차검증용 (daily_ohlcv와 대조)
    source          TEXT   NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (trade_date, ticker, market)
);
```

`volume`을 같이 받아두는 이유는 **`daily_ohlcv`와 대조해 조정 정책이 어긋나지 않는지 확인**하기
위해서다. 값이 다르면 그 자체가 신호다.

**PIT.** 문제없다. 거래일 종가 시점 확정값이고 사후 정정이 없다.

**이게 열어주는 것.**

| 지금 | N1 이후 |
|---|---|
| `px_amihud_20d` 분모 = `종가 × 거래량` | 실제 거래대금 |
| `market_cap_pit` = `종가 × DART issued_shares`(계단형) | KRX 공식 일별 시가총액 |
| 회전율 = `거래량 / DART issued_shares` | `거래대금 / 시가총액` (규모 중립) |
| `ev_net_share_issuance_yoy` = 문자열 매칭(I3 버그) | **상장주식수 일별 차분** |

마지막 줄이 특히 값이 크다. `ev_net_share_issuance_yoy`는 `isu_dcrs_stle` 카탈로그를 v2까지
고치고 vintage 정책 probe까지 돌린 항목인데(`09` §8), **KRX 상장주식수 차분이 그 경로를 통째로
우회한다.** 다만 차분에는 액면분할·무상증자 같은 기계적 변경이 섞이므로, **DART
`dart_capital_change_raw`와 결합해야 경제적 발행을 갈라낼 수 있다.** 즉 대체가 아니라
**독립 교차검증 축**으로 쓰는 게 맞다.

**주의 하나.** `get_market_ohlcv_by_date`의 시그니처를 확인해보니 `adjusted: bool = True`가
기본이고, 현재 `prices_pykrx/provider.py`는 이 인자를 명시하지 않는다. 즉 **수정주가로 받고
있을 가능성이 높다.** `02_feature_candidate.md` §2.3이 조정주가를 Phase 1 진입 조건으로 걸어둔
것과 관련되므로, N1 작업 중에 **상장주식수 급변일과 가격 점프를 대조해 실제 조정 여부를
확정**해두면 좋다. 이건 수집이 아니라 확인 작업이다.

**검증 (수집 후).**

- `daily_market_cap.volume` vs `daily_ohlcv.volume` 불일치율 — 0에 가까워야 한다
- `market_cap ÷ close` vs `listed_shares` 항등성
- `listed_shares` 급변일 ↔ `dart_capital_change_raw` 이벤트 대조
- 근사 `종가×거래량` vs 실제 `거래대금`의 일별 rank 상관 — 낮게 나오면 `px_amihud_20d` 재검정 필요

**리스크.** pykrx는 KRX 웹을 긁는 비공식 경로다. 스로틀·차단 정책이 `prices backfill`과 같다.
이번 세션에서 실호출은 네트워크가 막혀 확인하지 못했다 — **소규모 PoC(1개월치)를 먼저 돌려
컬럼명과 결측 패턴을 확정한 뒤 전량 백필한다.**

---

### N2. 업종 코드 + 설립일 + 결산월 (DART 기업개황)

**왜.** `11_feature_taxonomy.md`의 1순위다. 업계·학계 표준 모델(Barra, Gu-Kelly-Xiu)이 공통으로
두는 **industry 블록이 통째로 없고**, 그 결과 횡단 정규화 그룹이 KOSPI/KOSDAQ 둘뿐이다.

**원천.** OpenDART `company.json` (기업개황, DS001). 요청 인자는 `crtfc_key` + `corp_code`
하나뿐이다. 응답 필드를 확인했다.

| 필드 | 내용 | 쓰임 |
|---|---|---|
| **`induty_code`** | 업종코드 | **정규화 그룹 · 산업 팩터** |
| `corp_cls` | Y(유가) / K(코스닥) / N(코넥스) / E(기타) | 시장 구분 교차검증 |
| **`est_dt`** | 설립일 (YYYYMMDD) | **기업 연령(firm age)** — 알려진 predictor |
| **`acc_mt`** | 결산월 (MM) | **12월 결산이 아닌 기업 식별** — TTM·YoY 정합성 |
| `ceo_nm`, `adres`, `jurir_no`, `bizr_no`, `hm_url`, `ir_url` | 기타 | 지금은 안 쓴다 |

`est_dt`와 `acc_mt`는 원래 목표가 아니었는데 같은 호출에 딸려 온다. **`acc_mt`는 조용한
문제 하나를 드러낼 수 있다** — 결산월이 12월이 아닌 기업이 섞이면 분기 standalone 계산
(`02` §2.2의 `Q2=half−Q1` 등)과 YoY 비교가 어긋난다. 지금 이걸 구분하는 컬럼이 없다.

**중요 — 지금 `dart_corp_master`로는 안 된다.** `opendart_corp/provider.py`를 확인했다.
이건 `corpCode.xml` 벌크 zip을 파싱하는데, 거기엔 `corp_code / corp_name / stock_code /
modify_date`뿐이고 **업종이 없다.** `company.json`은 **corp_code당 1회 호출**하는 별도 API다.

**볼륨.** ticker 매핑이 있는 상장사 약 2,700개 × 1회 = **2,700 호출.** OpenDART 키당 일 20,000
한도이므로 **단일 실행으로 끝난다.** 기존 다중키 실행기(`opendart_common`)를 그대로 쓴다.

**저장 스키마 초안.** `dart_corp_master`에 컬럼을 추가하는 쪽이 자연스럽다. 다만 원천 API가
다르므로 **채워진 시점을 따로 기록**한다.

```sql
ALTER TABLE dart_corp_master
    ADD COLUMN IF NOT EXISTS induty_code   TEXT,
    ADD COLUMN IF NOT EXISTS corp_cls      TEXT,
    ADD COLUMN IF NOT EXISTS est_dt        DATE,
    ADD COLUMN IF NOT EXISTS acc_mt        TEXT,
    ADD COLUMN IF NOT EXISTS profile_fetched_at TIMESTAMPTZ;
```

**PIT — 여기가 이 항목의 유일한 약점이다.** `company.json`은 **현재 시점 업종만** 준다. 과거
업종 변경 이력이 없다. 사업 전환 기업(제조 → 바이오 등)에서 미래 정보가 샌다.

그래서 **처음부터 선을 긋는다.**

> 업종 코드는 **정규화 그룹과 진단 축으로만 쓴다. alpha 피쳐로 쓰지 않는다.**
> 알파로 쓰려면 N4(준-PIT 경로)가 필요하다.

이 선을 지키면 `02` §0.2 원칙 2(룩어헤드 원천 차단)를 어기지 않는다. 업종 중립화는 "그 시점에
그 업종이었는지"보다 "구조적으로 비슷한 회사끼리 묶는지"가 본질이라 오염 정도가 훨씬 작다.
그래도 **문서에 명시하고, 업종이 실제로 바뀐 기업 수를 N4로 사후 측정**한다.

**검증.**

- `induty_code` 결측률, 업종별 종목 수 분포 (한 업종에 5종목 미만이면 z-score가 불안정)
- `corp_cls` vs `stock_master.market` 불일치 건수
- `acc_mt != '12'` 종목 수 → 재무 마트에 미치는 범위 산정
- 업종 코드 체계 확인 — 표준산업분류 몇 자리인지, 대/중/소분류 중 어느 수준으로 묶을지 결정

**결정이 필요한 것.** 업종 분류 **깊이**다. 표준산업분류는 자릿수가 깊을수록 그룹이 작아진다.
Barra는 GICS 45개 산업을 쓴다. 2,700 종목이면 **20~40개 그룹이 적당**하다 — 그룹당 70~130종목이면
횡단 z-score가 안정적이다. 너무 깊게 자르면 그룹당 종목이 몇 개뿐이라 정규화가 망가진다.
**이건 결과를 보기 전에 고정해야 한다**(§9).

---

### N3. PIT 유니버스 — 과거 시점 상장 종목 (pykrx)

**왜.** 생존편향이다. `stock_master`의 DELISTED가 **28개뿐**이다. 원인을 코드에서 확인했다 —
`sync_universe.py`는 **스냅샷 간 diff로만** 상폐를 잡는다. 즉 스냅샷 수집을 시작한 이후의
상폐만 기록된다. 그 이전 12년치 상폐 종목은 표본에 아예 없다.

이게 왜 지금 중요한가. `11_feature_taxonomy.md` §5.5에서 지적했듯 **전이·부실·생애주기 Decline
피쳐는 생존편향에 직격**당한다. "적자에서 흑자로 돌아선 기업"만 남고 "적자에서 상폐된 기업"이
빠지면 흑자전환 피쳐의 성과가 구조적으로 부풀려진다. **이 축을 열기 전에 반드시 선행돼야 한다.**

**원천.** `pykrx.stock.get_market_ticker_list(date, market)` — **`date` 인자를 받는다.**
과거 임의 시점의 상장 종목 목록을 그대로 준다.

**수집 방식.** 매 거래일까지 갈 필요 없다. 상폐·신규상장은 월 단위 해상도로 충분하다.

```text
for d in 월말 거래일 (2014-06 ~ 현재, 약 145개):
    for m in [KOSPI, KOSDAQ]:
        get_market_ticker_list(d, market=m)
```

호출 약 **290회.** 매우 싸다.

**저장 — 새 테이블이 필요 없다.** `stock_master_snapshot` + `stock_master_snapshot_items`가
이미 이 목적으로 설계돼 있다(DDL 주석: "diff any two snapshots to find new listings, delistings").
현재 55개 스냅샷이 있다. **`as_of_date`를 과거로 두고 같은 테이블에 백필**하면 된다.

주의: 백필 스냅샷은 실제 수집 시점과 `as_of_date`가 다르다. `source`를 구분(`PYKRX_BACKFILL`)해
두고, 기존 스냅샷과 섞어 diff할 때 혼동하지 않게 한다.

**PIT.** 깨끗하다. 그 날짜에 상장돼 있었는지만 본다.

**이게 열어주는 것.**

- 마트의 유니버스 필터를 `stock_master.status = ACTIVE`가 아니라 **그 날짜의 스냅샷 소속**으로
  바꿀 수 있다
- 상폐 종목의 `daily_ohlcv`·`krx_security_flow_raw`가 이미 있는지 확인 → 없으면 **N3의 후속으로
  상폐 종목 가격 백필**이 필요하다. 이건 별도 판단이다
- 상폐 자체를 라벨/이벤트로 쓸 수 있다(부실 예측)

**검증.**

- 스냅샷별 종목 수 시계열 — 급락 구간은 수집 실패이지 상폐가 아니다
- 연도별 신규상장/상폐 건수를 KRX 공표치와 대조
- **상폐 종목 중 `daily_ohlcv`에 가격이 있는 비율** — 이게 낮으면 편향이 여전히 남는다

**한계를 미리 적는다.** 목록이 복원돼도 **상폐 직전 구간의 가격·수급 데이터가 없으면 편향은
완전히 안 없어진다.** 그리고 상폐 사유(부실/합병/자진)를 pykrx는 주지 않는다. 이 문서는
목록 복원까지만 계획하고, 가격 백필과 사유 수집은 목록을 본 뒤에 판단한다.

---

## 4. 2차 묶음 — N4 · N5

### N4. 업종 지수 구성종목 (pykrx) — N2의 준-PIT 대안

**왜.** N2의 PIT 약점을 메운다. 지수 구성종목은 **그 시점의 실제 소속**이라 룩어헤드가 없다.

**원천.** `get_index_ticker_list(date, market)` 로 지수 목록을 받고,
`get_index_portfolio_deposit_file(ticker, date)` 로 구성종목을 받는다.

**중요한 제약 — docstring에 명시돼 있다.** `get_index_portfolio_deposit_file`은
**2014년 5월 2일까지만 조회 가능**하다. 다행히 현재 표본 시작(2014-06)과 거의 맞는다.

**볼륨.** KOSPI/KOSDAQ 업종지수 약 20–30개 × 월말 145회 ≈ **3,000~4,000 호출.**

**이게 N2보다 나은 점과 못한 점.**

| | N2 (DART induty_code) | N4 (KRX 업종지수) |
|---|---|---|
| PIT | ✗ 현재 시점만 | **○ 시점별 실제 소속** |
| 커버리지 | 상장사 전체 | **지수 미편입 종목 누락** |
| 분류 깊이 | 표준산업분류 (조정 가능) | KRX 업종 (고정) |
| 비용 | 2,700 호출 1회 | 3~4천 호출 |

**둘 다 받아서 대조하는 게 맞다.** N2로 전 종목을 덮고, N4로 **업종이 실제로 바뀐 기업이
몇 개인지 측정**한다. 그 수가 작으면 N2의 PIT 약점이 실무적으로 무시할 수준이라는 근거가 되고,
크면 N4를 알파 경로로 승격한다.

`common_feature_series` 카탈로그에 이미 `industry_*` 4종이 **inactive(데이터 0건)**으로 있다
(`industry_krx_semiconductor_krx` 등). N4는 그 항목을 살리는 작업과 같은 자리다 — 다만
**지수 시계열이 아니라 구성종목**이 목표라는 점이 다르다. 지수 시계열도 같이 받으면 §7의
산업 모멘텀 피쳐 재료가 된다.

### N5. 임원·주요주주 소유 / 대량보유 (DART 지분공시 DS004)

**왜.** 내부자 매수는 미래 수익을 예측하고 **매수가 매도보다 정보량이 크다**(매도는 유동성·
분산 목적이 섞인다). 최근 연구는 내부자 거래가 이례현상 롱숏 종목의 미래 수익까지 예측한다고
보고한다.

**원천.** 두 개다.

| API | 내용 |
|---|---|
| `elestock` | **임원·주요주주 특정증권등 소유상황보고** |
| `majorstock` | **대량보유 상황보고**(5% rule) |

**볼륨.** corp_code당 1회. 2,700 × 2 = **5,400 호출.** 1회 실행으로 끝난다.

**PIT — 이 항목의 강점이다.** 보고 의무가 있어 **보고서 접수일이 명확**하다. 기존 재무 피쳐와
같은 `available_from` 규칙을 그대로 적용하면 된다. `dart_filing_receipt_raw`를 이미 수집하고
있어 접수일 결합 패턴도 있다.

**확인이 필요한 것.** 응답이 **전체 이력을 주는지 최근분만 주는지**를 PoC로 확인해야 한다.
최근분만 준다면 기간 파라미터나 접수번호 기반 반복 수집이 필요해 볼륨이 크게 늘어난다.
**이 확인 전에는 볼륨 추정을 신뢰하지 않는다.**

**저장.** 이벤트성 raw 테이블 두 개. `dart_capital_change_raw`와 같은 모양
(`rcept_no` + 보고자 + 변동 내역 + `raw_payload` JSON).

---

## 5. 3차 묶음 — N6 · N7 · N8

### N6. 직원·임원·최대주주·감사의견 (DART 정기보고서 DS002)

**왜.** 업계는 workforce analytics(고용 추이·이직률·채용공고)를 대체데이터로 비싸게 산다.
**한국은 직원 수와 1인 평균 급여가 정기보고서에 공시된다.** 무료다.

| API | 내용 | 후보 피쳐 |
|---|---|---|
| `empSttus` | 직원 현황 (직원 수, 평균 근속연수, 1인 평균 급여) | `hc_employee_growth_yoy`, `hc_revenue_per_employee` |
| `exctvSttus` | 임원 현황 | 경영진 교체 |
| `hyslrSttus` | 최대주주 현황 | `own_major_stake` |
| `hyslrChgSttus` | 최대주주 **변동** 현황 | `own_control_change` (전이형) |
| `accnutAdtorNmNdAdtOpinion` | 감사인·감사의견 | **비적정 의견 = 강한 부실 신호** |

**기존 패턴을 그대로 쓴다.** 현재 이미 DS002 엔드포인트 4개를 쓰고 있다 — `alotMatter.json`,
`stockTotqySttus.json`, `tesstkAcqsDspsSttus.json`, `irdsSttus.json`. 키 구조
(`corp_code × bsns_year × reprt_code`)와 저장 모양이 같다. **구현 비용이 가장 낮은 항목군인데
볼륨만 크다.**

**볼륨 — 여기가 이 항목의 유일한 문제다.**

```text
2,700 corp × 12 년 × 5 endpoint = 162,000 호출     # 사업보고서(11011)만
2,700 corp × 12 년 × 4 reprt × 5 = 648,000 호출    # 분기까지 전부
```

**연 1회(11011)로 충분하다.** 직원 수·최대주주·감사의견은 분기별로 볼 이유가 없다. 그러면
162,000 호출이고, 키 3개(일 60,000)면 **약 3일**이다. `bin/dart-backfill-all-years.sh`와 같은
규모라 운영 패턴도 이미 있다.

**우선순위를 3차로 둔 이유가 이 볼륨이다.** 1·2차 묶음이 다 끝난 뒤에 시작한다.

**시의성 있는 축 하나.** 2024년 밸류업 공시가 도입돼 2026년 7월 기준 749개사가 공시했고,
2024-09에 밸류업 지수 100종목이 나왔다. `ev_payout_yield`·`ev_net_share_issuance_yoy`가 이 레짐
변화 위에 놓여 있다(`02` D3도 "2025~ 소각 증가, 레짐 변화 유의"라고 적어뒀다). 다만
**표본이 2024년 이후뿐이라 지금 검정력이 거의 없다.** 수집은 해두되 검정은 나중이다.

### N7. KRX 공식 밸류에이션 (pykrx)

**왜.** `fin_value_z`(B등급)를 **독립 원천으로 교차검증**하기 위해서다. 지금 밸류는 전부 DART
canonical에서 만들고 있고, `10_known_issues.md` I1(결측이 1분위로 대체)·I7(매핑 병목) 같은
결함이 계속 나온다. KRX가 계산해 매일 공표하는 PER/PBR/DIV가 있으면 **어디까지가 데이터
문제인지 갈린다.**

**원천.** `get_market_fundamental_by_ticker(date, market)` — docstring 확인 결과 반환 컬럼은
**BPS, PER, PBR, EPS, DIV, DPS** 6개다. 날짜별 전종목 1 호출, 약 **3,000 호출.**

**용도를 분명히 한다.** 이건 **피쳐가 아니라 진단 원천**으로 먼저 쓴다. KRX 값은 산출 규칙이
블랙박스고 적자기업 처리(PER 결측)가 `fin_value_z`의 설계 의도(적자기업 보존)와 다르다.
**피쳐로 승격할지는 대조 결과를 보고 정한다.**

### N8. 고용·실업률 (ECOS)

**왜.** `11_feature_taxonomy.md` §8.1에서 확인했듯 `common_feature_series` 33개에
**고용 관련 시리즈가 없다.** 유가·물가·통화·심리·금리·환율은 있다.

**비용이 거의 0이다.** `definitions/common_features.py`에 시리즈 정의를 추가하고 `common seed`를
돌리면 끝이다. 새 어댑터가 필요 없다(ECOS 어댑터 기존재).

**그런데 우선순위는 낮다.** 이유는 `11_feature_taxonomy.md` §8.5에 적은 그대로다 — 월간이고
발표 지연이 커서 temporal placebo에 특히 취약하고, 이미 있는 `macro_consumer_sentiment`·
`macro_m2`와 정보가 겹칠 것이다. **무엇보다 매크로 조건화 구조 자체가 아직 0개다.**
레짐 변수의 종류를 늘리는 것보다 조건화를 한 번이라도 돌려보는 게 먼저다.

**같이 처리할 것.** `common_feature_series`의 inactive 7개(pykrx 지수 폴백 3, KRX 산업지수 4)를
이번에 정리한다. N4가 산업지수를 다시 건드리므로 같은 작업 묶음이다.

---

## 6. 후순위 / 하지 않는 것

### N9. 공시 원문 텍스트 (DART 원본파일) — 후순위

Lazy Prices 계열(정기보고서 문구 변화가 미래 수익·이익·부도를 예측)의 원천이다. 효과 근거는
두텁지만 **용량과 파싱 비용이 이 문서의 다른 모든 항목을 합친 것보다 크다.**

**그 전에 할 수 있는 게 있다.** `dart_filing_receipt_raw`(2015~2026)가 **이미 수집돼 있는데
알파 피쳐로는 한 개도 안 쓰인다.** 현재 용도는 `fin_sue_event`와 Phase B 품질 진단뿐이다.
접수 메타데이터만으로 공시 빈도·급증·정정 비율·`report_nm` 기반 이벤트 분류가 나온다.
**이건 신규 수집이 아니므로 이 문서 범위 밖이다**(§8).

### 수집하지 않는다고 적어두는 것

| 대상 | 이유 |
|---|---|
| 애널리스트 컨센서스·추정치 수정 | 유료. 대체: `fin_sue`의 seasonal random walk 근사 |
| 개별 종목 옵션 (IV skew 등) | **한국은 개별주 옵션이 사실상 없다.** KOSPI200 옵션은 횡단면에 못 쓰고 시장 레짐 변수로만 가능 |
| 투자자 주의(Google 검색량 등) | 안정적 원천 없음. 대체: `px_turnover_shock` |
| 뉴스·소셜 감성 | 원천 없음. 대체: N9 |
| 대체데이터(위성·카드결제·웹트래픽) | 비용 과다. 대체: N6 직원 현황 |
| ESG 등급 | 유료. 대체: N6 감사의견·지배구조 |
| 일중 미시구조(호가·체결) | **스코프 제외** (`ports/prices.py` 스텁, CLAUDE.md) |

**금액 기준 투자자 순매수**는 애매한 위치라 따로 적는다. 현재 `krx_security_flow_raw`는
**수량 기준만** 있다. pykrx `get_market_trading_value_and_volume_by_ticker`로 금액 기준을 받을
수는 있다. 다만 지금 flow 피쳐의 정규화 분모가 거래량이라 형태가 일관돼 있고, 순매수 8개 중
7개가 D 또는 보류 등급이다. **금액 기준으로 바꾼다고 부호가 뒤집힐 근거가 약하다.**
N1의 거래대금이 들어오면 `수량 × 종가 / 거래대금`으로 근사할 수 있으므로, **N1 이후에 다시
판단**한다.

---

## 7. 실행 순서

```
1차 (병행 가능 — 서로 다른 API·테이블)
├─ N1 일별 시총·거래대금·상장주식수·종가 (pykrx) ← PoC → 전량 백필       → 02
├─ N2 업종 코드 + 설립일·결산월 (DART)         ← 단일 실행               → 03
└─ N3 PIT 유니버스 백필 (pykrx)                ← 월말 145회 × 2          → 04
   (N1·N3 PoC는 하나로 묶는다 — 같은 날짜의 두 응답을 대조해야 한다)

2차
├─ N4 업종 지수 구성종목 (pykrx)               ← N2 PIT 약점 측정        → 05
└─ N5 임원·주요주주 / 대량보유 (DART DS004)    ← PoC로 이력 범위 확인    → 06

3차
├─ N6 직원·임원·최대주주·감사의견 (DART DS002) ← 약 3일 · ledger 선행    → 07
│   (대상 산출이 N3에 의존한다)
├─ N7 KRX 공식 밸류에이션 (pykrx)              ← 진단 원천으로 먼저      → 08
└─ N8 고용 시리즈 (ECOS) + inactive 시리즈 정리                          → 08

후순위
└─ N9 공시 원문 텍스트                          ← 조건부, 지금은 안 한다 → 09
```

**각 단계 공통 작업은 [`01_implementation_checklist.md`](01_implementation_checklist.md)에
따로 정리했다.** 요약하면 새 테이블 하나가 **6곳에 등록**돼야 하고(DDL / `remote_sync.py` /
**`service/profiling/catalog.py`** / exporter TOML / `bin` 배열 / `research/etl/config.py`),
서비스는 부분실패 finalizer와 skip-if-present를 갖추며, **"행이 있으면 완료"로 skip하지
않고**(§2.4), **pykrx 계열은 PoC가 선행**이다.

**prod 반영은 `sdc-release` 스킬**로 한다(버전 범프 → 태그 → sj2 compose 갱신). 백필은
Cronicle 일회성 이벤트로 돌리고 **끝나면 이벤트를 지운다** — 과거에 상시 이벤트로 남겨 두어
혼란이 생긴 적이 있다.

---

## 8. 이 문서 범위 밖 — 수집 없이 계산만 하면 되는 것

혼동을 막기 위해 명시한다. 아래는 **새 수집이 필요 없다.** 상세는
[`01_feature_candidate/11_feature_taxonomy.md`](../01_feature_candidate/11_feature_taxonomy.md)에 있다.

| 항목 | 필요한 것 | 참조 |
|---|---|---|
| 레버리지·재무위험 | canonical `total_liabilities`·`total_equity`·`interest_paid` (전부 있음) | §9.1 |
| 기업 생애주기 (Dickinson) | canonical 3대 현금흐름 (전부 있음) | §9.2 |
| 재무 Δ·상태 전이 | 기존 `feat_fin_scan_daily` 위에 얹는다 | §5 |
| 매크로 exposure beta | 기존 `common_feature_observation_raw` | §8.3 |
| 계절성 | `daily_ohlcv`만으로 됨 | §9.8 |
| 통계적 peer 리드래그 | `daily_ohlcv` 수익률 상관 | §7.2 |
| 공시 빈도·정정 비율 | `dart_filing_receipt_raw` (수집돼 있음, 알파 미사용) | §9.4 |
| 운전자본·R&D | **raw에는 있고 `metric_rules` 매핑만 없다** | §9.1, §9.3 |

마지막 줄이 특히 헷갈리기 쉽다. 유동자산·유동부채·R&D는 `dart_financial_statement_raw`에
계정이 있을 가능성이 높다 — **`metric_rules` 확장이지 신규 수집이 아니다.**

---

## 9. 규율 — 수집과 검정을 섞지 않는다

수집은 지금 시작해도 된다. **검정은 다르다.**

1. **BH 모집단이 커지면 기존 발견의 문턱이 올라간다.** 현재 `m_ab=113`에서 discovery 45개가
   나왔고 강등이 0건이다(`00_status.md` §4). 새 원천으로 family를 늘리면 그 여유가 준다.
2. **새 config로 별도 사전등록.** 기존 run의 `config_hash=e55c3046…`를 건드리지 않는다.
3. **결과를 보기 전에 고정해야 할 것이 이미 몇 개 있다.**
   - N2: **업종 분류 깊이**(§3 N2 마지막). 여러 깊이를 시도해 제일 잘 나오는 걸 고르면
     window grid search와 같은 위반이다
   - N1: 거래대금·시가총액을 **기존 피쳐의 분모로 교체할지, 별도 variant로 둘지**.
     교체하면 발행된 Phase A/B run의 재현 경로가 깨진다
   - N5: 내부자 순매수의 **부호와 윈도**
4. **holdout은 여전히 한 번만.** feature·horizon·variant·interaction 선택이 전부 끝난 뒤다.
5. **N3(PIT 유니버스)는 전이·부실 피쳐의 선행 조건이다**(`11_feature_taxonomy.md` §5.5).
   N3 없이 그 축의 결론을 내지 않는다.
6. **재실행 비용을 기억한다.** 실측으로 Phase B 5시간 30분, Phase A 4시간 41분이다. 수집할
   때마다 재검정하지 않고 **묶어서 한 번에** 돌린다.

**그리고 지금 크리티컬 패스는 Phase C·acceptance gate 인계**(`00_status.md` §5-1b)다.
수집은 그것과 독립이라 병행해도 되지만, **재실행 순서는 인계가 끝난 뒤에 정한다.**

---

## 부록. 이 문서의 사실 확인 근거

수집 가능성 판단은 전부 실물 확인이다. 추정으로 적은 곳은 그렇다고 표시했다.

| 주장 | 확인 방법 | 결과 |
|---|---|---|
| `daily_ohlcv`에 거래대금 없음 | `sql/postgres_ddl.sql` 76~104행 | 컬럼 10개, 거래대금 없음 |
| pykrx가 시총·거래대금·상장주식수·**종가** 제공 | `stock_api.py` `get_market_cap_by_ticker` | **5개 컬럼**. 종가 포함 |
| **응답에 시장 구분 컬럼이 없다** | 같은 곳 (index=티커) | **시장별 호출 필요 → 호출 2배** |
| **`adjusted=True`는 KRX가 아니라 naver 경로** | `stock_api.py` 236~240행 | `daily_ohlcv`는 naver 수정주가 |
| **pykrx가 결측을 0으로 캐스팅** | `wrap.py` 249·281행 | 진짜 0과 결측이 구분되지 않는다 |
| pykrx가 과거 시점 종목 목록 제공 | `get_market_ticker_list` 시그니처 | `date` 인자 있음 |
| 지수 구성종목 조회 한계 | `get_index_portfolio_deposit_file.__doc__` | **2014-05-02까지만** |
| KRX 밸류에이션 컬럼 | `get_market_fundamental_by_ticker.__doc__` | BPS/PER/PBR/EPS/DIV/DPS |
| pykrx 수정주가 기본값 | `get_market_ohlcv_by_date` 시그니처 | `adjusted: bool = True` |
| `upsert_stock_master`가 `stock_master`도 갱신 | `repositories.py` 78~145행 | 3단계 — N3는 전용 메서드 필요 |
| `Source` enum에 `PYKRX_BACKFILL` 없음 | `domain/enums.py` | 추가 필요 |
| **profiling catalog 등록이 테스트로 강제** | `tests/unit/test_profiling.py` `test_catalog_covers_all_pipeline_tables` | 등록 6번째 지점 |
| 마트가 결산월을 12월로 하드코딩 | `research/etl/marts/metric_vintages.py` `_calendar_period_end_expr` | 비12월 결산 기업이 어긋난다 |
| no-data 캐시 한계 | `sync_dart_*.py` `[:1000]`, `dart_target_plan.py` `limit=20` | 장기 백필 완료 상태로 못 쓴다 |
| `empSttus`는 `rcept_no`로 조회 불가 | DS002 개발가이드 | **final-vintage만 받는다** |
| `elestock`에 변동일·가격·사유 없음 | DS004 개발가이드 | "매수" 판정 불가, lag 계산 불가 |
| DS004 요청 인자는 `corp_code`뿐 | DS004 개발가이드 | **연도 루프 우회 불가** |
| `company.json`에 `induty_code` 있음 | [OpenDART 개발가이드 DS001/2019002](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019002) | 확인. `est_dt`·`acc_mt`·`corp_cls`도 포함 |
| `dart_corp_master`에 업종 없음 | `sql/postgres_ddl.sql` 128~139행, `opendart_corp/provider.py` | corpCode.xml 파싱 — 4개 필드뿐 |
| 현재 쓰는 OpenDART 엔드포인트 | 어댑터 grep | list / fnlttSinglAcntAll / alotMatter / stockTotqySttus / tesstkAcqsDspsSttus / irdsSttus |
| DS002·DS004 추가 엔드포인트 존재 | [dart-fss API 문서](https://dart-fss.readthedocs.io/en/latest/dart_api.html), [OpenDART API 목록](https://opendart.fss.or.kr/intro/infoApiList.do) | `empSttus`, `exctvSttus`, `hyslrSttus`, `hyslrChgSttus`, `elestock`, `majorstock` |
| 상폐 감지가 스냅샷 diff 기반 | `service/sync_universe.py` 95~125행 | 스냅샷 시작 이후만 잡힘 |
| 고용 시리즈 없음 | `definitions/common_features.py` grep | 고용/실업 시리즈 0건 |

**실호출로 확인하지 못한 것.** 이번 세션에서 pykrx 실호출이 네트워크 차단으로 실패했다
(`Expecting value: line 1 column 1`). 위 표의 pykrx 항목은 **설치본 1.2.8의 시그니처와
docstring 기준**이다. **N1·N3·N4·N7은 소규모 PoC로 실제 응답을 확인한 뒤 전량 백필한다.**

N5(`elestock`/`majorstock`)의 **응답이 전체 이력인지 최근분인지**도 미확인이다. 볼륨 추정이
여기에 달려 있으므로 PoC가 먼저다.

---

## 10. 개정 이력 — 2026-08-15 재검증

초안을 코드와 API 문서로 다시 검증해 **7군데를 고쳤다.** 계획의 방향은 유지했지만
스키마·비용·PIT 판단이 바뀐 곳이 있다.

| # | 고친 것 | 근거 | 영향 |
|---|---|---|---|
| 1 | **N1 스키마에 `source_close` 추가** | `get_market_cap_by_ticker`는 5컬럼(종가 포함) | 항등성 검증과 조정주가 확인이 비로소 가능 |
| 2 | **N1·N7 호출량 2배** (각 ~6천) | 응답에 시장 구분 컬럼 없음 | `stock_master` 조인은 이전상장 종목에서 룩어헤드 |
| 3 | **V1/V3 해석 교정** | `adjusted=True`는 naver 경로 | 거래량 불일치는 조정 정책이 아니라 원천 차이 |
| 4 | **등록 5곳 → 6곳** | `test_profiling.py`가 profile catalog 등록을 강제 | 빠뜨리면 테스트가 깨진다 |
| 5 | **N2 업종 깊이 전제 교정** | KSIC 3자리는 소분류 234개 (중분류는 2자리 77개) | "3자리 = 30~60 그룹"은 틀렸다. 매핑을 명시 설계 |
| 6 | **N2를 scored backtest에서 배제** | 정규화 그룹으로만 써도 과거 z-score에 미래 분류가 들어간다 | 진단 전용. 정식 variant는 N4 이후 |
| 7 | **N5 피쳐 재정의** | `elestock`에 변동일·가격·사유 없음 | "매수"가 아니라 `ins_holding_increase` |

이 밖에 반영한 것들이다.

- **완료 ledger**(`01` §2.4) — "행이 있으면 완료"로 skip하면 부분 저장이 영구히 남는다.
  N6은 ledger 필수(현재 no-data 캐시가 run당 1,000개·최근 20 run 한계)
- **N3 역할 축소** — 월말 스냅샷은 정확한 PIT가 아니다. 일별 유니버스는 N1 행이 담당하고
  N3는 감사·교차검증으로. 그리고 `upsert_stock_master`가 `stock_master`도 갱신하므로
  **전용 storage 메서드와 `Source.PYKRX_BACKFILL`이 새로 필요**하다
- **N2 `acc_mt`는 조사로 끝나지 않는다** — `metric_vintages.py`가 결산월을 12월로
  하드코딩하고 있어 실제 수정 작업이 뒤따른다
- **N6 final-vintage 한계 명시** — DS002는 `rcept_no`로 조회할 수 없어 최종 정정본만 온다.
  `rcept_no`를 UNIQUE에 넣고, 부실 신호는 이 위에서 결론 내지 않는다.
  대상도 현재 상장사가 아니라 **역사적 상장사**로 잡는다(생존편향)
- **N4 업종 변경 측정 정의 강화** — 지수 편입·제외에는 신규상장·유동성·방법론 개편이 섞인다.
  4개 조건을 모두 만족하는 종목만 센다. 임계값은 통계가 아니라 **운영 규칙**임을 명시
- **N7 `PER == 0`이 결측 sentinel** — pykrx가 `'-'`를 `0`으로 치환한다. C5 IC는 exploratory
- **N8 사후 개정** — `period-end + 20일`은 발표 지연만 막는다. 중복 판정은 raw level이 아니라
  **변환 후·availability 정렬 후** 상관으로
- **N9는 절 단위 다운로드가 불가능** — `document.xml`은 `rcept_no` 하나로 전체 ZIP을 준다.
  "전체 다운로드 후 특정 절만 보존"으로 고쳤고, 원문은 object storage에 둔다

