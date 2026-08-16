# 조사 — N7 · N4 대체 경로 (K-6d)

- 조사일: 2026-08-16
- 계기: KRX Open API에 **PER/PBR/배당수익률과 지수 구성종목이 없다**
  ([`krx_open_api.md`](krx_open_api.md) §3.2). 두 패키지는 **기존 데이터가 0이라**
  대체 경로가 없으면 열리지 않는다.
- 선행: [`flows_alternatives.md`](flows_alternatives.md) — 같은 축의 `flows` 조사

---

## 0. 한 줄 결론

**둘 다 폐기가 아니라 축소다. 그리고 N7은 대체재보다 근거가 먼저 문제였다.**

N7이 팔던 진단 근거 셋(I1·I7·I6)을 원문에서 확인해보니 **전부 다른 작업으로 이미 귀속돼 있다.**
남는 가치는 있지만 **6,000일 이력이 필요 없다.**

N4는 목적 둘 중 하나가 이미 충족됐고, 남은 하나는 **지수 구성종목보다 나은 원천이 있다.**

---

## 1. N7 — 근거부터 무너진다

### 1.1 대체재 목록

| 후보 | PER/PBR | 비고 |
|---|---|---|
| KRX Open API | ❌ | 확정 |
| 공공데이터포털 `주식시세정보` | ❌ | 다만 §3의 별건이 나왔다 |
| 공공데이터포털 `기업재무정보` | ❌ | 재무제표 항목이지 비율이 아니다 |
| **KIS `inquire_price`** (`FHKST01010100`) | **✅** `per`·`pbr`·`eps`·`bps` | 독립·무료. **현재값만** |
| KRX 데이터 상품 / Koscom datamall | ? | **두 페이지 다 500 에러** |
| KRX 화면 다운로드 | ✅ | `MDC0201020502` PER/PBR/배당수익률(개별종목) |

> KIS 필드는 서드파티 문서 다수가 일치하는 것으로 확인했다. **공식 명세 대조는 안 했다.**

### 1.2 그런데 근거 셋이 이미 다른 데로 귀속돼 있다

[`08_w3_valuation_and_macro.md`](../08_w3_valuation_and_macro.md) A1은 N7의 목적을
**`fin_value_z` 교차검증 + I1·I7 진단**으로 적었다.
[`10_known_issues.md`](../../01_feature_candidate/10_known_issues.md)를 다시 읽으면
셋 다 원인이 이미 확정돼 있다.

**I1은 데이터 문제가 아니라 SQL 버그다.**

```sql
LEAST(GREATEST(fin_book_to_market, p01), p99)   -- DuckDB는 NULL을 건너뛴다
LEAST(GREATEST(NULL, 0.1), 0.9)  →  0.1        -- NULL이 아니다
```

§2에 원인·위치(`fin_scan.py:216-231`)·수정 SQL·검증 방법이 전부 적혀 있다.
**독립 PER/PBR이 여기 기여할 게 없다. 코드를 고치면 끝난다.**

**I7은 밸류에이션 원천 문제가 아니라 DART canonical 매핑 문제다.**
`revenue` 8,103행 / `gross_profit` 7,362행 vs `net_income` 141,011행.
§5.5.3이 **"XBRL fallback 보강이 정답"**이라고 이미 결론냈다.

**등급 B 상한(I6)은 정정분 재수집이 답이다.** `net_income` revision_ratio 0.1056.

### 1.3 그래서 N7에 남는 것

**I1·I7을 고친 뒤 canonical 매핑이 맞는지 독립 원천으로 대조하는 것.** 이건 진짜다 —
윈저라이즈 버그를 고쳐도 매핑이 틀렸는지는 여전히 모른다.

**다만 6,000일 이력이 필요 없다.** 매핑 검증은 횡단면 문제다.
몇 개 시점에서 2,763종목의 PER/PBR을 우리 값과 맞춰보면 된다.
KIS로 시점당 약 2.3분, 무료다.

### 1.4 N7 권고 — 축소

- **취소**: 새 테이블 `daily_market_fundamental` + 6,000 호출 백필
- **대체**: KIS 횡단면 대조 (시점 수 개)
- **선행**: **I1·I7·I6 수정이 먼저다.** 고치기 전에 대조하면 이미 아는 결함을 다시 발견할 뿐이다
- **미확정**: KIS에 `DIV`(배당수익률)가 있는가 — `per/pbr/eps/bps`만 확인됐다

---

## 2. N4 — 목적 하나는 이미 충족, 남은 하나는 더 나은 원천이 있다

### 2.1 목적 B는 N4 없이 산다

[`05_w2_industry_index.md`](../05_w2_industry_index.md) §1의 목적 B는
관계 피쳐(산업 모멘텀·리드래그·업종 수급)의 **"같은 업종 종목 집합"** 재료다.

**이미 있다.** N2-7b 완료로 확인했다.

```
corps | with_induty | codes | profiled
 3959 |        3959 |   630 |     3959
```

`definitions/industry_groups.py`가 이 `induty_code`를 KSIC 중분류로 접어
`fin_scan.py`의 횡단면 z-score에 쓰고 있다. **지수 구성종목이 아니어도 된다.**

### 2.2 목적 A — 지수 구성종목보다 나은 원천

목적 A는 **"업종이 실제로 바뀐 기업이 몇 개인가"** = N2의 PIT 약점 크기다.

| 후보 | 결과 |
|---|---|
| KRX Open API | 구성종목 **없음** (확정) |
| 공공데이터포털 | **없음** |
| KIS `inquire_index_category_price` | 업종별 시세, **현재 시점** |
| **KRX `업종분류 현황`** (`MDC0201020506`) | **전종목 업종 CSV 다운로드** |

**화면 다운로드는 차단 안내가 명시적으로 허용한 공식 경로다.**

그리고 **목적 A에는 이쪽이 더 맞다.** 계획 문서 스스로 경고한 부분이 있다.

> 지수 편입·제외에는 업종 변경 말고도 **신규상장, 유동성 기준 미달, 지수 방법론 개편**이
> 전부 섞인다. 측정 3의 정의가 이 작업에서 가장 조심할 부분이다.

`05` §4의 N4-PR6 측정 3은 그래서 **조건 3개를 걸어 후보를 좁히는 설계**였다.
업종분류 현황에는 그 잡음이 없다 — **업종을 직접 읽는다.**

### 2.3 N4 권고 — 재정의

- **취소**: `krx_index_constituent` 테이블 + 3,600 호출 지수 구성종목 수집
- **대체**: **업종분류 현황 월별 스냅샷**
- **확인 필요 (딱 하나)**: 그 화면이 **과거 기준일 조회를 받는가**
  - 받으면 → 목적 A가 풀린다. 월별 소급 스냅샷으로 업종 변경 이력을 만든다
  - 안 받으면 → 앞으로만 쌓인다. 목적 A는 **몇 년 뒤에나** 측정 가능
- **목적 B는 이 결정과 무관하다** — 이미 N2로 돌아간다

---

## 3. 별건 — 공공데이터포털이 K-2를 우회한다

`금융위원회_주식시세정보`(`getStockPriceInfo`)의 응답 필드다.

```
basDt, srtnCd, isinCd, itmsNm, mrktCtg, clpr, vs, fltRt,
mkp, hipr, lopr, trqu, trPrc, lstgStCnt, mrktTotAmt
```

**`mrktTotAmt`(시가총액) · `lstgStCnt`(상장주식수) · `trPrc`(거래대금) — N1의 payload 그대로다.**

| | |
|---|---|
| 엔드포인트 | `apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo` |
| 범위 조회 | **`beginBasDt` / `endBasDt` 지원** |
| 한도 | 개발계정 하루 10,000회 (운영계정 증설 신청) |
| 갱신 | 일 1회, 영업일 기준 익일 13시 이후 |

**K-2에서 "개발명세서를 봐야 안다"고 남겨둔 질문에 다른 문으로 답이 나왔다.**
KRX 승인을 기다리는 동안 이쪽으로 갈 수도 있다.

> **단정하지 않는다.** 필드 목록의 출처가 서드파티 문서다.
> 공식 활용가이드 대조와 **이력 깊이 확인**이 남아 있다.

---

## 4. 두 패키지를 어떻게 둘 것인가

**폐기가 아니라 축소다.** 원래 계획의 KRX 스크래핑 의존분만 덜어내면 목적은 살아남는다.

| | 원래 | 축소 후 | 잃는 것 |
|---|---|---|---|
| **N7** | 새 테이블 + 6,000 호출 | KIS 횡단면 대조 | **시계열 밸류에이션 피쳐 가능성** — 원래도 "피쳐가 아니라 진단"이었다 |
| **N4** | `krx_index_constituent` + 3,600 호출 | 업종분류 현황 스냅샷 | **지수 소속 기반 관계 피쳐** — 목적 B는 N2로 대체됨 |

---

## 5. 남은 확인 항목

- [ ] KIS `inquire_price`에 **`DIV`(배당수익률)** 가 있는가 — `per/pbr/eps/bps`만 확인됨
- [ ] KRX **업종분류 현황이 과거 기준일 조회를 받는가** — N4의 유일한 분기점
- [ ] `getStockPriceInfo` **공식 명세 대조 + 이력 깊이**
- [ ] KRX 데이터 상품 / Koscom datamall — **두 페이지 다 500.** 전화 문의(1577-0088)

---

## 출처

- [금융위원회_주식시세정보](https://www.data.go.kr/data/15094808/openapi.do)
- [금융위원회_기업 재무정보](https://www.data.go.kr/data/15043459/openapi.do)
- [KIS Developers](https://apiportal.koreainvestment.com/apiservice)
- [KRX 업종분류 현황](https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506)
- [KRX PER/PBR/배당수익률(개별종목)](https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020502)
- [koscom datamall](https://datamall.koscom.co.kr/kor/datamall/stock/dailySearchData.do?screenId=100010&menuNo=200001) — 500
- [getStockPriceInfo 필드 예시](https://white.seolpyo.com/entry/126/)
