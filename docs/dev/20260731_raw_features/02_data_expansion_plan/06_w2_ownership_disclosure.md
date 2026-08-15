# 06. N5 — 임원·주요주주 소유 / 대량보유 (2차)

- 작성일: 2026-08-15
- 공통 규약: [`01_implementation_checklist.md`](01_implementation_checklist.md)
- 원천: OpenDART 지분공시 종합정보(DS004) — `elestock`, `majorstock`
- 새 테이블: `dart_insider_holding_raw`, `dart_major_holding_raw`
- 예상 규모: **PoC 전에는 추정하지 않는다**(§2)

---

## 1. 왜

내부자 거래는 문헌이 일관된 축이다.

- 내부자 거래는 미래 수익을 예측하고, **매수가 매도보다 정보량이 크다** — 매도는 유동성·분산
  목적이 섞이고 소송 위험이 부정적 사적 정보에 기반한 매도를 억제한다
- 최근 연구는 내부자 거래가 **이례현상 롱숏 종목에서 미래 이례현상 수익까지 예측**한다고
  보고한다. 즉 기존 팩터와 독립이 아니라 **기존 팩터의 신뢰도를 조건화**하는 정보다
- 한국 시장 연구도 자사주 매입 전후 내부자 거래, 단기매매차익 규제 하의 거래 패턴을 다룬다

**카테고리로는 C3(수급·소유)의 빈 칸이다.** 현재 flow 8개는 전부 시장 참여자 집계
(외국인/기관/개인)이고, **기업 내부자는 0개**다.

**PIT가 깨끗한 것이 이 항목의 강점이다.** 보고 의무가 있어 접수일이 명확하다. 기존 재무 피쳐와
같은 `available_from` 규칙을 그대로 쓰면 된다.

**다만 이 원천은 "내부자 매수"를 직접 재지 못한다.** API 응답을 확인한 결과 §3.5의 제약이
있어, 피쳐는 **보유수량 증감**까지만 정의된다.

---

## 2. 볼륨을 지금 추정하지 않는 이유

두 API의 **응답 범위가 미확인**이다.

| 가능성 | 볼륨 | 수집 방식 |
|---|---|---|
| 응답이 **전체 이력**을 준다 | corp당 1회 → **5,400 호출** | 단일 실행 |
| 응답이 **최근분만** 준다 | 접수번호/기간 반복 → **수십만 호출** | 연도 루프 + 재개 |

**차이가 100배다.** 그래서 PoC가 다른 어떤 항목보다 먼저다. `00_new_collection_plan.md`의
"5,400 호출" 추정은 낙관적 가정이고, **PoC 전에는 신뢰하지 않는다.**

---

## 3. 두 API의 차이

| API | 정식 명칭 | 보고 주체 | 성격 |
|---|---|---|---|
| `elestock` | 임원·주요주주 특정증권등 **소유상황보고** | 임원, 10% 이상 주주 | **개인의 매매** — 정보 신호 |
| `majorstock` | **대량보유 상황보고** (5% rule) | 5% 이상 보유자 | **지배권 변동** — 인수·경영권 분쟁 |

**둘은 다른 것을 잰다.** `elestock`이 §1 문헌의 주 대상이고, `majorstock`은 이벤트성
(적대적 인수, 행동주의 펀드 진입)이다. **피쳐 설계에서 섞으면 안 된다.**

우선순위를 굳이 매기면 **`elestock`이 먼저**다. 정기적으로 발생해 표본이 크고, 문헌 근거가
더 두텁다.

---

## 3.5. 응답 필드를 확인했다 — 계획을 두 군데 고쳐야 한다

[OpenDART 개발가이드](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS004&apiId=2019022)에서
확인한 실제 필드다.

| API | 요청 인자 | 응답 필드 |
|---|---|---|
| `elestock` | `crtfc_key`, **`corp_code`뿐** | `rcept_no`, `rcept_dt`, `corp_code`, `corp_name`, `repror`, `isu_exctv_rgist_at`, `isu_exctv_ofcps`, `isu_main_shrholdr`, `sp_stock_lmp_cnt`, `sp_stock_lmp_irds_cnt`, `sp_stock_lmp_rate`, `sp_stock_lmp_irds_rate` |
| `majorstock` | `crtfc_key`, **`corp_code`뿐** | `rcept_no`, `rcept_dt`, `corp_code`, `corp_name`, `report_tp`, `repror`, `stkqy`, `stkqy_irds`, `stkrt`, `stkrt_irds`, `ctr_stkqy`, `ctr_stkrt`, `report_resn` |

**여기서 계획이 두 군데 틀렸다.**

### ① 변동일·가격·사유가 없다 → `insider_report_lag_days`를 계산할 수 없다

응답에 있는 날짜는 **`rcept_dt`(공시 접수일) 하나뿐**이다. 실제 매매일도, 취득 단가도,
취득 사유도 없다. 따라서

- 처음 계획한 `insider_report_lag_days = rcept_dt − 변동일`은 **계산 불가**다. 삭제한다
- **양(+)의 `sp_stock_lmp_irds_cnt`가 "매수"라는 보장이 없다.** 스톡옵션 행사, 무상증자
  배정, 상속·증여, 주식배당이 전부 같은 부호로 들어온다

> **피쳐 이름을 `ins_buy_intensity`가 아니라 `ins_holding_increase`로 둔다.**
> "매수"로 승격하려면 공시 원문에서 취득 원인을 파싱해야 한다 —
> [N9](09_w4_filing_text.md) 범위이고 지금은 하지 않는다.

**분모도 바뀐다.** 처음에 시가총액을 제안했는데, 응답이 **수량**이므로
**`listed_shares`(→ [N1](02_w1_daily_market_cap.md))가 자연스럽다.** `sp_stock_lmp_rate`가
이미 비율을 주므로 그것도 교차검증에 쓴다.

### ② 요청 인자가 `corp_code`뿐이다 → 연도 루프가 불가능하다

기간·연도 파라미터가 없다. 따라서 §2에서 "최근분만 주면 연도 루프"라고 적은 폴백은
**성립하지 않는다.** 응답이 최근분뿐이면 폴백은 이것뿐이다.

```
list.json (dart_filing_receipt_raw, 이미 수집돼 있다)
  → 지분공시 report_nm 필터로 과거 rcept_no 목록 추출
  → 접수번호별 원문 파싱                       ← N9급 비용
```

**즉 PoC에서 "최근분만"이 확인되면 이 작업 패키지는 2차가 아니라 후순위로 내려간다.**
연도 루프로 우회할 여지가 없다.

---

## 4. 스키마

기존 `dart_capital_change_raw`와 같은 모양이다 — `rcept_no` + 보고자 + 변동 내역 +
`raw_payload` JSONB.

```sql
-- sql/postgres_ddl.sql
CREATE TABLE IF NOT EXISTS dart_insider_holding_raw (
    raw_id          BIGSERIAL   PRIMARY KEY,
    corp_code       TEXT        NOT NULL,
    ticker          TEXT,
    rcept_no        TEXT        NOT NULL,   -- 접수번호 → available_from의 근거
    rcept_dt        DATE,                   -- 접수일자
    repror          TEXT,                   -- 보고자
    isu_exctv_rgist_at   TEXT,              -- 등기임원 여부
    isu_exctv_ofcps      TEXT,              -- 직위
    isu_main_shrholdr    TEXT,              -- 주요주주 구분
    sp_stock_lmp_cnt     BIGINT,            -- 특정증권등 소유수량
    sp_stock_lmp_irds_cnt BIGINT,           -- 증감수량
    sp_stock_lmp_rate    NUMERIC,
    raw_payload     JSONB       NOT NULL,
    source          TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    UNIQUE (corp_code, rcept_no, repror, sp_stock_lmp_cnt, sp_stock_lmp_irds_cnt)
);

CREATE TABLE IF NOT EXISTS dart_major_holding_raw (
    raw_id          BIGSERIAL   PRIMARY KEY,
    corp_code       TEXT        NOT NULL,
    ticker          TEXT,
    rcept_no        TEXT        NOT NULL,
    rcept_dt        DATE,
    repror          TEXT,
    stkqy           BIGINT,                 -- 보유주식등의 수
    stkqy_irds      BIGINT,                 -- 증감
    stkrt           NUMERIC,                -- 보유비율
    stkrt_irds      NUMERIC,
    report_resn     TEXT,                   -- 보고사유 ★ 경영참여/단순투자 구분
    raw_payload     JSONB       NOT NULL,
    source          TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    UNIQUE (corp_code, rcept_no, repror, stkqy)
);
```

**컬럼명은 §3.5에서 확인한 실제 응답 필드 기준이다.** `majorstock`에는 `report_tp`·
`ctr_stkqy`·`ctr_stkrt`도 있으니 컬럼으로 뽑을지 PoC 때 정한다.
**`raw_payload` JSONB에 원본을 통째로 남기므로, 컬럼을 몇 개 놓쳐도 나중에 복구된다** —
이 저장소의 raw 레이어 원칙 그대로다.

`UNIQUE` 제약이 skip-if-present와 idempotency의 근거다. **한 접수번호에 보고자·종류가 여러 행
들어올 수 있어** 단일 `rcept_no`로는 부족하다. `00_status.md` §4c의 B-2 결함(한 filing의 fact가
여러 기간·축에 걸치는데 filing당 하나로 다뤘다)과 **같은 함정**이므로 PoC에서 실제 다중 행
패턴을 반드시 확인한다.

**단 값 컬럼(`sp_stock_lmp_cnt` 등)을 UNIQUE에 넣는 것은 위험하다.** 값이 정정되면 같은
논리적 행이 중복 삽입된다. **`(corp_code, rcept_no, row_ordinal)`**로 가는 편이 안전하다 —
`row_ordinal`은 응답 배열 안의 순서다. 순서가 불안정하면 **payload 해시**로 대체한다.
어느 쪽으로 갈지는 PoC에서 같은 corp를 두 번 호출해 순서가 유지되는지 보고 정한다.

**등록 6곳**(`01` §1). 익스포터는 `raw_id_range` 전략, 파티션은 `["year(rcept_dt)"]`,
`jsonb_columns = ["raw_payload"]`, `bin/...sh`의 `raw_id_tables` 배열.

---

## 5. 작업 순서

### N5-PR1 — PoC ★ (다른 무엇보다 먼저)

```
확인 항목
├─ ★ 응답 범위 — 전체 이력인가 최근분인가          → §2의 100배 갈림길
│                                                    최근분이면 §3.5 ②로 후순위 강등
├─ 한 rcept_no에 몇 행이 오는가 (보고자·증권종류 다중)
├─ 같은 corp를 두 번 호출했을 때 행 순서가 유지되는가 → row_ordinal vs payload hash
├─ sp_stock_lmp_irds_cnt의 부호 분포 (음수가 실제로 오는가)
├─ 필드 타입·결측 패턴
└─ 상장 전 이력·비상장 기간 데이터가 섞이는가
```

요청 파라미터는 이미 확인됐다 — **`corp_code`뿐**이다(§3.5).

**대상은 이력이 길고 짧은 것을 섞는다** — 오래된 대형주(005930), 최근 상장(259960),
경영권 분쟁 이력이 있는 종목, 지주회사.

산출물: `poc/n5_ownership.md` + **확정 볼륨 추정** + 수집 전략 결정.

**PoC 결과가 나쁘면(최근분만 제공) 이 작업 패키지의 우선순위를 재검토한다.** 수십만 호출은
N6과 같은 급이라 2차가 아니라 3차로 내려간다.

### N5-PR2 — 스키마 + 등록 6곳

PoC에서 확정한 필드로 DDL을 고쳐 쓴다.

### N5-PR3 — 어댑터 + 포트

```
ports/ownership.py                    OwnershipDisclosureProvider(Protocol)
                                        fetch_insider_holdings(corp, ...) -> ...
                                        fetch_major_holdings(corp, ...) -> ...
adapters/opendart_ownership/provider.py
domain/models.py                      InsiderHoldingRow, MajorHoldingRow, OwnershipSyncResult
domain/enums.py                       RunType.DART_OWNERSHIP_SYNC = "dart_ownership_sync"
```

OpenDART 다중키 실행기 사용(`01` §2.2) — exit 75 재개 포함.

### N5-PR4 — 서비스 + CLI

```
service/sync_dart_ownership.py
cli/app.py:  dart sync-ownership [--kind insider|major|all] [--tickers] [--years] [--force]
```

skip-if-present 키는 **`(corp_code)`**다. 연도 축이 없다(§3.5 ②). 다만 **최신 공시가 계속
쌓이므로 주기적 재실행이 필요**하다 — `dart sync-filings`가 "현재 연도는 항상 재조회"하는
패턴과 같은 성격이다. `--refresh-older-than` 같은 인자로 재조회 주기를 제어한다.

### N5-PR5 — 테스트

- 다중 행 rcept_no 파싱 (§4의 함정)
- skip-if-present, `--force`, 부분 실패 → `partial`
- **픽스처를 실제 모양으로** — 한 접수에 보고자 3명, 증권 종류 2개가 섞인 형태

### N5-PR6 — 실행

```bash
uv run krx-collector dart sync-ownership --kind insider
uv run krx-collector dart sync-ownership --kind major
```

`bin/dart-backfill-all-years.sh`에 단계로 붙일지는 볼륨에 따라 정한다.

---

## 6. PIT 규칙 — 여기가 설계의 핵심

**노출 시점의 후보는 `rcept_dt` 하나뿐이다.** 응답에 변동일이 없기 때문이다(§3.5 ①).
그래서 룩어헤드를 막는 방식이 재무와 조금 다르다.

**접수시각(장중/장후)도 없다.** 당일 종가 시점에 이미 알려졌다고 가정하면, 장 마감 직전
접수 건에서 룩어헤드가 생긴다. 그래서

> **노출 시점 = `rcept_dt`의 다음 거래일.**

`02_feature_candidate.md` §2.2가 재무에 대해 "접수시각 dimension 확보 전에는 보수 lag"를
쓴 것과 같은 논리다. 보수적으로 하루 미루는 비용보다 룩어헤드 위험이 크다.

**`insider_report_lag_days`는 정의할 수 없다.** 변동일이 없어서다. 처음 계획에 있었으나
삭제한다. 대신 **접수 밀집도**(같은 corp의 최근 60일 보고 건수)를 보조 지표로 둔다.

---

## 7. 결과 보기 전에 고정할 것

1. **피쳐 이름과 해석** — `ins_holding_increase`로 둔다. **"매수"라고 부르지 않는다**(§3.5 ①).
   증가분에 스톡옵션·무상증자·상속이 섞여 있다는 사실을 이름과 문서에 남긴다
   - 권고: `ins_holding_increase_60d`(증가만)와 `ins_holding_net_60d`(순증감)를
     **둘 다 사전등록**하되 부호는 각각 `+`로 고정
2. **윈도** — `02` §6.2가 window grid search를 금지한다. **{60, 120}일 두 개만** 사전등록한다
   (보고 빈도가 낮아 짧은 윈도는 표본이 안 나온다)
3. **정규화 분모** — **`listed_shares`**([N1](02_w1_daily_market_cap.md))로 간다. 응답이
   수량이므로 수량 기준 분모가 맞다. 처음에 시가총액을 제안했으나 단위가 안 맞는다.
   `sp_stock_lmp_rate`(원천이 준 비율)와 교차검증한다
4. **`majorstock`의 `report_resn`(보고사유) 취급** — 경영참여와 단순투자는 다른 이벤트다.
   섞을지 나눌지 미리 정한다. **권고: 나눈다**

---

## 8. 리스크

| 리스크 | 대응 |
|---|---|
| **응답 범위 미확인 → 볼륨 100배 차이** | N5-PR1 PoC가 전제. **연도 루프 우회가 불가능**하므로(§3.5 ②) 최근분만이면 후순위 강등 |
| **증감이 매수가 아니다** | §3.5 ① — 피쳐명·문서에 명시. 원인 파싱은 N9 범위 |
| 한 접수에 다중 행 → 조인 버그 | B-2 결함(§4)과 같은 함정. 픽스처를 실제 모양으로 |
| 표본이 얇다 (내부자 거래는 빈도가 낮다) | 종목·기간별 관측 수를 먼저 세고 검정력을 판단. **`fin_sue` 표본 0 사례를 반복하지 않는다** |
| 접수일 당일 노출 → 룩어헤드 | §6 — **다음 거래일 노출**. as-of join 테스트를 명시적으로 작성 |
| 지주회사·특수관계인 중복 보고 | 보고자 단위 dedup 정책을 PoC에서 확정 |

**세 번째가 가장 현실적인 리스크다.** `fin_sue`는 coverage 0.0000으로 측정조차 못 했다
(`09` §7). N5도 **수집 직후 관측 수를 세어 검정력이 나오는지부터 확인**하고, 안 나오면
피쳐 개발로 넘어가지 않는다.

---

## 9. 완료 기준

공통 DoD(`01` §7)에 더해:

- [ ] `poc/n5_ownership.md` — **응답 범위 확정**과 볼륨 재추정
- [ ] 두 테이블 백필 완료, 연도별 행 수·종목 수 기록
- [ ] **커버리지 측정**: 종목당 연평균 보고 건수, 관측이 0인 종목 비율
- [ ] `sp_stock_lmp_irds_cnt` 부호 분포와 `sp_stock_lmp_rate` 정합성 기록
- [ ] §7의 네 결정이 결과 보기 전에 문서에 고정
- [ ] **§3.5 ② 판정 기록** — 응답 범위. "최근분만"이면 후순위 강등 결정을 문서에 남긴다
- [ ] **검정력 판단 기록**: 표본이 피쳐 검증에 충분한가. 부족하면 그렇게 적고 멈춘다
