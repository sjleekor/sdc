# 07. N6 — 직원 · 임원 · 최대주주 · 감사의견 (3차)

- 작성일: 2026-08-15
- 공통 규약: [`01_implementation_checklist.md`](01_implementation_checklist.md)
- 원천: OpenDART 정기보고서 주요정보(DS002) 5종
- 새 테이블: `dart_employee_raw`, `dart_governance_raw`
- 확정 규모: **83,700 요청 설계 · 2015~2025 raw 백필 완료**
- 전체 분석: [`poc/n6_analysis_20260827.md`](poc/n6_analysis_20260827.md)

---

## 1. 왜 — 구현은 가장 쉽고 볼륨만 크다

**구현 비용이 이 계획에서 가장 낮다.** 현재 이미 DS002 엔드포인트 4개를 쓰고 있다 —
어댑터 grep으로 확인했다.

```
alotMatter.json           → dart_shareholder_return_raw (dividend)
stockTotqySttus.json      → dart_share_count_raw
tesstkAcqsDspsSttus.json  → dart_shareholder_return_raw (treasury_stock)
irdsSttus.json            → dart_capital_change_raw
```

**키 구조(`corp_code × bsns_year × reprt_code`)와 저장 모양이 완전히 같다.** 다중키 실행기,
exit 75 재개, skip-if-present, 부분실패 finalizer가 전부 그대로 쓰인다.

**그런데 볼륨이 크다.** 그래서 3차다.

### 왜 이 데이터인가

업계는 workforce analytics(고용 추이·이직률·채용공고)를 대체데이터로 비싸게 산다.
**한국은 직원 수와 1인 평균 급여가 정기보고서에 공시된다.** 무료다.
`11_feature_taxonomy.md` §9.9의 "유료 대체데이터의 무료 근사"가 이것이다.

| API | 내용 | 후보 피쳐 |
|---|---|---|
| `empSttus` | 직원 현황 — 직원 수, 평균 근속연수, 1인 평균 급여 | `hc_employee_growth_yoy`, `hc_revenue_per_employee`, `hc_avg_pay_growth` |
| `exctvSttus` | 임원 현황 | 경영진 교체율 |
| `hyslrSttus` | 최대주주 현황 | `own_major_stake` |
| `hyslrChgSttus` | 최대주주 **변동** 현황 | `own_control_change` — **T3(전이)형** |
| `accnutAdtorNmNdAdtOpinion` | 감사인·감사의견 | **비적정 의견 = 강한 부실 신호** |

**`hyslrChgSttus`와 감사의견이 특히 값이 있다.** 둘 다 `11_feature_taxonomy.md`가 지적한
T3(상태 전이) 형태이고, 현재 25 family에 T3가 0개다.

---

## 2. 볼륨 — 사업보고서만 받는다

```text
2,700 corp × 12 년 × 5 endpoint × 4 reprt = 648,000 호출   ← 전부
2,700 corp × 12 년 × 5 endpoint × 1 reprt = 162,000 호출   ← 사업보고서(11011)만
```

**연 1회로 충분하다.** 직원 수·최대주주·감사의견을 분기별로 볼 이유가 없다. 분기 공시에도
나오지만 값이 거의 안 변한다.

**단 대상 수를 2,700으로 잡으면 안 된다.** 현재 상장사만 대상으로 하면 **감사의견·부실 신호
피쳐에 생존편향이 그대로 들어온다** — 비적정 의견을 받고 상폐된 기업이 표본에서 빠지기 때문이다.
이 작업 패키지의 핵심 후보가 바로 그 부실 신호인데 편향된 표본으로 재면 의미가 없다.

> **대상은 [N3](04_w1_pit_universe.md)의 PIT 유니버스와 `dart_filing_receipt_raw`(2015~2026)를
> 합쳐 만든 "역사적 상장사" 집합**이다. ticker 매핑 법인 3,959건이 상한이다.
> **N3가 선행 조건이 된다** — 원래 3차였지만 대상 산출이 1차 결과에 의존한다.

키 3개(일 60,000)면 **약 3일**이다. `bin/dart-backfill-all-years.sh`와 같은 규모라
운영 패턴이 이미 있다.

**예외 하나.** `hyslrChgSttus`(최대주주 변동)는 **전이 이벤트라 시점 해상도가 중요**하다.
연 1회 스냅샷으로는 변동 시점을 못 잡는다. PoC에서 응답에 변동일자가 들어 있는지 확인하고,
없으면 **이것만 분기까지 받는다**(추가 2,700 × 12 × 3 = 97,200 호출).

---

## 3. 스키마 — 테이블 두 개로 묶는다

엔드포인트가 5개지만 테이블을 5개 만들지 않는다. **성격이 둘로 갈린다.**

```sql
-- 1) 직원·임원 — 사람과 보수
CREATE TABLE IF NOT EXISTS dart_employee_raw (
    raw_id          BIGSERIAL   PRIMARY KEY,
    corp_code       TEXT        NOT NULL,
    ticker          TEXT,
    bsns_year       INT         NOT NULL,
    reprt_code      TEXT        NOT NULL,
    rcept_no        TEXT        NOT NULL,   -- ★ NOT NULL + UNIQUE 포함 (아래 vintage 설명)
    statement_type  TEXT        NOT NULL,   -- 'employee' | 'executive'
    row_ordinal     INT         NOT NULL,   -- 응답 배열 안의 순서 (또는 payload hash)
    raw_payload     JSONB       NOT NULL,
    source          TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    UNIQUE (corp_code, bsns_year, reprt_code, statement_type, rcept_no, row_ordinal)
);

-- 2) 지배구조 — 최대주주·감사의견
CREATE TABLE IF NOT EXISTS dart_governance_raw (
    raw_id          BIGSERIAL   PRIMARY KEY,
    corp_code       TEXT        NOT NULL,
    ticker          TEXT,
    bsns_year       INT         NOT NULL,
    reprt_code      TEXT        NOT NULL,
    rcept_no        TEXT        NOT NULL,
    statement_type  TEXT        NOT NULL,   -- 'major_shareholder' | 'major_change' | 'audit_opinion'
    row_ordinal     INT         NOT NULL,
    raw_payload     JSONB       NOT NULL,
    source          TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    UNIQUE (corp_code, bsns_year, reprt_code, statement_type, rcept_no, row_ordinal)
);
```

**`rcept_no`를 NOT NULL로 두고 UNIQUE에 넣는 이유가 vintage다.** 같은 사업연도 보고서가
정정되면 새 접수번호가 붙는다. UNIQUE에 `rcept_no`가 없으면 **정정본이 기존 행을 덮어써서
과거 시점 값을 복원할 수 없다.** `dart_capital_change_raw`에서 vintage 정책 probe까지 돌려
strict PIT을 채택한 트랙에서 같은 실수를 반복하면 안 된다.

**`row_key`(자연어 조합) 대신 `row_ordinal`을 쓴다.** 부문명·성별 같은 자연어는 연도마다
표기가 바뀌어 조인이 깨진다. 응답 배열의 순서가 안정적이지 않으면 **payload 해시**로 대체한다 —
PoC에서 같은 요청을 두 번 보내 순서가 유지되는지 확인한 뒤 정한다.

**`statement_type` + `raw_payload` 패턴은 기존 `dart_shareholder_return_raw`와 같다.**
그 테이블도 `dividend`/`treasury_stock` 두 종류를 한 테이블에 담고 있다. 같은 규율을 따르면
익스포터·미러링 등록이 5개가 아니라 2개로 끝난다.

**한 응답에 몇 행이 오는지를 PoC에서 확인한다** — 직원 현황은 사업부문·성별로, 최대주주는
특수관계인별로 여러 행이 온다. `00_status.md` §4c의 B-2 결함과 같은 함정이다.

**등록 6곳**(`01` §1). `raw_id_range` 전략, 파티션 `["bsns_year", "reprt_code"]`,
`jsonb_columns = ["raw_payload"]`, `raw_id_tables` 배열.

---

## 3.5. 이건 vintage PIT이 아니라 final-vintage 백필이다

**계획에서 반드시 명시해야 할 한계다.**

[DS002 개발가이드](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019011)를
확인했다. `empSttus`의 요청 인자는 **`corp_code` + `bsns_year` + `reprt_code`뿐이고,
`rcept_no`로 조회하는 인자가 없다.** 응답에 `rcept_no`가 담겨 나올 뿐이다.

즉 오늘 FY2016 보고서를 요청하면 **2017년 당시 원본이 아니라 그 뒤 정정까지 반영된 최종본**이
온다. 나머지 4개 엔드포인트도 같은 구조다.

> **결론: N6 백필로 얻는 것은 "final-vintage"다.** 각 시점에 실제로 공시돼 있던 값이 아니다.

**두 가지 대응 중 하나를 고른다.**

| 안 | 내용 | 비용 |
|---|---|---|
| **A (권고)** | **한계를 명시하고 final-vintage로 쓴다.** `rcept_no`를 저장해 두면 이후 재수집에서 정정 발생 시점을 관측할 수 있다 | 0 |
| B | 접수번호별 원문 파싱으로 엄격 vintage 복원 | [N9](09_w4_filing_text.md)급 |

**A를 고르면 그 사실이 피쳐 문서와 evidence grade에 따라붙어야 한다.** `dart_capital_change_raw`
vintage probe(`09` §8)처럼 "얼마나 자주 바뀌는가"를 재는 것은 가능하다 — 지금 백필한 값과
1년 뒤 재수집한 값을 비교하면 된다. **다만 그건 앞으로 쌓이는 데이터에서만 가능하고,
과거 구간에는 소급 적용할 수 없다.**

감사의견·최대주주 변동처럼 **정정이 곧 신호인 항목**에서는 이 한계가 특히 아프다.
그래서 **부실 신호 피쳐는 final-vintage 위에서 결론 내지 않는다.**

> **2026-08-27 실측.** employee 28,283 corp-year 중 사업보고서 접수번호가 둘 이상인 곳은
> 6,744건이다. filing receipt 36,673개 중 N6 값과 짝지어진 건 28,437개뿐이라 과거 접수번호
> **8,236개가 값 없이 남는다.** 추가 vintage 보존율은 1.84%다. 채택 후보는 최종
> `rcept_no` 날짜부터만 쓰고 `source_warning=final_vintage`, evidence grade 상한 `B`를 둔다.

---

## 4. 작업 순서

### N6-PR1 — PoC (호출 50건 미만)

5개 엔드포인트 × 대표 종목 10개.

```
확인 항목
├─ 응답 행 수와 구분 차원, ★ 같은 요청 2회의 행 순서 안정성 → row_ordinal vs payload hash
├─ rcept_no 값 분포        → available_from 계산 가능한가, 정정본 식별 가능한가
├─ hyslrChgSttus에 변동일자가 있는가  → §2의 예외 판단
├─ 감사의견 코드 체계 (적정/한정/부적정/의견거절)
├─ 직원 수 필드 — 정규직/계약직/합계 구분
└─ 2015년 데이터 존재 여부 (2015는 사업보고서만 수집돼 있다)
```

산출물: `poc/n6_periodic_extras.md`.

### N6-PR2 — 스키마 + 등록 6곳 + **완료 ledger**

§3 그대로. 그리고 **`01` §2.4의 `collection_slice_state` ledger를 여기서 공통 컴포넌트로
만든다.** 16만 호출이 며칠에 걸쳐 여러 run으로 나뉘는데, 현재 완료 상태 추적에는 한계가 있다.

- `no_data_request_keys`가 run당 **1,000개로 잘린다**
- negative cache가 **최근 20개 run만** 읽는다

즉 `ingestion_runs.params`로는 며칠짜리 백필의 완료 상태를 복원할 수 없다. **N6이 이 계획에서
ledger가 필수인 유일한 작업 패키지다.**

### N6-PR3 — 어댑터 + 포트

```
ports/employee.py / ports/governance.py   (또는 하나로 묶어 ports/periodic_extras.py)
adapters/opendart_periodic_extras/provider.py
domain/enums.py    RunType.DART_EMPLOYEE_SYNC, RunType.DART_GOVERNANCE_SYNC
```

**기존 `opendart_share_info` 어댑터의 구조를 그대로 복제**한다 — 그쪽이 이미 한 어댑터에서
여러 DS002 엔드포인트를 다룬다.

### N6-PR4 — 서비스 + CLI

```
service/sync_dart_periodic_extras.py
cli/app.py:  dart sync-periodic-extras
               [--kind employee|executive|major|major-change|audit|all]
               [--years] [--tickers] [--reprt-codes] [--force]
```

`--reprt-codes` 기본값은 **`11011`(사업보고서)만**이다(§2).

### N6-PR5 — 테스트

- 5종 파싱, 다중 행, skip-if-present, 부분 실패 → `partial`
- **정정본 시나리오** — 같은 `(corp, year, reprt)`에 다른 `rcept_no`가 오면 **덮어쓰지 않고
  행이 늘어나는지**. §3.5의 핵심이다
- **exit 75 재개 경로** — 16만 호출이라 중간에 반드시 걸린다. **ledger 기반 재개**가 실제로
  동작하는지가 이 작업 패키지에서 가장 중요한 테스트다

### N6-PR6 — 백필

```bash
# 연도를 나눠 돌린다. 한 번에 다 하려다 exit 75로 끊기면 진행 상황 파악이 어렵다
uv run krx-collector dart sync-periodic-extras --years 2015,2016,2017 --kind all
uv run krx-collector dart sync-periodic-extras --years 2018,2019,2020 --kind all
...
```

`bin/dart-backfill-all-years.sh`에 단계로 추가하되, **기존 단계(financials·share-info·xbrl·
filings)보다 뒤에 붙인다.** 앞 단계가 키 한도를 먼저 쓰는 게 낫다 — 그쪽이 더 중요하다.

---

## 5. 밸류업 공시 — 같이 볼 것

2024년 정부가 기업가치 제고 계획 공시를 도입했고 2026년 7월 기준 **749개사**가 공시했다.
2024-09에 밸류업 지수 100종목이 나왔다.

**`ev_payout_yield`·`ev_net_share_issuance_yoy`가 이 레짐 변화 위에 놓여 있다.**
`02_feature_candidate.md` D3도 "2025~ 밸류업 정책으로 소각 증가 — 레짐 변화 유의"라고 적어뒀다.

수집 경로는 `dart_filing_receipt_raw`의 `report_nm` 분류로 잡힐 가능성이 높다 —
**신규 수집이 아니라 기존 데이터 분류 작업**이다. N6 PoC 때 같이 확인한다.

**다만 검정은 지금 하지 않는다.** 표본이 2024년 이후 2년뿐이고 holdout 경계가 2025-08-01이다.
**검정력이 거의 없다.** 수집·분류만 해두고 표본이 쌓이면 본다.

---

## 6. 결과 보기 전에 고정할 것 — **2026-08-18 확정 (N6-7)**

PoC(`poc/n6_periodic_extras.md` §6) 권고안대로 다섯 개를 고정한다. ⑤만 권고안에
숫자가 없어 규칙을 새로 세웠고, 그 과정에서 **권고안이 지목한 대조 원천이 틀렸다는
것이 측정으로 드러났다**(⑤ 참고).

1. **직원 수 정의 → `sm`(합계).** 정규직(`rgllbr_co`)과 계약직(`cnttk_co`)을 나누는
   기준이 기업마다 다르다. 합계만 그 차이를 안 탄다
2. **`hc_revenue_per_employee` 분모 → 기말 직원 수.** 직원 수는 기말 스냅샷이고 매출은
   기간 누적이라 시점이 안 맞지만, 기초·기말 평균을 쓰면 전년 값이 필요해진다 —
   상장 첫 해와 공시가 빠진 해가 전부 빈 값이 된다
3. **감사의견 인코딩 → 이진**(적정 / 비적정). 비적정이 드물어 4단계 순서형은 칸마다
   표본이 안 남는다
4. **부호** — 문헌 방향이 명확한 것만 고정한다
   - 감사 비적정 → `−` (부실 신호) **고정**
   - 최대주주 지분 → **미고정.** 지배구조 문헌은 방향이 갈린다(참호 효과 vs 이해일치).
     `flow_individual_netbuy_to_volume`처럼 **미고정임을 사전에 적는다**
   - 직원 증가 → **미고정.** 성장 신호이면서 동시에 비용 증가다
5. **합병·분할 보정** — 아래 2게이트. 둘 다 맞을 때만 `hc_employee_growth_yoy`를
   **결측 처리**한다

   | 게이트 | 조건 |
   |---|---|
   | 증거 | 해당 사업연도 안에 `dart_filing_receipt_raw.report_nm`이 `합병등종료보고서(분할)` 또는 `합병등종료보고서(합병)` |
   | 크기 | \|`hc_employee_growth_yoy`\| **≥ 30%** — PoC 표준편차 13.7%의 약 2σ |

   증거만 있고 크기가 미달이면 결측 없이 **플래그 `hc_structural_change = 1`만** 남긴다.
   작은 자회사 합병까지 결측 처리하면 멀쩡한 관측을 버리게 되고, 플래그를 남겨두면
   "증거가 있는 해는 전부 뺀다"로 바꿀 때 재수집이 필요 없다.

   **`dart_capital_change_raw`는 쓰지 않는다.** PoC 권고안은 "`listed_shares` 급변 또는
   `dart_capital_change_raw`와 대조"였는데, **동기가 된 LG화학 사례를 바로 그 방법이
   놓친다.** 물적분할은 모회사 주식 수를 바꾸지 않기 때문이다 — 2026-08-12 lake로 확인한
   결과 LG화학(`00356361`)의 2019~2021 `dart_capital_change_raw`에는 2020년 분할 흔적이
   전혀 없고, `isu_dcrs_stle` 전체 값 15종에도 **합병·회사분할이 아예 없다**(`주식분할`
   7,067건은 액면분할이라 다른 사건이다). 반면 `합병등종료보고서(분할)`은 2020-12-04로
   정확히 찍혀 있다. **결정(`주요사항보고서(회사분할결정)`)이 아니라 종료보고서를 쓰는
   이유**도 같다 — 결정은 철회될 수 있다.

   적용 규모: 2015~2025 상장 corp-year **43,549건 중 증거가 있는 해는 1,023건(2.35%)**,
   크기 게이트를 통과하는 건 그보다 훨씬 적다.

   **한계** — `dart_filing_receipt_raw`가 `active_only=True`로 수집돼 있어
   ([`poc/survivorship_gap.md`](poc/survivorship_gap.md)) **상폐 기업은 이 보정을 못
   받는다.** N6-5(S-1 선행)로 대상 집합을 다시 짤 때 같이 재적용한다.

   **2026-08-27 재측정** — S-1 확장 뒤 2015~2025 증거는 **1,175 corp-year**다. 연속
   직원 수가 있는 1,090건에 크기 게이트를 적용했고 **378건**을 mask한다. 위 1,023건은
   active 중심 filing으로 계산한 과거 값으로 남겨둔다.

---

## 7. 리스크

| 리스크 | 대응 |
|---|---|
| **16만 호출 — 키 한도로 여러 번 끊긴다** | exit 75 재개가 전제. N6-PR5에서 반드시 테스트 |
| 다른 DART 백필과 키 경쟁 | `dart-backfill-all-years.sh`에서 **마지막 단계**로. 야간 배치 |
| `row_ordinal` 순서 불안정 → 조인 버그 | B-2 결함과 같은 함정. PoC에서 2회 호출 순서 확인, 불안정하면 payload hash |
| 2015년 데이터 부재 | 2015는 사업보고서만 수집돼 있다. 시작 연도를 2016으로 내릴 수도 |
| 연 1회 해상도 → 전이 시점 부정확 | `hyslrChgSttus`만 분기로 올린다(§2 예외) |
| 표본은 크지만 **변동이 거의 없다** | 직원 수·최대주주는 연 단위로 잘 안 변한다. **횡단면 변동이 실제로 있는지 수집 직후 확인** |
| **final-vintage만 받는다** | §3.5 — 한계 명시. 부실 신호는 이 위에서 결론 내지 않는다 |
| 대상을 현재 상장사로 잡으면 생존편향 | §2 — N3 PIT 유니버스 + filing_receipt로 역사적 상장사 집합 구성 |

**마지막이 실질적으로 가장 큰 리스크다.** 3일 걸려 16만 호출을 해놓고 "값이 거의 안 변해서
횡단면 신호가 없다"가 나올 수 있다. 그래서 **N6-PR1 PoC에서 10개 종목의 5년치 변동 폭을
먼저 본다.** 변동이 없으면 이 작업 패키지를 접거나 축소한다.

---

## 8. 완료 기준

공통 DoD(`01` §7)에 더해:

- [x] `poc/n6_periodic_extras.md` — `row_ordinal` vs hash 확정, **변동 폭 사전 확인**
- [x] **`collection_slice_state` ledger 구현·동작 확인** (`01` §2.4)
- [x] 대상 집합이 **역사적 상장사**로 구성됐는가 (현재 상장사 2,700이 아님)
- [x] **§3.5 final-vintage 한계가 문서에 명시**되고, 부실 신호 피쳐에 그 제한이 붙었는가
- [x] 2015~2025 사업보고서 백필 완료, 연도별 coverage 기록
- [x] exit 75 재개 경로가 ledger 테스트와 실 DB slice 재실행에서 동작했는가
- [x] **횡단면 변동 측정**: [`poc/n6_analysis_20260827.md`](poc/n6_analysis_20260827.md)
- [x] §6의 다섯 결정 고정
- [x] 밸류업 공시 분류 가능성 확인 결과(§5)

---

## 9. 2026-08-27 최종 후보

전체 백필 분석 결과 다음 4개 feature만 새 Horizon Scan config에 넣는다.

- `hc_employee_growth_yoy`
- `hc_revenue_per_employee`
- `own_major_stake`
- `own_major_stake_chg`

`hc_avg_pay_growth`는 단위·집계 정규화 전까지 보류한다. `gov_audit_opinion_flag`와
`own_control_change`는 final-vintage·availability 계약을 통과하지 못해 `NE`다.
밸류업 분류는 1,104건·765법인까지 가능하지만 2024년 이후 표본이라 검정을 미룬다.
