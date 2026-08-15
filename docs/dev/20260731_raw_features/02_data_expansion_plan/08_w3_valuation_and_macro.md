# 08. N7 · N8 — KRX 공식 밸류에이션과 고용 지표 (3차)

- 작성일: 2026-08-15
- 공통 규약: [`01_implementation_checklist.md`](01_implementation_checklist.md)
- N7: pykrx `get_market_fundamental_by_ticker` · 새 테이블 `daily_market_fundamental`
- N8: ECOS 고용 시리즈 · **기존 `common_feature_series` 확장, 새 테이블 없음**
- 두 항목을 한 문서에 묶은 이유: 둘 다 작고, 둘 다 **피쳐가 아니라 보조 역할**로 시작한다

---

# 파트 A — N7. KRX 공식 밸류에이션

## A1. 왜 — 피쳐가 아니라 진단 원천이다

**`fin_value_z`(B등급)를 독립 원천으로 교차검증하기 위해서다.**

지금 밸류는 전부 DART canonical에서 만든다. 그런데 결함이 계속 나온다.

- `10_known_issues.md` **I1** — 구성요소가 없는 종목이 그날 그 시장의 1분위 값으로 대체돼
  전체 행의 **29.2%**가 "가장 비싼" 끝단에 몰린다
- **I7** — `revenue`·`gross_profit` canonical 매핑이 진짜 병목이다
- `revision_ratio` 0.1056~0.1259가 임계 0.10을 넘어 **등급 상한이 B로 묶였다**

**KRX가 매일 공표하는 PER/PBR/DIV가 있으면 어디까지가 데이터 문제인지 갈린다.** 값이 크게
어긋나는 종목·구간을 짚어내면 I1·I7의 실제 범위가 나온다.

## A2. 원천 확인 결과

```python
get_market_fundamental_by_ticker(date, market='KOSPI', alternative=False)
# docstring 반환 예시:
#            BPS        PER       PBR   EPS       DIV   DPS
# 티커
# 095570    6802   4.660156  0.669922   982  6.550781   300
```

**날짜별 전종목 1 호출.** 다만 [N1](02_w1_daily_market_cap.md) §2 ②와 같은 이유로
**시장 구분 컬럼이 없어 KOSPI/KOSDAQ을 따로 호출**해야 한다 → 약 **6,000 호출**.

**결측이 0으로 온다.** `wrap.py`가 `'-'`와 빈 문자열을 `"0"`으로 치환한 뒤 캐스팅한다.

```python
df = df.replace(r"\-$", "0", regex=True)
df = df.replace("", "0", regex=True)
df = df.astype({"BPS": np.int32, "PER": np.float64, ...})
```

> **따라서 "KRX PER 결측"이라는 것은 없다. `PER == 0`이 결측 sentinel이다.**
> 어댑터에서 `PER/PBR/DIV == 0` → NULL로 정규화한다. PBR이 진짜 0인 종목은 없고,
> PER이 0인 종목도 없다(적자면 KRX가 `-`로 준다).
> **DPS·EPS는 진짜 0이 가능하므로 그대로 둔다.**

> **미검증.** 실호출 실패. 위 치환 로직은 설치본 소스 기준이다. PoC 필요(`01` §3).

## A3. 스키마

```sql
CREATE TABLE IF NOT EXISTS daily_market_fundamental (
    trade_date      DATE        NOT NULL,
    ticker          TEXT        NOT NULL,
    market          TEXT        NOT NULL,
    bps             NUMERIC,
    per             NUMERIC,
    pbr             NUMERIC,
    eps             NUMERIC,
    div_yield       NUMERIC,    -- DIV (%)
    dps             NUMERIC,
    source          TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (trade_date, ticker, market)
);

CREATE INDEX IF NOT EXISTS ix_daily_market_fundamental_sync_cursor
    ON daily_market_fundamental (fetched_at, trade_date, ticker, market);
```

**등록 6곳**(`01` §1). `date_month` 전략, `date_month_tables` 배열 — N1과 같은 모양이다.
skip 규칙도 N1과 같다 — **행 존재가 아니라 슬라이스 완료**로 판정한다(`01` §2.4).

**N1과 합칠 수 있는가.** 키가 `(trade_date, ticker, market)`로 같아서 한 테이블에 넣고 싶어지는데,
**나눈다.** 원천 엔드포인트가 다르고 백필 시점이 다르며, N1은 1차·N7은 3차라 진행 속도가 다르다.
합치면 N7 백필이 끝날 때까지 N1 컬럼이 부분 채움 상태로 남는다.

## A4. 작업 순서

N1(`02_w1_daily_market_cap.md`)과 **구조가 거의 같다.** PR 구성도 같다.

| PR | 내용 |
|---|---|
| N7-PR1 | PoC — 컬럼명, **`0` sentinel 실측 확인**, 과거 구간 응답, 시장별 호출 |
| N7-PR2 | 스키마 + 등록 6곳 |
| N7-PR3 | 어댑터(`adapters/fundamental_pykrx/`) + 포트 + 도메인 |
| N7-PR4 | 스토리지 (슬라이스 = `(trade_date, market)`, 원자적 upsert + 행 수 대조) |
| N7-PR5 | 서비스 + CLI `prices fundamental-backfill` |
| N7-PR6 | 테스트 |
| N7-PR7 | 백필 (최근 → 과거 순) |
| **N7-PR8** | **대조 분석 — 이 작업의 주 산출물** |

**N1을 먼저 끝낸 뒤 시작하면 PR3~PR6은 거의 복사다.** 그래서 3차에 둬도 부담이 적다.

## A5. N7-PR8 대조 분석 — 실제 목적

| # | 측정 | 의미 |
|---|---|---|
| C1 | KRX `pbr` 역수 vs canonical `fin_book_to_market` 일별 rank 상관 | 낮으면 매핑 문제 |
| C2 | 상관이 낮은 종목의 특성 (업종·규모·`fin_age_days`) | I7 병목의 실제 범위 |
| C3 | **`PER == 0`(= 결측 sentinel) 종목 = 적자기업.** 그 집합이 I1의 "1분위 몰림" 집합과 겹치는가 | **I1의 직접 검증** |
| C4 | KRX `div_yield` vs `ev_payout_yield`(배당 부분) | 배당 매핑 검증 |
| C5 | KRX PBR로 만든 밸류 z와 `fin_value_z`의 IC 비교 | 어느 쪽이 나은가 — **exploratory 전용**(§A6) |

**C3가 핵심이다.** I1은 "구성요소가 없는 종목이 1분위로 대체된다"는 결함인데, 적자기업이
E/P를 못 내는 게 주요 원인이다. KRX가 적자기업 PER을 `-`(→ `0`)로 처리하므로 **두 집합의
교집합을 재면 I1의 원인이 확정된다.** `0`을 결측으로 정규화하지 않으면 이 측정 자체가 깨진다.

## A6. 용도를 분명히 한다 — 피쳐 승격은 나중

**KRX 값은 산출 규칙이 블랙박스다.** 그리고 적자기업 처리(PER 결측)가 `fin_value_z`의 설계
의도(적자기업 보존, `02` §3.3)와 **정면으로 다르다.**

> **N7은 진단 원천으로 먼저 쓴다. 피쳐로 승격할지는 A5 대조 결과를 보고 정한다.**

승격한다면 **`fin_value_z`의 대체가 아니라 별도 family**다. 산출 주체가 다르면 다른 피쳐다.

**그리고 C5의 IC는 exploratory다.** 같은 데이터로 "어느 쪽이 나은가"를 보고 그 결과로
승격을 정하면, 그 IC는 이미 선택에 쓰인 값이라 확증 근거가 못 된다. **승격하기로 정했으면
새 config로 사전등록해 confirmatory run을 따로 연다.** `02_feature_candidate.md` §6.2와
`04_specific_plan_B.md` §12의 규율 그대로다.

## A7. 완료 기준

- [ ] `poc/n7_fundamental.md`
- [ ] 2014-06 ~ 현재 백필 완료
- [ ] **C1~C5 대조 결과 문서화** — C5는 **exploratory로 표기**
- [ ] `0` sentinel 정규화가 코드에 반영됐는가 (PER/PBR/DIV만, EPS/DPS 제외)
- [ ] I1·I7의 실제 범위에 대한 결론 기록 → `10_known_issues.md`에 반영 여부 판단
- [ ] 피쳐 승격 여부 결정과 근거 기록

---

# 파트 B — N8. 고용 지표 (ECOS)

## B1. 왜 — 그리고 왜 우선순위가 낮은가

`common_feature_series` 33개에 **고용 관련 시리즈가 없다.** 유가·물가·통화·심리·금리·환율은
있다(`11_feature_taxonomy.md` §8.1). 사용자가 지목한 세 지표 중 유가·물가는 있고 **고용만
빠져 있다.**

**비용은 거의 0이다.** ECOS 어댑터가 이미 있고 `macro_cpi`·`macro_ppi`·`macro_m2`·
`macro_consumer_sentiment`가 같은 경로로 들어온다. **`definitions/common_features.py`에
시리즈 정의를 추가하고 `common seed` → `common sync`를 돌리면 끝이다.**

**그런데 순위는 뒤다.** 이유가 셋이다.

1. **월간이고 발표 지연이 크다.** `11_feature_taxonomy.md` §8.4의 경고를 그대로 받는다 —
   자기상관이 극도로 높아 **temporal placebo에 특히 취약**하다. 이번 검증에서 유의한 셀 24개를
   전부 떨어뜨린 게 그 게이트다
2. **기존 시리즈와 겹친다.** `macro_consumer_sentiment`·`macro_m2`가 이미 경기 국면을 잰다
3. **무엇보다 매크로 조건화 구조 자체가 0개다.** 레짐 변수의 종류를 늘리는 것보다
   **조건화를 한 번이라도 돌려보는 게 먼저다**(`11_feature_taxonomy.md` §8.3의 exposure ①)

## B2. 작업 내용

```
definitions/common_features.py
  + CommonFeatureSeries(series_id="macro_unemployment_rate", source=ECOS, freq=M, ...)
  + CommonFeatureSeries(series_id="macro_employment_rate",   source=ECOS, freq=M, ...)
  (+ 필요 시 macro_employed_persons)
```

**PIT 정책이 이미 있다.** 매크로 월간 시리즈는 `period-end + 20일 보수적 availability`
정책을 쓴다(`00_raw_feature_inventory.md` §4-8). 같은 정책을 그대로 적용한다.
**`available_from_date`를 반드시 쓴다.**

### B1.5. 발표 지연과 사후 개정은 다른 문제다

**`period-end + 20일`은 발표 지연(publication lag)만 막는다.** 고용·실업 통계는 그것 말고
문제가 하나 더 있다 — **계절조정 시계열이 사후에 개정된다.**

오늘 ECOS에서 2018년 실업률을 받으면, 그건 2018년 당시 공표값이 아니라 **그 뒤 여러 차례
개정된 최신 시계열**이다. `available_from_date`를 아무리 정확히 맞춰도 **값 자체에 미래
개정이 들어 있다.**

[N6 §3.5](07_w3_periodic_report_extras.md)의 final-vintage와 같은 종류의 문제다. 대응도 같다.

- ECOS가 vintage 조회를 지원하는지 **N8-PR1에서 확인**한다
- 지원하지 않으면 **한계를 명시하고 final-vintage로 쓴다.** 그리고
  **원계열(계절조정 전)을 우선 채택**한다 — 계절조정 값보다 개정 폭이 작다

| PR | 내용 |
|---|---|
| N8-PR1 | ECOS 통계표 코드 확인 + **vintage 조회 지원 여부** + 원계열/계절조정 선택 |
| N8-PR2 | `common_features.py`에 시리즈 추가 + 유닛 테스트 |
| N8-PR3 | `common seed` → `common sync` 실행, 커버리지 확인 |
| N8-PR4 | 기존 매크로 시리즈와의 중복 측정 → **중복이면 추가하지 않는다** |

**N8-PR4가 게이트다.** 다만 **raw level 상관은 게이트로 쓰지 않는다.** 월간 매크로 시계열은
서로 다른 지표끼리도 추세 때문에 level 상관이 높게 나온다. 의미가 없다.

> **실제 모델 입력 형태로 비교한다.** YoY · Δ · surprise(전월 대비 예상 밖 변화)를 만들고,
> **`available_from_date`로 정렬한 뒤** `macro_consumer_sentiment`·`macro_m2`의 같은 변환과
> 상관을 잰다. 그 상관이 0.8을 넘으면 추가하지 않는다.

카탈로그를 늘리는 것 자체가 비용이다 — readiness 게이트(§B3)가 그 비용의 일부다.

## B3. 같이 처리할 것 — inactive 시리즈 정리

`common_feature_series` 33개 중 **inactive 7개**가 있다 — pykrx 지수 폴백 3종,
KRX 산업지수 4종(데이터 0건).

- 산업지수 4종은 [N4](05_w2_industry_index.md) §6에서 다룬다
- pykrx 지수 폴백 3종은 여기서 정리한다 (활성화 / 삭제 중 택일)

**주의.** `00_status.md` §5-1이 지적한 대로 **common feature readiness 게이트가 창 정렬
문제로 37개 중 4개만 ready**다. macro 계열은 2013-06부터, 일별 계열은 2014-06부터 시작하는데
coverage 창이 가장 이른 날짜에 맞춰져 있어 뒤에 시작한 계열이 앞쪽 약 257 거래일을 비운
것으로 잡힌다.

> **시리즈를 늘리기 전에 이 창 정렬 문제를 먼저 정리하는 게 맞다.** 안 그러면 게이트가 더
> 어지러워진다. N8-PR2 전에 판단한다.

## B4. 완료 기준

- [ ] ECOS 통계표 코드 확인 기록 + **vintage 지원 여부**(§B1.5)
- [ ] 원계열/계절조정 선택과 근거
- [ ] **N8-PR4 중복 측정 결과** — **변환 후·availability 정렬 후** 상관. 중복이면
      "추가하지 않음"도 정당한 결론이다
- [ ] readiness 창 정렬 문제에 대한 판단 기록(§B3)
- [ ] inactive pykrx 폴백 3종 처리 결정
