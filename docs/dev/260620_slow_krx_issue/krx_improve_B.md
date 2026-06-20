# KRX Security Flows: 대체 출처 / 전종목 엔드포인트 전환안 (Plan B)

작성일: 2026-06-20

관련 문서:

- `krx_flows_slow_issue_analysis.md` — slow issue 원인 분석 및 운영/스케줄 개선안 (Plan A)
- `krx_collection_throttle_timing.md` — KRX 수집 경로별 소요시간/throttle 비율 조사

## 이 문서의 질문

> security flows 수집에 시간이 너무 많이 걸린다. **KRX 말고 다른 곳에서** 받을 수 있는가?

결론을 먼저 정리한다.

1. **출처 전환(KRX → 타사)은 답이 아니다.** pykrx도 같은 KRX 서버를 긁고, FinanceDataReader는
   flow 데이터 자체를 제공하지 않으며, 한국 종목 flow는 KRX가 원천 독점이다.
2. **진짜 원인은 출처가 아니라 "요청 단위"다.** investor/shorting을 **종목당 1요청**으로
   받고 있어 ~2768 종목 × throttle이 발생한다.
3. **해법은 같은 KRX의 "전종목(all-tickers-per-date)" 엔드포인트로 전환**하는 것이다. KRX에는
   이미 전종목 엔드포인트가 있고, 현재 어댑터가 foreign에만 그걸 쓰고 investor/shorting에는
   안 쓰고 있다. 전환 시 요청 수가 100배 이상 감소한다.

## 1. 현재 무엇을 어떻게 받고 있나 (코드 검증)

기준 코드: `src/krx_collector/adapters/flows_krx/provider.py`, `parsers.py`.
모두 KRX MDC(`data.krx.co.kr`) JSON 엔드포인트 직접 호출이다.

| group | bld | pykrx 대응 클래스 | 요청 단위 | 요청 수(대략) | 속도 |
|---|---|---|---|---:|---|
| foreign_holding | `MDCSTAT03701` | - | **전종목 / (날짜 × 시장)** | 거래일 × 시장 = 수십 | ✅ 빠름 |
| investor | `MDCSTAT02302` | `투자자별_거래실적_개별종목` | **종목당 1요청** (range) | ~2768 | ❌ 느림 |
| shorting status (volume/value) | `MDCSTAT30001` | `개별종목_공매도_종합정보` | **종목당 1요청** (range) | ~2768 | ❌ 느림 |
| shorting balance | `MDCSTAT30502` | `개별종목_공매도_잔고` | **종목당 1요청** (위와 묶여 2 POST) | ~2768 | ❌ 느림 |

즉 foreign만 전종목 엔드포인트를 쓰고, investor·shorting은 종목 단위라
~5500 요청 × `8.0s` logical rate limit이 누적되어 multi-hour가 된다(Plan A "비용 모델" 참고).

## 2. 대체 출처 후보 검증 결과

| 후보 | repo 의존성 | flow 제공? | 실제 출처 | 판정 |
|---|---|---|---|---|
| **pykrx** | 이미 있음 (`pykrx>=1.2.8`) | 제공 | **동일 `data.krx.co.kr`, 동일 `MDCSTAT*` bld** (`KrxWebIo`) | ❌ 출처 전환 의미 없음 — throttle/차단 특성 동일 |
| **FinanceDataReader** | 이미 있음 (`finance-datareader>=0.9.110`) | 미제공 | - | ❌ short/investor/foreign flow 자체 없음 |
| 네이버/다음 금융, 증권사 API(키움·KIS 등) | 없음 | 일부 | 결국 KRX 재배포 | ❌ 진짜 대체 출처 아님, 실시간/공식 일별 flow 무료 제공 없음 |
| 유료 데이터 벤더 | 없음 | 제공 | KRX 라이선스 | △ 비용/계약 별도 논의 |

검증 근거:

- pykrx의 `get_shorting_*` / `get_market_net_purchases_*` 함수는 `website/krx/market/core.py`의
  `KrxWebIo` 서브클래스를 호출하며, 그 `bld`가 우리가 쓰는 것과 **동일한 `dbms/MDC/STAT/...`
  코드**다(아래 §3 표). 즉 pykrx = "KRX를 다른 코드로 긁는 것"일 뿐이다.
- FinanceDataReader 소스에 short/investor/순매수/공매도 관련 구현 없음(grep 0건).

→ **"다른 곳"은 사실상 없다. 단, pykrx 코드를 통해 KRX 자체의 더 효율적인 엔드포인트를
발견했다.**

## 3. 진짜 해법: 같은 KRX의 "전종목" 엔드포인트로 전환

pykrx가 사용하는, **현재 우리 어댑터가 쓰지 않는 전종목 엔드포인트**가 존재한다.
요청 단위를 "종목당" → "날짜·시장당"으로 바꾼다.

| 데이터 | 현재 (종목 단위) | 전환 대상 (전종목 / 날짜·시장) | pykrx 대응 | 요청 수 변화 |
|---|---|---|---|---|
| shorting 거래(volume/value) | `MDCSTAT30001` 종목별 | **`MDCSTAT30101`** | `개별종목_공매도_거래_전종목` | ~2768 → 거래일 × 시장 |
| shorting 잔고(balance) | `MDCSTAT30502` 종목별 | **`MDCSTAT30501`** (날짜 단위) | `전종목_공매도_잔고` | ~2768 → 거래일 × 시장 |
| investor 순매수 | `MDCSTAT02302` 종목별 | **`MDCSTAT02401`** | `투자자별_순매수상위종목` | ⚠️ §4 주의 |

### 효과 추정

shorting 두 metric을 전종목으로 바꾸면 요청 수가 대략
`거래일수 × 시장수 × 2(거래/잔고)` 수준이 된다.

- 예: lookback 11거래일 × 2시장 × 2 = **44건** 수준
- 현재 shorting ~5500 요청 대비 **100배 이상 감소**
- foreign처럼 분 단위로 끝난다.

이는 Plan A의 "분리 / lag-aware / lookback 축소"보다 **더 근본적**이다. Plan A는 요청 건수를
그대로 둔 채 스케줄·완전성으로 우회하는 반면, 이 전환은 **요청 건수 자체를 100배 줄인다.**

## 4. 주의점 / 리스크

### 4.1 investor 전종목 커버리지 불확실 (가장 큰 리스크)

`MDCSTAT02401`(`투자자별_순매수상위종목`)은 이름·docstring상 **"순매수 상위 종목"** 으로,
전종목이 아니라 **투자자유형별 상위 랭킹**만 반환할 가능성이 크다.

- shorting과 달리 investor는 "전종목 일별 순매수"를 한 번에 주는 공식 엔드포인트가 명확하지 않다.
- 따라서 **실제 응답 행 수가 전 종목을 덮는지 라이브로 검증**해야 한다.
- 전종목 보장이 안 되면 investor는 종목 단위(`MDCSTAT02302`)로 남기고,
  Plan A대로 daily critical path에서 분리해 별도 스케줄로 운영하는 것이 현실적이다.

### 4.2 파서 신규 작성 필요 (컬럼 스키마 상이)

전종목 엔드포인트는 종목별 엔드포인트와 **컬럼 구성이 다르다.** 현재 `parse_shorting_rows`는
`MDCSTAT30001`의 `CVSRTSELL_TRDVOL/TRDVAL`과 `MDCSTAT30502`의 `BAL_QTY`를 파싱하므로
재사용 불가.

- `MDCSTAT30101`(전종목 거래): `ISU_CD`(또는 단축코드), `CVSRTSELL_TRDVOL`, `CVSRTSELL_TRDVAL`,
  `ACC_TRDVOL`, `ACC_TRDVAL`, 비중 컬럼 — 행이 종목 단위로 펼쳐진다.
- `MDCSTAT30501`(전종목 잔고): `ISU_CD`, `BAL_QTY`, `LIST_SHRS`, `BAL_AMT`, `MKTCAP`, `BAL_RTO`.
- 각각에 대해 `parse_*_all_tickers` 파서를 새로 만들고, 기존
  `SHORT_SELLING_VOLUME/VALUE/BALANCE_QUANTITY` metric spec에 매핑한다.

### 4.3 KRX 차단 특성은 동일

엔드포인트를 바꿔도 **같은 KRX 서버**이므로 non-JSON(HTML) 차단·rate limit 위험은 그대로다.
다만 요청 수가 100배 줄면 차단 누적 자체가 크게 줄어드는 부수 효과가 있다.
HumanThrottle/error backoff 정책은 유지한다.

### 4.4 날짜 단위 요청과 lookback의 관계

전종목 엔드포인트는 "날짜(또는 짧은 range) × 시장" 단위다. lookback 11일이면 시장당 최대 11회
(또는 range 1회 × 시장) 수준이라, lookback을 키워도 비용이 선형으로만 늘고 종목 수와 무관해진다.
→ Plan A의 "lookback 축소" 필요성도 함께 완화된다.

## 5. 권장 진행 순서

1. **라이브 검증 (선행 필수)**
   - `MDCSTAT30101`, `MDCSTAT30501`이 전 종목을 실제로 반환하는지 응답 행 수 확인.
   - `MDCSTAT02401`(investor)이 전종목인지 상위 랭킹인지 행 수로 판별.
   - 컬럼명 실측(위 §4.2 가정 확정).
2. **shorting 전종목 어댑터 구현** (가장 큰 효과, investor와 독립적으로 선행 가능)
   - 전종목 거래(`30101`) + 전종목 잔고(`30501`) fetch + 파서 신규.
   - 요청 단위를 "날짜·시장"으로, upsert/완전성 판정은 기존 metric_code 유지.
3. **investor 처리 결정**
   - 전종목 가능 → investor도 전환.
   - 전종목 불가 → 종목 단위 유지 + Plan A의 분리 스케줄로 운영.
4. **foreign은 현행 유지** (이미 전종목).
5. 회귀 검증: 전종목 경로 결과가 기존 종목별 경로와 동일 값인지 표본 종목으로 대조.

## 6. Plan A와의 관계

- Plan A(분리 / lag-aware / lookback 축소 / circuit breaker)는 **요청 건수를 둔 채** 운영
  영향을 줄이는 우회책이다 — 즉시 적용 가능하고 운영 복구에 유효하다.
- Plan B(전종목 엔드포인트)는 **요청 건수 자체를 제거**하는 근본책이다 — 구현 비용이 있지만
  shorting을 분 단위로 끝낼 수 있다.
- 권장: **운영 복구는 Plan A로 즉시 처리**하고, **shorting은 Plan B로 근본 해결**한다.
  Plan B로 shorting이 빨라지면 Plan A의 shorting 분리/lag-aware는 상당 부분 불필요해진다.

## 7. 후속 확인 필요

- `MDCSTAT30101` / `MDCSTAT30501` / `MDCSTAT02401`의 실제 응답 스키마와 전종목 커버리지(라이브).
- KOSDAQ/KONEX 포함 시장 매핑(`STK`/`KSQ`/`KNX`)이 세 엔드포인트에서 동일하게 동작하는지.
- 전종목 잔고(`30501`)의 보고 기준일(`RPT_DUTY_OCCR_DD` 대응 컬럼)과 거래일 정렬 방식.
- 휴장/데이터 지연 시 전종목 엔드포인트가 빈 응답인지 직전일 응답인지(완전성 판정에 영향).
