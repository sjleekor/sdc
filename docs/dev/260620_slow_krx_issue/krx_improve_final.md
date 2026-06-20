# KRX Security Flows 개선 최종안

작성일: 2026-06-20  
수정 반영: 2026-06-20

상태: **Phase 1 shorting bulk, Phase 2 investor bulk, phase별 run counter 구현 반영 완료**.

운영 방침: KRX flows는 기존처럼 **하나의 `flows sync` 작업 안에서 일렬 실행**한다. Cronicle event를
foreign/investor/shorting으로 분리하거나 병렬화하는 변경은 이번 배포 범위에 포함하지 않는다.

본 문서는 다음 자료를 종합한 뒤, 2026-06-20 코드 반영 결과에 맞춰 갱신한 최종 배포 전 계획이다.

- `krx_improve_A.md` — KRX 외부 소스 검토 + 전종목 bulk endpoint 전환안
- `krx_improve_B.md` — 대체 출처 검증 + 전종목 endpoint 전환안
- `krx_flows_slow_issue_analysis.md` — slow issue 원인/운영 개선안
- `krx_collection_throttle_timing.md` — 경로별 소요시간/throttle 비율 실측

## 0. 한 줄 결론

`sdc_daily_krx_flows`가 26시간 이상 걸리던 근본 원인은 출처가 아니라 **요청 단위**였다.
기존 investor/shorting은 종목당 요청 구조였고, `8.0s` logical rate limit과 결합되어 multi-hour
실행 시간이 구조적으로 발생했다.

수정 후 수집 단위는 다음처럼 바뀌었다.

- `foreign_holding`: 기존처럼 `날짜 × 시장` 전종목 endpoint 유지
- `investor`: `MDCSTAT02401` bulk endpoint로 `날짜 × 시장` 단위 실행
- `shorting trading`: `MDCSTAT30101` bulk endpoint로 `날짜 × 시장` 단위 실행
- `shorting balance`: `MDCSTAT30501` bulk endpoint로 `날짜 × 시장` 단위 실행

11거래일, 2시장 기준 service logical request는 약 `5558`건에서 `88`건으로 줄고, 실제 KRX HTTP
POST는 약 `8326`건에서 `154`건으로 줄어든다. KRX 작업은 계속 일렬로 실행하지만, 일렬 실행의
대상이 종목 단위에서 날짜·시장 단위로 바뀌었기 때문에 구조적 24시간 초과 문제는 해소될 것으로
예상한다.

## 1. 확정 사실

| 항목 | 결론 |
|---|---|
| 외부 소스 대체 | 의미 없음. pykrx도 같은 KRX MDC bld를 사용하고, FinanceDataReader/네이버/증권사는 flow 원천 대체가 아니다. |
| 병목 원인 | 종목당 요청 구조. lookback 축소나 missing-only만으로는 정상 일일 요청 수를 근본적으로 줄일 수 없다. |
| shorting bulk | `MDCSTAT30101`, `MDCSTAT30501` 모두 전종목 row count와 표본 parity를 확인했고 구현 완료. |
| investor bulk | `MDCSTAT02401`은 100% active universe가 아니라 기존 per-ticker endpoint coverage와의 parity 기준으로 채택. `individual(8000)`을 base row set으로 삼고, 누락된 institution/foreign component는 0으로 보정한다. |
| foreign 합산 | `foreign(9000)` 단독이 아니라 `foreign(9000) + other_foreign(9001)`이 기존 외국인 순매수와 일치한다. |
| 운영 구조 | KRX flow 작업은 분리하지 않는다. public group은 `foreign_holding`, `investor`, `shorting`을 유지하고, bulk endpoint phase는 서비스 내부 구현 상세로 둔다. |

## 2. 현재 구현 구조

기준 파일:

- `src/krx_collector/ports/flows.py`
- `src/krx_collector/adapters/flows_krx/provider.py`
- `src/krx_collector/adapters/flows_krx/parsers.py`
- `src/krx_collector/service/sync_krx_flows.py`

| group / phase | endpoint | 실행 단위 | provider method | 비고 |
|---|---|---|---|---|
| `foreign_holding` | `MDCSTAT03701` | 날짜 × 시장 | `fetch_foreign_holding_shares` | 기존 전종목 경로 유지 |
| `investor_bulk` | `MDCSTAT02401` | 날짜 × 시장 | `fetch_investor_net_volume_bulk` | 내부에서 `7050`, `8000`, `9000`, `9001` 네 번 POST |
| `shorting_trading` | `MDCSTAT30101` | 날짜 × 시장 | `fetch_shorting_trading_bulk` | volume/value |
| `shorting_balance` | `MDCSTAT30501` | 날짜 × 시장 | `fetch_shorting_balance_bulk` | balance quantity |

legacy 종목별 메서드는 삭제하지 않고 유지한다.

- `fetch_investor_net_volume` → `MDCSTAT02302`
- `fetch_shorting_metrics` → `MDCSTAT30001` + `MDCSTAT30502`

현재 service runtime 경로는 bulk 메서드를 사용한다. legacy 경로는 parity 검증, 수동 보정, 코드 롤백 시
참조용으로 남겨둔다.

## 3. Endpoint / 파서 매핑

| metric | 기존 endpoint | bulk endpoint | parser |
|---|---|---|---|
| `foreign_holding_shares` | `MDCSTAT03701` | 현행 유지 | `parse_foreign_holding_rows` |
| `institution_net_buy_volume` | `MDCSTAT02302` | `MDCSTAT02401` | `parse_investor_net_volume_bulk_rows` |
| `individual_net_buy_volume` | `MDCSTAT02302` | `MDCSTAT02401` | `parse_investor_net_volume_bulk_rows` |
| `foreign_net_buy_volume` | `MDCSTAT02302` | `MDCSTAT02401` | `parse_investor_net_volume_bulk_rows` |
| `short_selling_volume` | `MDCSTAT30001` | `MDCSTAT30101` | `parse_shorting_trading_bulk_rows` |
| `short_selling_value` | `MDCSTAT30001` | `MDCSTAT30101` | `parse_shorting_trading_bulk_rows` |
| `short_selling_balance_quantity` | `MDCSTAT30502` | `MDCSTAT30501` | `parse_shorting_balance_bulk_rows` |

Investor bulk 세부 규칙:

- `invstTpCd=8000` individual 응답을 base row set으로 사용한다.
- `invstTpCd=7050` institution row가 없으면 해당 ticker의 institution 값은 0으로 채운다.
- `invstTpCd=9000` foreign과 `invstTpCd=9001` other foreign을 더해 `foreign_net_buy_volume`으로 저장한다.
- institution/foreign/other foreign row 누락은 KRX가 0값 row를 생략하는 경우로 보고, base row set 안에서만 0으로 보정한다.
- investor 완전성 skip 판정은 active universe 100%가 아니라 기존 coverage parity를 고려해 90% threshold를 사용한다.

각 raw row의 `source_bld`는 실제 bulk endpoint bld로 저장한다.

## 4. 요청 수 / 런타임 모델

기준: 11거래일, 2시장, active universe 약 2768 ticker.

| 구분 | 변경 전 service logical | 변경 전 KRX HTTP POST | 변경 후 service logical | 변경 후 KRX HTTP POST |
|---|---:|---:|---:|---:|
| foreign | 22 | 22 | 22 | 22 |
| investor | 2768 | 2768 | 22 | 88 |
| shorting trading | 2768에 포함 | 2768 | 22 | 22 |
| shorting balance | 2768에 포함 | 2768 | 22 | 22 |
| **합계** | **5558** | **8326** | **88** | **154** |

`requests_attempted`는 service logical request 기준이다. investor bulk는 service request 1건 안에서 KRX
HTTP POST 4건을 수행하므로, HTTP POST 수와 service counter 수가 다르다.

8초 logical sleep 하한만 보면:

- 변경 전: `5558 × 8s ≈ 12.4h`
- 변경 후: `88 × 8s ≈ 11.7m`

실제 runtime은 KRX HTTP delay, long rest, 인증 cooldown, error backoff, KRX 응답 시간에 의해 늘어난다.
그래도 지배항이던 종목 단위 요청이 사라졌으므로 전체 Security flows는 24시간을 넘는 구조에서 벗어나고,
배포 후 run counter로 실제 phase별 시간을 확인한다.

## 5. 결정 사항 갱신

| # | 결정 지점 | 최종 상태 |
|---|---|---|
| D1 | investor bulk 채택 | **채택 및 구현 완료.** 단, active universe 100% 보장이 아니라 legacy coverage parity + zero-fill rule 기준이다. |
| D2 | 운영 분리 vs 구조 전환 | **운영 분리 미채택.** KRX flow 작업은 하나로 유지하고 내부 요청 단위만 bulk로 전환한다. |
| D3 | 구현 우선순위 | shorting bulk를 먼저 구현했고, 이후 investor bulk까지 구현 완료. |
| D4 | shorting balance lag allowance | 명시적 lag allowance는 아직 미구현. bulk 전환 후 최신 balance 결손이 종목 수만큼 증폭되지 않으므로 배포 후 관측 항목으로 둔다. |
| D5 | 종목별 provider 처리 | legacy 종목별 경로 유지. runtime은 bulk 경로 사용. |
| D6 | 계측 | phase별 service counter 구현 완료. HTTP elapsed/sleep/backoff counter는 후속 과제로 유지. |
| D7 | KRX 실행 방식 | foreign/investor/shorting을 별도 작업으로 쪼개지 않고 기존 sync 안에서 순차 실행한다. |

## 6. 구현 완료 내역

### Phase 1 — shorting 전종목 bulk 전환

완료:

1. `FlowProvider`에 bulk 메서드 추가.
2. `KrxDirectFlowProvider`에 `fetch_shorting_trading_bulk`, `fetch_shorting_balance_bulk` 구현.
3. `parse_shorting_trading_bulk_rows`, `parse_shorting_balance_bulk_rows` 구현.
4. `sync_krx_flows`의 shorting runtime을 종목 단위에서 `날짜 × 시장 × endpoint` 단위로 전환.
5. 기존 complete ticker 판정을 metric별 daily-market coverage 판정으로 전환.
6. unit fixture와 parser/service 테스트 추가.

라이브 검증 결과:

- 2026-06-17 KOSPI trading: 946 tickers, 1892 records
- 2026-06-17 KOSPI balance: 946 tickers
- 2026-06-17 KOSDAQ trading: 1822 tickers, 3644 records
- 2026-06-17 KOSDAQ balance: 1822 tickers
- 표본 종목 parity가 sj2 DB 기존 값과 일치함

관측된 리스크:

- `MDCSTAT30501`에서 non-JSON HTML 응답이 한 번 관측됐고, endpoint별 재시도에서는 성공했다.
- 따라서 circuit breaker/fast-fail은 여전히 후속 안정화 과제로 남긴다.

### Phase 2 — investor bulk 전환

완료:

1. `FlowProvider`에 `fetch_investor_net_volume_bulk(trade_date, market, tickers)` 추가.
2. `KrxDirectFlowProvider`에서 `MDCSTAT02401`을 investor code 4종으로 호출.
3. `parse_investor_net_volume_bulk_rows` 구현.
4. `sync_krx_flows` investor runtime을 종목 단위에서 `날짜 × 시장` 단위로 전환.
5. investor skip completeness threshold를 90%로 적용.
6. unit fixture와 parser/service 테스트 추가.

라이브 검증 결과:

- `individual(8000)` row count가 완료일 기준 기존 per-ticker DB coverage와 일치했다.
- `institution(7050)`은 0값 row 일부를 생략하므로 base row set 안에서 missing=0 처리가 필요했다.
- `foreign(9000)` 단독은 기존 `TRDVAL4`와 불일치했고, `foreign(9000) + other_foreign(9001)`이 일치했다.
- 2026-06-19 KOSPI 표본 `000660`, `005930`, `035720` bulk 결과가 sj2 DB 값과 일치했다.

### Phase 2.5 — phase별 counter

완료:

- `KrxFlowSyncResult.phase_counts` 추가.
- `ingestion_runs.counts`에 phase별 flat counter 저장.
- CLI 완료 출력에 phase별 counter 표시.

저장되는 key:

- `foreign_holding_requests_attempted`
- `foreign_holding_requests_skipped`
- `foreign_holding_rows_upserted`
- `foreign_holding_no_data_requests`
- `foreign_holding_error_count`
- `investor_bulk_requests_attempted`
- `investor_bulk_requests_skipped`
- `investor_bulk_rows_upserted`
- `investor_bulk_no_data_requests`
- `investor_bulk_error_count`
- `shorting_bulk_requests_attempted`
- `shorting_bulk_requests_skipped`
- `shorting_bulk_rows_upserted`
- `shorting_bulk_no_data_requests`
- `shorting_bulk_error_count`

11거래일 × 2시장 기준, 정상 catch-up 실행에서 기대하는 service counter는 대략:

- `foreign_holding_requests_attempted = 22`
- `investor_bulk_requests_attempted = 22`
- `shorting_bulk_requests_attempted = 44`

이미 수집된 구간은 skip counter로 이동한다.

## 7. 검증 완료 항목

로컬 검증:

- `uv run black ...` 통과
- `uv run ruff check ...` 통과
- `uv run pytest tests/unit/test_flows_krx.py tests/unit/test_sync_krx_flows.py` 통과
- `uv run pytest tests/unit` 통과: 434 passed

라이브 검증:

- shorting bulk row count/parity 확인 완료
- investor bulk component rule 확인 완료
- investor 표본 smoke 확인 완료

## 8. 배포 전 체크리스트

1. KRX Cronicle 작업은 분리하지 않고 기존 `flows sync` 작업을 사용한다.
2. 배포 후 첫 실행에서 `ingestion_runs.counts`의 phase counter를 확인한다.
3. 11거래일 전체 catch-up이면 service logical request가 `foreign 22 + investor 22 + shorting 44` 근처인지 확인한다.
4. investor는 내부 HTTP POST가 service counter보다 4배 많다는 점을 로그 해석 시 구분한다.
5. partial error가 발생하면 error key가 `investor:YYYY-MM-DD:MARKET`, `shorting_trading:YYYY-MM-DD:MARKET`, `shorting_balance:YYYY-MM-DD:MARKET` 중 어디에 몰리는지 확인한다.
6. non-JSON HTML 오류가 반복되면 Phase 3의 circuit breaker/fast-fail을 우선 적용한다.

## 9. 후속 과제

| 과제 | 우선순위 | 설명 |
|---|---|---|
| circuit breaker / fast-fail | 높음 | 같은 bld/group에서 non-JSON이 반복되면 긴 retry/backoff를 태우지 않고 partial로 빠르게 종료. |
| HTTP elapsed/sleep/backoff 계측 | 중간 | `KrxMdcClient`/`HumanThrottle` 수준에서 실제 HTTP call, sleep, backoff 시간을 누적. |
| shorting balance lag-aware skip | 중간 | bulk 후에도 최신 잔고 제공 지연이 반복되면 최신 1~2거래일 balance completeness 예외 적용. |
| `FLOW_LOOKBACK_DAYS` 축소 | 중간 | 첫 bulk 배포 결과 확인 후 14일에서 2~3일로 운영 설정 축소 검토. |
| bulk runtime feature flag | 낮음 | 코드 revert 없이 legacy 경로로 전환해야 하는 운영 요구가 생기면 추가. 현재는 분기면 증가를 피하기 위해 미구현. |

## 10. 리스크와 보완

| 리스크 | 영향 | 보완 |
|---|---|---|
| KRX bulk endpoint non-JSON/HTML 응답 | partial error 또는 지연 | HumanThrottle 유지, 요청 수 감소, 배포 후 circuit breaker 우선 적용 |
| investor endpoint가 active universe 전체를 반환하지 않음 | 100% universe 기준 결손처럼 보일 수 있음 | legacy coverage parity 기준, individual base row set, 90% completeness threshold |
| institution/foreign 0값 row 누락 | 잘못된 결손 처리 가능 | missing component를 0으로 채우고 raw payload에 component 상태 저장 |
| 외국인 순매수 구성 오해 | 값 불일치 | `9000 + 9001` 합산 규칙으로 저장 |
| 공매도 잔고 최신일 지연 | 최신 balance 일부 no-data/partial | bulk로 증폭을 줄이고, 반복되면 lag-aware skip 적용 |
| legacy fallback flag 부재 | 즉시 runtime switch 어려움 | legacy 메서드는 유지. 문제 시 코드 revert 또는 feature flag 추가 |

## 11. 기대 결과

| 항목 | 변경 전 | 변경 후 |
|---|---:|---:|
| service logical request | ~5558 | ~88 |
| KRX HTTP POST | ~8326 | ~154 |
| logical sleep 하한 | ~12.4h | ~11.7m |
| shorting | multi-hour | 분 단위 |
| investor | 6~7h 수준 | 분 단위 |
| 26h run 발생 가능성 | 높음 | 크게 낮아짐 |

최종 해법은 출처 변경이 아니라 **같은 KRX의 전종목 endpoint로 요청 단위를 바꾸는 것**이다. 이번
수정은 그 구조 전환을 코드에 반영했고, 배포 후에는 phase별 counter로 실제 절감 효과를 확인한다.
