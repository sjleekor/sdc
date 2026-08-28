# N7 KIS 횡단면 대조 — B/M·E/P 매핑과 C4/C5 처분

- 실행일: 2026-08-27
- KIS 기준일: 2026-08-27 현재가
- canonical 입력: `snapshot_date=2026-08-23/source=sj2_remote`, 최신 거래일 2026-08-21
- 실행 코드: `research/analysis/n7_kis_cross_section.py`
- 결과: `research/output/n7_kis_cross_section/as_of_date=2026-08-27/`

## 결론

N7은 **B/M 매핑 대조를 통과**했다. 가격 날짜를 맞춘 시장 내 rank correlation은 0.9274다.
E/P는 0.6547로 B/M보다 낮지만, KIS PER 결측의 90.3%가 canonical E/P 결측 또는 손실과
겹쳤다. 적자기업 처리 차이가 큰 이유다.

KIS로 확인할 수 있는 범위는 B/M과 E/P뿐이다. CFO/P·S/P와 I7의 revenue 매핑은 확인하지
못한다. KIS 밸류를 새 feature로 올리지 않고, 원래 목적대로 진단 원천에서 끝낸다.

C4와 C5는 **폐기**한다.

- C4: KIS `inquire_price`의 실제 80필드에 `DIV`가 없다.
- C5: 한 시점 현재값으로 미래수익률 IC를 계산할 수 없다. 이를 위해 historical 백필을
  되살리지 않는다.

## 실행량과 coverage

| 항목 | 값 |
|---|---:|
| 활성 KOSPI/KOSDAQ 대상 | 2,764 |
| 정상 응답 | 2,764 |
| PBR 유효 | 2,461 |
| PER 유효 | 1,517 |
| B/M pair | 2,318 |
| E/P pair | 1,145 |
| 논리 `inquire_price` 호출 | 2,764 |
| rate-limit 재시도 | 10 |
| 실제 `inquire_price` HTTP | 2,774 |
| token 발급 HTTP | 1 |
| 실제 외부 HTTP 합계 | **2,775** |

실행은 5req/s로 했다. `EGW00201`이 10회 나왔지만 모두 첫 재시도에서 복구됐다. 정상 응답
2,764개는 append-only checkpoint에 모두 남았다.

## C1 — 매핑 rank correlation

| 대조 | rank correlation |
|---|---:|
| `1/PBR` ↔ canonical B/M, 8월 21일 값 그대로 | 0.9243 |
| `1/PBR` ↔ canonical B/M, KIS 현재가로 가격 정렬 | **0.9274** |
| KIS BPS ↔ canonical BPS proxy | 0.8561 |
| `1/PER` ↔ canonical E/P, 8월 21일 값 그대로 | 0.6520 |
| `1/PER` ↔ canonical E/P, KIS 현재가로 가격 정렬 | **0.6547** |
| KIS EPS ↔ canonical EPS proxy | 0.8447 |

가격 정렬 값은 canonical 최신 종가 대비 KIS 현재가 비율만 반영한다. BPS/EPS proxy는
canonical ratio에 최신 종가를 곱했다. KIS와 canonical의 회계 기준·발행주식 수 정의가 다를
수 있으므로 level 일치를 주장하지 않는다.

## C2 — B/M 불일치 종목

시장 내 B/M rank gap 중앙값은 0.0244, p90은 0.1329다. gap이 0.25 이상인 종목은
104개다.

| 시장 | pair | rank correlation | gap 중앙값 | gap p90 |
|---|---:|---:|---:|---:|
| KOSDAQ | 1,557 | 0.9060 | 0.0225 | 0.1342 |
| KOSPI | 761 | 0.9406 | 0.0289 | 0.1329 |

gap 0.25 이상 집합의 `fin_age_days` 평균은 82.7일이고 `fin_log_mcap` 중앙값은 25.36이다.
전체 불일치 목록은 `n7_bm_rank_disagreement.parquet`에 남겼다. 현재 `induty_code`를 과거에
소급한 업종 진단은 N2와 같은 look-ahead 한계가 있어 채택 근거로 쓰지 않는다.

## C3 — PER 결측과 I1 취약 집합

| 집합 | 종목 |
|---|---:|
| KIS PER 결측 또는 0 | 1,247 |
| canonical E/P 결측 또는 손실 | 1,600 |
| 두 집합의 겹침 | **1,126 (KIS 집합의 90.3%)** |
| I1 취약 집합: 유효 component 2개 미만 | 380 |
| KIS PER 결측과 I1 취약 집합 겹침 | 301 (KIS 집합의 24.1%) |

KIS PER 결측은 대부분 적자 또는 canonical E/P 결측을 가리킨다. 다만 I1 취약 집합 전체를
설명하지는 않는다. I1은 네 component의 결측 처리 SQL 버그였고 이미 고쳤기 때문이다.

## 최종 처분

- `daily_market_fundamental` 테이블과 N7-2~N7-7 historical 백필은 취소 상태로 닫는다.
- KIS B/M·E/P는 독립 진단 결과로만 보존한다.
- CFO/P·S/P와 I7 revenue 매핑은 N7 완료 범위에서 제외한다.
- C4·C5는 다른 시계열로 옮기지 않고 폐기한다.
- 다음 단계는 N7을 제외한 신규 feature config 사전등록이다.
