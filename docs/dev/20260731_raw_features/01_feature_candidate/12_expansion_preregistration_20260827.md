# 신규 feature Horizon Scan 사전등록 — 2026-08-27

- config: `research/analysis/horizon_scan_expansion_20260827.yaml`
- config hash: `889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea`
- base config: `horizon_scan_config.yaml`, `ab0de634…`
- 등록 시점: 신규 feature의 label·IC·p-value 계산 전

## 결론

기존 25 family와 75개 Phase A 가설은 바꾸지 않는다. 확장 config는 Phase B에 10 family와
40개 candidate cell을 더한다. Phase B는 기존 38개에서 78개로 늘고, 결합 BH 모집단은
75 + 78 = **153개**다.

N7 KIS 결과는 진단으로 끝냈으므로 넣지 않는다. N2 업종 중립 variant도 현재 업종을 과거에
소급하는 look-ahead 때문에 넣지 않는다.

N8 실업률·고용률은 같은 날짜의 모든 종목에 같은 값이다. 횡단면 rank IC에 넣으면 상수라서
Phase B family로 등록하지 않고 Phase C 조건부 regime 후보로 분리한다.

## 신규 Phase B registry

| family | primary feature | expected sign | primary horizon | candidate cell | 비고 |
|---|---|:---:|---|---:|---|
| `mcap_krx_log` | `mcap_krx_log` | − | 60, 120 | 4 | KRX 상장주식 수 기반 size |
| `ev_filing_activity` | `ev_filing_burst_60d` | 양방향 | 20, 60 | 4 | count·120일은 secondary |
| `ev_amendment_ratio` | `ev_amendment_ratio_1y` | − | 60, 120 | 4 | 보고 품질 |
| `own_insider_filing_activity` | `own_insider_filing_burst_60d` | 양방향 | 20, 60 | 4 | 보유 증가·감소가 섞여 있음 |
| `own_major_filing_activity` | `own_major_filing_60d` | 양방향 | 20, 60 | 4 | 5% rule 공시 빈도 |
| `own_amendment_ratio` | `own_amendment_ratio_1y` | − | 60, 120 | 4 | 지분공시 정정 비율 |
| `hc_employee_growth` | `hc_employee_growth_yoy` | 양방향 | 60, 120 | 4 | final-vintage, grade ≤ B |
| `hc_productivity` | `hc_revenue_per_employee` | 양방향 | 60, 120 | 4 | final-vintage, grade ≤ B |
| `own_major_stake_level` | `own_major_stake` | 양방향 | 60, 120 | 4 | final-vintage, grade ≤ B |
| `own_major_stake_change` | `own_major_stake_chg` | 양방향 | 60, 120 | 4 | final-vintage, grade ≤ B |

각 family는 cumulative cell과 primary horizon 끝에 맞는 bucket cell을 함께 등록한다.
`[60, 120]`은 cumulative 2개와 bucket 2개, `[20, 60]`도 같은 방식으로 4개다.

N6 원문은 3개 경제 family·4개 feature로 묶었지만, 현재 registry는 한 family에 primary를
하나만 허용한다. `own_major_stake` level과 change를 별도 scan family로 나눠 둘 다 BH 모집단에
넣었다. `fdr_family=ownership`은 같게 둔다.

## N8 Phase C 사전등록

| feature | source | transform | availability | 역할 |
|---|---|---|---|---|
| `macro_unemployment_rate_level` | `macro_unemployment_rate` | level | 월말 + 20일 | 조건부 regime |
| `macro_employment_rate_level` | `macro_employment_rate` | level | 월말 + 20일 | 조건부 regime |

Phase C는 A/B/AB에서 부호 반전이나 경제적으로 설명할 수 있는 조건부 패턴이 나올 때만 연다.
regime cut을 결과에 맞춰 고르지 않는다. 실제 조건화 계약은 Phase C를 열기로 결정한 뒤 이
두 level의 사전등록된 변환만 사용해 고정한다.

## 보존 규칙

- 기존 `horizon_scan_config.yaml`과 `config_hash=ab0de634…` 산출물은 그대로 둔다.
- 확장 config는 `extends` overlay로 읽되 hash는 merge된 전체 계약에서 계산한다.
- N6 네 family는 `source_warning=final_vintage`, `grade_cap=B`를 카드에 연결한다.
- 양방향 family는 새 결과의 관측 부호를 따라 period·robustness 방향을 정한다. 결과가 나온 뒤
  기대 부호를 바꾸지 않는다.
- holdout은 feature·horizon·variant·Phase C 선택을 모두 끝낸 뒤 한 번만 연다.

## 실행 결과 — 2026-08-28

이 절은 사전등록 계약을 바꾸지 않고 실행 lineage와 판정만 덧붙인다.

| phase | run_id | 결과 |
|---|---|---|
| A | `20260827T221729-4e0ae8b0` | 75/75 valid, BH 57, primary discovery 32 |
| B | `20260828T123313-4e0ae8b0` | 78/78 ready, BH 59, primary discovery 55 |
| AB | `20260828T165038-4e0ae8b0` | 153가설, discovery 87, `screen_pass` 40, B-cell A23·B17·C35·D3 |

AB content hash는 `0258eb5d172a8885c80e9c9435bc8f07f1a5a0415660619e9eba412feba42938`다.
결합 permutation은 `p=0.0099`였고, 확장 뒤 기존 Phase A discovery 변화는 0개다.

Phase C는 열지 않는다. 부호 반전은 `own_insider_filing_activity`와 `px_resid_mom_12_1`에만
있었고 효과가 작으며 discovery가 없었다. 경제적으로 설명할 수 있는 조건부 패턴이라는
사전등록 조건을 못 맞췄다. N8 regime 후보는 그대로 dormant 상태로 둔다.

T2 validation은 AB `screen_pass` family의 primary feature 14개를 함께 추가했다. baseline 대비
Rank IC 변화는 h5 `+0.0031`, h20 `+0.0011`, h60 `+0.0003`이고, 비용 반영 spread 변화는
각각 `+0.0017`, `+0.0030`, `+0.0080`이다. 세 horizon이 모두 좋아졌지만 최종 채택은 새 h60
holdout을 한 번 평가할 2026년 10~11월까지 미룬다.
