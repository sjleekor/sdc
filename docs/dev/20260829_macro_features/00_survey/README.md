# 00_survey — 매크로 피쳐 발굴 사전 조사 (2026-08-29)

`docs/dev/20260731_raw_features`의 35 family 검증이 끝난 뒤, 거시 경제 지표 기반 피쳐를 새로 발굴하기 전에
한 조사 기록이다. **설계·사전등록 문서가 아니다.** 여기서 나온 우선순위를 다음 디렉터리에서 계약으로 못 박는다.

| 문서 | 답하는 질문 | 한 줄 결론 |
|---|---|---|
| [00_existing_features_vs_macro.md](00_existing_features_vs_macro.md) | 기존 35개 피쳐에 매크로 정보가 이미 들어 있나 | **직접 겹침 0.** 계보·baseline 모델 모두 매크로 없음. 검증 틀(날짜 내 rank)이 매크로를 소거한다. 그러나 피쳐의 날짜별 수준·분산은 국면과 같이 움직인다(외국인 순매수–시장폭 ρ 0.63, 수급 IQR–거래대금 ρ −0.72) |
| [01_macro_predictor_literature_survey.md](01_macro_predictor_literature_survey.md) | 학계·업계는 어떤 거시 지표를 중요하게 보나 | 시장 방향 예측은 대부분 실패(GWZ 2024: 70~80% 소멸; 한국 OOS 생존은 B/M·인플레이션·기간 스프레드). **국면이 횡단면 이상현상을 바꾼다는 근거는 단단**(CGH 2004, DM 2016, SYY 2012; 한국 Kim-Park-Ok 2019, Kang-Kwon-Park 2014, 박종원 2020) |
| [02_candidate_indicators_and_sources.md](02_candidate_indicators_and_sources.md) | 무엇을 어디서 어떻게 가져와 어떤 형태로 쓰나 | 우선순위 1 축(VIX·시장 상태·유동성 국면·기간 스프레드)은 **원천이 이미 카탈로그에 있다.** 비용 0 추가는 회사채 스프레드·기준금리·ESI(ECOS). 국내 원천은 vintage가 없어 개정 지표는 보류 |
| `appendix/` | `00` §4의 상관 표 원본 CSV | 월말 134개 표본 / 일별 2,742개 |

## 다음 단계 (요약)

0. **일별 IC 시계열 저장** — 조건부 IC(③)의 전제. 새 수집 없음.
1. **exposure 베타(①)와 interaction(②) 사전등록** — `beta_usdkrw`(양방향)·`beta_wti`·`beta_kr10y_chg`·`beta_sp500_lag1`·`beta_vix_chg`;
   `flow_foreign_* × (usdkrw, ΔVIX)`, `px_idio_vol`/`px_maxret × VIX 국면`, 수급 3종·`px_amihud × 거래대금 국면`,
   `px_mom_12_1 × 시장 상태`, `fin_log_mcap × (kosdaq−kospi)`. 새 수집 없음.
2. ECOS 시리즈 추가(회사채 AA-/BBB-, CD 91일, 기준금리, ESI) + FRED 추가(BAA10Y, HY OAS, 광의 달러, breakeven, EPU).
3. 새 어댑터 — 관세청 10일 수출 잠정치(`DATAGO_KEY`), SOX.

세 갈래(①②③)의 정의와 왜 매크로 level을 피쳐로 넣을 수 없는지는 `00` §3, 변환·PIT 규칙은 `02` §4에 있다.

단계 0·1의 사전등록 설계는 [`../01_design/`](../01_design/00_overview.md)에 있다.
