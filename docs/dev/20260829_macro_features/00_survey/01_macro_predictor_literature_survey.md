# 01. 주가 예측에 쓰이는 거시 지표 — 학계·업계 조사

- 작성일: 2026-08-29
- 목적: 새 매크로 피쳐를 사전등록하기 전에, **학계와 업계가 어떤 거시·시장 전체 변수를 중요하게 보는지**,
  그 근거가 표본 내인지 표본 외인지, 한국에서 재현됐는지를 정리한다.
- 범위: (A) 시장 전체 방향(equity premium) 예측, (B) 횡단면 종목 선택의 조건화·exposure. 우리
  라벨은 시장 내 순위라 **(B)가 본 작업의 목적**이고, (A)는 국면 변수를 고르는 재료다(`00` §3).
- 근거 수준 표기: 원문(PDF·본문)까지 확인한 것은 그대로 적었고, 초록·2차 자료에 의존한 것은 그렇게
  적었다. 확인하지 못한 것은 "찾지 못함"으로 남겼다.
- 조사 방법: 웹 검색으로 원문·초록·보고서를 직접 확인했다. 조사 시점은 2026-08-29다.

---

## 1. 한 장 요약

**세 문장이면 된다.**

1. **시장 전체 방향을 거시 변수로 예측하는 일은 학계에서 거의 실패로 정리됐다.** Welch & Goyal(2008)이
   14개 표준 변수의 표본 외 예측력이 없다고 보였고, Goyal, Welch & Zafirov(2024)가 그 뒤 나온 29개
   변수를 다시 검정해 "10%는 작동, 10~20%는 애매, 70~80%는 소멸"이라고 결론냈다. 한국도 같다 —
   전성주(2020)에서 12개 변수 중 표본 외에서 살아남은 것은 B/M과 인플레이션 둘뿐이다.
2. **반면 거시·시장 상태가 횡단면 이상현상의 크기와 부호를 바꾼다는 근거는 단단하다.** 모멘텀은
   상승장 뒤에만 작동하고(Cooper, Gutierrez & Hameed 2004) 고변동성 뒤 무너진다(Daniel & Moskowitz
   2016). 심리가 높을 때 이상현상의 short leg가 커진다(Stambaugh, Yu & Yuan 2012). 한국에서도
   VIX 하락 뒤 달에만 저변동성 효과가 나타나고(Kim, Park & Ok 2019), 외국인 가격영향은 상승장에서만
   유의하다(Kang, Kwon & Park 2014).
3. **한국 시장에서 "예측변수"로 반복해 살아남은 것은 국고채 기간 스프레드, 시장 밸류에이션(B/M),
   전일 미국 주가·VIX 셋이다.** 환율·수출·경기선행지수·BSI·소비자심리는 동행하거나 주가가 오히려
   선행한다. 이들은 **예측 피쳐가 아니라 국면 변수**로 써야 한다. 업계(삼성증권·IBK·MSCI·AQR)가
   실제로 그렇게 쓰고 있다.

우리 프로젝트에 주는 함의는 `00_existing_features_vs_macro.md` §5와 같다. 매크로 level을 피쳐로
넣는 길은 닫혀 있고(날짜 내 상수), 열려 있는 길은 종목별 exposure(①), 국면 × 종목 특성
interaction(②), 국면별 조건부 IC(③)다. 이 문서 §6의 표가 어느 지표를 어느 길에 쓸지 정리한다.

---

## 2. 시장 전체 방향 예측 — 글로벌 학계

### 2.1 표준 변수 목록과 그 운명

**Welch & Goyal (2008, RFS).** 월별 14개 — d/p, d/y, e/p, d/e, svar(실현분산), b/m, ntis(순주식발행),
tbl(단기금리), lty(장기금리), ltr(장기채 수익률), tms(기간 스프레드), dfy(신용 스프레드 BAA−AAA),
dfr(신용 수익률 차), infl — 와 분기 cay·i/k, 연간 eqis. 결론은 "지난 30년간 표본 내·외 모두 예측력이
나쁘고 불안정하며 실시간 투자자에게 도움이 안 됐다"다.
[RFS](https://academic.oup.com/rfs/article-abstract/21/4/1455/1565737) ·
[저자 PDF](https://www.ivo-welch.info/research/journalcopy/2008-rfs.pdf)

**Goyal, Welch & Zafirov (2024, RFS 37(11)).** 2008년 이후 26개 논문의 29개 신변수를 2021년 말까지
재검정했다(저자 발표 슬라이드 원문 확인). 대상에는 분산위험프리미엄(vrp), 내재분산, tail risk,
output gap, 정렬 심리지수, 유가 변화, 공매도 잔고, 애널리스트 의견 불일치, 14개 기술지표, 다우
52주 고점 근접도, 평균 상관 등이 들어 있다. **신변수의 1/3 이상은 표본 내 유의성도 사라졌고, 남은
것의 절반은 표본 외가 나쁘다.** 표본 외까지 살아남은 것은 14개 기술지표, 공매도 잔고, 총 accruals,
4분기 PCE 성장률, 구변수 중 tbl·i/k·eqis 정도다. VRP·tail risk·output gap·sentiment는 살아남지
못했다. "연간 변수가 월간보다 잘 예측한다"는 관찰도 있다.
[RFS](https://academic.oup.com/rfs/article/37/11/3490/7749383) ·
[슬라이드](https://www.ivo-welch.org/research/presentations/99-stanford-prediction.pdf)

**Rapach, Strauss & Zhou (2010, RFS).** 개별 회귀는 불안정하지만 예측 결합(forecast combination)은
표본 외 이득이 안정적이고, **예측력은 경기 침체기에 집중**된다.
[RFS](https://academic.oup.com/rfs/article-abstract/23/2/821/1604687)

**Dong, Li, Rapach & Zhou (2022, JF).** 100개 이상현상의 long-short 수익률을 shrinkage로 결합하면
다음 달 시장 초과수익을 표본 외에서 유의하게 예측한다. 부호는 **음(−)** — 이상현상 수익이 높으면
다음 달 시장이 약하다(과대평가 교정의 지속). 우리처럼 횡단면 신호를 이미 갖고 있는 쪽에서는
"횡단면 → 시장"이라는 역방향 정보다.
[JF](https://onlinelibrary.wiley.com/doi/abs/10.1111/jofi.13099)

### 2.2 거시 요인·불확실성·경기 지수

| 연구 | 변수 | 결과 |
|---|---|---|
| Chen, Roll & Ross (1986) | 산업생산 성장, 기대·비기대 인플레이션, 기간 스프레드, 신용 스프레드의 **surprise** | 횡단면에서 가격에 반영되는 위험. 거시 요인 모형의 원형. [JB](https://ideas.repec.org/a/ucp/jnlbus/v59y1986i3p383-403.html) |
| Ludvigson & Ng (2009) | 130여 거시 시계열의 "실물"·"인플레이션" 요인 | 채권 초과수익 예측, forward rate 대비 R² 15~25%p 개선. [RFS](https://academic.oup.com/rfs/article-abstract/22/12/5027/1577464) |
| Jurado, Ludvigson & Ng (2015, AER) | 거시 불확실성(예측 불가 성분의 공통요인) | VIX보다 드물지만 크고 오래가는 에피소드. [AER](https://www.aeaweb.org/articles?id=10.1257%2Faer.20131193) |
| Baker, Bloom & Davis (2016, QJE) | EPU(신문 기사 빈도) | 주가 변동성↑, 투자·고용↓. Brogaard & Detzel(2015)은 EPU 베타 최고 포트폴리오가 연 5.53% 저성과. [QJE](https://academic.oup.com/qje/article-abstract/131/4/1593/2468873) |
| Bollerslev, Tauchen & Zhou (2009) / Bekaert & Hoerova (2014) | VRP = 내재분산 − 실현분산 | 분기 horizon 예측력. 단 GWZ(2024)에서 표본 외 실패·데이터 개정 문제. [BTZ](https://academic.oup.com/rfs/article-abstract/22/11/4463/1565787) |
| ADS · CFNAI · NFCI | 실시간 경기지수·금융조건 지수 | 필라델피아·시카고 연은 공개. Campbell & Diebold는 기대 경기가 기대 초과수익에 **반경기적** 영향. [ADS](https://www.philadelphiafed.org/surveys-and-data/real-time-data-research/ads) · [CFNAI](https://www.chicagofed.org/research/data/index-live) · [NFCI](https://www.chicagofed.org/research/data/nfci/about) |
| Bybee, Kelly, Manela & Xiu (2024, JF) | WSJ 기사 토픽 비중 | 경기·시장 수익률 예측, 텍스트 증강 VAR. [JF](https://onlinelibrary.wiley.com/doi/full/10.1111/jofi.13377) |

### 2.3 이 절에서 가져갈 것

- 시장 방향 예측을 목표로 삼지 않는다. 우리 라벨이 그걸 이미 빼 버렸고, 학계 결론도 부정적이다.
- 그러나 **어떤 변수가 "국면"을 나누는 데 쓰이는지**는 이 목록에서 나온다 — 기간 스프레드, 신용
  스프레드, 실현분산/VIX, 인플레이션, 단기금리, 그리고 경기 지수(ADS·CFNAI 류).
- 예측력이 **침체기에 집중**된다는 RSZ(2010)의 관찰은 조건부 IC 설계의 근거다. 국면을 나누지
  않으면 평균에 묻힌다.

---

## 3. 횡단면 조건화 — 글로벌 학계

이 절이 우리 작업의 직접 근거다.

### 3.1 매크로 × 종목 특성 interaction — 기계학습 자산가격 결정

**Gu, Kelly & Xiu (2020, RFS).** 원문 확인. 8개 거시 변수 **dp, ep, bm, ntis, tbl, tms, dfy, svar**
(Welch-Goyal 정의)를 94개 종목 특성과 곱해 `z_{i,t} = x_t ⊗ c_{i,t}`(x_t는 상수 포함 9차원)로 만든다.
94 × 9 + 74(SIC 더미) = 920개 공변량. Table 4 거시 변수 중요도: **모든 모형이 집계 b/m을 핵심**으로
꼽고 **svar는 거의 무용**하다. 선형·GLM은 dfy·tbl을, 트리·신경망은 tms·ntis를 중시한다.
[RFS](https://academic.oup.com/rfs/article/33/5/2223/5758276) ·
[NBER PDF](https://www.nber.org/system/files/working_papers/w25398/w25398.pdf)

> 우리 틀에서 이것은 §2의 ②(interaction)다. GKX는 거시 level을 그대로 곱하지만, 우리 라벨은 날짜
> 내 순위라 `x_t × c_{i,t}`에서 x_t가 날짜 상수여도 c_{i,t}의 가중치가 날짜마다 바뀌는 효과는
> 남는다 — 즉 **국면에 따라 특성의 유효 계수가 달라지는 것**을 잡는다.

**Chen, Pelger & Zhu (2024, MS).** 원문 확인. 178개 거시 시계열(FRED-MD 124 + 46개 특성의 횡단면
중앙값 + WG 8개)을 LSTM으로 **4개 hidden state**로 압축해 SDF 가중치의 조건변수로 쓴다. 두 경고가
중요하다 — "마지막 변화율만 넣으면 동태 정보가 사라지고, **원시 거시변수를 그대로 넣으면 빼는 것보다
성과가 나빠진다**."
[MS](https://pubsonline.informs.org/doi/10.1287/mnsc.2023.4695) · [arXiv](https://arxiv.org/abs/1904.00745)

**Kelly & Xiu (2023) 서베이.** 원문 확인. "종목 특성은 수백 개, 시장 거시 예측변수는 수십 개"이며
수익률 예측은 signal-to-noise가 "notoriously low". WG 2008의 비판과 RSZ 2010의 결합 반론을 함께
소개한다. [NBER](https://www.nber.org/papers/w31502)

### 3.2 국면이 이상현상을 바꾼다 — 고전 결과

| 연구 | 국면 변수 | 결과 | 우리 피쳐와의 접점 |
|---|---|---|---|
| Cooper, Gutierrez & Hameed (2004, JF) — 원문 확인 | 직전 36개월 시장수익률 부호(UP/DOWN) | 모멘텀 월수익 UP 뒤 0.93%, DOWN 뒤 −0.37%(비유의). 12·24개월 lookback에도 강건 | `px_mom_12_1`(D, 부호 반대). 국면을 나누면 다른 그림일 수 있다 |
| Daniel & Moskowitz (2016, JFE) | bear 지표(직전 24개월 시장 음수) × 시장 분산 | 모멘텀 크래시는 하락 뒤 고변동성 상태에서 시장 반등과 함께 발생. 동적 전략이 Sharpe 약 2배 | `px_mom_12_1`, `px_near_52w_high` |
| Wang & Xu (2015, JEF) | 시장 변동성 | 고변동성 뒤 모멘텀 저수익, 특히 하락장 | 같음 |
| Stambaugh, Yu & Yuan (2012, JFE) | Baker-Wurgler 심리지수 | 심리 높을 때 11개 이상현상 long-short 커지고, **효과는 전부 short leg** | 밸류·발생액·순발행 등 T2 피쳐. 한국에 공개 심리지수가 없다는 것이 제약 |
| Bali, Brown & Tang (2017, JFE) | JLN 불확실성 **베타** | 불확실성 베타 최저 십분위가 최고 대비 연 6% 초과수익 | ① exposure의 원형 |
| Rapach, Strauss & Zhou (2013, JF) | 미국 lagged 수익률 | 1980~2010 다른 선진국 수익률을 양(+)으로 예측. 한국은 표본 밖 | `cf_global_sp500_ret_1d`가 이미 카탈로그에 있다 |

### 3.3 이 절에서 가져갈 것

- interaction의 국면 변수는 **적고 해석 가능한 것**으로 — GKX Table 4 기준으로 집계 b/m,
  tms(기간 스프레드), dfy(신용), tbl(단기금리), 그리고 CGH·DM의 **시장 상태(누적수익 부호·분산)**.
- **원시 level을 그대로 넣지 않는다**(CPZ 2024). 변환(z-score·부호 flag·변화율)과 압축이 필요하다.
- exposure 베타(Bali-Brown-Tang 2017)는 횡단면 피쳐로 바로 검정 가능하다.

---

## 4. 업계 관행

| 기관 | 틀 | 국면 변수 | 출처 |
|---|---|---|---|
| MSCI | 팩터·섹터 국면 연구 | **미국 10년물 수준·1개월 변화 5분위 + OECD CLI**. "금리 변화가 수준보다 영향이 컸다". Barra 거시 팩터 모형의 공개 문서는 찾지 못함 | [MSCI](https://www.msci.com/research-and-insights/blog-post/factor-and-sector-behavior-across-macro-regimes) · [Macro-Finance](https://www.msci.com/research-and-insights/paper/the-msci-macro-finance-model) |
| Bridgewater | All Weather | 성장↑↓ × 인플레이션↑↓ 4분면 | [Bridgewater](https://www.bridgewater.com/research-and-insights/the-all-weather-story) |
| AQR (Brooks 2017) | Macro Momentum | 성장·인플레이션(예측치 12개월 변화), 무역(수출가중 환율 1년 변화), 통화정책(2년물 1년 변화), 리스크 심리(주식 1년 초과수익) | [AQR PDF](https://www.aqr.com/-/media/AQR/Documents/Insights/White-Papers/A-Half-Century-of-Macro-Momentum.pdf) |
| Fidelity | 경기순환 섹터 로테이션 | early/mid/late/recession | [Fidelity](https://www.fidelity.com/viewpoints/investing-ideas/sector-investing-business-cycle) |
| BlackRock | 팩터 로테이션 | regime · fundamentals · sentiment 3신호 | [BlackRock](https://www.blackrock.com/us/financial-professionals/insights/factor-rotation) |
| 캔자스시티 연은 | RORO 지수 | 신용·주식 변동성·펀딩·통화/금 4범주 | [KC Fed](https://www.kansascityfed.org/data-and-trends/risk-on-risk-off-index/) |
| J.P. Morgan | JPMaQS | **point-in-time** 거시 quantamental 지표 — PIT를 상품화한 예 | [JPM](https://www.jpmorgan.com/markets/jpmaqs) |
| **삼성증권 (2022.5)** | 4국면 팩터 로테이션 | **수출 4년 z-score**(경기 축, 발표가 빨라서) × **CPI z-score**(물가 축). Recovery 이익모멘텀 20.0%, Expansion 밸류 12.8%, Slowdown 역사이즈 53.2%·밸류 38.9%, Contraction 퀄리티 7.4% (2009.1~2022.3 L/S 연환산). 국면 완전예지 시 CAGR 14.9%/Sharpe 1.3 vs 동일가중 4%/0.6. **Slowdown 표본 14개월** | [PDF](https://www.samsungpop.com/common.do?cmd=down&saveKey=research.pdf&fileName=1010/2022052516211115K_02_03.pdf&contentType=application/pdf) |
| **IBK투자증권 (2026.1)** | 유사 국면 팩터 타이밍 | KOSPI·S&P500·미/한 장단기금리차·VIX·구리·유가·원/달러 **8개 변수의 YoY 3년 z-score** | [PDF](https://m.ibks.com/iko/IKO01/download.do?seq=2100&gubun=STRATEGY&menuCode=IKO010101&attatchCd=ATTATCH1) |

공통점이 셋이다. **(a) 변수는 몇 개 안 쓴다** — 성장·물가·금리·위험선호 축에 한두 개씩. **(b) level이
아니라 z-score·변화·분위로 쓴다.** **(c) 국면은 시점 t에 관측 가능한 정의**를 쓴다(NBER 침체 날짜
같은 사후 확정 변수는 쓰지 않는다).

---

## 5. 한국 실증

### 5.1 환율 ↔ 외국인 수급 ↔ KOSPI — 환율은 예측변수가 아니다

- **인과 방향은 주가 → 외국인 순매수 → 환율이다.** 이한재(2012, 2006~2011 일별 VAR/SVAR): KOSPI
  수익률이 외국인 순매수비율과 원/달러 수익률을 선행하고, 외국인 순매수가 다시 환율을 선행.
  [DBpia](https://www.dbpia.co.kr/journal/articleDetail?nodeId=NODE02003581)
- BIS WP 245 (Chai-Anant & Ho 2008, 한국 포함 6개 아시아, 1999~2006): 주가와 외국인 순매수는 양방향,
  **통화 수익률은 외국인 수요에 거의 영향 없음**, 순매수가 단기 환율 변화를 일부 설명. 한국·대만·
  태국에서 가장 뚜렷. [BIS](https://www.bis.org/publ/work245.pdf)
- 이충언(2005): 외국인 순매수 → 원화 절상은 **일별·주별에서만**, 월별에서는 사라짐.
  [KCI](https://www.kci.go.kr/kciportal/landing/article.kci?arti_id=ART001113819)
- 자본시장연구원 21-28 (2010.1~2021.8 월별, 표본 내): 외국인 순매매 = KOSPI 상승률(+), 원화 절상률(+),
  다우(+), 외평채 CDS(−), 글로벌 위험지표(−), adj. R² 0.45~0.59.
  [KCMI PDF](https://www.kcmi.re.kr/kcmifile/report_data/1472/reportpdf_1472.pdf)
- 김상배(2023, DCC-GARCH+분위회귀): 외국인 지분율 **감소**만 상관이 높은 국면에서 유의 — 비대칭.
  [KCI](https://www.kci.go.kr/kciportal/ci/sereArticleSearch/ciSereArtiView.kci?sereArticleSearchBean.artiId=ART002932244)

> `00` §4.3에서 본 `flow_foreign_netbuy_20d`–`usdkrw_ret_20d` ρ −0.30은 이 문헌과 부호가 같다. 다만
> 문헌은 그 관계가 **동행**이라고 말한다. 환율은 `flow_foreign_*`의 국면 변수(②)로 쓰되, 환율
> 자체를 예측 재료로 기대하지 않는다.

### 5.2 수출·반도체 사이클 — 실무 핵심, 학술 공백

- 학술 검정은 얇다. 감형규·신용재(2017, 2000~2016 월별, 표본 내): 산업생산·경기선행지수 증가율 (+),
  CD금리 (−), 원/달러 증가율 (+). **수출 직접 검정, 특히 1~20일 잠정치 검정 논문은 찾지 못함.**
  [DBpia](https://www.dbpia.co.kr/journal/articleDetail?nodeId=NODE07099020)
- 실무에서는 경기 축의 대표 변수다. 삼성증권은 발표가 빠르다는 이유로 수출 z-score를 쓴다(§4).
- 반도체 연동은 구조적으로 커졌다. 자본시장연구원(2026.7): **KOSPI–SOX 상관 0.29(2020) → 0.45(2026),
  삼성전자+SK하이닉스 시총 비중 23% → 55%**, 두 종목 상관 0.82.
  [KCMI](https://www.kcmi.re.kr/report/report_view?report_no=2315)
- DRAM 고정가 → 주가 선행을 정량 검정한 자료는 찾지 못했다. 근거 약함.

> 우리 라벨은 시장 내 순위이므로 "반도체가 시장을 끌어올린다"는 정보는 빠진다. 남는 것은
> **종목별 반도체 사이클 exposure(①)**다 — `beta_sox`, `beta_semis_export`. 업종 코드가 없는 지금
> 반도체 밸류체인을 데이터로 근사하는 우회로가 된다.

### 5.3 금리 — 기간 스프레드만 반복해 살아남는다

- **전성주(2020, 보험금융연구 31(1))** — 2000.10~2017.12, 12개 변수, 1/3/12/24개월, MSE-F·ENC-NEW
  부트스트랩. 표본 내: default spread(AA- − BBB-), B/M, D/P, 배당수익률(단기), E/P(장기) 유의.
  **표본 외: B/M과 인플레이션만 생존.** 기간·신용 스프레드·단기/장기금리 전부 탈락.
  [KIRI PDF](https://www.kiri.or.kr/pdf/%EC%A0%84%EB%AC%B8%EC%9E%90%EB%A3%8C/KIRI_2020312_03.pdf)
- **Yoon & Kim (2025, 한국증권학회지 54(3))** — 2008.10~2023.12 월별, LASSO/RF 등. 통화·경기·심리·
  글로벌 변수 중 **국고채 스프레드가 모든 모형에서 일관되게 중요**, 다음이 KOSPI 시차 수익률. 표본
  외(2019~2023) rolling LASSO 누적 110.8% vs 매수보유 30%. 소비자심리·BSI·ESI·M2는 일관되지 않음.
  [e-KJFS](https://www.e-kjfs.org/journal/view.php?number=1014&viewtype=pubreader)
- 윤선중·전귀환(2023, 금융연구): CD 3개월 − KOFR 스프레드가 2개월 이상 누적 KOSPI 수익률을 예측,
  예측력은 기간 스프레드 성분에서. [KISS](https://kiss.kstudy.com/Detail/Ar?key=4010913)
- 양철원(2013): 기간 스프레드는 일관, **신용 스프레드는 위기(1997·2008)에만 강함.**
  [DBpia](https://www.dbpia.co.kr/journal/articleDetail?nodeId=NODE02304454)
- **한미 금리차는 주식자금을 설명하지 못한다.** 한국은행(2022.7): 과거 3차례 역전기에 외국인 증권자금
  순유입. 자본시장연구원(2023.11): 2000년 이후 4차례 역전에서 유출 없음, **VIX만 유의**. KIEP(2020):
  주식자금은 글로벌 금리(push) > 국내 금리(pull).
  [BOK](https://www.bok.or.kr/portal/bbs/B0000347/view.do?nttId=10072008&menuNo=201106) ·
  [KCMI](https://www.kcmi.re.kr/report/report_view?report_no=1762)

### 5.4 글로벌 위험선호 — 근거가 가장 많다

- 정제련·한덕희(2008, VAR): 미국 VIX → 한국은 단방향, S&P500 ↔ KOSPI200 양방향, 전이 시차 약 2일,
  **하락기에 효과가 더 큼.** [KCI](https://www.kci.go.kr/kciportal/landing/article.kci?arti_id=ART001309787)
- 박승혁 외(2022, 2002~2021 GARCH-M): VIX와 한국 수익률 유의한 음(−), 2008년 이후 효과 절반으로,
  단 **VIX>20 또는 일간 +3% 이상이면 강화.** 문턱형 비대칭이다.
  [KCI](https://www.kci.go.kr/kciportal/ci/sereArticleSearch/ciSereArtiView.kci?sereArticleSearchBean.artiId=ART002813637)
- 국가 CDS: 한덕희·이상원(2009): CDS → 채권 단방향, CDS ↔ KOSPI 양방향, 충격 반응 약 3일.
  [KCI](https://kci.go.kr/kciportal/ci/sereArticleSearch/ciSereArtiView.kci?sereArticleSearchBean.artiId=ART001389472)
- VKOSPI: 이정환 외(2024): VKOSPI 상승+급락 뒤 반전, 하락+급등 뒤 지속. VKOSPI−실현변동성
  프리미엄의 예측력 보고도 있다.
  [KCI](https://www.kci.go.kr/kciportal/ci/sereArticleSearch/ciSereArtiView.kci?sereArticleSearchBean.artiId=ART003055121)
- 선물: 유시용·권태훈(2009): 외국인 **예상외** 선물 순매수 → 현물 (+). 김태우·옥기율(2015): 외국인
  선물 포지션은 정보우위, 극단 변동성기엔 소멸. 야간선물이 익일 주간시장 방향을 예측.
  [KCI](https://www.kci.go.kr/kciportal/ci/sereArticleSearch/ciSereArtiView.kci?sereArticleSearchBean.artiId=ART001419402)

### 5.5 국내 거시 발표 — 동행·후행

- 김주일·김병렬(2016): 전경련 BSI는 KOSPI를 예측하지 못하고 **KOSPI가 BSI를 예측.**
  [KCI](https://www.kci.go.kr/kciportal/ci/sereArticleSearch/ciSereArtiView.kci?sereArticleSearchBean.artiId=ART002146075)
- 옥기율·김지수(2012): 소비자심리지수는 발표 전에 이미 반영, 부정 발표만 과대반응.
- **경기선행지수는 구성항목에 KOSPI가 들어 있어** 예측력 검정이 순환논리가 된다.
  [경향신문](https://www.khan.co.kr/article/202605031517001)
- 김종권·유한수(2013): 산업생산 ↔ KOSPI 피드백, 반응 정점 약 8개월; 콜금리 상승 → KOSDAQ 4개월 내 (−).
- 표본 외에서 살아남은 국내 거시 변수는 전성주(2020)의 **인플레이션**이 사실상 유일하다.

### 5.6 중국 연계 — 약하다

- 안유화(2012): 홍콩이 한국을 선행, 상해·심천은 고립. 김재광·신용태(2018): 일본·중국·홍콩 지수로
  익일 KOSPI 방향 정확도 48.4% — 동전 던지기.
- 문정훈·한규식(2024, 2012~2023): KOSPI에는 **미국 변수 영향이 중국보다 큼.**
- 장한익(2024, TVP-VARX): 위안/달러 충격의 KOSPI 반응 부호가 시간에 따라 (−)→(+)로 바뀜.
- **중국 PMI를 직접 검정한 한국 논문은 없다.**

### 5.7 조건부·국면 의존 횡단면 이상현상 — 핵심

| 연구 | 국면 변수 | 결과 | 우리 피쳐 |
|---|---|---|---|
| **Kang, Kwon & Park (2014, EMFT)** 2000.12~2007.2 | 시장 상태(상승/하락) | 외국인 가격영향은 **상승장에서만** 유의, 대형주 집중. 상승장 외국인 매수 대형주에 6개월 모멘텀 | `flow_foreign_netbuy_to_volume`(D). 국면을 나누면 부호가 갈릴 수 있다 |
| **박종원 외 (2020, 한국증권학회지 49(4))** | 시장 상태·회전율 | 한국은 음(−)의 모멘텀(반전)이 기본, **전환시장에서 더 강하고 고회전율 종목에서만** 유의 | `px_mom_12_1`(D, 반대 부호)·`px_turnover_shock`(D, 반대 부호 5/5 일관) — 두 D 결과를 한 번에 설명하는 후보 |
| 이창준·김창하 (2018) | 시장 유동성 | 유동성 높을수록 모멘텀 이익 증가; 시장 비유동성은 모멘텀 수익에 (−) 예측력 | `px_amihud_20d`, 시장 거래대금 국면 |
| **Kim, Park & Ok (2019)** | ΔVIX | VIX 하락 다음 달 저IVOL−고IVOL 스프레드 **2.5%**, VIX 상승 다음 달 **소멸** | `px_idio_vol_60d`·`px_maxret_20d`(A) — IC가 국면 의존일 가능성 |
| 엄철준 외 (2024, 2000.7~2023.6) | 시장 상태·계절성 | 규모효과는 **국면 무관하게 유의**, 개인의 소형주 순매수가 동인 | `fin_log_mcap`(A, 게이트 전부 통과)와 정합 |
| Han, Lee & Kang (2020) | — | 148개 이상현상 중 37.8% 재현. 밸류 69%, 모멘텀 67%, 수익성 5%. **KOSDAQ 제외 시 모멘텀 급감** | `11` §4가 이미 인용 |
| 김수경·변영태 (2011) | — | t−1 외국인 순매수강도는 t일 close-to-close에 (−), close-to-open에 (+) | 일중 분해 — 우리 범위 밖 |
| 삼성증권 (2022) · IBK (2026) | 수출·CPI z / 8변수 z | 국면별 팩터 성과가 크게 다름(§4) | 팩터 타이밍의 실무 근거 |

**여기서 나오는 가장 중요한 가설 둘.**

1. 우리 D등급 셋(`px_mom_12_1`, `px_turnover_shock`, `flow_foreign_netbuy`)은 "신호 없음"이 아니라
   **국면에 따라 부호가 갈리는 신호를 평균낸 결과**일 수 있다. 박종원(2020)과 Kang-Kwon-Park(2014)이
   정확히 그 구조를 보고했다.
2. A등급 변동성 축(`px_idio_vol_60d`, `px_maxret_20d`)의 IC는 **ΔVIX 국면에 따라 켜지고 꺼질** 수
   있다(Kim-Park-Ok 2019). 평균 IC가 커도 국면별로는 0인 구간이 있을 수 있다.

둘 다 **일별 IC 시계열이 있어야 검정할 수 있다**(`00` §4.4).

### 5.8 거시 exposure 베타의 횡단면 가격 결정

- **Chu (2022, FRL 50)** — 로컬·글로벌·환율 세 요인의 세미베타 중 3개가 한국 횡단면에서 유의한
  프리미엄, 특히 **하방 공변동 세미베타**. CAPM보다 설명력 우수.
  [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S1544612322004433)
- 송민규·조성빈(2011, 2002~2009): 환노출 기업 수가 변동성 큰 기간에 급증. 고강석(2019): 원화 절상 시
  거의 전 업종 하락이지만 **위기 제외 시 절반 이상 업종이 (+)** — 부호 불안정. 김진웅(2024): 달러
  베타 음(−), 위기 시 더 커짐.
- 유가: 한국 섹터 수익률은 유가 불확실성 충격에 비대칭 반응(RIBAF 2025).
- **금리 베타를 한국 횡단면 가격 결정 요인으로 검정한 논문은 찾지 못했다.** 공백이다.

> ① exposure 베타는 한국에서도 근거가 있다(Chu 2022). 다만 고강석(2019)의 부호 불안정 경고를
> 받아야 한다 — **환율 베타는 방향을 사전등록하기 어렵고, 양방향 family로 등록하거나 하방 세미베타
> 형태가 낫다.**

---

## 6. 종합 — 지표별 근거 강도와 우리 프로젝트에서의 역할

역할 표기: ① 종목별 exposure 베타(횡단면 피쳐) · ② 국면 × 종목 특성 interaction · ③ 조건부 IC 검증
설계. 우선순위는 근거 강도 × 우리 데이터 현황(`02_` 문서)을 함께 본 것이다.

| 지표 | 글로벌 근거 | 한국 근거 | 역할 | 우선순위 |
|---|---|---|---|---|
| **VIX level·ΔVIX (+ 문턱 20/25)** | Whaley; DM 2016; Kim-Park-Ok | 단방향 전이 2일, VIX>20 비대칭, IVOL 효과 on/off | ②(`px_idio_vol`·`px_maxret`·`flow_foreign_*`), ③ | **1** |
| **시장 상태 (KOSPI 12/24/36개월 누적수익 부호, 실현분산)** | CGH 2004, DM 2016, Wang-Xu | Kang-Kwon-Park 2014, 박종원 2020 | ②(`px_mom_12_1`, `flow_foreign_*`), ③ | **1** |
| **시장 유동성 국면 (KOSPI/KOSDAQ 거래대금 z, 회전율)** | Avramov et al. | 이창준-김창하 2018; `00` §4.3의 IQR ρ −0.72 | ②(수급 3종, `px_amihud`, `px_turnover_shock`), ③ | **1** |
| **국고채 기간 스프레드 (10y−3y) level·Δ·부호** | tms(WG), GKX 트리 모형 중시, Estrella-Hardouvelis | Yoon-Kim 2025 최상위, 윤선중-전귀환 2023 | ②(레버리지·규모·밸류 × 스프레드), ③ | 2 |
| **전일 S&P500·Nasdaq 수익률** | RSZ 2013 | 2일 전이, 개장 10분 반영 | ①(`beta_sp500_lagged`), ② | 2 |
| **USD/KRW 변화·달러지수** | BIS: 달러 절상 → EM 금융조건 악화 | 예측변수 아님(동행), 달러 베타 (−), FX 세미베타 프리미엄(Chu) | ①(`beta_usdkrw`, 하방 세미베타), ②(`flow_foreign_* × usdkrw`) | 2 |
| **국가 CDS 5y** | — | CDS ↔ KOSPI 3일 동학, 외국인 자금 (−) | ②(`flow_foreign_*` 국면) | 3 (원천 확인 필요) |
| **신용 스프레드 (회사채 AA-/BBB- − 국고 3y)** | dfy(WG·GKX) | 위기 국면에만 강함(양철원 2013), OOS 탈락 | ②(레버리지·규모 × 스프레드) | 3 |
| **인플레이션 CPI YoY·가속 여부** | infl(WG), Neville et al. 2021 | 전성주 2020 OOS 생존, 삼성 물가 축 | ②(밸류·규모 × 인플레 국면) | 3 |
| **수출 YoY z-score · 반도체 수출 · SOX** | — | 실무 경기 축(삼성), SOX 상관 0.45↑ | ①(`beta_sox`, `beta_export`), ② | 3 (학술 공백 → 자체 검정) |
| **집계 밸류에이션 (시장 B/M, E/P)** | GKX Table 4 최상위 | 전성주 2020 유일 생존 | ②(개별 밸류 × 집계 밸류) | 3 (횡단면 집계로 자체 계산 가능) |
| **단기금리·기준금리·CD−KOFR** | tbl(WG·GWZ 생존) | 윤선중-전귀환 2023 | ② | 4 |
| **VKOSPI·VRP** | BTZ; GWZ에서 OOS 실패 | 이정환 2024 | ② (VIX와 중복 확인 후) | 4 |
| **심리지수 (Baker-Wurgler 류)** | SYY 2012 short leg | 한국 공개 지수 없음 | ② | 4 (원천 없음) |
| **경기선행지수·BSI·소비자심리·M2** | — | 동행·후행, 선행지수는 KOSPI 포함(순환) | ②(보조) | 5 |
| **불확실성·EPU (미국·한국)** | JLN 2015, BBD 2016, Bali-Brown-Tang 2017 | 분위 인과만(Balcilar 2019) | ①(`beta_epu`) | 5 |
| **중국 PMI·CNY** | — | 약함, 미국 > 중국 | — | 보류 |
| **고용 (실업률·고용률)** | — | Yoon-Kim에서 비일관 | ②(dormant N8) | 보류 유지 (`12`) |

**우선순위 1의 세 축은 이미 카탈로그에 원천이 있다** — VIX, KOSPI 지수·거래대금, 시장 폭. 새 수집
없이 ②·③을 시작할 수 있다는 뜻이다. 상세는 `02_` 문서.

---

## 7. 방법론 주의점 — 문헌이 강조하고 우리 게이트가 이미 묻는 것

| 주의점 | 문헌 | 우리 틀에서의 대응 |
|---|---|---|
| **공표 지연·vintage 개정** — 최종 수정치로 검정하면 예측력이 과장된다. Ghysels-Horan-Moench(2018): real-time 데이터에서 거시 변수의 채권수익 예측력이 크게 줄고, 수정분이 예측력의 상당 부분 | [GHM](https://academic.oup.com/rfs/article-abstract/31/2/678/4090998) | `available_from_date`(월말 + 20일) 정책 기존재. **개정 이력은 없다** — N8 §B1.5와 같은 한계. 원계열 우선 |
| **지속성·Stambaugh bias** — 자기상관 높은 회귀변수는 소표본 편향 | [Hjalmarsson](https://www.federalreserve.gov/econres/ifdp/the-stambaugh-bias-in-panel-predictive-regressions.htm) | 국면 변수는 피쳐가 아니라 조건이므로 직접 회귀하지 않는다. 그러나 ③에서 국면별 IC 차이의 유의성을 볼 때 유효 표본이 작다는 점은 같다 |
| **낮은 OOS R²·데이터 마이닝** — GWZ 결론대로 다수 변수 소멸 | [CT 2008](https://academic.oup.com/rfs/article-abstract/21/4/1509/1567518) | 국면 cut·변수 선택을 **결과 보기 전에 사전등록**(`12`의 Phase C 규율) |
| **regime 정의의 look-ahead** — NBER 침체 날짜 등 사후 확정 변수 사용 금지 | CGH·DM·MSCI 방식 | 시점 t에 관측 가능한 정의만: 누적수익 부호, VIX 문턱, z-score(과거 창) |
| **원시 level 투입 금지** — CPZ 2024: 성과 악화 | [MS](https://pubsonline.informs.org/doi/10.1287/mnsc.2023.4695) | z-score·Δ·부호 flag로 변환. 월간 계열은 YoY·가속도 |
| **temporal placebo** — 국면 변수는 오래 같은 값을 유지해 시계열을 밀어도 비슷한 신호 | `11` §8.4 | ②는 종목 특성과의 곱이라 횡단면 변동이 남지만, **국면이 길수록 placebo 통과가 어렵다.** ①(exposure)이 가장 안전 |
| **국면 표본 크기** — 삼성 Slowdown 14개월 | §4 | 4국면은 표본이 갈린다. 2국면(고/저)부터 시작 |

---

## 8. 참고 문헌 (조사 시점 2026-08-29)

글로벌 — 시계열
- Welch & Goyal (2008, RFS) https://academic.oup.com/rfs/article-abstract/21/4/1455/1565737
- Goyal, Welch & Zafirov (2024, RFS) https://academic.oup.com/rfs/article/37/11/3490/7749383 · 슬라이드 https://www.ivo-welch.org/research/presentations/99-stanford-prediction.pdf
- Rapach, Strauss & Zhou (2010, RFS) https://academic.oup.com/rfs/article-abstract/23/2/821/1604687
- Dong, Li, Rapach & Zhou (2022, JF) https://onlinelibrary.wiley.com/doi/abs/10.1111/jofi.13099
- Chen, Roll & Ross (1986) https://ideas.repec.org/a/ucp/jnlbus/v59y1986i3p383-403.html
- Ludvigson & Ng (2009, RFS) https://academic.oup.com/rfs/article-abstract/22/12/5027/1577464
- Jurado, Ludvigson & Ng (2015, AER) https://www.aeaweb.org/articles?id=10.1257%2Faer.20131193
- Baker, Bloom & Davis (2016, QJE) https://academic.oup.com/qje/article-abstract/131/4/1593/2468873
- Bollerslev, Tauchen & Zhou (2009, RFS) https://academic.oup.com/rfs/article-abstract/22/11/4463/1565787
- Bekaert & Hoerova (2014) https://ideas.repec.org/a/eee/econom/v183y2014i2p181-192.html
- Bybee, Kelly, Manela & Xiu (2024, JF) https://onlinelibrary.wiley.com/doi/full/10.1111/jofi.13377

글로벌 — 횡단면 조건화
- Gu, Kelly & Xiu (2020, RFS) https://academic.oup.com/rfs/article/33/5/2223/5758276
- Chen, Pelger & Zhu (2024, MS) https://pubsonline.informs.org/doi/10.1287/mnsc.2023.4695
- Kelly & Xiu (2023) https://www.nber.org/papers/w31502
- Cooper, Gutierrez & Hameed (2004, JF) https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2004.00665.x
- Daniel & Moskowitz (2016, JFE) https://www.nber.org/papers/w20439
- Wang & Xu (2015, JEF) https://www.sciencedirect.com/science/article/abs/pii/S0927539814001224
- Stambaugh, Yu & Yuan (2012, JFE) https://ideas.repec.org/a/eee/jfinec/v104y2012i2p288-302.html
- Bali, Brown & Tang (2017, JFE) https://www.sciencedirect.com/science/article/abs/pii/S0304405X17302374
- Rapach, Strauss & Zhou (2013, JF) https://onlinelibrary.wiley.com/doi/abs/10.1111/jofi.12041
- BIS QR 2020 (달러와 EME) https://www.bis.org/publ/qtrpdf/r_qt2012b.htm

업계
- MSCI regime https://www.msci.com/research-and-insights/blog-post/factor-and-sector-behavior-across-macro-regimes
- AQR Macro Momentum https://www.aqr.com/-/media/AQR/Documents/Insights/White-Papers/A-Half-Century-of-Macro-Momentum.pdf
- 삼성증권 (2022.5) 국면별 팩터 https://www.samsungpop.com/common.do?cmd=down&saveKey=research.pdf&fileName=1010/2022052516211115K_02_03.pdf&contentType=application/pdf
- IBK투자증권 (2026.1) https://m.ibks.com/iko/IKO01/download.do?seq=2100&gubun=STRATEGY&menuCode=IKO010101&attatchCd=ATTATCH1

한국
- 전성주 (2020, 보험금융연구) https://www.kiri.or.kr/pdf/%EC%A0%84%EB%AC%B8%EC%9E%90%EB%A3%8C/KIRI_2020312_03.pdf
- Yoon & Kim (2025, 한국증권학회지) https://www.e-kjfs.org/journal/view.php?number=1014&viewtype=pubreader
- 윤선중·전귀환 (2023, 금융연구) https://kiss.kstudy.com/Detail/Ar?key=4010913
- 이한재 (2012) https://www.dbpia.co.kr/journal/articleDetail?nodeId=NODE02003581
- Chai-Anant & Ho (2008, BIS WP 245) https://www.bis.org/publ/work245.pdf
- 자본시장연구원 21-28 https://www.kcmi.re.kr/kcmifile/report_data/1472/reportpdf_1472.pdf
- 자본시장연구원 (2026.7) 반도체 집중 https://www.kcmi.re.kr/report/report_view?report_no=2315
- 자본시장연구원 (2023.11) 한미 금리차 https://www.kcmi.re.kr/report/report_view?report_no=1762
- 한국은행 (2022.7) 금리 역전과 자본유출 https://www.bok.or.kr/portal/bbs/B0000347/view.do?nttId=10072008&menuNo=201106
- Kang, Kwon & Park (2014, EMFT) https://ideas.repec.org/a/mes/emfitr/v50y2014is5p131-147.html
- 박종원 외 (2020, 한국증권학회지) https://www.kci.go.kr/kciportal/ci/sereArticleSearch/ciSereArtiView.kci?sereArticleSearchBean.artiId=ART002618717
- Kim, Park & Ok (2019) https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3540597
- 엄철준 외 (2024) https://www.kci.go.kr/kciportal/landing/article.kci?arti_id=ART003130705
- Han, Lee & Kang (2020) https://www.emerald.com/jdqs/article/28/2/3/206237/Market-anomalies-in-the-Korean-stock-market
- Chu (2022, FRL) https://www.sciencedirect.com/science/article/abs/pii/S1544612322004433
- 박승혁 외 (2022) VIX 비대칭 https://www.kci.go.kr/kciportal/ci/sereArticleSearch/ciSereArtiView.kci?sereArticleSearchBean.artiId=ART002813637
- 정제련·한덕희 (2008) https://www.kci.go.kr/kciportal/landing/article.kci?arti_id=ART001309787
- 한덕희·이상원 (2009) CDS https://kci.go.kr/kciportal/ci/sereArticleSearch/ciSereArtiView.kci?sereArticleSearchBean.artiId=ART001389472
- 김주일·김병렬 (2016) BSI https://www.kci.go.kr/kciportal/ci/sereArticleSearch/ciSereArtiView.kci?sereArticleSearchBean.artiId=ART002146075
- 문정훈·한규식 (2024) 미·중 영향 https://www.kci.go.kr/kciportal/landing/article.kci?arti_id=ART003071049
