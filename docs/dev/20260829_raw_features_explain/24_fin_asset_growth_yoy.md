# 24. `fin_asset_growth_yoy` — 총자산 증가율

- 작성일: 2026-08-29
- family: `fin_asset_growth_yoy` · primary feature: 동명 · domain: financial
- **Phase B** · fdr_family `financial` · 기대 부호 `−` · 관측 부호 `−`(사실상 0)
- **discovery 0/4 · screen-pass 0/4** · 등급 **C 4개** · source quality `warn`
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**신호가 없다.** 35개 family 중 |IC|가 가장 작다 — 네 cell이 **0.0003 ~ 0.0044**이고
최소 BH q가 **0.831**이다. t값도 −0.04 ~ −0.36으로 0에 붙어 있다.

부호는 기대와 같은 `−`인데 **크기가 0과 구분되지 않는다.**

**이 family는 "부호가 반대여서 떨어진" 다른 D등급들과 성격이 다르다.**

| 유형 | 예 | 실제 상태 |
|---|---|---|
| 부호가 반대인데 안정적 | `px_turnover_shock` | 반대 방향으로 5/5 일관, q = 3e-11 |
| 부호가 반대이고 불안정 | `px_mom_12_1` | 방향이 오락가락, q = 0.059 |
| **신호 자체가 없음** | **`fin_asset_growth_yoy`** | **IC ≈ 0, q = 0.83** |

**부수 효과 하나가 특히 흥미롭다.** |IC|가 0에 가까워서 **거래가능 유지율이
0.110 ~ 14.53으로 폭주한다** (§5.4). 비율의 분모가 0에 가까우면 비율이 의미를 잃는다는
걸 보여 주는 사례다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의

```sql
-- research/etl/features/fin_scan.py:280
CASE WHEN total_assets_lag4q_selected > 0
     THEN total_assets_selected / total_assets_lag4q_selected - 1
END AS fin_asset_growth_yoy
```

**총자산의 4분기 전 대비 증가율**이다.

- +0.20이면 1년 동안 자산이 20% 늘었다
- 음수면 줄었다

### 2.2 같은 fs_basis를 쓴다

[23_fin_accruals_to_assets.md](23_fin_accruals_to_assets.md) §2.2와 같다. 하루의 회계
기준(CFS/OFS)을 `net_income` 기준으로 하나 정하고 모든 지표가 그 기준을 따른다.

이 family는 지표가 하나(`total_assets`)뿐이라 §2.2의 이점이 직접 드러나지는 않지만,
**분자와 분모(4분기 전 값)가 같은 기준**이라는 점은 중요하다. 연결 편입·제외가 있으면
기준이 바뀌어 증가율이 튄다.

### 2.3 `value_lag_4q`가 원천이다

`total_assets_lag4q_selected`는 B-3의 `value_lag_4q`다. **4분기 전에 실제로 보고됐던 값**이다.

즉 "지금 시점에서 본 1년 전 총자산"이 아니라 **"1년 전에 공시됐던 총자산"**이다. 사후
정정을 반영하지 않는다는 점에서 PIT 원칙에 맞다.

### 2.4 PIT와 vintage 나이

`asset_growth_available_from` 기준 interval join이다. 정본 변형은 **`native_t`**다.

| 시장 | 평균 나이 | 95분위 나이 |
|---|---:|---:|
| KOSDAQ | 74.0일 | 180일 |
| KOSPI | 74.2일 | 174일 |

평균 74일 된 정보다. [23](23_fin_accruals_to_assets.md)과 같은 수준이다.

### 2.5 산식 버전

`formula_version: fin_v4`. 같은 마트의 다섯 family가 공유한다.
`fin_v1`의 NULL 가드 버그는 밸류 계열에 직접 영향을 줬고 이 family는 상대적으로 덜 받았지만,
버전은 함께 올라간다 (`10_known_issues.md` I1).

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/fin_scan.py:280` |
| fs_basis 통일 설계 | 같은 파일 모듈 docstring |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:460` |

---

## 3. 왜 예측한다고 봤나 — 가설

### 3.1 메커니즘

**투자 이례현상(investment anomaly)이다.**

Cooper, Gulen & Schill (2008)의 발견이다. 자산을 빠르게 늘린 회사가 이후 수익률이 낮다.
설명은 두 갈래다.

- **과잉투자.** 경영진이 좋은 시절에 과도하게 투자하고, 그 투자가 기대만큼 수익을 내지
   못한다.
- **과대평가.** 자산이 늘었다는 건 자금을 조달했다는 뜻이고, 자금 조달은 주가가 비쌀 때
   일어난다 ([20_ev_net_share_issuance_yoy.md](20_ev_net_share_issuance_yoy.md) §3.1과
   같은 논리다).

**둘 다 과대평가 계열 가설**이라 부호가 음수다.

### 3.2 기대 부호

`−`. 자산 증가율이 클수록 이후 초과수익률 순위가 낮다.

### 3.3 사전등록 horizon

```yaml
# horizon_scan_config.yaml:466
primary_horizon_set: [60, 120]
exploratory_horizon_set: [20, 40]
include_bucket_primary: true
```

분기 공시 기반의 느린 지표이므로 긴 horizon에 걸었다. cell은 4개다.

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 60~120일 | **BH 통과 cell 0개** |
| 부호 | `−` | `−`이지만 크기가 0 |

### 3.4 한국 시장 단서 — 미리 예고돼 있었다

`02_feature_candidate.md` §1의 10번 항목(`Q4`)이다.

> 투자/자산성장 | `fin_asset_growth_yoy` | Q4 | `-` | A | R1

그런데 `11_feature_taxonomy.md` §4가 인용한 Han, Lee & Kang (2020)의 한국 복제 결과가
결정적이다.

> 수익성 5.0%(엄격 기준 0.0%), **투자 24.1% 복제율**이 `fin_gross_profitability`와
> **`fin_asset_growth_yoy`의 무신호**를 … 예측한다. **데이터 결함이 아니라 시장 특성일
> 가능성이 크게 올라간다.**

**148개 이례현상을 한국에서 복제한 연구에서 투자 카테고리 복제율이 24.1%였다.** 미국에서
잘 작동하는 이 이례현상이 한국에서는 4개 중 1개꼴로만 재현된다는 뜻이다.

**이번 결과가 그 예측과 맞았다.**

분류 좌표는 C2(재무 기반 상태) × T1(변화) × U다.

### 3.5 근거 문헌

Cooper, Gulen & Schill (2008), *Asset Growth and the Cross-Section of Stock Returns*.
등급 A (미국 기준).

---

## 4. 얼마나 효과가 있었나 — 사실상 0이다

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이(정렬) | AB q | 판정 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→60 | **−0.00032** | −0.005 | **−0.04** | +0.56%p | **1.000** | BH 실패 (C) |
| cum | 0→120 | −0.00438 | −0.069 | −0.35 | +1.27%p | 0.797 | BH 실패 (C) |
| bucket | 40→60 | −0.00040 | −0.007 | −0.08 | +0.35%p | 0.992 | BH 실패 (C) |
| bucket | 60→120 | −0.00293 | −0.051 | −0.36 | +0.89%p | 0.797 | BH 실패 (C) |

**35개 중 |IC|가 가장 작다.** `cum 0→60`의 −0.00032는 소수점 넷째 자리에서야 0이 아니다.

t값이 −0.04다. **이보다 0에 가까울 수 없다.**

### 4.2 그런데 5분위 차이는 0이 아니다

IC가 0인데 5분위 정렬 차이가 +0.56%p ~ +1.27%p로 나온다.

**해석하면 안 된다.** 두 가지 이유다.

1. **어느 값도 유의하지 않다.** t = −0.04 ~ −0.36이다. 5분위 차이의 표준오차는 산출하지
   않았지만, IC가 이 수준이면 spread도 잡음 안에 있다고 봐야 한다.
2. **§5.4에서 보듯 이 family의 비율 지표들이 전부 불안정하다.** 분모가 0에 가까울 때
   나오는 값이다.

**"IC는 0인데 수익은 난다"는 이야기로 읽으면 안 된다.**

### 4.3 크기 비교

Phase B 재무 계열 안에서도 압도적으로 작다.

| family | 대표 \|IC\| | 최소 BH q |
|---|---:|---:|
| `fin_value_z` | 0.122 | ~0 |
| `fin_log_mcap` | 0.115 | ~0 |
| `fin_gross_profitability` | (§25 참조) | 0.42 |
| `fin_accruals_to_assets` | 0.018 | 0.0004 |
| **`fin_asset_growth_yoy`** | **0.004** | **0.797** |

### 4.4 신호의 모양

| 관찰 | 값 |
|---|---|
| `peak_cell` | `cum 0→120` |
| `peak_ic_mean` | **−0.00438** |
| `q_fdr_phase_b_min` | 0.831 |

`peak_ic_mean`이 −0.0044라는 건 **가장 강한 cell조차 0.4% 수준**이라는 뜻이다.

---

## 5. 진짜인가 — 강건성

**신호가 없으므로 강건성 검사가 대부분 의미를 잃는다.** 다만 그 무의미함 자체가 정보다.

### 5.1 기간 일관성 — 4구간 중 1~2구간

| cell | `valid_subperiods` | `sign_consistent_subperiods` | `period_sign_pass` |
|---|---:|---:|---|
| cum 0→60 | 4 | 1 | False |
| cum 0→120 | 4 | 2 | False |
| bucket 40→60 | 4 | 2 | False |
| bucket 60→120 | 4 | 2 | False |

**절반 안팎이다.** 동전 던지기와 구분되지 않는다.

표본이 2016-06-27부터라 구간이 5개가 아니라 4개다.

### 5.2 시간 placebo — 전면 실패

| cell | `p_temporal_nw` |
|---|---:|
| cum 0→60 | **0.9901** |
| cum 0→120 | **0.9208** |
| bucket 60→120 | **0.8713** |

기준은 0.10이다. **0.99는 사실상 최댓값이다** — 100번의 시간 이동 placebo가 거의 전부
관측값만큼 극단적이었다.

35개 중 가장 나쁜 값이다. `px_resid_mom_12_1`(0.614), `px_near_52w_high`(0.772)보다도 높다.

### 5.3 비중첩 offset — `complete`인데 두 cell만 통과

`cum 0→120`, `bucket 60→120`은 `nonoverlap_robustness_pass = True`,
`cum 0→60`은 `False`다.

**신호가 없을 때 이런 검사는 통과 여부가 무작위에 가깝다.** 통과했다는 사실에 의미를
두면 안 된다.

### 5.4 거래가능 유지율이 폭주한다 — 이 문서의 교훈

| cell | `tradable_retention` | `tradable_pass` |
|---|---:|---|
| cum 0→60 | **14.533** | **False** |
| cum 0→120 | **0.410** | **False** |
| bucket 40→60 | **4.791** | **False** |
| bucket 60→120 | **0.110** | **False** |

**14.5배와 0.11배가 같은 family 안에 있다.**

이유는 정의에 있다.

```python
# research/analysis/horizon_scan_runner.py:920
retention = abs(ic_tradable) / abs(ic_broad)
```

**분모가 `broad` IC의 절대값**이다. 이 family는 그 값이 0.0003 수준이다. **0에 가까운 수로
나누면 결과가 폭주한다.**

`cum 0→60`은 broad −0.00032, tradable이 그 14.5배인 −0.0047 정도다. 절대 크기로 보면
**둘 다 0이다.** 비율만 14.5로 나온다.

`tradable_pass`가 전부 `False`인 것도 같은 이유다 — 게이트는 유지율뿐 아니라 **부호 일치**도
요구하는데(`same_direction`), IC가 0 근처면 부호가 쉽게 뒤집힌다.

**유지율을 읽을 때는 항상 분모의 절대 크기를 함께 봐야 한다.** 이 사례가 그 이유를
보여 준다.

### 5.5 생존편향 — 두 cell 실패

`available_direction_pass`가 `cum 0→60`과 `bucket 40→60`에서 **False**다.
`common_survivor`와 `available`의 IC 부호가 뒤집혔다는 뜻이다.

이것도 §5.4와 같은 이유다. **0 근처에서는 부호가 쉽게 뒤집힌다.**

### 5.6 source quality — `warn`

[23_fin_accruals_to_assets.md](23_fin_accruals_to_assets.md) §5.6과 **완전히 같은 값**이다.
같은 마트·같은 분기 재무 vintage를 쓰기 때문이다.

| 항목 | 값 |
|---|---|
| `source_quality_reasons` | `revision` |
| `revision_ratio` | 0.1014 (`total_assets`) |
| `mapping_fallback_ratio` | 0.3176 (`net_income`) |

**총자산의 10.1%가 사후 정정됐다.** 이 family는 총자산이 유일한 입력이므로 **이 경고가
직접 걸린다.**

### 5.7 등급 C인 이유

`failed_gates`가 cell마다 셋~다섯 개다.

| cell | `failed_gates` |
|---|---|
| cum 0→60 | `primary_discovery`, `tradable_pass`, `period_sign_pass`, `available_direction_pass`, `robustness_pass` |
| bucket 40→60 | `primary_discovery`, `tradable_pass`, `period_sign_pass`, `available_direction_pass` |
| cum 0→120 | `primary_discovery`, `tradable_pass`, `period_sign_pass`, `robustness_pass` |
| bucket 60→120 | 동일 |

**available 방향 게이트 실패가 등급 상한을 C로 만든다**
(`decision.available_sign_flip_max_grade: C`).

---

## 6. 표본과 커버리지

| 항목 | 값 |
|---|---|
| 유효 표본 | **2016-06-27 ~ 2025-02-05** |
| 유효 거래일 | **1,927일** |
| 날짜당 평균 종목 수 | **919~921개** |
| `coverage_ratio` | **0.631** |
| 관측 행 수 | 4,548,653 |

[23](23_fin_accruals_to_assets.md)(0.596, 865종목)보다 조금 낫다. 총자산 하나만 있으면
되고 영업현금흐름이 필요 없기 때문이다.

시장별로는 KOSDAQ 0.663 / KOSPI 0.581이다.

---

## 7. 중복성

### A×B 교차 상관

| 상대 family | 평균 순위상관 | 유효일 | 범위 |
|---|---:|---:|---|
| `px_amihud_20d` | **−0.224** | 1,927 | −0.30 ~ +0.12 |
| `px_mom_12_1` | **+0.157** | 1,927 | −0.23 ~ +0.28 |
| `px_near_52w_high` | +0.087 | 1,927 | −0.11 ~ +0.26 |

**두 관계가 눈에 띈다.**

- **`px_amihud_20d`와 −0.224.** 자산이 큰 폭으로 늘어난 회사는 비유동성이 낮다 =
  규모가 크다. `px_amihud_20d`가 사실상 규모 지표라는 점을
  ([07_px_amihud_20d.md](07_px_amihud_20d.md) §7) 생각하면, **자산성장이 규모와 얽혀 있다**는
  뜻이다.
- **`px_mom_12_1`과 +0.157.** 1년간 주가가 오른 회사가 자산도 늘렸다. 둘 다 1년 창을 쓴다.

**신호가 없는 family인데도 다른 축과 상당히 겹친다.** 정보를 담고 있긴 하지만 그 정보가
미래 수익률과 연결되지 않는다는 뜻이다.

### 확인하지 않은 중복

같은 `feat_fin_scan_daily` 마트의 다섯 family 간 B×B 상관이 없다
([23](23_fin_accruals_to_assets.md) §7 참조).

[20_ev_net_share_issuance_yoy.md](20_ev_net_share_issuance_yoy.md)와도 경제적으로 얽힌다 —
자산이 늘려면 대개 자금을 조달해야 하고, 그게 주식 발행이다. **두 family가 같은 현상의
두 측면일 수 있는데 상관을 재지 않았다.**

---

## 8. 한계와 확인 못 한 것

1. **신호가 없다** (§4). 최소 q 0.797, t −0.04 ~ −0.36. 이 family에 대해 말할 수 있는 건
   여기까지다.
2. **다만 "한국에서 안 된다"는 결론은 조심해야 한다.** §3.4의 복제율 24.1%는 "안 된다"가
   아니라 "4개 중 1개만 재현된다"는 뜻이다. **한 번의 측정으로 시장 특성을 확정할 수 없다.**
3. **거래가능 유지율이 무의미하다** (§5.4). 0.11 ~ 14.53. 이 값을 성능 지표로 읽으면 안 된다.
4. **표본이 2016년부터라 기간 검정이 4구간뿐이다** (§5.1, §6).
5. **총자산의 10.1%가 사후 정정됐다** (§5.6). 유일한 입력이라 직접 영향이다.
6. **평균 74일 된 정보를 쓴다** (§2.4).
7. **`ev_net_share_issuance_yoy`와의 관계를 안 쟀다** (§7).
8. **업종 중립화가 없다.** 업종마다 자산 성장 속도가 근본적으로 다르다. 은행과 게임회사가
   한 KOSPI 풀에서 비교된다.
9. **분해하지 않았다.** 자산 증가를 유형자산·재고·매출채권 등으로 나누면 다른 결과가 나올
   수 있는데 확인하지 않았다.
10. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나

**T2 14-feature bundle에 안 들어갔다.** discovery 0개라 후보에서 빠졌다.

---

## 10. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# |IC| 가 0 에 가까우면 tradable_retention 이 무의미해진다
print(duckdb.sql(f"""
  select family, scan_type, h_start, h_end, ic_mean, t_nw,
         q_fdr_global_ab, tradable_retention, tradable_pass,
         available_direction_pass, p_temporal_nw, evidence_grade
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  where family='fin_asset_growth_yoy' order by scan_type, h_end
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 원천 품질 | 같은 B run의 `core/quarterly_metric_quality.parquet` |
| 산식 | `research/etl/features/fin_scan.py:280` |
| 유지율 정의 | `research/analysis/horizon_scan_runner.py:920` |
| 한국 복제 연구 대조 | `01_feature_candidate/11_feature_taxonomy.md` §4 |
| 서술 대조 | `01_feature_candidate/09_all_feature_results.md` §7 |
