# 32. `own_insider_filing_activity` — 임원·주요주주 공시 급증

- 작성일: 2026-08-29
- family: `own_insider_filing_activity` · primary feature: **`own_insider_filing_burst_60d`** · domain: ownership
- **Phase B** · fdr_family `ownership` · **기대 부호 없음(양방향)** · 관측 부호 불명
- **discovery 0/4 · screen-pass 0/4** · 등급 **C 2개 / D 2개** · source quality `not_applicable`
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**신호가 없다. 그리고 커버리지가 35개 중 가장 낮다.**

| 항목 | 값 |
|---|---|
| 최대 \|IC\| | **0.0032** |
| 최소 BH q | **0.371** |
| 최대 \|t(NW)\| | **0.99** |
| **`coverage_ratio`** | **0.253** |
| **날짜당 종목 수** | **312개** |
| 부호 | **cell마다 갈린다** (누적 `−`, 구간 `+`) |

[24_fin_asset_growth_yoy.md](24_fin_asset_growth_yoy.md)와 같은 "신호 없음" 유형인데,
**원인이 다르다.** 저쪽은 값은 다 있는데 예측력이 없었고, 이쪽은 **애초에 4분의 3에
값이 없다.**

임원·주요주주가 소유상황보고서를 낸 적 있는 회사만 값을 갖는데, 그런 회사가 전체의
4분의 1뿐이다.

---

## 2. 무엇을 재는가 — 산식 정본

### 2.1 정의 — [19](19_ev_filing_activity.md)와 같은 틀, 세는 대상만 다름

**1단계 — 60거래일 임원·주요주주 공시 건수**

```sql
-- research/etl/features/filing_activity.py:122
SUM(insider_filings) OVER (
    PARTITION BY ticker, market ORDER BY trade_date
    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
) AS own_insider_filing_60d
```

**2단계 — 그 회사의 평소 수준으로 나눈다**

```sql
-- research/etl/features/filing_activity.py:136
CASE WHEN session_ordinal >= 250 THEN
    own_insider_filing_60d / NULLIF(
        quantile_cont(own_insider_filing_60d, 0.5) OVER (
            PARTITION BY ticker, market ORDER BY trade_date
            ROWS BETWEEN 249 PRECEDING AND CURRENT ROW
        ), 0)
END AS own_insider_filing_burst_60d
```

**[19_ev_filing_activity.md](19_ev_filing_activity.md) §2.1과 완전히 같은 CASE 문에서
나온다.** 코드의 `burst_columns` 루프가 `ev_filing`과 `own_insider_filing` 둘을 한 번에
만든다.

**차이는 분자에 세는 공시 종류뿐이다.**

| family | 세는 공시 |
|---|---|
| `ev_filing_activity` | **전체 공시** |
| **`own_insider_filing_activity`** | **임원·주요주주 특정증권등 소유상황보고서만** |

### 2.2 세는 공시가 무엇인가

```python
# research/etl/features/filing_activity.py:69
#: 임원ㆍ주요주주 특정증권등 소유상황보고서 — the ``elestock`` event, as a receipt.
INSIDER_MARKER = "주요주주특정증권등소유상황보고서"
```

**임원이나 주요주주가 자기 회사 주식을 사고팔면 내는 공시다.** 원천에 **139,697건,
2,607종목**이 있다.

`elestock`은 OpenDART의 해당 API 이름이다.
[18_ev_amendment_ratio.md](18_ev_amendment_ratio.md) §2.6에서 본 대로, 그 API가 2년치만
주기 때문에 **접수 이력에서 건수만 세는 방식으로 대체**했다.

> The collected path could have given **holdings changes**, this one gives
> **filing counts**.
> — `research/etl/features/filing_activity.py` 모듈 docstring

**이 대체가 이 family의 성격을 결정한다.** 누가 얼마나 샀는지는 모르고, **몇 건 냈는지만**
안다. 매수와 매도를 구분하지 못한다 (§8).

### 2.3 분모가 0이면 값이 없다 — 커버리지 25%의 원인

```sql
NULLIF(quantile_cont(own_insider_filing_60d, 0.5) OVER (...), 0)
```

**250거래일 동안의 60일 누적 건수 중앙값이 0이면 NULL이다.**

임원·주요주주 공시를 **거의 안 내는 회사**는 그 중앙값이 0이고, 값이 만들어지지 않는다.

결과가 커버리지 **0.253**, 날짜당 **312종목**이다. 35개 중 최저다.

| family | 커버리지 | 날짜당 종목 |
|---|---:|---:|
| `ev_filing_activity` | 0.821 | 999 |
| `own_amendment_ratio` | 0.754 | 879 |
| **`own_insider_filing_activity`** | **0.253** | **312** |

**전체 종목의 4분의 1만 이 신호로 판단할 수 있다는 뜻이다.**

### 2.4 250일 baseline과 PIT

[19_ev_filing_activity.md](19_ev_filing_activity.md) §2.3~§2.5와 같다.

- `session_ordinal >= 250` 조건 때문에 유효 시작이 **2015-07-06**이다
- 접수일 D의 공시는 **D+1 거래일부터** 노출
- 공시가 없는 날을 0으로 채운다
- 정본 변형은 **`native_t`**, `formula_version: filing_v3`

### 2.5 등록됐지만 안 돌린 것

```yaml
features:
  - {column: own_insider_filing_burst_60d,  role: primary}
  - {column: own_insider_filing_60d,        role: secondary}
  - {column: own_insider_filing_120d,       role: secondary}
  - {column: own_insider_filing_burst_120d, role: secondary}
```

secondary 셋이 등록돼 있는데 **이번 run에는 primary 4개 cell만 있다.**

§2.3의 커버리지 문제를 생각하면 **건수(count) 형태가 비율(burst)보다 나았을 수 있다** —
분모가 0이어도 건수는 0으로 정의되기 때문이다. **확인하지 않았다.**

### 2.6 코드 위치

| 대상 | 경로 |
|---|---|
| 건수 산식 | `research/etl/features/filing_activity.py:122` |
| burst 산식 | `research/etl/features/filing_activity.py:136` |
| 공시 종류 상수 | `research/etl/features/filing_activity.py:69` |
| 대체 설계 근거 | 같은 파일 모듈 docstring |
| 사전등록 | `research/analysis/horizon_scan_expansion_20260827.yaml` |

---

## 3. 왜 방향을 열어 뒀나

### 3.1 건수만으로는 방향을 알 수 없다

```yaml
- family: own_insider_filing_activity
  expected_sign: null       # ← 방향을 걸지 않았다
```

**이 family에서 방향을 열어 둔 이유는 다른 양방향 family와 다르다.**

[12_flow_individual_netbuy_to_volume.md](12_flow_individual_netbuy_to_volume.md)나
[19_ev_filing_activity.md](19_ev_filing_activity.md)는 **가설이 갈려서** 열어 뒀다.
이 family는 **측정값 자체가 방향 정보를 안 담아서** 열어 뒀다.

| 가설 | 메커니즘 | 예측 부호 |
|---|---|---|
| **내부자 매수** | 임원이 자기 회사를 산다 = 좋은 신호 | `+` |
| **내부자 매도** | 임원이 판다 = 나쁜 신호 | `−` |
| **지분 변동 활발** | 경영권 분쟁·담보 등 불안정 | `−` |

**§2.2에서 본 대로 건수는 매수와 매도를 구분하지 않는다.** 임원이 많이 사도 공시가 늘고,
많이 팔아도 공시가 는다. **두 반대 방향이 같은 값으로 섞인다.**

**신호가 안 나온 가장 그럴듯한 이유가 이것이다** (§8).

### 3.2 사전등록 horizon

```yaml
primary_horizon_set: [20, 60]
exploratory_horizon_set: [1, 5, 10, 40, 120]
include_bucket_primary: true
```

[19_ev_filing_activity.md](19_ev_filing_activity.md)와 같다. 60일 창의 급증 지표라 반응이
빠를 것으로 봤다. cell은 4개다.

| | 사전등록 primary | 실제 결과 |
|---|---|---|
| 밴드 | 20~60일 | **discovery 0개** |
| 부호 | 없음 | **cell마다 갈림** |

### 3.3 사전등록 시점

2026-08-27 확장 등록분이다 (`outcome_blind: true`).

분류 좌표는 **C3(수급·소유·내부자)** × T2(놀라움) × U다.
`11_feature_taxonomy.md` §2.1이 지목한 C3의 빈 칸 **「내부자·최대주주」**를 메우는 시도다.

### 3.4 근거 문헌

없다. 접수 이력에서 만든 신규 지표다. 내부자 거래 이례현상(Lakonishok & Lee 2001 등)이
가장 가까운 배경이지만 **그 연구들은 거래 금액·방향을 쓰고 이 피처는 건수만 쓴다.**

---

## 4. 얼마나 효과가 있었나 — 신호가 없다

### 4.1 사전등록 cell 전체 (`broad` × `common_survivor` × `native_t`)

| scan | horizon | Rank IC | ICIR | t(NW) | 5분위 차이 | AB q | 등급 |
|---|---|---:|---:|---:|---:|---:|---|
| cum | 0→20 | **−0.00036** | −0.008 | **−0.12** | +0.05%p | 0.984 | C |
| cum | 0→60 | **−0.00022** | −0.005 | **−0.04** | +0.26%p | **1.000** | C |
| bucket | 10→20 | +0.00173 | 0.039 | 0.76 | +0.05%p | 0.511 | **D** |
| bucket | 40→60 | **+0.00322** | 0.069 | 0.99 | +0.07%p | 0.371 | **D** |

**|IC|가 0.0032를 넘지 않는다.** `cum 0→60`의 −0.00022는 소수점 넷째 자리에서야 0이 아니다.

t값이 −0.04다. [24_fin_asset_growth_yoy.md](24_fin_asset_growth_yoy.md)와 나란히 **35개 중
0에 가장 가깝다.**

### 4.2 부호가 cell마다 갈린다

**누적 cell은 음수, 구간 cell은 양수다.**

| cell 유형 | 부호 |
|---|---|
| `cum 0→20`, `cum 0→60` | `−` |
| `bucket 10→20`, `bucket 40→60` | `+` |

양방향 family라 기대 부호가 없으므로 이 자체가 게이트 실패는 아니다. 다만
**§5.1의 기간 일관성이 누적 cell에서 2/5로 떨어지는 것과 이어진다.**

**신호가 없을 때 나타나는 전형적인 모습이다.**

### 4.3 등급 C와 D가 갈린 이유

| cell | 등급 | `failed_gates` |
|---|---|---|
| bucket 10→20 | **D** | `primary_discovery` |
| bucket 40→60 | **D** | `primary_discovery` |
| cum 0→20 | C | `primary_discovery`, `period_sign_pass`, `available_direction_pass` |
| cum 0→60 | C | `primary_discovery`, `tradable_pass`, `period_sign_pass`, `available_direction_pass`, `robustness_pass` |

**bucket 두 개는 실패 목록이 짧은데 D이고, cum 두 개는 목록이 긴데 C다.**

거꾸로 보이지만 규칙대로다.

- **D**: `no_signal_or_wrong_sign_or_robustness_fail` — 신호가 없으면 D
- **C**: `exploratory_or_secondary_or_available_sign_flip` — **available 부호 뒤집힘이 있으면
  등급 상한이 C**가 되어 D보다 위에 놓인다 (`decision.available_sign_flip_max_grade: C`)

**C가 D보다 나은 판정이 아니다.** 등급 평가 순서가 `[R, C, A, B, D]`라 C 조건에 먼저
걸리면 거기서 확정된다.

---

## 5. 진짜인가 — 강건성

**신호가 없으므로 대부분의 검사가 의미를 잃는다.** 다만 실패 양상 자체가 정보다.

### 5.1 기간 일관성 — cell마다 2/5와 4/5로 갈린다

| cell | `valid_subperiods` | `sign_consistent_subperiods` | `period_sign_pass` |
|---|---:|---:|---|
| cum 0→20 | 5 | **2** | **False** |
| cum 0→60 | 5 | **2** | **False** |
| bucket 10→20 | 5 | 4 | True |
| bucket 40→60 | 5 | 4 | True |

**누적 cell이 2/5다.** 양방향 family이므로 관측 부호 기준인데, 다섯 구간 중 둘만 맞았다.
**동전 던지기와 구분되지 않는다.**

구간은 5개다 — 표본이 2015-07-06부터라 `2014_2016`이 부분적으로 잡힌다.

### 5.2 거래가능 유지율이 폭주한다

| cell | `tradable_retention` | `tradable_pass` |
|---|---:|---|
| cum 0→20 | 0.628 | True |
| cum 0→60 | **3.143** | **False** |
| bucket 10→20 | 1.158 | True |
| bucket 40→60 | **1.592** | True |

**3.14와 0.63이 같은 family 안에 있다.**

[24_fin_asset_growth_yoy.md](24_fin_asset_growth_yoy.md) §5.4와 같은 현상이다. 유지율 정의가
`|tradable IC| / |broad IC|`인데 **분모가 0.0002 수준이면 비율이 폭주한다.**

`cum 0→60`의 `tradable_pass`가 False인 건 부호가 뒤집혔기 때문이다.

**유지율을 읽을 때는 항상 분모의 절대 크기를 함께 봐야 한다.**

### 5.3 생존편향 — 누적 두 cell 실패

`available_direction_pass`가 `cum 0→20`, `cum 0→60`에서 **False**다.
`common_survivor`와 `available`의 IC 부호가 뒤집혔다.

**§5.2와 같은 이유다.** 0 근처에서는 부호가 쉽게 뒤집힌다.

### 5.4 시간 placebo

| cell | `p_temporal_nw` |
|---|---:|
| cum 0→60 | **0.9802** |
| 나머지 셋 | 대상 아님 (NW lag < 59) |

**0.98이다.** 100번의 시간 이동 placebo가 거의 전부 관측값만큼 극단적이었다.

### 5.5 비중첩 offset

`cum 0→60`만 `offset_status = complete`이고 `nonoverlap_robustness_pass = False`다.
검정을 돌릴 수는 있었는데 통과하지 못했다.

### 5.6 source quality — 경고 없음

`source_quality_status` = **`not_applicable`**. 접수 이력은 사후 수정되지 않는다.

**품질 경고가 없는데도 D등급인 사례다.** 원천이 깨끗한 것과 신호가 있는 것은 별개다.

---

## 6. 표본과 커버리지 — 35개 중 최저

| 항목 | 값 |
|---|---|
| 유효 표본 | **2015-07-06 ~ 2025-02-05** |
| 유효 거래일 | **2,354일** |
| **날짜당 평균 종목 수** | **312개** |
| **`coverage_ratio`** | **0.253** |
| 관측 행 수 | **1,741,331** |

**커버리지·종목 수·관측 행 수 모두 35개 중 최저다.**

시장별로는 KOSDAQ 0.230 / KOSPI 0.288이다. **드물게 KOSPI가 더 높다** — 대형주 쪽에서
임원·주요주주 공시가 더 자주 나온다는 뜻으로 보이지만 확인하지 않았다.

**날짜당 312종목**이면 5분위 각 칸에 62종목뿐이다. 횡단면 순위의 분해능이 그만큼 낮다.

§2.3에서 본 대로 **분모(250일 중앙값)가 0인 회사가 4분의 3이다.**

---

## 7. 중복성

**A×B 상관 산출물에 이 family의 행이 없다.** primary 목록에는 들어가지만 상대 family와의
유효 교집합 날짜가 부족했던 것으로 보인다 — 확인하지 않았다.

`family_summary`의 `top_rank_correlation_pair`는 `px_amihud_20d`, 값은 **−0.084**다.

### 확인하지 않은 중복

1. **[19_ev_filing_activity.md](19_ev_filing_activity.md)와 부분집합 관계다.** 전체 공시
   건수와 임원 공시 건수는 분자가 포함 관계다. 게다가 **같은 CASE 문에서 나온다**(§2.1).
   B×B 상관이 없다.
2. **[33_own_major_filing_activity.md](33_own_major_filing_activity.md)와 형제다.** 하나는
   임원 공시, 하나는 5% 대량보유 공시를 센다. 둘 다 지분 공시다.
3. **[31_own_amendment_ratio.md](31_own_amendment_ratio.md)의 분모와 겹친다.** 저쪽의
   `ownership_filings`가 임원 + 대량보유이므로 이 family의 분자를 포함한다.

---

## 8. 한계와 확인 못 한 것

1. **매수와 매도를 구분하지 못한다** (§3.1). 이 family가 신호를 못 낸 가장 그럴듯한
   이유다. 반대 방향 두 사건이 같은 값으로 섞인다. **건수 기반 대체 설계의 근본 한계다.**
2. **커버리지가 25%다** (§6). 종목의 4분의 3에 값이 없다.
3. **신호가 없다** (§4). 최대 |IC| 0.0032, 최소 q 0.371, 최대 |t| 0.99.
4. **유지율이 무의미하다** (§5.2). 0.63 ~ 3.14.
5. **secondary 셋을 안 돌렸다** (§2.5). 특히 **건수(count) 형태가 커버리지 문제를 피할 수
   있었는데 확인하지 않았다.**
6. **형제 family들과의 중복이 미확인이다** (§7).
7. **금액·지분율을 안 본다.** 원천 API(`elestock`)는 2년치만 주지만, 그 2년치라도
   금액·방향을 보는 별도 진단은 하지 않았다.
8. **업종 중립화가 없다.**
9. **어느 종목이 언제 기여했는지 모른다** ([00_읽는_법.md](00_읽는_법.md) §7).
10. **holdout을 열지 않았다.**

---

## 9. 모델에서는 어땠나

**T2 14-feature bundle에 안 들어갔다.** discovery 0개라 후보에서 빠졌다.

같은 ownership 계열에서는 [31_own_amendment_ratio.md](31_own_amendment_ratio.md),
[33_own_major_filing_activity.md](33_own_major_filing_activity.md),
[34_own_major_stake_change.md](34_own_major_stake_change.md),
[35_own_major_stake_level.md](35_own_major_stake_level.md) 넷이 들어갔다.

---

## 10. 다음에 할 일

이 family는 **"안 된다"가 아니라 "이 형태로는 못 잰다"**로 분류하는 게 맞다. §3.1과 §8의
1번이 이유다.

세 갈래가 있다.

1. **방향을 담은 지표로 바꾼다.** `elestock` API가 주는 2년치로 매수·매도를 나눈 별도
   피처를 만들고, 짧은 표본을 감안해 exploratory로 사전등록한다.
2. **건수 형태(secondary)를 돌린다** (§2.5). 커버리지가 25%에서 크게 늘어날 수 있다.
3. **`own_major_filing_activity`와 합친다.** 임원과 5% 대량보유를 나눠 세는 게 의미가
   있는지부터 확인한다.

**어느 쪽이든 새 config로 사전등록해야 한다.**

---

## 11. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
B=f"research/output/horizon_scan/phase=B/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T123313-4e0ae8b0"
# 커버리지가 가장 낮은 family 를 확인한다
print(duckdb.sql(f"""
  select family, primary_feature, coverage_ratio, observations, effective_start
  from '{B}/core/family_summary.parquet'
  order by coverage_ratio limit 8
""").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| Phase B cell 상세 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/horizon_ic.parquet` |
| 커버리지 | 같은 B run의 `core/feature_coverage.parquet` |
| 산식 | `research/etl/features/filing_activity.py:122`, `:136` |
| 대체 설계 근거 | 같은 파일 모듈 docstring |
| 등급 규칙 | `research/analysis/horizon_scan_config.yaml`의 `evidence_grade`, `decision` |
| C3 빈 칸 지목 | `01_feature_candidate/11_feature_taxonomy.md` §2.1 |
