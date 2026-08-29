# 22. `fin_sue` — 실적 서프라이즈 (SUE)

- 작성일: 2026-08-29
- family: `fin_sue` · primary feature: 동명 · domain: event (실적)
- **Phase B** · fdr_family `event` · 기대 부호 `+` · **관측 부호 없음**
- **status `insufficient` — 6개 cell 전부 평가 불가** · 등급 **C** · source quality `warn`
- 공통 기준과 용어는 [00_읽는_법.md](00_읽는_법.md)를 먼저 본다

---

## 1. 한 줄 요약

**계산하려 했는데 표본이 없어서 평가하지 못했다.** 6개 cell 전부 `status = insufficient`,
`status_reason = no_formation_rows`, IC·q5·q 전부 `NaN`이다.

**신호가 없다는 뜻이 아니다.** 데이터가 없다는 뜻이다.

| 항목 | 값 |
|---|---|
| `readiness` | **ready** (의존성은 다 준비됨) |
| `ready_cells` | 6 / 6 |
| `evaluated_cells` | 6 |
| **`observations`** | **0** |
| **`coverage_ratio`** | **0.00000** |
| **`effective_start`** | **2025-05-02** |
| `q_fdr_phase_b_min` | 1.00000 |

`effective_start`가 **2025-05-02**다. 그런데 `common_survivor` 표본은 120일 라벨이 필요해
formation이 **2025-02-05**에서 끝난다. **표본 구간과 데이터 구간이 겹치지 않는다.**

**35개 중 유일하게 `insufficient`인 family이고, 보고서의 "insufficient 6 cell"이 전부
이 family다.**

---

## 2. `ready`인데 `insufficient`인 이유 — 두 상태는 다르다

**이 문서에서 가장 중요한 구분이다.**

| 상태 | 뜻 | 이 family |
|---|---|---|
| `ready` | feature·label **의존성이 준비된** 상태 | **예** (6/6) |
| `evaluated` | 계산을 **시도한** 상태 | **예** (6개) |
| `insufficient` | formation row가 없어 **IC를 못 만든** 상태 | **예** (6개) |
| `valid` | IC가 실제로 나온 상태 | 아니오 |

보고서 설계 문서가 이 구분을 못 박았다.

> `ready` — feature·label dependency가 준비된 상태. **IC가 실제 계산됐다는 뜻은 아니다.**
> `insufficient` — formation row가 없어 IC를 계산하지 못한 상태.
> — `13_feature_performance_html_report_plan.md` §6.2

그래서 보고서는 이렇게 센다.

```
Phase B 78개 cell = readiness 전부 통과
  그중 fin_sue 6개는 no_formation_rows
→ evaluable cell 은 72개
```

**78과 72를 섞으면 안 된다.**

그리고 `—`와 `0`도 다르다. Phase B 구현 로그가 규칙을 적었다.

> 평가 셀이 0이면 discovery·screen_pass 건수도 `0`이 아니라 `—`다 —
> **"재보니 0"과 "안 쟀다"는 다른 사실이다.**
> — `08_phase_b_implementation_log.md`

---

## 3. 무엇을 재려 했나 — 산식 정본

### 3.1 정의

```sql
-- research/etl/features/sue_event.py:252
CASE WHEN ewf.history_count >= 8 AND ewf.history_stddev > 0
     THEN ewf.seasonal_change / ewf.history_stddev
END AS fin_sue
```

**분기 EPS의 전년 동기 대비 변화를, 그 변화의 과거 표준편차로 나눈 값**이다.
표준화된 예상외 이익(Standardized Unexpected Earnings)이다.

- `seasonal_change` = 이번 분기 EPS − 4분기 전 EPS
- `history_stddev` = 그 계절 변화의 과거 표준편차
- **과거 이력이 8분기 이상**(`MIN_SUE_HISTORY = 8`)이어야 계산한다

값이 +2면 "평소 변동폭의 두 배만큼 좋았다"는 뜻이다.

### 3.2 Phase B에서 유일한 event-time 마트다

```
grain: (ticker, original_rcept_no, event_formation_date, market)
```

모듈 docstring이 설명한다.

> This is the one Phase B mart that is **event-time, not daily-continuous**:
> every other B-3..B-5 mart broadcasts a PIT value across every trading day
> until the next one supersedes it; this mart instead measures the market's
> reaction to **one specific announcement** over a fixed 60-session window
> after it.

다른 피처는 "오늘 이 종목의 값은 얼마인가"를 매일 답하지만, 이건 **"이 실적 발표 이후
60거래일 동안 어떻게 됐나"**를 발표 건마다 답한다.

그래서 cell 구조도 다르다.

```yaml
# horizon_scan_config.yaml:490
primary_horizon_set: []          # ← 비어 있다
exploratory_horizon_set: []
include_bucket_primary: false
event_buckets: [[0,3], [3,5], [5,10], [10,20], [20,40], [40,60]]
```

`cell_type`이 다른 family의 `cumulative`/`bucket`이 아니라 **`event_bucket`**이다.
발표일로부터 0~3일, 3~5일, …, 40~60일 여섯 구간이다.

### 3.3 원본 이벤트만 센다

동일 실적을 여러 번 공시하면 **원본 하나만** 이벤트로 잡는다. B-2의
`captured_vintage_status`/`is_revision`으로 판정하고, 같은 날 정정은
`same_day_effective_rcept_no`로 합친다.

docstring이 규율을 적었다.

> an event only exists here when B-2 could actually confirm it, never from a
> bare captured rcept_no (§3.5: "raw에 우연히 남은 최소 rcept_no를 original로
> 간주하지 않는다")

### 3.4 비교 EPS는 대체 방법을 쓴다 — 의도적 축소

**정직하게 기록된 한계다.**

> §4.6's *primary* method reconstructs comparative weighted-average shares from
> a separate "prior-year" XBRL duration context inside the *same* filing.
> **This repository does not parse multiple XBRL contexts per concept per
> filing**, so that reconstruction is not buildable without new XBRL-parsing
> work. This module instead always uses what §4.6 itself defines as the
> secondary `as_was_comparative` method … labeled `comparative_policy='as_was_lag4q'`
> in the output **so nothing claims to be the primary … figure it is not.**

즉 같은 공시 안의 전년 동기 수치를 읽는 게 원래 방법인데, XBRL 다중 컨텍스트 파싱이 없어
**4분기 전에 실제로 보고됐던 값**을 대신 쓴다. 산출물에
`comparative_policy = 'as_was_lag4q'`로 표시된다.

### 3.5 PIT

`event_formation_date`가 B-2의 `available_from`이고, 이미 "공시 다음 KRX 세션"이다.
정본 변형은 **`lag1`**이다.

### 3.6 코드 위치

| 대상 | 경로 |
|---|---|
| 산식 | `research/etl/features/sue_event.py:252` |
| 이벤트 정의·한계 | 같은 파일 모듈 docstring |
| 최소 이력 상수 | `research/etl/features/sue_event.py:68` |
| 사전등록 | `research/analysis/horizon_scan_config.yaml:485` |

---

## 4. 왜 예측한다고 봤나 — 가설

### 4.1 메커니즘

**실적 발표 후 표류(PEAD, Post-Earnings-Announcement Drift)다.**

시장이 실적 서프라이즈에 즉시 반응하지 않고 몇 주에 걸쳐 천천히 반영한다는 관찰이다.
Ball & Brown (1968) 이래 가장 오래되고 가장 널리 복제된 이례현상 중 하나다.

**과소반응 가설**이다. 좋은 실적이 나오면 그날 오르긴 하는데 충분히 오르지 않고, 이후
몇 주 동안 계속 오른다.

### 4.2 기대 부호

`+`. 서프라이즈가 클수록 이후 초과수익률이 높다.

### 4.3 사전등록 — event bucket 여섯

```yaml
event_buckets: [[0,3], [3,5], [5,10], [10,20], [20,40], [40,60]]
```

**앞이 촘촘하고 뒤가 성기다.** PEAD가 발표 직후에 가장 강하고 점차 약해진다는 예측을 격자
자체에 담았다.

| | 사전등록 | 실제 결과 |
|---|---|---|
| cell | event bucket 6개 | **6개 전부 `insufficient`** |
| 부호 | `+` | **관측 없음** |

### 4.4 한국 시장 단서

`02_feature_candidate.md` §1의 12번 항목이다.

> 실적 서프라이즈/PEAD | `fin_sue`, `fin_earnings_drift` | Q6 | 서프라이즈 방향 |
> **A-KR** | R1

`A-KR`은 한국에서도 확인된 이례현상이라는 표시다. **기대가 컸던 축이다.**

`fin_earnings_drift`는 만들지 않았다.

분류 좌표는 C2(재무 기반 상태) × **T2(놀라움)** × U다.

### 4.5 근거 문헌

Ball & Brown (1968), Bernard & Thomas (1989).

---

## 5. 왜 표본이 없나 — 원인

### 5.1 XBRL 백필이 끝나지 않았다

Phase B 구현 로그가 원인을 지목했다.

> **커버리지가 얇은 family 둘.** `fin_sue`는 effective start 2025-05-02에 coverage 0.0000이고
> **(B-1 6항 receipt-targeted XBRL 백필이 필요한 그 지점)** … 둘 다 q가 1.0 / 0.42로 나온
> family와 일치한다 — **신호가 없다기보다 표본이 없다.**
> — `08_phase_b_implementation_log.md`

SUE를 만들려면 §3.1의 조건이 필요하다.

1. 분기별 `controlling_net_income`과 `weighted_avg_shares`
2. 그 값들의 **4분기 전 vintage**(`value_lag_4q`)
3. **과거 8분기 이상의 이력**(`MIN_SUE_HISTORY = 8`)

세 번째 조건이 결정적이다. **8분기 = 2년치 분기 실적이 XBRL로 다 있어야 한 건의 SUE가
나온다.** XBRL 수집이 최근 구간에 몰려 있으면 그 조건을 만족하는 이벤트가 최근에만 생긴다.

`effective_start = 2025-05-02`가 그 결과다.

### 5.2 표본 구간과 겹치지 않는다

```
fin_sue 데이터:        2025-05-02 ~ (그 이후)
common_survivor 표본:  2014-06-01 ~ 2025-02-05
                                    ↑ 120일 라벨 때문에 여기서 끝
```

**두 구간이 겹치지 않는다.** 그래서 formation row가 0이고 `no_formation_rows`가 된다.

`observations = 0`, `coverage_ratio = 0.0`이 이를 그대로 보여 준다.

### 5.3 해결 경로는 이미 알려져 있다

CLI에 전용 명령이 있다.

```
dart backfill-xbrl-receipts --targets-file <파일>
```

프로젝트 문서가 이 명령을 **"수동으로 남긴다"**고 적었다 — 어느 접수를 다시 받을지 고르는
게 별도 분석이기 때문이다 (`CLAUDE.md`).

**즉 코드 문제가 아니라 수집 범위 문제다.**

---

## 6. 판정은 어떻게 기록됐나

### 6.1 AB cell 전체

| scan | bucket | status | IC | AB q | discovery | screen | grade | failed gates |
|---|---|---|---|---:|---|---|---|---|
| event | 0→3 | **insufficient** | — | 1.000 | False | False | **C** | `primary_discovery`, `tradable_pass`, `period_sign_pass`, `robustness_pass` |
| event | 3→5 | insufficient | — | 1.000 | False | False | C | 동일 |
| event | 5→10 | insufficient | — | 1.000 | False | False | C | 동일 |
| event | 10→20 | insufficient | — | 1.000 | False | False | C | 동일 |
| event | 20→40 | insufficient | — | 1.000 | False | False | C | 동일 |
| event | 40→60 | insufficient | — | 1.000 | False | False | C | 동일 |

**`failed_gates`에 게이트 이름이 네 개 적혀 있지만 실제로 실패한 게 아니다.** 계산할 값이
없으니 모든 게이트가 자동으로 미통과 처리된 것이다.

`q_fdr_global_ab = 1.0`도 마찬가지다. BH는 p값이 없는 항목에 1.0을 넣는다
(`stats.bh_missing_p_value: 1.0`). **"전혀 유의하지 않다"가 아니라 "p값이 없다"는 뜻이다.**

### 6.2 등급 C의 의미

Phase A와 Phase B의 C 정의가 다르다는 점을 먼저 짚어야 한다
([00_읽는_법.md](00_읽는_법.md) §4.5).

- Phase A의 C: 탐색·보조이거나 available 부호 뒤집힘
- Phase B의 C: 강건성 또는 availability 방향 게이트 실패

**이 family의 C는 둘 중 어느 쪽도 아니다.** 계산 자체를 못 해서 기본값으로 떨어진 등급이다.

### 6.3 source quality — `warn`

| 항목 | 값 |
|---|---|
| `source_quality_status` | `warn` |
| `source_quality_reasons` | `revision` |
| `revision_ratio` | **0.1270** |
| `revision_worst_metric` | `weighted_avg_shares` |
| `mapping_fallback_ratio` | 0.3424 (`controlling_net_income`) |

**가중평균주식수의 12.7%가 사후 정정됐다.** 그리고 지배주주순이익의 34.2%가 매핑 대체
경로로 채워졌다.

**표본이 생겼을 때도 이 경고는 남는다.** 등급 상한 자체는 없지만(`grade_cap: None`) A는
받기 어렵다.

---

## 7. 표본과 커버리지

| 항목 | 값 |
|---|---|
| `effective_start` | **2025-05-02** |
| `coverage_ratio` | **0.00000** |
| `observations` | **0** |
| 평가 가능 cell | **0 / 6** |

커버리지 출처도 다르다. Phase B 구현 로그가 적었다.

> `fin_sue`는 **grain이 달라 `event_coverage`에서 커버리지를 가져오고**, 연속 패널 개념인
> `min_names_per_date`는 비슷한 숫자를 빌려 오지 않고 비운다.

다른 17개 family가 `feature_coverage.parquet`을 쓰는데 이 family만
`event_coverage.parquet`을 쓴다. **비슷해 보이는 숫자를 빌려 오지 않는다**는 규율이다.

---

## 8. 중복성

**A×B 상관 산출물에 없다.** `top_rank_correlation_pair`가 `None`이다. 값이 없으니 상관도
계산할 수 없다.

### 확인하지 않은 중복

표본이 생기면 확인해야 할 것들이다.

- [25_fin_gross_profitability.md](25_fin_gross_profitability.md), [27_fin_value_z.md](27_fin_value_z.md)와
  같은 분기 재무 vintage를 원천으로 쓴다. 특히 `controlling_net_income`을 공유한다.
- `ev_amendment_ratio`·`ev_filing_activity`와 같은 `dart_filing_receipt_raw`를 쓴다
  (원본 이벤트 판정에).

---

## 9. 한계와 확인 못 한 것

1. **표본이 없어 아무것도 평가하지 못했다** (§5). 이 family에 대해 말할 수 있는 건 여기까지다.
2. **XBRL 백필이 필요하다** (§5.3). `dart backfill-xbrl-receipts`로 어느 접수를 다시 받을지
   고르는 분석이 선행돼야 한다.
3. **비교 EPS가 대체 방법이다** (§3.4). XBRL 다중 컨텍스트 파싱이 없어
   `as_was_lag4q`를 쓴다. 표본이 생겨도 이 한계는 남는다.
4. **가중평균주식수의 12.7%가 사후 정정됐다** (§6.3).
5. **`fin_earnings_drift`를 만들지 않았다** (§4.4). 같은 축의 짝인데 없다.
6. **8분기 이력 요구가 커버리지를 크게 제한한다** (§5.1). 신규 상장사는 2년간 값이 없다.
7. **holdout을 열지 않았다.**

---

## 10. 모델에서는 어땠나

**T2 14-feature bundle에 안 들어갔다.** 값이 없으니 당연하다.

T2 후보 14개는 전부 값이 있는 family다.

---

## 11. 다음에 할 일

이 family는 **"안 됐다"가 아니라 "아직 못 쟀다"**로 분류해야 한다. 순서는 이렇다.

1. `dart backfill-xbrl-receipts`의 대상 접수를 고른다 — 8분기 이력을 만들려면 어느 구간의
   XBRL이 필요한지 역산한다.
2. 백필 후 `feat_fin_scan_daily`·`fin_sue_event`를 다시 만든다.
3. `event_coverage.parquet`에서 `effective_start`가 표본 구간 안으로 들어왔는지 확인한다.
4. **같은 사전등록(event bucket 6개, 기대 부호 `+`)으로 다시 돌린다.** 결과를 보고 격자를
   바꾸면 사전등록이 무의미해진다.

---

## 12. 원본 추적

```bash
cd "$(git rev-parse --show-toplevel)"
uv run --extra analysis python - <<'PY'
import duckdb
CFG="889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea"
B=f"research/output/horizon_scan/phase=B/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T123313-4e0ae8b0"
AB=f"research/output/horizon_scan/phase=AB/snapshot_date=2026-08-23/source=sj2_remote/config_hash={CFG}/run_id=20260828T165038-4e0ae8b0"
# insufficient 6 cell — IC 는 NULL 이고 q 는 1.0 이다
print(duckdb.sql(f"""
  select feature, cell_type, h_start, h_end, status, ic_mean,
         q_fdr_global_ab, evidence_grade, failed_gates
  from '{AB}/combined_ab_primary_hypotheses.parquet'
  where family='fin_sue' order by h_start
""").df().to_string())
# 커버리지는 event_coverage 에서 온다
print(duckdb.sql(f"select * from '{B}/core/event_coverage.parquet' limit 20").df().to_string())
PY
```

| 항목 | 위치 |
|---|---|
| **최종 판정** | `phase=AB/…/run_id=20260828T165038-4e0ae8b0/combined_ab_primary_hypotheses.parquet` |
| 이벤트 커버리지 | `phase=B/…/run_id=20260828T123313-4e0ae8b0/core/event_coverage.parquet` |
| 이벤트 IC (빈 결과) | 같은 B run의 `core/event_ic.parquet` |
| 원천 품질 | 같은 B run의 `core/quarterly_metric_quality.parquet` |
| 산식 | `research/etl/features/sue_event.py:252` |
| 설계 의도·한계 | 같은 파일 모듈 docstring |
| 커버리지 원인 진단 | `01_feature_candidate/08_phase_b_implementation_log.md` §3.0 |
| `ready`/`insufficient` 구분 | `01_feature_candidate/13_feature_performance_html_report_plan.md` §6.2 |
