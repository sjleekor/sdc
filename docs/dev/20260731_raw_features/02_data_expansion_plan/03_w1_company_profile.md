# 03. N2 — 업종 코드 · 설립일 · 결산월 (1차)

- 작성일: 2026-08-15
- 공통 규약: [`01_implementation_checklist.md`](01_implementation_checklist.md)
- 원천: OpenDART `company.json` (기업개황, DS001) · 기존 `dart_corp_master` 확장
- 예상 규모: 호출 **약 3,959회(ticker 매핑 법인 전체) · 단일 실행으로 끝난다.** 이 계획에서 비용이 가장 낮다

---

## 1. 왜

[`11_feature_taxonomy.md`](../01_feature_candidate/11_feature_taxonomy.md)의 1순위다.
업계·학계 표준 모델이 공통으로 두는 **industry 블록이 통째로 없다** — Barra는 country·**industry**·
style 3블록이고 Gu, Kelly & Xiu (2020)는 94 characteristic에 **74개 industry dummy**를 붙인다.

현재 `fin_scan.py`의 횡단 정규화가 전부 `PARTITION BY trade_date, market`이다.
**은행·바이오·조선·게임이 같은 KOSPI 풀에서 z-score를 받는다.**

이게 지금 결과를 흐리는 지점 넷은 `11_feature_taxonomy.md` §6.2에 있다. 요약하면
`fin_value_z`가 업종 더미를 재고 있을 가능성, `fin_gross_profitability`가 금융업에서 개념
미성립, **`fin_accruals_to_assets` 부호 반전의 세 번째 후보**, `ev_payout_yield`의 업종 편중이다.

**새 피쳐를 하나도 안 만들고 기존 결과의 해석을 바꾼다.** 그래서 1순위다.

---

## 2. 원천 확인 결과

**지금 `dart_corp_master`로는 안 된다.** `adapters/opendart_corp/provider.py`를 확인했다 —
이건 `corpCode.xml` **벌크 zip**을 파싱하고 `corp_code / corp_name / stock_code / modify_date`
4개만 꺼낸다. 업종이 없다.

`company.json`(기업개황)은 **corp_code당 1회 호출하는 별도 API**다.
[OpenDART 개발가이드 DS001/2019002](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019002)에서
응답 필드를 확인했다.

| 필드 | 내용 | 쓸 것인가 |
|---|---|---|
| **`induty_code`** | 업종코드 | **○ 주 목적** |
| **`est_dt`** | 설립일 (YYYYMMDD) | **○ 기업 연령(firm age) — 알려진 predictor** |
| **`acc_mt`** | 결산월 (MM) | **○ 12월 결산 아닌 기업 식별** |
| `corp_cls` | Y(유가)/K(코스닥)/N(코넥스)/E(기타) | ○ 시장 구분 교차검증 |
| `ceo_nm`, `adres`, `jurir_no`, `bizr_no`, `hm_url`, `ir_url`, `phn_no`, `fax_no`, `corp_name_eng` | 기타 | ✗ 지금은 안 쓴다 |

**`acc_mt`는 부수 소득이 아니라 실제 결함을 드러낸다.** 확인해보니 마트가 **결산월을 12월로
하드코딩**하고 있다.

```sql
-- research/etl/marts/metric_vintages.py — _calendar_period_end_expr
CASE reprt_code
  WHEN '11013' THEN make_date(year, 3, 31)
  WHEN '11012' THEN make_date(year, 6, 30)
  WHEN '11014' THEN make_date(year, 9, 30)
  WHEN '11011' THEN make_date(year, 12, 31)
END
```

즉 3월 결산 기업의 1분기 보고서도 `period_end = 3/31`로 잡힌다. 실제로는 6/30이다.
`02_feature_candidate.md` §2.2의 분기 standalone 계산(`Q2=half−Q1` 등)과 YoY 비교, TTM이
전부 어긋난다. **지금 이걸 구분하는 컬럼이 없어서 문제가 있는지조차 몰랐다.**

> **따라서 `acc_mt`는 "영향 범위 조사"로 끝나지 않는다.** 비12월 결산 기업 수가 유의미하면
> `metric_vintages.py`의 period_end 산출을 `acc_mt` 기반으로 고치는 **실제 수정 작업**이
> 뒤따른다. §7 V4가 그 판단 근거다.

---

## 3. 스키마

새 테이블을 만들지 않고 **`dart_corp_master`를 확장**한다. 키가 같고(`corp_code`) 성격도 같다.

```sql
-- sql/postgres_ddl.sql — dart_corp_master 아래 ALTER 블록에 추가
ALTER TABLE dart_corp_master
    ADD COLUMN IF NOT EXISTS induty_code        TEXT,
    ADD COLUMN IF NOT EXISTS corp_cls           TEXT,
    ADD COLUMN IF NOT EXISTS est_dt             DATE,
    ADD COLUMN IF NOT EXISTS acc_mt             TEXT,
    ADD COLUMN IF NOT EXISTS profile_raw        JSONB,
    ADD COLUMN IF NOT EXISTS profile_fetched_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS ix_dart_corp_master_induty
    ON dart_corp_master (induty_code);
```

**`profile_fetched_at`이 skip-if-present 키다.** NULL이면 아직 안 받은 것이고, 값이 있으면
건너뛴다. `--force`나 `--refresh-older-than`으로 다시 받을 수 있게 한다.

**`profile_raw` JSONB를 남기는 이유**는 지금 안 쓰기로 한 필드(대표자·주소·법인번호)를 나중에
쓸 수 있게 하기 위해서다. 재수집 비용이 2,700 호출이라 크지 않지만, 원본 보존이 이 저장소의
raw 레이어 원칙과 맞는다.

### 등록 — 새 테이블이 아니라 컬럼 추가다

새 테이블이 아니라 컬럼 추가라서 등록 범위가 줄어든다.

| # | 파일 | 할 일 |
|---|---|---|
| 1 | `sql/postgres_ddl.sql` | 위 `ALTER` |
| 2 | `infra/db_postgres/remote_sync.py` | `dart_corp_master`의 `SYNC_TABLE_SPECS` **컬럼 목록에 신규 6개 추가** |
| 3 | `service/profiling/catalog.py` | `DART_CORP_MASTER` spec — `category_cols`에 `induty_code`·`corp_cls`, `null_cols`에 `est_dt`·`acc_mt` 추가 |
| 4 | `tools/raw-parquet-exporter/config/export_tables.toml` | `dart_corp_master`에 `jsonb_columns = ["profile_raw"]` **추가** |
| — | `docs/database.md` | 컬럼 설명 갱신 |

`bin/...sh`와 `research/etl/config.py`는 `dart_corp_master`가 이미 등록돼 있어 손댈 게 없다.

**2·3·4번을 빠뜨리기 쉽다.** 컬럼만 늘리는 작업이라 "등록은 이미 돼 있다"고 넘기기 쉬운데,

- `remote_sync`의 SELECT/INSERT 컬럼 목록은 명시적이라 **추가하지 않으면 미러링에서 조용히
  누락된다.** 새 테이블과 달리 **테스트가 잡아주지 않는다**
- profiling catalog의 `category_cols`/`null_cols`에 없으면 프로파일에 안 잡힌다
- JSONB를 선언하지 않으면 문자열로 export된다

---

## 4. 작업 순서

### N2-PR1 — PoC (호출 20건)

```bash
# 대표 종목 몇 개로 응답 확인
# 삼성전자(005930), KB금융(105560), 셀트리온(068270), HMM(011200), 크래프톤(259960)
```

**확인할 것.**

- **`induty_code` 실제 자릿수 분포** — 전부 3자리인가, 길이가 섞이는가 (§5의 결정에 직결)
- **2자리 prefix로 잘랐을 때 그룹 수와 그룹당 종목 수** — §5 표의 가정 검증
- 결측률 — 상장사 중 업종이 비어 있는 비율
- `acc_mt != '12'` 사례가 실제로 나오는가 (→ `metric_vintages.py` 수정 여부)
- `est_dt` 포맷과 결측
- 지주회사·금융지주가 어떤 코드를 받는가 (§5)

산출물: `poc/n2_company_profile.md`.

### N2-PR2 — 스키마 + 등록

§3 그대로. 빈 컬럼 상태로 `db init` → `db sync-remote` → export가 통과해야 한다.

### N2-PR3 — 어댑터 + 포트

```
ports/corp_codes.py         CorpProfileProvider(Protocol)
                              fetch_company_profile(corp) -> CompanyProfileResult
adapters/opendart_corp/provider.py   기존 파일에 메서드 추가 (같은 어댑터 안)
domain/models.py            CompanyProfile, CompanyProfileSyncResult
domain/enums.py             RunType.DART_CORP_PROFILE_SYNC = "dart_corp_profile_sync"
```

`opendart_common`의 다중키 실행기를 쓴다 — `01` §2.2 그대로.

### N2-PR4 — 서비스 + CLI

```
service/sync_dart_corp_profile.py
cli/app.py:  dart sync-corp-profile [--tickers] [--force] [--rate-limit-seconds]
```

대상은 `storage.get_dart_corp_master(active_only=True)` 중 **ticker 매핑이 있는 것만**이다
(인벤토리 §2.7 기준 116,503 법인 중 3,959건). 비상장 법인까지 받으면 호출이 30배로 뛴다.

### N2-PR5 — 테스트

- `tests/unit/test_dart_corp_profile.py` — 파싱, `profile_fetched_at` skip, 부분 실패 → `partial`
- 픽스처는 **결측 필드가 있는 응답, `acc_mt='03'`인 응답, 업종 없는 응답**을 포함한다

### N2-PR6 — 실행

```bash
uv run krx-collector dart sync-corp-profile
```

키 1개(일 20,000)로도 단일 실행에서 끝난다.

### N2-PR7 — 업종 그룹 정의와 마트 반영

**여기가 실제 값이 나오는 단계다.** 수집만으로는 아무것도 안 바뀐다.

1. `definitions/`에 **업종 그룹 매핑**을 순수 코드로 추가한다
   (`definitions/industry_groups.py`). `metric_rules.py`·`common_features.py`와 같은 자리다 —
   마트가 import하고 Postgres 테이블이 아니다.
2. `research/etl/features/fin_scan.py`의 z-score 윈도에 `industry_group`을 추가한 **variant**를
   만든다. 기존 경로를 덮어쓰지 않는다.
3. 기존 셀과 나란히 놓고 비교한다.

---

## 5. 결과 보기 전에 고정할 것 — 업종 분류 깊이

**이게 이 작업 패키지에서 유일하게 어려운 결정이다.**

`induty_code`는 **KSIC(한국표준산업분류)** 코드다. 삼성전자가 `264`처럼 **3자리**로 온다.
KSIC 제11차 기준 계층은 이렇다.

| 계층 | 자릿수 | 분류 수 | 상장 2,700개 기준 그룹당 |
|---|---|---:|---|
| 대분류 | 알파벳 | 21 | 130 |
| **중분류** | **2자리** | **77** | **35** |
| 소분류 | **3자리** | **234** | **12** ← 정규화가 망가진다 |
| 세분류 | 4자리 | 495 | 5 이하 |

**처음 계획이 여기서 틀렸다.** "3자리 중분류 30~60개"라고 적었는데, 3자리는 **소분류이고 234개**다.
그대로 쓰면 그룹당 12종목이라 횡단 z-score가 성립하지 않는다.

참고로 Barra는 GICS 45개 산업 팩터를 쓴다. **목표는 30~50개 그룹**이고, KSIC로는 어느
한 자릿수로 딱 떨어지지 않는다. 그래서 **매핑을 명시적으로 만들어야 한다.** 세 안 중 하나다.

| 안 | 방법 | 장단 |
|---|---|---|
| **A** | **2자리 prefix + 소수 업종 병합** | 77 → 병합 후 40~50. 가장 단순하고 KSIC 체계를 유지 |
| B | 2자리 prefix + **금융·지주 override** | A에 더해 금융업만 세분(은행/증권/보험 분리) |
| C | GICS 유사 30~50개 custom mapping | 문헌 비교가 쉽지만 매핑 설계 비용이 크다 |

**권고는 B다.** 금융·지주는 계정 구조가 달라 `fin_gross_profitability`·`fin_accruals`의
해석을 흐리는 주범이므로(§1), 거기만 세분할 값이 있다.

**규율상 반드시 지킬 것.**

> 깊이·병합 규칙을 여러 개 만들어 놓고 제일 잘 나오는 걸 고르면 **window grid search와 같은
> 위반**이다(`02_feature_candidate.md` §6.2). **PoC에서 실제 코드 분포를 본 뒤 매핑 하나를
> 고정**하고, 그 결정과 근거를 이 문서에 적은 다음 마트를 돌린다.

출처: [통계청 한국표준산업분류 체계](https://kostat.go.kr/boardDownload.es?bid=246&list_no=428849&seq=9)

**보조 규칙 두 개도 같이 고정한다.**

- **최소 그룹 크기**: 그룹 내 종목이 N개 미만인 날은 그 그룹을 상위 분류로 합친다.
  N을 미리 정한다(권고: 20).
- **금융·지주 취급**: 별도 그룹으로 둘지 아예 제외할지. `02` §3.4가 "섹터 더미 또는 제외"라고만
  적고 미이행 상태다. **권고: 제외가 아니라 별도 그룹.** 제외하면 `fin_accruals` 부호 반전의
  원인 규명이 안 된다 — 금융을 뺐더니 부호가 돌아오는지가 바로 그 검증이다.

---

## 6. PIT 한계 — 선을 먼저 긋는다

`company.json`은 **현재 시점 업종만** 준다. 과거 업종 변경 이력이 없다.

**처음 계획은 여기를 안이하게 적었다.** "업종을 alpha 피쳐로 쓰지 않으면 룩어헤드가 아니다"는
**틀린 논증이다.** 업종을 정규화 그룹으로만 써도, **과거 시점의 z-score 값 자체가 미래에
확정된 업종 분류로 계산된다.** 즉 피쳐 값에 미래 정보가 들어간다. 이름을 어디에 쓰느냐와
무관하다.

> **N2로 만든 업종 중립 값은 진단 지표까지만 쓴다.**
> **scored backtest·acceptance gate·holdout에는 넣지 않는다.**

허용되는 것과 금지되는 것을 구분해 적는다.

| 쓸 수 있다 (진단) | 쓸 수 없다 (scored) |
|---|---|
| 업종별 `fin_value_z` 중앙값 분산 (§7 V6) | 업종 중립 피쳐로 walk-forward OOS |
| 금융·지주 제외 시 `fin_accruals` 부호가 돌아오는지 | acceptance gate 증분성 판정 |
| 업종별 조건부 IC — **"어디가 문제인가"를 보는 용도** | holdout 평가 |
| coverage·결측 진단 | 채택 결론 |

**정식 업종 중립 variant는 [N4](05_w2_industry_index.md)가 역사적 업종 경로를 확인한 뒤에
만든다.** N4는 그 시점의 실제 지수 소속이라 룩어헤드가 없다.

이 제한이 N2의 가치를 없애지는 않는다. §1에서 든 네 가지(`fin_value_z` 정체,
`fin_gross_profitability` 무신호, `fin_accruals` 부호 반전, `ev_payout_yield` 편중)는
**전부 진단 질문**이라 이 범위 안에서 답이 나온다.

---

## 7. 수집 후 검증

| # | 검증 | 기대 | 어긋나면 |
|---|---|---|---|
| V1 | `induty_code` 결측률 | < 2% | 결측 종목의 처리 정책 필요 |
| V2 | 그룹별 종목 수 분포 (선택한 깊이 기준) | 그룹당 20 이상 | 깊이 재검토 — §5 |
| V3 | `corp_cls` vs `stock_master.market` 불일치 | 0 | 매핑 오류 또는 시장 이전 종목 |
| V4 | `acc_mt != '12'` 종목 수와 시총 비중 | — | **`metric_vintages.py` period_end 하드코딩 수정 여부 판단**(§2) |
| V5 | `est_dt` 결측률·이상치(미래 날짜 등) | — | firm age 피쳐 사용 가능 여부 |
| V6 | 업종별 `fin_value_z` 중앙값 분산 | — | **분산이 크면 §1의 ① 가설이 확인된다** |

V6가 이 작업의 핵심 산출물이다. **업종별 밸류 중앙값이 크게 다르면, 지금 `fin_value_z`가
"싼 종목"이 아니라 "원래 B/M이 높은 업종"을 뽑고 있었다는 직접 증거**가 된다.

---

## 8. 완료 기준

공통 DoD(`01` §7)에 더해:

- [ ] `poc/n2_company_profile.md`에 업종 코드 **실제 자릿수 분포**와 2자리 prefix 그룹 수
- [ ] **§5의 매핑안(A/B/C)·최소 그룹 크기·금융 취급 세 결정이 결과 보기 전에 문서에 고정**
- [ ] `definitions/industry_groups.py` — 순수 코드, Storage 의존 없음
- [ ] 검증 V1~V6 실측치 기록
- [ ] §6의 PIT 한계 명시 + **scored backtest 금지선이 문서에 남았는가**
- [ ] `fin_scan.py`에 업종 중립 **variant** 추가 (기존 경로 유지, 진단 전용)
- [ ] **`acc_mt` 후속 판단 기록**(§2) — 비12월 결산 기업 수를 근거로 `metric_vintages.py`
      period_end 수정이 필요한지. 필요하면 별도 작업 문서 생성
