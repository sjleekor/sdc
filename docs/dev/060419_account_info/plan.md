# 계정/재무/수급/사업지표 수집 구현 계획

## 목적

현재 파이프라인은 `종목 유니버스`와 `일봉 OHLCV`만 수집합니다. 다음 단계에서는 개별 종목의 재무 raw 값, 주식수 관련 raw 값, 재무상태표/현금흐름표 raw 값, 주주환원 raw 값, 수급 raw 값, 사업/운영 KPI를 가능한 한 많이 수집할 수 있는 구조를 추가합니다.

이 문서의 목표는 다음 세 가지입니다.

1. 어떤 데이터 소스를 우선 채택할지 결정한다.
2. 현재 레포의 포트/어댑터 구조에 맞는 순차 구현 순서를 정한다.
3. 각 데이터군별 커버리지, 한계, 후속 확장 전략을 명확히 남긴다.

## 결론 요약

가장 현실적인 기본 전략은 다음과 같습니다.

- `OpenDART`를 재무/주식수/배당/자사주 관련 원천 소스로 사용한다.
- `KRX 정보데이터시스템` 및 `pykrx`를 일별 수급 데이터 소스로 사용한다.
- `사업/운영 KPI`는 표준 API로 해결하지 않고, 공시 원문 및 IR 자료 파서 계층으로 분리한다.

즉, 구현 우선순위는 아래와 같습니다.

1. `OpenDART 재무/주식수/배당/자사주`
2. `KRX/pykrx 수급`
3. `OpenDART XBRL 세부 계정 확장`
4. `비정형 사업 KPI 파서`

## 데이터 소스 전략

### 1. OpenDART

가장 먼저 붙여야 하는 공식 소스입니다.

주요 사용 대상:

- 기업 실적 raw 값
- 재무상태표 raw 값
- 현금흐름표 raw 값
- 발행주식수 / 자기주식수
- DPS / 총배당금
- 자사주 취득/처분/소각 관련 값

핵심 API 범주:

- `fnlttSinglAcntAll`
  - 단일회사 전체 재무제표 계정
  - BS / IS / CIS / CF / SCE 계정 수집의 중심
- `fnlttXbrl`
  - XBRL 원문
  - 표준 API에 직접 노출되지 않는 세부 계정, 주석, 확장 taxonomy 보강용
- `stockTotqySttus`
  - 발행주식수, 자기주식수 등 주식 총수 관련
- `alotMatter`
  - 배당 관련 raw
- `tesstkAcqsDspsSttus`
  - 자기주식 취득/처분 현황

### 2. KRX 정보데이터시스템 / pykrx

일별 수급 지표는 이 축이 중심입니다.

주요 사용 대상:

- 외국인 보유주식수
- 외국인 순매수 수량
- 기관 순매수 수량
- 개인 순매수 수량
- 프로그램 순매수 수량
- 공매도 거래량
- 공매도 거래대금
- 공매도 잔고 수량
- 대차잔고 수량

실행 원칙:

- 먼저 `pykrx`로 커버되는 항목을 붙인다.
- `pykrx`가 불안정하거나 누락하는 화면은 KRX 직접 어댑터를 별도로 둔다.

### 3. 사업/운영 KPI

다음 항목은 표준화된 공개 API로 해결되지 않는 경우가 많습니다.

- 수주금액
- 수주잔고
- 출하량
- 판매량
- 생산량
- CAPA
- 가동률
- ASP
- 고객 수
- 가입자 수
- 해지 건수
- 점포 수
- 객단가
- 방문자 수
- MAU
- DAU

따라서 별도 원칙이 필요합니다.

- 범용 API 수집 대상으로 보지 않는다.
- `공시 원문`, `사업보고서`, `IR 자료 PDF`, `실적발표 자료`에서 추출한다.
- 섹터별 파서 레지스트리 방식으로 구현한다.

## 항목별 커버리지 평가

### A. OpenDART만으로 높은 확률로 커버 가능한 항목

다음 항목은 우선 구현 대상으로 본다.

- 매출액
- 매출원가
- 매출총이익
- 판매비와관리비
- 영업이익
- 법인세차감전이익
- 법인세비용
- 당기순이익
- 지배주주순이익
- 비지배주주순이익
- 자산
- 총자산
- 부채
- 총부채
- 자본
- 총자본
- 현금및현금성자산
- 매출채권
- 재고자산
- 유형자산
- 무형자산
- 관계기업투자
- 이연법인세자산
- 매입채무
- 단기차입금
- 유동성장기부채
- 사채
- 장기차입금
- 충당부채
- 리스부채
- 이연법인세부채
- 자본금
- 자본잉여금
- 이익잉여금
- 기타포괄손익누계액
- 자기주식
- 지배주주지분
- 비지배주주지분
- 영업활동현금흐름
- 투자활동현금흐름
- 재무활동현금흐름
- 기초현금
- 기말현금
- 발행주식수
- 자기주식 수
- 주당배당금

### B. OpenDART XBRL까지 가야 커버율이 올라가는 항목

다음 항목은 표준 계정 API만으로는 누락 가능성이 높습니다.

- 영업외수익
- 영업외비용
- 감가상각비
- 무형자산상각비
- 연구개발비
- 광고선전비
- 인건비
- 이자수익
- 이자비용
- 단기금융자산
- 기타유동자산
- 투자자산
- 기타유동부채
- 기타자본항목
- 가중평균주식수
- 희석주식수
- 설비투자금액(CAPEX)
- 배당금 지급액
- 차입금 증가액
- 차입금 상환액
- 주식 발행으로 인한 현금유입
- 자사주 매입금액
- 총배당금
- 자사주 매입 수량
- 자사주 소각 수량

이 항목들은 계정명 표준화가 완전하지 않으므로, `account_id` 중심 적재와 alias 매핑이 필수입니다.

### C. KRX/pykrx 중심 항목

- 외국인 보유주식수
- 외국인 순매수 수량
- 기관 순매수 수량
- 개인 순매수 수량
- 프로그램 순매수 수량
- 공매도 거래량
- 공매도 거래대금
- 공매도 잔고 수량
- 대차잔고 수량

### D. 공개 무료 소스로 안정 수집이 어려운 항목

현재 기준으로 아래 항목은 구현 난도가 높거나, 종목별 표준 API가 불명확합니다.

- 신용매수 잔고
- 일부 사업 KPI 전반

이 항목들은 `후순위`, `부분 지원`, 또는 `유료 벤더 검토` 대상으로 둡니다.

## 구현 원칙

### 원칙 1. 원천 raw 적재와 정규화 계층을 분리한다

직접 `매출액`, `영업이익` 같은 최종 컬럼만 만드는 방식은 피합니다.

먼저 해야 할 일:

- OpenDART 원문 raw 적재
- KRX 수급 raw 적재

그 다음 해야 할 일:

- raw -> canonical metric 매핑
- canonical metric -> 분석용 wide mart 또는 view 구성

이 구조를 택하는 이유:

- 공시 계정명이 분기/연도/기업별로 달라질 수 있음
- 동일 의미 계정의 alias가 많음
- 초기 매핑 정확도가 낮아도 raw 재처리로 복구 가능함

### 원칙 2. 연결/별도를 함께 저장한다

재무 데이터는 반드시 아래 구분을 저장합니다.

- `CFS` 연결
- `OFS` 별도

이 구분이 없으면 아래 값의 의미가 흐려집니다.

- 지배/비지배 관련 값
- 배당 가능 이익 판단
- 주당지표 원재료

### 원칙 3. 계정명보다 계정 ID를 우선한다

가능한 경우 아래 순서로 신뢰합니다.

1. `account_id`
2. `account_nm`
3. 보조 alias 규칙

계정명만으로 매핑하면 회사별 표현 차이에 매우 취약합니다.

### 원칙 4. 사업 KPI는 범용 정규화보다 섹터별 파서를 우선한다

예시:

- 조선/방산: 수주금액, 수주잔고
- 반도체/화학: 생산량, 출하량, CAPA, 가동률
- 통신: 가입자 수, 해지 건수
- 플랫폼/게임: MAU, DAU, 결제지표
- 유통/프랜차이즈: 점포 수, 객단가, 방문자 수

## 현재 코드베이스에 맞춘 목표 아키텍처

현재 구조는 `Ports & Adapters`이므로 아래 포트를 추가하는 방향이 적합합니다.

### 신규 Provider 포트

- `FinancialStatementProvider`
  - OpenDART 재무계정 fetch
- `ShareCountProvider`
  - 발행주식수, 자기주식수, 희석/가중주식수 fetch
- `ShareholderReturnProvider`
  - DPS, 총배당금, 자사주 취득/처분/소각 fetch
- `FlowProvider`
  - 투자자별 순매수, 외국인 보유, 공매도, 대차 데이터 fetch
- `OperatingMetricProvider`
  - 사업 KPI fetch
  - 초기에는 범용 provider보다 parser registry 오케스트레이터에 가깝게 설계

### 신규 Storage 책임

초기에는 raw 중심 테이블을 우선 추가합니다.

- OpenDART 재무 raw 테이블
- OpenDART 주식수 raw 테이블
- OpenDART 배당/자사주 raw 테이블
- KRX 수급 raw 테이블
- metric mapping 테이블
- 정규화 결과 테이블 또는 materialized view

## 권장 DB 설계

초기 설계는 `정규화 결과 테이블`보다 `원천 raw 보관`을 우선합니다.

### 1. `dart_corp_master`

용도:

- KRX ticker와 DART `corp_code` 매핑

주요 컬럼:

- `corp_code`
- `ticker`
- `corp_name`
- `market`
- `is_active`
- `updated_at`

비고:

- OpenDART의 기업고유번호 목록과 현재 `stock_master`를 조인해 유지

### 2. `dart_financial_statement_raw`

용도:

- `fnlttSinglAcntAll` 및 후속 XBRL 파싱 결과 저장

주요 컬럼:

- `corp_code`
- `ticker`
- `bsns_year`
- `reprt_code`
- `fs_div`
- `sj_div`
- `account_id`
- `account_nm`
- `thstrm_amount`
- `frmtrm_amount`
- `bfefrmtrm_amount`
- `currency`
- `rcept_no`
- `source`
- `fetched_at`
- `raw_payload`

유니크 후보:

- `(corp_code, bsns_year, reprt_code, fs_div, sj_div, account_id, rcept_no)`

### 3. `dart_share_count_raw`

용도:

- 주식 총수 현황 raw 저장

주요 컬럼:

- `corp_code`
- `ticker`
- `bsns_year`
- `reprt_code`
- `stock_knd`
- `issued_shares`
- `treasury_shares`
- `distributable_shares`
- `rcept_no`
- `fetched_at`
- `raw_payload`

### 4. `dart_shareholder_return_raw`

용도:

- 배당 / 자기주식 취득 / 자기주식 처분 / 소각 관련 raw 저장

주요 컬럼:

- `corp_code`
- `ticker`
- `bsns_year`
- `reprt_code`
- `statement_type`
- `metric_code`
- `metric_name`
- `value`
- `unit`
- `rcept_no`
- `fetched_at`
- `raw_payload`

### 5. `krx_security_flow_raw`

용도:

- 일자별 수급 raw 저장

주요 컬럼:

- `trade_date`
- `ticker`
- `market`
- `metric_code`
- `metric_name`
- `value`
- `unit`
- `source`
- `fetched_at`
- `raw_payload`

### 6. `metric_catalog`

용도:

- 시스템 내부 canonical metric 정의

예시 metric code:

- `revenue`
- `cogs`
- `gross_profit`
- `sga`
- `operating_income`
- `net_income`
- `issued_shares`
- `weighted_avg_shares`
- `cash_and_cash_equivalents`
- `operating_cash_flow`
- `foreign_net_buy`
- `short_selling_volume`

### 7. `metric_mapping_rule`

용도:

- raw 계정과 canonical metric의 연결 규칙 관리

주요 컬럼:

- `source_system`
- `statement_scope`
- `sj_div`
- `account_id`
- `account_nm_pattern`
- `metric_code`
- `priority`
- `is_active`

### 8. `stock_metric_fact`

용도:

- 정규화 완료된 종목별/기간별 metric 저장

주요 컬럼:

- `ticker`
- `market`
- `metric_code`
- `period_type`
- `period_end`
- `fs_div`
- `value`
- `unit`
- `source`
- `rcept_no`
- `fetched_at`

## 순차 구현 계획

아래 순서는 기능 가치, 구현 난이도, 정합성 리스크를 함께 고려한 권장 순서입니다.

### Phase 0. 준비 작업

목표:

- OpenDART 사용을 위한 기반 마련
- 향후 raw 적재에 필요한 스키마와 enum 정리

작업:

- `.env.example` 및 설정에 `OPENDART_API_KEY` 추가
- source enum에 `OPENDART`, `KRX` 계열 추가
- ingestion run type 확장
- 기본 DDL에 신규 raw 테이블 초안 추가
- DART `corp_code`와 `ticker` 매핑 전략 문서화

완료 기준:

- 설정 로드 가능
- DB 초기화 시 신규 테이블 생성 가능

### Phase 1. DART 기업 식별자 매핑 구축

목표:

- `ticker -> corp_code` 매핑을 안정적으로 확보

작업:

- OpenDART 기업고유번호 파일 수집 어댑터 구현
- `dart_corp_master` 적재 서비스 구현
- `stock_master`와 join하여 현재 상장 종목 기준 정합성 검증
- 상장폐지/스팩/우선주/리츠 등 예외 케이스 점검

완료 기준:

- 활성 종목 대부분에 대해 `corp_code` 매핑 가능
- 매핑 누락 종목 보고서 출력 가능

### Phase 2. DART 재무 raw 적재

목표:

- 재무제표 주요 계정을 raw 형태로 저장

작업:

- `FinancialStatementProvider` 추가
- `fnlttSinglAcntAll` 어댑터 구현
- 연도/분기별 재무 raw 수집 서비스 구현
- `dart_financial_statement_raw` upsert 구현
- `CFS`/`OFS`, `BS`/`IS`/`CIS`/`CF`/`SCE` 구분 저장
- 장애 대응을 위한 재시도, rate limit, partial failure 기록 추가

우선 수집 대상:

- 손익계산서
- 재무상태표
- 현금흐름표

완료 기준:

- 지정 종목/기간에 대해 raw 재무계정 적재 가능
- 동일 보고서 재수집 시 idempotent upsert 동작

### Phase 3. 주식수 / 배당 / 자사주 raw 적재

목표:

- 주당 데이터 원재료와 주주환원 raw 확보

작업:

- `ShareCountProvider` 구현
- `ShareholderReturnProvider` 구현
- `stockTotqySttus`, `alotMatter`, `tesstkAcqsDspsSttus` 어댑터 추가
- `dart_share_count_raw`, `dart_shareholder_return_raw` 적재 서비스 구현

우선 수집 대상:

- 발행주식수
- 자기주식 수
- DPS
- 총배당금
- 자사주 매입/처분/소각 수량

완료 기준:

- 보고서 기준 주식수/주주환원 raw 적재 가능
- 동일 기업의 재무/주주환원 데이터 연결 가능

### Phase 4. canonical metric 매핑 1차

목표:

- 가장 중요한 raw 값을 바로 쓸 수 있는 지표로 정규화

작업:

- `metric_catalog` 정의
- `metric_mapping_rule` 정의
- raw -> metric 변환 배치 구현
- 우선순위 규칙 작성
  - `account_id` 우선
  - `account_nm` 보조
  - `CFS` 우선, 없으면 `OFS`

1차 포함 metric:

- 매출액
- 매출원가
- 매출총이익
- 판매비와관리비
- 영업이익
- 당기순이익
- 지배주주순이익
- 총자산
- 총부채
- 총자본
- 현금및현금성자산
- 영업활동현금흐름
- 투자활동현금흐름
- 재무활동현금흐름
- 발행주식수
- 자기주식 수
- DPS

완료 기준:

- 정규화 결과를 SQL 한 번으로 조회 가능
- 샘플 종목에 대해 수작업 대조가 가능

### Phase 5. XBRL 기반 고급 계정 확장

목표:

- 기본 API에서 누락되는 세부 계정 커버리지 확대

작업:

- `fnlttXbrl` 다운로드 및 파서 구현
- XBRL taxonomy / context / fact 파싱 구조 설계
- 세부 계정 alias 보강
- 재무주석 또는 비표준 계정명 기반 매핑 규칙 보완

우선 확장 대상:

- 감가상각비
- 무형자산상각비
- 연구개발비
- 광고선전비
- 인건비
- 이자수익
- 이자비용
- 가중평균주식수
- 희석주식수
- CAPEX
- 배당금 지급액
- 차입금 증가액
- 차입금 상환액
- 자사주 매입금액

완료 기준:

- 기본 API 대비 metric 커버리지 상승
- 누락/충돌 계정에 대한 예외 리포트 출력 가능

### Phase 6. KRX/pykrx 수급 raw 적재

목표:

- 종목별 일자 기준 수급 데이터 확보

작업:

- `FlowProvider` 추가
- `pykrx` 우선 어댑터 구현
- 부족 항목은 KRX 직접 어댑터 추가
- `krx_security_flow_raw` 적재 서비스 구현
- 일별 증분 수집 방식 정의

우선 수집 대상:

- 외국인 보유주식수
- 외국인 순매수 수량
- 기관 순매수 수량
- 개인 순매수 수량
- 공매도 거래량
- 공매도 거래대금
- 공매도 잔고 수량
- 대차잔고 수량

후순위:

- 프로그램 순매수 수량
- 신용매수 잔고

완료 기준:

- 지정 일자/종목에 대해 수급 raw 적재 가능
- OHLCV와 조인 가능한 동일 키 구조 확보

### Phase 7. 사업 KPI 프레임워크

목표:

- 비정형 운영 지표를 위한 확장 가능한 수집 프레임워크 구축

작업:

- `OperatingMetricProvider`보다는 `OperatingMetricExtractorRegistry` 중심 설계
- 기업/섹터별 extractor 인터페이스 정의
- 원문 문서 메타테이블 설계
- 첫 번째 섹터 파일럿 구현

권장 파일럿 순서:

1. 조선/방산: 수주금액, 수주잔고
2. 통신: 가입자 수, 해지 건수
3. 유통/프랜차이즈: 점포 수
4. 플랫폼/게임: MAU, DAU

완료 기준:

- 특정 섹터에 대해 재현 가능한 KPI 추출 가능
- 섹터별 extractor 추가 비용이 예측 가능해짐

## 테스트 및 검증 계획

### 단위 테스트

- DART 응답 파서 테스트
- metric mapping 규칙 테스트
- 수급 파서 테스트
- XBRL fact 추출 테스트

### 통합 테스트

- 샘플 종목 3~5개에 대한 end-to-end 수집
- 동일 실행 재수행 시 upsert 정합성 검증
- 부분 실패 후 재실행 복구 검증

### 샘플 검증 종목 추천

- 삼성전자: 대형주, 연결재무, 배당/자사주 이벤트 풍부
- SK하이닉스: 반도체 계정 다양성 확인
- NAVER: 플랫폼형 KPI 확장 후보
- 현대차: 제조업 재고/유형자산/판매비 구조 확인
- 한국조선해양 또는 한화에어로스페이스: 수주잔고 KPI 후보

### 검증 방식

- DART 원문 숫자와 DB 적재값 직접 대조
- KRX 웹 화면과 수급 raw 대조
- 분기/반기/사업보고서 간 period 정렬 검증

## 예상 리스크

### 1. DART 계정명 표준화 한계

리스크:

- 같은 의미의 계정이 회사별로 다른 이름으로 들어올 수 있음

대응:

- `account_id` 우선
- raw 적재 유지
- mapping rule 버전 관리

### 2. XBRL 파싱 복잡도

리스크:

- context 축, 단위, 기간, 연결/별도 구분 처리 난이도 높음

대응:

- 초기에는 `fnlttSinglAcntAll` 우선
- XBRL은 후속 phase에서 점진 확장

### 3. 수급 소스 불안정성

리스크:

- KRX 화면 구조 또는 비공식 요청 포맷 변경 가능

대응:

- `pykrx` 우선
- 직접 KRX 어댑터는 최소 범위로 유지
- source별 장애 분리

### 4. 사업 KPI 범용화 실패

리스크:

- 모든 업종에 대해 단일 파서 전략이 실패할 가능성 높음

대응:

- 섹터별 extractor 전략 채택
- 범용 metric보다 문서 원문과 parser provenance를 함께 저장

## 구현 우선순위 최종 제안

실제 착수 순서는 아래를 권장합니다.

1. `Phase 0`: 설정/DDL/enum 확장
2. `Phase 1`: DART corp_code 매핑
3. `Phase 2`: 재무 raw 적재
4. `Phase 3`: 주식수/배당/자사주 raw 적재
5. `Phase 4`: canonical metric 1차
6. `Phase 6`: 수급 raw 적재
7. `Phase 5`: XBRL 고급 확장
8. `Phase 7`: 사업 KPI 프레임워크

주의:

- 번호상 `Phase 5`보다 `Phase 6`을 먼저 구현하는 것이 실용적입니다.
- 이유는 수급 데이터가 구조가 단순하고 사용자 가치가 빠르게 나오기 때문입니다.

## 1차 구현 범위 제안

첫 릴리스는 아래까지만 해도 충분히 가치가 큽니다.

- DART corp_code 매핑
- DART 재무 raw 적재
- DART 주식수 raw 적재
- DART 배당/자사주 raw 적재
- canonical metric 1차
- KRX/pykrx 수급 raw 적재 일부

1차에서 보장할 결과:

- 재무제표 주요 raw 값 대부분 조회 가능
- 주당지표 원재료의 핵심 값 확보
- 기본 주주환원 데이터 확보
- 주요 수급 지표 확보

후속 릴리스 범위:

- XBRL 기반 세부 계정 확대
- 프로그램 매매/신용잔고 보강
- 사업 KPI 섹터별 파서

## 문서 외 참고 사항

현재 레포의 `README.md`, `docs/architecture.md`, `sql/postgres_ddl.sql` 기준으로 보면 이 확장은 기존 포트/어댑터 구조를 그대로 유지하면서 `새 Provider + raw 테이블 + 정규화 서비스`를 추가하는 방향이 가장 자연스럽습니다.

즉, 처음부터 최종 분석용 wide table을 설계하기보다 아래 순서를 지키는 것이 안전합니다.

1. raw 확보
2. 매핑 규칙 구축
3. canonical metric 생성
4. 사업 KPI 특화 파서 확장

## 실제 세부 구현 순서 추적표

아래 표는 실제 구현 진행 시 체크리스트로 사용합니다. `상태` 컬럼은 아래 값 중 하나로 관리합니다.

- `TODO`: 아직 시작 전
- `DOING`: 구현 중
- `DONE`: 구현 완료
- `BLOCKED`: 외부 이슈 또는 설계 재검토 필요
- `SKIP`: 현 단계에서 제외

| 순서 | 단계 | 세부 작업 | 주요 산출물 | 검증 기준 | 상태 | 구현 일자 | 담당/메모 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Phase 0 | `OPENDART_API_KEY` 설정 추가 및 설정 로더 확장 | `.env.example`, settings | 로컬 설정 로드 성공 | DONE | 2026-04-19 | Codex |
| 2 | Phase 0 | source/run type enum 확장 | enum, domain model | 신규 source가 CLI/서비스에서 사용 가능 | DONE | 2026-04-19 | Codex |
| 3 | Phase 0 | 신규 raw 테이블 DDL 초안 추가 | `sql/postgres_ddl.sql` | `db init` 시 테이블 생성 성공 | DONE | 2026-04-19 | Codex |
| 4 | Phase 0 | DB 문서 및 아키텍처 문서 업데이트 | `docs/database.md`, 관련 문서 | 신규 테이블/흐름이 문서에 반영됨 | DONE | 2026-04-19 | Codex |
| 5 | Phase 1 | OpenDART corp code 수집 어댑터 구현 | adapter/provider | corp code 목록 fetch 성공 | DONE | 2026-04-19 | Codex |
| 6 | Phase 1 | `dart_corp_master` 저장소/서비스 구현 | storage, service | ticker-corp_code upsert 가능 | DONE | 2026-04-19 | Codex |
| 7 | Phase 1 | `stock_master`와 매핑 검증 리포트 구현 | validation/report 로직 | 매핑률/누락 종목 출력 가능 | DONE | 2026-04-19 | Codex |
| 8 | Phase 2 | `FinancialStatementProvider` 포트 추가 | port interface | 서비스 계층에서 주입 가능 | DONE | 2026-04-19 | Codex |
| 9 | Phase 2 | `fnlttSinglAcntAll` OpenDART 어댑터 구현 | adapter | 단일 종목/기간 fetch 성공 | DONE | 2026-04-19 | Codex |
| 10 | Phase 2 | `dart_financial_statement_raw` 저장소 구현 | repository | raw upsert idempotent 동작 | DONE | 2026-04-19 | Codex |
| 11 | Phase 2 | 재무 raw 수집 서비스 구현 | service/use-case | 종목/기간 배치 적재 성공 | DONE | 2026-04-19 | Codex |
| 12 | Phase 2 | CLI 명령 추가 | CLI entrypoint | 명령 실행으로 수집 가능 | DONE | 2026-04-19 | Codex |
| 13 | Phase 2 | 샘플 종목 기준 재무 raw 통합 검증 | test/verification | DART 수치와 DB 값 대조 성공 | DONE | 2026-04-19 | Codex. `005930 / 2025 / 11011 / CFS` 실적 raw 139건 적재 확인 |
| 14 | Phase 3 | `ShareCountProvider` 포트/어댑터 구현 | port, adapter | 발행주식수/자기주식수 fetch 성공 | DONE | 2026-04-19 | Codex |
| 15 | Phase 3 | `ShareholderReturnProvider` 포트/어댑터 구현 | port, adapter | DPS/자사주 이벤트 fetch 성공 | DONE | 2026-04-19 | Codex |
| 16 | Phase 3 | 주식수 raw 저장소 구현 | repository | `dart_share_count_raw` upsert 성공 | DONE | 2026-04-19 | Codex |
| 17 | Phase 3 | 배당/자사주 raw 저장소 구현 | repository | `dart_shareholder_return_raw` upsert 성공 | DONE | 2026-04-19 | Codex |
| 18 | Phase 3 | 관련 수집 서비스 및 CLI 추가 | service, CLI | 보고서 기준 raw 적재 가능 | DONE | 2026-04-19 | Codex. `005930 / 2025 / 11011` 기준 share count 4건, shareholder return 135건 적재 확인 |
| 19 | Phase 4 | `metric_catalog`/`metric_mapping_rule` 테이블 추가 | DDL, storage | canonical rule 저장 가능 | DONE | 2026-04-19 | Codex |
| 20 | Phase 4 | raw -> metric 변환 서비스 구현 | normalization service | 주요 지표 변환 성공 | DONE | 2026-04-19 | Codex |
| 21 | Phase 4 | 1차 metric 매핑 규칙 작성 | mapping rule seed | revenue, OI, NI 등 매핑 성공 | DONE | 2026-04-19 | Codex |
| 22 | Phase 4 | `stock_metric_fact` 저장/조회 구현 | repository/query | SQL로 정규화 지표 조회 가능 | DONE | 2026-04-19 | Codex |
| 23 | Phase 4 | 샘플 기업 정규화 결과 검증 | test/verification | 공시 수치와 핵심 지표 일치 | DONE | 2026-04-19 | Codex. `005930 / 2025 / 11011` 기준 17개 canonical fact 생성 확인 |
| 24 | Phase 6 | `FlowProvider` 포트 추가 | port interface | 수급 계층 주입 가능 | DONE | 2026-04-19 | Codex |
| 25 | Phase 6 | pykrx 기반 수급 어댑터 1차 구현 | adapter | 외국인/기관/개인 순매수 fetch 성공 | DONE | 2026-04-19 | Codex. `foreign_holding_shares`, 투자자별 순매수, 공매도 metric 파서 및 provider 구현 |
| 26 | Phase 6 | 공매도/잔고/대차 데이터 어댑터 구현 | adapter | 공매도/대차 raw fetch 성공 | BLOCKED | 2026-04-19 | Codex. 공매도/잔고는 구현, `borrow_balance_quantity`는 `pykrx` 안정 경로 부재로 pending |
| 27 | Phase 6 | `krx_security_flow_raw` 저장소 구현 | repository | 일자/종목 기준 upsert 성공 | DONE | 2026-04-19 | Codex |
| 28 | Phase 6 | 수급 수집 서비스 및 CLI 추가 | service, CLI | 증분 수집 가능 | DONE | 2026-04-19 | Codex. `uv run krx-collector flows sync` 추가, provider timeout/fallback 포함 |
| 29 | Phase 6 | OHLCV와 수급 조인 검증 | verification query | 동일 키로 결합 가능 | BLOCKED | 2026-04-19 | Codex. 실호출 `flows sync --tickers 005930 --start 2026-04-17 --end 2026-04-17`은 pykrx/KRX timeout으로 live 적재 실패 |
| 30 | Phase 5 | XBRL 다운로드/파서 기반 구조 추가 | adapter/parser | XBRL 원문 로딩 성공 | DONE | 2026-04-19 | Codex. `dart sync-xbrl --tickers 005930 --bsns-years 2025 --reprt-codes 11011`로 문서 1건, fact 498건 적재 확인 |
| 31 | Phase 5 | 세부 계정 fact 추출 구현 | parser/service | 감가상각/이자비용 등 추출 가능 | DONE | 2026-04-19 | Codex. `dart_xbrl_fact_raw`에서 context/unit/label 포함 fact 추출 구현 |
| 32 | Phase 5 | 고급 metric 매핑 규칙 보강 | mapping rules | weighted/diluted shares 등 매핑 가능 | DONE | 2026-04-19 | Codex. `weighted_avg_shares`, `diluted_shares`, `depreciation_expense`, `amortization_intangible_assets` 및 CF 세부 계정 12종 추가 |
| 33 | Phase 5 | 커버리지 리포트 구현 | report | 기본 API 대비 추가 커버리지 측정 가능 | DONE | 2026-04-19 | Codex. `metrics coverage-report` 추가, `005930 / 2025 / 11011` 기준 29개 fact 생성 확인 |
| 34 | Phase 7 | 원문 문서 메타 스키마 설계 | DDL/design | 사업 KPI 원문 provenance 저장 가능 | DONE | 2026-04-19 | Codex. `operating_source_document`, `operating_metric_fact` 테이블 추가 |
| 35 | Phase 7 | extractor registry 인터페이스 구현 | service/interface | 섹터별 parser 등록 가능 | DONE | 2026-04-19 | Codex. `OperatingMetricExtractorRegistry` 및 기본 registry factory 구현 |
| 36 | Phase 7 | 파일럿 섹터 1개 구현 | parser/extractor | 예: 수주잔고 또는 가입자 수 추출 성공 | DONE | 2026-04-19 | Codex. `shipbuilding_defense` 파일럿 extractor로 `order_intake_amount`, `order_backlog_amount` 추출 구현 |
| 37 | Phase 7 | 사업 KPI 정규화/저장 규칙 정의 | metric schema/rule | KPI fact 저장 가능 | DONE | 2026-04-19 | Codex. `operating process-document` CLI로 샘플 문서 1건 처리, fact 2건 적재 확인 |
| 38 | 공통 | 재시도/rate limit/partial failure 처리 공통화 | util, infra | 외부 API 오류 시 복구 가능 | DONE | 2026-04-19 | Codex. `util/pipeline.py` 추가 후 DART 재무/주식수/XBRL, KRX 수급에 공통 재시도와 jitter sleep 적용 |
| 39 | 공통 | ingestion run/counts/error_summary 확장 | audit schema/service | 신규 파이프라인 감사 로그 기록 | DONE | 2026-04-19 | Codex. `RunStatus.PARTIAL` 추가, 공통 run finalizer로 `partial_failure_count`, `completed_request_count`, 샘플 request key 요약 기록 |
| 40 | 공통 | 단위 테스트 추가 | tests/unit | 파서/매핑 핵심 로직 테스트 통과 | DONE | 2026-04-19 | Codex. `test_pipeline_common.py`로 재시도/partial 상태 단위 테스트 추가 |
| 41 | 공통 | 통합 테스트 추가 | tests/integration | 샘플 종목 end-to-end 통과 | DONE | 2026-04-19 | Codex. operating KPI 샘플 문서의 DB round-trip 통합 테스트 추가 |
| 42 | 공통 | 운영 문서 및 runbook 갱신 | docs/operations.md 등 | 실행/복구 절차 문서화 완료 | DONE | 2026-04-19 | Codex. DART/XBRL/수급/사업 KPI 실행 예시와 `partial` 상태 해석, 재실행 절차 문서화 |
