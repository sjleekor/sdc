# 다음 세션 시작 프롬프트 — K-6f KIS flows 어댑터

작성: 2026-08-16 · 이 파일 자체를 붙여넣거나 "이 파일을 읽고 진행해줘"로 시작한다.
작업이 끝나면 **이 파일을 지운다.**

---

**K-6f — `flows`를 KRX 스크래핑에서 KIS Developers로 옮긴다.**

## 먼저 읽을 것

| 순서 | 파일 | 볼 부분 |
|---|---|---|
| 1 | `docs/dev/20260731_raw_features/02_data_expansion_plan/10_work_breakdown.md` | **"K 묶음"만.** 그 위는 배경 |
| 2 | `.../poc/flows_alternatives.md` | **§3.1b 전환 조건 · §5b 실호출 결과** |
| 3 | `.../poc/krx_access_inventory.md` | §1(경로 전체) · §8.1(운영 지속 논리) |
| 4 | `docs/operations.md` | "KRX 접근 제한" 절 |

**한 줄 배경.** 2026-08-16 KRX가 **약관 제10조 제2호(자동화 수집 금지) 위반**으로
이 호스트 IP를 차단했다. 속도 문제가 아니라 권한 문제라 공식 경로로 옮기는 중이다.
`flows`(투자자 순매수 · 공매도 · 외국인 보유)가 첫 대상이고,
**필드는 실호출로 이미 확정했다.**

## 이미 결정된 것 — 다시 논의하지 마라

- **K-0b 안 B** — Marketplace 수집기는 **한시적 위반으로 계속 돌린다.** 교체가 최우선.
  근거는 "요청량이 작아서"가 아니라 **"되메울 수 없는 공백을 피하려고 감수한다"**이다
- **새 백필을 시작하지 않는다.** 일 증분만 유지. 차단을 부른 게 백필이다
- **`flows`는 6/7 대체 가능.** `공매도 잔고 수량`만 미해결 (KRX 전용)
- **KIS 자격증명은 로컬 `.env`에 있다** — `KIS_APP_KEY` / `KIS_APP_SECRET` /
  `KIS_BASE_URL` / `KIS_TIMEOUT_SECONDS`. **prod에는 아직 없다**

## 확정된 엔드포인트 (실호출로 검증됨)

| 우리 metric | 엔드포인트 | TR_ID | 페이지 |
|---|---|---|---|
| 외국인 보유주식수 | `/uapi/domestic-stock/v1/quotations/inquire-price` | `FHKST01010100` | 현재값 (`frgn_hldn_qty`) |
| 공매도 거래량·거래대금 | `.../daily-short-sale` | `FHPST04830000` | **100행/호출**, 구간 지정 |
| 개인·외국인·기관 순매수 | `.../investor-trade-by-stock-daily` | `FHPTJ04160001` | **30행/호출**, `FID_INPUT_DATE_1`=종료일 |

이력은 **2014-04까지** 닿는다. `DIV`와 **공매도 잔고는 없다.**

## 시작 전에 답할 설계 결정 하나 — 파서 모양이 여기서 갈린다

`investor-trade-by-stock-daily`의 `output2`가 **101필드**다.
기관을 **증권 · 투신 · 은행 · 보험 · 사모 · 기금 · 기타법인**으로 쪼개고
순매수 **수량과 대금**을 모두 준다. 현재 `krx_security_flow_raw`는 **기관 통합 하나**뿐이다.

- **(a) 기존 7 metric만 채운다** — 교체에 집중. 세분 필드는 버린다
- **(b) 세분 metric도 같이 적재한다** — 확장. metric 체계가 커지고 마트 영향 검토 필요

**정하고 시작한다.** 나는 (a)를 권한다 — 교체와 확장을 한 PR에 섞으면
회귀 원인을 가릴 수 없다. (b)는 교체가 끝난 뒤 별도로.

## 구현 순서

1. **`settings.py` `kis_*` 필드 + 토큰 캐시 + 토큰버킷 스로틀**
   - 토큰: 유효 **1일**, 6시간 내 재발급은 같은 값, **발급할 때마다 알림톡이 발송된다**
   - 수집기는 매번 `docker compose run --rm`으로 새 컨테이너다 →
     **호스트 볼륨 캐시가 필수.** `collector` 서비스에 **현재 볼륨 마운트가 하나도 없다**
   - 초당 20건은 **공식 quota**다 → **토큰버킷**. KRX용 `HumanThrottle`을 쓰지 마라
     (그건 탐지 회피용 랜덤 지연이라 성격이 다르다)
2. `ports/` + `adapters/flows_kis/`
3. **전환 조건 6개** (`flows_alternatives.md` §3.1b) — **이게 어댑터보다 작업량이 크다**
   - source 전환 cursor (`Source.KIS`로 바꾸면 증분 시작점이 사라진다)
   - 종목 · 날짜 · 페이지 checkpoint (실패 단위가 종목별 구멍으로 바뀐다)
   - no-data tombstone
   - **실 HTTP · retry 계수** (현행 `requests_attempted`는 논리 작업 수다)
   - 전역 auth · rate-limit 서킷 브레이커
   - source-aware freshness (`service/freshness.py`가 `Source.KRX` 고정)
4. 테스트
5. 검증 후 `flows sync`의 KRX 경로 폐기 (K-5)

**운영 메모.** `investor-trade-by-stock-daily`가 호출당 30 거래일을 주므로
**매일 돌 필요가 없다.** 주 1회로도 창이 겹쳐 놓친 날을 자동으로 메운다.

## 제약

- **`data.krx.co.kr` / `kind.krx.co.kr`에 요청 금지.** 차단 상태이고 약관 대상이다
- **prod(sj2-server) 변경 금지.** 읽기는 괜찮다
- **`domain/`·`service/`는 `adapters/`·`infra/`를 import하지 않는다.** 배선은 `cli/app.py`에서만
- **새 백필 실행 금지** (K-0b 조건)
- **KIS 토큰을 함부로 재발급하지 마라** — 발급마다 알림톡이 간다. 캐시 확인이 먼저다
- `.env`를 transcript에 출력하지 마라

## 참고 — 이전 세션의 프로브 스크립트

`kis_probe.py` / `kis_depth.py`를 scratchpad에 만들어 뒀다.
경로가 세션마다 달라 남아 있지 않을 수 있고, 그러면 다시 만들어야 한다
(**토큰 재발급 = 알림톡 1회**). 필요한 호출 형태는 §"확정된 엔드포인트"에 다 있다.
