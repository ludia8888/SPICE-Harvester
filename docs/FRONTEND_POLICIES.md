# Frontend Policies (TanStack Query + Zustand)

이 문서는 SPICE-Harvester 프론트엔드가 **백엔드의 이벤트 소싱/CQRS 특성**에 맞게 동작하도록, 팀이 따라야 할 “정책(Policy)”을 못 박아 둔 것입니다.

## 1) 컨텍스트 정책: URL이 SSoT, Store는 캐시

- 컨텍스트(세계관): `project(db_name)`, `branch`, `lang`
- **SSoT**: URL query (`?project=...&branch=...&lang=...`)
- Zustand는 UI가 쓰기 쉬운 “캐시”이며, URL이 바뀌면 항상 Zustand가 따라갑니다.
- LocalStorage 키: `spice.project`, `spice.branch`, `spice.language`
- 구현:
  - URL 파싱/구독: `frontend/src/state/urlContext.ts`
  - Zustand 컨텍스트 스토어: `frontend/src/store/useAppStore.ts`
  - URL↔Store 동기화 부트스트랩: `frontend/src/app/AppBootstrap.tsx`

## 2) 인증 정책: 기본은 메모리, Remember me는 옵션

- 토큰은 기본적으로 **메모리 저장**(새로고침 시 재입력).
- `Remember token`을 켠 경우에만 localStorage에 저장.
- “위험 작업”은 별도의 `Admin mode` 토글로 명시적으로 활성화.
- LocalStorage 키: `spice.adminToken`, `spice.rememberToken`, `spice.theme`
- BFF 헤더: `X-Admin-Token` 또는 `Authorization: Bearer <token>`
- 구현:
  - 상태/저장: `frontend/src/store/useAppStore.ts`
  - 설정 UI: `frontend/src/AppShell.tsx`, `frontend/src/components/SettingsDialog.tsx`

## 3) Command Tracker 정책: 202 Accepted는 “작업”이다

- 쓰기 요청은 `202 + command_id`를 반환하며, 프론트는 이를 “작업”으로 추적합니다.
- 완료 의미를 2단계로 분리:
  - `WRITE_DONE` (커맨드 완료)
  - `VISIBLE_IN_SEARCH` (리드모델/검색에 반영 완료)
- LocalStorage 키: `commandTracker.items`
- 구현:
  - 상태 추적/폴링: `frontend/src/commands/useCommandTracker.ts`
  - invalidate 중앙 테이블: `frontend/src/commands/commandInvalidationMap.ts`

## 4) Query 정책: 키/무효화 규칙을 중앙집중

- QueryKey는 공통 팩토리로만 생성합니다.
- invalidate는 command kind 기반으로 중앙 테이블에서만 결정합니다.
- 구현:
  - QueryKey 팩토리: `frontend/src/query/queryKeys.ts`

## 5) OCC 정책: expected_seq는 사용자 입력이 아니라 “리소스 버전”

- UI 입력으로 expected_seq를 받지 않습니다.
- BFF 편의 엔드포인트로 현재 expected_seq를 조회해 자동 첨부합니다.
- 구현:
  - BFF: `GET /api/v1/databases/{db_name}/expected-seq`
  - Frontend: `frontend/src/api/bff.ts` (`getDatabaseExpectedSeq`)

## 6) 에러 정책: 문제 유형 분류로 UX 자동화

- 공통 분류: `AUTH`, `OCC_CONFLICT`, `VALIDATION`, `TEMPORARY`, `UNKNOWN`
- 구현:
  - `frontend/src/errors/classifyError.ts`

## 7) 레이트리밋 정책: 제한적 자동 재시도

- 429는 `Retry-After` 기반 카운트다운을 항상 표시합니다.
- 자동 재시도는 **AI/Import/Connector만 1회** 허용합니다.
- 나머지 요청은 자동 재시도 없이 사용자가 수동 재시도합니다.
- 구현:
  - `frontend/src/api/useRateLimitRetry.ts`
  - 적용 화면: Graph Explorer(AI), Sheets Hub, Import Sheets/Excel, Schema Suggestion

## 8) 브랜치 예외 정책: 읽기/쓰기/보호 규칙 명시

- Instances 읽기 API는 branch를 받지 않거나 무시하므로 **Branch ignored** 배지 고정.
- Import commit은 항상 `main`으로 기록되므로, main 이외 브랜치에서는 확인 체크 필수.
- 보호 브랜치 정책은 Summary의 `policy.is_protected_branch`로 확인하고,
  Ontology 적용/머지 Resolve는 `adminToken + adminMode`가 없으면 차단.
