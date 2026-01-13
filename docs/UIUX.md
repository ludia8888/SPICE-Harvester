> Updated: 2026-01-08  
> Status: external reference (Ontology editor UI patterns). Not a description of the current SPICE HARVESTER frontend.

이 문서는 “온톨로지 편집기(모델링 UI)”를 만들 때 필요한 **화면 구성/워크플로우/안전장치**를 정리한 레퍼런스 노트입니다. 실제 구현 범위/정합성은 `docs/frontend.md`와 코드 기준으로 확인합니다.

## 목표

- 온톨로지 리소스(ObjectType/LinkType/ActionType/ValueType/Interface/Group/SharedProperty 등)를 **코드 없이** 편집 가능
- 브랜치/제안/히스토리로 변경을 **안전하게** 관리(검토/승인/배포 게이트)
- 편집 결과가 “실행(Writeback) + 감사(ActionLog) + 재현(Versioning)”으로 연결

## 전체 레이아웃(권장)

- **Top bar**
  - 전역 검색(이름/별칭/RID)
  - 새 리소스 생성(New)
  - 브랜치 전환/생성
- **Left sidebar**
  - Discover(홈), Proposals(제안), History(히스토리)
  - 리소스별 목록 섹션(Object Types, Link Types, Action Types, Value Types, …)
- **Main**
  - 리스트/상세/편집 폼
- **Right inspector(선택)**
  - 선택 항목 상세, 관련 링크, 변경 diff, 권한/상태 등

## 핵심 화면

### 1) Discover (홈 대시보드)

- 즐겨찾기/최근 본 항목/최근 변경/내 제안(프로포절) 요약
- 초보 사용자에게는 “가이드 카드(첫 작업 안내)” 제공

### 2) Resource List (리소스 목록)

- 섹션별 리스트 + 필터 + 검색
- 상태 뱃지(active/experimental/deprecated), 인덱싱/검증 상태 표시
- 클릭 시 상세 화면으로 이동

### 3) Resource Detail (리소스 상세)

권장 탭 구성(리소스 타입별로 일부 상이):
- Overview: 요약/설명/상태/태그
- Schema: 필드/타입/제약(즉시 검증)
- Security: permission policy, required roles, 접근 정책
- Datasources/Mapping: backing source, key spec, 샘플/미리보기
- History/Diff: 변경 이력, branch 간 비교

### 4) Proposal / Review (제안/검토)

- 변경 diff를 “사람이 읽기 쉬운” 형태로 제공(요약 + 상세)
- 승인/거부/코멘트/리뷰 체크리스트
- 안전 규칙: protected branch 직접 변경 금지, validate/simulate-first 강제

## 생성/편집 워크플로우 패턴

- **Wizard(마법사) 기반 생성**: 복잡한 설정을 단계로 분해
  - ObjectType: 이름/ID → 키 spec → 필드/관계 → 권한 → 확인
  - LinkType: from/to → cardinality → predicate → 제약 → 확인
  - ActionType: input schema → submission criteria → 구현(템플릿/규칙) → simulate → 승인/제출
- **실시간 검증**: “저장 눌렀더니 실패”가 아니라, 작성 단계에서 대부분 잡음
- **Undo/Redo & Draft**: 가능하면 초안 저장 + 되돌리기 제공(최소: 명시적 discard)

## 안전장치(필수)

- validate 결과를 UI에서 구조화 표시(필드/경로/수정 힌트)
- simulate 결과(overlay/lakeFS/ES 효과 포함)를 “diff”로 보여주고, 그 다음에만 submit
- 권한/조건 미달은 에러 코드에 따라 사용자 처방이 달라져야 함
  - 권한 부족: 승인 요청/역할 부여 안내
  - 조건 불충족(state mismatch): 선행 작업/상태 전환 안내
- DEGRADE 상태(예: overlay degraded)는 UI에서 위험 표시 + 자동 실행 금지

## 운영 UX(관측/추적)

- 비동기 커맨드(202) 추적 Drawer/페이지(상태 폴링/WS)
- Audit/Lineage 화면과 연동(“왜 이렇게 됐지?”를 클릭 몇 번으로 재현)
- 오류 분류(enterprise code / severity / retryable / human_required)를 그대로 노출

## 문서화 규칙

- “무엇을 클릭하면 무엇이 보이고 어떤 API가 호출되는지”를 화면 단위로 명시
- “거짓 UX” 금지: 백엔드가 제공하지 않는 일관성/보장을 UI가 가정하지 않기
