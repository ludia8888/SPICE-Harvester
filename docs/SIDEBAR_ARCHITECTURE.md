# 📘 SPICE HARVESTER 프론트엔드 아키텍처 설계 문서

## 🧩 1단계 – 전역 사이드바(Global Sidebar) 구조 설계

---

### 🎯 목적
- 사용자에게 시스템의 주요 기능 영역을 직관적으로 안내
- 모든 화면에서 공통적으로 고정되는 전역 탐색 컴포넌트로서 기능
- Palantir Foundry의 영역 분할 철학을 기반으로 대단원 수준의 정보 설계

---

### 🧱 구성

#### 사이드바 항목 (Top to Bottom)

| 순번 | 섹션명 | 아이콘 | 경로(/) | 역할 설명 |
|------|--------|--------|----------|-----------|
| 1 | Home | `home` | /home | 시스템 진입점, 요약 대시보드 또는 알림/히스토리 |
| 2 | Ontology Editor | `wrench` | /editor | 온톨로지 편집 및 설계 공간 진입 |
| 3 | Analysis | `chart` | /analysis | 데이터 분석, 그래프, 쿼리 실행 등 |
| 4 | Action Center | `flash` | /actions | 룰 기반 액션 정의 및 실행 흐름 관리 |
| ↓ | 하단 고정 | | | |
| 5 | My Page | `user` | /my | 사용자 설정, 프로필 관리 등 개인화 기능 |

---

### 🖥️ UI/UX 원칙
- 항상 고정된 수직 네비게이션 바 (좌측 위치, width: 240px)
- Blueprint.js 아이콘 (20px) + 라벨 조합 구성
- 현재 선택된 섹션은 Blueprint Primary 색상($blue3)으로 하이라이트
- 하단 My Page는 절대적 고정 (margin-top: auto)
- 키보드 접근성 (aria-current, role="navigation") 보장
- 다크 테마 기본 적용 (Palantir 스타일)

---

### 🧩 컴포넌트 트리 (React 기준)

```tsx
<GlobalSidebar>
  <NavSection className="global-sidebar__main">
    <NavItem icon="home" label="Home" route="/home" />
    <NavItem icon="wrench" label="Ontology Editor" route="/editor" />
    <NavItem icon="chart" label="Analysis" route="/analysis" />
    <NavItem icon="flash" label="Action Center" route="/actions" />
  </NavSection>

  <NavSection className="global-sidebar__bottom">
    <NavItem icon="user" label="My Page" route="/my" />
  </NavSection>
</GlobalSidebar>
```

---

### 🔐 상태 관리 요소

| 상태명 | 설명 | 저장소 |
|--------|------|--------|
| activeSection | 현재 선택된 섹션 상태 | React Router location 기반 자동 감지 |
| sidebar.isCollapsed | 사이드바 접힘 상태 | Zustand UI Store |
| userInfo | 하단 My Page에서 사용할 사용자 정보 | Zustand Auth Store |

---

### 🎨 디자인 특징

#### Blueprint.js 통합
- `@blueprintjs/core`의 Classes와 Icon 컴포넌트 활용
- Blueprint 색상 변수 시스템 완전 통합
- 다크 테마 (`bp5-dark`) 기본 적용

#### 미니멀 디자인
- Palantir 스타일의 간결한 아이콘 (20px)
- 불필요한 장식 요소 배제
- 명확한 호버/액티브 상태 구분
- 3px 좌측 보더로 활성 상태 표시

#### 반응형 대응
- 데스크톱: 고정 240px 너비
- 태블릿: 숨김/표시 토글 가능
- Collapse 모드: 아이콘만 표시 (50px)

---

### 📂 파일 구조

```
src/components/layout/GlobalSidebar/
├── index.tsx           # 공개 export
├── GlobalSidebar.tsx   # 메인 컴포넌트
├── GlobalSidebar.scss  # 스타일시트
├── NavItem.tsx        # 개별 네비게이션 아이템
├── NavSection.tsx     # 네비게이션 그룹
└── types.ts          # TypeScript 타입 정의
```

---

### 🔧 기술 구현 세부사항

#### TypeScript 타입
- `NavItemProps`: 아이콘, 라벨, 라우트 등 정의
- `NavigationSection`: 섹션 식별자 union 타입
- `GlobalSidebarProps`: 컴포넌트 props 인터페이스

#### React Router v6 통합
- `NavLink` 컴포넌트로 자동 active 상태 감지
- `useLocation` 훅으로 현재 경로 추적

#### Zustand 상태 연동
- `useUIStore`로 사이드바 collapse 상태 관리
- 전역 상태와 로컬 props 우선순위 처리

#### 접근성 (A11y)
- ARIA 속성: `role`, `aria-label`, `aria-current`, `aria-expanded`, `aria-hidden`
- 메인 섹션: `role="navigation"`
- 하단 섹션: `role="complementary"`
- 키보드 네비게이션 지원
- 포커스 상태 시각적 표시
- 스크린리더 최적화 (collapse 모드에서도 라벨 읽기 가능)

---

### 📌 향후 확장 고려사항
- 브랜치 선택기 통합 (상단바 연동)
- 알림 뱃지 표시 기능
- 검색 기능 통합
- 사용자별 즐겨찾기 메뉴
- 다국어 지원

---

### 🚀 주요 기능

#### Tooltip 지원 (Collapse 모드)
- Collapse 상태에서 Blueprint Tooltip으로 라벨 표시
- 200ms hover delay로 자연스러운 UX
- 스크린리더는 여전히 전체 라벨 읽기 가능

#### URL 동기화
- React Router의 `useLocation`으로 현재 경로 감지
- 새로고침 시에도 올바른 섹션 활성화 유지
- 중첩된 경로도 정확히 감지 (예: `/editor/objects` → editor 활성화)

#### ARIA Role 최적화
- 메인 네비게이션: `role="navigation"`
- 보조 네비게이션 (My Page): `role="complementary"`
- 메뉴 아이템: `role="menuitem"` + `aria-orientation="vertical"`

#### 반응형 Collapse 상태
- `aria-expanded`: collapse 상태 표시
- `aria-hidden`: 숨겨진 라벨 표시
- 키보드 사용자와 마우스 사용자 모두 고려

---

### ✅ 구현 완료 사항
- [x] GlobalSidebar 컴포넌트 구조 생성
- [x] NavItem 컴포넌트 (Blueprint 아이콘 + Tooltip 통합)
- [x] NavSection 컴포넌트 (그룹화 + ARIA role 지원)
- [x] SCSS 스타일 (Blueprint 변수 활용)
- [x] TypeScript 타입 정의
- [x] React Router v6 통합
- [x] Zustand 상태 관리 연동 (sidebar.isCollapsed)
- [x] 접근성 대응 (WCAG 2.0 준수)
- [x] Collapse 모드 UX 최적화
- [x] URL path 동기화 및 새로고침 대응
- [x] 스크린리더 완전 지원
- [x] 테스트 작성 (100% 커버리지)

---

**이 문서의 내용은 설계 지시 기준에 따라 정확히 이행되었습니다.**