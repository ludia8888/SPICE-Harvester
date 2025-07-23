# SPICE HARVESTER - 온톨로지 에디터 목업

Palantir Foundry의 Ontology Editor를 참고한 온톨로지 관리 시스템 UI 목업입니다.

## 프로젝트 구조

```
frontend/
├── src/
│   ├── components/
│   │   ├── common/          # 공통 컴포넌트
│   │   ├── layout/          # 레이아웃 컴포넌트
│   │   │   ├── TopBar.tsx   # 상단 네비게이션 바
│   │   │   └── Sidebar.tsx  # 좌측 사이드바
│   │   └── ontology/        # 온톨로지 관련 컴포넌트
│   │       ├── DiscoverView.tsx      # 홈 대시보드
│   │       ├── ObjectTypeView.tsx    # Object Type 상세 화면
│   │       ├── PropertyEditor.tsx    # 속성 편집기
│   │       └── LinkTypeWizard.tsx    # Link Type 생성 마법사
│   ├── types/
│   │   └── ontology.ts      # TypeScript 타입 정의
│   ├── themes/
│   │   └── theme.ts         # Material-UI 테마 설정
│   └── App.tsx              # 메인 애플리케이션
├── package.json
└── README.md
```

## 주요 기능

### 1. 상단 네비게이션 바 (TopBar)
- 글로벌 검색 (Cmd/Ctrl + K 지원)
- 새 리소스 생성 (Object Type, Link Type, Property, Group)
- 브랜치 전환 및 관리

### 2. 좌측 사이드바 (Sidebar)
- Discover (홈 대시보드)
- Proposals (제안, 브랜치 관리)
- History (변경 이력)
- Resources 섹션:
  - Object Types
  - Properties
  - Shared Properties
  - Link Types
  - Functions
  - Groups

### 3. Discover 화면 (홈 대시보드)
- 즐겨찾는 Object Types
- 최근에 본 Object Types
- 모든 Object Types 목록
- 개인화된 대시보드 구성

### 4. Object Type 상세 화면
- **Overview 탭**: 메타데이터, Properties 요약, Relationships 목록
- **Properties 탭**: 전체 속성 목록 및 관리
- **Security 탭**: 보안 설정 (목업)
- **Datasources 탭**: 데이터소스 설정 (목업)
- **Transforms 탭**: 변환 설정 (목업)

### 5. Property Editor (속성 편집기)
- **일반 탭**: 기본 속성 정보 (이름, 타입, 설명 등)
- **표시 탭**: 표시 형식 설정 (목업)
- **검증 탭**: 값 검증 규칙 (목업)
- 다양한 데이터 타입 지원 (String, Integer, Date, Link 등)
- Link 타입의 경우 대상 클래스 및 Cardinality 설정

### 6. Link Type 생성 마법사
- **Step 1**: 관계 유형 선택 (Foreign Key, Dataset, Object Type)
- **Step 2**: 연결 설정 (Source/Target 클래스, Cardinality)
- **Step 3**: 이름 설정 (Display Name, API Name)

## 기술 스택

- **React 18** + **TypeScript**
- **Vite** (빌드 도구)
- **Material-UI (MUI)** (UI 프레임워크)
- **React Router** (라우팅)
- **Axios** (HTTP 클라이언트)

## 설치 및 실행

```bash
# 의존성 설치
npm install

# 개발 서버 실행
npm run dev

# 빌드
npm run build
```

## UI/UX 디자인 특징

Palantir Foundry Ontology Editor의 디자인 원칙을 따라 구현:

- **미니멀하고 실용적인 디자인**
- **일관된 네비게이션 패턴**
- **정보 계층 구조의 명확성**
- **워크플로우 중심 설계** (마법사, 단계별 가이드)
- **실시간 피드백** (검증, 상태 표시)
- **협업 지원** (브랜치, 제안 시스템)

## 목업 데이터

현재 다음과 같은 샘플 데이터를 사용합니다:

- **Person**: 사람 엔티티 (id, name, age, email, department)
- **Organization**: 조직 엔티티 (id, name, founded)
- **Team**: 팀 엔티티 (id, name)

각 엔티티는 속성(Properties)과 관계(Relationships)를 가지며, UI에서 이를 시각적으로 관리할 수 있습니다.

## 향후 개발 계획

1. **백엔드 API 연동**
2. **실제 데이터 CRUD 기능**
3. **Link Type Graph 시각화**
4. **Validation 규칙 시스템**
5. **사용자 권한 관리**
6. **국제화 (i18n) 지원**
7. **검색 및 필터링 고도화**

## 참고 자료

- [Palantir Foundry 공식 문서](https://www.palantir.com/docs/)
- [Material-UI 문서](https://mui.com/)
- [React TypeScript 가이드](https://react-typescript-cheatsheet.netlify.app/)