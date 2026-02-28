# 프론트엔드 둘러보기

> 이 문서는 Spice OS 프론트엔드의 기술 스택, 디렉토리 구조, 그리고 각 UI 페이지가 백엔드의 어떤 개념/API에 연결되는지를 설명합니다.

---

## 기술 스택

| 영역 | 기술 | 버전 | 역할 |
|------|------|------|------|
| **프레임워크** | React | 18.3 | UI 컴포넌트 렌더링 |
| **언어** | TypeScript | - | 타입 안전성 |
| **빌드 도구** | Vite | 5.x | 빠른 빌드 + HMR (Hot Module Replacement) |
| **UI 라이브러리** | BlueprintJS | 6.x | 엔터프라이즈 UI 컴포넌트 (테이블, 다이얼로그 등) |
| **상태 관리** | Zustand | 5.x | 경량 전역 상태 |
| **서버 상태** | TanStack React Query | 5.x | API 요청 캐싱 + 갱신 |
| **그래프** | ReactFlow / Cytoscape | 11.x / 3.x | 파이프라인 빌더, 그래프 탐색 |
| **차트** | Recharts | 3.x | 데이터 시각화 |
| **코드 에디터** | Monaco Editor | - | SQL/Python 코드 편집 |
| **테스트** | Vitest + Playwright | - | 단위 + E2E 테스트 |

---

## 디렉토리 구조

```
frontend/
├── src/
│   ├── api/                     # 백엔드 API 클라이언트
│   │   ├── bff.ts               #   모든 API 타입과 호출 함수 (127KB!)
│   │   ├── config.ts            #   API_BASE_URL 설정
│   │   └── useRateLimitRetry.ts #   429 에러 자동 재시도
│   │
│   ├── pages/                   # 페이지 컴포넌트 (23개)
│   │   ├── OverviewPage.tsx     #   대시보드
│   │   ├── DatabasesPage.tsx    #   데이터베이스(온톨로지) 관리
│   │   ├── OntologyPage.tsx     #   객체 유형/링크 유형 정의
│   │   ├── InstancesPage.tsx    #   인스턴스 CRUD
│   │   ├── ObjectExplorerPage.tsx  # 객체 탐색/검색
│   │   ├── PipelineBuilderPage.tsx # 파이프라인 빌더 (55KB)
│   │   ├── ConnectionsPage.tsx  #   외부 데이터 소스 관리 (46KB)
│   │   └── ...                  #   기타 페이지들
│   │
│   ├── components/              # 재사용 가능한 UI 컴포넌트
│   │   ├── layout/              #   네비게이션, 사이드바
│   │   ├── pipeline/            #   파이프라인 빌더 관련 (18개 하위 디렉토리)
│   │   ├── agent/               #   AI 어시스턴트 채팅 UI
│   │   └── ux/                  #   유틸리티 컴포넌트
│   │
│   ├── store/                   # 전역 상태
│   │   └── useAppStore.ts       #   Zustand 스토어
│   │
│   ├── app/                     # 앱 부트스트랩
│   │   ├── AppRouter.tsx        #   라우팅 설정
│   │   └── AppBootstrap.tsx     #   초기화 로직
│   │
│   ├── commands/                # 커맨드 추적 (비동기 작업 상태)
│   │   ├── CommandTrackerDrawer.tsx
│   │   └── useCommandTracker.ts
│   │
│   ├── types/                   # TypeScript 타입 정의
│   │   └── app.ts
│   │
│   └── styles.css               # Tailwind 기반 스타일링
│
├── tests/                       # 테스트
├── package.json                 # 의존성 관리
├── vite.config.ts               # Vite 빌드 설정
└── tsconfig.json                # TypeScript 설정
```

---

## 페이지별 가이드

### 주요 페이지 ↔ 백엔드 매핑

| 페이지 | 파일 | 백엔드 개념 | 주요 API |
|--------|------|------------|---------|
| **Overview** | `OverviewPage.tsx` | 전체 현황 대시보드 | 여러 API 집계 |
| **Databases** | `DatabasesPage.tsx` | 온톨로지 컨테이너 관리 | `GET/POST /api/v1/databases` |
| **Ontology** | `OntologyPage.tsx` | 객체 유형, 링크 유형, 속성 정의 | `/api/v2/ontologies/{db}/objectTypes` |
| **Instances** | `InstancesPage.tsx` | 인스턴스 CRUD + 검색 | `/api/v2/.../objects/search` |
| **Object Explorer** | `ObjectExplorerPage.tsx` | 객체 상세 탐색 | `/api/v2/.../objects/{pk}` |
| **Pipeline Builder** | `PipelineBuilderPage.tsx` | DAG 기반 ETL 파이프라인 | `/api/v1/pipelines/*` |
| **Connections** | `ConnectionsPage.tsx` | 외부 DB 커넥터 관리 | `/api/v2/connectivity/*` |
| **Datasets** | `DatasetsPage.tsx` | 데이터셋 목록/상세 | `/api/v2/datasets/*` |
| **Dataset Analysis** | `DatasetAnalysisPage.tsx` | 데이터셋 분석/프로파일 | `/api/v1/datasets/*/profile` |
| **Objectify** | `ObjectifyPage.tsx` | 데이터셋 → 인스턴스 매핑 | `/api/v1/objectify/*` |
| **Actions** | `ActionsPage.tsx` | 액션 정의/실행 이력 | `/api/v2/.../actions/*` |
| **Lineage** | `LineagePage.tsx` | 데이터 리니지 그래프 | `/api/v1/lineage/*` |
| **Graph Explorer** | `GraphExplorerPage.tsx` | 관계 그래프 시각화 | `/api/v1/graph/*` |
| **Query Builder** | `QueryBuilderPage.tsx` | SQL 쿼리 빌더 | `/api/v1/query/*` |
| **Governance** | `GovernancePage.tsx` | 접근 제어/권한 관리 | `/api/v1/governance/*` |
| **Scheduler** | `SchedulerPage.tsx` | 파이프라인 스케줄 관리 | `/api/v1/pipelines/schedules/*` |
| **Mappings** | `MappingsPage.tsx` | 필드 매핑 설정 | `/api/v1/mappings/*` |
| **Audit** | `AuditPage.tsx` | 감사 로그 조회 | `/api/v1/audit/*` |
| **Tasks** | `TasksPage.tsx` | 비동기 작업 추적 | `/api/v1/command-status/*` |
| **AI Assistant** | `AIAssistantPage.tsx` | LLM 기반 데이터 분석 | `/api/v1/agent-proxy/*` |
| **Admin** | `AdminPage.tsx` | 시스템 관리 | `/api/v1/admin/*` |
| **Login** | `LoginPage.tsx` | 로그인/인증 | `/api/v2/auth/token` |

---

## API 클라이언트 (`bff.ts`)

`frontend/src/api/bff.ts`는 **모든 백엔드 API 호출을 한 파일에 모아둔** 중앙 API 클라이언트입니다 (127KB).

**핵심 패턴:**

```typescript
// 1. 타입 정의
interface DatabaseRecord {
  name: string;
  displayName: string;
  description?: string;
}

// 2. API 응답 래퍼
interface ApiResponse<T> {
  status: "success" | "error";
  message?: string;
  data?: T;
  errors?: string[];
}

// 3. API 호출 함수
async function getDatabases(): Promise<ApiResponse<DatabaseRecord[]>> {
  const response = await fetch(`${API_BASE_URL}/api/v1/databases`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  return response.json();
}
```

---

## 상태 관리

### Zustand (`store/useAppStore.ts`)

전역 상태를 **단순하게** 관리합니다:

```typescript
// Zustand 스토어 사용 예시
const useAppStore = create((set) => ({
  selectedDatabase: null,
  setSelectedDatabase: (db) => set({ selectedDatabase: db }),
}));

// 컴포넌트에서 사용
function MyComponent() {
  const db = useAppStore((state) => state.selectedDatabase);
  return <div>{db?.name}</div>;
}
```

### React Query (서버 상태)

API 데이터는 React Query로 관리합니다:

```typescript
// API 데이터 페칭 + 자동 캐싱
const { data, isLoading } = useQuery({
  queryKey: ['databases'],
  queryFn: () => bff.getDatabases(),
});
```

**React Query의 장점:**
- 자동 캐싱 (같은 데이터를 다시 요청하지 않음)
- 자동 재시도 (네트워크 에러 시)
- 백그라운드 갱신 (stale 데이터 자동 업데이트)

---

## 프론트엔드 로컬 실행

```bash
cd frontend
npm install    # 의존성 설치
npm run dev    # 개발 서버 시작 (http://localhost:5173)
```

**Vite의 HMR(Hot Module Replacement)** 덕분에, 코드를 수정하면 브라우저가 자동으로 새로고침됩니다.

### 빌드

```bash
npm run build     # 프로덕션 빌드
npm run preview   # 빌드 결과 미리보기
```

---

## 프론트엔드 코드를 읽는 순서 (추천)

1. **`src/app/AppRouter.tsx`** - 어떤 URL이 어떤 페이지로 연결되는지 확인
2. **`src/pages/OverviewPage.tsx`** - 가장 간단한 페이지부터 읽기
3. **`src/api/bff.ts`** - API 타입과 호출 방식 이해
4. **`src/pages/OntologyPage.tsx`** - 핵심 기능 페이지
5. **`src/pages/PipelineBuilderPage.tsx`** - 가장 복잡한 페이지 (ReactFlow 사용)

---

## 다음으로 읽을 문서

- [개발 워크플로](08-DEVELOPMENT-WORKFLOW.md) - 프론트엔드/백엔드 코드를 수정하는 방법
- [테스트 가이드](09-TESTING-GUIDE.md) - 프론트엔드 테스트 실행법
