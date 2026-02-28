# 프론트엔드 둘러보기

> Spice OS 프론트엔드의 기술 스택, 디렉토리 구조, 그리고 각 UI 페이지가 백엔드의 어떤 API에 연결되는지 살펴볼게요.

---

## 기술 스택

> 💡 익숙한 기술이 많을 거예요. React + TypeScript 기반이고, 상태 관리는 Zustand로 가볍게 가져갑니다.

### 코어

| 기술 | 버전 | 역할 |
|:---|:---|:---|
| React | 18.3 | UI 컴포넌트 렌더링 |
| TypeScript | - | 타입 안전성 |
| Vite | 5.x | 빠른 빌드 + HMR (Hot Module Replacement) |

### UI와 상태 관리

| 기술 | 버전 | 역할 |
|:---|:---|:---|
| BlueprintJS | 6.x | 엔터프라이즈 UI 컴포넌트 (테이블, 다이얼로그 등) |
| Zustand | 5.x | 경량 전역 상태 |
| TanStack React Query | 5.x | API 요청 캐싱 + 갱신 |

### 시각화와 도구

| 기술 | 버전 | 역할 |
|:---|:---|:---|
| ReactFlow / Cytoscape | 11.x / 3.x | 파이프라인 빌더, 그래프 탐색 |
| Recharts | 3.x | 데이터 시각화 |
| Monaco Editor | - | SQL/Python 코드 편집 |
| Vitest + Playwright | - | 단위 + E2E 테스트 |

---

## 디렉토리 구조

> 💡 처음에는 `src/pages/`와 `src/api/`만 알면 충분해요. 나머지는 필요할 때 하나씩 살펴보면 됩니다.

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

> 💡 "이 화면에서 호출하는 API가 뭐지?" 싶을 때 이 표를 참고하세요.

### 핵심 페이지

가장 자주 작업하게 될 페이지들이에요.

| 페이지 | 파일 | 주요 API |
|:---|:---|:---|
| **Overview** | `OverviewPage.tsx` | 여러 API 집계 |
| **Databases** | `DatabasesPage.tsx` | `GET/POST /api/v1/databases` |
| **Ontology** | `OntologyPage.tsx` | `/api/v2/ontologies/{db}/objectTypes` |
| **Instances** | `InstancesPage.tsx` | `/api/v2/.../objects/search` |
| **Object Explorer** | `ObjectExplorerPage.tsx` | `/api/v2/.../objects/{pk}` |
| **Pipeline Builder** | `PipelineBuilderPage.tsx` | `/api/v1/pipelines/*` |

### 데이터 관리 페이지

| 페이지 | 파일 | 주요 API |
|:---|:---|:---|
| **Connections** | `ConnectionsPage.tsx` | `/api/v2/connectivity/*` |
| **Datasets** | `DatasetsPage.tsx` | `/api/v2/datasets/*` |
| **Dataset Analysis** | `DatasetAnalysisPage.tsx` | `/api/v1/datasets/*/profile` |
| **Objectify** | `ObjectifyPage.tsx` | `/api/v1/objectify/*` |
| **Actions** | `ActionsPage.tsx` | `/api/v2/.../actions/*` |

### 분석과 시각화 페이지

| 페이지 | 파일 | 주요 API |
|:---|:---|:---|
| **Lineage** | `LineagePage.tsx` | `/api/v1/lineage/*` |
| **Graph Explorer** | `GraphExplorerPage.tsx` | `/api/v1/graph/*` |
| **Query Builder** | `QueryBuilderPage.tsx` | `/api/v1/query/*` |

### 운영/관리 페이지

| 페이지 | 파일 | 주요 API |
|:---|:---|:---|
| **Governance** | `GovernancePage.tsx` | `/api/v1/governance/*` |
| **Scheduler** | `SchedulerPage.tsx` | `/api/v1/pipelines/schedules/*` |
| **Mappings** | `MappingsPage.tsx` | `/api/v1/mappings/*` |
| **Audit** | `AuditPage.tsx` | `/api/v1/audit/*` |
| **Tasks** | `TasksPage.tsx` | `/api/v1/command-status/*` |
| **AI Assistant** | `AIAssistantPage.tsx` | `/api/v1/agent-proxy/*` |
| **Admin** | `AdminPage.tsx` | `/api/v1/admin/*` |
| **Login** | `LoginPage.tsx` | `/api/v2/auth/token` |

---

## API 클라이언트 (`bff.ts`)

> 💡 이 파일 하나에 모든 API 호출이 들어 있어요. 127KB로 꽤 크지만, 패턴이 일관되어서 금방 익숙해질 거예요.

`frontend/src/api/bff.ts`는 **모든 백엔드 API 호출을 한 곳에 모아둔** 중앙 API 클라이언트예요.

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

> 💡 클라이언트 상태는 Zustand, 서버 상태는 React Query로 나눠서 관리해요. 이 구분이 중요합니다!

### Zustand (`store/useAppStore.ts`)

전역 상태를 **심플하게** 관리해요.

```tsx
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

API에서 가져온 데이터는 React Query로 관리해요.

```typescript
// API 데이터 페칭 + 자동 캐싱
const { data, isLoading } = useQuery({
  queryKey: ['databases'],
  queryFn: () => bff.getDatabases(),
});
```

**React Query가 해주는 일:**
- ✅ **자동 캐싱** - 같은 데이터를 다시 요청하지 않아요
- ✅ **자동 재시도** - 네트워크 에러가 나면 알아서 다시 시도해요
- ✅ **백그라운드 갱신** - 오래된 데이터를 자동으로 업데이트해요

---

## 프론트엔드 로컬 실행

```bash
cd frontend
npm install    # 의존성 설치
npm run dev    # 개발 서버 시작 (http://localhost:5173)
```

Vite의 **HMR(Hot Module Replacement)** 덕분에 코드를 수정하면 브라우저가 자동으로 반영돼요. 새로고침할 필요가 없습니다.

### 빌드

```bash
npm run build     # 프로덕션 빌드
npm run preview   # 빌드 결과 미리보기
```

---

## 프론트엔드 코드를 읽는 순서 (추천)

> 💡 처음부터 모든 코드를 읽으려 하지 마세요. 아래 순서대로 하나씩 따라가면 자연스럽게 전체 구조가 보여요.

1. **`src/app/AppRouter.tsx`** - 어떤 URL이 어떤 페이지로 연결되는지 확인해요
2. **`src/pages/OverviewPage.tsx`** - 가장 간단한 페이지부터 읽어보세요
3. **`src/api/bff.ts`** - API 타입과 호출 방식을 파악해요
4. **`src/pages/OntologyPage.tsx`** - 핵심 기능인 객체 유형 (Object Type) / 링크 유형 (Link Type) / 속성 (Property) 정의 페이지예요
5. **`src/pages/PipelineBuilderPage.tsx`** - 가장 복잡한 페이지예요. ReactFlow를 사용한 DAG 빌더입니다

---

## 다음으로 읽을 문서

- [개발 워크플로](08-DEVELOPMENT-WORKFLOW.md) - 프론트엔드/백엔드 코드를 수정하는 방법
- [테스트 가이드](09-TESTING-GUIDE.md) - 프론트엔드 테스트 실행법
