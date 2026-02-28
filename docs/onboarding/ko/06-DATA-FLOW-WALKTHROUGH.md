# 데이터 흐름 추적 - 요청이 시스템을 어떻게 통과하나요?

> 3가지 대표 시나리오를 **단계별로 추적**해 볼게요. 각 단계에서 어떤 코드가 실행되는지 파일 경로와 함께 설명합니다.

💡 **이 문서를 읽으면 좋은 점:** 디버깅할 때 "이 요청이 어디를 거치지?"를 바로 파악할 수 있어요.

---

## 시나리오 1: 객체 검색 (Read Path)

> 💡 가장 빈번한 요청이에요. 읽기 경로가 얼마나 단순한지 확인해 보세요.

**상황:** 사용자가 프론트엔드에서 "Engineering 부서 직원 검색"을 합니다.

```
API: POST /api/v2/ontologies/{db}/objects/Employee/search
Body: { "where": { "type": "eq", "field": "department", "value": "Engineering" } }
```

### 검색 흐름

```mermaid
sequenceDiagram
    participant B as 🖥️ Browser
    participant BFF as 🚪 BFF (:8002)
    participant OMS as 🧠 OMS (:8000)
    participant ES as 🔍 Elasticsearch

    B->>BFF: POST /api/v2/.../Employee/search
    Note over BFF: JWT 토큰 검증<br/>middleware/auth.py
    BFF->>OMS: 내부 HTTP 전달
    Note over OMS: SearchJsonQueryV2 → ES DSL 변환<br/>routers/query.py
    OMS->>ES: { "query": { "term": { "department": "Engineering" } } }
    ES-->>OMS: 검색 결과 (hits)
    Note over OMS: Foundry v2 형식으로 변환<br/>(__rid, __primaryKey 추가)
    OMS-->>BFF: 변환된 응답
    BFF-->>B: JSON 응답
    Note over B: React Query 캐싱 + 렌더링
```

### 검색 관련 파일

| 단계 | 파일 |
|:---|:---|
| 프론트엔드 API 호출 | `frontend/src/api/bff.ts` → `searchObjects()` |
| 인증 미들웨어 | `backend/bff/middleware/auth.py` |
| BFF 라우터 | `backend/bff/routers/foundry_ontology_v2.py` |
| OMS 검색 핸들러 | `backend/oms/routers/query.py` |
| ES 쿼리 빌더 | SearchJsonQueryV2 → Elasticsearch DSL 변환 |

- ✅ **소요 시간:** ~10-50ms
- ✅ **접근 저장소:** Elasticsearch (주), PostgreSQL (스키마 오버레이)

💡 **읽기가 빠른 이유:** PostgreSQL을 거치지 않고 Elasticsearch에서 바로 결과를 가져오거든요. 이게 CQRS의 장점이에요.

---

## 시나리오 2: 객체 생성 (Write Path)

> 💡 쓰기 경로는 읽기보다 복잡해요. "즉시 응답 → 비동기 처리" 패턴을 주목해 보세요.

**상황:** 사용자가 새 직원 인스턴스를 생성합니다.

```
API: POST /api/v2/ontologies/{db}/actions/createObject
Body: { "parameters": { "objectType": "Employee", "properties": { ... } } }
```

### 생성 흐름

```mermaid
sequenceDiagram
    participant B as 🖥️ Browser
    participant BFF as 🚪 BFF (:8002)
    participant OMS as 🧠 OMS (:8000)
    participant PG as 🐘 PostgreSQL
    participant S3 as 📁 MinIO (S3)
    participant KF as 📮 Kafka
    participant PW as 📊 Projection Worker
    participant IW as 💾 Instance Worker

    B->>BFF: POST /actions/createObject
    BFF->>OMS: 액션 전달

    Note over OMS: 스키마 검증<br/>(필수 필드, 타입 체크)
    OMS->>S3: INSTANCE_CREATED 이벤트 영구 저장
    OMS->>PG: 런타임 상태 저장
    OMS->>KF: 이벤트 게시 (instances-created)
    OMS-->>BFF: 즉시 응답 (승인됨, ~100ms)
    BFF-->>B: 200 OK

    Note over KF: 비동기 처리 시작 (1~5초)

    par 병렬 Worker 처리
        KF->>PW: 이벤트 전달
        PW->>PW: ES 문서 인덱싱
        Note over PW: → 검색 가능해짐
    and
        KF->>IW: 이벤트 전달
        IW->>PG: 인스턴스 상태 갱신
    end
```

### 생성 관련 파일

| 단계 | 파일 |
|:---|:---|
| BFF 라우터 | `backend/bff/routers/foundry_ontology_v2.py` |
| OMS 액션 핸들러 | `backend/oms/routers/action_async.py` |
| 이벤트 모델 | `backend/shared/models/event_envelope.py` |
| Projection Worker | `backend/projection_worker/main.py` |
| Instance Worker | `backend/instance_worker/main.py` |

- ✅ **소요 시간:** 응답 ~100ms, 전체 리드모델 반영 1~5초
- ✅ **접근 저장소:** PostgreSQL, S3/MinIO, Kafka, Elasticsearch (비동기)

⚠️ **주의할 점:** 생성 직후 바로 검색하면 아직 ES에 인덱싱이 안 됐을 수 있어요. 이걸 "최종 일관성(eventual consistency)"이라고 해요.

---

## 시나리오 3: 데이터 파이프라인 (CSV → 인스턴스)

> 💡 가장 복잡한 흐름이에요. 파일 업로드부터 인스턴스 생성까지 여러 서비스를 거칩니다.

**상황:** CSV 파일을 업로드하고, 파이프라인으로 변환해서, 온톨로지 인스턴스를 만들어요.

### 파이프라인 흐름

```mermaid
sequenceDiagram
    participant B as 🖥️ Browser
    participant BFF as 🚪 BFF
    participant LF as 🌿 LakeFS
    participant PG as 🐘 PostgreSQL
    participant KF as 📮 Kafka
    participant OW as 📦 Objectify Worker
    participant OMS as 🧠 OMS

    B->>BFF: CSV 파일 업로드
    Note over BFF: POST /api/v2/datasets/{id}/files

    BFF->>LF: S3 Gateway로 파일 저장
    Note over LF: 브랜치에 커밋<br/>⚠️ _ensure_lakefs_branch_exists() 필수

    BFF->>PG: 데이터셋 메타데이터 저장
    Note over PG: dataset_registry 갱신

    B->>BFF: Objectify 작업 요청
    Note over BFF: POST /api/v1/objectify/jobs<br/>매핑 스펙: CSV 컬럼 → Property

    BFF->>KF: objectify-jobs 토픽에 발행

    KF->>OW: 작업 전달
    Note over OW: 1. LakeFS에서 CSV 읽기<br/>2. 매핑 스펙 적용<br/>3. 타입 변환 + 검증<br/>4. 리니지 기록

    OW->>OMS: 인스턴스 벌크 생성 (API)
    Note over OMS: 시나리오 2와 동일한<br/>이벤트 → Worker 흐름 실행

    OMS-->>OW: 생성 완료
```

### 파이프라인 관련 파일

| 단계 | 파일 |
|:---|:---|
| 파일 업로드 라우터 | `backend/bff/routers/foundry_datasets_v2.py` |
| LakeFS 클라이언트 | `backend/shared/services/storage/lakefs_client.py` |
| 데이터셋 레지스트리 | `backend/shared/services/registries/dataset_registry.py` |
| Objectify 라우터 | `backend/bff/routers/objectify_incremental.py` |
| Objectify Worker | `backend/objectify_worker/main.py` |

- ✅ **소요 시간:** CSV 크기에 따라 수초~수분
- ✅ **접근 저장소:** LakeFS (파일), PostgreSQL (메타데이터), Kafka (작업), Elasticsearch (인덱싱)

⚠️ **자주 겪는 문제:** LakeFS에 파일을 저장하려면 브랜치가 먼저 존재해야 해요. `_ensure_lakefs_branch_exists()`를 빠뜨리면 "NoSuchBucket" 에러가 발생합니다.

---

## 데이터 저장소별 역할 정리

> 💡 "이 데이터는 어디에 저장되지?" 싶을 때 참고하세요.

```mermaid
graph TB
    subgraph PG ["🐘 PostgreSQL (:5433)"]
        PG1["Source of Truth (진실의 원천)"]
        PG2["온톨로지 스키마 · 인스턴스 원본"]
        PG3["레지스트리 상태 · 액션 로그"]
    end

    subgraph ES ["🔍 Elasticsearch (:9200)"]
        ES1["Read Model (검색용 복사본)"]
        ES2["인스턴스 전문 검색 · 집계/분석"]
        ES3["속도 우선 (약간의 지연 허용)"]
    end

    subgraph S3 ["📁 MinIO/S3 (:9000)"]
        S31["Event Store (영구 기록)"]
        S32["모든 이벤트의 불변 기록"]
        S33["절대 삭제/수정하지 않음"]
    end

    subgraph LFS ["🌿 LakeFS (:48080)"]
        LF1["Git for Data Files"]
        LF2["CSV/Parquet 버전 관리"]
        LF3["브랜치 · 커밋 · 머지"]
    end

    subgraph RDS ["⚡ Redis (:6379)"]
        RD1["임시 캐시"]
        RD2["API 응답 캐시 · 세션"]
        RD3["휘발성 (없어져도 복구 가능)"]
    end

    subgraph KFK ["📮 Kafka (:39092)"]
        KF1["Message Broker"]
        KF2["서비스 간 비동기 통신"]
        KF3["순서 보장 · at-least-once"]
    end

    style PG fill:#E3F2FD,stroke:#1565C0,color:#000
    style ES fill:#E8F5E9,stroke:#2E7D32,color:#000
    style S3 fill:#FFF3E0,stroke:#E65100,color:#000
    style LFS fill:#F1F8E9,stroke:#558B2F,color:#000
    style RDS fill:#FFF9C4,stroke:#F57F17,color:#000
    style KFK fill:#FCE4EC,stroke:#880E4F,color:#000
```

---

## 이벤트 라이프사이클

> 💡 시스템에서 데이터가 변경되면, 항상 이 6단계를 거쳐요.

모든 데이터 변경은 아래 라이프사이클을 따릅니다.

```mermaid
graph LR
    A1["1️⃣ 액션 제출"] --> A2["2️⃣ 이벤트 생성"]
    A2 --> A3["3️⃣ 이벤트 저장"]
    A3 --> A4["4️⃣ 이벤트 게시"]
    A4 --> A5["5️⃣ 워커 처리"]
    A5 --> A6["6️⃣ 조회 가능"]

    A1 -.- N1["BFF/OMS에서 검증"]
    A2 -.- N2["불변, 타임스탬프, 행위자 태그"]
    A3 -.- N3["S3/MinIO (영구)"]
    A4 -.- N4["Kafka (실시간 전파)"]
    A5 -.- N5["PG 갱신, ES 인덱싱, 리니지 기록"]
    A6 -.- N6["ES 검색, PG 직접 조회"]

    style A1 fill:#E3F2FD,stroke:#1565C0,color:#000
    style A2 fill:#E8F5E9,stroke:#2E7D32,color:#000
    style A3 fill:#FFF3E0,stroke:#E65100,color:#000
    style A4 fill:#FFF9C4,stroke:#F57F17,color:#000
    style A5 fill:#F3E5F5,stroke:#6A1B9A,color:#000
    style A6 fill:#C8E6C9,stroke:#2E7D32,color:#000
```

---

## 다음으로 읽을 문서

- [프론트엔드 둘러보기](07-FRONTEND-TOUR.md) - UI 페이지가 이 데이터 흐름을 어떻게 보여주는지
- [개발 워크플로](08-DEVELOPMENT-WORKFLOW.md) - 이 흐름에 새 기능을 추가하는 방법
