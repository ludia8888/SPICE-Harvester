# 30일 학습 로드맵

> Spice OS에 처음 합류한 개발자가 **4주 만에 자립적으로 코드를 작성**할 수 있도록 안내하는 학습 로드맵이에요. 매일 1~2시간씩 투자하는 것을 기준으로 합니다.

---

## Week 1: 기초 이해 (Day 1~5)

> 목표: "이 프로젝트가 뭔지 이해하고, 내 컴퓨터에서 실행한다"

### Day 1: 환경 설정 (예상 소요: 90분)

- [ ] README.ko.md (프로젝트 루트) 읽기 (15분)
- [ ] [로컬 환경 설정](03-LOCAL-SETUP.md) 따라하기 (60분)
- [ ] 전체 스택 실행 + 헬스 체크 확인
- [ ] MinIO Console (http://localhost:9001), Kafka UI (http://localhost:8080) 접속 확인

✅ **체크포인트:** `curl http://localhost:8002/api/v1/health`가 `"healthy"` 응답을 반환하면 성공이에요!

### Day 2: 제품 이해 (예상 소요: 70분)

- [ ] [이 제품이 뭔가요?](01-WHAT-IS-SPICE.md) 읽기 (20분)
- [ ] [멘탈 모델](02-MENTAL-MODEL.md) 읽기 (30분)
- [ ] docs-portal 핵심 개념 문서 훑어보기 (20분)

✅ **체크포인트:** 동료에게 "이 프로젝트가 뭘 하는 건지" 1분 안에 설명할 수 있으면 성공이에요!

### Day 3: 첫 API 호출 (예상 소요: 60분)

- [ ] [첫 API 호출](04-FIRST-API-CALL.md) 따라하기 (60분)

이 실습에서는 아래 흐름을 직접 경험해 볼 거예요:
- 데이터베이스 생성
- 객체 유형 (Object Type) 정의
- 인스턴스 (Instance) 생성
- 검색

각 단계에서 "뒤에서 무슨 일이 일어나는가"를 생각하면서 진행해 보세요.

✅ **체크포인트:** curl로 직접 인스턴스를 생성하고 검색할 수 있으면 성공이에요!

### Day 4: 아키텍처 이해 (예상 소요: 70분)

- [ ] [아키텍처 이해하기](05-ARCHITECTURE-EXPLAINED.md) Level 1, Level 2 읽기 (40분)
- [ ] docs-portal 아키텍처 투어 문서 읽기 (30분)

핵심 포인트:
- 4개 레이어 구조 이해하기
- Read Path와 Write Path의 차이 이해하기

✅ **체크포인트:** "요청이 BFF → OMS → ES를 거쳐 응답된다"를 그림으로 그릴 수 있으면 성공이에요!

### Day 5: 프론트엔드 둘러보기 (예상 소요: 60분)

- [ ] [프론트엔드 둘러보기](07-FRONTEND-TOUR.md) 읽기 (30분)
- [ ] 프론트엔드 로컬 실행 (`cd frontend && npm run dev`)
- [ ] 주요 페이지 5개를 직접 클릭하며 둘러보기 (30분)

둘러볼 페이지:
- Overview
- Databases
- Ontology
- Instances
- Pipeline Builder

💡 브라우저 DevTools의 Network 탭을 열어두고, 각 페이지가 어떤 API를 호출하는지 관찰해 보세요.

✅ **체크포인트:** "이 UI 페이지는 어떤 백엔드 API를 호출한다"를 3개 이상 말할 수 있으면 성공이에요!

---

## Week 2: 코드 이해 (Day 6~10)

> 목표: "코드를 읽고 구조를 이해한다"

### Day 6: BFF 코드 읽기 (예상 소요: 90분)

- [ ] `backend/bff/main.py` 전체 읽기 (40분)
  - 서비스 초기화 과정, 라우터 등록, DI 컨테이너 이해
- [ ] `backend/bff/routers/foundry_ontology_v2.py` 처음 200줄 읽기 (30분)
  - Foundry v2 API 패턴 이해
- [ ] `backend/bff/middleware/auth.py` 읽기 (20분)
  - 인증 미들웨어 이해

✅ **체크포인트:** BFF에 어떤 라우터가 있고, 라우터가 어떤 구조인지 설명할 수 있으면 성공이에요!

### Day 7: OMS 코드 읽기 (예상 소요: 90분)

- [ ] `backend/oms/main.py` 전체 읽기 (30분)
- [ ] `backend/oms/routers/query.py` 처음 200줄 읽기 (30분)
  - SearchJsonQueryV2 → Elasticsearch DSL 변환 이해
- [ ] `backend/oms/routers/action_async.py` 처음 200줄 읽기 (30분)
  - 액션 (Action) 처리 흐름 이해

✅ **체크포인트:** OMS가 BFF와 어떻게 다른지, 어떤 역할을 하는지 설명할 수 있으면 성공이에요!

### Day 8: 데이터 흐름 추적 (예상 소요: 90분)

- [ ] [데이터 흐름 추적](06-DATA-FLOW-WALKTHROUGH.md) 읽기 (30분)
- [ ] 시나리오 1 (검색)의 코드를 실제로 찾아 읽어보기 (30분)
- [ ] 시나리오 2 (생성)의 코드를 실제로 찾아 읽어보기 (30분)

✅ **체크포인트:** 검색 요청이 BFF의 어떤 파일 → OMS의 어떤 파일을 거치는지 알 수 있으면 성공이에요!

### Day 9: 공유 라이브러리 + 워커 읽기 (예상 소요: 95분)

#### 공유 라이브러리

- [ ] `backend/shared/models/ontology.py` 읽기 (20분)
- [ ] `backend/shared/models/common.py` 읽기 (15분)

#### 워커 패턴

- [ ] `backend/projection_worker/main.py` 읽기 (30분)
  - 이벤트 → ES 인덱싱 흐름 이해
- [ ] `backend/objectify_worker/main.py` 읽기 (30분)
  - 데이터셋 → 인스턴스 변환 흐름 이해

✅ **체크포인트:** Worker가 Kafka에서 메시지를 받아 어떻게 처리하는지 설명할 수 있으면 성공이에요!

### Day 10: 테스트 이해 (예상 소요: 80분)

- [ ] [테스트 가이드](09-TESTING-GUIDE.md) 읽기 (20분)
- [ ] `make backend-unit` 실행하여 전체 테스트 통과 확인 (10분)
- [ ] 단위 테스트 파일 2~3개 읽기 (30분)
  - 테스트 패턴 (Arrange/Act/Assert) 이해
- [ ] E2E 테스트 1개 읽기 (`test_openapi_contract_smoke.py`) (20분)

✅ **체크포인트:** 특정 테스트 파일 하나를 지정해서 단독 실행할 수 있으면 성공이에요!

---

## Week 3: 실전 연습 (Day 11~15)

> 목표: "코드를 직접 수정하고 테스트를 작성한다"

### Day 11: 첫 코드 수정 (예상 소요: 80분)

- [ ] [개발 워크플로](08-DEVELOPMENT-WORKFLOW.md) 읽기 (20분)
- [ ] 간단한 연습: 기존 API 엔드포인트의 응답에 필드 하나 추가해보기 (60분)

연습 순서:
1. 모델에 필드 추가
2. 서비스에서 값 채우기
3. 테스트 수정

✅ **체크포인트:** 코드 변경 → 테스트 실행 → 통과를 스스로 완료할 수 있으면 성공이에요!

### Day 12: 단위 테스트 작성 (예상 소요: 90분)

- [ ] 기존 서비스 함수에 대한 단위 테스트 2~3개 작성 (60분)
- [ ] Mock 사용법 익히기 — AsyncMock, MagicMock (20분)
- [ ] `PYTHONPATH=backend python3 -m pytest` 으로 내 테스트만 실행 (10분)

✅ **체크포인트:** 새 테스트 파일을 만들고 통과시킬 수 있으면 성공이에요!

### Day 13: 파이프라인 개념 이해 (예상 소요: 90분)

- [ ] docs-portal의 파이프라인 도구 레퍼런스 읽기 (30분)
- [ ] 프론트엔드 Pipeline Builder 페이지에서 간단한 파이프라인 만들어보기 (30분)
- [ ] `backend/pipeline_worker/main.py` 읽기 (30분)

✅ **체크포인트:** 파이프라인 노드(Input/Transform/Output)의 개념을 이해하면 성공이에요!

### Day 14: 커넥터/데이터 임포트 이해 (예상 소요: 60분)

- [ ] `backend/data_connector/` 코드 구조 읽기 (20분)
- [ ] 지원하는 커넥터 타입 6가지 확인 (10분)
- [ ] Import Mode 이해하기 (30분)

Import Mode 종류:
- SNAPSHOT
- APPEND
- INCREMENTAL
- CDC
- UPDATE

✅ **체크포인트:** 외부 DB 데이터가 어떻게 Spice OS로 들어오는지 설명할 수 있으면 성공이에요!

### Day 15: Event Sourcing 심화 (예상 소요: 90분)

- [ ] docs-portal의 이벤트 소싱 한국어 문서 읽기 (30분)
- [ ] `backend/shared/models/event_envelope.py` 읽기 (20분)
- [ ] MinIO Console에서 실제 저장된 이벤트 확인 — http://localhost:9001 (20분)
- [ ] [아키텍처 이해하기](05-ARCHITECTURE-EXPLAINED.md) Level 3 읽기 (20분)

✅ **체크포인트:** "왜 Event Sourcing을 쓰는지"를 3가지 이유로 설명할 수 있으면 성공이에요!

---

## Week 4: 자립 (Day 16~20+)

> 목표: "실제 이슈를 수정하고 PR을 제출한다"

### Day 16~17: 역할별 가이드 (예상 소요: 각 120분)

본인 역할에 맞는 docs-portal 가이드를 읽어보세요:

- **백엔드 개발:** 아키텍처/서비스 토폴로지 문서
- **프론트엔드 개발:** 프론트엔드 투어 + React/Zustand 심화
- **DevOps:** 모니터링/트러블슈팅 문서

추가로:
- [ ] 코드 표준(code-standards) 문서 읽기

### Day 18~19: 실제 이슈 수정 (예상 소요: 각 120분)

- [ ] GitHub Issues에서 `good-first-issue` 라벨 찾기
- [ ] 또는 팀 리더에게 간단한 작업 할당 받기
- [ ] feature 브랜치 생성 → 코드 수정 → 테스트 → PR 작성

💡 첫 PR은 작은 것으로 시작하는 게 좋아요. 오타 수정이나 로그 메시지 개선도 훌륭한 첫 PR이에요.

### Day 20: 코드 리뷰 (예상 소요: 90분)

- [ ] 다른 팀원의 PR 코드 리뷰 참여

코드 리뷰에서 확인할 포인트:
- 코드 스타일이 프로젝트 컨벤션에 맞는지
- 테스트가 충분히 작성되었는지
- 아키텍처 패턴(Router → Service → Model)을 따르는지

✅ **체크포인트:** 첫 PR을 제출하고 머지할 수 있으면 성공이에요!

---

## 지속 학습 (Day 21~30+)

> 목표: "더 깊이 배우고, 팀에 기여한다"

- [ ] E2E 테스트 작성 시도
- [ ] 모니터링 도구 사용해 보기
  - Grafana: http://localhost:13000
  - Jaeger: http://localhost:16686
- [ ] 다른 신규 팀원의 온보딩 도와주기 (가르치면서 배우기)
- [ ] [트러블슈팅 FAQ](10-TROUBLESHOOTING-FAQ.md)에 새로 발견한 문제 추가하기

💡 가장 효과적인 학습법은 **다른 사람에게 설명하는 것**이에요. 새 팀원이 합류하면 이 로드맵을 함께 진행해 보세요.

---

## 핵심 파일 읽기 우선순위 (Top 15)

> 💡 빠르게 코드를 이해하고 싶다면, 이 순서로 파일을 읽어보세요.

### BFF/OMS 서비스 구조

| 순서 | 파일 | 이유 |
|------|------|------|
| 1 | `backend/bff/main.py` | BFF 서비스 전체 구조 |
| 2 | `backend/oms/main.py` | OMS 서비스 전체 구조 |

### 핵심 모델/설정

| 순서 | 파일 | 이유 |
|------|------|------|
| 3 | `backend/shared/models/ontology.py` | 핵심 데이터 모델 |
| 4 | `backend/shared/models/common.py` | 공통 타입 정의 |
| 7 | `backend/shared/config/settings.py` | 환경 설정 |
| 8 | `backend/shared/models/event_envelope.py` | 이벤트 구조 |

### 라우터/검색

| 순서 | 파일 | 이유 |
|------|------|------|
| 5 | `backend/bff/routers/foundry_ontology_v2.py` (처음 300줄) | Foundry v2 API 패턴 |
| 6 | `backend/oms/routers/query.py` (처음 200줄) | 검색 로직 |

### 워커/레지스트리

| 순서 | 파일 | 이유 |
|------|------|------|
| 9 | `backend/projection_worker/main.py` | 워커 패턴 |
| 10 | `backend/objectify_worker/main.py` | 데이터 변환 |
| 11 | `backend/shared/services/registries/dataset_registry.py` | 레지스트리 패턴 |

### 프론트엔드 (코드)

| 순서 | 파일 | 이유 |
|------|------|------|
| 12 | `frontend/src/api/bff.ts` (타입 정의 부분) | API 클라이언트 |
| 13 | `frontend/src/app/AppRouter.tsx` | 프론트엔드 라우팅 |
| 14 | `frontend/src/pages/OntologyPage.tsx` | 핵심 UI 페이지 |

### 인프라 (Docker Compose)

| 순서 | 파일 | 이유 |
|------|------|------|
| 15 | `docker-compose.full.yml` | 전체 인프라 구성 |

---

## 유용한 참고 자료

### 내부 문서

- docs-portal 한국어 (`docs-portal/i18n/ko/`) — 상세 아키텍처, API, 운영 가이드
- `docs/ARCHITECTURE.md` — 자동 생성 아키텍처 문서 (영문)
- `docs/API_REFERENCE.md` — 전체 API 레퍼런스 (영문)

### 외부 기술 문서

#### 백엔드
- [FastAPI 공식 문서](https://fastapi.tiangolo.com/) — 백엔드 프레임워크
- [Pydantic v2 문서](https://docs.pydantic.dev/) — 데이터 검증

#### 프론트엔드 (외부 문서)
- [React 공식 문서](https://react.dev/) — 프론트엔드 프레임워크
- [BlueprintJS 문서](https://blueprintjs.com/) — UI 컴포넌트
- [Zustand 문서](https://zustand-demo.pmnd.rs/) — 상태 관리

#### 인프라 (외부 문서)
- [Apache Kafka 문서](https://kafka.apache.org/documentation/) — 메시징
- [Elasticsearch 문서](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html) — 검색 엔진
