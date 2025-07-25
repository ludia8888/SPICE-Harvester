# SPICE HARVESTER 프로젝트 구조 가이드

## 개요
SPICE HARVESTER 프로젝트는 간결하고 직관적인 구조로 구성되어 있습니다.

> **📌 최종 업데이트: 2025-07-26**  
> 현재 프로젝트는 플랫 구조로 구성되어 있으며, 모든 sys.path.insert 구문이 제거되었습니다.  
> **🔥 NEW**: Git-like 버전 관리 시스템 완전 구현 (7/7 기능 100% 작동)  
> **🚀 ULTRA UPDATE**: 코드 중복 제거, API 표준화, 성능 최적화 완료  
> 새로운 기능: Service Factory 패턴, ApiResponse 표준화, HTTP 연결 풀링

> ### 🔥 최신 리팩토링 완료 (2025-07-26)
> - ✅ **코드 중복 제거**: Service Factory로 600+ 라인 제거
> - ✅ **API 표준화**: ApiResponse 모델로 모든 엔드포인트 통일
> - ✅ **에러 처리 개선**: 404, 409, 400 상태 코드 정확한 매핑
> - ✅ **성능 최적화**: HTTP 연결 풀링 (50/100), Semaphore(50)
> - ✅ **BFF-OMS 통합**: 자동 property name 생성, XSD 타입 매핑

## 변경 사항

### 1. 디렉토리 구조 변경

**현재 구조:**
```
backend/
├── pyproject.toml          # Python 패키지 설정
├── bff/                    # Backend for Frontend 서비스
├── oms/                    # Ontology Management Service
├── funnel/                 # Type Inference Service
├── shared/                 # 공유 컴포넌트
│   ├── config/            # 서비스 설정
│   ├── dependencies/      # 의존성 주입
│   ├── interfaces/        # 서비스 인터페이스
│   ├── models/            # 공유 모델
│   ├── validators/        # 검증기
│   ├── serializers/       # 직렬화
│   ├── security/          # 보안 유틸리티
│   └── utils/             # 유틸리티
└── data_connector/         # 데이터 커넥터
```

### 2. Import 방식 변경

#### 기본 Import 변경

**기존 방식:**
```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
from models.ontology import OntologyCreateRequest
```

**새로운 방식 (현재):**
```python
from shared.models.ontology import OntologyCreateRequest
```

#### 서비스별 Import 예시

**OMS 서비스에서 shared 모듈 사용:**
```python
# 기존
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
from models.common import BaseResponse

# 새로운 방식 (현재)
from shared.models.common import BaseResponse
```

**BFF 서비스에서 OMS 클라이언트 사용:**
```python
# 기존
from services.oms_client import OMSClient

# 새로운 방식 (현재)
from bff.services.oms_client import OMSClient
```

**테스트 코드에서 여러 모듈 사용:**
```python
# 기존 (conftest.py)
backend_root = Path(__file__).parent.parent
sys.path.insert(0, str(backend_root / "shared"))
sys.path.insert(0, str(backend_root / "backend-for-frontend"))
sys.path.insert(0, str(backend_root / "ontology-management-service"))

# 새로운 방식 (현재)
# 더 이상 sys.path 조작 불필요
from shared.models import *
from bff.services import *
from oms.entities import *
```

### 3. 개발 환경 설정

#### 패키지 설치 (개발 모드)
```bash
cd backend
pip install -e .
```

#### 추가 개발 의존성 설치
```bash
pip install -e ".[dev]"
```

### 4. Docker 설정 변경

**기존 Dockerfile:**
```dockerfile
ENV PYTHONPATH=/app:/app/shared
COPY ./shared /app/shared
```

**새로운 Dockerfile (현재):**
```dockerfile
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
```

### 5. 마이그레이션 단계

#### Phase 1: 준비 (✅ 완료)
- [x] pyproject.toml 생성
- [x] 디렉토리 구조 변경 계획
- [x] shared 모듈 import 경로 파악

#### Phase 2: 서비스 마이그레이션 (✅ 완료)
- [x] ontology-management-service → oms로 이름 변경
- [x] backend-for-frontend → bff로 이름 변경
- [x] 모든 import 경로 업데이트
- [x] Funnel 서비스 통합 (Port 8003)
- [x] Data Connector 서비스 유지

#### Phase 3: 테스트 및 검증 (✅ 완료)
- [x] 단위 테스트 실행
- [x] 통합 테스트 실행
- [x] Docker 빌드 테스트
- [x] 서비스 실행 테스트

#### Phase 4: 정리 (✅ 완료)
- [x] 모든 sys.path.insert 제거
- [x] 문서 업데이트
- [x] 백업 생성

### 6. 마이그레이션 완료 현황

**✅ 완료된 사항:**
- 84개의 sys.path.insert 구문 모두 제거
- 서비스 이름 간소화 (oms, bff, funnel)
- 표준 Python import 경로 사용
- IDE 자동완성 및 타입 체킹 정상 작동

### 7. 문제 해결

**Import 오류 발생 시:**
1. 패키지가 정상적으로 설치되었는지 확인: `pip list | grep spice-harvester`
2. PYTHONPATH 설정 확인: `echo $PYTHONPATH`
3. 상대 경로 대신 절대 경로 사용

**IDE 자동완성이 작동하지 않을 때:**
1. IDE를 재시작
2. Python 인터프리터 재설정
3. 프로젝트 인덱스 재생성

### 8. 이점

1. **개발 효율성 향상**
   - IDE 자동완성 및 타입 힌트 정상 작동
   - 정적 분석 도구 활용 가능

2. **안정성 향상**
   - 예측 가능한 import 동작
   - 배포 환경과 개발 환경의 일관성

3. **유지보수성 개선**
   - 명확한 의존성 관계
   - 표준 Python 패키지 구조

### 9. 최신 기능 추가 (2025-07-26)

#### 🎯 코드 중복 제거 및 표준화 (NEW)

**Service Factory 패턴:**
```python
# shared/services/service_factory.py
from shared.services.service_factory import create_fastapi_service, ServiceInfo

# 기존: 각 서비스마다 100+ 라인의 초기화 코드
app = FastAPI()
app.add_middleware(CORSMiddleware, ...)
app.add_middleware(LoggingMiddleware, ...)
# ... 많은 중복 코드

# 새로운 방식:
service_info = ServiceInfo(
    name="OMS",
    version="1.0.0",
    port=8000
)
app = create_fastapi_service(service_info)
```

**ApiResponse 표준화:**
```python
# shared/models/responses.py
from shared.models.responses import ApiResponse

# 기존: 각 엔드포인트마다 다른 응답 형식
return {"success": True, "data": {...}}  # BFF
return {"result": "success", "databases": [...]}  # OMS

# 새로운 방식:
return ApiResponse.success(
    message="데이터베이스 목록 조회 성공",
    data={"databases": databases}
).to_dict()
```

**BFF Adapter Service:**
```python
# bff/services/adapter_service.py
class BFFAdapterService:
    """BFF와 OMS 간의 비즈니스 로직 중앙화"""
    
    async def create_ontology_with_label(self, db_name, label, properties):
        # 라벨에서 ID 자동 생성
        ontology_id = generate_simple_id(label)
        
        # property name 자동 생성
        for prop in properties:
            if 'name' not in prop and 'label' in prop:
                prop['name'] = generate_simple_id(prop['label'])
        
        # OMS로 전달
        return await self._terminus.create_class(db_name, {...})
```

#### 🚀 성능 최적화 (NEW)

**HTTP 연결 풀링:**
```python
# oms/services/async_terminus.py
import httpx

# 기존: 매 요청마다 새 연결
client = httpx.AsyncClient()
response = await client.get(...)
await client.aclose()

# 새로운 방식: 연결 재사용
limits = httpx.Limits(
    max_keepalive_connections=50,
    max_connections=100,
    keepalive_expiry=30.0
)
self._client = httpx.AsyncClient(limits=limits)
```

**동시성 제어:**
```python
# TerminusDB 보호를 위한 동시 요청 제한
self._request_semaphore = asyncio.Semaphore(50)

async def _make_request(self, method, url, data, params):
    async with self._request_semaphore:
        # 최대 50개 동시 요청만 허용
        response = await self._client.request(...)
```

#### 🔧 에러 처리 개선 (NEW)

**HTTP 상태 코드 정확한 매핑:**
```python
# oms/routers/ontology.py
try:
    result = await terminus.create_ontology(...)
except DuplicateOntologyError:
    # 기존: 500 에러로 반환
    # 새로운 방식: 409 Conflict
    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=f"온톨로지 '{ontology_id}'이(가) 이미 존재합니다"
    )
```

**404 에러 올바른 전파:**
```python
# bff/dependencies.py
async def get_class(self, db_name: str, class_id: str):
    try:
        response = await client.get_ontology(db_name, class_id)
        return response.get("data")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None  # BFF에서 404로 처리
        raise
```

### 10. 기존 기능 (2025-07-25)

#### 🔥 Git-like 버전 관리 시스템 (NEW)

완전한 git-like 기능이 구현되어 프로덕션에서 사용 가능합니다:

**Branch 관리:**
```python
# AsyncTerminusService를 통한 브랜치 조작
from oms.services.async_terminus import AsyncTerminusService

terminus = AsyncTerminusService()

# 브랜치 생성
await terminus.create_branch("my_db", "experiment/feature-a", "main")

# 브랜치 목록 조회
branches = await terminus.list_branches("my_db")

# 브랜치 삭제
await terminus.delete_branch("my_db", "experiment/old-feature")
```

**Diff 및 비교:**
```python
# 3단계 diff 시스템 (commit-based, schema-level, property-level)
diff_result = await terminus.diff("my_db", "main", "experiment/feature-a")

# 결과는 실제 차이점을 상세히 포함
for change in diff_result:
    if change['type'] == 'class_modified':
        print(f"Class {change['class_id']} modified:")
        for prop_change in change.get('property_changes', []):
            print(f"  - {prop_change['property']}: {prop_change['change']}")
```

**Merge 연산:**
```python
# TerminusDB의 rebase API를 활용한 실제 병합
merge_result = await terminus.merge(
    "my_db", 
    "experiment/feature-a", 
    "main",
    message="Merge feature-a into main",
    author="developer"
)

if merge_result.get('merged'):
    print("Merge successful!")
else:
    print(f"Merge failed: {merge_result.get('error')}")
```

**Pull Request 워크플로:**
```python
# PR 생성
pr = await terminus.create_pull_request(
    "my_db",
    source_branch="experiment/feature-a",
    target_branch="main",
    title="Add new Product features",
    description="This PR adds important functionality"
)

# 충돌 확인
if pr['can_merge']:
    # PR 병합
    merge_result = await terminus.merge_pull_request(
        "my_db", 
        pr['id'],
        merge_message="Merged via PR",
        author="maintainer"
    )
else:
    print(f"PR has conflicts: {pr['conflicts']}")
```

**Multi-Branch 실험 환경:**
```python
# 실험 환경 매니저 사용
from test_multi_branch_experiment import MultiExperimentEnvironment

env = MultiExperimentEnvironment(terminus, "my_db")

# 여러 실험 브랜치 생성
await env.create_experiment("simple-schema")
await env.create_experiment("complex-schema")
await env.create_experiment("hybrid-schema")

# 실험 간 비교
diff = await env.compare_experiments("simple-schema", "complex-schema")

# 성공한 실험을 main에 병합
success = await env.merge_successful_experiment("complex-schema")
```

**Rollback 및 이력 관리:**
```python
# 커밋 히스토리 조회
commits = await terminus.get_commit_history("my_db", branch="main", limit=10)

# 특정 커밋으로 롤백
rollback_result = await terminus.reset_to_commit(
    "my_db",
    "commit_1737757890123",
    branch="main",
    author="admin"
)
```

#### Git 기능 마이그레이션 가이드

기존 프로젝트에서 새로운 git 기능을 사용하려면:

1. **서비스 업데이트 확인:**
```bash
# 최신 코드 pull
git pull origin main

# 의존성 업데이트
pip install -r requirements.txt
```

2. **TerminusDB v11.x 호환성 확인:**
```python
# 새로운 AsyncTerminusService 사용
from oms.services.async_terminus import AsyncTerminusService

# 기존 TerminusService 대신 AsyncTerminusService 사용
terminus = AsyncTerminusService()
```

3. **API 엔드포인트 업데이트:**
```bash
# 새로운 git 관련 엔드포인트들
curl http://localhost:8000/api/v1/database/my_db/branches
curl http://localhost:8000/api/v1/database/my_db/commits
curl "http://localhost:8000/api/v1/database/my_db/diff?from_branch=main&to_branch=feature"
```

#### 이전 기능 업데이트 (2025-07-22)

#### Property-to-Relationship 자동 변환 (기존 기능)
```python
# OMS에서 자동으로 property를 relationship으로 변환
from oms.services.property_to_relationship_converter import PropertyToRelationshipConverter

# 클래스 정의에서 type="link" 사용
{
    "properties": [{
        "name": "author",
        "type": "link",
        "linkTarget": "Person"  # 자동으로 Person 클래스와의 관계로 변환
    }]
}
```

#### 고급 제약조건 시스템 (기존 기능)
```python
# 제약조건 추출 및 검증
from oms.utils.constraint_extractor import ConstraintExtractor

constraints = {
    "min": 0,
    "max": 100,
    "pattern": "^[A-Z][a-z]+$",
    "min_length": 2,
    "max_length": 50
}
```

#### TerminusDB v11.x 스키마 타입 (기존 기능)
```python
# 새로운 스키마 타입 지원
from oms.utils.terminus_schema_types import TerminusSchemaBuilder

builder = TerminusSchemaBuilder()
builder.add_enum_property("status", ["draft", "published"])
builder.add_geopoint_property("location")
builder.add_one_of_type("value", ["xsd:string", "xsd:integer"])
```

### 11. 마이그레이션 가이드 - 최신 리팩토링 적용

기존 프로젝트에 최신 리팩토링을 적용하려면:

#### 1. Service Factory 적용:
```python
# 기존 main.py의 초기화 코드를 대체
# bff/main.py, oms/main.py, funnel/main.py

from shared.services.service_factory import create_fastapi_service, ServiceInfo

service_info = ServiceInfo(
    name="OMS",
    version="1.0.0",
    description="Ontology Management Service",
    port=int(os.getenv("OMS_PORT", "8000"))
)

app = create_fastapi_service(service_info)
```

#### 2. ApiResponse 표준화:
```python
# 모든 라우터에서 ApiResponse 사용
from shared.models.responses import ApiResponse

@router.get("/list")
async def list_databases():
    databases = await terminus.list_databases()
    return ApiResponse.success(
        message="데이터베이스 목록 조회 성공",
        data={"databases": databases}
    ).to_dict()
```

#### 3. 검증 Dependencies 적용:
```python
# oms/dependencies.py의 검증 로직 사용
from oms.dependencies import ensure_database_exists, ValidatedDatabaseName

@router.post("/{db_name}/ontology")
async def create_ontology(
    db_name: str = Depends(ensure_database_exists),
    ontology_data: Dict[str, Any] = ...
):
    # db_name은 이미 검증됨
    pass
```

#### 4. 환경 변수 업데이트:
```bash
# .env 파일
TERMINUS_SERVER_URL=http://localhost:6364  # 6363 → 6364
TERMINUS_KEY=admin  # admin123 → admin
OMS_PORT=8000  # 8005 → 8000
FUNNEL_PORT=8004  # 8003 → 8004
```

### 12. 참고 자료

- [Python Packaging User Guide](https://packaging.python.org/)
- [setuptools Documentation](https://setuptools.pypa.io/)
- [PEP 517 -- A build-system independent format for source trees](https://www.python.org/dev/peps/pep-0517/)
- [TerminusDB v11.x Documentation](https://terminusdb.com/docs/)