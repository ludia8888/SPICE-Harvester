# SPICE HARVESTER 프로젝트 구조 가이드

## 개요
SPICE HARVESTER 프로젝트는 간결하고 직관적인 구조로 구성되어 있습니다.

> **📌 최종 업데이트: 2025-07-22**  
> 현재 프로젝트는 플랫 구조로 구성되어 있으며, 모든 sys.path.insert 구문이 제거되었습니다.  
> 새로운 기능: Property-to-Relationship 자동 변환, 고급 제약조건 시스템, TerminusDB v11.x 완전 지원

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

### 9. 최신 기능 추가 (2025-07-22)

#### Property-to-Relationship 자동 변환
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

#### 고급 제약조건 시스템
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

#### TerminusDB v11.x 스키마 타입
```python
# 새로운 스키마 타입 지원
from oms.utils.terminus_schema_types import TerminusSchemaBuilder

builder = TerminusSchemaBuilder()
builder.add_enum_property("status", ["draft", "published"])
builder.add_geopoint_property("location")
builder.add_one_of_type("value", ["xsd:string", "xsd:integer"])
```

### 10. 참고 자료

- [Python Packaging User Guide](https://packaging.python.org/)
- [setuptools Documentation](https://setuptools.pypa.io/)
- [PEP 517 -- A build-system independent format for source trees](https://www.python.org/dev/peps/pep-0517/)
- [TerminusDB v11.x Documentation](https://terminusdb.com/docs/)