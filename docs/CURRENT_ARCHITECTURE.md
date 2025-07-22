# SPICE HARVESTER - 현재 아키텍처 상태

> **최종 업데이트**: 2025-07-22  
> **버전**: 2.1

## 📁 프로젝트 구조

```
SPICE HARVESTER/
├── frontend/              # (향후 프론트엔드 예정)
├── backend/               # 모든 백엔드 서비스
│   ├── bff/              # Backend for Frontend 서비스
│   ├── oms/              # Ontology Management Service
│   ├── funnel/           # Type Inference Service
│   ├── data_connector/   # External Data Connectors
│   ├── shared/           # 공통 컴포넌트
│   ├── tests/            # 통합 테스트
│   ├── docs/             # 문서
│   └── docker-compose.yml
└── docs/                 # 프로젝트 전체 문서
```

## 🚀 서비스 아키텍처

### 서비스 구성
| 서비스 | 포트 | 설명 |
|--------|------|------|
| OMS | 8000 | 핵심 온톨로지 관리 (내부 ID 기반) |
| BFF | 8002 | 프론트엔드 API 게이트웨이 (사용자 친화적 레이블) |
| Funnel | 8003 | 타입 추론 및 스키마 제안 |
| TerminusDB | 6363 | 그래프 데이터베이스 |

### 아키텍처 다이어그램
```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Frontend      │────▶│      BFF        │────▶│      OMS        │     │    Funnel       │
│   (Port 3000)   │     │   (Port 8002)   │     │   (Port 8000)   │     │   (Port 8003)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
                                │                         │                         │
                                └─────────────────────────┼─────────────────────────┘
                                                          ▼
                                                  ┌─────────────────┐
                                                  │   TerminusDB    │
                                                  │   (Port 6363)   │
                                                  └─────────────────┘
```

## 📦 Import 구조

### 현재 Import 패턴
```python
# shared 모듈 import
from shared.models.ontology import OntologyCreateRequest
from shared.utils.jsonld import JSONToJSONLDConverter
from shared.config.service_config import ServiceConfig

# 서비스 간 import
from bff.services.oms_client import OMSClient
from oms.services.async_terminus import AsyncTerminusService

# 내부 모듈 import
from .routers import ontology_router
from .dependencies import get_terminus_service
```

### ❌ 사용하지 않는 패턴
```python
# sys.path.insert 사용 금지
import sys
sys.path.insert(0, "...")  # 이렇게 하지 마세요!

# spice_harvester 패키지 경로 사용 안함
from spice_harvester.shared...  # 사용하지 않음
```

## 🔧 환경 설정

### 포트 설정 (환경변수)
```bash
# .env 파일
OMS_PORT=8000
BFF_PORT=8002
FUNNEL_PORT=8003
TERMINUS_SERVER_URL=http://terminusdb:6363
```

### 서비스 URL 설정
```bash
OMS_BASE_URL=http://localhost:8000
BFF_BASE_URL=http://localhost:8002
FUNNEL_BASE_URL=http://localhost:8003
```

## 🏗️ 개발 환경

### 로컬 개발
```bash
# 1. 환경 설정
cd backend
cp .env.example .env

# 2. 가상환경 생성
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. 의존성 설치
pip install -r requirements.txt

# 4. 서비스 시작
python start_services.py  # 모든 서비스 한번에 시작

# 또는 개별 시작
./start_oms.sh
./start_bff.sh
./start_funnel.sh
```

### Docker 환경
```bash
# 빌드 및 실행
docker-compose up --build

# 백그라운드 실행
docker-compose up -d

# 로그 확인
docker-compose logs -f [service-name]
```

## 📋 주요 기능

### OMS (Ontology Management Service)
- **내부 ID 기반** 온톨로지 관리
- TerminusDB와 직접 통신
- 복합 타입 검증 및 직렬화
- 관계 관리 및 순환 참조 감지
- Property-to-Relationship 자동 변환
- 고급 제약조건 추출 및 검증
- TerminusDB v11.x 모든 스키마 타입 지원

### BFF (Backend for Frontend)
- **사용자 친화적 레이블** 기반 API
- OMS와 Funnel 서비스 오케스트레이션
- 라벨-ID 매핑 관리
- 병합 충돌 해결

### Funnel (Type Inference Service)
- 데이터로부터 타입 자동 추론
- Google Sheets 및 CSV 지원
- 스키마 제안 및 검증
- 복합 타입 감지 (EMAIL, PHONE, URL, MONEY 등)

### Shared Components
- 공통 모델 및 스키마
- 복합 타입 검증기
- 보안 유틸리티
- 설정 관리

## 🔄 데이터 흐름

1. **클라이언트 → BFF**: 사용자 친화적 레이블로 요청
2. **BFF → OMS**: 내부 ID로 변환하여 전달
3. **OMS → TerminusDB**: 실제 데이터 저장/조회
4. **Funnel → BFF**: 타입 추론 결과 제공
5. **BFF → 클라이언트**: 레이블로 변환된 응답

## 📝 헬스 체크 엔드포인트

- **OMS**: http://localhost:8000/health
- **BFF**: http://localhost:8002/health
- **Funnel**: http://localhost:8003/health
- **TerminusDB**: http://localhost:6363/api/

## 🚨 중요 사항

1. **포트 번호**: BFF는 8002번 포트 사용 (8001 아님)
2. **Import 경로**: 직접 경로 사용 (sys.path.insert 사용 안함)
3. **서비스 이름**: 간소화된 이름 사용 (oms, bff, funnel)
4. **구조**: 플랫 구조 채택 (중첩된 패키지 구조 아님)

## 📚 관련 문서

- [상세 아키텍처](./DETAILED_ARCHITECTURE.md)
- [배포 가이드](./backend/docs/deployment/DEPLOYMENT_GUIDE.md)
- [프론트엔드 개발 가이드](./backend/docs/development/FRONTEND_DEVELOPMENT_GUIDE.md)
- [포트 설정 가이드](./backend/PORT_CONFIGURATION.md)