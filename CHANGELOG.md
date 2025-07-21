# SPICE HARVESTER 변경 로그

이 문서는 SPICE HARVESTER 프로젝트의 주요 구조적 변경사항을 추적합니다.

## [2.0.0] - 2025-07-20

### 변경사항
- 문서 업데이트: 현재 코드 구조와 일치하도록 모든 문서 갱신
- BFF 포트 수정: DEPLOYMENT_GUIDE.md에서 8001 → 8002로 수정
- 새 문서 추가: CURRENT_ARCHITECTURE.md 생성으로 현재 상태 명확화
- 문서 정리: 오래된 마이그레이션 문서를 archive 폴더로 이동

### 현재 구조
- **서비스 포트**:
  - OMS: 8000
  - BFF: 8002
  - Funnel: 8003
  - TerminusDB: 6363

## [1.5.0] - 2025-07-18

### 추가
- HTTPS 지원 구현
- CORS 자동 설정 기능
- 포트 설정 중앙화 (ServiceConfig)

### 변경사항
- 디렉토리 구조 단순화: `backend/spice_harvester/*` → `backend/*`
- Import 경로 변경: `from spice_harvester.shared...` → `from shared...`
- 서비스 이름 간소화:
  - `ontology-management-service` → `oms`
  - `backend-for-frontend` → `bff`

## [1.0.0] - 2025-07-17

### 초기 릴리즈
- sys.path.insert 제거 완료
- 표준 Python 패키지 구조 채택
- 마이크로서비스 아키텍처 구현:
  - OMS (Ontology Management Service)
  - BFF (Backend for Frontend)
  - Funnel (Type Inference Service)
  - Shared Components

### 주요 기능
- 온톨로지 관리
- 복합 타입 시스템
- 관계 관리
- 다국어 지원
- Google Sheets 연동

## [0.1.0] - 2025-07-01

### 프로젝트 시작
- 초기 프로토타입 개발
- TerminusDB 통합
- 기본 CRUD 작업 구현

---

## 버전 관리 정책

- **Major (X.0.0)**: 구조적 변경, 호환성 깨짐
- **Minor (0.X.0)**: 새 기능 추가, 하위 호환성 유지
- **Patch (0.0.X)**: 버그 수정, 문서 업데이트

## 관련 링크

- [현재 아키텍처](./docs/CURRENT_ARCHITECTURE.md)
- [상세 아키텍처](./docs/DETAILED_ARCHITECTURE.md)
- [배포 가이드](./backend/docs/deployment/DEPLOYMENT_GUIDE.md)