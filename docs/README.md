# SPICE HARVESTER 문서

SPICE HARVESTER 프로젝트 문서에 오신 것을 환영합니다. 이 디렉토리에는 카테고리별로 정리된 모든 프로젝트 문서가 포함되어 있습니다.

## 📚 문서 구조

### 🏗️ 아키텍처
- [상세 아키텍처 문서 (한글)](DETAILED_ARCHITECTURE.md) - 시스템 아키텍처 전체 개요
- [아키텍처 다이어그램](architecture/README.md) - Mermaid 형식의 시각화된 아키텍처
  - [서비스 상호작용 다이어그램](architecture/service_interactions.mmd)
  - [데이터 흐름 다이어그램](architecture/data_flow.mmd)
  - [백엔드 클래스 다이어그램](architecture/backend_classes.mmd)

### 🛠️ 개발 가이드
- [프론트엔드 개발 가이드 (영어)](development/FRONTEND_DEVELOPMENT_GUIDE.md)
- [완전한 프론트엔드 개발 가이드](development/COMPLETE_FRONTEND_DEVELOPMENT_GUIDE.md)
- [완전한 프론트엔드 개발 가이드 (한글)](development/완전한_프론트엔드_개발_가이드.md)
- [데이터베이스 목록 수정 요약](development/DATABASE_LIST_FIX_SUMMARY.md)

### 🚀 배포
- [배포 가이드 (한글)](deployment/DEPLOYMENT_GUIDE.md) - Docker 기반 프로덕션 배포
- [마이그레이션 가이드](../backend/MIGRATION_GUIDE.md) - sys.path 제거 마이그레이션 (완료)
- [포트 설정 가이드](../backend/PORT_CONFIGURATION.md) - 서비스별 포트 설정

### 🧪 테스트
- [복잡한 타입 테스트 문서](testing/COMPLEX_TYPES_TEST_README.md) - 10가지 복합 타입 테스트
- [OMS 프로덕션 테스트 문서](testing/OMS_PRODUCTION_TEST_README.md) - 프로덕션 준비 테스트
- [테스트 조직 보고서](testing/TEST_ORGANIZATION_REPORT.md) *(존재하는 경우)*

### 🔒 보안
- [보안 문서](security/SECURITY.md) - 보안 모범 사례 및 가이드라인

### 📋 요구사항
- [통합 요구사항](requirements/requirements.txt)
- [BFF 요구사항](requirements/bff-requirements.txt)
- [OMS 요구사항](requirements/oms-requirements.txt)
- [Funnel 요구사항](requirements/funnel-requirements.txt)
- [테스트 요구사항](requirements/tests-requirements.txt)

## 📖 빠른 링크

### 개발자를 위한 링크
1. [상세 아키텍처 문서](DETAILED_ARCHITECTURE.md)로 시스템 이해하기
2. [프론트엔드 개발 가이드](development/FRONTEND_DEVELOPMENT_GUIDE.md)로 시작하기
3. [보안 문서](security/SECURITY.md) 검토하기
4. [포트 설정 가이드](../backend/PORT_CONFIGURATION.md) 확인하기

### 테스터를 위한 링크
1. [복잡한 타입 테스트 문서](testing/COMPLEX_TYPES_TEST_README.md) - 10가지 복합 타입
2. [OMS 프로덕션 테스트 문서](testing/OMS_PRODUCTION_TEST_README.md) 검토하기

### DevOps를 위한 링크
1. [배포 가이드](deployment/DEPLOYMENT_GUIDE.md) 따라하기
2. [마이그레이션 완료 문서](../backend/MIGRATION_GUIDE.md) 확인하기
3. 의존성에 대한 모든 [요구사항](requirements/) 확인하기

## 🔍 문서 표준

### 파일 명명 규칙
- 주요 가이드는 대문자 사용 (예: `DEPLOYMENT_GUIDE.md`)
- 언더스코어를 사용한 설명적인 이름
- 번역된 문서는 언어 접미사 포함 (예: `_KR.md`)

### 콘텐츠 구조
- 긴 문서에는 항상 목차 포함
- 명확한 섹션 헤더 사용
- 해당하는 경우 코드 예제 포함
- 코드 변경사항과 함께 문서 최신 상태 유지

## 🤝 문서 기여하기

새로운 문서를 추가할 때:
1. 적절한 카테고리 디렉토리에 배치
2. 이 README.md에 문서 링크 업데이트
3. 기존 형식 표준 따르기
4. 생성 및 최종 업데이트 날짜 포함

## 🌟 주요 변경사항 (2025-07-22)

### 현재 구조
- **플랫 구조**: `backend/bff/`, `backend/oms/`, `backend/funnel/`, `backend/shared/`
- **Import 방식**: `from shared.models...` (sys.path.insert 사용 안함)
- **포트 설정**: 환경변수로 중앙 관리

### 서비스 이름 변경
- `backend-for-frontend` → `bff`
- `ontology-management-service` → `oms`
- 새 서비스 추가: `funnel` (타입 추론 서비스)

### 포트 할당
- **OMS**: 8000
- **BFF**: 8002  
- **Funnel**: 8003
- **TerminusDB**: 6363

### 마이그레이션 완료
- ✅ 모든 sys.path.insert 제거
- ✅ 표준 Python import 구조 적용
- ✅ IDE 자동완성 및 타입 체킹 지원

### 주요 기능
- **타입 추론**: Google Sheets, CSV 등에서 자동 스키마 제안
- **복합 타입**: EMAIL, PHONE, URL, MONEY 등 10가지 타입 지원
- **HTTPS/CORS**: 자동 CORS 설정, HTTPS 지원
- **포트 관리**: 환경변수 기반 중앙화된 포트 설정
- **Property-to-Relationship 자동 변환**: 클래스 속성을 관계로 자동 변환
- **고급 제약조건 시스템**: 상세한 제약조건 추출 및 검증
- **TerminusDB v11.x 완전 지원**: OneOfType, Foreign, GeoPoint 등 모든 타입

---
*최종 업데이트: 2025-07-22*