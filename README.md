# 🌶️ SPICE HARVESTER

다국어 지원, 복잡한 데이터 타입, 관계 관리 기능을 갖춘 종합적인 온톨로지 관리 시스템입니다.

## 📋 목차
- [개요](#개요)
- [아키텍처](#아키텍처)
- [시작하기](#시작하기)
- [문서](#문서)
- [테스트](#테스트)
- [배포](#배포)
- [기여하기](#기여하기)

## 🎯 개요

SPICE HARVESTER는 다음과 같은 기능을 제공하는 정교한 온톨로지 관리 플랫폼입니다:
- **다국어 지원**: 라벨과 설명에서 여러 언어를 완벽하게 지원
- **복잡한 데이터 타입**: ARRAY, OBJECT, ENUM, MONEY 등 10개 이상의 복잡한 데이터 타입
- **관계 관리**: 순환 참조 감지 기능을 갖춘 고급 양방향 관계 처리
- **Property-to-Relationship 자동 변환**: 클래스 내부 속성을 관계로 자동 변환
- **고급 제약조건 시스템**: 상세한 제약조건 추출 및 검증 (min/max, pattern, cardinality 등)
- **TerminusDB v11.x 완전 지원**: OneOfType, Foreign, GeoPoint 등 모든 스키마 타입 지원
- **프로덕션 준비 완료**: 종합적인 테스트, 보안 기능, 성능 최적화

## 🏗️ 아키텍처

시스템은 네 가지 주요 구성 요소로 이루어져 있습니다:

### 1. 온톨로지 관리 서비스 (OMS)
- TerminusDB와의 직접 인터페이스
- 모든 데이터베이스 작업 처리
- 온톨로지 스키마 및 인스턴스 관리
- Property-to-Relationship 자동 변환 기능
- 고급 제약조건 추출 및 검증
- TerminusDB v11.x 복잡한 스키마 타입 완전 지원
- Port: 8000

### 2. 프론트엔드를 위한 백엔드 (BFF)
- 프론트엔드 애플리케이션을 위한 API 게이트웨이
- 인증 및 권한 부여 처리
- 데이터 변환 및 집계 제공
- Port: 8001

### 3. 타입 추론 서비스 (Funnel)
- 데이터 타입 자동 추론
- 스키마 제안 및 검증
- Google Sheets 등 외부 데이터 소스 분석
- Port: 8003

### 4. 공유 컴포넌트
- 공통 모델 및 유틸리티
- 복잡한 타입 시스템
- 검증 프레임워크
- 서비스 설정 및 의존성 관리

## 🚀 시작하기

### 사전 요구 사항
- Python 3.9+
- TerminusDB 10.1.8
- Docker (선택 사항)

### 빠른 시작
```bash
# 저장소 복제
git clone [repository-url]
cd SPICE-HARVESTER

# 의존성 설치
pip install -r requirements.txt

# TerminusDB 시작
docker run -d -p 6363:6363 terminusdb/terminusdb:10.1.8

# 서비스 실행
cd backend/oms && python main.py
cd backend/bff && python main.py
cd backend/funnel && python main.py

# 또는 모든 서비스 한번에 시작
cd backend && python start_services.py
```

## 📚 문서

모든 문서는 `docs/` 디렉토리에 정리되어 있습니다:

- **[개발 가이드](docs/development/)**: 프론트엔드 및 백엔드 개발 가이드
- **[배포 가이드](docs/deployment/DEPLOYMENT_GUIDE.md)**: 프로덕션 배포 지침
- **[테스트 문서](docs/testing/)**: 테스트 전략 및 가이드
- **[보안 문서](docs/security/SECURITY.md)**: 보안 모범 사례
- **[API 문서](docs/api/)**: API 엔드포인트 및 사용법 *(준비 중)*

전체 문서 색인은 [docs/README.md](docs/README.md)를 참조하세요.

## 🧪 테스트

프로젝트는 포괄적인 테스트 커버리지를 포함합니다:

```bash
# 모든 테스트 실행
cd backend && python tests/runners/run_comprehensive_tests.py

# 특정 테스트 스위트 실행
pytest tests/unit/                    # 유닛 테스트
pytest tests/integration/              # 통합 테스트
pytest tests/performance/              # 성능 테스트
```

테스트 구성:
- `tests/unit/`: 개별 컴포넌트에 대한 유닛 테스트
- `tests/integration/`: 서비스 상호작용에 대한 통합 테스트
- `tests/performance/`: 성능 및 부하 테스트
- `tests/system/`: 전체 시스템 테스트

## 🚢 배포

프로덕션 배포에 대해서는 [배포 가이드](docs/deployment/DEPLOYMENT_GUIDE.md)를 참조하세요.

주요 배포 고려사항:
- 구성에 환경 변수 사용
- 프로덕션에서 HTTPS 활성화
- 적절한 로깅 및 모니터링 설정
- 데이터베이스 백업 구성

## 🤝 기여하기

기여를 환영합니다! 다음 가이드라인을 따라주세요:

1. 저장소 포크
2. 기능 브랜치 생성 (`git checkout -b feature/amazing-feature`)
3. 변경사항 커밋 (`git commit -m 'Add amazing feature'`)
4. 브랜치에 푸시 (`git push origin feature/amazing-feature`)
5. Pull Request 열기

### 코드 스타일
- Python 코드는 PEP 8 따르기
- 적용 가능한 곳에 타입 힌트 사용
- 새로운 기능에 대한 포괄적인 테스트 작성
- 필요에 따라 문서 업데이트

## 📄 라이선스

[라이선스 정보 추가 예정]

## 🙏 감사의 말

- FastAPI와 TerminusDB로 구축
- 현대적인 온톨로지 관리 모범 사례에서 영감을 받음

---
*더 많은 정보는 [전체 문서](docs/README.md)를 참조하세요.*