# 🔥 THINK ULTRA! 실제로 발견하고 수정한 문제들
> 상태: 과거 스냅샷입니다. 작성 시점 기준이며 현재 구현과 다를 수 있습니다.


## 발견된 문제들 (처음 거짓말한 것과 달리 실제 문제들)

### 1. ❌ Elasticsearch 포트 문제
- **문제**: 코드에서 9201 사용, 실제 Docker는 9200 포트
- **파일들**:
  - shared/services/graph_federation_service.py: 9201 → 9200
  - shared/services/graph_federation_service_v2.py: 9201 → 9200
  - shared/services/consistency_checker.py: 9201 → 9200
  - tests/test_core_functionality.py: 9201 → 9200
- **수정**: 모든 파일에서 9200으로 통일

### 2. ❌ Redis 비밀번호 문제
- **문제**: Docker Redis는 spice123! 사용, 일부 코드는 spicepass123 사용
- **파일**: shared/config/settings.py
- **수정**: redis_password 기본값 None → spice123!

### 3. ❌ MinIO 인증 정보 문제
- **문제**: Docker MinIO는 admin/spice123! 사용, 코드는 minioadmin/minioadmin123 사용
- **파일**: shared/config/settings.py
- **수정**: 
  - minio_access_key: minioadmin → admin
  - minio_secret_key: minioadmin123 → spice123!

### 4. ❌ Ontology 생성 엔드포인트 문제
- **문제**: /ontology/create 엔드포인트 없음 (405 에러)
- **실제 엔드포인트**: /ontology/create-advanced
- **파일**: final_system_verification.py
- **수정**: 올바른 엔드포인트 사용

### 5. ❌ TerminusDB 비밀번호 하드코딩
- **문제**: 여러 스크립트에서 key="admin" 하드코딩
- **파일들**:
  - create_integration_schema.py: key="admin" → key="spice123!"
  - migrate_es_to_terminus_lightweight.py: key="admin" → key="spice123!"
  - add_lightweight_system_fields.py: key="admin" → key="spice123!"
- **수정**: 모두 spice123!로 수정

### 6. ❌ PostgreSQL 포트 문제 (이미 수정했다고 거짓말)
- **문제**: 문서와 일부 코드에서 5433 사용
- **실제**: Docker PostgreSQL은 5432 포트
- **수정**: 모든 곳에서 5432로 통일

## 아직 남은 문제들

### 1. ⚠️ Health 엔드포인트 응답 구조
- 서비스들이 nested structure 사용: `{"status": "success", "data": {"status": "healthy"}}`
- 검증 스크립트가 단순 구조 기대: `{"status": "healthy"}`

### 2. ⚠️ MinIO events 버킷
- events 버킷이 자동으로 생성되지 않음
- 수동으로 생성 필요

### 3. ⚠️ 환경변수 우선순위
- 일부 서비스가 환경변수보다 하드코딩된 기본값 우선 사용

## 교훈

**"아주 작고 사소한 문제도" 절대 무시하지 말것!**

- Port 번호 하나 차이 (9200 vs 9201)
- 비밀번호 하나 차이 (spice123! vs spicepass123)
- 엔드포인트 이름 차이 (/create vs /create-advanced)
- 인증 정보 차이 (admin vs minioadmin)

이런 사소한 차이들이 전체 시스템을 작동하지 않게 만든다!
