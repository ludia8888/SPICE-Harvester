# 🔥 SPICE HARVESTER CORS Configuration Guide

## 📋 목차

1. [개요](#개요)
2. [CORS 설정 방법](#cors-설정-방법)
3. [환경별 설정](#환경별-설정)
4. [서비스별 설정](#서비스별-설정)
5. [테스트 및 디버깅](#테스트-및-디버깅)
6. [문제 해결](#문제-해결)
7. [보안 고려사항](#보안-고려사항)

---

## 🌍 개요

SPICE HARVESTER는 환경변수 기반의 **동적 CORS 설정**을 지원합니다. 이를 통해 개발 환경에서는 유연하게, 프로덕션 환경에서는 안전하게 CORS를 관리할 수 있습니다.

### 🎯 주요 특징

- ✅ **환경별 자동 설정**: 개발/스테이징/프로덕션 환경에 따른 자동 설정
- ✅ **동적 포트 지원**: 프론트엔드 개발에서 사용하는 일반적인 포트들 자동 허용
- ✅ **중앙 집중식 관리**: `ServiceConfig` 클래스를 통한 통일된 설정 관리
- ✅ **실시간 디버깅(옵트인)**: `ENABLE_DEBUG_ENDPOINTS=true`일 때 각 서비스의 `/debug/cors`로 설정 확인
- ✅ **보안 강화**: 프로덕션 환경에서 제한적인 헤더 및 도메인 허용

---

## 🚀 빠른 시작 (Quick Start)

### 즉시 테스트하기

**1단계: 환경변수 설정** (선택사항)
```bash
# .env 파일 생성 (기본 설정으로도 동작함)
cp .env.example .env

# 또는 직접 설정
export ENVIRONMENT=development
export CORS_ENABLED=true
```

**2단계: 서비스 시작**
```bash
# 모든 서비스 시작
python backend/start_services.py --env development
# 또는: cd backend && python start_services.py --env development

# 또는 개별 시작
python -m bff.main &
python -m oms.main &
python -m funnel.main &
```

**3단계: CORS 설정 확인**
```bash
# Debug endpoints는 기본값으로 비활성화되어 있습니다(보안/표면적 최소화).
export ENABLE_DEBUG_ENDPOINTS=true

# 각 서비스의 CORS 설정 확인 (development only)
curl http://localhost:8002/debug/cors | jq  # BFF
curl http://localhost:8000/debug/cors | jq  # OMS
curl http://localhost:8003/debug/cors | jq  # Funnel
```

**4단계: 자동 테스트 실행**
```bash
# 의존성 설치 (필요시)
pip install aiohttp colorama

# CORS 테스트 실행
python test_cors_configuration.py
```

---

## ⚙️ CORS 설정 방법

### 1. 환경변수 설정

`.env` 파일에 다음과 같이 설정하세요:

```bash
# 환경 설정
ENVIRONMENT=development  # development, staging, production

# CORS 설정
CORS_ENABLED=true
CORS_ORIGINS=["http://localhost:3000", "http://localhost:3001", "http://localhost:5173"]
```

### 2. 자동 설정 (권장)

환경변수를 설정하지 않으면 다음과 같이 자동 설정됩니다:

#### 개발 환경 (development)
```python
# 자동으로 허용되는 포트들
origins = [
    "http://localhost:3000",   # React 기본 포트
    "http://localhost:3001",   # React 대체 포트
    "http://localhost:3002",   # React 대체 포트
    "http://localhost:5173",   # Vite 기본 포트
    "http://localhost:5174",   # Vite 대체 포트
    "http://localhost:8080",   # Vue.js 기본 포트
    "http://localhost:8081",   # Vue.js 대체 포트
    "http://localhost:8082",   # Vue.js 대체 포트
    "http://localhost:4200",   # Angular 기본 포트
    "http://localhost:4201",   # Angular 대체 포트
    "http://127.0.0.1:3000",   # 로컬 IP
    "https://localhost:3000",  # HTTPS 버전
    "*"                        # 최대 유연성
]
```

#### 프로덕션 환경 (production)
```python
# 명시적으로 허용된 도메인만
origins = [
    "https://app.spice-harvester.com",
    "https://www.spice-harvester.com",
    "https://spice-harvester.com"
]
```

---

## 🎨 환경별 설정

### 개발 환경 설정

```bash
# .env.development
ENVIRONMENT=development
CORS_ENABLED=true
# CORS_ORIGINS는 설정하지 않음 (자동 설정 사용)
```

### 스테이징 환경 설정

```bash
# .env.staging
ENVIRONMENT=staging
CORS_ENABLED=true
CORS_ORIGINS=["https://staging.spice-harvester.com", "https://test.spice-harvester.com"]
```

### 프로덕션 환경 설정

```bash
# .env.production
ENVIRONMENT=production
CORS_ENABLED=true
CORS_ORIGINS=["https://app.spice-harvester.com", "https://www.spice-harvester.com"]
```

---

## 🔧 서비스별 설정

### BFF (Backend for Frontend)
- **포트**: 8002
- **주요 용도**: 프론트엔드와 직접 통신
- **CORS 설정**: 가장 관대한 설정 (프론트엔드 개발 지원)

### OMS (Ontology Management Service)
- **포트**: 8000
- **주요 용도**: 내부 API (BFF를 통해 접근)
- **CORS 설정**: 중간 수준의 보안 설정

### Funnel (Type Inference Service)
- **포트**: 8003
- **주요 용도**: 타입 추론 전용 서비스
- **CORS 설정**: 필요에 따라 제한적 설정

---

## 🔍 테스트 및 디버깅

### 1. CORS 디버그 엔드포인트

각 서비스는 개발 환경에서 다음 엔드포인트를 제공합니다:

```bash
# BFF 서비스
curl http://localhost:8002/debug/cors

# OMS 서비스
curl http://localhost:8000/debug/cors

# Funnel 서비스
curl http://localhost:8003/debug/cors
```

### 2. 자동 테스트 스크립트

```bash
# CORS 설정 테스트 실행
python test_cors_configuration.py
```

이 스크립트는 다음을 수행합니다:
- 모든 서비스의 CORS 설정 확인
- 다양한 Origin에서 preflight 및 실제 요청 테스트
- 결과를 `cors_test_results.json`에 저장

### 3. 수동 테스트

```bash
# Preflight 요청 테스트
curl -X OPTIONS \\
     -H "Origin: http://localhost:3000" \\
     -H "Access-Control-Request-Method: GET" \\
     -H "Access-Control-Request-Headers: Content-Type" \\
     http://localhost:8002/api/v1/health -v

# 실제 요청 테스트
curl -X GET \\
     -H "Origin: http://localhost:3000" \\
     -H "Content-Type: application/json" \\
     http://localhost:8002/api/v1/health -v
```

---

## 🚨 문제 해결

### CORS 에러 발생 시

1. **서비스 상태 확인**
   ```bash
   curl http://localhost:8002/api/v1/health
   curl http://localhost:8000/health
   curl http://localhost:8003/health
   ```

2. **CORS 설정 확인**
   ```bash
   export ENABLE_DEBUG_ENDPOINTS=true
   curl http://localhost:8002/debug/cors
   ```

3. **특정 포트 허용**
   ```bash
   export CORS_ORIGINS='["http://localhost:3000", "http://localhost:YOUR_PORT"]'
   ```

### 자주 발생하는 문제

#### 1. CORS 에러: "Access to fetch at ... has been blocked by CORS policy"

**해결 방법**:
1. 서비스가 실행 중인지 확인
2. 환경변수 설정 확인
3. 디버그 엔드포인트로 현재 설정 확인

```bash
# 현재 CORS 설정 확인
curl http://localhost:8002/debug/cors
```

#### 2. 특정 포트에서만 CORS 에러 발생

**해결 방법**:
1. `.env` 파일에 해당 포트 추가
2. 또는 개발 환경에서 `CORS_ORIGINS` 설정 제거 (자동 설정 사용)

```bash
# .env 파일에 추가
CORS_ORIGINS=["http://localhost:3000", "http://localhost:YOUR_PORT"]
```

#### 3. 프로덕션에서 CORS 에러

**해결 방법**:
1. 프로덕션 도메인이 `CORS_ORIGINS`에 포함되어 있는지 확인
2. HTTPS 사용 중인지 확인 (HTTP와 HTTPS는 다른 origin)

```bash
# 프로덕션 설정 예시
CORS_ORIGINS=["https://yourdomain.com", "https://www.yourdomain.com"]
```

### 디버깅 팁

1. **브라우저 개발자 도구**: Network 탭에서 CORS 헤더 확인
2. **서버 로그**: 서비스 시작 시 CORS 설정 로그 확인
3. **테스트 스크립트**: 자동화된 테스트로 전체 설정 검증

---

## 🔒 보안 고려사항

### 개발 환경

```javascript
// 개발 환경에서는 유연한 설정 사용
const corsConfig = {
  allow_origins: ["*"],  // 모든 origin 허용
  allow_methods: ["*"],  // 모든 HTTP 메서드 허용
  allow_headers: ["*"],  // 모든 헤더 허용
  max_age: 3600         // 1시간 캐시
};
```

### 프로덕션 환경

```javascript
// 프로덕션 환경에서는 제한적 설정 사용
const corsConfig = {
  allow_origins: ["https://yourdomain.com"],  // 특정 도메인만 허용
  allow_methods: ["GET", "POST", "PUT", "DELETE"],  // 필요한 메서드만
  allow_headers: ["Accept", "Content-Type", "Authorization"],  // 필요한 헤더만
  max_age: 86400  // 24시간 캐시
};
```

### 권장사항

1. **절대 프로덕션에서 `*` 사용 금지**
2. **HTTPS 사용**: 프로덕션에서는 항상 HTTPS 사용
3. **최소 권한 원칙**: 필요한 origin, 메서드, 헤더만 허용
4. **정기적인 검토**: CORS 설정을 정기적으로 검토하고 업데이트

---

## 📚 참고 자료

### 관련 파일

- `shared/config/service_config.py` - CORS 설정 메서드
- `bff/main.py` - BFF 서비스 CORS 설정
- `oms/main.py` - OMS 서비스 CORS 설정
- `funnel/main.py` - Funnel 서비스 CORS 설정
- `test_cors_configuration.py` - CORS 테스트 스크립트

### 유용한 명령어

```bash
# 모든 서비스 시작
python backend/start_services.py --env development

# CORS 테스트 실행
python test_cors_configuration.py

# 특정 서비스 CORS 설정 확인
export ENABLE_DEBUG_ENDPOINTS=true
curl http://localhost:8002/debug/cors | jq

# 서비스 상태 확인
curl http://localhost:8002/api/v1/health
curl http://localhost:8000/health
curl http://localhost:8003/health
```

---

## 🔄 업데이트 로그

| 날짜 | 버전 | 변경사항 |
|------|------|----------|
| 2025-01-18 | 1.0.0 | 초기 환경변수 기반 CORS 설정 구현 |
| 2025-01-18 | 1.1.0 | 디버그 엔드포인트 및 테스트 스크립트 추가 |

---

이 가이드를 통해 SPICE HARVESTER의 CORS 설정을 완벽하게 이해하고 관리할 수 있습니다. 추가 질문이나 문제가 있으면 개발팀에 문의하세요.

**🔥 THINK ULTRA!** - 완벽한 CORS 설정으로 안전하고 유연한 프론트엔드 개발을 지원합니다!
