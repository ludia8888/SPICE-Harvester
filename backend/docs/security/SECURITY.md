# 🔒 SPICE HARVESTER 보안 설정 가이드

최종 업데이트: 2025-07-20

## 시스템 구성

SPICE HARVESTER는 다음 서비스들로 구성되어 있습니다:
- **OMS (Ontology Management Service)** - 포트 8000
- **BFF (Backend for Frontend)** - 포트 8002  
- **Funnel (Type Inference Service)** - 포트 8003
- **TerminusDB** - 포트 6363

## 인증

### TerminusDB 인증
시스템은 TerminusDB에서 요구하는 대로 TerminusDB 연결에 HTTP 기본 인증을 사용합니다.

#### 보안 모범 사례:

1. **환경 변수 사용**
   - 소스 코드에 자격 증명을 하드코딩하지 마세요
   - 환경 변수에 `TERMINUS_USER`와 `TERMINUS_KEY` 설정
   - 로컬 개발을 위해 `.env` 파일 사용 (git에 커밋하지 마세요)

2. **프로덕션에서 HTTPS 사용**
   - 프로덕션에서는 항상 TerminusDB에 `https://` URL 사용
   - localhost가 아닌 연결에 HTTP를 사용하면 시스템이 경고합니다

3. **자격 증명 로테이션**
   - TerminusDB 자격 증명을 정기적으로 교체
   - 자격 증명 변경 시 환경 변수 업데이트

## 입력 검증 및 보안

### 입력 삭제 (Input Sanitization)
시스템은 `shared/security/input_sanitizer.py`를 통해 포괄적인 입력 삭제를 제공합니다:

```python
from shared.security.input_sanitizer import InputSanitizer

# 문자열 삭제
clean_text = InputSanitizer.sanitize_string(user_input)

# JSON 데이터 삭제
clean_data = InputSanitizer.sanitize_json(json_data)

# 파일명 삭제
safe_filename = InputSanitizer.sanitize_filename(filename)

# URL 검증
if InputSanitizer.validate_url(url):
    # URL이 안전함
```

주요 기능:
- HTML/JavaScript 인젝션 방지
- SQL 인젝션 방지  
- XSS 공격 방지
- 경로 순회 공격 방지
- 유니코드 정규화

### 데이터베이스 이름 및 클래스 ID 검증
- `validate_db_name()` - 데이터베이스 이름 검증
- `validate_class_id()` - 온톨로지 클래스 ID 검증
- 특수 문자 및 예약어 차단

## 보안 헤더

애플리케이션은 모든 HTTP 요청에 보안 헤더를 추가합니다:
- `X-Request-ID` - 요청 추적 및 디버깅용
- `User-Agent` - 요청을 만드는 서비스 식별

## HTTPS 구성

### 개발 환경 HTTPS
개발 환경에서 자체 서명 인증서를 사용할 수 있습니다:

```bash
# SSL 인증서 생성
./generate_ssl_certs.sh

# HTTPS로 서비스 시작
USE_HTTPS=true docker-compose -f docker-compose-https.yml up -d
```

### 프로덕션 HTTPS
프로덕션에서는 유효한 SSL 인증서를 사용해야 합니다:

```bash
# 환경 변수 설정
USE_HTTPS=true
SSL_CERT_PATH=/path/to/cert.pem
SSL_KEY_PATH=/path/to/key.pem
VERIFY_SSL=true
```

## CORS (Cross-Origin Resource Sharing)

### 자동 CORS 설정
개발 환경에서는 일반적인 프론트엔드 포트가 자동으로 허용됩니다:
- React: 3000, 3001, 3002
- Vite: 5173, 5174
- Angular: 4200
- Vue.js: 8080

### 프로덕션 CORS 설정
```bash
# .env 파일
CORS_ORIGINS=https://yourdomain.com,https://app.yourdomain.com
```

### 프로그래밍 방식 CORS 설정
```python
from shared.config.service_config import ServiceConfig

# 허용된 origin 가져오기
allowed_origins = ServiceConfig.get_cors_origins()
```

## 환경 변수

필수 보안 관련 환경 변수:
```bash
# TerminusDB 자격 증명 (필수)
TERMINUS_USER=admin
TERMINUS_KEY=your_secure_key_here  # 강력한 키 생성

# JWT/세션 보안 (인증 기능에 필수)
SECRET_KEY=your-secret-key-here  # 다음을 사용하여 생성: openssl rand -hex 32
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30

# HTTPS 구성 (프로덕션)
TERMINUS_SERVER_URL=https://your-terminus-server.com
```

## 안전한 키 생성

### JWT용 비밀 키 생성:
```bash
openssl rand -hex 32
```

### 강력한 비밀번호 생성:
```bash
openssl rand -base64 32
```

## 보안 체크리스트

프로덕션 배포 전:

### 자격 증명 및 비밀
- [ ] 모든 자격 증명이 환경 변수에서 로드됨
- [ ] 소스 코드에 하드코딩된 비밀 없음
- [ ] SECRET_KEY가 강력하고 고유함
- [ ] 기본 비밀번호가 변경됨

### HTTPS/TLS
- [ ] 모든 서비스가 HTTPS를 통해 제공됨
- [ ] 유효한 SSL 인증서 설치됨
- [ ] 모든 외부 연결에 HTTPS가 활성화됨
- [ ] VERIFY_SSL이 프로덕션에서 true로 설정됨

### 입력 검증 및 보안
- [ ] InputSanitizer가 모든 사용자 입력에 적용됨
- [ ] 모든 엔드포인트에서 입력 검증이 활성화됨
- [ ] 파일 업로드 크기 제한이 설정됨
- [ ] 허용된 파일 확장자만 허용됨

### CORS 및 네트워크
- [ ] CORS가 특정 도메인만 허용하도록 구성됨
- [ ] 내부 서비스 포트가 외부에 노출되지 않음
- [ ] 방화벽 규칙이 적절히 설정됨

### 로깅 및 모니터링  
- [ ] 오류 메시지가 민감한 정보를 노출하지 않음
- [ ] 로깅에 자격 증명이나 민감한 데이터가 포함되지 않음
- [ ] 로그 파일이 안전하게 저장됨
- [ ] 보안 이벤트 모니터링이 설정됨

### 서비스별 보안
- [ ] OMS: TerminusDB 연결이 보안됨
- [ ] BFF: 레이블 매핑 데이터가 보호됨
- [ ] Funnel: 타입 추론 요청이 검증됨
- [ ] 모든 서비스 간 통신이 내부 네트워크에서만 발생함

### 기타
- [ ] 속도 제한이 구성됨 (해당하는 경우)
- [ ] 정기적인 보안 업데이트가 예정됨
- [ ] 백업 및 복구 절차가 테스트됨
- [ ] 침입 감지 시스템이 활성화됨

## 보안 문제 신고

보안 취약점을 발견하면 다음으로 신고해 주세요:
- 이메일: security@spice-harvester.com
- 보안 취약점에 대해 공개 GitHub 이슈를 생성하지 마세요