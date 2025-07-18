# 보안 설정 가이드

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

## 입력 검증

모든 사용자 입력은 다음을 사용하여 검증됩니다:
- `validate_db_name()` - 데이터베이스 이름 검증
- `validate_class_id()` - 온톨로지 클래스 ID 검증
- `sanitize_input()` - 일반 입력 삭제

## 보안 헤더

애플리케이션은 모든 HTTP 요청에 보안 헤더를 추가합니다:
- `X-Request-ID` - 요청 추적 및 디버깅용
- `User-Agent` - 요청을 만드는 서비스 식별

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
- [ ] 모든 자격 증명이 환경 변수에서 로드됨
- [ ] 소스 코드에 하드코딩된 비밀 없음
- [ ] 모든 외부 연결에 HTTPS가 활성화됨
- [ ] 모든 엔드포인트에서 입력 검증이 활성화됨
- [ ] 오류 메시지가 민감한 정보를 노출하지 않음
- [ ] 로깅에 자격 증명이나 민감한 데이터가 포함되지 않음
- [ ] 속도 제한이 구성됨 (해당하는 경우)
- [ ] CORS가 적절히 구성됨
- [ ] 데이터베이스 연결이 SSL/TLS를 사용함
- [ ] 정기적인 보안 업데이트가 예정됨

## 보안 문제 신고

보안 취약점을 발견하면 다음으로 신고해 주세요:
- 이메일: security@spice-harvester.com
- 보안 취약점에 대해 공개 GitHub 이슈를 생성하지 마세요