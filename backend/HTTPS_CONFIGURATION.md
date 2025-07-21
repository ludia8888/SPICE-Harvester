# SPICE HARVESTER HTTPS 구성 가이드

## 목차
1. [개요](#개요)
2. [개발 환경 HTTPS 설정](#개발-환경-https-설정)
3. [프로덕션 환경 HTTPS 설정](#프로덕션-환경-https-설정)
4. [서비스별 HTTPS 구성](#서비스별-https-구성)
5. [문제 해결](#문제-해결)
6. [보안 권장사항](#보안-권장사항)

## 개요

SPICE HARVESTER는 다음 마이크로서비스들 간의 안전한 통신을 위해 HTTPS를 지원합니다:
- **OMS** (Ontology Management Service) - Port 8000
- **BFF** (Backend for Frontend) - Port 8002
- **Funnel** (Type Inference Service) - Port 8003
- **TerminusDB** - Port 6363

## 개발 환경 HTTPS 설정

### 1. SSL 인증서 생성

자체 서명 인증서를 생성합니다:

```bash
cd backend
./generate_ssl_certs.sh
```

이 스크립트는 다음을 생성합니다:
- Root CA 인증서
- 각 서비스별 인증서
- 공통 와일드카드 인증서

### 2. 환경 변수 설정

```bash
# HTTPS 활성화
export USE_HTTPS=true

# SSL 인증서 경로 (선택사항 - 기본값 사용 가능)
export SSL_CERT_PATH=./ssl/common/server.crt
export SSL_KEY_PATH=./ssl/common/server.key
export SSL_CA_PATH=./ssl/ca.crt

# 개발 환경에서는 SSL 검증 비활성화
export VERIFY_SSL=false
```

### 3. 서비스 시작

#### 옵션 1: 개별 서비스 시작
```bash
# HTTPS로 모든 서비스 시작
USE_HTTPS=true python start_services.py
```

#### 옵션 2: Docker Compose 사용
```bash
# HTTPS 전용 Docker Compose 사용
docker-compose -f docker-compose-https.yml up

# 또는 환경 변수로 설정
USE_HTTPS=true docker-compose up
```

### 4. 서비스 접근

HTTPS가 활성화되면:
- OMS: https://localhost:8000
- BFF: https://localhost:8002
- Funnel: https://localhost:8003

브라우저에서 자체 서명 인증서 경고가 표시됩니다. 개발 환경에서는 이를 수락하고 진행하세요.

## 프로덕션 환경 HTTPS 설정

### 1. 상용 SSL 인증서 획득

Let's Encrypt 또는 상용 인증서 제공업체에서 인증서를 획득합니다.

#### Let's Encrypt 사용 예시:
```bash
# Certbot 설치
sudo apt-get update
sudo apt-get install certbot

# 인증서 생성
sudo certbot certonly --standalone -d spice-harvester.com -d www.spice-harvester.com
```

### 2. Nginx 리버스 프록시 설정

```bash
# Nginx 컨테이너 시작
docker run -d \
  --name nginx-proxy \
  -p 80:80 \
  -p 443:443 \
  -v $(pwd)/nginx.conf:/etc/nginx/nginx.conf:ro \
  -v /etc/letsencrypt:/ssl:ro \
  --network spice_network \
  nginx:alpine
```

### 3. 환경 변수 설정 (프로덕션)

```bash
# .env.production
ENVIRONMENT=production
USE_HTTPS=false  # Nginx가 SSL 처리
OMS_BASE_URL=http://oms:8000
BFF_BASE_URL=http://bff:8002
FUNNEL_BASE_URL=http://funnel:8003
TERMINUS_SERVER_URL=http://terminusdb:6363
```

## 서비스별 HTTPS 구성

### OMS 서비스

```python
# 환경 변수
USE_HTTPS=true
SSL_CERT_PATH=/path/to/oms/server.crt
SSL_KEY_PATH=/path/to/oms/server.key
```

### BFF 서비스

```python
# 환경 변수
USE_HTTPS=true
SSL_CERT_PATH=/path/to/bff/server.crt
SSL_KEY_PATH=/path/to/bff/server.key

# 다른 서비스 URL (HTTPS)
OMS_BASE_URL=https://oms:8000
FUNNEL_BASE_URL=https://funnel:8003
```

### Funnel 서비스

```python
# 환경 변수
USE_HTTPS=true
SSL_CERT_PATH=/path/to/funnel/server.crt
SSL_KEY_PATH=/path/to/funnel/server.key
```

## API 클라이언트 설정

### Python 클라이언트 예시

```python
import requests

# 개발 환경 (자체 서명 인증서)
response = requests.get('https://localhost:8002/api/v1/health', verify=False)

# 또는 CA 인증서 지정
response = requests.get(
    'https://localhost:8002/api/v1/health', 
    verify='./ssl/ca.crt'
)

# 프로덕션 환경
response = requests.get('https://api.spice-harvester.com/v1/health')
```

### JavaScript/Node.js 클라이언트

```javascript
// 개발 환경
process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;

const axios = require('axios');
const https = require('https');

const agent = new https.Agent({  
  rejectUnauthorized: false
});

axios.get('https://localhost:8002/api/v1/health', { httpsAgent: agent })
  .then(response => console.log(response.data));
```

## 문제 해결

### 1. SSL 인증서 오류

**문제**: "SSL: CERTIFICATE_VERIFY_FAILED"
```bash
# 해결 방법 1: 개발 환경에서는 SSL 검증 비활성화
export VERIFY_SSL=false

# 해결 방법 2: CA 인증서를 시스템에 추가
sudo cp ssl/ca.crt /usr/local/share/ca-certificates/
sudo update-ca-certificates
```

### 2. 포트 연결 오류

**문제**: "Connection refused on HTTPS port"
```bash
# 서비스가 HTTPS로 실행 중인지 확인
curl -k https://localhost:8000/health

# 환경 변수 확인
echo $USE_HTTPS
```

### 3. Mixed Content 오류

**문제**: 브라우저에서 "Mixed Content" 경고
- 모든 리소스가 HTTPS를 통해 로드되는지 확인
- API 호출 URL이 https://로 시작하는지 확인

## 보안 권장사항

### 개발 환경
1. 자체 서명 인증서는 개발 환경에서만 사용
2. `VERIFY_SSL=false`는 개발 환경에서만 설정
3. 민감한 데이터는 개발 환경에서도 암호화하여 전송

### 프로덕션 환경
1. **반드시** 유효한 SSL 인증서 사용 (Let's Encrypt 또는 상용)
2. TLS 1.2 이상만 허용
3. HSTS (HTTP Strict Transport Security) 헤더 설정
4. 정기적인 인증서 갱신 (Let's Encrypt는 90일마다)
5. SSL Labs 테스트로 보안 등급 확인: https://www.ssllabs.com/ssltest/

### 인증서 갱신

Let's Encrypt 자동 갱신 설정:
```bash
# Crontab 추가
0 0 * * * /usr/bin/certbot renew --quiet --post-hook "docker restart nginx-proxy"
```

## 모니터링

### SSL 인증서 만료 모니터링
```bash
# 인증서 만료일 확인
openssl x509 -enddate -noout -in /path/to/cert.pem

# 알림 스크립트 예시
#!/bin/bash
CERT_FILE="/ssl/live/spice-harvester.com/cert.pem"
DAYS_UNTIL_EXPIRY=$(( ($(date -d "$(openssl x509 -enddate -noout -in $CERT_FILE | cut -d= -f2)" +%s) - $(date +%s)) / 86400 ))

if [ $DAYS_UNTIL_EXPIRY -lt 30 ]; then
    echo "WARNING: SSL certificate expires in $DAYS_UNTIL_EXPIRY days"
    # 알림 전송 로직
fi
```

## 성능 최적화

1. **HTTP/2 활성화**: Nginx 설정에 `http2` 플래그 추가
2. **SSL Session 캐싱**: 재연결 시 성능 향상
3. **OCSP Stapling**: 인증서 검증 속도 향상
4. **적절한 Cipher Suite 선택**: 보안과 성능의 균형

---

추가 질문이나 문제가 있으면 GitHub Issues에 보고해주세요.