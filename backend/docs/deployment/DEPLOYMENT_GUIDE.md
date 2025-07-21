# SPICE 시스템 배포 가이드

## 🚀 프로덕션 배포 가이드

### 사전 요구사항

1. **Docker & Docker Compose**
   ```bash
   # Docker 설치
   curl -fsSL https://get.docker.com -o get-docker.sh
   sudo sh get-docker.sh
   
   # Docker Compose 설치
   sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
   sudo chmod +x /usr/local/bin/docker-compose
   ```

2. **시스템 요구사항**
   - CPU: 2코어 이상
   - RAM: 최소 4GB (8GB 권장)
   - 저장공간: 20GB 이상
   - OS: Linux (Ubuntu 20.04+ 권장)

### 빠른 시작

1. **저장소 복제**
   ```bash
   git clone <repository-url>
   cd SPICE\ HARVESTER/backend
   ```

2. **환경 설정**
   ```bash
   cp .env.example .env
   # 프로덕션 값으로 .env 파일 편집
   ```

3. **시스템 배포**
   ```bash
   # HTTP로 시작
   docker-compose up -d
   
   # 또는 HTTPS로 시작
   docker-compose -f docker-compose-https.yml up -d
   
   # 또는 개발 환경에서
   python start_services.py
   ```

### 배포 명령어

```bash
# 모든 서비스 빌드 및 시작
docker-compose up --build

# 백그라운드에서 실행
docker-compose up -d

# 모든 서비스 중지
docker-compose down

# 로그 보기
docker-compose logs -f [service-name]

# 테스트 실행
pytest tests/

# 모든 것 정리 (데이터 포함)
docker-compose down -v
```

### 포트 설정 (환경변수)

```bash
# .env 파일에서 포트 설정
OMS_PORT=8000
BFF_PORT=8002
FUNNEL_PORT=8003
TERMINUS_SERVER_PORT=6363

# 서비스 URL 설정
OMS_BASE_URL=http://oms:8000
BFF_BASE_URL=http://bff:8002
FUNNEL_BASE_URL=http://funnel:8003
TERMINUS_SERVER_URL=http://terminusdb:6363
```

### 서비스 아키텍처

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

### 헬스 체크

모든 서비스는 헬스 체크 엔드포인트를 포함합니다:

- **BFF**: http://localhost:8002/health
- **OMS**: http://localhost:8000/health
- **Funnel**: http://localhost:8003/health
- **TerminusDB**: http://localhost:6363/api/

### 모니터링

1. **서비스 상태 확인**
   ```bash
   docker-compose ps
   ```

2. **실시간 로그 보기**
   ```bash
   docker-compose logs -f [service-name]
   ```

3. **리소스 사용량**
   ```bash
   docker stats
   ```

### 백업 및 복구

1. **데이터 백업**
   ```bash
   # TerminusDB 데이터 백업
   docker run --rm -v spice_terminusdb_data:/data -v $(pwd):/backup alpine tar czf /backup/terminusdb-backup-$(date +%Y%m%d).tar.gz -C /data .
   
   # BFF 데이터 백업 (라벨 매핑)
   docker run --rm -v spice_bff_data:/data -v $(pwd):/backup alpine tar czf /backup/bff-backup-$(date +%Y%m%d).tar.gz -C /data .
   ```

2. **데이터 복원**
   ```bash
   # TerminusDB 데이터 복원
   docker run --rm -v spice_terminusdb_data:/data -v $(pwd):/backup alpine tar xzf /backup/terminusdb-backup-YYYYMMDD.tar.gz -C /data
   
   # BFF 데이터 복원
   docker run --rm -v spice_bff_data:/data -v $(pwd):/backup alpine tar xzf /backup/bff-backup-YYYYMMDD.tar.gz -C /data
   ```

### SSL/TLS 설정

#### 자체 서명 인증서 생성
```bash
# SSL 인증서 생성
./generate_ssl_certs.sh
```

#### HTTPS로 실행
```bash
# HTTPS 모드로 실행
USE_HTTPS=true docker-compose -f docker-compose-https.yml up -d
```

#### Nginx 리버스 프록시 설정
프로덕션에서는 SSL과 함께 리버스 프록시(nginx)를 사용하세요:

```nginx
server {
    listen 443 ssl;
    server_name your-domain.com;
    
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    
    location /api/bff/ {
        proxy_pass http://localhost:8002/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
    
    location /api/oms/ {
        proxy_pass http://localhost:8000/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
    
    location /api/funnel/ {
        proxy_pass http://localhost:8003/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### CORS 설정

개발 환경에서는 자동으로 일반적인 프론트엔드 포트가 허용됩니다:
- React (3000, 3001, 3002)
- Vite (5173, 5174)
- Angular (4200)
- Vue.js (8080)

프로덕션에서는 환경변수로 설정:
```bash
# .env 파일
CORS_ORIGINS=https://yourdomain.com,https://app.yourdomain.com
```

### 보안 모범 사례

1. **기본 비밀번호 변경**
   - .env에서 TERMINUS_KEY 업데이트
   - 강력한 SECRET_KEY 사용

2. **네트워크 격리**
   - Docker 네트워크 사용
   - 필요한 포트만 노출

3. **정기적인 업데이트**
   ```bash
   docker-compose pull
   ./deploy.sh up
   ```

4. **방화벽 활성화**
   ```bash
   sudo ufw allow 22/tcp
   sudo ufw allow 443/tcp
   sudo ufw enable
   ```

### 문제 해결

1. **서비스가 시작되지 않을 때**
   ```bash
   # 로그 확인
   docker-compose logs [service-name]
   
   # 포트 충돌 확인
   sudo lsof -i :8000
   sudo lsof -i :8002
   sudo lsof -i :8003
   sudo lsof -i :6363
   ```

2. **성능 문제**
   ```bash
   # Docker 리소스 증가
   # /etc/docker/daemon.json 편집
   {
     "default-ulimits": {
       "nofile": {
         "Name": "nofile",
         "Hard": 64000,
         "Soft": 64000
       }
     }
   }
   ```

3. **데이터베이스 연결 오류**
   - TerminusDB가 정상인지 확인
   - 네트워크 연결 확인
   - 자격 증명 확인

### 프로덕션 체크리스트

- [ ] 환경 변수 설정 완료
- [ ] SSL 인증서 설치 완료
- [ ] 방화벽 규칙 설정 완료
- [ ] 백업 전략 수립 완료
- [ ] 모니터링 알림 설정 완료
- [ ] 리소스 제한 설정 완료
- [ ] 로그 순환 활성화 완료
- [ ] 보안 업데이트 일정 수립 완료

### Docker Compose 설정

```yaml
version: '3.8'

services:
  bff:
    build: ./bff
    image: spice-harvester/bff:latest
    ports:
      - "8002:8002"
    environment:
      - OMS_BASE_URL=http://oms:8000
      - FUNNEL_BASE_URL=http://funnel:8003
      - LOG_LEVEL=${LOG_LEVEL:-INFO}
      - USE_HTTPS=${USE_HTTPS:-false}
      - SSL_CERT_PATH=${SSL_CERT_PATH:-}
      - SSL_KEY_PATH=${SSL_KEY_PATH:-}
      - VERIFY_SSL=${VERIFY_SSL:-false}
    depends_on:
      - oms
      - funnel
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8002/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      
  oms:
    build: ./oms
    image: spice-harvester/oms:latest
    ports:
      - "8000:8000"
    environment:
      - TERMINUS_SERVER_URL=http://terminusdb:6363
      - TERMINUS_USER=${TERMINUS_USER:-admin}
      - TERMINUS_ACCOUNT=${TERMINUS_ACCOUNT:-admin}
      - TERMINUS_KEY=${TERMINUS_KEY:-admin123}
      - LOG_LEVEL=${LOG_LEVEL:-INFO}
    depends_on:
      - terminusdb
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      
  funnel:
    build: ./funnel
    image: spice-harvester/funnel:latest
    ports:
      - "8003:8003"
    environment:
      - BFF_URL=http://bff:8002
      - LOG_LEVEL=${LOG_LEVEL:-INFO}
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8003/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      
  terminusdb:
    image: terminusdb/terminusdb-server:latest
    ports:
      - "6363:6363"
    volumes:
      - terminus_data:/app/terminusdb/storage
    environment:
      - TERMINUSDB_ADMIN_PASS=${TERMINUSDB_ADMIN_PASS:-admin123}
      - TERMINUSDB_SERVER_NAME=${TERMINUSDB_SERVER_NAME:-SpiceTerminusDB}
      - TERMINUSDB_AUTOLOGIN=false
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:6363/api/"]
      interval: 30s
      timeout: 10s
      retries: 5
      
volumes:
  terminus_data:
  bff_data:
```

### 최신 기능

- **HTTPS 지원**: 자체 서명 인증서 또는 Let's Encrypt
- **CORS 자동 설정**: 개발 환경에서 자동 포트 허용
- **Google Sheets 통합**: 스키마 제안을 위한 데이터 연결
- **타입 추론 서비스**: 복합 데이터 타입 자동 감지

### 지원

문제 발생 시:
1. 서비스 로그 확인
2. 헬스 엔드포인트 검토
3. 오류 메시지 확인
4. 상세 내용과 함께 GitHub 이슈 생성

---

## 🎉 축하합니다!

SPICE 시스템이 다음 기능과 함께 프로덕션 준비가 완료되었습니다:
- ✅ 고가용성
- ✅ 자동 헬스 체크
- ✅ 데이터 영속성
- ✅ 쉬운 확장
- ✅ 포괄적인 로깅
- ✅ 보안 모범 사례

---

**최종 업데이트**: 2025-07-20