#!/bin/bash

# SSL 인증서 생성 스크립트
# 개발 환경용 자체 서명 인증서 생성

set -e

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🔐 SPICE HARVESTER 개발환경 SSL 인증서 생성 시작${NC}"

# SSL 디렉토리 생성
SSL_DIR="./ssl"
mkdir -p $SSL_DIR

# 인증서 유효 기간 (일)
DAYS=365

# 서비스 목록
SERVICES=("oms" "bff" "funnel" "terminusdb")
PORTS=("8000" "8002" "8003" "6363")

# Root CA 생성
echo -e "\n${YELLOW}1. Root CA 생성중...${NC}"
if [ ! -f "$SSL_DIR/ca.key" ]; then
    # CA 개인키 생성
    openssl genrsa -out $SSL_DIR/ca.key 4096
    
    # CA 인증서 생성
    openssl req -new -x509 -days $DAYS -key $SSL_DIR/ca.key -out $SSL_DIR/ca.crt \
        -subj "/C=KR/ST=Seoul/L=Seoul/O=SPICE HARVESTER/OU=Development/CN=SPICE HARVESTER Root CA"
    
    echo -e "${GREEN}✓ Root CA 생성 완료${NC}"
else
    echo -e "${YELLOW}! Root CA가 이미 존재합니다. 기존 CA 사용${NC}"
fi

# 각 서비스별 인증서 생성
for i in ${!SERVICES[@]}; do
    SERVICE=${SERVICES[$i]}
    PORT=${PORTS[$i]}
    
    echo -e "\n${YELLOW}2. $SERVICE 서비스 인증서 생성중...${NC}"
    
    # 서비스 디렉토리 생성
    mkdir -p $SSL_DIR/$SERVICE
    
    # 서비스 개인키 생성
    openssl genrsa -out $SSL_DIR/$SERVICE/server.key 2048
    
    # 서비스 CSR 생성
    openssl req -new -key $SSL_DIR/$SERVICE/server.key -out $SSL_DIR/$SERVICE/server.csr \
        -subj "/C=KR/ST=Seoul/L=Seoul/O=SPICE HARVESTER/OU=$SERVICE/CN=localhost"
    
    # SAN (Subject Alternative Name) 설정 파일 생성
    cat > $SSL_DIR/$SERVICE/san.ext <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = $SERVICE
DNS.3 = $SERVICE.spice-harvester.local
IP.1 = 127.0.0.1
IP.2 = ::1
EOF
    
    # 서비스 인증서 생성 (CA로 서명)
    openssl x509 -req -in $SSL_DIR/$SERVICE/server.csr \
        -CA $SSL_DIR/ca.crt -CAkey $SSL_DIR/ca.key -CAcreateserial \
        -out $SSL_DIR/$SERVICE/server.crt -days $DAYS \
        -sha256 -extfile $SSL_DIR/$SERVICE/san.ext
    
    # PEM 형식 통합 파일 생성 (일부 서비스에서 필요)
    cat $SSL_DIR/$SERVICE/server.crt $SSL_DIR/$SERVICE/server.key > $SSL_DIR/$SERVICE/server.pem
    
    # 정리
    rm $SSL_DIR/$SERVICE/server.csr $SSL_DIR/$SERVICE/san.ext
    
    echo -e "${GREEN}✓ $SERVICE 인증서 생성 완료${NC}"
done

# 공통 인증서 생성 (모든 서비스에서 사용 가능)
echo -e "\n${YELLOW}3. 공통 와일드카드 인증서 생성중...${NC}"
mkdir -p $SSL_DIR/common

openssl genrsa -out $SSL_DIR/common/server.key 2048

openssl req -new -key $SSL_DIR/common/server.key -out $SSL_DIR/common/server.csr \
    -subj "/C=KR/ST=Seoul/L=Seoul/O=SPICE HARVESTER/OU=Development/CN=*.spice-harvester.local"

cat > $SSL_DIR/common/san.ext <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = *.spice-harvester.local
DNS.3 = oms
DNS.4 = bff
DNS.5 = funnel
DNS.6 = terminusdb
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

openssl x509 -req -in $SSL_DIR/common/server.csr \
    -CA $SSL_DIR/ca.crt -CAkey $SSL_DIR/ca.key -CAcreateserial \
    -out $SSL_DIR/common/server.crt -days $DAYS \
    -sha256 -extfile $SSL_DIR/common/san.ext

cat $SSL_DIR/common/server.crt $SSL_DIR/common/server.key > $SSL_DIR/common/server.pem

rm $SSL_DIR/common/server.csr $SSL_DIR/common/san.ext

echo -e "${GREEN}✓ 공통 와일드카드 인증서 생성 완료${NC}"

# 인증서 정보 파일 생성
echo -e "\n${YELLOW}4. 인증서 정보 파일 생성중...${NC}"
cat > $SSL_DIR/README.md <<EOF
# SPICE HARVESTER SSL 인증서

## 생성된 인증서
- **Root CA**: ssl/ca.crt, ssl/ca.key
- **OMS**: ssl/oms/server.crt, ssl/oms/server.key
- **BFF**: ssl/bff/server.crt, ssl/bff/server.key
- **Funnel**: ssl/funnel/server.crt, ssl/funnel/server.key
- **TerminusDB**: ssl/terminusdb/server.crt, ssl/terminusdb/server.key
- **공통 와일드카드**: ssl/common/server.crt, ssl/common/server.key

## 사용 방법

### 1. 환경 변수 설정
\`\`\`bash
export USE_HTTPS=true
export SSL_CERT_PATH=./ssl/common/server.crt
export SSL_KEY_PATH=./ssl/common/server.key
\`\`\`

### 2. 서비스별 설정
각 서비스의 main.py에서 SSL 옵션을 활성화하면 자동으로 인증서를 로드합니다.

### 3. 클라이언트 설정
개발 환경에서는 자체 서명 인증서를 사용하므로, 클라이언트에서 인증서 검증을 비활성화해야 할 수 있습니다:

\`\`\`python
# Python requests
requests.get('https://localhost:8000', verify=False)

# 또는 CA 인증서 지정
requests.get('https://localhost:8000', verify='./ssl/ca.crt')
\`\`\`

### 4. 브라우저에서 사용
1. ca.crt를 시스템 신뢰 저장소에 추가
2. 또는 브라우저에서 보안 예외 추가

## 인증서 갱신
인증서는 365일 후 만료됩니다. 갱신하려면:
\`\`\`bash
./generate_ssl_certs.sh
\`\`\`

## 프로덕션 환경
프로덕션에서는 Let's Encrypt 또는 상용 인증서를 사용하세요.
EOF

# 권한 설정
chmod 600 $SSL_DIR/ca.key
chmod 600 $SSL_DIR/*/server.key
chmod 644 $SSL_DIR/ca.crt
chmod 644 $SSL_DIR/*/server.crt

echo -e "\n${GREEN}✅ SSL 인증서 생성 완료!${NC}"
echo -e "${YELLOW}📁 인증서 위치: $SSL_DIR${NC}"
echo -e "${YELLOW}📄 사용법: $SSL_DIR/README.md 참조${NC}"

# .gitignore에 SSL 디렉토리 추가
if ! grep -q "^ssl/" .gitignore 2>/dev/null; then
    echo -e "\n# SSL certificates" >> .gitignore
    echo "ssl/" >> .gitignore
    echo -e "${GREEN}✓ .gitignore에 SSL 디렉토리 추가됨${NC}"
fi

echo -e "\n${GREEN}🎉 모든 작업이 완료되었습니다!${NC}"