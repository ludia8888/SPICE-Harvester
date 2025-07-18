#!/bin/bash
# Google Sheets 커넥터 통합 테스트 스크립트

echo "🚀 Google Sheets 커넥터 통합 테스트 시작"
echo "=================================="

# 현재 디렉토리 저장
BACKEND_DIR="/Users/isihyeon/Desktop/SPICE HARVESTER/backend"
cd "$BACKEND_DIR"

# 1. BFF 서비스 상태 확인
echo "1️⃣ BFF 서비스 상태 확인..."
if curl -s http://localhost:8002/ > /dev/null 2>&1; then
    echo "✅ BFF 서비스가 실행 중입니다."
    
    # 기존 BFF 프로세스 종료
    echo "🔄 기존 BFF 서비스 재시작 중..."
    pkill -f "python.*backend-for-frontend/main.py" 2>/dev/null
    sleep 2
fi

# 2. BFF 서비스 시작 (백그라운드)
echo "2️⃣ BFF 서비스 시작 중..."
python backend-for-frontend/main.py > bff_test.log 2>&1 &
BFF_PID=$!
echo "   PID: $BFF_PID"

# 서비스 시작 대기
echo "⏳ 서비스 시작 대기 (5초)..."
sleep 5

# 3. Google Sheets 커넥터 헬스 체크
echo "3️⃣ Google Sheets 커넥터 헬스 체크..."
curl -X GET http://localhost:8002/api/v1/connectors/google/health \
     -H "Accept: application/json" 2>/dev/null | python -m json.tool

# 4. 유틸리티 테스트 실행
echo -e "\n4️⃣ 유틸리티 함수 테스트..."
python tests/connectors/test_google_sheets_simple.py

# 5. API 통합 테스트 실행
echo -e "\n5️⃣ API 통합 테스트..."
python tests/connectors/test_google_sheets_api.py

# 6. BFF 서비스 로그 확인
echo -e "\n6️⃣ BFF 서비스 로그 (마지막 20줄)..."
tail -n 20 bff_test.log

# 7. 정리
echo -e "\n7️⃣ 테스트 완료. BFF 서비스 종료 중..."
kill $BFF_PID 2>/dev/null

echo "=================================="
echo "✅ 모든 테스트 완료!"