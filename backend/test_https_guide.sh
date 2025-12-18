#!/bin/bash

# SPICE HARVESTER HTTPS 테스트 가이드 스크립트

echo "🔥 SPICE HARVESTER HTTPS 테스트 가이드"
echo "====================================="
echo ""
echo "1. HTTP 모드 테스트:"
echo "   python test_https_integration.py"
echo ""
echo "2. HTTPS 모드 테스트 (자체 서명 인증서):"
echo "   USE_HTTPS=true python test_https_integration.py"
echo ""
echo "3. HTTPS 모드 테스트 (SSL 검증 활성화):"
echo "   USE_HTTPS=true VERIFY_SSL=true python test_https_integration.py"
echo ""
echo "4. 서비스 시작:"
echo "   - HTTP:  python start_services.py --env development"
echo "   - HTTPS: USE_HTTPS=true python start_services.py --env development"
echo ""
echo "5. Docker Compose:"
echo "   - HTTP:  docker compose up"
echo "   - HTTPS: docker compose -f docker-compose-https.yml up"
echo ""
echo "테스트를 시작하시겠습니까? (y/n)"
read -r response

if [[ "$response" == "y" ]]; then
    echo ""
    echo "어떤 모드로 테스트하시겠습니까?"
    echo "1) HTTP"
    echo "2) HTTPS (SSL 검증 비활성화)"
    echo "3) HTTPS (SSL 검증 활성화)"
    read -r mode
    
    case $mode in
        1)
            echo "HTTP 모드로 테스트를 실행합니다..."
            python test_https_integration.py
            ;;
        2)
            echo "HTTPS 모드로 테스트를 실행합니다 (SSL 검증 비활성화)..."
            USE_HTTPS=true python test_https_integration.py
            ;;
        3)
            echo "HTTPS 모드로 테스트를 실행합니다 (SSL 검증 활성화)..."
            USE_HTTPS=true VERIFY_SSL=true python test_https_integration.py
            ;;
        *)
            echo "잘못된 선택입니다."
            exit 1
            ;;
    esac
else
    echo "테스트를 취소했습니다."
fi
