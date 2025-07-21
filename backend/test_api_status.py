#!/usr/bin/env python3
"""
API 상태 확인 스크립트
"""

import requests
import json
from typing import Dict, Any


def check_service(name: str, url: str) -> Dict[str, Any]:
    """서비스 상태 확인"""
    try:
        # Health check
        health_response = requests.get(f"{url}/health")
        health_status = health_response.status_code == 200
        
        # API docs check
        docs_response = requests.get(f"{url}/docs")
        docs_status = docs_response.status_code == 200
        
        return {
            "name": name,
            "url": url,
            "health_check": health_status,
            "api_docs": docs_status,
            "status": "OK" if health_status and docs_status else "FAIL"
        }
    except Exception as e:
        return {
            "name": name,
            "url": url,
            "health_check": False,
            "api_docs": False,
            "status": "ERROR",
            "error": str(e)
        }


def test_cors(url: str) -> Dict[str, Any]:
    """CORS 설정 테스트"""
    try:
        headers = {
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET"
        }
        response = requests.options(f"{url}/health", headers=headers)
        
        cors_headers = {
            "Access-Control-Allow-Origin": response.headers.get("Access-Control-Allow-Origin"),
            "Access-Control-Allow-Methods": response.headers.get("Access-Control-Allow-Methods"),
            "Access-Control-Allow-Headers": response.headers.get("Access-Control-Allow-Headers")
        }
        
        return {
            "status_code": response.status_code,
            "cors_enabled": response.status_code == 200,
            "headers": cors_headers
        }
    except Exception as e:
        return {
            "cors_enabled": False,
            "error": str(e)
        }


def main():
    """메인 함수"""
    services = [
        ("BFF Service", "http://localhost:8002"),
        ("OMS Service", "http://localhost:8000"),
        ("Funnel Service", "http://localhost:8003")
    ]
    
    print("=" * 60)
    print("SPICE HARVESTER API 상태 확인")
    print("=" * 60)
    
    # 1. 서비스 상태 확인
    print("\n1. 서비스 상태:")
    for name, url in services:
        result = check_service(name, url)
        print(f"\n{name}:")
        print(f"  - URL: {url}")
        print(f"  - Health Check: {'✓' if result['health_check'] else '✗'}")
        print(f"  - API Docs: {'✓' if result['api_docs'] else '✗'}")
        print(f"  - Status: {result['status']}")
        if "error" in result:
            print(f"  - Error: {result['error']}")
    
    # 2. CORS 설정 확인
    print("\n\n2. CORS 설정:")
    for name, url in services:
        result = test_cors(url)
        print(f"\n{name}:")
        print(f"  - CORS Enabled: {'✓' if result['cors_enabled'] else '✗'}")
        if "headers" in result:
            print(f"  - Allow-Origin: {result['headers']['Access-Control-Allow-Origin']}")
            print(f"  - Allow-Methods: {result['headers']['Access-Control-Allow-Methods']}")
    
    # 3. API 엔드포인트 확인
    print("\n\n3. API 엔드포인트 확인:")
    
    # BFF 엔드포인트
    print("\nBFF Service 엔드포인트:")
    bff_endpoints = [
        ("GET", "/api/v1/databases", "데이터베이스 목록"),
        ("POST", "/api/v1/databases", "데이터베이스 생성"),
        ("GET", "/database/{db_name}/ontology/{class_label}", "온톨로지 조회"),
        ("POST", "/database/{db_name}/ontology", "온톨로지 생성"),
        ("GET", "/database/{db_name}/branches", "브랜치 목록")
    ]
    
    for method, endpoint, desc in bff_endpoints:
        print(f"  - {method:6} {endpoint:50} {desc}")
    
    # 4. 인증 설정 확인
    print("\n\n4. 인증/인가 설정:")
    print("  - 현재 인증 시스템: 없음 (개발 중)")
    print("  - API 키 인증: 미구현")
    print("  - JWT 토큰 인증: 미구현")
    
    # 5. 응답 형식 확인
    print("\n\n5. API 응답 형식 확인:")
    try:
        response = requests.get("http://localhost:8002/health")
        if response.status_code == 200:
            data = response.json()
            print(f"  - 응답 형식: JSON")
            print(f"  - 응답 예시: {json.dumps(data, indent=2)}")
    except Exception as e:
        print(f"  - 오류: {e}")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()