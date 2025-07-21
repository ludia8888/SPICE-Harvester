#!/usr/bin/env python3
"""
🔥 THINK ULTRA! CORS Configuration Test Script
SPICE HARVESTER - 모든 서비스의 CORS 설정을 테스트하는 스크립트
"""

import asyncio
import aiohttp
import json
import sys
from typing import Dict, List, Any
from dataclasses import dataclass

# 🔥 BUG FIX: colorama 의존성 문제 해결
try:
    from colorama import Fore, Style, init
    # Colorama 초기화
    init(autoreset=True)
    COLORAMA_AVAILABLE = True
except ImportError:
    # colorama가 없으면 빈 문자열 사용
    class MockFore:
        CYAN = ""
        GREEN = ""
        RED = ""
        YELLOW = ""
        BLUE = ""
        MAGENTA = ""
    
    class MockStyle:
        RESET_ALL = ""
    
    Fore = MockFore()
    Style = MockStyle()
    COLORAMA_AVAILABLE = False
    print("⚠️  colorama 패키지가 없습니다. 컬러 출력이 비활성화됩니다.")
    print("   설치하려면: pip install colorama")

@dataclass
class ServiceInfo:
    """서비스 정보"""
    name: str
    url: str
    health_endpoint: str = "/health"
    debug_endpoint: str = "/debug/cors"

# 테스트할 서비스 목록
SERVICES = [
    ServiceInfo("BFF", "http://localhost:8002"),
    ServiceInfo("OMS", "http://localhost:8000"), 
    ServiceInfo("Funnel", "http://localhost:8003")
]

# 테스트할 Origin 목록
TEST_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:3001",
    "http://localhost:3002", 
    "http://localhost:5173",
    "http://localhost:4200",
    "https://localhost:3000",
    "http://127.0.0.1:3000",
    "https://app.spice-harvester.com",
    "https://malicious-site.com",  # 허용되지 않는 origin
    "null",  # 특수 케이스
]

class CORSTestResults:
    """CORS 테스트 결과를 저장하는 클래스"""
    
    def __init__(self):
        self.results = {}
        self.summary = {
            "total_tests": 0,
            "passed_tests": 0,
            "failed_tests": 0,
            "services_tested": 0
        }
    
    def add_result(self, service: str, origin: str, result: Dict[str, Any]):
        """테스트 결과 추가"""
        if service not in self.results:
            self.results[service] = {}
        self.results[service][origin] = result
        self.summary["total_tests"] += 1
        if result.get("cors_allowed", False):
            self.summary["passed_tests"] += 1
        else:
            self.summary["failed_tests"] += 1
    
    def print_summary(self):
        """테스트 결과 요약 출력"""
        print(f"\n{Fore.CYAN}{'='*60}")
        print(f"{Fore.CYAN}🔥 CORS 테스트 결과 요약")
        print(f"{Fore.CYAN}{'='*60}")
        print(f"{Fore.GREEN}✓ 통과: {self.summary['passed_tests']}")
        print(f"{Fore.RED}✗ 실패: {self.summary['failed_tests']}")
        print(f"{Fore.YELLOW}📊 총 테스트: {self.summary['total_tests']}")
        print(f"{Fore.BLUE}🌐 서비스 수: {self.summary['services_tested']}")
        
        # 서비스별 상세 결과
        for service, origins in self.results.items():
            print(f"\n{Fore.MAGENTA}🔧 {service} 서비스:")
            for origin, result in origins.items():
                status = f"{Fore.GREEN}✓" if result.get("cors_allowed", False) else f"{Fore.RED}✗"
                print(f"  {status} {origin}: {result.get('status', 'Unknown')}")

async def test_cors_preflight(session: aiohttp.ClientSession, service: ServiceInfo, origin: str) -> Dict[str, Any]:
    """CORS preflight 요청 테스트"""
    headers = {
        "Origin": origin,
        "Access-Control-Request-Method": "GET",
        "Access-Control-Request-Headers": "Content-Type,Authorization"
    }
    
    try:
        async with session.options(
            f"{service.url}{service.health_endpoint}",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=5)
        ) as response:
            cors_headers = {
                "Access-Control-Allow-Origin": response.headers.get("Access-Control-Allow-Origin"),
                "Access-Control-Allow-Methods": response.headers.get("Access-Control-Allow-Methods"),
                "Access-Control-Allow-Headers": response.headers.get("Access-Control-Allow-Headers"),
                "Access-Control-Allow-Credentials": response.headers.get("Access-Control-Allow-Credentials")
            }
            
            # CORS가 허용되는지 확인
            cors_allowed = (
                cors_headers["Access-Control-Allow-Origin"] == origin or
                cors_headers["Access-Control-Allow-Origin"] == "*"
            )
            
            return {
                "status": "success",
                "cors_allowed": cors_allowed,
                "status_code": response.status,
                "cors_headers": cors_headers
            }
    
    except asyncio.TimeoutError:
        return {
            "status": "timeout",
            "cors_allowed": False,
            "error": "Request timeout"
        }
    except Exception as e:
        return {
            "status": "error",
            "cors_allowed": False,
            "error": str(e)
        }

async def test_actual_request(session: aiohttp.ClientSession, service: ServiceInfo, origin: str) -> Dict[str, Any]:
    """실제 CORS 요청 테스트"""
    headers = {
        "Origin": origin,
        "Content-Type": "application/json"
    }
    
    try:
        async with session.get(
            f"{service.url}{service.health_endpoint}",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=5)
        ) as response:
            cors_headers = {
                "Access-Control-Allow-Origin": response.headers.get("Access-Control-Allow-Origin"),
                "Access-Control-Allow-Credentials": response.headers.get("Access-Control-Allow-Credentials")
            }
            
            # CORS가 허용되는지 확인
            cors_allowed = (
                cors_headers["Access-Control-Allow-Origin"] == origin or
                cors_headers["Access-Control-Allow-Origin"] == "*"
            )
            
            return {
                "status": "success",
                "cors_allowed": cors_allowed,
                "status_code": response.status,
                "cors_headers": cors_headers,
                "response_data": await response.json() if response.content_type == "application/json" else None
            }
    
    except asyncio.TimeoutError:
        return {
            "status": "timeout",
            "cors_allowed": False,
            "error": "Request timeout"
        }
    except Exception as e:
        return {
            "status": "error",
            "cors_allowed": False,
            "error": str(e)
        }

async def get_cors_debug_info(session: aiohttp.ClientSession, service: ServiceInfo) -> Dict[str, Any]:
    """서비스의 CORS 디버그 정보 가져오기"""
    try:
        async with session.get(
            f"{service.url}{service.debug_endpoint}",
            timeout=aiohttp.ClientTimeout(total=5)
        ) as response:
            if response.status == 200:
                return await response.json()
            else:
                return {"error": f"Debug endpoint returned {response.status}"}
    except Exception as e:
        return {"error": str(e)}

async def test_service_cors(session: aiohttp.ClientSession, service: ServiceInfo, results: CORSTestResults):
    """특정 서비스의 CORS 설정 테스트"""
    print(f"\n{Fore.YELLOW}🔧 Testing {service.name} service ({service.url})")
    
    # 디버그 정보 가져오기
    debug_info = await get_cors_debug_info(session, service)
    if "error" not in debug_info:
        print(f"{Fore.BLUE}📋 CORS Config:")
        print(f"  - Enabled: {debug_info.get('enabled', 'Unknown')}")
        print(f"  - Environment: {debug_info.get('environment', 'Unknown')}")
        print(f"  - Origins: {debug_info.get('origins', [])[:3]}...")
    
    # 각 Origin에 대해 테스트
    for origin in TEST_ORIGINS:
        print(f"{Fore.CYAN}  Testing origin: {origin}")
        
        # Preflight 요청 테스트
        preflight_result = await test_cors_preflight(session, service, origin)
        
        # 실제 요청 테스트
        actual_result = await test_actual_request(session, service, origin)
        
        # 결과 통합
        combined_result = {
            "preflight": preflight_result,
            "actual": actual_result,
            "cors_allowed": preflight_result.get("cors_allowed", False) and actual_result.get("cors_allowed", False)
        }
        
        results.add_result(service.name, origin, combined_result)
        
        # 실시간 결과 출력
        status = f"{Fore.GREEN}✓" if combined_result["cors_allowed"] else f"{Fore.RED}✗"
        print(f"    {status} CORS {'allowed' if combined_result['cors_allowed'] else 'blocked'}")
    
    results.summary["services_tested"] += 1

async def main():
    """메인 테스트 함수"""
    print(f"{Fore.MAGENTA}🔥 SPICE HARVESTER CORS Configuration Test")
    print(f"{Fore.MAGENTA}{'='*50}")
    
    results = CORSTestResults()
    
    async with aiohttp.ClientSession() as session:
        # 각 서비스 테스트
        for service in SERVICES:
            try:
                await test_service_cors(session, service, results)
            except Exception as e:
                print(f"{Fore.RED}❌ Failed to test {service.name}: {e}")
                results.summary["services_tested"] += 1
    
    # 결과 요약 출력
    results.print_summary()
    
    # 결과를 JSON 파일로 저장
    try:
        with open("cors_test_results.json", "w", encoding="utf-8") as f:
            json.dump(results.results, f, indent=2, ensure_ascii=False)
        print(f"\n{Fore.GREEN}📄 결과가 cors_test_results.json에 저장되었습니다.")
    except Exception as e:
        print(f"{Fore.RED}❌ 결과 저장 실패: {e}")
    
    # 권장사항 출력
    print(f"\n{Fore.YELLOW}💡 권장사항:")
    print(f"  1. 개발 환경에서는 모든 localhost 포트가 허용되어야 합니다.")
    print(f"  2. 프로덕션 환경에서는 특정 도메인만 허용되어야 합니다.")
    print(f"  3. malicious-site.com 같은 허용되지 않은 origin은 차단되어야 합니다.")
    print(f"  4. 각 서비스의 /debug/cors 엔드포인트에서 설정을 확인할 수 있습니다.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}⚠️  테스트가 사용자에 의해 중단되었습니다.")
        sys.exit(1)
    except Exception as e:
        print(f"\n{Fore.RED}❌ 테스트 실행 중 오류 발생: {e}")
        sys.exit(1)