#!/usr/bin/env python3
"""
SPICE HARVESTER HTTPS 통합 테스트
모든 서비스가 HTTP/HTTPS에서 올바르게 작동하는지 검증합니다.
"""

import requests
import urllib3
import json
import sys
import os
import time
from typing import Dict, List, Tuple, Optional
from datetime import datetime

# 개발 환경에서 SSL 경고 비활성화
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class HTTPSIntegrationTest:
    """HTTPS 통합 테스트 클래스"""
    
    def __init__(self):
        self.use_https = os.getenv("USE_HTTPS", "false").lower() in ("true", "1", "yes", "on")
        self.protocol = "https" if self.use_https else "http"
        self.verify_ssl = os.getenv("VERIFY_SSL", "false").lower() in ("true", "1", "yes", "on")
        
        # CA 인증서 경로
        self.ca_cert = os.getenv("SSL_CA_PATH", "./ssl/ca.crt")
        if self.verify_ssl and os.path.exists(self.ca_cert):
            self.verify = self.ca_cert
        else:
            self.verify = self.verify_ssl
        
        # 서비스 URL
        self.services = {
            "OMS": f"{self.protocol}://localhost:8000",
            "BFF": f"{self.protocol}://localhost:8002",
            "Funnel": f"{self.protocol}://localhost:8003"
        }
        
        self.results = []
        
    def log(self, message: str, level: str = "INFO"):
        """로그 메시지 출력"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        prefix = {
            "INFO": "ℹ️ ",
            "SUCCESS": "✅",
            "ERROR": "❌",
            "WARNING": "⚠️ "
        }.get(level, "")
        print(f"[{timestamp}] {prefix} {message}")
        
    def test_service_health(self, name: str, url: str) -> bool:
        """서비스 헬스 체크"""
        try:
            response = requests.get(
                f"{url}/health",
                timeout=5,
                verify=self.verify
            )
            if response.status_code == 200:
                self.log(f"{name} 헬스 체크 성공 ({self.protocol.upper()})", "SUCCESS")
                return True
            else:
                self.log(f"{name} 헬스 체크 실패: {response.status_code}", "ERROR")
                return False
        except requests.exceptions.SSLError as e:
            self.log(f"{name} SSL 오류: {str(e)}", "ERROR")
            return False
        except Exception as e:
            self.log(f"{name} 연결 실패: {str(e)}", "ERROR")
            return False
    
    def test_inter_service_communication(self) -> bool:
        """서비스 간 통신 테스트"""
        self.log("서비스 간 통신 테스트 시작", "INFO")
        
        try:
            # BFF가 OMS와 통신할 수 있는지 테스트
            response = requests.get(
                f"{self.services['BFF']}/api/v1/databases",
                timeout=10,
                verify=self.verify
            )
            
            if response.status_code in [200, 404]:  # 404는 DB가 없을 때
                self.log("BFF → OMS 통신 성공", "SUCCESS")
                return True
            else:
                self.log(f"BFF → OMS 통신 실패: {response.status_code}", "ERROR")
                return False
                
        except Exception as e:
            self.log(f"서비스 간 통신 실패: {str(e)}", "ERROR")
            return False
    
    def test_ssl_certificate(self, name: str, url: str) -> Optional[Dict]:
        """SSL 인증서 정보 확인"""
        if not self.use_https:
            return None
            
        try:
            import ssl
            import socket
            from urllib.parse import urlparse
            
            parsed = urlparse(url)
            hostname = parsed.hostname
            port = parsed.port or 443
            
            context = ssl.create_default_context()
            if not self.verify_ssl:
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
            
            with socket.create_connection((hostname, port), timeout=5) as sock:
                with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                    cert = ssock.getpeercert()
                    
                    if cert:
                        self.log(f"{name} SSL 인증서 확인 완료", "SUCCESS")
                        return {
                            "subject": dict(x[0] for x in cert['subject']),
                            "issuer": dict(x[0] for x in cert['issuer']),
                            "notAfter": cert['notAfter']
                        }
                    else:
                        self.log(f"{name} SSL 인증서 정보 없음", "WARNING")
                        return None
                        
        except Exception as e:
            self.log(f"{name} SSL 인증서 확인 실패: {str(e)}", "ERROR")
            return None
    
    def test_api_endpoints(self) -> List[Tuple[str, bool]]:
        """주요 API 엔드포인트 테스트"""
        endpoints = [
            ("BFF Docs", f"{self.services['BFF']}/docs"),
            ("OMS API", f"{self.services['OMS']}/api/v1/databases"),
            ("Funnel Health", f"{self.services['Funnel']}/health"),
        ]
        
        results = []
        for name, url in endpoints:
            try:
                response = requests.get(url, timeout=5, verify=self.verify)
                success = response.status_code < 500
                results.append((name, success))
                
                if success:
                    self.log(f"{name} 엔드포인트 정상", "SUCCESS")
                else:
                    self.log(f"{name} 엔드포인트 오류: {response.status_code}", "ERROR")
                    
            except Exception as e:
                self.log(f"{name} 엔드포인트 접근 실패: {str(e)}", "ERROR")
                results.append((name, False))
        
        return results
    
    def test_create_database(self) -> bool:
        """데이터베이스 생성 테스트"""
        try:
            test_db_name = f"test_https_{int(time.time())}"
            
            response = requests.post(
                f"{self.services['BFF']}/api/v1/databases",
                json={
                    "name": test_db_name,
                    "description": "HTTPS 통합 테스트용 데이터베이스"
                },
                timeout=10,
                verify=self.verify
            )
            
            if response.status_code == 201:
                self.log(f"테스트 데이터베이스 생성 성공: {test_db_name}", "SUCCESS")
                
                # 정리 - 데이터베이스 삭제
                delete_response = requests.delete(
                    f"{self.services['BFF']}/api/v1/databases/{test_db_name}",
                    timeout=10,
                    verify=self.verify
                )
                
                if delete_response.status_code == 200:
                    self.log("테스트 데이터베이스 정리 완료", "SUCCESS")
                
                return True
            else:
                self.log(f"데이터베이스 생성 실패: {response.status_code}", "ERROR")
                return False
                
        except Exception as e:
            self.log(f"데이터베이스 작업 실패: {str(e)}", "ERROR")
            return False
    
    def run_all_tests(self):
        """모든 테스트 실행"""
        self.log(f"{'='*60}", "INFO")
        self.log(f"SPICE HARVESTER {self.protocol.upper()} 통합 테스트 시작", "INFO")
        self.log(f"SSL 검증: {'활성화' if self.verify_ssl else '비활성화'}", "INFO")
        self.log(f"{'='*60}", "INFO")
        
        # 1. 서비스 헬스 체크
        self.log("\n1. 서비스 헬스 체크", "INFO")
        health_results = {}
        for name, url in self.services.items():
            health_results[name] = self.test_service_health(name, url)
        
        # 2. SSL 인증서 확인 (HTTPS인 경우)
        if self.use_https:
            self.log("\n2. SSL 인증서 확인", "INFO")
            for name, url in self.services.items():
                cert_info = self.test_ssl_certificate(name, url)
                if cert_info:
                    self.log(f"  - Subject: {cert_info['subject']}", "INFO")
                    self.log(f"  - Issuer: {cert_info['issuer']}", "INFO")
                    self.log(f"  - 만료일: {cert_info['notAfter']}", "INFO")
        
        # 3. 서비스 간 통신 테스트
        self.log("\n3. 서비스 간 통신 테스트", "INFO")
        comm_result = self.test_inter_service_communication()
        
        # 4. API 엔드포인트 테스트
        self.log("\n4. API 엔드포인트 테스트", "INFO")
        api_results = self.test_api_endpoints()
        
        # 5. 데이터베이스 작업 테스트
        self.log("\n5. 데이터베이스 작업 테스트", "INFO")
        db_result = self.test_create_database()
        
        # 결과 요약
        self.log(f"\n{'='*60}", "INFO")
        self.log("테스트 결과 요약", "INFO")
        self.log(f"{'='*60}", "INFO")
        
        total_tests = 0
        passed_tests = 0
        
        # 헬스 체크 결과
        for name, result in health_results.items():
            total_tests += 1
            if result:
                passed_tests += 1
            status = "PASS" if result else "FAIL"
            self.log(f"{name} 헬스 체크: {status}", "INFO")
        
        # 통신 테스트 결과
        total_tests += 1
        if comm_result:
            passed_tests += 1
        self.log(f"서비스 간 통신: {'PASS' if comm_result else 'FAIL'}", "INFO")
        
        # API 엔드포인트 결과
        for name, result in api_results:
            total_tests += 1
            if result:
                passed_tests += 1
            status = "PASS" if result else "FAIL"
            self.log(f"{name}: {status}", "INFO")
        
        # 데이터베이스 작업 결과
        total_tests += 1
        if db_result:
            passed_tests += 1
        self.log(f"데이터베이스 작업: {'PASS' if db_result else 'FAIL'}", "INFO")
        
        # 최종 결과
        self.log(f"\n총 테스트: {total_tests}, 성공: {passed_tests}, 실패: {total_tests - passed_tests}", "INFO")
        
        if passed_tests == total_tests:
            self.log(f"\n🎉 모든 테스트 통과! {self.protocol.upper()} 구성이 정상적으로 작동합니다.", "SUCCESS")
            return 0
        else:
            self.log(f"\n⚠️  일부 테스트 실패. 로그를 확인하세요.", "WARNING")
            return 1


def main():
    """메인 함수"""
    tester = HTTPSIntegrationTest()
    return tester.run_all_tests()


if __name__ == "__main__":
    sys.exit(main())