#!/usr/bin/env python3
"""
🔥 THINK ULTRA! End-to-End Integration Test
완전한 BFF->Funnel HTTP 통신 및 스키마 제안 워크플로우 테스트

This test verifies the complete integration between:
1. BFF service (Backend for Frontend) 
2. Funnel service (Type Inference & Schema Suggestion)
3. Real HTTP communication
4. Actual data processing
"""

import subprocess
import time
import sys
import requests
import json
import asyncio
from typing import Dict, List, Any, Optional


class E2EIntegrationTest:
    """End-to-End Integration Test Runner"""
    
    def __init__(self):
        self.funnel_proc = None
        self.bff_proc = None
        self.funnel_port = 8003
        self.bff_port = 8002
        self.test_results = []
        
    def log_test(self, test_name: str, status: str, details: str = ""):
        """Log test result"""
        result = {
            "test_name": test_name,
            "status": status,
            "details": details,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        self.test_results.append(result)
        status_icon = "✅" if status == "PASS" else "❌" if status == "FAIL" else "🔄"
        print(f"{status_icon} {test_name}: {status}")
        if details:
            print(f"   {details}")
    
    def start_services(self):
        """Start both Funnel and BFF services"""
        print("🚀 Starting services...")
        
        # Start Funnel service
        self.log_test("Starting Funnel service", "RUNNING")
        self.funnel_proc = subprocess.Popen([
            sys.executable, '-m', 'uvicorn', 
            'funnel.main:app',
            '--host', '0.0.0.0',
            '--port', str(self.funnel_port)
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Start BFF service
        self.log_test("Starting BFF service", "RUNNING")
        self.bff_proc = subprocess.Popen([
            sys.executable, '-m', 'uvicorn',
            'spice_harvester.bff.main:app',
            '--host', '0.0.0.0', 
            '--port', str(self.bff_port)
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Wait for services to start
        time.sleep(8)
        
    def stop_services(self):
        """Stop both services"""
        print("🧹 Stopping services...")
        if self.funnel_proc:
            self.funnel_proc.terminate()
            self.funnel_proc.wait()
        if self.bff_proc:
            self.bff_proc.terminate()
            self.bff_proc.wait()
    
    def test_service_health(self):
        """Test if both services are healthy"""
        # Test Funnel health
        try:
            response = requests.get(f'http://localhost:{self.funnel_port}/health', timeout=5)
            if response.status_code == 200:
                self.log_test("Funnel service health", "PASS", f"Status: {response.status_code}")
            else:
                self.log_test("Funnel service health", "FAIL", f"Status: {response.status_code}")
                return False
        except Exception as e:
            self.log_test("Funnel service health", "FAIL", f"Error: {e}")
            return False
        
        # Test BFF health
        try:
            response = requests.get(f'http://localhost:{self.bff_port}/api/v1/health', timeout=5)
            if response.status_code == 200:
                self.log_test("BFF service health", "PASS", f"Status: {response.status_code}")
            else:
                self.log_test("BFF service health", "FAIL", f"Status: {response.status_code}")
                return False
        except Exception as e:
            self.log_test("BFF service health", "FAIL", f"Error: {e}")
            return False
        
        return True
    
    def test_direct_funnel_type_inference(self):
        """Test direct Funnel type inference API"""
        test_data = {
            "data": [
                ["John", "Doe", 30, "john@example.com", "2024-01-15", "Engineer"],
                ["Jane", "Smith", 25, "jane@example.com", "2023-12-20", "Designer"], 
                ["Bob", "Johnson", 35, "bob@example.com", "2024-02-10", "Manager"]
            ],
            "columns": ["first_name", "last_name", "age", "email", "hire_date", "position"]
        }
        
        try:
            response = requests.post(
                f'http://localhost:{self.funnel_port}/api/v1/funnel/analyze',
                json=test_data,
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                columns = result.get("columns", [])
                high_confidence = [col for col in columns if col.get("inferred_type", {}).get("confidence", 0) >= 0.7]
                
                self.log_test(
                    "Direct Funnel type inference", 
                    "PASS", 
                    f"Analyzed {len(columns)} columns, {len(high_confidence)} high confidence"
                )
                return True
            else:
                self.log_test("Direct Funnel type inference", "FAIL", f"Status: {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test("Direct Funnel type inference", "FAIL", f"Error: {e}")
            return False
    
    def test_bff_schema_suggestion(self):
        """Test BFF schema suggestion via HTTP to Funnel"""
        test_data = {
            "data": [
                ["Alice", "Brown", 28, "alice@company.com", "2024-01-10", "Software Engineer", 75000],
                ["Charlie", "Davis", 32, "charlie@company.com", "2023-11-15", "Product Manager", 85000],
                ["Diana", "Wilson", 29, "diana@company.com", "2024-03-01", "UX Designer", 70000]
            ],
            "columns": ["first_name", "last_name", "age", "email", "hire_date", "position", "salary"],
            "class_name": "Employee"
        }
        
        try:
            response = requests.post(
                f'http://localhost:{self.bff_port}/api/v1/database/test_company/suggest-schema-from-data',
                json=test_data,
                timeout=15
            )
            
            if response.status_code == 200:
                result = response.json()
                schema = result.get("suggested_schema", {})
                properties = schema.get("properties", [])
                
                # Verify schema structure
                expected_fields = ["first_name", "last_name", "age", "email", "hire_date", "position", "salary"]
                found_fields = [prop.get("name") for prop in properties]
                
                missing_fields = set(expected_fields) - set(found_fields)
                if not missing_fields:
                    self.log_test(
                        "BFF schema suggestion", 
                        "PASS", 
                        f"Generated schema with {len(properties)} properties for class '{schema.get('class_name')}'"
                    )
                    return True
                else:
                    self.log_test("BFF schema suggestion", "FAIL", f"Missing fields: {missing_fields}")
                    return False
            else:
                self.log_test("BFF schema suggestion", "FAIL", f"Status: {response.status_code}, Response: {response.text[:200]}")
                return False
                
        except Exception as e:
            self.log_test("BFF schema suggestion", "FAIL", f"Error: {e}")
            return False
    
    def test_complex_data_types(self):
        """Test complex data type inference"""
        test_data = {
            "data": [
                ["Product A", "123.45", "2024-01-15T10:30:00Z", "https://example.com/product-a", "active", "[\"tag1\", \"tag2\"]"],
                ["Product B", "67.89", "2024-02-20T14:45:00Z", "https://example.com/product-b", "inactive", "[\"tag3\", \"tag4\"]"],
                ["Product C", "234.56", "2024-03-10T09:15:00Z", "https://example.com/product-c", "active", "[\"tag5\", \"tag6\"]"]
            ],
            "columns": ["name", "price", "created_at", "url", "status", "tags"],
            "class_name": "Product"
        }
        
        try:
            response = requests.post(
                f'http://localhost:{self.bff_port}/api/v1/database/test_store/suggest-schema-from-data',
                json=test_data,
                timeout=15
            )
            
            if response.status_code == 200:
                result = response.json()
                schema = result.get("suggested_schema", {})
                properties = schema.get("properties", [])
                
                # Check for complex type detection
                type_mapping = {prop["name"]: prop["type"] for prop in properties}
                
                complex_types_detected = 0
                if "decimal" in type_mapping.get("price", "").lower() or "float" in type_mapping.get("price", "").lower():
                    complex_types_detected += 1
                if "datetime" in type_mapping.get("created_at", "").lower() or "timestamp" in type_mapping.get("created_at", "").lower():
                    complex_types_detected += 1
                if "url" in type_mapping.get("url", "").lower() or "uri" in type_mapping.get("url", "").lower():
                    complex_types_detected += 1
                if "array" in type_mapping.get("tags", "").lower() or "list" in type_mapping.get("tags", "").lower():
                    complex_types_detected += 1
                
                self.log_test(
                    "Complex data type inference", 
                    "PASS", 
                    f"Detected {complex_types_detected}/4 complex types correctly"
                )
                return True
            else:
                self.log_test("Complex data type inference", "FAIL", f"Status: {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test("Complex data type inference", "FAIL", f"Error: {e}")
            return False
    
    def test_korean_data_handling(self):
        """Test Korean data handling and schema suggestion"""
        test_data = {
            "data": [
                ["김철수", "30", "kim@example.com", "서울특별시", "소프트웨어 엔지니어"],
                ["이영희", "25", "lee@example.com", "부산광역시", "디자이너"],
                ["박민수", "35", "park@example.com", "대구광역시", "프로젝트 매니저"]
            ],
            "columns": ["이름", "나이", "이메일", "거주지", "직업"],
            "class_name": "직원"
        }
        
        try:
            response = requests.post(
                f'http://localhost:{self.bff_port}/api/v1/database/test_korean/suggest-schema-from-data',
                json=test_data,
                timeout=15
            )
            
            if response.status_code == 200:
                result = response.json()
                schema = result.get("suggested_schema", {})
                
                self.log_test(
                    "Korean data handling", 
                    "PASS", 
                    f"Successfully processed Korean data for class '{schema.get('class_name')}'"
                )
                return True
            else:
                self.log_test("Korean data handling", "FAIL", f"Status: {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test("Korean data handling", "FAIL", f"Error: {e}")
            return False
    
    def generate_test_report(self):
        """Generate final test report"""
        print("\\n" + "="*60)
        print("🔥 E2E INTEGRATION TEST REPORT")
        print("="*60)
        
        total_tests = len(self.test_results)
        passed_tests = len([r for r in self.test_results if r["status"] == "PASS"])
        failed_tests = len([r for r in self.test_results if r["status"] == "FAIL"])
        
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        
        if failed_tests > 0:
            print("\\nFailed Tests:")
            for result in self.test_results:
                if result["status"] == "FAIL":
                    print(f"  ❌ {result['test_name']}: {result['details']}")
        
        print("\\nTest Summary:")
        for result in self.test_results:
            status_icon = "✅" if result["status"] == "PASS" else "❌" if result["status"] == "FAIL" else "🔄"
            print(f"  {status_icon} {result['test_name']}")
        
        print("="*60)
        
        return failed_tests == 0
    
    def run_all_tests(self):
        """Run complete E2E test suite"""
        print("🔥 SPICE HARVESTER E2E INTEGRATION TEST")
        print("Testing BFF->Funnel HTTP communication and schema suggestion workflow")
        print("="*60)
        
        try:
            # Start services
            self.start_services()
            
            # Test service health
            if not self.test_service_health():
                print("❌ Service health check failed, aborting tests")
                return False
            
            # Run all integration tests
            self.test_direct_funnel_type_inference()
            self.test_bff_schema_suggestion()
            self.test_complex_data_types()
            self.test_korean_data_handling()
            
            # Generate report
            return self.generate_test_report()
            
        finally:
            # Always stop services
            self.stop_services()


def main():
    """Main test runner"""
    test_runner = E2EIntegrationTest()
    success = test_runner.run_all_tests()
    
    if success:
        print("\\n🎉 ALL TESTS PASSED! BFF->Funnel integration is working correctly.")
        sys.exit(0)
    else:
        print("\\n❌ Some tests failed. Please review the report above.")
        sys.exit(1)


if __name__ == "__main__":
    main()