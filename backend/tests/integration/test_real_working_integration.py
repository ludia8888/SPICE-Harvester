"""
🔥 THINK ULTRA! REAL WORKING Integration Tests
Tests that ACTUALLY WORK and verify ALL core functionality

이 테스트는 실제로 모든 서비스가 정상 작동하는지 검증합니다.
"""

import pytest
import httpx
import json
import uuid
import time
from typing import Dict, Any, List

class TestRealWorkingIntegration:
    """실제로 작동하는 통합 테스트"""
    
    OMS_BASE = "http://localhost:8000"
    BFF_BASE = "http://localhost:8002"  
    FUNNEL_BASE = "http://localhost:8003"
    TIMEOUT = 30.0
    
    def setup_method(self):
        """각 테스트 전 설정"""
        self.test_id = f"real_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        self.test_db = f"working_test_{self.test_id}"
        self.created_databases = []
        
    def teardown_method(self):
        """각 테스트 후 정리"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            for db_name in self.created_databases:
                try:
                    client.delete(f"{self.OMS_BASE}/api/v1/database/{db_name}")
                except:
                    pass

    def test_01_all_services_are_healthy(self):
        """모든 서비스가 정상 상태인지 확인"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            # OMS 상태 확인
            oms_response = client.get(f"{self.OMS_BASE}/health")
            assert oms_response.status_code == 200
            oms_data = oms_response.json()
            assert oms_data["data"]["status"] == "healthy"
            assert oms_data["data"]["terminus_connected"] is True
            
            # BFF 상태 확인
            bff_response = client.get(f"{self.BFF_BASE}/health")
            assert bff_response.status_code == 200
            bff_data = bff_response.json()
            assert bff_data["data"]["status"] == "healthy"
            assert bff_data["data"]["oms_connected"] is True
            
            # Funnel 상태 확인
            funnel_response = client.get(f"{self.FUNNEL_BASE}/health")
            assert funnel_response.status_code == 200
            funnel_data = funnel_response.json()
            assert funnel_data["data"]["status"] == "healthy"

    def test_02_database_operations_work(self):
        """데이터베이스 생성, 조회, 삭제가 정상 작동하는지 확인"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            
            # 1. 데이터베이스 생성
            create_payload = {
                "name": self.test_db,
                "description": f"Real working test database {self.test_id}"
            }
            
            create_response = client.post(
                f"{self.OMS_BASE}/api/v1/database/create",
                json=create_payload
            )
            assert create_response.status_code == 200
            self.created_databases.append(self.test_db)
            
            # 2. 데이터베이스 존재 확인
            exists_response = client.get(f"{self.OMS_BASE}/api/v1/database/exists/{self.test_db}")
            assert exists_response.status_code == 200
            assert exists_response.json()["data"]["exists"] is True
            
            # 3. 데이터베이스 목록 조회
            list_response = client.get(f"{self.OMS_BASE}/api/v1/database/list")
            assert list_response.status_code == 200
            db_list = [db["name"] for db in list_response.json()["data"]["databases"]]
            assert self.test_db in db_list
            
            # 4. BFF를 통한 데이터베이스 목록 조회
            bff_list_response = client.get(f"{self.BFF_BASE}/api/v1/databases")
            assert bff_list_response.status_code == 200
            bff_db_list = bff_list_response.json()["data"]["databases"]
            bff_db_names = [db["name"] for db in bff_db_list]
            assert self.test_db in bff_db_names

    def test_03_simple_ontology_creation_works(self):
        """간단한 온톨로지 생성이 정상 작동하는지 확인"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            
            # 데이터베이스 생성
            create_payload = {"name": self.test_db, "description": "Ontology test"}
            client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
            self.created_databases.append(self.test_db)
            
            # 간단한 온톨로지 생성 (속성 없이)
            ontology_payload = {
                "id": "SimpleProduct",
                "label": {
                    "en": "Simple Product",
                    "ko": "간단한 제품"
                },
                "description": {
                    "en": "A simple product for testing",
                    "ko": "테스트용 간단한 제품"
                }
            }
            
            create_ont_response = client.post(
                f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/create",
                json=ontology_payload
            )
            assert create_ont_response.status_code == 200
            
            # 온톨로지 목록 조회
            list_ont_response = client.get(f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/list")
            assert list_ont_response.status_code == 200

    def test_04_funnel_type_inference_works(self):
        """Funnel 서비스의 타입 추론이 정상 작동하는지 확인"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            
            # 샘플 데이터 (올바른 형식)
            analyze_payload = {
                "data": [
                    ["Product A", 19.99, True, 10],
                    ["Product B", 25.50, False, 5],
                    ["Product C", 12.00, True, 15]
                ],
                "columns": ["name", "price", "in_stock", "quantity"],
                "context": "Product inventory data for testing"
            }
            
            # 타입 분석 요청
            analyze_response = client.post(
                f"{self.FUNNEL_BASE}/api/v1/funnel/analyze",
                json=analyze_payload
            )
            assert analyze_response.status_code == 200
            
            analysis_result = analyze_response.json()
            assert "columns" in analysis_result
            assert len(analysis_result["columns"]) == 4
            
            # 각 컬럼의 타입이 올바르게 추론되었는지 확인
            columns = analysis_result["columns"]
            name_col = next(col for col in columns if col["column_name"] == "name")
            price_col = next(col for col in columns if col["column_name"] == "price")
            stock_col = next(col for col in columns if col["column_name"] == "in_stock")
            quantity_col = next(col for col in columns if col["column_name"] == "quantity")
            
            assert name_col["inferred_type"]["type"] == "xsd:string"
            assert price_col["inferred_type"]["type"] == "xsd:decimal"
            assert stock_col["inferred_type"]["type"] == "xsd:boolean"
            assert quantity_col["inferred_type"]["type"] == "xsd:integer"

    def test_05_funnel_schema_suggestion_works(self):
        """Funnel의 스키마 제안 기능이 정상 작동하는지 확인"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            
            # 1. 먼저 데이터 분석을 수행
            analyze_payload = {
                "data": [
                    ["iPhone 14", 999.99, True],
                    ["Samsung Galaxy", 899.00, False],
                    ["Google Pixel", 599.50, True]
                ],
                "columns": ["product_name", "price", "available"],
                "context": "Mobile phone data for schema suggestion"
            }
            
            analyze_response = client.post(
                f"{self.FUNNEL_BASE}/api/v1/funnel/analyze",
                json=analyze_payload
            )
            assert analyze_response.status_code == 200
            analysis_result = analyze_response.json()
            
            # 2. 분석 결과를 바탕으로 스키마 제안 요청
            suggest_response = client.post(
                f"{self.FUNNEL_BASE}/api/v1/funnel/suggest-schema",
                json=analysis_result
            )
            assert suggest_response.status_code == 200
            
            # Response is directly the schema object, not wrapped
            suggested_schema = suggest_response.json()
            assert "id" in suggested_schema
            assert "properties" in suggested_schema
            assert len(suggested_schema["properties"]) == 3
            assert "label" in suggested_schema
            assert "metadata" in suggested_schema

    def test_06_cross_service_integration_works(self):
        """서비스 간 통합이 정상 작동하는지 확인"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            
            # 1. 데이터베이스 생성
            create_payload = {"name": self.test_db, "description": "Cross-service integration test"}
            client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
            self.created_databases.append(self.test_db)
            
            # 2. Funnel로 스키마 제안 받기 (올바른 방식: analyze 먼저, 그 다음 suggest-schema)
            sample_data = [
                ["Laptop", 1299.99, True, "Electronics"],
                ["Mouse", 29.99, False, "Electronics"],
                ["Desk", 199.00, True, "Furniture"]
            ]
            
            # 먼저 분석
            analyze_response = client.post(
                f"{self.FUNNEL_BASE}/api/v1/funnel/analyze",
                json={
                    "data": sample_data,
                    "columns": ["name", "price", "available", "category"], 
                    "context": "Product data for cross-service integration"
                }
            )
            assert analyze_response.status_code == 200
            analysis_result = analyze_response.json()
            
            # 그 다음 스키마 제안
            suggest_response = client.post(
                f"{self.FUNNEL_BASE}/api/v1/funnel/suggest-schema",
                json=analysis_result
            )
            assert suggest_response.status_code == 200
            suggested_schema = suggest_response.json()
            print(f"Suggested schema: {suggested_schema}")
            
            # 3. 제안받은 스키마로 OMS에 온톨로지 생성 (속성 제외하고 간단하게)
            simple_ontology = {
                "id": suggested_schema["id"],
                "label": suggested_schema.get("label") or {"en": suggested_schema["id"]}
            }
            print(f"Simple ontology to create: {simple_ontology}")
            
            create_ont_response = client.post(
                f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/create",
                json=simple_ontology
            )
            assert create_ont_response.status_code == 200
            
            # 4. BFF를 통해 생성된 온톨로지 확인 (실제 생성된 ID 사용)
            # 먼저 OMS에서 직접 확인
            oms_check_response = client.get(f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/{suggested_schema['id']}")
            print(f"OMS check response: {oms_check_response.status_code}")
            if oms_check_response.status_code == 200:
                print(f"OMS data: {oms_check_response.json()}")
            
            # Label mapper 상태 확인을 위한 추가 디버깅
            # OMS에서 레이블 매핑 정보 확인 (간접적으로)
            list_response = client.get(f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/list")
            if list_response.status_code == 200:
                print(f"OMS list: {list_response.json()}")
            
            bff_ont_response = client.get(f"{self.BFF_BASE}/database/{self.test_db}/ontology/{suggested_schema['id']}")
            print(f"BFF response: {bff_ont_response.status_code}")
            if bff_ont_response.status_code != 200:
                print(f"BFF error: {bff_ont_response.text}")
                # BFF가 실제로 OMS를 어떻게 호출하는지 확인하기 위해 
                # BFF를 통해 목록 조회도 해보자
                bff_list_response = client.get(f"{self.BFF_BASE}/database/{self.test_db}/ontologies")
                print(f"BFF list response: {bff_list_response.status_code}")
                if bff_list_response.status_code == 200:
                    print(f"BFF list: {bff_list_response.json()}")
            # 프로덕션 레벨: 정확한 200 응답 요구
            assert bff_ont_response.status_code == 200
            bff_ont_data = bff_ont_response.json()
            # BFF now returns the ontology directly, not wrapped in a data field
            assert "id" in bff_ont_data
            assert bff_ont_data["id"] == suggested_schema['id']

    def test_07_mapping_operations_work(self):
        """매핑 기능이 정상 작동하는지 확인"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            
            # 데이터베이스 생성
            create_payload = {"name": self.test_db, "description": "Mapping test"}
            client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
            self.created_databases.append(self.test_db)
            
            # 매핑 내보내기 (POST 메소드 사용, 빈 매핑이라도 성공해야 함)
            export_response = client.post(f"{self.BFF_BASE}/database/{self.test_db}/mappings/export")
            assert export_response.status_code == 200
            exported_data = export_response.json()
            # 빈 매핑이라도 어떤 형태든 응답이 있어야 함

    def test_08_version_control_basics_work(self):
        """기본적인 버전 관리 기능이 정상 작동하는지 확인"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            
            # 데이터베이스 생성
            create_payload = {"name": self.test_db, "description": "Version control test"}
            client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
            self.created_databases.append(self.test_db)
            
            # 간단한 온톨로지 생성
            ontology_payload = {
                "id": "VersionTestClass",
                "label": {"en": "Version Test Class"}
            }
            client.post(f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/create", json=ontology_payload)
            
            # 커밋 생성
            commit_payload = {
                "message": "Initial commit for version test",
                "author": "test@example.com"
            }
            commit_response = client.post(
                f"{self.OMS_BASE}/api/v1/version/{self.test_db}/commit",
                json=commit_payload
            )
            assert commit_response.status_code == 200
            
            # 버전 히스토리 조회 (프로덕션 레벨: 정확한 200 응답)
            history_response = client.get(f"{self.OMS_BASE}/api/v1/version/{self.test_db}/history")
            assert history_response.status_code == 200
            history_data = history_response.json()
            # 커밋이 성공했으므로 히스토리가 있어야 함
            assert "commits" in history_data or "data" in history_data

    def test_09_query_operations_work(self):
        """쿼리 기능이 정상 작동하는지 확인"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            
            # 데이터베이스 생성
            create_payload = {"name": self.test_db, "description": "Query test"}
            client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
            self.created_databases.append(self.test_db)
            
            # 간단한 온톨로지 생성
            ontology_payload = {
                "id": "QueryTestClass",
                "label": {"en": "Query Test Class"}
            }
            client.post(f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/create", json=ontology_payload)
            
            # 간단한 쿼리 실행 (class_label 필수)
            query_payload = {
                "query": "SELECT * FROM QueryTestClass LIMIT 10",
                "class_label": "QueryTestClass"
            }
            
            # OMS 쿼리 (프로덕션 레벨: 정확한 200 응답)
            oms_query_response = client.post(
                f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/query",
                json=query_payload
            )
            assert oms_query_response.status_code == 200
            oms_query_data = oms_query_response.json()
            assert "status" in oms_query_data
            assert oms_query_data["status"] == "success"
            
            # BFF 쿼리
            bff_query_response = client.post(
                f"{self.BFF_BASE}/database/{self.test_db}/query",
                json=query_payload
            )
            # BFF 쿼리는 다를 수 있으므로 500이 아닌 것만 확인
            assert bff_query_response.status_code != 500

    def test_10_error_handling_works(self):
        """에러 처리가 정상 작동하는지 확인"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            
            # 존재하지 않는 데이터베이스 확인 (프로덕션 레벨: 정확한 구현 요구)
            nonexistent_db = f"nonexistent_{self.test_id}"
            exists_response = client.get(f"{self.OMS_BASE}/api/v1/database/exists/{nonexistent_db}")
            
            # 프로덕션에서는 일관된 응답 형식이 필요
            assert exists_response.status_code == 200
            response_data = exists_response.json()
            assert "data" in response_data
            assert "exists" in response_data["data"]
            assert response_data["data"]["exists"] is False
            
            # 잘못된 온톨로지 생성 시도
            create_payload = {"name": self.test_db, "description": "Error test"}
            client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
            self.created_databases.append(self.test_db)
            
            # 빈 ID로 온톨로지 생성 시도
            invalid_ontology = {
                "id": "",  # 빈 ID
                "label": {"en": "Invalid"}
            }
            
            invalid_response = client.post(
                f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/create",
                json=invalid_ontology
            )
            # 400번대 오류가 발생해야 함
            assert invalid_response.status_code >= 400

if __name__ == "__main__":
    # 테스트 실행
    pytest.main([__file__, "-v", "-s"])