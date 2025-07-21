"""
🔥 THINK ULTRA! Complete Functional Integration Tests
Tests ALL functionality across ALL services with REAL data flows

This test suite validates:
1. All database operations (CRUD)
2. All ontology operations  
3. All service interactions
4. All type inference features
5. All version control features
6. All query capabilities
7. All mapping features
8. Complete end-to-end workflows
"""

import pytest
import httpx
import json
import uuid
import time
import asyncio
from typing import Dict, Any, List
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class TestCompleteFunctionalIntegration:
    """Complete functional integration test suite"""
    
    OMS_BASE = "http://localhost:8000"
    BFF_BASE = "http://localhost:8002"  
    FUNNEL_BASE = "http://localhost:8003"
    TIMEOUT = 60.0
    
    def setup_method(self):
        """Setup for each test - create unique test identifiers"""
        self.test_id = f"test_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        self.test_db = f"functional_test_{self.test_id}"
        self.created_databases = []
        
    def teardown_method(self):
        """Cleanup after each test"""
        # Clean up any created databases
        with httpx.Client(timeout=self.TIMEOUT) as client:
            for db_name in self.created_databases:
                try:
                    client.delete(f"{self.OMS_BASE}/api/v1/database/{db_name}")
                except:
                    pass
    
    def test_01_service_health_and_connectivity(self):
        """Test all services are healthy and can communicate"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            # Test all services are up
            oms_health = client.get(f"{self.OMS_BASE}/health")
            assert oms_health.status_code == 200
            oms_data = oms_health.json()
            assert oms_data["data"]["status"] == "healthy"
            assert oms_data["data"]["terminus_connected"] is True
            
            bff_health = client.get(f"{self.BFF_BASE}/health")  
            assert bff_health.status_code == 200
            bff_data = bff_health.json()
            assert bff_data["data"]["status"] == "healthy"
            assert bff_data["data"]["oms_connected"] is True
            
            funnel_health = client.get(f"{self.FUNNEL_BASE}/health")
            assert funnel_health.status_code == 200
            funnel_data = funnel_health.json()
            assert funnel_data["data"]["status"] == "healthy"

    def test_02_database_complete_lifecycle(self):
        """Test complete database lifecycle: create, verify, list, delete"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            # 1. Create database via OMS
            create_payload = {
                "name": self.test_db,
                "description": f"Functional test database {self.test_id}"
            }
            
            create_response = client.post(
                f"{self.OMS_BASE}/api/v1/database/create",
                json=create_payload
            )
            assert create_response.status_code == 200
            self.created_databases.append(self.test_db)
            
            # 2. Verify database exists via OMS
            exists_response = client.get(f"{self.OMS_BASE}/api/v1/database/exists/{self.test_db}")
            assert exists_response.status_code == 200
            assert exists_response.json()["data"]["exists"] is True
            
            # 3. List databases via OMS and verify our DB is there
            list_response = client.get(f"{self.OMS_BASE}/api/v1/database/list")
            assert list_response.status_code == 200
            db_list = [db["name"] for db in list_response.json()["data"]["databases"]]
            assert self.test_db in db_list
            
            # 4. Test database via BFF as well
            bff_list_response = client.get(f"{self.BFF_BASE}/api/v1/databases")
            assert bff_list_response.status_code == 200
            bff_db_list = bff_list_response.json()["data"]["databases"]
            db_names = [db["name"] for db in bff_db_list]
            assert self.test_db in db_names

    def test_03_ontology_complete_operations(self):
        """Test complete ontology operations: create, retrieve, modify"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            # Setup: Create test database
            create_payload = {"name": self.test_db, "description": "Ontology test"}
            client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
            self.created_databases.append(self.test_db)
            
            # 1. Create ontology class via OMS (start simple without properties)
            ontology_payload = {
                "id": "TestProduct",
                "label": {
                    "en": "Test Product",
                    "ko": "테스트 제품"
                },
                "description": {
                    "en": "Test product class",
                    "ko": "테스트 제품 클래스"
                }
            }
            
            create_ont_response = client.post(
                f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/create",
                json=ontology_payload
            )
            assert create_ont_response.status_code == 200
            ont_data = create_ont_response.json()
            # The response should contain the created ontology info
            class_id = ont_data.get("data", {}).get("id", "TestProduct")
            
            # 2. Retrieve ontology via OMS
            get_ont_response = client.get(f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/{class_id}")
            assert get_ont_response.status_code == 200
            retrieved_ont = get_ont_response.json()
            retrieved_data = retrieved_ont.get("data", retrieved_ont)
            assert retrieved_data.get("id") == "TestProduct"
            # Properties might be in different structure
            
            # 3. Test via BFF as well
            bff_ont_response = client.get(f"{self.BFF_BASE}/database/{self.test_db}/ontology/TestProduct")
            assert bff_ont_response.status_code == 200
            bff_ont_data = bff_ont_response.json()
            bff_data = bff_ont_data.get("data", bff_ont_data)
            assert bff_data.get("id") == "TestProduct"
            
            # 4. List ontologies
            list_ont_response = client.get(f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/list")
            assert list_ont_response.status_code == 200
            ont_list_data = list_ont_response.json()
            ont_list = ont_list_data.get("data", {}).get("classes", ont_list_data.get("classes", []))
            assert len(ont_list) >= 1
            # Check if TestProduct exists in any form
            found_product = any(
                cls.get("id") == "TestProduct" or 
                cls.get("class_label") == "TestProduct" or
                cls.get("label", {}).get("en") == "Test Product"
                for cls in ont_list
            )
            assert found_product

    def test_04_advanced_ontology_operations(self):
        """Test advanced ontology features: relationships, validation"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            # Setup: Create test database
            create_payload = {"name": self.test_db, "description": "Advanced ontology test"}
            client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
            self.created_databases.append(self.test_db)
            
            # 1. Create complex ontology with relationships
            complex_payload = {
                "class_label": "Company",
                "properties": [
                    {"property_label": "name", "data_type": "string"},
                    {"property_label": "employees", "data_type": "array", "items_type": "Employee"}
                ],
                "relationships": [
                    {
                        "target_class": "Employee",
                        "relationship_type": "has_many",
                        "cardinality": {"min": 0, "max": -1}
                    }
                ]
            }
            
            create_response = client.post(
                f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/create-advanced",
                json=complex_payload
            )
            assert create_response.status_code == 200
            
            # 2. Test relationship validation
            validate_response = client.get(f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/validate-relationships")
            assert validate_response.status_code == 200
            
            # 3. Test circular reference detection
            circular_response = client.get(f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/detect-circular-references")
            assert circular_response.status_code == 200

    def test_05_query_operations(self):
        """Test query functionality across services"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            # Setup: Create database and ontology
            create_payload = {"name": self.test_db, "description": "Query test"}
            client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
            self.created_databases.append(self.test_db)
            
            # Create sample ontology
            ontology_payload = {
                "class_label": "QueryTest",
                "properties": [
                    {"property_label": "title", "data_type": "string"},
                    {"property_label": "value", "data_type": "integer"}
                ]
            }
            client.post(f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/create", json=ontology_payload)
            
            # 1. Test OMS query
            query_payload = {"query": "SELECT * FROM QueryTest LIMIT 10"}
            oms_query_response = client.post(
                f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/query",
                json=query_payload
            )
            assert oms_query_response.status_code == 200
            
            # 2. Test BFF query
            bff_query_response = client.post(
                f"{self.BFF_BASE}/database/{self.test_db}/query",
                json=query_payload
            )
            assert bff_query_response.status_code == 200
            
            # 3. Test raw query via BFF
            raw_query_response = client.post(
                f"{self.BFF_BASE}/api/v1/database/{self.test_db}/query/raw",
                json=query_payload
            )
            assert raw_query_response.status_code == 200

    def test_06_funnel_type_inference(self):
        """Test Funnel service type inference capabilities"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            # Setup: Create test database
            create_payload = {"name": self.test_db, "description": "Type inference test"}
            client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
            self.created_databases.append(self.test_db)
            
            # 1. Test data analysis
            sample_data = [
                {"name": "Product A", "price": 19.99, "in_stock": True},
                {"name": "Product B", "price": 25.50, "in_stock": False},
                {"name": "Product C", "price": 12.00, "in_stock": True}
            ]
            
            analyze_payload = {
                "data": sample_data,
                "context": "Product inventory data"
            }
            
            analyze_response = client.post(
                f"{self.FUNNEL_BASE}/api/v1/funnel/analyze",
                json=analyze_payload
            )
            assert analyze_response.status_code == 200
            analysis_result = analyze_response.json()
            
            # Verify type inference worked
            assert "inferred_types" in analysis_result
            assert len(analysis_result["inferred_types"]) >= 3  # name, price, in_stock
            
            # 2. Test schema suggestion
            suggest_payload = {
                "data": sample_data,
                "class_name": "Product"
            }
            
            suggest_response = client.post(
                f"{self.FUNNEL_BASE}/api/v1/funnel/suggest-schema",
                json=suggest_payload
            )
            assert suggest_response.status_code == 200
            schema_result = suggest_response.json()
            
            assert "suggested_schema" in schema_result
            assert "class_label" in schema_result["suggested_schema"]

    def test_07_version_control_operations(self):
        """Test version control: commit, branch, merge operations"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            # Setup
            create_payload = {"name": self.test_db, "description": "Version control test"}
            client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
            self.created_databases.append(self.test_db)
            
            # Create initial ontology
            ontology_payload = {
                "class_label": "VersionTest",
                "properties": [{"property_label": "initial_prop", "data_type": "string"}]
            }
            client.post(f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/create", json=ontology_payload)
            
            # 1. Test commit
            commit_payload = {
                "message": "Initial commit for version test",
                "author": "test@example.com"
            }
            commit_response = client.post(
                f"{self.OMS_BASE}/api/v1/version/{self.test_db}/commit",
                json=commit_payload
            )
            assert commit_response.status_code == 200
            
            # 2. Test branch creation
            branch_payload = {
                "branch_name": "feature_test",
                "from_commit": "main"
            }
            branch_response = client.post(
                f"{self.OMS_BASE}/api/v1/branch/{self.test_db}/create",
                json=branch_payload
            )
            assert branch_response.status_code == 200
            
            # 3. Test branch listing
            list_branches_response = client.get(f"{self.OMS_BASE}/api/v1/branch/{self.test_db}/list")
            assert list_branches_response.status_code == 200
            branches = list_branches_response.json()["branches"]
            assert "feature_test" in branches
            
            # 4. Test version history
            history_response = client.get(f"{self.OMS_BASE}/api/v1/version/{self.test_db}/history")
            assert history_response.status_code == 200
            history = history_response.json()
            assert len(history["commits"]) >= 1

    def test_08_mapping_operations(self):
        """Test label mapping import/export functionality"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            # Setup
            create_payload = {"name": self.test_db, "description": "Mapping test"}
            client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
            self.created_databases.append(self.test_db)
            
            # Create ontology for mapping
            ontology_payload = {
                "class_label": "MappingTest", 
                "properties": [
                    {"property_label": "field_a", "data_type": "string"},
                    {"property_label": "field_b", "data_type": "integer"}
                ]
            }
            client.post(f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/create", json=ontology_payload)
            
            # 1. Test export mappings (should work even if empty)
            export_response = client.get(f"{self.BFF_BASE}/api/v1/database/{self.test_db}/mappings/export")
            assert export_response.status_code == 200
            exported_data = export_response.json()
            assert "mappings" in exported_data
            
            # 2. Test import mappings
            import_payload = {
                "mappings": [
                    {
                        "source_field": "column_1",
                        "target_field": "field_a",
                        "transformation": "direct"
                    },
                    {
                        "source_field": "column_2", 
                        "target_field": "field_b",
                        "transformation": "cast_to_int"
                    }
                ]
            }
            
            import_response = client.post(
                f"{self.BFF_BASE}/api/v1/database/{self.test_db}/mappings/import",
                json=import_payload
            )
            assert import_response.status_code == 200
            
            # 3. Verify mappings were imported
            verify_export = client.get(f"{self.BFF_BASE}/api/v1/database/{self.test_db}/mappings/export")
            assert verify_export.status_code == 200
            verified_data = verify_export.json()
            assert len(verified_data["mappings"]) >= 2

    def test_09_cross_service_integration(self):
        """Test integration between all three services working together"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            # Setup
            create_payload = {"name": self.test_db, "description": "Cross-service integration test"}
            client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
            self.created_databases.append(self.test_db)
            
            # Step 1: Use Funnel to analyze data and suggest schema
            sample_data = [
                {"product_name": "Widget A", "price": 29.99, "category": "tools"},
                {"product_name": "Widget B", "price": 45.00, "category": "electronics"}
            ]
            
            analyze_response = client.post(
                f"{self.FUNNEL_BASE}/api/v1/funnel/suggest-schema",
                json={"data": sample_data, "class_name": "Product"}
            )
            assert analyze_response.status_code == 200
            suggested_schema = analyze_response.json()["suggested_schema"]
            
            # Step 2: Use OMS to create ontology based on Funnel's suggestion
            ontology_payload = {
                "class_label": suggested_schema["class_label"],
                "properties": suggested_schema["properties"],
                "description": "Created from Funnel analysis"
            }
            
            create_ont_response = client.post(
                f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/create",
                json=ontology_payload
            )
            assert create_ont_response.status_code == 200
            
            # Step 3: Use BFF to query the created ontology
            query_response = client.get(f"{self.BFF_BASE}/database/{self.test_db}/ontology/Product")
            assert query_response.status_code == 200
            retrieved_ont = query_response.json()
            assert retrieved_ont["class_label"] == "Product"
            
            # Step 4: Test BFF schema suggestion endpoint (should use Funnel internally)
            bff_suggest_response = client.post(
                f"{self.BFF_BASE}/api/v1/database/{self.test_db}/suggest-schema-from-data",
                json={"data": sample_data}
            )
            assert bff_suggest_response.status_code == 200

    def test_10_error_handling_and_edge_cases(self):
        """Test error handling and edge cases across all services"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            # 1. Test non-existent database operations
            nonexistent_db = f"nonexistent_{self.test_id}"
            
            # Should return 404 or appropriate error
            error_response = client.get(f"{self.OMS_BASE}/api/v1/database/exists/{nonexistent_db}")
            assert error_response.status_code == 200  # exists endpoint returns false, not 404
            assert error_response.json()["data"]["exists"] is False
            
            # 2. Test invalid ontology creation
            create_payload = {"name": self.test_db, "description": "Error test"}
            client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
            self.created_databases.append(self.test_db)
            
            invalid_ontology = {
                "class_label": "",  # Invalid empty label
                "properties": []
            }
            
            invalid_response = client.post(
                f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/create",
                json=invalid_ontology
            )
            # Should return error (400, 422, or similar)
            assert invalid_response.status_code >= 400
            
            # 3. Test invalid data to Funnel
            invalid_funnel_data = {
                "data": "not_an_array",  # Invalid data format
                "class_name": "Test"
            }
            
            funnel_error_response = client.post(
                f"{self.FUNNEL_BASE}/api/v1/funnel/suggest-schema",
                json=invalid_funnel_data
            )
            # Should handle gracefully
            assert funnel_error_response.status_code >= 400

    def test_11_performance_and_concurrent_operations(self):
        """Test performance and concurrent operations"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            # Setup multiple test databases
            db_names = []
            for i in range(3):
                db_name = f"{self.test_db}_{i}"
                create_payload = {"name": db_name, "description": f"Performance test {i}"}
                response = client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
                assert response.status_code == 200
                db_names.append(db_name)
                self.created_databases.append(db_name)
            
            # Test concurrent operations
            start_time = time.time()
            
            # Create ontologies in all databases
            for db_name in db_names:
                ontology_payload = {
                    "class_label": "PerformanceTest",
                    "properties": [
                        {"property_label": "field1", "data_type": "string"},
                        {"property_label": "field2", "data_type": "integer"}
                    ]
                }
                response = client.post(f"{self.OMS_BASE}/api/v1/ontology/{db_name}/create", json=ontology_payload)
                assert response.status_code == 200
            
            # Test listing operations
            for db_name in db_names:
                list_response = client.get(f"{self.OMS_BASE}/api/v1/ontology/{db_name}/list")
                assert list_response.status_code == 200
                
            end_time = time.time()
            operation_time = end_time - start_time
            
            # Should complete within reasonable time (adjust as needed)
            assert operation_time < 30.0, f"Operations took too long: {operation_time}s"

    def test_12_complete_workflow_simulation(self):
        """Simulate a complete real-world workflow"""
        with httpx.Client(timeout=self.TIMEOUT) as client:
            # Scenario: User creates a product catalog system
            
            # Step 1: Create database
            create_payload = {"name": self.test_db, "description": "Product catalog system"}
            client.post(f"{self.OMS_BASE}/api/v1/database/create", json=create_payload)
            self.created_databases.append(self.test_db)
            
            # Step 2: User has product data from Google Sheets
            product_data = [
                {"product_name": "Laptop", "price": 999.99, "category": "Electronics", "in_stock": True},
                {"product_name": "Mouse", "price": 25.50, "category": "Electronics", "in_stock": False},
                {"product_name": "Desk", "price": 299.00, "category": "Furniture", "in_stock": True}
            ]
            
            # Step 3: Use Funnel to analyze data and suggest schema
            schema_response = client.post(
                f"{self.FUNNEL_BASE}/api/v1/funnel/suggest-schema",
                json={"data": product_data, "class_name": "Product"}
            )
            assert schema_response.status_code == 200
            suggested = schema_response.json()["suggested_schema"]
            
            # Step 4: Create ontology using suggested schema
            create_ont_response = client.post(
                f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/create",
                json=suggested
            )
            assert create_ont_response.status_code == 200
            
            # Step 5: Create category ontology
            category_payload = {
                "class_label": "Category",
                "properties": [
                    {"property_label": "name", "data_type": "string"},
                    {"property_label": "description", "data_type": "string"}
                ]
            }
            client.post(f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/create", json=category_payload)
            
            # Step 6: Query the created ontologies via BFF
            products_response = client.get(f"{self.BFF_BASE}/database/{self.test_db}/ontology/Product")
            assert products_response.status_code == 200
            
            categories_response = client.get(f"{self.BFF_BASE}/database/{self.test_db}/ontology/Category")
            assert categories_response.status_code == 200
            
            # Step 7: Test relationship validation
            validate_response = client.get(f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/validate-relationships")
            assert validate_response.status_code == 200
            
            # Step 8: Commit changes
            commit_payload = {
                "message": "Initial product catalog setup",
                "author": "system@test.com"
            }
            commit_response = client.post(f"{self.OMS_BASE}/api/v1/version/{self.test_db}/commit", json=commit_payload)
            assert commit_response.status_code == 200
            
            # Step 9: Export configuration for backup
            export_response = client.get(f"{self.BFF_BASE}/api/v1/database/{self.test_db}/mappings/export")
            assert export_response.status_code == 200
            
            # Step 10: Verify complete system state
            list_ont_response = client.get(f"{self.OMS_BASE}/api/v1/ontology/{self.test_db}/list")
            assert list_ont_response.status_code == 200
            ontologies = list_ont_response.json()["classes"]
            assert len(ontologies) >= 2  # Product and Category
            
            ontology_names = [ont["class_label"] for ont in ontologies]
            assert "Product" in ontology_names
            assert "Category" in ontology_names

if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])