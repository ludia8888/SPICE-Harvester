"""
Simple unit test for OMS ontology router to verify functionality
Works around import issues by running as a standalone script
"""

import sys
import os
from pathlib import Path

# Add parent directory to path for imports
backend_path = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(backend_path))

from fastapi.testclient import TestClient
from typing import Dict, Any, List, Optional

# Import after setting path
from oms.routers.ontology import router
from oms.dependencies import set_services
from shared.utils.jsonld import JSONToJSONLDConverter
from shared.utils.label_mapper import LabelMapper


# Mock AsyncTerminusService for testing
class MockAsyncTerminusService:
    """Mock AsyncTerminusService for testing"""
    
    def __init__(self):
        self.databases = ["test_db"]
        self.ontologies = {}
        self.should_fail = False
    
    async def database_exists(self, name: str) -> bool:
        return name in self.databases
    
    async def list_ontology_classes(self, db_name: str) -> List[Dict[str, Any]]:
        if self.should_fail:
            raise Exception("Failed to list classes")
        return self.ontologies.get(db_name, {}).get("classes", [])
    
    async def list_properties(self, db_name: str) -> List[Dict[str, Any]]:
        return self.ontologies.get(db_name, {}).get("properties", [])
    
    async def list_relationships(self, db_name: str) -> List[Dict[str, Any]]:
        return self.ontologies.get(db_name, {}).get("relationships", [])
    
    async def create_ontology_class(self, db_name: str, class_data: Dict[str, Any]):
        if db_name not in self.ontologies:
            self.ontologies[db_name] = {"classes": [], "properties": [], "relationships": []}
        
        # Check for duplicates
        for cls in self.ontologies[db_name]["classes"]:
            if cls.get("id") == class_data.get("id"):
                raise Exception("Class already exists")
        
        self.ontologies[db_name]["classes"].append(class_data)
        return class_data
    
    async def create_property(self, db_name: str, property_data: Dict[str, Any]):
        if db_name not in self.ontologies:
            self.ontologies[db_name] = {"classes": [], "properties": [], "relationships": []}
        
        self.ontologies[db_name]["properties"].append(property_data)
        return property_data
    
    async def create_relationship(self, db_name: str, relationship_data: Dict[str, Any]):
        if db_name not in self.ontologies:
            self.ontologies[db_name] = {"classes": [], "properties": [], "relationships": []}
        
        self.ontologies[db_name]["relationships"].append(relationship_data)
        return relationship_data
    
    async def get_ontology(self, db_name: str, class_id: str) -> Dict[str, Any]:
        classes = self.ontologies.get(db_name, {}).get("classes", [])
        for cls in classes:
            if cls.get("id") == class_id:
                return cls
        raise Exception("Class not found")
    
    async def update_ontology_class(self, db_name: str, class_id: str, update_data: Dict[str, Any]):
        classes = self.ontologies.get(db_name, {}).get("classes", [])
        for cls in classes:
            if cls.get("id") == class_id:
                cls.update(update_data)
                return cls
        raise Exception("Class not found")
    
    async def delete_ontology_class(self, db_name: str, class_id: str):
        if db_name not in self.ontologies:
            raise Exception("Database not found")
        classes = self.ontologies[db_name]["classes"]
        self.ontologies[db_name]["classes"] = [c for c in classes if c.get("id") != class_id]
    
    async def query_ontologies(self, db_name: str, query_data: Dict[str, Any]):
        classes = self.ontologies.get(db_name, {}).get("classes", [])
        results = []
        for cls in classes:
            if query_data.get("type"):
                if cls.get("type") == query_data["type"]:
                    results.append(cls)
            else:
                results.append(cls)
        return results


def test_oms_ontology_operations():
    """Test basic OMS ontology operations"""
    print("\n=== Testing OMS Ontology Router ===")
    
    # Create test app
    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    
    # Setup mock service
    mock_terminus = MockAsyncTerminusService()
    set_services(mock_terminus, JSONToJSONLDConverter())
    
    # Test 1: List ontologies (empty)
    print("\n1. Testing list ontologies (empty)...")
    response = client.get("/ontology/test_db/list")
    if response.status_code != 200:
        print(f"Response status: {response.status_code}")
        print(f"Response body: {response.json()}")
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert len(response.json()["data"]["ontologies"]) == 0
    print("✓ List empty ontologies passed")
    
    # Test 2: Create ontology
    print("\n2. Testing create ontology...")
    ontology_data = {
        "id": "Person",
        "type": "Class",
        "label": {"en": "Person", "ko": "사람"},
        "description": {"en": "A human being"},
        "properties": []
    }
    response = client.post("/ontology/test_db/create", json=ontology_data)
    if response.status_code != 200:
        print(f"Response status: {response.status_code}")
        print(f"Response body: {response.json()}")
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    print("✓ Create ontology passed")
    
    # Test 3: Create duplicate ontology  
    print("\n3. Testing create duplicate ontology...")
    response = client.post("/ontology/test_db/create", json=ontology_data)
    assert response.status_code == 409
    assert "이미 존재합니다" in response.json()["detail"]
    print("✓ Duplicate ontology validation passed")
    
    # Test 4: Get ontology
    print("\n4. Testing get ontology...")
    response = client.get("/ontology/test_db/Person")
    if response.status_code != 200:
        print(f"Response status: {response.status_code}")
        print(f"Response body: {response.json()}")
    assert response.status_code == 200
    assert response.json()["data"]["id"] == "Person"
    print("✓ Get ontology passed")
    
    # Test 5: Update ontology
    print("\n5. Testing update ontology...")
    update_data = {
        "description": {"en": "An updated human being", "ko": "업데이트된 사람"}
    }
    response = client.put("/ontology/test_db/Person", json=update_data)
    if response.status_code != 200:
        print(f"Response status: {response.status_code}")
        print(f"Response body: {response.json()}")
    assert response.status_code == 200
    assert "수정되었습니다" in response.json()["message"]
    print("✓ Update ontology passed")
    
    # Test 6: Query ontologies
    print("\n6. Testing query ontologies...")
    query_data = {
        "type": "Class"
    }
    response = client.post("/ontology/test_db/query", json=query_data)
    if response.status_code != 200:
        print(f"Response status: {response.status_code}")
        print(f"Response body: {response.json()}")
    assert response.status_code == 200
    assert len(response.json()["data"]["results"]) >= 1
    print("✓ Query ontologies passed")
    
    # Test 7: List all ontologies
    print("\n7. Testing list all ontologies...")
    response = client.get("/ontology/test_db/list")
    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data["classes"]) == 1
    print("✓ List all ontologies passed")
    
    # Test 8: Delete ontology
    print("\n8. Testing delete ontology...")
    response = client.delete("/ontology/test_db/Person")
    if response.status_code != 200:
        print(f"Response status: {response.status_code}")
        print(f"Response body: {response.json()}")
    assert response.status_code == 200
    assert "삭제되었습니다" in response.json()["message"]
    print("✓ Delete ontology passed")
    
    # Test 9: Database not found
    print("\n9. Testing database not found...")
    response = client.get("/ontology/nonexistent_db/list")
    assert response.status_code == 404
    assert "찾을 수 없습니다" in response.json()["detail"]
    print("✓ Database not found error passed")
    
    # Test 10: Error handling
    print("\n10. Testing error handling...")
    mock_terminus.should_fail = True
    response = client.get("/ontology/test_db/list")
    assert response.status_code == 500
    assert "조회 실패" in response.json()["detail"]
    print("✓ Error handling passed")
    
    print("\n=== All OMS Ontology Router Tests Passed! ===")
    return True


if __name__ == "__main__":
    # Run the test directly
    try:
        success = test_oms_ontology_operations()
        if success:
            print("\n✅ OMS ontology router unit tests completed successfully!")
            exit(0)
        else:
            print("\n❌ OMS ontology router unit tests failed!")
            exit(1)
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)