"""
Simple unit test for OMS database router to verify functionality
Works around import issues by running as a standalone script
"""

import sys
import os
from pathlib import Path

# Add parent directory to path for imports
backend_path = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(backend_path))

import pytest
from fastapi.testclient import TestClient
from typing import List, Optional

# Import after setting path
from oms.routers.database import router
from oms.dependencies import set_services
from shared.utils.jsonld import JSONToJSONLDConverter


# Mock AsyncTerminusService for testing
class MockAsyncTerminusService:
    """Mock AsyncTerminusService for testing"""
    
    def __init__(self):
        self.databases = []
        self.should_fail = False
    
    async def list_databases(self) -> List[str]:
        if self.should_fail:
            raise Exception("Failed to list databases")
        return self.databases
    
    async def create_database(self, name: str, description: Optional[str] = None):
        if "existing_db" in name:
            raise Exception("데이터베이스가 이미 존재합니다")
        return {"name": name, "description": description}
    
    async def database_exists(self, name: str) -> bool:
        return name in self.databases
    
    async def delete_database(self, name: str):
        if name not in self.databases:
            raise Exception("Database not found")
        self.databases.remove(name)


def test_oms_database_operations():
    """Test basic OMS database operations"""
    print("\n=== Testing OMS Database Router ===")
    
    # Create test app
    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    
    # Setup mock service
    mock_terminus = MockAsyncTerminusService()
    set_services(mock_terminus, JSONToJSONLDConverter())
    
    # Test 1: List databases (empty)
    print("\n1. Testing list databases (empty)...")
    response = client.get("/database/list")
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert response.json()["data"]["databases"] == []
    print("✓ List empty databases passed")
    
    # Test 2: Create database
    print("\n2. Testing create database...")
    response = client.post("/database/create", json={"name": "test_db", "description": "Test database"})
    assert response.status_code == 200
    assert "생성되었습니다" in response.json()["message"]
    print("✓ Create database passed")
    
    # Test 3: Create database with invalid name
    print("\n3. Testing create database with invalid name...")
    response = client.post("/database/create", json={"name": "test@db"})
    assert response.status_code == 400
    assert "보안 위반" in response.json()["detail"]
    print("✓ Invalid name validation passed")
    
    # Test 4: Create database without name
    print("\n4. Testing create database without name...")
    response = client.post("/database/create", json={"description": "No name"})
    assert response.status_code == 400
    assert "이름이 필요합니다" in response.json()["detail"]
    print("✓ Missing name validation passed")
    
    # Test 5: Delete protected database
    print("\n5. Testing delete protected database...")
    response = client.delete("/database/_system")
    assert response.status_code == 400
    assert "보안 위반" in response.json()["detail"]
    print("✓ Protected database validation passed")
    
    # Test 6: Database exists check
    print("\n6. Testing database exists...")
    mock_terminus.databases = ["existing_db"]
    response = client.get("/database/exists/existing_db")
    assert response.status_code == 200
    assert response.json()["data"]["exists"] is True
    print("✓ Database exists check passed")
    
    # Test 7: Database not exists
    print("\n7. Testing database not exists...")
    response = client.get("/database/exists/nonexistent_db")
    assert response.status_code == 404
    assert "찾을 수 없습니다" in response.json()["detail"]
    print("✓ Database not exists check passed")
    
    # Test 8: Error handling
    print("\n8. Testing error handling...")
    mock_terminus.should_fail = True
    response = client.get("/database/list")
    assert response.status_code == 500
    assert "조회 실패" in response.json()["detail"]
    print("✓ Error handling passed")
    
    print("\n=== All OMS Database Router Tests Passed! ===")
    return True


if __name__ == "__main__":
    # Run the test directly
    try:
        success = test_oms_database_operations()
        if success:
            print("\n✅ OMS database router unit tests completed successfully!")
            exit(0)
        else:
            print("\n❌ OMS database router unit tests failed!")
            exit(1)
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)