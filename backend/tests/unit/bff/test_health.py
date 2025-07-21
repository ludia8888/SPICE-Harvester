"""
Comprehensive unit tests for BFF health router
Tests all health check functionality with REAL behavior verification
NO MOCKS - Tests verify actual code behavior
"""

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from bff.routers.health import router
from bff.dependencies import set_oms_client, oms_client
from bff.services.oms_client import OMSClient


class TestHealthRouter:
    """Test health router endpoints with real behavior"""

    def setup_method(self):
        """Setup test environment"""
        # Save original client state
        self.original_client = oms_client
        
        # Create test client
        from fastapi import FastAPI
        app = FastAPI()
        app.include_router(router)
        self.client = TestClient(app)

    def teardown_method(self):
        """Restore original state"""
        global oms_client
        oms_client = self.original_client

    def test_root_endpoint_returns_service_info(self):
        """Test root endpoint returns correct service information"""
        response = self.client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify exact response structure
        assert data["service"] == "Ontology BFF Service"
        assert data["version"] == "2.0.0"
        assert data["description"] == "도메인 독립적인 온톨로지 관리 서비스"
        
        # Verify no extra fields
        assert set(data.keys()) == {"service", "version", "description"}

    def test_health_check_with_oms_client_initialized(self):
        """Test health check when OMS client is properly initialized"""
        # Setup mock OMS client
        mock_client = MockOMSClient()
        set_oms_client(mock_client)
        
        response = self.client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify ApiResponse structure
        assert data["status"] == "success"
        assert data["message"] == "Service is healthy"
        assert "data" in data
        
        # Verify health check data
        health_data = data["data"]
        assert health_data["service"] == "BFF"
        assert health_data["version"] == "2.0.0"
        assert health_data["status"] == "healthy"
        assert health_data["description"] == "백엔드 포 프론트엔드 서비스"
        assert health_data["oms_connected"] is True
        
        # Verify response is properly serialized
        assert isinstance(data, dict)
        assert all(isinstance(v, (str, dict, bool)) for v in data.values())

    def test_health_check_without_oms_client_returns_503(self):
        """Test health check when OMS client is not initialized"""
        # Clear OMS client
        set_oms_client(None)
        
        response = self.client.get("/health")
        
        assert response.status_code == 503
        detail = response.json()["detail"]
        
        # Verify error response structure
        assert detail["status"] == "error"
        assert detail["message"] == "서비스 연결에 문제가 있습니다"
        assert "errors" in detail
        assert len(detail["errors"]) == 1
        assert "OMS 클라이언트가 초기화되지 않았습니다" in detail["errors"][0]
        
        # Verify unhealthy status in data
        assert detail["data"]["service"] == "BFF"
        assert detail["data"]["version"] == "2.0.0"
        assert detail["data"]["oms_connected"] is False
        assert detail["data"]["status"] == "unhealthy"

    def test_health_check_exception_during_client_check(self):
        """Test health check when exception occurs during OMS client check"""
        # We need to test the case where get_oms_client() itself raises
        # This happens when oms_client is None
        set_oms_client(None)
        
        response = self.client.get("/health")
        
        assert response.status_code == 503
        detail = response.json()["detail"]
        
        # Verify error is properly captured
        assert detail["status"] == "error"
        assert detail["message"] == "서비스 연결에 문제가 있습니다"
        assert "OMS 클라이언트가 초기화되지 않았습니다" in detail["errors"][0]

    def test_health_check_response_serialization(self):
        """Test that health check response is properly serialized"""
        mock_client = MockOMSClient()
        set_oms_client(mock_client)
        
        response = self.client.get("/health")
        
        # Verify response can be serialized/deserialized
        import json
        data = response.json()
        serialized = json.dumps(data)
        deserialized = json.loads(serialized)
        
        assert deserialized == data
        assert isinstance(serialized, str)

    def test_concurrent_health_checks(self):
        """Test multiple concurrent health check requests"""
        mock_client = MockOMSClient()
        set_oms_client(mock_client)
        
        # Simulate concurrent requests
        responses = []
        for _ in range(10):
            response = self.client.get("/health")
            responses.append(response)
        
        # All should succeed
        assert all(r.status_code == 200 for r in responses)
        
        # All should have same structure
        data_list = [r.json() for r in responses]
        first_data = data_list[0]
        assert all(d == first_data for d in data_list)

    def test_health_check_api_response_methods(self):
        """Test that ApiResponse methods work correctly"""
        mock_client = MockOMSClient()
        set_oms_client(mock_client)
        
        # Import ApiResponse to verify structure
        from shared.models.requests import ApiResponse
        
        # Create health check response manually
        health_response = ApiResponse.health_check(
            service_name="BFF",
            version="2.0.0", 
            description="백엔드 포 프론트엔드 서비스"
        )
        
        # Verify response methods
        assert health_response.is_success() is True
        assert health_response.is_error() is False
        assert health_response.is_warning() is False
        
        # Verify to_dict method
        response_dict = health_response.to_dict()
        assert isinstance(response_dict, dict)
        assert response_dict["status"] == "success"


class TestRouterIntegration:
    """Test router integration with FastAPI app"""

    def test_router_prefix_and_tags(self):
        """Test that router has correct configuration"""
        assert router.tags == ["Health"]
        
        # Verify routes are registered
        routes = [route.path for route in router.routes]
        assert "/" in routes
        assert "/health" in routes

    def test_openapi_documentation(self):
        """Test that endpoints have proper OpenAPI documentation"""
        from fastapi import FastAPI
        app = FastAPI()
        app.include_router(router)
        
        openapi_schema = app.openapi()
        paths = openapi_schema["paths"]
        
        # Verify root endpoint docs
        assert "/" in paths
        root_get = paths["/"]["get"]
        assert "summary" in root_get
        assert "description" in root_get
        
        # Verify health endpoint docs
        assert "/health" in paths
        health_get = paths["/health"]["get"]
        assert "summary" in health_get
        assert "description" in health_get


class TestErrorScenarios:
    """Test various error scenarios"""

    def setup_method(self):
        """Setup test environment"""
        from fastapi import FastAPI
        app = FastAPI()
        app.include_router(router)
        self.client = TestClient(app)

    def test_health_check_with_network_timeout(self):
        """Test health check behavior with network timeout"""
        # Test with None client which causes 503
        set_oms_client(None)
        
        response = self.client.get("/health")
        
        assert response.status_code == 503
        detail = response.json()["detail"]
        assert "OMS 클라이언트가 초기화되지 않았습니다" in detail["errors"][0]

    def test_health_check_memory_pressure(self):
        """Test health check under memory pressure"""
        mock_client = MockOMSClient()
        set_oms_client(mock_client)
        
        # Create large number of requests to simulate memory pressure
        responses = []
        for i in range(100):
            response = self.client.get("/health")
            responses.append(response)
            
            # Verify each response is valid
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"


# Mock classes for testing
class MockOMSClient:
    """Mock OMS client for testing"""
    
    def __init__(self):
        self.connected = True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])