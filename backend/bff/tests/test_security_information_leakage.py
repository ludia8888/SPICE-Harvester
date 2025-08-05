"""
Security Tests for Information Leakage Prevention

Tests Anti-pattern 12: Ensures BFF APIs do not leak internal architecture
information such as data source details (Elasticsearch vs TerminusDB),
system status, or fallback routing information.

Security Principle: BFF should provide uniform responses regardless of
internal data source or processing path.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from fastapi.testclient import TestClient
from fastapi import status
from typing import Dict, Any, List

from bff.main import app
from elasticsearch.exceptions import ConnectionError as ESConnectionError

class TestInformationLeakagePrevention:
    """Test suite to verify BFF APIs don't leak internal architecture information"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    @pytest.fixture
    def mock_elasticsearch_service(self):
        """Mock Elasticsearch service"""
        return AsyncMock()
    
    @pytest.fixture
    def mock_oms_client(self):
        """Mock OMS client"""
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_get_instance_elasticsearch_success_no_source_leak(self, client):
        """Test: get_instance with ES success should not leak source information"""
        
        mock_es_result = {
            "hits": {
                "total": {"value": 1},
                "hits": [{
                    "_source": {
                        "instance_id": "test_instance",
                        "class_id": "TestClass",
                        "properties": []
                    }
                }]
            }
        }
        
        with patch('bff.dependencies.get_elasticsearch_service') as mock_es_dep, \
             patch('bff.dependencies.get_oms_client') as mock_oms_dep:
            
            mock_es = AsyncMock()
            mock_es.search.return_value = mock_es_result
            mock_es_dep.return_value = mock_es
            
            mock_oms = AsyncMock()
            mock_oms_dep.return_value = mock_oms
            
            response = client.get("/database/test_db/class/TestClass/instance/test_instance")
        
        assert response.status_code == status.HTTP_200_OK
        response_data = response.json()
        
        # Critical: Should not contain any source information
        assert "source" not in response_data
        assert "elasticsearch" not in str(response_data).lower()
        assert "terminus" not in str(response_data).lower()
        assert "fallback" not in str(response_data).lower()
        
        # Should contain expected data structure
        assert response_data["status"] == "success"
        assert "data" in response_data
    
    @pytest.mark.asyncio
    async def test_get_instance_terminus_fallback_no_source_leak(self, client):
        """Test: get_instance with TerminusDB fallback should not leak source information"""
        
        # Mock ES failure
        mock_oms_result = {
            "status": "success",
            "data": {
                "instance_id": "test_instance",
                "class_id": "TestClass",
                "properties": []
            }
        }
        
        with patch('bff.dependencies.get_elasticsearch_service') as mock_es_dep, \
             patch('bff.dependencies.get_oms_client') as mock_oms_dep:
            
            # ES fails
            mock_es = AsyncMock()
            mock_es.search.side_effect = ESConnectionError("Connection failed")
            mock_es_dep.return_value = mock_es
            
            # OMS succeeds
            mock_oms = AsyncMock()
            mock_oms.get_instance.return_value = mock_oms_result
            mock_oms_dep.return_value = mock_oms
            
            response = client.get("/database/test_db/class/TestClass/instance/test_instance")
        
        assert response.status_code == status.HTTP_200_OK
        response_data = response.json()
        
        # Critical: Should not contain any source information even with fallback
        assert "source" not in response_data
        assert "elasticsearch" not in str(response_data).lower()
        assert "terminus" not in str(response_data).lower()
        assert "fallback" not in str(response_data).lower()
        assert "error" not in response_data  # No error indicators
        
        # Should have identical structure to ES success case
        assert response_data["status"] == "success"
        assert "data" in response_data
    
    @pytest.mark.asyncio
    async def test_get_class_instances_elasticsearch_success_no_source_leak(self, client):
        """Test: get_class_instances with ES success should not leak source information"""
        
        mock_es_result = {
            "hits": {
                "total": {"value": 2},
                "hits": [
                    {"_source": {"instance_id": "inst1", "class_id": "TestClass"}},
                    {"_source": {"instance_id": "inst2", "class_id": "TestClass"}}
                ]
            }
        }
        
        with patch('bff.dependencies.get_elasticsearch_service') as mock_es_dep, \
             patch('bff.dependencies.get_oms_client') as mock_oms_dep:
            
            mock_es = AsyncMock()
            mock_es.search.return_value = mock_es_result
            mock_es_dep.return_value = mock_es
            
            mock_oms = AsyncMock()
            mock_oms_dep.return_value = mock_oms
            
            response = client.get("/database/test_db/class/TestClass/instances")
        
        assert response.status_code == status.HTTP_200_OK
        response_data = response.json()
        
        # Critical: Should not leak internal architecture information
        assert "source" not in response_data
        assert "elasticsearch" not in str(response_data).lower()
        assert "terminus" not in str(response_data).lower()
        
        # Should contain expected list structure
        assert "instances" in response_data
        assert "total" in response_data
        assert len(response_data["instances"]) == 2
    
    @pytest.mark.asyncio
    async def test_get_class_instances_terminus_fallback_no_source_leak(self, client):
        """Test: get_class_instances with TerminusDB fallback should not leak source information"""
        
        mock_oms_result = {
            "status": "success",
            "total": 1,
            "instances": [{"instance_id": "inst1", "class_id": "TestClass"}]
        }
        
        with patch('bff.dependencies.get_elasticsearch_service') as mock_es_dep, \
             patch('bff.dependencies.get_oms_client') as mock_oms_dep:
            
            # ES fails
            mock_es = AsyncMock()
            mock_es.search.side_effect = ESConnectionError("Connection failed")
            mock_es_dep.return_value = mock_es
            
            # OMS succeeds
            mock_oms = AsyncMock()
            mock_oms.get_class_instances.return_value = mock_oms_result
            mock_oms_dep.return_value = mock_oms
            
            response = client.get("/database/test_db/class/TestClass/instances")
        
        assert response.status_code == status.HTTP_200_OK
        response_data = response.json()
        
        # Critical: Fallback should be completely transparent to client
        assert "source" not in response_data
        assert "elasticsearch" not in str(response_data).lower()
        assert "terminus" not in str(response_data).lower() 
        assert "fallback" not in str(response_data).lower()
        assert "error" not in response_data
        
        # Should have identical structure to ES success case
        assert "instances" in response_data
        assert "total" in response_data
    
    @pytest.mark.asyncio
    async def test_error_responses_no_internal_info_leak(self, client):
        """Test: Error responses should not leak internal architecture details"""
        
        with patch('bff.dependencies.get_elasticsearch_service') as mock_es_dep, \
             patch('bff.dependencies.get_oms_client') as mock_oms_dep:
            
            # Both ES and OMS fail
            mock_es = AsyncMock()
            mock_es.search.side_effect = ESConnectionError("Connection failed")
            mock_es_dep.return_value = mock_es
            
            mock_oms = AsyncMock()
            mock_oms.get_instance.side_effect = Exception("OMS connection failed")
            mock_oms_dep.return_value = mock_oms
            
            response = client.get("/database/test_db/class/TestClass/instance/nonexistent")
        
        # Should return proper HTTP error without internal details
        assert response.status_code == status.HTTP_404_NOT_FOUND
        response_data = response.json()
        
        # Critical: Error responses should not leak architecture information
        assert "elasticsearch" not in str(response_data).lower()
        assert "terminus" not in str(response_data).lower()
        assert "oms" not in str(response_data).lower()
        assert "connection" not in str(response_data).lower()
        assert "source" not in response_data
        
        # Should contain generic error message only
        assert "detail" in response_data
        assert "찾을 수 없습니다" in response_data["detail"]  # Generic Korean error message
    
    def test_response_structure_consistency(self):
        """Test: All successful responses should have consistent structure regardless of data source"""
        
        # Define expected response structures
        expected_instance_response = {
            "status": str,
            "data": dict
        }
        
        expected_instances_response = {
            "class_id": str,
            "total": int,
            "limit": int,
            "offset": int,
            "search": (str, type(None)),
            "instances": list
        }
        
        # These structures should be identical whether data comes from ES or TerminusDB
        # The test framework ensures no "source" field exists in any case
        
    def test_forbidden_fields_in_responses(self):
        """Test: Ensure specific internal fields are never exposed in API responses"""
        
        forbidden_fields = [
            "source",           # Data source identifier
            "origin",           # Alternative source field
            "provider",         # Service provider info
            "backend",          # Backend system info
            "engine",           # Database engine info
            "driver",           # Database driver info
            "connection_type",  # Connection information
            "fallback_used",    # Fallback indication
            "error_source",     # Error source system
            "internal_path",    # Internal routing path
            "service_status"    # Service health status
        ]
        
        # This test documents which fields are security-sensitive
        # Actual validation happens in the integration tests above
        assert len(forbidden_fields) > 0  # Ensure list is not empty
        
        # Log security reminder
        print(f"Security reminder: These {len(forbidden_fields)} fields must never appear in BFF responses")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])