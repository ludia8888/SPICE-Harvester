"""
BFF Test Fixtures

Test utilities specifically for BFF service testing with the modernized
dependency injection system.

Key features:
1. ✅ BFF-specific mock services (OMSClient, TerminusService)
2. ✅ Mock BFF service container
3. ✅ Test utilities for WebSocket and type inference
4. ✅ Integration test helpers
"""

import pytest
from typing import AsyncGenerator, Dict, Any
from unittest.mock import Mock, AsyncMock
from contextlib import asynccontextmanager

from shared.testing.config_fixtures import (
    TestApplicationSettings, MockServiceContainer, 
    isolated_test_environment, create_mock_label_mapper,
    create_mock_jsonld_converter, create_mock_redis_service,
    create_mock_elasticsearch_service
)

# BFF specific imports
from bff.services.oms_client import OMSClient
from bff.dependencies import BFFDependencyProvider
# TerminusService is not in shared, but AsyncTerminusService is in OMS
# For testing, we'll mock it directly
from bff.services.funnel_type_inference_adapter import FunnelHTTPTypeInferenceAdapter
# BFFServiceContainer is now integrated into the dependency system


def create_mock_oms_client() -> Mock:
    """Create mock OMS client for testing"""
    mock_oms = AsyncMock(spec=OMSClient)
    
    # Mock common OMS operations
    mock_oms.check_health = AsyncMock(return_value=True)
    mock_oms.list_databases = AsyncMock(return_value={
        "status": "success",
        "data": {
            "databases": [
                {"name": "test_db1"},
                {"name": "test_db2"}
            ]
        }
    })
    mock_oms.create_database = AsyncMock(return_value={
        "status": "success", 
        "data": {"database": {"name": "new_db"}}
    })
    mock_oms.list_ontologies = AsyncMock(return_value={
        "status": "success",
        "data": {
            "ontologies": [
                {"id": "class1", "label": "Test Class 1"},
                {"id": "class2", "label": "Test Class 2"}
            ]
        }
    })
    mock_oms.get_ontology = AsyncMock(return_value={
        "status": "success",
        "data": {
            "id": "test_class",
            "label": "Test Class",
            "description": "Test description",
            "properties": []
        }
    })
    mock_oms.create_ontology = AsyncMock(return_value={
        "status": "success",
        "data": {"id": "new_ontology", "label": "New Ontology"}
    })
    mock_oms.update_ontology = AsyncMock(return_value={
        "status": "success",
        "data": {"id": "updated_ontology"}
    })
    mock_oms.delete_ontology = AsyncMock(return_value={
        "status": "success",
        "message": "Ontology deleted"
    })
    mock_oms.query_ontologies = AsyncMock(return_value={
        "status": "success",
        "data": {"results": []}
    })
    
    # Mock version control operations
    mock_oms.create_branch = AsyncMock(return_value={
        "status": "success",
        "data": {"branch": {"name": "new_branch"}}
    })
    mock_oms.get_version_history = AsyncMock(return_value={
        "status": "success",
        "data": {"commits": []}
    })
    
    # Mock close method
    mock_oms.close = AsyncMock()
    
    return mock_oms


def create_mock_terminus_service() -> Mock:
    """Create mock TerminusService for testing"""
    # Creating AsyncMock without spec since TerminusService is not available in BFF
    mock_terminus = AsyncMock()
    
    # Mock all TerminusService methods
    mock_terminus.list_databases = AsyncMock(return_value=["test_db1", "test_db2"])
    mock_terminus.create_database = AsyncMock(return_value={"status": "success"})
    mock_terminus.delete_database = AsyncMock(return_value={"status": "success"})
    mock_terminus.get_database_info = AsyncMock(return_value={"status": "success"})
    mock_terminus.list_classes = AsyncMock(return_value=[
        {"id": "class1", "label": "Test Class 1"},
        {"id": "class2", "label": "Test Class 2"}
    ])
    mock_terminus.create_class = AsyncMock(return_value={
        "id": "new_class", "label": "New Class"
    })
    mock_terminus.get_class = AsyncMock(return_value={
        "id": "test_class", "label": "Test Class"
    })
    mock_terminus.update_class = AsyncMock(return_value={"status": "success"})
    mock_terminus.delete_class = AsyncMock(return_value={"status": "success"})
    mock_terminus.query_database = AsyncMock(return_value={"results": []})
    
    # Mock branch operations
    mock_terminus.create_branch = AsyncMock(return_value={"status": "success"})
    mock_terminus.delete_branch = AsyncMock(return_value={"status": "success"})
    mock_terminus.checkout = AsyncMock(return_value={"status": "success"})
    mock_terminus.commit_changes = AsyncMock(return_value={"status": "success"})
    mock_terminus.get_commit_history = AsyncMock(return_value={"commits": []})
    mock_terminus.get_diff = AsyncMock(return_value={"changes": []})
    mock_terminus.merge_branches = AsyncMock(return_value={"status": "success"})
    mock_terminus.rollback = AsyncMock(return_value={"status": "success"})
    mock_terminus.get_branch_info = AsyncMock(return_value={"status": "success"})
    
    # Mock merge conflict operations
    mock_terminus.simulate_merge = AsyncMock(return_value={
        "data": {"merge_preview": {"conflicts": []}}
    })
    mock_terminus.resolve_merge_conflicts = AsyncMock(return_value={"status": "success"})
    mock_terminus.create_ontology_with_advanced_relationships = AsyncMock(return_value={
        "status": "success"
    })
    
    return mock_terminus


def create_mock_type_inference_adapter() -> Mock:
    """Create mock type inference adapter for testing"""
    mock_adapter = AsyncMock(spec=FunnelHTTPTypeInferenceAdapter)
    mock_adapter.infer_types = AsyncMock(return_value={
        "inferred_types": ["string", "number"],
        "confidence": 0.95
    })
    mock_adapter.close = AsyncMock()
    return mock_adapter


class MockBFFServiceContainer:
    """
    Mock BFF service container for testing
    
    This provides isolated BFF services for testing without requiring
    actual service connections.
    """
    
    def __init__(self, container: MockServiceContainer, settings: TestApplicationSettings):
        self.container = container
        self.settings = settings
        self._bff_services = {}
    
    async def initialize_bff_services(self) -> None:
        """Initialize mock BFF services"""
        self._bff_services['oms_client'] = create_mock_oms_client()
        self._bff_services['label_mapper'] = create_mock_label_mapper()
        self._bff_services['type_inference_adapter'] = create_mock_type_inference_adapter()
        self._bff_services['websocket_service'] = AsyncMock()
        self._bff_services['redis_service'] = create_mock_redis_service()
    
    async def shutdown_bff_services(self) -> None:
        """Shutdown mock BFF services"""
        for service in self._bff_services.values():
            if hasattr(service, 'close'):
                await service.close()
        self._bff_services.clear()
    
    def get_oms_client(self) -> Mock:
        """Get mock OMS client"""
        if 'oms_client' not in self._bff_services:
            raise RuntimeError("OMS client not initialized")
        return self._bff_services['oms_client']
    
    def get_label_mapper(self) -> Mock:
        """Get mock label mapper"""
        if 'label_mapper' not in self._bff_services:
            raise RuntimeError("Label mapper not initialized")
        return self._bff_services['label_mapper']


@pytest.fixture
async def mock_bff_container(test_settings: TestApplicationSettings) -> AsyncGenerator[MockBFFServiceContainer, None]:
    """Pytest fixture for mock BFF service container"""
    base_container = MockServiceContainer(test_settings)
    bff_container = MockBFFServiceContainer(base_container, test_settings)
    
    await bff_container.initialize_bff_services()
    
    yield bff_container
    
    await bff_container.shutdown_bff_services()


@pytest.fixture
def mock_oms_client():
    """Pytest fixture for mock OMS client"""
    return create_mock_oms_client()


@pytest.fixture  
def mock_terminus_service():
    """Pytest fixture for mock TerminusService"""
    return create_mock_terminus_service()


@pytest.fixture
def mock_type_inference_adapter():
    """Pytest fixture for mock type inference adapter"""
    return create_mock_type_inference_adapter()


@asynccontextmanager
async def isolated_bff_test_environment(**config_overrides):
    """
    Async context manager for isolated BFF test environment
    
    This provides a complete BFF testing environment with all services mocked.
    """
    async with isolated_test_environment(**config_overrides) as (container, settings):
        # Create BFF-specific container
        bff_container = MockBFFServiceContainer(container, settings)
        await bff_container.initialize_bff_services()
        
        try:
            yield bff_container, settings
        finally:
            await bff_container.shutdown_bff_services()


class BFFTestClient:
    """
    Test client for BFF service
    
    This provides utilities for testing BFF endpoints with mocked dependencies.
    """
    
    def __init__(self, bff_container: MockBFFServiceContainer, settings: TestApplicationSettings):
        self.container = bff_container
        self.settings = settings
    
    async def test_ontology_retrieval(self, db_name: str, class_label: str) -> Dict[str, Any]:
        """Test ontology retrieval operation"""
        oms_client = self.container.get_oms_client()
        label_mapper = self.container.get_label_mapper()
        
        # Simulate the operation
        class_id = await label_mapper.get_class_id(db_name, class_label, "ko")
        ontology = await oms_client.get_ontology(db_name, class_id)
        
        return ontology
    
    async def test_ontology_creation(self, db_name: str, ontology_data: Dict[str, Any]) -> Dict[str, Any]:
        """Test ontology creation operation"""
        oms_client = self.container.get_oms_client()
        result = await oms_client.create_ontology(db_name, ontology_data)
        return result
    
    async def test_database_operations(self, db_name: str) -> Dict[str, Any]:
        """Test database operations"""
        oms_client = self.container.get_oms_client()
        
        # Test database listing
        databases = await oms_client.list_databases()
        
        # Test database creation
        create_result = await oms_client.create_database(db_name)
        
        return {
            "databases": databases,
            "create_result": create_result
        }


@pytest.fixture
async def bff_test_client(mock_bff_container, test_settings) -> BFFTestClient:
    """Pytest fixture for BFF test client"""
    return BFFTestClient(mock_bff_container, test_settings)


# Utility functions for common test scenarios
def setup_ontology_test_data() -> Dict[str, Any]:
    """Setup common test data for ontology operations"""
    return {
        "id": "test_ontology",
        "label": {"ko": "테스트 온톨로지", "en": "Test Ontology"},
        "description": {"ko": "테스트용 온톨로지", "en": "Test ontology"},
        "properties": [
            {
                "id": "name",
                "label": {"ko": "이름", "en": "Name"},
                "type": "string",
                "required": True
            }
        ],
        "relationships": []
    }


def setup_database_test_data() -> Dict[str, Any]:
    """Setup common test data for database operations"""
    return {
        "name": "test_database",
        "description": "Test database for BFF testing"
    }


# Example usage documentation
"""
Example usage in BFF tests:

# Test with isolated BFF environment
@pytest.mark.asyncio
async def test_bff_ontology_operations():
    async with isolated_bff_test_environment() as (bff_container, settings):
        test_client = BFFTestClient(bff_container, settings)
        
        # Test ontology retrieval
        ontology = await test_client.test_ontology_retrieval("test_db", "test_class")
        assert ontology["status"] == "success"
        
        # Test ontology creation
        ontology_data = setup_ontology_test_data()
        result = await test_client.test_ontology_creation("test_db", ontology_data)
        assert result["status"] == "success"

# Test with pytest fixtures
@pytest.mark.asyncio
async def test_with_bff_fixtures(bff_test_client, mock_oms_client):
    # Configure mock behavior
    mock_oms_client.get_ontology.return_value = {
        "status": "success", 
        "data": {"id": "test", "label": "Test"}
    }
    
    result = await bff_test_client.test_ontology_retrieval("test_db", "test_class")
    assert result["data"]["id"] == "test"
"""