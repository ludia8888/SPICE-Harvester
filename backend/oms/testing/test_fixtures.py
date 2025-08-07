"""
OMS Test Fixtures

Test utilities specifically for OMS service testing with the modernized
dependency injection system.

Key features:
1. ✅ OMS-specific mock services (AsyncTerminusService, OutboxService)
2. ✅ Mock OMS service container
3. ✅ Test utilities for PostgreSQL and TerminusDB operations
4. ✅ Integration test helpers
"""

import pytest
from typing import AsyncGenerator, Dict, Any, List, Optional
from unittest.mock import Mock, AsyncMock
from contextlib import asynccontextmanager

from shared.testing.config_fixtures import (
    TestApplicationSettings, MockServiceContainer, 
    isolated_test_environment, create_mock_label_mapper,
    create_mock_jsonld_converter, create_mock_redis_service,
    create_mock_elasticsearch_service
)

# OMS specific imports
from oms.services.async_terminus import AsyncTerminusService
from oms.database.outbox import OutboxService
from oms.dependencies import OMSDependencyProvider
from shared.models.config import ConnectionConfig
from shared.services.command_status_service import CommandStatusService


def create_mock_async_terminus_service() -> Mock:
    """Create mock AsyncTerminusService for testing"""
    mock_terminus = AsyncMock(spec=AsyncTerminusService)
    
    # Mock connection operations
    mock_terminus.connect = AsyncMock(return_value=True)
    mock_terminus.disconnect = AsyncMock()
    mock_terminus.check_connection = AsyncMock(return_value=True)
    
    # Mock database operations
    mock_terminus.list_databases = AsyncMock(return_value=[
        {"name": "test_db1", "description": "Test Database 1"},
        {"name": "test_db2", "description": "Test Database 2"}
    ])
    mock_terminus.create_database = AsyncMock(return_value={
        "status": "success",
        "message": "Database created successfully",
        "database": {"name": "new_db", "description": "New Database"}
    })
    mock_terminus.delete_database = AsyncMock(return_value={
        "status": "success",
        "message": "Database deleted successfully"
    })
    mock_terminus.database_exists = AsyncMock(return_value=True)
    
    # Mock ontology operations
    mock_terminus.list_ontologies = AsyncMock(return_value={
        "status": "success",
        "data": {
            "ontologies": [
                {
                    "id": "onto1",
                    "label": "Test Ontology 1",
                    "description": "First test ontology",
                    "created_at": "2024-01-01T00:00:00Z"
                },
                {
                    "id": "onto2", 
                    "label": "Test Ontology 2",
                    "description": "Second test ontology",
                    "created_at": "2024-01-02T00:00:00Z"
                }
            ]
        }
    })
    mock_terminus.create_ontology = AsyncMock(return_value={
        "status": "success",
        "message": "Ontology created successfully",
        "data": {
            "id": "new_ontology",
            "label": "New Ontology",
            "description": "Newly created ontology"
        }
    })
    mock_terminus.get_ontology = AsyncMock(return_value={
        "status": "success",
        "data": {
            "id": "test_ontology",
            "label": "Test Ontology",
            "description": "Test ontology description",
            "properties": [],
            "relationships": [],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z"
        }
    })
    mock_terminus.update_ontology = AsyncMock(return_value={
        "status": "success",
        "message": "Ontology updated successfully",
        "data": {"id": "updated_ontology"}
    })
    mock_terminus.delete_ontology = AsyncMock(return_value={
        "status": "success",
        "message": "Ontology deleted successfully"
    })
    
    # Mock query operations
    mock_terminus.query_ontologies = AsyncMock(return_value={
        "status": "success",
        "data": {
            "results": [],
            "total": 0
        }
    })
    mock_terminus.execute_woql_query = AsyncMock(return_value={
        "status": "success",
        "data": {"bindings": []}
    })
    
    # Mock branch operations
    mock_terminus.create_branch = AsyncMock(return_value={
        "status": "success",
        "message": "Branch created successfully",
        "branch": {"name": "new_branch", "from": "main"}
    })
    mock_terminus.list_branches = AsyncMock(return_value={
        "status": "success",
        "branches": ["main", "dev", "feature"]
    })
    mock_terminus.checkout_branch = AsyncMock(return_value={
        "status": "success",
        "message": "Checked out to branch"
    })
    mock_terminus.merge_branches = AsyncMock(return_value={
        "status": "success",
        "message": "Branches merged successfully"
    })
    
    # Mock version operations
    mock_terminus.commit = AsyncMock(return_value={
        "status": "success",
        "message": "Changes committed",
        "commit_id": "abc123"
    })
    mock_terminus.get_commit_history = AsyncMock(return_value={
        "status": "success",
        "commits": [
            {
                "id": "abc123",
                "message": "Test commit",
                "author": "test_user",
                "timestamp": "2024-01-01T00:00:00Z"
            }
        ]
    })
    
    return mock_terminus


def create_mock_outbox_service() -> Mock:
    """Create mock OutboxService for testing"""
    mock_outbox = AsyncMock(spec=OutboxService)
    
    # Mock event publishing
    mock_outbox.publish_event = AsyncMock(return_value=True)
    mock_outbox.publish_ontology_created = AsyncMock(return_value=True)
    mock_outbox.publish_ontology_updated = AsyncMock(return_value=True)
    mock_outbox.publish_ontology_deleted = AsyncMock(return_value=True)
    
    # Mock event retrieval
    mock_outbox.get_pending_events = AsyncMock(return_value=[])
    mock_outbox.mark_event_processed = AsyncMock(return_value=True)
    mock_outbox.get_event_by_id = AsyncMock(return_value=None)
    
    # Mock health check
    mock_outbox.health_check = AsyncMock(return_value=True)
    
    return mock_outbox


def create_mock_postgres_db() -> Mock:
    """Create mock PostgreSQL database connection for testing"""
    mock_db = AsyncMock()
    mock_db.connect = AsyncMock()
    mock_db.disconnect = AsyncMock()
    mock_db.execute = AsyncMock(return_value=None)
    mock_db.fetch = AsyncMock(return_value=[])
    mock_db.fetchrow = AsyncMock(return_value=None)
    return mock_db


class MockOMSServiceContainer:
    """
    Mock OMS service container for testing
    
    This provides isolated OMS services for testing without requiring
    actual service connections.
    """
    
    def __init__(self, container: MockServiceContainer, settings: TestApplicationSettings):
        self.container = container
        self.settings = settings
        self._oms_services = {}
        self._postgres_db = None
    
    async def initialize_oms_services(self) -> None:
        """Initialize mock OMS services"""
        self._oms_services['terminus_service'] = create_mock_async_terminus_service()
        self._oms_services['jsonld_converter'] = create_mock_jsonld_converter()
        self._oms_services['label_mapper'] = create_mock_label_mapper()
        self._oms_services['outbox_service'] = create_mock_outbox_service()
        self._oms_services['redis_service'] = create_mock_redis_service()
        self._oms_services['command_status_service'] = AsyncMock(spec=CommandStatusService)
        self._oms_services['elasticsearch_service'] = create_mock_elasticsearch_service()
        self._postgres_db = create_mock_postgres_db()
    
    async def shutdown_oms_services(self) -> None:
        """Shutdown mock OMS services"""
        for service in self._oms_services.values():
            if hasattr(service, 'close'):
                await service.close()
            elif hasattr(service, 'disconnect'):
                await service.disconnect()
        self._oms_services.clear()
    
    def get_terminus_service(self) -> Mock:
        """Get mock TerminusDB service"""
        if 'terminus_service' not in self._oms_services:
            raise RuntimeError("TerminusDB service not initialized")
        return self._oms_services['terminus_service']
    
    def get_jsonld_converter(self) -> Mock:
        """Get mock JSON-LD converter"""
        if 'jsonld_converter' not in self._oms_services:
            raise RuntimeError("JSON-LD converter not initialized")
        return self._oms_services['jsonld_converter']
    
    def get_label_mapper(self) -> Mock:
        """Get mock label mapper"""
        if 'label_mapper' not in self._oms_services:
            raise RuntimeError("Label mapper not initialized")
        return self._oms_services['label_mapper']
    
    def get_outbox_service(self) -> Optional[Mock]:
        """Get mock outbox service"""
        return self._oms_services.get('outbox_service')
    
    def get_redis_service(self) -> Optional[Mock]:
        """Get mock Redis service"""
        return self._oms_services.get('redis_service')
    
    def get_command_status_service(self) -> Optional[Mock]:
        """Get mock command status service"""
        return self._oms_services.get('command_status_service')
    
    def get_elasticsearch_service(self) -> Optional[Mock]:
        """Get mock Elasticsearch service"""
        return self._oms_services.get('elasticsearch_service')


@pytest.fixture
async def mock_oms_container(test_settings: TestApplicationSettings) -> AsyncGenerator[MockOMSServiceContainer, None]:
    """Pytest fixture for mock OMS service container"""
    base_container = MockServiceContainer(test_settings)
    oms_container = MockOMSServiceContainer(base_container, test_settings)
    
    await oms_container.initialize_oms_services()
    
    yield oms_container
    
    await oms_container.shutdown_oms_services()


@pytest.fixture
def mock_async_terminus_service():
    """Pytest fixture for mock AsyncTerminusService"""
    return create_mock_async_terminus_service()


@pytest.fixture
def mock_outbox_service():
    """Pytest fixture for mock OutboxService"""
    return create_mock_outbox_service()


@pytest.fixture
def mock_postgres_db():
    """Pytest fixture for mock PostgreSQL database"""
    return create_mock_postgres_db()


@asynccontextmanager
async def isolated_oms_test_environment(**config_overrides):
    """
    Async context manager for isolated OMS test environment
    
    This provides a complete OMS testing environment with all services mocked.
    """
    async with isolated_test_environment(**config_overrides) as (container, settings):
        # Create OMS-specific container
        oms_container = MockOMSServiceContainer(container, settings)
        await oms_container.initialize_oms_services()
        
        try:
            yield oms_container, settings
        finally:
            await oms_container.shutdown_oms_services()


class OMSTestClient:
    """
    Test client for OMS service
    
    This provides utilities for testing OMS endpoints with mocked dependencies.
    """
    
    def __init__(self, oms_container: MockOMSServiceContainer, settings: TestApplicationSettings):
        self.container = oms_container
        self.settings = settings
    
    async def test_database_operations(self, db_name: str) -> Dict[str, Any]:
        """Test database operations"""
        terminus_service = self.container.get_terminus_service()
        
        # Test database listing
        databases = await terminus_service.list_databases()
        
        # Test database creation
        create_result = await terminus_service.create_database(db_name, "Test database")
        
        # Test database existence check
        exists = await terminus_service.database_exists(db_name)
        
        return {
            "databases": databases,
            "create_result": create_result,
            "exists": exists
        }
    
    async def test_ontology_operations(self, db_name: str, ontology_data: Dict[str, Any]) -> Dict[str, Any]:
        """Test ontology operations"""
        terminus_service = self.container.get_terminus_service()
        outbox_service = self.container.get_outbox_service()
        
        # Test ontology creation
        create_result = await terminus_service.create_ontology(db_name, ontology_data)
        
        # Test event publishing
        if outbox_service:
            await outbox_service.publish_ontology_created(
                db_name, 
                create_result.get("data", {}).get("id", "test_id"),
                ontology_data
            )
        
        # Test ontology retrieval
        ontology_id = create_result.get("data", {}).get("id", "test_id")
        get_result = await terminus_service.get_ontology(db_name, ontology_id)
        
        # Test ontology listing
        list_result = await terminus_service.list_ontologies(db_name)
        
        return {
            "create_result": create_result,
            "get_result": get_result,
            "list_result": list_result
        }
    
    async def test_branch_operations(self, db_name: str, branch_name: str) -> Dict[str, Any]:
        """Test branch operations"""
        terminus_service = self.container.get_terminus_service()
        
        # Test branch creation
        create_result = await terminus_service.create_branch(db_name, branch_name, "main")
        
        # Test branch listing
        list_result = await terminus_service.list_branches(db_name)
        
        # Test checkout
        checkout_result = await terminus_service.checkout_branch(db_name, branch_name)
        
        return {
            "create_result": create_result,
            "list_result": list_result,
            "checkout_result": checkout_result
        }
    
    async def test_version_operations(self, db_name: str) -> Dict[str, Any]:
        """Test version control operations"""
        terminus_service = self.container.get_terminus_service()
        
        # Test commit
        commit_result = await terminus_service.commit(
            db_name, 
            "Test commit", 
            "test_user"
        )
        
        # Test commit history
        history_result = await terminus_service.get_commit_history(db_name)
        
        return {
            "commit_result": commit_result,
            "history_result": history_result
        }
    
    async def test_query_operations(self, db_name: str, query: str) -> Dict[str, Any]:
        """Test query operations"""
        terminus_service = self.container.get_terminus_service()
        
        # Test ontology query
        query_result = await terminus_service.query_ontologies(db_name, query)
        
        # Test WOQL query
        woql_result = await terminus_service.execute_woql_query(db_name, query)
        
        return {
            "query_result": query_result,
            "woql_result": woql_result
        }


@pytest.fixture
async def oms_test_client(mock_oms_container, test_settings) -> OMSTestClient:
    """Pytest fixture for OMS test client"""
    return OMSTestClient(mock_oms_container, test_settings)


# Utility functions for common test scenarios
def setup_test_ontology_data() -> Dict[str, Any]:
    """Setup common test data for ontology operations"""
    return {
        "id": "test_ontology",
        "label": "Test Ontology",
        "description": "Test ontology for OMS testing",
        "properties": [
            {
                "id": "name",
                "label": "Name",
                "type": "string",
                "required": True
            },
            {
                "id": "description", 
                "label": "Description",
                "type": "string",
                "required": False
            }
        ],
        "relationships": [],
        "abstract": False
    }


def setup_test_database_data() -> Dict[str, Any]:
    """Setup common test data for database operations"""
    return {
        "name": "test_database",
        "description": "Test database for OMS testing",
        "comment": "Created during testing"
    }


def setup_test_branch_data() -> Dict[str, Any]:
    """Setup common test data for branch operations"""
    return {
        "name": "test_branch",
        "from_branch": "main",
        "description": "Test branch for testing"
    }


def setup_test_commit_data() -> Dict[str, Any]:
    """Setup common test data for commit operations"""
    return {
        "message": "Test commit message",
        "author": "test_user",
        "description": "Test commit for testing"
    }


# Example usage documentation
"""
Example usage in OMS tests:

# Test with isolated OMS environment
@pytest.mark.asyncio
async def test_oms_ontology_operations():
    async with isolated_oms_test_environment() as (oms_container, settings):
        test_client = OMSTestClient(oms_container, settings)
        
        # Test ontology operations
        ontology_data = setup_test_ontology_data()
        result = await test_client.test_ontology_operations("test_db", ontology_data)
        assert result["create_result"]["status"] == "success"
        
        # Test database operations
        db_result = await test_client.test_database_operations("test_db")
        assert len(db_result["databases"]) >= 0

# Test with pytest fixtures
@pytest.mark.asyncio
async def test_with_oms_fixtures(oms_test_client, mock_async_terminus_service):
    # Configure mock behavior
    mock_async_terminus_service.create_ontology.return_value = {
        "status": "success", 
        "data": {"id": "new_onto", "label": "New Ontology"}
    }
    
    ontology_data = setup_test_ontology_data()
    result = await oms_test_client.test_ontology_operations("test_db", ontology_data)
    assert result["create_result"]["data"]["id"] == "new_onto"

# Test specific service interactions
@pytest.mark.asyncio
async def test_outbox_integration(mock_oms_container):
    terminus_service = mock_oms_container.get_terminus_service()
    outbox_service = mock_oms_container.get_outbox_service()
    
    # Test ontology creation with event publishing
    ontology_data = setup_test_ontology_data()
    create_result = await terminus_service.create_ontology("test_db", ontology_data)
    
    await outbox_service.publish_ontology_created(
        "test_db", 
        create_result["data"]["id"], 
        ontology_data
    )
    
    # Verify event was published
    outbox_service.publish_ontology_created.assert_called_once()
"""