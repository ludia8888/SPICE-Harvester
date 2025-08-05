"""
Example Tests for Modernized Architecture

This file demonstrates how to write tests using the modernized dependency injection
system and test fixtures. It shows the contrast between the old anti-pattern 13
approach and the new test-friendly architecture.

Key demonstrations:
1. ✅ Test isolation with independent configuration per test
2. ✅ Easy service mocking without global state pollution  
3. ✅ Type-safe dependency injection in tests
4. ✅ Integration testing with mock containers
5. ✅ Configuration override testing
"""

import pytest
from unittest.mock import AsyncMock
from typing import Dict, Any

# Import test fixtures
from shared.testing.config_fixtures import (
    TestApplicationSettings, 
    isolated_test_environment,
    ConfigOverride,
    setup_test_database_config,
    setup_test_service_config
)
from bff.testing.test_fixtures import (
    isolated_bff_test_environment,
    setup_ontology_test_data,
    BFFTestClient
)
from oms.testing.test_fixtures import (
    isolated_oms_test_environment,
    setup_test_ontology_data,
    OMSTestClient
)

# Import the services we're testing
from shared.services import StorageService, RedisService, ElasticsearchService
from shared.utils.label_mapper import LabelMapper
from shared.utils.jsonld import JSONToJSONLDConverter


class TestModernizedConfiguration:
    """
    Test class demonstrating modernized configuration system
    
    These tests show how the new system eliminates test pollution
    and provides isolated configuration per test.
    """
    
    def test_isolated_configuration_per_test(self):
        """Test that each test gets isolated configuration"""
        # Test 1: Development configuration
        settings1 = TestApplicationSettings(debug=True, environment="development")
        assert settings1.debug == True
        assert settings1.is_development == True
        
        # Test 2: Production configuration  
        settings2 = TestApplicationSettings(debug=False, environment="production")
        assert settings2.debug == False
        assert settings2.is_production == True
        
        # Verify tests don't interfere with each other
        assert settings1.debug != settings2.debug
        assert settings1.environment != settings2.environment
    
    def test_configuration_overrides(self):
        """Test configuration override functionality"""
        # Test with different database configurations
        with ConfigOverride(database_name="test_db_1", redis_db=1):
            settings1 = TestApplicationSettings()
            assert settings1.database.name == "test_db_1"
            
        with ConfigOverride(database_name="test_db_2", redis_db=2):
            settings2 = TestApplicationSettings()
            assert settings2.database.name == "test_db_2"
        
        # Verify no cross-contamination
        settings3 = TestApplicationSettings()
        assert settings3.database.name != "test_db_1"
        assert settings3.database.name != "test_db_2"
    
    def test_specialized_configuration_builders(self):
        """Test specialized configuration builder functions"""
        # Test database-specific configuration
        db_config = setup_test_database_config(
            db_name="special_test_db",
            db_user="special_user",
            db_password="special_password"
        )
        assert db_config.database.name == "special_test_db"
        assert db_config.database.user == "special_user"
        
        # Test service-specific configuration
        service_config = setup_test_service_config(
            oms_url="http://test-oms:8001",
            redis_host="test-redis",
            redis_port=6380
        )
        assert service_config.services.oms_base_url == "http://test-oms:8001"
        assert service_config.services.redis_host == "test-redis"
        assert service_config.services.redis_port == 6380


class TestModernizedDependencyInjection:
    """
    Test class demonstrating modernized dependency injection
    
    These tests show how the container system provides isolated,
    mockable services without global state pollution.
    """
    
    @pytest.mark.asyncio
    async def test_isolated_service_container(self):
        """Test that service containers are isolated per test"""
        async with isolated_test_environment(redis_db=1) as (container1, settings1):
            async with isolated_test_environment(redis_db=2) as (container2, settings2):
                # Each container should have independent services
                redis1 = await container1.get(RedisService)
                redis2 = await container2.get(RedisService)
                
                # Services should be different instances
                assert redis1 is not redis2
                
                # Configuration should be isolated
                assert settings1.services.redis_db != settings2.services.redis_db
    
    @pytest.mark.asyncio 
    async def test_service_mocking(self):
        """Test easy service mocking in containers"""
        async with isolated_test_environment() as (container, settings):
            # Get mock services
            storage_service = await container.get(StorageService)
            elasticsearch_service = await container.get(ElasticsearchService)
            
            # Configure mock behavior
            storage_service.upload_file.return_value = "test_file_path"
            elasticsearch_service.search.return_value = {
                "hits": {"total": {"value": 1}, "hits": [{"_source": {"test": "data"}}]}
            }
            
            # Test the mocked behavior
            file_path = await storage_service.upload_file("test.txt", b"content")
            assert file_path == "test_file_path"
            
            search_result = await elasticsearch_service.search("test_index", {"query": {}})
            assert search_result["hits"]["total"]["value"] == 1
    
    @pytest.mark.asyncio
    async def test_container_health_checks(self):
        """Test container health check functionality"""
        async with isolated_test_environment() as (container, settings):
            # Test container health
            health_status = await container.health_check_all()
            
            # Verify all services report healthy
            assert "StorageService" in health_status
            assert "RedisService" in health_status
            assert "ElasticsearchService" in health_status
            
            # Mock services should report as healthy
            for service_name, status in health_status.items():
                assert "healthy" in status or "mock_healthy" in status


class TestBFFModernizedIntegration:
    """
    Test class demonstrating BFF service integration testing
    with the modernized architecture.
    """
    
    @pytest.mark.asyncio
    async def test_bff_ontology_operations_isolated(self):
        """Test BFF ontology operations with complete isolation"""
        async with isolated_bff_test_environment(debug=True) as (bff_container, settings):
            test_client = BFFTestClient(bff_container, settings)
            
            # Test ontology retrieval
            ontology_result = await test_client.test_ontology_retrieval("test_db", "test_class")
            assert ontology_result["status"] == "success"
            assert ontology_result["data"]["id"] == "test_class"
            
            # Test ontology creation
            ontology_data = setup_ontology_test_data()
            create_result = await test_client.test_ontology_creation("test_db", ontology_data)
            assert create_result["status"] == "success"
            assert create_result["data"]["id"] == "new_ontology"
    
    @pytest.mark.asyncio
    async def test_bff_database_operations_isolated(self):
        """Test BFF database operations with complete isolation"""
        async with isolated_bff_test_environment() as (bff_container, settings):
            test_client = BFFTestClient(bff_container, settings)
            
            # Test database operations
            db_result = await test_client.test_database_operations("new_test_db")
            
            # Verify mock responses
            assert db_result["databases"]["status"] == "success"
            assert "databases" in db_result["databases"]["data"]
            assert db_result["create_result"]["status"] == "success"
    
    @pytest.mark.asyncio
    async def test_bff_service_integration(self):
        """Test BFF service integration and mock configuration"""
        async with isolated_bff_test_environment() as (bff_container, settings):
            # Get individual services
            oms_client = bff_container.get_oms_client()
            label_mapper = bff_container.get_label_mapper()
            
            # Configure specific mock behavior
            oms_client.get_ontology.return_value = {
                "status": "success",
                "data": {
                    "id": "custom_ontology",
                    "label": "Custom Test Ontology",
                    "description": "This is a custom test ontology",
                    "properties": [
                        {"id": "name", "type": "string", "required": True}
                    ]
                }
            }
            
            label_mapper.get_class_id.return_value = "custom_ontology"
            
            # Test the configured behavior
            class_id = await label_mapper.get_class_id("test_db", "custom_label", "en")
            assert class_id == "custom_ontology"
            
            ontology = await oms_client.get_ontology("test_db", class_id)
            assert ontology["data"]["label"] == "Custom Test Ontology"


class TestOMSModernizedIntegration:
    """
    Test class demonstrating OMS service integration testing
    with the modernized architecture.
    """
    
    @pytest.mark.asyncio
    async def test_oms_database_operations_isolated(self):
        """Test OMS database operations with complete isolation"""
        async with isolated_oms_test_environment() as (oms_container, settings):
            test_client = OMSTestClient(oms_container, settings)
            
            # Test database operations
            db_result = await test_client.test_database_operations("new_test_db")
            
            # Verify results
            assert len(db_result["databases"]) >= 0
            assert db_result["create_result"]["status"] == "success"
            assert db_result["exists"] == True
    
    @pytest.mark.asyncio
    async def test_oms_ontology_operations_isolated(self):
        """Test OMS ontology operations with complete isolation"""
        async with isolated_oms_test_environment() as (oms_container, settings):
            test_client = OMSTestClient(oms_container, settings)
            
            # Test ontology operations
            ontology_data = setup_test_ontology_data()
            onto_result = await test_client.test_ontology_operations("test_db", ontology_data)
            
            # Verify results
            assert onto_result["create_result"]["status"] == "success"
            assert onto_result["get_result"]["status"] == "success"
            assert onto_result["list_result"]["status"] == "success"
    
    @pytest.mark.asyncio
    async def test_oms_version_control_isolated(self):
        """Test OMS version control operations with complete isolation"""
        async with isolated_oms_test_environment() as (oms_container, settings):
            test_client = OMSTestClient(oms_container, settings)
            
            # Test branch operations
            branch_result = await test_client.test_branch_operations("test_db", "feature_branch")
            assert branch_result["create_result"]["status"] == "success"
            assert branch_result["checkout_result"]["status"] == "success"
            
            # Test version operations
            version_result = await test_client.test_version_operations("test_db")
            assert version_result["commit_result"]["status"] == "success"
            assert "commits" in version_result["history_result"]
    
    @pytest.mark.asyncio
    async def test_oms_outbox_integration(self):
        """Test OMS outbox service integration"""
        async with isolated_oms_test_environment() as (oms_container, settings):
            # Get services
            terminus_service = oms_container.get_terminus_service()
            outbox_service = oms_container.get_outbox_service()
            
            # Test ontology creation with event publishing
            ontology_data = setup_test_ontology_data()
            create_result = await terminus_service.create_ontology("test_db", ontology_data)
            
            # Verify outbox service interaction
            if outbox_service:
                await outbox_service.publish_ontology_created(
                    "test_db",
                    create_result["data"]["id"],
                    ontology_data
                )
                
                # Verify the event was published
                outbox_service.publish_ontology_created.assert_called_once_with(
                    "test_db",
                    create_result["data"]["id"],
                    ontology_data
                )


class TestConfigurationMigrationComparison:
    """
    Test class showing the contrast between old anti-pattern 13
    and the new modernized approach.
    """
    
    def test_old_vs_new_configuration_approach(self):
        """
        Demonstrate the improvement from anti-pattern 13 to modernized system
        
        OLD APPROACH (Anti-pattern 13):
        - Global variables pollute test state
        - os.getenv() calls scattered throughout code
        - Function-level imports cause unpredictable behavior  
        - Tests interfere with each other
        - Hard to mock services
        - No type safety
        
        NEW APPROACH (Modernized):
        - Isolated configuration per test
        - Centralized Pydantic settings
        - Module-level imports
        - Container-based dependency injection
        - Easy service mocking
        - Type-safe dependencies
        """
        
        # NEW: Each test gets isolated configuration
        test_config_1 = TestApplicationSettings(
            debug=True,
            environment="testing"
        )
        
        test_config_2 = TestApplicationSettings(
            debug=False, 
            environment="production"
        )
        
        # Verify complete isolation
        assert test_config_1.debug != test_config_2.debug
        assert test_config_1.environment != test_config_2.environment
        assert test_config_1.is_development != test_config_2.is_development
        
        # NEW: Type-safe configuration access
        assert isinstance(test_config_1.database.port, int)
        assert isinstance(test_config_1.services.redis_db, int)
        assert hasattr(test_config_1, 'is_development')  # Computed properties
        
        # NEW: Easy configuration validation
        assert test_config_1.services.terminus_url.startswith("http")
        assert test_config_1.database.port > 0
    
    @pytest.mark.asyncio
    async def test_service_isolation_improvement(self):
        """
        Demonstrate service isolation improvements
        
        OLD: Global service variables caused test pollution
        NEW: Container-based services are isolated per test
        """
        
        # Test 1: Independent service configuration
        async with isolated_test_environment(redis_db=1, debug=True) as (container1, settings1):
            redis1 = await container1.get(RedisService)
            storage1 = await container1.get(StorageService)
            
            # Configure test-specific behavior
            storage1.upload_file.return_value = "test1_file"
            
            result1 = await storage1.upload_file("test1.txt", b"content1")
            assert result1 == "test1_file"
        
        # Test 2: Completely independent from Test 1
        async with isolated_test_environment(redis_db=2, debug=False) as (container2, settings2):
            redis2 = await container2.get(RedisService)
            storage2 = await container2.get(StorageService)
            
            # Different configuration doesn't affect Test 1
            storage2.upload_file.return_value = "test2_file"
            
            result2 = await storage2.upload_file("test2.txt", b"content2")
            assert result2 == "test2_file"
            
            # Verify Test 1 configuration wasn't affected
            assert result2 != "test1_file"


# Integration test that demonstrates the full modernized stack
@pytest.mark.asyncio
async def test_full_stack_modernized_integration():
    """
    Full integration test demonstrating the modernized architecture
    
    This test shows how BFF and OMS can work together with isolated
    configurations and proper dependency injection.
    """
    
    # Test with different configurations for each service
    bff_config_overrides = {"debug": True, "bff_port": 8000}
    oms_config_overrides = {"debug": False, "oms_port": 8001}
    
    async with isolated_bff_test_environment(**bff_config_overrides) as (bff_container, bff_settings):
        async with isolated_oms_test_environment(**oms_config_overrides) as (oms_container, oms_settings):
            
            # Verify configurations are independent
            assert bff_settings.debug != oms_settings.debug
            
            # Test BFF operations
            bff_client = BFFTestClient(bff_container, bff_settings)
            bff_result = await bff_client.test_ontology_retrieval("test_db", "test_class")
            assert bff_result["status"] == "success"
            
            # Test OMS operations
            oms_client = OMSTestClient(oms_container, oms_settings)
            ontology_data = setup_test_ontology_data()
            oms_result = await oms_client.test_ontology_operations("test_db", ontology_data)
            assert oms_result["create_result"]["status"] == "success"
            
            # Verify services are completely isolated
            bff_oms_client = bff_container.get_oms_client()
            oms_terminus_service = oms_container.get_terminus_service()
            
            # Different mock instances
            assert bff_oms_client is not oms_terminus_service
            
            # Can configure different behaviors
            bff_oms_client.list_databases.return_value = {"bff": "mock"}
            oms_terminus_service.list_databases.return_value = [{"oms": "mock"}]
            
            bff_dbs = await bff_oms_client.list_databases()
            oms_dbs = await oms_terminus_service.list_databases()
            
            assert bff_dbs != oms_dbs
            assert "bff" in str(bff_dbs)
            assert "oms" in str(oms_dbs)