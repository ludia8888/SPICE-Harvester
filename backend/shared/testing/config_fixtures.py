"""
Test Configuration Fixtures

This module provides test utilities for the modernized configuration system,
enabling test isolation and easy mocking of services and settings.

Key features:
1. ✅ Isolated test settings that don't affect global state
2. ✅ Mock service container for testing
3. ✅ Easy service mocking utilities
4. ✅ Test database configuration
5. ✅ Configuration override utilities
"""

import asyncio
import os
from typing import Any, Dict, Optional, Type, TypeVar, AsyncGenerator, Generator
from unittest.mock import Mock, AsyncMock
import pytest
from contextlib import asynccontextmanager

from shared.config.settings import ApplicationSettings, Environment, DatabaseSettings, ServiceSettings
from shared.dependencies import ServiceContainer
from shared.services import (
    StorageService, RedisService, ElasticsearchService, 
    CommandStatusService
)
from shared.utils.label_mapper import LabelMapper
from shared.utils.jsonld import JSONToJSONLDConverter

T = TypeVar('T')


class TestApplicationSettings(ApplicationSettings):
    """
    Test-specific application settings that provide safe defaults for testing
    
    This class ensures that tests don't interfere with production configuration
    and provides consistent test environment setup.
    """
    
    def __init__(self, **overrides):
        # Set test-safe defaults
        test_defaults = {
            "environment": Environment.TESTING,
            "debug": True,
            "database": DatabaseSettings(
                host="localhost",
                port=5432,
                name="spice_harvester_test",
                user="test_user",
                password="test_password"
            ),  
            "services": ServiceSettings(
                terminus_url="http://localhost:6363",
                terminus_user="test_admin",
                terminus_account="test_admin", 
                terminus_key="test_key",
                oms_base_url="http://localhost:8001",
                bff_base_url="http://localhost:8000",
                redis_host="localhost",
                redis_port=6379,
                redis_db=1,  # Use different DB for tests
                elasticsearch_host="localhost",
                elasticsearch_port=9200,
                elasticsearch_username="elastic",
                elasticsearch_password="test_password"
            )
        }
        
        # Merge with any user overrides
        test_defaults.update(overrides)
        
        # Initialize with test settings
        super().__init__(**test_defaults)


class MockServiceContainer:
    """
    Mock service container for testing
    
    This provides a test-friendly version of ServiceContainer that:
    - Doesn't require actual service connections
    - Allows easy service mocking
    - Provides isolated service instances per test
    """
    
    def __init__(self, settings: TestApplicationSettings):
        self.settings = settings
        self._services: Dict[Type, Any] = {}
        self._factories: Dict[Type, callable] = {}
        self.is_initialized = True
    
    def register_singleton(self, service_type: Type[T], factory: callable) -> None:
        """Register a service factory"""
        self._factories[service_type] = factory
    
    def register_mock(self, service_type: Type[T], mock_instance: Any) -> None:
        """Register a mock service instance directly"""
        self._services[service_type] = mock_instance
    
    def has(self, service_type: Type[T]) -> bool:
        """Check if service type is registered"""
        return service_type in self._services or service_type in self._factories
    
    async def get(self, service_type: Type[T]) -> T:
        """Get service instance (async)"""
        if service_type in self._services:
            return self._services[service_type]
        
        if service_type in self._factories:
            factory = self._factories[service_type]
            if asyncio.iscoroutinefunction(factory):
                instance = await factory(self.settings)
            else:
                instance = factory(self.settings)
            self._services[service_type] = instance
            return instance
        
        raise ValueError(f"Service {service_type} not registered")
    
    def get_sync(self, service_type: Type[T]) -> T:
        """Get service instance (sync)"""
        if service_type in self._services:
            return self._services[service_type]
        
        if service_type in self._factories:
            factory = self._factories[service_type]
            instance = factory(self.settings)
            self._services[service_type] = instance
            return instance
        
        raise ValueError(f"Service {service_type} not registered")
    
    async def health_check_all(self) -> Dict[str, str]:
        """Mock health check for all services"""
        health_status = {}
        for service_type, service in self._services.items():
            service_name = service_type.__name__
            if hasattr(service, 'health_check'):
                try:
                    if asyncio.iscoroutinefunction(service.health_check):
                        is_healthy = await service.health_check()
                    else:
                        is_healthy = service.health_check()
                    health_status[service_name] = "healthy" if is_healthy else "unhealthy"
                except Exception as e:
                    health_status[service_name] = f"error: {str(e)}"
            else:
                health_status[service_name] = "mock_healthy"
        return health_status
    
    async def shutdown(self) -> None:
        """Shutdown all services"""
        for service in self._services.values():
            if hasattr(service, 'close'):
                try:
                    if asyncio.iscoroutinefunction(service.close):
                        await service.close()
                    else:
                        service.close()
                except Exception:
                    pass  # Ignore errors during test cleanup
            elif hasattr(service, 'disconnect'):
                try:
                    if asyncio.iscoroutinefunction(service.disconnect):
                        await service.disconnect()
                    else:
                        service.disconnect()
                except Exception:
                    pass  # Ignore errors during test cleanup


def create_mock_storage_service() -> Mock:
    """Create mock storage service for testing"""
    mock_storage = AsyncMock(spec=StorageService)
    mock_storage.health_check = AsyncMock(return_value=True)
    mock_storage.upload_file = AsyncMock(return_value="mock_file_path")
    mock_storage.download_file = AsyncMock(return_value=b"mock_file_content")
    mock_storage.delete_file = AsyncMock(return_value=True)
    return mock_storage


def create_mock_redis_service() -> Mock:
    """Create mock Redis service for testing"""
    mock_redis = AsyncMock(spec=RedisService)
    mock_redis.health_check = AsyncMock(return_value=True)
    mock_redis.connect = AsyncMock()
    mock_redis.disconnect = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.set = AsyncMock(return_value=True)
    mock_redis.delete = AsyncMock(return_value=True)
    return mock_redis


def create_mock_elasticsearch_service() -> Mock:
    """Create mock Elasticsearch service for testing"""
    mock_elasticsearch = AsyncMock(spec=ElasticsearchService)
    mock_elasticsearch.health_check = AsyncMock(return_value=True)
    mock_elasticsearch.connect = AsyncMock()
    mock_elasticsearch.disconnect = AsyncMock()
    mock_elasticsearch.search = AsyncMock(return_value={"hits": {"total": {"value": 0}, "hits": []}})
    mock_elasticsearch.index = AsyncMock(return_value={"_id": "mock_id"})
    return mock_elasticsearch


def create_mock_label_mapper() -> Mock:
    """Create mock label mapper for testing"""
    mock_mapper = Mock(spec=LabelMapper)
    mock_mapper.get_class_id = AsyncMock(return_value="mock_class_id")
    mock_mapper.convert_to_display = AsyncMock(return_value={"id": "mock_id", "label": "mock_label"})
    return mock_mapper


def create_mock_jsonld_converter() -> Mock:
    """Create mock JSON-LD converter for testing"""
    mock_converter = Mock(spec=JSONToJSONLDConverter)
    mock_converter.to_jsonld = Mock(return_value={"@context": {}, "@type": "mock"})
    mock_converter.from_jsonld = Mock(return_value={"type": "mock"})
    return mock_converter


@pytest.fixture
def test_settings() -> TestApplicationSettings:
    """Pytest fixture for test application settings"""
    return TestApplicationSettings()


@pytest.fixture
def test_settings_with_overrides():
    """Pytest fixture factory for test settings with custom overrides"""
    def _create_settings(**overrides) -> TestApplicationSettings:
        return TestApplicationSettings(**overrides)
    return _create_settings


@pytest.fixture
async def mock_container(test_settings: TestApplicationSettings) -> AsyncGenerator[MockServiceContainer, None]:
    """Pytest fixture for mock service container"""
    container = MockServiceContainer(test_settings)
    
    # Register common mock services
    container.register_mock(StorageService, create_mock_storage_service())
    container.register_mock(RedisService, create_mock_redis_service())
    container.register_mock(ElasticsearchService, create_mock_elasticsearch_service())
    container.register_mock(LabelMapper, create_mock_label_mapper())
    container.register_mock(JSONToJSONLDConverter, create_mock_jsonld_converter())
    
    yield container
    
    # Cleanup
    await container.shutdown()


@pytest.fixture
def mock_command_status_service():
    """Pytest fixture for mock command status service"""
    mock_service = AsyncMock(spec=CommandStatusService)
    mock_service.set_status = AsyncMock()
    mock_service.get_status = AsyncMock(return_value="completed")
    mock_service.set_progress = AsyncMock()
    return mock_service


class ConfigOverride:
    """
    Context manager for temporary configuration overrides
    
    This allows tests to temporarily override specific configuration
    values without affecting other tests.
    """
    
    def __init__(self, **overrides):
        self.overrides = overrides
        self.original_values = {}
    
    def __enter__(self):
        # Store original environment variables
        for key, value in self.overrides.items():
            env_key = key.upper()
            self.original_values[env_key] = os.environ.get(env_key)
            os.environ[env_key] = str(value)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore original environment variables
        for env_key, original_value in self.original_values.items():
            if original_value is None:
                os.environ.pop(env_key, None)
            else:
                os.environ[env_key] = original_value


@asynccontextmanager
async def isolated_test_environment(**config_overrides):
    """
    Async context manager for completely isolated test environment
    
    This ensures that tests run with isolated configuration and services,
    preventing test interference and providing clean test state.
    """
    with ConfigOverride(**config_overrides):
        # Create isolated test settings
        settings = TestApplicationSettings()
        
        # Create isolated mock container
        container = MockServiceContainer(settings)
        
        # Register standard mock services
        container.register_mock(StorageService, create_mock_storage_service())
        container.register_mock(RedisService, create_mock_redis_service())
        container.register_mock(ElasticsearchService, create_mock_elasticsearch_service())
        container.register_mock(LabelMapper, create_mock_label_mapper())
        container.register_mock(JSONToJSONLDConverter, create_mock_jsonld_converter())
        
        try:
            yield container, settings
        finally:
            await container.shutdown()


# Utility functions for test setup
def setup_test_database_config(**overrides) -> TestApplicationSettings:
    """Create test settings with database configuration"""
    db_overrides = {
        "database": DatabaseSettings(
            host=overrides.get("db_host", "localhost"),
            port=overrides.get("db_port", 5432),
            name=overrides.get("db_name", "test_db"),
            user=overrides.get("db_user", "test_user"),
            password=overrides.get("db_password", "test_password")
        )
    }
    return TestApplicationSettings(**db_overrides)


def setup_test_service_config(**overrides) -> TestApplicationSettings:
    """Create test settings with service configuration"""
    service_overrides = {
        "services": ServiceSettings(
            terminus_url=overrides.get("terminus_url", "http://localhost:6363"),
            oms_base_url=overrides.get("oms_url", "http://localhost:8001"),
            bff_base_url=overrides.get("bff_url", "http://localhost:8000"),
            redis_host=overrides.get("redis_host", "localhost"),
            redis_port=overrides.get("redis_port", 6379),
            redis_db=overrides.get("redis_db", 1),
            elasticsearch_host=overrides.get("es_host", "localhost"),
            elasticsearch_port=overrides.get("es_port", 9200)
        )
    }
    return TestApplicationSettings(**service_overrides)


# Example usage and documentation
"""
Example usage in tests:

# Basic test with isolated settings
@pytest.mark.asyncio
async def test_with_isolated_config():
    async with isolated_test_environment(debug=True, redis_db=2) as (container, settings):
        storage_service = await container.get(StorageService)
        result = await storage_service.upload_file("test.txt", b"content")
        assert result == "mock_file_path"

# Using pytest fixtures
@pytest.mark.asyncio 
async def test_with_fixtures(mock_container, test_settings):
    label_mapper = await mock_container.get(LabelMapper)
    class_id = await label_mapper.get_class_id("test_db", "test_label", "en")
    assert class_id == "mock_class_id"

# Configuration override
def test_with_config_override():
    with ConfigOverride(redis_port=6380, debug=False):
        settings = TestApplicationSettings()
        assert settings.services.redis_port == 6380
        assert settings.debug == False
"""