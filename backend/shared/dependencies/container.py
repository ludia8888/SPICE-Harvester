"""
Modern Dependency Injection Container for SPICE HARVESTER

This module provides a centralized service container to replace global variables
and scattered dependency management, resolving anti-pattern 13.

Features:
- Type-safe service registration and retrieval
- Singleton service management
- Lazy initialization support
- Service lifecycle management
- Thread-safe operation
- Test-friendly mocking support
"""

import asyncio
import logging
from typing import Dict, Any, TypeVar, Type, Optional, Callable, Generic, Protocol
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager

from shared.config.settings import ApplicationSettings

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ServiceLifecycle(Protocol):
    """Protocol for services that have lifecycle management"""
    
    async def initialize(self) -> None:
        """Initialize the service"""
        ...
    
    async def health_check(self) -> bool:
        """Check if the service is healthy"""
        ...
    
    async def shutdown(self) -> None:
        """Shutdown the service gracefully"""
        ...


class ServiceFactory(Protocol, Generic[T]):
    """Protocol for service factory functions"""
    
    def __call__(self, settings: ApplicationSettings) -> T:
        """Create a service instance from settings"""
        ...


@dataclass
class ServiceRegistration:
    """Service registration information"""
    service_type: Type
    instance: Optional[Any] = None
    factory: Optional[Callable[[ApplicationSettings], Any]] = None
    singleton: bool = True
    initialized: bool = False


class ServiceContainer:
    """
    Modern dependency injection container
    
    This replaces the scattered global variables and setter/getter patterns
    throughout the codebase with a centralized, type-safe service management system.
    """
    
    def __init__(self, settings: ApplicationSettings):
        """
        Initialize the service container
        
        Args:
            settings: Application settings instance
        """
        self.settings = settings
        self._services: Dict[str, ServiceRegistration] = {}
        self._lock = asyncio.Lock()
        self._initialized = False
        
    @property
    def is_initialized(self) -> bool:
        """Check if container is initialized"""
        return self._initialized
    
    def register_singleton(
        self, 
        service_type: Type[T], 
        factory: Callable[[ApplicationSettings], T]
    ) -> None:
        """
        Register a singleton service with a factory function
        
        Args:
            service_type: The service class/type
            factory: Factory function that creates the service
        """
        service_name = service_type.__name__
        if service_name in self._services:
            logger.warning(f"Service {service_name} is already registered, overriding")
        
        self._services[service_name] = ServiceRegistration(
            service_type=service_type,
            factory=factory,
            singleton=True
        )
        logger.debug(f"Registered singleton service: {service_name}")
    
    def register_instance(self, service_type: Type[T], instance: T) -> None:
        """
        Register a service instance directly
        
        Args:
            service_type: The service class/type
            instance: Pre-created service instance
        """
        service_name = service_type.__name__
        if service_name in self._services:
            logger.warning(f"Service {service_name} is already registered, overriding")
        
        self._services[service_name] = ServiceRegistration(
            service_type=service_type,
            instance=instance,
            singleton=True,
            initialized=True
        )
        logger.debug(f"Registered service instance: {service_name}")
    
    async def get(self, service_type: Type[T]) -> T:
        """
        Get a service instance (thread-safe)
        
        Args:
            service_type: The service class/type to retrieve
            
        Returns:
            Service instance
            
        Raises:
            ValueError: If service is not registered
            RuntimeError: If service creation fails
        """
        service_name = service_type.__name__
        
        if service_name not in self._services:
            raise ValueError(f"Service {service_name} is not registered")
        
        registration = self._services[service_name]
        
        # Return existing instance if available
        if registration.instance is not None:
            return registration.instance
        
        # Create new instance (thread-safe)
        async with self._lock:
            # Double-check pattern - another thread might have created it
            if registration.instance is not None:
                return registration.instance
            
            if registration.factory is None:
                raise RuntimeError(f"No factory function registered for {service_name}")
            
            try:
                logger.debug(f"Creating service instance: {service_name}")
                instance = registration.factory(self.settings)
                
                # Initialize if service supports lifecycle
                if hasattr(instance, 'initialize'):
                    await instance.initialize()
                    logger.debug(f"Initialized service: {service_name}")
                
                registration.instance = instance
                registration.initialized = True
                
                return instance
                
            except Exception as e:
                logger.error(f"Failed to create service {service_name}: {e}")
                raise RuntimeError(f"Service creation failed for {service_name}: {e}")
    
    def has(self, service_type: Type[T]) -> bool:
        """
        Check if a service is registered
        
        Args:
            service_type: The service class/type to check
            
        Returns:
            True if service is registered
        """
        return service_type.__name__ in self._services
    
    def is_created(self, service_type: Type[T]) -> bool:
        """
        Check if a service instance has been created
        
        Args:
            service_type: The service class/type to check
            
        Returns:
            True if service instance exists
        """
        service_name = service_type.__name__
        if service_name not in self._services:
            return False
        return self._services[service_name].instance is not None
    
    async def health_check_all(self) -> Dict[str, bool]:
        """
        Perform health check on all created services
        
        Returns:
            Dictionary mapping service names to health status
        """
        results = {}
        
        for service_name, registration in self._services.items():
            if registration.instance is None:
                results[service_name] = True  # Not created yet, considered healthy
                continue
            
            try:
                if hasattr(registration.instance, 'health_check'):
                    is_healthy = await registration.instance.health_check()
                    results[service_name] = is_healthy
                else:
                    results[service_name] = True  # No health check method, assume healthy
            except Exception as e:
                logger.error(f"Health check failed for {service_name}: {e}")
                results[service_name] = False
        
        return results
    
    async def shutdown_all(self) -> None:
        """
        Shutdown all created services gracefully
        """
        logger.info("Shutting down all services")
        
        for service_name, registration in self._services.items():
            if registration.instance is None:
                continue
            
            try:
                if hasattr(registration.instance, 'shutdown'):
                    await registration.instance.shutdown()
                    logger.debug(f"Service {service_name} shut down successfully")
            except Exception as e:
                logger.error(f"Error shutting down service {service_name}: {e}")
        
        # Clear all instances
        for registration in self._services.values():
            registration.instance = None
            registration.initialized = False
        
        self._initialized = False
        logger.info("All services shut down")
    
    def get_service_info(self) -> Dict[str, Dict[str, Any]]:
        """
        Get information about registered services
        
        Returns:
            Dictionary with service information
        """
        info = {}
        for service_name, registration in self._services.items():
            info[service_name] = {
                'type': registration.service_type.__name__,
                'singleton': registration.singleton,
                'created': registration.instance is not None,
                'initialized': registration.initialized
            }
        return info
    
    async def initialize_container(self) -> None:
        """
        Initialize the container and mark as ready
        """
        self._initialized = True
        logger.info(f"Service container initialized with {len(self._services)} registered services")


# Global container instance (replaces scattered global variables)
_container: Optional[ServiceContainer] = None
_container_lock = asyncio.Lock()


async def get_container() -> ServiceContainer:
    """
    Get the global service container
    
    Returns:
        ServiceContainer: The global container instance
        
    Raises:
        RuntimeError: If container is not initialized
    """
    if _container is None:
        raise RuntimeError(
            "Service container not initialized. "
            "Call initialize_container() in your application startup."
        )
    return _container


async def initialize_container(settings: ApplicationSettings) -> ServiceContainer:
    """
    Initialize the global service container (thread-safe)
    
    This should be called once during application startup.
    
    Args:
        settings: Application settings instance
        
    Returns:
        ServiceContainer: The initialized container
    """
    global _container
    
    async with _container_lock:
        if _container is not None:
            logger.warning("Service container already initialized, returning existing instance")
            return _container
        
        _container = ServiceContainer(settings)
        await _container.initialize_container()
        
        logger.info("Global service container initialized")
        return _container


async def shutdown_container() -> None:
    """
    Shutdown the global service container
    
    This should be called during application shutdown.
    """
    global _container
    
    if _container is not None:
        await _container.shutdown_all()
        _container = None
        logger.info("Global service container shut down")


@asynccontextmanager
async def container_lifespan(settings: ApplicationSettings):
    """
    Async context manager for container lifecycle
    
    Usage:
        async with container_lifespan(settings) as container:
            # Use container here
            pass
    
    Args:
        settings: Application settings
        
    Yields:
        ServiceContainer: The initialized container
    """
    container = await initialize_container(settings)
    try:
        yield container
    finally:
        await shutdown_container()


def get_settings_from_container() -> ApplicationSettings:
    """
    Get settings from the global container (synchronous)
    
    Returns:
        ApplicationSettings: The settings instance
        
    Raises:
        RuntimeError: If container is not initialized
    """
    if _container is None:
        raise RuntimeError("Service container not initialized")
    return _container.settings