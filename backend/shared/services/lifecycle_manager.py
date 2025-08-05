"""
Service Lifecycle Manager

This module provides comprehensive service lifecycle management for the modernized
architecture, including startup, shutdown, monitoring, and graceful degradation.

Key features:
1. ✅ Structured service initialization and shutdown
2. ✅ Dependency-aware startup ordering  
3. ✅ Graceful degradation when services fail
4. ✅ Service state monitoring and recovery
5. ✅ Performance metrics and observability
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Set, Any, Callable, Awaitable
from weakref import WeakSet

from .health_check import (
    HealthCheckInterface, HealthCheckAggregator, HealthCheckResult,
    HealthStatus, ServiceType, AggregatedHealthStatus
)

logger = logging.getLogger(__name__)


class ServiceState(Enum):
    """Service lifecycle states"""
    UNINITIALIZED = "uninitialized"
    INITIALIZING = "initializing"
    RUNNING = "running"
    DEGRADED = "degraded"
    SHUTTING_DOWN = "shutting_down"
    STOPPED = "stopped"
    FAILED = "failed"


class ServicePriority(Enum):
    """Service initialization priority levels"""
    CRITICAL = 0    # Database, core infrastructure
    HIGH = 1        # Redis, authentication
    MEDIUM = 2      # Business logic services
    LOW = 3         # Optional services like monitoring


@dataclass
class ServiceMetrics:
    """Metrics for a service"""
    name: str
    startup_time_ms: float = 0.0
    total_requests: int = 0
    failed_requests: int = 0
    average_response_time_ms: float = 0.0
    last_health_check: Optional[datetime] = None
    health_check_failures: int = 0
    state_changes: List[Dict[str, Any]] = field(default_factory=list)
    
    def add_state_change(self, from_state: ServiceState, to_state: ServiceState, reason: str = ""):
        """Record a state change"""
        self.state_changes.append({
            "from_state": from_state.value,
            "to_state": to_state.value,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "reason": reason
        })
    
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        if self.total_requests == 0:
            return 100.0
        return ((self.total_requests - self.failed_requests) / self.total_requests) * 100.0


class ServiceLifecycleInterface(ABC):
    """Interface for services that participate in lifecycle management"""
    
    @property
    @abstractmethod
    def service_name(self) -> str:
        """Return the service name"""
        pass
    
    @property
    @abstractmethod
    def service_type(self) -> ServiceType:
        """Return the service type classification"""
        pass
    
    @property
    @abstractmethod
    def service_priority(self) -> ServicePriority:
        """Return the service initialization priority"""
        pass
    
    @property
    @abstractmethod
    def dependencies(self) -> List[str]:
        """Return list of service names this service depends on"""
        pass
    
    @abstractmethod
    async def initialize(self) -> bool:
        """Initialize the service. Return True if successful."""
        pass
    
    @abstractmethod
    async def shutdown(self) -> bool:
        """Shutdown the service. Return True if successful."""
        pass
    
    @abstractmethod
    async def health_check(self) -> HealthCheckResult:
        """Perform health check"""
        pass
    
    async def on_dependency_failed(self, dependency_name: str, error: Exception) -> bool:
        """
        Called when a dependency fails.
        Return True if this service can continue without the dependency.
        """
        return False


@dataclass
class ManagedService:
    """A service under lifecycle management"""
    service: ServiceLifecycleInterface
    health_checker: Optional[HealthCheckInterface] = None
    state: ServiceState = ServiceState.UNINITIALIZED
    metrics: ServiceMetrics = None
    last_error: Optional[str] = None
    startup_attempts: int = 0
    max_startup_attempts: int = 3
    
    def __post_init__(self):
        if self.metrics is None:
            self.metrics = ServiceMetrics(name=self.service.service_name)


class ServiceLifecycleManager:
    """
    Manages the lifecycle of all services in the application
    
    Provides dependency-aware startup, monitoring, and graceful shutdown.
    """
    
    def __init__(self):
        self.services: Dict[str, ManagedService] = {}
        self.health_aggregator = HealthCheckAggregator()
        self.startup_hooks: List[Callable[[], Awaitable[None]]] = []
        self.shutdown_hooks: List[Callable[[], Awaitable[None]]] = []
        self.monitoring_task: Optional[asyncio.Task] = None
        self.monitoring_interval: float = 30.0  # seconds
        self.state_change_callbacks: WeakSet = WeakSet()
        self._shutdown_event = asyncio.Event()
    
    def register_service(
        self, 
        service: ServiceLifecycleInterface,
        health_checker: Optional[HealthCheckInterface] = None,
        max_startup_attempts: int = 3
    ):
        """Register a service for lifecycle management"""
        managed_service = ManagedService(
            service=service,
            health_checker=health_checker,
            max_startup_attempts=max_startup_attempts
        )
        
        self.services[service.service_name] = managed_service
        
        # Register health checker if provided
        if health_checker:
            self.health_aggregator.register_health_checker(health_checker)
        
        logger.info(f"Registered service: {service.service_name}")
    
    def add_startup_hook(self, hook: Callable[[], Awaitable[None]]):
        """Add a startup hook"""
        self.startup_hooks.append(hook)
    
    def add_shutdown_hook(self, hook: Callable[[], Awaitable[None]]):
        """Add a shutdown hook"""
        self.shutdown_hooks.append(hook)
    
    def add_state_change_callback(self, callback: Callable[[str, ServiceState, ServiceState], None]):
        """Add a callback for service state changes"""
        self.state_change_callbacks.add(callback)
    
    async def start_all_services(self) -> bool:
        """Start all services in dependency order"""
        logger.info("Starting service lifecycle manager...")
        
        try:
            # Execute startup hooks
            for hook in self.startup_hooks:
                try:
                    await hook()
                except Exception as e:
                    logger.error(f"Startup hook failed: {e}")
            
            # Get initialization order
            init_order = self._get_initialization_order()
            
            # Initialize services in order
            all_started = True
            for service_name in init_order:
                success = await self._start_service(service_name)
                if not success:
                    managed_service = self.services[service_name]
                    if managed_service.service.service_type == ServiceType.CORE:
                        logger.error(f"Critical service {service_name} failed to start")
                        all_started = False
                        break
                    else:
                        logger.warning(f"Non-critical service {service_name} failed to start, continuing")
            
            # Start monitoring
            if all_started or self._has_running_services():
                await self._start_monitoring()
            
            logger.info(f"Service lifecycle manager started. Services running: {self._count_running_services()}")
            return all_started
            
        except Exception as e:
            logger.error(f"Failed to start services: {e}")
            return False
    
    async def shutdown_all_services(self):
        """Shutdown all services in reverse dependency order"""
        logger.info("Shutting down service lifecycle manager...")
        
        # Signal shutdown
        self._shutdown_event.set()
        
        # Stop monitoring
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        
        # Get shutdown order (reverse of initialization)
        init_order = self._get_initialization_order()
        shutdown_order = list(reversed(init_order))
        
        # Shutdown services
        for service_name in shutdown_order:
            await self._shutdown_service(service_name)
        
        # Execute shutdown hooks
        for hook in reversed(self.shutdown_hooks):
            try:
                await hook()
            except Exception as e:
                logger.error(f"Shutdown hook failed: {e}")
        
        logger.info("Service lifecycle manager shutdown complete")
    
    async def get_system_health(self) -> AggregatedHealthStatus:
        """Get comprehensive system health status"""
        return await self.health_aggregator.check_all_services()
    
    def get_service_metrics(self, service_name: str) -> Optional[ServiceMetrics]:
        """Get metrics for a specific service"""
        managed_service = self.services.get(service_name)
        return managed_service.metrics if managed_service else None
    
    def get_all_service_metrics(self) -> Dict[str, ServiceMetrics]:
        """Get metrics for all services"""
        return {
            name: managed_service.metrics 
            for name, managed_service in self.services.items()
        }
    
    def get_service_state(self, service_name: str) -> Optional[ServiceState]:
        """Get current state of a service"""
        managed_service = self.services.get(service_name)
        return managed_service.state if managed_service else None
    
    async def restart_service(self, service_name: str) -> bool:
        """Restart a specific service"""
        logger.info(f"Restarting service: {service_name}")
        
        # Shutdown first
        await self._shutdown_service(service_name)
        
        # Wait a bit for cleanup
        await asyncio.sleep(1.0)
        
        # Start again
        return await self._start_service(service_name)
    
    def _get_initialization_order(self) -> List[str]:
        """Get service initialization order based on dependencies and priorities"""
        # Topological sort with priority ordering
        ordered = []
        visited = set()
        temp_visited = set()
        
        def visit(service_name: str):
            if service_name in temp_visited:
                raise ValueError(f"Circular dependency detected involving {service_name}")
            if service_name in visited:
                return
            
            temp_visited.add(service_name)
            
            # Visit dependencies first
            managed_service = self.services[service_name]
            for dep_name in managed_service.service.dependencies:
                if dep_name in self.services:
                    visit(dep_name)
            
            temp_visited.remove(service_name)
            visited.add(service_name)
            ordered.append(service_name)
        
        # Sort services by priority first
        services_by_priority = sorted(
            self.services.keys(),
            key=lambda name: (
                self.services[name].service.service_priority.value,
                self.services[name].service.service_name
            )
        )
        
        # Visit all services
        for service_name in services_by_priority:
            visit(service_name)
        
        return ordered
    
    async def _start_service(self, service_name: str) -> bool:
        """Start a single service"""
        managed_service = self.services.get(service_name)
        if not managed_service:
            logger.error(f"Service {service_name} not registered")
            return False
        
        if managed_service.state == ServiceState.RUNNING:
            return True
        
        logger.info(f"Starting service: {service_name}")
        
        # Update state
        self._change_service_state(managed_service, ServiceState.INITIALIZING)
        
        start_time = time.time()
        
        try:
            # Check dependencies
            for dep_name in managed_service.service.dependencies:
                dep_service = self.services.get(dep_name)
                if not dep_service or dep_service.state != ServiceState.RUNNING:
                    logger.error(f"Dependency {dep_name} not running for service {service_name}")
                    raise Exception(f"Dependency {dep_name} not available")
            
            # Initialize the service
            success = await managed_service.service.initialize()
            
            if success:
                # Record startup time
                startup_time = (time.time() - start_time) * 1000
                managed_service.metrics.startup_time_ms = startup_time
                
                # Update state
                self._change_service_state(managed_service, ServiceState.RUNNING)
                
                logger.info(f"Service {service_name} started successfully in {startup_time:.1f}ms")
                return True
            else:
                raise Exception("Service initialization returned False")
                
        except Exception as e:
            managed_service.startup_attempts += 1
            managed_service.last_error = str(e)
            
            logger.error(f"Failed to start service {service_name} (attempt {managed_service.startup_attempts}): {e}")
            
            # Determine final state
            if managed_service.startup_attempts >= managed_service.max_startup_attempts:
                self._change_service_state(managed_service, ServiceState.FAILED, f"Max startup attempts exceeded: {e}")
            else:
                self._change_service_state(managed_service, ServiceState.STOPPED, str(e))
            
            return False
    
    async def _shutdown_service(self, service_name: str):
        """Shutdown a single service"""
        managed_service = self.services.get(service_name)
        if not managed_service or managed_service.state in [ServiceState.STOPPED, ServiceState.UNINITIALIZED]:
            return
        
        logger.info(f"Shutting down service: {service_name}")
        
        # Update state
        self._change_service_state(managed_service, ServiceState.SHUTTING_DOWN)
        
        try:
            # Shutdown the service
            await managed_service.service.shutdown()
            self._change_service_state(managed_service, ServiceState.STOPPED)
            logger.info(f"Service {service_name} shut down successfully")
            
        except Exception as e:
            managed_service.last_error = str(e)
            self._change_service_state(managed_service, ServiceState.FAILED, f"Shutdown failed: {e}")
            logger.error(f"Failed to shutdown service {service_name}: {e}")
    
    def _change_service_state(self, managed_service: ManagedService, new_state: ServiceState, reason: str = ""):
        """Change service state and notify callbacks"""
        old_state = managed_service.state
        managed_service.state = new_state
        managed_service.metrics.add_state_change(old_state, new_state, reason)
        
        # Notify callbacks
        for callback in self.state_change_callbacks:
            try:
                callback(managed_service.service.service_name, old_state, new_state)
            except Exception as e:
                logger.error(f"State change callback failed: {e}")
    
    async def _start_monitoring(self):
        """Start background monitoring task"""
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
    
    async def _monitoring_loop(self):
        """Background monitoring loop"""
        logger.info("Starting service monitoring loop")
        
        while not self._shutdown_event.is_set():
            try:
                await self._perform_health_checks()
                await asyncio.sleep(self.monitoring_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(5.0)  # Brief pause before retrying
    
    async def _perform_health_checks(self):
        """Perform health checks on all services"""
        for service_name, managed_service in self.services.items():
            if managed_service.state != ServiceState.RUNNING:
                continue
            
            try:
                # Perform health check
                health_result = await managed_service.service.health_check()
                managed_service.metrics.last_health_check = datetime.now(timezone.utc)
                
                # Handle health check result
                if health_result.status == HealthStatus.UNHEALTHY:
                    managed_service.metrics.health_check_failures += 1
                    
                    # Consider degrading or restarting the service
                    if managed_service.metrics.health_check_failures >= 3:
                        logger.warning(f"Service {service_name} has failed multiple health checks")
                        self._change_service_state(
                            managed_service, 
                            ServiceState.DEGRADED, 
                            "Multiple health check failures"
                        )
                else:
                    # Reset failure count on successful health check
                    managed_service.metrics.health_check_failures = 0
                    
                    # Restore to running if degraded
                    if managed_service.state == ServiceState.DEGRADED:
                        self._change_service_state(managed_service, ServiceState.RUNNING, "Health restored")
                        
            except Exception as e:
                logger.error(f"Health check failed for service {service_name}: {e}")
                managed_service.metrics.health_check_failures += 1
    
    def _has_running_services(self) -> bool:
        """Check if any services are running"""
        return any(
            managed_service.state == ServiceState.RUNNING 
            for managed_service in self.services.values()
        )
    
    def _count_running_services(self) -> int:
        """Count number of running services"""
        return sum(
            1 for managed_service in self.services.values()
            if managed_service.state == ServiceState.RUNNING
        )


@asynccontextmanager
async def managed_lifecycle(lifecycle_manager: ServiceLifecycleManager):
    """Context manager for automatic service lifecycle management"""
    try:
        success = await lifecycle_manager.start_all_services()
        if not success:
            logger.warning("Some services failed to start")
        yield lifecycle_manager
    finally:
        await lifecycle_manager.shutdown_all_services()


# Utility functions for common service patterns
def create_simple_service_wrapper(
    name: str,
    service_type: ServiceType,
    priority: ServicePriority,
    dependencies: List[str],
    init_func: Callable[[], Awaitable[bool]],
    shutdown_func: Callable[[], Awaitable[bool]],
    health_func: Callable[[], Awaitable[HealthCheckResult]]
) -> ServiceLifecycleInterface:
    """Create a simple service wrapper for functions"""
    
    class SimpleServiceWrapper(ServiceLifecycleInterface):
        @property
        def service_name(self) -> str:
            return name
        
        @property
        def service_type(self) -> ServiceType:
            return service_type
        
        @property
        def service_priority(self) -> ServicePriority:
            return priority
        
        @property
        def dependencies(self) -> List[str]:
            return dependencies
        
        async def initialize(self) -> bool:
            return await init_func()
        
        async def shutdown(self) -> bool:
            return await shutdown_func()
        
        async def health_check(self) -> HealthCheckResult:
            return await health_func()
    
    return SimpleServiceWrapper()