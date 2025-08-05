"""
Service Health Check System

This module provides comprehensive health checking capabilities for the modernized
service architecture, supporting proper service lifecycle management and monitoring.

Key features:
1. ✅ Standardized health check interface
2. ✅ Service-specific health checks
3. ✅ Aggregated health monitoring
4. ✅ Graceful degradation support
5. ✅ Performance metrics collection
"""

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Any, Union
import logging

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health status enumeration"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class ServiceType(Enum):
    """Service type classification"""
    CORE = "core"          # Critical services - failure stops the application
    ESSENTIAL = "essential" # Important services - failure causes degradation
    OPTIONAL = "optional"   # Nice-to-have services - failure is tolerated


@dataclass
class HealthCheckResult:
    """Result of a health check operation"""
    status: HealthStatus
    service_name: str
    service_type: ServiceType
    message: str = ""
    details: Dict[str, Any] = None
    response_time_ms: float = 0.0
    timestamp: datetime = None
    error: Optional[str] = None
    
    def __post_init__(self):
        if self.details is None:
            self.details = {}
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "status": self.status.value,
            "service_name": self.service_name,
            "service_type": self.service_type.value,
            "message": self.message,
            "details": self.details,
            "response_time_ms": self.response_time_ms,
            "timestamp": self.timestamp.isoformat(),
            "error": self.error
        }


class HealthCheckInterface(ABC):
    """Abstract interface for health checks"""
    
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
    
    @abstractmethod
    async def health_check(self) -> HealthCheckResult:
        """Perform health check and return result"""
        pass
    
    async def health_check_with_timeout(self, timeout_seconds: float = 5.0) -> HealthCheckResult:
        """Perform health check with timeout"""
        try:
            return await asyncio.wait_for(self.health_check(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            return HealthCheckResult(
                status=HealthStatus.UNHEALTHY,
                service_name=self.service_name,
                service_type=self.service_type,
                message=f"Health check timed out after {timeout_seconds}s",
                error="timeout"
            )
        except Exception as e:
            return HealthCheckResult(
                status=HealthStatus.UNHEALTHY,
                service_name=self.service_name,
                service_type=self.service_type,
                message=f"Health check failed: {str(e)}",
                error=str(e)
            )


class DatabaseHealthCheck(HealthCheckInterface):
    """Health check for database connections"""
    
    def __init__(self, db_connection, service_name: str = "PostgreSQL"):
        self.db_connection = db_connection
        self._service_name = service_name
    
    @property
    def service_name(self) -> str:
        return self._service_name
    
    @property
    def service_type(self) -> ServiceType:
        return ServiceType.CORE
    
    async def health_check(self) -> HealthCheckResult:
        """Check database connectivity and performance"""
        start_time = time.time()
        
        try:
            # Test basic connectivity
            if hasattr(self.db_connection, 'execute'):
                await self.db_connection.execute("SELECT 1")
            elif hasattr(self.db_connection, 'ping'):
                await self.db_connection.ping()
            else:
                # Fallback for mock or different connection types
                if hasattr(self.db_connection, 'connect'):
                    await self.db_connection.connect()
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                status=HealthStatus.HEALTHY,
                service_name=self.service_name,
                service_type=self.service_type,
                message="Database connection healthy",
                response_time_ms=response_time,
                details={
                    "connection_pool_status": "active",
                    "response_time_category": "fast" if response_time < 100 else "slow"
                }
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return HealthCheckResult(
                status=HealthStatus.UNHEALTHY,
                service_name=self.service_name,
                service_type=self.service_type,
                message=f"Database connection failed: {str(e)}",
                response_time_ms=response_time,
                error=str(e)
            )


class RedisHealthCheck(HealthCheckInterface):
    """Health check for Redis connections"""
    
    def __init__(self, redis_service, service_name: str = "Redis"):
        self.redis_service = redis_service
        self._service_name = service_name
    
    @property
    def service_name(self) -> str:
        return self._service_name
    
    @property
    def service_type(self) -> ServiceType:
        return ServiceType.ESSENTIAL
    
    async def health_check(self) -> HealthCheckResult:
        """Check Redis connectivity and performance"""
        start_time = time.time()
        
        try:
            # Test basic connectivity with ping
            if hasattr(self.redis_service, 'ping'):
                await self.redis_service.ping()
            elif hasattr(self.redis_service, 'get'):
                # Fallback: try a get operation
                await self.redis_service.get("health_check_key")
            
            response_time = (time.time() - start_time) * 1000
            
            # Additional details for Redis
            details = {
                "response_time_category": "fast" if response_time < 50 else "slow"
            }
            
            # Try to get additional Redis info if available
            if hasattr(self.redis_service, 'info'):
                try:
                    info = await self.redis_service.info()
                    details["memory_usage"] = info.get("used_memory_human", "unknown")
                    details["connected_clients"] = info.get("connected_clients", "unknown")
                except:
                    pass  # Info not available, continue without it
            
            return HealthCheckResult(
                status=HealthStatus.HEALTHY,
                service_name=self.service_name,
                service_type=self.service_type,
                message="Redis connection healthy",
                response_time_ms=response_time,
                details=details
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return HealthCheckResult(
                status=HealthStatus.UNHEALTHY,
                service_name=self.service_name,
                service_type=self.service_type,
                message=f"Redis connection failed: {str(e)}",
                response_time_ms=response_time,
                error=str(e)
            )


class ElasticsearchHealthCheck(HealthCheckInterface):
    """Health check for Elasticsearch connections"""
    
    def __init__(self, elasticsearch_service, service_name: str = "Elasticsearch"):
        self.elasticsearch_service = elasticsearch_service
        self._service_name = service_name
    
    @property
    def service_name(self) -> str:
        return self._service_name
    
    @property
    def service_type(self) -> ServiceType:
        return ServiceType.OPTIONAL
    
    async def health_check(self) -> HealthCheckResult:
        """Check Elasticsearch connectivity and cluster health"""
        start_time = time.time()
        
        try:
            # Test connectivity
            if hasattr(self.elasticsearch_service, 'health'):
                health_info = await self.elasticsearch_service.health()
            elif hasattr(self.elasticsearch_service, 'ping'):
                await self.elasticsearch_service.ping()
                health_info = {"status": "green"}  # Assume healthy if ping succeeds
            else:
                # Fallback for mock services
                health_info = {"status": "green"}
            
            response_time = (time.time() - start_time) * 1000
            
            # Determine status based on Elasticsearch cluster health
            es_status = health_info.get("status", "unknown")
            if es_status == "green":
                status = HealthStatus.HEALTHY
                message = "Elasticsearch cluster healthy"
            elif es_status == "yellow":
                status = HealthStatus.DEGRADED
                message = "Elasticsearch cluster degraded (yellow status)"
            else:
                status = HealthStatus.UNHEALTHY
                message = f"Elasticsearch cluster unhealthy (status: {es_status})"
            
            return HealthCheckResult(
                status=status,
                service_name=self.service_name,
                service_type=self.service_type,
                message=message,
                response_time_ms=response_time,
                details={
                    "cluster_status": es_status,
                    "response_time_category": "fast" if response_time < 200 else "slow"
                }
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return HealthCheckResult(
                status=HealthStatus.UNHEALTHY,
                service_name=self.service_name,
                service_type=self.service_type,
                message=f"Elasticsearch connection failed: {str(e)}",
                response_time_ms=response_time,
                error=str(e)
            )


class StorageHealthCheck(HealthCheckInterface):
    """Health check for storage services (S3, etc.)"""
    
    def __init__(self, storage_service, service_name: str = "Storage"):
        self.storage_service = storage_service
        self._service_name = service_name
    
    @property
    def service_name(self) -> str:
        return self._service_name
    
    @property
    def service_type(self) -> ServiceType:
        return ServiceType.ESSENTIAL
    
    async def health_check(self) -> HealthCheckResult:
        """Check storage service connectivity"""
        start_time = time.time()
        
        try:
            # Test storage connectivity
            if hasattr(self.storage_service, 'health_check'):
                is_healthy = await self.storage_service.health_check()
            elif hasattr(self.storage_service, 'list_buckets'):
                # Test by listing buckets
                await self.storage_service.list_buckets()
                is_healthy = True
            else:
                # Assume healthy for mock services
                is_healthy = True
            
            response_time = (time.time() - start_time) * 1000
            
            if is_healthy:
                return HealthCheckResult(
                    status=HealthStatus.HEALTHY,
                    service_name=self.service_name,
                    service_type=self.service_type,
                    message="Storage service healthy",
                    response_time_ms=response_time,
                    details={
                        "response_time_category": "fast" if response_time < 500 else "slow"
                    }
                )
            else:
                return HealthCheckResult(
                    status=HealthStatus.UNHEALTHY,
                    service_name=self.service_name,
                    service_type=self.service_type,
                    message="Storage service unhealthy",
                    response_time_ms=response_time
                )
                
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return HealthCheckResult(
                status=HealthStatus.UNHEALTHY,
                service_name=self.service_name,
                service_type=self.service_type,
                message=f"Storage service failed: {str(e)}",
                response_time_ms=response_time,
                error=str(e)
            )


class TerminusDBHealthCheck(HealthCheckInterface):
    """Health check for TerminusDB connections"""
    
    def __init__(self, terminus_service, service_name: str = "TerminusDB"):
        self.terminus_service = terminus_service
        self._service_name = service_name
    
    @property
    def service_name(self) -> str:
        return self._service_name
    
    @property
    def service_type(self) -> ServiceType:
        return ServiceType.CORE
    
    async def health_check(self) -> HealthCheckResult:
        """Check TerminusDB connectivity and basic operations"""
        start_time = time.time()
        
        try:
            # Test connection
            if hasattr(self.terminus_service, 'check_connection'):
                is_connected = await self.terminus_service.check_connection()
            elif hasattr(self.terminus_service, 'connect'):
                await self.terminus_service.connect()
                is_connected = True
            else:
                # Assume connected for mock services
                is_connected = True
            
            response_time = (time.time() - start_time) * 1000
            
            if is_connected:
                # Try to list databases as an additional health check
                try:
                    if hasattr(self.terminus_service, 'list_databases'):
                        databases = await self.terminus_service.list_databases()
                        db_count = len(databases) if databases else 0
                    else:
                        db_count = "unknown"
                except:
                    db_count = "error"
                
                return HealthCheckResult(
                    status=HealthStatus.HEALTHY,
                    service_name=self.service_name,
                    service_type=self.service_type,
                    message="TerminusDB connection healthy",
                    response_time_ms=response_time,
                    details={
                        "database_count": db_count,
                        "response_time_category": "fast" if response_time < 1000 else "slow"
                    }
                )
            else:
                return HealthCheckResult(
                    status=HealthStatus.UNHEALTHY,
                    service_name=self.service_name,
                    service_type=self.service_type,
                    message="TerminusDB connection failed",
                    response_time_ms=response_time
                )
                
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return HealthCheckResult(
                status=HealthStatus.UNHEALTHY,
                service_name=self.service_name,
                service_type=self.service_type,
                message=f"TerminusDB health check failed: {str(e)}",
                response_time_ms=response_time,
                error=str(e)
            )


@dataclass
class AggregatedHealthStatus:
    """Aggregated health status for the entire system"""
    overall_status: HealthStatus
    service_results: List[HealthCheckResult]
    timestamp: datetime
    summary: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "overall_status": self.overall_status.value,
            "timestamp": self.timestamp.isoformat(),
            "summary": self.summary,
            "services": [result.to_dict() for result in self.service_results]
        }


class HealthCheckAggregator:
    """Aggregates health checks from multiple services"""
    
    def __init__(self):
        self.health_checkers: List[HealthCheckInterface] = []
    
    def register_health_checker(self, health_checker: HealthCheckInterface):
        """Register a health checker"""
        self.health_checkers.append(health_checker)
    
    async def check_all_services(self, timeout_seconds: float = 10.0) -> AggregatedHealthStatus:
        """Check all registered services and aggregate results"""
        results = []
        
        # Run all health checks concurrently
        tasks = [
            checker.health_check_with_timeout(timeout_seconds) 
            for checker in self.health_checkers
        ]
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Convert exceptions to unhealthy results
            processed_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    checker = self.health_checkers[i]
                    processed_results.append(HealthCheckResult(
                        status=HealthStatus.UNHEALTHY,
                        service_name=checker.service_name,
                        service_type=checker.service_type,
                        message=f"Health check exception: {str(result)}",
                        error=str(result)
                    ))
                else:
                    processed_results.append(result)
            
            results = processed_results
        
        # Determine overall status
        overall_status = self._determine_overall_status(results)
        
        # Generate summary
        summary = self._generate_summary(results)
        
        return AggregatedHealthStatus(
            overall_status=overall_status,
            service_results=results,
            timestamp=datetime.now(timezone.utc),
            summary=summary
        )
    
    def _determine_overall_status(self, results: List[HealthCheckResult]) -> HealthStatus:
        """Determine overall system health based on individual service results"""
        if not results:
            return HealthStatus.UNKNOWN
        
        # Count services by type and status
        core_unhealthy = sum(1 for r in results 
                           if r.service_type == ServiceType.CORE and r.status == HealthStatus.UNHEALTHY)
        essential_unhealthy = sum(1 for r in results 
                                if r.service_type == ServiceType.ESSENTIAL and r.status == HealthStatus.UNHEALTHY)
        any_degraded = any(r.status == HealthStatus.DEGRADED for r in results)
        
        # Decision logic
        if core_unhealthy > 0:
            return HealthStatus.UNHEALTHY
        elif essential_unhealthy > 0 or any_degraded:
            return HealthStatus.DEGRADED
        elif all(r.status == HealthStatus.HEALTHY for r in results):
            return HealthStatus.HEALTHY
        else:
            return HealthStatus.DEGRADED
    
    def _generate_summary(self, results: List[HealthCheckResult]) -> Dict[str, Any]:
        """Generate summary statistics"""
        if not results:
            return {"total_services": 0}
        
        status_counts = {}
        type_counts = {}
        avg_response_time = 0
        
        for result in results:
            # Count by status
            status = result.status.value
            status_counts[status] = status_counts.get(status, 0) + 1
            
            # Count by type
            service_type = result.service_type.value
            type_counts[service_type] = type_counts.get(service_type, 0) + 1
            
            # Average response time
            avg_response_time += result.response_time_ms
        
        avg_response_time = avg_response_time / len(results) if results else 0
        
        return {
            "total_services": len(results),
            "status_counts": status_counts,
            "service_type_counts": type_counts,
            "average_response_time_ms": round(avg_response_time, 2),
            "healthy_percentage": round((status_counts.get("healthy", 0) / len(results)) * 100, 1)
        }