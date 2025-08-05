"""
Monitoring and Observability Router

This module provides comprehensive monitoring endpoints for the modernized
service architecture, including health checks, metrics, and service status.

Key features:
1. ✅ Comprehensive health check endpoints
2. ✅ Service metrics and performance data
3. ✅ Real-time service status monitoring
4. ✅ Configuration visibility for debugging
5. ✅ Readiness and liveness probes for K8s
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import JSONResponse

from shared.config.settings import ApplicationSettings
from shared.dependencies import get_container, ServiceContainer
from shared.services.health_check import (
    HealthStatus, ServiceType, HealthCheckResult, AggregatedHealthStatus
)
from shared.services.lifecycle_manager import (
    ServiceLifecycleManager, ServiceState, ServiceMetrics
)

router = APIRouter()


# Dependency injection for monitoring endpoints
async def get_settings() -> ApplicationSettings:
    """Get application settings for monitoring"""
    from shared.config.settings import settings
    return settings


async def get_lifecycle_manager() -> Optional[ServiceLifecycleManager]:
    """Get service lifecycle manager if available"""
    try:
        container = await get_container()
        if hasattr(container, 'lifecycle_manager'):
            return container.lifecycle_manager
        return None
    except Exception:
        return None


@router.get("/health", 
           summary="Basic Health Check",
           description="Basic health check endpoint for load balancers")
async def basic_health_check():
    """
    Basic health check endpoint
    
    Returns simple OK status for load balancers and basic monitoring.
    """
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "spice-harvester"
    }


@router.get("/health/detailed",
           summary="Detailed Health Check", 
           description="Comprehensive health check with service details")
async def detailed_health_check(
    include_metrics: bool = Query(False, description="Include performance metrics"),
    lifecycle_manager: Optional[ServiceLifecycleManager] = Depends(get_lifecycle_manager),
    settings: ApplicationSettings = Depends(get_settings)
):
    """
    Detailed health check with comprehensive service information
    
    Provides detailed health status of all services, dependencies,
    and optional performance metrics.
    """
    health_data = {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "environment": settings.environment.value,
        "version": "1.0.0",
        "services": {}
    }
    
    if lifecycle_manager:
        try:
            # Get aggregated health status
            system_health = await lifecycle_manager.get_system_health()
            
            health_data.update({
                "status": system_health.overall_status.value,
                "summary": system_health.summary,
                "services": {
                    result.service_name: {
                        "status": result.status.value,
                        "type": result.service_type.value,
                        "message": result.message,
                        "response_time_ms": result.response_time_ms,
                        "timestamp": result.timestamp.isoformat(),
                        "error": result.error
                    }
                    for result in system_health.service_results
                }
            })
            
            # Add metrics if requested
            if include_metrics:
                service_metrics = lifecycle_manager.get_all_service_metrics()
                for service_name, metrics in service_metrics.items():
                    if service_name in health_data["services"]:
                        health_data["services"][service_name]["metrics"] = {
                            "startup_time_ms": metrics.startup_time_ms,
                            "total_requests": metrics.total_requests,
                            "failed_requests": metrics.failed_requests,
                            "success_rate": metrics.success_rate(),
                            "average_response_time_ms": metrics.average_response_time_ms,
                            "health_check_failures": metrics.health_check_failures
                        }
            
        except Exception as e:
            health_data.update({
                "status": "error",
                "error": str(e),
                "services": {"lifecycle_manager": {"status": "error", "error": str(e)}}
            })
    else:
        health_data.update({
            "status": "degraded",
            "message": "Lifecycle manager not available",
            "services": {"lifecycle_manager": {"status": "not_available"}}
        })
    
    # Set appropriate HTTP status code
    if health_data["status"] == "ok":
        status_code = status.HTTP_200_OK
    elif health_data["status"] == "degraded":
        status_code = status.HTTP_200_OK  # Still operational
    else:
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    
    return JSONResponse(content=health_data, status_code=status_code)


@router.get("/health/readiness",
           summary="Kubernetes Readiness Probe",
           description="Readiness probe for Kubernetes deployments")
async def readiness_probe(
    lifecycle_manager: Optional[ServiceLifecycleManager] = Depends(get_lifecycle_manager)
):
    """
    Kubernetes readiness probe
    
    Returns 200 if the service is ready to receive traffic,
    503 if not ready yet.
    """
    if not lifecycle_manager:
        return JSONResponse(
            content={"ready": False, "reason": "Lifecycle manager not available"},
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE
        )
    
    try:
        # Check if critical services are running
        critical_services_ready = True
        ready_services = []
        not_ready_services = []
        
        for service_name, managed_service in lifecycle_manager.services.items():
            service_ready = managed_service.state == ServiceState.RUNNING
            
            if service_ready:
                ready_services.append(service_name)
            else:
                not_ready_services.append({
                    "name": service_name,
                    "state": managed_service.state.value,
                    "type": managed_service.service.service_type.value
                })
                
                # Critical services must be ready
                if managed_service.service.service_type == ServiceType.CORE:
                    critical_services_ready = False
        
        ready = critical_services_ready and len(ready_services) > 0
        
        response_data = {
            "ready": ready,
            "ready_services": ready_services,
            "not_ready_services": not_ready_services,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        status_code = status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE
        return JSONResponse(content=response_data, status_code=status_code)
        
    except Exception as e:
        return JSONResponse(
            content={"ready": False, "error": str(e)},
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE
        )


@router.get("/health/liveness",
           summary="Kubernetes Liveness Probe", 
           description="Liveness probe for Kubernetes deployments")
async def liveness_probe(
    lifecycle_manager: Optional[ServiceLifecycleManager] = Depends(get_lifecycle_manager)
):
    """
    Kubernetes liveness probe
    
    Returns 200 if the service is alive and should not be restarted,
    503 if the service should be restarted.
    """
    try:
        # Basic liveness check - if we can respond, we're alive
        alive = True
        reason = "Service responsive"
        
        # Additional check: if lifecycle manager is available, check for deadlocks
        if lifecycle_manager:
            # Simple check: can we query service states?
            service_count = len(lifecycle_manager.services)
            if service_count == 0:
                alive = False
                reason = "No services registered"
        
        response_data = {
            "alive": alive,
            "reason": reason,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        status_code = status.HTTP_200_OK if alive else status.HTTP_503_SERVICE_UNAVAILABLE
        return JSONResponse(content=response_data, status_code=status_code)
        
    except Exception as e:
        return JSONResponse(
            content={"alive": False, "error": str(e)},
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE
        )


@router.get("/metrics",
           summary="Service Metrics",
           description="Comprehensive service performance metrics")
async def get_service_metrics(
    service_name: Optional[str] = Query(None, description="Filter by service name"),
    lifecycle_manager: Optional[ServiceLifecycleManager] = Depends(get_lifecycle_manager)
):
    """
    Get comprehensive service metrics
    
    Returns performance metrics, health statistics, and operational data
    for all services or a specific service.
    """
    if not lifecycle_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Lifecycle manager not available"
        )
    
    try:
        if service_name:
            # Get metrics for specific service
            metrics = lifecycle_manager.get_service_metrics(service_name)
            if not metrics:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Service '{service_name}' not found"
                )
            
            return {
                "service": service_name,
                "metrics": {
                    "startup_time_ms": metrics.startup_time_ms,
                    "total_requests": metrics.total_requests,
                    "failed_requests": metrics.failed_requests,
                    "success_rate": metrics.success_rate(),
                    "average_response_time_ms": metrics.average_response_time_ms,
                    "last_health_check": metrics.last_health_check.isoformat() if metrics.last_health_check else None,
                    "health_check_failures": metrics.health_check_failures,
                    "state_changes": metrics.state_changes
                },
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        else:
            # Get metrics for all services
            all_metrics = lifecycle_manager.get_all_service_metrics()
            
            metrics_data = {
                "services": {},
                "summary": {
                    "total_services": len(all_metrics),
                    "average_startup_time_ms": 0,
                    "total_requests": 0,
                    "total_failed_requests": 0,
                    "overall_success_rate": 0
                },
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            # Aggregate metrics
            total_startup_time = 0
            total_requests = 0
            total_failed_requests = 0
            
            for service_name, metrics in all_metrics.items():
                metrics_data["services"][service_name] = {
                    "startup_time_ms": metrics.startup_time_ms,
                    "total_requests": metrics.total_requests,
                    "failed_requests": metrics.failed_requests,
                    "success_rate": metrics.success_rate(),
                    "average_response_time_ms": metrics.average_response_time_ms,
                    "health_check_failures": metrics.health_check_failures
                }
                
                total_startup_time += metrics.startup_time_ms
                total_requests += metrics.total_requests
                total_failed_requests += metrics.failed_requests
            
            # Calculate summary statistics
            if len(all_metrics) > 0:
                metrics_data["summary"]["average_startup_time_ms"] = total_startup_time / len(all_metrics)
            
            metrics_data["summary"]["total_requests"] = total_requests
            metrics_data["summary"]["total_failed_requests"] = total_failed_requests
            
            if total_requests > 0:
                metrics_data["summary"]["overall_success_rate"] = (
                    (total_requests - total_failed_requests) / total_requests
                ) * 100
            else:
                metrics_data["summary"]["overall_success_rate"] = 100.0
            
            return metrics_data
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve metrics: {str(e)}"
        )


@router.get("/status", 
           summary="Service Status Overview",
           description="Current status of all services")
async def get_service_status(
    lifecycle_manager: Optional[ServiceLifecycleManager] = Depends(get_lifecycle_manager)
):
    """
    Get current status of all services
    
    Returns the current state, configuration, and runtime information
    for all managed services.
    """
    if not lifecycle_manager:
        return {
            "status": "degraded",
            "message": "Lifecycle manager not available",
            "services": {},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    try:
        status_data = {
            "status": "ok",
            "services": {},
            "summary": {
                "total_services": len(lifecycle_manager.services),
                "running": 0,
                "degraded": 0,
                "failed": 0,
                "stopped": 0
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        # Get status for each service
        for service_name, managed_service in lifecycle_manager.services.items():
            service_info = {
                "name": service_name,
                "state": managed_service.state.value,
                "type": managed_service.service.service_type.value,
                "priority": managed_service.service.service_priority.value,
                "dependencies": managed_service.service.dependencies,
                "startup_attempts": managed_service.startup_attempts,
                "max_startup_attempts": managed_service.max_startup_attempts,
                "last_error": managed_service.last_error
            }
            
            status_data["services"][service_name] = service_info
            
            # Update summary counts
            if managed_service.state == ServiceState.RUNNING:
                status_data["summary"]["running"] += 1
            elif managed_service.state == ServiceState.DEGRADED:
                status_data["summary"]["degraded"] += 1
            elif managed_service.state == ServiceState.FAILED:
                status_data["summary"]["failed"] += 1
            else:
                status_data["summary"]["stopped"] += 1
        
        # Determine overall status
        if status_data["summary"]["failed"] > 0:
            status_data["status"] = "degraded"
        elif status_data["summary"]["running"] == 0:
            status_data["status"] = "unhealthy"
        
        return status_data
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "services": {},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }


@router.get("/config",
           summary="Configuration Overview",
           description="Current application configuration for debugging")
async def get_configuration_overview(
    include_sensitive: bool = Query(False, description="Include sensitive configuration values"),
    settings: ApplicationSettings = Depends(get_settings)
):
    """
    Get current application configuration
    
    Returns sanitized configuration information for debugging and monitoring.
    Only includes sensitive values if explicitly requested and in development mode.
    """
    config_data = {
        "environment": settings.environment.value,
        "debug": settings.debug,
        "database": {
            "host": settings.database.host,
            "port": settings.database.port,
            "name": settings.database.name,
            "user": settings.database.user,
            # Password is sensitive
        },
        "services": {
            "terminus_url": settings.services.terminus_url,
            "oms_base_url": settings.services.oms_base_url, 
            "bff_base_url": settings.services.bff_base_url,
            "redis_host": settings.services.redis_host,
            "redis_port": settings.services.redis_port,
            "redis_db": settings.services.redis_db,
            "elasticsearch_host": settings.services.elasticsearch_host,
            "elasticsearch_port": settings.services.elasticsearch_port,
            "elasticsearch_username": settings.services.elasticsearch_username,
            # Passwords and keys are sensitive
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    # Include sensitive values only in development mode and if explicitly requested
    if include_sensitive and settings.is_development:
        config_data["database"]["password"] = settings.database.password
        config_data["services"]["terminus_user"] = settings.services.terminus_user
        config_data["services"]["terminus_account"] = settings.services.terminus_account
        config_data["services"]["terminus_key"] = settings.services.terminus_key
        config_data["services"]["elasticsearch_password"] = settings.services.elasticsearch_password
        config_data["_warning"] = "Sensitive values included - development mode only"
    else:
        config_data["_note"] = "Sensitive values masked for security"
    
    return config_data


@router.post("/services/{service_name}/restart",
            summary="Restart Service",
            description="Restart a specific service")
async def restart_service(
    service_name: str,
    lifecycle_manager: Optional[ServiceLifecycleManager] = Depends(get_lifecycle_manager)
):
    """
    Restart a specific service
    
    This endpoint allows manual restart of individual services for
    troubleshooting and maintenance.
    """
    if not lifecycle_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Lifecycle manager not available"
        )
    
    if service_name not in lifecycle_manager.services:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Service '{service_name}' not found"
        )
    
    try:
        success = await lifecycle_manager.restart_service(service_name)
        
        return {
            "service": service_name,
            "restart_successful": success,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message": f"Service {service_name} restart {'successful' if success else 'failed'}"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to restart service {service_name}: {str(e)}"
        )


@router.get("/dependencies",
           summary="Service Dependencies",
           description="Service dependency graph and status")
async def get_service_dependencies(
    lifecycle_manager: Optional[ServiceLifecycleManager] = Depends(get_lifecycle_manager)
):
    """
    Get service dependency information
    
    Returns the service dependency graph showing how services depend
    on each other and their current status.
    """
    if not lifecycle_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Lifecycle manager not available"
        )
    
    try:
        dependency_data = {
            "services": {},
            "dependency_graph": {},
            "initialization_order": [],
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        # Build service information
        for service_name, managed_service in lifecycle_manager.services.items():
            service_info = {
                "name": service_name,
                "type": managed_service.service.service_type.value,
                "priority": managed_service.service.service_priority.value,
                "state": managed_service.state.value,
                "dependencies": managed_service.service.dependencies,
                "dependents": []  # Will be filled below
            }
            
            dependency_data["services"][service_name] = service_info
            dependency_data["dependency_graph"][service_name] = managed_service.service.dependencies
        
        # Find dependents (reverse dependencies)
        for service_name, managed_service in lifecycle_manager.services.items():
            for dep_name in managed_service.service.dependencies:
                if dep_name in dependency_data["services"]:
                    dependency_data["services"][dep_name]["dependents"].append(service_name)
        
        # Get initialization order
        try:
            dependency_data["initialization_order"] = lifecycle_manager._get_initialization_order()
        except Exception as e:
            dependency_data["initialization_order_error"] = str(e)
        
        return dependency_data
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve dependency information: {str(e)}"
        )


@router.get("/background-tasks/metrics",
           summary="Background Task Metrics",
           description="Get metrics for background task execution")
async def get_background_task_metrics(
    container: ServiceContainer = Depends(get_container)
):
    """
    Get background task execution metrics
    
    Returns comprehensive metrics about background task execution,
    including success rates, performance, and current load.
    
    This endpoint addresses Anti-pattern 14 by providing visibility
    into all background tasks running in the system.
    """
    try:
        # Get BackgroundTaskManager from container
        from shared.services.background_task_manager import BackgroundTaskManager
        
        if not container.has(BackgroundTaskManager):
            return {
                "status": "not_available",
                "message": "Background task manager not initialized",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        
        task_manager = await container.get(BackgroundTaskManager)
        metrics = await task_manager.get_task_metrics()
        
        return {
            "status": "ok",
            "metrics": {
                "total_tasks": metrics.total_tasks,
                "active_tasks": metrics.active_tasks,
                "pending_tasks": metrics.pending_tasks,
                "processing_tasks": metrics.processing_tasks,
                "retrying_tasks": metrics.retrying_tasks,
                "completed_tasks": metrics.completed_tasks,
                "failed_tasks": metrics.failed_tasks,
                "success_rate": metrics.success_rate,
                "average_duration_seconds": metrics.average_duration,
                "tasks_by_type": metrics.tasks_by_type
            },
            "health": {
                "status": "healthy" if metrics.success_rate >= 90 else "degraded",
                "issues": []
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        return JSONResponse(
            content={
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@router.get("/background-tasks/active",
           summary="Active Background Tasks",
           description="List all currently active background tasks")
async def get_active_background_tasks(
    limit: int = Query(100, ge=1, le=1000, description="Maximum tasks to return"),
    container: ServiceContainer = Depends(get_container)
):
    """
    Get list of all active background tasks
    
    Returns detailed information about currently running background tasks,
    helping identify potential issues with stuck or long-running tasks.
    """
    try:
        from shared.services.background_task_manager import BackgroundTaskManager
        from shared.models.background_task import TaskStatus
        
        if not container.has(BackgroundTaskManager):
            return {
                "status": "not_available",
                "message": "Background task manager not initialized",
                "tasks": [],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        
        task_manager = await container.get(BackgroundTaskManager)
        
        # Get all active tasks (pending, processing, retrying)
        active_statuses = [TaskStatus.PENDING, TaskStatus.PROCESSING, TaskStatus.RETRYING]
        active_tasks = []
        
        for status in active_statuses:
            tasks = await task_manager.get_all_tasks(status=status, limit=limit)
            active_tasks.extend(tasks)
        
        # Sort by created_at (most recent first)
        active_tasks.sort(key=lambda t: t.created_at, reverse=True)
        active_tasks = active_tasks[:limit]
        
        return {
            "status": "ok",
            "count": len(active_tasks),
            "tasks": [
                {
                    "task_id": task.task_id,
                    "task_name": task.task_name,
                    "task_type": task.task_type,
                    "status": task.status.value,
                    "created_at": task.created_at.isoformat(),
                    "started_at": task.started_at.isoformat() if task.started_at else None,
                    "duration_seconds": task.duration,
                    "progress": task.progress.dict() if task.progress else None,
                    "retry_count": task.retry_count,
                    "metadata": task.metadata
                }
                for task in active_tasks
            ],
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        return JSONResponse(
            content={
                "status": "error",
                "error": str(e),
                "tasks": [],
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@router.get("/background-tasks/health",
           summary="Background Task System Health",
           description="Check health of background task processing system")
async def get_background_task_health(
    container: ServiceContainer = Depends(get_container)
):
    """
    Get health status of background task processing system
    
    Provides health checks and diagnostics for the background task system,
    including identification of potential issues like:
    - High failure rates
    - Stuck tasks
    - Queue backlog
    - Resource exhaustion
    """
    try:
        from shared.services.background_task_manager import BackgroundTaskManager
        
        if not container.has(BackgroundTaskManager):
            return JSONResponse(
                content={
                    "status": "unavailable",
                    "healthy": False,
                    "message": "Background task manager not initialized",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                },
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE
            )
        
        task_manager = await container.get(BackgroundTaskManager)
        metrics = await task_manager.get_task_metrics()
        
        # Health checks
        issues = []
        warnings = []
        healthy = True
        
        # Check success rate
        if metrics.success_rate < 80:
            issues.append(f"Low success rate: {metrics.success_rate:.1f}%")
            healthy = False
        elif metrics.success_rate < 90:
            warnings.append(f"Success rate below optimal: {metrics.success_rate:.1f}%")
        
        # Check for stuck tasks
        if metrics.processing_tasks > 50:
            issues.append(f"High number of processing tasks: {metrics.processing_tasks}")
            healthy = False
        elif metrics.processing_tasks > 20:
            warnings.append(f"Many tasks in processing: {metrics.processing_tasks}")
        
        # Check retry queue
        if metrics.retrying_tasks > 10:
            warnings.append(f"High retry queue: {metrics.retrying_tasks} tasks")
        
        # Check pending queue
        if metrics.pending_tasks > 100:
            warnings.append(f"Large pending queue: {metrics.pending_tasks} tasks")
        
        # Check for dead tasks (tasks stuck in processing for too long)
        dead_tasks = await task_manager._get_dead_tasks()
        if len(dead_tasks) > 0:
            issues.append(f"Found {len(dead_tasks)} potentially dead tasks")
            healthy = False
        
        health_status = "healthy" if healthy and not warnings else "degraded" if healthy else "unhealthy"
        
        response_data = {
            "status": health_status,
            "healthy": healthy,
            "issues": issues,
            "warnings": warnings,
            "metrics_summary": {
                "total_tasks": metrics.total_tasks,
                "active_tasks": metrics.active_tasks,
                "success_rate": metrics.success_rate,
                "average_duration_seconds": metrics.average_duration
            },
            "recommendations": []
        }
        
        # Add recommendations based on issues
        if metrics.success_rate < 80:
            response_data["recommendations"].append(
                "Investigate failing tasks to identify common patterns"
            )
        
        if metrics.processing_tasks > 20:
            response_data["recommendations"].append(
                "Consider scaling up workers or optimizing task processing"
            )
        
        if len(dead_tasks) > 0:
            response_data["recommendations"].append(
                "Run cleanup process to mark dead tasks as failed"
            )
        
        response_data["timestamp"] = datetime.now(timezone.utc).isoformat()
        
        status_code = status.HTTP_200_OK if healthy else status.HTTP_503_SERVICE_UNAVAILABLE
        return JSONResponse(content=response_data, status_code=status_code)
        
    except Exception as e:
        return JSONResponse(
            content={
                "status": "error",
                "healthy": False,
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )