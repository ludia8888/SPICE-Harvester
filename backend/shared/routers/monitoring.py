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
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import JSONResponse
from starlette.responses import RedirectResponse

from shared.config.settings import ApplicationSettings
from shared.dependencies import get_container, ServiceContainer

router = APIRouter(tags=["Monitoring"])


# Dependency injection for monitoring endpoints
async def get_settings() -> ApplicationSettings:
    """Get application settings for monitoring"""
    from shared.config.settings import settings
    return settings


async def _check_service_instance(instance: Any) -> Dict[str, Any]:
    """
    Best-effort, runtime-validated service health check.

    We intentionally avoid cargo-cult "always healthy" defaults. If a service exposes
    `health_check()` / `ping()` / `check_connection()` we execute it and record latency.
    """

    start = time.monotonic()
    try:
        if hasattr(instance, "health_check"):
            result = await instance.health_check()
            ok = bool(getattr(result, "status", result)) if not isinstance(result, bool) else result
            ok = bool(ok)
            method = "health_check"
        elif hasattr(instance, "ping"):
            ok = bool(await instance.ping())
            method = "ping"
        elif hasattr(instance, "check_connection"):
            ok = bool(await instance.check_connection())
            method = "check_connection"
        else:
            return {
                "status": "unknown",
                "checked_via": None,
                "latency_ms": int((time.monotonic() - start) * 1000),
            }
    except Exception as e:
        return {
            "status": "unhealthy",
            "checked_via": None,
            "latency_ms": int((time.monotonic() - start) * 1000),
            "error": str(e),
        }

    return {
        "status": "healthy" if ok else "unhealthy",
        "checked_via": method,
        "latency_ms": int((time.monotonic() - start) * 1000),
    }


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
    settings: ApplicationSettings = Depends(get_settings),
    container: ServiceContainer = Depends(get_container),
):
    """
    Detailed health check with comprehensive service information
    
    Provides detailed health status of all services, dependencies,
    and optional performance metrics.
    """
    # Validate real runtime wiring: only services that are actually initialized can be checked.
    # This avoids the old cargo-cult lifecycle manager dependency that was never wired.
    services: Dict[str, Any] = {}
    checked = 0
    unhealthy = 0

    # Container internals are used intentionally (monitoring-only) so we can access created instances.
    registrations = getattr(container, "_services", {})  # noqa: SLF001
    for name, registration in registrations.items():
        meta = {
            "type": getattr(getattr(registration, "service_type", None), "__name__", None),
            "singleton": bool(getattr(registration, "singleton", True)),
            "created": getattr(registration, "instance", None) is not None,
            "initialized": bool(getattr(registration, "initialized", False)),
        }

        instance = getattr(registration, "instance", None)
        if instance is None:
            services[name] = {**meta, "status": "not_initialized"}
            continue

        result = await _check_service_instance(instance)
        checked += 1
        if result.get("status") != "healthy":
            unhealthy += 1
        services[name] = {**meta, **result}

    overall = "ok" if unhealthy == 0 else "degraded"
    status_code = status.HTTP_200_OK

    health_data: Dict[str, Any] = {
        "status": overall,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "environment": settings.environment.value,
        "version": "1.0.0",
        "summary": {
            "registered_services": len(registrations),
            "checked_services": checked,
            "unhealthy_services": unhealthy,
        },
        "services": services,
        "note": "This endpoint checks only initialized services. Uninitialized services are reported as not_initialized.",
    }

    if include_metrics:
        health_data["metrics"] = {"container_initialized": bool(container.is_initialized)}

    return JSONResponse(content=health_data, status_code=status_code)


@router.get("/health/readiness",
           summary="Kubernetes Readiness Probe",
           description="Readiness probe for Kubernetes deployments")
async def readiness_probe(
    container: ServiceContainer = Depends(get_container)
):
    """
    Kubernetes readiness probe
    
    Returns 200 if the service is ready to receive traffic,
    503 if not ready yet.
    """
    return JSONResponse(
        content={
            "ready": bool(container.is_initialized),
            "reason": "Container initialized",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
        status_code=status.HTTP_200_OK if container.is_initialized else status.HTTP_503_SERVICE_UNAVAILABLE,
    )


@router.get("/health/liveness",
           summary="Kubernetes Liveness Probe", 
           description="Liveness probe for Kubernetes deployments")
async def liveness_probe(
    container: ServiceContainer = Depends(get_container)
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
        
        response_data = {
            "alive": alive,
            "reason": reason,
            "container_initialized": bool(container.is_initialized),
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
    service_name: Optional[str] = Query(None, description="(deprecated) Filter by service name"),
):
    """
    Get comprehensive service metrics
    
    Returns performance metrics, health statistics, and operational data
    for all services or a specific service.
    """
    # Deprecated endpoint: the canonical Prometheus scrape target is `/metrics`.
    # Keep this endpoint as a redirect so dashboards/operators don't 404.
    _ = service_name  # explicitly unused
    return RedirectResponse(url="/metrics", status_code=status.HTTP_307_TEMPORARY_REDIRECT)


@router.get("/status", 
           summary="Service Status Overview",
           description="Current status of all services")
async def get_service_status(
    container: ServiceContainer = Depends(get_container)
):
    """
    Get current status of all services
    
    Returns the current state, configuration, and runtime information
    for all managed services.
    """
    service_info = container.get_service_info()
    created = sum(1 for meta in service_info.values() if meta.get("created"))

    return {
        "status": "ok" if container.is_initialized else "unhealthy",
        "container_initialized": bool(container.is_initialized),
        "summary": {
            "registered_services": len(service_info),
            "created_services": created,
        },
        "services": service_info,
        "timestamp": datetime.now(timezone.utc).isoformat(),
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
    config_data: Dict[str, Any] = {
        "environment": settings.environment.value,
        "debug": settings.debug,
        "database": {
            "postgres": {
                "host": settings.database.postgres_host,
                "port": settings.database.postgres_port,
                "db": settings.database.postgres_db,
                "user": settings.database.postgres_user,
            },
            "redis": {
                "host": settings.database.redis_host,
                "port": settings.database.redis_port,
                "password_configured": bool(settings.database.redis_password),
            },
            "elasticsearch": {
                "host": settings.database.elasticsearch_host,
                "port": settings.database.elasticsearch_port,
                "username": settings.database.elasticsearch_username,
            },
            "terminusdb": {
                "url": settings.database.terminus_url,
                "user": settings.database.terminus_user,
                "account": settings.database.terminus_account,
            },
            "kafka": {
                "servers": settings.database.kafka_servers,
            },
        },
        "services": {
            "oms_base_url": settings.services.oms_base_url,
            "bff_base_url": settings.services.bff_base_url,
            "funnel_base_url": settings.services.funnel_base_url,
        },
        "storage": {
            "minio_endpoint_url": settings.storage.minio_endpoint_url,
            "instance_bucket": settings.storage.instance_bucket,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    
    # Include secrets only in development mode and if explicitly requested.
    if include_sensitive and settings.is_development:
        config_data["database"]["postgres"]["password"] = settings.database.postgres_password
        config_data["database"]["redis"]["password"] = settings.database.redis_password
        config_data["database"]["elasticsearch"]["password"] = settings.database.elasticsearch_password
        config_data["database"]["terminusdb"]["password"] = settings.database.terminus_password
        config_data["storage"]["minio_access_key"] = settings.storage.minio_access_key
        config_data["storage"]["minio_secret_key"] = settings.storage.minio_secret_key
        config_data["_warning"] = "Sensitive values included (development mode only)"
    else:
        config_data["_note"] = "Sensitive values are masked by default"
    
    return config_data


@router.post("/services/{service_name}/restart",
            summary="Restart Service",
            description="Restart a specific service",
            include_in_schema=False)
async def restart_service(
    service_name: str,
    _: ServiceContainer = Depends(get_container),
):
    """
    Restart a specific service
    
    This endpoint allows manual restart of individual services for
    troubleshooting and maintenance.
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Service restart is not supported via API in this deployment.",
    )


@router.get("/dependencies",
           summary="Service Dependencies",
           description="Service dependency graph and status",
           include_in_schema=False)
async def get_service_dependencies(
    _: ServiceContainer = Depends(get_container)
):
    """
    Get service dependency information
    
    Returns the service dependency graph showing how services depend
    on each other and their current status.
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Dependency graph is not tracked by the ServiceContainer. Use /health/detailed and /status instead.",
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
                    "progress": task.progress.model_dump(mode="json") if task.progress else None,
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
