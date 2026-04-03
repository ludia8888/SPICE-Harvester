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

import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from fastapi import APIRouter, Depends, Request, status, Query
from fastapi.responses import JSONResponse
from starlette.responses import RedirectResponse

from shared.config.settings import ApplicationSettings
from shared.dependencies import get_container, ServiceContainer
from shared.dependencies.container import ServiceToken
from shared.dependencies.providers import InitializedBackgroundTaskManagerDep, get_settings_dependency
from shared.config.settings import get_settings as get_settings_ssot
from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode, classified_http_exception
from shared.observability.request_context import get_correlation_id, get_request_id
from shared.services.core.runtime_status import (
    availability_surface,
    build_runtime_issue,
    get_runtime_status,
    normalize_runtime_state,
    normalize_runtime_status,
)
import logging

router = APIRouter(tags=["Monitoring"])

def _resolve_service_name(request: Optional[Request]) -> str:
    if request is not None:
        name = getattr(request.app.state, "service_name", None)
        if name:
            return str(name)
    return get_settings_ssot().observability.service_name_effective


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
                "status": "degraded",
                "checked_via": None,
                "latency_ms": int((time.monotonic() - start) * 1000),
                "probe_status": "unsupported",
            }
    except Exception as e:
        logging.getLogger(__name__).warning("Exception fallback at shared/routers/monitoring.py:69", exc_info=True)
        return {
            "status": "hard_down",
            "checked_via": None,
            "latency_ms": int((time.monotonic() - start) * 1000),
            "error": str(e),
            "probe_status": "failed",
        }

    return {
        "status": "ready" if ok else "hard_down",
        "checked_via": method,
        "latency_ms": int((time.monotonic() - start) * 1000),
        "probe_status": "ok" if ok else "failed",
    }


def _runtime_status_from_request(request: Optional[Request]) -> Optional[Dict[str, Any]]:
    if request is None:
        return None
    return get_runtime_status(request.app, attr_names=("runtime_status", "bff_runtime_status"))


async def _safe_get_container() -> Optional[ServiceContainer]:
    try:
        return await get_container()
    except RuntimeError:
        return None


def _availability_surface(
    *,
    request: Optional[Request],
    container: Optional[ServiceContainer],
    runtime_status: Optional[Dict[str, Any]],
    unhealthy_services: int = 0,
) -> Dict[str, Any]:
    return availability_surface(
        service=_resolve_service_name(request),
        container_ready=bool(container is not None and container.is_initialized),
        runtime_status=runtime_status,
        unhealthy_services=unhealthy_services,
    )


def _background_task_manager_state(issues: list[dict[str, Any]]) -> str:
    states = [
        normalize_runtime_state(issue.get("state"), default="degraded")
        for issue in issues
        if isinstance(issue, dict)
    ]
    if any(state == "hard_down" for state in states):
        return "hard_down"
    if any(state == "degraded" for state in states):
        return "degraded"
    return "ready"


def _background_task_surface(
    request: Request,
    *,
    issues: list[dict[str, Any]],
    status_reason: str,
    message: str,
) -> dict[str, Any]:
    manager_state = _background_task_manager_state(issues)
    return availability_surface(
        service=f"{_resolve_service_name(request)}.background_tasks",
        container_ready=True,
        runtime_status=normalize_runtime_status({"degraded": bool(issues), "issues": issues}),
        dependency_status_overrides={"background_task_manager": manager_state},
        status_reason_override=status_reason,
        message=message,
    )


def _background_task_manager_unavailable_payload(request: Request) -> dict[str, Any]:
    issues = [
        build_runtime_issue(
            component="background_tasks",
            dependency="background_task_manager",
            message="Background task manager not initialized",
            state="hard_down",
            classification="unavailable",
            affected_features=("background_tasks", "background_task_metrics"),
            affects_readiness=True,
        )
    ]
    payload = _background_task_surface(
        request,
        issues=issues,
        status_reason="Background task manager not initialized",
        message="Background task manager not initialized",
    )
    payload["timestamp"] = datetime.now(timezone.utc).isoformat()
    return payload


async def _background_task_health_snapshot(task_manager: Any) -> dict[str, Any]:
    metrics = await task_manager.get_task_metrics()
    dead_tasks = await task_manager._get_dead_tasks()

    issues: list[dict[str, Any]] = []
    warnings: list[str] = []
    recommendations: list[str] = []
    healthy = True

    if metrics.success_rate < 80:
        healthy = False
        issues.append(
            build_runtime_issue(
                component="background_tasks",
                dependency="background_task_manager",
                message=f"Low success rate: {metrics.success_rate:.1f}%",
                state="degraded",
                classification="retryable",
                affected_features=("background_tasks",),
                affects_readiness=False,
            )
        )
        recommendations.append("Investigate failing tasks to identify common patterns")
    elif metrics.success_rate < 90:
        warning = f"Success rate below optimal: {metrics.success_rate:.1f}%"
        warnings.append(warning)
        issues.append(
            build_runtime_issue(
                component="background_tasks",
                dependency="background_task_manager",
                message=warning,
                state="degraded",
                classification="retryable",
                affected_features=("background_tasks",),
                affects_readiness=False,
            )
        )

    if metrics.processing_tasks > 50:
        healthy = False
        issues.append(
            build_runtime_issue(
                component="background_tasks",
                dependency="background_task_manager",
                message=f"High number of processing tasks: {metrics.processing_tasks}",
                state="degraded",
                classification="retryable",
                affected_features=("background_tasks",),
                affects_readiness=False,
            )
        )
        recommendations.append("Consider scaling up workers or optimizing task processing")
    elif metrics.processing_tasks > 20:
        warning = f"Many tasks in processing: {metrics.processing_tasks}"
        warnings.append(warning)
        issues.append(
            build_runtime_issue(
                component="background_tasks",
                dependency="background_task_manager",
                message=warning,
                state="degraded",
                classification="retryable",
                affected_features=("background_tasks",),
                affects_readiness=False,
            )
        )
        recommendations.append("Consider scaling up workers or optimizing task processing")

    if metrics.retrying_tasks > 10:
        warning = f"High retry queue: {metrics.retrying_tasks} tasks"
        warnings.append(warning)
        issues.append(
            build_runtime_issue(
                component="background_tasks",
                dependency="background_task_manager",
                message=warning,
                state="degraded",
                classification="retryable",
                affected_features=("background_tasks",),
                affects_readiness=False,
            )
        )

    if metrics.pending_tasks > 100:
        warning = f"Large pending queue: {metrics.pending_tasks} tasks"
        warnings.append(warning)
        issues.append(
            build_runtime_issue(
                component="background_tasks",
                dependency="background_task_manager",
                message=warning,
                state="degraded",
                classification="retryable",
                affected_features=("background_tasks",),
                affects_readiness=False,
            )
        )

    if len(dead_tasks) > 0:
        healthy = False
        issues.append(
            build_runtime_issue(
                component="background_tasks",
                dependency="background_task_manager",
                message=f"Found {len(dead_tasks)} potentially dead tasks",
                state="degraded",
                classification="retryable",
                affected_features=("background_tasks",),
                affects_readiness=False,
            )
        )
        recommendations.append("Run cleanup process to mark dead tasks as failed")

    return {
        "metrics": metrics,
        "dead_tasks": dead_tasks,
        "issues": issues,
        "warnings": warnings,
        "recommendations": recommendations,
        "healthy": healthy,
    }


def _service_registration_key(key: object) -> str:
    if isinstance(key, str):
        return key
    if isinstance(key, ServiceToken):
        service_name = getattr(key.service_type, "__name__", None)
        if service_name:
            return f"{key.name}:{service_name}"
        return key.name
    if isinstance(key, type):
        return key.__name__
    name = getattr(key, "name", None)
    if isinstance(name, str) and name.strip():
        return name.strip()
    return repr(key)


async def _collect_runtime_service_snapshots(container: ServiceContainer) -> dict[str, Any]:
    services: Dict[str, Any] = {}
    checked = 0
    unhealthy = 0
    instantiated_count = 0

    registrations = getattr(container, "_services", {})  # noqa: SLF001
    for name, registration in registrations.items():
        registration_key = _service_registration_key(name)
        instantiated = getattr(registration, "instance", None) is not None
        if instantiated:
            instantiated_count += 1
        meta = {
            "type": getattr(getattr(registration, "service_type", None), "__name__", None),
            "singleton": bool(getattr(registration, "singleton", True)),
            "instantiated": instantiated,
            "created": instantiated,
            "initialized": bool(getattr(registration, "initialized", False)),
        }

        instance = getattr(registration, "instance", None)
        if instance is None:
            services[registration_key] = {
                **meta,
                "status": "degraded",
                "probe_status": "not_initialized",
            }
            continue

        result = await _check_service_instance(instance)
        checked += 1
        if result.get("status") == "hard_down":
            unhealthy += 1
        services[registration_key] = {**meta, **result}

    return {
        "services": services,
        "checked": checked,
        "unhealthy": unhealthy,
        "registered": len(registrations),
        "instantiated": instantiated_count,
    }


@router.get("/health", 
           summary="Basic Health Check",
           description="Readiness/liveness signal for traffic routing")
async def basic_health_check(request: Request):
    """
    Readiness/liveness health endpoint for routing decisions.

    This endpoint is intentionally lightweight:
    - no per-service lazy initialization
    - no not_initialized noise
    - returns only whether this process should receive traffic
    """
    container = await _safe_get_container()
    runtime_status = _runtime_status_from_request(request)
    payload = _availability_surface(
        request=request,
        container=container,
        runtime_status=runtime_status,
    )
    payload["timestamp"] = datetime.now(timezone.utc).isoformat()
    status_code = status.HTTP_200_OK if payload["ready"] else status.HTTP_503_SERVICE_UNAVAILABLE
    return JSONResponse(
        status_code=status_code,
        content=payload,
    )


@router.get("/health/detailed",
           summary="Detailed Health Check", 
           description="Comprehensive health check with service details")
async def detailed_health_check(
    request: Request,
    include_metrics: bool = Query(False, description="Include performance metrics"),
    settings: ApplicationSettings = Depends(get_settings_dependency),
):
    """
    Detailed health check with comprehensive service information
    
    Provides detailed health status of all services, dependencies,
    and optional performance metrics.
    """
    container = await _safe_get_container()
    runtime_status = _runtime_status_from_request(request)

    if container is None:
        availability = _availability_surface(
            request=request,
            container=container,
            runtime_status=runtime_status,
        )
        health_data: Dict[str, Any] = {
            **availability,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "environment": settings.environment.value,
            "version": "1.0.0",
            "summary": {
                "registered_services": 0,
                "checked_services": 0,
                "unhealthy_services": 0,
            },
            "services": {},
            "note": "Service container is not initialized.",
        }
        if include_metrics:
            health_data["metrics"] = {"container_initialized": False}
        return JSONResponse(content=health_data, status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

    snapshot = await _collect_runtime_service_snapshots(container)
    services = snapshot["services"]
    checked = int(snapshot["checked"])
    unhealthy = int(snapshot["unhealthy"])
    registered = int(snapshot["registered"])

    availability = _availability_surface(
        request=request,
        container=container,
        runtime_status=runtime_status,
        unhealthy_services=unhealthy,
    )
    status_code = status.HTTP_200_OK if availability["ready"] else status.HTTP_503_SERVICE_UNAVAILABLE

    health_data: Dict[str, Any] = {
        **availability,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "environment": settings.environment.value,
        "version": "1.0.0",
        "summary": {
            "registered_services": registered,
            "checked_services": checked,
            "unhealthy_services": unhealthy,
        },
        "services": services,
        "note": "This endpoint checks only initialized services. Uninstantiated services are reported as not_initialized.",
    }

    if include_metrics:
        health_data["metrics"] = {"container_initialized": bool(container.is_initialized)}

    return JSONResponse(content=health_data, status_code=status_code)


@router.get("/health/readiness",
           summary="Kubernetes Readiness Probe",
           description="Readiness probe for Kubernetes deployments")
async def readiness_probe(request: Request):
    """
    Kubernetes readiness probe
    
    Returns 200 if the service is ready to receive traffic,
    503 if not ready yet.
    """
    runtime_status = _runtime_status_from_request(request)
    container = await _safe_get_container()
    payload = _availability_surface(
        request=request,
        container=container,
        runtime_status=runtime_status,
    )
    payload["reason"] = "Ready for traffic" if payload["ready"] else "Not ready for traffic"
    payload["timestamp"] = datetime.now(timezone.utc).isoformat()
    return JSONResponse(
        content=payload,
        status_code=status.HTTP_200_OK if payload["ready"] else status.HTTP_503_SERVICE_UNAVAILABLE,
    )


@router.get("/health/liveness",
           summary="Kubernetes Liveness Probe", 
           description="Liveness probe for Kubernetes deployments")
async def liveness_probe(
    request: Request,
):
    """
    Kubernetes liveness probe
    
    Returns 200 if the service is alive and should not be restarted,
    503 if the service should be restarted.
    """
    try:
        container = await _safe_get_container()
        payload = availability_surface(
            service=_resolve_service_name(request),
            container_ready=True,
            runtime_status={},
        )
        payload.update(
            {
                "alive": True,
                "container_initialized": bool(container is not None and container.is_initialized),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
        return JSONResponse(content=payload, status_code=status.HTTP_200_OK)
    except Exception as e:
        logging.getLogger(__name__).warning("Exception fallback at shared/routers/monitoring.py:213", exc_info=True)
        payload = _availability_surface(
            request=request,
            container=None,
            runtime_status={
                "issues": [
                    {
                        "component": "liveness",
                        "dependency": "service_process",
                        "message": str(e),
                        "state": "hard_down",
                        "classification": "internal",
                        "affects_readiness": True,
                        "affected_features": ["service_process"],
                    }
                ]
            },
        )
        payload.update(
            {
                "alive": False,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
        return JSONResponse(
            content=payload,
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
    headers: Dict[str, str] = {}
    if service_name is not None:
        headers["Deprecation"] = "true"
        headers["Warning"] = (
            '299 - "The \'service_name\' query parameter is deprecated and will be removed in the next release."'
        )
    return RedirectResponse(
        url="/metrics",
        status_code=status.HTTP_307_TEMPORARY_REDIRECT,
        headers=headers or None,
    )


@router.get("/status",
           summary="Service Status Overview",
           description="Current status of all services",
           include_in_schema=False)
async def get_service_status(
    request: Request,
):
    """
    Get current status of all services
    
    Returns the current state, configuration, and runtime information
    for all managed services.
    """
    container = await _safe_get_container()
    runtime_status = _runtime_status_from_request(request)
    if container is None:
        payload = _availability_surface(
            request=request,
            container=container,
            runtime_status=runtime_status,
        )
        payload.update(
            {
                "container_initialized": False,
                "summary": {
                    "registered_services": 0,
                    "instantiated_services": 0,
                    "created_services": 0,
                },
                "services": {},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
        return JSONResponse(
            content=payload,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    snapshot = await _collect_runtime_service_snapshots(container)
    service_info = snapshot["services"]
    instantiated = int(snapshot["instantiated"])
    unhealthy = int(snapshot["unhealthy"])
    registered = int(snapshot["registered"])
    payload = _availability_surface(
        request=request,
        container=container,
        runtime_status=runtime_status,
        unhealthy_services=unhealthy,
    )
    payload.update(
        {
            "container_initialized": bool(container.is_initialized),
            "summary": {
                "registered_services": registered,
                "instantiated_services": instantiated,
                "created_services": instantiated,
                "unhealthy_services": unhealthy,
            },
            "services": service_info,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )

    return JSONResponse(
        content=payload,
        status_code=status.HTTP_200_OK if payload["ready"] else status.HTTP_503_SERVICE_UNAVAILABLE,
    )


@router.get("/config",
           summary="Configuration Overview",
           description="Current application configuration for debugging")
async def get_configuration_overview(
    include_sensitive: bool = Query(False, description="Include sensitive configuration values"),
    settings: ApplicationSettings = Depends(get_settings_dependency)
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
            "kafka": {
                "servers": settings.database.kafka_servers,
            },
        },
        "services": {
            "oms_base_url": settings.services.oms_base_url,
            "bff_base_url": settings.services.bff_base_url,
            "agent_base_url": settings.services.agent_base_url,
            "funnel_runtime": "internal",
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
    raise classified_http_exception(
        status.HTTP_501_NOT_IMPLEMENTED,
        "Service restart is not supported via API in this deployment.",
        code=ErrorCode.FEATURE_NOT_IMPLEMENTED,
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
    raise classified_http_exception(
        status.HTTP_501_NOT_IMPLEMENTED,
        "Dependency graph is not tracked by the ServiceContainer. Use /health/detailed and /status instead.",
        code=ErrorCode.FEATURE_NOT_IMPLEMENTED,
    )


@router.get("/background-tasks/metrics",
           summary="Background Task Metrics",
           description="Get metrics for background task execution",
           include_in_schema=False)
async def get_background_task_metrics(
    request: Request,
    task_manager: InitializedBackgroundTaskManagerDep,
):
    """
    Get background task execution metrics
    
    Returns comprehensive metrics about background task execution,
    including success rates, performance, and current load.
    
    This endpoint addresses Anti-pattern 14 by providing visibility
    into all background tasks running in the system.
    """
    try:
        if task_manager is None:
            return JSONResponse(
                content=_background_task_manager_unavailable_payload(request),
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        snapshot = await _background_task_health_snapshot(task_manager)
        metrics = snapshot["metrics"]
        surface = _background_task_surface(
            request,
            issues=list(snapshot["issues"]),
            status_reason=(
                "Background task metrics within thresholds"
                if not snapshot["issues"]
                else "Background task metrics exceeded thresholds"
            ),
            message=(
                "Background task metrics within thresholds"
                if not snapshot["issues"]
                else "Background task metrics exceeded thresholds"
            ),
        )

        return {
            **surface,
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
                "status": surface["status"],
                "issues": [issue["message"] for issue in snapshot["issues"]],
                "warnings": list(snapshot["warnings"]),
                "recommendations": list(snapshot["recommendations"]),
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logging.getLogger(__name__).warning("Exception fallback at shared/routers/monitoring.py:431", exc_info=True)
        payload = build_error_envelope(
            service_name=_resolve_service_name(request),
            message="Failed to load background task metrics",
            detail=str(e),
            code=ErrorCode.INTERNAL_ERROR,
            category=ErrorCategory.INTERNAL,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            request_id=get_request_id(),
            correlation_id=get_correlation_id(),
        )
        return JSONResponse(
            content=payload,
            status_code=payload.get("http_status", status.HTTP_500_INTERNAL_SERVER_ERROR),
        )


@router.get("/background-tasks/active",
           summary="Active Background Tasks",
           description="List all currently active background tasks",
           include_in_schema=False)
async def get_active_background_tasks(
    request: Request,
    limit: int = Query(100, ge=1, le=1000, description="Maximum tasks to return"),
    *,
    task_manager: InitializedBackgroundTaskManagerDep,
):
    """
    Get list of all active background tasks
    
    Returns detailed information about currently running background tasks,
    helping identify potential issues with stuck or long-running tasks.
    """
    try:
        from shared.models.background_task import TaskStatus

        if task_manager is None:
            payload = _background_task_manager_unavailable_payload(request)
            payload["tasks"] = []
            payload["count"] = 0
            return JSONResponse(content=payload, status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

        # Get all active tasks (pending, processing, retrying)
        active_statuses = [TaskStatus.PENDING, TaskStatus.PROCESSING, TaskStatus.RETRYING]
        active_tasks = []

        for status in active_statuses:
            tasks = await task_manager.get_all_tasks(status=status, limit=limit)
            active_tasks.extend(tasks)

        # Sort by created_at (most recent first)
        active_tasks.sort(key=lambda t: t.created_at, reverse=True)
        active_tasks = active_tasks[:limit]
        surface = _background_task_surface(
            request,
            issues=[],
            status_reason="Background task manager operational",
            message="Background task manager operational",
        )

        return {
            **surface,
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
        logging.getLogger(__name__).warning("Exception fallback at shared/routers/monitoring.py:507", exc_info=True)
        payload = build_error_envelope(
            service_name=_resolve_service_name(request),
            message="Failed to list active background tasks",
            detail=str(e),
            code=ErrorCode.INTERNAL_ERROR,
            category=ErrorCategory.INTERNAL,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            request_id=get_request_id(),
            correlation_id=get_correlation_id(),
            context={"tasks": []},
        )
        return JSONResponse(
            content=payload,
            status_code=payload.get("http_status", status.HTTP_500_INTERNAL_SERVER_ERROR),
        )


@router.get("/background-tasks/health",
           summary="Background Task System Health",
           description="Check health of background task processing system")
async def get_background_task_health(
    request: Request,
    task_manager: InitializedBackgroundTaskManagerDep,
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
        if task_manager is None:
            return JSONResponse(
                content=_background_task_manager_unavailable_payload(request),
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        snapshot = await _background_task_health_snapshot(task_manager)
        metrics = snapshot["metrics"]
        surface = _background_task_surface(
            request,
            issues=list(snapshot["issues"]),
            status_reason=(
                "Background task metrics within thresholds"
                if not snapshot["issues"]
                else "Background task metrics exceeded thresholds"
            ),
            message=(
                "Background task metrics within thresholds"
                if not snapshot["issues"]
                else "Background task metrics exceeded thresholds"
            ),
        )

        response_data = {
            **surface,
            "healthy": bool(snapshot["healthy"]),
            "issues": [issue["message"] for issue in snapshot["issues"]],
            "warnings": list(snapshot["warnings"]),
            "metrics_summary": {
                "total_tasks": metrics.total_tasks,
                "active_tasks": metrics.active_tasks,
                "success_rate": metrics.success_rate,
                "average_duration_seconds": metrics.average_duration
            },
            "recommendations": list(snapshot["recommendations"]),
        }

        response_data["timestamp"] = datetime.now(timezone.utc).isoformat()

        status_code = status.HTTP_200_OK if snapshot["healthy"] else status.HTTP_503_SERVICE_UNAVAILABLE
        return JSONResponse(content=response_data, status_code=status_code)
        
    except Exception as e:
        logging.getLogger(__name__).warning("Exception fallback at shared/routers/monitoring.py:625", exc_info=True)
        payload = build_error_envelope(
            service_name=_resolve_service_name(request),
            message="Failed to evaluate background task health",
            detail=str(e),
            code=ErrorCode.INTERNAL_ERROR,
            category=ErrorCategory.INTERNAL,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            request_id=get_request_id(),
            correlation_id=get_correlation_id(),
            context={"healthy": False},
        )
        return JSONResponse(
            content=payload,
            status_code=payload.get("http_status", status.HTTP_500_INTERNAL_SERVER_ERROR),
        )
