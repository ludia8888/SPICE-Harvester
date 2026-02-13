"""Actions API (BFF).

This module defines HTTP routes only. Business logic lives in
`bff.services.actions_service` (Facade), and request schemas live in
`bff.schemas.actions_requests`.
"""

from shared.observability.tracing import trace_endpoint

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query, Request, status

from bff.dependencies import get_action_log_registry, get_oms_client
from bff.schemas.actions_requests import ActionSimulateRequest, ActionSubmitRequest
from bff.services import actions_service
from bff.services.oms_client import OMSClient
from shared.security.database_access import enforce_database_role
from shared.services.registries.action_log_registry import ActionLogRegistry

router = APIRouter(prefix="/databases/{db_name}/actions", tags=["Actions"])


@router.post(
    "/{action_type_id}/submit",
    status_code=status.HTTP_202_ACCEPTED,
)
@trace_endpoint("bff.actions.submit_action")
async def submit_action(
    db_name: str,
    action_type_id: str,
    request: ActionSubmitRequest,
    http_request: Request,
    base_branch: str = Query("main", description="Base branch (Terminus) for authoritative reads"),
    overlay_branch: Optional[str] = Query(default=None, description="Overlay branch override (ES)"),
    oms_client: OMSClient = Depends(get_oms_client),
) -> Dict[str, Any]:
    return await actions_service.submit_action(
        db_name=db_name,
        action_type_id=action_type_id,
        body=request,
        http_request=http_request,
        base_branch=base_branch,
        overlay_branch=overlay_branch,
        oms_client=oms_client,
        enforce_role=enforce_database_role,
    )


@router.post(
    "/{action_type_id}/simulate",
    status_code=status.HTTP_200_OK,
)
@trace_endpoint("bff.actions.simulate_action")
async def simulate_action(
    db_name: str,
    action_type_id: str,
    request: ActionSimulateRequest,
    http_request: Request,
    oms_client: OMSClient = Depends(get_oms_client),
) -> Dict[str, Any]:
    """
    Action writeback simulation (dry-run) surface.

    Enforces user/database access and forwards intent-only simulation requests to OMS.
    """
    return await actions_service.simulate_action(
        db_name=db_name,
        action_type_id=action_type_id,
        body=request,
        http_request=http_request,
        oms_client=oms_client,
        enforce_role=enforce_database_role,
    )


@router.get("/logs/{action_log_id}")
@trace_endpoint("bff.actions.get_action_log")
async def get_action_log(
    db_name: str,
    action_log_id: str,
    http_request: Request,
    action_logs: ActionLogRegistry = Depends(get_action_log_registry),
) -> Dict[str, Any]:
    return await actions_service.get_action_log(
        db_name=db_name,
        action_log_id=action_log_id,
        http_request=http_request,
        action_logs=action_logs,
        enforce_role=enforce_database_role,
    )


@router.get("/logs")
@trace_endpoint("bff.actions.list_action_logs")
async def list_action_logs(
    db_name: str,
    http_request: Request,
    status_filter: Optional[List[str]] = Query(default=None, alias="status"),
    action_type_id: Optional[str] = Query(default=None),
    submitted_by: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    action_logs: ActionLogRegistry = Depends(get_action_log_registry),
) -> Dict[str, Any]:
    return await actions_service.list_action_logs(
        db_name=db_name,
        http_request=http_request,
        status_filter=status_filter,
        action_type_id=action_type_id,
        submitted_by=submitted_by,
        limit=limit,
        offset=offset,
        action_logs=action_logs,
        enforce_role=enforce_database_role,
    )


@router.get("/simulations")
@trace_endpoint("bff.actions.list_action_simulations")
async def list_action_simulations(
    db_name: str,
    http_request: Request,
    action_type_id: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> Dict[str, Any]:
    return await actions_service.list_action_simulations(
        db_name=db_name,
        http_request=http_request,
        action_type_id=action_type_id,
        limit=limit,
        offset=offset,
        enforce_role=enforce_database_role,
    )


@router.get("/simulations/{simulation_id}")
@trace_endpoint("bff.actions.get_action_simulation")
async def get_action_simulation(
    db_name: str,
    simulation_id: str,
    http_request: Request,
    include_versions: bool = Query(default=True),
    version_limit: int = Query(default=20, ge=1, le=200),
) -> Dict[str, Any]:
    return await actions_service.get_action_simulation(
        db_name=db_name,
        simulation_id=simulation_id,
        http_request=http_request,
        include_versions=include_versions,
        version_limit=version_limit,
        enforce_role=enforce_database_role,
    )


@router.get("/simulations/{simulation_id}/versions")
@trace_endpoint("bff.actions.list_action_simulation_versions")
async def list_action_simulation_versions(
    db_name: str,
    simulation_id: str,
    http_request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> Dict[str, Any]:
    return await actions_service.list_action_simulation_versions(
        db_name=db_name,
        simulation_id=simulation_id,
        http_request=http_request,
        limit=limit,
        offset=offset,
        enforce_role=enforce_database_role,
    )


@router.get("/simulations/{simulation_id}/versions/{version}")
@trace_endpoint("bff.actions.get_action_simulation_version")
async def get_action_simulation_version(
    db_name: str,
    simulation_id: str,
    version: int,
    http_request: Request,
) -> Dict[str, Any]:
    return await actions_service.get_action_simulation_version(
        db_name=db_name,
        simulation_id=simulation_id,
        version=version,
        http_request=http_request,
        enforce_role=enforce_database_role,
    )
