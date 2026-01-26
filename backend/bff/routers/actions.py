"""
Actions API (BFF).

This is the user-facing submission surface for Action-only writeback.
It enforces user/database access and forwards intent-only submissions to OMS.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field, field_validator

from bff.dependencies import get_action_log_registry, get_oms_client
from bff.services.oms_client import OMSClient
from shared.security.database_access import DOMAIN_MODEL_ROLES, enforce_database_role, resolve_database_actor
from shared.security.input_sanitizer import SecurityViolationError, sanitize_input, validate_db_name
from shared.services.registries.action_log_registry import ActionLogRecord, ActionLogRegistry

from shared.services.registries.action_simulation_registry import ActionSimulationRegistry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databases/{db_name}/actions", tags=["Actions"])


def _raise_httpx_as_http_exception(exc: httpx.HTTPStatusError) -> None:
    resp = getattr(exc, "response", None)
    if resp is None:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS 요청 실패") from exc

    try:
        payload = resp.json()
    except Exception:
        payload = (resp.text or "").strip() or f"OMS returned HTTP {resp.status_code}"

    detail = payload.get("detail") if isinstance(payload, dict) and "detail" in payload else payload
    raise HTTPException(status_code=int(resp.status_code), detail=detail) from exc


class ActionSubmitRequest(BaseModel):
    input: Dict[str, Any] = Field(default_factory=dict, description="Intent-only action input payload")
    correlation_id: Optional[str] = Field(default=None, description="Correlation id for trace/audit")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Optional metadata")


class ActionSimulateScenarioRequest(BaseModel):
    scenario_id: Optional[str] = Field(default=None, description="Optional client-provided scenario identifier")
    conflict_policy: Optional[str] = Field(
        default=None,
        description="Optional conflict_policy override for this scenario (WRITEBACK_WINS|BASE_WINS|FAIL|MANUAL_REVIEW)",
    )


class ActionSimulateStatePatch(BaseModel):
    """Patch-like state override for decision simulation (what-if)."""

    set: Dict[str, Any] = Field(default_factory=dict)
    unset: List[str] = Field(default_factory=list)
    link_add: List[Any] = Field(default_factory=list)
    link_remove: List[Any] = Field(default_factory=list)
    delete: bool = Field(default=False, description="delete is not supported for simulation assumptions")

    @field_validator("delete")
    @classmethod
    def _reject_delete(cls, value: bool) -> bool:
        if value:
            raise ValueError("delete is not supported for simulation assumptions")
        return False


class ActionSimulateObservedBaseOverrides(BaseModel):
    """Override observed_base snapshot fields/links to simulate stale reads."""

    fields: Dict[str, Any] = Field(default_factory=dict)
    links: Dict[str, Any] = Field(default_factory=dict)


class ActionSimulateTargetAssumption(BaseModel):
    class_id: str
    instance_id: str
    base_overrides: Optional[ActionSimulateStatePatch] = None
    observed_base_overrides: Optional[ActionSimulateObservedBaseOverrides] = None


class ActionSimulateAssumptions(BaseModel):
    targets: List[ActionSimulateTargetAssumption] = Field(
        default_factory=list,
        description="Per-target state injections (Level 2 what-if base state assumptions).",
    )


class ActionSimulateRequest(BaseModel):
    input: Dict[str, Any] = Field(default_factory=dict, description="Intent-only action input payload")
    correlation_id: Optional[str] = Field(default=None, description="Correlation id for trace/audit")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Optional metadata")
    base_branch: str = Field("main", description="Base branch for authoritative reads (default: main)")
    overlay_branch: Optional[str] = Field(
        default=None,
        description="Optional overlay branch override (default derived from writeback_target)",
    )
    simulation_id: Optional[str] = Field(default=None, description="Optional simulation id for versioned reruns")
    title: Optional[str] = Field(default=None, max_length=200)
    description: Optional[str] = Field(default=None, max_length=2000)
    scenarios: Optional[List[ActionSimulateScenarioRequest]] = Field(
        default=None,
        description="Optional scenario list (policy comparisons). If omitted, server simulates the effective policy only.",
    )
    include_effects: bool = Field(default=True, description="If true, compute downstream lakeFS/ES overlay effects")
    assumptions: Optional[ActionSimulateAssumptions] = Field(
        default=None,
        description="Optional decision simulation assumptions (Level 2 state injection).",
    )


def _parse_uuid(value: str) -> UUID:
    try:
        return UUID(str(value))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid UUID") from exc


def _dt_iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    try:
        return value.isoformat()
    except Exception:
        return str(value)


def _serialize_action_log(record: ActionLogRecord) -> Dict[str, Any]:
    return {
        "class_id": "ActionLog",
        "instance_id": record.action_log_id,
        "rid": f"action_log:{record.action_log_id}",
        "action_log_id": record.action_log_id,
        "db_name": record.db_name,
        "action_type_id": record.action_type_id,
        "action_type_rid": record.action_type_rid,
        "resource_rid": record.resource_rid,
        "ontology_commit_id": record.ontology_commit_id,
        "input": record.input,
        "status": record.status,
        "result": record.result,
        "correlation_id": record.correlation_id,
        "submitted_by": record.submitted_by,
        "submitted_at": _dt_iso(record.submitted_at),
        "finished_at": _dt_iso(record.finished_at),
        "writeback_target": record.writeback_target,
        "writeback_commit_id": record.writeback_commit_id,
        "action_applied_event_id": record.action_applied_event_id,
        "action_applied_seq": record.action_applied_seq,
        "metadata": record.metadata,
        "updated_at": _dt_iso(record.updated_at),
    }


def _serialize_action_simulation(record: Any) -> Dict[str, Any]:
    return {
        "simulation_id": getattr(record, "simulation_id", None),
        "db_name": getattr(record, "db_name", None),
        "action_type_id": getattr(record, "action_type_id", None),
        "title": getattr(record, "title", None),
        "description": getattr(record, "description", None),
        "created_by": getattr(record, "created_by", None),
        "created_by_type": getattr(record, "created_by_type", None),
        "created_at": _dt_iso(getattr(record, "created_at", None)),
        "updated_at": _dt_iso(getattr(record, "updated_at", None)),
    }


def _serialize_action_simulation_version(record: Any) -> Dict[str, Any]:
    return {
        "simulation_id": getattr(record, "simulation_id", None),
        "version": getattr(record, "version", None),
        "status": getattr(record, "status", None),
        "base_branch": getattr(record, "base_branch", None),
        "overlay_branch": getattr(record, "overlay_branch", None),
        "ontology_commit_id": getattr(record, "ontology_commit_id", None),
        "action_type_rid": getattr(record, "action_type_rid", None),
        "preview_action_log_id": getattr(record, "preview_action_log_id", None),
        "input": getattr(record, "input", None),
        "assumptions": getattr(record, "assumptions", None),
        "scenarios": getattr(record, "scenarios", None),
        "result": getattr(record, "result", None),
        "error": getattr(record, "error", None),
        "created_by": getattr(record, "created_by", None),
        "created_by_type": getattr(record, "created_by_type", None),
        "created_at": _dt_iso(getattr(record, "created_at", None)),
    }


@router.post(
    "/{action_type_id}/submit",
    status_code=status.HTTP_202_ACCEPTED,
)
async def submit_action(
    db_name: str,
    action_type_id: str,
    request: ActionSubmitRequest,
    http_request: Request,
    base_branch: str = Query("main", description="Base branch (Terminus) for authoritative reads"),
    overlay_branch: Optional[str] = Query(default=None, description="Overlay branch override (ES)"),
    oms_client: OMSClient = Depends(get_oms_client),
) -> Dict[str, Any]:
    try:
        db_name = validate_db_name(db_name)
        action_type_id = str(action_type_id or "").strip()
        if not action_type_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="action_type_id is required")

        # Permission gate (minimum governance baseline): domain model roles for operational writeback.
        try:
            await enforce_database_role(headers=http_request.headers, db_name=db_name, required_roles=DOMAIN_MODEL_ROLES)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

        principal_type, principal_id = resolve_database_actor(http_request.headers)
        sanitized_input = sanitize_input(request.input)

        oms_payload = {
            "input": sanitized_input,
            "correlation_id": request.correlation_id,
            "metadata": {
                **(request.metadata or {}),
                "user_id": principal_id,
                "user_type": principal_type,
            },
            "base_branch": base_branch,
            "overlay_branch": overlay_branch,
        }

        return await oms_client.post(
            f"/api/v1/actions/{db_name}/async/{action_type_id}/submit",
            json=oms_payload,
        )

    except HTTPException:
        raise
    except httpx.HTTPStatusError as e:
        _raise_httpx_as_http_exception(e)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS 요청 실패") from e
    except SecurityViolationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e


@router.post(
    "/{action_type_id}/simulate",
    status_code=status.HTTP_200_OK,
)
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
    try:
        db_name = validate_db_name(db_name)
        action_type_id = str(action_type_id or "").strip()
        if not action_type_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="action_type_id is required")

        try:
            await enforce_database_role(headers=http_request.headers, db_name=db_name, required_roles=DOMAIN_MODEL_ROLES)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

        principal_type, principal_id = resolve_database_actor(http_request.headers)
        sanitized_input = sanitize_input(request.input)

        oms_payload = {
            "input": sanitized_input,
            "correlation_id": request.correlation_id,
            "metadata": {
                **(request.metadata or {}),
                "user_id": principal_id,
                "user_type": principal_type,
            },
            "base_branch": request.base_branch,
            "overlay_branch": request.overlay_branch,
            "simulation_id": request.simulation_id,
            "title": request.title,
            "description": request.description,
            "scenarios": [s.model_dump(exclude_none=True) for s in (request.scenarios or [])],
            "include_effects": bool(request.include_effects),
        }
        if request.assumptions is not None:
            oms_payload["assumptions"] = request.assumptions.model_dump(exclude_none=True)

        return await oms_client.post(
            f"/api/v1/actions/{db_name}/async/{action_type_id}/simulate",
            json=oms_payload,
        )
    except HTTPException:
        raise
    except httpx.HTTPStatusError as e:
        _raise_httpx_as_http_exception(e)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS 요청 실패") from e
    except SecurityViolationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e


@router.get("/logs/{action_log_id}")
async def get_action_log(
    db_name: str,
    action_log_id: str,
    http_request: Request,
    action_logs: ActionLogRegistry = Depends(get_action_log_registry),
) -> Dict[str, Any]:
    db_name = validate_db_name(db_name)
    action_log_uuid = _parse_uuid(action_log_id)

    try:
        await enforce_database_role(headers=http_request.headers, db_name=db_name, required_roles=DOMAIN_MODEL_ROLES)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    record = await action_logs.get_log(action_log_id=str(action_log_uuid))
    if not record or record.db_name != db_name:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="ActionLog not found")

    return {"status": "success", "data": _serialize_action_log(record)}


@router.get("/logs")
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
    db_name = validate_db_name(db_name)

    try:
        await enforce_database_role(headers=http_request.headers, db_name=db_name, required_roles=DOMAIN_MODEL_ROLES)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    records = await action_logs.list_logs(
        db_name=db_name,
        statuses=status_filter,
        action_type_id=action_type_id,
        submitted_by=submitted_by,
        limit=limit,
        offset=offset,
    )
    return {
        "status": "success",
        "count": len(records),
        "data": [_serialize_action_log(rec) for rec in records],
    }


@router.get("/simulations")
async def list_action_simulations(
    db_name: str,
    http_request: Request,
    action_type_id: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> Dict[str, Any]:
    db_name = validate_db_name(db_name)
    try:
        await enforce_database_role(headers=http_request.headers, db_name=db_name, required_roles=DOMAIN_MODEL_ROLES)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    registry = ActionSimulationRegistry()
    await registry.connect()
    try:
        sims = await registry.list_simulations(db_name=db_name, action_type_id=action_type_id, limit=limit, offset=offset)
        return {
            "status": "success",
            "count": len(sims),
            "data": [_serialize_action_simulation(sim) for sim in sims],
        }
    finally:
        await registry.close()


@router.get("/simulations/{simulation_id}")
async def get_action_simulation(
    db_name: str,
    simulation_id: str,
    http_request: Request,
    include_versions: bool = Query(default=True),
    version_limit: int = Query(default=20, ge=1, le=200),
) -> Dict[str, Any]:
    db_name = validate_db_name(db_name)
    simulation_uuid = _parse_uuid(simulation_id)

    try:
        await enforce_database_role(headers=http_request.headers, db_name=db_name, required_roles=DOMAIN_MODEL_ROLES)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    registry = ActionSimulationRegistry()
    await registry.connect()
    try:
        sim = await registry.get_simulation(simulation_id=str(simulation_uuid))
        if not sim or sim.db_name != db_name:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="ActionSimulation not found")

        payload: Dict[str, Any] = {"simulation": _serialize_action_simulation(sim)}
        if include_versions:
            versions = await registry.list_versions(simulation_id=str(simulation_uuid), limit=version_limit, offset=0)
            payload["versions"] = [_serialize_action_simulation_version(v) for v in versions]
        return {"status": "success", "data": payload}
    finally:
        await registry.close()


@router.get("/simulations/{simulation_id}/versions")
async def list_action_simulation_versions(
    db_name: str,
    simulation_id: str,
    http_request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> Dict[str, Any]:
    db_name = validate_db_name(db_name)
    simulation_uuid = _parse_uuid(simulation_id)

    try:
        await enforce_database_role(headers=http_request.headers, db_name=db_name, required_roles=DOMAIN_MODEL_ROLES)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    registry = ActionSimulationRegistry()
    await registry.connect()
    try:
        sim = await registry.get_simulation(simulation_id=str(simulation_uuid))
        if not sim or sim.db_name != db_name:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="ActionSimulation not found")
        versions = await registry.list_versions(simulation_id=str(simulation_uuid), limit=limit, offset=offset)
        return {
            "status": "success",
            "count": len(versions),
            "data": [_serialize_action_simulation_version(v) for v in versions],
        }
    finally:
        await registry.close()


@router.get("/simulations/{simulation_id}/versions/{version}")
async def get_action_simulation_version(
    db_name: str,
    simulation_id: str,
    version: int,
    http_request: Request,
) -> Dict[str, Any]:
    db_name = validate_db_name(db_name)
    simulation_uuid = _parse_uuid(simulation_id)
    try:
        await enforce_database_role(headers=http_request.headers, db_name=db_name, required_roles=DOMAIN_MODEL_ROLES)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    registry = ActionSimulationRegistry()
    await registry.connect()
    try:
        sim = await registry.get_simulation(simulation_id=str(simulation_uuid))
        if not sim or sim.db_name != db_name:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="ActionSimulation not found")
        ver = await registry.get_version(simulation_id=str(simulation_uuid), version=int(version))
        if not ver:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="ActionSimulationVersion not found")
        return {"status": "success", "data": _serialize_action_simulation_version(ver)}
    finally:
        await registry.close()
