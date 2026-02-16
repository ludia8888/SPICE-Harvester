"""Actions service (BFF).

Extracted from `bff.routers.actions` to keep routers thin, deduplicate
authorization + OMS forwarding, and centralize ActionSimulationRegistry flows
(Facade pattern).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, Dict, List, Optional
from uuid import UUID

import httpx
from fastapi import Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception
from bff.schemas.actions_requests import (
    ActionSimulateRequest,
    ActionSubmitBatchRequest,
    ActionSubmitRequest,
    ActionUndoRequest,
)
from bff.services.input_validation_service import sanitized_payload, validated_db_name
from bff.services.oms_client import OMSClient
from bff.utils.action_log_serialization import dt_iso, serialize_action_log_record
from bff.utils.httpx_exceptions import raise_httpx_as_http_exception
from shared.security.database_access import DOMAIN_MODEL_ROLES, resolve_database_actor
from shared.services.registries.action_log_registry import ActionLogRegistry
from shared.services.registries.action_simulation_registry import ActionSimulationRegistry
from shared.observability.tracing import trace_external_call, trace_db_operation

EnforceDatabaseRoleFn = Callable[..., Any]


def _require_action_type_id(action_type_id: str) -> str:
    action_type_id = str(action_type_id or "").strip()
    if not action_type_id:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "action_type_id is required", code=ErrorCode.ACTION_INPUT_INVALID)
    return action_type_id


async def _enforce_domain_model_role(
    *,
    http_request: Request,
    db_name: str,
    enforce_role: EnforceDatabaseRoleFn,
) -> None:
    try:
        await enforce_role(headers=http_request.headers, db_name=db_name, required_roles=DOMAIN_MODEL_ROLES)
    except ValueError as exc:
        raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED) from exc


def _parse_uuid(value: str) -> UUID:
    try:
        return UUID(str(value))
    except Exception as exc:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "Invalid UUID", code=ErrorCode.ACTION_INPUT_INVALID) from exc


def _oms_metadata(
    *,
    request_metadata: Dict[str, Any],
    principal_type: str,
    principal_id: str,
) -> Dict[str, Any]:
    return {
        **(request_metadata or {}),
        "user_id": principal_id,
        "user_type": principal_type,
    }


async def _oms_post(*, oms_client: OMSClient, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return await oms_client.post(path, json=payload)
    except httpx.HTTPStatusError as exc:
        raise_httpx_as_http_exception(exc)
        raise  # pragma: no cover
    except httpx.HTTPError as exc:
        raise classified_http_exception(status.HTTP_502_BAD_GATEWAY, "OMS 요청 실패", code=ErrorCode.UPSTREAM_ERROR) from exc


@asynccontextmanager
async def _action_simulation_registry() -> AsyncIterator[ActionSimulationRegistry]:
    registry = ActionSimulationRegistry()
    await registry.connect()
    try:
        yield registry
    finally:
        await registry.close()


def _serialize_action_simulation(record: Any) -> Dict[str, Any]:
    return {
        "simulation_id": getattr(record, "simulation_id", None),
        "db_name": getattr(record, "db_name", None),
        "action_type_id": getattr(record, "action_type_id", None),
        "title": getattr(record, "title", None),
        "description": getattr(record, "description", None),
        "created_by": getattr(record, "created_by", None),
        "created_by_type": getattr(record, "created_by_type", None),
        "created_at": dt_iso(getattr(record, "created_at", None)),
        "updated_at": dt_iso(getattr(record, "updated_at", None)),
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
        "created_at": dt_iso(getattr(record, "created_at", None)),
    }


@trace_external_call("bff.actions.submit_action")
async def submit_action(
    *,
    db_name: str,
    action_type_id: str,
    body: ActionSubmitRequest,
    http_request: Request,
    base_branch: str,
    overlay_branch: Optional[str],
    oms_client: OMSClient,
    enforce_role: EnforceDatabaseRoleFn,
) -> Dict[str, Any]:
    db_name = validated_db_name(db_name)
    action_type_id = _require_action_type_id(action_type_id)

    await _enforce_domain_model_role(http_request=http_request, db_name=db_name, enforce_role=enforce_role)
    principal_type, principal_id = resolve_database_actor(http_request.headers)

    oms_payload = {
        "input": sanitized_payload(body.input),
        "correlation_id": body.correlation_id,
        "metadata": _oms_metadata(
            request_metadata=body.metadata,
            principal_type=principal_type,
            principal_id=principal_id,
        ),
        "base_branch": base_branch,
        "overlay_branch": overlay_branch,
    }

    return await _oms_post(
        oms_client=oms_client,
        path=f"/api/v1/actions/{db_name}/async/{action_type_id}/submit",
        payload=oms_payload,
    )


@trace_external_call("bff.actions.submit_action_batch")
async def submit_action_batch(
    *,
    db_name: str,
    action_type_id: str,
    body: ActionSubmitBatchRequest,
    http_request: Request,
    oms_client: OMSClient,
    enforce_role: EnforceDatabaseRoleFn,
) -> Dict[str, Any]:
    db_name = validated_db_name(db_name)
    action_type_id = _require_action_type_id(action_type_id)

    await _enforce_domain_model_role(http_request=http_request, db_name=db_name, enforce_role=enforce_role)
    principal_type, principal_id = resolve_database_actor(http_request.headers)

    item_payloads: List[Dict[str, Any]] = []
    for item in body.items:
        item_payloads.append(
            {
                "request_id": item.request_id,
                "input": sanitized_payload(item.input),
                "correlation_id": item.correlation_id,
                "metadata": _oms_metadata(
                    request_metadata=item.metadata,
                    principal_type=principal_type,
                    principal_id=principal_id,
                ),
                "base_branch": item.base_branch,
                "overlay_branch": item.overlay_branch,
                "depends_on": list(item.depends_on or []),
                "trigger_on": item.trigger_on,
                "dependencies": [dep.model_dump(exclude_none=True) for dep in (item.dependencies or [])],
            }
        )

    oms_payload = {
        "items": item_payloads,
        "base_branch": body.base_branch,
        "overlay_branch": body.overlay_branch,
    }
    return await _oms_post(
        oms_client=oms_client,
        path=f"/api/v1/actions/{db_name}/async/{action_type_id}/submit-batch",
        payload=oms_payload,
    )


@trace_external_call("bff.actions.undo_action")
async def undo_action(
    *,
    db_name: str,
    action_log_id: str,
    body: ActionUndoRequest,
    http_request: Request,
    oms_client: OMSClient,
    enforce_role: EnforceDatabaseRoleFn,
) -> Dict[str, Any]:
    db_name = validated_db_name(db_name)
    _parse_uuid(action_log_id)

    await _enforce_domain_model_role(http_request=http_request, db_name=db_name, enforce_role=enforce_role)
    principal_type, principal_id = resolve_database_actor(http_request.headers)

    oms_payload = {
        "reason": body.reason,
        "correlation_id": body.correlation_id,
        "metadata": _oms_metadata(
            request_metadata=body.metadata,
            principal_type=principal_type,
            principal_id=principal_id,
        ),
        "base_branch": body.base_branch,
        "overlay_branch": body.overlay_branch,
    }
    return await _oms_post(
        oms_client=oms_client,
        path=f"/api/v1/actions/{db_name}/async/logs/{action_log_id}/undo",
        payload=oms_payload,
    )


@trace_external_call("bff.actions.simulate_action")
async def simulate_action(
    *,
    db_name: str,
    action_type_id: str,
    body: ActionSimulateRequest,
    http_request: Request,
    oms_client: OMSClient,
    enforce_role: EnforceDatabaseRoleFn,
) -> Dict[str, Any]:
    db_name = validated_db_name(db_name)
    action_type_id = _require_action_type_id(action_type_id)

    await _enforce_domain_model_role(http_request=http_request, db_name=db_name, enforce_role=enforce_role)
    principal_type, principal_id = resolve_database_actor(http_request.headers)

    oms_payload: Dict[str, Any] = {
        "input": sanitized_payload(body.input),
        "correlation_id": body.correlation_id,
        "metadata": _oms_metadata(
            request_metadata=body.metadata,
            principal_type=principal_type,
            principal_id=principal_id,
        ),
        "base_branch": body.base_branch,
        "overlay_branch": body.overlay_branch,
        "simulation_id": body.simulation_id,
        "title": body.title,
        "description": body.description,
        "scenarios": [s.model_dump(exclude_none=True) for s in (body.scenarios or [])],
        "include_effects": bool(body.include_effects),
    }
    if body.assumptions is not None:
        oms_payload["assumptions"] = body.assumptions.model_dump(exclude_none=True)

    return await _oms_post(
        oms_client=oms_client,
        path=f"/api/v1/actions/{db_name}/async/{action_type_id}/simulate",
        payload=oms_payload,
    )


@trace_db_operation("bff.actions.get_action_log")
async def get_action_log(
    *,
    db_name: str,
    action_log_id: str,
    http_request: Request,
    action_logs: ActionLogRegistry,
    enforce_role: EnforceDatabaseRoleFn,
) -> Dict[str, Any]:
    db_name = validated_db_name(db_name)
    action_log_uuid = _parse_uuid(action_log_id)

    await _enforce_domain_model_role(http_request=http_request, db_name=db_name, enforce_role=enforce_role)

    record = await action_logs.get_log(action_log_id=str(action_log_uuid))
    if not record or record.db_name != db_name:
        raise classified_http_exception(status.HTTP_404_NOT_FOUND, "ActionLog not found", code=ErrorCode.RESOURCE_NOT_FOUND)

    return {"status": "success", "data": serialize_action_log_record(record)}


@trace_db_operation("bff.actions.list_action_logs")
async def list_action_logs(
    *,
    db_name: str,
    http_request: Request,
    status_filter: Optional[List[str]],
    action_type_id: Optional[str],
    submitted_by: Optional[str],
    limit: int,
    offset: int,
    action_logs: ActionLogRegistry,
    enforce_role: EnforceDatabaseRoleFn,
) -> Dict[str, Any]:
    db_name = validated_db_name(db_name)

    await _enforce_domain_model_role(http_request=http_request, db_name=db_name, enforce_role=enforce_role)

    records = await action_logs.list_logs(
        db_name=db_name,
        statuses=status_filter,
        action_type_id=action_type_id,
        submitted_by=submitted_by,
        limit=limit,
        offset=offset,
    )
    return {"status": "success", "count": len(records), "data": [serialize_action_log_record(rec) for rec in records]}


@trace_db_operation("bff.actions.list_action_simulations")
async def list_action_simulations(
    *,
    db_name: str,
    http_request: Request,
    action_type_id: Optional[str],
    limit: int,
    offset: int,
    enforce_role: EnforceDatabaseRoleFn,
) -> Dict[str, Any]:
    db_name = validated_db_name(db_name)
    await _enforce_domain_model_role(http_request=http_request, db_name=db_name, enforce_role=enforce_role)

    async with _action_simulation_registry() as registry:
        sims = await registry.list_simulations(
            db_name=db_name,
            action_type_id=action_type_id,
            limit=limit,
            offset=offset,
        )
        return {"status": "success", "count": len(sims), "data": [_serialize_action_simulation(sim) for sim in sims]}


@trace_db_operation("bff.actions.get_action_simulation")
async def get_action_simulation(
    *,
    db_name: str,
    simulation_id: str,
    http_request: Request,
    include_versions: bool,
    version_limit: int,
    enforce_role: EnforceDatabaseRoleFn,
) -> Dict[str, Any]:
    db_name = validated_db_name(db_name)
    simulation_uuid = _parse_uuid(simulation_id)
    await _enforce_domain_model_role(http_request=http_request, db_name=db_name, enforce_role=enforce_role)

    async with _action_simulation_registry() as registry:
        sim = await registry.get_simulation(simulation_id=str(simulation_uuid))
        if not sim or sim.db_name != db_name:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "ActionSimulation not found", code=ErrorCode.RESOURCE_NOT_FOUND)

        payload: Dict[str, Any] = {"simulation": _serialize_action_simulation(sim)}
        if include_versions:
            versions = await registry.list_versions(simulation_id=str(simulation_uuid), limit=version_limit, offset=0)
            payload["versions"] = [_serialize_action_simulation_version(v) for v in versions]
        return {"status": "success", "data": payload}


@trace_db_operation("bff.actions.list_action_simulation_versions")
async def list_action_simulation_versions(
    *,
    db_name: str,
    simulation_id: str,
    http_request: Request,
    limit: int,
    offset: int,
    enforce_role: EnforceDatabaseRoleFn,
) -> Dict[str, Any]:
    db_name = validated_db_name(db_name)
    simulation_uuid = _parse_uuid(simulation_id)
    await _enforce_domain_model_role(http_request=http_request, db_name=db_name, enforce_role=enforce_role)

    async with _action_simulation_registry() as registry:
        sim = await registry.get_simulation(simulation_id=str(simulation_uuid))
        if not sim or sim.db_name != db_name:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "ActionSimulation not found", code=ErrorCode.RESOURCE_NOT_FOUND)

        versions = await registry.list_versions(simulation_id=str(simulation_uuid), limit=limit, offset=offset)
        return {
            "status": "success",
            "count": len(versions),
            "data": [_serialize_action_simulation_version(v) for v in versions],
        }


@trace_db_operation("bff.actions.get_action_simulation_version")
async def get_action_simulation_version(
    *,
    db_name: str,
    simulation_id: str,
    version: int,
    http_request: Request,
    enforce_role: EnforceDatabaseRoleFn,
) -> Dict[str, Any]:
    db_name = validated_db_name(db_name)
    simulation_uuid = _parse_uuid(simulation_id)
    await _enforce_domain_model_role(http_request=http_request, db_name=db_name, enforce_role=enforce_role)

    async with _action_simulation_registry() as registry:
        sim = await registry.get_simulation(simulation_id=str(simulation_uuid))
        if not sim or sim.db_name != db_name:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "ActionSimulation not found", code=ErrorCode.RESOURCE_NOT_FOUND)

        ver = await registry.get_version(simulation_id=str(simulation_uuid), version=int(version))
        if not ver:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "ActionSimulationVersion not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        return {"status": "success", "data": _serialize_action_simulation_version(ver)}
