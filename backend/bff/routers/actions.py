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
from pydantic import BaseModel, Field

from bff.dependencies import get_action_log_registry, get_oms_client
from bff.services.oms_client import OMSClient
from shared.security.database_access import DOMAIN_MODEL_ROLES, enforce_database_role, resolve_database_actor
from shared.security.input_sanitizer import SecurityViolationError, sanitize_input, validate_db_name
from shared.services.action_log_registry import ActionLogRecord, ActionLogRegistry

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

        # Permission gate (Foundry-aligned minimum): domain model roles for operational writeback.
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
