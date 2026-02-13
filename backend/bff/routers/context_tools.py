from __future__ import annotations
from shared.observability.tracing import trace_endpoint

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception
from pydantic import BaseModel, Field

from bff.dependencies import OMSClientDep
from bff.routers.registry_deps import get_agent_policy_registry, get_dataset_registry
from bff.services.pipeline_plan_tenant_service import resolve_verified_tenant_user
from bff.services.oms_client import OMSClient
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.models.responses import ApiResponse
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.services.registries.agent_policy_registry import AgentPolicyRegistry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/context-tools", tags=["Context Tools"])


def _policy_set(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, (list, tuple, set)):
        return {str(v).strip() for v in value if str(v).strip()}
    if isinstance(value, str):
        return {part.strip() for part in value.split(",") if part.strip()}
    return set()


def _enforce_policy_value(*, allowed: set[str], value: str) -> None:
    if allowed and value not in allowed:
        raise classified_http_exception(status.HTTP_403_FORBIDDEN, "Permission denied", code=ErrorCode.PERMISSION_DENIED)


class OntologySnapshotRequest(BaseModel):
    db_name: str = Field(..., min_length=1, max_length=200)
    ontology_id: str = Field(..., min_length=1, max_length=200)
    branch: str = Field(default="main", min_length=1, max_length=200)


class DatasetDescribeRequest(BaseModel):
    db_name: str = Field(..., min_length=1, max_length=200)
    branch: str = Field(default="main", min_length=1, max_length=200)
    dataset_ids: Optional[list[str]] = None


def _filter_dataset_ids(
    dataset_ids: Optional[list[str]],
    *,
    allowed_dataset_ids: set[str],
) -> Optional[list[str]]:
    if dataset_ids is None:
        return sorted(allowed_dataset_ids) if allowed_dataset_ids else None
    cleaned = [str(item).strip() for item in dataset_ids if str(item).strip()]
    if not cleaned:
        return sorted(allowed_dataset_ids) if allowed_dataset_ids else []
    if not allowed_dataset_ids:
        return cleaned
    return [item for item in cleaned if item in allowed_dataset_ids]


@router.post("/datasets/describe", response_model=ApiResponse)
@trace_endpoint("bff.context_tools.describe_datasets")
async def describe_datasets(
    body: DatasetDescribeRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
) -> ApiResponse:
    tenant_id, _user_id = resolve_verified_tenant_user(request)
    policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
    data_policies = getattr(policy, "data_policies", None) if policy is not None else None
    data_policies = data_policies if isinstance(data_policies, dict) else {}

    payload = sanitize_input(body.model_dump(exclude_none=True))
    db_name = validate_db_name(str(payload.get("db_name") or "").strip())
    branch = str(payload.get("branch") or "main").strip() or "main"

    allowed_db_names = _policy_set(data_policies.get("allowed_db_names"))
    _enforce_policy_value(allowed=allowed_db_names, value=db_name)

    allowed_dataset_ids = _policy_set(data_policies.get("allowed_dataset_ids"))
    requested_ids = payload.get("dataset_ids")
    requested_ids_list = requested_ids if isinstance(requested_ids, list) else None
    effective_ids = _filter_dataset_ids(requested_ids_list, allowed_dataset_ids=allowed_dataset_ids)

    records = await dataset_registry.list_datasets(db_name=db_name, branch=branch)
    if effective_ids is not None:
        allowed = set(effective_ids)
        records = [record for record in records if str(record.get("dataset_id") or "") in allowed]

    overview = [
        {
            "dataset_id": record.get("dataset_id"),
            "name": record.get("name"),
            "db_name": record.get("db_name"),
            "branch": record.get("branch") or branch,
            "source_type": record.get("source_type"),
            "description": record.get("description"),
            "schema_json": record.get("schema_json"),
            "sample_json": record.get("sample_json"),
            "latest_commit_id": record.get("latest_commit_id"),
            "row_count": record.get("row_count"),
            "updated_at": record.get("updated_at"),
        }
        for record in records
        if isinstance(record, dict)
    ]

    return ApiResponse.success(
        message="Dataset context extracted",
        data={
            "context": {
                "db_name": db_name,
                "branch": branch,
                "datasets_overview": overview,
            }
        },
    )


@router.post("/ontology/snapshot", response_model=ApiResponse)
@trace_endpoint("bff.context_tools.snapshot_ontology")
async def snapshot_ontology(
    body: OntologySnapshotRequest,
    request: Request,
    oms_client: OMSClient = OMSClientDep,
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
) -> ApiResponse:
    tenant_id, _user_id = resolve_verified_tenant_user(request)
    policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
    data_policies = getattr(policy, "data_policies", None) if policy is not None else None
    data_policies = data_policies if isinstance(data_policies, dict) else {}

    payload = sanitize_input(body.model_dump(exclude_none=True))
    db_name = validate_db_name(str(payload.get("db_name") or "").strip())
    branch = str(payload.get("branch") or "main").strip() or "main"
    ontology_id = str(payload.get("ontology_id") or "").strip()
    if not ontology_id:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "ontology_id is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

    allowed_db_names = _policy_set(data_policies.get("allowed_db_names"))
    _enforce_policy_value(allowed=allowed_db_names, value=db_name)
    allowed_branches = _policy_set(data_policies.get("allowed_branches"))
    _enforce_policy_value(allowed=allowed_branches, value=branch)
    allowed_ontology_ids = _policy_set(data_policies.get("allowed_ontology_ids"))
    _enforce_policy_value(allowed=allowed_ontology_ids, value=ontology_id)

    try:
        payload_obj = await oms_client.get(
            f"/api/v1/database/{db_name}/ontology/{ontology_id}",
            params={"branch": branch},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Ontology snapshot failed")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc

    # OMS responses vary by endpoint; keep compatibility.
    ontology = payload_obj.get("data", payload_obj) if isinstance(payload_obj, dict) else payload_obj
    return ApiResponse.success(
        message="Ontology snapshot extracted",
        data={
            "db_name": db_name,
            "branch": branch,
            "ontology_id": ontology_id,
            "ontology": ontology,
        },
    )
