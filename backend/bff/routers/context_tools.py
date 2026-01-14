from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from bff.dependencies import OMSClientDep
from bff.services.oms_client import OMSClient
from bff.services.pipeline_context_pack import build_pipeline_context_pack
from shared.models.responses import ApiResponse
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.services.agent_policy_registry import AgentPolicyRegistry
from shared.services.dataset_registry import DatasetRegistry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/context-tools", tags=["Context Tools"])


async def get_agent_policy_registry() -> AgentPolicyRegistry:
    from bff.main import get_agent_policy_registry as _get_agent_policy_registry

    return await _get_agent_policy_registry()


async def get_dataset_registry() -> DatasetRegistry:
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


def _resolve_verified_principal(request: Request) -> str:
    user = getattr(request.state, "user", None)
    if user is None or not getattr(user, "verified", False):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User JWT required")
    tenant_id = getattr(user, "tenant_id", None) or getattr(user, "org_id", None) or "default"
    return str(tenant_id)


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
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied")


class DatasetDescribeRequest(BaseModel):
    db_name: str = Field(..., min_length=1, max_length=200)
    branch: Optional[str] = Field(default=None, max_length=200)
    dataset_ids: Optional[list[str]] = None
    max_selected_datasets: int = Field(default=6, ge=1, le=20)
    max_sample_rows: int = Field(default=20, ge=0, le=50)
    max_join_candidates: int = Field(default=10, ge=0, le=30)


@router.post("/datasets/describe", response_model=ApiResponse)
async def describe_datasets(
    body: DatasetDescribeRequest,
    request: Request,
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    tenant_id = _resolve_verified_principal(request)
    policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
    data_policies = getattr(policy, "data_policies", None) if policy is not None else None
    data_policies = data_policies if isinstance(data_policies, dict) else {}

    payload = sanitize_input(body.model_dump(exclude_none=True))
    db_name = validate_db_name(str(payload.get("db_name") or "").strip())
    branch = str(payload.get("branch") or "").strip() or None
    dataset_ids = payload.get("dataset_ids")
    dataset_ids_list = [str(v).strip() for v in (dataset_ids or []) if str(v).strip()] if isinstance(dataset_ids, list) else []

    allowed_db_names = _policy_set(data_policies.get("allowed_db_names"))
    _enforce_policy_value(allowed=allowed_db_names, value=db_name)
    allowed_branches = _policy_set(data_policies.get("allowed_branches"))
    if branch:
        _enforce_policy_value(allowed=allowed_branches, value=branch)

    allowed_dataset_ids = _policy_set(data_policies.get("allowed_dataset_ids"))
    if allowed_dataset_ids:
        if dataset_ids_list:
            for did in dataset_ids_list:
                _enforce_policy_value(allowed=allowed_dataset_ids, value=did)
        else:
            # Constrain overview when the tenant policy is scoped to a fixed set.
            dataset_ids_list = sorted(allowed_dataset_ids)

    try:
        pack = await build_pipeline_context_pack(
            db_name=db_name,
            branch=branch,
            dataset_ids=dataset_ids_list or None,
            dataset_registry=dataset_registry,
            max_selected_datasets=int(payload.get("max_selected_datasets") or 6),
            max_sample_rows=int(payload.get("max_sample_rows") or 20),
            max_join_candidates=int(payload.get("max_join_candidates") or 10),
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Dataset context tool failed")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    if allowed_dataset_ids:
        overview = pack.get("datasets_overview")
        if isinstance(overview, list):
            pack["datasets_overview"] = [item for item in overview if str((item or {}).get("dataset_id") or "") in allowed_dataset_ids]
        selected = pack.get("selected_datasets")
        if isinstance(selected, list):
            pack["selected_datasets"] = [item for item in selected if str((item or {}).get("dataset_id") or "") in allowed_dataset_ids]

    return ApiResponse.success(message="Dataset context extracted", data={"context": pack})


class OntologySnapshotRequest(BaseModel):
    db_name: str = Field(..., min_length=1, max_length=200)
    ontology_id: str = Field(..., min_length=1, max_length=200)
    branch: str = Field(default="main", min_length=1, max_length=200)


@router.post("/ontology/snapshot", response_model=ApiResponse)
async def snapshot_ontology(
    body: OntologySnapshotRequest,
    request: Request,
    oms_client: OMSClient = OMSClientDep,
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
) -> ApiResponse:
    tenant_id = _resolve_verified_principal(request)
    policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
    data_policies = getattr(policy, "data_policies", None) if policy is not None else None
    data_policies = data_policies if isinstance(data_policies, dict) else {}

    payload = sanitize_input(body.model_dump(exclude_none=True))
    db_name = validate_db_name(str(payload.get("db_name") or "").strip())
    branch = str(payload.get("branch") or "main").strip() or "main"
    ontology_id = str(payload.get("ontology_id") or "").strip()
    if not ontology_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ontology_id is required")

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
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

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

