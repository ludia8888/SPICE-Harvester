from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from bff.dependencies import OMSClientDep
from bff.services.oms_client import OMSClient
from shared.models.responses import ApiResponse
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.services.registries.agent_policy_registry import AgentPolicyRegistry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/context-tools", tags=["Context Tools"])


async def get_agent_policy_registry() -> AgentPolicyRegistry:
    from bff.main import get_agent_policy_registry as _get_agent_policy_registry

    return await _get_agent_policy_registry()


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

