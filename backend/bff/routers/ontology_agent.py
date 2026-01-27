"""
Ontology Agent API Router

Provides the POST /api/v1/ontology-agent/runs endpoint for
autonomous ontology schema creation/mapping via natural language.
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status

from shared.models.requests import ApiResponse
from shared.security.auth_utils import enforce_db_scope
from shared.services.agent.llm_gateway import LLMGateway
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.storage.redis_service import RedisService, create_redis_service_legacy
from shared.config.settings import get_settings
from bff.services.ontology_agent_models import OntologyAgentRunRequest, OntologyAgentRunResponse
from bff.services.ontology_agent_autonomous_loop import run_ontology_agent_autonomous

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ontology-agent", tags=["Ontology Agent"])


def _get_tenant_id(request: Request) -> str:
    """Extract tenant ID from request headers."""
    return str(
        request.headers.get("X-Tenant-ID")
        or request.headers.get("X-Project")
        or request.headers.get("X-DB-Name")
        or "default"
    ).strip()


def _get_user_id(request: Request) -> Optional[str]:
    """Extract user ID from request headers."""
    user_id = request.headers.get("X-User-ID") or request.headers.get("X-Actor")
    return str(user_id).strip() if user_id else None


def _get_actor(request: Request) -> str:
    """Extract actor from request headers."""
    return str(
        request.headers.get("X-Actor")
        or request.headers.get("X-User-ID")
        or "system"
    ).strip()


async def _get_llm_gateway() -> LLMGateway:
    """Get LLM gateway instance."""
    return LLMGateway()


async def _get_redis_service() -> Optional[RedisService]:
    """Get Redis service if available."""
    try:
        return create_redis_service_legacy()
    except Exception:
        return None


async def _get_audit_store() -> Optional[AuditLogStore]:
    """Get audit store if available."""
    try:
        return AuditLogStore()
    except Exception:
        return None


@router.post(
    "/runs",
    response_model=ApiResponse,
    summary="Run ontology agent",
    description="""
    Execute the autonomous ontology agent to create or modify ontology schemas
    based on natural language instructions.

    Example goals:
    - "이 데이터셋으로 Customer 클래스 만들어줘"
    - "email, name, phone 필드를 기존 Person 클래스에 매핑해줘"
    - "Customer와 Order 사이에 hasOrders 관계 만들어줘"
    """,
)
async def run_ontology_agent(
    request: Request,
    body: OntologyAgentRunRequest,
) -> ApiResponse:
    """
    Run the autonomous ontology agent.

    The agent will:
    1. Parse the natural language goal
    2. Use available tools to build/modify ontology schemas
    3. Validate the schema before saving
    4. Return the created/modified ontology or ask clarification questions
    """
    tenant_id = _get_tenant_id(request)
    user_id = _get_user_id(request)
    actor = _get_actor(request)

    # Enforce DB scope if configured
    try:
        enforce_db_scope(request.headers, db_name=body.db_name)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Database access denied: {exc}",
        )

    # Get services
    llm_gateway = await _get_llm_gateway()
    redis_service = await _get_redis_service()
    audit_store = await _get_audit_store()

    # Get data policies from settings
    settings = get_settings()
    data_policies = None
    if hasattr(settings, "data_policies"):
        data_policies = getattr(settings, "data_policies", None)

    # Get allowed/selected models
    selected_model = body.selected_model
    allowed_models = None
    if hasattr(settings.llm, "allowed_models"):
        allowed_models = list(settings.llm.allowed_models or [])

    try:
        result = await run_ontology_agent_autonomous(
            goal=body.goal,
            db_name=body.db_name,
            branch=body.branch,
            target_class_id=body.target_class_id,
            dataset_sample=body.dataset_sample,
            answers=body.answers,
            actor=actor,
            tenant_id=tenant_id,
            user_id=user_id,
            data_policies=data_policies,
            selected_model=selected_model,
            allowed_models=allowed_models,
            llm_gateway=llm_gateway,
            redis_service=redis_service,
            audit_store=audit_store,
        )

        # Map status to API response status
        agent_status = result.get("status", "failed")
        api_status = "success"
        message = "Ontology agent completed successfully"

        if agent_status == "clarification_required":
            api_status = "clarification_required"
            message = "Agent needs clarification"
        elif agent_status == "partial":
            api_status = "partial"
            message = "Agent completed with partial results"
        elif agent_status == "failed":
            api_status = "error"
            message = "Ontology agent failed"

        return ApiResponse(
            status=api_status,
            message=message,
            data={
                "run_id": result.get("run_id"),
                "ontology": result.get("ontology"),
                "questions": result.get("questions", []),
                "validation_errors": result.get("validation_errors", []),
                "validation_warnings": result.get("validation_warnings", []),
                "mapping_suggestions": result.get("mapping_suggestions"),
                "planner": result.get("planner"),
                "llm": result.get("llm"),
            },
        )

    except Exception as exc:
        logger.exception("Ontology agent failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ontology agent error: {exc}",
        )
