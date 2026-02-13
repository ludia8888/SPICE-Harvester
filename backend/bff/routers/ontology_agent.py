"""
Ontology Agent API Router

Convenience endpoint for pure ontology tasks.
Delegates to Pipeline Agent with empty dataset_ids.

For combined pipeline + ontology tasks, use /api/v1/agent/pipeline-runs directly.
"""

import logging
from typing import Any, Dict, Optional
from shared.observability.tracing import trace_endpoint

from fastapi import APIRouter, Depends, HTTPException, Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception
from pydantic import BaseModel, Field

from shared.models.requests import ApiResponse
from shared.models.pipeline_plan import PipelinePlanDataScope
from shared.security.auth_utils import enforce_db_scope
from shared.services.agent.llm_gateway import LLMGateway
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.pipeline_plan_registry import PipelinePlanRegistry
from shared.services.storage.redis_service import RedisService, create_redis_service
from shared.config.settings import get_settings
from bff.services.pipeline_agent_autonomous_loop import run_pipeline_agent_mcp_autonomous

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ontology-agent", tags=["Ontology Agent"])


class OntologyAgentRunRequest(BaseModel):
    """Request body for ontology agent runs."""

    goal: str = Field(..., description="Natural language goal for ontology creation/modification")
    db_name: str = Field(..., description="Database name")
    branch: str = Field(default="main", description="Branch name")
    target_class_id: Optional[str] = Field(default=None, description="Target class ID for modification")
    dataset_sample: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Sample data for schema inference: {columns: [], data: [[]]}",
    )
    answers: Optional[Dict[str, Any]] = Field(default=None, description="Answers to clarification questions")
    selected_model: Optional[str] = Field(default=None, description="LLM model to use")


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


@router.post(
    "/runs",
    response_model=ApiResponse,
    summary="Run ontology agent",
    description="""
    Execute the autonomous ontology agent to create or modify ontology schemas
    based on natural language instructions.

    This is a convenience endpoint that delegates to the Pipeline Agent with empty dataset_ids.
    For combined pipeline + ontology tasks, use /api/v1/agent/pipeline-runs directly.

    Example goals:
    - "이 데이터셋으로 Customer 클래스 만들어줘"
    - "email, name, phone 필드를 기존 Person 클래스에 매핑해줘"
    - "Customer와 Order 사이에 hasOrders 관계 만들어줘"
    """,
)
@trace_endpoint("bff.ontology.run_ontology_agent")
async def run_ontology_agent(
    request: Request,
    body: OntologyAgentRunRequest,
) -> ApiResponse:
    """
    Run the autonomous ontology agent by delegating to Pipeline Agent.
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
        raise classified_http_exception(
            status.HTTP_403_FORBIDDEN,
            f"Database access denied: {exc}",
            code=ErrorCode.PERMISSION_DENIED,
        )

    settings = get_settings()

    # Get services
    llm_gateway = LLMGateway()
    try:
        redis_service = create_redis_service(settings)
    except Exception:
        logging.getLogger(__name__).warning("Ontology agent redis service init fallback", exc_info=True)
        redis_service = None
    try:
        audit_store = AuditLogStore()
    except Exception:
        logging.getLogger(__name__).warning("Ontology agent audit store init fallback", exc_info=True)
        audit_store = None

    # Get registries (required by pipeline agent, but won't be used for ontology-only)
    dataset_registry = DatasetRegistry()
    plan_registry = PipelinePlanRegistry()

    data_policies = getattr(settings, "data_policies", None)
    allowed_models = list(settings.llm.allowed_models or []) if hasattr(settings.llm, "allowed_models") else None

    # Build goal with ontology hint if sample data provided
    goal = body.goal
    if body.dataset_sample:
        # Append sample data context to help the agent
        columns = body.dataset_sample.get("columns", [])
        if columns and "컬럼" not in goal and "column" not in goal.lower():
            goal = f"{goal} (컬럼: {', '.join(columns)})"

    # Build planner hints for ontology task
    planner_hints = {
        "ontology_mode": True,
        "target_class_id": body.target_class_id,
    }

    # Build task spec with sample data if provided
    task_spec = None
    if body.dataset_sample:
        task_spec = {"dataset_sample": body.dataset_sample}

    try:
        result = await run_pipeline_agent_mcp_autonomous(
            goal=goal,
            data_scope=PipelinePlanDataScope(
                db_name=body.db_name,
                branch=body.branch,
                dataset_ids=[],  # Empty for ontology-only tasks
            ),
            answers=body.answers,
            planner_hints=planner_hints,
            task_spec=task_spec,
            persist_plan=False,  # Don't persist pipeline plan for ontology-only
            actor=actor,
            tenant_id=tenant_id,
            user_id=user_id,
            data_policies=data_policies,
            selected_model=body.selected_model,
            allowed_models=allowed_models,
            llm_gateway=llm_gateway,
            redis_service=redis_service,
            audit_store=audit_store,
            dataset_registry=dataset_registry,
            plan_registry=plan_registry,
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

        # Extract ontology from result (may be in different places depending on agent state)
        ontology = result.get("ontology")
        if not ontology and isinstance(result.get("report"), dict):
            ontology = result["report"].get("ontology")

        return ApiResponse(
            status=api_status,
            message=message,
            data={
                "run_id": result.get("run_id"),
                "ontology": ontology,
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
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"Ontology agent error: {exc}",
            code=ErrorCode.INTERNAL_ERROR,
        )
