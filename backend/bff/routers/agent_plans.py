"""
Agent plan validation API (BFF).

Validates planner output against allowlist + risk policy.
"""

import logging

from datetime import datetime, timezone
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from bff.services.agent_plan_compiler import compile_agent_plan
from bff.services.agent_plan_validation import validate_agent_plan
from shared.dependencies.providers import AuditLogStoreDep, LLMGatewayDep, RedisServiceDep
from shared.middleware.rate_limiter import RateLimitPresets, rate_limit
from shared.models.agent_plan import AgentPlan, AgentPlanDataScope
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import sanitize_input
from shared.services.agent_registry import AgentRegistry
from shared.services.agent_tool_registry import AgentToolRegistry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent-plans", tags=["Agent Plans"])


async def get_agent_tool_registry() -> AgentToolRegistry:
    from bff.main import get_agent_tool_registry as _get_agent_tool_registry

    return await _get_agent_tool_registry()


async def get_agent_registry() -> AgentRegistry:
    from bff.main import get_agent_registry as _get_agent_registry

    return await _get_agent_registry()


class AgentPlanApprovalRequest(BaseModel):
    decision: str = Field(..., min_length=1, max_length=40)
    step_id: str | None = Field(default=None, max_length=200)
    comment: str | None = Field(default=None, max_length=2000)
    metadata: dict | None = Field(default=None)


class AgentPlanCompileRequest(BaseModel):
    goal: str = Field(..., min_length=1, max_length=2000)
    data_scope: AgentPlanDataScope | None = Field(default=None)
    answers: dict | None = Field(default=None, description="Clarification answers from prior compile response")


@router.post("/compile", response_model=ApiResponse)
@rate_limit(**RateLimitPresets.STRICT)
async def compile_plan(
    body: AgentPlanCompileRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
) -> ApiResponse:
    payload = sanitize_input(body.model_dump(exclude_none=True))
    goal = str(payload.get("goal") or "").strip()
    data_scope = body.data_scope
    answers = payload.get("answers")
    actor = (
        request.headers.get("X-User-ID")
        or request.headers.get("X-User")
        or request.headers.get("X-Actor")
        or "system"
    )

    result = await compile_agent_plan(
        goal=goal,
        data_scope=data_scope,
        answers=answers if isinstance(answers, dict) else None,
        actor=str(actor),
        tool_registry=tool_registry,
        llm_gateway=llm,
        redis_service=redis_service,
        audit_store=audit_store,
    )

    response_data = {
        "status": result.status,
        "plan": result.plan.model_dump(mode="json") if result.plan else None,
        "validation_errors": list(result.validation_errors or []),
        "validation_warnings": list(result.validation_warnings or []),
        "questions": [q.model_dump(mode="json") for q in (result.questions or [])],
        "planner": {
            "confidence": result.planner_confidence,
            "notes": result.planner_notes,
        },
        "llm": (
            {
                "provider": result.llm_meta.provider,
                "model": result.llm_meta.model,
                "cache_hit": result.llm_meta.cache_hit,
                "latency_ms": result.llm_meta.latency_ms,
            }
            if result.llm_meta
            else None
        ),
    }

    if result.status == "success":
        return ApiResponse.success(message="Agent plan compiled", data=response_data)
    if result.status == "clarification_required":
        return ApiResponse.warning(message="Clarification required", data=response_data)

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail={"errors": result.validation_errors, "warnings": result.validation_warnings},
    )


@router.post("/validate", response_model=ApiResponse)
async def validate_plan(
    plan: AgentPlan,
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
) -> ApiResponse:
    try:
        result = await validate_agent_plan(plan=plan, tool_registry=tool_registry)
        if result.errors:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"errors": result.errors, "warnings": result.warnings},
            )
        message = "Agent plan validated"
        data = {
            "plan": result.plan.model_dump(mode="json"),
            "warnings": result.warnings,
        }
        if result.warnings:
            return ApiResponse.warning(message=message, data=data)
        return ApiResponse.success(message=message, data=data)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to validate agent plan: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.post("/{plan_id}/approvals", response_model=ApiResponse)
async def approve_plan(
    plan_id: str,
    request: Request,
    body: AgentPlanApprovalRequest,
    agent_registry: AgentRegistry = Depends(get_agent_registry),
) -> ApiResponse:
    try:
        try:
            plan_id = str(UUID(plan_id))
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc
        payload = sanitize_input(body.model_dump(exclude_none=True))
        decision = str(payload.get("decision") or "").strip().upper()
        if not decision:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="decision is required")

        approved_by = (
            request.headers.get("X-User-ID")
            or request.headers.get("X-User")
            or request.headers.get("X-Actor")
            or "system"
        )
        record = await agent_registry.create_approval(
            approval_id=str(uuid4()),
            plan_id=plan_id,
            step_id=payload.get("step_id"),
            decision=decision,
            approved_by=str(approved_by),
            approved_at=datetime.now(timezone.utc),
            comment=payload.get("comment"),
            metadata=payload.get("metadata") or {},
        )
        return ApiResponse.created(
            message="Agent plan approval recorded",
            data={
                "approval": {
                    "approval_id": record.approval_id,
                    "plan_id": record.plan_id,
                    "step_id": record.step_id,
                    "decision": record.decision,
                    "approved_by": record.approved_by,
                    "approved_at": record.approved_at.isoformat(),
                    "comment": record.comment,
                    "metadata": record.metadata,
                }
            },
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to record agent plan approval: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))
