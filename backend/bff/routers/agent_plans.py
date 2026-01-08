"""
Agent plan validation API (BFF).

Validates planner output against allowlist + risk policy.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, status

from bff.services.agent_plan_validation import validate_agent_plan
from shared.models.agent_plan import AgentPlan
from shared.models.requests import ApiResponse
from shared.services.agent_tool_registry import AgentToolRegistry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent-plans", tags=["Agent Plans"])


async def get_agent_tool_registry() -> AgentToolRegistry:
    from bff.main import get_agent_tool_registry as _get_agent_tool_registry

    return await _get_agent_tool_registry()


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
