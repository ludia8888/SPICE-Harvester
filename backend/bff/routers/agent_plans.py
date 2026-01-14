"""
Agent plan validation API (BFF).

Validates planner output against allowlist + risk policy.
"""

import contextlib
import logging

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from bff.dependencies import get_action_log_registry
from bff.services.agent_plan_compiler import compile_agent_plan
from bff.services.agent_plan_validation import validate_agent_plan
from bff.services.operational_memory import build_operational_context_pack
from bff.services.pipeline_context_pack import build_pipeline_context_pack
from shared.dependencies.providers import AuditLogStoreDep, LLMGatewayDep, RedisServiceDep
from shared.middleware.rate_limiter import RateLimitPresets, rate_limit
from shared.models.agent_plan import AgentPlan, AgentPlanDataScope
from shared.models.agent_plan_report import PlanPatchOp
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import sanitize_input
from shared.services.action_log_registry import ActionLogRegistry
from shared.services.agent_registry import AgentRegistry
from shared.services.agent_model_registry import AgentModelRegistry
from shared.services.agent_plan_registry import AgentPlanRegistry
from shared.services.agent_tool_registry import AgentToolRegistry
from shared.services.dataset_registry import DatasetRegistry
from shared.services.llm_quota import LLMQuotaExceededError
from shared.config.service_config import ServiceConfig
from shared.config.settings import get_settings
from shared.utils.json_patch import JsonPatchError, apply_json_patch

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent-plans", tags=["Agent Plans"])

_AGENT_FORWARD_HEADER_ALLOWLIST = {
    "accept",
    "accept-language",
    "authorization",
    "content-type",
    "user-agent",
    "x-admin-token",
    "x-actor",
    "x-actor-type",
    "x-org-id",
    "x-principal-id",
    "x-principal-type",
    "x-request-id",
    "x-tenant-id",
    "x-user",
    "x-user-id",
    "x-user-type",
}
_APPROVED_DECISIONS = {"APPROVED", "APPROVE", "ALLOW", "YES"}
_REJECTED_DECISIONS = {"REJECTED", "REJECT", "DENY", "NO"}


class _NullAgentModelRegistry:
    async def get_model(self, *, model_id: str):  # noqa: ANN001
        return None


def _resolve_tenant_id(request: Request) -> str:
    user = getattr(request.state, "user", None)
    candidate = (
        getattr(user, "tenant_id", None)
        or getattr(user, "org_id", None)
        or request.headers.get("X-Tenant-ID")
        or request.headers.get("X-Org-ID")
        or "default"
    )
    return str(candidate).strip() or "default"


async def _resolve_tenant_policy_constraints(
    request: Request,
) -> tuple[Optional[list[str]], Optional[str], Optional[list[str]]]:
    """
    Best-effort tenant policy lookup (AUTH-005).

    Keeps unit-tests + degraded startup paths working by returning (None, None)
    when the policy registry is unavailable.
    """

    tenant_id = _resolve_tenant_id(request)
    try:
        from bff.main import get_agent_policy_registry as _get_agent_policy_registry

        policy_registry = await _get_agent_policy_registry()
        policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
    except Exception:
        return None, None, None

    if not policy:
        return None, None, None

    allowed_tools = [str(t).strip() for t in (policy.allowed_tools or []) if str(t).strip()]
    allowed_tool_ids = allowed_tools or None

    allowed_models = [str(m).strip() for m in (policy.allowed_models or []) if str(m).strip()]
    if policy.default_model:
        allowed_models.append(str(policy.default_model).strip())
    allowed_models = [m for m in allowed_models if m]
    allowed_models = list(dict.fromkeys(allowed_models))
    allowed_models_final = allowed_models or None

    selected_model: Optional[str] = None
    if policy.default_model:
        selected_model = str(policy.default_model).strip() or None
    if selected_model is None and allowed_models_final:
        selected_model = str(allowed_models_final[0]).strip() or None

    return allowed_tool_ids, selected_model, allowed_models_final


def _forward_headers_to_agent(request: Request) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for key, value in request.headers.items():
        if not value:
            continue
        if key.lower() in _AGENT_FORWARD_HEADER_ALLOWLIST:
            headers[key] = value
    headers.setdefault("X-Spice-Caller", "bff")
    return headers


def _render_path(template: str, path_params: Dict[str, Any]) -> str:
    path = str(template or "")
    for key, value in (path_params or {}).items():
        token = "{" + str(key) + "}"
        if token in path and value not in (None, ""):
            path = path.replace(token, str(value))
    return path


def _is_action_simulate_path(path: str) -> bool:
    return str(path or "").rstrip("/").endswith("/simulate")


def _is_preview_safe_step(method: str, path: str, policy_risk_level: str) -> bool:
    normalized_method = str(method or "").strip().upper()
    if normalized_method == "GET":
        return True
    if _is_action_simulate_path(path):
        return True
    return str(policy_risk_level or "").strip().lower() == "read"


async def _call_agent_create_run(*, request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
    agent_url = ServiceConfig.get_agent_url().rstrip("/")
    url = f"{agent_url}/api/v1/agent/runs"
    timeout_seconds = get_settings().clients.agent_proxy_timeout_seconds
    ssl_config = ServiceConfig.get_client_ssl_config()

    async with httpx.AsyncClient(timeout=timeout_seconds, verify=ssl_config.get("verify", True)) as client:
        resp = await client.post(url, headers=_forward_headers_to_agent(request), json=payload)
    try:
        body = resp.json()
    except Exception:
        body = {"status": "error", "message": (resp.text or "").strip() or "Agent service error"}

    if resp.status_code >= 400:
        raise HTTPException(status_code=int(resp.status_code), detail=body.get("detail") if isinstance(body, dict) else body)
    return body if isinstance(body, dict) else {"status": "success", "data": body}


async def get_agent_tool_registry() -> AgentToolRegistry:
    from bff.main import get_agent_tool_registry as _get_agent_tool_registry

    return await _get_agent_tool_registry()


async def get_agent_registry() -> AgentRegistry:
    from bff.main import get_agent_registry as _get_agent_registry

    return await _get_agent_registry()


async def get_agent_plan_registry() -> AgentPlanRegistry:
    from bff.main import get_agent_plan_registry as _get_agent_plan_registry

    return await _get_agent_plan_registry()


async def get_dataset_registry() -> DatasetRegistry:
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


async def get_agent_model_registry() -> AgentModelRegistry:
    from bff.main import get_agent_model_registry as _get_agent_model_registry

    try:
        return await _get_agent_model_registry()
    except Exception:
        return _NullAgentModelRegistry()  # type: ignore[return-value]


class AgentPlanApprovalRequest(BaseModel):
    decision: str = Field(..., min_length=1, max_length=40)
    step_id: str | None = Field(default=None, max_length=200)
    comment: str | None = Field(default=None, max_length=2000)
    metadata: dict | None = Field(default=None)


class AgentPlanCompileRequest(BaseModel):
    goal: str = Field(..., min_length=1, max_length=2000)
    data_scope: AgentPlanDataScope | None = Field(default=None)
    answers: dict | None = Field(default=None, description="Clarification answers from prior compile response")


class AgentContextPackRequest(BaseModel):
    db_name: str | None = Field(default=None, max_length=200)
    action_type_id: str | None = Field(default=None, max_length=200)
    max_decisions: int = Field(default=5, ge=0, le=20)
    max_simulations: int = Field(default=5, ge=0, le=20)


class AgentPlanApplyPatchRequest(BaseModel):
    patch: List[PlanPatchOp] = Field(default_factory=list, description="JSON patch operations to apply")


@router.post("/context-pack", response_model=ApiResponse)
@rate_limit(**RateLimitPresets.STRICT)
async def build_context_pack(
    body: AgentContextPackRequest,
    request: Request,
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
    action_logs: ActionLogRegistry = Depends(get_action_log_registry),
) -> ApiResponse:
    payload = sanitize_input(body.model_dump(exclude_none=True))
    actor = (
        request.headers.get("X-User-ID")
        or request.headers.get("X-User")
        or request.headers.get("X-Actor")
        or "system"
    )

    try:
        pack = await build_operational_context_pack(
            db_name=str(payload.get("db_name") or "").strip() or None,
            actor=str(actor),
            action_type_id=str(payload.get("action_type_id") or "").strip() or None,
            action_logs=action_logs,
            tool_registry=tool_registry,
            max_decisions=int(payload.get("max_decisions") or 5),
            max_simulations=int(payload.get("max_simulations") or 5),
        )
    except Exception as exc:
        logger.error("Failed to build operational context pack: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to build context pack") from exc

    return ApiResponse.success(message="Operational context pack built", data=pack)


@router.post("/{plan_id}/apply-patch", response_model=ApiResponse)
async def apply_plan_patch(
    plan_id: str,
    body: AgentPlanApplyPatchRequest,
    request: Request,
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    tenant_id = _resolve_tenant_id(request)
    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent plan not found")

    patch_ops = [op.model_dump(mode="json", by_alias=True) for op in (body.patch or [])]
    if not patch_ops:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="patch must not be empty")

    try:
        patched = apply_json_patch(record.plan, patch_ops)
    except JsonPatchError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    try:
        patched_plan = AgentPlan.model_validate(patched)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Patched plan invalid: {exc}") from exc

    allowed_tool_ids, _selected_model, _allowed_models = await _resolve_tenant_policy_constraints(request)
    validation = await validate_agent_plan(plan=patched_plan, tool_registry=tool_registry, allowed_tool_ids=allowed_tool_ids)
    new_status = "COMPILED" if not validation.errors else "DRAFT"
    await plan_registry.upsert_plan(
        plan_id=plan_id,
        tenant_id=tenant_id,
        status=new_status,
        goal=str(validation.plan.goal or ""),
        risk_level=str(validation.plan.risk_level.value if hasattr(validation.plan.risk_level, "value") else validation.plan.risk_level),
        requires_approval=bool(validation.plan.requires_approval),
        plan=validation.plan.model_dump(mode="json"),
        created_by=record.created_by,
    )

    return ApiResponse.success(
        message="Agent plan patched",
        data={
            "plan_id": plan_id,
            "status": new_status,
            "plan": validation.plan.model_dump(mode="json"),
            "validation_errors": validation.errors,
            "validation_warnings": validation.warnings,
            "compilation_report": validation.compilation_report.model_dump(mode="json"),
        },
    )


@router.post("/compile", response_model=ApiResponse)
@rate_limit(**RateLimitPresets.STRICT)
async def compile_plan(
    body: AgentPlanCompileRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
    action_logs: ActionLogRegistry = Depends(get_action_log_registry),
    model_registry: AgentModelRegistry = Depends(get_agent_model_registry),
) -> ApiResponse:
    payload = sanitize_input(body.model_dump(exclude_none=True))
    goal = str(payload.get("goal") or "").strip()
    data_scope = body.data_scope
    answers = payload.get("answers")
    tenant_id = _resolve_tenant_id(request)
    allowed_tool_ids, selected_model, allowed_models = await _resolve_tenant_policy_constraints(request)
    user = getattr(request.state, "user", None)
    user_id = str(getattr(user, "id", "") or "").strip() or None
    data_policies: Optional[Dict[str, Any]] = None
    with contextlib.suppress(Exception):
        from bff.main import get_agent_policy_registry as _get_agent_policy_registry

        policy_registry = await _get_agent_policy_registry()
        policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
        if policy:
            data_policies = dict(getattr(policy, "data_policies", None) or {})
    actor = (
        request.headers.get("X-User-ID")
        or request.headers.get("X-User")
        or request.headers.get("X-Actor")
        or "system"
    )

    use_native_tool_calling = False
    if bool(get_settings().llm.native_tool_calling) and selected_model:
        with contextlib.suppress(Exception):
            model_record = await model_registry.get_model(model_id=str(selected_model))
            if model_record and bool(getattr(model_record, "supports_native_tool_calling", False)):
                use_native_tool_calling = True

    context_pack: Optional[Dict[str, Any]] = None
    try:
        action_type_hint = None
        if isinstance(answers, dict):
            action_type_hint = (
                answers.get("action_type_id")
                or answers.get("actionTypeId")
                or answers.get("action_type")
            )
        context_pack = await build_operational_context_pack(
            db_name=(data_scope.db_name if data_scope else None),
            actor=str(actor),
            action_type_id=str(action_type_hint) if action_type_hint else None,
            action_logs=action_logs,
            tool_registry=tool_registry,
        )
    except Exception as exc:
        logger.warning("Failed to build operational context pack: %s", exc)
        context_pack = None

    try:
        if data_scope and data_scope.db_name:
            try:
                dataset_registry = await get_dataset_registry()
            except HTTPException as exc:
                logger.warning("Dataset registry unavailable (%s): %s", exc.status_code, exc.detail)
                dataset_registry = None

            if dataset_registry is None:
                raise RuntimeError("Dataset registry unavailable")
            dataset_ids: list[Any] = []
            if data_scope.dataset_id:
                dataset_ids.append(data_scope.dataset_id)
            if isinstance(answers, dict):
                for key in ("dataset_ids", "datasetIds", "dataset_id", "datasetId"):
                    value = answers.get(key)
                    if value in (None, ""):
                        continue
                    if isinstance(value, list):
                        dataset_ids.extend(value)
                    else:
                        dataset_ids.append(value)

            pipeline_pack = await build_pipeline_context_pack(
                db_name=str(data_scope.db_name),
                branch=str(data_scope.branch) if data_scope.branch else None,
                dataset_ids=[str(item) for item in dataset_ids if item not in (None, "")],
                dataset_registry=dataset_registry,
            )
            if context_pack is None:
                context_pack = {}
            context_pack["pipeline_builder"] = pipeline_pack
    except Exception as exc:
        logger.warning("Failed to build pipeline context pack: %s", exc)

    try:
        result = await compile_agent_plan(
            goal=goal,
            data_scope=data_scope,
            answers=answers if isinstance(answers, dict) else None,
            context_pack=context_pack,
            actor=str(actor),
            tenant_id=tenant_id,
            user_id=user_id,
            data_policies=data_policies,
            allowed_tool_ids=allowed_tool_ids,
            selected_model=selected_model,
            allowed_models=allowed_models,
            use_native_tool_calling=use_native_tool_calling,
            tool_registry=tool_registry,
            llm_gateway=llm,
            redis_service=redis_service,
            audit_store=audit_store,
        )
    except LLMQuotaExceededError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)) from exc

    response_data = {
        "status": result.status,
        "plan_id": result.plan_id,
        "plan": result.plan.model_dump(mode="json") if result.plan else None,
        "validation_errors": list(result.validation_errors or []),
        "validation_warnings": list(result.validation_warnings or []),
        "questions": [q.model_dump(mode="json") for q in (result.questions or [])],
        "compilation_report": result.compilation_report.model_dump(mode="json") if result.compilation_report else None,
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
        if result.plan:
            await plan_registry.upsert_plan(
                plan_id=result.plan_id,
                tenant_id=tenant_id,
                status="COMPILED",
                goal=str(result.plan.goal or ""),
                risk_level=str(result.plan.risk_level.value if hasattr(result.plan.risk_level, "value") else result.plan.risk_level),
                requires_approval=bool(result.plan.requires_approval),
                plan=result.plan.model_dump(mode="json"),
                created_by=str(actor),
            )
        return ApiResponse.success(message="Agent plan compiled", data=response_data)
    if result.status == "clarification_required":
        if result.plan:
            await plan_registry.upsert_plan(
                plan_id=result.plan_id,
                tenant_id=tenant_id,
                status="DRAFT",
                goal=str(result.plan.goal or ""),
                risk_level=str(result.plan.risk_level.value if hasattr(result.plan.risk_level, "value") else result.plan.risk_level),
                requires_approval=bool(result.plan.requires_approval),
                plan=result.plan.model_dump(mode="json"),
                created_by=str(actor),
            )
        return ApiResponse.warning(message="Clarification required", data=response_data)

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail={"errors": result.validation_errors, "warnings": result.validation_warnings},
    )


@router.get("/{plan_id}", response_model=ApiResponse)
async def get_plan(
    plan_id: str,
    request: Request,
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    tenant_id = _resolve_tenant_id(request)
    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent plan not found")

    return ApiResponse.success(
        message="Agent plan retrieved",
        data={
            "plan": record.plan,
            "plan_id": record.plan_id,
            "plan_digest": record.plan_digest,
            "status": record.status,
            "created_by": record.created_by,
            "created_at": record.created_at.isoformat(),
            "updated_at": record.updated_at.isoformat(),
        },
    )


@router.post("/{plan_id}/preview", response_model=ApiResponse, status_code=status.HTTP_202_ACCEPTED)
async def preview_plan(
    plan_id: str,
    request: Request,
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    tenant_id = _resolve_tenant_id(request)
    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent plan not found")

    plan = AgentPlan.model_validate(record.plan)
    allowed_tool_ids, _selected_model, _allowed_models = await _resolve_tenant_policy_constraints(request)
    validation = await validate_agent_plan(plan=plan, tool_registry=tool_registry, allowed_tool_ids=allowed_tool_ids)
    if validation.errors:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"errors": validation.errors, "warnings": validation.warnings})

    steps_payload: List[Dict[str, Any]] = []
    for step in validation.plan.steps:
        policy = await tool_registry.get_tool_policy(tool_id=step.tool_id)
        if not policy:
            break
        path = _render_path(policy.path, step.path_params)
        if not _is_preview_safe_step(step.method or policy.method, path, policy.risk_level):
            break
        headers: Dict[str, str] = {}
        if step.idempotency_key:
            headers["Idempotency-Key"] = str(step.idempotency_key)
        steps_payload.append(
            {
                "step_id": step.step_id,
                "tool_id": step.tool_id,
                "service": "bff",
                "method": step.method or policy.method,
                "path": path,
                "query": step.query or {},
                "body": step.body,
                "headers": headers,
                "data_scope": {**(validation.plan.data_scope.model_dump(mode="json") or {}), **(step.data_scope or {})},
                "description": step.description,
                "produces": list(step.produces or []),
                "consumes": list(step.consumes or []),
            }
        )

    if not steps_payload:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No preview-safe steps found in plan")

    run_payload = {
        "goal": validation.plan.goal,
        "steps": steps_payload,
        "context": {
            "plan_id": plan_id,
            "risk_level": str(validation.plan.risk_level.value if hasattr(validation.plan.risk_level, "value") else validation.plan.risk_level),
            "plan_snapshot": validation.plan.model_dump(mode="json"),
        },
        "dry_run": False,
        "request_id": str(uuid4()),
    }
    agent_resp = await _call_agent_create_run(request=request, payload=run_payload)
    return ApiResponse.accepted(message="Agent plan preview started", data=agent_resp.get("data") if isinstance(agent_resp, dict) else None)


@router.post("/{plan_id}/execute", response_model=ApiResponse, status_code=status.HTTP_202_ACCEPTED)
async def execute_plan(
    plan_id: str,
    request: Request,
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
    agent_registry: AgentRegistry = Depends(get_agent_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    tenant_id = _resolve_tenant_id(request)
    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent plan not found")

    plan = AgentPlan.model_validate(record.plan)
    allowed_tool_ids, _selected_model, _allowed_models = await _resolve_tenant_policy_constraints(request)
    validation = await validate_agent_plan(plan=plan, tool_registry=tool_registry, allowed_tool_ids=allowed_tool_ids)
    if validation.errors:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"errors": validation.errors, "warnings": validation.warnings})

    if validation.plan.requires_approval:
        approvals = await agent_registry.list_approvals(plan_id=plan_id, tenant_id=tenant_id)
        if not approvals:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Plan approval required")
        latest = max(approvals, key=lambda a: a.approved_at)
        decision = str(latest.decision or "").strip().upper()
        if decision in _REJECTED_DECISIONS or decision not in _APPROVED_DECISIONS:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Plan approval required")

    steps_payload: List[Dict[str, Any]] = []
    for step in validation.plan.steps:
        policy = await tool_registry.get_tool_policy(tool_id=step.tool_id)
        if not policy:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"tool_id not in allowlist: {step.tool_id}")
        path = _render_path(policy.path, step.path_params)
        headers: Dict[str, str] = {}
        if step.idempotency_key:
            headers["Idempotency-Key"] = str(step.idempotency_key)
        steps_payload.append(
            {
                "step_id": step.step_id,
                "tool_id": step.tool_id,
                "service": "bff",
                "method": step.method or policy.method,
                "path": path,
                "query": step.query or {},
                "body": step.body,
                "headers": headers,
                "data_scope": {**(validation.plan.data_scope.model_dump(mode="json") or {}), **(step.data_scope or {})},
                "description": step.description,
                "produces": list(step.produces or []),
                "consumes": list(step.consumes or []),
            }
        )

    run_payload = {
        "goal": validation.plan.goal,
        "steps": steps_payload,
        "context": {
            "plan_id": plan_id,
            "risk_level": str(validation.plan.risk_level.value if hasattr(validation.plan.risk_level, "value") else validation.plan.risk_level),
            "plan_snapshot": validation.plan.model_dump(mode="json"),
        },
        "dry_run": False,
        "request_id": str(uuid4()),
    }
    agent_resp = await _call_agent_create_run(request=request, payload=run_payload)
    return ApiResponse.accepted(message="Agent plan execution started", data=agent_resp.get("data") if isinstance(agent_resp, dict) else None)


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
                detail={
                    "errors": result.errors,
                    "warnings": result.warnings,
                    "compilation_report": result.compilation_report.model_dump(mode="json"),
                },
            )
        message = "Agent plan validated"
        data = {
            "plan": result.plan.model_dump(mode="json"),
            "warnings": result.warnings,
            "compilation_report": result.compilation_report.model_dump(mode="json"),
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
        tenant_id = _resolve_tenant_id(request)
        record = await agent_registry.create_approval(
            approval_id=str(uuid4()),
            plan_id=plan_id,
            tenant_id=tenant_id,
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
