from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from bff.services.pipeline_context_pack import build_pipeline_context_pack
from bff.services.pipeline_cleansing_agent import apply_cleansing_plan
from bff.services.pipeline_join_agent import select_join_keys
from bff.services.pipeline_join_evaluator import evaluate_pipeline_joins
from bff.services.pipeline_transform_agent import apply_transform_plan, apply_transform_plan_mcp
from bff.services.pipeline_intent_verifier import verify_pipeline_intent
from bff.services.pipeline_task_spec_agent import infer_pipeline_task_spec
from bff.services.pipeline_plan_compiler import (
    PipelinePlanCompileResult,
    compile_pipeline_plan,
    compile_pipeline_plan_mcp,
    repair_pipeline_plan,
    repair_pipeline_plan_mcp,
)
from bff.services.pipeline_plan_autonomous_compiler import compile_pipeline_plan_mcp_autonomous
from bff.services.pipeline_output_splitter import split_pipeline_outputs
from bff.services.pipeline_plan_validation import validate_pipeline_plan
from bff.services.pipeline_spec_generator import generate_pipeline_specs
from bff.dependencies import OMSClientDep
from shared.config.settings import get_settings
from shared.models.pipeline_plan import PipelinePlan, PipelinePlanDataScope
from shared.models.responses import ApiResponse
from shared.security.input_sanitizer import (
    InputSanitizer,
    SecurityViolationError,
    sanitize_input,
    sanitize_label_input,
    validate_db_name,
)
from shared.security.auth_utils import enforce_db_scope
from shared.services.agent_policy_registry import AgentPolicyRegistry
from shared.services.dataset_registry import DatasetRegistry
from shared.services.dataset_profile_registry import DatasetProfileRegistry
from shared.services.pipeline_executor import PipelineExecutor
from shared.services.pipeline_preview_inspector import inspect_preview
from shared.utils.canonical_json import sha256_canonical_json_prefixed
from shared.services.pipeline_plan_registry import PipelinePlanRegistry
from shared.services.pipeline_registry import PipelineRegistry
from shared.dependencies.providers import AuditLogStoreDep, RedisServiceDep, LLMGatewayDep
from shared.services.llm_quota import LLMQuotaExceededError
from shared.services.objectify_registry import ObjectifyRegistry
from bff.services.oms_client import OMSClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pipeline-plans", tags=["Pipeline Plans"])

_PREVIEW_SANITIZER = InputSanitizer()
_SYS_TS_EXPR_RE = re.compile(r"^\s*(?P<col>_sys_ingested_at|_sys_valid_from)\s*=\s*to_timestamp\('.*'\)\s*$")
_CONTEXT_PACK_CACHE_PREFIX = "pipeline-context-pack"
_CONTEXT_PACK_CACHE_TTL_SECONDS = 120
# Join sampling needs to be large enough to avoid false "empty join" results,
# but small enough to keep preview runs lightweight in dev.
_JOIN_SAMPLE_MIN_ROWS = 800
_JOIN_SAMPLE_MULTI_MIN_ROWS = 5000
_PREVIEW_MAX_OUTPUT_ROWS = 20000


def _definition_has_join(definition_json: Dict[str, Any]) -> bool:
    nodes = definition_json.get("nodes")
    if not isinstance(nodes, list):
        return False
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if str(node.get("type") or "").strip().lower() != "transform":
            continue
        metadata = node.get("metadata")
        if not isinstance(metadata, dict):
            continue
        if str(metadata.get("operation") or "").strip().lower() == "join":
            return True
    return False


def _definition_join_count(definition_json: Dict[str, Any]) -> int:
    nodes = definition_json.get("nodes")
    if not isinstance(nodes, list):
        return 0
    count = 0
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if str(node.get("type") or "").strip().lower() != "transform":
            continue
        metadata = node.get("metadata")
        if not isinstance(metadata, dict):
            continue
        if str(metadata.get("operation") or "").strip().lower() == "join":
            count += 1
    return count


async def get_dataset_registry() -> DatasetRegistry:
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


async def get_dataset_profile_registry() -> DatasetProfileRegistry:
    from bff.main import get_dataset_profile_registry as _get_dataset_profile_registry

    return await _get_dataset_profile_registry()


async def get_pipeline_registry() -> PipelineRegistry:
    from bff.main import get_pipeline_registry as _get_pipeline_registry

    return await _get_pipeline_registry()


async def get_pipeline_plan_registry() -> PipelinePlanRegistry:
    from bff.main import get_pipeline_plan_registry as _get_pipeline_plan_registry

    return await _get_pipeline_plan_registry()


async def get_agent_policy_registry() -> AgentPolicyRegistry:
    from bff.main import get_agent_policy_registry as _get_agent_policy_registry

    return await _get_agent_policy_registry()


async def get_objectify_registry() -> ObjectifyRegistry:
    from bff.main import get_objectify_registry as _get_objectify_registry

    return await _get_objectify_registry()


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


def _resolve_actor(request: Request) -> str:
    return (
        request.headers.get("X-User-ID")
        or request.headers.get("X-User")
        or request.headers.get("X-Actor")
        or "system"
    )


def _context_pack_cache_key(payload: Dict[str, Any]) -> str:
    db_name = str(payload.get("db_name") or "default").strip() or "default"
    digest = sha256_canonical_json_prefixed(payload)
    return f"{_CONTEXT_PACK_CACHE_PREFIX}:{db_name}:{digest}"


def _serialize_run_tables(run_tables: Dict[str, Any], *, limit: int) -> Dict[str, Dict[str, Any]]:
    payload: Dict[str, Dict[str, Any]] = {}
    resolved_limit = max(0, int(limit))
    for node_id, table in (run_tables or {}).items():
        if not isinstance(node_id, str) or not node_id.strip():
            continue
        if not isinstance(table, dict):
            continue
        columns = table.get("columns")
        rows = table.get("rows")
        if not isinstance(columns, list) or not isinstance(rows, list):
            continue
        payload[node_id] = {
            "columns": columns,
            "rows": rows[:resolved_limit] if resolved_limit else rows,
        }
    return payload


def _normalize_definition_for_digest(definition_json: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(definition_json, dict):
        return {}
    normalized = dict(definition_json)
    if "__preview_meta__" in normalized:
        normalized.pop("__preview_meta__", None)
    nodes_raw = normalized.get("nodes")
    if not isinstance(nodes_raw, list):
        return normalized
    updated = False
    nodes: List[Any] = []
    for node in nodes_raw:
        if not isinstance(node, dict):
            nodes.append(node)
            continue
        metadata = node.get("metadata")
        if not isinstance(metadata, dict):
            nodes.append(node)
            continue
        operation = str(metadata.get("operation") or "").strip().lower()
        if operation != "compute":
            nodes.append(node)
            continue
        expression = metadata.get("expression")
        if not isinstance(expression, str):
            nodes.append(node)
            continue
        match = _SYS_TS_EXPR_RE.match(expression)
        if not match:
            nodes.append(node)
            continue
        col = match.group("col")
        next_metadata = dict(metadata)
        next_metadata["expression"] = f"{col} = to_timestamp('__SYS_TIME__')"
        next_node = dict(node)
        next_node["metadata"] = next_metadata
        nodes.append(next_node)
        updated = True
    if updated:
        normalized["nodes"] = nodes
    return normalized


def _definition_digest(definition_json: Dict[str, Any]) -> str:
    normalized = _normalize_definition_for_digest(definition_json)
    return sha256_canonical_json_prefixed(normalized)


def _sanitize_label_dict_with_limits(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise SecurityViolationError(f"Expected dict, got {type(payload)}")
    if len(payload) > _PREVIEW_SANITIZER.max_dict_keys:
        raise SecurityViolationError(
            f"Too many keys in dict: {len(payload)} > {_PREVIEW_SANITIZER.max_dict_keys}"
        )
    return _PREVIEW_SANITIZER.sanitize_label_dict(payload)


def _sanitize_preview_columns(columns: list[Any]) -> list[Any]:
    if not isinstance(columns, list):
        raise SecurityViolationError(f"Expected list, got {type(columns)}")
    if len(columns) > _PREVIEW_SANITIZER.max_list_items:
        raise SecurityViolationError(
            f"Too many items in list: {len(columns)} > {_PREVIEW_SANITIZER.max_list_items}"
        )
    sanitized: list[Any] = []
    for col in columns:
        if isinstance(col, dict):
            sanitized.append(sanitize_input(col))
        elif isinstance(col, str):
            sanitized.append(_PREVIEW_SANITIZER.sanitize_label_key(col))
        else:
            sanitized.append(_PREVIEW_SANITIZER.sanitize_any(col))
    return sanitized


def _sanitize_preview_rows(rows: list[Any]) -> list[Any]:
    if not isinstance(rows, list):
        raise SecurityViolationError(f"Expected list, got {type(rows)}")
    if len(rows) > _PREVIEW_SANITIZER.max_list_items:
        raise SecurityViolationError(
            f"Too many items in list: {len(rows)} > {_PREVIEW_SANITIZER.max_list_items}"
        )
    sanitized: list[Any] = []
    for row in rows:
        if isinstance(row, dict):
            sanitized.append(_sanitize_label_dict_with_limits(row))
        else:
            sanitized.append(_PREVIEW_SANITIZER.sanitize_any(row))
    return sanitized


def _sanitize_preview_table(table: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(table, dict):
        raise SecurityViolationError(f"Expected dict, got {type(table)}")
    if len(table) > _PREVIEW_SANITIZER.max_dict_keys:
        raise SecurityViolationError(
            f"Too many keys in dict: {len(table)} > {_PREVIEW_SANITIZER.max_dict_keys}"
        )
    sanitized: Dict[str, Any] = {}
    for key, value in table.items():
        if not isinstance(key, str):
            raise SecurityViolationError("Table keys must be strings")
        clean_key = _PREVIEW_SANITIZER.sanitize_field_name(key)
        if key == "columns" and isinstance(value, list):
            sanitized[clean_key] = _sanitize_preview_columns(value)
        elif key == "rows" and isinstance(value, list):
            sanitized[clean_key] = _sanitize_preview_rows(value)
        elif key in {"column_stats", "cast_stats"} and isinstance(value, dict):
            sanitized[clean_key] = _sanitize_label_dict_with_limits(value)
        else:
            sanitized[clean_key] = _PREVIEW_SANITIZER.sanitize_any(value)
    return sanitized


def _sanitize_preview_tables(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if not isinstance(payload, dict):
        raise SecurityViolationError(f"Expected dict, got {type(payload)}")
    if len(payload) > _PREVIEW_SANITIZER.max_dict_keys:
        raise SecurityViolationError(
            f"Too many keys in dict: {len(payload)} > {_PREVIEW_SANITIZER.max_dict_keys}"
        )
    sanitized: Dict[str, Dict[str, Any]] = {}
    for key, value in payload.items():
        if not isinstance(key, str):
            raise SecurityViolationError("Preview table keys must be strings")
        clean_key = _PREVIEW_SANITIZER.sanitize_label_key(key)
        sanitized[clean_key] = _sanitize_preview_table(value)
    return sanitized


async def _resolve_tenant_policy(
    request: Request,
) -> tuple[Optional[str], Optional[list[str]], Optional[Dict[str, Any]]]:
    tenant_id = _resolve_tenant_id(request)
    try:
        policy_registry = await get_agent_policy_registry()
        policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
    except Exception:
        return None, None, None

    if not policy:
        return None, None, None

    allowed_models = [str(m).strip() for m in (policy.allowed_models or []) if str(m).strip()]
    if policy.default_model:
        allowed_models.append(str(policy.default_model).strip())
    allowed_models = [m for m in allowed_models if m]
    allowed_models_final = list(dict.fromkeys(allowed_models)) or None

    selected_model: Optional[str] = None
    if policy.default_model:
        selected_model = str(policy.default_model).strip() or None
    if selected_model is None and allowed_models_final:
        selected_model = str(allowed_models_final[0]).strip() or None

    data_policies = dict(getattr(policy, "data_policies", None) or {})
    return selected_model, allowed_models_final, data_policies


class PipelinePlanCompileRequest(BaseModel):
    goal: str = Field(..., min_length=1, max_length=2000)
    data_scope: PipelinePlanDataScope | None = Field(default=None)
    answers: dict | None = Field(default=None)
    planner_hints: dict | None = Field(default=None)
    task_spec: dict | None = Field(default=None)


class PipelineJoinKeysRequest(BaseModel):
    goal: str = Field(..., min_length=1, max_length=2000)
    data_scope: PipelinePlanDataScope | None = Field(default=None)
    context_pack: dict | None = Field(default=None)
    max_joins: int = Field(default=4, ge=0, le=12)
    feedback: dict | None = Field(default=None)


class PipelinePlanPreviewRequest(BaseModel):
    node_id: str | None = Field(default=None, max_length=200)
    limit: int = Field(default=200, ge=1, le=200)
    include_run_tables: bool = Field(default=False)
    run_table_limit: int = Field(default=200, ge=1, le=1000)


class PipelinePlanRepairRequest(BaseModel):
    validation_errors: list[str] = Field(default_factory=list)
    validation_warnings: list[str] = Field(default_factory=list)
    preflight: dict | None = Field(default=None)
    preview: dict | None = Field(default=None)


class PipelineContextPackRequest(BaseModel):
    db_name: str = Field(..., min_length=1, max_length=200)
    branch: str | None = Field(default=None, max_length=200)
    dataset_ids: list[str] | None = Field(default=None)
    max_datasets_overview: int = Field(default=20, ge=1, le=100)
    max_selected_datasets: int = Field(default=6, ge=1, le=20)
    max_sample_rows: int = Field(default=20, ge=1, le=200)
    max_join_candidates: int = Field(default=10, ge=1, le=50)
    max_pk_candidates: int = Field(default=6, ge=1, le=20)


class PipelineOutputBinding(BaseModel):
    dataset_id: str = Field(..., min_length=1, max_length=200)
    dataset_version_id: str | None = Field(default=None, max_length=200)
    dataset_branch: str | None = Field(default=None, max_length=200)
    artifact_output_name: str | None = Field(default=None, max_length=200)
    output_kind: str | None = Field(default=None, max_length=20)
    target_class_id: str | None = Field(default=None, max_length=200)
    source_class_id: str | None = Field(default=None, max_length=200)
    link_type_id: str | None = Field(default=None, max_length=200)
    predicate: str | None = Field(default=None, max_length=200)
    cardinality: str | None = Field(default=None, max_length=40)
    source_key_column: str | None = Field(default=None, max_length=200)
    target_key_column: str | None = Field(default=None, max_length=200)
    relationship_spec_type: str | None = Field(default=None, max_length=40)


class PipelinePlanGenerateSpecsRequest(BaseModel):
    apply: bool = Field(default=False)
    output_bindings: Dict[str, PipelineOutputBinding] | None = Field(default=None)
    output_previews: Dict[str, Dict[str, Any]] | None = Field(default=None)
    auto_sync: bool = Field(default=True)
    ontology_branch: str | None = Field(default=None, max_length=200)
    dangling_policy: str = Field(default="FAIL")
    dedupe_policy: str = Field(default="DEDUP")


class PipelineTaskSpecRequest(BaseModel):
    goal: str = Field(..., min_length=1, max_length=2000)
    data_scope: PipelinePlanDataScope | None = Field(default=None)
    context_pack: dict | None = Field(default=None)


class PipelineScopeClarifyRequest(BaseModel):
    goal: str = Field(..., min_length=1, max_length=2000)
    data_scope: PipelinePlanDataScope | None = Field(default=None)
    task_spec: dict | None = Field(default=None)
    intent_status: str = Field(default="unknown", max_length=40)
    missing_requirements: list[str] | None = Field(default=None)
    suggested_actions: list[str] | None = Field(default=None)
    intent_warnings: list[str] | None = Field(default=None)
    context_pack: dict | None = Field(default=None)


class PipelinePlanSplitOutputsRequest(BaseModel):
    output_bindings: Dict[str, PipelineOutputBinding] | None = Field(default=None)


class PipelinePlanInspectPreviewRequest(BaseModel):
    preview: Dict[str, Any] | None = Field(default=None)
    node_id: str | None = Field(default=None, max_length=200)
    limit: int = Field(default=200, ge=1, le=200)


class PipelinePlanCleanseRequest(BaseModel):
    preview: Dict[str, Any] | None = Field(default=None)
    inspector: Dict[str, Any] | None = Field(default=None)
    max_actions: int = Field(default=6, ge=0, le=20)
    planner_hints: Dict[str, Any] | None = Field(default=None)


class PipelinePlanTransformRequest(BaseModel):
    join_plan: list[dict[str, Any]] | None = Field(default=None)
    cleansing_hints: list[dict[str, Any]] | None = Field(default=None)
    context_pack: dict | None = Field(default=None)
    planner_hints: dict | None = Field(default=None)


class PipelinePlanIntentCheckRequest(BaseModel):
    join_plan: list[dict[str, Any]] | None = Field(default=None)
    cleansing_hints: list[dict[str, Any]] | None = Field(default=None)
    context_pack: dict | None = Field(default=None)


class PipelinePlanEvaluateJoinsRequest(BaseModel):
    node_id: str | None = Field(default=None, max_length=200)
    run_tables: Dict[str, Dict[str, Any]] | None = Field(default=None)
    definition_digest: str | None = Field(default=None, max_length=200)


def _plan_outputs_need_split(plan: PipelinePlan) -> bool:
    outputs = list(plan.outputs or [])
    if not outputs:
        return True
    for output in outputs:
        kind = str(getattr(output.output_kind, "value", output.output_kind) or "unknown").strip().lower()
        if kind == "object":
            if not output.target_class_id:
                return True
            continue
        if kind == "link":
            missing = [
                name
                for name in (
                    "link_type_id",
                    "source_class_id",
                    "target_class_id",
                    "predicate",
                    "cardinality",
                    "source_key_column",
                    "target_key_column",
                    "relationship_spec_type",
                )
                if not getattr(output, name, None)
            ]
            if missing:
                return True
            continue
        return True
    return False


@router.post("/compile", response_model=ApiResponse)
async def compile_plan(
    body: PipelinePlanCompileRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    profile_registry: DatasetProfileRegistry = Depends(get_dataset_profile_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    if not bool(get_settings().pipeline_plan.llm_enabled):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Pipeline planner is disabled")

    raw_payload = body.model_dump(exclude_none=True)
    raw_answers = raw_payload.pop("answers", None)
    payload = sanitize_input(raw_payload)
    goal = str(payload.get("goal") or "").strip()
    data_scope = body.data_scope
    answers = sanitize_label_input(raw_answers) if isinstance(raw_answers, dict) else None
    planner_hints = payload.get("planner_hints") if isinstance(payload.get("planner_hints"), dict) else None
    task_spec = payload.get("task_spec") if isinstance(payload.get("task_spec"), dict) else None
    if not data_scope or not data_scope.db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="data_scope.db_name is required")

    tenant_id = _resolve_tenant_id(request)
    actor = _resolve_actor(request)
    user = getattr(request.state, "user", None)
    user_id = str(getattr(user, "id", "") or "").strip() or None

    selected_model, allowed_models, data_policies = await _resolve_tenant_policy(request)

    db_name = validate_db_name(str(data_scope.db_name))
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    branch = str(data_scope.branch) if data_scope and data_scope.branch else None

    context_pack = None
    try:
        dataset_ids = list(data_scope.dataset_ids) if data_scope else []
        context_pack = await build_pipeline_context_pack(
            db_name=str(data_scope.db_name) if data_scope and data_scope.db_name else "",
            branch=str(data_scope.branch) if data_scope and data_scope.branch else None,
            dataset_ids=dataset_ids or None,
            dataset_registry=dataset_registry,
            profile_registry=profile_registry,
        )
    except Exception as exc:
        logger.warning("Failed to build pipeline context pack: %s", exc)
        context_pack = None

    try:
        if bool(get_settings().pipeline_plan.mcp_autonomous_enabled):
            compiler = compile_pipeline_plan_mcp_autonomous
        elif bool(get_settings().pipeline_plan.mcp_planner_enabled):
            compiler = compile_pipeline_plan_mcp
        else:
            compiler = compile_pipeline_plan
        result: PipelinePlanCompileResult = await compiler(
            goal=goal,
            data_scope=data_scope,
            answers=answers,
            context_pack=context_pack,
            planner_hints=planner_hints,
            task_spec=task_spec,
            actor=actor,
            tenant_id=tenant_id,
            user_id=user_id,
            data_policies=data_policies,
            selected_model=selected_model,
            allowed_models=allowed_models,
            llm_gateway=llm,
            redis_service=redis_service,
            audit_store=audit_store,
            dataset_registry=dataset_registry,
        )
    except LLMQuotaExceededError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Pipeline plan compile failed: %s", exc)
        raise

    compiled_plan = result.plan
    validation_errors = list(result.validation_errors or [])
    validation_warnings = list(result.validation_warnings or [])
    response_status = result.status
    output_split_info: dict | None = None

    allow_output_split = True
    if isinstance(task_spec, dict):
        allow_output_split = bool(task_spec.get("allow_specs"))

    if compiled_plan and response_status == "success" and _plan_outputs_need_split(compiled_plan) and allow_output_split:
        try:
            split_result = await split_pipeline_outputs(
                plan=compiled_plan,
                output_bindings=None,
                context_pack=context_pack,
                actor=actor,
                tenant_id=tenant_id,
                user_id=user_id,
                data_policies=data_policies,
                selected_model=selected_model,
                allowed_models=allowed_models,
                llm_gateway=llm,
                redis_service=redis_service,
                audit_store=audit_store,
            )
        except LLMQuotaExceededError as exc:
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)) from exc
        except Exception as exc:
            logger.error("Pipeline output split failed: %s", exc)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to split outputs") from exc

        compiled_plan = compiled_plan.model_copy(update={"outputs": split_result.outputs})
        validation = await validate_pipeline_plan(
            plan=compiled_plan,
            dataset_registry=dataset_registry,
            db_name=db_name,
            branch=branch,
            require_output=True,
            context_pack=context_pack,
        )
        compiled_plan = validation.plan
        validation_errors = list(validation.errors or [])
        validation_warnings = list(validation.warnings or [])
        if split_result.warnings:
            validation_warnings.extend([f"output_split: {item}" for item in split_result.warnings])
        if split_result.notes:
            validation_warnings.extend([f"output_split_note: {item}" for item in split_result.notes])
        output_split_info = {
            "confidence": split_result.confidence,
            "notes": split_result.notes,
            "warnings": split_result.warnings,
        }
        if validation_errors:
            response_status = "clarification_required"

    response_data = {
        "status": response_status,
        "plan_id": result.plan_id,
        "plan": compiled_plan.model_dump(mode="json") if compiled_plan else None,
        "validation_errors": validation_errors,
        "validation_warnings": validation_warnings,
        "questions": [q.model_dump(mode="json") for q in (result.questions or [])],
        "compilation_report": result.compilation_report.model_dump(mode="json") if result.compilation_report else None,
        "preflight": result.preflight,
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
        "output_split": output_split_info,
    }

    if compiled_plan:
        status_value = "COMPILED" if response_status == "success" else "DRAFT"
        await plan_registry.upsert_plan(
            plan_id=result.plan_id,
            tenant_id=tenant_id,
            status=status_value,
            goal=str(compiled_plan.goal or ""),
            db_name=str(compiled_plan.data_scope.db_name or "") if compiled_plan.data_scope else None,
            branch=str(compiled_plan.data_scope.branch or "") if compiled_plan.data_scope else None,
            plan=compiled_plan.model_dump(mode="json"),
            created_by=actor,
        )

    return ApiResponse.success(message="Pipeline plan compiled", data=response_data)


@router.post("/task-spec", response_model=ApiResponse)
async def infer_task_spec(
    body: PipelineTaskSpecRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    profile_registry: DatasetProfileRegistry = Depends(get_dataset_profile_registry),
) -> ApiResponse:
    payload = sanitize_input(body.model_dump(exclude_none=True))
    goal = str(payload.get("goal") or "").strip()
    data_scope = body.data_scope
    context_pack = body.context_pack if isinstance(body.context_pack, dict) else None
    if not data_scope or not data_scope.db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="data_scope.db_name is required")

    tenant_id = _resolve_tenant_id(request)
    actor = _resolve_actor(request)
    user = getattr(request.state, "user", None)
    user_id = str(getattr(user, "id", "") or "").strip() or None

    selected_model, allowed_models, data_policies = await _resolve_tenant_policy(request)

    db_name = validate_db_name(str(data_scope.db_name))
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    if context_pack is None:
        try:
            dataset_ids = list(data_scope.dataset_ids) if data_scope else []
            context_pack = await build_pipeline_context_pack(
                db_name=str(data_scope.db_name) if data_scope and data_scope.db_name else "",
                branch=str(data_scope.branch) if data_scope and data_scope.branch else None,
                dataset_ids=dataset_ids or None,
                dataset_registry=dataset_registry,
                profile_registry=profile_registry,
            )
        except Exception as exc:
            logger.warning("Failed to build pipeline context pack: %s", exc)
            context_pack = {}

    try:
        result = await infer_pipeline_task_spec(
            goal=goal,
            data_scope=data_scope,
            context_pack=context_pack,
            actor=actor,
            tenant_id=tenant_id,
            user_id=user_id,
            data_policies=data_policies,
            selected_model=selected_model,
            allowed_models=allowed_models,
            llm_gateway=llm,
            redis_service=redis_service,
            audit_store=audit_store,
        )
    except LLMQuotaExceededError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Pipeline task spec inference failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to infer task spec") from exc

    response_data = {
        "status": result.status,
        "task_spec": result.task_spec.model_dump(mode="json") if result.task_spec else None,
        "questions": list(result.questions or []),
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
    return ApiResponse.success(message="Pipeline task spec ready", data=response_data)


@router.post("/clarify-scope", response_model=ApiResponse)
async def clarify_scope(
    body: PipelineScopeClarifyRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    profile_registry: DatasetProfileRegistry = Depends(get_dataset_profile_registry),
) -> ApiResponse:
    if not bool(get_settings().pipeline_plan.llm_enabled):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Pipeline planner is disabled")

    payload = sanitize_input(body.model_dump(exclude_none=True))
    goal = str(payload.get("goal") or "").strip()
    task_spec = payload.get("task_spec") if isinstance(payload.get("task_spec"), dict) else None
    intent_feedback = {
        "status": str(payload.get("intent_status") or "").strip(),
        "missing_requirements": payload.get("missing_requirements") if isinstance(payload.get("missing_requirements"), list) else [],
        "suggested_actions": payload.get("suggested_actions") if isinstance(payload.get("suggested_actions"), list) else [],
        "warnings": payload.get("intent_warnings") if isinstance(payload.get("intent_warnings"), list) else [],
    }
    data_scope = body.data_scope
    context_pack = body.context_pack if isinstance(body.context_pack, dict) else None
    if not data_scope or not data_scope.db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="data_scope.db_name is required")

    tenant_id = _resolve_tenant_id(request)
    actor = _resolve_actor(request)
    user = getattr(request.state, "user", None)
    user_id = str(getattr(user, "id", "") or "").strip() or None

    selected_model, allowed_models, data_policies = await _resolve_tenant_policy(request)

    db_name = validate_db_name(str(data_scope.db_name))
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    if context_pack is None:
        try:
            dataset_ids = list(data_scope.dataset_ids) if data_scope else []
            context_pack = await build_pipeline_context_pack(
                db_name=str(data_scope.db_name) if data_scope and data_scope.db_name else "",
                branch=str(data_scope.branch) if data_scope and data_scope.branch else None,
                dataset_ids=dataset_ids or None,
                dataset_registry=dataset_registry,
                profile_registry=profile_registry,
            )
        except Exception as exc:
            logger.warning("Failed to build pipeline context pack: %s", exc)
            context_pack = None

    try:
        from bff.services.pipeline_scope_clarifier import clarify_pipeline_scope

        result = await clarify_pipeline_scope(
            goal=goal,
            task_spec=task_spec,
            intent_feedback=intent_feedback,
            context_pack=context_pack,
            actor=actor,
            tenant_id=tenant_id,
            user_id=user_id,
            data_policies=data_policies,
            selected_model=selected_model,
            allowed_models=allowed_models,
            llm_gateway=llm,
            redis_service=redis_service,
            audit_store=audit_store,
        )
    except LLMQuotaExceededError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Pipeline scope clarification failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to clarify scope") from exc

    response_data = {
        "status": result.status,
        "questions": [q.model_dump(mode="json") for q in (result.questions or [])],
        "notes": list(result.notes or []),
        "warnings": list(result.warnings or []),
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
    return ApiResponse.success(message="Pipeline scope clarification ready", data=response_data)


@router.post("/join-keys", response_model=ApiResponse)
async def join_keys(
    body: PipelineJoinKeysRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    profile_registry: DatasetProfileRegistry = Depends(get_dataset_profile_registry),
) -> ApiResponse:
    if not bool(get_settings().pipeline_plan.llm_enabled):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Pipeline planner is disabled")

    payload = sanitize_input(
        {
            "goal": body.goal,
            "data_scope": body.data_scope.model_dump(mode="json") if body.data_scope else None,
            "max_joins": body.max_joins,
        }
    )
    goal = str(payload.get("goal") or "").strip()
    data_scope = body.data_scope
    context_pack = body.context_pack if isinstance(body.context_pack, dict) else None
    if not data_scope or not data_scope.db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="data_scope.db_name is required")

    tenant_id = _resolve_tenant_id(request)
    actor = _resolve_actor(request)
    user = getattr(request.state, "user", None)
    user_id = str(getattr(user, "id", "") or "").strip() or None

    selected_model, allowed_models, data_policies = await _resolve_tenant_policy(request)

    db_name = validate_db_name(str(data_scope.db_name))
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    if context_pack is None:
        try:
            dataset_ids = list(data_scope.dataset_ids) if data_scope else []
            context_pack = await build_pipeline_context_pack(
                db_name=str(data_scope.db_name) if data_scope and data_scope.db_name else "",
                branch=str(data_scope.branch) if data_scope and data_scope.branch else None,
                dataset_ids=dataset_ids or None,
                dataset_registry=dataset_registry,
                profile_registry=profile_registry,
            )
        except Exception as exc:
            logger.warning("Failed to build pipeline context pack: %s", exc)
            context_pack = {}

    try:
        feedback = sanitize_input(body.feedback) if isinstance(body.feedback, dict) else None
        result = await select_join_keys(
            goal=goal,
            context_pack=context_pack or {},
            max_joins=int(payload.get("max_joins") or 0),
            feedback=feedback,
            actor=actor,
            tenant_id=tenant_id,
            user_id=user_id,
            data_policies=data_policies,
            selected_model=selected_model,
            allowed_models=allowed_models,
            llm_gateway=llm,
            redis_service=redis_service,
            audit_store=audit_store,
        )
    except LLMQuotaExceededError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Pipeline join selection failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to select join keys") from exc

    response_data = {
        "joins": [item.model_dump(mode="json") for item in result.joins],
        "confidence": result.confidence,
        "notes": result.notes,
        "warnings": result.warnings,
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

    return ApiResponse.success(message="Join keys selected", data=response_data)


@router.post("/context-pack", response_model=ApiResponse)
async def build_context_pack(
    body: PipelineContextPackRequest,
    request: Request,
    redis_service: RedisServiceDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    profile_registry: DatasetProfileRegistry = Depends(get_dataset_profile_registry),
) -> ApiResponse:
    payload = sanitize_input(body.model_dump(exclude_none=True))
    db_name = validate_db_name(str(payload.get("db_name") or "").strip())
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    cache_key: Optional[str] = None
    if redis_service:
        try:
            cache_key = _context_pack_cache_key(payload)
            cached = await redis_service.client.get(cache_key)
            if cached:
                if isinstance(cached, (bytes, bytearray)):
                    cached = cached.decode("utf-8", "ignore")
                cached_pack = json.loads(str(cached))
                if isinstance(cached_pack, dict):
                    return ApiResponse.success(message="Pipeline context pack cached", data=cached_pack)
        except Exception as exc:
            logger.warning("Context pack cache read failed: %s", exc)

    try:
        pack = await build_pipeline_context_pack(
            db_name=db_name,
            branch=str(payload.get("branch") or "").strip() or None,
            dataset_ids=payload.get("dataset_ids"),
            dataset_registry=dataset_registry,
            profile_registry=profile_registry,
            max_datasets_overview=int(payload.get("max_datasets_overview") or 20),
            max_selected_datasets=int(payload.get("max_selected_datasets") or 6),
            max_sample_rows=int(payload.get("max_sample_rows") or 20),
            max_join_candidates=int(payload.get("max_join_candidates") or 10),
            max_pk_candidates=int(payload.get("max_pk_candidates") or 6),
        )
    except Exception as exc:
        logger.error("Failed to build pipeline context pack: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to build context pack") from exc

    if cache_key:
        try:
            await redis_service.client.set(
                cache_key,
                json.dumps(pack, ensure_ascii=False),
                ex=_CONTEXT_PACK_CACHE_TTL_SECONDS,
            )
        except Exception as exc:
            logger.warning("Context pack cache write failed: %s", exc)

    return ApiResponse.success(message="Pipeline context pack built", data=pack)


@router.get("/{plan_id}", response_model=ApiResponse)
async def get_plan(
    plan_id: str,
    request: Request,
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    tenant_id = _resolve_tenant_id(request)
    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline plan not found")

    return ApiResponse.success(message="Pipeline plan fetched", data={"plan": record.plan})


@router.post("/{plan_id}/preview", response_model=ApiResponse)
async def preview_plan(
    plan_id: str,
    body: PipelinePlanPreviewRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    tenant_id = _resolve_tenant_id(request)
    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline plan not found")

    try:
        plan = PipelinePlan.model_validate(record.plan)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Stored plan invalid: {exc}") from exc
    if not plan.data_scope.db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")

    db_name = str(plan.data_scope.db_name or "").strip()
    if not db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    validation = await validate_pipeline_plan(
        plan=plan,
        dataset_registry=dataset_registry,
        db_name=db_name,
        branch=str(plan.data_scope.branch or "") or None,
        require_output=True,
    )

    if validation.errors:
        return ApiResponse.warning(
            message="Pipeline plan invalid",
            data={
                "validation_errors": validation.errors,
                "validation_warnings": validation.warnings,
                "preflight": validation.preflight,
            },
        )

    node_id = str(body.node_id or "").strip() or None
    limit = int(body.limit or 200)
    include_run_tables = bool(body.include_run_tables)
    run_table_limit = int(body.run_table_limit or limit)

    preview_definition = dict(validation.plan.definition_json)
    preview_meta = dict(preview_definition.get("__preview_meta__") or {})
    preview_meta.setdefault("branch", str(plan.data_scope.branch or "") or "main")
    sample_limit = max(limit, run_table_limit) if include_run_tables else limit
    # Join previews are prone to "empty join" false-negatives when sampling the
    # first N rows from each input independently. Use a larger input sample so
    # join coverage evaluation is meaningful for typical PK/FK joins.
    join_count = _definition_join_count(preview_definition)
    if join_count:
        sample_limit = max(int(sample_limit or 0), _JOIN_SAMPLE_MIN_ROWS)
        if join_count > 1:
            # Multi-join pipelines amplify the independent-sampling problem; bump the input sample
            # to make it more likely that chained joins produce non-empty previews.
            #
            # Scale with the number of joins (join chain length). Cap to keep previews bounded.
            target = _JOIN_SAMPLE_MULTI_MIN_ROWS * max(1, join_count - 1)
            sample_limit = max(int(sample_limit or 0), min(target, 20000))
        # Safety valve: cap join outputs during preview to avoid OOM on bad join keys.
        preview_meta.setdefault("max_output_rows", _PREVIEW_MAX_OUTPUT_ROWS)
    if sample_limit:
        preview_meta["sample_limit"] = sample_limit
    preview_definition["__preview_meta__"] = preview_meta

    actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
    storage_service = None
    try:
        storage_service = await pipeline_registry.get_lakefs_storage(user_id=actor_user_id)
    except Exception as exc:
        logger.warning("Failed to init lakeFS storage for pipeline preview: %s", exc)
        storage_service = None

    executor = PipelineExecutor(dataset_registry, pipeline_registry=pipeline_registry, storage_service=storage_service)
    definition_digest = _definition_digest(preview_definition)
    run_tables_payload: Dict[str, Dict[str, Any]] | None = None
    try:
        if include_run_tables:
            run_result = await executor.run(definition=preview_definition, db_name=db_name)
            selected = executor._select_table(run_result, node_id)
            preview = executor._table_to_sample(selected, limit=limit)
            raw_tables = {
                table_node_id: {"columns": table.columns, "rows": table.rows}
                for table_node_id, table in run_result.tables.items()
            }
            run_tables_payload = _serialize_run_tables(raw_tables, limit=run_table_limit)
        else:
            preview = await executor.preview(
                definition=preview_definition,
                db_name=db_name,
                node_id=node_id,
                limit=limit,
            )
    except ValueError as exc:
        return ApiResponse.warning(
            message="Pipeline preview failed",
            data={
                "preflight": validation.preflight,
                "error": str(exc),
            },
        )

    data: Dict[str, Any] = {
        "preflight": validation.preflight,
        "preview": preview,
        "definition_digest": definition_digest,
    }
    if run_tables_payload is not None:
        data["run_tables"] = run_tables_payload

    return ApiResponse.success(message="Pipeline preview ready", data=data)


@router.post("/{plan_id}/inspect-preview", response_model=ApiResponse)
async def inspect_plan_preview(
    plan_id: str,
    body: PipelinePlanInspectPreviewRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    tenant_id = _resolve_tenant_id(request)
    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline plan not found")

    try:
        plan = PipelinePlan.model_validate(record.plan)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Stored plan invalid: {exc}") from exc
    if not plan.data_scope.db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")

    db_name = str(plan.data_scope.db_name or "").strip()
    if not db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    preview_payload = body.preview if isinstance(body.preview, dict) else None
    preflight = None
    if preview_payload is None:
        validation = await validate_pipeline_plan(
            plan=plan,
            dataset_registry=dataset_registry,
            db_name=db_name,
            branch=str(plan.data_scope.branch or "") or None,
            require_output=True,
        )
        preflight = validation.preflight
        if validation.errors:
            return ApiResponse.warning(
                message="Pipeline plan invalid",
                data={
                    "validation_errors": validation.errors,
                    "validation_warnings": validation.warnings,
                    "preflight": validation.preflight,
                },
            )

        node_id = str(body.node_id or "").strip() or None
        limit = int(body.limit or 200)
        preview_definition = dict(validation.plan.definition_json)
        preview_meta = dict(preview_definition.get("__preview_meta__") or {})
        preview_meta.setdefault("branch", str(plan.data_scope.branch or "") or "main")
        if _definition_has_join(preview_definition):
            preview_meta["sample_limit"] = max(limit, _JOIN_SAMPLE_MIN_ROWS)
        preview_definition["__preview_meta__"] = preview_meta
        actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
        storage_service = None
        try:
            storage_service = await pipeline_registry.get_lakefs_storage(user_id=actor_user_id)
        except Exception as exc:
            logger.warning("Failed to init lakeFS storage for pipeline preview inspection: %s", exc)
            storage_service = None
        executor = PipelineExecutor(dataset_registry, pipeline_registry=pipeline_registry, storage_service=storage_service)
        preview_payload = await executor.preview(
            definition=preview_definition,
            db_name=db_name,
            node_id=node_id,
            limit=limit,
        )

    inspector = inspect_preview(preview_payload or {})
    return ApiResponse.success(
        message="Pipeline preview inspected",
        data={
            "preflight": preflight,
            "inspector": inspector,
        },
    )


@router.post("/{plan_id}/evaluate-joins", response_model=ApiResponse)
async def evaluate_joins(
    plan_id: str,
    body: PipelinePlanEvaluateJoinsRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    tenant_id = _resolve_tenant_id(request)
    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline plan not found")

    try:
        plan = PipelinePlan.model_validate(record.plan)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Stored plan invalid: {exc}") from exc
    if not plan.data_scope.db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")

    db_name = str(plan.data_scope.db_name or "").strip()
    if not db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    validation = await validate_pipeline_plan(
        plan=plan,
        dataset_registry=dataset_registry,
        db_name=db_name,
        branch=str(plan.data_scope.branch or "") or None,
        require_output=True,
    )
    if validation.errors:
        return ApiResponse.warning(
            message="Pipeline plan invalid",
            data={
                "validation_errors": validation.errors,
                "validation_warnings": validation.warnings,
                "preflight": validation.preflight,
            },
        )

    raw_payload = body.model_dump(exclude_none=True)
    run_tables_raw = raw_payload.pop("run_tables", None)
    payload = sanitize_input(raw_payload)
    run_tables = _sanitize_preview_tables(run_tables_raw) if isinstance(run_tables_raw, dict) else None
    definition_digest = str(payload.get("definition_digest") or "").strip() or None
    expected_digest = _definition_digest(validation.plan.definition_json)
    digest_warnings: List[str] = []
    if run_tables is not None and definition_digest:
        if definition_digest != expected_digest:
            digest_warnings.append("run_tables ignored due to definition_digest mismatch")
            run_tables = None
    elif run_tables is not None and not definition_digest:
        digest_warnings.append("run_tables provided without definition_digest; skipping digest check")

    actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
    storage_service = None
    try:
        storage_service = await pipeline_registry.get_lakefs_storage(user_id=actor_user_id)
    except Exception as exc:
        logger.warning("Failed to init lakeFS storage for join evaluation: %s", exc)
        storage_service = None

    evaluations, warnings = await evaluate_pipeline_joins(
        definition_json={
            **dict(validation.plan.definition_json),
            "__preview_meta__": {
                **dict((validation.plan.definition_json or {}).get("__preview_meta__") or {}),
                "branch": str(plan.data_scope.branch or "") or "main",
            },
        },
        db_name=db_name,
        dataset_registry=dataset_registry,
        node_filter=str(body.node_id or "").strip() or None,
        run_tables=run_tables,
        storage_service=storage_service,
    )

    warnings = digest_warnings + warnings
    data = {
        "plan_id": plan_id,
        "evaluations": [item.__dict__ for item in evaluations],
        "warnings": warnings,
    }
    if warnings:
        return ApiResponse.partial(message="Join evaluation completed with warnings", data=data, errors=warnings)
    return ApiResponse.success(message="Join evaluation completed", data=data)


@router.post("/{plan_id}/repair", response_model=ApiResponse)
async def repair_plan(
    plan_id: str,
    body: PipelinePlanRepairRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    profile_registry: DatasetProfileRegistry = Depends(get_dataset_profile_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    if not bool(get_settings().pipeline_plan.llm_enabled):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Pipeline planner is disabled")

    tenant_id = _resolve_tenant_id(request)
    actor = _resolve_actor(request)
    user = getattr(request.state, "user", None)
    user_id = str(getattr(user, "id", "") or "").strip() or None

    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline plan not found")

    try:
        plan = PipelinePlan.model_validate(record.plan)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Stored plan invalid: {exc}") from exc

    selected_model, allowed_models, data_policies = await _resolve_tenant_policy(request)

    context_pack = None
    try:
        dataset_ids = list(plan.data_scope.dataset_ids or [])
        context_pack = await build_pipeline_context_pack(
            db_name=str(plan.data_scope.db_name or ""),
            branch=str(plan.data_scope.branch or "") or None,
            dataset_ids=dataset_ids or None,
            dataset_registry=dataset_registry,
            profile_registry=profile_registry,
        )
    except Exception as exc:
        logger.warning("Failed to build pipeline context pack: %s", exc)
        context_pack = None

    try:
        repairer = repair_pipeline_plan_mcp if bool(get_settings().pipeline_plan.mcp_repair_enabled) else repair_pipeline_plan
        result = await repairer(
            plan=plan,
            validation_errors=body.validation_errors,
            validation_warnings=body.validation_warnings,
            preflight=body.preflight,
            preview=body.preview,
            context_pack=context_pack,
            actor=actor,
            tenant_id=tenant_id,
            user_id=user_id,
            data_policies=data_policies,
            selected_model=selected_model,
            allowed_models=allowed_models,
            llm_gateway=llm,
            redis_service=redis_service,
            audit_store=audit_store,
            dataset_registry=dataset_registry,
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
        "preflight": result.preflight,
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

    if result.plan:
        status_value = "COMPILED" if result.status == "success" else "DRAFT"
        await plan_registry.upsert_plan(
            plan_id=result.plan_id,
            tenant_id=tenant_id,
            status=status_value,
            goal=str(result.plan.goal or ""),
            db_name=str(result.plan.data_scope.db_name or "") if result.plan.data_scope else None,
            branch=str(result.plan.data_scope.branch or "") if result.plan.data_scope else None,
            plan=result.plan.model_dump(mode="json"),
            created_by=actor,
        )

    return ApiResponse.success(message="Pipeline plan repaired", data=response_data)


@router.post("/{plan_id}/cleanse", response_model=ApiResponse)
async def cleanse_plan(
    plan_id: str,
    body: PipelinePlanCleanseRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    if not bool(get_settings().pipeline_plan.llm_enabled):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Pipeline planner is disabled")

    tenant_id = _resolve_tenant_id(request)
    actor = _resolve_actor(request)
    user = getattr(request.state, "user", None)
    user_id = str(getattr(user, "id", "") or "").strip() or None

    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline plan not found")

    try:
        plan = PipelinePlan.model_validate(record.plan)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Stored plan invalid: {exc}") from exc
    if not plan.data_scope.db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")

    db_name = str(plan.data_scope.db_name or "").strip()
    if not db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    if plan.task_spec is not None and not bool(plan.task_spec.allow_cleansing):
        return ApiResponse.warning(
            message="Task scope forbids cleansing",
            data={
                "plan_id": plan_id,
                "task_spec": plan.task_spec.model_dump(mode="json"),
            },
        )

    selected_model, allowed_models, data_policies = await _resolve_tenant_policy(request)

    preview_payload = body.preview if isinstance(body.preview, dict) else None
    inspector_payload = body.inspector if isinstance(body.inspector, dict) else None
    preflight = None
    if preview_payload is None:
        validation = await validate_pipeline_plan(
            plan=plan,
            dataset_registry=dataset_registry,
            db_name=db_name,
            branch=str(plan.data_scope.branch or "") or None,
            require_output=True,
        )
        preflight = validation.preflight
        if validation.errors:
            return ApiResponse.warning(
                message="Pipeline plan invalid",
                data={
                    "validation_errors": validation.errors,
                    "validation_warnings": validation.warnings,
                    "preflight": validation.preflight,
                },
            )

        preview_definition = dict(validation.plan.definition_json)
        preview_meta = dict(preview_definition.get("__preview_meta__") or {})
        preview_meta.setdefault("branch", str(plan.data_scope.branch or "") or "main")
        preview_definition["__preview_meta__"] = preview_meta
        executor = PipelineExecutor(dataset_registry)
        preview_payload = await executor.preview(
            definition=preview_definition,
            db_name=db_name,
            node_id=None,
            limit=200,
        )

    if inspector_payload is None:
        inspector_payload = inspect_preview(preview_payload or {})

    if not bool(inspector_payload.get("needs_cleansing")):
        return ApiResponse.success(
            message="No cleansing actions required",
            data={
                "plan_id": plan_id,
                "preflight": preflight,
                "inspector": inspector_payload,
                "plan": plan.model_dump(mode="json"),
                "actions_applied": [],
            },
        )

    try:
        result = await apply_cleansing_plan(
            plan=plan,
            inspector=inspector_payload,
            max_actions=int(body.max_actions or 0),
            planner_hints=body.planner_hints,
            actor=actor,
            tenant_id=tenant_id,
            user_id=user_id,
            data_policies=data_policies,
            selected_model=selected_model,
            allowed_models=allowed_models,
            llm_gateway=llm,
            redis_service=redis_service,
            audit_store=audit_store,
            db_name=db_name,
            branch=str(plan.data_scope.branch or "") or None,
            dataset_registry=dataset_registry,
        )
    except LLMQuotaExceededError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Pipeline cleanse failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to cleanse plan") from exc

    if result.validation_errors:
        return ApiResponse.partial(
            message="Pipeline cleanse failed validation",
            data={
                "plan_id": plan_id,
                "plan": result.plan.model_dump(mode="json"),
                "preflight": preflight,
                "inspector": inspector_payload,
                "actions_applied": result.actions_applied,
                "validation_errors": result.validation_errors,
                "validation_warnings": result.validation_warnings,
                "planner": {"confidence": result.confidence, "notes": result.notes, "warnings": result.warnings},
            },
            errors=result.validation_errors,
        )

    await plan_registry.upsert_plan(
        plan_id=plan_id,
        tenant_id=tenant_id,
        status="COMPILED",
        goal=str(result.plan.goal or ""),
        db_name=str(result.plan.data_scope.db_name or "") if result.plan.data_scope else None,
        branch=str(result.plan.data_scope.branch or "") if result.plan.data_scope else None,
        plan=result.plan.model_dump(mode="json"),
        created_by=actor,
    )

    return ApiResponse.success(
        message="Pipeline plan cleansed",
        data={
            "plan_id": plan_id,
            "plan": result.plan.model_dump(mode="json"),
            "preflight": preflight,
            "inspector": inspector_payload,
            "actions_applied": result.actions_applied,
            "validation_errors": result.validation_errors,
            "validation_warnings": result.validation_warnings,
            "planner": {"confidence": result.confidence, "notes": result.notes, "warnings": result.warnings},
        },
    )


@router.post("/{plan_id}/transform", response_model=ApiResponse)
async def transform_plan(
    plan_id: str,
    body: PipelinePlanTransformRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    profile_registry: DatasetProfileRegistry = Depends(get_dataset_profile_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    if not bool(get_settings().pipeline_plan.llm_enabled):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Pipeline planner is disabled")

    tenant_id = _resolve_tenant_id(request)
    actor = _resolve_actor(request)
    user = getattr(request.state, "user", None)
    user_id = str(getattr(user, "id", "") or "").strip() or None

    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline plan not found")

    try:
        plan = PipelinePlan.model_validate(record.plan)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Stored plan invalid: {exc}") from exc
    if not plan.data_scope.db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")

    db_name = str(plan.data_scope.db_name or "").strip()
    if not db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    selected_model, allowed_models, data_policies = await _resolve_tenant_policy(request)

    raw_payload = body.model_dump(exclude_none=True)
    context_pack = raw_payload.pop("context_pack", None)
    payload = sanitize_input(raw_payload)
    join_plan = payload.get("join_plan") if isinstance(payload.get("join_plan"), list) else None
    cleansing_hints = payload.get("cleansing_hints") if isinstance(payload.get("cleansing_hints"), list) else None
    planner_hints = payload.get("planner_hints") if isinstance(payload.get("planner_hints"), dict) else None
    context_pack = context_pack if isinstance(context_pack, dict) else None

    if context_pack is None:
        try:
            dataset_ids = list(plan.data_scope.dataset_ids or [])
            context_pack = await build_pipeline_context_pack(
                db_name=str(plan.data_scope.db_name or ""),
                branch=str(plan.data_scope.branch or "") or None,
                dataset_ids=dataset_ids or None,
                dataset_registry=dataset_registry,
                profile_registry=profile_registry,
            )
        except Exception as exc:
            logger.warning("Failed to build pipeline context pack: %s", exc)
            context_pack = None

    try:
        transformer = (
            apply_transform_plan_mcp
            if bool(get_settings().pipeline_plan.mcp_transform_enabled)
            else apply_transform_plan
        )
        result = await transformer(
            plan=plan,
            join_plan=join_plan,
            cleansing_hints=cleansing_hints,
            context_pack=context_pack,
            planner_hints=planner_hints,
            actor=actor,
            tenant_id=tenant_id,
            user_id=user_id,
            data_policies=data_policies,
            selected_model=selected_model,
            allowed_models=allowed_models,
            llm_gateway=llm,
            redis_service=redis_service,
            audit_store=audit_store,
            db_name=db_name,
            branch=str(plan.data_scope.branch or "") or None,
            dataset_registry=dataset_registry,
        )
    except LLMQuotaExceededError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Pipeline transform failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to transform plan") from exc

    if result.validation_errors:
        return ApiResponse.partial(
            message="Pipeline transform failed validation",
            data={
                "plan_id": plan_id,
                "plan": result.plan.model_dump(mode="json"),
                "validation_errors": result.validation_errors,
                "validation_warnings": result.validation_warnings,
                "planner": {"confidence": result.confidence, "notes": result.notes, "warnings": result.warnings},
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
            },
            errors=result.validation_errors,
        )

    await plan_registry.upsert_plan(
        plan_id=plan_id,
        tenant_id=tenant_id,
        status="COMPILED",
        goal=str(result.plan.goal or ""),
        db_name=str(result.plan.data_scope.db_name or "") if result.plan.data_scope else None,
        branch=str(result.plan.data_scope.branch or "") if result.plan.data_scope else None,
        plan=result.plan.model_dump(mode="json"),
        created_by=actor,
    )

    return ApiResponse.success(
        message="Pipeline plan transformed",
        data={
            "plan_id": plan_id,
            "plan": result.plan.model_dump(mode="json"),
            "validation_errors": result.validation_errors,
            "validation_warnings": result.validation_warnings,
            "planner": {"confidence": result.confidence, "notes": result.notes, "warnings": result.warnings},
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
        },
    )


@router.post("/{plan_id}/verify-intent", response_model=ApiResponse)
async def verify_intent(
    plan_id: str,
    body: PipelinePlanIntentCheckRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    profile_registry: DatasetProfileRegistry = Depends(get_dataset_profile_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    if not bool(get_settings().pipeline_plan.llm_enabled):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Pipeline planner is disabled")

    tenant_id = _resolve_tenant_id(request)
    actor = _resolve_actor(request)
    user = getattr(request.state, "user", None)
    user_id = str(getattr(user, "id", "") or "").strip() or None

    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline plan not found")

    try:
        plan = PipelinePlan.model_validate(record.plan)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Stored plan invalid: {exc}") from exc
    if not plan.data_scope.db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")

    db_name = str(plan.data_scope.db_name or "").strip()
    if not db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    selected_model, allowed_models, data_policies = await _resolve_tenant_policy(request)

    raw_payload = body.model_dump(exclude_none=True)
    context_pack = raw_payload.pop("context_pack", None)
    payload = sanitize_input(raw_payload)
    join_plan = payload.get("join_plan") if isinstance(payload.get("join_plan"), list) else None
    cleansing_hints = payload.get("cleansing_hints") if isinstance(payload.get("cleansing_hints"), list) else None
    context_pack = context_pack if isinstance(context_pack, dict) else None

    if context_pack is None:
        try:
            dataset_ids = list(plan.data_scope.dataset_ids or [])
            context_pack = await build_pipeline_context_pack(
                db_name=str(plan.data_scope.db_name or ""),
                branch=str(plan.data_scope.branch or "") or None,
                dataset_ids=dataset_ids or None,
                dataset_registry=dataset_registry,
                profile_registry=profile_registry,
            )
        except Exception as exc:
            logger.warning("Failed to build pipeline context pack: %s", exc)
            context_pack = None

    try:
        result = await verify_pipeline_intent(
            plan=plan,
            join_plan=join_plan,
            cleansing_hints=cleansing_hints,
            context_pack=context_pack,
            actor=actor,
            tenant_id=tenant_id,
            user_id=user_id,
            data_policies=data_policies,
            selected_model=selected_model,
            allowed_models=allowed_models,
            llm_gateway=llm,
            redis_service=redis_service,
            audit_store=audit_store,
        )
    except LLMQuotaExceededError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Pipeline intent verification failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to verify intent") from exc

    response_data = {
        "status": result.status,
        "confidence": result.confidence,
        "missing_requirements": result.missing_requirements,
        "suggested_actions": result.suggested_actions,
        "questions": [q.model_dump(mode="json") for q in (result.questions or [])],
        "warnings": result.warnings,
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

    return ApiResponse.success(message="Pipeline intent verified", data=response_data)


@router.post("/{plan_id}/split-outputs", response_model=ApiResponse)
async def split_outputs(
    plan_id: str,
    body: PipelinePlanSplitOutputsRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    profile_registry: DatasetProfileRegistry = Depends(get_dataset_profile_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    if not bool(get_settings().pipeline_plan.llm_enabled):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Pipeline planner is disabled")

    tenant_id = _resolve_tenant_id(request)
    actor = _resolve_actor(request)
    user = getattr(request.state, "user", None)
    user_id = str(getattr(user, "id", "") or "").strip() or None

    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline plan not found")

    try:
        plan = PipelinePlan.model_validate(record.plan)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Stored plan invalid: {exc}") from exc

    db_name = str(plan.data_scope.db_name or "").strip()
    if not db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    if plan.task_spec is not None and not bool(plan.task_spec.allow_specs):
        return ApiResponse.warning(
            message="Task scope forbids output enrichment (mapping/specs not requested)",
            data={
                "plan_id": plan_id,
                "task_spec": plan.task_spec.model_dump(mode="json"),
            },
        )

    selected_model, allowed_models, data_policies = await _resolve_tenant_policy(request)

    context_pack = None
    try:
        dataset_ids = list(plan.data_scope.dataset_ids or [])
        context_pack = await build_pipeline_context_pack(
            db_name=str(plan.data_scope.db_name or ""),
            branch=str(plan.data_scope.branch or "") or None,
            dataset_ids=dataset_ids or None,
            dataset_registry=dataset_registry,
            profile_registry=profile_registry,
        )
    except Exception as exc:
        logger.warning("Failed to build pipeline context pack: %s", exc)
        context_pack = None

    payload = sanitize_input(body.model_dump(exclude_none=True))
    bindings_payload = payload.get("output_bindings") if isinstance(payload.get("output_bindings"), dict) else None

    try:
        result = await split_pipeline_outputs(
            plan=plan,
            output_bindings=bindings_payload,
            context_pack=context_pack,
            actor=actor,
            tenant_id=tenant_id,
            user_id=user_id,
            data_policies=data_policies,
            selected_model=selected_model,
            allowed_models=allowed_models,
            llm_gateway=llm,
            redis_service=redis_service,
            audit_store=audit_store,
        )
    except LLMQuotaExceededError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Pipeline output split failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to split outputs") from exc

    updated_plan = plan.model_copy(update={"outputs": result.outputs})
    validation = await validate_pipeline_plan(
        plan=updated_plan,
        dataset_registry=dataset_registry,
        db_name=db_name,
        branch=str(updated_plan.data_scope.branch or "") or None,
        require_output=True,
    )

    status_value = "COMPILED" if not validation.errors else "DRAFT"
    await plan_registry.upsert_plan(
        plan_id=plan_id,
        tenant_id=tenant_id,
        status=status_value,
        goal=str(updated_plan.goal or ""),
        db_name=str(updated_plan.data_scope.db_name or "") if updated_plan.data_scope else None,
        branch=str(updated_plan.data_scope.branch or "") if updated_plan.data_scope else None,
        plan=updated_plan.model_dump(mode="json"),
        created_by=actor,
    )

    response_data = {
        "status": "success" if not validation.errors else "warning",
        "plan_id": plan_id,
        "plan": updated_plan.model_dump(mode="json"),
        "validation_errors": list(validation.errors or []),
        "validation_warnings": list(validation.warnings or []),
        "planner": {
            "confidence": result.confidence,
            "notes": result.notes,
            "warnings": result.warnings,
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

    if validation.errors:
        return ApiResponse.partial(
            message="Pipeline output split completed with issues",
            data=response_data,
            errors=list(validation.errors or []),
        )

    return ApiResponse.success(message="Pipeline outputs updated", data=response_data)


@router.post("/{plan_id}/generate-specs", response_model=ApiResponse)
async def generate_specs(
    plan_id: str,
    body: PipelinePlanGenerateSpecsRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
    oms_client: OMSClient = OMSClientDep,
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    tenant_id = _resolve_tenant_id(request)
    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline plan not found")

    try:
        plan = PipelinePlan.model_validate(record.plan)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Stored plan invalid: {exc}") from exc

    db_name = str(plan.data_scope.db_name or "").strip()
    if not db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    if plan.task_spec is not None and not bool(plan.task_spec.allow_specs):
        return ApiResponse.warning(
            message="Task scope forbids spec generation (mapping not requested)",
            data={
                "plan_id": plan_id,
                "task_spec": plan.task_spec.model_dump(mode="json"),
            },
        )

    raw_payload = body.model_dump(exclude_none=True)
    preview_payload_raw = raw_payload.pop("output_previews", None)
    payload = sanitize_input(raw_payload)
    bindings_payload = payload.get("output_bindings") if isinstance(payload.get("output_bindings"), dict) else None
    preview_payload = (
        _sanitize_preview_tables(preview_payload_raw) if isinstance(preview_payload_raw, dict) else None
    )

    results = await generate_pipeline_specs(
        plan=plan,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        oms_client=oms_client,
        request=request,
        preview_overrides=preview_payload,
        output_bindings=bindings_payload,
        apply_specs=bool(payload.get("apply")),
        auto_sync=bool(payload.get("auto_sync", True)),
        ontology_branch=str(payload.get("ontology_branch") or "").strip() or None,
        dangling_policy=str(payload.get("dangling_policy") or "FAIL").strip().upper(),
        dedupe_policy=str(payload.get("dedupe_policy") or "DEDUP").strip().upper(),
    )

    errors: List[str] = []
    for item in results:
        errors.extend(item.errors)

    data = {
        "plan_id": plan_id,
        "specs": [item.__dict__ for item in results],
    }

    if errors:
        return ApiResponse.partial(message="Pipeline specs generated with issues", data=data, errors=errors)
    return ApiResponse.success(message="Pipeline specs generated", data=data)
