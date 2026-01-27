from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from bff.services.pipeline_join_evaluator import evaluate_pipeline_joins
from bff.services.pipeline_plan_models import PipelinePlanCompileResult
from bff.services.pipeline_plan_autonomous_compiler import compile_pipeline_plan_mcp_autonomous
from bff.services.pipeline_plan_validation import validate_pipeline_plan
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
from shared.services.registries.agent_policy_registry import AgentPolicyRegistry
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.dataset_profile_registry import DatasetProfileRegistry
from shared.services.pipeline.pipeline_executor import PipelineExecutor
from shared.services.pipeline.pipeline_preview_inspector import inspect_preview
from shared.utils.canonical_json import sha256_canonical_json_prefixed
from shared.services.registries.pipeline_plan_registry import PipelinePlanRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.dependencies.providers import AuditLogStoreDep, RedisServiceDep, LLMGatewayDep
from shared.services.agent.llm_quota import LLMQuotaExceededError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pipeline-plans", tags=["Pipeline Plans"])

_PREVIEW_SANITIZER = InputSanitizer()
_SYS_TS_EXPR_RE = re.compile(r"^\s*(?P<col>_sys_ingested_at|_sys_valid_from)\s*=\s*to_timestamp\('.*'\)\s*$")
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

class PipelinePlanPreviewRequest(BaseModel):
    node_id: str | None = Field(default=None, max_length=200)
    limit: int = Field(default=200, ge=1, le=200)
    include_run_tables: bool = Field(default=False)
    run_table_limit: int = Field(default=200, ge=1, le=1000)

class PipelinePlanInspectPreviewRequest(BaseModel):
    preview: Dict[str, Any] | None = Field(default=None)
    node_id: str | None = Field(default=None, max_length=200)
    limit: int = Field(default=200, ge=1, le=200)

class PipelinePlanEvaluateJoinsRequest(BaseModel):
    node_id: str | None = Field(default=None, max_length=200)
    run_tables: Dict[str, Dict[str, Any]] | None = Field(default=None)
    definition_digest: str | None = Field(default=None, max_length=200)


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

    try:
        # The pipeline planner is a single autonomous MCP loop (no multi-agent subflows).
        result: PipelinePlanCompileResult = await compile_pipeline_plan_mcp_autonomous(
            goal=goal,
            data_scope=data_scope,
            answers=answers,
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
            plan_registry=plan_registry,
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
