"""Pipeline plan preview service (BFF).

Extracted from `bff.routers.pipeline_plans_preview` to keep routers thin and to
deduplicate preview/inspection/join-evaluation orchestration (Facade pattern).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import Request

from bff.schemas.pipeline_plans_requests import (
    PipelinePlanEvaluateJoinsRequest,
    PipelinePlanInspectPreviewRequest,
    PipelinePlanPreviewRequest,
)
from bff.services.pipeline_join_evaluator import evaluate_pipeline_joins
from bff.services.pipeline_plan_scoping_service import PipelinePlanRequestContext, _load_scoped_plan
from bff.services.pipeline_plan_validation import validate_pipeline_plan
from bff.services.pipeline_plan_preview_utils import (
    _JOIN_SAMPLE_MIN_ROWS,
    _JOIN_SAMPLE_MULTI_MIN_ROWS,
    _PREVIEW_MAX_OUTPUT_ROWS,
    _definition_digest,
    _definition_has_join,
    _definition_join_count,
    _sanitize_preview_tables,
    _serialize_run_tables,
)
from shared.models.pipeline_plan import PipelinePlan
from shared.models.responses import ApiResponse
from shared.security.input_sanitizer import sanitize_input
from shared.services.pipeline.pipeline_executor import PipelineExecutor
from shared.services.pipeline.pipeline_preview_inspector import inspect_preview
from shared.services.pipeline.pipeline_preview_policy import evaluate_preview_policy
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.pipeline_plan_registry import PipelinePlanRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.storage.storage_service import StorageService
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


async def _try_get_lakefs_storage(
    *, pipeline_registry: PipelineRegistry, request: Request, purpose: str
) -> Optional[StorageService]:
    actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
    try:
        return await pipeline_registry.get_lakefs_storage(user_id=actor_user_id)
    except Exception as exc:
        logger.warning("Failed to init lakeFS storage for %s: %s", purpose, exc)
        return None


async def _validate_or_warn(
    *,
    plan: PipelinePlan,
    dataset_registry: DatasetRegistry,
    pipeline_registry: PipelineRegistry,
    db_name: str,
    branch: Optional[str],
    require_output: bool,
) -> tuple[Optional[Any], Optional[ApiResponse]]:
    validation = await validate_pipeline_plan(
        plan=plan,
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
        db_name=db_name,
        branch=str(branch or "") or None,
        require_output=require_output,
    )
    if validation.errors:
        return None, ApiResponse.warning(
            message="Pipeline plan invalid",
            data={
                "validation_errors": validation.errors,
                "validation_warnings": validation.warnings,
                "preflight": validation.preflight,
            },
        )
    return validation, None


@trace_external_call("bff.pipeline_plan_preview.preview_plan")
async def preview_plan(
    *,
    plan_id: str,
    body: PipelinePlanPreviewRequest,
    request: Request,
    dataset_registry: DatasetRegistry,
    pipeline_registry: PipelineRegistry,
    plan_registry: PipelinePlanRegistry,
) -> ApiResponse:
    ctx = await _load_scoped_plan(plan_id=plan_id, request=request, plan_registry=plan_registry)
    plan: PipelinePlan = ctx.plan
    db_name = ctx.db_name
    branch = ctx.branch

    validation, warning_resp = await _validate_or_warn(
        plan=plan,
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
        db_name=db_name,
        branch=branch,
        require_output=True,
    )
    if warning_resp is not None:
        return warning_resp

    node_id = str(body.node_id or "").strip() or None
    limit = int(body.limit or 200)
    include_run_tables = bool(body.include_run_tables)
    run_table_limit = int(body.run_table_limit or limit)

    preview_definition = dict(validation.plan.definition_json)
    preview_meta = dict(preview_definition.get("__preview_meta__") or {})
    preview_meta.setdefault("branch", str(branch or "") or "main")
    sample_limit = max(limit, run_table_limit) if include_run_tables else limit

    join_count = _definition_join_count(preview_definition)
    if join_count:
        sample_limit = max(int(sample_limit or 0), _JOIN_SAMPLE_MIN_ROWS)
        if join_count > 1:
            target = _JOIN_SAMPLE_MULTI_MIN_ROWS * max(1, join_count - 1)
            sample_limit = max(int(sample_limit or 0), min(target, 20000))
        preview_meta.setdefault("max_output_rows", _PREVIEW_MAX_OUTPUT_ROWS)
    if sample_limit:
        preview_meta["sample_limit"] = sample_limit
    preview_definition["__preview_meta__"] = preview_meta

    preview_policy = evaluate_preview_policy(preview_definition)
    policy_level = str(preview_policy.get("level") or "allow").strip().lower()
    if policy_level == "deny":
        return ApiResponse.warning(
            message="Pipeline preview denied by policy",
            data={
                "preflight": validation.preflight,
                "preview_status": "preview_denied",
                "preview_policy": preview_policy,
                "hint": "Fix the denied operations (see preview_policy) before preview/build/deploy.",
                "preview": {"columns": [], "rows": [], "row_count": 0},
                "definition_digest": _definition_digest(preview_definition),
            },
        )
    if policy_level == "require_spark":
        return ApiResponse.warning(
            message="Pipeline preview requires Spark-backed execution",
            data={
                "preflight": validation.preflight,
                "preview_status": "requires_spark_preview",
                "preview_policy": preview_policy,
                "hint": "This plan uses Spark semantics that the lightweight preview engine cannot validate safely. Save the pipeline, then use Build (Spark) to verify.",
                "preview": {"columns": [], "rows": [], "row_count": 0},
                "definition_digest": _definition_digest(preview_definition),
            },
        )

    storage_service = await _try_get_lakefs_storage(pipeline_registry=pipeline_registry, request=request, purpose="pipeline preview")

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
                "preview_status": "preview_failed",
                "preview_policy": preview_policy,
            },
        )

    data: Dict[str, Any] = {
        "preflight": validation.preflight,
        "preview": preview,
        "definition_digest": definition_digest,
        "preview_status": "success",
        "preview_policy": preview_policy,
    }
    if run_tables_payload is not None:
        data["run_tables"] = run_tables_payload

    return ApiResponse.success(message="Pipeline preview ready", data=data)


@trace_external_call("bff.pipeline_plan_preview.inspect_plan_preview")
async def inspect_plan_preview(
    *,
    plan_id: str,
    body: PipelinePlanInspectPreviewRequest,
    request: Request,
    dataset_registry: DatasetRegistry,
    pipeline_registry: PipelineRegistry,
    plan_registry: PipelinePlanRegistry,
) -> ApiResponse:
    ctx = await _load_scoped_plan(plan_id=plan_id, request=request, plan_registry=plan_registry)
    plan: PipelinePlan = ctx.plan
    db_name = ctx.db_name
    branch = ctx.branch

    preview_payload = body.preview if isinstance(body.preview, dict) else None
    preflight = None
    if preview_payload is None:
        validation, warning_resp = await _validate_or_warn(
            plan=plan,
            dataset_registry=dataset_registry,
            pipeline_registry=pipeline_registry,
            db_name=db_name,
            branch=branch,
            require_output=True,
        )
        if warning_resp is not None:
            return warning_resp

        preflight = validation.preflight

        node_id = str(body.node_id or "").strip() or None
        limit = int(body.limit or 200)
        preview_definition = dict(validation.plan.definition_json)
        preview_meta = dict(preview_definition.get("__preview_meta__") or {})
        preview_meta.setdefault("branch", str(branch or "") or "main")
        if _definition_has_join(preview_definition):
            preview_meta["sample_limit"] = max(limit, _JOIN_SAMPLE_MIN_ROWS)
        preview_definition["__preview_meta__"] = preview_meta

        storage_service = await _try_get_lakefs_storage(
            pipeline_registry=pipeline_registry,
            request=request,
            purpose="pipeline preview inspection",
        )
        executor = PipelineExecutor(
            dataset_registry,
            pipeline_registry=pipeline_registry,
            storage_service=storage_service,
        )
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


@trace_external_call("bff.pipeline_plan_preview.evaluate_joins")
async def evaluate_joins(
    *,
    plan_id: str,
    body: PipelinePlanEvaluateJoinsRequest,
    request: Request,
    dataset_registry: DatasetRegistry,
    pipeline_registry: PipelineRegistry,
    plan_registry: PipelinePlanRegistry,
) -> ApiResponse:
    ctx: PipelinePlanRequestContext = await _load_scoped_plan(plan_id=plan_id, request=request, plan_registry=plan_registry)
    plan: PipelinePlan = ctx.plan
    db_name = ctx.db_name
    branch = ctx.branch

    validation, warning_resp = await _validate_or_warn(
        plan=plan,
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
        db_name=db_name,
        branch=branch,
        require_output=True,
    )
    if warning_resp is not None:
        return warning_resp

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

    storage_service = await _try_get_lakefs_storage(pipeline_registry=pipeline_registry, request=request, purpose="join evaluation")

    evaluations, warnings = await evaluate_pipeline_joins(
        definition_json={
            **dict(validation.plan.definition_json),
            "__preview_meta__": {
                **dict((validation.plan.definition_json or {}).get("__preview_meta__") or {}),
                "branch": str(branch or "") or "main",
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
        "plan_id": ctx.plan_id,
        "evaluations": [item.__dict__ for item in evaluations],
        "warnings": warnings,
    }
    if warnings:
        return ApiResponse.partial(message="Join evaluation completed with warnings", data=data, errors=warnings)
    return ApiResponse.success(message="Join evaluation completed", data=data)
