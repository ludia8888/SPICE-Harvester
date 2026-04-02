"""
Preview/build execution helpers for pipeline execution.

Keeps mode-specific orchestration out of the main service facade while
preserving the existing public service entrypoints.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional
from uuid import uuid4

import httpx
from fastapi import status

from bff.routers.pipeline_ops import (
    _resolve_definition_commit_id,
    _run_pipeline_preflight,
    _stable_definition_hash,
)
from bff.routers.pipeline_shared import _log_pipeline_audit, _resolve_principal
from bff.services.oms_client import OMSClient
from bff.services.ontology_occ_guard_service import resolve_branch_head_commit_with_bootstrap
from bff.services.pipeline_execution_queue import publish_build_pipeline_job, publish_preview_pipeline_job
from bff.services.pipeline_execution_requests import (
    _PipelineRunContext,
    _PipelineRunRequestPayload,
    _PreparedBuildExecution,
    _PreparedPreviewExecution,
)
from shared.dependencies.providers import AuditLogStoreDep
from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode, classified_http_exception
from shared.models.pipeline_job import PipelineJob
from shared.models.requests import ApiResponse
from shared.services.pipeline.pipeline_job_queue import PipelineJobQueue
from shared.services.pipeline.pipeline_scheduler import _is_valid_cron_expression
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.utils.event_utils import build_command_event
from shared.utils.time_utils import utcnow


logger = logging.getLogger(__name__)


def _build_ontology_ref(branch: str) -> str:
    resolved = str(branch or "").strip() or "main"
    return f"branch:{resolved}"


def _parse_pipeline_run_request_payload(
    *,
    sanitized: Dict[str, Any],
    allow_sampling_strategy: bool,
    default_limit: int = 200,
) -> _PipelineRunRequestPayload:
    limit = int(sanitized.get("limit") or default_limit)
    limit = max(1, min(limit, 500))
    definition_json = (
        sanitized.get("definition_json")
        if isinstance(sanitized.get("definition_json"), dict)
        else None
    )
    node_id = str(sanitized.get("node_id") or "").strip() or None
    db_name = str(sanitized.get("db_name") or "").strip()
    expectations = (
        sanitized.get("expectations")
        if isinstance(sanitized.get("expectations"), list)
        else None
    )
    schema_contract = (
        sanitized.get("schema_contract")
        if isinstance(sanitized.get("schema_contract"), list)
        else None
    )
    sampling_strategy: Optional[dict[str, Any]] = None
    if allow_sampling_strategy:
        raw_sampling_strategy = sanitized.get("sampling_strategy") or sanitized.get("samplingStrategy")
        if raw_sampling_strategy is not None and not isinstance(raw_sampling_strategy, dict):
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "sampling_strategy must be an object",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        if isinstance(raw_sampling_strategy, dict):
            sampling_strategy = raw_sampling_strategy
    return _PipelineRunRequestPayload(
        limit=limit,
        definition_json=definition_json,
        node_id=node_id,
        db_name=db_name,
        expectations=expectations,
        schema_contract=schema_contract,
        branch=str(sanitized.get("branch") or "").strip() or None,
        sampling_strategy=sampling_strategy,
    )


def _merge_definition_contract_overrides(
    *,
    definition_json: Optional[Dict[str, Any]],
    expectations: Optional[list[Any]],
    schema_contract: Optional[list[Any]],
) -> Dict[str, Any]:
    merged_definition = definition_json or {}
    if expectations is not None:
        merged_definition = {**merged_definition, "expectations": expectations}
    if schema_contract is not None:
        merged_definition = {**merged_definition, "schemaContract": schema_contract}
    return merged_definition


async def _resolve_pipeline_run_context(
    *,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str,
    request_payload: _PipelineRunRequestPayload,
    validate_db: Any,
    clear_node_for_streaming_pipeline: bool,
    archived_branch_error: str,
) -> _PipelineRunContext:
    pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    if not pipeline:
        raise classified_http_exception(
            status.HTTP_404_NOT_FOUND,
            "Pipeline not found",
            code=ErrorCode.PIPELINE_NOT_FOUND,
        )
    node_id = request_payload.node_id
    if clear_node_for_streaming_pipeline and str(pipeline.pipeline_type or "").strip().lower() in {
        "stream",
        "streaming",
    }:
        node_id = None
    branch_state = await pipeline_registry.get_pipeline_branch(
        db_name=pipeline.db_name,
        branch=pipeline.branch,
    )
    if branch_state and branch_state.get("archived"):
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            archived_branch_error,
            code=ErrorCode.CONFLICT,
        )
    if request_payload.branch and request_payload.branch != pipeline.branch:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "branch does not match pipeline branch",
            code=ErrorCode.CONFLICT,
        )
    latest = await pipeline_registry.get_latest_version(
        pipeline_id=pipeline_id,
        branch=pipeline.branch,
    )
    base_definition = request_payload.definition_json or (latest.definition_json if latest else {}) or {}
    definition_json = _merge_definition_contract_overrides(
        definition_json=base_definition,
        expectations=request_payload.expectations,
        schema_contract=request_payload.schema_contract,
    )
    db_name = request_payload.db_name or (pipeline.db_name if pipeline else "")
    if not db_name:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "db_name is required",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    if validate_db:
        db_name = validate_db(db_name)
    return _PipelineRunContext(
        pipeline=pipeline,
        latest=latest,
        definition_json=definition_json,
        db_name=db_name,
        node_id=node_id,
        requested_branch=request_payload.branch,
    )


def _build_preview_job_id(
    *,
    pipeline_id: str,
    node_id: Optional[str],
    definition_hash: str,
) -> str:
    node_key = (node_id or "pipeline").replace("node-", "").replace("/", "-").replace(" ", "-")
    node_key = node_key[:16] if node_key else "pipeline"
    hash_key = str(definition_hash or uuid4().hex)[:12]
    return f"preview-{pipeline_id}-{hash_key}-{node_key}"


def _build_preview_existing_run_response(
    *,
    pipeline_id: str,
    job_id: str,
    limit: int,
    preflight: Dict[str, Any],
    existing_run: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not existing_run:
        return None
    status_value = str(existing_run.get("status") or "").upper()
    sample_json_existing = (
        existing_run.get("sample_json")
        if isinstance(existing_run.get("sample_json"), dict)
        else {}
    )
    if status_value in {"SUCCESS", "FAILED"} and sample_json_existing:
        return ApiResponse.success(
            message="Pipeline preview",
            data={
                "pipeline_id": pipeline_id,
                "job_id": job_id,
                "limit": limit,
                "preflight": preflight,
                "sample": sample_json_existing,
            },
        ).to_dict()
    return ApiResponse.success(
        message="Pipeline preview",
        data={
            "pipeline_id": pipeline_id,
            "job_id": job_id,
            "limit": limit,
            "preflight": preflight,
            "sample": {"queued": True, "limit": limit, "job_id": job_id},
        },
    ).to_dict()


async def _record_preview_run_queued(
    *,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str,
    job_id: str,
    node_id: Optional[str],
    limit: int,
) -> None:
    await pipeline_registry.record_preview(
        pipeline_id=pipeline_id,
        status="QUEUED",
        row_count=0,
        sample_json={"queued": True, "limit": limit, "job_id": job_id},
        job_id=job_id,
        node_id=node_id,
    )
    await pipeline_registry.record_run(
        pipeline_id=pipeline_id,
        job_id=job_id,
        mode="preview",
        status="QUEUED",
        node_id=node_id,
        sample_json={"queued": True, "limit": limit},
    )


def _build_preview_definition(
    *,
    definition_json: Dict[str, Any],
    branch: Optional[str],
    preview_limit: int,
    sampling_strategy: Optional[dict[str, Any]],
) -> Dict[str, Any]:
    preview_definition = dict(definition_json)
    preview_meta = dict(preview_definition.get("__preview_meta__") or {})
    preview_meta["branch"] = branch or "main"
    preview_meta["sample_limit"] = preview_limit
    preview_meta["max_output_rows"] = preview_limit
    preview_meta["sample_based_execution"] = True
    preview_meta["skip_production_checks"] = True
    preview_meta["skip_output_recording"] = True
    if sampling_strategy:
        preview_meta["sampling_strategy"] = sampling_strategy
    preview_definition["__preview_meta__"] = preview_meta
    return preview_definition


async def _append_preview_command_event(
    *,
    event_store: Any,
    pipeline_id: str,
    limit: int,
) -> None:
    event = build_command_event(
        event_type="PIPELINE_PREVIEWED",
        aggregate_type="Pipeline",
        aggregate_id=pipeline_id,
        data={"pipeline_id": pipeline_id, "row_limit": limit},
        command_type="RUN_PIPELINE_PREVIEW",
    )
    try:
        await event_store.connect()
        await event_store.append_event(event)
    except Exception as exc:
        logger.warning("Failed to append pipeline preview event: %s", exc)


def _assert_build_preflight_passes(preflight: Dict[str, Any]) -> None:
    if preflight.get("has_blocking_errors"):
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            {
                "message": "Pipeline preflight checks failed",
                "category": ErrorCategory.CONFLICT.value,
                "preflight": preflight,
            },
            code=ErrorCode.PIPELINE_PREFLIGHT_FAILED,
        )


async def _resolve_build_ontology_head_commit(
    *,
    oms_client: OMSClient,
    db_name: str,
    ontology_branch: str,
) -> str:
    ontology_head_commit_id: Optional[str] = None
    try:
        ontology_head_commit_id = await resolve_branch_head_commit_with_bootstrap(
            oms_client=oms_client,
            db_name=db_name,
            branch=ontology_branch,
            source_branch="main",
            max_attempts=5,
            initial_backoff_seconds=0.15,
            max_backoff_seconds=1.0,
            warning_logger=logger,
        )
    except httpx.HTTPStatusError as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        logger.warning(
            "Failed to resolve ontology head commit (db=%s branch=%s http=%s): %s",
            db_name,
            ontology_branch,
            status_code,
            exc,
        )
    except Exception as exc:
        logger.warning(
            "Failed to resolve ontology head commit (db=%s branch=%s): %s",
            db_name,
            ontology_branch,
            exc,
        )
    return ontology_head_commit_id or _build_ontology_ref(ontology_branch)


def _build_definition_with_ontology_meta(
    *,
    definition_json: Dict[str, Any],
    ontology_branch: str,
    ontology_head_commit_id: str,
) -> Dict[str, Any]:
    build_definition = dict(definition_json)
    build_definition["__build_meta__"] = {
        "branch": ontology_branch,
        "ontology": {
            "ref": _build_ontology_ref(ontology_branch),
            "branch": ontology_branch,
            "commit": ontology_head_commit_id,
        },
    }
    return build_definition


async def _emit_build_requested_event(
    *,
    emit_pipeline_control_plane_event: Any,
    pipeline_id: str,
    job_id: str,
    db_name: str,
    branch: str,
    node_id: Optional[str],
    definition_hash: str,
    limit: int,
    principal_type: str,
    principal_id: str,
) -> None:
    await emit_pipeline_control_plane_event(
        event_type="PIPELINE_BUILD_REQUESTED",
        pipeline_id=pipeline_id,
        event_id=f"build-requested-{job_id}",
        actor=principal_id,
        data={
            "pipeline_id": pipeline_id,
            "job_id": job_id,
            "db_name": db_name,
            "branch": branch,
            "node_id": node_id,
            "definition_hash": definition_hash,
            "limit": limit,
            "principal_type": principal_type,
            "principal_id": principal_id,
        },
    )


async def prepare_preview_execution(
    *,
    pipeline_id: str,
    sanitized: Dict[str, Any],
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
) -> _PreparedPreviewExecution:
    request_payload = _parse_pipeline_run_request_payload(
        sanitized=sanitized,
        allow_sampling_strategy=True,
        default_limit=500,
    )
    run_context = await _resolve_pipeline_run_context(
        pipeline_registry=pipeline_registry,
        pipeline_id=pipeline_id,
        request_payload=request_payload,
        validate_db=False,
        clear_node_for_streaming_pipeline=False,
        archived_branch_error="Archived branch cannot be built; restore the branch first",
    )
    preflight = await _run_pipeline_preflight(
        definition_json=run_context.definition_json,
        db_name=run_context.db_name,
        branch=request_payload.branch or run_context.pipeline.branch,
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
    )
    definition_hash = _stable_definition_hash(run_context.definition_json)
    definition_commit_id = _resolve_definition_commit_id(
        run_context.definition_json,
        run_context.latest,
        definition_hash,
    )
    job_id = _build_preview_job_id(
        pipeline_id=pipeline_id,
        node_id=run_context.node_id,
        definition_hash=definition_hash,
    )
    return _PreparedPreviewExecution(
        request_payload=request_payload,
        run_context=run_context,
        preflight=preflight,
        definition_hash=definition_hash,
        definition_commit_id=definition_commit_id,
        job_id=job_id,
    )


async def dispatch_preview_execution(
    *,
    pipeline_id: str,
    prepared: _PreparedPreviewExecution,
    pipeline_registry: PipelineRegistry,
    pipeline_job_queue: PipelineJobQueue,
    event_store: Any,
) -> Dict[str, Any]:
    existing_run = await pipeline_registry.get_run(
        pipeline_id=pipeline_id,
        job_id=prepared.job_id,
    )
    existing_response = _build_preview_existing_run_response(
        pipeline_id=pipeline_id,
        job_id=prepared.job_id,
        limit=prepared.request_payload.limit,
        preflight=prepared.preflight,
        existing_run=existing_run,
    )
    if existing_response is not None:
        return existing_response
    await _record_preview_run_queued(
        pipeline_registry=pipeline_registry,
        pipeline_id=pipeline_id,
        job_id=prepared.job_id,
        node_id=prepared.run_context.node_id,
        limit=prepared.request_payload.limit,
    )
    preview_definition = _build_preview_definition(
        definition_json=prepared.run_context.definition_json,
        branch=prepared.request_payload.branch,
        preview_limit=prepared.request_payload.limit,
        sampling_strategy=prepared.request_payload.sampling_strategy,
    )
    job = PipelineJob(
        job_id=prepared.job_id,
        pipeline_id=pipeline_id,
        db_name=prepared.run_context.db_name,
        pipeline_type=prepared.run_context.pipeline.pipeline_type if prepared.run_context.pipeline else "batch",
        definition_json=preview_definition,
        definition_hash=prepared.definition_hash,
        definition_commit_id=prepared.definition_commit_id,
        node_id=prepared.run_context.node_id,
        output_dataset_name="preview_output",
        mode="preview",
        preview_limit=prepared.request_payload.limit,
        branch=prepared.run_context.pipeline.branch,
    )
    await publish_preview_pipeline_job(
        pipeline_job_queue=pipeline_job_queue,
        pipeline_registry=pipeline_registry,
        job=job,
        pipeline_id=pipeline_id,
        node_id=prepared.run_context.node_id,
    )
    await _append_preview_command_event(
        event_store=event_store,
        pipeline_id=pipeline_id,
        limit=prepared.request_payload.limit,
    )
    return ApiResponse.success(
        message="Preview queued",
        data={
            "pipeline_id": pipeline_id,
            "job_id": prepared.job_id,
            "limit": prepared.request_payload.limit,
            "preflight": prepared.preflight,
            "sample": {"queued": True, "job_id": prepared.job_id},
        },
    ).to_dict()


async def prepare_build_execution(
    *,
    pipeline_id: str,
    sanitized: Dict[str, Any],
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    oms_client: OMSClient,
    validate_db_name: Any,
) -> _PreparedBuildExecution:
    request_payload = _parse_pipeline_run_request_payload(
        sanitized=sanitized,
        allow_sampling_strategy=False,
    )
    run_context = await _resolve_pipeline_run_context(
        pipeline_registry=pipeline_registry,
        pipeline_id=pipeline_id,
        request_payload=request_payload,
        validate_db=validate_db_name,
        clear_node_for_streaming_pipeline=True,
        archived_branch_error="Archived branch cannot be deployed; restore the branch first",
    )
    preflight = await _run_pipeline_preflight(
        definition_json=run_context.definition_json,
        db_name=run_context.db_name,
        branch=request_payload.branch or run_context.pipeline.branch,
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
    )
    _assert_build_preflight_passes(preflight)
    definition_hash = _stable_definition_hash(run_context.definition_json)
    definition_commit_id = _resolve_definition_commit_id(
        run_context.definition_json,
        run_context.latest,
        definition_hash,
    )
    ontology_branch = request_payload.branch or run_context.pipeline.branch or "main"
    ontology_head_commit_id = await _resolve_build_ontology_head_commit(
        oms_client=oms_client,
        db_name=run_context.db_name,
        ontology_branch=ontology_branch,
    )
    return _PreparedBuildExecution(
        request_payload=request_payload,
        run_context=run_context,
        preflight=preflight,
        definition_hash=definition_hash,
        definition_commit_id=definition_commit_id,
        job_id=f"build-{pipeline_id}-{uuid4()}",
        ontology_branch=ontology_branch,
        ontology_head_commit_id=ontology_head_commit_id,
    )


async def dispatch_build_execution(
    *,
    pipeline_id: str,
    prepared: _PreparedBuildExecution,
    pipeline_registry: PipelineRegistry,
    pipeline_job_queue: PipelineJobQueue,
    emit_pipeline_control_plane_event: Any,
    request: Any,
) -> Dict[str, Any]:
    await pipeline_registry.record_run(
        pipeline_id=pipeline_id,
        job_id=prepared.job_id,
        mode="build",
        status="QUEUED",
        node_id=prepared.run_context.node_id,
        output_json={"queued": True, "limit": prepared.request_payload.limit},
    )
    build_definition = _build_definition_with_ontology_meta(
        definition_json=prepared.run_context.definition_json,
        ontology_branch=prepared.ontology_branch,
        ontology_head_commit_id=prepared.ontology_head_commit_id,
    )
    job = PipelineJob(
        job_id=prepared.job_id,
        pipeline_id=pipeline_id,
        db_name=prepared.run_context.db_name,
        pipeline_type=prepared.run_context.pipeline.pipeline_type if prepared.run_context.pipeline else "batch",
        definition_json=build_definition,
        definition_hash=prepared.definition_hash,
        definition_commit_id=prepared.definition_commit_id,
        node_id=prepared.run_context.node_id,
        output_dataset_name="build_output",
        mode="build",
        preview_limit=prepared.request_payload.limit,
        branch=prepared.run_context.pipeline.branch,
    )
    await publish_build_pipeline_job(
        pipeline_job_queue=pipeline_job_queue,
        pipeline_registry=pipeline_registry,
        job=job,
        pipeline_id=pipeline_id,
        node_id=prepared.run_context.node_id,
    )
    principal_type, principal_id = _resolve_principal(request)
    await _emit_build_requested_event(
        emit_pipeline_control_plane_event=emit_pipeline_control_plane_event,
        pipeline_id=pipeline_id,
        job_id=prepared.job_id,
        db_name=prepared.run_context.db_name,
        branch=prepared.run_context.pipeline.branch,
        node_id=prepared.run_context.node_id,
        definition_hash=prepared.definition_hash,
        limit=prepared.request_payload.limit,
        principal_type=principal_type,
        principal_id=principal_id,
    )
    return ApiResponse.success(
        message="Build queued",
        data={
            "pipeline_id": pipeline_id,
            "job_id": prepared.job_id,
            "limit": prepared.request_payload.limit,
            "preflight": prepared.preflight,
        },
    ).to_dict()


async def record_preview_validation_failure(
    *,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str,
    node_id: Optional[str],
    error_detail: str,
) -> None:
    validation_payload = build_error_envelope(
        service_name="bff",
        message="Pipeline preview validation failed",
        detail=error_detail,
        code=ErrorCode.REQUEST_VALIDATION_FAILED,
        category=ErrorCategory.INPUT,
        status_code=422,
        errors=[error_detail],
        context={"pipeline_id": pipeline_id, "node_id": node_id, "mode": "preview"},
        external_code="PIPELINE_DEFINITION_INVALID",
    )
    await pipeline_registry.record_preview(
        pipeline_id=pipeline_id,
        status="FAILED",
        row_count=0,
        sample_json=validation_payload,
        node_id=node_id,
    )
    await pipeline_registry.record_run(
        pipeline_id=pipeline_id,
        job_id=f"preview-{pipeline_id}-invalid",
        mode="preview",
        status="FAILED",
        node_id=node_id,
        sample_json=validation_payload,
        finished_at=utcnow(),
    )


async def log_pipeline_preview_failure(
    *,
    audit_store: AuditLogStoreDep,
    request: Any,
    pipeline_id: str,
    error_detail: str,
) -> None:
    await _log_pipeline_audit(
        audit_store,
        request=request,
        action="PIPELINE_PREVIEW_REQUESTED",
        status="failure",
        pipeline_id=pipeline_id,
        error=error_detail,
    )
    logger.error("Failed to preview pipeline: %s", error_detail)
