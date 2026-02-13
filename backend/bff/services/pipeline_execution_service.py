"""
Pipeline execution domain logic (BFF).

Extracted from `bff.routers.pipeline_execution` to keep routers thin.
"""

import asyncio
import logging
from typing import Any, Dict, Optional
from uuid import uuid4

import httpx
from fastapi import HTTPException, Request, status
from bff.routers.pipeline_ops import (
    _detect_breaking_schema_changes,
    _normalize_dependencies_payload,
    _pipeline_requires_proposal,
    _resolve_definition_commit_id,
    _resolve_output_pk_columns,
    _run_pipeline_preflight,
    _stable_definition_hash,
    _validate_dependency_targets,
)
from bff.routers.pipeline_shared import (
    _ensure_pipeline_permission,
    _log_pipeline_audit,
    _require_pipeline_idempotency_key,
    _resolve_principal,
)
from bff.services.oms_client import OMSClient
from bff.services.ontology_occ_guard_service import (
    fetch_branch_head_commit_id,
    resolve_branch_head_commit_with_bootstrap,
)
from shared.config.app_config import AppConfig
from shared.dependencies.providers import AuditLogStoreDep, LineageStoreDep
from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode, classified_http_exception
from shared.models.pipeline_job import PipelineJob
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.services.pipeline.pipeline_job_queue import PipelineJobQueue
from shared.services.pipeline.pipeline_profiler import compute_column_stats
from shared.services.pipeline.pipeline_scheduler import _is_valid_cron_expression
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.storage.lakefs_client import LakeFSConflictError, LakeFSError
from shared.utils.branch_utils import protected_branch_write_message
from shared.utils.event_utils import build_command_event
from shared.utils.key_spec import normalize_key_spec
from shared.utils.s3_uri import build_s3_uri, parse_s3_uri
from shared.utils.schema_hash import compute_schema_hash
from shared.utils.time_utils import utcnow
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


@trace_external_call("bff.pipeline_execution.preview_pipeline")
async def preview_pipeline(
    *,
    pipeline_id: str,
    payload: Dict[str, Any],
    request: Request | Any = None,
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry,
    pipeline_job_queue: PipelineJobQueue,
    dataset_registry: DatasetRegistry,
    event_store: Any,
) -> ApiResponse:
    try:
        # Require idempotency key for preview operations
        idempotency_key = _require_pipeline_idempotency_key(request, operation="pipeline preview")
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="edit",
        )
        sanitized = sanitize_input(payload)
        limit = int(sanitized.get("limit") or 200)
        limit = max(1, min(limit, 500))
        definition_json = sanitized.get("definition_json") if isinstance(sanitized.get("definition_json"), dict) else None
        node_id = str(sanitized.get("node_id") or "").strip() or None
        db_name = str(sanitized.get("db_name") or "").strip()
        expectations = sanitized.get("expectations") if isinstance(sanitized.get("expectations"), list) else None
        schema_contract = sanitized.get("schema_contract") if isinstance(sanitized.get("schema_contract"), list) else None
        sampling_strategy = sanitized.get("sampling_strategy") or sanitized.get("samplingStrategy")
        if sampling_strategy is not None and not isinstance(sampling_strategy, dict):
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "sampling_strategy must be an object", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        branch = str(sanitized.get("branch") or "").strip() or None
        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Pipeline not found", code=ErrorCode.PIPELINE_NOT_FOUND)
        branch_state = await pipeline_registry.get_pipeline_branch(db_name=pipeline.db_name, branch=pipeline.branch)
        if branch_state and branch_state.get("archived"):
            raise classified_http_exception(status.HTTP_409_CONFLICT, "Archived branch cannot be built; restore the branch first", code=ErrorCode.CONFLICT)
        if branch and branch != pipeline.branch:
            raise classified_http_exception(status.HTTP_409_CONFLICT, "branch does not match pipeline branch", code=ErrorCode.CONFLICT)
        latest = await pipeline_registry.get_latest_version(
            pipeline_id=pipeline_id,
            branch=pipeline.branch,
        )
        if not definition_json:
            definition_json = latest.definition_json if latest else {}
        if not db_name:
            db_name = pipeline.db_name if pipeline else ""
        if not db_name:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "db_name is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        if expectations is not None:
            definition_json = {**definition_json, "expectations": expectations}
        if schema_contract is not None:
            definition_json = {**definition_json, "schemaContract": schema_contract}

        preflight = await _run_pipeline_preflight(
            definition_json=definition_json,
            db_name=db_name,
            branch=branch or pipeline.branch,
            dataset_registry=dataset_registry,
        )

        definition_hash = _stable_definition_hash(definition_json)
        definition_commit_id = _resolve_definition_commit_id(definition_json, latest, definition_hash)
        node_key = (node_id or "pipeline").replace("node-", "").replace("/", "-").replace(" ", "-")
        node_key = node_key[:16] if node_key else "pipeline"
        hash_key = str(definition_hash or uuid4().hex)[:12]
        job_id = f"preview-{pipeline_id}-{hash_key}-{node_key}"

        existing_run = await pipeline_registry.get_run(pipeline_id=pipeline_id, job_id=job_id)
        if existing_run:
            status_value = str(existing_run.get("status") or "").upper()
            sample_json_existing = existing_run.get("sample_json") if isinstance(existing_run.get("sample_json"), dict) else {}
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

        preview_definition = dict(definition_json)
        preview_meta = dict(preview_definition.get("__preview_meta__") or {})
        preview_meta["branch"] = branch or "main"
        if sampling_strategy:
            preview_meta["sampling_strategy"] = sampling_strategy
        preview_definition["__preview_meta__"] = preview_meta
        job = PipelineJob(
            job_id=job_id,
            pipeline_id=pipeline_id,
            db_name=db_name,
            pipeline_type=pipeline.pipeline_type if pipeline else "batch",
            definition_json=preview_definition,
            definition_hash=definition_hash,
            definition_commit_id=definition_commit_id,
            node_id=node_id,
            output_dataset_name="preview_output",
            mode="preview",
            preview_limit=limit,
            branch=pipeline.branch,
        )
        try:
            await pipeline_job_queue.publish(job)
        except Exception as exc:
            error = str(exc)
            error_payload = build_error_envelope(
                service_name="bff",
                message="Failed to enqueue pipeline preview job",
                detail=error,
                code=ErrorCode.INTERNAL_ERROR,
                category=ErrorCategory.INTERNAL,
                status_code=500,
                errors=[error],
                context={
                    "pipeline_id": pipeline_id,
                    "job_id": job_id,
                    "node_id": node_id,
                    "mode": "preview",
                    "queued": False,
                },
                external_code="PIPELINE_EXECUTION_FAILED",
            )
            await pipeline_registry.record_preview(
                pipeline_id=pipeline_id,
                status="FAILED",
                row_count=0,
                sample_json=error_payload,
                job_id=job_id,
                node_id=node_id,
            )
            await pipeline_registry.record_run(
                pipeline_id=pipeline_id,
                job_id=job_id,
                mode="preview",
                status="FAILED",
                node_id=node_id,
                sample_json=error_payload,
                finished_at=utcnow(),
            )
            logger.error("Failed to enqueue pipeline preview job %s: %s", job_id, exc)
            raise classified_http_exception(status.HTTP_503_SERVICE_UNAVAILABLE, "Failed to enqueue preview job", code=ErrorCode.UPSTREAM_UNAVAILABLE)

        sample_payload = {"queued": True, "job_id": job_id}

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
        except Exception as e:
            logger.warning(f"Failed to append pipeline preview event: {e}")

        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PREVIEW_REQUESTED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={"job_id": job_id, "node_id": node_id, "limit": limit},
        )

        return ApiResponse.success(
            message="Preview queued",
            data={
                "pipeline_id": pipeline_id,
                "job_id": job_id,
                "limit": limit,
                "preflight": preflight,
                "sample": sample_payload,
            },
        ).to_dict()
    except ValueError as e:
        validation_payload = build_error_envelope(
            service_name="bff",
            message="Pipeline preview validation failed",
            detail=str(e),
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            status_code=422,
            errors=[str(e)],
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
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.PIPELINE_VALIDATION_FAILED)
    except HTTPException:
        raise
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PREVIEW_REQUESTED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(e),
        )
        logger.error(f"Failed to preview pipeline: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@trace_external_call("bff.pipeline_execution.build_pipeline")
async def build_pipeline(
    *,
    pipeline_id: str,
    payload: Dict[str, Any],
    request: Request | Any = None,
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry,
    pipeline_job_queue: PipelineJobQueue,
    dataset_registry: DatasetRegistry,
    oms_client: OMSClient,
    emit_pipeline_control_plane_event: Any,
) -> ApiResponse:
    try:
        # Require idempotency key for build operations
        idempotency_key = _require_pipeline_idempotency_key(request, operation="pipeline build")
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="edit",
        )
        sanitized = sanitize_input(payload)
        limit = int(sanitized.get("limit") or 200)
        limit = max(1, min(limit, 500))
        definition_json = sanitized.get("definition_json") if isinstance(sanitized.get("definition_json"), dict) else None
        node_id = str(sanitized.get("node_id") or "").strip() or None
        db_name = str(sanitized.get("db_name") or "").strip()
        expectations = sanitized.get("expectations") if isinstance(sanitized.get("expectations"), list) else None
        schema_contract = sanitized.get("schema_contract") if isinstance(sanitized.get("schema_contract"), list) else None
        branch = str(sanitized.get("branch") or "").strip() or None

        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Pipeline not found", code=ErrorCode.PIPELINE_NOT_FOUND)
        is_streaming_pipeline = str(pipeline.pipeline_type or "").strip().lower() in {"stream", "streaming"}
        if is_streaming_pipeline:
            node_id = None
        branch_state = await pipeline_registry.get_pipeline_branch(db_name=pipeline.db_name, branch=pipeline.branch)
        if branch_state and branch_state.get("archived"):
            raise classified_http_exception(status.HTTP_409_CONFLICT, "Archived branch cannot be deployed; restore the branch first", code=ErrorCode.CONFLICT)
        if branch and branch != pipeline.branch:
            raise classified_http_exception(status.HTTP_409_CONFLICT, "branch does not match pipeline branch", code=ErrorCode.CONFLICT)
        latest = await pipeline_registry.get_latest_version(
            pipeline_id=pipeline_id,
            branch=pipeline.branch,
        )
        if not definition_json:
            definition_json = latest.definition_json if latest else {}
        if not db_name:
            db_name = pipeline.db_name if pipeline else ""
        if not db_name:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "db_name is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        db_name = validate_db_name(db_name)

        if expectations is not None:
            definition_json = {**definition_json, "expectations": expectations}
        if schema_contract is not None:
            definition_json = {**definition_json, "schemaContract": schema_contract}

        preflight = await _run_pipeline_preflight(
            definition_json=definition_json,
            db_name=db_name,
            branch=branch or pipeline.branch,
            dataset_registry=dataset_registry,
        )
        if preflight.get("has_blocking_errors"):
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Pipeline preflight checks failed",
                code=ErrorCode.CONFLICT,
                category=ErrorCategory.CONFLICT,
                extra={"preflight": preflight},
            )

        definition_hash = _stable_definition_hash(definition_json)
        definition_commit_id = _resolve_definition_commit_id(definition_json, latest, definition_hash)

        job_id = f"build-{pipeline_id}-{uuid4()}"
        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=job_id,
            mode="build",
            status="QUEUED",
            node_id=node_id,
            output_json={"queued": True, "limit": limit},
        )

        ontology_branch = branch or pipeline.branch or "main"
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
            logger.warning("Failed to resolve ontology head commit (db=%s branch=%s): %s", db_name, ontology_branch, exc)

        if not ontology_head_commit_id:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Ontology head commit is unknown; create/publish ontology on this branch before building pipelines",
                code=ErrorCode.CONFLICT,
                category=ErrorCategory.CONFLICT,
                extra={"branch": ontology_branch},
            )

        build_definition = dict(definition_json)
        build_definition["__build_meta__"] = {
            "branch": ontology_branch,
            "ontology": {"branch": ontology_branch, "commit": ontology_head_commit_id},
        }
        job = PipelineJob(
            job_id=job_id,
            pipeline_id=pipeline_id,
            db_name=db_name,
            pipeline_type=pipeline.pipeline_type if pipeline else "batch",
            definition_json=build_definition,
            definition_hash=definition_hash,
            definition_commit_id=definition_commit_id,
            node_id=node_id,
            output_dataset_name="build_output",
            mode="build",
            preview_limit=limit,
            branch=pipeline.branch,
        )
        try:
            await pipeline_job_queue.publish(job)
        except Exception as exc:
            error = str(exc)
            error_payload = build_error_envelope(
                service_name="bff",
                message="Failed to enqueue pipeline build job",
                detail=error,
                code=ErrorCode.INTERNAL_ERROR,
                category=ErrorCategory.INTERNAL,
                status_code=500,
                errors=[error],
                context={
                    "pipeline_id": pipeline_id,
                    "job_id": job_id,
                    "node_id": node_id,
                    "mode": "build",
                },
                external_code="PIPELINE_EXECUTION_FAILED",
            )
            await pipeline_registry.record_run(
                pipeline_id=pipeline_id,
                job_id=job_id,
                mode="build",
                status="FAILED",
                node_id=node_id,
                output_json=error_payload,
                finished_at=utcnow(),
            )
            logger.error("Failed to enqueue pipeline build job %s: %s", job_id, exc)
            raise classified_http_exception(status.HTTP_503_SERVICE_UNAVAILABLE, "Failed to enqueue build job", code=ErrorCode.UPSTREAM_UNAVAILABLE)

        principal_type, principal_id = _resolve_principal(request)
        await emit_pipeline_control_plane_event(
            event_type="PIPELINE_BUILD_REQUESTED",
            pipeline_id=pipeline_id,
            event_id=f"build-requested-{job_id}",
            actor=principal_id,
            data={
                "pipeline_id": pipeline_id,
                "job_id": job_id,
                "db_name": db_name,
                "branch": pipeline.branch,
                "node_id": node_id,
                "definition_hash": definition_hash,
                "limit": limit,
                "principal_type": principal_type,
                "principal_id": principal_id,
            },
        )

        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BUILD_REQUESTED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={"job_id": job_id, "node_id": node_id, "limit": limit},
        )

        return ApiResponse.success(
            message="Build queued",
            data={"pipeline_id": pipeline_id, "job_id": job_id, "limit": limit, "preflight": preflight},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BUILD_REQUESTED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(e),
        )
        logger.error(f"Failed to build pipeline: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@trace_external_call("bff.pipeline_execution.deploy_pipeline")
async def deploy_pipeline(
    *,
    pipeline_id: str,
    payload: Dict[str, Any],
    request: Request | Any,
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    oms_client: OMSClient,
    lineage_store: LineageStoreDep,
    audit_store: AuditLogStoreDep,
    emit_pipeline_control_plane_event: Any,
    _acquire_pipeline_publish_lock: Any,
    _release_pipeline_publish_lock: Any,
) -> ApiResponse:
    try:
        # Require idempotency key for deploy operations
        idempotency_key = _require_pipeline_idempotency_key(request, operation="pipeline deploy")
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="approve",
        )
        sanitized = sanitize_input(payload)
        output = sanitized.get("output") if isinstance(sanitized.get("output"), dict) else {}
        definition_json = sanitized.get("definition_json") if isinstance(sanitized.get("definition_json"), dict) else None
        promote_build = bool(sanitized.get("promote_build") or sanitized.get("promoteBuild") or False)
        build_job_id = str(sanitized.get("build_job_id") or sanitized.get("buildJobId") or "").strip() or None
        artifact_id = str(sanitized.get("artifact_id") or sanitized.get("artifactId") or "").strip() or None
        replay_on_deploy = bool(
            sanitized.get("replay")
            or sanitized.get("replay_on_deploy")
            or sanitized.get("replayOnDeploy")
            or sanitized.get("replay_on_deploy")
        )
        dependencies_raw: Any = None
        if "dependencies" in sanitized:
            dependencies_raw = sanitized.get("dependencies")
        elif isinstance(definition_json, dict) and "dependencies" in definition_json:
            dependencies_raw = definition_json.get("dependencies")
        dependencies: Optional[list[dict[str, str]]] = None
        node_id = str(sanitized.get("node_id") or "").strip() or None
        dataset_name = str(output.get("dataset_name") or "").strip()
        db_name = str(output.get("db_name") or "").strip()
        outputs = sanitized.get("outputs") if isinstance(sanitized.get("outputs"), list) else None
        expectations = sanitized.get("expectations") if isinstance(sanitized.get("expectations"), list) else None
        schema_contract = sanitized.get("schema_contract") if isinstance(sanitized.get("schema_contract"), list) else None
        schedule_interval_seconds = None
        schedule_cron = None
        schedule = sanitized.get("schedule") or output.get("schedule")
        if isinstance(schedule, dict):
            schedule_interval_seconds = schedule.get("interval_seconds")
            schedule_cron = schedule.get("cron")
        elif isinstance(schedule, (int, float, str)):
            try:
                schedule_interval_seconds = int(schedule)
            except (TypeError, ValueError):
                schedule_interval_seconds = None
        if schedule_cron:
            schedule_cron = str(schedule_cron).strip()
        if schedule_interval_seconds is not None:
            try:
                schedule_interval_seconds = int(schedule_interval_seconds)
            except Exception:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "schedule_interval_seconds must be integer", code=ErrorCode.REQUEST_VALIDATION_FAILED)
            if schedule_interval_seconds <= 0:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "schedule_interval_seconds must be > 0", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        if schedule_interval_seconds and schedule_cron:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "Provide either schedule_interval_seconds or schedule_cron (not both)", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        if schedule_cron and not _is_valid_cron_expression(str(schedule_cron)):
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "schedule_cron must be a supported 5-field cron expression", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        branch = str(sanitized.get("branch") or "").strip() or None
        proposal_status = str(sanitized.get("proposal_status") or "").strip() or None
        proposal_title = str(sanitized.get("proposal_title") or "").strip() or None
        proposal_description = str(sanitized.get("proposal_description") or "").strip() or None
        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Pipeline not found", code=ErrorCode.PIPELINE_NOT_FOUND)
        if branch and branch != pipeline.branch:
            raise classified_http_exception(status.HTTP_409_CONFLICT, "branch does not match pipeline branch", code=ErrorCode.CONFLICT)
        if dependencies_raw is not None:
            dependencies = _normalize_dependencies_payload(dependencies_raw)
            await _validate_dependency_targets(
                pipeline_registry,
                db_name=str(pipeline.db_name),
                pipeline_id=pipeline_id,
                dependencies=dependencies,
            )

        resolved_branch = branch or (pipeline.branch if pipeline else None) or "main"
        proposal_required = _pipeline_requires_proposal(resolved_branch)
        proposal_bundle = getattr(pipeline, "proposal_bundle", {}) if pipeline else {}
        if proposal_required:
            if getattr(pipeline, "proposal_status", None) != "approved":
                raise classified_http_exception(status.HTTP_409_CONFLICT, protected_branch_write_message(), code=ErrorCode.CONFLICT)
            if not proposal_bundle:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Approved proposal is missing bundle metadata", code=ErrorCode.CONFLICT)
        latest = await pipeline_registry.get_latest_version(
            pipeline_id=pipeline_id,
            branch=resolved_branch,
        )
        if not definition_json:
            definition_json = latest.definition_json if latest else {}
        if expectations is not None:
            definition_json = {**(definition_json or {}), "expectations": expectations}
        if schema_contract is not None:
            definition_json = {**(definition_json or {}), "schemaContract": schema_contract}
        if outputs:
            definition_json = {**(definition_json or {}), "outputs": outputs}
        if not db_name:
            db_name = pipeline.db_name if pipeline else ""
        if not db_name:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "db_name is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        db_name = validate_db_name(db_name)

        definition_hash = _stable_definition_hash(definition_json or {})
        definition_commit_id = _resolve_definition_commit_id(definition_json or {}, latest, definition_hash)

        if not promote_build:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "deploy requires promote_build; run /build then /deploy with promote_build", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        if promote_build:
            artifact_record = None
            if artifact_id:
                artifact_record = await pipeline_registry.get_artifact(artifact_id=artifact_id)
                if not artifact_record:
                    raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Build artifact not found", code=ErrorCode.RESOURCE_NOT_FOUND)
                if artifact_record.pipeline_id != pipeline_id:
                    raise classified_http_exception(status.HTTP_409_CONFLICT, "artifact_id does not belong to this pipeline", code=ErrorCode.CONFLICT)
                if str(artifact_record.mode or "").lower() != "build":
                    raise classified_http_exception(status.HTTP_409_CONFLICT, "artifact_id is not a build artifact", code=ErrorCode.CONFLICT)
                if str(artifact_record.status or "").upper() != "SUCCESS":
                    raise classified_http_exception(status.HTTP_409_CONFLICT, "artifact_id is not a successful build artifact", code=ErrorCode.CONFLICT)
                if build_job_id and artifact_record.job_id != build_job_id:
                    raise classified_http_exception(status.HTTP_409_CONFLICT, "build_job_id does not match artifact_id", code=ErrorCode.CONFLICT)
                build_job_id = build_job_id or artifact_record.job_id

            if not build_job_id:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "build_job_id is required for promote_build", code=ErrorCode.REQUEST_VALIDATION_FAILED)

            build_run = await pipeline_registry.get_run(pipeline_id=pipeline_id, job_id=build_job_id)
            if not build_run:
                raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Build run not found", code=ErrorCode.RESOURCE_NOT_FOUND)
            if str(build_run.get("mode") or "").lower() != "build":
                raise classified_http_exception(status.HTTP_409_CONFLICT, "build_job_id is not a build run", code=ErrorCode.CONFLICT)
            build_status = str(build_run.get("status") or "").upper()
            if build_status != "SUCCESS":
                output_json = build_run.get("output_json")
                errors: list[str] = []
                if isinstance(output_json, dict):
                    raw_errors = output_json.get("errors")
                    if isinstance(raw_errors, list):
                        errors = [str(item) for item in raw_errors if str(item).strip()]
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Build is not successful yet",
                    code=ErrorCode.PIPELINE_BUILD_FAILED,
                    category=ErrorCategory.CONFLICT,
                    extra={
                        "build_status": build_status or None,
                        "errors": errors,
                        "build_job_id": build_job_id,
                    },
                )

            output_json = build_run.get("output_json") or {}
            if not isinstance(output_json, dict):
                output_json = {}
            build_hash = str(output_json.get("definition_hash") or "").strip()
            if build_hash and build_hash != definition_hash:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Build definition does not match deploy definition", code=ErrorCode.CONFLICT)
            if artifact_record and artifact_record.definition_hash and artifact_record.definition_hash != definition_hash:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Build artifact definition does not match deploy definition", code=ErrorCode.CONFLICT)

            if artifact_record is None:
                artifact_record = await pipeline_registry.get_artifact_by_job(
                    pipeline_id=pipeline_id,
                    job_id=build_job_id,
                    mode="build",
                )

            build_branch = str(output_json.get("branch") or "").strip() or "main"
            if build_branch != resolved_branch:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Build branch does not match deploy branch", code=ErrorCode.CONFLICT)

            build_ontology = output_json.get("ontology") if isinstance(output_json.get("ontology"), dict) else {}
            build_ontology_commit = str(build_ontology.get("commit") or "").strip() or None
            if not build_ontology_commit:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Build output is missing ontology commit id; re-run build after ontology is ready",
                    code=ErrorCode.CONFLICT,
                    category=ErrorCategory.CONFLICT,
                )

            if proposal_required:
                bundle_build_job_id = str(proposal_bundle.get("build_job_id") or "").strip()
                if not bundle_build_job_id:
                    raise classified_http_exception(status.HTTP_409_CONFLICT, "Approved proposal is missing build_job_id", code=ErrorCode.CONFLICT)
                if bundle_build_job_id != build_job_id:
                    raise classified_http_exception(
                        status.HTTP_409_CONFLICT,
                        "Approved proposal build does not match deploy build",
                        code=ErrorCode.CONFLICT,
                        category=ErrorCategory.CONFLICT,
                        extra={
                            "proposal_build_job_id": bundle_build_job_id,
                            "deploy_build_job_id": build_job_id,
                        },
                    )
                bundle_artifact_id = str(proposal_bundle.get("artifact_id") or "").strip()
                if bundle_artifact_id and artifact_record and bundle_artifact_id != artifact_record.artifact_id:
                    raise classified_http_exception(
                        status.HTTP_409_CONFLICT,
                        "Approved proposal artifact does not match deploy artifact",
                        code=ErrorCode.CONFLICT,
                        category=ErrorCategory.CONFLICT,
                        extra={
                            "proposal_artifact_id": bundle_artifact_id,
                            "deploy_artifact_id": artifact_record.artifact_id,
                        },
                    )
                bundle_definition_hash = str(proposal_bundle.get("definition_hash") or "").strip()
                if bundle_definition_hash and bundle_definition_hash != definition_hash:
                    raise classified_http_exception(
                        status.HTTP_409_CONFLICT,
                        "Approved proposal definition hash does not match deploy definition",
                        code=ErrorCode.CONFLICT,
                        category=ErrorCategory.CONFLICT,
                        extra={
                            "proposal_definition_hash": bundle_definition_hash,
                            "deploy_definition_hash": definition_hash,
                        },
                    )
                bundle_ontology_commit = str((proposal_bundle.get("ontology") or {}).get("commit") or "").strip()
                if bundle_ontology_commit and bundle_ontology_commit != build_ontology_commit:
                    raise classified_http_exception(
                        status.HTTP_409_CONFLICT,
                        "Approved proposal ontology commit does not match build",
                        code=ErrorCode.CONFLICT,
                        category=ErrorCategory.CONFLICT,
                        extra={
                            "proposal_ontology_commit": bundle_ontology_commit,
                            "build_ontology_commit": build_ontology_commit,
                        },
                    )
                bundle_lakefs_commit = str((proposal_bundle.get("lakefs") or {}).get("commit_id") or "").strip()
                build_lakefs_commit = str((output_json.get("lakefs") or {}).get("commit_id") or "").strip()
                if bundle_lakefs_commit and build_lakefs_commit and bundle_lakefs_commit != build_lakefs_commit:
                    raise classified_http_exception(
                        status.HTTP_409_CONFLICT,
                        "Approved proposal lakeFS commit does not match build",
                        code=ErrorCode.LAKEFS_CONFLICT,
                        category=ErrorCategory.CONFLICT,
                        extra={
                            "proposal_commit_id": bundle_lakefs_commit,
                            "build_commit_id": build_lakefs_commit,
                        },
                    )
                mapping_specs_payload = proposal_bundle.get("mapping_specs")
                if isinstance(mapping_specs_payload, list):
                    mismatches: list[dict[str, Any]] = []
                    for item in mapping_specs_payload:
                        if not isinstance(item, dict):
                            continue
                        dataset_id = str(item.get("dataset_id") or "").strip()
                        dataset_branch = str(item.get("dataset_branch") or resolved_branch).strip() or resolved_branch
                        target_class_id = str(item.get("target_class_id") or "").strip() or None
                        expected_spec_id = str(item.get("mapping_spec_id") or "").strip()
                        expected_version = item.get("mapping_spec_version")
                        if not dataset_id:
                            mismatches.append(
                                {
                                    "reason": "missing_dataset_id",
                                    "mapping_spec_id": expected_spec_id or None,
                                }
                            )
                            continue
                        current = await objectify_registry.get_active_mapping_spec(
                            dataset_id=dataset_id,
                            dataset_branch=dataset_branch,
                            target_class_id=target_class_id,
                        )
                        if not current:
                            mismatches.append(
                                {
                                    "reason": "mapping_spec_inactive",
                                    "dataset_id": dataset_id,
                                    "dataset_branch": dataset_branch,
                                    "target_class_id": target_class_id,
                                    "mapping_spec_id": expected_spec_id or None,
                                }
                            )
                            continue
                        if expected_spec_id and current.mapping_spec_id != expected_spec_id:
                            mismatches.append(
                                {
                                    "reason": "mapping_spec_id_mismatch",
                                    "dataset_id": dataset_id,
                                    "dataset_branch": dataset_branch,
                                    "target_class_id": target_class_id,
                                    "proposal_mapping_spec_id": expected_spec_id,
                                    "current_mapping_spec_id": current.mapping_spec_id,
                                }
                            )
                        if expected_version is not None:
                            try:
                                expected_version_int = int(expected_version)
                            except Exception:
                                logging.getLogger(__name__).warning("Broad exception fallback at bff/services/pipeline_execution_service.py:844", exc_info=True)
                                expected_version_int = None
                            if expected_version_int is not None and expected_version_int != current.version:
                                mismatches.append(
                                    {
                                        "reason": "mapping_spec_version_mismatch",
                                        "dataset_id": dataset_id,
                                        "dataset_branch": dataset_branch,
                                        "target_class_id": target_class_id,
                                        "proposal_version": expected_version_int,
                                        "current_version": current.version,
                                    }
                                )
                    if mismatches:
                        raise classified_http_exception(
                            status.HTTP_409_CONFLICT,
                            "Approved proposal mapping specs no longer match active specs",
                            code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                            category=ErrorCategory.CONFLICT,
                            extra={"mismatches": mismatches},
                        )
            try:
                prod_head_commit = await fetch_branch_head_commit_id(
                    oms_client=oms_client,
                    db_name=db_name,
                    branch=resolved_branch,
                )
            except Exception as exc:
                raise classified_http_exception(
                    status.HTTP_503_SERVICE_UNAVAILABLE,
                    "Failed to verify ontology head commit; OMS unavailable",
                    code=ErrorCode.OMS_UNAVAILABLE,
                    category=ErrorCategory.UPSTREAM,
                    extra={"error": str(exc)},
                ) from exc
            if not prod_head_commit:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Target branch ontology head commit is unknown; publish ontology first",
                    code=ErrorCode.CONFLICT,
                    category=ErrorCategory.CONFLICT,
                    extra={"target_branch": resolved_branch},
                )
            if build_ontology_commit != prod_head_commit:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Ontology version mismatch; publish ontology and rebuild before deploy",
                    code=ErrorCode.TERMINUS_CONFLICT,
                    category=ErrorCategory.CONFLICT,
                    extra={
                        "bundle_terminus_commit_id": build_ontology_commit,
                        "prod_head_commit_id": prod_head_commit,
                        "target_branch": resolved_branch,
                    },
                )

            outputs_payload = None
            if artifact_record and artifact_record.outputs:
                outputs_payload = artifact_record.outputs
            if outputs_payload is None:
                outputs_payload = output_json.get("outputs")
            outputs_list: list[dict[str, Any]] = []
            if isinstance(outputs_payload, list):
                outputs_list = [item for item in outputs_payload if isinstance(item, dict)]

            execution_semantics = str(output_json.get("execution_semantics") or "").strip().lower()
            is_streaming_promotion = execution_semantics == "streaming" or str(pipeline.pipeline_type or "").strip().lower() in {"stream", "streaming"}
            if not node_id and not is_streaming_promotion:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "node_id is required for promote_build", code=ErrorCode.REQUEST_VALIDATION_FAILED)
            selected_outputs: list[dict[str, Any]]
            if is_streaming_promotion:
                if not outputs_list:
                    raise classified_http_exception(status.HTTP_409_CONFLICT, "Build outputs are missing", code=ErrorCode.PIPELINE_BUILD_FAILED)
                selected_outputs = outputs_list
            else:
                selected_output = None
                for item in outputs_list:
                    if str(item.get("node_id") or "").strip() == str(node_id).strip():
                        selected_output = item
                        break
                if not selected_output:
                    raise classified_http_exception(status.HTTP_409_CONFLICT, "Build output for node_id not found", code=ErrorCode.PIPELINE_BUILD_FAILED)
                selected_outputs = [selected_output]

            first_artifact_key = str(selected_outputs[0].get("artifact_key") or "").strip()
            if not first_artifact_key:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Build staged artifact_key is missing", code=ErrorCode.PIPELINE_BUILD_FAILED)
            parsed_first = parse_s3_uri(first_artifact_key)
            if not parsed_first:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Build artifact_key is not a valid s3:// URI", code=ErrorCode.PIPELINE_BUILD_FAILED)
            staged_bucket, _ = parsed_first

            lakefs_meta = output_json.get("lakefs") if isinstance(output_json.get("lakefs"), dict) else {}
            if artifact_record:
                if artifact_record.lakefs_repository:
                    lakefs_meta.setdefault("repository", artifact_record.lakefs_repository)
                if artifact_record.lakefs_branch:
                    lakefs_meta.setdefault("build_branch", artifact_record.lakefs_branch)
                if artifact_record.lakefs_commit_id:
                    lakefs_meta.setdefault("commit_id", artifact_record.lakefs_commit_id)
            expected_repo = str(lakefs_meta.get("repository") or "").strip()
            expected_build_branch = str(lakefs_meta.get("build_branch") or "").strip()
            if expected_repo and expected_repo != staged_bucket:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Build artifact_key repository does not match build metadata",
                    code=ErrorCode.CONFLICT,
                    category=ErrorCategory.CONFLICT,
                    extra={
                        "artifact_bucket": staged_bucket,
                        "expected_repository": expected_repo,
                    },
                )
            if not expected_build_branch:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Build output is missing lakefs.build_branch",
                    code=ErrorCode.PIPELINE_BUILD_FAILED,
                    category=ErrorCategory.CONFLICT,
                )
            build_ref = expected_build_branch
            staged_prefix = f"{build_ref}/"
            normalized_outputs: list[dict[str, Any]] = []
            any_breaking_changes: list[dict[str, Any]] = []
            for item in selected_outputs:
                staged_dataset_name = str(item.get("dataset_name") or item.get("datasetName") or "").strip()
                if not staged_dataset_name:
                    raise classified_http_exception(status.HTTP_409_CONFLICT, "Build dataset_name is missing", code=ErrorCode.PIPELINE_BUILD_FAILED)
                staged_artifact_key = str(item.get("artifact_key") or "").strip()
                if not staged_artifact_key:
                    raise classified_http_exception(status.HTTP_409_CONFLICT, "Build staged artifact_key is missing", code=ErrorCode.PIPELINE_BUILD_FAILED)
                parsed = parse_s3_uri(staged_artifact_key)
                if not parsed:
                    raise classified_http_exception(status.HTTP_409_CONFLICT, "Build artifact_key is not a valid s3:// URI", code=ErrorCode.PIPELINE_BUILD_FAILED)
                bucket, key = parsed
                if bucket != staged_bucket:
                    raise classified_http_exception(
                        status.HTTP_409_CONFLICT,
                        "Build outputs must use a single lakeFS repository",
                        code=ErrorCode.CONFLICT,
                        category=ErrorCategory.CONFLICT,
                        extra={
                            "artifact_bucket": bucket,
                            "expected_repository": staged_bucket,
                        },
                    )
                if not key.startswith(staged_prefix):
                    raise classified_http_exception(
                        status.HTTP_409_CONFLICT,
                        "Build artifact_key does not match build branch metadata",
                        code=ErrorCode.PIPELINE_BUILD_FAILED,
                        category=ErrorCategory.CONFLICT,
                        extra={
                            "artifact_key": staged_artifact_key,
                            "artifact_path": key,
                            "expected_build_branch": build_ref,
                        },
                    )
                artifact_path = key[len(staged_prefix) :]
                if not artifact_path:
                    raise classified_http_exception(
                        status.HTTP_409_CONFLICT,
                        "Build artifact_key is missing artifact path after build ref",
                        code=ErrorCode.PIPELINE_BUILD_FAILED,
                        category=ErrorCategory.CONFLICT,
                        extra={
                            "artifact_key": staged_artifact_key,
                            "expected_build_branch": build_ref,
                        },
                    )

                schema_columns = item.get("columns") if isinstance(item.get("columns"), list) else []
                sample_rows = item.get("rows") if isinstance(item.get("rows"), list) else []
                normalized_sample_rows = [row for row in sample_rows if isinstance(row, dict)]
                row_count_raw = item.get("row_count")
                delta_row_count_raw = item.get("delta_row_count") if "delta_row_count" in item else item.get("deltaRowCount")
                try:
                    row_count_int = int(row_count_raw) if row_count_raw is not None else None
                except Exception:
                    logging.getLogger(__name__).warning("Broad exception fallback at bff/services/pipeline_execution_service.py:1022", exc_info=True)
                    row_count_int = None
                try:
                    delta_row_count_int = int(delta_row_count_raw) if delta_row_count_raw is not None else None
                except Exception:
                    logging.getLogger(__name__).warning("Broad exception fallback at bff/services/pipeline_execution_service.py:1026", exc_info=True)
                    delta_row_count_int = None

                dataset = await dataset_registry.get_dataset_by_name(
                    db_name=db_name,
                    name=staged_dataset_name,
                    branch=resolved_branch,
                )
                breaking_changes: list[dict[str, str]] = []
                if dataset:
                    breaking_changes = _detect_breaking_schema_changes(
                        previous_schema=dataset.schema_json,
                        next_columns=schema_columns,
                    )
                    if breaking_changes:
                        any_breaking_changes.append(
                            {
                                "dataset_name": staged_dataset_name,
                                "node_id": item.get("node_id"),
                                "breaking_changes": breaking_changes,
                            }
                        )

                normalized_outputs.append(
                    {
                        "node_id": item.get("node_id"),
                        "dataset_name": staged_dataset_name,
                        "build_artifact_key": staged_artifact_key,
                        "artifact_path": artifact_path,
                        "columns": schema_columns,
                        "rows": normalized_sample_rows,
                        "row_count": row_count_int,
                        "delta_row_count": delta_row_count_int,
                        "breaking_changes": breaking_changes,
                    }
                )

            if any_breaking_changes and not replay_on_deploy:
                breaking_payload: Any = any_breaking_changes
                if not is_streaming_promotion and normalized_outputs:
                    breaking_payload = normalized_outputs[0].get("breaking_changes") or []
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Breaking schema change detected; replay_on_deploy is required to proceed",
                    code=ErrorCode.CONFLICT,
                    category=ErrorCategory.CONFLICT,
                    extra={"breaking_changes": breaking_payload},
                )

            promote_job_id = f"promote-{uuid4().hex}"
            principal_type, principal_id = _resolve_principal(request)
            await emit_pipeline_control_plane_event(
                event_type="PIPELINE_DEPLOY_REQUESTED",
                pipeline_id=pipeline_id,
                event_id=f"deploy-requested-{promote_job_id}",
                actor=principal_id,
                data={
                    "pipeline_id": pipeline_id,
                    "promote_job_id": promote_job_id,
                    "build_job_id": build_job_id,
                    "artifact_id": (artifact_record.artifact_id if artifact_record else None),
                    "db_name": db_name,
                    "branch": resolved_branch,
                    "node_id": node_id,
                    "definition_hash": definition_hash,
                    "replay_on_deploy": replay_on_deploy,
                    "principal_type": principal_type,
                    "principal_id": principal_id,
                },
            )

            actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
            lakefs_client = await pipeline_registry.get_lakefs_client(user_id=actor_user_id)
            publish_lock = await _acquire_pipeline_publish_lock(
                pipeline_id=str(pipeline_id),
                branch=resolved_branch,
                job_id=promote_job_id,
            )

            try:
                merge_commit_id = await lakefs_client.merge(
                    repository=staged_bucket,
                    source_ref=build_ref,
                    destination_branch=resolved_branch,
                    message=f"Promote build {build_job_id} -> {resolved_branch} (pipeline {pipeline.db_name}/{pipeline.name})",
                    metadata={
                        "pipeline_id": str(pipeline_id),
                        "db_name": db_name,
                        "pipeline_name": str(pipeline.name),
                        "build_job_id": str(build_job_id),
                        "node_id": str(node_id) if node_id else "*",
                    },
                    # Promotions are intentionally idempotent. A single build branch typically contains
                    # artifacts for *all* output nodes. If one output was already promoted (merge done),
                    # subsequent output promotions for the same build must still succeed so we can
                    # register the remaining dataset versions even when lakeFS reports "no changes".
                    allow_empty=True,
                )
            except LakeFSConflictError as exc:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "lakeFS merge conflict during promotion",
                    code=ErrorCode.LAKEFS_CONFLICT,
                    category=ErrorCategory.CONFLICT,
                    extra={
                        "repository": staged_bucket,
                        "source_ref": build_ref,
                        "destination_branch": resolved_branch,
                        "error": str(exc),
                    },
                ) from exc
            except LakeFSError as exc:
                raise classified_http_exception(
                    status.HTTP_503_SERVICE_UNAVAILABLE,
                    "lakeFS merge failed during promotion",
                    code=ErrorCode.LAKEFS_ERROR,
                    category=ErrorCategory.UPSTREAM,
                    extra={
                        "repository": staged_bucket,
                        "source_ref": build_ref,
                        "destination_branch": resolved_branch,
                        "error": str(exc),
                    },
                ) from exc
            finally:
                if publish_lock:
                    await _release_pipeline_publish_lock(*publish_lock)

            build_outputs: list[dict[str, Any]] = []
            for item in normalized_outputs:
                staged_dataset_name = str(item.get("dataset_name") or "")
                artifact_path = str(item.get("artifact_path") or "")
                promoted_artifact_key = build_s3_uri(staged_bucket, f"{merge_commit_id}/{artifact_path}")
                schema_columns = item.get("columns") if isinstance(item.get("columns"), list) else []
                normalized_sample_rows = item.get("rows") if isinstance(item.get("rows"), list) else []
                row_count_int = item.get("row_count")
                delta_row_count_int = item.get("delta_row_count")

                dataset = await dataset_registry.get_dataset_by_name(
                    db_name=db_name,
                    name=staged_dataset_name,
                    branch=resolved_branch,
                )
                if not dataset:
                    dataset = await dataset_registry.create_dataset(
                        db_name=db_name,
                        name=staged_dataset_name,
                        description=None,
                        source_type="pipeline",
                        source_ref=str(pipeline_id),
                        schema_json={"columns": schema_columns},
                        branch=resolved_branch,
                    )

                schema_hash = compute_schema_hash(schema_columns)
                sample_json: dict[str, Any] = {
                    "columns": schema_columns,
                    "rows": normalized_sample_rows,
                    "row_count": row_count_int,
                    "sample_row_count": len(normalized_sample_rows),
                    "column_stats": compute_column_stats(rows=normalized_sample_rows, columns=schema_columns),
                }
                if delta_row_count_int is not None:
                    sample_json["delta_row_count"] = delta_row_count_int

                dataset_version = await dataset_registry.add_version(
                    dataset_id=dataset.dataset_id,
                    lakefs_commit_id=merge_commit_id,
                    artifact_key=promoted_artifact_key,
                    row_count=row_count_int,
                    sample_json=sample_json,
                    schema_json={"columns": schema_columns},
                    promoted_from_artifact_id=(artifact_record.artifact_id if artifact_record else None),
                )

                pk_columns = _resolve_output_pk_columns(
                    definition_json=definition_json or {},
                    node_id=str(item.get("node_id") or "").strip() or None,
                    output_name=staged_dataset_name,
                )
                existing_key_spec = await dataset_registry.get_key_spec_for_dataset(dataset_id=dataset.dataset_id)
                if pk_columns or existing_key_spec:
                    if existing_key_spec:
                        normalized_spec = normalize_key_spec(existing_key_spec.spec)
                        existing_pk = normalized_spec.get("primary_key") or []
                        if existing_pk and not pk_columns:
                            raise classified_http_exception(
                                status.HTTP_409_CONFLICT,
                                "Pipeline output is missing declared primary key columns",
                                code=ErrorCode.CONFLICT,
                                category=ErrorCategory.CONFLICT,
                                extra={
                                    "dataset_id": dataset.dataset_id,
                                    "expected_primary_key": existing_pk,
                                },
                            )
                        if pk_columns and set(existing_pk) and set(existing_pk) != set(pk_columns):
                            raise classified_http_exception(
                                status.HTTP_409_CONFLICT,
                                "Pipeline output primary key does not match key spec",
                                code=ErrorCode.CONFLICT,
                                category=ErrorCategory.CONFLICT,
                                extra={
                                    "dataset_id": dataset.dataset_id,
                                    "expected_primary_key": existing_pk,
                                    "observed_primary_key": pk_columns,
                                },
                            )
                        if pk_columns and not existing_pk:
                            raise classified_http_exception(
                                status.HTTP_409_CONFLICT,
                                "Pipeline output declares primary key but key spec is empty",
                                code=ErrorCode.CONFLICT,
                                category=ErrorCategory.CONFLICT,
                                extra={
                                    "dataset_id": dataset.dataset_id,
                                    "observed_primary_key": pk_columns,
                                },
                            )
                    elif pk_columns:
                        await dataset_registry.create_key_spec(
                            dataset_id=dataset.dataset_id,
                            spec={
                                "primary_key": pk_columns,
                                "source": "pipeline",
                                "node_id": str(item.get("node_id") or "").strip() or None,
                            },
                        )

                mapping_spec_id = None
                mapping_spec_version = None
                mapping_spec_target_class_id = None
                mapping_specs_payload = None
                if isinstance(proposal_bundle, dict):
                    mapping_specs_payload = proposal_bundle.get("mapping_specs")
                if dataset and isinstance(mapping_specs_payload, list):
                    for spec in mapping_specs_payload:
                        if not isinstance(spec, dict):
                            continue
                        spec_dataset_id = str(spec.get("dataset_id") or "").strip()
                        if spec_dataset_id != dataset.dataset_id:
                            continue
                        spec_branch = str(spec.get("dataset_branch") or resolved_branch).strip() or resolved_branch
                        if spec_branch != dataset.branch:
                            continue
                        mapping_spec_id = str(spec.get("mapping_spec_id") or "").strip() or None
                        mapping_spec_version = spec.get("mapping_spec_version")
                        mapping_spec_target_class_id = str(spec.get("target_class_id") or "").strip() or None
                        break
                if mapping_spec_id is None and dataset:
                    mapping_spec = await objectify_registry.get_active_mapping_spec(
                        dataset_id=dataset.dataset_id,
                        dataset_branch=dataset.branch,
                        artifact_output_name=staged_dataset_name,
                        schema_hash=schema_hash,
                    )
                    if mapping_spec:
                        mapping_spec_id = mapping_spec.mapping_spec_id
                        mapping_spec_version = mapping_spec.version
                        mapping_spec_target_class_id = mapping_spec.target_class_id

                await pipeline_registry.record_promotion_manifest(
                    pipeline_id=pipeline_id,
                    db_name=db_name,
                    build_job_id=build_job_id,
                    artifact_id=(artifact_record.artifact_id if artifact_record else None),
                    definition_hash=definition_hash,
                    lakefs_repository=staged_bucket,
                    lakefs_commit_id=merge_commit_id,
                    ontology_commit_id=build_ontology_commit,
                    mapping_spec_id=mapping_spec_id,
                    mapping_spec_version=mapping_spec_version,
                    mapping_spec_target_class_id=mapping_spec_target_class_id,
                    promoted_dataset_version_id=dataset_version.version_id,
                    promoted_dataset_name=staged_dataset_name,
                    target_branch=resolved_branch,
                    promoted_by=principal_id,
                    metadata={
                        "node_id": item.get("node_id"),
                        "build_artifact_key": item.get("build_artifact_key"),
                    },
                )

                build_outputs.append(
                    {
                        "node_id": item.get("node_id"),
                        "dataset_name": staged_dataset_name,
                        "dataset_id": dataset.dataset_id,  # Critical: Return dataset_id for downstream tools
                        "dataset_version_id": dataset_version.version_id,  # Critical: Return version_id for objectify
                        "artifact_key": promoted_artifact_key,
                        "row_count": row_count_int,
                        "delta_row_count": delta_row_count_int,
                        "build_artifact_key": item.get("build_artifact_key"),
                        "build_ref": build_ref,
                        "artifact_path": artifact_path,
                        "merge_commit_id": merge_commit_id,
                        "breaking_changes": item.get("breaking_changes") or [],
                        "replay_on_deploy": replay_on_deploy,
                        "promoted_from_artifact_id": (artifact_record.artifact_id if artifact_record else None),
                    }
                )

            deployed_commit_id = merge_commit_id
            await pipeline_registry.record_build(
                pipeline_id=pipeline_id,
                status="DEPLOYED",
                output_json={
                    "outputs": build_outputs,
                    "ontology": build_ontology,
                    "promoted_from_build_job_id": build_job_id,
                    "promoted_from_artifact_id": (artifact_record.artifact_id if artifact_record else None),
                    "promote_job_id": promote_job_id,
                    "lakefs": {
                        "repository": staged_bucket,
                        "source_ref": build_ref,
                        "destination_branch": resolved_branch,
                        "merge_commit_id": merge_commit_id,
                    },
                },
                deployed_commit_id=deployed_commit_id,
            )
            await pipeline_registry.record_run(
                pipeline_id=pipeline_id,
                job_id=promote_job_id,
                mode="deploy",
                status="DEPLOYED",
                node_id=node_id,
                row_count=(build_outputs[0].get("row_count") if build_outputs else None),
                pipeline_spec_hash=definition_hash,
                pipeline_spec_commit_id=definition_commit_id,
                output_lakefs_commit_id=merge_commit_id,
                output_json={
                    "outputs": build_outputs,
                    "ontology": build_ontology,
                    "promoted_from_build_job_id": build_job_id,
                    "promoted_from_artifact_id": (artifact_record.artifact_id if artifact_record else None),
                    "lakefs": {
                        "repository": staged_bucket,
                        "source_ref": build_ref,
                        "destination_branch": resolved_branch,
                        "merge_commit_id": merge_commit_id,
                    },
                },
                finished_at=utcnow(),
            )

            await emit_pipeline_control_plane_event(
                event_type="PIPELINE_DEPLOY_PROMOTED",
                pipeline_id=pipeline_id,
                event_id=promote_job_id,
                actor=principal_id,
                data={
                    "pipeline_id": pipeline_id,
                    "promote_job_id": promote_job_id,
                    "build_job_id": build_job_id,
                    "artifact_id": (artifact_record.artifact_id if artifact_record else None),
                    "db_name": db_name,
                    "branch": resolved_branch,
                    "node_id": node_id,
                    "merge_commit_id": merge_commit_id,
                    "definition_hash": definition_hash,
                    "replay_on_deploy": replay_on_deploy,
                    "principal_type": principal_type,
                    "principal_id": principal_id,
                },
            )

            if lineage_store:
                for item in build_outputs:
                    promoted_artifact_key = str(item.get("artifact_key") or "")
                    parsed = parse_s3_uri(promoted_artifact_key)
                    if not parsed:
                        continue
                    bucket, key = parsed
                    try:
                        await lineage_store.record_link(
                            from_node_id=lineage_store.node_aggregate("Pipeline", str(pipeline_id)),
                            to_node_id=lineage_store.node_artifact("s3", bucket, key),
                            edge_type="pipeline_output_stored",
                            occurred_at=utcnow(),
                            db_name=db_name,
                                edge_metadata={
                                    "db_name": db_name,
                                    "pipeline_id": str(pipeline_id),
                                    "artifact_key": promoted_artifact_key,
                                    "dataset_name": item.get("dataset_name"),
                                    "node_id": item.get("node_id"),
                                    "promoted_from_build_job_id": build_job_id,
                                    "promoted_from_artifact_id": (artifact_record.artifact_id if artifact_record else None),
                                    "build_artifact_key": item.get("build_artifact_key"),
                                    "build_ref": build_ref,
                                    "artifact_path": item.get("artifact_path"),
                                    "merge_commit_id": merge_commit_id,
                                },
                        )
                    except Exception as exc:
                        logger.warning("Lineage record_link failed (promotion deploy): %s", exc)

            if schedule_interval_seconds or schedule_cron or branch or proposal_status or proposal_title or proposal_description:
                await pipeline_registry.update_pipeline(
                    pipeline_id=pipeline_id,
                    schedule_interval_seconds=schedule_interval_seconds,
                    schedule_cron=schedule_cron,
                    branch=branch,
                    proposal_status=proposal_status,
                    proposal_title=proposal_title,
                    proposal_description=proposal_description,
                )
            if dependencies is not None:
                await pipeline_registry.replace_dependencies(pipeline_id=pipeline_id, dependencies=dependencies)

            await _log_pipeline_audit(
                audit_store,
                request=request,
                action="PIPELINE_DEPLOYED",
                status="success",
                pipeline_id=pipeline_id,
                metadata={
                    "promote_build": promote_build,
                    "build_job_id": build_job_id,
                    "artifact_id": (artifact_record.artifact_id if artifact_record else None),
                    "node_id": node_id,
                    "merge_commit_id": deployed_commit_id,
                    "branch": resolved_branch,
                    "replay_on_deploy": replay_on_deploy,
                },
            )

            return ApiResponse.success(
                message="Pipeline deployed (promoted from build)",
                data={
                    "pipeline_id": pipeline_id,
                    "job_id": promote_job_id,
                    "deployed_commit_id": deployed_commit_id,
                    "outputs": build_outputs,
                    "artifact_id": (artifact_record.artifact_id if artifact_record else None),
                },
            ).to_dict()

    except HTTPException:
        raise
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_DEPLOYED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(e),
        )
        logger.error(f"Failed to deploy pipeline: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)
