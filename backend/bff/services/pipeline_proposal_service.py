"""Pipeline proposal domain logic (BFF).

Extracted from `bff.routers.pipeline_proposals` to keep routers thin while
preserving a stable import surface for tests and composition roots.
"""

import logging
from typing import Any, Optional

from fastapi import HTTPException, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.pipeline_shared import (
    _ensure_pipeline_permission,
    _log_pipeline_audit,
    _require_pipeline_idempotency_key,
    _filter_pipeline_records_for_read_access,
)
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.services.registries.pipeline_registry import PipelineMergeNotSupportedError
from shared.services.storage.lakefs_client import LakeFSConflictError
from shared.utils.schema_hash import compute_schema_hash
from shared.utils.time_utils import utcnow
from shared.observability.tracing import trace_db_operation

logger = logging.getLogger(__name__)


def _normalize_ontology_bundle(*, ontology_meta: dict[str, Any], build_branch: str) -> dict[str, Optional[str]]:
    branch_value = str(ontology_meta.get("branch") or "").strip() or build_branch
    ref_value = str(ontology_meta.get("ref") or "").strip() or f"branch:{build_branch}"
    commit_value = str(ontology_meta.get("commit") or "").strip() or ref_value
    return {
        "branch": branch_value or None,
        "ref": ref_value or None,
        "commit": commit_value or None,
    }


def normalize_mapping_spec_ids(raw: Any) -> list[str]:
    if raw is None:
        return []
    values: list[str] = []
    if isinstance(raw, str):
        raw = raw.split(",") if "," in raw else [raw]
    if isinstance(raw, list):
        for item in raw:
            if item is None:
                continue
            value = str(item).strip()
            if value:
                values.append(value)
    else:
        value = str(raw).strip()
        if value:
            values.append(value)
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


async def _build_proposal_bundle(
    *,
    pipeline: Any,
    build_job_id: Optional[str],
    mapping_spec_ids: list[str],
    pipeline_registry: Any,
    dataset_registry: Any,
    objectify_registry: Any,
) -> dict[str, Any]:
    bundle: dict[str, Any] = {
        "captured_at": utcnow().isoformat(),
        "pipeline_id": str(pipeline.pipeline_id),
        "db_name": str(pipeline.db_name),
        "branch": str(pipeline.branch or "main"),
    }

    outputs_for_lookup: list[dict[str, Any]] = []

    if build_job_id:
        build_run = await pipeline_registry.get_run(pipeline_id=pipeline.pipeline_id, job_id=build_job_id)
        if not build_run:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Build run not found for proposal bundle", code=ErrorCode.RESOURCE_NOT_FOUND)
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
                code=ErrorCode.CONFLICT,
                extra={
                    "build_status": build_status or None,
                    "errors": errors,
                    "build_job_id": build_job_id,
                },
            )

        output_json = build_run.get("output_json") if isinstance(build_run.get("output_json"), dict) else {}
        definition_hash = (
            str(output_json.get("definition_hash") or build_run.get("pipeline_spec_hash") or "").strip() or None
        )
        definition_commit_id = (
            str(output_json.get("pipeline_spec_commit_id") or build_run.get("pipeline_spec_commit_id") or "").strip()
            or None
        )
        build_branch = str(output_json.get("branch") or pipeline.branch or "main").strip() or "main"
        lakefs_meta = output_json.get("lakefs") if isinstance(output_json.get("lakefs"), dict) else {}
        ontology_meta = output_json.get("ontology") if isinstance(output_json.get("ontology"), dict) else {}
        normalized_ontology = _normalize_ontology_bundle(
            ontology_meta=ontology_meta,
            build_branch=build_branch,
        )

        bundle.update(
            {
                "build_job_id": build_job_id,
                "definition_hash": definition_hash,
                "definition_commit_id": definition_commit_id,
                "build_branch": build_branch,
                "lakefs": {
                    "repository": str(lakefs_meta.get("repository") or "").strip() or None,
                    "build_branch": str(lakefs_meta.get("build_branch") or "").strip() or None,
                    "commit_id": str(lakefs_meta.get("commit_id") or "").strip() or None,
                },
                "ontology": normalized_ontology,
            }
        )

        outputs_payload = output_json.get("outputs") if isinstance(output_json.get("outputs"), list) else []
        for item in outputs_payload:
            if not isinstance(item, dict):
                continue
            dataset_name = str(item.get("dataset_name") or item.get("datasetName") or "").strip()
            if not dataset_name:
                continue
            outputs_for_lookup.append(
                {
                    "dataset_name": dataset_name,
                    "node_id": str(item.get("node_id") or "").strip() or None,
                    "artifact_key": str(item.get("artifact_key") or "").strip() or None,
                }
            )
        if outputs_for_lookup:
            bundle["outputs"] = outputs_for_lookup

        artifact_record = await pipeline_registry.get_artifact_by_job(
            pipeline_id=pipeline.pipeline_id,
            job_id=build_job_id,
            mode="build",
        )
        if artifact_record:
            bundle["artifact_id"] = artifact_record.artifact_id

    mapping_specs: list[dict[str, Any]] = []
    seen_spec_ids: set[str] = set()

    if mapping_spec_ids:
        for mapping_spec_id in mapping_spec_ids:
            record = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
            if not record:
                raise classified_http_exception(
                    status.HTTP_404_NOT_FOUND,
                    "Mapping spec not found",
                    code=ErrorCode.RESOURCE_NOT_FOUND,
                    extra={"mapping_spec_id": mapping_spec_id},
                )
            if record.mapping_spec_id in seen_spec_ids:
                continue
            seen_spec_ids.add(record.mapping_spec_id)
            mapping_specs.append(
                {
                    "mapping_spec_id": record.mapping_spec_id,
                    "mapping_spec_version": record.version,
                    "dataset_id": record.dataset_id,
                    "dataset_branch": record.dataset_branch,
                    "target_class_id": record.target_class_id,
                }
            )

    for output in outputs_for_lookup:
        dataset_name = str(output.get("dataset_name") or "").strip()
        if not dataset_name:
            continue
        schema_hash = str(output.get("schema_hash") or "").strip() or None
        if not schema_hash and isinstance(output.get("columns"), list):
            schema_hash = compute_schema_hash(output.get("columns"))
        dataset = await dataset_registry.get_dataset_by_name(
            db_name=str(pipeline.db_name),
            name=dataset_name,
            branch=str(pipeline.branch or "main"),
        )
        if not dataset:
            continue
        mapping_spec = await objectify_registry.get_active_mapping_spec(
            dataset_id=dataset.dataset_id,
            dataset_branch=dataset.branch,
            artifact_output_name=dataset_name,
            schema_hash=schema_hash,
        )
        if not mapping_spec:
            continue
        if mapping_spec.mapping_spec_id in seen_spec_ids:
            continue
        seen_spec_ids.add(mapping_spec.mapping_spec_id)
        mapping_specs.append(
            {
                "mapping_spec_id": mapping_spec.mapping_spec_id,
                "mapping_spec_version": mapping_spec.version,
                "dataset_id": mapping_spec.dataset_id,
                "dataset_branch": mapping_spec.dataset_branch,
                "target_class_id": mapping_spec.target_class_id,
                "dataset_name": dataset_name,
            }
        )

    if mapping_specs:
        bundle["mapping_specs"] = mapping_specs
    return bundle


@trace_db_operation("bff.pipeline_proposal.list_pipeline_proposals")
async def list_pipeline_proposals(
    *,
    db_name: str,
    branch: Optional[str],
    status_filter: Optional[str],
    pipeline_registry: Any,
    request: Any,
) -> dict[str, Any]:
    try:
        db_name = validate_db_name(db_name)
        proposals = await pipeline_registry.list_proposals(db_name=db_name, branch=branch, status=status_filter)
        filtered = await _filter_pipeline_records_for_read_access(
            pipeline_registry,
            records=proposals,
            request=request,
        )
        return ApiResponse.success(
            message="Pipeline proposals fetched",
            data={"proposals": filtered, "count": len(filtered)},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list pipeline proposals: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc


@trace_db_operation("bff.pipeline_proposal.submit_pipeline_proposal")
async def submit_pipeline_proposal(
    *,
    pipeline_id: str,
    payload: dict[str, Any],
    audit_store: Any,
    pipeline_registry: Any,
    dataset_registry: Any,
    objectify_registry: Any,
    request: Any,
) -> dict[str, Any]:
    try:
        _require_pipeline_idempotency_key(request, operation="proposal submit")
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="edit",
        )
        sanitized = sanitize_input(payload)
        title = str(sanitized.get("title") or "").strip()
        if not title:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "title is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        description = str(sanitized.get("description") or "").strip() or None
        build_job_id = str(sanitized.get("build_job_id") or sanitized.get("buildJobId") or "").strip() or None
        mapping_spec_ids = normalize_mapping_spec_ids(
            sanitized.get("mapping_spec_ids")
            or sanitized.get("mappingSpecIds")
            or sanitized.get("mapping_spec_id")
            or sanitized.get("mappingSpecId")
        )
        proposal_bundle = None
        if build_job_id or mapping_spec_ids:
            pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
            if not pipeline:
                raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Pipeline not found", code=ErrorCode.PIPELINE_NOT_FOUND)
            proposal_bundle = await _build_proposal_bundle(
                pipeline=pipeline,
                build_job_id=build_job_id,
                mapping_spec_ids=mapping_spec_ids,
                pipeline_registry=pipeline_registry,
                dataset_registry=dataset_registry,
                objectify_registry=objectify_registry,
            )
        record = await pipeline_registry.submit_proposal(
            pipeline_id=pipeline_id,
            title=title,
            description=description,
            proposal_bundle=proposal_bundle,
        )
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PROPOSAL_SUBMITTED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={"title": title, "description": description, "build_job_id": build_job_id},
        )
        return ApiResponse.success(
            message="Proposal submitted",
            data={"proposal": {**record.__dict__}},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PROPOSAL_SUBMITTED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(exc),
        )
        logger.error("Failed to submit proposal: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc


@trace_db_operation("bff.pipeline_proposal.approve_pipeline_proposal")
async def approve_pipeline_proposal(
    *,
    pipeline_id: str,
    payload: dict[str, Any],
    audit_store: Any,
    pipeline_registry: Any,
    request: Any,
) -> dict[str, Any]:
    try:
        _require_pipeline_idempotency_key(request, operation="proposal approve")
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="approve",
        )
        sanitized = sanitize_input(payload)
        to_branch = str(sanitized.get("merge_into") or "main").strip() or "main"
        review_comment = str(sanitized.get("review_comment") or "").strip() or None
        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Pipeline not found", code=ErrorCode.PIPELINE_NOT_FOUND)
        if pipeline.proposal_status != "pending":
            raise classified_http_exception(status.HTTP_409_CONFLICT, "No pending proposal to approve", code=ErrorCode.CONFLICT)

        from_branch = str(pipeline.branch or "main")
        if from_branch != to_branch:
            try:
                await pipeline_registry.merge_branch(
                    pipeline_id=pipeline_id,
                    from_branch=from_branch,
                    to_branch=to_branch,
                )
            except PipelineMergeNotSupportedError as exc:
                raise classified_http_exception(status.HTTP_409_CONFLICT, str(exc), code=ErrorCode.CONFLICT) from exc
            except LakeFSConflictError as exc:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "lakeFS merge conflict",
                    code=ErrorCode.MERGE_CONFLICT,
                    extra={
                        "detail": str(exc),
                        "source_branch": from_branch,
                        "target_branch": to_branch,
                    },
                ) from exc

        record = await pipeline_registry.review_proposal(
            pipeline_id=pipeline_id,
            status="approved",
            review_comment=review_comment,
        )
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PROPOSAL_APPROVED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={"from_branch": from_branch, "to_branch": to_branch, "review_comment": review_comment},
        )
        return ApiResponse.success(
            message="Proposal approved",
            data={"proposal": {**record.__dict__}},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PROPOSAL_APPROVED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(exc),
        )
        logger.error("Failed to approve proposal: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc


@trace_db_operation("bff.pipeline_proposal.reject_pipeline_proposal")
async def reject_pipeline_proposal(
    *,
    pipeline_id: str,
    payload: dict[str, Any],
    audit_store: Any,
    pipeline_registry: Any,
    request: Any,
) -> dict[str, Any]:
    try:
        _require_pipeline_idempotency_key(request, operation="proposal reject")
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="approve",
        )
        sanitized = sanitize_input(payload)
        review_comment = str(sanitized.get("review_comment") or "").strip() or None
        record = await pipeline_registry.review_proposal(
            pipeline_id=pipeline_id,
            status="rejected",
            review_comment=review_comment,
        )
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PROPOSAL_REJECTED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={"review_comment": review_comment},
        )
        return ApiResponse.success(
            message="Proposal rejected",
            data={"proposal": {**record.__dict__}},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PROPOSAL_REJECTED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(exc),
        )
        logger.error("Failed to reject proposal: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc
