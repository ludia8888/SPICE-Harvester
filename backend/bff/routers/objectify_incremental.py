"""
Objectify incremental execution endpoints (BFF).

Composed by `bff.routers.objectify` via router composition (Composite pattern).
"""

import logging
from typing import Any, Dict
from uuid import uuid4
from shared.observability.tracing import trace_endpoint

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.objectify_deps import get_dataset_registry, get_objectify_registry, _require_db_role
from bff.schemas.objectify_requests import TriggerIncrementalRequest
from shared.models.objectify_job import ObjectifyJob
from shared.models.requests import ApiResponse
from shared.security.database_access import DATA_ENGINEER_ROLES
from shared.security.input_sanitizer import sanitize_input
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Objectify"])


@router.post(
    "/mapping-specs/{mapping_spec_id}/trigger-incremental",
    summary="Trigger incremental objectify",
    description="Trigger objectify in incremental mode, processing only rows changed since last run.",
)
@trace_endpoint("bff.objectify.trigger_incremental_objectify")
async def trigger_incremental_objectify(
    mapping_spec_id: str,
    request: Request,
    body: TriggerIncrementalRequest = TriggerIncrementalRequest(),
    branch: str = Query(default="master"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> Dict[str, Any]:
    """Trigger objectify with incremental execution mode."""
    mapping_spec_id = sanitize_input(mapping_spec_id)

    try:
        mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
        if not mapping_spec:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, f"Mapping spec not found: {mapping_spec_id}", code=ErrorCode.RESOURCE_NOT_FOUND)

        dataset = await dataset_registry.get_dataset(dataset_id=str(mapping_spec.dataset_id))
        if not dataset:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, f"Dataset not found: {mapping_spec.dataset_id}", code=ErrorCode.RESOURCE_NOT_FOUND)

        await _require_db_role(request, db_name=dataset.db_name, roles=DATA_ENGINEER_ROLES)

        if body.force_full_refresh:
            await objectify_registry.delete_watermark(mapping_spec_id=mapping_spec_id, dataset_branch=branch)
            previous_watermark = None
            watermark_column = body.watermark_column.strip() if body.watermark_column else None
            base_commit_id = None
        else:
            watermark = await objectify_registry.get_watermark(mapping_spec_id=mapping_spec_id, dataset_branch=branch)
            previous_watermark = watermark.get("watermark_value") if watermark else None
            watermark_column = (
                body.watermark_column.strip()
                if body.watermark_column
                else (str(watermark.get("watermark_column") or "").strip() if isinstance(watermark, dict) else None)
            ) or None
            base_commit_id = (
                str(watermark.get("lakefs_commit_id") or "").strip()
                if isinstance(watermark, dict)
                else None
            ) or None

        version = await dataset_registry.get_latest_version(dataset_id=str(mapping_spec.dataset_id))
        if not version:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "No dataset version available", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        # Ensure mapping spec backing version advances to the latest dataset version (same schema_hash),
        # otherwise objectify-worker will reject the job as a backing_datasource_version_mismatch.
        base_options = dict(mapping_spec.options or {})
        try:
            if mapping_spec.backing_datasource_id:
                backing_version = await dataset_registry.get_or_create_backing_datasource_version(
                    backing_id=mapping_spec.backing_datasource_id,
                    dataset_version_id=version.version_id,
                    schema_hash=mapping_spec.schema_hash,
                    metadata={"artifact_key": getattr(version, "artifact_key", None)},
                )
                if backing_version and mapping_spec.backing_datasource_version_id != backing_version.version_id:
                    impact_scope = base_options.get("impact_scope") if isinstance(base_options.get("impact_scope"), dict) else {}
                    impact_scope = dict(impact_scope or {})
                    impact_scope["dataset_version_id"] = version.version_id
                    impact_scope.setdefault("schema_hash", mapping_spec.schema_hash)
                    impact_scope.setdefault("artifact_output_name", mapping_spec.artifact_output_name)
                    base_options["impact_scope"] = impact_scope
                    updated = await objectify_registry.update_mapping_spec(
                        mapping_spec_id=mapping_spec.mapping_spec_id,
                        backing_datasource_version_id=backing_version.version_id,
                        options=base_options,
                    )
                    if updated:
                        mapping_spec = updated
                        base_options = dict(mapping_spec.options or base_options)
        except Exception as exc:
            logger.warning(
                "Failed to advance mapping spec backing version (mapping_spec_id=%s dataset_version_id=%s): %s",
                mapping_spec.mapping_spec_id,
                getattr(version, "version_id", None),
                exc,
            )

        job_id = str(uuid4())
        options = dict(base_options)
        options["execution_mode"] = body.execution_mode
        if watermark_column:
            options["watermark_column"] = watermark_column
        if previous_watermark is not None:
            options["previous_watermark"] = previous_watermark

        dedupe_key = objectify_registry.build_dedupe_key(
            dataset_id=str(mapping_spec.dataset_id),
            dataset_branch=dataset.branch,
            mapping_spec_id=mapping_spec.mapping_spec_id,
            mapping_spec_version=mapping_spec.version,
            dataset_version_id=version.version_id,
            artifact_id=None,
            artifact_output_name=mapping_spec.artifact_output_name,
        )
        dedupe_key = f"{dedupe_key}|mode:{body.execution_mode}"

        job = ObjectifyJob(
            job_id=job_id,
            db_name=dataset.db_name,
            dataset_id=str(mapping_spec.dataset_id),
            dataset_version_id=version.version_id,
            dedupe_key=dedupe_key,
            dataset_branch=dataset.branch,
            artifact_key=version.artifact_key,
            mapping_spec_id=mapping_spec.mapping_spec_id,
            mapping_spec_version=mapping_spec.version,
            target_class_id=mapping_spec.target_class_id,
            execution_mode=body.execution_mode,
            watermark_column=watermark_column,
            previous_watermark=str(previous_watermark) if previous_watermark is not None else None,
            base_commit_id=base_commit_id if body.execution_mode == "delta" else None,
            max_rows=body.max_rows or options.get("max_rows"),
            batch_size=body.batch_size or options.get("batch_size"),
            options=options,
        )

        record = await objectify_registry.enqueue_objectify_job(job=job)

        return ApiResponse.success(
            message=f"Incremental objectify job queued ({body.execution_mode} mode)",
            data={
                "job_id": record.job_id,
                "mapping_spec_id": mapping_spec_id,
                "execution_mode": body.execution_mode,
                "watermark_column": body.watermark_column,
                "previous_watermark": previous_watermark,
                "status": "QUEUED",
            },
        ).to_dict()

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("trigger_incremental_objectify failed: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc


@router.get(
    "/mapping-specs/{mapping_spec_id}/watermark",
    summary="Get objectify watermark",
    description="Get the current watermark state for a mapping spec.",
)
@trace_endpoint("bff.objectify.get_mapping_spec_watermark")
async def get_mapping_spec_watermark(
    mapping_spec_id: str,
    request: Request,
    branch: str = Query(default="master"),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> Dict[str, Any]:
    """Get watermark state for incremental objectify."""
    mapping_spec_id = sanitize_input(mapping_spec_id)

    try:
        watermark = await objectify_registry.get_watermark(mapping_spec_id=mapping_spec_id, dataset_branch=branch)

        if not watermark:
            return ApiResponse.success(
                message="No watermark found",
                data={
                    "mapping_spec_id": mapping_spec_id,
                    "branch": branch,
                    "watermark": None,
                    "message": "Objectify has not run in incremental mode yet",
                },
            ).to_dict()

        return ApiResponse.success(
            message="Watermark retrieved",
            data={"mapping_spec_id": mapping_spec_id, "branch": branch, "watermark": watermark},
        ).to_dict()

    except Exception as exc:
        logger.error("get_mapping_spec_watermark failed: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc
