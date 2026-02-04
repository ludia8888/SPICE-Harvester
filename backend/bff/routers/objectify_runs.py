"""
Objectify run endpoints (BFF).

Composed by `bff.routers.objectify` via router composition (Composite pattern).
"""

import logging
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status

from bff.routers.objectify_deps import (
    get_dataset_registry,
    get_objectify_job_queue,
    get_objectify_registry,
    get_pipeline_registry,
    _require_db_role,
)
from bff.routers.objectify_ops import _compute_schema_hash_from_sample, _match_output_name
from bff.schemas.objectify_requests import TriggerObjectifyRequest
from shared.models.objectify_job import ObjectifyJob
from shared.models.requests import ApiResponse
from shared.security.auth_utils import enforce_db_scope
from shared.security.database_access import DATA_ENGINEER_ROLES
from shared.utils.s3_uri import parse_s3_uri
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Objectify"])


@router.post("/datasets/{dataset_id}/run", response_model=Dict[str, Any])
async def run_objectify(
    dataset_id: str,
    body: TriggerObjectifyRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
):
    try:
        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

        try:
            enforce_db_scope(request.headers, db_name=dataset.db_name)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
        await _require_db_role(request, db_name=dataset.db_name, roles=DATA_ENGINEER_ROLES)

        artifact_id = str(body.artifact_id or "").strip() or None
        artifact_output_name = str(body.artifact_output_name or "").strip() or None
        if artifact_id and body.dataset_version_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="dataset_version_id and artifact_id are mutually exclusive",
            )

        version = None
        artifact_key = None
        resolved_output_name = None
        resolved_schema_hash = None

        if artifact_id:
            artifact = await pipeline_registry.get_artifact(artifact_id=artifact_id)
            if not artifact:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline artifact not found")
            if str(artifact.status or "").upper() != "SUCCESS":
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Artifact is not successful")
            if str(artifact.mode or "").lower() != "build":
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Artifact is not a build artifact")
            outputs = artifact.outputs or []
            if not outputs:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Artifact has no outputs")
            if not artifact_output_name:
                if len(outputs) == 1:
                    output = outputs[0]
                    artifact_output_name = (
                        str(output.get("output_name") or output.get("dataset_name") or output.get("node_id") or "").strip()
                        or None
                    )
                if not artifact_output_name:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="artifact_output_name is required")
            matches = [out for out in outputs if _match_output_name(out, artifact_output_name)]
            if not matches:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Artifact output not found")
            if len(matches) > 1:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Artifact output is ambiguous")
            selected = matches[0]
            artifact_key = str(selected.get("artifact_commit_key") or selected.get("artifact_key") or "").strip() or None
            if not artifact_key:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Artifact output is missing artifact_key")
            if not parse_s3_uri(artifact_key):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Artifact output is not an s3:// URI")
            resolved_output_name = artifact_output_name
            resolved_schema_hash = str(selected.get("schema_hash") or "").strip() or None
            if not resolved_schema_hash:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Artifact output is missing schema_hash")
        else:
            if body.dataset_version_id:
                version = await dataset_registry.get_version(version_id=body.dataset_version_id)
            else:
                version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
            if not version:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset version not found")
            if version.dataset_id != dataset_id:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Dataset version mismatch")
            if not version.artifact_key:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Dataset version is missing artifact_key")
            artifact_key = version.artifact_key
            resolved_output_name = dataset.name
            resolved_schema_hash = _compute_schema_hash_from_sample(version.sample_json)
            if not resolved_schema_hash:
                resolved_schema_hash = _compute_schema_hash_from_sample(dataset.schema_json)
            if not resolved_schema_hash:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Dataset schema_hash could not be determined for objectify",
                )

        if body.mapping_spec_id:
            mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=body.mapping_spec_id)
        else:
            mapping_spec = await objectify_registry.get_active_mapping_spec(
                dataset_id=dataset_id,
                dataset_branch=dataset.branch,
                artifact_output_name=resolved_output_name,
                schema_hash=resolved_schema_hash,
            )
        if not mapping_spec:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Active mapping spec not found for output/schema",
            )
        if mapping_spec.dataset_id != dataset_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Mapping spec does not match dataset")
        if resolved_output_name and mapping_spec.artifact_output_name and mapping_spec.artifact_output_name != resolved_output_name:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Mapping spec output does not match input")
        if (
            resolved_schema_hash
            and mapping_spec.schema_hash
            and mapping_spec.schema_hash != resolved_schema_hash
            and not mapping_spec.backing_datasource_version_id
        ):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Mapping spec schema_hash mismatch")

        if mapping_spec.backing_datasource_version_id:
            if artifact_id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Mapping spec backing datasource version cannot be used with artifact inputs",
                )
            backing_version = await dataset_registry.get_backing_datasource_version(version_id=mapping_spec.backing_datasource_version_id)
            if not backing_version:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Mapping spec backing datasource version not found",
                )
            version = await dataset_registry.get_version(version_id=backing_version.dataset_version_id)
            if not version or version.dataset_id != dataset_id:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Backing datasource version dataset not found",
                )
            if body.dataset_version_id and str(body.dataset_version_id) != version.version_id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Dataset version does not match mapping spec backing datasource version",
                )
            if not version.artifact_key:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Backing datasource version is missing artifact_key",
                )
            artifact_key = version.artifact_key
            resolved_output_name = dataset.name
            resolved_schema_hash = backing_version.schema_hash

        dedupe_key = objectify_registry.build_dedupe_key(
            dataset_id=dataset_id,
            dataset_branch=dataset.branch,
            mapping_spec_id=mapping_spec.mapping_spec_id,
            mapping_spec_version=mapping_spec.version,
            dataset_version_id=(version.version_id if version else None),
            artifact_id=artifact_id,
            artifact_output_name=resolved_output_name,
        )
        existing = await objectify_registry.get_objectify_job_by_dedupe_key(dedupe_key=dedupe_key)
        if existing:
            return ApiResponse.success(
                message="Objectify job already queued",
                data={
                    "job_id": existing.job_id,
                    "mapping_spec_id": mapping_spec.mapping_spec_id,
                    "dataset_id": dataset_id,
                    "dataset_version_id": version.version_id if version else None,
                    "artifact_id": artifact_id,
                    "artifact_output_name": resolved_output_name,
                    "artifact_key": artifact_key,
                    "status": existing.status,
                },
            ).to_dict()

        job_id = str(uuid4())
        options = dict(mapping_spec.options or {})
        override_options = body.options if isinstance(body.options, dict) else {}
        options.update(override_options)

        job = ObjectifyJob(
            job_id=job_id,
            db_name=dataset.db_name,
            dataset_id=dataset_id,
            dataset_version_id=(version.version_id if version else None),
            artifact_id=artifact_id,
            artifact_output_name=resolved_output_name,
            dedupe_key=dedupe_key,
            dataset_branch=dataset.branch,
            artifact_key=artifact_key,
            mapping_spec_id=mapping_spec.mapping_spec_id,
            mapping_spec_version=mapping_spec.version,
            target_class_id=mapping_spec.target_class_id,
            ontology_branch=options.get("ontology_branch"),
            max_rows=body.max_rows or options.get("max_rows"),
            batch_size=body.batch_size or options.get("batch_size"),
            allow_partial=bool(body.allow_partial or options.get("allow_partial")),
            options=options,
        )

        await job_queue.publish(job, require_delivery=False)

        return ApiResponse.success(
            message="Objectify job queued",
            data={
                "job_id": job_id,
                "mapping_spec_id": mapping_spec.mapping_spec_id,
                "dataset_id": dataset_id,
                "dataset_version_id": (version.version_id if version else None),
                "artifact_id": artifact_id,
                "artifact_output_name": resolved_output_name,
                "artifact_key": artifact_key,
                "status": "QUEUED",
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to enqueue objectify job: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

