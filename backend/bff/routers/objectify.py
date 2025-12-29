"""
Objectify (Dataset -> Ontology) API.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from shared.models.objectify_job import ObjectifyJob
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import sanitize_input, validate_class_id
from shared.security.auth_utils import enforce_db_scope
from shared.services.dataset_registry import DatasetRegistry
from shared.services.objectify_registry import ObjectifyRegistry
from shared.services.objectify_job_queue import ObjectifyJobQueue
from shared.services.pipeline_registry import PipelineRegistry
from shared.utils.s3_uri import parse_s3_uri

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/objectify", tags=["Objectify"])


async def get_dataset_registry() -> DatasetRegistry:
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


async def get_objectify_registry() -> ObjectifyRegistry:
    from bff.main import get_objectify_registry as _get_objectify_registry

    return await _get_objectify_registry()


async def get_objectify_job_queue(
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> ObjectifyJobQueue:
    return ObjectifyJobQueue(objectify_registry=objectify_registry)


async def get_pipeline_registry() -> PipelineRegistry:
    from bff.main import get_pipeline_registry as _get_pipeline_registry

    return await _get_pipeline_registry()


class MappingSpecField(BaseModel):
    source_field: str
    target_field: str


class CreateMappingSpecRequest(BaseModel):
    dataset_id: str
    dataset_branch: Optional[str] = Field(default=None, description="Dataset branch (default: dataset branch)")
    target_class_id: str
    mappings: List[MappingSpecField]
    target_field_types: Optional[Dict[str, str]] = Field(default=None, description="Optional target field types")
    status: str = Field(default="ACTIVE")
    auto_sync: bool = Field(default=True)
    options: Optional[Dict[str, Any]] = Field(default=None)


class TriggerObjectifyRequest(BaseModel):
    mapping_spec_id: Optional[str] = Field(default=None)
    dataset_version_id: Optional[str] = Field(default=None)
    artifact_id: Optional[str] = Field(default=None)
    artifact_output_name: Optional[str] = Field(default=None)
    batch_size: Optional[int] = Field(default=None)
    max_rows: Optional[int] = Field(default=None)
    allow_partial: bool = Field(default=False)
    options: Optional[Dict[str, Any]] = Field(default=None)


def _match_output_name(output: Dict[str, Any], name: str) -> bool:
    if not name:
        return False
    target = name.strip()
    if not target:
        return False
    for key in ("output_name", "dataset_name", "node_id"):
        candidate = str(output.get(key) or "").strip()
        if candidate and candidate == target:
            return True
    return False


@router.post("/mapping-specs", response_model=Dict[str, Any], status_code=status.HTTP_201_CREATED)
async def create_mapping_spec(
    body: CreateMappingSpecRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
):
    try:
        payload = sanitize_input(body.model_dump())
        dataset_id = str(payload.get("dataset_id") or "").strip()
        target_class_id = validate_class_id(payload.get("target_class_id") or "")
        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
        try:
            enforce_db_scope(request.headers, db_name=dataset.db_name)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))

        dataset_branch = str(payload.get("dataset_branch") or dataset.branch or "main").strip() or "main"
        mappings = payload.get("mappings") or []
        if not isinstance(mappings, list) or not mappings:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="mappings is required")

        target_field_types = payload.get("target_field_types") if isinstance(payload.get("target_field_types"), dict) else None
        options = payload.get("options") if isinstance(payload.get("options"), dict) else None
        status_value = str(payload.get("status") or "ACTIVE").strip().upper()
        auto_sync = bool(payload.get("auto_sync", True))

        record = await objectify_registry.create_mapping_spec(
            dataset_id=dataset_id,
            dataset_branch=dataset_branch,
            target_class_id=target_class_id,
            mappings=mappings,
            target_field_types=target_field_types,
            status=status_value,
            auto_sync=auto_sync,
            options=options,
        )
        return ApiResponse.success(
            message="Mapping spec created",
            data={
                "mapping_spec": {
                    **record.__dict__,
                    "mappings": record.mappings,
                    "target_field_types": record.target_field_types,
                    "options": record.options,
                }
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to create mapping spec: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/mapping-specs", response_model=Dict[str, Any])
async def list_mapping_specs(
    dataset_id: Optional[str] = Query(default=None),
    include_inactive: bool = Query(default=False),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
):
    try:
        records = await objectify_registry.list_mapping_specs(
            dataset_id=dataset_id,
            include_inactive=include_inactive,
        )
        return ApiResponse.success(
            message="Mapping specs retrieved",
            data={"mapping_specs": [record.__dict__ for record in records]},
        ).to_dict()
    except Exception as exc:
        logger.error("Failed to list mapping specs: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


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
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))

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
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail="Dataset version is missing artifact_key"
                )
            artifact_key = version.artifact_key

        if body.mapping_spec_id:
            mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=body.mapping_spec_id)
        else:
            mapping_spec = await objectify_registry.get_active_mapping_spec(
                dataset_id=dataset_id,
                dataset_branch=dataset.branch,
            )
        if not mapping_spec:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Mapping spec not found")
        if mapping_spec.dataset_id != dataset_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Mapping spec does not match dataset")

        if artifact_id:
            existing = await objectify_registry.find_objectify_job_for_artifact(
                artifact_id=artifact_id,
                artifact_output_name=resolved_output_name or "",
                mapping_spec_id=mapping_spec.mapping_spec_id,
                mapping_spec_version=mapping_spec.version,
                statuses=["QUEUED", "ENQUEUE_REQUESTED", "ENQUEUED", "RUNNING", "SUBMITTED"],
            )
            if existing:
                return ApiResponse.success(
                    message="Objectify job already queued",
                    data={
                        "job_id": existing.job_id,
                        "mapping_spec_id": mapping_spec.mapping_spec_id,
                        "dataset_id": dataset_id,
                        "artifact_id": artifact_id,
                        "artifact_output_name": resolved_output_name,
                        "artifact_key": artifact_key,
                        "status": existing.status,
                    },
                ).to_dict()
        else:
            existing = await objectify_registry.find_objectify_job(
                dataset_version_id=version.version_id,
                mapping_spec_id=mapping_spec.mapping_spec_id,
                mapping_spec_version=mapping_spec.version,
                statuses=["QUEUED", "ENQUEUE_REQUESTED", "ENQUEUED", "RUNNING", "SUBMITTED"],
            )
            if existing:
                return ApiResponse.success(
                    message="Objectify job already queued",
                    data={
                        "job_id": existing.job_id,
                        "mapping_spec_id": mapping_spec.mapping_spec_id,
                        "dataset_id": dataset_id,
                        "dataset_version_id": version.version_id,
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
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))
