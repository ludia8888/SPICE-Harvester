"""
Objectify run orchestration (BFF).

Extracted from `bff.routers.objectify_runs` to keep routers thin and to provide
a focused Facade for objectify job submission.
"""

from __future__ import annotations

import logging
from typing import Any, Dict
from uuid import uuid4

from fastapi import HTTPException, Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.objectify_deps import _require_db_role
from bff.schemas.objectify_requests import TriggerObjectifyRequest
from shared.models.objectify_job import ObjectifyJob
from shared.models.requests import ApiResponse
from shared.security.auth_utils import enforce_db_scope
from shared.security.database_access import DATA_ENGINEER_ROLES
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.registries.backing_source_adapter import (
    get_mapping_from_oms,
    is_oms_mapping_spec,
)
from shared.utils.objectify_outputs import match_output_name
from shared.utils.schema_hash import compute_schema_hash_from_sample
from shared.utils.s3_uri import parse_s3_uri
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


@trace_external_call("bff.objectify_run.run_objectify")
async def run_objectify(
    *,
    dataset_id: str,
    body: TriggerObjectifyRequest,
    request: Request,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    job_queue: ObjectifyJobQueue,
    pipeline_registry: PipelineRegistry,
    oms_client: Any = None,
) -> Dict[str, Any]:
    try:
        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset not found", code=ErrorCode.RESOURCE_NOT_FOUND)

        try:
            enforce_db_scope(request.headers, db_name=dataset.db_name)
        except ValueError as exc:
            raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED) from exc
        await _require_db_role(request, db_name=dataset.db_name, roles=DATA_ENGINEER_ROLES)

        artifact_id = str(body.artifact_id or "").strip() or None
        artifact_output_name = str(body.artifact_output_name or "").strip() or None
        if artifact_id and body.dataset_version_id:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "dataset_version_id and artifact_id are mutually exclusive",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        version = None
        artifact_key = None
        resolved_output_name = None
        resolved_schema_hash = None

        if artifact_id:
            artifact = await pipeline_registry.get_artifact(artifact_id=artifact_id)
            if not artifact:
                raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Pipeline artifact not found", code=ErrorCode.RESOURCE_NOT_FOUND)
            if str(artifact.status or "").upper() != "SUCCESS":
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Artifact is not successful", code=ErrorCode.CONFLICT)
            if str(artifact.mode or "").lower() != "build":
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Artifact is not a build artifact", code=ErrorCode.CONFLICT)
            outputs = artifact.outputs or []
            if not outputs:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Artifact has no outputs", code=ErrorCode.CONFLICT)
            if not artifact_output_name:
                if len(outputs) == 1:
                    output = outputs[0]
                    artifact_output_name = (
                        str(output.get("output_name") or output.get("dataset_name") or output.get("node_id") or "").strip()
                        or None
                    )
                if not artifact_output_name:
                    raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "artifact_output_name is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
            matches = [out for out in outputs if match_output_name(out, artifact_output_name)]
            if not matches:
                raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Artifact output not found", code=ErrorCode.RESOURCE_NOT_FOUND)
            if len(matches) > 1:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Artifact output is ambiguous", code=ErrorCode.CONFLICT)
            selected = matches[0]
            artifact_key = str(selected.get("artifact_commit_key") or selected.get("artifact_key") or "").strip() or None
            if not artifact_key:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Artifact output is missing artifact_key", code=ErrorCode.CONFLICT)
            if not parse_s3_uri(artifact_key):
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "Artifact output is not an s3:// URI", code=ErrorCode.REQUEST_VALIDATION_FAILED)
            resolved_output_name = artifact_output_name
            resolved_schema_hash = str(selected.get("schema_hash") or "").strip() or None
            if not resolved_schema_hash:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Artifact output is missing schema_hash", code=ErrorCode.CONFLICT)
        else:
            if body.dataset_version_id:
                version = await dataset_registry.get_version(version_id=body.dataset_version_id)
            else:
                version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
            if not version:
                raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset version not found", code=ErrorCode.RESOURCE_NOT_FOUND)
            if version.dataset_id != dataset_id:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Dataset version mismatch", code=ErrorCode.OBJECTIFY_CONTRACT_ERROR)
            if not version.artifact_key:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "Dataset version is missing artifact_key", code=ErrorCode.REQUEST_VALIDATION_FAILED)
            artifact_key = version.artifact_key
            resolved_output_name = dataset.name
            resolved_schema_hash = compute_schema_hash_from_sample(version.sample_json)
            if not resolved_schema_hash:
                resolved_schema_hash = compute_schema_hash_from_sample(dataset.schema_json)
            if not resolved_schema_hash:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Dataset schema_hash could not be determined for objectify",
                    code=ErrorCode.CONFLICT,
                )

        # ── Mapping resolution: OMS backing_source (by target_class_id) or PostgreSQL ──
        mapping_spec = None
        oms_mode = False
        target_class_id = str(body.target_class_id or "").strip() or None

        if target_class_id and not body.mapping_spec_id and oms_client is not None:
            try:
                oms_spec = await get_mapping_from_oms(
                    oms_client.client,
                    oms_base_url=oms_client.base_url,
                    db_name=dataset.db_name,
                    target_class_id=target_class_id,
                    branch=dataset.branch or "main",
                    admin_token=oms_client._get_auth_token(),
                )
                if oms_spec:
                    mapping_spec = oms_spec
                    oms_mode = True
            except Exception as oms_exc:
                logger.warning(
                    "OMS backing_source lookup failed for %s (falling back to PG): %s",
                    target_class_id,
                    oms_exc,
                )

        if not mapping_spec:
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
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Active mapping spec not found for output/schema",
                code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
            )
        if not oms_mode and mapping_spec.dataset_id != dataset_id:
            raise classified_http_exception(status.HTTP_409_CONFLICT, "Mapping spec does not match dataset", code=ErrorCode.OBJECTIFY_CONTRACT_ERROR)
        if not oms_mode and resolved_output_name and mapping_spec.artifact_output_name and mapping_spec.artifact_output_name != resolved_output_name:
            raise classified_http_exception(status.HTTP_409_CONFLICT, "Mapping spec output does not match input", code=ErrorCode.OBJECTIFY_CONTRACT_ERROR)
        if (
            not oms_mode
            and resolved_schema_hash
            and mapping_spec.schema_hash
            and mapping_spec.schema_hash != resolved_schema_hash
            and not mapping_spec.backing_datasource_version_id
        ):
            raise classified_http_exception(status.HTTP_409_CONFLICT, "Mapping spec schema_hash mismatch", code=ErrorCode.OBJECTIFY_CONTRACT_ERROR)

        if mapping_spec.backing_datasource_version_id:
            if artifact_id:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Mapping spec backing datasource version cannot be used with artifact inputs",
                    code=ErrorCode.CONFLICT,
                )
            backing_version = await dataset_registry.get_backing_datasource_version(
                version_id=mapping_spec.backing_datasource_version_id
            )
            if not backing_version:
                raise classified_http_exception(
                    status.HTTP_404_NOT_FOUND,
                    "Mapping spec backing datasource version not found",
                    code=ErrorCode.RESOURCE_NOT_FOUND,
                )
            version = await dataset_registry.get_version(version_id=backing_version.dataset_version_id)
            if not version or version.dataset_id != dataset_id:
                raise classified_http_exception(
                    status.HTTP_404_NOT_FOUND,
                    "Backing datasource version dataset not found",
                    code=ErrorCode.RESOURCE_NOT_FOUND,
                )
            if body.dataset_version_id and str(body.dataset_version_id) != version.version_id:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Dataset version does not match mapping spec backing datasource version",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                )
            if not version.artifact_key:
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    "Backing datasource version is missing artifact_key",
                    code=ErrorCode.REQUEST_VALIDATION_FAILED,
                )
            artifact_key = version.artifact_key
            resolved_output_name = dataset.name
            resolved_schema_hash = backing_version.schema_hash

        is_oms = is_oms_mapping_spec(mapping_spec.mapping_spec_id) if mapping_spec.mapping_spec_id else oms_mode
        dedupe_spec_id = mapping_spec.target_class_id if is_oms else mapping_spec.mapping_spec_id
        dedupe_key = objectify_registry.build_dedupe_key(
            dataset_id=dataset_id,
            dataset_branch=dataset.branch,
            mapping_spec_id=dedupe_spec_id,
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
                    "oms_mode": is_oms,
                },
            ).to_dict()

        job_id = str(uuid4())
        options = dict(mapping_spec.options or {})
        override_options = body.options if isinstance(body.options, dict) else {}
        options.update(override_options)

        # OMS-sourced: mapping_spec_id=None (can't store "oms:..." in PG uuid column)
        pg_spec_id = None if is_oms else mapping_spec.mapping_spec_id
        pg_spec_version = None if is_oms else mapping_spec.version

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
            mapping_spec_id=pg_spec_id,
            mapping_spec_version=pg_spec_version,
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
                "oms_mode": is_oms,
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to enqueue objectify job: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc
