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
from shared.utils.import_type_normalization import normalize_import_target_type
from shared.utils.s3_uri import parse_s3_uri
from shared.utils.schema_hash import compute_schema_hash
from bff.dependencies import OMSClientDep
from bff.services.oms_client import OMSClient

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
    artifact_output_name: Optional[str] = Field(
        default=None, description="Artifact output name this mapping spec applies to (default: dataset name)"
    )
    schema_hash: Optional[str] = Field(default=None, description="Schema hash for the bound output")
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


def _compute_schema_hash_from_sample(sample_json: Any) -> Optional[str]:
    if not isinstance(sample_json, dict):
        return None
    columns = sample_json.get("columns")
    if not isinstance(columns, list) or not columns:
        return None
    return compute_schema_hash(columns)


def _extract_schema_columns(schema: Any) -> List[str]:
    if not isinstance(schema, dict):
        return []
    columns = schema.get("columns")
    if isinstance(columns, list):
        names: List[str] = []
        for col in columns:
            if isinstance(col, dict):
                name = str(col.get("name") or col.get("column") or "").strip()
            else:
                name = str(col).strip()
            if name:
                names.append(name)
        return names
    fields = schema.get("fields")
    if isinstance(fields, list):
        names = []
        for col in fields:
            if not isinstance(col, dict):
                continue
            name = str(col.get("name") or "").strip()
            if name:
                names.append(name)
        return names
    props = schema.get("properties")
    if isinstance(props, dict):
        return [str(key).strip() for key in props.keys() if str(key).strip()]
    return []


def _normalize_ontology_payload(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    if isinstance(payload, dict):
        return payload
    return {}


def _extract_ontology_fields(payload: Any) -> tuple[Dict[str, Dict[str, Any]], set[str]]:
    data = _normalize_ontology_payload(payload)
    properties = data.get("properties") if isinstance(data, dict) else None
    relationships = data.get("relationships") if isinstance(data, dict) else None

    prop_map: Dict[str, Dict[str, Any]] = {}
    if isinstance(properties, list):
        for prop in properties:
            if not isinstance(prop, dict):
                continue
            name = str(prop.get("name") or "").strip()
            if not name:
                continue
            prop_map[name] = prop

    rel_names: set[str] = set()
    if isinstance(relationships, list):
        for rel in relationships:
            if not isinstance(rel, dict):
                continue
            predicate = str(rel.get("predicate") or rel.get("name") or "").strip()
            if predicate:
                rel_names.add(predicate)

    return prop_map, rel_names


def _resolve_import_type(raw_type: Any) -> Optional[str]:
    if not raw_type:
        return None
    raw = str(raw_type).strip()
    if not raw:
        return None
    lowered = raw.lower()
    if lowered.startswith("xsd:"):
        return raw
    supported = {
        "string",
        "text",
        "integer",
        "int",
        "long",
        "decimal",
        "number",
        "float",
        "double",
        "boolean",
        "bool",
        "date",
        "datetime",
        "timestamp",
        "email",
        "url",
        "uri",
        "uuid",
        "ip",
        "phone",
        "json",
    }
    if lowered not in supported:
        return None
    return normalize_import_target_type(lowered)


@router.post("/mapping-specs", response_model=Dict[str, Any], status_code=status.HTTP_201_CREATED)
async def create_mapping_spec(
    body: CreateMappingSpecRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    oms_client: OMSClient = OMSClientDep,
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
        artifact_output_name = str(payload.get("artifact_output_name") or dataset.name or "").strip()
        if not artifact_output_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="artifact_output_name is required")
        schema_hash = str(payload.get("schema_hash") or "").strip()
        if not schema_hash:
            latest_version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
            schema_hash = _compute_schema_hash_from_sample(latest_version.sample_json) if latest_version else None
            if not schema_hash:
                schema_hash = _compute_schema_hash_from_sample(dataset.schema_json)
        if not schema_hash:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="schema_hash is required for mapping spec")
        latest_version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
        schema_columns = _extract_schema_columns(
            latest_version.sample_json if latest_version else dataset.schema_json
        )
        if not schema_columns:
            schema_columns = _extract_schema_columns(dataset.schema_json)
        if not schema_columns:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Dataset schema columns are required for mapping spec validation",
            )
        mappings = payload.get("mappings") or []
        if not isinstance(mappings, list) or not mappings:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="mappings is required")

        target_field_types = payload.get("target_field_types") if isinstance(payload.get("target_field_types"), dict) else None
        options = payload.get("options") if isinstance(payload.get("options"), dict) else None
        status_value = str(payload.get("status") or "ACTIVE").strip().upper()
        auto_sync = bool(payload.get("auto_sync", True))

        missing_sources: List[str] = []
        mapped_targets: List[str] = []
        for item in mappings:
            if not isinstance(item, dict):
                continue
            source_field = str(item.get("source_field") or "").strip()
            target_field = str(item.get("target_field") or "").strip()
            if source_field and source_field not in schema_columns:
                missing_sources.append(source_field)
            if target_field:
                mapped_targets.append(target_field)
        if missing_sources:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "MAPPING_SPEC_SOURCE_MISSING", "missing_sources": sorted(set(missing_sources))},
            )

        ontology_branch = str((options or {}).get("ontology_branch") or dataset.branch or "main").strip() or "main"
        ontology_payload = await oms_client.get_ontology(
            dataset.db_name, target_class_id, branch=ontology_branch
        )
        prop_map, rel_names = _extract_ontology_fields(ontology_payload)
        if not prop_map:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Target ontology class schema is required for mapping spec validation",
            )

        unknown_targets = [t for t in mapped_targets if t not in prop_map]
        relationship_targets = [t for t in mapped_targets if t in rel_names]
        if unknown_targets:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "MAPPING_SPEC_TARGET_UNKNOWN", "missing_targets": sorted(set(unknown_targets))},
            )
        if relationship_targets:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "code": "MAPPING_SPEC_RELATIONSHIP_TARGET",
                    "targets": sorted(set(relationship_targets)),
                },
            )

        required_fields = {
            name
            for name, meta in prop_map.items()
            if bool(meta.get("required"))
        }
        explicit_pk = {
            name
            for name, meta in prop_map.items()
            if bool(meta.get("primary_key") or meta.get("primaryKey"))
        }
        if not explicit_pk:
            expected_pk = f"{target_class_id.lower()}_id"
            if expected_pk in prop_map:
                explicit_pk.add(expected_pk)

        options = options or {}
        pk_targets = options.get("primary_key_targets") or options.get("target_primary_keys")
        if isinstance(pk_targets, str):
            pk_targets = [pk.strip() for pk in pk_targets.split(",") if pk.strip()]
        if isinstance(pk_targets, list):
            pk_targets = [str(pk).strip() for pk in pk_targets if str(pk).strip()]
        else:
            pk_targets = None
        if not pk_targets:
            pk_targets = sorted(explicit_pk)

        missing_required = sorted(required_fields - set(mapped_targets))
        if missing_required:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "MAPPING_SPEC_REQUIRED_MISSING", "missing_targets": missing_required},
            )
        if not pk_targets:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="primary_key_targets is required when no primary_key is defined on target class",
            )
        missing_pk = sorted(set(pk_targets) - set(mapped_targets))
        if missing_pk:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "MAPPING_SPEC_PRIMARY_KEY_MISSING", "missing_targets": missing_pk},
            )

        resolved_field_types: Dict[str, str] = {}
        unsupported_types: List[str] = []
        for target in mapped_targets:
            prop = prop_map.get(target) or {}
            raw_type = prop.get("type") or prop.get("data_type") or prop.get("datatype")
            if raw_type in {"link", "array"}:
                unsupported_types.append(target)
                continue
            import_type = _resolve_import_type(raw_type)
            if not import_type:
                unsupported_types.append(target)
                continue
            resolved_field_types[target] = import_type
        if unsupported_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "MAPPING_SPEC_UNSUPPORTED_TYPES", "targets": sorted(set(unsupported_types))},
            )

        if target_field_types:
            mismatches: List[Dict[str, Any]] = []
            for target in mapped_targets:
                provided = target_field_types.get(target)
                expected = resolved_field_types.get(target)
                if not provided:
                    mismatches.append(
                        {"target_field": target, "reason": "missing", "expected": expected}
                    )
                else:
                    normalized = normalize_import_target_type(provided)
                    if expected and normalized != expected:
                        mismatches.append(
                            {
                                "target_field": target,
                                "reason": "mismatch",
                                "expected": expected,
                                "provided": provided,
                            }
                        )
            if mismatches:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={"code": "MAPPING_SPEC_TARGET_TYPE_MISMATCH", "mismatches": mismatches},
                )

        target_field_types = resolved_field_types

        record = await objectify_registry.create_mapping_spec(
            dataset_id=dataset_id,
            dataset_branch=dataset_branch,
            artifact_output_name=artifact_output_name,
            schema_hash=schema_hash,
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
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT, detail="Artifact output is missing schema_hash"
                )
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
        if resolved_schema_hash and mapping_spec.schema_hash and mapping_spec.schema_hash != resolved_schema_hash:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Mapping spec schema_hash mismatch")

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
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))
