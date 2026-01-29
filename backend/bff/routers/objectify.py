"""
Objectify (Dataset -> Ontology) API.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from uuid import uuid4

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from shared.models.objectify_job import ObjectifyJob
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import sanitize_input, validate_branch_name, validate_class_id
from shared.security.auth_utils import enforce_db_scope
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.pipeline.pipeline_schema_utils import normalize_schema_type
from shared.utils.import_type_normalization import normalize_import_target_type
from shared.utils.key_spec import normalize_key_spec
from shared.utils.s3_uri import parse_s3_uri
from shared.utils.schema_hash import compute_schema_hash
from shared.security.database_access import DATA_ENGINEER_ROLES, DOMAIN_MODEL_ROLES, enforce_database_role
from bff.dependencies import OMSClientDep
from bff.services.oms_client import OMSClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/objectify", tags=["Objectify"])


_ALLOWED_SOURCE_TYPES = {
    "xsd:string",
    "xsd:integer",
    "xsd:decimal",
    "xsd:boolean",
    "xsd:date",
    "xsd:dateTime",
    "xsd:time",
}

_TYPE_COMPATIBILITY = {
    "xsd:string": {"xsd:string"},
    "xsd:integer": {"xsd:integer"},
    "xsd:decimal": {"xsd:decimal", "xsd:integer"},
    "xsd:boolean": {"xsd:boolean"},
    "xsd:date": {"xsd:date", "xsd:dateTime"},
    "xsd:dateTime": {"xsd:dateTime", "xsd:date"},
    "xsd:time": {"xsd:time"},
}


def _is_type_compatible(source_type: str, target_type: str) -> bool:
    allowed = _TYPE_COMPATIBILITY.get(target_type)
    if allowed is None:
        return source_type == target_type
    return source_type in allowed


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


async def _require_db_role(request: Request, *, db_name: str, roles) -> None:  # noqa: ANN001
    try:
        await enforce_database_role(headers=request.headers, db_name=db_name, required_roles=roles)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc


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
    backing_datasource_id: Optional[str] = Field(default=None, description="Backing datasource id")
    backing_datasource_version_id: Optional[str] = Field(default=None, description="Backing datasource version id")
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


class RunObjectifyDAGRequest(BaseModel):
    """Topologically enqueue objectify jobs based on mapping-spec relationship dependencies."""

    class_ids: List[str] = Field(..., min_length=1, description="Target ontology class ids to objectify")
    branch: str = Field(default="main", description="Ontology branch to use for all jobs")
    include_dependencies: bool = Field(
        default=True,
        description="If true, automatically include transitive dependencies referenced via relationships",
    )
    max_depth: int = Field(default=10, ge=0, le=50, description="Max dependency expansion depth")
    dry_run: bool = Field(default=False, description="If true, only returns the computed plan (no jobs queued)")
    options: Optional[Dict[str, Any]] = Field(default=None, description="Options merged into each job options")


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


def _extract_schema_types(schema: Any) -> Dict[str, str]:
    if not isinstance(schema, dict):
        return {}
    columns = schema.get("columns")
    if isinstance(columns, list):
        output: Dict[str, str] = {}
        for col in columns:
            if isinstance(col, dict):
                name = str(col.get("name") or col.get("column") or "").strip()
                raw_type = col.get("type") or col.get("data_type") or col.get("datatype")
            else:
                name = str(col).strip()
                raw_type = None
            if name:
                output[name] = normalize_schema_type(raw_type)
        return output
    fields = schema.get("fields")
    if isinstance(fields, list):
        output = {}
        for col in fields:
            if not isinstance(col, dict):
                continue
            name = str(col.get("name") or "").strip()
            if name:
                output[name] = normalize_schema_type(col.get("type"))
        return output
    props = schema.get("properties")
    if isinstance(props, dict):
        return {str(key).strip(): normalize_schema_type(val) for key, val in props.items() if str(key).strip()}
    return {}


def _normalize_mapping_pair(item: Any) -> Optional[tuple[str, str]]:
    if not isinstance(item, dict):
        return None
    source = str(item.get("source_field") or "").strip()
    target = str(item.get("target_field") or "").strip()
    if not source or not target:
        return None
    return source, target


def _build_mapping_change_summary(
    previous_mappings: List[Dict[str, Any]],
    new_mappings: List[Dict[str, Any]],
) -> Dict[str, Any]:
    previous_pairs = {
        pair for pair in (_normalize_mapping_pair(item) for item in previous_mappings) if pair
    }
    new_pairs = {pair for pair in (_normalize_mapping_pair(item) for item in new_mappings) if pair}
    added_pairs = sorted(new_pairs - previous_pairs)
    removed_pairs = sorted(previous_pairs - new_pairs)

    previous_by_target: Dict[str, set[str]] = {}
    for source, target in previous_pairs:
        previous_by_target.setdefault(target, set()).add(source)

    new_by_target: Dict[str, set[str]] = {}
    for source, target in new_pairs:
        new_by_target.setdefault(target, set()).add(source)

    changed_targets: List[Dict[str, Any]] = []
    for target in sorted(set(previous_by_target) | set(new_by_target)):
        previous_sources = previous_by_target.get(target, set())
        new_sources = new_by_target.get(target, set())
        if previous_sources != new_sources:
            changed_targets.append(
                {
                    "target_field": target,
                    "previous_sources": sorted(previous_sources),
                    "new_sources": sorted(new_sources),
                }
            )

    impacted_targets = {
        target for _, target in added_pairs + removed_pairs
    }
    impacted_targets.update(item["target_field"] for item in changed_targets)

    impacted_sources = {
        source for source, _ in added_pairs + removed_pairs
    }
    for item in changed_targets:
        impacted_sources.update(item["previous_sources"])
        impacted_sources.update(item["new_sources"])

    return {
        "added": [{"source_field": source, "target_field": target} for source, target in added_pairs],
        "removed": [{"source_field": source, "target_field": target} for source, target in removed_pairs],
        "changed_targets": changed_targets,
        "impacted_targets": sorted(impacted_targets),
        "impacted_sources": sorted(impacted_sources),
        "counts": {
            "added": len(added_pairs),
            "removed": len(removed_pairs),
            "changed_targets": len(changed_targets),
        },
        "has_changes": bool(added_pairs or removed_pairs or changed_targets),
    }


def _normalize_ontology_payload(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    if isinstance(payload, dict):
        return payload
    return {}


def _extract_resource_payload(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    if isinstance(payload, dict):
        return payload
    return {}


def _extract_ontology_fields(payload: Any) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
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

    rel_map: Dict[str, Dict[str, Any]] = {}
    if isinstance(relationships, list):
        for rel in relationships:
            if not isinstance(rel, dict):
                continue
            predicate = str(rel.get("predicate") or rel.get("name") or "").strip()
            if predicate:
                rel_map[predicate] = rel

    return prop_map, rel_map


def _resolve_import_type(raw_type: Any) -> Optional[str]:
    if not raw_type:
        return None
    raw = str(raw_type).strip()
    if not raw:
        return None
    lowered = raw.lower()
    if lowered.startswith("xsd:"):
        return raw
    if lowered.startswith("sys:"):
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
        "array",
        "struct",
        "object",
        "vector",
        "geopoint",
        "geoshape",
        "marking",
        "cipher",
        "attachment",
        "media",
        "time_series",
        "timeseries",
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
        recorded_gate = False

        async def _record_gate_result(
            *,
            status_value: str,
            detail: Any,
            mapping_spec_id: Optional[str] = None,
        ) -> None:
            nonlocal recorded_gate
            if recorded_gate:
                return
            recorded_gate = True
            try:
                subject_id = mapping_spec_id or f"{dataset_id}:{target_class_id}"
                await dataset_registry.record_gate_result(
                    scope="objectify_preflight",
                    subject_type="mapping_spec",
                    subject_id=subject_id,
                    status=status_value,
                    details={
                        "dataset_id": dataset_id,
                        "target_class_id": target_class_id,
                        "detail": detail,
                    },
                )
            except Exception as exc:
                logger.warning("Failed to record mapping spec gate result: %s", exc)

        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
        try:
            enforce_db_scope(request.headers, db_name=dataset.db_name)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
        await _require_db_role(request, db_name=dataset.db_name, roles=DOMAIN_MODEL_ROLES)

        dataset_branch = str(payload.get("dataset_branch") or dataset.branch or "main").strip() or "main"
        artifact_output_name = str(payload.get("artifact_output_name") or dataset.name or "").strip()
        if not artifact_output_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="artifact_output_name is required")
        backing_datasource_id = str(payload.get("backing_datasource_id") or "").strip() or None
        backing_datasource_version_id = str(payload.get("backing_datasource_version_id") or "").strip() or None
        backing = None
        backing_version = None
        if backing_datasource_version_id:
            backing_version = await dataset_registry.get_backing_datasource_version(
                version_id=backing_datasource_version_id
            )
            if not backing_version:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backing datasource version not found")
            backing = await dataset_registry.get_backing_datasource(backing_id=backing_version.backing_id)
            if not backing or backing.dataset_id != dataset_id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Backing datasource version does not match dataset",
                )
        if backing_datasource_id and not backing:
            backing = await dataset_registry.get_backing_datasource(backing_id=backing_datasource_id)
            if not backing:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backing datasource not found")
            if backing.dataset_id != dataset_id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Backing datasource does not match dataset",
                )
        if not backing:
            backing = await dataset_registry.get_or_create_backing_datasource(
                dataset=dataset,
                source_type=dataset.source_type,
                source_ref=dataset.source_ref,
            )

        schema_hash = str(payload.get("schema_hash") or "").strip() or None
        schema_version = None
        if backing_version:
            if schema_hash and backing_version.schema_hash and schema_hash != backing_version.schema_hash:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "MAPPING_SPEC_BACKING_SCHEMA_MISMATCH",
                        "expected": backing_version.schema_hash,
                        "provided": schema_hash,
                    },
                )
            schema_hash = backing_version.schema_hash
            schema_version = await dataset_registry.get_version(
                version_id=backing_version.dataset_version_id
            )
            if not schema_version:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Backing datasource version dataset is missing",
                )
        else:
            schema_version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
            if not schema_version:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Dataset version is required for mapping spec validation",
                )
            if not schema_hash:
                schema_hash = _compute_schema_hash_from_sample(schema_version.sample_json)
                if not schema_hash:
                    schema_hash = _compute_schema_hash_from_sample(dataset.schema_json)
            if not schema_hash:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="schema_hash is required for mapping spec",
                )
            backing_version = await dataset_registry.get_or_create_backing_datasource_version(
                backing_id=backing.backing_id,
                dataset_version_id=schema_version.version_id,
                schema_hash=schema_hash,
            )

        schema_columns = _extract_schema_columns(
            schema_version.sample_json if schema_version else dataset.schema_json
        )
        if not schema_columns:
            schema_columns = _extract_schema_columns(dataset.schema_json)
        if not schema_columns:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Dataset schema columns are required for mapping spec validation",
            )
        schema_types = _extract_schema_types(
            schema_version.sample_json if schema_version else dataset.schema_json
        )
        if not schema_types:
            schema_types = _extract_schema_types(dataset.schema_json)
        mappings = payload.get("mappings") or []
        if not isinstance(mappings, list) or not mappings:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="mappings is required")

        target_field_types = payload.get("target_field_types") if isinstance(payload.get("target_field_types"), dict) else None
        options = payload.get("options") if isinstance(payload.get("options"), dict) else None
        status_value = str(payload.get("status") or "ACTIVE").strip().upper()
        auto_sync = bool(payload.get("auto_sync", True))
        if options is None:
            options = {}
        change_reason = request.headers.get("X-Change-Reason") or request.headers.get("X-Change-Note")
        if change_reason and "change_reason" not in options:
            options["change_reason"] = str(change_reason).strip()
        options.setdefault(
            "impact_scope",
            {
                "schema_hash": schema_hash,
                "dataset_version_id": schema_version.version_id if schema_version else None,
                "artifact_output_name": artifact_output_name,
            },
        )
        try:
            previous_spec = await objectify_registry.get_active_mapping_spec(
                dataset_id=dataset_id,
                dataset_branch=dataset_branch,
                target_class_id=target_class_id,
                artifact_output_name=artifact_output_name,
                schema_hash=schema_hash,
            )
        except Exception as exc:
            logger.warning("Failed to resolve previous mapping spec: %s", exc)
            previous_spec = None
        if previous_spec:
            change_summary = _build_mapping_change_summary(previous_spec.mappings, mappings)
            change_summary.setdefault("previous_mapping_spec_id", previous_spec.mapping_spec_id)
            change_summary.setdefault("previous_version", previous_spec.version)
            change_summary.setdefault("expected_version", previous_spec.version + 1)
            options.setdefault("previous_mapping_spec_id", previous_spec.mapping_spec_id)
            options.setdefault("change_summary", change_summary)

        missing_sources: List[str] = []
        mapped_targets: List[str] = []
        mapping_sources: List[str] = []
        for item in mappings:
            if not isinstance(item, dict):
                continue
            source_field = str(item.get("source_field") or "").strip()
            target_field = str(item.get("target_field") or "").strip()
            if source_field and source_field not in schema_columns:
                missing_sources.append(source_field)
            if source_field:
                mapping_sources.append(source_field)
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
        prop_map, rel_map = _extract_ontology_fields(ontology_payload)
        if not prop_map:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Target ontology class schema is required for mapping spec validation",
            )

        object_type_payload = None
        object_type_spec: Dict[str, Any] = {}
        try:
            object_type_payload = await oms_client.get_ontology_resource(
                dataset.db_name,
                resource_type="object_type",
                resource_id=target_class_id,
                branch=ontology_branch,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != status.HTTP_404_NOT_FOUND:
                raise
        if not object_type_payload:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"code": "OBJECT_TYPE_CONTRACT_MISSING", "class_id": target_class_id},
            )
        object_type_resource = _extract_resource_payload(object_type_payload)
        if isinstance(object_type_resource, dict):
            object_type_spec = (
                object_type_resource.get("spec") if isinstance(object_type_resource.get("spec"), dict) else {}
            )
        status_value = str(object_type_spec.get("status") or "ACTIVE").strip().upper()
        if status_value != "ACTIVE":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"code": "OBJECT_TYPE_INACTIVE", "status": status_value},
            )
        normalized_pk_spec = normalize_key_spec(object_type_spec.get("pk_spec") or {}, columns=list(prop_map.keys()))
        if not normalized_pk_spec.get("primary_key"):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"code": "OBJECT_TYPE_PRIMARY_KEY_MISSING", "class_id": target_class_id},
            )
        if not normalized_pk_spec.get("title_key"):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"code": "OBJECT_TYPE_TITLE_KEY_MISSING", "class_id": target_class_id},
            )

        options = payload.get("options") if isinstance(payload.get("options"), dict) else {}
        link_index_mode = str(options.get("mode") or options.get("job_type") or "").strip().lower() == "link_index"

        backing_source = (
            object_type_spec.get("backing_source")
            if isinstance(object_type_spec.get("backing_source"), dict)
            else {}
        )
        backing_kind = str(backing_source.get("kind") or "").strip().lower()
        backing_ref = str(backing_source.get("ref") or "").strip()
        backing_schema = str(backing_source.get("schema_hash") or backing_source.get("schemaHash") or "").strip()
        if backing_kind and not link_index_mode:
            if backing_kind in {"backing_datasource", "backing-datasource", "backingdatasource"}:
                if backing_ref and backing and backing_ref != backing.backing_id:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "code": "OBJECT_TYPE_BACKING_MISMATCH",
                            "expected": backing_ref,
                            "observed": backing.backing_id,
                        },
                    )
            elif backing_kind in {"dataset", "dataset_id", "dataset-id"}:
                if backing_ref and backing_ref != dataset.dataset_id:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "code": "OBJECT_TYPE_BACKING_DATASET_MISMATCH",
                            "expected": backing_ref,
                            "observed": dataset.dataset_id,
                        },
                    )
        if backing_schema and schema_hash and backing_schema != schema_hash and not link_index_mode:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "OBJECT_TYPE_SCHEMA_HASH_MISMATCH",
                    "expected": backing_schema,
                    "observed": schema_hash,
                },
            )

        unknown_targets = [t for t in mapped_targets if t not in prop_map and t not in rel_map]
        relationship_targets = [t for t in mapped_targets if t in rel_map]
        if unknown_targets:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "MAPPING_SPEC_TARGET_UNKNOWN", "missing_targets": sorted(set(unknown_targets))},
            )
        options = options or {}
        if not link_index_mode:
            unsupported_relationships: List[str] = []
            for target in relationship_targets:
                rel = rel_map.get(target) or {}
                cardinality = str(rel.get("cardinality") or "").strip().lower()
                if cardinality.endswith(":n") or cardinality.endswith(":m"):
                    unsupported_relationships.append(target)
            if unsupported_relationships:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "code": "MAPPING_SPEC_RELATIONSHIP_CARDINALITY_UNSUPPORTED",
                        "targets": sorted(set(unsupported_relationships)),
                    },
                )
        elif not relationship_targets:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "MAPPING_SPEC_RELATIONSHIP_REQUIRED", "message": "link_index requires relationship targets"},
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
        object_type_pk_targets = [str(item).strip() for item in normalized_pk_spec.get("primary_key") or [] if str(item).strip()]
        object_type_title_targets = [str(item).strip() for item in normalized_pk_spec.get("title_key") or [] if str(item).strip()]
        required_fields.update(
            {str(item).strip() for item in normalized_pk_spec.get("required_fields") or [] if str(item).strip()}
        )
        if link_index_mode:
            required_fields = set()
        if not explicit_pk:
            expected_pk = f"{target_class_id.lower()}_id"
            if expected_pk in prop_map:
                explicit_pk.add(expected_pk)

        pk_targets = options.get("primary_key_targets") or options.get("target_primary_keys")
        if isinstance(pk_targets, str):
            pk_targets = [pk.strip() for pk in pk_targets.split(",") if pk.strip()]
        if isinstance(pk_targets, list):
            pk_targets = [str(pk).strip() for pk in pk_targets if str(pk).strip()]
        else:
            pk_targets = None
        if not pk_targets:
            pk_targets = object_type_pk_targets or sorted(explicit_pk)
        if object_type_pk_targets and set(pk_targets) != set(object_type_pk_targets):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "code": "MAPPING_SPEC_PRIMARY_KEY_MISMATCH",
                    "expected": object_type_pk_targets,
                    "observed": pk_targets,
                },
            )

        key_spec = await dataset_registry.get_key_spec_for_dataset(
            dataset_id=dataset_id,
            dataset_version_id=(schema_version.version_id if schema_version else None),
        )
        if key_spec:
            normalized_spec = normalize_key_spec(key_spec.spec)
            key_pk_sources = set(normalized_spec.get("primary_key") or [])
            mapping_source_set = {src for src in mapping_sources if src}
            if key_pk_sources:
                missing_pk_sources = sorted(key_pk_sources - mapping_source_set)
                if missing_pk_sources:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "code": "MAPPING_SPEC_DATASET_PK_MISSING",
                            "missing_sources": missing_pk_sources,
                        },
                    )
                invalid_pk_targets = sorted(
                    {
                        str(item.get("target_field") or "").strip()
                        for item in mappings
                        if isinstance(item, dict)
                        and str(item.get("source_field") or "").strip() in key_pk_sources
                        and str(item.get("target_field") or "").strip() not in set(pk_targets or [])
                    }
                )
                if invalid_pk_targets:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "code": "MAPPING_SPEC_DATASET_PK_TARGET_MISMATCH",
                            "targets": invalid_pk_targets,
                        },
                    )
            required_sources = set(normalized_spec.get("required_fields") or [])
            if required_sources:
                missing_required_sources = sorted(required_sources - mapping_source_set)
                if missing_required_sources:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "code": "MAPPING_SPEC_REQUIRED_SOURCE_MISSING",
                            "missing_sources": missing_required_sources,
                        },
                    )
            options.setdefault("key_spec_id", key_spec.key_spec_id)

        if not link_index_mode:
            missing_required = sorted(required_fields - set(mapped_targets))
            if missing_required:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={"code": "MAPPING_SPEC_REQUIRED_MISSING", "missing_targets": missing_required},
                )
            missing_title = sorted(set(object_type_title_targets) - set(mapped_targets))
            if missing_title:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={"code": "MAPPING_SPEC_TITLE_KEY_MISSING", "missing_targets": missing_title},
                )
            unique_key_fields = {
                str(field).strip()
                for key_fields in (normalized_pk_spec.get("unique_keys") or [])
                if isinstance(key_fields, list)
                for field in key_fields
                if str(field).strip()
            }
            missing_unique = sorted(unique_key_fields - set(mapped_targets))
            if missing_unique:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={"code": "MAPPING_SPEC_UNIQUE_KEY_MISSING", "missing_targets": missing_unique},
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
            if target in rel_map:
                continue
            prop = prop_map.get(target) or {}
            raw_type = prop.get("type") or prop.get("data_type") or prop.get("datatype")
            is_relationship = bool(
                raw_type == "link"
                or prop.get("isRelationship")
                or prop.get("target")
                or prop.get("linkTarget")
            )
            items = prop.get("items") if isinstance(prop, dict) else None
            if isinstance(items, dict):
                item_type = items.get("type")
                if item_type == "link" and (items.get("target") or items.get("linkTarget")):
                    is_relationship = True
            if is_relationship:
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

        missing_source_types: List[str] = []
        unsupported_source_types: List[Dict[str, Any]] = []
        incompatible_types: List[Dict[str, Any]] = []
        for item in mappings:
            if not isinstance(item, dict):
                continue
            source_field = str(item.get("source_field") or "").strip()
            target_field = str(item.get("target_field") or "").strip()
            if not source_field or not target_field:
                continue
            if target_field in rel_map:
                continue
            source_type = schema_types.get(source_field)
            if not source_type:
                missing_source_types.append(source_field)
                continue
            if source_type not in _ALLOWED_SOURCE_TYPES:
                unsupported_source_types.append(
                    {"source_field": source_field, "source_type": source_type}
                )
                continue
            expected_type = resolved_field_types.get(target_field)
            if expected_type and not _is_type_compatible(source_type, expected_type):
                incompatible_types.append(
                    {
                        "source_field": source_field,
                        "source_type": source_type,
                        "target_field": target_field,
                        "expected_type": expected_type,
                    }
                )
        if missing_source_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "code": "MAPPING_SPEC_SOURCE_TYPE_UNKNOWN",
                    "missing_sources": sorted(set(missing_source_types)),
                },
            )
        if unsupported_source_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "code": "MAPPING_SPEC_SOURCE_TYPE_UNSUPPORTED",
                    "sources": unsupported_source_types,
                },
            )
        if incompatible_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "code": "MAPPING_SPEC_TYPE_INCOMPATIBLE",
                    "mismatches": incompatible_types,
                },
            )

        if target_field_types:
            mismatches: List[Dict[str, Any]] = []
            for target in mapped_targets:
                if target in rel_map:
                    continue
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
            backing_datasource_id=(backing.backing_id if backing else None),
            backing_datasource_version_id=(backing_version.version_id if backing_version else None),
            target_class_id=target_class_id,
            mappings=mappings,
            target_field_types=target_field_types,
            status=status_value,
            auto_sync=auto_sync,
            options=options,
        )
        await _record_gate_result(
            status_value="PASS",
            detail={
                "mapping_spec_id": record.mapping_spec_id,
                "schema_hash": schema_hash,
                "backing_datasource_id": record.backing_datasource_id,
                "backing_datasource_version_id": record.backing_datasource_version_id,
            },
            mapping_spec_id=record.mapping_spec_id,
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
    except HTTPException as exc:
        await _record_gate_result(status_value="FAIL", detail=exc.detail)
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
            backing_version = await dataset_registry.get_backing_datasource_version(
                version_id=mapping_spec.backing_datasource_version_id
            )
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
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.post("/databases/{db_name}/run-dag", response_model=Dict[str, Any])
async def run_objectify_dag(
    db_name: str,
    body: RunObjectifyDAGRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    oms_client: OMSClient = OMSClientDep,
) -> Dict[str, Any]:
    """
    Enterprise helper: enqueue multiple objectify jobs in dependency order.

    Dependency graph is derived from:
    - Ontology relationships (predicate -> target class)
    - Mapping spec mappings that target relationship predicates
    """
    try:
        db_name = sanitize_input(db_name)
        enforce_db_scope(request.headers, db_name=db_name)
        await _require_db_role(request, db_name=db_name, roles=DATA_ENGINEER_ROLES)

        branch = validate_branch_name(body.branch or "main")
        include_dependencies = bool(body.include_dependencies)
        max_depth = int(body.max_depth or 0)
        run_id = uuid4().hex
        override_options = body.options if isinstance(body.options, dict) else {}

        from collections import defaultdict, deque

        class_infos: Dict[str, Dict[str, Any]] = {}
        deps_by_class: Dict[str, set[str]] = defaultdict(set)  # class -> deps
        dependents_by_class: Dict[str, set[str]] = defaultdict(set)  # dep -> dependents
        missing_deps_by_class: Dict[str, set[str]] = defaultdict(set)

        async def _fetch_object_type_contract(class_id: str) -> Dict[str, Any]:
            try:
                resp = await oms_client.get_ontology_resource(
                    db_name,
                    resource_type="object_type",
                    resource_id=class_id,
                    branch=branch,
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == status.HTTP_404_NOT_FOUND:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={"code": "OBJECT_TYPE_CONTRACT_MISSING", "class_id": class_id},
                    ) from exc
                raise
            resource = _extract_resource_payload(resp)
            spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
            return {"resource": resource, "spec": spec}

        async def _resolve_mapping_spec_for_object_type(
            *,
            class_id: str,
            backing_source: Dict[str, Any],
        ):
            dataset_id = str(backing_source.get("dataset_id") or "").strip()
            dataset_branch = str(backing_source.get("branch") or "main").strip() or "main"
            schema_hash = str(
                backing_source.get("schema_hash") or backing_source.get("schemaHash") or ""
            ).strip() or None
            if not dataset_id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={"code": "OBJECT_TYPE_BACKING_DATASET_MISSING", "class_id": class_id},
                )

            mapping_spec = await objectify_registry.get_active_mapping_spec(
                dataset_id=dataset_id,
                dataset_branch=dataset_branch,
                target_class_id=class_id,
                schema_hash=schema_hash,
            )
            if not mapping_spec and schema_hash:
                # Best-effort fallback: allow orchestration to proceed if schema_hash is absent in older specs.
                mapping_spec = await objectify_registry.get_active_mapping_spec(
                    dataset_id=dataset_id,
                    dataset_branch=dataset_branch,
                    target_class_id=class_id,
                )
            if not mapping_spec:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "MAPPING_SPEC_NOT_FOUND",
                        "class_id": class_id,
                        "dataset_id": dataset_id,
                        "dataset_branch": dataset_branch,
                        "schema_hash": schema_hash,
                    },
                )
            return mapping_spec, dataset_id, dataset_branch

        async def _fetch_relationship_targets(class_id: str) -> Dict[str, str]:
            ontology_payload = await oms_client.get_ontology(db_name, class_id, branch=branch)
            _, rel_map = _extract_ontology_fields(ontology_payload)
            targets: Dict[str, str] = {}
            for predicate, rel in rel_map.items():
                if not isinstance(rel, dict):
                    continue
                target = str(
                    rel.get("target")
                    or rel.get("target_class")
                    or rel.get("targetClass")
                    or rel.get("target_class_id")
                    or ""
                ).strip()
                if predicate and target:
                    targets[predicate] = target
            return targets

        async def _load_class_info(class_id: str) -> Dict[str, Any]:
            if class_id in class_infos:
                return class_infos[class_id]

            class_id = validate_class_id(class_id)
            contract = await _fetch_object_type_contract(class_id)
            spec = contract.get("spec") if isinstance(contract.get("spec"), dict) else {}
            backing_source = (
                spec.get("backing_source") if isinstance(spec.get("backing_source"), dict) else {}
            )
            mapping_spec, dataset_id, dataset_branch = await _resolve_mapping_spec_for_object_type(
                class_id=class_id,
                backing_source=backing_source,
            )

            dataset_version_id = str(backing_source.get("dataset_version_id") or "").strip() or None
            version = None
            if dataset_version_id:
                version = await dataset_registry.get_version(version_id=dataset_version_id)
                if not version or str(getattr(version, "dataset_id", "")) != dataset_id:
                    version = None
            if not version:
                version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
            if not version or not getattr(version, "artifact_key", None):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={"code": "DATASET_VERSION_MISSING", "class_id": class_id, "dataset_id": dataset_id},
                )

            rel_targets = await _fetch_relationship_targets(class_id)
            info = {
                "class_id": class_id,
                "object_type": contract.get("resource") or {},
                "object_type_spec": spec,
                "backing_source": backing_source,
                "dataset_id": dataset_id,
                "dataset_branch": dataset_branch,
                "dataset_version_id": str(getattr(version, "version_id", "")),
                "artifact_key": str(getattr(version, "artifact_key", "")),
                "mapping_spec": mapping_spec,
                "relationship_targets": rel_targets,  # predicate -> target class
            }
            class_infos[class_id] = info
            return info

        # Build dependency closure
        start = [validate_class_id(c) for c in (body.class_ids or []) if str(c).strip()]
        if not start:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="class_ids is required")
        requested_set = set(start)

        queue = deque([(c, 0) for c in start])
        while queue:
            current, depth = queue.popleft()
            info = await _load_class_info(current)
            mapping_spec = info["mapping_spec"]
            rel_targets = info["relationship_targets"]

            for mapping in (mapping_spec.mappings or []):
                if not isinstance(mapping, dict):
                    continue
                target_field = str(mapping.get("target_field") or "").strip()
                if not target_field:
                    continue
                dep_class = rel_targets.get(target_field)
                if not dep_class:
                    continue
                dep_class = validate_class_id(dep_class)
                deps_by_class[current].add(dep_class)
                dependents_by_class[dep_class].add(current)
                if include_dependencies:
                    if dep_class not in class_infos and depth < max_depth:
                        queue.append((dep_class, depth + 1))
                    elif dep_class not in class_infos and depth >= max_depth:
                        missing_deps_by_class[current].add(dep_class)
                else:
                    if dep_class not in requested_set:
                        missing_deps_by_class[current].add(dep_class)

        if missing_deps_by_class:
            missing_detail = {cls: sorted(deps) for cls, deps in missing_deps_by_class.items() if deps}
            if missing_detail:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "OBJECTIFY_DAG_DEPENDENCIES_MISSING",
                        "message": (
                            "Missing dependency classes for safe objectify ordering. "
                            "Re-run with include_dependencies=true or include the dependencies in class_ids."
                        ),
                        "missing_dependencies": missing_detail,
                        "requested_classes": sorted(requested_set),
                        "branch": branch,
                        "max_depth": max_depth,
                    },
                )

        # Topological sort (Kahn) with deterministic priority:
        # Prefer classes that unblock earlier requested roots first.
        #
        # Note: Objectify jobs for a db:branch are typically serialized by Kafka key, so the enqueue order
        # materially impacts time-to-first-result. We therefore bias the ordering towards the user's
        # requested class_ids (and their dependency chain) rather than arbitrary lexicographic order.
        import heapq

        all_classes = sorted(class_infos.keys())
        in_degree: Dict[str, int] = {c: len(deps_by_class.get(c, set())) for c in all_classes}

        start_order = {class_id: idx for idx, class_id in enumerate(start)}
        default_rank = len(start_order) + 1000
        priority_rank: Dict[str, int] = {c: default_rank for c in all_classes}

        rank_queue = deque()
        for class_id, rank in start_order.items():
            if class_id not in priority_rank:
                continue
            if rank < priority_rank[class_id]:
                priority_rank[class_id] = rank
                rank_queue.append(class_id)

        # Propagate root priority to upstream dependencies (Transaction -> BankAccount -> Person).
        while rank_queue:
            node = rank_queue.popleft()
            node_rank = priority_rank.get(node, default_rank)
            for dep in deps_by_class.get(node, set()):
                if dep not in priority_rank:
                    continue
                if node_rank < priority_rank[dep]:
                    priority_rank[dep] = node_rank
                    rank_queue.append(dep)

        ready: list[tuple[int, str]] = []
        for class_id in all_classes:
            if in_degree.get(class_id, 0) == 0:
                heapq.heappush(ready, (priority_rank.get(class_id, default_rank), class_id))

        ordered: List[str] = []
        while ready:
            _, node = heapq.heappop(ready)
            ordered.append(node)
            for dependent in sorted(dependents_by_class.get(node, set())):
                if dependent not in in_degree:
                    continue
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    heapq.heappush(ready, (priority_rank.get(dependent, default_rank), dependent))

        if len(ordered) != len(all_classes):
            remaining = sorted([c for c in all_classes if in_degree.get(c, 0) > 0])
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "OBJECTIFY_DAG_CYCLE_DETECTED",
                    "message": "Cycle detected in objectify dependency graph; cannot compute safe order",
                    "remaining": remaining,
                },
            )

        plan = [
            {
                "class_id": class_id,
                "dataset_id": class_infos[class_id]["dataset_id"],
                "dataset_branch": class_infos[class_id]["dataset_branch"],
                "dataset_version_id": class_infos[class_id]["dataset_version_id"],
                "mapping_spec_id": class_infos[class_id]["mapping_spec"].mapping_spec_id,
                "mapping_spec_version": class_infos[class_id]["mapping_spec"].version,
            }
            for class_id in ordered
        ]

        if body.dry_run:
            return ApiResponse.success(
                message="Objectify DAG plan computed (dry_run)",
                data={"run_id": run_id, "branch": branch, "ordered_classes": ordered, "plan": plan},
            ).to_dict()

        import asyncio
        import time

        queued: List[Dict[str, Any]] = []
        job_ids_by_class: Dict[str, str] = {}
        deduped_by_class: Dict[str, bool] = {}

        async def _enqueue_class_job(class_id: str) -> str:
            info = class_infos[class_id]
            mapping_spec = info["mapping_spec"]
            dataset_id = info["dataset_id"]
            dataset_branch = info["dataset_branch"]
            dataset_version_id = info["dataset_version_id"]
            artifact_key = info["artifact_key"]

            dedupe_key = objectify_registry.build_dedupe_key(
                dataset_id=dataset_id,
                dataset_branch=dataset_branch,
                mapping_spec_id=mapping_spec.mapping_spec_id,
                mapping_spec_version=mapping_spec.version,
                dataset_version_id=dataset_version_id,
                artifact_id=None,
                artifact_output_name=mapping_spec.artifact_output_name,
            )
            existing = await objectify_registry.get_objectify_job_by_dedupe_key(dedupe_key=dedupe_key)
            if existing:
                job_ids_by_class[class_id] = existing.job_id
                deduped_by_class[class_id] = True
                return existing.job_id

            job_id = str(uuid4())
            options = dict(mapping_spec.options or {})
            options.update(override_options)
            options.setdefault("run_id", run_id)
            options.setdefault("orchestrator", "dag")
            options.setdefault("dag_branch", branch)
            options.setdefault("dag_root_classes", start)
            options.setdefault("include_dependencies", include_dependencies)

            job = ObjectifyJob(
                job_id=job_id,
                db_name=db_name,
                dataset_id=dataset_id,
                dataset_version_id=dataset_version_id,
                artifact_output_name=mapping_spec.artifact_output_name,
                dedupe_key=dedupe_key,
                dataset_branch=dataset_branch,
                artifact_key=artifact_key,
                mapping_spec_id=mapping_spec.mapping_spec_id,
                mapping_spec_version=mapping_spec.version,
                target_class_id=class_id,
                ontology_branch=branch,
                max_rows=options.get("max_rows"),
                batch_size=options.get("batch_size"),
                allow_partial=bool(options.get("allow_partial")),
                options=options,
            )
            await job_queue.publish(job, require_delivery=False)
            job_ids_by_class[class_id] = job_id
            deduped_by_class[class_id] = False
            return job_id

        def _extract_command_status(payload: Any) -> str:
            if not isinstance(payload, dict):
                return ""
            data = payload.get("data") if isinstance(payload.get("data"), dict) else None
            raw = payload.get("status")
            if data and data.get("status") is not None:
                raw = data.get("status")
            return str(raw or "").strip().upper()

        async def _wait_for_objectify_submitted(
            job_id: str,
            *,
            timeout_seconds: int = 600,
        ) -> List[str]:
            deadline = time.monotonic() + float(timeout_seconds)
            last_status: Optional[str] = None
            while time.monotonic() < deadline:
                record = await objectify_registry.get_objectify_job(job_id=job_id)
                if not record:
                    await asyncio.sleep(0.5)
                    continue
                status_value = str(record.status or "").strip().upper()
                last_status = status_value
                if status_value in {"SUBMITTED", "COMPLETED"}:
                    report = record.report or {}
                    command_ids = report.get("command_ids") if isinstance(report, dict) else None
                    if not isinstance(command_ids, list):
                        command_ids = []
                    normalized = [str(cid).strip() for cid in command_ids if str(cid).strip()]
                    # Back-compat: some producers may only populate the top-level command_id column.
                    if not normalized and getattr(record, "command_id", None):
                        normalized = [str(record.command_id).strip()]
                    if not normalized:
                        raise RuntimeError(
                            f"Objectify job submitted without command_ids (job_id={job_id} status={status_value})"
                        )
                    return normalized
                if status_value in {"FAILED", "CANCELLED"}:
                    raise RuntimeError(f"Objectify job failed (job_id={job_id} error={record.error})")
                await asyncio.sleep(0.5)
            raise TimeoutError(f"Timed out waiting for objectify job submission (job_id={job_id} last={last_status})")

        async def _wait_for_command_terminal(
            command_id: str,
            *,
            timeout_seconds: int = 600,
        ) -> None:
            deadline = time.monotonic() + float(timeout_seconds)
            last_status: Optional[str] = None
            while time.monotonic() < deadline:
                try:
                    payload = await oms_client.get(f"/api/v1/commands/{command_id}/status")
                except httpx.HTTPStatusError as exc:
                    status_code = getattr(getattr(exc, "response", None), "status_code", None)
                    if status_code == status.HTTP_404_NOT_FOUND:
                        await asyncio.sleep(0.5)
                        continue
                    raise
                status_value = _extract_command_status(payload)
                last_status = status_value or last_status
                if status_value == "COMPLETED":
                    return
                if status_value in {"FAILED", "CANCELLED"}:
                    raise RuntimeError(f"Command failed (command_id={command_id} status={status_value})")
                await asyncio.sleep(0.5)
            raise TimeoutError(
                f"Timed out waiting for command completion (command_id={command_id} last={last_status})"
            )

        async def _wait_for_class_ready(class_id: str) -> None:
            job_id = job_ids_by_class.get(class_id)
            if not job_id:
                raise RuntimeError(f"Missing job_id for class {class_id}")
            command_ids = await _wait_for_objectify_submitted(job_id)
            for command_id in command_ids:
                await _wait_for_command_terminal(command_id)

        async def _run_ready_orchestrator(initial_classes: List[str]) -> None:
            deps = {c: set(deps_by_class.get(c, set())) for c in ordered}
            started: set[str] = set(initial_classes)
            completed: set[str] = set()
            inflight: Dict[str, asyncio.Task] = {}

            for class_id in initial_classes:
                inflight[class_id] = asyncio.create_task(
                    _wait_for_class_ready(class_id),
                    name=f"objectify-dag:{db_name}:{run_id}:{class_id}",
                )

            while len(completed) < len(ordered):
                # Enqueue any newly-ready classes immediately (Foundry-style frontier scheduling).
                for class_id in ordered:
                    if class_id in started:
                        continue
                    if deps.get(class_id, set()) <= completed:
                        await _enqueue_class_job(class_id)
                        started.add(class_id)
                        inflight[class_id] = asyncio.create_task(
                            _wait_for_class_ready(class_id),
                            name=f"objectify-dag:{db_name}:{run_id}:{class_id}",
                        )

                if not inflight:
                    remaining = [c for c in ordered if c not in completed]
                    raise RuntimeError(f"Objectify DAG deadlocked (remaining={remaining})")

                done, _pending = await asyncio.wait(
                    list(inflight.values()),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in done:
                    finished_class: Optional[str] = None
                    for class_id, inflight_task in inflight.items():
                        if inflight_task is task:
                            finished_class = class_id
                            break
                    if finished_class is None:
                        continue
                    del inflight[finished_class]
                    await task  # propagate exception
                    completed.add(finished_class)

        initial_ready = [c for c in ordered if not deps_by_class.get(c)]
        if not initial_ready:
            initial_ready = [ordered[0]]

        # Enqueue only the initial frontier synchronously to avoid blocking the request.
        for class_id in initial_ready:
            info = class_infos[class_id]
            mapping_spec = info["mapping_spec"]
            dataset_id = info["dataset_id"]
            dataset_branch = info["dataset_branch"]
            dataset_version_id = info["dataset_version_id"]
            job_id = await _enqueue_class_job(class_id)
            record = await objectify_registry.get_objectify_job(job_id=job_id)
            queued.append(
                {
                    "class_id": class_id,
                    "job_id": job_id,
                    "mapping_spec_id": mapping_spec.mapping_spec_id,
                    "dataset_id": dataset_id,
                    "dataset_version_id": dataset_version_id,
                    "status": record.status if record else "QUEUED",
                    "deduped": bool(deduped_by_class.get(class_id)),
                }
            )

        orchestrator_task = asyncio.create_task(
            _run_ready_orchestrator(initial_ready),
            name=f"objectify-dag:{db_name}:{run_id}",
        )

        def _log_orchestrator_result(task: asyncio.Task) -> None:
            try:
                exc = task.exception()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("Objectify DAG task introspection failed: %s", exc)
                return
            if exc:
                logger.error("Objectify DAG orchestration failed (db=%s run_id=%s): %s", db_name, run_id, exc, exc_info=True)
            else:
                logger.info("Objectify DAG orchestration completed (db=%s run_id=%s)", db_name, run_id)

        orchestrator_task.add_done_callback(_log_orchestrator_result)

        return ApiResponse.success(
            message="Objectify DAG queued",
            data={
                "run_id": run_id,
                "branch": branch,
                "ordered_classes": ordered,
                "plan": plan,
                "jobs": queued,
            },
        ).to_dict()
    except httpx.HTTPStatusError as exc:
        detail: Any
        try:
            detail = exc.response.json()
        except Exception:
            detail = exc.response.text
        raise HTTPException(status_code=exc.response.status_code, detail=detail) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to orchestrate objectify DAG: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


# ============================================================
# Enterprise Features: FK Detection, Incremental Objectify
# ============================================================


class DetectRelationshipsRequest(BaseModel):
    confidence_threshold: float = Field(default=0.6, ge=0.0, le=1.0)
    include_sample_analysis: bool = Field(default=True)


class DetectRelationshipsResponse(BaseModel):
    patterns: List[Dict[str, Any]] = Field(default_factory=list)
    suggestions: List[Dict[str, Any]] = Field(default_factory=list)


@router.post(
    "/databases/{db_name}/datasets/{dataset_id}/detect-relationships",
    summary="Detect FK relationships in a dataset",
    description="Analyzes dataset columns to detect potential foreign key relationships based on naming conventions and value overlap.",
)
async def detect_relationships(
    db_name: str,
    dataset_id: str,
    request: Request,
    body: DetectRelationshipsRequest = DetectRelationshipsRequest(),
    branch: str = Query(default="main"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> Dict[str, Any]:
    """Detect potential FK relationships in a dataset."""
    db_name = sanitize_input(db_name)
    dataset_id = sanitize_input(dataset_id)
    await _require_db_role(request, db_name=db_name, roles=DATA_ENGINEER_ROLES)

    try:
        from shared.services.pipeline.fk_pattern_detector import (
            ForeignKeyPatternDetector,
            FKDetectionConfig,
            TargetCandidate,
        )

        # Get source dataset
        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset not found: {dataset_id}",
            )

        schema = dataset.schema_json or {}
        columns = schema.get("columns") or schema.get("fields") or []

        # Get other datasets as target candidates
        all_datasets = await dataset_registry.list_datasets(
            db_name=db_name, branch=branch, limit=200
        )
        target_candidates = []
        for ds in all_datasets:
            if ds.dataset_id == dataset_id:
                continue
            ds_schema = ds.schema_json or {}
            ds_columns = ds_schema.get("columns") or ds_schema.get("fields") or []
            pk_cols = [c.get("name") for c in ds_columns if c.get("name", "").lower() in ("id", "pk")]
            if not pk_cols and ds_columns:
                pk_cols = [ds_columns[0].get("name", "id")]
            target_candidates.append(TargetCandidate(
                candidate_type="dataset",
                candidate_id=ds.dataset_id,
                candidate_name=ds.name or ds.dataset_id,
                pk_columns=pk_cols,
            ))

        # Detect FK patterns
        config = FKDetectionConfig(min_confidence=body.confidence_threshold)
        detector = ForeignKeyPatternDetector(config)
        patterns = detector.detect_patterns(
            source_dataset_id=dataset_id,
            source_schema=columns,
            target_candidates=target_candidates,
        )

        # Generate link type suggestions
        suggestions = []
        for pattern in patterns:
            suggestion = detector.suggest_link_type(pattern)
            suggestions.append(suggestion)

        return ApiResponse.success(
            message=f"Detected {len(patterns)} potential FK relationships",
            data={
                "dataset_id": dataset_id,
                "db_name": db_name,
                "patterns_found": len(patterns),
                "patterns": [
                    {
                        "source_column": p.source_column,
                        "target_dataset_id": p.target_dataset_id,
                        "target_object_type": p.target_object_type,
                        "target_pk_field": p.target_pk_field,
                        "confidence": p.confidence,
                        "detection_method": p.detection_method,
                        "reasons": p.reasons,
                    }
                    for p in patterns
                ],
                "suggestions": suggestions,
            },
        ).to_dict()

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("detect_relationships failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        )


class TriggerIncrementalRequest(BaseModel):
    execution_mode: str = Field(default="incremental", pattern="^(full|incremental|delta)$")
    watermark_column: Optional[str] = None
    force_full_refresh: bool = Field(default=False)
    max_rows: Optional[int] = None
    batch_size: Optional[int] = None


@router.post(
    "/mapping-specs/{mapping_spec_id}/trigger-incremental",
    summary="Trigger incremental objectify",
    description="Trigger objectify in incremental mode, processing only rows changed since last run.",
)
async def trigger_incremental_objectify(
    mapping_spec_id: str,
    request: Request,
    body: TriggerIncrementalRequest = TriggerIncrementalRequest(),
    branch: str = Query(default="main"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
) -> Dict[str, Any]:
    """Trigger objectify with incremental execution mode."""
    mapping_spec_id = sanitize_input(mapping_spec_id)

    try:
        mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
        if not mapping_spec:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Mapping spec not found: {mapping_spec_id}",
            )

        dataset = await dataset_registry.get_dataset(dataset_id=str(mapping_spec.dataset_id))
        if not dataset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset not found: {mapping_spec.dataset_id}",
            )

        await _require_db_role(request, db_name=dataset.db_name, roles=DATA_ENGINEER_ROLES)

        # Handle watermark
        if body.force_full_refresh:
            await objectify_registry.delete_watermark(
                mapping_spec_id=mapping_spec_id,
                dataset_branch=branch,
            )
            previous_watermark = None
        else:
            watermark = await objectify_registry.get_watermark(
                mapping_spec_id=mapping_spec_id,
                dataset_branch=branch,
            )
            previous_watermark = watermark.get("watermark_value") if watermark else None

        # Get latest dataset version
        version = await dataset_registry.get_latest_version(dataset_id=str(mapping_spec.dataset_id))
        if not version:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No dataset version available",
            )

        # Build job with incremental options
        job_id = str(uuid4())
        options = dict(mapping_spec.options or {})
        options["execution_mode"] = body.execution_mode
        if body.watermark_column:
            options["watermark_column"] = body.watermark_column
        if previous_watermark:
            options["previous_watermark"] = previous_watermark

        dedupe_key = objectify_registry.build_dedupe_key(
            dataset_id=str(mapping_spec.dataset_id),
            dataset_branch=dataset.branch,
            mapping_spec_id=mapping_spec.mapping_spec_id,
            mapping_spec_version=mapping_spec.version,
            dataset_version_id=version.version_id,
        )

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
            max_rows=body.max_rows or options.get("max_rows"),
            batch_size=body.batch_size or options.get("batch_size"),
            options=options,
        )

        await job_queue.publish(job, require_delivery=False)

        return ApiResponse.success(
            message=f"Incremental objectify job queued ({body.execution_mode} mode)",
            data={
                "job_id": job_id,
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
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        )


@router.get(
    "/mapping-specs/{mapping_spec_id}/watermark",
    summary="Get objectify watermark",
    description="Get the current watermark state for a mapping spec.",
)
async def get_mapping_spec_watermark(
    mapping_spec_id: str,
    request: Request,
    branch: str = Query(default="main"),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> Dict[str, Any]:
    """Get watermark state for incremental objectify."""
    mapping_spec_id = sanitize_input(mapping_spec_id)

    try:
        watermark = await objectify_registry.get_watermark(
            mapping_spec_id=mapping_spec_id,
            dataset_branch=branch,
        )

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
            data={
                "mapping_spec_id": mapping_spec_id,
                "branch": branch,
                "watermark": watermark,
            },
        ).to_dict()

    except Exception as exc:
        logger.error("get_mapping_spec_watermark failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        )
