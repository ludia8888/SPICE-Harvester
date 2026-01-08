"""
Link type + relationship spec management.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, ConfigDict, Field

from bff.dependencies import OMSClientDep
from bff.services.oms_client import OMSClient
from bff.routers import objectify as objectify_router
from shared.models.requests import ApiResponse
from shared.security.auth_utils import enforce_db_scope
from shared.security.input_sanitizer import sanitize_input, validate_db_name, validate_branch_name
from shared.services.dataset_registry import DatasetRegistry
from shared.services.objectify_registry import ObjectifyRegistry
from shared.services.pipeline_schema_utils import normalize_schema_type
from shared.utils.import_type_normalization import normalize_import_target_type
from shared.utils.key_spec import normalize_key_spec
from shared.utils.schema_hash import compute_schema_hash
from shared.models.objectify_job import ObjectifyJob
from shared.security.database_access import (
    DATA_ENGINEER_ROLES,
    DOMAIN_MODEL_ROLES,
    enforce_database_role,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databases/{db_name}/ontology", tags=["Ontology Link Types"])


async def get_dataset_registry() -> DatasetRegistry:
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


async def get_objectify_registry() -> ObjectifyRegistry:
    from bff.main import get_objectify_registry as _get_objectify_registry

    return await _get_objectify_registry()


LINK_EDIT_ROLES = DOMAIN_MODEL_ROLES | DATA_ENGINEER_ROLES


async def _require_db_role(request: Request, *, db_name: str, roles) -> None:  # noqa: ANN001
    try:
        await enforce_database_role(headers=request.headers, db_name=db_name, required_roles=roles)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc


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


def _extract_schema_columns(schema: Any) -> List[Dict[str, Any]]:
    if not isinstance(schema, dict):
        return []
    columns = schema.get("columns")
    if isinstance(columns, list):
        output: List[Dict[str, Any]] = []
        for col in columns:
            if isinstance(col, dict):
                name = str(col.get("name") or col.get("column") or "").strip()
                raw_type = col.get("type") or col.get("data_type") or col.get("datatype")
            else:
                name = str(col).strip()
                raw_type = None
            if name:
                output.append({"name": name, "type": raw_type})
        return output
    fields = schema.get("fields")
    if isinstance(fields, list):
        output = []
        for col in fields:
            if not isinstance(col, dict):
                continue
            name = str(col.get("name") or "").strip()
            if name:
                output.append({"name": name, "type": col.get("type")})
        return output
    return []


def _extract_schema_types(schema: Any) -> Dict[str, str]:
    columns = _extract_schema_columns(schema)
    output: Dict[str, str] = {}
    for col in columns:
        name = str(col.get("name") or "").strip()
        raw_type = col.get("type")
        if name:
            output[name] = normalize_schema_type(raw_type)
    return output


def _compute_schema_hash(schema: Any) -> Optional[str]:
    columns = _extract_schema_columns(schema)
    if not columns:
        return None
    return compute_schema_hash(columns)


def _build_join_schema(
    *,
    source_key_column: str,
    target_key_column: str,
    source_key_type: Optional[str],
    target_key_type: Optional[str],
) -> Dict[str, Any]:
    return {
        "columns": [
            {"name": source_key_column, "type": source_key_type or "xsd:string"},
            {"name": target_key_column, "type": target_key_type or "xsd:string"},
        ]
    }


def _extract_ontology_properties(payload: Any) -> Dict[str, Dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        payload = payload["data"]
    if not isinstance(payload, dict):
        return {}
    props = payload.get("properties")
    output: Dict[str, Dict[str, Any]] = {}
    if isinstance(props, list):
        for prop in props:
            if not isinstance(prop, dict):
                continue
            name = str(prop.get("name") or "").strip()
            if name:
                output[name] = prop
    return output


def _extract_ontology_relationships(payload: Any) -> Dict[str, Dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        payload = payload["data"]
    if not isinstance(payload, dict):
        return {}
    rels = payload.get("relationships")
    output: Dict[str, Dict[str, Any]] = {}
    if isinstance(rels, list):
        for rel in rels:
            if not isinstance(rel, dict):
                continue
            predicate = str(rel.get("predicate") or rel.get("name") or "").strip()
            if predicate:
                output[predicate] = rel
    return output


class ForeignKeyRelationshipSpec(BaseModel):
    type: str = Field("foreign_key")
    source_dataset_id: Optional[str] = Field(default=None)
    source_dataset_version_id: Optional[str] = Field(default=None)
    source_pk_fields: Optional[List[str]] = Field(default=None)
    fk_column: str
    target_pk_field: Optional[str] = Field(default=None)
    dangling_policy: str = Field(default="FAIL")
    auto_sync: bool = Field(default=True)


class JoinTableRelationshipSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str = Field("join_table")
    join_dataset_id: Optional[str] = Field(default=None)
    join_dataset_version_id: Optional[str] = Field(default=None)
    join_dataset_name: Optional[str] = Field(default=None)
    join_dataset_branch: Optional[str] = Field(default=None)
    auto_create: bool = Field(default=False, alias="autoCreate")
    source_key_column: str
    target_key_column: str
    dedupe_policy: str = Field(default="DEDUP")
    dangling_policy: str = Field(default="FAIL")
    auto_sync: bool = Field(default=True)


class ObjectBackedRelationshipSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str = Field("object_backed")
    relationship_object_type: str
    relationship_dataset_id: Optional[str] = Field(default=None)
    relationship_dataset_version_id: Optional[str] = Field(default=None)
    source_key_column: str
    target_key_column: str
    dedupe_policy: str = Field(default="DEDUP")
    dangling_policy: str = Field(default="FAIL")
    auto_sync: bool = Field(default=True)


class LinkTypeRequest(BaseModel):
    id: str = Field(..., description="Link type id")
    label: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    from_ref: str = Field(..., alias="from")
    to_ref: str = Field(..., alias="to")
    predicate: str
    cardinality: str
    status: str = Field(default="ACTIVE")
    metadata: Dict[str, Any] = Field(default_factory=dict)
    relationship_spec: Dict[str, Any] = Field(default_factory=dict)
    trigger_index: bool = Field(default=True)


class LinkTypeUpdateRequest(BaseModel):
    label: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    relationship_spec: Optional[Dict[str, Any]] = None
    trigger_index: bool = Field(default=True)


class LinkEditRequest(BaseModel):
    source_instance_id: str
    target_instance_id: str
    edit_type: str = Field(..., description="ADD or REMOVE")
    branch: Optional[str] = Field(default="main")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)


def _normalize_spec_type(value: str) -> str:
    return str(value or "").strip().lower()


def _normalize_policy(value: Optional[str], *, default: str) -> str:
    raw = str(value or default).strip().upper() or default
    if raw not in {"FAIL", "WARN", "DEDUP"}:
        return default
    return raw


def _normalize_pk_fields(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    return []


async def _resolve_object_type_contract(
    *,
    oms_client: OMSClient,
    db_name: str,
    class_id: str,
    branch: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    resource_payload = await oms_client.get_ontology_resource(
        db_name,
        resource_type="object_type",
        resource_id=class_id,
        branch=branch,
    )
    resource = resource_payload.get("data") if isinstance(resource_payload, dict) else None
    if not isinstance(resource, dict):
        resource = resource_payload if isinstance(resource_payload, dict) else {}
    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    return resource, spec


async def _resolve_dataset_and_version(
    *,
    dataset_registry: DatasetRegistry,
    dataset_id: str,
    dataset_version_id: Optional[str],
) -> Tuple[Any, Any, str]:
    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    version = None
    if dataset_version_id:
        version = await dataset_registry.get_version(version_id=dataset_version_id)
        if not version or version.dataset_id != dataset.dataset_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset version not found")
    if not version:
        version = await dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
    if not version:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Dataset version is required")
    schema_hash = _compute_schema_hash(version.sample_json or dataset.schema_json)
    if not schema_hash:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="schema_hash is required for relationship spec")
    return dataset, version, schema_hash


async def _ensure_join_dataset(
    *,
    dataset_registry: DatasetRegistry,
    request: Request,
    db_name: str,
    join_dataset_id: Optional[str],
    join_dataset_version_id: Optional[str],
    join_dataset_name: Optional[str],
    join_dataset_branch: Optional[str],
    auto_create: bool,
    default_name: str,
    source_key_column: str,
    target_key_column: str,
    source_key_type: Optional[str],
    target_key_type: Optional[str],
) -> Tuple[Any, Any, str]:
    dataset = None
    version = None
    dataset_branch = (join_dataset_branch or "main").strip() or "main"
    if join_dataset_id:
        dataset = await dataset_registry.get_dataset(dataset_id=join_dataset_id)
        if not dataset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Join dataset not found")
    else:
        if not auto_create:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="join_dataset_id is required")
        name = (join_dataset_name or default_name or "").strip()
        if not name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="join_dataset_name is required")
        dataset = await dataset_registry.get_dataset_by_name(db_name=db_name, name=name, branch=dataset_branch)
        if not dataset:
            schema_json = _build_join_schema(
                source_key_column=source_key_column,
                target_key_column=target_key_column,
                source_key_type=source_key_type,
                target_key_type=target_key_type,
            )
            dataset = await dataset_registry.create_dataset(
                db_name=db_name,
                name=name,
                description=f"Auto-created join table for {default_name}",
                source_type="join_table",
                source_ref=None,
                schema_json=schema_json,
                branch=dataset_branch,
            )

    enforce_db_scope(request.headers, db_name=dataset.db_name)
    if dataset.db_name != db_name:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Join dataset does not belong to requested database",
        )

    if join_dataset_version_id:
        version = await dataset_registry.get_version(version_id=join_dataset_version_id)
        if not version or version.dataset_id != dataset.dataset_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Join dataset version not found")
    if not version:
        version = await dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
    if not version and auto_create:
        schema_json = _build_join_schema(
            source_key_column=source_key_column,
            target_key_column=target_key_column,
            source_key_type=source_key_type,
            target_key_type=target_key_type,
        )
        version = await dataset_registry.add_version(
            dataset_id=dataset.dataset_id,
            lakefs_commit_id=f"auto_join_{uuid4().hex}",
            artifact_key=None,
            row_count=0,
            sample_json=schema_json,
            schema_json=schema_json,
        )
    if not version:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Join dataset version is required")

    schema_hash = _compute_schema_hash(version.sample_json or dataset.schema_json)
    if not schema_hash:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="schema_hash is required for relationship spec")
    return dataset, version, schema_hash


def _resolve_property_type(prop_map: Dict[str, Dict[str, Any]], field: str) -> Optional[str]:
    meta = prop_map.get(field)
    if not meta:
        return None
    raw_type = meta.get("type") or meta.get("data_type") or meta.get("datatype")
    return normalize_import_target_type(raw_type)


async def _build_mapping_request(
    *,
    db_name: str,
    request: Request,
    oms_client: OMSClient,
    dataset_registry: DatasetRegistry,
    relationship_spec_id: str,
    link_type_id: str,
    source_class: str,
    target_class: str,
    predicate: str,
    cardinality: str,
    branch: str,
    source_props: Dict[str, Dict[str, Any]],
    target_props: Dict[str, Dict[str, Any]],
    source_contract: Dict[str, Any],
    target_contract: Dict[str, Any],
    spec_payload: Dict[str, Any],
) -> Tuple[objectify_router.CreateMappingSpecRequest, str, Optional[str], str]:
    spec_type = _normalize_spec_type(spec_payload.get("type") or "")
    normalized_type = "join_table" if spec_type == "object_backed" else spec_type
    if normalized_type not in {"foreign_key", "join_table"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="relationship_spec.type is required")

    source_pk = normalize_key_spec(source_contract.get("pk_spec") or {}, columns=list(source_props.keys()))
    target_pk = normalize_key_spec(target_contract.get("pk_spec") or {}, columns=list(target_props.keys()))
    source_pk_fields = [str(v).strip() for v in source_pk.get("primary_key") or [] if str(v).strip()]
    target_pk_fields = [str(v).strip() for v in target_pk.get("primary_key") or [] if str(v).strip()]
    if not source_pk_fields or not target_pk_fields:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "OBJECT_TYPE_PRIMARY_KEY_MISSING", "source": source_class, "target": target_class},
        )

    if normalized_type == "foreign_key":
        fk_spec = ForeignKeyRelationshipSpec(**spec_payload)
        fk_column = str(fk_spec.fk_column or "").strip()
        if not fk_column:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="fk_column is required")

        backing_source = source_contract.get("backing_source") if isinstance(source_contract.get("backing_source"), dict) else {}
        backing_id = str(backing_source.get("ref") or "").strip() or None
        dataset_id = fk_spec.source_dataset_id or None
        dataset_version_id = fk_spec.source_dataset_version_id or None
        if not dataset_id:
            if not backing_id:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="source backing datasource missing")
            backing = await dataset_registry.get_backing_datasource(backing_id=backing_id)
            if not backing:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backing datasource not found")
            dataset_id = backing.dataset_id

        dataset, version, schema_hash = await _resolve_dataset_and_version(
            dataset_registry=dataset_registry,
            dataset_id=dataset_id,
            dataset_version_id=dataset_version_id,
        )
        enforce_db_scope(request.headers, db_name=dataset.db_name)

        schema_types = _extract_schema_types(version.sample_json or dataset.schema_json)
        if fk_column not in schema_types:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="fk_column not found in dataset")

        target_pk_field = str(fk_spec.target_pk_field or (target_pk_fields[0] if len(target_pk_fields) == 1 else "")).strip()
        if not target_pk_field:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="target_pk_field is required")
        if target_pk_field not in target_props:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="target_pk_field missing from ontology")

        source_pk_fields = _normalize_pk_fields(fk_spec.source_pk_fields) or source_pk_fields
        for field in source_pk_fields:
            if field not in schema_types:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"source pk column missing: {field}")

        fk_type = schema_types.get(fk_column)
        target_pk_type = _resolve_property_type(target_props, target_pk_field)
        if not fk_type or not target_pk_type or not _is_type_compatible(fk_type, target_pk_type):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "RELATIONSHIP_FK_TYPE_MISMATCH",
                    "fk_type": fk_type,
                    "target_pk_type": target_pk_type,
                },
            )

        for field in source_pk_fields:
            source_pk_type = schema_types.get(field)
            expected = _resolve_property_type(source_props, field) or target_pk_type
            if source_pk_type and expected and not _is_type_compatible(source_pk_type, expected):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "RELATIONSHIP_SOURCE_PK_TYPE_MISMATCH",
                        "field": field,
                        "observed": source_pk_type,
                        "expected": expected,
                    },
                )

        mappings = [
            {"source_field": field, "target_field": field}
            for field in source_pk_fields
        ]
        mappings.append({"source_field": fk_column, "target_field": predicate})

        backing = await dataset_registry.get_or_create_backing_datasource(
            dataset=dataset,
            source_type=dataset.source_type,
            source_ref=dataset.source_ref,
        )
        backing_version = await dataset_registry.get_or_create_backing_datasource_version(
            backing_id=backing.backing_id,
            dataset_version_id=version.version_id,
            schema_hash=schema_hash,
            metadata={"artifact_key": version.artifact_key},
        )

        options = {
            "mode": "link_index",
            "relationship_spec_id": relationship_spec_id,
            "link_type_id": link_type_id,
            "relationship_kind": "foreign_key",
            "dangling_policy": _normalize_policy(fk_spec.dangling_policy, default="FAIL"),
            "relationship_meta": {
                predicate: {"target": target_class, "cardinality": cardinality},
            },
        }

        mapping_request = objectify_router.CreateMappingSpecRequest(
            dataset_id=dataset.dataset_id,
            dataset_branch=dataset.branch,
            artifact_output_name=dataset.name,
            schema_hash=schema_hash,
            backing_datasource_id=backing.backing_id,
            backing_datasource_version_id=backing_version.version_id,
            target_class_id=source_class,
            mappings=[objectify_router.MappingSpecField(**m) for m in mappings],
            status="ACTIVE",
            auto_sync=fk_spec.auto_sync,
            options=options,
        )
        return mapping_request, dataset.dataset_id, dataset_version_id, spec_type

    object_backed_spec: Optional[ObjectBackedRelationshipSpec] = None
    relationship_object_type = None
    join_spec = JoinTableRelationshipSpec(**spec_payload)
    if spec_type == "object_backed":
        object_backed_spec = ObjectBackedRelationshipSpec(**spec_payload)
        relationship_object_type = str(object_backed_spec.relationship_object_type or "").strip()
        if not relationship_object_type:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="relationship_object_type is required for object_backed",
            )
        _, relationship_contract = await _resolve_object_type_contract(
            oms_client=oms_client,
            db_name=db_name,
            class_id=relationship_object_type,
            branch=branch,
        )
        backing_source = (
            relationship_contract.get("backing_source")
            if isinstance(relationship_contract.get("backing_source"), dict)
            else {}
        )
        join_spec = JoinTableRelationshipSpec(
            type="join_table",
            join_dataset_id=object_backed_spec.relationship_dataset_id
            or str(backing_source.get("dataset_id") or "").strip()
            or None,
            join_dataset_version_id=object_backed_spec.relationship_dataset_version_id
            or str(backing_source.get("dataset_version_id") or "").strip()
            or None,
            source_key_column=object_backed_spec.source_key_column,
            target_key_column=object_backed_spec.target_key_column,
            dedupe_policy=object_backed_spec.dedupe_policy,
            dangling_policy=object_backed_spec.dangling_policy,
            auto_sync=object_backed_spec.auto_sync,
        )

    source_pk_field = source_pk_fields[0] if len(source_pk_fields) == 1 else None
    target_pk_field = target_pk_fields[0] if len(target_pk_fields) == 1 else None
    if not source_pk_field or not target_pk_field:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="composite pk requires explicit mapping")

    source_pk_type = _resolve_property_type(source_props, source_pk_field)
    target_pk_type = _resolve_property_type(target_props, target_pk_field)

    dataset, version, schema_hash = await _ensure_join_dataset(
        dataset_registry=dataset_registry,
        request=request,
        db_name=db_name,
        join_dataset_id=str(join_spec.join_dataset_id or "").strip() or None,
        join_dataset_version_id=join_spec.join_dataset_version_id,
        join_dataset_name=join_spec.join_dataset_name,
        join_dataset_branch=join_spec.join_dataset_branch,
        auto_create=bool(join_spec.auto_create),
        default_name=f"{source_class}_{predicate}_{target_class}_join",
        source_key_column=str(join_spec.source_key_column or "").strip(),
        target_key_column=str(join_spec.target_key_column or "").strip(),
        source_key_type=source_pk_type,
        target_key_type=target_pk_type,
    )

    schema_types = _extract_schema_types(version.sample_json or dataset.schema_json)
    source_key_column = str(join_spec.source_key_column or "").strip()
    target_key_column = str(join_spec.target_key_column or "").strip()
    if source_key_column not in schema_types:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="source_key_column missing")
    if target_key_column not in schema_types:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="target_key_column missing")

    source_type = schema_types.get(source_key_column)
    target_type = schema_types.get(target_key_column)
    if not source_type or not source_pk_type or not _is_type_compatible(source_type, source_pk_type):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "RELATIONSHIP_JOIN_SOURCE_TYPE_MISMATCH", "observed": source_type, "expected": source_pk_type},
        )
    if not target_type or not target_pk_type or not _is_type_compatible(target_type, target_pk_type):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "RELATIONSHIP_JOIN_TARGET_TYPE_MISMATCH", "observed": target_type, "expected": target_pk_type},
        )

    mappings = [
        {"source_field": source_key_column, "target_field": source_pk_field},
        {"source_field": target_key_column, "target_field": predicate},
    ]

    backing = await dataset_registry.get_or_create_backing_datasource(
        dataset=dataset,
        source_type=dataset.source_type,
        source_ref=dataset.source_ref,
    )
    backing_version = await dataset_registry.get_or_create_backing_datasource_version(
        backing_id=backing.backing_id,
        dataset_version_id=version.version_id,
        schema_hash=schema_hash,
        metadata={"artifact_key": version.artifact_key},
    )

    options = {
        "mode": "link_index",
        "relationship_spec_id": relationship_spec_id,
        "link_type_id": link_type_id,
        "relationship_kind": "object_backed" if spec_type == "object_backed" else "join_table",
        "dedupe_policy": _normalize_policy(join_spec.dedupe_policy, default="DEDUP"),
        "dangling_policy": _normalize_policy(join_spec.dangling_policy, default="FAIL"),
        "full_sync": True,
        "relationship_meta": {
            predicate: {"target": target_class, "cardinality": cardinality},
        },
    }
    if relationship_object_type:
        options["relationship_object_type"] = relationship_object_type

    mapping_request = objectify_router.CreateMappingSpecRequest(
        dataset_id=dataset.dataset_id,
        dataset_branch=dataset.branch,
        artifact_output_name=dataset.name,
        schema_hash=schema_hash,
        backing_datasource_id=backing.backing_id,
        backing_datasource_version_id=backing_version.version_id,
        target_class_id=source_class,
        mappings=[objectify_router.MappingSpecField(**m) for m in mappings],
        status="ACTIVE",
        auto_sync=join_spec.auto_sync,
        options=options,
    )
    return mapping_request, dataset.dataset_id, version.version_id if version else None, spec_type


@router.post("/link-types", status_code=status.HTTP_201_CREATED, response_model=ApiResponse)
async def create_link_type(
    db_name: str,
    body: LinkTypeRequest,
    request: Request,
    branch: str = Query("main", description="Target branch"),
    expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
    oms_client: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        await _require_db_role(request, db_name=db_name, roles=DOMAIN_MODEL_ROLES)
        payload = sanitize_input(body.model_dump(by_alias=True))

        link_type_id = str(payload.get("id") or "").strip()
        if not link_type_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="id is required")

        source_class = str(payload.get("from") or "").strip()
        target_class = str(payload.get("to") or "").strip()
        if not source_class or not target_class:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="from/to are required")

        predicate = str(payload.get("predicate") or "").strip()
        if not predicate:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="predicate is required")

        cardinality = str(payload.get("cardinality") or "").strip()
        if not cardinality:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="cardinality is required")

        spec_payload = payload.get("relationship_spec") if isinstance(payload.get("relationship_spec"), dict) else {}
        spec_type = _normalize_spec_type(spec_payload.get("type") or "")
        normalized_type = "join_table" if spec_type == "object_backed" else spec_type
        if normalized_type not in {"foreign_key", "join_table"}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="relationship_spec.type is required")

        source_ontology = await oms_client.get_ontology(db_name, source_class, branch=branch)
        target_ontology = await oms_client.get_ontology(db_name, target_class, branch=branch)
        if not source_ontology or not target_ontology:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ontology class not found")

        source_props = _extract_ontology_properties(source_ontology)
        source_rels = _extract_ontology_relationships(source_ontology)
        target_props = _extract_ontology_properties(target_ontology)

        if predicate not in source_rels:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"code": "LINK_TYPE_PREDICATE_MISSING", "predicate": predicate},
            )

        source_resource, source_contract = await _resolve_object_type_contract(
            oms_client=oms_client,
            db_name=db_name,
            class_id=source_class,
            branch=branch,
        )
        target_resource, target_contract = await _resolve_object_type_contract(
            oms_client=oms_client,
            db_name=db_name,
            class_id=target_class,
            branch=branch,
        )

        source_pk = normalize_key_spec(source_contract.get("pk_spec") or {}, columns=list(source_props.keys()))
        target_pk = normalize_key_spec(target_contract.get("pk_spec") or {}, columns=list(target_props.keys()))
        source_pk_fields = [str(v).strip() for v in source_pk.get("primary_key") or [] if str(v).strip()]
        target_pk_fields = [str(v).strip() for v in target_pk.get("primary_key") or [] if str(v).strip()]
        if not source_pk_fields or not target_pk_fields:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"code": "OBJECT_TYPE_PRIMARY_KEY_MISSING", "source": source_class, "target": target_class},
            )

        relationship_spec_id = str(uuid4())
        relationship_object_type = None

        resolved_dataset_version_id = None
        if normalized_type == "foreign_key":
            fk_spec = ForeignKeyRelationshipSpec(**spec_payload)
            fk_column = str(fk_spec.fk_column or "").strip()
            if not fk_column:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="fk_column is required")

            backing_source = source_contract.get("backing_source") if isinstance(source_contract.get("backing_source"), dict) else {}
            backing_id = str(backing_source.get("ref") or "").strip() or None
            dataset_id = fk_spec.source_dataset_id or None
            dataset_version_id = fk_spec.source_dataset_version_id or None
            if not dataset_id:
                if not backing_id:
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="source backing datasource missing")
                backing = await dataset_registry.get_backing_datasource(backing_id=backing_id)
                if not backing:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backing datasource not found")
                dataset_id = backing.dataset_id

            dataset, version, schema_hash = await _resolve_dataset_and_version(
                dataset_registry=dataset_registry,
                dataset_id=dataset_id,
                dataset_version_id=dataset_version_id,
            )
            enforce_db_scope(request.headers, db_name=dataset.db_name)
            resolved_dataset_version_id = version.version_id

            schema_types = _extract_schema_types(version.sample_json or dataset.schema_json)
            if fk_column not in schema_types:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="fk_column not found in dataset")

            target_pk_field = str(fk_spec.target_pk_field or (target_pk_fields[0] if len(target_pk_fields) == 1 else "")).strip()
            if not target_pk_field:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="target_pk_field is required")
            if target_pk_field not in target_props:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="target_pk_field missing from ontology")

            source_pk_fields = _normalize_pk_fields(fk_spec.source_pk_fields) or source_pk_fields
            for field in source_pk_fields:
                if field not in schema_types:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"source pk column missing: {field}")

            fk_type = schema_types.get(fk_column)
            target_pk_type = _resolve_property_type(target_props, target_pk_field)
            if not fk_type or not target_pk_type or not _is_type_compatible(fk_type, target_pk_type):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "RELATIONSHIP_FK_TYPE_MISMATCH",
                        "fk_type": fk_type,
                        "target_pk_type": target_pk_type,
                    },
                )

            for field in source_pk_fields:
                source_pk_type = schema_types.get(field)
                expected = _resolve_property_type(source_props, field) or target_pk_type
                if source_pk_type and expected and not _is_type_compatible(source_pk_type, expected):
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "code": "RELATIONSHIP_SOURCE_PK_TYPE_MISMATCH",
                            "field": field,
                            "observed": source_pk_type,
                            "expected": expected,
                        },
                    )

            mappings = [
                {"source_field": field, "target_field": field}
                for field in source_pk_fields
            ]
            mappings.append({"source_field": fk_column, "target_field": predicate})

            backing = await dataset_registry.get_or_create_backing_datasource(
                dataset=dataset,
                source_type=dataset.source_type,
                source_ref=dataset.source_ref,
            )
            backing_version = await dataset_registry.get_or_create_backing_datasource_version(
                backing_id=backing.backing_id,
                dataset_version_id=version.version_id,
                schema_hash=schema_hash,
                metadata={"artifact_key": version.artifact_key},
            )

            options = {
                "mode": "link_index",
                "relationship_spec_id": relationship_spec_id,
                "link_type_id": link_type_id,
                "relationship_kind": "foreign_key",
                "dangling_policy": _normalize_policy(fk_spec.dangling_policy, default="FAIL"),
                "relationship_meta": {
                    predicate: {"target": target_class, "cardinality": cardinality},
                },
            }

            mapping_request = objectify_router.CreateMappingSpecRequest(
                dataset_id=dataset.dataset_id,
                dataset_branch=dataset.branch,
                artifact_output_name=dataset.name,
                schema_hash=schema_hash,
                backing_datasource_id=backing.backing_id,
                backing_datasource_version_id=backing_version.version_id,
                target_class_id=source_class,
                mappings=[objectify_router.MappingSpecField(**m) for m in mappings],
                status="ACTIVE",
                auto_sync=fk_spec.auto_sync,
                options=options,
            )
        else:
            object_backed_spec: Optional[ObjectBackedRelationshipSpec] = None
            relationship_object_type = None
            join_spec = JoinTableRelationshipSpec(**spec_payload)
            if spec_type == "object_backed":
                object_backed_spec = ObjectBackedRelationshipSpec(**spec_payload)
                relationship_object_type = str(object_backed_spec.relationship_object_type or "").strip()
                if not relationship_object_type:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="relationship_object_type is required for object_backed",
                    )
                _, relationship_contract = await _resolve_object_type_contract(
                    oms_client=oms_client,
                    db_name=db_name,
                    class_id=relationship_object_type,
                    branch=branch,
                )
                backing_source = (
                    relationship_contract.get("backing_source")
                    if isinstance(relationship_contract.get("backing_source"), dict)
                    else {}
                )
                join_spec = JoinTableRelationshipSpec(
                    type="join_table",
                    join_dataset_id=object_backed_spec.relationship_dataset_id
                    or str(backing_source.get("dataset_id") or "").strip()
                    or None,
                    join_dataset_version_id=object_backed_spec.relationship_dataset_version_id
                    or str(backing_source.get("dataset_version_id") or "").strip()
                    or None,
                    source_key_column=object_backed_spec.source_key_column,
                    target_key_column=object_backed_spec.target_key_column,
                    dedupe_policy=object_backed_spec.dedupe_policy,
                    dangling_policy=object_backed_spec.dangling_policy,
                    auto_sync=object_backed_spec.auto_sync,
                )

            source_pk_field = source_pk_fields[0] if len(source_pk_fields) == 1 else None
            target_pk_field = target_pk_fields[0] if len(target_pk_fields) == 1 else None
            if not source_pk_field or not target_pk_field:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="composite pk requires explicit mapping")

            source_pk_type = _resolve_property_type(source_props, source_pk_field)
            target_pk_type = _resolve_property_type(target_props, target_pk_field)

            dataset, version, schema_hash = await _ensure_join_dataset(
                dataset_registry=dataset_registry,
                request=request,
                db_name=db_name,
                join_dataset_id=str(join_spec.join_dataset_id or "").strip() or None,
                join_dataset_version_id=join_spec.join_dataset_version_id,
                join_dataset_name=join_spec.join_dataset_name,
                join_dataset_branch=join_spec.join_dataset_branch,
                auto_create=bool(join_spec.auto_create),
                default_name=f"{source_class}_{predicate}_{target_class}_join",
                source_key_column=str(join_spec.source_key_column or "").strip(),
                target_key_column=str(join_spec.target_key_column or "").strip(),
                source_key_type=source_pk_type,
                target_key_type=target_pk_type,
            )
            resolved_dataset_version_id = version.version_id

            schema_types = _extract_schema_types(version.sample_json or dataset.schema_json)
            source_key_column = str(join_spec.source_key_column or "").strip()
            target_key_column = str(join_spec.target_key_column or "").strip()
            if source_key_column not in schema_types:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="source_key_column missing")
            if target_key_column not in schema_types:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="target_key_column missing")

            source_type = schema_types.get(source_key_column)
            target_type = schema_types.get(target_key_column)
            if not source_type or not source_pk_type or not _is_type_compatible(source_type, source_pk_type):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={"code": "RELATIONSHIP_JOIN_SOURCE_TYPE_MISMATCH", "observed": source_type, "expected": source_pk_type},
                )
            if not target_type or not target_pk_type or not _is_type_compatible(target_type, target_pk_type):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={"code": "RELATIONSHIP_JOIN_TARGET_TYPE_MISMATCH", "observed": target_type, "expected": target_pk_type},
                )

            mappings = [
                {"source_field": source_key_column, "target_field": source_pk_field},
                {"source_field": target_key_column, "target_field": predicate},
            ]

            backing = await dataset_registry.get_or_create_backing_datasource(
                dataset=dataset,
                source_type=dataset.source_type,
                source_ref=dataset.source_ref,
            )
            backing_version = await dataset_registry.get_or_create_backing_datasource_version(
                backing_id=backing.backing_id,
                dataset_version_id=version.version_id,
                schema_hash=schema_hash,
                metadata={"artifact_key": version.artifact_key},
            )

            options = {
                "mode": "link_index",
                "relationship_spec_id": relationship_spec_id,
                "link_type_id": link_type_id,
                "relationship_kind": "object_backed" if spec_type == "object_backed" else "join_table",
                "dedupe_policy": _normalize_policy(join_spec.dedupe_policy, default="DEDUP"),
                "dangling_policy": _normalize_policy(join_spec.dangling_policy, default="FAIL"),
                "full_sync": True,
                "relationship_meta": {
                    predicate: {"target": target_class, "cardinality": cardinality},
                },
            }
            if relationship_object_type:
                options["relationship_object_type"] = relationship_object_type

            mapping_request = objectify_router.CreateMappingSpecRequest(
                dataset_id=dataset.dataset_id,
                dataset_branch=dataset.branch,
                artifact_output_name=dataset.name,
                schema_hash=schema_hash,
                backing_datasource_id=backing.backing_id,
                backing_datasource_version_id=backing_version.version_id,
                target_class_id=source_class,
                mappings=[objectify_router.MappingSpecField(**m) for m in mappings],
                status="ACTIVE",
                auto_sync=join_spec.auto_sync,
                options=options,
            )

        mapping_response = await objectify_router.create_mapping_spec(
            body=mapping_request,
            request=request,
            dataset_registry=dataset_registry,
            objectify_registry=objectify_registry,
            oms_client=oms_client,
        )
        mapping_payload = mapping_response.get("data", {}).get("mapping_spec")
        if not isinstance(mapping_payload, dict):
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create mapping spec")

        mapping_spec_id = str(mapping_payload.get("mapping_spec_id") or "").strip()
        mapping_spec_version = int(mapping_payload.get("version") or mapping_payload.get("mapping_spec_version") or 1)

        resolved_spec_payload = dict(spec_payload)
        if normalized_type == "join_table":
            resolved_spec_payload.setdefault("join_dataset_id", mapping_request.dataset_id)
            if resolved_dataset_version_id:
                resolved_spec_payload.setdefault("join_dataset_version_id", resolved_dataset_version_id)
        if spec_type == "object_backed" and relationship_object_type:
            resolved_spec_payload.setdefault("relationship_object_type", relationship_object_type)

        relationship_spec_summary = {
            **resolved_spec_payload,
            "relationship_spec_id": relationship_spec_id,
            "spec_type": spec_type,
            "dataset_id": mapping_request.dataset_id,
            "dataset_version_id": resolved_dataset_version_id,
            "mapping_spec_id": mapping_spec_id,
            "mapping_spec_version": mapping_spec_version,
        }

        link_payload = {
            "id": link_type_id,
            "label": payload.get("label") or link_type_id,
            "description": payload.get("description"),
            "metadata": payload.get("metadata") or {},
            "from": source_class,
            "to": target_class,
            "predicate": predicate,
            "cardinality": cardinality,
            "spec": {
                "relationship_spec_id": relationship_spec_id,
                "relationship_spec_type": spec_type,
                "mapping_spec_id": mapping_spec_id,
                "mapping_spec_version": mapping_spec_version,
                "relationship_spec": relationship_spec_summary,
                "status": payload.get("status") or "ACTIVE",
            },
        }

        link_response = await oms_client.create_ontology_resource(
            db_name,
            resource_type="link_type",
            payload=link_payload,
            branch=branch,
            expected_head_commit=expected_head_commit,
        )
        link_resource = link_response.get("data") if isinstance(link_response, dict) else link_response

        relationship_record = await dataset_registry.create_relationship_spec(
            relationship_spec_id=relationship_spec_id,
            link_type_id=link_type_id,
            db_name=db_name,
            source_object_type=source_class,
            target_object_type=target_class,
            predicate=predicate,
            spec_type=spec_type,
            dataset_id=mapping_request.dataset_id,
            dataset_version_id=resolved_dataset_version_id,
            mapping_spec_id=mapping_spec_id,
            mapping_spec_version=mapping_spec_version,
            spec=resolved_spec_payload,
            status=str(payload.get("status") or "ACTIVE").upper(),
            auto_sync=bool(mapping_request.auto_sync),
        )

        if payload.get("trigger_index"):
            await _enqueue_link_index_job(
                dataset_registry=dataset_registry,
                objectify_registry=objectify_registry,
                mapping_spec_id=mapping_spec_id,
                mapping_spec_version=mapping_spec_version,
                dataset_id=mapping_request.dataset_id,
                dataset_version_id=None,
            )

        return ApiResponse.success(
            message="Link type created",
            data={
                "link_type": link_resource,
                "relationship_spec": relationship_record.__dict__,
                "mapping_spec": mapping_payload,
            },
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to create link type: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


async def _enqueue_link_index_job(
    *,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    mapping_spec_id: str,
    mapping_spec_version: int,
    dataset_id: str,
    dataset_version_id: Optional[str],
) -> Optional[str]:
    if not objectify_registry:
        return None
    mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
    if not mapping_spec:
        return None
    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return None
    version = None
    if dataset_version_id:
        version = await dataset_registry.get_version(version_id=dataset_version_id)
        if not version or version.dataset_id != dataset.dataset_id:
            return None
    if not version:
        version = await dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
    if not version or not version.artifact_key:
        return None

    dedupe_key = objectify_registry.build_dedupe_key(
        dataset_id=dataset.dataset_id,
        dataset_branch=dataset.branch,
        mapping_spec_id=mapping_spec.mapping_spec_id,
        mapping_spec_version=mapping_spec_version,
        dataset_version_id=version.version_id,
        artifact_id=None,
        artifact_output_name=dataset.name,
    )
    existing = await objectify_registry.get_objectify_job_by_dedupe_key(dedupe_key=dedupe_key)
    if existing:
        return existing.job_id

    job_id = str(uuid4())
    options = dict(mapping_spec.options or {})
    job = ObjectifyJob(
        job_id=job_id,
        db_name=dataset.db_name,
        dataset_id=dataset.dataset_id,
        dataset_version_id=version.version_id,
        artifact_output_name=dataset.name,
        dedupe_key=dedupe_key,
        dataset_branch=dataset.branch,
        artifact_key=version.artifact_key or "",
        mapping_spec_id=mapping_spec.mapping_spec_id,
        mapping_spec_version=mapping_spec_version,
        target_class_id=mapping_spec.target_class_id,
        ontology_branch=options.get("ontology_branch"),
        max_rows=options.get("max_rows"),
        batch_size=options.get("batch_size"),
        allow_partial=bool(options.get("allow_partial")),
        options=options,
    )
    await objectify_registry.enqueue_objectify_job(job=job)
    return job_id


@router.get("/link-types", response_model=ApiResponse)
async def list_link_types(
    db_name: str,
    branch: str = Query("main", description="Target branch"),
    oms_client: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        resources = await oms_client.list_ontology_resources(db_name, resource_type="link_type", branch=branch)
        items = resources.get("resources") if isinstance(resources, dict) else None
        if not isinstance(items, list):
            items = []

        enriched = []
        for entry in items:
            if not isinstance(entry, dict):
                continue
            link_id = str(entry.get("id") or "").strip()
            relationship_spec = None
            if link_id:
                record = await dataset_registry.get_relationship_spec(link_type_id=link_id)
                if record:
                    relationship_spec = record.__dict__
            enriched.append({"link_type": entry, "relationship_spec": relationship_spec})

        return ApiResponse.success(
            message="Link types retrieved",
            data={"link_types": enriched, "total": len(enriched)},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list link types: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/link-types/{link_type_id}", response_model=ApiResponse)
async def get_link_type(
    db_name: str,
    link_type_id: str,
    request: Request,
    branch: str = Query("main", description="Target branch"),
    oms_client: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        await _require_db_role(request, db_name=db_name, roles=LINK_EDIT_ROLES)
        link_type_id = str(link_type_id or "").strip()
        if not link_type_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="link_type_id is required")

        try:
            payload = await oms_client.get_ontology_resource(
                db_name,
                resource_type="link_type",
                resource_id=link_type_id,
                branch=branch,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == status.HTTP_404_NOT_FOUND:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Link type not found") from exc
            raise
        resource = payload.get("data") if isinstance(payload, dict) else payload

        relationship_spec = await dataset_registry.get_relationship_spec(link_type_id=link_type_id)
        return ApiResponse.success(
            message="Link type retrieved",
            data={
                "link_type": resource,
                "relationship_spec": relationship_spec.__dict__ if relationship_spec else None,
            },
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get link type: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/link-types/{link_type_id}/edits", response_model=ApiResponse)
async def list_link_edits(
    db_name: str,
    link_type_id: str,
    branch: str = Query("main", description="Target branch"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        link_type_id = str(link_type_id or "").strip()
        if not link_type_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="link_type_id is required")
        branch = validate_branch_name(branch)

        relationship_spec = await dataset_registry.get_relationship_spec(link_type_id=link_type_id)
        if not relationship_spec:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Relationship spec not found")
        if relationship_spec.db_name != db_name:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Link type does not belong to requested database")

        edits = await dataset_registry.list_link_edits(
            db_name=db_name,
            link_type_id=link_type_id,
            branch=branch,
            status="ACTIVE",
        )
        return ApiResponse.success(
            message="Link edits retrieved",
            data={"link_edits": [e.__dict__ for e in edits]},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list link edits: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.post("/link-types/{link_type_id}/edits", response_model=ApiResponse)
async def create_link_edit(
    db_name: str,
    link_type_id: str,
    body: LinkEditRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        link_type_id = str(link_type_id or "").strip()
        if not link_type_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="link_type_id is required")

        relationship_spec = await dataset_registry.get_relationship_spec(link_type_id=link_type_id)
        if not relationship_spec:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Relationship spec not found")
        if relationship_spec.db_name != db_name:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Link type does not belong to requested database")

        spec = relationship_spec.spec or {}
        edits_enabled = bool(spec.get("edits_enabled") or spec.get("editsEnabled"))
        if not edits_enabled:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"code": "LINK_EDITS_DISABLED", "link_type_id": link_type_id},
            )

        payload = sanitize_input(body.model_dump())
        branch = validate_branch_name(payload.get("branch") or "main")
        edit_type = str(payload.get("edit_type") or "").strip().upper()
        if edit_type not in {"ADD", "REMOVE"}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="edit_type must be ADD or REMOVE")

        source_instance_id = str(payload.get("source_instance_id") or "").strip()
        target_instance_id = str(payload.get("target_instance_id") or "").strip()
        if not source_instance_id or not target_instance_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="source_instance_id and target_instance_id are required",
            )

        record = await dataset_registry.record_link_edit(
            db_name=db_name,
            link_type_id=link_type_id,
            branch=branch,
            source_object_type=relationship_spec.source_object_type,
            target_object_type=relationship_spec.target_object_type,
            predicate=relationship_spec.predicate,
            source_instance_id=source_instance_id,
            target_instance_id=target_instance_id,
            edit_type=edit_type,
            status="ACTIVE",
            metadata=payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
        )
        return ApiResponse.success(message="Link edit recorded", data={"link_edit": record.__dict__})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to record link edit: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.put("/link-types/{link_type_id}", response_model=ApiResponse)
async def update_link_type(
    db_name: str,
    link_type_id: str,
    body: LinkTypeUpdateRequest,
    request: Request,
    branch: str = Query(..., description="Target branch"),
    expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
    oms_client: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        await _require_db_role(request, db_name=db_name, roles=DOMAIN_MODEL_ROLES)
        link_type_id = str(link_type_id or "").strip()
        if not link_type_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="link_type_id is required")

        existing = await dataset_registry.get_relationship_spec(link_type_id=link_type_id)
        if not existing:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Relationship spec not found")

        payload = sanitize_input(body.model_dump(exclude_unset=True))
        relationship_spec_payload = payload.get("relationship_spec") if isinstance(payload.get("relationship_spec"), dict) else None
        link_resource_payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="link_type",
            resource_id=link_type_id,
            branch=branch,
        )
        link_resource = link_resource_payload.get("data") if isinstance(link_resource_payload, dict) else link_resource_payload
        link_spec = link_resource.get("spec") if isinstance(link_resource, dict) else {}
        existing_cardinality = str(link_spec.get("cardinality") or "").strip()

        mapping_spec_id = existing.mapping_spec_id
        mapping_spec_version = existing.mapping_spec_version
        updated_spec = existing.spec
        resolved_dataset_id = existing.dataset_id
        resolved_dataset_version_id = existing.dataset_version_id

        if relationship_spec_payload is not None:
            source_ontology = await oms_client.get_ontology(db_name, existing.source_object_type, branch=branch)
            target_ontology = await oms_client.get_ontology(db_name, existing.target_object_type, branch=branch)
            if not source_ontology or not target_ontology:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ontology class not found")

            source_props = _extract_ontology_properties(source_ontology)
            target_props = _extract_ontology_properties(target_ontology)

            _, source_contract = await _resolve_object_type_contract(
                oms_client=oms_client,
                db_name=db_name,
                class_id=existing.source_object_type,
                branch=branch,
            )
            _, target_contract = await _resolve_object_type_contract(
                oms_client=oms_client,
                db_name=db_name,
                class_id=existing.target_object_type,
                branch=branch,
            )

            mapping_request, resolved_dataset_id, resolved_dataset_version_id, resolved_spec_type = await _build_mapping_request(
                db_name=db_name,
                request=request,
                oms_client=oms_client,
                dataset_registry=dataset_registry,
                relationship_spec_id=existing.relationship_spec_id,
                link_type_id=link_type_id,
                source_class=existing.source_object_type,
                target_class=existing.target_object_type,
                predicate=existing.predicate,
                cardinality=existing_cardinality,
                branch=branch,
                source_props=source_props,
                target_props=target_props,
                source_contract=source_contract,
                target_contract=target_contract,
                spec_payload=relationship_spec_payload,
            )

            mapping_response = await objectify_router.create_mapping_spec(
                body=mapping_request,
                request=request,
                dataset_registry=dataset_registry,
                objectify_registry=objectify_registry,
                oms_client=oms_client,
            )
            mapping_payload = mapping_response.get("data", {}).get("mapping_spec")
            if not isinstance(mapping_payload, dict):
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create mapping spec")
            mapping_spec_id = str(mapping_payload.get("mapping_spec_id") or "").strip()
            mapping_spec_version = int(mapping_payload.get("version") or mapping_payload.get("mapping_spec_version") or 1)
            updated_spec = dict(relationship_spec_payload)
            if resolved_spec_type in {"join_table", "object_backed"}:
                updated_spec.setdefault("join_dataset_id", mapping_request.dataset_id)
                if resolved_dataset_version_id:
                    updated_spec.setdefault("join_dataset_version_id", resolved_dataset_version_id)
            if resolved_spec_type == "object_backed":
                relationship_object_type = str(updated_spec.get("relationship_object_type") or "").strip()
                if relationship_object_type:
                    updated_spec["relationship_object_type"] = relationship_object_type

        updated = await dataset_registry.update_relationship_spec(
            relationship_spec_id=existing.relationship_spec_id,
            status=payload.get("status"),
            auto_sync=payload.get("auto_sync") if payload.get("auto_sync") is not None else None,
            spec=updated_spec,
            dataset_id=resolved_dataset_id,
            dataset_version_id=resolved_dataset_version_id,
            mapping_spec_id=mapping_spec_id,
            mapping_spec_version=mapping_spec_version,
        )
        if not updated:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Relationship spec not found")

        resource_payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="link_type",
            resource_id=link_type_id,
            branch=branch,
        )
        resource = resource_payload.get("data") if isinstance(resource_payload, dict) else resource_payload
        if not isinstance(resource, dict):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Link type not found")

        spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
        if not isinstance(spec, dict):
            spec = {}
        spec.update(
            {
                "relationship_spec_id": existing.relationship_spec_id,
                "relationship_spec_type": existing.spec_type,
                "mapping_spec_id": mapping_spec_id,
                "mapping_spec_version": mapping_spec_version,
                "relationship_spec": {
                    **(updated.spec if isinstance(updated.spec, dict) else {}),
                    "relationship_spec_id": existing.relationship_spec_id,
                    "spec_type": existing.spec_type,
                    "dataset_id": updated.dataset_id,
                    "dataset_version_id": updated.dataset_version_id,
                    "mapping_spec_id": mapping_spec_id,
                    "mapping_spec_version": mapping_spec_version,
                },
            }
        )
        if payload.get("status"):
            spec["status"] = payload.get("status")
        resource.update(
            {
                "label": payload.get("label") or resource.get("label") or link_type_id,
                "description": payload.get("description") or resource.get("description"),
                "metadata": payload.get("metadata") if payload.get("metadata") is not None else resource.get("metadata"),
                "spec": spec,
            }
        )

        updated_resource = await oms_client.update_ontology_resource(
            db_name,
            resource_type="link_type",
            resource_id=link_type_id,
            payload=resource,
            branch=branch,
            expected_head_commit=expected_head_commit,
        )

        if payload.get("trigger_index"):
            await _enqueue_link_index_job(
                dataset_registry=dataset_registry,
                objectify_registry=objectify_registry,
                mapping_spec_id=mapping_spec_id,
                mapping_spec_version=mapping_spec_version,
                dataset_id=existing.dataset_id,
                dataset_version_id=existing.dataset_version_id,
            )

        return ApiResponse.success(
            message="Link type updated",
            data={
                "link_type": updated_resource.get("data") if isinstance(updated_resource, dict) else updated_resource,
                "relationship_spec": updated.__dict__,
            },
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to update link type: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.post("/link-types/{link_type_id}/reindex", response_model=ApiResponse)
async def reindex_link_type(
    db_name: str,
    link_type_id: str,
    request: Request,
    dataset_version_id: Optional[str] = Query(default=None),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        await _require_db_role(request, db_name=db_name, roles=DATA_ENGINEER_ROLES)
        link_type_id = str(link_type_id or "").strip()
        if not link_type_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="link_type_id is required")
        record = await dataset_registry.get_relationship_spec(link_type_id=link_type_id)
        if not record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Relationship spec not found")

        job_id = await _enqueue_link_index_job(
            dataset_registry=dataset_registry,
            objectify_registry=objectify_registry,
            mapping_spec_id=record.mapping_spec_id,
            mapping_spec_version=record.mapping_spec_version,
            dataset_id=record.dataset_id,
            dataset_version_id=dataset_version_id,
        )
        if not job_id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Unable to enqueue link index job")
        return ApiResponse.success(message="Link indexing enqueued", data={"job_id": job_id})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to reindex link type: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))
