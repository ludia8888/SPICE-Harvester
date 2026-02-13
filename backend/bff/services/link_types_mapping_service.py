"""Link type relationship-spec mapping helpers (BFF).

This module contains the domain logic used to translate link-type relationship
specs (foreign_key/join_table/object_backed) into Objectify mapping spec
requests.

Routers should remain thin and delegate to this service.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from fastapi import HTTPException, Request, status

from shared.errors.error_types import ErrorCode, ErrorCategory, classified_http_exception
from shared.errors.legacy_codes import LegacyErrorCode

from bff.schemas.link_types_requests import (
    ForeignKeyRelationshipSpec,
    JoinTableRelationshipSpec,
    ObjectBackedRelationshipSpec,
)
from bff.schemas.objectify_requests import CreateMappingSpecRequest, MappingSpecField
from bff.services.oms_client import OMSClient
from shared.security.auth_utils import enforce_db_scope
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.pipeline.pipeline_schema_utils import normalize_schema_type
from shared.utils.import_type_normalization import normalize_import_target_type
from shared.utils.key_spec import normalize_key_spec
from shared.utils.payload_utils import unwrap_data_payload
from shared.utils.schema_columns import (
    extract_schema_columns as _extract_schema_columns_raw,
    extract_schema_type_map as _extract_schema_type_map_raw,
)
from shared.utils.schema_hash import compute_schema_hash_from_payload
from shared.utils.schema_type_compatibility import is_type_compatible
from shared.utils.string_list_utils import normalize_string_list
from shared.observability.tracing import trace_external_call, trace_db_operation


def extract_schema_columns(schema: Any) -> List[Dict[str, Any]]:
    return _extract_schema_columns_raw(schema)


def extract_schema_types(schema: Any) -> Dict[str, str]:
    return _extract_schema_type_map_raw(schema, normalizer=normalize_schema_type)


def compute_schema_hash(schema: Any) -> Optional[str]:
    return compute_schema_hash_from_payload(schema)


def build_join_schema(
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


def extract_ontology_properties(payload: Any) -> Dict[str, Dict[str, Any]]:
    data = unwrap_data_payload(payload)
    props = data.get("properties") if isinstance(data, dict) else None
    output: Dict[str, Dict[str, Any]] = {}
    if isinstance(props, list):
        for prop in props:
            if not isinstance(prop, dict):
                continue
            name = str(prop.get("name") or "").strip()
            if name:
                output[name] = prop
    return output


def extract_ontology_relationships(payload: Any) -> Dict[str, Dict[str, Any]]:
    data = unwrap_data_payload(payload)
    rels = data.get("relationships") if isinstance(data, dict) else None
    output: Dict[str, Dict[str, Any]] = {}
    if isinstance(rels, list):
        for rel in rels:
            if not isinstance(rel, dict):
                continue
            predicate = str(rel.get("predicate") or rel.get("name") or "").strip()
            if predicate:
                output[predicate] = rel
    return output


def normalize_spec_type(value: str) -> str:
    return str(value or "").strip().lower()


def normalize_policy(value: Optional[str], *, default: str) -> str:
    raw = str(value or default).strip().upper() or default
    if raw not in {"FAIL", "WARN", "DEDUP"}:
        return default
    return raw


def normalize_pk_fields(value: Any) -> List[str]:
    return normalize_string_list(value)


@trace_external_call("bff.link_types_mapping.resolve_object_type_contract")
async def resolve_object_type_contract(
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


@trace_db_operation("bff.link_types_mapping.resolve_dataset_and_version")
async def resolve_dataset_and_version(
    *,
    dataset_registry: DatasetRegistry,
    dataset_id: str,
    dataset_version_id: Optional[str],
) -> Tuple[Any, Any, str]:
    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset not found", code=ErrorCode.RESOURCE_NOT_FOUND)
    version = None
    if dataset_version_id:
        version = await dataset_registry.get_version(version_id=dataset_version_id)
        if not version or version.dataset_id != dataset.dataset_id:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset version not found", code=ErrorCode.RESOURCE_NOT_FOUND)
    if not version:
        version = await dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
    if not version:
        raise classified_http_exception(status.HTTP_409_CONFLICT, "Dataset version is required", code=ErrorCode.CONFLICT)
    schema_hash = compute_schema_hash(version.sample_json or dataset.schema_json)
    if not schema_hash:
        raise classified_http_exception(status.HTTP_409_CONFLICT, "schema_hash is required for relationship spec", code=ErrorCode.CONFLICT)
    return dataset, version, schema_hash


@trace_db_operation("bff.link_types_mapping.ensure_join_dataset")
async def ensure_join_dataset(
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
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Join dataset not found", code=ErrorCode.RESOURCE_NOT_FOUND)
    else:
        if not auto_create:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "join_dataset_id is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        name = (join_dataset_name or default_name or "").strip()
        if not name:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "join_dataset_name is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        dataset = await dataset_registry.get_dataset_by_name(db_name=db_name, name=name, branch=dataset_branch)
        if not dataset:
            schema_json = build_join_schema(
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
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Join dataset does not belong to requested database",
            code=ErrorCode.CONFLICT,
        )

    if join_dataset_version_id:
        version = await dataset_registry.get_version(version_id=join_dataset_version_id)
        if not version or version.dataset_id != dataset.dataset_id:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Join dataset version not found", code=ErrorCode.RESOURCE_NOT_FOUND)
    if not version:
        version = await dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
    if not version and auto_create:
        schema_json = build_join_schema(
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
        raise classified_http_exception(status.HTTP_409_CONFLICT, "Join dataset version is required", code=ErrorCode.CONFLICT)

    schema_hash = compute_schema_hash(version.sample_json or dataset.schema_json)
    if not schema_hash:
        raise classified_http_exception(status.HTTP_409_CONFLICT, "schema_hash is required for relationship spec", code=ErrorCode.CONFLICT)
    return dataset, version, schema_hash


def resolve_property_type(prop_map: Dict[str, Dict[str, Any]], field: str) -> Optional[str]:
    meta = prop_map.get(field)
    if not meta:
        return None
    raw_type = meta.get("type") or meta.get("data_type") or meta.get("datatype")
    return normalize_import_target_type(raw_type)


def _extract_pk_fields(*, contract: Dict[str, Any], props: Dict[str, Dict[str, Any]]) -> List[str]:
    pk = normalize_key_spec(contract.get("pk_spec") or {}, columns=list(props.keys()))
    return [str(v).strip() for v in pk.get("primary_key") or [] if str(v).strip()]


@dataclass(frozen=True)
class _MappingContext:
    db_name: str
    request: Request
    oms_client: Optional[OMSClient]
    dataset_registry: DatasetRegistry
    relationship_spec_id: str
    link_type_id: str
    source_class: str
    target_class: str
    predicate: str
    cardinality: str
    branch: str
    source_props: Dict[str, Dict[str, Any]]
    target_props: Dict[str, Dict[str, Any]]
    source_contract: Dict[str, Any]
    target_contract: Dict[str, Any]


@dataclass(frozen=True)
class _MappingResult:
    mapping_request: CreateMappingSpecRequest
    dataset_id: str
    dataset_version_id: Optional[str]
    spec_type: str


class _ForeignKeyMappingStrategy:
    async def build(
        self,
        *,
        ctx: _MappingContext,
        fk_spec: ForeignKeyRelationshipSpec,
        source_pk_fields: List[str],
        target_pk_fields: List[str],
    ) -> _MappingResult:
        fk_column = str(fk_spec.fk_column or "").strip()
        if not fk_column:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "fk_column is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        backing_source = (
            ctx.source_contract.get("backing_source")
            if isinstance(ctx.source_contract.get("backing_source"), dict)
            else {}
        )
        backing_id = str(backing_source.get("ref") or "").strip() or None
        dataset_id = fk_spec.source_dataset_id or None
        dataset_version_id = fk_spec.source_dataset_version_id or None
        if not dataset_id:
            if not backing_id:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "source backing datasource missing", code=ErrorCode.CONFLICT)
            backing = await ctx.dataset_registry.get_backing_datasource(backing_id=backing_id)
            if not backing:
                raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Backing datasource not found", code=ErrorCode.RESOURCE_NOT_FOUND)
            dataset_id = backing.dataset_id

        dataset, version, schema_hash = await resolve_dataset_and_version(
            dataset_registry=ctx.dataset_registry,
            dataset_id=dataset_id,
            dataset_version_id=dataset_version_id,
        )
        enforce_db_scope(ctx.request.headers, db_name=dataset.db_name)

        schema_types = extract_schema_types(version.sample_json or dataset.schema_json)
        if fk_column not in schema_types:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "fk_column not found in dataset", code=ErrorCode.OBJECTIFY_MAPPING_ERROR)

        target_pk_field = str(
            fk_spec.target_pk_field or (target_pk_fields[0] if len(target_pk_fields) == 1 else "")
        ).strip()
        if not target_pk_field:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "target_pk_field is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        if target_pk_field not in ctx.target_props:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "target_pk_field missing from ontology", code=ErrorCode.OBJECTIFY_MAPPING_ERROR)

        effective_source_pk_fields = normalize_pk_fields(fk_spec.source_pk_fields) or source_pk_fields
        for field in effective_source_pk_fields:
            if field not in schema_types:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, f"source pk column missing: {field}", code=ErrorCode.OBJECTIFY_MAPPING_ERROR)

        fk_type = schema_types.get(fk_column)
        target_pk_type = resolve_property_type(ctx.target_props, target_pk_field)
        if not fk_type or not target_pk_type or not is_type_compatible(fk_type, target_pk_type):
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Relationship foreign key type mismatch",
                code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                external_code=LegacyErrorCode.RELATIONSHIP_FK_TYPE_MISMATCH,
                extra={"fk_type": fk_type, "target_pk_type": target_pk_type},
            )

        for field in effective_source_pk_fields:
            source_pk_type = schema_types.get(field)
            expected = resolve_property_type(ctx.source_props, field) or target_pk_type
            if source_pk_type and expected and not is_type_compatible(source_pk_type, expected):
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Relationship source primary key type mismatch",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                    external_code=LegacyErrorCode.RELATIONSHIP_SOURCE_PK_TYPE_MISMATCH,
                    extra={"field": field, "observed": source_pk_type, "expected": expected},
                )

        mappings = [{"source_field": field, "target_field": field} for field in effective_source_pk_fields]
        mappings.append({"source_field": fk_column, "target_field": ctx.predicate})

        backing = await ctx.dataset_registry.get_or_create_backing_datasource(
            dataset=dataset,
            source_type=dataset.source_type,
            source_ref=dataset.source_ref,
        )
        backing_version = await ctx.dataset_registry.get_or_create_backing_datasource_version(
            backing_id=backing.backing_id,
            dataset_version_id=version.version_id,
            schema_hash=schema_hash,
            metadata={"artifact_key": version.artifact_key},
        )

        options = {
            "mode": "link_index",
            "relationship_spec_id": ctx.relationship_spec_id,
            "link_type_id": ctx.link_type_id,
            "relationship_kind": "foreign_key",
            "dangling_policy": normalize_policy(fk_spec.dangling_policy, default="FAIL"),
            "relationship_meta": {
                ctx.predicate: {"target": ctx.target_class, "cardinality": ctx.cardinality},
            },
        }

        mapping_request = CreateMappingSpecRequest(
            dataset_id=dataset.dataset_id,
            dataset_branch=dataset.branch,
            artifact_output_name=dataset.name,
            schema_hash=schema_hash,
            backing_datasource_id=backing.backing_id,
            backing_datasource_version_id=backing_version.version_id,
            target_class_id=ctx.source_class,
            mappings=[MappingSpecField(**m) for m in mappings],
            status="ACTIVE",
            auto_sync=fk_spec.auto_sync,
            options=options,
        )
        return _MappingResult(
            mapping_request=mapping_request,
            dataset_id=dataset.dataset_id,
            dataset_version_id=version.version_id,
            spec_type="foreign_key",
        )


class _JoinTableMappingStrategy:
    async def build(
        self,
        *,
        ctx: _MappingContext,
        join_spec: JoinTableRelationshipSpec,
        source_pk_fields: List[str],
        target_pk_fields: List[str],
        spec_type: str,
        relationship_kind: str,
        relationship_object_type: Optional[str],
    ) -> _MappingResult:
        source_pk_field = source_pk_fields[0] if len(source_pk_fields) == 1 else None
        target_pk_field = target_pk_fields[0] if len(target_pk_fields) == 1 else None
        if not source_pk_field or not target_pk_field:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "composite pk requires explicit mapping", code=ErrorCode.OBJECTIFY_MAPPING_ERROR)

        source_pk_type = resolve_property_type(ctx.source_props, source_pk_field)
        target_pk_type = resolve_property_type(ctx.target_props, target_pk_field)

        dataset, version, schema_hash = await ensure_join_dataset(
            dataset_registry=ctx.dataset_registry,
            request=ctx.request,
            db_name=ctx.db_name,
            join_dataset_id=str(join_spec.join_dataset_id or "").strip() or None,
            join_dataset_version_id=join_spec.join_dataset_version_id,
            join_dataset_name=join_spec.join_dataset_name,
            join_dataset_branch=join_spec.join_dataset_branch,
            auto_create=bool(join_spec.auto_create),
            default_name=f"{ctx.source_class}_{ctx.predicate}_{ctx.target_class}_join",
            source_key_column=str(join_spec.source_key_column or "").strip(),
            target_key_column=str(join_spec.target_key_column or "").strip(),
            source_key_type=source_pk_type,
            target_key_type=target_pk_type,
        )

        schema_types = extract_schema_types(version.sample_json or dataset.schema_json)
        source_key_column = str(join_spec.source_key_column or "").strip()
        target_key_column = str(join_spec.target_key_column or "").strip()
        if source_key_column not in schema_types:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "source_key_column missing", code=ErrorCode.OBJECTIFY_MAPPING_ERROR)
        if target_key_column not in schema_types:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "target_key_column missing", code=ErrorCode.OBJECTIFY_MAPPING_ERROR)

        source_type = schema_types.get(source_key_column)
        target_type = schema_types.get(target_key_column)
        if not source_type or not source_pk_type or not is_type_compatible(source_type, source_pk_type):
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Join source type mismatch",
                code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                external_code=LegacyErrorCode.RELATIONSHIP_JOIN_SOURCE_TYPE_MISMATCH,
                extra={"observed": source_type, "expected": source_pk_type},
            )
        if not target_type or not target_pk_type or not is_type_compatible(target_type, target_pk_type):
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Join target type mismatch",
                code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                external_code=LegacyErrorCode.RELATIONSHIP_JOIN_TARGET_TYPE_MISMATCH,
                extra={"observed": target_type, "expected": target_pk_type},
            )

        mappings = [
            {"source_field": source_key_column, "target_field": source_pk_field},
            {"source_field": target_key_column, "target_field": ctx.predicate},
        ]

        backing = await ctx.dataset_registry.get_or_create_backing_datasource(
            dataset=dataset,
            source_type=dataset.source_type,
            source_ref=dataset.source_ref,
        )
        backing_version = await ctx.dataset_registry.get_or_create_backing_datasource_version(
            backing_id=backing.backing_id,
            dataset_version_id=version.version_id,
            schema_hash=schema_hash,
            metadata={"artifact_key": version.artifact_key},
        )

        options = {
            "mode": "link_index",
            "relationship_spec_id": ctx.relationship_spec_id,
            "link_type_id": ctx.link_type_id,
            "relationship_kind": relationship_kind,
            "dedupe_policy": normalize_policy(join_spec.dedupe_policy, default="DEDUP"),
            "dangling_policy": normalize_policy(join_spec.dangling_policy, default="FAIL"),
            "full_sync": True,
            "relationship_meta": {
                ctx.predicate: {"target": ctx.target_class, "cardinality": ctx.cardinality},
            },
        }
        if relationship_object_type:
            options["relationship_object_type"] = relationship_object_type

        mapping_request = CreateMappingSpecRequest(
            dataset_id=dataset.dataset_id,
            dataset_branch=dataset.branch,
            artifact_output_name=dataset.name,
            schema_hash=schema_hash,
            backing_datasource_id=backing.backing_id,
            backing_datasource_version_id=backing_version.version_id,
            target_class_id=ctx.source_class,
            mappings=[MappingSpecField(**m) for m in mappings],
            status="ACTIVE",
            auto_sync=join_spec.auto_sync,
            options=options,
        )
        return _MappingResult(
            mapping_request=mapping_request,
            dataset_id=dataset.dataset_id,
            dataset_version_id=version.version_id if version else None,
            spec_type=spec_type,
        )


class _ObjectBackedMappingStrategy:
    def __init__(self, join_strategy: _JoinTableMappingStrategy) -> None:
        self._join_strategy = join_strategy

    async def build(
        self,
        *,
        ctx: _MappingContext,
        object_backed_spec: ObjectBackedRelationshipSpec,
        source_pk_fields: List[str],
        target_pk_fields: List[str],
    ) -> _MappingResult:
        relationship_object_type = str(object_backed_spec.relationship_object_type or "").strip()
        if not relationship_object_type:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "relationship_object_type is required for object_backed",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        if ctx.oms_client is None:
            raise classified_http_exception(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                "OMS client is required for object_backed",
                code=ErrorCode.INTERNAL_ERROR,
            )

        _, relationship_contract = await resolve_object_type_contract(
            oms_client=ctx.oms_client,
            db_name=ctx.db_name,
            class_id=relationship_object_type,
            branch=ctx.branch,
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

        return await self._join_strategy.build(
            ctx=ctx,
            join_spec=join_spec,
            source_pk_fields=source_pk_fields,
            target_pk_fields=target_pk_fields,
            spec_type="object_backed",
            relationship_kind="object_backed",
            relationship_object_type=relationship_object_type,
        )


@trace_db_operation("bff.link_types_mapping.build_mapping_request")
async def build_mapping_request(
    *,
    db_name: str,
    request: Request,
    oms_client: Optional[OMSClient],
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
) -> Tuple[CreateMappingSpecRequest, str, Optional[str], str]:
    spec_type = normalize_spec_type(spec_payload.get("type") or "")
    normalized_type = "join_table" if spec_type == "object_backed" else spec_type
    if normalized_type not in {"foreign_key", "join_table"}:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "relationship_spec.type is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

    source_pk_fields = _extract_pk_fields(contract=source_contract, props=source_props)
    target_pk_fields = _extract_pk_fields(contract=target_contract, props=target_props)
    if not source_pk_fields or not target_pk_fields:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Object type primary key is missing",
            code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
            external_code=LegacyErrorCode.OBJECT_TYPE_PRIMARY_KEY_MISSING,
            extra={"source": source_class, "target": target_class},
        )

    ctx = _MappingContext(
        db_name=db_name,
        request=request,
        oms_client=oms_client,
        dataset_registry=dataset_registry,
        relationship_spec_id=relationship_spec_id,
        link_type_id=link_type_id,
        source_class=source_class,
        target_class=target_class,
        predicate=predicate,
        cardinality=cardinality,
        branch=branch,
        source_props=source_props,
        target_props=target_props,
        source_contract=source_contract,
        target_contract=target_contract,
    )

    if normalized_type == "foreign_key":
        fk_spec = ForeignKeyRelationshipSpec(**spec_payload)
        result = await _ForeignKeyMappingStrategy().build(
            ctx=ctx,
            fk_spec=fk_spec,
            source_pk_fields=source_pk_fields,
            target_pk_fields=target_pk_fields,
        )
    else:
        join_strategy = _JoinTableMappingStrategy()
        if spec_type == "object_backed":
            object_backed_spec = ObjectBackedRelationshipSpec(**spec_payload)
            result = await _ObjectBackedMappingStrategy(join_strategy).build(
                ctx=ctx,
                object_backed_spec=object_backed_spec,
                source_pk_fields=source_pk_fields,
                target_pk_fields=target_pk_fields,
            )
        else:
            join_spec = JoinTableRelationshipSpec(**spec_payload)
            result = await join_strategy.build(
                ctx=ctx,
                join_spec=join_spec,
                source_pk_fields=source_pk_fields,
                target_pk_fields=target_pk_fields,
                spec_type="join_table",
                relationship_kind="join_table",
                relationship_object_type=None,
            )

    return (
        result.mapping_request,
        result.dataset_id,
        result.dataset_version_id,
        result.spec_type,
    )
