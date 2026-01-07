"""
Object type contracts: backing datasource binding + key spec enforcement.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from bff.dependencies import OMSClientDep
from bff.services.oms_client import OMSClient
from bff.services.mapping_suggestion_service import MappingSuggestionService
from bff.routers import objectify as objectify_router
from shared.models.requests import ApiResponse
from shared.security.auth_utils import enforce_db_scope
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.services.dataset_registry import DatasetRegistry
from shared.services.objectify_registry import ObjectifyRegistry
from shared.utils.key_spec import normalize_key_spec
from shared.utils.schema_hash import compute_schema_hash

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databases/{db_name}/ontology", tags=["Ontology Object Types"])


async def get_dataset_registry() -> DatasetRegistry:
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


async def get_objectify_registry() -> ObjectifyRegistry:
    from bff.main import get_objectify_registry as _get_objectify_registry

    return await _get_objectify_registry()


class ObjectTypeContractRequest(BaseModel):
    class_id: str = Field(..., description="Ontology class id")
    backing_dataset_id: Optional[str] = Field(default=None, description="Dataset id backing the object type")
    backing_datasource_id: Optional[str] = Field(default=None, description="Backing datasource id")
    backing_datasource_version_id: Optional[str] = Field(default=None, description="Backing datasource version id")
    dataset_version_id: Optional[str] = Field(default=None, description="Dataset version id")
    schema_hash: Optional[str] = Field(default=None, description="Schema hash override")
    pk_spec: Dict[str, Any] = Field(default_factory=dict, description="Key spec (primary/title/unique/nullability)")
    mapping_spec_id: Optional[str] = Field(default=None, description="Mapping spec id for property mapping")
    mapping_spec_version: Optional[int] = Field(default=None, description="Mapping spec version")
    status: str = Field(default="ACTIVE", description="Contract status")
    auto_generate_mapping: bool = Field(default=False, description="Auto-generate property mapping")
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ObjectTypeContractUpdate(BaseModel):
    backing_dataset_id: Optional[str] = None
    backing_datasource_id: Optional[str] = None
    backing_datasource_version_id: Optional[str] = None
    dataset_version_id: Optional[str] = None
    schema_hash: Optional[str] = None
    pk_spec: Optional[Dict[str, Any]] = None
    mapping_spec_id: Optional[str] = None
    mapping_spec_version: Optional[int] = None
    status: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    migration: Optional[Dict[str, Any]] = None


def _extract_resource_payload(response: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(response, dict):
        return {}
    payload = response.get("data") if isinstance(response.get("data"), dict) else None
    if isinstance(payload, dict):
        return payload
    return response


def _schema_hash_from_version(sample_json: Any, schema_json: Any) -> Optional[str]:
    if isinstance(sample_json, dict):
        columns = sample_json.get("columns")
        if isinstance(columns, list) and columns:
            return compute_schema_hash(columns)
    if isinstance(schema_json, dict):
        columns = schema_json.get("columns")
        if isinstance(columns, list) and columns:
            return compute_schema_hash(columns)
    return None


async def _resolve_backing(
    *,
    db_name: str,
    request: Request,
    dataset_registry: DatasetRegistry,
    backing_dataset_id: Optional[str],
    backing_datasource_id: Optional[str],
    backing_datasource_version_id: Optional[str],
    dataset_version_id: Optional[str],
    schema_hash: Optional[str],
) -> Tuple[Any, Any, Any, Any, str]:
    dataset = None
    backing = None
    backing_version = None
    version = None

    if backing_datasource_version_id:
        backing_version = await dataset_registry.get_backing_datasource_version(
            version_id=backing_datasource_version_id
        )
        if not backing_version:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backing datasource version not found")
        backing = await dataset_registry.get_backing_datasource(backing_id=backing_version.backing_id)
        if not backing:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backing datasource not found")
        dataset = await dataset_registry.get_dataset(dataset_id=backing.dataset_id)
        if not dataset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    elif backing_datasource_id:
        backing = await dataset_registry.get_backing_datasource(backing_id=backing_datasource_id)
        if not backing:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backing datasource not found")
        dataset = await dataset_registry.get_dataset(dataset_id=backing.dataset_id)
    elif backing_dataset_id:
        dataset = await dataset_registry.get_dataset(dataset_id=backing_dataset_id)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="backing_dataset_id or backing_datasource_id or backing_datasource_version_id is required",
        )

    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    enforce_db_scope(request.headers, db_name=dataset.db_name)
    if dataset.db_name != db_name:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Backing dataset does not belong to requested database",
        )

    if not backing:
        backing = await dataset_registry.get_or_create_backing_datasource(
            dataset=dataset,
            source_type=dataset.source_type,
            source_ref=dataset.source_ref,
        )

    if dataset_version_id:
        version = await dataset_registry.get_version(version_id=dataset_version_id)
        if not version or version.dataset_id != dataset.dataset_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset version not found")
    if not version:
        version = await dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
    if not version:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Dataset version is required")

    if backing_version and backing_version.dataset_version_id != version.version_id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Backing datasource version does not match dataset version",
        )

    resolved_schema_hash = schema_hash or (backing_version.schema_hash if backing_version else None)
    if not resolved_schema_hash:
        resolved_schema_hash = _schema_hash_from_version(version.sample_json, dataset.schema_json)
    if not resolved_schema_hash:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="schema_hash is required for backing datasource",
        )

    if not backing_version:
        backing_version = await dataset_registry.get_or_create_backing_datasource_version(
            backing_id=backing.backing_id,
            dataset_version_id=version.version_id,
            schema_hash=resolved_schema_hash,
            metadata={"artifact_key": version.artifact_key},
        )
    return dataset, backing, backing_version, version, resolved_schema_hash


def _extract_ontology_properties(payload: Any) -> set[str]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        payload = payload["data"]
    if not isinstance(payload, dict):
        return set()
    props = payload.get("properties")
    names: set[str] = set()
    if isinstance(props, list):
        for prop in props:
            if isinstance(prop, dict):
                name = str(prop.get("name") or "").strip()
                if name:
                    names.add(name)
    return names


def _extract_schema_columns(schema: Any) -> list[dict[str, Any]]:
    if not isinstance(schema, dict):
        return []
    columns = schema.get("columns")
    if isinstance(columns, list):
        output: list[dict[str, Any]] = []
        for col in columns:
            if isinstance(col, dict):
                name = str(col.get("name") or col.get("column") or "").strip()
                col_type = col.get("type") or col.get("data_type") or col.get("datatype")
            else:
                name = str(col).strip()
                col_type = None
            if name:
                output.append({"name": name, "type": col_type})
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


@router.post("/object-types", status_code=status.HTTP_201_CREATED, response_model=ApiResponse)
async def create_object_type_contract(
    db_name: str,
    body: ObjectTypeContractRequest,
    request: Request,
    branch: str = Query("main", description="Target branch"),
    expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
    oms_client: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        payload = sanitize_input(body.model_dump(exclude_unset=True))
        class_id = str(payload.get("class_id") or "").strip()
        if not class_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="class_id is required")

        ontology_payload = await oms_client.get_ontology(db_name, class_id, branch=branch)
        if not ontology_payload:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ontology class not found")

        dataset, backing, backing_version, version, resolved_schema_hash = await _resolve_backing(
            db_name=db_name,
            request=request,
            dataset_registry=dataset_registry,
            backing_dataset_id=payload.get("backing_dataset_id"),
            backing_datasource_id=payload.get("backing_datasource_id"),
            backing_datasource_version_id=payload.get("backing_datasource_version_id"),
            dataset_version_id=payload.get("dataset_version_id"),
            schema_hash=payload.get("schema_hash"),
        )

        ontology_props = _extract_ontology_properties(ontology_payload)
        pk_spec = normalize_key_spec(payload.get("pk_spec") or {}, columns=sorted(ontology_props))
        if not pk_spec.get("primary_key"):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="pk_spec.primary_key is required")
        if not pk_spec.get("title_key"):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="pk_spec.title_key is required")
        missing_keys = sorted(set(pk_spec.get("primary_key", []) + pk_spec.get("title_key", [])) - ontology_props)
        if missing_keys:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "OBJECT_TYPE_KEY_FIELDS_MISSING", "fields": missing_keys},
            )

        mapping_spec_id = str(payload.get("mapping_spec_id") or "").strip() or None
        mapping_spec_version = payload.get("mapping_spec_version")
        mapping_spec_payload = None
        if mapping_spec_id:
            mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
            if not mapping_spec:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Mapping spec not found")
            if mapping_spec.target_class_id != class_id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Mapping spec target_class_id mismatch",
                )
            if mapping_spec.dataset_id != dataset.dataset_id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Mapping spec dataset mismatch",
                )
            mapping_spec_version = mapping_spec_version or mapping_spec.version
            mapping_spec_payload = {
                "mapping_spec_id": mapping_spec.mapping_spec_id,
                "mapping_spec_version": mapping_spec_version,
                "status": mapping_spec.status,
            }

        backing_source = {
            "kind": "backing_datasource",
            "ref": backing.backing_id,
            "version_id": backing_version.version_id,
            "schema_hash": resolved_schema_hash,
            "dataset_id": dataset.dataset_id,
            "dataset_version_id": version.version_id,
            "artifact_key": version.artifact_key,
            "branch": dataset.branch,
        }

        resource_payload = {
            "id": class_id,
            "label": (ontology_payload.get("label") or class_id),
            "description": ontology_payload.get("description"),
            "metadata": payload.get("metadata") or {},
            "spec": {
                "backing_source": backing_source,
                "pk_spec": pk_spec,
                "mapping_spec": mapping_spec_payload or {},
                "status": str(payload.get("status") or "ACTIVE").upper(),
            },
        }

        response = await oms_client.create_ontology_resource(
            db_name,
            resource_type="object_type",
            payload=resource_payload,
            branch=branch,
            expected_head_commit=expected_head_commit,
        )
        resource = _extract_resource_payload(response)

        if payload.get("auto_generate_mapping") and not mapping_spec_id:
            source_schema = _extract_schema_columns(version.sample_json or dataset.schema_json)
            target_schema = []
            if isinstance(ontology_payload, dict):
                properties = ontology_payload.get("properties") or []
                if isinstance(properties, list):
                    for prop in properties:
                        if not isinstance(prop, dict):
                            continue
                        name = str(prop.get("name") or "").strip()
                        if not name:
                            continue
                        target_schema.append({"name": name, "type": prop.get("type")})

            suggestion_service = MappingSuggestionService()
            suggestion = suggestion_service.suggest_mappings(source_schema=source_schema, target_schema=target_schema)
            mappings = [
                {"source_field": m.source_field, "target_field": m.target_field}
                for m in suggestion.mappings
            ]
            if not mappings:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={"code": "OBJECT_TYPE_AUTO_MAPPING_EMPTY", "class_id": class_id},
                )
            auto_body = objectify_router.CreateMappingSpecRequest(
                dataset_id=dataset.dataset_id,
                dataset_branch=dataset.branch,
                artifact_output_name=dataset.name,
                schema_hash=resolved_schema_hash,
                backing_datasource_id=backing.backing_id,
                backing_datasource_version_id=backing_version.version_id,
                target_class_id=class_id,
                mappings=[objectify_router.MappingSpecField(**m) for m in mappings],
                status="ACTIVE",
                auto_sync=True,
                options={"auto_generated": True, "mapping_source": "object_type_contract"},
            )
            mapping_response = await objectify_router.create_mapping_spec(
                body=auto_body,
                request=request,
                dataset_registry=dataset_registry,
                objectify_registry=objectify_registry,
                oms_client=oms_client,
            )
            mapping_payload = (
                mapping_response.get("data", {}).get("mapping_spec")
                if isinstance(mapping_response, dict)
                else None
            )
            if isinstance(mapping_payload, dict):
                resource_payload["spec"]["mapping_spec"] = {
                    "mapping_spec_id": mapping_payload.get("mapping_spec_id"),
                    "mapping_spec_version": mapping_payload.get("version"),
                    "status": mapping_payload.get("status"),
                }
                head_payload = await oms_client.get_version_head(db_name, branch=branch)
                head_data = head_payload.get("data") if isinstance(head_payload, dict) else {}
                expected_head = None
                if isinstance(head_data, dict):
                    expected_head = (
                        head_data.get("head_commit_id")
                        or head_data.get("commit")
                        or head_data.get("head_commit")
                    )
                response = await oms_client.update_ontology_resource(
                    db_name,
                    resource_type="object_type",
                    resource_id=class_id,
                    payload=resource_payload,
                    branch=branch,
                    expected_head_commit=expected_head or expected_head_commit,
                )
                resource = _extract_resource_payload(response)

        return ApiResponse.success(
            message="Object type contract created",
            data={
                "object_type": resource,
                "backing_datasource": backing.__dict__,
                "backing_datasource_version": backing_version.__dict__,
            },
        )
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
        logger.error("Failed to create object type contract: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/object-types/{class_id}", response_model=ApiResponse)
async def get_object_type_contract(
    db_name: str,
    class_id: str,
    request: Request,
    branch: str = Query("main", description="Target branch"),
    oms_client: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        class_id = str(class_id or "").strip()
        if not class_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="class_id is required")
        response = await oms_client.get_ontology_resource(
            db_name,
            resource_type="object_type",
            resource_id=class_id,
            branch=branch,
        )
        resource = _extract_resource_payload(response)
        spec = resource.get("spec") if isinstance(resource, dict) else {}
        if not isinstance(spec, dict):
            spec = {}
        backing_source = spec.get("backing_source") if isinstance(spec.get("backing_source"), dict) else {}
        backing_id = str(backing_source.get("ref") or "").strip() or None
        backing_version_id = str(backing_source.get("version_id") or "").strip() or None

        backing_payload = None
        backing_version_payload = None
        if backing_id:
            backing = await dataset_registry.get_backing_datasource(backing_id=backing_id)
            if backing:
                enforce_db_scope(request.headers, db_name=backing.db_name)
                backing_payload = backing.__dict__
                if backing_version_id:
                    backing_version = await dataset_registry.get_backing_datasource_version(
                        version_id=backing_version_id
                    )
                    if backing_version:
                        backing_version_payload = backing_version.__dict__

        mapping_spec_payload = None
        mapping_spec = spec.get("mapping_spec") if isinstance(spec.get("mapping_spec"), dict) else {}
        mapping_spec_id = str(mapping_spec.get("mapping_spec_id") or "").strip() or None
        if mapping_spec_id:
            record = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
            if record:
                mapping_spec_payload = {
                    **record.__dict__,
                    "mappings": record.mappings,
                    "target_field_types": record.target_field_types,
                    "options": record.options,
                }

        return ApiResponse.success(
            message="Object type contract retrieved",
            data={
                "object_type": resource,
                "backing_datasource": backing_payload,
                "backing_datasource_version": backing_version_payload,
                "mapping_spec": mapping_spec_payload,
            },
        )
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
        logger.error("Failed to get object type contract: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.put("/object-types/{class_id}", response_model=ApiResponse)
async def update_object_type_contract(
    db_name: str,
    class_id: str,
    body: ObjectTypeContractUpdate,
    request: Request,
    branch: str = Query(..., description="Target branch"),
    expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
    oms_client: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        class_id = str(class_id or "").strip()
        if not class_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="class_id is required")

        existing_response = await oms_client.get_ontology_resource(
            db_name,
            resource_type="object_type",
            resource_id=class_id,
            branch=branch,
        )
        existing_resource = _extract_resource_payload(existing_response)
        existing_spec = existing_resource.get("spec") if isinstance(existing_resource, dict) else {}
        if not isinstance(existing_spec, dict):
            existing_spec = {}

        payload = sanitize_input(body.model_dump(exclude_unset=True))
        backing_spec = existing_spec.get("backing_source") if isinstance(existing_spec.get("backing_source"), dict) else {}
        if any(
            key in payload
            for key in (
                "backing_dataset_id",
                "backing_datasource_id",
                "backing_datasource_version_id",
                "dataset_version_id",
                "schema_hash",
            )
        ):
            dataset, backing, backing_version, version, resolved_schema_hash = await _resolve_backing(
                db_name=db_name,
                request=request,
                dataset_registry=dataset_registry,
                backing_dataset_id=payload.get("backing_dataset_id"),
                backing_datasource_id=payload.get("backing_datasource_id"),
                backing_datasource_version_id=payload.get("backing_datasource_version_id"),
                dataset_version_id=payload.get("dataset_version_id"),
                schema_hash=payload.get("schema_hash"),
            )
            backing_spec = {
                "kind": "backing_datasource",
                "ref": backing.backing_id,
                "version_id": backing_version.version_id,
                "schema_hash": resolved_schema_hash,
                "dataset_id": dataset.dataset_id,
                "dataset_version_id": version.version_id,
                "artifact_key": version.artifact_key,
                "branch": dataset.branch,
            }

        pk_spec = existing_spec.get("pk_spec") if isinstance(existing_spec.get("pk_spec"), dict) else {}
        if isinstance(payload.get("pk_spec"), dict):
            ontology_payload = await oms_client.get_ontology(db_name, class_id, branch=branch)
            ontology_props = _extract_ontology_properties(ontology_payload)
            pk_spec = normalize_key_spec(payload.get("pk_spec"), columns=sorted(ontology_props))
            if not pk_spec.get("primary_key"):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="pk_spec.primary_key is required")
            if not pk_spec.get("title_key"):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="pk_spec.title_key is required")
            missing_keys = sorted(
                set(pk_spec.get("primary_key", []) + pk_spec.get("title_key", [])) - ontology_props
            )
            if missing_keys:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={"code": "OBJECT_TYPE_KEY_FIELDS_MISSING", "fields": missing_keys},
                )

        mapping_spec_payload = existing_spec.get("mapping_spec") if isinstance(existing_spec.get("mapping_spec"), dict) else {}
        if payload.get("mapping_spec_id") is not None:
            mapping_spec_id = str(payload.get("mapping_spec_id") or "").strip() or None
            if mapping_spec_id:
                mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
                if not mapping_spec:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Mapping spec not found")
                if mapping_spec.target_class_id != class_id:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="Mapping spec target_class_id mismatch",
                    )
                mapping_spec_payload = {
                    "mapping_spec_id": mapping_spec.mapping_spec_id,
                    "mapping_spec_version": payload.get("mapping_spec_version") or mapping_spec.version,
                    "status": mapping_spec.status,
                }
            else:
                mapping_spec_payload = {}

        status_value = payload.get("status") or existing_spec.get("status") or "ACTIVE"
        metadata = payload.get("metadata") if payload.get("metadata") is not None else existing_resource.get("metadata")

        existing_backing = existing_spec.get("backing_source") if isinstance(existing_spec.get("backing_source"), dict) else {}
        backing_changed = False
        if backing_spec:
            if str(backing_spec.get("ref") or "") != str(existing_backing.get("ref") or ""):
                backing_changed = True
            if str(backing_spec.get("schema_hash") or "") != str(existing_backing.get("schema_hash") or ""):
                backing_changed = True
        pk_changed = False
        if isinstance(pk_spec, dict):
            prev_pk = normalize_key_spec(existing_spec.get("pk_spec") or {})
            if set(prev_pk.get("primary_key") or []) != set(pk_spec.get("primary_key") or []):
                pk_changed = True
            if set(prev_pk.get("title_key") or []) != set(pk_spec.get("title_key") or []):
                pk_changed = True

        migration_payload = payload.get("migration") if isinstance(payload.get("migration"), dict) else {}
        migration_approved = bool(migration_payload.get("approved")) if migration_payload else False
        reset_edits = bool(
            migration_payload.get("reset_edits")
            or migration_payload.get("resetEdits")
            or migration_payload.get("edits_reset")
        )
        edit_count = None
        if pk_changed:
            edit_count = await dataset_registry.count_instance_edits(db_name=db_name, class_id=class_id)
            if edit_count and not reset_edits:
                await dataset_registry.record_gate_result(
                    scope="object_type_migration",
                    subject_type="object_type",
                    subject_id=class_id,
                    status="FAIL",
                    details={
                        "backing_changed": backing_changed,
                        "pk_changed": pk_changed,
                        "edit_count": edit_count,
                        "message": "PK 변경 시 편집 이력 초기화 필요",
                    },
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "OBJECT_TYPE_EDIT_RESET_REQUIRED",
                        "edit_count": edit_count,
                        "message": "PK 변경 전 편집 이력 초기화가 필요합니다.",
                    },
                )
        if (backing_changed or pk_changed) and not migration_approved:
            await dataset_registry.record_gate_result(
                scope="object_type_migration",
                subject_type="object_type",
                subject_id=class_id,
                status="FAIL",
                details={
                    "backing_changed": backing_changed,
                    "pk_changed": pk_changed,
                    "message": "Migration approval required to change backing source or keys",
                },
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "OBJECT_TYPE_MIGRATION_REQUIRED",
                    "backing_changed": backing_changed,
                    "pk_changed": pk_changed,
                },
            )

        if pk_changed and reset_edits and edit_count:
            try:
                cleared = await dataset_registry.clear_instance_edits(db_name=db_name, class_id=class_id)
            except Exception:
                cleared = None
            migration_payload = {
                **(migration_payload or {}),
                "reset_edits": True,
                "cleared_edits": cleared,
            }

        updated_payload = {
            "id": class_id,
            "label": existing_resource.get("label") or class_id,
            "description": existing_resource.get("description"),
            "metadata": metadata or {},
            "spec": {
                **existing_spec,
                "backing_source": backing_spec,
                "pk_spec": pk_spec,
                "mapping_spec": mapping_spec_payload,
                "status": str(status_value).upper(),
            },
        }

        response = await oms_client.update_ontology_resource(
            db_name,
            resource_type="object_type",
            resource_id=class_id,
            payload=updated_payload,
            branch=branch,
            expected_head_commit=expected_head_commit,
        )

        if backing_changed or pk_changed:
            await dataset_registry.record_gate_result(
                scope="object_type_migration",
                subject_type="object_type",
                subject_id=class_id,
                status="PASS",
                details={
                    "backing_changed": backing_changed,
                "pk_changed": pk_changed,
                "approved": True,
                "edit_count": edit_count,
                "reset_edits": reset_edits,
                "note": migration_payload.get("note") if migration_payload else None,
            },
        )

        resource = _extract_resource_payload(response)
        return ApiResponse.success(message="Object type contract updated", data={"object_type": resource})
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
        logger.error("Failed to update object type contract: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))
