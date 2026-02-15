"""Object type contract service (BFF).

Contains the core business logic for managing object type contracts while
keeping routers thin and testable (Facade / Service Layer pattern).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import HTTPException, Request, status

from bff.schemas.objectify_requests import CreateMappingSpecRequest, MappingSpecField
from bff.schemas.object_types_requests import ObjectTypeContractRequest, ObjectTypeContractUpdate
from bff.services.mapping_suggestion_service import MappingSuggestionService
from bff.services.oms_client import OMSClient
from bff.services.ontology_occ_guard_service import (
    fetch_branch_head_commit_id,
    resolve_expected_head_commit,
)
from bff.services.objectify_mapping_spec_service import create_mapping_spec as create_mapping_spec_service
from bff.routers.objectify_job_ops import enqueue_objectify_job_for_mapping_spec
from shared.models.requests import ApiResponse
from shared.security.auth_utils import enforce_db_scope
from shared.security.input_sanitizer import sanitize_input
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.utils.key_spec import normalize_key_spec
from shared.utils.schema_columns import extract_schema_columns
from shared.utils.schema_hash import compute_schema_hash
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.errors.legacy_codes import LegacyErrorCode
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


def _extract_resource_payload(response: Any) -> Dict[str, Any]:
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
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Backing datasource version not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        backing = await dataset_registry.get_backing_datasource(backing_id=backing_version.backing_id)
        if not backing:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Backing datasource not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        dataset = await dataset_registry.get_dataset(dataset_id=backing.dataset_id)
        if not dataset:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset not found", code=ErrorCode.RESOURCE_NOT_FOUND)
    elif backing_datasource_id:
        backing = await dataset_registry.get_backing_datasource(backing_id=backing_datasource_id)
        if not backing:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Backing datasource not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        dataset = await dataset_registry.get_dataset(dataset_id=backing.dataset_id)
    elif backing_dataset_id:
        dataset = await dataset_registry.get_dataset(dataset_id=backing_dataset_id)
    else:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "backing_dataset_id or backing_datasource_id or backing_datasource_version_id is required",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )

    if not dataset:
        raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset not found", code=ErrorCode.RESOURCE_NOT_FOUND)
    enforce_db_scope(request.headers, db_name=dataset.db_name)
    if dataset.db_name != db_name:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Backing dataset does not belong to requested database",
            code=ErrorCode.CONFLICT,
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
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset version not found", code=ErrorCode.RESOURCE_NOT_FOUND)
    if not version:
        version = await dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
    if not version:
        raise classified_http_exception(status.HTTP_409_CONFLICT, "Dataset version is required", code=ErrorCode.CONFLICT)

    if backing_version and backing_version.dataset_version_id != version.version_id:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Backing datasource version does not match dataset version",
            code=ErrorCode.CONFLICT,
        )

    resolved_schema_hash = schema_hash or (backing_version.schema_hash if backing_version else None)
    if not resolved_schema_hash:
        resolved_schema_hash = _schema_hash_from_version(version.sample_json, dataset.schema_json)
    if not resolved_schema_hash:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "schema_hash is required for backing datasource",
            code=ErrorCode.CONFLICT,
        )

    if not backing_version:
        backing_version = await dataset_registry.get_or_create_backing_datasource_version(
            backing_id=backing.backing_id,
            dataset_version_id=version.version_id,
            schema_hash=resolved_schema_hash,
            metadata={"artifact_key": version.artifact_key},
        )
    return dataset, backing, backing_version, version, resolved_schema_hash


def _extract_ontology_property_names(payload: Any) -> set[str]:
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


def _normalize_and_validate_pk_spec(raw_pk_spec: Any, *, ontology_property_names: set[str]) -> Dict[str, Any]:
    pk_spec = normalize_key_spec(raw_pk_spec or {}, columns=sorted(ontology_property_names))
    if not pk_spec.get("primary_key"):
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "pk_spec.primary_key is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
    if not pk_spec.get("title_key"):
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "pk_spec.title_key is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
    missing_keys = sorted(set(pk_spec.get("primary_key", []) + pk_spec.get("title_key", [])) - ontology_property_names)
    if missing_keys:
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Object type key fields are missing from ontology properties",
            code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
            external_code=LegacyErrorCode.OBJECT_TYPE_KEY_FIELDS_MISSING,
            extra={"fields": missing_keys},
        )
    return pk_spec


@trace_external_call("bff.object_type_contract.create_object_type_contract")
async def create_object_type_contract(
    *,
    db_name: str,
    body: ObjectTypeContractRequest,
    request: Request,
    branch: str,
    expected_head_commit: Optional[str],
    oms_client: OMSClient,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
) -> ApiResponse:
    try:
        payload = sanitize_input(body.model_dump(exclude_unset=True))
        class_id = str(payload.get("class_id") or "").strip()
        if not class_id:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "class_id is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        ontology_payload = await oms_client.get_ontology(db_name, class_id, branch=branch)
        if not ontology_payload:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Ontology class not found", code=ErrorCode.ONTOLOGY_NOT_FOUND)

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

        ontology_props = _extract_ontology_property_names(ontology_payload)
        pk_spec = _normalize_and_validate_pk_spec(payload.get("pk_spec"), ontology_property_names=ontology_props)

        mapping_spec_id = str(payload.get("mapping_spec_id") or "").strip() or None
        mapping_spec_version = payload.get("mapping_spec_version")
        mapping_spec_payload = None
        if mapping_spec_id:
            mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
            if not mapping_spec:
                raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Mapping spec not found", code=ErrorCode.RESOURCE_NOT_FOUND)
            if mapping_spec.target_class_id != class_id:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Mapping spec target_class_id mismatch",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                )
            if mapping_spec.dataset_id != dataset.dataset_id:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Mapping spec dataset mismatch",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
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

        resource_payload: Dict[str, Any] = {
            "id": class_id,
            "label": (ontology_payload.get("label") or class_id) if isinstance(ontology_payload, dict) else class_id,
            "description": ontology_payload.get("description") if isinstance(ontology_payload, dict) else None,
            "metadata": payload.get("metadata") or {},
            "spec": {
                "backing_source": backing_source,
                "pk_spec": pk_spec,
                "mapping_spec": mapping_spec_payload or {},
                "status": str(payload.get("status") or "ACTIVE").upper(),
            },
        }

        expected_head = await resolve_expected_head_commit(
            oms_client=oms_client,
            db_name=db_name,
            branch=branch,
            expected_head_commit=expected_head_commit,
        )
        response = await oms_client.create_ontology_resource(
            db_name,
            resource_type="object_type",
            payload=resource_payload,
            branch=branch,
            expected_head_commit=expected_head,
        )
        resource = _extract_resource_payload(response)

        if payload.get("auto_generate_mapping") and not mapping_spec_id:
            source_schema = extract_schema_columns(version.sample_json or dataset.schema_json)
            target_schema: List[Dict[str, Any]] = []
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

            suggestion = MappingSuggestionService().suggest_mappings(
                source_schema=source_schema,
                target_schema=target_schema,
            )
            mappings = [{"source_field": m.source_field, "target_field": m.target_field} for m in suggestion.mappings]
            if not mappings:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Auto-generated mapping is empty",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                    external_code=LegacyErrorCode.OBJECT_TYPE_AUTO_MAPPING_EMPTY,
                    extra={"class_id": class_id},
                )
            auto_body = CreateMappingSpecRequest(
                dataset_id=dataset.dataset_id,
                dataset_branch=dataset.branch,
                artifact_output_name=dataset.name,
                schema_hash=resolved_schema_hash,
                backing_datasource_id=backing.backing_id,
                backing_datasource_version_id=backing_version.version_id,
                target_class_id=class_id,
                mappings=[MappingSpecField(**m) for m in mappings],
                status="ACTIVE",
                auto_sync=True,
                options={"auto_generated": True, "mapping_source": "object_type_contract"},
            )
            mapping_response = await create_mapping_spec_service(
                body=auto_body,
                request=request,
                dataset_registry=dataset_registry,
                objectify_registry=objectify_registry,
                oms_client=oms_client,
            )
            mapping_payload = mapping_response.get("data", {}).get("mapping_spec") if isinstance(mapping_response, dict) else None
            if isinstance(mapping_payload, dict):
                resource_payload["spec"]["mapping_spec"] = {
                    "mapping_spec_id": mapping_payload.get("mapping_spec_id"),
                    "mapping_spec_version": mapping_payload.get("version"),
                    "status": mapping_payload.get("status"),
                }
                head_commit_id = await fetch_branch_head_commit_id(
                    oms_client=oms_client,
                    db_name=db_name,
                    branch=branch,
                )
                response = await oms_client.update_ontology_resource(
                    db_name,
                    resource_type="object_type",
                    resource_id=class_id,
                    payload=resource_payload,
                    branch=branch,
                    expected_head_commit=head_commit_id or expected_head_commit,
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
        try:
            detail: Any = exc.response.json()
        except Exception:
            logging.getLogger(__name__).warning("Broad exception fallback at bff/services/object_type_contract_service.py:371", exc_info=True)
            detail = exc.response.text
        raise classified_http_exception(exc.response.status_code, str(detail), code=ErrorCode.UPSTREAM_ERROR) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to create object type contract: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR)


def _normalize_field_list(raw_value: Any) -> List[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return [str(v).strip() for v in raw_value if str(v).strip()]
    if isinstance(raw_value, str):
        return [v.strip() for v in raw_value.split(",") if v.strip()]
    return []


def _normalize_field_moves(raw_value: Any) -> Dict[str, str]:
    moves: Dict[str, str] = {}
    if isinstance(raw_value, dict):
        for old_field, new_field in raw_value.items():
            if str(old_field).strip() and str(new_field).strip():
                moves[str(old_field).strip()] = str(new_field).strip()
    elif isinstance(raw_value, list):
        for item in raw_value:
            if not isinstance(item, dict):
                continue
            old_field = item.get("from") or item.get("old") or item.get("source")
            new_field = item.get("to") or item.get("new") or item.get("target")
            if str(old_field).strip() and str(new_field).strip():
                moves[str(old_field).strip()] = str(new_field).strip()
    return moves


@trace_external_call("bff.object_type_contract.update_object_type_contract")
async def update_object_type_contract(
    *,
    db_name: str,
    class_id: str,
    body: ObjectTypeContractUpdate,
    request: Request,
    branch: str,
    expected_head_commit: Optional[str],
    oms_client: OMSClient,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
) -> ApiResponse:
    try:
        class_id = str(class_id or "").strip()
        if not class_id:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "class_id is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

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
        dataset = None
        backing = None
        backing_version = None
        version = None
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
            ontology_props = _extract_ontology_property_names(ontology_payload)
            pk_spec = _normalize_and_validate_pk_spec(payload.get("pk_spec"), ontology_property_names=ontology_props)

        mapping_spec_payload = existing_spec.get("mapping_spec") if isinstance(existing_spec.get("mapping_spec"), dict) else {}
        if payload.get("mapping_spec_id") is not None:
            mapping_spec_id = str(payload.get("mapping_spec_id") or "").strip() or None
            if mapping_spec_id:
                mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
                if not mapping_spec:
                    raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Mapping spec not found", code=ErrorCode.RESOURCE_NOT_FOUND)
                if mapping_spec.target_class_id != class_id:
                    raise classified_http_exception(
                        status.HTTP_409_CONFLICT,
                        "Mapping spec target_class_id mismatch",
                        code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
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
        mapping_spec_id_value = None
        mapping_spec_version_value = None
        if isinstance(mapping_spec_payload, dict):
            mapping_spec_id_value = str(mapping_spec_payload.get("mapping_spec_id") or "").strip() or None
            mapping_spec_version_value = mapping_spec_payload.get("mapping_spec_version")
        mapping_spec_record = None

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
        id_remap_raw = (
            migration_payload.get("id_remap")
            or migration_payload.get("idRemap")
            or migration_payload.get("id_remap_plan")
            or migration_payload.get("idRemapPlan")
        )
        id_remap: Dict[str, str] = {}
        if isinstance(id_remap_raw, dict):
            for old_id, new_id in id_remap_raw.items():
                if str(old_id).strip() and str(new_id).strip():
                    id_remap[str(old_id).strip()] = str(new_id).strip()
        elif isinstance(id_remap_raw, list):
            for item in id_remap_raw:
                if not isinstance(item, dict):
                    continue
                old_id = item.get("from") or item.get("old") or item.get("source")
                new_id = item.get("to") or item.get("new") or item.get("target")
                if str(old_id).strip() and str(new_id).strip():
                    id_remap[str(old_id).strip()] = str(new_id).strip()

        reindex_required = migration_approved and backing_changed
        status_value_upper = str(status_value or "ACTIVE").strip().upper()
        if reindex_required and status_value_upper == "ACTIVE":
            if dataset is None or version is None:
                existing_backing_id = str(existing_backing.get("ref") or "").strip()
                existing_backing_version_id = str(
                    existing_backing.get("version_id")
                    or existing_backing.get("versionId")
                    or existing_backing.get("backing_version_id")
                    or ""
                ).strip()
                if existing_backing_id:
                    backing = await dataset_registry.get_backing_datasource(backing_id=existing_backing_id)
                    if backing:
                        dataset = await dataset_registry.get_dataset(dataset_id=backing.dataset_id)
                if existing_backing_version_id:
                    backing_version = await dataset_registry.get_backing_datasource_version(
                        version_id=existing_backing_version_id
                    )
                    if backing_version:
                        version = await dataset_registry.get_version(
                            version_id=backing_version.dataset_version_id
                        )
            if not mapping_spec_id_value:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Mapping spec is required for object type migration",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                    external_code=LegacyErrorCode.OBJECT_TYPE_MAPPING_SPEC_REQUIRED,
                    extra={"class_id": class_id},
                )
            mapping_spec_record = await objectify_registry.get_mapping_spec(
                mapping_spec_id=mapping_spec_id_value
            )
            if not mapping_spec_record:
                raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Mapping spec not found", code=ErrorCode.RESOURCE_NOT_FOUND)
            if mapping_spec_record.target_class_id != class_id:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Mapping spec target_class_id mismatch",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                )
            if not dataset or not version:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Object type backing version is required",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                    external_code=LegacyErrorCode.OBJECT_TYPE_BACKING_VERSION_REQUIRED,
                    extra={"class_id": class_id},
                )
            if mapping_spec_record.dataset_id != dataset.dataset_id:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Mapping spec dataset does not match object type backing dataset",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                    external_code=LegacyErrorCode.MAPPING_SPEC_DATASET_MISMATCH,
                    extra={
                        "mapping_spec_id": mapping_spec_record.mapping_spec_id,
                        "dataset_id": dataset.dataset_id,
                    },
                )
            if not getattr(version, "artifact_key", None):
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Object type backing artifact is required",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                    external_code=LegacyErrorCode.OBJECT_TYPE_BACKING_ARTIFACT_REQUIRED,
                    extra={"class_id": class_id},
                )

        edit_field_moves = _normalize_field_moves(
            migration_payload.get("edit_field_moves")
            or migration_payload.get("editFieldMoves")
            or migration_payload.get("field_moves")
            or migration_payload.get("fieldMoves")
        )
        edit_field_drops = _normalize_field_list(
            migration_payload.get("edit_field_drops")
            or migration_payload.get("editFieldDrops")
            or migration_payload.get("field_drops")
            or migration_payload.get("fieldDrops")
        )
        edit_field_invalidates = _normalize_field_list(
            migration_payload.get("edit_field_invalidates")
            or migration_payload.get("editFieldInvalidates")
            or migration_payload.get("field_invalidates")
            or migration_payload.get("fieldInvalidates")
        )
        impact_fields = sorted({*edit_field_moves.keys(), *edit_field_drops, *edit_field_invalidates})

        edit_impact = None
        edit_count = None
        if pk_changed or impact_fields:
            if impact_fields:
                edit_impact = await dataset_registry.get_instance_edit_field_stats(
                    db_name=db_name,
                    class_id=class_id,
                    fields=impact_fields,
                    status="ACTIVE",
                )
                edit_count = edit_impact.get("total") if isinstance(edit_impact, dict) else None
            if edit_count is None:
                edit_count = await dataset_registry.count_instance_edits(
                    db_name=db_name,
                    class_id=class_id,
                    status="ACTIVE",
                )
        if pk_changed:
            if edit_count and not reset_edits and not id_remap:
                await dataset_registry.record_gate_result(
                    scope="object_type_migration",
                    subject_type="object_type",
                    subject_id=class_id,
                    status="FAIL",
                    details={
                        "backing_changed": backing_changed,
                        "pk_changed": pk_changed,
                        "edit_count": edit_count,
                        "edit_impact": edit_impact,
                        "message": "PK 변경 시 편집 이력 초기화 필요",
                    },
                )
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "PK 변경 전 편집 이력 초기화가 필요합니다.",
                    code=ErrorCode.CONFLICT,
                    external_code=LegacyErrorCode.OBJECT_TYPE_EDIT_RESET_REQUIRED,
                    extra={"edit_count": edit_count},
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
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Migration approval required to change backing source or keys",
                code=ErrorCode.CONFLICT,
                external_code=LegacyErrorCode.OBJECT_TYPE_MIGRATION_REQUIRED,
                extra={"backing_changed": backing_changed, "pk_changed": pk_changed},
            )

        edit_actions: Dict[str, Any] = {}
        if migration_approved and impact_fields and not reset_edits:
            try:
                if edit_field_moves:
                    moved = await dataset_registry.apply_instance_edit_field_moves(
                        db_name=db_name,
                        class_id=class_id,
                        field_moves=edit_field_moves,
                        status="ACTIVE",
                    )
                    edit_actions["moved_edits"] = moved
                if edit_field_drops:
                    dropped = await dataset_registry.update_instance_edit_status_by_fields(
                        db_name=db_name,
                        class_id=class_id,
                        fields=edit_field_drops,
                        new_status="DROPPED",
                        status="ACTIVE",
                        metadata_note="field_drop",
                    )
                    edit_actions["dropped_edits"] = dropped
                if edit_field_invalidates:
                    invalidated = await dataset_registry.update_instance_edit_status_by_fields(
                        db_name=db_name,
                        class_id=class_id,
                        fields=edit_field_invalidates,
                        new_status="INVALID",
                        status="ACTIVE",
                        metadata_note="field_invalidate",
                    )
                    edit_actions["invalidated_edits"] = invalidated
            except Exception as exc:
                logging.getLogger(__name__).warning("Broad exception fallback at bff/services/object_type_contract_service.py:810", exc_info=True)
                edit_actions["error"] = str(exc)

        if pk_changed and id_remap and edit_count:
            try:
                remapped = await dataset_registry.remap_instance_edits(
                    db_name=db_name,
                    class_id=class_id,
                    id_map=id_remap,
                    status="ACTIVE",
                )
            except Exception:
                logging.getLogger(__name__).warning("Broad exception fallback at bff/services/object_type_contract_service.py:821", exc_info=True)
                remapped = None
            migration_payload = {
                **(migration_payload or {}),
                "id_remap": id_remap,
                "remapped_edits": remapped,
            }
        if pk_changed and reset_edits and edit_count:
            try:
                cleared = await dataset_registry.clear_instance_edits(db_name=db_name, class_id=class_id)
            except Exception:
                logging.getLogger(__name__).warning("Broad exception fallback at bff/services/object_type_contract_service.py:831", exc_info=True)
                cleared = None
            migration_payload = {
                **(migration_payload or {}),
                "reset_edits": True,
                "cleared_edits": cleared,
            }
        if impact_fields or edit_actions:
            migration_payload = {
                **(migration_payload or {}),
                "edit_policy": {
                    "moves": edit_field_moves or None,
                    "drops": edit_field_drops or None,
                    "invalidates": edit_field_invalidates or None,
                },
                "edit_impact": edit_impact,
                "edit_actions": edit_actions or None,
            }

        updated_payload = {
            "id": class_id,
            "label": existing_resource.get("label") or class_id if isinstance(existing_resource, dict) else class_id,
            "description": existing_resource.get("description") if isinstance(existing_resource, dict) else None,
            "metadata": metadata or {},
            "spec": {
                **existing_spec,
                "backing_source": backing_spec,
                "pk_spec": pk_spec,
                "mapping_spec": mapping_spec_payload,
                "status": str(status_value).upper(),
            },
        }

        expected_head = await resolve_expected_head_commit(
            oms_client=oms_client,
            db_name=db_name,
            branch=branch,
            expected_head_commit=expected_head_commit,
        )
        response = await oms_client.update_ontology_resource(
            db_name,
            resource_type="object_type",
            resource_id=class_id,
            payload=updated_payload,
            branch=branch,
            expected_head_commit=expected_head,
        )

        reindex_job_id = None
        reindex_error = None
        if reindex_required and status_value_upper == "ACTIVE":
            reason = "backing_and_pk_changed" if pk_changed else "backing_changed"
            try:
                reindex_job_id = await enqueue_objectify_job_for_mapping_spec(
                    objectify_registry=objectify_registry,
                    mapping_spec_id=mapping_spec_id_value or "",
                    mapping_spec_version=mapping_spec_version_value,
                    dataset=dataset,
                    version=version,
                    mapping_spec_record=mapping_spec_record,
                    options_override={"object_type_migration": True},
                    options_defaults={"object_type_migration_reason": reason},
                    strict_dataset_match=True,
                )
            except Exception as exc:
                reindex_error = str(exc)
                logger.warning("Failed to enqueue objectify reindex: %s", exc)

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
                    "id_remap": bool(id_remap),
                    "note": migration_payload.get("note") if migration_payload else None,
                    "reindex_job_id": reindex_job_id,
                    "reindex_error": reindex_error,
                },
            )
        if migration_payload:
            try:
                await dataset_registry.create_schema_migration_plan(
                    db_name=db_name,
                    subject_type="object_type",
                    subject_id=class_id,
                    status="APPROVED" if migration_approved else "PENDING",
                    plan={
                        **(migration_payload or {}),
                        "backing_changed": backing_changed,
                        "pk_changed": pk_changed,
                        "edit_count": edit_count,
                        "reset_edits": reset_edits,
                        "id_remap": id_remap or None,
                    },
                )
            except Exception as exc:
                logger.warning("Failed to record migration plan: %s", exc)

        resource = _extract_resource_payload(response)
        data: Dict[str, Any] = {"object_type": resource}
        if reindex_job_id or reindex_error:
            data["reindex_job_id"] = reindex_job_id
            data["reindex_error"] = reindex_error
        return ApiResponse.success(message="Object type contract updated", data=data)
    except httpx.HTTPStatusError as exc:
        try:
            detail: Any = exc.response.json()
        except Exception:
            logging.getLogger(__name__).warning("Broad exception fallback at bff/services/object_type_contract_service.py:945", exc_info=True)
            detail = exc.response.text
        raise classified_http_exception(exc.response.status_code, str(detail), code=ErrorCode.UPSTREAM_ERROR) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to update object type contract: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR)
