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
from shared.models.responses import ApiResponse
from shared.security.auth_utils import enforce_db_scope
from shared.security.input_sanitizer import sanitize_input
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.utils.key_spec import derive_key_spec_from_properties, normalize_key_spec
from shared.utils.ontology_fields import list_ontology_properties
from shared.utils.object_type_backing import list_backing_sources, select_primary_backing_source
from shared.utils.payload_utils import unwrap_data_payload
from shared.utils.schema_columns import extract_schema_columns
from shared.utils.schema_hash import compute_schema_hash
from shared.utils.string_list_utils import normalize_string_list
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.errors.external_codes import ExternalErrorCode
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)

_CONTRACT_STATUS_ACTIVE = "ACTIVE"
_CONTRACT_STATUS_DRAFT = "DRAFT"


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


def _normalize_contract_status(raw_status: Any, *, default: str = _CONTRACT_STATUS_ACTIVE) -> str:
    status_value = str(raw_status or default).strip().upper()
    return status_value or default


def _is_effective_backing_source(backing: Any) -> bool:
    if not isinstance(backing, dict) or not backing:
        return False
    return bool(
        str(backing.get("dataset_id") or "").strip()
        or str(backing.get("ref") or "").strip()
        or str(backing.get("kind") or "").strip()
    )


def _extract_backing_sources(spec: Any) -> List[Dict[str, Any]]:
    return list_backing_sources(spec)


def _extract_primary_backing_source(spec: Any) -> Dict[str, Any]:
    return select_primary_backing_source(spec)


def _build_backing_source(
    *,
    dataset: Any,
    backing: Any,
    backing_version: Any,
    version: Any,
    resolved_schema_hash: str,
) -> Dict[str, Any]:
    return {
        "kind": "backing_datasource",
        "ref": backing.backing_id,
        "version_id": backing_version.version_id,
        "schema_hash": resolved_schema_hash,
        "dataset_id": dataset.dataset_id,
        "dataset_version_id": version.version_id,
        "artifact_key": version.artifact_key,
        "branch": dataset.branch,
    }


def _apply_backing_hints_from_sources(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    normalized = dict(payload)
    if any(
        str(normalized.get(key) or "").strip()
        for key in (
            "backing_dataset_id",
            "backing_datasource_id",
            "backing_datasource_version_id",
            "dataset_version_id",
            "schema_hash",
        )
    ):
        return normalized

    source_candidates = list_backing_sources(normalized)
    if not source_candidates:
        return normalized

    source = source_candidates[0]
    normalized["backing_dataset_id"] = (
        str(source.get("backing_dataset_id") or source.get("dataset_id") or "").strip() or None
    )
    normalized["backing_datasource_id"] = (
        str(source.get("backing_datasource_id") or source.get("ref") or "").strip() or None
    )
    normalized["backing_datasource_version_id"] = (
        str(
            source.get("backing_datasource_version_id")
            or source.get("version_id")
            or source.get("backing_version_id")
            or ""
        ).strip()
        or None
    )
    normalized["dataset_version_id"] = str(source.get("dataset_version_id") or "").strip() or None
    normalized["schema_hash"] = str(source.get("schema_hash") or source.get("schemaHash") or "").strip() or None
    return normalized


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
    names: set[str] = set()
    for prop in list_ontology_properties(payload):
        name = str(prop.get("name") or prop.get("id") or "").strip()
        if name:
            names.add(name)
    return names


def _infer_pk_spec_from_ontology(
    *,
    ontology_payload: Any,
    class_id: str,
) -> Dict[str, Any]:
    properties = list_ontology_properties(ontology_payload)
    if not properties:
        return {}

    names: List[str] = []
    normalized_properties: List[Dict[str, Any]] = []
    for entry in properties:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or entry.get("id") or "").strip()
        if not name:
            continue
        names.append(name)
        normalized_entry = dict(entry)
        normalized_entry["name"] = name
        normalized_properties.append(normalized_entry)

    if not names:
        return {}

    derived_key_spec = derive_key_spec_from_properties(normalized_properties)
    primary_candidates = list(derived_key_spec.get("primary_key") or [])
    title_candidates = list(derived_key_spec.get("title_key") or [])

    primary = primary_candidates[:]
    if not primary:
        expected_pk = f"{class_id.lower()}_id".strip() if class_id else ""
        if expected_pk and expected_pk in names:
            primary = [expected_pk]
        elif "id" in names:
            primary = ["id"]
        else:
            primary = [names[0]]

    title = title_candidates[:]
    if not title:
        if "name" in names:
            title = ["name"]
        elif primary:
            title = [primary[0]]

    if not primary or not title:
        return {}
    return {
        "primary_key": primary,
        "title_key": title,
    }


def _build_target_schema_from_ontology(ontology_payload: Any) -> List[Dict[str, Any]]:
    target_schema: List[Dict[str, Any]] = []
    for prop in list_ontology_properties(ontology_payload):
        name = str(prop.get("name") or prop.get("id") or "").strip()
        if not name:
            continue
        target_schema.append({"name": name, "type": prop.get("type")})
    return target_schema


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
            external_code=ExternalErrorCode.OBJECT_TYPE_KEY_FIELDS_MISSING,
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
        payload = _apply_backing_hints_from_sources(
            sanitize_input(body.model_dump(exclude_unset=True))
        )
        class_id = str(payload.get("class_id") or "").strip()
        if not class_id:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "class_id is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        ontology_payload = await oms_client.get_ontology(db_name, class_id, branch=branch)
        if not ontology_payload:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Ontology class not found", code=ErrorCode.ONTOLOGY_NOT_FOUND)

        status_value = _normalize_contract_status(payload.get("status"))
        active_contract = status_value == _CONTRACT_STATUS_ACTIVE
        mapping_requested = bool(str(payload.get("mapping_spec_id") or "").strip())
        auto_generate_mapping = bool(payload.get("auto_generate_mapping"))
        backing_hints_present = any(
            str(payload.get(key) or "").strip()
            for key in (
                "backing_dataset_id",
                "backing_datasource_id",
                "backing_datasource_version_id",
                "dataset_version_id",
                "schema_hash",
            )
        )

        dataset = None
        backing = None
        backing_version = None
        version = None
        resolved_schema_hash = None
        if active_contract or mapping_requested or auto_generate_mapping or backing_hints_present:
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
        raw_pk_spec = payload.get("pk_spec")
        if isinstance(raw_pk_spec, dict) and raw_pk_spec:
            pk_spec = _normalize_and_validate_pk_spec(raw_pk_spec, ontology_property_names=ontology_props)
        elif active_contract:
            pk_spec = _normalize_and_validate_pk_spec(raw_pk_spec, ontology_property_names=ontology_props)
        else:
            inferred_pk_spec = _infer_pk_spec_from_ontology(
                ontology_payload=ontology_payload,
                class_id=class_id,
            )
            pk_spec = (
                _normalize_and_validate_pk_spec(inferred_pk_spec, ontology_property_names=ontology_props)
                if inferred_pk_spec
                else {}
            )

        mapping_spec_id = str(payload.get("mapping_spec_id") or "").strip() or None
        mapping_spec_version = payload.get("mapping_spec_version")
        mapping_spec_payload = None
        if mapping_spec_id:
            if not dataset:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Mapping spec requires backing dataset context",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                )
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

        backing_source = (
            _build_backing_source(
                dataset=dataset,
                backing=backing,
                backing_version=backing_version,
                version=version,
                resolved_schema_hash=resolved_schema_hash or "",
            )
            if dataset and backing and backing_version and version and resolved_schema_hash
            else {}
        )
        backing_sources = [backing_source] if _is_effective_backing_source(backing_source) else []

        resource_payload: Dict[str, Any] = {
            "id": class_id,
            "label": (ontology_payload.get("label") or class_id) if isinstance(ontology_payload, dict) else class_id,
            "description": ontology_payload.get("description") if isinstance(ontology_payload, dict) else None,
            "metadata": payload.get("metadata") or {},
            "spec": {
                "backing_source": backing_source,
                "backing_sources": backing_sources,
                "pk_spec": pk_spec,
                "mapping_spec": mapping_spec_payload or {},
                "status": status_value,
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
        resource = unwrap_data_payload(response)

        if auto_generate_mapping and status_value != _CONTRACT_STATUS_ACTIVE:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Auto-generated mapping requires ACTIVE object type contract",
                code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
            )
        if auto_generate_mapping and not mapping_spec_id:
            if not dataset or not backing or not backing_version or not version or not resolved_schema_hash:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Backing dataset and version are required for auto mapping generation",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                )
            source_schema = extract_schema_columns(version.sample_json or dataset.schema_json)
            target_schema = _build_target_schema_from_ontology(ontology_payload)

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
                    external_code=ExternalErrorCode.OBJECT_TYPE_AUTO_MAPPING_EMPTY,
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
                resource = unwrap_data_payload(response)

        data: Dict[str, Any] = {
            "object_type": resource,
        }
        if backing is not None:
            data["backing_datasource"] = backing.__dict__
        if backing_version is not None:
            data["backing_datasource_version"] = backing_version.__dict__

        return ApiResponse.success(
            message="Object type contract created",
            data=data,
        )
    except httpx.HTTPStatusError as exc:
        try:
            detail: Any = exc.response.json()
        except Exception:
            logging.getLogger(__name__).warning("Exception fallback at bff/services/object_type_contract_service.py:371", exc_info=True)
            detail = exc.response.text
        raise classified_http_exception(exc.response.status_code, str(detail), code=ErrorCode.UPSTREAM_ERROR) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to create object type contract: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR)


def _normalize_field_list(raw_value: Any) -> List[str]:
    return normalize_string_list(raw_value)


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


def _has_effective_pk_spec(spec: Dict[str, Any]) -> bool:
    if not isinstance(spec, dict):
        return False
    normalized = normalize_key_spec(spec.get("pk_spec") or {})
    return bool(normalized.get("primary_key")) and bool(normalized.get("title_key"))


def _has_effective_backing_source(spec: Dict[str, Any]) -> bool:
    if not isinstance(spec, dict):
        return False
    for source in _extract_backing_sources(spec):
        if _is_effective_backing_source(source):
            return True
    return False


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
        existing_resource = unwrap_data_payload(existing_response)
        existing_spec = existing_resource.get("spec") if isinstance(existing_resource, dict) else {}
        if not isinstance(existing_spec, dict):
            existing_spec = {}

        payload = _apply_backing_hints_from_sources(
            sanitize_input(body.model_dump(exclude_unset=True))
        )
        dataset = None
        backing = None
        backing_version = None
        version = None
        backing_spec = _extract_primary_backing_source(existing_spec)
        backing_sources_spec = _extract_backing_sources(existing_spec)
        backing_payload_keys = (
            "backing_dataset_id",
            "backing_datasource_id",
            "backing_datasource_version_id",
            "dataset_version_id",
            "schema_hash",
        )
        backing_inputs_provided = any(key in payload for key in backing_payload_keys)
        if backing_inputs_provided:
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
            backing_spec = _build_backing_source(
                dataset=dataset,
                backing=backing,
                backing_version=backing_version,
                version=version,
                resolved_schema_hash=resolved_schema_hash,
            )
            backing_sources_spec = [backing_spec]

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

        status_value = _normalize_contract_status(payload.get("status") or existing_spec.get("status") or _CONTRACT_STATUS_ACTIVE)
        if status_value == _CONTRACT_STATUS_ACTIVE and not _is_effective_backing_source(backing_spec):
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Backing source is required when status is ACTIVE",
                code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
            )
        if status_value == _CONTRACT_STATUS_ACTIVE:
            normalized_pk = normalize_key_spec(pk_spec if isinstance(pk_spec, dict) else {})
            if not normalized_pk.get("primary_key") or not normalized_pk.get("title_key"):
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "pk_spec.primary_key and pk_spec.title_key are required when status is ACTIVE",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                )
        metadata = payload.get("metadata") if payload.get("metadata") is not None else existing_resource.get("metadata")
        mapping_spec_id_value = None
        mapping_spec_version_value = None
        if isinstance(mapping_spec_payload, dict):
            mapping_spec_id_value = str(mapping_spec_payload.get("mapping_spec_id") or "").strip() or None
            mapping_spec_version_value = mapping_spec_payload.get("mapping_spec_version")
        mapping_spec_record = None

        existing_backing = _extract_primary_backing_source(existing_spec)
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

        bootstrap_contract_seed = not _has_effective_backing_source(existing_spec) and not _has_effective_pk_spec(
            existing_spec
        )
        migration_backing_changed = backing_changed and not bootstrap_contract_seed
        migration_pk_changed = pk_changed and not bootstrap_contract_seed

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

        reindex_required = migration_approved and migration_backing_changed
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
                    external_code=ExternalErrorCode.OBJECT_TYPE_MAPPING_SPEC_REQUIRED,
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
                    external_code=ExternalErrorCode.OBJECT_TYPE_BACKING_VERSION_REQUIRED,
                    extra={"class_id": class_id},
                )
            if mapping_spec_record.dataset_id != dataset.dataset_id:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Mapping spec dataset does not match object type backing dataset",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                    external_code=ExternalErrorCode.MAPPING_SPEC_DATASET_MISMATCH,
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
                    external_code=ExternalErrorCode.OBJECT_TYPE_BACKING_ARTIFACT_REQUIRED,
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
        if migration_pk_changed or impact_fields:
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
        if migration_pk_changed:
            if edit_count and not reset_edits and not id_remap:
                await dataset_registry.record_gate_result(
                    scope="object_type_migration",
                    subject_type="object_type",
                    subject_id=class_id,
                    status="FAIL",
                    details={
                        "backing_changed": migration_backing_changed,
                        "pk_changed": migration_pk_changed,
                        "bootstrap_contract_seed": bootstrap_contract_seed,
                        "edit_count": edit_count,
                        "edit_impact": edit_impact,
                        "message": "PK 변경 시 편집 이력 초기화 필요",
                    },
                )
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "PK 변경 전 편집 이력 초기화가 필요합니다.",
                    code=ErrorCode.CONFLICT,
                    external_code=ExternalErrorCode.OBJECT_TYPE_EDIT_RESET_REQUIRED,
                    extra={"edit_count": edit_count},
                )
        if (migration_backing_changed or migration_pk_changed) and not migration_approved:
            await dataset_registry.record_gate_result(
                scope="object_type_migration",
                subject_type="object_type",
                subject_id=class_id,
                status="FAIL",
                details={
                    "backing_changed": migration_backing_changed,
                    "pk_changed": migration_pk_changed,
                    "bootstrap_contract_seed": bootstrap_contract_seed,
                    "message": "Migration approval required to change backing source or keys",
                },
            )
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Migration approval required to change backing source or keys",
                code=ErrorCode.CONFLICT,
                external_code=ExternalErrorCode.OBJECT_TYPE_MIGRATION_REQUIRED,
                extra={"backing_changed": migration_backing_changed, "pk_changed": migration_pk_changed},
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
                logging.getLogger(__name__).warning("Exception fallback at bff/services/object_type_contract_service.py:810", exc_info=True)
                edit_actions["error"] = str(exc)

        if migration_pk_changed and id_remap and edit_count:
            try:
                remapped = await dataset_registry.remap_instance_edits(
                    db_name=db_name,
                    class_id=class_id,
                    id_map=id_remap,
                    status="ACTIVE",
                )
            except Exception:
                logging.getLogger(__name__).warning("Exception fallback at bff/services/object_type_contract_service.py:821", exc_info=True)
                remapped = None
            migration_payload = {
                **(migration_payload or {}),
                "id_remap": id_remap,
                "remapped_edits": remapped,
            }
        if migration_pk_changed and reset_edits and edit_count:
            try:
                cleared = await dataset_registry.clear_instance_edits(db_name=db_name, class_id=class_id)
            except Exception:
                logging.getLogger(__name__).warning("Exception fallback at bff/services/object_type_contract_service.py:831", exc_info=True)
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
                "backing_sources": backing_sources_spec,
                "pk_spec": pk_spec,
                "mapping_spec": mapping_spec_payload,
                "status": status_value,
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
            reason = "backing_and_pk_changed" if migration_pk_changed else "backing_changed"
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

        if migration_backing_changed or migration_pk_changed:
            await dataset_registry.record_gate_result(
                scope="object_type_migration",
                subject_type="object_type",
                subject_id=class_id,
                status="PASS",
                details={
                    "backing_changed": migration_backing_changed,
                    "pk_changed": migration_pk_changed,
                    "bootstrap_contract_seed": bootstrap_contract_seed,
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
                        "backing_changed": migration_backing_changed,
                        "pk_changed": migration_pk_changed,
                        "bootstrap_contract_seed": bootstrap_contract_seed,
                        "edit_count": edit_count,
                        "reset_edits": reset_edits,
                        "id_remap": id_remap or None,
                    },
                )
            except Exception as exc:
                logger.warning("Failed to record migration plan: %s", exc)

        resource = unwrap_data_payload(response)
        data: Dict[str, Any] = {"object_type": resource}
        if reindex_job_id or reindex_error:
            data["reindex_job_id"] = reindex_job_id
            data["reindex_error"] = reindex_error
        return ApiResponse.success(message="Object type contract updated", data=data)
    except httpx.HTTPStatusError as exc:
        try:
            detail: Any = exc.response.json()
        except Exception:
            logging.getLogger(__name__).warning("Exception fallback at bff/services/object_type_contract_service.py:945", exc_info=True)
            detail = exc.response.text
        raise classified_http_exception(exc.response.status_code, str(detail), code=ErrorCode.UPSTREAM_ERROR) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to update object type contract: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR)
