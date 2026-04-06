"""
Objectify mapping spec service (BFF).

This module provides a stable, reusable application service for creating
mapping specs. It is used by multiple routers (e.g. objectify + object types)
to avoid router-to-router coupling and duplicated validation logic.
"""

import logging
from typing import Any, Dict, List, Optional

import httpx
from fastapi import HTTPException, Request, status

from bff.routers.objectify_deps import _require_db_role
from bff.services.objectify_ops_service import _build_mapping_change_summary
from bff.schemas.objectify_requests import CreateMappingSpecRequest
from bff.services.oms_client import OMSClient
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.errors.external_codes import ExternalErrorCode
from shared.models.responses import ApiResponse
from shared.security.auth_utils import enforce_db_scope
from shared.security.database_access import DOMAIN_MODEL_ROLES
from shared.security.input_sanitizer import sanitize_input, validate_class_id
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.utils.import_type_normalization import normalize_import_target_type, resolve_import_type
from shared.utils.key_spec import normalize_key_spec, normalize_object_type_key_spec
from shared.utils.ontology_fields import extract_ontology_fields
from shared.utils.object_type_backing import select_primary_backing_source
from shared.utils.payload_utils import unwrap_data_payload
from shared.utils.schema_columns import extract_schema_column_names
from shared.utils.schema_hash import compute_schema_hash_from_sample
from shared.utils.string_list_utils import normalize_string_list
from shared.observability.tracing import trace_db_operation

logger = logging.getLogger(__name__)


@trace_db_operation("bff.objectify_mapping_spec.create_mapping_spec")
async def create_mapping_spec(
    *,
    body: CreateMappingSpecRequest,
    request: Request,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    oms_client: OMSClient,
) -> Dict[str, Any]:
    """
    Create an objectify mapping spec with full validation.

    Returns an ApiResponse payload (dict) to keep router behavior stable.
    """
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
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        try:
            enforce_db_scope(request.headers, db_name=dataset.db_name)
        except ValueError as exc:
            raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED) from exc
        await _require_db_role(request, db_name=dataset.db_name, roles=DOMAIN_MODEL_ROLES)

        dataset_branch = str(payload.get("dataset_branch") or dataset.branch or "main").strip() or "main"
        artifact_output_name = str(payload.get("artifact_output_name") or dataset.name or "").strip()
        if not artifact_output_name:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "artifact_output_name is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        backing_datasource_id = str(payload.get("backing_datasource_id") or "").strip() or None
        backing_datasource_version_id = str(payload.get("backing_datasource_version_id") or "").strip() or None
        backing = None
        backing_version = None
        if backing_datasource_version_id:
            backing_version = await dataset_registry.get_backing_datasource_version(version_id=backing_datasource_version_id)
            if not backing_version:
                raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Backing datasource version not found", code=ErrorCode.RESOURCE_NOT_FOUND)
            backing = await dataset_registry.get_backing_datasource(backing_id=backing_version.backing_id)
            if not backing or backing.dataset_id != dataset_id:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Backing datasource version does not match dataset", code=ErrorCode.OBJECTIFY_CONTRACT_ERROR)
        if backing_datasource_id and not backing:
            backing = await dataset_registry.get_backing_datasource(backing_id=backing_datasource_id)
            if not backing:
                raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Backing datasource not found", code=ErrorCode.RESOURCE_NOT_FOUND)
            if backing.dataset_id != dataset_id:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Backing datasource does not match dataset", code=ErrorCode.OBJECTIFY_CONTRACT_ERROR)
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
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Mapping spec backing schema mismatch",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                    external_code=ExternalErrorCode.MAPPING_SPEC_BACKING_SCHEMA_MISMATCH,
                    extra={"expected": backing_version.schema_hash, "provided": schema_hash},
                )
            schema_hash = backing_version.schema_hash
            schema_version = await dataset_registry.get_version(version_id=backing_version.dataset_version_id)
            if not schema_version:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "Backing datasource version dataset is missing", code=ErrorCode.OBJECTIFY_CONTRACT_ERROR)
        else:
            schema_version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
            if not schema_version:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Dataset version is required for mapping spec validation",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                )
            if not schema_hash:
                schema_hash = compute_schema_hash_from_sample(schema_version.sample_json)
                if not schema_hash:
                    schema_hash = compute_schema_hash_from_sample(dataset.schema_json)
            if not schema_hash:
                raise classified_http_exception(status.HTTP_409_CONFLICT, "schema_hash is required for mapping spec", code=ErrorCode.OBJECTIFY_CONTRACT_ERROR)
            backing_version = await dataset_registry.get_or_create_backing_datasource_version(
                backing_id=backing.backing_id,
                dataset_version_id=schema_version.version_id,
                schema_hash=schema_hash,
            )

        schema_columns = extract_schema_column_names(schema_version.sample_json if schema_version else dataset.schema_json)
        if not schema_columns:
            schema_columns = extract_schema_column_names(dataset.schema_json)
        if not schema_columns:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Dataset schema columns are required for mapping spec validation",
                code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
            )
        mappings = payload.get("mappings") or []
        if not isinstance(mappings, list) or not mappings:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "mappings is required", code=ErrorCode.OBJECTIFY_MAPPING_ERROR)

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
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "Mapping spec source fields are missing",
                code=ErrorCode.OBJECTIFY_MAPPING_ERROR,
                external_code=ExternalErrorCode.MAPPING_SPEC_SOURCE_MISSING,
                extra={"missing_sources": sorted(set(missing_sources))},
            )

        ontology_branch = str((options or {}).get("ontology_branch") or dataset_branch or dataset.branch or "main").strip() or "main"
        ontology_payload = await oms_client.get_ontology(dataset.db_name, target_class_id, branch=ontology_branch)
        prop_map, rel_map = extract_ontology_fields(ontology_payload)
        if not prop_map:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Target ontology class schema is required for mapping spec validation",
                code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
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
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Object type contract missing",
                code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                external_code=ExternalErrorCode.OBJECT_TYPE_CONTRACT_MISSING,
                extra={"class_id": target_class_id},
            )
        object_type_resource = unwrap_data_payload(object_type_payload)
        if isinstance(object_type_resource, dict):
            object_type_spec = object_type_resource.get("spec") if isinstance(object_type_resource.get("spec"), dict) else {}
        status_value = str(object_type_spec.get("status") or "ACTIVE").strip().upper()
        if status_value != "ACTIVE":
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Object type is inactive",
                code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                external_code=ExternalErrorCode.OBJECT_TYPE_INACTIVE,
                extra={"status": status_value},
            )
        normalized_pk_spec = normalize_object_type_key_spec(object_type_spec, columns=list(prop_map.keys()))
        if not normalized_pk_spec.get("primary_key"):
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Object type primary key is missing",
                code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                external_code=ExternalErrorCode.OBJECT_TYPE_PRIMARY_KEY_MISSING,
                extra={"class_id": target_class_id},
            )
        if not normalized_pk_spec.get("title_key"):
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Object type title key is missing",
                code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                external_code=ExternalErrorCode.OBJECT_TYPE_TITLE_KEY_MISSING,
                extra={"class_id": target_class_id},
            )

        options = options or {}
        link_index_mode = str(options.get("mode") or options.get("job_type") or "").strip().lower() == "link_index"

        backing_source = select_primary_backing_source(object_type_spec)
        backing_kind = str(backing_source.get("kind") or "").strip().lower()
        backing_ref = str(backing_source.get("ref") or "").strip()
        backing_schema = str(backing_source.get("schema_hash") or backing_source.get("schemaHash") or "").strip()
        if backing_kind and not link_index_mode:
            if backing_kind in {"backing_datasource", "backing-datasource", "backingdatasource"}:
                if backing_ref and backing and backing_ref != backing.backing_id:
                    raise classified_http_exception(
                        status.HTTP_409_CONFLICT,
                        "Object type backing datasource mismatch",
                        code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                        external_code=ExternalErrorCode.OBJECT_TYPE_BACKING_MISMATCH,
                        extra={"expected": backing_ref, "observed": backing.backing_id},
                    )
            elif backing_kind in {"dataset", "dataset_id", "dataset-id"}:
                if backing_ref and backing_ref != dataset.dataset_id:
                    raise classified_http_exception(
                        status.HTTP_409_CONFLICT,
                        "Object type backing dataset mismatch",
                        code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                        external_code=ExternalErrorCode.OBJECT_TYPE_BACKING_DATASET_MISMATCH,
                        extra={"expected": backing_ref, "observed": dataset.dataset_id},
                    )
        if backing_schema and schema_hash and backing_schema != schema_hash and not link_index_mode:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Object type schema hash mismatch",
                code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                external_code=ExternalErrorCode.OBJECT_TYPE_SCHEMA_HASH_MISMATCH,
                extra={"expected": backing_schema, "observed": schema_hash},
            )

        unknown_targets = [t for t in mapped_targets if t not in prop_map and t not in rel_map]
        relationship_targets = [t for t in mapped_targets if t in rel_map]
        if unknown_targets:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "Unknown mapping spec target fields",
                code=ErrorCode.OBJECTIFY_MAPPING_ERROR,
                external_code=ExternalErrorCode.MAPPING_SPEC_TARGET_UNKNOWN,
                extra={"missing_targets": sorted(set(unknown_targets))},
            )
        if not link_index_mode:
            unsupported_relationships: List[str] = []
            for target in relationship_targets:
                rel = rel_map.get(target) or {}
                cardinality = str(rel.get("cardinality") or "").strip().lower()
                if cardinality.endswith(":n") or cardinality.endswith(":m"):
                    unsupported_relationships.append(target)
            if unsupported_relationships:
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    "Unsupported relationship cardinality for mapping targets",
                    code=ErrorCode.OBJECTIFY_MAPPING_ERROR,
                    external_code=ExternalErrorCode.MAPPING_SPEC_RELATIONSHIP_CARDINALITY_UNSUPPORTED,
                    extra={"targets": sorted(set(unsupported_relationships))},
                )
        elif not relationship_targets:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "link_index requires relationship targets",
                code=ErrorCode.OBJECTIFY_MAPPING_ERROR,
                external_code=ExternalErrorCode.MAPPING_SPEC_RELATIONSHIP_REQUIRED,
            )

        required_fields = {name for name, meta in prop_map.items() if bool(meta.get("required"))}
        explicit_pk = {name for name, meta in prop_map.items() if bool(meta.get("primary_key") or meta.get("primaryKey"))}
        object_type_pk_targets = [str(item).strip() for item in normalized_pk_spec.get("primary_key") or [] if str(item).strip()]
        object_type_title_targets = [str(item).strip() for item in normalized_pk_spec.get("title_key") or [] if str(item).strip()]
        required_fields.update({str(item).strip() for item in normalized_pk_spec.get("required_fields") or [] if str(item).strip()})
        if link_index_mode:
            required_fields = set()
        if not explicit_pk:
            expected_pk = f"{target_class_id.lower()}_id"
            if expected_pk in prop_map:
                explicit_pk.add(expected_pk)

        pk_targets = options.get("primary_key_targets") or options.get("target_primary_keys")
        pk_targets = normalize_string_list(pk_targets) if pk_targets is not None else None
        if not pk_targets:
            pk_targets = object_type_pk_targets or sorted(explicit_pk)
        if object_type_pk_targets and set(pk_targets) != set(object_type_pk_targets):
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "Mapping spec primary key targets mismatch",
                code=ErrorCode.OBJECTIFY_MAPPING_ERROR,
                external_code=ExternalErrorCode.MAPPING_SPEC_PRIMARY_KEY_MISMATCH,
                extra={"expected": object_type_pk_targets, "observed": pk_targets},
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
                    raise classified_http_exception(
                        status.HTTP_400_BAD_REQUEST,
                        "Dataset primary key source fields are missing in mapping spec",
                        code=ErrorCode.OBJECTIFY_MAPPING_ERROR,
                        external_code=ExternalErrorCode.MAPPING_SPEC_DATASET_PK_MISSING,
                        extra={"missing_sources": missing_pk_sources},
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
                    raise classified_http_exception(
                        status.HTTP_400_BAD_REQUEST,
                        "Dataset primary key target mapping mismatch",
                        code=ErrorCode.OBJECTIFY_MAPPING_ERROR,
                        external_code=ExternalErrorCode.MAPPING_SPEC_DATASET_PK_TARGET_MISMATCH,
                        extra={"targets": invalid_pk_targets},
                    )
            required_sources = set(normalized_spec.get("required_fields") or [])
            if required_sources:
                missing_required_sources = sorted(required_sources - mapping_source_set)
                if missing_required_sources:
                    raise classified_http_exception(
                        status.HTTP_400_BAD_REQUEST,
                        "Required source fields are missing in mapping spec",
                        code=ErrorCode.OBJECTIFY_MAPPING_ERROR,
                        external_code=ExternalErrorCode.MAPPING_SPEC_REQUIRED_SOURCE_MISSING,
                        extra={"missing_sources": missing_required_sources},
                    )
            options.setdefault("key_spec_id", key_spec.key_spec_id)

        if not link_index_mode:
            missing_required = sorted(required_fields - set(mapped_targets))
            if missing_required:
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    "Required target fields are missing in mapping spec",
                    code=ErrorCode.OBJECTIFY_MAPPING_ERROR,
                    external_code=ExternalErrorCode.MAPPING_SPEC_REQUIRED_MISSING,
                    extra={"missing_targets": missing_required},
                )
            missing_title = sorted(set(object_type_title_targets) - set(mapped_targets))
            if missing_title:
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    "Title key targets are missing in mapping spec",
                    code=ErrorCode.OBJECTIFY_MAPPING_ERROR,
                    external_code=ExternalErrorCode.MAPPING_SPEC_TITLE_KEY_MISSING,
                    extra={"missing_targets": missing_title},
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
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    "Unique key targets are missing in mapping spec",
                    code=ErrorCode.OBJECTIFY_MAPPING_ERROR,
                    external_code=ExternalErrorCode.MAPPING_SPEC_UNIQUE_KEY_MISSING,
                    extra={"missing_targets": missing_unique},
                )
        if not pk_targets:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "primary_key_targets is required when no primary_key is defined on target class", code=ErrorCode.OBJECTIFY_MAPPING_ERROR)
        missing_pk = sorted(set(pk_targets) - set(mapped_targets))
        if missing_pk:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "Primary key targets are missing in mapping spec",
                code=ErrorCode.OBJECTIFY_MAPPING_ERROR,
                external_code=ExternalErrorCode.MAPPING_SPEC_PRIMARY_KEY_MISSING,
                extra={"missing_targets": missing_pk},
            )

        resolved_field_types: Dict[str, str] = {}
        unsupported_types: List[str] = []
        for target in mapped_targets:
            if target in rel_map:
                continue
            prop = prop_map.get(target) or {}
            raw_type = prop.get("type") or prop.get("data_type") or prop.get("datatype")
            is_relationship = bool(raw_type == "link" or prop.get("isRelationship") or prop.get("target") or prop.get("linkTarget"))
            items = prop.get("items") if isinstance(prop, dict) else None
            if isinstance(items, dict):
                item_type = items.get("type")
                if item_type == "link" and (items.get("target") or items.get("linkTarget")):
                    is_relationship = True
            if is_relationship:
                unsupported_types.append(target)
                continue
            import_type = resolve_import_type(raw_type)
            if not import_type:
                unsupported_types.append(target)
                continue
            resolved_field_types[target] = import_type
        if unsupported_types:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "Unsupported ontology target types in mapping spec",
                code=ErrorCode.OBJECTIFY_MAPPING_ERROR,
                external_code=ExternalErrorCode.MAPPING_SPEC_UNSUPPORTED_TYPES,
                extra={"targets": sorted(set(unsupported_types))},
            )

        # NOTE: Palantir Foundry style — no type compatibility blocking at mapping
        # spec creation time.  Type coercion is done at objectify/write time via
        # coerce_value() in sheet_import_service.py.  This avoids false-positive
        # rejections (e.g. integer CSV column → string ontology property).

        if target_field_types:
            mismatches: List[Dict[str, Any]] = []
            for target in mapped_targets:
                if target in rel_map:
                    continue
                provided = target_field_types.get(target)
                expected = resolved_field_types.get(target)
                if not provided:
                    mismatches.append({"target_field": target, "reason": "missing", "expected": expected})
                else:
                    normalized = normalize_import_target_type(provided)
                    if expected and normalized != expected:
                        mismatches.append({"target_field": target, "reason": "mismatch", "expected": expected, "provided": provided})
            if mismatches:
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    "Provided target field types do not match resolved ontology types",
                    code=ErrorCode.OBJECTIFY_MAPPING_ERROR,
                    external_code=ExternalErrorCode.MAPPING_SPEC_TARGET_TYPE_MISMATCH,
                    extra={"mismatches": mismatches},
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
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc
