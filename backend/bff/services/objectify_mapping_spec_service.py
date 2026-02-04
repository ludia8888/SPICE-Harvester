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
from bff.services.objectify_ops_service import (
    _ALLOWED_SOURCE_TYPES,
    _build_mapping_change_summary,
    _compute_schema_hash_from_sample,
    _extract_ontology_fields,
    _extract_schema_columns,
    _extract_schema_types,
    _is_type_compatible,
    _resolve_import_type,
    _unwrap_data_payload,
)
from bff.schemas.objectify_requests import CreateMappingSpecRequest
from bff.services.oms_client import OMSClient
from shared.models.requests import ApiResponse
from shared.security.auth_utils import enforce_db_scope
from shared.security.database_access import DOMAIN_MODEL_ROLES
from shared.security.input_sanitizer import sanitize_input, validate_class_id
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.utils.import_type_normalization import normalize_import_target_type
from shared.utils.key_spec import normalize_key_spec

logger = logging.getLogger(__name__)


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
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
        try:
            enforce_db_scope(request.headers, db_name=dataset.db_name)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
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
            backing_version = await dataset_registry.get_backing_datasource_version(version_id=backing_datasource_version_id)
            if not backing_version:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backing datasource version not found")
            backing = await dataset_registry.get_backing_datasource(backing_id=backing_version.backing_id)
            if not backing or backing.dataset_id != dataset_id:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Backing datasource version does not match dataset")
        if backing_datasource_id and not backing:
            backing = await dataset_registry.get_backing_datasource(backing_id=backing_datasource_id)
            if not backing:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backing datasource not found")
            if backing.dataset_id != dataset_id:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Backing datasource does not match dataset")
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
            schema_version = await dataset_registry.get_version(version_id=backing_version.dataset_version_id)
            if not schema_version:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Backing datasource version dataset is missing")
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
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="schema_hash is required for mapping spec")
            backing_version = await dataset_registry.get_or_create_backing_datasource_version(
                backing_id=backing.backing_id,
                dataset_version_id=schema_version.version_id,
                schema_hash=schema_hash,
            )

        schema_columns = _extract_schema_columns(schema_version.sample_json if schema_version else dataset.schema_json)
        if not schema_columns:
            schema_columns = _extract_schema_columns(dataset.schema_json)
        if not schema_columns:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Dataset schema columns are required for mapping spec validation",
            )
        schema_types = _extract_schema_types(schema_version.sample_json if schema_version else dataset.schema_json)
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
        ontology_payload = await oms_client.get_ontology(dataset.db_name, target_class_id, branch=ontology_branch)
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
        object_type_resource = _unwrap_data_payload(object_type_payload)
        if isinstance(object_type_resource, dict):
            object_type_spec = object_type_resource.get("spec") if isinstance(object_type_resource.get("spec"), dict) else {}
        status_value = str(object_type_spec.get("status") or "ACTIVE").strip().upper()
        if status_value != "ACTIVE":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail={"code": "OBJECT_TYPE_INACTIVE", "status": status_value})
        normalized_pk_spec = normalize_key_spec(object_type_spec.get("pk_spec") or {}, columns=list(prop_map.keys()))
        if not normalized_pk_spec.get("primary_key"):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail={"code": "OBJECT_TYPE_PRIMARY_KEY_MISSING", "class_id": target_class_id})
        if not normalized_pk_spec.get("title_key"):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail={"code": "OBJECT_TYPE_TITLE_KEY_MISSING", "class_id": target_class_id})

        options = options or {}
        link_index_mode = str(options.get("mode") or options.get("job_type") or "").strip().lower() == "link_index"

        backing_source = object_type_spec.get("backing_source") if isinstance(object_type_spec.get("backing_source"), dict) else {}
        backing_kind = str(backing_source.get("kind") or "").strip().lower()
        backing_ref = str(backing_source.get("ref") or "").strip()
        backing_schema = str(backing_source.get("schema_hash") or backing_source.get("schemaHash") or "").strip()
        if backing_kind and not link_index_mode:
            if backing_kind in {"backing_datasource", "backing-datasource", "backingdatasource"}:
                if backing_ref and backing and backing_ref != backing.backing_id:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={"code": "OBJECT_TYPE_BACKING_MISMATCH", "expected": backing_ref, "observed": backing.backing_id},
                    )
            elif backing_kind in {"dataset", "dataset_id", "dataset-id"}:
                if backing_ref and backing_ref != dataset.dataset_id:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={"code": "OBJECT_TYPE_BACKING_DATASET_MISMATCH", "expected": backing_ref, "observed": dataset.dataset_id},
                    )
        if backing_schema and schema_hash and backing_schema != schema_hash and not link_index_mode:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"code": "OBJECT_TYPE_SCHEMA_HASH_MISMATCH", "expected": backing_schema, "observed": schema_hash},
            )

        unknown_targets = [t for t in mapped_targets if t not in prop_map and t not in rel_map]
        relationship_targets = [t for t in mapped_targets if t in rel_map]
        if unknown_targets:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "MAPPING_SPEC_TARGET_UNKNOWN", "missing_targets": sorted(set(unknown_targets))},
            )
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
                    detail={"code": "MAPPING_SPEC_RELATIONSHIP_CARDINALITY_UNSUPPORTED", "targets": sorted(set(unsupported_relationships))},
                )
        elif not relationship_targets:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "MAPPING_SPEC_RELATIONSHIP_REQUIRED", "message": "link_index requires relationship targets"},
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
                detail={"code": "MAPPING_SPEC_PRIMARY_KEY_MISMATCH", "expected": object_type_pk_targets, "observed": pk_targets},
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
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "MAPPING_SPEC_DATASET_PK_MISSING", "missing_sources": missing_pk_sources})
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
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "MAPPING_SPEC_DATASET_PK_TARGET_MISMATCH", "targets": invalid_pk_targets})
            required_sources = set(normalized_spec.get("required_fields") or [])
            if required_sources:
                missing_required_sources = sorted(required_sources - mapping_source_set)
                if missing_required_sources:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "MAPPING_SPEC_REQUIRED_SOURCE_MISSING", "missing_sources": missing_required_sources})
            options.setdefault("key_spec_id", key_spec.key_spec_id)

        if not link_index_mode:
            missing_required = sorted(required_fields - set(mapped_targets))
            if missing_required:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "MAPPING_SPEC_REQUIRED_MISSING", "missing_targets": missing_required})
            missing_title = sorted(set(object_type_title_targets) - set(mapped_targets))
            if missing_title:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "MAPPING_SPEC_TITLE_KEY_MISSING", "missing_targets": missing_title})
            unique_key_fields = {
                str(field).strip()
                for key_fields in (normalized_pk_spec.get("unique_keys") or [])
                if isinstance(key_fields, list)
                for field in key_fields
                if str(field).strip()
            }
            missing_unique = sorted(unique_key_fields - set(mapped_targets))
            if missing_unique:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "MAPPING_SPEC_UNIQUE_KEY_MISSING", "missing_targets": missing_unique})
        if not pk_targets:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="primary_key_targets is required when no primary_key is defined on target class")
        missing_pk = sorted(set(pk_targets) - set(mapped_targets))
        if missing_pk:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "MAPPING_SPEC_PRIMARY_KEY_MISSING", "missing_targets": missing_pk})

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
            import_type = _resolve_import_type(raw_type)
            if not import_type:
                unsupported_types.append(target)
                continue
            resolved_field_types[target] = import_type
        if unsupported_types:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "MAPPING_SPEC_UNSUPPORTED_TYPES", "targets": sorted(set(unsupported_types))})

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
                unsupported_source_types.append({"source_field": source_field, "source_type": source_type})
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
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "MAPPING_SPEC_SOURCE_TYPE_UNKNOWN", "missing_sources": sorted(set(missing_source_types))})
        if unsupported_source_types:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "MAPPING_SPEC_SOURCE_TYPE_UNSUPPORTED", "sources": unsupported_source_types})
        if incompatible_types:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "MAPPING_SPEC_TYPE_INCOMPATIBLE", "mismatches": incompatible_types})

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
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "MAPPING_SPEC_TARGET_TYPE_MISMATCH", "mismatches": mismatches})

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
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
