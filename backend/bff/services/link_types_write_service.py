"""Link type write service (BFF).

Extracted from `bff.routers.link_types_write` to keep routers thin, to
deduplicate create/update flows, and to centralize OMS + registry error mapping
(Facade pattern).
"""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict, Optional
from uuid import uuid4

import httpx
from fastapi import HTTPException, Request, status

from bff.schemas.link_types_requests import LinkTypeRequest, LinkTypeUpdateRequest
from bff.services.link_types_mapping_service import (
    build_mapping_request,
    extract_ontology_properties,
    extract_ontology_relationships,
    normalize_spec_type,
    resolve_object_type_contract,
)
from bff.services.input_validation_service import (
    sanitized_payload,
    validated_branch_name,
    validated_db_name,
)
from bff.services.oms_client import OMSClient
from bff.services.ontology_occ_guard_service import resolve_expected_head_commit
from bff.utils.httpx_exceptions import raise_httpx_as_http_exception
from shared.models.requests import ApiResponse
from shared.security.database_access import DATA_ENGINEER_ROLES, DOMAIN_MODEL_ROLES
from shared.security.input_sanitizer import SecurityViolationError
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry

logger = logging.getLogger(__name__)

EnforceRoleFn = Callable[[Request], Awaitable[None]]
CreateMappingSpecFn = Callable[..., Awaitable[Dict[str, Any]]]
EnqueueObjectifyJobFn = Callable[..., Awaitable[Optional[str]]]


async def _require_role(
    *,
    request: Request,
    db_name: str,
    roles: Any,
    enforce_role: Callable[..., Awaitable[None]],
) -> None:
    await enforce_role(request, db_name=db_name, roles=roles)


def _require_non_empty(value: Any, *, detail: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)
    return text


def _extract_mapping_payload(mapping_response: Dict[str, Any]) -> Dict[str, Any]:
    mapping_payload = mapping_response.get("data", {}).get("mapping_spec")
    if not isinstance(mapping_payload, dict):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create mapping spec",
        )
    return mapping_payload


async def _enqueue_link_index_job(
    *,
    enqueue_job: EnqueueObjectifyJobFn,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    mapping_spec_id: str,
    mapping_spec_version: int,
    dataset_id: str,
    dataset_version_id: Optional[str],
) -> Optional[str]:
    return await enqueue_job(
        objectify_registry=objectify_registry,
        mapping_spec_id=mapping_spec_id,
        mapping_spec_version=mapping_spec_version,
        dataset_registry=dataset_registry,
        dataset_id=dataset_id,
        dataset_version_id=dataset_version_id,
    )


async def create_link_type(
    *,
    db_name: str,
    body: LinkTypeRequest,
    request: Request,
    branch: str,
    expected_head_commit: Optional[str],
    oms_client: OMSClient,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    enforce_role: Callable[..., Awaitable[None]],
    create_mapping_spec: CreateMappingSpecFn,
    enqueue_job: EnqueueObjectifyJobFn,
) -> ApiResponse:
    try:
        db_name = validated_db_name(db_name)
        branch = validated_branch_name(branch)
        await _require_role(request=request, db_name=db_name, roles=DOMAIN_MODEL_ROLES, enforce_role=enforce_role)
        payload = sanitized_payload(body.model_dump(by_alias=True))

        link_type_id = _require_non_empty(payload.get("id"), detail="id is required")
        source_class = _require_non_empty(payload.get("from"), detail="from/to are required")
        target_class = _require_non_empty(payload.get("to"), detail="from/to are required")
        predicate = _require_non_empty(payload.get("predicate"), detail="predicate is required")
        cardinality = _require_non_empty(payload.get("cardinality"), detail="cardinality is required")

        spec_payload = payload.get("relationship_spec") if isinstance(payload.get("relationship_spec"), dict) else {}
        spec_type = normalize_spec_type(str(spec_payload.get("type") or ""))
        normalized_type = "join_table" if spec_type == "object_backed" else spec_type
        if normalized_type not in {"foreign_key", "join_table"}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="relationship_spec.type is required")

        try:
            source_ontology = await oms_client.get_ontology(db_name, source_class, branch=branch)
            target_ontology = await oms_client.get_ontology(db_name, target_class, branch=branch)
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == status.HTTP_404_NOT_FOUND:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ontology class not found") from exc
            raise_httpx_as_http_exception(exc)
            raise  # pragma: no cover

        source_props = extract_ontology_properties(source_ontology)
        source_rels = extract_ontology_relationships(source_ontology)
        target_props = extract_ontology_properties(target_ontology)
        if predicate not in source_rels:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"code": "LINK_TYPE_PREDICATE_MISSING", "predicate": predicate},
            )

        _, source_contract = await resolve_object_type_contract(
            oms_client=oms_client,
            db_name=db_name,
            class_id=source_class,
            branch=branch,
        )
        _, target_contract = await resolve_object_type_contract(
            oms_client=oms_client,
            db_name=db_name,
            class_id=target_class,
            branch=branch,
        )

        relationship_spec_id = str(uuid4())
        mapping_request, _, resolved_dataset_version_id, resolved_spec_type = await build_mapping_request(
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
            spec_payload=spec_payload,
        )

        mapping_response = await create_mapping_spec(
            body=mapping_request,
            request=request,
            dataset_registry=dataset_registry,
            objectify_registry=objectify_registry,
            oms_client=oms_client,
        )
        mapping_payload = _extract_mapping_payload(mapping_response)
        mapping_spec_id = _require_non_empty(mapping_payload.get("mapping_spec_id"), detail="mapping_spec_id is required")
        mapping_spec_version = int(mapping_payload.get("version") or mapping_payload.get("mapping_spec_version") or 1)

        resolved_spec_payload = dict(spec_payload)
        if resolved_spec_type in {"join_table", "object_backed"}:
            resolved_spec_payload.setdefault("join_dataset_id", mapping_request.dataset_id)
            if resolved_dataset_version_id:
                resolved_spec_payload.setdefault("join_dataset_version_id", resolved_dataset_version_id)
        if resolved_spec_type == "object_backed":
            relationship_object_type = str(resolved_spec_payload.get("relationship_object_type") or "").strip()
            if relationship_object_type:
                resolved_spec_payload.setdefault("relationship_object_type", relationship_object_type)

        relationship_spec_summary = {
            **resolved_spec_payload,
            "relationship_spec_id": relationship_spec_id,
            "spec_type": resolved_spec_type,
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
                "relationship_spec_type": resolved_spec_type,
                "mapping_spec_id": mapping_spec_id,
                "mapping_spec_version": mapping_spec_version,
                "relationship_spec": relationship_spec_summary,
                "status": payload.get("status") or "ACTIVE",
            },
        }

        resolved_expected_head = await resolve_expected_head_commit(
            oms_client=oms_client,
            db_name=db_name,
            branch=branch,
            expected_head_commit=expected_head_commit,
        )

        try:
            link_response = await oms_client.create_ontology_resource(
                db_name,
                resource_type="link_type",
                payload=link_payload,
                branch=branch,
                expected_head_commit=resolved_expected_head,
            )
        except httpx.HTTPStatusError as exc:
            raise_httpx_as_http_exception(exc)
            raise  # pragma: no cover
        link_resource = link_response.get("data") if isinstance(link_response, dict) else link_response

        relationship_record = await dataset_registry.create_relationship_spec(
            relationship_spec_id=relationship_spec_id,
            link_type_id=link_type_id,
            db_name=db_name,
            source_object_type=source_class,
            target_object_type=target_class,
            predicate=predicate,
            spec_type=resolved_spec_type,
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
                enqueue_job=enqueue_job,
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

    except httpx.HTTPStatusError as exc:
        raise_httpx_as_http_exception(exc)
    except (SecurityViolationError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to create link type: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def update_link_type(
    *,
    db_name: str,
    link_type_id: str,
    body: LinkTypeUpdateRequest,
    request: Request,
    branch: str,
    expected_head_commit: Optional[str],
    oms_client: OMSClient,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    enforce_role: Callable[..., Awaitable[None]],
    create_mapping_spec: CreateMappingSpecFn,
    enqueue_job: EnqueueObjectifyJobFn,
) -> ApiResponse:
    try:
        db_name = validated_db_name(db_name)
        branch = validated_branch_name(branch)
        await _require_role(request=request, db_name=db_name, roles=DOMAIN_MODEL_ROLES, enforce_role=enforce_role)
        link_type_id = _require_non_empty(link_type_id, detail="link_type_id is required")

        existing = await dataset_registry.get_relationship_spec(link_type_id=link_type_id)
        if not existing:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Relationship spec not found")

        payload = sanitized_payload(body.model_dump(exclude_unset=True))
        relationship_spec_payload = (
            payload.get("relationship_spec") if isinstance(payload.get("relationship_spec"), dict) else None
        )

        try:
            link_resource_payload = await oms_client.get_ontology_resource(
                db_name,
                resource_type="link_type",
                resource_id=link_type_id,
                branch=branch,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == status.HTTP_404_NOT_FOUND:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Link type not found") from exc
            raise_httpx_as_http_exception(exc)
            raise  # pragma: no cover
        link_resource = link_resource_payload.get("data") if isinstance(link_resource_payload, dict) else link_resource_payload
        link_spec = link_resource.get("spec") if isinstance(link_resource, dict) else {}
        existing_cardinality = str(link_spec.get("cardinality") or "").strip()

        mapping_spec_id = existing.mapping_spec_id
        mapping_spec_version = existing.mapping_spec_version
        updated_spec = existing.spec
        resolved_dataset_id = existing.dataset_id
        resolved_dataset_version_id = existing.dataset_version_id

        if relationship_spec_payload is not None:
            try:
                source_ontology = await oms_client.get_ontology(db_name, existing.source_object_type, branch=branch)
                target_ontology = await oms_client.get_ontology(db_name, existing.target_object_type, branch=branch)
            except httpx.HTTPStatusError as exc:
                if exc.response is not None and exc.response.status_code == status.HTTP_404_NOT_FOUND:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ontology class not found") from exc
                raise_httpx_as_http_exception(exc)
                raise  # pragma: no cover

            source_props = extract_ontology_properties(source_ontology)
            target_props = extract_ontology_properties(target_ontology)

            _, source_contract = await resolve_object_type_contract(
                oms_client=oms_client,
                db_name=db_name,
                class_id=existing.source_object_type,
                branch=branch,
            )
            _, target_contract = await resolve_object_type_contract(
                oms_client=oms_client,
                db_name=db_name,
                class_id=existing.target_object_type,
                branch=branch,
            )

            mapping_request, resolved_dataset_id, resolved_dataset_version_id, resolved_spec_type = await build_mapping_request(
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

            mapping_response = await create_mapping_spec(
                body=mapping_request,
                request=request,
                dataset_registry=dataset_registry,
                objectify_registry=objectify_registry,
                oms_client=oms_client,
            )
            mapping_payload = _extract_mapping_payload(mapping_response)
            mapping_spec_id = _require_non_empty(mapping_payload.get("mapping_spec_id"), detail="mapping_spec_id is required")
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

        resolved_expected_head = await resolve_expected_head_commit(
            oms_client=oms_client,
            db_name=db_name,
            branch=branch,
            expected_head_commit=expected_head_commit,
        )

        updated_resource = await oms_client.update_ontology_resource(
            db_name,
            resource_type="link_type",
            resource_id=link_type_id,
            payload=resource,
            branch=branch,
            expected_head_commit=resolved_expected_head,
        )

        if payload.get("trigger_index"):
            await _enqueue_link_index_job(
                enqueue_job=enqueue_job,
                dataset_registry=dataset_registry,
                objectify_registry=objectify_registry,
                mapping_spec_id=mapping_spec_id,
                mapping_spec_version=mapping_spec_version,
                dataset_id=updated.dataset_id,
                dataset_version_id=updated.dataset_version_id,
            )

        return ApiResponse.success(
            message="Link type updated",
            data={
                "link_type": updated_resource.get("data") if isinstance(updated_resource, dict) else updated_resource,
                "relationship_spec": updated.__dict__,
            },
        )

    except httpx.HTTPStatusError as exc:
        raise_httpx_as_http_exception(exc)
    except (SecurityViolationError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to update link type: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def reindex_link_type(
    *,
    db_name: str,
    link_type_id: str,
    request: Request,
    dataset_version_id: Optional[str],
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    enforce_role: Callable[..., Awaitable[None]],
    enqueue_job: EnqueueObjectifyJobFn,
) -> ApiResponse:
    try:
        db_name = validated_db_name(db_name)
        await _require_role(request=request, db_name=db_name, roles=DATA_ENGINEER_ROLES, enforce_role=enforce_role)
        link_type_id = _require_non_empty(link_type_id, detail="link_type_id is required")

        record = await dataset_registry.get_relationship_spec(link_type_id=link_type_id)
        if not record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Relationship spec not found")

        job_id = await _enqueue_link_index_job(
            enqueue_job=enqueue_job,
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

    except (SecurityViolationError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to reindex link type: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
