"""
Instance domain logic (BFF).

Extracted from `bff.routers.instances` to keep routers thin and to deduplicate
Elasticsearch/OMS/writeback overlay read paths.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import HTTPException, status
from elasticsearch.exceptions import ConnectionError as ESConnectionError, NotFoundError, RequestError

from shared.errors.error_types import ErrorCode, ErrorCategory, classified_http_exception

from bff.utils.action_log_serialization import ACTION_LOG_CLASS_ID, serialize_action_log_record
from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.config.search_config import get_instances_index_name
from shared.security.database_access import DOMAIN_MODEL_ROLES, enforce_database_role
from shared.security.input_sanitizer import (
    sanitize_es_query,
    validate_branch_name,
    validate_class_id,
    validate_db_name,
    validate_instance_id,
)
from shared.services.core.writeback_merge_service import WritebackMergeService
from shared.services.registries.action_log_registry import ActionLogRecord, ActionLogRegistry
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.storage.elasticsearch_service import ElasticsearchService
from shared.services.storage.lakefs_storage_service import create_lakefs_storage_service
from shared.services.storage.storage_service import create_storage_service
from shared.utils.access_policy import apply_access_policy
from shared.utils.writeback_lifecycle import derive_lifecycle_id, overlay_doc_id
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OverlayContext:
    writeback_enabled: bool
    resolved_overlay_branch: Optional[str]
    overlay_required: bool


def _is_action_log_class_id(class_id: str) -> bool:
    return str(class_id or "").strip().lower() == ACTION_LOG_CLASS_ID.lower()


def _action_log_as_instance(record: ActionLogRecord) -> Dict[str, Any]:
    return serialize_action_log_record(record)


def _projection_unavailable_detail(
    *,
    message: str,
    base_branch: str,
    overlay_branch: Optional[str],
    writeback_enabled: bool,
    class_id: Optional[str] = None,
    instance_id: Optional[str] = None,
) -> Dict[str, Any]:
    detail: Dict[str, Any] = {
        "error": "overlay_degraded",
        "message": message,
        "base_branch": base_branch,
        "overlay_branch": overlay_branch,
        "overlay_status": "DEGRADED",
        "writeback_enabled": writeback_enabled,
        "writeback_edits_present": None,
    }
    if class_id:
        detail["class_id"] = class_id
    if instance_id:
        detail["instance_id"] = instance_id
    return detail


async def _apply_access_policy_to_instances(
    *,
    dataset_registry: DatasetRegistry,
    db_name: str,
    class_id: str,
    instances: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], bool]:
    if not instances:
        return instances, False
    policy = await dataset_registry.get_access_policy(
        db_name=db_name,
        scope="data_access",
        subject_type="object_type",
        subject_id=class_id,
    )
    if not policy:
        return instances, False
    filtered, _ = apply_access_policy(instances, policy=policy.policy)
    return filtered, True


def _normalize_es_search_result(result: Any) -> tuple[int, List[Dict[str, Any]]]:
    """
    Normalize Elasticsearch search results across return shapes.

    Supported shapes:
    1) elasticsearch-py raw: {"hits": {"total": {"value": int}, "hits": [{"_source": {...}}]}}
    2) shared ElasticsearchService.search(): {"total": int, "hits": [{...}], "aggregations": {...}}
    """
    if not result or not isinstance(result, dict):
        return 0, []

    # shared ElasticsearchService.search() shape
    hits = result.get("hits")
    total = result.get("total")
    if isinstance(hits, list):
        try:
            total_value = int(total or 0)
        except (TypeError, ValueError):
            total_value = len(hits)
        return total_value, [h for h in hits if isinstance(h, dict)]

    # raw elasticsearch-py shape
    if isinstance(hits, dict):
        total_obj = hits.get("total") if isinstance(hits.get("total"), dict) else None
        total_value_raw = total_obj.get("value") if isinstance(total_obj, dict) else hits.get("total")
        try:
            total_value = int(total_value_raw or 0)
        except (TypeError, ValueError):
            total_value = 0

        sources: List[Dict[str, Any]] = []
        for hit in hits.get("hits") or []:
            if not isinstance(hit, dict):
                continue
            source = hit.get("_source")
            if isinstance(source, dict):
                sources.append(source)
        return total_value, sources

    return 0, []


def _resolve_overlay_context(
    *,
    db_name: str,
    class_id: str,
    overlay_branch: Optional[str],
) -> OverlayContext:
    writeback_enabled = bool(
        AppConfig.WRITEBACK_READ_OVERLAY and AppConfig.is_writeback_enabled_object_type(class_id)
    )
    requested_overlay_branch = str(overlay_branch).strip() if overlay_branch else None
    if requested_overlay_branch:
        resolved_overlay_branch = requested_overlay_branch
    elif writeback_enabled:
        resolved_overlay_branch = AppConfig.get_ontology_writeback_branch(db_name)
    else:
        resolved_overlay_branch = None
    overlay_required = writeback_enabled or bool(requested_overlay_branch)
    return OverlayContext(
        writeback_enabled=writeback_enabled,
        resolved_overlay_branch=resolved_overlay_branch,
        overlay_required=overlay_required,
    )


def _sanitize_search_query(search: Optional[str]) -> Optional[str]:
    if not search:
        return None
    if len(search) > 100:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "Search query too long (max 100 characters)",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    try:
        from shared.security.input_sanitizer import input_sanitizer

        validated_search = input_sanitizer.sanitize_string(search, max_length=100)
        return sanitize_es_query(validated_search)
    except Exception as exc:
        logger.warning("Search query security violation: %s", exc)
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "Invalid search query format",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        ) from exc


def _overlay_key_for_doc(doc: Dict[str, Any]) -> Optional[str]:
    if not isinstance(doc, dict):
        return None
    iid = doc.get("instance_id")
    if iid is None:
        return None
    iid_str = str(iid).strip()
    if not iid_str:
        return None
    lifecycle = str(doc.get("lifecycle_id") or "").strip()
    if not lifecycle:
        payload = doc.get("data")
        if isinstance(payload, dict):
            lifecycle = derive_lifecycle_id(payload)
    lifecycle = lifecycle or "lc-0"
    return overlay_doc_id(instance_id=iid_str, lifecycle_id=lifecycle)


def _merge_overlay_instances(
    *,
    base_instances: List[Dict[str, Any]],
    overlay_instances: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    overlay_by_key: Dict[str, Dict[str, Any]] = {}
    for doc in overlay_instances:
        key = _overlay_key_for_doc(doc)
        if key:
            overlay_by_key[key] = doc

    merged: List[Dict[str, Any]] = []
    for inst in base_instances:
        key = _overlay_key_for_doc(inst)
        if key and key in overlay_by_key:
            inst = overlay_by_key[key]
        if isinstance(inst, dict) and inst.get("overlay_tombstone") is True:
            continue
        merged.append(inst)

    # Include overlay-only docs that weren't in the base page (best-effort).
    base_keys = {_overlay_key_for_doc(inst) for inst in merged if isinstance(inst, dict)}
    for key, inst in overlay_by_key.items():
        if key not in base_keys and isinstance(inst, dict) and inst.get("overlay_tombstone") is not True:
            merged.append(inst)
    return merged


@trace_external_call("bff.instances.list_class_instances")
async def list_class_instances(
    *,
    db_name: str,
    class_id: str,
    request_headers: Any,
    base_branch: str,
    overlay_branch: Optional[str],
    branch: Optional[str],
    limit: int,
    offset: int,
    search: Optional[str],
    status_filter: Optional[List[str]],
    action_type_id: Optional[str],
    submitted_by: Optional[str],
    elasticsearch_service: ElasticsearchService,
    dataset_registry: DatasetRegistry,
    action_logs: Optional[ActionLogRegistry],
) -> Dict[str, Any]:
    db_name = validate_db_name(db_name)
    class_id = validate_class_id(class_id)
    resolved_base_branch = validate_branch_name(branch or base_branch or "main")

    if _is_action_log_class_id(class_id):
        try:
            await enforce_database_role(
                headers=request_headers,
                db_name=db_name,
                required_roles=DOMAIN_MODEL_ROLES,
            )
        except ValueError as exc:
            raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED) from exc

        if search:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "search is not supported for ActionLog instances",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        if not action_logs:
            raise classified_http_exception(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "ActionLogRegistry not available",
                code=ErrorCode.UPSTREAM_UNAVAILABLE,
            )

        records = await action_logs.list_logs(
            db_name=db_name,
            statuses=status_filter,
            action_type_id=action_type_id,
            submitted_by=submitted_by,
            limit=limit,
            offset=offset,
        )

        return {
            "class_id": class_id,
            "total": len(records),
            "limit": limit,
            "offset": offset,
            "search": search,
            "base_branch": resolved_base_branch,
            "overlay_branch": None,
            "overlay_status": "DISABLED",
            "writeback_enabled": False,
            "writeback_edits_present": None,
            "instances": [_action_log_as_instance(rec) for rec in records],
        }

    overlay = _resolve_overlay_context(db_name=db_name, class_id=class_id, overlay_branch=overlay_branch)
    sanitized_search = _sanitize_search_query(search)

    base_index_name = get_instances_index_name(db_name, branch=resolved_base_branch)
    overlay_index_name = (
        get_instances_index_name(db_name, branch=overlay.resolved_overlay_branch)
        if overlay.resolved_overlay_branch
        else None
    )

    query: Dict[str, Any] = {"bool": {"must": [{"term": {"class_id": class_id}}]}}
    if sanitized_search:
        query["bool"]["must"].append(
            {
                "simple_query_string": {
                    "query": sanitized_search,
                    "fields": ["*"],
                    "default_operator": "AND",
                    "analyze_wildcard": False,
                    "allow_leading_wildcard": False,
                }
            }
        )

    es_result = None
    es_error: Optional[str] = None
    overlay_result = None
    overlay_error: Optional[str] = None
    overlay_status = "DISABLED" if not overlay.resolved_overlay_branch else "ACTIVE"

    try:
        es_result = await elasticsearch_service.search(
            index=base_index_name,
            query=query,
            size=limit,
            from_=offset,
            sort=[{"event_timestamp": {"order": "desc"}}],
        )
    except (ESConnectionError, ConnectionRefusedError, TimeoutError) as exc:
        logger.warning("Elasticsearch connection failed while listing instances: %s", exc)
        es_error = "connection"
    except NotFoundError as exc:
        logger.warning("Elasticsearch index not found while listing instances: %s", exc)
        es_error = "not_found"
    except RequestError as exc:
        logger.error("Elasticsearch query error while listing instances: %s", exc)
        es_error = "query"
    except Exception as exc:
        logger.error("Unexpected Elasticsearch error while listing instances: %s", exc)
        es_error = "unknown"

    if overlay_index_name and not es_error:
        try:
            overlay_result = await elasticsearch_service.search(
                index=overlay_index_name,
                query=query,
                size=limit,
                from_=offset,
                sort=[{"event_timestamp": {"order": "desc"}}],
            )
        except NotFoundError:
            # Treat a missing overlay index as an empty overlay (no edits yet).
            overlay_result = None
        except (ESConnectionError, ConnectionRefusedError, TimeoutError):
            overlay_error = "connection"
            overlay_status = "DEGRADED"
        except RequestError:
            overlay_error = "query"
            overlay_status = "DEGRADED"
        except Exception:
            logging.getLogger(__name__).warning("Broad exception fallback at bff/services/instances_service.py:378", exc_info=True)
            overlay_error = "unknown"
            overlay_status = "DEGRADED"

    if overlay.overlay_required and overlay_status == "DEGRADED":
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Overlay index unavailable; cannot serve authoritative view.",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
            extra=_projection_unavailable_detail(
                message="Overlay index unavailable; cannot serve authoritative view.",
                class_id=class_id,
                base_branch=resolved_base_branch,
                overlay_branch=overlay.resolved_overlay_branch,
                writeback_enabled=overlay.writeback_enabled,
            ),
        )

    if es_result and not es_error:
        total, instances = _normalize_es_search_result(es_result)
        if overlay_result and not overlay_error:
            _overlay_total, overlay_hits = _normalize_es_search_result(overlay_result)
            instances = _merge_overlay_instances(base_instances=instances, overlay_instances=overlay_hits)

        instances, access_filtered = await _apply_access_policy_to_instances(
            dataset_registry=dataset_registry,
            db_name=db_name,
            class_id=class_id,
            instances=instances,
        )
        if access_filtered:
            total = len(instances)
        return {
            "class_id": class_id,
            "total": total,
            "limit": limit,
            "offset": offset,
            "search": search,
            "base_branch": resolved_base_branch,
            "overlay_branch": overlay.resolved_overlay_branch,
            "overlay_status": overlay_status,
            "writeback_enabled": overlay.writeback_enabled,
            "writeback_edits_present": None,
            "instances": instances,
        }

    if es_error:
        if overlay.overlay_required:
            raise classified_http_exception(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "Overlay index unavailable; cannot serve authoritative view.",
                code=ErrorCode.UPSTREAM_UNAVAILABLE,
                extra=_projection_unavailable_detail(
                    message="Overlay index unavailable; cannot serve authoritative view.",
                    class_id=class_id,
                    base_branch=resolved_base_branch,
                    overlay_branch=overlay.resolved_overlay_branch,
                    writeback_enabled=overlay.writeback_enabled,
                ),
            )
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Base projection unavailable; cannot serve authoritative view.",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
            extra=_projection_unavailable_detail(
                message="Base projection unavailable; cannot serve authoritative view.",
                class_id=class_id,
                base_branch=resolved_base_branch,
                overlay_branch=overlay.resolved_overlay_branch,
                writeback_enabled=overlay.writeback_enabled,
            ),
        )

    return {
        "class_id": class_id,
        "total": 0,
        "limit": limit,
        "offset": offset,
        "search": search,
        "instances": [],
    }


@trace_external_call("bff.instances.get_class_sample_values")
async def get_class_sample_values(
    *,
    db_name: str,
    class_id: str,
    property_name: Optional[str],
    base_branch: str,
    branch: Optional[str],
    limit: int,
    elasticsearch_service: ElasticsearchService,
    dataset_registry: DatasetRegistry,
) -> Dict[str, Any]:
    db_name = validate_db_name(db_name)
    class_id = validate_class_id(class_id)
    resolved_base_branch = validate_branch_name(branch or base_branch or "main")

    instances: List[Dict[str, Any]]
    index_name = get_instances_index_name(db_name, branch=resolved_base_branch)
    try:
        es_result = await elasticsearch_service.search(
            index=index_name,
            query={"bool": {"must": [{"term": {"class_id": class_id}}]}},
            size=limit,
            from_=0,
            sort=[{"event_timestamp": {"order": "desc"}}],
        )
    except (ESConnectionError, ConnectionRefusedError, TimeoutError, NotFoundError, RequestError) as exc:
        logger.warning("Elasticsearch unavailable while loading sample values for class %s: %s", class_id, exc)
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Base projection unavailable; cannot serve authoritative view.",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
            extra=_projection_unavailable_detail(
                message="Base projection unavailable; cannot serve authoritative view.",
                class_id=class_id,
                base_branch=resolved_base_branch,
                overlay_branch=None,
                writeback_enabled=False,
            ),
        ) from exc
    except Exception as exc:
        logger.error("Unexpected Elasticsearch error while loading sample values for class %s: %s", class_id, exc)
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Base projection unavailable; cannot serve authoritative view.",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
            extra=_projection_unavailable_detail(
                message="Base projection unavailable; cannot serve authoritative view.",
                class_id=class_id,
                base_branch=resolved_base_branch,
                overlay_branch=None,
                writeback_enabled=False,
            ),
        ) from exc
    _total, instances = _normalize_es_search_result(es_result)
    instances, _ = await _apply_access_policy_to_instances(
        dataset_registry=dataset_registry,
        db_name=db_name,
        class_id=class_id,
        instances=[inst for inst in instances if isinstance(inst, dict)],
    )

    def _normalize_field(name: str) -> str:
        if not name:
            return name
        if "/" in name:
            name = name.rsplit("/", 1)[-1]
        if "#" in name:
            name = name.rsplit("#", 1)[-1]
        return name

    if property_name:
        key = _normalize_field(property_name)
        values: List[Any] = []
        for inst in instances:
            if not isinstance(inst, dict):
                continue
            if key not in inst:
                continue
            v = inst.get(key)
            if isinstance(v, list):
                values.extend(v)
            else:
                values.append(v)

        return {
            "class_id": class_id,
            "property": property_name,
            "total": len(values),
            "values": values,
        }

    property_values: Dict[str, List[Any]] = {}
    exclude_keys = {"@id", "@type", "class_id", "instance_id"}
    for inst in instances:
        if not isinstance(inst, dict):
            continue
        for k, v in inst.items():
            if k in exclude_keys:
                continue
            if v is None:
                continue
            bucket = property_values.setdefault(k, [])
            if isinstance(v, list):
                bucket.extend(v)
            else:
                bucket.append(v)

    return {
        "class_id": class_id,
        "total": len(property_values),
        "property_values": property_values,
    }


async def _server_merge_fallback(
    *,
    db_name: str,
    class_id: str,
    instance_id: str,
    resolved_base_branch: str,
    resolved_overlay_branch: Optional[str],
    writeback_enabled: bool,
    dataset_registry: DatasetRegistry,
) -> Dict[str, Any]:
    settings = get_settings()
    base_storage = create_storage_service(settings)
    lakefs_storage = create_lakefs_storage_service(settings)
    if not base_storage or not lakefs_storage:
        raise RuntimeError("server_merge_unavailable")

    writeback_repo = AppConfig.ONTOLOGY_WRITEBACK_REPO
    writeback_branch = resolved_overlay_branch or AppConfig.get_ontology_writeback_branch(db_name)
    merger = WritebackMergeService(base_storage=base_storage, lakefs_storage=lakefs_storage)
    merged = await merger.merge_instance(
        db_name=db_name,
        base_branch=resolved_base_branch,
        overlay_branch=resolved_overlay_branch or writeback_branch,
        class_id=class_id,
        instance_id=instance_id,
        writeback_repo=writeback_repo,
        writeback_branch=writeback_branch,
    )

    filtered, _ = await _apply_access_policy_to_instances(
        dataset_registry=dataset_registry,
        db_name=db_name,
        class_id=class_id,
        instances=[merged.document],
    )
    if not filtered:
        raise classified_http_exception(
            status.HTTP_404_NOT_FOUND,
            f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
            code=ErrorCode.RESOURCE_NOT_FOUND,
        )

    if merged.overlay_tombstone:
        raise classified_http_exception(
            status.HTTP_404_NOT_FOUND,
            f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
            code=ErrorCode.RESOURCE_NOT_FOUND,
            extra={
                "base_branch": resolved_base_branch,
                "overlay_branch": resolved_overlay_branch,
                "overlay_status": "DEGRADED",
                "writeback_enabled": writeback_enabled,
                "writeback_edits_present": merged.writeback_edits_present,
            },
        )

    return {
        "status": "success",
        "base_branch": resolved_base_branch,
        "overlay_branch": resolved_overlay_branch,
        "overlay_status": "DEGRADED",
        "writeback_enabled": writeback_enabled,
        "writeback_edits_present": merged.writeback_edits_present,
        "data": filtered[0],
    }


@trace_external_call("bff.instances.get_instance_detail")
async def get_instance_detail(
    *,
    db_name: str,
    class_id: str,
    instance_id: str,
    request_headers: Any,
    base_branch: str,
    overlay_branch: Optional[str],
    branch: Optional[str],
    elasticsearch_service: ElasticsearchService,
    dataset_registry: DatasetRegistry,
    action_logs: Optional[ActionLogRegistry],
) -> Dict[str, Any]:
    db_name = validate_db_name(db_name)
    class_id = validate_class_id(class_id)
    instance_id = validate_instance_id(instance_id)
    resolved_base_branch = validate_branch_name(branch or base_branch or "main")

    if _is_action_log_class_id(class_id):
        try:
            await enforce_database_role(
                headers=request_headers,
                db_name=db_name,
                required_roles=DOMAIN_MODEL_ROLES,
            )
        except ValueError as exc:
            raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED) from exc

        try:
            action_log_uuid = UUID(str(instance_id))
        except Exception as exc:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "Invalid ActionLog UUID",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            ) from exc

        if not action_logs:
            raise classified_http_exception(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "ActionLogRegistry not available",
                code=ErrorCode.UPSTREAM_UNAVAILABLE,
            )

        record = await action_logs.get_log(action_log_id=str(action_log_uuid))
        if not record or record.db_name != db_name:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "ActionLog not found", code=ErrorCode.RESOURCE_NOT_FOUND)

        return {"status": "success", "data": _action_log_as_instance(record)}

    overlay = _resolve_overlay_context(db_name=db_name, class_id=class_id, overlay_branch=overlay_branch)
    base_index_name = get_instances_index_name(db_name, branch=resolved_base_branch)
    overlay_index_name = (
        get_instances_index_name(db_name, branch=overlay.resolved_overlay_branch)
        if overlay.resolved_overlay_branch
        else None
    )

    query: Dict[str, Any] = {
        "bool": {
            "must": [
                {"term": {"instance_id": instance_id}},
                {"term": {"class_id": class_id}},
            ]
        }
    }

    overlay_status = "DISABLED" if not overlay.resolved_overlay_branch else "ACTIVE"

    try:
        result = await elasticsearch_service.search(
            index=base_index_name,
            query=query,
            size=1,
        )

        _total, sources = _normalize_es_search_result(result)
        if sources:
            base_doc = sources[0]
            lifecycle_id = str(base_doc.get("lifecycle_id") or "").strip() if isinstance(base_doc, dict) else ""
            if not lifecycle_id and isinstance(base_doc, dict) and isinstance(base_doc.get("data"), dict):
                lifecycle_id = derive_lifecycle_id(base_doc["data"])
            lifecycle_id = lifecycle_id or "lc-0"

            if overlay_index_name:
                overlay_id = overlay_doc_id(instance_id=instance_id, lifecycle_id=lifecycle_id)
                overlay_doc = await elasticsearch_service.get_document(overlay_index_name, overlay_id)
                if overlay_doc is None:
                    # Back-compat: older overlay docs used `_id == instance_id`.
                    try:
                        overlay_result = await elasticsearch_service.search(
                            index=overlay_index_name,
                            query={
                                "bool": {
                                    "must": [
                                        {"term": {"instance_id": instance_id}},
                                        {"term": {"class_id": class_id}},
                                        {"term": {"lifecycle_id": lifecycle_id}},
                                    ]
                                }
                            },
                            size=1,
                        )
                        _overlay_total, overlay_sources = _normalize_es_search_result(overlay_result)
                        if overlay_sources:
                            overlay_doc = overlay_sources[0]
                    except NotFoundError:
                        overlay_doc = None

                if isinstance(overlay_doc, dict) and overlay_doc.get("overlay_tombstone") is True:
                    raise classified_http_exception(
                        status.HTTP_404_NOT_FOUND,
                        f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                        code=ErrorCode.RESOURCE_NOT_FOUND,
                        extra={
                            "base_branch": resolved_base_branch,
                            "overlay_branch": overlay.resolved_overlay_branch,
                            "overlay_status": overlay_status,
                            "writeback_enabled": overlay.writeback_enabled,
                            "writeback_edits_present": None,
                        },
                    )

                if isinstance(overlay_doc, dict):
                    filtered_overlay, _ = await _apply_access_policy_to_instances(
                        dataset_registry=dataset_registry,
                        db_name=db_name,
                        class_id=class_id,
                        instances=[overlay_doc],
                    )
                    if filtered_overlay:
                        return {
                            "status": "success",
                            "base_branch": resolved_base_branch,
                            "overlay_branch": overlay.resolved_overlay_branch,
                            "overlay_status": overlay_status,
                            "writeback_enabled": overlay.writeback_enabled,
                            "writeback_edits_present": None,
                            "data": filtered_overlay[0],
                        }

            filtered, _ = await _apply_access_policy_to_instances(
                dataset_registry=dataset_registry,
                db_name=db_name,
                class_id=class_id,
                instances=[base_doc],
            )
            if not filtered:
                raise classified_http_exception(
                    status.HTTP_404_NOT_FOUND,
                    f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                    code=ErrorCode.RESOURCE_NOT_FOUND,
                )
            return {
                "status": "success",
                "base_branch": resolved_base_branch,
                "overlay_branch": overlay.resolved_overlay_branch,
                "overlay_status": overlay_status,
                "writeback_enabled": overlay.writeback_enabled,
                "writeback_edits_present": None,
                "data": filtered[0],
            }

        if overlay.writeback_enabled:
            try:
                return await _server_merge_fallback(
                    db_name=db_name,
                    class_id=class_id,
                    instance_id=instance_id,
                    resolved_base_branch=resolved_base_branch,
                    resolved_overlay_branch=overlay.resolved_overlay_branch,
                    writeback_enabled=overlay.writeback_enabled,
                    dataset_registry=dataset_registry,
                )
            except FileNotFoundError:
                raise classified_http_exception(
                    status.HTTP_404_NOT_FOUND,
                    f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                    code=ErrorCode.RESOURCE_NOT_FOUND,
                    extra={
                        "base_branch": resolved_base_branch,
                        "overlay_branch": overlay.resolved_overlay_branch,
                        "overlay_status": "DEGRADED",
                        "writeback_enabled": overlay.writeback_enabled,
                        "writeback_edits_present": None,
                    },
                )
            except HTTPException:
                raise
            except Exception:
                raise classified_http_exception(
                    status.HTTP_503_SERVICE_UNAVAILABLE,
                    "Base projection missing; cannot serve authoritative view for writeback-enabled types.",
                    code=ErrorCode.UPSTREAM_UNAVAILABLE,
                    extra=_projection_unavailable_detail(
                        message="Base projection missing; cannot serve authoritative view for writeback-enabled types.",
                        class_id=class_id,
                        instance_id=instance_id,
                        base_branch=resolved_base_branch,
                        overlay_branch=overlay.resolved_overlay_branch,
                        writeback_enabled=overlay.writeback_enabled,
                    ),
                )

        raise classified_http_exception(
            status.HTTP_404_NOT_FOUND,
            f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
            code=ErrorCode.RESOURCE_NOT_FOUND,
        )

    except HTTPException:
        raise
    except (ESConnectionError, ConnectionRefusedError, TimeoutError) as exc:
        if overlay.overlay_required:
            try:
                return await _server_merge_fallback(
                    db_name=db_name,
                    class_id=class_id,
                    instance_id=instance_id,
                    resolved_base_branch=resolved_base_branch,
                    resolved_overlay_branch=overlay.resolved_overlay_branch,
                    writeback_enabled=overlay.writeback_enabled,
                    dataset_registry=dataset_registry,
                )
            except FileNotFoundError:
                raise classified_http_exception(
                    status.HTTP_404_NOT_FOUND,
                    f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                    code=ErrorCode.RESOURCE_NOT_FOUND,
                ) from exc
            except HTTPException:
                raise
            except Exception:
                raise classified_http_exception(
                    status.HTTP_503_SERVICE_UNAVAILABLE,
                    "Overlay index unavailable; cannot serve authoritative view.",
                    code=ErrorCode.UPSTREAM_UNAVAILABLE,
                    extra=_projection_unavailable_detail(
                        message="Overlay index unavailable; cannot serve authoritative view.",
                        class_id=class_id,
                        instance_id=instance_id,
                        base_branch=resolved_base_branch,
                        overlay_branch=overlay.resolved_overlay_branch,
                        writeback_enabled=overlay.writeback_enabled,
                    ),
                ) from exc
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Base projection unavailable; cannot serve authoritative view.",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
            extra=_projection_unavailable_detail(
                message="Base projection unavailable; cannot serve authoritative view.",
                class_id=class_id,
                instance_id=instance_id,
                base_branch=resolved_base_branch,
                overlay_branch=overlay.resolved_overlay_branch,
                writeback_enabled=overlay.writeback_enabled,
            ),
        ) from exc
    except NotFoundError as exc:
        if overlay.overlay_required:
            try:
                return await _server_merge_fallback(
                    db_name=db_name,
                    class_id=class_id,
                    instance_id=instance_id,
                    resolved_base_branch=resolved_base_branch,
                    resolved_overlay_branch=overlay.resolved_overlay_branch,
                    writeback_enabled=overlay.writeback_enabled,
                    dataset_registry=dataset_registry,
                )
            except FileNotFoundError:
                raise classified_http_exception(
                    status.HTTP_404_NOT_FOUND,
                    f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                    code=ErrorCode.RESOURCE_NOT_FOUND,
                ) from exc
            except HTTPException:
                raise
            except Exception:
                raise classified_http_exception(
                    status.HTTP_503_SERVICE_UNAVAILABLE,
                    "Overlay index unavailable; cannot serve authoritative view.",
                    code=ErrorCode.UPSTREAM_UNAVAILABLE,
                    extra=_projection_unavailable_detail(
                        message="Overlay index unavailable; cannot serve authoritative view.",
                        class_id=class_id,
                        instance_id=instance_id,
                        base_branch=resolved_base_branch,
                        overlay_branch=overlay.resolved_overlay_branch,
                        writeback_enabled=overlay.writeback_enabled,
                    ),
                ) from exc
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Base projection unavailable; cannot serve authoritative view.",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
            extra=_projection_unavailable_detail(
                message="Base projection unavailable; cannot serve authoritative view.",
                class_id=class_id,
                instance_id=instance_id,
                base_branch=resolved_base_branch,
                overlay_branch=overlay.resolved_overlay_branch,
                writeback_enabled=overlay.writeback_enabled,
            ),
        ) from exc
    except RequestError as exc:
        if overlay.overlay_required:
            try:
                return await _server_merge_fallback(
                    db_name=db_name,
                    class_id=class_id,
                    instance_id=instance_id,
                    resolved_base_branch=resolved_base_branch,
                    resolved_overlay_branch=overlay.resolved_overlay_branch,
                    writeback_enabled=overlay.writeback_enabled,
                    dataset_registry=dataset_registry,
                )
            except FileNotFoundError:
                raise classified_http_exception(
                    status.HTTP_404_NOT_FOUND,
                    f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                    code=ErrorCode.RESOURCE_NOT_FOUND,
                ) from exc
            except HTTPException:
                raise
            except Exception:
                raise classified_http_exception(
                    status.HTTP_503_SERVICE_UNAVAILABLE,
                    "Overlay index unavailable; cannot serve authoritative view.",
                    code=ErrorCode.UPSTREAM_UNAVAILABLE,
                    extra=_projection_unavailable_detail(
                        message="Overlay index unavailable; cannot serve authoritative view.",
                        class_id=class_id,
                        instance_id=instance_id,
                        base_branch=resolved_base_branch,
                        overlay_branch=overlay.resolved_overlay_branch,
                        writeback_enabled=overlay.writeback_enabled,
                    ),
                ) from exc
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Base projection unavailable; cannot serve authoritative view.",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
            extra=_projection_unavailable_detail(
                message="Base projection unavailable; cannot serve authoritative view.",
                class_id=class_id,
                instance_id=instance_id,
                base_branch=resolved_base_branch,
                overlay_branch=overlay.resolved_overlay_branch,
                writeback_enabled=overlay.writeback_enabled,
            ),
        ) from exc
    except Exception as exc:
        if overlay.overlay_required:
            try:
                return await _server_merge_fallback(
                    db_name=db_name,
                    class_id=class_id,
                    instance_id=instance_id,
                    resolved_base_branch=resolved_base_branch,
                    resolved_overlay_branch=overlay.resolved_overlay_branch,
                    writeback_enabled=overlay.writeback_enabled,
                    dataset_registry=dataset_registry,
                )
            except FileNotFoundError:
                raise classified_http_exception(
                    status.HTTP_404_NOT_FOUND,
                    f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                    code=ErrorCode.RESOURCE_NOT_FOUND,
                ) from exc
            except HTTPException:
                raise
            except Exception:
                raise classified_http_exception(
                    status.HTTP_503_SERVICE_UNAVAILABLE,
                    "Overlay index unavailable; cannot serve authoritative view.",
                    code=ErrorCode.UPSTREAM_UNAVAILABLE,
                    extra=_projection_unavailable_detail(
                        message="Overlay index unavailable; cannot serve authoritative view.",
                        class_id=class_id,
                        instance_id=instance_id,
                        base_branch=resolved_base_branch,
                        overlay_branch=overlay.resolved_overlay_branch,
                        writeback_enabled=overlay.writeback_enabled,
                    ),
                ) from exc
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Base projection unavailable; cannot serve authoritative view.",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
            extra=_projection_unavailable_detail(
                message="Base projection unavailable; cannot serve authoritative view.",
                class_id=class_id,
                instance_id=instance_id,
                base_branch=resolved_base_branch,
                overlay_branch=overlay.resolved_overlay_branch,
                writeback_enabled=overlay.writeback_enabled,
            ),
        ) from exc
