"""Admin projection recompute service (BFF).

Extracted from `bff.routers.admin_recompute_projection` to keep routers thin and
to isolate projection-specific indexing behavior behind small Strategy objects.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Literal, Optional, Protocol
from uuid import uuid4

from fastapi import BackgroundTasks, Request, status
from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.admin_task_monitor import monitor_admin_task
from bff.schemas.admin_projection_requests import RecomputeProjectionRequest, RecomputeProjectionResponse
from shared.config.search_config import (
    get_default_index_settings,
    get_instances_index_name,
    get_ontologies_index_name,
)
from shared.dependencies.providers import BackgroundTaskManagerDep
from shared.models.background_task import TaskStatus
from shared.models.lineage_edge_types import EDGE_EVENT_MATERIALIZED_ES_DOCUMENT
from shared.security.input_sanitizer import validate_branch_name, validate_db_name
from shared.services.registries.lineage_store import LineageStore
from shared.services.storage.elasticsearch_service import ElasticsearchService, promote_alias_to_index
from shared.services.storage.event_store import EventStore
from shared.services.storage.redis_service import RedisService
from shared.utils.language import coerce_localized_text, get_default_language, select_localized_text
from shared.utils.ontology_version import split_ref_commit
from shared.observability.tracing import trace_external_call, trace_db_operation

logger = logging.getLogger(__name__)


def _normalize_dt(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _text_with_keyword_field() -> Dict[str, Any]:
    return {
        "type": "text",
        "fields": {
            "keyword": {"type": "keyword"},
        },
    }


def _i18n_text_mapping() -> Dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "en": _text_with_keyword_field(),
            "ko": _text_with_keyword_field(),
        },
    }


def _fallback_projection_mapping(*, projection: str) -> Dict[str, Any]:
    base_settings = {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "1s",
    }
    if projection == "instances":
        return {
            "mappings": {
                "properties": {
                    "instance_id": {"type": "keyword"},
                    "class_id": {"type": "keyword"},
                    "class_label": _text_with_keyword_field(),
                    "properties": {
                        "type": "nested",
                        "properties": {
                            "name": {"type": "keyword"},
                            "value": {
                                "type": "text",
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "numeric": {"type": "double", "ignore_malformed": True},
                                },
                            },
                            "type": {"type": "keyword"},
                        },
                    },
                    "data": {"enabled": False},
                    "event_id": {"type": "keyword"},
                    "event_timestamp": {"type": "date"},
                    "version": {"type": "long"},
                    "db_name": {"type": "keyword"},
                    "branch": {"type": "keyword"},
                    "ontology_ref": {"type": "keyword"},
                    "ontology_commit": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                },
            },
            "settings": base_settings,
        }
    return {
        "mappings": {
            "properties": {
                "class_id": {"type": "keyword"},
                "label": _text_with_keyword_field(),
                "label_i18n": _i18n_text_mapping(),
                "description": {"type": "text"},
                "description_i18n": _i18n_text_mapping(),
                "properties": {
                    "type": "nested",
                    "properties": {
                        "name": {"type": "keyword"},
                        "label": _text_with_keyword_field(),
                        "label_i18n": _i18n_text_mapping(),
                        "type": {"type": "keyword"},
                        "required": {"type": "boolean"},
                        "description": {"type": "text"},
                        "description_i18n": _i18n_text_mapping(),
                    },
                },
                "relationships": {
                    "type": "nested",
                    "properties": {
                        "predicate": {"type": "keyword"},
                        "target": {"type": "keyword"},
                        "label": _text_with_keyword_field(),
                        "label_i18n": _i18n_text_mapping(),
                        "cardinality": {"type": "keyword"},
                        "description": {"type": "text"},
                        "description_i18n": _i18n_text_mapping(),
                        "inverse_label": _text_with_keyword_field(),
                        "inverse_label_i18n": _i18n_text_mapping(),
                    },
                },
                "parent_classes": {"type": "keyword"},
                "child_classes": {"type": "keyword"},
                "db_name": {"type": "keyword"},
                "branch": {"type": "keyword"},
                "ontology_ref": {"type": "keyword"},
                "ontology_commit": {"type": "keyword"},
                "version": {"type": "long"},
                "event_id": {"type": "keyword"},
                "event_timestamp": {"type": "date"},
                "created_at": {"type": "date"},
                "updated_at": {"type": "date"},
            },
        },
        "settings": base_settings,
    }


def _load_projection_mapping(*, projection: str) -> Dict[str, Any]:
    filename = "instances_mapping.json" if projection == "instances" else "ontologies_mapping.json"
    backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    mapping_path = os.path.join(backend_dir, "projection_worker", "mappings", filename)
    try:
        with open(mapping_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(
            "Projection mapping file missing (%s); using in-code fallback mapping for %s",
            mapping_path,
            projection,
        )
        return _fallback_projection_mapping(projection=projection)


async def _ensure_es_connected(es: ElasticsearchService) -> None:
    if getattr(es, "_client", None) is None:
        await es.connect()


def _versioning_kwargs(seq: Optional[int]) -> Dict[str, Any]:
    if seq is None:
        return {"version": None, "version_type": None}
    return {"version": int(seq), "version_type": "external_gte"}


def _validate_recompute_projection_mode(*, projection: str) -> None:
    if str(projection).strip().lower() != "instances":
        return
    raise classified_http_exception(
        status.HTTP_400_BAD_REQUEST,
        "Instances projection recompute from event replay is unsupported in dataset-primary mode. "
        "Run objectify full reindex from dataset artifacts instead.",
        code=ErrorCode.REQUEST_VALIDATION_FAILED,
        extra={
            "error": "REQUEST_VALIDATION_FAILED",
            "projection": "instances",
            "write_path_mode": "dataset_primary_index",
        },
    )


def _normalize_localized_field(value: Any, *, default_lang: str) -> tuple[str, Dict[str, str]]:
    i18n_map = coerce_localized_text(value, default_lang=default_lang)
    text = select_localized_text(value, lang=default_lang)
    if not text and value is not None and not i18n_map:
        try:
            text = str(value).strip()
        except Exception:
            logger.warning(
                "Failed to coerce localized field to string during projection recompute",
                exc_info=True,
            )
            text = ""
    return text, i18n_map


def _normalize_ontology_properties(
    properties: Any,
    *,
    default_lang: str,
) -> list[Dict[str, Any]]:
    normalized: list[Dict[str, Any]] = []
    if not isinstance(properties, list):
        return normalized
    for prop in properties:
        if not isinstance(prop, dict):
            continue
        item = dict(prop)
        label_text, label_i18n = _normalize_localized_field(prop.get("label"), default_lang=default_lang)
        if label_text or "label" in item:
            item["label"] = label_text
        if label_i18n:
            item["label_i18n"] = label_i18n
        else:
            item.pop("label_i18n", None)

        description_text, description_i18n = _normalize_localized_field(
            prop.get("description"),
            default_lang=default_lang,
        )
        if description_text or "description" in item:
            item["description"] = description_text
        if description_i18n:
            item["description_i18n"] = description_i18n
        else:
            item.pop("description_i18n", None)
        normalized.append(item)
    return normalized


def _normalize_ontology_relationships(
    relationships: Any,
    *,
    default_lang: str,
) -> list[Dict[str, Any]]:
    normalized: list[Dict[str, Any]] = []
    if not isinstance(relationships, list):
        return normalized
    for rel in relationships:
        if not isinstance(rel, dict):
            continue
        item = dict(rel)
        label_text, label_i18n = _normalize_localized_field(rel.get("label"), default_lang=default_lang)
        if label_text or "label" in item:
            item["label"] = label_text
        if label_i18n:
            item["label_i18n"] = label_i18n
        else:
            item.pop("label_i18n", None)

        description_text, description_i18n = _normalize_localized_field(
            rel.get("description"),
            default_lang=default_lang,
        )
        if description_text or "description" in item:
            item["description"] = description_text
        if description_i18n:
            item["description_i18n"] = description_i18n
        else:
            item.pop("description_i18n", None)

        inverse_label_text, inverse_label_i18n = _normalize_localized_field(
            rel.get("inverse_label"),
            default_lang=default_lang,
        )
        if inverse_label_text or "inverse_label" in item:
            item["inverse_label"] = inverse_label_text
        if inverse_label_i18n:
            item["inverse_label_i18n"] = inverse_label_i18n
        else:
            item.pop("inverse_label_i18n", None)
        normalized.append(item)
    return normalized


@dataclass(frozen=True)
class IndexDecision:
    action: Literal["index", "delete", "skip"]
    doc_id: Optional[str] = None
    document: Optional[Dict[str, Any]] = None
    record_lineage: bool = False


class ProjectionStrategy(Protocol):
    name: Literal["instances", "ontologies"]

    @property
    def event_types(self) -> Iterable[str]:
        ...

    def base_index(self, *, db_name: str, branch: str) -> str:
        ...

    def new_index(self, *, db_name: str, branch: str, version_suffix: str) -> str:
        ...

    async def decide(
        self,
        *,
        envelope: Any,
        data: Dict[str, Any],
        db_name: str,
        branch: str,
        new_index: str,
        ontology_ref: Optional[str],
        ontology_commit: Optional[str],
        seq: Optional[int],
        event_ts: datetime,
        created_at_cache: Dict[str, str],
        elasticsearch_service: ElasticsearchService,
    ) -> IndexDecision:
        ...


class InstancesProjectionStrategy:
    name: Literal["instances"] = "instances"
    event_types = ("INSTANCE_CREATED", "INSTANCE_UPDATED", "INSTANCE_DELETED")

    def base_index(self, *, db_name: str, branch: str) -> str:
        return get_instances_index_name(db_name, branch=branch)

    def new_index(self, *, db_name: str, branch: str, version_suffix: str) -> str:
        return get_instances_index_name(db_name, version=version_suffix, branch=branch)

    async def decide(
        self,
        *,
        envelope: Any,
        data: Dict[str, Any],
        db_name: str,
        branch: str,
        new_index: str,
        ontology_ref: Optional[str],
        ontology_commit: Optional[str],
        seq: Optional[int],
        event_ts: datetime,
        created_at_cache: Dict[str, str],
        elasticsearch_service: ElasticsearchService,
    ) -> IndexDecision:
        instance_id = data.get("instance_id")
        class_id = data.get("class_id")
        if not instance_id or not class_id:
            return IndexDecision(action="skip")

        doc_id = str(instance_id)
        created_at = created_at_cache.get(doc_id)
        if envelope.event_type == "INSTANCE_CREATED":
            created_at = created_at or event_ts.isoformat()
            created_at_cache.setdefault(doc_id, created_at)

        if envelope.event_type == "INSTANCE_UPDATED" and created_at is None:
            existing = await elasticsearch_service.get_document(new_index, doc_id)
            created_at = (existing or {}).get("created_at") or event_ts.isoformat()
            created_at_cache[doc_id] = created_at

        if envelope.event_type in {"INSTANCE_CREATED", "INSTANCE_UPDATED"}:
            doc = {
                "instance_id": doc_id,
                "class_id": str(class_id),
                "class_label": str(class_id),
                "properties": [],
                "data": data,
                "event_id": str(envelope.event_id),
                "event_sequence": seq,
                "event_timestamp": event_ts.isoformat(),
                "version": int(seq or 1),
                "db_name": db_name,
                "branch": branch,
                "ontology_ref": ontology_ref,
                "ontology_commit": ontology_commit,
                "created_at": created_at or event_ts.isoformat(),
                "updated_at": event_ts.isoformat(),
            }
            return IndexDecision(action="index", doc_id=doc_id, document=doc, record_lineage=True)

        if envelope.event_type == "INSTANCE_DELETED":
            if branch != "main":
                tombstone_doc = {
                    "instance_id": doc_id,
                    "class_id": str(class_id),
                    "db_name": db_name,
                    "branch": branch,
                    "ontology_ref": ontology_ref,
                    "ontology_commit": ontology_commit,
                    "deleted": True,
                    "deleted_at": event_ts.isoformat(),
                    "event_id": str(envelope.event_id),
                    "event_sequence": seq,
                    "event_timestamp": event_ts.isoformat(),
                    "version": int(seq or 1),
                    "updated_at": event_ts.isoformat(),
                }
                return IndexDecision(action="index", doc_id=doc_id, document=tombstone_doc, record_lineage=False)

            return IndexDecision(action="delete", doc_id=doc_id, record_lineage=False)

        return IndexDecision(action="skip")


class OntologiesProjectionStrategy:
    name: Literal["ontologies"] = "ontologies"
    event_types = ("ONTOLOGY_CLASS_CREATED", "ONTOLOGY_CLASS_UPDATED", "ONTOLOGY_CLASS_DELETED")

    def base_index(self, *, db_name: str, branch: str) -> str:
        return get_ontologies_index_name(db_name, branch=branch)

    def new_index(self, *, db_name: str, branch: str, version_suffix: str) -> str:
        return get_ontologies_index_name(db_name, version=version_suffix, branch=branch)

    async def decide(
        self,
        *,
        envelope: Any,
        data: Dict[str, Any],
        db_name: str,
        branch: str,
        new_index: str,
        ontology_ref: Optional[str],
        ontology_commit: Optional[str],
        seq: Optional[int],
        event_ts: datetime,
        created_at_cache: Dict[str, str],
        elasticsearch_service: ElasticsearchService,
    ) -> IndexDecision:
        class_id = data.get("class_id") or data.get("id")
        if not class_id:
            return IndexDecision(action="skip")
        doc_id = str(class_id)

        created_at = created_at_cache.get(doc_id)
        if envelope.event_type == "ONTOLOGY_CLASS_CREATED":
            created_at = created_at or event_ts.isoformat()
            created_at_cache.setdefault(doc_id, created_at)

        if envelope.event_type == "ONTOLOGY_CLASS_UPDATED" and created_at is None:
            existing = await elasticsearch_service.get_document(new_index, doc_id)
            created_at = (existing or {}).get("created_at") or event_ts.isoformat()
            created_at_cache[doc_id] = created_at

        if envelope.event_type in {"ONTOLOGY_CLASS_CREATED", "ONTOLOGY_CLASS_UPDATED"}:
            default_lang = get_default_language()
            label_text, label_i18n = _normalize_localized_field(data.get("label"), default_lang=default_lang)
            description_text, description_i18n = _normalize_localized_field(
                data.get("description"),
                default_lang=default_lang,
            )
            normalized_properties = _normalize_ontology_properties(
                data.get("properties", []),
                default_lang=default_lang,
            )
            normalized_relationships = _normalize_ontology_relationships(
                data.get("relationships", []),
                default_lang=default_lang,
            )
            doc = {
                "class_id": doc_id,
                "label": label_text,
                "description": description_text,
                "properties": normalized_properties,
                "relationships": normalized_relationships,
                "parent_classes": data.get("parent_classes", []),
                "child_classes": data.get("child_classes", []),
                "db_name": db_name,
                "branch": branch,
                "ontology_ref": ontology_ref,
                "ontology_commit": ontology_commit,
                "version": int(seq or 1),
                "event_id": str(envelope.event_id),
                "event_sequence": seq,
                "event_timestamp": event_ts.isoformat(),
                "created_at": created_at or event_ts.isoformat(),
                "updated_at": event_ts.isoformat(),
            }
            if label_i18n:
                doc["label_i18n"] = label_i18n
            if description_i18n:
                doc["description_i18n"] = description_i18n
            return IndexDecision(action="index", doc_id=doc_id, document=doc, record_lineage=True)

        if envelope.event_type == "ONTOLOGY_CLASS_DELETED":
            if branch != "main":
                tombstone_doc = {
                    "class_id": doc_id,
                    "db_name": db_name,
                    "branch": branch,
                    "ontology_ref": ontology_ref,
                    "ontology_commit": ontology_commit,
                    "deleted": True,
                    "deleted_at": event_ts.isoformat(),
                    "event_id": str(envelope.event_id),
                    "event_sequence": seq,
                    "event_timestamp": event_ts.isoformat(),
                    "version": int(seq or 1),
                    "updated_at": event_ts.isoformat(),
                }
                return IndexDecision(action="index", doc_id=doc_id, document=tombstone_doc, record_lineage=False)

            return IndexDecision(action="delete", doc_id=doc_id, record_lineage=False)

        return IndexDecision(action="skip")


def _strategy_for_projection(projection: str) -> ProjectionStrategy:
    if projection == "instances":
        return InstancesProjectionStrategy()
    if projection == "ontologies":
        return OntologiesProjectionStrategy()
    raise ValueError(f"Unsupported projection: {projection}")


@trace_db_operation("bff.admin_recompute.start_recompute_projection")
async def start_recompute_projection(
    *,
    http_request: Request,
    request: RecomputeProjectionRequest,
    background_tasks: BackgroundTasks,
    task_manager: BackgroundTaskManagerDep,
    redis_service: RedisService,
    audit_store: Any,
    lineage_store: LineageStore,
    elasticsearch_service: ElasticsearchService,
) -> RecomputeProjectionResponse:
    _validate_recompute_projection_mode(projection=str(request.projection))

    task_id = str(uuid4())
    requested_by = getattr(http_request.state, "admin_actor", None)
    request_ip = getattr(getattr(http_request, "client", None), "host", None)

    background_task_id = await task_manager.create_task(
        recompute_projection_task,
        task_id=task_id,
        request=request,
        elasticsearch_service=elasticsearch_service,
        redis_service=redis_service,
        audit_store=audit_store,
        lineage_store=lineage_store,
        task_manager=task_manager,
        requested_by=requested_by,
        request_ip=request_ip,
        task_name=f"Recompute projection: {request.projection} ({request.db_name})",
        task_type="projection_recompute",
        metadata={
            "db_name": request.db_name,
            "branch": request.branch,
            "projection": request.projection,
            "requested_by": requested_by,
            "request_ip": request_ip,
            "requested_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    background_tasks.add_task(monitor_admin_task, task_id=background_task_id, task_manager=task_manager)

    return RecomputeProjectionResponse(
        task_id=background_task_id,
        status="accepted",
        message=f"Projection recompute task started: {request.projection}",
        status_url=f"/api/v1/tasks/{background_task_id}",
    )


@trace_db_operation("bff.admin_recompute.get_recompute_projection_result")
async def get_recompute_projection_result(
    *,
    task_id: str,
    task_manager: BackgroundTaskManagerDep,
    redis_service: RedisService,
) -> Dict[str, Any]:
    task = await task_manager.get_task_status(task_id)
    if not task:
        raise classified_http_exception(404, f"Task {task_id} not found", code=ErrorCode.RESOURCE_NOT_FOUND)
    if not task.is_complete:
        return {
            "task_id": task_id,
            "status": "processing",
            "task_status": task.status.value,
            "progress": task.progress.model_dump(mode="json") if task.progress else None,
            "started_at": task.started_at.isoformat() if task.started_at else None,
            "message": f"Task {task_id} is still running",
        }
    if task.status == TaskStatus.FAILED:
        return {
            "task_id": task_id,
            "status": "failed",
            "error": task.result.error if task.result else "Unknown error",
            "message": "Projection recompute failed",
        }

    result_key = f"recompute_result:{task_id}"
    result = await redis_service.get_json(result_key)
    if not result:
        raise classified_http_exception(404, f"Result for task {task_id} not found or expired", code=ErrorCode.RESOURCE_NOT_FOUND)
    return result


async def _log_audit_safe(*, audit_store: Any, action: str, db_name: str, resource_id: str, metadata: Dict[str, Any], occurred_at: datetime) -> None:
    try:
        await audit_store.log(
            partition_key=f"db:{db_name}",
            actor="bff_admin",
            action=action,
            status="success",
            resource_type="es_index",
            resource_id=resource_id,
            metadata=metadata,
            occurred_at=occurred_at,
        )
    except Exception as exc:
        logger.warning(
            "Audit log failed: %s (db=%s resource_id=%s): %s",
            action,
            db_name,
            resource_id,
            exc,
            exc_info=True,
        )


async def _maybe_record_lineage(
    *,
    lineage_store: LineageStore,
    envelope: Any,
    db_name: str,
    branch: str,
    index_name: str,
    doc_id: str,
    seq: Optional[int],
    event_ts: datetime,
    ontology_ref: Optional[str],
    ontology_commit: Optional[str],
) -> None:
    await lineage_store.record_link(
        from_node_id=lineage_store.node_event(str(envelope.event_id)),
        to_node_id=lineage_store.node_artifact("es", index_name, doc_id),
        edge_type=EDGE_EVENT_MATERIALIZED_ES_DOCUMENT,
        occurred_at=event_ts,
        db_name=db_name,
        branch=branch,
        edge_metadata={
            "projection_name": "recompute",
            "db_name": db_name,
            "branch": branch,
            "index": index_name,
            "doc_id": doc_id,
            "sequence_number": seq,
            "ontology_ref": ontology_ref,
            "ontology_commit": ontology_commit,
        },
    )


@trace_external_call("bff.admin_recompute.recompute_projection_task")
async def recompute_projection_task(  # noqa: PLR0915
    task_id: str,
    request: RecomputeProjectionRequest,
    elasticsearch_service: ElasticsearchService,
    redis_service: RedisService,
    audit_store: Any,
    lineage_store: LineageStore,
    task_manager: Any | None = None,
    requested_by: Optional[str] = None,
    request_ip: Optional[str] = None,
) -> None:
    started_at = datetime.now(timezone.utc)

    db_name = validate_db_name(request.db_name)
    branch = validate_branch_name(request.branch or "main")
    projection = str(request.projection)
    if projection == "instances":
        raise RuntimeError(
            "Instances projection recompute from event replay is unsupported in dataset-primary mode. "
            "Run objectify full reindex from dataset artifacts instead."
        )
    strategy = _strategy_for_projection(projection)
    from_dt = _normalize_dt(request.from_ts)
    to_dt = _normalize_dt(request.to_ts) if request.to_ts else datetime.now(timezone.utc)

    if to_dt < from_dt:
        raise ValueError("to_ts must be >= from_ts")

    await _ensure_es_connected(elasticsearch_service)

    version_suffix = f"v{int(started_at.timestamp())}"
    base_index = strategy.base_index(db_name=db_name, branch=branch)
    new_index = strategy.new_index(db_name=db_name, branch=branch, version_suffix=version_suffix)

    await _log_audit_safe(
        audit_store=audit_store,
        action="RECOMPUTE_PROJECTION_STARTED",
        db_name=db_name,
        resource_id=new_index,
        metadata={
            "db_name": db_name,
            "branch": branch,
            "projection": projection,
            "base_index": base_index,
            "new_index": new_index,
            "from_ts": from_dt.isoformat(),
            "to_ts": to_dt.isoformat(),
            "promote": bool(request.promote),
            "requested_by": requested_by,
            "request_ip": request_ip,
        },
        occurred_at=started_at,
    )

    mapping = _load_projection_mapping(projection=projection)
    settings_payload = dict(mapping.get("settings", {}) or {})
    settings_payload.update(get_default_index_settings())

    if await elasticsearch_service.index_exists(new_index):
        await elasticsearch_service.delete_index(new_index)

    await elasticsearch_service.create_index(new_index, mappings=mapping.get("mappings"), settings=settings_payload)

    if task_manager is not None:
        try:
            initial_total = max(int(request.max_events) if request.max_events is not None else 1, 1)
            await task_manager.update_progress(
                task_id=task_id,
                current=0,
                total=initial_total,
                message=f"Initializing {projection} projection replay ({db_name}/{branch})",
                metadata={
                    "projection": projection,
                    "db_name": db_name,
                    "branch": branch,
                    "processed_events": 0,
                },
            )
        except Exception:
            logger.warning("Failed to initialize recompute projection task progress", exc_info=True)

    event_store = EventStore()
    await event_store.connect()

    processed = 0
    indexed = 0
    deleted = 0
    skipped = 0

    created_at_cache: Dict[str, str] = {}

    if task_manager is not None:
        try:
            replay_total = max(int(request.max_events) if request.max_events is not None else 1, 1)
            await task_manager.update_progress(
                task_id=task_id,
                current=0,
                total=replay_total,
                message=f"Starting {projection} projection replay ({db_name}/{branch})",
                metadata={
                    "projection": projection,
                    "db_name": db_name,
                    "branch": branch,
                    "processed_events": 0,
                },
            )
        except Exception:
            logger.warning("Failed to initialize replay progress state", exc_info=True)

    async for envelope in event_store.replay_events(from_dt, to_dt, event_types=list(strategy.event_types)):
        if request.max_events is not None and processed >= int(request.max_events):
            break

        processed += 1
        if task_manager is not None and (processed == 1 or processed % 100 == 0):
            progress_total = int(request.max_events) if request.max_events is not None else processed + 1
            progress_total = max(progress_total, processed)
            try:
                await task_manager.update_progress(
                    task_id=task_id,
                    current=processed,
                    total=progress_total,
                    message=f"Recomputing {projection} projection ({db_name}/{branch}): processed {processed} events",
                    metadata={
                        "projection": projection,
                        "db_name": db_name,
                        "branch": branch,
                        "processed_events": processed,
                    },
                )
            except Exception:
                logger.warning("Failed to update recompute projection task progress", exc_info=True)

        data = envelope.data if isinstance(envelope.data, dict) else {}
        if str(data.get("db_name") or "") != db_name:
            skipped += 1
            continue
        if str(data.get("branch") or "main") != branch:
            skipped += 1
            continue

        meta = envelope.metadata if isinstance(envelope.metadata, dict) else {}
        ontology_ref, ontology_commit = split_ref_commit(meta.get("ontology"))

        seq = envelope.sequence_number
        event_ts = envelope.occurred_at
        if event_ts.tzinfo is None:
            event_ts = event_ts.replace(tzinfo=timezone.utc)

        decision = await strategy.decide(
            envelope=envelope,
            data=data,
            db_name=db_name,
            branch=branch,
            new_index=new_index,
            ontology_ref=ontology_ref,
            ontology_commit=ontology_commit,
            seq=seq,
            event_ts=event_ts,
            created_at_cache=created_at_cache,
            elasticsearch_service=elasticsearch_service,
        )

        if decision.action == "skip" or not decision.doc_id:
            skipped += 1
            continue

        versioning = _versioning_kwargs(seq)
        if decision.action == "index":
            if not isinstance(decision.document, dict):
                skipped += 1
                continue
            await elasticsearch_service.index_document(
                new_index,
                decision.document,
                doc_id=decision.doc_id,
                refresh=False,
                version=versioning["version"],
                version_type=versioning["version_type"],
            )
            indexed += 1
            if decision.record_lineage:
                await _maybe_record_lineage(
                    lineage_store=lineage_store,
                    envelope=envelope,
                    db_name=db_name,
                    branch=branch,
                    index_name=new_index,
                    doc_id=decision.doc_id,
                    seq=seq,
                    event_ts=event_ts,
                    ontology_ref=ontology_ref,
                    ontology_commit=ontology_commit,
                )
            continue

        if decision.action == "delete":
            await elasticsearch_service.delete_document(
                new_index,
                decision.doc_id,
                refresh=False,
                version=versioning["version"],
                version_type=versioning["version_type"],
            )
            deleted += 1
            continue

        skipped += 1

    if task_manager is not None:
        try:
            completed_total = max(processed, 1)
            await task_manager.update_progress(
                task_id=task_id,
                current=completed_total,
                total=completed_total,
                message=f"Recompute projection completed with {processed} processed events",
                metadata={
                    "projection": projection,
                    "db_name": db_name,
                    "branch": branch,
                    "processed_events": processed,
                    "indexed_docs": indexed,
                    "deleted_docs": deleted,
                    "skipped_events": skipped,
                },
            )
        except Exception:
            logger.warning("Failed to finalize recompute projection task progress", exc_info=True)

    await elasticsearch_service.refresh_index(new_index)

    promoted = False
    promote_error: Optional[str] = None
    if request.promote:
        promoted, promote_error = await promote_alias_to_index(
            elasticsearch_service=elasticsearch_service,
            base_index=base_index,
            new_index=new_index,
            allow_delete_base_index=bool(request.allow_delete_base_index),
        )

    completed_at = datetime.now(timezone.utc)
    result = {
        "task_id": task_id,
        "status": "completed",
        "db_name": db_name,
        "branch": branch,
        "projection": projection,
        "base_index": base_index,
        "new_index": new_index,
        "from_ts": from_dt.isoformat(),
        "to_ts": to_dt.isoformat(),
        "processed_events": processed,
        "indexed_docs": indexed,
        "deleted_docs": deleted,
        "skipped_events": skipped,
        "promoted": promoted,
        "promote_error": promote_error,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "next_steps": (
            None
            if promoted
            else {
                "note": "Index rebuilt but not promoted. You can re-run with promote=true, or manually switch alias/index in Elasticsearch.",
                "base_index": base_index,
                "new_index": new_index,
            }
        ),
    }

    await redis_service.set_json(key=f"recompute_result:{task_id}", value=result, ttl=3600)

    await _log_audit_safe(
        audit_store=audit_store,
        action="RECOMPUTE_PROJECTION_COMPLETED",
        db_name=db_name,
        resource_id=new_index,
        metadata=result,
        occurred_at=completed_at,
    )
