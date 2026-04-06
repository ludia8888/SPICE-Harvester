import hashlib
import logging
from typing import Any, Awaitable, Callable, Dict
from uuid import NAMESPACE_URL, uuid4, uuid5

import httpx
from fastapi import APIRouter, Body, Depends, Query, Request, Response, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from bff.routers.data_connector_deps import (
    get_connector_adapter_factory,
    get_connector_registry,
    get_dataset_registry,
    get_google_sheets_service,
    get_objectify_job_queue,
    get_objectify_registry,
    get_pipeline_registry,
)
from bff.routers.foundry_connectivity_v2_common import (
    _DEFAULT_PARENT_FOLDER_RID,
    _KIND_TO_CONNECTION_CONFIG_TYPE,
    _build_execute_run_output,
    _cdc_enabled_for_flags,
    _coerce_bool,
    _connection_id_from_rid,
    _connection_kind_from_source,
    _connection_rid,
    _connection_source_types,
    _dataset_id_from_any,
    _dataset_id_from_rid,
    _dataset_rid,
    _default_export_settings,
    _display_name_from_payload,
    _export_run_id_from_rid,
    _export_run_response_from_task,
    _export_run_rid,
    _extract_connection_configuration,
    _extract_destination,
    _extract_secrets_from_configuration,
    _extract_secrets_from_payload,
    _file_import_id_from_rid,
    _file_import_rid,
    _file_import_source_types,
    _foundry_error,
    _jdbc_enabled_for_flags,
    _invalid_pagination_response,
    _is_valid_http_url,
    _iso_timestamp,
    _normalize_connection_config_for_storage,
    _normalize_export_run_headers,
    _normalize_export_run_method,
    _normalize_export_run_timeout_seconds,
    _normalize_export_settings,
    _normalize_jdbc_driver_file_name,
    _normalize_markings,
    _parse_page_offset,
    _parse_page_size_value,
    _public_custom_jdbc_driver_metadata,
    _require_preview_or_400,
    _requires_cdc_feature,
    _resolve_feature_flags_settings,
    _resource_branch_name,
    _resource_import_config_from_source,
    _resource_import_mode_from_source,
    _resource_invalid_config_response,
    _resource_parent_rid,
    _resolve_file_import_config,
    _resolve_table_import_config,
    _resolve_virtual_table_config,
    _safe_import_mode,
    _table_import_id_from_rid,
    _table_import_rid,
    _table_import_source_types,
    _upsert_custom_jdbc_driver_metadata,
    _validated_execute_result_payload,
    _validate_parent_rid_matches_connection,
    _virtual_table_id_from_rid,
    _virtual_table_rid,
    _virtual_table_source_types,
)
from bff.services import data_connector_pipelining_service, data_connector_registration_service
from data_connector.adapters.factory import (
    ConnectorAdapterFactory,
    SUPPORTED_CONNECTOR_KINDS,
    connection_source_type_for_kind,
    connector_kind_from_source_type,
    file_import_source_type_for_kind,
    resolve_connector_kind_from_connection_config,
    table_import_source_type_for_kind,
    virtual_table_source_type_for_kind,
)
from data_connector.adapters.import_config_validators import (
    is_jdbc_connector_kind,
    normalize_import_mode,
    validate_resource_import_config,
)
from data_connector.google_sheets.service import GoogleSheetsService
from shared.config.settings import get_settings
from shared.dependencies.providers import AuditLogStoreDep, BackgroundTaskManagerDep, LineageStoreDep
from shared.foundry.auth import require_scopes
from shared.foundry.rids import build_rid
from shared.middleware.rate_limiter import RateLimitPresets, rate_limit
from shared.observability.tracing import trace_endpoint
from shared.models.background_task import BackgroundTask
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.connector_registry import ConnectorMapping, ConnectorRegistry, ConnectorSource
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/connectivity", tags=["Foundry Connectivity v2"])

_CONNECTIVITY_FETCH_LIMIT_START = 5000
_CONNECTIVITY_FETCH_LIMIT_MAX = 2_000_000

_feature_flags_settings = lambda: _resolve_feature_flags_settings(get_settings)
_jdbc_enabled_for_db = lambda db_name: _jdbc_enabled_for_flags(_feature_flags_settings(), db_name)
_cdc_enabled_for_db = lambda db_name: _cdc_enabled_for_flags(_feature_flags_settings(), db_name)


async def _run_connection_export(
    *,
    task_id: str,
    connection_id: str,
    connection_rid: str,
    target_url: str,
    method: str,
    request_headers: Dict[str, str],
    payload: Any,
    timeout_seconds: float,
    dry_run: bool,
    actor: str | None,
    audit_store: AuditLogStore,
) -> Dict[str, Any]:
    occurred_at = utcnow()
    delivery: Dict[str, Any] = {
        "targetUrl": target_url,
        "method": method,
        "dryRun": bool(dry_run),
    }
    audit_log_id: str | None = None
    partition_key = f"connectivity:{connection_id}"
    action = "CONNECTIVITY_EXPORT_RUN"

    try:
        if dry_run:
            delivery["status"] = "SKIPPED"
            delivery["httpStatus"] = None
            delivery["responseSnippet"] = None
        else:
            request_kwargs: Dict[str, Any] = {
                "method": method,
                "url": target_url,
                "headers": request_headers,
            }
            if method in {"POST", "PUT", "PATCH"}:
                request_kwargs["json"] = payload
            async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
                response = await client.request(**request_kwargs)
            response_snippet = response.text[:512]
            delivery["httpStatus"] = int(response.status_code)
            delivery["responseSnippet"] = response_snippet
            delivery["status"] = "DELIVERED" if response.status_code < 400 else "FAILED"
            if response.status_code >= 400:
                raise RuntimeError(
                    f"Export webhook returned HTTP {response.status_code}: {response_snippet}"
                )

        audit_id = await audit_store.log(
            partition_key=partition_key,
            actor=actor or "system",
            action=action,
            status="success",
            resource_type="connection",
            resource_id=connection_id,
            command_id=task_id,
            metadata={
                "connectionRid": connection_rid,
                "targetUrl": target_url,
                "method": method,
                "dryRun": bool(dry_run),
                "delivery": delivery,
            },
            occurred_at=occurred_at,
        )
        audit_log_id = str(audit_id)

        return {
            "rid": _export_run_rid(task_id),
            "connectionRid": connection_rid,
            "status": "SUCCEEDED",
            "auditLogId": audit_log_id,
            "sideEffectDelivery": delivery,
            "writebackStatus": "SUCCEEDED" if not dry_run else "SKIPPED",
        }
    except Exception as exc:
        try:
            audit_id = await audit_store.log(
                partition_key=partition_key,
                actor=actor or "system",
                action=action,
                status="failure",
                resource_type="connection",
                resource_id=connection_id,
                command_id=task_id,
                metadata={
                    "connectionRid": connection_rid,
                    "targetUrl": target_url,
                    "method": method,
                    "dryRun": bool(dry_run),
                    "delivery": delivery,
                },
                error=str(exc),
                occurred_at=utcnow(),
            )
            audit_log_id = str(audit_id)
        except Exception as audit_exc:
            logger.warning(
                "Failed to persist connectivity export audit failure log (connection_id=%s task_id=%s): %s",
                connection_id,
                task_id,
                audit_exc,
                exc_info=True,
            )

        error_message = str(exc)
        if audit_log_id:
            error_message = f"{error_message} (auditLogId={audit_log_id})"
        raise RuntimeError(error_message) from exc


def _connector_import_pipeline_id(
    *,
    namespace: str,
    connection_id: str,
    source_type: str,
    source_id: str,
) -> str:
    return str(uuid5(NAMESPACE_URL, f"spice:{namespace}:{connection_id}:{source_type}:{source_id}"))


def _build_pipeline_job_id(*, pipeline_id: str) -> str:
    return f"build-{pipeline_id}-{uuid4()}"


async def _resolve_dataset_context(
    *,
    payload: Dict[str, Any],
    dataset_registry: DatasetRegistry,
) -> tuple[str | None, str | None, str | None]:
    dataset_rid = str(payload.get("datasetRid") or "").strip() or None
    if not dataset_rid:
        return None, None, None

    dataset_id = _dataset_id_from_rid(dataset_rid)
    if not dataset_id:
        return dataset_rid, None, None

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if dataset is None:
        raise LookupError("datasetRid not found")

    resolved_dataset_rid = _dataset_rid(dataset.dataset_id)
    dataset_branch = str(dataset.branch or "").strip() or "main"
    dataset_db_name = str(dataset.db_name or "").strip() or None
    return resolved_dataset_rid, dataset_db_name, dataset_branch


async def _load_connection_source(
    *,
    connector_registry: ConnectorRegistry,
    connection_id: str,
) -> ConnectorSource | None:
    for source_type in _connection_source_types():
        source = await connector_registry.get_source(source_type=source_type, source_id=connection_id)
        if source is not None:
            return source
    return None


async def _load_connection_source_or_404(
    *,
    connector_registry: ConnectorRegistry,
    connection_rid: str,
) -> tuple[str | None, ConnectorSource | None, JSONResponse | None]:
    connection_id = _connection_id_from_rid(connection_rid)
    if not connection_id:
        return None, None, _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )
    source = await _load_connection_source(connector_registry=connector_registry, connection_id=connection_id)
    if source is None:
        return connection_id, None, _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="ConnectionNotFound",
            parameters={"connectionRid": connection_rid},
    )
    return connection_id, source, None


async def _load_connector_import_source(
    *,
    connector_registry: ConnectorRegistry,
    connection_id: str,
    source_id: str,
    source_types: tuple[str, ...],
) -> tuple[ConnectorSource | None, ConnectorMapping | None]:
    for source_type in source_types:
        source = await connector_registry.get_source(source_type=source_type, source_id=source_id)
        if source is None:
            continue
        cfg = source.config_json or {}
        owner_connection = str(cfg.get("connection_id") or "").strip()
        if owner_connection and owner_connection != connection_id:
            continue
        mapping = await connector_registry.get_mapping(source_type=source.source_type, source_id=source.source_id)
        return source, mapping
    return None, None


async def _list_connection_owned_sources(
    *,
    connector_registry: ConnectorRegistry,
    connection_id: str,
    source_types: tuple[str, ...],
) -> list[ConnectorSource]:
    owned_sources: list[ConnectorSource] = []
    for source_type in source_types:
        sources = await _list_all_enabled_sources_for_type(
            connector_registry=connector_registry,
            source_type=source_type,
        )
        for src in sources:
            if str((src.config_json or {}).get("connection_id") or "").strip() != connection_id:
                continue
            owned_sources.append(src)
    owned_sources.sort(key=lambda src: str(src.source_id))
    return owned_sources


async def _list_all_enabled_sources_for_type(
    *,
    connector_registry: ConnectorRegistry,
    source_type: str,
) -> list[ConnectorSource]:
    limit = _CONNECTIVITY_FETCH_LIMIT_START
    while True:
        sources = await connector_registry.list_sources(
            source_type=source_type,
            enabled=True,
            limit=limit,
        )
        if len(sources) < limit or limit >= _CONNECTIVITY_FETCH_LIMIT_MAX:
            return sources
        limit = min(limit * 2, _CONNECTIVITY_FETCH_LIMIT_MAX)


async def _list_all_connectivity_export_tasks(task_manager: Any) -> list[BackgroundTask]:
    redis_service = getattr(task_manager, "redis", None)
    if redis_service is None or not hasattr(redis_service, "scan_keys") or not hasattr(redis_service, "get_json"):
        return await task_manager.get_all_tasks(
            task_type="connectivity_export_run",
            limit=_CONNECTIVITY_FETCH_LIMIT_MAX,
        )

    task_keys = await redis_service.scan_keys("background_task:*")
    tasks: list[BackgroundTask] = []
    for key in task_keys:
        task_data = await redis_service.get_json(key)
        if not isinstance(task_data, dict):
            continue
        try:
            task = BackgroundTask(**task_data)
        except ValidationError as exc:
            logger.warning("Skipping malformed background task payload for key %s: %s", key, exc)
            continue
        if task.task_type != "connectivity_export_run":
            continue
        tasks.append(task)

    tasks.sort(key=lambda item: item.created_at, reverse=True)
    return tasks


async def _load_table_import_source(
    *,
    connector_registry: ConnectorRegistry,
    connection_id: str,
    table_import_id: str,
) -> tuple[ConnectorSource | None, ConnectorMapping | None]:
    return await _load_connector_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        source_id=table_import_id,
        source_types=_table_import_source_types(),
    )


async def _load_file_import_source(
    *,
    connector_registry: ConnectorRegistry,
    connection_id: str,
    file_import_id: str,
) -> tuple[ConnectorSource | None, ConnectorMapping | None]:
    return await _load_connector_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        source_id=file_import_id,
        source_types=_file_import_source_types(),
    )


async def _load_virtual_table_source(
    *,
    connector_registry: ConnectorRegistry,
    connection_id: str,
    virtual_table_id: str,
) -> tuple[ConnectorSource | None, ConnectorMapping | None]:
    return await _load_connector_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        source_id=virtual_table_id,
        source_types=_virtual_table_source_types(),
    )


async def _resolve_output_dataset_rid(
    *,
    source: ConnectorSource,
    mapping: ConnectorMapping | None,
    dataset_registry: DatasetRegistry,
) -> str:
    cfg = source.config_json or {}
    configured_id = _dataset_id_from_any(cfg.get("dataset_rid"))
    if configured_id:
        return _dataset_rid(configured_id)

    dataset_id = _dataset_id_from_any(cfg.get("dataset_id"))
    if dataset_id:
        return _dataset_rid(dataset_id)

    target_db = str(mapping.target_db_name or "").strip() if mapping else ""
    target_branch = str(mapping.target_branch or "").strip() if mapping else ""
    if target_db:
        dataset = await dataset_registry.get_dataset_by_source_ref(
            db_name=target_db,
            source_type="connector",
            source_ref=f"{source.source_type}:{source.source_id}",
            branch=target_branch or "main",
        )
        if dataset is not None:
            return _dataset_rid(dataset.dataset_id)

    return _dataset_rid(f"connector-{source.source_type}-{source.source_id}")


async def _build_connector_import_response(
    *,
    connection_id: str,
    source: ConnectorSource,
    mapping: ConnectorMapping | None,
    dataset_registry: DatasetRegistry,
    rid_builder: Callable[[str], str],
    config_key: str,
    default_config_factory: Callable[[str], Dict[str, Any]],
    default_name_factory: Callable[[str, str], str],
    name_hint_keys: tuple[str, ...] = (),
    include_import_mode: bool = False,
    include_allow_schema_changes: bool = False,
    include_markings: bool = False,
) -> Dict[str, Any]:
    cfg = source.config_json or {}
    connector_kind = connector_kind_from_source_type(source.source_type, strict=True)
    dataset_rid = await _resolve_output_dataset_rid(source=source, mapping=mapping, dataset_registry=dataset_registry)

    config = cfg.get(config_key) if isinstance(cfg.get(config_key), dict) else {}
    if not config:
        config = default_config_factory(connector_kind)

    name = str(cfg.get("name") or "").strip() or str(cfg.get("display_name") or "").strip()
    if not name:
        for key in name_hint_keys:
            name = str(cfg.get(key) or "").strip()
            if name:
                break
    if not name:
        name = default_name_factory(connector_kind, source.source_id)

    output: Dict[str, Any] = {
        "rid": rid_builder(source.source_id),
        "name": name,
        "parentRid": _resource_parent_rid(cfg=cfg, connection_id=connection_id),
        "displayName": name,
        "connectionRid": _connection_rid(connection_id),
        "datasetRid": dataset_rid,
        "branchName": _resource_branch_name(cfg=cfg, mapping=mapping),
        "config": config,
    }
    if include_import_mode:
        output["importMode"] = _safe_import_mode(cfg.get("import_mode"))
    if include_allow_schema_changes:
        output["allowSchemaChanges"] = bool(cfg.get("allow_schema_changes") or False)
    if include_markings:
        output["markings"] = cfg.get("markings") if isinstance(cfg.get("markings"), list) else []
    return output


async def _build_table_import_response(
    *,
    connection_id: str,
    source: ConnectorSource,
    mapping: ConnectorMapping | None,
    dataset_registry: DatasetRegistry,
) -> Dict[str, Any]:
    return await _build_connector_import_response(
        connection_id=connection_id,
        source=source,
        mapping=mapping,
        dataset_registry=dataset_registry,
        rid_builder=_table_import_rid,
        config_key="table_import_config",
        default_config_factory=lambda connector_kind: _resolve_table_import_config({}, connector_kind=connector_kind),
        default_name_factory=lambda connector_kind, source_id: f"{connector_kind} import {source_id}",
        name_hint_keys=("sheet_title",),
        include_import_mode=True,
        include_allow_schema_changes=True,
    )


async def _build_file_import_response(
    *,
    connection_id: str,
    source: ConnectorSource,
    mapping: ConnectorMapping | None,
    dataset_registry: DatasetRegistry,
) -> Dict[str, Any]:
    return await _build_connector_import_response(
        connection_id=connection_id,
        source=source,
        mapping=mapping,
        dataset_registry=dataset_registry,
        rid_builder=_file_import_rid,
        config_key="file_import_config",
        default_config_factory=lambda connector_kind: _resolve_file_import_config({}, connector_kind=connector_kind),
        default_name_factory=lambda connector_kind, source_id: f"{connector_kind} file import {source_id}",
        name_hint_keys=("file_pattern",),
        include_import_mode=True,
        include_allow_schema_changes=True,
    )


async def _build_virtual_table_response(
    *,
    connection_id: str,
    source: ConnectorSource,
    mapping: ConnectorMapping | None,
    dataset_registry: DatasetRegistry,
) -> Dict[str, Any]:
    return await _build_connector_import_response(
        connection_id=connection_id,
        source=source,
        mapping=mapping,
        dataset_registry=dataset_registry,
        rid_builder=_virtual_table_rid,
        config_key="virtual_table_config",
        default_config_factory=lambda connector_kind: _resolve_virtual_table_config({}, connector_kind=connector_kind),
        default_name_factory=lambda connector_kind, source_id: f"{connector_kind} virtual table {source_id}",
        include_markings=True,
    )


def _resolved_display_names(
    payload: Dict[str, Any],
    *,
    fallback_display_name: str,
    fallback_name: str | None = None,
) -> tuple[str, str]:
    display_name = _display_name_from_payload(payload, fallback=fallback_display_name)
    name = _display_name_from_payload(payload, fallback=fallback_name or fallback_display_name)
    return display_name, name


def _resolved_allow_schema_changes(
    payload: Dict[str, Any],
    *,
    existing_cfg: Dict[str, Any] | None = None,
    default: bool = False,
) -> bool:
    if payload.get("allowSchemaChanges") is not None:
        return bool(payload.get("allowSchemaChanges"))
    if existing_cfg is not None:
        return bool(existing_cfg.get("allow_schema_changes") or False)
    return default


def _build_connector_import_source_config(
    *,
    connection_id: str,
    display_name: str,
    name: str,
    import_mode: str,
    config_key: str,
    resource_config: Dict[str, Any],
    existing_cfg: Dict[str, Any] | None = None,
    dataset_rid: str | None = None,
    branch_name: str | None = None,
    allow_schema_changes: bool | None = None,
    resource_kind: str | None = None,
    extra_updates: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    cfg = dict(existing_cfg or {})
    if extra_updates:
        cfg.update(extra_updates)
    cfg.update(
        {
            "connection_id": connection_id,
            "display_name": display_name,
            "name": name,
            "parent_rid": _connection_rid(connection_id),
            "import_mode": import_mode,
            config_key: resource_config,
        }
    )
    if dataset_rid is not None:
        cfg["dataset_rid"] = dataset_rid
    if branch_name:
        cfg["branch_name"] = branch_name
    if allow_schema_changes is not None:
        cfg["allow_schema_changes"] = allow_schema_changes
    if resource_kind:
        cfg["resource_kind"] = resource_kind
    return cfg


def _connector_import_mapping_status(
    *,
    target_db_name: str | None,
    target_class_label: str | None,
) -> str:
    return "confirmed" if str(target_db_name or "").strip() and str(target_class_label or "").strip() else "draft"


async def _upsert_created_connector_import_mapping(
    *,
    connector_registry: ConnectorRegistry,
    source_type: str,
    source_id: str,
    target_db_name: str | None,
    target_branch: str | None,
    target_class_label: str | None,
) -> ConnectorMapping | None:
    if not str(target_db_name or "").strip():
        return None
    return await connector_registry.upsert_mapping(
        source_type=source_type,
        source_id=source_id,
        enabled=bool(target_db_name and target_class_label),
        status=_connector_import_mapping_status(
            target_db_name=target_db_name,
            target_class_label=target_class_label,
        ),
        target_db_name=target_db_name,
        target_branch=target_branch,
        target_class_label=target_class_label,
        field_mappings=[],
    )


async def _upsert_replaced_connector_import_mapping(
    *,
    connector_registry: ConnectorRegistry,
    source_type: str,
    source_id: str,
    mapping: ConnectorMapping | None,
    destination_db: str | None,
    destination_branch: str | None,
    destination_object_type: str | None,
) -> ConnectorMapping | None:
    if mapping is None or not (destination_db or destination_branch or destination_object_type):
        return mapping
    target_db_name = destination_db or mapping.target_db_name
    target_branch = destination_branch or mapping.target_branch
    target_class_label = destination_object_type or mapping.target_class_label
    return await connector_registry.upsert_mapping(
        source_type=source_type,
        source_id=source_id,
        enabled=bool(destination_db or mapping.enabled),
        status=_connector_import_mapping_status(
            target_db_name=target_db_name,
            target_class_label=target_class_label,
        ),
        target_db_name=target_db_name,
        target_branch=target_branch,
        target_class_label=target_class_label,
        field_mappings=mapping.field_mappings,
    )


async def _connection_configuration(
    *,
    source: ConnectorSource,
    connector_adapter_factory: ConnectorAdapterFactory,
) -> Dict[str, Any]:
    kind = _connection_kind_from_source(source)
    adapter = connector_adapter_factory.get_adapter(kind)
    config = await adapter.get_public_configuration(config=dict(source.config_json or {}))
    if not config.get("type"):
        config["type"] = _KIND_TO_CONNECTION_CONFIG_TYPE.get(kind, "GoogleSheetsConnectionConfig")
    custom_jdbc_drivers = _public_custom_jdbc_driver_metadata(dict(source.config_json or {}))
    if custom_jdbc_drivers:
        config["customJdbcDrivers"] = custom_jdbc_drivers
    return config


async def _connection_response(
    *,
    source: ConnectorSource,
    connector_adapter_factory: ConnectorAdapterFactory,
) -> Dict[str, Any]:
    cfg = source.config_json or {}
    display_name = (
        str(cfg.get("display_name") or "").strip()
        or str(cfg.get("name") or "").strip()
        or f"Connection {source.source_id}"
    )

    conn_status = "CONNECTED"
    if not source.enabled:
        conn_status = "DISCONNECTED"
    elif cfg.get("status"):
        text = str(cfg["status"]).strip().upper()
        if text in {"ERROR", "FAILED"}:
            conn_status = "ERROR"
        elif text in {"DISCONNECTED", "DISABLED"}:
            conn_status = "DISCONNECTED"

    connection_configuration = await _connection_configuration(source=source, connector_adapter_factory=connector_adapter_factory)
    parent_folder_rid = (
        str(cfg.get("parent_folder_rid") or cfg.get("parentFolderRid") or "").strip() or _DEFAULT_PARENT_FOLDER_RID
    )
    export_settings = _normalize_export_settings(cfg.get("export_settings") or cfg.get("exportSettings"))
    markings = _normalize_markings(cfg.get("markings"))

    response = {
        "rid": _connection_rid(source.source_id),
        "displayName": display_name,
        "connectionConfiguration": connection_configuration,
        # Compatibility alias for existing internal clients.
        "configuration": connection_configuration,
        "parentFolderRid": parent_folder_rid,
        "exportSettings": export_settings,
        "status": conn_status,
        "createdTime": _iso_timestamp(source.created_at),
        "updatedTime": _iso_timestamp(source.updated_at),
    }
    if markings:
        response["markings"] = markings
    return response


async def _record_execute_failure_run(
    *,
    pipeline_registry: PipelineRegistry,
    pipeline_id: str | None,
    job_id: str | None,
    connection_rid: str,
    resource_rid: str,
    branch_name: str,
    requested_by: str,
    error_detail: str,
    resource_field: str = "tableImportRid",
    started_at: Any = None,
) -> None:
    if not pipeline_id or not job_id:
        return
    try:
        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=job_id,
            mode="build",
            status="FAILED",
            output_json=_build_execute_run_output(
                connection_rid=connection_rid,
                resource_rid=resource_rid,
                resource_field=resource_field,
                branch_name=branch_name,
                requested_by=requested_by,
                error_detail=error_detail,
            ),
            started_at=started_at or utcnow(),
            finished_at=utcnow(),
        )
    except Exception as record_exc:
        logger.warning("Failed to record execute failure build run: %s", record_exc)


async def _ensure_connector_import_pipeline(
    *,
    pipeline_registry: PipelineRegistry,
    connection_id: str,
    source: ConnectorSource,
    mapping: ConnectorMapping,
    namespace: str,
    pipeline_name_prefix: str,
    pipeline_type: str,
    location_segment: str,
    description_label: str,
) -> str:
    pipeline_id = _connector_import_pipeline_id(
        namespace=namespace,
        connection_id=connection_id,
        source_type=source.source_type,
        source_id=source.source_id,
    )
    existing = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    if existing is not None:
        return pipeline_id

    db_name = str(mapping.target_db_name or "").strip() or "main"
    branch = str(mapping.target_branch or "").strip() or "main"
    pipeline_name = f"{pipeline_name_prefix}_{connection_id}_{source.source_type}_{source.source_id}"
    await pipeline_registry.create_pipeline(
        pipeline_id=pipeline_id,
        db_name=db_name,
        name=pipeline_name,
        description=f"Foundry connectivity {description_label} pipeline for {source.source_type}:{source.source_id}",
        pipeline_type=pipeline_type,
        location=f"connectivity/connections/{connection_id}/{location_segment}/{source.source_id}",
        status="active",
        branch=branch,
    )
    return pipeline_id


async def _execute_connector_import_build(
    *,
    request: Request,
    google_sheets_service: GoogleSheetsService,
    connector_adapter_factory: ConnectorAdapterFactory,
    connector_registry: ConnectorRegistry,
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    objectify_job_queue: ObjectifyJobQueue,
    connection_id: str,
    source: ConnectorSource,
    mapping: ConnectorMapping,
    resource_kind: str,
    resource_kind_key: str,
    resource_log_id: str,
    resource_rid: str,
    resource_field: str,
    pipeline_namespace: str,
    pipeline_name_prefix: str,
    pipeline_type: str,
    location_segment: str,
    start_pipelining: Callable[..., Awaitable[Any]],
) -> str | JSONResponse:
    branch_name = str(mapping.target_branch or "").strip() or "main"
    actor_user_id = str(request.headers.get("X-User-ID") or "").strip() or None
    requested_by = actor_user_id or "system"
    connection_rid = _connection_rid(connection_id)
    pipeline_id: str | None = None
    job_id: str | None = None
    started_at: Any = None

    try:
        pipeline_id = await _ensure_connector_import_pipeline(
            pipeline_registry=pipeline_registry,
            connection_id=connection_id,
            source=source,
            mapping=mapping,
            namespace=pipeline_namespace,
            pipeline_name_prefix=pipeline_name_prefix,
            pipeline_type=pipeline_type,
            location_segment=location_segment,
            description_label=resource_kind,
        )
        job_id = _build_pipeline_job_id(pipeline_id=pipeline_id)
        started_at = utcnow()
        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=job_id,
            mode="build",
            status="RUNNING",
            output_json=_build_execute_run_output(
                connection_rid=connection_rid,
                resource_rid=resource_rid,
                resource_field=resource_field,
                branch_name=branch_name,
                requested_by=requested_by,
            ),
            started_at=started_at,
        )

        result_payload = await start_pipelining(
            source=source,
            mapping=mapping,
            google_sheets_service=google_sheets_service,
            connector_adapter_factory=connector_adapter_factory,
            connector_registry=connector_registry,
            pipeline_registry=pipeline_registry,
            dataset_registry=dataset_registry,
            objectify_registry=objectify_registry,
            objectify_job_queue=objectify_job_queue,
            actor_user_id=actor_user_id,
        )
        result_payload = _validated_execute_result_payload(
            result_payload=result_payload,
            resource_kind=resource_kind,
        )
        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=job_id,
            mode="build",
            status="SUCCESS",
            output_json=_build_execute_run_output(
                connection_rid=connection_rid,
                resource_rid=resource_rid,
                resource_field=resource_field,
                branch_name=branch_name,
                requested_by=requested_by,
                result_payload=result_payload if isinstance(result_payload, dict) else None,
            ),
            started_at=started_at,
            finished_at=utcnow(),
        )
    except ValueError as exc:
        logger.warning("Invalid %s runtime configuration for %s: %s", resource_kind, resource_log_id, exc)
        await _record_execute_failure_run(
            pipeline_registry=pipeline_registry,
            pipeline_id=pipeline_id,
            job_id=job_id,
            connection_rid=connection_rid,
            resource_rid=resource_rid,
            resource_field=resource_field,
            branch_name=branch_name,
            requested_by=requested_by,
            error_detail=str(exc),
            started_at=started_at,
        )
        return _resource_invalid_config_response(resource_kind=resource_kind_key, message=str(exc))
    except Exception as exc:
        logger.error("Failed to execute %s %s: %s", resource_kind, resource_log_id, exc)
        await _record_execute_failure_run(
            pipeline_registry=pipeline_registry,
            pipeline_id=pipeline_id,
            job_id=job_id,
            connection_rid=connection_rid,
            resource_rid=resource_rid,
            resource_field=resource_field,
            branch_name=branch_name,
            requested_by=requested_by,
            error_detail=str(exc),
            started_at=started_at,
        )
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"message": f"Failed to execute {resource_kind}"},
        )

    assert job_id is not None
    return build_rid("build", job_id)


async def _load_validated_executable_connector_import(
    *,
    connector_registry: ConnectorRegistry,
    connection_id: str,
    source_id: str,
    source_types: tuple[str, ...],
    resource_kind: str,
    resource_display_name: str,
    resource_error_name: str,
    not_found_parameters: Dict[str, Any],
    validate_cdc: bool = True,
) -> tuple[ConnectorSource, ConnectorMapping] | JSONResponse:
    source, mapping = await _load_connector_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        source_id=source_id,
        source_types=source_types,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name=f"{resource_error_name}NotFound",
            parameters=not_found_parameters,
        )
    if mapping is None or not str(mapping.target_db_name or "").strip():
        return _foundry_error(
            status.HTTP_409_CONFLICT,
            error_code="CONFLICT",
            error_name=f"{resource_error_name}NotReady",
            parameters={"message": f"{resource_display_name} mapping is incomplete; target ontology is required"},
        )

    connector_kind = connector_kind_from_source_type(source.source_type, strict=True)
    source_cfg = dict(source.config_json or {})
    target_db = str(mapping.target_db_name or "").strip() or None
    try:
        import_mode = _resource_import_mode_from_source(source_cfg=source_cfg, resource_kind=resource_kind)
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind=resource_kind, message=str(exc))
    if is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(target_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )
    if validate_cdc and _requires_cdc_feature(import_mode) and not _cdc_enabled_for_db(target_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "CDC connectivity is disabled for this ontology"},
        )
    try:
        validate_resource_import_config(
            resource_kind=resource_kind,
            connector_kind=connector_kind,
            import_mode=import_mode,
            config=_resource_import_config_from_source(source_cfg=source_cfg, resource_kind=resource_kind),
        )
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind=resource_kind, message=str(exc))
    return source, mapping


@router.post("/connections/{connectionRid}/tableImports")
@trace_endpoint("bff.foundry_v2_connectivity.create_table_import")
async def create_table_import_v2(
    connectionRid: str,
    payload: Dict[str, Any],
    request: Request,
    preview: bool = Query(default=False),
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(["api:connectivity-write"]),
    *,
    lineage_store: LineageStoreDep,
):
    _ = request
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id, connection, error = await _load_connection_source_or_404(
        connector_registry=connector_registry,
        connection_rid=connectionRid,
    )
    if error is not None:
        return error
    assert connection_id is not None
    assert connection is not None
    parent_error = _validate_parent_rid_matches_connection(payload=payload, connection_id=connection_id)
    if parent_error is not None:
        return parent_error

    connector_kind = _connection_kind_from_source(connection)

    try:
        dataset_rid_input, dataset_db_name, dataset_branch = await _resolve_dataset_context(
            payload=payload,
            dataset_registry=dataset_registry,
        )
    except LookupError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": payload.get("datasetRid")},
        )

    destination_db, destination_branch, destination_object_type = _extract_destination(payload)
    resolved_db = destination_db or dataset_db_name
    resolved_branch = destination_branch or dataset_branch or "main"

    if is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )

    try:
        import_mode = normalize_import_mode(payload.get("importMode"))
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="table_import", message=str(exc))
    if _requires_cdc_feature(import_mode) and not _cdc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "CDC connectivity is disabled for this ontology"},
        )

    if connector_kind == "google_sheets":
        try:
            source_payload = payload.get("source") if isinstance(payload.get("source"), dict) else {}
            sheet_url = str(
                source_payload.get("sheetUrl")
                or source_payload.get("url")
                or (payload.get("config") or {}).get("sheetUrl")
                or (payload.get("config") or {}).get("url")
                or ""
            ).strip()
            if not sheet_url:
                raise ValueError("sheetUrl is required")

            worksheet_name = str(
                source_payload.get("worksheetName")
                or source_payload.get("worksheet")
                or (payload.get("config") or {}).get("worksheetName")
                or ""
            ).strip() or None
            polling_interval = int(payload.get("pollingIntervalSeconds") or source_payload.get("pollingIntervalSeconds") or 300)

            registered = await data_connector_registration_service.register_google_sheet(
                sheet_data={
                    "sheet_url": sheet_url,
                    "worksheet_name": worksheet_name,
                    "polling_interval": polling_interval,
                    "connection_id": connection_id,
                    "database_name": resolved_db,
                    "branch": resolved_branch,
                    "class_label": destination_object_type,
                    "auto_import": bool(resolved_db and destination_object_type),
                    "api_key": source_payload.get("apiKey") or payload.get("apiKey"),
                },
                google_sheets_service=google_sheets_service,
                connector_registry=connector_registry,
                dataset_registry=dataset_registry,
                lineage_store=lineage_store,
            )
            data = registered.get("data") if isinstance(registered, dict) else {}
            table_import_id = str(data.get("sheet_id") or "").strip()
            if not table_import_id:
                raise ValueError("sheet_id missing from registration response")
            dataset_payload = data.get("dataset") if isinstance(data.get("dataset"), dict) else {}
            resolved_dataset_rid = dataset_rid_input
            resolved_dataset_id = str(dataset_payload.get("dataset_id") or "").strip()
            if resolved_dataset_id:
                resolved_dataset_rid = _dataset_rid(resolved_dataset_id)

            source = await connector_registry.get_source(
                source_type=table_import_source_type_for_kind("google_sheets"),
                source_id=table_import_id,
            )
            if source is None:
                raise ValueError("table import source could not be resolved")
            cfg = dict(source.config_json or {})
            display_name, name = _resolved_display_names(
                payload,
                fallback_display_name=f"Google Sheet Import {table_import_id}",
            )
            cfg = _build_connector_import_source_config(
                connection_id=connection_id,
                dataset_rid=resolved_dataset_rid,
                display_name=display_name,
                name=name,
                import_mode=import_mode,
                config_key="table_import_config",
                resource_config=_resolve_table_import_config(
                    payload,
                    connector_kind="google_sheets",
                    sheet_url=str(cfg.get("sheet_url") or sheet_url),
                    worksheet_name=str(cfg.get("worksheet_name") or worksheet_name or "").strip() or None,
                ),
                existing_cfg=cfg,
                branch_name=resolved_branch,
                allow_schema_changes=_resolved_allow_schema_changes(payload, default=False),
            )
            validate_resource_import_config(
                resource_kind="table_import",
                connector_kind="google_sheets",
                import_mode=import_mode,
                config=cfg.get("table_import_config") if isinstance(cfg.get("table_import_config"), dict) else {},
            )
            await connector_registry.upsert_source(
                source_type=source.source_type,
                source_id=source.source_id,
                enabled=True,
                config_json=cfg,
            )
            await _upsert_created_connector_import_mapping(
                connector_registry=connector_registry,
                source_type=source.source_type,
                source_id=source.source_id,
                target_db_name=resolved_db,
                target_branch=resolved_branch,
                target_class_label=destination_object_type,
            )
        except ValueError as exc:
            return _resource_invalid_config_response(resource_kind="table_import", message=str(exc))
    else:
        table_import_id = str(payload.get("tableImportId") or uuid4()).strip()
        source_type = table_import_source_type_for_kind(connector_kind)
        display_name, name = _resolved_display_names(
            payload,
            fallback_display_name=f"{connector_kind} import {table_import_id}",
        )
        allow_schema_changes = _resolved_allow_schema_changes(payload, default=False)
        table_import_config = _resolve_table_import_config(payload, connector_kind=connector_kind)
        try:
            validate_resource_import_config(
                resource_kind="table_import",
                connector_kind=connector_kind,
                import_mode=import_mode,
                config=table_import_config,
            )
        except ValueError as exc:
            return _resource_invalid_config_response(resource_kind="table_import", message=str(exc))

        await connector_registry.upsert_source(
            source_type=source_type,
            source_id=table_import_id,
            enabled=True,
            config_json=_build_connector_import_source_config(
                connection_id=connection_id,
                dataset_rid=dataset_rid_input,
                display_name=display_name,
                name=name,
                import_mode=import_mode,
                config_key="table_import_config",
                resource_config=table_import_config,
                branch_name=resolved_branch,
                allow_schema_changes=allow_schema_changes,
            ),
        )
        await _upsert_created_connector_import_mapping(
            connector_registry=connector_registry,
            source_type=source_type,
            source_id=table_import_id,
            target_db_name=resolved_db,
            target_branch=resolved_branch,
            target_class_label=destination_object_type,
        )

    source, mapping = await _load_table_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        table_import_id=table_import_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"message": "Failed to resolve created table import"},
        )

    return await _build_table_import_response(
        connection_id=connection_id,
        source=source,
        mapping=mapping,
        dataset_registry=dataset_registry,
    )


@router.get("/connections/{connectionRid}/tableImports/{tableImportRid}")
@trace_endpoint("bff.foundry_v2_connectivity.get_table_import")
async def get_table_import_v2(
    connectionRid: str,
    tableImportRid: str,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(["api:connectivity-read"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    table_import_id = _table_import_id_from_rid(tableImportRid)
    if not table_import_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "tableImportRid is invalid"},
        )

    source, mapping = await _load_table_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        table_import_id=table_import_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="TableImportNotFound",
            parameters={"connectionRid": connectionRid, "tableImportRid": tableImportRid},
        )

    return await _build_table_import_response(
        connection_id=connection_id,
        source=source,
        mapping=mapping,
        dataset_registry=dataset_registry,
    )


@router.get("/connections/{connectionRid}/tableImports")
@trace_endpoint("bff.foundry_v2_connectivity.list_table_imports")
async def list_table_imports_v2(
    connectionRid: str,
    preview: bool = Query(default=False),
    pageSize: int | None = Query(default=None),
    pageToken: str | None = Query(default=None),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(["api:connectivity-read"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    connection = await _load_connection_source(connector_registry=connector_registry, connection_id=connection_id)
    if connection is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="ConnectionNotFound",
            parameters={"connectionRid": connectionRid},
        )

    page_size = _parse_page_size_value(pageSize)
    if isinstance(page_size, JSONResponse):
        return page_size
    offset = _parse_page_offset(pageToken)
    if isinstance(offset, JSONResponse):
        return offset

    owned_sources = await _list_connection_owned_sources(
        connector_registry=connector_registry,
        connection_id=connection_id,
        source_types=_table_import_source_types(),
    )
    page_items = owned_sources[offset : offset + page_size]

    data: list[dict[str, Any]] = []
    for src in page_items:
        mapping = await connector_registry.get_mapping(source_type=src.source_type, source_id=src.source_id)
        data.append(
            await _build_table_import_response(
                connection_id=connection_id,
                source=src,
                mapping=mapping,
                dataset_registry=dataset_registry,
            )
        )

    next_token = offset + len(page_items)
    return {
        "data": data,
        "nextPageToken": str(next_token) if next_token < len(owned_sources) else None,
    }


@router.put("/connections/{connectionRid}/tableImports/{tableImportRid}")
@trace_endpoint("bff.foundry_v2_connectivity.replace_table_import")
async def replace_table_import_v2(
    connectionRid: str,
    tableImportRid: str,
    payload: Dict[str, Any],
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )
    parent_error = _validate_parent_rid_matches_connection(payload=payload, connection_id=connection_id)
    if parent_error is not None:
        return parent_error

    table_import_id = _table_import_id_from_rid(tableImportRid)
    if not table_import_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "tableImportRid is invalid"},
        )

    source, mapping = await _load_table_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        table_import_id=table_import_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="TableImportNotFound",
            parameters={"connectionRid": connectionRid, "tableImportRid": tableImportRid},
        )

    cfg = dict(source.config_json or {})
    connector_kind = connector_kind_from_source_type(source.source_type, strict=True)

    try:
        import_mode = normalize_import_mode(payload.get("importMode") or cfg.get("import_mode"))
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="table_import", message=str(exc))

    destination_db, destination_branch, destination_object_type = _extract_destination(payload)
    resolved_db = destination_db or (mapping.target_db_name if mapping else None)

    if is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )
    if _requires_cdc_feature(import_mode) and not _cdc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "CDC connectivity is disabled for this ontology"},
        )

    table_import_config = _resolve_table_import_config(
        payload,
        connector_kind=connector_kind,
        sheet_url=str(cfg.get("sheet_url") or ""),
        worksheet_name=str(cfg.get("worksheet_name") or "").strip() or None,
    )
    try:
        validate_resource_import_config(
            resource_kind="table_import",
            connector_kind=connector_kind,
            import_mode=import_mode,
            config=table_import_config,
        )
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="table_import", message=str(exc))

    display_name, name = _resolved_display_names(
        payload,
        fallback_display_name=cfg.get("display_name") or f"{connector_kind} import {table_import_id}",
        fallback_name=cfg.get("name") or cfg.get("display_name") or f"{connector_kind} import {table_import_id}",
    )
    cfg = _build_connector_import_source_config(
        connection_id=connection_id,
        display_name=display_name,
        name=name,
        import_mode=import_mode,
        config_key="table_import_config",
        resource_config=table_import_config,
        existing_cfg=cfg,
        branch_name=destination_branch,
        allow_schema_changes=_resolved_allow_schema_changes(payload, existing_cfg=cfg),
    )

    await connector_registry.upsert_source(
        source_type=source.source_type,
        source_id=source.source_id,
        enabled=source.enabled,
        config_json=cfg,
    )

    await _upsert_replaced_connector_import_mapping(
        connector_registry=connector_registry,
        source_type=source.source_type,
        source_id=source.source_id,
        mapping=mapping,
        destination_db=destination_db,
        destination_branch=destination_branch,
        destination_object_type=destination_object_type,
    )

    refreshed_source, refreshed_mapping = await _load_table_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        table_import_id=table_import_id,
    )
    if refreshed_source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="TableImportNotFound",
            parameters={"connectionRid": connectionRid, "tableImportRid": tableImportRid},
        )

    return await _build_table_import_response(
        connection_id=connection_id,
        source=refreshed_source,
        mapping=refreshed_mapping,
        dataset_registry=dataset_registry,
    )


@router.delete("/connections/{connectionRid}/tableImports/{tableImportRid}")
@trace_endpoint("bff.foundry_v2_connectivity.delete_table_import")
async def delete_table_import_v2(
    connectionRid: str,
    tableImportRid: str,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    table_import_id = _table_import_id_from_rid(tableImportRid)
    if not table_import_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "tableImportRid is invalid"},
        )

    source, _mapping = await _load_table_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        table_import_id=table_import_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="TableImportNotFound",
            parameters={"connectionRid": connectionRid, "tableImportRid": tableImportRid},
        )

    deleted = await connector_registry.delete_source(
        source_type=source.source_type,
        source_id=source.source_id,
    )
    if not deleted:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="TableImportNotFound",
            parameters={"connectionRid": connectionRid, "tableImportRid": tableImportRid},
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/connections/{connectionRid}/tableImports/{tableImportRid}/execute")
@trace_endpoint("bff.foundry_v2_connectivity.execute_table_import")
async def execute_table_import_v2(
    connectionRid: str,
    tableImportRid: str,
    request: Request,
    preview: bool = Query(default=False),
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
    connector_adapter_factory: ConnectorAdapterFactory = Depends(get_connector_adapter_factory),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    objectify_job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    table_import_id = _table_import_id_from_rid(tableImportRid)
    if not table_import_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "tableImportRid is invalid"},
        )

    loaded = await _load_validated_executable_connector_import(
        connector_registry=connector_registry,
        connection_id=connection_id,
        source_id=table_import_id,
        source_types=_table_import_source_types(),
        resource_kind="table_import",
        resource_display_name="table import",
        resource_error_name="TableImport",
        not_found_parameters={"connectionRid": connectionRid, "tableImportRid": tableImportRid},
    )
    if isinstance(loaded, JSONResponse):
        return loaded
    source, mapping = loaded

    return await _execute_connector_import_build(
        request=request,
        google_sheets_service=google_sheets_service,
        connector_adapter_factory=connector_adapter_factory,
        connector_registry=connector_registry,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        objectify_job_queue=objectify_job_queue,
        connection_id=connection_id,
        source=source,
        mapping=mapping,
        resource_kind="table import",
        resource_kind_key="table_import",
        resource_log_id=tableImportRid,
        resource_rid=_table_import_rid(source.source_id),
        resource_field="tableImportRid",
        pipeline_namespace="table-import",
        pipeline_name_prefix="table_import",
        pipeline_type="connector_table_import",
        location_segment="tableImports",
        start_pipelining=data_connector_pipelining_service.start_pipelining_table_import,
    )


@router.post("/connections/{connectionRid}/fileImports")
@trace_endpoint("bff.foundry_v2_connectivity.create_file_import")
async def create_file_import_v2(
    connectionRid: str,
    payload: Dict[str, Any],
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error

    connection_id, connection, error = await _load_connection_source_or_404(
        connector_registry=connector_registry,
        connection_rid=connectionRid,
    )
    if error is not None:
        return error
    assert connection_id is not None
    assert connection is not None
    parent_error = _validate_parent_rid_matches_connection(payload=payload, connection_id=connection_id)
    if parent_error is not None:
        return parent_error

    connector_kind = _connection_kind_from_source(connection)
    destination_db, destination_branch, destination_object_type = _extract_destination(payload)
    resolved_db = destination_db
    resolved_branch = destination_branch or "main"

    if is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )

    try:
        import_mode = normalize_import_mode(payload.get("importMode"))
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="file_import", message=str(exc))
    if _requires_cdc_feature(import_mode) and not _cdc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "CDC connectivity is disabled for this ontology"},
        )

    file_import_id = str(payload.get("fileImportId") or uuid4()).strip()
    source_type = file_import_source_type_for_kind(connector_kind)
    display_name, name = _resolved_display_names(
        payload,
        fallback_display_name=f"{connector_kind} file import {file_import_id}",
    )
    allow_schema_changes = _resolved_allow_schema_changes(payload, default=False)
    file_import_config = _resolve_file_import_config(payload, connector_kind=connector_kind)
    try:
        validate_resource_import_config(
            resource_kind="file_import",
            connector_kind=connector_kind,
            import_mode=import_mode,
            config=file_import_config,
        )
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="file_import", message=str(exc))

    try:
        dataset_rid_input, dataset_db_name, dataset_branch = await _resolve_dataset_context(
            payload=payload,
            dataset_registry=dataset_registry,
        )
    except LookupError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": payload.get("datasetRid")},
        )
    if not resolved_db:
        resolved_db = dataset_db_name
    if dataset_branch and destination_branch == "main":
        resolved_branch = dataset_branch

    await connector_registry.upsert_source(
        source_type=source_type,
        source_id=file_import_id,
        enabled=True,
        config_json=_build_connector_import_source_config(
            connection_id=connection_id,
            dataset_rid=dataset_rid_input,
            display_name=display_name,
            name=name,
            import_mode=import_mode,
            config_key="file_import_config",
            resource_config=file_import_config,
            branch_name=resolved_branch,
            allow_schema_changes=allow_schema_changes,
            resource_kind="file_import",
        ),
    )
    await _upsert_created_connector_import_mapping(
        connector_registry=connector_registry,
        source_type=source_type,
        source_id=file_import_id,
        target_db_name=resolved_db,
        target_branch=resolved_branch,
        target_class_label=destination_object_type,
    )

    source, mapping = await _load_file_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        file_import_id=file_import_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"message": "Failed to resolve created file import"},
        )

    return await _build_file_import_response(
        connection_id=connection_id,
        source=source,
        mapping=mapping,
        dataset_registry=dataset_registry,
    )


@router.get("/connections/{connectionRid}/fileImports/{fileImportRid}")
@trace_endpoint("bff.foundry_v2_connectivity.get_file_import")
async def get_file_import_v2(
    connectionRid: str,
    fileImportRid: str,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(["api:connectivity-read"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error

    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    file_import_id = _file_import_id_from_rid(fileImportRid)
    if not file_import_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "fileImportRid is invalid"},
        )

    source, mapping = await _load_file_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        file_import_id=file_import_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="FileImportNotFound",
            parameters={"connectionRid": connectionRid, "fileImportRid": fileImportRid},
        )

    return await _build_file_import_response(
        connection_id=connection_id,
        source=source,
        mapping=mapping,
        dataset_registry=dataset_registry,
    )


@router.get("/connections/{connectionRid}/fileImports")
@trace_endpoint("bff.foundry_v2_connectivity.list_file_imports")
async def list_file_imports_v2(
    connectionRid: str,
    preview: bool = Query(default=False),
    pageSize: int | None = Query(default=None),
    pageToken: str | None = Query(default=None),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(["api:connectivity-read"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error

    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    page_size = _parse_page_size_value(pageSize)
    if isinstance(page_size, JSONResponse):
        return page_size
    offset = _parse_page_offset(pageToken)
    if isinstance(offset, JSONResponse):
        return offset

    owned_sources = await _list_connection_owned_sources(
        connector_registry=connector_registry,
        connection_id=connection_id,
        source_types=_file_import_source_types(),
    )
    page_items = owned_sources[offset : offset + page_size]
    data: list[dict[str, Any]] = []
    for src in page_items:
        mapping = await connector_registry.get_mapping(source_type=src.source_type, source_id=src.source_id)
        data.append(
            await _build_file_import_response(
                connection_id=connection_id,
                source=src,
                mapping=mapping,
                dataset_registry=dataset_registry,
            )
        )
    next_token = offset + len(page_items)
    return {"data": data, "nextPageToken": str(next_token) if next_token < len(owned_sources) else None}


@router.put("/connections/{connectionRid}/fileImports/{fileImportRid}")
@trace_endpoint("bff.foundry_v2_connectivity.replace_file_import")
async def replace_file_import_v2(
    connectionRid: str,
    fileImportRid: str,
    payload: Dict[str, Any],
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error

    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )
    parent_error = _validate_parent_rid_matches_connection(payload=payload, connection_id=connection_id)
    if parent_error is not None:
        return parent_error
    file_import_id = _file_import_id_from_rid(fileImportRid)
    if not file_import_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "fileImportRid is invalid"},
        )

    source, mapping = await _load_file_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        file_import_id=file_import_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="FileImportNotFound",
            parameters={"connectionRid": connectionRid, "fileImportRid": fileImportRid},
        )

    cfg = dict(source.config_json or {})
    connector_kind = connector_kind_from_source_type(source.source_type, strict=True)
    try:
        import_mode = normalize_import_mode(payload.get("importMode") or cfg.get("import_mode"))
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="file_import", message=str(exc))

    destination_db, destination_branch, destination_object_type = _extract_destination(payload)
    resolved_db = destination_db or (mapping.target_db_name if mapping else None)
    if is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )
    if _requires_cdc_feature(import_mode) and not _cdc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "CDC connectivity is disabled for this ontology"},
        )

    file_import_config = _resolve_file_import_config(payload, connector_kind=connector_kind)
    try:
        validate_resource_import_config(
            resource_kind="file_import",
            connector_kind=connector_kind,
            import_mode=import_mode,
            config=file_import_config,
        )
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="file_import", message=str(exc))
    display_name, name = _resolved_display_names(
        payload,
        fallback_display_name=cfg.get("display_name") or f"{connector_kind} file import {file_import_id}",
        fallback_name=cfg.get("name") or cfg.get("display_name") or f"{connector_kind} file import {file_import_id}",
    )
    cfg = _build_connector_import_source_config(
        connection_id=connection_id,
        display_name=display_name,
        name=name,
        import_mode=import_mode,
        config_key="file_import_config",
        resource_config=file_import_config,
        existing_cfg=cfg,
        branch_name=destination_branch,
        allow_schema_changes=_resolved_allow_schema_changes(payload, existing_cfg=cfg),
    )
    await connector_registry.upsert_source(
        source_type=source.source_type,
        source_id=source.source_id,
        enabled=source.enabled,
        config_json=cfg,
    )

    await _upsert_replaced_connector_import_mapping(
        connector_registry=connector_registry,
        source_type=source.source_type,
        source_id=source.source_id,
        mapping=mapping,
        destination_db=destination_db,
        destination_branch=destination_branch,
        destination_object_type=destination_object_type,
    )

    refreshed_source, refreshed_mapping = await _load_file_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        file_import_id=file_import_id,
    )
    if refreshed_source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="FileImportNotFound",
            parameters={"connectionRid": connectionRid, "fileImportRid": fileImportRid},
        )
    return await _build_file_import_response(
        connection_id=connection_id,
        source=refreshed_source,
        mapping=refreshed_mapping,
        dataset_registry=dataset_registry,
    )


@router.delete("/connections/{connectionRid}/fileImports/{fileImportRid}")
@trace_endpoint("bff.foundry_v2_connectivity.delete_file_import")
async def delete_file_import_v2(
    connectionRid: str,
    fileImportRid: str,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error

    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )
    file_import_id = _file_import_id_from_rid(fileImportRid)
    if not file_import_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "fileImportRid is invalid"},
        )

    source, _mapping = await _load_file_import_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        file_import_id=file_import_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="FileImportNotFound",
            parameters={"connectionRid": connectionRid, "fileImportRid": fileImportRid},
        )
    deleted = await connector_registry.delete_source(source_type=source.source_type, source_id=source.source_id)
    if not deleted:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="FileImportNotFound",
            parameters={"connectionRid": connectionRid, "fileImportRid": fileImportRid},
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/connections/{connectionRid}/fileImports/{fileImportRid}/execute")
@trace_endpoint("bff.foundry_v2_connectivity.execute_file_import")
async def execute_file_import_v2(
    connectionRid: str,
    fileImportRid: str,
    request: Request,
    preview: bool = Query(default=False),
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
    connector_adapter_factory: ConnectorAdapterFactory = Depends(get_connector_adapter_factory),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    objectify_job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error

    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )
    file_import_id = _file_import_id_from_rid(fileImportRid)
    if not file_import_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "fileImportRid is invalid"},
        )

    loaded = await _load_validated_executable_connector_import(
        connector_registry=connector_registry,
        connection_id=connection_id,
        source_id=file_import_id,
        source_types=_file_import_source_types(),
        resource_kind="file_import",
        resource_display_name="file import",
        resource_error_name="FileImport",
        not_found_parameters={"connectionRid": connectionRid, "fileImportRid": fileImportRid},
    )
    if isinstance(loaded, JSONResponse):
        return loaded
    source, mapping = loaded

    return await _execute_connector_import_build(
        request=request,
        google_sheets_service=google_sheets_service,
        connector_adapter_factory=connector_adapter_factory,
        connector_registry=connector_registry,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        objectify_job_queue=objectify_job_queue,
        connection_id=connection_id,
        source=source,
        mapping=mapping,
        resource_kind="file import",
        resource_kind_key="file_import",
        resource_log_id=fileImportRid,
        resource_rid=_file_import_rid(source.source_id),
        resource_field="fileImportRid",
        pipeline_namespace="file-import",
        pipeline_name_prefix="file_import",
        pipeline_type="connector_file_import",
        location_segment="fileImports",
        start_pipelining=data_connector_pipelining_service.start_pipelining_file_import,
    )


@router.post("/connections/{connectionRid}/virtualTables/{virtualTableRid}/execute")
@trace_endpoint("bff.foundry_v2_connectivity.execute_virtual_table")
async def execute_virtual_table_v2(
    connectionRid: str,
    virtualTableRid: str,
    request: Request,
    preview: bool = Query(default=False),
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
    connector_adapter_factory: ConnectorAdapterFactory = Depends(get_connector_adapter_factory),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    objectify_job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error

    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )
    virtual_table_id = _virtual_table_id_from_rid(virtualTableRid)
    if not virtual_table_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "virtualTableRid is invalid"},
        )

    loaded = await _load_validated_executable_connector_import(
        connector_registry=connector_registry,
        connection_id=connection_id,
        source_id=virtual_table_id,
        source_types=_virtual_table_source_types(),
        resource_kind="virtual_table",
        resource_display_name="virtual table",
        resource_error_name="VirtualTable",
        not_found_parameters={"connectionRid": connectionRid, "virtualTableRid": virtualTableRid},
        validate_cdc=False,
    )
    if isinstance(loaded, JSONResponse):
        return loaded
    source, mapping = loaded

    return await _execute_connector_import_build(
        request=request,
        google_sheets_service=google_sheets_service,
        connector_adapter_factory=connector_adapter_factory,
        connector_registry=connector_registry,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        objectify_job_queue=objectify_job_queue,
        connection_id=connection_id,
        source=source,
        mapping=mapping,
        resource_kind="virtual table",
        resource_kind_key="virtual_table",
        resource_log_id=virtualTableRid,
        resource_rid=_virtual_table_rid(source.source_id),
        resource_field="virtualTableRid",
        pipeline_namespace="virtual-table",
        pipeline_name_prefix="virtual_table",
        pipeline_type="connector_virtual_table",
        location_segment="virtualTables",
        start_pipelining=data_connector_pipelining_service.start_pipelining_virtual_table,
    )


@router.post("/connections/{connectionRid}/virtualTables")
@trace_endpoint("bff.foundry_v2_connectivity.create_virtual_table")
async def create_virtual_table_v2(
    connectionRid: str,
    payload: Dict[str, Any],
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error

    connection_id, connection, error = await _load_connection_source_or_404(
        connector_registry=connector_registry,
        connection_rid=connectionRid,
    )
    if error is not None:
        return error
    assert connection_id is not None
    assert connection is not None
    parent_error = _validate_parent_rid_matches_connection(payload=payload, connection_id=connection_id)
    if parent_error is not None:
        return parent_error

    connector_kind = _connection_kind_from_source(connection)
    destination_db, destination_branch, destination_object_type = _extract_destination(payload)
    resolved_db = destination_db
    resolved_branch = destination_branch or "main"
    if is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )

    try:
        import_mode = normalize_import_mode(payload.get("importMode") or "SNAPSHOT")
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="virtual_table", message=str(exc))

    virtual_table_id = str(payload.get("virtualTableId") or uuid4()).strip()
    source_type = virtual_table_source_type_for_kind(connector_kind)
    virtual_table_config = _resolve_virtual_table_config(payload, connector_kind=connector_kind)
    try:
        validate_resource_import_config(
            resource_kind="virtual_table",
            connector_kind=connector_kind,
            import_mode=import_mode,
            config=virtual_table_config,
        )
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="virtual_table", message=str(exc))
    display_name, name = _resolved_display_names(
        payload,
        fallback_display_name=f"{connector_kind} virtual table {virtual_table_id}",
    )

    try:
        dataset_rid_input, dataset_db_name, dataset_branch = await _resolve_dataset_context(
            payload=payload,
            dataset_registry=dataset_registry,
        )
    except LookupError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="DatasetNotFound",
            parameters={"datasetRid": payload.get("datasetRid")},
        )
    if not resolved_db:
        resolved_db = dataset_db_name
    if dataset_branch and destination_branch == "main":
        resolved_branch = dataset_branch

    await connector_registry.upsert_source(
        source_type=source_type,
        source_id=virtual_table_id,
        enabled=True,
        config_json=_build_connector_import_source_config(
            connection_id=connection_id,
            dataset_rid=dataset_rid_input,
            display_name=display_name,
            name=name,
            import_mode=import_mode,
            config_key="virtual_table_config",
            resource_config=virtual_table_config,
            branch_name=resolved_branch,
            resource_kind="virtual_table",
        ),
    )
    await _upsert_created_connector_import_mapping(
        connector_registry=connector_registry,
        source_type=source_type,
        source_id=virtual_table_id,
        target_db_name=resolved_db,
        target_branch=resolved_branch,
        target_class_label=destination_object_type,
    )

    source, mapping = await _load_virtual_table_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        virtual_table_id=virtual_table_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"message": "Failed to resolve created virtual table"},
        )
    return await _build_virtual_table_response(
        connection_id=connection_id,
        source=source,
        mapping=mapping,
        dataset_registry=dataset_registry,
    )


@router.get("/connections/{connectionRid}/virtualTables/{virtualTableRid}")
@trace_endpoint("bff.foundry_v2_connectivity.get_virtual_table")
async def get_virtual_table_v2(
    connectionRid: str,
    virtualTableRid: str,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(["api:connectivity-read"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error

    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )
    virtual_table_id = _virtual_table_id_from_rid(virtualTableRid)
    if not virtual_table_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "virtualTableRid is invalid"},
        )

    source, mapping = await _load_virtual_table_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        virtual_table_id=virtual_table_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="VirtualTableNotFound",
            parameters={"connectionRid": connectionRid, "virtualTableRid": virtualTableRid},
        )

    return await _build_virtual_table_response(
        connection_id=connection_id,
        source=source,
        mapping=mapping,
        dataset_registry=dataset_registry,
    )


@router.get("/connections/{connectionRid}/virtualTables")
@trace_endpoint("bff.foundry_v2_connectivity.list_virtual_tables")
async def list_virtual_tables_v2(
    connectionRid: str,
    preview: bool = Query(default=False),
    pageSize: int | None = Query(default=None),
    pageToken: str | None = Query(default=None),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(["api:connectivity-read"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error

    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )

    page_size = _parse_page_size_value(pageSize)
    if isinstance(page_size, JSONResponse):
        return page_size
    offset = _parse_page_offset(pageToken)
    if isinstance(offset, JSONResponse):
        return offset

    owned_sources = await _list_connection_owned_sources(
        connector_registry=connector_registry,
        connection_id=connection_id,
        source_types=_virtual_table_source_types(),
    )
    page_items = owned_sources[offset : offset + page_size]
    data: list[dict[str, Any]] = []
    for src in page_items:
        mapping = await connector_registry.get_mapping(source_type=src.source_type, source_id=src.source_id)
        data.append(
            await _build_virtual_table_response(
                connection_id=connection_id,
                source=src,
                mapping=mapping,
                dataset_registry=dataset_registry,
            )
        )
    next_token = offset + len(page_items)
    return {"data": data, "nextPageToken": str(next_token) if next_token < len(owned_sources) else None}


@router.put("/connections/{connectionRid}/virtualTables/{virtualTableRid}")
@trace_endpoint("bff.foundry_v2_connectivity.replace_virtual_table")
async def replace_virtual_table_v2(
    connectionRid: str,
    virtualTableRid: str,
    payload: Dict[str, Any],
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error

    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )
    parent_error = _validate_parent_rid_matches_connection(payload=payload, connection_id=connection_id)
    if parent_error is not None:
        return parent_error
    virtual_table_id = _virtual_table_id_from_rid(virtualTableRid)
    if not virtual_table_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "virtualTableRid is invalid"},
        )

    source, mapping = await _load_virtual_table_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        virtual_table_id=virtual_table_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="VirtualTableNotFound",
            parameters={"connectionRid": connectionRid, "virtualTableRid": virtualTableRid},
        )

    cfg = dict(source.config_json or {})
    connector_kind = connector_kind_from_source_type(source.source_type, strict=True)
    destination_db, destination_branch, destination_object_type = _extract_destination(payload)
    resolved_db = destination_db or (mapping.target_db_name if mapping else None)
    try:
        import_mode = normalize_import_mode(payload.get("importMode") or cfg.get("import_mode") or "SNAPSHOT")
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="virtual_table", message=str(exc))
    if is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )
    virtual_table_config = _resolve_virtual_table_config(payload, connector_kind=connector_kind)
    try:
        validate_resource_import_config(
            resource_kind="virtual_table",
            connector_kind=connector_kind,
            import_mode=import_mode,
            config=virtual_table_config,
        )
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="virtual_table", message=str(exc))

    display_name, name = _resolved_display_names(
        payload,
        fallback_display_name=cfg.get("display_name") or f"{connector_kind} virtual table {virtual_table_id}",
        fallback_name=cfg.get("name") or cfg.get("display_name") or f"{connector_kind} virtual table {virtual_table_id}",
    )
    cfg = _build_connector_import_source_config(
        connection_id=connection_id,
        display_name=display_name,
        name=name,
        import_mode=import_mode,
        config_key="virtual_table_config",
        resource_config=virtual_table_config,
        existing_cfg=cfg,
        branch_name=destination_branch,
    )
    await connector_registry.upsert_source(
        source_type=source.source_type,
        source_id=source.source_id,
        enabled=source.enabled,
        config_json=cfg,
    )

    await _upsert_replaced_connector_import_mapping(
        connector_registry=connector_registry,
        source_type=source.source_type,
        source_id=source.source_id,
        mapping=mapping,
        destination_db=destination_db,
        destination_branch=destination_branch,
        destination_object_type=destination_object_type,
    )

    refreshed_source, refreshed_mapping = await _load_virtual_table_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        virtual_table_id=virtual_table_id,
    )
    if refreshed_source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="VirtualTableNotFound",
            parameters={"connectionRid": connectionRid, "virtualTableRid": virtualTableRid},
        )
    return await _build_virtual_table_response(
        connection_id=connection_id,
        source=refreshed_source,
        mapping=refreshed_mapping,
        dataset_registry=dataset_registry,
    )


@router.delete("/connections/{connectionRid}/virtualTables/{virtualTableRid}")
@trace_endpoint("bff.foundry_v2_connectivity.delete_virtual_table")
async def delete_virtual_table_v2(
    connectionRid: str,
    virtualTableRid: str,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error

    connection_id = _connection_id_from_rid(connectionRid)
    if not connection_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "connectionRid is invalid"},
        )
    virtual_table_id = _virtual_table_id_from_rid(virtualTableRid)
    if not virtual_table_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "virtualTableRid is invalid"},
        )

    source, _mapping = await _load_virtual_table_source(
        connector_registry=connector_registry,
        connection_id=connection_id,
        virtual_table_id=virtual_table_id,
    )
    if source is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="VirtualTableNotFound",
            parameters={"connectionRid": connectionRid, "virtualTableRid": virtualTableRid},
        )
    deleted = await connector_registry.delete_source(source_type=source.source_type, source_id=source.source_id)
    if not deleted:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="VirtualTableNotFound",
            parameters={"connectionRid": connectionRid, "virtualTableRid": virtualTableRid},
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/connections")
@trace_endpoint("bff.foundry_v2_connectivity.create_connection")
async def create_connection_v2(
    payload: Dict[str, Any],
    request: Request,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    connector_adapter_factory: ConnectorAdapterFactory = Depends(get_connector_adapter_factory),
    _: None = require_scopes(["api:connectivity-write"]),
) -> JSONResponse:
    _ = request
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    display_name = str(payload.get("displayName") or payload.get("name") or "").strip()
    if not display_name:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "displayName is required"},
        )

    configuration = _extract_connection_configuration(payload)
    kind = resolve_connector_kind_from_connection_config(configuration)
    if kind not in SUPPORTED_CONNECTOR_KINDS:
        kind = "google_sheets"

    # Connection create has no ontology context, so JDBC rollout is gated by global switch.
    if is_jdbc_connector_kind(kind):
        if not bool(getattr(_feature_flags_settings(), "enable_foundry_connectivity_jdbc", False)):
            return _foundry_error(
                status.HTTP_403_FORBIDDEN,
                error_code="PERMISSION_DENIED",
                error_name="ConnectivityFeatureDisabled",
                parameters={"message": "JDBC connectivity is disabled"},
            )

    connection_id = str(uuid4())
    source_type = connection_source_type_for_kind(kind)
    parent_folder_rid = str(payload.get("parentFolderRid") or "").strip() or _DEFAULT_PARENT_FOLDER_RID
    export_settings = _normalize_export_settings(payload.get("exportSettings"))
    markings = _normalize_markings(payload.get("markings"))

    config_json = _normalize_connection_config_for_storage(configuration, kind=kind)
    config_json.update(
        {
            "display_name": display_name,
            "name": display_name,
            "status": "CONNECTED",
            "connector_kind": kind,
            "parent_folder_rid": parent_folder_rid,
            "export_settings": export_settings,
        }
    )
    if markings:
        config_json["markings"] = markings

    try:
        await connector_registry.upsert_source(
            source_type=source_type,
            source_id=connection_id,
            enabled=True,
            config_json=config_json,
        )

        secrets = _extract_secrets_from_configuration(configuration, kind=kind)
        if secrets:
            await connector_registry.upsert_connection_secrets(
                source_type=source_type,
                source_id=connection_id,
                secrets_json=secrets,
            )
    except Exception as exc:
        logger.error("Failed to create connection: %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="ConnectionCreationFailed",
            parameters={"message": "Failed to create connection"},
        )

    created = await connector_registry.get_source(source_type=source_type, source_id=connection_id)
    if created is None:
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="ConnectionCreationFailed",
            parameters={"message": "Connection created but could not be retrieved"},
        )

    return JSONResponse(
        status_code=status.HTTP_201_CREATED,
        content=await _connection_response(source=created, connector_adapter_factory=connector_adapter_factory),
    )


@router.get("/connections/{connectionRid}")
@trace_endpoint("bff.foundry_v2_connectivity.get_connection")
async def get_connection_v2(
    connectionRid: str,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    connector_adapter_factory: ConnectorAdapterFactory = Depends(get_connector_adapter_factory),
    _: None = require_scopes(["api:connectivity-read"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id, source, error = await _load_connection_source_or_404(
        connector_registry=connector_registry,
        connection_rid=connectionRid,
    )
    _ = connection_id
    if error is not None:
        return error
    assert source is not None
    return await _connection_response(source=source, connector_adapter_factory=connector_adapter_factory)


@router.get("/connections")
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("bff.foundry_v2_connectivity.list_connections")
async def list_connections_v2(
    request: Request,
    preview: bool = Query(default=False),
    pageSize: int = Query(default=100, ge=1, le=500),
    pageToken: str | None = Query(default=None),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    connector_adapter_factory: ConnectorAdapterFactory = Depends(get_connector_adapter_factory),
    _: None = require_scopes(["api:connectivity-read"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    offset = _parse_page_offset(pageToken)
    if isinstance(offset, JSONResponse):
        return offset

    all_connections: list[ConnectorSource] = []
    for source_type in _connection_source_types():
        all_connections.extend(
            await _list_all_enabled_sources_for_type(
                connector_registry=connector_registry,
                source_type=source_type,
            )
        )
    all_connections.sort(key=lambda src: str(src.source_id))

    page_items = all_connections[offset : offset + pageSize]
    data = [await _connection_response(source=conn, connector_adapter_factory=connector_adapter_factory) for conn in page_items]

    next_token = offset + len(page_items)
    return {
        "data": data,
        "nextPageToken": str(next_token) if next_token < len(all_connections) else None,
    }


@router.delete("/connections/{connectionRid}")
@trace_endpoint("bff.foundry_v2_connectivity.delete_connection")
async def delete_connection_v2(
    connectionRid: str,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id, source, error = await _load_connection_source_or_404(
        connector_registry=connector_registry,
        connection_rid=connectionRid,
    )
    if error is not None:
        return error
    assert connection_id is not None
    assert source is not None

    deleted = bool(
        await connector_registry.delete_source(
            source_type=source.source_type,
            source_id=connection_id,
        )
    )
    if not deleted:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="ConnectionNotFound",
            parameters={"connectionRid": connectionRid},
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/connections/{connectionRid}/test")
@trace_endpoint("bff.foundry_v2_connectivity.test_connection")
async def test_connection_v2(
    connectionRid: str,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    connector_adapter_factory: ConnectorAdapterFactory = Depends(get_connector_adapter_factory),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id, source, error = await _load_connection_source_or_404(
        connector_registry=connector_registry,
        connection_rid=connectionRid,
    )
    if error is not None:
        return error
    assert connection_id is not None
    assert source is not None

    kind = _connection_kind_from_source(source)
    adapter = connector_adapter_factory.get_adapter(kind)
    secrets = await connector_registry.get_connection_secrets(
        source_type=source.source_type,
        source_id=source.source_id,
    )
    cfg = dict(source.config_json or {})

    if not secrets and not any(cfg.get(k) for k in {"access_token", "refresh_token", "api_key", "password", "dsn"}):
        return {
            "connectionRid": _connection_rid(connection_id),
            "status": "WARNING",
            "message": "No credentials configured",
        }

    result = await adapter.test_connection(config=cfg, secrets=secrets)
    test_result: dict[str, Any] = {
        "connectionRid": _connection_rid(connection_id),
        "status": "SUCCEEDED" if result.ok else "FAILED",
        "message": result.message,
    }
    if result.details:
        test_result["details"] = result.details

    try:
        merged_cfg = dict(cfg)
        merged_cfg["status"] = "CONNECTED" if result.ok else "ERROR"
        await connector_registry.upsert_source(
            source_type=source.source_type,
            source_id=connection_id,
            enabled=source.enabled,
            config_json=merged_cfg,
        )
    except Exception as status_exc:
        logger.warning(
            "Failed to persist connection test status after test run (connection_id=%s): %s",
            connection_id,
            status_exc,
            exc_info=True,
        )

    return test_result


@router.get("/connections/{connectionRid}/getConfiguration")
@trace_endpoint("bff.foundry_v2_connectivity.get_connection_configuration")
async def get_connection_configuration_v2(
    connectionRid: str,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    connector_adapter_factory: ConnectorAdapterFactory = Depends(get_connector_adapter_factory),
    _: None = require_scopes(["api:connectivity-read"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id, source, error = await _load_connection_source_or_404(
        connector_registry=connector_registry,
        connection_rid=connectionRid,
    )
    if error is not None:
        return error
    assert connection_id is not None
    assert source is not None

    config = await _connection_configuration(
        source=source,
        connector_adapter_factory=connector_adapter_factory,
    )
    return {
        "connectionRid": _connection_rid(connection_id),
        "connectionConfiguration": config,
        # Compatibility alias for existing internal clients.
        "configuration": config,
    }


@router.post("/connections/getConfigurationBatch")
@trace_endpoint("bff.foundry_v2_connectivity.get_connection_configuration_batch")
async def get_connection_configuration_batch_v2(
    payload: list[Dict[str, Any]] = Body(...),
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    connector_adapter_factory: ConnectorAdapterFactory = Depends(get_connector_adapter_factory),
    _: None = require_scopes(["api:connectivity-read"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error

    requested: list[str] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        rid = str(item.get("connectionRid") or "").strip()
        if rid:
            requested.append(rid)
    if not requested:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "At least one connectionRid is required"},
        )
    if len(requested) > 200:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "Maximum batch size is 200"},
        )

    data: Dict[str, Dict[str, Any]] = {}
    for rid in requested:
        connection_id = _connection_id_from_rid(rid)
        if not connection_id:
            continue
        source = await _load_connection_source(connector_registry=connector_registry, connection_id=connection_id)
        if source is None:
            continue
        data[rid] = await _connection_configuration(
            source=source,
            connector_adapter_factory=connector_adapter_factory,
        )

    return {"data": data}


@router.post("/connections/{connectionRid}/updateSecrets")
@trace_endpoint("bff.foundry_v2_connectivity.update_connection_secrets")
async def update_connection_secrets_v2(
    connectionRid: str,
    payload: Dict[str, Any],
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id, source, error = await _load_connection_source_or_404(
        connector_registry=connector_registry,
        connection_rid=connectionRid,
    )
    if error is not None:
        return error
    assert connection_id is not None
    assert source is not None

    kind = _connection_kind_from_source(source)
    incoming_secrets = _extract_secrets_from_payload(payload, kind=kind)
    if not incoming_secrets:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "No supported secret fields were provided"},
        )

    existing = await connector_registry.get_connection_secrets(
        source_type=source.source_type,
        source_id=source.source_id,
    )
    merged = dict(existing)
    merged.update(incoming_secrets)
    await connector_registry.upsert_connection_secrets(
        source_type=source.source_type,
        source_id=source.source_id,
        secrets_json=merged,
    )

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/connections/{connectionRid}/updateExportSettings")
@trace_endpoint("bff.foundry_v2_connectivity.update_connection_export_settings")
async def update_connection_export_settings_v2(
    connectionRid: str,
    payload: Dict[str, Any],
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id, source, error = await _load_connection_source_or_404(
        connector_registry=connector_registry,
        connection_rid=connectionRid,
    )
    if error is not None:
        return error
    assert connection_id is not None
    assert source is not None

    settings_payload = payload.get("exportSettings") if isinstance(payload.get("exportSettings"), dict) else payload
    export_settings = _normalize_export_settings(settings_payload)
    markings = _normalize_markings(payload.get("markings"))

    cfg = dict(source.config_json or {})
    cfg["export_settings"] = export_settings
    if markings:
        cfg["markings"] = markings
    await connector_registry.upsert_source(
        source_type=source.source_type,
        source_id=source.source_id,
        enabled=source.enabled,
        config_json=cfg,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/connections/{connectionRid}/exportRuns")
@trace_endpoint("bff.foundry_v2_connectivity.create_connection_export_run")
async def create_connection_export_run_v2(
    connectionRid: str,
    payload: Dict[str, Any],
    request: Request,
    task_manager: BackgroundTaskManagerDep,
    audit_store: AuditLogStoreDep,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    _: None = require_scopes(["api:connectivity-write"]),
) -> JSONResponse:
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id, source, error = await _load_connection_source_or_404(
        connector_registry=connector_registry,
        connection_rid=connectionRid,
    )
    if error is not None:
        return error
    assert connection_id is not None
    assert source is not None

    cfg = dict(source.config_json or {})
    export_settings = _normalize_export_settings(cfg.get("export_settings") or cfg.get("exportSettings"))
    markings = _normalize_markings(cfg.get("markings"))
    if not bool(export_settings.get("exportsEnabled", True)):
        return _foundry_error(
            status.HTTP_409_CONFLICT,
            error_code="CONFLICT",
            error_name="ExportDisabled",
            parameters={"message": "exportsEnabled is false for this connection"},
        )

    target = payload.get("target") if isinstance(payload.get("target"), dict) else {}
    target_url = str(
        payload.get("targetUrl")
        or payload.get("webhookUrl")
        or target.get("url")
        or ""
    ).strip()
    if not _is_valid_http_url(target_url):
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "targetUrl/webhookUrl must be an absolute http(s) URL"},
        )

    try:
        method = _normalize_export_run_method(payload.get("method") or target.get("method"))
    except ValueError as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": str(exc)},
        )

    request_headers = _normalize_export_run_headers(payload.get("headers") or target.get("headers"))
    timeout_seconds = _normalize_export_run_timeout_seconds(payload.get("timeoutMs"))
    dry_run = _coerce_bool(payload.get("dryRun"), default=False)
    export_payload = payload.get("payload")
    bypass_markings_validation = bool(export_settings.get("exportEnabledWithoutMarkingsValidation", False))
    if not dry_run and not bypass_markings_validation and not markings:
        return _foundry_error(
            status.HTTP_409_CONFLICT,
            error_code="CONFLICT",
            error_name="ExportMarkingsRequired",
            parameters={
                "message": "markings are required unless exportEnabledWithoutMarkingsValidation=true",
            },
        )

    run_id = str(uuid4())
    actor = (request.headers.get("X-User-ID") or "").strip() or None
    task_id = await task_manager.create_task(
        _run_connection_export,
        task_id=run_id,
        task_name=f"Connectivity export run ({connection_id})",
        task_type="connectivity_export_run",
        metadata={
            "connection_id": connection_id,
            "connection_rid": _connection_rid(connection_id),
            "target_url": target_url,
            "method": method,
            "dry_run": bool(dry_run),
            "requested_at": _iso_timestamp(utcnow()),
            "requested_by": actor,
        },
        connection_id=connection_id,
        connection_rid=_connection_rid(connection_id),
        target_url=target_url,
        method=method,
        request_headers=request_headers,
        payload=export_payload,
        timeout_seconds=timeout_seconds,
        dry_run=dry_run,
        actor=actor,
        audit_store=audit_store,
    )

    task = await task_manager.get_task_status(task_id)
    if task is None:
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="ExportRunFailed",
            parameters={"message": "Failed to create export run task"},
        )

    response = _export_run_response_from_task(connection_id=connection_id, task=task)
    response["status"] = "QUEUED" if response.get("status") == "RUNNING" else response.get("status")
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response)


@router.get("/connections/{connectionRid}/exportRuns/{exportRunRid}")
@trace_endpoint("bff.foundry_v2_connectivity.get_connection_export_run")
async def get_connection_export_run_v2(
    connectionRid: str,
    exportRunRid: str,
    task_manager: BackgroundTaskManagerDep,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    _: None = require_scopes(["api:connectivity-read"]),
) -> JSONResponse:
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id, source, error = await _load_connection_source_or_404(
        connector_registry=connector_registry,
        connection_rid=connectionRid,
    )
    if error is not None:
        return error
    assert connection_id is not None
    assert source is not None

    run_id = _export_run_id_from_rid(exportRunRid)
    if not run_id:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "exportRunRid is invalid"},
        )

    task = await task_manager.get_task_status(run_id)
    if task is None:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="ExportRunNotFound",
            parameters={"exportRunRid": exportRunRid, "connectionRid": connectionRid},
        )

    metadata = getattr(task, "metadata", {}) if isinstance(getattr(task, "metadata", {}), dict) else {}
    if str(metadata.get("connection_id") or "").strip() != connection_id:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="ExportRunNotFound",
            parameters={"exportRunRid": exportRunRid, "connectionRid": connectionRid},
        )

    return JSONResponse(content=_export_run_response_from_task(connection_id=connection_id, task=task))


@router.get("/connections/{connectionRid}/exportRuns")
@trace_endpoint("bff.foundry_v2_connectivity.list_connection_export_runs")
async def list_connection_export_runs_v2(
    connectionRid: str,
    task_manager: BackgroundTaskManagerDep,
    preview: bool = Query(default=False),
    pageSize: int = Query(default=100, ge=1, le=500),
    pageToken: str | None = Query(default=None),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    _: None = require_scopes(["api:connectivity-read"]),
) -> JSONResponse:
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id, source, error = await _load_connection_source_or_404(
        connector_registry=connector_registry,
        connection_rid=connectionRid,
    )
    if error is not None:
        return error
    assert connection_id is not None
    assert source is not None

    offset = _parse_page_offset(pageToken)
    if isinstance(offset, JSONResponse):
        return offset

    tasks = await _list_all_connectivity_export_tasks(task_manager)
    scoped_tasks = []
    for task in tasks:
        metadata = getattr(task, "metadata", {}) if isinstance(getattr(task, "metadata", {}), dict) else {}
        if str(metadata.get("connection_id") or "").strip() == connection_id:
            scoped_tasks.append(task)

    page = scoped_tasks[offset : offset + pageSize]
    data = [_export_run_response_from_task(connection_id=connection_id, task=item) for item in page]
    next_token = offset + len(page)
    return JSONResponse(
        content={
            "data": data,
            "nextPageToken": str(next_token) if next_token < len(scoped_tasks) else None,
        }
    )


@router.post("/connections/{connectionRid}/uploadCustomJdbcDrivers")
@trace_endpoint("bff.foundry_v2_connectivity.upload_custom_jdbc_drivers")
async def upload_custom_jdbc_drivers_v2(
    connectionRid: str,
    request: Request,
    driverBytes: bytes = Body(..., media_type="application/octet-stream"),
    fileName: str = Query(...),
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    connector_adapter_factory: ConnectorAdapterFactory = Depends(get_connector_adapter_factory),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    _: None = require_scopes(["api:connectivity-write"]),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    connection_id, source, error = await _load_connection_source_or_404(
        connector_registry=connector_registry,
        connection_rid=connectionRid,
    )
    if error is not None:
        return error
    assert connection_id is not None
    assert source is not None

    kind = _connection_kind_from_source(source)
    if not is_jdbc_connector_kind(kind):
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "uploadCustomJdbcDrivers is only supported for JDBC connections"},
        )

    normalized_file_name = _normalize_jdbc_driver_file_name(fileName)
    if not normalized_file_name:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "fileName is invalid"},
        )

    content = bytes(driverBytes or b"")
    if not content:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "Request body must contain driver bytes"},
        )

    sha256_hex = hashlib.sha256(content).hexdigest()
    repository = str(get_settings().storage.lakefs_artifacts_repository or "").strip() or "pipeline-artifacts"
    object_key = f"connectivity/jdbc-drivers/{connection_id}/{sha256_hex}/{normalized_file_name}"
    actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
    storage = await pipeline_registry.get_lakefs_storage(user_id=actor_user_id)
    content_type = str(request.headers.get("Content-Type") or "application/octet-stream").strip() or "application/octet-stream"
    await storage.save_bytes(
        repository,
        f"main/{object_key}",
        content,
        content_type=content_type,
        metadata={
            "connectionRid": _connection_rid(connection_id),
            "connectorKind": kind,
            "sha256": sha256_hex,
        },
    )

    cfg = _upsert_custom_jdbc_driver_metadata(
        config_json=dict(source.config_json or {}),
        file_name=normalized_file_name,
        sha256_hex=sha256_hex,
        size_bytes=len(content),
        repository=repository,
        object_key=object_key,
    )
    updated = await connector_registry.upsert_source(
        source_type=source.source_type,
        source_id=source.source_id,
        enabled=source.enabled,
        config_json=cfg,
    )
    return await _connection_response(
        source=updated,
        connector_adapter_factory=connector_adapter_factory,
    )
