import hashlib
import logging
from typing import Any, Dict
from uuid import NAMESPACE_URL, uuid4, uuid5

from fastapi import APIRouter, Body, Depends, Query, Request, Response, status
from fastapi.responses import JSONResponse

from bff.routers.data_connector_deps import (
    get_connector_adapter_factory,
    get_connector_registry,
    get_dataset_registry,
    get_google_sheets_service,
    get_objectify_job_queue,
    get_objectify_registry,
    get_pipeline_registry,
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
from data_connector.google_sheets.service import GoogleSheetsService
from shared.config.settings import get_settings
from shared.dependencies.providers import LineageStoreDep
from shared.observability.tracing import trace_endpoint
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.connector_registry import ConnectorMapping, ConnectorRegistry, ConnectorSource
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/connectivity", tags=["Foundry Connectivity v2"])

_CONNECTION_RID_PREFIXES = (
    "ri.spice.main.connection.",
    "ri.foundry.main.connection.",
)
_TABLE_IMPORT_RID_PREFIXES = (
    "ri.spice.main.table-import.",
    "ri.foundry.main.table-import.",
)
_FILE_IMPORT_RID_PREFIXES = (
    "ri.spice.main.file-import.",
    "ri.foundry.main.file-import.",
)
_VIRTUAL_TABLE_RID_PREFIXES = (
    "ri.spice.main.virtual-table.",
    "ri.foundry.main.virtual-table.",
)
_DATASET_RID_PREFIXES = (
    "ri.spice.main.dataset.",
    "ri.foundry.main.dataset.",
)

_TABLE_IMPORT_CONFIG_TYPES = {
    "jdbcImportConfig",
    "postgreSqlImportConfig",
    "snowflakeImportConfig",
    "mySqlImportConfig",
    "sqlServerImportConfig",
}
_FILE_IMPORT_CONFIG_TYPES = {
    "jdbcFileImportConfig",
    "postgreSqlFileImportConfig",
    "snowflakeFileImportConfig",
    "mySqlFileImportConfig",
    "sqlServerFileImportConfig",
}
_VIRTUAL_TABLE_CONFIG_TYPES = {
    "jdbcVirtualTableConfig",
    "postgreSqlVirtualTableConfig",
    "snowflakeVirtualTableConfig",
    "mySqlVirtualTableConfig",
    "sqlServerVirtualTableConfig",
}
_TABLE_IMPORT_MODES = {"SNAPSHOT", "APPEND", "UPDATE", "INCREMENTAL", "CDC"}
_DEFAULT_PARENT_FOLDER_RID = "ri.compass.main.folder.root"

_KIND_TO_CONNECTION_CONFIG_TYPE = {
    "google_sheets": "GoogleSheetsConnectionConfig",
    "snowflake": "SnowflakeConnectionConfig",
    "postgresql": "PostgreSqlConnectionConfig",
    "mysql": "MySqlConnectionConfig",
    "sqlserver": "SqlServerConnectionConfig",
}
_KIND_TO_DEFAULT_IMPORT_CONFIG_TYPE = {
    "google_sheets": "jdbcImportConfig",
    "snowflake": "snowflakeImportConfig",
    "postgresql": "postgreSqlImportConfig",
    "mysql": "jdbcImportConfig",
    "sqlserver": "jdbcImportConfig",
}
_KIND_TO_DEFAULT_FILE_IMPORT_CONFIG_TYPE = {
    "google_sheets": "jdbcFileImportConfig",
    "snowflake": "snowflakeFileImportConfig",
    "postgresql": "postgreSqlFileImportConfig",
    "mysql": "mySqlFileImportConfig",
    "sqlserver": "sqlServerFileImportConfig",
}
_KIND_TO_DEFAULT_VIRTUAL_TABLE_CONFIG_TYPE = {
    "google_sheets": "jdbcVirtualTableConfig",
    "snowflake": "snowflakeVirtualTableConfig",
    "postgresql": "postgreSqlVirtualTableConfig",
    "mysql": "mySqlVirtualTableConfig",
    "sqlserver": "sqlServerVirtualTableConfig",
}

_SECRET_KEYS_BY_KIND = {
    "google_sheets": {"accessToken", "refreshToken", "apiKey", "serviceAccountJson"},
    "snowflake": {"password", "privateKey", "token"},
    "postgresql": {"password", "dsn"},
    "mysql": {"password", "dsn"},
    "sqlserver": {"password", "connectionString"},
}


def _foundry_error(
    status_code: int,
    *,
    error_code: str,
    error_name: str,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "errorCode": error_code,
            "errorName": error_name,
            "errorInstanceId": str(uuid4()),
            "parameters": parameters or {},
        },
    )


def _connection_id_from_rid(connection_rid: str) -> str | None:
    text = str(connection_rid or "").strip()
    if not text:
        return None
    for prefix in _CONNECTION_RID_PREFIXES:
        if text.startswith(prefix):
            return text[len(prefix) :].strip() or None
    if text.startswith("ri."):
        return None
    return text


def _connection_rid(connection_id: str) -> str:
    return f"ri.spice.main.connection.{connection_id}"


def _table_import_id_from_rid(table_import_rid: str) -> str | None:
    text = str(table_import_rid or "").strip()
    if not text:
        return None
    for prefix in _TABLE_IMPORT_RID_PREFIXES:
        if text.startswith(prefix):
            return text[len(prefix) :].strip() or None
    if text.startswith("ri."):
        return None
    return text


def _table_import_rid(table_import_id: str) -> str:
    return f"ri.spice.main.table-import.{table_import_id}"


def _file_import_id_from_rid(file_import_rid: str) -> str | None:
    text = str(file_import_rid or "").strip()
    if not text:
        return None
    for prefix in _FILE_IMPORT_RID_PREFIXES:
        if text.startswith(prefix):
            return text[len(prefix) :].strip() or None
    if text.startswith("ri."):
        return None
    return text


def _file_import_rid(file_import_id: str) -> str:
    return f"ri.spice.main.file-import.{file_import_id}"


def _virtual_table_id_from_rid(virtual_table_rid: str) -> str | None:
    text = str(virtual_table_rid or "").strip()
    if not text:
        return None
    for prefix in _VIRTUAL_TABLE_RID_PREFIXES:
        if text.startswith(prefix):
            return text[len(prefix) :].strip() or None
    if text.startswith("ri."):
        return None
    return text


def _virtual_table_rid(virtual_table_id: str) -> str:
    return f"ri.spice.main.virtual-table.{virtual_table_id}"


def _dataset_id_from_rid(dataset_rid: str) -> str | None:
    text = str(dataset_rid or "").strip()
    if not text:
        return None
    for prefix in _DATASET_RID_PREFIXES:
        if text.startswith(prefix):
            return text[len(prefix) :].strip() or None
    if text.startswith("ri."):
        return None
    return text


def _dataset_rid(dataset_id: str) -> str:
    return f"ri.spice.main.dataset.{dataset_id}"


def _table_import_pipeline_id(*, connection_id: str, source_type: str, source_id: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"spice:table-import:{connection_id}:{source_type}:{source_id}"))


def _table_import_build_job_id(*, pipeline_id: str) -> str:
    return f"build-{pipeline_id}-{uuid4()}"


def _file_import_pipeline_id(*, connection_id: str, source_type: str, source_id: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"spice:file-import:{connection_id}:{source_type}:{source_id}"))


def _virtual_table_pipeline_id(*, connection_id: str, source_type: str, source_id: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"spice:virtual-table:{connection_id}:{source_type}:{source_id}"))


def _parse_allowlist(raw: str) -> set[str]:
    return {item.strip().lower() for item in str(raw or "").split(",") if item.strip()}


def _is_flag_or_allowlist_enabled(*, global_enabled: bool, allowlist_raw: str, db_name: str | None) -> bool:
    if global_enabled:
        return True
    db = str(db_name or "").strip().lower()
    if not db:
        return False
    return db in _parse_allowlist(allowlist_raw)


def _jdbc_enabled_for_db(db_name: str | None) -> bool:
    flags = get_settings().feature_flags
    return _is_flag_or_allowlist_enabled(
        global_enabled=bool(getattr(flags, "enable_foundry_connectivity_jdbc", False)),
        allowlist_raw=str(getattr(flags, "foundry_connectivity_jdbc_db_allowlist", "") or ""),
        db_name=db_name,
    )


def _cdc_enabled_for_db(db_name: str | None) -> bool:
    flags = get_settings().feature_flags
    return _is_flag_or_allowlist_enabled(
        global_enabled=bool(getattr(flags, "enable_foundry_connectivity_cdc", False)),
        allowlist_raw=str(getattr(flags, "foundry_connectivity_cdc_db_allowlist", "") or ""),
        db_name=db_name,
    )


def _connection_source_types() -> tuple[str, ...]:
    return tuple(connection_source_type_for_kind(kind) for kind in SUPPORTED_CONNECTOR_KINDS)


def _table_import_source_types() -> tuple[str, ...]:
    return tuple(table_import_source_type_for_kind(kind) for kind in SUPPORTED_CONNECTOR_KINDS)


def _file_import_source_types() -> tuple[str, ...]:
    return tuple(file_import_source_type_for_kind(kind) for kind in SUPPORTED_CONNECTOR_KINDS)


def _virtual_table_source_types() -> tuple[str, ...]:
    return tuple(virtual_table_source_type_for_kind(kind) for kind in SUPPORTED_CONNECTOR_KINDS)


def _connection_kind_from_source(source: ConnectorSource) -> str:
    inferred = connector_kind_from_source_type(source.source_type)
    if inferred in SUPPORTED_CONNECTOR_KINDS:
        return inferred
    return resolve_connector_kind_from_connection_config(source.config_json or {})


def _is_jdbc_connector_kind(kind: str) -> bool:
    return str(kind or "").strip().lower() != "google_sheets"


def _require_preview_or_400(preview: bool) -> JSONResponse | None:
    if preview:
        return None
    return _foundry_error(
        status.HTTP_400_BAD_REQUEST,
        error_code="INVALID_ARGUMENT",
        error_name="ApiFeaturePreviewUsageOnly",
        parameters={"message": "preview=true is required for this endpoint"},
    )


def _default_export_settings() -> Dict[str, Any]:
    return {
        "markingIds": [],
        "sharing": {"scope": "DEFAULT"},
    }


def _normalize_export_settings(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return _default_export_settings()
    marking_ids_raw = value.get("markingIds")
    marking_ids = []
    if isinstance(marking_ids_raw, list):
        marking_ids = [str(item).strip() for item in marking_ids_raw if str(item).strip()]
    sharing_raw = value.get("sharing") if isinstance(value.get("sharing"), dict) else {}
    sharing = dict(sharing_raw)
    if not str(sharing.get("scope") or "").strip():
        sharing["scope"] = "DEFAULT"
    return {
        "markingIds": marking_ids,
        "sharing": sharing,
    }


def _normalize_jdbc_driver_file_name(file_name: str) -> str | None:
    text = str(file_name or "").strip()
    if not text:
        return None
    name = text.rsplit("/", 1)[-1]
    name = name.rsplit("\\", 1)[-1]
    if not name:
        return None
    if "." not in name:
        return None
    return name


def _upsert_custom_jdbc_driver_metadata(
    *,
    config_json: Dict[str, Any],
    file_name: str,
    sha256_hex: str,
    size_bytes: int,
    repository: str,
    object_key: str,
) -> Dict[str, Any]:
    cfg = dict(config_json or {})
    existing_raw = cfg.get("custom_jdbc_drivers")
    existing_items = existing_raw if isinstance(existing_raw, list) else []
    retained: list[Dict[str, Any]] = []
    for item in existing_items:
        if not isinstance(item, dict):
            continue
        if str(item.get("fileName") or "").strip() == file_name:
            continue
        retained.append(dict(item))
    retained.append(
        {
            "fileName": file_name,
            "sha256": sha256_hex,
            "sizeBytes": int(size_bytes),
            "uploadedTime": _iso_timestamp(utcnow()),
            "repository": repository,
            "objectKey": object_key,
        }
    )
    cfg["custom_jdbc_drivers"] = retained
    return cfg


def _public_custom_jdbc_driver_metadata(config: Dict[str, Any]) -> list[Dict[str, Any]]:
    raw = config.get("custom_jdbc_drivers")
    items = raw if isinstance(raw, list) else []
    out: list[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        file_name = str(item.get("fileName") or "").strip()
        if not file_name:
            continue
        payload: Dict[str, Any] = {"fileName": file_name}
        sha256_hex = str(item.get("sha256") or "").strip()
        if sha256_hex:
            payload["sha256"] = sha256_hex
        if item.get("sizeBytes") is not None:
            try:
                payload["sizeBytes"] = int(item.get("sizeBytes"))
            except (TypeError, ValueError):
                pass
        uploaded_time = item.get("uploadedTime")
        if uploaded_time not in (None, ""):
            payload["uploadedTime"] = str(uploaded_time)
        out.append(payload)
    return out


def _extract_connection_configuration(payload: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(payload.get("connectionConfiguration"), dict):
        return dict(payload["connectionConfiguration"])
    if isinstance(payload.get("configuration"), dict):
        return dict(payload["configuration"])
    return {}


def _display_name_from_payload(payload: Dict[str, Any], *, fallback: str) -> str:
    return str(payload.get("name") or payload.get("displayName") or fallback).strip()


def _validate_parent_rid_matches_connection(
    *,
    payload: Dict[str, Any],
    connection_id: str,
) -> JSONResponse | None:
    parent_rid = str(payload.get("parentRid") or "").strip()
    if not parent_rid:
        return None
    parent_connection_id = _connection_id_from_rid(parent_rid)
    if parent_connection_id == connection_id:
        return None
    return _foundry_error(
        status.HTTP_400_BAD_REQUEST,
        error_code="INVALID_ARGUMENT",
        error_name="InvalidArgument",
        parameters={"message": "parentRid must match the path connectionRid"},
    )


def _resolve_table_import_config(
    payload: Dict[str, Any],
    *,
    connector_kind: str,
    sheet_url: str | None = None,
    worksheet_name: str | None = None,
) -> Dict[str, Any]:
    config = payload.get("config") if isinstance(payload.get("config"), dict) else {}
    config_type = str(config.get("type") or "").strip()

    if config_type and config_type in _TABLE_IMPORT_CONFIG_TYPES:
        resolved = dict(config)
        if connector_kind == "google_sheets" and not resolved.get("query") and sheet_url:
            suffix = f"#{worksheet_name}" if worksheet_name else ""
            resolved["query"] = f"SELECT * FROM EXTERNAL_SHEET('{sheet_url}{suffix}')"
        return resolved

    resolved = dict(config)
    resolved["type"] = _KIND_TO_DEFAULT_IMPORT_CONFIG_TYPE.get(connector_kind, "jdbcImportConfig")

    if connector_kind == "google_sheets" and not resolved.get("query") and sheet_url:
        suffix = f"#{worksheet_name}" if worksheet_name else ""
        resolved["query"] = f"SELECT * FROM EXTERNAL_SHEET('{sheet_url}{suffix}')"

    return resolved


def _resolve_file_import_config(
    payload: Dict[str, Any],
    *,
    connector_kind: str,
) -> Dict[str, Any]:
    config = payload.get("config") if isinstance(payload.get("config"), dict) else {}
    config_type = str(config.get("type") or "").strip()
    if config_type and config_type in _FILE_IMPORT_CONFIG_TYPES:
        return dict(config)

    source = payload.get("source") if isinstance(payload.get("source"), dict) else {}
    resolved = dict(config)
    resolved["type"] = _KIND_TO_DEFAULT_FILE_IMPORT_CONFIG_TYPE.get(connector_kind, "jdbcFileImportConfig")
    if "fileImportFilters" not in resolved:
        filters = payload.get("fileImportFilters")
        if not isinstance(filters, dict):
            filters = source.get("fileImportFilters") if isinstance(source.get("fileImportFilters"), dict) else None
        if isinstance(filters, dict):
            resolved["fileImportFilters"] = filters
    for key in ("subfolder", "path", "filePattern", "fileFormat"):
        if key in resolved:
            continue
        value = payload.get(key)
        if value in (None, ""):
            value = source.get(key)
        if value not in (None, ""):
            resolved[key] = value
    return resolved


def _resolve_virtual_table_config(
    payload: Dict[str, Any],
    *,
    connector_kind: str,
) -> Dict[str, Any]:
    config = payload.get("config") if isinstance(payload.get("config"), dict) else {}
    config_type = str(config.get("type") or "").strip()
    if config_type and config_type in _VIRTUAL_TABLE_CONFIG_TYPES:
        return dict(config)

    source = payload.get("source") if isinstance(payload.get("source"), dict) else {}
    resolved = dict(config)
    resolved["type"] = _KIND_TO_DEFAULT_VIRTUAL_TABLE_CONFIG_TYPE.get(connector_kind, "jdbcVirtualTableConfig")
    for key in ("query", "table", "schema", "database"):
        if key in resolved:
            continue
        value = payload.get(key)
        if value in (None, ""):
            value = source.get(key)
        if value not in (None, ""):
            resolved[key] = value
    return resolved


def _extract_destination(payload: Dict[str, Any]) -> tuple[str | None, str | None, str | None]:
    destination = payload.get("destination") if isinstance(payload.get("destination"), dict) else {}
    db_name = str(
        payload.get("ontology")
        or payload.get("databaseName")
        or destination.get("ontology")
        or destination.get("databaseName")
        or ""
    ).strip() or None
    branch = str(
        payload.get("branchName")
        or destination.get("branchName")
        or destination.get("branch")
        or "main"
    ).strip() or "main"
    object_type = str(
        payload.get("objectType")
        or payload.get("classLabel")
        or destination.get("objectType")
        or destination.get("classLabel")
        or ""
    ).strip() or None
    return db_name, branch, object_type


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


async def _load_table_import_source(
    *,
    connector_registry: ConnectorRegistry,
    connection_id: str,
    table_import_id: str,
) -> tuple[ConnectorSource | None, ConnectorMapping | None]:
    for source_type in _table_import_source_types():
        source = await connector_registry.get_source(source_type=source_type, source_id=table_import_id)
        if source is None:
            continue
        cfg = source.config_json or {}
        owner_connection = str(cfg.get("connection_id") or "").strip()
        if owner_connection and owner_connection != connection_id:
            continue
        mapping = await connector_registry.get_mapping(source_type=source.source_type, source_id=source.source_id)
        return source, mapping
    return None, None


async def _load_file_import_source(
    *,
    connector_registry: ConnectorRegistry,
    connection_id: str,
    file_import_id: str,
) -> tuple[ConnectorSource | None, ConnectorMapping | None]:
    for source_type in _file_import_source_types():
        source = await connector_registry.get_source(source_type=source_type, source_id=file_import_id)
        if source is None:
            continue
        cfg = source.config_json or {}
        owner_connection = str(cfg.get("connection_id") or "").strip()
        if owner_connection and owner_connection != connection_id:
            continue
        mapping = await connector_registry.get_mapping(source_type=source.source_type, source_id=source.source_id)
        return source, mapping
    return None, None


async def _load_virtual_table_source(
    *,
    connector_registry: ConnectorRegistry,
    connection_id: str,
    virtual_table_id: str,
) -> tuple[ConnectorSource | None, ConnectorMapping | None]:
    for source_type in _virtual_table_source_types():
        source = await connector_registry.get_source(source_type=source_type, source_id=virtual_table_id)
        if source is None:
            continue
        cfg = source.config_json or {}
        owner_connection = str(cfg.get("connection_id") or "").strip()
        if owner_connection and owner_connection != connection_id:
            continue
        mapping = await connector_registry.get_mapping(source_type=source.source_type, source_id=source.source_id)
        return source, mapping
    return None, None


async def _resolve_output_dataset_rid(
    *,
    source: ConnectorSource,
    mapping: ConnectorMapping | None,
    dataset_registry: DatasetRegistry,
) -> str:
    cfg = source.config_json or {}
    configured_rid = str(cfg.get("dataset_rid") or "").strip()
    if configured_rid:
        return configured_rid

    dataset_id = _dataset_id_from_rid(str(cfg.get("dataset_id") or "").strip())
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


async def _build_table_import_response(
    *,
    connection_id: str,
    source: ConnectorSource,
    mapping: ConnectorMapping | None,
    dataset_registry: DatasetRegistry,
) -> Dict[str, Any]:
    cfg = source.config_json or {}
    connector_kind = connector_kind_from_source_type(source.source_type)
    dataset_rid = await _resolve_output_dataset_rid(source=source, mapping=mapping, dataset_registry=dataset_registry)

    import_mode = str(cfg.get("import_mode") or "SNAPSHOT").strip().upper()
    if import_mode not in _TABLE_IMPORT_MODES:
        import_mode = "SNAPSHOT"

    table_import_config = cfg.get("table_import_config") if isinstance(cfg.get("table_import_config"), dict) else {}
    if not table_import_config:
        table_import_config = _resolve_table_import_config({}, connector_kind=connector_kind)

    name = (
        str(cfg.get("name") or "").strip()
        or str(cfg.get("display_name") or "").strip()
        or str(cfg.get("sheet_title") or "").strip()
        or f"{connector_kind} import {source.source_id}"
    )
    parent_rid = str(cfg.get("parent_rid") or "").strip() or _connection_rid(connection_id)

    return {
        "rid": _table_import_rid(source.source_id),
        "name": name,
        "parentRid": parent_rid,
        # Compatibility aliases for existing clients.
        "displayName": name,
        "connectionRid": _connection_rid(connection_id),
        "datasetRid": dataset_rid,
        "branchName": str(mapping.target_branch or "").strip() if mapping else (str(cfg.get("branch_name") or "").strip() or None),
        "importMode": import_mode,
        "allowSchemaChanges": bool(cfg.get("allow_schema_changes") or False),
        "config": table_import_config,
    }


async def _build_file_import_response(
    *,
    connection_id: str,
    source: ConnectorSource,
    mapping: ConnectorMapping | None,
    dataset_registry: DatasetRegistry,
) -> Dict[str, Any]:
    cfg = source.config_json or {}
    connector_kind = connector_kind_from_source_type(source.source_type)
    dataset_rid = await _resolve_output_dataset_rid(source=source, mapping=mapping, dataset_registry=dataset_registry)

    import_mode = str(cfg.get("import_mode") or "SNAPSHOT").strip().upper()
    if import_mode not in _TABLE_IMPORT_MODES:
        import_mode = "SNAPSHOT"

    file_import_config = cfg.get("table_import_config") if isinstance(cfg.get("table_import_config"), dict) else {}
    if not file_import_config:
        file_import_config = _resolve_file_import_config({}, connector_kind=connector_kind)

    name = (
        str(cfg.get("name") or "").strip()
        or str(cfg.get("display_name") or "").strip()
        or str(cfg.get("file_pattern") or "").strip()
        or f"{connector_kind} file import {source.source_id}"
    )
    parent_rid = str(cfg.get("parent_rid") or "").strip() or _connection_rid(connection_id)

    return {
        "rid": _file_import_rid(source.source_id),
        "name": name,
        "parentRid": parent_rid,
        # Compatibility aliases for existing clients.
        "displayName": name,
        "connectionRid": _connection_rid(connection_id),
        "datasetRid": dataset_rid,
        "branchName": str(mapping.target_branch or "").strip() if mapping else (str(cfg.get("branch_name") or "").strip() or None),
        "importMode": import_mode,
        "allowSchemaChanges": bool(cfg.get("allow_schema_changes") or False),
        "config": file_import_config,
    }


async def _build_virtual_table_response(
    *,
    connection_id: str,
    source: ConnectorSource,
    mapping: ConnectorMapping | None,
    dataset_registry: DatasetRegistry,
) -> Dict[str, Any]:
    cfg = source.config_json or {}
    connector_kind = connector_kind_from_source_type(source.source_type)
    dataset_rid = await _resolve_output_dataset_rid(source=source, mapping=mapping, dataset_registry=dataset_registry)

    virtual_table_config = cfg.get("virtual_table_config") if isinstance(cfg.get("virtual_table_config"), dict) else {}
    if not virtual_table_config:
        virtual_table_config = _resolve_virtual_table_config({}, connector_kind=connector_kind)

    name = (
        str(cfg.get("name") or "").strip()
        or str(cfg.get("display_name") or "").strip()
        or f"{connector_kind} virtual table {source.source_id}"
    )
    parent_rid = str(cfg.get("parent_rid") or "").strip() or _connection_rid(connection_id)
    markings = cfg.get("markings") if isinstance(cfg.get("markings"), list) else []
    return {
        "rid": _virtual_table_rid(source.source_id),
        "name": name,
        "parentRid": parent_rid,
        "markings": markings,
        # Compatibility aliases for existing clients.
        "displayName": name,
        "connectionRid": _connection_rid(connection_id),
        "datasetRid": dataset_rid,
        "branchName": str(mapping.target_branch or "").strip() if mapping else (str(cfg.get("branch_name") or "").strip() or None),
        "config": virtual_table_config,
    }


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

    return {
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


def _iso_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _normalize_connection_config_for_storage(configuration: Dict[str, Any], *, kind: str) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "type": _KIND_TO_CONNECTION_CONFIG_TYPE.get(kind, "GoogleSheetsConnectionConfig"),
    }

    # Store non-secret fields only.
    for key, value in (configuration or {}).items():
        if key in _SECRET_KEYS_BY_KIND.get(kind, set()):
            continue
        if value in (None, ""):
            continue
        snake_key = "".join([f"_{c.lower()}" if c.isupper() else c for c in str(key)]).lstrip("_")
        cfg[snake_key] = value

    if kind == "google_sheets":
        if configuration.get("accountEmail"):
            cfg["account_email"] = configuration.get("accountEmail")
        if configuration.get("sheetUrl"):
            cfg["sheet_url"] = configuration.get("sheetUrl")
    return cfg


def _extract_secrets_from_configuration(configuration: Dict[str, Any], *, kind: str) -> Dict[str, Any]:
    secrets: Dict[str, Any] = {}
    allowed = _SECRET_KEYS_BY_KIND.get(kind, set())
    for key in allowed:
        value = configuration.get(key)
        if value in (None, ""):
            continue
        snake_key = "".join([f"_{c.lower()}" if c.isupper() else c for c in str(key)]).lstrip("_")
        secrets[snake_key] = value
    return secrets


def _extract_secrets_from_payload(payload: Dict[str, Any], *, kind: str) -> Dict[str, Any]:
    base = payload.get("secrets") if isinstance(payload.get("secrets"), dict) else payload
    secrets: Dict[str, Any] = {}
    allowed = _SECRET_KEYS_BY_KIND.get(kind, set())
    for key in allowed:
        if key in base and base.get(key) not in (None, ""):
            snake_key = "".join([f"_{c.lower()}" if c.isupper() else c for c in str(key)]).lstrip("_")
            secrets[snake_key] = base.get(key)
    # Accept snake_case too for internal calls.
    for key in {"access_token", "refresh_token", "api_key", "service_account_json", "password", "private_key", "token", "dsn"}:
        if key in base and base.get(key) not in (None, ""):
            secrets[key] = base.get(key)
    for key in {"connection_string"}:
        if key in base and base.get(key) not in (None, ""):
            secrets[key] = base.get(key)
    return secrets


def _build_execute_run_output(
    *,
    connection_rid: str,
    resource_rid: str,
    branch_name: str,
    requested_by: str,
    resource_field: str = "tableImportRid",
    result_payload: Dict[str, Any] | None = None,
    error_detail: str | None = None,
) -> Dict[str, Any]:
    output: Dict[str, Any] = {
        "connectionRid": connection_rid,
        resource_field: resource_rid,
        "branch": branch_name,
        "requested_by": requested_by,
        "outputs": [],
    }
    data = result_payload.get("data") if isinstance(result_payload, dict) else {}
    dataset_raw = data.get("dataset") if isinstance(data, dict) else {}
    dataset = dataset_raw if isinstance(dataset_raw, dict) else {}
    dataset_id = str(dataset.get("dataset_id") or "").strip()
    if dataset_id:
        output["outputs"] = [{"datasetRid": _dataset_rid(dataset_id)}]
    if error_detail:
        output["errors"] = [error_detail]
    return output


async def _ensure_table_import_pipeline(
    *,
    pipeline_registry: PipelineRegistry,
    connection_id: str,
    source: ConnectorSource,
    mapping: ConnectorMapping,
) -> str:
    pipeline_id = _table_import_pipeline_id(
        connection_id=connection_id,
        source_type=source.source_type,
        source_id=source.source_id,
    )
    existing = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    if existing is not None:
        return pipeline_id

    db_name = str(mapping.target_db_name or "").strip() or "main"
    branch = str(mapping.target_branch or "").strip() or "main"
    pipeline_name = f"table_import_{connection_id}_{source.source_type}_{source.source_id}"
    await pipeline_registry.create_pipeline(
        pipeline_id=pipeline_id,
        db_name=db_name,
        name=pipeline_name,
        description=f"Foundry connectivity table import pipeline for {source.source_type}:{source.source_id}",
        pipeline_type="connector_table_import",
        location=f"connectivity/connections/{connection_id}/tableImports/{source.source_id}",
        status="active",
        branch=branch,
    )
    return pipeline_id


async def _ensure_file_import_pipeline(
    *,
    pipeline_registry: PipelineRegistry,
    connection_id: str,
    source: ConnectorSource,
    mapping: ConnectorMapping,
) -> str:
    pipeline_id = _file_import_pipeline_id(
        connection_id=connection_id,
        source_type=source.source_type,
        source_id=source.source_id,
    )
    existing = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    if existing is not None:
        return pipeline_id

    db_name = str(mapping.target_db_name or "").strip() or "main"
    branch = str(mapping.target_branch or "").strip() or "main"
    pipeline_name = f"file_import_{connection_id}_{source.source_type}_{source.source_id}"
    await pipeline_registry.create_pipeline(
        pipeline_id=pipeline_id,
        db_name=db_name,
        name=pipeline_name,
        description=f"Foundry connectivity file import pipeline for {source.source_type}:{source.source_id}",
        pipeline_type="connector_file_import",
        location=f"connectivity/connections/{connection_id}/fileImports/{source.source_id}",
        status="active",
        branch=branch,
    )
    return pipeline_id


async def _ensure_virtual_table_pipeline(
    *,
    pipeline_registry: PipelineRegistry,
    connection_id: str,
    source: ConnectorSource,
    mapping: ConnectorMapping,
) -> str:
    pipeline_id = _virtual_table_pipeline_id(
        connection_id=connection_id,
        source_type=source.source_type,
        source_id=source.source_id,
    )
    existing = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
    if existing is not None:
        return pipeline_id

    db_name = str(mapping.target_db_name or "").strip() or "main"
    branch = str(mapping.target_branch or "").strip() or "main"
    pipeline_name = f"virtual_table_{connection_id}_{source.source_type}_{source.source_id}"
    await pipeline_registry.create_pipeline(
        pipeline_id=pipeline_id,
        db_name=db_name,
        name=pipeline_name,
        description=f"Foundry connectivity virtual table pipeline for {source.source_type}:{source.source_id}",
        pipeline_type="connector_virtual_table",
        location=f"connectivity/connections/{connection_id}/virtualTables/{source.source_id}",
        status="active",
        branch=branch,
    )
    return pipeline_id


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

    if _is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )

    import_mode = str(payload.get("importMode") or "SNAPSHOT").strip().upper()
    if import_mode not in _TABLE_IMPORT_MODES:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "importMode must be one of SNAPSHOT, APPEND, UPDATE, INCREMENTAL, CDC"},
        )
    if import_mode == "CDC" and not _cdc_enabled_for_db(resolved_db):
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
            name = _display_name_from_payload(payload, fallback=f"Google Sheet Import {table_import_id}")
            cfg.update(
                {
                    "connection_id": connection_id,
                    "dataset_rid": resolved_dataset_rid,
                    "display_name": name,
                    "name": name,
                    "parent_rid": _connection_rid(connection_id),
                    "import_mode": import_mode,
                    "allow_schema_changes": bool(payload.get("allowSchemaChanges") or False),
                    "table_import_config": _resolve_table_import_config(
                        payload,
                        connector_kind="google_sheets",
                        sheet_url=str(cfg.get("sheet_url") or sheet_url),
                        worksheet_name=str(cfg.get("worksheet_name") or worksheet_name or "").strip() or None,
                    ),
                    "branch_name": resolved_branch,
                }
            )
            await connector_registry.upsert_source(
                source_type=source.source_type,
                source_id=source.source_id,
                enabled=True,
                config_json=cfg,
            )
            if resolved_db:
                await connector_registry.upsert_mapping(
                    source_type=source.source_type,
                    source_id=source.source_id,
                    enabled=bool(resolved_db and destination_object_type),
                    status="confirmed" if resolved_db and destination_object_type else "draft",
                    target_db_name=resolved_db,
                    target_branch=resolved_branch,
                    target_class_label=destination_object_type,
                    field_mappings=[],
                )
        except ValueError as exc:
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": str(exc)},
            )
    else:
        table_import_id = str(payload.get("tableImportId") or uuid4()).strip()
        source_type = table_import_source_type_for_kind(connector_kind)
        display_name = _display_name_from_payload(payload, fallback=f"{connector_kind} import {table_import_id}")
        allow_schema_changes = bool(payload.get("allowSchemaChanges") or False)
        table_import_config = _resolve_table_import_config(payload, connector_kind=connector_kind)

        if not table_import_config.get("query") and import_mode in {"SNAPSHOT", "APPEND", "UPDATE", "INCREMENTAL"}:
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "config.query is required for JDBC table imports"},
            )

        await connector_registry.upsert_source(
            source_type=source_type,
            source_id=table_import_id,
            enabled=True,
            config_json={
                "connection_id": connection_id,
                "dataset_rid": dataset_rid_input,
                "display_name": display_name,
                "name": display_name,
                "parent_rid": _connection_rid(connection_id),
                "import_mode": import_mode,
                "allow_schema_changes": allow_schema_changes,
                "table_import_config": table_import_config,
                "branch_name": resolved_branch,
            },
        )
        if resolved_db:
            await connector_registry.upsert_mapping(
                source_type=source_type,
                source_id=table_import_id,
                enabled=bool(resolved_db and destination_object_type),
                status="confirmed" if resolved_db and destination_object_type else "draft",
                target_db_name=resolved_db,
                target_branch=resolved_branch,
                target_class_label=destination_object_type,
                field_mappings=[],
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

    if pageSize is None:
        page_size = 100
    else:
        try:
            page_size = int(pageSize)
        except (TypeError, ValueError):
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageSize must be an integer"},
            )
        if page_size <= 0:
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageSize must be > 0"},
            )

    offset = 0
    if pageToken:
        try:
            offset = int(pageToken)
        except (TypeError, ValueError):
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageToken is invalid"},
            )
        if offset < 0:
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageToken is invalid"},
            )

    owned_sources: list[ConnectorSource] = []
    for source_type in _table_import_source_types():
        sources = await connector_registry.list_sources(source_type=source_type, enabled=True, limit=5000)
        for src in sources:
            if str((src.config_json or {}).get("connection_id") or "").strip() != connection_id:
                continue
            owned_sources.append(src)

    owned_sources.sort(key=lambda src: str(src.source_id))
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
    connector_kind = connector_kind_from_source_type(source.source_type)

    import_mode = str(payload.get("importMode") or cfg.get("import_mode") or "SNAPSHOT").strip().upper()
    if import_mode not in _TABLE_IMPORT_MODES:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "importMode must be one of SNAPSHOT, APPEND, UPDATE, INCREMENTAL, CDC"},
        )

    destination_db, destination_branch, destination_object_type = _extract_destination(payload)
    resolved_db = destination_db or (mapping.target_db_name if mapping else None)

    if _is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )
    if import_mode == "CDC" and not _cdc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "CDC connectivity is disabled for this ontology"},
        )

    cfg.update(
        {
            "display_name": _display_name_from_payload(payload, fallback=cfg.get("display_name") or f"{connector_kind} import {table_import_id}"),
            "name": _display_name_from_payload(payload, fallback=cfg.get("name") or cfg.get("display_name") or f"{connector_kind} import {table_import_id}"),
            "parent_rid": _connection_rid(connection_id),
            "import_mode": import_mode,
            "allow_schema_changes": bool(
                payload.get("allowSchemaChanges")
                if payload.get("allowSchemaChanges") is not None
                else cfg.get("allow_schema_changes")
            ),
            "table_import_config": _resolve_table_import_config(
                payload,
                connector_kind=connector_kind,
                sheet_url=str(cfg.get("sheet_url") or ""),
                worksheet_name=str(cfg.get("worksheet_name") or "").strip() or None,
            ),
            "connection_id": connection_id,
        }
    )
    if destination_branch:
        cfg["branch_name"] = destination_branch

    await connector_registry.upsert_source(
        source_type=source.source_type,
        source_id=source.source_id,
        enabled=source.enabled,
        config_json=cfg,
    )

    if mapping and (destination_db or destination_object_type or destination_branch):
        await connector_registry.upsert_mapping(
            source_type=source.source_type,
            source_id=source.source_id,
            enabled=bool(destination_db or mapping.enabled),
            status="confirmed" if (destination_db or mapping.target_db_name) and (destination_object_type or mapping.target_class_label) else "draft",
            target_db_name=destination_db or mapping.target_db_name,
            target_branch=destination_branch or mapping.target_branch,
            target_class_label=destination_object_type or mapping.target_class_label,
            field_mappings=mapping.field_mappings,
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

    await connector_registry.set_source_enabled(
        source_type=source.source_type,
        source_id=source.source_id,
        enabled=False,
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
    if mapping is None or not str(mapping.target_db_name or "").strip():
        return _foundry_error(
            status.HTTP_409_CONFLICT,
            error_code="CONFLICT",
            error_name="TableImportNotReady",
            parameters={"message": "table import mapping is incomplete; target ontology is required"},
        )

    connector_kind = connector_kind_from_source_type(source.source_type)
    target_db = str(mapping.target_db_name or "").strip() or None
    import_mode = str((source.config_json or {}).get("import_mode") or "SNAPSHOT").strip().upper()
    if _is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(target_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )
    if import_mode == "CDC" and not _cdc_enabled_for_db(target_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "CDC connectivity is disabled for this ontology"},
        )

    branch_name = str(mapping.target_branch or "").strip() or "main"
    requested_by = str(request.headers.get("X-User-ID") or "").strip() or "system"
    connection_rid = _connection_rid(connection_id)
    table_import_rid = _table_import_rid(source.source_id)

    try:
        pipeline_id = await _ensure_table_import_pipeline(
            pipeline_registry=pipeline_registry,
            connection_id=connection_id,
            source=source,
            mapping=mapping,
        )
        job_id = _table_import_build_job_id(pipeline_id=pipeline_id)
        started_at = utcnow()

        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=job_id,
            mode="build",
            status="RUNNING",
            output_json=_build_execute_run_output(
                connection_rid=connection_rid,
                resource_rid=table_import_rid,
                branch_name=branch_name,
                requested_by=requested_by,
            ),
            started_at=started_at,
        )

        result_payload = await data_connector_pipelining_service.start_pipelining_table_import(
            source=source,
            mapping=mapping,
            google_sheets_service=google_sheets_service,
            connector_adapter_factory=connector_adapter_factory,
            connector_registry=connector_registry,
            pipeline_registry=pipeline_registry,
            dataset_registry=dataset_registry,
            objectify_registry=objectify_registry,
            objectify_job_queue=objectify_job_queue,
            actor_user_id=str(request.headers.get("X-User-ID") or "").strip() or None,
        )

        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=job_id,
            mode="build",
            status="SUCCESS",
            output_json=_build_execute_run_output(
                connection_rid=connection_rid,
                resource_rid=table_import_rid,
                branch_name=branch_name,
                requested_by=requested_by,
                result_payload=result_payload if isinstance(result_payload, dict) else None,
            ),
            started_at=started_at,
            finished_at=utcnow(),
        )
    except Exception as exc:
        logger.error("Failed to execute table import %s: %s", tableImportRid, exc)
        try:
            if "job_id" in locals() and "pipeline_id" in locals():
                await pipeline_registry.record_run(
                    pipeline_id=str(locals().get("pipeline_id")),
                    job_id=str(locals().get("job_id")),
                    mode="build",
                    status="FAILED",
                    output_json=_build_execute_run_output(
                        connection_rid=connection_rid,
                        resource_rid=table_import_rid,
                        branch_name=branch_name,
                        requested_by=requested_by,
                        error_detail=str(exc),
                    ),
                    started_at=locals().get("started_at") or utcnow(),
                    finished_at=utcnow(),
                )
        except Exception as record_exc:
            logger.warning("Failed to record execute_table_import failure build run: %s", record_exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"message": "Failed to execute table import"},
        )

    return f"ri.spice.main.build.{job_id}"


@router.post("/connections/{connectionRid}/fileImports")
@trace_endpoint("bff.foundry_v2_connectivity.create_file_import")
async def create_file_import_v2(
    connectionRid: str,
    payload: Dict[str, Any],
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
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

    if _is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )

    import_mode = str(payload.get("importMode") or "SNAPSHOT").strip().upper()
    if import_mode not in _TABLE_IMPORT_MODES:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "importMode must be one of SNAPSHOT, APPEND, UPDATE, INCREMENTAL, CDC"},
        )
    if import_mode == "CDC" and not _cdc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "CDC connectivity is disabled for this ontology"},
        )

    file_import_id = str(payload.get("fileImportId") or uuid4()).strip()
    source_type = file_import_source_type_for_kind(connector_kind)
    display_name = _display_name_from_payload(payload, fallback=f"{connector_kind} file import {file_import_id}")
    allow_schema_changes = bool(payload.get("allowSchemaChanges") or False)
    file_import_config = _resolve_file_import_config(payload, connector_kind=connector_kind)

    if _is_jdbc_connector_kind(connector_kind) and not str(file_import_config.get("query") or "").strip():
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "config.query is required for JDBC file imports"},
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
        source_id=file_import_id,
        enabled=True,
        config_json={
            "connection_id": connection_id,
            "dataset_rid": dataset_rid_input,
            "display_name": display_name,
            "name": display_name,
            "parent_rid": _connection_rid(connection_id),
            "import_mode": import_mode,
            "allow_schema_changes": allow_schema_changes,
            # Keep execution path unified with table import runtime.
            "table_import_config": file_import_config,
            "branch_name": resolved_branch,
            "resource_kind": "file_import",
        },
    )
    if resolved_db:
        await connector_registry.upsert_mapping(
            source_type=source_type,
            source_id=file_import_id,
            enabled=bool(resolved_db and destination_object_type),
            status="confirmed" if resolved_db and destination_object_type else "draft",
            target_db_name=resolved_db,
            target_branch=resolved_branch,
            target_class_label=destination_object_type,
            field_mappings=[],
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

    if pageSize is None:
        page_size = 100
    else:
        try:
            page_size = int(pageSize)
        except (TypeError, ValueError):
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageSize must be an integer"},
            )
        if page_size <= 0:
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageSize must be > 0"},
            )

    offset = 0
    if pageToken:
        try:
            offset = int(pageToken)
        except (TypeError, ValueError):
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageToken is invalid"},
            )
        if offset < 0:
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageToken is invalid"},
            )

    owned_sources: list[ConnectorSource] = []
    for source_type in _file_import_source_types():
        sources = await connector_registry.list_sources(source_type=source_type, enabled=True, limit=5000)
        for src in sources:
            if str((src.config_json or {}).get("connection_id") or "").strip() != connection_id:
                continue
            owned_sources.append(src)

    owned_sources.sort(key=lambda src: str(src.source_id))
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
    connector_kind = connector_kind_from_source_type(source.source_type)
    import_mode = str(payload.get("importMode") or cfg.get("import_mode") or "SNAPSHOT").strip().upper()
    if import_mode not in _TABLE_IMPORT_MODES:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": "importMode must be one of SNAPSHOT, APPEND, UPDATE, INCREMENTAL, CDC"},
        )

    destination_db, destination_branch, destination_object_type = _extract_destination(payload)
    resolved_db = destination_db or (mapping.target_db_name if mapping else None)
    if _is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )
    if import_mode == "CDC" and not _cdc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "CDC connectivity is disabled for this ontology"},
        )

    file_import_config = _resolve_file_import_config(payload, connector_kind=connector_kind)
    cfg.update(
        {
            "display_name": _display_name_from_payload(payload, fallback=cfg.get("display_name") or f"{connector_kind} file import {file_import_id}"),
            "name": _display_name_from_payload(payload, fallback=cfg.get("name") or cfg.get("display_name") or f"{connector_kind} file import {file_import_id}"),
            "parent_rid": _connection_rid(connection_id),
            "import_mode": import_mode,
            "allow_schema_changes": bool(
                payload.get("allowSchemaChanges")
                if payload.get("allowSchemaChanges") is not None
                else cfg.get("allow_schema_changes")
            ),
            "table_import_config": file_import_config,
            "connection_id": connection_id,
        }
    )
    if destination_branch:
        cfg["branch_name"] = destination_branch
    await connector_registry.upsert_source(
        source_type=source.source_type,
        source_id=source.source_id,
        enabled=source.enabled,
        config_json=cfg,
    )

    if mapping and (destination_db or destination_object_type or destination_branch):
        await connector_registry.upsert_mapping(
            source_type=source.source_type,
            source_id=source.source_id,
            enabled=bool(destination_db or mapping.enabled),
            status="confirmed" if (destination_db or mapping.target_db_name) and (destination_object_type or mapping.target_class_label) else "draft",
            target_db_name=destination_db or mapping.target_db_name,
            target_branch=destination_branch or mapping.target_branch,
            target_class_label=destination_object_type or mapping.target_class_label,
            field_mappings=mapping.field_mappings,
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
    await connector_registry.set_source_enabled(source_type=source.source_type, source_id=source.source_id, enabled=False)
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
    if mapping is None or not str(mapping.target_db_name or "").strip():
        return _foundry_error(
            status.HTTP_409_CONFLICT,
            error_code="CONFLICT",
            error_name="FileImportNotReady",
            parameters={"message": "file import mapping is incomplete; target ontology is required"},
        )

    connector_kind = connector_kind_from_source_type(source.source_type)
    target_db = str(mapping.target_db_name or "").strip() or None
    import_mode = str((source.config_json or {}).get("import_mode") or "SNAPSHOT").strip().upper()
    if _is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(target_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )
    if import_mode == "CDC" and not _cdc_enabled_for_db(target_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "CDC connectivity is disabled for this ontology"},
        )

    branch_name = str(mapping.target_branch or "").strip() or "main"
    requested_by = str(request.headers.get("X-User-ID") or "").strip() or "system"
    connection_rid = _connection_rid(connection_id)
    file_import_rid = _file_import_rid(source.source_id)

    try:
        pipeline_id = await _ensure_file_import_pipeline(
            pipeline_registry=pipeline_registry,
            connection_id=connection_id,
            source=source,
            mapping=mapping,
        )
        job_id = _table_import_build_job_id(pipeline_id=pipeline_id)
        started_at = utcnow()
        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=job_id,
            mode="build",
            status="RUNNING",
            output_json=_build_execute_run_output(
                connection_rid=connection_rid,
                resource_rid=file_import_rid,
                resource_field="fileImportRid",
                branch_name=branch_name,
                requested_by=requested_by,
            ),
            started_at=started_at,
        )

        result_payload = await data_connector_pipelining_service.start_pipelining_table_import(
            source=source,
            mapping=mapping,
            google_sheets_service=google_sheets_service,
            connector_adapter_factory=connector_adapter_factory,
            connector_registry=connector_registry,
            pipeline_registry=pipeline_registry,
            dataset_registry=dataset_registry,
            objectify_registry=objectify_registry,
            objectify_job_queue=objectify_job_queue,
            actor_user_id=str(request.headers.get("X-User-ID") or "").strip() or None,
        )
        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=job_id,
            mode="build",
            status="SUCCESS",
            output_json=_build_execute_run_output(
                connection_rid=connection_rid,
                resource_rid=file_import_rid,
                resource_field="fileImportRid",
                branch_name=branch_name,
                requested_by=requested_by,
                result_payload=result_payload if isinstance(result_payload, dict) else None,
            ),
            started_at=started_at,
            finished_at=utcnow(),
        )
    except Exception as exc:
        logger.error("Failed to execute file import %s: %s", fileImportRid, exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"message": "Failed to execute file import"},
        )

    return f"ri.spice.main.build.{job_id}"


@router.post("/connections/{connectionRid}/virtualTables")
@trace_endpoint("bff.foundry_v2_connectivity.create_virtual_table")
async def create_virtual_table_v2(
    connectionRid: str,
    payload: Dict[str, Any],
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
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
    if _is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )

    virtual_table_id = str(payload.get("virtualTableId") or uuid4()).strip()
    source_type = virtual_table_source_type_for_kind(connector_kind)
    virtual_table_config = _resolve_virtual_table_config(payload, connector_kind=connector_kind)
    display_name = _display_name_from_payload(payload, fallback=f"{connector_kind} virtual table {virtual_table_id}")

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
        config_json={
            "connection_id": connection_id,
            "dataset_rid": dataset_rid_input,
            "display_name": display_name,
            "name": display_name,
            "parent_rid": _connection_rid(connection_id),
            "virtual_table_config": virtual_table_config,
            "branch_name": resolved_branch,
            "resource_kind": "virtual_table",
        },
    )
    if resolved_db:
        await connector_registry.upsert_mapping(
            source_type=source_type,
            source_id=virtual_table_id,
            enabled=bool(resolved_db and destination_object_type),
            status="confirmed" if resolved_db and destination_object_type else "draft",
            target_db_name=resolved_db,
            target_branch=resolved_branch,
            target_class_label=destination_object_type,
            field_mappings=[],
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

    if pageSize is None:
        page_size = 100
    else:
        try:
            page_size = int(pageSize)
        except (TypeError, ValueError):
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageSize must be an integer"},
            )
        if page_size <= 0:
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageSize must be > 0"},
            )

    offset = 0
    if pageToken:
        try:
            offset = int(pageToken)
        except (TypeError, ValueError):
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageToken is invalid"},
            )
        if offset < 0:
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageToken is invalid"},
            )

    owned_sources: list[ConnectorSource] = []
    for source_type in _virtual_table_source_types():
        sources = await connector_registry.list_sources(source_type=source_type, enabled=True, limit=5000)
        for src in sources:
            if str((src.config_json or {}).get("connection_id") or "").strip() != connection_id:
                continue
            owned_sources.append(src)

    owned_sources.sort(key=lambda src: str(src.source_id))
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
    connector_kind = connector_kind_from_source_type(source.source_type)
    destination_db, destination_branch, destination_object_type = _extract_destination(payload)
    resolved_db = destination_db or (mapping.target_db_name if mapping else None)
    if _is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(resolved_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )

    cfg.update(
        {
            "display_name": _display_name_from_payload(payload, fallback=cfg.get("display_name") or f"{connector_kind} virtual table {virtual_table_id}"),
            "name": _display_name_from_payload(payload, fallback=cfg.get("name") or cfg.get("display_name") or f"{connector_kind} virtual table {virtual_table_id}"),
            "parent_rid": _connection_rid(connection_id),
            "virtual_table_config": _resolve_virtual_table_config(payload, connector_kind=connector_kind),
            "connection_id": connection_id,
        }
    )
    if destination_branch:
        cfg["branch_name"] = destination_branch
    await connector_registry.upsert_source(
        source_type=source.source_type,
        source_id=source.source_id,
        enabled=source.enabled,
        config_json=cfg,
    )

    if mapping and (destination_db or destination_object_type or destination_branch):
        await connector_registry.upsert_mapping(
            source_type=source.source_type,
            source_id=source.source_id,
            enabled=bool(destination_db or mapping.enabled),
            status="confirmed" if (destination_db or mapping.target_db_name) and (destination_object_type or mapping.target_class_label) else "draft",
            target_db_name=destination_db or mapping.target_db_name,
            target_branch=destination_branch or mapping.target_branch,
            target_class_label=destination_object_type or mapping.target_class_label,
            field_mappings=mapping.field_mappings,
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
    await connector_registry.set_source_enabled(source_type=source.source_type, source_id=source.source_id, enabled=False)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/connections")
@trace_endpoint("bff.foundry_v2_connectivity.create_connection")
async def create_connection_v2(
    payload: Dict[str, Any],
    request: Request,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    connector_adapter_factory: ConnectorAdapterFactory = Depends(get_connector_adapter_factory),
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
    if _is_jdbc_connector_kind(kind):
        if not bool(getattr(get_settings().feature_flags, "enable_foundry_connectivity_jdbc", False)):
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
@trace_endpoint("bff.foundry_v2_connectivity.list_connections")
async def list_connections_v2(
    preview: bool = Query(default=False),
    pageSize: int = Query(default=100, ge=1, le=500),
    pageToken: str | None = Query(default=None),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    connector_adapter_factory: ConnectorAdapterFactory = Depends(get_connector_adapter_factory),
):
    preview_error = _require_preview_or_400(preview)
    if preview_error is not None:
        return preview_error
    offset = 0
    if pageToken:
        try:
            offset = int(pageToken)
        except (TypeError, ValueError):
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageToken is invalid"},
            )
        if offset < 0:
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "pageToken is invalid"},
            )

    all_connections: list[ConnectorSource] = []
    for source_type in _connection_source_types():
        all_connections.extend(await connector_registry.list_sources(source_type=source_type, enabled=True, limit=5000))
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

    await connector_registry.set_source_enabled(
        source_type=source.source_type,
        source_id=connection_id,
        enabled=False,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/connections/{connectionRid}/test")
@trace_endpoint("bff.foundry_v2_connectivity.test_connection")
async def test_connection_v2(
    connectionRid: str,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    connector_adapter_factory: ConnectorAdapterFactory = Depends(get_connector_adapter_factory),
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
    except Exception:
        pass

    return test_result


@router.get("/connections/{connectionRid}/getConfiguration")
@trace_endpoint("bff.foundry_v2_connectivity.get_connection_configuration")
async def get_connection_configuration_v2(
    connectionRid: str,
    preview: bool = Query(default=False),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    connector_adapter_factory: ConnectorAdapterFactory = Depends(get_connector_adapter_factory),
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

    cfg = dict(source.config_json or {})
    cfg["export_settings"] = export_settings
    await connector_registry.upsert_source(
        source_type=source.source_type,
        source_id=source.source_id,
        enabled=source.enabled,
        config_json=cfg,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


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
    if not _is_jdbc_connector_kind(kind):
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
