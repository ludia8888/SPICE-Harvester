from __future__ import annotations

import logging
from typing import Any, Dict
from urllib.parse import urlparse

from fastapi import status
from fastapi.responses import JSONResponse

from bff.routers.foundry_dataset_rid_common import _dataset_id_from_rid, _dataset_rid
from data_connector.adapters.factory import (
    SUPPORTED_CONNECTOR_KINDS,
    connection_source_type_for_kind,
    connector_kind_from_source_type,
    file_import_source_type_for_kind,
    resolve_connector_kind_from_connection_config,
    table_import_source_type_for_kind,
    virtual_table_source_type_for_kind,
)
from data_connector.adapters.import_config_validators import is_jdbc_connector_kind, normalize_import_mode
from shared.config.settings import get_settings
from shared.foundry.errors import foundry_error as _foundry_error
from shared.foundry.rids import build_rid, parse_rid
from shared.models.background_task import TaskStatus
from shared.services.registries.connector_registry import ConnectorMapping, ConnectorSource
from shared.utils.bool_utils import coerce_optional_bool
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)

_TABLE_IMPORT_CONFIG_TYPES = {
    "jdbcImportConfig",
    "postgreSqlImportConfig",
    "snowflakeImportConfig",
    "mySqlImportConfig",
    "sqlServerImportConfig",
    "oracleImportConfig",
}
_FILE_IMPORT_CONFIG_TYPES = {
    "jdbcFileImportConfig",
    "postgreSqlFileImportConfig",
    "snowflakeFileImportConfig",
    "mySqlFileImportConfig",
    "sqlServerFileImportConfig",
    "oracleFileImportConfig",
}
_VIRTUAL_TABLE_CONFIG_TYPES = {
    "jdbcVirtualTableConfig",
    "postgreSqlVirtualTableConfig",
    "snowflakeVirtualTableConfig",
    "mySqlVirtualTableConfig",
    "sqlServerVirtualTableConfig",
    "oracleVirtualTableConfig",
}
_DEFAULT_PARENT_FOLDER_RID = "ri.compass.main.folder.root"

_KIND_TO_CONNECTION_CONFIG_TYPE = {
    "google_sheets": "GoogleSheetsConnectionConfig",
    "snowflake": "SnowflakeConnectionConfig",
    "postgresql": "PostgreSqlConnectionConfig",
    "mysql": "MySqlConnectionConfig",
    "sqlserver": "SqlServerConnectionConfig",
    "oracle": "OracleConnectionConfig",
}
_KIND_TO_DEFAULT_IMPORT_CONFIG_TYPE = {
    "google_sheets": "jdbcImportConfig",
    "snowflake": "snowflakeImportConfig",
    "postgresql": "postgreSqlImportConfig",
    "mysql": "jdbcImportConfig",
    "sqlserver": "jdbcImportConfig",
    "oracle": "oracleImportConfig",
}
_KIND_TO_DEFAULT_FILE_IMPORT_CONFIG_TYPE = {
    "google_sheets": "jdbcFileImportConfig",
    "snowflake": "snowflakeFileImportConfig",
    "postgresql": "postgreSqlFileImportConfig",
    "mysql": "mySqlFileImportConfig",
    "sqlserver": "sqlServerFileImportConfig",
    "oracle": "oracleFileImportConfig",
}
_KIND_TO_DEFAULT_VIRTUAL_TABLE_CONFIG_TYPE = {
    "google_sheets": "jdbcVirtualTableConfig",
    "snowflake": "snowflakeVirtualTableConfig",
    "postgresql": "postgreSqlVirtualTableConfig",
    "mysql": "mySqlVirtualTableConfig",
    "sqlserver": "sqlServerVirtualTableConfig",
    "oracle": "oracleVirtualTableConfig",
}

_SECRET_KEYS_BY_KIND = {
    "google_sheets": {"accessToken", "refreshToken", "apiKey", "serviceAccountJson"},
    "snowflake": {"password", "privateKey", "token"},
    "postgresql": {"password", "dsn"},
    "mysql": {"password", "dsn"},
    "sqlserver": {"password", "connectionString"},
    "oracle": {"password", "dsn"},
}

_RESOURCE_CONFIG_ERROR_NAMES = {
    "table_import": "TableImportInvalidConfig",
    "file_import": "FileImportInvalidConfig",
    "virtual_table": "VirtualTableInvalidConfig",
}


def _coerce_bool(value: Any, *, default: bool) -> bool:
    return coerce_optional_bool(
        value,
        default=default,
        allow_numeric=True,
        allow_short_tokens=True,
    )


def _id_from_rid(raw_rid: str, *, expected_kind: str) -> str | None:
    text = str(raw_rid or "").strip()
    if not text:
        return None
    try:
        kind, parsed = parse_rid(text)
    except ValueError:
        return None
    if kind != expected_kind:
        return None
    return parsed


def _connection_id_from_rid(connection_rid: str) -> str | None:
    return _id_from_rid(connection_rid, expected_kind="connection")


def _connection_rid(connection_id: str) -> str:
    return build_rid("connection", connection_id)


def _table_import_id_from_rid(table_import_rid: str) -> str | None:
    return _id_from_rid(table_import_rid, expected_kind="table-import")


def _table_import_rid(table_import_id: str) -> str:
    return build_rid("table-import", table_import_id)


def _file_import_id_from_rid(file_import_rid: str) -> str | None:
    return _id_from_rid(file_import_rid, expected_kind="file-import")


def _file_import_rid(file_import_id: str) -> str:
    return build_rid("file-import", file_import_id)


def _virtual_table_id_from_rid(virtual_table_rid: str) -> str | None:
    return _id_from_rid(virtual_table_rid, expected_kind="virtual-table")


def _virtual_table_rid(virtual_table_id: str) -> str:
    return build_rid("virtual-table", virtual_table_id)


def _dataset_id_from_any(raw: Any) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if text.startswith("ri."):
        return _dataset_id_from_rid(text)
    return text


def _export_run_rid(run_id: str) -> str:
    return build_rid("export-run", run_id)


def _export_run_id_from_rid(export_run_rid: str) -> str | None:
    return _id_from_rid(export_run_rid, expected_kind="export-run")


def _normalize_export_run_method(raw: Any) -> str:
    method = str(raw or "POST").strip().upper() or "POST"
    if method not in {"GET", "POST", "PUT", "PATCH"}:
        raise ValueError("method must be one of GET, POST, PUT, PATCH")
    return method


def _normalize_export_run_headers(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    normalized: Dict[str, str] = {}
    for key, value in raw.items():
        header_name = str(key or "").strip()
        if not header_name:
            continue
        lower = header_name.lower()
        if lower in {"host", "content-length"}:
            continue
        normalized[header_name] = str(value if value is not None else "")
    return normalized


def _normalize_export_run_timeout_seconds(raw: Any) -> float:
    try:
        timeout_ms = int(raw)
    except (TypeError, ValueError):
        timeout_ms = 10_000
    timeout_ms = max(1_000, min(timeout_ms, 60_000))
    return timeout_ms / 1000.0


def _is_valid_http_url(raw: Any) -> bool:
    parsed = urlparse(str(raw or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _task_status_to_export_status(task_status: TaskStatus) -> str:
    if task_status == TaskStatus.PENDING:
        return "QUEUED"
    if task_status in {TaskStatus.PROCESSING, TaskStatus.RETRYING}:
        return "RUNNING"
    if task_status == TaskStatus.COMPLETED:
        return "SUCCEEDED"
    if task_status == TaskStatus.CANCELLED:
        return "CANCELLED"
    return "FAILED"


def _iso_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _export_run_response_from_task(*, connection_id: str, task: Any) -> Dict[str, Any]:
    result_data = {}
    result = getattr(task, "result", None)
    if result is not None and getattr(result, "data", None) and isinstance(result.data, dict):
        result_data = dict(result.data)
    metadata = getattr(task, "metadata", {}) if isinstance(getattr(task, "metadata", {}), dict) else {}

    audit_log_id = result_data.get("auditLogId") or metadata.get("audit_log_id")
    side_effect_delivery = result_data.get("sideEffectDelivery")
    writeback_status = result_data.get("writebackStatus")

    task_status = getattr(task, "status", TaskStatus.FAILED)
    if not isinstance(task_status, TaskStatus):
        try:
            task_status = TaskStatus(str(task_status))
        except ValueError:
            task_status = TaskStatus.FAILED

    response: Dict[str, Any] = {
        "rid": _export_run_rid(str(getattr(task, "task_id", ""))),
        "connectionRid": _connection_rid(connection_id),
        "status": _task_status_to_export_status(task_status),
        "taskId": str(getattr(task, "task_id", "")),
        "createdTime": _iso_timestamp(getattr(task, "created_at", None)),
        "startedTime": _iso_timestamp(getattr(task, "started_at", None)),
        "completedTime": _iso_timestamp(getattr(task, "completed_at", None)),
    }
    if isinstance(audit_log_id, str) and audit_log_id.strip():
        response["auditLogId"] = audit_log_id
    if isinstance(side_effect_delivery, dict):
        response["sideEffectDelivery"] = side_effect_delivery
    if writeback_status not in (None, ""):
        response["writebackStatus"] = str(writeback_status)
    if result is not None and getattr(result, "error", None):
        response["error"] = str(result.error)
    return response


def _parse_allowlist(raw: str) -> set[str]:
    return {item.strip().lower() for item in str(raw or "").split(",") if item.strip()}


def _invalid_pagination_response(message: str) -> JSONResponse:
    return _foundry_error(
        status.HTTP_400_BAD_REQUEST,
        error_code="INVALID_ARGUMENT",
        error_name="InvalidArgument",
        parameters={"message": message},
    )


def _parse_page_size_value(page_size: Any, *, default: int = 100) -> int | JSONResponse:
    if page_size is None:
        return default
    try:
        parsed = int(page_size)
    except (TypeError, ValueError):
        return _invalid_pagination_response("pageSize must be an integer")
    if parsed <= 0:
        return _invalid_pagination_response("pageSize must be > 0")
    return parsed


def _parse_page_offset(page_token: str | None) -> int | JSONResponse:
    if not page_token:
        return 0
    try:
        offset = int(page_token)
    except (TypeError, ValueError):
        return _invalid_pagination_response("pageToken is invalid")
    if offset < 0:
        return _invalid_pagination_response("pageToken is invalid")
    return offset


def _is_flag_or_allowlist_enabled(*, global_enabled: bool, allowlist_raw: str, db_name: str | None) -> bool:
    if global_enabled:
        return True
    db = str(db_name or "").strip().lower()
    if not db:
        return False
    return db in _parse_allowlist(allowlist_raw)


def _resolve_feature_flags_settings(settings_provider=get_settings) -> Any:
    settings = settings_provider()
    flags = getattr(settings, "feature_flags", None)
    if flags is None:
        flags = getattr(settings, "features", None)
    return flags


def _jdbc_enabled_for_flags(flags: Any, db_name: str | None) -> bool:
    return _is_flag_or_allowlist_enabled(
        global_enabled=bool(getattr(flags, "enable_foundry_connectivity_jdbc", False)),
        allowlist_raw=str(getattr(flags, "foundry_connectivity_jdbc_db_allowlist", "") or ""),
        db_name=db_name,
    )


def _cdc_enabled_for_flags(flags: Any, db_name: str | None) -> bool:
    return _is_flag_or_allowlist_enabled(
        global_enabled=bool(getattr(flags, "enable_foundry_connectivity_cdc", False)),
        allowlist_raw=str(getattr(flags, "foundry_connectivity_cdc_db_allowlist", "") or ""),
        db_name=db_name,
    )


def _feature_flags_settings() -> Any:
    return _resolve_feature_flags_settings()


def _jdbc_enabled_for_db(db_name: str | None) -> bool:
    return _jdbc_enabled_for_flags(_feature_flags_settings(), db_name)


def _cdc_enabled_for_db(db_name: str | None) -> bool:
    return _cdc_enabled_for_flags(_feature_flags_settings(), db_name)


def _requires_cdc_feature(import_mode: str) -> bool:
    return str(import_mode or "").strip().upper() in {"CDC", "STREAMING"}


def _connection_source_types() -> tuple[str, ...]:
    return tuple(connection_source_type_for_kind(kind) for kind in SUPPORTED_CONNECTOR_KINDS)


def _table_import_source_types() -> tuple[str, ...]:
    return tuple(table_import_source_type_for_kind(kind) for kind in SUPPORTED_CONNECTOR_KINDS)


def _file_import_source_types() -> tuple[str, ...]:
    return tuple(file_import_source_type_for_kind(kind) for kind in SUPPORTED_CONNECTOR_KINDS)


def _virtual_table_source_types() -> tuple[str, ...]:
    return tuple(virtual_table_source_type_for_kind(kind) for kind in SUPPORTED_CONNECTOR_KINDS)


def _connection_kind_from_source(source: ConnectorSource) -> str:
    try:
        return connector_kind_from_source_type(source.source_type, strict=True)
    except ValueError:
        inferred = resolve_connector_kind_from_connection_config(source.config_json or {})
        if inferred in SUPPORTED_CONNECTOR_KINDS:
            logger.warning(
                "Unknown connection source_type '%s'; inferred connector kind '%s' from connection configuration",
                source.source_type,
                inferred,
            )
            return inferred
        raise


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
        "exportsEnabled": True,
        "exportEnabledWithoutMarkingsValidation": False,
    }


def _normalize_export_settings(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return _default_export_settings()
    exports_enabled_raw = value.get("exportsEnabled")
    without_markings_raw = value.get("exportEnabledWithoutMarkingsValidation")
    if exports_enabled_raw is None:
        exports_enabled_raw = True
    if without_markings_raw is None:
        without_markings_raw = False
    return {
        "exportsEnabled": coerce_optional_bool(
            exports_enabled_raw,
            default=True,
            allow_numeric=True,
        ),
        "exportEnabledWithoutMarkingsValidation": coerce_optional_bool(
            without_markings_raw,
            default=False,
            allow_numeric=True,
        ),
    }


def _normalize_markings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in value:
        token = str(raw or "").strip()
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(token)
    return normalized


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
    if not name.lower().endswith(".jar"):
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
            except (TypeError, ValueError) as exc:
                logger.debug(
                    "Dropping invalid custom JDBC driver size metadata (fileName=%s sizeBytes=%r): %s",
                    file_name,
                    item.get("sizeBytes"),
                    exc,
                )
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


def _resource_invalid_config_response(
    *,
    resource_kind: str,
    message: str,
) -> JSONResponse:
    return _foundry_error(
        status.HTTP_400_BAD_REQUEST,
        error_code="INVALID_ARGUMENT",
        error_name=_RESOURCE_CONFIG_ERROR_NAMES.get(resource_kind, "InvalidArgument"),
        parameters={"message": str(message or "Invalid configuration")},
    )


def _resource_import_mode_from_source(*, source_cfg: Dict[str, Any], resource_kind: str) -> str:
    if resource_kind == "virtual_table":
        return normalize_import_mode(source_cfg.get("import_mode") or "SNAPSHOT")
    return normalize_import_mode(source_cfg.get("import_mode"))


def _safe_import_mode(raw_mode: Any) -> str:
    try:
        return normalize_import_mode(raw_mode)
    except ValueError:
        return "SNAPSHOT"


def _resource_import_config_from_source(*, source_cfg: Dict[str, Any], resource_kind: str) -> Dict[str, Any]:
    config_key = {
        "table_import": "table_import_config",
        "file_import": "file_import_config",
        "virtual_table": "virtual_table_config",
    }.get(resource_kind, "")
    raw = source_cfg.get(config_key)
    return dict(raw) if isinstance(raw, dict) else {}


def _resource_parent_rid(*, cfg: Dict[str, Any], connection_id: str) -> str:
    raw_parent = str(cfg.get("parent_rid") or "").strip()
    if not raw_parent:
        return _connection_rid(connection_id)
    return _connection_rid(_connection_id_from_rid(raw_parent) or connection_id)


def _resource_branch_name(*, cfg: Dict[str, Any], mapping: ConnectorMapping | None) -> str | None:
    if mapping is not None:
        return str(mapping.target_branch or "").strip() or None
    return str(cfg.get("branch_name") or "").strip() or None


def _normalize_connection_config_for_storage(configuration: Dict[str, Any], *, kind: str) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "type": _KIND_TO_CONNECTION_CONFIG_TYPE.get(kind, "GoogleSheetsConnectionConfig"),
    }

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
    for key in {
        "access_token",
        "refresh_token",
        "api_key",
        "service_account_json",
        "password",
        "private_key",
        "token",
        "dsn",
    }:
        if key in base and base.get(key) not in (None, ""):
            secrets[key] = base.get(key)
    if "connection_string" in base and base.get("connection_string") not in (None, ""):
        secrets["connection_string"] = base.get("connection_string")
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


def _validated_execute_result_payload(
    *,
    result_payload: Any,
    resource_kind: str,
) -> Dict[str, Any]:
    if not isinstance(result_payload, dict):
        raise RuntimeError(f"{resource_kind} execution returned an invalid payload")
    status_value = str(result_payload.get("status") or "").strip().lower()
    if status_value not in {"success", "succeeded", "ok"}:
        raise RuntimeError(f"{resource_kind} execution did not report success")
    return result_payload
