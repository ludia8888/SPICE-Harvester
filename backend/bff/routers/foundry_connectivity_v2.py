import hashlib
import logging
from typing import Any, Dict
from urllib.parse import urlparse
from uuid import NAMESPACE_URL, uuid4, uuid5

import httpx
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
from data_connector.adapters.import_config_validators import (
    is_jdbc_connector_kind,
    normalize_import_mode,
    validate_resource_import_config,
)
from data_connector.google_sheets.service import GoogleSheetsService
from shared.config.settings import get_settings
from shared.dependencies.providers import AuditLogStoreDep, BackgroundTaskManagerDep, LineageStoreDep
from shared.foundry.auth import require_scopes
from shared.foundry.errors import foundry_error
from shared.foundry.rids import build_rid, parse_rid
from shared.middleware.rate_limiter import RateLimitPresets, rate_limit
from shared.models.background_task import TaskStatus
from shared.observability.tracing import trace_endpoint
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.connector_registry import ConnectorMapping, ConnectorRegistry, ConnectorSource
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/connectivity", tags=["Foundry Connectivity v2"])

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


def _foundry_error(
    status_code: int,
    *,
    error_code: str,
    error_name: str,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    return foundry_error(
        status_code,
        error_code=error_code,
        error_name=error_name,
        parameters=parameters or {},
    )


def _connection_id_from_rid(connection_rid: str) -> str | None:
    text = str(connection_rid or "").strip()
    if not text:
        return None
    try:
        kind, parsed = parse_rid(text)
    except ValueError:
        return None
    if kind != "connection":
        return None
    return parsed


def _connection_rid(connection_id: str) -> str:
    return build_rid("connection", connection_id)


def _table_import_id_from_rid(table_import_rid: str) -> str | None:
    text = str(table_import_rid or "").strip()
    if not text:
        return None
    try:
        kind, parsed = parse_rid(text)
    except ValueError:
        return None
    if kind != "table-import":
        return None
    return parsed


def _table_import_rid(table_import_id: str) -> str:
    return build_rid("table-import", table_import_id)


def _file_import_id_from_rid(file_import_rid: str) -> str | None:
    text = str(file_import_rid or "").strip()
    if not text:
        return None
    try:
        kind, parsed = parse_rid(text)
    except ValueError:
        return None
    if kind != "file-import":
        return None
    return parsed


def _file_import_rid(file_import_id: str) -> str:
    return build_rid("file-import", file_import_id)


def _virtual_table_id_from_rid(virtual_table_rid: str) -> str | None:
    text = str(virtual_table_rid or "").strip()
    if not text:
        return None
    try:
        kind, parsed = parse_rid(text)
    except ValueError:
        return None
    if kind != "virtual-table":
        return None
    return parsed


def _virtual_table_rid(virtual_table_id: str) -> str:
    return build_rid("virtual-table", virtual_table_id)


def _dataset_id_from_rid(dataset_rid: str) -> str | None:
    text = str(dataset_rid or "").strip()
    if not text:
        return None
    try:
        kind, parsed = parse_rid(text)
    except ValueError:
        return None
    if kind != "dataset":
        return None
    return parsed


def _dataset_id_from_any(raw: Any) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if text.startswith("ri."):
        return _dataset_id_from_rid(text)
    return text


def _dataset_rid(dataset_id: str) -> str:
    return build_rid("dataset", dataset_id)


def _export_run_rid(run_id: str) -> str:
    return build_rid("export-run", run_id)


def _export_run_id_from_rid(export_run_rid: str) -> str | None:
    text = str(export_run_rid or "").strip()
    if not text:
        return None
    try:
        kind, parsed = parse_rid(text)
    except ValueError:
        return None
    if kind != "export-run":
        return None
    return parsed


def _coerce_bool(raw: Any, *, default: bool) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(raw)
    text = str(raw or "").strip().lower()
    if text in {"true", "1", "yes", "on"}:
        return True
    if text in {"false", "0", "no", "off"}:
        return False
    return default


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
        except Exception:
            pass

        error_message = str(exc)
        if audit_log_id:
            error_message = f"{error_message} (auditLogId={audit_log_id})"
        raise RuntimeError(error_message) from exc


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


def _feature_flags_settings() -> Any:
    settings = get_settings()
    flags = getattr(settings, "feature_flags", None)
    if flags is None:
        # ApplicationSettings exposes `features`; keep legacy `feature_flags` compatibility.
        flags = getattr(settings, "features", None)
    return flags


def _jdbc_enabled_for_db(db_name: str | None) -> bool:
    flags = _feature_flags_settings()
    return _is_flag_or_allowlist_enabled(
        global_enabled=bool(getattr(flags, "enable_foundry_connectivity_jdbc", False)),
        allowlist_raw=str(getattr(flags, "foundry_connectivity_jdbc_db_allowlist", "") or ""),
        db_name=db_name,
    )


def _cdc_enabled_for_db(db_name: str | None) -> bool:
    flags = _feature_flags_settings()
    return _is_flag_or_allowlist_enabled(
        global_enabled=bool(getattr(flags, "enable_foundry_connectivity_cdc", False)),
        allowlist_raw=str(getattr(flags, "foundry_connectivity_cdc_db_allowlist", "") or ""),
        db_name=db_name,
    )


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

    def _coerce_bool(raw: Any, *, default: bool) -> bool:
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)):
            return bool(raw)
        text = str(raw or "").strip().lower()
        if text in {"true", "1", "yes", "on"}:
            return True
        if text in {"false", "0", "no", "off"}:
            return False
        return default

    exports_enabled_raw = value.get("exportsEnabled")
    without_markings_raw = value.get("exportEnabledWithoutMarkingsValidation")
    if exports_enabled_raw is None:
        exports_enabled_raw = True
    if without_markings_raw is None:
        without_markings_raw = False
    return {
        "exportsEnabled": _coerce_bool(exports_enabled_raw, default=True),
        "exportEnabledWithoutMarkingsValidation": _coerce_bool(without_markings_raw, default=False),
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
        or "master"
    ).strip() or "master"
    object_type = str(
        payload.get("objectType")
        or payload.get("classLabel")
        or destination.get("objectType")
        or destination.get("classLabel")
        or ""
    ).strip() or None
    return db_name, branch, object_type


_RESOURCE_CONFIG_ERROR_NAMES = {
    "table_import": "TableImportInvalidConfig",
    "file_import": "FileImportInvalidConfig",
    "virtual_table": "VirtualTableInvalidConfig",
}


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
    dataset_branch = str(dataset.branch or "").strip() or "master"
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
            branch=target_branch or "master",
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
    connector_kind = connector_kind_from_source_type(source.source_type, strict=True)
    dataset_rid = await _resolve_output_dataset_rid(source=source, mapping=mapping, dataset_registry=dataset_registry)

    import_mode = _safe_import_mode(cfg.get("import_mode"))

    table_import_config = cfg.get("table_import_config") if isinstance(cfg.get("table_import_config"), dict) else {}
    if not table_import_config:
        table_import_config = _resolve_table_import_config({}, connector_kind=connector_kind)

    name = (
        str(cfg.get("name") or "").strip()
        or str(cfg.get("display_name") or "").strip()
        or str(cfg.get("sheet_title") or "").strip()
        or f"{connector_kind} import {source.source_id}"
    )
    parent_rid = (
        _connection_rid(_connection_id_from_rid(str(cfg.get("parent_rid") or "").strip()) or connection_id)
        if str(cfg.get("parent_rid") or "").strip()
        else _connection_rid(connection_id)
    )

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
    connector_kind = connector_kind_from_source_type(source.source_type, strict=True)
    dataset_rid = await _resolve_output_dataset_rid(source=source, mapping=mapping, dataset_registry=dataset_registry)

    import_mode = _safe_import_mode(cfg.get("import_mode"))

    file_import_config = cfg.get("file_import_config") if isinstance(cfg.get("file_import_config"), dict) else {}
    if not file_import_config:
        file_import_config = _resolve_file_import_config({}, connector_kind=connector_kind)

    name = (
        str(cfg.get("name") or "").strip()
        or str(cfg.get("display_name") or "").strip()
        or str(cfg.get("file_pattern") or "").strip()
        or f"{connector_kind} file import {source.source_id}"
    )
    parent_rid = (
        _connection_rid(_connection_id_from_rid(str(cfg.get("parent_rid") or "").strip()) or connection_id)
        if str(cfg.get("parent_rid") or "").strip()
        else _connection_rid(connection_id)
    )

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
    connector_kind = connector_kind_from_source_type(source.source_type, strict=True)
    dataset_rid = await _resolve_output_dataset_rid(source=source, mapping=mapping, dataset_registry=dataset_registry)

    virtual_table_config = cfg.get("virtual_table_config") if isinstance(cfg.get("virtual_table_config"), dict) else {}
    if not virtual_table_config:
        virtual_table_config = _resolve_virtual_table_config({}, connector_kind=connector_kind)

    name = (
        str(cfg.get("name") or "").strip()
        or str(cfg.get("display_name") or "").strip()
        or f"{connector_kind} virtual table {source.source_id}"
    )
    parent_rid = (
        _connection_rid(_connection_id_from_rid(str(cfg.get("parent_rid") or "").strip()) or connection_id)
        if str(cfg.get("parent_rid") or "").strip()
        else _connection_rid(connection_id)
    )
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
    branch = str(mapping.target_branch or "").strip() or "master"
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
    branch = str(mapping.target_branch or "").strip() or "master"
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
    branch = str(mapping.target_branch or "").strip() or "master"
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
    resolved_branch = destination_branch or dataset_branch or "master"

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
            return _resource_invalid_config_response(resource_kind="table_import", message=str(exc))
    else:
        table_import_id = str(payload.get("tableImportId") or uuid4()).strip()
        source_type = table_import_source_type_for_kind(connector_kind)
        display_name = _display_name_from_payload(payload, fallback=f"{connector_kind} import {table_import_id}")
        allow_schema_changes = bool(payload.get("allowSchemaChanges") or False)
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
            "table_import_config": table_import_config,
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

    connector_kind = connector_kind_from_source_type(source.source_type, strict=True)
    source_cfg = dict(source.config_json or {})
    target_db = str(mapping.target_db_name or "").strip() or None
    try:
        import_mode = _resource_import_mode_from_source(source_cfg=source_cfg, resource_kind="table_import")
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="table_import", message=str(exc))
    if is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(target_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )
    if _requires_cdc_feature(import_mode) and not _cdc_enabled_for_db(target_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "CDC connectivity is disabled for this ontology"},
        )
    try:
        validate_resource_import_config(
            resource_kind="table_import",
            connector_kind=connector_kind,
            import_mode=import_mode,
            config=_resource_import_config_from_source(source_cfg=source_cfg, resource_kind="table_import"),
        )
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="table_import", message=str(exc))

    branch_name = str(mapping.target_branch or "").strip() or "master"
    requested_by = str(request.headers.get("X-User-ID") or "").strip() or "system"
    connection_rid = _connection_rid(connection_id)
    table_import_rid = _table_import_rid(source.source_id)
    pipeline_id: str | None = None
    job_id: str | None = None
    started_at: Any = None

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
        result_payload = _validated_execute_result_payload(
            result_payload=result_payload,
            resource_kind="table import",
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
    except ValueError as exc:
        logger.warning("Invalid table import runtime configuration for %s: %s", tableImportRid, exc)
        await _record_execute_failure_run(
            pipeline_registry=pipeline_registry,
            pipeline_id=pipeline_id,
            job_id=job_id,
            connection_rid=connection_rid,
            resource_rid=table_import_rid,
            branch_name=branch_name,
            requested_by=requested_by,
            error_detail=str(exc),
            started_at=started_at,
        )
        return _resource_invalid_config_response(resource_kind="table_import", message=str(exc))
    except Exception as exc:
        logger.error("Failed to execute table import %s: %s", tableImportRid, exc)
        await _record_execute_failure_run(
            pipeline_registry=pipeline_registry,
            pipeline_id=pipeline_id,
            job_id=job_id,
            connection_rid=connection_rid,
            resource_rid=table_import_rid,
            branch_name=branch_name,
            requested_by=requested_by,
            error_detail=str(exc),
            started_at=started_at,
        )
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"message": "Failed to execute table import"},
        )

    return build_rid("build", job_id)


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
    resolved_branch = destination_branch or "master"

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
    display_name = _display_name_from_payload(payload, fallback=f"{connector_kind} file import {file_import_id}")
    allow_schema_changes = bool(payload.get("allowSchemaChanges") or False)
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
    if dataset_branch and destination_branch == "master":
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
            "file_import_config": file_import_config,
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
            "file_import_config": file_import_config,
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

    connector_kind = connector_kind_from_source_type(source.source_type, strict=True)
    source_cfg = dict(source.config_json or {})
    target_db = str(mapping.target_db_name or "").strip() or None
    try:
        import_mode = _resource_import_mode_from_source(source_cfg=source_cfg, resource_kind="file_import")
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="file_import", message=str(exc))
    if is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(target_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )
    if _requires_cdc_feature(import_mode) and not _cdc_enabled_for_db(target_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "CDC connectivity is disabled for this ontology"},
        )
    try:
        validate_resource_import_config(
            resource_kind="file_import",
            connector_kind=connector_kind,
            import_mode=import_mode,
            config=_resource_import_config_from_source(source_cfg=source_cfg, resource_kind="file_import"),
        )
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="file_import", message=str(exc))

    branch_name = str(mapping.target_branch or "").strip() or "master"
    requested_by = str(request.headers.get("X-User-ID") or "").strip() or "system"
    connection_rid = _connection_rid(connection_id)
    file_import_rid = _file_import_rid(source.source_id)
    pipeline_id: str | None = None
    job_id: str | None = None
    started_at: Any = None

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

        result_payload = await data_connector_pipelining_service.start_pipelining_file_import(
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
        result_payload = _validated_execute_result_payload(
            result_payload=result_payload,
            resource_kind="file import",
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
    except ValueError as exc:
        logger.warning("Invalid file import runtime configuration for %s: %s", fileImportRid, exc)
        await _record_execute_failure_run(
            pipeline_registry=pipeline_registry,
            pipeline_id=pipeline_id,
            job_id=job_id,
            connection_rid=connection_rid,
            resource_rid=file_import_rid,
            resource_field="fileImportRid",
            branch_name=branch_name,
            requested_by=requested_by,
            error_detail=str(exc),
            started_at=started_at,
        )
        return _resource_invalid_config_response(resource_kind="file_import", message=str(exc))
    except Exception as exc:
        logger.error("Failed to execute file import %s: %s", fileImportRid, exc)
        await _record_execute_failure_run(
            pipeline_registry=pipeline_registry,
            pipeline_id=pipeline_id,
            job_id=job_id,
            connection_rid=connection_rid,
            resource_rid=file_import_rid,
            resource_field="fileImportRid",
            branch_name=branch_name,
            requested_by=requested_by,
            error_detail=str(exc),
            started_at=started_at,
        )
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"message": "Failed to execute file import"},
        )

    return build_rid("build", job_id)


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
    if mapping is None or not str(mapping.target_db_name or "").strip():
        return _foundry_error(
            status.HTTP_409_CONFLICT,
            error_code="CONFLICT",
            error_name="VirtualTableNotReady",
            parameters={"message": "virtual table mapping is incomplete; target ontology is required"},
        )

    connector_kind = connector_kind_from_source_type(source.source_type, strict=True)
    source_cfg = dict(source.config_json or {})
    target_db = str(mapping.target_db_name or "").strip() or None
    try:
        import_mode = _resource_import_mode_from_source(source_cfg=source_cfg, resource_kind="virtual_table")
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="virtual_table", message=str(exc))
    if is_jdbc_connector_kind(connector_kind) and not _jdbc_enabled_for_db(target_db):
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="ConnectivityFeatureDisabled",
            parameters={"message": "JDBC connectivity is disabled for this ontology"},
        )
    try:
        validate_resource_import_config(
            resource_kind="virtual_table",
            connector_kind=connector_kind,
            import_mode=import_mode,
            config=_resource_import_config_from_source(source_cfg=source_cfg, resource_kind="virtual_table"),
        )
    except ValueError as exc:
        return _resource_invalid_config_response(resource_kind="virtual_table", message=str(exc))

    branch_name = str(mapping.target_branch or "").strip() or "master"
    requested_by = str(request.headers.get("X-User-ID") or "").strip() or "system"
    connection_rid = _connection_rid(connection_id)
    virtual_table_rid = _virtual_table_rid(source.source_id)
    pipeline_id: str | None = None
    job_id: str | None = None
    started_at: Any = None

    try:
        pipeline_id = await _ensure_virtual_table_pipeline(
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
                resource_rid=virtual_table_rid,
                resource_field="virtualTableRid",
                branch_name=branch_name,
                requested_by=requested_by,
            ),
            started_at=started_at,
        )

        result_payload = await data_connector_pipelining_service.start_pipelining_virtual_table(
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
        result_payload = _validated_execute_result_payload(
            result_payload=result_payload,
            resource_kind="virtual table",
        )
        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=job_id,
            mode="build",
            status="SUCCESS",
            output_json=_build_execute_run_output(
                connection_rid=connection_rid,
                resource_rid=virtual_table_rid,
                resource_field="virtualTableRid",
                branch_name=branch_name,
                requested_by=requested_by,
                result_payload=result_payload if isinstance(result_payload, dict) else None,
            ),
            started_at=started_at,
            finished_at=utcnow(),
        )
    except ValueError as exc:
        logger.warning("Invalid virtual table runtime configuration for %s: %s", virtualTableRid, exc)
        await _record_execute_failure_run(
            pipeline_registry=pipeline_registry,
            pipeline_id=pipeline_id,
            job_id=job_id,
            connection_rid=connection_rid,
            resource_rid=virtual_table_rid,
            resource_field="virtualTableRid",
            branch_name=branch_name,
            requested_by=requested_by,
            error_detail=str(exc),
            started_at=started_at,
        )
        return _resource_invalid_config_response(resource_kind="virtual_table", message=str(exc))
    except Exception as exc:
        logger.error("Failed to execute virtual table %s: %s", virtualTableRid, exc)
        await _record_execute_failure_run(
            pipeline_registry=pipeline_registry,
            pipeline_id=pipeline_id,
            job_id=job_id,
            connection_rid=connection_rid,
            resource_rid=virtual_table_rid,
            resource_field="virtualTableRid",
            branch_name=branch_name,
            requested_by=requested_by,
            error_detail=str(exc),
            started_at=started_at,
        )
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"message": "Failed to execute virtual table"},
        )

    return build_rid("build", job_id)


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
    resolved_branch = destination_branch or "master"
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
    if dataset_branch and destination_branch == "master":
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
                "import_mode": import_mode,
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

    cfg.update(
        {
            "display_name": _display_name_from_payload(payload, fallback=cfg.get("display_name") or f"{connector_kind} virtual table {virtual_table_id}"),
            "name": _display_name_from_payload(payload, fallback=cfg.get("name") or cfg.get("display_name") or f"{connector_kind} virtual table {virtual_table_id}"),
            "parent_rid": _connection_rid(connection_id),
            "import_mode": import_mode,
            "virtual_table_config": virtual_table_config,
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

    tasks = await task_manager.get_all_tasks(task_type="connectivity_export_run", limit=5000)
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
