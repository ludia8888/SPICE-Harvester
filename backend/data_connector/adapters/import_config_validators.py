from __future__ import annotations

from typing import Any, Dict

from data_connector.adapters.sql_query_guard import normalize_sql_query

_TABLE_IMPORT_MODE_ORDER = ("SNAPSHOT", "APPEND", "UPDATE", "INCREMENTAL", "CDC", "STREAMING")
TABLE_IMPORT_MODES = frozenset(_TABLE_IMPORT_MODE_ORDER)
CDC_COMPAT_IMPORT_MODES = frozenset({"CDC", "STREAMING"})
APPEND_MERGE_IMPORT_MODES = frozenset({"APPEND", "INCREMENTAL", "CDC", "STREAMING"})
FILE_IMPORT_SELECTOR_FIELDS = ("fileImportFilters", "path", "subfolder", "filePattern", "fileFormat")


def is_jdbc_connector_kind(connector_kind: str) -> bool:
    return str(connector_kind or "").strip().lower() != "google_sheets"


def normalize_import_mode(raw_mode: Any, *, mode_field_name: str = "importMode") -> str:
    mode = str(raw_mode or "SNAPSHOT").strip().upper()
    if mode not in TABLE_IMPORT_MODES:
        allowed = ", ".join(_TABLE_IMPORT_MODE_ORDER)
        raise ValueError(f"{mode_field_name} must be one of {allowed}")
    return mode


def normalized_query(config: Dict[str, Any], *, field_name: str) -> str:
    raw_query = str(config.get(field_name) or "").strip()
    if not raw_query:
        return ""
    return normalize_sql_query(raw_query, field_name=field_name)


def validate_jdbc_mode_requirements(
    *,
    resource_label: str,
    import_mode: str,
    config: Dict[str, Any],
) -> None:
    query = normalized_query(config, field_name="query")
    cdc_query = normalized_query(config, field_name="cdcQuery")
    watermark = str(config.get("watermarkColumn") or config.get("watermark_column") or "").strip()

    if import_mode in {"SNAPSHOT", "APPEND", "UPDATE"}:
        if not query:
            raise ValueError(f"{resource_label} config.query is required for JDBC connectors")
        return
    if import_mode == "INCREMENTAL":
        if not query:
            raise ValueError(f"{resource_label} config.query is required for INCREMENTAL mode")
        if not watermark:
            raise ValueError(f"{resource_label} watermarkColumn is required for INCREMENTAL mode")
        return
    if import_mode in CDC_COMPAT_IMPORT_MODES:
        if cdc_query:
            return
        if not query:
            raise ValueError(f"{resource_label} config.cdcQuery or config.query is required for CDC/STREAMING mode")
        if not watermark:
            raise ValueError(f"{resource_label} watermarkColumn is required for CDC/STREAMING mode without cdcQuery")


def validate_resource_import_config(
    *,
    resource_kind: str,
    connector_kind: str,
    import_mode: str,
    config: Dict[str, Any],
    virtual_table_snapshot_error: str = "virtual tables support only SNAPSHOT mode",
) -> None:
    if not isinstance(config, dict) or not config:
        raise ValueError(f"{resource_kind.replace('_', ' ')} config is required")

    if resource_kind == "virtual_table" and import_mode != "SNAPSHOT":
        raise ValueError(virtual_table_snapshot_error)

    if is_jdbc_connector_kind(connector_kind):
        validate_jdbc_mode_requirements(
            resource_label=resource_kind.replace("_", " "),
            import_mode=import_mode,
            config=config,
        )
        return

    if resource_kind == "file_import":
        has_selector = any(
            key in config and config.get(key) not in (None, "", [], {})
            for key in FILE_IMPORT_SELECTOR_FIELDS
        )
        if not has_selector:
            raise ValueError(
                "file import requires at least one selector: fileImportFilters, path, subfolder, filePattern, or fileFormat"
            )
