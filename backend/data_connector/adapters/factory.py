from __future__ import annotations

import logging
from typing import Any, Dict

from data_connector.adapters.base import ConnectorAdapter
from data_connector.google_sheets.service import GoogleSheetsService
from data_connector.mysql.service import MySQLConnectorService
from data_connector.postgresql.service import PostgreSQLConnectorService
from data_connector.sqlserver.service import SqlServerConnectorService
from data_connector.snowflake.service import SnowflakeConnectorService

SUPPORTED_CONNECTOR_KINDS = {"google_sheets", "snowflake", "postgresql", "mysql", "sqlserver"}

_CONNECTION_SOURCE_TYPE_BY_KIND = {
    "google_sheets": "google_sheets_connection",
    "snowflake": "snowflake_connection",
    "postgresql": "postgresql_connection",
    "mysql": "mysql_connection",
    "sqlserver": "sqlserver_connection",
}

_TABLE_IMPORT_SOURCE_TYPE_BY_KIND = {
    "google_sheets": "google_sheets",
    "snowflake": "snowflake_table_import",
    "postgresql": "postgresql_table_import",
    "mysql": "mysql_table_import",
    "sqlserver": "sqlserver_table_import",
}

_FILE_IMPORT_SOURCE_TYPE_BY_KIND = {
    "google_sheets": "google_sheets_file_import",
    "snowflake": "snowflake_file_import",
    "postgresql": "postgresql_file_import",
    "mysql": "mysql_file_import",
    "sqlserver": "sqlserver_file_import",
}

_VIRTUAL_TABLE_SOURCE_TYPE_BY_KIND = {
    "google_sheets": "google_sheets_virtual_table",
    "snowflake": "snowflake_virtual_table",
    "postgresql": "postgresql_virtual_table",
    "mysql": "mysql_virtual_table",
    "sqlserver": "sqlserver_virtual_table",
}

logger = logging.getLogger(__name__)


def connection_source_type_for_kind(connector_kind: str) -> str:
    kind = str(connector_kind or "").strip().lower()
    return _CONNECTION_SOURCE_TYPE_BY_KIND.get(kind, "google_sheets_connection")


def table_import_source_type_for_kind(connector_kind: str) -> str:
    kind = str(connector_kind or "").strip().lower()
    return _TABLE_IMPORT_SOURCE_TYPE_BY_KIND.get(kind, "google_sheets")


def file_import_source_type_for_kind(connector_kind: str) -> str:
    kind = str(connector_kind or "").strip().lower()
    return _FILE_IMPORT_SOURCE_TYPE_BY_KIND.get(kind, "google_sheets_file_import")


def virtual_table_source_type_for_kind(connector_kind: str) -> str:
    kind = str(connector_kind or "").strip().lower()
    return _VIRTUAL_TABLE_SOURCE_TYPE_BY_KIND.get(kind, "google_sheets_virtual_table")


def connector_kind_from_source_type(source_type: str, *, strict: bool = False) -> str:
    st = str(source_type or "").strip().lower()
    for kind, mapped in _CONNECTION_SOURCE_TYPE_BY_KIND.items():
        if mapped == st:
            return kind
    for kind, mapped in _TABLE_IMPORT_SOURCE_TYPE_BY_KIND.items():
        if mapped == st:
            return kind
    for kind, mapped in _FILE_IMPORT_SOURCE_TYPE_BY_KIND.items():
        if mapped == st:
            return kind
    for kind, mapped in _VIRTUAL_TABLE_SOURCE_TYPE_BY_KIND.items():
        if mapped == st:
            return kind
    if strict and st:
        raise ValueError(f"Unknown connector source_type: {st}")
    if st:
        logger.warning("Unknown connector source_type '%s'; defaulting to google_sheets adapter", st)
    return "google_sheets"


def resolve_connector_kind_from_connection_config(configuration: Dict[str, Any]) -> str:
    cfg = configuration or {}
    cfg_type = str(cfg.get("type") or "").strip().lower()
    driver = str(cfg.get("driver") or cfg.get("jdbcDriver") or cfg.get("jdbc_driver") or "").strip().lower()
    provider = str(
        cfg.get("provider")
        or cfg.get("databaseType")
        or cfg.get("database_type")
        or cfg.get("engine")
        or ""
    ).strip().lower()
    url = str(cfg.get("url") or cfg.get("jdbcUrl") or cfg.get("jdbc_url") or "").strip().lower()

    if cfg_type in {"snowflakeconnectionconfig", "snowflake"} or "snowflake" in {provider, driver} or "snowflake" in url:
        return "snowflake"
    if cfg_type in {"postgresqlconnectionconfig", "postgresql", "postgres"} or "postgres" in provider or "postgres" in driver or "postgres" in url:
        return "postgresql"
    if cfg_type in {"mysqlconnectionconfig", "mysql"} or "mysql" in provider or "mysql" in driver or "mysql" in url:
        return "mysql"
    if (
        cfg_type in {"sqlserverconnectionconfig", "sqlserver", "mssqlconnectionconfig", "mssql"}
        or provider in {"sqlserver", "mssql"}
        or "sqlserver" in driver
        or "jtds" in driver
        or url.startswith("jdbc:sqlserver:")
    ):
        return "sqlserver"
    return "google_sheets"


class _GoogleSheetsAdapter(ConnectorAdapter):
    connector_kind = "google_sheets"

    def __init__(self, *, service: GoogleSheetsService):
        self._service = service

    async def get_public_configuration(self, *, config: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {"type": "GoogleSheetsConnectionConfig"}
        for source_key, target_key in (
            ("sheet_url", "sheetUrl"),
            ("sheetUrl", "sheetUrl"),
            ("account_email", "accountEmail"),
            ("accountEmail", "accountEmail"),
            ("scopes", "scopes"),
        ):
            value = config.get(source_key)
            if value not in (None, ""):
                out[target_key] = value
        return out

    async def test_connection(self, *, config: Dict[str, Any], secrets: Dict[str, Any]):
        access_token = secrets.get("access_token") or config.get("access_token")
        api_key = secrets.get("api_key") or config.get("api_key")
        sheet_url = config.get("sheet_url") or config.get("sheetUrl")
        if not sheet_url:
            return type("ConnTest", (), {"ok": True, "message": "Credentials present (no test URL)", "details": {}})()
        try:
            from data_connector.google_sheets.utils import extract_sheet_id

            await self._service._get_sheet_metadata(  # noqa: SLF001
                extract_sheet_id(str(sheet_url)),
                api_key=str(api_key) if api_key else None,
                access_token=str(access_token) if access_token else None,
            )
            return type("ConnTest", (), {"ok": True, "message": "Connection is healthy", "details": {}})()
        except Exception as exc:
            return type("ConnTest", (), {"ok": False, "message": str(exc), "details": {"error": str(exc)}})()

    async def snapshot_extract(self, *, config: Dict[str, Any], secrets: Dict[str, Any], import_config: Dict[str, Any]):
        _ = import_config
        sheet_url = str(config.get("sheet_url") or config.get("sheetUrl") or "").strip()
        worksheet_name = str(config.get("worksheet_name") or "").strip() or None
        if not sheet_url:
            raise ValueError("sheet_url is required")
        _sheet_id, _metadata, _worksheet_title, _worksheet_sheet_id, values = await self._service.fetch_sheet_values(
            sheet_url,
            worksheet_name=worksheet_name,
            access_token=(secrets.get("access_token") or config.get("access_token")),
            api_key=(secrets.get("api_key") or config.get("api_key")),
        )
        from data_connector.google_sheets.utils import normalize_sheet_data

        columns, rows = normalize_sheet_data(values)
        dict_rows: list[dict[str, Any]] = []
        for row in rows:
            dict_rows.append({columns[i]: row[i] if i < len(row) else None for i in range(len(columns))})
        from data_connector.adapters.base import ConnectorExtractResult

        return ConnectorExtractResult(columns=columns, rows=dict_rows, next_state={})

    async def incremental_extract(self, *, config: Dict[str, Any], secrets: Dict[str, Any], import_config: Dict[str, Any], sync_state: Dict[str, Any]):
        return await self.snapshot_extract(config=config, secrets=secrets, import_config=import_config)

    async def cdc_extract(self, *, config: Dict[str, Any], secrets: Dict[str, Any], import_config: Dict[str, Any], sync_state: Dict[str, Any]):
        return await self.snapshot_extract(config=config, secrets=secrets, import_config=import_config)

    async def peek_change_token(self, *, config: Dict[str, Any], secrets: Dict[str, Any], import_config: Dict[str, Any] | None = None):
        _ = import_config
        sheet_url = str(config.get("sheet_url") or config.get("sheetUrl") or "").strip()
        if not sheet_url:
            return None
        _sheet_id, _metadata, _worksheet_title, _worksheet_sheet_id, values = await self._service.fetch_sheet_values(
            sheet_url,
            worksheet_name=str(config.get("worksheet_name") or "").strip() or None,
            access_token=(secrets.get("access_token") or config.get("access_token")),
            api_key=(secrets.get("api_key") or config.get("api_key")),
        )
        from data_connector.google_sheets.utils import calculate_data_hash

        return calculate_data_hash(values)


class ConnectorAdapterFactory:
    def __init__(self, *, google_sheets_service: GoogleSheetsService):
        self._google_sheets_service = google_sheets_service
        self._snowflake = SnowflakeConnectorService()
        self._postgresql = PostgreSQLConnectorService()
        self._mysql = MySQLConnectorService()
        self._sqlserver = SqlServerConnectorService()

    def get_adapter(self, connector_kind: str) -> ConnectorAdapter:
        kind = str(connector_kind or "").strip().lower()
        if kind == "snowflake":
            return self._snowflake
        if kind == "postgresql":
            return self._postgresql
        if kind == "mysql":
            return self._mysql
        if kind == "sqlserver":
            return self._sqlserver
        return _GoogleSheetsAdapter(service=self._google_sheets_service)
