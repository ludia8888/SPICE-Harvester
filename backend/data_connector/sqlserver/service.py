from __future__ import annotations

from typing import Any, Dict, Optional

from data_connector.adapters.dbapi_sql_connector import DbApiSqlConnectorAdapter


class SqlServerConnectorService(DbApiSqlConnectorAdapter):
    connector_kind = "sqlserver"
    adapter_display_name = "SQL Server"
    window_placeholder_style = "qmark"
    default_cdc_strategy = "change_tracking"
    include_cdc_strategy_in_next_state = True
    driver_module_name = "pyodbc"
    driver_dependency_name = "pyodbc"
    connect_call_style = "single_argument"

    def _connection_string(self, *, config: Dict[str, Any], secrets: Dict[str, Any]) -> str:
        conn_str = str(
            secrets.get("connection_string")
            or config.get("connection_string")
            or config.get("connectionString")
            or ""
        ).strip()
        if conn_str:
            return conn_str

        host = str(config.get("host") or "").strip()
        port = int(config.get("port") or 1433)
        database = str(config.get("database") or "").strip()
        username = str(config.get("username") or config.get("user") or "").strip()
        password = str(secrets.get("password") or config.get("password") or "").strip()
        driver = str(config.get("driver") or "ODBC Driver 18 for SQL Server").strip()
        encrypt = "yes" if bool(config.get("encrypt", True)) else "no"
        trust_cert = "yes" if bool(config.get("trustServerCertificate", True)) else "no"
        if not host or not database or not username:
            raise ValueError("SQL Server connection requires host/database/username")
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"Encrypt={encrypt};"
            f"TrustServerCertificate={trust_cert};"
        )

    def _connect_single_argument(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
    ) -> str:
        return self._connection_string(config=config, secrets=secrets)

    def _connect_extra_kwargs(
        self,
        *,
        driver: Any,
        purpose: str,
    ) -> Dict[str, Any]:
        _ = driver
        return {"timeout": 5 if purpose == "test" else 10}

    def _default_cdc_token_column(self, *, cdc_strategy: str, import_config: Dict[str, Any]) -> str:
        _ = import_config
        if cdc_strategy == "change_tracking":
            return "SYS_CHANGE_VERSION"
        if cdc_strategy == "cdc":
            return "__$start_lsn"
        return ""

    async def get_public_configuration(self, *, config: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {"type": "SqlServerConnectionConfig"}
        for key in ("host", "port", "database", "username", "driver"):
            value = config.get(key)
            if value not in (None, ""):
                out[key] = value
        return out

    async def peek_change_token(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        return await super().peek_change_token(
            config=config,
            secrets=secrets,
            import_config=import_config,
        )

    async def _peek_strategy_token(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Dict[str, Any],
    ) -> Optional[str]:
        cdc_strategy = str(import_config.get("cdcStrategy") or import_config.get("cdc_strategy") or "").strip().lower()
        if cdc_strategy == "change_tracking":
            result = await self._fetch(
                query="SELECT CHANGE_TRACKING_CURRENT_VERSION() AS token",
                config=config,
                secrets=secrets,
            )
            if result.rows:
                value = result.rows[0].get("token") or result.rows[0].get("TOKEN")
                return str(value) if value is not None else None

        if cdc_strategy == "cdc":
            result = await self._fetch(
                query="SELECT CONVERT(varchar(200), sys.fn_cdc_get_max_lsn(), 1) AS token",
                config=config,
                secrets=secrets,
            )
            if result.rows:
                value = result.rows[0].get("token") or result.rows[0].get("TOKEN")
                return str(value) if value is not None else None
        return None
