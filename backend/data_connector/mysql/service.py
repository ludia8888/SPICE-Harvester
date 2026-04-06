from __future__ import annotations

from typing import Any, Dict, Optional
from urllib.parse import urlparse

from data_connector.adapters.dbapi_sql_connector import DbApiSqlConnectorAdapter
from data_connector.adapters.blocking_query import run_blocking_query


def _parse_mysql_dsn(dsn: str) -> Dict[str, Any]:
    parsed = urlparse(dsn)
    if parsed.scheme.lower() not in {"mysql", "mariadb"}:
        raise ValueError("MySQL DSN must use mysql:// or mariadb://")
    host = parsed.hostname or ""
    port = int(parsed.port or 3306)
    database = str(parsed.path or "").lstrip("/")
    user = parsed.username or ""
    password = parsed.password or ""
    if not host or not database or not user:
        raise ValueError("MySQL DSN requires host, database, and username")
    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
    }


class MySQLConnectorService(DbApiSqlConnectorAdapter):
    connector_kind = "mysql"
    adapter_display_name = "MySQL"
    default_cdc_strategy = "binlog"
    include_cdc_strategy_in_next_state = True
    driver_error_attr_name = "MySQLError"
    driver_module_name = "pymysql"
    driver_dependency_name = "pymysql"
    connect_call_style = "kwargs"

    def _connect_kwargs(self, *, config: Dict[str, Any], secrets: Dict[str, Any]) -> Dict[str, Any]:
        dsn = str(config.get("dsn") or secrets.get("dsn") or "").strip()
        if dsn:
            return _parse_mysql_dsn(dsn)

        host = str(config.get("host") or "").strip()
        port = int(config.get("port") or 3306)
        database = str(config.get("database") or "").strip()
        user = str(config.get("username") or config.get("user") or "").strip()
        password = str(secrets.get("password") or config.get("password") or "").strip()
        if not host or not database or not user:
            raise ValueError("MySQL connection requires host/database/username")

        return {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
        }

    def _connect_extra_kwargs(
        self,
        *,
        driver: Any,
        purpose: str,
    ) -> Dict[str, Any]:
        timeout_kwargs = {
            "connect_timeout": 5,
            "read_timeout": 10 if purpose == "test" else 30,
            "write_timeout": 10 if purpose == "test" else 30,
        }
        return {
            "cursorclass": driver.cursors.DictCursor,
            **timeout_kwargs,
            "autocommit": True,
        }

    def _default_cdc_token_column(self, *, cdc_strategy: str, import_config: Dict[str, Any]) -> str:
        _ = import_config
        if cdc_strategy in {"binlog", "mysql_cdc"}:
            return "binlog_pos"
        return ""

    async def get_public_configuration(self, *, config: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {"type": "MySqlConnectionConfig"}
        for key in ("host", "port", "database", "username", "schema"):
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
        if cdc_strategy in {"binlog", "mysql_cdc"}:
            driver = self._require_driver()

            def _binlog_token() -> Optional[str]:
                conn = driver.connect(
                    **self._connect_kwargs(config=config, secrets=secrets),
                    cursorclass=driver.cursors.DictCursor,
                    connect_timeout=5,
                    read_timeout=10,
                    write_timeout=10,
                    autocommit=True,
                )
                try:
                    with conn.cursor() as cur:
                        cur.execute("SHOW MASTER STATUS")
                        row = cur.fetchone()
                        if not row:
                            return None
                        file_name = row.get("File")
                        position = row.get("Position")
                        if file_name is None or position is None:
                            return None
                        return f"{file_name}:{position}"
                finally:
                    conn.close()

            value = await run_blocking_query(
                _binlog_token,
                adapter_name="MySQL",
                operation="peek_change_token binlog",
            )
            if value is not None:
                return value
        return None
