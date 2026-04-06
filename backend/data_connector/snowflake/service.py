from __future__ import annotations

from typing import Any, Dict, Optional

from data_connector.adapters.dbapi_sql_connector import DbApiSqlConnectorAdapter
from data_connector.adapters.sql_connector_common import sanitize_simple_identifier as _sanitize_identifier


class SnowflakeConnectorService(DbApiSqlConnectorAdapter):
    connector_kind = "snowflake"
    adapter_display_name = "Snowflake"
    default_cdc_strategy = "stream"
    include_cdc_strategy_in_next_state = True
    driver_module_name = "snowflake.connector"
    driver_dependency_name = "snowflake-connector-python"
    connect_call_style = "kwargs"

    async def get_public_configuration(self, *, config: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {"type": "SnowflakeConnectionConfig"}
        for key in ("account", "warehouse", "database", "schema", "role", "username"):
            value = config.get(key)
            if value not in (None, ""):
                out[key] = value
        return out

    def _connect_kwargs(self, *, config: Dict[str, Any], secrets: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "account": str(config.get("account") or "").strip(),
            "user": str(config.get("username") or config.get("user") or "").strip(),
            "password": str(secrets.get("password") or config.get("password") or "").strip(),
            "warehouse": str(config.get("warehouse") or "").strip() or None,
            "database": str(config.get("database") or "").strip() or None,
            "schema": str(config.get("schema") or "").strip() or None,
            "role": str(config.get("role") or "").strip() or None,
        }

    def _default_cdc_token_column(self, *, cdc_strategy: str, import_config: Dict[str, Any]) -> str:
        if cdc_strategy in {"stream", "change_tracking"}:
            return "METADATA$ROW_ID"
        if cdc_strategy == "timestamp":
            return self._first_import_value(import_config, "watermarkColumn", "watermark_column")
        return ""

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
        stream_name = str(import_config.get("stream") or import_config.get("streamName") or "").strip()
        if cdc_strategy in {"stream", "change_tracking"} and stream_name:
            safe_stream = _sanitize_identifier(stream_name, field_name="streamName")
            result = await self._fetch(
                query=f"SELECT MAX(METADATA$ROW_ID) AS token FROM {safe_stream}",
                config=config,
                secrets=secrets,
            )
            if result.rows:
                value = result.rows[0].get("TOKEN") or result.rows[0].get("token")
                return str(value) if value is not None else None
        return None
