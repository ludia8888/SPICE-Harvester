from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from data_connector.adapters.dbapi_sql_connector import DbApiSqlConnectorAdapter
from data_connector.adapters.sql_connector_common import (
    row_value_case_insensitive as _row_value,
    sanitize_simple_identifier as _sanitize_identifier,
)
from data_connector.adapters.sql_query_guard import normalize_sql_query
logger = logging.getLogger(__name__)


class OracleConnectorService(DbApiSqlConnectorAdapter):
    connector_kind = "oracle"
    adapter_display_name = "Oracle"
    connection_probe_sql = "SELECT 1 FROM dual"
    window_placeholder_style = "oracle"
    window_include_as = False
    driver_module_name = "oracledb"
    driver_dependency_name = "python-oracledb"
    connect_call_style = "kwargs"

    async def get_public_configuration(self, *, config: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {"type": "OracleConnectionConfig"}
        for key in ("host", "port", "serviceName", "sid", "username", "schema"):
            # allow snake_case input aliases from stored config
            source_keys = {key}
            if key == "serviceName":
                source_keys.add("service_name")
            for source_key in source_keys:
                value = config.get(source_key)
                if value not in (None, ""):
                    out[key] = value
                    break
        return out

    def _connect_kwargs(self, *, config: Dict[str, Any], secrets: Dict[str, Any]) -> Dict[str, Any]:
        dsn = str(config.get("dsn") or secrets.get("dsn") or "").strip()
        user = str(config.get("username") or config.get("user") or "").strip()
        password = str(secrets.get("password") or config.get("password") or "").strip()
        if dsn:
            return {"user": user, "password": password, "dsn": dsn}

        host = str(config.get("host") or "").strip()
        port = int(config.get("port") or 1521)
        service_name = str(config.get("serviceName") or config.get("service_name") or "").strip()
        sid = str(config.get("sid") or "").strip()
        if not host or not user or (not service_name and not sid):
            raise ValueError("Oracle connection requires host/username and serviceName or sid")
        dsn_suffix = f"/{service_name}" if service_name else f":{sid}"
        return {"user": user, "password": password, "dsn": f"{host}:{port}{dsn_suffix}"}

    async def peek_change_token(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        cfg = import_config or {}
        try:
            token_value = await self._peek_token_query_value(
                config=config,
                secrets=secrets,
                import_config=cfg,
            )
            if token_value is not None:
                return token_value
            if self._first_import_value(cfg, "tokenQuery", "token_query"):
                logger.debug("Oracle peek_change_token: tokenQuery returned no rows")
        except Exception as exc:
            logger.warning("Oracle peek_change_token tokenQuery failed: %s", exc)

        cdc_strategy = str(cfg.get("cdcStrategy") or cfg.get("cdc_strategy") or "").strip().lower()
        if cdc_strategy in {"oracle_scn", "scn"}:
            try:
                result = await self._fetch(query="SELECT CURRENT_SCN AS SCN FROM V$DATABASE", config=config, secrets=secrets)
                if result.rows:
                    value = _row_value(result.rows[0], "SCN")
                    return str(value) if value not in (None, "") else None
                logger.debug("Oracle peek_change_token: SCN query returned no rows")
            except Exception as exc:
                logger.warning("Oracle peek_change_token SCN query failed: %s", exc)

        token_column_raw = str(
            cfg.get("cdcTokenColumn")
            or cfg.get("cdc_token_column")
            or cfg.get("watermarkColumn")
            or cfg.get("watermark_column")
            or ""
        ).strip()
        token_column = _sanitize_identifier(token_column_raw, field_name="tokenColumn") if token_column_raw else None
        base_query_raw = str(cfg.get("cdcQuery") or cfg.get("cdc_query") or cfg.get("query") or "").strip()
        if token_column and base_query_raw:
            base_query = normalize_sql_query(base_query_raw, field_name="query")
            query = f"SELECT MAX({token_column}) AS cursor_token FROM ({base_query}) source_rows"
            try:
                result = await self._fetch(query=query, config=config, secrets=secrets)
                if result.rows:
                    value = _row_value(result.rows[0], "cursor_token")
                    return str(value) if value not in (None, "") else None
                logger.debug("Oracle peek_change_token: MAX token query returned no rows")
            except Exception as exc:
                logger.warning("Oracle peek_change_token MAX token query failed: %s", exc)
        logger.debug("Oracle peek_change_token: no token strategy produced a value")
        return None
