from __future__ import annotations

import re
from typing import Any, Dict, Optional

from data_connector.adapters.base import ConnectorAdapter, ConnectorConnectionTestResult, ConnectorExtractResult
from data_connector.adapters.blocking_query import run_blocking_query
from data_connector.adapters.sql_query_guard import normalize_sql_query

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$.]*$")


def _safe_columns(rows: list[Dict[str, Any]]) -> list[str]:
    if not rows:
        return []
    return [str(k) for k in rows[0].keys()]


def _sanitize_identifier(value: str, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    if not _IDENTIFIER_RE.fullmatch(text):
        raise ValueError(f"{field_name} must be a simple identifier")
    return text


def _row_value(row: Dict[str, Any], key: str) -> Any:
    if key in row:
        return row.get(key)
    lower = key.lower()
    for candidate, value in row.items():
        if str(candidate).lower() == lower:
            return value
    return None


class SqlServerConnectorService(ConnectorAdapter):
    connector_kind = "sqlserver"

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

    def _require_driver(self):
        try:
            import pyodbc  # type: ignore

            return pyodbc
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "pyodbc is required for SQL Server runtime. "
                "Install dependency before enabling sqlserver connector."
            ) from exc

    async def get_public_configuration(self, *, config: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {"type": "SqlServerConnectionConfig"}
        for key in ("host", "port", "database", "username", "driver"):
            value = config.get(key)
            if value not in (None, ""):
                out[key] = value
        return out

    async def test_connection(self, *, config: Dict[str, Any], secrets: Dict[str, Any]) -> ConnectorConnectionTestResult:
        try:
            driver = self._require_driver()

            def _run() -> None:
                conn = driver.connect(self._connection_string(config=config, secrets=secrets), timeout=5)
                try:
                    cur = conn.cursor()
                    try:
                        cur.execute("SELECT 1")
                        cur.fetchall()
                    finally:
                        cur.close()
                finally:
                    conn.close()

            await run_blocking_query(_run, adapter_name="SQL Server", operation="connection test")
            return ConnectorConnectionTestResult(ok=True, message="Connection is healthy", details={})
        except Exception as exc:
            return ConnectorConnectionTestResult(ok=False, message=str(exc), details={"error": str(exc)})

    async def _fetch(
        self,
        *,
        query: str,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        params: list[Any] | None = None,
    ) -> ConnectorExtractResult:
        if "mockRows" in config:
            rows = config.get("mockRows") or []
            if not isinstance(rows, list):
                rows = []
            dict_rows = [dict(row) for row in rows if isinstance(row, dict)]
            return ConnectorExtractResult(columns=_safe_columns(dict_rows), rows=dict_rows, next_state={})

        driver = self._require_driver()

        def _run() -> ConnectorExtractResult:
            conn = driver.connect(self._connection_string(config=config, secrets=secrets), timeout=10)
            try:
                cur = conn.cursor()
                try:
                    cur.execute(query, params or [])
                    columns = [str(desc[0]) for desc in cur.description or []]
                    fetched = cur.fetchall() or []
                    rows_dict: list[Dict[str, Any]] = []
                    for row in fetched:
                        rows_dict.append({columns[i]: row[i] for i in range(len(columns))})
                    return ConnectorExtractResult(columns=columns, rows=rows_dict, next_state={})
                finally:
                    cur.close()
            finally:
                conn.close()

        return await run_blocking_query(_run, adapter_name="SQL Server", operation="query fetch")

    async def snapshot_extract(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Dict[str, Any],
    ) -> ConnectorExtractResult:
        query = normalize_sql_query(str(import_config.get("query") or ""), field_name="query")
        return await self._fetch(query=query, config=config, secrets=secrets)

    async def incremental_extract(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Dict[str, Any],
        sync_state: Dict[str, Any],
    ) -> ConnectorExtractResult:
        base_query = normalize_sql_query(str(import_config.get("query") or ""), field_name="query")
        watermark_column_raw = str(import_config.get("watermarkColumn") or import_config.get("watermark_column") or "").strip()
        if not base_query or not watermark_column_raw:
            raise ValueError("incremental mode requires query and watermarkColumn")

        watermark_column = _sanitize_identifier(watermark_column_raw, field_name="watermarkColumn")
        tie_breaker_raw = str(
            import_config.get("watermarkTieBreakerColumn")
            or import_config.get("watermark_tie_breaker_column")
            or ""
        ).strip()
        tie_breaker = _sanitize_identifier(tie_breaker_raw, field_name="watermarkTieBreakerColumn") if tie_breaker_raw else None

        token = sync_state.get("watermark")
        tie_token = sync_state.get("watermark_tiebreaker")

        if token is None:
            query = base_query
            params: list[Any] = []
        else:
            if tie_breaker and tie_token is not None:
                predicate = (
                    f"({watermark_column} > ? OR ({watermark_column} = ? AND {tie_breaker} > ?))"
                )
                params = [token, token, tie_token]
                order_clause = f"{watermark_column} ASC, {tie_breaker} ASC"
            else:
                predicate = f"{watermark_column} > ?"
                params = [token]
                order_clause = f"{watermark_column} ASC"
            query = f"SELECT * FROM ({base_query}) AS base WHERE {predicate} ORDER BY {order_clause}"

        result = await self._fetch(query=query, config=config, secrets=secrets, params=params if token is not None else None)
        if result.rows:
            last = result.rows[-1]
            next_state: Dict[str, Any] = {"watermark": _row_value(last, watermark_column)}
            if tie_breaker:
                next_state["watermark_tiebreaker"] = _row_value(last, tie_breaker)
            result = ConnectorExtractResult(columns=result.columns, rows=result.rows, next_state=next_state)
        return result

    async def cdc_extract(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Dict[str, Any],
        sync_state: Dict[str, Any],
    ) -> ConnectorExtractResult:
        cdc_query_raw = str(import_config.get("cdcQuery") or import_config.get("cdc_query") or "").strip()
        cdc_query = normalize_sql_query(cdc_query_raw, field_name="cdcQuery") if cdc_query_raw else ""
        cdc_strategy = str(import_config.get("cdcStrategy") or import_config.get("cdc_strategy") or "change_tracking").strip().lower()
        token_column_raw = str(import_config.get("cdcTokenColumn") or import_config.get("cdc_token_column") or "").strip()
        if not token_column_raw and cdc_strategy == "change_tracking":
            token_column_raw = "SYS_CHANGE_VERSION"
        if not token_column_raw and cdc_strategy == "cdc":
            token_column_raw = "__$start_lsn"
        token_column = _sanitize_identifier(token_column_raw, field_name="cdcTokenColumn") if token_column_raw else None

        tie_breaker_raw = str(
            import_config.get("cdcTieBreakerColumn")
            or import_config.get("cdc_tie_breaker_column")
            or ""
        ).strip()
        tie_breaker = _sanitize_identifier(tie_breaker_raw, field_name="cdcTieBreakerColumn") if tie_breaker_raw else None

        if not cdc_query:
            return await self.incremental_extract(
                config=config,
                secrets=secrets,
                import_config=import_config,
                sync_state=sync_state,
            )

        token = sync_state.get("cdc_token")
        tie_token = sync_state.get("cdc_tiebreaker")
        if token is None or not token_column:
            query = cdc_query
            params: list[Any] = []
        else:
            if tie_breaker and tie_token is not None:
                predicate = f"({token_column} > ? OR ({token_column} = ? AND {tie_breaker} > ?))"
                params = [token, token, tie_token]
                order_clause = f"{token_column} ASC, {tie_breaker} ASC"
            else:
                predicate = f"{token_column} > ?"
                params = [token]
                order_clause = f"{token_column} ASC"
            query = f"SELECT * FROM ({cdc_query}) AS cdc WHERE {predicate} ORDER BY {order_clause}"

        result = await self._fetch(query=query, config=config, secrets=secrets, params=params if token is not None and token_column else None)
        if result.rows and token_column:
            last = result.rows[-1]
            next_state: Dict[str, Any] = {
                "cdc_token": _row_value(last, token_column),
                "cdc_strategy": cdc_strategy,
            }
            if tie_breaker:
                next_state["cdc_tiebreaker"] = _row_value(last, tie_breaker)
            result = ConnectorExtractResult(columns=result.columns, rows=result.rows, next_state=next_state)
        return result

    async def peek_change_token(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        cfg = import_config or {}
        token_query_raw = str(cfg.get("tokenQuery") or cfg.get("token_query") or "").strip()
        token_query = normalize_sql_query(token_query_raw, field_name="tokenQuery") if token_query_raw else ""
        if token_query:
            result = await self._fetch(query=token_query, config=config, secrets=secrets)
            if result.rows and result.columns:
                value = result.rows[0].get(result.columns[0])
                return str(value) if value is not None else None

        cdc_strategy = str(cfg.get("cdcStrategy") or cfg.get("cdc_strategy") or "").strip().lower()
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

        watermark_column_raw = str(cfg.get("watermarkColumn") or cfg.get("watermark_column") or "").strip()
        table_name_raw = str(cfg.get("table") or cfg.get("tableName") or "").strip()
        if not watermark_column_raw or not table_name_raw:
            return None

        watermark_column = _sanitize_identifier(watermark_column_raw, field_name="watermarkColumn")
        table_name = _sanitize_identifier(table_name_raw, field_name="tableName")
        result = await self._fetch(
            query=f"SELECT MAX({watermark_column}) AS token FROM {table_name}",
            config=config,
            secrets=secrets,
        )
        if result.rows:
            value = result.rows[0].get("token") or result.rows[0].get("TOKEN")
            return str(value) if value is not None else None
        return None
