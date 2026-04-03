from __future__ import annotations

import re
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from data_connector.adapters.base import ConnectorAdapter, ConnectorConnectionTestResult, ConnectorExtractResult
from data_connector.adapters.blocking_query import run_blocking_query
from data_connector.adapters.sql_query_guard import build_ordered_wrapper_query, normalize_sql_query

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$.]*$")


def _safe_columns(rows: list[Dict[str, Any]]) -> list[str]:
    if not rows:
        return []
    return [str(k) for k in rows[0].keys()]


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


class MySQLConnectorService(ConnectorAdapter):
    connector_kind = "mysql"

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

    def _require_driver(self):
        try:
            import pymysql  # type: ignore

            return pymysql
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "pymysql is required for MySQL runtime. "
                "Install dependency before enabling mysql connector."
            ) from exc

    async def get_public_configuration(self, *, config: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {"type": "MySqlConnectionConfig"}
        for key in ("host", "port", "database", "username", "schema"):
            value = config.get(key)
            if value not in (None, ""):
                out[key] = value
        return out

    async def test_connection(self, *, config: Dict[str, Any], secrets: Dict[str, Any]) -> ConnectorConnectionTestResult:
        handled_errors: tuple[type[BaseException], ...] = (
            RuntimeError,
            ValueError,
            OSError,
            TimeoutError,
        )
        try:
            driver = self._require_driver()
            driver_error = getattr(driver, "MySQLError", None)
            if isinstance(driver_error, type) and issubclass(driver_error, BaseException):
                handled_errors = handled_errors + (driver_error,)

            def _run() -> None:
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
                        cur.execute("SELECT 1")
                        cur.fetchall()
                finally:
                    conn.close()

            await run_blocking_query(_run, adapter_name="MySQL", operation="connection test")
            return ConnectorConnectionTestResult(ok=True, message="Connection is healthy", details={})
        except handled_errors as exc:
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
            conn = driver.connect(
                **self._connect_kwargs(config=config, secrets=secrets),
                cursorclass=driver.cursors.DictCursor,
                connect_timeout=5,
                read_timeout=30,
                write_timeout=30,
                autocommit=True,
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(query, params or [])
                    fetched = cur.fetchall() or []
                    rows_dict = [dict(row) for row in fetched]
                    return ConnectorExtractResult(
                        columns=_safe_columns(rows_dict),
                        rows=rows_dict,
                        next_state={},
                    )
            finally:
                conn.close()

        return await run_blocking_query(_run, adapter_name="MySQL", operation="query fetch")

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
        order_columns = [watermark_column]
        if tie_breaker:
            order_columns.append(tie_breaker)
        if token is None:
            query = build_ordered_wrapper_query(base_query, order_columns=order_columns, alias="base")
            params: list[Any] = []
        else:
            if tie_breaker and tie_token is not None:
                predicate = (
                    f"({watermark_column} > %s OR ({watermark_column} = %s AND {tie_breaker} > %s))"
                )
                params = [token, token, tie_token]
                order_clause = f"{watermark_column} ASC, {tie_breaker} ASC"
            else:
                predicate = f"{watermark_column} > %s"
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
        cdc_strategy = str(import_config.get("cdcStrategy") or import_config.get("cdc_strategy") or "binlog").strip().lower()
        token_column_raw = str(import_config.get("cdcTokenColumn") or import_config.get("cdc_token_column") or "").strip()
        if not token_column_raw and cdc_strategy in {"binlog", "mysql_cdc"}:
            token_column_raw = "binlog_pos"
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
        order_columns = [token_column] if token_column else []
        if tie_breaker:
            order_columns.append(tie_breaker)
        if token is None or not token_column:
            query = build_ordered_wrapper_query(cdc_query, order_columns=order_columns, alias="cdc") if order_columns else cdc_query
            params: list[Any] = []
        else:
            if tie_breaker and tie_token is not None:
                predicate = f"({token_column} > %s OR ({token_column} = %s AND {tie_breaker} > %s))"
                params = [token, token, tie_token]
                order_clause = f"{token_column} ASC, {tie_breaker} ASC"
            else:
                predicate = f"{token_column} > %s"
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
