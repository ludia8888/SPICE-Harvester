from __future__ import annotations

import logging
import re
from typing import Any, Dict, Optional

from data_connector.adapters.base import ConnectorAdapter, ConnectorConnectionTestResult, ConnectorExtractResult
from data_connector.adapters.blocking_query import run_blocking_query
from data_connector.adapters.sql_query_guard import build_ordered_wrapper_query, normalize_sql_query

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$.]*$")
logger = logging.getLogger(__name__)


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


class OracleConnectorService(ConnectorAdapter):
    connector_kind = "oracle"

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

    def _require_driver(self):
        try:
            import oracledb  # type: ignore

            return oracledb
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "python-oracledb is required for Oracle runtime. "
                "Install dependency before enabling oracle connector."
            ) from exc

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

    async def test_connection(self, *, config: Dict[str, Any], secrets: Dict[str, Any]) -> ConnectorConnectionTestResult:
        try:
            oracledb = self._require_driver()

            def _run() -> None:
                conn = oracledb.connect(**self._connect_kwargs(config=config, secrets=secrets))
                try:
                    cur = conn.cursor()
                    try:
                        cur.execute("SELECT 1 FROM dual")
                        cur.fetchall()
                    finally:
                        cur.close()
                finally:
                    conn.close()

            await run_blocking_query(_run, adapter_name="Oracle", operation="connection test")
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

        oracledb = self._require_driver()

        def _run() -> ConnectorExtractResult:
            conn = oracledb.connect(**self._connect_kwargs(config=config, secrets=secrets))
            try:
                cur = conn.cursor()
                try:
                    cur.execute(query, params or [])
                    names = [str(desc[0]) for desc in cur.description or []]
                    fetched = cur.fetchall() or []
                    rows_dict: list[Dict[str, Any]] = []
                    for row in fetched:
                        rows_dict.append({names[i]: row[i] for i in range(len(names))})
                    return ConnectorExtractResult(columns=names, rows=rows_dict, next_state={})
                finally:
                    cur.close()
            finally:
                conn.close()

        return await run_blocking_query(_run, adapter_name="Oracle", operation="query fetch")

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
            query = build_ordered_wrapper_query(base_query, order_columns=order_columns, alias="base", include_as=False)
            params: list[Any] = []
        else:
            if tie_breaker and tie_token is not None:
                predicate = f"({watermark_column} > :1 OR ({watermark_column} = :1 AND {tie_breaker} > :2))"
                params = [token, tie_token]
                order_clause = f"{watermark_column} ASC, {tie_breaker} ASC"
            else:
                predicate = f"{watermark_column} > :1"
                params = [token]
                order_clause = f"{watermark_column} ASC"
            query = f"SELECT * FROM ({base_query}) base WHERE {predicate} ORDER BY {order_clause}"

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
        token_column_raw = str(import_config.get("cdcTokenColumn") or import_config.get("cdc_token_column") or "").strip()
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
            query = (
                build_ordered_wrapper_query(
                    cdc_query,
                    order_columns=order_columns,
                    alias="cdc",
                    include_as=False,
                )
                if order_columns
                else cdc_query
            )
            params: list[Any] = []
        else:
            if tie_breaker and tie_token is not None:
                predicate = f"({token_column} > :1 OR ({token_column} = :1 AND {tie_breaker} > :2))"
                params = [token, tie_token]
                order_clause = f"{token_column} ASC, {tie_breaker} ASC"
            else:
                predicate = f"{token_column} > :1"
                params = [token]
                order_clause = f"{token_column} ASC"
            query = f"SELECT * FROM ({cdc_query}) cdc WHERE {predicate} ORDER BY {order_clause}"

        result = await self._fetch(
            query=query,
            config=config,
            secrets=secrets,
            params=params if token is not None and token_column else None,
        )
        if result.rows and token_column:
            last = result.rows[-1]
            next_state: Dict[str, Any] = {"cdc_token": _row_value(last, token_column)}
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
            try:
                result = await self._fetch(query=token_query, config=config, secrets=secrets)
                if result.rows and result.columns:
                    first_col = result.columns[0]
                    value = _row_value(result.rows[0], first_col)
                    return str(value) if value not in (None, "") else None
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
