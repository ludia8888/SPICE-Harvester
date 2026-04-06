from __future__ import annotations

import importlib
from typing import Any, Dict, Sequence

from data_connector.adapters.base import ConnectorAdapter, ConnectorConnectionTestResult, ConnectorExtractResult
from data_connector.adapters.blocking_dbapi import fetch_dbapi_rows, run_dbapi_connection_test
from data_connector.adapters.sql_connector_common import (
    row_value_case_insensitive,
    safe_columns,
    sanitize_simple_identifier,
)
from data_connector.adapters.sql_query_guard import normalize_sql_query
from data_connector.adapters.sql_window_queries import build_windowed_extract_query


class DbApiSqlConnectorAdapter(ConnectorAdapter):
    adapter_display_name: str
    connection_probe_sql: str = "SELECT 1"
    window_placeholder_style: str = "pyformat"
    window_include_as: bool = True
    default_cdc_strategy: str = ""
    include_cdc_strategy_in_next_state: bool = False
    driver_error_attr_name: str = "Error"
    driver_module_name: str = ""
    driver_dependency_name: str = ""
    connect_call_style: str = "custom"

    def _require_driver(self) -> Any:
        if not self.driver_module_name:
            raise NotImplementedError
        try:
            return importlib.import_module(self.driver_module_name)
        except ImportError as exc:  # pragma: no cover
            dependency_name = self.driver_dependency_name or self.driver_module_name
            connector_kind = getattr(self, "connector_kind", self.__class__.__name__)
            raise RuntimeError(
                f"{dependency_name} is required for {self.adapter_display_name} runtime. "
                f"Install dependency before enabling {connector_kind} connector."
            ) from exc

    def _connect_kwargs(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
    ) -> Dict[str, Any]:
        raise NotImplementedError

    def _connect_single_argument(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
    ) -> Any:
        raise NotImplementedError

    def _connect_extra_kwargs(
        self,
        *,
        driver: Any,
        purpose: str,
    ) -> Dict[str, Any]:
        return {}

    def _driver_error_types(self, driver: Any) -> tuple[type[BaseException], ...]:
        driver_error = getattr(driver, self.driver_error_attr_name, None)
        if isinstance(driver_error, type) and issubclass(driver_error, BaseException):
            return (driver_error,)
        return ()

    def _build_connect_call(
        self,
        *,
        driver: Any,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        purpose: str,
    ) -> tuple[Sequence[Any], Dict[str, Any]]:
        extra_kwargs = self._connect_extra_kwargs(driver=driver, purpose=purpose)
        if self.connect_call_style == "kwargs":
            return (), {
                **self._connect_kwargs(config=config, secrets=secrets),
                **extra_kwargs,
            }
        if self.connect_call_style == "single_argument":
            return (
                (self._connect_single_argument(config=config, secrets=secrets),),
                extra_kwargs,
            )
        raise NotImplementedError

    def _connect(self, *, driver: Any, config: Dict[str, Any], secrets: Dict[str, Any], purpose: str) -> Any:
        args, kwargs = self._build_connect_call(
            driver=driver,
            config=config,
            secrets=secrets,
            purpose=purpose,
        )
        return driver.connect(*args, **kwargs)

    @staticmethod
    def _first_import_value(import_config: Dict[str, Any], *keys: str) -> str:
        for key in keys:
            value = import_config.get(key)
            if value not in (None, ""):
                return str(value).strip()
        return ""

    def _default_cdc_token_column(self, *, cdc_strategy: str, import_config: Dict[str, Any]) -> str:
        _ = cdc_strategy, import_config
        return ""

    def _resolved_cdc_strategy(self, *, import_config: Dict[str, Any]) -> str:
        configured = self._first_import_value(import_config, "cdcStrategy", "cdc_strategy")
        if configured:
            return configured.lower()
        return str(self.default_cdc_strategy or "").strip().lower()

    def _next_state_from_last_row(
        self,
        *,
        last_row: Dict[str, Any],
        token_column: str,
        tie_breaker: str | None,
        token_state_key: str,
        tie_state_key: str,
        cdc_strategy: str | None = None,
    ) -> Dict[str, Any]:
        next_state: Dict[str, Any] = {
            token_state_key: row_value_case_insensitive(last_row, token_column),
        }
        if self.include_cdc_strategy_in_next_state and cdc_strategy:
            next_state["cdc_strategy"] = cdc_strategy
        if tie_breaker:
            next_state[tie_state_key] = row_value_case_insensitive(last_row, tie_breaker)
        return next_state

    async def _peek_token_query_value(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Dict[str, Any],
    ) -> str | None:
        token_query_raw = self._first_import_value(import_config, "tokenQuery", "token_query")
        token_query = normalize_sql_query(token_query_raw, field_name="tokenQuery") if token_query_raw else ""
        if not token_query:
            return None
        result = await self._fetch(query=token_query, config=config, secrets=secrets)
        if result.rows and result.columns:
            value = row_value_case_insensitive(result.rows[0], result.columns[0])
            return str(value) if value is not None else None
        return None

    async def _peek_max_table_token(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Dict[str, Any],
    ) -> str | None:
        watermark_column_raw = self._first_import_value(import_config, "watermarkColumn", "watermark_column")
        table_name_raw = self._first_import_value(import_config, "table", "tableName")
        if not watermark_column_raw or not table_name_raw:
            return None

        watermark_column = sanitize_simple_identifier(watermark_column_raw, field_name="watermarkColumn")
        table_name = sanitize_simple_identifier(table_name_raw, field_name="tableName")
        result = await self._fetch(
            query=f"SELECT MAX({watermark_column}) AS token FROM {table_name}",
            config=config,
            secrets=secrets,
        )
        if result.rows:
            value = row_value_case_insensitive(result.rows[0], "token")
            return str(value) if value is not None else None
        return None

    async def _peek_strategy_token(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Dict[str, Any],
    ) -> str | None:
        _ = config, secrets, import_config
        return None

    async def test_connection(self, *, config: Dict[str, Any], secrets: Dict[str, Any]) -> ConnectorConnectionTestResult:
        handled_errors: tuple[type[BaseException], ...] = (
            RuntimeError,
            ValueError,
            OSError,
            TimeoutError,
        )
        try:
            driver = self._require_driver()
            handled_errors = handled_errors + self._driver_error_types(driver)
            await run_dbapi_connection_test(
                adapter_name=self.adapter_display_name,
                connect=lambda: self._connect(
                    driver=driver,
                    config=config,
                    secrets=secrets,
                    purpose="test",
                ),
                probe_sql=self.connection_probe_sql,
            )
            return ConnectorConnectionTestResult(ok=True, message="Connection is healthy", details={})
        except handled_errors as exc:
            return ConnectorConnectionTestResult(ok=False, message=str(exc), details={"error": str(exc)})

    async def _fetch(
        self,
        *,
        query: str,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        params: Sequence[Any] | None = None,
    ) -> ConnectorExtractResult:
        if "mockRows" in config:
            rows = config.get("mockRows") or []
            if not isinstance(rows, list):
                rows = []
            dict_rows = [dict(row) for row in rows if isinstance(row, dict)]
            return ConnectorExtractResult(columns=safe_columns(dict_rows), rows=dict_rows, next_state={})

        driver = self._require_driver()
        return await fetch_dbapi_rows(
            adapter_name=self.adapter_display_name,
            connect=lambda: self._connect(
                driver=driver,
                config=config,
                secrets=secrets,
                purpose="query",
            ),
            query=query,
            params=params,
        )

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
        base_query = normalize_sql_query(self._first_import_value(import_config, "query"), field_name="query")
        watermark_column_raw = self._first_import_value(import_config, "watermarkColumn", "watermark_column")
        if not base_query or not watermark_column_raw:
            raise ValueError("incremental mode requires query and watermarkColumn")

        watermark_column = sanitize_simple_identifier(watermark_column_raw, field_name="watermarkColumn")
        tie_breaker_raw = self._first_import_value(
            import_config,
            "watermarkTieBreakerColumn",
            "watermark_tie_breaker_column",
        )
        tie_breaker = (
            sanitize_simple_identifier(tie_breaker_raw, field_name="watermarkTieBreakerColumn")
            if tie_breaker_raw
            else None
        )

        token = sync_state.get("watermark")
        tie_token = sync_state.get("watermark_tiebreaker")
        query, params = build_windowed_extract_query(
            base_query=base_query,
            token_column=watermark_column,
            tie_breaker=tie_breaker,
            token=token,
            tie_token=tie_token,
            alias="base",
            placeholder_style=self.window_placeholder_style,
            include_as=self.window_include_as,
        )

        result = await self._fetch(
            query=query,
            config=config,
            secrets=secrets,
            params=params if token is not None else None,
        )
        if result.rows:
            last_row = result.rows[-1]
            result = ConnectorExtractResult(
                columns=result.columns,
                rows=result.rows,
                next_state=self._next_state_from_last_row(
                    last_row=last_row,
                    token_column=watermark_column,
                    tie_breaker=tie_breaker,
                    token_state_key="watermark",
                    tie_state_key="watermark_tiebreaker",
                ),
            )
        return result

    async def cdc_extract(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Dict[str, Any],
        sync_state: Dict[str, Any],
    ) -> ConnectorExtractResult:
        cdc_query_raw = self._first_import_value(import_config, "cdcQuery", "cdc_query")
        cdc_query = normalize_sql_query(cdc_query_raw, field_name="cdcQuery") if cdc_query_raw else ""
        cdc_strategy = self._resolved_cdc_strategy(import_config=import_config)
        token_column_raw = self._first_import_value(import_config, "cdcTokenColumn", "cdc_token_column")
        if not token_column_raw:
            token_column_raw = self._default_cdc_token_column(
                cdc_strategy=cdc_strategy,
                import_config=import_config,
            )
        token_column = (
            sanitize_simple_identifier(token_column_raw, field_name="cdcTokenColumn")
            if token_column_raw
            else None
        )

        tie_breaker_raw = self._first_import_value(
            import_config,
            "cdcTieBreakerColumn",
            "cdc_tie_breaker_column",
        )
        tie_breaker = (
            sanitize_simple_identifier(tie_breaker_raw, field_name="cdcTieBreakerColumn")
            if tie_breaker_raw
            else None
        )

        if not cdc_query:
            return await self.incremental_extract(
                config=config,
                secrets=secrets,
                import_config=import_config,
                sync_state=sync_state,
            )

        token = sync_state.get("cdc_token")
        tie_token = sync_state.get("cdc_tiebreaker")
        if token_column:
            query, params = build_windowed_extract_query(
                base_query=cdc_query,
                token_column=token_column,
                tie_breaker=tie_breaker,
                token=token,
                tie_token=tie_token,
                alias="cdc",
                placeholder_style=self.window_placeholder_style,
                include_as=self.window_include_as,
            )
        else:
            query, params = cdc_query, []

        result = await self._fetch(
            query=query,
            config=config,
            secrets=secrets,
            params=params if token is not None and token_column else None,
        )
        if result.rows and token_column:
            last_row = result.rows[-1]
            result = ConnectorExtractResult(
                columns=result.columns,
                rows=result.rows,
                next_state=self._next_state_from_last_row(
                    last_row=last_row,
                    token_column=token_column,
                    tie_breaker=tie_breaker,
                    token_state_key="cdc_token",
                    tie_state_key="cdc_tiebreaker",
                    cdc_strategy=cdc_strategy,
                ),
            )
        return result

    async def peek_change_token(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Dict[str, Any] | None = None,
    ) -> str | None:
        cfg = import_config or {}
        token_value = await self._peek_token_query_value(
            config=config,
            secrets=secrets,
            import_config=cfg,
        )
        if token_value is not None:
            return token_value

        strategy_value = await self._peek_strategy_token(
            config=config,
            secrets=secrets,
            import_config=cfg,
        )
        if strategy_value is not None:
            return strategy_value

        return await self._peek_max_table_token(
            config=config,
            secrets=secrets,
            import_config=cfg,
        )
