from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence

from data_connector.adapters.base import ConnectorExtractResult
from data_connector.adapters.blocking_query import run_blocking_query
from data_connector.adapters.sql_connector_common import mapping_rows_to_dict_rows, safe_columns


def _cursor_result_to_rows(
    *,
    cursor: Any,
    fetched: Iterable[Any],
) -> tuple[list[str], list[Dict[str, Any]]]:
    fetched_list = list(fetched or [])
    if not fetched_list:
        return [], []
    first = fetched_list[0]
    if isinstance(first, Mapping):
        rows = mapping_rows_to_dict_rows(
            row for row in fetched_list if isinstance(row, Mapping)
        )
        return safe_columns(rows), rows
    columns = [str(desc[0]) for desc in getattr(cursor, "description", None) or []]
    rows: list[Dict[str, Any]] = []
    for row in fetched_list:
        if isinstance(row, Mapping):
            rows.append(dict(row))
            continue
        rows.append({columns[i]: row[i] for i in range(len(columns))})
    return columns, rows


async def run_dbapi_connection_test(
    *,
    adapter_name: str,
    connect: Callable[[], Any],
    probe_sql: str,
) -> None:
    def _run() -> None:
        conn = connect()
        try:
            cur = conn.cursor()
            try:
                cur.execute(probe_sql)
                cur.fetchall()
            finally:
                cur.close()
        finally:
            conn.close()

    await run_blocking_query(_run, adapter_name=adapter_name, operation="connection test")


async def fetch_dbapi_rows(
    *,
    adapter_name: str,
    connect: Callable[[], Any],
    query: str,
    params: Optional[Sequence[Any]] = None,
) -> ConnectorExtractResult:
    def _run() -> ConnectorExtractResult:
        conn = connect()
        try:
            cur = conn.cursor()
            try:
                cur.execute(query, params or [])
                columns, rows = _cursor_result_to_rows(
                    cursor=cur,
                    fetched=cur.fetchall() or [],
                )
                return ConnectorExtractResult(columns=columns, rows=rows, next_state={})
            finally:
                cur.close()
        finally:
            conn.close()

    return await run_blocking_query(_run, adapter_name=adapter_name, operation="query fetch")
