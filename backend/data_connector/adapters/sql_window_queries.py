from __future__ import annotations

from typing import Any, List, Sequence, Tuple

from data_connector.adapters.sql_query_guard import build_ordered_wrapper_query


def _comparison_placeholders(
    *,
    placeholder_style: str,
    tie_breaker_enabled: bool,
) -> tuple[str, str, List[int]]:
    if placeholder_style == "oracle":
        if tie_breaker_enabled:
            return ":1", ":2", [0, 1]
        return ":1", "", [0]
    if placeholder_style == "qmark":
        if tie_breaker_enabled:
            return "?", "?", [0, 0, 1]
        return "?", "", [0]
    if tie_breaker_enabled:
        return "%s", "%s", [0, 0, 1]
    return "%s", "", [0]


def build_windowed_extract_query(
    *,
    base_query: str,
    token_column: str,
    tie_breaker: str | None,
    token: Any,
    tie_token: Any,
    alias: str,
    placeholder_style: str,
    include_as: bool = True,
) -> Tuple[str, List[Any]]:
    order_columns = [token_column]
    if tie_breaker:
        order_columns.append(tie_breaker)

    if token is None:
        return (
            build_ordered_wrapper_query(
                base_query,
                order_columns=order_columns,
                alias=alias,
                include_as=include_as,
            ),
            [],
        )

    tie_breaker_enabled = bool(tie_breaker and tie_token is not None)
    left_placeholder, right_placeholder, param_indexes = _comparison_placeholders(
        placeholder_style=placeholder_style,
        tie_breaker_enabled=tie_breaker_enabled,
    )
    if tie_breaker_enabled:
        predicate = (
            f"({token_column} > {left_placeholder} "
            f"OR ({token_column} = {left_placeholder} AND {tie_breaker} > {right_placeholder}))"
        )
        order_clause = f"{token_column} ASC, {tie_breaker} ASC"
        values: Sequence[Any] = [token, tie_token]
    else:
        predicate = f"{token_column} > {left_placeholder}"
        order_clause = f"{token_column} ASC"
        values = [token]

    params = [values[index] for index in param_indexes]
    alias_sql = f"AS {alias}" if include_as else alias
    return (
        f"SELECT * FROM ({base_query}) {alias_sql} WHERE {predicate} ORDER BY {order_clause}",
        params,
    )
