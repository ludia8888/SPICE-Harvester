from __future__ import annotations

from typing import Sequence


def normalize_sql_query(query: str, *, field_name: str = "query") -> str:
    """Normalize and validate externally provided SQL query text.

    - Trims whitespace
    - Allows a single trailing ';'
    - Rejects multiple statements separated by ';'
    """
    text = str(query or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required")

    normalized = text
    while normalized.endswith(";"):
        normalized = normalized[:-1].rstrip()

    if ";" in normalized:
        raise ValueError(f"{field_name} must contain exactly one SQL statement")
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def build_ordered_wrapper_query(
    query: str,
    *,
    order_columns: Sequence[str],
    alias: str = "base",
    include_as: bool = True,
) -> str:
    normalized_columns = [str(column or "").strip() for column in order_columns if str(column or "").strip()]
    if not normalized_columns:
        raise ValueError("order_columns is required")
    order_clause = ", ".join(f"{column} ASC" for column in normalized_columns)
    alias_clause = f"AS {alias}" if include_as else alias
    return f"SELECT * FROM ({query}) {alias_clause} ORDER BY {order_clause}"
