from __future__ import annotations


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

