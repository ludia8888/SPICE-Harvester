"""
Helpers for Funnel schema-related normalization.
"""

from __future__ import annotations


def normalize_property_name(column_name: str) -> str:
    """Normalize raw column names into schema-safe property identifiers."""
    normalized = column_name.replace(" ", "_").replace("-", "_")
    normalized = "".join(ch for ch in normalized if ch.isalnum() or ch == "_")
    normalized = normalized.lower()
    if normalized and normalized[0].isdigit():
        normalized = "col_" + normalized
    if not normalized:
        normalized = "column"
    return normalized
