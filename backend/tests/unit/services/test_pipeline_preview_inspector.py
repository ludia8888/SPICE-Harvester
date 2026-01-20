from __future__ import annotations

import pytest

from shared.services.pipeline_preview_inspector import inspect_preview
from shared.services.pipeline_profiler import compute_column_stats


@pytest.mark.unit
def test_preview_inspector_suggests_normalize_and_casts() -> None:
    columns = [
        {"name": "name", "type": "xsd:string"},
        {"name": "amount", "type": "xsd:string"},
        {"name": "flag", "type": "xsd:string"},
    ]
    rows = [
        {"name": " Alice ", "amount": "1", "flag": "true"},
        {"name": "", "amount": "2", "flag": "false"},
        {"name": "   ", "amount": "3", "flag": "true"},
    ]
    preview = {
        "columns": columns,
        "rows": rows,
        "column_stats": compute_column_stats(rows=rows, columns=columns),
    }

    inspector = inspect_preview(preview)
    suggestions = inspector.get("suggestions") or []

    assert inspector.get("needs_cleansing") is True
    assert any(item.get("operation") == "normalize" and item.get("column") == "name" for item in suggestions)
    assert any(
        item.get("operation") == "cast" and item.get("column") == "amount" and item.get("target_type") == "xsd:integer"
        for item in suggestions
    )
    assert any(
        item.get("operation") == "cast" and item.get("column") == "flag" and item.get("target_type") == "xsd:boolean"
        for item in suggestions
    )


@pytest.mark.unit
def test_preview_inspector_no_cleansing_needed_for_clean_columns() -> None:
    columns = [{"name": "id", "type": "xsd:integer"}]
    rows = [{"id": 1}, {"id": 2}, {"id": 3}]
    preview = {
        "columns": columns,
        "rows": rows,
        "column_stats": compute_column_stats(rows=rows, columns=columns),
    }

    inspector = inspect_preview(preview)
    assert inspector.get("needs_cleansing") is False
    assert inspector.get("suggestions") == []
