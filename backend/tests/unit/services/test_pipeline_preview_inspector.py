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


@pytest.mark.unit
def test_preview_inspector_suggests_dedupe_for_duplicate_rows() -> None:
    columns = [{"name": "id", "type": "xsd:integer"}, {"name": "value", "type": "xsd:string"}]
    rows = [{"id": idx + 1, "value": f"v{idx + 1}"} for idx in range(9)]
    rows.append({"id": 1, "value": "v1"})
    preview = {
        "columns": columns,
        "rows": rows,
        "column_stats": compute_column_stats(rows=rows, columns=columns),
    }

    inspector = inspect_preview(preview)
    suggestions = inspector.get("suggestions") or []

    dedupe = next((item for item in suggestions if item.get("operation") == "dedupe"), None)
    assert dedupe is not None
    assert "id" in (dedupe.get("columns") or [])


@pytest.mark.unit
def test_preview_inspector_suggests_regex_replace_for_phone() -> None:
    columns = [{"name": "phone", "type": "xsd:string"}]
    rows = [{"phone": "(123) 456-7890"}, {"phone": "123 456 7890"}]
    preview = {
        "columns": columns,
        "rows": rows,
        "column_stats": compute_column_stats(rows=rows, columns=columns),
    }

    inspector = inspect_preview(preview)
    suggestions = inspector.get("suggestions") or []

    assert any(
        item.get("operation") == "regexReplace"
        and item.get("column") == "phone"
        for item in suggestions
    )
