from __future__ import annotations

from bff.services.pipeline_intent_verifier import (
    _extract_selected_columns,
    _extract_mentioned_columns,
    _looks_like_output_column_requirement,
)


def test_output_column_requirement_detection_without_column_keyword():
    # Some models omit "column(s)" and only say "Output must include ...".
    msg = "Output must include 'order_id', 'customer_unique_id', 'order_purchase_timestamp' only."
    assert _looks_like_output_column_requirement(msg) is True


def test_output_column_requirement_detection_unquoted_comma_list():
    msg = "Output must include only order_id, customer_unique_id, order_purchase_timestamp."
    assert _looks_like_output_column_requirement(msg) is True


def test_extract_mentioned_columns_ignores_only_and_specified():
    msg = "Output must include only order_id, customer_unique_id, order_purchase_timestamp columns."
    assert _extract_mentioned_columns(msg) == ["order_id", "customer_unique_id", "order_purchase_timestamp"]


def test_extract_mentioned_columns_ignores_dataset_wording():
    msg = "The output dataset must include only the specified columns: order_id, customer_unique_id, order_purchase_timestamp."
    assert _extract_mentioned_columns(msg) == ["order_id", "customer_unique_id", "order_purchase_timestamp"]


def test_extract_mentioned_columns_bare_identifier():
    # Some LLM responses return the missing column name directly.
    assert _extract_mentioned_columns("customer_city") == ["customer_city"]


def test_extract_mentioned_columns_missing_message():
    msg = "customer_city column is missing from the output"
    assert _extract_mentioned_columns(msg) == ["customer_city"]


def test_extract_selected_columns_prefers_output_projection():
    # If multiple select nodes exist, prefer the select that feeds an output node.
    definition_json = {
        "nodes": [
            {
                "id": "select_unused",
                "type": "transform",
                "metadata": {"operation": "select", "columns": ["foo"]},
            },
            {
                "id": "select_final",
                "type": "transform",
                "metadata": {"operation": "select", "columns": ["order_id", "customer_unique_id"]},
            },
            {"id": "output_1", "type": "output", "metadata": {"outputName": "out"}},
        ],
        "edges": [
            {"from": "select_final", "to": "output_1"},
        ],
    }

    assert _extract_selected_columns(definition_json) == ["order_id", "customer_unique_id"]
