from bff.routers.foundry_ontology_v2 import (
    _dedupe_rows_by_identity,
    _extract_ontology_resource,
    _extract_ontology_resource_rows,
    _resolve_source_primary_key_from_row,
)


def test_resolve_source_primary_key_from_row_strips_typed_refs() -> None:
    row = {
        "__primaryKey": "Order/order-1",
        "properties": {"order_id": "Order/order-ignored"},
    }

    assert _resolve_source_primary_key_from_row(row, primary_key_field="properties.order_id") == "order-1"


def test_dedupe_rows_by_identity_normalizes_typed_refs() -> None:
    rows = [
        {"__apiName": "Order", "__primaryKey": "Order/order-1", "value": 1},
        {"__apiName": "Order", "__primaryKey": "order-1", "value": 2},
        {"__apiName": "Order", "__primaryKey": "Order/order-2", "value": 3},
    ]

    deduped = _dedupe_rows_by_identity(rows)

    assert deduped == [
        {"__apiName": "Order", "__primaryKey": "Order/order-1", "value": 1},
        {"__apiName": "Order", "__primaryKey": "Order/order-2", "value": 3},
    ]


def test_extract_ontology_resource_helpers_unwrap_payload_data() -> None:
    payload = {
        "data": {
            "id": "Order",
            "resources": [
                {"id": "Order"},
                "ignored",
            ],
        }
    }

    assert _extract_ontology_resource(payload) == {
        "id": "Order",
        "resources": [
            {"id": "Order"},
            "ignored",
        ],
    }
    assert _extract_ontology_resource_rows(payload) == [{"id": "Order"}]
