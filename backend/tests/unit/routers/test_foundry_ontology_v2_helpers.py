from __future__ import annotations

import pytest
from starlette.requests import Request

from bff.routers import foundry_ontology_v2 as router_v2
from bff.routers.foundry_ontology_v2 import (
    _dedupe_rows_by_identity,
    _extract_ontology_resource,
    _extract_ontology_resource_rows,
    _resolve_source_primary_key_from_row,
)
from bff.routers.foundry_ontology_v2_errors import _preflight_error_response
from shared.security.database_access import DatabaseAccessRegistryUnavailableError


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


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [(b"x-user-id", b"alice")],
        }
    )


@pytest.mark.asyncio
async def test_foundry_domain_role_read_allows_registry_degrade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def _fake_enforce(**kwargs):  # noqa: ANN003
        captured.update(kwargs)

    monkeypatch.setattr(router_v2, "enforce_database_role", _fake_enforce)

    await router_v2._require_domain_role(_request(), db_name="demo")

    assert captured["allow_if_registry_unavailable"] is True


@pytest.mark.asyncio
async def test_foundry_domain_role_write_fails_closed_on_registry_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def _fake_enforce(**kwargs):  # noqa: ANN003
        captured.update(kwargs)
        raise DatabaseAccessRegistryUnavailableError("Database access registry unavailable")

    monkeypatch.setattr(router_v2, "enforce_database_role", _fake_enforce)

    with pytest.raises(DatabaseAccessRegistryUnavailableError):
        await router_v2._require_domain_write_role(_request(), db_name="demo")

    assert captured["allow_if_registry_unavailable"] is False


def test_foundry_preflight_maps_registry_unavailable_to_503() -> None:
    response = _preflight_error_response(
        DatabaseAccessRegistryUnavailableError("Database access registry unavailable"),
        ontology="demo",
    )

    assert response.status_code == 503
