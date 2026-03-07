from __future__ import annotations

import pytest

from bff.services.ai_service import _load_schema_context


class _FakeOMSClient:
    async def list_ontologies(self, db_name: str):
        assert db_name == "demo"
        return {
            "data": {
                "ontologies": [
                    {
                        "id": "Order",
                        "label": "Order",
                        "properties": [
                            {"id": "order_id", "label": "Order ID", "type": "xsd:string"},
                            {"name": "display_name", "label": "Display Name", "type": "xsd:string"},
                        ],
                        "relationships": [
                            {"name": "managed_by", "label": "Managed By", "target": "User"},
                        ],
                    }
                ]
            }
        }


@pytest.mark.asyncio
async def test_load_schema_context_uses_id_only_properties_and_name_only_relationships() -> None:
    context = await _load_schema_context(
        db_name="demo",
        oms=_FakeOMSClient(),
        redis_service=None,
    )

    assert context["class_count"] == 1
    assert context["classes"] == [
        {
            "id": "Order",
            "label": "Order",
            "properties": [
                {"name": "order_id", "label": "Order ID", "type": "xsd:string"},
                {"name": "display_name", "label": "Display Name", "type": "xsd:string"},
            ],
            "relationships": [
                {"predicate": "managed_by", "label": "Managed By", "target": "User"},
            ],
        }
    ]
    assert context["relationship_edges"] == [
        {"from": "Order", "predicate": "managed_by", "label": "Managed By", "to": "User"}
    ]
