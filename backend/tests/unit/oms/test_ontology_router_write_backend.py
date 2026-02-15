from __future__ import annotations

import pytest

from oms.routers import ontology as ontology_router


class _ResourceServiceStub:
    async def get_resource(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: str,
        resource_id: str,
    ) -> dict:
        _ = (db_name, branch, resource_type)
        return {
            "id": resource_id,
            "label": "Ticket",
            "description": "Ticket type",
            "metadata": {"source": "resource"},
            "spec": {
                "id": resource_id,
                "label": "Ticket",
                "description": "Ticket type",
                "parent_class": "Thing",
                "abstract": False,
                "properties": [{"name": "title", "type": "string"}],
                "relationships": [],
            },
        }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_load_existing_ontology_for_write_reads_from_resource_registry_without_terminus(monkeypatch) -> None:
    monkeypatch.setattr(ontology_router, "OntologyResourceService", lambda *_args, **_kwargs: _ResourceServiceStub())

    existing = await ontology_router._load_existing_ontology_for_write(
        db_name="demo",
        class_id="Ticket",
        branch="main",
    )

    assert existing is not None
    assert existing.get("id") == "Ticket"
    assert existing.get("parent_class") == "Thing"
    assert isinstance(existing.get("properties"), list)
