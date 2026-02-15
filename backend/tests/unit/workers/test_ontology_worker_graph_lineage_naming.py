from __future__ import annotations

from typing import Any

import pytest
from unittest.mock import AsyncMock

from ontology_worker.main import OntologyWorker


class _InMemoryOntologyResourceService:
    def __init__(self) -> None:
        self._rows: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    async def create_resource(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: str,
        resource_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        key = (db_name, branch, resource_type, resource_id)
        if key in self._rows:
            raise RuntimeError("already exists")
        row = dict(payload)
        row.setdefault("id", resource_id)
        row["resource_type"] = resource_type
        self._rows[key] = row
        return dict(row)

    async def delete_resource(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: str,
        resource_id: str,
    ) -> None:
        self._rows.pop((db_name, branch, resource_type, resource_id), None)

    async def get_resource(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: str,
        resource_id: str,
    ) -> dict[str, Any] | None:
        row = self._rows.get((db_name, branch, resource_type, resource_id))
        return dict(row) if row else None


class _ObservabilityStub:
    def __init__(self) -> None:
        self.links: list[dict[str, Any]] = []

    async def record_link(self, **kwargs: Any) -> None:
        self.links.append(kwargs)


class _AuditStoreStub:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    async def log(self, **kwargs: Any) -> None:
        self.rows.append(kwargs)


def _build_worker() -> tuple[OntologyWorker, _InMemoryOntologyResourceService, _ObservabilityStub, _AuditStoreStub]:
    worker = OntologyWorker()
    worker.ontology_resource_backend = "postgres"
    worker.key_spec_registry = None
    worker.command_status_service = None
    worker.publish_event = AsyncMock(return_value=None)
    store = _InMemoryOntologyResourceService()
    worker._ontology_resource_service = lambda: store  # type: ignore[method-assign]
    obs = _ObservabilityStub()
    audit = _AuditStoreStub()
    worker.observability = obs
    worker.audit_store = audit
    return worker, store, obs, audit


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_ontology_records_graph_lineage_and_audit_naming() -> None:
    worker, _, obs, audit = _build_worker()

    await worker.handle_create_ontology(
        {
            "command_id": "00000000-0000-0000-0000-000000000111",
            "created_by": "tester",
            "payload": {
                "db_name": "demo",
                "branch": "main",
                "class_id": "Ticket",
                "label": "Ticket",
                "description": "Ticket class",
                "properties": [],
                "relationships": [],
            },
        }
    )

    assert obs.links
    link = obs.links[-1]
    assert str(link.get("to_node_id", "")).startswith("artifact:graph:")
    assert link.get("edge_type") == "event_wrote_graph_document"
    assert str(link.get("to_label", "")).startswith("graph:")
    assert "terminus" not in str(link).lower()

    assert audit.rows
    row = audit.rows[-1]
    assert row.get("action") == "ONTOLOGY_GRAPH_WRITE"
    assert row.get("resource_type") == "graph_schema"
    assert str(row.get("resource_id", "")).startswith("graph:")
    assert "terminus" not in str(row).lower()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_delete_ontology_records_graph_lineage_and_audit_naming() -> None:
    worker, store, obs, audit = _build_worker()

    await store.create_resource(
        "demo",
        branch="main",
        resource_type="object_type",
        resource_id="Ticket",
        payload={
            "id": "Ticket",
            "label": "Ticket",
            "description": "Ticket class",
            "metadata": {},
            "spec": {
                "id": "Ticket",
                "label": "Ticket",
                "description": "Ticket class",
                "parent_class": None,
                "abstract": False,
                "properties": [],
                "relationships": [],
            },
        },
    )

    await worker.handle_delete_ontology(
        {
            "command_id": "00000000-0000-0000-0000-000000000222",
            "created_by": "tester",
            "payload": {
                "db_name": "demo",
                "branch": "main",
                "class_id": "Ticket",
            },
        }
    )

    assert obs.links
    link = obs.links[-1]
    assert str(link.get("to_node_id", "")).startswith("artifact:graph:")
    assert link.get("edge_type") == "event_deleted_graph_document"
    assert str(link.get("to_label", "")).startswith("graph:")
    assert "terminus" not in str(link).lower()

    assert audit.rows
    row = audit.rows[-1]
    assert row.get("action") == "ONTOLOGY_GRAPH_DELETE"
    assert row.get("resource_type") == "graph_schema"
    assert str(row.get("resource_id", "")).startswith("graph:")
    assert "terminus" not in str(row).lower()
