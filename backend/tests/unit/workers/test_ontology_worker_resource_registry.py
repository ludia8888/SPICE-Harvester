from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

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

    async def update_resource(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: str,
        resource_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        key = (db_name, branch, resource_type, resource_id)
        if key not in self._rows:
            raise RuntimeError("not found")
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


def _build_worker_with_resource_store(
    service: _InMemoryOntologyResourceService,
) -> tuple[OntologyWorker, _ObservabilityStub, _AuditStoreStub]:
    worker = OntologyWorker()
    worker.ontology_resource_backend = "postgres"
    worker.key_spec_registry = None
    worker.command_status_service = None
    worker.publish_event = AsyncMock(return_value=None)
    worker._ontology_resource_service = lambda: service  # type: ignore[method-assign]
    obs = _ObservabilityStub()
    audit = _AuditStoreStub()
    worker.observability = obs
    worker.audit_store = audit
    return worker, obs, audit


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_ontology_uses_object_type_resource_registry_in_postgres_profile() -> None:
    store = _InMemoryOntologyResourceService()
    worker, obs, audit = _build_worker_with_resource_store(store)

    await worker.handle_create_ontology(
        {
            "command_id": "00000000-0000-0000-0000-000000000301",
            "created_by": "tester",
            "payload": {
                "db_name": "demo",
                "branch": "main",
                "class_id": "Ticket",
                "label": "Ticket",
                "description": "Ticket class",
                "properties": [{"name": "title", "type": "string"}],
                "relationships": [],
                "metadata": {"source": "test"},
            },
        }
    )

    stored = await store.get_resource(
        "demo",
        branch="main",
        resource_type="object_type",
        resource_id="Ticket",
    )
    assert stored is not None
    assert stored.get("id") == "Ticket"
    assert stored.get("resource_type") == "object_type"
    assert isinstance(stored.get("spec"), dict)
    assert obs.links
    assert audit.rows


@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_ontology_uses_object_type_resource_registry_in_postgres_profile() -> None:
    store = _InMemoryOntologyResourceService()
    worker, _, _ = _build_worker_with_resource_store(store)

    await store.create_resource(
        "demo",
        branch="main",
        resource_type="object_type",
        resource_id="Ticket",
        payload={
            "id": "Ticket",
            "label": "Ticket",
            "description": "old",
            "metadata": {"source": "seed"},
            "spec": {
                "id": "Ticket",
                "label": "Ticket",
                "description": "old",
                "parent_class": None,
                "abstract": False,
                "properties": [],
                "relationships": [],
            },
        },
    )

    await worker.handle_update_ontology(
        {
            "command_id": "00000000-0000-0000-0000-000000000302",
            "created_by": "tester",
            "payload": {
                "db_name": "demo",
                "branch": "main",
                "class_id": "Ticket",
                "updates": {"description": "new"},
            },
        }
    )

    updated = await store.get_resource(
        "demo",
        branch="main",
        resource_type="object_type",
        resource_id="Ticket",
    )
    assert updated is not None
    assert updated.get("description") == "new"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_delete_ontology_uses_object_type_resource_registry_in_postgres_profile() -> None:
    store = _InMemoryOntologyResourceService()
    worker, _, _ = _build_worker_with_resource_store(store)

    await store.create_resource(
        "demo",
        branch="main",
        resource_type="object_type",
        resource_id="Ticket",
        payload={
            "id": "Ticket",
            "label": "Ticket",
            "description": "to-delete",
            "metadata": {},
            "spec": {
                "id": "Ticket",
                "label": "Ticket",
                "description": "to-delete",
                "parent_class": None,
                "abstract": False,
                "properties": [],
                "relationships": [],
            },
        },
    )

    await worker.handle_delete_ontology(
        {
            "command_id": "00000000-0000-0000-0000-000000000303",
            "created_by": "tester",
            "payload": {
                "db_name": "demo",
                "branch": "main",
                "class_id": "Ticket",
            },
        }
    )

    deleted = await store.get_resource(
        "demo",
        branch="main",
        resource_type="object_type",
        resource_id="Ticket",
    )
    assert deleted is None
