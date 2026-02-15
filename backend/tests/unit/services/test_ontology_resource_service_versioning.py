from __future__ import annotations

import pytest

from oms.services.ontology_resources import OntologyResourceService


@pytest.mark.unit
def test_payload_to_document_sets_initial_version_and_rev() -> None:
    service = OntologyResourceService(postgres_url="postgresql://localhost/test")
    doc = service._payload_to_document(  # noqa: SLF001
        "object_type",
        "Ticket",
        {"label": "Ticket", "spec": {"properties": []}},
        doc_id="__ontology_resource/object_type:Ticket",
        is_create=True,
    )

    assert doc["version"] == 1
    assert doc["metadata"]["rev"] == 1


@pytest.mark.unit
def test_payload_to_document_increments_version_and_rev_from_existing() -> None:
    service = OntologyResourceService(postgres_url="postgresql://localhost/test")
    existing = {
        "resource_type": "object_type",
        "resource_id": "Ticket",
        "label": "Ticket",
        "metadata": {"rev": 3, "owner": "ops"},
        "version": 3,
        "created_at": "2026-01-01T00:00:00+00:00",
    }

    doc = service._payload_to_document(  # noqa: SLF001
        "object_type",
        "Ticket",
        {"metadata": {"owner": "platform"}},
        doc_id="__ontology_resource/object_type:Ticket",
        is_create=False,
        existing=existing,
    )

    assert doc["version"] == 4
    assert doc["metadata"]["rev"] == 4
    assert doc["metadata"]["owner"] == "platform"
