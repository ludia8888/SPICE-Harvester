from __future__ import annotations

import pytest

import oms.routers.ontology_extensions as ontology_extensions
from oms.routers.ontology_extensions import _compute_ontology_health


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_ontology_health_supports_resource_only_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeResourceService:
        def __init__(self) -> None:
            return None

        async def list_resources(self, db_name: str, *, branch: str):  # noqa: ARG002
            return [
                {
                    "resource_type": "object_type",
                    "id": "Ticket",
                    "spec": {},
                    "metadata": {},
                }
            ]

    monkeypatch.setattr(ontology_extensions, "OntologyResourceService", _FakeResourceService)

    data = await _compute_ontology_health(
        db_name="demo",
        branch="main",
    )
    assert data["summary"]["schema_checks_skipped"] is True
    assert data["summary"]["resource_issues"] >= 1
