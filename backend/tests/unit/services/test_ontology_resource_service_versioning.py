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


@pytest.mark.unit
def test_normalize_branch_for_write_strips_branch_prefix() -> None:
    service = OntologyResourceService(postgres_url="postgresql://localhost/test")

    assert service._normalize_branch_for_write("branch:main") == "main"  # noqa: SLF001
    assert service._normalize_branch_for_write("main") == "main"  # noqa: SLF001
    assert service._normalize_branch_for_write("   ") == "master"  # noqa: SLF001


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_resource_falls_back_to_deployed_target_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    service = OntologyResourceService(postgres_url="postgresql://localhost/test")
    looked_up: list[str] = []

    async def _fake_ensure() -> None:
        return None

    async def _fake_resolve_target(*, db_name: str, ontology_commit_id: str) -> str | None:
        _ = db_name
        if ontology_commit_id == "commit-1":
            return "main"
        return None

    async def _fake_get(
        *,
        db_name: str,  # noqa: ARG001
        branch: str,
        resource_type: str,  # noqa: ARG001
        resource_id: str,  # noqa: ARG001
    ) -> dict | None:
        looked_up.append(branch)
        if branch != "main":
            return None
        return {
            "db_name": "demo",
            "branch": "main",
            "resource_type": "action_type",
            "resource_id": "ApproveTicket",
            "label": "Approve Ticket",
            "description": None,
            "label_i18n": None,
            "description_i18n": None,
            "spec": {},
            "metadata": {},
            "version": 1,
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T00:00:00+00:00",
        }

    monkeypatch.setattr(service, "_ensure_postgres_schema", _fake_ensure)
    monkeypatch.setattr(service, "_resolve_deployed_target_branch", _fake_resolve_target)
    monkeypatch.setattr(service, "_get_resource_document_postgres", _fake_get)

    resolved = await service.get_resource(
        "demo",
        branch="commit-1",
        resource_type="action_type",
        resource_id="ApproveTicket",
    )

    assert resolved is not None
    assert resolved.get("id") == "ApproveTicket"
    assert looked_up == ["commit-1", "main"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_materialize_commit_snapshot_delegates_to_promote(monkeypatch: pytest.MonkeyPatch) -> None:
    service = OntologyResourceService(postgres_url="postgresql://localhost/test")
    captured: dict[str, str] = {}

    async def _fake_promote(
        db_name: str,
        *,
        source_branch: str,
        target_branch: str,
    ) -> int:
        captured["db_name"] = db_name
        captured["source_branch"] = source_branch
        captured["target_branch"] = target_branch
        return 3

    monkeypatch.setattr(service, "promote_branch_resources", _fake_promote)
    copied = await service.materialize_commit_snapshot(
        "demo",
        source_branch="main",
        ontology_commit_id="commit-xyz",
    )

    assert copied == 3
    assert captured == {
        "db_name": "demo",
        "source_branch": "main",
        "target_branch": "commit-xyz",
    }
