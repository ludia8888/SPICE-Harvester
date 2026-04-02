from __future__ import annotations

import pytest

from oms.routers import ontology_extensions


class _FakeOntologyResourceService:
    def __init__(self) -> None:
        self.rows = [{"id": f"Type{i}"} for i in range(2505)]

    async def list_resources(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ):
        _ = db_name, branch, resource_type
        return self.rows[offset : offset + limit]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_list_resources_reports_total_matches_not_page_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ontology_extensions, "OntologyResourceService", _FakeOntologyResourceService)

    payload = await ontology_extensions.list_resources(
        db_name="demo",
        branch="main",
        resource_type="object_type",
        limit=2,
        offset=1,
    )

    assert payload["data"]["total"] == 2505
    assert len(payload["data"]["resources"]) == 2
