from __future__ import annotations

from typing import Any, Dict

import pytest

from oms.routers import ontology_extensions as router


class _FakePRService:
    def __init__(self, pr_data: Dict[str, Any]) -> None:
        self._pr_data = pr_data

    async def get_pull_request(self, proposal_id: str) -> Dict[str, Any]:
        _ = proposal_id
        return dict(self._pr_data)

    async def merge_pull_request(self, *, pr_id: str, merge_message: str | None, author: str) -> Dict[str, Any]:
        _ = (pr_id, merge_message, author)
        return {"id": "p1", "status": "merged"}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_approve_proposal_promotes_ontology_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, str] = {}

    class _FakeResourceService:
        async def promote_branch_resources(
            self,
            db_name: str,
            *,
            source_branch: str,
            target_branch: str,
        ) -> int:
            captured["db_name"] = db_name
            captured["source_branch"] = source_branch
            captured["target_branch"] = target_branch
            return 2

    monkeypatch.setattr(router, "_require_health_gate", lambda _branch: False)
    monkeypatch.setattr(router, "OntologyResourceService", lambda *_args, **_kwargs: _FakeResourceService())

    pr_service = _FakePRService(
        {
            "db_name": "demo",
            "source_branch": "feature/actions",
            "target_branch": "main",
            "source_commit_id": "branch:feature/actions",
        }
    )

    response = await router.approve_ontology_proposal(
        db_name="demo",
        proposal_id="p1",
        request=router.OntologyApproveRequest(author="qa"),
        pr_service=pr_service,
    )

    data = response.get("data") if isinstance(response, dict) else None
    assert isinstance(data, dict)
    assert data.get("promoted_resource_count") == 2
    assert captured == {
        "db_name": "demo",
        "source_branch": "feature/actions",
        "target_branch": "main",
    }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_deploy_materializes_commit_snapshot_for_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeRegistry:
        async def record_deployment(self, **kwargs: Any) -> Dict[str, Any]:
            _ = kwargs
            return {"deployment_id": "d1", "status": "succeeded"}

    class _FakeResourceService:
        async def materialize_commit_snapshot(
            self,
            db_name: str,  # noqa: ARG002
            *,
            source_branch: str,  # noqa: ARG002
            ontology_commit_id: str,  # noqa: ARG002
        ) -> int:
            return 4

    async def _fake_health(**_kwargs: Any) -> Dict[str, Any]:
        return {"summary": {"issues": 0}}

    monkeypatch.setattr(router, "_compute_ontology_health", _fake_health)
    monkeypatch.setattr(router, "OntologyDeploymentRegistryV2", lambda *_args, **_kwargs: _FakeRegistry())
    monkeypatch.setattr(router, "OntologyResourceService", lambda *_args, **_kwargs: _FakeResourceService())

    pr_service = _FakePRService(
        {
            "db_name": "demo",
            "target_branch": "main",
            "status": "merged",
            "merge_commit_id": "branch:main",
            "source_branch": "feature/actions",
        }
    )

    response = await router.deploy_ontology(
        db_name="demo",
        request=router.OntologyDeployRequest(
            proposal_id="p1",
            ontology_commit_id="branch:main",
            author="qa",
        ),
        pr_service=pr_service,
    )

    data = response.get("data") if isinstance(response, dict) else None
    assert isinstance(data, dict)
    assert data.get("resource_snapshot_count") == 4
