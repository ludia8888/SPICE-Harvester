from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from bff.routers.document_bundles import DocumentBundleSearchRequest, search_document_bundle
from shared.security.user_context import UserPrincipal


class _Request:
    def __init__(self, *, principal: UserPrincipal) -> None:
        self.state = SimpleNamespace(user=principal)


class _FakePolicyRegistry:
    def __init__(self, *, allowed_bundle_ids: list[str] | None = None) -> None:
        self._allowed = allowed_bundle_ids

    async def get_tenant_policy(self, *, tenant_id: str):  # noqa: ANN001
        _ = tenant_id
        if self._allowed is None:
            return None
        return SimpleNamespace(data_policies={"allowed_document_bundle_ids": list(self._allowed)})


class _FakeContext7:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def search(self, *, query: str, limit: int, filters=None):  # noqa: ANN001
        self.calls.append({"query": query, "limit": limit, "filters": dict(filters or {})})
        return [
            {"id": "doc-1", "title": "Hello", "score": 0.9, "snippet": "World"},
            {"id": "doc-2", "content": "x" * 2000},
        ]


def _principal(*, tenant_id: str = "tenant-1") -> UserPrincipal:
    return UserPrincipal(id="user-1", tenant_id=tenant_id, verified=True)


@pytest.mark.asyncio
async def test_document_bundle_search_enforces_allowed_bundle_ids() -> None:
    req = _Request(principal=_principal())
    policy = _FakePolicyRegistry(allowed_bundle_ids=["allowed"])
    client = _FakeContext7()

    with pytest.raises(HTTPException) as exc:
        await search_document_bundle(
            bundle_id="blocked",
            body=DocumentBundleSearchRequest(query="hi"),
            request=req,  # type: ignore[arg-type]
            policy_registry=policy,  # type: ignore[arg-type]
            client=client,  # type: ignore[arg-type]
        )
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_document_bundle_search_returns_citations() -> None:
    req = _Request(principal=_principal())
    policy = _FakePolicyRegistry(allowed_bundle_ids=["docs"])
    client = _FakeContext7()

    resp = await search_document_bundle(
        bundle_id="docs",
        body=DocumentBundleSearchRequest(query="hello", limit=2),
        request=req,  # type: ignore[arg-type]
        policy_registry=policy,  # type: ignore[arg-type]
        client=client,  # type: ignore[arg-type]
    )
    assert resp.status == "success"
    assert resp.data and resp.data["count"] == 2
    assert resp.data["citations"]
    assert resp.data["results"][0]["citation_id"].startswith("context7:docs:")
    assert resp.data["results"][1]["snippet"].endswith("…")

