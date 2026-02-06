import pytest

from shared.services.storage.lakefs_branch_utils import ensure_lakefs_branch
from shared.services.storage.lakefs_client import LakeFSConflictError


class _FakeLakeFSClient:
    def __init__(self, should_conflict: bool = False) -> None:
        self.should_conflict = should_conflict
        self.calls: list[tuple[str, str, str]] = []

    async def create_branch(self, *, repository: str, name: str, source: str = "main") -> None:
        self.calls.append((repository, name, source))
        if self.should_conflict:
            raise LakeFSConflictError("already exists")


@pytest.mark.asyncio
async def test_ensure_lakefs_branch_creates_branch() -> None:
    client = _FakeLakeFSClient()
    await ensure_lakefs_branch(lakefs_client=client, repository="repo", branch="feature", source="main")
    assert client.calls == [("repo", "feature", "main")]


@pytest.mark.asyncio
async def test_ensure_lakefs_branch_ignores_conflict() -> None:
    client = _FakeLakeFSClient(should_conflict=True)
    await ensure_lakefs_branch(lakefs_client=client, repository="repo", branch="feature", source="main")
    assert client.calls == [("repo", "feature", "main")]


@pytest.mark.asyncio
async def test_ensure_lakefs_branch_requires_client() -> None:
    try:
        await ensure_lakefs_branch(lakefs_client=None, repository="repo", branch="feature", source="main")
    except RuntimeError as exc:
        assert "lakefs_client not initialized" in str(exc)
    else:
        raise AssertionError("RuntimeError expected when lakefs_client is missing")
