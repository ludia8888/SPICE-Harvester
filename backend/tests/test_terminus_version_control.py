"""
TerminusDB Version Control integration tests.

These tests validate the TerminusDB v12-compatible branch APIs used by OMS:
- Branch listing via `/api/document/{account}/{db}/local/_commits?type=Branch`
- Branch create/delete via `/api/branch/{account}/{db}/local/branch/{branch}`

Why this exists:
OMS' branch router depends on these primitives. Validating them here keeps the
failure mode obvious even when OMS is deployed elsewhere (e.g., via SSH tunnel).
"""

from __future__ import annotations

import uuid

import pytest

from oms.services.terminus.database import DatabaseService
from oms.services.terminus.version_control import VersionControlService


@pytest.mark.integration
@pytest.mark.asyncio
async def test_terminus_branch_lifecycle_v12():
    db_name = f"test_vc_{uuid.uuid4().hex[:10]}"
    branch_name = f"feature/{uuid.uuid4().hex[:8]}"

    db_service = DatabaseService()
    vc_service = VersionControlService()

    try:
        await db_service.create_database(db_name, description="version-control integration test")

        branches = await vc_service.list_branches(db_name)
        names = {b.get("name") for b in branches if isinstance(b, dict)}
        assert "main" in names

        await vc_service.create_branch(db_name, branch_name, source_branch="main")

        branches = await vc_service.list_branches(db_name)
        names = {b.get("name") for b in branches if isinstance(b, dict)}
        assert branch_name in names

        # Commit history (log) should be readable for main.
        commits = await vc_service.get_commits(db_name, branch_name="main", limit=10, offset=0)
        assert isinstance(commits, list)
        assert commits, "Expected at least the initial commit in a fresh database"

        # Diff should be callable even when comparing the same ref.
        diff = await vc_service.diff(db_name, "main", "main")
        assert diff is not None

        assert await vc_service.delete_branch(db_name, branch_name) is True

        branches = await vc_service.list_branches(db_name)
        names = {b.get("name") for b in branches if isinstance(b, dict)}
        assert branch_name not in names
    finally:
        # Best-effort cleanup: delete branch (if still present) and remove the database.
        try:
            await vc_service.delete_branch(db_name, branch_name)
        except Exception:
            pass
        try:
            await db_service.delete_database(db_name)
        except Exception:
            pass
        await vc_service.disconnect()
        await db_service.disconnect()
