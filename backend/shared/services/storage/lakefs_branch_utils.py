from __future__ import annotations

from typing import Optional

from shared.services.storage.lakefs_client import LakeFSClient, LakeFSConflictError


async def ensure_lakefs_branch(
    *,
    lakefs_client: Optional[LakeFSClient],
    repository: str,
    branch: str,
    source: str = "main",
) -> None:
    if not lakefs_client:
        raise RuntimeError("lakefs_client not initialized")
    try:
        await lakefs_client.create_branch(repository=repository, name=branch, source=source)
    except LakeFSConflictError:
        return

