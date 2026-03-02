"""Canonical lakeFS branch-creation helper.

Consolidates the three previous implementations:
- ``shared.services.storage.lakefs_branch_utils.ensure_lakefs_branch`` (minimal)
- ``bff.routers.pipeline_datasets_ops_lakefs._ensure_lakefs_branch_exists`` (sanitize + skip)
- ``shared.services.registries.pipeline_registry.PipelineRegistry.ensure_lakefs_branch`` (strip + skip ``main``)

All call-sites now delegate here for consistent behaviour.
"""

from __future__ import annotations

from typing import Optional

from shared.observability.tracing import trace_external_call
from shared.services.storage.lakefs_client import LakeFSClient, LakeFSConflictError
from shared.utils.path_utils import safe_lakefs_ref


@trace_external_call("lakefs.ensure_branch")
async def ensure_lakefs_branch(
    *,
    lakefs_client: Optional[LakeFSClient],
    repository: str,
    branch: str,
    source: str = "main",
    sanitize: bool = False,
    skip_if_source_matches: bool = False,
) -> None:
    """Create ``branch`` from ``source`` in *repository*, swallowing "already exists".

    Parameters
    ----------
    lakefs_client:
        A connected :class:`LakeFSClient`. ``RuntimeError`` is raised when ``None``.
    repository / branch / source:
        Standard lakeFS coordinates.
    sanitize:
        When ``True``, branch and source names are cleaned via
        :func:`safe_lakefs_ref` (e.g. stripping invalid characters).
    skip_if_source_matches:
        When ``True``, the call is a no-op if the resolved branch equals the
        resolved source (e.g. both are ``"main"``).
    """
    if not lakefs_client:
        raise RuntimeError("lakefs_client not initialized")

    resolved_branch = safe_lakefs_ref(branch) if sanitize else branch
    resolved_source = safe_lakefs_ref(source) if sanitize else source

    if skip_if_source_matches and resolved_branch == resolved_source:
        return

    try:
        await lakefs_client.create_branch(
            repository=repository, name=resolved_branch, source=resolved_source
        )
    except LakeFSConflictError:
        return
