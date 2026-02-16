"""Ontology optimistic concurrency guard helpers.

Foundry-style profile no longer depends on ontology branch/version head commit
contracts. These helpers keep a stable call signature for callers while
normalizing to optional OCC tokens.
"""

from __future__ import annotations

from typing import Any, Optional

from bff.services.oms_client import OMSClient
from shared.observability.tracing import trace_external_call


def _normalize_ref(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


@trace_external_call("bff.ontology_occ_guard.fetch_branch_head_commit_id")
async def fetch_branch_head_commit_id(
    *,
    oms_client: OMSClient,
    db_name: str,
    branch: str,
) -> Optional[str]:
    _ = oms_client, db_name, branch
    # Foundry-style profile: no branch head commit contract.
    return None


@trace_external_call("bff.ontology_occ_guard.resolve_expected_head_commit")
async def resolve_expected_head_commit(
    *,
    oms_client: OMSClient,
    db_name: str,
    branch: str,
    expected_head_commit: Optional[str],
    allow_none: bool = False,
    unresolved_detail: str = "expected_head_commit is required (could not resolve branch head)",
) -> Optional[str]:
    _ = oms_client, db_name, branch, allow_none, unresolved_detail
    expected_head = _normalize_ref(expected_head_commit)
    if expected_head:
        return expected_head
    # In Foundry-style profile, resource writes rely on OMS-side OCC fallback.
    return None


@trace_external_call("bff.ontology_occ_guard.resolve_branch_head_commit_with_bootstrap")
async def resolve_branch_head_commit_with_bootstrap(
    *,
    oms_client: OMSClient,
    db_name: str,
    branch: str,
    source_branch: str = "main",
    max_attempts: int = 5,
    initial_backoff_seconds: float = 0.15,
    max_backoff_seconds: float = 1.0,
    warning_logger: Any = None,
) -> Optional[str]:
    _ = (
        oms_client,
        db_name,
        branch,
        source_branch,
        max_attempts,
        initial_backoff_seconds,
        max_backoff_seconds,
        warning_logger,
    )
    # Foundry-style profile does not bootstrap ontology branches.
    return None
