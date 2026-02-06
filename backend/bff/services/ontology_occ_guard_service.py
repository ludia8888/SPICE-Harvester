"""Ontology optimistic concurrency guard helpers.

Centralizes the `expected_head_commit` resolution flow used by ontology write
services so callers can share one consistent strategy.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import httpx
from fastapi import HTTPException, status

from bff.services.oms_client import OMSClient


def _normalize_ref(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _extract_head_commit_id(head_payload: Any) -> Optional[str]:
    if not isinstance(head_payload, dict):
        return None
    head_data = head_payload.get("data") if isinstance(head_payload.get("data"), dict) else head_payload
    if not isinstance(head_data, dict):
        return None
    return (
        _normalize_ref(head_data.get("head_commit_id"))
        or _normalize_ref(head_data.get("commit"))
        or _normalize_ref(head_data.get("head_commit"))
    )


async def fetch_branch_head_commit_id(
    *,
    oms_client: OMSClient,
    db_name: str,
    branch: str,
) -> Optional[str]:
    head_payload = await oms_client.get_version_head(db_name, branch=branch)
    return _extract_head_commit_id(head_payload)


async def resolve_expected_head_commit(
    *,
    oms_client: OMSClient,
    db_name: str,
    branch: str,
    expected_head_commit: Optional[str],
    allow_none: bool = False,
    unresolved_detail: str = "expected_head_commit is required (could not resolve branch head)",
) -> Optional[str]:
    expected_head = _normalize_ref(expected_head_commit)
    if expected_head:
        return expected_head

    resolved_head = await fetch_branch_head_commit_id(
        oms_client=oms_client,
        db_name=db_name,
        branch=branch,
    )
    if resolved_head:
        return resolved_head
    if allow_none:
        return None
    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=unresolved_detail)


async def resolve_branch_head_commit_with_bootstrap(
    *,
    oms_client: OMSClient,
    db_name: str,
    branch: str,
    source_branch: str = "main",
    max_attempts: int = 5,
    initial_backoff_seconds: float = 0.15,
    max_backoff_seconds: float = 1.0,
    warning_logger: Optional[logging.Logger] = None,
) -> Optional[str]:
    log = warning_logger or logging.getLogger(__name__)
    attempts = max(1, int(max_attempts))

    try:
        return await fetch_branch_head_commit_id(oms_client=oms_client, db_name=db_name, branch=branch)
    except httpx.HTTPStatusError as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        if status_code != 404 or branch == source_branch:
            raise

    try:
        try:
            await oms_client.create_branch(
                db_name,
                {"branch_name": branch, "from_branch": source_branch},
            )
        except httpx.HTTPStatusError as create_exc:
            create_status = getattr(getattr(create_exc, "response", None), "status_code", None)
            if create_status != 409:
                raise

        backoff_seconds = float(initial_backoff_seconds)
        for attempt in range(attempts):
            try:
                commit_id = await fetch_branch_head_commit_id(oms_client=oms_client, db_name=db_name, branch=branch)
            except httpx.HTTPStatusError as head_exc:
                head_status = getattr(getattr(head_exc, "response", None), "status_code", None)
                if head_status == 404 and attempt < (attempts - 1):
                    await asyncio.sleep(backoff_seconds)
                    backoff_seconds = min(backoff_seconds * 2, float(max_backoff_seconds))
                    continue
                raise
            if commit_id:
                return commit_id
            if attempt < (attempts - 1):
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, float(max_backoff_seconds))
    except Exception as exc:
        log.warning(
            "Failed to ensure ontology branch (db=%s branch=%s): %s",
            db_name,
            branch,
            exc,
        )
    return None
