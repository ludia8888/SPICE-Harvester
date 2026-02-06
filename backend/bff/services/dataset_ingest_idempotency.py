"""Dataset ingest idempotency helpers (BFF).

Centralizes idempotency-key reuse checks across dataset ingest entry points
(tabular upload, media upload, and direct version creation).
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import HTTPException, status


async def resolve_existing_version_or_raise(
    *,
    dataset_registry: Any,
    ingest_request: Any,
    expected_dataset_id: str,
    request_fingerprint: str,
) -> Optional[Any]:
    """
    Enforce idempotency invariants for an ingest request.

    Returns an existing dataset version when the request was already published.
    Raises HTTP 409 for conflicts or prior failures.
    """

    if ingest_request is None:
        return None

    if str(getattr(ingest_request, "dataset_id", "")) != str(expected_dataset_id):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency key already used for a different dataset",
        )

    existing_fingerprint = getattr(ingest_request, "request_fingerprint", None)
    if existing_fingerprint and str(existing_fingerprint) != str(request_fingerprint):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency key reuse detected with different payload",
        )

    if str(getattr(ingest_request, "status", "")).upper() == "FAILED":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=getattr(ingest_request, "error", None) or "Previous ingest failed",
        )

    if str(getattr(ingest_request, "status", "")).upper() == "PUBLISHED":
        get_version = getattr(dataset_registry, "get_version_by_ingest_request", None)
        if callable(get_version):
            return await get_version(ingest_request_id=ingest_request.ingest_request_id)
        return None

    return None

