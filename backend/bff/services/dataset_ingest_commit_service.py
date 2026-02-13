"""Shared dataset ingest commit helpers.

Centralizes the common "lakeFS commit -> ingest committed state" workflow used
across dataset upload/version services.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import bff.routers.pipeline_datasets_ops as ops
from shared.utils.s3_uri import build_s3_uri
from shared.observability.tracing import trace_external_call


@dataclass(frozen=True)
class LakeFSCommitArtifact:
    commit_id: str
    artifact_key: str
    created_commit: bool


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


@trace_external_call("bff.dataset_ingest_commit.ensure_lakefs_commit_artifact")
async def ensure_lakefs_commit_artifact(
    *,
    ingest_request: Any,
    lakefs_client: Any,
    lakefs_storage_service: Any,
    repository: str,
    branch: str,
    commit_message: str,
    commit_metadata: Dict[str, Any],
    object_key: str,
    expected_checksum: Optional[str] = None,
    source_branch: str = "main",
    artifact_key_builder: Optional[Callable[[str], str]] = None,
    initial_commit_id: Optional[str] = None,
    initial_artifact_key: Optional[str] = None,
) -> LakeFSCommitArtifact:
    commit_id = _normalize_text(initial_commit_id)
    if not commit_id:
        commit_id = _normalize_text(getattr(ingest_request, "lakefs_commit_id", None))
    artifact_key = _normalize_text(initial_artifact_key)
    if not artifact_key:
        artifact_key = _normalize_text(getattr(ingest_request, "artifact_key", None))

    created_commit = False
    if not commit_id or not artifact_key:
        await ops._ensure_lakefs_branch_exists(
            lakefs_client=lakefs_client,
            repository=repository,
            branch=branch,
            source_branch=source_branch,
        )
        commit_id = await ops._commit_lakefs_with_predicate_fallback(
            lakefs_client=lakefs_client,
            lakefs_storage_service=lakefs_storage_service,
            repository=repository,
            branch=branch,
            message=commit_message,
            metadata=dict(commit_metadata or {}),
            object_key=object_key,
            expected_checksum=expected_checksum,
        )
        commit_id = _normalize_text(commit_id)
        if artifact_key_builder is not None:
            artifact_key = _normalize_text(artifact_key_builder(commit_id))
        else:
            artifact_key = build_s3_uri(repository, f"{commit_id}/{object_key}")
        created_commit = True

    return LakeFSCommitArtifact(
        commit_id=commit_id,
        artifact_key=artifact_key,
        created_commit=created_commit,
    )


@trace_db_operation("bff.dataset_ingest_commit.persist_ingest_commit_state")
async def persist_ingest_commit_state(
    *,
    dataset_registry: Any,
    ingest_request: Any,
    ingest_transaction: Optional[Any],
    commit_id: str,
    artifact_key: str,
    force: bool = False,
) -> Any:
    commit_id_final = _normalize_text(commit_id)
    artifact_key_final = _normalize_text(artifact_key)
    current_commit = _normalize_text(getattr(ingest_request, "lakefs_commit_id", None))
    current_artifact_key = _normalize_text(getattr(ingest_request, "artifact_key", None))

    should_persist = bool(force)
    if not should_persist and (current_commit != commit_id_final or current_artifact_key != artifact_key_final):
        should_persist = True

    if not should_persist:
        return ingest_request

    updated = await dataset_registry.mark_ingest_committed(
        ingest_request_id=ingest_request.ingest_request_id,
        lakefs_commit_id=commit_id_final,
        artifact_key=artifact_key_final,
    )
    if ingest_transaction:
        await dataset_registry.mark_ingest_transaction_committed(
            ingest_request_id=ingest_request.ingest_request_id,
            lakefs_commit_id=commit_id_final,
            artifact_key=artifact_key_final,
        )
    return updated
