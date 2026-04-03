from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import asyncpg

from shared.observability.context_propagation import enrich_metadata_with_current_trace
from shared.services.core.write_path_contract import (
    build_write_path_contract,
    followup_completed,
    followup_skipped,
)
from shared.services.registries.dataset_registry_models import DatasetVersionRecord
from shared.services.registries.dataset_registry_models import DatasetIngestOutboxItem
from shared.utils.json_utils import coerce_json_dataset, normalize_json_payload

logger = logging.getLogger(__name__)


def _inject_dataset_version(outbox_entries: List[Dict[str, Any]], dataset_version_id: str) -> None:
    for entry in outbox_entries or []:
        if not isinstance(entry, dict):
            continue
        kind = str(entry.get("kind") or "").lower()
        payload = entry.get("payload")
        if not isinstance(payload, dict):
            continue
        if kind == "eventstore":
            if payload.get("event_type") != "DATASET_VERSION_CREATED":
                continue
            data = payload.get("data")
            if not isinstance(data, dict):
                continue
            inner = data.get("payload")
            if isinstance(inner, dict) and "dataset_version_id" not in inner:
                inner = {**inner, "dataset_version_id": dataset_version_id}
                payload["data"] = {**data, "payload": inner}
                entry["payload"] = payload
        elif kind == "lineage":
            meta = payload.get("edge_metadata")
            if isinstance(meta, dict) and "dataset_version_id" not in meta:
                payload["edge_metadata"] = {**meta, "dataset_version_id": dataset_version_id}
            if isinstance(payload.get("from_node_id"), str) and payload["from_node_id"].startswith("event:"):
                payload["from_node_id"] = f"agg:DatasetVersion:{dataset_version_id}"
            entry["payload"] = payload


def _log_dataset_ingest_publish_contract(
    *,
    ingest_request_id: str,
    dataset_version_id: str,
    schema_applied: bool,
    outbox_entries: Optional[List[Dict[str, Any]]],
    recovery_mode: str,
) -> None:
    contract = build_write_path_contract(
        authoritative_write="dataset_ingest_publish",
        followups=[
            followup_completed(
                "dataset_version_commit",
                details={
                    "dataset_version_id": dataset_version_id,
                    "recovery_mode": recovery_mode,
                },
            ),
            followup_completed(
                "dataset_schema_apply",
                details={"applied": schema_applied},
            )
            if schema_applied
            else followup_skipped(
                "dataset_schema_apply",
                details={"reason": "schema_unchanged"},
            ),
            followup_completed("dataset_backing_version_upsert"),
            followup_completed("dataset_ingest_request_status"),
            followup_completed("dataset_ingest_transaction_status"),
            followup_completed(
                "dataset_ingest_outbox",
                details={"count": len(outbox_entries or [])},
            )
            if outbox_entries
            else followup_skipped(
                "dataset_ingest_outbox",
                details={"reason": "not_requested"},
            ),
        ],
    )
    logger.info(
        "Dataset ingest write path contract: %s",
        contract,
        extra={
            "write_path_contract": contract,
            "dataset_ingest_request_id": ingest_request_id,
            "dataset_version_id": dataset_version_id,
        },
    )


async def publish_ingest_request(
    registry: Any,
    *,
    ingest_request_id: str,
    dataset_id: str,
    lakefs_commit_id: str,
    artifact_key: Optional[str],
    row_count: Optional[int],
    sample_json: Optional[Dict[str, Any]],
    schema_json: Optional[Dict[str, Any]],
    apply_schema: bool = True,
    outbox_entries: Optional[List[Dict[str, Any]]] = None,
) -> DatasetVersionRecord:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    sample_json = sample_json or {}
    sample_payload = normalize_json_payload(sample_json)
    schema_payload = normalize_json_payload(schema_json) if schema_json is not None else None
    schema_hash = registry._resolve_version_schema_hash(sample_json=sample_json, schema_json=schema_json)
    schema_applied = schema_payload is not None and apply_schema

    async with registry._pool.acquire() as conn:
        async with conn.transaction():
            if schema_applied:
                await conn.execute(
                    f"""
                    UPDATE {registry._schema}.datasets
                    SET schema_json = $2::jsonb, updated_at = NOW()
                    WHERE dataset_id = $1
                    """,
                    dataset_id,
                    schema_payload,
                )
            existing = await conn.fetchrow(
                f"""
                SELECT version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                       ingest_request_id, promoted_from_artifact_id, created_at
                FROM {registry._schema}.dataset_versions
                WHERE ingest_request_id = $1::uuid
                """,
                ingest_request_id,
            )
            if existing:
                dataset_version_id = str(existing["version_id"])
                await registry._upsert_backing_version_for_dataset_version(
                    conn,
                    dataset_id=dataset_id,
                    dataset_version_id=dataset_version_id,
                    artifact_key=artifact_key,
                    schema_hash=schema_hash,
                )
                await conn.execute(
                    f"""
                    UPDATE {registry._schema}.dataset_ingest_requests
                    SET status = 'PUBLISHED',
                        published_at = COALESCE(published_at, NOW()),
                        updated_at = NOW()
                    WHERE ingest_request_id = $1::uuid
                    """,
                    ingest_request_id,
                )
                await conn.execute(
                    f"""
                    UPDATE {registry._schema}.dataset_ingest_transactions
                    SET status = 'COMMITTED',
                        committed_at = COALESCE(committed_at, NOW()),
                        updated_at = NOW()
                    WHERE ingest_request_id = $1::uuid
                    """,
                    ingest_request_id,
                )
                if outbox_entries:
                    has_outbox = await conn.fetchval(
                        f"""
                        SELECT 1 FROM {registry._schema}.dataset_ingest_outbox
                        WHERE ingest_request_id = $1::uuid
                          AND status <> 'dead'
                        LIMIT 1
                        """,
                        ingest_request_id,
                    )
                    if not has_outbox:
                        _inject_dataset_version(outbox_entries, dataset_version_id)
                        for entry in outbox_entries:
                            payload = entry.get("payload") or {}
                            if isinstance(payload, dict):
                                enrich_metadata_with_current_trace(payload)
                            await conn.execute(
                                f"""
                                INSERT INTO {registry._schema}.dataset_ingest_outbox (
                                    outbox_id, ingest_request_id, kind, payload, status
                                ) VALUES ($1::uuid, $2::uuid, $3, $4::jsonb, 'pending')
                                """,
                                str(uuid4()),
                                ingest_request_id,
                                str(entry.get("kind") or "eventstore"),
                                normalize_json_payload(payload),
                            )
                _log_dataset_ingest_publish_contract(
                    ingest_request_id=ingest_request_id,
                    dataset_version_id=dataset_version_id,
                    schema_applied=schema_applied,
                    outbox_entries=outbox_entries,
                    recovery_mode="existing_request",
                )
                return DatasetVersionRecord(
                    version_id=dataset_version_id,
                    dataset_id=str(existing["dataset_id"]),
                    lakefs_commit_id=str(existing["lakefs_commit_id"]),
                    artifact_key=existing["artifact_key"],
                    row_count=existing["row_count"],
                    sample_json=coerce_json_dataset(existing["sample_json"]),
                    ingest_request_id=str(existing["ingest_request_id"]) if existing["ingest_request_id"] else None,
                    promoted_from_artifact_id=(
                        str(existing["promoted_from_artifact_id"]) if existing["promoted_from_artifact_id"] else None
                    ),
                    created_at=existing["created_at"],
                )

            try:
                async with conn.transaction():
                    row = await conn.fetchrow(
                        f"""
                        INSERT INTO {registry._schema}.dataset_versions (
                            version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                            ingest_request_id, promoted_from_artifact_id
                        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::uuid, NULL)
                        RETURNING version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                                  ingest_request_id, promoted_from_artifact_id, created_at
                        """,
                        str(uuid4()),
                        dataset_id,
                        lakefs_commit_id,
                        artifact_key,
                        row_count,
                        sample_payload,
                        ingest_request_id,
                    )
            except asyncpg.UniqueViolationError:
                existing = await conn.fetchrow(
                    f"""
                    SELECT version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                           ingest_request_id, promoted_from_artifact_id, created_at
                    FROM {registry._schema}.dataset_versions
                    WHERE ingest_request_id = $1::uuid
                    LIMIT 1
                    """,
                    ingest_request_id,
                )
                if not existing:
                    existing = await conn.fetchrow(
                        f"""
                        SELECT version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                               ingest_request_id, promoted_from_artifact_id, created_at
                        FROM {registry._schema}.dataset_versions
                        WHERE dataset_id = $1 AND lakefs_commit_id = $2
                        LIMIT 1
                        """,
                        dataset_id,
                        lakefs_commit_id,
                    )
                if existing and existing["ingest_request_id"] and str(existing["ingest_request_id"]) != ingest_request_id:
                    raise RuntimeError("dataset version already linked to a different ingest_request_id")
                if not existing:
                    raise
                dataset_version_id = str(existing["version_id"])
                await registry._upsert_backing_version_for_dataset_version(
                    conn,
                    dataset_id=dataset_id,
                    dataset_version_id=dataset_version_id,
                    artifact_key=artifact_key,
                    schema_hash=schema_hash,
                )
                await conn.execute(
                    f"""
                    UPDATE {registry._schema}.dataset_ingest_requests
                    SET status = 'PUBLISHED',
                        lakefs_commit_id = $2,
                        artifact_key = $3,
                        published_at = NOW(),
                        updated_at = NOW()
                    WHERE ingest_request_id = $1::uuid
                    """,
                    ingest_request_id,
                    lakefs_commit_id,
                    artifact_key,
                )
                await conn.execute(
                    f"""
                    UPDATE {registry._schema}.dataset_ingest_transactions
                    SET status = 'COMMITTED',
                        lakefs_commit_id = $2,
                        artifact_key = $3,
                        committed_at = COALESCE(committed_at, NOW()),
                        updated_at = NOW()
                    WHERE ingest_request_id = $1::uuid
                    """,
                    ingest_request_id,
                    lakefs_commit_id,
                    artifact_key,
                )
                if outbox_entries:
                    has_outbox = await conn.fetchval(
                        f"""
                        SELECT 1 FROM {registry._schema}.dataset_ingest_outbox
                        WHERE ingest_request_id = $1::uuid
                          AND status <> 'dead'
                        LIMIT 1
                        """,
                        ingest_request_id,
                    )
                    if not has_outbox:
                        _inject_dataset_version(outbox_entries, dataset_version_id)
                        for entry in outbox_entries:
                            payload = entry.get("payload") or {}
                            if isinstance(payload, dict):
                                enrich_metadata_with_current_trace(payload)
                            await conn.execute(
                                f"""
                                INSERT INTO {registry._schema}.dataset_ingest_outbox (
                                    outbox_id, ingest_request_id, kind, payload, status
                                ) VALUES ($1::uuid, $2::uuid, $3, $4::jsonb, 'pending')
                                """,
                                str(uuid4()),
                                ingest_request_id,
                                str(entry.get("kind") or "eventstore"),
                                normalize_json_payload(payload),
                            )
                _log_dataset_ingest_publish_contract(
                    ingest_request_id=ingest_request_id,
                    dataset_version_id=dataset_version_id,
                    schema_applied=schema_applied,
                    outbox_entries=outbox_entries,
                    recovery_mode="duplicate_insert_recovered",
                )
                return DatasetVersionRecord(
                    version_id=dataset_version_id,
                    dataset_id=str(existing["dataset_id"]),
                    lakefs_commit_id=str(existing["lakefs_commit_id"]),
                    artifact_key=existing["artifact_key"],
                    row_count=existing["row_count"],
                    sample_json=coerce_json_dataset(existing["sample_json"]),
                    ingest_request_id=str(existing["ingest_request_id"]) if existing["ingest_request_id"] else None,
                    promoted_from_artifact_id=(
                        str(existing["promoted_from_artifact_id"]) if existing["promoted_from_artifact_id"] else None
                    ),
                    created_at=existing["created_at"],
                )
            if not row:
                raise RuntimeError("Failed to publish dataset version")
            dataset_version_id = str(row["version_id"])
            await registry._upsert_backing_version_for_dataset_version(
                conn,
                dataset_id=dataset_id,
                dataset_version_id=dataset_version_id,
                artifact_key=artifact_key,
                schema_hash=schema_hash,
            )
            await conn.execute(
                f"""
                UPDATE {registry._schema}.dataset_ingest_requests
                SET status = 'PUBLISHED',
                    lakefs_commit_id = $2,
                    artifact_key = $3,
                    published_at = NOW(),
                    updated_at = NOW()
                WHERE ingest_request_id = $1::uuid
                """,
                ingest_request_id,
                lakefs_commit_id,
                artifact_key,
            )
            await conn.execute(
                f"""
                UPDATE {registry._schema}.dataset_ingest_transactions
                SET status = 'COMMITTED',
                    lakefs_commit_id = $2,
                    artifact_key = $3,
                    committed_at = COALESCE(committed_at, NOW()),
                    updated_at = NOW()
                WHERE ingest_request_id = $1::uuid
                """,
                ingest_request_id,
                lakefs_commit_id,
                artifact_key,
            )
            if outbox_entries:
                _inject_dataset_version(outbox_entries, dataset_version_id)
                for entry in outbox_entries:
                    payload = entry.get("payload") or {}
                    if isinstance(payload, dict):
                        enrich_metadata_with_current_trace(payload)
                    await conn.execute(
                        f"""
                        INSERT INTO {registry._schema}.dataset_ingest_outbox (
                            outbox_id, ingest_request_id, kind, payload, status
                        ) VALUES ($1::uuid, $2::uuid, $3, $4::jsonb, 'pending')
                        """,
                        str(uuid4()),
                        ingest_request_id,
                        str(entry.get("kind") or "eventstore"),
                        normalize_json_payload(payload),
                    )
            _log_dataset_ingest_publish_contract(
                ingest_request_id=ingest_request_id,
                dataset_version_id=dataset_version_id,
                schema_applied=schema_applied,
                outbox_entries=outbox_entries,
                recovery_mode="created",
            )
            return DatasetVersionRecord(
                version_id=dataset_version_id,
                dataset_id=str(row["dataset_id"]),
                lakefs_commit_id=str(row["lakefs_commit_id"]),
                artifact_key=row["artifact_key"],
                row_count=row["row_count"],
                sample_json=coerce_json_dataset(row["sample_json"]),
                ingest_request_id=str(row["ingest_request_id"]) if row["ingest_request_id"] else None,
                promoted_from_artifact_id=(
                    str(row["promoted_from_artifact_id"]) if row["promoted_from_artifact_id"] else None
                ),
                created_at=row["created_at"],
            )


async def claim_ingest_outbox_batch(
    registry: Any,
    *,
    limit: int = 50,
    claimed_by: Optional[str] = None,
    claim_timeout_seconds: int = 300,
) -> List[DatasetIngestOutboxItem]:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    async with registry._pool.acquire() as conn:
        async with conn.transaction():
            claim_timeout = max(0, int(claim_timeout_seconds))
            clause = """
                WHERE (
                    status IN ('pending', 'failed')
                    AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
                )
            """
            values: List[Any] = [limit]
            if claim_timeout > 0:
                clause += f"""
                    OR (
                        status = 'publishing'
                        AND (claimed_at IS NULL OR claimed_at <= NOW() - (${len(values) + 1}::int * INTERVAL '1 second'))
                    )
                """
                values.append(claim_timeout)
            rows = await conn.fetch(
                f"""
                SELECT outbox_id, ingest_request_id, kind, payload, status, publish_attempts, error,
                       retry_count, last_error,
                       claimed_by, claimed_at, next_attempt_at, created_at, updated_at
                FROM {registry._schema}.dataset_ingest_outbox
                {clause}
                ORDER BY created_at ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
                """,
                *values,
            )
            if not rows:
                return []
            outbox_ids = [str(row["outbox_id"]) for row in rows]
            await conn.execute(
                f"""
                UPDATE {registry._schema}.dataset_ingest_outbox
                SET status = 'publishing',
                    publish_attempts = publish_attempts + 1,
                    retry_count = retry_count + 1,
                    claimed_by = $2,
                    claimed_at = NOW(),
                    updated_at = NOW()
                WHERE outbox_id = ANY($1::uuid[])
                """,
                outbox_ids,
                claimed_by,
            )
            return [
                DatasetIngestOutboxItem(
                    outbox_id=str(row["outbox_id"]),
                    ingest_request_id=str(row["ingest_request_id"]),
                    kind=row["kind"],
                    payload=coerce_json_dataset(row["payload"]),
                    status=row["status"],
                    publish_attempts=int(row["publish_attempts"]),
                    error=row["error"],
                    retry_count=int(row["retry_count"] or 0),
                    last_error=row["last_error"],
                    claimed_by=str(row["claimed_by"]) if row["claimed_by"] else None,
                    claimed_at=row["claimed_at"],
                    next_attempt_at=row["next_attempt_at"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
                for row in rows
            ]


async def mark_ingest_outbox_published(registry: Any, *, outbox_id: str) -> None:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    async with registry._pool.acquire() as conn:
        await conn.execute(
            f"""
            UPDATE {registry._schema}.dataset_ingest_outbox
            SET status = 'published',
                updated_at = NOW(),
                next_attempt_at = NULL,
                error = NULL,
                last_error = NULL,
                claimed_by = NULL,
                claimed_at = NULL
            WHERE outbox_id = $1::uuid
            """,
            outbox_id,
        )


async def mark_ingest_outbox_failed(
    registry: Any,
    *,
    outbox_id: str,
    error: str,
    next_attempt_at: Optional[datetime] = None,
) -> None:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    async with registry._pool.acquire() as conn:
        await conn.execute(
            f"""
            UPDATE {registry._schema}.dataset_ingest_outbox
            SET status = 'failed',
                error = $2,
                last_error = $2,
                next_attempt_at = $3,
                claimed_by = NULL,
                claimed_at = NULL,
                updated_at = NOW()
            WHERE outbox_id = $1::uuid
            """,
            outbox_id,
            error,
            next_attempt_at,
        )


async def mark_ingest_outbox_dead(registry: Any, *, outbox_id: str, error: str) -> None:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    async with registry._pool.acquire() as conn:
        await conn.execute(
            f"""
            UPDATE {registry._schema}.dataset_ingest_outbox
            SET status = 'dead',
                error = $2,
                last_error = $2,
                next_attempt_at = NULL,
                claimed_by = NULL,
                claimed_at = NULL,
                updated_at = NOW()
            WHERE outbox_id = $1::uuid
            """,
            outbox_id,
            error,
        )


async def purge_ingest_outbox(
    registry: Any,
    *,
    retention_days: int = 7,
    limit: int = 10_000,
) -> int:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    retention_days = max(1, int(retention_days))
    limit = max(1, int(limit))
    async with registry._pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            WITH doomed AS (
                SELECT ctid
                FROM {registry._schema}.dataset_ingest_outbox
                WHERE status = 'published'
                  AND updated_at < NOW() - ($1::int * INTERVAL '1 day')
                ORDER BY updated_at ASC
                LIMIT $2
            )
            DELETE FROM {registry._schema}.dataset_ingest_outbox
            WHERE ctid IN (SELECT ctid FROM doomed)
            RETURNING 1
            """,
            retention_days,
            limit,
        )
    return len(rows or [])


async def get_ingest_outbox_metrics(registry: Any) -> Dict[str, Any]:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    async with registry._pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT status, count(*) AS count, MIN(created_at) AS oldest_created_at,
                   MIN(next_attempt_at) AS next_attempt_at,
                   MAX(retry_count) AS max_retry
            FROM {registry._schema}.dataset_ingest_outbox
            GROUP BY status
            """
        )
    counts = {str(row["status"]): int(row["count"]) for row in rows or []}
    backlog_statuses = {"pending", "publishing", "failed"}
    backlog = sum(counts.get(status, 0) for status in backlog_statuses)
    oldest_candidates = [
        row["oldest_created_at"]
        for row in rows or []
        if row["status"] in backlog_statuses and row["oldest_created_at"]
    ]
    oldest_created = min(oldest_candidates) if oldest_candidates else None
    next_attempt = min((row["next_attempt_at"] for row in rows or [] if row["next_attempt_at"]), default=None)
    now = datetime.now(timezone.utc)
    oldest_age_seconds = None
    if oldest_created:
        try:
            oldest_age_seconds = int((now - oldest_created).total_seconds())
        except Exception:
            logging.getLogger(__name__).warning(
                "Exception fallback at shared/services/registries/dataset_registry_publish.py:get_ingest_outbox_metrics",
                exc_info=True,
            )
            oldest_age_seconds = None

    return {
        "counts": counts,
        "backlog": backlog,
        "oldest_created_at": oldest_created.isoformat() if oldest_created else None,
        "oldest_age_seconds": oldest_age_seconds,
        "next_attempt_at": next_attempt.isoformat() if next_attempt else None,
    }
