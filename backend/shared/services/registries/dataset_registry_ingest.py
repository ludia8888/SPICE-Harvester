from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from uuid import uuid4

import asyncpg

from shared.config.settings import get_settings
from shared.services.registries.dataset_registry_models import (
    DatasetIngestRequestRecord,
    DatasetIngestTransactionRecord,
)
from shared.services.registries.dataset_registry_rows import (
    row_to_dataset,
    row_to_dataset_ingest_request,
    row_to_dataset_ingest_transaction,
)
from shared.utils.json_utils import coerce_json_dataset
from shared.utils.json_utils import normalize_json_payload
from shared.utils.s3_uri import parse_s3_uri
from shared.utils.time_utils import utcnow

if TYPE_CHECKING:
    from shared.services.registries.dataset_registry import DatasetRegistry


logger = logging.getLogger(__name__)


def _require_pool(registry: "DatasetRegistry") -> Any:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    return registry._pool


async def create_ingest_request(
    registry: "DatasetRegistry",
    *,
    dataset_id: str,
    db_name: str,
    branch: str,
    idempotency_key: str,
    request_fingerprint: Optional[str],
    schema_json: Optional[Dict[str, Any]] = None,
    sample_json: Optional[Dict[str, Any]] = None,
    row_count: Optional[int] = None,
    source_metadata: Optional[Dict[str, Any]] = None,
) -> tuple[DatasetIngestRequestRecord, bool]:
    pool = _require_pool(registry)
    ingest_request_id = str(uuid4())
    schema_payload = normalize_json_payload(schema_json) if schema_json is not None else None
    sample_payload = normalize_json_payload(sample_json) if sample_json is not None else None
    source_payload = normalize_json_payload(source_metadata) if source_metadata is not None else None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {registry._schema}.dataset_ingest_requests (
                ingest_request_id, dataset_id, db_name, branch, idempotency_key,
                request_fingerprint, status, schema_json, sample_json, row_count, source_metadata
            ) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, 'RECEIVED', $7::jsonb, $8::jsonb, $9, $10::jsonb)
            ON CONFLICT (idempotency_key) DO UPDATE
            SET updated_at = NOW(),
                schema_json = COALESCE(EXCLUDED.schema_json, {registry._schema}.dataset_ingest_requests.schema_json),
                sample_json = COALESCE(EXCLUDED.sample_json, {registry._schema}.dataset_ingest_requests.sample_json),
                row_count = COALESCE(EXCLUDED.row_count, {registry._schema}.dataset_ingest_requests.row_count),
                source_metadata = COALESCE(EXCLUDED.source_metadata, {registry._schema}.dataset_ingest_requests.source_metadata)
            RETURNING ingest_request_id, dataset_id, db_name, branch, idempotency_key, request_fingerprint,
                      status, lakefs_commit_id, artifact_key, schema_json, schema_status,
                      schema_approved_at, schema_approved_by,
                      sample_json, row_count, source_metadata, error, created_at, updated_at, published_at
            """,
            ingest_request_id,
            dataset_id,
            db_name,
            branch,
            idempotency_key,
            request_fingerprint,
            schema_payload,
            sample_payload,
            row_count,
            source_payload,
        )
        if not row:
            raise RuntimeError("Failed to create ingest request")
        record = row_to_dataset_ingest_request(row)
        is_new = record.ingest_request_id == ingest_request_id
        return record, is_new


async def get_ingest_request_by_key(
    registry: "DatasetRegistry",
    *,
    idempotency_key: str,
) -> Optional[DatasetIngestRequestRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT ingest_request_id, dataset_id, db_name, branch, idempotency_key, request_fingerprint,
                   status, lakefs_commit_id, artifact_key, schema_json, schema_status,
                   schema_approved_at, schema_approved_by,
                   sample_json, row_count, source_metadata, error, created_at, updated_at, published_at
            FROM {registry._schema}.dataset_ingest_requests
            WHERE idempotency_key = $1
            """,
            idempotency_key,
        )
        return row_to_dataset_ingest_request(row) if row else None


async def get_ingest_request(
    registry: "DatasetRegistry",
    *,
    ingest_request_id: str,
) -> Optional[DatasetIngestRequestRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT ingest_request_id, dataset_id, db_name, branch, idempotency_key, request_fingerprint,
                   status, lakefs_commit_id, artifact_key, schema_json, schema_status,
                   schema_approved_at, schema_approved_by,
                   sample_json, row_count, source_metadata, error, created_at, updated_at, published_at
            FROM {registry._schema}.dataset_ingest_requests
            WHERE ingest_request_id = $1::uuid
            """,
            ingest_request_id,
        )
        return row_to_dataset_ingest_request(row) if row else None


async def get_ingest_transaction(
    registry: "DatasetRegistry",
    *,
    ingest_request_id: str,
) -> Optional[DatasetIngestTransactionRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT transaction_id, ingest_request_id, status, lakefs_commit_id, artifact_key,
                   error, created_at, updated_at, committed_at, aborted_at
            FROM {registry._schema}.dataset_ingest_transactions
            WHERE ingest_request_id = $1::uuid
            """,
            ingest_request_id,
        )
        return row_to_dataset_ingest_transaction(row) if row else None


async def get_ingest_transaction_by_id(
    registry: "DatasetRegistry",
    *,
    transaction_id: str,
) -> Optional[DatasetIngestTransactionRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT transaction_id, ingest_request_id, status, lakefs_commit_id, artifact_key,
                   error, created_at, updated_at, committed_at, aborted_at
            FROM {registry._schema}.dataset_ingest_transactions
            WHERE transaction_id = $1::uuid
            """,
            transaction_id,
        )
        return row_to_dataset_ingest_transaction(row) if row else None


async def list_ingest_transactions_for_dataset(
    registry: "DatasetRegistry",
    *,
    dataset_id: str,
    branch: str,
    limit: int = 100,
    offset: int = 0,
) -> List[DatasetIngestTransactionRecord]:
    pool = _require_pool(registry)
    resolved_limit = max(1, int(limit))
    resolved_offset = max(0, int(offset))
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT t.transaction_id, t.ingest_request_id, t.status, t.lakefs_commit_id, t.artifact_key,
                   t.error, t.created_at, t.updated_at, t.committed_at, t.aborted_at
            FROM {registry._schema}.dataset_ingest_transactions t
            JOIN {registry._schema}.dataset_ingest_requests r
              ON r.ingest_request_id = t.ingest_request_id
            WHERE r.dataset_id = $1::uuid
              AND r.branch = $2
            ORDER BY t.created_at DESC
            LIMIT $3 OFFSET $4
            """,
            dataset_id,
            branch,
            resolved_limit,
            resolved_offset,
        )
    return [row_to_dataset_ingest_transaction(row) for row in rows or []]


async def create_ingest_transaction(
    registry: "DatasetRegistry",
    *,
    ingest_request_id: str,
    status: str = "OPEN",
) -> DatasetIngestTransactionRecord:
    pool = _require_pool(registry)
    transaction_id = str(uuid4())
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {registry._schema}.dataset_ingest_transactions (
                transaction_id, ingest_request_id, status
            ) VALUES ($1::uuid, $2::uuid, $3)
            ON CONFLICT (ingest_request_id) DO UPDATE
            SET updated_at = NOW()
            RETURNING transaction_id, ingest_request_id, status, lakefs_commit_id, artifact_key,
                      error, created_at, updated_at, committed_at, aborted_at
            """,
            transaction_id,
            ingest_request_id,
            status,
        )
        if not row:
            raise RuntimeError("Failed to create ingest transaction")
        return row_to_dataset_ingest_transaction(row)


async def mark_ingest_transaction_committed(
    registry: "DatasetRegistry",
    *,
    ingest_request_id: str,
    lakefs_commit_id: str,
    artifact_key: Optional[str],
) -> Optional[DatasetIngestTransactionRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE {registry._schema}.dataset_ingest_transactions
            SET status = 'COMMITTED',
                lakefs_commit_id = $2,
                artifact_key = $3,
                committed_at = COALESCE(committed_at, NOW()),
                updated_at = NOW()
            WHERE ingest_request_id = $1::uuid
            RETURNING transaction_id, ingest_request_id, status, lakefs_commit_id, artifact_key,
                      error, created_at, updated_at, committed_at, aborted_at
            """,
            ingest_request_id,
            lakefs_commit_id,
            artifact_key,
        )
        return row_to_dataset_ingest_transaction(row) if row else None


async def mark_ingest_transaction_aborted(
    registry: "DatasetRegistry",
    *,
    ingest_request_id: str,
    error: str,
) -> Optional[DatasetIngestTransactionRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE {registry._schema}.dataset_ingest_transactions
            SET status = 'ABORTED',
                error = $2,
                aborted_at = COALESCE(aborted_at, NOW()),
                updated_at = NOW()
            WHERE ingest_request_id = $1::uuid
            RETURNING transaction_id, ingest_request_id, status, lakefs_commit_id, artifact_key,
                      error, created_at, updated_at, committed_at, aborted_at
            """,
            ingest_request_id,
            error,
        )
        return row_to_dataset_ingest_transaction(row) if row else None


async def mark_ingest_committed(
    registry: "DatasetRegistry",
    *,
    ingest_request_id: str,
    lakefs_commit_id: str,
    artifact_key: Optional[str],
) -> DatasetIngestRequestRecord:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE {registry._schema}.dataset_ingest_requests
            SET status = 'RAW_COMMITTED',
                lakefs_commit_id = $2,
                artifact_key = $3,
                updated_at = NOW()
            WHERE ingest_request_id = $1::uuid
              AND (lakefs_commit_id IS NULL OR lakefs_commit_id = '')
            RETURNING ingest_request_id, dataset_id, db_name, branch, idempotency_key, request_fingerprint,
                      status, lakefs_commit_id, artifact_key, schema_json, schema_status,
                      schema_approved_at, schema_approved_by,
                      sample_json, row_count, source_metadata, error, created_at, updated_at, published_at
            """,
            ingest_request_id,
            lakefs_commit_id,
            artifact_key,
        )
        if not row:
            row = await conn.fetchrow(
                f"""
                SELECT ingest_request_id, dataset_id, db_name, branch, idempotency_key, request_fingerprint,
                       status, lakefs_commit_id, artifact_key, schema_json, schema_status,
                       schema_approved_at, schema_approved_by,
                       sample_json, row_count, source_metadata, error, created_at, updated_at, published_at
                FROM {registry._schema}.dataset_ingest_requests
                WHERE ingest_request_id = $1::uuid
                """,
                ingest_request_id,
            )
        if not row:
            raise RuntimeError("Ingest request not found")
        return row_to_dataset_ingest_request(row)


async def mark_ingest_failed(
    registry: "DatasetRegistry",
    *,
    ingest_request_id: str,
    error: str,
) -> None:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            UPDATE {registry._schema}.dataset_ingest_requests
            SET status = 'FAILED',
                error = $2,
                updated_at = NOW()
            WHERE ingest_request_id = $1::uuid
            """,
            ingest_request_id,
            error,
        )
        await conn.execute(
            f"""
            UPDATE {registry._schema}.dataset_ingest_transactions
            SET status = 'ABORTED',
                error = $2,
                aborted_at = COALESCE(aborted_at, NOW()),
                updated_at = NOW()
            WHERE ingest_request_id = $1::uuid
            """,
            ingest_request_id,
            error,
        )


async def reconcile_ingest_state(
    registry: "DatasetRegistry",
    *,
    stale_after_seconds: int = 3600,
    limit: int = 200,
    use_lock: bool = True,
    lock_key: Optional[int] = None,
) -> Dict[str, int]:
    """
    Best-effort reconciliation for ingest atomicity.

    - Publishes RAW_COMMITTED ingests that never finalized into dataset_versions.
    - Closes OPEN transactions that are stale (marks ingest FAILED/ABORTED).
    - Repairs transactions that should be COMMITTED based on ingest status.
    """
    pool = _require_pool(registry)
    results = {"published": 0, "outbox_repaired": 0, "aborted": 0, "committed_tx": 0, "skipped": 0}
    cutoff = utcnow() - timedelta(seconds=max(60, int(stale_after_seconds)))

    lock_conn: Optional[asyncpg.Connection] = None
    resolved_lock_key = int(lock_key) if lock_key is not None else int(get_settings().workers.ingest_reconciler.lock_key)

    try:
        if use_lock and resolved_lock_key is not None:
            lock_conn = await pool.acquire()
            locked = await lock_conn.fetchval("SELECT pg_try_advisory_lock($1)", resolved_lock_key)
            if not locked:
                results["skipped"] = 1
                return results

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT ingest_request_id, dataset_id, db_name, branch, lakefs_commit_id, artifact_key,
                       schema_json, sample_json, row_count
                FROM {registry._schema}.dataset_ingest_requests
                WHERE status = 'RAW_COMMITTED'
                  AND lakefs_commit_id IS NOT NULL
                  AND artifact_key IS NOT NULL
                ORDER BY updated_at ASC
                LIMIT $1
                """,
                limit,
            )

        for row in rows or []:
            ingest_request_id = str(row["ingest_request_id"])
            dataset_id = str(row["dataset_id"])
            db_name = str(row["db_name"])
            lakefs_commit_id = str(row["lakefs_commit_id"])
            artifact_key = row["artifact_key"]
            sample_json = coerce_json_dataset(row["sample_json"]) or {}
            schema_json = coerce_json_dataset(row["schema_json"]) or {}
            row_count = row["row_count"]

            try:
                dataset = await registry.get_dataset(dataset_id=dataset_id)
                dataset_name = dataset.name if dataset else ""
                transaction = await registry.get_ingest_transaction(ingest_request_id=ingest_request_id)
                transaction_id = transaction.transaction_id if transaction else None
                from shared.services.events.dataset_ingest_outbox import build_dataset_event_payload

                outbox_entries = [
                    {
                        "kind": "eventstore",
                        "payload": build_dataset_event_payload(
                            event_id=ingest_request_id,
                            event_type="DATASET_VERSION_CREATED",
                            aggregate_type="Dataset",
                            aggregate_id=dataset_id,
                            command_type="INGEST_DATASET_SNAPSHOT",
                            actor=None,
                            data={
                                "dataset_id": dataset_id,
                                "db_name": db_name,
                                "name": dataset_name,
                                "lakefs_commit_id": lakefs_commit_id,
                                "artifact_key": artifact_key,
                                "transaction_id": transaction_id,
                            },
                        ),
                    }
                ]
                await registry.publish_ingest_request(
                    ingest_request_id=ingest_request_id,
                    dataset_id=dataset_id,
                    lakefs_commit_id=lakefs_commit_id,
                    artifact_key=artifact_key,
                    row_count=row_count,
                    sample_json=sample_json,
                    schema_json=schema_json,
                    outbox_entries=outbox_entries,
                )
                results["published"] += 1
            except Exception as exc:
                logger.warning("Failed to reconcile RAW_COMMITTED ingest %s: %s", ingest_request_id, exc)

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT v.version_id, v.dataset_id, v.lakefs_commit_id, v.artifact_key,
                       r.ingest_request_id, r.db_name, r.branch, r.schema_json, r.sample_json, r.row_count,
                       d.name AS dataset_name,
                       t.transaction_id
                FROM {registry._schema}.dataset_versions v
                JOIN {registry._schema}.dataset_ingest_requests r
                  ON r.ingest_request_id = v.ingest_request_id
                JOIN {registry._schema}.datasets d
                  ON d.dataset_id = v.dataset_id
                LEFT JOIN {registry._schema}.dataset_ingest_transactions t
                  ON t.ingest_request_id = r.ingest_request_id
                WHERE r.status = 'PUBLISHED'
                  AND NOT EXISTS (
                    SELECT 1
                    FROM {registry._schema}.dataset_ingest_outbox o
                    WHERE o.ingest_request_id = r.ingest_request_id
                      AND o.status <> 'dead'
                  )
                ORDER BY v.created_at ASC
                LIMIT $1
                """,
                limit,
            )

        for row in rows or []:
            ingest_request_id = str(row["ingest_request_id"])
            dataset_id = str(row["dataset_id"])
            db_name = str(row["db_name"])
            dataset_name = str(row["dataset_name"] or "")
            lakefs_commit_id = str(row["lakefs_commit_id"])
            artifact_key = row["artifact_key"]
            schema_json = coerce_json_dataset(row["schema_json"]) if row["schema_json"] is not None else None
            sample_json = coerce_json_dataset(row["sample_json"]) if row["sample_json"] is not None else None
            row_count = row["row_count"]
            transaction_id = str(row["transaction_id"]) if row["transaction_id"] else None
            from shared.services.events.dataset_ingest_outbox import build_dataset_event_payload

            outbox_entries: list[dict[str, Any]] = [
                {
                    "kind": "eventstore",
                    "payload": build_dataset_event_payload(
                        event_id=ingest_request_id,
                        event_type="DATASET_VERSION_CREATED",
                        aggregate_type="Dataset",
                        aggregate_id=dataset_id,
                        command_type="INGEST_DATASET_SNAPSHOT",
                        actor=None,
                        data={
                            "dataset_id": dataset_id,
                            "db_name": db_name,
                            "name": dataset_name,
                            "lakefs_commit_id": lakefs_commit_id,
                            "artifact_key": artifact_key,
                            "transaction_id": transaction_id,
                        },
                    ),
                }
            ]
            if artifact_key:
                parsed = parse_s3_uri(artifact_key)
                if parsed:
                    bucket, key = parsed
                    outbox_entries.append(
                        {
                            "kind": "lineage",
                            "payload": {
                                "from_node_id": f"event:{ingest_request_id}",
                                "to_node_id": f"artifact:s3:{bucket}:{key}",
                                "edge_type": "dataset_artifact_stored",
                                "occurred_at": utcnow(),
                                "from_label": "ingest_reconciler",
                                "to_label": artifact_key,
                                "db_name": db_name,
                                "edge_metadata": {
                                    "db_name": db_name,
                                    "dataset_id": dataset_id,
                                    "dataset_name": dataset_name,
                                    "bucket": bucket,
                                    "key": key,
                                    "source": "ingest_reconciler",
                                },
                            },
                        }
                    )

            try:
                await registry.publish_ingest_request(
                    ingest_request_id=ingest_request_id,
                    dataset_id=dataset_id,
                    lakefs_commit_id=lakefs_commit_id,
                    artifact_key=artifact_key,
                    row_count=row_count,
                    sample_json=sample_json,
                    schema_json=schema_json,
                    outbox_entries=outbox_entries,
                )
                results["outbox_repaired"] += 1
            except Exception as exc:
                logger.warning("Failed to repair ingest outbox for %s: %s", ingest_request_id, exc)

        async with pool.acquire() as conn:
            repaired = await conn.execute(
                f"""
                UPDATE {registry._schema}.dataset_ingest_transactions t
                SET status = 'COMMITTED',
                    committed_at = COALESCE(committed_at, NOW()),
                    updated_at = NOW()
                FROM {registry._schema}.dataset_ingest_requests r
                WHERE r.ingest_request_id = t.ingest_request_id
                  AND r.status = 'PUBLISHED'
                  AND t.status = 'OPEN'
                """
            )
        if isinstance(repaired, str) and repaired.startswith("UPDATE"):
            try:
                results["committed_tx"] = int(repaired.split()[-1])
            except (IndexError, ValueError) as parse_exc:
                logger.warning("Failed to parse committed transaction update count from '%s': %s", repaired, parse_exc)

        async with pool.acquire() as conn:
            stale_rows = await conn.fetch(
                f"""
                SELECT t.ingest_request_id
                FROM {registry._schema}.dataset_ingest_transactions t
                JOIN {registry._schema}.dataset_ingest_requests r
                  ON r.ingest_request_id = t.ingest_request_id
                WHERE t.status = 'OPEN'
                  AND r.status IN ('RECEIVED', 'FAILED')
                  AND t.updated_at < $1
                ORDER BY t.updated_at ASC
                LIMIT $2
                """,
                cutoff,
                limit,
            )

        for row in stale_rows or []:
            ingest_request_id = str(row["ingest_request_id"])
            try:
                await registry.mark_ingest_failed(
                    ingest_request_id=ingest_request_id,
                        error="reconciler_timeout",
                )
                results["aborted"] += 1
            except Exception as exc:
                logger.warning("Failed to abort stale ingest transaction %s: %s", ingest_request_id, exc)

        return results
    finally:
        if lock_conn is not None:
            try:
                if use_lock and resolved_lock_key is not None:
                    await lock_conn.execute("SELECT pg_advisory_unlock($1)", resolved_lock_key)
            finally:
                await pool.release(lock_conn)


async def update_ingest_request_payload(
    registry: "DatasetRegistry",
    *,
    ingest_request_id: str,
    schema_json: Optional[Dict[str, Any]] = None,
    sample_json: Optional[Dict[str, Any]] = None,
    row_count: Optional[int] = None,
    source_metadata: Optional[Dict[str, Any]] = None,
) -> None:
    pool = _require_pool(registry)
    schema_payload = normalize_json_payload(schema_json) if schema_json is not None else None
    sample_payload = normalize_json_payload(sample_json) if sample_json is not None else None
    source_payload = normalize_json_payload(source_metadata) if source_metadata is not None else None
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            UPDATE {registry._schema}.dataset_ingest_requests
            SET schema_json = COALESCE($2::jsonb, schema_json),
                sample_json = COALESCE($3::jsonb, sample_json),
                row_count = COALESCE($4, row_count),
                source_metadata = COALESCE($5::jsonb, source_metadata),
                updated_at = NOW()
            WHERE ingest_request_id = $1::uuid
            """,
            ingest_request_id,
            schema_payload,
            sample_payload,
            row_count,
            source_payload,
        )


async def approve_ingest_schema(
    registry: "DatasetRegistry",
    *,
    ingest_request_id: str,
    schema_json: Optional[Dict[str, Any]] = None,
    approved_by: Optional[str] = None,
) -> tuple[Any, DatasetIngestRequestRecord]:
    pool = _require_pool(registry)
    approved_at = utcnow()

    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                f"""
                SELECT ingest_request_id, dataset_id, db_name, branch, idempotency_key, request_fingerprint,
                       status, lakefs_commit_id, artifact_key, schema_json, schema_status,
                       schema_approved_at, schema_approved_by,
                       sample_json, row_count, source_metadata, error, created_at, updated_at, published_at
                FROM {registry._schema}.dataset_ingest_requests
                WHERE ingest_request_id = $1::uuid
                FOR UPDATE
                """,
                ingest_request_id,
            )
            if not row:
                raise RuntimeError("Ingest request not found")
            payload = schema_json if schema_json is not None else coerce_json_dataset(row["schema_json"])
            payload = payload or {}
            if not isinstance(payload, dict):
                raise ValueError("schema_json must be an object")
            if not payload:
                raise ValueError("schema_json is required to approve")
            payload_json = normalize_json_payload(payload)

            dataset_id = str(row["dataset_id"])
            await conn.execute(
                f"""
                UPDATE {registry._schema}.datasets
                SET schema_json = $2::jsonb, updated_at = NOW()
                WHERE dataset_id = $1::uuid
                """,
                dataset_id,
                payload_json,
            )
            await conn.execute(
                f"""
                UPDATE {registry._schema}.dataset_ingest_requests
                SET schema_json = $2::jsonb,
                    schema_status = 'APPROVED',
                    schema_approved_at = $4,
                    schema_approved_by = $3,
                    updated_at = NOW()
                WHERE ingest_request_id = $1::uuid
                """,
                ingest_request_id,
                payload_json,
                approved_by,
                approved_at,
            )
            dataset_row = await conn.fetchrow(
                f"""
                SELECT dataset_id, db_name, name, description, source_type,
                       source_ref, branch, schema_json, created_at, updated_at
                FROM {registry._schema}.datasets
                WHERE dataset_id = $1::uuid
                """,
                dataset_id,
            )
            if not dataset_row:
                raise RuntimeError("Dataset not found for ingest request")

    dataset_record = row_to_dataset(dataset_row)
    updated_request = DatasetIngestRequestRecord(
        ingest_request_id=str(row["ingest_request_id"]),
        dataset_id=str(row["dataset_id"]),
        db_name=row["db_name"],
        branch=row["branch"],
        idempotency_key=row["idempotency_key"],
        request_fingerprint=row["request_fingerprint"],
        status=row["status"],
        lakefs_commit_id=row["lakefs_commit_id"],
        artifact_key=row["artifact_key"],
        schema_json=payload,
        schema_status="APPROVED",
        schema_approved_at=approved_at,
        schema_approved_by=approved_by,
        sample_json=coerce_json_dataset(row["sample_json"]) or {},
        row_count=row["row_count"],
        source_metadata=coerce_json_dataset(row["source_metadata"]) or {},
        error=row["error"],
        created_at=row["created_at"],
        updated_at=dataset_row["updated_at"],
        published_at=row["published_at"],
    )
    return dataset_record, updated_request
