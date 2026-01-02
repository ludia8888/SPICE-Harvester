"""
Dataset Registry (Foundry-style) - durable dataset metadata in Postgres.

Stores dataset metadata + versions (artifact references + samples).
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import asyncpg

from shared.config.service_config import ServiceConfig
from shared.observability.context_propagation import enrich_metadata_with_current_trace
from shared.utils.s3_uri import is_s3_uri, parse_s3_uri
from shared.utils.json_utils import coerce_json_dataset, normalize_json_payload
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class DatasetRecord:
    dataset_id: str
    db_name: str
    name: str
    description: Optional[str]
    source_type: str
    source_ref: Optional[str]
    branch: str
    schema_json: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class DatasetVersionRecord:
    version_id: str
    dataset_id: str
    lakefs_commit_id: str
    artifact_key: Optional[str]
    row_count: Optional[int]
    sample_json: Dict[str, Any]
    ingest_request_id: Optional[str]
    promoted_from_artifact_id: Optional[str]
    created_at: datetime


@dataclass(frozen=True)
class DatasetIngestRequestRecord:
    ingest_request_id: str
    dataset_id: str
    db_name: str
    branch: str
    idempotency_key: str
    request_fingerprint: Optional[str]
    status: str
    lakefs_commit_id: Optional[str]
    artifact_key: Optional[str]
    schema_json: Dict[str, Any]
    sample_json: Dict[str, Any]
    row_count: Optional[int]
    source_metadata: Dict[str, Any]
    error: Optional[str]
    created_at: datetime
    updated_at: datetime
    published_at: Optional[datetime]


@dataclass(frozen=True)
class DatasetIngestTransactionRecord:
    transaction_id: str
    ingest_request_id: str
    status: str
    lakefs_commit_id: Optional[str]
    artifact_key: Optional[str]
    error: Optional[str]
    created_at: datetime
    updated_at: datetime
    committed_at: Optional[datetime]
    aborted_at: Optional[datetime]


@dataclass(frozen=True)
class DatasetIngestOutboxItem:
    outbox_id: str
    ingest_request_id: str
    kind: str
    payload: Dict[str, Any]
    status: str
    publish_attempts: int
    retry_count: int
    error: Optional[str]
    last_error: Optional[str]
    claimed_by: Optional[str]
    claimed_at: Optional[datetime]
    next_attempt_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


def _inject_dataset_version(outbox_entries: List[Dict[str, Any]], dataset_version_id: str) -> None:
    """
    Ensure dataset_version_id is propagated into outbox payloads that depend on it.

    This allows outbox-based reconciliation to recover lineage/event metadata
    even if the ingest job crashes between lakeFS commit and registry publish.
    """
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


class DatasetRegistry:
    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_datasets",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
    ):
        self._dsn = dsn or ServiceConfig.get_postgres_url()
        self._schema = schema
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_min = int(os.getenv("DATASET_REGISTRY_PG_POOL_MIN", str(pool_min or 1)))
        self._pool_max = int(os.getenv("DATASET_REGISTRY_PG_POOL_MAX", str(pool_max or 5)))

    async def initialize(self) -> None:
        await self.connect()

    async def connect(self) -> None:
        if self._pool:
            return
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._pool_min,
            max_size=self._pool_max,
            command_timeout=int(os.getenv("DATASET_REGISTRY_PG_COMMAND_TIMEOUT", "30")),
        )
        await self.ensure_schema()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def ensure_schema(self) -> None:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")

        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.datasets (
                    dataset_id UUID PRIMARY KEY,
                    db_name TEXT NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    source_type TEXT NOT NULL,
                    source_ref TEXT,
                    branch TEXT NOT NULL DEFAULT 'main',
                    schema_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (db_name, name, branch)
                )
                """
            )

            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.datasets
                    ADD COLUMN IF NOT EXISTS branch TEXT NOT NULL DEFAULT 'main'
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.datasets
                    DROP CONSTRAINT IF EXISTS datasets_db_name_name_key
                """
            )
            await conn.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS datasets_db_name_name_branch_key
                ON {self._schema}.datasets(db_name, name, branch)
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.dataset_versions (
                    version_id UUID PRIMARY KEY,
                    dataset_id UUID NOT NULL,
                    lakefs_commit_id TEXT NOT NULL,
                    artifact_key TEXT,
                    row_count INTEGER,
                    sample_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    ingest_request_id UUID,
                    promoted_from_artifact_id UUID,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (dataset_id, lakefs_commit_id),
                    FOREIGN KEY (dataset_id)
                        REFERENCES {self._schema}.datasets(dataset_id)
                        ON DELETE CASCADE
                )
                """
            )

            # Migrate legacy integer versioning to lakeFS commit ids.
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.dataset_versions
                    ADD COLUMN IF NOT EXISTS lakefs_commit_id TEXT
                """
            )
            # Best-effort backfill:
            # - If artifact_key is an s3:// URI, the second path segment is the ref (commit/branch).
            # - Otherwise, synthesize a stable legacy identifier so existing rows remain addressable.
            await conn.execute(
                f"""
                UPDATE {self._schema}.dataset_versions
                SET lakefs_commit_id = COALESCE(
                    NULLIF(lakefs_commit_id, ''),
                    NULLIF(
                        CASE
                            WHEN split_part(replace(COALESCE(artifact_key, ''), 's3://', ''), '/', 2)
                                 IN ('', 'datasets', 'pipelines', 'pipelines-staging', 'checkpoints', 'events', 'indexes')
                            THEN NULL
                            ELSE split_part(replace(COALESCE(artifact_key, ''), 's3://', ''), '/', 2)
                        END,
                        ''
                    ),
                    'legacy-' || version_id::text
                )
                WHERE lakefs_commit_id IS NULL OR lakefs_commit_id = ''
                """
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.dataset_versions DROP CONSTRAINT IF EXISTS dataset_versions_dataset_id_version_key"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.dataset_versions DROP COLUMN IF EXISTS version"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.dataset_versions ALTER COLUMN lakefs_commit_id SET NOT NULL"
            )
            await conn.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS dataset_versions_dataset_id_lakefs_commit_id_key
                ON {self._schema}.dataset_versions(dataset_id, lakefs_commit_id)
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.dataset_versions
                    ADD COLUMN IF NOT EXISTS ingest_request_id UUID
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.dataset_versions
                    ADD COLUMN IF NOT EXISTS promoted_from_artifact_id UUID
                """
            )
            await conn.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS dataset_versions_ingest_request_id_key
                ON {self._schema}.dataset_versions(ingest_request_id)
                WHERE ingest_request_id IS NOT NULL
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_dataset_versions_ingest_request_id
                ON {self._schema}.dataset_versions(ingest_request_id)
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_dataset_versions_promoted_artifact
                ON {self._schema}.dataset_versions(promoted_from_artifact_id)
                """
            )

            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_datasets_db_name ON {self._schema}.datasets(db_name)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_dataset_versions_dataset_id ON {self._schema}.dataset_versions(dataset_id)"
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.dataset_ingest_requests (
                    ingest_request_id UUID PRIMARY KEY,
                    dataset_id UUID NOT NULL,
                    db_name TEXT NOT NULL,
                    branch TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    request_fingerprint TEXT,
                    status TEXT NOT NULL,
                    lakefs_commit_id TEXT,
                    artifact_key TEXT,
                    schema_json JSONB,
                    sample_json JSONB,
                    row_count INTEGER,
                    source_metadata JSONB,
                    error TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    published_at TIMESTAMPTZ,
                    UNIQUE (idempotency_key),
                    FOREIGN KEY (dataset_id)
                        REFERENCES {self._schema}.datasets(dataset_id)
                        ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_dataset_ingest_requests_status
                ON {self._schema}.dataset_ingest_requests(status, created_at)
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.dataset_ingest_requests
                    ADD COLUMN IF NOT EXISTS schema_json JSONB
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.dataset_ingest_requests
                    ADD COLUMN IF NOT EXISTS sample_json JSONB
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.dataset_ingest_requests
                    ADD COLUMN IF NOT EXISTS row_count INTEGER
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.dataset_ingest_requests
                    ADD COLUMN IF NOT EXISTS source_metadata JSONB
                """
            )
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.dataset_ingest_transactions (
                    transaction_id UUID PRIMARY KEY,
                    ingest_request_id UUID NOT NULL,
                    status TEXT NOT NULL,
                    lakefs_commit_id TEXT,
                    artifact_key TEXT,
                    error TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    committed_at TIMESTAMPTZ,
                    aborted_at TIMESTAMPTZ,
                    UNIQUE (ingest_request_id),
                    FOREIGN KEY (ingest_request_id)
                        REFERENCES {self._schema}.dataset_ingest_requests(ingest_request_id)
                        ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_dataset_ingest_transactions_status
                ON {self._schema}.dataset_ingest_transactions(status, created_at)
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.dataset_ingest_outbox (
                    outbox_id UUID PRIMARY KEY,
                    ingest_request_id UUID NOT NULL,
                    kind TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    publish_attempts INTEGER NOT NULL DEFAULT 0,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    last_error TEXT,
                    claimed_by TEXT,
                    claimed_at TIMESTAMPTZ,
                    next_attempt_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    FOREIGN KEY (ingest_request_id)
                        REFERENCES {self._schema}.dataset_ingest_requests(ingest_request_id)
                        ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.dataset_ingest_outbox
                    ADD COLUMN IF NOT EXISTS claimed_by TEXT,
                    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ,
                    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ,
                    ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS last_error TEXT
                """
            )
            await conn.execute(
                f"""
                UPDATE {self._schema}.dataset_ingest_outbox
                SET retry_count = GREATEST(retry_count, publish_attempts),
                    last_error = COALESCE(last_error, error)
                WHERE retry_count = 0 AND publish_attempts > 0
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_dataset_ingest_outbox_status
                ON {self._schema}.dataset_ingest_outbox(status, created_at)
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_dataset_ingest_outbox_status_next
                ON {self._schema}.dataset_ingest_outbox(status, next_attempt_at, created_at)
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_dataset_ingest_outbox_claimed
                ON {self._schema}.dataset_ingest_outbox(status, claimed_at)
                """
            )

    async def create_dataset(
        self,
        *,
        db_name: str,
        name: str,
        description: Optional[str],
        source_type: str,
        source_ref: Optional[str] = None,
        schema_json: Optional[Dict[str, Any]] = None,
        branch: str = "main",
        dataset_id: Optional[str] = None,
    ) -> DatasetRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")

        dataset_id = dataset_id or str(uuid4())
        schema_json = schema_json or {}
        schema_payload = normalize_json_payload(schema_json)

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.datasets (
                    dataset_id, db_name, name, description, source_type, source_ref, branch, schema_json
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                RETURNING dataset_id, db_name, name, description, source_type, source_ref,
                          branch, schema_json, created_at, updated_at
                """,
                dataset_id,
                db_name,
                name,
                description,
                source_type,
                source_ref,
                branch,
                schema_payload,
            )
            if not row:
                raise RuntimeError("Failed to create dataset")
            return DatasetRecord(
                dataset_id=str(row["dataset_id"]),
                db_name=str(row["db_name"]),
                name=str(row["name"]),
                description=row["description"],
                source_type=str(row["source_type"]),
                source_ref=row["source_ref"],
                branch=str(row["branch"]),
                schema_json=coerce_json_dataset(row["schema_json"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def list_datasets(self, *, db_name: str, branch: Optional[str] = None) -> List[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")

        clause = "WHERE d.db_name = $1"
        values: List[Any] = [db_name]
        if branch:
            clause += f" AND d.branch = ${len(values) + 1}"
            values.append(branch)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT d.dataset_id, d.db_name, d.name, d.description, d.source_type,
                       d.source_ref, d.branch, d.schema_json, d.created_at, d.updated_at,
                       v.lakefs_commit_id AS latest_commit_id, v.artifact_key, v.row_count,
                       v.sample_json, v.created_at AS version_created_at
                FROM {self._schema}.datasets d
                LEFT JOIN LATERAL (
                    SELECT lakefs_commit_id, artifact_key, row_count, sample_json, created_at
                    FROM {self._schema}.dataset_versions
                    WHERE dataset_id = d.dataset_id
                    ORDER BY created_at DESC
                    LIMIT 1
                ) v ON TRUE
                {clause}
                ORDER BY d.updated_at DESC
                """,
                *values,
            )

        output: List[Dict[str, Any]] = []
        for row in rows or []:
            output.append(
                {
                    "dataset_id": str(row["dataset_id"]),
                    "db_name": str(row["db_name"]),
                    "name": str(row["name"]),
                    "description": row["description"],
                    "source_type": str(row["source_type"]),
                    "source_ref": row["source_ref"],
                    "branch": str(row["branch"]),
                    "schema_json": coerce_json_dataset(row["schema_json"]),
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "latest_commit_id": row["latest_commit_id"],
                    "artifact_key": row["artifact_key"],
                    "row_count": row["row_count"],
                    "sample_json": coerce_json_dataset(row["sample_json"]),
                    "version_created_at": row["version_created_at"],
                }
            )
        return output

    async def get_dataset(self, *, dataset_id: str) -> Optional[DatasetRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT dataset_id, db_name, name, description, source_type,
                       source_ref, branch, schema_json, created_at, updated_at
                FROM {self._schema}.datasets
                WHERE dataset_id = $1
                """,
                dataset_id,
            )
            if not row:
                return None
            return DatasetRecord(
                dataset_id=str(row["dataset_id"]),
                db_name=str(row["db_name"]),
                name=str(row["name"]),
                description=row["description"],
                source_type=str(row["source_type"]),
                source_ref=row["source_ref"],
                branch=str(row["branch"]),
                schema_json=coerce_json_dataset(row["schema_json"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def get_dataset_by_name(
        self,
        *,
        db_name: str,
        name: str,
        branch: str = "main",
    ) -> Optional[DatasetRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT dataset_id, db_name, name, description, source_type,
                       source_ref, branch, schema_json, created_at, updated_at
                FROM {self._schema}.datasets
                WHERE db_name = $1 AND name = $2 AND branch = $3
                """,
                db_name,
                name,
                branch,
            )
            if not row:
                return None
            return DatasetRecord(
                dataset_id=str(row["dataset_id"]),
                db_name=str(row["db_name"]),
                name=str(row["name"]),
                description=row["description"],
                source_type=str(row["source_type"]),
                source_ref=row["source_ref"],
                branch=str(row["branch"]),
                schema_json=coerce_json_dataset(row["schema_json"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def get_dataset_by_source_ref(
        self,
        *,
        db_name: str,
        source_type: str,
        source_ref: str,
        branch: str = "main",
    ) -> Optional[DatasetRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT dataset_id, db_name, name, description, source_type,
                       source_ref, branch, schema_json, created_at, updated_at
                FROM {self._schema}.datasets
                WHERE db_name = $1 AND source_type = $2 AND source_ref = $3 AND branch = $4
                """,
                db_name,
                source_type,
                source_ref,
                branch,
            )
            if not row:
                return None
            return DatasetRecord(
                dataset_id=str(row["dataset_id"]),
                db_name=str(row["db_name"]),
                name=str(row["name"]),
                description=row["description"],
                source_type=str(row["source_type"]),
                source_ref=row["source_ref"],
                branch=str(row["branch"]),
                schema_json=coerce_json_dataset(row["schema_json"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def add_version(
        self,
        *,
        dataset_id: str,
        lakefs_commit_id: str,
        artifact_key: Optional[str],
        row_count: Optional[int],
        sample_json: Optional[Dict[str, Any]] = None,
        schema_json: Optional[Dict[str, Any]] = None,
        version_id: Optional[str] = None,
        ingest_request_id: Optional[str] = None,
        promoted_from_artifact_id: Optional[str] = None,
    ) -> DatasetVersionRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        version_id = version_id or str(uuid4())
        sample_json = sample_json or {}
        sample_payload = normalize_json_payload(sample_json)
        lakefs_commit_id = str(lakefs_commit_id or "").strip()
        if not lakefs_commit_id:
            raise ValueError("lakefs_commit_id is required")
        if artifact_key:
            artifact_key = artifact_key.strip()
            if artifact_key and not is_s3_uri(artifact_key):
                raise ValueError("artifact_key must be an s3:// URI")
        schema_payload = None
        if schema_json is not None:
            schema_payload = normalize_json_payload(schema_json)

        async with self._pool.acquire() as conn:
            if schema_payload is not None:
                await conn.execute(
                    f"""
                    UPDATE {self._schema}.datasets
                    SET schema_json = $2::jsonb, updated_at = NOW()
                    WHERE dataset_id = $1
                    """,
                    dataset_id,
                    schema_payload,
                )
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.dataset_versions (
                    version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                    ingest_request_id, promoted_from_artifact_id
                ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::uuid, $8::uuid)
                RETURNING version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                          ingest_request_id, promoted_from_artifact_id, created_at
                """,
                version_id,
                dataset_id,
                lakefs_commit_id,
                artifact_key,
                row_count,
                sample_payload,
                ingest_request_id,
                promoted_from_artifact_id,
            )
            if not row:
                raise RuntimeError("Failed to create dataset version")
            return DatasetVersionRecord(
                version_id=str(row["version_id"]),
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

    async def get_latest_version(self, *, dataset_id: str) -> Optional[DatasetVersionRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                       ingest_request_id, promoted_from_artifact_id, created_at
                FROM {self._schema}.dataset_versions
                WHERE dataset_id = $1
                ORDER BY created_at DESC
                LIMIT 1
                """,
                dataset_id,
            )
            if not row:
                return None
            return DatasetVersionRecord(
                version_id=str(row["version_id"]),
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

    async def get_version(self, *, version_id: str) -> Optional[DatasetVersionRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                       ingest_request_id, promoted_from_artifact_id, created_at
                FROM {self._schema}.dataset_versions
                WHERE version_id = $1::uuid
                """,
                version_id,
            )
            if not row:
                return None
            return DatasetVersionRecord(
                version_id=str(row["version_id"]),
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

    async def get_version_by_ingest_request(
        self,
        *,
        ingest_request_id: str,
    ) -> Optional[DatasetVersionRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                       ingest_request_id, promoted_from_artifact_id, created_at
                FROM {self._schema}.dataset_versions
                WHERE ingest_request_id = $1::uuid
                """,
                ingest_request_id,
            )
            if not row:
                return None
            return DatasetVersionRecord(
                version_id=str(row["version_id"]),
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

    async def get_ingest_request_by_key(
        self,
        *,
        idempotency_key: str,
    ) -> Optional[DatasetIngestRequestRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT ingest_request_id, dataset_id, db_name, branch, idempotency_key, request_fingerprint,
                       status, lakefs_commit_id, artifact_key, schema_json, sample_json, row_count,
                       source_metadata, error, created_at, updated_at, published_at
                FROM {self._schema}.dataset_ingest_requests
                WHERE idempotency_key = $1
                """,
                idempotency_key,
            )
            if not row:
                return None
            return DatasetIngestRequestRecord(
                ingest_request_id=str(row["ingest_request_id"]),
                dataset_id=str(row["dataset_id"]),
                db_name=row["db_name"],
                branch=row["branch"],
                idempotency_key=row["idempotency_key"],
                request_fingerprint=row["request_fingerprint"],
                status=row["status"],
                lakefs_commit_id=row["lakefs_commit_id"],
                artifact_key=row["artifact_key"],
                schema_json=coerce_json_dataset(row["schema_json"]) or {},
                sample_json=coerce_json_dataset(row["sample_json"]) or {},
                row_count=row["row_count"],
                source_metadata=coerce_json_dataset(row["source_metadata"]) or {},
                error=row["error"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                published_at=row["published_at"],
            )

    async def create_ingest_request(
        self,
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
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        ingest_request_id = str(uuid4())
        schema_payload = normalize_json_payload(schema_json) if schema_json is not None else None
        sample_payload = normalize_json_payload(sample_json) if sample_json is not None else None
        source_payload = normalize_json_payload(source_metadata) if source_metadata is not None else None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.dataset_ingest_requests (
                    ingest_request_id, dataset_id, db_name, branch, idempotency_key,
                    request_fingerprint, status, schema_json, sample_json, row_count, source_metadata
                ) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, 'RECEIVED', $7::jsonb, $8::jsonb, $9, $10::jsonb)
                ON CONFLICT (idempotency_key) DO UPDATE
                SET updated_at = NOW(),
                    schema_json = COALESCE(EXCLUDED.schema_json, {self._schema}.dataset_ingest_requests.schema_json),
                    sample_json = COALESCE(EXCLUDED.sample_json, {self._schema}.dataset_ingest_requests.sample_json),
                    row_count = COALESCE(EXCLUDED.row_count, {self._schema}.dataset_ingest_requests.row_count),
                    source_metadata = COALESCE(EXCLUDED.source_metadata, {self._schema}.dataset_ingest_requests.source_metadata)
                RETURNING ingest_request_id, dataset_id, db_name, branch, idempotency_key, request_fingerprint,
                          status, lakefs_commit_id, artifact_key, schema_json, sample_json, row_count,
                          source_metadata, error, created_at, updated_at, published_at
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
            record = DatasetIngestRequestRecord(
                ingest_request_id=str(row["ingest_request_id"]),
                dataset_id=str(row["dataset_id"]),
                db_name=row["db_name"],
                branch=row["branch"],
                idempotency_key=row["idempotency_key"],
                request_fingerprint=row["request_fingerprint"],
                status=row["status"],
                lakefs_commit_id=row["lakefs_commit_id"],
                artifact_key=row["artifact_key"],
                schema_json=coerce_json_dataset(row["schema_json"]) or {},
                sample_json=coerce_json_dataset(row["sample_json"]) or {},
                row_count=row["row_count"],
                source_metadata=coerce_json_dataset(row["source_metadata"]) or {},
                error=row["error"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                published_at=row["published_at"],
            )
            is_new = record.ingest_request_id == ingest_request_id
            return record, is_new

    async def get_ingest_transaction(
        self,
        *,
        ingest_request_id: str,
    ) -> Optional[DatasetIngestTransactionRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT transaction_id, ingest_request_id, status, lakefs_commit_id, artifact_key,
                       error, created_at, updated_at, committed_at, aborted_at
                FROM {self._schema}.dataset_ingest_transactions
                WHERE ingest_request_id = $1::uuid
                """,
                ingest_request_id,
            )
            if not row:
                return None
            return DatasetIngestTransactionRecord(
                transaction_id=str(row["transaction_id"]),
                ingest_request_id=str(row["ingest_request_id"]),
                status=row["status"],
                lakefs_commit_id=row["lakefs_commit_id"],
                artifact_key=row["artifact_key"],
                error=row["error"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                committed_at=row["committed_at"],
                aborted_at=row["aborted_at"],
            )

    async def create_ingest_transaction(
        self,
        *,
        ingest_request_id: str,
        status: str = "OPEN",
    ) -> DatasetIngestTransactionRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        transaction_id = str(uuid4())
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.dataset_ingest_transactions (
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
            return DatasetIngestTransactionRecord(
                transaction_id=str(row["transaction_id"]),
                ingest_request_id=str(row["ingest_request_id"]),
                status=row["status"],
                lakefs_commit_id=row["lakefs_commit_id"],
                artifact_key=row["artifact_key"],
                error=row["error"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                committed_at=row["committed_at"],
                aborted_at=row["aborted_at"],
            )

    async def mark_ingest_transaction_committed(
        self,
        *,
        ingest_request_id: str,
        lakefs_commit_id: str,
        artifact_key: Optional[str],
    ) -> Optional[DatasetIngestTransactionRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.dataset_ingest_transactions
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
            if not row:
                return None
            return DatasetIngestTransactionRecord(
                transaction_id=str(row["transaction_id"]),
                ingest_request_id=str(row["ingest_request_id"]),
                status=row["status"],
                lakefs_commit_id=row["lakefs_commit_id"],
                artifact_key=row["artifact_key"],
                error=row["error"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                committed_at=row["committed_at"],
                aborted_at=row["aborted_at"],
            )

    async def mark_ingest_transaction_aborted(
        self,
        *,
        ingest_request_id: str,
        error: str,
    ) -> Optional[DatasetIngestTransactionRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.dataset_ingest_transactions
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
            if not row:
                return None
            return DatasetIngestTransactionRecord(
                transaction_id=str(row["transaction_id"]),
                ingest_request_id=str(row["ingest_request_id"]),
                status=row["status"],
                lakefs_commit_id=row["lakefs_commit_id"],
                artifact_key=row["artifact_key"],
                error=row["error"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                committed_at=row["committed_at"],
                aborted_at=row["aborted_at"],
            )

    async def mark_ingest_committed(
        self,
        *,
        ingest_request_id: str,
        lakefs_commit_id: str,
        artifact_key: Optional[str],
    ) -> DatasetIngestRequestRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.dataset_ingest_requests
                SET status = 'RAW_COMMITTED',
                    lakefs_commit_id = $2,
                    artifact_key = $3,
                    updated_at = NOW()
                WHERE ingest_request_id = $1::uuid
                  AND (lakefs_commit_id IS NULL OR lakefs_commit_id = '')
                RETURNING ingest_request_id, dataset_id, db_name, branch, idempotency_key, request_fingerprint,
                          status, lakefs_commit_id, artifact_key, schema_json, sample_json, row_count,
                          source_metadata, error, created_at, updated_at, published_at
                """,
                ingest_request_id,
                lakefs_commit_id,
                artifact_key,
            )
            if not row:
                row = await conn.fetchrow(
                    f"""
                    SELECT ingest_request_id, dataset_id, db_name, branch, idempotency_key, request_fingerprint,
                           status, lakefs_commit_id, artifact_key, schema_json, sample_json, row_count,
                           source_metadata, error, created_at, updated_at, published_at
                    FROM {self._schema}.dataset_ingest_requests
                    WHERE ingest_request_id = $1::uuid
                    """,
                    ingest_request_id,
                )
            if not row:
                raise RuntimeError("Ingest request not found")
            return DatasetIngestRequestRecord(
                ingest_request_id=str(row["ingest_request_id"]),
                dataset_id=str(row["dataset_id"]),
                db_name=row["db_name"],
                branch=row["branch"],
                idempotency_key=row["idempotency_key"],
                request_fingerprint=row["request_fingerprint"],
                status=row["status"],
                lakefs_commit_id=row["lakefs_commit_id"],
                artifact_key=row["artifact_key"],
                schema_json=coerce_json_dataset(row["schema_json"]) or {},
                sample_json=coerce_json_dataset(row["sample_json"]) or {},
                row_count=row["row_count"],
                source_metadata=coerce_json_dataset(row["source_metadata"]) or {},
                error=row["error"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                published_at=row["published_at"],
            )

    async def mark_ingest_failed(
        self,
        *,
        ingest_request_id: str,
        error: str,
    ) -> None:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.dataset_ingest_requests
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
                UPDATE {self._schema}.dataset_ingest_transactions
                SET status = 'ABORTED',
                    error = $2,
                    aborted_at = COALESCE(aborted_at, NOW()),
                    updated_at = NOW()
                WHERE ingest_request_id = $1::uuid
                """,
                ingest_request_id,
                error,
            )

    async def update_ingest_request_payload(
        self,
        *,
        ingest_request_id: str,
        schema_json: Optional[Dict[str, Any]] = None,
        sample_json: Optional[Dict[str, Any]] = None,
        row_count: Optional[int] = None,
        source_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        schema_payload = normalize_json_payload(schema_json) if schema_json is not None else None
        sample_payload = normalize_json_payload(sample_json) if sample_json is not None else None
        source_payload = normalize_json_payload(source_metadata) if source_metadata is not None else None
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.dataset_ingest_requests
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

    async def publish_ingest_request(
        self,
        *,
        ingest_request_id: str,
        dataset_id: str,
        lakefs_commit_id: str,
        artifact_key: Optional[str],
        row_count: Optional[int],
        sample_json: Optional[Dict[str, Any]],
        schema_json: Optional[Dict[str, Any]],
        outbox_entries: Optional[List[Dict[str, Any]]] = None,
    ) -> DatasetVersionRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        sample_json = sample_json or {}
        sample_payload = normalize_json_payload(sample_json)
        schema_payload = None
        if schema_json is not None:
            schema_payload = normalize_json_payload(schema_json)

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if schema_payload is not None:
                    await conn.execute(
                        f"""
                        UPDATE {self._schema}.datasets
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
                    FROM {self._schema}.dataset_versions
                    WHERE ingest_request_id = $1::uuid
                    """,
                    ingest_request_id,
                )
                if existing:
                    dataset_version_id = str(existing["version_id"])
                    await conn.execute(
                        f"""
                        UPDATE {self._schema}.dataset_ingest_requests
                        SET status = 'PUBLISHED',
                            published_at = COALESCE(published_at, NOW()),
                            updated_at = NOW()
                        WHERE ingest_request_id = $1::uuid
                        """,
                        ingest_request_id,
                    )
                    await conn.execute(
                        f"""
                        UPDATE {self._schema}.dataset_ingest_transactions
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
                            SELECT 1 FROM {self._schema}.dataset_ingest_outbox
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
                                    INSERT INTO {self._schema}.dataset_ingest_outbox (
                                        outbox_id, ingest_request_id, kind, payload, status
                                    ) VALUES ($1::uuid, $2::uuid, $3, $4::jsonb, 'pending')
                                    """,
                                    str(uuid4()),
                                    ingest_request_id,
                                    str(entry.get("kind") or "eventstore"),
                                    normalize_json_payload(payload),
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
                            INSERT INTO {self._schema}.dataset_versions (
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
                        FROM {self._schema}.dataset_versions
                        WHERE dataset_id = $1 AND lakefs_commit_id = $2
                        LIMIT 1
                        """,
                        dataset_id,
                        lakefs_commit_id,
                    )
                    if not existing:
                        raise
                    dataset_version_id = str(existing["version_id"])
                    await conn.execute(
                        f"""
                        UPDATE {self._schema}.dataset_ingest_requests
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
                        UPDATE {self._schema}.dataset_ingest_transactions
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
                            SELECT 1 FROM {self._schema}.dataset_ingest_outbox
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
                                    INSERT INTO {self._schema}.dataset_ingest_outbox (
                                        outbox_id, ingest_request_id, kind, payload, status
                                    ) VALUES ($1::uuid, $2::uuid, $3, $4::jsonb, 'pending')
                                    """,
                                    str(uuid4()),
                                    ingest_request_id,
                                    str(entry.get("kind") or "eventstore"),
                                    normalize_json_payload(payload),
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
                await conn.execute(
                    f"""
                    UPDATE {self._schema}.dataset_ingest_requests
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
                    UPDATE {self._schema}.dataset_ingest_transactions
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
                            INSERT INTO {self._schema}.dataset_ingest_outbox (
                                outbox_id, ingest_request_id, kind, payload, status
                            ) VALUES ($1::uuid, $2::uuid, $3, $4::jsonb, 'pending')
                            """,
                            str(uuid4()),
                            ingest_request_id,
                            str(entry.get("kind") or "eventstore"),
                            normalize_json_payload(payload),
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
        self,
        *,
        limit: int = 50,
        claimed_by: Optional[str] = None,
        claim_timeout_seconds: int = 300,
    ) -> List[DatasetIngestOutboxItem]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
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
                    FROM {self._schema}.dataset_ingest_outbox
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
                    UPDATE {self._schema}.dataset_ingest_outbox
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

    async def mark_ingest_outbox_published(self, *, outbox_id: str) -> None:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.dataset_ingest_outbox
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
        self,
        *,
        outbox_id: str,
        error: str,
        next_attempt_at: Optional[datetime] = None,
    ) -> None:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.dataset_ingest_outbox
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

    async def mark_ingest_outbox_dead(self, *, outbox_id: str, error: str) -> None:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.dataset_ingest_outbox
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
        self,
        *,
        retention_days: int = 7,
        limit: int = 10_000,
    ) -> int:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        retention_days = max(1, int(retention_days))
        limit = max(1, int(limit))
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                WITH doomed AS (
                    SELECT ctid
                    FROM {self._schema}.dataset_ingest_outbox
                    WHERE status = 'published'
                      AND updated_at < NOW() - ($1::int * INTERVAL '1 day')
                    ORDER BY updated_at ASC
                    LIMIT $2
                )
                DELETE FROM {self._schema}.dataset_ingest_outbox
                WHERE ctid IN (SELECT ctid FROM doomed)
                RETURNING 1
                """,
                retention_days,
                limit,
            )
        return len(rows or [])

    async def get_ingest_outbox_metrics(self) -> Dict[str, Any]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT status, count(*) AS count, MIN(created_at) AS oldest_created_at,
                       MIN(next_attempt_at) AS next_attempt_at,
                       MAX(retry_count) AS max_retry
                FROM {self._schema}.dataset_ingest_outbox
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
        next_attempt = min(
            (row["next_attempt_at"] for row in rows or [] if row["next_attempt_at"]), default=None
        )
        now = datetime.now(timezone.utc)
        oldest_age_seconds = None
        if oldest_created:
            try:
                oldest_age_seconds = int((now - oldest_created).total_seconds())
            except Exception:
                oldest_age_seconds = None

        return {
            "counts": counts,
            "backlog": backlog,
            "oldest_created_at": oldest_created.isoformat() if oldest_created else None,
            "oldest_age_seconds": oldest_age_seconds,
            "next_attempt_at": next_attempt.isoformat() if next_attempt else None,
        }

    async def reconcile_ingest_state(
        self,
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
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")

        results = {"published": 0, "outbox_repaired": 0, "aborted": 0, "committed_tx": 0, "skipped": 0}
        cutoff = datetime.utcnow() - timedelta(seconds=max(60, int(stale_after_seconds)))

        lock_conn: Optional[asyncpg.Connection] = None
        resolved_lock_key = lock_key
        if resolved_lock_key is None:
            try:
                resolved_lock_key = int(os.getenv("DATASET_INGEST_RECONCILER_LOCK_KEY", "910214"))
            except ValueError:
                resolved_lock_key = 910214

        try:
            if use_lock and resolved_lock_key is not None:
                lock_conn = await self._pool.acquire()
                locked = await lock_conn.fetchval("SELECT pg_try_advisory_lock($1)", resolved_lock_key)
                if not locked:
                    results["skipped"] = 1
                    return results

            # 1) Publish RAW_COMMITTED ingests that never created a dataset_version.
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    f"""
                    SELECT ingest_request_id, dataset_id, db_name, branch, lakefs_commit_id, artifact_key,
                           schema_json, sample_json, row_count
                    FROM {self._schema}.dataset_ingest_requests
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
                branch = str(row["branch"])
                lakefs_commit_id = str(row["lakefs_commit_id"])
                artifact_key = row["artifact_key"]
                sample_json = coerce_json_dataset(row["sample_json"]) or {}
                schema_json = coerce_json_dataset(row["schema_json"]) or {}
                row_count = row["row_count"]

                try:
                    dataset = await self.get_dataset(dataset_id=dataset_id)
                    dataset_name = dataset.name if dataset else ""
                    transaction = await self.get_ingest_transaction(ingest_request_id=ingest_request_id)
                    transaction_id = transaction.transaction_id if transaction else None
                    from shared.services.dataset_ingest_outbox import build_dataset_event_payload

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
                    await self.publish_ingest_request(
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

            # 1b) Repair published dataset versions missing outbox rows.
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    f"""
                    SELECT v.version_id, v.dataset_id, v.lakefs_commit_id, v.artifact_key,
                           r.ingest_request_id, r.db_name, r.branch, r.schema_json, r.sample_json, r.row_count,
                           d.name AS dataset_name,
                           t.transaction_id
                    FROM {self._schema}.dataset_versions v
                    JOIN {self._schema}.dataset_ingest_requests r
                      ON r.ingest_request_id = v.ingest_request_id
                    JOIN {self._schema}.datasets d
                      ON d.dataset_id = v.dataset_id
                    LEFT JOIN {self._schema}.dataset_ingest_transactions t
                      ON t.ingest_request_id = r.ingest_request_id
                    WHERE r.status = 'PUBLISHED'
                      AND NOT EXISTS (
                        SELECT 1
                        FROM {self._schema}.dataset_ingest_outbox o
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
                from shared.services.dataset_ingest_outbox import build_dataset_event_payload

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
                    await self.publish_ingest_request(
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

            # 2) Repair OPEN transactions for already-published requests.
            async with self._pool.acquire() as conn:
                repaired = await conn.execute(
                    f"""
                    UPDATE {self._schema}.dataset_ingest_transactions t
                    SET status = 'COMMITTED',
                        committed_at = COALESCE(committed_at, NOW()),
                        updated_at = NOW()
                    FROM {self._schema}.dataset_ingest_requests r
                    WHERE r.ingest_request_id = t.ingest_request_id
                      AND r.status = 'PUBLISHED'
                      AND t.status = 'OPEN'
                    """
                )
            if isinstance(repaired, str) and repaired.startswith("UPDATE"):
                try:
                    results["committed_tx"] = int(repaired.split()[-1])
                except Exception:
                    pass

            # 3) Abort stale OPEN transactions.
            async with self._pool.acquire() as conn:
                stale_rows = await conn.fetch(
                    f"""
                    SELECT t.ingest_request_id
                    FROM {self._schema}.dataset_ingest_transactions t
                    JOIN {self._schema}.dataset_ingest_requests r
                      ON r.ingest_request_id = t.ingest_request_id
                    WHERE t.status = 'OPEN'
                      AND t.created_at < $1
                      AND r.status IN ('RECEIVED', 'RAW_COMMITTED')
                    ORDER BY t.created_at ASC
                    LIMIT $2
                    """,
                    cutoff,
                    limit,
                )
            for row in stale_rows or []:
                ingest_request_id = str(row["ingest_request_id"])
                try:
                    await self.mark_ingest_failed(
                        ingest_request_id=ingest_request_id,
                        error="reconciler_timeout",
                    )
                    results["aborted"] += 1
                except Exception as exc:
                    logger.warning("Failed to abort stale ingest %s: %s", ingest_request_id, exc)
        finally:
            if lock_conn is not None:
                if resolved_lock_key is not None:
                    try:
                        await lock_conn.execute("SELECT pg_advisory_unlock($1)", resolved_lock_key)
                    except Exception:
                        logger.warning("Failed to release ingest reconciler lock", exc_info=True)
                await self._pool.release(lock_conn)

        return results
