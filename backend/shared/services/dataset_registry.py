"""
Dataset Registry (Foundry-style) - durable dataset metadata in Postgres.

Stores dataset metadata + versions (artifact references + samples).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

import asyncpg

from shared.config.service_config import ServiceConfig
from shared.utils.s3_uri import is_s3_uri
from shared.utils.json_utils import coerce_json_dataset, normalize_json_payload


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
    error: Optional[str]
    created_at: datetime
    updated_at: datetime


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
                    error TEXT,
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
                CREATE INDEX IF NOT EXISTS idx_dataset_ingest_outbox_status
                ON {self._schema}.dataset_ingest_outbox(status, created_at)
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
                    version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, ingest_request_id
                ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::uuid)
                RETURNING version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, ingest_request_id, created_at
                """,
                version_id,
                dataset_id,
                lakefs_commit_id,
                artifact_key,
                row_count,
                sample_payload,
                ingest_request_id,
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
                created_at=row["created_at"],
            )

    async def get_latest_version(self, *, dataset_id: str) -> Optional[DatasetVersionRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                       ingest_request_id, created_at
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
                created_at=row["created_at"],
            )

    async def get_version(self, *, version_id: str) -> Optional[DatasetVersionRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                       ingest_request_id, created_at
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
                       ingest_request_id, created_at
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
                       status, lakefs_commit_id, artifact_key, error, created_at, updated_at, published_at
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
    ) -> tuple[DatasetIngestRequestRecord, bool]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        ingest_request_id = str(uuid4())
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.dataset_ingest_requests (
                    ingest_request_id, dataset_id, db_name, branch, idempotency_key,
                    request_fingerprint, status
                ) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, 'RECEIVED')
                ON CONFLICT (idempotency_key) DO UPDATE
                SET updated_at = NOW()
                RETURNING ingest_request_id, dataset_id, db_name, branch, idempotency_key, request_fingerprint,
                          status, lakefs_commit_id, artifact_key, error, created_at, updated_at, published_at
                """,
                ingest_request_id,
                dataset_id,
                db_name,
                branch,
                idempotency_key,
                request_fingerprint,
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
                          status, lakefs_commit_id, artifact_key, error, created_at, updated_at, published_at
                """,
                ingest_request_id,
                lakefs_commit_id,
                artifact_key,
            )
            if not row:
                row = await conn.fetchrow(
                    f"""
                    SELECT ingest_request_id, dataset_id, db_name, branch, idempotency_key, request_fingerprint,
                           status, lakefs_commit_id, artifact_key, error, created_at, updated_at, published_at
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
                           ingest_request_id, created_at
                    FROM {self._schema}.dataset_versions
                    WHERE ingest_request_id = $1::uuid
                    """,
                    ingest_request_id,
                )
                if existing:
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
                    return DatasetVersionRecord(
                        version_id=str(existing["version_id"]),
                        dataset_id=str(existing["dataset_id"]),
                        lakefs_commit_id=str(existing["lakefs_commit_id"]),
                        artifact_key=existing["artifact_key"],
                        row_count=existing["row_count"],
                        sample_json=coerce_json_dataset(existing["sample_json"]),
                        ingest_request_id=str(existing["ingest_request_id"]) if existing["ingest_request_id"] else None,
                        created_at=existing["created_at"],
                    )

                row = await conn.fetchrow(
                    f"""
                    INSERT INTO {self._schema}.dataset_versions (
                        version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, ingest_request_id
                    ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::uuid)
                    RETURNING version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                              ingest_request_id, created_at
                    """,
                    str(uuid4()),
                    dataset_id,
                    lakefs_commit_id,
                    artifact_key,
                    row_count,
                    sample_payload,
                    ingest_request_id,
                )
                if not row:
                    raise RuntimeError("Failed to publish dataset version")
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
                    for entry in outbox_entries:
                        await conn.execute(
                            f"""
                            INSERT INTO {self._schema}.dataset_ingest_outbox (
                                outbox_id, ingest_request_id, kind, payload, status
                            ) VALUES ($1::uuid, $2::uuid, $3, $4::jsonb, 'pending')
                            """,
                            str(uuid4()),
                            ingest_request_id,
                            str(entry.get("kind") or "eventstore"),
                            normalize_json_payload(entry.get("payload") or {}),
                        )
                return DatasetVersionRecord(
                    version_id=str(row["version_id"]),
                    dataset_id=str(row["dataset_id"]),
                    lakefs_commit_id=str(row["lakefs_commit_id"]),
                    artifact_key=row["artifact_key"],
                    row_count=row["row_count"],
                    sample_json=coerce_json_dataset(row["sample_json"]),
                    ingest_request_id=str(row["ingest_request_id"]) if row["ingest_request_id"] else None,
                    created_at=row["created_at"],
                )

    async def claim_ingest_outbox_batch(self, *, limit: int = 50) -> List[DatasetIngestOutboxItem]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    f"""
                    SELECT outbox_id, ingest_request_id, kind, payload, status, publish_attempts, error,
                           created_at, updated_at
                    FROM {self._schema}.dataset_ingest_outbox
                    WHERE status = 'pending'
                    ORDER BY created_at ASC
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                    """,
                    limit,
                )
                if not rows:
                    return []
                outbox_ids = [str(row["outbox_id"]) for row in rows]
                await conn.execute(
                    f"""
                    UPDATE {self._schema}.dataset_ingest_outbox
                    SET status = 'publishing',
                        publish_attempts = publish_attempts + 1,
                        updated_at = NOW()
                    WHERE outbox_id = ANY($1::uuid[])
                    """,
                    outbox_ids,
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
                    updated_at = NOW()
                WHERE outbox_id = $1::uuid
                """,
                outbox_id,
            )

    async def mark_ingest_outbox_failed(self, *, outbox_id: str, error: str) -> None:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.dataset_ingest_outbox
                SET status = 'failed',
                    error = $2,
                    updated_at = NOW()
                WHERE outbox_id = $1::uuid
                """,
                outbox_id,
                error,
            )
