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
from shared.utils.schema_hash import compute_schema_hash
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
    schema_status: str
    schema_approved_at: Optional[datetime]
    schema_approved_by: Optional[str]
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


@dataclass(frozen=True)
class BackingDatasourceRecord:
    backing_id: str
    dataset_id: str
    db_name: str
    name: str
    description: Optional[str]
    source_type: str
    source_ref: Optional[str]
    branch: str
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class BackingDatasourceVersionRecord:
    version_id: str
    backing_id: str
    dataset_version_id: str
    schema_hash: str
    artifact_key: Optional[str]
    metadata: Dict[str, Any]
    status: str
    created_at: datetime


@dataclass(frozen=True)
class KeySpecRecord:
    key_spec_id: str
    dataset_id: str
    dataset_version_id: Optional[str]
    spec: Dict[str, Any]
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class GatePolicyRecord:
    policy_id: str
    scope: str
    name: str
    description: Optional[str]
    rules: Dict[str, Any]
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class GateResultRecord:
    result_id: str
    policy_id: str
    scope: str
    subject_type: str
    subject_id: str
    status: str
    details: Dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class AccessPolicyRecord:
    policy_id: str
    db_name: str
    scope: str
    subject_type: str
    subject_id: str
    policy: Dict[str, Any]
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class InstanceEditRecord:
    edit_id: str
    db_name: str
    class_id: str
    instance_id: str
    edit_type: str
    status: str
    fields: List[str]
    metadata: Dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class RelationshipSpecRecord:
    relationship_spec_id: str
    link_type_id: str
    db_name: str
    source_object_type: str
    target_object_type: str
    predicate: str
    spec_type: str
    dataset_id: str
    dataset_version_id: Optional[str]
    mapping_spec_id: str
    mapping_spec_version: int
    spec: Dict[str, Any]
    status: str
    auto_sync: bool
    last_index_status: Optional[str]
    last_indexed_at: Optional[datetime]
    last_index_result_id: Optional[str]
    last_index_stats: Dict[str, Any]
    last_index_dataset_version_id: Optional[str]
    last_index_mapping_spec_version: Optional[int]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class RelationshipIndexResultRecord:
    result_id: str
    relationship_spec_id: str
    link_type_id: str
    db_name: str
    dataset_id: str
    dataset_version_id: Optional[str]
    mapping_spec_id: str
    mapping_spec_version: int
    status: str
    stats: Dict[str, Any]
    errors: List[Dict[str, Any]]
    lineage: Dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class LinkEditRecord:
    edit_id: str
    db_name: str
    link_type_id: str
    branch: str
    source_object_type: str
    target_object_type: str
    predicate: str
    source_instance_id: str
    target_instance_id: str
    edit_type: str
    status: str
    metadata: Dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class SchemaMigrationPlanRecord:
    plan_id: str
    db_name: str
    subject_type: str
    subject_id: str
    status: str
    plan: Dict[str, Any]
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


def _extract_schema_columns(schema: Any) -> List[Dict[str, Any]]:
    if isinstance(schema, dict):
        columns = schema.get("columns")
        if isinstance(columns, list):
            normalized: List[Dict[str, Any]] = []
            for item in columns:
                if isinstance(item, dict):
                    normalized.append(dict(item))
                else:
                    name = str(item).strip()
                    if name:
                        normalized.append({"name": name})
            return normalized
    if isinstance(schema, list):
        normalized = []
        for item in schema:
            if isinstance(item, dict):
                normalized.append(dict(item))
            else:
                name = str(item).strip()
                if name:
                    normalized.append({"name": name})
        return normalized
    return []


def _compute_schema_hash_from_payload(payload: Any) -> Optional[str]:
    columns = _extract_schema_columns(payload)
    if not columns:
        return None
    return compute_schema_hash(columns)


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

    @staticmethod
    def _row_to_backing(row: asyncpg.Record) -> BackingDatasourceRecord:
        return BackingDatasourceRecord(
            backing_id=str(row["backing_id"]),
            dataset_id=str(row["dataset_id"]),
            db_name=row["db_name"],
            name=row["name"],
            description=row["description"],
            source_type=row["source_type"],
            source_ref=row["source_ref"],
            branch=row["branch"],
            status=row["status"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_backing_version(row: asyncpg.Record) -> BackingDatasourceVersionRecord:
        return BackingDatasourceVersionRecord(
            version_id=str(row["backing_version_id"]),
            backing_id=str(row["backing_id"]),
            dataset_version_id=str(row["dataset_version_id"]),
            schema_hash=row["schema_hash"],
            artifact_key=row["artifact_key"],
            metadata=coerce_json_dataset(row["metadata"]) or {},
            status=row["status"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _row_to_key_spec(row: asyncpg.Record) -> KeySpecRecord:
        return KeySpecRecord(
            key_spec_id=str(row["key_spec_id"]),
            dataset_id=str(row["dataset_id"]),
            dataset_version_id=(str(row["dataset_version_id"]) if row["dataset_version_id"] else None),
            spec=coerce_json_dataset(row["spec"]) or {},
            status=row["status"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_gate_policy(row: asyncpg.Record) -> GatePolicyRecord:
        return GatePolicyRecord(
            policy_id=str(row["policy_id"]),
            scope=row["scope"],
            name=row["name"],
            description=row["description"],
            rules=coerce_json_dataset(row["rules"]) or {},
            status=row["status"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_gate_result(row: asyncpg.Record) -> GateResultRecord:
        return GateResultRecord(
            result_id=str(row["result_id"]),
            policy_id=str(row["policy_id"]),
            scope=row["scope"],
            subject_type=row["subject_type"],
            subject_id=row["subject_id"],
            status=row["status"],
            details=coerce_json_dataset(row["details"]) or {},
            created_at=row["created_at"],
        )

    @staticmethod
    def _row_to_access_policy(row: asyncpg.Record) -> AccessPolicyRecord:
        return AccessPolicyRecord(
            policy_id=str(row["policy_id"]),
            db_name=row["db_name"],
            scope=row["scope"],
            subject_type=row["subject_type"],
            subject_id=row["subject_id"],
            policy=coerce_json_dataset(row["policy"]) or {},
            status=row["status"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_instance_edit(row: asyncpg.Record) -> InstanceEditRecord:
        fields = coerce_json_dataset(row["fields"]) if "fields" in row else None
        if not isinstance(fields, list):
            fields = []
        return InstanceEditRecord(
            edit_id=str(row["edit_id"]),
            db_name=row["db_name"],
            class_id=row["class_id"],
            instance_id=row["instance_id"],
            edit_type=row["edit_type"],
            status=str(row["status"] or "ACTIVE") if "status" in row else "ACTIVE",
            fields=[str(value) for value in fields if str(value).strip()],
            metadata=coerce_json_dataset(row["metadata"]) or {},
            created_at=row["created_at"],
        )

    @staticmethod
    def _row_to_relationship_spec(row: asyncpg.Record) -> RelationshipSpecRecord:
        return RelationshipSpecRecord(
            relationship_spec_id=str(row["relationship_spec_id"]),
            link_type_id=str(row["link_type_id"]),
            db_name=row["db_name"],
            source_object_type=row["source_object_type"],
            target_object_type=row["target_object_type"],
            predicate=row["predicate"],
            spec_type=row["spec_type"],
            dataset_id=str(row["dataset_id"]),
            dataset_version_id=str(row["dataset_version_id"]) if row["dataset_version_id"] else None,
            mapping_spec_id=str(row["mapping_spec_id"]),
            mapping_spec_version=int(row["mapping_spec_version"]),
            spec=coerce_json_dataset(row["spec"]) or {},
            status=row["status"],
            auto_sync=bool(row["auto_sync"]),
            last_index_status=row["last_index_status"],
            last_indexed_at=row["last_indexed_at"],
            last_index_result_id=str(row["last_index_result_id"]) if row["last_index_result_id"] else None,
            last_index_stats=coerce_json_dataset(row["last_index_stats"]) or {},
            last_index_dataset_version_id=(
                str(row["last_index_dataset_version_id"]) if row["last_index_dataset_version_id"] else None
            ),
            last_index_mapping_spec_version=(
                int(row["last_index_mapping_spec_version"])
                if row["last_index_mapping_spec_version"] is not None
                else None
            ),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_relationship_index_result(row: asyncpg.Record) -> RelationshipIndexResultRecord:
        return RelationshipIndexResultRecord(
            result_id=str(row["result_id"]),
            relationship_spec_id=str(row["relationship_spec_id"]),
            link_type_id=str(row["link_type_id"]),
            db_name=row["db_name"],
            dataset_id=str(row["dataset_id"]),
            dataset_version_id=str(row["dataset_version_id"]) if row["dataset_version_id"] else None,
            mapping_spec_id=str(row["mapping_spec_id"]),
            mapping_spec_version=int(row["mapping_spec_version"]),
            status=str(row["status"]),
            stats=coerce_json_dataset(row["stats"]) or {},
            errors=coerce_json_dataset(row["errors"]) or [],
            lineage=coerce_json_dataset(row["lineage"]) or {},
            created_at=row["created_at"],
        )

    @staticmethod
    def _row_to_link_edit(row: asyncpg.Record) -> LinkEditRecord:
        return LinkEditRecord(
            edit_id=str(row["edit_id"]),
            db_name=row["db_name"],
            link_type_id=str(row["link_type_id"]),
            branch=row["branch"],
            source_object_type=row["source_object_type"],
            target_object_type=row["target_object_type"],
            predicate=row["predicate"],
            source_instance_id=row["source_instance_id"],
            target_instance_id=row["target_instance_id"],
            edit_type=row["edit_type"],
            status=row["status"],
            metadata=coerce_json_dataset(row["metadata"]) or {},
            created_at=row["created_at"],
        )

    @staticmethod
    def _row_to_schema_migration_plan(row: asyncpg.Record) -> SchemaMigrationPlanRecord:
        return SchemaMigrationPlanRecord(
            plan_id=str(row["plan_id"]),
            db_name=row["db_name"],
            subject_type=row["subject_type"],
            subject_id=row["subject_id"],
            status=row["status"],
            plan=coerce_json_dataset(row["plan"]) or {},
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

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
                    schema_status TEXT NOT NULL DEFAULT 'PENDING',
                    schema_approved_at TIMESTAMPTZ,
                    schema_approved_by TEXT,
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
                    ADD COLUMN IF NOT EXISTS schema_status TEXT NOT NULL DEFAULT 'PENDING'
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.dataset_ingest_requests
                    ADD COLUMN IF NOT EXISTS schema_approved_at TIMESTAMPTZ
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.dataset_ingest_requests
                    ADD COLUMN IF NOT EXISTS schema_approved_by TEXT
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
                UPDATE {self._schema}.dataset_ingest_requests
                SET schema_status = 'PENDING'
                WHERE schema_status IS NULL
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

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.backing_datasources (
                    backing_id UUID PRIMARY KEY,
                    dataset_id UUID NOT NULL,
                    db_name TEXT NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    source_type TEXT NOT NULL DEFAULT 'dataset',
                    source_ref TEXT,
                    branch TEXT NOT NULL DEFAULT 'main',
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (dataset_id, branch),
                    UNIQUE (db_name, name, branch),
                    FOREIGN KEY (dataset_id)
                        REFERENCES {self._schema}.datasets(dataset_id)
                        ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_backing_datasources_dataset
                ON {self._schema}.backing_datasources(dataset_id, branch)
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.backing_datasource_versions (
                    backing_version_id UUID PRIMARY KEY,
                    backing_id UUID NOT NULL,
                    dataset_version_id UUID NOT NULL,
                    schema_hash TEXT NOT NULL,
                    artifact_key TEXT,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (backing_id, dataset_version_id),
                    FOREIGN KEY (backing_id)
                        REFERENCES {self._schema}.backing_datasources(backing_id)
                        ON DELETE CASCADE,
                    FOREIGN KEY (dataset_version_id)
                        REFERENCES {self._schema}.dataset_versions(version_id)
                        ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_backing_versions_dataset_version
                ON {self._schema}.backing_datasource_versions(dataset_version_id)
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.key_specs (
                    key_spec_id UUID PRIMARY KEY,
                    dataset_id UUID NOT NULL,
                    dataset_version_id UUID,
                    spec JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (dataset_id, dataset_version_id),
                    FOREIGN KEY (dataset_id)
                        REFERENCES {self._schema}.datasets(dataset_id)
                        ON DELETE CASCADE,
                    FOREIGN KEY (dataset_version_id)
                        REFERENCES {self._schema}.dataset_versions(version_id)
                        ON DELETE SET NULL
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_key_specs_dataset
                ON {self._schema}.key_specs(dataset_id)
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.gate_policies (
                    policy_id UUID PRIMARY KEY,
                    scope TEXT NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    rules JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (scope, name)
                )
                """
            )
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.gate_results (
                    result_id UUID PRIMARY KEY,
                    policy_id UUID NOT NULL,
                    scope TEXT NOT NULL,
                    subject_type TEXT NOT NULL,
                    subject_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    details JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    FOREIGN KEY (policy_id)
                        REFERENCES {self._schema}.gate_policies(policy_id)
                        ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_gate_results_subject
                ON {self._schema}.gate_results(scope, subject_type, subject_id)
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.access_policies (
                    policy_id UUID PRIMARY KEY,
                    db_name TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    subject_type TEXT NOT NULL,
                    subject_id TEXT NOT NULL,
                    policy JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (db_name, scope, subject_type, subject_id)
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_access_policies_subject
                ON {self._schema}.access_policies(db_name, subject_type, subject_id)
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.instance_edits (
                    edit_id UUID PRIMARY KEY,
                    db_name TEXT NOT NULL,
                    class_id TEXT NOT NULL,
                    instance_id TEXT NOT NULL,
                    edit_type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    fields JSONB NOT NULL DEFAULT '[]'::jsonb,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_instance_edits_class
                ON {self._schema}.instance_edits(db_name, class_id)
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_instance_edits_instance
                ON {self._schema}.instance_edits(db_name, class_id, instance_id)
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.instance_edits
                    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'ACTIVE'
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.instance_edits
                    ADD COLUMN IF NOT EXISTS fields JSONB NOT NULL DEFAULT '[]'::jsonb
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_instance_edits_status
                ON {self._schema}.instance_edits(db_name, class_id, status)
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_instance_edits_fields
                ON {self._schema}.instance_edits
                USING GIN (fields)
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.relationship_specs (
                    relationship_spec_id UUID PRIMARY KEY,
                    link_type_id TEXT NOT NULL,
                    db_name TEXT NOT NULL,
                    source_object_type TEXT NOT NULL,
                    target_object_type TEXT NOT NULL,
                    predicate TEXT NOT NULL,
                    spec_type TEXT NOT NULL,
                    dataset_id UUID NOT NULL,
                    dataset_version_id UUID,
                    mapping_spec_id UUID NOT NULL,
                    mapping_spec_version INTEGER NOT NULL DEFAULT 1,
                    spec JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    auto_sync BOOLEAN NOT NULL DEFAULT TRUE,
                    last_index_status TEXT,
                    last_indexed_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (link_type_id),
                    FOREIGN KEY (dataset_id)
                        REFERENCES {self._schema}.datasets(dataset_id)
                        ON DELETE CASCADE,
                    FOREIGN KEY (dataset_version_id)
                        REFERENCES {self._schema}.dataset_versions(version_id)
                        ON DELETE SET NULL
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_relationship_specs_dataset
                ON {self._schema}.relationship_specs(dataset_id, status)
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_relationship_specs_db
                ON {self._schema}.relationship_specs(db_name, status)
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.relationship_specs
                    ADD COLUMN IF NOT EXISTS last_index_result_id UUID
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.relationship_specs
                    ADD COLUMN IF NOT EXISTS last_index_stats JSONB NOT NULL DEFAULT '{{}}'::jsonb
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.relationship_specs
                    ADD COLUMN IF NOT EXISTS last_index_dataset_version_id UUID
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.relationship_specs
                    ADD COLUMN IF NOT EXISTS last_index_mapping_spec_version INTEGER
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.relationship_index_results (
                    result_id UUID PRIMARY KEY,
                    relationship_spec_id UUID NOT NULL,
                    link_type_id TEXT NOT NULL,
                    db_name TEXT NOT NULL,
                    dataset_id UUID NOT NULL,
                    dataset_version_id UUID,
                    mapping_spec_id UUID NOT NULL,
                    mapping_spec_version INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    stats JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    errors JSONB NOT NULL DEFAULT '[]'::jsonb,
                    lineage JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    FOREIGN KEY (relationship_spec_id)
                        REFERENCES {self._schema}.relationship_specs(relationship_spec_id)
                        ON DELETE CASCADE,
                    FOREIGN KEY (dataset_id)
                        REFERENCES {self._schema}.datasets(dataset_id)
                        ON DELETE CASCADE,
                    FOREIGN KEY (dataset_version_id)
                        REFERENCES {self._schema}.dataset_versions(version_id)
                        ON DELETE SET NULL
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_relationship_index_results_spec
                ON {self._schema}.relationship_index_results(relationship_spec_id, status)
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_relationship_index_results_link
                ON {self._schema}.relationship_index_results(link_type_id, status)
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_relationship_index_results_db
                ON {self._schema}.relationship_index_results(db_name, status)
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.link_edits (
                    edit_id UUID PRIMARY KEY,
                    db_name TEXT NOT NULL,
                    link_type_id TEXT NOT NULL,
                    branch TEXT NOT NULL DEFAULT 'main',
                    source_object_type TEXT NOT NULL,
                    target_object_type TEXT NOT NULL,
                    predicate TEXT NOT NULL,
                    source_instance_id TEXT NOT NULL,
                    target_instance_id TEXT NOT NULL,
                    edit_type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_link_edits_link
                ON {self._schema}.link_edits(link_type_id, status)
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_link_edits_source
                ON {self._schema}.link_edits(db_name, source_object_type, source_instance_id)
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_link_edits_target
                ON {self._schema}.link_edits(db_name, target_object_type, target_instance_id)
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.schema_migration_plans (
                    plan_id UUID PRIMARY KEY,
                    db_name TEXT NOT NULL,
                    subject_type TEXT NOT NULL,
                    subject_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    plan JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_schema_migration_plans_subject
                ON {self._schema}.schema_migration_plans(db_name, subject_type, subject_id)
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
            record = DatasetVersionRecord(
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

        try:
            backing = await self.get_backing_datasource_by_dataset(dataset_id=dataset_id)
            if backing:
                schema_hash = None
                if isinstance(sample_json, dict) and isinstance(sample_json.get("columns"), list):
                    schema_hash = compute_schema_hash(sample_json.get("columns") or [])
                elif isinstance(schema_json, dict) and isinstance(schema_json.get("columns"), list):
                    schema_hash = compute_schema_hash(schema_json.get("columns") or [])
                if schema_hash:
                    await self.get_or_create_backing_datasource_version(
                        backing_id=backing.backing_id,
                        dataset_version_id=record.version_id,
                        schema_hash=schema_hash,
                        metadata={"artifact_key": record.artifact_key} if record.artifact_key else None,
                    )
        except Exception as exc:
            logger.warning("Failed to create backing datasource version: %s", exc)

        return record

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

    async def create_backing_datasource(
        self,
        *,
        dataset_id: str,
        db_name: str,
        name: str,
        branch: str = "main",
        description: Optional[str] = None,
        source_type: str = "dataset",
        source_ref: Optional[str] = None,
        backing_id: Optional[str] = None,
    ) -> BackingDatasourceRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        backing_id = backing_id or str(uuid4())
        branch = branch or "main"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.backing_datasources (
                    backing_id, dataset_id, db_name, name, description, source_type, source_ref, branch
                ) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8)
                RETURNING backing_id, dataset_id, db_name, name, description, source_type, source_ref,
                          branch, status, created_at, updated_at
                """,
                backing_id,
                dataset_id,
                db_name,
                name,
                description,
                source_type,
                source_ref,
                branch,
            )
            if not row:
                raise RuntimeError("Failed to create backing datasource")
            return self._row_to_backing(row)

    async def get_backing_datasource(self, *, backing_id: str) -> Optional[BackingDatasourceRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT backing_id, dataset_id, db_name, name, description, source_type, source_ref,
                       branch, status, created_at, updated_at
                FROM {self._schema}.backing_datasources
                WHERE backing_id = $1::uuid
                """,
                backing_id,
            )
            if not row:
                return None
            return self._row_to_backing(row)

    async def get_backing_datasource_by_dataset(
        self,
        *,
        dataset_id: str,
        branch: Optional[str] = None,
    ) -> Optional[BackingDatasourceRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT backing_id, dataset_id, db_name, name, description, source_type, source_ref,
                       branch, status, created_at, updated_at
                FROM {self._schema}.backing_datasources
                WHERE dataset_id = $1::uuid
                  AND ($2::text IS NULL OR branch = $2)
                ORDER BY created_at DESC
                LIMIT 1
                """,
                dataset_id,
                branch,
            )
            if not row:
                return None
            return self._row_to_backing(row)

    async def list_backing_datasources(
        self,
        *,
        db_name: str,
        branch: Optional[str] = None,
        limit: int = 200,
    ) -> List[BackingDatasourceRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT backing_id, dataset_id, db_name, name, description, source_type, source_ref,
                       branch, status, created_at, updated_at
                FROM {self._schema}.backing_datasources
                WHERE db_name = $1
                  AND ($2::text IS NULL OR branch = $2)
                ORDER BY created_at DESC
                LIMIT $3
                """,
                db_name,
                branch,
                limit,
            )
            return [self._row_to_backing(row) for row in rows]

    async def get_or_create_backing_datasource(
        self,
        *,
        dataset: DatasetRecord,
        source_type: str = "dataset",
        source_ref: Optional[str] = None,
    ) -> BackingDatasourceRecord:
        existing = await self.get_backing_datasource_by_dataset(
            dataset_id=dataset.dataset_id,
            branch=dataset.branch,
        )
        if existing:
            return existing
        return await self.create_backing_datasource(
            dataset_id=dataset.dataset_id,
            db_name=dataset.db_name,
            name=dataset.name,
            description=dataset.description,
            source_type=source_type,
            source_ref=source_ref,
            branch=dataset.branch,
        )

    async def create_backing_datasource_version(
        self,
        *,
        backing_id: str,
        dataset_version_id: str,
        schema_hash: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> BackingDatasourceVersionRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        backing = await self.get_backing_datasource(backing_id=backing_id)
        if not backing:
            raise RuntimeError("Backing datasource not found")
        version = await self.get_version(version_id=dataset_version_id)
        if not version or version.dataset_id != backing.dataset_id:
            raise RuntimeError("Dataset version mismatch")
        if not schema_hash:
            schema_hash = _compute_schema_hash_from_payload(version.sample_json)
            if not schema_hash:
                dataset = await self.get_dataset(dataset_id=backing.dataset_id)
                schema_hash = _compute_schema_hash_from_payload(
                    dataset.schema_json if dataset else {}
                )
        if not schema_hash:
            raise RuntimeError("schema_hash is required for backing datasource version")
        metadata_payload = normalize_json_payload(metadata or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.backing_datasource_versions (
                    backing_version_id, backing_id, dataset_version_id, schema_hash, artifact_key, metadata
                ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6::jsonb)
                RETURNING backing_version_id, backing_id, dataset_version_id, schema_hash, artifact_key,
                          metadata, status, created_at
                """,
                str(uuid4()),
                backing_id,
                dataset_version_id,
                schema_hash,
                version.artifact_key,
                metadata_payload,
            )
            if not row:
                raise RuntimeError("Failed to create backing datasource version")
            return self._row_to_backing_version(row)

    async def get_backing_datasource_version(
        self,
        *,
        version_id: str,
    ) -> Optional[BackingDatasourceVersionRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT backing_version_id, backing_id, dataset_version_id, schema_hash, artifact_key,
                       metadata, status, created_at
                FROM {self._schema}.backing_datasource_versions
                WHERE backing_version_id = $1::uuid
                """,
                version_id,
            )
            if not row:
                return None
            return self._row_to_backing_version(row)

    async def get_backing_datasource_version_by_dataset_version(
        self,
        *,
        dataset_version_id: str,
    ) -> Optional[BackingDatasourceVersionRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT backing_version_id, backing_id, dataset_version_id, schema_hash, artifact_key,
                       metadata, status, created_at
                FROM {self._schema}.backing_datasource_versions
                WHERE dataset_version_id = $1::uuid
                ORDER BY created_at DESC
                LIMIT 1
                """,
                dataset_version_id,
            )
            if not row:
                return None
            return self._row_to_backing_version(row)

    async def list_backing_datasource_versions(
        self,
        *,
        backing_id: str,
        limit: int = 200,
    ) -> List[BackingDatasourceVersionRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT backing_version_id, backing_id, dataset_version_id, schema_hash, artifact_key,
                       metadata, status, created_at
                FROM {self._schema}.backing_datasource_versions
                WHERE backing_id = $1::uuid
                ORDER BY created_at DESC
                LIMIT $2
                """,
                backing_id,
                limit,
            )
            return [self._row_to_backing_version(row) for row in rows]

    async def get_or_create_backing_datasource_version(
        self,
        *,
        backing_id: str,
        dataset_version_id: str,
        schema_hash: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> BackingDatasourceVersionRecord:
        existing = await self.get_backing_datasource_version_by_dataset_version(
            dataset_version_id=dataset_version_id,
        )
        if existing and existing.backing_id == backing_id:
            return existing
        return await self.create_backing_datasource_version(
            backing_id=backing_id,
            dataset_version_id=dataset_version_id,
            schema_hash=schema_hash,
            metadata=metadata,
        )

    async def create_key_spec(
        self,
        *,
        dataset_id: str,
        spec: Dict[str, Any],
        dataset_version_id: Optional[str] = None,
        status: str = "ACTIVE",
        key_spec_id: Optional[str] = None,
    ) -> KeySpecRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        key_spec_id = key_spec_id or str(uuid4())
        payload = normalize_json_payload(spec or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.key_specs (
                    key_spec_id, dataset_id, dataset_version_id, spec, status
                ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4::jsonb, $5)
                RETURNING key_spec_id, dataset_id, dataset_version_id, spec, status, created_at, updated_at
                """,
                key_spec_id,
                dataset_id,
                dataset_version_id,
                payload,
                status,
            )
            if not row:
                raise RuntimeError("Failed to create key spec")
            return self._row_to_key_spec(row)

    async def get_key_spec(self, *, key_spec_id: str) -> Optional[KeySpecRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT key_spec_id, dataset_id, dataset_version_id, spec, status, created_at, updated_at
                FROM {self._schema}.key_specs
                WHERE key_spec_id = $1::uuid
                """,
                key_spec_id,
            )
            if not row:
                return None
            return self._row_to_key_spec(row)

    async def get_key_spec_for_dataset(
        self,
        *,
        dataset_id: str,
        dataset_version_id: Optional[str] = None,
    ) -> Optional[KeySpecRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            if dataset_version_id:
                row = await conn.fetchrow(
                    f"""
                    SELECT key_spec_id, dataset_id, dataset_version_id, spec, status, created_at, updated_at
                    FROM {self._schema}.key_specs
                    WHERE dataset_id = $1::uuid
                      AND dataset_version_id = $2::uuid
                      AND status = 'ACTIVE'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    dataset_id,
                    dataset_version_id,
                )
                if row:
                    return self._row_to_key_spec(row)
            row = await conn.fetchrow(
                f"""
                SELECT key_spec_id, dataset_id, dataset_version_id, spec, status, created_at, updated_at
                FROM {self._schema}.key_specs
                WHERE dataset_id = $1::uuid
                  AND dataset_version_id IS NULL
                  AND status = 'ACTIVE'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                dataset_id,
            )
            if not row:
                return None
            return self._row_to_key_spec(row)

    async def list_key_specs(
        self,
        *,
        dataset_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[KeySpecRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT key_spec_id, dataset_id, dataset_version_id, spec, status, created_at, updated_at
                FROM {self._schema}.key_specs
                WHERE ($1::uuid IS NULL OR dataset_id = $1::uuid)
                ORDER BY created_at DESC
                LIMIT $2
                """,
                dataset_id,
                limit,
            )
            return [self._row_to_key_spec(row) for row in rows]

    async def upsert_gate_policy(
        self,
        *,
        scope: str,
        name: str,
        description: Optional[str] = None,
        rules: Optional[Dict[str, Any]] = None,
        status: str = "ACTIVE",
    ) -> GatePolicyRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        rules_payload = normalize_json_payload(rules or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.gate_policies (
                    policy_id, scope, name, description, rules, status
                ) VALUES ($1::uuid, $2, $3, $4, $5::jsonb, $6)
                ON CONFLICT (scope, name)
                DO UPDATE SET description = EXCLUDED.description,
                              rules = EXCLUDED.rules,
                              status = EXCLUDED.status,
                              updated_at = NOW()
                RETURNING policy_id, scope, name, description, rules, status, created_at, updated_at
                """,
                str(uuid4()),
                scope,
                name,
                description,
                rules_payload,
                status,
            )
            if not row:
                raise RuntimeError("Failed to upsert gate policy")
            return self._row_to_gate_policy(row)

    async def get_gate_policy(
        self,
        *,
        scope: str,
        name: str,
    ) -> Optional[GatePolicyRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT policy_id, scope, name, description, rules, status, created_at, updated_at
                FROM {self._schema}.gate_policies
                WHERE scope = $1 AND name = $2
                """,
                scope,
                name,
            )
            if not row:
                return None
            return self._row_to_gate_policy(row)

    async def list_gate_policies(
        self,
        *,
        scope: Optional[str] = None,
        limit: int = 200,
    ) -> List[GatePolicyRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT policy_id, scope, name, description, rules, status, created_at, updated_at
                FROM {self._schema}.gate_policies
                WHERE ($1::text IS NULL OR scope = $1)
                ORDER BY created_at DESC
                LIMIT $2
                """,
                scope,
                limit,
            )
            return [self._row_to_gate_policy(row) for row in rows]

    async def record_gate_result(
        self,
        *,
        scope: str,
        subject_type: str,
        subject_id: str,
        status: str,
        details: Optional[Dict[str, Any]] = None,
        policy_name: str = "default",
    ) -> GateResultRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        policy = await self.get_gate_policy(scope=scope, name=policy_name)
        if not policy:
            policy = await self.upsert_gate_policy(
                scope=scope,
                name=policy_name,
                description=f"Default gate policy for {scope}",
                rules={},
                status="ACTIVE",
            )
        payload = normalize_json_payload(details or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.gate_results (
                    result_id, policy_id, scope, subject_type, subject_id, status, details
                ) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::jsonb)
                RETURNING result_id, policy_id, scope, subject_type, subject_id, status, details, created_at
                """,
                str(uuid4()),
                policy.policy_id,
                scope,
                subject_type,
                subject_id,
                status,
                payload,
            )
            if not row:
                raise RuntimeError("Failed to record gate result")
            return self._row_to_gate_result(row)

    async def list_gate_results(
        self,
        *,
        scope: Optional[str] = None,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[GateResultRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT result_id, policy_id, scope, subject_type, subject_id, status, details, created_at
                FROM {self._schema}.gate_results
                WHERE ($1::text IS NULL OR scope = $1)
                  AND ($2::text IS NULL OR subject_type = $2)
                  AND ($3::text IS NULL OR subject_id = $3)
                ORDER BY created_at DESC
                LIMIT $4
                """,
                scope,
                subject_type,
                subject_id,
                limit,
            )
            return [self._row_to_gate_result(row) for row in rows]

    async def upsert_access_policy(
        self,
        *,
        db_name: str,
        scope: str,
        subject_type: str,
        subject_id: str,
        policy: Optional[Dict[str, Any]] = None,
        status: str = "ACTIVE",
    ) -> AccessPolicyRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        policy_payload = normalize_json_payload(policy or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.access_policies (
                    policy_id, db_name, scope, subject_type, subject_id, policy, status
                ) VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb, $7)
                ON CONFLICT (db_name, scope, subject_type, subject_id)
                DO UPDATE SET policy = EXCLUDED.policy,
                              status = EXCLUDED.status,
                              updated_at = NOW()
                RETURNING policy_id, db_name, scope, subject_type, subject_id, policy, status, created_at, updated_at
                """,
                str(uuid4()),
                db_name,
                scope,
                subject_type,
                subject_id,
                policy_payload,
                status,
            )
            if not row:
                raise RuntimeError("Failed to upsert access policy")
            return self._row_to_access_policy(row)

    async def get_access_policy(
        self,
        *,
        db_name: str,
        scope: str,
        subject_type: str,
        subject_id: str,
        status: Optional[str] = "ACTIVE",
    ) -> Optional[AccessPolicyRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT policy_id, db_name, scope, subject_type, subject_id, policy, status, created_at, updated_at
                FROM {self._schema}.access_policies
                WHERE db_name = $1 AND scope = $2 AND subject_type = $3 AND subject_id = $4
                  AND ($5::text IS NULL OR status = $5)
                """,
                db_name,
                scope,
                subject_type,
                subject_id,
                status,
            )
            if not row:
                return None
            return self._row_to_access_policy(row)

    async def list_access_policies(
        self,
        *,
        db_name: Optional[str] = None,
        scope: Optional[str] = None,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[AccessPolicyRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT policy_id, db_name, scope, subject_type, subject_id, policy, status, created_at, updated_at
                FROM {self._schema}.access_policies
                WHERE ($1::text IS NULL OR db_name = $1)
                  AND ($2::text IS NULL OR scope = $2)
                  AND ($3::text IS NULL OR subject_type = $3)
                  AND ($4::text IS NULL OR subject_id = $4)
                  AND ($5::text IS NULL OR status = $5)
                ORDER BY created_at DESC
                LIMIT $6
                """,
                db_name,
                scope,
                subject_type,
                subject_id,
                status,
                limit,
            )
            return [self._row_to_access_policy(row) for row in rows]

    async def record_instance_edit(
        self,
        *,
        db_name: str,
        class_id: str,
        instance_id: str,
        edit_type: str,
        metadata: Optional[Dict[str, Any]] = None,
        status: str = "ACTIVE",
        fields: Optional[List[str]] = None,
    ) -> InstanceEditRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        cleaned_fields = [str(field).strip() for field in (fields or []) if str(field).strip()]
        payload = normalize_json_payload(metadata or {})
        if cleaned_fields:
            payload = {**payload, "fields": cleaned_fields}
        status_value = str(status or "ACTIVE").strip().upper() or "ACTIVE"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.instance_edits (
                    edit_id, db_name, class_id, instance_id, edit_type, status, fields, metadata
                ) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb)
                RETURNING edit_id, db_name, class_id, instance_id, edit_type, status, fields, metadata, created_at
                """,
                str(uuid4()),
                db_name,
                class_id,
                instance_id,
                edit_type,
                status_value,
                normalize_json_payload(cleaned_fields),
                payload,
            )
            if not row:
                raise RuntimeError("Failed to record instance edit")
            return self._row_to_instance_edit(row)

    async def count_instance_edits(
        self,
        *,
        db_name: str,
        class_id: str,
        status: Optional[str] = None,
    ) -> int:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            value = await conn.fetchval(
                f"""
                SELECT COUNT(*)
                FROM {self._schema}.instance_edits
                WHERE db_name = $1 AND class_id = $2
                  AND ($3::text IS NULL OR status = $3)
                """,
                db_name,
                class_id,
                status,
            )
            return int(value or 0)

    async def list_instance_edits(
        self,
        *,
        db_name: str,
        class_id: Optional[str] = None,
        instance_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[InstanceEditRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT edit_id, db_name, class_id, instance_id, edit_type, status, fields, metadata, created_at
                FROM {self._schema}.instance_edits
                WHERE db_name = $1
                  AND ($2::text IS NULL OR class_id = $2)
                  AND ($3::text IS NULL OR instance_id = $3)
                  AND ($4::text IS NULL OR status = $4)
                ORDER BY created_at DESC
                LIMIT $5
                """,
                db_name,
                class_id,
                instance_id,
                status,
                limit,
            )
            return [self._row_to_instance_edit(row) for row in rows]

    async def clear_instance_edits(
        self,
        *,
        db_name: str,
        class_id: str,
    ) -> int:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                f"""
                DELETE FROM {self._schema}.instance_edits
                WHERE db_name = $1 AND class_id = $2
                """,
                db_name,
                class_id,
            )
            try:
                return int(str(result).split()[-1])
            except Exception:
                return 0

    async def remap_instance_edits(
        self,
        *,
        db_name: str,
        class_id: str,
        id_map: Dict[str, str],
        status: Optional[str] = None,
    ) -> int:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        if not id_map:
            return 0
        updated = 0
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for old_id, new_id in id_map.items():
                    if not old_id or not new_id:
                        continue
                    result = await conn.execute(
                        f"""
                        UPDATE {self._schema}.instance_edits
                        SET instance_id = $1
                        WHERE db_name = $2 AND class_id = $3 AND instance_id = $4
                          AND ($5::text IS NULL OR status = $5)
                        """,
                        str(new_id),
                        db_name,
                        class_id,
                        str(old_id),
                        status,
                    )
                    try:
                        updated += int(str(result).split()[-1])
                    except Exception:
                        continue
        return updated

    async def get_instance_edit_field_stats(
        self,
        *,
        db_name: str,
        class_id: str,
        fields: List[str],
        status: Optional[str] = "ACTIVE",
    ) -> Dict[str, Any]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        normalized_fields = [str(field).strip() for field in fields if str(field).strip()]
        status_value = str(status).strip().upper() if status else None
        async with self._pool.acquire() as conn:
            total = await conn.fetchval(
                f"""
                SELECT COUNT(*)
                FROM {self._schema}.instance_edits
                WHERE db_name = $1 AND class_id = $2
                  AND ($3::text IS NULL OR status = $3)
                """,
                db_name,
                class_id,
                status_value,
            )
            empty_fields = await conn.fetchval(
                f"""
                SELECT COUNT(*)
                FROM {self._schema}.instance_edits
                WHERE db_name = $1 AND class_id = $2
                  AND ($3::text IS NULL OR status = $3)
                  AND jsonb_array_length(fields) = 0
                """,
                db_name,
                class_id,
                status_value,
            )
            affected = 0
            if normalized_fields:
                affected = await conn.fetchval(
                    f"""
                    SELECT COUNT(*)
                    FROM {self._schema}.instance_edits
                    WHERE db_name = $1 AND class_id = $2
                      AND ($3::text IS NULL OR status = $3)
                      AND fields ?| $4::text[]
                    """,
                    db_name,
                    class_id,
                    status_value,
                    normalized_fields,
                )
            per_field: Dict[str, int] = {}
            for field in normalized_fields:
                value = await conn.fetchval(
                    f"""
                    SELECT COUNT(*)
                    FROM {self._schema}.instance_edits
                    WHERE db_name = $1 AND class_id = $2
                      AND ($3::text IS NULL OR status = $3)
                      AND fields ? $4
                    """,
                    db_name,
                    class_id,
                    status_value,
                    field,
                )
                per_field[field] = int(value or 0)
            return {
                "total": int(total or 0),
                "affected": int(affected or 0),
                "empty_fields": int(empty_fields or 0),
                "per_field": per_field,
            }

    async def apply_instance_edit_field_moves(
        self,
        *,
        db_name: str,
        class_id: str,
        field_moves: Dict[str, str],
        status: Optional[str] = "ACTIVE",
    ) -> int:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        if not field_moves:
            return 0
        status_value = str(status).strip().upper() if status else None
        updated = 0
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for old_field, new_field in field_moves.items():
                    old_name = str(old_field or "").strip()
                    new_name = str(new_field or "").strip()
                    if not old_name or not new_name or old_name == new_name:
                        continue
                    result = await conn.execute(
                        f"""
                        UPDATE {self._schema}.instance_edits
                        SET fields = (
                                SELECT jsonb_agg(
                                    CASE WHEN value = $1 THEN $2 ELSE value END
                                )
                                FROM jsonb_array_elements_text(fields) AS value
                            ),
                            metadata = jsonb_set(
                                metadata,
                                '{{fields}}',
                                (
                                    SELECT jsonb_agg(
                                        CASE WHEN value = $1 THEN $2 ELSE value END
                                    )
                                    FROM jsonb_array_elements_text(
                                        CASE
                                            WHEN jsonb_typeof(metadata->'fields') = 'array'
                                                THEN metadata->'fields'
                                            ELSE '[]'::jsonb
                                        END
                                    ) AS value
                                ),
                                true
                            )
                        WHERE db_name = $3 AND class_id = $4
                          AND ($5::text IS NULL OR status = $5)
                          AND fields ? $1
                        """,
                        old_name,
                        new_name,
                        db_name,
                        class_id,
                        status_value,
                    )
                    try:
                        updated += int(str(result).split()[-1])
                    except Exception:
                        continue
        return updated

    async def update_instance_edit_status_by_fields(
        self,
        *,
        db_name: str,
        class_id: str,
        fields: List[str],
        new_status: str,
        status: Optional[str] = "ACTIVE",
        metadata_note: Optional[str] = None,
    ) -> int:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        normalized_fields = [str(field).strip() for field in fields if str(field).strip()]
        if not normalized_fields:
            return 0
        status_value = str(status).strip().upper() if status else None
        next_status = str(new_status or "").strip().upper()
        if not next_status:
            return 0
        payload_note = metadata_note or None
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                f"""
                UPDATE {self._schema}.instance_edits
                SET status = $1,
                    metadata = jsonb_set(
                        CASE
                            WHEN $6::text IS NULL THEN metadata
                            ELSE jsonb_set(
                                metadata,
                                '{{status_note}}',
                                to_jsonb($6::text),
                                true
                            )
                        END,
                        '{{status_updated_at}}',
                        to_jsonb($7::text),
                        true
                    )
                WHERE db_name = $2 AND class_id = $3
                  AND ($4::text IS NULL OR status = $4)
                  AND fields ?| $5::text[]
                """,
                next_status,
                db_name,
                class_id,
                status_value,
                normalized_fields,
                payload_note,
                utcnow().isoformat(),
            )
        try:
            return int(str(result).split()[-1])
        except Exception:
            return 0

    async def create_relationship_spec(
        self,
        *,
        link_type_id: str,
        db_name: str,
        source_object_type: str,
        target_object_type: str,
        predicate: str,
        spec_type: str,
        dataset_id: str,
        mapping_spec_id: str,
        mapping_spec_version: int,
        spec: Dict[str, Any],
        dataset_version_id: Optional[str] = None,
        status: str = "ACTIVE",
        auto_sync: bool = True,
        relationship_spec_id: Optional[str] = None,
    ) -> RelationshipSpecRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        relationship_spec_id = relationship_spec_id or str(uuid4())
        payload = normalize_json_payload(spec or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.relationship_specs (
                    relationship_spec_id, link_type_id, db_name, source_object_type, target_object_type,
                    predicate, spec_type, dataset_id, dataset_version_id, mapping_spec_id, mapping_spec_version,
                    spec, status, auto_sync
                ) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8::uuid, $9::uuid, $10::uuid, $11, $12::jsonb, $13, $14)
                RETURNING relationship_spec_id, link_type_id, db_name, source_object_type, target_object_type,
                          predicate, spec_type, dataset_id, dataset_version_id, mapping_spec_id, mapping_spec_version,
                          spec, status, auto_sync, last_index_status, last_indexed_at, last_index_result_id,
                          last_index_stats, last_index_dataset_version_id, last_index_mapping_spec_version,
                          created_at, updated_at
                """,
                relationship_spec_id,
                link_type_id,
                db_name,
                source_object_type,
                target_object_type,
                predicate,
                spec_type,
                dataset_id,
                dataset_version_id,
                mapping_spec_id,
                mapping_spec_version,
                payload,
                status,
                auto_sync,
            )
            if not row:
                raise RuntimeError("Failed to create relationship spec")
            return self._row_to_relationship_spec(row)

    async def update_relationship_spec(
        self,
        *,
        relationship_spec_id: str,
        status: Optional[str] = None,
        spec: Optional[Dict[str, Any]] = None,
        auto_sync: Optional[bool] = None,
        dataset_id: Optional[str] = None,
        dataset_version_id: Optional[str] = None,
        mapping_spec_id: Optional[str] = None,
        mapping_spec_version: Optional[int] = None,
    ) -> Optional[RelationshipSpecRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        updates: List[str] = []
        values: List[Any] = []
        if status is not None:
            updates.append("status = $%s" % (len(values) + 1))
            values.append(status)
        if spec is not None:
            updates.append("spec = $%s::jsonb" % (len(values) + 1))
            values.append(normalize_json_payload(spec))
        if auto_sync is not None:
            updates.append("auto_sync = $%s" % (len(values) + 1))
            values.append(auto_sync)
        if dataset_id is not None:
            updates.append("dataset_id = $%s::uuid" % (len(values) + 1))
            values.append(dataset_id)
        if dataset_version_id is not None:
            updates.append("dataset_version_id = $%s::uuid" % (len(values) + 1))
            values.append(dataset_version_id)
        if mapping_spec_id is not None:
            updates.append("mapping_spec_id = $%s::uuid" % (len(values) + 1))
            values.append(mapping_spec_id)
        if mapping_spec_version is not None:
            updates.append("mapping_spec_version = $%s" % (len(values) + 1))
            values.append(mapping_spec_version)
        if not updates:
            return await self.get_relationship_spec(relationship_spec_id=relationship_spec_id)
        values.append(relationship_spec_id)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.relationship_specs
                SET {", ".join(updates)}, updated_at = NOW()
                WHERE relationship_spec_id = ${len(values)}::uuid
                RETURNING relationship_spec_id, link_type_id, db_name, source_object_type, target_object_type,
                          predicate, spec_type, dataset_id, dataset_version_id, mapping_spec_id, mapping_spec_version,
                          spec, status, auto_sync, last_index_status, last_indexed_at, last_index_result_id,
                          last_index_stats, last_index_dataset_version_id, last_index_mapping_spec_version,
                          created_at, updated_at
                """,
                *values,
            )
            if not row:
                return None
            return self._row_to_relationship_spec(row)

    async def record_relationship_index_result(
        self,
        *,
        relationship_spec_id: str,
        status: str,
        stats: Optional[Dict[str, Any]] = None,
        errors: Optional[List[Dict[str, Any]]] = None,
        dataset_version_id: Optional[str] = None,
        mapping_spec_version: Optional[int] = None,
        lineage: Optional[Dict[str, Any]] = None,
        indexed_at: Optional[datetime] = None,
    ) -> Optional[RelationshipIndexResultRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        indexed_at = indexed_at or utcnow()
        stats_payload = normalize_json_payload(stats or {})
        errors_payload = normalize_json_payload(errors or [])
        lineage_payload = normalize_json_payload(lineage or {})
        async with self._pool.acquire() as conn:
            spec_row = await conn.fetchrow(
                f"""
                SELECT relationship_spec_id, link_type_id, db_name, dataset_id, dataset_version_id,
                       mapping_spec_id, mapping_spec_version
                FROM {self._schema}.relationship_specs
                WHERE relationship_spec_id = $1::uuid
                """,
                relationship_spec_id,
            )
            if not spec_row:
                return None
            resolved_dataset_version_id = (
                dataset_version_id or (str(spec_row["dataset_version_id"]) if spec_row["dataset_version_id"] else None)
            )
            resolved_mapping_version = int(mapping_spec_version or spec_row["mapping_spec_version"])
            result_id = str(uuid4())
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.relationship_index_results (
                    result_id, relationship_spec_id, link_type_id, db_name, dataset_id, dataset_version_id,
                    mapping_spec_id, mapping_spec_version, status, stats, errors, lineage
                ) VALUES (
                    $1::uuid, $2::uuid, $3, $4, $5::uuid, $6::uuid,
                    $7::uuid, $8, $9, $10::jsonb, $11::jsonb, $12::jsonb
                )
                RETURNING result_id, relationship_spec_id, link_type_id, db_name, dataset_id, dataset_version_id,
                          mapping_spec_id, mapping_spec_version, status, stats, errors, lineage, created_at
                """,
                result_id,
                relationship_spec_id,
                str(spec_row["link_type_id"]),
                str(spec_row["db_name"]),
                str(spec_row["dataset_id"]),
                resolved_dataset_version_id,
                str(spec_row["mapping_spec_id"]),
                resolved_mapping_version,
                status,
                stats_payload,
                errors_payload,
                lineage_payload,
            )
            await conn.execute(
                f"""
                UPDATE {self._schema}.relationship_specs
                SET last_index_status = $1,
                    last_indexed_at = $2,
                    last_index_result_id = $3::uuid,
                    last_index_stats = $4::jsonb,
                    last_index_dataset_version_id = $5::uuid,
                    last_index_mapping_spec_version = $6,
                    updated_at = NOW()
                WHERE relationship_spec_id = $7::uuid
                """,
                status,
                indexed_at,
                result_id,
                stats_payload,
                resolved_dataset_version_id,
                resolved_mapping_version,
                relationship_spec_id,
            )
            if not row:
                return None
            return self._row_to_relationship_index_result(row)

    async def get_relationship_spec(
        self,
        *,
        relationship_spec_id: Optional[str] = None,
        link_type_id: Optional[str] = None,
    ) -> Optional[RelationshipSpecRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        if not relationship_spec_id and not link_type_id:
            raise ValueError("relationship_spec_id or link_type_id is required")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT relationship_spec_id, link_type_id, db_name, source_object_type, target_object_type,
                       predicate, spec_type, dataset_id, dataset_version_id, mapping_spec_id, mapping_spec_version,
                       spec, status, auto_sync, last_index_status, last_indexed_at, last_index_result_id,
                       last_index_stats, last_index_dataset_version_id, last_index_mapping_spec_version,
                       created_at, updated_at
                FROM {self._schema}.relationship_specs
                WHERE ($1::uuid IS NULL OR relationship_spec_id = $1::uuid)
                  AND ($2::text IS NULL OR link_type_id = $2)
                ORDER BY created_at DESC
                LIMIT 1
                """,
                relationship_spec_id,
                link_type_id,
            )
            if not row:
                return None
            return self._row_to_relationship_spec(row)

    async def list_relationship_specs(
        self,
        *,
        db_name: Optional[str] = None,
        dataset_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[RelationshipSpecRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT relationship_spec_id, link_type_id, db_name, source_object_type, target_object_type,
                       predicate, spec_type, dataset_id, dataset_version_id, mapping_spec_id, mapping_spec_version,
                       spec, status, auto_sync, last_index_status, last_indexed_at, last_index_result_id,
                       last_index_stats, last_index_dataset_version_id, last_index_mapping_spec_version,
                       created_at, updated_at
                FROM {self._schema}.relationship_specs
                WHERE ($1::text IS NULL OR db_name = $1)
                  AND ($2::uuid IS NULL OR dataset_id = $2::uuid)
                  AND ($3::text IS NULL OR status = $3)
                ORDER BY created_at DESC
                LIMIT $4
                """,
                db_name,
                dataset_id,
                status,
                limit,
            )
            return [self._row_to_relationship_spec(row) for row in rows]

    async def list_relationship_specs_by_relationship_object_type(
        self,
        *,
        db_name: str,
        relationship_object_type: str,
        status: Optional[str] = "ACTIVE",
        limit: int = 200,
    ) -> List[RelationshipSpecRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT relationship_spec_id, link_type_id, db_name, source_object_type, target_object_type,
                       predicate, spec_type, dataset_id, dataset_version_id, mapping_spec_id, mapping_spec_version,
                       spec, status, auto_sync, last_index_status, last_indexed_at, last_index_result_id,
                       last_index_stats, last_index_dataset_version_id, last_index_mapping_spec_version,
                       created_at, updated_at
                FROM {self._schema}.relationship_specs
                WHERE db_name = $1
                  AND spec_type = 'object_backed'
                  AND spec->>'relationship_object_type' = $2
                  AND ($3::text IS NULL OR status = $3)
                ORDER BY created_at DESC
                LIMIT $4
                """,
                db_name,
                relationship_object_type,
                status,
                limit,
            )
            return [self._row_to_relationship_spec(row) for row in rows]

    async def list_relationship_index_results(
        self,
        *,
        relationship_spec_id: Optional[str] = None,
        link_type_id: Optional[str] = None,
        db_name: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[RelationshipIndexResultRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT result_id, relationship_spec_id, link_type_id, db_name, dataset_id, dataset_version_id,
                       mapping_spec_id, mapping_spec_version, status, stats, errors, lineage, created_at
                FROM {self._schema}.relationship_index_results
                WHERE ($1::uuid IS NULL OR relationship_spec_id = $1::uuid)
                  AND ($2::text IS NULL OR link_type_id = $2)
                  AND ($3::text IS NULL OR db_name = $3)
                  AND ($4::text IS NULL OR status = $4)
                ORDER BY created_at DESC
                LIMIT $5
                """,
                relationship_spec_id,
                link_type_id,
                db_name,
                status,
                limit,
            )
            return [self._row_to_relationship_index_result(row) for row in rows]

    async def record_link_edit(
        self,
        *,
        db_name: str,
        link_type_id: str,
        branch: str,
        source_object_type: str,
        target_object_type: str,
        predicate: str,
        source_instance_id: str,
        target_instance_id: str,
        edit_type: str,
        status: str = "ACTIVE",
        metadata: Optional[Dict[str, Any]] = None,
        edit_id: Optional[str] = None,
    ) -> LinkEditRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        edit_id = edit_id or str(uuid4())
        payload = normalize_json_payload(metadata or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.link_edits (
                    edit_id, db_name, link_type_id, branch, source_object_type, target_object_type,
                    predicate, source_instance_id, target_instance_id, edit_type, status, metadata
                ) VALUES (
                    $1::uuid, $2, $3, $4, $5, $6,
                    $7, $8, $9, $10, $11, $12::jsonb
                )
                RETURNING edit_id, db_name, link_type_id, branch, source_object_type, target_object_type,
                          predicate, source_instance_id, target_instance_id, edit_type, status, metadata, created_at
                """,
                edit_id,
                db_name,
                link_type_id,
                branch,
                source_object_type,
                target_object_type,
                predicate,
                source_instance_id,
                target_instance_id,
                edit_type,
                status,
                payload,
            )
            if not row:
                raise RuntimeError("Failed to record link edit")
            return self._row_to_link_edit(row)

    async def list_link_edits(
        self,
        *,
        db_name: str,
        link_type_id: Optional[str] = None,
        branch: Optional[str] = None,
        status: Optional[str] = None,
        source_instance_id: Optional[str] = None,
        target_instance_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[LinkEditRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT edit_id, db_name, link_type_id, branch, source_object_type, target_object_type,
                       predicate, source_instance_id, target_instance_id, edit_type, status, metadata, created_at
                FROM {self._schema}.link_edits
                WHERE db_name = $1
                  AND ($2::text IS NULL OR link_type_id = $2)
                  AND ($3::text IS NULL OR branch = $3)
                  AND ($4::text IS NULL OR status = $4)
                  AND ($5::text IS NULL OR source_instance_id = $5)
                  AND ($6::text IS NULL OR target_instance_id = $6)
                ORDER BY created_at DESC
                LIMIT $7
                """,
                db_name,
                link_type_id,
                branch,
                status,
                source_instance_id,
                target_instance_id,
                limit,
            )
            return [self._row_to_link_edit(row) for row in rows]

    async def clear_link_edits(
        self,
        *,
        db_name: str,
        link_type_id: str,
        branch: Optional[str] = None,
    ) -> int:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                f"""
                DELETE FROM {self._schema}.link_edits
                WHERE db_name = $1 AND link_type_id = $2
                  AND ($3::text IS NULL OR branch = $3)
                """,
                db_name,
                link_type_id,
                branch,
            )
            try:
                return int(str(result).split()[-1])
            except Exception:
                return 0

    async def create_schema_migration_plan(
        self,
        *,
        db_name: str,
        subject_type: str,
        subject_id: str,
        plan: Dict[str, Any],
        status: str = "PENDING",
        plan_id: Optional[str] = None,
    ) -> SchemaMigrationPlanRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        plan_id = plan_id or str(uuid4())
        payload = normalize_json_payload(plan or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.schema_migration_plans (
                    plan_id, db_name, subject_type, subject_id, status, plan
                ) VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb)
                RETURNING plan_id, db_name, subject_type, subject_id, status, plan, created_at, updated_at
                """,
                plan_id,
                db_name,
                subject_type,
                subject_id,
                status,
                payload,
            )
            if not row:
                raise RuntimeError("Failed to create schema migration plan")
            return self._row_to_schema_migration_plan(row)

    async def list_schema_migration_plans(
        self,
        *,
        db_name: Optional[str] = None,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[SchemaMigrationPlanRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT plan_id, db_name, subject_type, subject_id, status, plan, created_at, updated_at
                FROM {self._schema}.schema_migration_plans
                WHERE ($1::text IS NULL OR db_name = $1)
                  AND ($2::text IS NULL OR subject_type = $2)
                  AND ($3::text IS NULL OR subject_id = $3)
                  AND ($4::text IS NULL OR status = $4)
                ORDER BY created_at DESC
                LIMIT $5
                """,
                db_name,
                subject_type,
                subject_id,
                status,
                limit,
            )
            return [self._row_to_schema_migration_plan(row) for row in rows]

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
                       status, lakefs_commit_id, artifact_key, schema_json, schema_status,
                       schema_approved_at, schema_approved_by,
                       sample_json, row_count, source_metadata, error, created_at, updated_at, published_at
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
                schema_status=str(row["schema_status"] or "PENDING"),
                schema_approved_at=row["schema_approved_at"],
                schema_approved_by=row["schema_approved_by"],
                sample_json=coerce_json_dataset(row["sample_json"]) or {},
                row_count=row["row_count"],
                source_metadata=coerce_json_dataset(row["source_metadata"]) or {},
                error=row["error"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                published_at=row["published_at"],
            )

    async def get_ingest_request(
        self,
        *,
        ingest_request_id: str,
    ) -> Optional[DatasetIngestRequestRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT ingest_request_id, dataset_id, db_name, branch, idempotency_key, request_fingerprint,
                       status, lakefs_commit_id, artifact_key, schema_json, schema_status,
                       schema_approved_at, schema_approved_by,
                       sample_json, row_count, source_metadata, error, created_at, updated_at, published_at
                FROM {self._schema}.dataset_ingest_requests
                WHERE ingest_request_id = $1::uuid
                """,
                ingest_request_id,
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
                schema_status=str(row["schema_status"] or "PENDING"),
                schema_approved_at=row["schema_approved_at"],
                schema_approved_by=row["schema_approved_by"],
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
                schema_status=str(row["schema_status"] or "PENDING"),
                schema_approved_at=row["schema_approved_at"],
                schema_approved_by=row["schema_approved_by"],
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
                schema_status=str(row["schema_status"] or "PENDING"),
                schema_approved_at=row["schema_approved_at"],
                schema_approved_by=row["schema_approved_by"],
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

    async def approve_ingest_schema(
        self,
        *,
        ingest_request_id: str,
        schema_json: Optional[Dict[str, Any]] = None,
        approved_by: Optional[str] = None,
    ) -> tuple[DatasetRecord, DatasetIngestRequestRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        schema_payload = normalize_json_payload(schema_json) if schema_json is not None else None
        approved_at = utcnow()
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    f"""
                    SELECT ingest_request_id, dataset_id, db_name, branch, idempotency_key, request_fingerprint,
                           status, lakefs_commit_id, artifact_key, schema_json, schema_status,
                           schema_approved_at, schema_approved_by,
                           sample_json, row_count, source_metadata, error, created_at, updated_at, published_at
                    FROM {self._schema}.dataset_ingest_requests
                    WHERE ingest_request_id = $1::uuid
                    FOR UPDATE
                    """,
                    ingest_request_id,
                )
                if not row:
                    raise RuntimeError("Ingest request not found")
                payload = schema_payload if schema_payload is not None else coerce_json_dataset(row["schema_json"])
                payload = payload or {}
                if not isinstance(payload, dict):
                    raise ValueError("schema_json must be an object")
                if not payload:
                    raise ValueError("schema_json is required to approve")

                dataset_id = str(row["dataset_id"])
                await conn.execute(
                    f"""
                    UPDATE {self._schema}.datasets
                    SET schema_json = $2::jsonb, updated_at = NOW()
                    WHERE dataset_id = $1::uuid
                    """,
                    dataset_id,
                    payload,
                )
                await conn.execute(
                    f"""
                    UPDATE {self._schema}.dataset_ingest_requests
                    SET schema_json = $2::jsonb,
                        schema_status = 'APPROVED',
                        schema_approved_at = $4,
                        schema_approved_by = $3,
                        updated_at = NOW()
                    WHERE ingest_request_id = $1::uuid
                    """,
                    ingest_request_id,
                    payload,
                    approved_by,
                    approved_at,
                )
                dataset_row = await conn.fetchrow(
                    f"""
                    SELECT dataset_id, db_name, name, description, source_type,
                           source_ref, branch, schema_json, created_at, updated_at
                    FROM {self._schema}.datasets
                    WHERE dataset_id = $1::uuid
                    """,
                    dataset_id,
                )
                if not dataset_row:
                    raise RuntimeError("Dataset not found for ingest request")

        dataset_record = DatasetRecord(
            dataset_id=str(dataset_row["dataset_id"]),
            db_name=str(dataset_row["db_name"]),
            name=str(dataset_row["name"]),
            description=dataset_row["description"],
            source_type=str(dataset_row["source_type"]),
            source_ref=dataset_row["source_ref"],
            branch=str(dataset_row["branch"]),
            schema_json=coerce_json_dataset(dataset_row["schema_json"]),
            created_at=dataset_row["created_at"],
            updated_at=dataset_row["updated_at"],
        )
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
        apply_schema: bool = True,
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
                if schema_payload is not None and apply_schema:
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
