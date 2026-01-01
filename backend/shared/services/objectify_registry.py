"""
Objectify registry (mapping specs + objectify jobs) backed by Postgres.

Tracks dataset→ontology mapping specs and objectify job lifecycle.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import asyncpg
from asyncpg.exceptions import UniqueViolationError

from shared.config.service_config import ServiceConfig

from shared.models.objectify_job import ObjectifyJob
from shared.utils.json_utils import coerce_json_dataset, normalize_json_payload


def _coerce_json_list(value: Any) -> List[Dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, dict):
        return [value]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
        return []
    try:
        return list(value)
    except Exception:
        return []


@dataclass(frozen=True)
class OntologyMappingSpecRecord:
    mapping_spec_id: str
    dataset_id: str
    dataset_branch: str
    artifact_output_name: Optional[str]
    schema_hash: Optional[str]
    target_class_id: str
    mappings: List[Dict[str, Any]]
    target_field_types: Dict[str, str]
    status: str
    version: int
    auto_sync: bool
    options: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ObjectifyJobRecord:
    job_id: str
    mapping_spec_id: str
    mapping_spec_version: int
    dedupe_key: Optional[str]
    dataset_id: str
    dataset_version_id: Optional[str]
    artifact_id: Optional[str]
    artifact_output_name: Optional[str]
    dataset_branch: str
    target_class_id: str
    status: str
    command_id: Optional[str]
    error: Optional[str]
    report: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime]


@dataclass(frozen=True)
class ObjectifyOutboxItem:
    outbox_id: str
    job_id: str
    payload: Dict[str, Any]
    status: str
    publish_attempts: int
    error: Optional[str]
    claimed_by: Optional[str]
    claimed_at: Optional[datetime]
    next_attempt_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


class ObjectifyRegistry:
    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_objectify",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
    ) -> None:
        self._dsn = dsn or ServiceConfig.get_postgres_url()
        self._schema = schema
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_min = int(os.getenv("OBJECTIFY_PG_POOL_MIN", str(pool_min or 1)))
        self._pool_max = int(os.getenv("OBJECTIFY_PG_POOL_MAX", str(pool_max or 5)))

    async def initialize(self) -> None:
        await self.connect()

    async def connect(self) -> None:
        if self._pool:
            return
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._pool_min,
            max_size=self._pool_max,
            command_timeout=int(os.getenv("OBJECTIFY_PG_COMMAND_TIMEOUT", "30")),
        )
        await self.ensure_schema()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def ensure_schema(self) -> None:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.ontology_mapping_specs (
                    mapping_spec_id UUID PRIMARY KEY,
                    dataset_id UUID NOT NULL,
                    dataset_branch TEXT NOT NULL DEFAULT 'main',
                    artifact_output_name TEXT,
                    schema_hash TEXT,
                    target_class_id TEXT NOT NULL,
                    mappings JSONB NOT NULL DEFAULT '[]'::jsonb,
                    target_field_types JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    version INTEGER NOT NULL DEFAULT 1,
                    auto_sync BOOLEAN NOT NULL DEFAULT true,
                    options JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_mapping_specs_dataset
                ON {self._schema}.ontology_mapping_specs(dataset_id, dataset_branch, target_class_id)
                """
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.ontology_mapping_specs ADD COLUMN IF NOT EXISTS artifact_output_name TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.ontology_mapping_specs ADD COLUMN IF NOT EXISTS schema_hash TEXT"
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_mapping_specs_output
                ON {self._schema}.ontology_mapping_specs(dataset_id, dataset_branch, artifact_output_name, schema_hash, status)
                """
            )
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.objectify_jobs (
                    job_id UUID PRIMARY KEY,
                    mapping_spec_id UUID NOT NULL,
                    mapping_spec_version INTEGER NOT NULL,
                    dedupe_key TEXT,
                    dataset_id UUID NOT NULL,
                    dataset_version_id UUID,
                    artifact_id UUID,
                    artifact_output_name TEXT,
                    dataset_branch TEXT NOT NULL DEFAULT 'main',
                    target_class_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'QUEUED',
                    command_id TEXT,
                    error TEXT,
                    report JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    completed_at TIMESTAMPTZ,
                    FOREIGN KEY (mapping_spec_id)
                        REFERENCES {self._schema}.ontology_mapping_specs(mapping_spec_id)
                        ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.objectify_job_outbox (
                    outbox_id UUID PRIMARY KEY,
                    job_id UUID NOT NULL,
                    payload JSONB NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    publish_attempts INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    claimed_by TEXT,
                    claimed_at TIMESTAMPTZ,
                    next_attempt_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    FOREIGN KEY (job_id)
                        REFERENCES {self._schema}.objectify_jobs(job_id)
                        ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_objectify_outbox_status
                ON {self._schema}.objectify_job_outbox(status, next_attempt_at, created_at)
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_objectify_outbox_claimed
                ON {self._schema}.objectify_job_outbox(status, claimed_at)
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_objectify_outbox_job
                ON {self._schema}.objectify_job_outbox(job_id, status, created_at)
                """
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.objectify_job_outbox ADD COLUMN IF NOT EXISTS claimed_by TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.objectify_job_outbox ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.objectify_jobs ADD COLUMN IF NOT EXISTS artifact_id UUID"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.objectify_jobs ADD COLUMN IF NOT EXISTS artifact_output_name TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.objectify_jobs ADD COLUMN IF NOT EXISTS dedupe_key TEXT"
            )
            await conn.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_objectify_jobs_dedupe_key
                ON {self._schema}.objectify_jobs(dedupe_key)
                WHERE dedupe_key IS NOT NULL
                """
            )
            await conn.execute(
                f"""
                UPDATE {self._schema}.objectify_jobs
                SET dedupe_key = COALESCE(dedupe_key, job_id::text)
                WHERE dedupe_key IS NULL
                """
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.objectify_jobs ALTER COLUMN dataset_version_id DROP NOT NULL"
            )
            await conn.execute(
                f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'objectify_jobs_input_xor'
                          AND conrelid = '{self._schema}.objectify_jobs'::regclass
                    ) THEN
                        ALTER TABLE {self._schema}.objectify_jobs
                        ADD CONSTRAINT objectify_jobs_input_xor
                        CHECK (
                            (dataset_version_id IS NOT NULL AND artifact_id IS NULL)
                            OR (dataset_version_id IS NULL AND artifact_id IS NOT NULL)
                        );
                    END IF;
                END $$;
                """
            )
            await conn.execute(
                f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'objectify_jobs_artifact_output_required'
                          AND conrelid = '{self._schema}.objectify_jobs'::regclass
                    ) THEN
                        ALTER TABLE {self._schema}.objectify_jobs
                        ADD CONSTRAINT objectify_jobs_artifact_output_required
                        CHECK (
                            artifact_id IS NULL
                            OR (artifact_output_name IS NOT NULL AND length(trim(artifact_output_name)) > 0)
                        );
                    END IF;
                END $$;
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_objectify_jobs_status
                ON {self._schema}.objectify_jobs(status, created_at)
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_objectify_jobs_version
                ON {self._schema}.objectify_jobs(dataset_version_id, mapping_spec_id, mapping_spec_version, status)
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_objectify_jobs_artifact
                ON {self._schema}.objectify_jobs(artifact_id, artifact_output_name, mapping_spec_id, mapping_spec_version, status)
                """
            )

    @staticmethod
    def _normalize_optional(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @staticmethod
    def build_dedupe_key(
        *,
        dataset_id: str,
        dataset_branch: str,
        mapping_spec_id: str,
        mapping_spec_version: int,
        dataset_version_id: Optional[str],
        artifact_id: Optional[str],
        artifact_output_name: Optional[str],
    ) -> str:
        parts: List[str] = [
            f"spec:{mapping_spec_id}",
            f"version:{int(mapping_spec_version)}",
            f"dataset:{dataset_id}",
            f"branch:{dataset_branch or 'main'}",
        ]
        if dataset_version_id:
            parts.append(f"dataset_version:{dataset_version_id}")
        if artifact_id:
            parts.append(f"artifact:{artifact_id}")
            if artifact_output_name:
                parts.append(f"output:{artifact_output_name}")
        return "|".join(parts)

    def _validate_objectify_inputs(
        self,
        *,
        dataset_version_id: Optional[str],
        artifact_id: Optional[str],
        artifact_output_name: Optional[str],
    ) -> None:
        has_version = bool(dataset_version_id)
        has_artifact = bool(artifact_id)
        if has_version == has_artifact:
            raise ValueError("Exactly one of dataset_version_id or artifact_id is required")
        if has_artifact and not artifact_output_name:
            raise ValueError("artifact_output_name is required when artifact_id is set")

    async def create_mapping_spec(
        self,
        *,
        dataset_id: str,
        dataset_branch: str,
        artifact_output_name: str,
        schema_hash: str,
        target_class_id: str,
        mappings: List[Dict[str, Any]],
        target_field_types: Optional[Dict[str, str]] = None,
        status: str = "ACTIVE",
        auto_sync: bool = True,
        options: Optional[Dict[str, Any]] = None,
    ) -> OntologyMappingSpecRecord:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        dataset_branch = dataset_branch or "main"
        artifact_output_name = self._normalize_optional(artifact_output_name) or ""
        schema_hash = self._normalize_optional(schema_hash) or ""
        if not artifact_output_name:
            raise ValueError("artifact_output_name is required")
        if not schema_hash:
            raise ValueError("schema_hash is required")
        target_field_types = target_field_types or {}
        options = options or {}
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    f"""
                    SELECT COALESCE(MAX(version), 0) AS current_version
                    FROM {self._schema}.ontology_mapping_specs
                    WHERE dataset_id = $1::uuid
                      AND dataset_branch = $2
                      AND target_class_id = $3
                      AND artifact_output_name = $4
                      AND schema_hash = $5
                    """,
                    dataset_id,
                    dataset_branch,
                    target_class_id,
                    artifact_output_name,
                    schema_hash,
                )
                next_version = int(row["current_version"] or 0) + 1 if row else 1

                mapping_spec_id = str(uuid4())
                await conn.execute(
                    f"""
                    INSERT INTO {self._schema}.ontology_mapping_specs (
                        mapping_spec_id, dataset_id, dataset_branch, artifact_output_name, schema_hash,
                        target_class_id, mappings, target_field_types, status, version, auto_sync, options
                    ) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, $11, $12::jsonb)
                    """,
                    mapping_spec_id,
                    dataset_id,
                    dataset_branch,
                    artifact_output_name,
                    schema_hash,
                    target_class_id,
                    normalize_json_payload(mappings),
                    normalize_json_payload(target_field_types),
                    status,
                    next_version,
                    bool(auto_sync),
                    normalize_json_payload(options),
                )
                if status.upper() == "ACTIVE":
                    await conn.execute(
                        f"""
                        UPDATE {self._schema}.ontology_mapping_specs
                        SET status = 'INACTIVE', updated_at = NOW()
                        WHERE dataset_id = $1::uuid
                          AND dataset_branch = $2
                          AND target_class_id = $3
                          AND artifact_output_name = $4
                          AND schema_hash = $5
                          AND mapping_spec_id != $6::uuid
                          AND status = 'ACTIVE'
                        """,
                        dataset_id,
                        dataset_branch,
                        target_class_id,
                        artifact_output_name,
                        schema_hash,
                        mapping_spec_id,
                    )
                record = await conn.fetchrow(
                    f"""
                    SELECT mapping_spec_id, dataset_id, dataset_branch, artifact_output_name, schema_hash,
                           target_class_id, mappings, target_field_types, status, version, auto_sync, options,
                           created_at, updated_at
                    FROM {self._schema}.ontology_mapping_specs
                    WHERE mapping_spec_id = $1::uuid
                    """,
                    mapping_spec_id,
                )
            if not record:
                raise RuntimeError("Failed to create mapping spec")
            return OntologyMappingSpecRecord(
                mapping_spec_id=str(record["mapping_spec_id"]),
                dataset_id=str(record["dataset_id"]),
                dataset_branch=str(record["dataset_branch"]),
                artifact_output_name=record["artifact_output_name"],
                schema_hash=record["schema_hash"],
                target_class_id=str(record["target_class_id"]),
                mappings=_coerce_json_list(record["mappings"]),
                target_field_types=coerce_json_dataset(record["target_field_types"]) or {},
                status=str(record["status"]),
                version=int(record["version"]),
                auto_sync=bool(record["auto_sync"]),
                options=coerce_json_dataset(record["options"]) or {},
                created_at=record["created_at"],
                updated_at=record["updated_at"],
            )

    async def get_mapping_spec(self, *, mapping_spec_id: str) -> Optional[OntologyMappingSpecRecord]:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT mapping_spec_id, dataset_id, dataset_branch, artifact_output_name, schema_hash,
                       target_class_id, mappings, target_field_types, status, version, auto_sync, options,
                       created_at, updated_at
                FROM {self._schema}.ontology_mapping_specs
                WHERE mapping_spec_id = $1::uuid
                """,
                mapping_spec_id,
            )
            if not row:
                return None
            return OntologyMappingSpecRecord(
                mapping_spec_id=str(row["mapping_spec_id"]),
                dataset_id=str(row["dataset_id"]),
                dataset_branch=str(row["dataset_branch"]),
                artifact_output_name=row["artifact_output_name"],
                schema_hash=row["schema_hash"],
                target_class_id=str(row["target_class_id"]),
                mappings=_coerce_json_list(row["mappings"]),
                target_field_types=coerce_json_dataset(row["target_field_types"]) or {},
                status=str(row["status"]),
                version=int(row["version"]),
                auto_sync=bool(row["auto_sync"]),
                options=coerce_json_dataset(row["options"]) or {},
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def list_mapping_specs(
        self,
        *,
        dataset_id: Optional[str] = None,
        include_inactive: bool = False,
    ) -> List[OntologyMappingSpecRecord]:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        clause = ""
        params: List[Any] = []
        if dataset_id:
            clause = "WHERE dataset_id = $1::uuid"
            params.append(dataset_id)
            if not include_inactive:
                clause += " AND status = 'ACTIVE'"
        elif not include_inactive:
            clause = "WHERE status = 'ACTIVE'"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT mapping_spec_id, dataset_id, dataset_branch, artifact_output_name, schema_hash,
                       target_class_id, mappings, target_field_types, status, version, auto_sync, options,
                       created_at, updated_at
                FROM {self._schema}.ontology_mapping_specs
                {clause}
                ORDER BY created_at DESC
                """,
                *params,
            )
            records = []
            for row in rows:
                records.append(
                    OntologyMappingSpecRecord(
                        mapping_spec_id=str(row["mapping_spec_id"]),
                        dataset_id=str(row["dataset_id"]),
                        dataset_branch=str(row["dataset_branch"]),
                        artifact_output_name=row["artifact_output_name"],
                        schema_hash=row["schema_hash"],
                        target_class_id=str(row["target_class_id"]),
                        mappings=_coerce_json_list(row["mappings"]),
                        target_field_types=coerce_json_dataset(row["target_field_types"]) or {},
                        status=str(row["status"]),
                        version=int(row["version"]),
                        auto_sync=bool(row["auto_sync"]),
                        options=coerce_json_dataset(row["options"]) or {},
                        created_at=row["created_at"],
                        updated_at=row["updated_at"],
                    )
                )
            return records

    async def get_active_mapping_spec(
        self,
        *,
        dataset_id: str,
        dataset_branch: str,
        target_class_id: Optional[str] = None,
        artifact_output_name: Optional[str] = None,
        schema_hash: Optional[str] = None,
    ) -> Optional[OntologyMappingSpecRecord]:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        dataset_branch = dataset_branch or "main"
        clause = "dataset_id = $1::uuid AND dataset_branch = $2 AND status = 'ACTIVE'"
        params: List[Any] = [dataset_id, dataset_branch]
        param_index = len(params) + 1
        if target_class_id:
            clause += f" AND target_class_id = ${param_index}"
            params.append(target_class_id)
            param_index += 1
        if artifact_output_name:
            clause += f" AND artifact_output_name = ${param_index}"
            params.append(artifact_output_name)
            param_index += 1
        if schema_hash:
            clause += f" AND schema_hash = ${param_index}"
            params.append(schema_hash)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT mapping_spec_id, dataset_id, dataset_branch, artifact_output_name, schema_hash,
                       target_class_id, mappings, target_field_types, status, version, auto_sync, options,
                       created_at, updated_at
                FROM {self._schema}.ontology_mapping_specs
                WHERE {clause}
                ORDER BY version DESC, created_at DESC
                LIMIT 1
                """,
                *params,
            )
            if not row:
                return None
            return OntologyMappingSpecRecord(
                mapping_spec_id=str(row["mapping_spec_id"]),
                dataset_id=str(row["dataset_id"]),
                dataset_branch=str(row["dataset_branch"]),
                artifact_output_name=row["artifact_output_name"],
                schema_hash=row["schema_hash"],
                target_class_id=str(row["target_class_id"]),
                mappings=_coerce_json_list(row["mappings"]),
                target_field_types=coerce_json_dataset(row["target_field_types"]) or {},
                status=str(row["status"]),
                version=int(row["version"]),
                auto_sync=bool(row["auto_sync"]),
                options=coerce_json_dataset(row["options"]) or {},
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def create_objectify_job(
        self,
        *,
        job_id: str,
        mapping_spec_id: str,
        mapping_spec_version: int,
        dataset_id: str,
        dataset_version_id: Optional[str] = None,
        artifact_id: Optional[str] = None,
        artifact_output_name: Optional[str] = None,
        dataset_branch: str,
        target_class_id: str,
        status: str = "QUEUED",
        outbox_payload: Optional[Dict[str, Any]] = None,
        dedupe_key: Optional[str] = None,
    ) -> ObjectifyJobRecord:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        dataset_branch = dataset_branch or "main"
        dataset_version_id = self._normalize_optional(dataset_version_id)
        artifact_id = self._normalize_optional(artifact_id)
        artifact_output_name = self._normalize_optional(artifact_output_name)
        self._validate_objectify_inputs(
            dataset_version_id=dataset_version_id,
            artifact_id=artifact_id,
            artifact_output_name=artifact_output_name,
        )
        dedupe_key = dedupe_key or self.build_dedupe_key(
            dataset_id=dataset_id,
            dataset_branch=dataset_branch,
            mapping_spec_id=mapping_spec_id,
            mapping_spec_version=mapping_spec_version,
            dataset_version_id=dataset_version_id,
            artifact_id=artifact_id,
            artifact_output_name=artifact_output_name,
        )
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = None
                try:
                    row = await conn.fetchrow(
                        f"""
                        INSERT INTO {self._schema}.objectify_jobs (
                            job_id, mapping_spec_id, mapping_spec_version, dedupe_key, dataset_id, dataset_version_id,
                            artifact_id, artifact_output_name, dataset_branch, target_class_id, status
                        ) VALUES ($1::uuid, $2::uuid, $3, $4, $5::uuid, $6::uuid, $7::uuid, $8, $9, $10, $11)
                        RETURNING job_id, mapping_spec_id, mapping_spec_version, dedupe_key, dataset_id, dataset_version_id,
                                  artifact_id, artifact_output_name,
                                  dataset_branch, target_class_id, status, command_id, error, report,
                                  created_at, updated_at, completed_at
                        """,
                        job_id,
                        mapping_spec_id,
                        int(mapping_spec_version),
                        dedupe_key,
                        dataset_id,
                        dataset_version_id,
                        artifact_id,
                        artifact_output_name,
                        dataset_branch,
                        target_class_id,
                        status,
                    )
                except UniqueViolationError:
                    row = await conn.fetchrow(
                        f"""
                        SELECT job_id, mapping_spec_id, mapping_spec_version, dedupe_key, dataset_id, dataset_version_id,
                               artifact_id, artifact_output_name,
                               dataset_branch, target_class_id, status, command_id, error, report,
                               created_at, updated_at, completed_at
                        FROM {self._schema}.objectify_jobs
                        WHERE dedupe_key = $1
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        dedupe_key,
                    )
                if outbox_payload is not None:
                    await conn.execute(
                        f"""
                        INSERT INTO {self._schema}.objectify_job_outbox (
                            outbox_id, job_id, payload, status, created_at, updated_at
                        ) VALUES ($1::uuid, $2::uuid, $3::jsonb, 'pending', NOW(), NOW())
                        """,
                        str(uuid4()),
                        job_id,
                        normalize_json_payload(outbox_payload),
                    )
            if not row:
                raise RuntimeError("Failed to create objectify job")
            return ObjectifyJobRecord(
                job_id=str(row["job_id"]),
                mapping_spec_id=str(row["mapping_spec_id"]),
                mapping_spec_version=int(row["mapping_spec_version"]),
                dedupe_key=row["dedupe_key"],
                dataset_id=str(row["dataset_id"]),
                dataset_version_id=str(row["dataset_version_id"]) if row["dataset_version_id"] else None,
                artifact_id=str(row["artifact_id"]) if row["artifact_id"] else None,
                artifact_output_name=str(row["artifact_output_name"]) if row["artifact_output_name"] else None,
                dataset_branch=str(row["dataset_branch"]),
                target_class_id=str(row["target_class_id"]),
                status=str(row["status"]),
                command_id=row["command_id"],
                error=row["error"],
                report=coerce_json_dataset(row["report"]) or {},
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                completed_at=row["completed_at"],
            )

    async def get_objectify_metrics(self) -> Dict[str, Any]:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        async with self._pool.acquire() as conn:
            outbox_rows = await conn.fetch(
                f"""
                SELECT status, count(*) AS count, MIN(created_at) AS oldest_created_at,
                       MIN(next_attempt_at) AS next_attempt_at,
                       MAX(publish_attempts) AS max_attempts
                FROM {self._schema}.objectify_job_outbox
                GROUP BY status
                """
            )
            job_rows = await conn.fetch(
                f"""
                SELECT status, count(*) AS count, MIN(created_at) AS oldest_created_at
                FROM {self._schema}.objectify_jobs
                GROUP BY status
                """
            )

        outbox_counts = {str(row["status"]): int(row["count"]) for row in outbox_rows or []}
        job_counts = {str(row["status"]): int(row["count"]) for row in job_rows or []}
        oldest_created = min((row["oldest_created_at"] for row in outbox_rows or [] if row["oldest_created_at"]), default=None)
        next_attempt = min((row["next_attempt_at"] for row in outbox_rows or [] if row["next_attempt_at"]), default=None)
        now = datetime.now(timezone.utc)
        oldest_age_seconds = None
        if oldest_created:
            try:
                oldest_age_seconds = int((now - oldest_created).total_seconds())
            except Exception:
                oldest_age_seconds = None

        return {
            "outbox": {
                "counts": outbox_counts,
                "oldest_created_at": oldest_created.isoformat() if oldest_created else None,
                "oldest_age_seconds": oldest_age_seconds,
                "next_attempt_at": next_attempt.isoformat() if next_attempt else None,
            },
            "jobs": {
                "counts": job_counts,
            },
        }

    async def enqueue_objectify_job(self, *, job: ObjectifyJob) -> ObjectifyJobRecord:
        payload = job.model_dump(mode="json")
        dedupe_key = job.dedupe_key or self.build_dedupe_key(
            dataset_id=job.dataset_id,
            dataset_branch=job.dataset_branch,
            mapping_spec_id=job.mapping_spec_id,
            mapping_spec_version=job.mapping_spec_version,
            dataset_version_id=job.dataset_version_id,
            artifact_id=job.artifact_id,
            artifact_output_name=job.artifact_output_name,
        )
        return await self.create_objectify_job(
            job_id=job.job_id,
            mapping_spec_id=job.mapping_spec_id,
            mapping_spec_version=job.mapping_spec_version,
            dataset_id=job.dataset_id,
            dataset_version_id=job.dataset_version_id,
            artifact_id=job.artifact_id,
            artifact_output_name=job.artifact_output_name,
            dataset_branch=job.dataset_branch,
            target_class_id=job.target_class_id,
            status="ENQUEUE_REQUESTED",
            outbox_payload=payload,
            dedupe_key=dedupe_key,
        )

    async def enqueue_outbox_for_job(
        self,
        *,
        job_id: str,
        payload: Dict[str, Any],
    ) -> str:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        outbox_id = str(uuid4())
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._schema}.objectify_job_outbox (
                    outbox_id, job_id, payload, status, created_at, updated_at
                ) VALUES ($1::uuid, $2::uuid, $3::jsonb, 'pending', NOW(), NOW())
                """,
                outbox_id,
                job_id,
                normalize_json_payload(payload),
            )
        return outbox_id

    async def has_outbox_for_job(
        self,
        *,
        job_id: str,
        statuses: Optional[List[str]] = None,
    ) -> bool:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        clause = "WHERE job_id = $1::uuid"
        values: List[Any] = [job_id]
        if statuses:
            clause += f" AND status = ANY(${len(values) + 1}::text[])"
            values.append(statuses)
        async with self._pool.acquire() as conn:
            row = await conn.fetchval(
                f"""
                SELECT 1
                FROM {self._schema}.objectify_job_outbox
                {clause}
                LIMIT 1
                """,
                *values,
            )
        return bool(row)

    async def claim_objectify_outbox_batch(
        self,
        *,
        limit: int = 50,
        claimed_by: Optional[str] = None,
        claim_timeout_seconds: int = 300,
    ) -> List[ObjectifyOutboxItem]:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
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
                    SELECT outbox_id, job_id, payload, status, publish_attempts, error,
                           claimed_by, claimed_at,
                           next_attempt_at, created_at, updated_at
                    FROM {self._schema}.objectify_job_outbox
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
                    UPDATE {self._schema}.objectify_job_outbox
                    SET status = 'publishing',
                        publish_attempts = publish_attempts + 1,
                        claimed_by = $2,
                        claimed_at = NOW(),
                        updated_at = NOW()
                    WHERE outbox_id = ANY($1::uuid[])
                    """,
                    outbox_ids,
                    claimed_by,
                )
                return [
                    ObjectifyOutboxItem(
                        outbox_id=str(row["outbox_id"]),
                        job_id=str(row["job_id"]),
                        payload=coerce_json_dataset(row["payload"]),
                        status=str(row["status"]),
                        publish_attempts=int(row["publish_attempts"]),
                        error=row["error"],
                        claimed_by=str(row["claimed_by"]) if row["claimed_by"] else None,
                        claimed_at=row["claimed_at"],
                        next_attempt_at=row["next_attempt_at"],
                        created_at=row["created_at"],
                        updated_at=row["updated_at"],
                    )
                    for row in rows
                ]

    async def mark_objectify_outbox_published(self, *, outbox_id: str, job_id: str) -> None:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"""
                    UPDATE {self._schema}.objectify_job_outbox
                    SET status = 'published',
                        updated_at = NOW(),
                        next_attempt_at = NULL,
                        error = NULL,
                        claimed_by = NULL,
                        claimed_at = NULL
                    WHERE outbox_id = $1::uuid
                    """,
                    outbox_id,
                )
                await conn.execute(
                    f"""
                    UPDATE {self._schema}.objectify_jobs
                    SET status = 'ENQUEUED',
                        updated_at = NOW()
                    WHERE job_id = $1::uuid
                      AND status IN ('QUEUED', 'ENQUEUE_REQUESTED')
                    """,
                    job_id,
                )

    async def mark_objectify_outbox_failed(
        self,
        *,
        outbox_id: str,
        error: str,
        next_attempt_at: Optional[datetime] = None,
    ) -> None:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.objectify_job_outbox
                SET status = 'failed',
                    error = $2,
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

    async def purge_objectify_outbox(
        self,
        *,
        retention_days: int = 7,
        limit: int = 10_000,
    ) -> int:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        retention_days = max(1, int(retention_days))
        limit = max(1, int(limit))
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                WITH doomed AS (
                    SELECT ctid
                    FROM {self._schema}.objectify_job_outbox
                    WHERE status = 'published'
                      AND updated_at < NOW() - ($1::int * INTERVAL '1 day')
                    ORDER BY updated_at ASC
                    LIMIT $2
                )
                DELETE FROM {self._schema}.objectify_job_outbox
                WHERE ctid IN (SELECT ctid FROM doomed)
                RETURNING 1
                """,
                retention_days,
                limit,
            )
        return len(rows)

    async def list_objectify_jobs(
        self,
        *,
        statuses: List[str],
        older_than: Optional[datetime] = None,
        limit: int = 200,
    ) -> List[ObjectifyJobRecord]:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        if not statuses:
            return []
        status_clause = "WHERE status = ANY($1::text[])"
        values: List[Any] = [statuses]
        if older_than is not None:
            status_clause += f" AND updated_at < ${len(values) + 1}"
            values.append(older_than)
        status_clause += f" ORDER BY updated_at ASC LIMIT ${len(values) + 1}"
        values.append(limit)
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT job_id, mapping_spec_id, mapping_spec_version, dedupe_key, dataset_id, dataset_version_id,
                       artifact_id, artifact_output_name,
                       dataset_branch, target_class_id, status, command_id, error, report,
                       created_at, updated_at, completed_at
                FROM {self._schema}.objectify_jobs
                {status_clause}
                """,
                *values,
            )
        return [
            ObjectifyJobRecord(
                job_id=str(row["job_id"]),
                mapping_spec_id=str(row["mapping_spec_id"]),
                mapping_spec_version=int(row["mapping_spec_version"]),
                dedupe_key=row["dedupe_key"],
                dataset_id=str(row["dataset_id"]),
                dataset_version_id=str(row["dataset_version_id"]) if row["dataset_version_id"] else None,
                artifact_id=str(row["artifact_id"]) if row["artifact_id"] else None,
                artifact_output_name=str(row["artifact_output_name"]) if row["artifact_output_name"] else None,
                dataset_branch=str(row["dataset_branch"]),
                target_class_id=str(row["target_class_id"]),
                status=str(row["status"]),
                command_id=row["command_id"],
                error=row["error"],
                report=coerce_json_dataset(row["report"]) or {},
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                completed_at=row["completed_at"],
            )
            for row in rows
        ]

    async def get_objectify_job(self, *, job_id: str) -> Optional[ObjectifyJobRecord]:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT job_id, mapping_spec_id, mapping_spec_version, dedupe_key, dataset_id, dataset_version_id,
                       artifact_id, artifact_output_name,
                       dataset_branch, target_class_id, status, command_id, error, report,
                       created_at, updated_at, completed_at
                FROM {self._schema}.objectify_jobs
                WHERE job_id = $1::uuid
                """,
                job_id,
            )
            if not row:
                return None
            return ObjectifyJobRecord(
                job_id=str(row["job_id"]),
                mapping_spec_id=str(row["mapping_spec_id"]),
                mapping_spec_version=int(row["mapping_spec_version"]),
                dedupe_key=row["dedupe_key"],
                dataset_id=str(row["dataset_id"]),
                dataset_version_id=str(row["dataset_version_id"]) if row["dataset_version_id"] else None,
                artifact_id=str(row["artifact_id"]) if row["artifact_id"] else None,
                artifact_output_name=str(row["artifact_output_name"]) if row["artifact_output_name"] else None,
                dataset_branch=str(row["dataset_branch"]),
                target_class_id=str(row["target_class_id"]),
                status=str(row["status"]),
                command_id=row["command_id"],
                error=row["error"],
                report=coerce_json_dataset(row["report"]) or {},
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                completed_at=row["completed_at"],
            )

    async def get_objectify_job_by_dedupe_key(self, *, dedupe_key: str) -> Optional[ObjectifyJobRecord]:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        dedupe_key = self._normalize_optional(dedupe_key)
        if not dedupe_key:
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT job_id, mapping_spec_id, mapping_spec_version, dedupe_key, dataset_id, dataset_version_id,
                       artifact_id, artifact_output_name,
                       dataset_branch, target_class_id, status, command_id, error, report,
                       created_at, updated_at, completed_at
                FROM {self._schema}.objectify_jobs
                WHERE dedupe_key = $1
                ORDER BY created_at DESC
                LIMIT 1
                """,
                dedupe_key,
            )
            if not row:
                return None
            return ObjectifyJobRecord(
                job_id=str(row["job_id"]),
                mapping_spec_id=str(row["mapping_spec_id"]),
                mapping_spec_version=int(row["mapping_spec_version"]),
                dedupe_key=row["dedupe_key"],
                dataset_id=str(row["dataset_id"]),
                dataset_version_id=str(row["dataset_version_id"]) if row["dataset_version_id"] else None,
                artifact_id=str(row["artifact_id"]) if row["artifact_id"] else None,
                artifact_output_name=str(row["artifact_output_name"]) if row["artifact_output_name"] else None,
                dataset_branch=str(row["dataset_branch"]),
                target_class_id=str(row["target_class_id"]),
                status=str(row["status"]),
                command_id=row["command_id"],
                error=row["error"],
                report=coerce_json_dataset(row["report"]) or {},
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                completed_at=row["completed_at"],
            )

    async def find_objectify_job(
        self,
        *,
        dataset_version_id: str,
        mapping_spec_id: str,
        mapping_spec_version: int,
        statuses: Optional[List[str]] = None,
    ) -> Optional[ObjectifyJobRecord]:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        dataset_version_id = self._normalize_optional(dataset_version_id)
        if not dataset_version_id:
            raise ValueError("dataset_version_id is required")
        clause = "dataset_version_id = $1::uuid AND mapping_spec_id = $2::uuid AND mapping_spec_version = $3"
        params: List[Any] = [dataset_version_id, mapping_spec_id, int(mapping_spec_version)]
        if statuses:
            clause += " AND status = ANY($4::text[])"
            params.append([str(s) for s in statuses])
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT job_id, mapping_spec_id, mapping_spec_version, dedupe_key, dataset_id, dataset_version_id,
                       artifact_id, artifact_output_name,
                       dataset_branch, target_class_id, status, command_id, error, report,
                       created_at, updated_at, completed_at
                FROM {self._schema}.objectify_jobs
                WHERE {clause}
                ORDER BY created_at DESC
                LIMIT 1
                """,
                *params,
            )
            if not row:
                return None
            return ObjectifyJobRecord(
                job_id=str(row["job_id"]),
                mapping_spec_id=str(row["mapping_spec_id"]),
                mapping_spec_version=int(row["mapping_spec_version"]),
                dedupe_key=row["dedupe_key"],
                dataset_id=str(row["dataset_id"]),
                dataset_version_id=str(row["dataset_version_id"]) if row["dataset_version_id"] else None,
                artifact_id=str(row["artifact_id"]) if row["artifact_id"] else None,
                artifact_output_name=str(row["artifact_output_name"]) if row["artifact_output_name"] else None,
                dataset_branch=str(row["dataset_branch"]),
                target_class_id=str(row["target_class_id"]),
                status=str(row["status"]),
                command_id=row["command_id"],
                error=row["error"],
                report=coerce_json_dataset(row["report"]) or {},
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                completed_at=row["completed_at"],
            )

    async def find_objectify_job_for_artifact(
        self,
        *,
        artifact_id: str,
        artifact_output_name: str,
        mapping_spec_id: str,
        mapping_spec_version: int,
        statuses: Optional[List[str]] = None,
    ) -> Optional[ObjectifyJobRecord]:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        artifact_id = self._normalize_optional(artifact_id)
        artifact_output_name = self._normalize_optional(artifact_output_name)
        if not artifact_id or not artifact_output_name:
            raise ValueError("artifact_id and artifact_output_name are required")
        clause = (
            "artifact_id = $1::uuid AND artifact_output_name = $2 "
            "AND mapping_spec_id = $3::uuid AND mapping_spec_version = $4"
        )
        params: List[Any] = [artifact_id, artifact_output_name, mapping_spec_id, int(mapping_spec_version)]
        if statuses:
            clause += " AND status = ANY($5::text[])"
            params.append([str(s) for s in statuses])
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT job_id, mapping_spec_id, mapping_spec_version, dedupe_key, dataset_id, dataset_version_id,
                       artifact_id, artifact_output_name,
                       dataset_branch, target_class_id, status, command_id, error, report,
                       created_at, updated_at, completed_at
                FROM {self._schema}.objectify_jobs
                WHERE {clause}
                ORDER BY created_at DESC
                LIMIT 1
                """,
                *params,
            )
            if not row:
                return None
            return ObjectifyJobRecord(
                job_id=str(row["job_id"]),
                mapping_spec_id=str(row["mapping_spec_id"]),
                mapping_spec_version=int(row["mapping_spec_version"]),
                dedupe_key=row["dedupe_key"],
                dataset_id=str(row["dataset_id"]),
                dataset_version_id=str(row["dataset_version_id"]) if row["dataset_version_id"] else None,
                artifact_id=str(row["artifact_id"]) if row["artifact_id"] else None,
                artifact_output_name=str(row["artifact_output_name"]) if row["artifact_output_name"] else None,
                dataset_branch=str(row["dataset_branch"]),
                target_class_id=str(row["target_class_id"]),
                status=str(row["status"]),
                command_id=row["command_id"],
                error=row["error"],
                report=coerce_json_dataset(row["report"]) or {},
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                completed_at=row["completed_at"],
            )

    async def update_objectify_job_status(
        self,
        *,
        job_id: str,
        status: str,
        command_id: Optional[str] = None,
        error: Optional[str] = None,
        report: Optional[Dict[str, Any]] = None,
        completed_at: Optional[datetime] = None,
    ) -> Optional[ObjectifyJobRecord]:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.objectify_jobs
                SET status = $2,
                    command_id = COALESCE($3, command_id),
                    error = COALESCE($4, error),
                    report = COALESCE($5::jsonb, report),
                    completed_at = COALESCE($6, completed_at),
                    updated_at = NOW()
                WHERE job_id = $1::uuid
                RETURNING job_id, mapping_spec_id, mapping_spec_version, dedupe_key, dataset_id, dataset_version_id,
                          artifact_id, artifact_output_name,
                          dataset_branch, target_class_id, status, command_id, error, report,
                          created_at, updated_at, completed_at
                """,
                job_id,
                status,
                command_id,
                error,
                normalize_json_payload(report) if report is not None else None,
                completed_at,
            )
            if not row:
                return None
            return ObjectifyJobRecord(
                job_id=str(row["job_id"]),
                mapping_spec_id=str(row["mapping_spec_id"]),
                mapping_spec_version=int(row["mapping_spec_version"]),
                dedupe_key=row["dedupe_key"],
                dataset_id=str(row["dataset_id"]),
                dataset_version_id=str(row["dataset_version_id"]) if row["dataset_version_id"] else None,
                artifact_id=str(row["artifact_id"]) if row["artifact_id"] else None,
                artifact_output_name=str(row["artifact_output_name"]) if row["artifact_output_name"] else None,
                dataset_branch=str(row["dataset_branch"]),
                target_class_id=str(row["target_class_id"]),
                status=str(row["status"]),
                command_id=row["command_id"],
                error=row["error"],
                report=coerce_json_dataset(row["report"]) or {},
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                completed_at=row["completed_at"],
            )
