"""
Objectify registry (mapping specs + objectify jobs) backed by Postgres.

Tracks dataset→ontology mapping specs and objectify job lifecycle.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

import asyncpg

from shared.config.service_config import ServiceConfig
import json

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
    dataset_id: str
    dataset_version_id: str
    dataset_branch: str
    target_class_id: str
    status: str
    command_id: Optional[str]
    error: Optional[str]
    report: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime]


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
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.objectify_jobs (
                    job_id UUID PRIMARY KEY,
                    mapping_spec_id UUID NOT NULL,
                    mapping_spec_version INTEGER NOT NULL,
                    dataset_id UUID NOT NULL,
                    dataset_version_id UUID NOT NULL,
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

    async def create_mapping_spec(
        self,
        *,
        dataset_id: str,
        dataset_branch: str,
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
                    """,
                    dataset_id,
                    dataset_branch,
                    target_class_id,
                )
                next_version = int(row["current_version"] or 0) + 1 if row else 1

                mapping_spec_id = str(uuid4())
                await conn.execute(
                    f"""
                    INSERT INTO {self._schema}.ontology_mapping_specs (
                        mapping_spec_id, dataset_id, dataset_branch, target_class_id,
                        mappings, target_field_types, status, version, auto_sync, options
                    ) VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9, $10::jsonb)
                    """,
                    mapping_spec_id,
                    dataset_id,
                    dataset_branch,
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
                          AND mapping_spec_id != $4::uuid
                          AND status = 'ACTIVE'
                        """,
                        dataset_id,
                        dataset_branch,
                        target_class_id,
                        mapping_spec_id,
                    )
                record = await conn.fetchrow(
                    f"""
                    SELECT mapping_spec_id, dataset_id, dataset_branch, target_class_id,
                           mappings, target_field_types, status, version, auto_sync, options,
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
                SELECT mapping_spec_id, dataset_id, dataset_branch, target_class_id,
                       mappings, target_field_types, status, version, auto_sync, options,
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
                SELECT mapping_spec_id, dataset_id, dataset_branch, target_class_id,
                       mappings, target_field_types, status, version, auto_sync, options,
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
    ) -> Optional[OntologyMappingSpecRecord]:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        dataset_branch = dataset_branch or "main"
        clause = "dataset_id = $1::uuid AND dataset_branch = $2 AND status = 'ACTIVE'"
        params: List[Any] = [dataset_id, dataset_branch]
        if target_class_id:
            clause += " AND target_class_id = $3"
            params.append(target_class_id)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT mapping_spec_id, dataset_id, dataset_branch, target_class_id,
                       mappings, target_field_types, status, version, auto_sync, options,
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
        dataset_version_id: str,
        dataset_branch: str,
        target_class_id: str,
        status: str = "QUEUED",
    ) -> ObjectifyJobRecord:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        dataset_branch = dataset_branch or "main"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.objectify_jobs (
                    job_id, mapping_spec_id, mapping_spec_version, dataset_id, dataset_version_id,
                    dataset_branch, target_class_id, status
                ) VALUES ($1::uuid, $2::uuid, $3, $4::uuid, $5::uuid, $6, $7, $8)
                RETURNING job_id, mapping_spec_id, mapping_spec_version, dataset_id, dataset_version_id,
                          dataset_branch, target_class_id, status, command_id, error, report,
                          created_at, updated_at, completed_at
                """,
                job_id,
                mapping_spec_id,
                int(mapping_spec_version),
                dataset_id,
                dataset_version_id,
                dataset_branch,
                target_class_id,
                status,
            )
            if not row:
                raise RuntimeError("Failed to create objectify job")
            return ObjectifyJobRecord(
                job_id=str(row["job_id"]),
                mapping_spec_id=str(row["mapping_spec_id"]),
                mapping_spec_version=int(row["mapping_spec_version"]),
                dataset_id=str(row["dataset_id"]),
                dataset_version_id=str(row["dataset_version_id"]),
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

    async def get_objectify_job(self, *, job_id: str) -> Optional[ObjectifyJobRecord]:
        if not self._pool:
            raise RuntimeError("ObjectifyRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT job_id, mapping_spec_id, mapping_spec_version, dataset_id, dataset_version_id,
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
                dataset_id=str(row["dataset_id"]),
                dataset_version_id=str(row["dataset_version_id"]),
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
        clause = "dataset_version_id = $1::uuid AND mapping_spec_id = $2::uuid AND mapping_spec_version = $3"
        params: List[Any] = [dataset_version_id, mapping_spec_id, int(mapping_spec_version)]
        if statuses:
            clause += " AND status = ANY($4::text[])"
            params.append([str(s) for s in statuses])
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT job_id, mapping_spec_id, mapping_spec_version, dataset_id, dataset_version_id,
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
                dataset_id=str(row["dataset_id"]),
                dataset_version_id=str(row["dataset_version_id"]),
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
                RETURNING job_id, mapping_spec_id, mapping_spec_version, dataset_id, dataset_version_id,
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
                dataset_id=str(row["dataset_id"]),
                dataset_version_id=str(row["dataset_version_id"]),
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
