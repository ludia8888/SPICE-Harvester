"""
Pipeline Registry (Foundry-style) - durable pipeline metadata in Postgres.

Stores pipeline definitions, versions, and latest preview/build status.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

import asyncpg

from shared.config.service_config import ServiceConfig


@dataclass(frozen=True)
class PipelineRecord:
    pipeline_id: str
    db_name: str
    name: str
    description: Optional[str]
    pipeline_type: str
    location: str
    status: str
    last_preview_status: Optional[str]
    last_preview_at: Optional[datetime]
    last_preview_rows: Optional[int]
    last_preview_sample: Dict[str, Any]
    last_build_status: Optional[str]
    last_build_at: Optional[datetime]
    last_build_output: Dict[str, Any]
    deployed_at: Optional[datetime]
    deployed_version: Optional[int]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class PipelineVersionRecord:
    version_id: str
    pipeline_id: str
    version: int
    definition_json: Dict[str, Any]
    created_at: datetime


class PipelineRegistry:
    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_pipelines",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
    ):
        self._dsn = dsn or ServiceConfig.get_postgres_url()
        self._schema = schema
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_min = int(os.getenv("PIPELINE_REGISTRY_PG_POOL_MIN", str(pool_min or 1)))
        self._pool_max = int(os.getenv("PIPELINE_REGISTRY_PG_POOL_MAX", str(pool_max or 5)))

    async def initialize(self) -> None:
        await self.connect()

    async def connect(self) -> None:
        if self._pool:
            return
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._pool_min,
            max_size=self._pool_max,
            command_timeout=int(os.getenv("PIPELINE_REGISTRY_PG_COMMAND_TIMEOUT", "30")),
        )
        await self.ensure_schema()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def ensure_schema(self) -> None:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")

        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.pipelines (
                    pipeline_id UUID PRIMARY KEY,
                    db_name TEXT NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    pipeline_type TEXT NOT NULL,
                    location TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft',
                    last_preview_status TEXT,
                    last_preview_at TIMESTAMPTZ,
                    last_preview_rows INTEGER,
                    last_preview_sample JSONB NOT NULL DEFAULT '{}'::jsonb,
                    last_build_status TEXT,
                    last_build_at TIMESTAMPTZ,
                    last_build_output JSONB NOT NULL DEFAULT '{}'::jsonb,
                    deployed_at TIMESTAMPTZ,
                    deployed_version INTEGER,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (db_name, name)
                )
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.pipeline_versions (
                    version_id UUID PRIMARY KEY,
                    pipeline_id UUID NOT NULL,
                    version INTEGER NOT NULL,
                    definition_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (pipeline_id, version),
                    FOREIGN KEY (pipeline_id)
                        REFERENCES {self._schema}.pipelines(pipeline_id)
                        ON DELETE CASCADE
                )
                """
            )

            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipelines_db_name ON {self._schema}.pipelines(db_name)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_versions_pipeline_id ON {self._schema}.pipeline_versions(pipeline_id)"
            )

    async def create_pipeline(
        self,
        *,
        db_name: str,
        name: str,
        description: Optional[str],
        pipeline_type: str,
        location: str,
        status: str = "draft",
        pipeline_id: Optional[str] = None,
    ) -> PipelineRecord:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")

        pipeline_id = pipeline_id or str(uuid4())

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.pipelines (
                    pipeline_id, db_name, name, description, pipeline_type, location, status
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING pipeline_id, db_name, name, description, pipeline_type, location, status,
                          last_preview_status, last_preview_at, last_preview_rows, last_preview_sample,
                          last_build_status, last_build_at, last_build_output,
                          deployed_at, deployed_version, created_at, updated_at
                """,
                pipeline_id,
                db_name,
                name,
                description,
                pipeline_type,
                location,
                status,
            )
            if not row:
                raise RuntimeError("Failed to create pipeline")
            return PipelineRecord(
                pipeline_id=str(row["pipeline_id"]),
                db_name=str(row["db_name"]),
                name=str(row["name"]),
                description=row["description"],
                pipeline_type=str(row["pipeline_type"]),
                location=str(row["location"]),
                status=str(row["status"]),
                last_preview_status=row["last_preview_status"],
                last_preview_at=row["last_preview_at"],
                last_preview_rows=row["last_preview_rows"],
                last_preview_sample=dict(row["last_preview_sample"] or {}),
                last_build_status=row["last_build_status"],
                last_build_at=row["last_build_at"],
                last_build_output=dict(row["last_build_output"] or {}),
                deployed_at=row["deployed_at"],
                deployed_version=row["deployed_version"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def list_pipelines(self, *, db_name: str) -> List[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT p.pipeline_id, p.db_name, p.name, p.description, p.pipeline_type, p.location,
                       p.status, p.last_preview_status, p.last_preview_at, p.last_preview_rows,
                       p.last_preview_sample, p.last_build_status, p.last_build_at, p.last_build_output,
                       p.deployed_at, p.deployed_version, p.created_at, p.updated_at,
                       v.version AS latest_version, v.definition_json, v.created_at AS version_created_at
                FROM {self._schema}.pipelines p
                LEFT JOIN LATERAL (
                    SELECT version, definition_json, created_at
                    FROM {self._schema}.pipeline_versions
                    WHERE pipeline_id = p.pipeline_id
                    ORDER BY version DESC
                    LIMIT 1
                ) v ON TRUE
                WHERE p.db_name = $1
                ORDER BY p.updated_at DESC
                """,
                db_name,
            )

        output: List[Dict[str, Any]] = []
        for row in rows or []:
            output.append(
                {
                    "pipeline_id": str(row["pipeline_id"]),
                    "db_name": str(row["db_name"]),
                    "name": str(row["name"]),
                    "description": row["description"],
                    "pipeline_type": str(row["pipeline_type"]),
                    "location": str(row["location"]),
                    "status": str(row["status"]),
                    "last_preview_status": row["last_preview_status"],
                    "last_preview_at": row["last_preview_at"],
                    "last_preview_rows": row["last_preview_rows"],
                    "last_preview_sample": dict(row["last_preview_sample"] or {}),
                    "last_build_status": row["last_build_status"],
                    "last_build_at": row["last_build_at"],
                    "last_build_output": dict(row["last_build_output"] or {}),
                    "deployed_at": row["deployed_at"],
                    "deployed_version": row["deployed_version"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "latest_version": row["latest_version"],
                    "definition_json": dict(row["definition_json"] or {}),
                    "version_created_at": row["version_created_at"],
                }
            )
        return output

    async def get_pipeline(self, *, pipeline_id: str) -> Optional[PipelineRecord]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT pipeline_id, db_name, name, description, pipeline_type, location, status,
                       last_preview_status, last_preview_at, last_preview_rows, last_preview_sample,
                       last_build_status, last_build_at, last_build_output,
                       deployed_at, deployed_version, created_at, updated_at
                FROM {self._schema}.pipelines
                WHERE pipeline_id = $1
                """,
                pipeline_id,
            )
            if not row:
                return None
            return PipelineRecord(
                pipeline_id=str(row["pipeline_id"]),
                db_name=str(row["db_name"]),
                name=str(row["name"]),
                description=row["description"],
                pipeline_type=str(row["pipeline_type"]),
                location=str(row["location"]),
                status=str(row["status"]),
                last_preview_status=row["last_preview_status"],
                last_preview_at=row["last_preview_at"],
                last_preview_rows=row["last_preview_rows"],
                last_preview_sample=dict(row["last_preview_sample"] or {}),
                last_build_status=row["last_build_status"],
                last_build_at=row["last_build_at"],
                last_build_output=dict(row["last_build_output"] or {}),
                deployed_at=row["deployed_at"],
                deployed_version=row["deployed_version"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def get_pipeline_by_name(self, *, db_name: str, name: str) -> Optional[PipelineRecord]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT pipeline_id, db_name, name, description, pipeline_type, location, status,
                       last_preview_status, last_preview_at, last_preview_rows, last_preview_sample,
                       last_build_status, last_build_at, last_build_output,
                       deployed_at, deployed_version, created_at, updated_at
                FROM {self._schema}.pipelines
                WHERE db_name = $1 AND name = $2
                """,
                db_name,
                name,
            )
            if not row:
                return None
            return PipelineRecord(
                pipeline_id=str(row["pipeline_id"]),
                db_name=str(row["db_name"]),
                name=str(row["name"]),
                description=row["description"],
                pipeline_type=str(row["pipeline_type"]),
                location=str(row["location"]),
                status=str(row["status"]),
                last_preview_status=row["last_preview_status"],
                last_preview_at=row["last_preview_at"],
                last_preview_rows=row["last_preview_rows"],
                last_preview_sample=dict(row["last_preview_sample"] or {}),
                last_build_status=row["last_build_status"],
                last_build_at=row["last_build_at"],
                last_build_output=dict(row["last_build_output"] or {}),
                deployed_at=row["deployed_at"],
                deployed_version=row["deployed_version"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def update_pipeline(
        self,
        *,
        pipeline_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        location: Optional[str] = None,
        status: Optional[str] = None,
    ) -> PipelineRecord:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        fields: List[str] = []
        values: List[Any] = []

        def _append(field: str, value: Any) -> None:
            fields.append(f"{field} = ${len(values) + 1}")
            values.append(value)

        if name is not None:
            _append("name", name)
        if description is not None:
            _append("description", description)
        if location is not None:
            _append("location", location)
        if status is not None:
            _append("status", status)

        if not fields:
            pipeline = await self.get_pipeline(pipeline_id=pipeline_id)
            if not pipeline:
                raise RuntimeError("Pipeline not found")
            return pipeline

        values.append(pipeline_id)
        set_clause = ", ".join(fields) + ", updated_at = NOW()"

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.pipelines
                SET {set_clause}
                WHERE pipeline_id = ${len(values)}
                RETURNING pipeline_id, db_name, name, description, pipeline_type, location, status,
                          last_preview_status, last_preview_at, last_preview_rows, last_preview_sample,
                          last_build_status, last_build_at, last_build_output,
                          deployed_at, deployed_version, created_at, updated_at
                """,
                *values,
            )
            if not row:
                raise RuntimeError("Failed to update pipeline")
            return PipelineRecord(
                pipeline_id=str(row["pipeline_id"]),
                db_name=str(row["db_name"]),
                name=str(row["name"]),
                description=row["description"],
                pipeline_type=str(row["pipeline_type"]),
                location=str(row["location"]),
                status=str(row["status"]),
                last_preview_status=row["last_preview_status"],
                last_preview_at=row["last_preview_at"],
                last_preview_rows=row["last_preview_rows"],
                last_preview_sample=dict(row["last_preview_sample"] or {}),
                last_build_status=row["last_build_status"],
                last_build_at=row["last_build_at"],
                last_build_output=dict(row["last_build_output"] or {}),
                deployed_at=row["deployed_at"],
                deployed_version=row["deployed_version"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def add_version(
        self,
        *,
        pipeline_id: str,
        definition_json: Optional[Dict[str, Any]] = None,
        version_id: Optional[str] = None,
    ) -> PipelineVersionRecord:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        version_id = version_id or str(uuid4())
        definition_json = definition_json or {}

        async with self._pool.acquire() as conn:
            version = await conn.fetchval(
                f"SELECT COALESCE(MAX(version), 0) + 1 FROM {self._schema}.pipeline_versions WHERE pipeline_id = $1",
                pipeline_id,
            )
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.pipeline_versions (
                    version_id, pipeline_id, version, definition_json
                ) VALUES ($1, $2, $3, $4)
                RETURNING version_id, pipeline_id, version, definition_json, created_at
                """,
                version_id,
                pipeline_id,
                int(version),
                definition_json,
            )
            if not row:
                raise RuntimeError("Failed to create pipeline version")
            return PipelineVersionRecord(
                version_id=str(row["version_id"]),
                pipeline_id=str(row["pipeline_id"]),
                version=int(row["version"]),
                definition_json=dict(row["definition_json"] or {}),
                created_at=row["created_at"],
            )

    async def get_latest_version(self, *, pipeline_id: str) -> Optional[PipelineVersionRecord]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT version_id, pipeline_id, version, definition_json, created_at
                FROM {self._schema}.pipeline_versions
                WHERE pipeline_id = $1
                ORDER BY version DESC
                LIMIT 1
                """,
                pipeline_id,
            )
            if not row:
                return None
            return PipelineVersionRecord(
                version_id=str(row["version_id"]),
                pipeline_id=str(row["pipeline_id"]),
                version=int(row["version"]),
                definition_json=dict(row["definition_json"] or {}),
                created_at=row["created_at"],
            )

    async def record_preview(
        self,
        *,
        pipeline_id: str,
        status: str,
        row_count: Optional[int],
        sample_json: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        sample_json = sample_json or {}
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.pipelines
                SET last_preview_status = $2,
                    last_preview_at = NOW(),
                    last_preview_rows = $3,
                    last_preview_sample = $4,
                    updated_at = NOW()
                WHERE pipeline_id = $1
                """,
                pipeline_id,
                status,
                row_count,
                sample_json,
            )

    async def record_build(
        self,
        *,
        pipeline_id: str,
        status: str,
        output_json: Optional[Dict[str, Any]] = None,
        deployed_version: Optional[int] = None,
    ) -> None:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        output_json = output_json or {}
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.pipelines
                SET last_build_status = $2,
                    last_build_at = NOW(),
                    last_build_output = $3,
                    deployed_at = CASE WHEN $4::int IS NULL THEN deployed_at ELSE NOW() END,
                    deployed_version = COALESCE($4::int, deployed_version),
                    updated_at = NOW()
                WHERE pipeline_id = $1
                """,
                pipeline_id,
                status,
                output_json,
                deployed_version,
            )
