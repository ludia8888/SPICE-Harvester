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


def _normalize_json_payload(value: Any) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    return json.dumps(value)


def _coerce_json(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
            return {"value": parsed}
        except Exception:
            return {"raw": value}
    try:
        return dict(value)
    except Exception:
        return {}


@dataclass(frozen=True)
class DatasetRecord:
    dataset_id: str
    db_name: str
    name: str
    description: Optional[str]
    source_type: str
    source_ref: Optional[str]
    schema_json: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class DatasetVersionRecord:
    version_id: str
    dataset_id: str
    version: int
    artifact_key: Optional[str]
    row_count: Optional[int]
    sample_json: Dict[str, Any]
    created_at: datetime


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
                    schema_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (db_name, name)
                )
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.dataset_versions (
                    version_id UUID PRIMARY KEY,
                    dataset_id UUID NOT NULL,
                    version INTEGER NOT NULL,
                    artifact_key TEXT,
                    row_count INTEGER,
                    sample_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (dataset_id, version),
                    FOREIGN KEY (dataset_id)
                        REFERENCES {self._schema}.datasets(dataset_id)
                        ON DELETE CASCADE
                )
                """
            )

            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_datasets_db_name ON {self._schema}.datasets(db_name)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_dataset_versions_dataset_id ON {self._schema}.dataset_versions(dataset_id)"
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
        dataset_id: Optional[str] = None,
    ) -> DatasetRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")

        dataset_id = dataset_id or str(uuid4())
        schema_json = schema_json or {}
        schema_payload = _normalize_json_payload(schema_json)

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.datasets (
                    dataset_id, db_name, name, description, source_type, source_ref, schema_json
                ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                RETURNING dataset_id, db_name, name, description, source_type, source_ref,
                          schema_json, created_at, updated_at
                """,
                dataset_id,
                db_name,
                name,
                description,
                source_type,
                source_ref,
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
                schema_json=_coerce_json(row["schema_json"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def list_datasets(self, *, db_name: str) -> List[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT d.dataset_id, d.db_name, d.name, d.description, d.source_type,
                       d.source_ref, d.schema_json, d.created_at, d.updated_at,
                       v.version AS latest_version, v.artifact_key, v.row_count,
                       v.sample_json, v.created_at AS version_created_at
                FROM {self._schema}.datasets d
                LEFT JOIN LATERAL (
                    SELECT version, artifact_key, row_count, sample_json, created_at
                    FROM {self._schema}.dataset_versions
                    WHERE dataset_id = d.dataset_id
                    ORDER BY version DESC
                    LIMIT 1
                ) v ON TRUE
                WHERE d.db_name = $1
                ORDER BY d.updated_at DESC
                """,
                db_name,
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
                    "schema_json": _coerce_json(row["schema_json"]),
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "latest_version": row["latest_version"],
                    "artifact_key": row["artifact_key"],
                    "row_count": row["row_count"],
                    "sample_json": _coerce_json(row["sample_json"]),
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
                       source_ref, schema_json, created_at, updated_at
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
                schema_json=_coerce_json(row["schema_json"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def get_dataset_by_name(self, *, db_name: str, name: str) -> Optional[DatasetRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT dataset_id, db_name, name, description, source_type,
                       source_ref, schema_json, created_at, updated_at
                FROM {self._schema}.datasets
                WHERE db_name = $1 AND name = $2
                """,
                db_name,
                name,
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
                schema_json=_coerce_json(row["schema_json"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def get_dataset_by_source_ref(
        self,
        *,
        db_name: str,
        source_type: str,
        source_ref: str,
    ) -> Optional[DatasetRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT dataset_id, db_name, name, description, source_type,
                       source_ref, schema_json, created_at, updated_at
                FROM {self._schema}.datasets
                WHERE db_name = $1 AND source_type = $2 AND source_ref = $3
                """,
                db_name,
                source_type,
                source_ref,
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
                schema_json=_coerce_json(row["schema_json"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def add_version(
        self,
        *,
        dataset_id: str,
        artifact_key: Optional[str],
        row_count: Optional[int],
        sample_json: Optional[Dict[str, Any]] = None,
        schema_json: Optional[Dict[str, Any]] = None,
        version_id: Optional[str] = None,
    ) -> DatasetVersionRecord:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        version_id = version_id or str(uuid4())
        sample_json = sample_json or {}
        sample_payload = _normalize_json_payload(sample_json)
        if artifact_key:
            artifact_key = artifact_key.strip()
            if artifact_key and not is_s3_uri(artifact_key):
                raise ValueError("artifact_key must be an s3:// URI")
        schema_payload = None
        if schema_json is not None:
            schema_payload = _normalize_json_payload(schema_json)

        async with self._pool.acquire() as conn:
            version = await conn.fetchval(
                f"SELECT COALESCE(MAX(version), 0) + 1 FROM {self._schema}.dataset_versions WHERE dataset_id = $1",
                dataset_id,
            )
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
                    version_id, dataset_id, version, artifact_key, row_count, sample_json
                ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                RETURNING version_id, dataset_id, version, artifact_key, row_count, sample_json, created_at
                """,
                version_id,
                dataset_id,
                int(version),
                artifact_key,
                row_count,
                sample_payload,
            )
            if not row:
                raise RuntimeError("Failed to create dataset version")
            return DatasetVersionRecord(
                version_id=str(row["version_id"]),
                dataset_id=str(row["dataset_id"]),
                version=int(row["version"]),
                artifact_key=row["artifact_key"],
                row_count=row["row_count"],
                sample_json=_coerce_json(row["sample_json"]),
                created_at=row["created_at"],
            )

    async def get_latest_version(self, *, dataset_id: str) -> Optional[DatasetVersionRecord]:
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT version_id, dataset_id, version, artifact_key, row_count, sample_json, created_at
                FROM {self._schema}.dataset_versions
                WHERE dataset_id = $1
                ORDER BY version DESC
                LIMIT 1
                """,
                dataset_id,
            )
            if not row:
                return None
            return DatasetVersionRecord(
                version_id=str(row["version_id"]),
                dataset_id=str(row["dataset_id"]),
                version=int(row["version"]),
                artifact_key=row["artifact_key"],
                row_count=row["row_count"],
                sample_json=_coerce_json(row["sample_json"]),
                created_at=row["created_at"],
            )
