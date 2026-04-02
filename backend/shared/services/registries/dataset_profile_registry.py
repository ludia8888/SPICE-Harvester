"""
Dataset profile registry (Postgres).

Stores derived DatasetProfile artifacts so planners can reuse stable
statistics (null/unique ratios, PK candidates, schema hash) without
recomputing every request.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import uuid4

import asyncpg

from shared.config.settings import get_settings
from shared.services.registries.postgres_schema_registry import PostgresSchemaRegistry
from shared.utils.canonical_json import sha256_canonical_json_prefixed
from shared.utils.json_utils import coerce_json_dataset, normalize_json_payload


@dataclass(frozen=True)
class DatasetProfileRecord:
    profile_id: str
    dataset_id: str
    dataset_version_id: Optional[str]
    db_name: str
    branch: Optional[str]
    schema_hash: Optional[str]
    profile: Dict[str, Any]
    profile_digest: str
    computed_at: datetime
    created_at: datetime
    updated_at: datetime


class DatasetProfileRegistry(PostgresSchemaRegistry):
    _REQUIRED_TABLES = ("dataset_profiles",)

    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_datasets",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
        allow_runtime_ddl_bootstrap: Optional[bool] = None,
    ) -> None:
        perf = get_settings().performance
        resolved_pool_min = int(pool_min) if pool_min is not None else int(perf.dataset_registry_pg_pool_min)
        resolved_pool_max = int(pool_max) if pool_max is not None else int(perf.dataset_registry_pg_pool_max)
        super().__init__(
            dsn=dsn,
            schema=schema,
            pool_min=resolved_pool_min,
            pool_max=resolved_pool_max,
            command_timeout=int(perf.dataset_registry_pg_command_timeout_seconds),
            allow_runtime_ddl_bootstrap=allow_runtime_ddl_bootstrap,
        )

    def _required_tables(self) -> tuple[str, ...]:
        return self._REQUIRED_TABLES

    async def _ensure_tables(self, conn: asyncpg.Connection) -> None:  # type: ignore[override]
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.dataset_profiles (
                profile_id UUID PRIMARY KEY,
                dataset_id UUID NOT NULL,
                dataset_version_id UUID,
                db_name TEXT NOT NULL,
                branch TEXT,
                schema_hash TEXT,
                profile JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                profile_digest TEXT NOT NULL DEFAULT '',
                computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_dataset_profiles_dataset
            ON {self._schema}.dataset_profiles(dataset_id, computed_at DESC)
            """
        )
        await conn.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_profiles_version
            ON {self._schema}.dataset_profiles(dataset_version_id)
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_dataset_profiles_db
            ON {self._schema}.dataset_profiles(db_name, computed_at DESC)
            """
        )

    def _row_to_profile(self, row: asyncpg.Record) -> DatasetProfileRecord:
        return DatasetProfileRecord(
            profile_id=str(row["profile_id"]),
            dataset_id=str(row["dataset_id"]),
            dataset_version_id=str(row["dataset_version_id"]) if row["dataset_version_id"] else None,
            db_name=str(row["db_name"]),
            branch=str(row["branch"]) if row["branch"] else None,
            schema_hash=str(row["schema_hash"]) if row["schema_hash"] else None,
            profile=coerce_json_dataset(row["profile"]),
            profile_digest=str(row["profile_digest"] or ""),
            computed_at=row["computed_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def upsert_profile(
        self,
        *,
        dataset_id: str,
        dataset_version_id: Optional[str],
        db_name: str,
        branch: Optional[str],
        schema_hash: Optional[str],
        profile: Dict[str, Any],
    ) -> DatasetProfileRecord:
        if not self._pool:
            raise RuntimeError("DatasetProfileRegistry not connected")

        profile_payload = normalize_json_payload(profile or {})
        digest = sha256_canonical_json_prefixed(profile or {})
        profile_id = str(uuid4())

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.dataset_profiles (
                    profile_id, dataset_id, dataset_version_id, db_name, branch,
                    schema_hash, profile, profile_digest, computed_at
                ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7::jsonb, $8, NOW())
                ON CONFLICT (dataset_version_id) DO UPDATE SET
                    dataset_id = EXCLUDED.dataset_id,
                    db_name = EXCLUDED.db_name,
                    branch = EXCLUDED.branch,
                    schema_hash = EXCLUDED.schema_hash,
                    profile = EXCLUDED.profile,
                    profile_digest = EXCLUDED.profile_digest,
                    computed_at = NOW(),
                    updated_at = NOW()
                RETURNING profile_id, dataset_id, dataset_version_id, db_name, branch,
                          schema_hash, profile, profile_digest, computed_at, created_at, updated_at
                """,
                profile_id,
                dataset_id,
                dataset_version_id,
                db_name,
                branch,
                schema_hash,
                profile_payload,
                digest,
            )
        if not row:
            raise RuntimeError("Failed to upsert dataset profile")
        return self._row_to_profile(row)

    async def get_latest_profile(
        self,
        *,
        dataset_id: str,
        dataset_version_id: Optional[str] = None,
    ) -> Optional[DatasetProfileRecord]:
        if not self._pool:
            raise RuntimeError("DatasetProfileRegistry not connected")

        if dataset_version_id:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"""
                    SELECT profile_id, dataset_id, dataset_version_id, db_name, branch,
                           schema_hash, profile, profile_digest, computed_at, created_at, updated_at
                    FROM {self._schema}.dataset_profiles
                    WHERE dataset_version_id = $1::uuid
                    """,
                    dataset_version_id,
                )
            return self._row_to_profile(row) if row else None

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT profile_id, dataset_id, dataset_version_id, db_name, branch,
                       schema_hash, profile, profile_digest, computed_at, created_at, updated_at
                FROM {self._schema}.dataset_profiles
                WHERE dataset_id = $1::uuid
                ORDER BY computed_at DESC
                LIMIT 1
                """,
                dataset_id,
            )
        return self._row_to_profile(row) if row else None
