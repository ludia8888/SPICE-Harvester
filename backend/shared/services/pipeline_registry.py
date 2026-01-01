"""
Pipeline Registry (Foundry-style) - durable pipeline metadata in Postgres.

Stores pipeline definitions, versions, and latest preview/build status.
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import asyncpg
from cryptography.fernet import Fernet, InvalidToken

from shared.config.service_config import ServiceConfig
from shared.services.lakefs_client import (
    LakeFSClient,
    LakeFSConfig,
    LakeFSConflictError,
    LakeFSError,
)
from shared.services.lakefs_storage_service import LakeFSStorageService
from shared.utils.path_utils import safe_path_segment
from shared.utils.time_utils import utcnow
from shared.utils.json_utils import coerce_json_pipeline, normalize_json_payload


class PipelineMergeNotSupportedError(RuntimeError):
    pass


class PipelineAlreadyExistsError(RuntimeError):
    def __init__(self, *, db_name: str, name: str, branch: str) -> None:
        super().__init__(f"Pipeline already exists (db_name={db_name} name={name} branch={branch})")
        self.db_name = db_name
        self.name = name
        self.branch = branch


def _ensure_json_string(value: Any) -> str:
    normalized = normalize_json_payload(value, default_handler=str)
    if isinstance(normalized, str):
        return normalized
    return json.dumps(normalized, default=str)


    def _normalize_output_list(value: Optional[List[Dict[str, Any]]], *, field_name: str) -> List[Dict[str, Any]]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError(f"{field_name} must be a list")
        normalized: List[Dict[str, Any]] = []
        for item in value:
            if not isinstance(item, dict):
                raise ValueError(f"{field_name} items must be objects")
            normalized.append(item)
        return normalized


def _is_production_env() -> bool:
    raw = (os.getenv("ENVIRONMENT") or os.getenv("APP_ENV") or os.getenv("APP_ENVIRONMENT") or "").strip().lower()
    return raw in {"prod", "production"}


def _lakefs_credentials_source() -> str:
    raw = (os.getenv("LAKEFS_CREDENTIALS_SOURCE") or "").strip().lower()
    if raw in {"db", "database"}:
        return "db"
    if raw in {"env", "environment"}:
        return "env"
    return "db" if _is_production_env() else "env"


def _lakefs_service_principal() -> str:
    return (os.getenv("LAKEFS_SERVICE_PRINCIPAL") or os.getenv("SERVICE_NAME") or "bff").strip() or "bff"


def _lakefs_fernet() -> Fernet:
    key = (os.getenv("LAKEFS_CREDENTIALS_ENCRYPTION_KEY") or "").strip()
    if not key:
        raise RuntimeError(
            "LAKEFS_CREDENTIALS_ENCRYPTION_KEY is required to store lakeFS credentials in Postgres. "
            "Generate one with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
        )
    try:
        return Fernet(key.encode("utf-8"))
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Invalid LAKEFS_CREDENTIALS_ENCRYPTION_KEY for Fernet") from exc


def _encrypt_secret(secret_access_key: str) -> str:
    secret = str(secret_access_key or "").strip()
    if not secret:
        raise ValueError("secret_access_key is required")
    return _lakefs_fernet().encrypt(secret.encode("utf-8")).decode("utf-8")


def _decrypt_secret(encrypted: str) -> str:
    token = str(encrypted or "").strip()
    if not token:
        raise ValueError("encrypted secret is empty")
    try:
        return _lakefs_fernet().decrypt(token.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        raise RuntimeError("Failed to decrypt lakeFS credential secret (invalid token / wrong key)") from exc


_PIPELINE_ROLE_ORDER = {
    "read": 1,
    "edit": 2,
    "approve": 3,
    "admin": 4,
}


def _normalize_pipeline_role(value: Optional[str]) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return "read"
    if raw in _PIPELINE_ROLE_ORDER:
        return raw
    return "read"


def _normalize_principal_type(value: Optional[str]) -> str:
    raw = (value or "").strip().lower()
    if raw in {"user", "service"}:
        return raw
    return "user"


def _role_allows(required: str, assigned: str) -> bool:
    required_rank = _PIPELINE_ROLE_ORDER.get(_normalize_pipeline_role(required), 0)
    assigned_rank = _PIPELINE_ROLE_ORDER.get(_normalize_pipeline_role(assigned), 0)
    return assigned_rank >= required_rank


def _definition_object_key(*, db_name: str, pipeline_name: str) -> str:
    # Store pipeline definitions as immutable JSON snapshots in lakeFS (branch/ref is the first path segment).
    safe_db = safe_path_segment(db_name)
    safe_name = safe_path_segment(pipeline_name)
    return f"pipeline-definitions/{safe_db}/{safe_name}/definition.json"


@dataclass(frozen=True)
class LakeFSCredentials:
    principal_type: str  # user|service
    principal_id: str
    access_key_id: str
    secret_access_key: str
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str] = None


def _row_to_pipeline_record(row: asyncpg.Record) -> PipelineRecord:
    return PipelineRecord(
        pipeline_id=str(row["pipeline_id"]),
        db_name=str(row["db_name"]),
        name=str(row["name"]),
        description=row["description"],
        pipeline_type=str(row["pipeline_type"]),
        location=str(row["location"]),
        status=str(row["status"]),
        branch=str(row["branch"]),
        lakefs_repository=row["lakefs_repository"],
        proposal_status=row["proposal_status"],
        proposal_title=row["proposal_title"],
        proposal_description=row["proposal_description"],
        proposal_submitted_at=row["proposal_submitted_at"],
        proposal_reviewed_at=row["proposal_reviewed_at"],
        proposal_review_comment=row["proposal_review_comment"],
        proposal_bundle=coerce_json_pipeline(row["proposal_bundle"]),
        last_preview_status=row["last_preview_status"],
        last_preview_at=row["last_preview_at"],
        last_preview_rows=row["last_preview_rows"],
        last_preview_job_id=row["last_preview_job_id"],
        last_preview_node_id=row["last_preview_node_id"],
        last_preview_sample=coerce_json_pipeline(row["last_preview_sample"]),
        last_preview_nodes=coerce_json_pipeline(row["last_preview_nodes"]),
        last_build_status=row["last_build_status"],
        last_build_at=row["last_build_at"],
        last_build_output=coerce_json_pipeline(row["last_build_output"]),
        deployed_at=row["deployed_at"],
        deployed_commit_id=row["deployed_commit_id"],
        schedule_interval_seconds=row["schedule_interval_seconds"],
        schedule_cron=row["schedule_cron"],
        last_scheduled_at=row["last_scheduled_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@dataclass(frozen=True)
class PipelineRecord:
    pipeline_id: str
    db_name: str
    name: str
    description: Optional[str]
    pipeline_type: str
    location: str
    status: str
    branch: str
    lakefs_repository: Optional[str]
    proposal_status: Optional[str]
    proposal_title: Optional[str]
    proposal_description: Optional[str]
    proposal_submitted_at: Optional[datetime]
    proposal_reviewed_at: Optional[datetime]
    proposal_review_comment: Optional[str]
    proposal_bundle: Dict[str, Any]
    last_preview_status: Optional[str]
    last_preview_at: Optional[datetime]
    last_preview_rows: Optional[int]
    last_preview_job_id: Optional[str]
    last_preview_node_id: Optional[str]
    last_preview_sample: Dict[str, Any]
    last_preview_nodes: Dict[str, Any]
    last_build_status: Optional[str]
    last_build_at: Optional[datetime]
    last_build_output: Dict[str, Any]
    deployed_at: Optional[datetime]
    deployed_commit_id: Optional[str]
    schedule_interval_seconds: Optional[int]
    schedule_cron: Optional[str]
    last_scheduled_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class PipelineVersionRecord:
    version_id: str
    pipeline_id: str
    branch: str
    lakefs_commit_id: str
    definition_json: Dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class PipelineUdfRecord:
    udf_id: str
    db_name: str
    name: str
    description: Optional[str]
    latest_version: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class PipelineUdfVersionRecord:
    version_id: str
    udf_id: str
    version: int
    code: str
    created_at: datetime


@dataclass(frozen=True)
class PipelineArtifactRecord:
    artifact_id: str
    pipeline_id: str
    job_id: str
    run_id: Optional[str]
    mode: str
    status: str
    definition_hash: Optional[str]
    definition_commit_id: Optional[str]
    pipeline_spec_hash: Optional[str]
    pipeline_spec_commit_id: Optional[str]
    inputs: Dict[str, Any]
    lakefs_repository: Optional[str]
    lakefs_branch: Optional[str]
    lakefs_commit_id: Optional[str]
    outputs: List[Dict[str, Any]]
    declared_outputs: List[Dict[str, Any]]
    sampling_strategy: Dict[str, Any]
    error: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class PromotionManifestRecord:
    manifest_id: str
    pipeline_id: str
    db_name: str
    build_job_id: str
    artifact_id: Optional[str]
    definition_hash: Optional[str]
    lakefs_repository: str
    lakefs_commit_id: str
    ontology_commit_id: str
    mapping_spec_id: Optional[str]
    mapping_spec_version: Optional[int]
    mapping_spec_target_class_id: Optional[str]
    promoted_dataset_version_id: str
    promoted_dataset_name: Optional[str]
    target_branch: str
    promoted_by: Optional[str]
    promoted_at: datetime
    metadata: Dict[str, Any]


def _row_to_pipeline_artifact(row: asyncpg.Record) -> PipelineArtifactRecord:
    return PipelineArtifactRecord(
        artifact_id=str(row["artifact_id"]),
        pipeline_id=str(row["pipeline_id"]),
        job_id=str(row["job_id"]),
        run_id=str(row["run_id"]) if row["run_id"] else None,
        mode=str(row["mode"]),
        status=str(row["status"]),
        definition_hash=row["definition_hash"],
        definition_commit_id=row["definition_commit_id"],
        pipeline_spec_hash=row["pipeline_spec_hash"],
        pipeline_spec_commit_id=row["pipeline_spec_commit_id"],
        inputs=coerce_json_pipeline(row["inputs"]),
        lakefs_repository=row["lakefs_repository"],
        lakefs_branch=row["lakefs_branch"],
        lakefs_commit_id=row["lakefs_commit_id"],
        outputs=coerce_json_pipeline(row["outputs"]),
        declared_outputs=coerce_json_pipeline(row["declared_outputs"]),
        sampling_strategy=coerce_json_pipeline(row["sampling_strategy"]),
        error=coerce_json_pipeline(row["error"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


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

    async def _get_lakefs_credentials(
        self,
        *,
        principal_type: str,
        principal_id: str,
    ) -> Optional[LakeFSCredentials]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        principal_type = str(principal_type or "").strip().lower()
        principal_id = str(principal_id or "").strip()
        if principal_type not in {"user", "service"}:
            raise ValueError("principal_type must be 'user' or 'service'")
        if not principal_id:
            raise ValueError("principal_id is required")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT principal_type, principal_id, access_key_id, secret_access_key_enc,
                       created_at, updated_at, created_by
                FROM {self._schema}.lakefs_credentials
                WHERE principal_type = $1 AND principal_id = $2
                """,
                principal_type,
                principal_id,
            )
        if not row:
            return None
        return LakeFSCredentials(
            principal_type=str(row["principal_type"]),
            principal_id=str(row["principal_id"]),
            access_key_id=str(row["access_key_id"]),
            secret_access_key=_decrypt_secret(str(row["secret_access_key_enc"])),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            created_by=row["created_by"],
        )

    async def upsert_lakefs_credentials(
        self,
        *,
        principal_type: str,
        principal_id: str,
        access_key_id: str,
        secret_access_key: str,
        created_by: Optional[str] = None,
    ) -> None:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        principal_type = str(principal_type or "").strip().lower()
        principal_id = str(principal_id or "").strip()
        access_key_id = str(access_key_id or "").strip()
        secret_access_key = str(secret_access_key or "").strip()
        created_by = (str(created_by or "").strip() or None) if created_by is not None else None
        if principal_type not in {"user", "service"}:
            raise ValueError("principal_type must be 'user' or 'service'")
        if not principal_id:
            raise ValueError("principal_id is required")
        if not access_key_id or not secret_access_key:
            raise ValueError("access_key_id and secret_access_key are required")
        secret_enc = _encrypt_secret(secret_access_key)

        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._schema}.lakefs_credentials (
                    principal_type, principal_id, access_key_id, secret_access_key_enc, created_by,
                    created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
                ON CONFLICT (principal_type, principal_id)
                DO UPDATE SET
                    access_key_id = EXCLUDED.access_key_id,
                    secret_access_key_enc = EXCLUDED.secret_access_key_enc,
                    created_by = EXCLUDED.created_by,
                    updated_at = NOW()
                """,
                principal_type,
                principal_id,
                access_key_id,
                secret_enc,
                created_by,
            )

    async def list_lakefs_credentials(self) -> list[dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT principal_type, principal_id, access_key_id, created_by, created_at, updated_at
                FROM {self._schema}.lakefs_credentials
                ORDER BY principal_type ASC, principal_id ASC
                """
            )
        return [
            {
                "principal_type": str(row["principal_type"]),
                "principal_id": str(row["principal_id"]),
                "access_key_id": str(row["access_key_id"]),
                "created_by": row["created_by"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in rows or []
        ]

    async def resolve_lakefs_credentials(self, *, user_id: Optional[str] = None) -> LakeFSCredentials:
        source = _lakefs_credentials_source()
        if source not in {"db", "env"}:
            source = "env"
        requested_user = str(user_id or "").strip() or None
        if source == "db":
            if requested_user:
                creds = await self._get_lakefs_credentials(principal_type="user", principal_id=requested_user)
                if creds:
                    return creds

            service_principal = _lakefs_service_principal()
            creds = await self._get_lakefs_credentials(principal_type="service", principal_id=service_principal)
            if creds:
                return creds
            raise RuntimeError(
                f"lakeFS credentials not configured in Postgres for service principal '{service_principal}'. "
                "Use the admin API to upsert credentials."
            )

        # env fallback (dev/test)
        access_key_id = str(os.getenv("LAKEFS_ACCESS_KEY_ID") or "").strip()
        secret_access_key = str(os.getenv("LAKEFS_SECRET_ACCESS_KEY") or "").strip()
        if not access_key_id or not secret_access_key:
            raise RuntimeError("lakeFS credentials are required (LAKEFS_ACCESS_KEY_ID/LAKEFS_SECRET_ACCESS_KEY)")
        now = utcnow()
        return LakeFSCredentials(
            principal_type="service",
            principal_id=_lakefs_service_principal(),
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            created_at=now,
            updated_at=now,
            created_by=None,
        )

    async def get_lakefs_client(self, *, user_id: Optional[str] = None) -> LakeFSClient:
        creds = await self.resolve_lakefs_credentials(user_id=user_id)
        api_url = (os.getenv("LAKEFS_API_URL") or ServiceConfig.get_lakefs_api_url()).rstrip("/")
        return LakeFSClient(
            config=LakeFSConfig(
                api_url=api_url,
                access_key_id=creds.access_key_id,
                secret_access_key=creds.secret_access_key,
            )
        )

    async def get_lakefs_storage(self, *, user_id: Optional[str] = None) -> LakeFSStorageService:
        creds = await self.resolve_lakefs_credentials(user_id=user_id)
        endpoint = ServiceConfig.get_lakefs_s3_endpoint()
        return LakeFSStorageService(
            endpoint_url=endpoint,
            access_key=creds.access_key_id,
            secret_key=creds.secret_access_key,
            use_ssl=endpoint.startswith("https"),
        )

    def _resolve_repository(self, *, pipeline: PipelineRecord) -> str:
        repo = (pipeline.lakefs_repository or "").strip()
        if repo:
            return repo
        repo = str(os.getenv("LAKEFS_ARTIFACTS_REPOSITORY") or "").strip()
        return repo or "pipeline-artifacts"

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
                CREATE TABLE IF NOT EXISTS {self._schema}.lakefs_credentials (
                    principal_type TEXT NOT NULL,
                    principal_id TEXT NOT NULL,
                    access_key_id TEXT NOT NULL,
                    secret_access_key_enc TEXT NOT NULL,
                    created_by TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (principal_type, principal_id)
                )
                """
            )
            await conn.execute(
                f"""
                DO $$
                BEGIN
                    ALTER TABLE {self._schema}.lakefs_credentials
                        ADD CONSTRAINT lakefs_credentials_principal_type_check
                        CHECK (principal_type IN ('user', 'service'));
                EXCEPTION
                    WHEN duplicate_object THEN NULL;
                END $$;
                """
            )

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
                    branch TEXT NOT NULL DEFAULT 'main',
                    lakefs_repository TEXT,
                    proposal_status TEXT,
                    proposal_title TEXT,
                    proposal_description TEXT,
                    proposal_submitted_at TIMESTAMPTZ,
                    proposal_reviewed_at TIMESTAMPTZ,
                    proposal_review_comment TEXT,
                    proposal_bundle JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    last_preview_status TEXT,
                    last_preview_at TIMESTAMPTZ,
                    last_preview_rows INTEGER,
                    last_preview_job_id TEXT,
                    last_preview_node_id TEXT,
                    last_preview_sample JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    last_preview_nodes JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    last_build_status TEXT,
                    last_build_at TIMESTAMPTZ,
                    last_build_output JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    deployed_at TIMESTAMPTZ,
                    deployed_commit_id TEXT,
                    schedule_interval_seconds INTEGER,
                    schedule_cron TEXT,
                    last_scheduled_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (db_name, name, branch)
                )
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.pipeline_branches (
                    db_name TEXT NOT NULL,
                    branch TEXT NOT NULL,
                    archived BOOLEAN NOT NULL DEFAULT FALSE,
                    archived_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (db_name, branch)
                )
                """
            )

            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.pipelines
                    ADD COLUMN IF NOT EXISTS branch TEXT NOT NULL DEFAULT 'main',
                    ADD COLUMN IF NOT EXISTS lakefs_repository TEXT,
                    ADD COLUMN IF NOT EXISTS proposal_status TEXT,
                    ADD COLUMN IF NOT EXISTS proposal_title TEXT,
                    ADD COLUMN IF NOT EXISTS proposal_description TEXT,
                    ADD COLUMN IF NOT EXISTS proposal_submitted_at TIMESTAMPTZ,
                    ADD COLUMN IF NOT EXISTS proposal_reviewed_at TIMESTAMPTZ,
                    ADD COLUMN IF NOT EXISTS proposal_review_comment TEXT,
                    ADD COLUMN IF NOT EXISTS proposal_bundle JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    ADD COLUMN IF NOT EXISTS last_preview_job_id TEXT,
                    ADD COLUMN IF NOT EXISTS last_preview_node_id TEXT,
                    ADD COLUMN IF NOT EXISTS last_preview_nodes JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    ADD COLUMN IF NOT EXISTS deployed_commit_id TEXT,
                    ADD COLUMN IF NOT EXISTS schedule_interval_seconds INTEGER,
                    ADD COLUMN IF NOT EXISTS schedule_cron TEXT,
                    ADD COLUMN IF NOT EXISTS last_scheduled_at TIMESTAMPTZ
                """
            )
            await conn.execute(
                f"""
                INSERT INTO {self._schema}.pipeline_branches (db_name, branch, archived)
                SELECT DISTINCT db_name, branch, FALSE
                FROM {self._schema}.pipelines
                ON CONFLICT (db_name, branch) DO NOTHING
                """
            )

            # lakeFS refactor: remove legacy branch-base + integer deployed version columns.
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.pipelines
                    DROP COLUMN IF EXISTS branch_base_branch,
                    DROP COLUMN IF EXISTS branch_base_version,
                    DROP COLUMN IF EXISTS deployed_version
                """
            )

            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.pipelines
                    DROP CONSTRAINT IF EXISTS pipelines_db_name_name_key
                """
            )
            await conn.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS pipelines_db_name_name_branch_key
                ON {self._schema}.pipelines(db_name, name, branch)
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.pipeline_versions (
                    version_id UUID PRIMARY KEY,
                    pipeline_id UUID NOT NULL,
                    branch TEXT NOT NULL DEFAULT 'main',
                    lakefs_commit_id TEXT NOT NULL,
                    definition_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (pipeline_id, branch, lakefs_commit_id),
                    FOREIGN KEY (pipeline_id)
                        REFERENCES {self._schema}.pipelines(pipeline_id)
                        ON DELETE CASCADE
                )
                """
            )

            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.pipeline_versions
                    ADD COLUMN IF NOT EXISTS branch TEXT NOT NULL DEFAULT 'main',
                    ADD COLUMN IF NOT EXISTS lakefs_commit_id TEXT
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.pipeline_versions
                    DROP COLUMN IF EXISTS version
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.pipeline_versions
                    DROP CONSTRAINT IF EXISTS pipeline_versions_pipeline_id_branch_version_key
                """
            )
            await conn.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS pipeline_versions_pipeline_id_branch_commit_key
                ON {self._schema}.pipeline_versions(pipeline_id, branch, lakefs_commit_id)
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.pipeline_runs (
                    run_id UUID PRIMARY KEY,
                    pipeline_id UUID NOT NULL,
                    job_id TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    node_id TEXT,
                    row_count INTEGER,
                    sample_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    output_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    pipeline_spec_commit_id TEXT,
                    pipeline_spec_hash TEXT,
                    input_lakefs_commits JSONB,
                    output_lakefs_commit_id TEXT,
                    spark_conf JSONB,
                    code_version TEXT,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ,
                    UNIQUE (pipeline_id, job_id),
                    FOREIGN KEY (pipeline_id)
                        REFERENCES {self._schema}.pipelines(pipeline_id)
                        ON DELETE CASCADE
                )
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.pipeline_artifacts (
                    artifact_id UUID PRIMARY KEY,
                    pipeline_id UUID NOT NULL,
                    job_id TEXT NOT NULL,
                    run_id UUID,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    definition_hash TEXT,
                    definition_commit_id TEXT,
                    pipeline_spec_hash TEXT,
                    pipeline_spec_commit_id TEXT,
                    inputs JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    lakefs_repository TEXT,
                    lakefs_branch TEXT,
                    lakefs_commit_id TEXT,
                    outputs JSONB NOT NULL DEFAULT '[]'::jsonb,
                    declared_outputs JSONB NOT NULL DEFAULT '[]'::jsonb,
                    sampling_strategy JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    error JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (pipeline_id, job_id, mode),
                    FOREIGN KEY (pipeline_id)
                        REFERENCES {self._schema}.pipelines(pipeline_id)
                        ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'pipeline_artifacts_outputs_array'
                          AND conrelid = '{self._schema}.pipeline_artifacts'::regclass
                    ) THEN
                        ALTER TABLE {self._schema}.pipeline_artifacts
                        ADD CONSTRAINT pipeline_artifacts_outputs_array
                        CHECK (jsonb_typeof(outputs) = 'array') NOT VALID;
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
                        WHERE conname = 'pipeline_artifacts_declared_outputs_array'
                          AND conrelid = '{self._schema}.pipeline_artifacts'::regclass
                    ) THEN
                        ALTER TABLE {self._schema}.pipeline_artifacts
                        ADD CONSTRAINT pipeline_artifacts_declared_outputs_array
                        CHECK (jsonb_typeof(declared_outputs) = 'array') NOT VALID;
                    END IF;
                END $$;
                """
            )
            await conn.execute(
                f"""
                UPDATE {self._schema}.pipeline_artifacts
                SET outputs = jsonb_build_array(outputs)
                WHERE jsonb_typeof(outputs) = 'object'
                """
            )
            await conn.execute(
                f"""
                UPDATE {self._schema}.pipeline_artifacts
                SET outputs = '[]'::jsonb
                WHERE outputs IS NULL OR jsonb_typeof(outputs) <> 'array'
                """
            )
            await conn.execute(
                f"""
                UPDATE {self._schema}.pipeline_artifacts
                SET declared_outputs = jsonb_build_array(declared_outputs)
                WHERE jsonb_typeof(declared_outputs) = 'object'
                """
            )
            await conn.execute(
                f"""
                UPDATE {self._schema}.pipeline_artifacts
                SET declared_outputs = '[]'::jsonb
                WHERE declared_outputs IS NULL OR jsonb_typeof(declared_outputs) <> 'array'
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.promotion_manifests (
                    manifest_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    pipeline_id UUID NOT NULL,
                    db_name TEXT NOT NULL,
                    build_job_id TEXT NOT NULL,
                    artifact_id UUID,
                    definition_hash TEXT,
                    lakefs_repository TEXT NOT NULL,
                    lakefs_commit_id TEXT NOT NULL,
                    ontology_commit_id TEXT NOT NULL,
                    mapping_spec_id TEXT,
                    mapping_spec_version INTEGER,
                    mapping_spec_target_class_id TEXT,
                    promoted_dataset_version_id UUID NOT NULL,
                    promoted_dataset_name TEXT,
                    target_branch TEXT NOT NULL,
                    promoted_by TEXT,
                    promoted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    metadata JSONB
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_promotion_manifests_pipeline_id ON {self._schema}.promotion_manifests(pipeline_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_promotion_manifests_build_job ON {self._schema}.promotion_manifests(build_job_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_promotion_manifests_artifact_id ON {self._schema}.promotion_manifests(artifact_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_promotion_manifests_dataset_version ON {self._schema}.promotion_manifests(promoted_dataset_version_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_promotion_manifests_lakefs_commit ON {self._schema}.promotion_manifests(lakefs_commit_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_promotion_manifests_ontology_commit ON {self._schema}.promotion_manifests(ontology_commit_id)"
            )

            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.pipeline_runs
                    ADD COLUMN IF NOT EXISTS pipeline_spec_commit_id TEXT,
                    ADD COLUMN IF NOT EXISTS pipeline_spec_hash TEXT,
                    ADD COLUMN IF NOT EXISTS input_lakefs_commits JSONB,
                    ADD COLUMN IF NOT EXISTS output_lakefs_commit_id TEXT,
                    ADD COLUMN IF NOT EXISTS spark_conf JSONB,
                    ADD COLUMN IF NOT EXISTS code_version TEXT
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.pipeline_watermarks (
                    pipeline_id UUID NOT NULL,
                    branch TEXT NOT NULL,
                    watermarks_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (pipeline_id, branch),
                    FOREIGN KEY (pipeline_id)
                        REFERENCES {self._schema}.pipelines(pipeline_id)
                        ON DELETE CASCADE
                )
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.pipeline_dependencies (
                    pipeline_id UUID NOT NULL,
                    depends_on_pipeline_id UUID NOT NULL,
                    required_status TEXT NOT NULL DEFAULT 'DEPLOYED',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (pipeline_id, depends_on_pipeline_id),
                    FOREIGN KEY (pipeline_id)
                        REFERENCES {self._schema}.pipelines(pipeline_id)
                        ON DELETE CASCADE,
                    FOREIGN KEY (depends_on_pipeline_id)
                        REFERENCES {self._schema}.pipelines(pipeline_id)
                        ON DELETE RESTRICT
                )
                """
            )
            await conn.execute(
                f"""
                DO $$
                BEGIN
                    ALTER TABLE {self._schema}.pipeline_dependencies
                        ADD CONSTRAINT pipeline_dependencies_required_status_check
                        CHECK (required_status IN ('DEPLOYED', 'SUCCESS'));
                EXCEPTION
                    WHEN duplicate_object THEN NULL;
                END $$;
                """
            )
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.pipeline_permissions (
                    pipeline_id UUID NOT NULL,
                    principal_type TEXT NOT NULL,
                    principal_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (pipeline_id, principal_type, principal_id),
                    FOREIGN KEY (pipeline_id)
                        REFERENCES {self._schema}.pipelines(pipeline_id)
                        ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                f"""
                DO $$
                BEGIN
                    ALTER TABLE {self._schema}.pipeline_permissions
                        ADD CONSTRAINT pipeline_permissions_role_check
                        CHECK (role IN ('read', 'edit', 'approve', 'admin'));
                EXCEPTION
                    WHEN duplicate_object THEN NULL;
                END $$;
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.pipeline_udfs (
                    udf_id UUID PRIMARY KEY,
                    db_name TEXT NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    latest_version INTEGER NOT NULL DEFAULT 1,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (db_name, name)
                )
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.pipeline_udf_versions (
                    version_id UUID PRIMARY KEY,
                    udf_id UUID NOT NULL,
                    version INTEGER NOT NULL,
                    code TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (udf_id, version),
                    FOREIGN KEY (udf_id)
                        REFERENCES {self._schema}.pipeline_udfs(udf_id)
                        ON DELETE CASCADE
                )
                """
            )

            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipelines_db_name ON {self._schema}.pipelines(db_name)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipelines_schedule ON {self._schema}.pipelines(schedule_interval_seconds)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipelines_schedule_cron ON {self._schema}.pipelines(schedule_cron)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_versions_pipeline_id ON {self._schema}.pipeline_versions(pipeline_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_versions_branch ON {self._schema}.pipeline_versions(branch)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_artifacts_pipeline_id ON {self._schema}.pipeline_artifacts(pipeline_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_artifacts_job_id ON {self._schema}.pipeline_artifacts(job_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_dependencies_pipeline_id ON {self._schema}.pipeline_dependencies(pipeline_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_dependencies_depends_on ON {self._schema}.pipeline_dependencies(depends_on_pipeline_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_permissions_pipeline_id ON {self._schema}.pipeline_permissions(pipeline_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_permissions_principal ON {self._schema}.pipeline_permissions(principal_type, principal_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_watermarks_pipeline_id ON {self._schema}.pipeline_watermarks(pipeline_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_udfs_db_name ON {self._schema}.pipeline_udfs(db_name)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_udf_versions_udf_id ON {self._schema}.pipeline_udf_versions(udf_id)"
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.pipeline_versions
                    ADD COLUMN IF NOT EXISTS branch TEXT NOT NULL DEFAULT 'main'
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {self._schema}.pipeline_versions
                    DROP CONSTRAINT IF EXISTS pipeline_versions_pipeline_id_version_key
                """
            )
            await conn.execute(
                f"""
                DROP INDEX IF EXISTS {self._schema}.pipeline_versions_pipeline_id_branch_version_key
                """
            )
            await conn.execute(
                f"""
                UPDATE {self._schema}.pipeline_versions v
                SET branch = p.branch
                FROM {self._schema}.pipelines p
                WHERE v.pipeline_id = p.pipeline_id AND (v.branch IS NULL OR v.branch = '')
                """
            )

    async def list_dependencies(self, *, pipeline_id: str) -> List[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT depends_on_pipeline_id, required_status
                FROM {self._schema}.pipeline_dependencies
                WHERE pipeline_id = $1
                ORDER BY created_at ASC
                """,
                pipeline_id,
            )
        return [
            {"pipeline_id": str(row["depends_on_pipeline_id"]), "status": str(row["required_status"])}
            for row in rows or []
        ]

    async def replace_dependencies(
        self,
        *,
        pipeline_id: str,
        dependencies: List[Dict[str, str]],
    ) -> None:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        normalized: List[Dict[str, str]] = []
        for dep in dependencies or []:
            if not isinstance(dep, dict):
                continue
            depends_on = str(dep.get("pipeline_id") or "").strip()
            status = str(dep.get("status") or dep.get("required_status") or "DEPLOYED").strip().upper()
            if not depends_on:
                continue
            normalized.append({"pipeline_id": depends_on, "status": status})

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"DELETE FROM {self._schema}.pipeline_dependencies WHERE pipeline_id = $1",
                    pipeline_id,
                )
                if not normalized:
                    return
                await conn.executemany(
                    f"""
                    INSERT INTO {self._schema}.pipeline_dependencies (
                        pipeline_id, depends_on_pipeline_id, required_status, created_at, updated_at
                    ) VALUES ($1, $2, $3, NOW(), NOW())
                    ON CONFLICT (pipeline_id, depends_on_pipeline_id)
                    DO UPDATE SET required_status = EXCLUDED.required_status, updated_at = NOW()
                    """,
                    [(pipeline_id, dep["pipeline_id"], dep["status"]) for dep in normalized],
                )

    async def grant_permission(
        self,
        *,
        pipeline_id: str,
        principal_type: str,
        principal_id: str,
        role: str,
    ) -> None:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        principal_type = _normalize_principal_type(principal_type)
        principal_id = str(principal_id or "").strip()
        if not principal_id:
            raise ValueError("principal_id is required")
        role = _normalize_pipeline_role(role)
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._schema}.pipeline_permissions (
                    pipeline_id, principal_type, principal_id, role, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, NOW(), NOW())
                ON CONFLICT (pipeline_id, principal_type, principal_id)
                DO UPDATE SET role = EXCLUDED.role, updated_at = NOW()
                """,
                pipeline_id,
                principal_type,
                principal_id,
                role,
            )

    async def revoke_permission(
        self,
        *,
        pipeline_id: str,
        principal_type: str,
        principal_id: str,
    ) -> None:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        principal_type = _normalize_principal_type(principal_type)
        principal_id = str(principal_id or "").strip()
        if not principal_id:
            return
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                DELETE FROM {self._schema}.pipeline_permissions
                WHERE pipeline_id = $1 AND principal_type = $2 AND principal_id = $3
                """,
                pipeline_id,
                principal_type,
                principal_id,
            )

    async def list_permissions(self, *, pipeline_id: str) -> List[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT principal_type, principal_id, role, created_at, updated_at
                FROM {self._schema}.pipeline_permissions
                WHERE pipeline_id = $1
                ORDER BY principal_type ASC, principal_id ASC
                """,
                pipeline_id,
            )
        return [
            {
                "principal_type": str(row["principal_type"]),
                "principal_id": str(row["principal_id"]),
                "role": str(row["role"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in rows or []
        ]

    async def has_any_permissions(self, *, pipeline_id: str) -> bool:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT 1
                FROM {self._schema}.pipeline_permissions
                WHERE pipeline_id = $1
                LIMIT 1
                """,
                pipeline_id,
            )
        return row is not None

    async def get_permission_role(
        self,
        *,
        pipeline_id: str,
        principal_type: str,
        principal_id: str,
    ) -> Optional[str]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        principal_type = _normalize_principal_type(principal_type)
        principal_id = str(principal_id or "").strip()
        if not principal_id:
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT role
                FROM {self._schema}.pipeline_permissions
                WHERE pipeline_id = $1 AND principal_type = $2 AND principal_id = $3
                """,
                pipeline_id,
                principal_type,
                principal_id,
            )
        if not row:
            return None
        return str(row["role"])

    async def has_permission(
        self,
        *,
        pipeline_id: str,
        principal_type: str,
        principal_id: str,
        required_role: str,
    ) -> bool:
        role = await self.get_permission_role(
            pipeline_id=pipeline_id,
            principal_type=principal_type,
            principal_id=principal_id,
        )
        if not role:
            return False
        return _role_allows(required_role, role)

    async def create_pipeline(
        self,
        *,
        db_name: str,
        name: str,
        description: Optional[str],
        pipeline_type: str,
        location: str,
        status: str = "draft",
        branch: str = "main",
        lakefs_repository: Optional[str] = None,
        proposal_status: Optional[str] = None,
        proposal_title: Optional[str] = None,
        proposal_description: Optional[str] = None,
        proposal_submitted_at: Optional[datetime] = None,
        proposal_reviewed_at: Optional[datetime] = None,
        proposal_review_comment: Optional[str] = None,
        proposal_bundle: Optional[Dict[str, Any]] = None,
        schedule_interval_seconds: Optional[int] = None,
        schedule_cron: Optional[str] = None,
        pipeline_id: Optional[str] = None,
    ) -> PipelineRecord:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")

        pipeline_id = pipeline_id or str(uuid4())
        lakefs_repository = (lakefs_repository or "").strip() or (
            str(os.getenv("LAKEFS_ARTIFACTS_REPOSITORY") or "").strip() or "pipeline-artifacts"
        )

        async with self._pool.acquire() as conn:
            try:
                async with conn.transaction():
                    row = await conn.fetchrow(
                        f"""
                        INSERT INTO {self._schema}.pipelines (
                            pipeline_id, db_name, name, description, pipeline_type, location, status,
                            branch, lakefs_repository,
                            proposal_status, proposal_title, proposal_description,
                            proposal_submitted_at, proposal_reviewed_at, proposal_review_comment, proposal_bundle,
                            schedule_interval_seconds, schedule_cron
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                        RETURNING pipeline_id, db_name, name, description, pipeline_type, location, status,
                                  branch, lakefs_repository,
                                  proposal_status, proposal_title, proposal_description,
                                  proposal_submitted_at, proposal_reviewed_at, proposal_review_comment, proposal_bundle,
                                  last_preview_status, last_preview_at, last_preview_rows,
                                  last_preview_job_id, last_preview_node_id, last_preview_sample, last_preview_nodes,
                                  last_build_status, last_build_at, last_build_output,
                                  deployed_at, deployed_commit_id, schedule_interval_seconds, schedule_cron, last_scheduled_at,
                                  created_at, updated_at
                        """,
                        pipeline_id,
                        db_name,
                        name,
                        description,
                        pipeline_type,
                        location,
                        status,
                        branch,
                        lakefs_repository,
                        proposal_status,
                        proposal_title,
                        proposal_description,
                        proposal_submitted_at,
                        proposal_reviewed_at,
                        proposal_review_comment,
                        _ensure_json_string(proposal_bundle or {}),
                        schedule_interval_seconds,
                        schedule_cron,
                    )
                    await conn.execute(
                        f"""
                        INSERT INTO {self._schema}.pipeline_branches (db_name, branch, archived, created_at, updated_at)
                        VALUES ($1, $2, FALSE, NOW(), NOW())
                        ON CONFLICT (db_name, branch)
                        DO UPDATE SET updated_at = NOW()
                        """,
                        db_name,
                        branch,
                    )
            except asyncpg.UniqueViolationError as exc:
                raise PipelineAlreadyExistsError(db_name=db_name, name=name, branch=branch) from exc
            if not row:
                raise RuntimeError("Failed to create pipeline")
            return _row_to_pipeline_record(row)

    async def list_pipelines(self, *, db_name: str, branch: Optional[str] = None) -> List[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")

        async with self._pool.acquire() as conn:
            if branch:
                rows = await conn.fetch(
                    f"""
                    SELECT p.pipeline_id, p.db_name, p.name, p.description, p.pipeline_type, p.location,
                           p.status, p.branch, p.lakefs_repository, p.proposal_status, p.proposal_title, p.proposal_description,
                           p.proposal_submitted_at, p.proposal_reviewed_at, p.proposal_review_comment, p.proposal_bundle,
                           p.last_preview_status, p.last_preview_at, p.last_preview_rows,
                           p.last_preview_job_id, p.last_preview_node_id,
                           p.last_preview_sample, p.last_preview_nodes,
                           p.last_build_status, p.last_build_at, p.last_build_output,
                           p.deployed_at, p.deployed_commit_id, p.schedule_interval_seconds, p.schedule_cron, p.last_scheduled_at,
                           p.created_at, p.updated_at,
                           v.lakefs_commit_id AS latest_commit_id, v.definition_json, v.created_at AS version_created_at
                    FROM {self._schema}.pipelines p
                    LEFT JOIN LATERAL (
                    SELECT lakefs_commit_id, definition_json, created_at
                    FROM {self._schema}.pipeline_versions
                    WHERE pipeline_id = p.pipeline_id AND branch = p.branch AND lakefs_commit_id IS NOT NULL
                    ORDER BY created_at DESC
                    LIMIT 1
                ) v ON TRUE
                    WHERE p.db_name = $1 AND p.branch = $2
                    ORDER BY p.updated_at DESC
                    """,
                    db_name,
                    branch,
                )
            else:
                rows = await conn.fetch(
                    f"""
                    SELECT p.pipeline_id, p.db_name, p.name, p.description, p.pipeline_type, p.location,
                           p.status, p.branch, p.lakefs_repository, p.proposal_status, p.proposal_title, p.proposal_description,
                           p.proposal_submitted_at, p.proposal_reviewed_at, p.proposal_review_comment, p.proposal_bundle,
                           p.last_preview_status, p.last_preview_at, p.last_preview_rows,
                           p.last_preview_job_id, p.last_preview_node_id,
                           p.last_preview_sample, p.last_preview_nodes,
                           p.last_build_status, p.last_build_at, p.last_build_output,
                           p.deployed_at, p.deployed_commit_id, p.schedule_interval_seconds, p.schedule_cron, p.last_scheduled_at,
                           p.created_at, p.updated_at,
                           v.lakefs_commit_id AS latest_commit_id, v.definition_json, v.created_at AS version_created_at
                    FROM {self._schema}.pipelines p
                    LEFT JOIN LATERAL (
                    SELECT lakefs_commit_id, definition_json, created_at
                    FROM {self._schema}.pipeline_versions
                    WHERE pipeline_id = p.pipeline_id AND branch = p.branch AND lakefs_commit_id IS NOT NULL
                    ORDER BY created_at DESC
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
                    "branch": str(row["branch"]),
                    "lakefs_repository": row["lakefs_repository"],
                    "proposal_status": row["proposal_status"],
                    "proposal_title": row["proposal_title"],
                    "proposal_description": row["proposal_description"],
                    "proposal_submitted_at": row["proposal_submitted_at"],
                    "proposal_reviewed_at": row["proposal_reviewed_at"],
                    "proposal_review_comment": row["proposal_review_comment"],
                    "proposal_bundle": coerce_json_pipeline(row["proposal_bundle"]),
                    "last_preview_status": row["last_preview_status"],
                    "last_preview_at": row["last_preview_at"],
                    "last_preview_rows": row["last_preview_rows"],
                    "last_preview_job_id": row["last_preview_job_id"],
                    "last_preview_node_id": row["last_preview_node_id"],
                    "last_preview_sample": coerce_json_pipeline(row["last_preview_sample"]),
                    "last_preview_nodes": coerce_json_pipeline(row["last_preview_nodes"]),
                    "last_build_status": row["last_build_status"],
                    "last_build_at": row["last_build_at"],
                    "last_build_output": coerce_json_pipeline(row["last_build_output"]),
                    "deployed_at": row["deployed_at"],
                    "deployed_commit_id": row["deployed_commit_id"],
                    "schedule_interval_seconds": row["schedule_interval_seconds"],
                    "schedule_cron": row["schedule_cron"],
                    "last_scheduled_at": row["last_scheduled_at"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "latest_commit_id": row["latest_commit_id"],
                    "definition_json": coerce_json_pipeline(row["definition_json"]),
                    "version_created_at": row["version_created_at"],
                }
            )
        return output

    async def list_proposals(
        self,
        *,
        db_name: str,
        branch: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")

        clauses = ["db_name = $1", "proposal_status IS NOT NULL"]
        values: List[Any] = [db_name]
        if branch:
            clauses.append(f"branch = ${len(values) + 1}")
            values.append(branch)
        if status:
            clauses.append(f"proposal_status = ${len(values) + 1}")
            values.append(status)
        where_clause = " AND ".join(clauses)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
               SELECT pipeline_id, db_name, name, branch,
                       proposal_status, proposal_title, proposal_description,
                       proposal_submitted_at, proposal_reviewed_at, proposal_review_comment, proposal_bundle
                FROM {self._schema}.pipelines
                WHERE {where_clause}
                ORDER BY proposal_submitted_at DESC NULLS LAST, updated_at DESC
                """,
                *values,
            )

        output: List[Dict[str, Any]] = []
        for row in rows or []:
            output.append(
                {
                    "proposal_id": str(row["pipeline_id"]),
                    "pipeline_id": str(row["pipeline_id"]),
                    "db_name": str(row["db_name"]),
                    "name": str(row["name"]),
                    "branch": str(row["branch"]),
                    "status": row["proposal_status"],
                    "title": row["proposal_title"],
                    "description": row["proposal_description"],
                    "submitted_at": row["proposal_submitted_at"],
                    "reviewed_at": row["proposal_reviewed_at"],
                    "review_comment": row["proposal_review_comment"],
                    "proposal_bundle": coerce_json_pipeline(row["proposal_bundle"]),
                }
            )
        return output

    async def submit_proposal(
        self,
        *,
        pipeline_id: str,
        title: str,
        description: Optional[str],
        proposal_bundle: Optional[Dict[str, Any]] = None,
    ) -> PipelineRecord:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.pipelines
                SET proposal_status = 'pending',
                    proposal_title = $2,
                    proposal_description = $3,
                    proposal_submitted_at = NOW(),
                    proposal_reviewed_at = NULL,
                    proposal_review_comment = NULL,
                    proposal_bundle = $4,
                    updated_at = NOW()
                WHERE pipeline_id = $1
                RETURNING pipeline_id, db_name, name, description, pipeline_type, location, status,
                          branch, lakefs_repository,
                          proposal_status, proposal_title, proposal_description,
                          proposal_submitted_at, proposal_reviewed_at, proposal_review_comment, proposal_bundle,
                          last_preview_status, last_preview_at, last_preview_rows,
                          last_preview_job_id, last_preview_node_id,
                          last_preview_sample, last_preview_nodes,
                          last_build_status, last_build_at, last_build_output,
                          deployed_at, deployed_commit_id, schedule_interval_seconds, schedule_cron, last_scheduled_at,
                          created_at, updated_at
                """,
                pipeline_id,
                title,
                description,
                _ensure_json_string(proposal_bundle or {}),
            )
            if not row:
                raise RuntimeError("Pipeline not found")
            return _row_to_pipeline_record(row)

    async def review_proposal(
        self,
        *,
        pipeline_id: str,
        status: str,
        review_comment: Optional[str],
    ) -> PipelineRecord:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.pipelines
                SET proposal_status = $2,
                    proposal_review_comment = $3,
                    proposal_reviewed_at = NOW(),
                    updated_at = NOW()
                WHERE pipeline_id = $1
                RETURNING pipeline_id, db_name, name, description, pipeline_type, location, status,
                          branch, lakefs_repository,
                          proposal_status, proposal_title, proposal_description,
                          proposal_submitted_at, proposal_reviewed_at, proposal_review_comment, proposal_bundle,
                          last_preview_status, last_preview_at, last_preview_rows,
                          last_preview_job_id, last_preview_node_id,
                          last_preview_sample, last_preview_nodes,
                          last_build_status, last_build_at, last_build_output,
                          deployed_at, deployed_commit_id, schedule_interval_seconds, schedule_cron, last_scheduled_at,
                          created_at, updated_at
                """,
                pipeline_id,
                status,
                review_comment,
            )
            if not row:
                raise RuntimeError("Pipeline not found")
            return _row_to_pipeline_record(row)

    async def merge_branch(
        self,
        *,
        pipeline_id: str,
        from_branch: str,
        to_branch: str,
        user_id: Optional[str] = None,
    ) -> PipelineVersionRecord:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")

        resolved_from = str(from_branch or "").strip()
        resolved_to = str(to_branch or "").strip()
        if not resolved_from or not resolved_to:
            raise ValueError("from_branch and to_branch are required")
        if resolved_from == resolved_to:
            raise PipelineMergeNotSupportedError("from_branch and to_branch must be different")

        source = await self.get_pipeline(pipeline_id=pipeline_id)
        if not source:
            raise RuntimeError("Source pipeline not found")

        repository = self._resolve_repository(pipeline=source)
        try:
            merge_commit_id = await (await self.get_lakefs_client(user_id=user_id)).merge(
                repository=repository,
                source_ref=resolved_from,
                destination_branch=resolved_to,
                message=f"Merge {resolved_from} -> {resolved_to} (pipeline {source.db_name}/{source.name})",
                metadata={
                    "pipeline_id": source.pipeline_id,
                    "db_name": source.db_name,
                    "pipeline_name": source.name,
                },
                allow_empty=True,
            )
        except LakeFSConflictError:
            raise
        except LakeFSError as e:
            raise RuntimeError(f"lakeFS merge failed: {e}") from e

        target = await self.get_pipeline_by_name(db_name=source.db_name, name=source.name, branch=resolved_to)
        if not target:
            target = await self.create_pipeline(
                db_name=source.db_name,
                name=source.name,
                description=source.description,
                pipeline_type=source.pipeline_type,
                location=source.location,
                status=source.status,
                branch=resolved_to,
                lakefs_repository=repository,
                proposal_status=None,
                proposal_title=None,
                proposal_description=None,
                schedule_interval_seconds=source.schedule_interval_seconds,
                schedule_cron=source.schedule_cron,
            )

        definition_key = _definition_object_key(db_name=source.db_name, pipeline_name=source.name)
        merged_definition: Dict[str, Any] = {}
        try:
            merged_definition = await (await self.get_lakefs_storage(user_id=user_id)).load_json(
                bucket=repository,
                key=f"{resolved_to}/{definition_key}",
            )
        except FileNotFoundError:
            merged_definition = {}

        version_id = str(uuid4())
        definition_payload = normalize_json_payload(merged_definition, default_handler=str)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.pipeline_versions (
                    version_id, pipeline_id, branch, lakefs_commit_id, definition_json
                ) VALUES ($1, $2, $3, $4, $5)
                RETURNING version_id, pipeline_id, branch, lakefs_commit_id, definition_json, created_at
                """,
                version_id,
                target.pipeline_id,
                resolved_to,
                merge_commit_id,
                definition_payload,
            )
        if not row:
            raise RuntimeError("Failed to record merge version")
        return PipelineVersionRecord(
            version_id=str(row["version_id"]),
            pipeline_id=str(row["pipeline_id"]),
            branch=str(row["branch"]),
            lakefs_commit_id=str(row["lakefs_commit_id"]),
            definition_json=coerce_json_pipeline(row["definition_json"]),
            created_at=row["created_at"],
        )

    async def get_pipeline(self, *, pipeline_id: str) -> Optional[PipelineRecord]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
               SELECT pipeline_id, db_name, name, description, pipeline_type, location, status,
                      branch, lakefs_repository,
                      proposal_status, proposal_title, proposal_description,
                      proposal_submitted_at, proposal_reviewed_at, proposal_review_comment, proposal_bundle,
                      last_preview_status, last_preview_at, last_preview_rows,
                      last_preview_job_id, last_preview_node_id,
                      last_preview_sample, last_preview_nodes,
                      last_build_status, last_build_at, last_build_output,
                       deployed_at, deployed_commit_id, schedule_interval_seconds, schedule_cron, last_scheduled_at,
                       created_at, updated_at
                FROM {self._schema}.pipelines
                WHERE pipeline_id = $1
                """,
                pipeline_id,
            )
            if not row:
                return None
            return _row_to_pipeline_record(row)

    async def get_pipeline_by_name(
        self,
        *,
        db_name: str,
        name: str,
        branch: str = "main",
    ) -> Optional[PipelineRecord]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
               SELECT pipeline_id, db_name, name, description, pipeline_type, location, status,
                      branch, lakefs_repository,
                      proposal_status, proposal_title, proposal_description,
                      proposal_submitted_at, proposal_reviewed_at, proposal_review_comment, proposal_bundle,
                      last_preview_status, last_preview_at, last_preview_rows,
                      last_preview_job_id, last_preview_node_id,
                      last_preview_sample, last_preview_nodes,
                      last_build_status, last_build_at, last_build_output,
                       deployed_at, deployed_commit_id, schedule_interval_seconds, schedule_cron, last_scheduled_at,
                       created_at, updated_at
                FROM {self._schema}.pipelines
                WHERE db_name = $1 AND name = $2 AND branch = $3
                """,
                db_name,
                name,
                branch,
            )
            if not row:
                return None
            return _row_to_pipeline_record(row)

    async def update_pipeline(
        self,
        *,
        pipeline_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        location: Optional[str] = None,
        status: Optional[str] = None,
        schedule_interval_seconds: Optional[int] = None,
        schedule_cron: Optional[str] = None,
        branch: Optional[str] = None,
        lakefs_repository: Optional[str] = None,
        proposal_status: Optional[str] = None,
        proposal_title: Optional[str] = None,
        proposal_description: Optional[str] = None,
        proposal_submitted_at: Optional[datetime] = None,
        proposal_reviewed_at: Optional[datetime] = None,
        proposal_review_comment: Optional[str] = None,
        proposal_bundle: Optional[Dict[str, Any]] = None,
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
        if schedule_interval_seconds is not None:
            _append("schedule_interval_seconds", schedule_interval_seconds)
        if schedule_cron is not None:
            _append("schedule_cron", schedule_cron)
        if branch is not None:
            _append("branch", branch)
        if lakefs_repository is not None:
            _append("lakefs_repository", lakefs_repository)
        if proposal_status is not None:
            _append("proposal_status", proposal_status)
        if proposal_title is not None:
            _append("proposal_title", proposal_title)
        if proposal_description is not None:
            _append("proposal_description", proposal_description)
        if proposal_submitted_at is not None:
            _append("proposal_submitted_at", proposal_submitted_at)
        if proposal_reviewed_at is not None:
            _append("proposal_reviewed_at", proposal_reviewed_at)
        if proposal_review_comment is not None:
            _append("proposal_review_comment", proposal_review_comment)
        if proposal_bundle is not None:
            _append("proposal_bundle", _ensure_json_string(proposal_bundle))

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
                          branch, lakefs_repository,
                          proposal_status, proposal_title, proposal_description,
                          proposal_submitted_at, proposal_reviewed_at, proposal_review_comment, proposal_bundle,
                          last_preview_status, last_preview_at, last_preview_rows,
                          last_preview_job_id, last_preview_node_id,
                          last_preview_sample, last_preview_nodes,
                          last_build_status, last_build_at, last_build_output,
                          deployed_at, deployed_commit_id, schedule_interval_seconds, schedule_cron, last_scheduled_at,
                          created_at, updated_at
                """,
                *values,
            )
            if not row:
                raise RuntimeError("Failed to update pipeline")
            return _row_to_pipeline_record(row)

    async def add_version(
        self,
        *,
        pipeline_id: str,
        branch: Optional[str] = None,
        definition_json: Optional[Dict[str, Any]] = None,
        version_id: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> PipelineVersionRecord:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        version_id = version_id or str(uuid4())
        definition_dict = dict(definition_json or {})

        pipeline = await self.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise RuntimeError("Pipeline not found")

        resolved_branch = (branch or pipeline.branch or "main").strip() or "main"
        repository = self._resolve_repository(pipeline=pipeline)

        definition_key = _definition_object_key(db_name=pipeline.db_name, pipeline_name=pipeline.name)
        checksum = await (await self.get_lakefs_storage(user_id=user_id)).save_json(
            bucket=repository,
            key=f"{resolved_branch}/{definition_key}",
            data=definition_dict,
            metadata={
                "pipeline_id": pipeline.pipeline_id,
                "db_name": pipeline.db_name,
                "pipeline_name": pipeline.name,
                "branch": resolved_branch,
            },
        )

        lakefs_client = await self.get_lakefs_client(user_id=user_id)
        lakefs_storage = await self.get_lakefs_storage(user_id=user_id)

        async def _definition_matches_commit(commit_id: str) -> bool:
            try:
                committed = await lakefs_storage.load_json(bucket=repository, key=f"{commit_id}/{definition_key}")
            except Exception:
                return False
            return committed == definition_dict

        commit_metadata = {
            "pipeline_id": pipeline.pipeline_id,
            "db_name": pipeline.db_name,
            "pipeline_name": pipeline.name,
            "branch": resolved_branch,
            "definition_checksum": checksum,
        }

        commit_id: Optional[str] = None
        backoff_seconds = 0.15
        for attempt in range(5):
            try:
                commit_id = await lakefs_client.commit(
                    repository=repository,
                    branch=resolved_branch,
                    message=f"Update pipeline definition ({pipeline.db_name}/{pipeline.name}:{resolved_branch})",
                    metadata=commit_metadata,
                )
                break
            except LakeFSConflictError:
                raise
            except LakeFSError as exc:
                message = str(exc or "")
                if "predicate failed" not in message.lower():
                    raise RuntimeError(f"lakeFS commit failed: {exc}") from exc

                # lakeFS commits all staged changes on a branch. Under concurrent callers, another
                # commit may have already published our staged object, making this commit a no-op.
                head_commit_id = await lakefs_client.get_branch_head_commit_id(
                    repository=repository,
                    branch=resolved_branch,
                )
                if await _definition_matches_commit(head_commit_id):
                    commit_id = head_commit_id
                    break

                if attempt >= 4:
                    raise RuntimeError(f"lakeFS commit failed: {exc}") from exc
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 1.0)

        if not commit_id:
            raise RuntimeError("lakeFS commit failed: no commit id returned")

        definition_payload = normalize_json_payload(definition_dict, default_handler=str)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.pipeline_versions (
                    version_id, pipeline_id, branch, lakefs_commit_id, definition_json
                ) VALUES ($1, $2, $3, $4, $5)
                RETURNING version_id, pipeline_id, branch, lakefs_commit_id, definition_json, created_at
                """,
                version_id,
                pipeline_id,
                resolved_branch,
                commit_id,
                definition_payload,
            )
        if not row:
            raise RuntimeError("Failed to create pipeline version")
        return PipelineVersionRecord(
            version_id=str(row["version_id"]),
            pipeline_id=str(row["pipeline_id"]),
            branch=str(row["branch"]),
            lakefs_commit_id=str(row["lakefs_commit_id"]),
            definition_json=coerce_json_pipeline(row["definition_json"]),
            created_at=row["created_at"],
        )

    async def get_latest_version(self, *, pipeline_id: str, branch: Optional[str] = None) -> Optional[PipelineVersionRecord]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        async with self._pool.acquire() as conn:
            if branch:
                row = await conn.fetchrow(
                    f"""
                    SELECT version_id, pipeline_id, branch, lakefs_commit_id, definition_json, created_at
                    FROM {self._schema}.pipeline_versions
                    WHERE pipeline_id = $1 AND branch = $2 AND lakefs_commit_id IS NOT NULL
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    pipeline_id,
                    branch,
                )
            else:
                row = await conn.fetchrow(
                    f"""
                    SELECT version_id, pipeline_id, branch, lakefs_commit_id, definition_json, created_at
                    FROM {self._schema}.pipeline_versions
                    WHERE pipeline_id = $1 AND lakefs_commit_id IS NOT NULL
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    pipeline_id,
                )
            if not row:
                return None
            return PipelineVersionRecord(
                version_id=str(row["version_id"]),
                pipeline_id=str(row["pipeline_id"]),
                branch=str(row["branch"]),
                lakefs_commit_id=str(row["lakefs_commit_id"]),
                definition_json=coerce_json_pipeline(row["definition_json"]),
                created_at=row["created_at"],
            )

    async def get_version(
        self,
        *,
        pipeline_id: str,
        lakefs_commit_id: str,
        branch: Optional[str] = None,
    ) -> Optional[PipelineVersionRecord]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        resolved_branch = branch
        if not resolved_branch:
            pipeline = await self.get_pipeline(pipeline_id=pipeline_id)
            resolved_branch = pipeline.branch if pipeline else "main"

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT version_id, pipeline_id, branch, lakefs_commit_id, definition_json, created_at
                FROM {self._schema}.pipeline_versions
                WHERE pipeline_id = $1 AND branch = $2 AND lakefs_commit_id = $3
                LIMIT 1
                """,
                pipeline_id,
                resolved_branch,
                str(lakefs_commit_id),
            )
            if not row:
                return None
            return PipelineVersionRecord(
                version_id=str(row["version_id"]),
                pipeline_id=str(row["pipeline_id"]),
                branch=str(row["branch"]),
                lakefs_commit_id=str(row["lakefs_commit_id"]),
                definition_json=coerce_json_pipeline(row["definition_json"]),
                created_at=row["created_at"],
            )

    async def record_preview(
        self,
        *,
        pipeline_id: str,
        status: str,
        row_count: Optional[int],
        sample_json: Optional[Dict[str, Any]] = None,
        job_id: Optional[str] = None,
        node_id: Optional[str] = None,
    ) -> None:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        status = str(status)
        sample_payload = _ensure_json_string(sample_json or {})
        async with self._pool.acquire() as conn:
            record = {}
            if node_id:
                record = {node_id: sample_json or {}}
            await conn.execute(
                f"""
                UPDATE {self._schema}.pipelines
                SET last_preview_status = $2,
                    last_preview_at = NOW(),
                    last_preview_rows = $3,
                    last_preview_job_id = COALESCE($5, last_preview_job_id),
                    last_preview_node_id = COALESCE($6, last_preview_node_id),
                    last_preview_sample = $4,
                    last_preview_nodes = CASE
                        WHEN $7::jsonb = '{{}}'::jsonb THEN last_preview_nodes
                        ELSE last_preview_nodes || $7::jsonb
                    END,
                    updated_at = NOW()
                WHERE pipeline_id = $1
                """,
                pipeline_id,
                status,
                row_count,
                sample_payload,
                job_id,
                node_id,
                _ensure_json_string(record),
            )

    async def record_run(
        self,
        *,
        pipeline_id: str,
        job_id: str,
        mode: str,
        status: str,
        node_id: Optional[str] = None,
        row_count: Optional[int] = None,
        sample_json: Optional[Dict[str, Any]] = None,
        output_json: Optional[Dict[str, Any]] = None,
        pipeline_spec_commit_id: Optional[str] = None,
        pipeline_spec_hash: Optional[str] = None,
        input_lakefs_commits: Optional[Any] = None,
        output_lakefs_commit_id: Optional[str] = None,
        spark_conf: Optional[Any] = None,
        code_version: Optional[str] = None,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
    ) -> None:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        job_id = str(job_id)
        mode = str(mode)
        status = str(status)
        sample_payload = _ensure_json_string(sample_json or {})
        output_payload = _ensure_json_string(output_json or {})
        input_payload = (
            _ensure_json_string(input_lakefs_commits)
            if input_lakefs_commits is not None
            else None
        )
        spark_payload = _ensure_json_string(spark_conf) if spark_conf is not None else None
        pipeline_spec_commit_id = (
            str(pipeline_spec_commit_id).strip()
            if pipeline_spec_commit_id is not None
            else None
        ) or None
        pipeline_spec_hash = (
            str(pipeline_spec_hash).strip()
            if pipeline_spec_hash is not None
            else None
        ) or None
        output_lakefs_commit_id = (
            str(output_lakefs_commit_id).strip()
            if output_lakefs_commit_id is not None
            else None
        ) or None
        code_version = (str(code_version).strip() if code_version is not None else None) or None
        started_at = started_at or utcnow()
        async with self._pool.acquire() as conn:
            run_id = str(uuid4())
            await conn.execute(
                f"""
                INSERT INTO {self._schema}.pipeline_runs (
                    run_id, pipeline_id, job_id, mode, status, node_id,
                    row_count, sample_json, output_json, pipeline_spec_commit_id, pipeline_spec_hash,
                    input_lakefs_commits, output_lakefs_commit_id, spark_conf, code_version,
                    started_at, finished_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10, $11, $12::jsonb, $13, $14::jsonb, $15, $16, $17)
                ON CONFLICT (pipeline_id, job_id)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    node_id = COALESCE(EXCLUDED.node_id, pipeline_runs.node_id),
                    row_count = COALESCE(EXCLUDED.row_count, pipeline_runs.row_count),
                    sample_json = CASE
                        WHEN EXCLUDED.sample_json = '{{}}'::jsonb THEN pipeline_runs.sample_json
                        ELSE EXCLUDED.sample_json
                    END,
                    output_json = CASE
                        WHEN EXCLUDED.output_json = '{{}}'::jsonb THEN pipeline_runs.output_json
                        ELSE EXCLUDED.output_json
                    END,
                    pipeline_spec_commit_id = COALESCE(EXCLUDED.pipeline_spec_commit_id, pipeline_runs.pipeline_spec_commit_id),
                    pipeline_spec_hash = COALESCE(EXCLUDED.pipeline_spec_hash, pipeline_runs.pipeline_spec_hash),
                    input_lakefs_commits = CASE
                        WHEN EXCLUDED.input_lakefs_commits IS NULL THEN pipeline_runs.input_lakefs_commits
                        ELSE EXCLUDED.input_lakefs_commits
                    END,
                    output_lakefs_commit_id = COALESCE(EXCLUDED.output_lakefs_commit_id, pipeline_runs.output_lakefs_commit_id),
                    spark_conf = CASE
                        WHEN EXCLUDED.spark_conf IS NULL THEN pipeline_runs.spark_conf
                        ELSE EXCLUDED.spark_conf
                    END,
                    code_version = COALESCE(EXCLUDED.code_version, pipeline_runs.code_version),
                    finished_at = COALESCE(EXCLUDED.finished_at, pipeline_runs.finished_at),
                    started_at = COALESCE(pipeline_runs.started_at, EXCLUDED.started_at),
                    run_id = pipeline_runs.run_id
                """,
                run_id,
                pipeline_id,
                job_id,
                mode,
                status,
                node_id,
                row_count,
                sample_payload,
                output_payload,
                pipeline_spec_commit_id,
                pipeline_spec_hash,
                input_payload,
                output_lakefs_commit_id,
                spark_payload,
                code_version,
                started_at,
                finished_at,
            )

    async def upsert_artifact(
        self,
        *,
        pipeline_id: str,
        job_id: str,
        mode: str,
        status: str,
        run_id: Optional[str] = None,
        artifact_id: Optional[str] = None,
        definition_hash: Optional[str] = None,
        definition_commit_id: Optional[str] = None,
        pipeline_spec_hash: Optional[str] = None,
        pipeline_spec_commit_id: Optional[str] = None,
        inputs: Optional[Dict[str, Any]] = None,
        lakefs_repository: Optional[str] = None,
        lakefs_branch: Optional[str] = None,
        lakefs_commit_id: Optional[str] = None,
        outputs: Optional[List[Dict[str, Any]]] = None,
        declared_outputs: Optional[List[Dict[str, Any]]] = None,
        sampling_strategy: Optional[Dict[str, Any]] = None,
        error: Optional[Dict[str, Any]] = None,
    ) -> str:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        job_id = str(job_id)
        mode = str(mode)
        status = str(status)
        artifact_id = artifact_id or str(uuid4())
        run_id = str(run_id) if run_id else None
        definition_hash = (str(definition_hash).strip() if definition_hash is not None else None) or None
        definition_commit_id = (
            str(definition_commit_id).strip() if definition_commit_id is not None else None
        ) or None
        pipeline_spec_hash = (str(pipeline_spec_hash).strip() if pipeline_spec_hash is not None else None) or None
        pipeline_spec_commit_id = (
            str(pipeline_spec_commit_id).strip() if pipeline_spec_commit_id is not None else None
        ) or None
        inputs_payload = _ensure_json_string(inputs or {})
        normalized_outputs = _normalize_output_list(outputs, field_name="outputs")
        normalized_declared = _normalize_output_list(declared_outputs, field_name="declared_outputs")
        outputs_payload = _ensure_json_string(normalized_outputs)
        declared_payload = _ensure_json_string(normalized_declared)
        sampling_payload = _ensure_json_string(sampling_strategy or {})
        error_payload = _ensure_json_string(error or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.pipeline_artifacts (
                    artifact_id, pipeline_id, job_id, run_id, mode, status,
                    definition_hash, definition_commit_id, pipeline_spec_hash, pipeline_spec_commit_id,
                    inputs, lakefs_repository, lakefs_branch, lakefs_commit_id,
                    outputs, declared_outputs, sampling_strategy, error,
                    created_at, updated_at
                ) VALUES (
                    $1::uuid, $2::uuid, $3, $4::uuid, $5, $6,
                    $7, $8, $9, $10,
                    $11::jsonb, $12, $13, $14,
                    $15::jsonb, $16::jsonb, $17::jsonb, $18::jsonb,
                    NOW(), NOW()
                )
                ON CONFLICT (pipeline_id, job_id, mode)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    run_id = COALESCE(EXCLUDED.run_id, pipeline_artifacts.run_id),
                    definition_hash = COALESCE(EXCLUDED.definition_hash, pipeline_artifacts.definition_hash),
                    definition_commit_id = COALESCE(EXCLUDED.definition_commit_id, pipeline_artifacts.definition_commit_id),
                    pipeline_spec_hash = COALESCE(EXCLUDED.pipeline_spec_hash, pipeline_artifacts.pipeline_spec_hash),
                    pipeline_spec_commit_id = COALESCE(EXCLUDED.pipeline_spec_commit_id, pipeline_artifacts.pipeline_spec_commit_id),
                    inputs = CASE
                        WHEN EXCLUDED.inputs = '{{}}'::jsonb THEN pipeline_artifacts.inputs
                        ELSE EXCLUDED.inputs
                    END,
                    lakefs_repository = COALESCE(EXCLUDED.lakefs_repository, pipeline_artifacts.lakefs_repository),
                    lakefs_branch = COALESCE(EXCLUDED.lakefs_branch, pipeline_artifacts.lakefs_branch),
                    lakefs_commit_id = COALESCE(EXCLUDED.lakefs_commit_id, pipeline_artifacts.lakefs_commit_id),
                    outputs = CASE
                        WHEN EXCLUDED.outputs = '[]'::jsonb THEN pipeline_artifacts.outputs
                        ELSE EXCLUDED.outputs
                    END,
                    declared_outputs = CASE
                        WHEN EXCLUDED.declared_outputs = '[]'::jsonb THEN pipeline_artifacts.declared_outputs
                        ELSE EXCLUDED.declared_outputs
                    END,
                    sampling_strategy = CASE
                        WHEN EXCLUDED.sampling_strategy = '{{}}'::jsonb THEN pipeline_artifacts.sampling_strategy
                        ELSE EXCLUDED.sampling_strategy
                    END,
                    error = CASE
                        WHEN EXCLUDED.error = '{{}}'::jsonb THEN pipeline_artifacts.error
                        ELSE EXCLUDED.error
                    END,
                    updated_at = NOW()
                RETURNING artifact_id
                """,
                artifact_id,
                pipeline_id,
                job_id,
                run_id,
                mode,
                status,
                definition_hash,
                definition_commit_id,
                pipeline_spec_hash,
                pipeline_spec_commit_id,
                inputs_payload,
                lakefs_repository,
                lakefs_branch,
                lakefs_commit_id,
                outputs_payload,
                declared_payload,
                sampling_payload,
                error_payload,
            )
        if not row:
            raise RuntimeError("Failed to upsert pipeline artifact")
        return str(row["artifact_id"])

    async def get_artifact(self, *, artifact_id: str) -> Optional[PipelineArtifactRecord]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT artifact_id, pipeline_id, job_id, run_id, mode, status,
                       definition_hash, definition_commit_id, pipeline_spec_hash, pipeline_spec_commit_id,
                       inputs, lakefs_repository, lakefs_branch, lakefs_commit_id,
                       outputs, declared_outputs, sampling_strategy, error,
                       created_at, updated_at
                FROM {self._schema}.pipeline_artifacts
                WHERE artifact_id = $1::uuid
                LIMIT 1
                """,
                artifact_id,
            )
        if not row:
            return None
        return _row_to_pipeline_artifact(row)

    async def get_artifact_by_job(
        self,
        *,
        pipeline_id: str,
        job_id: str,
        mode: Optional[str] = None,
    ) -> Optional[PipelineArtifactRecord]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        job_id = str(job_id)
        mode_clause = ""
        values: list[Any] = [pipeline_id, job_id]
        if mode:
            mode_clause = f"AND mode = ${len(values) + 1}"
            values.append(str(mode))
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT artifact_id, pipeline_id, job_id, run_id, mode, status,
                       definition_hash, definition_commit_id, pipeline_spec_hash, pipeline_spec_commit_id,
                       inputs, lakefs_repository, lakefs_branch, lakefs_commit_id,
                       outputs, declared_outputs, sampling_strategy, error,
                       created_at, updated_at
                FROM {self._schema}.pipeline_artifacts
                WHERE pipeline_id = $1::uuid AND job_id = $2
                {mode_clause}
                ORDER BY created_at DESC
                LIMIT 1
                """,
                *values,
            )
        if not row:
            return None
        return _row_to_pipeline_artifact(row)

    async def list_artifacts(
        self,
        *,
        pipeline_id: str,
        limit: int = 50,
        mode: Optional[str] = None,
    ) -> List[PipelineArtifactRecord]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        limit = max(1, min(int(limit), 200))
        clause = ""
        values: list[Any] = [pipeline_id, limit]
        if mode:
            clause = f"AND mode = ${len(values) + 1}"
            values.append(str(mode))
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT artifact_id, pipeline_id, job_id, run_id, mode, status,
                       definition_hash, definition_commit_id, pipeline_spec_hash, pipeline_spec_commit_id,
                       inputs, lakefs_repository, lakefs_branch, lakefs_commit_id,
                       outputs, declared_outputs, sampling_strategy, error,
                       created_at, updated_at
                FROM {self._schema}.pipeline_artifacts
                WHERE pipeline_id = $1::uuid
                {clause}
                ORDER BY created_at DESC
                LIMIT $2
                """,
                *values,
            )
        return [_row_to_pipeline_artifact(row) for row in rows or []]

    async def list_runs(
        self,
        *,
        pipeline_id: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        limit = max(1, min(int(limit), 200))
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT run_id, pipeline_id, job_id, mode, status, node_id,
                       row_count, sample_json, output_json, pipeline_spec_commit_id, pipeline_spec_hash,
                       input_lakefs_commits, output_lakefs_commit_id, spark_conf, code_version,
                       started_at, finished_at
                FROM {self._schema}.pipeline_runs
                WHERE pipeline_id = $1
                ORDER BY started_at DESC
                LIMIT $2
                """,
                pipeline_id,
                limit,
            )
        output: List[Dict[str, Any]] = []
        for row in rows or []:
            output.append(
                {
                    "run_id": str(row["run_id"]),
                    "pipeline_id": str(row["pipeline_id"]),
                    "job_id": str(row["job_id"]),
                    "mode": row["mode"],
                    "status": row["status"],
                    "node_id": row["node_id"],
                    "row_count": row["row_count"],
                    "sample_json": coerce_json_pipeline(row["sample_json"]),
                    "output_json": coerce_json_pipeline(row["output_json"]),
                    "pipeline_spec_commit_id": row["pipeline_spec_commit_id"],
                    "pipeline_spec_hash": row["pipeline_spec_hash"],
                    "input_lakefs_commits": coerce_json_pipeline(row["input_lakefs_commits"]),
                    "output_lakefs_commit_id": row["output_lakefs_commit_id"],
                    "spark_conf": coerce_json_pipeline(row["spark_conf"]),
                    "code_version": row["code_version"],
                    "started_at": row["started_at"],
                    "finished_at": row["finished_at"],
                }
            )
        return output

    async def get_run(
        self,
        *,
        pipeline_id: str,
        job_id: str,
    ) -> Optional[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        job_id = str(job_id)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT run_id, pipeline_id, job_id, mode, status, node_id,
                       row_count, sample_json, output_json, pipeline_spec_commit_id, pipeline_spec_hash,
                       input_lakefs_commits, output_lakefs_commit_id, spark_conf, code_version,
                       started_at, finished_at
                FROM {self._schema}.pipeline_runs
                WHERE pipeline_id = $1 AND job_id = $2
                LIMIT 1
                """,
                pipeline_id,
                job_id,
            )
        if not row:
            return None
        return {
            "run_id": str(row["run_id"]),
            "pipeline_id": str(row["pipeline_id"]),
            "job_id": str(row["job_id"]),
            "mode": row["mode"],
            "status": row["status"],
            "node_id": row["node_id"],
            "row_count": row["row_count"],
            "sample_json": coerce_json_pipeline(row["sample_json"]),
            "output_json": coerce_json_pipeline(row["output_json"]),
            "pipeline_spec_commit_id": row["pipeline_spec_commit_id"],
            "pipeline_spec_hash": row["pipeline_spec_hash"],
            "input_lakefs_commits": coerce_json_pipeline(row["input_lakefs_commits"]),
            "output_lakefs_commit_id": row["output_lakefs_commit_id"],
            "spark_conf": coerce_json_pipeline(row["spark_conf"]),
            "code_version": row["code_version"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
        }

    async def get_watermarks(self, *, pipeline_id: str, branch: str) -> Dict[str, Any]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        branch = (str(branch or "").strip() or "main")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT watermarks_json
                FROM {self._schema}.pipeline_watermarks
                WHERE pipeline_id = $1 AND branch = $2
                LIMIT 1
                """,
                pipeline_id,
                branch,
            )
        if not row:
            return {}
        return coerce_json_pipeline(row["watermarks_json"])

    async def upsert_watermarks(
        self,
        *,
        pipeline_id: str,
        branch: str,
        watermarks: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        branch = (str(branch or "").strip() or "main")
        payload = _ensure_json_string(watermarks or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.pipeline_watermarks (
                    pipeline_id, branch, watermarks_json, created_at, updated_at
                ) VALUES ($1, $2, $3::jsonb, NOW(), NOW())
                ON CONFLICT (pipeline_id, branch)
                DO UPDATE SET watermarks_json = EXCLUDED.watermarks_json, updated_at = NOW()
                RETURNING watermarks_json, created_at, updated_at
                """,
                pipeline_id,
                branch,
                payload,
            )
        if not row:
            return {}
        return {
            "branch": branch,
            "watermarks": coerce_json_pipeline(row["watermarks_json"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    async def record_build(
        self,
        *,
        pipeline_id: str,
        status: str,
        output_json: Optional[Dict[str, Any]] = None,
        deployed_commit_id: Optional[str] = None,
    ) -> None:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        pipeline_id = str(pipeline_id)
        status = str(status)
        output_payload = _ensure_json_string(output_json or {})
        deployed_commit_id = (str(deployed_commit_id).strip() if deployed_commit_id is not None else None) or None
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.pipelines
                SET last_build_status = $2,
                    last_build_at = NOW(),
                    last_build_output = $3,
                    deployed_at = CASE WHEN $4::text IS NULL THEN deployed_at ELSE NOW() END,
                    deployed_commit_id = COALESCE($4::text, deployed_commit_id),
                    updated_at = NOW()
                WHERE pipeline_id = $1
                """,
                pipeline_id,
                status,
                output_payload,
                deployed_commit_id,
            )

    async def record_promotion_manifest(
        self,
        *,
        pipeline_id: str,
        db_name: str,
        build_job_id: str,
        artifact_id: Optional[str],
        definition_hash: Optional[str],
        lakefs_repository: str,
        lakefs_commit_id: str,
        ontology_commit_id: str,
        mapping_spec_id: Optional[str] = None,
        mapping_spec_version: Optional[int] = None,
        mapping_spec_target_class_id: Optional[str] = None,
        promoted_dataset_version_id: str,
        promoted_dataset_name: Optional[str] = None,
        target_branch: str,
        promoted_by: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        manifest_id: Optional[str] = None,
        promoted_at: Optional[datetime] = None,
    ) -> str:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        manifest_id = manifest_id or str(uuid4())
        pipeline_id = str(pipeline_id)
        db_name = str(db_name)
        build_job_id = str(build_job_id)
        lakefs_repository = str(lakefs_repository or "").strip()
        lakefs_commit_id = str(lakefs_commit_id or "").strip()
        ontology_commit_id = str(ontology_commit_id or "").strip()
        target_branch = str(target_branch or "").strip()
        if not pipeline_id:
            raise ValueError("pipeline_id is required")
        if not db_name:
            raise ValueError("db_name is required")
        if not build_job_id:
            raise ValueError("build_job_id is required")
        if not lakefs_repository:
            raise ValueError("lakefs_repository is required")
        if not lakefs_commit_id:
            raise ValueError("lakefs_commit_id is required")
        if not ontology_commit_id:
            raise ValueError("ontology_commit_id is required")
        if not target_branch:
            raise ValueError("target_branch is required")
        promoted_dataset_version_id = str(promoted_dataset_version_id)
        if not promoted_dataset_version_id:
            raise ValueError("promoted_dataset_version_id is required")
        if mapping_spec_version is not None:
            mapping_spec_version = int(mapping_spec_version)
        metadata_payload = _ensure_json_string(metadata or {})
        promoted_at = promoted_at or utcnow()

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.promotion_manifests (
                    manifest_id,
                    pipeline_id,
                    db_name,
                    build_job_id,
                    artifact_id,
                    definition_hash,
                    lakefs_repository,
                    lakefs_commit_id,
                    ontology_commit_id,
                    mapping_spec_id,
                    mapping_spec_version,
                    mapping_spec_target_class_id,
                    promoted_dataset_version_id,
                    promoted_dataset_name,
                    target_branch,
                    promoted_by,
                    promoted_at,
                    metadata
                ) VALUES (
                    $1::uuid,
                    $2::uuid,
                    $3,
                    $4,
                    $5::uuid,
                    $6,
                    $7,
                    $8,
                    $9,
                    $10,
                    $11,
                    $12,
                    $13::uuid,
                    $14,
                    $15,
                    $16,
                    $17,
                    $18::jsonb
                )
                RETURNING manifest_id
                """,
                manifest_id,
                pipeline_id,
                db_name,
                build_job_id,
                artifact_id,
                definition_hash,
                lakefs_repository,
                lakefs_commit_id,
                ontology_commit_id,
                mapping_spec_id,
                mapping_spec_version,
                mapping_spec_target_class_id,
                promoted_dataset_version_id,
                promoted_dataset_name,
                target_branch,
                promoted_by,
                promoted_at,
                metadata_payload,
            )
            if not row:
                raise RuntimeError("Failed to record promotion manifest")
            return str(row["manifest_id"])

    async def list_scheduled_pipelines(self) -> List[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT p.pipeline_id, p.db_name, p.pipeline_type, p.schedule_interval_seconds,
                       p.schedule_cron, p.last_scheduled_at, p.last_build_status, p.last_build_at, p.name, p.branch,
                       v.lakefs_commit_id AS latest_commit_id, v.definition_json
                FROM {self._schema}.pipelines p
                LEFT JOIN LATERAL (
                    SELECT lakefs_commit_id, definition_json
                    FROM {self._schema}.pipeline_versions
                    WHERE pipeline_id = p.pipeline_id AND branch = p.branch AND lakefs_commit_id IS NOT NULL
                    ORDER BY created_at DESC
                    LIMIT 1
                ) v ON TRUE
                WHERE p.schedule_interval_seconds IS NOT NULL
                   OR p.schedule_cron IS NOT NULL
                   OR EXISTS (
                       SELECT 1
                       FROM {self._schema}.pipeline_dependencies d
                       WHERE d.pipeline_id = p.pipeline_id
                   )
                """
            )
            pipeline_ids = [row["pipeline_id"] for row in rows or []]
            dependencies_map: Dict[str, List[Dict[str, Any]]] = {}
            if pipeline_ids:
                dep_rows = await conn.fetch(
                    f"""
                    SELECT pipeline_id, depends_on_pipeline_id, required_status
                    FROM {self._schema}.pipeline_dependencies
                    WHERE pipeline_id = ANY($1::uuid[])
                    ORDER BY created_at ASC
                    """,
                    pipeline_ids,
                )
                for dep_row in dep_rows or []:
                    pid = str(dep_row["pipeline_id"])
                    dependencies_map.setdefault(pid, []).append(
                        {
                            "pipeline_id": str(dep_row["depends_on_pipeline_id"]),
                            "status": str(dep_row["required_status"]),
                        }
                    )
        output: List[Dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["output_dataset_name"] = payload.get("name")
            payload["dependencies"] = dependencies_map.get(str(payload.get("pipeline_id")), [])
            output.append(payload)
        return output

    async def record_schedule_tick(self, *, pipeline_id: str, scheduled_at: datetime) -> None:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.pipelines
                SET last_scheduled_at = $2,
                    updated_at = NOW()
                WHERE pipeline_id = $1
                """,
                pipeline_id,
                scheduled_at,
            )

    async def list_pipeline_branches(self, *, db_name: str) -> List[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT branch, archived, archived_at, created_at, updated_at
                FROM {self._schema}.pipeline_branches
                WHERE db_name = $1
                ORDER BY branch
                """,
                db_name,
            )
        output: list[dict[str, Any]] = []
        for row in rows or []:
            output.append(
                {
                    "db_name": db_name,
                    "branch": str(row["branch"]),
                    "archived": bool(row["archived"]),
                    "archived_at": row["archived_at"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
            )
        return output

    async def get_pipeline_branch(self, *, db_name: str, branch: str) -> Optional[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        db_name = str(db_name)
        branch = str(branch)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT branch, archived, archived_at, created_at, updated_at
                FROM {self._schema}.pipeline_branches
                WHERE db_name = $1 AND branch = $2
                """,
                db_name,
                branch,
            )
        if not row:
            return None
        return {
            "db_name": db_name,
            "branch": str(row["branch"]),
            "archived": bool(row["archived"]),
            "archived_at": row["archived_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    async def archive_pipeline_branch(self, *, db_name: str, branch: str) -> Dict[str, Any]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        db_name = str(db_name)
        branch = str(branch)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"""
                    INSERT INTO {self._schema}.pipeline_branches (db_name, branch, archived, archived_at, created_at, updated_at)
                    VALUES ($1, $2, TRUE, NOW(), NOW(), NOW())
                    ON CONFLICT (db_name, branch)
                    DO UPDATE SET archived = TRUE, archived_at = NOW(), updated_at = NOW()
                    """,
                    db_name,
                    branch,
                )
                row = await conn.fetchrow(
                    f"""
                    SELECT branch, archived, archived_at, created_at, updated_at
                    FROM {self._schema}.pipeline_branches
                    WHERE db_name = $1 AND branch = $2
                    """,
                    db_name,
                    branch,
                )
        if not row:
            raise RuntimeError("Failed to archive branch")
        return {
            "db_name": db_name,
            "branch": str(row["branch"]),
            "archived": bool(row["archived"]),
            "archived_at": row["archived_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    async def restore_pipeline_branch(self, *, db_name: str, branch: str) -> Dict[str, Any]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        db_name = str(db_name)
        branch = str(branch)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"""
                    INSERT INTO {self._schema}.pipeline_branches (db_name, branch, archived, archived_at, created_at, updated_at)
                    VALUES ($1, $2, FALSE, NULL, NOW(), NOW())
                    ON CONFLICT (db_name, branch)
                    DO UPDATE SET archived = FALSE, archived_at = NULL, updated_at = NOW()
                    """,
                    db_name,
                    branch,
                )
                row = await conn.fetchrow(
                    f"""
                    SELECT branch, archived, archived_at, created_at, updated_at
                    FROM {self._schema}.pipeline_branches
                    WHERE db_name = $1 AND branch = $2
                    """,
                    db_name,
                    branch,
                )
        if not row:
            raise RuntimeError("Failed to restore branch")
        return {
            "db_name": db_name,
            "branch": str(row["branch"]),
            "archived": bool(row["archived"]),
            "archived_at": row["archived_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    async def create_branch(
        self,
        *,
        pipeline_id: str,
        new_branch: str,
        user_id: Optional[str] = None,
    ) -> PipelineRecord:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        new_branch = (new_branch or "").strip()
        if not new_branch:
            raise RuntimeError("Branch name is required")
        source = await self.get_pipeline(pipeline_id=pipeline_id)
        if not source:
            raise RuntimeError("Pipeline not found")

        repository = self._resolve_repository(pipeline=source)
        try:
            await (await self.get_lakefs_client(user_id=user_id)).create_branch(
                repository=repository,
                name=new_branch,
                source=source.branch,
            )
        except LakeFSConflictError:
            # Branch already exists (idempotent create).
            pass
        except LakeFSError as e:
            raise RuntimeError(f"lakeFS create_branch failed: {e}") from e

        existing = await self.get_pipeline_by_name(
            db_name=source.db_name,
            name=source.name,
            branch=new_branch,
        )
        if existing:
            return existing

        try:
            created = await self.create_pipeline(
                db_name=source.db_name,
                name=source.name,
                description=source.description,
                pipeline_type=source.pipeline_type,
                location=source.location,
                status=source.status,
                branch=new_branch,
                lakefs_repository=repository,
                proposal_status=None,
                proposal_title=None,
                proposal_description=None,
                schedule_interval_seconds=source.schedule_interval_seconds,
                schedule_cron=source.schedule_cron,
            )
        except PipelineAlreadyExistsError:
            existing = await self.get_pipeline_by_name(
                db_name=source.db_name,
                name=source.name,
                branch=new_branch,
            )
            if existing:
                return existing
            raise

        # Seed the branch with the latest committed definition (no new commit).
        latest = await self.get_latest_version(pipeline_id=pipeline_id, branch=source.branch)
        if latest:
            version_id = str(uuid4())
            definition_payload = normalize_json_payload(latest.definition_json, default_handler=str)
            async with self._pool.acquire() as conn:
                await conn.execute(
                    f"""
                    INSERT INTO {self._schema}.pipeline_versions (
                        version_id, pipeline_id, branch, lakefs_commit_id, definition_json
                    ) VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (pipeline_id, branch, lakefs_commit_id) DO NOTHING
                    """,
                    version_id,
                    created.pipeline_id,
                    new_branch,
                    latest.lakefs_commit_id,
                    definition_payload,
                )
        return created

    async def create_udf(
        self,
        *,
        db_name: str,
        name: str,
        code: str,
        description: Optional[str] = None,
    ) -> PipelineUdfRecord:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        name = (name or "").strip()
        if not name:
            raise RuntimeError("UDF name is required")
        code = (code or "").strip()
        if not code:
            raise RuntimeError("UDF code is required")

        udf_id = str(uuid4())
        version_id = str(uuid4())

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"""
                    INSERT INTO {self._schema}.pipeline_udfs (
                        udf_id, db_name, name, description, latest_version
                    ) VALUES ($1, $2, $3, $4, 1)
                    """,
                    udf_id,
                    db_name,
                    name,
                    description,
                )
                await conn.execute(
                    f"""
                    INSERT INTO {self._schema}.pipeline_udf_versions (
                        version_id, udf_id, version, code
                    ) VALUES ($1, $2, 1, $3)
                    """,
                    version_id,
                    udf_id,
                    code,
                )
                row = await conn.fetchrow(
                    f"SELECT * FROM {self._schema}.pipeline_udfs WHERE udf_id = $1",
                    udf_id,
                )
        if not row:
            raise RuntimeError("Failed to create UDF")
        return PipelineUdfRecord(
            udf_id=str(row["udf_id"]),
            db_name=row["db_name"],
            name=row["name"],
            description=row["description"],
            latest_version=int(row["latest_version"] or 1),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def create_udf_version(
        self,
        *,
        udf_id: str,
        code: str,
    ) -> PipelineUdfVersionRecord:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        code = (code or "").strip()
        if not code:
            raise RuntimeError("UDF code is required")
        udf_id = str(udf_id)

        version_id = str(uuid4())
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                current = await conn.fetchrow(
                    f"SELECT latest_version FROM {self._schema}.pipeline_udfs WHERE udf_id = $1",
                    udf_id,
                )
                if not current:
                    raise RuntimeError("UDF not found")
                next_version = int(current["latest_version"] or 0) + 1
                await conn.execute(
                    f"""
                    INSERT INTO {self._schema}.pipeline_udf_versions (
                        version_id, udf_id, version, code
                    ) VALUES ($1, $2, $3, $4)
                    """,
                    version_id,
                    udf_id,
                    next_version,
                    code,
                )
                await conn.execute(
                    f"""
                    UPDATE {self._schema}.pipeline_udfs
                    SET latest_version = $1, updated_at = NOW()
                    WHERE udf_id = $2
                    """,
                    next_version,
                    udf_id,
                )
                row = await conn.fetchrow(
                    f"""
                    SELECT * FROM {self._schema}.pipeline_udf_versions
                    WHERE udf_id = $1 AND version = $2
                    """,
                    udf_id,
                    next_version,
                )
        if not row:
            raise RuntimeError("Failed to create UDF version")
        return PipelineUdfVersionRecord(
            version_id=str(row["version_id"]),
            udf_id=str(row["udf_id"]),
            version=int(row["version"]),
            code=row["code"],
            created_at=row["created_at"],
        )

    async def get_udf(self, *, udf_id: str) -> Optional[PipelineUdfRecord]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT * FROM {self._schema}.pipeline_udfs WHERE udf_id = $1",
                str(udf_id),
            )
        if not row:
            return None
        return PipelineUdfRecord(
            udf_id=str(row["udf_id"]),
            db_name=row["db_name"],
            name=row["name"],
            description=row["description"],
            latest_version=int(row["latest_version"] or 1),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def get_udf_version(
        self,
        *,
        udf_id: str,
        version: int,
    ) -> Optional[PipelineUdfVersionRecord]:
        if not self._pool:
            raise RuntimeError("PipelineRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT * FROM {self._schema}.pipeline_udf_versions
                WHERE udf_id = $1 AND version = $2
                """,
                str(udf_id),
                int(version),
            )
        if not row:
            return None
        return PipelineUdfVersionRecord(
            version_id=str(row["version_id"]),
            udf_id=str(row["udf_id"]),
            version=int(row["version"]),
            code=row["code"],
            created_at=row["created_at"],
        )

    async def get_udf_latest_version(self, *, udf_id: str) -> Optional[PipelineUdfVersionRecord]:
        udf = await self.get_udf(udf_id=str(udf_id))
        if not udf:
            return None
        return await self.get_udf_version(udf_id=udf.udf_id, version=udf.latest_version)
