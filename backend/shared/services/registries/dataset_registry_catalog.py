from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional
from uuid import uuid4

from shared.services.registries.dataset_registry_get_or_create import get_or_create_record
from shared.services.registries.dataset_registry_common import require_dataset_registry_pool as _require_pool
from shared.services.registries.dataset_registry_models import (
    BackingDatasourceRecord,
    BackingDatasourceVersionRecord,
    DatasetRecord,
    DatasetVersionRecord,
)
from shared.services.registries.dataset_registry_rows import (
    row_to_backing,
    row_to_backing_version,
    row_to_dataset,
    row_to_dataset_version,
)
from shared.utils.json_utils import coerce_json_dataset, normalize_json_payload
from shared.utils.schema_hash import compute_schema_hash_from_payload
from shared.utils.s3_uri import is_s3_uri

if TYPE_CHECKING:
    from shared.services.registries.dataset_registry import DatasetRegistry


logger = logging.getLogger(__name__)
async def create_dataset(
    registry: "DatasetRegistry",
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
    pool = _require_pool(registry)
    resolved_dataset_id = dataset_id or str(uuid4())
    schema_payload = normalize_json_payload(schema_json or {})

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {registry._schema}.datasets (
                dataset_id, db_name, name, description, source_type, source_ref, branch, schema_json
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            RETURNING dataset_id, db_name, name, description, source_type, source_ref,
                      branch, schema_json, created_at, updated_at
            """,
            resolved_dataset_id,
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
        return row_to_dataset(row)


async def list_datasets(
    registry: "DatasetRegistry",
    *,
    db_name: str,
    branch: Optional[str] = None,
) -> List[Dict[str, Any]]:
    pool = _require_pool(registry)

    clause = "WHERE d.db_name = $1"
    values: List[Any] = [db_name]
    if branch:
        clause += f" AND d.branch = ${len(values) + 1}"
        values.append(branch)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT d.dataset_id, d.db_name, d.name, d.description, d.source_type,
                   d.source_ref, d.branch, d.schema_json, d.created_at, d.updated_at,
                   v.version_id AS dataset_version_id, v.lakefs_commit_id AS latest_commit_id,
                   v.artifact_key, v.row_count, v.sample_json, v.created_at AS version_created_at
            FROM {registry._schema}.datasets d
            LEFT JOIN LATERAL (
                SELECT version_id, lakefs_commit_id, artifact_key, row_count, sample_json, created_at
                FROM {registry._schema}.dataset_versions
                WHERE dataset_id = d.dataset_id
                ORDER BY created_at DESC
                LIMIT 1
            ) v ON TRUE
            {clause}
            ORDER BY d.updated_at DESC
            """,
            *values,
        )

    return [
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
            "dataset_version_id": str(row["dataset_version_id"]) if row["dataset_version_id"] else None,
            "latest_commit_id": row["latest_commit_id"],
            "artifact_key": row["artifact_key"],
            "row_count": row["row_count"],
            "sample_json": coerce_json_dataset(row["sample_json"]),
            "version_created_at": row["version_created_at"],
        }
        for row in rows or []
    ]


async def get_dataset(
    registry: "DatasetRegistry",
    *,
    dataset_id: str,
) -> Optional[DatasetRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT dataset_id, db_name, name, description, source_type,
                   source_ref, branch, schema_json, created_at, updated_at
            FROM {registry._schema}.datasets
            WHERE dataset_id = $1
            """,
            dataset_id,
        )
        return row_to_dataset(row) if row else None


async def get_dataset_by_name(
    registry: "DatasetRegistry",
    *,
    db_name: str,
    name: str,
    branch: str = "main",
) -> Optional[DatasetRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT dataset_id, db_name, name, description, source_type,
                   source_ref, branch, schema_json, created_at, updated_at
            FROM {registry._schema}.datasets
            WHERE db_name = $1 AND name = $2 AND branch = $3
            """,
            db_name,
            name,
            branch,
        )
        return row_to_dataset(row) if row else None


async def get_dataset_by_source_ref(
    registry: "DatasetRegistry",
    *,
    db_name: str,
    source_type: str,
    source_ref: str,
    branch: str = "main",
) -> Optional[DatasetRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT dataset_id, db_name, name, description, source_type,
                   source_ref, branch, schema_json, created_at, updated_at
            FROM {registry._schema}.datasets
            WHERE db_name = $1 AND source_type = $2 AND source_ref = $3 AND branch = $4
            """,
            db_name,
            source_type,
            source_ref,
            branch,
        )
        return row_to_dataset(row) if row else None


async def add_version(
    registry: "DatasetRegistry",
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
    pool = _require_pool(registry)
    resolved_version_id = version_id or str(uuid4())
    resolved_sample = sample_json or {}
    sample_payload = normalize_json_payload(resolved_sample)
    resolved_commit_id = str(lakefs_commit_id or "").strip()
    if not resolved_commit_id:
        raise ValueError("lakefs_commit_id is required")
    if artifact_key:
        artifact_key = artifact_key.strip()
        if artifact_key and not is_s3_uri(artifact_key):
            raise ValueError("artifact_key must be an s3:// URI")
    schema_payload = normalize_json_payload(schema_json) if schema_json is not None else None
    schema_hash = registry._resolve_version_schema_hash(sample_json=resolved_sample, schema_json=schema_json)

    async with pool.acquire() as conn:
        async with conn.transaction():
            if schema_payload is not None:
                await conn.execute(
                    f"""
                    UPDATE {registry._schema}.datasets
                    SET schema_json = $2::jsonb, updated_at = NOW()
                    WHERE dataset_id = $1
                    """,
                    dataset_id,
                    schema_payload,
                )
            row = await conn.fetchrow(
                f"""
                INSERT INTO {registry._schema}.dataset_versions (
                    version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                    ingest_request_id, promoted_from_artifact_id
                ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::uuid, $8::uuid)
                RETURNING version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                          ingest_request_id, promoted_from_artifact_id, created_at
                """,
                resolved_version_id,
                dataset_id,
                resolved_commit_id,
                artifact_key,
                row_count,
                sample_payload,
                ingest_request_id,
                promoted_from_artifact_id,
            )
            if not row:
                raise RuntimeError("Failed to create dataset version")
            await registry._upsert_backing_version_for_dataset_version(
                conn,
                dataset_id=dataset_id,
                dataset_version_id=str(row["version_id"]),
                artifact_key=artifact_key,
                schema_hash=schema_hash,
            )
        if not row:
            raise RuntimeError("Failed to create dataset version")
        return row_to_dataset_version(row)


async def list_all_datasets(
    registry: "DatasetRegistry",
    *,
    branch: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    order_by: Optional[str] = None,
) -> List[Dict[str, Any]]:
    pool = _require_pool(registry)
    resolved_limit = max(1, int(limit))
    resolved_offset = max(0, int(offset))
    clause = ""
    values: List[Any] = []
    if branch:
        clause = "WHERE d.branch = $1"
        values.append(str(branch))
    normalized_order = str(order_by or "").strip().upper()
    if "CREATED" in normalized_order:
        order_clause = "ORDER BY d.created_at DESC"
    elif "NAME" in normalized_order:
        order_clause = "ORDER BY d.name ASC"
    else:
        order_clause = "ORDER BY d.updated_at DESC"
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT d.dataset_id, d.db_name, d.name, d.description, d.source_type,
                   d.source_ref, d.branch, d.schema_json, d.created_at, d.updated_at
            FROM {registry._schema}.datasets d
            {clause}
            {order_clause}
            LIMIT ${len(values) + 1} OFFSET ${len(values) + 2}
            """,
            *values,
            resolved_limit,
            resolved_offset,
        )
    return [
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
        }
        for row in rows or []
    ]


async def count_datasets_by_db_names(
    registry: "DatasetRegistry",
    *,
    db_names: Iterable[str],
    branch: Optional[str] = None,
) -> Dict[str, int]:
    pool = _require_pool(registry)
    names = [name for name in db_names if name]
    if not names:
        return {}

    clause = "WHERE d.db_name = ANY($1)"
    values: List[Any] = [names]
    if branch:
        clause += f" AND d.branch = ${len(values) + 1}"
        values.append(branch)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT d.db_name, COUNT(*) AS count
            FROM {registry._schema}.datasets d
            {clause}
            GROUP BY d.db_name
            """,
            *values,
        )
    return {str(row["db_name"]): int(row["count"] or 0) for row in rows or []}


async def get_latest_version(
    registry: "DatasetRegistry",
    *,
    dataset_id: str,
) -> Optional[DatasetVersionRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                   ingest_request_id, promoted_from_artifact_id, created_at
            FROM {registry._schema}.dataset_versions
            WHERE dataset_id = $1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            dataset_id,
        )
        return row_to_dataset_version(row) if row else None


async def get_version(
    registry: "DatasetRegistry",
    *,
    version_id: str,
) -> Optional[DatasetVersionRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                   ingest_request_id, promoted_from_artifact_id, created_at
            FROM {registry._schema}.dataset_versions
            WHERE version_id = $1::uuid
            """,
            version_id,
        )
        return row_to_dataset_version(row) if row else None


async def get_version_by_ingest_request(
    registry: "DatasetRegistry",
    *,
    ingest_request_id: str,
) -> Optional[DatasetVersionRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT version_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json,
                   ingest_request_id, promoted_from_artifact_id, created_at
            FROM {registry._schema}.dataset_versions
            WHERE ingest_request_id = $1::uuid
            """,
            ingest_request_id,
        )
        return row_to_dataset_version(row) if row else None


async def create_backing_datasource(
    registry: "DatasetRegistry",
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
    pool = _require_pool(registry)
    resolved_backing_id = backing_id or str(uuid4())
    resolved_branch = branch or "main"
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {registry._schema}.backing_datasources (
                backing_id, dataset_id, db_name, name, description, source_type, source_ref, branch
            ) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8)
            RETURNING backing_id, dataset_id, db_name, name, description, source_type, source_ref,
                      branch, status, created_at, updated_at
            """,
            resolved_backing_id,
            dataset_id,
            db_name,
            name,
            description,
            source_type,
            source_ref,
            resolved_branch,
        )
        if not row:
            raise RuntimeError("Failed to create backing datasource")
        return row_to_backing(row)


async def get_backing_datasource(
    registry: "DatasetRegistry",
    *,
    backing_id: str,
) -> Optional[BackingDatasourceRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT backing_id, dataset_id, db_name, name, description, source_type, source_ref,
                   branch, status, created_at, updated_at
            FROM {registry._schema}.backing_datasources
            WHERE backing_id = $1::uuid
            """,
            backing_id,
        )
        return row_to_backing(row) if row else None


async def get_backing_datasource_by_dataset(
    registry: "DatasetRegistry",
    *,
    dataset_id: str,
    branch: Optional[str] = None,
) -> Optional[BackingDatasourceRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT backing_id, dataset_id, db_name, name, description, source_type, source_ref,
                   branch, status, created_at, updated_at
            FROM {registry._schema}.backing_datasources
            WHERE dataset_id = $1::uuid
              AND ($2::text IS NULL OR branch = $2)
            ORDER BY created_at DESC
            LIMIT 1
            """,
            dataset_id,
            branch,
        )
        return row_to_backing(row) if row else None


async def list_backing_datasources(
    registry: "DatasetRegistry",
    *,
    db_name: str,
    branch: Optional[str] = None,
    limit: int = 200,
) -> List[BackingDatasourceRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT backing_id, dataset_id, db_name, name, description, source_type, source_ref,
                   branch, status, created_at, updated_at
            FROM {registry._schema}.backing_datasources
            WHERE db_name = $1
              AND ($2::text IS NULL OR branch = $2)
            ORDER BY created_at DESC
            LIMIT $3
            """,
            db_name,
            branch,
            limit,
        )
        return [row_to_backing(row) for row in rows]


async def get_or_create_backing_datasource(
    registry: "DatasetRegistry",
    *,
    dataset: DatasetRecord,
    source_type: str = "dataset",
    source_ref: Optional[str] = None,
) -> BackingDatasourceRecord:
    record, _ = await get_or_create_record(
        lookup=lambda: registry.get_backing_datasource_by_dataset(
            dataset_id=dataset.dataset_id,
            branch=dataset.branch,
        ),
        create=lambda: registry.create_backing_datasource(
            dataset_id=dataset.dataset_id,
            db_name=dataset.db_name,
            name=dataset.name,
            description=dataset.description,
            source_type=source_type,
            source_ref=source_ref,
            branch=dataset.branch,
        ),
        conflict_context=f"backing-datasource:{dataset.db_name}/{dataset.name}@{dataset.branch}",
    )
    return record


async def resolve_backing_datasource_version_materialization(
    registry: "DatasetRegistry",
    *,
    backing_id: str,
    dataset_version_id: str,
    schema_hash: Optional[str] = None,
) -> tuple[BackingDatasourceRecord, DatasetVersionRecord, str]:
    backing = await registry.get_backing_datasource(backing_id=backing_id)
    if not backing:
        raise RuntimeError("Backing datasource not found")
    version = await registry.get_version(version_id=dataset_version_id)
    if not version or version.dataset_id != backing.dataset_id:
        raise RuntimeError("Dataset version mismatch")

    provided_schema_hash = str(schema_hash or "").strip() or None
    canonical_schema_hash = compute_schema_hash_from_payload(version.sample_json)
    if not canonical_schema_hash:
        dataset = await registry.get_dataset(dataset_id=backing.dataset_id)
        canonical_schema_hash = compute_schema_hash_from_payload(dataset.schema_json if dataset else {})
    if canonical_schema_hash:
        if provided_schema_hash and provided_schema_hash != canonical_schema_hash:
            logger.warning(
                "Backing datasource version schema_hash mismatch; using canonical hash "
                "(backing_id=%s dataset_version_id=%s provided=%s canonical=%s)",
                backing_id,
                dataset_version_id,
                provided_schema_hash,
                canonical_schema_hash,
            )
        return backing, version, canonical_schema_hash
    if provided_schema_hash:
        return backing, version, provided_schema_hash
    raise RuntimeError("schema_hash is required for backing datasource version")


async def insert_backing_datasource_version(
    registry: "DatasetRegistry",
    *,
    backing_id: str,
    dataset_version_id: str,
    schema_hash: str,
    artifact_key: Optional[str],
    metadata_payload: Dict[str, Any],
) -> BackingDatasourceVersionRecord:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {registry._schema}.backing_datasource_versions (
                backing_version_id, backing_id, dataset_version_id, schema_hash, artifact_key, metadata
            ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6::jsonb)
            RETURNING backing_version_id, backing_id, dataset_version_id, schema_hash, artifact_key,
                      metadata, status, created_at
            """,
            str(uuid4()),
            backing_id,
            dataset_version_id,
            schema_hash,
            artifact_key,
            metadata_payload,
        )
        if not row:
            raise RuntimeError("Failed to create backing datasource version")
        return row_to_backing_version(row)


async def reconcile_backing_datasource_version(
    registry: "DatasetRegistry",
    *,
    record: BackingDatasourceVersionRecord,
    schema_hash: str,
    artifact_key: Optional[str],
) -> BackingDatasourceVersionRecord:
    if record.schema_hash == schema_hash and record.artifact_key == artifact_key:
        return record
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE {registry._schema}.backing_datasource_versions
            SET schema_hash = $2,
                artifact_key = $3
            WHERE backing_version_id = $1::uuid
            RETURNING backing_version_id, backing_id, dataset_version_id, schema_hash, artifact_key,
                      metadata, status, created_at
            """,
            record.version_id,
            schema_hash,
            artifact_key,
        )
        if not row:
            raise RuntimeError("Failed to reconcile backing datasource version")
        return row_to_backing_version(row)


async def create_backing_datasource_version(
    registry: "DatasetRegistry",
    *,
    backing_id: str,
    dataset_version_id: str,
    schema_hash: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> BackingDatasourceVersionRecord:
    _, version, resolved_schema_hash = await registry._resolve_backing_datasource_version_materialization(
        backing_id=backing_id,
        dataset_version_id=dataset_version_id,
        schema_hash=schema_hash,
    )
    return await registry._insert_backing_datasource_version(
        backing_id=backing_id,
        dataset_version_id=dataset_version_id,
        schema_hash=resolved_schema_hash,
        artifact_key=version.artifact_key,
        metadata_payload=normalize_json_payload(metadata or {}),
    )


async def get_backing_datasource_version(
    registry: "DatasetRegistry",
    *,
    version_id: str,
) -> Optional[BackingDatasourceVersionRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT backing_version_id, backing_id, dataset_version_id, schema_hash, artifact_key,
                   metadata, status, created_at
            FROM {registry._schema}.backing_datasource_versions
            WHERE backing_version_id = $1::uuid
            """,
            version_id,
        )
        return row_to_backing_version(row) if row else None


async def get_backing_datasource_version_by_dataset_version(
    registry: "DatasetRegistry",
    *,
    dataset_version_id: str,
) -> Optional[BackingDatasourceVersionRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT backing_version_id, backing_id, dataset_version_id, schema_hash, artifact_key,
                   metadata, status, created_at
            FROM {registry._schema}.backing_datasource_versions
            WHERE dataset_version_id = $1::uuid
            ORDER BY created_at DESC
            LIMIT 1
            """,
            dataset_version_id,
        )
        return row_to_backing_version(row) if row else None


async def get_backing_datasource_version_for_dataset(
    registry: "DatasetRegistry",
    *,
    backing_id: str,
    dataset_version_id: str,
) -> Optional[BackingDatasourceVersionRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT backing_version_id, backing_id, dataset_version_id, schema_hash, artifact_key,
                   metadata, status, created_at
            FROM {registry._schema}.backing_datasource_versions
            WHERE backing_id = $1::uuid
              AND dataset_version_id = $2::uuid
            LIMIT 1
            """,
            backing_id,
            dataset_version_id,
        )
        return row_to_backing_version(row) if row else None


async def list_backing_datasource_versions(
    registry: "DatasetRegistry",
    *,
    backing_id: str,
    limit: int = 200,
) -> List[BackingDatasourceVersionRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT backing_version_id, backing_id, dataset_version_id, schema_hash, artifact_key,
                   metadata, status, created_at
            FROM {registry._schema}.backing_datasource_versions
            WHERE backing_id = $1::uuid
            ORDER BY created_at DESC
            LIMIT $2
            """,
            backing_id,
            limit,
        )
        return [row_to_backing_version(row) for row in rows]


async def get_or_create_backing_datasource_version(
    registry: "DatasetRegistry",
    *,
    backing_id: str,
    dataset_version_id: str,
    schema_hash: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> BackingDatasourceVersionRecord:
    _, version, resolved_schema_hash = await registry._resolve_backing_datasource_version_materialization(
        backing_id=backing_id,
        dataset_version_id=dataset_version_id,
        schema_hash=schema_hash,
    )
    record, _ = await get_or_create_record(
        lookup=lambda: registry.get_backing_datasource_version_for_dataset(
            backing_id=backing_id,
            dataset_version_id=dataset_version_id,
        ),
        create=lambda: registry._insert_backing_datasource_version(
            backing_id=backing_id,
            dataset_version_id=dataset_version_id,
            schema_hash=resolved_schema_hash,
            artifact_key=version.artifact_key,
            metadata_payload=normalize_json_payload(metadata or {}),
        ),
        conflict_context=f"backing-datasource-version:{backing_id}:{dataset_version_id}",
    )
    return await registry._reconcile_backing_datasource_version(
        record=record,
        schema_hash=resolved_schema_hash,
        artifact_key=version.artifact_key,
    )
