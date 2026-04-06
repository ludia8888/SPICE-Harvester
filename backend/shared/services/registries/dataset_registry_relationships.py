from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from uuid import uuid4

from shared.services.registries.dataset_registry_common import require_dataset_registry_pool as _require_pool
from shared.utils.json_utils import normalize_json_payload
from shared.utils.time_utils import utcnow

from shared.services.registries.dataset_registry_models import RelationshipSpecRecord

if TYPE_CHECKING:
    from shared.services.registries.dataset_registry import DatasetRegistry
async def list_relationship_specs(
    registry: "DatasetRegistry",
    *,
    db_name: Optional[str] = None,
    dataset_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 200,
) -> List[RelationshipSpecRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT relationship_spec_id, link_type_id, db_name, source_object_type, target_object_type,
                   predicate, spec_type, dataset_id, dataset_version_id, mapping_spec_id, mapping_spec_version,
                   spec, status, auto_sync, last_index_status, last_indexed_at, last_index_result_id,
                   last_index_stats, last_index_dataset_version_id, last_index_mapping_spec_version,
                   created_at, updated_at
            FROM {registry._schema}.relationship_specs
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
        return [registry._row_to_relationship_spec(row) for row in rows]


async def list_relationship_specs_by_relationship_object_type(
    registry: "DatasetRegistry",
    *,
    db_name: str,
    relationship_object_type: str,
    status: Optional[str] = "ACTIVE",
    limit: int = 200,
) -> List[RelationshipSpecRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT relationship_spec_id, link_type_id, db_name, source_object_type, target_object_type,
                   predicate, spec_type, dataset_id, dataset_version_id, mapping_spec_id, mapping_spec_version,
                   spec, status, auto_sync, last_index_status, last_indexed_at, last_index_result_id,
                   last_index_stats, last_index_dataset_version_id, last_index_mapping_spec_version,
                   created_at, updated_at
            FROM {registry._schema}.relationship_specs
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
        return [registry._row_to_relationship_spec(row) for row in rows]


async def create_relationship_spec(
    registry: "DatasetRegistry",
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
    pool = _require_pool(registry)
    resolved_relationship_spec_id = relationship_spec_id or str(uuid4())
    payload = normalize_json_payload(spec or {})
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {registry._schema}.relationship_specs (
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
            resolved_relationship_spec_id,
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
        return registry._row_to_relationship_spec(row)


async def update_relationship_spec(
    registry: "DatasetRegistry",
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
    pool = _require_pool(registry)
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
        return await get_relationship_spec(registry, relationship_spec_id=relationship_spec_id)

    values.append(relationship_spec_id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE {registry._schema}.relationship_specs
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
        return registry._row_to_relationship_spec(row)


async def record_relationship_index_result(
    registry: "DatasetRegistry",
    *,
    relationship_spec_id: str,
    status: str,
    stats: Optional[Dict[str, Any]] = None,
    errors: Optional[List[Dict[str, Any]]] = None,
    dataset_version_id: Optional[str] = None,
    mapping_spec_version: Optional[int] = None,
    lineage: Optional[Dict[str, Any]] = None,
    indexed_at: Optional[datetime] = None,
):
    pool = _require_pool(registry)
    resolved_indexed_at = indexed_at or utcnow()
    stats_payload = normalize_json_payload(stats or {})
    errors_payload = normalize_json_payload(errors or [])
    lineage_payload = normalize_json_payload(lineage or {})
    async with pool.acquire() as conn:
        spec_row = await conn.fetchrow(
            f"""
            SELECT relationship_spec_id, link_type_id, db_name, dataset_id, dataset_version_id,
                   mapping_spec_id, mapping_spec_version
            FROM {registry._schema}.relationship_specs
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
            INSERT INTO {registry._schema}.relationship_index_results (
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
            UPDATE {registry._schema}.relationship_specs
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
            resolved_indexed_at,
            result_id,
            stats_payload,
            resolved_dataset_version_id,
            resolved_mapping_version,
            relationship_spec_id,
        )
        if not row:
            return None
        return registry._row_to_relationship_index_result(row)


async def get_relationship_spec(
    registry: "DatasetRegistry",
    *,
    relationship_spec_id: Optional[str] = None,
    link_type_id: Optional[str] = None,
) -> Optional[RelationshipSpecRecord]:
    pool = _require_pool(registry)
    if not relationship_spec_id and not link_type_id:
        raise ValueError("relationship_spec_id or link_type_id is required")
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT relationship_spec_id, link_type_id, db_name, source_object_type, target_object_type,
                   predicate, spec_type, dataset_id, dataset_version_id, mapping_spec_id, mapping_spec_version,
                   spec, status, auto_sync, last_index_status, last_indexed_at, last_index_result_id,
                   last_index_stats, last_index_dataset_version_id, last_index_mapping_spec_version,
                   created_at, updated_at
            FROM {registry._schema}.relationship_specs
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
        return registry._row_to_relationship_spec(row)
