from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from shared.services.registries.dataset_registry_models import (
    InstanceEditRecord,
    LinkEditRecord,
    RelationshipIndexResultRecord,
    SchemaMigrationPlanRecord,
)
from shared.utils.json_utils import coerce_json_dataset, normalize_json_payload
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)


async def record_instance_edit(
    registry: Any,
    *,
    db_name: str,
    class_id: str,
    instance_id: str,
    edit_type: str,
    metadata: Optional[Dict[str, Any]] = None,
    status: str = "ACTIVE",
    fields: Optional[List[str]] = None,
) -> InstanceEditRecord:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    cleaned_fields = [str(field).strip() for field in (fields or []) if str(field).strip()]
    metadata_obj = coerce_json_dataset(metadata) if metadata is not None else {}
    if cleaned_fields:
        metadata_obj = {**metadata_obj, "fields": cleaned_fields}
    metadata_payload = normalize_json_payload(metadata_obj)
    status_value = str(status or "ACTIVE").strip().upper() or "ACTIVE"
    async with registry._pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {registry._schema}.instance_edits (
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
            metadata_payload,
        )
        if not row:
            raise RuntimeError("Failed to record instance edit")
        return registry._row_to_instance_edit(row)


async def count_instance_edits(
    registry: Any,
    *,
    db_name: str,
    class_id: str,
    status: Optional[str] = None,
) -> int:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    async with registry._pool.acquire() as conn:
        value = await conn.fetchval(
            f"""
            SELECT COUNT(*)
            FROM {registry._schema}.instance_edits
            WHERE db_name = $1 AND class_id = $2
              AND ($3::text IS NULL OR status = $3)
            """,
            db_name,
            class_id,
            status,
        )
        return int(value or 0)


async def list_instance_edits(
    registry: Any,
    *,
    db_name: str,
    class_id: Optional[str] = None,
    instance_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 200,
) -> List[InstanceEditRecord]:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    async with registry._pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT edit_id, db_name, class_id, instance_id, edit_type, status, fields, metadata, created_at
            FROM {registry._schema}.instance_edits
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
        return [registry._row_to_instance_edit(row) for row in rows]


async def clear_instance_edits(
    registry: Any,
    *,
    db_name: str,
    class_id: str,
) -> int:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    async with registry._pool.acquire() as conn:
        result = await conn.execute(
            f"""
            DELETE FROM {registry._schema}.instance_edits
            WHERE db_name = $1 AND class_id = $2
            """,
            db_name,
            class_id,
        )
    try:
        return int(str(result).split()[-1])
    except Exception:
        logger.warning("Exception fallback at dataset_registry_edits.clear_instance_edits", exc_info=True)
        return 0


async def remap_instance_edits(
    registry: Any,
    *,
    db_name: str,
    class_id: str,
    id_map: Dict[str, str],
    status: Optional[str] = None,
) -> int:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    if not id_map:
        return 0
    updated = 0
    async with registry._pool.acquire() as conn:
        async with conn.transaction():
            for old_id, new_id in id_map.items():
                if not old_id or not new_id:
                    continue
                result = await conn.execute(
                    f"""
                    UPDATE {registry._schema}.instance_edits
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
                    logger.warning("Exception fallback at dataset_registry_edits.remap_instance_edits", exc_info=True)
                    continue
    return updated


async def get_instance_edit_field_stats(
    registry: Any,
    *,
    db_name: str,
    class_id: str,
    fields: List[str],
    status: Optional[str] = "ACTIVE",
) -> Dict[str, Any]:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    normalized_fields = [str(field).strip() for field in fields if str(field).strip()]
    status_value = str(status).strip().upper() if status else None
    async with registry._pool.acquire() as conn:
        total = await conn.fetchval(
            f"""
            SELECT COUNT(*)
            FROM {registry._schema}.instance_edits
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
            FROM {registry._schema}.instance_edits
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
                FROM {registry._schema}.instance_edits
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
                FROM {registry._schema}.instance_edits
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
    registry: Any,
    *,
    db_name: str,
    class_id: str,
    field_moves: Dict[str, str],
    status: Optional[str] = "ACTIVE",
) -> int:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    if not field_moves:
        return 0
    status_value = str(status).strip().upper() if status else None
    updated = 0
    async with registry._pool.acquire() as conn:
        async with conn.transaction():
            for old_field, new_field in field_moves.items():
                old_name = str(old_field or "").strip()
                new_name = str(new_field or "").strip()
                if not old_name or not new_name or old_name == new_name:
                    continue
                result = await conn.execute(
                    f"""
                    UPDATE {registry._schema}.instance_edits
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
                    logger.warning("Exception fallback at dataset_registry_edits.apply_instance_edit_field_moves", exc_info=True)
                    continue
    return updated


async def update_instance_edit_status_by_fields(
    registry: Any,
    *,
    db_name: str,
    class_id: str,
    fields: List[str],
    new_status: str,
    status: Optional[str] = "ACTIVE",
    metadata_note: Optional[str] = None,
) -> int:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    normalized_fields = [str(field).strip() for field in fields if str(field).strip()]
    if not normalized_fields:
        return 0
    status_value = str(status).strip().upper() if status else None
    next_status = str(new_status or "").strip().upper()
    if not next_status:
        return 0
    payload_note = metadata_note or None
    async with registry._pool.acquire() as conn:
        result = await conn.execute(
            f"""
            UPDATE {registry._schema}.instance_edits
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
        logger.warning("Exception fallback at dataset_registry_edits.update_instance_edit_status_by_fields", exc_info=True)
        return 0


async def list_relationship_index_results(
    registry: Any,
    *,
    relationship_spec_id: Optional[str] = None,
    link_type_id: Optional[str] = None,
    db_name: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 200,
) -> List[RelationshipIndexResultRecord]:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    async with registry._pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT result_id, relationship_spec_id, link_type_id, db_name, dataset_id, dataset_version_id,
                   mapping_spec_id, mapping_spec_version, status, stats, errors, lineage, created_at
            FROM {registry._schema}.relationship_index_results
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
        return [registry._row_to_relationship_index_result(row) for row in rows]


async def record_link_edit(
    registry: Any,
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
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    edit_id = edit_id or str(uuid4())
    payload = normalize_json_payload(metadata or {})
    async with registry._pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {registry._schema}.link_edits (
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
        return registry._row_to_link_edit(row)


async def list_link_edits(
    registry: Any,
    *,
    db_name: str,
    link_type_id: Optional[str] = None,
    branch: Optional[str] = None,
    status: Optional[str] = None,
    source_instance_id: Optional[str] = None,
    target_instance_id: Optional[str] = None,
    limit: int = 200,
) -> List[LinkEditRecord]:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    async with registry._pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT edit_id, db_name, link_type_id, branch, source_object_type, target_object_type,
                   predicate, source_instance_id, target_instance_id, edit_type, status, metadata, created_at
            FROM {registry._schema}.link_edits
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
        return [registry._row_to_link_edit(row) for row in rows]


async def clear_link_edits(
    registry: Any,
    *,
    db_name: str,
    link_type_id: str,
    branch: Optional[str] = None,
) -> int:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    async with registry._pool.acquire() as conn:
        result = await conn.execute(
            f"""
            DELETE FROM {registry._schema}.link_edits
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
        logger.warning("Exception fallback at dataset_registry_edits.clear_link_edits", exc_info=True)
        return 0


async def create_schema_migration_plan(
    registry: Any,
    *,
    db_name: str,
    subject_type: str,
    subject_id: str,
    plan: Dict[str, Any],
    status: str = "PENDING",
    plan_id: Optional[str] = None,
) -> SchemaMigrationPlanRecord:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    plan_id = plan_id or str(uuid4())
    payload = normalize_json_payload(plan or {})
    async with registry._pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {registry._schema}.schema_migration_plans (
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
        return registry._row_to_schema_migration_plan(row)


async def list_schema_migration_plans(
    registry: Any,
    *,
    db_name: Optional[str] = None,
    subject_type: Optional[str] = None,
    subject_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 200,
) -> List[SchemaMigrationPlanRecord]:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    async with registry._pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT plan_id, db_name, subject_type, subject_id, status, plan, created_at, updated_at
            FROM {registry._schema}.schema_migration_plans
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
        return [registry._row_to_schema_migration_plan(row) for row in rows]
