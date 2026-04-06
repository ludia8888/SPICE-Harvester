from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional
from uuid import uuid4

import asyncpg

from shared.services.registries.dataset_registry_common import require_dataset_registry_pool as _require_pool
from shared.services.registries.dataset_registry_models import (
    AccessPolicyRecord,
    GatePolicyRecord,
    GateResultRecord,
    KeySpecRecord,
)
from shared.utils.json_utils import normalize_json_payload

if TYPE_CHECKING:
    from shared.services.registries.dataset_registry import DatasetRegistry
async def create_key_spec(
    registry: "DatasetRegistry",
    *,
    dataset_id: str,
    spec: Dict[str, Any],
    dataset_version_id: Optional[str] = None,
    status: str = "ACTIVE",
    key_spec_id: Optional[str] = None,
) -> KeySpecRecord:
    pool = _require_pool(registry)
    resolved_key_spec_id = key_spec_id or str(uuid4())
    payload = normalize_json_payload(spec or {})
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {registry._schema}.key_specs (
                key_spec_id, dataset_id, dataset_version_id, spec, status
            ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4::jsonb, $5)
            RETURNING key_spec_id, dataset_id, dataset_version_id, spec, status, created_at, updated_at
            """,
            resolved_key_spec_id,
            dataset_id,
            dataset_version_id,
            payload,
            status,
        )
        if not row:
            raise RuntimeError("Failed to create key spec")
        return registry._row_to_key_spec(row)


async def get_key_spec_for_scope(
    registry: "DatasetRegistry",
    conn: asyncpg.Connection,
    *,
    dataset_id: str,
    dataset_version_id: Optional[str] = None,
) -> Optional[asyncpg.Record]:
    if dataset_version_id:
        return await conn.fetchrow(
            f"""
            SELECT key_spec_id, dataset_id, dataset_version_id, spec, status, created_at, updated_at
            FROM {registry._schema}.key_specs
            WHERE dataset_id = $1::uuid
              AND dataset_version_id = $2::uuid
              AND status = 'ACTIVE'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            dataset_id,
            dataset_version_id,
        )
    return await conn.fetchrow(
        f"""
        SELECT key_spec_id, dataset_id, dataset_version_id, spec, status, created_at, updated_at
        FROM {registry._schema}.key_specs
        WHERE dataset_id = $1::uuid
          AND dataset_version_id IS NULL
          AND status = 'ACTIVE'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        dataset_id,
    )


async def get_or_create_key_spec(
    registry: "DatasetRegistry",
    *,
    dataset_id: str,
    spec: Dict[str, Any],
    dataset_version_id: Optional[str] = None,
    status: str = "ACTIVE",
    key_spec_id: Optional[str] = None,
) -> tuple[KeySpecRecord, bool]:
    pool = _require_pool(registry)
    resolved_key_spec_id = key_spec_id or str(uuid4())
    payload = normalize_json_payload(spec or {})
    lock_key = registry._key_spec_scope_lock_key(
        dataset_id=dataset_id,
        dataset_version_id=dataset_version_id,
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", lock_key)
            existing = await get_key_spec_for_scope(
                registry,
                conn,
                dataset_id=dataset_id,
                dataset_version_id=dataset_version_id,
            )
            if existing:
                return registry._row_to_key_spec(existing), False

            row = await conn.fetchrow(
                f"""
                INSERT INTO {registry._schema}.key_specs (
                    key_spec_id, dataset_id, dataset_version_id, spec, status
                ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4::jsonb, $5)
                RETURNING key_spec_id, dataset_id, dataset_version_id, spec, status, created_at, updated_at
                """,
                resolved_key_spec_id,
                dataset_id,
                dataset_version_id,
                payload,
                status,
            )
            if not row:
                raise RuntimeError("Failed to create key spec")
            return registry._row_to_key_spec(row), True


async def get_key_spec(
    registry: "DatasetRegistry",
    *,
    key_spec_id: str,
) -> Optional[KeySpecRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT key_spec_id, dataset_id, dataset_version_id, spec, status, created_at, updated_at
            FROM {registry._schema}.key_specs
            WHERE key_spec_id = $1::uuid
            """,
            key_spec_id,
        )
        if not row:
            return None
        return registry._row_to_key_spec(row)


async def get_key_spec_for_dataset(
    registry: "DatasetRegistry",
    *,
    dataset_id: str,
    dataset_version_id: Optional[str] = None,
) -> Optional[KeySpecRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        if dataset_version_id:
            row = await get_key_spec_for_scope(
                registry,
                conn,
                dataset_id=dataset_id,
                dataset_version_id=dataset_version_id,
            )
            if row:
                return registry._row_to_key_spec(row)
        row = await get_key_spec_for_scope(
            registry,
            conn,
            dataset_id=dataset_id,
        )
        if not row:
            return None
        return registry._row_to_key_spec(row)


async def list_key_specs(
    registry: "DatasetRegistry",
    *,
    dataset_id: Optional[str] = None,
    limit: int = 200,
) -> List[KeySpecRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT key_spec_id, dataset_id, dataset_version_id, spec, status, created_at, updated_at
            FROM {registry._schema}.key_specs
            WHERE ($1::uuid IS NULL OR dataset_id = $1::uuid)
            ORDER BY created_at DESC
            LIMIT $2
            """,
            dataset_id,
            limit,
        )
        return [registry._row_to_key_spec(row) for row in rows]


async def upsert_gate_policy(
    registry: "DatasetRegistry",
    *,
    scope: str,
    name: str,
    description: Optional[str] = None,
    rules: Optional[Dict[str, Any]] = None,
    status: str = "ACTIVE",
) -> GatePolicyRecord:
    pool = _require_pool(registry)
    rules_payload = normalize_json_payload(rules or {})
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {registry._schema}.gate_policies (
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
        return registry._row_to_gate_policy(row)


async def get_gate_policy(
    registry: "DatasetRegistry",
    *,
    scope: str,
    name: str,
) -> Optional[GatePolicyRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT policy_id, scope, name, description, rules, status, created_at, updated_at
            FROM {registry._schema}.gate_policies
            WHERE scope = $1 AND name = $2
            """,
            scope,
            name,
        )
        if not row:
            return None
        return registry._row_to_gate_policy(row)


async def list_gate_policies(
    registry: "DatasetRegistry",
    *,
    scope: Optional[str] = None,
    limit: int = 200,
) -> List[GatePolicyRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT policy_id, scope, name, description, rules, status, created_at, updated_at
            FROM {registry._schema}.gate_policies
            WHERE ($1::text IS NULL OR scope = $1)
            ORDER BY created_at DESC
            LIMIT $2
            """,
            scope,
            limit,
        )
        return [registry._row_to_gate_policy(row) for row in rows]


async def record_gate_result(
    registry: "DatasetRegistry",
    *,
    scope: str,
    subject_type: str,
    subject_id: str,
    status: str,
    details: Optional[Dict[str, Any]] = None,
    policy_name: str = "default",
) -> GateResultRecord:
    pool = _require_pool(registry)
    policy = await get_gate_policy(registry, scope=scope, name=policy_name)
    if not policy:
        policy = await upsert_gate_policy(
            registry,
            scope=scope,
            name=policy_name,
            description=f"Default gate policy for {scope}",
            rules={},
            status="ACTIVE",
        )
    payload = normalize_json_payload(details or {})
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {registry._schema}.gate_results (
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
        return registry._row_to_gate_result(row)


async def list_gate_results(
    registry: "DatasetRegistry",
    *,
    scope: Optional[str] = None,
    subject_type: Optional[str] = None,
    subject_id: Optional[str] = None,
    limit: int = 200,
) -> List[GateResultRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT result_id, policy_id, scope, subject_type, subject_id, status, details, created_at
            FROM {registry._schema}.gate_results
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
        return [registry._row_to_gate_result(row) for row in rows]


async def upsert_access_policy(
    registry: "DatasetRegistry",
    *,
    db_name: str,
    scope: str,
    subject_type: str,
    subject_id: str,
    policy: Optional[Dict[str, Any]] = None,
    status: str = "ACTIVE",
) -> AccessPolicyRecord:
    pool = _require_pool(registry)
    policy_payload = normalize_json_payload(policy or {})
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {registry._schema}.access_policies (
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
        return registry._row_to_access_policy(row)


async def get_access_policy(
    registry: "DatasetRegistry",
    *,
    db_name: str,
    scope: str,
    subject_type: str,
    subject_id: str,
    status: Optional[str] = "ACTIVE",
) -> Optional[AccessPolicyRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT policy_id, db_name, scope, subject_type, subject_id, policy, status, created_at, updated_at
            FROM {registry._schema}.access_policies
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
        return registry._row_to_access_policy(row)


async def list_access_policies(
    registry: "DatasetRegistry",
    *,
    db_name: Optional[str] = None,
    scope: Optional[str] = None,
    subject_type: Optional[str] = None,
    subject_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 200,
) -> list[AccessPolicyRecord]:
    pool = _require_pool(registry)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT policy_id, db_name, scope, subject_type, subject_id, policy, status, created_at, updated_at
            FROM {registry._schema}.access_policies
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
        return [registry._row_to_access_policy(row) for row in rows]
