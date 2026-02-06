"""
Tenant-scoped agent policy registry (Postgres).

This is the central SSoT for enterprise policy enforcement (AUTH-005):
- allowed model list (LLM allowlist)
- allowed tool list (tool enablement default/guard)
- auto-approval rules (scaffold; enforced by planners/executors)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg

from shared.services.registries.postgres_schema_registry import PostgresSchemaRegistry
from shared.utils.json_utils import coerce_json_list, normalize_json_payload


@dataclass(frozen=True)
class AgentTenantPolicyRecord:
    tenant_id: str
    allowed_models: List[str]
    allowed_tools: List[str]
    default_model: Optional[str]
    auto_approve_rules: Dict[str, Any]
    data_policies: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


class AgentPolicyRegistry(PostgresSchemaRegistry):
    async def _ensure_tables(self, conn: asyncpg.Connection) -> None:  # type: ignore[override]
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.agent_tenant_policies (
                tenant_id TEXT PRIMARY KEY,
                allowed_models JSONB NOT NULL DEFAULT '[]'::jsonb,
                allowed_tools JSONB NOT NULL DEFAULT '[]'::jsonb,
                default_model TEXT,
                auto_approve_rules JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                data_policies JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_agent_tenant_policies_updated_at ON {self._schema}.agent_tenant_policies(updated_at)"
        )

    def _row_to_policy(self, row: asyncpg.Record) -> AgentTenantPolicyRecord:
        return AgentTenantPolicyRecord(
            tenant_id=str(row["tenant_id"]),
            allowed_models=[str(item) for item in coerce_json_list(row.get("allowed_models")) if str(item).strip()],
            allowed_tools=[str(item) for item in coerce_json_list(row.get("allowed_tools")) if str(item).strip()],
            default_model=row.get("default_model"),
            auto_approve_rules=dict(row.get("auto_approve_rules") or {}),
            data_policies=dict(row.get("data_policies") or {}),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def upsert_tenant_policy(
        self,
        *,
        tenant_id: str,
        allowed_models: Optional[List[str]] = None,
        allowed_tools: Optional[List[str]] = None,
        default_model: Optional[str] = None,
        auto_approve_rules: Optional[Dict[str, Any]] = None,
        data_policies: Optional[Dict[str, Any]] = None,
    ) -> AgentTenantPolicyRecord:
        if not self._pool:
            raise RuntimeError("AgentPolicyRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        models_payload = normalize_json_payload([m for m in (allowed_models or []) if str(m).strip()])
        tools_payload = normalize_json_payload([t for t in (allowed_tools or []) if str(t).strip()])
        auto_payload = normalize_json_payload(auto_approve_rules or {})
        data_payload = normalize_json_payload(data_policies or {})
        default_model_value = str(default_model).strip() if default_model is not None else None
        if default_model_value == "":
            default_model_value = None

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_tenant_policies (
                    tenant_id, allowed_models, allowed_tools, default_model,
                    auto_approve_rules, data_policies
                )
                VALUES ($1, $2::jsonb, $3::jsonb, $4, $5::jsonb, $6::jsonb)
                ON CONFLICT (tenant_id) DO UPDATE SET
                    allowed_models = EXCLUDED.allowed_models,
                    allowed_tools = EXCLUDED.allowed_tools,
                    default_model = EXCLUDED.default_model,
                    auto_approve_rules = EXCLUDED.auto_approve_rules,
                    data_policies = EXCLUDED.data_policies,
                    updated_at = NOW()
                RETURNING tenant_id, allowed_models, allowed_tools, default_model,
                          auto_approve_rules, data_policies, created_at, updated_at
                """,
                tenant_value,
                models_payload,
                tools_payload,
                default_model_value,
                auto_payload,
                data_payload,
            )
        return self._row_to_policy(row)

    async def get_tenant_policy(self, *, tenant_id: str) -> Optional[AgentTenantPolicyRecord]:
        if not self._pool:
            raise RuntimeError("AgentPolicyRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT tenant_id, allowed_models, allowed_tools, default_model,
                       auto_approve_rules, data_policies, created_at, updated_at
                FROM {self._schema}.agent_tenant_policies
                WHERE tenant_id = $1
                """,
                tenant_value,
            )
        return self._row_to_policy(row) if row else None

    async def list_tenant_policies(self, *, limit: int = 200, offset: int = 0) -> List[AgentTenantPolicyRecord]:
        if not self._pool:
            raise RuntimeError("AgentPolicyRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT tenant_id, allowed_models, allowed_tools, default_model,
                       auto_approve_rules, data_policies, created_at, updated_at
                FROM {self._schema}.agent_tenant_policies
                ORDER BY tenant_id ASC
                LIMIT $1
                OFFSET $2
                """,
                int(limit),
                int(offset),
            )
        return [self._row_to_policy(row) for row in rows]
