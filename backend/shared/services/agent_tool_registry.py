"""
Agent tool allowlist registry (Postgres).

Stores per-tool policies used to validate planner output before execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
from typing import Any, Dict, List, Optional

import asyncpg

from shared.config.settings import get_settings
from shared.utils.json_utils import normalize_json_payload


def _coerce_json_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            return []
    return []


def _coerce_json_dict(value: Any) -> Dict[str, Any]:
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
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
        return {"value": parsed}
    return {}


@dataclass(frozen=True)
class AgentToolPolicyRecord:
    tool_id: str
    method: str
    path: str
    risk_level: str
    requires_approval: bool
    requires_idempotency_key: bool
    status: str
    roles: List[str]
    max_payload_bytes: Optional[int]
    created_at: datetime
    updated_at: datetime
    version: str = "v1"
    tool_type: str = "unknown"
    input_schema: Dict[str, Any] = field(default_factory=dict)
    output_schema: Dict[str, Any] = field(default_factory=dict)
    timeout_seconds: Optional[float] = None
    retry_policy: Dict[str, Any] = field(default_factory=dict)
    resource_scopes: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class AgentToolRegistry:
    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_agent",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
    ) -> None:
        self._dsn = dsn or get_settings().database.postgres_url
        self._schema = schema
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_min = int(pool_min or 1)
        self._pool_max = int(pool_max or 5)

    async def initialize(self) -> None:
        await self.connect()

    async def connect(self) -> None:
        if self._pool:
            return
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._pool_min,
            max_size=self._pool_max,
            command_timeout=30,
        )
        await self.ensure_schema()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def shutdown(self) -> None:
        await self.close()

    async def ensure_schema(self) -> None:
        if not self._pool:
            raise RuntimeError("AgentToolRegistry not connected")

        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_tool_policies (
                    tool_id TEXT PRIMARY KEY,
                    method TEXT NOT NULL,
                    path TEXT NOT NULL,
                    risk_level TEXT NOT NULL DEFAULT 'read',
                    requires_approval BOOLEAN NOT NULL DEFAULT false,
                    requires_idempotency_key BOOLEAN NOT NULL DEFAULT false,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    roles JSONB NOT NULL DEFAULT '[]'::jsonb,
                    max_payload_bytes INTEGER,
                    version TEXT NOT NULL DEFAULT 'v1',
                    tool_type TEXT NOT NULL DEFAULT 'unknown',
                    input_schema JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    output_schema JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    timeout_seconds DOUBLE PRECISION,
                    retry_policy JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    resource_scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS version TEXT NOT NULL DEFAULT 'v1'"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS tool_type TEXT NOT NULL DEFAULT 'unknown'"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS input_schema JSONB NOT NULL DEFAULT '{{}}'::jsonb"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS output_schema JSONB NOT NULL DEFAULT '{{}}'::jsonb"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS timeout_seconds DOUBLE PRECISION"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS retry_policy JSONB NOT NULL DEFAULT '{{}}'::jsonb"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS resource_scopes JSONB NOT NULL DEFAULT '[]'::jsonb"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb"
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_agent_tool_policies_status
                ON {self._schema}.agent_tool_policies(status)
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_tool_policies_type ON {self._schema}.agent_tool_policies(tool_type)"
            )

    def _row_to_policy(self, row: asyncpg.Record) -> AgentToolPolicyRecord:
        return AgentToolPolicyRecord(
            tool_id=str(row["tool_id"]),
            method=str(row["method"]),
            path=str(row["path"]),
            risk_level=str(row["risk_level"]),
            requires_approval=bool(row["requires_approval"]),
            requires_idempotency_key=bool(row["requires_idempotency_key"]),
            status=str(row["status"]),
            roles=[str(role) for role in _coerce_json_list(row["roles"]) if role],
            max_payload_bytes=row["max_payload_bytes"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            version=str(row.get("version") or "v1"),
            tool_type=str(row.get("tool_type") or "unknown"),
            input_schema=_coerce_json_dict(row.get("input_schema")),
            output_schema=_coerce_json_dict(row.get("output_schema")),
            timeout_seconds=float(row["timeout_seconds"]) if row.get("timeout_seconds") is not None else None,
            retry_policy=_coerce_json_dict(row.get("retry_policy")),
            resource_scopes=[str(scope) for scope in _coerce_json_list(row.get("resource_scopes")) if str(scope).strip()],
            metadata=_coerce_json_dict(row.get("metadata")),
        )

    async def upsert_tool_policy(
        self,
        *,
        tool_id: str,
        method: str,
        path: str,
        risk_level: str,
        requires_approval: bool = False,
        requires_idempotency_key: bool = False,
        status: str = "ACTIVE",
        roles: Optional[List[str]] = None,
        max_payload_bytes: Optional[int] = None,
        version: str = "v1",
        tool_type: str = "unknown",
        input_schema: Optional[Dict[str, Any]] = None,
        output_schema: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[float] = None,
        retry_policy: Optional[Dict[str, Any]] = None,
        resource_scopes: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AgentToolPolicyRecord:
        if not self._pool:
            raise RuntimeError("AgentToolRegistry not connected")
        roles_payload = normalize_json_payload(roles or [])
        input_payload = normalize_json_payload(input_schema or {})
        output_payload = normalize_json_payload(output_schema or {})
        retry_payload = normalize_json_payload(retry_policy or {})
        scopes_payload = normalize_json_payload(resource_scopes or [])
        metadata_payload = normalize_json_payload(metadata or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_tool_policies (
                    tool_id, method, path, risk_level, requires_approval, requires_idempotency_key,
                    status, roles, max_payload_bytes,
                    version, tool_type, input_schema, output_schema, timeout_seconds, retry_policy, resource_scopes, metadata
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9,
                        $10, $11, $12::jsonb, $13::jsonb, $14, $15::jsonb, $16::jsonb, $17::jsonb)
                ON CONFLICT (tool_id) DO UPDATE SET
                    method = EXCLUDED.method,
                    path = EXCLUDED.path,
                    risk_level = EXCLUDED.risk_level,
                    requires_approval = EXCLUDED.requires_approval,
                    requires_idempotency_key = EXCLUDED.requires_idempotency_key,
                    status = EXCLUDED.status,
                    roles = EXCLUDED.roles,
                    max_payload_bytes = EXCLUDED.max_payload_bytes,
                    version = EXCLUDED.version,
                    tool_type = EXCLUDED.tool_type,
                    input_schema = EXCLUDED.input_schema,
                    output_schema = EXCLUDED.output_schema,
                    timeout_seconds = EXCLUDED.timeout_seconds,
                    retry_policy = EXCLUDED.retry_policy,
                    resource_scopes = EXCLUDED.resource_scopes,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                RETURNING tool_id, method, path, risk_level, requires_approval, requires_idempotency_key,
                          status, roles, max_payload_bytes, created_at, updated_at,
                          version, tool_type, input_schema, output_schema, timeout_seconds, retry_policy, resource_scopes, metadata
                """,
                tool_id,
                method,
                path,
                risk_level,
                requires_approval,
                requires_idempotency_key,
                status,
                roles_payload,
                max_payload_bytes,
                str(version or "v1"),
                str(tool_type or "unknown"),
                input_payload,
                output_payload,
                float(timeout_seconds) if timeout_seconds is not None else None,
                retry_payload,
                scopes_payload,
                metadata_payload,
            )
        return self._row_to_policy(row)

    async def get_tool_policy(self, *, tool_id: str) -> Optional[AgentToolPolicyRecord]:
        if not self._pool:
            raise RuntimeError("AgentToolRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT tool_id, method, path, risk_level, requires_approval, requires_idempotency_key,
                       status, roles, max_payload_bytes, created_at, updated_at,
                       version, tool_type, input_schema, output_schema, timeout_seconds, retry_policy, resource_scopes, metadata
                FROM {self._schema}.agent_tool_policies
                WHERE tool_id = $1
                """,
                tool_id,
            )
        return self._row_to_policy(row) if row else None

    async def list_tool_policies(
        self,
        *,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[AgentToolPolicyRecord]:
        if not self._pool:
            raise RuntimeError("AgentToolRegistry not connected")
        clauses = []
        values: List[Any] = []
        if status:
            values.append(status)
            clauses.append(f"status = ${len(values)}")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        values.append(limit)
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT tool_id, method, path, risk_level, requires_approval, requires_idempotency_key,
                       status, roles, max_payload_bytes, created_at, updated_at,
                       version, tool_type, input_schema, output_schema, timeout_seconds, retry_policy, resource_scopes, metadata
                FROM {self._schema}.agent_tool_policies
                {where}
                ORDER BY tool_id ASC
                LIMIT ${len(values)}
                """,
                *values,
            )
        return [self._row_to_policy(row) for row in rows]
