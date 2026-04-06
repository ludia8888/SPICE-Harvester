"""
Agent function registry (Postgres).

This provides TOOL-005 "Function tool" support:
- register a function (id + version/tag + handler)
- execute a registered function safely via an allowlisted endpoint

Security posture:
- functions are resolved by allowlist + registry status
- handlers are from a built-in dispatch table (no arbitrary code execution)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg

from shared.services.registries.postgres_schema_registry import PostgresSchemaRegistry
from shared.utils.json_utils import coerce_json_dict, coerce_json_list, normalize_json_payload


@dataclass(frozen=True)
class AgentFunctionRecord:
    function_id: str
    version: str
    status: str
    handler: str
    tags: List[str]
    roles: List[str]
    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any]
    metadata: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


class AgentFunctionRegistry(PostgresSchemaRegistry):
    _REQUIRED_TABLES = ("agent_functions",)

    async def _ensure_tables(self, conn: asyncpg.Connection) -> None:  # type: ignore[override]
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.agent_functions (
                function_id TEXT NOT NULL,
                version TEXT NOT NULL DEFAULT 'v1',
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                handler TEXT NOT NULL,
                tags JSONB NOT NULL DEFAULT '[]'::jsonb,
                roles JSONB NOT NULL DEFAULT '[]'::jsonb,
                input_schema JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                output_schema JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (function_id, version)
            )
            """
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_agent_functions_status ON {self._schema}.agent_functions(status)"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_agent_functions_function_id ON {self._schema}.agent_functions(function_id)"
        )

    def _row_to_record(self, row: asyncpg.Record) -> AgentFunctionRecord:
        return AgentFunctionRecord(
            function_id=str(row["function_id"]),
            version=str(row["version"]),
            status=str(row["status"]),
            handler=str(row["handler"]),
            tags=[str(t).strip() for t in coerce_json_list(row.get("tags")) if str(t).strip()],
            roles=[str(r).strip() for r in coerce_json_list(row.get("roles")) if str(r).strip()],
            input_schema=coerce_json_dict(row.get("input_schema")),
            output_schema=coerce_json_dict(row.get("output_schema")),
            metadata=coerce_json_dict(row.get("metadata")),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def upsert_function(
        self,
        *,
        function_id: str,
        version: str = "v1",
        status: str = "ACTIVE",
        handler: str,
        tags: Optional[List[str]] = None,
        roles: Optional[List[str]] = None,
        input_schema: Optional[Dict[str, Any]] = None,
        output_schema: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AgentFunctionRecord:
        if not self._pool:
            raise RuntimeError("AgentFunctionRegistry not connected")
        fid = str(function_id or "").strip()
        if not fid:
            raise ValueError("function_id is required")
        ver = str(version or "v1").strip() or "v1"
        handler_value = str(handler or "").strip()
        if not handler_value:
            raise ValueError("handler is required")
        status_value = str(status or "ACTIVE").strip().upper() or "ACTIVE"
        tags_payload = normalize_json_payload([str(t).strip() for t in (tags or []) if str(t).strip()])
        roles_payload = normalize_json_payload([str(r).strip() for r in (roles or []) if str(r).strip()])
        input_payload = normalize_json_payload(input_schema or {})
        output_payload = normalize_json_payload(output_schema or {})
        meta_payload = normalize_json_payload(metadata or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_functions (
                    function_id, version, status, handler,
                    tags, roles, input_schema, output_schema, metadata
                )
                VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb)
                ON CONFLICT (function_id, version) DO UPDATE SET
                    status = EXCLUDED.status,
                    handler = EXCLUDED.handler,
                    tags = EXCLUDED.tags,
                    roles = EXCLUDED.roles,
                    input_schema = EXCLUDED.input_schema,
                    output_schema = EXCLUDED.output_schema,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                RETURNING function_id, version, status, handler, tags, roles,
                          input_schema, output_schema, metadata, created_at, updated_at
                """,
                fid,
                ver,
                status_value,
                handler_value,
                tags_payload,
                roles_payload,
                input_payload,
                output_payload,
                meta_payload,
            )
        return self._row_to_record(row)

    async def get_function(
        self,
        *,
        function_id: str,
        version: str,
        status: Optional[str] = None,
    ) -> Optional[AgentFunctionRecord]:
        if not self._pool:
            raise RuntimeError("AgentFunctionRegistry not connected")
        fid = str(function_id or "").strip()
        ver = str(version or "").strip()
        if not fid or not ver:
            return None
        clauses: list[str] = ["function_id = $1", "version = $2"]
        values: list[Any] = [fid, ver]
        if status:
            values.append(str(status).strip().upper())
            clauses.append(f"status = ${len(values)}")
        where = " AND ".join(clauses)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT function_id, version, status, handler, tags, roles,
                       input_schema, output_schema, metadata, created_at, updated_at
                FROM {self._schema}.agent_functions
                WHERE {where}
                """,
                *values,
            )
        return self._row_to_record(row) if row else None

    async def list_functions(
        self,
        *,
        function_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> List[AgentFunctionRecord]:
        if not self._pool:
            raise RuntimeError("AgentFunctionRegistry not connected")
        clauses: list[str] = []
        values: list[Any] = []
        if function_id:
            values.append(str(function_id).strip())
            clauses.append(f"function_id = ${len(values)}")
        if status:
            values.append(str(status).strip().upper())
            clauses.append(f"status = ${len(values)}")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        values.extend([int(limit), int(offset)])
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT function_id, version, status, handler, tags, roles,
                       input_schema, output_schema, metadata, created_at, updated_at
                FROM {self._schema}.agent_functions
                {where}
                ORDER BY function_id ASC, version DESC
                LIMIT ${len(values) - 1}
                OFFSET ${len(values)}
                """,
                *values,
            )
        return [self._row_to_record(row) for row in rows]
