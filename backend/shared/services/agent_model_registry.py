"""
Global agent model registry (Postgres).

Stores model metadata/capabilities used for:
- Model allowlist UX (admin can manage without deploy)
- Capability-aware fallback (e.g., native tool calling → prompt-based)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, Dict, List, Optional

import asyncpg

from shared.config.service_config import ServiceConfig
from shared.utils.json_utils import normalize_json_payload


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
        except Exception:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


@dataclass(frozen=True)
class AgentModelRecord:
    model_id: str
    provider: str
    display_name: Optional[str]
    status: str
    supports_json_mode: bool
    supports_native_tool_calling: bool
    max_context_tokens: Optional[int]
    max_output_tokens: Optional[int]
    prompt_per_1k: Optional[float]
    completion_per_1k: Optional[float]
    metadata: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


class AgentModelRegistry:
    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_agent",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
    ) -> None:
        self._dsn = dsn or ServiceConfig.get_postgres_url()
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
            raise RuntimeError("AgentModelRegistry not connected")
        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_models (
                    model_id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    display_name TEXT,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    supports_json_mode BOOLEAN NOT NULL DEFAULT true,
                    supports_native_tool_calling BOOLEAN NOT NULL DEFAULT false,
                    max_context_tokens INTEGER,
                    max_output_tokens INTEGER,
                    prompt_per_1k DOUBLE PRECISION,
                    completion_per_1k DOUBLE PRECISION,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_agent_models_status
                ON {self._schema}.agent_models(status)
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_agent_models_provider
                ON {self._schema}.agent_models(provider)
                """
            )

    def _row_to_model(self, row: asyncpg.Record) -> AgentModelRecord:
        return AgentModelRecord(
            model_id=str(row["model_id"]),
            provider=str(row["provider"]),
            display_name=row.get("display_name"),
            status=str(row["status"]),
            supports_json_mode=bool(row["supports_json_mode"]),
            supports_native_tool_calling=bool(row["supports_native_tool_calling"]),
            max_context_tokens=row.get("max_context_tokens"),
            max_output_tokens=row.get("max_output_tokens"),
            prompt_per_1k=row.get("prompt_per_1k"),
            completion_per_1k=row.get("completion_per_1k"),
            metadata=_coerce_json(row.get("metadata")),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def upsert_model(
        self,
        *,
        model_id: str,
        provider: str,
        display_name: Optional[str] = None,
        status: str = "ACTIVE",
        supports_json_mode: bool = True,
        supports_native_tool_calling: bool = False,
        max_context_tokens: Optional[int] = None,
        max_output_tokens: Optional[int] = None,
        prompt_per_1k: Optional[float] = None,
        completion_per_1k: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AgentModelRecord:
        if not self._pool:
            raise RuntimeError("AgentModelRegistry not connected")
        model_value = str(model_id or "").strip()
        if not model_value:
            raise ValueError("model_id is required")
        provider_value = str(provider or "").strip().lower()
        if not provider_value:
            raise ValueError("provider is required")
        display_value = str(display_name).strip() if display_name is not None else None
        if display_value == "":
            display_value = None
        status_value = str(status or "ACTIVE").strip().upper() or "ACTIVE"
        meta_payload = normalize_json_payload(metadata or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_models (
                    model_id, provider, display_name, status,
                    supports_json_mode, supports_native_tool_calling,
                    max_context_tokens, max_output_tokens,
                    prompt_per_1k, completion_per_1k,
                    metadata
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
                ON CONFLICT (model_id) DO UPDATE SET
                    provider = EXCLUDED.provider,
                    display_name = EXCLUDED.display_name,
                    status = EXCLUDED.status,
                    supports_json_mode = EXCLUDED.supports_json_mode,
                    supports_native_tool_calling = EXCLUDED.supports_native_tool_calling,
                    max_context_tokens = EXCLUDED.max_context_tokens,
                    max_output_tokens = EXCLUDED.max_output_tokens,
                    prompt_per_1k = EXCLUDED.prompt_per_1k,
                    completion_per_1k = EXCLUDED.completion_per_1k,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                RETURNING model_id, provider, display_name, status,
                          supports_json_mode, supports_native_tool_calling,
                          max_context_tokens, max_output_tokens,
                          prompt_per_1k, completion_per_1k,
                          metadata, created_at, updated_at
                """,
                model_value,
                provider_value,
                display_value,
                status_value,
                bool(supports_json_mode),
                bool(supports_native_tool_calling),
                max_context_tokens,
                max_output_tokens,
                prompt_per_1k,
                completion_per_1k,
                meta_payload,
            )
        return self._row_to_model(row)

    async def get_model(self, *, model_id: str) -> Optional[AgentModelRecord]:
        if not self._pool:
            raise RuntimeError("AgentModelRegistry not connected")
        model_value = str(model_id or "").strip()
        if not model_value:
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT model_id, provider, display_name, status,
                       supports_json_mode, supports_native_tool_calling,
                       max_context_tokens, max_output_tokens,
                       prompt_per_1k, completion_per_1k,
                       metadata, created_at, updated_at
                FROM {self._schema}.agent_models
                WHERE model_id = $1
                """,
                model_value,
            )
        return self._row_to_model(row) if row else None

    async def list_models(
        self,
        *,
        status: Optional[str] = None,
        provider: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> List[AgentModelRecord]:
        if not self._pool:
            raise RuntimeError("AgentModelRegistry not connected")
        clauses: list[str] = []
        values: list[Any] = []
        if status:
            values.append(str(status).strip().upper())
            clauses.append(f"status = ${len(values)}")
        if provider:
            values.append(str(provider).strip().lower())
            clauses.append(f"provider = ${len(values)}")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        values.extend([int(limit), int(offset)])
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT model_id, provider, display_name, status,
                       supports_json_mode, supports_native_tool_calling,
                       max_context_tokens, max_output_tokens,
                       prompt_per_1k, completion_per_1k,
                       metadata, created_at, updated_at
                FROM {self._schema}.agent_models
                {where}
                ORDER BY model_id ASC
                LIMIT ${len(values)-1}
                OFFSET ${len(values)}
                """,
                *values,
            )
        return [self._row_to_model(row) for row in rows]

