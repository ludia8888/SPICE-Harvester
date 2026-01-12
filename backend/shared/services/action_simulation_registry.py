"""
Action simulation registry (Postgres).

This is a control-plane store for Decision Simulation (what-if) runs for Action writeback.
It is intentionally separate from ActionLog:
- ActionLog is the durable audit record of executed decisions.
- ActionSimulation is a versioned preview artifact that never mutates lakeFS/ES/EventStore.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg

from shared.config.service_config import ServiceConfig
from shared.utils.json_utils import coerce_json_dataset, normalize_json_payload


def _coerce_json_list(value: Any) -> List[Dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        # Some callers may accidentally store {"value": [...]}.
        nested = value.get("value")
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, dict)]
        return []
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            import json

            parsed = json.loads(raw)
        except Exception:
            return []
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
        if isinstance(parsed, dict):
            nested = parsed.get("value")
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
        return []
    return []


@dataclass(frozen=True)
class ActionSimulationRecord:
    simulation_id: str
    db_name: str
    action_type_id: str
    title: Optional[str]
    description: Optional[str]
    created_by: str
    created_by_type: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ActionSimulationVersionRecord:
    simulation_id: str
    version: int
    status: str
    base_branch: str
    overlay_branch: str
    ontology_commit_id: Optional[str]
    action_type_rid: Optional[str]
    preview_action_log_id: Optional[str]
    input: Dict[str, Any]
    scenarios: List[Dict[str, Any]]
    result: Optional[Dict[str, Any]]
    error: Optional[Dict[str, Any]]
    created_by: str
    created_by_type: str
    created_at: datetime


class ActionSimulationRegistry:
    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_action_simulations",
        pool_min: int = 1,
        pool_max: int = 5,
    ) -> None:
        self._dsn = dsn or ServiceConfig.get_postgres_url()
        self._schema = schema
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_min = int(pool_min or 1)
        self._pool_max = int(pool_max or 5)

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

    async def initialize(self) -> None:
        await self.connect()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def shutdown(self) -> None:
        await self.close()

    async def ensure_schema(self) -> None:
        if not self._pool:
            raise RuntimeError("ActionSimulationRegistry not connected")

        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.action_simulations (
                    simulation_id UUID PRIMARY KEY,
                    db_name TEXT NOT NULL,
                    action_type_id TEXT NOT NULL,
                    title TEXT,
                    description TEXT,
                    created_by TEXT NOT NULL,
                    created_by_type TEXT NOT NULL DEFAULT 'user',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_action_simulations_db ON {self._schema}.action_simulations(db_name, created_at DESC)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_action_simulations_action_type ON {self._schema}.action_simulations(db_name, action_type_id, created_at DESC)"
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.action_simulation_versions (
                    simulation_id UUID NOT NULL
                        REFERENCES {self._schema}.action_simulations(simulation_id)
                        ON DELETE CASCADE,
                    version INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    base_branch TEXT NOT NULL DEFAULT 'main',
                    overlay_branch TEXT NOT NULL DEFAULT 'main',
                    ontology_commit_id TEXT,
                    action_type_rid TEXT,
                    preview_action_log_id TEXT,
                    input JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    scenarios JSONB NOT NULL DEFAULT '[]'::jsonb,
                    result JSONB,
                    error JSONB,
                    created_by TEXT NOT NULL,
                    created_by_type TEXT NOT NULL DEFAULT 'user',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (simulation_id, version)
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_action_sim_versions_sim ON {self._schema}.action_simulation_versions(simulation_id, version DESC)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_action_sim_versions_status ON {self._schema}.action_simulation_versions(status, created_at DESC)"
            )

    def _row_to_simulation(self, row: asyncpg.Record) -> ActionSimulationRecord:
        return ActionSimulationRecord(
            simulation_id=str(row["simulation_id"]),
            db_name=str(row["db_name"]),
            action_type_id=str(row["action_type_id"]),
            title=row.get("title"),
            description=row.get("description"),
            created_by=str(row["created_by"]),
            created_by_type=str(row["created_by_type"] or "user"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_version(self, row: asyncpg.Record) -> ActionSimulationVersionRecord:
        return ActionSimulationVersionRecord(
            simulation_id=str(row["simulation_id"]),
            version=int(row["version"]),
            status=str(row["status"]),
            base_branch=str(row["base_branch"]),
            overlay_branch=str(row["overlay_branch"]),
            ontology_commit_id=row.get("ontology_commit_id"),
            action_type_rid=row.get("action_type_rid"),
            preview_action_log_id=row.get("preview_action_log_id"),
            input=coerce_json_dataset(row.get("input")),
            scenarios=_coerce_json_list(row.get("scenarios")),
            result=coerce_json_dataset(row.get("result")) if row.get("result") is not None else None,
            error=coerce_json_dataset(row.get("error")) if row.get("error") is not None else None,
            created_by=str(row["created_by"]),
            created_by_type=str(row["created_by_type"] or "user"),
            created_at=row["created_at"],
        )

    async def create_simulation(
        self,
        *,
        simulation_id: str,
        db_name: str,
        action_type_id: str,
        created_by: str,
        created_by_type: str = "user",
        title: Optional[str] = None,
        description: Optional[str] = None,
    ) -> ActionSimulationRecord:
        if not self._pool:
            raise RuntimeError("ActionSimulationRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.action_simulations (
                    simulation_id, db_name, action_type_id, title, description,
                    created_by, created_by_type
                )
                VALUES ($1::uuid, $2, $3, $4, $5, $6, $7)
                RETURNING simulation_id, db_name, action_type_id, title, description,
                          created_by, created_by_type, created_at, updated_at
                """,
                simulation_id,
                db_name,
                action_type_id,
                title,
                description,
                created_by,
                created_by_type,
            )
        return self._row_to_simulation(row)

    async def get_simulation(self, *, simulation_id: str) -> Optional[ActionSimulationRecord]:
        if not self._pool:
            raise RuntimeError("ActionSimulationRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT simulation_id, db_name, action_type_id, title, description,
                       created_by, created_by_type, created_at, updated_at
                FROM {self._schema}.action_simulations
                WHERE simulation_id = $1::uuid
                """,
                simulation_id,
            )
        if not row:
            return None
        return self._row_to_simulation(row)

    async def list_simulations(
        self,
        *,
        db_name: str,
        action_type_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[ActionSimulationRecord]:
        if not self._pool:
            raise RuntimeError("ActionSimulationRegistry not connected")
        limit = max(1, min(int(limit), 200))
        offset = max(0, int(offset))
        async with self._pool.acquire() as conn:
            if action_type_id:
                rows = await conn.fetch(
                    f"""
                    SELECT simulation_id, db_name, action_type_id, title, description,
                           created_by, created_by_type, created_at, updated_at
                    FROM {self._schema}.action_simulations
                    WHERE db_name = $1 AND action_type_id = $2
                    ORDER BY created_at DESC
                    LIMIT $3 OFFSET $4
                    """,
                    db_name,
                    action_type_id,
                    limit,
                    offset,
                )
            else:
                rows = await conn.fetch(
                    f"""
                    SELECT simulation_id, db_name, action_type_id, title, description,
                           created_by, created_by_type, created_at, updated_at
                    FROM {self._schema}.action_simulations
                    WHERE db_name = $1
                    ORDER BY created_at DESC
                    LIMIT $2 OFFSET $3
                    """,
                    db_name,
                    limit,
                    offset,
                )
        return [self._row_to_simulation(row) for row in rows]

    async def next_version(self, *, simulation_id: str) -> int:
        if not self._pool:
            raise RuntimeError("ActionSimulationRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT COALESCE(MAX(version), 0) AS max_version FROM {self._schema}.action_simulation_versions WHERE simulation_id = $1::uuid",
                simulation_id,
            )
        max_version = int(row["max_version"]) if row and row.get("max_version") is not None else 0
        return max_version + 1

    async def create_version(
        self,
        *,
        simulation_id: str,
        version: int,
        status: str,
        base_branch: str,
        overlay_branch: str,
        ontology_commit_id: Optional[str],
        action_type_rid: Optional[str],
        preview_action_log_id: Optional[str],
        input_payload: Dict[str, Any],
        scenarios: List[Dict[str, Any]],
        result: Optional[Dict[str, Any]],
        error: Optional[Dict[str, Any]],
        created_by: str,
        created_by_type: str = "user",
    ) -> ActionSimulationVersionRecord:
        if not self._pool:
            raise RuntimeError("ActionSimulationRegistry not connected")
        payload_input = normalize_json_payload(input_payload)
        payload_scenarios = normalize_json_payload(scenarios)
        payload_result = normalize_json_payload(result) if result is not None else None
        payload_error = normalize_json_payload(error) if error is not None else None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.action_simulation_versions (
                    simulation_id, version, status, base_branch, overlay_branch,
                    ontology_commit_id, action_type_rid, preview_action_log_id,
                    input, scenarios, result, error,
                    created_by, created_by_type
                )
                VALUES (
                    $1::uuid, $2, $3, $4, $5,
                    $6, $7, $8,
                    $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb,
                    $13, $14
                )
                RETURNING simulation_id, version, status, base_branch, overlay_branch,
                          ontology_commit_id, action_type_rid, preview_action_log_id,
                          input, scenarios, result, error,
                          created_by, created_by_type, created_at
                """,
                simulation_id,
                int(version),
                str(status),
                str(base_branch),
                str(overlay_branch),
                ontology_commit_id,
                action_type_rid,
                preview_action_log_id,
                payload_input,
                payload_scenarios,
                payload_result,
                payload_error,
                created_by,
                created_by_type,
            )
            await conn.execute(
                f"UPDATE {self._schema}.action_simulations SET updated_at = NOW() WHERE simulation_id = $1::uuid",
                simulation_id,
            )
        return self._row_to_version(row)

    async def list_versions(self, *, simulation_id: str, limit: int = 50, offset: int = 0) -> List[ActionSimulationVersionRecord]:
        if not self._pool:
            raise RuntimeError("ActionSimulationRegistry not connected")
        limit = max(1, min(int(limit), 200))
        offset = max(0, int(offset))
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT simulation_id, version, status, base_branch, overlay_branch,
                       ontology_commit_id, action_type_rid, preview_action_log_id,
                       input, scenarios, result, error,
                       created_by, created_by_type, created_at
                FROM {self._schema}.action_simulation_versions
                WHERE simulation_id = $1::uuid
                ORDER BY version DESC
                LIMIT $2 OFFSET $3
                """,
                simulation_id,
                limit,
                offset,
            )
        return [self._row_to_version(row) for row in rows]

    async def get_version(self, *, simulation_id: str, version: int) -> Optional[ActionSimulationVersionRecord]:
        if not self._pool:
            raise RuntimeError("ActionSimulationRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT simulation_id, version, status, base_branch, overlay_branch,
                       ontology_commit_id, action_type_rid, preview_action_log_id,
                       input, scenarios, result, error,
                       created_by, created_by_type, created_at
                FROM {self._schema}.action_simulation_versions
                WHERE simulation_id = $1::uuid AND version = $2
                """,
                simulation_id,
                int(version),
            )
        if not row:
            return None
        return self._row_to_version(row)
