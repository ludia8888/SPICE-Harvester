"""
Connector Registry (Foundry-style) - durable state in Postgres.

This service stores:
- connector sources (type/id + config + enabled)
- connector mappings (human/OMS-confirmed mapping gating auto-import)
- sync state (cursor/hash, last success/failure, retry/backoff)
- update outbox (durable enqueue for Kafka connector-updates)

Design goals:
- At-least-once delivery with deterministic event ids (no missed updates).
- One shared runtime (trigger + sync worker) can scale to many connectors.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import asyncpg

from shared.config.service_config import ServiceConfig
from shared.models.event_envelope import EventEnvelope
from shared.utils.json_utils import coerce_json_strict
from shared.utils.time_utils import utcnow


@dataclass(frozen=True)
class ConnectorSource:
    source_type: str
    source_id: str
    enabled: bool
    config_json: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ConnectorMapping:
    mapping_id: str
    source_type: str
    source_id: str
    status: str
    enabled: bool
    target_db_name: Optional[str]
    target_branch: Optional[str]
    target_class_label: Optional[str]
    field_mappings: List[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class SyncState:
    source_type: str
    source_id: str
    last_seen_cursor: Optional[str]
    last_emitted_seq: int
    last_polled_at: Optional[datetime]
    last_success_at: Optional[datetime]
    last_failure_at: Optional[datetime]
    last_error: Optional[str]
    attempt_count: int
    rate_limit_until: Optional[datetime]
    next_retry_at: Optional[datetime]
    last_command_id: Optional[str]
    updated_at: datetime


@dataclass(frozen=True)
class OutboxItem:
    outbox_id: str
    event_id: str
    source_type: str
    source_id: str
    sequence_number: Optional[int]
    payload: Dict[str, Any]
    status: str
    publish_attempts: int
    created_at: datetime
    published_at: Optional[datetime]
    last_error: Optional[str]


class ConnectorRegistry:
    _MAPPING_NAMESPACE: UUID = uuid5(NAMESPACE_URL, "spice-harvester:connector-mappings")

    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_connectors",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
    ):
        self._dsn = dsn or ServiceConfig.get_postgres_url()
        self._schema = schema
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_min = int(os.getenv("CONNECTOR_REGISTRY_PG_POOL_MIN", str(pool_min or 1)))
        self._pool_max = int(os.getenv("CONNECTOR_REGISTRY_PG_POOL_MAX", str(pool_max or 5)))

    async def initialize(self) -> None:
        await self.connect()

    async def connect(self) -> None:
        if self._pool:
            return
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._pool_min,
            max_size=self._pool_max,
            command_timeout=int(os.getenv("CONNECTOR_REGISTRY_PG_COMMAND_TIMEOUT", "30")),
        )
        await self.ensure_schema()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def ensure_schema(self) -> None:
        if not self._pool:
            raise RuntimeError("ConnectorRegistry not connected")

        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.connector_sources (
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    config_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (source_type, source_id)
                )
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.connector_mappings (
                    mapping_id UUID PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft',
                    enabled BOOLEAN NOT NULL DEFAULT FALSE,
                    target_db_name TEXT,
                    target_branch TEXT,
                    target_class_label TEXT,
                    field_mappings JSONB NOT NULL DEFAULT '[]'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (source_type, source_id),
                    FOREIGN KEY (source_type, source_id)
                        REFERENCES {self._schema}.connector_sources(source_type, source_id)
                        ON DELETE CASCADE
                )
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.connector_sync_state (
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    last_seen_cursor TEXT,
                    last_emitted_seq BIGINT NOT NULL DEFAULT 0,
                    last_polled_at TIMESTAMPTZ,
                    last_success_at TIMESTAMPTZ,
                    last_failure_at TIMESTAMPTZ,
                    last_error TEXT,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    rate_limit_until TIMESTAMPTZ,
                    next_retry_at TIMESTAMPTZ,
                    last_command_id TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (source_type, source_id),
                    FOREIGN KEY (source_type, source_id)
                        REFERENCES {self._schema}.connector_sources(source_type, source_id)
                        ON DELETE CASCADE
                )
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.connector_update_outbox (
                    outbox_id UUID PRIMARY KEY,
                    event_id TEXT NOT NULL UNIQUE,
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    sequence_number BIGINT,
                    payload JSONB NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    publish_attempts INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    published_at TIMESTAMPTZ,
                    last_error TEXT,
                    FOREIGN KEY (source_type, source_id)
                        REFERENCES {self._schema}.connector_sources(source_type, source_id)
                        ON DELETE CASCADE
                )
                """
            )

            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_connector_outbox_status ON {self._schema}.connector_update_outbox(status, created_at)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_connector_sources_enabled ON {self._schema}.connector_sources(enabled)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_connector_sync_next_retry ON {self._schema}.connector_sync_state(next_retry_at)"
            )

    # -----------------
    # Sources / Mapping
    # -----------------

    async def upsert_source(
        self,
        *,
        source_type: str,
        source_id: str,
        config_json: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
    ) -> ConnectorSource:
        if not self._pool:
            raise RuntimeError("ConnectorRegistry not connected")
        st = (source_type or "").strip()
        sid = (source_id or "").strip()
        if not st:
            raise ValueError("source_type is required")
        if not sid:
            raise ValueError("source_id is required")

        config_json = coerce_json_strict(config_json)

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.connector_sources (source_type, source_id, enabled, config_json, created_at, updated_at)
                VALUES ($1, $2, $3, $4::jsonb, NOW(), NOW())
                ON CONFLICT (source_type, source_id) DO UPDATE
                  SET enabled = EXCLUDED.enabled,
                      config_json = EXCLUDED.config_json,
                      updated_at = NOW()
                RETURNING source_type, source_id, enabled, config_json, created_at, updated_at
                """,
                st,
                sid,
                bool(enabled),
                config_json,
            )
            if not row:
                raise RuntimeError("Failed to upsert connector source")
            return ConnectorSource(
                source_type=str(row["source_type"]),
                source_id=str(row["source_id"]),
                enabled=bool(row["enabled"]),
                config_json=dict(row["config_json"] or {}),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def set_source_enabled(self, *, source_type: str, source_id: str, enabled: bool) -> bool:
        if not self._pool:
            raise RuntimeError("ConnectorRegistry not connected")
        st = (source_type or "").strip()
        sid = (source_id or "").strip()
        async with self._pool.acquire() as conn:
            res = await conn.execute(
                f"""
                UPDATE {self._schema}.connector_sources
                SET enabled = $3, updated_at = NOW()
                WHERE source_type = $1 AND source_id = $2
                """,
                st,
                sid,
                bool(enabled),
            )
            return res.upper().startswith("UPDATE ") and not res.endswith(" 0")

    async def get_source(self, *, source_type: str, source_id: str) -> Optional[ConnectorSource]:
        if not self._pool:
            raise RuntimeError("ConnectorRegistry not connected")
        st = (source_type or "").strip()
        sid = (source_id or "").strip()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT source_type, source_id, enabled, config_json, created_at, updated_at
                FROM {self._schema}.connector_sources
                WHERE source_type = $1 AND source_id = $2
                """,
                st,
                sid,
            )
            if not row:
                return None
            return ConnectorSource(
                source_type=str(row["source_type"]),
                source_id=str(row["source_id"]),
                enabled=bool(row["enabled"]),
                config_json=dict(row["config_json"] or {}),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def list_sources(
        self,
        *,
        source_type: Optional[str] = None,
        enabled: Optional[bool] = None,
        limit: int = 500,
    ) -> List[ConnectorSource]:
        if not self._pool:
            raise RuntimeError("ConnectorRegistry not connected")
        clauses: List[str] = []
        args: List[Any] = []
        if source_type:
            clauses.append(f"source_type = ${len(args) + 1}")
            args.append(str(source_type).strip())
        if enabled is not None:
            clauses.append(f"enabled = ${len(args) + 1}")
            args.append(bool(enabled))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = (
            f"SELECT source_type, source_id, enabled, config_json, created_at, updated_at "
            f"FROM {self._schema}.connector_sources {where} "
            f"ORDER BY updated_at DESC LIMIT {max(1, int(limit))}"
        )
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            out: List[ConnectorSource] = []
            for r in rows or []:
                out.append(
                    ConnectorSource(
                        source_type=str(r["source_type"]),
                        source_id=str(r["source_id"]),
                        enabled=bool(r["enabled"]),
                        config_json=dict(r["config_json"] or {}),
                        created_at=r["created_at"],
                        updated_at=r["updated_at"],
                    )
                )
            return out

    def _deterministic_mapping_id(self, *, source_type: str, source_id: str) -> str:
        st = (source_type or "").strip()
        sid = (source_id or "").strip()
        return str(uuid5(self._MAPPING_NAMESPACE, f"{st}:{sid}"))

    async def upsert_mapping(
        self,
        *,
        source_type: str,
        source_id: str,
        enabled: bool,
        status: str = "draft",
        target_db_name: Optional[str] = None,
        target_branch: Optional[str] = None,
        target_class_label: Optional[str] = None,
        field_mappings: Optional[List[Dict[str, Any]]] = None,
    ) -> ConnectorMapping:
        if not self._pool:
            raise RuntimeError("ConnectorRegistry not connected")
        st = (source_type or "").strip()
        sid = (source_id or "").strip()
        if not st:
            raise ValueError("source_type is required")
        if not sid:
            raise ValueError("source_id is required")

        mapping_id = self._deterministic_mapping_id(source_type=st, source_id=sid)
        field_mappings = field_mappings or []
        if not isinstance(field_mappings, list):
            raise TypeError("field_mappings must be a list")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.connector_mappings (
                    mapping_id, source_type, source_id, status, enabled,
                    target_db_name, target_branch, target_class_label, field_mappings, created_at, updated_at
                )
                VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, NOW(), NOW())
                ON CONFLICT (source_type, source_id) DO UPDATE
                  SET status = EXCLUDED.status,
                      enabled = EXCLUDED.enabled,
                      target_db_name = EXCLUDED.target_db_name,
                      target_branch = EXCLUDED.target_branch,
                      target_class_label = EXCLUDED.target_class_label,
                      field_mappings = EXCLUDED.field_mappings,
                      updated_at = NOW()
                RETURNING mapping_id, source_type, source_id, status, enabled, target_db_name, target_branch, target_class_label,
                          field_mappings, created_at, updated_at
                """,
                mapping_id,
                st,
                sid,
                (status or "draft").strip() or "draft",
                bool(enabled),
                (target_db_name or "").strip() or None,
                (target_branch or "").strip() or None,
                (target_class_label or "").strip() or None,
                field_mappings,
            )
            if not row:
                raise RuntimeError("Failed to upsert connector mapping")
            return ConnectorMapping(
                mapping_id=str(row["mapping_id"]),
                source_type=str(row["source_type"]),
                source_id=str(row["source_id"]),
                status=str(row["status"]),
                enabled=bool(row["enabled"]),
                target_db_name=row["target_db_name"],
                target_branch=row["target_branch"],
                target_class_label=row["target_class_label"],
                field_mappings=list(row["field_mappings"] or []),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def get_mapping(self, *, source_type: str, source_id: str) -> Optional[ConnectorMapping]:
        if not self._pool:
            raise RuntimeError("ConnectorRegistry not connected")
        st = (source_type or "").strip()
        sid = (source_id or "").strip()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT mapping_id, source_type, source_id, status, enabled, target_db_name, target_branch,
                       target_class_label, field_mappings, created_at, updated_at
                FROM {self._schema}.connector_mappings
                WHERE source_type = $1 AND source_id = $2
                """,
                st,
                sid,
            )
            if not row:
                return None
            return ConnectorMapping(
                mapping_id=str(row["mapping_id"]),
                source_type=str(row["source_type"]),
                source_id=str(row["source_id"]),
                status=str(row["status"]),
                enabled=bool(row["enabled"]),
                target_db_name=row["target_db_name"],
                target_branch=row["target_branch"],
                target_class_label=row["target_class_label"],
                field_mappings=list(row["field_mappings"] or []),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    # -----------------
    # Change detection
    # -----------------

    async def record_poll_result(
        self,
        *,
        source_type: str,
        source_id: str,
        current_cursor: Optional[str],
        kafka_topic: Optional[str] = None,
    ) -> Optional[EventEnvelope]:
        """
        Record a poll result, and enqueue a connector update event when the cursor changed.

        This is transactionally safe:
        - cursor/state is advanced only together with outbox insert
        - publisher can retry outbox publishing on crash/network failure
        """
        if not self._pool:
            raise RuntimeError("ConnectorRegistry not connected")
        st = (source_type or "").strip()
        sid = (source_id or "").strip()
        now = utcnow()
        cursor_norm = (current_cursor or "").strip() or None

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                state = await conn.fetchrow(
                    f"""
                    SELECT last_seen_cursor, last_emitted_seq
                    FROM {self._schema}.connector_sync_state
                    WHERE source_type = $1 AND source_id = $2
                    FOR UPDATE
                    """,
                    st,
                    sid,
                )
                if not state:
                    await conn.execute(
                        f"""
                        INSERT INTO {self._schema}.connector_sync_state (source_type, source_id, last_seen_cursor, last_emitted_seq, last_polled_at, updated_at)
                        VALUES ($1, $2, NULL, 0, $3, $3)
                        ON CONFLICT (source_type, source_id) DO NOTHING
                        """,
                        st,
                        sid,
                        now,
                    )
                    previous_cursor = None
                    last_emitted_seq = 0
                else:
                    previous_cursor = state["last_seen_cursor"]
                    last_emitted_seq = int(state["last_emitted_seq"] or 0)

                # Always update last_polled_at for observability.
                await conn.execute(
                    f"""
                    UPDATE {self._schema}.connector_sync_state
                    SET last_polled_at = $3, updated_at = $3
                    WHERE source_type = $1 AND source_id = $2
                    """,
                    st,
                    sid,
                    now,
                )

                if cursor_norm and previous_cursor and str(previous_cursor) == cursor_norm:
                    return None
                if cursor_norm is None and previous_cursor is None:
                    return None

                seq = last_emitted_seq + 1
                envelope = EventEnvelope.from_connector_update(
                    source_type=st,
                    source_id=sid,
                    cursor=cursor_norm,
                    previous_cursor=str(previous_cursor) if previous_cursor is not None else None,
                    sequence_number=seq,
                    occurred_at=now,
                    kafka_topic=kafka_topic,
                )

                await conn.execute(
                    f"""
                    UPDATE {self._schema}.connector_sync_state
                    SET last_seen_cursor = $3,
                        last_emitted_seq = $4,
                        updated_at = $5
                    WHERE source_type = $1 AND source_id = $2
                    """,
                    st,
                    sid,
                    cursor_norm,
                    seq,
                    now,
                )

                await conn.execute(
                    f"""
                    INSERT INTO {self._schema}.connector_update_outbox (
                        outbox_id, event_id, source_type, source_id, sequence_number, payload, status, publish_attempts, created_at
                    )
                    VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb, 'pending', 0, $7)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    str(uuid4()),
                    str(envelope.event_id),
                    st,
                    sid,
                    seq,
                    envelope.model_dump(mode="json"),
                    now,
                )

                return envelope

    async def claim_outbox_batch(self, *, limit: int = 50) -> List[OutboxItem]:
        if not self._pool:
            raise RuntimeError("ConnectorRegistry not connected")
        lim = max(1, min(int(limit), 500))
        now = utcnow()
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    f"""
                    SELECT outbox_id, event_id, source_type, source_id, sequence_number, payload,
                           status, publish_attempts, created_at, published_at, last_error
                    FROM {self._schema}.connector_update_outbox
                    WHERE status = 'pending'
                    ORDER BY created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT {lim}
                    """
                )
                if not rows:
                    return []

                outbox_ids = [UUID(str(r["outbox_id"])) for r in rows]
                await conn.execute(
                    f"""
                    UPDATE {self._schema}.connector_update_outbox
                    SET status = 'publishing',
                        publish_attempts = publish_attempts + 1,
                        last_error = NULL
                    WHERE outbox_id = ANY($1::uuid[])
                    """,
                    outbox_ids,
                )

                items: List[OutboxItem] = []
                for r in rows:
                    items.append(
                        OutboxItem(
                            outbox_id=str(r["outbox_id"]),
                            event_id=str(r["event_id"]),
                            source_type=str(r["source_type"]),
                            source_id=str(r["source_id"]),
                            sequence_number=int(r["sequence_number"]) if r["sequence_number"] is not None else None,
                            payload=dict(r["payload"] or {}),
                            status="publishing",
                            publish_attempts=int(r["publish_attempts"] or 0) + 1,
                            created_at=r["created_at"],
                            published_at=r["published_at"],
                            last_error=r["last_error"],
                        )
                    )
                return items

    async def mark_outbox_published(self, *, outbox_id: str) -> None:
        if not self._pool:
            raise RuntimeError("ConnectorRegistry not connected")
        now = utcnow()
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.connector_update_outbox
                SET status = 'published', published_at = $2, last_error = NULL
                WHERE outbox_id = $1::uuid
                """,
                outbox_id,
                now,
            )

    async def mark_outbox_failed(self, *, outbox_id: str, error: str) -> None:
        if not self._pool:
            raise RuntimeError("ConnectorRegistry not connected")
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.connector_update_outbox
                SET status = 'pending', last_error = $2
                WHERE outbox_id = $1::uuid
                """,
                outbox_id,
                (error or "").strip()[:4000] or "unknown error",
            )

    # -----------------
    # Sync status (best-effort observability)
    # -----------------

    async def record_sync_outcome(
        self,
        *,
        source_type: str,
        source_id: str,
        success: bool,
        command_id: Optional[str] = None,
        error: Optional[str] = None,
        next_retry_at: Optional[datetime] = None,
        rate_limit_until: Optional[datetime] = None,
    ) -> None:
        if not self._pool:
            raise RuntimeError("ConnectorRegistry not connected")
        st = (source_type or "").strip()
        sid = (source_id or "").strip()
        now = utcnow()
        error_norm = (error or "").strip()[:4000] if error else None
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"""
                    INSERT INTO {self._schema}.connector_sync_state (source_type, source_id, last_seen_cursor, last_emitted_seq, updated_at)
                    VALUES ($1, $2, NULL, 0, $3)
                    ON CONFLICT (source_type, source_id) DO NOTHING
                    """,
                    st,
                    sid,
                    now,
                )

                if success:
                    await conn.execute(
                        f"""
                        UPDATE {self._schema}.connector_sync_state
                        SET last_success_at = $3,
                            last_error = NULL,
                            attempt_count = 0,
                            next_retry_at = NULL,
                            rate_limit_until = NULL,
                            last_command_id = COALESCE($4, last_command_id),
                            updated_at = $3
                        WHERE source_type = $1 AND source_id = $2
                        """,
                        st,
                        sid,
                        now,
                        command_id,
                    )
                else:
                    await conn.execute(
                        f"""
                        UPDATE {self._schema}.connector_sync_state
                        SET last_failure_at = $3,
                            last_error = $4,
                            attempt_count = attempt_count + 1,
                            next_retry_at = COALESCE($5, next_retry_at),
                            rate_limit_until = COALESCE($6, rate_limit_until),
                            last_command_id = COALESCE($7, last_command_id),
                            updated_at = $3
                        WHERE source_type = $1 AND source_id = $2
                        """,
                        st,
                        sid,
                        now,
                        error_norm,
                        next_retry_at,
                        rate_limit_until,
                        command_id,
                    )

    async def get_sync_state(self, *, source_type: str, source_id: str) -> Optional[SyncState]:
        if not self._pool:
            raise RuntimeError("ConnectorRegistry not connected")
        st = (source_type or "").strip()
        sid = (source_id or "").strip()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT source_type, source_id, last_seen_cursor, last_emitted_seq, last_polled_at,
                       last_success_at, last_failure_at, last_error, attempt_count, rate_limit_until,
                       next_retry_at, last_command_id, updated_at
                FROM {self._schema}.connector_sync_state
                WHERE source_type = $1 AND source_id = $2
                """,
                st,
                sid,
            )
            if not row:
                return None
            return SyncState(
                source_type=str(row["source_type"]),
                source_id=str(row["source_id"]),
                last_seen_cursor=row["last_seen_cursor"],
                last_emitted_seq=int(row["last_emitted_seq"] or 0),
                last_polled_at=row["last_polled_at"],
                last_success_at=row["last_success_at"],
                last_failure_at=row["last_failure_at"],
                last_error=row["last_error"],
                attempt_count=int(row["attempt_count"] or 0),
                rate_limit_until=row["rate_limit_until"],
                next_retry_at=row["next_retry_at"],
                last_command_id=row["last_command_id"],
                updated_at=row["updated_at"],
            )
