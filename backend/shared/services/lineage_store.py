"""
First-class lineage (provenance) store.

This service persists a lineage graph (nodes + edges) in Postgres so that:
- Lineage is queryable as a graph (upstream/downstream traversal)
- Operational tools (impact analysis / rollback planning) can depend on it
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import asyncpg

from shared.config.service_config import ServiceConfig
from shared.models.event_envelope import EventEnvelope
from shared.models.lineage import LineageDirection, LineageEdge, LineageGraph, LineageNode
from shared.utils.ontology_version import extract_ontology_version


class LineageStore:
    """
    Postgres-backed lineage store.

    Node id convention (string, stable):
    - event:<event_id>
    - agg:<aggregate_type>:<aggregate_id>
    - artifact:<kind>:<id...>
    """

    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_lineage",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
    ):
        self._dsn = dsn or ServiceConfig.get_postgres_url()
        self._schema = schema
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_min = int(os.getenv("LINEAGE_PG_POOL_MIN", str(pool_min or 1)))
        self._pool_max = int(os.getenv("LINEAGE_PG_POOL_MAX", str(pool_max or 5)))

    async def initialize(self) -> None:
        await self.connect()

    async def connect(self) -> None:
        if self._pool:
            return
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._pool_min,
            max_size=self._pool_max,
            command_timeout=int(os.getenv("LINEAGE_PG_COMMAND_TIMEOUT", "30")),
        )
        await self.ensure_schema()

    async def shutdown(self) -> None:
        await self.close()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def health_check(self) -> bool:
        try:
            if not self._pool:
                await self.connect()
            async with self._pool.acquire() as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    async def ensure_schema(self) -> None:
        if not self._pool:
            raise RuntimeError("LineageStore not connected")

        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.lineage_nodes (
                    node_id TEXT PRIMARY KEY,
                    node_type TEXT NOT NULL,
                    label TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    event_id TEXT,
                    aggregate_type TEXT,
                    aggregate_id TEXT,
                    artifact_kind TEXT,
                    artifact_key TEXT,
                    db_name TEXT,
                    run_id TEXT,
                    code_sha TEXT,
                    schema_version TEXT,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb
                )
                """
            )
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.lineage_edges (
                    edge_id UUID PRIMARY KEY,
                    from_node_id TEXT NOT NULL,
                    to_node_id TEXT NOT NULL,
                    edge_type TEXT NOT NULL,
                    projection_name TEXT NOT NULL DEFAULT '',
                    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    db_name TEXT,
                    run_id TEXT,
                    code_sha TEXT,
                    schema_version TEXT,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb
                )
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.lineage_backfill_queue (
                    backfill_id UUID PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    s3_bucket TEXT,
                    s3_key TEXT,
                    db_name TEXT,
                    occurred_at TIMESTAMPTZ,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_attempt_at TIMESTAMPTZ,
                    last_error TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_lineage_backfill_event_unique
                ON {self._schema}.lineage_backfill_queue(event_id)
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_lineage_backfill_status_time ON {self._schema}.lineage_backfill_queue(status, created_at)"
            )
            # Add missing columns if the table already existed (best-effort forwards compatibility).
            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_nodes ADD COLUMN IF NOT EXISTS recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_nodes ADD COLUMN IF NOT EXISTS event_id TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_nodes ADD COLUMN IF NOT EXISTS aggregate_type TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_nodes ADD COLUMN IF NOT EXISTS aggregate_id TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_nodes ADD COLUMN IF NOT EXISTS artifact_kind TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_nodes ADD COLUMN IF NOT EXISTS artifact_key TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_nodes ADD COLUMN IF NOT EXISTS db_name TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_nodes ADD COLUMN IF NOT EXISTS run_id TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_nodes ADD COLUMN IF NOT EXISTS code_sha TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_nodes ADD COLUMN IF NOT EXISTS schema_version TEXT"
            )

            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_edges ADD COLUMN IF NOT EXISTS projection_name TEXT NOT NULL DEFAULT ''"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_edges ADD COLUMN IF NOT EXISTS recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_edges ADD COLUMN IF NOT EXISTS db_name TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_edges ADD COLUMN IF NOT EXISTS run_id TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_edges ADD COLUMN IF NOT EXISTS code_sha TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.lineage_edges ADD COLUMN IF NOT EXISTS schema_version TEXT"
            )

            # Backfill db_name columns from existing metadata (best-effort).
            try:
                await conn.execute(
                    f"""
                    UPDATE {self._schema}.lineage_edges
                    SET db_name = COALESCE(db_name, metadata->>'db_name')
                    WHERE db_name IS NULL AND (metadata ? 'db_name')
                    """
                )
            except Exception:
                pass
            try:
                await conn.execute(
                    f"""
                    UPDATE {self._schema}.lineage_nodes
                    SET db_name = COALESCE(db_name, metadata->>'db_name')
                    WHERE db_name IS NULL AND (metadata ? 'db_name')
                    """
                )
            except Exception:
                pass

            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_lineage_edges_from ON {self._schema}.lineage_edges(from_node_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_lineage_edges_to ON {self._schema}.lineage_edges(to_node_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_lineage_edges_type ON {self._schema}.lineage_edges(edge_type)"
            )
            # Idempotency: ensure retries don't create duplicate edges.
            # If duplicates already exist (older runs), deduplicate before creating the unique index.
            try:
                await conn.execute(
                    f"""
                    WITH ranked AS (
                        SELECT edge_id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY from_node_id, to_node_id, edge_type, projection_name
                                   ORDER BY recorded_at ASC, edge_id ASC
                               ) AS rn
                        FROM {self._schema}.lineage_edges
                    )
                    DELETE FROM {self._schema}.lineage_edges
                    WHERE edge_id IN (SELECT edge_id FROM ranked WHERE rn > 1)
                    """
                )
            except Exception:
                # Best-effort cleanup; index creation will still enforce correctness.
                pass

            await conn.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_lineage_edges_unique
                ON {self._schema}.lineage_edges(from_node_id, to_node_id, edge_type, projection_name)
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_lineage_edges_from_type ON {self._schema}.lineage_edges(from_node_id, edge_type)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_lineage_edges_to_type ON {self._schema}.lineage_edges(to_node_id, edge_type)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_lineage_edges_db_time ON {self._schema}.lineage_edges(db_name, occurred_at)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_lineage_nodes_event_id ON {self._schema}.lineage_nodes(event_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_lineage_nodes_aggregate ON {self._schema}.lineage_nodes(aggregate_type, aggregate_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_lineage_nodes_artifact ON {self._schema}.lineage_nodes(artifact_kind, artifact_key)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_lineage_nodes_db ON {self._schema}.lineage_nodes(db_name)"
            )

    # -------------------------
    # Node/edge helpers
    # -------------------------

    @staticmethod
    def node_event(event_id: str) -> str:
        return f"event:{event_id}"

    @staticmethod
    def node_aggregate(aggregate_type: str, aggregate_id: str) -> str:
        return f"agg:{aggregate_type}:{aggregate_id}"

    @staticmethod
    def node_artifact(kind: str, *parts: str) -> str:
        safe_parts = [p for p in parts if isinstance(p, str) and p != ""]
        return "artifact:" + ":".join([kind, *safe_parts])

    @staticmethod
    def _infer_node_type(node_id: str) -> str:
        if not isinstance(node_id, str):
            return "unknown"
        if node_id.startswith("event:"):
            return "event"
        if node_id.startswith("agg:"):
            return "aggregate"
        if node_id.startswith("artifact:"):
            return "artifact"
        return "unknown"

    @staticmethod
    def _parse_node_id(node_id: str) -> Dict[str, Optional[str]]:
        """
        Decompose node_id into queryable columns (best-effort).

        This keeps the stable string id as the primary identifier while allowing
        indexes/filters without expensive substring ops.
        """
        if not isinstance(node_id, str):
            return {
                "event_id": None,
                "aggregate_type": None,
                "aggregate_id": None,
                "artifact_kind": None,
                "artifact_key": None,
            }

        if node_id.startswith("event:"):
            return {
                "event_id": node_id[len("event:") :],
                "aggregate_type": None,
                "aggregate_id": None,
                "artifact_kind": None,
                "artifact_key": None,
            }

        if node_id.startswith("agg:"):
            rest = node_id[len("agg:") :]
            parts = rest.split(":", 1)
            aggregate_type = parts[0] if parts else None
            aggregate_id = parts[1] if len(parts) > 1 else None
            return {
                "event_id": None,
                "aggregate_type": aggregate_type,
                "aggregate_id": aggregate_id,
                "artifact_kind": None,
                "artifact_key": None,
            }

        if node_id.startswith("artifact:"):
            rest = node_id[len("artifact:") :]
            parts = rest.split(":", 1)
            artifact_kind = parts[0] if parts else None
            artifact_key = parts[1] if len(parts) > 1 else None
            return {
                "event_id": None,
                "aggregate_type": None,
                "aggregate_id": None,
                "artifact_kind": artifact_kind,
                "artifact_key": artifact_key,
            }

        return {
            "event_id": None,
            "aggregate_type": None,
            "aggregate_id": None,
            "artifact_kind": None,
            "artifact_key": None,
        }

    @staticmethod
    def _env_first(*keys: str) -> Optional[str]:
        for k in keys:
            v = os.getenv(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return None

    @classmethod
    def _run_context(cls) -> Dict[str, Optional[str]]:
        return {
            "run_id": cls._env_first("PIPELINE_RUN_ID", "RUN_ID", "EXECUTION_ID"),
            "code_sha": cls._env_first("CODE_SHA", "GIT_SHA", "COMMIT_SHA"),
        }

    @staticmethod
    def _deterministic_edge_id(*parts: str) -> UUID:
        token = "|".join([p for p in parts if p is not None])
        return uuid5(NAMESPACE_URL, f"spice-lineage:{token}")

    @staticmethod
    def _coerce_metadata(value: Any) -> Dict[str, Any]:
        """
        Coerce a Postgres JSONB value into a Python dict.

        asyncpg can return json/jsonb as a `str` depending on codecs; this helper
        makes read paths resilient.
        """
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
                if isinstance(parsed, dict):
                    return parsed
                return {"value": parsed}
            except Exception:
                return {"raw": value}
        if isinstance(value, (list, tuple)):
            try:
                return dict(value)
            except Exception:
                return {"raw": value}
        try:
            return dict(value)
        except Exception:
            return {}

    async def upsert_node(
        self,
        *,
        node_id: str,
        node_type: Optional[str] = None,
        label: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        created_at: Optional[datetime] = None,
        recorded_at: Optional[datetime] = None,
        db_name: Optional[str] = None,
        run_id: Optional[str] = None,
        code_sha: Optional[str] = None,
        schema_version: Optional[str] = None,
    ) -> None:
        if not self._pool:
            await self.connect()

        node_type = node_type or self._infer_node_type(node_id)
        metadata = metadata or {}
        created_at = created_at or datetime.now(timezone.utc)
        recorded_at = recorded_at or datetime.now(timezone.utc)

        parsed = self._parse_node_id(node_id)
        ctx = self._run_context()
        run_id = run_id or ctx.get("run_id")
        code_sha = code_sha or ctx.get("code_sha")
        schema_version = schema_version or (metadata.get("schema_version") if isinstance(metadata, dict) else None)
        db_name = db_name or (metadata.get("db_name") if isinstance(metadata, dict) else None)

        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._schema}.lineage_nodes (
                    node_id, node_type, label, created_at, recorded_at,
                    event_id, aggregate_type, aggregate_id, artifact_kind, artifact_key,
                    db_name, run_id, code_sha, schema_version, metadata
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::jsonb)
                ON CONFLICT (node_id) DO UPDATE
                SET node_type = EXCLUDED.node_type,
                    label = COALESCE(EXCLUDED.label, {self._schema}.lineage_nodes.label),
                    recorded_at = GREATEST({self._schema}.lineage_nodes.recorded_at, EXCLUDED.recorded_at),
                    db_name = COALESCE(EXCLUDED.db_name, {self._schema}.lineage_nodes.db_name),
                    run_id = COALESCE(EXCLUDED.run_id, {self._schema}.lineage_nodes.run_id),
                    code_sha = COALESCE(EXCLUDED.code_sha, {self._schema}.lineage_nodes.code_sha),
                    schema_version = COALESCE(EXCLUDED.schema_version, {self._schema}.lineage_nodes.schema_version),
                    metadata = {self._schema}.lineage_nodes.metadata || EXCLUDED.metadata
                """,
                node_id,
                node_type,
                label,
                created_at,
                recorded_at,
                parsed.get("event_id"),
                parsed.get("aggregate_type"),
                parsed.get("aggregate_id"),
                parsed.get("artifact_kind"),
                parsed.get("artifact_key"),
                db_name,
                run_id,
                code_sha,
                schema_version,
                json.dumps(metadata, ensure_ascii=False),
            )

    async def insert_edge(
        self,
        *,
        from_node_id: str,
        to_node_id: str,
        edge_type: str,
        occurred_at: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None,
        projection_name: Optional[str] = None,
        recorded_at: Optional[datetime] = None,
        db_name: Optional[str] = None,
        run_id: Optional[str] = None,
        code_sha: Optional[str] = None,
        schema_version: Optional[str] = None,
        edge_id: Optional[UUID] = None,
    ) -> UUID:
        if not self._pool:
            await self.connect()

        occurred_at = occurred_at or datetime.now(timezone.utc)
        recorded_at = recorded_at or datetime.now(timezone.utc)
        metadata = metadata or {}
        projection_name = projection_name or (metadata.get("projection_name") if isinstance(metadata, dict) else None) or ""

        ctx = self._run_context()
        run_id = run_id or (metadata.get("run_id") if isinstance(metadata, dict) else None) or ctx.get("run_id")
        code_sha = code_sha or (metadata.get("code_sha") if isinstance(metadata, dict) else None) or ctx.get("code_sha")
        schema_version = schema_version or (
            metadata.get("schema_version") if isinstance(metadata, dict) else None
        )
        db_name = db_name or (metadata.get("db_name") if isinstance(metadata, dict) else None)

        # Deterministic edge id is useful for debugging but uniqueness is enforced at the DB level.
        edge_id = edge_id or uuid4()

        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._schema}.lineage_edges (
                    edge_id, from_node_id, to_node_id, edge_type, projection_name,
                    occurred_at, recorded_at, db_name, run_id, code_sha, schema_version, metadata
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
                ON CONFLICT (from_node_id, to_node_id, edge_type, projection_name) DO NOTHING
                """,
                edge_id,
                from_node_id,
                to_node_id,
                edge_type,
                projection_name,
                occurred_at,
                recorded_at,
                db_name,
                run_id,
                code_sha,
                schema_version,
                json.dumps(metadata, ensure_ascii=False),
            )
        return edge_id

    # -------------------------
    # Backfill queue (eventual recovery)
    # -------------------------

    async def enqueue_backfill(
        self,
        *,
        envelope: EventEnvelope,
        s3_bucket: Optional[str],
        s3_key: Optional[str],
        error: str,
    ) -> None:
        """
        Best-effort enqueue for eventual lineage recovery.

        This does not guarantee immediate lineage recording, but provides a durable
        signal that "this event is missing lineage" so that a backfill job can
        replay it later.
        """
        if not self._pool:
            await self.connect()

        db_name = envelope.data.get("db_name") if isinstance(envelope.data, dict) else None
        occurred_at = envelope.occurred_at
        if occurred_at.tzinfo is None:
            occurred_at = occurred_at.replace(tzinfo=timezone.utc)

        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._schema}.lineage_backfill_queue (
                    backfill_id, event_id, s3_bucket, s3_key, db_name, occurred_at,
                    status, attempts, last_attempt_at, last_error, created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'pending', 0, NULL, $7, NOW(), NOW())
                ON CONFLICT (event_id) DO UPDATE
                SET s3_bucket = COALESCE(EXCLUDED.s3_bucket, {self._schema}.lineage_backfill_queue.s3_bucket),
                    s3_key = COALESCE(EXCLUDED.s3_key, {self._schema}.lineage_backfill_queue.s3_key),
                    db_name = COALESCE(EXCLUDED.db_name, {self._schema}.lineage_backfill_queue.db_name),
                    occurred_at = COALESCE(EXCLUDED.occurred_at, {self._schema}.lineage_backfill_queue.occurred_at),
                    status = 'pending',
                    last_error = EXCLUDED.last_error,
                    updated_at = NOW()
                """,
                uuid4(),
                str(envelope.event_id),
                s3_bucket,
                s3_key,
                db_name,
                occurred_at,
                (error or "")[:4000],
            )

    async def mark_backfill_done(self, *, event_id: str) -> None:
        if not self._pool:
            await self.connect()
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.lineage_backfill_queue
                SET status = 'done',
                    updated_at = NOW()
                WHERE event_id = $1
                """,
                event_id,
            )

    async def mark_backfill_failed(self, *, event_id: str, error: str) -> None:
        if not self._pool:
            await self.connect()
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.lineage_backfill_queue
                SET status = 'pending',
                    last_error = $2,
                    updated_at = NOW()
                WHERE event_id = $1
                """,
                event_id,
                (error or "")[:4000],
            )

    async def claim_backfill_batch(
        self,
        *,
        limit: int = 100,
        db_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Claim a batch of pending backfill rows (best-effort).

        This is intentionally simple (no distributed lease). It works well when a
        single backfill job runs, or when jobs process disjoint db_name scopes.
        """
        if not self._pool:
            await self.connect()

        limit = max(1, min(int(limit), 1000))

        clauses = ["status = 'pending'"]
        params: List[Any] = []
        if db_name:
            params.append(db_name)
            clauses.append(f"db_name = ${len(params)}")
        where = " AND ".join(clauses)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT event_id, s3_bucket, s3_key, db_name, occurred_at, attempts, last_error, created_at, updated_at
                FROM {self._schema}.lineage_backfill_queue
                WHERE {where}
                ORDER BY created_at ASC
                LIMIT {limit}
                """,
                *params,
            )

            # Mark as in-progress for observability (still idempotent; no strict lock).
            event_ids = [str(r["event_id"]) for r in rows]
            if event_ids:
                await conn.execute(
                    f"""
                    UPDATE {self._schema}.lineage_backfill_queue
                    SET status = 'in_progress',
                        attempts = attempts + 1,
                        last_attempt_at = NOW(),
                        updated_at = NOW()
                    WHERE event_id = ANY($1::text[])
                    """,
                    event_ids,
                )

        items: List[Dict[str, Any]] = []
        for r in rows:
            items.append({k: (r[k].isoformat() if hasattr(r[k], "isoformat") else r[k]) for k in r.keys()})
        return items

    async def get_backfill_metrics(self, *, db_name: Optional[str] = None) -> Dict[str, Any]:
        if not self._pool:
            await self.connect()
        clauses = ["status = 'pending'"]
        params: List[Any] = []
        if db_name:
            params.append(db_name)
            clauses.append(f"db_name = ${len(params)}")
        where = " AND ".join(clauses)

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT COUNT(*)::bigint AS pending_count,
                       MIN(created_at) AS oldest_created_at
                FROM {self._schema}.lineage_backfill_queue
                WHERE {where}
                """,
                *params,
            )
        pending = int(row["pending_count"] or 0) if row else 0
        oldest = row["oldest_created_at"] if row else None
        now = datetime.now(timezone.utc)
        lag_seconds: Optional[float] = None
        if oldest:
            oldest_ts = oldest if oldest.tzinfo else oldest.replace(tzinfo=timezone.utc)
            lag_seconds = max(0.0, (now - oldest_ts).total_seconds())
        return {
            "pending_count": pending,
            "oldest_created_at": oldest.isoformat() if oldest else None,
            "lineage_lag_seconds": lag_seconds,
        }

    # -------------------------
    # Metrics helpers
    # -------------------------

    async def count_edges(
        self,
        *,
        edge_type: Optional[str] = None,
        db_name: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> int:
        if not self._pool:
            await self.connect()

        clauses: List[str] = []
        params: List[Any] = []

        def _add(condition: str, value: Any) -> None:
            params.append(value)
            clauses.append(condition.replace("$X", f"${len(params)}"))

        if edge_type:
            _add("edge_type = $X", edge_type)
        if db_name:
            _add("db_name = $X", db_name)
        if since:
            _add("occurred_at >= $X", since)
        if until:
            _add("occurred_at <= $X", until)

        where = " WHERE " + " AND ".join(clauses) if clauses else ""

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT COUNT(*)::bigint AS c FROM {self._schema}.lineage_edges{where}",
                *params,
            )
        return int(row["c"] or 0) if row else 0

    async def get_latest_edges_to(
        self,
        *,
        to_node_ids: Sequence[str],
        edge_type: Optional[str] = None,
        projection_name: Optional[str] = None,
        db_name: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch the latest edge (by occurred_at) for each `to_node_id`.

        This is useful for provenance metadata in read APIs:
        - last event that wrote a Terminus document
        - last event that materialized an ES document
        """
        if not self._pool:
            await self.connect()

        ids = [i for i in to_node_ids if isinstance(i, str) and i.strip()]
        if not ids:
            return {}

        max_ids = int(os.getenv("LINEAGE_LATEST_EDGES_MAX_IDS", "5000"))
        if len(ids) > max_ids:
            raise ValueError(f"too many node ids (max={max_ids})")

        clauses: List[str] = ["to_node_id = ANY($1::text[])"]
        params: List[Any] = [ids]

        def _add(condition: str, value: Any) -> None:
            params.append(value)
            clauses.append(condition.replace("$X", f"${len(params)}"))

        if edge_type:
            _add("edge_type = $X", edge_type)
        if projection_name is not None:
            _add("projection_name = $X", projection_name)
        if db_name:
            _add("db_name = $X", db_name)

        where = " AND ".join(clauses)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT DISTINCT ON (to_node_id)
                       to_node_id, from_node_id, edge_type, projection_name,
                       occurred_at, recorded_at, metadata
                FROM {self._schema}.lineage_edges
                WHERE {where}
                ORDER BY to_node_id, occurred_at DESC, recorded_at DESC
                """,
                *params,
            )

        latest: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            to_node_id = row["to_node_id"]
            from_node_id = row["from_node_id"]
            event_id = (
                from_node_id[len("event:") :]
                if isinstance(from_node_id, str) and from_node_id.startswith("event:")
                else from_node_id
            )
            occurred_at = row["occurred_at"]
            occurred_at = occurred_at if occurred_at and occurred_at.tzinfo else (
                occurred_at.replace(tzinfo=timezone.utc) if occurred_at else None
            )
            latest[str(to_node_id)] = {
                "from_node_id": str(from_node_id),
                "event_id": str(event_id) if event_id is not None else None,
                "edge_type": str(row["edge_type"]),
                "projection_name": str(row["projection_name"] or ""),
                "occurred_at": occurred_at.isoformat() if occurred_at else None,
                "metadata": self._coerce_metadata(row["metadata"]),
            }
        return latest

    async def record_link(
        self,
        *,
        from_node_id: str,
        to_node_id: str,
        edge_type: str,
        occurred_at: Optional[datetime] = None,
        edge_metadata: Optional[Dict[str, Any]] = None,
        from_label: Optional[str] = None,
        to_label: Optional[str] = None,
        from_type: Optional[str] = None,
        to_type: Optional[str] = None,
        from_metadata: Optional[Dict[str, Any]] = None,
        to_metadata: Optional[Dict[str, Any]] = None,
        db_name: Optional[str] = None,
        projection_name: Optional[str] = None,
        run_id: Optional[str] = None,
        code_sha: Optional[str] = None,
        schema_version: Optional[str] = None,
        edge_id: Optional[UUID] = None,
    ) -> UUID:
        ctx = self._run_context()
        run_id = run_id or ctx.get("run_id")
        code_sha = code_sha or ctx.get("code_sha")
        if edge_metadata is None:
            edge_metadata = {}
        if db_name is None and isinstance(edge_metadata, dict):
            db_name = edge_metadata.get("db_name")
        if projection_name is None and isinstance(edge_metadata, dict):
            projection_name = edge_metadata.get("projection_name")
        if schema_version is None and isinstance(edge_metadata, dict):
            schema_version = edge_metadata.get("schema_version")

        await self.upsert_node(
            node_id=from_node_id,
            node_type=from_type,
            label=from_label,
            metadata=from_metadata,
            created_at=occurred_at,
            recorded_at=datetime.now(timezone.utc),
            db_name=db_name,
            run_id=run_id,
            code_sha=code_sha,
            schema_version=schema_version,
        )
        await self.upsert_node(
            node_id=to_node_id,
            node_type=to_type,
            label=to_label,
            metadata=to_metadata,
            created_at=occurred_at,
            recorded_at=datetime.now(timezone.utc),
            db_name=db_name,
            run_id=run_id,
            code_sha=code_sha,
            schema_version=schema_version,
        )
        return await self.insert_edge(
            from_node_id=from_node_id,
            to_node_id=to_node_id,
            edge_type=edge_type,
            occurred_at=occurred_at,
            metadata=edge_metadata,
            projection_name=projection_name,
            recorded_at=datetime.now(timezone.utc),
            db_name=db_name,
            run_id=run_id,
            code_sha=code_sha,
            schema_version=schema_version,
            edge_id=edge_id,
        )

    # -------------------------
    # High-level recording APIs
    # -------------------------

    async def record_event_envelope(
        self,
        envelope: EventEnvelope,
        *,
        s3_bucket: Optional[str] = None,
        s3_key: Optional[str] = None,
    ) -> None:
        """
        Record the core lineage relationships for an EventEnvelope:
        - aggregate -> event
        - (optional) event -> event_store_object
        - (optional) command_event -> domain_event (causation)
        """
        occurred_at = envelope.occurred_at
        if occurred_at.tzinfo is None:
            occurred_at = occurred_at.replace(tzinfo=timezone.utc)

        agg_node = self.node_aggregate(envelope.aggregate_type, envelope.aggregate_id)
        event_node = self.node_event(str(envelope.event_id))
        db_name = envelope.data.get("db_name") if isinstance(envelope.data, dict) else None
        producer_service = envelope.metadata.get("service") if isinstance(envelope.metadata, dict) else None
        producer_run_id = envelope.metadata.get("run_id") if isinstance(envelope.metadata, dict) else None
        producer_code_sha = envelope.metadata.get("code_sha") if isinstance(envelope.metadata, dict) else None
        ontology = extract_ontology_version(envelope_metadata=envelope.metadata, envelope_data=envelope.data)

        await self.record_link(
            from_node_id=agg_node,
            to_node_id=event_node,
            edge_type="aggregate_emitted_event",
            occurred_at=occurred_at,
            from_label=f"{envelope.aggregate_type}:{envelope.aggregate_id}",
            to_label=envelope.event_type,
            db_name=db_name,
            edge_metadata={
                "event_type": envelope.event_type,
                "sequence_number": envelope.sequence_number,
                "kind": (envelope.metadata or {}).get("kind") if isinstance(envelope.metadata, dict) else None,
                "kafka_topic": (envelope.metadata or {}).get("kafka_topic") if isinstance(envelope.metadata, dict) else None,
                "db_name": db_name,
                "producer_service": producer_service,
                "producer_run_id": str(producer_run_id) if producer_run_id else None,
                "producer_code_sha": str(producer_code_sha) if producer_code_sha else None,
                "schema_version": envelope.schema_version,
                "ontology": ontology,
            },
        )

        if s3_bucket and s3_key:
            artifact_node = self.node_artifact("s3", s3_bucket, s3_key)
            await self.record_link(
                from_node_id=event_node,
                to_node_id=artifact_node,
                edge_type="event_stored_in_object_store",
                occurred_at=occurred_at,
                to_label=f"s3://{s3_bucket}/{s3_key}",
                db_name=db_name,
                edge_metadata={
                    "bucket": s3_bucket,
                    "key": s3_key,
                    "db_name": db_name,
                    "producer_service": producer_service,
                    "producer_run_id": str(producer_run_id) if producer_run_id else None,
                    "producer_code_sha": str(producer_code_sha) if producer_code_sha else None,
                    "schema_version": envelope.schema_version,
                    "ontology": ontology,
                },
            )

        if isinstance(envelope.metadata, dict):
            command_id = envelope.metadata.get("command_id")
            kind = envelope.metadata.get("kind")
            if kind == "domain" and command_id:
                command_node = self.node_event(str(command_id))
                await self.record_link(
                    from_node_id=command_node,
                    to_node_id=event_node,
                    edge_type="command_caused_domain_event",
                    occurred_at=occurred_at,
                    from_label="command",
                    to_label=envelope.event_type,
                    db_name=db_name,
                    edge_metadata={
                        "command_id": str(command_id),
                        "db_name": db_name,
                        "producer_service": producer_service,
                        "producer_run_id": str(producer_run_id) if producer_run_id else None,
                        "producer_code_sha": str(producer_code_sha) if producer_code_sha else None,
                        "schema_version": envelope.schema_version,
                        "ontology": ontology,
                    },
                )

    # -------------------------
    # Query
    # -------------------------

    @staticmethod
    def normalize_root(root: str) -> str:
        if not isinstance(root, str):
            raise ValueError("root must be a string")
        root = root.strip()
        if not root:
            raise ValueError("root is required")
        if root.startswith(("event:", "agg:", "artifact:")):
            return root
        # Shorthand: treat UUID-like values as event ids.
        if len(root) >= 32 and all(c.isalnum() or c in "-_" for c in root):
            return f"event:{root}"
        return root

    async def get_graph(
        self,
        *,
        root: str,
        direction: LineageDirection = "both",
        max_depth: int = 5,
        max_nodes: int = 500,
        max_edges: int = 2000,
        db_name: Optional[str] = None,
    ) -> LineageGraph:
        if not self._pool:
            await self.connect()

        root_node = self.normalize_root(root)
        max_depth = max(0, int(max_depth))
        max_nodes = max(1, int(max_nodes))
        max_edges = max(1, int(max_edges))

        visited: set[str] = {root_node}
        edges_by_id: Dict[str, LineageEdge] = {}
        warnings: List[str] = []

        async def _walk(direction_local: str) -> None:
            nonlocal visited, edges_by_id, warnings

            frontier: set[str] = {root_node}
            for depth in range(max_depth):
                if not frontier:
                    return
                if len(visited) >= max_nodes:
                    warnings.append(f"node_limit_reached(max_nodes={max_nodes})")
                    return
                if len(edges_by_id) >= max_edges:
                    warnings.append(f"edge_limit_reached(max_edges={max_edges})")
                    return

                if direction_local == "downstream":
                    column = "from_node_id"
                else:
                    column = "to_node_id"

                remaining_edges = max_edges - len(edges_by_id)
                remaining_nodes = max_nodes - len(visited)
                limit = max(1, min(10000, remaining_edges + 50))

                async with self._pool.acquire() as conn:
                    if db_name:
                        rows = await conn.fetch(
                            f"""
                            SELECT edge_id, from_node_id, to_node_id, edge_type, occurred_at, recorded_at, metadata
                            FROM {self._schema}.lineage_edges
                            WHERE {column} = ANY($1::text[])
                              AND db_name = $3
                            ORDER BY occurred_at ASC
                            LIMIT $2
                            """,
                            list(frontier),
                            limit,
                            db_name,
                        )
                    else:
                        rows = await conn.fetch(
                            f"""
                            SELECT edge_id, from_node_id, to_node_id, edge_type, occurred_at, recorded_at, metadata
                            FROM {self._schema}.lineage_edges
                            WHERE {column} = ANY($1::text[])
                            ORDER BY occurred_at ASC
                            LIMIT $2
                            """,
                            list(frontier),
                            limit,
                        )

                next_frontier: set[str] = set()
                for row in rows:
                    edge_id = str(row["edge_id"])
                    if edge_id not in edges_by_id:
                        edges_by_id[edge_id] = LineageEdge(
                            edge_id=edge_id,
                            from_node_id=row["from_node_id"],
                            to_node_id=row["to_node_id"],
                            edge_type=row["edge_type"],
                            occurred_at=row["occurred_at"],
                            recorded_at=row["recorded_at"],
                            metadata=self._coerce_metadata(row["metadata"]),
                        )

                    next_node = row["to_node_id"] if direction_local == "downstream" else row["from_node_id"]
                    if next_node not in visited:
                        if len(visited) >= max_nodes:
                            warnings.append(f"node_limit_reached(max_nodes={max_nodes})")
                            return
                        visited.add(next_node)
                        next_frontier.add(next_node)

                frontier = next_frontier

        if direction in {"downstream", "both"}:
            await _walk("downstream")
        if direction in {"upstream", "both"}:
            await _walk("upstream")

        node_rows: List[asyncpg.Record] = []
        async with self._pool.acquire() as conn:
            node_rows = await conn.fetch(
                f"""
                SELECT node_id, node_type, label, created_at, recorded_at, metadata
                FROM {self._schema}.lineage_nodes
                WHERE node_id = ANY($1::text[])
                """,
                list(visited),
            )

        nodes_by_id: Dict[str, LineageNode] = {}
        for row in node_rows:
            node_id = row["node_id"]
            nodes_by_id[node_id] = LineageNode(
                node_id=node_id,
                node_type=row["node_type"],
                label=row["label"],
                created_at=row["created_at"],
                recorded_at=row["recorded_at"],
                metadata=self._coerce_metadata(row["metadata"]),
            )

        # Ensure root node is present even if not stored yet.
        if root_node not in nodes_by_id:
            nodes_by_id[root_node] = LineageNode(
                node_id=root_node,
                node_type=self._infer_node_type(root_node),
                label=None,
                created_at=None,
                recorded_at=None,
                metadata={},
            )

        return LineageGraph(
            root=root_node,
            direction=direction,
            max_depth=max_depth,
            nodes=list(nodes_by_id.values()),
            edges=list(edges_by_id.values()),
            warnings=warnings,
        )


def create_lineage_store(settings: Any) -> LineageStore:
    # settings is unused; ServiceConfig reads envs and handles docker/local.
    return LineageStore()
