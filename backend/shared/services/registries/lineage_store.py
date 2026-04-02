"""
First-class lineage (provenance) store.

This service persists a lineage graph (nodes + edges) in Postgres so that:
- Lineage is queryable as a graph (upstream/downstream traversal)
- Operational tools (impact analysis / rollback planning) can depend on it
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import asyncpg

from shared.config.settings import get_settings
from shared.models.event_envelope import EventEnvelope
from shared.models.lineage_edge_types import (
    EDGE_AGGREGATE_EMITTED_EVENT,
    EDGE_COMMAND_CAUSED_DOMAIN_EVENT,
    EDGE_EVENT_STORED_IN_OBJECT_STORE,
)
from shared.models.lineage import LineageDirection, LineageEdge, LineageGraph, LineageNode
from shared.services.registries.postgres_schema_registry import PostgresSchemaRegistry
from shared.utils.ontology_version import extract_ontology_version
from shared.utils.sql_filter_builder import SqlFilterBuilder
import logging


class LineageStore(PostgresSchemaRegistry):
    """
    Postgres-backed lineage store.

    Node id convention (string, stable):
    - event:<event_id>
    - agg:<aggregate_type>:<aggregate_id>
    - artifact:<kind>:<id...>
    """

    _DISALLOWED_ES_ARTIFACT_ALIAS_PREFIX = "artifact:elasticsearch:"
    _DISALLOWED_ES_EDGE_TYPE_ALIAS = "event_wrote_es_document"
    _DISALLOWED_S3_EDGE_TYPE_ALIAS = "event_wrote_s3_object"
    _REQUIRED_TABLES = (
        "lineage_nodes",
        "lineage_edges",
        "lineage_backfill_queue",
    )

    @staticmethod
    def _is_ignorable_schema_backfill_error(exc: asyncpg.PostgresError) -> bool:
        return isinstance(
            exc,
            (
                asyncpg.UndefinedTableError,
                asyncpg.UndefinedColumnError,
                asyncpg.UndefinedFunctionError,
            ),
        )

    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_lineage",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
        allow_runtime_ddl_bootstrap: Optional[bool] = None,
    ):
        perf = get_settings().performance
        pool_min_value = int(pool_min) if pool_min is not None else int(perf.lineage_pg_pool_min)
        pool_max_value = int(pool_max) if pool_max is not None else int(perf.lineage_pg_pool_max)
        super().__init__(
            dsn=dsn,
            schema=schema,
            pool_min=pool_min_value,
            pool_max=pool_max_value,
            command_timeout=int(perf.lineage_pg_command_timeout_seconds),
            allow_runtime_ddl_bootstrap=allow_runtime_ddl_bootstrap,
        )

    def _required_tables(self) -> tuple[str, ...]:
        return self._REQUIRED_TABLES

    async def _ensure_tables(self, conn: asyncpg.Connection) -> None:
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
                branch TEXT,
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
                branch TEXT,
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
                branch TEXT,
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
        await conn.execute(
            f"ALTER TABLE {self._schema}.lineage_backfill_queue ADD COLUMN IF NOT EXISTS branch TEXT"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_lineage_backfill_db_branch_status ON {self._schema}.lineage_backfill_queue(db_name, branch, status, created_at)"
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
            f"ALTER TABLE {self._schema}.lineage_nodes ADD COLUMN IF NOT EXISTS branch TEXT"
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
            f"ALTER TABLE {self._schema}.lineage_edges ADD COLUMN IF NOT EXISTS branch TEXT"
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
        except asyncpg.PostgresError as exc:
            if self._is_ignorable_schema_backfill_error(exc):
                logging.getLogger(__name__).warning(
                    "Best-effort lineage edge db_name backfill skipped due to schema incompatibility: %s",
                    exc,
                    exc_info=True,
                )
            else:
                raise
        try:
            await conn.execute(
                f"""
                UPDATE {self._schema}.lineage_nodes
                SET db_name = COALESCE(db_name, metadata->>'db_name')
                WHERE db_name IS NULL AND (metadata ? 'db_name')
                """
            )
        except asyncpg.PostgresError as exc:
            if self._is_ignorable_schema_backfill_error(exc):
                logging.getLogger(__name__).warning(
                    "Best-effort lineage node db_name backfill skipped due to schema incompatibility: %s",
                    exc,
                    exc_info=True,
                )
            else:
                raise
        try:
            await conn.execute(
                f"""
                UPDATE {self._schema}.lineage_edges
                SET branch = COALESCE(branch, metadata->>'branch')
                WHERE branch IS NULL AND (metadata ? 'branch')
                """
            )
        except asyncpg.PostgresError as exc:
            if self._is_ignorable_schema_backfill_error(exc):
                logging.getLogger(__name__).warning(
                    "Best-effort lineage edge branch backfill skipped due to schema incompatibility: %s",
                    exc,
                    exc_info=True,
                )
            else:
                raise
        try:
            await conn.execute(
                f"""
                UPDATE {self._schema}.lineage_nodes
                SET branch = COALESCE(branch, metadata->>'branch')
                WHERE branch IS NULL AND (metadata ? 'branch')
                """
            )
        except asyncpg.PostgresError as exc:
            if self._is_ignorable_schema_backfill_error(exc):
                logging.getLogger(__name__).warning(
                    "Best-effort lineage node branch backfill skipped due to schema incompatibility: %s",
                    exc,
                    exc_info=True,
                )
            else:
                raise
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_lineage_edges_from ON {self._schema}.lineage_edges(from_node_id)"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_lineage_edges_to ON {self._schema}.lineage_edges(to_node_id)"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_lineage_edges_to_time ON {self._schema}.lineage_edges(to_node_id, occurred_at DESC)"
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
        except asyncpg.PostgresError as exc:
            if self._is_ignorable_schema_backfill_error(exc):
                logging.getLogger(__name__).warning(
                    "Best-effort lineage edge dedupe skipped due to schema incompatibility: %s",
                    exc,
                    exc_info=True,
                )
            else:
                raise

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
            f"CREATE INDEX IF NOT EXISTS idx_lineage_edges_db_branch_time ON {self._schema}.lineage_edges(db_name, branch, occurred_at)"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_lineage_edges_projection_time ON {self._schema}.lineage_edges(projection_name, occurred_at DESC)"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_lineage_edges_run_id_time ON {self._schema}.lineage_edges(run_id, occurred_at)"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_lineage_edges_meta_producer_run_time ON {self._schema}.lineage_edges((metadata->>'producer_run_id'), occurred_at)"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_lineage_edges_meta_run_time ON {self._schema}.lineage_edges((metadata->>'run_id'), occurred_at)"
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
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_lineage_nodes_db_branch ON {self._schema}.lineage_nodes(db_name, branch)"
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

    @classmethod
    def _validate_no_disallowed_aliases(cls, *, node_id: Optional[str] = None, edge_type: Optional[str] = None) -> None:
        if isinstance(node_id, str) and node_id.startswith(cls._DISALLOWED_ES_ARTIFACT_ALIAS_PREFIX):
            raise ValueError("deprecated lineage node id alias is not supported: artifact:elasticsearch:*")
        if isinstance(edge_type, str) and edge_type == cls._DISALLOWED_ES_EDGE_TYPE_ALIAS:
            raise ValueError("deprecated lineage edge type alias is not supported: event_wrote_es_document")
        if isinstance(edge_type, str) and edge_type == cls._DISALLOWED_S3_EDGE_TYPE_ALIAS:
            raise ValueError("deprecated lineage edge type alias is not supported: event_wrote_s3_object")

    @classmethod
    def _canonicalize_node_id(cls, node_id: str) -> str:
        cls._validate_no_disallowed_aliases(node_id=node_id)
        return node_id

    @classmethod
    def _canonicalize_edge_type(cls, edge_type: str) -> str:
        cls._validate_no_disallowed_aliases(edge_type=edge_type)
        return edge_type

    @staticmethod
    def _infer_branch_from_node_id(node_id: str) -> Optional[str]:
        if not isinstance(node_id, str):
            return None
        normalized = LineageStore._canonicalize_node_id(node_id)
        if normalized.startswith("artifact:graph:"):
            parts = normalized.split(":", 4)
            if len(parts) >= 5 and parts[3]:
                return parts[3]
            return None
        if normalized.startswith("artifact:es:"):
            parts = normalized.split(":", 4)
            # artifact:es:<db>:<branch>:<id...>
            if len(parts) >= 5 and parts[3]:
                return parts[3]
            return None
        if normalized.startswith("agg:"):
            parts = normalized.split(":", 2)
            if len(parts) < 3:
                return None
            aggregate_id = parts[2]
            agg_parts = aggregate_id.split(":", 2)
            if len(agg_parts) >= 2 and agg_parts[1]:
                return agg_parts[1]
        return None

    @staticmethod
    def _infer_node_type(node_id: str) -> str:
        if not isinstance(node_id, str):
            return "unknown"
        node_id = LineageStore._canonicalize_node_id(node_id)
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
        node_id = LineageStore._canonicalize_node_id(node_id)

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

    @classmethod
    def _run_context(cls) -> Dict[str, Optional[str]]:
        obs = get_settings().observability
        return {"run_id": obs.run_id, "code_sha": obs.code_sha}

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
            except json.JSONDecodeError:
                logging.getLogger(__name__).warning(
                    "Invalid JSON metadata payload in lineage store; preserving raw metadata string",
                    exc_info=True,
                )
                return {"raw": value}
        if isinstance(value, (list, tuple)):
            try:
                return dict(value)
            except (TypeError, ValueError):
                logging.getLogger(__name__).warning(
                    "Invalid key/value metadata sequence in lineage store; preserving raw metadata value",
                    exc_info=True,
                )
                return {"raw": value}
        try:
            return dict(value)
        except (TypeError, ValueError):
            logging.getLogger(__name__).warning(
                "Metadata coercion failed in lineage store; defaulting metadata to empty dict",
                exc_info=True,
            )
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
        branch: Optional[str] = None,
        run_id: Optional[str] = None,
        code_sha: Optional[str] = None,
        schema_version: Optional[str] = None,
    ) -> None:
        if not self._pool:
            await self.connect()

        node_id = self._canonicalize_node_id(node_id)
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
        branch = branch or (metadata.get("branch") if isinstance(metadata, dict) else None) or self._infer_branch_from_node_id(node_id)

        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._schema}.lineage_nodes (
                    node_id, node_type, label, created_at, recorded_at,
                    event_id, aggregate_type, aggregate_id, artifact_kind, artifact_key,
                    db_name, branch, run_id, code_sha, schema_version, metadata
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16::jsonb)
                ON CONFLICT (node_id) DO UPDATE
                SET node_type = EXCLUDED.node_type,
                    label = COALESCE(EXCLUDED.label, {self._schema}.lineage_nodes.label),
                    recorded_at = GREATEST({self._schema}.lineage_nodes.recorded_at, EXCLUDED.recorded_at),
                    db_name = COALESCE(EXCLUDED.db_name, {self._schema}.lineage_nodes.db_name),
                    branch = COALESCE(EXCLUDED.branch, {self._schema}.lineage_nodes.branch),
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
                branch,
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
        branch: Optional[str] = None,
        run_id: Optional[str] = None,
        code_sha: Optional[str] = None,
        schema_version: Optional[str] = None,
        edge_id: Optional[UUID] = None,
    ) -> UUID:
        if not self._pool:
            await self.connect()

        from_node_id = self._canonicalize_node_id(from_node_id)
        to_node_id = self._canonicalize_node_id(to_node_id)
        edge_type = self._canonicalize_edge_type(edge_type)
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
        branch = branch or (metadata.get("branch") if isinstance(metadata, dict) else None)

        # Deterministic edge id is useful for debugging but uniqueness is enforced at the DB level.
        edge_id = edge_id or uuid4()

        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._schema}.lineage_edges (
                    edge_id, from_node_id, to_node_id, edge_type, projection_name,
                    occurred_at, recorded_at, db_name, branch, run_id, code_sha, schema_version, metadata
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb)
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
                branch,
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
        branch = envelope.data.get("branch") if isinstance(envelope.data, dict) else None
        occurred_at = envelope.occurred_at
        if occurred_at.tzinfo is None:
            occurred_at = occurred_at.replace(tzinfo=timezone.utc)

        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._schema}.lineage_backfill_queue (
                    backfill_id, event_id, s3_bucket, s3_key, db_name, branch, occurred_at,
                    status, attempts, last_attempt_at, last_error, created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', 0, NULL, $8, NOW(), NOW())
                ON CONFLICT (event_id) DO UPDATE
                SET s3_bucket = COALESCE(EXCLUDED.s3_bucket, {self._schema}.lineage_backfill_queue.s3_bucket),
                    s3_key = COALESCE(EXCLUDED.s3_key, {self._schema}.lineage_backfill_queue.s3_key),
                    db_name = COALESCE(EXCLUDED.db_name, {self._schema}.lineage_backfill_queue.db_name),
                    branch = COALESCE(EXCLUDED.branch, {self._schema}.lineage_backfill_queue.branch),
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
                branch,
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
        branch: Optional[str] = None,
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
        if branch:
            params.append(branch)
            clauses.append(f"branch = ${len(params)}")
        where = " AND ".join(clauses)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT event_id, s3_bucket, s3_key, db_name, branch, occurred_at, attempts, last_error, created_at, updated_at
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

    async def get_backfill_metrics(self, *, db_name: Optional[str] = None, branch: Optional[str] = None) -> Dict[str, Any]:
        if not self._pool:
            await self.connect()
        clauses = ["status = 'pending'"]
        params: List[Any] = []
        if db_name:
            params.append(db_name)
            clauses.append(f"db_name = ${len(params)}")
        if branch:
            params.append(branch)
            clauses.append(f"branch = ${len(params)}")
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
        branch: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> int:
        if not self._pool:
            await self.connect()

        filters = SqlFilterBuilder()

        if edge_type:
            filters.add("edge_type = $X", self._canonicalize_edge_type(edge_type))
        if db_name:
            filters.add("db_name = $X", db_name)
        if branch:
            filters.add("branch = $X", branch)
        if since:
            filters.add("occurred_at >= $X", since)
        if until:
            filters.add("occurred_at <= $X", until)

        where = filters.where()

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT COUNT(*)::bigint AS c FROM {self._schema}.lineage_edges{where}",
                *filters.params,
            )
        return int(row["c"] or 0) if row else 0

    @classmethod
    def _run_id_predicate(cls) -> str:
        return "(run_id = $X OR metadata->>'producer_run_id' = $X OR metadata->>'run_id' = $X)"

    async def list_edges(
        self,
        *,
        edge_type: Optional[str] = None,
        projection_name: Optional[str] = None,
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
        run_id: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 5000,
    ) -> List[Dict[str, Any]]:
        """
        List lineage edges in reverse chronological order for timeline/ops analysis.
        """
        if not self._pool:
            await self.connect()

        limit = max(1, min(int(limit), 20000))
        filters = SqlFilterBuilder()

        if edge_type:
            filters.add("edge_type = $X", self._canonicalize_edge_type(edge_type))
        if projection_name is not None:
            filters.add("projection_name = $X", projection_name)
        if db_name:
            filters.add("db_name = $X", db_name)
        if branch:
            filters.add("branch = $X", branch)
        if run_id:
            filters.add(self._run_id_predicate(), run_id)
        if since:
            filters.add("occurred_at >= $X", since)
        if until:
            filters.add("occurred_at <= $X", until)

        where = filters.where()
        limit_param = len(filters.params) + 1

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT edge_id, from_node_id, to_node_id, edge_type, projection_name,
                       occurred_at, recorded_at, metadata
                FROM {self._schema}.lineage_edges{where}
                ORDER BY occurred_at DESC, recorded_at DESC
                LIMIT ${limit_param}
                """,
                *filters.params,
                limit,
            )

        items: List[Dict[str, Any]] = []
        for row in rows:
            occurred_at = row["occurred_at"]
            occurred_at = occurred_at if occurred_at and occurred_at.tzinfo else (
                occurred_at.replace(tzinfo=timezone.utc) if occurred_at else None
            )
            recorded_at = row["recorded_at"]
            recorded_at = recorded_at if recorded_at and recorded_at.tzinfo else (
                recorded_at.replace(tzinfo=timezone.utc) if recorded_at else None
            )
            items.append(
                {
                    "edge_id": str(row["edge_id"]),
                    "from_node_id": self._canonicalize_node_id(str(row["from_node_id"])),
                    "to_node_id": self._canonicalize_node_id(str(row["to_node_id"])),
                    "edge_type": self._canonicalize_edge_type(str(row["edge_type"])),
                    "projection_name": str(row["projection_name"] or ""),
                    "occurred_at": occurred_at,
                    "recorded_at": recorded_at,
                    "metadata": self._coerce_metadata(row["metadata"]),
                }
            )
        return items

    async def list_run_summaries(
        self,
        *,
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
        edge_type: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Aggregate lineage activity by run/build id.
        """
        if not self._pool:
            await self.connect()

        limit = max(1, min(int(limit), 5000))
        filters = SqlFilterBuilder()

        if db_name:
            filters.add("db_name = $X", db_name)
        if branch:
            filters.add("branch = $X", branch)
        if edge_type:
            filters.add("edge_type = $X", self._canonicalize_edge_type(edge_type))
        if since:
            filters.add("occurred_at >= $X", since if since.tzinfo else since.replace(tzinfo=timezone.utc))
        if until:
            filters.add("occurred_at <= $X", until if until.tzinfo else until.replace(tzinfo=timezone.utc))

        inner_where = filters.where()
        limit_param = len(filters.params) + 1

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT run_id,
                       MIN(occurred_at) AS first_occurred_at,
                       MAX(occurred_at) AS last_occurred_at,
                       COUNT(*)::bigint AS edge_count,
                       COUNT(DISTINCT to_node_id) FILTER (WHERE to_node_id LIKE 'artifact:%')::bigint AS impacted_artifact_count,
                       COUNT(DISTINCT projection_name) FILTER (WHERE projection_name <> '')::bigint AS impacted_projection_count
                FROM (
                    SELECT occurred_at,
                           to_node_id,
                           projection_name,
                           COALESCE(NULLIF(run_id, ''), NULLIF(metadata->>'producer_run_id', ''), NULLIF(metadata->>'run_id', '')) AS run_id
                    FROM {self._schema}.lineage_edges{inner_where}
                ) t
                WHERE run_id IS NOT NULL
                GROUP BY run_id
                ORDER BY last_occurred_at DESC NULLS LAST, run_id ASC
                LIMIT ${limit_param}
                """,
                *filters.params,
                limit,
            )

        items: List[Dict[str, Any]] = []
        for row in rows:
            first_at = row["first_occurred_at"]
            first_at = first_at if first_at and first_at.tzinfo else (
                first_at.replace(tzinfo=timezone.utc) if first_at else None
            )
            last_at = row["last_occurred_at"]
            last_at = last_at if last_at and last_at.tzinfo else (
                last_at.replace(tzinfo=timezone.utc) if last_at else None
            )
            items.append(
                {
                    "run_id": str(row["run_id"]),
                    "first_occurred_at": first_at,
                    "last_occurred_at": last_at,
                    "edge_count": int(row["edge_count"] or 0),
                    "impacted_artifact_count": int(row["impacted_artifact_count"] or 0),
                    "impacted_projection_count": int(row["impacted_projection_count"] or 0),
                }
            )
        return items

    async def list_artifact_latest_writes(
        self,
        *,
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
        artifact_kind: Optional[str] = None,
        as_of: Optional[datetime] = None,
        limit: int = 5000,
    ) -> List[Dict[str, Any]]:
        """
        Return latest write timestamp per artifact node for freshness diagnostics.
        """
        if not self._pool:
            await self.connect()

        limit = max(1, min(int(limit), 20000))
        params: List[Any] = []
        node_filters: List[str] = ["n.node_type = 'artifact'"]
        edge_filters: List[str] = []

        if db_name:
            params.append(db_name)
            node_filters.append(f"n.db_name = ${len(params)}")
            edge_filters.append(f"e.db_name = ${len(params)}")
        if branch:
            params.append(branch)
            node_filters.append(f"n.branch = ${len(params)}")
            edge_filters.append(f"e.branch = ${len(params)}")
        if artifact_kind:
            params.append(artifact_kind)
            node_filters.append(f"n.artifact_kind = ${len(params)}")
        if as_of:
            params.append(as_of if as_of.tzinfo else as_of.replace(tzinfo=timezone.utc))
            edge_filters.append(f"e.occurred_at <= ${len(params)}")

        node_where = " AND ".join(node_filters)
        edge_where = " AND ".join(edge_filters)
        join_condition = "n.node_id = e.to_node_id"
        if edge_where:
            join_condition = f"{join_condition} AND {edge_where}"

        limit_param = len(params) + 1
        query = f"""
            SELECT n.node_id,
                   n.label,
                   n.artifact_kind,
                   n.metadata AS node_metadata,
                   MAX(e.occurred_at) AS last_occurred_at,
                   COUNT(e.edge_id)::bigint AS write_count
            FROM {self._schema}.lineage_nodes n
            LEFT JOIN {self._schema}.lineage_edges e
              ON {join_condition}
            WHERE {node_where}
            GROUP BY n.node_id, n.label, n.artifact_kind, n.metadata
            ORDER BY last_occurred_at DESC NULLS LAST, n.node_id ASC
            LIMIT ${limit_param}
        """

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params, limit)

        items: List[Dict[str, Any]] = []
        for row in rows:
            last_occurred_at = row["last_occurred_at"]
            last_occurred_at = last_occurred_at if last_occurred_at and last_occurred_at.tzinfo else (
                last_occurred_at.replace(tzinfo=timezone.utc) if last_occurred_at else None
            )
            node_id = self._canonicalize_node_id(str(row["node_id"]))
            artifact_kind_value = row["artifact_kind"] or self._parse_node_id(node_id).get("artifact_kind")
            items.append(
                {
                    "node_id": node_id,
                    "label": row["label"],
                    "artifact_kind": artifact_kind_value,
                    "last_occurred_at": last_occurred_at,
                    "write_count": int(row["write_count"] or 0),
                    "metadata": self._coerce_metadata(row["node_metadata"]),
                }
            )
        return items

    async def list_projection_latest_writes(
        self,
        *,
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
        as_of: Optional[datetime] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Return latest write timestamp per projection for freshness diagnostics.
        """
        if not self._pool:
            await self.connect()

        limit = max(1, min(int(limit), 5000))
        filters = SqlFilterBuilder(clauses=["projection_name <> ''"], params=[])
        if db_name:
            filters.add("db_name = $X", db_name)
        if branch:
            filters.add("branch = $X", branch)
        if as_of:
            filters.add("occurred_at <= $X", as_of if as_of.tzinfo else as_of.replace(tzinfo=timezone.utc))

        where = filters.join()
        limit_param = len(filters.params) + 1
        query = f"""
            SELECT projection_name,
                   MAX(occurred_at) AS last_occurred_at,
                   COUNT(*)::bigint AS edge_count
            FROM {self._schema}.lineage_edges
            WHERE {where}
            GROUP BY projection_name
            ORDER BY last_occurred_at DESC NULLS LAST, projection_name ASC
            LIMIT ${limit_param}
        """

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *filters.params, limit)

        items: List[Dict[str, Any]] = []
        for row in rows:
            last_occurred_at = row["last_occurred_at"]
            last_occurred_at = last_occurred_at if last_occurred_at and last_occurred_at.tzinfo else (
                last_occurred_at.replace(tzinfo=timezone.utc) if last_occurred_at else None
            )
            items.append(
                {
                    "projection_name": str(row["projection_name"] or ""),
                    "last_occurred_at": last_occurred_at,
                    "edge_count": int(row["edge_count"] or 0),
                }
            )
        return items

    async def get_latest_edges_to(
        self,
        *,
        to_node_ids: Sequence[str],
        edge_type: Optional[str] = None,
        projection_name: Optional[str] = None,
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch the latest edge (by occurred_at) for each `to_node_id`.

        This is useful for provenance metadata in read APIs:
        - last event that wrote a graph record
        - last event that materialized an ES document
        """
        if not self._pool:
            await self.connect()

        ids = [self._canonicalize_node_id(i.strip()) for i in to_node_ids if isinstance(i, str) and i.strip()]
        if not ids:
            return {}
        lookup_ids = list(dict.fromkeys(ids))

        max_ids = int(get_settings().performance.lineage_latest_edges_max_ids)
        if len(ids) > max_ids:
            raise ValueError(f"too many node ids (max={max_ids})")

        filters = SqlFilterBuilder(
            clauses=["to_node_id = ANY($1::text[])"],
            params=[lookup_ids],
        )

        if edge_type:
            filters.add("edge_type = $X", self._canonicalize_edge_type(edge_type))
        if projection_name is not None:
            filters.add("projection_name = $X", projection_name)
        if db_name:
            filters.add("db_name = $X", db_name)
        if branch:
            filters.add("branch = $X", branch)

        where = filters.join()

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT DISTINCT ON (to_node_id)
                       to_node_id, from_node_id, edge_type, projection_name,
                       occurred_at, recorded_at, run_id, code_sha, metadata
                FROM {self._schema}.lineage_edges
                WHERE {where}
                ORDER BY to_node_id, occurred_at DESC, recorded_at DESC
                """,
                *filters.params,
            )

        latest: Dict[str, Dict[str, Any]] = {}
        latest_sort_keys: Dict[str, tuple[datetime, datetime]] = {}
        for row in rows:
            to_node_id = self._canonicalize_node_id(str(row["to_node_id"]))
            from_node_id = self._canonicalize_node_id(str(row["from_node_id"]))
            normalized_edge_type = self._canonicalize_edge_type(str(row["edge_type"]))
            event_id = (
                from_node_id[len("event:") :]
                if isinstance(from_node_id, str) and from_node_id.startswith("event:")
                else from_node_id
            )
            occurred_at = row["occurred_at"]
            occurred_at = occurred_at if occurred_at and occurred_at.tzinfo else (
                occurred_at.replace(tzinfo=timezone.utc) if occurred_at else None
            )
            recorded_at = row["recorded_at"]
            recorded_at = recorded_at if recorded_at and recorded_at.tzinfo else (
                recorded_at.replace(tzinfo=timezone.utc) if recorded_at else None
            )
            occurred_sort = occurred_at or datetime.min.replace(tzinfo=timezone.utc)
            recorded_sort = recorded_at or datetime.min.replace(tzinfo=timezone.utc)
            sort_key = (occurred_sort, recorded_sort)
            existing_sort = latest_sort_keys.get(str(to_node_id))
            if existing_sort and existing_sort >= sort_key:
                continue
            latest_sort_keys[str(to_node_id)] = sort_key
            latest[str(to_node_id)] = {
                "from_node_id": str(from_node_id),
                "event_id": str(event_id) if event_id is not None else None,
                "edge_type": str(normalized_edge_type),
                "projection_name": str(row["projection_name"] or ""),
                "occurred_at": occurred_at.isoformat() if occurred_at else None,
                "run_id": str(row.get("run_id")) if row.get("run_id") is not None else None,
                "code_sha": str(row.get("code_sha")) if row.get("code_sha") is not None else None,
                "metadata": self._coerce_metadata(row["metadata"]),
            }
        return latest

    async def get_latest_edges_from(
        self,
        *,
        from_node_ids: Sequence[str],
        edge_type: Optional[str] = None,
        projection_name: Optional[str] = None,
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch the latest edge (by occurred_at) for each `from_node_id`.

        This helps classify stale resources by whether their direct parent has
        newer updates (parent stale) or only upstream ancestors are newer.
        """
        if not self._pool:
            await self.connect()

        ids = [self._canonicalize_node_id(i.strip()) for i in from_node_ids if isinstance(i, str) and i.strip()]
        if not ids:
            return {}
        lookup_ids = list(dict.fromkeys(ids))

        max_ids = int(get_settings().performance.lineage_latest_edges_max_ids)
        if len(ids) > max_ids:
            raise ValueError(f"too many node ids (max={max_ids})")

        filters = SqlFilterBuilder(
            clauses=["from_node_id = ANY($1::text[])"],
            params=[lookup_ids],
        )
        if edge_type:
            filters.add("edge_type = $X", self._canonicalize_edge_type(edge_type))
        if projection_name is not None:
            filters.add("projection_name = $X", projection_name)
        if db_name:
            filters.add("db_name = $X", db_name)
        if branch:
            filters.add("branch = $X", branch)
        where = filters.join()

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT DISTINCT ON (from_node_id)
                       from_node_id, to_node_id, edge_type, projection_name,
                       occurred_at, recorded_at, run_id, code_sha, metadata
                FROM {self._schema}.lineage_edges
                WHERE {where}
                ORDER BY from_node_id, occurred_at DESC, recorded_at DESC
                """,
                *filters.params,
            )

        latest: Dict[str, Dict[str, Any]] = {}
        latest_sort_keys: Dict[str, tuple[datetime, datetime]] = {}
        for row in rows:
            from_node_id = self._canonicalize_node_id(str(row["from_node_id"]))
            to_node_id = self._canonicalize_node_id(str(row["to_node_id"]))
            normalized_edge_type = self._canonicalize_edge_type(str(row["edge_type"]))
            occurred_at = row["occurred_at"]
            occurred_at = occurred_at if occurred_at and occurred_at.tzinfo else (
                occurred_at.replace(tzinfo=timezone.utc) if occurred_at else None
            )
            recorded_at = row["recorded_at"]
            recorded_at = recorded_at if recorded_at and recorded_at.tzinfo else (
                recorded_at.replace(tzinfo=timezone.utc) if recorded_at else None
            )
            occurred_sort = occurred_at or datetime.min.replace(tzinfo=timezone.utc)
            recorded_sort = recorded_at or datetime.min.replace(tzinfo=timezone.utc)
            sort_key = (occurred_sort, recorded_sort)
            existing_sort = latest_sort_keys.get(str(from_node_id))
            if existing_sort and existing_sort >= sort_key:
                continue
            latest_sort_keys[str(from_node_id)] = sort_key
            latest[str(from_node_id)] = {
                "from_node_id": str(from_node_id),
                "to_node_id": str(to_node_id),
                "edge_type": str(normalized_edge_type),
                "projection_name": str(row["projection_name"] or ""),
                "occurred_at": occurred_at.isoformat() if occurred_at else None,
                "run_id": str(row.get("run_id")) if row.get("run_id") is not None else None,
                "code_sha": str(row.get("code_sha")) if row.get("code_sha") is not None else None,
                "metadata": self._coerce_metadata(row["metadata"]),
            }
        return latest

    async def get_latest_edges_for_projections(
        self,
        *,
        projection_names: Sequence[str],
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch the latest edge (by occurred_at) for each projection.

        Useful for out-of-date diagnostics where operators need the latest writer
        context for each stale projection.
        """
        if not self._pool:
            await self.connect()

        names = [str(name).strip() for name in projection_names if isinstance(name, str) and str(name).strip()]
        if not names:
            return {}

        max_ids = int(get_settings().performance.lineage_latest_edges_max_ids)
        if len(names) > max_ids:
            raise ValueError(f"too many projection names (max={max_ids})")

        filters = SqlFilterBuilder(
            clauses=["projection_name = ANY($1::text[])"],
            params=[names],
        )
        if db_name:
            filters.add("db_name = $X", db_name)
        if branch:
            filters.add("branch = $X", branch)
        where = filters.join()

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT DISTINCT ON (projection_name)
                       projection_name, from_node_id, to_node_id, edge_type,
                       occurred_at, recorded_at, run_id, code_sha, metadata
                FROM {self._schema}.lineage_edges
                WHERE {where}
                ORDER BY projection_name, occurred_at DESC, recorded_at DESC
                """,
                *filters.params,
            )

        latest: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            projection_name = str(row["projection_name"] or "").strip()
            if not projection_name:
                continue

            from_node_id = self._canonicalize_node_id(str(row["from_node_id"]))
            to_node_id = self._canonicalize_node_id(str(row["to_node_id"]))
            edge_type = self._canonicalize_edge_type(str(row["edge_type"]))

            occurred_at = row["occurred_at"]
            occurred_at = occurred_at if occurred_at and occurred_at.tzinfo else (
                occurred_at.replace(tzinfo=timezone.utc) if occurred_at else None
            )
            metadata = self._coerce_metadata(row["metadata"])
            run_id = (
                row["run_id"]
                or (metadata.get("producer_run_id") if isinstance(metadata, dict) else None)
                or (metadata.get("run_id") if isinstance(metadata, dict) else None)
            )

            event_id = (
                from_node_id[len("event:") :]
                if isinstance(from_node_id, str) and from_node_id.startswith("event:")
                else from_node_id
            )
            latest[projection_name] = {
                "projection_name": projection_name,
                "from_node_id": str(from_node_id),
                "to_node_id": str(to_node_id),
                "event_id": str(event_id) if event_id is not None else None,
                "edge_type": str(edge_type),
                "occurred_at": occurred_at.isoformat() if occurred_at else None,
                "run_id": str(run_id) if run_id is not None else None,
                "code_sha": str(row.get("code_sha")) if row.get("code_sha") is not None else None,
                "metadata": metadata,
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
        branch: Optional[str] = None,
        projection_name: Optional[str] = None,
        run_id: Optional[str] = None,
        code_sha: Optional[str] = None,
        schema_version: Optional[str] = None,
        edge_id: Optional[UUID] = None,
    ) -> UUID:
        from_node_id = self._canonicalize_node_id(from_node_id)
        to_node_id = self._canonicalize_node_id(to_node_id)
        edge_type = self._canonicalize_edge_type(edge_type)
        ctx = self._run_context()
        run_id = run_id or ctx.get("run_id")
        code_sha = code_sha or ctx.get("code_sha")
        if edge_metadata is None:
            edge_metadata = {}
        if db_name is None and isinstance(edge_metadata, dict):
            db_name = edge_metadata.get("db_name")
        if branch is None and isinstance(edge_metadata, dict):
            branch = edge_metadata.get("branch")
        if branch is None and isinstance(from_metadata, dict):
            branch = from_metadata.get("branch")
        if branch is None and isinstance(to_metadata, dict):
            branch = to_metadata.get("branch")
        branch = branch or self._infer_branch_from_node_id(to_node_id) or self._infer_branch_from_node_id(from_node_id)
        if branch and isinstance(edge_metadata, dict) and "branch" not in edge_metadata:
            edge_metadata["branch"] = branch
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
            branch=branch,
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
            branch=branch,
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
            branch=branch,
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
        branch = envelope.data.get("branch") if isinstance(envelope.data, dict) else None
        producer_service = envelope.metadata.get("service") if isinstance(envelope.metadata, dict) else None
        producer_run_id = envelope.metadata.get("run_id") if isinstance(envelope.metadata, dict) else None
        producer_code_sha = envelope.metadata.get("code_sha") if isinstance(envelope.metadata, dict) else None
        ontology = extract_ontology_version(envelope_metadata=envelope.metadata, envelope_data=envelope.data)

        await self.record_link(
            from_node_id=agg_node,
            to_node_id=event_node,
            edge_type=EDGE_AGGREGATE_EMITTED_EVENT,
            occurred_at=occurred_at,
            from_label=f"{envelope.aggregate_type}:{envelope.aggregate_id}",
            to_label=envelope.event_type,
            db_name=db_name,
            branch=branch,
            edge_metadata={
                "event_type": envelope.event_type,
                "sequence_number": envelope.sequence_number,
                "kind": (envelope.metadata or {}).get("kind") if isinstance(envelope.metadata, dict) else None,
                "kafka_topic": (envelope.metadata or {}).get("kafka_topic") if isinstance(envelope.metadata, dict) else None,
                "db_name": db_name,
                "branch": branch,
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
                edge_type=EDGE_EVENT_STORED_IN_OBJECT_STORE,
                occurred_at=occurred_at,
                to_label=f"s3://{s3_bucket}/{s3_key}",
                db_name=db_name,
                branch=branch,
                edge_metadata={
                    "bucket": s3_bucket,
                    "key": s3_key,
                    "db_name": db_name,
                    "branch": branch,
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
                    edge_type=EDGE_COMMAND_CAUSED_DOMAIN_EVENT,
                    occurred_at=occurred_at,
                    from_label="command",
                    to_label=envelope.event_type,
                    db_name=db_name,
                    branch=branch,
                    edge_metadata={
                        "command_id": str(command_id),
                        "db_name": db_name,
                        "branch": branch,
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
            return LineageStore._canonicalize_node_id(root)
        # Shorthand: treat UUID-like values as event ids.
        if len(root) >= 32 and all(c.isalnum() or c in "-_" for c in root):
            return f"event:{root}"
        return LineageStore._canonicalize_node_id(root)

    async def get_graph(
        self,
        *,
        root: str,
        direction: LineageDirection = "both",
        max_depth: int = 5,
        max_nodes: int = 500,
        max_edges: int = 2000,
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
        as_of: Optional[datetime] = None,
    ) -> LineageGraph:
        if not self._pool:
            await self.connect()

        root_node = self.normalize_root(root)
        max_depth = max(0, int(max_depth))
        max_nodes = max(1, int(max_nodes))
        max_edges = max(1, int(max_edges))
        if as_of and as_of.tzinfo is None:
            as_of = as_of.replace(tzinfo=timezone.utc)

        visited: set[str] = {root_node}
        edges_by_key: Dict[str, LineageEdge] = {}
        warnings: List[str] = []

        async def _walk(direction_local: str) -> None:
            nonlocal visited, edges_by_key, warnings

            frontier: set[str] = {root_node}
            for depth in range(max_depth):
                if not frontier:
                    return
                if len(visited) >= max_nodes:
                    warnings.append(f"node_limit_reached(max_nodes={max_nodes})")
                    return
                if len(edges_by_key) >= max_edges:
                    warnings.append(f"edge_limit_reached(max_edges={max_edges})")
                    return

                if direction_local == "downstream":
                    column = "from_node_id"
                else:
                    column = "to_node_id"

                remaining_edges = max_edges - len(edges_by_key)
                limit = max(1, min(10000, remaining_edges + 50))
                frontier_lookup = list(
                    dict.fromkeys(
                        [
                            self._canonicalize_node_id(node_id)
                            for node_id in frontier
                            if isinstance(node_id, str) and node_id
                        ]
                    )
                )

                async with self._pool.acquire() as conn:
                    params: List[Any] = [frontier_lookup, limit]
                    predicates: List[str] = [f"{column} = ANY($1::text[])"]
                    if db_name:
                        params.append(db_name)
                        predicates.append(f"db_name = ${len(params)}")
                    if branch:
                        params.append(branch)
                        predicates.append(f"branch = ${len(params)}")
                    if as_of:
                        params.append(as_of)
                        predicates.append(f"occurred_at <= ${len(params)}")
                    where_clause = " AND ".join(predicates)
                    rows = await conn.fetch(
                        f"""
                        SELECT edge_id, from_node_id, to_node_id, edge_type, projection_name, occurred_at, recorded_at, metadata
                        FROM {self._schema}.lineage_edges
                        WHERE {where_clause}
                        ORDER BY occurred_at ASC
                        LIMIT $2
                        """,
                        *params,
                    )

                next_frontier: set[str] = set()
                for row in rows:
                    from_node_id = self._canonicalize_node_id(str(row["from_node_id"]))
                    to_node_id = self._canonicalize_node_id(str(row["to_node_id"]))
                    edge_type = self._canonicalize_edge_type(str(row["edge_type"]))
                    projection_name = str(row["projection_name"] or "")
                    edge_key = "|".join([from_node_id, to_node_id, edge_type, projection_name])
                    if edge_key not in edges_by_key:
                        metadata = self._coerce_metadata(row["metadata"])
                        if projection_name and "projection_name" not in metadata:
                            metadata["projection_name"] = projection_name
                        edges_by_key[edge_key] = LineageEdge(
                            edge_id=str(row["edge_id"]),
                            from_node_id=from_node_id,
                            to_node_id=to_node_id,
                            edge_type=edge_type,
                            occurred_at=row["occurred_at"],
                            recorded_at=row["recorded_at"],
                            metadata=metadata,
                        )

                    next_node = to_node_id if direction_local == "downstream" else from_node_id
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
        node_lookup_ids = list(
            dict.fromkeys(
                [
                    self._canonicalize_node_id(node_id)
                    for node_id in visited
                    if isinstance(node_id, str) and node_id
                ]
            )
        )
        async with self._pool.acquire() as conn:
            node_rows = await conn.fetch(
                f"""
                SELECT node_id, node_type, label, created_at, recorded_at, metadata
                FROM {self._schema}.lineage_nodes
                WHERE node_id = ANY($1::text[])
                """,
                node_lookup_ids,
            )

        nodes_by_id: Dict[str, LineageNode] = {}
        for row in node_rows:
            raw_node_id = str(row["node_id"])
            node_id = self._canonicalize_node_id(raw_node_id)
            existing_node = nodes_by_id.get(node_id)
            if existing_node and existing_node.node_id == node_id and raw_node_id != node_id:
                continue
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
            edges=list(edges_by_key.values()),
            warnings=warnings,
        )


def create_lineage_store(settings: Any) -> LineageStore:
    # settings is unused; LineageStore resolves configuration via `get_settings()` when needed.
    return LineageStore()
