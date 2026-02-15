"""
Ontology resource storage service.

Foundry-style ontology resource storage (PostgreSQL canonical backend).
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from oms.exceptions import DatabaseError, OntologyNotFoundError
from shared.config.settings import get_settings
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


RESOURCE_CLASS_ID = "__ontology_resource"
RESOURCE_DOC_PREFIX = "__ontology_resource"

RESOURCE_TYPE_ALIASES = {
    "object_type": "object_type",
    "object_types": "object_type",
    "object-type": "object_type",
    "object-types": "object_type",
    "link_type": "link_type",
    "link_types": "link_type",
    "link-type": "link_type",
    "link-types": "link_type",
    "shared_property": "shared_property",
    "shared_properties": "shared_property",
    "shared-property": "shared_property",
    "shared-properties": "shared_property",
    "value_type": "value_type",
    "value_types": "value_type",
    "value-type": "value_type",
    "value-types": "value_type",
    "interface": "interface",
    "interfaces": "interface",
    "group": "group",
    "groups": "group",
    "function": "function",
    "functions": "function",
    "action_type": "action_type",
    "action_types": "action_type",
    "action-type": "action_type",
    "action-types": "action_type",
}


def normalize_resource_type(value: str) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        raise ValueError("resource_type is required")
    normalized = RESOURCE_TYPE_ALIASES.get(raw)
    if not normalized:
        raise ValueError(f"Unsupported resource_type: {value}")
    return normalized


def _resource_doc_id(resource_type: str, resource_id: str) -> str:
    # Stable resource identifier format for registry records.
    return f"{RESOURCE_DOC_PREFIX}/{resource_type}:{resource_id}"


def _localized_to_string(value: Any) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
    if value is None:
        return None, None
    if isinstance(value, str):
        text = value.strip()
        return text or None, None
    if isinstance(value, dict):
        lang_map: Dict[str, str] = {}
        for k, v in value.items():
            if v is None:
                continue
            v_str = str(v).strip()
            if not v_str:
                continue
            lang_map[str(k)] = v_str
        if not lang_map:
            return None, None
        for key in ("en", "ko"):
            if key in lang_map:
                return lang_map[key], lang_map
        return next(iter(lang_map.values())), lang_map
    text = str(value).strip()
    return text or None, None


def _normalize_backend(value: Optional[str]) -> str:
    _ = value
    return "postgres"


def _json_like_to_dict(value: Any, *, default: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    if value is None:
        return default
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return default
        try:
            parsed = json.loads(raw)
        except Exception:
            logging.getLogger(__name__).warning(
                "Broad exception fallback at oms/services/ontology_resources.py:117",
                exc_info=True,
            )
            return default
        if isinstance(parsed, dict):
            return parsed
        return default
    return default


def _to_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    raw = str(value).strip()
    return raw or None


def _to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    raw = str(value or "").strip()
    if not raw:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        logging.getLogger(__name__).warning(
            "Broad exception fallback at oms/services/ontology_resources.py:148",
            exc_info=True,
        )
        return datetime.fromtimestamp(0, tz=timezone.utc)


class OntologyResourceService:
    """CRUD for ontology resources on PostgreSQL canonical backend."""

    _pg_pools: Dict[str, asyncpg.Pool] = {}
    _pg_pool_lock: Optional[asyncio.Lock] = None

    def __init__(
        self,
        legacy_adapter: Optional[Any] = None,
        *,
        backend: Optional[str] = None,
        postgres_url: Optional[str] = None,
        pool_min: int = 1,
        pool_max: int = 5,
    ):
        settings = get_settings()

        _ = backend
        self.backend = _normalize_backend(settings.ontology.resource_storage_backend)
        _ = legacy_adapter

        self._postgres_url = str(postgres_url or settings.database.postgres_url).strip()
        self._pool_min = max(1, int(pool_min))
        self._pool_max = max(self._pool_min, int(pool_max))

    @classmethod
    async def _get_pool(
        cls,
        *,
        postgres_url: str,
        pool_min: int,
        pool_max: int,
    ) -> asyncpg.Pool:
        pool = cls._pg_pools.get(postgres_url)
        if pool:
            return pool

        if cls._pg_pool_lock is None:
            cls._pg_pool_lock = asyncio.Lock()

        async with cls._pg_pool_lock:
            pool = cls._pg_pools.get(postgres_url)
            if pool:
                return pool
            pool = await asyncpg.create_pool(
                postgres_url,
                min_size=pool_min,
                max_size=pool_max,
                command_timeout=60,
            )
            cls._pg_pools[postgres_url] = pool
            return pool

    async def _ensure_pool(self) -> asyncpg.Pool:
        if not self._postgres_url:
            raise RuntimeError("PostgreSQL URL is not configured for ontology resources")
        return await self._get_pool(
            postgres_url=self._postgres_url,
            pool_min=self._pool_min,
            pool_max=self._pool_max,
        )

    async def _ensure_postgres_schema(self) -> None:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ontology_resources (
                    db_name TEXT NOT NULL,
                    branch TEXT NOT NULL,
                    resource_type TEXT NOT NULL,
                    resource_id TEXT NOT NULL,
                    label TEXT NOT NULL,
                    description TEXT,
                    label_i18n JSONB,
                    description_i18n JSONB,
                    spec JSONB NOT NULL DEFAULT '{}'::jsonb,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    version BIGINT NOT NULL DEFAULT 1,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (db_name, branch, resource_type, resource_id)
                );
                """
            )
            await conn.execute(
                """
                ALTER TABLE ontology_resources
                ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ontology_resources_lookup
                ON ontology_resources (db_name, branch, resource_type, updated_at DESC);
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ontology_resource_versions (
                    db_name TEXT NOT NULL,
                    branch TEXT NOT NULL,
                    resource_type TEXT NOT NULL,
                    resource_id TEXT NOT NULL,
                    version BIGINT NOT NULL,
                    operation TEXT NOT NULL,
                    snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (db_name, branch, resource_type, resource_id, version)
                );
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ontology_resource_versions_lookup
                ON ontology_resource_versions (
                    db_name,
                    branch,
                    resource_type,
                    resource_id,
                    version DESC
                );
                """
            )

    @trace_external_call("oms.ontology_resources.ensure_resource_schema")
    async def ensure_resource_schema(self, db_name: str, *, branch: str) -> None:
        _ = (db_name, branch)
        await self._ensure_postgres_schema()

    @trace_external_call("oms.ontology_resources.create_resource")
    async def create_resource(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: str,
        resource_id: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        normalized_type = normalize_resource_type(resource_type)
        await self._ensure_postgres_schema()
        existing = await self.get_resource(
            db_name,
            branch=branch,
            resource_type=normalized_type,
            resource_id=resource_id,
        )
        if existing:
            raise DatabaseError(f"Resource '{resource_id}' already exists")

        doc_id = _resource_doc_id(normalized_type, resource_id)
        doc = self._payload_to_document(normalized_type, resource_id, payload, doc_id=doc_id, is_create=True)
        persisted = await self._insert_resource_document_postgres(
            db_name=db_name,
            branch=branch,
            doc=doc,
        )
        return self._document_to_payload(persisted)

    @trace_external_call("oms.ontology_resources.update_resource")
    async def update_resource(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: str,
        resource_id: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        normalized_type = normalize_resource_type(resource_type)
        await self._ensure_postgres_schema()
        existing = await self._get_resource_document_postgres(
            db_name=db_name,
            branch=branch,
            resource_type=normalized_type,
            resource_id=resource_id,
        )
        if not existing:
            raise OntologyNotFoundError(f"Resource '{resource_id}' not found")

        doc_id = _resource_doc_id(normalized_type, resource_id)
        doc = self._payload_to_document(
            normalized_type,
            resource_id,
            payload,
            doc_id=doc_id,
            is_create=False,
            existing=existing,
        )
        persisted = await self._upsert_resource_document_postgres(
            db_name=db_name,
            branch=branch,
            doc=doc,
        )
        return self._document_to_payload(persisted)

    @trace_external_call("oms.ontology_resources.delete_resource")
    async def delete_resource(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: str,
        resource_id: str,
    ) -> None:
        normalized_type = normalize_resource_type(resource_type)
        await self._ensure_postgres_schema()
        await self._delete_resource_postgres(
            db_name=db_name,
            branch=branch,
            resource_type=normalized_type,
            resource_id=resource_id,
        )

    @trace_external_call("oms.ontology_resources.get_resource")
    async def get_resource(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: str,
        resource_id: str,
    ) -> Optional[Dict[str, Any]]:
        normalized_type = normalize_resource_type(resource_type)

        await self._ensure_postgres_schema()
        doc = await self._get_resource_document_postgres(
            db_name=db_name,
            branch=branch,
            resource_type=normalized_type,
            resource_id=resource_id,
        )
        if doc:
            return self._document_to_payload(doc)
        return None

    @trace_external_call("oms.ontology_resources.list_resources")
    async def list_resources(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        normalized_type = normalize_resource_type(resource_type) if resource_type else None
        await self._ensure_postgres_schema()
        docs = await self._list_resource_documents_postgres(
            db_name=db_name,
            branch=branch,
            resource_type=normalized_type,
            limit=limit,
            offset=offset,
        )
        return [self._document_to_payload(doc) for doc in docs]

    async def _insert_resource_document_postgres(
        self,
        *,
        db_name: str,
        branch: str,
        doc: Dict[str, Any],
    ) -> Dict[str, Any]:
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow(
                        """
                        INSERT INTO ontology_resources (
                            db_name,
                            branch,
                            resource_type,
                            resource_id,
                            label,
                            description,
                            label_i18n,
                            description_i18n,
                            spec,
                            metadata,
                            version,
                            created_at,
                            updated_at
                        )
                        VALUES (
                            $1, $2, $3, $4, $5, $6,
                            $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb,
                            $11::bigint, $12::timestamptz, $13::timestamptz
                        )
                        RETURNING
                            db_name, branch, resource_type, resource_id, label, description,
                            label_i18n, description_i18n, spec, metadata, version, created_at, updated_at
                        """,
                        str(db_name),
                        str(branch),
                        str(doc.get("resource_type") or ""),
                        str(doc.get("resource_id") or ""),
                        str(doc.get("label") or ""),
                        doc.get("description"),
                        json.dumps(doc.get("label_i18n"), ensure_ascii=False),
                        json.dumps(doc.get("description_i18n"), ensure_ascii=False),
                        json.dumps(doc.get("spec") or {}, ensure_ascii=False),
                        json.dumps(doc.get("metadata") or {}, ensure_ascii=False),
                        int(doc.get("version") or 1),
                        str(doc.get("created_at") or datetime.now(timezone.utc).isoformat()),
                        str(doc.get("updated_at") or datetime.now(timezone.utc).isoformat()),
                    )
                    if not row:
                        raise DatabaseError("Failed to create ontology resource")
                    persisted = self._row_to_document(row)
                    await self._append_resource_version_postgres(
                        conn,
                        db_name=str(db_name),
                        branch=str(branch),
                        resource_type=str(persisted.get("resource_type") or ""),
                        resource_id=str(persisted.get("resource_id") or ""),
                        version=int(persisted.get("version") or 1),
                        operation="CREATE",
                        snapshot=self._document_to_payload(persisted),
                        created_at=_to_datetime(persisted.get("updated_at")),
                    )
        except asyncpg.UniqueViolationError as exc:
            raise DatabaseError(
                f"Resource '{doc.get('resource_id')}' already exists"
            ) from exc

        return persisted

    async def _upsert_resource_document_postgres(
        self,
        *,
        db_name: str,
        branch: str,
        doc: Dict[str, Any],
    ) -> Dict[str, Any]:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    INSERT INTO ontology_resources (
                        db_name,
                        branch,
                        resource_type,
                        resource_id,
                        label,
                        description,
                        label_i18n,
                        description_i18n,
                        spec,
                        metadata,
                        version,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6,
                        $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb,
                        $11::bigint, $12::timestamptz, $13::timestamptz
                    )
                    ON CONFLICT (db_name, branch, resource_type, resource_id) DO UPDATE SET
                        label = EXCLUDED.label,
                        description = EXCLUDED.description,
                        label_i18n = EXCLUDED.label_i18n,
                        description_i18n = EXCLUDED.description_i18n,
                        spec = EXCLUDED.spec,
                        metadata = EXCLUDED.metadata,
                        version = EXCLUDED.version,
                        created_at = COALESCE(ontology_resources.created_at, EXCLUDED.created_at),
                        updated_at = EXCLUDED.updated_at
                    RETURNING
                        db_name, branch, resource_type, resource_id, label, description,
                        label_i18n, description_i18n, spec, metadata, version, created_at, updated_at
                    """,
                    str(db_name),
                    str(branch),
                    str(doc.get("resource_type") or ""),
                    str(doc.get("resource_id") or ""),
                    str(doc.get("label") or ""),
                    doc.get("description"),
                    json.dumps(doc.get("label_i18n"), ensure_ascii=False),
                    json.dumps(doc.get("description_i18n"), ensure_ascii=False),
                    json.dumps(doc.get("spec") or {}, ensure_ascii=False),
                    json.dumps(doc.get("metadata") or {}, ensure_ascii=False),
                    int(doc.get("version") or 1),
                    str(doc.get("created_at") or datetime.now(timezone.utc).isoformat()),
                    str(doc.get("updated_at") or datetime.now(timezone.utc).isoformat()),
                )
                if not row:
                    raise DatabaseError("Failed to upsert ontology resource")
                persisted = self._row_to_document(row)
                await self._append_resource_version_postgres(
                    conn,
                    db_name=str(db_name),
                    branch=str(branch),
                    resource_type=str(persisted.get("resource_type") or ""),
                    resource_id=str(persisted.get("resource_id") or ""),
                    version=int(persisted.get("version") or 1),
                    operation="UPDATE",
                    snapshot=self._document_to_payload(persisted),
                    created_at=_to_datetime(persisted.get("updated_at")),
                )
        return persisted

    async def _append_resource_version_postgres(
        self,
        conn: asyncpg.Connection,
        *,
        db_name: str,
        branch: str,
        resource_type: str,
        resource_id: str,
        version: int,
        operation: str,
        snapshot: Dict[str, Any],
        created_at: datetime,
    ) -> None:
        await conn.execute(
            """
            INSERT INTO ontology_resource_versions (
                db_name,
                branch,
                resource_type,
                resource_id,
                version,
                operation,
                snapshot,
                created_at
            )
            VALUES ($1, $2, $3, $4, $5::bigint, $6, $7::jsonb, $8::timestamptz)
            ON CONFLICT (db_name, branch, resource_type, resource_id, version) DO NOTHING
            """,
            db_name,
            branch,
            resource_type,
            resource_id,
            int(version),
            str(operation),
            json.dumps(snapshot or {}, ensure_ascii=False),
            created_at,
        )

    async def _delete_resource_postgres(
        self,
        *,
        db_name: str,
        branch: str,
        resource_type: str,
        resource_id: str,
    ) -> bool:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    DELETE FROM ontology_resources
                    WHERE db_name = $1
                      AND branch = $2
                      AND resource_type = $3
                      AND resource_id = $4
                    RETURNING
                        db_name, branch, resource_type, resource_id, label, description,
                        label_i18n, description_i18n, spec, metadata, version, created_at, updated_at
                    """,
                    str(db_name),
                    str(branch),
                    str(resource_type),
                    str(resource_id),
                )
                if row:
                    deleted = self._row_to_document(row)
                    delete_version = int(deleted.get("version") or 0) + 1
                    snapshot = self._document_to_payload(deleted)
                    snapshot["deleted"] = True
                    snapshot["deleted_at"] = datetime.now(timezone.utc).isoformat()
                    snapshot["version"] = delete_version
                    await self._append_resource_version_postgres(
                        conn,
                        db_name=str(db_name),
                        branch=str(branch),
                        resource_type=str(resource_type),
                        resource_id=str(resource_id),
                        version=delete_version,
                        operation="DELETE",
                        snapshot=snapshot,
                        created_at=datetime.now(timezone.utc),
                    )
        return row is not None

    async def _get_resource_document_postgres(
        self,
        *,
        db_name: str,
        branch: str,
        resource_type: str,
        resource_id: str,
    ) -> Optional[Dict[str, Any]]:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    db_name, branch, resource_type, resource_id, label, description,
                    label_i18n, description_i18n, spec, metadata, version, created_at, updated_at
                FROM ontology_resources
                WHERE db_name = $1
                  AND branch = $2
                  AND resource_type = $3
                  AND resource_id = $4
                """,
                str(db_name),
                str(branch),
                str(resource_type),
                str(resource_id),
            )
        if not row:
            return None
        return self._row_to_document(row)

    async def _list_resource_documents_postgres(
        self,
        *,
        db_name: str,
        branch: str,
        resource_type: Optional[str],
        limit: int,
        offset: int,
    ) -> List[Dict[str, Any]]:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            if resource_type:
                rows = await conn.fetch(
                    """
                    SELECT
                        db_name, branch, resource_type, resource_id, label, description,
                        label_i18n, description_i18n, spec, metadata, version, created_at, updated_at
                    FROM ontology_resources
                    WHERE db_name = $1
                      AND branch = $2
                      AND resource_type = $3
                    ORDER BY updated_at DESC, resource_type ASC, resource_id ASC
                    LIMIT $4 OFFSET $5
                    """,
                    str(db_name),
                    str(branch),
                    str(resource_type),
                    int(limit),
                    int(offset),
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT
                        db_name, branch, resource_type, resource_id, label, description,
                        label_i18n, description_i18n, spec, metadata, version, created_at, updated_at
                    FROM ontology_resources
                    WHERE db_name = $1
                      AND branch = $2
                    ORDER BY updated_at DESC, resource_type ASC, resource_id ASC
                    LIMIT $3 OFFSET $4
                    """,
                    str(db_name),
                    str(branch),
                    int(limit),
                    int(offset),
                )
        return [self._row_to_document(row) for row in rows or []]

    def _payload_to_document(
        self,
        resource_type: str,
        resource_id: str,
        payload: Dict[str, Any],
        *,
        doc_id: str,
        is_create: bool,
        existing: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        base_payload = dict(payload or {})

        label_raw = base_payload.pop("label", None)
        description_raw = base_payload.pop("description", None)

        label, label_i18n = _localized_to_string(label_raw)
        description, description_i18n = _localized_to_string(description_raw)

        spec = base_payload.pop("spec", None)
        metadata = base_payload.pop("metadata", None)

        spec_payload = spec if isinstance(spec, dict) else {}
        metadata_payload = metadata if isinstance(metadata, dict) else {}

        existing_metadata = (existing or {}).get("metadata")
        if not isinstance(existing_metadata, dict):
            existing_metadata = {}
        existing_version = 0
        try:
            existing_version = int((existing or {}).get("version") or 0)
        except (TypeError, ValueError):
            existing_version = 0

        if is_create:
            version = 1
            merged_metadata = dict(metadata_payload or {})
        else:
            version = max(1, existing_version + 1)
            merged_metadata = {**existing_metadata, **(metadata_payload or {})}

        # Keep legacy metadata revision for compatibility with existing clients.
        merged_metadata["rev"] = version

        if base_payload:
            spec_payload = {**base_payload, **spec_payload}

        now = datetime.now(timezone.utc)
        created_at = (existing or {}).get("created_at") if not is_create else None
        if not created_at:
            created_at = now.isoformat()

        doc = {
            "@id": doc_id,
            "@type": RESOURCE_CLASS_ID,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "label": label or resource_id,
            "description": description,
            "label_i18n": label_i18n,
            "description_i18n": description_i18n,
            "spec": spec_payload or None,
            "metadata": merged_metadata or None,
            "version": version,
            "created_at": created_at,
            "updated_at": now.isoformat(),
        }
        return {k: v for k, v in doc.items() if v is not None}

    def _document_to_payload(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        label_i18n = doc.get("label_i18n")
        description_i18n = doc.get("description_i18n")
        label = label_i18n if isinstance(label_i18n, dict) and label_i18n else doc.get("label")
        description = (
            description_i18n
            if isinstance(description_i18n, dict) and description_i18n
            else doc.get("description")
        )
        return {
            "resource_type": doc.get("resource_type"),
            "id": doc.get("resource_id"),
            "label": label,
            "description": description,
            "spec": doc.get("spec") or {},
            "metadata": doc.get("metadata") or {},
            "version": int(doc.get("version") or 1),
            "created_at": _to_iso(doc.get("created_at")),
            "updated_at": _to_iso(doc.get("updated_at")),
        }

    def _row_to_document(self, row: Any) -> Dict[str, Any]:
        resource_type = str(row["resource_type"])
        resource_id = str(row["resource_id"])
        metadata = _json_like_to_dict(row["metadata"], default={}) or {}
        raw_version = None
        try:
            raw_version = row["version"]
        except Exception:
            raw_version = None
        version = int(raw_version or 0)
        if version <= 0:
            try:
                version = int(metadata.get("rev") or 0)
            except (TypeError, ValueError):
                version = 0
        version = max(1, version)
        metadata["rev"] = version
        doc = {
            "@id": _resource_doc_id(resource_type, resource_id),
            "@type": RESOURCE_CLASS_ID,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "label": str(row["label"] or resource_id),
            "description": row["description"],
            "label_i18n": _json_like_to_dict(row["label_i18n"]),
            "description_i18n": _json_like_to_dict(row["description_i18n"]),
            "spec": _json_like_to_dict(row["spec"], default={}) or {},
            "metadata": metadata,
            "version": version,
            "created_at": _to_iso(row["created_at"]),
            "updated_at": _to_iso(row["updated_at"]),
        }
        return doc
