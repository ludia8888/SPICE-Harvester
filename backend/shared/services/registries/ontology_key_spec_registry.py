"""
Ontology KeySpec Registry (Postgres SSoT).

Some older schema documents may discard custom per-property metadata such as
primaryKey/titleKey flags. To make ontology primary keys authoritative and
round-trip safe, we persist the ordered key spec in Postgres and overlay it
back into ontology reads.

Key: (db_name, branch, class_id) -> {primary_key: [...], title_key: [...]}
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg

from shared.config.settings import get_settings
from shared.utils.json_utils import json_default

logger = logging.getLogger(__name__)


def _normalize_str_list(values: Any) -> List[str]:
    if values is None:
        return []
    if isinstance(values, str):
        values = [v.strip() for v in values.split(",")]
    if not isinstance(values, list):
        return []
    out: List[str] = []
    seen: set[str] = set()
    for raw in values:
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        # Preserve order, avoid accidental duplicates.
        if text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out

@dataclass(frozen=True)
class OntologyKeySpec:
    db_name: str
    branch: str
    class_id: str
    primary_key: List[str]
    title_key: List[str]
    updated_at: datetime
    created_at: datetime


class OntologyKeySpecRegistry:
    """
    Persist ordered ontology key specs in Postgres.

    This is a small, highly stable registry used by:
    - ontology_worker (write path) after schema mutations succeed
    - OMS get/list ontology (read path) to overlay flags back into responses
    """

    def __init__(
        self,
        *,
        postgres_url: Optional[str] = None,
        pool_min: int = 1,
        pool_max: int = 5,
    ) -> None:
        self._postgres_url = (postgres_url or get_settings().database.postgres_url).strip()
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_min = max(1, int(pool_min))
        self._pool_max = max(self._pool_min, int(pool_max))

    async def _ensure_pool(self) -> asyncpg.Pool:
        if self._pool:
            return self._pool
        self._pool = await asyncpg.create_pool(
            self._postgres_url,
            min_size=self._pool_min,
            max_size=self._pool_max,
            command_timeout=60,
        )
        return self._pool

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def ensure_schema(self) -> None:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ontology_key_specs (
                    db_name VARCHAR(255) NOT NULL,
                    branch VARCHAR(255) NOT NULL,
                    class_id VARCHAR(255) NOT NULL,
                    primary_key JSONB NOT NULL DEFAULT '[]'::jsonb,
                    title_key JSONB NOT NULL DEFAULT '[]'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (db_name, branch, class_id)
                );
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ontology_key_specs_db_branch ON ontology_key_specs(db_name, branch);"
            )

    async def upsert_key_spec(
        self,
        *,
        db_name: str,
        branch: str,
        class_id: str,
        primary_key: Any,
        title_key: Any,
    ) -> OntologyKeySpec:
        await self.ensure_schema()
        pk = _normalize_str_list(primary_key)
        title = _normalize_str_list(title_key)
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO ontology_key_specs (db_name, branch, class_id, primary_key, title_key)
                VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
                ON CONFLICT (db_name, branch, class_id)
                DO UPDATE SET
                    primary_key = EXCLUDED.primary_key,
                    title_key = EXCLUDED.title_key,
                    updated_at = NOW()
                RETURNING db_name, branch, class_id, primary_key, title_key, created_at, updated_at;
                """,
                str(db_name),
                str(branch),
                str(class_id),
                json.dumps(pk, default=json_default),
                json.dumps(title, default=json_default),
            )
        return self._row_to_model(row)

    async def get_key_spec(
        self,
        *,
        db_name: str,
        branch: str,
        class_id: str,
    ) -> Optional[OntologyKeySpec]:
        await self.ensure_schema()
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT db_name, branch, class_id, primary_key, title_key, created_at, updated_at
                FROM ontology_key_specs
                WHERE db_name = $1 AND branch = $2 AND class_id = $3
                """,
                str(db_name),
                str(branch),
                str(class_id),
            )
        return self._row_to_model(row) if row else None

    async def delete_key_spec(
        self,
        *,
        db_name: str,
        branch: str,
        class_id: str,
    ) -> None:
        await self.ensure_schema()
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM ontology_key_specs WHERE db_name = $1 AND branch = $2 AND class_id = $3",
                str(db_name),
                str(branch),
                str(class_id),
            )

    async def get_key_spec_index(
        self,
        *,
        db_name: str,
        branch: str,
        class_ids: List[str],
    ) -> Dict[str, OntologyKeySpec]:
        """
        Fetch many key specs in a single query.

        Returns:
            {class_id: OntologyKeySpec}
        """
        normalized_ids = [str(c).strip() for c in (class_ids or []) if str(c).strip()]
        if not normalized_ids:
            return {}
        await self.ensure_schema()
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT db_name, branch, class_id, primary_key, title_key, created_at, updated_at
                FROM ontology_key_specs
                WHERE db_name = $1 AND branch = $2 AND class_id = ANY($3::text[])
                """,
                str(db_name),
                str(branch),
                normalized_ids,
            )
        out: Dict[str, OntologyKeySpec] = {}
        for row in rows or []:
            spec = self._row_to_model(row)
            out[spec.class_id] = spec
        return out

    @staticmethod
    def _row_to_model(row: Any) -> OntologyKeySpec:
        def _as_list(value: Any) -> List[str]:
            if value is None:
                return []
            if isinstance(value, list):
                return [str(v) for v in value if str(v)]
            if isinstance(value, str):
                raw = value.strip()
                if not raw:
                    return []
                try:
                    parsed = json.loads(raw)
                except Exception:
                    logging.getLogger(__name__).warning("Exception fallback at shared/services/registries/ontology_key_spec_registry.py:239", exc_info=True)
                    return []
                if isinstance(parsed, list):
                    return [str(v) for v in parsed if str(v)]
                return []
            return []

        return OntologyKeySpec(
            db_name=str(row["db_name"]),
            branch=str(row["branch"]),
            class_id=str(row["class_id"]),
            primary_key=_as_list(row["primary_key"]),
            title_key=_as_list(row["title_key"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
