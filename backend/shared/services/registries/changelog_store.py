"""
Objectify Changelog Store.

Persists delta summaries for each objectify job execution to Postgres.
Enables audit tracking of what changed per objectify run — a key Palantir
Foundry capability (Changelog Datasets).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)


class ChangelogStore:
    """CRUD operations for objectify_changelog table."""

    def __init__(self, pool: Any) -> None:
        """pool: asyncpg connection pool."""
        self._pool = pool

    async def record_changelog(
        self,
        *,
        job_id: str,
        db_name: str,
        branch: str = "main",
        target_class_id: str,
        execution_mode: str = "full",
        mapping_spec_id: Optional[str] = None,
        added_count: int = 0,
        modified_count: int = 0,
        deleted_count: int = 0,
        total_instances: int = 0,
        delta_summary: Optional[Dict[str, Any]] = None,
        delta_s3_key: Optional[str] = None,
        lakefs_base_commit: Optional[str] = None,
        lakefs_target_commit: Optional[str] = None,
        watermark_before: Optional[str] = None,
        watermark_after: Optional[str] = None,
        duration_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Record a changelog entry for an objectify job execution."""
        changelog_id = str(uuid4())
        delta_json = json.dumps(delta_summary) if delta_summary else None

        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO objectify_changelog (
                        changelog_id, job_id, mapping_spec_id, db_name, branch,
                        target_class_id, execution_mode,
                        added_count, modified_count, deleted_count, total_instances,
                        delta_summary, delta_s3_key,
                        lakefs_base_commit, lakefs_target_commit,
                        watermark_before, watermark_after,
                        duration_ms, created_at
                    ) VALUES (
                        $1, $2, $3, $4, $5,
                        $6, $7,
                        $8, $9, $10, $11,
                        $12::jsonb, $13,
                        $14, $15,
                        $16, $17,
                        $18, NOW()
                    )
                    """,
                    changelog_id,
                    job_id,
                    mapping_spec_id,
                    db_name,
                    branch,
                    target_class_id,
                    execution_mode,
                    added_count,
                    modified_count,
                    deleted_count,
                    total_instances,
                    delta_json,
                    delta_s3_key,
                    lakefs_base_commit,
                    lakefs_target_commit,
                    watermark_before,
                    watermark_after,
                    duration_ms,
                )
        except Exception:
            logger.warning("Failed to record changelog for job %s", job_id, exc_info=True)
            return {"changelog_id": changelog_id, "recorded": False}

        return {
            "changelog_id": changelog_id,
            "recorded": True,
            "job_id": job_id,
            "execution_mode": execution_mode,
            "added_count": added_count,
            "modified_count": modified_count,
            "deleted_count": deleted_count,
        }

    async def list_changelogs(
        self,
        *,
        db_name: str,
        branch: str = "main",
        target_class_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List recent changelogs for a database."""
        try:
            async with self._pool.acquire() as conn:
                if target_class_id:
                    rows = await conn.fetch(
                        """
                        SELECT * FROM objectify_changelog
                        WHERE db_name = $1 AND branch = $2 AND target_class_id = $3
                        ORDER BY created_at DESC
                        LIMIT $4 OFFSET $5
                        """,
                        db_name,
                        branch,
                        target_class_id,
                        limit,
                        offset,
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT * FROM objectify_changelog
                        WHERE db_name = $1 AND branch = $2
                        ORDER BY created_at DESC
                        LIMIT $3 OFFSET $4
                        """,
                        db_name,
                        branch,
                        limit,
                        offset,
                    )
                return [dict(r) for r in rows]
        except Exception:
            logger.warning("Failed to list changelogs for %s", db_name, exc_info=True)
            return []

    async def get_changelog(self, *, changelog_id: str) -> Optional[Dict[str, Any]]:
        """Get a single changelog entry by ID."""
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM objectify_changelog WHERE changelog_id = $1",
                    changelog_id,
                )
                return dict(row) if row else None
        except Exception:
            logger.warning("Failed to get changelog %s", changelog_id, exc_info=True)
            return None
