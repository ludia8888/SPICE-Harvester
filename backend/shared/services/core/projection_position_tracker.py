"""
Projection Position Tracker.

Tracks the last processed event sequence for each projection (instances,
ontologies) per database and branch. Enables:
- Resumable processing after worker restarts
- Consistency monitoring (projection lag detection)
- Palantir-style projection lifecycle management
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class ProjectionPositionTracker:
    """CRUD operations for projection_positions table."""

    def __init__(self, pool: Any) -> None:
        """pool: asyncpg connection pool."""
        self._pool = pool

    async def get_position(
        self,
        *,
        projection_name: str,
        db_name: str,
        branch: str = "main",
    ) -> Optional[Dict[str, Any]]:
        """Get the current position for a projection."""
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT projection_name, db_name, branch,
                           last_sequence, last_event_id, last_job_id, updated_at
                    FROM projection_positions
                    WHERE projection_name = $1 AND db_name = $2 AND branch = $3
                    """,
                    projection_name,
                    db_name,
                    branch,
                )
                return dict(row) if row else None
        except Exception:
            logger.debug(
                "Failed to get position for %s/%s/%s",
                projection_name, db_name, branch,
                exc_info=True,
            )
            return None

    async def update_position(
        self,
        *,
        projection_name: str,
        db_name: str,
        branch: str = "main",
        last_sequence: int,
        last_event_id: Optional[str] = None,
        last_job_id: Optional[str] = None,
    ) -> bool:
        """Update (upsert) the position for a projection."""
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO projection_positions
                        (projection_name, db_name, branch, last_sequence, last_event_id, last_job_id, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, NOW())
                    ON CONFLICT (projection_name, db_name, branch) DO UPDATE SET
                        last_sequence = GREATEST(projection_positions.last_sequence, EXCLUDED.last_sequence),
                        last_event_id = EXCLUDED.last_event_id,
                        last_job_id = EXCLUDED.last_job_id,
                        updated_at = NOW()
                    """,
                    projection_name,
                    db_name,
                    branch,
                    last_sequence,
                    last_event_id,
                    last_job_id,
                )
                return True
        except Exception:
            logger.warning(
                "Failed to update position for %s/%s/%s",
                projection_name, db_name, branch,
                exc_info=True,
            )
            return False

    async def reset_position(
        self,
        *,
        projection_name: str,
        db_name: str,
        branch: str = "main",
    ) -> bool:
        """Reset position to zero (for full rebuild)."""
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    DELETE FROM projection_positions
                    WHERE projection_name = $1 AND db_name = $2 AND branch = $3
                    """,
                    projection_name,
                    db_name,
                    branch,
                )
                return True
        except Exception:
            logger.warning(
                "Failed to reset position for %s/%s/%s",
                projection_name, db_name, branch,
                exc_info=True,
            )
            return False

    async def list_positions(
        self,
        *,
        db_name: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """List all projection positions, optionally filtered by db_name."""
        try:
            async with self._pool.acquire() as conn:
                if db_name:
                    rows = await conn.fetch(
                        """
                        SELECT * FROM projection_positions
                        WHERE db_name = $1
                        ORDER BY updated_at DESC
                        LIMIT $2
                        """,
                        db_name,
                        limit,
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT * FROM projection_positions
                        ORDER BY updated_at DESC
                        LIMIT $1
                        """,
                        limit,
                    )
                return [dict(r) for r in rows]
        except Exception:
            logger.warning("Failed to list positions", exc_info=True)
            return []

    async def compute_lag(
        self,
        *,
        projection_name: str,
        db_name: str,
        branch: str = "main",
        current_sequence: int,
    ) -> Dict[str, Any]:
        """Compute the lag between a projection position and current head."""
        pos = await self.get_position(
            projection_name=projection_name,
            db_name=db_name,
            branch=branch,
        )
        last_seq = pos["last_sequence"] if pos else 0
        lag = max(0, current_sequence - last_seq)
        return {
            "projection_name": projection_name,
            "db_name": db_name,
            "branch": branch,
            "last_sequence": last_seq,
            "current_sequence": current_sequence,
            "lag": lag,
            "healthy": lag < 1000,  # Configurable threshold
        }
