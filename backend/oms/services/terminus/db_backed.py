"""
Database-backed Terminus service base.

Provides shared lifecycle for Terminus services that internally use
`DatabaseService` for `ensure_db_exists` checks.
"""

from __future__ import annotations

from .base import BaseTerminusService
from .database import DatabaseService


class DatabaseBackedTerminusService(BaseTerminusService):
    """Base class for Terminus services that compose a nested DatabaseService."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_service = DatabaseService(*args, **kwargs)

    async def disconnect(self) -> None:
        try:
            await self.db_service.disconnect()
        finally:
            await super().disconnect()
