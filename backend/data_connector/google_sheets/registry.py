"""
Google Sheets Connector - Registry

Keeps track of registered sheets for polling.

Design goals:
- Redis-backed when available (durable, multi-instance friendly)
- In-memory fallback (dev/tests)
- Best-effort: registry failures should not crash the BFF
"""

from __future__ import annotations

import asyncio
import os
from typing import Dict, List, Optional

from redis.asyncio import Redis

from .models import RegisteredSheet


class GoogleSheetsRegistry:
    def __init__(self, redis_client: Optional[Redis] = None):
        self._redis = redis_client
        self._memory: Dict[str, RegisteredSheet] = {}
        self._lock = asyncio.Lock()

        prefix = (os.getenv("GOOGLE_SHEETS_REGISTRY_KEY_PREFIX") or "spice:google_sheets:sheet:").strip()
        self._key_prefix = prefix or "spice:google_sheets:sheet:"
        self._ids_key = (os.getenv("GOOGLE_SHEETS_REGISTRY_IDS_KEY") or "spice:google_sheets:sheets").strip()

    def _key(self, sheet_id: str) -> str:
        return f"{self._key_prefix}{sheet_id}"

    async def upsert(self, sheet: RegisteredSheet) -> RegisteredSheet:
        if self._redis:
            try:
                pipe = self._redis.pipeline()
                pipe.set(self._key(sheet.sheet_id), sheet.model_dump_json())
                pipe.sadd(self._ids_key, sheet.sheet_id)
                await pipe.execute()
            except Exception:
                # Fail-open to memory fallback.
                pass

        async with self._lock:
            self._memory[sheet.sheet_id] = sheet
        return sheet

    async def get(self, sheet_id: str) -> Optional[RegisteredSheet]:
        if self._redis:
            try:
                raw = await self._redis.get(self._key(sheet_id))
                if raw:
                    return RegisteredSheet.model_validate_json(raw)
            except Exception:
                pass

        async with self._lock:
            return self._memory.get(sheet_id)

    async def list_all(self) -> List[RegisteredSheet]:
        if self._redis:
            try:
                ids = await self._redis.smembers(self._ids_key)
                sheet_ids = [i.decode("utf-8") if isinstance(i, (bytes, bytearray)) else str(i) for i in ids or []]
                if sheet_ids:
                    keys = [self._key(sid) for sid in sheet_ids]
                    raws = await self._redis.mget(keys)
                    sheets: List[RegisteredSheet] = []
                    for raw in raws or []:
                        if not raw:
                            continue
                        try:
                            sheets.append(RegisteredSheet.model_validate_json(raw))
                        except Exception:
                            continue
                    if sheets:
                        return sheets
            except Exception:
                pass

        async with self._lock:
            return list(self._memory.values())

    async def list(self) -> List[RegisteredSheet]:
        return await self.list_all()

    async def list_active(self) -> List[RegisteredSheet]:
        sheets = await self.list_all()
        return [s for s in sheets if s and s.is_active]

    async def delete(self, sheet_id: str) -> bool:
        deleted = False
        if self._redis:
            try:
                pipe = self._redis.pipeline()
                pipe.delete(self._key(sheet_id))
                pipe.srem(self._ids_key, sheet_id)
                results = await pipe.execute()
                deleted = bool(results and results[0])
            except Exception:
                pass

        async with self._lock:
            if sheet_id in self._memory:
                deleted = True
                self._memory.pop(sheet_id, None)

        return deleted
