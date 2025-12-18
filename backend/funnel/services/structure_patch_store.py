"""
In-memory store for structure-analysis patches.

This is intentionally lightweight. In production, back this with Redis/DB.
"""

from __future__ import annotations

import time
from typing import Dict, Optional, Tuple

from shared.models.structure_patch import SheetStructurePatch


# signature -> (updated_at_epoch, patch)
_PATCH_STORE: Dict[str, Tuple[float, SheetStructurePatch]] = {}


def get_patch(sheet_signature: str) -> Optional[SheetStructurePatch]:
    item = _PATCH_STORE.get(sheet_signature)
    if not item:
        return None
    return item[1]


def upsert_patch(patch: SheetStructurePatch) -> SheetStructurePatch:
    _PATCH_STORE[patch.sheet_signature] = (time.time(), patch)
    return patch


def delete_patch(sheet_signature: str) -> bool:
    return _PATCH_STORE.pop(sheet_signature, None) is not None

