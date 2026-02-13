from __future__ import annotations

import json
from typing import Any
import logging


def approx_token_count(payload: Any, *, empty_collections_as_zero: bool = False) -> int:
    if payload is None:
        return 0
    if empty_collections_as_zero and payload in ("", {}, []):
        return 0
    text = str(payload)
    text = text.strip()
    if not text:
        return 0
    return max(1, int((len(text) + 3) / 4))


def approx_token_count_json(payload: Any, *, empty_collections_as_zero: bool = False) -> int:
    if payload is None:
        return 0
    if empty_collections_as_zero and payload in ("", {}, []):
        return 0
    try:
        text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        logging.getLogger(__name__).warning("Broad exception fallback at shared/utils/token_count.py:26", exc_info=True)
        text = str(payload)
    text = text.strip()
    if not text:
        return 0
    return max(1, int((len(text) + 3) / 4))
