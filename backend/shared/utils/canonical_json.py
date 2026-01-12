from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from typing import Any

CANONICAL_JSON_VERSION = "v1"


def _default(value: Any) -> Any:
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def canonical_json_dumps(value: Any) -> str:
    """
    Deterministic JSON serialization (fixed rules, versioned).

    Notes:
    - Uses sorted keys + stable separators.
    - Rejects NaN/Infinity to avoid non-portable encodings.
    - Datetimes are coerced to UTC ISO8601.
    """
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_default,
        allow_nan=False,
    )


def sha256_canonical_json(value: Any) -> str:
    body = canonical_json_dumps(value).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def sha256_canonical_json_prefixed(value: Any) -> str:
    return f"sha256:{sha256_canonical_json(value)}"

