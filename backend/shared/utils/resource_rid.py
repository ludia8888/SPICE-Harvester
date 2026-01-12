from __future__ import annotations

from typing import Any, Optional


def parse_metadata_rev(metadata: Any) -> int:
    if not isinstance(metadata, dict) or not metadata:
        return 1
    raw = metadata.get("rev")
    try:
        rev = int(raw)
    except (TypeError, ValueError):
        return 1
    return rev if rev >= 1 else 1


def format_resource_rid(*, resource_type: str, resource_id: str, rev: Optional[int]) -> str:
    rt = str(resource_type or "").strip()
    rid = str(resource_id or "").strip()
    if not rt or not rid:
        raise ValueError("resource_type and resource_id are required")
    r = int(rev or 1)
    if r < 1:
        r = 1
    return f"{rt}:{rid}@{r}"


def strip_rid_revision(resource_rid: str) -> str:
    """
    Extract the logical identifier portion from a RID that may include a revision suffix.

    Examples:
      - "object_type:Ticket@5" -> "Ticket"
      - "Ticket@5" -> "Ticket"
      - "Ticket" -> "Ticket"
    """
    rid = str(resource_rid or "").strip()
    if ":" in rid:
        rid = rid.split(":", 1)[1]
    if "@" in rid:
        rid = rid.split("@", 1)[0]
    return rid.strip()
