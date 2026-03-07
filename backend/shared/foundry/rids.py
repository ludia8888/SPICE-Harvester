from __future__ import annotations

from typing import Iterable, Literal, Optional, Tuple


FoundryRidKind = Literal[
    "dataset",
    "folder",
    "transaction",
    "file",
    "pipeline",
    "build",
    "job",
    "schedule",
    "connection",
    "table-import",
    "file-import",
    "virtual-table",
    "export-run",
]


_DEFAULT_ALLOWED_PREFIXES: Tuple[str, ...] = (
    "ri.foundry.main.",
    "ri.spice.main.",
)


def build_rid(kind: str, id: str) -> str:
    kind_value = str(kind or "").strip()
    id_value = str(id or "").strip()
    if not kind_value:
        raise ValueError("kind is required")
    if not id_value:
        raise ValueError("id is required")
    return f"ri.foundry.main.{kind_value}.{id_value}"


def parse_rid(
    rid: str,
    *,
    allowed_prefixes: Iterable[str] = _DEFAULT_ALLOWED_PREFIXES,
) -> tuple[str, str]:
    value = str(rid or "").strip()
    if not value:
        raise ValueError("rid is required")

    for prefix in allowed_prefixes:
        prefix_value = str(prefix or "").strip()
        if prefix_value and value.startswith(prefix_value):
            remainder = value[len(prefix_value) :].strip()
            if "." not in remainder:
                raise ValueError("rid missing kind/id segments")
            kind, raw_id = remainder.split(".", 1)
            kind = kind.strip()
            raw_id = raw_id.strip()
            if not kind or not raw_id:
                raise ValueError("rid missing kind/id segments")
            return kind, raw_id

    raise ValueError("rid prefix not allowed")


def rid_id_for_kind(
    rid: str,
    expected_kind: str,
    *,
    allowed_prefixes: Iterable[str] = _DEFAULT_ALLOWED_PREFIXES,
) -> Optional[str]:
    try:
        kind, rid_id = parse_rid(rid, allowed_prefixes=allowed_prefixes)
    except ValueError:
        return None
    if kind != str(expected_kind or "").strip():
        return None
    return rid_id


def extract_build_job_id(build_ref: str) -> Optional[str]:
    value = str(build_ref or "").strip()
    if not value:
        return None
    if value.startswith("build://"):
        job_id = value[len("build://") :].strip()
        return job_id or None
    return rid_id_for_kind(value, "build")
