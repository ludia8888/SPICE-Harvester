"""
Ontology (semantic contract) version helpers.

We treat "ontology_version" as a lightweight stamp (ref + commit) that can be
attached to events, lineage edges, audit logs, and projections so we can later
reproduce past decisions under the semantic contract that was used at the time.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

from shared.utils.commit_utils import coerce_commit_id


def normalize_ontology_version(value: Any) -> Optional[Dict[str, str]]:
    """
    Normalize an ontology version payload.

    Accepted shapes:
    - {"ref": "...", "commit": "..."}
    - {"ref": "..."} / {"commit": "..."} (partial, best-effort)
    """
    if not isinstance(value, dict):
        return None

    ref = value.get("ref")
    commit = value.get("commit")

    if ref is not None:
        ref = str(ref).strip() or None
    if commit is not None:
        commit = str(commit).strip() or None

    if not ref and not commit:
        return None

    out: Dict[str, str] = {}
    if ref:
        out["ref"] = ref
    if commit:
        out["commit"] = commit
    return out


def build_ontology_version(*, branch: str, commit: Optional[str]) -> Dict[str, str]:
    ref = f"branch:{branch}"
    out: Dict[str, str] = {"ref": ref}
    if commit:
        out["commit"] = str(commit).strip()
    return out


def extract_ontology_version(*, envelope_metadata: Any = None, envelope_data: Any = None) -> Optional[Dict[str, str]]:
    """
    Extract ontology_version stamp from either:
    - EventEnvelope.metadata (domain events, preferred)
    - EventEnvelope.data["metadata"] (command events)
    """
    ont = None
    if isinstance(envelope_metadata, dict):
        ont = normalize_ontology_version(envelope_metadata.get("ontology"))
    if ont:
        return ont

    if isinstance(envelope_data, dict):
        cmd_meta = envelope_data.get("metadata")
        if isinstance(cmd_meta, dict):
            return normalize_ontology_version(cmd_meta.get("ontology"))
    return None


def split_ref_commit(value: Any) -> Tuple[Optional[str], Optional[str]]:
    ont = normalize_ontology_version(value)
    if not ont:
        return None, None
    return ont.get("ref"), ont.get("commit")


async def resolve_ontology_version(
    terminus: Any,
    *,
    db_name: str,
    branch: str,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, str]:
    """
    Best-effort ontology semantic contract stamp (ref + commit).

    terminus is expected to expose version_control_service.list_branches(db_name).
    """
    commit: Optional[str] = None
    if not terminus:
        return build_ontology_version(branch=branch, commit=None)
    try:
        branches = await terminus.version_control_service.list_branches(db_name)
        for item in branches or []:
            if isinstance(item, dict) and item.get("name") == branch:
                commit = coerce_commit_id(item.get("head"))
                break
    except Exception as exc:
        if logger:
            logger.debug(f"Failed to resolve ontology version (db={db_name}, branch={branch}): {exc}")
    return build_ontology_version(branch=branch, commit=commit)
