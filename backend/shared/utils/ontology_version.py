"""
Ontology (semantic contract) version helpers.

We treat "ontology_version" as a lightweight stamp (ref + commit) that can be
attached to events, lineage edges, audit logs, and projections so we can later
reproduce past decisions under the semantic contract that was used at the time.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

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
    """
    Build a minimal ontology stamp for audit + replay.

    Enterprise contract:
    - `ref` is always `branch:<name>`
    - `commit` must be present as well; when an immutable commit id isn't available,
      we fall back to the same branch ref (floating head semantics).
    """
    ref = f"branch:{branch}"
    commit_norm = str(commit).strip() if commit is not None else ""
    return {"ref": ref, "commit": commit_norm or ref}


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
    source: Any,
    *,
    db_name: str,
    branch: str,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, str]:
    """
    Resolve ontology semantic contract stamp in Foundry-style branch-ref form.

    Deprecated branch/version lookups are intentionally removed. The canonical
    contract is `ref=branch:<name>`, with commit falling back to the same ref
    when an immutable commit id is unavailable.
    """
    _ = (source, db_name, logger)
    return build_ontology_version(branch=branch, commit=None)
