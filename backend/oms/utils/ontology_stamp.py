from __future__ import annotations

from typing import Any, Dict

from shared.utils.ontology_version import normalize_ontology_version


def merge_ontology_stamp(existing: Any, resolved: Dict[str, str]) -> Dict[str, str]:
    existing_norm = normalize_ontology_version(existing)
    if not existing_norm:
        return dict(resolved)

    merged = dict(existing_norm)
    if "ref" not in merged and resolved.get("ref"):
        merged["ref"] = resolved["ref"]
    if "commit" not in merged and resolved.get("commit"):
        merged["commit"] = resolved["commit"]
    return merged
