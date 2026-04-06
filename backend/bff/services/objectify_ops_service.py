"""Objectify helper utilities (BFF).

Shared objectify helper logic for router/service call sites.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from shared.utils.mapping_pairs import normalize_mapping_pair as _normalize_mapping_pair


def _build_mapping_change_summary(
    previous_mappings: List[Dict[str, Any]],
    new_mappings: List[Dict[str, Any]],
) -> Dict[str, Any]:
    previous_pairs = {pair for pair in (_normalize_mapping_pair(item) for item in previous_mappings) if pair}
    new_pairs = {pair for pair in (_normalize_mapping_pair(item) for item in new_mappings) if pair}
    added_pairs = sorted(new_pairs - previous_pairs)
    removed_pairs = sorted(previous_pairs - new_pairs)

    previous_by_target: Dict[str, set[str]] = {}
    for source, target in previous_pairs:
        previous_by_target.setdefault(target, set()).add(source)

    new_by_target: Dict[str, set[str]] = {}
    for source, target in new_pairs:
        new_by_target.setdefault(target, set()).add(source)

    changed_targets: List[Dict[str, Any]] = []
    for target in sorted(set(previous_by_target) | set(new_by_target)):
        previous_sources = previous_by_target.get(target, set())
        new_sources = new_by_target.get(target, set())
        if previous_sources != new_sources:
            changed_targets.append(
                {
                    "target_field": target,
                    "previous_sources": sorted(previous_sources),
                    "new_sources": sorted(new_sources),
                }
            )

    impacted_targets = {target for _, target in added_pairs + removed_pairs}
    impacted_targets.update(item["target_field"] for item in changed_targets)

    impacted_sources = {source for source, _ in added_pairs + removed_pairs}
    for item in changed_targets:
        impacted_sources.update(item["previous_sources"])
        impacted_sources.update(item["new_sources"])

    return {
        "added": [{"source_field": source, "target_field": target} for source, target in added_pairs],
        "removed": [{"source_field": source, "target_field": target} for source, target in removed_pairs],
        "changed_targets": changed_targets,
        "impacted_targets": sorted(impacted_targets),
        "impacted_sources": sorted(impacted_sources),
        "counts": {
            "added": len(added_pairs),
            "removed": len(removed_pairs),
            "changed_targets": len(changed_targets),
        },
        "has_changes": bool(added_pairs or removed_pairs or changed_targets),
    }
