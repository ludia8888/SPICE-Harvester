from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Mapping, Optional, Sequence, List


@dataclass(frozen=True)
class DatasetSelection:
    dataset_id: Optional[str]
    dataset_name: Optional[str]
    requested_branch: str
    candidate_branches: List[str]


@dataclass(frozen=True)
class DatasetResolution:
    dataset: Optional[Any]
    version: Optional[Any]
    requested_branch: str
    resolved_branch: Optional[str]
    dataset_id: Optional[str]
    dataset_name: Optional[str]
    used_fallback: bool
    candidate_branches: List[str]


def resolve_fallback_branches(fallback_raw: Optional[str] = None) -> List[str]:
    raw = fallback_raw
    if raw is None:
        raw = os.getenv("PIPELINE_FALLBACK_BRANCHES", "main")
    if not isinstance(raw, str):
        raw = str(raw)
    branches = [item.strip() for item in raw.split(",") if item.strip()]
    if "main" not in branches:
        branches.append("main")
    return branches


def build_branch_candidates(
    requested_branch: str,
    fallback_branches: Optional[Sequence[str]] = None,
    *,
    fallback_raw: Optional[str] = None,
) -> List[str]:
    if fallback_branches is None:
        fallback_branches = resolve_fallback_branches(fallback_raw)
    candidates: List[str] = []
    for candidate in [requested_branch, *fallback_branches]:
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    return candidates


def normalize_dataset_selection(
    metadata: Mapping[str, Any],
    *,
    default_branch: str,
    fallback_raw: Optional[str] = None,
) -> DatasetSelection:
    dataset_id = metadata.get("datasetId") or metadata.get("dataset_id")
    dataset_name = metadata.get("datasetName") or metadata.get("dataset_name")
    requested_branch = (
        metadata.get("datasetBranch") or metadata.get("dataset_branch") or default_branch or "main"
    )

    dataset_id = str(dataset_id).strip() if dataset_id else None
    dataset_name = str(dataset_name).strip() if dataset_name else None
    requested_branch = str(requested_branch).strip() or "main"

    candidates = build_branch_candidates(requested_branch, fallback_raw=fallback_raw)
    return DatasetSelection(
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        requested_branch=requested_branch,
        candidate_branches=candidates,
    )


async def resolve_dataset_version(
    dataset_registry: Any,
    *,
    db_name: str,
    selection: DatasetSelection,
) -> DatasetResolution:
    dataset_id = selection.dataset_id
    dataset_name = selection.dataset_name
    requested_branch = selection.requested_branch
    candidates = selection.candidate_branches

    dataset = None
    version = None
    resolved_branch = None
    resolved_name = dataset_name
    fallback_dataset = None
    fallback_branch = None

    if dataset_id:
        dataset = await dataset_registry.get_dataset(dataset_id=str(dataset_id))
        if dataset:
            resolved_branch = getattr(dataset, "branch", None)
            resolved_name = resolved_name or getattr(dataset, "name", None)
            version = await dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)

    if (not version) and resolved_name:
        for candidate_branch in candidates:
            found = await dataset_registry.get_dataset_by_name(
                db_name=db_name,
                name=str(resolved_name),
                branch=candidate_branch,
            )
            if not found:
                continue
            found_version = await dataset_registry.get_latest_version(dataset_id=found.dataset_id)
            if found_version:
                dataset = found
                version = found_version
                resolved_branch = candidate_branch
                resolved_name = getattr(found, "name", resolved_name)
                break
            if fallback_dataset is None:
                fallback_dataset = found
                fallback_branch = candidate_branch

    if dataset is None and fallback_dataset is not None:
        dataset = fallback_dataset
        resolved_branch = fallback_branch
        resolved_name = getattr(dataset, "name", resolved_name)

    if dataset and not resolved_branch:
        resolved_branch = getattr(dataset, "branch", None)

    resolved_branch_value = resolved_branch or requested_branch
    used_fallback = bool(dataset and resolved_branch_value != requested_branch)

    return DatasetResolution(
        dataset=dataset,
        version=version,
        requested_branch=requested_branch,
        resolved_branch=resolved_branch,
        dataset_id=dataset_id,
        dataset_name=resolved_name,
        used_fallback=used_fallback,
        candidate_branches=list(candidates),
    )
