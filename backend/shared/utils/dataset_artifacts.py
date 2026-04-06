"""Shared helpers for dataset artifact storage layout."""

from __future__ import annotations

from shared.utils.path_utils import safe_path_segment


def dataset_artifact_prefix(*, db_name: str, dataset_id: str, dataset_name: str) -> str:
    safe_name = safe_path_segment(dataset_name)
    return f"datasets/{db_name}/{dataset_id}/{safe_name}"
