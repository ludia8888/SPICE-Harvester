"""
Pipeline worker helpers.

This module intentionally keeps the underscored helper names that are imported by
unit tests, while allowing `pipeline_worker.main` to shrink into a clearer
composition root.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from shared.config.settings import get_settings
from shared.models.pipeline_job import PipelineJob
from shared.services.pipeline.pipeline_definition_utils import resolve_execution_semantics

_SENSITIVE_CONF_TOKENS = (
    "secret",
    "password",
    "token",
    "access.key",
    "secret.key",
    "session.token",
    "aws_access",
    "aws_secret",
    "credentials",
)


def _resolve_code_version() -> Optional[str]:
    return get_settings().observability.code_sha


def _is_sensitive_conf_key(key: str) -> bool:
    lowered = str(key or "").lower()
    return any(token in lowered for token in _SENSITIVE_CONF_TOKENS)


def _resolve_lakefs_repository() -> str:
    repo = str(get_settings().storage.lakefs_artifacts_repository or "").strip()
    return repo or "pipeline-artifacts"


def _resolve_watermark_column(*, incremental: Dict[str, Any], metadata: Dict[str, Any]) -> Optional[str]:
    for key in ("watermark_column", "watermarkColumn", "watermark"):
        value = metadata.get(key) if isinstance(metadata, dict) else None
        if isinstance(value, str) and value.strip():
            return value.strip()
        value = incremental.get(key) if isinstance(incremental, dict) else None
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _max_watermark_from_snapshots(
    input_snapshots: list[dict[str, Any]], *, watermark_column: str
) -> Optional[Any]:
    resolved_column = str(watermark_column or "").strip()
    if not resolved_column:
        return None
    max_value: Optional[Any] = None
    for snapshot in input_snapshots or []:
        if not isinstance(snapshot, dict):
            continue
        if str(snapshot.get("watermark_column") or "").strip() != resolved_column:
            continue
        candidate = snapshot.get("watermark_max")
        if candidate is None:
            continue
        if max_value is None:
            max_value = candidate
            continue
        try:
            if candidate > max_value:
                max_value = candidate
        except TypeError:
            try:
                if float(candidate) > float(max_value):
                    max_value = candidate
            except Exception:
                if str(candidate) > str(max_value):
                    max_value = candidate
    return max_value


def _watermark_values_match(left: Any, right: Any) -> bool:
    if left == right:
        return True
    try:
        return float(left) == float(right)
    except Exception:
        return str(left) == str(right)


def _collect_watermark_keys_from_snapshots(
    input_snapshots: list[dict[str, Any]], *, watermark_column: str, watermark_value: Any
) -> list[str]:
    resolved_column = str(watermark_column or "").strip()
    if not resolved_column or watermark_value is None:
        return []
    keys: list[str] = []
    for snapshot in input_snapshots or []:
        if not isinstance(snapshot, dict):
            continue
        if str(snapshot.get("watermark_column") or "").strip() != resolved_column:
            continue
        candidate_value = snapshot.get("watermark_max")
        if candidate_value is None or not _watermark_values_match(candidate_value, watermark_value):
            continue
        raw_keys = snapshot.get("watermark_keys") or snapshot.get("watermarkKeys")
        if not isinstance(raw_keys, list):
            continue
        for item in raw_keys:
            item_str = str(item or "").strip()
            if item_str:
                keys.append(item_str)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in keys:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _collect_input_commit_map(input_snapshots: list[dict[str, Any]]) -> Dict[str, str]:
    commits: Dict[str, str] = {}
    for snapshot in input_snapshots or []:
        if not isinstance(snapshot, dict):
            continue
        node_id = str(snapshot.get("node_id") or "").strip()
        commit_id = str(snapshot.get("lakefs_commit_id") or "").strip()
        if node_id and commit_id:
            commits[node_id] = commit_id
    return commits


def _inputs_diff_empty(input_snapshots: list[dict[str, Any]]) -> bool:
    diff_snapshots = [
        snapshot
        for snapshot in input_snapshots or []
        if isinstance(snapshot, dict) and snapshot.get("diff_requested")
    ]
    if not diff_snapshots:
        return False
    return all(bool(snapshot.get("diff_ok")) and bool(snapshot.get("diff_empty")) for snapshot in diff_snapshots)


def _resolve_execution_semantics(*, job: PipelineJob, definition: Dict[str, Any]) -> str:
    return resolve_execution_semantics(definition, pipeline_type=str(job.pipeline_type or ""))


def _resolve_output_format(*, definition: Dict[str, Any], output_metadata: Dict[str, Any]) -> str:
    settings = definition.get("settings") if isinstance(definition.get("settings"), dict) else {}
    for key in ("outputFormat", "output_format", "format"):
        raw = output_metadata.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip().lower()
        raw = settings.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip().lower()
        raw = definition.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip().lower()
    return "parquet"


def _resolve_partition_columns(
    *,
    definition: Dict[str, Any],
    output_metadata: Dict[str, Any],
) -> List[str]:
    settings = definition.get("settings") if isinstance(definition.get("settings"), dict) else {}
    raw = (
        output_metadata.get("partitionBy")
        or output_metadata.get("partition_by")
        or settings.get("partitionBy")
        or settings.get("partition_by")
        or definition.get("partitionBy")
        or definition.get("partition_by")
    )
    if raw is None:
        return []
    if isinstance(raw, str):
        values = [item.strip() for item in raw.split(",") if item.strip()]
    elif isinstance(raw, list):
        values = [str(item).strip() for item in raw if str(item).strip()]
    else:
        return []
    seen: set[str] = set()
    deduped: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped

