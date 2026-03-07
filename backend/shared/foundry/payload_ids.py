"""Shared helpers for extracting Foundry pipeline/build identifiers from payloads."""

from __future__ import annotations

from typing import Any, Iterable, Optional, Sequence

from shared.foundry.rids import build_rid, extract_build_job_id


def _iter_payload_dicts(payload: Any, *, nested_fields: Sequence[str]) -> Iterable[dict[str, Any]]:
    if not isinstance(payload, dict):
        return
    yield payload
    for field in nested_fields:
        nested = payload.get(field)
        if isinstance(nested, dict):
            yield nested


def _extract_string_value(
    payload: Any,
    *,
    keys: Sequence[str],
    nested_fields: Sequence[str],
) -> Optional[str]:
    for container in _iter_payload_dicts(payload, nested_fields=nested_fields):
        for key in keys:
            value = str(container.get(key) or "").strip()
            if value:
                return value
    return None


def extract_pipeline_job_id(
    payload: Any,
    *,
    nested_fields: Sequence[str] = ("data", "result"),
    job_keys: Sequence[str] = ("job_id", "jobId"),
    build_ref_keys: Sequence[str] = ("build_rid", "buildRid", "rid"),
) -> Optional[str]:
    direct = _extract_string_value(payload, keys=job_keys, nested_fields=nested_fields)
    if direct:
        return direct
    for container in _iter_payload_dicts(payload, nested_fields=nested_fields):
        for key in build_ref_keys:
            job_id = extract_build_job_id(str(container.get(key) or ""))
            if job_id:
                return job_id
    return None


def extract_pipeline_id(
    payload: Any,
    *,
    nested_fields: Sequence[str] = ("data", "result"),
    keys: Sequence[str] = ("pipeline_id", "pipelineId"),
) -> Optional[str]:
    return _extract_string_value(payload, keys=keys, nested_fields=nested_fields)


def extract_build_rid(
    payload: Any,
    *,
    nested_fields: Sequence[str] = ("data", "result"),
    keys: Sequence[str] = ("build_rid", "buildRid", "rid"),
) -> Optional[str]:
    for container in _iter_payload_dicts(payload, nested_fields=nested_fields):
        for key in keys:
            job_id = extract_build_job_id(str(container.get(key) or ""))
            if job_id:
                return build_rid("build", job_id)
    return None
