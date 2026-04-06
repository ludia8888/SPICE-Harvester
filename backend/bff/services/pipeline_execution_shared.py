"""Shared helpers for pipeline execution service modules.

This module keeps small execution-contract helpers in one place so preview,
deploy, and main execution flows do not silently drift apart.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import status

from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.services.pipeline.pipeline_scheduler import _is_valid_cron_expression


def _build_ontology_ref(branch: str) -> str:
    resolved = str(branch or "").strip() or "main"
    return f"branch:{resolved}"


def _parse_optional_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _extract_deploy_dependencies_raw(
    *,
    sanitized: Dict[str, Any],
    definition_json: Optional[Dict[str, Any]],
) -> Any:
    if "dependencies" in sanitized:
        return sanitized.get("dependencies")
    if isinstance(definition_json, dict) and "dependencies" in definition_json:
        return definition_json.get("dependencies")
    return None


def _parse_deploy_schedule_fields(
    *,
    sanitized: Dict[str, Any],
    output: Dict[str, Any],
) -> tuple[Optional[int], Optional[str]]:
    schedule_interval_seconds = None
    schedule_cron = None
    schedule = sanitized.get("schedule") or output.get("schedule")
    if isinstance(schedule, dict):
        schedule_interval_seconds = schedule.get("interval_seconds")
        schedule_cron = schedule.get("cron")
    elif isinstance(schedule, (int, float, str)):
        try:
            schedule_interval_seconds = int(schedule)
        except (TypeError, ValueError):
            schedule_interval_seconds = None
    if schedule_cron:
        schedule_cron = str(schedule_cron).strip()
    if schedule_interval_seconds is not None:
        try:
            schedule_interval_seconds = int(schedule_interval_seconds)
        except Exception:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "schedule_interval_seconds must be integer",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        if schedule_interval_seconds <= 0:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "schedule_interval_seconds must be > 0",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
    if schedule_interval_seconds and schedule_cron:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "Provide either schedule_interval_seconds or schedule_cron (not both)",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    if schedule_cron and not _is_valid_cron_expression(str(schedule_cron)):
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "schedule_cron must be a supported 5-field cron expression",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    return schedule_interval_seconds, schedule_cron
