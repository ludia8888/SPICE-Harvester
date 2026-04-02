from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import status

from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode, classified_http_exception
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)


def _job_enqueue_failure_payload(
    *,
    message: str,
    error: str,
    pipeline_id: str,
    job_id: str,
    node_id: Optional[str],
    mode: str,
    queued: bool | None = None,
) -> dict[str, Any]:
    context: dict[str, Any] = {
        "pipeline_id": pipeline_id,
        "job_id": job_id,
        "node_id": node_id,
        "mode": mode,
    }
    if queued is not None:
        context["queued"] = queued
    return build_error_envelope(
        service_name="bff",
        message=message,
        detail=error,
        code=ErrorCode.INTERNAL_ERROR,
        category=ErrorCategory.INTERNAL,
        status_code=500,
        errors=[error],
        context=context,
        external_code="PIPELINE_EXECUTION_FAILED",
    )


async def publish_preview_pipeline_job(
    *,
    pipeline_job_queue: Any,
    pipeline_registry: Any,
    job: Any,
    pipeline_id: str,
    node_id: Optional[str],
) -> None:
    try:
        await pipeline_job_queue.publish(job)
    except Exception as exc:
        error_payload = _job_enqueue_failure_payload(
            message="Failed to enqueue pipeline preview job",
            error=str(exc),
            pipeline_id=pipeline_id,
            job_id=job.job_id,
            node_id=node_id,
            mode="preview",
            queued=False,
        )
        await pipeline_registry.record_preview(
            pipeline_id=pipeline_id,
            status="FAILED",
            row_count=0,
            sample_json=error_payload,
            job_id=job.job_id,
            node_id=node_id,
        )
        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=job.job_id,
            mode="preview",
            status="FAILED",
            node_id=node_id,
            sample_json=error_payload,
            finished_at=utcnow(),
        )
        logger.error("Failed to enqueue pipeline preview job %s: %s", job.job_id, exc)
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Failed to enqueue preview job",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
        ) from exc


async def publish_build_pipeline_job(
    *,
    pipeline_job_queue: Any,
    pipeline_registry: Any,
    job: Any,
    pipeline_id: str,
    node_id: Optional[str],
) -> None:
    try:
        await pipeline_job_queue.publish(job)
    except Exception as exc:
        error_payload = _job_enqueue_failure_payload(
            message="Failed to enqueue pipeline build job",
            error=str(exc),
            pipeline_id=pipeline_id,
            job_id=job.job_id,
            node_id=node_id,
            mode="build",
        )
        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=job.job_id,
            mode="build",
            status="FAILED",
            node_id=node_id,
            output_json=error_payload,
            finished_at=utcnow(),
        )
        logger.error("Failed to enqueue pipeline build job %s: %s", job.job_id, exc)
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Failed to enqueue build job",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
        ) from exc
