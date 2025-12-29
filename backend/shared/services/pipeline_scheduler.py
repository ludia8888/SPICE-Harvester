"""
Pipeline Scheduler - periodic triggering for pipeline jobs.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from shared.services.pipeline_job_queue import PipelineJobQueue
from shared.services.pipeline_registry import PipelineRegistry
from shared.models.pipeline_job import PipelineJob
from shared.services.pipeline_control_plane_events import emit_pipeline_control_plane_event
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)
_utcnow = utcnow


@dataclass
class ScheduledPipeline:
    pipeline_id: str
    db_name: str
    output_dataset_name: str
    schedule_interval_seconds: Optional[int]
    schedule_cron: Optional[str]


@dataclass(frozen=True)
class DependencyEvaluation:
    satisfied: bool
    latest_build_at: Optional[datetime]
    details: list[dict[str, Any]]


class PipelineScheduler:
    def __init__(
        self,
        registry: PipelineRegistry,
        queue: PipelineJobQueue,
        poll_seconds: int = 30,
    ) -> None:
        self.registry = registry
        self.queue = queue
        self.poll_seconds = poll_seconds
        self._running = False

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                await self._tick()
            except Exception as exc:
                logger.error("PipelineScheduler tick failed: %s", exc)
            await asyncio.sleep(self.poll_seconds)

    async def stop(self) -> None:
        self._running = False

    async def _tick(self) -> None:
        pipelines = await self.registry.list_scheduled_pipelines()
        now = _utcnow()
        for pipeline in pipelines:
            interval = pipeline.get("schedule_interval_seconds")
            cron = pipeline.get("schedule_cron")
            last_run = pipeline.get("last_scheduled_at")
            last_build_at = pipeline.get("last_build_at")
            last_build_status = str(pipeline.get("last_build_status") or "").strip().upper()
            definition = pipeline.get("definition_json") or {}
            if not isinstance(definition, dict):
                definition = {}

            dependencies_from_registry = pipeline.get("dependencies")
            dependencies = []
            if isinstance(dependencies_from_registry, list) and dependencies_from_registry:
                dependencies = [
                    {
                        "pipeline_id": str(item.get("pipeline_id") or "").strip(),
                        "status": str(item.get("status") or "DEPLOYED").strip().upper(),
                    }
                    for item in dependencies_from_registry
                    if isinstance(item, dict) and str(item.get("pipeline_id") or "").strip()
                ]
            else:
                dependencies_present = "dependencies" in definition
                dependencies_raw = definition.get("dependencies") if dependencies_present else None
                dependencies, invalid_dep_entries = _normalize_dependencies(dependencies_raw)
                if dependencies_present and not isinstance(dependencies_raw, list):
                    await self._record_scheduler_config_error(
                        pipeline_id=str(pipeline.get("pipeline_id") or ""),
                        now=now,
                        error_key="invalid_dependencies",
                        detail="dependencies must be a list of {pipelineId|pipeline_id, status?}",
                        extra={"dependencies_raw_type": type(dependencies_raw).__name__},
                    )
                    continue
                if isinstance(dependencies_raw, list) and dependencies_raw and not dependencies:
                    await self._record_scheduler_config_error(
                        pipeline_id=str(pipeline.get("pipeline_id") or ""),
                        now=now,
                        error_key="invalid_dependencies",
                        detail="dependencies list is present but none could be parsed (expected pipelineId/pipeline_id)",
                        extra={"invalid_entries": invalid_dep_entries, "dependency_count": len(dependencies_raw)},
                    )
                    continue
                if invalid_dep_entries:
                    logger.warning(
                        "PipelineScheduler: ignored %s invalid dependency entr%s (pipeline_id=%s)",
                        invalid_dep_entries,
                        "y" if invalid_dep_entries == 1 else "ies",
                        pipeline.get("pipeline_id"),
                    )

            if cron:
                cron = str(cron).strip()
                if cron and not _is_valid_cron_expression(cron):
                    if not interval:
                        await self._record_scheduler_config_error(
                            pipeline_id=str(pipeline.get("pipeline_id") or ""),
                            now=now,
                            error_key="invalid_cron",
                            detail="schedule_cron is not supported (expected 5-field cron with *, */n, a-b, a-b/n, n)",
                            extra={"schedule_cron": cron},
                        )
                        continue
                    logger.warning(
                        "PipelineScheduler: invalid cron expression ignored (pipeline_id=%s cron=%s)",
                        pipeline.get("pipeline_id"),
                        cron,
                    )
                    cron = None

            schedule_trigger = _should_run_schedule(now, last_run, interval, cron)
            if not dependencies and not schedule_trigger:
                continue

            dependency_evaluation: Optional[DependencyEvaluation] = None
            if dependencies:
                try:
                    dependency_evaluation = await _evaluate_dependencies(self.registry, dependencies)
                except ValueError as exc:
                    await self._record_scheduler_config_error(
                        pipeline_id=str(pipeline.get("pipeline_id") or ""),
                        now=now,
                        error_key="missing_dependency_pipeline",
                        detail=str(exc),
                        extra={"dependencies": dependencies},
                    )
                    continue

            if schedule_trigger:
                if dependency_evaluation and not dependency_evaluation.satisfied:
                    await self._record_scheduler_ignored(
                        pipeline_id=str(pipeline.get("pipeline_id") or ""),
                        now=now,
                        reason="dependency_not_satisfied",
                        detail="schedule due but dependency status requirements not met",
                        extra={
                            "dependencies": dependency_evaluation.details,
                            "schedule": {"interval_seconds": interval, "cron": cron},
                        },
                    )
                    await self.registry.record_schedule_tick(pipeline_id=pipeline["pipeline_id"], scheduled_at=now)
                    continue
                if dependency_evaluation and dependency_evaluation.latest_build_at and last_build_status in {"DEPLOYED", "SUCCESS"} and last_build_at:
                    if dependency_evaluation.latest_build_at <= last_build_at:
                        await self._record_scheduler_ignored(
                            pipeline_id=str(pipeline.get("pipeline_id") or ""),
                            now=now,
                            reason="up_to_date",
                            detail="schedule due but dependencies are not newer than last pipeline build",
                            extra={
                                "dependencies_latest_build_at": dependency_evaluation.latest_build_at.isoformat(),
                                "pipeline_last_build_at": last_build_at.isoformat() if hasattr(last_build_at, "isoformat") else str(last_build_at),
                                "dependencies": dependency_evaluation.details,
                                "schedule": {"interval_seconds": interval, "cron": cron},
                            },
                        )
                        await self.registry.record_schedule_tick(pipeline_id=pipeline["pipeline_id"], scheduled_at=now)
                        continue

            dependency_trigger = False
            if dependency_evaluation and dependency_evaluation.satisfied and dependency_evaluation.latest_build_at:
                reference_time: Optional[datetime]
                if last_run and last_build_at:
                    reference_time = max(last_run, last_build_at)
                else:
                    reference_time = last_run or last_build_at
                if not reference_time or dependency_evaluation.latest_build_at > reference_time:
                    dependency_trigger = True

            if not dependency_trigger and not schedule_trigger:
                continue
            trigger_reason = "schedule" if schedule_trigger else "dependency"
            if dependency_trigger and schedule_trigger:
                trigger_reason = "schedule_and_dependency"
            job = PipelineJob(
                job_id=f"schedule-{pipeline['pipeline_id']}-{int(now.timestamp())}",
                pipeline_id=pipeline["pipeline_id"],
                db_name=pipeline["db_name"],
                pipeline_type=pipeline.get("pipeline_type") or "batch",
                definition_json=definition,
                definition_commit_id=pipeline.get("latest_commit_id"),
                output_dataset_name=pipeline.get("output_dataset_name") or "pipeline_output",
                mode="deploy",
                schedule_interval_seconds=interval,
                schedule_cron=cron,
                branch=pipeline.get("branch"),
            )
            await self.queue.publish(job)
            await emit_pipeline_control_plane_event(
                event_type="PIPELINE_SCHEDULE_TRIGGERED",
                pipeline_id=str(pipeline.get("pipeline_id") or ""),
                event_id=f"schedule-triggered-{job.job_id}",
                data={
                    "pipeline_id": str(pipeline.get("pipeline_id") or ""),
                    "job_id": job.job_id,
                    "db_name": pipeline.get("db_name"),
                    "branch": pipeline.get("branch"),
                    "trigger": trigger_reason,
                    "schedule": {"interval_seconds": interval, "cron": cron},
                    "dependencies": dependency_evaluation.details if dependency_evaluation else None,
                },
            )
            await self.registry.record_schedule_tick(pipeline_id=pipeline["pipeline_id"], scheduled_at=now)

    async def _record_scheduler_config_error(
        self,
        *,
        pipeline_id: str,
        now: datetime,
        error_key: str,
        detail: str,
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        pipeline_id = str(pipeline_id or "").strip()
        if not pipeline_id:
            logger.warning("PipelineScheduler config error (%s) without pipeline_id: %s", error_key, detail)
            return
        payload: dict[str, Any] = {"error": error_key, "detail": detail}
        if extra:
            payload["extra"] = extra
        try:
            await self.registry.record_run(
                pipeline_id=pipeline_id,
                job_id=f"schedule-config-{error_key}",
                mode="schedule",
                status="FAILED",
                output_json=payload,
                finished_at=now,
            )
        except Exception as exc:
            logger.error(
                "PipelineScheduler failed to record config error (pipeline_id=%s error=%s): %s",
                pipeline_id,
                error_key,
                exc,
            )

    async def _record_scheduler_ignored(
        self,
        *,
        pipeline_id: str,
        now: datetime,
        reason: str,
        detail: str,
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        pipeline_id = str(pipeline_id or "").strip()
        if not pipeline_id:
            logger.warning("PipelineScheduler ignored (%s) without pipeline_id: %s", reason, detail)
            return
        payload: dict[str, Any] = {"reason": reason, "detail": detail}
        if extra:
            payload["extra"] = extra
        try:
            job_id = f"schedule-ignored-{reason}-{int(now.timestamp())}"
            await self.registry.record_run(
                pipeline_id=pipeline_id,
                job_id=job_id,
                mode="schedule",
                status="IGNORED",
                output_json=payload,
                finished_at=now,
            )
            await emit_pipeline_control_plane_event(
                event_type="PIPELINE_SCHEDULE_IGNORED",
                pipeline_id=pipeline_id,
                event_id=job_id,
                data={
                    "pipeline_id": pipeline_id,
                    "job_id": job_id,
                    "reason": reason,
                    "detail": detail,
                    "extra": extra,
                },
            )
        except Exception as exc:
            logger.error(
                "PipelineScheduler failed to record ignored run (pipeline_id=%s reason=%s): %s",
                pipeline_id,
                reason,
                exc,
            )


def _should_run_schedule(
    now: datetime,
    last_run: Optional[datetime],
    interval: Optional[int],
    cron: Optional[str],
) -> bool:
    if cron:
        if last_run and last_run.replace(second=0, microsecond=0) == now.replace(second=0, microsecond=0):
            return False
        return _cron_matches(now, cron)
    if interval:
        if last_run and (now - last_run).total_seconds() < interval:
            return False
        return True
    return False


def _cron_matches(now: datetime, expression: str) -> bool:
    parts = [part.strip() for part in expression.split() if part.strip()]
    if len(parts) != 5:
        return False
    minute, hour, day, month, weekday = parts
    cron_weekday = (now.weekday() + 1) % 7
    return (
        _cron_field_matches(minute, now.minute) and
        _cron_field_matches(hour, now.hour) and
        _cron_field_matches(day, now.day) and
        _cron_field_matches(month, now.month) and
        _cron_field_matches(weekday, cron_weekday)
    )


def _cron_field_matches(field: str, value: int) -> bool:
    if field == "*":
        return True
    options = field.split(",")
    for option in options:
        if option == "*":
            return True
        if option.startswith("*/"):
            try:
                step = int(option[2:])
                if step and value % step == 0:
                    return True
            except ValueError:
                continue
        if "-" in option:
            try:
                range_part, _, step_part = option.partition("/")
                start_str, end_str = range_part.split("-", 1)
                start = int(start_str)
                end = int(end_str)
                step = int(step_part) if step_part else 1
                if start <= value <= end and (value - start) % step == 0:
                    return True
            except ValueError:
                continue
        else:
            try:
                if int(option) == value:
                    return True
            except ValueError:
                continue
    return False


def _normalize_dependencies(raw: object) -> tuple[list[dict[str, str]], int]:
    if raw is None:
        return [], 0
    if not isinstance(raw, list):
        return [], 1
    output: list[dict[str, str]] = []
    invalid = 0
    for item in raw:
        if not isinstance(item, dict):
            invalid += 1
            continue
        pipeline_id = str(item.get("pipeline_id") or item.get("pipelineId") or "").strip()
        status = str(item.get("status") or "DEPLOYED").strip().upper()
        if not pipeline_id:
            invalid += 1
            continue
        output.append({"pipeline_id": pipeline_id, "status": status})
    return output, invalid


async def _dependencies_satisfied(
    registry: PipelineRegistry,
    dependencies: list[dict[str, str]],
) -> tuple[bool, Optional[datetime]]:
    evaluation = await _evaluate_dependencies(registry, dependencies)
    return evaluation.satisfied, evaluation.latest_build_at


async def _evaluate_dependencies(
    registry: PipelineRegistry,
    dependencies: list[dict[str, str]],
) -> DependencyEvaluation:
    satisfied = True
    latest_build_at: Optional[datetime] = None
    details: list[dict[str, Any]] = []
    for dep in dependencies:
        pipeline_id = dep["pipeline_id"]
        required_status = str(dep.get("status") or "DEPLOYED").upper()
        record = await registry.get_pipeline(pipeline_id=pipeline_id)
        if not record:
            raise ValueError(f"Dependency pipeline not found: {pipeline_id}")
        last_status = (record.last_build_status or "").upper()
        last_build_at = record.last_build_at
        ok = False
        if required_status == "SUCCESS":
            ok = last_status in {"DEPLOYED", "SUCCESS"}
        else:
            ok = last_status == required_status
        if not ok:
            satisfied = False
        if last_build_at:
            latest_build_at = max(latest_build_at or last_build_at, last_build_at)
        details.append(
            {
                "pipeline_id": pipeline_id,
                "required_status": required_status,
                "last_build_status": last_status,
                "last_build_at": last_build_at.isoformat() if last_build_at else None,
            }
        )
    return DependencyEvaluation(satisfied=satisfied, latest_build_at=latest_build_at, details=details)


def _is_valid_cron_expression(expression: str) -> bool:
    parts = [part.strip() for part in str(expression or "").split() if part.strip()]
    if len(parts) != 5:
        return False
    return all(_is_valid_cron_field(part) for part in parts)


def _is_valid_cron_field(field: str) -> bool:
    if not field:
        return False
    if field == "*":
        return True
    for option in field.split(","):
        option = option.strip()
        if not option:
            return False
        if option == "*":
            continue
        if option.startswith("*/"):
            try:
                step = int(option[2:])
            except ValueError:
                return False
            if step <= 0:
                return False
            continue
        if "-" in option:
            range_part, _, step_part = option.partition("/")
            try:
                start_str, end_str = range_part.split("-", 1)
                start = int(start_str)
                end = int(end_str)
            except ValueError:
                return False
            if start > end:
                return False
            if step_part:
                try:
                    step = int(step_part)
                except ValueError:
                    return False
                if step <= 0:
                    return False
            continue
        try:
            int(option)
        except ValueError:
            return False
    return True
