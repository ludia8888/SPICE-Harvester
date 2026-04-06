"""Objectify DAG orchestration service (BFF).

Extracted from `bff.routers.objectify_dag` to keep routers thin and to isolate
the orchestration logic behind a small Facade.
"""

from __future__ import annotations

import asyncio
import heapq
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence
from uuid import uuid4

import httpx
from fastapi import HTTPException, Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.schemas.objectify_requests import RunObjectifyDAGRequest
from bff.services.database_role_guard import enforce_database_role_or_http_error
from bff.services.oms_client import OMSClient
from bff.utils.httpx_exceptions import raise_httpx_as_http_exception
from shared.models.objectify_job import ObjectifyJob
from shared.models.responses import ApiResponse
from shared.security.auth_utils import enforce_db_scope
from shared.security.database_access import DATA_ENGINEER_ROLES
from shared.security.input_sanitizer import SecurityViolationError, sanitize_input, validate_branch_name, validate_class_id
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.pipeline.pipeline_dataset_utils import build_branch_candidates
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.observability.tracing import trace_external_call
from shared.utils.ontology_fields import extract_ontology_fields
from shared.utils.object_type_backing import list_backing_sources, select_primary_backing_source
from shared.utils.payload_utils import unwrap_data_payload

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _DagPlanItem:
    class_id: str
    dataset_id: str
    dataset_branch: str
    dataset_version_id: str
    mapping_spec_id: str
    mapping_spec_version: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "class_id": self.class_id,
            "dataset_id": self.dataset_id,
            "dataset_branch": self.dataset_branch,
            "dataset_version_id": self.dataset_version_id,
            "mapping_spec_id": self.mapping_spec_id,
            "mapping_spec_version": self.mapping_spec_version,
        }


class _ObjectifyDagOrchestrator:
    def __init__(
        self,
        *,
        db_name: str,
        body: RunObjectifyDAGRequest,
        dataset_registry: DatasetRegistry,
        objectify_registry: ObjectifyRegistry,
        job_queue: ObjectifyJobQueue,
        oms_client: OMSClient,
    ) -> None:
        self.db_name = db_name
        self.dataset_registry = dataset_registry
        self.objectify_registry = objectify_registry
        self.job_queue = job_queue
        self.oms_client = oms_client

        self.branch = validate_branch_name(body.branch or "main")
        self.include_dependencies = bool(body.include_dependencies)
        self.max_depth = int(body.max_depth or 0)
        self.run_id = uuid4().hex
        self.override_options = body.options if isinstance(body.options, dict) else {}

        self.class_infos: Dict[str, Dict[str, Any]] = {}
        self.deps_by_class: Dict[str, set[str]] = defaultdict(set)
        self.dependents_by_class: Dict[str, set[str]] = defaultdict(set)
        self.missing_deps_by_class: Dict[str, set[str]] = defaultdict(set)

        self.job_ids_by_class: Dict[str, str] = {}
        self.deduped_by_class: Dict[str, bool] = {}

    async def _fetch_object_type_contract(self, class_id: str) -> Dict[str, Any]:
        try:
            resp = await self.oms_client.get_ontology_resource(
                self.db_name,
                resource_type="object_type",
                resource_id=class_id,
                branch=self.branch,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == status.HTTP_404_NOT_FOUND:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Object type contract not found",
                    code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                    extra={"class_id": class_id},
                ) from exc
            raise
        resource = unwrap_data_payload(resp)
        spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
        return {"resource": resource, "spec": spec}

    async def _resolve_mapping_spec_for_object_type(
        self,
        *,
        class_id: str,
        backing_source: Dict[str, Any],
    ) -> tuple[Any, str, str]:
        dataset_id = str(backing_source.get("dataset_id") or "").strip()
        dataset_branch = str(backing_source.get("branch") or "main").strip() or "main"
        schema_hash = str(backing_source.get("schema_hash") or backing_source.get("schemaHash") or "").strip() or None
        if not dataset_id:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Object type backing dataset is missing",
                code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                extra={"class_id": class_id},
            )

        branch_candidates = build_branch_candidates(dataset_branch)

        mapping_spec = None
        resolved_branch = dataset_branch
        for candidate in branch_candidates:
            mapping_spec = await self.objectify_registry.get_active_mapping_spec(
                dataset_id=dataset_id,
                dataset_branch=candidate,
                target_class_id=class_id,
                schema_hash=schema_hash,
            )
            if not mapping_spec and schema_hash:
                mapping_spec = await self.objectify_registry.get_active_mapping_spec(
                    dataset_id=dataset_id,
                    dataset_branch=candidate,
                    target_class_id=class_id,
                )
            if mapping_spec:
                resolved_branch = candidate
                break
        if not mapping_spec:
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Active mapping spec not found for object type contract",
                code=ErrorCode.OBJECTIFY_CONTRACT_ERROR,
                extra={
                    "class_id": class_id,
                    "dataset_id": dataset_id,
                    "dataset_branch": dataset_branch,
                    "dataset_branch_candidates": branch_candidates,
                    "schema_hash": schema_hash,
                },
            )
        return mapping_spec, dataset_id, resolved_branch

    async def _fetch_relationship_targets(self, class_id: str) -> Dict[str, str]:
        ontology_payload = await self.oms_client.get_ontology(self.db_name, class_id, branch=self.branch)
        _, rel_map = extract_ontology_fields(ontology_payload)
        targets: Dict[str, str] = {}
        for predicate, rel in rel_map.items():
            if not isinstance(rel, dict):
                continue
            target = str(
                rel.get("target")
                or rel.get("target_class")
                or rel.get("targetClass")
                or rel.get("target_class_id")
                or ""
            ).strip()
            if predicate and target:
                targets[predicate] = target
        return targets

    async def _load_class_info(self, class_id: str) -> Dict[str, Any]:
        if class_id in self.class_infos:
            return self.class_infos[class_id]

        class_id = validate_class_id(class_id)
        contract = await self._fetch_object_type_contract(class_id)
        spec = contract.get("spec") if isinstance(contract.get("spec"), dict) else {}
        backing_sources = list_backing_sources(spec)
        backing_source = select_primary_backing_source(spec)

        mapping_spec, dataset_id, dataset_branch = await self._resolve_mapping_spec_for_object_type(
            class_id=class_id,
            backing_source=backing_source,
        )

        dataset_version_id = str(backing_source.get("dataset_version_id") or "").strip() or None
        version = None
        if dataset_version_id:
            version = await self.dataset_registry.get_version(version_id=dataset_version_id)
            if not version or str(getattr(version, "dataset_id", "")) != dataset_id:
                version = None
        if not version:
            version = await self.dataset_registry.get_latest_version(dataset_id=dataset_id)
        if not version or not getattr(version, "artifact_key", None):
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Dataset version is missing for objectify DAG",
                code=ErrorCode.CONFLICT,
                extra={"class_id": class_id, "dataset_id": dataset_id},
            )

        rel_targets = await self._fetch_relationship_targets(class_id)
        info = {
            "class_id": class_id,
            "object_type": contract.get("resource") or {},
            "object_type_spec": spec,
            "backing_source": backing_source,
            "backing_sources": backing_sources,
            "dataset_id": dataset_id,
            "dataset_branch": dataset_branch,
            "dataset_version_id": str(getattr(version, "version_id", "")),
            "artifact_key": str(getattr(version, "artifact_key", "")),
            "mapping_spec": mapping_spec,
            "relationship_targets": rel_targets,
        }
        self.class_infos[class_id] = info
        return info

    async def _build_dependency_closure(self, *, start: List[str]) -> None:
        requested_set = set(start)

        queue = deque([(c, 0) for c in start])
        while queue:
            current, depth = queue.popleft()
            info = await self._load_class_info(current)
            mapping_spec = info["mapping_spec"]
            rel_targets = info["relationship_targets"]

            for mapping in (mapping_spec.mappings or []):
                if not isinstance(mapping, dict):
                    continue
                target_field = str(mapping.get("target_field") or "").strip()
                if not target_field:
                    continue
                dep_class = rel_targets.get(target_field)
                if not dep_class:
                    continue
                dep_class = validate_class_id(dep_class)
                self.deps_by_class[current].add(dep_class)
                self.dependents_by_class[dep_class].add(current)
                if self.include_dependencies:
                    if dep_class not in self.class_infos and depth < self.max_depth:
                        queue.append((dep_class, depth + 1))
                    elif dep_class not in self.class_infos and depth >= self.max_depth:
                        self.missing_deps_by_class[current].add(dep_class)
                else:
                    if dep_class not in requested_set:
                        self.missing_deps_by_class[current].add(dep_class)

        if self.missing_deps_by_class:
            missing_detail = {
                cls: sorted(deps) for cls, deps in self.missing_deps_by_class.items() if deps
            }
            if missing_detail:
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Missing dependency classes for safe objectify ordering. "
                    "Re-run with include_dependencies=true or include the dependencies in class_ids.",
                    code=ErrorCode.CONFLICT,
                    extra={
                        "missing_dependencies": missing_detail,
                        "requested_classes": sorted(requested_set),
                        "branch": self.branch,
                        "max_depth": self.max_depth,
                    },
                )

    def _toposort(self, *, start: List[str]) -> List[str]:
        all_classes = sorted(self.class_infos.keys())
        in_degree: Dict[str, int] = {c: len(self.deps_by_class.get(c, set())) for c in all_classes}

        start_order = {class_id: idx for idx, class_id in enumerate(start)}
        default_rank = len(start_order) + 1000
        priority_rank: Dict[str, int] = {c: default_rank for c in all_classes}

        rank_queue = deque()
        for class_id, rank in start_order.items():
            if class_id not in priority_rank:
                continue
            if rank < priority_rank[class_id]:
                priority_rank[class_id] = rank
                rank_queue.append(class_id)

        while rank_queue:
            node = rank_queue.popleft()
            node_rank = priority_rank.get(node, default_rank)
            for dep in self.deps_by_class.get(node, set()):
                if dep not in priority_rank:
                    continue
                if node_rank < priority_rank[dep]:
                    priority_rank[dep] = node_rank
                    rank_queue.append(dep)

        ready: list[tuple[int, str]] = []
        for class_id in all_classes:
            if in_degree.get(class_id, 0) == 0:
                heapq.heappush(ready, (priority_rank.get(class_id, default_rank), class_id))

        ordered: List[str] = []
        while ready:
            _, node = heapq.heappop(ready)
            ordered.append(node)
            for dependent in sorted(self.dependents_by_class.get(node, set())):
                if dependent not in in_degree:
                    continue
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    heapq.heappush(ready, (priority_rank.get(dependent, default_rank), dependent))

        if len(ordered) != len(all_classes):
            remaining = sorted([c for c in all_classes if in_degree.get(c, 0) > 0])
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Cycle detected in objectify dependency graph; cannot compute safe order",
                code=ErrorCode.CONFLICT,
                extra={"remaining": remaining},
            )
        return ordered

    def _build_plan(self, *, ordered: Sequence[str]) -> List[_DagPlanItem]:
        plan: List[_DagPlanItem] = []
        for class_id in ordered:
            info = self.class_infos[class_id]
            mapping_spec = info["mapping_spec"]
            plan.append(
                _DagPlanItem(
                    class_id=class_id,
                    dataset_id=info["dataset_id"],
                    dataset_branch=info["dataset_branch"],
                    dataset_version_id=info["dataset_version_id"],
                    mapping_spec_id=mapping_spec.mapping_spec_id,
                    mapping_spec_version=mapping_spec.version,
                )
            )
        return plan

    async def compute_plan(self, *, class_ids: Sequence[str]) -> tuple[List[str], List[_DagPlanItem], List[str]]:
        start = [validate_class_id(c) for c in class_ids if str(c).strip()]
        if not start:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "class_ids is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        await self._build_dependency_closure(start=start)
        ordered = self._toposort(start=start)
        return ordered, self._build_plan(ordered=ordered), start

    async def _enqueue_class_job(
        self,
        *,
        class_id: str,
        run_id: str,
        start: Sequence[str],
    ) -> str:
        info = self.class_infos[class_id]
        mapping_spec = info["mapping_spec"]
        dataset_id = info["dataset_id"]
        dataset_branch = info["dataset_branch"]
        dataset_version_id = info["dataset_version_id"]
        artifact_key = info["artifact_key"]

        dedupe_key = self.objectify_registry.build_dedupe_key(
            dataset_id=dataset_id,
            dataset_branch=dataset_branch,
            mapping_spec_id=mapping_spec.mapping_spec_id,
            mapping_spec_version=mapping_spec.version,
            dataset_version_id=dataset_version_id,
            artifact_id=None,
            artifact_output_name=mapping_spec.artifact_output_name,
        )
        job_id = str(uuid4())
        options = dict(mapping_spec.options or {})
        options.update(self.override_options)
        options.setdefault("run_id", run_id)
        options.setdefault("orchestrator", "dag")
        options.setdefault("dag_branch", self.branch)
        options.setdefault("dag_root_classes", list(start))
        options.setdefault("include_dependencies", self.include_dependencies)
        execution_mode = str(options.get("execution_mode") or "full").strip().lower() or "full"
        if execution_mode not in {"full", "incremental", "delta"}:
            execution_mode = "full"

        job = ObjectifyJob(
            job_id=job_id,
            db_name=self.db_name,
            dataset_id=dataset_id,
            dataset_version_id=dataset_version_id,
            artifact_output_name=mapping_spec.artifact_output_name,
            dedupe_key=dedupe_key,
            dataset_branch=dataset_branch,
            artifact_key=artifact_key,
            mapping_spec_id=mapping_spec.mapping_spec_id,
            mapping_spec_version=mapping_spec.version,
            target_class_id=class_id,
            ontology_branch=self.branch,
            execution_mode=execution_mode,
            max_rows=options.get("max_rows"),
            batch_size=options.get("batch_size"),
            allow_partial=bool(options.get("allow_partial")),
            options=options,
        )
        enqueue_result = await self.job_queue.publish(job, require_delivery=False)
        resolved_job_id = enqueue_result.record.job_id
        self.job_ids_by_class[class_id] = resolved_job_id
        self.deduped_by_class[class_id] = not enqueue_result.created
        return resolved_job_id

    @staticmethod
    def _extract_command_status(payload: Any) -> str:
        if not isinstance(payload, dict):
            return ""
        data = payload.get("data") if isinstance(payload.get("data"), dict) else None
        raw = payload.get("status")
        if data and data.get("status") is not None:
            raw = data.get("status")
        return str(raw or "").strip().upper()

    @staticmethod
    def _is_dataset_primary_mode(report: Dict[str, Any]) -> bool:
        write_path_mode = str(report.get("write_path_mode") or "").strip().lower()
        if not write_path_mode:
            # Dataset-primary is the only supported write path now.
            return True
        return write_path_mode == "dataset_primary_index"

    async def _wait_for_objectify_submitted(
        self,
        job_id: str,
        *,
        timeout_seconds: int = 600,
    ) -> List[str]:
        deadline = time.monotonic() + float(timeout_seconds)
        last_status: Optional[str] = None
        while time.monotonic() < deadline:
            record = await self.objectify_registry.get_objectify_job(job_id=job_id)
            if not record:
                await asyncio.sleep(0.5)
                continue
            status_value = str(record.status or "").strip().upper()
            last_status = status_value
            if status_value in {"SUBMITTED", "COMPLETED"}:
                report = record.report or {}
                if status_value == "COMPLETED" and self._is_dataset_primary_mode(report):
                    # Dataset-primary objectify commits its authoritative state in-worker.
                    # Reported command_ids are only a debug sample of instance-event files.
                    return []
                command_ids = report.get("command_ids") if isinstance(report, dict) else None
                if not isinstance(command_ids, list):
                    command_ids = []
                normalized = [str(cid).strip() for cid in command_ids if str(cid).strip()]
                if not normalized and getattr(record, "command_id", None):
                    normalized = [str(record.command_id).strip()]
                if not normalized:
                    raise RuntimeError(
                        f"Objectify job submitted without command_ids (job_id={job_id} status={status_value})"
                    )
                return normalized
            if status_value in {"FAILED", "CANCELLED"}:
                raise RuntimeError(f"Objectify job failed (job_id={job_id} error={record.error})")
            await asyncio.sleep(0.5)
        raise TimeoutError(f"Timed out waiting for objectify job submission (job_id={job_id} last={last_status})")

    async def _wait_for_command_terminal(
        self,
        command_id: str,
        *,
        timeout_seconds: int = 600,
    ) -> None:
        deadline = time.monotonic() + float(timeout_seconds)
        last_status: Optional[str] = None
        while time.monotonic() < deadline:
            try:
                payload = await self.oms_client.get(f"/api/v1/commands/{command_id}/status")
            except httpx.HTTPStatusError as exc:
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                if status_code == status.HTTP_404_NOT_FOUND:
                    await asyncio.sleep(0.5)
                    continue
                raise
            status_value = self._extract_command_status(payload)
            last_status = status_value or last_status
            if status_value == "COMPLETED":
                return
            if status_value in {"FAILED", "CANCELLED"}:
                raise RuntimeError(f"Command failed (command_id={command_id} status={status_value})")
            await asyncio.sleep(0.5)
        raise TimeoutError(f"Timed out waiting for command completion (command_id={command_id} last={last_status})")

    async def _wait_for_class_ready(self, *, class_id: str) -> None:
        job_id = self.job_ids_by_class.get(class_id)
        if not job_id:
            raise RuntimeError(f"Missing job_id for class {class_id}")
        command_ids = await self._wait_for_objectify_submitted(job_id)
        for command_id in command_ids:
            await self._wait_for_command_terminal(command_id)

    async def _run_ready_orchestrator(
        self,
        *,
        ordered: Sequence[str],
        initial_classes: List[str],
        start: Sequence[str],
    ) -> None:
        deps = {c: set(self.deps_by_class.get(c, set())) for c in ordered}
        started: set[str] = set(initial_classes)
        completed: set[str] = set()
        inflight: Dict[str, asyncio.Task] = {}

        for class_id in initial_classes:
            inflight[class_id] = asyncio.create_task(
                self._wait_for_class_ready(class_id=class_id),
                name=f"objectify-dag:{self.db_name}:{self.run_id}:{class_id}",
            )

        while len(completed) < len(ordered):
            for class_id in ordered:
                if class_id in started:
                    continue
                if deps.get(class_id, set()) <= completed:
                    await self._enqueue_class_job(class_id=class_id, run_id=self.run_id, start=start)
                    started.add(class_id)
                    inflight[class_id] = asyncio.create_task(
                        self._wait_for_class_ready(class_id=class_id),
                        name=f"objectify-dag:{self.db_name}:{self.run_id}:{class_id}",
                    )

            if not inflight:
                remaining = [c for c in ordered if c not in completed]
                raise RuntimeError(f"Objectify DAG deadlocked (remaining={remaining})")

            done, _pending = await asyncio.wait(list(inflight.values()), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                finished_class: Optional[str] = None
                for class_id, inflight_task in inflight.items():
                    if inflight_task is task:
                        finished_class = class_id
                        break
                if finished_class is None:
                    continue
                del inflight[finished_class]
                await task
                completed.add(finished_class)

    async def enqueue_plan(
        self,
        *,
        ordered: Sequence[str],
        start: Sequence[str],
    ) -> Dict[str, Any]:
        queued: List[Dict[str, Any]] = []

        initial_ready = [c for c in ordered if not self.deps_by_class.get(c)]
        if not initial_ready:
            initial_ready = [str(ordered[0])]

        for class_id in initial_ready:
            info = self.class_infos[class_id]
            mapping_spec = info["mapping_spec"]
            dataset_id = info["dataset_id"]
            dataset_version_id = info["dataset_version_id"]

            job_id = await self._enqueue_class_job(class_id=class_id, run_id=self.run_id, start=start)
            record = await self.objectify_registry.get_objectify_job(job_id=job_id)
            queued.append(
                {
                    "class_id": class_id,
                    "job_id": job_id,
                    "mapping_spec_id": mapping_spec.mapping_spec_id,
                    "dataset_id": dataset_id,
                    "dataset_version_id": dataset_version_id,
                    "status": record.status if record else "QUEUED",
                    "deduped": bool(self.deduped_by_class.get(class_id)),
                }
            )

        orchestrator_task = asyncio.create_task(
            self._run_ready_orchestrator(ordered=ordered, initial_classes=initial_ready, start=start),
            name=f"objectify-dag:{self.db_name}:{self.run_id}",
        )

        def _log_orchestrator_result(task: asyncio.Task) -> None:
            try:
                exc = task.exception()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("Objectify DAG task introspection failed: %s", exc)
                return
            if exc:
                logger.error(
                    "Objectify DAG orchestration failed (db=%s run_id=%s): %s",
                    self.db_name,
                    self.run_id,
                    exc,
                    exc_info=True,
                )
            else:
                logger.info("Objectify DAG orchestration completed (db=%s run_id=%s)", self.db_name, self.run_id)

        orchestrator_task.add_done_callback(_log_orchestrator_result)
        return {"run_id": self.run_id, "jobs": queued}


@trace_external_call("bff.objectify_dag.run_objectify_dag")
async def run_objectify_dag(
    *,
    db_name: str,
    body: RunObjectifyDAGRequest,
    request: Request,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    job_queue: ObjectifyJobQueue,
    oms_client: OMSClient,
) -> Dict[str, Any]:
    """
    Enterprise helper: enqueue multiple objectify jobs in dependency order.

    Dependency graph is derived from:
    - Ontology relationships (predicate -> target class)
    - Mapping spec mappings that target relationship predicates
    """
    try:
        db_name = sanitize_input(db_name)
        try:
            enforce_db_scope(request.headers, db_name=db_name)
        except ValueError as exc:
            raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED) from exc

        await enforce_database_role_or_http_error(
            headers=request.headers,
            db_name=db_name,
            required_roles=DATA_ENGINEER_ROLES,
        )

        orchestrator = _ObjectifyDagOrchestrator(
            db_name=db_name,
            body=body,
            dataset_registry=dataset_registry,
            objectify_registry=objectify_registry,
            job_queue=job_queue,
            oms_client=oms_client,
        )

        ordered, plan_items, start = await orchestrator.compute_plan(class_ids=body.class_ids or [])
        plan = [item.to_dict() for item in plan_items]

        if body.dry_run:
            return ApiResponse.success(
                message="Objectify DAG plan computed (dry_run)",
                data={"run_id": orchestrator.run_id, "branch": orchestrator.branch, "ordered_classes": ordered, "plan": plan},
            ).to_dict()

        enqueue_info = await orchestrator.enqueue_plan(ordered=ordered, start=start)
        return ApiResponse.success(
            message="Objectify DAG queued",
            data={
                "run_id": enqueue_info["run_id"],
                "branch": orchestrator.branch,
                "ordered_classes": ordered,
                "plan": plan,
                "jobs": enqueue_info["jobs"],
            },
        ).to_dict()

    except httpx.HTTPStatusError as exc:
        raise_httpx_as_http_exception(exc)
    except (SecurityViolationError, ValueError) as exc:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to orchestrate objectify DAG: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc
