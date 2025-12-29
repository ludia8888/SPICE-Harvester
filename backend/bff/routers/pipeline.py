"""
Pipeline Builder API (BFF)

Provides CRUD for pipelines + preview/build entrypoints.
"""

import asyncio
import csv
import hashlib
import io
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import quote
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile, status

from shared.models.requests import ApiResponse
from shared.models.event_envelope import EventEnvelope
from shared.services.dataset_registry import DatasetRegistry
from shared.services.dataset_ingest_outbox import (
    flush_dataset_ingest_outbox,
    build_dataset_event_payload,
)
from shared.services.objectify_registry import ObjectifyRegistry
from shared.services.objectify_job_queue import ObjectifyJobQueue
from shared.services.lakefs_client import LakeFSClient, LakeFSConflictError, LakeFSError
from shared.services.pipeline_registry import PipelineAlreadyExistsError, PipelineMergeNotSupportedError, PipelineRegistry
from shared.services.pipeline_job_queue import PipelineJobQueue
from shared.models.pipeline_job import PipelineJob
from shared.models.objectify_job import ObjectifyJob
from shared.services.pipeline_executor import PipelineExecutor
from shared.services.event_store import event_store
from shared.services.pipeline_control_plane_events import emit_pipeline_control_plane_event
from shared.dependencies.providers import AuditLogStoreDep, LineageStoreDep
from shared.services.pipeline_artifact_store import PipelineArtifactStore
from shared.services.pipeline_profiler import compute_column_stats
from shared.services.pipeline_schema_utils import normalize_schema_type
from shared.services.pipeline_scheduler import _is_valid_cron_expression
from shared.services.redis_service import create_redis_service_legacy
from shared.utils.env_utils import parse_bool_env, parse_int_env
from shared.utils.s3_uri import build_s3_uri, parse_s3_uri
from shared.utils.event_utils import build_command_event
from shared.utils.path_utils import safe_lakefs_ref, safe_path_segment
from shared.utils.time_utils import utcnow
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.security.auth_utils import enforce_db_scope
from shared.observability.tracing import trace_endpoint
from shared.config.app_config import AppConfig
from bff.dependencies import get_oms_client
from bff.services.oms_client import OMSClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pipelines", tags=["Pipeline Builder"])

_DEPENDENCY_STATUS_ALLOWLIST = {"DEPLOYED", "SUCCESS"}
_utcnow = utcnow


def _resolve_pipeline_protected_branches() -> set[str]:
    raw = os.getenv("PIPELINE_PROTECTED_BRANCHES", "main")
    return {branch.strip() for branch in raw.split(",") if branch.strip()}


def _pipeline_requires_proposal(branch: str) -> bool:
    if not parse_bool_env("PIPELINE_REQUIRE_PROPOSALS", False):
        return False
    resolved = (branch or "").strip() or "main"
    return resolved in _resolve_pipeline_protected_branches()


def _normalize_mapping_spec_ids(raw: Any) -> list[str]:
    if raw is None:
        return []
    values: list[str] = []
    if isinstance(raw, str):
        raw = raw.split(",") if "," in raw else [raw]
    if isinstance(raw, list):
        for item in raw:
            if item is None:
                continue
            value = str(item).strip()
            if value:
                values.append(value)
    else:
        value = str(raw).strip()
        if value:
            values.append(value)
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


async def _build_proposal_bundle(
    *,
    pipeline,
    build_job_id: Optional[str],
    mapping_spec_ids: list[str],
    pipeline_registry: PipelineRegistry,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
) -> dict[str, Any]:
    bundle: dict[str, Any] = {
        "captured_at": utcnow().isoformat(),
        "pipeline_id": str(pipeline.pipeline_id),
        "db_name": str(pipeline.db_name),
        "branch": str(pipeline.branch or "main"),
    }

    outputs_for_lookup: list[dict[str, Any]] = []

    if build_job_id:
        build_run = await pipeline_registry.get_run(pipeline_id=pipeline.pipeline_id, job_id=build_job_id)
        if not build_run:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Build run not found for proposal bundle")
        if str(build_run.get("mode") or "").lower() != "build":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="build_job_id is not a build run")
        build_status = str(build_run.get("status") or "").upper()
        if build_status != "SUCCESS":
            output_json = build_run.get("output_json")
            errors: list[str] = []
            if isinstance(output_json, dict):
                raw_errors = output_json.get("errors")
                if isinstance(raw_errors, list):
                    errors = [str(item) for item in raw_errors if str(item).strip()]
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "BUILD_NOT_SUCCESS",
                    "message": "Build is not successful yet",
                    "build_status": build_status or None,
                    "errors": errors,
                    "build_job_id": build_job_id,
                },
            )

        output_json = build_run.get("output_json") if isinstance(build_run.get("output_json"), dict) else {}
        definition_hash = (
            str(output_json.get("definition_hash") or build_run.get("pipeline_spec_hash") or "").strip() or None
        )
        definition_commit_id = (
            str(output_json.get("pipeline_spec_commit_id") or build_run.get("pipeline_spec_commit_id") or "").strip()
            or None
        )
        build_branch = str(output_json.get("branch") or pipeline.branch or "main").strip() or "main"
        lakefs_meta = output_json.get("lakefs") if isinstance(output_json.get("lakefs"), dict) else {}
        ontology_meta = output_json.get("ontology") if isinstance(output_json.get("ontology"), dict) else {}

        bundle.update(
            {
                "build_job_id": build_job_id,
                "definition_hash": definition_hash,
                "definition_commit_id": definition_commit_id,
                "build_branch": build_branch,
                "lakefs": {
                    "repository": str(lakefs_meta.get("repository") or "").strip() or None,
                    "build_branch": str(lakefs_meta.get("build_branch") or "").strip() or None,
                    "commit_id": str(lakefs_meta.get("commit_id") or "").strip() or None,
                },
                "ontology": {
                    "branch": str(ontology_meta.get("branch") or "").strip() or None,
                    "commit": str(ontology_meta.get("commit") or "").strip() or None,
                },
            }
        )

        outputs_payload = output_json.get("outputs") if isinstance(output_json.get("outputs"), list) else []
        for item in outputs_payload:
            if not isinstance(item, dict):
                continue
            dataset_name = str(item.get("dataset_name") or item.get("datasetName") or "").strip()
            if not dataset_name:
                continue
            outputs_for_lookup.append(
                {
                    "dataset_name": dataset_name,
                    "node_id": str(item.get("node_id") or "").strip() or None,
                    "artifact_key": str(item.get("artifact_key") or "").strip() or None,
                }
            )
        if outputs_for_lookup:
            bundle["outputs"] = outputs_for_lookup

        artifact_record = await pipeline_registry.get_artifact_by_job(
            pipeline_id=pipeline.pipeline_id,
            job_id=build_job_id,
            mode="build",
        )
        if artifact_record:
            bundle["artifact_id"] = artifact_record.artifact_id

    mapping_specs: list[dict[str, Any]] = []
    seen_spec_ids: set[str] = set()

    if mapping_spec_ids:
        for mapping_spec_id in mapping_spec_ids:
            record = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
            if not record:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={"code": "MAPPING_SPEC_NOT_FOUND", "mapping_spec_id": mapping_spec_id},
                )
            if record.mapping_spec_id in seen_spec_ids:
                continue
            seen_spec_ids.add(record.mapping_spec_id)
            mapping_specs.append(
                {
                    "mapping_spec_id": record.mapping_spec_id,
                    "mapping_spec_version": record.version,
                    "dataset_id": record.dataset_id,
                    "dataset_branch": record.dataset_branch,
                    "target_class_id": record.target_class_id,
                }
            )

    for output in outputs_for_lookup:
        dataset_name = str(output.get("dataset_name") or "").strip()
        if not dataset_name:
            continue
        dataset = await dataset_registry.get_dataset_by_name(
            db_name=str(pipeline.db_name),
            name=dataset_name,
            branch=str(pipeline.branch or "main"),
        )
        if not dataset:
            continue
        mapping_spec = await objectify_registry.get_active_mapping_spec(
            dataset_id=dataset.dataset_id,
            dataset_branch=dataset.branch,
        )
        if not mapping_spec:
            continue
        if mapping_spec.mapping_spec_id in seen_spec_ids:
            continue
        seen_spec_ids.add(mapping_spec.mapping_spec_id)
        mapping_specs.append(
            {
                "mapping_spec_id": mapping_spec.mapping_spec_id,
                "mapping_spec_version": mapping_spec.version,
                "dataset_id": mapping_spec.dataset_id,
                "dataset_branch": mapping_spec.dataset_branch,
                "target_class_id": mapping_spec.target_class_id,
                "dataset_name": dataset_name,
            }
        )

    if mapping_specs:
        bundle["mapping_specs"] = mapping_specs
    return bundle


async def get_dataset_registry() -> DatasetRegistry:
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


async def get_pipeline_registry() -> PipelineRegistry:
    from bff.main import get_pipeline_registry as _get_pipeline_registry

    return await _get_pipeline_registry()




async def get_pipeline_job_queue() -> PipelineJobQueue:
    return PipelineJobQueue()


async def get_pipeline_executor() -> PipelineExecutor:
    from bff.main import get_pipeline_executor as _get_pipeline_executor

    return await _get_pipeline_executor()


async def get_objectify_registry() -> ObjectifyRegistry:
    from bff.main import get_objectify_registry as _get_objectify_registry

    return await _get_objectify_registry()


async def get_objectify_job_queue(
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> ObjectifyJobQueue:
    return ObjectifyJobQueue(objectify_registry=objectify_registry)


async def _ensure_ingest_transaction(
    dataset_registry: DatasetRegistry,
    *,
    ingest_request_id: str,
):
    transaction = await dataset_registry.get_ingest_transaction(
        ingest_request_id=ingest_request_id,
    )
    if transaction:
        return transaction
    return await dataset_registry.create_ingest_transaction(
        ingest_request_id=ingest_request_id,
    )


async def _maybe_enqueue_objectify_job(
    *,
    dataset,
    version,
    objectify_registry: Optional[ObjectifyRegistry],
    job_queue: Optional[ObjectifyJobQueue],
    actor_user_id: Optional[str],
) -> Optional[str]:
    if not objectify_registry or not job_queue:
        return None
    if not getattr(version, "artifact_key", None):
        return None
    mapping_spec = await objectify_registry.get_active_mapping_spec(
        dataset_id=dataset.dataset_id,
        dataset_branch=dataset.branch,
    )
    if not mapping_spec or not mapping_spec.auto_sync:
        return None
    existing = await objectify_registry.find_objectify_job(
        dataset_version_id=version.version_id,
        mapping_spec_id=mapping_spec.mapping_spec_id,
        mapping_spec_version=mapping_spec.version,
        statuses=["QUEUED", "ENQUEUE_REQUESTED", "ENQUEUED", "RUNNING", "SUBMITTED"],
    )
    if existing:
        return existing.job_id
    job_id = str(uuid4())
    options = dict(mapping_spec.options or {})
    job = ObjectifyJob(
        job_id=job_id,
        db_name=dataset.db_name,
        dataset_id=dataset.dataset_id,
        dataset_version_id=version.version_id,
        dataset_branch=dataset.branch,
        artifact_key=version.artifact_key or "",
        mapping_spec_id=mapping_spec.mapping_spec_id,
        mapping_spec_version=mapping_spec.version,
        target_class_id=mapping_spec.target_class_id,
        ontology_branch=options.get("ontology_branch"),
        max_rows=options.get("max_rows"),
        batch_size=options.get("batch_size"),
        allow_partial=bool(options.get("allow_partial")),
        options={
            **options,
            "actor_user_id": actor_user_id,
        },
    )
    await job_queue.publish(job, require_delivery=False)
    return job_id


def _stable_definition_hash(definition_json: Dict[str, Any]) -> str:
    try:
        payload = json.dumps(definition_json or {}, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    except Exception:
        payload = json.dumps(str(definition_json))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _resolve_definition_commit_id(
    definition_json: Dict[str, Any],
    latest_version: Optional[Any],
    definition_hash: Optional[str] = None,
) -> Optional[str]:
    if not latest_version:
        return None
    if definition_hash is None:
        definition_hash = _stable_definition_hash(definition_json)
    latest_hash = _stable_definition_hash(getattr(latest_version, "definition_json", {}) or {})
    if latest_hash == definition_hash:
        return str(getattr(latest_version, "lakefs_commit_id", "") or "").strip() or None
    return None


async def _acquire_pipeline_publish_lock(
    *,
    pipeline_id: str,
    branch: str,
    job_id: str,
) -> Optional[tuple[Any, str, str]]:
    if not parse_bool_env("PIPELINE_LOCKS_ENABLED", True):
        return None
    redis_service = create_redis_service_legacy()
    await redis_service.connect()
    lock_key = f"pipeline-lock:{pipeline_id}:{safe_lakefs_ref(branch)}"
    ttl_seconds = parse_int_env("PIPELINE_LOCK_TTL_SECONDS", 3600, min_value=60, max_value=86_400)
    retry_seconds = parse_int_env("PIPELINE_LOCK_RETRY_SECONDS", 5, min_value=1, max_value=600)
    timeout_seconds = parse_int_env(
        "PIPELINE_LOCK_ACQUIRE_TIMEOUT_SECONDS", 30, min_value=5, max_value=3600
    )
    token = f"{job_id}:{uuid4().hex}"
    start = time.monotonic()
    while True:
        acquired = await redis_service.client.set(lock_key, token, nx=True, ex=ttl_seconds)
        if acquired:
            return redis_service, lock_key, token
        if time.monotonic() - start >= timeout_seconds:
            await redis_service.disconnect()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Pipeline branch is busy; retry after the current publish completes",
            )
        await asyncio.sleep(retry_seconds)


async def _release_pipeline_publish_lock(redis_service: Any, lock_key: str, token: str) -> None:
    script = (
        "if redis.call('GET', KEYS[1]) == ARGV[1] then "
        "return redis.call('DEL', KEYS[1]) else return 0 end"
    )
    try:
        await redis_service.client.eval(script, 1, lock_key, token)
    except Exception as exc:
        logger.warning("Failed to release pipeline publish lock (key=%s): %s", lock_key, exc)
    finally:
        await redis_service.disconnect()


def _normalize_schema_column_type(value: Any) -> str:
    return normalize_schema_type(value)


def _coerce_schema_columns(raw: Any) -> list[dict[str, str]]:
    if not raw:
        return []
    if isinstance(raw, dict) and "columns" in raw:
        raw = raw.get("columns")
    if not isinstance(raw, list):
        return []
    output: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("column") or "").strip()
        if not name:
            continue
        output.append({"name": name, "type": _normalize_schema_column_type(item.get("type"))})
    return output


def _detect_breaking_schema_changes(*, previous_schema: Any, next_columns: Any) -> list[dict[str, str]]:
    previous_cols = _coerce_schema_columns(previous_schema)
    next_cols = _coerce_schema_columns(next_columns)
    if not previous_cols or not next_cols:
        return []

    prev_map = {item["name"]: item.get("type") or "" for item in previous_cols}
    next_map = {item["name"]: item.get("type") or "" for item in next_cols}

    breaking: list[dict[str, str]] = []
    for name, prev_type in prev_map.items():
        if name not in next_map:
            breaking.append({"kind": "MISSING_COLUMN", "column": name})
            continue
        next_type = next_map.get(name) or ""
        if prev_type and next_type and prev_type != next_type:
            breaking.append({"kind": "TYPE_CHANGED", "column": name, "from": prev_type, "to": next_type})
    return breaking


def _normalize_dependencies_payload(raw: Any) -> list[dict[str, str]]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="dependencies must be a list")
    normalized: dict[str, str] = {}
    for item in raw:
        if not isinstance(item, dict):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="dependencies items must be objects")
        pipeline_id = str(item.get("pipeline_id") or item.get("pipelineId") or item.get("pipelineID") or "").strip()
        status_value = str(item.get("status") or item.get("required_status") or "DEPLOYED").strip().upper()
        if not pipeline_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="dependencies pipeline_id is required")
        try:
            UUID(pipeline_id)
        except Exception:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"dependency pipeline_id must be UUID: {pipeline_id}")
        if status_value not in _DEPENDENCY_STATUS_ALLOWLIST:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"dependency status must be one of {sorted(_DEPENDENCY_STATUS_ALLOWLIST)}",
            )
        normalized[pipeline_id] = status_value
    return [{"pipeline_id": pipeline_id, "status": status_value} for pipeline_id, status_value in normalized.items()]


async def _validate_dependency_targets(
    pipeline_registry: PipelineRegistry,
    *,
    db_name: str,
    pipeline_id: Optional[str],
    dependencies: list[dict[str, str]],
) -> None:
    for dep in dependencies:
        dep_id = str(dep.get("pipeline_id") or "").strip()
        if not dep_id:
            continue
        if pipeline_id and dep_id == str(pipeline_id):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="pipeline cannot depend on itself")
        record = await pipeline_registry.get_pipeline(pipeline_id=dep_id)
        if not record:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"dependency pipeline not found: {dep_id}")
        if str(record.db_name) != str(db_name):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"dependency pipeline must be in same db (expected {db_name} got {record.db_name})",
            )


def _format_dependencies_for_api(dependencies: list[dict[str, str]]) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    for dep in dependencies or []:
        if not isinstance(dep, dict):
            continue
        pipeline_id = str(dep.get("pipeline_id") or "").strip()
        if not pipeline_id:
            continue
        status_value = str(dep.get("status") or "DEPLOYED").strip().upper()
        output.append({"pipelineId": pipeline_id, "status": status_value})
    return output


def _resolve_principal(request: Optional[Request]) -> tuple[str, str]:
    if request is None:
        return "service", "bff"
    headers = request.headers
    principal_id = (
        headers.get("X-Principal-Id")
        or headers.get("X-Principal-ID")
        or headers.get("X-Actor")
        or headers.get("X-User-Id")
        or headers.get("X-User-ID")
        or ""
    ).strip()
    principal_type = (
        headers.get("X-Principal-Type")
        or headers.get("X-Principal-TYPE")
        or headers.get("X-Actor-Type")
        or ""
    ).strip()
    if not principal_id:
        return "service", "bff"
    normalized_type = principal_type.lower()
    if normalized_type not in {"user", "service"}:
        normalized_type = "user"
    return normalized_type, principal_id


def _actor_label(principal_type: str, principal_id: str) -> str:
    principal_type = principal_type or "user"
    principal_id = principal_id or "unknown"
    return f"{principal_type}:{principal_id}"


async def _ensure_pipeline_permission(
    pipeline_registry: PipelineRegistry,
    *,
    pipeline_id: str,
    request: Optional[Request],
    required_role: str,
) -> tuple[str, str]:
    principal_type, principal_id = _resolve_principal(request)
    has_any = await pipeline_registry.has_any_permissions(pipeline_id=pipeline_id)
    if not has_any:
        await pipeline_registry.grant_permission(
            pipeline_id=pipeline_id,
            principal_type=principal_type,
            principal_id=principal_id,
            role="admin",
        )
        return principal_type, principal_id
    allowed = await pipeline_registry.has_permission(
        pipeline_id=pipeline_id,
        principal_type=principal_type,
        principal_id=principal_id,
        required_role=required_role,
    )
    if not allowed:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied")
    return principal_type, principal_id


async def _log_pipeline_audit(
    audit_store: AuditLogStoreDep,
    *,
    request: Optional[Request],
    action: str,
    status: str,
    pipeline_id: Optional[str] = None,
    db_name: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> None:
    principal_type, principal_id = _resolve_principal(request)
    actor = _actor_label(principal_type, principal_id)
    partition_key = f"pipeline:{pipeline_id}" if pipeline_id else f"db:{db_name or 'unknown'}"
    await audit_store.log(
        partition_key=partition_key,
        actor=actor,
        action=action,
        status="success" if status.lower() == "success" else "failure",
        resource_type="pipeline" if pipeline_id else "pipeline_branch",
        resource_id=str(pipeline_id) if pipeline_id else None,
        metadata=metadata or {},
        error=error,
    )


def _normalize_location(location: str) -> str:
    cleaned = (location or "").strip()
    if not cleaned:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="location is required")
    if "personal" in cleaned.lower():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="personal locations are not allowed",
        )
    return cleaned


def _default_dataset_name(filename: str) -> str:
    base = (filename or "").strip()
    if not base:
        return "excel_dataset"
    if "." in base:
        base = ".".join(base.split(".")[:-1]) or base
    return base or "excel_dataset"


def _extract_node_ids(definition_json: Optional[Dict[str, Any]]) -> set[str]:
    if not isinstance(definition_json, dict):
        return set()
    nodes = definition_json.get("nodes") or []
    if not isinstance(nodes, list):
        return set()
    output: set[str] = set()
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or node.get("node_id") or "").strip()
        if node_id:
            output.add(node_id)
    return output


def _extract_edge_ids(definition_json: Optional[Dict[str, Any]]) -> set[str]:
    if not isinstance(definition_json, dict):
        return set()
    edges = definition_json.get("edges") or []
    if not isinstance(edges, list):
        return set()
    output: set[str] = set()
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        edge_id = str(edge.get("id") or "").strip()
        if not edge_id:
            source = str(edge.get("source") or edge.get("from") or "").strip()
            target = str(edge.get("target") or edge.get("to") or "").strip()
            if source or target:
                edge_id = f"{source}->{target}"
        if edge_id:
            output.add(edge_id)
    return output


def _definition_diff(
    previous: Optional[Dict[str, Any]],
    current: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    prev_nodes = _extract_node_ids(previous)
    next_nodes = _extract_node_ids(current)
    prev_edges = _extract_edge_ids(previous)
    next_edges = _extract_edge_ids(current)
    return {
        "nodes_added": sorted(next_nodes - prev_nodes),
        "nodes_removed": sorted(prev_nodes - next_nodes),
        "edges_added": sorted(next_edges - prev_edges),
        "edges_removed": sorted(prev_edges - next_edges),
        "node_count": len(next_nodes),
        "edge_count": len(next_edges),
    }


def _normalize_table_bbox(
    *,
    table_top: Optional[int],
    table_left: Optional[int],
    table_bottom: Optional[int],
    table_right: Optional[int],
) -> Optional[Dict[str, int]]:
    parts = [table_top, table_left, table_bottom, table_right]
    if all(part is None for part in parts):
        return None
    if any(part is None for part in parts):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="table_top/table_left/table_bottom/table_right must be provided together",
        )
    return {
        "top": int(table_top),
        "left": int(table_left),
        "bottom": int(table_bottom),
        "right": int(table_right),
    }


def _normalize_inferred_type(type_value: Any) -> str:
    if not type_value:
        return "xsd:string"
    raw = str(type_value).strip()
    if not raw:
        return "xsd:string"
    if raw.startswith("xsd:"):
        return raw
    lowered = raw.lower()
    if lowered == "money":
        return "xsd:decimal"
    if lowered in {"integer", "int"}:
        return "xsd:integer"
    if lowered in {"decimal", "number", "float", "double"}:
        return "xsd:decimal"
    if lowered in {"boolean", "bool"}:
        return "xsd:boolean"
    if lowered in {"datetime", "timestamp"}:
        return "xsd:dateTime"
    if lowered == "date":
        return "xsd:dateTime"
    return "xsd:string"


def _build_schema_columns(columns: list[str], inferred_schema: list[Dict[str, Any]]) -> list[Dict[str, str]]:
    inferred_map: Dict[str, str] = {}
    for item in inferred_schema or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("column_name") or "").strip()
        if not name:
            continue
        inferred_type = item.get("type") or item.get("data_type")
        if not inferred_type:
            inferred_payload = item.get("inferred_type")
            if isinstance(inferred_payload, dict):
                inferred_type = inferred_payload.get("type")
        inferred_map[name] = _normalize_inferred_type(inferred_type)

    schema_columns: list[Dict[str, str]] = []
    for col in columns:
        col_name = str(col)
        if not col_name:
            continue
        schema_columns.append({"name": col_name, "type": inferred_map.get(col_name, "xsd:string")})
    return schema_columns


def _columns_from_schema(schema_columns: list[Dict[str, str]]) -> list[Dict[str, str]]:
    return [
        {"name": str(col.get("name") or ""), "type": str(col.get("type") or "String")}
        for col in schema_columns
        if str(col.get("name") or "")
    ]


def _rows_from_preview(columns: list[str], sample_rows: list[list[Any]]) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for row in sample_rows or []:
        if not isinstance(row, list):
            continue
        payload: Dict[str, Any] = {}
        for index, col in enumerate(columns):
            payload[str(col)] = row[index] if index < len(row) else None
        rows.append(payload)
    return rows


def _detect_csv_delimiter(sample: str) -> str:
    candidates = [",", "\t", ";", "|"]
    line = next((line for line in sample.splitlines() if line.strip()), "")
    if not line:
        return ","
    scores = {delimiter: line.count(delimiter) for delimiter in candidates}
    best = max(scores, key=scores.get)
    return best if scores.get(best, 0) > 0 else ","


def _parse_csv_file(
    file_obj: Any,
    *,
    delimiter: str,
    has_header: bool,
    preview_limit: int = 200,
) -> tuple[list[str], list[list[str]], int, str]:
    if not hasattr(file_obj, "read"):
        raise ValueError("file object does not support read()")
    if not hasattr(file_obj, "seek"):
        raise ValueError("file object does not support seek()")

    class _HashingReader(io.RawIOBase):
        def __init__(self, raw: Any, hasher: "hashlib._Hash") -> None:
            self._raw = raw
            self._hasher = hasher

        def read(self, size: int = -1) -> bytes:
            chunk = self._raw.read(size)
            if chunk:
                self._hasher.update(chunk)
            return chunk

        def readinto(self, b: bytearray | memoryview) -> int:
            chunk = self.read(len(b))
            if not chunk:
                return 0
            b[: len(chunk)] = chunk
            return len(chunk)

        def readable(self) -> bool:
            return True

        def seekable(self) -> bool:
            return hasattr(self._raw, "seek")

        def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
            return self._raw.seek(offset, whence)

        def tell(self) -> int:
            return self._raw.tell()

    file_obj.seek(0)
    hasher = hashlib.sha256()
    hashing_reader = _HashingReader(file_obj, hasher)
    buffered = io.BufferedReader(hashing_reader)
    text_stream = io.TextIOWrapper(buffered, encoding="utf-8", errors="replace", newline="")
    columns: list[str] = []
    preview_rows: list[list[str]] = []
    total_rows = 0

    try:
        reader = csv.reader(text_stream, delimiter=delimiter)
        for row in reader:
            if not row or all(str(cell).strip() == "" for cell in row):
                continue
            if not columns:
                if has_header:
                    columns = [str(cell).strip() or f"column_{index + 1}" for index, cell in enumerate(row)]
                    continue
                columns = [f"column_{index + 1}" for index in range(len(row))]
            normalized = [str(cell).strip() for cell in row]
            if len(normalized) < len(columns):
                normalized.extend([""] * (len(columns) - len(normalized)))
            elif len(normalized) > len(columns):
                normalized = normalized[: len(columns)]
            total_rows += 1
            if len(preview_rows) < preview_limit:
                preview_rows.append(normalized)
    finally:
        try:
            text_stream.detach()
        except Exception:
            pass
        try:
            file_obj.seek(0)
        except Exception:
            pass

    return columns, preview_rows, total_rows, hasher.hexdigest()


def _parse_csv_content(
    content: str,
    *,
    delimiter: str,
    has_header: bool,
    preview_limit: int = 200,
) -> tuple[list[str], list[list[str]], int]:
    reader = csv.reader(io.StringIO(content), delimiter=delimiter)
    columns: list[str] = []
    preview_rows: list[list[str]] = []
    total_rows = 0

    for row in reader:
        if not row or all(str(cell).strip() == "" for cell in row):
            continue
        if not columns:
            if has_header:
                columns = [str(cell).strip() or f"column_{index + 1}" for index, cell in enumerate(row)]
                continue
            columns = [f"column_{index + 1}" for index in range(len(row))]
        normalized = [str(cell).strip() for cell in row]
        if len(normalized) < len(columns):
            normalized.extend([""] * (len(columns) - len(normalized)))
        elif len(normalized) > len(columns):
            normalized = normalized[: len(columns)]
        total_rows += 1
        if len(preview_rows) < preview_limit:
            preview_rows.append(normalized)

    return columns, preview_rows, total_rows


def _require_idempotency_key(request: Optional[Request]) -> str:
    if request is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Idempotency-Key header is required")
    key = (
        request.headers.get("Idempotency-Key")
        or request.headers.get("X-Idempotency-Key")
        or ""
    ).strip()
    if not key:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Idempotency-Key header is required")
    return key


def _build_ingest_request_fingerprint(payload: Dict[str, Any]) -> str:
    try:
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    except Exception:
        serialized = json.dumps(str(payload))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _ingest_staging_prefix(prefix: str, ingest_request_id: str) -> str:
    safe_request_id = safe_path_segment(ingest_request_id)
    return f"{prefix}/staging/{safe_request_id}"


def _sanitize_s3_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not metadata:
        return {}
    sanitized: Dict[str, str] = {}
    for key, value in metadata.items():
        if value is None:
            continue
        raw = str(value)
        try:
            raw.encode("ascii")
            sanitized[key] = raw
        except UnicodeEncodeError:
            sanitized[key] = quote(raw, safe="")
    return sanitized


def _resolve_lakefs_raw_repository() -> str:
    repo = (os.getenv("LAKEFS_RAW_REPOSITORY") or "").strip()
    return repo or "raw-datasets"


async def _ensure_lakefs_branch_exists(
    *,
    lakefs_client: LakeFSClient,
    repository: str,
    branch: str,
    source_branch: str = "main",
) -> None:
    resolved_branch = safe_lakefs_ref(branch)
    resolved_source = safe_lakefs_ref(source_branch)
    if resolved_branch == resolved_source:
        return
    try:
        await lakefs_client.create_branch(
            repository=repository,
            name=resolved_branch,
            source=resolved_source,
        )
    except LakeFSConflictError:
        return


def _extract_lakefs_ref_from_artifact_key(artifact_key: str) -> str:
    parsed = parse_s3_uri(artifact_key)
    if not parsed:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="artifact_key must be an s3:// URI")
    _, key = parsed
    parts = [part for part in str(key).split("/") if part]
    if len(parts) < 2:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="artifact_key must include lakeFS ref prefix")
    return parts[0]


def _dataset_artifact_prefix(*, db_name: str, dataset_id: str, dataset_name: str) -> str:
    safe_name = safe_path_segment(dataset_name)
    return f"datasets/{db_name}/{dataset_id}/{safe_name}"


@router.get("", response_model=ApiResponse)
@trace_endpoint("list_pipelines")
async def list_pipelines(
    db_name: str,
    branch: Optional[str] = None,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        pipelines = await pipeline_registry.list_pipelines(db_name=db_name, branch=branch)
        principal_type, principal_id = _resolve_principal(request)
        filtered: list[dict[str, Any]] = []
        for pipeline in pipelines:
            pipeline_id = str(pipeline.get("pipeline_id") or "").strip()
            if not pipeline_id:
                continue
            if not await pipeline_registry.has_any_permissions(pipeline_id=pipeline_id):
                await pipeline_registry.grant_permission(
                    pipeline_id=pipeline_id,
                    principal_type=principal_type,
                    principal_id=principal_id,
                    role="admin",
                )
                filtered.append(pipeline)
                continue
            if await pipeline_registry.has_permission(
                pipeline_id=pipeline_id,
                principal_type=principal_type,
                principal_id=principal_id,
                required_role="read",
            ):
                filtered.append(pipeline)
        return ApiResponse.success(
            message="Pipelines fetched",
            data={"pipelines": filtered, "count": len(filtered)},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list pipelines: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/proposals", response_model=ApiResponse)
@trace_endpoint("list_pipeline_proposals")
async def list_pipeline_proposals(
    db_name: str,
    branch: Optional[str] = None,
    status_filter: Optional[str] = Query(default=None, alias="status"),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        proposals = await pipeline_registry.list_proposals(db_name=db_name, branch=branch, status=status_filter)
        principal_type, principal_id = _resolve_principal(request)
        filtered: list[dict[str, Any]] = []
        for proposal in proposals:
            pipeline_id = str(proposal.get("pipeline_id") or proposal.get("pipelineId") or "").strip()
            if not pipeline_id:
                continue
            if not await pipeline_registry.has_any_permissions(pipeline_id=pipeline_id):
                await pipeline_registry.grant_permission(
                    pipeline_id=pipeline_id,
                    principal_type=principal_type,
                    principal_id=principal_id,
                    role="admin",
                )
                filtered.append(proposal)
                continue
            if await pipeline_registry.has_permission(
                pipeline_id=pipeline_id,
                principal_type=principal_type,
                principal_id=principal_id,
                required_role="read",
            ):
                filtered.append(proposal)
        return ApiResponse.success(
            message="Pipeline proposals fetched",
            data={"proposals": filtered, "count": len(filtered)},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list pipeline proposals: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/{pipeline_id}/proposals", response_model=ApiResponse)
@trace_endpoint("submit_pipeline_proposal")
async def submit_pipeline_proposal(
    pipeline_id: str,
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="edit",
        )
        sanitized = sanitize_input(payload)
        title = str(sanitized.get("title") or "").strip()
        if not title:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="title is required")
        description = str(sanitized.get("description") or "").strip() or None
        build_job_id = str(sanitized.get("build_job_id") or sanitized.get("buildJobId") or "").strip() or None
        mapping_spec_ids = _normalize_mapping_spec_ids(
            sanitized.get("mapping_spec_ids")
            or sanitized.get("mappingSpecIds")
            or sanitized.get("mapping_spec_id")
            or sanitized.get("mappingSpecId")
        )
        proposal_bundle = None
        if build_job_id or mapping_spec_ids:
            pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
            if not pipeline:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
            proposal_bundle = await _build_proposal_bundle(
                pipeline=pipeline,
                build_job_id=build_job_id,
                mapping_spec_ids=mapping_spec_ids,
                pipeline_registry=pipeline_registry,
                dataset_registry=dataset_registry,
                objectify_registry=objectify_registry,
            )
        record = await pipeline_registry.submit_proposal(
            pipeline_id=pipeline_id,
            title=title,
            description=description,
            proposal_bundle=proposal_bundle,
        )
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PROPOSAL_SUBMITTED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={"title": title, "description": description, "build_job_id": build_job_id},
        )
        return ApiResponse.success(
            message="Proposal submitted",
            data={"proposal": {**record.__dict__}},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PROPOSAL_SUBMITTED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(e),
        )
        logger.error(f"Failed to submit proposal: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/{pipeline_id}/proposals/approve", response_model=ApiResponse)
@trace_endpoint("approve_pipeline_proposal")
async def approve_pipeline_proposal(
    pipeline_id: str,
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="approve",
        )
        sanitized = sanitize_input(payload)
        to_branch = str(sanitized.get("merge_into") or "main")
        review_comment = str(sanitized.get("review_comment") or "").strip() or None
        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
        if pipeline.proposal_status != "pending":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="No pending proposal to approve",
            )

        from_branch = str(pipeline.branch or "main")
        if from_branch != to_branch:
            try:
                await pipeline_registry.merge_branch(
                    pipeline_id=pipeline_id,
                    from_branch=from_branch,
                    to_branch=to_branch,
                )
            except PipelineMergeNotSupportedError as e:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={"code": "MERGE_NOT_SUPPORTED", "message": str(e)},
                )
            except LakeFSConflictError as e:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "MERGE_CONFLICT",
                        "message": "lakeFS merge conflict",
                        "detail": str(e),
                        "source_branch": from_branch,
                        "target_branch": to_branch,
                    },
                )

        record = await pipeline_registry.review_proposal(
            pipeline_id=pipeline_id,
            status="approved",
            review_comment=review_comment,
        )
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PROPOSAL_APPROVED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={"from_branch": from_branch, "to_branch": to_branch, "review_comment": review_comment},
        )
        return ApiResponse.success(
            message="Proposal approved",
            data={"proposal": {**record.__dict__}},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PROPOSAL_APPROVED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(e),
        )
        logger.error(f"Failed to approve proposal: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/{pipeline_id}/proposals/reject", response_model=ApiResponse)
@trace_endpoint("reject_pipeline_proposal")
async def reject_pipeline_proposal(
    pipeline_id: str,
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="approve",
        )
        sanitized = sanitize_input(payload)
        review_comment = str(sanitized.get("review_comment") or "").strip() or None
        record = await pipeline_registry.review_proposal(
            pipeline_id=pipeline_id,
            status="rejected",
            review_comment=review_comment,
        )
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PROPOSAL_REJECTED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={"review_comment": review_comment},
        )
        return ApiResponse.success(
            message="Proposal rejected",
            data={"proposal": {**record.__dict__}},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PROPOSAL_REJECTED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(e),
        )
        logger.error(f"Failed to reject proposal: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/datasets", response_model=ApiResponse)
@trace_endpoint("list_datasets")
async def list_datasets(
    db_name: str,
    branch: Optional[str] = Query(default=None),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        datasets = await dataset_registry.list_datasets(db_name=db_name, branch=branch)
        return ApiResponse.success(
            message="Datasets fetched",
            data={"datasets": datasets, "count": len(datasets)},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list datasets: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/branches", response_model=ApiResponse)
@trace_endpoint("list_pipeline_branches")
async def list_pipeline_branches(
    db_name: str,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        branches = await pipeline_registry.list_pipeline_branches(db_name=db_name)
        return ApiResponse.success(
            message="Pipeline branches fetched",
            data={"branches": branches, "count": len(branches)},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list pipeline branches: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/branches/{branch}/archive", response_model=ApiResponse)
@trace_endpoint("archive_pipeline_branch")
async def archive_pipeline_branch(
    branch: str,
    db_name: str,
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        resolved_branch = safe_lakefs_ref(branch)
        if resolved_branch == "main":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="main branch cannot be archived")
        existing = await pipeline_registry.get_pipeline_branch(db_name=db_name, branch=resolved_branch)
        if not existing:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Branch not found")
        record = await pipeline_registry.archive_pipeline_branch(db_name=db_name, branch=resolved_branch)
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BRANCH_ARCHIVED",
            status="success",
            db_name=db_name,
            metadata={"branch": resolved_branch},
        )
        return ApiResponse.success(message="Branch archived", data={"branch": record}).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BRANCH_ARCHIVED",
            status="failure",
            db_name=db_name,
            error=str(e),
        )
        logger.error(f"Failed to archive pipeline branch: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/branches/{branch}/restore", response_model=ApiResponse)
@trace_endpoint("restore_pipeline_branch")
async def restore_pipeline_branch(
    branch: str,
    db_name: str,
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        resolved_branch = safe_lakefs_ref(branch)
        existing = await pipeline_registry.get_pipeline_branch(db_name=db_name, branch=resolved_branch)
        if not existing:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Branch not found")
        record = await pipeline_registry.restore_pipeline_branch(db_name=db_name, branch=resolved_branch)
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BRANCH_RESTORED",
            status="success",
            db_name=db_name,
            metadata={"branch": resolved_branch},
        )
        return ApiResponse.success(message="Branch restored", data={"branch": record}).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BRANCH_RESTORED",
            status="failure",
            db_name=db_name,
            error=str(e),
        )
        logger.error(f"Failed to restore pipeline branch: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/{pipeline_id}/runs", response_model=ApiResponse)
@trace_endpoint("list_pipeline_runs")
async def list_pipeline_runs(
    pipeline_id: str,
    limit: int = Query(default=50, ge=1, le=200),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="read",
        )
        runs = await pipeline_registry.list_runs(pipeline_id=pipeline_id, limit=limit)
        return ApiResponse.success(
            message="Pipeline runs fetched",
            data={"runs": runs, "count": len(runs)},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list pipeline runs: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/{pipeline_id}/artifacts", response_model=ApiResponse)
@trace_endpoint("list_pipeline_artifacts")
async def list_pipeline_artifacts(
    pipeline_id: str,
    mode: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="read",
        )
        artifacts = await pipeline_registry.list_artifacts(
            pipeline_id=pipeline_id,
            limit=limit,
            mode=mode,
        )
        payload = [artifact.__dict__ for artifact in artifacts]
        return ApiResponse.success(message="Pipeline artifacts", data={"artifacts": payload}).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list pipeline artifacts: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/{pipeline_id}/artifacts/{artifact_id}", response_model=ApiResponse)
@trace_endpoint("get_pipeline_artifact")
async def get_pipeline_artifact(
    pipeline_id: str,
    artifact_id: str,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="read",
        )
        artifact = await pipeline_registry.get_artifact(artifact_id=artifact_id)
        if not artifact or artifact.pipeline_id != pipeline_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Artifact not found")
        return ApiResponse.success(message="Pipeline artifact", data={"artifact": artifact.__dict__}).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch pipeline artifact: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.post("/{pipeline_id}/branches", response_model=ApiResponse)
@trace_endpoint("create_pipeline_branch")
async def create_pipeline_branch(
    pipeline_id: str,
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="edit",
        )
        actor_user_id = (request.headers.get("X-User-ID") or "").strip() if request else ""
        actor_user_id = actor_user_id or None

        sanitized = sanitize_input(payload)
        requested_branch = str(sanitized.get("branch") or "").strip()
        if not requested_branch:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="branch is required")

        # Pipeline branches are backed by lakeFS refs (single path segment for S3 gateway).
        branch = safe_lakefs_ref(requested_branch)
        if branch == "main" and requested_branch.lower() != "main":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid branch name")

        record = await pipeline_registry.create_branch(pipeline_id=pipeline_id, new_branch=branch, user_id=actor_user_id)
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BRANCH_CREATED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={"branch": branch},
        )
        return ApiResponse.success(
            message="Pipeline branch created",
            data={"branch": {**record.__dict__}},
        ).to_dict()
    except HTTPException:
        raise
    except (PipelineAlreadyExistsError,) as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BRANCH_CREATED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(e),
        )
        message = str(e)
        if isinstance(e, LakeFSConflictError):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Branch already exists")
        if isinstance(e, LakeFSError):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)
        if "not found" in message.lower():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
        logger.error(f"Failed to create pipeline branch: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message)


@router.get("/{pipeline_id}", response_model=ApiResponse)
@trace_endpoint("get_pipeline")
async def get_pipeline(
    pipeline_id: str,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    branch: Optional[str] = Query(default=None),
    preview_node_id: Optional[str] = Query(default=None, alias="preview_node_id"),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="read",
        )
        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
        version = await pipeline_registry.get_latest_version(
            pipeline_id=pipeline_id,
            branch=branch or pipeline.branch,
        )
        dependencies = await pipeline_registry.list_dependencies(pipeline_id=pipeline_id)
        dependencies_for_api = _format_dependencies_for_api(dependencies)
        definition_payload: Dict[str, Any] = version.definition_json if version else {}
        if not isinstance(definition_payload, dict):
            definition_payload = {}
        if dependencies_for_api:
            definition_payload = {**definition_payload, "dependencies": dependencies_for_api}
        pipeline_payload = {
            **pipeline.__dict__,
            "definition_json": definition_payload,
            "version_id": version.version_id if version else None,
            "version": version.lakefs_commit_id if version else None,
            "commit_id": version.lakefs_commit_id if version else None,
            "dependencies": dependencies_for_api,
        }
        if preview_node_id:
            preview_nodes = pipeline.last_preview_nodes or {}
            if preview_node_id in preview_nodes:
                pipeline_payload["last_preview_sample"] = preview_nodes.get(preview_node_id)
                pipeline_payload["last_preview_node_id"] = preview_node_id
        return ApiResponse.success(
            message="Pipeline fetched",
            data={"pipeline": pipeline_payload},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch pipeline: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/{pipeline_id}/readiness", response_model=ApiResponse)
@trace_endpoint("get_pipeline_readiness")
async def get_pipeline_readiness(
    pipeline_id: str,
    branch: Optional[str] = Query(default=None),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="read",
        )
        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")

        resolved_branch = (branch or pipeline.branch or "main").strip() or "main"
        version = await pipeline_registry.get_latest_version(pipeline_id=pipeline_id, branch=resolved_branch)
        definition = version.definition_json if version else {}
        if not isinstance(definition, dict):
            definition = {}
        nodes = definition.get("nodes") or []
        if not isinstance(nodes, list):
            nodes = []

        fallback_raw = os.getenv("PIPELINE_FALLBACK_BRANCHES", "main")
        fallback_candidates = [b.strip() for b in fallback_raw.split(",") if b.strip()]
        if "main" not in fallback_candidates:
            fallback_candidates.append("main")

        inputs: list[dict[str, Any]] = []
        overall_blocked = False
        overall_pending = False

        for node in nodes:
            if not isinstance(node, dict):
                continue
            if str(node.get("type") or "") != "input":
                continue
            node_id = str(node.get("id") or "").strip() or None
            metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
            dataset_id = str(metadata.get("datasetId") or metadata.get("dataset_id") or "").strip() or None
            dataset_name = str(metadata.get("datasetName") or metadata.get("dataset_name") or "").strip() or None
            requested_dataset_branch = str(
                metadata.get("datasetBranch") or metadata.get("dataset_branch") or resolved_branch
            ).strip() or resolved_branch

            if not dataset_id and not dataset_name:
                overall_blocked = True
                inputs.append(
                    {
                        "node_id": node_id,
                        "status": "INVALID_CONFIG",
                        "detail": "datasetId or datasetName is required",
                    }
                )
                continue

            candidates: list[str] = []
            for candidate in [requested_dataset_branch, *fallback_candidates]:
                if candidate and candidate not in candidates:
                    candidates.append(candidate)

            dataset = None
            version_rec = None
            resolved_dataset_branch = None

            if dataset_id:
                dataset = await dataset_registry.get_dataset(dataset_id=str(dataset_id))
                if dataset:
                    version_rec = await dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
                    dataset_name = dataset_name or dataset.name
                    resolved_dataset_branch = dataset.branch

            if (not dataset or not version_rec) and dataset_name:
                dataset = None
                version_rec = None
                resolved_dataset_branch = None
                for candidate_branch in candidates:
                    found = await dataset_registry.get_dataset_by_name(
                        db_name=pipeline.db_name,
                        name=dataset_name,
                        branch=candidate_branch,
                    )
                    if not found:
                        continue
                    found_version = await dataset_registry.get_latest_version(dataset_id=found.dataset_id)
                    if not found_version:
                        continue
                    dataset = found
                    version_rec = found_version
                    resolved_dataset_branch = candidate_branch
                    break

            if not dataset:
                overall_blocked = True
                inputs.append(
                    {
                        "node_id": node_id,
                        "dataset_id": dataset_id,
                        "dataset_name": dataset_name,
                        "requested_branch": requested_dataset_branch,
                        "status": "MISSING_DATASET",
                        "detail": "dataset not found in requested/fallback branches",
                    }
                )
                continue

            if not version_rec:
                overall_pending = True
                inputs.append(
                    {
                        "node_id": node_id,
                        "dataset_id": dataset.dataset_id,
                        "dataset_name": dataset.name,
                        "requested_branch": requested_dataset_branch,
                        "resolved_branch": resolved_dataset_branch or dataset.branch,
                        "status": "NO_VERSIONS",
                        "detail": "dataset has no versions in requested/fallback branches",
                    }
                )
                continue

            used_fallback = bool((resolved_dataset_branch or dataset.branch) != requested_dataset_branch)
            inputs.append(
                {
                    "node_id": node_id,
                    "dataset_id": dataset.dataset_id,
                    "dataset_name": dataset.name,
                    "requested_branch": requested_dataset_branch,
                    "resolved_branch": resolved_dataset_branch or dataset.branch,
                    "used_fallback": used_fallback,
                    "status": "READY",
                    "latest_commit_id": version_rec.lakefs_commit_id,
                    "latest_version": version_rec.lakefs_commit_id,
                    "artifact_key": version_rec.artifact_key,
                }
            )

        overall_status = "READY_FOR_PREVIEW"
        if overall_blocked:
            overall_status = "BLOCKED"
        elif overall_pending:
            overall_status = "PENDING"

        return ApiResponse.success(
            message="Pipeline readiness fetched",
            data={
                "pipeline_id": pipeline_id,
                "branch": resolved_branch,
                "version_id": version.version_id if version else None,
                "commit_id": version.lakefs_commit_id if version else None,
                "version": version.lakefs_commit_id if version else None,
                "status": overall_status,
                "inputs": inputs,
                "fallback_branches": fallback_candidates,
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch pipeline readiness: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("", response_model=ApiResponse)
@trace_endpoint("create_pipeline")
async def create_pipeline(
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        sanitized = sanitize_input(payload)
        db_name = validate_db_name(str(sanitized.get("db_name") or ""))
        name = str(sanitized.get("name") or "").strip()
        if not name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="name is required")
        pipeline_type = str(sanitized.get("pipeline_type") or "batch").strip() or "batch"
        location = _normalize_location(str(sanitized.get("location") or ""))
        description = str(sanitized.get("description") or "").strip() or None
        definition_json = sanitized.get("definition_json") if isinstance(sanitized, dict) else {}
        if not isinstance(definition_json, dict):
            definition_json = {}

        dependencies_raw: Any = None
        if "dependencies" in sanitized:
            dependencies_raw = sanitized.get("dependencies")
        elif "dependencies" in definition_json:
            dependencies_raw = definition_json.get("dependencies")
        dependencies: Optional[list[dict[str, str]]] = None
        if dependencies_raw is not None:
            dependencies = _normalize_dependencies_payload(dependencies_raw)
            await _validate_dependency_targets(
                pipeline_registry,
                db_name=db_name,
                pipeline_id=None,
                dependencies=dependencies,
            )
            definition_json = {**definition_json}
            definition_json.pop("dependencies", None)

        branch = str(sanitized.get("branch") or "main").strip() or "main"
        proposal_status = str(sanitized.get("proposal_status") or "").strip() or None
        proposal_title = str(sanitized.get("proposal_title") or "").strip() or None
        proposal_description = str(sanitized.get("proposal_description") or "").strip() or None
        schedule_interval_seconds = sanitized.get("schedule_interval_seconds")
        schedule_cron = sanitized.get("schedule_cron")
        schedule = sanitized.get("schedule") if isinstance(sanitized.get("schedule"), dict) else None
        proposal_submitted_at = sanitized.get("proposal_submitted_at")
        proposal_reviewed_at = sanitized.get("proposal_reviewed_at")
        proposal_review_comment = sanitized.get("proposal_review_comment")
        if schedule:
            schedule_interval_seconds = schedule.get("interval_seconds", schedule_interval_seconds)
            schedule_cron = schedule.get("cron", schedule_cron)
        if schedule_interval_seconds is not None:
            try:
                schedule_interval_seconds = int(schedule_interval_seconds)
            except Exception:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="schedule_interval_seconds must be integer")
            if schedule_interval_seconds <= 0:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="schedule_interval_seconds must be > 0")
        schedule_cron = str(schedule_cron).strip() if schedule_cron else None
        if schedule_interval_seconds and schedule_cron:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provide either schedule_interval_seconds or schedule_cron (not both)")
        if schedule_cron and not _is_valid_cron_expression(schedule_cron):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="schedule_cron must be a supported 5-field cron expression")
        proposal_submitted_at = None if not proposal_submitted_at else proposal_submitted_at
        proposal_reviewed_at = None if not proposal_reviewed_at else proposal_reviewed_at
        proposal_review_comment = str(proposal_review_comment).strip() if proposal_review_comment else None

        record = await pipeline_registry.create_pipeline(
            db_name=db_name,
            name=name,
            description=description,
            pipeline_type=pipeline_type,
            location=location,
            branch=branch,
            proposal_status=proposal_status,
            proposal_title=proposal_title,
            proposal_description=proposal_description,
            proposal_submitted_at=proposal_submitted_at,
            proposal_reviewed_at=proposal_reviewed_at,
            proposal_review_comment=proposal_review_comment,
            schedule_interval_seconds=schedule_interval_seconds,
            schedule_cron=schedule_cron,
        )
        principal_type, principal_id = _resolve_principal(request)
        await pipeline_registry.grant_permission(
            pipeline_id=record.pipeline_id,
            principal_type=principal_type,
            principal_id=principal_id,
            role="admin",
        )
        version = await pipeline_registry.add_version(
            pipeline_id=record.pipeline_id,
            branch=branch,
            definition_json=definition_json,
        )
        if dependencies is not None:
            await pipeline_registry.replace_dependencies(pipeline_id=record.pipeline_id, dependencies=dependencies)
        dependencies_payload = await pipeline_registry.list_dependencies(pipeline_id=record.pipeline_id)
        dependencies_for_api = _format_dependencies_for_api(dependencies_payload)
        if dependencies_for_api:
            definition_json = {**definition_json, "dependencies": dependencies_for_api}

        event = build_command_event(
            event_type="PIPELINE_CREATED",
            aggregate_type="Pipeline",
            aggregate_id=record.pipeline_id,
            data={
                "db_name": db_name,
                "pipeline_id": record.pipeline_id,
                "name": name,
                "pipeline_type": pipeline_type,
                "location": location,
                "definition_json": definition_json,
                "version_id": version.version_id,
                "commit_id": version.lakefs_commit_id,
                "version": version.lakefs_commit_id,
                "branch": branch,
            },
            command_type="CREATE_PIPELINE",
        )
        event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
        try:
            await event_store.connect()
            await event_store.append_event(event)
        except Exception as e:
            logger.warning(f"Failed to append pipeline create event: {e}")

        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_CREATED",
            status="success",
            pipeline_id=record.pipeline_id,
            metadata={
                "db_name": db_name,
                "name": name,
                "pipeline_type": pipeline_type,
                "branch": branch,
                "commit_id": version.lakefs_commit_id if version else None,
            },
        )

        return ApiResponse.success(
            message="Pipeline created",
            data={
                "pipeline": {
                    **record.__dict__,
                    "definition_json": definition_json,
                    "version_id": version.version_id,
                    "commit_id": version.lakefs_commit_id,
                    "version": version.lakefs_commit_id,
                    "dependencies": dependencies_for_api,
                },
            },
        ).to_dict()
    except HTTPException:
        raise
    except PipelineAlreadyExistsError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "PIPELINE_ALREADY_EXISTS", "message": str(exc)},
        ) from exc
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_CREATED",
            status="failure",
            db_name=str(sanitized.get("db_name") or ""),
            error=str(e),
        )
        logger.error(f"Failed to create pipeline: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.put("/{pipeline_id}", response_model=ApiResponse)
@trace_endpoint("update_pipeline")
async def update_pipeline(
    pipeline_id: str,
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="edit",
        )
        sanitized = sanitize_input(payload)
        definition_json = sanitized.get("definition_json") if isinstance(sanitized, dict) else None
        if definition_json is not None and not isinstance(definition_json, dict):
            definition_json = {}

        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
        previous_version = await pipeline_registry.get_latest_version(
            pipeline_id=pipeline_id,
            branch=pipeline.branch,
        )
        previous_definition = previous_version.definition_json if previous_version else {}
        branch_state = await pipeline_registry.get_pipeline_branch(db_name=pipeline.db_name, branch=pipeline.branch)
        if branch_state and branch_state.get("archived"):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Archived branch cannot be updated; restore the branch before making changes",
            )
        if pipeline.branch in _resolve_pipeline_protected_branches() and definition_json is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Protected branch cannot be updated directly; create a branch and submit a proposal",
            )
        db_name = str(pipeline.db_name)

        dependencies_raw: Any = None
        if "dependencies" in sanitized:
            dependencies_raw = sanitized.get("dependencies")
        elif isinstance(definition_json, dict) and "dependencies" in definition_json:
            dependencies_raw = definition_json.get("dependencies")
        dependencies: Optional[list[dict[str, str]]] = None
        if dependencies_raw is not None:
            dependencies = _normalize_dependencies_payload(dependencies_raw)
            await _validate_dependency_targets(
                pipeline_registry,
                db_name=db_name,
                pipeline_id=pipeline_id,
                dependencies=dependencies,
            )
            if isinstance(definition_json, dict):
                definition_json = {**definition_json}
                definition_json.pop("dependencies", None)
        schedule_interval_seconds = sanitized.get("schedule_interval_seconds")
        schedule_cron = sanitized.get("schedule_cron")
        schedule = sanitized.get("schedule") if isinstance(sanitized.get("schedule"), dict) else None
        proposal_submitted_at = sanitized.get("proposal_submitted_at")
        proposal_reviewed_at = sanitized.get("proposal_reviewed_at")
        proposal_review_comment = sanitized.get("proposal_review_comment")
        if schedule:
            schedule_interval_seconds = schedule.get("interval_seconds", schedule_interval_seconds)
            schedule_cron = schedule.get("cron", schedule_cron)
        if schedule_interval_seconds is not None:
            try:
                schedule_interval_seconds = int(schedule_interval_seconds)
            except Exception:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="schedule_interval_seconds must be integer")
            if schedule_interval_seconds <= 0:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="schedule_interval_seconds must be > 0")
        schedule_cron = str(schedule_cron).strip() if schedule_cron else None
        if schedule_interval_seconds and schedule_cron:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provide either schedule_interval_seconds or schedule_cron (not both)")
        if schedule_cron and not _is_valid_cron_expression(schedule_cron):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="schedule_cron must be a supported 5-field cron expression")
        proposal_submitted_at = None if not proposal_submitted_at else proposal_submitted_at
        proposal_reviewed_at = None if not proposal_reviewed_at else proposal_reviewed_at
        proposal_review_comment = str(proposal_review_comment).strip() if proposal_review_comment else None
        branch = str(sanitized.get("branch") or "").strip() or None
        expectations = sanitized.get("expectations") if isinstance(sanitized.get("expectations"), list) else None
        schema_contract = (
            sanitized.get("schema_contract") if isinstance(sanitized.get("schema_contract"), list) else None
        )
        if expectations is not None:
            definition_json = {**definition_json, "expectations": expectations}
        if schema_contract is not None:
            definition_json = {**definition_json, "schemaContract": schema_contract}

        update = await pipeline_registry.update_pipeline(
            pipeline_id=pipeline_id,
            name=str(sanitized.get("name") or "").strip() or None,
            description=str(sanitized.get("description") or "").strip() or None,
            location=(_normalize_location(str(sanitized.get("location") or "")) if "location" in sanitized else None),
            status=str(sanitized.get("status") or "").strip() or None,
            schedule_interval_seconds=schedule_interval_seconds,
            schedule_cron=schedule_cron,
            branch=branch,
            proposal_status=str(sanitized.get("proposal_status") or "").strip() or None,
            proposal_title=str(sanitized.get("proposal_title") or "").strip() or None,
            proposal_description=str(sanitized.get("proposal_description") or "").strip() or None,
            proposal_submitted_at=proposal_submitted_at,
            proposal_reviewed_at=proposal_reviewed_at,
            proposal_review_comment=proposal_review_comment,
        )
        version = None
        if definition_json is not None:
            version = await pipeline_registry.add_version(
                pipeline_id=pipeline_id,
                branch=branch or update.branch,
                definition_json=definition_json,
            )
        if dependencies is not None:
            await pipeline_registry.replace_dependencies(pipeline_id=pipeline_id, dependencies=dependencies)
        dependencies_payload = await pipeline_registry.list_dependencies(pipeline_id=pipeline_id)
        dependencies_for_api = _format_dependencies_for_api(dependencies_payload)
        if definition_json is not None and dependencies_for_api:
            definition_json = {**definition_json, "dependencies": dependencies_for_api}

        if definition_json is not None:
            diff_payload = _definition_diff(previous_definition, definition_json)
        else:
            diff_payload = {}

        event = build_command_event(
            event_type="PIPELINE_UPDATED",
            aggregate_type="Pipeline",
            aggregate_id=pipeline_id,
            data={
                "pipeline_id": pipeline_id,
                "definition_json": definition_json,
                "version_id": version.version_id if version else None,
                "commit_id": version.lakefs_commit_id if version else None,
                "version": version.lakefs_commit_id if version else None,
                "branch": branch or update.branch,
            },
            command_type="UPDATE_PIPELINE",
        )
        event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
        try:
            await event_store.connect()
            await event_store.append_event(event)
        except Exception as e:
            logger.warning(f"Failed to append pipeline update event: {e}")

        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_UPDATED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={
                "branch": branch or update.branch,
                "commit_id": version.lakefs_commit_id if version else None,
                "definition_diff": diff_payload,
            },
        )

        return ApiResponse.success(
            message="Pipeline updated",
            data={
                "pipeline": {
                    **update.__dict__,
                    "definition_json": definition_json if definition_json is not None else None,
                    "version_id": version.version_id if version else None,
                    "commit_id": version.lakefs_commit_id if version else None,
                    "version": version.lakefs_commit_id if version else None,
                    "dependencies": dependencies_for_api,
                }
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_UPDATED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(e),
        )
        logger.error(f"Failed to update pipeline: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/{pipeline_id}/preview", response_model=ApiResponse)
@trace_endpoint("preview_pipeline")
async def preview_pipeline(
    pipeline_id: str,
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    pipeline_job_queue: PipelineJobQueue = Depends(get_pipeline_job_queue),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="edit",
        )
        sanitized = sanitize_input(payload)
        limit = int(sanitized.get("limit") or 200)
        limit = max(1, min(limit, 500))
        definition_json = sanitized.get("definition_json") if isinstance(sanitized.get("definition_json"), dict) else None
        node_id = str(sanitized.get("node_id") or "").strip() or None
        db_name = str(sanitized.get("db_name") or "").strip()
        expectations = sanitized.get("expectations") if isinstance(sanitized.get("expectations"), list) else None
        schema_contract = (
            sanitized.get("schema_contract") if isinstance(sanitized.get("schema_contract"), list) else None
        )
        branch = str(sanitized.get("branch") or "").strip() or None
        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
        branch_state = await pipeline_registry.get_pipeline_branch(db_name=pipeline.db_name, branch=pipeline.branch)
        if branch_state and branch_state.get("archived"):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Archived branch cannot be built; restore the branch first",
            )
        if branch and branch != pipeline.branch:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="branch does not match pipeline branch")
        latest = await pipeline_registry.get_latest_version(
            pipeline_id=pipeline_id,
            branch=pipeline.branch,
        )
        if not definition_json:
            definition_json = latest.definition_json if latest else {}
        if not db_name:
            db_name = pipeline.db_name if pipeline else ""
        if not db_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="db_name is required")

        if expectations is not None:
            definition_json = {**definition_json, "expectations": expectations}
        if schema_contract is not None:
            definition_json = {**definition_json, "schemaContract": schema_contract}

        definition_hash = _stable_definition_hash(definition_json)
        definition_commit_id = _resolve_definition_commit_id(definition_json, latest, definition_hash)
        node_key = (node_id or "pipeline").replace("node-", "").replace("/", "-").replace(" ", "-")
        node_key = node_key[:16] if node_key else "pipeline"
        hash_key = str(definition_hash or uuid4().hex)[:12]
        job_id = f"preview-{pipeline_id}-{hash_key}-{node_key}"

        existing_run = await pipeline_registry.get_run(pipeline_id=pipeline_id, job_id=job_id)
        if existing_run:
            status_value = str(existing_run.get("status") or "").upper()
            sample_json_existing = (
                existing_run.get("sample_json") if isinstance(existing_run.get("sample_json"), dict) else {}
            )
            if status_value in {"SUCCESS", "FAILED"} and sample_json_existing:
                return ApiResponse.success(
                    message="Pipeline preview",
                    data={
                        "pipeline_id": pipeline_id,
                        "job_id": job_id,
                        "limit": limit,
                        "sample": sample_json_existing,
                    },
                ).to_dict()
            return ApiResponse.success(
                message="Pipeline preview",
                data={
                    "pipeline_id": pipeline_id,
                    "job_id": job_id,
                    "limit": limit,
                    "sample": {"queued": True, "limit": limit, "job_id": job_id},
                },
            ).to_dict()
        await pipeline_registry.record_preview(
            pipeline_id=pipeline_id,
            status="QUEUED",
            row_count=0,
            sample_json={"queued": True, "limit": limit, "job_id": job_id},
            job_id=job_id,
            node_id=node_id,
        )

        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=job_id,
            mode="preview",
            status="QUEUED",
            node_id=node_id,
            sample_json={"queued": True, "limit": limit},
        )

        preview_definition = dict(definition_json)
        preview_definition["__preview_meta__"] = {
            "branch": branch or "main",
        }
        job = PipelineJob(
            job_id=job_id,
            pipeline_id=pipeline_id,
            db_name=db_name,
            pipeline_type=pipeline.pipeline_type if pipeline else "batch",
            definition_json=preview_definition,
            definition_hash=definition_hash,
            definition_commit_id=definition_commit_id,
            node_id=node_id,
            output_dataset_name="preview_output",
            mode="preview",
            preview_limit=limit,
            branch=pipeline.branch,
        )
        try:
            await pipeline_job_queue.publish(job, require_delivery=False)
        except Exception as exc:
            error = str(exc)
            await pipeline_registry.record_preview(
                pipeline_id=pipeline_id,
                status="FAILED",
                row_count=0,
                sample_json={"queued": False, "job_id": job_id, "errors": [error]},
                job_id=job_id,
                node_id=node_id,
            )
            await pipeline_registry.record_run(
                pipeline_id=pipeline_id,
                job_id=job_id,
                mode="preview",
                status="FAILED",
                node_id=node_id,
                sample_json={"errors": [error]},
                finished_at=utcnow(),
            )
            logger.error("Failed to enqueue pipeline preview job %s: %s", job_id, exc)
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Failed to enqueue preview job")

        sample_payload = {"queued": True, "job_id": job_id}

        event = build_command_event(
            event_type="PIPELINE_PREVIEWED",
            aggregate_type="Pipeline",
            aggregate_id=pipeline_id,
            data={"pipeline_id": pipeline_id, "row_limit": limit},
            command_type="RUN_PIPELINE_PREVIEW",
        )
        try:
            await event_store.connect()
            await event_store.append_event(event)
        except Exception as e:
            logger.warning(f"Failed to append pipeline preview event: {e}")

        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PREVIEW_REQUESTED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={"job_id": job_id, "node_id": node_id, "limit": limit},
        )

        return ApiResponse.success(
            message="Preview queued",
            data={"pipeline_id": pipeline_id, "job_id": job_id, "limit": limit, "sample": sample_payload},
        ).to_dict()
    except ValueError as e:
        await pipeline_registry.record_preview(
            pipeline_id=pipeline_id,
            status="FAILED",
            row_count=0,
            sample_json={"errors": [str(e)]},
            node_id=node_id,
        )
        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=f"preview-{pipeline_id}-invalid",
            mode="preview",
            status="FAILED",
            node_id=node_id,
            sample_json={"errors": [str(e)]},
            finished_at=utcnow(),
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_PREVIEW_REQUESTED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(e),
        )
        logger.error(f"Failed to preview pipeline: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/{pipeline_id}/build", response_model=ApiResponse)
@trace_endpoint("build_pipeline")
async def build_pipeline(
    pipeline_id: str,
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    pipeline_job_queue: PipelineJobQueue = Depends(get_pipeline_job_queue),
    oms_client: OMSClient = Depends(get_oms_client),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="edit",
        )
        sanitized = sanitize_input(payload)
        limit = int(sanitized.get("limit") or 200)
        limit = max(1, min(limit, 500))
        definition_json = sanitized.get("definition_json") if isinstance(sanitized.get("definition_json"), dict) else None
        node_id = str(sanitized.get("node_id") or "").strip() or None
        db_name = str(sanitized.get("db_name") or "").strip()
        expectations = sanitized.get("expectations") if isinstance(sanitized.get("expectations"), list) else None
        schema_contract = (
            sanitized.get("schema_contract") if isinstance(sanitized.get("schema_contract"), list) else None
        )
        branch = str(sanitized.get("branch") or "").strip() or None

        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
        is_streaming_pipeline = str(pipeline.pipeline_type or "").strip().lower() in {"stream", "streaming"}
        if is_streaming_pipeline:
            node_id = None
        branch_state = await pipeline_registry.get_pipeline_branch(db_name=pipeline.db_name, branch=pipeline.branch)
        if branch_state and branch_state.get("archived"):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Archived branch cannot be deployed; restore the branch first",
            )
        if branch and branch != pipeline.branch:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="branch does not match pipeline branch")
        latest = await pipeline_registry.get_latest_version(
            pipeline_id=pipeline_id,
            branch=pipeline.branch,
        )
        if not definition_json:
            definition_json = latest.definition_json if latest else {}
        if not db_name:
            db_name = pipeline.db_name if pipeline else ""
        if not db_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="db_name is required")
        db_name = validate_db_name(db_name)

        if expectations is not None:
            definition_json = {**definition_json, "expectations": expectations}
        if schema_contract is not None:
            definition_json = {**definition_json, "schemaContract": schema_contract}

        definition_hash = _stable_definition_hash(definition_json)
        definition_commit_id = _resolve_definition_commit_id(definition_json, latest, definition_hash)

        job_id = f"build-{pipeline_id}-{uuid4()}"
        await pipeline_registry.record_run(
            pipeline_id=pipeline_id,
            job_id=job_id,
            mode="build",
            status="QUEUED",
            node_id=node_id,
            output_json={"queued": True, "limit": limit},
        )

        ontology_branch = branch or pipeline.branch or "main"
        ontology_head_commit_id: Optional[str] = None
        try:
            head_response = await oms_client.get_version_head(db_name, branch=ontology_branch)
            if isinstance(head_response, dict) and head_response.get("status") == "success":
                data = head_response.get("data") if isinstance(head_response.get("data"), dict) else {}
                ontology_head_commit_id = str(data.get("head_commit_id") or "").strip() or None
        except Exception as exc:
            logger.warning("Failed to resolve ontology head commit (db=%s branch=%s): %s", db_name, ontology_branch, exc)

        build_definition = dict(definition_json)
        build_definition["__build_meta__"] = {
            "branch": ontology_branch,
            "ontology": {"branch": ontology_branch, "commit": ontology_head_commit_id},
        }
        job = PipelineJob(
            job_id=job_id,
            pipeline_id=pipeline_id,
            db_name=db_name,
            pipeline_type=pipeline.pipeline_type if pipeline else "batch",
            definition_json=build_definition,
            definition_hash=definition_hash,
            definition_commit_id=definition_commit_id,
            node_id=node_id,
            output_dataset_name="build_output",
            mode="build",
            preview_limit=limit,
            branch=pipeline.branch,
        )
        try:
            await pipeline_job_queue.publish(job)
        except Exception as exc:
            error = str(exc)
            await pipeline_registry.record_run(
                pipeline_id=pipeline_id,
                job_id=job_id,
                mode="build",
                status="FAILED",
                node_id=node_id,
                output_json={"errors": [error]},
                finished_at=utcnow(),
            )
            logger.error("Failed to enqueue pipeline build job %s: %s", job_id, exc)
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Failed to enqueue build job")

        principal_type, principal_id = _resolve_principal(request)
        await emit_pipeline_control_plane_event(
            event_type="PIPELINE_BUILD_REQUESTED",
            pipeline_id=pipeline_id,
            event_id=f"build-requested-{job_id}",
            actor=principal_id,
            data={
                "pipeline_id": pipeline_id,
                "job_id": job_id,
                "db_name": db_name,
                "branch": pipeline.branch,
                "node_id": node_id,
                "definition_hash": definition_hash,
                "limit": limit,
                "principal_type": principal_type,
                "principal_id": principal_id,
            },
        )

        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BUILD_REQUESTED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={"job_id": job_id, "node_id": node_id, "limit": limit},
        )

        return ApiResponse.success(
            message="Build queued",
            data={"pipeline_id": pipeline_id, "job_id": job_id, "limit": limit},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BUILD_REQUESTED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(e),
        )
        logger.error(f"Failed to build pipeline: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/{pipeline_id}/deploy", response_model=ApiResponse)
@trace_endpoint("deploy_pipeline")
async def deploy_pipeline(
    pipeline_id: str,
    payload: Dict[str, Any],
    request: Request,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    oms_client: OMSClient = Depends(get_oms_client),
    *,
    lineage_store: LineageStoreDep,
    audit_store: AuditLogStoreDep,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="approve",
        )
        sanitized = sanitize_input(payload)
        output = sanitized.get("output") if isinstance(sanitized.get("output"), dict) else {}
        definition_json = sanitized.get("definition_json") if isinstance(sanitized.get("definition_json"), dict) else None
        promote_build = bool(sanitized.get("promote_build") or sanitized.get("promoteBuild") or False)
        build_job_id = str(sanitized.get("build_job_id") or sanitized.get("buildJobId") or "").strip() or None
        artifact_id = str(sanitized.get("artifact_id") or sanitized.get("artifactId") or "").strip() or None
        replay_on_deploy = bool(
            sanitized.get("replay")
            or sanitized.get("replay_on_deploy")
            or sanitized.get("replayOnDeploy")
            or sanitized.get("replay_on_deploy")
        )
        dependencies_raw: Any = None
        if "dependencies" in sanitized:
            dependencies_raw = sanitized.get("dependencies")
        elif isinstance(definition_json, dict) and "dependencies" in definition_json:
            dependencies_raw = definition_json.get("dependencies")
        dependencies: Optional[list[dict[str, str]]] = None
        node_id = str(sanitized.get("node_id") or "").strip() or None
        dataset_name = str(output.get("dataset_name") or "").strip()
        db_name = str(output.get("db_name") or "").strip()
        outputs = sanitized.get("outputs") if isinstance(sanitized.get("outputs"), list) else None
        expectations = sanitized.get("expectations") if isinstance(sanitized.get("expectations"), list) else None
        schema_contract = (
            sanitized.get("schema_contract") if isinstance(sanitized.get("schema_contract"), list) else None
        )
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
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="schedule_interval_seconds must be integer")
            if schedule_interval_seconds <= 0:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="schedule_interval_seconds must be > 0")
        if schedule_interval_seconds and schedule_cron:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provide either schedule_interval_seconds or schedule_cron (not both)")
        if schedule_cron and not _is_valid_cron_expression(str(schedule_cron)):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="schedule_cron must be a supported 5-field cron expression")

        branch = str(sanitized.get("branch") or "").strip() or None
        proposal_status = str(sanitized.get("proposal_status") or "").strip() or None
        proposal_title = str(sanitized.get("proposal_title") or "").strip() or None
        proposal_description = str(sanitized.get("proposal_description") or "").strip() or None
        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
        if branch and branch != pipeline.branch:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="branch does not match pipeline branch")
        if dependencies_raw is not None:
            dependencies = _normalize_dependencies_payload(dependencies_raw)
            await _validate_dependency_targets(
                pipeline_registry,
                db_name=str(pipeline.db_name),
                pipeline_id=pipeline_id,
                dependencies=dependencies,
            )

        resolved_branch = branch or (pipeline.branch if pipeline else None) or "main"
        proposal_required = _pipeline_requires_proposal(resolved_branch)
        proposal_bundle = getattr(pipeline, "proposal_bundle", {}) if pipeline else {}
        if proposal_required:
            if getattr(pipeline, "proposal_status", None) != "approved":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Approved proposal is required for protected branches",
                )
            if not proposal_bundle:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Approved proposal is missing bundle metadata",
                )
        latest = await pipeline_registry.get_latest_version(
            pipeline_id=pipeline_id,
            branch=resolved_branch,
        )
        if not definition_json:
            definition_json = latest.definition_json if latest else {}
        if expectations is not None:
            definition_json = {**(definition_json or {}), "expectations": expectations}
        if schema_contract is not None:
            definition_json = {**(definition_json or {}), "schemaContract": schema_contract}
        if outputs:
            definition_json = {**(definition_json or {}), "outputs": outputs}
        if not db_name:
            db_name = pipeline.db_name if pipeline else ""
        if not db_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="db_name is required")
        db_name = validate_db_name(db_name)

        definition_hash = _stable_definition_hash(definition_json or {})
        definition_commit_id = _resolve_definition_commit_id(definition_json or {}, latest, definition_hash)

        if not promote_build:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="deploy requires promote_build; run /build then /deploy with promote_build",
            )

        if promote_build:
            artifact_record = None
            if artifact_id:
                artifact_record = await pipeline_registry.get_artifact(artifact_id=artifact_id)
                if not artifact_record:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Build artifact not found")
                if artifact_record.pipeline_id != pipeline_id:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="artifact_id does not belong to this pipeline",
                    )
                if str(artifact_record.mode or "").lower() != "build":
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="artifact_id is not a build artifact",
                    )
                if str(artifact_record.status or "").upper() != "SUCCESS":
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="artifact_id is not a successful build artifact",
                    )
                if build_job_id and artifact_record.job_id != build_job_id:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="build_job_id does not match artifact_id",
                    )
                build_job_id = build_job_id or artifact_record.job_id

            if not build_job_id:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="build_job_id is required for promote_build")

            build_run = await pipeline_registry.get_run(pipeline_id=pipeline_id, job_id=build_job_id)
            if not build_run:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Build run not found")
            if str(build_run.get("mode") or "").lower() != "build":
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="build_job_id is not a build run")
            build_status = str(build_run.get("status") or "").upper()
            if build_status != "SUCCESS":
                output_json = build_run.get("output_json")
                errors: list[str] = []
                if isinstance(output_json, dict):
                    raw_errors = output_json.get("errors")
                    if isinstance(raw_errors, list):
                        errors = [str(item) for item in raw_errors if str(item).strip()]
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "BUILD_NOT_SUCCESS",
                        "message": "Build is not successful yet",
                        "build_status": build_status or None,
                        "errors": errors,
                        "build_job_id": build_job_id,
                    },
                )

            output_json = build_run.get("output_json") or {}
            if not isinstance(output_json, dict):
                output_json = {}
            build_hash = str(output_json.get("definition_hash") or "").strip()
            if build_hash and build_hash != definition_hash:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Build definition does not match deploy definition")
            if artifact_record and artifact_record.definition_hash and artifact_record.definition_hash != definition_hash:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Build artifact definition does not match deploy definition",
                )

            if artifact_record is None:
                artifact_record = await pipeline_registry.get_artifact_by_job(
                    pipeline_id=pipeline_id,
                    job_id=build_job_id,
                    mode="build",
                )

            build_branch = str(output_json.get("branch") or "").strip() or "main"
            if build_branch != resolved_branch:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Build branch does not match deploy branch",
                )

            build_ontology = output_json.get("ontology") if isinstance(output_json.get("ontology"), dict) else {}
            build_ontology_commit = str(build_ontology.get("commit") or "").strip() or None
            if not build_ontology_commit:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "ONTOLOGY_VERSION_UNKNOWN",
                        "message": "Build output is missing ontology commit id; re-run build after ontology is ready",
                    },
                )

            if proposal_required:
                bundle_build_job_id = str(proposal_bundle.get("build_job_id") or "").strip()
                if not bundle_build_job_id:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="Approved proposal is missing build_job_id",
                    )
                if bundle_build_job_id != build_job_id:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "code": "PROPOSAL_BUILD_MISMATCH",
                            "message": "Approved proposal build does not match deploy build",
                            "proposal_build_job_id": bundle_build_job_id,
                            "deploy_build_job_id": build_job_id,
                        },
                    )
                bundle_artifact_id = str(proposal_bundle.get("artifact_id") or "").strip()
                if bundle_artifact_id and artifact_record and bundle_artifact_id != artifact_record.artifact_id:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "code": "PROPOSAL_ARTIFACT_MISMATCH",
                            "message": "Approved proposal artifact does not match deploy artifact",
                            "proposal_artifact_id": bundle_artifact_id,
                            "deploy_artifact_id": artifact_record.artifact_id,
                        },
                    )
                bundle_definition_hash = str(proposal_bundle.get("definition_hash") or "").strip()
                if bundle_definition_hash and bundle_definition_hash != definition_hash:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "code": "PROPOSAL_DEFINITION_MISMATCH",
                            "message": "Approved proposal definition hash does not match deploy definition",
                            "proposal_definition_hash": bundle_definition_hash,
                            "deploy_definition_hash": definition_hash,
                        },
                    )
                bundle_ontology_commit = str(
                    (proposal_bundle.get("ontology") or {}).get("commit") or ""
                ).strip()
                if bundle_ontology_commit and bundle_ontology_commit != build_ontology_commit:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "code": "PROPOSAL_ONTOLOGY_MISMATCH",
                            "message": "Approved proposal ontology commit does not match build",
                            "proposal_ontology_commit": bundle_ontology_commit,
                            "build_ontology_commit": build_ontology_commit,
                        },
                    )
                bundle_lakefs_commit = str(
                    (proposal_bundle.get("lakefs") or {}).get("commit_id") or ""
                ).strip()
                build_lakefs_commit = str((output_json.get("lakefs") or {}).get("commit_id") or "").strip()
                if bundle_lakefs_commit and build_lakefs_commit and bundle_lakefs_commit != build_lakefs_commit:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "code": "PROPOSAL_LAKEFS_MISMATCH",
                            "message": "Approved proposal lakeFS commit does not match build",
                            "proposal_commit_id": bundle_lakefs_commit,
                            "build_commit_id": build_lakefs_commit,
                        },
                    )
                mapping_specs_payload = proposal_bundle.get("mapping_specs")
                if isinstance(mapping_specs_payload, list):
                    mismatches: list[dict[str, Any]] = []
                    for item in mapping_specs_payload:
                        if not isinstance(item, dict):
                            continue
                        dataset_id = str(item.get("dataset_id") or "").strip()
                        dataset_branch = str(item.get("dataset_branch") or resolved_branch).strip() or resolved_branch
                        target_class_id = str(item.get("target_class_id") or "").strip() or None
                        expected_spec_id = str(item.get("mapping_spec_id") or "").strip()
                        expected_version = item.get("mapping_spec_version")
                        if not dataset_id:
                            mismatches.append(
                                {
                                    "reason": "missing_dataset_id",
                                    "mapping_spec_id": expected_spec_id or None,
                                }
                            )
                            continue
                        current = await objectify_registry.get_active_mapping_spec(
                            dataset_id=dataset_id,
                            dataset_branch=dataset_branch,
                            target_class_id=target_class_id,
                        )
                        if not current:
                            mismatches.append(
                                {
                                    "reason": "mapping_spec_inactive",
                                    "dataset_id": dataset_id,
                                    "dataset_branch": dataset_branch,
                                    "target_class_id": target_class_id,
                                    "mapping_spec_id": expected_spec_id or None,
                                }
                            )
                            continue
                        if expected_spec_id and current.mapping_spec_id != expected_spec_id:
                            mismatches.append(
                                {
                                    "reason": "mapping_spec_id_mismatch",
                                    "dataset_id": dataset_id,
                                    "dataset_branch": dataset_branch,
                                    "target_class_id": target_class_id,
                                    "proposal_mapping_spec_id": expected_spec_id,
                                    "current_mapping_spec_id": current.mapping_spec_id,
                                }
                            )
                        if expected_version is not None:
                            try:
                                expected_version_int = int(expected_version)
                            except Exception:
                                expected_version_int = None
                            if expected_version_int is not None and expected_version_int != current.version:
                                mismatches.append(
                                    {
                                        "reason": "mapping_spec_version_mismatch",
                                        "dataset_id": dataset_id,
                                        "dataset_branch": dataset_branch,
                                        "target_class_id": target_class_id,
                                        "proposal_version": expected_version_int,
                                        "current_version": current.version,
                                    }
                                )
                    if mismatches:
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail={
                                "code": "MAPPING_SPEC_MISMATCH",
                                "message": "Approved proposal mapping specs no longer match active specs",
                                "mismatches": mismatches,
                            },
                        )
            try:
                head_response = await oms_client.get_version_head(db_name, branch=resolved_branch)
                head_data = head_response.get("data") if isinstance(head_response, dict) else {}
                prod_head_commit = (
                    str(head_data.get("head_commit_id") or "").strip()
                    if isinstance(head_data, dict)
                    else ""
                )
                prod_head_commit = prod_head_commit or None
            except Exception as exc:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "code": "ONTOLOGY_GATE_UNAVAILABLE",
                        "message": "Failed to verify ontology head commit; OMS unavailable",
                        "error": str(exc),
                    },
                ) from exc
            if not prod_head_commit:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "ONTOLOGY_VERSION_UNKNOWN",
                        "message": "Target branch ontology head commit is unknown; publish ontology first",
                        "target_branch": resolved_branch,
                    },
                )
            if build_ontology_commit != prod_head_commit:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "ONTOLOGY_VERSION_MISMATCH",
                        "message": "Ontology version mismatch; publish ontology and rebuild before deploy",
                        "bundle_terminus_commit_id": build_ontology_commit,
                        "prod_head_commit_id": prod_head_commit,
                        "target_branch": resolved_branch,
                    },
                )

            outputs_payload = None
            if artifact_record and artifact_record.outputs:
                outputs_payload = artifact_record.outputs
            if outputs_payload is None:
                outputs_payload = output_json.get("outputs")
            outputs_list: list[dict[str, Any]] = []
            if isinstance(outputs_payload, list):
                outputs_list = [item for item in outputs_payload if isinstance(item, dict)]

            execution_semantics = str(output_json.get("execution_semantics") or "").strip().lower()
            is_streaming_promotion = execution_semantics == "streaming" or str(pipeline.pipeline_type or "").strip().lower() in {"stream", "streaming"}
            if not node_id and not is_streaming_promotion:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="node_id is required for promote_build")
            selected_outputs: list[dict[str, Any]]
            if is_streaming_promotion:
                if not outputs_list:
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Build outputs are missing")
                selected_outputs = outputs_list
            else:
                selected_output = None
                for item in outputs_list:
                    if str(item.get("node_id") or "").strip() == str(node_id).strip():
                        selected_output = item
                        break
                if not selected_output:
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Build output for node_id not found")
                selected_outputs = [selected_output]

            first_artifact_key = str(selected_outputs[0].get("artifact_key") or "").strip()
            if not first_artifact_key:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Build staged artifact_key is missing")
            parsed_first = parse_s3_uri(first_artifact_key)
            if not parsed_first:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Build artifact_key is not a valid s3:// URI")
            staged_bucket, _ = parsed_first

            lakefs_meta = output_json.get("lakefs") if isinstance(output_json.get("lakefs"), dict) else {}
            if artifact_record:
                if artifact_record.lakefs_repository:
                    lakefs_meta.setdefault("repository", artifact_record.lakefs_repository)
                if artifact_record.lakefs_branch:
                    lakefs_meta.setdefault("build_branch", artifact_record.lakefs_branch)
                if artifact_record.lakefs_commit_id:
                    lakefs_meta.setdefault("commit_id", artifact_record.lakefs_commit_id)
            expected_repo = str(lakefs_meta.get("repository") or "").strip()
            expected_build_branch = str(lakefs_meta.get("build_branch") or "").strip()
            if expected_repo and expected_repo != staged_bucket:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "INVALID_BUILD_REPOSITORY",
                        "message": "Build artifact_key repository does not match build metadata",
                        "artifact_bucket": staged_bucket,
                        "expected_repository": expected_repo,
                    },
                )
            if not expected_build_branch:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "INVALID_BUILD_REF",
                        "message": "Build output is missing lakefs.build_branch",
                    },
                )
            build_ref = expected_build_branch
            staged_prefix = f"{build_ref}/"
            normalized_outputs: list[dict[str, Any]] = []
            any_breaking_changes: list[dict[str, Any]] = []
            for item in selected_outputs:
                staged_dataset_name = str(item.get("dataset_name") or item.get("datasetName") or "").strip()
                if not staged_dataset_name:
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Build dataset_name is missing")
                staged_artifact_key = str(item.get("artifact_key") or "").strip()
                if not staged_artifact_key:
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Build staged artifact_key is missing")
                parsed = parse_s3_uri(staged_artifact_key)
                if not parsed:
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Build artifact_key is not a valid s3:// URI")
                bucket, key = parsed
                if bucket != staged_bucket:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "code": "INVALID_BUILD_REPOSITORY",
                            "message": "Build outputs must use a single lakeFS repository",
                            "artifact_bucket": bucket,
                            "expected_repository": staged_bucket,
                        },
                    )
                if not key.startswith(staged_prefix):
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "code": "INVALID_BUILD_REF",
                            "message": "Build artifact_key does not match build branch metadata",
                            "artifact_key": staged_artifact_key,
                            "artifact_path": key,
                            "expected_build_branch": build_ref,
                        },
                    )
                artifact_path = key[len(staged_prefix) :]
                if not artifact_path:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "code": "INVALID_BUILD_REF",
                            "message": "Build artifact_key is missing artifact path after build ref",
                            "artifact_key": staged_artifact_key,
                            "expected_build_branch": build_ref,
                        },
                    )

                schema_columns = item.get("columns") if isinstance(item.get("columns"), list) else []
                sample_rows = item.get("rows") if isinstance(item.get("rows"), list) else []
                normalized_sample_rows = [row for row in sample_rows if isinstance(row, dict)]
                row_count_raw = item.get("row_count")
                delta_row_count_raw = item.get("delta_row_count") if "delta_row_count" in item else item.get("deltaRowCount")
                try:
                    row_count_int = int(row_count_raw) if row_count_raw is not None else None
                except Exception:
                    row_count_int = None
                try:
                    delta_row_count_int = int(delta_row_count_raw) if delta_row_count_raw is not None else None
                except Exception:
                    delta_row_count_int = None

                dataset = await dataset_registry.get_dataset_by_name(
                    db_name=db_name,
                    name=staged_dataset_name,
                    branch=resolved_branch,
                )
                breaking_changes: list[dict[str, str]] = []
                if dataset:
                    breaking_changes = _detect_breaking_schema_changes(
                        previous_schema=dataset.schema_json,
                        next_columns=schema_columns,
                    )
                    if breaking_changes:
                        any_breaking_changes.append(
                            {
                                "dataset_name": staged_dataset_name,
                                "node_id": item.get("node_id"),
                                "breaking_changes": breaking_changes,
                            }
                        )

                normalized_outputs.append(
                    {
                        "node_id": item.get("node_id"),
                        "dataset_name": staged_dataset_name,
                        "build_artifact_key": staged_artifact_key,
                        "artifact_path": artifact_path,
                        "columns": schema_columns,
                        "rows": normalized_sample_rows,
                        "row_count": row_count_int,
                        "delta_row_count": delta_row_count_int,
                        "breaking_changes": breaking_changes,
                    }
                )

            if any_breaking_changes and not replay_on_deploy:
                breaking_payload: Any = any_breaking_changes
                if not is_streaming_promotion and normalized_outputs:
                    breaking_payload = normalized_outputs[0].get("breaking_changes") or []
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "REPLAY_REQUIRED",
                        "message": "Breaking schema change detected; replay_on_deploy is required to proceed",
                        "breaking_changes": breaking_payload,
                    },
                )

            promote_job_id = f"promote-{uuid4().hex}"
            principal_type, principal_id = _resolve_principal(request)
            await emit_pipeline_control_plane_event(
                event_type="PIPELINE_DEPLOY_REQUESTED",
                pipeline_id=pipeline_id,
                event_id=f"deploy-requested-{promote_job_id}",
                actor=principal_id,
                data={
                    "pipeline_id": pipeline_id,
                    "promote_job_id": promote_job_id,
                    "build_job_id": build_job_id,
                    "artifact_id": (artifact_record.artifact_id if artifact_record else None),
                    "db_name": db_name,
                    "branch": resolved_branch,
                    "node_id": node_id,
                    "definition_hash": definition_hash,
                    "replay_on_deploy": replay_on_deploy,
                    "principal_type": principal_type,
                    "principal_id": principal_id,
                },
            )

            actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
            lakefs_client = await pipeline_registry.get_lakefs_client(user_id=actor_user_id)
            publish_lock = await _acquire_pipeline_publish_lock(
                pipeline_id=str(pipeline_id),
                branch=resolved_branch,
                job_id=promote_job_id,
            )

            try:
                merge_commit_id = await lakefs_client.merge(
                    repository=staged_bucket,
                    source_ref=build_ref,
                    destination_branch=resolved_branch,
                    message=f"Promote build {build_job_id} -> {resolved_branch} (pipeline {pipeline.db_name}/{pipeline.name})",
                    metadata={
                        "pipeline_id": str(pipeline_id),
                        "db_name": db_name,
                        "pipeline_name": str(pipeline.name),
                        "build_job_id": str(build_job_id),
                        "node_id": str(node_id) if node_id else "*",
                    },
                    allow_empty=False,
                )
            except LakeFSConflictError as exc:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "LAKEFS_MERGE_CONFLICT",
                        "message": "lakeFS merge conflict during promotion",
                        "repository": staged_bucket,
                        "source_ref": build_ref,
                        "destination_branch": resolved_branch,
                        "error": str(exc),
                    },
                ) from exc
            except LakeFSError as exc:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "code": "LAKEFS_MERGE_FAILED",
                        "message": "lakeFS merge failed during promotion",
                        "repository": staged_bucket,
                        "source_ref": build_ref,
                        "destination_branch": resolved_branch,
                        "error": str(exc),
                    },
                ) from exc
            finally:
                if publish_lock:
                    await _release_pipeline_publish_lock(*publish_lock)

            build_outputs: list[dict[str, Any]] = []
            for item in normalized_outputs:
                staged_dataset_name = str(item.get("dataset_name") or "")
                artifact_path = str(item.get("artifact_path") or "")
                promoted_artifact_key = build_s3_uri(staged_bucket, f"{merge_commit_id}/{artifact_path}")
                schema_columns = item.get("columns") if isinstance(item.get("columns"), list) else []
                normalized_sample_rows = item.get("rows") if isinstance(item.get("rows"), list) else []
                row_count_int = item.get("row_count")
                delta_row_count_int = item.get("delta_row_count")

                dataset = await dataset_registry.get_dataset_by_name(
                    db_name=db_name,
                    name=staged_dataset_name,
                    branch=resolved_branch,
                )
                if not dataset:
                    dataset = await dataset_registry.create_dataset(
                        db_name=db_name,
                        name=staged_dataset_name,
                        description=None,
                        source_type="pipeline",
                        source_ref=str(pipeline_id),
                        schema_json={"columns": schema_columns},
                        branch=resolved_branch,
                    )

                sample_json: dict[str, Any] = {
                    "columns": schema_columns,
                    "rows": normalized_sample_rows,
                    "row_count": row_count_int,
                    "sample_row_count": len(normalized_sample_rows),
                    "column_stats": compute_column_stats(rows=normalized_sample_rows, columns=schema_columns),
                }
                if delta_row_count_int is not None:
                    sample_json["delta_row_count"] = delta_row_count_int

                await dataset_registry.add_version(
                    dataset_id=dataset.dataset_id,
                    lakefs_commit_id=merge_commit_id,
                    artifact_key=promoted_artifact_key,
                    row_count=row_count_int,
                    sample_json=sample_json,
                    schema_json={"columns": schema_columns},
                    promoted_from_artifact_id=(artifact_record.artifact_id if artifact_record else None),
                )

                build_outputs.append(
                    {
                        "node_id": item.get("node_id"),
                        "dataset_name": staged_dataset_name,
                        "artifact_key": promoted_artifact_key,
                        "row_count": row_count_int,
                        "delta_row_count": delta_row_count_int,
                        "build_artifact_key": item.get("build_artifact_key"),
                        "build_ref": build_ref,
                        "artifact_path": artifact_path,
                        "merge_commit_id": merge_commit_id,
                        "breaking_changes": item.get("breaking_changes") or [],
                        "replay_on_deploy": replay_on_deploy,
                        "promoted_from_artifact_id": (artifact_record.artifact_id if artifact_record else None),
                    }
                )

            deployed_commit_id = merge_commit_id
            await pipeline_registry.record_build(
                pipeline_id=pipeline_id,
                status="DEPLOYED",
                output_json={
                    "outputs": build_outputs,
                    "ontology": build_ontology,
                    "promoted_from_build_job_id": build_job_id,
                    "promoted_from_artifact_id": (artifact_record.artifact_id if artifact_record else None),
                    "promote_job_id": promote_job_id,
                    "lakefs": {
                        "repository": staged_bucket,
                        "source_ref": build_ref,
                        "destination_branch": resolved_branch,
                        "merge_commit_id": merge_commit_id,
                    },
                },
                deployed_commit_id=deployed_commit_id,
            )
            await pipeline_registry.record_run(
                pipeline_id=pipeline_id,
                job_id=promote_job_id,
                mode="deploy",
                status="DEPLOYED",
                node_id=node_id,
                row_count=(build_outputs[0].get("row_count") if build_outputs else None),
                pipeline_spec_hash=definition_hash,
                pipeline_spec_commit_id=definition_commit_id,
                output_lakefs_commit_id=merge_commit_id,
                output_json={
                    "outputs": build_outputs,
                    "ontology": build_ontology,
                    "promoted_from_build_job_id": build_job_id,
                    "promoted_from_artifact_id": (artifact_record.artifact_id if artifact_record else None),
                    "lakefs": {
                        "repository": staged_bucket,
                        "source_ref": build_ref,
                        "destination_branch": resolved_branch,
                        "merge_commit_id": merge_commit_id,
                    },
                },
                finished_at=utcnow(),
            )

            await emit_pipeline_control_plane_event(
                event_type="PIPELINE_DEPLOY_PROMOTED",
                pipeline_id=pipeline_id,
                event_id=promote_job_id,
                actor=principal_id,
                data={
                    "pipeline_id": pipeline_id,
                    "promote_job_id": promote_job_id,
                    "build_job_id": build_job_id,
                    "artifact_id": (artifact_record.artifact_id if artifact_record else None),
                    "db_name": db_name,
                    "branch": resolved_branch,
                    "node_id": node_id,
                    "merge_commit_id": merge_commit_id,
                    "definition_hash": definition_hash,
                    "replay_on_deploy": replay_on_deploy,
                    "principal_type": principal_type,
                    "principal_id": principal_id,
                },
            )

            if lineage_store:
                for item in build_outputs:
                    promoted_artifact_key = str(item.get("artifact_key") or "")
                    parsed = parse_s3_uri(promoted_artifact_key)
                    if not parsed:
                        continue
                    bucket, key = parsed
                    try:
                        await lineage_store.record_link(
                            from_node_id=lineage_store.node_aggregate("Pipeline", str(pipeline_id)),
                            to_node_id=lineage_store.node_artifact("s3", bucket, key),
                            edge_type="pipeline_output_stored",
                            occurred_at=utcnow(),
                            db_name=db_name,
                                edge_metadata={
                                    "db_name": db_name,
                                    "pipeline_id": str(pipeline_id),
                                    "artifact_key": promoted_artifact_key,
                                    "dataset_name": item.get("dataset_name"),
                                    "node_id": item.get("node_id"),
                                    "promoted_from_build_job_id": build_job_id,
                                    "promoted_from_artifact_id": (artifact_record.artifact_id if artifact_record else None),
                                    "build_artifact_key": item.get("build_artifact_key"),
                                    "build_ref": build_ref,
                                    "artifact_path": item.get("artifact_path"),
                                    "merge_commit_id": merge_commit_id,
                                },
                        )
                    except Exception as exc:
                        logger.warning("Lineage record_link failed (promotion deploy): %s", exc)

            if schedule_interval_seconds or schedule_cron or branch or proposal_status or proposal_title or proposal_description:
                await pipeline_registry.update_pipeline(
                    pipeline_id=pipeline_id,
                    schedule_interval_seconds=schedule_interval_seconds,
                    schedule_cron=schedule_cron,
                    branch=branch,
                    proposal_status=proposal_status,
                    proposal_title=proposal_title,
                    proposal_description=proposal_description,
                )
            if dependencies is not None:
                await pipeline_registry.replace_dependencies(pipeline_id=pipeline_id, dependencies=dependencies)

            await _log_pipeline_audit(
                audit_store,
                request=request,
                action="PIPELINE_DEPLOYED",
                status="success",
                pipeline_id=pipeline_id,
                metadata={
                    "promote_build": promote_build,
                    "build_job_id": build_job_id,
                    "artifact_id": (artifact_record.artifact_id if artifact_record else None),
                    "node_id": node_id,
                    "merge_commit_id": deployed_commit_id,
                    "branch": resolved_branch,
                    "replay_on_deploy": replay_on_deploy,
                },
            )

            return ApiResponse.success(
                message="Pipeline deployed (promoted from build)",
                data={
                    "pipeline_id": pipeline_id,
                    "job_id": promote_job_id,
                    "deployed_commit_id": deployed_commit_id,
                    "outputs": build_outputs,
                    "artifact_id": (artifact_record.artifact_id if artifact_record else None),
                },
            ).to_dict()

    except HTTPException:
        raise
    except Exception as e:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_DEPLOYED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(e),
        )
        logger.error(f"Failed to deploy pipeline: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/datasets", response_model=ApiResponse)
@trace_endpoint("create_dataset")
async def create_dataset(
    payload: Dict[str, Any],
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        sanitized = sanitize_input(payload)
        db_name = validate_db_name(str(sanitized.get("db_name") or ""))
        name = str(sanitized.get("name") or "").strip()
        if not name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="name is required")
        description = str(sanitized.get("description") or "").strip() or None
        source_type = str(sanitized.get("source_type") or "manual").strip() or "manual"
        source_ref = str(sanitized.get("source_ref") or "").strip() or None
        branch = str(sanitized.get("branch") or "main").strip() or "main"
        schema_json = sanitized.get("schema_json") if isinstance(sanitized.get("schema_json"), dict) else {}

        dataset = await dataset_registry.create_dataset(
            db_name=db_name,
            name=name,
            description=description,
            source_type=source_type,
            source_ref=source_ref,
            schema_json=schema_json,
            branch=branch,
        )

        event = build_command_event(
            event_type="DATASET_CREATED",
            aggregate_type="Dataset",
            aggregate_id=dataset.dataset_id,
            data={"dataset_id": dataset.dataset_id, "db_name": db_name, "name": name},
            command_type="CREATE_DATASET",
        )
        event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
        try:
            await event_store.connect()
            await event_store.append_event(event)
        except Exception as e:
            logger.warning(f"Failed to append dataset create event: {e}")

        return ApiResponse.success(
            message="Dataset created",
            data={"dataset": dataset.__dict__},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create dataset: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/datasets/{dataset_id}/versions", response_model=ApiResponse)
@trace_endpoint("create_dataset_version")
async def create_dataset_version(
    dataset_id: str,
    payload: Dict[str, Any],
    request: Request,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    objectify_job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
) -> ApiResponse:
    ingest_request = None
    ingest_transaction = None
    ingest_transaction = None
    ingest_transaction = None
    try:
        actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
        lakefs_storage_service = await pipeline_registry.get_lakefs_storage(user_id=actor_user_id)
        lakefs_client = await pipeline_registry.get_lakefs_client(user_id=actor_user_id)
        idempotency_key = (request.headers.get("Idempotency-Key") or "").strip()
        if not idempotency_key:
            idempotency_key = f"manual-{uuid4().hex}"

        sanitized = sanitize_input(payload)
        sample_json = sanitized.get("sample_json") if isinstance(sanitized.get("sample_json"), dict) else {}
        schema_json = sanitized.get("schema_json") if isinstance(sanitized.get("schema_json"), dict) else None
        row_count = None
        if sanitized.get("row_count") is not None:
            try:
                row_count = int(sanitized.get("row_count"))
            except Exception:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="row_count must be integer")
        if row_count is None and isinstance(sample_json, dict):
            rows = sample_json.get("rows")
            if isinstance(rows, list):
                row_count = len(rows)

        artifact_key = str(sanitized.get("artifact_key") or "").strip() or None
        lakefs_commit_id = str(
            sanitized.get("lakefs_commit_id") or sanitized.get("lakefsCommitId") or sanitized.get("commit_id") or ""
        ).strip() or None

        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
        try:
            enforce_db_scope(request.headers, db_name=dataset.db_name)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))

        dataset_branch = safe_lakefs_ref(dataset.branch or "main")
        if not hasattr(dataset_registry, "create_ingest_request"):
            if not artifact_key:
                repo = _resolve_lakefs_raw_repository()
                await _ensure_lakefs_branch_exists(
                    lakefs_client=lakefs_client,
                    repository=repo,
                    branch=dataset_branch,
                    source_branch="main",
                )
                prefix = _dataset_artifact_prefix(
                    db_name=dataset.db_name, dataset_id=dataset.dataset_id, dataset_name=dataset.name
                )
                object_key = f"{prefix}/data.json"
                await lakefs_storage_service.save_json(
                    repo,
                    f"{dataset_branch}/{object_key}",
                    sample_json,
                    metadata=_sanitize_s3_metadata(
                        {
                            "db_name": dataset.db_name,
                            "dataset_id": dataset.dataset_id,
                            "dataset_name": dataset.name,
                            "source": "manual",
                        }
                    ),
                )
                lakefs_commit_id = await lakefs_client.commit(
                    repository=repo,
                    branch=dataset_branch,
                    message=f"Manual dataset version {dataset.db_name}/{dataset.name}",
                    metadata={
                        "dataset_id": dataset.dataset_id,
                        "db_name": dataset.db_name,
                        "dataset_name": dataset.name,
                        "source_type": str(dataset.source_type or "manual"),
                    },
                )
                artifact_key = build_s3_uri(repo, f"{lakefs_commit_id}/{object_key}")
            version = await dataset_registry.add_version(
                dataset_id=dataset.dataset_id,
                lakefs_commit_id=lakefs_commit_id or "",
                artifact_key=artifact_key,
                row_count=row_count,
                sample_json=sample_json,
                schema_json=schema_json,
            )
            return ApiResponse.success(
                message="Dataset version created",
                data={"version": version.__dict__},
            ).to_dict()
        request_fingerprint = _build_ingest_request_fingerprint(
            {
                "dataset_id": dataset_id,
                "db_name": dataset.db_name,
                "branch": dataset_branch,
                "source_type": str(dataset.source_type or "manual"),
                "artifact_key": artifact_key,
                "lakefs_commit_id": lakefs_commit_id,
                "sample_json": sample_json,
                "row_count": row_count,
            }
        )
        ingest_request, _ = await dataset_registry.create_ingest_request(
            dataset_id=dataset_id,
            db_name=dataset.db_name,
            branch=dataset_branch,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            schema_json=schema_json,
            sample_json=sample_json,
            row_count=row_count,
        )
        ingest_transaction = await _ensure_ingest_transaction(
            dataset_registry,
            ingest_request_id=ingest_request.ingest_request_id,
        )
        if ingest_request.dataset_id != dataset_id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Idempotency key already used for a different dataset")
        if ingest_request.request_fingerprint and ingest_request.request_fingerprint != request_fingerprint:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Idempotency key reuse detected with different payload")
        if ingest_request.status == "FAILED":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=ingest_request.error or "Previous ingest failed")
        if ingest_request.status == "PUBLISHED":
            existing_version = await dataset_registry.get_version_by_ingest_request(
                ingest_request_id=ingest_request.ingest_request_id
            )
            if existing_version:
                await flush_dataset_ingest_outbox(
                    dataset_registry=dataset_registry,
                    lineage_store=None,
                )
                return ApiResponse.success(
                    message="Dataset version created",
                    data={"version": existing_version.__dict__},
                ).to_dict()

        if artifact_key:
            ref = _extract_lakefs_ref_from_artifact_key(artifact_key)
            if ref == dataset_branch:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="artifact_key must reference an immutable lakeFS commit id (not a branch ref)",
                )
            if lakefs_commit_id and lakefs_commit_id != ref:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="lakefs_commit_id does not match artifact_key ref")
            lakefs_commit_id = lakefs_commit_id or ref
        else:
            # For manual datasets, materialize the sample payload into lakeFS and commit a new version.
            repo = _resolve_lakefs_raw_repository()
            await _ensure_lakefs_branch_exists(
                lakefs_client=lakefs_client,
                repository=repo,
                branch=dataset_branch,
                source_branch="main",
            )
            prefix = _dataset_artifact_prefix(db_name=dataset.db_name, dataset_id=dataset.dataset_id, dataset_name=dataset.name)
            staging_prefix = _ingest_staging_prefix(prefix, ingest_request.ingest_request_id)
            object_key = f"{staging_prefix}/data.json"
            await lakefs_storage_service.save_json(
                repo,
                f"{dataset_branch}/{object_key}",
                sample_json,
                metadata=_sanitize_s3_metadata(
                    {
                        "db_name": dataset.db_name,
                        "dataset_id": dataset.dataset_id,
                        "dataset_name": dataset.name,
                        "source": "manual",
                        "ingest_request_id": ingest_request.ingest_request_id,
                    }
                ),
            )
            lakefs_commit_id = await lakefs_client.commit(
                repository=repo,
                branch=dataset_branch,
                message=f"Manual dataset version {dataset.db_name}/{dataset.name}",
                metadata={
                    "dataset_id": dataset.dataset_id,
                    "db_name": dataset.db_name,
                    "dataset_name": dataset.name,
                    "source_type": str(dataset.source_type or "manual"),
                    "ingest_request_id": ingest_request.ingest_request_id,
                    "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                },
            )
            artifact_key = build_s3_uri(repo, f"{lakefs_commit_id}/{object_key}")

        if not lakefs_commit_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="lakefs_commit_id is required")
        ingest_request = await dataset_registry.mark_ingest_committed(
            ingest_request_id=ingest_request.ingest_request_id,
            lakefs_commit_id=lakefs_commit_id,
            artifact_key=artifact_key,
        )
        if ingest_transaction:
            await dataset_registry.mark_ingest_transaction_committed(
                ingest_request_id=ingest_request.ingest_request_id,
                lakefs_commit_id=lakefs_commit_id,
                artifact_key=artifact_key,
            )
        version_event_id = ingest_request.ingest_request_id
        outbox_entries = [
            {
                "kind": "eventstore",
                "payload": build_dataset_event_payload(
                    event_id=version_event_id,
                    event_type="DATASET_VERSION_CREATED",
                    aggregate_type="Dataset",
                    aggregate_id=dataset_id,
                    command_type="INGEST_DATASET_SNAPSHOT",
                    actor=actor_user_id,
                    data={
                        "dataset_id": dataset_id,
                        "lakefs_commit_id": lakefs_commit_id,
                        "artifact_key": artifact_key,
                        "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                    },
                ),
            }
        ]
        try:
            version = await dataset_registry.publish_ingest_request(
                ingest_request_id=ingest_request.ingest_request_id,
                dataset_id=dataset_id,
                lakefs_commit_id=lakefs_commit_id,
                artifact_key=artifact_key,
                row_count=row_count,
                sample_json=sample_json,
                schema_json=schema_json,
                outbox_entries=outbox_entries,
            )
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

        objectify_job_id = await _maybe_enqueue_objectify_job(
            dataset=dataset,
            version=version,
            objectify_registry=objectify_registry,
            job_queue=objectify_job_queue,
            actor_user_id=actor_user_id,
        )

        await flush_dataset_ingest_outbox(
            dataset_registry=dataset_registry,
            lineage_store=None,
        )

        return ApiResponse.success(
            message="Dataset version created",
            data={"version": version.__dict__, "objectify_job_id": objectify_job_id},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        if ingest_request is not None:
            try:
                await dataset_registry.mark_ingest_failed(
                    ingest_request_id=ingest_request.ingest_request_id,
                    error=str(e),
                )
                await dataset_registry.mark_ingest_transaction_aborted(
                    ingest_request_id=ingest_request.ingest_request_id,
                    error=str(e),
                )
            except Exception as exc:
                logger.warning("Failed to mark manual ingest request failed: %s", exc)
        logger.error(f"Failed to create dataset version: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/datasets/excel-upload", response_model=ApiResponse)
@trace_endpoint("upload_excel_dataset")
async def upload_excel_dataset(
    db_name: str = Query(..., description="Database name"),
    branch: Optional[str] = Query(default=None),
    file: UploadFile = File(...),
    dataset_name: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    sheet_name: Optional[str] = Form(None),
    table_id: Optional[str] = Form(None),
    table_top: Optional[int] = Form(None),
    table_left: Optional[int] = Form(None),
    table_bottom: Optional[int] = Form(None),
    table_right: Optional[int] = Form(None),
    request: Request = None,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    objectify_job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    *,
    lineage_store: LineageStoreDep,
) -> ApiResponse:
    """
    Excel 업로드 → preview/스키마 추론 → dataset registry 저장 + artifact 저장

    Pipeline Builder 전용: raw 데이터를 canonical dataset으로 저장합니다.
    """
    ingest_request = None
    try:
        actor_user_id = (request.headers.get("X-User-ID") or "").strip() if request else ""
        actor_user_id = actor_user_id or None
        lakefs_storage_service = await pipeline_registry.get_lakefs_storage(user_id=actor_user_id)
        lakefs_client = await pipeline_registry.get_lakefs_client(user_id=actor_user_id)
        idempotency_key = _require_idempotency_key(request)

        db_name = validate_db_name(db_name)
        try:
            enforce_db_scope(request.headers, db_name=db_name)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
        filename = file.filename or "upload.xlsx"
        if not filename.lower().endswith((".xlsx", ".xlsm")):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only .xlsx/.xlsm files are supported",
            )

        sample_bytes = await asyncio.to_thread(file.file.read, 65536)
        if not sample_bytes:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty file")
        try:
            file.file.seek(0)
        except Exception:
            pass

        resolved_name = (dataset_name or "").strip() or _default_dataset_name(filename)
        if not resolved_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="dataset_name is required")

        bbox = _normalize_table_bbox(
            table_top=table_top,
            table_left=table_left,
            table_bottom=table_bottom,
            table_right=table_right,
        )

        from bff.services.funnel_client import FunnelClient

        async with FunnelClient() as funnel_client:
            result, content_hash = await funnel_client.excel_to_structure_preview_stream(
                fileobj=file.file,
                filename=filename,
                sheet_name=sheet_name,
                table_id=table_id,
                table_bbox=bbox,
                include_complex_types=True,
            )

        preview = result.get("preview") or {}
        columns = [str(col) for col in (preview.get("columns") or []) if str(col)]
        inferred_schema = preview.get("inferred_schema") or []
        schema_columns = _build_schema_columns(columns, inferred_schema)
        schema_json = {"columns": schema_columns}
        sample_rows = _rows_from_preview(columns, preview.get("sample_data") or [])
        total_rows = preview.get("total_rows")
        row_count = int(total_rows) if isinstance(total_rows, int) else None
        if row_count is None and sample_rows:
            row_count = len(sample_rows)

        dataset_branch = (branch or "main").strip() or "main"
        dataset_branch = safe_lakefs_ref(dataset_branch)
        dataset = await dataset_registry.get_dataset_by_name(
            db_name=db_name,
            name=resolved_name,
            branch=dataset_branch,
        )
        created_dataset = False
        if not dataset:
            dataset = await dataset_registry.create_dataset(
                db_name=db_name,
                name=resolved_name,
                description=description.strip() if description else None,
                source_type="excel_upload",
                source_ref=filename,
                schema_json=schema_json,
                branch=dataset_branch,
            )
            created_dataset = True
        request_fingerprint = _build_ingest_request_fingerprint(
            {
                "db_name": db_name,
                "branch": dataset_branch,
                "dataset_id": dataset.dataset_id,
                "dataset_name": resolved_name,
                "source_type": "excel_upload",
                "filename": filename,
                "sheet_name": sheet_name,
                "table_id": table_id,
                "table_bbox": bbox,
                "content_sha256": content_hash,
            }
        )
        ingest_request, _ = await dataset_registry.create_ingest_request(
            dataset_id=dataset.dataset_id,
            db_name=db_name,
            branch=dataset_branch,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            schema_json=schema_json,
            sample_json={"columns": _columns_from_schema(schema_columns), "rows": sample_rows},
            row_count=row_count,
            source_metadata=preview.get("source_metadata") if isinstance(preview, dict) else None,
        )
        ingest_transaction = await _ensure_ingest_transaction(
            dataset_registry,
            ingest_request_id=ingest_request.ingest_request_id,
        )
        if ingest_request.dataset_id != dataset.dataset_id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Idempotency key already used for a different dataset")
        if ingest_request.request_fingerprint and ingest_request.request_fingerprint != request_fingerprint:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Idempotency key reuse detected with different payload")
        if ingest_request.status == "FAILED":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=ingest_request.error or "Previous ingest failed")

        if ingest_request.status == "PUBLISHED":
            existing_version = await dataset_registry.get_version_by_ingest_request(
                ingest_request_id=ingest_request.ingest_request_id
            )
            if existing_version:
                await flush_dataset_ingest_outbox(
                    dataset_registry=dataset_registry,
                    lineage_store=lineage_store,
                )
                return ApiResponse.success(
                    message="Excel dataset created",
                    data={
                        "dataset": dataset.__dict__,
                        "version": existing_version.__dict__,
                        "preview": {"columns": _columns_from_schema(schema_columns), "rows": sample_rows},
                        "source": preview.get("source_metadata"),
                    },
                ).to_dict()

        commit_id = ingest_request.lakefs_commit_id
        artifact_key = ingest_request.artifact_key
        if not commit_id or not artifact_key:
            repo = _resolve_lakefs_raw_repository()
            await _ensure_lakefs_branch_exists(
                lakefs_client=lakefs_client,
                repository=repo,
                branch=dataset_branch,
                source_branch="main",
            )
            prefix = _dataset_artifact_prefix(db_name=db_name, dataset_id=dataset.dataset_id, dataset_name=dataset.name)
            staging_prefix = _ingest_staging_prefix(prefix, ingest_request.ingest_request_id)
            object_key = f"{staging_prefix}/source.xlsx"
            await lakefs_storage_service.save_fileobj(
                repo,
                f"{dataset_branch}/{object_key}",
                file.file,
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                metadata=_sanitize_s3_metadata(
                    {
                        "db_name": db_name,
                        "dataset_name": resolved_name,
                        "source": "excel_upload",
                        "ingest_request_id": ingest_request.ingest_request_id,
                    }
                ),
                checksum=content_hash,
            )
            commit_id = await lakefs_client.commit(
                repository=repo,
                branch=dataset_branch,
                message=f"Excel dataset upload {db_name}/{resolved_name}",
                metadata={
                    "dataset_id": dataset.dataset_id,
                    "db_name": db_name,
                    "dataset_name": resolved_name,
                    "source_type": "excel_upload",
                    "filename": filename,
                    "ingest_request_id": ingest_request.ingest_request_id,
                    "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                    "content_sha256": content_hash,
                },
            )
            artifact_key = build_s3_uri(repo, f"{commit_id}/{object_key}")
            ingest_request = await dataset_registry.mark_ingest_committed(
                ingest_request_id=ingest_request.ingest_request_id,
                lakefs_commit_id=commit_id,
                artifact_key=artifact_key,
            )
            if ingest_transaction:
                await dataset_registry.mark_ingest_transaction_committed(
                    ingest_request_id=ingest_request.ingest_request_id,
                    lakefs_commit_id=commit_id,
                    artifact_key=artifact_key,
                )

        outbox_entries: list[dict[str, Any]] = []
        if created_dataset:
            outbox_entries.append(
                {
                    "kind": "eventstore",
                    "payload": build_dataset_event_payload(
                        event_id=dataset.dataset_id,
                        event_type="DATASET_CREATED",
                        aggregate_type="Dataset",
                        aggregate_id=dataset.dataset_id,
                        command_type="CREATE_DATASET",
                        actor=actor_user_id,
                        data={
                            "dataset_id": dataset.dataset_id,
                            "db_name": db_name,
                            "name": resolved_name,
                            "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                        },
                    ),
                }
            )
        version_event_id = ingest_request.ingest_request_id
        outbox_entries.append(
            {
                "kind": "eventstore",
                "payload": build_dataset_event_payload(
                    event_id=version_event_id,
                    event_type="DATASET_VERSION_CREATED",
                    aggregate_type="Dataset",
                    aggregate_id=dataset.dataset_id,
                    command_type="INGEST_DATASET_SNAPSHOT",
                    actor=actor_user_id,
                    data={
                        "dataset_id": dataset.dataset_id,
                        "db_name": db_name,
                        "name": resolved_name,
                        "lakefs_commit_id": commit_id,
                        "artifact_key": artifact_key,
                        "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                    },
                ),
            }
        )
        if lineage_store and artifact_key:
            parsed = parse_s3_uri(artifact_key)
            if parsed:
                bucket, key = parsed
                outbox_entries.append(
                    {
                        "kind": "lineage",
                        "payload": {
                            "from_node_id": lineage_store.node_event(str(version_event_id)),
                            "to_node_id": lineage_store.node_artifact("s3", bucket, key),
                            "edge_type": "dataset_artifact_stored",
                            "occurred_at": utcnow(),
                            "from_label": "excel_upload",
                            "to_label": artifact_key,
                            "db_name": db_name,
                            "edge_metadata": {
                                "db_name": db_name,
                                "dataset_id": dataset.dataset_id,
                                "dataset_name": resolved_name,
                                "bucket": bucket,
                                "key": key,
                                "source": "excel_upload",
                                "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                            },
                        },
                    }
                )

        try:
            version = await dataset_registry.publish_ingest_request(
                ingest_request_id=ingest_request.ingest_request_id,
                dataset_id=dataset.dataset_id,
                lakefs_commit_id=commit_id or "",
                artifact_key=artifact_key,
                row_count=row_count,
                sample_json={"columns": _columns_from_schema(schema_columns), "rows": sample_rows},
                schema_json=schema_json,
                outbox_entries=outbox_entries,
            )
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

        objectify_job_id = await _maybe_enqueue_objectify_job(
            dataset=dataset,
            version=version,
            objectify_registry=objectify_registry,
            job_queue=objectify_job_queue,
            actor_user_id=actor_user_id,
        )

        await flush_dataset_ingest_outbox(
            dataset_registry=dataset_registry,
            lineage_store=lineage_store,
        )

        return ApiResponse.success(
            message="Excel dataset created",
            data={
                "dataset": dataset.__dict__,
                "version": version.__dict__,
                "objectify_job_id": objectify_job_id,
                "preview": {"columns": _columns_from_schema(schema_columns), "rows": sample_rows},
                "source": preview.get("source_metadata"),
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        if ingest_request is not None:
            try:
                await dataset_registry.mark_ingest_failed(
                    ingest_request_id=ingest_request.ingest_request_id,
                    error=str(e),
                )
                await dataset_registry.mark_ingest_transaction_aborted(
                    ingest_request_id=ingest_request.ingest_request_id,
                    error=str(e),
                )
            except Exception as exc:
                logger.warning("Failed to mark excel ingest request failed: %s", exc)
        logger.error(f"Failed to upload excel dataset: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/datasets/csv-upload", response_model=ApiResponse)
@trace_endpoint("upload_csv_dataset")
async def upload_csv_dataset(
    db_name: str = Query(..., description="Database name"),
    branch: Optional[str] = Query(default=None),
    file: UploadFile = File(...),
    dataset_name: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    delimiter: Optional[str] = Form(None),
    has_header: bool = Form(True),
    request: Request = None,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    objectify_job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    *,
    lineage_store: LineageStoreDep,
) -> ApiResponse:
    """
    CSV 업로드 → preview/타입 추론 → dataset registry 저장 + artifact 저장

    Pipeline Builder 전용: raw 데이터를 canonical dataset으로 저장합니다.
    """
    ingest_request = None
    try:
        actor_user_id = (request.headers.get("X-User-ID") or "").strip() if request else ""
        actor_user_id = actor_user_id or None
        lakefs_storage_service = await pipeline_registry.get_lakefs_storage(user_id=actor_user_id)
        lakefs_client = await pipeline_registry.get_lakefs_client(user_id=actor_user_id)
        idempotency_key = _require_idempotency_key(request)

        db_name = validate_db_name(db_name)
        try:
            enforce_db_scope(request.headers, db_name=db_name)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
        filename = file.filename or "upload.csv"
        if not filename.lower().endswith(".csv"):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only .csv files are supported")

        sample_bytes = await asyncio.to_thread(file.file.read, 65536)
        if not sample_bytes:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty file")

        resolved_name = (dataset_name or "").strip() or _default_dataset_name(filename)
        if not resolved_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="dataset_name is required")

        sample_text = sample_bytes.decode("utf-8", errors="replace")
        resolved_delimiter = delimiter or _detect_csv_delimiter(sample_text)
        columns, preview_rows, total_rows, content_hash = await asyncio.to_thread(
            _parse_csv_file,
            file.file,
            delimiter=resolved_delimiter,
            has_header=has_header,
        )
        if not columns:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No columns detected")

        inferred_schema: list[Dict[str, Any]] = []
        try:
            from bff.services.funnel_client import FunnelClient

            async with FunnelClient() as funnel_client:
                analysis = await funnel_client.analyze_dataset(
                    {
                        "data": preview_rows,
                        "columns": columns,
                        "sample_size": min(len(preview_rows), 500),
                        "include_complex_types": True,
                    }
                )
            inferred_schema = analysis.get("columns") or []
        except Exception as exc:
            logger.warning(f"CSV type inference failed: {exc}")

        schema_columns = _build_schema_columns(columns, inferred_schema)
        schema_json = {"columns": schema_columns}
        sample_rows = _rows_from_preview(columns, preview_rows)
        row_count = total_rows if total_rows > 0 else len(sample_rows)

        dataset_branch = (branch or "main").strip() or "main"
        dataset_branch = safe_lakefs_ref(dataset_branch)
        dataset = await dataset_registry.get_dataset_by_name(
            db_name=db_name,
            name=resolved_name,
            branch=dataset_branch,
        )
        created_dataset = False
        if not dataset:
            dataset = await dataset_registry.create_dataset(
                db_name=db_name,
                name=resolved_name,
                description=description.strip() if description else None,
                source_type="csv_upload",
                source_ref=filename,
                schema_json=schema_json,
                branch=dataset_branch,
            )
            created_dataset = True
        request_fingerprint = _build_ingest_request_fingerprint(
            {
                "db_name": db_name,
                "branch": dataset_branch,
                "dataset_id": dataset.dataset_id,
                "dataset_name": resolved_name,
                "source_type": "csv_upload",
                "filename": filename,
                "delimiter": resolved_delimiter,
                "has_header": has_header,
                "content_sha256": content_hash,
            }
        )
        ingest_request, _ = await dataset_registry.create_ingest_request(
            dataset_id=dataset.dataset_id,
            db_name=db_name,
            branch=dataset_branch,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            schema_json=schema_json,
            sample_json={"columns": _columns_from_schema(schema_columns), "rows": sample_rows},
            row_count=row_count,
            source_metadata={
                "type": "csv",
                "filename": filename,
                "delimiter": resolved_delimiter,
                "has_header": has_header,
            },
        )
        ingest_transaction = await _ensure_ingest_transaction(
            dataset_registry,
            ingest_request_id=ingest_request.ingest_request_id,
        )
        if ingest_request.dataset_id != dataset.dataset_id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Idempotency key already used for a different dataset")
        if ingest_request.request_fingerprint and ingest_request.request_fingerprint != request_fingerprint:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Idempotency key reuse detected with different payload")
        if ingest_request.status == "FAILED":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=ingest_request.error or "Previous ingest failed")

        if ingest_request.status == "PUBLISHED":
            existing_version = await dataset_registry.get_version_by_ingest_request(
                ingest_request_id=ingest_request.ingest_request_id
            )
            if existing_version:
                await flush_dataset_ingest_outbox(
                    dataset_registry=dataset_registry,
                    lineage_store=lineage_store,
                )
                return ApiResponse.success(
                    message="CSV dataset created",
                    data={
                        "dataset": dataset.__dict__,
                        "version": existing_version.__dict__,
                        "preview": {"columns": _columns_from_schema(schema_columns), "rows": sample_rows},
                        "source": {
                            "type": "csv",
                            "filename": filename,
                            "delimiter": resolved_delimiter,
                            "has_header": has_header,
                        },
                    },
                ).to_dict()

        commit_id = ingest_request.lakefs_commit_id
        artifact_key = ingest_request.artifact_key
        if not commit_id or not artifact_key:
            repo = _resolve_lakefs_raw_repository()
            await _ensure_lakefs_branch_exists(
                lakefs_client=lakefs_client,
                repository=repo,
                branch=dataset_branch,
                source_branch="main",
            )
            prefix = _dataset_artifact_prefix(db_name=db_name, dataset_id=dataset.dataset_id, dataset_name=dataset.name)
            staging_prefix = _ingest_staging_prefix(prefix, ingest_request.ingest_request_id)
            object_key = f"{staging_prefix}/source.csv"
            file.file.seek(0)
            metadata_payload = _sanitize_s3_metadata(
                {
                    "db_name": db_name,
                    "dataset_name": resolved_name,
                    "source": "csv_upload",
                    "ingest_request_id": ingest_request.ingest_request_id,
                }
            )
            save_fileobj = getattr(lakefs_storage_service, "save_fileobj", None)
            if callable(save_fileobj):
                await save_fileobj(
                    repo,
                    f"{dataset_branch}/{object_key}",
                    file.file,
                    content_type="text/csv",
                    metadata=metadata_payload,
                    checksum=content_hash,
                )
            else:
                save_bytes = getattr(lakefs_storage_service, "save_bytes", None)
                if not callable(save_bytes):
                    raise RuntimeError("lakeFS storage service missing save_fileobj/save_bytes")
                content = await asyncio.to_thread(file.file.read)
                await save_bytes(
                    repo,
                    f"{dataset_branch}/{object_key}",
                    content,
                    content_type="text/csv",
                    metadata=metadata_payload,
                )
            commit_id = await lakefs_client.commit(
                repository=repo,
                branch=dataset_branch,
                message=f"CSV dataset upload {db_name}/{resolved_name}",
                metadata={
                    "dataset_id": dataset.dataset_id,
                    "db_name": db_name,
                    "dataset_name": resolved_name,
                    "source_type": "csv_upload",
                    "filename": filename,
                    "delimiter": resolved_delimiter,
                    "has_header": has_header,
                    "ingest_request_id": ingest_request.ingest_request_id,
                    "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                    "content_sha256": content_hash,
                },
            )
            artifact_key = build_s3_uri(repo, f"{commit_id}/{object_key}")
            ingest_request = await dataset_registry.mark_ingest_committed(
                ingest_request_id=ingest_request.ingest_request_id,
                lakefs_commit_id=commit_id,
                artifact_key=artifact_key,
            )
            if ingest_transaction:
                await dataset_registry.mark_ingest_transaction_committed(
                    ingest_request_id=ingest_request.ingest_request_id,
                    lakefs_commit_id=commit_id,
                    artifact_key=artifact_key,
                )

        outbox_entries: list[dict[str, Any]] = []
        if created_dataset:
            outbox_entries.append(
                {
                    "kind": "eventstore",
                    "payload": build_dataset_event_payload(
                        event_id=dataset.dataset_id,
                        event_type="DATASET_CREATED",
                        aggregate_type="Dataset",
                        aggregate_id=dataset.dataset_id,
                        command_type="CREATE_DATASET",
                        actor=actor_user_id,
                        data={
                            "dataset_id": dataset.dataset_id,
                            "db_name": db_name,
                            "name": resolved_name,
                            "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                        },
                    ),
                }
            )
        version_event_id = ingest_request.ingest_request_id
        outbox_entries.append(
            {
                "kind": "eventstore",
                "payload": build_dataset_event_payload(
                    event_id=version_event_id,
                    event_type="DATASET_VERSION_CREATED",
                    aggregate_type="Dataset",
                    aggregate_id=dataset.dataset_id,
                    command_type="INGEST_DATASET_SNAPSHOT",
                    actor=actor_user_id,
                    data={
                        "dataset_id": dataset.dataset_id,
                        "db_name": db_name,
                        "name": resolved_name,
                        "lakefs_commit_id": commit_id,
                        "artifact_key": artifact_key,
                        "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                    },
                ),
            }
        )
        if lineage_store and artifact_key:
            parsed = parse_s3_uri(artifact_key)
            if parsed:
                bucket, key = parsed
                outbox_entries.append(
                    {
                        "kind": "lineage",
                        "payload": {
                            "from_node_id": lineage_store.node_event(str(version_event_id)),
                            "to_node_id": lineage_store.node_artifact("s3", bucket, key),
                            "edge_type": "dataset_artifact_stored",
                            "occurred_at": utcnow(),
                            "from_label": "csv_upload",
                            "to_label": artifact_key,
                            "db_name": db_name,
                            "edge_metadata": {
                                "db_name": db_name,
                                "dataset_id": dataset.dataset_id,
                                "dataset_name": resolved_name,
                                "bucket": bucket,
                                "key": key,
                                "source": "csv_upload",
                                "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                            },
                        },
                    }
                )

        try:
            version = await dataset_registry.publish_ingest_request(
                ingest_request_id=ingest_request.ingest_request_id,
                dataset_id=dataset.dataset_id,
                lakefs_commit_id=commit_id or "",
                artifact_key=artifact_key,
                row_count=row_count,
                sample_json={"columns": _columns_from_schema(schema_columns), "rows": sample_rows},
                schema_json=schema_json,
                outbox_entries=outbox_entries,
            )
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

        objectify_job_id = await _maybe_enqueue_objectify_job(
            dataset=dataset,
            version=version,
            objectify_registry=objectify_registry,
            job_queue=objectify_job_queue,
            actor_user_id=actor_user_id,
        )

        await flush_dataset_ingest_outbox(
            dataset_registry=dataset_registry,
            lineage_store=lineage_store,
        )

        return ApiResponse.success(
            message="CSV dataset created",
            data={
                "dataset": dataset.__dict__,
                "version": version.__dict__,
                "objectify_job_id": objectify_job_id,
                "preview": {"columns": _columns_from_schema(schema_columns), "rows": sample_rows},
                "source": {
                    "type": "csv",
                    "filename": filename,
                    "delimiter": resolved_delimiter,
                    "has_header": has_header,
                },
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        if ingest_request is not None:
            try:
                await dataset_registry.mark_ingest_failed(
                    ingest_request_id=ingest_request.ingest_request_id,
                    error=str(e),
                )
                await dataset_registry.mark_ingest_transaction_aborted(
                    ingest_request_id=ingest_request.ingest_request_id,
                    error=str(e),
                )
            except Exception as exc:
                logger.warning("Failed to mark csv ingest request failed: %s", exc)
        logger.exception("Failed to upload csv dataset")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/datasets/media-upload", response_model=ApiResponse)
@trace_endpoint("upload_media_dataset")
async def upload_media_dataset(
    db_name: str = Query(..., description="Database name"),
    branch: Optional[str] = Query(default=None),
    files: list[UploadFile] = File(...),
    dataset_name: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    request: Request = None,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    objectify_job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    *,
    lineage_store: LineageStoreDep,
) -> ApiResponse:
    """
    Media upload → store raw files to S3/MinIO → register a "media" dataset version.

    The dataset version artifact_key points to a prefix that contains the uploaded blobs.
    The pipeline runtime can treat this as an unstructured/media input (without parsing into a table).
    """
    ingest_request = None
    ingest_transaction = None
    try:
        actor_user_id = (request.headers.get("X-User-ID") or "").strip() if request else ""
        actor_user_id = actor_user_id or None
        lakefs_storage_service = await pipeline_registry.get_lakefs_storage(user_id=actor_user_id)
        lakefs_client = await pipeline_registry.get_lakefs_client(user_id=actor_user_id)
        idempotency_key = _require_idempotency_key(request)

        db_name = validate_db_name(db_name)
        dataset_branch = (branch or "main").strip() or "main"
        dataset_branch = safe_lakefs_ref(dataset_branch)

        resolved_name = (dataset_name or "").strip()
        if not resolved_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="dataset_name is required")

        if not files:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="files are required")

        uploaded: list[dict[str, Any]] = []
        for file in files:
            if not file:
                continue
            filename = (file.filename or "").strip()
            if not filename:
                filename = f"upload-{uuid4().hex}"
            content = await file.read()
            if not content:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Empty file: {filename}")
            content_hash = hashlib.sha256(content).hexdigest()
            uploaded.append(
                {
                    "filename": filename,
                    "content_type": (file.content_type or "application/octet-stream").strip()
                    or "application/octet-stream",
                    "size_bytes": len(content),
                    "content": content,
                    "content_sha256": content_hash,
                }
            )

        if not uploaded:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No valid files uploaded")

        schema_columns = [
            {"name": "s3_uri", "type": "xsd:string"},
            {"name": "filename", "type": "xsd:string"},
            {"name": "content_type", "type": "xsd:string"},
            {"name": "size_bytes", "type": "xsd:integer"},
        ]
        row_count = len(uploaded)
        ingest_sample_json = {"columns": schema_columns, "rows": []}

        dataset = await dataset_registry.get_dataset_by_name(
            db_name=db_name,
            name=resolved_name,
            branch=dataset_branch,
        )
        created_dataset = False
        if not dataset:
            dataset = await dataset_registry.create_dataset(
                db_name=db_name,
                name=resolved_name,
                description=(description or "").strip() or None,
                source_type="media",
                source_ref=None,
                schema_json={"columns": schema_columns},
                branch=dataset_branch,
            )
            created_dataset = True

        safe_name = (dataset.name or "media").replace(" ", "_")
        prefix = f"datasets-media/{db_name}/{dataset.dataset_id}/{safe_path_segment(safe_name)}"
        request_fingerprint = _build_ingest_request_fingerprint(
            {
                "db_name": db_name,
                "branch": dataset_branch,
                "dataset_id": dataset.dataset_id,
                "dataset_name": resolved_name,
                "source_type": "media_upload",
                "files": [
                    {
                        "filename": item["filename"],
                        "size_bytes": item["size_bytes"],
                        "content_sha256": item["content_sha256"],
                    }
                    for item in uploaded
                ],
            }
        )
        ingest_request, _ = await dataset_registry.create_ingest_request(
            dataset_id=dataset.dataset_id,
            db_name=db_name,
            branch=dataset_branch,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            schema_json={"columns": schema_columns},
            sample_json=ingest_sample_json,
            row_count=row_count,
            source_metadata={"source_type": "media_upload", "file_count": row_count},
        )
        ingest_transaction = await _ensure_ingest_transaction(
            dataset_registry,
            ingest_request_id=ingest_request.ingest_request_id,
        )
        if ingest_request.dataset_id != dataset.dataset_id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Idempotency key already used for a different dataset")
        if ingest_request.request_fingerprint and ingest_request.request_fingerprint != request_fingerprint:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Idempotency key reuse detected with different payload")
        if ingest_request.status == "FAILED":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=ingest_request.error or "Previous ingest failed")

        if ingest_request.status == "PUBLISHED":
            existing_version = await dataset_registry.get_version_by_ingest_request(
                ingest_request_id=ingest_request.ingest_request_id
            )
            if existing_version:
                await flush_dataset_ingest_outbox(
                    dataset_registry=dataset_registry,
                    lineage_store=lineage_store,
                )
                return ApiResponse.success(
                    message="Media dataset created",
                    data={
                        "dataset": dataset.__dict__,
                        "version": existing_version.__dict__,
                        "preview": {"columns": schema_columns, "rows": []},
                        "artifact_key": existing_version.artifact_key,
                    },
                ).to_dict()

        repo = _resolve_lakefs_raw_repository()
        await _ensure_lakefs_branch_exists(
            lakefs_client=lakefs_client,
            repository=repo,
            branch=dataset_branch,
            source_branch="main",
        )
        commit_id = ingest_request.lakefs_commit_id
        artifact_key = ingest_request.artifact_key
        staging_prefix = _ingest_staging_prefix(prefix, ingest_request.ingest_request_id)
        if not commit_id or not artifact_key:
            for index, item in enumerate(uploaded):
                filename = str(item.get("filename") or f"upload-{index}")
                content_type = str(item.get("content_type") or "application/octet-stream")
                blob = item.get("content") or b""
                object_key = f"{staging_prefix}/{index:04d}_{filename}"
                await lakefs_storage_service.save_bytes(
                    repo,
                    f"{dataset_branch}/{object_key}",
                    blob,
                    content_type=content_type,
                    metadata=_sanitize_s3_metadata(
                        {
                            "db_name": db_name,
                            "dataset_id": dataset.dataset_id,
                            "dataset_name": dataset.name,
                            "source": "media_upload",
                            "content_type": content_type,
                            "ingest_request_id": ingest_request.ingest_request_id,
                        }
                    ),
                )

            commit_id = await lakefs_client.commit(
                repository=repo,
                branch=dataset_branch,
                message=f"Media dataset upload {db_name}/{resolved_name}",
                metadata={
                    "dataset_id": dataset.dataset_id,
                    "db_name": db_name,
                    "dataset_name": resolved_name,
                    "source_type": "media_upload",
                    "file_count": len(uploaded),
                    "ingest_request_id": ingest_request.ingest_request_id,
                    "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                },
            )

            artifact_key = build_s3_uri(repo, f"{commit_id}/{staging_prefix}")
            ingest_request = await dataset_registry.mark_ingest_committed(
                ingest_request_id=ingest_request.ingest_request_id,
                lakefs_commit_id=commit_id,
                artifact_key=artifact_key,
            )
            if ingest_transaction:
                await dataset_registry.mark_ingest_transaction_committed(
                    ingest_request_id=ingest_request.ingest_request_id,
                    lakefs_commit_id=commit_id,
                    artifact_key=artifact_key,
                )
        sample_rows: list[dict[str, Any]] = []
        for index, item in enumerate(uploaded):
            filename = str(item.get("filename") or f"upload-{index}")
            content_type = str(item.get("content_type") or "application/octet-stream")
            size_bytes = int(item.get("size_bytes") or 0)
            object_key = f"{staging_prefix}/{index:04d}_{filename}"
            sample_rows.append(
                {
                    "s3_uri": build_s3_uri(repo, f"{commit_id}/{object_key}"),
                    "filename": filename,
                    "content_type": content_type,
                    "size_bytes": size_bytes,
                }
            )

        await dataset_registry.update_ingest_request_payload(
            ingest_request_id=ingest_request.ingest_request_id,
            sample_json={"columns": schema_columns, "rows": sample_rows},
            row_count=row_count,
        )

        outbox_entries: list[dict[str, Any]] = []
        if created_dataset:
            outbox_entries.append(
                {
                    "kind": "eventstore",
                    "payload": build_dataset_event_payload(
                        event_id=dataset.dataset_id,
                        event_type="DATASET_CREATED",
                        aggregate_type="Dataset",
                        aggregate_id=dataset.dataset_id,
                        command_type="CREATE_DATASET",
                        actor=actor_user_id,
                        data={
                            "dataset_id": dataset.dataset_id,
                            "db_name": db_name,
                            "name": resolved_name,
                            "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                        },
                    ),
                }
            )
        version_event_id = ingest_request.ingest_request_id
        outbox_entries.append(
            {
                "kind": "eventstore",
                "payload": build_dataset_event_payload(
                    event_id=version_event_id,
                    event_type="DATASET_VERSION_CREATED",
                    aggregate_type="Dataset",
                    aggregate_id=dataset.dataset_id,
                    command_type="INGEST_MEDIA_SET",
                    actor=actor_user_id,
                    data={
                        "dataset_id": dataset.dataset_id,
                        "db_name": db_name,
                        "name": resolved_name,
                        "lakefs_commit_id": commit_id,
                        "artifact_key": artifact_key,
                        "source_type": "media",
                        "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                    },
                ),
            }
        )
        if lineage_store and artifact_key:
            parsed = parse_s3_uri(artifact_key)
            if parsed:
                bucket, key = parsed
                outbox_entries.append(
                    {
                        "kind": "lineage",
                        "payload": {
                            "from_node_id": lineage_store.node_event(str(version_event_id)),
                            "to_node_id": lineage_store.node_artifact("s3", bucket, key),
                            "edge_type": "dataset_artifact_stored",
                            "occurred_at": utcnow(),
                            "from_label": "media_upload",
                            "to_label": artifact_key,
                            "db_name": db_name,
                            "edge_metadata": {
                                "db_name": db_name,
                                "dataset_id": dataset.dataset_id,
                                "dataset_name": resolved_name,
                                "bucket": bucket,
                                "key": key,
                                "source": "media_upload",
                                "file_count": row_count,
                                "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                            },
                        },
                    }
                )

        try:
            version = await dataset_registry.publish_ingest_request(
                ingest_request_id=ingest_request.ingest_request_id,
                dataset_id=dataset.dataset_id,
                lakefs_commit_id=commit_id or "",
                artifact_key=artifact_key,
                row_count=row_count,
                sample_json={
                    "columns": schema_columns,
                    "rows": sample_rows[: min(50, len(sample_rows))],
                    "row_count": row_count,
                    "sample_row_count": min(50, len(sample_rows)),
                },
                schema_json={"columns": schema_columns},
                outbox_entries=outbox_entries,
            )
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

        objectify_job_id = await _maybe_enqueue_objectify_job(
            dataset=dataset,
            version=version,
            objectify_registry=objectify_registry,
            job_queue=objectify_job_queue,
            actor_user_id=actor_user_id,
        )

        await flush_dataset_ingest_outbox(
            dataset_registry=dataset_registry,
            lineage_store=lineage_store,
        )

        return ApiResponse.success(
            message="Media dataset created",
            data={
                "dataset": dataset.__dict__,
                "version": version.__dict__,
                "objectify_job_id": objectify_job_id,
                "preview": {"columns": _columns_from_schema(schema_columns), "rows": sample_rows[: min(6, len(sample_rows))]},
                "artifact_key": artifact_key,
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        if ingest_request is not None:
            try:
                await dataset_registry.mark_ingest_failed(
                    ingest_request_id=ingest_request.ingest_request_id,
                    error=str(e),
                )
                await dataset_registry.mark_ingest_transaction_aborted(
                    ingest_request_id=ingest_request.ingest_request_id,
                    error=str(e),
                )
            except Exception as exc:
                logger.warning("Failed to mark media ingest request failed: %s", exc)
        logger.error(f"Failed to upload media dataset: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
