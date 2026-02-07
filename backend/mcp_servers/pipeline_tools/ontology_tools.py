from __future__ import annotations

import logging
from typing import Any, Awaitable, Dict, List, Callable, Optional

from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.utils.llm_safety import mask_pii

from mcp_servers.pipeline_mcp_errors import missing_required_params, tool_error
from mcp_servers.pipeline_mcp_http import bff_json, oms_json

logger = logging.getLogger(__name__)

ToolHandler = Callable[[Any, Dict[str, Any]], Awaitable[Any]]


async def _ontology_register_object_type(_server: Any, arguments: Dict[str, Any]) -> Any:
    db_name = str(arguments.get("db_name") or "").strip()
    class_id = str(arguments.get("class_id") or "").strip()
    dataset_id = str(arguments.get("dataset_id") or "").strip()
    primary_key = arguments.get("primary_key") or []
    title_key = arguments.get("title_key") or []
    branch = str(arguments.get("branch") or "main").strip()

    if not db_name or not class_id or not dataset_id:
        return missing_required_params(
            "ontology_register_object_type",
            ["db_name", "class_id", "dataset_id", "primary_key", "title_key"],
            arguments,
        )

    pk_list = (
        [str(k).strip() for k in primary_key if str(k).strip()]
        if isinstance(primary_key, list)
        else [str(primary_key).strip()]
    )
    title_list = (
        [str(k).strip() for k in title_key if str(k).strip()]
        if isinstance(title_key, list)
        else [str(title_key).strip()]
    )

    if not pk_list:
        return tool_error("primary_key must be a non-empty list of field names")
    if not title_list:
        return tool_error("title_key must be a non-empty list of field names")

    try:
        head_resp = await oms_json(
            "GET",
            f"/api/v1/version/{db_name}/head",
            params={"branch": branch},
        )
        head_data = head_resp.get("data") if isinstance(head_resp.get("data"), dict) else {}
        head_commit = (
            head_data.get("head_commit_id")
            or head_data.get("commit")
            or head_data.get("head_commit")
            or ""
        )
        if not head_commit:
            return tool_error(
                f"No head commit found for branch '{branch}'. Please create an ontology class first.",
                status_code=404,
                code=ErrorCode.RESOURCE_NOT_FOUND,
                category=ErrorCategory.RESOURCE,
            )

        object_type_payload: Dict[str, Any] = {
            "id": class_id,
            "label": class_id,
            "description": f"Object type contract for {class_id}",
            "spec": {
                "status": "ACTIVE",
                "pk_spec": {"primary_key": pk_list, "title_key": title_list},
                "backing_source": {"dataset_id": dataset_id},
            },
        }

        resp = await oms_json(
            "POST",
            f"/api/v1/database/{db_name}/ontology/resources/object_type",
            params={"branch": branch, "expected_head_commit": head_commit},
            json_body=object_type_payload,
            timeout_seconds=30.0,
        )

        if resp.get("error") and "409" in str(resp.get("error")):
            logger.info("object_type exists, updating with PUT db=%s class=%s", db_name, class_id)
            resp = await oms_json(
                "PUT",
                f"/api/v1/database/{db_name}/ontology/resources/object_type/{class_id}",
                params={"branch": branch, "expected_head_commit": head_commit},
                json_body=object_type_payload,
                timeout_seconds=30.0,
            )

        if resp.get("error"):
            payload = tool_error(
                str(resp.get("error") or "OMS request failed"),
                status_code=502,
                code=ErrorCode.UPSTREAM_ERROR,
                category=ErrorCategory.UPSTREAM,
                context={"db_name": db_name, "class_id": class_id},
            )
            payload["db_name"] = db_name
            payload["class_id"] = class_id
            return payload

        return {
            "status": "success",
            "message": f"Object type '{class_id}' registered successfully",
            "db_name": db_name,
            "class_id": class_id,
            "dataset_id": dataset_id,
            "primary_key": pk_list,
            "title_key": title_list,
            "hint": "You can now run objectify_run to create instances",
        }

    except Exception as exc:
        logger.error("ontology_register_object_type failed: %s", exc)
        payload = tool_error(
            str(exc),
            status_code=500,
            code=ErrorCode.INTERNAL_ERROR,
            category=ErrorCategory.INTERNAL,
            context={"db_name": db_name, "class_id": class_id},
        )
        payload["db_name"] = db_name
        payload["class_id"] = class_id
        return payload


async def _ontology_query_instances(_server: Any, arguments: Dict[str, Any]) -> Any:
    db_name = str(arguments.get("db_name") or "").strip()
    class_id = str(arguments.get("class_id") or "").strip()
    limit = int(arguments.get("limit") or 10)
    limit = max(1, min(limit, 100))
    branch = str(arguments.get("branch") or "main").strip()
    filters = arguments.get("filters") or {}

    if not db_name or not class_id:
        return missing_required_params("ontology_query_instances", ["db_name", "class_id"], arguments)

    query_body: Dict[str, Any] = {"class_type": class_id, "limit": limit}
    if filters and isinstance(filters, dict):
        query_body["filters"] = filters

    try:
        resp = await bff_json(
            "POST",
            f"/graph-query/{db_name}/simple",
            db_name=db_name,
            principal_id=None,
            principal_type=None,
            json_body=query_body,
            params={"base_branch": branch},
            timeout_seconds=30.0,
        )

        if resp.get("error"):
            payload = tool_error(
                str(resp.get("error") or "Query failed"),
                status_code=502,
                code=ErrorCode.UPSTREAM_ERROR,
                category=ErrorCategory.UPSTREAM,
                context={"db_name": db_name, "class_id": class_id},
            )
            payload["db_name"] = db_name
            payload["class_id"] = class_id
            return payload

        data = resp.get("data") if isinstance(resp.get("data"), dict) else resp
        instances = data.get("instances") or data.get("results") or data.get("rows") or []
        total_count = data.get("total_count") or data.get("count") or len(instances)

        masked_instances = mask_pii(instances[:limit]) if instances else []

        return {
            "status": "success",
            "db_name": db_name,
            "class_id": class_id,
            "branch": branch,
            "total_count": total_count,
            "returned_count": len(masked_instances),
            "instances": masked_instances,
        }
    except Exception as exc:
        logger.warning("ontology_query_instances failed: %s", exc)
        payload = tool_error(
            f"Query failed: {str(exc)[:200]}",
            status_code=500,
            code=ErrorCode.INTERNAL_ERROR,
            category=ErrorCategory.INTERNAL,
            context={"db_name": db_name, "class_id": class_id},
        )
        payload["db_name"] = db_name
        payload["class_id"] = class_id
        return payload


async def _detect_foreign_keys(server: Any, arguments: Dict[str, Any]) -> Any:
    db_name = str(arguments.get("db_name") or "").strip()
    dataset_id = str(arguments.get("dataset_id") or "").strip()
    confidence_threshold = float(arguments.get("confidence_threshold") or 0.6)
    branch = str(arguments.get("branch") or "main").strip()

    if not db_name or not dataset_id:
        return missing_required_params("detect_foreign_keys", ["db_name", "dataset_id"], arguments)

    try:
        from shared.services.pipeline.fk_pattern_detector import (
            ForeignKeyPatternDetector,
            FKDetectionConfig,
            TargetCandidate,
        )

        dataset_registry, _ = await server._ensure_registries()
        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)

        if not dataset:
            return tool_error(
                f"Dataset not found: {dataset_id}",
                status_code=404,
                code=ErrorCode.RESOURCE_NOT_FOUND,
                category=ErrorCategory.RESOURCE,
            )

        schema = dataset.schema_json or {}
        columns = schema.get("columns") or schema.get("fields") or []

        datasets = await dataset_registry.list_datasets(db_name=db_name, branch=branch)
        target_candidates = []
        for ds in datasets:
            # list_datasets returns List[Dict], access via dict keys
            ds_id = ds.get("dataset_id") or ds.get("id") or "" if isinstance(ds, dict) else getattr(ds, "dataset_id", "")
            if str(ds_id) == dataset_id:
                continue
            ds_schema = (ds.get("schema_json") if isinstance(ds, dict) else getattr(ds, "schema_json", None)) or {}
            if isinstance(ds_schema, str):
                import json as _json
                try:
                    ds_schema = _json.loads(ds_schema)
                except Exception:
                    ds_schema = {}
            ds_columns = ds_schema.get("columns") or ds_schema.get("fields") or []
            pk_cols = [
                c.get("name")
                for c in ds_columns
                if isinstance(c, dict) and str(c.get("name", "")).lower() in ("id", "pk")
            ]
            if not pk_cols and ds_columns:
                first = ds_columns[0]
                pk_cols = [first.get("name", "id")] if isinstance(first, dict) else ["id"]
            ds_name = (ds.get("name") if isinstance(ds, dict) else getattr(ds, "name", None)) or str(ds_id)
            target_candidates.append(
                TargetCandidate(
                    candidate_type="dataset",
                    candidate_id=str(ds_id),
                    candidate_name=ds_name,
                    pk_columns=pk_cols,
                )
            )

        config = FKDetectionConfig(min_confidence=confidence_threshold)
        detector = ForeignKeyPatternDetector(config)
        patterns = detector.detect_patterns(
            source_dataset_id=dataset_id,
            source_schema=columns,
            target_candidates=target_candidates,
        )

        return {
            "status": "success",
            "dataset_id": dataset_id,
            "db_name": db_name,
            "patterns_found": len(patterns),
            "patterns": [
                {
                    "source_column": p.source_column,
                    "target_dataset_id": p.target_dataset_id,
                    "target_object_type": p.target_object_type,
                    "target_pk_field": p.target_pk_field,
                    "confidence": p.confidence,
                    "detection_method": p.detection_method,
                    "reasons": p.reasons,
                }
                for p in patterns
            ],
        }
    except Exception as exc:
        logger.warning("detect_foreign_keys failed: %s", exc)
        return tool_error(
            str(exc)[:300],
            status_code=500,
            code=ErrorCode.INTERNAL_ERROR,
            category=ErrorCategory.INTERNAL,
        )


async def _create_link_type_from_fk(_server: Any, arguments: Dict[str, Any]) -> Any:
    db_name = str(arguments.get("db_name") or "").strip()
    fk_pattern = arguments.get("fk_pattern") or {}
    source_class_id = str(arguments.get("source_class_id") or "").strip()
    target_class_id = str(arguments.get("target_class_id") or "").strip()
    predicate = str(arguments.get("predicate") or "").strip()
    cardinality = str(arguments.get("cardinality") or "n:1").strip()
    branch = str(arguments.get("branch") or "main").strip()

    if not db_name or not fk_pattern or not source_class_id or not target_class_id:
        return missing_required_params(
            "create_link_type_from_fk",
            ["db_name", "fk_pattern", "source_class_id", "target_class_id"],
            arguments,
        )

    if not predicate:
        import re

        source_col = fk_pattern.get("source_column", "")
        name_part = re.sub(r"(_id|_fk|_key|Id|Fk)$", "", source_col)
        predicate = "has" + "".join(w.capitalize() for w in name_part.split("_"))

    try:
        link_type_body = {
            "predicate": predicate,
            "source_class": source_class_id,
            "target_class": target_class_id,
            "cardinality": cardinality,
            "relationship_spec": {
                "spec_type": "foreign_key",
                "source_column": fk_pattern.get("source_column"),
                "target_pk_field": fk_pattern.get("target_pk_field") or "id",
            },
        }

        resp = await bff_json(
            "POST",
            f"/databases/{db_name}/link-types",
            db_name=db_name,
            principal_id=None,
            principal_type=None,
            json_body=link_type_body,
            params={"branch": branch},
            timeout_seconds=30.0,
        )

        if resp.get("error"):
            payload = tool_error(
                str(resp.get("error") or "BFF request failed"),
                status_code=502,
                code=ErrorCode.UPSTREAM_ERROR,
                category=ErrorCategory.UPSTREAM,
                context={"db_name": db_name},
            )
            payload["db_name"] = db_name
            return payload

        return {
            "status": "success",
            "link_type_created": True,
            "predicate": predicate,
            "source_class": source_class_id,
            "target_class": target_class_id,
            "cardinality": cardinality,
        }
    except Exception as exc:
        logger.warning("create_link_type_from_fk failed: %s", exc)
        return tool_error(
            str(exc)[:300],
            status_code=500,
            code=ErrorCode.INTERNAL_ERROR,
            category=ErrorCategory.INTERNAL,
        )


ONTOLOGY_TOOL_HANDLERS: Dict[str, ToolHandler] = {
    "ontology_register_object_type": _ontology_register_object_type,
    "ontology_query_instances": _ontology_query_instances,
    "detect_foreign_keys": _detect_foreign_keys,
    "create_link_type_from_fk": _create_link_type_from_fk,
}


def build_ontology_tool_handlers() -> Dict[str, ToolHandler]:
    return dict(ONTOLOGY_TOOL_HANDLERS)

