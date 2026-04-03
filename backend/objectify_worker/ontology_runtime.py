from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

from shared.models.objectify_job import ObjectifyJob


logger = logging.getLogger(__name__)


async def fetch_target_field_types(worker: Any, job: ObjectifyJob) -> Dict[str, str]:
    payload = await fetch_class_schema(worker, job)
    prop_map, _ = worker._extract_ontology_fields(payload)
    field_types: Dict[str, str] = {}
    for name, meta in prop_map.items():
        raw_type = meta.get("type") or meta.get("data_type") or meta.get("datatype")
        is_relationship = bool(
            raw_type == "link"
            or meta.get("isRelationship")
            or meta.get("target")
            or meta.get("linkTarget")
        )
        items = meta.get("items") if isinstance(meta, dict) else None
        if isinstance(items, dict):
            item_type = items.get("type")
            if item_type == "link" and (items.get("target") or items.get("linkTarget")):
                is_relationship = True
        if is_relationship:
            continue
        import_type = worker._resolve_import_type(raw_type)
        if import_type:
            field_types[name] = import_type
    return field_types


async def fetch_class_schema(worker: Any, job: ObjectifyJob) -> Dict[str, Any]:
    if not worker.http:
        return {}
    branch = job.ontology_branch or job.dataset_branch or "main"
    try:
        resp = await worker.http.get_ontology_typed(
            db_name=job.db_name,
            class_id=job.target_class_id,
            branch=branch,
        )
        if resp.status_code == 200:
            return resp.json() if resp.text else {}
        logger.debug(
            "OMS ontology fetch returned %d for %s/%s",
            resp.status_code,
            job.db_name,
            job.target_class_id,
        )
        return {}
    except Exception as exc:
        logger.debug(
            "OMS ontology fetch failed for %s/%s: %s",
            job.db_name,
            job.target_class_id,
            exc,
        )
        return {}


async def fetch_object_type_contract(worker: Any, job: ObjectifyJob) -> Dict[str, Any]:
    if not worker.http:
        return {}
    branch = job.ontology_branch or job.dataset_branch or "main"
    resp = await worker.http.get(
        f"/api/v1/database/{job.db_name}/ontology/resources/object_type/{job.target_class_id}",
        params={"branch": branch},
    )
    if resp.status_code == 404:
        return {}
    resp.raise_for_status()
    return resp.json() if resp.text else {}


async def fetch_value_type_defs(
    worker: Any,
    job: ObjectifyJob,
    value_type_refs: set[str],
) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    if not worker.http or not value_type_refs:
        return {}, []

    branch = job.ontology_branch or job.dataset_branch or "main"
    defs: Dict[str, Dict[str, Any]] = {}
    missing: List[str] = []

    for ref in sorted(value_type_refs):
        resp = await worker.http.get(
            f"/api/v1/database/{job.db_name}/ontology/resources/value_type/{ref}",
            params={"branch": branch},
        )
        if resp.status_code == 404:
            missing.append(ref)
            continue
        resp.raise_for_status()
        payload = resp.json() if resp.text else {}
        resource = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(resource, dict):
            resource = payload if isinstance(payload, dict) else {}
        spec = resource.get("spec") if isinstance(resource, dict) else {}
        if not isinstance(spec, dict):
            spec = {}
        defs[ref] = {
            "base_type": spec.get("base_type") or spec.get("baseType"),
            "constraints": spec.get("constraints") or {},
        }

    return defs, missing


async def fetch_ontology_version(worker: Any, job: ObjectifyJob) -> Dict[str, str]:
    branch = job.ontology_branch or job.dataset_branch or "main"
    ref = f"branch:{branch}"
    return {"ref": ref, "commit": ref}
