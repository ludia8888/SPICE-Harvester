"""
Pipeline plan spec generator.

Builds mapping/relationship spec drafts (and optionally persists them)
from PipelinePlan outputs.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from fastapi import HTTPException, Request
from pydantic import ValidationError

from bff.routers import objectify as objectify_router
from bff.routers import link_types as link_types_router
from bff.services.oms_client import OMSClient
from shared.models.pipeline_plan import PipelinePlan, PipelinePlanOutput
from shared.services.dataset_registry import DatasetRegistry
from shared.services.objectify_registry import ObjectifyRegistry
from shared.utils.key_spec import normalize_key_spec

logger = logging.getLogger(__name__)

_NAME_TOKEN_RE = re.compile(r"[a-z0-9]+")


@dataclass(frozen=True)
class PipelineSpecOutputResult:
    output_name: str
    output_kind: str
    mapping_spec: Optional[Dict[str, Any]]
    relationship_spec: Optional[Dict[str, Any]]
    applied: bool
    errors: List[str]
    warnings: List[str]


def _normalize_name(value: str) -> str:
    raw = str(value or "").lower()
    return "".join(_NAME_TOKEN_RE.findall(raw))


def _extract_preview_columns(preview: Dict[str, Any]) -> List[str]:
    columns = preview.get("columns") if isinstance(preview, dict) else None
    if isinstance(columns, list):
        names: List[str] = []
        for col in columns:
            if isinstance(col, dict):
                name = str(col.get("name") or "").strip()
            else:
                name = str(col or "").strip()
            if name:
                names.append(name)
        return names
    return []


def _build_column_index(columns: List[str]) -> Dict[str, str]:
    output: Dict[str, str] = {}
    for col in columns:
        if col.startswith("_sys_"):
            continue
        key = _normalize_name(col)
        if key and key not in output:
            output[key] = col
    return output


def _build_property_index(properties: Dict[str, Any]) -> Dict[str, str]:
    output: Dict[str, str] = {}
    for name in properties.keys():
        key = _normalize_name(name)
        if key and key not in output:
            output[key] = name
    return output


def _match_columns_to_properties(columns: List[str], properties: Dict[str, Any]) -> List[Dict[str, str]]:
    column_index = _build_column_index(columns)
    prop_index = _build_property_index(properties)
    mappings: List[Dict[str, str]] = []
    used_targets: set[str] = set()
    for key, col_name in column_index.items():
        target = prop_index.get(key)
        if not target:
            continue
        if target in used_targets:
            continue
        used_targets.add(target)
        mappings.append({"source_field": col_name, "target_field": target})
    return mappings


def _resolve_output_node_id(definition_json: Dict[str, Any], output_name: str) -> Optional[str]:
    nodes = definition_json.get("nodes") if isinstance(definition_json, dict) else None
    if not isinstance(nodes, list):
        return None
    target = str(output_name or "").strip()
    if not target:
        return None
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if node.get("type") != "output":
            continue
        metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        name = str(
            metadata.get("outputName")
            or metadata.get("datasetName")
            or node.get("title")
            or node.get("id")
            or ""
        ).strip()
        if name and name == target:
            return str(node.get("id") or "").strip() or None
    return None


def _merge_output_binding(output: PipelinePlanOutput, binding: Optional[Dict[str, Any]]) -> PipelinePlanOutput:
    if not binding:
        return output

    updates: Dict[str, Any] = {}
    binding_kind = str(binding.get("output_kind") or "").strip().lower()
    output_kind = str(output.output_kind or "unknown")
    if binding_kind in {"object", "link", "unknown"}:
        updates["output_kind"] = binding_kind
    elif output_kind == "unknown":
        if binding.get("link_type_id") or binding.get("predicate"):
            updates["output_kind"] = "link"
        elif binding.get("target_class_id"):
            updates["output_kind"] = "object"

    for field in (
        "target_class_id",
        "source_class_id",
        "link_type_id",
        "predicate",
        "cardinality",
        "source_key_column",
        "target_key_column",
        "relationship_spec_type",
    ):
        if getattr(output, field) is None and binding.get(field):
            updates[field] = binding.get(field)

    if not updates:
        return output

    payload = output.model_dump(mode="json")
    payload.update({key: value for key, value in updates.items() if value is not None})
    try:
        return PipelinePlanOutput.model_validate(payload)
    except ValidationError:
        return output


async def _preview_for_output(
    *,
    plan: PipelinePlan,
    output_name: str,
    preview_overrides: Optional[Dict[str, Dict[str, Any]]],
    dataset_registry: DatasetRegistry,
) -> Optional[Dict[str, Any]]:
    if preview_overrides and output_name in preview_overrides:
        return preview_overrides[output_name]

    from shared.services.pipeline_executor import PipelineExecutor

    definition = dict(plan.definition_json or {})
    node_id = _resolve_output_node_id(definition, output_name)
    preview_meta = dict(definition.get("__preview_meta__") or {})
    preview_meta.setdefault("branch", str(plan.data_scope.branch or "") or "main")
    definition["__preview_meta__"] = preview_meta

    executor = PipelineExecutor(dataset_registry)
    try:
        return await executor.preview(
            definition=definition,
            db_name=str(plan.data_scope.db_name or "").strip(),
            node_id=node_id,
            limit=200,
        )
    except Exception as exc:
        logger.warning("Preview failed for output %s: %s", output_name, exc)
        return None


async def _generate_object_spec(
    *,
    output: PipelinePlanOutput,
    plan: PipelinePlan,
    preview: Optional[Dict[str, Any]],
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    oms_client: OMSClient,
    request: Request,
    output_binding: Optional[Dict[str, Any]],
    apply_specs: bool,
    auto_sync: bool,
    ontology_branch: Optional[str],
) -> PipelineSpecOutputResult:
    errors: List[str] = []
    warnings: List[str] = []
    mapping_spec: Optional[Dict[str, Any]] = None

    target_class_id = str(output.target_class_id or "").strip()
    if not target_class_id:
        errors.append("target_class_id is required for object outputs")

    preview_columns = _extract_preview_columns(preview or {})
    if not preview_columns:
        warnings.append("preview columns missing; mapping may be incomplete")

    properties: Dict[str, Any] = {}
    if target_class_id:
        try:
            ontology_payload = await oms_client.get_ontology(
                str(plan.data_scope.db_name or "").strip(),
                target_class_id,
                branch=ontology_branch or (plan.data_scope.branch or "main"),
            )
            properties, _ = objectify_router._extract_ontology_fields(ontology_payload)
        except Exception as exc:
            errors.append(f"failed to fetch ontology for {target_class_id}: {exc}")

    mappings = _match_columns_to_properties(preview_columns, properties)
    if not mappings:
        warnings.append("no matching columns found for ontology properties")

    dataset_id = str((output_binding or {}).get("dataset_id") or "").strip() or None
    dataset_branch = str((output_binding or {}).get("dataset_branch") or plan.data_scope.branch or "main").strip() or "main"
    artifact_output_name = str(
        (output_binding or {}).get("artifact_output_name") or output.output_name
    ).strip()

    draft = {
        "dataset_id": dataset_id,
        "dataset_branch": dataset_branch,
        "artifact_output_name": artifact_output_name,
        "target_class_id": target_class_id,
        "mappings": mappings,
        "auto_sync": auto_sync,
    }

    if apply_specs:
        if not dataset_id:
            errors.append("dataset_id is required to apply mapping specs")
        if not mappings:
            errors.append("no column mappings available for mapping spec")
        if not errors:
            try:
                request_payload = objectify_router.CreateMappingSpecRequest(
                    dataset_id=dataset_id,
                    dataset_branch=dataset_branch,
                    artifact_output_name=artifact_output_name,
                    schema_hash=None,
                    target_class_id=target_class_id,
                    mappings=[objectify_router.MappingSpecField(**m) for m in mappings],
                    status="ACTIVE",
                    auto_sync=auto_sync,
                    options=None,
                )
                response = await objectify_router.create_mapping_spec(
                    body=request_payload,
                    request=request,
                    dataset_registry=dataset_registry,
                    objectify_registry=objectify_registry,
                    oms_client=oms_client,
                )
                mapping_spec = response.get("data", {}).get("mapping_spec") if isinstance(response, dict) else None
            except HTTPException as exc:
                errors.append(str(exc.detail))
            except Exception as exc:
                errors.append(str(exc))

    if mapping_spec is None:
        mapping_spec = draft

    return PipelineSpecOutputResult(
        output_name=output.output_name,
        output_kind="object",
        mapping_spec=mapping_spec,
        relationship_spec=None,
        applied=apply_specs and not errors,
        errors=errors,
        warnings=warnings,
    )


async def _generate_link_spec(
    *,
    output: PipelinePlanOutput,
    plan: PipelinePlan,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    oms_client: OMSClient,
    request: Request,
    output_binding: Optional[Dict[str, Any]],
    apply_specs: bool,
    auto_sync: bool,
    ontology_branch: Optional[str],
    dangling_policy: str,
    dedupe_policy: str,
) -> PipelineSpecOutputResult:
    errors: List[str] = []
    warnings: List[str] = []
    mapping_spec: Optional[Dict[str, Any]] = None
    relationship_spec: Optional[Dict[str, Any]] = None
    source_pk_fields: List[str] = []

    link_type_id = str(output.link_type_id or "").strip()
    source_class = str(output.source_class_id or "").strip()
    target_class = str(output.target_class_id or "").strip()
    predicate = str(output.predicate or "").strip()
    cardinality = str(output.cardinality or "").strip()
    source_key_column = str(output.source_key_column or "").strip()
    target_key_column = str(output.target_key_column or "").strip()

    for label, value in (
        ("link_type_id", link_type_id),
        ("source_class_id", source_class),
        ("target_class_id", target_class),
        ("predicate", predicate),
        ("cardinality", cardinality),
        ("source_key_column", source_key_column),
        ("target_key_column", target_key_column),
    ):
        if not value:
            errors.append(f"{label} is required for link outputs")

    dataset_id = str((output_binding or {}).get("dataset_id") or "").strip() or None
    dataset_version_id = str((output_binding or {}).get("dataset_version_id") or "").strip() or None
    dataset_branch = str((output_binding or {}).get("dataset_branch") or plan.data_scope.branch or "main").strip() or "main"
    relationship_type = str(output.relationship_spec_type or "join_table").strip().lower() or "join_table"
    auto_create = False
    if apply_specs and not dataset_id:
        if relationship_type == "join_table":
            auto_create = True
            warnings.append("dataset_id missing; join dataset will be auto-created")
        else:
            errors.append("dataset_id is required to apply link specs")

    if errors:
        return PipelineSpecOutputResult(
            output_name=output.output_name,
            output_kind="link",
            mapping_spec=None,
            relationship_spec=None,
            applied=False,
            errors=errors,
            warnings=warnings,
        )

    if not apply_specs:
        if not dataset_id:
            warnings.append("dataset_id missing; link specs will be unbound drafts")
        mapping_spec = {
            "dataset_id": dataset_id,
            "dataset_branch": dataset_branch,
            "link_type_id": link_type_id,
            "mappings": [
                {"source_field": source_key_column, "target_field": ""},
                {"source_field": target_key_column, "target_field": predicate},
            ],
        }
        relationship_spec = {
            "relationship_spec_id": None,
            "link_type_id": link_type_id,
            "predicate": predicate,
            "spec_type": output.relationship_spec_type or "join_table",
            "spec": {
                "type": output.relationship_spec_type or "join_table",
                "join_dataset_id": dataset_id,
                "join_dataset_version_id": dataset_version_id,
                "source_key_column": source_key_column,
                "target_key_column": target_key_column,
                "dedupe_policy": dedupe_policy,
                "dangling_policy": dangling_policy,
                "auto_sync": auto_sync,
            },
        }
        return PipelineSpecOutputResult(
            output_name=output.output_name,
            output_kind="link",
            mapping_spec=mapping_spec,
            relationship_spec=relationship_spec,
            applied=False,
            errors=[],
            warnings=warnings,
        )

    try:
        source_ontology = await oms_client.get_ontology(
            str(plan.data_scope.db_name or "").strip(),
            source_class,
            branch=ontology_branch or (plan.data_scope.branch or "main"),
        )
        target_ontology = await oms_client.get_ontology(
            str(plan.data_scope.db_name or "").strip(),
            target_class,
            branch=ontology_branch or (plan.data_scope.branch or "main"),
        )
        source_props = link_types_router._extract_ontology_properties(source_ontology)
        target_props = link_types_router._extract_ontology_properties(target_ontology)

        _, source_contract = await link_types_router._resolve_object_type_contract(
            oms_client=oms_client,
            db_name=str(plan.data_scope.db_name or "").strip(),
            class_id=source_class,
            branch=ontology_branch or (plan.data_scope.branch or "main"),
        )
        _, target_contract = await link_types_router._resolve_object_type_contract(
            oms_client=oms_client,
            db_name=str(plan.data_scope.db_name or "").strip(),
            class_id=target_class,
            branch=ontology_branch or (plan.data_scope.branch or "main"),
        )

        source_pk = normalize_key_spec(source_contract.get("pk_spec") or {}, columns=list(source_props.keys()))
        target_pk = normalize_key_spec(target_contract.get("pk_spec") or {}, columns=list(target_props.keys()))
        source_pk_fields = [str(v).strip() for v in source_pk.get("primary_key") or [] if str(v).strip()]
        target_pk_fields = [str(v).strip() for v in target_pk.get("primary_key") or [] if str(v).strip()]
        if len(source_pk_fields) != 1 or len(target_pk_fields) != 1:
            errors.append("composite primary keys require explicit mapping")

        spec_payload = {
            "type": relationship_type,
            "join_dataset_id": dataset_id,
            "join_dataset_version_id": dataset_version_id,
            "join_dataset_branch": dataset_branch,
            "join_dataset_name": output.output_name,
            "autoCreate": auto_create,
            "source_key_column": source_key_column,
            "target_key_column": target_key_column,
            "dedupe_policy": dedupe_policy,
            "dangling_policy": dangling_policy,
            "auto_sync": auto_sync,
        }

        relationship_spec_id = str(uuid4())
        mapping_request, resolved_dataset_id, resolved_dataset_version_id, resolved_spec_type = await link_types_router._build_mapping_request(
            db_name=str(plan.data_scope.db_name or "").strip(),
            request=request,
            oms_client=oms_client,
            dataset_registry=dataset_registry,
            relationship_spec_id=relationship_spec_id,
            link_type_id=link_type_id,
            source_class=source_class,
            target_class=target_class,
            predicate=predicate,
            cardinality=cardinality,
            branch=ontology_branch or (plan.data_scope.branch or "main"),
            source_props=source_props,
            target_props=target_props,
            source_contract=source_contract,
            target_contract=target_contract,
            spec_payload=spec_payload,
        )

        mapping_response = await objectify_router.create_mapping_spec(
            body=mapping_request,
            request=request,
            dataset_registry=dataset_registry,
            objectify_registry=objectify_registry,
            oms_client=oms_client,
        )
        mapping_spec = mapping_response.get("data", {}).get("mapping_spec") if isinstance(mapping_response, dict) else None
        if not isinstance(mapping_spec, dict):
            errors.append("failed to create mapping spec for link output")
        else:
            relationship_record = await dataset_registry.create_relationship_spec(
                relationship_spec_id=relationship_spec_id,
                link_type_id=link_type_id,
                db_name=str(plan.data_scope.db_name or "").strip(),
                source_object_type=source_class,
                target_object_type=target_class,
                predicate=predicate,
                spec_type=resolved_spec_type,
                dataset_id=resolved_dataset_id,
                dataset_version_id=resolved_dataset_version_id,
                mapping_spec_id=str(mapping_spec.get("mapping_spec_id") or ""),
                mapping_spec_version=int(mapping_spec.get("version") or mapping_spec.get("mapping_spec_version") or 1),
                spec=spec_payload,
                status="ACTIVE",
                auto_sync=auto_sync,
            )
            relationship_spec = relationship_record.__dict__ if relationship_record else None
    except HTTPException as exc:
        errors.append(str(exc.detail))
    except Exception as exc:
        errors.append(str(exc))

    if mapping_spec is None:
        mapping_spec = {
            "dataset_id": dataset_id,
            "dataset_branch": dataset_branch,
            "link_type_id": link_type_id,
            "mappings": [
                {"source_field": source_key_column, "target_field": (source_pk_fields[0] if source_pk_fields else "")},
                {"source_field": target_key_column, "target_field": predicate},
            ],
        }

    if relationship_spec is None:
        relationship_spec = {
            "relationship_spec_id": None,
            "link_type_id": link_type_id,
            "predicate": predicate,
            "spec_type": output.relationship_spec_type or "join_table",
            "spec": {
                "type": output.relationship_spec_type or "join_table",
                "join_dataset_id": dataset_id,
                "join_dataset_version_id": dataset_version_id,
                "source_key_column": source_key_column,
                "target_key_column": target_key_column,
                "dedupe_policy": dedupe_policy,
                "dangling_policy": dangling_policy,
                "auto_sync": auto_sync,
            },
        }

    return PipelineSpecOutputResult(
        output_name=output.output_name,
        output_kind="link",
        mapping_spec=mapping_spec,
        relationship_spec=relationship_spec,
        applied=apply_specs and not errors,
        errors=errors,
        warnings=warnings,
    )


async def generate_pipeline_specs(
    *,
    plan: PipelinePlan,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    oms_client: OMSClient,
    request: Request,
    preview_overrides: Optional[Dict[str, Dict[str, Any]]],
    output_bindings: Optional[Dict[str, Dict[str, Any]]],
    apply_specs: bool,
    auto_sync: bool,
    ontology_branch: Optional[str],
    dangling_policy: str,
    dedupe_policy: str,
) -> List[PipelineSpecOutputResult]:
    results: List[PipelineSpecOutputResult] = []
    for output in plan.outputs or []:
        preview = await _preview_for_output(
            plan=plan,
            output_name=output.output_name,
            preview_overrides=preview_overrides,
            dataset_registry=dataset_registry,
        )
        binding = (output_bindings or {}).get(output.output_name)
        effective_output = _merge_output_binding(output, binding)
        kind = str(effective_output.output_kind)
        if kind == "object":
            results.append(
                await _generate_object_spec(
                    output=effective_output,
                    plan=plan,
                    preview=preview,
                    dataset_registry=dataset_registry,
                    objectify_registry=objectify_registry,
                    oms_client=oms_client,
                    request=request,
                    output_binding=binding,
                    apply_specs=apply_specs,
                    auto_sync=auto_sync,
                    ontology_branch=ontology_branch,
                )
            )
        elif kind == "link":
            results.append(
                await _generate_link_spec(
                    output=effective_output,
                    plan=plan,
                    dataset_registry=dataset_registry,
                    objectify_registry=objectify_registry,
                    oms_client=oms_client,
                    request=request,
                    output_binding=binding,
                    apply_specs=apply_specs,
                    auto_sync=auto_sync,
                    ontology_branch=ontology_branch,
                    dangling_policy=dangling_policy,
                    dedupe_policy=dedupe_policy,
                )
            )
        else:
            results.append(
                PipelineSpecOutputResult(
                    output_name=output.output_name,
                    output_kind="unknown",
                    mapping_spec=None,
                    relationship_spec=None,
                    applied=False,
                    errors=["output_kind is unknown"],
                    warnings=[],
                )
            )
    return results
