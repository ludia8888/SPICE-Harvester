from __future__ import annotations

from typing import Any, Dict, List

from shared.services.pipeline.pipeline_definition_validator import (
    PipelineDefinitionValidationPolicy,
    validate_pipeline_definition,
)
from shared.services.pipeline.pipeline_transform_spec import SUPPORTED_TRANSFORMS


class PipelineValidationDomain:
    def __init__(self, worker: Any) -> None:
        self._worker = worker

    def validate_execution_prerequisites(self) -> None:
        if not self._worker.pipeline_registry or not self._worker.dataset_registry:
            raise RuntimeError("Registry services not available")
        if not self._worker.spark:
            raise RuntimeError("Spark session not initialized")
        if not self._worker.storage:
            raise RuntimeError("Storage service not available")

    def validate_required_subgraph(
        self,
        nodes: Dict[str, Dict[str, Any]],
        incoming: Dict[str, List[str]],
        required_node_ids: set[str],
    ) -> List[str]:
        errors: List[str] = []
        if not required_node_ids:
            return errors
        for node_id in required_node_ids:
            node = nodes.get(node_id) or {}
            node_type = str(node.get("type") or "transform")
            if node_type in {"transform", "output"} and not incoming.get(node_id):
                errors.append(f"{node_type} node {node_id} has no input")
        return errors

    def validate_definition(self, definition: Dict[str, Any], *, require_output: bool = True) -> List[str]:
        policy = PipelineDefinitionValidationPolicy(
            supported_ops=SUPPORTED_TRANSFORMS,
            require_output=require_output,
            normalize_metadata=True,
            require_udf_reference=self._worker._udf_require_reference,
            require_udf_version_pinning=self._worker._udf_require_version_pinning,
        )
        validation = validate_pipeline_definition(definition, policy=policy)
        errors: List[str] = list(validation.errors)
        nodes = validation.nodes

        for node_id, node in nodes.items():
            if node.get("type") != "transform":
                continue
            metadata = node.get("metadata") or {}

            checks = metadata.get("schemaChecks") or []
            if checks:
                if not isinstance(checks, list):
                    errors.append(f"schemaChecks must be a list on node {node_id}")
                else:
                    for check in checks:
                        if not isinstance(check, dict):
                            errors.append(f"schema check invalid on node {node_id}")
                            continue
                        rule = str(check.get("rule") or "").strip()
                        column = str(check.get("column") or "").strip()
                        if not rule:
                            errors.append(f"schema check missing rule on node {node_id}")
                        if not column:
                            errors.append(f"schema check missing column on node {node_id}")

        expectations = definition.get("expectations") or []
        if expectations:
            if not isinstance(expectations, list):
                errors.append("expectations must be a list")
            else:
                for exp in expectations:
                    if not isinstance(exp, dict):
                        errors.append("expectation entry must be an object")
                        continue
                    rule = str(exp.get("rule") or "").strip()
                    if not rule:
                        errors.append("expectation missing rule")
                        continue
                    column = str(exp.get("column") or "").strip()
                    if rule not in {"row_count_min", "row_count_max"} and not column:
                        errors.append(f"expectation {rule} missing column")

        contract = definition.get("schemaContract") or definition.get("schema_contract") or []
        if contract:
            if not isinstance(contract, list):
                errors.append("schemaContract must be a list")
            else:
                for item in contract:
                    if not isinstance(item, dict):
                        errors.append("schemaContract entry must be an object")
                        continue
                    column = str(item.get("column") or "").strip()
                    if not column:
                        errors.append("schemaContract missing column")

        dependencies = definition.get("dependencies") or []
        if dependencies:
            if not isinstance(dependencies, list):
                errors.append("dependencies must be a list")
            else:
                for dep in dependencies:
                    if not isinstance(dep, dict):
                        errors.append("dependency entry must be an object")
                        continue
                    pipeline_id = dep.get("pipelineId") or dep.get("pipeline_id")
                    if not pipeline_id:
                        errors.append("dependency missing pipeline_id")

        return errors
