from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional

from shared.errors.runtime_exception_policy import RuntimeZone, record_lineage_or_raise
from shared.models.lineage_edge_types import EDGE_DATASET_VERSION_OBJECTIFIED
from shared.models.objectify_job import ObjectifyJob
from shared.services.registries.lineage_store import LineageStore


logger = logging.getLogger(__name__)


def build_column_lineage_pairs(mappings: Any, *, limit: int = 1000) -> List[Dict[str, str]]:
    if not isinstance(mappings, list):
        return []
    pairs: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    max_pairs = max(1, int(limit))
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        source_field = mapping.get("source_field")
        if source_field is None:
            source_field = mapping.get("sourceField")
        target_field = mapping.get("target_field")
        if target_field is None:
            target_field = mapping.get("targetField")
        source = str(source_field or "").strip()
        target = str(target_field or "").strip()
        if not source or not target:
            continue
        key = (source, target)
        if key in seen:
            continue
        seen.add(key)
        pairs.append({"source_field": source, "target_field": target})
        if len(pairs) >= max_pairs:
            break
    return pairs


async def record_lineage_header(
    worker: Any,
    *,
    job: ObjectifyJob,
    mapping_spec: Any,
    ontology_version: Optional[Dict[str, str]],
    input_type: str,
    artifact_output_name: Optional[str] = None,
) -> Optional[str]:
    if not worker.lineage_store:
        if worker.lineage_required:
            raise RuntimeError("LineageStore unavailable")
        return None

    async def _record_header_links() -> str:
        job_node = LineageStore.node_aggregate("ObjectifyJob", job.job_id)
        source_branch = worker._source_lineage_branch(job)
        target_branch = worker._target_lineage_branch(job)
        column_lineage_pairs = build_column_lineage_pairs(getattr(mapping_spec, "mappings", None))
        if input_type == "artifact":
            source_node = LineageStore.node_aggregate("PipelineArtifact", str(job.artifact_id))
            edge_type = "pipeline_artifact_objectify_job"
            edge_metadata = {
                "db_name": job.db_name,
                "branch": source_branch,
                "dataset_id": job.dataset_id,
                "artifact_id": job.artifact_id,
                "artifact_output_name": artifact_output_name or job.artifact_output_name,
                "mapping_spec_id": job.mapping_spec_id,
                "mapping_spec_version": job.mapping_spec_version,
                "target_class_id": job.target_class_id,
                "column_lineage_ref": f"objectify_mapping_spec:{mapping_spec.mapping_spec_id}:v{int(mapping_spec.version)}",
                "column_lineage_storage": "postgres.objectify_registry",
                "column_lineage_schema_version": "v1",
            }
        else:
            source_node = LineageStore.node_aggregate("DatasetVersion", str(job.dataset_version_id))
            edge_type = "dataset_version_objectify_job"
            edge_metadata = {
                "db_name": job.db_name,
                "branch": source_branch,
                "dataset_id": job.dataset_id,
                "dataset_version_id": job.dataset_version_id,
                "mapping_spec_id": job.mapping_spec_id,
                "mapping_spec_version": job.mapping_spec_version,
                "target_class_id": job.target_class_id,
                "column_lineage_ref": f"objectify_mapping_spec:{mapping_spec.mapping_spec_id}:v{int(mapping_spec.version)}",
                "column_lineage_storage": "postgres.objectify_registry",
                "column_lineage_schema_version": "v1",
            }
        if column_lineage_pairs is not None:
            edge_metadata["column_lineage_pairs"] = column_lineage_pairs
        await worker.lineage_store.record_link(  # type: ignore[union-attr]
            from_node_id=source_node,
            to_node_id=job_node,
            edge_type=edge_type,
            occurred_at=datetime.now(timezone.utc),
            db_name=job.db_name,
            branch=source_branch,
            edge_metadata=edge_metadata,
        )

        mapping_version_id = f"{mapping_spec.mapping_spec_id}:v{mapping_spec.version}"
        mapping_node = LineageStore.node_aggregate("MappingSpecVersion", mapping_version_id)
        await worker.lineage_store.record_link(  # type: ignore[union-attr]
            from_node_id=job_node,
            to_node_id=mapping_node,
            edge_type="objectify_job_mapping_spec",
            occurred_at=datetime.now(timezone.utc),
            db_name=job.db_name,
            branch=target_branch,
            edge_metadata={
                "db_name": job.db_name,
                "branch": target_branch,
                "mapping_spec_id": mapping_spec.mapping_spec_id,
                "mapping_spec_version": mapping_spec.version,
                "dataset_id": job.dataset_id,
                "target_class_id": job.target_class_id,
                "column_lineage_ref": f"objectify_mapping_spec:{mapping_spec.mapping_spec_id}:v{int(mapping_spec.version)}",
                "column_lineage_storage": "postgres.objectify_registry",
                "column_lineage_schema_version": "v1",
            },
        )

        if ontology_version:
            branch = target_branch
            ont_id = f"{job.db_name}:{branch}:{ontology_version.get('commit') or 'head'}"
            ont_node = LineageStore.node_aggregate("OntologyVersion", ont_id)
            await worker.lineage_store.record_link(  # type: ignore[union-attr]
                from_node_id=job_node,
                to_node_id=ont_node,
                edge_type="objectify_job_ontology_version",
                occurred_at=datetime.now(timezone.utc),
                db_name=job.db_name,
                branch=branch,
                edge_metadata={
                    "db_name": job.db_name,
                    "branch": branch,
                    "ontology": ontology_version,
                },
            )
        return job_node

    return await record_lineage_or_raise(
        lineage_store=worker.lineage_store,
        required=worker.lineage_required,
        record_call=_record_header_links,
        logger=logger,
        operation="objectify_worker.record_lineage_header",
        zone=RuntimeZone.CORE,
        context={
            "job_id": job.job_id,
            "db_name": job.db_name,
            "dataset_id": job.dataset_id,
            "mapping_spec_id": job.mapping_spec_id,
            "mapping_spec_version": job.mapping_spec_version,
            "target_class_id": job.target_class_id,
            "input_type": input_type,
        },
    )


async def record_instance_lineage(
    worker: Any,
    *,
    job: ObjectifyJob,
    job_node_id: Optional[str],
    instance_ids: List[str],
    mapping_spec_id: str,
    mapping_spec_version: int,
    column_lineage_pairs: Optional[List[Dict[str, str]]],
    ontology_version: Optional[Dict[str, str]],
    limit_remaining: int,
    input_type: str,
    artifact_output_name: Optional[str] = None,
) -> int:
    if not worker.lineage_store:
        if worker.lineage_required:
            raise RuntimeError("LineageStore unavailable")
        return limit_remaining
    if limit_remaining <= 0:
        return limit_remaining

    column_lineage_metadata: Dict[str, Any] = {
        "column_lineage_ref": f"objectify_mapping_spec:{mapping_spec_id}:v{int(mapping_spec_version)}",
        "column_lineage_storage": "postgres.objectify_registry",
        "column_lineage_schema_version": "v1",
    }
    if column_lineage_pairs is not None:
        column_lineage_metadata["column_lineage_pairs"] = column_lineage_pairs

    if input_type == "artifact":
        source_node = LineageStore.node_aggregate("PipelineArtifact", str(job.artifact_id))
        edge_type = "pipeline_artifact_objectified"
        lineage_branch = worker._target_lineage_branch(job)
        edge_metadata = {
            "db_name": job.db_name,
            "branch": lineage_branch,
            "dataset_id": job.dataset_id,
            "artifact_id": job.artifact_id,
            "artifact_output_name": artifact_output_name or job.artifact_output_name,
            "mapping_spec_id": mapping_spec_id,
            "mapping_spec_version": mapping_spec_version,
            "target_class_id": job.target_class_id,
            "ontology": ontology_version or {},
            **column_lineage_metadata,
        }
    else:
        source_node = LineageStore.node_aggregate("DatasetVersion", str(job.dataset_version_id))
        edge_type = EDGE_DATASET_VERSION_OBJECTIFIED
        lineage_branch = worker._target_lineage_branch(job)
        edge_metadata = {
            "db_name": job.db_name,
            "branch": lineage_branch,
            "dataset_id": job.dataset_id,
            "dataset_version_id": job.dataset_version_id,
            "mapping_spec_id": mapping_spec_id,
            "mapping_spec_version": mapping_spec_version,
            "target_class_id": job.target_class_id,
            "ontology": ontology_version or {},
            **column_lineage_metadata,
        }

    for instance_id in instance_ids:
        if limit_remaining <= 0:
            break
        aggregate_id = f"{job.db_name}:{lineage_branch}:{job.target_class_id}:{instance_id}"
        instance_node = LineageStore.node_aggregate("Instance", aggregate_id)

        async def _record_instance_links() -> bool:
            await worker.lineage_store.record_link(  # type: ignore[union-attr]
                from_node_id=source_node,
                to_node_id=instance_node,
                edge_type=edge_type,
                occurred_at=datetime.now(timezone.utc),
                db_name=job.db_name,
                branch=lineage_branch,
                edge_metadata=edge_metadata,
            )
            if job_node_id:
                await worker.lineage_store.record_link(  # type: ignore[union-attr]
                    from_node_id=job_node_id,
                    to_node_id=instance_node,
                    edge_type="objectify_job_created_instance",
                    occurred_at=datetime.now(timezone.utc),
                    db_name=job.db_name,
                    branch=lineage_branch,
                    edge_metadata={
                        "db_name": job.db_name,
                        "branch": lineage_branch,
                        "dataset_id": job.dataset_id,
                        "dataset_version_id": job.dataset_version_id,
                        "artifact_id": job.artifact_id,
                        "artifact_output_name": artifact_output_name or job.artifact_output_name,
                        "mapping_spec_id": mapping_spec_id,
                        "mapping_spec_version": mapping_spec_version,
                        "target_class_id": job.target_class_id,
                        "ontology": ontology_version or {},
                        **column_lineage_metadata,
                    },
                )
            return True

        recorded = await record_lineage_or_raise(
            lineage_store=worker.lineage_store,
            required=worker.lineage_required,
            record_call=_record_instance_links,
            logger=logger,
            operation="objectify_worker.record_instance_lineage",
            zone=RuntimeZone.CORE,
            context={
                "job_id": job.job_id,
                "db_name": job.db_name,
                "dataset_id": job.dataset_id,
                "instance_id": instance_id,
                "target_class_id": job.target_class_id,
                "input_type": input_type,
            },
        )
        if recorded:
            limit_remaining -= 1
    return limit_remaining
