from __future__ import annotations

import logging
from typing import Any, Optional, Tuple

from shared.models.objectify_job import ObjectifyJob
from shared.services.registries.backing_source_adapter import BackingSourceMappingSpec
from shared.utils.object_type_backing import select_primary_backing_source
from shared.utils.schema_hash import compute_schema_hash_from_payload
from shared.utils.s3_uri import parse_s3_uri

logger = logging.getLogger(__name__)


async def resolve_job_input_context(
    worker: Any,
    *,
    job: ObjectifyJob,
    fail_job: Any,
) -> Tuple[str, Optional[str], Optional[str]]:
    dataset = await worker.dataset_registry.get_dataset(dataset_id=job.dataset_id)
    if not dataset:
        await fail_job(f"dataset_not_found:{job.dataset_id}")
    if dataset.db_name != job.db_name:
        await fail_job("dataset_db_name_mismatch")

    input_type = "dataset_version" if job.dataset_version_id else "artifact"
    resolved_output_name: Optional[str] = None
    stable_seed: Optional[str] = None

    if job.dataset_version_id and job.artifact_id:
        await fail_job("objectify_input_conflict")
    if not job.dataset_version_id and not job.artifact_id:
        await fail_job("objectify_input_missing")

    if job.dataset_version_id:
        version = await worker.dataset_registry.get_version(version_id=job.dataset_version_id)
        if not version or version.dataset_id != job.dataset_id:
            await fail_job("dataset_version_mismatch")
        resolved_artifact_key = job.artifact_key or version.artifact_key
        if not resolved_artifact_key:
            await fail_job("artifact_key_missing")
        if version.artifact_key and job.artifact_key and version.artifact_key != job.artifact_key:
            await fail_job("artifact_key_mismatch")
        stable_seed = job.dataset_version_id
    else:
        resolved_artifact_key, resolved_output_name = await resolve_artifact_output(worker, job)
        if job.artifact_key and job.artifact_key != resolved_artifact_key:
            await fail_job("artifact_key_mismatch")
        stable_seed = f"artifact:{job.artifact_id}:{resolved_output_name}"

    job.artifact_key = resolved_artifact_key
    if resolved_output_name and not job.artifact_output_name:
        job.artifact_output_name = resolved_output_name

    return input_type, resolved_output_name, stable_seed


async def resolve_mapping_spec_for_job(
    worker: Any,
    *,
    job: ObjectifyJob,
    fail_job: Any,
) -> Any:
    if not job.mapping_spec_id:
        try:
            ot_contract = await worker._fetch_object_type_contract(job)
        except Exception as oms_fetch_exc:
            logger.error(
                "OMS object_type fetch failed (job_id=%s class_id=%s): %s",
                job.job_id,
                job.target_class_id,
                oms_fetch_exc,
            )
            raise
        if not ot_contract:
            await fail_job(f"oms_object_type_not_found:{job.target_class_id}")
        ot_resource = ot_contract.get("data") if isinstance(ot_contract.get("data"), dict) else ot_contract
        ot_spec = (ot_resource.get("spec") if isinstance(ot_resource, dict) else {}) or {}
        backing = select_primary_backing_source(ot_spec)
        backing_dataset_id = str(backing.get("dataset_id") or "").strip()
        if backing_dataset_id and backing_dataset_id != job.dataset_id:
            await fail_job(
                f"oms_backing_dataset_mismatch(job={job.dataset_id} backing={backing_dataset_id})"
            )
        prop_mappings = backing.get("property_mappings")
        if not prop_mappings or not isinstance(prop_mappings, list):
            await fail_job(f"oms_backing_source_no_property_mappings:{job.target_class_id}")

        dataset_branch = str(
            backing.get("dataset_branch")
            or backing.get("branch")
            or job.dataset_branch
            or "main"
        ).strip() or "main"
        return BackingSourceMappingSpec(
            mapping_spec_id=f"oms:{job.target_class_id}",
            dataset_id=backing_dataset_id or job.dataset_id,
            dataset_branch=dataset_branch,
            artifact_output_name=str(backing.get("artifact_output_name") or job.artifact_output_name or "").strip() or None,
            schema_hash=backing.get("schema_hash"),
            target_class_id=job.target_class_id,
            mappings=[
                {"source_field": str(m.get("source_field", "")), "target_field": str(m.get("target_field", ""))}
                for m in prop_mappings
                if isinstance(m, dict)
            ],
            target_field_types=backing.get("target_field_types") or {},
            auto_sync=backing.get("auto_sync", True),
            version=int(backing.get("mapping_version") or 1),
            status=str(ot_spec.get("status") or "ACTIVE"),
            backing_datasource_id=str(backing.get("backing_datasource_id") or backing.get("ref") or "").strip() or None,
            backing_datasource_version_id=str(
                backing.get("backing_datasource_version_id")
                or backing.get("version_id")
                or backing.get("backing_version_id")
                or ""
            ).strip() or None,
        )

    mapping_spec = await worker.objectify_registry.get_mapping_spec(mapping_spec_id=job.mapping_spec_id)
    if not mapping_spec:
        await fail_job(f"mapping_spec_not_found:{job.mapping_spec_id}")
    if mapping_spec.dataset_id != job.dataset_id:
        await fail_job("mapping_spec_dataset_mismatch")
    if job.mapping_spec_version is not None and int(mapping_spec.version) != int(job.mapping_spec_version):
        await fail_job(
            f"mapping_spec_version_mismatch(job={job.mapping_spec_version} spec={mapping_spec.version})"
        )
    if mapping_spec.backing_datasource_version_id:
        if job.artifact_id:
            await fail_job("backing_datasource_version_conflict")
        backing_version = await worker.dataset_registry.get_backing_datasource_version(
            version_id=mapping_spec.backing_datasource_version_id
        )
        if not backing_version:
            await fail_job("backing_datasource_version_missing")
        if not job.dataset_version_id:
            await fail_job("backing_datasource_version_required")
        if backing_version.dataset_version_id != job.dataset_version_id:
            resolved_backing_id = (
                str(getattr(mapping_spec, "backing_datasource_id", "") or "").strip()
                or str(getattr(backing_version, "backing_id", "") or "").strip()
            )
            if not resolved_backing_id:
                await fail_job("backing_datasource_id_missing")

            expected_schema_hash = str(getattr(mapping_spec, "schema_hash", "") or "").strip()
            job_version = await worker.dataset_registry.get_version(version_id=job.dataset_version_id)
            if not job_version:
                await fail_job("dataset_version_missing")
            observed_schema_hash = compute_schema_hash_from_payload(getattr(job_version, "sample_json", None))
            if expected_schema_hash and observed_schema_hash and expected_schema_hash != observed_schema_hash:
                await fail_job(
                    f"backing_schema_hash_mismatch(expected={expected_schema_hash} observed={observed_schema_hash})"
                )

            try:
                advanced = await worker.dataset_registry.get_or_create_backing_datasource_version(
                    backing_id=resolved_backing_id,
                    dataset_version_id=job.dataset_version_id,
                    schema_hash=expected_schema_hash or observed_schema_hash,
                    metadata={"artifact_key": job.artifact_key} if job.artifact_key else None,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to auto-advance backing datasource version (mapping_spec_id=%s dataset_version_id=%s): %s",
                    getattr(mapping_spec, "mapping_spec_id", None),
                    job.dataset_version_id,
                    exc,
                )
                await fail_job("backing_datasource_version_mismatch")
                raise

            try:
                updated = await worker.objectify_registry.update_mapping_spec(
                    mapping_spec_id=str(mapping_spec.mapping_spec_id),
                    backing_datasource_version_id=str(advanced.version_id),
                )
                if updated:
                    mapping_spec = updated
            except Exception as exc:
                logger.warning(
                    "Failed to advance mapping spec backing version in objectify-worker (mapping_spec_id=%s): %s",
                    getattr(mapping_spec, "mapping_spec_id", None),
                    exc,
                )
    return mapping_spec


async def resolve_artifact_output(worker: Any, job: ObjectifyJob) -> Tuple[str, str]:
    if not worker.pipeline_registry:
        raise RuntimeError("PipelineRegistry not initialized")
    if not job.artifact_id:
        raise ValueError("artifact_id is required")
    artifact = await worker.pipeline_registry.get_artifact(artifact_id=job.artifact_id)
    if not artifact:
        raise ValueError(f"artifact_not_found:{job.artifact_id}")
    if str(artifact.status or "").upper() != "SUCCESS":
        raise ValueError("artifact_not_success")
    if str(artifact.mode or "").lower() != "build":
        raise ValueError("artifact_not_build")
    outputs = artifact.outputs or []
    if not outputs:
        raise ValueError("artifact_outputs_missing")
    output_name = (job.artifact_output_name or "").strip()
    if not output_name:
        if len(outputs) == 1:
            output = outputs[0]
            output_name = (
                str(output.get("output_name") or output.get("dataset_name") or output.get("node_id") or "").strip()
            )
        if not output_name:
            raise ValueError("artifact_output_name_required")

    matches = []
    for output in outputs:
        for key in ("output_name", "dataset_name", "node_id"):
            candidate = str(output.get(key) or "").strip()
            if candidate and candidate == output_name:
                matches.append(output)
                break
    if not matches:
        raise ValueError("artifact_output_not_found")
    if len(matches) > 1:
        raise ValueError("artifact_output_ambiguous")

    selected = matches[0]
    artifact_key = str(selected.get("artifact_commit_key") or selected.get("artifact_key") or "").strip() or None
    if not artifact_key:
        raise ValueError("artifact_key_missing")
    if not parse_s3_uri(artifact_key):
        raise ValueError("invalid_artifact_key")
    return artifact_key, output_name
