from __future__ import annotations

from typing import Optional

import asyncpg

from shared.services.registries.dataset_registry_models import (
    AccessPolicyRecord,
    BackingDatasourceRecord,
    BackingDatasourceVersionRecord,
    DatasetIngestRequestRecord,
    DatasetIngestTransactionRecord,
    DatasetRecord,
    DatasetVersionRecord,
    GatePolicyRecord,
    GateResultRecord,
    InstanceEditRecord,
    KeySpecRecord,
    LinkEditRecord,
    RelationshipIndexResultRecord,
    RelationshipSpecRecord,
    SchemaMigrationPlanRecord,
)
from shared.utils.json_utils import coerce_json_dataset


def row_to_dataset(row: asyncpg.Record) -> DatasetRecord:
    return DatasetRecord(
        dataset_id=str(row["dataset_id"]),
        db_name=str(row["db_name"]),
        name=str(row["name"]),
        description=row["description"],
        source_type=str(row["source_type"]),
        source_ref=row["source_ref"],
        branch=str(row["branch"]),
        schema_json=coerce_json_dataset(row["schema_json"]) or {},
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def row_to_dataset_version(row: asyncpg.Record) -> DatasetVersionRecord:
    return DatasetVersionRecord(
        version_id=str(row["version_id"]),
        dataset_id=str(row["dataset_id"]),
        lakefs_commit_id=str(row["lakefs_commit_id"]),
        artifact_key=row["artifact_key"],
        row_count=row["row_count"],
        sample_json=coerce_json_dataset(row["sample_json"]) or {},
        ingest_request_id=str(row["ingest_request_id"]) if row["ingest_request_id"] else None,
        promoted_from_artifact_id=(
            str(row["promoted_from_artifact_id"]) if row["promoted_from_artifact_id"] else None
        ),
        created_at=row["created_at"],
    )


def row_to_dataset_ingest_request(row: asyncpg.Record) -> DatasetIngestRequestRecord:
    return DatasetIngestRequestRecord(
        ingest_request_id=str(row["ingest_request_id"]),
        dataset_id=str(row["dataset_id"]),
        db_name=row["db_name"],
        branch=row["branch"],
        idempotency_key=row["idempotency_key"],
        request_fingerprint=row["request_fingerprint"],
        status=row["status"],
        lakefs_commit_id=row["lakefs_commit_id"],
        artifact_key=row["artifact_key"],
        schema_json=coerce_json_dataset(row["schema_json"]) or {},
        schema_status=str(row["schema_status"] or "PENDING"),
        schema_approved_at=row["schema_approved_at"],
        schema_approved_by=row["schema_approved_by"],
        sample_json=coerce_json_dataset(row["sample_json"]) or {},
        row_count=row["row_count"],
        source_metadata=coerce_json_dataset(row["source_metadata"]) or {},
        error=row["error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        published_at=row["published_at"],
    )


def row_to_dataset_ingest_transaction(row: asyncpg.Record) -> DatasetIngestTransactionRecord:
    return DatasetIngestTransactionRecord(
        transaction_id=str(row["transaction_id"]),
        ingest_request_id=str(row["ingest_request_id"]),
        status=row["status"],
        lakefs_commit_id=row["lakefs_commit_id"],
        artifact_key=row["artifact_key"],
        error=row["error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        committed_at=row["committed_at"],
        aborted_at=row["aborted_at"],
    )


def row_to_backing(row: asyncpg.Record) -> BackingDatasourceRecord:
    return BackingDatasourceRecord(
        backing_id=str(row["backing_id"]),
        dataset_id=str(row["dataset_id"]),
        db_name=row["db_name"],
        name=row["name"],
        description=row["description"],
        source_type=row["source_type"],
        source_ref=row["source_ref"],
        branch=row["branch"],
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def row_to_backing_version(row: asyncpg.Record) -> BackingDatasourceVersionRecord:
    return BackingDatasourceVersionRecord(
        version_id=str(row["backing_version_id"]),
        backing_id=str(row["backing_id"]),
        dataset_version_id=str(row["dataset_version_id"]),
        schema_hash=row["schema_hash"],
        artifact_key=row["artifact_key"],
        metadata=coerce_json_dataset(row["metadata"]) or {},
        status=row["status"],
        created_at=row["created_at"],
    )


def row_to_key_spec(row: asyncpg.Record) -> KeySpecRecord:
    return KeySpecRecord(
        key_spec_id=str(row["key_spec_id"]),
        dataset_id=str(row["dataset_id"]),
        dataset_version_id=(str(row["dataset_version_id"]) if row["dataset_version_id"] else None),
        spec=coerce_json_dataset(row["spec"]) or {},
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def key_spec_scope_lock_key(*, dataset_id: str, dataset_version_id: Optional[str]) -> str:
    return f"dataset-key-spec:{dataset_id}:{dataset_version_id or 'dataset-default'}"


def row_to_gate_policy(row: asyncpg.Record) -> GatePolicyRecord:
    return GatePolicyRecord(
        policy_id=str(row["policy_id"]),
        scope=row["scope"],
        name=row["name"],
        description=row["description"],
        rules=coerce_json_dataset(row["rules"]) or {},
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def row_to_gate_result(row: asyncpg.Record) -> GateResultRecord:
    return GateResultRecord(
        result_id=str(row["result_id"]),
        policy_id=str(row["policy_id"]),
        scope=row["scope"],
        subject_type=row["subject_type"],
        subject_id=row["subject_id"],
        status=row["status"],
        details=coerce_json_dataset(row["details"]) or {},
        created_at=row["created_at"],
    )


def row_to_access_policy(row: asyncpg.Record) -> AccessPolicyRecord:
    return AccessPolicyRecord(
        policy_id=str(row["policy_id"]),
        db_name=row["db_name"],
        scope=row["scope"],
        subject_type=row["subject_type"],
        subject_id=row["subject_id"],
        policy=coerce_json_dataset(row["policy"]) or {},
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def row_to_instance_edit(row: asyncpg.Record) -> InstanceEditRecord:
    fields = coerce_json_dataset(row["fields"]) if "fields" in row else None
    if not isinstance(fields, list):
        fields = []
    return InstanceEditRecord(
        edit_id=str(row["edit_id"]),
        db_name=row["db_name"],
        class_id=row["class_id"],
        instance_id=row["instance_id"],
        edit_type=row["edit_type"],
        status=str(row["status"] or "ACTIVE") if "status" in row else "ACTIVE",
        fields=[str(value) for value in fields if str(value).strip()],
        metadata=coerce_json_dataset(row["metadata"]) or {},
        created_at=row["created_at"],
    )


def row_to_relationship_spec(row: asyncpg.Record) -> RelationshipSpecRecord:
    return RelationshipSpecRecord(
        relationship_spec_id=str(row["relationship_spec_id"]),
        link_type_id=str(row["link_type_id"]),
        db_name=row["db_name"],
        source_object_type=row["source_object_type"],
        target_object_type=row["target_object_type"],
        predicate=row["predicate"],
        spec_type=row["spec_type"],
        dataset_id=str(row["dataset_id"]),
        dataset_version_id=str(row["dataset_version_id"]) if row["dataset_version_id"] else None,
        mapping_spec_id=str(row["mapping_spec_id"]),
        mapping_spec_version=int(row["mapping_spec_version"]),
        spec=coerce_json_dataset(row["spec"]) or {},
        status=row["status"],
        auto_sync=bool(row["auto_sync"]),
        last_index_status=row["last_index_status"],
        last_indexed_at=row["last_indexed_at"],
        last_index_result_id=str(row["last_index_result_id"]) if row["last_index_result_id"] else None,
        last_index_stats=coerce_json_dataset(row["last_index_stats"]) or {},
        last_index_dataset_version_id=(
            str(row["last_index_dataset_version_id"]) if row["last_index_dataset_version_id"] else None
        ),
        last_index_mapping_spec_version=(
            int(row["last_index_mapping_spec_version"])
            if row["last_index_mapping_spec_version"] is not None
            else None
        ),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def row_to_relationship_index_result(row: asyncpg.Record) -> RelationshipIndexResultRecord:
    return RelationshipIndexResultRecord(
        result_id=str(row["result_id"]),
        relationship_spec_id=str(row["relationship_spec_id"]),
        link_type_id=str(row["link_type_id"]),
        db_name=row["db_name"],
        dataset_id=str(row["dataset_id"]),
        dataset_version_id=str(row["dataset_version_id"]) if row["dataset_version_id"] else None,
        mapping_spec_id=str(row["mapping_spec_id"]),
        mapping_spec_version=int(row["mapping_spec_version"]),
        status=str(row["status"]),
        stats=coerce_json_dataset(row["stats"]) or {},
        errors=coerce_json_dataset(row["errors"]) or [],
        lineage=coerce_json_dataset(row["lineage"]) or {},
        created_at=row["created_at"],
    )


def row_to_link_edit(row: asyncpg.Record) -> LinkEditRecord:
    return LinkEditRecord(
        edit_id=str(row["edit_id"]),
        db_name=row["db_name"],
        link_type_id=str(row["link_type_id"]),
        branch=row["branch"],
        source_object_type=row["source_object_type"],
        target_object_type=row["target_object_type"],
        predicate=row["predicate"],
        source_instance_id=row["source_instance_id"],
        target_instance_id=row["target_instance_id"],
        edit_type=row["edit_type"],
        status=row["status"],
        metadata=coerce_json_dataset(row["metadata"]) or {},
        created_at=row["created_at"],
    )


def row_to_schema_migration_plan(row: asyncpg.Record) -> SchemaMigrationPlanRecord:
    return SchemaMigrationPlanRecord(
        plan_id=str(row["plan_id"]),
        db_name=row["db_name"],
        subject_type=row["subject_type"],
        subject_id=row["subject_id"],
        status=row["status"],
        plan=coerce_json_dataset(row["plan"]) or {},
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
