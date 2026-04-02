from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class DatasetRecord:
    dataset_id: str
    db_name: str
    name: str
    description: Optional[str]
    source_type: str
    source_ref: Optional[str]
    branch: str
    schema_json: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class DatasetVersionRecord:
    version_id: str
    dataset_id: str
    lakefs_commit_id: str
    artifact_key: Optional[str]
    row_count: Optional[int]
    sample_json: Dict[str, Any]
    ingest_request_id: Optional[str]
    promoted_from_artifact_id: Optional[str]
    created_at: datetime


@dataclass(frozen=True)
class DatasetIngestRequestRecord:
    ingest_request_id: str
    dataset_id: str
    db_name: str
    branch: str
    idempotency_key: str
    request_fingerprint: Optional[str]
    status: str
    lakefs_commit_id: Optional[str]
    artifact_key: Optional[str]
    schema_json: Dict[str, Any]
    schema_status: str
    schema_approved_at: Optional[datetime]
    schema_approved_by: Optional[str]
    sample_json: Dict[str, Any]
    row_count: Optional[int]
    source_metadata: Dict[str, Any]
    error: Optional[str]
    created_at: datetime
    updated_at: datetime
    published_at: Optional[datetime]


@dataclass(frozen=True)
class DatasetIngestTransactionRecord:
    transaction_id: str
    ingest_request_id: str
    status: str
    lakefs_commit_id: Optional[str]
    artifact_key: Optional[str]
    error: Optional[str]
    created_at: datetime
    updated_at: datetime
    committed_at: Optional[datetime]
    aborted_at: Optional[datetime]


@dataclass(frozen=True)
class DatasetIngestOutboxItem:
    outbox_id: str
    ingest_request_id: str
    kind: str
    payload: Dict[str, Any]
    status: str
    publish_attempts: int
    retry_count: int
    error: Optional[str]
    last_error: Optional[str]
    claimed_by: Optional[str]
    claimed_at: Optional[datetime]
    next_attempt_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class BackingDatasourceRecord:
    backing_id: str
    dataset_id: str
    db_name: str
    name: str
    description: Optional[str]
    source_type: str
    source_ref: Optional[str]
    branch: str
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class BackingDatasourceVersionRecord:
    version_id: str
    backing_id: str
    dataset_version_id: str
    schema_hash: str
    artifact_key: Optional[str]
    metadata: Dict[str, Any]
    status: str
    created_at: datetime


@dataclass(frozen=True)
class KeySpecRecord:
    key_spec_id: str
    dataset_id: str
    dataset_version_id: Optional[str]
    spec: Dict[str, Any]
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class GatePolicyRecord:
    policy_id: str
    scope: str
    name: str
    description: Optional[str]
    rules: Dict[str, Any]
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class GateResultRecord:
    result_id: str
    policy_id: str
    scope: str
    subject_type: str
    subject_id: str
    status: str
    details: Dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class AccessPolicyRecord:
    policy_id: str
    db_name: str
    scope: str
    subject_type: str
    subject_id: str
    policy: Dict[str, Any]
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class InstanceEditRecord:
    edit_id: str
    db_name: str
    class_id: str
    instance_id: str
    edit_type: str
    status: str
    fields: List[str]
    metadata: Dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class RelationshipSpecRecord:
    relationship_spec_id: str
    link_type_id: str
    db_name: str
    source_object_type: str
    target_object_type: str
    predicate: str
    spec_type: str
    dataset_id: str
    dataset_version_id: Optional[str]
    mapping_spec_id: str
    mapping_spec_version: int
    spec: Dict[str, Any]
    status: str
    auto_sync: bool
    last_index_status: Optional[str]
    last_indexed_at: Optional[datetime]
    last_index_result_id: Optional[str]
    last_index_stats: Dict[str, Any]
    last_index_dataset_version_id: Optional[str]
    last_index_mapping_spec_version: Optional[int]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class RelationshipIndexResultRecord:
    result_id: str
    relationship_spec_id: str
    link_type_id: str
    db_name: str
    dataset_id: str
    dataset_version_id: Optional[str]
    mapping_spec_id: str
    mapping_spec_version: int
    status: str
    stats: Dict[str, Any]
    errors: List[Dict[str, Any]]
    lineage: Dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class LinkEditRecord:
    edit_id: str
    db_name: str
    link_type_id: str
    branch: str
    source_object_type: str
    target_object_type: str
    predicate: str
    source_instance_id: str
    target_instance_id: str
    edit_type: str
    status: str
    metadata: Dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class SchemaMigrationPlanRecord:
    plan_id: str
    db_name: str
    subject_type: str
    subject_id: str
    status: str
    plan: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
