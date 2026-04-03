"""
Dataset Registry - durable dataset metadata in Postgres.

Stores dataset metadata + versions (artifact references + samples).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

import asyncpg

from shared.config.settings import get_settings
from shared.observability.context_propagation import enrich_metadata_with_current_trace
from shared.services.registries import dataset_registry_catalog as _catalog_registry
from shared.services.registries import dataset_registry_edits as _edits_registry
from shared.services.registries import dataset_registry_governance as _governance_registry
from shared.services.registries import dataset_registry_ingest as _ingest_registry
from shared.services.registries import dataset_registry_publish as _publish_registry
from shared.services.registries import dataset_registry_schema as _schema_registry
from shared.services.registries.dataset_registry_models import (
    AccessPolicyRecord,
    BackingDatasourceRecord,
    BackingDatasourceVersionRecord,
    DatasetIngestOutboxItem,
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
from shared.services.registries.dataset_registry_rows import (
    key_spec_scope_lock_key,
    row_to_access_policy,
    row_to_backing,
    row_to_backing_version,
    row_to_gate_policy,
    row_to_gate_result,
    row_to_instance_edit,
    row_to_key_spec,
    row_to_link_edit,
    row_to_relationship_index_result,
    row_to_relationship_spec,
    row_to_schema_migration_plan,
)
from shared.services.registries import dataset_registry_relationships as _relationship_registry
from shared.services.registries.postgres_schema_registry import PostgresSchemaRegistry
from shared.utils.s3_uri import is_s3_uri, parse_s3_uri
from shared.utils.json_utils import coerce_json_dataset, normalize_json_payload
from shared.utils.schema_hash import compute_schema_hash
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)


def _inject_dataset_version(outbox_entries: List[Dict[str, Any]], dataset_version_id: str) -> None:
    """
    Ensure dataset_version_id is propagated into outbox payloads that depend on it.

    This allows outbox-based reconciliation to recover lineage/event metadata
    even if the ingest job crashes between lakeFS commit and registry publish.
    """
    for entry in outbox_entries or []:
        if not isinstance(entry, dict):
            continue
        kind = str(entry.get("kind") or "").lower()
        payload = entry.get("payload")
        if not isinstance(payload, dict):
            continue
        if kind == "eventstore":
            if payload.get("event_type") != "DATASET_VERSION_CREATED":
                continue
            data = payload.get("data")
            if not isinstance(data, dict):
                continue
            inner = data.get("payload")
            if isinstance(inner, dict) and "dataset_version_id" not in inner:
                inner = {**inner, "dataset_version_id": dataset_version_id}
                payload["data"] = {**data, "payload": inner}
                entry["payload"] = payload
        elif kind == "lineage":
            meta = payload.get("edge_metadata")
            if isinstance(meta, dict) and "dataset_version_id" not in meta:
                payload["edge_metadata"] = {**meta, "dataset_version_id": dataset_version_id}
            if isinstance(payload.get("from_node_id"), str) and payload["from_node_id"].startswith("event:"):
                payload["from_node_id"] = f"agg:DatasetVersion:{dataset_version_id}"
            entry["payload"] = payload


class DatasetRegistry(PostgresSchemaRegistry):
    _REQUIRED_TABLES = (
        "datasets",
        "dataset_versions",
        "dataset_ingest_requests",
        "dataset_ingest_transactions",
        "dataset_ingest_outbox",
        "backing_datasources",
        "backing_datasource_versions",
        "key_specs",
        "gate_policies",
        "gate_results",
        "access_policies",
        "instance_edits",
        "relationship_specs",
        "relationship_index_results",
        "link_edits",
        "schema_migration_plans",
    )

    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_datasets",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
        allow_runtime_ddl_bootstrap: Optional[bool] = None,
    ):
        perf = get_settings().performance
        pool_min_value = int(pool_min) if pool_min is not None else int(perf.dataset_registry_pg_pool_min)
        pool_max_value = int(pool_max) if pool_max is not None else int(perf.dataset_registry_pg_pool_max)
        super().__init__(
            dsn=dsn,
            schema=schema,
            pool_min=pool_min_value,
            pool_max=pool_max_value,
            command_timeout=int(perf.dataset_registry_pg_command_timeout_seconds),
            allow_runtime_ddl_bootstrap=allow_runtime_ddl_bootstrap,
        )

    def _required_tables(self) -> tuple[str, ...]:
        return self._REQUIRED_TABLES

    @staticmethod
    def _row_to_backing(row: asyncpg.Record) -> BackingDatasourceRecord:
        return row_to_backing(row)

    @staticmethod
    def _row_to_backing_version(row: asyncpg.Record) -> BackingDatasourceVersionRecord:
        return row_to_backing_version(row)

    @staticmethod
    def _resolve_version_schema_hash(
        *,
        sample_json: Optional[Dict[str, Any]],
        schema_json: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        if isinstance(sample_json, dict) and isinstance(sample_json.get("columns"), list):
            return compute_schema_hash(sample_json.get("columns") or [])
        if isinstance(schema_json, dict) and isinstance(schema_json.get("columns"), list):
            return compute_schema_hash(schema_json.get("columns") or [])
        return None

    async def _upsert_backing_version_for_dataset_version(
        self,
        conn: asyncpg.Connection,
        *,
        dataset_id: str,
        dataset_version_id: str,
        artifact_key: Optional[str],
        schema_hash: Optional[str],
    ) -> None:
        if not schema_hash:
            return
        backing = await conn.fetchrow(
            f"""
            SELECT backing_id
            FROM {self._schema}.backing_datasources
            WHERE dataset_id = $1::uuid
            ORDER BY created_at DESC
            LIMIT 1
            """,
            dataset_id,
        )
        if not backing:
            return
        metadata_payload = normalize_json_payload({"artifact_key": artifact_key} if artifact_key else {})
        row = await conn.fetchrow(
            f"""
            INSERT INTO {self._schema}.backing_datasource_versions (
                backing_version_id, backing_id, dataset_version_id, schema_hash, artifact_key, metadata
            ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6::jsonb)
            ON CONFLICT (backing_id, dataset_version_id) DO UPDATE
              SET schema_hash = EXCLUDED.schema_hash,
                  artifact_key = EXCLUDED.artifact_key,
                  metadata = EXCLUDED.metadata
            RETURNING backing_version_id
            """,
            str(uuid4()),
            str(backing["backing_id"]),
            dataset_version_id,
            schema_hash,
            artifact_key,
            metadata_payload,
        )
        if not row:
            raise RuntimeError("Failed to upsert backing datasource version")

    @staticmethod
    def _row_to_key_spec(row: asyncpg.Record) -> KeySpecRecord:
        return row_to_key_spec(row)

    @staticmethod
    def _key_spec_scope_lock_key(*, dataset_id: str, dataset_version_id: Optional[str]) -> str:
        return key_spec_scope_lock_key(dataset_id=dataset_id, dataset_version_id=dataset_version_id)

    @staticmethod
    def _row_to_gate_policy(row: asyncpg.Record) -> GatePolicyRecord:
        return row_to_gate_policy(row)

    @staticmethod
    def _row_to_gate_result(row: asyncpg.Record) -> GateResultRecord:
        return row_to_gate_result(row)

    @staticmethod
    def _row_to_access_policy(row: asyncpg.Record) -> AccessPolicyRecord:
        return row_to_access_policy(row)

    @staticmethod
    def _row_to_instance_edit(row: asyncpg.Record) -> InstanceEditRecord:
        return row_to_instance_edit(row)

    @staticmethod
    def _row_to_relationship_spec(row: asyncpg.Record) -> RelationshipSpecRecord:
        return row_to_relationship_spec(row)

    @staticmethod
    def _row_to_relationship_index_result(row: asyncpg.Record) -> RelationshipIndexResultRecord:
        return row_to_relationship_index_result(row)

    @staticmethod
    def _row_to_link_edit(row: asyncpg.Record) -> LinkEditRecord:
        return row_to_link_edit(row)

    @staticmethod
    def _row_to_schema_migration_plan(row: asyncpg.Record) -> SchemaMigrationPlanRecord:
        return row_to_schema_migration_plan(row)

    async def _ensure_tables(self, conn: asyncpg.Connection) -> None:
        await _schema_registry.ensure_tables(self, conn)

    async def create_dataset(
        self,
        *,
        db_name: str,
        name: str,
        description: Optional[str],
        source_type: str,
        source_ref: Optional[str] = None,
        schema_json: Optional[Dict[str, Any]] = None,
        branch: str = "main",
        dataset_id: Optional[str] = None,
    ) -> DatasetRecord:
        return await _catalog_registry.create_dataset(
            self,
            db_name=db_name,
            name=name,
            description=description,
            source_type=source_type,
            source_ref=source_ref,
            schema_json=schema_json,
            branch=branch,
            dataset_id=dataset_id,
        )

    async def list_datasets(self, *, db_name: str, branch: Optional[str] = None) -> List[Dict[str, Any]]:
        return await _catalog_registry.list_datasets(self, db_name=db_name, branch=branch)

    async def list_all_datasets(
        self,
        *,
        branch: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return await _catalog_registry.list_all_datasets(
            self,
            branch=branch,
            limit=limit,
            offset=offset,
            order_by=order_by,
        )

    async def count_datasets_by_db_names(
        self,
        *,
        db_names: Iterable[str],
        branch: Optional[str] = None,
    ) -> Dict[str, int]:
        return await _catalog_registry.count_datasets_by_db_names(
            self,
            db_names=db_names,
            branch=branch,
        )

    async def get_dataset(self, *, dataset_id: str) -> Optional[DatasetRecord]:
        return await _catalog_registry.get_dataset(self, dataset_id=dataset_id)

    async def delete_dataset(self, *, dataset_id: str) -> bool:
        """Delete a dataset and all related records (CASCADE)."""
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                f"DELETE FROM {self._schema}.datasets WHERE dataset_id = $1",
                dataset_id,
            )
            return result.endswith("1")

    async def get_dataset_by_name(
        self,
        *,
        db_name: str,
        name: str,
        branch: str = "main",
    ) -> Optional[DatasetRecord]:
        return await _catalog_registry.get_dataset_by_name(
            self,
            db_name=db_name,
            name=name,
            branch=branch,
        )

    async def update_schema(
        self,
        *,
        dataset_id: str,
        schema_json: Dict[str, Any],
    ) -> None:
        """Update the schema_json of an existing dataset."""
        if not self._pool:
            raise RuntimeError("DatasetRegistry not connected")
        schema_payload = normalize_json_payload(schema_json)
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.datasets
                SET schema_json = $2::jsonb,
                    updated_at = NOW()
                WHERE dataset_id = $1
                """,
                dataset_id,
                schema_payload,
            )

    async def get_dataset_by_source_ref(
        self,
        *,
        db_name: str,
        source_type: str,
        source_ref: str,
        branch: str = "main",
    ) -> Optional[DatasetRecord]:
        return await _catalog_registry.get_dataset_by_source_ref(
            self,
            db_name=db_name,
            source_type=source_type,
            source_ref=source_ref,
            branch=branch,
        )

    async def add_version(
        self,
        *,
        dataset_id: str,
        lakefs_commit_id: str,
        artifact_key: Optional[str],
        row_count: Optional[int],
        sample_json: Optional[Dict[str, Any]] = None,
        schema_json: Optional[Dict[str, Any]] = None,
        version_id: Optional[str] = None,
        ingest_request_id: Optional[str] = None,
        promoted_from_artifact_id: Optional[str] = None,
    ) -> DatasetVersionRecord:
        return await _catalog_registry.add_version(
            self,
            dataset_id=dataset_id,
            lakefs_commit_id=lakefs_commit_id,
            artifact_key=artifact_key,
            row_count=row_count,
            sample_json=sample_json,
            schema_json=schema_json,
            version_id=version_id,
            ingest_request_id=ingest_request_id,
            promoted_from_artifact_id=promoted_from_artifact_id,
        )

    async def get_latest_version(self, *, dataset_id: str) -> Optional[DatasetVersionRecord]:
        return await _catalog_registry.get_latest_version(self, dataset_id=dataset_id)

    async def get_version(self, *, version_id: str) -> Optional[DatasetVersionRecord]:
        return await _catalog_registry.get_version(self, version_id=version_id)

    async def get_version_by_ingest_request(
        self,
        *,
        ingest_request_id: str,
    ) -> Optional[DatasetVersionRecord]:
        return await _catalog_registry.get_version_by_ingest_request(self, ingest_request_id=ingest_request_id)

    async def create_backing_datasource(
        self,
        *,
        dataset_id: str,
        db_name: str,
        name: str,
        branch: str = "main",
        description: Optional[str] = None,
        source_type: str = "dataset",
        source_ref: Optional[str] = None,
        backing_id: Optional[str] = None,
    ) -> BackingDatasourceRecord:
        return await _catalog_registry.create_backing_datasource(
            self,
            dataset_id=dataset_id,
            db_name=db_name,
            name=name,
            branch=branch,
            description=description,
            source_type=source_type,
            source_ref=source_ref,
            backing_id=backing_id,
        )

    async def get_backing_datasource(self, *, backing_id: str) -> Optional[BackingDatasourceRecord]:
        return await _catalog_registry.get_backing_datasource(self, backing_id=backing_id)

    async def get_backing_datasource_by_dataset(
        self,
        *,
        dataset_id: str,
        branch: Optional[str] = None,
    ) -> Optional[BackingDatasourceRecord]:
        return await _catalog_registry.get_backing_datasource_by_dataset(
            self,
            dataset_id=dataset_id,
            branch=branch,
        )

    async def list_backing_datasources(
        self,
        *,
        db_name: str,
        branch: Optional[str] = None,
        limit: int = 200,
    ) -> List[BackingDatasourceRecord]:
        return await _catalog_registry.list_backing_datasources(
            self,
            db_name=db_name,
            branch=branch,
            limit=limit,
        )

    async def get_or_create_backing_datasource(
        self,
        *,
        dataset: DatasetRecord,
        source_type: str = "dataset",
        source_ref: Optional[str] = None,
    ) -> BackingDatasourceRecord:
        return await _catalog_registry.get_or_create_backing_datasource(
            self,
            dataset=dataset,
            source_type=source_type,
            source_ref=source_ref,
        )

    async def create_backing_datasource_version(
        self,
        *,
        backing_id: str,
        dataset_version_id: str,
        schema_hash: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> BackingDatasourceVersionRecord:
        return await _catalog_registry.create_backing_datasource_version(
            self,
            backing_id=backing_id,
            dataset_version_id=dataset_version_id,
            schema_hash=schema_hash,
            metadata=metadata,
        )

    async def _resolve_backing_datasource_version_materialization(
        self,
        *,
        backing_id: str,
        dataset_version_id: str,
        schema_hash: Optional[str] = None,
    ) -> tuple[BackingDatasourceRecord, DatasetVersionRecord, str]:
        return await _catalog_registry.resolve_backing_datasource_version_materialization(
            self,
            backing_id=backing_id,
            dataset_version_id=dataset_version_id,
            schema_hash=schema_hash,
        )

    async def _insert_backing_datasource_version(
        self,
        *,
        backing_id: str,
        dataset_version_id: str,
        schema_hash: str,
        artifact_key: Optional[str],
        metadata_payload: Dict[str, Any],
    ) -> BackingDatasourceVersionRecord:
        return await _catalog_registry.insert_backing_datasource_version(
            self,
            backing_id=backing_id,
            dataset_version_id=dataset_version_id,
            schema_hash=schema_hash,
            artifact_key=artifact_key,
            metadata_payload=metadata_payload,
        )

    async def _reconcile_backing_datasource_version(
        self,
        *,
        record: BackingDatasourceVersionRecord,
        schema_hash: str,
        artifact_key: Optional[str],
    ) -> BackingDatasourceVersionRecord:
        return await _catalog_registry.reconcile_backing_datasource_version(
            self,
            record=record,
            schema_hash=schema_hash,
            artifact_key=artifact_key,
        )

    async def get_backing_datasource_version(
        self,
        *,
        version_id: str,
    ) -> Optional[BackingDatasourceVersionRecord]:
        return await _catalog_registry.get_backing_datasource_version(self, version_id=version_id)

    async def get_backing_datasource_version_by_dataset_version(
        self,
        *,
        dataset_version_id: str,
    ) -> Optional[BackingDatasourceVersionRecord]:
        return await _catalog_registry.get_backing_datasource_version_by_dataset_version(
            self,
            dataset_version_id=dataset_version_id,
        )

    async def get_backing_datasource_version_for_dataset(
        self,
        *,
        backing_id: str,
        dataset_version_id: str,
    ) -> Optional[BackingDatasourceVersionRecord]:
        return await _catalog_registry.get_backing_datasource_version_for_dataset(
            self,
            backing_id=backing_id,
            dataset_version_id=dataset_version_id,
        )

    async def list_backing_datasource_versions(
        self,
        *,
        backing_id: str,
        limit: int = 200,
    ) -> List[BackingDatasourceVersionRecord]:
        return await _catalog_registry.list_backing_datasource_versions(
            self,
            backing_id=backing_id,
            limit=limit,
        )

    async def get_or_create_backing_datasource_version(
        self,
        *,
        backing_id: str,
        dataset_version_id: str,
        schema_hash: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> BackingDatasourceVersionRecord:
        return await _catalog_registry.get_or_create_backing_datasource_version(
            self,
            backing_id=backing_id,
            dataset_version_id=dataset_version_id,
            schema_hash=schema_hash,
            metadata=metadata,
        )

    async def create_key_spec(
        self,
        *,
        dataset_id: str,
        spec: Dict[str, Any],
        dataset_version_id: Optional[str] = None,
        status: str = "ACTIVE",
        key_spec_id: Optional[str] = None,
    ) -> KeySpecRecord:
        return await _governance_registry.create_key_spec(
            self,
            dataset_id=dataset_id,
            spec=spec,
            dataset_version_id=dataset_version_id,
            status=status,
            key_spec_id=key_spec_id,
        )

    async def _get_key_spec_for_scope(
        self,
        conn: asyncpg.Connection,
        *,
        dataset_id: str,
        dataset_version_id: Optional[str] = None,
    ) -> Optional[asyncpg.Record]:
        return await _governance_registry.get_key_spec_for_scope(
            self,
            conn,
            dataset_id=dataset_id,
            dataset_version_id=dataset_version_id,
        )

    async def get_or_create_key_spec(
        self,
        *,
        dataset_id: str,
        spec: Dict[str, Any],
        dataset_version_id: Optional[str] = None,
        status: str = "ACTIVE",
        key_spec_id: Optional[str] = None,
    ) -> tuple[KeySpecRecord, bool]:
        return await _governance_registry.get_or_create_key_spec(
            self,
            dataset_id=dataset_id,
            spec=spec,
            dataset_version_id=dataset_version_id,
            status=status,
            key_spec_id=key_spec_id,
        )

    async def get_key_spec(self, *, key_spec_id: str) -> Optional[KeySpecRecord]:
        return await _governance_registry.get_key_spec(self, key_spec_id=key_spec_id)

    async def get_key_spec_for_dataset(
        self,
        *,
        dataset_id: str,
        dataset_version_id: Optional[str] = None,
    ) -> Optional[KeySpecRecord]:
        return await _governance_registry.get_key_spec_for_dataset(
            self,
            dataset_id=dataset_id,
            dataset_version_id=dataset_version_id,
        )

    async def list_key_specs(
        self,
        *,
        dataset_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[KeySpecRecord]:
        return await _governance_registry.list_key_specs(
            self,
            dataset_id=dataset_id,
            limit=limit,
        )

    async def upsert_gate_policy(
        self,
        *,
        scope: str,
        name: str,
        description: Optional[str] = None,
        rules: Optional[Dict[str, Any]] = None,
        status: str = "ACTIVE",
    ) -> GatePolicyRecord:
        return await _governance_registry.upsert_gate_policy(
            self,
            scope=scope,
            name=name,
            description=description,
            rules=rules,
            status=status,
        )

    async def get_gate_policy(
        self,
        *,
        scope: str,
        name: str,
    ) -> Optional[GatePolicyRecord]:
        return await _governance_registry.get_gate_policy(
            self,
            scope=scope,
            name=name,
        )

    async def list_gate_policies(
        self,
        *,
        scope: Optional[str] = None,
        limit: int = 200,
    ) -> List[GatePolicyRecord]:
        return await _governance_registry.list_gate_policies(
            self,
            scope=scope,
            limit=limit,
        )

    async def record_gate_result(
        self,
        *,
        scope: str,
        subject_type: str,
        subject_id: str,
        status: str,
        details: Optional[Dict[str, Any]] = None,
        policy_name: str = "default",
    ) -> GateResultRecord:
        return await _governance_registry.record_gate_result(
            self,
            scope=scope,
            subject_type=subject_type,
            subject_id=subject_id,
            status=status,
            details=details,
            policy_name=policy_name,
        )

    async def list_gate_results(
        self,
        *,
        scope: Optional[str] = None,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[GateResultRecord]:
        return await _governance_registry.list_gate_results(
            self,
            scope=scope,
            subject_type=subject_type,
            subject_id=subject_id,
            limit=limit,
        )

    async def upsert_access_policy(
        self,
        *,
        db_name: str,
        scope: str,
        subject_type: str,
        subject_id: str,
        policy: Optional[Dict[str, Any]] = None,
        status: str = "ACTIVE",
    ) -> AccessPolicyRecord:
        return await _governance_registry.upsert_access_policy(
            self,
            db_name=db_name,
            scope=scope,
            subject_type=subject_type,
            subject_id=subject_id,
            policy=policy,
            status=status,
        )

    async def get_access_policy(
        self,
        *,
        db_name: str,
        scope: str,
        subject_type: str,
        subject_id: str,
        status: Optional[str] = "ACTIVE",
    ) -> Optional[AccessPolicyRecord]:
        return await _governance_registry.get_access_policy(
            self,
            db_name=db_name,
            scope=scope,
            subject_type=subject_type,
            subject_id=subject_id,
            status=status,
        )

    async def list_access_policies(
        self,
        *,
        db_name: Optional[str] = None,
        scope: Optional[str] = None,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[AccessPolicyRecord]:
        return await _governance_registry.list_access_policies(
            self,
            db_name=db_name,
            scope=scope,
            subject_type=subject_type,
            subject_id=subject_id,
            status=status,
            limit=limit,
        )

    async def record_instance_edit(
        self,
        *,
        db_name: str,
        class_id: str,
        instance_id: str,
        edit_type: str,
        metadata: Optional[Dict[str, Any]] = None,
        status: str = "ACTIVE",
        fields: Optional[List[str]] = None,
    ) -> InstanceEditRecord:
        return await _edits_registry.record_instance_edit(
            self,
            db_name=db_name,
            class_id=class_id,
            instance_id=instance_id,
            edit_type=edit_type,
            metadata=metadata,
            status=status,
            fields=fields,
        )

    async def count_instance_edits(
        self,
        *,
        db_name: str,
        class_id: str,
        status: Optional[str] = None,
    ) -> int:
        return await _edits_registry.count_instance_edits(
            self,
            db_name=db_name,
            class_id=class_id,
            status=status,
        )

    async def list_instance_edits(
        self,
        *,
        db_name: str,
        class_id: Optional[str] = None,
        instance_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[InstanceEditRecord]:
        return await _edits_registry.list_instance_edits(
            self,
            db_name=db_name,
            class_id=class_id,
            instance_id=instance_id,
            status=status,
            limit=limit,
        )

    async def clear_instance_edits(
        self,
        *,
        db_name: str,
        class_id: str,
    ) -> int:
        return await _edits_registry.clear_instance_edits(
            self,
            db_name=db_name,
            class_id=class_id,
        )

    async def remap_instance_edits(
        self,
        *,
        db_name: str,
        class_id: str,
        id_map: Dict[str, str],
        status: Optional[str] = None,
    ) -> int:
        return await _edits_registry.remap_instance_edits(
            self,
            db_name=db_name,
            class_id=class_id,
            id_map=id_map,
            status=status,
        )

    async def get_instance_edit_field_stats(
        self,
        *,
        db_name: str,
        class_id: str,
        fields: List[str],
        status: Optional[str] = "ACTIVE",
    ) -> Dict[str, Any]:
        return await _edits_registry.get_instance_edit_field_stats(
            self,
            db_name=db_name,
            class_id=class_id,
            fields=fields,
            status=status,
        )

    async def apply_instance_edit_field_moves(
        self,
        *,
        db_name: str,
        class_id: str,
        field_moves: Dict[str, str],
        status: Optional[str] = "ACTIVE",
    ) -> int:
        return await _edits_registry.apply_instance_edit_field_moves(
            self,
            db_name=db_name,
            class_id=class_id,
            field_moves=field_moves,
            status=status,
        )

    async def update_instance_edit_status_by_fields(
        self,
        *,
        db_name: str,
        class_id: str,
        fields: List[str],
        new_status: str,
        status: Optional[str] = "ACTIVE",
        metadata_note: Optional[str] = None,
    ) -> int:
        return await _edits_registry.update_instance_edit_status_by_fields(
            self,
            db_name=db_name,
            class_id=class_id,
            fields=fields,
            new_status=new_status,
            status=status,
            metadata_note=metadata_note,
        )

    async def create_relationship_spec(
        self,
        *,
        link_type_id: str,
        db_name: str,
        source_object_type: str,
        target_object_type: str,
        predicate: str,
        spec_type: str,
        dataset_id: str,
        mapping_spec_id: str,
        mapping_spec_version: int,
        spec: Dict[str, Any],
        dataset_version_id: Optional[str] = None,
        status: str = "ACTIVE",
        auto_sync: bool = True,
        relationship_spec_id: Optional[str] = None,
    ) -> RelationshipSpecRecord:
        return await _relationship_registry.create_relationship_spec(
            self,
            link_type_id=link_type_id,
            db_name=db_name,
            source_object_type=source_object_type,
            target_object_type=target_object_type,
            predicate=predicate,
            spec_type=spec_type,
            dataset_id=dataset_id,
            mapping_spec_id=mapping_spec_id,
            mapping_spec_version=mapping_spec_version,
            spec=spec,
            dataset_version_id=dataset_version_id,
            status=status,
            auto_sync=auto_sync,
            relationship_spec_id=relationship_spec_id,
        )

    async def update_relationship_spec(
        self,
        *,
        relationship_spec_id: str,
        status: Optional[str] = None,
        spec: Optional[Dict[str, Any]] = None,
        auto_sync: Optional[bool] = None,
        dataset_id: Optional[str] = None,
        dataset_version_id: Optional[str] = None,
        mapping_spec_id: Optional[str] = None,
        mapping_spec_version: Optional[int] = None,
    ) -> Optional[RelationshipSpecRecord]:
        return await _relationship_registry.update_relationship_spec(
            self,
            relationship_spec_id=relationship_spec_id,
            status=status,
            spec=spec,
            auto_sync=auto_sync,
            dataset_id=dataset_id,
            dataset_version_id=dataset_version_id,
            mapping_spec_id=mapping_spec_id,
            mapping_spec_version=mapping_spec_version,
        )

    async def record_relationship_index_result(
        self,
        *,
        relationship_spec_id: str,
        status: str,
        stats: Optional[Dict[str, Any]] = None,
        errors: Optional[List[Dict[str, Any]]] = None,
        dataset_version_id: Optional[str] = None,
        mapping_spec_version: Optional[int] = None,
        lineage: Optional[Dict[str, Any]] = None,
        indexed_at: Optional[datetime] = None,
    ) -> Optional[RelationshipIndexResultRecord]:
        return await _relationship_registry.record_relationship_index_result(
            self,
            relationship_spec_id=relationship_spec_id,
            status=status,
            stats=stats,
            errors=errors,
            dataset_version_id=dataset_version_id,
            mapping_spec_version=mapping_spec_version,
            lineage=lineage,
            indexed_at=indexed_at,
        )

    async def get_relationship_spec(
        self,
        *,
        relationship_spec_id: Optional[str] = None,
        link_type_id: Optional[str] = None,
    ) -> Optional[RelationshipSpecRecord]:
        return await _relationship_registry.get_relationship_spec(
            self,
            relationship_spec_id=relationship_spec_id,
            link_type_id=link_type_id,
        )

    async def list_relationship_specs(
        self,
        *,
        db_name: Optional[str] = None,
        dataset_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[RelationshipSpecRecord]:
        return await _relationship_registry.list_relationship_specs(
            self,
            db_name=db_name,
            dataset_id=dataset_id,
            status=status,
            limit=limit,
        )

    async def list_relationship_specs_by_relationship_object_type(
        self,
        *,
        db_name: str,
        relationship_object_type: str,
        status: Optional[str] = "ACTIVE",
        limit: int = 200,
    ) -> List[RelationshipSpecRecord]:
        return await _relationship_registry.list_relationship_specs_by_relationship_object_type(
            self,
            db_name=db_name,
            relationship_object_type=relationship_object_type,
            status=status,
            limit=limit,
        )

    async def list_relationship_index_results(
        self,
        *,
        relationship_spec_id: Optional[str] = None,
        link_type_id: Optional[str] = None,
        db_name: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[RelationshipIndexResultRecord]:
        return await _edits_registry.list_relationship_index_results(
            self,
            relationship_spec_id=relationship_spec_id,
            link_type_id=link_type_id,
            db_name=db_name,
            status=status,
            limit=limit,
        )

    async def record_link_edit(
        self,
        *,
        db_name: str,
        link_type_id: str,
        branch: str,
        source_object_type: str,
        target_object_type: str,
        predicate: str,
        source_instance_id: str,
        target_instance_id: str,
        edit_type: str,
        status: str = "ACTIVE",
        metadata: Optional[Dict[str, Any]] = None,
        edit_id: Optional[str] = None,
    ) -> LinkEditRecord:
        return await _edits_registry.record_link_edit(
            self,
            db_name=db_name,
            link_type_id=link_type_id,
            branch=branch,
            source_object_type=source_object_type,
            target_object_type=target_object_type,
            predicate=predicate,
            source_instance_id=source_instance_id,
            target_instance_id=target_instance_id,
            edit_type=edit_type,
            status=status,
            metadata=metadata,
            edit_id=edit_id,
        )

    async def list_link_edits(
        self,
        *,
        db_name: str,
        link_type_id: Optional[str] = None,
        branch: Optional[str] = None,
        status: Optional[str] = None,
        source_instance_id: Optional[str] = None,
        target_instance_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[LinkEditRecord]:
        return await _edits_registry.list_link_edits(
            self,
            db_name=db_name,
            link_type_id=link_type_id,
            branch=branch,
            status=status,
            source_instance_id=source_instance_id,
            target_instance_id=target_instance_id,
            limit=limit,
        )

    async def clear_link_edits(
        self,
        *,
        db_name: str,
        link_type_id: str,
        branch: Optional[str] = None,
    ) -> int:
        return await _edits_registry.clear_link_edits(
            self,
            db_name=db_name,
            link_type_id=link_type_id,
            branch=branch,
        )

    async def create_schema_migration_plan(
        self,
        *,
        db_name: str,
        subject_type: str,
        subject_id: str,
        plan: Dict[str, Any],
        status: str = "PENDING",
        plan_id: Optional[str] = None,
    ) -> SchemaMigrationPlanRecord:
        return await _edits_registry.create_schema_migration_plan(
            self,
            db_name=db_name,
            subject_type=subject_type,
            subject_id=subject_id,
            plan=plan,
            status=status,
            plan_id=plan_id,
        )

    async def list_schema_migration_plans(
        self,
        *,
        db_name: Optional[str] = None,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[SchemaMigrationPlanRecord]:
        return await _edits_registry.list_schema_migration_plans(
            self,
            db_name=db_name,
            subject_type=subject_type,
            subject_id=subject_id,
            status=status,
            limit=limit,
        )

    async def get_ingest_request_by_key(
        self,
        *,
        idempotency_key: str,
    ) -> Optional[DatasetIngestRequestRecord]:
        return await _ingest_registry.get_ingest_request_by_key(self, idempotency_key=idempotency_key)

    async def get_ingest_request(
        self,
        *,
        ingest_request_id: str,
    ) -> Optional[DatasetIngestRequestRecord]:
        return await _ingest_registry.get_ingest_request(self, ingest_request_id=ingest_request_id)

    async def create_ingest_request(
        self,
        *,
        dataset_id: str,
        db_name: str,
        branch: str,
        idempotency_key: str,
        request_fingerprint: Optional[str],
        schema_json: Optional[Dict[str, Any]] = None,
        sample_json: Optional[Dict[str, Any]] = None,
        row_count: Optional[int] = None,
        source_metadata: Optional[Dict[str, Any]] = None,
    ) -> tuple[DatasetIngestRequestRecord, bool]:
        return await _ingest_registry.create_ingest_request(
            self,
            dataset_id=dataset_id,
            db_name=db_name,
            branch=branch,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            schema_json=schema_json,
            sample_json=sample_json,
            row_count=row_count,
            source_metadata=source_metadata,
        )

    async def get_ingest_transaction(
        self,
        *,
        ingest_request_id: str,
    ) -> Optional[DatasetIngestTransactionRecord]:
        return await _ingest_registry.get_ingest_transaction(self, ingest_request_id=ingest_request_id)

    async def get_ingest_transaction_by_id(
        self,
        *,
        transaction_id: str,
    ) -> Optional[DatasetIngestTransactionRecord]:
        return await _ingest_registry.get_ingest_transaction_by_id(self, transaction_id=transaction_id)

    async def list_ingest_transactions_for_dataset(
        self,
        *,
        dataset_id: str,
        branch: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[DatasetIngestTransactionRecord]:
        return await _ingest_registry.list_ingest_transactions_for_dataset(
            self,
            dataset_id=dataset_id,
            branch=branch,
            limit=limit,
            offset=offset,
        )

    async def create_ingest_transaction(
        self,
        *,
        ingest_request_id: str,
        status: str = "OPEN",
    ) -> DatasetIngestTransactionRecord:
        return await _ingest_registry.create_ingest_transaction(
            self,
            ingest_request_id=ingest_request_id,
            status=status,
        )

    async def mark_ingest_transaction_committed(
        self,
        *,
        ingest_request_id: str,
        lakefs_commit_id: str,
        artifact_key: Optional[str],
    ) -> Optional[DatasetIngestTransactionRecord]:
        return await _ingest_registry.mark_ingest_transaction_committed(
            self,
            ingest_request_id=ingest_request_id,
            lakefs_commit_id=lakefs_commit_id,
            artifact_key=artifact_key,
        )

    async def mark_ingest_transaction_aborted(
        self,
        *,
        ingest_request_id: str,
        error: str,
    ) -> Optional[DatasetIngestTransactionRecord]:
        return await _ingest_registry.mark_ingest_transaction_aborted(
            self,
            ingest_request_id=ingest_request_id,
            error=error,
        )

    async def mark_ingest_committed(
        self,
        *,
        ingest_request_id: str,
        lakefs_commit_id: str,
        artifact_key: Optional[str],
    ) -> DatasetIngestRequestRecord:
        return await _ingest_registry.mark_ingest_committed(
            self,
            ingest_request_id=ingest_request_id,
            lakefs_commit_id=lakefs_commit_id,
            artifact_key=artifact_key,
        )

    async def mark_ingest_failed(
        self,
        *,
        ingest_request_id: str,
        error: str,
    ) -> None:
        await _ingest_registry.mark_ingest_failed(self, ingest_request_id=ingest_request_id, error=error)

    async def update_ingest_request_payload(
        self,
        *,
        ingest_request_id: str,
        schema_json: Optional[Dict[str, Any]] = None,
        sample_json: Optional[Dict[str, Any]] = None,
        row_count: Optional[int] = None,
        source_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        await _ingest_registry.update_ingest_request_payload(
            self,
            ingest_request_id=ingest_request_id,
            schema_json=schema_json,
            sample_json=sample_json,
            row_count=row_count,
            source_metadata=source_metadata,
        )

    async def approve_ingest_schema(
        self,
        *,
        ingest_request_id: str,
        schema_json: Optional[Dict[str, Any]] = None,
        approved_by: Optional[str] = None,
    ) -> tuple[DatasetRecord, DatasetIngestRequestRecord]:
        return await _ingest_registry.approve_ingest_schema(
            self,
            ingest_request_id=ingest_request_id,
            schema_json=schema_json,
            approved_by=approved_by,
        )

    async def publish_ingest_request(
        self,
        *,
        ingest_request_id: str,
        dataset_id: str,
        lakefs_commit_id: str,
        artifact_key: Optional[str],
        row_count: Optional[int],
        sample_json: Optional[Dict[str, Any]],
        schema_json: Optional[Dict[str, Any]],
        apply_schema: bool = True,
        outbox_entries: Optional[List[Dict[str, Any]]] = None,
    ) -> DatasetVersionRecord:
        return await _publish_registry.publish_ingest_request(
            self,
            ingest_request_id=ingest_request_id,
            dataset_id=dataset_id,
            lakefs_commit_id=lakefs_commit_id,
            artifact_key=artifact_key,
            row_count=row_count,
            sample_json=sample_json,
            schema_json=schema_json,
            apply_schema=apply_schema,
            outbox_entries=outbox_entries,
        )

    async def claim_ingest_outbox_batch(
        self,
        *,
        limit: int = 50,
        claimed_by: Optional[str] = None,
        claim_timeout_seconds: int = 300,
    ) -> List[DatasetIngestOutboxItem]:
        return await _publish_registry.claim_ingest_outbox_batch(
            self,
            limit=limit,
            claimed_by=claimed_by,
            claim_timeout_seconds=claim_timeout_seconds,
        )

    async def mark_ingest_outbox_published(self, *, outbox_id: str) -> None:
        await _publish_registry.mark_ingest_outbox_published(self, outbox_id=outbox_id)

    async def mark_ingest_outbox_failed(
        self,
        *,
        outbox_id: str,
        error: str,
        next_attempt_at: Optional[datetime] = None,
    ) -> None:
        await _publish_registry.mark_ingest_outbox_failed(
            self,
            outbox_id=outbox_id,
            error=error,
            next_attempt_at=next_attempt_at,
        )

    async def mark_ingest_outbox_dead(self, *, outbox_id: str, error: str) -> None:
        await _publish_registry.mark_ingest_outbox_dead(self, outbox_id=outbox_id, error=error)

    async def purge_ingest_outbox(
        self,
        *,
        retention_days: int = 7,
        limit: int = 10_000,
    ) -> int:
        return await _publish_registry.purge_ingest_outbox(
            self,
            retention_days=retention_days,
            limit=limit,
        )

    async def get_ingest_outbox_metrics(self) -> Dict[str, Any]:
        return await _publish_registry.get_ingest_outbox_metrics(self)

    async def reconcile_ingest_state(
        self,
        *,
        stale_after_seconds: int = 3600,
        limit: int = 200,
        use_lock: bool = True,
        lock_key: Optional[int] = None,
    ) -> Dict[str, int]:
        return await _ingest_registry.reconcile_ingest_state(
            self,
            stale_after_seconds=stale_after_seconds,
            limit=limit,
            use_lock=use_lock,
            lock_key=lock_key,
        )
