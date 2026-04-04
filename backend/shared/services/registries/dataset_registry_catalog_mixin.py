from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

import asyncpg

from shared.services.registries import dataset_registry_catalog as _catalog_registry
from shared.services.registries.dataset_registry_models import (
    BackingDatasourceRecord,
    BackingDatasourceVersionRecord,
    DatasetRecord,
    DatasetVersionRecord,
)
from shared.services.registries.dataset_registry_rows import (
    row_to_backing,
    row_to_backing_version,
)
from shared.utils.json_utils import normalize_json_payload
from shared.utils.schema_hash import compute_schema_hash


class DatasetRegistryCatalogMixin:
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
